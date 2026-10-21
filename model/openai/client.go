package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"crew-go/internal/panicerr"
	"crew-go/model"
	"crew-go/schema"
)

// maxRetryAfterSeconds 是 delta-seconds 形式能安全换算成 [time.Duration] 的上限
// （`math.MaxInt64 / int64(time.Second)`，约 292 年）。
//
// ⚠️ **这道上限挡的是整数溢出，不是「等太久」的策略**。`time.Duration(secs) * time.Second`
// 在 int64 上乘 1e9，超过这个数就绕回——实测 `Retry-After: 10000000000` 算出
// **-2346317h**，`20000000000` 算出 **+431459h（约 49 年）**。前者三个消费点都 gate
// 了 `> 0` 所以只是把负数塞进公开字段 `APIError.RetryAfter`（业务写
// `if e.RetryAfter != 0` 就会当成「服务端给了」）；**后者更糟——它是正的，会被原样采纳
// 睡下去**，只有 ctx deadline 捞得回来。
//
// 「服务端说多久就等多久、不设策略上限」那条取舍不受影响：`Retry-After: 86400`
// （日配额重置，合法且常见）照旧返 24h。
const maxRetryAfterSeconds = math.MaxInt64 / int64(time.Second)

// parseRetryAfter 解析 HTTP `Retry-After` 响应头，返回建议等待时长；解析不出返 0。
//
// RFC 9110 定义两种合法形式，provider 两种都在用：
//   - delta-seconds：`Retry-After: 120`
//   - HTTP-date：`Retry-After: Wed, 21 Oct 2015 07:28:00 GMT`
//
// now 由调用方传入而非内部取 time.Now()——HTTP-date 形式要算相对时长，注入时钟才能
// 稳定测试。已过期的 HTTP-date（服务端时钟偏移 / 传输延迟）返 0 走常规退避，不返负值。
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		// int64(secs) 而非裸 secs：常量被 int64(time.Second) 定成了 int64，而 Atoi 返 int。
		if secs <= 0 || int64(secs) > maxRetryAfterSeconds {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// Client 使用 OpenAI 聊天完成 API 实现 model.ChatModel。
type Client struct {
	apiKey    string
	baseURL   string
	model     string
	system    string // provider 名 (gen_ai.system observability attr); WithSystem 注入; 填进每条响应 Metadata.System
	tools     []openaiTool
	client    *http.Client
	extraBody map[string]any // 业务/provider 附加的 wire-level 字段 (DeepSeek thinking 等扩展); 见 WithExtraBody
	// structuredCap 结构化输出能力画像; nil=官方 OpenAI 默认 (全 strict);
	// WithStructuredCapability 覆盖 (provider 自报 / 业务应对 provider 演进)。
	structuredCap *model.StructuredCapability
	// optionalFinishReason 为 true 时，流正常结束而没收到 finish_reason 也算输出完整；
	// 见 WithOptionalFinishReason。
	optionalFinishReason bool
	// streamUnsupported 为 true 时 Stream 直接返回 model.ErrStreamUnsupported；见 WithStreamUnsupported。
	streamUnsupported bool
}

// NewClient 创建 OpenAI ChatModel。
func NewClient(apiKey, modelName string, opts ...ClientOption) *Client {
	c := &Client{
		apiKey:  apiKey,
		baseURL: "https://api.openai.com/v1",
		model:   modelName,
		client:  http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type ClientOption func(*Client)

func WithBaseURL(url string) ClientOption {
	return func(c *Client) { c.baseURL = url }
}

func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.client = hc }
}

// WithSystem 设置 model 供应商名 (gen_ai.system — observability trace span attr /
// 业务 cost metric label)。
//
// OpenAI 兼容 provider 复用同一个 openai.Client，各自标识 provider：openai="openai" /
// deepseek="deepseek" / qwen dashscope="dashscope" 等。填进每条响应的
// Message.Metadata.System (Generate + Stream 两路)。空则不上报 gen_ai.system。
func WithSystem(system string) ClientOption {
	return func(c *Client) { c.system = system }
}

// WithExtraBody 把额外字段合并到 wire payload (POST body 顶层)。
//
// 用于 OpenAI 兼容 provider 的扩展字段 (典型: DeepSeek `thinking: {type: "disabled"}`
// 控制思考模式 / Qwen3 同款 / 其他 provider 的自定义参数)。多次调用累积合并,
// 同名 key 后值覆盖前值。
//
// 字段直接顶层 merge — 不放在 messages 数组里 (那是 message 级扩展用 WithTools 等)。
func WithExtraBody(fields map[string]any) ClientOption {
	return func(c *Client) {
		if c.extraBody == nil {
			c.extraBody = make(map[string]any, len(fields))
		}
		for k, v := range fields {
			c.extraBody[k] = v
		}
	}
}

// WithStructuredCapability 覆盖本 client 的结构化输出能力画像（provider 自报，或业务
// 应对 provider 演进如未来 qwen 支持 strict）。不设时官方 OpenAI 端点默认全支持 strict
// （见 model.DefaultOpenAICapability）；openai.NewCompatible 的非 openai provider 默认走
// model.NonStrictToolCapability（见 NewCompatible）。
func WithStructuredCapability(cap model.StructuredCapability) ClientOption {
	return func(c *Client) { c.structuredCap = &cap }
}

// WithOptionalFinishReason 声明这个 provider 的流式响应可能不带 finish_reason：流正常结束
// （收到 [DONE] 或连接正常关闭）而没收到 finish_reason 时，按输出完整处理。
//
// 默认没收到 finish_reason 就判为截断：结束原因是「模型说完了」的唯一凭据，[DONE] 与连接
// 关闭只说明传输结束了。只给确实不发它的 provider 用；读出错、错误帧照样报错。
func WithOptionalFinishReason() ClientOption {
	return func(c *Client) { c.optionalFinishReason = true }
}

// WithStreamUnsupported 声明这个端点 / 模型不支持流式：Stream 不发请求，直接返回
// model.ErrStreamUnsupported，BasicAgent 据此改调 Generate。
//
// 给不接受 stream=true 的端点用。框架不从错误去猜「是不是不支持流式」，建流时的 400 照常
// 当成错误交给上层。
func WithStreamUnsupported() ClientOption {
	return func(c *Client) { c.streamUnsupported = true }
}

// StructuredCapability 返回本 client 对给定 model 的结构化输出能力画像（provider 自报，
// 满足 model.StructuredCapable）。modelName 供未来按变体细化（如 qwen-3-coder 不支持
// tool_choice=required）——P0 返配置的画像，业务对特殊变体用 WithStructuredCapability
// 覆盖。client 未配置时返官方 OpenAI 默认（全 strict）。
func (c *Client) StructuredCapability(modelName string) model.StructuredCapability {
	if c.structuredCap != nil {
		return *c.structuredCap
	}
	return model.DefaultOpenAICapability()
}

// WithTools 返回与给定工具绑定的新客户端。
func (c *Client) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *c
	cp.tools = make([]openaiTool, 0, len(tools))
	for _, t := range tools {
		cp.tools = append(cp.tools, openaiTool{
			Type: "function",
			Function: openaiFunction{
				Name:        t.Name,
				Description: t.Desc,
				Parameters:  t.Params,
			},
		})
	}
	return &cp, nil
}

// providerName 返回结构化错误（model.APIError）里的 provider 标识——优先 c.system
// （WithSystem 注入，如 "qwen" / "deepseek"），未配置时回落 "openai"（本包默认 provider）。
func (c *Client) providerName() string {
	if c.system != "" {
		return c.system
	}
	return "openai"
}

// Generate 调用 OpenAI API（非流式）。
func (c *Client) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	o := model.Apply(opts...)
	req := c.buildRequest(messages, o, false)

	respBody, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var resp openaiResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("openai：解码响应：%w", err)
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("openai：空选择")
	}
	// 结束原因跟流式同一套判法：内容被过滤、生成被打断的输出不完整，不当成正常结果
	if err := c.checkFinishReason(ctx, resp.Choices[0].FinishReason); err != nil {
		return nil, err
	}

	return c.convertResponse(&resp), nil
}

// Stream 调用启用流式传输的 OpenAI API。
//
// **token usage** (Round 8 集成验证): stream 默认启 stream_options.include_usage,
// reader 末尾会多收一个 **Content 空 + Metadata.Usage 非空** 的 *schema.Message
// (token 计费数据). 业务直接消费 reader 时:
//   - 想要 token: 读末尾 message 的 Metadata.Usage
//   - 不要 token: Content=="" 的 message 跳过即可 (不影响文本拼接)
//
// BasicAgent.streamOrGenerate 已自动聚合此 usage 到 final message.Metadata.Usage.
func (c *Client) Stream(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	if c.streamUnsupported {
		return nil, model.ErrStreamUnsupported
	}
	o := model.Apply(opts...)
	req := c.buildRequest(messages, o, true)

	httpReq, err := c.newHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, &model.APIError{Provider: c.providerName(), Err: fmt.Errorf("request failed: %w", err)}
	}

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		return nil, &model.APIError{
			Provider:   c.providerName(),
			StatusCode: httpResp.StatusCode,
			Body:       string(body),
			RetryAfter: parseRetryAfter(httpResp.Header.Get("Retry-After"), time.Now()),
		}
	}

	sr, send := schema.NewStreamReader[*schema.Message](8)
	go c.consumeStream(ctx, httpResp.Body, sr, send)
	return sr, nil
}

// --- 内部实现 ---

func (c *Client) buildRequest(messages []*schema.Message, o *model.Options, stream bool) *openaiRequest {
	modelName := c.model
	if o.Model != nil {
		modelName = *o.Model
	}

	req := &openaiRequest{
		Model:    modelName,
		Messages: convertMessages(messages),
		Stream:   stream,
	}
	// Round 8 集成验证 fix: stream 模式默认启 include_usage 让末尾 chunk 返 token
	// (OpenAI 官方推荐 stream 必启否则无计费数据). opt-out 而非
	// opt-in — 业务想要 token/cost 观测是默认诉求.
	if stream {
		req.StreamOptions = &openaiStreamOptions{IncludeUsage: true}
	}

	// 合并工具：绑定工具 + 每请求工具
	// 深拷贝避免并发调用时修改共享的 c.tools backing array
	allTools := make([]openaiTool, len(c.tools), len(c.tools)+len(o.Tools))
	copy(allTools, c.tools)
	if o.Tools != nil {
		for _, t := range o.Tools {
			allTools = append(allTools, openaiTool{
				Type: "function",
				Function: openaiFunction{
					Name:        t.Name,
					Description: t.Desc,
					Parameters:  t.Params,
				},
			})
		}
	}
	// AllowedTools 白名单：非空时只保留 Name 命中的工具（其余不发给 LLM）。
	if len(o.AllowedTools) > 0 {
		allowed := make(map[string]bool, len(o.AllowedTools))
		for _, n := range o.AllowedTools {
			allowed[n] = true
		}
		kept := make([]openaiTool, 0, len(allTools))
		for _, t := range allTools {
			if allowed[t.Function.Name] {
				kept = append(kept, t)
			}
		}
		allTools = kept
	}
	if len(allTools) > 0 {
		req.Tools = allTools
	}

	if o.Temperature != nil {
		req.Temperature = o.Temperature
	}
	if o.MaxTokens != nil {
		req.MaxTokens = o.MaxTokens
	}
	if o.TopP != nil {
		req.TopP = o.TopP
	}
	if o.Stop != nil {
		req.Stop = o.Stop
	}
	// response_format（结构化输出路径 1）：映射 model.ResponseFormat → wire。
	if o.ResponseFormat != nil {
		rf := &openaiResponseFormat{Type: o.ResponseFormat.Type}
		if js := o.ResponseFormat.JSONSchema; js != nil {
			rf.JSONSchema = &openaiResponseFormatJSONSchema{
				Name:   js.Name,
				Strict: js.Strict,
				Schema: js.Schema,
			}
		}
		req.ResponseFormat = rf
	}

	// per-request ExtraBody 叠在构造期 c.extraBody 之上（同 key per-request 赢、不同 key
	// 保留），存到 req 由 newHTTPRequest 顶层 merge 进 wire body。业务按次控制 provider
	// 专属参数（thinking / reasoning 等）经 model.WithExtraBody / 各 provider 便利包装。
	if len(c.extraBody) > 0 || len(o.ExtraBody) > 0 {
		eb := make(map[string]any, len(c.extraBody)+len(o.ExtraBody))
		for k, v := range c.extraBody {
			eb[k] = v
		}
		for k, v := range o.ExtraBody {
			eb[k] = v
		}
		req.extraBody = eb
	}

	// tool_choice 只在这次请求真有工具时发：没有工具时 provider 拒绝（required 配零工具返 400）。框架自己就会
	// 合法地产生没有工具的一轮（软降落），AllowedTools 也可能把工具滤光——省掉，不报错。「有没有工具」看的是
	// 最终请求体：ExtraBody 带了 tools 时合并会整个替换绑定的工具（provider 内置工具常走这条），以它为准。
	if o.ToolChoice != nil && wireHasTools(req) {
		req.ToolChoice = string(*o.ToolChoice)
	}

	return req
}

// wireHasTools 判断合并 ExtraBody 之后的请求体里有没有非空的 tools。
func wireHasTools(req *openaiRequest) bool {
	v, ok := req.extraBody["tools"]
	if !ok {
		return len(req.Tools) > 0
	}
	if v == nil {
		return false
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() > 0
	}
	return true
}

func (c *Client) doRequest(ctx context.Context, req *openaiRequest) ([]byte, error) {
	httpReq, err := c.newHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, &model.APIError{Provider: c.providerName(), Err: fmt.Errorf("request failed: %w", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &model.APIError{Provider: c.providerName(), Err: fmt.Errorf("read response: %w", err)}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &model.APIError{
			Provider:   c.providerName(),
			StatusCode: resp.StatusCode,
			Body:       string(body),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		}
	}
	return body, nil
}

func (c *Client) newHTTPRequest(ctx context.Context, req *openaiRequest) (*http.Request, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("openai: encode request: %w", err)
	}

	// 合并 extraBody 顶层字段：req.extraBody 是构造期 c.extraBody + per-request
	// model.WithExtraBody 的合并结果（buildRequest 算好，per-request 同 key 赢）。
	if len(req.extraBody) > 0 {
		body, err = mergeExtraFields(body, req.extraBody)
		if err != nil {
			return nil, fmt.Errorf("openai: merge extra body: %w", err)
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	return httpReq, nil
}

// mergeExtraFields 把 extra 字段顶层 merge 进已序列化的 wire body。
// 同名 key extra 值覆盖原 body 值 (extra 优先 — caller 显式注入的字段意图明确)。
func mergeExtraFields(body []byte, extra map[string]any) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	for k, v := range extra {
		m[k] = v
	}
	return json.Marshal(m)
}

func (c *Client) convertResponse(resp *openaiResponse) *schema.Message {
	choice := resp.Choices[0]
	msg := &schema.Message{
		Role:    schema.MessageRole(choice.Message.Role),
		Content: choice.Message.Content,
		// thinking 模型推理文本——存下来供多轮回传（ISSUE-89）
		ReasoningContent: choice.Message.ReasoningContent,
		// json_schema strict 拒答文本——解析进 Message.Refusal 供上层读（当前 P1 走 ModeTool
		// 不产 refusal；response_format strict 路径 P2/P3 未接，暂无生产消费方，§4.6）
		Refusal: choice.Message.Refusal,
	}

	// 填充元数据（完成原因 + 使用量 + model name + provider）
	if choice.FinishReason != "" || resp.Usage != nil || resp.Model != "" || c.system != "" {
		msg.Metadata = &schema.MessageMetadata{
			FinishReason: choice.FinishReason,
			Model:        resp.Model, // observability gen_ai.request.model / response.model
			System:       c.system,   // observability gen_ai.system (provider)
		}
		if resp.Usage != nil {
			usage := &schema.MessageUsage{
				InputTokens:  int32(resp.Usage.PromptTokens),
				OutputTokens: int32(resp.Usage.CompletionTokens),
				TotalTokens:  int32(resp.Usage.TotalTokens),
			}
			if resp.Usage.PromptTokensDetails != nil {
				usage.CachedInputTokens = int32(resp.Usage.PromptTokensDetails.CachedTokens)
			}
			msg.Metadata.Usage = usage
		}
	}

	// 工具调用 → ContentBlocks
	for _, tc := range choice.Message.ToolCalls {
		msg.ContentBlocks = append(msg.ContentBlocks, &schema.ContentBlock{
			Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{
				Id:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			},
		})
	}

	return msg
}

// indexToToolCallID 跨 stream 内的 chunks 跟踪 (index → tool_call_id) 映射——
// OpenAI 流式 delta 后续 chunks 只含 index 不含 id，需用首个 chunk 的 id 回填
// 让上层 streamOrGenerate 按 id 累加 args（audit Round 3 Bug #2 修复）。
//
// 单 stream 单 goroutine 访问，无需锁。
type indexToToolCallID map[int]string

// 流式日志的字段名。
const (
	logKeyProvider     = "provider"
	logKeyFinishReason = "finish_reason"
	logKeyErr          = "err"
)

// maxErrorDetailBytes 是放进错误文本的 provider 原文上限——错误文本会交给前端、对端与
// span，不该整帧照搬。
const maxErrorDetailBytes = 512

// streamState 记一次流式响应里跟「输出是否完整」有关的状态。
type streamState struct {
	// finishReason 是收到的结束原因，收到之前为空。完整性就看它：收到了才算模型说完，
	// [DONE] 与连接关闭都只说明传输结束了。
	finishReason string
	// lastUnrecognized 是最近一帧既没有 choices 也没有 usage 的原文（截短过）。流没收到
	// 结束原因就停了时放进错误里：有的 provider 把错误写成这种形状，不带 error 键。
	lastUnrecognized string
}

// consumeStream 读 SSE 响应体，把每个 chunk 转成 *schema.Message 交给 send，最后给流一个
// 结论：输出完整就闭流，否则先 SendError 再闭流。
//
// 只有收到 finish_reason 才算输出完整（配了 WithOptionalFinishReason 的 provider，流正常结束
// 也算）。下面这些都报错，不当正常结束：读响应体出错、调用方 ctx 取消、错误帧（data 里带
// error 键，或 event: error）、data 不是 JSON、结束原因表示输出被截掉或生成被打断、流结束时
// 没收到结束原因。只闭流不报错的话，消费方读到 io.EOF，会把半截内容当成完整答复。
func (c *Client) consumeStream(ctx context.Context, body io.ReadCloser, sr *schema.StreamReader[*schema.Message], send func(*schema.Message)) {
	defer body.Close()
	defer schema.CloseSend(sr)
	// review-v6 M-23 fix: SSE 流处理某 chunk 字段类型变更 / send 闭包路径 panic 时
	// goroutine 无 recover 让整进程 crash. defer recover 转 SendError 让 caller
	// reader.Recv 拿错走业务错路径而非进程死.
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			schema.SendError(sr, panicerr.WrapOpaque("openai consumeStream panic", pe))
			panicerr.Log(ctx, slog.LevelError, "openai: 流解析 goroutine 兜住 panic，已作为流错误交给消费方", pe)
		}
	}()

	// audit Round 3 Bug #2 修复：跨 chunks 跟踪 (index → tool_call_id)
	indexMap := make(indexToToolCallID)
	dec := newSSEDecoder(body)
	var st streamState
	for {
		select {
		case <-sr.Done():
			return // 消费方已经不要了，不用再给结论
		default:
		}
		// ctx 取消要作为错误交出去：只闭流的话，消费方的 Recv 可能先读到 io.EOF
		if err := ctx.Err(); err != nil {
			schema.SendError(sr, err)
			return
		}
		ev, err := dec.next()
		if err != nil {
			c.endStream(ctx, sr, &st, err)
			return
		}
		done, err := c.handleStreamEvent(ctx, ev, &st, indexMap, send)
		if err != nil {
			schema.SendError(sr, err)
			return
		}
		if done {
			c.endStream(ctx, sr, &st, nil)
			return
		}
	}
}

// endStream 在流结束时给出结论。readErr 为 nil 表示收到了 [DONE]，io.EOF 表示连接正常关闭，
// 其余是读响应体出错。
func (c *Client) endStream(ctx context.Context, sr *schema.StreamReader[*schema.Message], st *streamState, readErr error) {
	if err := ctx.Err(); err != nil {
		schema.SendError(sr, err)
		return
	}
	broken := readErr != nil && !errors.Is(readErr, io.EOF)
	if st.finishReason != "" {
		if broken {
			// 结束原因已经到了，输出是完整的；断掉的只可能是后面那一帧 usage
			slog.WarnContext(ctx, "openai: stream read failed after finish_reason, usage may be missing",
				logKeyProvider, c.providerName(), logKeyErr, readErr)
		}
		return
	}
	if broken {
		schema.SendError(sr, c.streamError(fmt.Errorf("read stream: %w", readErr)))
		return
	}
	if c.optionalFinishReason {
		return
	}
	detail := ""
	if st.lastUnrecognized != "" {
		detail = "; last unrecognized chunk: " + st.lastUnrecognized
	}
	schema.SendError(sr, c.streamError(fmt.Errorf("stream ended without finish_reason%s", detail)))
}

// handleStreamEvent 处理一个 SSE 事件；done 为 true 表示收到了 [DONE]。
func (c *Client) handleStreamEvent(ctx context.Context, ev sseEvent, st *streamState, indexMap indexToToolCallID, send func(*schema.Message)) (done bool, err error) {
	if ev.event == "error" {
		return false, c.streamError(fmt.Errorf("stream error event: %s", clip(ev.data)))
	}
	if strings.TrimSpace(ev.data) == "" {
		return false, nil
	}
	if strings.HasPrefix(ev.data, "[DONE]") {
		return true, nil
	}
	var chunk openaiStreamChunk
	if err := json.Unmarshal([]byte(ev.data), &chunk); err != nil {
		if st.finishReason != "" {
			// 结束原因已经到了，输出是完整的；后面这一帧多半是 usage，类型对不上也不影响结果
			slog.WarnContext(ctx, "openai: unparsable stream chunk after finish_reason, ignored",
				logKeyProvider, c.providerName(), logKeyErr, err)
			return false, nil
		}
		// 不带原文：这一帧多半是被截断的模型输出，可能含用户的个人信息，而错误文本会进 span、交给前端与对端
		return false, c.streamError(fmt.Errorf("invalid stream chunk (%d bytes): %w", len(ev.data), err))
	}
	if isErrorValue(chunk.Error) {
		return false, c.streamError(fmt.Errorf("stream error: %s", clip(string(chunk.Error))))
	}
	// Round 8 集成验证 fix: include_usage usage 两种 wire 格式兼容:
	//   - OpenAI 标准: 独立 usage chunk (choices 空 + usage 非空)
	//   - deepseek: usage 附在末尾 content chunk (choices 非空 +
	//     finish_reason=stop + usage 非空)
	// 统一: chunk.Usage 非空就转 schema.MessageUsage. choices 空时单独
	// 发 usage-only message; choices 非空时附到该 content message 的 Metadata.
	var streamUsage *schema.MessageUsage
	if chunk.Usage != nil {
		streamUsage = &schema.MessageUsage{
			InputTokens:  int32(chunk.Usage.PromptTokens),
			OutputTokens: int32(chunk.Usage.CompletionTokens),
			TotalTokens:  int32(chunk.Usage.TotalTokens),
		}
		if chunk.Usage.PromptTokensDetails != nil {
			streamUsage.CachedInputTokens = int32(chunk.Usage.PromptTokensDetails.CachedTokens)
		}
	}
	if len(chunk.Choices) == 0 {
		if streamUsage != nil {
			send(&schema.Message{
				Role:     schema.RoleAssistant,
				Metadata: &schema.MessageMetadata{Usage: streamUsage, Model: chunk.Model, System: c.system},
			})
		} else {
			st.lastUnrecognized = clip(ev.data)
		}
		return false, nil
	}
	delta := chunk.Choices[0].Delta
	msg := &schema.Message{
		Role:    schema.MessageRole(delta.Role),
		Content: delta.Content,
		// thinking 模型流式 reasoning_content delta——per-chunk emit，
		// 上层 streamOrGenerate 聚合成 final message 的 ReasoningContent（ISSUE-89）
		ReasoningContent: delta.ReasoningContent,
		// json_schema strict 拒答流式 delta——per-chunk emit，上层聚合（§4.6）
		Refusal: delta.Refusal,
	}
	for _, tc := range delta.ToolCalls {
		// audit Round 3 Bug #2 修复：OpenAI 流式 delta 后续 chunks
		// 仅含 index 不含 id——用 indexMap 按首 chunk id 回填，让
		// 上层 streamOrGenerate 按 id 正确累加 args。
		id := tc.ID
		if id == "" && tc.Index != nil {
			if mapped, ok := indexMap[*tc.Index]; ok {
				id = mapped
			}
		} else if id != "" && tc.Index != nil {
			indexMap[*tc.Index] = id
		}
		msg.ContentBlocks = append(msg.ContentBlocks, &schema.ContentBlock{
			Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{
				Id:        id,
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			},
		})
	}
	finishReason := chunk.Choices[0].FinishReason
	if finishReason != "" || streamUsage != nil || chunk.Model != "" || c.system != "" {
		msg.Metadata = &schema.MessageMetadata{
			FinishReason: finishReason,
			Model:        chunk.Model, // observability gen_ai.request.model / response.model
			System:       c.system,    // observability gen_ai.system (provider)
		}
		// deepseek 格式: usage 附在末尾 content chunk (Round 8 fix)
		if streamUsage != nil {
			msg.Metadata.Usage = streamUsage
		}
	}
	send(msg)
	if finishReason == "" {
		return false, nil
	}
	st.finishReason = finishReason
	return false, c.checkFinishReason(ctx, finishReason)
}

// checkFinishReason 按结束原因判断这次输出算不算完整（流式与 Generate 共用）：
//   - stop、tool_calls、function_call、length：完整。length 是被 max_tokens 截断，要不要让
//     业务看见另算，这里不当错误。
//   - 空：只会出现在 Generate 的响应里（流式只拿收到的结束原因来判），响应已经完整读完、
//     解析成功，按完整处理。
//   - content_filter（OpenAI 等）、sensitive（智谱：被安全审核拦截）：内容过滤截掉了输出，
//     返回包着 model.ErrContentFiltered 的错误。
//   - model_context_window_exceeded（智谱）：输入超出模型的上下文窗口，返回包着
//     model.ErrContextWindowExceeded 的错误（不可重试，上下文压缩的第 3 层认得出、会压缩后重试）。
//   - 生成被打断或出错、输出不完整、可以重试：insufficient_system_resource / aborted（DeepSeek）、
//     network_error（智谱：模型推理异常）、error（vLLM、Mistral）、abort（vLLM：请求被中止）、
//     repetition（vLLM：检测到重复的 token 模式，模型在胡言乱语）。
//   - 其余没见过的取值按完整处理，记一条告警：provider 新加的取值多半是正常结束的变体，
//     当成错误会让整轮失败。
func (c *Client) checkFinishReason(ctx context.Context, finishReason string) error {
	switch finishReason {
	case "", "stop", "tool_calls", "function_call", "length":
		return nil
	case "content_filter", "sensitive":
		return c.streamError(model.ErrContentFiltered)
	case "model_context_window_exceeded":
		return c.streamError(fmt.Errorf("%w (finish_reason=%s)", model.ErrContextWindowExceeded, finishReason))
	case "insufficient_system_resource", "aborted", "network_error", "error", "abort", "repetition":
		return c.streamError(fmt.Errorf("generation stopped by provider: finish_reason=%s", finishReason))
	default:
		slog.WarnContext(ctx, "openai: unknown finish_reason, treated as complete",
			logKeyProvider, c.providerName(), logKeyFinishReason, finishReason)
		return nil
	}
}

// streamError 把流式过程中的错误包成 *model.APIError（StatusCode 为 0：建流已经成功，这不是
// HTTP 层的错误）。
func (c *Client) streamError(err error) error {
	return &model.APIError{Provider: c.providerName(), Err: err}
}

// isErrorValue 判断 chunk 里的 error 字段是不是一个错误。跟官方 Python SDK 一样按解析之后的
// 「真值」判：没有这个键、null、false、0、空字符串、空数组、空对象都不算。
func isErrorValue(raw json.RawMessage) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return true // 整帧已经解析成功，这里不会失败；真失败了按错误处理
	}
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return true
}

// clip 把放进错误文本的 provider 原文截到 maxErrorDetailBytes，不切断 UTF-8 字符。
func clip(s string) string {
	if len(s) <= maxErrorDetailBytes {
		return s
	}
	n := maxErrorDetailBytes
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

func convertMessages(msgs []*schema.Message) []openaiMessage {
	out := make([]openaiMessage, 0, len(msgs))
	for _, m := range msgs {
		om := openaiMessage{
			Role:    string(m.Role),
			Content: m.Content,
			// thinking 模型多轮回传：assistant 的 reasoning_content 原样附回 wire，
			// 否则 DeepSeek/Qwen3 thinking 第 2 轮 400（ISSUE-89）。非 thinking 为空不发。
			ReasoningContent: m.ReasoningContent,
		}
		// 从 ContentBlocks 中提取工具调用和工具结果。
		// OpenAI API 要求每条 role=tool 消息只携带一个 tool_call_id。
		// 若一条 schema.Message 含多个 Toolresult block（非框架正常路径，但可能由
		// 外部代码构造），则将每个 block 拆为独立的 openaiMessage，避免静默丢弃。
		var extraToolResults []openaiMessage
		for _, cb := range m.ContentBlocks {
			if cb == nil {
				continue
			}
			if cb.Toolcall != nil {
				om.ToolCalls = append(om.ToolCalls, openaiToolCall{
					ID:   cb.Toolcall.Id,
					Type: "function",
					Function: openaiToolCallFunction{
						Name:      cb.Toolcall.Name,
						Arguments: cb.Toolcall.Arguments,
					},
				})
			}
			if cb.Toolresult != nil {
				if om.ToolCallID == "" {
					// 第一个 tool result：填入主消息
					om.ToolCallID = cb.Toolresult.ToolCallId
					if om.Content == "" {
						om.Content = cb.Toolresult.Content
					}
				} else {
					// 后续 tool result：拆为独立消息
					extraToolResults = append(extraToolResults, openaiMessage{
						Role:       string(m.Role),
						ToolCallID: cb.Toolresult.ToolCallId,
						Content:    cb.Toolresult.Content,
					})
				}
			}
		}
		out = append(out, om)
		out = append(out, extraToolResults...)
	}
	return out
}

// --- OpenAI API 类型 ---

type openaiRequest struct {
	Model         string               `json:"model"`
	Messages      []openaiMessage      `json:"messages"`
	Stream        bool                 `json:"stream,omitempty"`
	StreamOptions *openaiStreamOptions `json:"stream_options,omitempty"`
	Temperature   *float32             `json:"temperature,omitempty"`
	MaxTokens     *int                 `json:"max_tokens,omitempty"`
	TopP          *float32             `json:"top_p,omitempty"`
	Stop          []string             `json:"stop,omitempty"`
	Tools         []openaiTool         `json:"tools,omitempty"`
	ToolChoice    string               `json:"tool_choice,omitempty"`
	// ResponseFormat 结构化输出路径 1（json_object / json_schema）。nil 不发（自由文本）。
	ResponseFormat *openaiResponseFormat `json:"response_format,omitempty"`

	// extraBody 是构造期 c.extraBody 与 per-request model.Options.ExtraBody 的合并结果
	// （buildRequest 算好，同 key per-request 赢）。unexported → 不进 json.Marshal；由
	// newHTTPRequest 在 marshal 后顶层 merge（见 mergeExtraFields）。
	extraBody map[string]any
}

// openaiStreamOptions 控制 stream 模式额外行为. IncludeUsage=true 让 OpenAI/deepseek
// 在 stream 末尾多返一个 choices 空 + usage 非空的 chunk (token 计费数据).
// 不启时 stream 全程不返 usage → 业务 token/cost 观测失效 (Round 8 集成验证暴露).
type openaiStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// openaiResponseFormat 是 response_format wire 载荷（结构化输出路径 1）。
//   - {type:"json_object"}：约束合法 JSON（JSONSchema 为 nil）
//   - {type:"json_schema", json_schema:{name,strict,schema}}：按 schema 约束
type openaiResponseFormat struct {
	Type       string                          `json:"type"`
	JSONSchema *openaiResponseFormatJSONSchema `json:"json_schema,omitempty"`
}

// openaiResponseFormatJSONSchema 是 response_format json_schema 档的子载荷。
type openaiResponseFormatJSONSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict,omitempty"`
	Schema map[string]any `json:"schema,omitempty"`
}

type openaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content,omitempty"`
	// ReasoningContent thinking 模型的推理过程文本（reasoning_content）——响应解析进来、
	// 多轮回传时原样附回（DeepSeek/Qwen3 thinking 模式 wire 要求，见 ISSUE-89 /
	// schema.Message.ReasoningContent）。非 thinking 为空 omitempty 不发。
	ReasoningContent string           `json:"reasoning_content,omitempty"`
	ToolCalls        []openaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string           `json:"tool_call_id,omitempty"`
	Name             string           `json:"name,omitempty"`
	// Refusal json_schema strict 模式模型拒答时非空（结构化输出 §4.6）——响应解析进
	// Message.Refusal 供上层区分"拒答"vs"格式错"（当前 P1 ModeTool 路径不产 refusal，
	// response_format strict 路径 P2/P3 未接，暂无生产消费方）。请求侧不回传（omitempty 不发）。
	Refusal string `json:"refusal,omitempty"`
}

type openaiTool struct {
	Type     string         `json:"type"`
	Function openaiFunction `json:"function"`
}

type openaiFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type openaiToolCall struct {
	// Index OpenAI 流式 delta 用于关联多 tool_call 的 chunks——首 chunk 含 id+name+index，
	// 后续 chunks 仅含 args delta + index（无 id 无 name）。non-stream 响应不带 index。
	Index    *int                   `json:"index,omitempty"`
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Function openaiToolCallFunction `json:"function"`
}

type openaiToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openaiResponse struct {
	Choices []openaiChoice `json:"choices"`
	Usage   *openaiUsage   `json:"usage,omitempty"`
	Model   string         `json:"model,omitempty"` // provider 返的实际 model name
}

type openaiChoice struct {
	Message      openaiMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type openaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
	// PromptTokensDetails OpenAI prompt cache 命中 — Cached token 半价计费业务降本核心.
	// qwen / deepseek 也兼容此字段 (provider 命中 cache 时填). 不命中时为 nil 或 Cached=0.
	PromptTokensDetails *openaiPromptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

// openaiPromptTokensDetails OpenAI usage.prompt_tokens_details 子结构.
//   - CachedTokens: PromptTokens 中命中 prompt cache 的 token 数 (半价计费)
//
// audio_tokens 等其他字段我们暂不解析 (业务真用到再加).
type openaiPromptTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

type openaiStreamChunk struct {
	Choices []openaiStreamChoice `json:"choices"`
	// Usage Round 8 集成验证 fix: include_usage=true 时 OpenAI/deepseek 在 stream 末尾
	// 多返一个 choices 空 + usage 非空的 chunk. 解析它填到聚合 message 的 Metadata.Usage.
	Usage *openaiUsage `json:"usage,omitempty"`
	Model string       `json:"model,omitempty"` // provider 返的实际 model name (每 chunk 一致)
	// Error 是错误帧里的 error 字段（流中途出错时 provider 发 `data: {"error": ...}`）；
	// 按 isErrorValue 判断是不是错误。
	Error json.RawMessage `json:"error,omitempty"`
}

type openaiStreamChoice struct {
	Delta        openaiMessage `json:"delta"`
	FinishReason string        `json:"finish_reason"`
}
