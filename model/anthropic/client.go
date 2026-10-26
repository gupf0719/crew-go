// Package anthropic 提供 Anthropic Claude 的 model.ChatModel 实现，底层是官方 Go SDK
// github.com/anthropics/anthropic-sdk-go。请求走 Beta Messages API：thinking 的前缀绑定控制
// （block_binding）、服务端回退（fallbacks）、display=updates 这几个参数只在 SDK 的 beta 类型里有。
//
// 默认模型 [DefaultModel]（claude-opus-5-5）。跟 model/openai 的根本差异：
//
//   - assistant 消息原样回放。thinking 块带签名，签名绑定它产生时的前缀（system、tools、之前的每条
//     消息），回放时 API 重算比对，对不上的块按 [PrefixMismatch] 处理（默认丢掉）。crew-go 的 Message
//     把文本拼成一个 Content、工具调用单列，从它重建 assistant 消息会改掉块的顺序与边界。所以本包把
//     API 返回的 content 按请求形状存进 [AssistantContentAnnotation]，下一轮原样回放；这条消息的文本
//     或工具调用被改过（指纹对不上）时按快照原来的块序合并现在的内容。
//   - 不强制选工具。Opus 5.5 对 tool_choice 的 any / tool 返回 400，schema.ToolChoiceRequired 降为
//     不指定（等于 auto）；[Client.StructuredCapability] 据此报 SupportsToolChoiceRequired=false。
//   - 拒答是 HTTP 200。stop_reason=refusal 转成 [RefusalError]（包着 model.ErrContentFiltered，不可
//     重试）；默认请求服务端回退（fallbacks: "default"），被安全分类器拦下的请求多数由 Anthropic 指定的
//     回退模型接着答。开着回退也可能直接拿到拒答：有的类别不回退（如 reasoning_extraction）、回退模型
//     限流或过载时不执行回退、回退模型也拒绝。
//   - thinking 关不掉。Opus 5.5 只接受 adaptive thinking，想得多深用 [WithEffort] 控制。采样参数
//     （model.WithTemperature / WithTopP）原样转发：Opus 4.7 起的 Opus 模型只要带上就返回 400，
//     Fable 5.1 / Haiku 5.5 只接受默认值（都不重试）。
//   - max_tokens 截在工具调用中间时，那个调用带着解析不了的参数交出（流式是收到的原文，非流式是去掉
//     末尾 } 的 input），框架按「参数非法」拒掉、把错误交回模型，不会拿残缺的参数执行。
package anthropic

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"crew-go/model"
	"crew-go/schema"
)

const (
	// DefaultModel 是 NewClient 不指定模型时用的模型。
	DefaultModel = "claude-opus-5-5"
	// DefaultMaxTokens 是调用不指定 model.WithMaxTokens 时发的 max_tokens（Anthropic 要求必填）。
	// thinking 也计入它；64K 是官方给长 agentic 轮次的建议起点（Opus 5.5 输出上限 128K）。
	DefaultMaxTokens = 64000
	// ProviderName 是本包填进 Message.Metadata.System 与 model.APIError.Provider 的 provider 标识。
	ProviderName = "anthropic"
	// StrictToolExtraKey 是 schema.ToolInfo.Extra 里的键：值为 true 的工具带 strict: true 发出，
	// 模型给的参数严格符合 schema。schema 要满足 Anthropic strict 的约束（每个 object 都
	// additionalProperties: false 等），不满足时请求 400；本包不改写 schema。
	StrictToolExtraKey = "strict"
)

// Effort 是 output_config.effort 的取值，决定模型想得多深（也就决定了时延与花费）。
// 不设时用 API 的默认（Opus 5.5 是 medium）。
type Effort string

// effort 的五档。
const (
	EffortLow    Effort = "low"
	EffortMedium Effort = "medium"
	EffortHigh   Effort = "high"
	EffortXHigh  Effort = "xhigh"
	EffortMax    Effort = "max"
)

// ThinkingDisplay 是 thinking.display 的取值：thinking 块里返回什么文本。不设时用 API 的默认
// （Opus 5.5 是 omitted）。返回的文本流式时以 ReasoningContent 片段交出，Generate 时拼进
// Message.ReasoningContent。
type ThinkingDisplay string

const (
	// DisplayOmitted：thinking 块的文本为空，推理只留在签名里。
	DisplayOmitted ThinkingDisplay = "omitted"
	// DisplaySummarized：返回推理摘要，工具调用之间的进度说明也在其中。
	DisplaySummarized ThinkingDisplay = "summarized"
	// DisplayUpdates：只返回工具调用之间进度说明的摘要（beta，本包自动带请求头）。
	DisplayUpdates ThinkingDisplay = "updates"
)

// PrefixMismatch 是 thinking.block_binding.prefix_mismatch_behavior 的取值：回放的 thinking 块跟它
// 产生时的前缀对不上（更早的消息、system、tools 被改过）时 API 怎么办。
type PrefixMismatch string

const (
	// PrefixMismatchDropBlock 让 API 丢掉对不上的块，请求照常进行（默认）。代价比看上去大：API 丢的是
	// 第一个对不上的块**以及它之后的每一个 thinking 块**（直到下一个 compaction 块），而且之后每次请求
	// 都会再丢一遍——历史在某处被改过之后，这场会话余下的部分基本没有回放的推理可用，模型可能多想一些
	// 补回来。丢弃记录在 [InputTransformationsAnnotation] 里。
	PrefixMismatchDropBlock PrefixMismatch = "drop_block"
	// PrefixMismatchError 让请求返回 400。错误是确定性的，同一份历史重试还是 400。
	PrefixMismatchError PrefixMismatch = "error"
	// PrefixMismatchUnset 不发这个字段，只带 beta 请求头。2026-08-31 之前创建的账号默认不强制
	// 校验，这时 API 只把对不上的块记成 thinking_mismatch_allowed，不改变行为；之后创建的账号
	// 默认强制校验，对不上就 400。
	PrefixMismatchUnset PrefixMismatch = ""
)

// CacheTTL 是自动 prompt cache 断点的存活时间。
type CacheTTL string

const (
	// CacheTTL5m 是 5 分钟（API 默认）。
	CacheTTL5m CacheTTL = "5m"
	// CacheTTL1h 是 1 小时，写入价更高，适合两次调用间隔常超过 5 分钟的会话。
	CacheTTL1h CacheTTL = "1h"
)

// Client 用 Anthropic Messages API 实现 model.ChatModel。构造后只读，可并发使用。
type Client struct {
	sdk       anthropic.Client
	model     string
	maxTokens int64
	tools     []*schema.ToolInfo

	effort         Effort
	display        ThinkingDisplay
	prefixMismatch PrefixMismatch
	// thinkingConfig 为 false 时不发 thinking 字段（[WithoutThinkingConfig]）。
	thinkingConfig bool
	fallbacks      bool
	caching        bool
	cacheTTL       CacheTTL
	compaction     *Compaction // nil = 不开服务端压缩
	betas          []anthropic.AnthropicBeta
	extraBody      map[string]any
	requestTimeout time.Duration

	// sdkOptions 只在构造期收集（WithBaseURL 等），NewClient 用完即清空。
	sdkOptions []option.RequestOption
}

var (
	_ model.ChatModel         = (*Client)(nil)
	_ model.StructuredCapable = (*Client)(nil)
)

// ClientOption 配置 [Client]。
type ClientOption func(*Client)

// NewClient 创建 Claude ChatModel。apiKey 为空时由 SDK 从环境变量取凭证（ANTHROPIC_API_KEY 或
// ANTHROPIC_AUTH_TOKEN）；modelName 为空时用 [DefaultModel]。
//
// 默认：SDK 不自己重试（重试与故障切换交给 crew-go 的重试层，两层都重试会把次数乘起来）、不发 SDK
// 自己的 OpenTelemetry span（模型调用的观测由 crew-go 的 hook 层出，两边都发会重复计数）；
// 要改用 [WithRequestOptions] 传 SDK 选项覆盖。
func NewClient(apiKey, modelName string, opts ...ClientOption) *Client {
	c := &Client{
		model:          modelName,
		maxTokens:      DefaultMaxTokens,
		prefixMismatch: PrefixMismatchDropBlock,
		thinkingConfig: true,
		fallbacks:      true,
		caching:        true,
	}
	if c.model == "" {
		c.model = DefaultModel
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	base := []option.RequestOption{option.WithMaxRetries(0), option.WithoutOpenTelemetry()}
	if apiKey != "" {
		// 环境里另有 ANTHROPIC_AUTH_TOKEN 时 SDK 会先把它设成 Authorization 头；两个凭证头同时
		// 出现 API 直接拒绝，显式传了 key 就以它为准。
		base = append(base, option.WithAPIKey(apiKey), option.WithHeaderDel("Authorization"))
	}
	c.sdk = anthropic.NewClient(append(base, c.sdkOptions...)...)
	c.sdkOptions = nil
	return c
}

// WithBaseURL 设 API 地址（默认 https://api.anthropic.com/，或环境变量 ANTHROPIC_BASE_URL）。
func WithBaseURL(url string) ClientOption {
	return func(c *Client) { c.sdkOptions = append(c.sdkOptions, option.WithBaseURL(url)) }
}

// WithHTTPClient 设底层 HTTP 客户端。
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.sdkOptions = append(c.sdkOptions, option.WithHTTPClient(hc)) }
}

// WithRequestOptions 透传 SDK 的请求选项，排在本包默认选项之后，同一项以这里为准（例如
// option.WithMaxRetries(2) 打开 SDK 自己的重试）。
func WithRequestOptions(opts ...option.RequestOption) ClientOption {
	return func(c *Client) { c.sdkOptions = append(c.sdkOptions, opts...) }
}

// WithMaxTokens 设默认的 max_tokens（不设时 [DefaultMaxTokens]），按次用 model.WithMaxTokens 覆盖。
// n <= 0 时不改。
func WithMaxTokens(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.maxTokens = int64(n)
		}
	}
}

// WithEffort 设默认的 output_config.effort。按次改用 [WithEffortOption]；注意按次改顶层 effort 会让
// 这次请求的 prompt cache 失效。
func WithEffort(e Effort) ClientOption {
	return func(c *Client) { c.effort = e }
}

// WithThinkingDisplay 设 thinking.display。[DisplayUpdates] 需要的 beta 请求头由本包自动带上。
func WithThinkingDisplay(d ThinkingDisplay) ClientOption {
	return func(c *Client) { c.display = d }
}

// WithPrefixMismatch 设回放的 thinking 块对不上前缀时的处理（默认 [PrefixMismatchDropBlock]）。
func WithPrefixMismatch(p PrefixMismatch) ClientOption {
	return func(c *Client) { c.prefixMismatch = p }
}

// WithoutThinkingConfig 不发 thinking 字段（连同 display 与 block_binding），给不接受 adaptive
// thinking 的旧模型用。Opus 5.5 不需要它：那里不发 thinking 也是 adaptive。
func WithoutThinkingConfig() ClientOption {
	return func(c *Client) { c.thinkingConfig = false }
}

// WithFallbacks 开关服务端回退（默认开：fallbacks: "default"，beta server-side-fallback-2026-07-01）。
//
// 开着时被安全分类器拒绝的请求在同一次调用里由回退模型接着答；回退模型读不到原模型的 thinking 块
// （API 自动丢弃，记在 [InputTransformationsAnnotation]），回退经过记在 [FallbackAnnotation]。
// Amazon Bedrock、Vertex AI、Microsoft Foundry 等不支持这个参数的端点要关掉。
//
// 要指定回退模型（数组形式）时，先 WithFallbacks(false)，再经 [WithExtraBody] 传
// {"fallbacks": [{"model": ...}]} 并 [WithBetas]("server-side-fallback-2026-06-01")——数组形式与
// "default" 用的 beta 头不同，配错是 400。
func WithFallbacks(enabled bool) ClientOption {
	return func(c *Client) { c.fallbacks = enabled }
}

// WithPromptCaching 开关顶层自动 prompt cache（默认开：cache_control 自动放在最后一个可缓存块上）。
func WithPromptCaching(enabled bool) ClientOption {
	return func(c *Client) { c.caching = enabled }
}

// WithCacheTTL 设自动 cache 断点的存活时间（默认 API 的 5 分钟）。
func WithCacheTTL(ttl CacheTTL) ClientOption {
	return func(c *Client) { c.cacheTTL = ttl }
}

// Compaction 配服务端上下文压缩，见 [WithCompaction]。
type Compaction struct {
	// TriggerTokens 是输入达到多少 token 时压缩；<= 0 用 API 的默认（150000）。
	TriggerTokens int64
	// Instructions 是给摘要的补充要求（API 的 instructions 字段）；空串不发。按 Anthropic 迁移指南，带了它
	// 摘要只看对话正文、看不到之前的 thinking，所以要写清摘要必须保留什么。
	Instructions string
}

// WithCompaction 开服务端上下文压缩（beta compact-2026-01-12）：每次请求带 context_management 的
// compact_20260112，输入达到阈值时 API 先把更早的内容摘要成一个 compaction 块、再接着答。这个块随
// 回放快照（[AssistantContentAnnotation]）原样发回，API 据它把块之前的历史换成摘要——调用方的历史只追加、
// 不用改，preserved thinking 的前缀校验也不把它当改动（压缩之后从 compaction 块算起）。
//
// 代价：压缩让 prompt cache 从头开始；压缩之前的 thinking 不会带到之后。摘要那一步的用量不在顶层 usage
// 里（顶层只算模型答这一轮），记在 [CompactionAnnotation]。消息照常全发，本包不替调用方删历史。
//
// 不支持 pause_after_compaction（那会让一次调用只返回 compaction 块、stop_reason=compaction）。
func WithCompaction(cfg Compaction) ClientOption {
	return func(c *Client) { c.compaction = &cfg }
}

// WithBetas 追加 anthropic-beta 请求头的取值（本包自己需要的会自动带上，不用写）。
func WithBetas(betas ...string) ClientOption {
	return func(c *Client) {
		for _, b := range betas {
			c.betas = append(c.betas, anthropic.AnthropicBeta(b))
		}
	}
}

// WithExtraBody 设构造期的请求体扩展字段。多次调用累积，同一个顶层键后值覆盖前值。
//
// 跟 model/openai 的顶层覆盖不同，这里按 JSON merge patch（RFC 7386）并进请求体：对象逐键递归
// 合并，其他值整体替换，null 删除该键。所以 {"output_config": {"effort": "high"}} 只改 effort、
// 不碰 output_config 里的其他字段；{"fallbacks": nil} 删掉 fallbacks。按次的
// model.WithExtraBody 在它之后再合并一遍。
func WithExtraBody(fields map[string]any) ClientOption {
	return func(c *Client) {
		if c.extraBody == nil {
			c.extraBody = make(map[string]any, len(fields))
		}
		maps.Copy(c.extraBody, fields)
	}
}

// WithRequestTimeout 设 Generate 单次请求的超时。不设时按 max_tokens 估算（每 128K token 一小时，
// 至少 10 分钟、至多 1 小时）。Stream 不受它限制，靠调用方的 ctx。
func WithRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.requestTimeout = d }
}

// WithEffortOption 是 [WithEffort] 的按次版本（model.Option），经 agent_v2.WithRunModelOptions 透传。
// 走 model.WithExtraBody 的 output_config 键：跟其他按次设置 output_config 的选项同时用时，按
// model.WithExtraBody 的规则后者整体覆盖前者。按次改顶层 effort 会让这次请求的 prompt cache 失效。
func WithEffortOption(e Effort) model.Option {
	return model.WithExtraBody(map[string]any{"output_config": map[string]any{"effort": string(e)}})
}

// ModelName 返回构造时配的模型名（满足 model.ModelNamer；按次的 model.WithModel 覆盖它）。
func (c *Client) ModelName() string { return c.model }

// WithTools 返回绑定了 tools 的新 Client（原 Client 不变）。
func (c *Client) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *c
	cp.tools = append([]*schema.ToolInfo(nil), tools...)
	return &cp, nil
}

// StructuredCapability 自报结构化输出能力：支持原生 json_schema（output_config.format）与 strict
// 工具；不支持 json_object；不支持 tool_choice=required（Opus 5.5 / Sonnet 5.5 / Fable 5.1 对强制
// 选工具返回 400，本包把它降为不指定）。
func (c *Client) StructuredCapability(string) model.StructuredCapability {
	return model.StructuredCapability{
		SupportsJSONSchema:         true,
		SupportsJSONObject:         false,
		SupportsToolChoiceRequired: false,
		SupportsStrictTool:         true,
		DefaultMode:                model.ModeTool,
	}
}

// Generate 非流式调用。
func (c *Client) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	req, err := c.buildRequest(ctx, messages, model.Apply(opts...))
	if err != nil {
		return nil, err
	}
	reqOpts := append(req.opts, option.WithRequestTimeout(c.attemptTimeout(req.params.MaxTokens)))
	resp, err := c.sdk.Beta.Messages.New(ctx, req.params, reqOpts...)
	if err != nil {
		return nil, c.requestError(err)
	}
	if resp == nil {
		return nil, &model.APIError{Provider: ProviderName, Err: errors.New("empty response")}
	}
	turn, err := c.interpret(ctx, resp, nil)
	if err != nil {
		return nil, err
	}
	return &schema.Message{
		Role:             schema.RoleAssistant,
		Content:          turn.text,
		ReasoningContent: turn.reasoning,
		ContentBlocks:    turn.calls,
		Annotations:      turn.annotations,
		Metadata:         turn.metadata,
	}, nil
}

// Stream 流式调用。
//
// 文本与 thinking 文本按到达逐片交出（Content / ReasoningContent 片段）；工具调用、用量、结束原因与
// 回放标注在流正常结束后一次交出（最后一个片段）。工具调用压到最后，是因为服务端回退在中途切换模型时，
// 切换点之前的工具调用要作废（见 [FallbackAnnotation]），提前交出就收不回来了。
//
// 只有收到 message_stop 且结束原因不是拒答 / 超窗口才算完整；读错、ctx 取消、错误帧、没收到
// message_stop 就断开，都作为流的错误交出，不当正常结束。
func (c *Client) Stream(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	req, err := c.buildRequest(ctx, messages, model.Apply(opts...))
	if err != nil {
		return nil, err
	}
	// 读响应体用一个可单独取消的 ctx：消费方 Close 时取消它，正阻塞在读响应体上的 goroutine 立刻
	// 返回，连接随之释放。
	streamCtx, cancel := context.WithCancel(ctx)
	stream := c.sdk.Beta.Messages.NewStreaming(streamCtx, req.params, req.opts...)
	// SDK 在 NewStreaming 里就发出了请求：HTTP 层的错误（4xx / 5xx / 网络）此刻已在 Err 上，
	// 同步返回给调用方，让重试层按状态码与 Retry-After 处理。
	if err := stream.Err(); err != nil {
		_ = stream.Close() // 请求已失败，关闭的错误没有下文
		cancel()
		return nil, c.requestError(err)
	}
	sr, send := schema.NewStreamReader[*schema.Message](8)
	go c.consumeStream(ctx, cancel, stream, sr, send)
	return sr, nil
}

// attemptTimeout 是 Generate 单次请求的超时。SDK 对非流式请求有一道「预计超过 10 分钟就要求改用
// 流式」的闸（max_tokens 超过约 21K 就触发），显式给超时会绕过它；这里沿用它的估算口径但不报错。
func (c *Client) attemptTimeout(maxTokens int64) time.Duration {
	if c.requestTimeout > 0 {
		return c.requestTimeout
	}
	d := time.Duration(float64(time.Hour) * float64(maxTokens) / 128000)
	return min(max(d, 10*time.Minute), time.Hour)
}

// requestError 把发请求阶段的错误转成 *model.APIError：HTTP 错误带状态码、响应体与 Retry-After，
// 其余（网络、ctx 取消）放进 Err，errors.Is 照样认得出 context.Canceled。
func (c *Client) requestError(err error) error {
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode != 0 {
		var retryAfter time.Duration
		if apiErr.Response != nil {
			retryAfter = model.ParseRetryAfter(apiErr.Response.Header, time.Now())
		}
		return &model.APIError{
			Provider:   ProviderName,
			StatusCode: apiErr.StatusCode,
			Body:       apiErr.RawJSON(),
			RetryAfter: retryAfter,
			Err:        err,
		}
	}
	return &model.APIError{Provider: ProviderName, Err: fmt.Errorf("request failed: %w", err)}
}

// streamError 把建流之后的错误包成 *model.APIError（StatusCode 为 0：HTTP 层已经成功，这不是
// HTTP 错误；SDK 给流中错误帧填的是建流时的 200，不能当状态码用）。
func (c *Client) streamError(err error) error {
	return &model.APIError{Provider: ProviderName, Err: err}
}

// logKey* 是本包结构化日志的字段名。
const (
	logKeyProvider   = "provider"
	logKeyStopReason = "stop_reason"
	logKeyErr        = "err"
	logKeyType       = "type"
	logKeyPath       = "path"
	logKeyReason     = "reason"
	logKeyCount      = "count"
	logKeyTool       = "tool"
)

// warn 记一条带 provider 字段的告警。
func warn(ctx context.Context, msg string, args ...any) {
	slog.WarnContext(ctx, msg, append([]any{logKeyProvider, ProviderName}, args...)...)
}
