package anthropic

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	"github.com/anthropics/anthropic-sdk-go/shared/constant"

	"crew-go/model"
	"crew-go/schema"
)

// wireRequest 是一次调用要发的东西：SDK 参数，加上这次调用专属的请求选项（合并了 ExtraBody 的
// 请求体）。
type wireRequest struct {
	params anthropic.BetaMessageNewParams
	opts   []option.RequestOption
}

// buildRequest 把 crew-go 的消息与选项转成一次 Messages API 请求。
//
// 输出必须是输入的纯函数：同一段历史每次转出同样的内容。thinking 签名与 prompt cache 都按前缀比对，
// 转换里有任何随调用变化的成分（map 遍历顺序、时间戳……），更早的消息在下一轮就「变了」。
func (c *Client) buildRequest(ctx context.Context, messages []*schema.Message, o *model.Options) (*wireRequest, error) {
	modelName := c.model
	if o.Model != nil && *o.Model != "" {
		modelName = *o.Model
	}
	maxTokens := c.maxTokens
	if o.MaxTokens != nil && *o.MaxTokens > 0 {
		maxTokens = int64(*o.MaxTokens)
	}

	system, msgs, err := convertMessages(ctx, messages)
	if err != nil {
		return nil, err
	}
	params := anthropic.BetaMessageNewParams{
		Model:     anthropic.Model(modelName),
		MaxTokens: maxTokens,
		Messages:  msgs,
		System:    system,
	}

	tools, err := c.wireTools(o)
	if err != nil {
		return nil, err
	}
	if len(tools) > 0 {
		params.Tools = tools
	}
	// tool_choice 只在真有工具时发（零工具配 tool_choice 是 400）。「有没有工具」看合并 ExtraBody
	// 之后的请求体：ExtraBody 带了 tools 时整个替换掉上面这组。
	if choice, ok := toolChoice(o.ToolChoice); ok && wireHasTools(len(tools), c.extraBody, o.ExtraBody) {
		params.ToolChoice = choice
	}

	var betas []anthropic.AnthropicBeta
	if c.thinkingConfig {
		adaptive := anthropic.BetaThinkingConfigAdaptiveParam{}
		if c.display != "" {
			adaptive.Display = anthropic.BetaThinkingConfigAdaptiveDisplay(c.display)
		}
		if c.prefixMismatch != PrefixMismatchUnset {
			adaptive.BlockBinding = anthropic.BetaThinkingBlockBindingParam{
				PrefixMismatchBehavior: anthropic.BetaThinkingPrefixMismatchBehavior(c.prefixMismatch),
			}
		}
		params.Thinking = anthropic.BetaThinkingConfigParamUnion{OfAdaptive: &adaptive}
		// 不设字段也带这个头：未强制校验的账号据此在 input_transformations 里记下对不上的块。
		betas = append(betas, anthropic.AnthropicBetaThinkingBindingControls2026_08_01)
		if c.display == DisplayUpdates {
			betas = append(betas, anthropic.AnthropicBetaThinkingDisplayUpdates2026_08_18)
		}
	}
	if c.effort != "" {
		params.OutputConfig.Effort = anthropic.BetaOutputConfigEffort(c.effort)
	}
	if rf := o.ResponseFormat; rf != nil {
		switch rf.Type {
		case "", "text":
		case "json_schema":
			if rf.JSONSchema == nil || rf.JSONSchema.Schema == nil {
				return nil, errors.New("anthropic: response_format json_schema has no schema")
			}
			params.OutputConfig.Format = anthropic.BetaJSONOutputFormatParam{Schema: rf.JSONSchema.Schema}
		default:
			// json_object 没有对应的原生机制；照常发出去会得到不受约束的文本，所以明确报错
			// （StructuredCapability 已报 SupportsJSONObject=false）。
			return nil, fmt.Errorf("anthropic: response_format %q is not supported, use json_schema", rf.Type)
		}
	}
	if c.caching {
		cc := anthropic.NewBetaCacheControlEphemeralParam()
		if c.cacheTTL != "" {
			cc.TTL = anthropic.BetaCacheControlEphemeralTTL(c.cacheTTL)
		}
		params.CacheControl = cc
	}
	if cp := c.compaction; cp != nil {
		edit := anthropic.BetaCompact20260112EditParam{}
		if cp.TriggerTokens > 0 {
			edit.Trigger = anthropic.BetaInputTokensTriggerParam{Value: cp.TriggerTokens}
		}
		if cp.Instructions != "" {
			edit.Instructions = param.NewOpt(cp.Instructions)
		}
		params.ContextManagement = anthropic.BetaContextManagementConfigParam{
			Edits: []anthropic.BetaContextManagementConfigEditUnionParam{{OfCompact20260112: &edit}},
		}
		betas = append(betas, anthropic.AnthropicBetaCompact2026_01_12)
	}
	if c.fallbacks {
		params.Fallbacks = anthropic.BetaFallbacksParamUnion{OfDefault: constant.ValueOf[constant.Default]()}
		betas = append(betas, anthropic.AnthropicBetaServerSideFallback2026_07_01)
	}
	if len(o.Stop) > 0 {
		params.StopSequences = o.Stop
	}
	if o.Temperature != nil {
		params.Temperature = param.NewOpt(float64(*o.Temperature))
	}
	if o.TopP != nil {
		params.TopP = param.NewOpt(float64(*o.TopP))
	}
	params.Betas = dedupeBetas(append(betas, c.betas...))

	req := &wireRequest{params: params}
	if len(c.extraBody) > 0 || len(o.ExtraBody) > 0 {
		body, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("anthropic: encode request: %w", err)
		}
		body, err = applyMergePatch(body, c.extraBody, o.ExtraBody)
		if err != nil {
			return nil, fmt.Errorf("anthropic: merge extra body: %w", err)
		}
		req.opts = append(req.opts, option.WithRequestBody("application/json", body))
	}
	return req, nil
}

// convertMessages 把消息列表拆成顶层 system 与 messages。
//
//   - 开头连续的 system 消息 → 顶层 system 文本块（crew-go 的前言：AGENTS.md、提示词、长期记忆）。
//   - 其余 system 消息 → user 文本，包在 <system-reminder> 里。原生的 mid-conversation
//     role:"system" 有位置约束（必须跟在 user 之后，且是末条或后接 assistant），crew-go 插入的位置
//     不保证满足；而按位置在两种写法间切换的话，同一条消息会随后面追加的内容改变写法——那是对历史的
//     改写。一律转成 user 文本，写法只取决于消息本身。
//   - 同角色相邻的消息合并成一条（role=tool 与 user 都映射成 user）；user 消息里 tool_result 块
//     排在最前（API 要求 tool_result 紧跟 tool_use），其余内容按原序排在后面。
//
// 内容为空的消息跳过：API 不接受空的 content。
func convertMessages(ctx context.Context, in []*schema.Message) ([]anthropic.BetaTextBlockParam, []anthropic.BetaMessageParam, error) {
	var system []anthropic.BetaTextBlockParam
	i := 0
	for ; i < len(in); i++ {
		m := in[i]
		if m == nil {
			continue
		}
		if m.Role != schema.RoleSystem {
			break
		}
		if text := messageText(m); strings.TrimSpace(text) != "" {
			system = append(system, anthropic.BetaTextBlockParam{Text: text})
		}
	}

	var out []anthropic.BetaMessageParam
	for ; i < len(in); i++ {
		m := in[i]
		if m == nil {
			continue
		}
		role, blocks, err := convertMessage(ctx, m)
		if err != nil {
			return nil, nil, fmt.Errorf("anthropic: message %d (%s): %w", i, m.Role, err)
		}
		if len(blocks) == 0 {
			continue
		}
		if n := len(out); n > 0 && out[n-1].Role == role {
			out[n-1].Content = append(out[n-1].Content, blocks...)
			continue
		}
		out = append(out, anthropic.BetaMessageParam{Role: role, Content: blocks})
	}
	for k := range out {
		if out[k].Role == anthropic.BetaMessageParamRoleUser {
			out[k].Content = toolResultsFirst(out[k].Content)
		}
	}
	return system, out, nil
}

// convertMessage 转一条非前言的消息，返回它在 API 里的角色与内容块。
func convertMessage(ctx context.Context, m *schema.Message) (anthropic.BetaMessageParamRole, []anthropic.BetaContentBlockParamUnion, error) {
	switch m.Role {
	case schema.RoleUser:
		blocks, err := userBlocks(m)
		return anthropic.BetaMessageParamRoleUser, blocks, err
	case schema.RoleTool:
		blocks, err := toolResultBlocks(m)
		return anthropic.BetaMessageParamRoleUser, blocks, err
	case schema.RoleSystem:
		var blocks []anthropic.BetaContentBlockParamUnion
		if text := messageText(m); strings.TrimSpace(text) != "" {
			blocks = append(blocks, textBlock("<system-reminder>\n"+text+"\n</system-reminder>"))
		}
		return anthropic.BetaMessageParamRoleUser, blocks, nil
	case schema.RoleAssistant:
		return anthropic.BetaMessageParamRoleAssistant, assistantBlocks(ctx, m), nil
	default:
		return "", nil, fmt.Errorf("unsupported role %q", m.Role)
	}
}

// messageText 取消息的文本：Content，为空时退到文本块按序拼接（以换行分隔）。
func messageText(m *schema.Message) string {
	if m.Content != "" {
		return m.Content
	}
	var parts []string
	for _, cb := range m.ContentBlocks {
		if cb != nil && cb.Text != nil && cb.Text.Text != "" {
			parts = append(parts, cb.Text.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// userBlocks 转 user 消息：Content 在前，内容块按原序在后。
func userBlocks(m *schema.Message) ([]anthropic.BetaContentBlockParamUnion, error) {
	var blocks []anthropic.BetaContentBlockParamUnion
	if strings.TrimSpace(m.Content) != "" {
		blocks = append(blocks, textBlock(m.Content))
	}
	for i, cb := range m.ContentBlocks {
		if cb == nil {
			continue
		}
		b, ok, err := userContentBlock(cb)
		if err != nil {
			return nil, fmt.Errorf("content block %d: %w", i, err)
		}
		if ok {
			blocks = append(blocks, b)
		}
	}
	return blocks, nil
}

// userContentBlock 转 user 消息里的一个内容块。ok 为 false 表示这个块不产生 API 内容（空文本）。
// 认不出、或 Anthropic 不支持的块报错，不静默丢掉用户给的内容。
func userContentBlock(cb *schema.ContentBlock) (anthropic.BetaContentBlockParamUnion, bool, error) {
	var zero anthropic.BetaContentBlockParamUnion
	switch {
	case cb.Text != nil:
		if strings.TrimSpace(cb.Text.Text) == "" {
			return zero, false, nil
		}
		return textBlock(cb.Text.Text), true, nil
	case cb.ImageUrl != nil && cb.ImageUrl.ImageUrl != nil:
		b, err := imageBlock(cb.ImageUrl.ImageUrl.Url, cb.MediaType)
		return b, err == nil, err
	case cb.File != nil && cb.File.File != nil:
		b, err := fileBlock(cb.File.File, cb.MediaType)
		return b, err == nil, err
	case cb.Data != nil:
		raw, err := json.Marshal(cb.Data.Data)
		if err != nil {
			return zero, false, fmt.Errorf("encode data block: %w", err)
		}
		return textBlock(string(raw)), true, nil
	case cb.Toolresult != nil:
		return toolResultBlock(cb.Toolresult), true, nil
	case cb.InputAudio != nil:
		return zero, false, errors.New("input_audio blocks are not supported by the Messages API")
	default:
		return zero, false, fmt.Errorf("unsupported content block type %q", cb.Type)
	}
}

// imageBlock 转图片：data URL 发 base64，http(s) URL 发 url 源。
func imageBlock(url, mediaType string) (anthropic.BetaContentBlockParamUnion, error) {
	var zero anthropic.BetaContentBlockParamUnion
	if rest, ok := strings.CutPrefix(url, "data:"); ok {
		meta, data, ok := strings.Cut(rest, ",")
		mt, isBase64 := strings.CutSuffix(meta, ";base64")
		if !ok || !isBase64 {
			return zero, errors.New("image data URL must be base64 encoded")
		}
		if mt == "" {
			mt = mediaType
		}
		return anthropic.BetaContentBlockParamUnion{OfImage: &anthropic.BetaImageBlockParam{
			Source: anthropic.BetaImageBlockParamSourceUnion{OfBase64: &anthropic.BetaBase64ImageSourceParam{
				Data:      data,
				MediaType: anthropic.BetaBase64ImageSourceMediaType(mt),
			}},
		}}, nil
	}
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return anthropic.BetaContentBlockParamUnion{OfImage: &anthropic.BetaImageBlockParam{
			Source: anthropic.BetaImageBlockParamSourceUnion{OfURL: &anthropic.BetaURLImageSourceParam{URL: url}},
		}}, nil
	}
	return zero, fmt.Errorf("unsupported image URL scheme in %q", clip(url))
}

// fileBlock 转文件：PDF → document（base64 或 url 源），纯文本 → document（文本源），图片 → image。
func fileBlock(f *schema.FileData, mediaType string) (anthropic.BetaContentBlockParamUnion, error) {
	var zero anthropic.BetaContentBlockParamUnion
	title := func(d *anthropic.BetaRequestDocumentBlockParam) anthropic.BetaContentBlockParamUnion {
		if f.Filename != "" {
			d.Title = param.NewOpt(f.Filename)
		}
		return anthropic.BetaContentBlockParamUnion{OfDocument: d}
	}
	switch {
	case mediaType == "application/pdf" && f.FileData != "":
		return title(&anthropic.BetaRequestDocumentBlockParam{Source: anthropic.BetaRequestDocumentBlockSourceUnionParam{
			OfBase64: &anthropic.BetaBase64PDFSourceParam{Data: f.FileData},
		}}), nil
	case mediaType == "application/pdf" && f.URL != "":
		return title(&anthropic.BetaRequestDocumentBlockParam{Source: anthropic.BetaRequestDocumentBlockSourceUnionParam{
			OfURL: &anthropic.BetaURLPDFSourceParam{URL: f.URL},
		}}), nil
	case mediaType == "text/plain" && f.FileData != "":
		text, err := base64.StdEncoding.DecodeString(f.FileData)
		if err != nil {
			return zero, fmt.Errorf("decode text/plain file: %w", err)
		}
		return title(&anthropic.BetaRequestDocumentBlockParam{Source: anthropic.BetaRequestDocumentBlockSourceUnionParam{
			OfText: &anthropic.BetaPlainTextSourceParam{Data: string(text)},
		}}), nil
	case strings.HasPrefix(mediaType, "image/") && f.FileData != "":
		return imageBlock("data:"+mediaType+";base64,"+f.FileData, mediaType)
	case strings.HasPrefix(mediaType, "image/") && f.URL != "":
		return imageBlock(f.URL, mediaType)
	default:
		return zero, fmt.Errorf("unsupported file (media type %q)", mediaType)
	}
}

// toolResultBlocks 转 role=tool 消息：每个 tool_result 块一个。一个都没有是调用方的错——没有
// tool_use_id 就配不上任何工具调用。
func toolResultBlocks(m *schema.Message) ([]anthropic.BetaContentBlockParamUnion, error) {
	var blocks []anthropic.BetaContentBlockParamUnion
	for _, cb := range m.ContentBlocks {
		if cb != nil && cb.Toolresult != nil {
			blocks = append(blocks, toolResultBlock(cb.Toolresult))
		}
	}
	if len(blocks) == 0 {
		return nil, errors.New("tool message has no tool_result block")
	}
	return blocks, nil
}

// toolResultBlock 转一个工具结果。空白内容不发 content（API 不接受空文本块）。
func toolResultBlock(tr *schema.ToolresultContentBlock) anthropic.BetaContentBlockParamUnion {
	p := &anthropic.BetaToolResultBlockParam{ToolUseID: tr.ToolCallId}
	if strings.TrimSpace(tr.Content) != "" {
		p.Content = []anthropic.BetaToolResultBlockParamContentUnion{{OfText: &anthropic.BetaTextBlockParam{Text: tr.Content}}}
	}
	if tr.IsError {
		p.IsError = param.NewOpt(true)
	}
	return anthropic.BetaContentBlockParamUnion{OfToolResult: p}
}

// assistantBlocks 转 assistant 消息：
//   - 有回放快照、领域字段（Content 与工具调用）没被改过：原样回放快照。
//   - 有快照但被改过（钩子改写、业务替换）：见 [mergeEdited]，按快照的块序合并现在的内容。
//   - 没有快照（别的 provider 产生的、业务构造的）：文本在前，工具调用随后。
func assistantBlocks(ctx context.Context, m *schema.Message) []anthropic.BetaContentBlockParamUnion {
	calls := schema.GetToolCalls(m)
	snap, hasSnap := decodeReplay(m)
	if hasSnap && snap.Digest == replayDigest(m.Content, calls) {
		blocks := make([]anthropic.BetaContentBlockParamUnion, 0, len(snap.Content))
		for _, raw := range snap.Content {
			blocks = append(blocks, param.Override[anthropic.BetaContentBlockParamUnion](raw))
		}
		return blocks
	}
	if hasSnap {
		slog.DebugContext(ctx, "anthropic: assistant message changed after it was returned, merged into its original block order",
			logKeyProvider, ProviderName)
		return mergeEdited(m, snap, calls)
	}
	var blocks []anthropic.BetaContentBlockParamUnion
	if strings.TrimSpace(m.Content) != "" {
		blocks = append(blocks, textBlock(m.Content))
	}
	for _, tc := range calls {
		blocks = append(blocks, toolUseBlock(tc))
	}
	return blocks
}

// mergeEdited 把被改过的 assistant 消息按快照原来的块序转出来，块的相对顺序不变：
//   - thinking、redacted_thinking 等其余块原位保留；
//   - 文本没改（文本指纹对得上）时文本块原样保留；改了时在第一个文本块的位置放现在的 Content、其余
//     文本块去掉（快照里没有文本块就放在新增的工具调用之前）；
//   - 工具调用按 ID 对：名字与参数都没变的保留原样，变了的原位换成现在的，现在没有的去掉，新增的放最后。
//
// 为什么不把 thinking 归拢到最前：每个 thinking 块绑定的前缀里有它前面的块。按原序合并时，排在第一处
// 改动之前的 thinking 块前缀不变、照样有效；排在改动之后的（比如两个工具调用之间的进度说明）前缀变了，
// 按 prefix_mismatch_behavior 处理。归拢的话，连排在文本之后、改动之前的 thinking 块前缀也变了。
func mergeEdited(m *schema.Message, snap replayEnvelope, calls []schema.ToolCall) []anthropic.BetaContentBlockParamUnion {
	textSame := snap.TextDigest != "" && snap.TextDigest == replayDigest(m.Content, nil)
	current := make(map[string]schema.ToolCall, len(calls))
	for _, tc := range calls {
		current[tc.ID] = tc
	}
	used := make(map[string]bool, len(calls))
	hasText := strings.TrimSpace(m.Content) != ""
	placedText := false
	out := make([]anthropic.BetaContentBlockParamUnion, 0, len(snap.Content)+1)
	for _, raw := range snap.Content {
		b := parseRawBlock(raw)
		switch b.Type {
		case "text":
			if textSame {
				out = append(out, param.Override[anthropic.BetaContentBlockParamUnion](raw))
				continue
			}
			if !placedText && hasText {
				out = append(out, textBlock(m.Content))
			}
			placedText = true
		case "tool_use":
			tc, ok := current[b.ID]
			if !ok || used[b.ID] {
				continue // 这个工具调用被去掉了
			}
			used[b.ID] = true
			if tc.Function.Name == b.Name && sameJSON(toolInput(tc.Function.Arguments), b.Input) {
				out = append(out, param.Override[anthropic.BetaContentBlockParamUnion](raw))
				continue
			}
			out = append(out, toolUseBlock(tc))
		default:
			out = append(out, param.Override[anthropic.BetaContentBlockParamUnion](raw))
		}
	}
	if !textSame && !placedText && hasText {
		out = append(out, textBlock(m.Content))
	}
	for _, tc := range calls {
		if !used[tc.ID] {
			out = append(out, toolUseBlock(tc))
		}
	}
	return out
}

// sameJSON 判断两段 JSON 去掉空白之后是否逐字节相同（不比较键序与数字写法，那些不同就当改过）。
func sameJSON(a, b json.RawMessage) bool {
	var ca, cb bytes.Buffer
	if json.Compact(&ca, a) != nil || json.Compact(&cb, b) != nil {
		return false
	}
	return bytes.Equal(ca.Bytes(), cb.Bytes())
}

// toolUseBlock 把一个工具调用转成 tool_use 块。
func toolUseBlock(tc schema.ToolCall) anthropic.BetaContentBlockParamUnion {
	return anthropic.BetaContentBlockParamUnion{OfToolUse: &anthropic.BetaToolUseBlockParam{
		ID:    tc.ID,
		Name:  tc.Function.Name,
		Input: toolInput(tc.Function.Arguments),
	}}
}

// toolInput 把工具调用参数转成 tool_use.input。input 必须是 JSON 对象：参数不是合法的 JSON 对象时
// （被 max_tokens 截断、来自别的 provider）发 {}——跟 SDK 累积流式响应时对截断参数的处理一致。
func toolInput(args string) json.RawMessage {
	trimmed := bytes.TrimSpace([]byte(args))
	if len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed) {
		return json.RawMessage(trimmed)
	}
	return json.RawMessage(`{}`)
}

// textBlock 构造一个文本块。
func textBlock(text string) anthropic.BetaContentBlockParamUnion {
	return anthropic.BetaContentBlockParamUnion{OfText: &anthropic.BetaTextBlockParam{Text: text}}
}

// toolResultsFirst 把 tool_result 块稳定地挪到最前，其余块保持原有相对顺序。
func toolResultsFirst(blocks []anthropic.BetaContentBlockParamUnion) []anthropic.BetaContentBlockParamUnion {
	out := make([]anthropic.BetaContentBlockParamUnion, 0, len(blocks))
	for _, b := range blocks {
		if b.OfToolResult != nil {
			out = append(out, b)
		}
	}
	for _, b := range blocks {
		if b.OfToolResult == nil {
			out = append(out, b)
		}
	}
	return out
}

// wireTools 合并绑定的工具与按次传入的工具（同名时按次的生效），按 AllowedTools 过滤，按名字排序。
//
// 排序给 prompt cache 用：cache 按渲染后的前缀逐字比对，工具来自多个 MCP server 时注册顺序可能随
// 连接先后变化，排序后同一组工具总是同样的字节。（thinking 的前缀校验按名字集合比对工具，顺序本来
// 就不影响它。）
func (c *Client) wireTools(o *model.Options) ([]anthropic.BetaToolUnionParam, error) {
	byName := make(map[string]*schema.ToolInfo, len(c.tools)+len(o.Tools))
	for _, set := range [][]*schema.ToolInfo{c.tools, o.Tools} {
		for _, t := range set {
			if t != nil {
				byName[t.Name] = t
			}
		}
	}
	if len(o.AllowedTools) > 0 {
		allowed := make(map[string]bool, len(o.AllowedTools))
		for _, n := range o.AllowedTools {
			allowed[n] = true
		}
		for name := range byName {
			if !allowed[name] {
				delete(byName, name)
			}
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]anthropic.BetaToolUnionParam, 0, len(names))
	for _, name := range names {
		t := byName[name]
		schemaJSON, err := inputSchema(t.Params)
		if err != nil {
			return nil, fmt.Errorf("anthropic: tool %q: %w", name, err)
		}
		p := &anthropic.BetaToolParam{
			Name:        name,
			InputSchema: param.Override[anthropic.BetaToolInputSchemaParam](schemaJSON),
		}
		if t.Desc != "" {
			p.Description = param.NewOpt(t.Desc)
		}
		if strict, _ := t.Extra[StrictToolExtraKey].(bool); strict {
			p.Strict = param.NewOpt(true)
		}
		out = append(out, anthropic.BetaToolUnionParam{OfTool: p})
	}
	return out, nil
}

// inputSchema 把工具参数 schema 原样转成 input_schema；API 要求顶层 type 为 object，缺 type 时补上。
func inputSchema(params map[string]any) (json.RawMessage, error) {
	s := params
	if s == nil {
		s = map[string]any{"properties": map[string]any{}}
	}
	if _, ok := s["type"]; !ok {
		cp := make(map[string]any, len(s)+1)
		for k, v := range s {
			cp[k] = v
		}
		cp["type"] = "object"
		s = cp
	}
	raw, err := json.Marshal(s) // map 按键排序编码，同一份 schema 总是同样的字节
	if err != nil {
		return nil, fmt.Errorf("encode input schema: %w", err)
	}
	return raw, nil
}

// toolChoice 映射 tool_choice。none 原样发；auto 与 required 都不发（API 默认就是 auto，强制选工具
// 在 Opus 5.5 上是 400）——不发而不是发 {type: auto}，让同一种意图总是同一份请求体。
func toolChoice(tc *schema.ToolChoice) (anthropic.BetaToolChoiceUnionParam, bool) {
	if tc != nil && *tc == schema.ToolChoiceNone {
		return anthropic.BetaToolChoiceUnionParam{OfNone: &anthropic.BetaToolChoiceNoneParam{}}, true
	}
	return anthropic.BetaToolChoiceUnionParam{}, false
}

// wireHasTools 判断合并 ExtraBody 之后的请求体里有没有工具：后合并的 ExtraBody 里出现 tools 键时
// 以它为准（数组整体替换），null 或空数组表示没有。
func wireHasTools(n int, extras ...map[string]any) bool {
	has := n > 0
	for _, extra := range extras {
		v, ok := extra["tools"]
		if !ok {
			continue
		}
		has = false
		if v != nil {
			if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
				has = rv.Len() > 0
			} else {
				has = true
			}
		}
	}
	return has
}

// dedupeBetas 去掉重复的 beta 值，保留首次出现的顺序。
func dedupeBetas(betas []anthropic.AnthropicBeta) []anthropic.AnthropicBeta {
	seen := make(map[anthropic.AnthropicBeta]bool, len(betas))
	out := betas[:0]
	for _, b := range betas {
		if b == "" || seen[b] {
			continue
		}
		seen[b] = true
		out = append(out, b)
	}
	return out
}

// applyMergePatch 按 RFC 7386 把 patches 依次合并进 JSON 对象 body：对象逐键递归合并，其他值整体
// 替换，null 删除该键。patch 先经 JSON 往返归一（业务可能传结构体、map[string]string 等）。
func applyMergePatch(body []byte, patches ...map[string]any) ([]byte, error) {
	doc, err := decodeObject(body)
	if err != nil {
		return nil, err
	}
	for _, p := range patches {
		if len(p) == 0 {
			continue
		}
		raw, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		patch, err := decodeObject(raw)
		if err != nil {
			return nil, err
		}
		mergeObject(doc, patch)
	}
	return json.Marshal(doc)
}

// decodeObject 解一个 JSON 对象，数字保留原文（json.Number），重新编码时不变形。
func decodeObject(raw []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// mergeObject 是 RFC 7386 的 MergePatch：patch 里的对象与 dst 里的对象递归合并；dst 里没有对象时
// 当作空对象合并（patch 对象里的 null 同样被去掉）。
func mergeObject(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		pm, isObj := v.(map[string]any)
		if !isObj {
			dst[k] = v
			continue
		}
		dm, ok := dst[k].(map[string]any)
		if !ok {
			dm = map[string]any{}
		}
		mergeObject(dm, pm)
		dst[k] = dm
	}
}
