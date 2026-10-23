package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/anthropics/anthropic-sdk-go"

	"crew-go/model"
	"crew-go/schema"
)

// assistantTurn 是从一条完整响应里读出来的东西：Generate 整条用；Stream 只用 calls / annotations /
// metadata（文本与 thinking 文本已经按片交出过）。
type assistantTurn struct {
	// text 是文本块按 [textJoiner] 规则拼成的 Content。
	text string
	// reasoning 是非空的 thinking 文本以空行分隔拼成的 ReasoningContent。
	reasoning string
	// calls 是工具调用块（服务端回退切换点之前的已剔除）。
	calls       []*schema.ContentBlock
	annotations map[string]any
	metadata    *schema.MessageMetadata
}

// interpret 解读一条完整的响应：先按结束原因判断输出能不能用，再拼出领域字段、回放快照与元数据。
//
// rawInputs 是流式时按块下标收下的 tool_use 参数原文（input_json_delta 依次拼接，没经过 SDK 的修补）；
// Generate 传 nil。
func (c *Client) interpret(ctx context.Context, resp *anthropic.BetaMessage, rawInputs map[int]string) (*assistantTurn, error) {
	if err := c.checkStopReason(ctx, resp); err != nil {
		return nil, err
	}
	t := &assistantTurn{}
	t.text, t.reasoning = joinContent(resp.Content)

	// max_tokens 截在工具调用中间时，最后一块 tool_use 的参数不完整。SDK 累积流式响应时把截断的参数
	// 换成了 {}，原样交出就会让工具拿空参数执行（框架那道参数闸只拦解析失败的 JSON）。
	cut := -1
	if n := len(resp.Content); resp.StopReason == anthropic.BetaStopReasonMaxTokens && n > 0 && resp.Content[n-1].Type == "tool_use" {
		cut = n - 1
	}
	lastFallback := lastFallbackIndex(resp.Content)
	raws := make([]json.RawMessage, 0, len(resp.Content))
	for i, b := range resp.Content {
		if !replayable(i, b.Type, lastFallback) {
			continue
		}
		p := b.ToParam()
		if b.Type == "tool_use" {
			input := toolInput(string(b.Input))
			args := string(input)
			if i == cut {
				args = truncatedArguments(rawInputs[i], args)
				warn(ctx, "anthropic: max_tokens cut off a tool call, passing it on with unparseable arguments so it is rejected instead of run; raise max_tokens",
					logKeyTool, b.Name)
			}
			t.calls = append(t.calls, &schema.ContentBlock{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id:        b.ID,
					Name:      b.Name,
					Arguments: args,
				},
			})
			// ToParam 经 SDK 的类型化结构转一道，input 被解成 any：超过 2^53 的整数与高精度小数变成
			// float64 近似值。换回响应里的原文，回放的值逐个不变（值变了就是改了前缀）。
			if p.OfToolUse != nil {
				p.OfToolUse.Input = input
			}
		}
		raw, err := json.Marshal(p)
		if err != nil {
			return nil, c.streamError(fmt.Errorf("encode %s block for replay: %w", b.Type, err))
		}
		raws = append(raws, raw)
	}

	calls := schema.GetToolCalls(&schema.Message{ContentBlocks: t.calls})
	replay, err := encodeReplay(raws, t.text, calls)
	if err != nil {
		return nil, c.streamError(err)
	}
	t.annotations = map[string]any{AssistantContentAnnotation: replay}
	if tr := transformationValues(resp.InputTransformations); len(tr) > 0 {
		t.annotations[InputTransformationsAnnotation] = tr
		first := resp.InputTransformations[0]
		// 每条响应只记一条：历史被改过之后，后面每次请求都会把同一批块再丢一遍。
		warn(ctx, "anthropic: API transformed the request input",
			logKeyCount, len(resp.InputTransformations), logKeyType, first.Type, logKeyReason, first.Reason, logKeyPath, first.Path)
	}
	if hops := fallbackValues(resp.Content); len(hops) > 0 {
		t.annotations[FallbackAnnotation] = hops
	}
	t.metadata = &schema.MessageMetadata{
		FinishReason: finishReason(resp.StopReason),
		Model:        string(resp.Model),
		System:       ProviderName,
		Usage:        usageOf(resp.Usage),
	}
	return t, nil
}

// checkStopReason 按结束原因判断输出能不能当正常结果（Generate 与流式共用）：
//   - end_turn、tool_use、stop_sequence、max_tokens：可以。max_tokens 是被截断，要不要让业务看见
//     另算，这里不当错误（同 model/openai 对 length 的处理）。
//   - refusal：拒答（安全分类器或模型自己），返回 [RefusalError]，不可重试。已经流出的部分文本作废。
//   - model_context_window_exceeded：输入加输出撑满了上下文窗口，返回包着
//     model.ErrContextWindowExceeded 的错误（上下文压缩认得出，压缩后重试）。
//   - pause_turn、compaction 与没见过的取值：按完整处理，记一条告警。前两个只在用了服务端工具 /
//     服务端压缩时出现，本包不启用它们；新加的取值多半是正常结束的变体，当成错误会让整轮失败。
//   - 空：响应已经完整读完（流式要求先收到 message_stop），按完整处理。
func (c *Client) checkStopReason(ctx context.Context, resp *anthropic.BetaMessage) error {
	switch resp.StopReason {
	case "", anthropic.BetaStopReasonEndTurn, anthropic.BetaStopReasonToolUse,
		anthropic.BetaStopReasonStopSequence, anthropic.BetaStopReasonMaxTokens:
		return nil
	case anthropic.BetaStopReasonRefusal:
		d := resp.StopDetails
		return c.streamError(&RefusalError{
			Category:         string(d.Category),
			Explanation:      d.Explanation,
			RecommendedModel: d.RecommendedModel,
		})
	case anthropic.BetaStopReasonModelContextWindowExceeded:
		return c.streamError(fmt.Errorf("%w (stop_reason=%s)", model.ErrContextWindowExceeded, resp.StopReason))
	default:
		warn(ctx, "anthropic: unexpected stop_reason, treated as complete", logKeyStopReason, string(resp.StopReason))
		return nil
	}
}

// RefusalError 是 stop_reason=refusal 的错误：安全分类器或模型自己拒绝了这次请求。
//
// 它包着 model.ErrContentFiltered（errors.Is 认得出，且不可重试：同一份输入多半还会被拒）。开着
// 服务端回退（[WithFallbacks]）时看到它，可能是回退模型也拒绝了，也可能是这类拒答不回退（如
// reasoning_extraction）、或回退模型限流 / 过载没能执行（这时 RecommendedModel 可能有值）。
type RefusalError struct {
	// Category 是拒答类别（cyber、bio、reasoning_extraction……），API 没给时为空。只用于展示与
	// 观测，别据它分支：类别会增加，也可能为空。
	Category string
	// Explanation 是 API 给的说明，可能为空。
	Explanation string
	// RecommendedModel 是回退模型限流 / 过载、回退没能执行时，API 建议直接改用的模型；通常为空。
	RecommendedModel string
}

// Error 实现 error。
func (e *RefusalError) Error() string {
	var b strings.Builder
	b.WriteString("model refused the request (stop_reason=refusal")
	if e.Category != "" {
		b.WriteString(", category=")
		b.WriteString(e.Category)
	}
	b.WriteString(")")
	if e.Explanation != "" {
		b.WriteString(": ")
		b.WriteString(clip(e.Explanation))
	}
	return b.String()
}

// Unwrap 交出 model.ErrContentFiltered。
func (e *RefusalError) Unwrap() error { return model.ErrContentFiltered }

// finishReason 把 stop_reason 映射成 crew-go 惯用的结束原因（对齐 OpenAI 的取值）。
func finishReason(r anthropic.BetaStopReason) string {
	switch r {
	case anthropic.BetaStopReasonEndTurn, anthropic.BetaStopReasonStopSequence:
		return "stop"
	case anthropic.BetaStopReasonToolUse:
		return "tool_calls"
	case anthropic.BetaStopReasonMaxTokens:
		return "length"
	default:
		return string(r)
	}
}

// usageOf 把 Anthropic 的用量换成 [schema.MessageUsage] 的口径：Anthropic 的 input_tokens 不含
// cache 读写，这里 InputTokens 加回两者；output_tokens 含 thinking（按输出计费），OutputTokens 照原样、
// ThinkTokens 是其中 thinking 的那部分（子集，同 CachedInputTokens 之于 InputTokens）。
//
// 读的是顶层 usage。服务端回退时它只算接着答的那次尝试，被拒的那次在 usage.iterations 里（中途被拒的
// 按正常价计费），这里不汇总——两次尝试是不同模型、不同价格，加在一起挂在一个模型名下反而失真。
func usageOf(u anthropic.BetaUsage) *schema.MessageUsage {
	input := u.InputTokens + u.CacheReadInputTokens + u.CacheCreationInputTokens
	think := u.OutputTokensDetails.ThinkingTokens
	return &schema.MessageUsage{
		InputTokens:              clampInt32(input),
		CachedInputTokens:        clampInt32(u.CacheReadInputTokens),
		CacheCreationInputTokens: clampInt32(u.CacheCreationInputTokens),
		OutputTokens:             clampInt32(u.OutputTokens),
		ThinkTokens:              clampInt32(think),
		TotalTokens:              clampInt32(input + u.OutputTokens),
	}
}

func clampInt32(v int64) int32 {
	return int32(min(max(v, 0), math.MaxInt32))
}

// joinContent 拼出 Content 与 ReasoningContent。流式路径用同一个 [textJoiner] 逐片拼，两边拼出的
// 字符串一致——回放指纹按 Content 算，流式聚合出的 Content 必须等于这里的结果。
//
// 按服务端回退过滤之前的全部块来拼：回退只作废切换点之前的 thinking 与工具调用，文本照常保留，而
// 流式时这些块都已经流出去了。
func joinContent(blocks []anthropic.BetaContentBlockUnion) (text, reasoning string) {
	var tb, rb strings.Builder
	var j textJoiner
	for _, b := range blocks {
		switch b.Type {
		case "text":
			tb.WriteString(j.piece(b.Text))
		case "thinking":
			j.nonText()
			if b.Thinking != "" {
				if rb.Len() > 0 {
					rb.WriteString(reasoningSeparator)
				}
				rb.WriteString(b.Thinking)
			}
		default:
			j.nonText()
		}
	}
	return tb.String(), rb.String()
}

// reasoningSeparator 分隔相邻两个 thinking 块的文本。
const reasoningSeparator = "\n\n"

// textJoiner 把多个 text 块拼成一个 Content：相邻的 text 块直接相接（同一段话被引用标注等切开的
// 情形）；中间隔着别的块（工具调用、thinking）的两段文本是两次发言，补一个空行，已有空白时不补。
//
// 流式时逐片喂（每个 text_delta 一片），整条响应时每个 text 块一片，规则只在一个块的第一个非空片上
// 起作用，所以两种喂法拼出同一个字符串。
type textJoiner struct {
	wrote     bool // 已经拼出过非空文本
	gap       bool // 上一段非空文本之后出现过非 text 块
	lastSpace bool // 已拼出的文本以空白结尾
}

// nonText 记下出现了一个非 text 块。
func (j *textJoiner) nonText() {
	if j.wrote {
		j.gap = true
	}
}

// piece 返回这片文本该拼上去的字符串。
func (j *textJoiner) piece(s string) string {
	if s == "" {
		return ""
	}
	out := s
	if j.gap && !j.lastSpace && !startsWithSpace(s) {
		out = "\n\n" + s
	}
	j.wrote, j.gap = true, false
	j.lastSpace = endsWithSpace(s)
	return out
}

func startsWithSpace(s string) bool {
	r, _ := utf8.DecodeRuneInString(s)
	return unicode.IsSpace(r)
}

func endsWithSpace(s string) bool {
	r, _ := utf8.DecodeLastRuneInString(s)
	return unicode.IsSpace(r)
}

// lastFallbackIndex 是最后一个 fallback 块的下标，没有时 -1。
func lastFallbackIndex(blocks []anthropic.BetaContentBlockUnion) int {
	last := -1
	for i, b := range blocks {
		if b.Type == "fallback" {
			last = i
		}
	}
	return last
}

// replayable 按服务端回退的规则判断第 i 块是否保留（回放、交出工具调用）：最后一个 fallback 块之前
// 只留 text 块（thinking、redacted_thinking、tool_use 等都是被拒的那个模型的，不回放、不执行）；
// fallback 块本身是审计标记，也不回放（经过记在 [FallbackAnnotation]）。没有 fallback 块时都保留。
// 比官方规则严一点：切换点之前成对的服务端工具块官方允许回放，这里也不留（本包不启用服务端工具）。
func replayable(i int, blockType string, lastFallback int) bool {
	switch {
	case lastFallback < 0, i > lastFallback:
		return true
	case i < lastFallback:
		return blockType == "text"
	default: // fallback 块本身
		return false
	}
}

// truncatedArguments 是被 max_tokens 截断的工具调用交给框架的参数：故意是解析不了的 JSON，让框架按
// 「参数非法」拒掉这次调用、把错误交回模型重试——官方要求截断的 tool_use 不要执行。流式时用收到的
// 原文（截在哪儿就是哪儿，错误是 unexpected end of JSON input）；拿不到原文、或原文碰巧是完整的
// JSON 时，去掉末尾的 }。
func truncatedArguments(raw, input string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		s = strings.TrimSpace(input)
	}
	if json.Valid([]byte(s)) {
		s = strings.TrimSuffix(s, "}")
	}
	if s == "" {
		s = "{"
	}
	return s
}

// fallbackValues 是 [FallbackAnnotation] 的值：每个 fallback 块一项 {"from": 模型, "to": 模型}。
func fallbackValues(blocks []anthropic.BetaContentBlockUnion) []any {
	var out []any
	for _, b := range blocks {
		if b.Type == "fallback" {
			out = append(out, map[string]any{"from": string(b.From.Model), "to": string(b.To.Model)})
		}
	}
	return out
}

// transformationValues 是 [InputTransformationsAnnotation] 的值：按（种类, 原因）归并，每组
// {"type", "reason", "count", "first_path"}，按首次出现的顺序。
func transformationValues(in []anthropic.BetaInputTransformationUnion) []any {
	type group struct{ typ, reason string }
	at := make(map[group]map[string]any, 2)
	var out []any
	for _, x := range in {
		g := group{x.Type, x.Reason}
		if m, ok := at[g]; ok {
			m["count"] = m["count"].(int) + 1
			continue
		}
		m := map[string]any{"type": x.Type, "reason": x.Reason, "count": 1, "first_path": x.Path}
		at[g] = m
		out = append(out, m)
	}
	return out
}

// maxErrorDetailBytes 是放进错误文本的 provider 原文上限：错误文本会进 span、交给前端与对端。
const maxErrorDetailBytes = 512

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
