package agent_v2_test

// 增量摘要（可压缩区里已有上一份摘要）时的两道判据：可压缩量与缩减比只按上一份摘要之外的新内容算。

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// incrementalSummarizer 像真实模型那样做增量摘要：上一份摘要的内容保留，新内容压到十分之一（至少 50 字节）；
// capBytes > 0 时整份摘要不超过它（真实模型的摘要有长度上限，到了上限就把旧内容也压一压——这里截掉最早的部分）。
type incrementalSummarizer struct {
	mu       sync.Mutex
	calls    int
	capBytes int
}

func (s *incrementalSummarizer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	var prev, fresh strings.Builder
	for _, m := range msgs[:len(msgs)-1] { // 最后一条是摘要指令
		if m == nil || m.Role == schema.RoleSystem {
			continue
		}
		if v, _ := m.Annotations[agent_v2.AutoCompactSummaryAnnotation].(bool); v {
			prev.WriteString(strings.TrimPrefix(m.Content, "# Previous Conversation Summary\n\n"))
			continue
		}
		fresh.WriteString(m.Content)
		for _, tc := range schema.GetToolCalls(m) {
			fresh.WriteString(tc.Function.Arguments)
		}
	}
	digest := fresh.String()
	n := len(digest) / 10
	if n < 50 {
		n = 50
	}
	if n > len(digest) {
		n = len(digest)
	}
	out := prev.String() + digest[:n]
	if s.capBytes > 0 && len(out) > s.capBytes {
		out = out[len(out)-s.capBytes:]
	}
	return schema.AssistantMessage("<summary>" + out + "</summary>"), nil
}
func (s *incrementalSummarizer) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *incrementalSummarizer) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

// compactionOutcomes 数压缩事件里 AutoCompact 的结局：被判无效的（缩减比不够 / 超过摘要预算）。
type compactionOutcomes struct {
	agent_v2.NoOpOrchestrationHooks
	mu       sync.Mutex
	rejected int
	errs     []string
}

func (h *compactionOutcomes) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if ev.Type != agent_v2.CompactionEventType {
		return ctx
	}
	a, ok := ev.Metadata["action"].(*agent_v2.CompactionAction)
	if !ok || a.Layer != agent_v2.CompactionLayerAuto || a.After == nil || a.After.Err == "" {
		return ctx
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errs = append(h.errs, a.After.Err)
	if strings.Contains(a.After.Err, "insufficient reduction") || strings.Contains(a.After.Err, "exceeds SummaryMaxTokens") {
		h.rejected++
	}
	return ctx
}

// TestAutoCompact_IncrementalSummaryNotRejectedAsSummaryGrows：一次 Run 里摘要了很多次、上一份摘要已经长到它的上限附近时，
// 合法的增量摘要不该被判无效，压缩要一直起作用。上一份摘要已经是压缩过的，缩减比要按它之外的新内容算——按整个
// 可压缩区算的话，上一份摘要越长、两次触发之间的新内容越少（触发看的是整段输入，上一份摘要也在里面），新摘要越难
// 小于 0.8 倍：连续三次判无效让断路器跳闸，这个任务此后不再压缩（冷却 5 分钟），模型输入一路涨。
//
// 摘要桩像真实模型：保留上一份、新内容压到十分之一，整份不超过 25500 字节（约 8500 token，在 SummaryMaxTokens 9000
// 以内）。阈值 10000：摘要长到 8500 之后，两次触发之间的新内容只有一两轮工具结果（约 1300 token 一轮），旧判据
// 「新摘要 ≤ 0.8 × (上一份 + 新内容)」在这里判无效。断言都是正向的：没有一次压缩被判无效、模型输入有界。
func TestAutoCompact_IncrementalSummaryNotRejectedAsSummaryGrows(t *testing.T) {
	const rounds = 220
	m := &compactLoopModel{rounds: rounds}
	s := &incrementalSummarizer{capBytes: 25500}
	h := &compactionOutcomes{}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 20000,
		SummaryMaxTokens: 9000, SummaryBufferTokens: 1000, MinCompactableTokens: 1000}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(rounds+5), agent_v2.WithAutoCompact(cfg))
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), h)
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	if s.calls < 5 {
		t.Fatalf("fixture 没搭对：摘要只被调了 %d 次", s.calls)
	}
	if h.rejected > 0 {
		t.Fatalf("%d 轮里摘要被调 %d 次，其中 %d 次被判无效——上一份摘要被算进了可压缩量；首个错误：%s",
			rounds, s.calls, h.rejected, h.errs[0])
	}
	maxLate := 0
	for _, in := range m.inputs[len(m.inputs)-40:] {
		if n := compose_v2.EstimateMessageTokens(in); n > maxLate {
			maxLate = n
		}
	}
	if limit := 2 * cfg.Threshold(); maxLate > limit {
		t.Fatalf("最后 40 轮模型输入最多 %d token，超过阈值的两倍 %d——压缩停了，输入在涨", maxLate, limit)
	}
	t.Logf("%d 轮里摘要被调 %d 次，没有被判无效的；最后 40 轮模型输入最多 %d token（阈值 %d）", rounds, s.calls, maxLate, cfg.Threshold())
}

// TestAutoCompactRun_SummaryOverBudgetRejected：摘要本身超过 SummaryMaxTokens（阈值公式给它留的预算）时判无效——按比例
// 算再有效也不行：把上一份摘要原样保留、只追加新内容的模型会让摘要一轮轮变长，压完照样在阈值之上、下一轮又触发。
func TestAutoCompactRun_SummaryOverBudgetRejected(t *testing.T) {
	s := &incrementalSummarizer{} // 不设上限：保留上一份摘要原样
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 7000,
		SummaryMaxTokens: 2000, SummaryBufferTokens: 100, MinCompactableTokens: 1000}
	msgs := []*schema.Message{prevSummary(7500), schema.UserMessage("上一个问题"),
		schema.AssistantMessage(strings.Repeat("a", 9000))}
	msgs = append(msgs, schema.UserMessage("这一个问题"))
	msgs = append(msgs, toolRound("c2", strings.Repeat("b", 600))...)
	_, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if s.calls != 1 {
		t.Fatalf("fixture 没搭对：摘要模型被调 %d 次，want 1", s.calls)
	}
	if err == nil || !strings.Contains(err.Error(), "exceeds SummaryMaxTokens") {
		t.Fatalf("摘要超过 SummaryMaxTokens 时应判无效，got err=%v", err)
	}
}

// TestAutoCompactRun_OnlyLatestSummaryIsPrevious：可压缩区里有几份摘要时只有最新一份算「上一份」，更早的算新内容——
// 合并它们能压小。框架自己的路径上至多一份；业务自己写进来几份时，都算「不可再压」会让压缩一直被跳过。
func TestAutoCompactRun_OnlyLatestSummaryIsPrevious(t *testing.T) {
	s := &incrementalSummarizer{capBytes: 600}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 9000,
		SummaryMaxTokens: 1000, SummaryBufferTokens: 100, MinCompactableTokens: 1000}
	msgs := []*schema.Message{prevSummary(12000), prevSummary(12000), schema.UserMessage("上一个问题"),
		schema.AssistantMessage(strings.Repeat("a", 600))}
	msgs = append(msgs, schema.UserMessage("这一个问题"))
	msgs = append(msgs, toolRound("c2", strings.Repeat("b", 600))...)
	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatalf("合并两份摘要应能压下来：%v", err)
	}
	if s.calls != 1 {
		t.Fatalf("两份摘要之外的新内容不多，但更早那份摘要算新内容，应调一次摘要模型；实际 %d 次", s.calls)
	}
	if len(out) >= len(msgs) {
		t.Fatalf("压缩之后 %d 条，原来 %d 条——没压", len(out), len(msgs))
	}
}

// verbatimSummarizer 把上一份摘要与新内容原样抄进「摘要」——压缩无效的那种输出。
type verbatimSummarizer struct{ calls int }

func (s *verbatimSummarizer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.calls++
	var b strings.Builder
	for _, m := range msgs[:len(msgs)-1] {
		if m != nil && m.Role != schema.RoleSystem {
			b.WriteString(strings.TrimPrefix(m.Content, "# Previous Conversation Summary\n\n"))
		}
	}
	return schema.AssistantMessage("<summary>" + b.String() + "</summary>"), nil
}
func (s *verbatimSummarizer) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *verbatimSummarizer) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

// prevSummary 造一条上一次 AutoCompact 产出的摘要消息（约 chars/3 个 token）。
func prevSummary(chars int) *schema.Message {
	m := schema.UserMessage("# Previous Conversation Summary\n\n" + strings.Repeat("旧", chars/3))
	m.Annotations = map[string]any{agent_v2.AutoCompactSummaryAnnotation: true}
	return m
}

func toolRound(id, payload string) []*schema.Message {
	return []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}}),
		schema.ToolResultMessage(id, payload),
	}
}

// TestAutoCompactRun_LongPreviousSummaryLittleNewContentSkips：上一份摘要很长、它之外的新内容不到 MinCompactableTokens 时
// 跳过——不调摘要模型、不算失败。把上一份摘要算进可压缩量的话，这里会调一次模型、产出跟上一份差不多长的摘要、
// 被判「缩减比不够」，白调一次还计一次断路器失败。
func TestAutoCompactRun_LongPreviousSummaryLittleNewContentSkips(t *testing.T) {
	s := &incrementalSummarizer{}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 9000,
		SummaryMaxTokens: 100, SummaryBufferTokens: 100, MinCompactableTokens: 1000}
	msgs := []*schema.Message{prevSummary(24000), schema.UserMessage("上一个问题")}
	msgs = append(msgs, toolRound("c1", strings.Repeat("a", 600))...)
	msgs = append(msgs, schema.UserMessage("这一个问题"))
	msgs = append(msgs, toolRound("c2", strings.Repeat("b", 600))...)
	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatalf("新内容不够时应跳过、不报错：%v", err)
	}
	if s.calls != 0 {
		t.Fatalf("上一份摘要之外的新内容不到 MinCompactableTokens，却调了 %d 次摘要模型", s.calls)
	}
	if len(out) != len(msgs) {
		t.Fatalf("跳过时应原样返回，%d 条变成了 %d 条", len(msgs), len(out))
	}
}

// TestAutoCompactRun_VerbatimIncrementalSummaryRejected：增量摘要时把新内容原样抄进摘要的，照样被判「缩减比不够」——
// 上限是「上一份摘要 + 新内容的 MaxReductionRatio 倍」，不是不设上限。
func TestAutoCompactRun_VerbatimIncrementalSummaryRejected(t *testing.T) {
	s := &verbatimSummarizer{}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 7000,
		SummaryMaxTokens: 100, SummaryBufferTokens: 100, MinCompactableTokens: 1000}
	msgs := []*schema.Message{prevSummary(9000), schema.UserMessage("上一个问题"),
		schema.AssistantMessage(strings.Repeat("a", 12000))}
	msgs = append(msgs, schema.UserMessage("这一个问题"))
	msgs = append(msgs, toolRound("c2", strings.Repeat("b", 600))...)
	_, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if s.calls != 1 {
		t.Fatalf("fixture 没搭对：摘要模型被调 %d 次，want 1", s.calls)
	}
	if err == nil || !strings.Contains(err.Error(), "insufficient reduction") {
		t.Fatalf("新内容被原样抄进摘要时应判「缩减比不够」，got err=%v", err)
	}
}

// promptCapturingSummarizer 记下最后一次收到的摘要指令（最后一条消息），回一份固定的摘要。
type promptCapturingSummarizer struct {
	reply      string
	lastPrompt string
}

func (s *promptCapturingSummarizer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1] != nil {
		s.lastPrompt = msgs[len(msgs)-1].Content
	}
	return schema.AssistantMessage(s.reply), nil
}
func (s *promptCapturingSummarizer) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *promptCapturingSummarizer) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return s, nil
}

// TestAutoCompactRun_SpillIndexNotCountedAgainstSummaryCap：可压缩区里有 50 条转存过的工具结果，摘要模型只写一句话。框架附在
// 摘要后面的取回索引是框架加的、模型控制不了，SummaryMaxTokens 只比模型写的那部分——算进去的话索引自己就超过 1000，
// 每轮判无效、断路器跳闸（审计实测）。
func TestAutoCompactRun_SpillIndexNotCountedAgainstSummaryCap(t *testing.T) {
	s := &promptCapturingSummarizer{reply: "<summary>用户在排查日志，已 grep 多个文件。</summary>"}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
		SummaryMaxTokens: 1000, SummaryBufferTokens: 100, MinCompactableTokens: 1000}
	msgs := []*schema.Message{schema.UserMessage("帮我排查日志")}
	for i := 0; i < 50; i++ {
		round := toolRound(fmt.Sprintf("c%d", i), "【工具输出过大，已转存】预览 "+strings.Repeat("x", 1000))
		round[1].Annotations = map[string]any{
			agent_v2.ToolResultSpillAnnotation: fmt.Sprintf("sp_%016x", i),
			"crew_tool_result_spill_info":      "grep，2000000 字节，40000 行",
		}
		msgs = append(msgs, round...)
	}
	msgs = append(msgs, schema.UserMessage("这一个问题"))
	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatalf("摘要模型只写了一句话，却判无效：%v", err)
	}
	var sum *schema.Message
	for _, m := range out {
		if v, _ := m.Annotations[agent_v2.AutoCompactSummaryAnnotation].(bool); v {
			sum = m
		}
	}
	if sum == nil {
		t.Fatal("压缩结果里没有摘要消息")
	}
	if n := compose_v2.EstimateMessageTokens([]*schema.Message{sum}); n <= cfg.SummaryMaxTokens || !strings.Contains(sum.Content, "sp_0000000000000031") {
		t.Fatalf("fixture 没搭对：摘要消息 %d token，应附着 50 条索引、整条超过 SummaryMaxTokens", n)
	}
}

// TestAutoCompact_PromptCarriesSummaryBudget：摘要超过 SummaryMaxTokens 判无效，摘要请求里要写明这个数——不写的话照 9 段
// 提示词写出的摘要在它配得小时每次都超、每次白调一次。两种语言的提示词都要带。
func TestAutoCompact_PromptCarriesSummaryBudget(t *testing.T) {
	for _, lang := range []agent_v2.AutoCompactLanguage{agent_v2.AutoCompactLangCN, agent_v2.AutoCompactLangEN} {
		s := &promptCapturingSummarizer{reply: "<summary>短</summary>"}
		cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 7000,
			SummaryMaxTokens: 1234, SummaryBufferTokens: 100, MinCompactableTokens: 1000, Language: lang}
		msgs := []*schema.Message{schema.UserMessage("上一个问题"), schema.AssistantMessage(strings.Repeat("a", 24000)),
			schema.UserMessage("这一个问题")}
		if _, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs); err != nil {
			t.Fatalf("语言 %d：压缩失败：%v", lang, err)
		}
		if s.lastPrompt == "" {
			t.Fatalf("语言 %d：fixture 没搭对：没调摘要模型", lang)
		}
		if !strings.Contains(s.lastPrompt, "1234") {
			t.Errorf("语言 %d：摘要请求里没有写明长度预算 1234", lang)
		}
	}
}
