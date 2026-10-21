package agent_v2

import (
	"context"
	"strings"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

// acStubModel 是 shouldTrigger 的 cfg.Model 非 nil 占位（shouldTrigger 只检 model != nil，
// 不调用它）。
type acStubModel struct{}

func (acStubModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, nil
}

func (acStubModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (acStubModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return acStubModel{}, nil }

// TestAutoCompactShouldTrigger_UsesFilteredCharEstimateNotUsage 验证 MED-2 fix：
// shouldTrigger 用 EstimateMessageTokens（纯字符估，只看过滤后的 convMsgs）而非 EstimateTokens
// （优先读 LLM 返回的 Usage.InputTokens = 全 prompt，含已过滤的 skill body）。
//
// 构造：一条带 SkillBody marker 的 message（被 shouldTrigger 过滤掉）+ 一条 assistant 带
// Usage.InputTokens=180000（= LLM 看到的全 prompt，含 skill body）。过滤后 convMsgs 只剩
// [user "hi", assistant "ok"]，字符量 << threshold=150000 → 用 EstimateMessageTokens 不触发。
//
// sanity-revert：把 autocompact.go 的 EstimateMessageTokens 换回 EstimateTokens 后，它读
// assistant 的 Usage.InputTokens=180000 >= 150000 → shouldTrigger 返 true → 下面断言 fail，
// 精确指向 "Usage fast-path 架空了 skill/store 过滤 → 大 skill body 反复空触发烧断路器"。
func TestAutoCompactShouldTrigger_UsesFilteredCharEstimateNotUsage(t *testing.T) {
	cfg := &AutoCompactConfig{
		Enabled:             true,
		Model:               acStubModel{},
		ModelContextWindow:  200_000,
		SummaryMaxTokens:    20_000,
		SummaryBufferTokens: 30_000,
	}
	if got := cfg.Threshold(); got != 150_000 {
		t.Fatalf("threshold setup wrong: want 150000, got %d", got)
	}

	// 被过滤的 skill body（SkillBody marker）——不进 convMsgs，故其字符量与触发判定无关
	bodyMsg := schema.UserMessage(strings.Repeat("技术文档供给", 5000))
	bodyMsg.Annotations = map[string]any{SkillBodyAnnotation: true}

	// assistant 带 Usage.InputTokens=180000（反映 LLM 当时看到的全 prompt，含上面的 body）
	asst := schema.AssistantMessage("ok")
	asst.Metadata = &schema.MessageMetadata{Usage: &schema.MessageUsage{InputTokens: 180_000}}

	messages := []*schema.Message{
		schema.UserMessage("hi"),
		bodyMsg, // SkillBody marker → 被过滤出 convMsgs
		asst,    // Usage.InputTokens=180000（EstimateTokens 会误读）
	}

	// 过滤后 convMsgs = [user "hi", assistant "ok"]，字符估 ~几个 token << 150000 → 不该触发
	if cfg.shouldTrigger(nil, messages) {
		t.Fatalf("shouldTrigger 应为 FALSE：过滤后 convMsgs 字符估 << threshold=150000；" +
			"若用 EstimateTokens 会误读 assistant Usage.InputTokens=180000 >= 150000 而 true（Usage 架空过滤）")
	}

	// 非恒-false 对照：一条真正超长的 user 消息（不带 marker、不过滤）字符估 >> threshold
	// 时 shouldTrigger 必须 true——保证上面 false 是过滤生效、而非 shouldTrigger 总返 false。
	longMsgs := []*schema.Message{
		schema.UserMessage(strings.Repeat("长文本内容填充", 200_000)),
	}
	if !cfg.shouldTrigger(nil, longMsgs) {
		t.Fatalf("shouldTrigger 应为 TRUE：未过滤的超长消息字符估 >> threshold（防恒-false 假绿）")
	}
}

// TestAutoCompactShouldTrigger_SkipsOffloaded 验证 offloaded 孤儿 fix 的连带（crossverify
// 揪出）：offloaded 组改为整组保留不压缩后，shouldTrigger 必须同步跳过
// MicroCompactOffloadedAnnotation 消息——否则多个 offloaded 占位符把 current 顶到恒超阈值，
// AutoCompactRun 又整组保留它们不压缩 → 反复空触发摘要烧断路器（同 skill body 的病）。
//
// sanity-revert：删 shouldTrigger 里 MicroCompactOffloadedAnnotation 的 continue 分支后，
// offloaded 字符量算进 current >= threshold → 断言 fail，精确指向"offloaded 未跳 → 反复空触发"。
func TestAutoCompactShouldTrigger_SkipsOffloaded(t *testing.T) {
	cfg := &AutoCompactConfig{
		Enabled:             true,
		Model:               acStubModel{},
		ModelContextWindow:  200_000,
		SummaryMaxTokens:    20_000,
		SummaryBufferTokens: 30_000,
	}
	// threshold = 150000

	// 多个大 offloaded 占位符（带 marker）——字符量 >> threshold（模拟"多 offloaded 组累积"），
	// 但都该被 shouldTrigger 过滤（整组保留不压缩，算进估算会反复空触发）
	messages := []*schema.Message{schema.UserMessage("hi")}
	for i := 0; i < 5; i++ {
		off := schema.ToolResultMessage("tc", strings.Repeat("被卸载的大内容占位符填充", 20_000))
		off.Annotations = map[string]any{MicroCompactOffloadedAnnotation: true}
		messages = append(messages, off)
	}
	messages = append(messages, schema.AssistantMessage("ok"))

	// 过滤 offloaded 后 convMsgs = [user "hi", assistant "ok"]，字符估 << 150000 → 不触发
	if cfg.shouldTrigger(nil, messages) {
		t.Fatalf("shouldTrigger 应为 FALSE：offloaded 消息应被过滤不算触发估算；" +
			"若不跳 offloaded，5 组大占位符字符量 >> threshold 会反复空触发烧断路器")
	}
}
