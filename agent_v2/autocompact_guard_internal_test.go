package agent_v2

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// guardModel 是可配置响应 + 调用计数的 stub model。
// 计数用于断言"守卫生效时根本没调 LLM"——这是 MinCompactableTokens 的核心契约。
type guardModel struct {
	mu    sync.Mutex
	calls int
	resp  string
}

func (g *guardModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	return schema.AssistantMessage(g.resp), nil
}

func (g *guardModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (g *guardModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return g, nil }

func (g *guardModel) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// guardCfg 造一个阈值 = 2000 的 AutoCompactConfig（35000 - 20000 - 13000）。
func guardCfg(m model.ChatModel) *AutoCompactConfig {
	return &AutoCompactConfig{
		Enabled:             true,
		Model:               m,
		ModelContextWindow:  35_000,
		SummaryMaxTokens:    20_000,
		SummaryBufferTokens: 13_000,
		breakers:            &sync.Map{},
		breakerMaxFailures:  3,
	}
}

// TestAutoCompactRun_SkipsWhenCompactableRegionTooSmall 验证 MinCompactableTokens 守卫。
//
// **Bug 场景**：触发阈值算的是整个对话，可压缩区（early）却可能远小于它——单个巨大
// 当前轮（一次超大 tool_result）把总量顶过阈值，可压缩的历史其实没多少。修复前框架
// 照样调一次摘要 LLM，省不下几个 token，纯浪费 quota + 延迟；且每轮重复发生。
//
// **测试设计**：group1（early）~10 token，group2（tail）~10000 token。
// 总量远超阈值 2000 → 必然触发；early 远小于 MinCompactableTokens=500 → 必须跳过。
//   - fix 后：不调 LLM，原样返回
//   - fix 前：调一次 LLM 做无意义摘要
func TestAutoCompactRun_SkipsWhenCompactableRegionTooSmall(t *testing.T) {
	m := &guardModel{resp: "<summary>compressed</summary>"}
	cfg := guardCfg(m)
	cfg.MinCompactableTokens = 500

	msgs := []*schema.Message{
		schema.UserMessage("tiny"),                      // group1 = early
		schema.UserMessage(strings.Repeat("x", 40_000)), // group2 = tail
	}

	// 前置假设核对——测试的价值全靠这两个不等式成立
	earlyTokens := compose_v2.EstimateMessageTokens(msgs[:1])
	totalTokens := compose_v2.EstimateMessageTokens(msgs)
	if earlyTokens >= cfg.MinCompactableTokens {
		t.Fatalf("前置假设：early 应 < %d token，实际 %d", cfg.MinCompactableTokens, earlyTokens)
	}
	if totalTokens < cfg.Threshold() {
		t.Fatalf("前置假设：总量应 >= 阈值 %d 以触发压缩，实际 %d", cfg.Threshold(), totalTokens)
	}

	out, err := AutoCompactRun(context.Background(), cfg, msgs)

	// **断言顺序有意为之**：先查调用次数再查 err。守卫 2（MaxReductionRatio）会在
	// 守卫 1 缺失时顺手拦下这个用例并返错——若先断言 err，sanity-revert 守卫 1 时
	// fail 信息会指向 insufficient reduction，掩盖真正的缺陷（不该调 LLM 却调了）。
	if n := m.callCount(); n != 0 {
		t.Fatalf("early 仅 %d token（< 阈值 %d），不该调摘要 LLM；实际调了 %d 次",
			earlyTokens, cfg.MinCompactableTokens, n)
	}
	if err != nil {
		t.Fatalf("跳过路径应静默成功，得到 err=%v", err)
	}
	if len(out) != len(msgs) {
		t.Fatalf("跳过时应原样返回 %d 条消息，实际 %d 条", len(msgs), len(out))
	}
	for i := range out {
		if out[i] != msgs[i] {
			t.Fatalf("跳过时不应改动消息，第 %d 条被替换", i)
		}
	}
}

// TestAutoCompactRun_RejectsInsufficientReduction 验证 MaxReductionRatio 守卫。
//
// **Bug 场景**：LLM 摘要可能失控——不遵循指令原样复述、甚至比原文更长。修复前框架
// 照单全收：白花一次调用，还把上下文搞得更大，下轮更容易再次触发形成恶性循环。
//
// **测试设计**：early ~5000 token，stub 返回体量相当的 summary（~5000 token）
// > limit（5000 * 0.8 = 4000）→ 必须判失败回退。
//   - fix 后：返 insufficient reduction 错误 + 原样 messages
//   - fix 前：err=nil 且 messages 被替换成更大的 summary
func TestAutoCompactRun_RejectsInsufficientReduction(t *testing.T) {
	bulk := strings.Repeat("y", 20_000)
	m := &guardModel{resp: "<summary>" + bulk + "</summary>"}
	cfg := guardCfg(m)
	cfg.MinCompactableTokens = 100 // 放行守卫 1，专测守卫 2

	msgs := []*schema.Message{
		schema.UserMessage(bulk),       // group1 = early
		schema.UserMessage("now what"), // group2 = tail
	}

	earlyTokens := compose_v2.EstimateMessageTokens(msgs[:1])
	limit := int(float64(earlyTokens) * defaultAutoCompactMaxReductionRatio)
	summaryTokens := compose_v2.EstimateMessageTokens(
		[]*schema.Message{schema.UserMessage("# Previous Conversation Summary\n\n" + bulk)})
	if summaryTokens <= limit {
		t.Fatalf("前置假设：summary(%d) 必须 > limit(%d) 才能触发拒绝", summaryTokens, limit)
	}

	out, err := AutoCompactRun(context.Background(), cfg, msgs)
	if err == nil {
		t.Fatalf("摘要 %d token > 上限 %d，应判压缩无效返错；实际 err=nil 且返回 %d 条消息",
			summaryTokens, limit, len(out))
	}
	if !strings.Contains(err.Error(), "insufficient reduction") {
		t.Fatalf("应返 insufficient reduction 错误，实际：%v", err)
	}
	if len(out) != len(msgs) {
		t.Fatalf("压缩无效时应回退原 %d 条消息，实际 %d 条", len(msgs), len(out))
	}
	for i := range out {
		if out[i] != msgs[i] {
			t.Fatalf("压缩无效时应原样回退，第 %d 条被替换", i)
		}
	}
}

// TestAutoCompactRun_InsufficientReductionCountsTowardBreaker 验证"压缩无效"计入断路器。
//
// 底层 Generate 成功 ≠ 业务成功（CLAUDE.md「什么算成功」）——摘要持续无效时必须让
// 断路器跳，否则每轮都白烧一次 LLM 调用。设 breakerMaxFailures=1 让一次失败即跳。
func TestAutoCompactRun_InsufficientReductionCountsTowardBreaker(t *testing.T) {
	bulk := strings.Repeat("z", 20_000)
	m := &guardModel{resp: "<summary>" + bulk + "</summary>"}
	cfg := guardCfg(m)
	cfg.MinCompactableTokens = 100
	cfg.breakerMaxFailures = 1

	msgs := []*schema.Message{
		schema.UserMessage(bulk),
		schema.UserMessage("now what"),
	}

	entry := cfg.getOrCreateBreakerEntry("")
	if entry == nil {
		t.Fatal("前置假设：breakers 已初始化，entry 不应为 nil")
	}
	if !cfg.shouldTrigger(entry, msgs) {
		t.Fatal("前置假设：首次应可触发压缩")
	}

	if _, err := AutoCompactRun(context.Background(), cfg, msgs); err == nil {
		t.Fatal("前置假设：本次压缩应因缩减不足而失败")
	}

	if cfg.shouldTrigger(entry, msgs) {
		t.Fatal("压缩无效应计入断路器（maxFailures=1 → 一次即跳），但断路器仍放行——" +
			"摘要持续无效时会每轮白烧一次 LLM 调用")
	}
}
