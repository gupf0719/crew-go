package agent_v2

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

// dummyACModel 是 AutoCompact 测试用 stub model（cfg.Model 必须非 nil）。
type dummyACModel struct{}

func (d *dummyACModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("[mock summary]"), nil
}
func (d *dummyACModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (d *dummyACModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return d, nil
}

// TestAutoCompact_R29LOW1_ShouldTriggerSkipsMemoryMarker 是 R29 视角2 LOW-1 回归——
// 验证 AutoCompactConfig.shouldTrigger 跳带 StoreInjectedAnnotation marker 的 messages。
//
// **Bug 历史**：shouldTrigger 早期版本只跳 sys + SkillBody/Listing marker，**漏跳
// StoreInjectedAnnotation**——业务 BasicAgent + WithMemory 注入大 Memory.Read
// 消息时，shouldTrigger 把 Memory 内容算进 token → 反复触发 AutoCompactRun → 但
// AutoCompactRun 内部 splitProtectedGroups + per-msg filter 抽出 Memory 不进 summary
// input → 摘要无效 → 业务白付 3-5 次 LLM 调用 → 断路器最终跳。
//
// **修法**：autocompact.go::shouldTrigger 在 conv messages 循环中加跳 Memory marker。
//
// **测试设计**：构造 messages 含一条大 Memory marker 消息 + 一条小普通消息，
// 设阈值: 总 token > 阈值（含 Memory）/ 小普通消息 token < 阈值。
//   - fix 后: shouldTrigger 跳 Memory → 估算 token < 阈值 → 返 false
//   - fix 前: 算 Memory → 估算 token > 阈值 → 返 true
func TestAutoCompact_R29LOW1_ShouldTriggerSkipsMemoryMarker(t *testing.T) {
	// 制造一个大 Memory message（每字符约 0.25 token，写 8000 字符 → ~2000 token）
	bigContent := strings.Repeat("a", 8000)
	memMsg := schema.UserMessage(bigContent)
	memMsg.Annotations = map[string]any{StoreInjectedAnnotation: true}

	smallMsg := schema.UserMessage("small q") // ~2 token

	cfg := &AutoCompactConfig{
		Enabled:             true,
		Model:               &dummyACModel{},
		ModelContextWindow:  35_000, // 阈值 = 35000 - 20000 - 13000 = 2000
		SummaryMaxTokens:    20_000,
		SummaryBufferTokens: 13_000,
		breakers:            &sync.Map{},
		breakerMaxFailures:  3,
	}
	threshold := cfg.Threshold()
	if threshold != 2000 {
		t.Fatalf("前置假设：阈值应 2000，实际 %d", threshold)
	}

	messages := []*schema.Message{memMsg, smallMsg}

	// 直接调 unexported shouldTrigger
	got := cfg.shouldTrigger(nil, messages)
	if got {
		t.Errorf("R29 视角2 LOW-1 回归：含 Memory marker 时 shouldTrigger 应返 false（跳过 Memory 后小消息 < 阈值）；实际返 true\n" +
			"sanity-revert 删 shouldTrigger 内 StoreInjectedAnnotation 跳过分支 → Memory 大内容计入 token → 触发摘要无效循环 → 测试 fail")
	}

	// 反向验证：去掉 marker 后 shouldTrigger 应返 true（确认大 Memory 内容确实超阈）
	memMsgNoMarker := schema.UserMessage(bigContent)
	got2 := cfg.shouldTrigger(nil, []*schema.Message{memMsgNoMarker, smallMsg})
	if !got2 {
		t.Fatalf("前置假设：去 marker 后大内容应触发 shouldTrigger=true（验证测试有意义）；实际 false")
	}
}
