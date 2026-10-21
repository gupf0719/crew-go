package agent_v2

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

// ac76SummarizeStub 计 LLM Generate 被调次数 — 用于验证 ISSUE-76 fix 后 AutoCompact
// 真调 LLM 摘要（不再 silent 失效）。简化用 mu+int 不引 sync/atomic 依赖。
type ac76SummarizeStub struct {
	mu sync.Mutex
	c  int
}

func (s *ac76SummarizeStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	s.c++
	s.mu.Unlock()
	return schema.AssistantMessage("[summary]"), nil
}
func (s *ac76SummarizeStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *ac76SummarizeStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }
func (s *ac76SummarizeStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.c
}

// TestISSUE76_AutoCompact_ProtectedOnlyEarly_FallsBackToTriggerSummarize 验证
// ISSUE-76 修复: lastUserGroupIndex 找到的 user group 前面所有 group 都是 protected
// (mem/skill marker) 时,reset tailStartIdx 让真实 ReAct 轮组进 early summarize。
//
// **bug 历史 (ISSUE-76)**: buildInitialMessages 注入 Memory.Read 让
// GroupMessagesByAPIRound 把 Memory (无 runID) 独成 group 0,UserInput 落 group 1
// → lastUserGroupIndex 返 1 不触发 fallback → earlyGroups=[[mem]] → per-msg
// filter 抽空 → early=[] → AutoCompact 永不调 LLM 摘要 silent 失效。
//
// **fix**: AutoCompactRun 内 tailStartIdx > 0 但 [0..tailStartIdx) 全 protected
// 时 reset 到 len-1 走 fallback。
//
// **测试设计**:
//   - 构造 messages: [sys, mem(marker), user, asst1, tool1, asst2, tool2, asst3]
//   - GroupMessagesByAPIRound: groups=[[mem], [user], [asst1, tool1], ...] 多组
//   - lastUserGroupIndex 返 1 (user group 1)
//   - groups[0..1) = [[mem]] 全 protected → reset tailStartIdx = len-1
//   - early 现在含真实 ReAct 轮 → summarize 触发 LLM Generate
//   - sanity-revert 删 fix → early=[] → LLM 不被调
func TestISSUE76_AutoCompact_ProtectedOnlyEarly_FallsBackToTriggerSummarize(t *testing.T) {
	stub := &ac76SummarizeStub{}

	cfg := &AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  35_000, // threshold = 35000 - 20000 - 13000 = 2000
		SummaryMaxTokens:    20_000,
		SummaryBufferTokens: 13_000,
		breakers:            &sync.Map{},
		breakerMaxFailures:  3,
	}

	// Memory marker message (大内容触发 token 估算 > threshold)
	memContent := strings.Repeat("memory_content_word ", 500) // ~2500 token
	memMsg := schema.UserMessage(memContent)
	memMsg.Annotations = map[string]any{StoreInjectedAnnotation: true}

	// 真实 ReAct: user + 多轮 assistant — 每轮 assistant 设独立 RunID 让
	// GroupMessagesByAPIRound 分多组（这是业务真实多 ReAct 轮场景:每 LLM call
	// 一个 RunID 一组）。总 token > threshold (2000) 触发摘要。
	userMsg := schema.UserMessage("do research on X")
	mkAsst := func(content, runID string) *schema.Message {
		m := schema.AssistantMessage(content)
		m.Annotations = map[string]any{"run_id": runID}
		return m
	}
	asst1 := mkAsst(strings.Repeat("research step 1 detail ", 500), "run_1")
	asst2 := mkAsst(strings.Repeat("research step 2 detail ", 500), "run_2")
	asst3 := mkAsst(strings.Repeat("research step 3 detail ", 500), "run_3")

	messages := []*schema.Message{
		schema.SystemMessage("you are an agent"),
		memMsg,
		userMsg,
		asst1,
		asst2,
		asst3,
	}

	// 跑——stub 简化返 "[summary]" 不合 AutoCompactRun 解析 schema 让 err 非 nil
	// 但**测试目的是验证 fix 触发 LLM 调用**(early 走 fallback 拿真实 ReAct 轮);
	// stub 之后的解析失败不在本测试 scope。
	_, _ = AutoCompactRun(context.Background(), cfg, messages)

	// **核心断言** (ISSUE-76 fix): LLM Generate 被调 1 次 (实际触发 summarize)
	if calls := stub.callCount(); calls != 1 {
		t.Errorf("ISSUE-76 fix: AutoCompact 应触发 LLM 摘要 (early 走 fallback 拿真实 ReAct 轮);\n"+
			"实际 LLM 调用次数 = %d (期望 1)\n"+
			"sanity-revert 删 allProtectedGroups fallback → early=[] → LLM 不被调 → 测试 fail",
			calls)
	}
}

// TestISSUE76_AutoCompact_NoProtected_NoRegression 验证无 protected marker 时
// 正常 lastUserGroupIndex 路径不破坏 (反向回归)。
func TestISSUE76_AutoCompact_NoProtected_NoRegression(t *testing.T) {
	stub := &ac76SummarizeStub{}

	cfg := &AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  35_000,
		SummaryMaxTokens:    20_000,
		SummaryBufferTokens: 13_000,
		breakers:            &sync.Map{},
		breakerMaxFailures:  3,
	}

	// 无 Memory marker, 多 user 多轮 ReAct (典型多 turn 场景) — 总 token > threshold
	mkAsst := func(content, runID string) *schema.Message {
		m := schema.AssistantMessage(content)
		m.Annotations = map[string]any{"run_id": runID}
		return m
	}
	messages := []*schema.Message{
		schema.SystemMessage("agent"),
		schema.UserMessage("first turn user"),
		mkAsst(strings.Repeat("first answer detail ", 800), "r1"),
		schema.UserMessage("second turn user"),
		mkAsst(strings.Repeat("second answer detail ", 800), "r2"),
	}

	_, _ = AutoCompactRun(context.Background(), cfg, messages)
	// 触发 summarize (有真实 early)
	if calls := stub.callCount(); calls != 1 {
		t.Errorf("无 protected marker 时正常路径应触发 summarize 1 次;实际 %d", calls)
	}
}

// TestISSUE76_AllProtectedGroups_Helper 单元测 allProtectedGroups helper。
func TestISSUE76_AllProtectedGroups_Helper(t *testing.T) {
	memMsg := schema.UserMessage("mem")
	memMsg.Annotations = map[string]any{StoreInjectedAnnotation: true}

	skillBodyMsg := schema.UserMessage("skill body")
	skillBodyMsg.Annotations = map[string]any{SkillBodyAnnotation: true}

	plainMsg := schema.UserMessage("plain user")

	tests := []struct {
		name   string
		groups [][]*schema.Message
		want   bool
	}{
		{"empty", [][]*schema.Message{}, true},
		{"all mem", [][]*schema.Message{{memMsg}, {memMsg}}, true},
		{"mem + skill body", [][]*schema.Message{{memMsg, skillBodyMsg}}, true},
		{"all 2 markers", [][]*schema.Message{{memMsg, skillBodyMsg}}, true},
		{"含 plain", [][]*schema.Message{{memMsg}, {plainMsg}}, false},
		{"plain only", [][]*schema.Message{{plainMsg}}, false},
		{"含 nil msg (跳过)", [][]*schema.Message{{nil, memMsg}}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := allProtectedGroups(tc.groups); got != tc.want {
				t.Errorf("allProtectedGroups = %v, want %v", got, tc.want)
			}
		})
	}
}
