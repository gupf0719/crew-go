package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
)

// nsCapturingForkSubAgent 捕获 skill_tool fork 路径透下的 sub-agent.Run ctx
// graphNS + input.Content——用于验证 ISSUE-78 audit B HIGH-1 修复后 skill_tool
// fork 路径同样 inject tc.ID 维度。同时捕获 input 防 5 类炸弹·第 5 类（mock
// 不读 input 抓不到 query 透传 bug，audit C HIGH-2 fix）。
type nsCapturingForkSubAgent struct {
	name           string
	mu             sync.Mutex
	capturedNS     []string
	capturedInputs []string
}

func (a *nsCapturingForkSubAgent) Run(ctx context.Context, _ *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.mu.Lock()
	a.capturedNS = append(a.capturedNS, compose_v2.GetGraphNS(ctx))
	if input != nil {
		a.capturedInputs = append(a.capturedInputs, input.Content)
	} else {
		a.capturedInputs = append(a.capturedInputs, "")
	}
	a.mu.Unlock()
	return []*agent_v2.AgentEvent{
		agent_v2.NewExitEvent(a.name, schema.AssistantMessage("done")),
	}, nil
}
func (a *nsCapturingForkSubAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *nsCapturingForkSubAgent) Name() string        { return a.name }
func (a *nsCapturingForkSubAgent) Description() string { return "" }

// TestISSUE78_SkillFork_SameSkillTwoActivations_DistinctChildNS 验证 ISSUE-78
// audit B HIGH-1 fix——同 LLM 轮 emit 2 个 activate_skill 同 skill 不同 query
// 时，fork 路径 childNS 通过 tc.ID hash → JoinFanoutNS 物理隔离。
//
// **bug 历史**: skill_tool.go::runForkMode 原 `childNS := JoinNS(parentNS, subAgentNSName)`
// 仅 skill name 派生——同 skill 多 activate 共用同 childNS，与 ISSUE-78 AgentAsTool
// 完全相同的 cp 互覆问题。当前 toolsFn 串行 + early return 兜底未触发,未来 fan-out
// 化时变 HIGH。
//
// **fix**: skill_tool.go::runForkMode 同 AgentAsTool 协议——子 ns 经 nestedCallNS 派生，
// tool_call 维哈希进 `#`：parentNS/__skillfork_<name>#<hash>。
func TestISSUE78_SkillFork_SameSkillTwoActivations_DistinctChildNS(t *testing.T) {
	cap := &nsCapturingForkSubAgent{name: "skill_research"}

	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研专家", Context: "fork", Agent: "research_agent"},
		},
		activations: map[string]string{"research": "你是调研专家。"},
	}
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "research_agent" {
			return cap, nil
		}
		return nil, nil
	}

	// stub model: 一轮 emit 2 个同 skill activate_skill (不同 query) → 末轮 final
	tc1 := schema.ToolCall{ID: "tc_a_111", Type: "function", Function: schema.FunctionCall{
		Name: "activate_skill", Arguments: `{"skill":"research","query":"调研话题A"}`,
	}}
	tc2 := schema.ToolCall{ID: "tc_b_222", Type: "function", Function: schema.FunctionCall{
		Name: "activate_skill", Arguments: `{"skill":"research","query":"调研话题B"}`,
	}}
	stubModel := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{tc1, tc2}),
		schema.AssistantMessage("综合两 query 结果"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(stubModel),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithMaxRounds(5),
	)

	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()

	if len(cap.capturedNS) != 2 {
		t.Fatalf("sub-agent 应被调 2 次，实际 %d 次 (capturedNS=%v)", len(cap.capturedNS), cap.capturedNS)
	}

	ns1 := cap.capturedNS[0]
	ns2 := cap.capturedNS[1]

	// **核心断言**: 两次同 skill activate 的 graphNS 应不同 (tc.ID hash 不同)
	if ns1 == ns2 {
		t.Errorf("ISSUE-78 skill_fork fix: 同 LLM 轮 2 个同 skill activate_skill 应有 distinct childNS;\n"+
			"实际两次 graphNS 相同: %q\n"+
			"sanity-revert: skill_tool.go runForkMode 删 GetCurrentToolCallID 路径 → 两次 NS 相同 → 测试 fail",
			ns1)
	}

	// 验证两次 NS 都含 __skillfork_research base + fanout `#` 后缀
	for i, ns := range []string{ns1, ns2} {
		if !strings.Contains(ns, "__skillfork_research") || !strings.Contains(ns, "#") {
			t.Errorf("ISSUE-78 skill_fork fix: childNS[%d] 应含 __skillfork_research + '#' 后缀;实际 %q", i, ns)
		}
	}

	// **audit C HIGH-2 fix**: 验 query 透传到 sub-agent (5 类炸弹·第 5 类防御)。
	// args 含 query="调研话题A" / "调研话题B" — sub-agent.Run 收到的 input.Content
	// 应分别匹配。**sanity-revert**: 把 skill_tool.go::runForkMode 的
	// `schema.UserMessage(query)` 改 `schema.UserMessage("")` → 测试 fail。
	if len(cap.capturedInputs) != 2 {
		t.Fatalf("expected 2 captured inputs, got %d", len(cap.capturedInputs))
	}
	gotA := cap.capturedInputs[0] == "调研话题A" || cap.capturedInputs[1] == "调研话题A"
	gotB := cap.capturedInputs[0] == "调研话题B" || cap.capturedInputs[1] == "调研话题B"
	if !gotA || !gotB {
		t.Errorf("ISSUE-78 audit C HIGH-2 fix: skill_tool fork 应透 args[query] 到 sub-agent;\n"+
			"实际 capturedInputs=%v (期望含 '调研话题A' + '调研话题B')", cap.capturedInputs)
	}
}

// **audit C HIGH-3 删除**: 原 TestISSUE78_SkillFork_FallbackBehavior 命名误导——
// 注释声称测 caller 不经 BasicAgent toolsFn 的 fallback 路径,但实际用 parent.Run()
// 经 BasicAgent toolsFn 必 inject tc.ID 走 hash 路径,与 fallback 协议无关。真正
// fallback 路径需直接调 *skillTool.Call(),但 *skillTool 是 internal type 业务调
// 不到——fallback 分支是死代码。
//
// audit C HIGH-3 fix 同步删 skill_tool.go runForkMode 的 `else { JoinNS }` 死分支
// (CLAUDE.md "项目未上线不留兼容路径")——activate_skill 走 BasicAgent.makeToolsFn
// 唯一路径 GetCurrentToolCallID 必非空。

// TestISSUE78_SkillFork_NToolCalls_AllDistinctNS 验证 audit C MED-3 — 同 LLM 轮
// emit N=10 个同 skill activate_skill 都拿独立 NS（没有 hash collision）。
//
// **bomb 防御**: 用 map 去重验 capturedNS 全 distinct,任两 hash collision 即 fail。
// 当前 fnv32a + tc.ID 唯一,N=10 collision 概率 2.10e-08 — 实际几乎不可能 fail。
// 若 fix 把 hash 函数 regress（如改成 `len(tcID) % 4` 等弱 hash）→ 多个 ns 重叠 → fail。
func TestISSUE78_SkillFork_NToolCalls_AllDistinctNS(t *testing.T) {
	cap := &nsCapturingForkSubAgent{name: "skill_research"}

	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork", Agent: "research_agent"},
		},
		activations: map[string]string{"research": "你是调研专家。"},
	}
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "research_agent" {
			return cap, nil
		}
		return nil, nil
	}

	// 同 LLM 轮 emit 10 个同 skill 不同 query 的 activate_skill
	tcs := make([]schema.ToolCall, 0, 10)
	for i := 0; i < 10; i++ {
		tcs = append(tcs, schema.ToolCall{
			ID:   fmt.Sprintf("tc_%d_unique_id", i),
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "activate_skill",
				Arguments: fmt.Sprintf(`{"skill":"research","query":"q_%d"}`, i),
			},
		})
	}
	stubModel := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage(tcs),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(stubModel),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithMaxRounds(5),
	)
	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.capturedNS) != 10 {
		t.Fatalf("expected 10 captures, got %d", len(cap.capturedNS))
	}
	uniq := make(map[string]int)
	for _, ns := range cap.capturedNS {
		uniq[ns]++
	}
	if len(uniq) != 10 {
		t.Errorf("ISSUE-78 audit C MED-3 fix: 10 个同 skill activate_skill 应有 10 distinct childNS;\n"+
			"实际 %d unique NS (collisions=%v)\n"+
			"sanity-revert: 改 hash 为弱 hash (如 %% 4) → 多 ns 重叠 → 测试 fail",
			len(uniq), uniq)
	}
}
