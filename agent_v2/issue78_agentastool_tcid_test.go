package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// nsCapturingSubAgent 捕获 sub-agent.Run 收到的 ctx graphNS + input.Content——
// 用于验证 ISSUE-78 修复后 AgentAsTool 把 tc.ID 维度加进 childNS 让多个同名
// tool_call 拿独立 NS。同时捕获 input 防 5 类炸弹·第 5 类（mock 不读 input
// 抓不到 query/input 透传 bug，audit C HIGH-2 fix）。
type nsCapturingSubAgent struct {
	name           string
	mu             sync.Mutex
	capturedNS     []string
	capturedInputs []string
}

func (a *nsCapturingSubAgent) Run(ctx context.Context, _ *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
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
func (a *nsCapturingSubAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *nsCapturingSubAgent) Name() string        { return a.name }
func (a *nsCapturingSubAgent) Description() string { return "" }

// TestISSUE78_SameSubAgentTwoToolCalls_DistinctChildNS 验证 ISSUE-78 修——
// 同一 LLM 轮 emit 2 个调用同 sub-agent 的 tool_call (同 sub-agent 名) 时，
// childNS 通过 tc.ID hash → JoinFanoutNS 物理隔离两调用的 cp NS。
//
// **bug 历史 (ISSUE-78)**: childNS 仅 agent.Name() 派生 → 同名多 tool_call 共用
// 同 childNS → cp 互覆。当前 toolsFn 串行 + early return 兜底未触发，未来 fan-out
// 化时变 HIGH。
//
// **fix**: BasicAgent toolsFn 调 dispatchToolkit.Call 前 inject tc.ID 到 ctx
// (WithCurrentToolCallID)；AgentAsTool.Call 取 tc.ID 用 hash → JoinFanoutNS 加
// childNS。预防性修复让未来 fan-out 直接 race-free 无需架构改动。
//
// **测试设计**:
//   - 父 BasicAgent 用 stub model 一轮 emit 2 个同名 tool_call (id="tc_1", "tc_2")
//   - 同一 sub-agent 实例作 tool（同 Name）
//   - 验证 sub-agent.Run 收到 ctx graphNS 两次值不同 (tc.ID hash 不同)
func TestISSUE78_SameSubAgentTwoToolCalls_DistinctChildNS(t *testing.T) {
	cap := &nsCapturingSubAgent{name: "sub_agent"}
	tk := agent_v2_newToolkit()
	tk.Register(agent_v2.AgentAsTool(cap, agent_v2.WithAgentToolDescription("sub agent for capture")))

	// stub model: 一轮 emit 2 个同名 tool_call → 末轮 final
	tc1 := schema.ToolCall{ID: "tc_1_abc", Function: schema.FunctionCall{Name: "sub_agent", Arguments: `{"input":"first"}`}}
	tc2 := schema.ToolCall{ID: "tc_2_xyz", Function: schema.FunctionCall{Name: "sub_agent", Arguments: `{"input":"second"}`}}
	stubModel := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{tc1, tc2}),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(stubModel),
		agent_v2.WithToolkit(tk),
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

	// **核心断言**: 两次 sub-agent.Run 的 graphNS 应不同 (tc.ID hash 不同)
	ns1 := cap.capturedNS[0]
	ns2 := cap.capturedNS[1]
	if ns1 == ns2 {
		t.Errorf("ISSUE-78 fix: 同 LLM 轮 2 个同名 tool_call 应有 distinct childNS;\n"+
			"实际两次 graphNS 相同: %q\n"+
			"sanity-revert 删 toolsFn WithCurrentToolCallID inject 或 AgentAsTool.Call tc.ID 路径 → 两次 NS 相同 → 测试 fail",
			ns1)
	}

	// 验证两次 NS 都含 sub_agent base + fanout `#` 后缀
	for i, ns := range []string{ns1, ns2} {
		if !strings.Contains(ns, "sub_agent") || !strings.Contains(ns, "#") {
			t.Errorf("ISSUE-78 fix: childNS[%d] 应含 sub_agent + JoinFanoutNS '#' 后缀;实际 %q", i, ns)
		}
	}

	// **audit C HIGH-2 fix**: 验 input 透传到 sub-agent (5 类炸弹·第 5 类防御)。
	// args 含 input="first" / "second" — sub-agent.Run 收到的 *schema.Message
	// .Content 应分别匹配。**sanity-revert**: 把 agent_tool.go::Call 的
	// `schema.UserMessage(inputStr)` 改 `schema.UserMessage("")` → 测试 fail
	// 显示 "input 应分别透传 first/second"。
	if len(cap.capturedInputs) != 2 {
		t.Fatalf("expected 2 captured inputs, got %d", len(cap.capturedInputs))
	}
	gotFirst := cap.capturedInputs[0] == "first" || cap.capturedInputs[1] == "first"
	gotSecond := cap.capturedInputs[0] == "second" || cap.capturedInputs[1] == "second"
	if !gotFirst || !gotSecond {
		t.Errorf("ISSUE-78 audit C HIGH-2 fix: AgentAsTool 应透 args[input] 到 sub-agent;\n"+
			"实际 capturedInputs=%v (期望含 'first' + 'second')", cap.capturedInputs)
	}
}

// TestISSUE78_NoToolCallID_FallsBackToStaticPosition 验证 caller 不经 BasicAgent toolsFn
// 直接调 tool.Call 时（ctx 无 tc.ID），子 ns 按静态位置 + 输入派生：仍是 sub_agent 名下的一个
// `#` 实例；同一份输入算出同一个 ns（图外没有「第几次」可数，也没有节点重跑这回事），不同
// 输入各落一个槽。
func TestISSUE78_NoToolCallID_FallsBackToStaticPosition(t *testing.T) {
	cap := &nsCapturingSubAgent{name: "sub_agent"}
	tooly := agent_v2.AgentAsTool(cap, agent_v2.WithAgentToolDescription("test"))

	// 直接调 tool.Call 不经 BasicAgent toolsFn → ctx 无 tc.ID
	for _, in := range []string{"hi", "hi", "other"} {
		if _, err := tooly.Call(context.Background(), map[string]any{"input": in}); err != nil {
			t.Fatalf("Call err: %v", err)
		}
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.capturedNS) != 3 {
		t.Fatalf("expected 3 captures, got %d", len(cap.capturedNS))
	}
	for i, ns := range cap.capturedNS {
		segs, err := compose_v2.SplitNS(ns)
		if err != nil || len(segs) != 1 || segs[0].NodeID != "sub_agent" || segs[0].Kind != "fanout" {
			t.Errorf("第 %d 次调用的 ns %q 应是根下 sub_agent 的一个 # 实例（err=%v）", i+1, ns, err)
		}
	}
	if cap.capturedNS[0] != cap.capturedNS[1] {
		t.Errorf("同一份输入应算出同一个 ns：%q vs %q", cap.capturedNS[0], cap.capturedNS[1])
	}
	if cap.capturedNS[0] == cap.capturedNS[2] {
		t.Errorf("不同输入应各落一个槽，却都是 %q", cap.capturedNS[0])
	}
}

// agent_v2_newToolkit helper — wraps tool.NewToolkit.
func agent_v2_newToolkit() *toolPkgToolkit {
	return toolPkgNewToolkit()
}
