package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// R27a MED-2 + R29 视角4/5 HIGH-1 历史回填测试集——补 7 编排 .Resume NS forward
// 对称漏修。
//
// **背景**：R27a 1a70f89 commit 修了 BasicAgent.Resume forward 父 graphNS，但 0 测试改动。
// R29 afa6c33 commit 修了 6 编排（Sequential / Loop / Parallel / Handoff / Reflect /
// Routing）.Resume forward graphNS，也是 0 测试。共 **7 个 fix 裸跑无回归测试**。
//
// 业务场景: caller 持子编排 ref 直接调子.Resume(parentSID) 时 ctx 含父推 graphNS——
// fix 前 cfg.graphNS="" → LoadNS(sid, "") 拿父 cp 而非自己的 cp → GraphID 不一致
// 报 ErrGraphIDConflict 或 cm.Restore "channel not registered"。
//
// **测试设计**（套用 R31 GraphAgent.Resume NS forward 测试模板 r31_graph_resume_test.go）:
//   - 第一次 Run 子编排 + parent ns ctx → 子内抛 InterruptError → cp 存到 (sid, parentNS)
//   - 注入 fake_parent_graph_id 伪父 cp 在 (sid, "")
//   - 调子编排.Resume(parentSID) + 同 parent ns ctx
//   - fix 前: auto-load LoadNS(sid, "") 拿 fake_parent → GraphID 冲突报错
//   - fix 后: forward ns → LoadNS(sid, parentNS) 拿到正确 cp → 跑通

// interruptOnceAgent 第一次 Run 抛 InterruptError，Resume 后拿 GetResumeAnswer 续跑。
// 用于模拟 sub-agent 中抛中断的最简实现。
type interruptOnceAgent struct {
	name string
}

func (a *interruptOnceAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	// 算稳定 ID + 检查 resume answer
	stableID := compose_v2.CurrentInterruptSite(ctx).InterruptID()
	if ansMsg, ok := compose_v2.GetResumeAnswer(ctx, stableID); ok && ansMsg != nil {
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage("done:"+ansMsg.Content))}, nil
	}
	return nil, &compose_v2.InterruptError{
		Reason: "test_ask",
		Query:  &compose_v2.AskQuery{Question: "go on?", InterruptID: stableID},
	}
}
func (a *interruptOnceAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *interruptOnceAgent) Name() string        { return a.name }
func (a *interruptOnceAgent) Description() string { return "" }

// runFirstThenResume 通用流程: 第一次 Run + parent ns → 拿 interruptID → 注入 fake
// 父 cp → Resume → 验证不报 GraphID 冲突。返回 final + err 让 caller 自定义断言。
func runFirstThenResume(
	t *testing.T,
	parent string,
	run func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error),
	resume func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error),
) {
	t.Helper()
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ctx = compose_v2.WithGraphNS(ctx, parent)
	sess := agent_v2.NewRunSession()

	// 第一次 Run 抛 interrupt
	_, err := run(ctx, sess)
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expected interrupt, got %v", err)
	}

	// 注入伪父 cp 在 (sid, "") 触发 GraphID 冲突陷阱
	fakeParentCP := &compose_v2.Checkpoint{
		GraphID: "fake_parent_graph_id",
		Status:  compose_v2.CPStatusPending,
	}
	if err := cper.SaveNS(ctx, sess.RunID, "", fakeParentCP); err != nil {
		t.Fatalf("SaveNS fake parent: %v", err)
	}

	answers := map[string]*schema.Message{
		ie.Query.InterruptID: schema.UserMessage("yes"),
	}
	events, err := resume(ctx, sess.RunID, answers)
	if err != nil {
		t.Fatalf("Resume: %v (fix 前应报 ErrGraphIDConflict，sanity-revert 删 fix 后应触发此 fail)", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("expected final, got nil")
	}
	if !strings.Contains(final.Content, "done:") && !strings.Contains(final.Content, "yes") {
		t.Logf("final 内容（仅 info，agent 行为视编排不同）: %q", final.Content)
	}
}

// TestBasicAgent_R27aMED2_Resume_NSForward 补 R27a MED-2 BasicAgent.Resume 漏测。
//
// 仿 TestBasicAgent_HITLResume 模板:stubModel 第一轮返 tool_call、第二轮返 final；
// 工具首次调抛 InterruptError、Resume 后 GetResumeAnswerText 命中返答案。
func TestBasicAgent_R27aMED2_Resume_NSForward(t *testing.T) {
	const askID = "r27a_ask_id"
	askTool := tool.NewFuncTool(
		"ask", "ask user something",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, askID); ok {
				return tool.Success("user said: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: askID, Question: "what?"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"test"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	resp2 := schema.AssistantMessage("done:yes")
	m := newStubModel([]*schema.Message{resp1, resp2})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ba"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}

// TestSequentialAgent_R29HIGH1_Resume_NSForward 补 R29 视角4/5 HIGH-1 SequentialAgent.Resume 漏测。
func TestSequentialAgent_R29HIGH1_Resume_NSForward(t *testing.T) {
	sub := &interruptOnceAgent{name: "sub_seq"}
	ag := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("a", sub)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}

// TestLoopAgent_R29HIGH1_Resume_NSForward 补 LoopAgent.Resume 漏测。
func TestLoopAgent_R29HIGH1_Resume_NSForward(t *testing.T) {
	sub := &interruptOnceAgent{name: "sub_loop"}
	ag := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(1),
	).WithBody(sub)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}

// TestParallelAgent_R29HIGH1_Resume_NSForward 补 ParallelAgent.Resume 漏测。
func TestParallelAgent_R29HIGH1_Resume_NSForward(t *testing.T) {
	sub := &interruptOnceAgent{name: "sub_par"}
	ag := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("b1", sub)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}

// TestHandoffAgent_R29HIGH1_Resume_NSForward 补 HandoffAgent.Resume 漏测。
func TestHandoffAgent_R29HIGH1_Resume_NSForward(t *testing.T) {
	sub := &interruptOnceAgent{name: "sub_ha"}
	ag := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ha"),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", sub)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}

// TestReflectAgent_R29HIGH1_Resume_NSForward 补 ReflectAgent.Resume 漏测。
func TestReflectAgent_R29HIGH1_Resume_NSForward(t *testing.T) {
	sub := &interruptOnceAgent{name: "sub_rf"}
	ag := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("rf"),
		agent_v2.WithEvalFunc(func(_ context.Context, _ *schema.Message) (bool, error) { return true, nil }),
	).WithActor(sub)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}

// TestRoutingAgent_R29HIGH1_Resume_NSForward 补 RoutingAgent.Resume 漏测。
func TestRoutingAgent_R29HIGH1_Resume_NSForward(t *testing.T) {
	sub := &interruptOnceAgent{name: "sub_rt"}
	ag := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRouter(func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
			return "a", nil
		}),
	).AddBranch("a", sub)
	runFirstThenResume(t, "parent_x",
		func(ctx context.Context, sess *agent_v2.RunSession) ([]*agent_v2.AgentEvent, error) {
			return ag.Run(ctx, sess, schema.UserMessage("hi"))
		},
		func(ctx context.Context, sid string, answers map[string]*schema.Message) ([]*agent_v2.AgentEvent, error) {
			return ag.Resume(ctx, sid, agent_v2.WithResumeUserInputs(answers))
		},
	)
}
