package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 任务身份与 DetachTask 的边界：钩子改写 ctx 上的 sid 不改变任务；脱离出来的必须是另一个任务、它的
// 挂起不能从节点里原样返回；入口拒绝恒不可重试。

func dtBasic(name, reply string) agent_v2.Agent {
	return agent_v2.NewBasicAgent(agent_v2.WithName(name),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage(reply)})))
}

type dtPushSIDInAgentStart struct {
	hook.NoOpHooks
	target, sid string
}

func (h *dtPushSIDInAgentStart) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name == h.target {
		return compose_v2.WithSessionIDCtx(ctx, h.sid), nil
	}
	return ctx, nil
}

// TestTaskEntry_OnAgentStartPushedSID_MemberRuns：没配 checkpointer 的编排，OnAgentStart 钩子往 ctx 上推了
// 别的 sid（把会话 ID 当 sid 推）。任务仍是 session 的 RunID，自己的成员照常跑。
//
// 曾经编排的图拿钩子推的那个 sid 当自己的 sid，节点里认出的「所在任务」就是它：成员被判成别的任务的而
// 拒掉、整个 Run 终局失败（实测）。
func TestTaskEntry_OnAgentStartPushedSID_MemberRuns(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialHooks(&dtPushSIDInAgentStart{target: "seq", sid: "conv-42"})).
		AddStep("s1", dtBasic("worker", "worker done"))
	events, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("成员被拒：%v", err)
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content != "worker done" {
		t.Errorf("成员应照常跑完，实际产出 %v", f)
	}
}

// dtStep 是一个单节点 GraphAgent，节点体跑 body。
func dtStep(t *testing.T, body func(ctx context.Context) error) agent_v2.Agent {
	return hoGraphHost(t, "step", "call", func(ctx context.Context, _ string) (string, error) {
		if err := body(ctx); err != nil {
			return "", err
		}
		return "ok", nil
	})
}

// TestDetachTask_RejectionInsideTask_NotRetried：节点里先做一件有副作用的事，再 DetachTask 之后拿一个 RunID
// 为空的 session 调 agent——入口拒绝。外层编排配了重试，这个确定性错误不该被重试（副作用只一次），整个
// 任务终局失败。
//
// 曾经 DetachTask 之后的拒绝被当成顶层、返普通错误（可重试）：重试把扣款放大到 4 次（实测）。
func TestDetachTask_RejectionInsideTask_NotRetried(t *testing.T) {
	var charged atomic.Int32
	side := dtBasic("side", "side done")
	step := dtStep(t, func(ctx context.Context) error {
		charged.Add(1)
		_, err := side.Run(agent_v2.DetachTask(ctx), &agent_v2.RunSession{}, schema.UserMessage("x"))
		return err
	})
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialMaxRetries(3)).
		AddStep("step", step)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err == nil {
		t.Fatal("字面量空 RunID 的 session 应被拒")
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) || compose_v2.IsRetryable(err) {
		t.Errorf("在任务里脱离出来的拒绝应终局、不可重试，实际：%v", err)
	}
	if n := charged.Load(); n != 1 {
		t.Errorf("副作用应只做一次，实际 %d 次", n)
	}
}

// TestDetachTask_CurrentTaskSession_Rejected：DetachTask 之后传的正是当前任务的 session——那不是独立任务，
// 是在当前任务自己的执行里再开一遍它。入口拒。
//
// 曾经被当成新任务：清掉当前任务的 approve_always 规则、成功收尾时连当前任务挂着的兄弟存档一起删掉
// （均实测，err=nil）。
func TestDetachTask_CurrentTaskSession_Rejected(t *testing.T) {
	side := dtBasic("side", "side done")
	var sideErr error
	step := dtStep(t, func(ctx context.Context) error {
		_, sideErr = side.Run(agent_v2.DetachTask(ctx), agent_v2.GetSession(ctx), schema.UserMessage("x"))
		return sideErr
	})
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", step)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if sideErr == nil || !strings.Contains(sideErr.Error(), "脱离出来之前所在的任务") {
		t.Fatalf("应拒绝，实际 %v", sideErr)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("在任务里的拒绝应终局：%v", err)
	}
}

// TestDetachTask_ResumeCurrentTask_Rejected：DetachTask 之后续跑的正是当前任务——它正在执行，不需要也不能在
// 它自己的执行里续跑。入口拒。
//
// 曾经放行：当前任务正在跑的节点被再跑一遍（实测节点执行到第 3 次）。
func TestDetachTask_ResumeCurrentTask_Rejected(t *testing.T) {
	side := dtBasic("side", "side done")
	var resumeErr error
	step := dtStep(t, func(ctx context.Context) error {
		_, resumeErr = side.Resume(agent_v2.DetachTask(ctx), agent_v2.GetSession(ctx).RunID)
		return nil
	})
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", step)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	if resumeErr == nil || !strings.Contains(resumeErr.Error(), "脱离出来之前所在的任务") {
		t.Errorf("续跑当前任务应被拒，实际 %v", resumeErr)
	}
}

// TestDetachTask_SideTaskInterruptFromNode_Rejected：节点里 DetachTask 开的独立任务挂起了，节点把它的错原样
// 返回。当前任务答不了那个挂起——终局拒收；独立任务自己的挂起档原样留着（在顶层用它自己的 RunID 续）。
//
// 曾经当前任务把它当成自己的挂起：每次续跑独立任务都从头再问、换一个 ID，永不收敛（实测）。
func TestDetachTask_SideTaskInterruptFromNode_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	side := hoGraphHost(t, "side", "ask", func(ctx context.Context, _ string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "独立任务要人确认"})
		if err != nil {
			return "", err
		}
		return ans.Content, nil
	})
	sideSess := agent_v2.NewRunSession()
	var sideIsInterrupt bool
	step := dtStep(t, func(ctx context.Context) error {
		_, err := side.Run(agent_v2.DetachTask(ctx), sideSess, schema.UserMessage("x"))
		_, sideIsInterrupt = compose_v2.IsInterrupt(err)
		return err
	})
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", step)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	_, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if !sideIsInterrupt {
		t.Fatal("fixture 没搭对：节点体拿到的独立任务的错里应取得到中断")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("当前任务应终局拒收独立任务的挂起，实际：%v", err)
	}
	if pending, _ := cper.ListPending(context.Background(), sideSess.RunID); len(pending) == 0 {
		t.Error("独立任务自己的挂起档应原样留着")
	}
}

// TestRejectCall_TopLevel_NotRetryableNotAborted：纯顶层的入口拒绝（字面量空 RunID 的 session）不可重试，
// 但也不是 ErrRunAborted——什么都还没执行、存档原样在那儿，把调用改对再来即可。
func TestRejectCall_TopLevel_NotRetryableNotAborted(t *testing.T) {
	_, err := dtBasic("a", "x").Run(context.Background(), &agent_v2.RunSession{}, schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("字面量空 RunID 应被拒")
	}
	if compose_v2.IsRetryable(err) || errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("顶层拒绝应不可重试、不是终局失败，实际：%v", err)
	}
}

// dtBuilderHost 是 NewBasicAgentBuilder 自写 toolsFn 的宿主：串行调工具、**不**调 WithCurrentToolCallContext。
func dtBuilderHost(t *testing.T, name string, replies []*schema.Message, tk *tool.Toolkit) agent_v2.Agent {
	t.Helper()
	i := 0
	modelFn := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		if i >= len(replies) {
			return schema.AssistantMessage("[exhausted]"), nil
		}
		r := replies[i]
		i++
		return r, nil
	}
	toolsFn := func(ctx context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		out := make([]*schema.Message, 0, len(tcs))
		for _, tc := range tcs {
			args := map[string]any{}
			if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
				return nil, err
			}
			res, err := tk.Call(ctx, tc.Function.Name, args)
			if err != nil {
				return nil, err
			}
			s, _ := res.Output.(string)
			out = append(out, schema.ToolResultMessage(tc.ID, tc.Function.Name, s))
		}
		return out, nil
	}
	g, err := agent_v2.NewBasicAgentBuilder(modelFn, toolsFn).WithName(name).WithMaxRounds(4).Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(agent_v2.BasicAgentInputWrites),
		agent_v2.WithGraphOutputReader(agent_v2.BasicAgentFinalMessage))
}

// TestHistory_CustomToolsFnWithoutToolCallStamp_AgentAsToolOutputOnce：自写 toolsFn 串行调工具、没盖 tool_call
// 的章。AgentAsTool 的产出会被 tools 节点写成 tool_result，它这一层不该再补记——历史里只一份。
//
// 曾经按「在不在 tool_call 里」判补不补：自写 toolsFn 下那枚章是 false，产出记了两份（实测）。
func TestHistory_CustomToolsFnWithoutToolCallStamp_AgentAsToolOutputOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(&externalAgent{name: "helper"}))
	host := dtBuilderHost(t, "host", []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "helper", "查一下")}),
		schema.AssistantMessage("综合结论"),
	}, tk)
	ctx := agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(context.Background(), "conv-dt"), hs, "t")
	if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	n := 0
	for _, m := range loadHist(t, hs, ctx, "conv-dt") {
		if strings.Contains(m.Content, "【外部产出】查一下") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("被调方的产出应只记一份（tool_result），实际 %d 份", n)
	}
}
