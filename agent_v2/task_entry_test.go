package agent_v2_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 「这次 Run 是不是一个新任务」在编排的 Run 入口、`OnAgentStart` **之前**判定，判据是「上面有没有
// 一个已经建立的任务」。判定排在钩子之后时，钩子相位里发起的调用那一刻新任务还没宣告：session 里
// 还是上一个任务的审批规则、没有「新开」信号——被调方用上一个任务的 approve_always 静默执行工具，
// 或续上上一个任务被放弃时留下的存档（实测）。

// entryGuardHook 在顶层 agent 的 OnAgentStart 里经 RunSubAgent 跑一个 guard 子 agent。
type entryGuardHook struct {
	hook.NoOpHooks
	top     string
	guard   agent_v2.Agent
	enabled atomic.Bool
}

func (h *entryGuardHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if !h.enabled.Load() || info.Name != h.top {
		return ctx, nil
	}
	_, err := agent_v2.RunSubAgent(ctx, "guard", h.guard, schema.UserMessage("preflight"))
	return ctx, err
}

// TestEnterRunTask_OnAgentStartCallDoesNotInheritPreviousTaskApprovals：任务 A 里 guard 自己对 transfer_money 答了
// approve_always；同一个 session 上的任务 B，顶层的 OnAgentStart 里经 RunSubAgent 跑**同一个** guard，guard 调同一个
// 工具。guard 必须重新要审批（这里顶层钩子里的审批中断会被拒，所以表现为「没执行转账」），不能拿任务 A 的规则静默
// 执行。BasicAgent 与编排顶层两种都要对。
//
// 答 approve_always 的与后来被查的必须是同一个实例：规则只对答应它的那个 agent 生效，换一个实例时规则本来就不会
// 命中，任务维坏了这条也照样绿（审计实测：曾经任务 A 里答的是 host，这条在「规则不按任务分、新任务也不清」的变异下
// 仍然绿）。
func TestEnterRunTask_OnAgentStartCallDoesNotInheritPreviousTaskApprovals(t *testing.T) {
	for _, kind := range []string{"basic", "sequential"} {
		for _, nextTask := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/NextTask=%v", kind, nextTask), func(t *testing.T) {
				rt := &recordTool{name: "transfer_money"}
				guardTK := agent_v2_newToolkit()
				guardTK.Register(rt)
				guard := agent_v2.NewBasicAgent(
					agent_v2.WithName("guard"), agent_v2.WithToolkit(guardTK), agent_v2.WithMaxRounds(4),
					agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
					agent_v2.WithModel(newStubModel([]*schema.Message{
						oneToolCall("transfer_money"), schema.AssistantMessage("guard A ok"), // 任务 A：guard 自己答 approve_always
						oneToolCall("transfer_money"), schema.AssistantMessage("guard B ok"), // 任务 B：顶层钩子里调
					})),
				)
				hooks := &entryGuardHook{guard: guard}
				hostModel := newStubModel([]*schema.Message{schema.AssistantMessage("B 完成")})
				var top agent_v2.Agent
				if kind == "basic" {
					hooks.top = "host"
					top = agent_v2.NewBasicAgent(
						agent_v2.WithName("host"), agent_v2.WithMaxRounds(6),
						agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(hooks),
						agent_v2.WithModel(hostModel),
					)
				} else {
					hooks.top = "seq"
					step := agent_v2.NewBasicAgent(
						agent_v2.WithName("host"), agent_v2.WithMaxRounds(6),
						agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithModel(hostModel),
					)
					top = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
						agent_v2.WithSequentialHooks(hooks)).AddStep("s1", step)
				}
				ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
				sess := agent_v2.NewRunSession()
				grantApproveAlways(t, ctx, guard, sess, rt) // 任务 A：同一个 guard 实例答 approve_always、执行一次
				if got := rt.calls.Load(); got != 1 {
					t.Fatalf("前提没搭对：任务 A 应执行 1 次转账，实际 %d", got)
				}
				if nextTask {
					sess.NextTask()
				}
				hooks.enabled.Store(true)
				// 任务 B 的结局（钩子里的审批中断被拒）不是这条要断言的，断言的是转账有没有执行
				_, _ = top.Run(ctx, sess, schema.UserMessage("帮我看下这个月话费"))
				if got := rt.calls.Load(); got != 1 {
					t.Errorf("任务 B 的 guard（顶层 OnAgentStart 里经 RunSubAgent 调）用任务 A 的 approve_always "+
						"静默执行了转账（1 → %d）", got)
				}
			})
		}
	}
}

// entryEndAudit 在顶层 agent 的 OnAgentEnd 里经 RunSubAgent 跑一个会提问的 audit，并记下那一刻有没有
// 「新开」信号。OnAgentEnd 没有错误返回值，audit 的中断只能吞掉——它的挂起档留在自己的槽里。
type entryEndAudit struct {
	hook.NoOpHooks
	audit      agent_v2.Agent
	top        string
	freshAtEnd []bool
}

func (h *entryEndAudit) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if info.Name != h.top {
		return ctx
	}
	h.freshAtEnd = append(h.freshAtEnd, compose_v2.IsFreshRunCtx(ctx))
	_, _ = agent_v2.RunSubAgent(ctx, "audit", h.audit, schema.UserMessage("收尾审计"))
	return ctx
}

func entryPauseToolkit() *toolPkgToolkit {
	tk := agent_v2_newToolkit()
	tk.Register(tool.NewFuncTool("pause", "pause", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	return tk
}

// TestEnterRunTask_OnAgentEndSeesNewTaskSignal：同一个 RunID 上的第二个任务，顶层 OnAgentEnd 里发起
// 的调用必须看得到「新开」信号、不续上个任务被放弃时留下的存档。BasicAgent 曾经只在跑图的局部 ctx
// 里声明新任务，OnAgentEnd 那一层拿不到（实测续上了残档）；其余编排在 Run 层声明、拿得到——两者要
// 一致。任务 B 以中断收尾（成功收尾会清掉整棵子树，残档就看不到了）。
func TestEnterRunTask_OnAgentEndSeesNewTaskSignal(t *testing.T) {
	pause := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tcp", Type: "function", Function: schema.FunctionCall{Name: "pause", Arguments: `{}`}},
	})
	for _, kind := range []string{"basic", "sequential"} {
		t.Run(kind, func(t *testing.T) {
			firsts := &countLog{}
			audit := newTwoNodeAgent(t, "audit",
				func(_ context.Context, c string) (string, error) {
					firsts.inc(c)
					return "", nil
				},
				func(ctx context.Context, _, _ string) (string, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "审计通过吗？"})
					if err != nil {
						return "", err
					}
					return "审计:" + ans.Content, nil
				})
			hooks := &entryEndAudit{audit: audit}
			stepModel := newStubModel([]*schema.Message{schema.AssistantMessage("A"), pause})
			var top agent_v2.Agent
			if kind == "basic" {
				hooks.top = "host"
				top = agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithMaxRounds(3),
					agent_v2.WithHooks(hooks), agent_v2.WithToolkit(entryPauseToolkit()), agent_v2.WithModel(stepModel))
			} else {
				hooks.top = "seq"
				step := agent_v2.NewBasicAgent(agent_v2.WithName("step"), agent_v2.WithMaxRounds(3),
					agent_v2.WithToolkit(entryPauseToolkit()), agent_v2.WithModel(stepModel))
				top = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
					agent_v2.WithSequentialHooks(hooks)).AddStep("s1", step)
			}
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			if _, err := top.Run(ctx, sess, schema.UserMessage("任务A")); err != nil {
				t.Fatalf("任务 A 应正常收尾（audit 的中断在 OnAgentEnd 里被吞），got %v", err)
			}
			if n := firsts.get("收尾审计"); n != 1 {
				t.Fatalf("前提没搭对：任务 A 的 audit 应执行一次，got %d", n)
			}
			_, _ = top.Run(ctx, sess, schema.UserMessage("任务B"))
			if len(hooks.freshAtEnd) != 2 || !hooks.freshAtEnd[1] {
				t.Errorf("任务 B 的 OnAgentEnd 应看得到「新开」信号，got %v", hooks.freshAtEnd)
			}
			if n := firsts.get("收尾审计"); n != 2 {
				t.Errorf("任务 B 的 audit 应从头跑（first 共 2 次），实际 %d 次——续上了任务 A 被放弃时留下的存档", n)
			}
		})
	}
}

// TestNextTask_NewRunIDHasNoApprovalRules：业务在框架外（不在任何 agent / 图里）自己驱动任务，
// 任务 A 里答了 approve_always，NextTask 之后经 RunSubAgent 起任务 B——B 必须重新要审批。
//
// 经 RunSubAgent 发起的调用属于 session 当前的任务（续跑时要续得上，见 enterRunTask），它的入口不
// 会把这次调用当成新任务、也就不会在那里清规则。承重的是**规则按 RunID 分**（approvalRulesKey 里的
// RunID 那一维）：NextTask 换了 RunID，任务 B 看到的是一套新的、空的规则。把那一维去掉，任务 A 的
// approve_always 就在任务 B 里生效（转账静默执行）。
func TestNextTask_NewRunIDHasNoApprovalRules(t *testing.T) {
	rt := &recordTool{name: "transfer_money"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"), // 任务 A
			schema.AssistantMessage("A 完成"),
			oneToolCall("transfer_money"), // 任务 B——本该重新问
			schema.AssistantMessage("B 完成"),
		})),
	)
	base := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(base, sess, schema.UserMessage("转 5000"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应抛审批中断，got %v", err)
	}
	if _, err := ag.Resume(base, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)})); err != nil {
		t.Fatalf("任务 A Resume: %v", err)
	}
	sess.NextTask()
	_, err = agent_v2.RunSubAgent(agent_v2.WithSession(base, sess), "entry", ag, schema.UserMessage("看话费"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("任务 B 应重新要审批（中断），got %v", err)
	}
	if n := rt.calls.Load(); n != 1 {
		t.Errorf("任务 B 静默执行了转账（1 → %d）——任务 A 的 approve_always 泄漏过来", n)
	}
}
