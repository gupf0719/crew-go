package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 「这次 Run 在不在一个正在执行的任务里」由两件事定：节点执行范围与任务章（编排入口、嵌套调用的
// 执行器盖）——见 agent_v2 的 enclosingTask。在任务里时 session 的 RunID 必须属于这个任务，对不上
// 入口直接拒；开独立任务是显式动作（DetachTask）。任务的身份是 session 的 RunID，ctx 上推的 sid 只是
// 它的副本：下面的用例都**不**替新会话推 sid，那是业务最自然的写法。

// ── 嵌套调用的被调方属于调用方的任务，调用方是谁都一样 ─────────────────────────────

// customTopAgent 是业务自研的顶层 agent（不是九个编排之一），在自己的 Run 里经 RunSubAgent 调
// 子 agent；续跑是业务自己的写法：把答案放进 ctx、重跑自己的逻辑。
type customTopAgent struct{ inner agent_v2.Agent }

func (c *customTopAgent) Name() string        { return "custom" }
func (c *customTopAgent) Description() string { return "custom" }
func (c *customTopAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return agent_v2.RunSubAgent(agent_v2.WithSession(ctx, sess), "inner", c.inner, in)
}
func (c *customTopAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("custom: 续跑走 Run + ResumeContext")
}

func askingTwoNodeAgent(t *testing.T, name string, firsts *countLog) agent_v2.Agent {
	return newTwoNodeAgent(t, name,
		func(_ context.Context, c string) (string, error) { firsts.inc(c); return c, nil },
		func(ctx context.Context, c, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "ok?"})
			if err != nil {
				return "", err
			}
			return c + ":" + ans.Content, nil
		})
}

func answerCtx(ctx context.Context, id, answer string) context.Context {
	return compose_v2.WithResumeContextValue(ctx, &compose_v2.ResumeContext{
		UserInputs: map[string]*schema.Message{id: schema.UserMessage(answer)}})
}

// TestRunSubAgent_UnderCustomTopLevelAgent_ResumesOwnCheckpoint：业务自研的顶层 agent 经 RunSubAgent
// 调子 agent，子 agent 中断；业务带着答案重跑自己的逻辑。子 agent 必须续上自己的存档（第一个节点
// 不重跑、不换身份重问）——被当成新任务的话每次续跑都推「新开」信号、从头跑、换一个 ID 重问，
// 永远续不上。
func TestRunSubAgent_UnderCustomTopLevelAgent_ResumesOwnCheckpoint(t *testing.T) {
	firsts := &countLog{}
	custom := &customTopAgent{inner: askingTwoNodeAgent(t, "inner", firsts)}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	in := schema.UserMessage("task")
	_, err := custom.Run(ctx, sess, in)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断，got %v", err)
	}
	evs, err := custom.Run(answerCtx(ctx, ie.Query.InterruptID, "yes"), sess, in)
	if err != nil {
		t.Fatalf("续跑应收敛，got %v（第一个节点执行 %d 次）", err, firsts.get("task"))
	}
	if got := nestedFinal(evs); got != "task:yes" {
		t.Errorf("final = %q，want task:yes", got)
	}
	if n := firsts.get("task"); n != 1 {
		t.Errorf("子 agent 应续上自己的存档（第一个节点只执行 1 次），实际 %d 次", n)
	}
}

// TestAgentAsTool_CalledOutsideFramework_Resumes：业务在框架外直接调 AgentAsTool.Call（接到自己的
// LLM 循环、MCP 服务里），被调方中断；业务把答案放进 ctx 再调一次同一个调用，必须续上。
func TestAgentAsTool_CalledOutsideFramework_Resumes(t *testing.T) {
	firsts := &countLog{}
	at := agent_v2.AgentAsTool(askingTwoNodeAgent(t, "inner", firsts), agent_v2.WithAgentToolDescription("inner"))
	sess := agent_v2.NewRunSession()
	ctx := agent_v2.WithSession(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), sess)
	_, err := at.Call(ctx, map[string]any{"input": "task"})
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断，got %v", err)
	}
	res, err := at.Call(answerCtx(ctx, ie.Query.InterruptID, "yes"), map[string]any{"input": "task"})
	if err != nil {
		t.Fatalf("续跑应收敛，got %v（第一个节点执行 %d 次）", err, firsts.get("task"))
	}
	if got := resultText(res); got != "task:yes" {
		t.Errorf("结果 = %q，want task:yes", got)
	}
	if n := firsts.get("task"); n != 1 {
		t.Errorf("被调方应续上自己的存档（第一个节点只执行 1 次），实际 %d 次", n)
	}
}

// graphStartCallHook 在（顶层）图的 OnGraphStart 里经 RunSubAgent 调一个子 agent，失败只记下不上抛。
type graphStartCallHook struct {
	compose_v2.NoOpOrchestrationHooks
	sub agent_v2.Agent
}

func (h *graphStartCallHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	_, _ = agent_v2.RunSubAgent(ctx, "prep", h.sub, schema.UserMessage("预处理")) // 预处理失败不挡主流程
	return ctx, nil
}

// TestRunSubAgent_InTopLevelGraphHook_DirectCompose_ResumesOwnCheckpoint：业务直接跑 compose_v2 图（没有
// agent_v2 编排入口），图的 OnGraphStart 经 RunSubAgent 调的子 agent 第二个节点失败了一次；图随后
// 在提问处中断。续跑时 OnGraphStart 再触发，那次调用必须续上子 agent 的失败存档（第一个节点不重跑）
// ——顶层图的钩子相位没有编排入口的任务章，也不在节点执行范围里，只有嵌套调用执行器替被调方盖的
// 那枚章说明它属于这张图的任务。
func TestRunSubAgent_InTopLevelGraphHook_DirectCompose_ResumesOwnCheckpoint(t *testing.T) {
	firsts := &countLog{}
	var failed atomic.Bool
	sub := newTwoNodeAgent(t, "prep",
		func(_ context.Context, c string) (string, error) { firsts.inc(c); return c, nil },
		func(_ context.Context, c, _ string) (string, error) {
			if failed.CompareAndSwap(false, true) {
				return "", errors.New("下游抖了一下")
			}
			return c + ":done", nil
		})
	b := compose_v2.NewBuilder()
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := func(extra ...compose_v2.RunOption) []compose_v2.RunOption {
		return append([]compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("direct-graph"),
			compose_v2.WithHooks(&graphStartCallHook{sub: sub})}, extra...)
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts()...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在提问上，got %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts(
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}))...); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := firsts.get("预处理"); n != 1 {
		t.Errorf("续跑时 OnGraphStart 里那次调用应续上子 agent 的存档（第一个节点只执行 1 次），实际 %d 次", n)
	}
}

// ── 换了会话就是换了任务 ───────────────────────────────────────────────────────────

func approvalWorker(name string, rt *recordTool) agent_v2.Agent {
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(name), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"), schema.AssistantMessage("done"),
			oneToolCall("transfer_money"), schema.AssistantMessage("done"),
		})),
	)
}

// grantApproveAlways 跑一个任务：审批中断 → 答 approve_always → 执行一次。
func grantApproveAlways(t *testing.T, ctx context.Context, ag agent_v2.Agent, sess *agent_v2.RunSession, rt *recordTool) {
	t.Helper()
	_, err := ag.Run(ctx, sess, schema.UserMessage("转 5000"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应抛审批中断，got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)})); err != nil {
		t.Fatalf("任务 A Resume: %v", err)
	}
	if got := rt.calls.Load(); got != 1 {
		t.Fatalf("前提：任务 A 应执行 1 次，got %d", got)
	}
}

// hookCtxCapture 把名为 top 的 agent 在 OnAgentStart / OnAgentEnd 拿到的 ctx 留下来。
type hookCtxCapture struct {
	hook.NoOpHooks
	top            string
	mu             sync.Mutex
	startCtx, endC context.Context
}

func (h *hookCtxCapture) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name == h.top {
		h.mu.Lock()
		h.startCtx = ctx
		h.mu.Unlock()
	}
	return ctx, nil
}

func (h *hookCtxCapture) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if info.Name == h.top {
		h.mu.Lock()
		h.endC = ctx
		h.mu.Unlock()
	}
	return ctx
}

// requireRejectedNotInTask 断言一次 Run 因「session 不属于所在的任务」被入口拒掉：非中断、不可重试、
// 正文给出路（DetachTask）。
func requireRejectedNotInTask(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("应被入口拒掉（session 不属于所在的任务），got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("应是拒绝不是中断：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("嵌套位置上的拒绝应 wrap ErrRunAborted（确定性错误，不重试）：%v", err)
	}
	if !strings.Contains(err.Error(), "DetachTask") {
		t.Errorf("正文应给出路（DetachTask）：%v", err)
	}
}

// TestEnterRunTask_CapturedHookCtxOnOtherSession_RejectedUnlessDetached：业务留下顶层 agent X 钩子里的
// ctx，X 跑完之后拿它给另一个会话 Y 开任务。那个 ctx 上带着 X 的任务章：直接用 ⟹ 入口拒（Y 的
// session 不属于 X 那个任务），Y 一步也没跑；先 DetachTask ⟹ Y 的一个独立的新任务，Y 上一个任务答过
// 的 approve_always 不生效（重新要审批）。
func TestEnterRunTask_CapturedHookCtxOnOtherSession_RejectedUnlessDetached(t *testing.T) {
	for _, which := range []string{"start", "end"} {
		t.Run(which, func(t *testing.T) {
			root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			h := &hookCtxCapture{top: "x"}
			x := agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithHooks(h),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x done")})))
			if _, err := x.Run(root, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
				t.Fatalf("x: %v", err)
			}
			captured := h.startCtx
			if which == "end" {
				captured = h.endC
			}
			rt := &recordTool{name: "transfer_money"}
			y := approvalWorker("y", rt)
			sessY := agent_v2.NewRunSession()
			grantApproveAlways(t, root, y, sessY, rt)
			sessY.NextTask()
			_, err := y.Run(captured, sessY, schema.UserMessage("任务2"))
			requireRejectedNotInTask(t, err)
			if n := rt.calls.Load(); n != 1 {
				t.Errorf("被拒的那次不该执行任何东西，转账执行次数 %d（应仍为 1）", n)
			}
			_, err = y.Run(agent_v2.DetachTask(captured), sessY, schema.UserMessage("任务2"))
			if _, isInt := compose_v2.IsInterrupt(err); !isInt || rt.calls.Load() != 1 {
				t.Errorf("DetachTask 之后 Y 的新任务应重新要审批（中断、不执行），got err=%v 执行次数=%d", err, rt.calls.Load())
			}
		})
	}
}

// TestEnterRunTask_CapturedResumeHookCtxOnOtherSession_DetachedRunsOwnInput：同上，但 X 是续跑收尾的、
// 留下的是续跑时钩子的 ctx（不带「新开」信号）；Y 上一个任务停在中断被放弃。直接用那个 ctx ⟹ 拒；
// DetachTask 之后 Y 的新任务从头跑自己的输入（不被上个任务的存档吞掉）。
func TestEnterRunTask_CapturedResumeHookCtxOnOtherSession_DetachedRunsOwnInput(t *testing.T) {
	for _, which := range []string{"start", "end"} {
		t.Run(which, func(t *testing.T) {
			root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			h := &hookCtxCapture{top: "x"}
			x := agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithHooks(h), agent_v2.WithMaxRounds(4),
				agent_v2.WithToolkit(entryPauseToolkit()),
				agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("pause"), schema.AssistantMessage("x done")})))
			sessX := agent_v2.NewRunSession()
			_, err := x.Run(root, sessX, schema.UserMessage("hi"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("x 应中断: %v", err)
			}
			if _, err := x.Resume(root, sessX.RunID, agent_v2.WithResumeSession(sessX),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("go")})); err != nil {
				t.Fatalf("x resume: %v", err)
			}
			captured := h.startCtx
			if which == "end" {
				captured = h.endC
			}
			firsts := &countLog{}
			y := askingTwoNodeAgent(t, "y", firsts)
			sessY := agent_v2.NewRunSession()
			if _, err := y.Run(root, sessY, schema.UserMessage("Y1")); err == nil {
				t.Fatalf("Y1 应停在中断上")
			}
			_, err = y.Run(captured, sessY, schema.UserMessage("Y2"))
			requireRejectedNotInTask(t, err)
			if n := firsts.get("Y2"); n != 0 {
				t.Errorf("被拒的那次不该跑任何节点，Y2 的第一个节点执行了 %d 次", n)
			}
			_, _ = y.Run(agent_v2.DetachTask(captured), sessY, schema.UserMessage("Y2"))
			if n := firsts.get("Y2"); n != 1 {
				t.Errorf("DetachTask 之后 Y 的新任务的输入应进图（Y2 的第一个节点执行 1 次），实际 %d 次——被上个任务的存档吞掉", n)
			}
		})
	}
}

// TestEnterRunTask_NodeRunsOtherSession_RejectedUnlessDetached：X 的一个工具里（节点执行范围内）拿另一个
// 会话 Y 的 session 调 Y。节点执行范围说明「这里是 X 那张图的任务」，Y 的 RunID 不属于它：直接调 ⟹
// 拒；DetachTask 之后是 Y 的一个独立任务，Y 上一个任务的 approve_always 不生效（没 NextTask 也一样）。
func TestEnterRunTask_NodeRunsOtherSession_RejectedUnlessDetached(t *testing.T) {
	for _, detach := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "detached"}[detach], func(t *testing.T) {
			root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			rt := &recordTool{name: "transfer_money"}
			y := approvalWorker("y", rt)
			sessY := agent_v2.NewRunSession()
			grantApproveAlways(t, root, y, sessY, rt)
			// 不 NextTask：Y 的规则只能靠「这次 Run 是新任务」丢掉
			var yErr error
			tk := agent_v2_newToolkit()
			tk.Register(tool.NewFuncTool("kick", "kick", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					if detach {
						ctx = agent_v2.DetachTask(ctx)
					}
					_, yErr = y.Run(ctx, sessY, schema.UserMessage("任务2"))
					return tool.Success("kicked"), nil
				}))
			x := agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
				agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("kick"), schema.AssistantMessage("x done")})))
			if _, err := x.Run(root, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
				t.Fatalf("x: %v", err)
			}
			if !detach {
				requireRejectedNotInTask(t, yErr)
				if n := rt.calls.Load(); n != 1 {
					t.Errorf("被拒的那次不该执行任何东西，转账执行次数 %d（应仍为 1）", n)
				}
				return
			}
			if _, isInt := compose_v2.IsInterrupt(yErr); !isInt || rt.calls.Load() != 1 {
				t.Errorf("Y 的独立任务应重新要审批（中断、不执行），got err=%v 执行次数=%d", yErr, rt.calls.Load())
			}
		})
	}
}

// TestTask_NextTaskWithPinnedCtx_StartsFresh：业务把会话挂进一个长寿 ctx（`WithSession` 只做一次），
// 任务 A 经 RunSubAgent 调子 agent、停在提问上被放弃，`NextTask` 之后用同一个 ctx、同一份输入开任务
// B。任务 B 必须从头跑：ctx 上钉着的是任务 A 的 sid，按它认任务的话 B 接着跑 A 的存档——新任务的
// 输入没进图，接着问 A 的问题（实测）。
func TestTask_NextTaskWithPinnedCtx_StartsFresh(t *testing.T) {
	root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	firsts := &countLog{}
	inner := askingTwoNodeAgent(t, "inner", firsts)
	sess := agent_v2.NewRunSession()
	ctx := agent_v2.WithSession(root, sess)
	_, err := agent_v2.RunSubAgent(ctx, "inner", inner, schema.UserMessage("同一个问题"))
	ie1, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应停在提问上，got %v", err)
	}
	sess.NextTask()
	_, err = agent_v2.RunSubAgent(ctx, "inner", inner, schema.UserMessage("同一个问题"))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 B 应停在自己的提问上，got %v", err)
	}
	if n := firsts.get("同一个问题"); n != 2 {
		t.Errorf("任务 B 应从头跑（第一个节点第 2 次执行），实际 %d 次——接着跑了任务 A 的存档", n)
	}
	if ie1.Query.InterruptID == ie2.Query.InterruptID {
		t.Errorf("任务 B 的提问跟任务 A 的是同一个 ID——给 A 的答案会被 B 消费")
	}
}

// tcMsg 造一条只含一个 tool_call 的 assistant 消息。
func tcMsg(id, name string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: id, Type: "function", Function: schema.FunctionCall{Name: name, Arguments: `{"x":1}`}},
	})
}

// TestApproval_RulesArePerTask_SharedValuesBothDirections：父任务里 worker（经 RunSubAgent 调）答过 approve_always；
// 父 agent 的一个工具里用半共享子 session（values 跟父共用）+ NextTask、经 DetachTask 让**同一个** worker 开一个旁路
// 任务。两个方向都要成立：旁路任务拿不到父任务的规则（同参转账照样要审批），旁路任务开始时也不能把父任务的规则
// 清掉（父任务里 worker 再做一次同参转账不再问）。规则按 session 放一份时这两个方向只能顾一头。
//
// 答 approve_always 的与旁路任务里被查的必须是同一个实例：规则只对答应它的那个 agent 生效，换一个实例时第一个方向
// 本来就成立，任务维坏了这条也照样绿（审计实测：曾经旁路任务跑的是另一个实例）。
func TestApproval_RulesArePerTask_SharedValuesBothDirections(t *testing.T) {
	root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	rt := &recordTool{name: "transfer_money"}
	wtk := agent_v2_newToolkit()
	wtk.Register(rt)
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithToolkit(wtk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"), schema.AssistantMessage("grant done"), // 父任务：问、答 approve_always、执行
			oneToolCall("transfer_money"),                                        // 旁路任务：要再问（停在中断上、放弃）
			oneToolCall("transfer_money"), schema.AssistantMessage("again done"), // 父任务：规则还在，不再问
		})))
	var sideErr error
	tk := agent_v2_newToolkit()
	tk.Register(tool.NewFuncTool("grant", "转账", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, err := agent_v2.RunSubAgent(ctx, "worker", worker, schema.UserMessage("转 5000")); err != nil {
				return nil, err
			}
			return tool.Success("grant ok"), nil
		}))
	tk.Register(tool.NewFuncTool("side", "旁路", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			child := agent_v2.NewSharedChildSession(agent_v2.GetSession(ctx))
			child.NextTask()
			_, sideErr = worker.Run(agent_v2.DetachTask(ctx), child, schema.UserMessage("旁路"))
			return tool.Success("side ok"), nil
		}))
	tk.Register(tool.NewFuncTool("again", "再转一次", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, err := agent_v2.RunSubAgent(ctx, "worker", worker, schema.UserMessage("再转 5000")); err != nil {
				return nil, err
			}
			return tool.Success("again ok"), nil
		}))
	p := agent_v2.NewBasicAgent(agent_v2.WithName("p"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(8),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			tcMsg("g1", "grant"), tcMsg("s1", "side"), tcMsg("a1", "again"), schema.AssistantMessage("p done"),
		})))
	sess := agent_v2.NewRunSession()
	_, err := p.Run(root, sess, schema.UserMessage("转两次"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("worker 第一次转账应要审批，got %v", err)
	}
	_, err = p.Resume(root, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)}))
	if err != nil {
		t.Errorf("父任务里 worker 答过 approve_always，再做同参转账不该再问（旁路任务把父任务的规则清掉了）：%v", err)
	}
	if n := rt.calls.Load(); n != 2 {
		t.Errorf("父任务里的两次转账应都执行、旁路任务里的不执行，实际执行 %d 次", n)
	}
	if _, isInt := compose_v2.IsInterrupt(sideErr); !isInt {
		t.Errorf("旁路任务不该拿到父任务的规则（应要审批）：err=%v", sideErr)
	}
}

// TestTask_BareRunWithNewSessionInNode_IsRejected：直接跑 compose_v2 图、节点里裸调
// `sub.Run(ctx, NewRunSession(), ...)`。新 session 不属于那张图的任务 ⟹ 入口拒，子 agent 一步也不跑、
// 那张图终局失败（ErrRunAborted，存档清掉）。当成新任务读的话父节点每次重跑都新开一个，子 agent
// 问两个问题就永远答不完（实测活锁、每轮留一份孤儿存档）。同一个位置换成 RunSubAgent（被调方属于
// 调用方的任务、有自己的槽）一次续跑就收敛。
func TestTask_BareRunWithNewSessionInNode_IsRejected(t *testing.T) {
	for _, viaHelper := range []bool{false, true} {
		t.Run(map[bool]string{false: "bare_run", true: "run_subagent"}[viaHelper], func(t *testing.T) {
			firsts := &countLog{}
			sub := askingTwoNodeAgent(t, "sub", firsts)
			b := compose_v2.NewBuilder()
			b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				var err error
				if viaHelper {
					_, err = agent_v2.RunSubAgent(ctx, "sub", sub, schema.UserMessage("go"))
				} else {
					_, err = sub.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
				}
				return nil, err
			})
			b.AddEdge(compose_v2.StartNode, "host")
			b.AddEdge("host", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			cper := compose_v2.NewInMemoryCheckpointer()
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("g")}
			err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
			if !viaHelper {
				requireRejectedNotInTask(t, err)
				if n := firsts.get("go"); n != 0 {
					t.Errorf("被拒的子 agent 不该跑任何节点，第一个节点执行了 %d 次", n)
				}
				if pend, _ := cper.ListPending(context.Background(), "g"); len(pend) != 0 {
					t.Errorf("终局失败应清掉那张图的存档，还剩 %d 份", len(pend))
				}
				return
			}
			ie1, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在子 agent 的提问上，got %v", err)
			}
			err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts,
				compose_v2.WithResumeAnswers(map[string]*schema.Message{ie1.Query.InterruptID: schema.UserMessage("好")}))...)
			if err != nil || firsts.get("go") != 1 {
				t.Errorf("RunSubAgent 一次续跑就该收敛、子 agent 不从头跑：err=%v 第一个节点执行 %d 次", err, firsts.get("go"))
			}
		})
	}
}

// TestRun_LiteralSessionWithoutRunID_IsRejected：业务传字面量 session（`&RunSession{}`，RunID 为空）。
// RunID 是任务的身份、存档的主索引，框架替它补就得改业务的对象（同一个字面量被两个 goroutine 同时用时
// 那一写就是 data race）⟹ 入口拒，什么都不执行。顶层拒绝不 wrap ErrRunAborted——什么都还没发生，
// 改对了再来一次即可。Resume 带一个 RunID 为空的 session 同样拒，存档原样在，改对之后续得上。
func TestRun_LiteralSessionWithoutRunID_IsRejected(t *testing.T) {
	root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	firsts := &countLog{}
	ag := askingTwoNodeAgent(t, "a", firsts)
	_, err := ag.Run(root, &agent_v2.RunSession{}, schema.UserMessage("q"))
	if err == nil || !strings.Contains(err.Error(), "RunID 为空") {
		t.Fatalf("RunID 为空的 session 应被入口拒，got %v", err)
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("顶层拒绝不该 wrap ErrRunAborted（没有终局失败这回事）：%v", err)
	}
	if n := firsts.get("q"); n != 0 {
		t.Errorf("被拒的那次不该跑任何节点，实际 %d 次", n)
	}

	sess := agent_v2.NewRunSession()
	_, err = ag.Run(root, sess, schema.UserMessage("q"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在提问上，got %v", err)
	}
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")})
	_, err = ag.Resume(root, sess.RunID, agent_v2.WithResumeSession(&agent_v2.RunSession{}), answer)
	if err == nil || errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("Resume 带 RunID 为空的 session 应被拒（不 wrap ErrRunAborted），got %v", err)
	}
	if _, err := ag.Resume(root, sess.RunID, agent_v2.WithResumeSession(sess), answer); err != nil {
		t.Fatalf("存档应原样在、改对之后续得上：%v", err)
	}
	if n := firsts.get("q"); n != 1 {
		t.Errorf("续跑应续上存档（第一个节点只执行 1 次），实际 %d 次", n)
	}
}
