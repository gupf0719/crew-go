package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 从一个正在执行的任务里脱离出来的运行（agent_v2.DetachTask / compose_v2.DetachExecution）的两条不变量：
//
//  3. 不能 Run / Resume 这一路上任何一个仍在执行的任务——经 DetachTask 还是直接用 DetachExecution、脱离了几层、
//     在节点里还是在编排的 agent 级钩子里脱离，都一样。「仍在执行」按任务判：脱离的那次执行结束了、任务又续跑
//     起来（另一次执行）也算；任务挂起之后、续跑之前不算——用存下的 ctx 脱离再续跑它是合法的，续跑的那次执行
//     就是它自己，它的成员照常跑。
//  4. 它挂起时交出的中断，所在任务的节点不论怎么返回（原样、取出里面的 *InterruptError、再包一层、合并进别的
//     中断）都被终局拒收。

func doAsker(t *testing.T, name, q string) agent_v2.Agent {
	t.Helper()
	return hoGraphHost(t, name, "ask", func(ctx context.Context, _ string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
		if err != nil {
			return "", err
		}
		return ans.Content, nil
	})
}

// doWaitPending 轮询直到 sid 名下至少有一份挂起档（并行兄弟分支挂起之后才落档），返回份数；超时返 0。
func doWaitPending(cper compose_v2.Checkpointer, sid string) int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p, _ := cper.ListPending(context.Background(), sid); len(p) > 0 {
			return len(p)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 0
}

// TestDetach_SideInterruptReturnedAnyWay_Rejected（不变量 4）：节点里 DetachTask 开的独立任务挂起了，节点把它的
// 中断返回——原样、用 IsInterrupt 取出来返回、%w 再包一层、跟本节点自己的提问合并——都要被终局拒收；独立任务
// 自己的挂起档原样留着。
//
// 曾经只认外面那层包装：取出来返回、再包一层这两种自然写法下，外层把它当成自己的挂起收下，带累积答案续跑
// 5 轮问了 5 次、每次换一个 ID，留下 6 份孤儿挂起档（实测）。
func TestDetach_SideInterruptReturnedAnyWay_Rejected(t *testing.T) {
	cases := map[string]func(ctx context.Context, err error) error{
		"原样返回": func(_ context.Context, err error) error { return err },
		"取出来返回": func(_ context.Context, err error) error {
			ie, _ := compose_v2.IsInterrupt(err)
			return ie
		},
		"再包一层": func(_ context.Context, err error) error {
			ie, _ := compose_v2.IsInterrupt(err)
			return fmt.Errorf("side: %w", ie)
		},
		"errors.As 取出来返回": func(_ context.Context, err error) error {
			var ie *compose_v2.InterruptError
			if !errors.As(err, &ie) {
				return err
			}
			return ie
		},
		"合并进本节点的提问": func(ctx context.Context, err error) error {
			ie, _ := compose_v2.IsInterrupt(err)
			_, own := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "本节点自己的提问"})
			ownIE, _ := compose_v2.IsInterrupt(own)
			return compose_v2.MergeInterrupts([]*compose_v2.InterruptError{ownIE, ie})
		},
	}
	for name, ret := range cases {
		t.Run(name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			side := doAsker(t, "side", "独立任务要人确认")
			sideSess := agent_v2.NewRunSession()
			var sideInterrupted bool
			step := dtStep(t, func(ctx context.Context) error {
				_, err := side.Run(agent_v2.DetachTask(ctx), sideSess, schema.UserMessage("x"))
				if _, ok := compose_v2.IsInterrupt(err); !ok {
					return err
				}
				sideInterrupted = true
				return ret(ctx, err)
			})
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", step)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			_, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
			if !sideInterrupted {
				t.Fatal("fixture 没搭对：独立任务应挂起")
			}
			if _, isInt := compose_v2.IsInterrupt(err); isInt || !errors.Is(err, compose_v2.ErrRunAborted) {
				t.Errorf("当前任务应终局拒收独立任务的挂起，实际：%v", err)
			}
			if p, _ := cper.ListPending(context.Background(), sideSess.RunID); len(p) == 0 {
				t.Error("独立任务自己的挂起档应原样留着")
			}
		})
	}
}

// TestDetach_DetachExecutionDirect_CurrentTaskSession_Rejected（不变量 3）：节点里直接用 compose_v2.DetachExecution
// 脱离，再拿当前任务的 session 跑 agent——入口拒；并行兄弟分支挂着的存档原样留着。
//
// 曾经只有 DetachTask 记「从哪个任务脱离」：直接用 DetachExecution 时那个 agent 被当成新任务跑完收尾，把当前任务
// 名下兄弟分支挂着的存档删了（1 → 0，实测）。
func TestDetach_DetachExecutionDirect_CurrentTaskSession_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	asker := doAsker(t, "asker", "兄弟分支要人确认")
	x := dtBasic("x", "x done")
	var xErr error
	var before, after int
	bypass := hoGraphHost(t, "bypass", "call", func(ctx context.Context, _ string) (string, error) {
		sid := agent_v2.GetSession(ctx).RunID
		before = doWaitPending(cper, sid)
		_, xErr = x.Run(compose_v2.DetachExecution(ctx), agent_v2.GetSession(ctx), schema.UserMessage("y"))
		p, _ := cper.ListPending(context.Background(), sid)
		after = len(p)
		return "bypass done", nil
	})
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("asker", asker).AddBranch("bypass", bypass)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：asker 分支应挂起")
	}
	if before == 0 {
		t.Fatal("fixture 没搭对：跑 x 之前应看到兄弟分支的挂起档")
	}
	if xErr == nil || !strings.Contains(xErr.Error(), "脱离出来之前所在的任务") {
		t.Errorf("直接用 DetachExecution 之后拿当前任务的 session 跑 agent 应被拒，实际 %v", xErr)
	}
	if after != before {
		t.Errorf("兄弟分支挂着的存档被删了：%d → %d", before, after)
	}
}

// TestDetach_TwoLevels_OuterTaskSession_Rejected（不变量 3）：任务 A 的节点里脱离、开独立任务 B，B 的节点里再脱离一次，
// 然后拿 A 的 session 跑 agent、或续跑 A——A 一直在执行（正等着 B），都拒。
//
// 曾经只记最近一层（B），A 不被拒（实测）。
func TestDetach_TwoLevels_OuterTaskSession_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	x := dtBasic("x", "x done")
	sessA := agent_v2.NewRunSession()
	var runErr, resumeErr error
	inner := hoGraphHost(t, "inner", "call", func(ctx context.Context, _ string) (string, error) {
		_, runErr = x.Run(agent_v2.DetachTask(ctx), sessA, schema.UserMessage("y"))
		_, resumeErr = x.Resume(compose_v2.DetachExecution(ctx), sessA.RunID)
		return "inner done", nil
	})
	outer := hoGraphHost(t, "outer", "call", func(ctx context.Context, _ string) (string, error) {
		_, err := inner.Run(agent_v2.DetachTask(ctx), agent_v2.NewRunSession(), schema.UserMessage("b"))
		return "outer done", err
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	if _, err := outer.Run(ctx, sessA, schema.UserMessage("go")); err != nil {
		t.Fatalf("外层：%v", err)
	}
	for name, e := range map[string]error{"Run": runErr, "Resume": resumeErr} {
		if e == nil || !strings.Contains(e.Error(), "脱离出来之前所在的任务") {
			t.Errorf("两层脱离之后 %s 外层任务 A 应被拒，实际 %v", name, e)
		}
	}
}

// doDetachInAgentStart 在 OnAgentStart（编排的 agent 级钩子，跑在图外、不在任何节点执行范围里）里脱离，拿当前任务
// 的 session 跑 agent。
type doDetachInAgentStart struct {
	hook.NoOpHooks
	target string
	x      agent_v2.Agent
	err    error
	ran    bool
}

func (h *doDetachInAgentStart) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name == h.target && !h.ran {
		h.ran = true
		_, h.err = h.x.Run(agent_v2.DetachTask(ctx), agent_v2.GetSession(ctx), schema.UserMessage("y"))
	}
	return ctx, nil
}

// TestDetachTask_InAgentHook_CurrentTaskSession_Rejected（不变量 3）：编排的 OnAgentStart 跑在图外，引擎这一层看不到
// 所在的任务，靠 DetachTask 把本包的任务章交过去（compose_v2.DetachExecutionFrom）。拿当前任务的 session 跑 agent
// 同样要拒。
func TestDetachTask_InAgentHook_CurrentTaskSession_Rejected(t *testing.T) {
	h := &doDetachInAgentStart{target: "seq", x: dtBasic("x", "x done")}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialHooks(h)).
		AddStep("s1", dtBasic("worker", "worker done"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("外层：%v", err)
	}
	if !h.ran {
		t.Fatal("fixture 没搭对：钩子没跑")
	}
	if h.err == nil || !strings.Contains(h.err.Error(), "脱离出来之前所在的任务") {
		t.Errorf("在 agent 级钩子里脱离之后拿当前任务的 session 跑 agent 应被拒，实际 %v", h.err)
	}
}

// TestDetach_StoredCtxAfterTaskPaused_ResumeAllowed（不变量 3 的另一半）：任务挂起之后，用它执行时存下来的 ctx
// 先 WithoutCancel 再脱离（DetachExecution 的 godoc 教的写法），续跑这个任务本身——它已经不在执行，应当续得上。
//
// 曾经来源链只看 ctx 上有没有、不看那次执行还在不在跑：两种脱离都被拒，还报 ErrRunAborted（文案说存档已清，
// 存档其实原样还在），按 errors.Is 判终局的调用方会放弃一个续得上的任务（审计实测）。
func TestDetach_StoredCtxAfterTaskPaused_ResumeAllowed(t *testing.T) {
	for name, detach := range map[string]func(context.Context) context.Context{
		"DetachExecution": compose_v2.DetachExecution,
		"DetachTask":      agent_v2.DetachTask,
	} {
		t.Run(name, func(t *testing.T) {
			top := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			var stored context.Context
			a := hoGraphHost(t, "a", "ask", func(ctx context.Context, _ string) (string, error) {
				stored = ctx
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "A 要人确认"})
				if err != nil {
					return "", err
				}
				return ans.Content, nil
			})
			sess := agent_v2.NewRunSession()
			_, err := a.Run(top, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok || stored == nil {
				t.Fatalf("fixture 没搭对：A 应挂起，实际 %v", err)
			}
			d := detach(context.WithoutCancel(stored))
			evs, err := a.Resume(d, sess.RunID, agent_v2.WithResumeSession(sess),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
			if err != nil {
				t.Fatalf("任务已挂起，用存下的 ctx 脱离之后续跑它应当成功，实际：%v（ErrRunAborted=%v）", err, errors.Is(err, compose_v2.ErrRunAborted))
			}
			if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "yes" {
				t.Errorf("续跑应拿到答案 yes，实际 %v", f)
			}
		})
	}
}

// TestDetach_ReusedInterruptObject_NotStampedForLegitTask（不变量 4 的另一半）：业务把一个 *InterruptError 当包级
// 变量复用（「暂停」哨兵）。它经过一次顶层脱离出来的运行之后，在普通任务里作为自己的挂起返回，照常挂起。
//
// 曾经记号直接写在业务的对象上：经过一次脱离运行就永远带着，之后普通任务返回它被当成外来中断终局拒收、挂起档
// 0 份（审计实测）。
func TestDetach_ReusedInterruptObject_NotStampedForLegitTask(t *testing.T) {
	shared := &compose_v2.InterruptError{Reason: "pause",
		Query: &compose_v2.AskQuery{InterruptID: "pause-label", Question: "暂停一下"}}
	pauser := func(name string) agent_v2.Agent {
		return hoGraphHost(t, name, "n", func(context.Context, string) (string, error) { return "", shared })
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	top := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	if _, err := pauser("side").Run(compose_v2.DetachExecution(top), agent_v2.NewRunSession(), schema.UserMessage("x")); err == nil {
		t.Fatal("fixture 没搭对：脱离出来的运行应挂起")
	} else if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("fixture 没搭对：脱离出来的运行应挂起，实际 %v", err)
	}
	sess := agent_v2.NewRunSession()
	_, err := pauser("b").Run(top, sess, schema.UserMessage("y"))
	if _, ok := compose_v2.IsInterrupt(err); !ok || errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("普通任务返回自己的暂停应照常挂起，实际：%v", err)
	}
	if p, _ := cper.ListPending(context.Background(), sess.RunID); len(p) == 0 {
		t.Error("普通任务的挂起档应在")
	}
}

// doPendingNow 数 sid 下此刻挂着的存档份数（查询失败返 -1，让断言暴露出来）。
func doPendingNow(cper compose_v2.Checkpointer, sid string) int {
	p, err := cper.ListPending(context.Background(), sid)
	if err != nil {
		return -1
	}
	return len(p)
}

// doAskOnce 是节点体：第 n 次执行时先跑 onExec(n, ctx)，再问一个问题（答了就返回答案）。
func doAskOnce(onExec func(n int, ctx context.Context)) func(ctx context.Context, _ string) (string, error) {
	n := 0
	return func(ctx context.Context, _ string) (string, error) {
		n++
		onExec(n, ctx)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "A 要人确认"})
		if err != nil {
			return "", err
		}
		return ans.Content, nil
	}
}

// TestDetach_StoredCtxFromEarlierExecution_DuringResume_Rejected（不变量 3）：任务 A 第一次执行时节点存下 ctx、挂起；
// A 续跑（另一次执行）期间，节点拿上一次执行存下的 ctx（先 WithoutCancel）脱离，再拿 A 的 session 跑 agent、续跑 A、
// 放弃 A——A 正在执行，都拒；A 挂着的存档原样在，A 的节点只在那两次执行里各跑一次。
//
// 曾经「仍在执行」按那一次执行判：上一次执行已经结束，按不在执行算、放行——那个 agent 被当成新任务以 A 的 sid
// 跑完收尾，把 A 挂着的存档删了（1 → 0）；放弃同样删掉；续跑 A 则在 A 自己的执行里把它的节点再跑一遍（均实测）。
func TestDetach_StoredCtxFromEarlierExecution_DuringResume_Rejected(t *testing.T) {
	type action func(ctx context.Context, a, x agent_v2.Agent, sess *agent_v2.RunSession) error
	cases := map[string]action{
		"Run": func(ctx context.Context, _, x agent_v2.Agent, sess *agent_v2.RunSession) error {
			_, err := x.Run(ctx, sess, schema.UserMessage("y"))
			return err
		},
		"Resume": func(ctx context.Context, a, _ agent_v2.Agent, sess *agent_v2.RunSession) error {
			_, err := a.Resume(ctx, sess.RunID)
			return err
		},
		"放弃": func(ctx context.Context, _, x agent_v2.Agent, sess *agent_v2.RunSession) error {
			_, err := x.Resume(ctx, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
			return err
		},
	}
	for name, act := range cases {
		t.Run(name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			top := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			x := dtBasic("x", "x done")
			sess := agent_v2.NewRunSession()
			var a agent_v2.Agent
			var stored context.Context
			var actErr error
			execs, before, after := 0, -1, -1
			a = hoGraphHost(t, "a", "ask", doAskOnce(func(n int, ctx context.Context) {
				execs = n
				switch n {
				case 1:
					stored = ctx
				case 2:
					before = doPendingNow(cper, sess.RunID)
					actErr = act(compose_v2.DetachExecution(context.WithoutCancel(stored)), a, x, sess)
					after = doPendingNow(cper, sess.RunID)
				}
			}))
			_, err := a.Run(top, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok || stored == nil {
				t.Fatalf("fixture 没搭对：A 应挂起，实际 %v", err)
			}
			evs, err := a.Resume(top, sess.RunID, agent_v2.WithResumeSession(sess),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
			if before < 1 {
				t.Fatalf("fixture 没搭对：续跑期间应看到 A 挂着的存档，实际 %d 份", before)
			}
			if actErr == nil || !strings.Contains(actErr.Error(), "脱离出来之前所在的任务") {
				t.Errorf("A 正在续跑，用上一次执行存下的 ctx 脱离之后 %s A 应被拒，实际 %v", name, actErr)
			}
			if after != before {
				t.Errorf("A 挂着的存档被动了：%d → %d", before, after)
			}
			if execs != 2 {
				t.Errorf("A 的节点应只在两次执行里各跑一次，实际跑到第 %d 次", execs)
			}
			if err != nil {
				t.Fatalf("A 续跑：%v", err)
			}
			if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "yes" {
				t.Errorf("A 续跑应拿到答案 yes，实际 %v", f)
			}
		})
	}
}

// doWrapAsk 把单节点的 ask 包进各种编排（ask 是它唯一的成员 / 分支 / 步骤 / 被调方）。
func doWrapAsk() map[string]func(t *testing.T, ask agent_v2.Agent) agent_v2.Agent {
	return map[string]func(t *testing.T, ask agent_v2.Agent) agent_v2.Agent{
		"Sequential": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("w", ask)
		},
		"Parallel": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("w", ask)
		},
		"Loop": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewLoopAgent(agent_v2.WithLoopName("loop"), agent_v2.WithLoopMaxIterations(1)).WithBody(ask)
		},
		"Routing": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("route"),
				agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) { return "w", nil }),
			).AddBranch("w", ask)
		},
		"Reflect": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect"), agent_v2.WithReflectMaxRounds(1),
				agent_v2.WithEvalFunc(func(context.Context, *schema.Message) (bool, error) { return true, nil }),
			).WithActor(ask)
		},
		"Handoff": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("handoff"), agent_v2.WithHandoffInitial("w"),
				agent_v2.WithHandoffMaxHops(2)).AddAgent("w", ask)
		},
		"Plan": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
				if len(executed) > 0 {
					return nil, "done", nil
				}
				return []agent_v2.PlanStep{{Description: "干活", SubAgentName: "w"}}, "", nil
			}
			pa := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("plan"))
			pa.AddSubAgent("w", ask)
			return pa
		},
		"Basic+AgentAsTool": func(_ *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(ask, agent_v2.WithAgentToolDescription("干活")))
			return agent_v2.NewBasicAgent(agent_v2.WithName("basic"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1",
						Function: schema.FunctionCall{Name: ask.Name(), Arguments: `{"input":"干活"}`}}}),
					schema.AssistantMessage("好了"),
				})))
		},
		"Graph+RunSubAgent": func(t *testing.T, ask agent_v2.Agent) agent_v2.Agent {
			return hoGraphHost(t, "graph", "n", func(ctx context.Context, in string) (string, error) {
				evs, err := agent_v2.RunSubAgent(ctx, "w", ask, schema.UserMessage(in))
				if err != nil {
					return "", err
				}
				if f := agent_v2.FinalMessage(evs); f != nil {
					return f.Content, nil
				}
				return "", nil
			})
		},
	}
}

// TestDetach_StoredCtxDuringResume_EveryOrchestration_Rejected（不变量 3）：同上一条，任务换成每一种编排（提问的节点
// 在它的成员 / 分支 / 被调方里）——续跑期间拿上一次执行存下的 ctx 脱离、拿这个任务的 session 跑 agent，都拒，挂着的
// 存档原样在。按任务判靠的是任务的根图以 session 的 RunID 登记，哪种编排的根图换了 sid，它那一格就会红。
func TestDetach_StoredCtxDuringResume_EveryOrchestration_Rejected(t *testing.T) {
	for name, wrap := range doWrapAsk() {
		t.Run(name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			top := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			x := dtBasic("x", "x done")
			sess := agent_v2.NewRunSession()
			var stored context.Context
			var xErr error
			before, after, execs := -1, -1, 0
			ask := hoGraphHost(t, "ask", "ask", doAskOnce(func(n int, ctx context.Context) {
				execs = n
				switch n {
				case 1:
					stored = ctx
				case 2:
					before = doPendingNow(cper, sess.RunID)
					_, xErr = x.Run(compose_v2.DetachExecution(context.WithoutCancel(stored)), sess, schema.UserMessage("y"))
					after = doPendingNow(cper, sess.RunID)
				}
			}))
			ag := wrap(t, ask)
			_, err := ag.Run(top, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok || stored == nil {
				t.Fatalf("fixture 没搭对：应挂起，实际 %v", err)
			}
			_, err = ag.Resume(top, sess.RunID, agent_v2.WithResumeSession(sess),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
			if execs != 2 || before < 1 {
				t.Fatalf("fixture 没搭对：提问节点应执行 2 次、续跑期间应看到挂着的存档，实际 %d 次、%d 份（续跑 err=%v）", execs, before, err)
			}
			if xErr == nil || !strings.Contains(xErr.Error(), "脱离出来之前所在的任务") {
				t.Errorf("续跑期间用上一次存下的 ctx 脱离、跑这个任务应被拒，实际 %v", xErr)
			}
			if after != before {
				t.Errorf("挂着的存档被动了：%d → %d", before, after)
			}
			if err != nil {
				t.Errorf("续跑：%v", err)
			}
		})
	}
}

// TestDetach_TaskResumedWithStoredCtx_NodeDetachesAgain_Rejected（不变量 3）：任务 A 挂起之后用存下的 ctx 脱离、续跑 A
// （合法，见 TestDetach_StoredCtxAfterTaskPaused_ResumeAllowed）；续跑的那次执行里，节点用自己当前的 ctx 再 DetachTask，
// 拿 A 的 session 跑 agent——A 正在执行，拒；A 挂着的存档原样在。
//
// 曾经来源链按任务去重、先记的留下：续跑那次 ctx 带进来的是上一次执行的那条（已结束），节点这次脱离记的那条（这次
// 执行，仍在跑）被丢掉 ⟹ 放行，那个 agent 以 A 的 sid 跑完收尾，删了 A 挂着的存档（实测）。
func TestDetach_TaskResumedWithStoredCtx_NodeDetachesAgain_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	top := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	x := dtBasic("x", "x done")
	sess := agent_v2.NewRunSession()
	var stored context.Context
	var xErr error
	before, after := -1, -1
	a := hoGraphHost(t, "a", "ask", doAskOnce(func(n int, ctx context.Context) {
		switch n {
		case 1:
			stored = ctx
		case 2:
			before = doPendingNow(cper, sess.RunID)
			_, xErr = x.Run(agent_v2.DetachTask(ctx), sess, schema.UserMessage("y"))
			after = doPendingNow(cper, sess.RunID)
		}
	}))
	_, err := a.Run(top, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || stored == nil {
		t.Fatalf("fixture 没搭对：A 应挂起，实际 %v", err)
	}
	_, err = a.Resume(compose_v2.DetachExecution(context.WithoutCancel(stored)), sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("A 续跑：%v", err)
	}
	if before < 1 {
		t.Fatalf("fixture 没搭对：续跑期间应看到 A 挂着的存档，实际 %d 份", before)
	}
	if xErr == nil || !strings.Contains(xErr.Error(), "脱离出来之前所在的任务") {
		t.Errorf("A 正在续跑，节点再脱离之后拿 A 的 session 跑 agent 应被拒，实际 %v", xErr)
	}
	if after != before {
		t.Errorf("A 挂着的存档被动了：%d → %d", before, after)
	}
}

// doDetachOnResumeStart 在编排续跑时（第二次 OnAgentStart，跑在图外）DetachTask，拿当前任务的 session 跑 agent。
type doDetachOnResumeStart struct {
	hook.NoOpHooks
	target string
	x      agent_v2.Agent
	starts int
	err    error
	ran    bool
}

func (h *doDetachOnResumeStart) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name != h.target {
		return ctx, nil
	}
	h.starts++
	if h.starts == 2 {
		h.ran = true
		_, h.err = h.x.Run(agent_v2.DetachTask(ctx), agent_v2.GetSession(ctx), schema.UserMessage("y"))
	}
	return ctx, nil
}

// TestDetachTask_InAgentHookOfTaskResumedWithStoredCtx_Rejected（不变量 3）：任务 A 挂起之后用存下的 ctx 脱离、续跑 A；
// 续跑时 A 的 OnAgentStart（跑在图外）里 DetachTask，拿 A 的 session 跑 agent——A 正在续跑，拒。
//
// 曾经来源链按任务去重、先记的留下：续跑那次 ctx 带进来的是上一次执行的那条（已结束），钩子这次脱离记的那条（图外，
// 没有存活标记、按仍在执行算）被丢掉 ⟹ 放行（实测）。
func TestDetachTask_InAgentHookOfTaskResumedWithStoredCtx_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	top := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	var stored context.Context
	step := hoGraphHost(t, "step", "ask", doAskOnce(func(n int, ctx context.Context) {
		if n == 1 {
			stored = ctx
		}
	}))
	h := &doDetachOnResumeStart{target: "seq", x: dtBasic("x", "x done")}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialHooks(h)).AddStep("step", step)
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(top, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || stored == nil {
		t.Fatalf("fixture 没搭对：A 应挂起，实际 %v", err)
	}
	evs, err := seq.Resume(compose_v2.DetachExecution(context.WithoutCancel(stored)), sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if !h.ran {
		t.Fatal("fixture 没搭对：续跑时的钩子没跑")
	}
	if h.err == nil || !strings.Contains(h.err.Error(), "脱离出来之前所在的任务") {
		t.Errorf("A 正在续跑，在它的 agent 级钩子里脱离之后拿 A 的 session 跑 agent 应被拒，实际 %v", h.err)
	}
	if err != nil {
		t.Fatalf("A 续跑：%v", err)
	}
	if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "yes" {
		t.Errorf("A 续跑应拿到答案 yes，实际 %v", f)
	}
}

// TestDetach_StoredCtxAfterTaskPaused_ResumeOrchestrationWithMembers_Allowed（不变量 3 的另一半）：同
// TestDetach_StoredCtxAfterTaskPaused_ResumeAllowed，任务是带成员的编排。续跑的那次执行就是 A 自己：它的成员拿 A 的
// session 跑是 A 的一部分，不是「从 A 脱离出来又跑 A」——成员照常跑完。
func TestDetach_StoredCtxAfterTaskPaused_ResumeOrchestrationWithMembers_Allowed(t *testing.T) {
	for name, detach := range map[string]func(context.Context) context.Context{
		"DetachExecution": compose_v2.DetachExecution,
		"DetachTask":      agent_v2.DetachTask,
	} {
		t.Run(name, func(t *testing.T) {
			top := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			var stored context.Context
			ask := hoGraphHost(t, "ask", "ask", doAskOnce(func(n int, ctx context.Context) {
				if n == 1 {
					stored = ctx
				}
			}))
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
				AddStep("ask", ask).AddStep("worker", dtBasic("worker", "worker done"))
			sess := agent_v2.NewRunSession()
			_, err := seq.Run(top, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok || stored == nil {
				t.Fatalf("fixture 没搭对：A 应挂起，实际 %v", err)
			}
			evs, err := seq.Resume(detach(context.WithoutCancel(stored)), sess.RunID, agent_v2.WithResumeSession(sess),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
			if err != nil {
				t.Fatalf("任务已挂起，用存下的 ctx 脱离之后续跑它、它的成员应照常跑，实际：%v", err)
			}
			if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "worker done" {
				t.Errorf("续跑应跑完成员，实际产出 %v", f)
			}
		})
	}
}

// doRawRunWithSID 在 ctx 上直接 compose_v2.Run 一张单节点图、sid 给 sid，节点里经 RunSubAgent 跑 x；返回节点跑没跑、
// 那次调用的错。
func doRawRunWithSID(t *testing.T, ctx context.Context, cper compose_v2.Checkpointer, sid string, x agent_v2.Agent) (bool, error) {
	t.Helper()
	var subErr error
	ran := false
	b := compose_v2.NewBuilder().WithName("g2")
	b.AddNode("n", func(ctx2 context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ran = true
		_, subErr = agent_v2.RunSubAgent(ctx2, "w", x, schema.UserMessage("y"))
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g2, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	// 这张图本身跑成跑不成不看，只看它里面那次调用被不被拒
	_ = compose_v2.Run(ctx, g2, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	return ran, subErr
}

// doRawRunInAgentStart 在编排第一次 OnAgentStart（跑在图外）里 DetachTask，然后直接 Run 这个任务的 sid。
type doRawRunInAgentStart struct {
	hook.NoOpHooks
	t      *testing.T
	cper   compose_v2.Checkpointer
	x      agent_v2.Agent
	ran    bool
	subErr error
	done   bool
}

func (h *doRawRunInAgentStart) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name == "seq" && !h.done {
		h.done = true
		h.ran, h.subErr = doRawRunWithSID(h.t, agent_v2.DetachTask(ctx), h.cper, agent_v2.GetSession(ctx).RunID, h.x)
	}
	return ctx, nil
}

// TestDetach_RawRunWithRunningTaskSID_InnerCallStillRejected（不变量 3）：脱离之后直接 compose_v2.Run 一张图、sid 给的是
// 正在执行的这个任务的——那张图的节点里经 RunSubAgent 跑 agent（拿的是这个任务的 session），入口照样拒。在节点里脱离
// （那一项的存活标记为真）与在编排的 agent 级钩子里脱离（那一项没有存活标记）各一格。
//
// 曾经根图入口把自己的 sid 从来源链里一律去掉、不看那一项的执行还在不在跑：链上那条「仍在执行」被删，里面的调用
// 不再被拒、被调方真的跑了（审计实测；在那之前是报 ErrRunAborted）。引擎本身不拦直接 compose_v2.Run 一个正在执行
// 的任务的 sid，这条守的是 agent 入口那道拒绝不被遮住。
func TestDetach_RawRunWithRunningTaskSID_InnerCallStillRejected(t *testing.T) {
	check := func(t *testing.T, ran bool, subErr error) {
		t.Helper()
		if !ran {
			t.Fatal("fixture 没搭对：g2 的节点没跑")
		}
		if subErr == nil || !strings.Contains(subErr.Error(), "脱离出来之前所在的任务") {
			t.Errorf("直接 Run 一个正在执行的任务的 sid，里面拿这个任务的 session 跑 agent 应被拒，实际 %v", subErr)
		}
	}
	t.Run("节点里脱离", func(t *testing.T) {
		cper := compose_v2.NewInMemoryCheckpointer()
		x := dtBasic("x", "x done")
		var ran bool
		var subErr error
		step := dtStep(t, func(ctx context.Context) error {
			ran, subErr = doRawRunWithSID(t, compose_v2.DetachExecution(ctx), cper, agent_v2.GetSession(ctx).RunID, x)
			return nil
		})
		seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", step)
		if _, err := seq.Run(compose_v2.WithCheckpointerCtx(context.Background(), cper), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
			t.Fatalf("外层：%v", err)
		}
		check(t, ran, subErr)
	})
	t.Run("agent 级钩子里脱离", func(t *testing.T) {
		cper := compose_v2.NewInMemoryCheckpointer()
		h := &doRawRunInAgentStart{t: t, cper: cper, x: dtBasic("x", "x done")}
		seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialHooks(h)).
			AddStep("s1", dtBasic("worker", "worker done"))
		if _, err := seq.Run(compose_v2.WithCheckpointerCtx(context.Background(), cper), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
			t.Fatalf("外层：%v", err)
		}
		check(t, h.ran, h.subErr)
	})
}

// TestDetach_NilSessionWithSIDPushedBack_Rejected（不变量 3）：节点里 DetachTask 之后又把当前任务的 sid 推回 ctx
// （比如想让独立任务的 span 带上这个请求的 sid），session 传 nil——框架按 ctx 上的 sid 补出来的 RunID 就是这个仍在
// 执行的任务，入口拒；并行兄弟分支挂着的存档原样在。
//
// 曾经 session 传 nil 时入口补完直接返回、不查来源链：那个 agent 被当成新任务以当前任务的 sid 跑完收尾，删了兄弟
// 分支挂着的存档（1 → 0，审计实测；传显式 session 的同一个写法是拒的）。
func TestDetach_NilSessionWithSIDPushedBack_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	asker := doAsker(t, "asker", "兄弟分支要人确认")
	x := dtBasic("x", "x done")
	var xErr error
	before, after := -1, -1
	bypass := hoGraphHost(t, "bypass", "call", func(ctx context.Context, _ string) (string, error) {
		sid := agent_v2.GetSession(ctx).RunID
		before = doWaitPending(cper, sid)
		_, xErr = x.Run(compose_v2.WithSessionIDCtx(agent_v2.DetachTask(ctx), sid), nil, schema.UserMessage("y"))
		after = doPendingNow(cper, sid)
		return "bypass done", nil
	})
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("asker", asker).AddBranch("bypass", bypass)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：asker 分支应挂起")
	}
	if before < 1 {
		t.Fatalf("fixture 没搭对：跑 x 之前应看到兄弟分支的挂起档，实际 %d 份", before)
	}
	if xErr == nil || !strings.Contains(xErr.Error(), "脱离出来之前所在的任务") {
		t.Errorf("session 传 nil、ctx 上推回了当前任务的 sid，应被拒，实际 %v", xErr)
	}
	if after != before {
		t.Errorf("兄弟分支挂着的存档被删了：%d → %d", before, after)
	}
}
