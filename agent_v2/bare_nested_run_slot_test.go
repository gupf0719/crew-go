package agent_v2_test

// 节点里拿父的 session 裸调另一个 agent（`sub.Run(ctx, agent_v2.GetSession(ctx), …)`）：它没有自己的 ns，图会落进父图自己的
// 存档槽。曾经放行：两个 BasicAgent 的图身份相同，子层把父图正挂着的存档当成自己的读进来、用自己的工具接着跑父图的 frontier
// （旁路工具多执行一次），成功收尾时把那个槽清掉，err == nil。现在 compose_v2.Run 入口当场拒（ErrRunAborted，指向
// RunSubAgent），子层什么都不执行；经 RunSubAgent 调照常跑。

import (
	"context"
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

// bareNestedHost 建父 BasicAgent：第 1 轮调 side（先问人、答了才计数），第 2 轮调 consult（工具里调子 agent），最后收尾。
// 子 agent 也有一件叫 side 的工具（计子层的数）。viaRunSubAgent 决定 consult 里怎么调子 agent。
func bareNestedHost(viaRunSubAgent bool, sideHost, sideSub *atomic.Int32) agent_v2.Agent {
	subTk := tool.NewToolkit()
	subTk.Register(tool.NewFuncTool("side", "旁路工具", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			sideSub.Add(1)
			return tool.Success("sub side done"), nil
		}))
	sub := agent_v2.NewBasicAgent(agent_v2.WithName("sub"), agent_v2.WithToolkit(subTk), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("sub ok")})))

	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("side", "旁路工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "执行旁路吗"}); err != nil {
				return nil, err
			}
			sideHost.Add(1)
			return tool.Success("host side done"), nil
		}))
	tk.Register(tool.NewFuncTool("consult", "请教子 agent", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			var err error
			if viaRunSubAgent {
				_, err = agent_v2.RunSubAgent(ctx, "consult", sub, schema.UserMessage("帮忙看看"))
			} else {
				_, err = sub.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("帮忙看看"))
			}
			if err != nil {
				return nil, err
			}
			return tool.Success("consulted"), nil
		}))
	call := func(id, name string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: name, Arguments: `{}`}}})
	}
	return agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5),
		agent_v2.WithModel(newStubModel([]*schema.Message{call("c1", "side"), call("c2", "consult"), schema.AssistantMessage("host done")})))
}

// runBareNested 跑到问人、答了续跑，返回续跑的结果。
func runBareNested(t *testing.T, host agent_v2.Agent) error {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：第 1 轮 side 应停在问人上，实际 %v", err)
	}
	_, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("执行")}))
	return err
}

func TestBareNestedRun_OnParentSlotRejected(t *testing.T) {
	var sideHost, sideSub atomic.Int32
	err := runBareNested(t, bareNestedHost(false, &sideHost, &sideSub))
	if !errors.Is(err, compose_v2.ErrRunAborted) || !strings.Contains(err.Error(), "RunSubAgent") {
		t.Fatalf("节点里裸调的子 agent 落进父图自己的存档槽，应在入口被拒（ErrRunAborted、指向 RunSubAgent），实际 %v", err)
	}
	if sideSub.Load() != 0 {
		t.Fatalf("子层不该执行任何东西（曾经接着跑父图的 frontier、旁路工具多执行一次），实际执行了 %d 次", sideSub.Load())
	}
}

func TestBareNestedRun_ViaRunSubAgentRuns(t *testing.T) {
	var sideHost, sideSub atomic.Int32
	if err := runBareNested(t, bareNestedHost(true, &sideHost, &sideSub)); err != nil {
		t.Fatalf("经 RunSubAgent 调应照常跑完：%v", err)
	}
	if sideHost.Load() != 1 || sideSub.Load() != 0 {
		t.Fatalf("父的 side 执行一次、子的 side 不执行，实际 host=%d sub=%d", sideHost.Load(), sideSub.Load())
	}
}

// hookBareRun 在 agent 级钩子里拿宿主的 session 裸调 sub：onStart 时在续跑那次的 OnAgentStart 里，否则在停在问人之后的
// OnAgentEnd 里。那一刻宿主的图不在跑，但它的槽里放着它自己的挂起档。
type hookBareRun struct {
	hook.NoOpHooks
	onStart      bool
	sub          agent_v2.Agent
	starts, ends *int
	subErr       *error
}

func (h hookBareRun) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	*h.starts++
	if h.onStart && *h.starts == 2 {
		_, *h.subErr = h.sub.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("帮忙看看"))
	}
	return ctx, nil
}

func (h hookBareRun) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	*h.ends++
	if !h.onStart && *h.ends == 1 {
		_, *h.subErr = h.sub.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("帮忙看看"))
	}
	return ctx
}

// 宿主在 agent 级钩子里裸调别的 agent：子层在入口被拒、什么都不执行，宿主的挂起档完好、续跑照常（曾经子层把宿主的挂起档
// 读进来接着跑、旁路工具多执行一次、成功时删掉它，宿主随后续不上，审计实测）。
//
// 钩子里调的是一个经 EnterRun 进入、再透传给框架 agent 的壳时同样拒：壳不占槽，被透传的那个入口发现槽被宿主占着。
func TestBareNestedRun_FromAgentHookRejected(t *testing.T) {
	for _, tc := range []struct {
		onStart, viaShell bool
	}{{true, false}, {false, false}, {true, true}, {false, true}} {
		name := map[bool]string{true: "续跑那次的 OnAgentStart", false: "问人之后的 OnAgentEnd"}[tc.onStart] +
			map[bool]string{true: "/经 EnterRun 壳透传", false: ""}[tc.viaShell]
		onStart := tc.onStart
		t.Run(name, func(t *testing.T) {
			var sideHost, sideSub atomic.Int32
			var starts, ends int
			var subErr error
			subTk := tool.NewToolkit()
			subTk.Register(tool.NewFuncTool("side", "旁路工具", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					sideSub.Add(1)
					return tool.Success("sub side done"), nil
				}))
			var sub agent_v2.Agent = agent_v2.NewBasicAgent(agent_v2.WithName("sub"), agent_v2.WithToolkit(subTk), agent_v2.WithMaxRounds(3),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("sub ok")})))
			if tc.viaShell {
				sub = &enterRunShell{inner: sub}
			}
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("side", "旁路工具", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "执行旁路吗"}); err != nil {
						return nil, err
					}
					sideHost.Add(1)
					return tool.Success("host side done"), nil
				}))
			call := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
				Function: schema.FunctionCall{Name: "side", Arguments: `{}`}}})
			host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5),
				agent_v2.WithHooks(hookBareRun{onStart: onStart, sub: sub, starts: &starts, ends: &ends, subErr: &subErr}),
				agent_v2.WithModel(newStubModel([]*schema.Message{call, schema.AssistantMessage("host done")})))
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err := host.Run(ctx, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("fixture 没搭对：Run 应停在问人上，实际 %v", err)
			}
			_, rerr := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("执行")}))
			if !errors.Is(subErr, compose_v2.ErrRunAborted) || sideSub.Load() != 0 {
				t.Fatalf("钩子里裸调的子层应在入口被拒、不执行，实际 subErr=%v 子层 side 执行 %d 次", subErr, sideSub.Load())
			}
			if rerr != nil || sideHost.Load() != 1 {
				t.Fatalf("宿主的挂起档应完好、续跑照常，实际 resumeErr=%v 宿主 side 执行 %d 次", rerr, sideHost.Load())
			}
		})
	}
}

// enterRunShell 是按文档写的自研 agent：Run 入口调 agent_v2.EnterRun，然后把调用透传给框架 agent（同一个 ns）。
type enterRunShell struct {
	inner agent_v2.Agent
}

func (s *enterRunShell) Name() string        { return "shell" }
func (s *enterRunShell) Description() string { return "透传壳" }
func (s *enterRunShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, s.Name())
	if err != nil {
		return nil, err
	}
	return s.inner.Run(ctx, sess, in, agent_v2.RegrantRouteTo(opts, s, s.inner)...)
}
func (s *enterRunShell) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return s.inner.Resume(ctx, sid, opts...)
}

// 自研 agent 在入口调 EnterRun、再把调用透传给框架 agent（顶层，或作为编排成员）：照常跑完。EnterRun 不占槽——占了的话
// 被透传的那个跟它同一个 ns、拿不到凭证、跑图时被拒（曾经如此，审计实测）。
func TestShellWithEnterRun_DelegatesToFrameworkAgent(t *testing.T) {
	for _, asMember := range []bool{false, true} {
		t.Run(map[bool]string{false: "顶层", true: "Sequential 成员"}[asMember], func(t *testing.T) {
			inner := agent_v2.NewBasicAgent(agent_v2.WithName("inner"), agent_v2.WithMaxRounds(3),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("inner ok")})))
			var ag agent_v2.Agent = &enterRunShell{inner: inner}
			if asMember {
				ag = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("shell", ag)
			}
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
			if err != nil {
				t.Fatalf("壳透传给框架 agent 应照常跑完，实际 %v", err)
			}
			if final := agent_v2.FinalMessage(events); final == nil || final.Content != "inner ok" {
				t.Fatalf("最终答复应是被透传的 agent 的产出，实际 %+v", final)
			}
		})
	}
}
