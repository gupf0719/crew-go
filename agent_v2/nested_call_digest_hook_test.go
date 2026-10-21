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

// rewriteInputHook 在 OnAgentStart 里按 mode 改写输入，每次都加一点不同的东西（模拟钩子往
// 输入里塞时间戳、请求号）：inplace 原地改 Content、replace 换一条新消息、clear 清空消息、
// annotate 原地加标注。
type rewriteInputHook struct {
	hook.NoOpHooks
	mode string
	n    atomic.Int32
}

func (h *rewriteInputHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	k := h.n.Add(1)
	if len(in.Messages) == 0 || in.Messages[len(in.Messages)-1] == nil {
		return ctx, nil
	}
	last := in.Messages[len(in.Messages)-1]
	switch h.mode {
	case "inplace":
		last.Content += fmt.Sprintf("#%d", k)
	case "replace":
		in.Messages[len(in.Messages)-1] = schema.UserMessage(last.Content + fmt.Sprintf("#%d", k))
	case "clear":
		in.Messages = nil
	case "annotate":
		if last.Annotations == nil {
			last.Annotations = map[string]any{}
		}
		last.Annotations["req"] = fmt.Sprintf("r%d", k)
	}
	return ctx, nil
}

// orchestrationsWithHooks 是九个编排，各自挂上 h、以 p 作成员（BasicAgent 经 AgentAsTool 调它）。
func orchestrationsWithHooks() []struct {
	name  string
	build func(t *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent
} {
	return []struct {
		name  string
		build func(t *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent
	}{
		{"sequential", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s"), agent_v2.WithSequentialHooks(h)).AddStep("w", p)
		}},
		{"parallel", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("p"), agent_v2.WithParallelHooks(h)).AddBranch("w", p)
		}},
		{"loop", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewLoopAgent(agent_v2.WithLoopName("l"), agent_v2.WithLoopMaxIterations(1), agent_v2.WithLoopHooks(h)).WithBody(p)
		}},
		{"routing", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("r"),
				agent_v2.WithRoutingHooks(h),
				agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) { return "w", nil }),
			).AddBranch("w", p)
		}},
		{"reflect", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewReflectAgent(
				agent_v2.WithReflectName("rf"),
				agent_v2.WithReflectHooks(h),
				agent_v2.WithReflectMaxRounds(1),
				agent_v2.WithEvalFunc(func(context.Context, *schema.Message) (bool, error) { return true, nil }),
			).WithActor(p)
		}},
		{"handoff", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewHandoffAgent(
				agent_v2.WithHandoffName("h"),
				agent_v2.WithHandoffHooks(h),
				agent_v2.WithHandoffInitial("w"),
				agent_v2.WithHandoffMaxHops(2),
			).AddAgent("w", p)
		}},
		{"basic", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(p, agent_v2.WithAgentToolDescription("确认一件事")))
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("b"),
				agent_v2.WithHooks(h),
				agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(3),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", p.Name(), "干活")}),
					schema.AssistantMessage("好了"),
				})),
			)
		}},
		{"graph", func(t *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			b := compose_v2.NewBuilder()
			inCh := compose_v2.LastValue[*schema.Message]("in")
			outCh := compose_v2.LastValue[*schema.Message]("out")
			compose_v2.WithChannel(b, inCh)
			compose_v2.WithChannel(b, outCh)
			b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				events, err := agent_v2.RunSubAgent(ctx, "w", p, schema.UserMessage("干活"))
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(nestedFinal(events)))}, nil
			})
			b.AddEdge(compose_v2.StartNode, "n")
			b.AddEdge("n", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			return agent_v2.NewGraphAgent(g,
				agent_v2.WithGraphAgentName("ga"),
				agent_v2.WithGraphAgentHooks(h),
				agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(chs, outCh)
					return out
				}),
			)
		}},
		{"plan", func(_ *testing.T, h hook.Hooks, p agent_v2.Agent) agent_v2.Agent {
			planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
				if len(executed) > 0 {
					return nil, "done", nil
				}
				return []agent_v2.PlanStep{{Description: "干活", SubAgentName: "w"}}, "", nil
			}
			pa := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("pl"), agent_v2.WithPlanHooks(h))
			pa.AddSubAgent("w", p)
			return pa
		}},
	}
}

// TestNestedCall_IdentityTakenBeforeHookEveryOrchestration：九个编排各自的 OnAgentStart 钩子
// 每次都往输入里加点不同的东西（四种改写方式）。父节点重跑时给的是同一句话 ⟹ 仍是同一次调用：
// 续上自己的存档、用上答案，不告警。
//
// 每一格都承重：编排在自己的 Run 入口把调用的身份摘要交给引擎（续跑时引擎拿它与存档核对，对不上
// 按撞槽终局失败）。哪个编排把摘要挪到了钩子改写之后，首跑与续跑交的摘要就不同，那一格就红。
func TestNestedCall_IdentityTakenBeforeHookEveryOrchestration(t *testing.T) {
	capture := installWarnCapture(t)
	asker := func(t *testing.T) agent_v2.Agent {
		return newSingleNodeAgent(t, "asker", "work", nil, func(ctx context.Context, c string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				return "", err
			}
			return "ok:" + c + ":" + ans.Content, nil
		})
	}
	for _, c := range orchestrationsWithHooks() {
		for _, mode := range []string{"inplace", "replace", "clear", "annotate"} {
			t.Run(c.name+"/"+mode, func(t *testing.T) {
				h := &rewriteInputHook{mode: mode}
				orch := c.build(t, h, asker(t))
				host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
					events, err := agent_v2.RunSubAgent(ctx, "x", orch, schema.UserMessage("同样的任务"))
					if err != nil {
						return "", err
					}
					return nestedFinal(events), nil
				})
				cper := compose_v2.NewInMemoryCheckpointer()
				ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
				sess := agent_v2.NewRunSession()
				_, err := host.Run(ctx, sess, schema.UserMessage("go"))
				ie, ok := compose_v2.IsInterrupt(err)
				if !ok {
					t.Fatalf("首跑应停在成员的提问，got %v", err)
				}
				before := len(capture.digestWarnings())
				if _, err := host.Resume(ctx, sess.RunID,
					agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
					agent_v2.WithResumeSession(sess),
				); err != nil {
					t.Fatalf("续跑失败：%v——钩子的改写（%s）被算进了身份", err, mode)
				}
				// 前提：钩子在首跑与续跑各跑了一次，给的东西确实不同
				if h.n.Load() < 2 {
					t.Fatalf("前提不成立：钩子应在首跑与续跑各跑一次，实际 %d 次", h.n.Load())
				}
				if got := capture.digestWarnings()[before:]; len(got) != 0 {
					t.Fatalf("父节点给的是同一句话，钩子的改写（%s）不该算成输入变了；本次续跑的告警 ns：%v", mode, got)
				}
			})
		}
	}
}
