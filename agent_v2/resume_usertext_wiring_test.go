package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// userTextProbe 是个只干一件事的子 agent：记下自己被调用时读到的非寻址型文本。
type userTextProbe struct {
	saw    string
	called bool
}

func (p *userTextProbe) Name() string        { return "probe" }
func (p *userTextProbe) Description() string { return "记录 resume user text" }

func (p *userTextProbe) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	p.called = true
	p.saw, _ = compose_v2.GetResumeUserText(ctx)
	if p.saw == "" {
		// 首跑没有续跑负载 → 中断，等外层 Resume 再进来一次
		return nil, &compose_v2.InterruptError{
			Reason: "probe_ask",
			Query:  &compose_v2.AskQuery{Question: "?", InterruptID: "probe_q"},
		}
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("probe", schema.AssistantMessage("ok"))}, nil
}

func (p *userTextProbe) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestResumeUserText_AllOrchestrationsForward 钉住「每个编排的 Resume 都把非寻址型
// 负载递给引擎」。
//
// # 为什么必须逐个编排各一格
//
// `agent_v2.WithResumeUserText` → `resumeConfig.userText` → `runOpts` 这一段，**每个编排
// 各写一行**（9 处）。漏掉一个，业务用那个编排时 planner 就拿不到用户的新指示——静默降级
// 成「按原计划继续」，`err == nil`。
//
// ⚠️ **这一格一格是承重的**：审计实测过——补这个用例之前，把 9 行**全部**换成
// `_ = rcfg.userText`，`go test ./...` 28 个包全绿、`nested_smoke` 12/12。
// 引擎层那 3 条通道拆分哨兵测的是 `compose_v2.WithResumeUserText` 这个 **RunOption**，
// 而这里是**另一条链**，它们碰不到。
//
// ⚠️ **别用「都在一个 helper 里」来推**：这 9 行是各写各的，没有共用函数。
// 这正是本项目栽过多次的「协议变更 N 复证」形状——`fresh_run_signal_test.go` 里
// 同款教训（九格承重、删两格全套零反应）在这条协议上原样重演了一遍。
func TestResumeUserText_AllOrchestrationsForward(t *testing.T) {
	const text = "换个方向：先做 X"

	cases := []struct {
		name  string
		build func(p agent_v2.Agent) agent_v2.Agent
	}{
		{"sequential", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).AddStep("w", p)
		}},
		{"parallel", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("p")).AddBranch("w", p)
		}},
		{"loop", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewLoopAgent(
				agent_v2.WithLoopName("l"), agent_v2.WithLoopMaxIterations(1),
			).WithBody(p)
		}},
		{"routing", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("r"),
				agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) {
					return "w", nil
				}),
			).AddBranch("w", p)
		}},
		{"reflect", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewReflectAgent(
				agent_v2.WithReflectName("rf"), agent_v2.WithReflectMaxRounds(1),
				agent_v2.WithEvalFunc(func(context.Context, *schema.Message) (bool, error) { return true, nil }),
			).WithActor(p)
		}},
		{"handoff", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewHandoffAgent(
				agent_v2.WithHandoffName("h"), agent_v2.WithHandoffInitial("w"),
				agent_v2.WithHandoffMaxHops(2),
			).AddAgent("w", p)
		}},
		{"plan", func(p agent_v2.Agent) agent_v2.Agent {
			planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
				if len(executed) > 0 {
					return nil, "done", nil
				}
				return []agent_v2.PlanStep{{Description: "干活", SubAgentName: "w"}}, "", nil
			}
			pa := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("pl"))
			pa.AddSubAgent("w", p)
			return pa
		}},
		{"basic", func(p agent_v2.Agent) agent_v2.Agent {
			tk := newProbeToolkit(p)
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					schema.AssistantToolCallMessage([]schema.ToolCall{{
						ID:       "c1",
						Function: schema.FunctionCall{Name: p.Name(), Arguments: `{"input":"干活"}`},
					}}),
					schema.AssistantMessage("好了"),
				})),
			)
		}},
		{"graph", func(p agent_v2.Agent) agent_v2.Agent {
			outCh := compose_v2.LastValue[*schema.Message]("ut_out")
			b := compose_v2.NewBuilder()
			compose_v2.WithChannel(b, outCh)
			b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if _, err := agent_v2.RunSubAgent(ctx, "n", p, schema.UserMessage("干活")); err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{
					compose_v2.Write(outCh, schema.AssistantMessage("ok")),
				}, nil
			})
			b.AddEdge(compose_v2.StartNode, "n")
			b.AddEdge("n", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				panic(err)
			}
			return agent_v2.NewGraphAgent(g,
				agent_v2.WithGraphAgentName("ga"),
				agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(chs, outCh)
					return out
				}),
			)
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			probe := &userTextProbe{}
			top := c.build(probe)
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
			sess := agent_v2.NewRunSession()

			if _, err := top.Run(ctx, sess, schema.UserMessage("原始目标")); err == nil {
				t.Fatalf("首跑应中断（fixture 没搭对，这一格测不到任何东西）")
			}
			probe.saw = ""
			if _, err := top.Resume(ctx, sess.RunID,
				agent_v2.WithResumeUserText(text),
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{
					"probe_q": schema.UserMessage("答"),
				})); err != nil {
				t.Fatalf("Resume: %v", err)
			}
			if !probe.called {
				t.Fatal("探针没被调用——fixture 没搭对")
			}
			// **正向断言**：必须真读到那句话。只断「没报错」的话，递参漏了也是 err==nil。
			if probe.saw != text {
				t.Fatalf("%s 作顶层时没把非寻址型负载递下去：读到 %q，期望 %q\n"+
					"后果：业务用 %s 当顶层时，planner 拿不到用户的新指示——"+
					"静默按原计划继续，err 全程 nil。", c.name, probe.saw, text, c.name)
			}
		})
	}
}

// newProbeToolkit 把探针包成工具（BasicAgent 那一格用——它没有静态子列表）。
func newProbeToolkit(p agent_v2.Agent) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(p))
	return tk
}
