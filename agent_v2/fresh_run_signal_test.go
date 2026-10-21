package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// freshSignalProbe 是个只干一件事的子 agent：记下自己被调用时 ctx 里有没有「新开」信号。
type freshSignalProbe struct {
	saw    bool
	called bool
}

func (p *freshSignalProbe) Name() string        { return "probe" }
func (p *freshSignalProbe) Description() string { return "记录 fresh-run 信号" }

func (p *freshSignalProbe) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	p.called = true
	p.saw = compose_v2.IsFreshRunCtx(ctx)
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("probe", schema.AssistantMessage("ok"))}, nil
}

func (p *freshSignalProbe) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestFreshRunSignal_AllOrchestrationsPush 钉住「每个编排的顶层 Run 都把『这是新任务』
// 的信号传下去」。
//
// # 为什么必须逐个编排各一格
//
// 「续跑还是新开」这个判断的信息**只有入口有**——子层看自己的存档只能看到「有一份挂起
// 的」，看不到「它属于上一个已被放弃的任务」。所以顶层必须判一次并往下传，子层照办。
//
// 而顶层可以是**九个编排里的任何一个**（本表 9 格；BasicAgent / GraphAgent 两格是
// 后补的，补之前它们零覆盖——见下面 case 里那段）。漏掉一个，业务用那个当顶层时整条链就失效：
// 被放弃任务的 Pending 子 cp 会被下一个任务捡起来接着跑，**新任务的输入根本进不了图**。
//
// ⚠️ **这一格一格是承重的**：实测过——只有一条端到端泄漏用例（顶层写死 Sequential）时，
// 把 parallel / loop 的 push 点删掉，**全套测试零反应**。「九个都接了」这个事实本身
// 不产生断言，每个都得有自己的那一格。这正是本项目栽过四次的「协议变更四复证」形状。
//
// ⚠️ **别用「都调同一个 helper」来推**：谓词写对了不等于调用点调了它。信号现在确实
// 收敛成了一处（`enterRunTask`），但那**只消灭了「谓词漂移」**——「某个编排忘了
// 调」这个失效模式原样还在，而且它正是本表要钉的那个。
func TestFreshRunSignal_AllOrchestrationsPush(t *testing.T) {
	cases := []struct {
		name  string
		build func(probe agent_v2.Agent) agent_v2.Agent
	}{
		{"sequential", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).AddStep("w", p)
		}},
		{"parallel", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("p")).AddBranch("w", p)
		}},
		{"loop", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewLoopAgent(
				agent_v2.WithLoopName("l"),
				agent_v2.WithLoopMaxIterations(1),
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
				agent_v2.WithReflectName("rf"),
				agent_v2.WithReflectMaxRounds(1),
				agent_v2.WithEvalFunc(func(context.Context, *schema.Message) (bool, error) { return true, nil }),
			).WithActor(p)
		}},
		{"handoff", func(p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewHandoffAgent(
				agent_v2.WithHandoffName("h"),
				agent_v2.WithHandoffInitial("w"),
				agent_v2.WithHandoffMaxHops(2),
			).AddAgent("w", p)
		}},
		// **BasicAgent + GraphAgent 这两格是后补的，补之前它们零覆盖**：审计实测单删
		// 它们各自那行 ctx 下传，`go test ./...` 全绿、`nested_smoke` 12/12——而它俩恰好
		// 是（当时）唯二在自己 `runGraph` 里做这件事的。表里原本写着「九个编排各钉一格」，
		// 实际只有 7 格。**「覆盖了 A 就以为覆盖了 B」是这个坑的第五次**。
		//
		// 它们跟上面 7 个的形状不同：没有静态 sub-agent 列表，子 agent 分别经
		// `AgentAsTool`（进 toolkit）和节点内 `RunSubAgent` 挂进来。
		{"basic", func(p agent_v2.Agent) agent_v2.Agent {
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(p))
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("b"),
				agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(3),
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
			outCh := compose_v2.LastValue[*schema.Message]("ga_out")
			b := compose_v2.NewBuilder()
			compose_v2.WithChannel(b, outCh)
			b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				// 节点内调子 agent 走 RunSubAgent——业务的推荐姿势。
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			probe := &freshSignalProbe{}
			top := c.build(probe)
			if _, err := top.Run(context.Background(), agent_v2.NewRunSession(),
				schema.UserMessage("干点什么")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !probe.called {
				t.Fatalf("探针没被调用——fixture 没搭对，这一格测不到任何东西")
			}
			if !probe.saw {
				t.Fatalf("%s 作顶层时没有把「这是新任务」的信号传下去。\n"+
					"后果：业务用 %s 当顶层时，被放弃任务的 Pending 子 cp 会被下一个任务"+
					"捡起来接着跑，新任务的输入根本进不了图（err 全程 nil）。", c.name, c.name)
			}
		})
	}

	// 反向承重：**嵌套时不能 push**——那会让父 Resume 时子层拒绝 auto-load，
	// 嵌套 HITL 直接断（nested_smoke 从 12/12 掉到 3/12 就是这条）。
	//
	// ⚠️ 「算不算嵌套」的判据是**引擎在节点入口盖的执行章**（`GetCurrentNodeID`），
	// 不是 ns 字符串。所以这一格必须**真的经过一个父图节点**——曾用「手工 push 一个
	// 非空 graphNS」模拟嵌套，那在换判据之后不再等价：业务在顶层自己 push ns（租户
	// 前缀等）恰恰是**顶层**，此时不 push 新开信号才是缺陷（上个任务的残档会被捡走）。
	t.Run("嵌套层不重复 push", func(t *testing.T) {
		probe := &freshSignalProbe{}
		inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("in")).AddStep("w", probe)

		// 真实嵌套：父图的节点执行体里调子编排（引擎已盖章）
		bld := compose_v2.NewBuilder()
		bld.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			_, err := inner.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("干活"))
			return nil, err
		})
		bld.AddEdge(compose_v2.StartNode, "host")
		g, err := bld.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !probe.called {
			t.Fatalf("探针没被调用——fixture 没搭对，这一格测不到任何东西")
		}
		if probe.saw {
			t.Fatal("嵌套层（父图节点执行范围内）不该 push 新开信号——" +
				"父 Resume 时子层会据此拒绝 auto-load，嵌套 HITL 断链")
		}
	})

	// 与上一格互补：**顶层自己 push 了 ns 仍是顶层**，必须 push 新开信号。
	// 换判据之前这一格是坏的（ns 非空被当成嵌套 ⟹ 顶层不声明新任务 ⟹ 上个被放弃
	// 任务的残档被下一个任务捡走）。
	t.Run("顶层自带 ns 仍算顶层", func(t *testing.T) {
		probe := &freshSignalProbe{}
		top := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("top")).AddStep("w", probe)
		ctx := compose_v2.WithGraphNS(context.Background(), "tenant-a")
		if _, err := top.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("干活")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !probe.called {
			t.Fatalf("探针没被调用——fixture 没搭对")
		}
		if !probe.saw {
			t.Fatal("业务在顶层自己 push 了 ns（租户前缀等）时仍必须 push 新开信号——" +
				"否则上个被放弃任务的残档会被这个新任务捡走接着跑，err 全程 nil")
		}
	})
}

// TestFreshRunSignal_ConsumersHonorIt 钉住**消费者侧**：读这个信号的地方都真的读了。
//
// 上面那个测试只钉住**生产者**（九个入口 push 了没有）。审计实测：把两个消费者
// （`GraphAgent` 的播种判断、`compose_v2` 子图节点的续跑判断）漏读的那一项
// 删掉，**全套测试零反应**——生产者测试对它们完全看不见。
//
// 这两处正是本次审计抓到的两条 HIGH，都是「同一份判据的另一个拷贝没跟着改」：
//   - `GraphAgent` 那处的注释白纸黑字写着「必须与下面 WithFreshRun 的判据保持一致」，
//     而我改了 BasicAgent 那份、没改它——**这是同一处坑的第四次**；
//   - 子图节点自己 `LoadNS` + 显式 `WithResumeFrom(cp)`，而引擎闸门第一条就是
//     `resumeFrom == nil`，**整套信号协议在那条路径上被完整绕过**。
func TestFreshRunSignal_ConsumersHonorIt(t *testing.T) {
	// D：GraphAgent 嵌在编排里（它的 godoc 头一句就是这个用法）+ 上个任务留下 Pending 子 cp。
	// 信号漏读时：引擎从 StartNode 起跑、GraphAgent 侧跳过初始写入 ⟹ 节点拿到空输入。
	t.Run("GraphAgent 的播种路径听信号", func(t *testing.T) {
		var seen []string
		g := compose_v2.NewBuilder()
		inCh := compose_v2.LastValue[*schema.Message]("in")
		outCh := compose_v2.LastValue[*schema.Message]("out")
		compose_v2.WithChannel(g, inCh)
		compose_v2.WithChannel(g, outCh)
		askedOnce := false
		g.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			in, _ := compose_v2.Get(chs, inCh)
			if in == nil {
				seen = append(seen, "<nil 输入>")
				return nil, nil
			}
			seen = append(seen, in.Content)
			// **任务 A 必须停在「被放弃」这一步**：成功收尾的话 cleanupOnSuccess 会把
			// 子 cp 删掉，根本没有残留可泄漏——那样这条用例恒真（fixture 第 6 类炸弹，
			// 这个 session 里第六次栽在它上面）。
			if !askedOnce {
				askedOnce = true
				return nil, &compose_v2.InterruptError{
					Reason: "probe_ask",
					Query:  &compose_v2.AskQuery{Question: "确认？", InterruptID: "probe_q"},
				}
			}
			return []compose_v2.ChannelWrite{
				compose_v2.Write(outCh, schema.AssistantMessage("处理了:"+in.Content)),
			}, nil
		})
		g.AddEdge(compose_v2.StartNode, "work")
		g.AddEdge("work", compose_v2.EndNode)
		compiled, err := g.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}

		ga := agent_v2.NewGraphAgent(compiled,
			agent_v2.WithGraphAgentName("ga"),
			agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
			}),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				out, _ := compose_v2.Get(chs, outCh)
				return out
			}),
		)
		// **顶层是 Sequential**——GraphAgent 处在 graphNS 非空的位置，正是漏读会翻车的形态
		seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).AddStep("g", ga)

		cp := compose_v2.NewInMemoryCheckpointer()
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
		sess := agent_v2.NewRunSession()
		// 任务 A：节点抛中断 → **用户就此放弃**（留下 Pending 子 cp）
		_, err = seq.Run(ctx, sess, schema.UserMessage("任务A"))
		if _, isInt := compose_v2.IsInterrupt(err); !isInt {
			t.Fatalf("任务 A 应抛中断留下 Pending 子 cp，got %v", err)
		}
		// 同 session 发第二个任务（不 Resume 任务 A）
		if _, err := seq.Run(ctx, sess, schema.UserMessage("任务B")); err != nil {
			t.Fatalf("任务 B: %v", err)
		}

		// 承重：任务 B 的输入必须真的进图。漏读信号时这里会是 `<nil 输入>` 或者干脆是任务 A。
		if len(seen) < 2 || seen[len(seen)-1] != "任务B" {
			t.Fatalf("任务 B 的输入没进图，节点看到的依次是 %v\n"+
				"——GraphAgent 的播种路径漏读了「这是新任务」信号，"+
				"跑出「两头不着的 fresh run」", seen)
		}
	})
}
