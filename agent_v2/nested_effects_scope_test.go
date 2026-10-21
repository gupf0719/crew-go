package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 嵌套调用的完成记录连同那次调用做出的转交 / 路由决定一起记下（上行信号，见 side_signal.go），拿回时
// 交出。哪些决定发给谁由收件人定：被调方内部自己的 RoutingAgent / HandoffAgent 取走它们自己那一层
// 的，不往外带；嵌套调用里再嵌套调用时，里层交上来的跟着外层那次调用一起记——外层那次调用被拿回时
// 里层根本不跑。
//
// 每条用例都跑「不中断」与「中断后续跑」两遍，结果必须一样：不一样就是记录（或漏记）改变了行为。

type effReply struct {
	name, reply string
	runs        atomic.Int32
}

func (a *effReply) Name() string        { return a.name }
func (a *effReply) Description() string { return "reply " + a.name }
func (a *effReply) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	ev := agent_v2.NewExitEvent(a.name, schema.AssistantMessage(a.reply))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}
func (a *effReply) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// effModel：最后一条是 tool 结果时答 "done"，否则吐一批固定的 tool_call。
type effModel struct {
	calls atomic.Int32
	tc    schema.ToolCall
}

func (m *effModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{m.tc}), nil
}
func (m *effModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *effModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func effRouter(name, target string) (*agent_v2.BasicAgent, *effModel) {
	m := &effModel{tc: schema.ToolCall{ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"` + target + `"}`}}}
	return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3)), m
}

func effTriage(name, target string) *agent_v2.BasicAgent {
	m := &effModel{tc: schema.ToolCall{ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"` + target + `"}`}}}
	return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m), agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
}

// effFunc 是一个 Run 可配置的自研 agent。
type effFunc struct {
	name string
	runs atomic.Int32
	fn   func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error)
}

func (a *effFunc) Name() string        { return a.name }
func (a *effFunc) Description() string { return a.name }
func (a *effFunc) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *effFunc) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	return a.fn(ctx, a, in, opts)
}

func effExit(name, text string) []*agent_v2.AgentEvent {
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(name, schema.AssistantMessage(text))}
}

func effAsk(ctx context.Context, ask bool, q string) error {
	if !ask {
		return nil
	}
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
	return err
}

// effGraph 是一个单节点的 GraphAgent：节点体拿输入文本做 step。
func effGraph(t *testing.T, name string, step func(ctx context.Context, c string) (string, error)) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[string]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if in == nil {
			return nil, errors.New("nil input")
		}
		out, err := step(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, outCh)
			return schema.AssistantMessage(o)
		}))
}

// effDrive 跑到收敛，每次停下对所有提问答「是」（答案累积传），返回最终文本。
func effDrive(t *testing.T, ag agent_v2.Agent, input string) string {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	events, err := ag.Run(ctx, sess, schema.UserMessage(input))
	for i := 0; i < 4 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("是")
		}
		events, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
	}
	if err != nil {
		t.Fatalf("没能收敛：%v", err)
	}
	if m := agent_v2.FinalMessage(events); m != nil {
		return m.Content
	}
	return "<no final>"
}

func effRouting(cls agent_v2.Agent) (*agent_v2.RoutingAgent, *effReply, *effReply) {
	math := &effReply{name: "math", reply: "MATH 已处理"}
	fb := &effReply{name: "fb", reply: "FALLBACK 已处理"}
	return agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingFallback(fb)).AddBranch("math", math), math, fb
}

func effDesk(fd agent_v2.Agent) (*agent_v2.HandoffAgent, *effReply) {
	billing := &effReply{name: "billing", reply: "BILLING 已处理"}
	return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("frontdesk")).
		AddAgent("frontdesk", fd).AddAgent("billing", billing), billing
}

// TestNestedEffects_InnerRoutingAgentWritesAreNotEscapes：分类器经 RunSubAgent 调的是一个完整的
// RoutingAgent，它内部的 route_to 写的是它自己那一层的路由，外层 RoutingAgent 不能跟着走。
func TestNestedEffects_InnerRoutingAgentWritesAreNotEscapes(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprintf("ask=%v", ask), func(t *testing.T) {
			innerRouter, _ := effRouter("inner_router_llm", "x")
			innerX := &effReply{name: "ix", reply: "INNER-X"}
			inner := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("inner"),
				agent_v2.WithRoutingClassifier(innerRouter)).AddBranch("x", innerX)
			cls := effGraph(t, "classifier", func(ctx context.Context, c string) (string, error) {
				if _, err := agent_v2.RunSubAgent(ctx, "inner_rt", inner, schema.UserMessage(c)); err != nil {
					return "", err
				}
				return "已确认", effAsk(ctx, ask, "确认？")
			})
			outerX := &effReply{name: "ox", reply: "OUTER-X"}
			fb := &effReply{name: "fb", reply: "FALLBACK 已处理"}
			rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls),
				agent_v2.WithRoutingFallback(fb)).AddBranch("x", outerX)
			if got := effDrive(t, rt, "hi"); got != "FALLBACK 已处理" || outerX.runs.Load() != 0 {
				t.Errorf("内层 RoutingAgent 的路由被当成了外层的：最终 %q（外层 x 执行 %d 次）", got, outerX.runs.Load())
			}
		})
	}
}

// TestNestedEffects_InnerHandoffAgentWritesAreNotEscapes：前台经 RunSubAgent 调的是一个完整的
// HandoffAgent，它内部的转交归它自己，外层 HandoffAgent 不能跟着转。
func TestNestedEffects_InnerHandoffAgentWritesAreNotEscapes(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprintf("ask=%v", ask), func(t *testing.T) {
			innerBilling := &effReply{name: "billing", reply: "INNER-BILLING"}
			innerH := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("inner_desk"), agent_v2.WithHandoffInitial("a")).
				AddAgent("a", effTriage("inner_a", "billing")).AddAgent("billing", innerBilling)
			fd := &effFunc{name: "frontdesk"}
			fd.fn = func(ctx context.Context, _ agent_v2.Agent, in *schema.Message, _ []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if _, err := agent_v2.RunSubAgent(ctx, "inner", innerH, in); err != nil {
					return nil, err
				}
				if err := effAsk(ctx, ask, "确认？"); err != nil {
					return nil, err
				}
				return effExit("frontdesk", "前台自己答了"), nil
			}
			h, billing := effDesk(fd)
			if got := effDrive(t, h, "hi"); got != "前台自己答了" || billing.runs.Load() != 0 {
				t.Errorf("内层 HandoffAgent 的转交被当成了外层的：最终 %q（外层 billing 执行 %d 次）", got, billing.runs.Load())
			}
		})
	}
}

// TestNestedEffects_WrappedRouterRun_FollowsExecutionNotSession：被调方是个业务壳，它在自己的 Run 里
// 调路由 agent。决定跟着执行走、不跟着 session 对象走：
//
//   - 壳在同一段执行里调（session 传 nil，框架补一个属于所在任务的）：路由是壳这次执行做出的决定，
//     算数——不中断与中断续跑都走 math；
//   - 壳经 DetachTask 开一个独立的运行：那不是这段执行的一部分，它做出的路由不算——两遍都落兜底。
//
// 曾经路由写进 session KV，算不算数取决于路由 agent 拿到的是不是同一个 session 对象（传 nil 时框架
// 补的是另一个对象 ⟹ 不算），跟它是不是这段执行的一部分无关；那时只靠「拿回时别把它重放进调用方
// 的 session」才让两遍一致。
func TestNestedEffects_WrappedRouterRun_FollowsExecutionNotSession(t *testing.T) {
	for _, detached := range []bool{false, true} {
		for _, ask := range []bool{false, true} {
			t.Run(fmt.Sprintf("detached=%v/ask=%v", detached, ask), func(t *testing.T) {
				router, _ := effRouter("router_llm", "math")
				wrap := &effFunc{name: "wrap"}
				// 两种情形都显式转授（路由能力是签名，不是环境）：独立运行那一遍的路由 agent 同样有
				// route_to、同样调了它，不算数是因为它不在这段执行里——不是因为它没有资格。
				wrap.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
					if detached {
						return router.Run(agent_v2.DetachTask(ctx), agent_v2.NewRunSession(), in, agent_v2.RegrantRouteTo(opts, self, router)...)
					}
					return router.Run(ctx, nil, in, agent_v2.RegrantRouteTo(opts, self, router)...)
				}
				cls := effGraph(t, "classifier", func(ctx context.Context, c string) (string, error) {
					if _, err := agent_v2.RunSubAgent(ctx, "wrap", wrap, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, wrap)...); err != nil {
						return "", err
					}
					return "已确认", effAsk(ctx, ask, "确认？")
				})
				rt, math, fb := effRouting(cls)
				got := effDrive(t, rt, "算一下 1+1")
				if detached && (got != "FALLBACK 已处理" || math.runs.Load() != 0) {
					t.Errorf("独立运行做出的路由不该生效：最终 %q（math 执行 %d 次）", got, math.runs.Load())
				}
				if !detached && (got != "MATH 已处理" || math.runs.Load() != 1 || fb.runs.Load() != 0) {
					t.Errorf("同一段执行里调的路由 agent 做出的决定应生效：最终 %q（math %d 次，兜底 %d 次）",
						got, math.runs.Load(), fb.runs.Load())
				}
			})
		}
	}
}

// TestNestedEffects_OuterRecordCarriesInnerTransfer：两层嵌套（前台 → mid → 分诊），内外两层在同一次
// 执行里跑完，前台之后才请人。续跑时拿回的是前台那次对 mid 的记录，它必须带着分诊写下的转交——
// 这里 mid 根本不跑，没有「内层被拿回时重放」可依赖，只能靠写入那一刻沿记录器链上溯记给外层。
func TestNestedEffects_OuterRecordCarriesInnerTransfer(t *testing.T) {
	triage := effTriage("triage_llm", "billing")
	mid := &effFunc{name: "mid"}
	mid.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "triage", triage, in, agent_v2.RegrantHandoffTo(opts, self, triage)...); err != nil {
			return nil, err
		}
		return effExit("mid", "mid done"), nil
	}
	fd := &effFunc{name: "frontdesk"}
	fd.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "mid", mid, in, agent_v2.RegrantHandoffTo(opts, self, mid)...); err != nil {
			return nil, err
		}
		if err := effAsk(ctx, true, "前台确认？"); err != nil {
			return nil, err
		}
		return effExit("frontdesk", "前台自己答了"), nil
	}
	h, billing := effDesk(fd)
	if got := effDrive(t, h, "我要退款"); got != "BILLING 已处理" || billing.runs.Load() != 1 || mid.runs.Load() != 1 {
		t.Errorf("外层记录没带上内层的转交：最终 %q（billing 执行 %d 次，mid 执行 %d 次）", got, billing.runs.Load(), mid.runs.Load())
	}
}

// TestNestedEffects_OuterRecordCarriesInnerRoute：同上，路由那一侧（分类器 → mid → 路由 agent）。
func TestNestedEffects_OuterRecordCarriesInnerRoute(t *testing.T) {
	router, _ := effRouter("router_llm", "math")
	mid := effGraph(t, "mid", func(ctx context.Context, c string) (string, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "route_llm", router, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, router)...); err != nil {
			return "", err
		}
		return "mid done", nil
	})
	cls := effGraph(t, "classifier", func(ctx context.Context, c string) (string, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "mid", mid, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, mid)...); err != nil {
			return "", err
		}
		return "已确认", effAsk(ctx, true, "外层确认？")
	})
	rt, math, _ := effRouting(cls)
	if got := effDrive(t, rt, "算一下 1+1"); got != "MATH 已处理" || math.runs.Load() != 1 {
		t.Errorf("外层记录没带上内层的路由：最终 %q（math 执行 %d 次）", got, math.runs.Load())
	}
}
