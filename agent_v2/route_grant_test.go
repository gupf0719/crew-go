package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 路由能力是签名不是环境（见 agent_v2/route_grant.go）：RoutingAgent 只把它交给分类器这个对象，往下
// 走要显式转授。曾经资格是环境——分类器在同一段执行里请教的任何带 route_to 的 agent 都能替它定路由。

func grantRouting(cls agent_v2.Agent) (*agent_v2.RoutingAgent, *effReply, *effReply, *effReply) {
	math := &effReply{name: "math", reply: "MATH 已处理"}
	chat := &effReply{name: "chat", reply: "CHAT 已处理"}
	fb := &effReply{name: "fb", reply: "FALLBACK 已处理"}
	return agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingFallback(fb)).AddBranch("math", math).AddBranch("chat", chat), math, chat, fb
}

// chatShell 是一个业务壳分类器：先经 consult 请教别的 agent，再自己用 annotation 定 chat。
func chatShell(consult func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) error) *effFunc {
	shell := &effFunc{name: "shell"}
	shell.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if err := consult(ctx, self, in, opts); err != nil {
			return nil, err
		}
		out := schema.AssistantMessage("chat")
		out.Annotations = map[string]any{"route_target": "chat"}
		return effExitMsg("shell", out), nil
	}
	return shell
}

func effExitMsg(name string, m *schema.Message) []*agent_v2.AgentEvent {
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(name, m)}
}

// TestRouteGrant_ConsultedAgentDoesNotDecide：分类器（业务壳）请教一个带 route_to 的 agent（它会选
// math），自己定 chat。被请教的那个没被授予，它的 route_to 不算——最终走 chat。三种请教方式都一样：
// 同一段执行里裸调、经 RunSubAgent、原样转发收到的 opts（转发 ≠ 授予）。
//
// 「显式转授（对照）」那一组证明被请教的 agent 真的定得了路由：没有它，「它根本没调 route_to」也能
// 让前三组绿。
//
// 曾经资格是环境：前三组全部走 math，`err == nil`（审计实测）。
func TestRouteGrant_ConsultedAgentDoesNotDecide(t *testing.T) {
	cases := []struct {
		name    string
		consult func(helper agent_v2.Agent) func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) error
		want    string
	}{
		{"同一段执行里裸调", func(helper agent_v2.Agent) func(context.Context, agent_v2.Agent, *schema.Message, []agent_v2.RunOption) error {
			return func(ctx context.Context, _ agent_v2.Agent, in *schema.Message, _ []agent_v2.RunOption) error {
				_, err := helper.Run(ctx, nil, in)
				return err
			}
		}, "CHAT 已处理"},
		{"经 RunSubAgent", func(helper agent_v2.Agent) func(context.Context, agent_v2.Agent, *schema.Message, []agent_v2.RunOption) error {
			return func(ctx context.Context, _ agent_v2.Agent, in *schema.Message, _ []agent_v2.RunOption) error {
				_, err := agent_v2.RunSubAgent(ctx, "consult", helper, in)
				return err
			}
		}, "CHAT 已处理"},
		{"原样转发 opts", func(helper agent_v2.Agent) func(context.Context, agent_v2.Agent, *schema.Message, []agent_v2.RunOption) error {
			return func(ctx context.Context, _ agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) error {
				_, err := agent_v2.RunSubAgent(ctx, "consult", helper, in, opts...)
				return err
			}
		}, "CHAT 已处理"},
		{"显式转授（对照）", func(helper agent_v2.Agent) func(context.Context, agent_v2.Agent, *schema.Message, []agent_v2.RunOption) error {
			return func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) error {
				_, err := agent_v2.RunSubAgent(ctx, "consult", helper, in, agent_v2.RegrantRouteTo(opts, self, helper)...)
				return err
			}
		}, "MATH 已处理"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			helper, helperModel := effRouter("helper_llm", "math")
			rt, _, _, _ := grantRouting(chatShell(c.consult(helper)))
			if got := effDrive(t, rt, "随便聊聊"); got != c.want {
				t.Errorf("最终 %q，want %q", got, c.want)
			}
			if helperModel.calls.Load() == 0 {
				t.Error("前提不成立：被请教的 agent 根本没跑")
			}
		})
	}
}

// TestRouteGrant_ToolConsultDoesNotDecide：分类器 BasicAgent 有一件 RunnableAsTool 包的「请教」工具，
// 工具里经 RunSubAgent 调一个带 route_to 的 agent（它选 math）；分类器自己随后 route_to(chat)。被请教的那个不算。
// （工具里裸调 `helper.Run(ctx, nil, …)` 会让它的图落进分类器自己的存档槽，Run 入口拒。）
//
// 曾经两个决定都交进分类器那段执行，工具那个排在前面——同一批之后的下一批 route_to(chat) 后到为准，
// 而先请教、后自己没定的话就是 math（审计实测 R4 形态：请教里定的 math 生效）。这里分两批，钉住的是
// 「请教那次根本交不出决定」：只留请教、去掉分类器自己那次时应落兜底。
func TestRouteGrant_ToolConsultDoesNotDecide(t *testing.T) {
	for _, ownRoute := range []bool{true, false} {
		name := map[bool]string{true: "分类器自己定 chat", false: "只有请教"}[ownRoute]
		t.Run(name, func(t *testing.T) {
			helper, helperModel := effRouter("helper_llm", "math")
			consult := agent_v2.RunnableAsTool("consult", "请教另一个路由器", func(ctx context.Context, input string) (string, error) {
				if _, err := agent_v2.RunSubAgent(ctx, "consult", helper, schema.UserMessage(input)); err != nil {
					return "", err
				}
				return "参考意见已拿到", nil
			})
			tk := tool.NewToolkit()
			tk.Register(consult)
			msgs := []*schema.Message{schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
				Function: schema.FunctionCall{Name: "consult", Arguments: `{"input":"算一下"}`}}})}
			if ownRoute {
				msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Type: "function",
					Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"chat"}`}}}))
			} else {
				msgs = append(msgs, schema.AssistantMessage("想不好"))
			}
			cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(newStubModel(msgs)),
				agent_v2.WithToolkit(tk), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(5))
			rt, _, _, _ := grantRouting(cls)
			want := map[bool]string{true: "CHAT 已处理", false: "FALLBACK 已处理"}[ownRoute]
			if got := effDrive(t, rt, "随便聊聊"); got != want {
				t.Errorf("最终 %q，want %q", got, want)
			}
			if helperModel.calls.Load() == 0 {
				t.Error("前提不成立：被请教的 agent 根本没跑")
			}
		})
	}
}

// TestRegrantFromNode_OnlyTheGrantedLayer：分类器是 GraphAgent。节点里显式 RegrantFromNode 转给路由
// agent ⟹ 它定的 math 算数（对照）；节点调起的另一个 agent（孙辈那一层）在自己的 Run 里调
// RegrantFromNode 拿到的是空的——它不在被授予的那张图里——路由 agent 定不了，落兜底。
func TestRegrantFromNode_OnlyTheGrantedLayer(t *testing.T) {
	for _, viaGrandchild := range []bool{false, true} {
		name := map[bool]string{false: "节点显式转授（对照）", true: "孙辈借用外层的"}[viaGrandchild]
		t.Run(name, func(t *testing.T) {
			router, routerModel := effRouter("router_llm", "math")
			mid := &effFunc{name: "mid"}
			mid.fn = func(ctx context.Context, _ agent_v2.Agent, in *schema.Message, _ []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if _, err := agent_v2.RunSubAgent(ctx, "router", router, in, agent_v2.RegrantFromNode(ctx, router)...); err != nil {
					return nil, err
				}
				return effExit("mid", "mid done"), nil
			}
			cls := effGraph(t, "classifier", func(ctx context.Context, c string) (string, error) {
				var err error
				if viaGrandchild {
					_, err = agent_v2.RunSubAgent(ctx, "mid", mid, schema.UserMessage(c))
				} else {
					_, err = agent_v2.RunSubAgent(ctx, "router", router, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, router)...)
				}
				return "已分好", err
			})
			rt, _, _, _ := grantRouting(cls)
			want := map[bool]string{false: "MATH 已处理", true: "FALLBACK 已处理"}[viaGrandchild]
			if got := effDrive(t, rt, "算一下"); got != want {
				t.Errorf("最终 %q，want %q", got, want)
			}
			if routerModel.calls.Load() == 0 {
				t.Error("前提不成立：路由 agent 根本没跑")
			}
		})
	}
}

// bindCaptureModel 记下每次 WithTools 绑定的工具名，第一轮吐一个 route_to、看到工具结果后作答。
type bindCaptureModel struct {
	mu    sync.Mutex
	bound [][]string
	calls atomic.Int32
	last  *schema.Message
}

func (m *bindCaptureModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if n := len(msgs); n > 0 && msgs[n-1].Role == schema.RoleTool {
		m.mu.Lock()
		m.last = msgs[n-1]
		m.mu.Unlock()
		return schema.AssistantMessage("我自己答"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "r1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}}}), nil
}
func (m *bindCaptureModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *bindCaptureModel) WithTools(infos []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(infos))
	for _, ti := range infos {
		names = append(names, ti.Name)
	}
	m.mu.Lock()
	m.bound = append(m.bound, names)
	m.mu.Unlock()
	return m, nil
}

// TestRouteTool_NotGrantedHasNoTool：配了 WithRouteTool、但不在 RoutingAgent 的分类器位置上（没被授予）
// 的 agent，模型看不到 route_to；硬发一个（例如照着历史）拿到 RouteNoCoordinatorErrCode 软失败、接着
// 作答，Run 不报错。
func TestRouteTool_NotGrantedHasNoTool(t *testing.T) {
	m := &bindCaptureModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("lonely"), agent_v2.WithModel(m), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content != "我自己答" {
		t.Errorf("软失败之后应接着作答，实际 %+v", f)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, names := range m.bound {
		for _, n := range names {
			if n == agent_v2.RouteTool {
				t.Errorf("没被授予的 agent 不该看到 %s（绑定了 %v）", agent_v2.RouteTool, names)
			}
		}
	}
	if m.last == nil || !strings.Contains(m.last.Content, agent_v2.RouteNoCoordinatorErrCode) {
		t.Errorf("硬发的 route_to 应拿到 %s 软失败，实际 %+v", agent_v2.RouteNoCoordinatorErrCode, m.last)
	}
}

// TestOutputSchema_MustRejectRouteTool：WithOutputSchema + WithRouteTool 必须构造期 panic（结构化输出与
// 路由同为 return-direct，未实现裁决）。WithRouteTool 改成只翻 opt-in 之后，「toolkit 里有没有 route_to」
// 那条判据永远读不到——转交那侧踩过同一个坑（见 TestOutputSchema_MustRejectHandoffTransfer）。
func TestOutputSchema_MustRejectRouteTool(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("WithOutputSchema + WithRouteTool 必须构造期 panic")
		}
		if msg, _ := r.(string); !strings.Contains(msg, agent_v2.RouteTool) {
			t.Errorf("panic 信息该点名冲突的那件东西，实际 %q", msg)
		}
	}()
	_ = agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x")})),
		agent_v2.WithRouteTool(),
		agent_v2.WithOutputSchema("Ans", map[string]any{"type": "object"}),
	)
}

// regrantingClassifier 建一个 GraphAgent 分类器：call 决定节点体怎么跑（在哪个位置上显式转授给路由
// agent），节点产出固定的「已分好」——路由只能来自被转授的那个路由 agent。
func regrantingClassifier(t *testing.T, call func(ctx context.Context) error, inSubgraph bool) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[string]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !inSubgraph {
			if err := call(ctx); err != nil {
				return nil, err
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, "已分好")}, nil
	})
	if inSubgraph {
		sb := compose_v2.NewBuilder()
		sb.AddNode("inner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, call(ctx)
		})
		sb.AddEdge(compose_v2.StartNode, "inner")
		sb.AddEdge("inner", compose_v2.EndNode)
		sub, err := sb.Compile()
		if err != nil {
			t.Fatalf("compile sub: %v", err)
		}
		b.AddSubgraphNode("sub", sub)
		b.AddEdge(compose_v2.StartNode, "sub")
		b.AddEdge("sub", "n")
	} else {
		b.AddEdge(compose_v2.StartNode, "n")
	}
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("classifier"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, outCh)
			return schema.AssistantMessage(o)
		}))
}

// TestRegrantFromNode_SubgraphAndNestedGraphNodes：GraphAgent 分类器的子图节点、节点里直接 compose_v2.Run
// 的嵌套图节点，都是这个 GraphAgent 自己图里的代码——显式 RegrantFromNode 转给路由 agent，路由算数。
//
// 曾经按 ns 判「是不是本层」，这两处跑在子 ns 里被当成孙辈，转授静默拿不到、路由落到兜底，
// `err == nil`（审计实测）。顶层节点那组是对照。
func TestRegrantFromNode_SubgraphAndNestedGraphNodes(t *testing.T) {
	for _, pos := range []string{"顶层节点（对照）", "子图里的节点", "节点里直接 Run 的嵌套图"} {
		t.Run(pos, func(t *testing.T) {
			router, routerModel := effRouter("router_llm", "math")
			regrant := func(ctx context.Context) error {
				_, err := agent_v2.RunSubAgent(ctx, "router", router, schema.UserMessage("算一下"), agent_v2.RegrantFromNode(ctx, router)...)
				return err
			}
			call := regrant
			if pos == "节点里直接 Run 的嵌套图" {
				nb := compose_v2.NewBuilder()
				nb.AddNode("inner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					return nil, regrant(ctx)
				})
				nb.AddEdge(compose_v2.StartNode, "inner")
				nb.AddEdge("inner", compose_v2.EndNode)
				nested, err := nb.Compile()
				if err != nil {
					t.Fatalf("compile nested: %v", err)
				}
				call = func(ctx context.Context) error {
					return compose_v2.Run(ctx, nested, compose_v2.NewChannelManager(),
						compose_v2.WithRunNS(compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "nested")))
				}
			}
			cls := regrantingClassifier(t, call, pos == "子图里的节点")
			rt, _, _, _ := grantRouting(cls)
			if got := effDrive(t, rt, "算一下"); got != "MATH 已处理" {
				t.Errorf("被授予的 GraphAgent 在自己的图里显式转授，路由应生效：最终 %q", got)
			}
			if routerModel.calls.Load() == 0 {
				t.Error("前提不成立：路由 agent 根本没跑")
			}
		})
	}
}

// regrantingMember 是一个业务 agent：在自己的 Run 里经 RunSubAgent 调路由 agent，并用 RegrantFromNode
// 试着把「上方」的授予转过去。它从编排那里拿能力只该走签名（opts），拿不到上方放给节点代码的那份。
func regrantingMember(router agent_v2.Agent) *effFunc {
	m := &effFunc{name: "member"}
	m.fn = func(ctx context.Context, _ agent_v2.Agent, in *schema.Message, _ []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "router", router, in, agent_v2.RegrantFromNode(ctx, router)...); err != nil {
			return nil, err
		}
		return effExit("member", "成员答了"), nil
	}
	return m
}

// TestRegrantFromNode_FrameworkBoundariesShadow：框架把另一个 agent 调起来的入口（编排的成员、
// HandoffAgent 的候选、RoutingAgent 的分类器）都给被调方放一份空的。分类器（BasicAgent）的工具里直接
// 跑一个编排（不经 RunSubAgent，但推了子 ns），编排里的业务 agent 用 RegrantFromNode 拿不到分类器那份。
//
// 「拿得到」时路由 agent 定的 math 会生效——前两组发给外层 RoutingAgent（外层走 math），第三组发给内层
// （内层走 math）。分类器自己什么也不定，所以都该落兜底。
func TestRegrantFromNode_FrameworkBoundariesShadow(t *testing.T) {
	innerMath := &effReply{name: "inner_math", reply: "INNER-MATH"}
	innerFb := &effReply{name: "inner_fb", reply: "INNER-FALLBACK"}
	cases := []struct {
		name  string
		build func(router agent_v2.Agent) agent_v2.Agent
	}{
		{"编排的成员", func(r agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("m", regrantingMember(r))
		}},
		{"HandoffAgent 的候选", func(r agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("m")).
				AddAgent("m", regrantingMember(r))
		}},
		{"RoutingAgent 的分类器", func(r agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("inner_rt"), agent_v2.WithRoutingClassifier(regrantingMember(r)),
				agent_v2.WithRoutingFallback(innerFb)).AddBranch("math", innerMath)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			innerMath.runs.Store(0)
			innerFb.runs.Store(0)
			router, routerModel := effRouter("router_llm", "math")
			inner := c.build(router)
			tk := tool.NewToolkit()
			tk.Register(agent_v2.RunnableAsTool("direct", "直接跑一个编排", func(ctx context.Context, input string) (string, error) {
				nctx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "direct"))
				_, err := inner.Run(nctx, nil, schema.UserMessage(input))
				return "ok", err
			}))
			cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "d1", Type: "function",
					Function: schema.FunctionCall{Name: "direct", Arguments: `{"input":"算一下"}`}}}),
				schema.AssistantMessage("想不好")})),
				agent_v2.WithToolkit(tk), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(5))
			rt, _, _, _ := grantRouting(cls)
			got := effDrive(t, rt, "算一下")
			if got != "FALLBACK 已处理" || innerMath.runs.Load() != 0 {
				t.Errorf("编排里的业务 agent 拿到了分类器那份授予：外层最终 %q，内层 math 被调 %d 次", got, innerMath.runs.Load())
			}
			if routerModel.calls.Load() == 0 {
				t.Error("前提不成立：路由 agent 根本没跑")
			}
		})
	}
}

// TestRouteTool_GrantedWithoutCoordinatorSoftFails：被授予了路由能力、上下文里却没有在等的 RoutingAgent
// （分类器的工具经 DetachTask 开了独立任务，又显式把授予交了过去）——route_to 回软失败，模型接着作答。
//
// 曾经照样回「已路由到 math」并短路收尾：模型以为定了路由，实际什么也没发生（审计实测）。
func TestRouteTool_GrantedWithoutCoordinatorSoftFails(t *testing.T) {
	hm := &bindCaptureModel{}
	helper := agent_v2.NewBasicAgent(agent_v2.WithName("detached_llm"), agent_v2.WithModel(hm),
		agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
	tk := tool.NewToolkit()
	tk.Register(agent_v2.RunnableAsTool("detach", "开一个独立任务", func(ctx context.Context, input string) (string, error) {
		_, err := helper.Run(agent_v2.DetachTask(ctx), agent_v2.NewRunSession(), schema.UserMessage(input),
			agent_v2.RegrantFromNode(ctx, helper)...)
		return "ok", err
	}))
	cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "t1", Type: "function",
			Function: schema.FunctionCall{Name: "detach", Arguments: `{"input":"算"}`}}}),
		schema.AssistantMessage("想不好")})),
		agent_v2.WithToolkit(tk), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(5))
	rt, _, _, _ := grantRouting(cls)
	if got := effDrive(t, rt, "随便"); got != "FALLBACK 已处理" {
		t.Errorf("独立任务的路由不该生效：最终 %q", got)
	}
	hm.mu.Lock()
	defer hm.mu.Unlock()
	bound := false
	for _, names := range hm.bound {
		for _, n := range names {
			bound = bound || n == agent_v2.RouteTool
		}
	}
	if !bound {
		t.Fatal("前提不成立：独立任务里的 agent 没拿到显式交过去的授予")
	}
	if hm.last == nil || !strings.Contains(hm.last.Content, agent_v2.RouteNoCoordinatorErrCode) {
		t.Errorf("没有人在等这次路由，route_to 应回 %s 软失败、模型接着作答，实际模型看到 %+v",
			agent_v2.RouteNoCoordinatorErrCode, hm.last)
	}
}

// consultOnAgentStart：agent 级 OnAgentStart 里经 RunSubAgent 请教路由 agent，并用 RegrantFromNode 试着转授。
type consultOnAgentStart struct {
	hook.NoOpHooks
	router agent_v2.Agent
}

func (h *consultOnAgentStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	_, err := agent_v2.RunSubAgent(ctx, "router_on_start", h.router, schema.UserMessage("算一下"), agent_v2.RegrantFromNode(ctx, h.router)...)
	return ctx, err
}

// TestRegrantFromNode_RawRunAgentLevelHookGetsNoGrant：分类器（被授予路由）的工具里调起一个 BasicAgent，
// 它的 agent 级 OnAgentStart 钩子用 RegrantFromNode 转授给一个会定 math 的路由 agent。agent 级钩子跑在
// 自己的图外、拿不到授予——不管是经 RunSubAgent 调起，还是节点里裸调（推了子 ns）。曾经裸调时编排入口
// 没遮住 ctx 上调用方那份，钩子拿到了分类器的路由能力，被请教的 agent 替分类器定了路由（最终走 math），
// `err == nil`。
func TestRegrantFromNode_RawRunAgentLevelHookGetsNoGrant(t *testing.T) {
	for _, raw := range []bool{true, false} {
		t.Run(map[bool]string{true: "裸调+推子ns", false: "经RunSubAgent"}[raw], func(t *testing.T) {
			router, routerModel := effRouter("router_llm", "math")
			inner := agent_v2.NewBasicAgent(agent_v2.WithName("inner"),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("inner 答了")})),
				agent_v2.WithHooks(&consultOnAgentStart{router: router}), agent_v2.WithMaxRounds(3))
			tk := tool.NewToolkit()
			tk.Register(agent_v2.RunnableAsTool("direct", "调起 inner", func(ctx context.Context, input string) (string, error) {
				if raw {
					nctx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "direct"))
					_, err := inner.Run(nctx, nil, schema.UserMessage(input))
					return "ok", err
				}
				_, err := agent_v2.RunSubAgent(ctx, "inner", inner, schema.UserMessage(input))
				return "ok", err
			}))
			cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "d1", Type: "function",
					Function: schema.FunctionCall{Name: "direct", Arguments: `{"input":"算一下"}`}}}),
				schema.AssistantMessage("想不好")})),
				agent_v2.WithToolkit(tk), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(5))
			rt, math, _, fb := grantRouting(cls)
			got := effDrive(t, rt, "算一下")
			if routerModel.calls.Load() == 0 {
				t.Fatal("前提不成立：路由 agent 根本没跑")
			}
			if got != "FALLBACK 已处理" || math.runs.Load() != 0 || fb.runs.Load() != 1 {
				t.Errorf("agent 级钩子不该拿到分类器的路由能力：最终 %q（math %d 次、兜底 %d 次）", got, math.runs.Load(), fb.runs.Load())
			}
		})
	}
}
