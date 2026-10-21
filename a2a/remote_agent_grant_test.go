package a2a_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// grantReplyAgent 每次 Run 都直接答 reply。
type grantReplyAgent struct {
	name, reply string
	runs        atomic.Int32
}

func (a *grantReplyAgent) Name() string        { return a.name }
func (a *grantReplyAgent) Description() string { return "reply " + a.name }
func (a *grantReplyAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	ev := agent_v2.NewExitEvent(a.name, schema.AssistantMessage(a.reply))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}
func (a *grantReplyAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// grantToolModel：先调一次 first 工具，看到工具结果之后答 tail。
type grantToolModel struct {
	first schema.ToolCall
	tail  string
	calls atomic.Int32
}

func (m *grantToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage(m.tail), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{m.first}), nil
}
func (m *grantToolModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *grantToolModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// consultOnStart：agent 级 OnAgentStart 里经 RunSubAgent 请教一个会定路由的 agent，并用 RegrantFromNode
// 试着把能力转授给它。
type consultOnStart struct {
	hook.NoOpHooks
	router agent_v2.Agent
}

func (h *consultOnStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	_, err := agent_v2.RunSubAgent(ctx, "router_on_start", h.router, schema.UserMessage("算一下"), agent_v2.RegrantFromNode(ctx, h.router)...)
	return ctx, err
}

// selfImplAgent：自研 Agent——入口走 agent_v2.EnterRun，自己触发业务钩子。
type selfImplAgent struct{ hooks hook.Hooks }

func (a *selfImplAgent) Name() string        { return "selfimpl" }
func (a *selfImplAgent) Description() string { return "selfimpl" }
func (a *selfImplAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *selfImplAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, a.Name())
	if err != nil {
		return nil, err
	}
	ctx = agent_v2.WithSession(ctx, sess)
	if _, err := compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return a.hooks.OnAgentStart(c, &hook.AgentInfo{Name: a.Name()}, &hook.AgentInput{Messages: []*schema.Message{in}})
	}); err != nil { // 这个桩之后不再用采纳的 ctx
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.Name(), schema.AssistantMessage("self 答"))}, nil
}

// TestRawCalledAgent_AgentLevelHookGetsNoCallerGrant：被授予了路由能力的分类器，在工具里裸调一个
// agent（推了子 ns）；那个 agent 的 agent 级 OnAgentStart 用 RegrantFromNode 转授给一个会定 math 的路由
// agent。agent 级钩子跑在图外、拿不到授予，所以应当没有路由、落到兜底分支。
//
// BasicAgent 是对照（9 个编排的入口本来就遮）。a2a.RemoteAgent 与经 agent_v2.EnterRun 的自研 Agent 曾经
// 只判 session、不遮授予：路由定到了 math，`err == nil`。
func TestRawCalledAgent_AgentLevelHookGetsNoCallerGrant(t *testing.T) {
	base := startServer(t, fnExec(completedExec))
	for _, kind := range []string{"BasicAgent（对照）", "a2a.RemoteAgent", "自研Agent（EnterRun）"} {
		t.Run(kind, func(t *testing.T) {
			rm := &grantToolModel{first: schema.ToolCall{ID: "r1", Type: "function",
				Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}}, tail: "done"}
			router := agent_v2.NewBasicAgent(agent_v2.WithName("router_llm"), agent_v2.WithModel(rm),
				agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
			h := &consultOnStart{router: router}
			var inner agent_v2.Agent
			switch kind {
			case "BasicAgent（对照）":
				inner = agent_v2.NewBasicAgent(agent_v2.WithName("inner"),
					agent_v2.WithModel(&grantToolModel{tail: "x"}), agent_v2.WithHooks(h), agent_v2.WithMaxRounds(1))
			case "a2a.RemoteAgent":
				ra, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHooks(h))
				if err != nil {
					t.Fatal(err)
				}
				inner = ra
			default:
				inner = &selfImplAgent{hooks: h}
			}
			tk := tool.NewToolkit()
			tk.Register(agent_v2.RunnableAsTool("direct", "调起 inner", func(ctx context.Context, input string) (string, error) {
				nctx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "direct"))
				_, _ = inner.Run(nctx, nil, schema.UserMessage(input)) // 这里只关心它的钩子拿没拿到授予；inner 自己成没成不影响
				return "ok", nil
			}))
			cm := &grantToolModel{first: schema.ToolCall{ID: "d1", Type: "function",
				Function: schema.FunctionCall{Name: "direct", Arguments: `{"input":"算一下"}`}}, tail: "想不好"}
			cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(cm),
				agent_v2.WithToolkit(tk), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(5))
			math := &grantReplyAgent{name: "math", reply: "MATH 已处理"}
			chat := &grantReplyAgent{name: "chat", reply: "CHAT 已处理"}
			fb := &grantReplyAgent{name: "fb", reply: "FALLBACK 已处理"}
			rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls),
				agent_v2.WithRoutingFallback(fb)).AddBranch("math", math).AddBranch("chat", chat)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			events, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("算一下"))
			if err != nil {
				t.Fatal(err)
			}
			if rm.calls.Load() == 0 {
				t.Fatal("前提不成立：被请教的路由 agent 一次都没跑")
			}
			final := ""
			if m := agent_v2.FinalMessage(events); m != nil {
				final = m.Content
			}
			if final != "FALLBACK 已处理" || math.runs.Load() != 0 {
				t.Errorf("agent 级钩子不该拿到调用方的路由能力，应落到兜底：最终 %q（math %d 次、兜底 %d 次）",
					final, math.runs.Load(), fb.runs.Load())
			}
		})
	}
}
