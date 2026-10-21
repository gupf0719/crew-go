package agent_v2_test

// 采纳钩子返回的 ctx 按它是哪来的分（compose_v2.CallHook）：派生的听钩子的，外来的（别处缓存的、
// context.Background()）只借业务值，环境、调用方可配的与取消都取框架这次的。下面每条都是曾经的实测
// 缺陷（全程 err == nil），各带一组钩子不违约的对照。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// staleAgentStart：第一次 OnAgentStart 存下 ctx，之后（stale 时）都返回那一个——为了把 span 挂在一起；
// 钩子对象挂在 agent 上，跨请求共用。
type staleAgentStart struct {
	hook.NoOpHooks
	stale  bool
	mu     sync.Mutex
	cached context.Context
}

func (h *staleAgentStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cached == nil {
		h.cached = ctx
		return ctx, nil
	}
	if h.stale {
		return h.cached, nil
	}
	return ctx, nil
}

// backgroundAgentStart：OnAgentStart 直接返回 context.Background()。
type backgroundAgentStart struct{ hook.NoOpHooks }

func (backgroundAgentStart) OnAgentStart(context.Context, *hook.AgentInfo, *hook.AgentInput) (context.Context, error) {
	return context.Background(), nil
}

// ownerSpyModel 流式吐 "A"、"B"，记下每次被调时 ctx 上的调用主体。
type ownerSpyModel struct {
	mu     sync.Mutex
	owners []string
}

func (m *ownerSpyModel) note(ctx context.Context) {
	m.mu.Lock()
	m.owners = append(m.owners, compose_v2.GetOwner(ctx))
	m.mu.Unlock()
}
func (m *ownerSpyModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.note(ctx)
	return schema.AssistantMessage("AB"), nil
}
func (m *ownerSpyModel) Stream(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.note(ctx)
	sr, send := schema.NewStreamReader[*schema.Message](4)
	send(&schema.Message{Role: schema.RoleAssistant, Content: "A"})
	send(&schema.Message{Role: schema.RoleAssistant, Content: "B"})
	schema.CloseSend(sr)
	return sr, nil
}
func (m *ownerSpyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_StaleAgentStart_EnvOfThisRequest：两个请求像 agui 那样各自往 ctx 里推自己的编排
// 钩子（每个连接一个）与调用主体，同一个 agent。OnAgentStart 返回第一个请求存下的 ctx 时，第二个请求的
// 流式片段曾经全部送到第一个请求的连接上（Run1 收 4 个、Run2 收 0 个），模型看到的调用主体也是第一个
// 请求的。
func TestHookCtxOrigin_StaleAgentStart_EnvOfThisRequest(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("返回存下的ctx=%v", stale), func(t *testing.T) {
			m := &ownerSpyModel{}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(m), agent_v2.WithHooks(&staleAgentStart{stale: stale}))
			rec1, rec2 := &chunkCounter{}, &chunkCounter{}
			req := func(owner string, rec *chunkCounter) context.Context {
				ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
				return agent_v2.WithCtxOrchHooks(compose_v2.WithOwner(ctx, owner), rec)
			}
			for i, ctx := range []context.Context{req("alice", rec1), req("bob", rec2)} {
				if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("Q%d", i+1))); err != nil {
					t.Fatalf("Run %d: %v", i+1, err)
				}
			}
			if rec1.chunks.Load() != 2 || rec2.chunks.Load() != 2 {
				t.Errorf("每个请求的片段应只交给它自己的连接（各 2 个）：Run1 %d、Run2 %d", rec1.chunks.Load(), rec2.chunks.Load())
			}
			if fmt.Sprint(m.owners) != "[alice bob]" {
				t.Errorf("模型看到的调用主体应依次是 alice、bob，实际 %v", m.owners)
			}
		})
	}
}

// keywordTransferModel：最后一条是工具结果时答 done；用户输入含「转销售」时转给 sales；否则答 FRONT。
type keywordTransferModel struct{}

func (keywordTransferModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	for _, m := range msgs {
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "转销售") {
			return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "x1", Type: "function",
				Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"sales"}`}}}), nil
		}
	}
	return schema.AssistantMessage("FRONT 已处理"), nil
}
func (keywordTransferModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m keywordTransferModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_HandoffAgentStart_NoCrossTenantActive：两个租户递了同一个会话 ID（客户端生成，撞得上）。
// 调用主体进转交状态的键，正是为了这种情况下互不串。HandoffAgent 的 OnAgentStart 返回 alice 那次存下的
// ctx，或者直接返回 context.Background()（不缓存，只是没照契约写）时，bob 的全新对话曾经直接读到 alice
// 的转交状态、落到 sales——返回 Background 时调用主体变成空串，所有租户共用一个桶。
func TestHookCtxOrigin_HandoffAgentStart_NoCrossTenantActive(t *testing.T) {
	for _, c := range []struct {
		name string
		h    func() hook.Hooks
	}{
		{"派生（对照）", func() hook.Hooks { return &staleAgentStart{} }},
		{"返回存下的ctx", func() hook.Hooks { return &staleAgentStart{stale: true} }},
		{"返回Background", func() hook.Hooks { return backgroundAgentStart{} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			front := agent_v2.NewBasicAgent(agent_v2.WithName("front_llm"), agent_v2.WithModel(keywordTransferModel{}),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			sales := &effReply{name: "sales", reply: "SALES 已处理"}
			desk := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front"),
				agent_v2.WithHandoffHooks(c.h())).AddAgent("front", front).AddAgent("sales", sales)
			cp := compose_v2.NewInMemoryCheckpointer()
			req := func(owner string) context.Context {
				ctx := compose_v2.WithOwner(compose_v2.WithCheckpointerCtx(context.Background(), cp), owner)
				return compose_v2.WithThreadIDCtx(ctx, "t-1")
			}
			ev1, err := desk.Run(req("alice"), agent_v2.NewRunSession(), schema.UserMessage("请转销售"))
			if err != nil {
				t.Fatal(err)
			}
			ev2, err := desk.Run(req("bob"), agent_v2.NewRunSession(), schema.UserMessage("你好"))
			if err != nil {
				t.Fatal(err)
			}
			if got := agent_v2.FinalMessage(ev1).Content; got != "SALES 已处理" {
				t.Fatalf("前提不成立：alice 的对话应转到 sales，实际 %q", got)
			}
			if got := agent_v2.FinalMessage(ev2).Content; got != "FRONT 已处理" {
				t.Errorf("bob 的全新对话应从 front 开始，实际 %q（读到了 alice 的转交状态）", got)
			}
		})
	}
}

// fixedReplyModel 永远答同一句。
type fixedReplyModel struct{ reply string }

func (m fixedReplyModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.reply), nil
}
func (m fixedReplyModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m fixedReplyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_SharedHookStaleCtx_OwnStore：一个钩子对象挂在两个各自配了 store 的 agent 上（一个
// 日志 / trace 钩子给所有 agent，常见），第二个 agent 的 OnAgentStart 返回第一个那次存下的 ctx：b 的
// 长期记忆曾经写进了 a 的 store。
func TestHookCtxOrigin_SharedHookStaleCtx_OwnStore(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("返回存下的ctx=%v", stale), func(t *testing.T) {
			h := &staleAgentStart{stale: stale}
			stA, stB := store.NewInMemoryStore(), store.NewInMemoryStore()
			ns := store.NS("mem")
			a := agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithModel(fixedReplyModel{"RA"}),
				agent_v2.WithStore(stA), agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}), agent_v2.WithHooks(h))
			b := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(fixedReplyModel{"RB"}),
				agent_v2.WithStore(stB), agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}), agent_v2.WithHooks(h))
			effDrive(t, agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("a", a).AddStep("b", b), "hi")
			count := func(st store.Store) int {
				items, err := st.Search(context.Background(), ns, store.WithLimit(100))
				if err != nil {
					t.Fatal(err)
				}
				return len(items)
			}
			if na, nb := count(stA), count(stB); na != 1 || nb != 1 {
				t.Errorf("a、b 的长期记忆应各写进自己的 store（各 1 条），实际 stA %d 条、stB %d 条", na, nb)
			}
		})
	}
}

// askToolModel：最后一条是工具结果时答 done，否则调 approve 工具。
type askToolModel struct{}

func (askToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "ap1", Type: "function",
		Function: schema.FunctionCall{Name: "approve", Arguments: `{}`}}}), nil
}
func (askToolModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m askToolModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_StaleAgentStart_OwnCheckpointer：两个请求各用自己的 checkpointer（比如按租户分库）。
// OnAgentStart 返回第一个请求存下的 ctx 时，第二个请求的挂起档曾经落进第一个请求的 checkpointer，拿自己
// 的续跑报「没有存档」。
func TestHookCtxOrigin_StaleAgentStart_OwnCheckpointer(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("返回存下的ctx=%v", stale), func(t *testing.T) {
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("approve", "approve", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
					if err != nil {
						return nil, err
					}
					return &tool.Result{Success: true, Output: ans.Content}, nil
				}))
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(askToolModel{}), agent_v2.WithToolkit(tk),
				agent_v2.WithHooks(&staleAgentStart{stale: stale}), agent_v2.WithMaxRounds(3))
			cp1, cp2 := compose_v2.NewInMemoryCheckpointer(), compose_v2.NewInMemoryCheckpointer()
			ctx1 := compose_v2.WithCheckpointerCtx(context.Background(), cp1)
			ctx2 := compose_v2.WithCheckpointerCtx(context.Background(), cp2)
			if _, err := ag.Run(ctx1, agent_v2.NewRunSession(), schema.UserMessage("Q1")); err == nil {
				t.Fatal("前提：Run1 应挂起")
			}
			s2 := agent_v2.NewRunSession()
			_, err := ag.Run(ctx2, s2, schema.UserMessage("Q2"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("前提：Run2 应挂起，实际 %v", err)
			}
			p1, _ := cp1.ListPending(context.Background(), s2.RunID)
			p2, _ := cp2.ListPending(context.Background(), s2.RunID)
			if len(p1) != 0 || len(p2) == 0 {
				t.Errorf("Run2 的挂起档应落在它自己的 checkpointer 上：请求 1 的 %d 份、自己的 %d 份", len(p1), len(p2))
			}
			if _, err := ag.Resume(ctx2, s2.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}), agent_v2.WithResumeSession(s2)); err != nil {
				t.Errorf("用自己的 checkpointer 续跑应成功：%v", err)
			}
		})
	}
}

// cancelOnNextModel：看到 3 条工具结果之前一直调 noop；cancelNext 非空时，下一次被调时先执行它（模拟
// 客户端断开）。
type cancelOnNextModel struct {
	cancelNext atomic.Pointer[context.CancelFunc]
}

func (m *cancelOnNextModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if f := m.cancelNext.Swap(nil); f != nil {
		(*f)()
	}
	n := 0
	for _, x := range msgs {
		if x.Role == schema.RoleTool {
			n++
		}
	}
	if n >= 3 {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("n%d", n), Type: "function",
		Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}}}), nil
}
func (m *cancelOnNextModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *cancelOnNextModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_StaleAgentStart_CallerCancelStops：Run1 正常跑完（钩子存下它的 ctx）；Run2 跑起来后
// 调用方取消（客户端断开）。OnAgentStart 返回存下的 ctx 时，取消曾经传不进来：工具又跑了 3 次、err == nil。
func TestHookCtxOrigin_StaleAgentStart_CallerCancelStops(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("返回存下的ctx=%v", stale), func(t *testing.T) {
			var toolRuns atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					toolRuns.Add(1)
					return &tool.Result{Success: true, Output: "ok"}, nil
				}))
			m := &cancelOnNextModel{}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(6), agent_v2.WithHooks(&staleAgentStart{stale: stale}))
			cp := compose_v2.NewInMemoryCheckpointer()
			if _, err := ag.Run(compose_v2.WithCheckpointerCtx(context.Background(), cp), agent_v2.NewRunSession(), schema.UserMessage("Q1")); err != nil {
				t.Fatalf("Run1: %v", err)
			}
			before := toolRuns.Load()
			ctx2, cancel := context.WithCancel(compose_v2.WithCheckpointerCtx(context.Background(), cp))
			defer cancel()
			cf := context.CancelFunc(cancel)
			m.cancelNext.Store(&cf)
			_, err := ag.Run(ctx2, agent_v2.NewRunSession(), schema.UserMessage("Q2"))
			if !errors.Is(err, context.Canceled) {
				t.Errorf("调用方取消了这次请求，Run2 应以 context.Canceled 停下，实际 err=%v、之后工具又跑了 %d 次", err, toolRuns.Load()-before)
			}
		})
	}
}

// softCancelGuard：OnAgentStart 给这次 Run 装一个软取消（管理端的「预算守卫」，在派生的 ctx 上装——合乎
// 契约），第一次 OnModelEnd 之后触发它。
type softCancelGuard struct {
	hook.NoOpHooks
	install  bool
	sig      *compose_v2.SoftCancelSignal
	modelEnd atomic.Int32
}

func (h *softCancelGuard) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if !h.install {
		return ctx, nil
	}
	h.sig = compose_v2.NewSoftCancelSignal()
	return compose_v2.WithSoftCancelCtx(ctx, h.sig), nil
}

func (h *softCancelGuard) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	if h.modelEnd.Add(1) == 1 {
		h.sig.Trigger("token budget exceeded")
	}
	return ctx, nil
}

// loopToolModel：前 n 次调 noop 工具，之后答 done。
type loopToolModel struct {
	calls atomic.Int32
	n     int32
}

func (m *loopToolModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	c := m.calls.Add(1)
	if c > m.n {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("n%d", c), Type: "function",
		Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}}}), nil
}
func (m *loopToolModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *loopToolModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_HookInstalledSoftCancel：钩子在派生的 ctx 上装的软取消要生效。曾经软取消按执行状态
// 一律取框架的：触发之后模型又被调了 4 次、工具跑了 4 次、err == nil。对照组在 Run 之前把同一个信号推好。
func TestHookCtxOrigin_HookInstalledSoftCancel(t *testing.T) {
	for _, viaHook := range []bool{false, true} {
		t.Run(fmt.Sprintf("经OnAgentStart装=%v", viaHook), func(t *testing.T) {
			var toolRuns atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					toolRuns.Add(1)
					return &tool.Result{Success: true, Output: "ok"}, nil
				}))
			g := &softCancelGuard{install: viaHook}
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			if !viaHook {
				g.sig = compose_v2.NewSoftCancelSignal()
				ctx = compose_v2.WithSoftCancelCtx(ctx, g.sig)
			}
			m := &loopToolModel{n: 4}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(8), agent_v2.WithHooks(g))
			_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
			if !errors.Is(err, compose_v2.ErrSoftCanceled) || m.calls.Load() != 1 {
				t.Errorf("第一次模型调用之后触发了软取消，Run 应在下一个超步边界停下：err=%v、模型被调 %d 次、工具跑了 %d 次",
					err, m.calls.Load(), toolRuns.Load())
			}
		})
	}
}

// payToolModel：最后一条是工具结果时答 done，否则调 pay。
type payToolModel struct{}

func (payToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "pay1", Type: "function",
		Function: schema.FunctionCall{Name: "pay", Arguments: `{"amount":10}`}}}), nil
}
func (payToolModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m payToolModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtxOrigin_ToolMiddlewareStaleCtx：中间件第一次被调时存下 ctx，之后都把那一个交给 next（为了把
// span 挂在一起）。工具里的提问曾经每次续跑都换一个 ID：问了 4 次仍挂着、一次都没付款。
func TestHookCtxOrigin_ToolMiddlewareStaleCtx(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("交给next存下的ctx=%v", stale), func(t *testing.T) {
			var mu sync.Mutex
			var cached context.Context
			mw := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
					mu.Lock()
					if cached == nil {
						cached = ctx
					} else if stale {
						ctx = cached
					}
					mu.Unlock()
					return next(ctx, name, args)
				}
			}
			var paid atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("pay", "pay", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准付款 10 元？"})
					if err != nil {
						return nil, err
					}
					paid.Add(1)
					return &tool.Result{Success: true, Output: "paid:" + ans.Content}, nil
				}))
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(payToolModel{}), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(3), agent_v2.WithToolMiddleware(mw))
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			answers := map[string]*schema.Message{}
			_, err := ag.Run(ctx, sess, schema.UserMessage("付 10 元"))
			asks := 0
			for i := 0; i < 4 && err != nil; i++ {
				ie, ok := compose_v2.IsInterrupt(err)
				if !ok {
					break
				}
				asks++
				for _, q := range ie.AllQueries() {
					answers[q.InterruptID] = schema.UserMessage("是")
				}
				_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			}
			if err != nil || asks != 1 || paid.Load() != 1 {
				t.Errorf("问一次、答一次就该付款并收尾：问了 %d 次、付款 %d 次、err=%v", asks, paid.Load(), err)
			}
		})
	}
}

// scSpyModel：每次被调时记下 ctx 上的软取消信号；第一轮调 noop 工具，之后答 done。
type scSpyModel struct {
	mu   sync.Mutex
	seen []*compose_v2.SoftCancelSignal
}

func (m *scSpyModel) Generate(ctx context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.seen = append(m.seen, compose_v2.GetSoftCancel(ctx))
	m.mu.Unlock()
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "n1", Type: "function",
		Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}}}), nil
}
func (m *scSpyModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *scSpyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// pushSCHooks：在 at 指定的那个钩子里，往交给它的 ctx 上派生一层软取消再交回去。
type pushSCHooks struct {
	hook.NoOpHooks
	at  string
	sig *compose_v2.SoftCancelSignal
}

func (h *pushSCHooks) push(ctx context.Context, at string) context.Context {
	if h.at == at {
		return compose_v2.WithSoftCancelCtx(ctx, h.sig)
	}
	return ctx
}
func (h *pushSCHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	return h.push(ctx, "OnModelStart"), nil
}
func (h *pushSCHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	return h.push(ctx, "OnToolStart"), nil
}

// pushSCOrch：编排钩子版本。
type pushSCOrch struct {
	agent_v2.NoOpOrchestrationHooks
	at  string
	sig *compose_v2.SoftCancelSignal
}

func (h *pushSCOrch) OnGraphStart(ctx context.Context) (context.Context, error) {
	if h.at == "OnGraphStart" {
		return compose_v2.WithSoftCancelCtx(ctx, h.sig), nil
	}
	return ctx, nil
}
func (h *pushSCOrch) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if h.at == "OnNodeStart" && (nodeID == "model" || nodeID == "tools") {
		return compose_v2.WithSoftCancelCtx(ctx, h.sig), nil
	}
	return ctx, nil
}

// TestHookCtxOrigin_DerivedPushVisibleDownstream：每个采纳点上，钩子（或中间件）在派生的 ctx 上推的可配项
// 下游都看得到。标记要放在交出 ctx 之前——放在之后时次数照样对得上，派生的 ctx 却被当成外来的、推的东西
// 静默不生效（审计实测：挪一行，全套测试照绿）。这里逐个采纳点钉住「派生的就听钩子的」。
func TestHookCtxOrigin_DerivedPushVisibleDownstream(t *testing.T) {
	for _, at := range []string{"OnModelStart", "OnToolStart", "OnNodeStart", "OnGraphStart", "中间件"} {
		t.Run(at, func(t *testing.T) {
			sig := compose_v2.NewSoftCancelSignal()
			var toolSaw *compose_v2.SoftCancelSignal
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					toolSaw = compose_v2.GetSoftCancel(ctx)
					return &tool.Result{Success: true, Output: "ok"}, nil
				}))
			m := &scSpyModel{}
			opts := []agent_v2.Option{agent_v2.WithName("bot"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(4), agent_v2.WithHooks(&pushSCHooks{at: at, sig: sig})}
			if at == "中间件" {
				opts = append(opts, agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
					return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
						return next(compose_v2.WithSoftCancelCtx(ctx, sig), name, args)
					}
				}))
			}
			var runOpts []agent_v2.RunOption
			if at == "OnNodeStart" || at == "OnGraphStart" {
				runOpts = append(runOpts, agent_v2.WithOrchHooks(&pushSCOrch{at: at, sig: sig}))
			}
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"), runOpts...); err != nil {
				t.Fatal(err)
			}
			if at == "OnModelStart" {
				if len(m.seen) == 0 || m.seen[0] != sig {
					t.Errorf("模型应看到钩子推的软取消")
				}
				return
			}
			if toolSaw != sig {
				t.Errorf("工具体应看到 %s 推的软取消", at)
			}
		})
	}
}
