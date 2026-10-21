package compose_v2_test

// 节点级钩子（OnNodeStart / OnNodeEnd）里直接做出的决定交给这个成员这次执行、随它的提交走（下面四条）；
// 经 EventSink 发事件、在 OnNodeEvent 里做出的不生效——OnNodeEvent 只作观测（最后一条，覆盖各个发事件的
// 位置）。曾经经事件做出的也算数，靠在各段入口重绑 sink 让它跟着发事件的那一段走，一再漏出新的形态。

import (
	"context"
	"errors"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type nodeHookHooks struct {
	compose_v2.NoOpOrchestrationHooks
	node     compose_v2.NodeID
	viaStart bool // true：OnNodeStart 里做；false：OnNodeEnd（节点成功时）里做
	value    string
}

func (h *nodeHookHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if h.viaStart && id == h.node {
		sigEmit(ctx, h.value)
	}
	return ctx, nil
}

func (h *nodeHookHooks) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	if !h.viaStart && id == h.node && err == nil {
		sigEmit(ctx, h.value)
	}
	return ctx
}

// first 成功（OnNodeEnd 里做决定）→ ask 问人 → 续跑。first 已提交、续跑不再执行，它的钩子做出的决定应随它
// 留在存档里、续跑跑完时交出。
func TestNodeHookDecision_SurvivesPause(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("nodehook_pause")
	b.AddNode("first", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "first")
	b.AddEdge("first", "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g := rsCompile(t, b)
	opts := append(rsOpts(), compose_v2.WithHooks(&nodeHookHooks{node: "first", value: "from_first_hook"}))
	_, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	})
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在提问上，实际 %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	second, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
	})
	if err != nil {
		t.Fatalf("续跑：%v", err)
	}
	if got := sigValues(second, sigKindA, "d"); len(got) != 1 || got[0] != "from_first_hook" {
		t.Errorf("first 的 OnNodeEnd 做出的决定应随它的提交留下、续跑跑完时交出，实际 %v", got)
	}
}

// n0 节点体先做 from_n0，n1 的 OnNodeEnd 后做 from_n1_hook——后做的为准。
func TestNodeHookDecision_LaterWins(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("nodehook_order")
	b.AddNode("n0", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "from_n0")
		return nil, nil
	})
	b.AddNode("n1", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "n0")
	b.AddEdge("n0", "n1")
	b.AddEdge("n1", compose_v2.EndNode)
	g := rsCompile(t, b)
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(&nodeHookHooks{node: "n1", value: "from_n1_hook"}))
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "from_n1_hook" {
		t.Errorf("n1 的 OnNodeEnd 后做、应为准，实际 %v", got)
	}
}

// fanoutEndHooks：a、b 两个并发成员的 OnNodeEnd 各做一次决定（值 = 节点名）。
type fanoutEndHooks struct {
	compose_v2.NoOpOrchestrationHooks
}

func (h *fanoutEndHooks) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	if err == nil && (id == "a" || id == "b") {
		sigEmit(ctx, string(id))
	}
	return ctx
}

// 同一超步两个成员的 OnNodeEnd 都对同一个收件人做了决定——同一超步只能有一个成员做决定，按设计错误终局。
func TestNodeHookDecision_FanoutSingleWriter(t *testing.T) {
	noop := func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }
	bd := compose_v2.NewBuilder().WithName("nodehook_fanout")
	bd.AddNode("a", noop)
	bd.AddNode("b", noop)
	for _, n := range []compose_v2.NodeID{"a", "b"} {
		bd.AddEdge(compose_v2.StartNode, n)
		bd.AddEdge(n, compose_v2.EndNode)
	}
	g := rsCompile(t, bd)
	_, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithHooks(&fanoutEndHooks{}))
	})
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("两个并发成员都做了决定，应终局报 SignalConflictError，实际 %v", err)
	}
}

// 节点 n 的 OnNodeStart 做决定、节点体随后失败，调用方吞掉这张图的错（同分类器降级、业务把子图失败转成
// 工具失败结果）。成员失败 ⟹ 它这次执行做出的决定（含它的节点级钩子做出的）作废。
func TestNodeHookDecision_VoidedWithFailedMember(t *testing.T) {
	g := sigOneNodeGraph(t, "nodehook_fail", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, errors.New("节点失败")
	})
	var runErr error
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		runErr = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(&nodeHookHooks{node: "n", viaStart: true, value: "from_failed_member_hook"}))
		return nil // 调用方吞掉图的失败
	})
	if err != nil || runErr == nil {
		t.Fatalf("前提：图应失败、调用方吞掉；runErr=%v err=%v", runErr, err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 0 {
		t.Errorf("失败成员的节点级钩子做出的决定应随它作废，实际交出了 %v", got)
	}
}

// observeHooks：节点体、节点级钩子、条件边、OnGraphEnd 各发一个事件；OnNodeEvent 收到任何事件都试着做决定，
// 记下每次 EmitSignal 的返回值（按事件类型）。
type observeHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu       sync.Mutex
	accepted map[string]bool
}

func sendProbe(ctx context.Context, typ string) {
	if s := compose_v2.GetEventSink(ctx); s != nil {
		s.Send(compose_v2.Event{Type: typ})
	}
}

func (h *observeHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id == "n" {
		sendProbe(ctx, "from_node_start")
	}
	return ctx, nil
}

func (h *observeHooks) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	if id == "n" {
		sendProbe(ctx, "from_node_end")
	}
	return ctx
}

func (h *observeHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	sendProbe(ctx, "from_graph_end")
	return ctx
}

func (h *observeHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	ok := compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: ev.Type})
	h.mu.Lock()
	h.accepted[ev.Type] = h.accepted[ev.Type] || ok
	h.mu.Unlock()
	return ctx
}

// TestOnNodeEvent_IsObservationOnly：OnNodeEvent 只作观测——不管事件从哪儿发出来（节点体、节点体里 CaptureSignals
// 开的一段、节点级钩子、入口与节点的条件边、OnGraphEnd、引擎自己发的存档事件），在里面做出的上行信号都不生效：
// EmitSignal 返回 false、调用方什么都收不到。决定要在发事件的那段代码里直接做（上面几条）。
func TestOnNodeEvent_IsObservationOnly(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("observe_only")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sendProbe(ctx, "from_node_body")
		_, err := compose_v2.CaptureSignals(ctx, func(cc context.Context) error {
			sendProbe(cc, "from_capture_segment")
			return nil
		})
		return nil, err
	})
	b.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		sendProbe(ctx, "from_entry_cond")
		return "n", nil
	})
	b.AddConditionalEdge("n", func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		sendProbe(ctx, "from_cond")
		return compose_v2.EndNode, nil
	})
	g := rsCompile(t, b)
	h := &observeHooks{accepted: map[string]bool{}}
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(rsOpts(),
			compose_v2.WithHooks(h), compose_v2.WithDurability(compose_v2.DurabilitySync), compose_v2.WithEmitCpEvent(true))...)
	})
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	positions := []string{"from_node_body", "from_capture_segment", "from_node_start", "from_node_end",
		"from_entry_cond", "from_cond", "from_graph_end"}
	for _, typ := range positions {
		accepted, seen := h.accepted[typ]
		if !seen {
			t.Errorf("fixture 没搭对：OnNodeEvent 没收到 %s", typ)
			continue
		}
		if accepted {
			t.Errorf("%s 触发的 OnNodeEvent 里 EmitSignal 应返回 false（只作观测），实际收下了", typ)
		}
	}
	if len(h.accepted) <= len(positions) {
		t.Errorf("fixture 没搭对：开了存档事件、配了 sync durability，应还有引擎发的存档事件，实际只收到 %v", h.accepted)
	}
	for typ, accepted := range h.accepted {
		if accepted {
			t.Errorf("%s 触发的 OnNodeEvent 里做出的决定不该生效", typ)
		}
	}
	if sigs.Len() != 0 {
		t.Errorf("经事件做出的决定都不该交给调用方，实际 %d 条", sigs.Len())
	}
}

// storingHooks 在 OnNodeStart 存下节点的 ctx（带着节点这次执行的收信号处），在 OnNodeEvent 返回它。
type storingHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	stored context.Context
}

func (h *storingHooks) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.mu.Lock()
	h.stored = ctx
	h.mu.Unlock()
	return ctx, nil
}

func (h *storingHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stored != nil {
		return h.stored
	}
	return ctx
}

// emittingHooks 在 OnNodeEvent 里试着做决定，记下每次 EmitSignal 的返回值。
type emittingHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu  sync.Mutex
	oks []bool
}

func (h *emittingHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Event) context.Context {
	ok := compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "via_chain"})
	h.mu.Lock()
	h.oks = append(h.oks, ok)
	h.mu.Unlock()
	return ctx
}

// TestOnNodeEvent_ChainedStoredCtxCannotDecide：编排钩子链里前一个钩子在 OnNodeEvent 返回它在 OnNodeStart 存下的
// ctx，后一个钩子在 OnNodeEvent 里做决定——照样不生效。链把前一个返回的交给后一个，曾经原样交下去：带着节点的收
// 信号处，决定交到了调用方（审计实测）。
func TestOnNodeEvent_ChainedStoredCtxCannotDecide(t *testing.T) {
	g := sigOneNodeGraph(t, "chain_stored", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sendProbe(ctx, "body_evt")
		return nil, nil
	})
	emitter := &emittingHooks{}
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(compose_v2.NewChainOrchHooks(&storingHooks{}, emitter)))
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter.mu.Lock()
	defer emitter.mu.Unlock()
	if len(emitter.oks) == 0 {
		t.Fatal("fixture 没搭对：链里后一个钩子没收到事件")
	}
	for _, ok := range emitter.oks {
		if ok {
			t.Error("链里后一个钩子在 OnNodeEvent 里做的决定不该生效")
		}
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 0 {
		t.Errorf("决定不该交到调用方，实际 %v", got)
	}
}

type graphValKey struct{}
type nodeValKey struct{}

// ctxSeenHooks：OnGraphStart / OnNodeStart 各推一个值，各处各发一个事件；OnNodeEvent 记下每个事件带来的 nodeID 与
// ctx 上看得见的两个值。
type ctxSeenHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu   sync.Mutex
	seen map[string][3]any // 事件类型 → {nodeID, 图的值, 节点的值}
}

func (h *ctxSeenHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	sendProbe(ctx, "from_graph_start")
	return context.WithValue(ctx, graphValKey{}, "graph-span"), nil
}

func (h *ctxSeenHooks) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	sendProbe(ctx, "from_node_start")
	return context.WithValue(ctx, nodeValKey{}, "node-span"), nil
}

func (h *ctxSeenHooks) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	sendProbe(ctx, "from_node_end")
	return ctx
}

func (h *ctxSeenHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	sendProbe(ctx, "from_graph_end")
	return ctx
}

func (h *ctxSeenHooks) OnNodeEvent(ctx context.Context, id compose_v2.NodeID, ev compose_v2.Event) context.Context {
	h.mu.Lock()
	h.seen[ev.Type] = [3]any{id, ctx.Value(graphValKey{}), ctx.Value(nodeValKey{})}
	h.mu.Unlock()
	return ctx
}

// TestOnNodeEvent_GraphEventsCarryGraphCtx：节点体发的事件 nodeID 是那个节点、ctx 是节点的（带着 OnGraphStart 与
// OnNodeStart 推的值）；条件边、节点级钩子、OnGraphEnd 里发的是图级事件，nodeID 为空、ctx 是图的（带着 OnGraphStart
// 推的值）；OnGraphStart 钩子自己发的那一刻图的 ctx 还没定下。曾经图级事件一直带着 Run 入口的 ctx，业务按自己在
// OnGraphStart 推的 span 挂事件时挂到了调用方的 span 上（审计实测）。
func TestOnNodeEvent_GraphEventsCarryGraphCtx(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("event_ctx")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sendProbe(ctx, "from_node_body")
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddConditionalEdge("n", func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		sendProbe(ctx, "from_cond")
		return compose_v2.EndNode, nil
	})
	g := rsCompile(t, b)
	h := &ctxSeenHooks{seen: map[string][3]any{}}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), compose_v2.WithHooks(h)); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if got := h.seen["from_node_body"]; got != [3]any{compose_v2.NodeID("n"), "graph-span", "node-span"} {
		t.Errorf("节点体的事件应带着节点 n 与两层推的值，实际 %v", got)
	}
	for _, typ := range []string{"from_cond", "from_node_start", "from_node_end", "from_graph_end"} {
		got, ok := h.seen[typ]
		if !ok {
			t.Errorf("fixture 没搭对：没收到 %s", typ)
			continue
		}
		if got[0] != compose_v2.NodeID("") || got[1] != "graph-span" {
			t.Errorf("%s 是图级事件，应 nodeID 为空、带着 OnGraphStart 推的值，实际 %v", typ, got)
		}
	}
	if got := h.seen["from_graph_start"]; got[1] != nil {
		t.Errorf("OnGraphStart 钩子自己发的那一刻还没有它推的值，实际 %v", got)
	}
}
