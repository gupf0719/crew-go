package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// Phase 1.1 测试：OrchestrationHooks 5 回调集成 + 并发安全 + 配对承诺。
//
// 撞 audit F8 [MED] 完整版：channel-based 模型下 hook 集成路径——之前 PoC
// 故意不接，本组测试验证回调时序、错误传播、并发安全、配对不变量。

// recordingHooks 测试 helper——记录所有 hook 调用顺序 + 内容。
type recordingHooks struct {
	mu sync.Mutex
	ev []string

	failOnGraphStart  error
	failOnNodeStart   error
	failOnNodeStartID compose_v2.NodeID

	panicOnGraphEnd bool
	panicOnNodeEnd  bool
}

func (h *recordingHooks) record(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ev = append(h.ev, s)
}

func (h *recordingHooks) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.ev))
	copy(out, h.ev)
	return out
}

func (h *recordingHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	h.record("graph_start")
	return ctx, h.failOnGraphStart
}

func (h *recordingHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	if err != nil {
		h.record("graph_end:err=" + err.Error())
	} else {
		h.record("graph_end:ok")
	}
	if h.panicOnGraphEnd {
		panic("test panic in OnGraphEnd")
	}
	return ctx
}

func (h *recordingHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.record("node_start:" + string(id))
	if h.failOnNodeStartID != "" && id == h.failOnNodeStartID {
		return ctx, h.failOnNodeStart
	}
	return ctx, nil
}

func (h *recordingHooks) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	if err != nil {
		h.record("node_end:" + string(id) + ":err")
	} else {
		h.record("node_end:" + string(id) + ":ok")
	}
	if h.panicOnNodeEnd {
		panic("test panic in OnNodeEnd")
	}
	return ctx
}

func (h *recordingHooks) OnNodeEvent(ctx context.Context, id compose_v2.NodeID, ev compose_v2.Event) context.Context {
	h.record("node_event:" + string(id) + ":" + ev.Type + ":" + ev.Content)
	return ctx
}

// ── 基础时序验证 ──

func TestHooks_Lifecycle_BasicSequence(t *testing.T) {
	hooks := &recordingHooks{}
	g, err := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	expected := []string{
		"graph_start",
		"node_start:a",
		"node_end:a:ok",
		"graph_end:ok",
	}
	got := hooks.events()
	if !sliceEqual(got, expected) {
		t.Fatalf("hook 时序错\nexpected: %v\ngot:      %v", expected, got)
	}
}

// ── OnGraphStart 失败 + 配对承诺 ──

func TestHooks_OnGraphStartFails_StillCallsEnd(t *testing.T) {
	hooks := &recordingHooks{failOnGraphStart: errors.New("setup failed")}
	g, err := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			t.Fatal("不应执行节点（OnGraphStart 已失败）")
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks))
	if runErr == nil {
		t.Fatal("OnGraphStart 失败应让 Run 返错")
	}
	if !strings.Contains(runErr.Error(), "setup failed") {
		t.Fatalf("err 应含 OnGraphStart 错信息，实际 %q", runErr.Error())
	}

	got := hooks.events()
	// **配对承诺**：OnGraphStart 失败也调 OnGraphEnd
	if len(got) != 2 || got[0] != "graph_start" || !strings.HasPrefix(got[1], "graph_end:err") {
		t.Fatalf("OnGraphStart 失败应配对调 OnGraphEnd，实际事件 %v", got)
	}
}

// ── OnNodeStart 失败 + 配对承诺 ──

func TestHooks_OnNodeStartFails_StillCallsNodeEnd_NodeFuncSkipped(t *testing.T) {
	hooks := &recordingHooks{
		failOnNodeStart:   errors.New("node setup failed"),
		failOnNodeStartID: "victim",
	}

	var nodeFuncCalled atomic.Bool
	g, err := compose_v2.NewBuilder().
		AddNode("victim", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			nodeFuncCalled.Store(true)
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "victim").
		AddEdge("victim", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks))
	if runErr == nil {
		t.Fatal("OnNodeStart 失败应让 Run 返错")
	}
	if nodeFuncCalled.Load() {
		t.Fatal("OnNodeStart 失败时 NodeFunc 不应被调")
	}

	got := hooks.events()
	// 配对承诺：OnNodeStart 失败也调 OnNodeEnd（节点级配对）
	hasNodeStart, hasNodeEnd := false, false
	for _, ev := range got {
		if ev == "node_start:victim" {
			hasNodeStart = true
		}
		if strings.HasPrefix(ev, "node_end:victim") {
			hasNodeEnd = true
		}
	}
	if !hasNodeStart || !hasNodeEnd {
		t.Fatalf("OnNodeStart 失败应配对 OnNodeEnd，实际事件 %v", got)
	}
}

// ── 并发安全（fan-out 多 goroutine）──

func TestHooks_FanOut_ConcurrentSafe(t *testing.T) {
	hooks := &recordingHooks{}

	worker := func(name string) compose_v2.NodeFunc {
		return func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}
	}

	g, err := compose_v2.NewBuilder().
		AddNode("d", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddNode("w1", worker("w1")).
		AddNode("w2", worker("w2")).
		AddNode("w3", worker("w3")).
		AddNode("agg", worker("agg")).
		AddEdge(compose_v2.StartNode, "d").
		AddEdge("d", "w1").
		AddEdge("d", "w2").
		AddEdge("d", "w3").
		AddEdge("w1", "agg").
		AddEdge("w2", "agg").
		AddEdge("w3", "agg").
		AddEdge("agg", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 3 worker 各有 1 对 start/end（顺序非确定），不丢不重
	got := hooks.events()
	startCount := map[string]int{}
	endCount := map[string]int{}
	for _, ev := range got {
		switch {
		case strings.HasPrefix(ev, "node_start:"):
			id := strings.TrimPrefix(ev, "node_start:")
			startCount[id]++
		case strings.HasPrefix(ev, "node_end:") && strings.HasSuffix(ev, ":ok"):
			id := strings.TrimSuffix(strings.TrimPrefix(ev, "node_end:"), ":ok")
			endCount[id]++
		}
	}

	for _, id := range []string{"d", "w1", "w2", "w3", "agg"} {
		if startCount[id] != 1 {
			t.Errorf("node %q OnNodeStart 应调 1 次，实际 %d", id, startCount[id])
		}
		if endCount[id] != 1 {
			t.Errorf("node %q OnNodeEnd 应调 1 次，实际 %d", id, endCount[id])
		}
	}
}

// ── EventSink 实时投递 ──

func TestHooks_NodeEventSink_RealtimeDelivery(t *testing.T) {
	hooks := &recordingHooks{}

	g, err := compose_v2.NewBuilder().
		AddNode("streaming", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sink := compose_v2.GetEventSink(ctx)
			if sink == nil {
				t.Fatal("EventSink should be injected when hooks set")
			}
			sink.Send(compose_v2.Event{Type: "chunk", Content: "hello"})
			sink.Send(compose_v2.Event{Type: "chunk", Content: " world"})
			sink.Send(compose_v2.Event{Type: "complete", Content: "done"})
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "streaming").
		AddEdge("streaming", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := hooks.events()
	expected := []string{
		"graph_start",
		"node_start:streaming",
		"node_event:streaming:chunk:hello",
		"node_event:streaming: world", // type=chunk, content=" world"
		"node_event:streaming:complete:done",
		"node_end:streaming:ok",
		"graph_end:ok",
	}
	// 调整 expected[3] 准确格式
	expected[3] = "node_event:streaming:chunk: world"
	if !sliceEqual(got, expected) {
		t.Fatalf("hook 事件序列错\nexpected: %v\ngot:      %v", expected, got)
	}
}

// ── graph_ns 自动注入 ──
//
// 三方对照 (LangGraph subgraphs=True namespace tuple): nodeEventSink.Send 自动注
// `ev.Metadata["graph_ns"]` 让前端 SSE 流可按 ns 分通道展示父子嵌套. 业务自填时不覆盖.
//
// sanity-revert: 删 hooks.go::nodeEventSink.Send 的 graph_ns 注入块, 本测试 fail.

func TestHooks_NodeEventSink_AutoInjectsGraphNS(t *testing.T) {
	rec := &compose_v2.RecordingHooks{}

	g, err := compose_v2.NewBuilder().
		AddNode("streaming", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sink := compose_v2.GetEventSink(ctx)
			if sink == nil {
				t.Fatal("EventSink should be injected")
			}
			// 不预填 Metadata —— framework 应自动注 graph_ns
			sink.Send(compose_v2.Event{Type: "chunk", Content: "auto"})
			// 业务预填 graph_ns —— framework 不应覆盖
			sink.Send(compose_v2.Event{
				Type:     "chunk",
				Content:  "custom",
				Metadata: map[string]any{"graph_ns": "business/override"},
			})
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "streaming").
		AddEdge("streaming", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithHooks(rec),
		compose_v2.WithRunNS("parent/child"),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := rec.SnapshotEvents()
	if len(events) != 2 {
		t.Fatalf("应收 2 events, 实际 %d", len(events))
	}

	// 第 1 个: 业务不填 → framework 自动注
	if got, _ := events[0].Metadata["graph_ns"].(string); got != "parent/child" {
		t.Errorf("event[0] graph_ns: 期望 'parent/child', 实际 %q (full meta: %v)", got, events[0].Metadata)
	}

	// 第 2 个: 业务自填 → framework 不覆盖
	if got, _ := events[1].Metadata["graph_ns"].(string); got != "business/override" {
		t.Errorf("event[1] graph_ns: 业务自填应保留 'business/override', 实际 %q", got)
	}
}

// TestHooks_NodeEventSink_DoesNotMutateBusinessMap 验证 framework 注 graph_ns 时
// 不 mutate 业务持有的 Metadata map——业务复用同 map 实例多次 Send (反模式但合法)
// 时上次注入的 graph_ns 不应残留在业务 map 让"业务自填取胜"判断错误.
//
// audit follow-up MED-2: 之前 Send 内直接 `ev.Metadata["graph_ns"] = ns` 污染业务
// map. fix: 浅拷贝再注 (代价 +1 alloc/Send).
//
// sanity-revert: 删 hooks.go::Send 内浅拷贝块改回直 mutate, 本测试 fail.
func TestHooks_NodeEventSink_DoesNotMutateBusinessMap(t *testing.T) {
	rec := &compose_v2.RecordingHooks{}

	// 业务持 sharedMap 复用——典型场景 LLM 流式 chunk 业务想多 chunk 复用同 metadata
	sharedMap := map[string]any{"business_field": "v1"}

	g, err := compose_v2.NewBuilder().
		AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Metadata: sharedMap})
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithHooks(rec),
		compose_v2.WithRunNS("parent"),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 业务 sharedMap 不应被 framework 注入 graph_ns 污染
	if _, polluted := sharedMap["graph_ns"]; polluted {
		t.Fatalf("framework 不应 mutate 业务 sharedMap, 实际: %v", sharedMap)
	}
	if got := sharedMap["business_field"]; got != "v1" {
		t.Fatalf("业务原字段意外变化: business_field=%v", got)
	}

	// hook 收到的 event.Metadata 应是新 map 含 graph_ns + business_field
	events := rec.SnapshotEvents()
	if len(events) != 1 {
		t.Fatalf("应收 1 event, 实际 %d", len(events))
	}
	if got, _ := events[0].Metadata["graph_ns"].(string); got != "parent" {
		t.Errorf("event Metadata 应含 graph_ns=parent, 实际 %v", events[0].Metadata)
	}
	if got := events[0].Metadata["business_field"]; got != "v1" {
		t.Errorf("event Metadata 应保留业务 business_field=v1, 实际 %v", got)
	}
}

// TestHooks_NodeEventSink_NoGraphNSWhenEmpty 验证 graphNS 空时不注入 graph_ns key——
// 避免污染 Metadata (业务 nil Metadata 检查兼容).
func TestHooks_NodeEventSink_NoGraphNSWhenEmpty(t *testing.T) {
	rec := &compose_v2.RecordingHooks{}

	g, err := compose_v2.NewBuilder().
		AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "x"})
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	// 不传 WithRunNS —— ctx graphNS 为空
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(rec)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := rec.SnapshotEvents()
	if len(events) != 1 {
		t.Fatalf("应收 1 event, 实际 %d", len(events))
	}
	// graphNS 空时 Metadata 可以仍是 nil (业务原 Event.Metadata 未填), 不应被自动创建
	if events[0].Metadata != nil {
		if _, has := events[0].Metadata["graph_ns"]; has {
			t.Errorf("graphNS 空时不应注入 graph_ns, 实际 Metadata: %v", events[0].Metadata)
		}
	}
}

// ── nil-safety ──

func TestHooks_NilHooks_NoCrash(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	// 不传 WithHooks — 等价于 hooks=nil
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run with nil hooks: %v", err)
	}
}

// ── hook panic 收口 ──

// TestHooks_PanicInOnNodeEnd_NotCrashGraph：OnNodeEnd panic 不让 Run 的 goroutine 崩掉。节点体成功时它不改变结果（Run 成功、
// panic 记成一条 ERROR，见 observer_hook_panic_test.go）；节点体失败时照旧转成这个节点的错误，错误里带着 panic。
func TestHooks_PanicInOnNodeEnd_NotCrashGraph(t *testing.T) {
	for _, nodeFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "节点体成功", true: "节点体失败"}[nodeFails], func(t *testing.T) {
			hooks := &recordingHooks{panicOnNodeEnd: true}
			g, err := compose_v2.NewBuilder().
				AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					if nodeFails {
						return nil, errors.New("业务错")
					}
					return nil, nil
				}).
				AddEdge(compose_v2.StartNode, "a").
				AddEdge("a", compose_v2.EndNode).
				Compile()
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks))
			if !nodeFails {
				if err != nil {
					t.Fatalf("节点体成功时 OnNodeEnd panic 不该让 Run 失败，got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "panic") {
				t.Fatalf("节点体失败时 Run 应返回带 panic 的错误，got %v", err)
			}
		})
	}
}

// ── HITL InterruptError 时 hook 也按预期触发 ──

func TestHooks_InterruptError_OnNodeEndCalledWithErr(t *testing.T) {
	hooks := &recordingHooks{}

	g, err := compose_v2.NewBuilder().
		AddNode("ask", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_1", Question: "?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "ask").
		AddEdge("ask", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks))
	if _, isInt := compose_v2.IsInterrupt(runErr); !isInt {
		t.Fatalf("应返 InterruptError，实际 %v", runErr)
	}

	got := hooks.events()
	// 节点抛 InterruptError 时 OnNodeEnd 仍被调（监听语义），且带 err
	hasNodeEndWithErr := false
	for _, ev := range got {
		if ev == "node_end:ask:err" {
			hasNodeEndWithErr = true
			break
		}
	}
	if !hasNodeEndWithErr {
		t.Fatalf("InterruptError 时 OnNodeEnd 应配对调用并传 err，实际事件 %v", got)
	}
}

// ── ChainOrchHooks 测试 ──

// TestChainOrchHooks_OrderPreserved —— 多 hook 按注册顺序调用。
func TestChainOrchHooks_OrderPreserved(t *testing.T) {
	var log []string
	var mu sync.Mutex
	mk := func(id string) compose_v2.OrchestrationHooks {
		return &recordingChainHook{id: id, log: &log, mu: &mu}
	}
	chain := compose_v2.NewChainOrchHooks(mk("a"), mk("b"), mk("c"))

	g, err := compose_v2.NewBuilder().
		AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "x"})
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain)); err != nil {
		t.Fatal(err)
	}
	expected := []string{"a:gs", "b:gs", "c:gs", "a:ns", "b:ns", "c:ns",
		"a:ev", "b:ev", "c:ev", "a:ne", "b:ne", "c:ne", "a:ge", "b:ge", "c:ge"}
	if !sliceEqual(log, expected) {
		t.Fatalf("链调用顺序错\nexpect: %v\ngot:    %v", expected, log)
	}
}

// TestChainOrchHooks_NilHookSkipped —— nil hook 自动过滤不 panic。
func TestChainOrchHooks_NilHookSkipped(t *testing.T) {
	var log []string
	var mu sync.Mutex
	chain := compose_v2.NewChainOrchHooks(nil, &recordingChainHook{id: "real", log: &log, mu: &mu}, nil)

	g, _ := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain)); err != nil {
		t.Fatal(err)
	}
	expected := []string{"real:gs", "real:ns", "real:ne", "real:ge"}
	if !sliceEqual(log, expected) {
		t.Fatalf("expect %v, got %v", expected, log)
	}
}

// TestChainOrchHooks_PanicIsolation_NodeStart —— OnNodeStart 返 error，
// 单 hook panic 转 error fail-fast 中断后续 handlers + 跳过节点执行。
func TestChainOrchHooks_PanicIsolation_NodeStart(t *testing.T) {
	var log []string
	var mu sync.Mutex
	h1 := &recordingChainHook{id: "a", log: &log, mu: &mu}
	h2 := &panicChainHook{id: "b", log: &log, mu: &mu, panicOnNodeStart: true}
	h3 := &recordingChainHook{id: "c", log: &log, mu: &mu}
	chain := compose_v2.NewChainOrchHooks(h1, h2, h3)

	nodeRan := false
	g, _ := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			nodeRan = true
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain))
	if runErr == nil {
		t.Fatal("OnNodeStart panic 应转 error 中断 Run")
	}
	if !contains(log, "a:ns") {
		t.Errorf("h1 OnNodeStart 未被调")
	}
	if contains(log, "c:ns") {
		t.Errorf("h2 panic 后 h3 OnNodeStart 不应被调（fail-fast）")
	}
	if nodeRan {
		t.Errorf("OnNodeStart 错应跳过节点执行")
	}
}

// recordingChainHook 是 ChainOrchHooks 测试用的简单 recording hook。
type recordingChainHook struct {
	id  string
	log *[]string
	mu  *sync.Mutex
}

func (r *recordingChainHook) record(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*r.log = append(*r.log, s)
}

func (r *recordingChainHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	r.record(r.id + ":gs")
	return ctx, nil
}
func (r *recordingChainHook) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	r.record(r.id + ":ge")
	return ctx
}
func (r *recordingChainHook) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	r.record(r.id + ":ns")
	return ctx, nil
}
func (r *recordingChainHook) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	r.record(r.id + ":ne")
	return ctx
}
func (r *recordingChainHook) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Event) context.Context {
	r.record(r.id + ":ev")
	return ctx
}

// panicChainHook 在指定方法 panic——验证 ChainOrchHooks 的 panic 隔离。
type panicChainHook struct {
	id                string
	log               *[]string
	mu                *sync.Mutex
	panicOnGraphStart bool
	panicOnGraphEnd   bool
	panicOnNodeStart  bool
	panicOnNodeEnd    bool
	panicOnEvent      bool
}

func (p *panicChainHook) record(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*p.log = append(*p.log, s)
}

func (p *panicChainHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	p.record(p.id + ":gs")
	if p.panicOnGraphStart {
		panic("boom-gs")
	}
	return ctx, nil
}
func (p *panicChainHook) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	p.record(p.id + ":ge")
	if p.panicOnGraphEnd {
		panic("boom-ge")
	}
	return ctx
}
func (p *panicChainHook) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	p.record(p.id + ":ns")
	if p.panicOnNodeStart {
		panic("boom-ns")
	}
	return ctx, nil
}
func (p *panicChainHook) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	p.record(p.id + ":ne")
	if p.panicOnNodeEnd {
		panic("boom-ne")
	}
	return ctx
}
func (p *panicChainHook) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Event) context.Context {
	p.record(p.id + ":ev")
	if p.panicOnEvent {
		panic("boom-ev")
	}
	return ctx
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// ── helper ──

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
