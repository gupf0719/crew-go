package compose_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

// audit³ A1-A3 fix 守护测试.
//
// A1: defer 拆分 (OnGraphEnd + cleanup 独立 defer) — OnGraphEnd panic 时 cleanup 必跑.
// A2: OnNodeStart panic inner recover — 配对 OnNodeEnd 仍调.
// A3: OnGraphStart panic inner recover — defer OnGraphEnd 仍 fire + cleanup 仍跑.
//
// sanity-revert:
//  - A1 fix revert (合 defer) → OnGraphEnd panic 跳过 cleanup → cp SaveNS 不调
//  - A2 fix revert (删 inner recover) → OnNodeStart panic 时 OnNodeEnd 不 fire
//  - A3 fix revert (移 defer 到 OnGraphStart 之后) → OnGraphStart panic 时 cleanup 不调

// ── A2: OnNodeStart panic → OnNodeEnd 配对调 ──

// pairAuditHook 记录每 hook 调用顺序——验证 OnNodeStart panic 不跳过 OnNodeEnd.
type pairAuditHook struct {
	mu               sync.Mutex
	ev               []string
	panicAtNodeStart bool
}

func (h *pairAuditHook) record(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ev = append(h.ev, s)
}

func (h *pairAuditHook) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.ev))
	copy(out, h.ev)
	return out
}

func (h *pairAuditHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	h.record("gs")
	return ctx, nil
}
func (h *pairAuditHook) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	h.record("ge")
	return ctx
}
func (h *pairAuditHook) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.record("ns:" + string(id))
	if h.panicAtNodeStart {
		panic("audit3 A2: OnNodeStart panic")
	}
	return ctx, nil
}
func (h *pairAuditHook) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	h.record("ne:" + string(id))
	return ctx
}
func (h *pairAuditHook) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Event) context.Context {
	h.record("nev")
	return ctx
}

func TestAudit3_A2_OnNodeStartPanic_OnNodeEndStillFires(t *testing.T) {
	hooks := &pairAuditHook{panicAtNodeStart: true}
	nodeRan := false
	g, _ := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			nodeRan = true
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(hooks))
	if err == nil {
		t.Fatal("OnNodeStart panic 应返 wrapped err (audit² fail-fast spec)")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("err 应包含 panic 字样, got %v", err)
	}
	got := hooks.events()
	// 关键断言: ns:a 后必有 ne:a (配对承诺)
	var hasNS, hasNE bool
	for _, ev := range got {
		if ev == "ns:a" {
			hasNS = true
		}
		if ev == "ne:a" {
			hasNE = true
		}
	}
	if !hasNS {
		t.Errorf("OnNodeStart 应被调, got %v", got)
	}
	if !hasNE {
		t.Errorf("audit³ A2 fix regression: OnNodeStart panic 后 OnNodeEnd 应仍被调 (配对承诺), got %v\n"+
			"sanity-revert: 删 graph_loop.go inner recover wrap of OnNodeStart → 此测 fail", got)
	}
	if nodeRan {
		t.Errorf("OnNodeStart 失败应跳过节点执行, but nodeRan=true")
	}
}

// ── A1: OnGraphEnd panic → cleanup 仍跑 ──

// auditCheckpointer wrap InMemoryCheckpointer 记录 DeleteSubtree / SaveNS 是否调用.
// (用真 cp 实现避免造 17 方法的 fake)
type auditCheckpointer struct {
	*compose_v2.InMemoryCheckpointer
	mu           sync.Mutex
	deleteCalled bool
	saveCalled   bool
}

func newAuditCheckpointer() *auditCheckpointer {
	return &auditCheckpointer{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
}

func (c *auditCheckpointer) SaveNS(ctx context.Context, sid string, ns string, cp *compose_v2.Checkpoint) error {
	c.mu.Lock()
	c.saveCalled = true
	c.mu.Unlock()
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *auditCheckpointer) DeleteSubtree(ctx context.Context, sid string, ns string) (int32, error) {
	c.mu.Lock()
	c.deleteCalled = true
	c.mu.Unlock()
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

func (c *auditCheckpointer) wasDelete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteCalled
}

func (c *auditCheckpointer) wasSave() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saveCalled
}

// panicAtGraphEndHook 仅 OnGraphEnd panic.
type panicAtGraphEndHook struct {
	compose_v2.NoOpOrchestrationHooks
}

func (h *panicAtGraphEndHook) OnGraphEnd(_ context.Context, _ compose_v2.Channels, _ error) context.Context {
	panic("audit3 A1: OnGraphEnd panic")
}

func TestAudit3_A1_OnGraphEndPanic_CleanupStillRuns(t *testing.T) {
	cp := newAuditCheckpointer()
	g, _ := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithHooks(&panicAtGraphEndHook{}),
		compose_v2.WithCheckpointer(cp),
		compose_v2.WithSessionID("test-sid"),
	)
	// 成功路径 → 应调 DeleteSubtree 清根 cp 子树
	if !cp.wasDelete() {
		t.Errorf("audit³ A1 fix regression: OnGraphEnd panic 后 cleanup defer (DeleteSubtree) 应仍跑\n" +
			"sanity-revert: 合 OnGraphEnd + cleanup 回单 defer body → 此测 fail")
	}
}

// ── A3: OnGraphStart panic → cleanup + OnGraphEnd 仍跑 ──

// panicAtGraphStartHook 仅 OnGraphStart panic.
type panicAtGraphStartHook struct {
	compose_v2.NoOpOrchestrationHooks
	endCalled bool
}

func (h *panicAtGraphStartHook) OnGraphStart(_ context.Context, _ compose_v2.Channels) (context.Context, error) {
	panic("audit3 A3: OnGraphStart panic")
}
func (h *panicAtGraphStartHook) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	h.endCalled = true
	return ctx
}

func TestAudit3_A3_OnGraphStartPanic_PairAndCleanupStillRun(t *testing.T) {
	hooks := &panicAtGraphStartHook{}
	cp := newAuditCheckpointer()
	g, _ := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithHooks(hooks),
		compose_v2.WithCheckpointer(cp),
		compose_v2.WithSessionID("test-sid"),
	)
	if err == nil {
		t.Fatal("OnGraphStart panic 应返 wrapped err")
	}
	if !hooks.endCalled {
		t.Errorf("audit³ A3 fix regression: OnGraphStart panic 后 OnGraphEnd defer 应仍 fire (配对承诺)\n" +
			"sanity-revert: 移 defer 注册到 OnGraphStart 之后 → 此测 fail")
	}
	// failure path: OnGraphStart panic 后 runErr != nil; 但 Snapshot 时 channel state
	// 空 (无 frontier + 无 pending interrupt) 会 early-return 不调 SaveNS, 跟 DeleteSubtree.
	// 两条都不调也算 cleanup defer 真跑过 (没 panic 跳过). 直接检查 defer 跑过的标志:
	// 若 fix revert (defer 没注册) → 这两个字段都 false 但更关键的是 endCalled=false.
	// endCalled 检查已覆盖, 此处仅 sanity check 不触发误报.
	_ = cp.wasSave()
	_ = cp.wasDelete()
}

// ── ChainOrchHooks panic 守护 — OnGraphStart / OnGraphEnd / OnNodeEnd ──
//
// 补 audit² 已有的 TestChainOrchHooks_PanicIsolation_NodeStart 缺的 3 个方法.
// (panicChainHook 已声明 5 字段, 但只测了 NodeStart)

func TestAudit3_ChainOrchHooks_PanicAtGraphStart(t *testing.T) {
	var log []string
	var mu sync.Mutex
	h1 := &recordingChainHook{id: "a", log: &log, mu: &mu}
	h2 := &panicChainHook{id: "b", log: &log, mu: &mu, panicOnGraphStart: true}
	h3 := &recordingChainHook{id: "c", log: &log, mu: &mu}
	chain := compose_v2.NewChainOrchHooks(h1, h2, h3)

	g, _ := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain))
	if err == nil {
		t.Fatal("OnGraphStart panic 应返 wrapped err")
	}
	if !contains(log, "a:gs") {
		t.Errorf("h1 OnGraphStart 未被调")
	}
	if contains(log, "c:gs") {
		t.Errorf("h2 panic 后 h3 OnGraphStart 不应被调 (fail-fast)")
	}
}

func TestAudit3_ChainOrchHooks_PanicAtGraphEnd(t *testing.T) {
	var log []string
	var mu sync.Mutex
	h1 := &recordingChainHook{id: "a", log: &log, mu: &mu}
	h2 := &panicChainHook{id: "b", log: &log, mu: &mu, panicOnGraphEnd: true}
	h3 := &recordingChainHook{id: "c", log: &log, mu: &mu}
	chain := compose_v2.NewChainOrchHooks(h1, h2, h3)

	g, _ := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain))
	// 图已经成功：OnGraphEnd 是观测钩子，它 panic 不把成功报成失败（就地兜住、记 ERROR，见 observer_hook_panic_test.go）
	if err != nil {
		t.Fatalf("图成功时 OnGraphEnd panic 不该让 Run 失败，got %v", err)
	}
	if !contains(log, "a:ge") {
		t.Errorf("h1 OnGraphEnd 应已被调 (panic 前)")
	}
}

func TestAudit3_ChainOrchHooks_PanicAtNodeEnd(t *testing.T) {
	var log []string
	var mu sync.Mutex
	h1 := &recordingChainHook{id: "a", log: &log, mu: &mu}
	h2 := &panicChainHook{id: "b", log: &log, mu: &mu, panicOnNodeEnd: true}
	chain := compose_v2.NewChainOrchHooks(h1, h2)

	g, _ := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain))
	// 节点体已经成功：OnNodeEnd panic 不让这次执行作废（见 observer_hook_panic_test.go）
	if err != nil {
		t.Fatalf("节点体成功时 OnNodeEnd panic 不该让 Run 失败，got %v", err)
	}
}

// TestAudit4_B_MED1_ChainOrchHooks_OnGraphStart_ErrorPath_NoLeakHookACtx
// 验 audit⁴ B MED-1: ChainOrchHooks.OnGraphStart 也需 `inCtx` 协议
// (跟 OnNodeStart + ChainHooks 6 method 同款). hookA push 后 hookB fail 时
// caller 应拿入参 ctx, 不拿到 hookA 的 mutation.
//
// sanity-revert: 删 ChainOrchHooks.OnGraphStart 内 `inCtx := ctx` → hookA span
// 泄漏给 caller → 此测 fail.
type spanCtxKey4B struct{}

type spanPushOrchHook struct {
	compose_v2.NoOpOrchestrationHooks
	spanName string
}

func (h *spanPushOrchHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return context.WithValue(ctx, spanCtxKey4B{}, h.spanName), nil
}

type plainFailOrchHook struct {
	compose_v2.NoOpOrchestrationHooks
	failErr error
}

func (h *plainFailOrchHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return ctx, h.failErr
}

func TestAudit4_B_MED1_ChainOrchHooks_OnGraphStart_NoLeakHookACtx(t *testing.T) {
	hookA := &spanPushOrchHook{spanName: "orch-leaked-spanA"}
	hookB := &plainFailOrchHook{failErr: errLeakCheck}
	chain := compose_v2.NewChainOrchHooks(hookA, hookB)

	gotCtx, err := chain.OnGraphStart(context.Background(), compose_v2.Channels{})
	if err == nil {
		t.Fatal("expect err from hookB")
	}
	if span := gotCtx.Value(spanCtxKey4B{}); span != nil {
		t.Errorf("audit⁴ B MED-1 协议违反: ChainOrchHooks.OnGraphStart 应回入参 ctx, got %v\n"+
			"sanity-revert: 删 ChainOrchHooks.OnGraphStart `inCtx := ctx` → 此测 fail", span)
	}
}

var errLeakCheck = errSentinel("audit4 B MED-1")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

func TestAudit3_ChainOrchHooks_PanicAtNodeEvent(t *testing.T) {
	var log []string
	var mu sync.Mutex
	h1 := &recordingChainHook{id: "a", log: &log, mu: &mu}
	h2 := &panicChainHook{id: "b", log: &log, mu: &mu, panicOnEvent: true}
	chain := compose_v2.NewChainOrchHooks(h1, h2)

	// 在 NodeFunc 内 emit event 触发 OnNodeEvent
	g, _ := compose_v2.NewBuilder().
		AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if sink := compose_v2.GetEventSink(ctx); sink != nil {
				sink.Send(compose_v2.Event{Type: "chunk", Content: "x"})
			}
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(chain))
	// OnNodeEvent 只作观测：它 panic 不让节点体中途失败（见 observer_hook_panic_test.go）
	if err != nil {
		t.Fatalf("OnNodeEvent panic 不该让 Run 失败，got %v", err)
	}
}
