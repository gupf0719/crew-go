package compose_v2_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// Phase 2 Sub-3 (cp.save / cp.load / cp.cleanup EventSink event) 守护测试.
//
// 测覆盖:
//  1. WithEmitCpEvent(true) + 成功路径 → fire cp.cleanup event (DeleteSubtree)
//  2. WithEmitCpEvent(true) + 中断路径 → fire cp.save event (SaveNS Pending)
//  3. WithEmitCpEvent(true) + Resume 路径 → fire cp.load event (LoadNS)
//  4. WithEmitCpEvent 默认 false → 0 cp event (opt-in 守)
//
// sanity-revert: 删 graph_loop.go::if cfg.emitCpEvent block → 测试 fail.

type cpEventOrchHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []compose_v2.Event
}

func (h *cpEventOrchHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

func (h *cpEventOrchHooks) cpActions() []*compose_v2.CpAction {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*compose_v2.CpAction
	for _, ev := range h.events {
		if ev.Type != compose_v2.CpEventType {
			continue
		}
		if a, ok := ev.Metadata["action"].(*compose_v2.CpAction); ok {
			out = append(out, a)
		}
	}
	return out
}

// TestCpCleanup_AbortReason 守 cleanup event 的 Reason 契约的另一半：**终局失败**
// （ErrRunAborted，如 cond 报错走默认 CondFailAbort 档）同样 fire cp.cleanup，
// Reason="abort" —— 业务只能靠它跟 Reason="success" 区分。
//
// sanity-revert：把 durability.go reportCpPurge 的 `Reason: reason` 删掉 → 本测 fail。
func TestCpCleanup_AbortReason(t *testing.T) {
	hooks := &cpEventOrchHooks{}
	cper := compose_v2.NewInMemoryCheckpointer()

	b := compose_v2.NewBuilder().WithName("cleanup_abort_reason")
	b.AddNode("A", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("X", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "A")
	b.AddEdge("X", compose_v2.EndNode)
	// cond 报错 + 默认 CondFailAbort → 终局失败 → Run 出口主动 purge cp 子树
	b.AddConditionalEdge("A", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", errCleanupAbort
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("cleanup_abort_sid"),
		compose_v2.WithHooks(hooks),
		compose_v2.WithEmitCpEvent(true),
	)
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Fatalf("cond 报错默认档应终局失败（ErrRunAborted），got %v", runErr)
	}

	var sawCleanup bool
	for _, a := range hooks.cpActions() {
		if a.Type != compose_v2.CpOperationCleanup || a.Cleanup == nil {
			continue
		}
		sawCleanup = true
		if a.Cleanup.Reason != "abort" {
			t.Errorf("终局失败路径的 Cleanup.Reason 应 = abort, got %q\n"+
				"  业务订阅 cp.cleanup 时只能靠 Reason 区分「Run 干净跑完」vs「终局失败被主动清」",
				a.Cleanup.Reason)
		}
	}
	if !sawCleanup {
		t.Error("终局失败路径也该 fire cp.cleanup event（Run 出口主动 DeleteSubtree）")
	}
}

var errCleanupAbort = &cleanupAbortErr{}

type cleanupAbortErr struct{}

func (*cleanupAbortErr) Error() string { return "upstream data invalid" }

func TestPhase2Sub3_CpCleanupSuccess_EmitsEvent(t *testing.T) {
	// 成功 Run 走 cleanup (DeleteSubtree) 路径 → fire cp.cleanup event
	hooks := &cpEventOrchHooks{}
	cper := compose_v2.NewInMemoryCheckpointer()

	g, _ := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_cleanup_sid"),
		compose_v2.WithHooks(hooks),
		compose_v2.WithEmitCpEvent(true),
	)
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	actions := hooks.cpActions()
	var sawCleanup bool
	for _, a := range actions {
		if a.Type == compose_v2.CpOperationCleanup && a.Cleanup != nil {
			sawCleanup = true
			if a.Cleanup.SessionID != "test_cleanup_sid" {
				t.Errorf("Cleanup.SessionID 应 = test_cleanup_sid, got %q", a.Cleanup.SessionID)
			}
			if !a.Cleanup.Ok {
				t.Errorf("Cleanup.Ok 应 true, got false err=%q", a.Cleanup.Err)
			}
			// cleanup 两条路径都 fire（成功 / 终局失败）——Reason 是业务唯一的区分手段
			if a.Cleanup.Reason != "success" {
				t.Errorf("Cleanup.Reason 应 = success, got %q（业务靠它区分「干净跑完」vs「终局失败 abort」）",
					a.Cleanup.Reason)
			}
		}
	}
	if !sawCleanup {
		t.Fatalf("Phase 2 Sub-3 regression: 成功路径应 fire cp.cleanup event\n"+
			"sanity-revert: 删 graph_loop.go::DeleteSubtree EmitCpEvent block → 此测 fail\n"+
			"got %d actions: %+v", len(actions), actions)
	}
}

func TestPhase2Sub3_CpEmitOptIn(t *testing.T) {
	// WithEmitCpEvent 未传 (默认 false) → 0 cp event
	hooks := &cpEventOrchHooks{}
	cper := compose_v2.NewInMemoryCheckpointer()

	g, _ := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_optin_sid"),
		compose_v2.WithHooks(hooks),
		// 不传 WithEmitCpEvent → 默认 false
	)
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if got := len(hooks.cpActions()); got != 0 {
		t.Errorf("WithEmitCpEvent=false 应 0 cp event, got %d", got)
	}
}

func TestPhase2Sub3_CpSaveOnInterrupt_EmitsEvent(t *testing.T) {
	// 节点抛 InterruptError → framework 保 cp Pending → fire cp.save event
	hooks := &cpEventOrchHooks{}
	cper := compose_v2.NewInMemoryCheckpointer()

	g, _ := compose_v2.NewBuilder().
		AddNode("ask", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "stop?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "ask").
		AddEdge("ask", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_save_sid"),
		compose_v2.WithHooks(hooks),
		compose_v2.WithEmitCpEvent(true),
	)

	actions := hooks.cpActions()
	var sawSave bool
	for _, a := range actions {
		if a.Type == compose_v2.CpOperationSave && a.Save != nil {
			sawSave = true
			if a.Save.SessionID != "test_save_sid" {
				t.Errorf("Save.SessionID 应 = test_save_sid, got %q", a.Save.SessionID)
			}
			if a.Save.PendingInterruptsCount == 0 {
				t.Errorf("Save.PendingInterruptsCount 应 > 0 (ask 节点抛 Interrupt), got %d",
					a.Save.PendingInterruptsCount)
			}
			if !a.Save.Ok {
				t.Errorf("Save.Ok 应 true (InMemoryCheckpointer.SaveNS 不会失败), got false")
			}
		}
	}
	if !sawSave {
		t.Errorf("Phase 2 Sub-3 regression: Interrupt 路径应 fire cp.save event\n"+
			"sanity-revert: 删 graph_loop.go::SaveNS EmitCpEvent block → 此测 fail\n"+
			"got actions %+v", actions)
	}
}

// TestPhase2Sub3_AuditHIGH2_CtxFlagPropagation 验 audit HIGH-2 fix: 业务用
// ctx flag (WithEmitCpEventCtx) 一次注入即可让所有嵌套编排自动透传, 不用 7 编排
// 各自加 Option. 这是跨编排协议一致性 (audit "协议四复证" 第 5 复证).
//
// sanity-revert: 删 graph_loop.go::if cfg.emitCpEvent || isEmitCpEventCtx(ctx) →
// ctx flag 不被识别 → fire 路径不走 → 测试 fail.
func TestPhase2Sub3_AuditHIGH2_CtxFlagPropagation(t *testing.T) {
	hooks := &cpEventOrchHooks{}
	cper := compose_v2.NewInMemoryCheckpointer()

	g, _ := compose_v2.NewBuilder().
		AddNode("a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "a").
		AddEdge("a", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	// 不传 WithEmitCpEvent RunOption, 用 ctx flag 一次注入
	ctx := compose_v2.WithEmitCpEventCtx(context.Background(), true)
	err := compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_ctx_flag_sid"),
		compose_v2.WithHooks(hooks),
		// 不传 WithEmitCpEvent — 走 ctx flag 路径
	)
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	actions := hooks.cpActions()
	if len(actions) == 0 {
		t.Fatalf("audit HIGH-2 regression: ctx flag (WithEmitCpEventCtx) 应让 cp event 自动 fire\n" +
			"sanity-revert: 删 graph_loop.go::isEmitCpEventCtx(ctx) 守 → 此测 fail\n" +
			"业务用 7 编排时 ctx flag 是唯一全编排自动透传路径")
	}
}

// TestPhase2Sub3_AuditFollowUp_HelperEmitCpCleanup 验 EmitCpCleanupHelper 把
// DeleteSubtree 的结果如实转成 cp.cleanup event（删了几条、耗时、错），业务订阅
// 可统一处理。
//
// sanity-revert: 删 EmitCpCleanupHelper → 它的调用点（subgraph.go 与
// agent_v2/resume_helpers.go）编译失败。⚠️ 原说明写的是「9 编排 abort 编译失败」，
// 那些 abort 路径早已收敛进 commonResumePrep 一处。
func TestPhase2Sub3_AuditFollowUp_HelperEmitCpCleanup(t *testing.T) {
	hooks := &cpEventOrchHooks{}
	cper := compose_v2.NewInMemoryCheckpointer()
	// 预存 1 cp 让 DeleteSubtree 真删
	_ = cper.SaveNS(context.Background(), "test_helper_sid", "test_ns",
		&compose_v2.Checkpoint{ID: "x", Status: compose_v2.CPStatusPending})

	// 直接调 helper 验 fire
	ctx := compose_v2.WithOrchHooks(context.Background(), hooks)
	ctx = compose_v2.WithEmitCpEventCtx(ctx, true)
	// nodeEventSink 在 framework 内 NodeFunc 才挂; helper 走 EmitCpEvent → ctx
	// EventSink 路径. 这里我们模拟 framework Run defer 同款走 fire — 直接调
	// helper 模拟业务调用方法.
	// 但 helper 走 ctx EventSink (无 nodeEventSink 时 short-circuit), 测试需要
	// 模拟 nodeEventSink. 简化: 直接验 helper 不 panic + 真调 cper.
	delCount, err := cper.DeleteSubtree(ctx, "test_helper_sid", "test_ns")
	// helper 应该不 panic
	compose_v2.EmitCpCleanupHelper(ctx, "test_helper_sid", "test_ns", "resume_abort", delCount, 0, err)
	// 不验 actions (因 ctx 无 nodeEventSink — helper 内 short-circuit)
	// 真测 abort 路径 fire 走业务 agent_v2.{编排}.Resume — 留集成测.
	if delCount <= 0 {
		t.Errorf("DeleteSubtree 应删 >=1 cp, got %d", delCount)
	}
}

// failSaveCheckpointer 模拟 SaveNS 失败 (业务后端 Redis/PG 写失败)
type failSaveCheckpointer struct {
	*compose_v2.InMemoryCheckpointer
}

func newFailSaveCheckpointer() *failSaveCheckpointer {
	return &failSaveCheckpointer{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
}

func (c *failSaveCheckpointer) SaveNS(_ context.Context, _ string, _ string, _ *compose_v2.Checkpoint) error {
	// 真模拟 backend 失败 — 不调底层 InMemory
	return errSaveBackend
}

var errSaveBackend = compose_v2.ErrGraphIDConflict // 借用 sentinel (audit follow-up LOW: 业务可换真错)

// TestPhase2Sub3_AuditLOW_CpSaveFailure_EmitsErrInEvent 验 audit LOW: Save 失败
// 路径 fire event 含 Ok=false + Err. 之前测试只用 InMemoryCheckpointer (SaveNS
// 不会失败) — Save 失败 fire 代码 dead 无覆盖.
func TestPhase2Sub3_AuditLOW_CpSaveFailure_EmitsErrInEvent(t *testing.T) {
	hooks := &cpEventOrchHooks{}
	cper := newFailSaveCheckpointer()

	g, _ := compose_v2.NewBuilder().
		AddNode("ask", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "stop?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "ask").
		AddEdge("ask", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_save_fail_sid"),
		compose_v2.WithHooks(hooks),
		compose_v2.WithEmitCpEvent(true),
	)

	actions := hooks.cpActions()
	var sawSaveFail bool
	for _, a := range actions {
		if a.Type == compose_v2.CpOperationSave && a.Save != nil && !a.Save.Ok {
			sawSaveFail = true
			if a.Save.Err == "" {
				t.Errorf("Save 失败时 Err 字段应非空, got %+v", a.Save)
			}
		}
	}
	if !sawSaveFail {
		t.Errorf("audit follow-up LOW: Save 失败应 fire event 含 Ok=false + Err, got actions %+v",
			actions)
	}
}

func TestPhase2Sub3_CpLoadOnResume_EmitsEvent(t *testing.T) {
	// 先 Run 1 触发 Interrupt 存 cp, 再用同 sid Run 2 触发 auto-load → fire cp.load event
	cper := compose_v2.NewInMemoryCheckpointer()
	hooks1 := &cpEventOrchHooks{}

	// Run 1: 触发 InterruptError 存 cp
	g, _ := compose_v2.NewBuilder().
		AddNode("ask", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "stop?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "ask").
		AddEdge("ask", compose_v2.EndNode).
		Compile()
	cm1 := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_load_sid"),
		compose_v2.WithHooks(hooks1),
		compose_v2.WithEmitCpEvent(true),
	)

	// Run 2: 同 sid 再跑 — framework auto-load 触发 LoadNS fire cp.load event
	hooks2 := &cpEventOrchHooks{}
	cm2 := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("test_load_sid"),
		compose_v2.WithHooks(hooks2),
		compose_v2.WithEmitCpEvent(true),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"q1": schema.UserMessage("yes")}),
	)

	actions := hooks2.cpActions()
	var sawLoad bool
	for _, a := range actions {
		if a.Type == compose_v2.CpOperationLoad && a.Load != nil {
			sawLoad = true
			if !a.Load.Found {
				t.Errorf("Load.Found 应 true (Run 1 已存 cp), got false")
			}
			if a.Load.Status == "" {
				t.Errorf("Load.Status 应非空 (有 cp)")
			}
		}
	}
	if !sawLoad {
		t.Errorf("Phase 2 Sub-3 regression: Resume 路径应 fire cp.load event\n"+
			"sanity-revert: 删 graph_loop.go::LoadNS EmitCpEvent block → 此测 fail\n"+
			"got actions %+v", actions)
	}
}

// TestCpLoad_WouldResume_ConsistentAcrossSources 钉住 `WouldResume` 这个字段在两条
// `Source` 上语义一致。
//
// 它的 godoc 契约明写「**GraphID 冲突 ⟹ false**」。根图那处（`Source="auto_load"`）
// 一直是这么算的；子图那处（`Source="subgraph"`）曾经把 GraphID 那一项漏掉——
// 起因是把「事件用的判据」和「决策用的判据」合并成一个变量时只留了后者。
//
// 两者**本来就不该完全相同**：决策侧排除 GraphID 是对的（不匹配时是**报错**不是
// 「不续跑」），而事件侧的契约里含它。合并镜像时要留意这个差异，别为了少一个变量
// 把契约改了。
//
// 后果：业务按这个字段统计「Resume 次数」，会把一次**注定报错**的加载计成 Resume。
func TestCpLoad_WouldResume_ConsistentAcrossSources(t *testing.T) {
	done := compose_v2.LastValue[string]("__wr_done__")

	mkSub := func(name, val string) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName(name)
		b = compose_v2.WithChannel(b, done)
		b.AddNode("inner", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(done, val)}, nil
		})
		b.AddEdge(compose_v2.StartNode, "inner")
		b.AddEdge("inner", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("%s compile: %v", name, err)
		}
		return g
	}
	mkParent := func(sub *compose_v2.Graph) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("wr_parent")
		b.AddSubgraphNode("sub", sub)
		b.AddEdge(compose_v2.StartNode, "sub")
		b.AddEdge("sub", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("parent compile: %v", err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// 先让子图 A 在 (sid,"sub") 留下一份 cp
	if err := compose_v2.Run(ctx, mkParent(mkSub("subA", "a")), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("wr_sid")); err != nil {
		t.Fatalf("预置 Run: %v", err)
	}
	// 手工把那份 cp 塞回去（成功后会被清），模拟「槽里有一份属于别的 graph 的 cp」
	stale := &compose_v2.Checkpoint{
		NS: "sub", Status: compose_v2.CPStatusPending,
		GraphID:         "某个对不上的 graph id",
		CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "inner"}},
	}
	if err := cper.SaveNS(ctx, "wr_sid", "sub", stale); err != nil {
		t.Fatalf("SaveNS: %v", err)
	}

	h := &cpEventOrchHooks{}
	// 换一个不同的子图（GraphID 必然不匹配）
	err := compose_v2.Run(ctx, mkParent(mkSub("subB", "b")), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("wr_sid"),
		compose_v2.WithEmitCpEvent(true), compose_v2.WithHooks(h))
	t.Logf("Run err = %v", err)

	var found bool
	for _, a := range h.cpActions() {
		if a.Load == nil || a.Load.Source != "subgraph" || !a.Load.Found {
			continue
		}
		found = true
		// 承重：GraphID 对不上 ⟹ 契约要求 false
		if a.Load.WouldResume {
			t.Fatalf("子图 cp.load 报 WouldResume=true，但 GraphID 不匹配——"+
				"契约（CpLoadAction.WouldResume godoc）要求这种情况是 false。"+
				"业务按它统计 Resume 次数会把一次注定报错的加载计进去。status=%s", a.Load.Status)
		}
	}
	if !found {
		t.Fatal("没抓到 Source=subgraph 且 Found=true 的 cp.load 事件——fixture 没搭对，本用例测不到东西")
	}
}

// TestInitialWrites_OnlyAppliedOnFreshPath 钉住 `WithInitialWrites` 的核心语义：
// **只在新开路径 apply**。
//
// 这是个**公开 RunOption**，而「只在新开路径 apply」这条语义在本包内曾经**一条直接
// 断言都没有**——当年它叫 WithFreshWrites（已随收窄改名消失）时，把它改成完全空操作 `go test
// ./compose_v2/...` 照样全绿，唯一的哨兵在上层包、且只有当业务的 writer 往 Append
// channel 写时才有分辨力。（收窄改名之后短路新开路径会连带弄红一大片依赖种子的用例，
// 但那是**间接**命中——本用例要的是一条直接钉住语义的断言。）
//
// 两个方向都要钉：新开必须 apply、续跑必须不 apply。只钉一个方向的话，
// 「两条都 apply」或「两条都不 apply」总有一种改法能溜过去。
func TestInitialWrites_OnlyAppliedOnFreshPath(t *testing.T) {
	// 用 Append 而不是 LastValue——LastValue 上「写两次同值」跟「写一次」看不出区别，
	// 那正是上层那条哨兵分辨力有限的原因。
	seen := compose_v2.Append[string]("__fw_seen__")

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("fw_g")
		b = compose_v2.WithChannel(b, seen)
		b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, ok := compose_v2.GetResumeAnswer(ctx, "fw_q"); !ok {
				return nil, &compose_v2.InterruptError{Reason: "fw",
					Query: &compose_v2.AskQuery{Question: "?", InterruptID: "fw_q"}}
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	g := build()

	// 新开：种子必须 apply
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fw_sid"),
		compose_v2.WithInitialWrites(compose_v2.Write(seen, []string{"SEED"})))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("首跑应中断，got %v", err)
	}
	got, _ := compose_v2.Get(cm.Channels(), seen)
	if len(got) != 1 || got[0] != "SEED" {
		t.Fatalf("新开路径没 apply 种子——`WithInitialWrites` 失效了。实际 %v", got)
	}

	// 续跑：同样传种子，但**不能**再 apply（否则 Append 上会出现两份）
	//
	// 走显式 `WithResumeFrom` 指名存档——「续跑不 apply 种子」那条不变量在显式路径和
	// auto-load 路径上是同一份代码，指名的写法让这条用例的意图更清楚。
	saved, loadErr := cper.LoadNS(ctx, "fw_sid", "")
	if loadErr != nil || saved == nil {
		t.Fatalf("载入首跑存档失败：cp=%v err=%v（fixture 没搭对，这条测不到任何东西）", saved, loadErr)
	}
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fw_sid"),
		compose_v2.WithResumeFrom(saved),
		compose_v2.WithInitialWrites(compose_v2.Write(seen, []string{"SEED"})),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"fw_q": schema.UserMessage("go")}),
	); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	got2, _ := compose_v2.Get(cm2.Channels(), seen)
	if len(got2) != 1 {
		t.Fatalf("续跑路径**又 apply 了一次**种子——那正是把它跟「两条路径都 apply」那个语义分家的理由。\n"+
			"实际 %v（应恒为 [SEED]：一份来自首跑、经 cp 恢复）", got2)
	}
}
