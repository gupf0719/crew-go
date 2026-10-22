package compose_v2_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// counterNode 返回一个只计数的节点。
func counterNode(c *atomic.Int32) compose_v2.NodeFunc {
	return func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		c.Add(1)
		return nil, nil
	}
}

// mustBreakpoint 断言 err 是断点停下的中断，返回它的那一问。
func mustBreakpoint(t *testing.T, err error) *compose_v2.AskQuery {
	t.Helper()
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("该在断点处停下，实际 %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 1 || qs[0].Breakpoint == nil || qs[0].Reason != compose_v2.BreakpointInterruptReason {
		t.Fatalf("该是一条断点挂起，实际 %+v", qs)
	}
	if qs[0].InterruptID == "" {
		t.Fatal("断点挂起要有 InterruptID")
	}
	return qs[0]
}

// before 断点停下时这个超步一个节点都不执行（含不命中的兄弟）；入口那一批也判；续跑就是放行。
func TestBreakpoint_BeforeStopsWholeSuperstep(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_before")
	b.AddNode("a", counterNode(&aRuns))
	b.AddNode("b", counterNode(&bRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge(compose_v2.StartNode, "b")
	b.AddEdge("a", compose_v2.EndNode)
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_before_sid"
	run := func() error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper),
			compose_v2.WithSessionID(sid), compose_v2.WithInterruptBefore("a"))
	}
	q := mustBreakpoint(t, run())
	if !slices.Equal(q.Breakpoint.Before, []compose_v2.NodeID{"a"}) || len(q.Breakpoint.After) != 0 {
		t.Errorf("Breakpoint = %+v，want Before=[a]", q.Breakpoint)
	}
	if aRuns.Load() != 0 || bRuns.Load() != 0 {
		t.Fatalf("停在超步之前，a/b 都不该执行，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
	cp, _ := cper.LoadNS(ctx, sid, "")
	if cp == nil || cp.Status != compose_v2.CPStatusPending || len(cp.CurrentFrontier) != 2 {
		t.Fatalf("存档该是挂起档、frontier 是这个边界的两个成员，实际 %+v", cp)
	}
	if len(cp.PendingInterrupts) != 1 || cp.PendingInterrupts[0].Breakpoint == nil {
		t.Fatalf("挂起清单里该有这条断点，实际 %+v", cp.PendingInterrupts)
	}
	if err := run(); err != nil {
		t.Fatalf("续跑就是放行，该跑完，实际 %v", err)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Errorf("续跑之后 a/b 各跑一次，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// after 断点停在那个超步整体提交之后、下一个超步之前；续跑时停之前已执行的不重跑。
func TestBreakpoint_AfterStopsAtNextBoundary(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_after").WithInterruptAfter("a")
	b.AddNode("a", counterNode(&aRuns))
	b.AddNode("b", counterNode(&bRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_after_sid"
	run := func() error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	}
	q := mustBreakpoint(t, run())
	if !slices.Equal(q.Breakpoint.After, []compose_v2.NodeID{"a"}) || len(q.Breakpoint.Before) != 0 {
		t.Errorf("Breakpoint = %+v，want After=[a]", q.Breakpoint)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 0 {
		t.Fatalf("停在 a 之后、b 之前，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
	if err := run(); err != nil {
		t.Fatalf("续跑该跑完，实际 %v", err)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Errorf("续跑不重跑 a、跑一次 b，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// 逐步执行：每个边界停一次，一次续跑恰好越过一个边界；循环回到同一个节点时再停，每次停的 ID 都不同。
func TestBreakpoint_StepThroughLoopReArms(t *testing.T) {
	step := compose_v2.Sum("__bp_loop_step__")
	var aRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_loop").WithInterruptBefore(compose_v2.AllNodes)
	b = compose_v2.WithChannel(b, step)
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		aRuns.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(step, 1)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddConditionalEdge("a", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if n, _ := compose_v2.Get(chs, step); n >= 3 {
			return compose_v2.EndNode, nil
		}
		return "a", nil
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_loop_sid"
	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
		q := mustBreakpoint(t, err)
		if got := aRuns.Load(); got != int32(i) {
			t.Fatalf("第 %d 次停下时 a 该执行过 %d 次，实际 %d——一次续跑该恰好越过一个边界", i+1, i, got)
		}
		if ids[q.InterruptID] {
			t.Fatalf("第 %d 次停下的 ID %s 与之前的重复", i+1, q.InterruptID)
		}
		ids[q.InterruptID] = true
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)); err != nil {
		t.Fatalf("第三次执行之后图结束，该跑完，实际 %v", err)
	}
	if got := aRuns.Load(); got != 3 {
		t.Errorf("a 总共该执行 3 次，实际 %d", got)
	}
}

// 同一个边界上软取消与断点都要停时，断点优先：停下的那一次就是断点挂起。软取消先停的话，续跑恢复出这个还没
// 开始执行的边界时会重判 before（不会越过），但先多停了一次、那一次停下的不是断点挂起；after 断点则会被越过
// （恢复出来的边界不判 after）。
//
// sanity-revert：把推进之后的断点判定挪到循环顶的软取消检查之后 ⟹ 首跑返回 ErrSoftCanceled 而不是断点挂起，
// 本用例红。
func TestBreakpoint_WinsOverSoftCancelAtSameBoundary(t *testing.T) {
	var bRuns atomic.Int32
	sc := compose_v2.NewSoftCancelSignal()
	b := compose_v2.NewBuilder().WithName("bp_softcancel").WithInterruptBefore("b")
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sc.Trigger("用户点了停")
		return nil, nil
	})
	b.AddNode("b", counterNode(&bRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_sc_sid"
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid), compose_v2.WithSoftCancel(sc))
	mustBreakpoint(t, err)
	if bRuns.Load() != 0 {
		t.Fatal("停在 b 之前，b 不该执行")
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)); err != nil {
		t.Fatalf("续跑该跑完，实际 %v", err)
	}
	if bRuns.Load() != 1 {
		t.Errorf("续跑之后 b 执行一次，实际 %d", bRuns.Load())
	}
}

// recordingCheckpointer 记下每一次写进存储的档。
type recordingCheckpointer struct {
	*compose_v2.InMemoryCheckpointer
	mu    sync.Mutex
	saved []*compose_v2.Checkpoint
}

func (r *recordingCheckpointer) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	r.mu.Lock()
	c := *cp
	r.saved = append(r.saved, &c)
	r.mu.Unlock()
	return r.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

// sync 档每个超步边界都写档：断点所在的边界上，**不能**有一份「frontier 是这个边界、却没有断点挂起」的档落盘
// ——进程在两次写之间崩溃的话，从那份档恢复的续跑会越过 after 断点（恢复出来的边界不判 after）。before 断点
// 在恢复时会重判、照样停下（本用例只配了 before，钉的是写档与判断点的先后：判断点在前，就不会落下那样一份档）。
//
// sanity-revert：把推进之后的断点判定挪到 durability 中途写档之后 ⟹ 先落一份 frontier=[b]、没有挂起的档，本用例红。
func TestBreakpoint_NoDurableCheckpointPastBreakpoint(t *testing.T) {
	var bRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_durability").WithInterruptBefore("b")
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddNode("b", counterNode(&bRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := &recordingCheckpointer{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("bp_dur_sid"), compose_v2.WithDurability(compose_v2.DurabilitySync))
	mustBreakpoint(t, err)
	cper.mu.Lock()
	defer cper.mu.Unlock()
	for i, cp := range cper.saved {
		atB := false
		for _, m := range cp.CurrentFrontier {
			if m.NodeID == "b" {
				atB = true
			}
		}
		if !atB {
			continue
		}
		hasBP := false
		for _, q := range cp.PendingInterrupts {
			if q != nil && q.Breakpoint != nil {
				hasBP = true
			}
		}
		if !hasBP {
			t.Errorf("第 %d 份落盘的档 frontier 停在 b、却没有断点挂起——断点判在写档之后了（after 断点从这样的档恢复会被越过）", i+1)
		}
	}
}

// 没配 checkpointer 时用 Snapshot + WithResumeFrom 续跑（godoc 里写的那条路）。
func TestBreakpoint_ResumeFromSnapshotWithoutCheckpointer(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_snapshot")
	b.AddNode("a", counterNode(&aRuns))
	b.AddNode("b", counterNode(&bRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cm := compose_v2.NewChannelManager()
	mustBreakpoint(t, compose_v2.Run(context.Background(), g, cm, compose_v2.WithInterruptBefore("b")))
	snap, err := cm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), compose_v2.WithResumeFrom(snap),
		compose_v2.WithInterruptBefore("b")); err != nil {
		t.Fatalf("从快照续跑该放行，实际 %v", err)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Errorf("a/b 各执行一次，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// 同超步兄弟中断时 after 断点推迟：这个超步没有整体提交，续跑把它补完的那一刻才停，只停一次。
func TestBreakpoint_AfterDeferredBySiblingInterrupt(t *testing.T) {
	var bRuns, cRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_after_sibling").WithInterruptAfter("b")
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗？"})
		return nil, err
	})
	b.AddNode("b", counterNode(&bRuns))
	b.AddNode("c", counterNode(&cRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge(compose_v2.StartNode, "b")
	b.AddEdge("a", "c")
	b.AddEdge("b", "c")
	b.AddEdge("c", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_after_sibling_sid"
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.AllQueries()[0].Breakpoint != nil {
		t.Fatalf("第一次停下该是 a 的提问，实际 %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers))
	q := mustBreakpoint(t, err)
	if !slices.Equal(q.Breakpoint.After, []compose_v2.NodeID{"b"}) {
		t.Errorf("Breakpoint = %+v，want After=[b]", q.Breakpoint)
	}
	if bRuns.Load() != 1 || cRuns.Load() != 0 {
		t.Fatalf("b 只跑一次（部分失败时已成功）、c 还没跑，实际 b=%d c=%d", bRuns.Load(), cRuns.Load())
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers)); err != nil {
		t.Fatalf("再续跑该跑完，实际 %v", err)
	}
	if cRuns.Load() != 1 {
		t.Errorf("c 执行一次，实际 %d", cRuns.Load())
	}
}

// 图在那个超步之后结束：after 断点不停，Run 正常收尾。
func TestBreakpoint_AfterLastNodeDoesNotStop(t *testing.T) {
	var aRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_after_last").WithInterruptAfter("a")
	b.AddNode("a", counterNode(&aRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("图结束了，不该停，实际 %v", err)
	}
}

// 节点名写错：编译期断点 Compile 报错；运行期断点 Run 报错，不重试、不清槽里别人的档。
func TestBreakpoint_InvalidNodeName(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("bp_invalid").WithInterruptBefore("nope")
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", compose_v2.EndNode)
	if _, err := b.Compile(); err == nil {
		t.Fatal("编译期断点里不存在的节点该让 Compile 报错")
	}

	// 先留一份挂起档，再用写错的运行期断点跑：报错、不重试、档还在
	b2 := compose_v2.NewBuilder().WithName("bp_invalid_run")
	b2.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "?"})
		return nil, err
	})
	b2.AddEdge(compose_v2.StartNode, "a")
	b2.AddEdge("a", compose_v2.EndNode)
	g, err := b2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_invalid_sid"
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))); !ok {
		t.Fatal("先留一份挂起档")
	}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithInterruptAfter("typo"))
	if err == nil {
		t.Fatal("运行期断点里不存在的节点该让 Run 报错")
	}
	if compose_v2.IsRetryable(err) {
		t.Error("节点名写错重试必然一样，不该可重试")
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Error("不该带 ErrRunAborted（那会清掉槽里与这次无关的档）")
	}
	if cp, _ := cper.LoadNS(ctx, sid, ""); cp == nil || !cp.HasResumableWork() {
		t.Errorf("槽里先前的挂起档不该被动，实际 %+v", cp)
	}
}

// 子图 builder 上声明的断点随子图生效：在父图里跑时照样停，NS 是子图的；续跑放行。
func TestBreakpoint_SubgraphCompiledBreakpointFiresInParent(t *testing.T) {
	var xRuns, yRuns atomic.Int32
	cb := compose_v2.NewBuilder().WithName("bp_child").WithInterruptBefore("y")
	cb.AddNode("x", counterNode(&xRuns))
	cb.AddNode("y", counterNode(&yRuns))
	cb.AddEdge(compose_v2.StartNode, "x")
	cb.AddEdge("x", "y")
	cb.AddEdge("y", compose_v2.EndNode)
	child, err := cb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	pb := compose_v2.NewBuilder().WithName("bp_parent")
	pb.AddSubgraphNode("sub", child)
	pb.AddEdge(compose_v2.StartNode, "sub")
	pb.AddEdge("sub", compose_v2.EndNode)
	parent, err := pb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_sub_sid"
	run := func() error {
		return compose_v2.Run(ctx, parent, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	}
	q := mustBreakpoint(t, run())
	if q.Breakpoint.NS == "" || !slices.Equal(q.Breakpoint.Before, []compose_v2.NodeID{"y"}) {
		t.Errorf("Breakpoint = %+v，want 子图的 ns + Before=[y]", q.Breakpoint)
	}
	if xRuns.Load() != 1 || yRuns.Load() != 0 {
		t.Fatalf("停在子图的 y 之前，实际 x=%d y=%d", xRuns.Load(), yRuns.Load())
	}
	if err := run(); err != nil {
		t.Fatalf("续跑该放行，实际 %v", err)
	}
	if xRuns.Load() != 1 || yRuns.Load() != 1 {
		t.Errorf("x 不重跑、y 跑一次，实际 x=%d y=%d", xRuns.Load(), yRuns.Load())
	}
}

// 调用方拿到的挂起清单是副本：改它碰不到框架的记录。
func TestBreakpoint_PendingInterruptsCopyIsDeep(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("bp_copy").WithInterruptBefore("a")
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cm := compose_v2.NewChannelManager()
	mustBreakpoint(t, compose_v2.Run(context.Background(), g, cm))
	got := cm.PendingInterrupts()
	got[0].Breakpoint.Before[0] = "tampered"
	if again := cm.PendingInterrupts(); again[0].Breakpoint.Before[0] != "a" {
		t.Errorf("改副本污染了框架的记录：%v", again[0].Breakpoint.Before)
	}
}

// 嵌在别的节点里、又没有 checkpointer 的图停在断点上没法续：续跑会从头再跑那张图、停在同一个断点（审计
// 实测活锁，节点副作用每续一次重放一次）。所以这时报错而不是停。
func TestBreakpoint_NestedWithoutCheckpointerErrors(t *testing.T) {
	var xRuns, yRuns atomic.Int32
	cb := compose_v2.NewBuilder().WithName("bp_nested_nocp_child").WithInterruptBefore("y")
	cb.AddNode("x", counterNode(&xRuns))
	cb.AddNode("y", counterNode(&yRuns))
	cb.AddEdge(compose_v2.StartNode, "x")
	cb.AddEdge("x", "y")
	cb.AddEdge("y", compose_v2.EndNode)
	child, err := cb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	pb := compose_v2.NewBuilder().WithName("bp_nested_nocp_parent")
	pb.AddSubgraphNode("sub", child)
	pb.AddEdge(compose_v2.StartNode, "sub")
	pb.AddEdge("sub", compose_v2.EndNode)
	parent, err := pb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	err = compose_v2.Run(context.Background(), parent, compose_v2.NewChannelManager())
	if err == nil {
		t.Fatal("没有 checkpointer 的子图停在断点上该报错")
	}
	if _, ok := compose_v2.IsInterrupt(err); ok {
		t.Fatalf("不该返回一个续不了的断点挂起，实际 %v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Error("配置问题重试必然一样，不该可重试")
	}
	if xRuns.Load() != 1 || yRuns.Load() != 0 {
		t.Errorf("x 跑一次、y 不跑，实际 x=%d y=%d", xRuns.Load(), yRuns.Load())
	}
}

// 续跑恢复出一个还没开始执行的边界（软取消停在那儿）：这次 Run 新加的 before 断点照常判（审计实测：
// 曾经恢复出来的边界一律不判，b 没停就执行了）。
func TestBreakpoint_RuntimeBreakpointOnUnstartedRestoredBoundary(t *testing.T) {
	var bRuns atomic.Int32
	sc := compose_v2.NewSoftCancelSignal()
	b := compose_v2.NewBuilder().WithName("bp_restored_unstarted")
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sc.Trigger("先停一下")
		return nil, nil
	})
	b.AddNode("b", counterNode(&bRuns))
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_restored_unstarted_sid"
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid), compose_v2.WithSoftCancel(sc)); !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("首跑该软取消停在 b 之前的边界，实际 %v", err)
	}
	run := func() error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper),
			compose_v2.WithSessionID(sid), compose_v2.WithInterruptBefore("b"))
	}
	mustBreakpoint(t, run())
	if bRuns.Load() != 0 {
		t.Fatal("这次 Run 声明了 b 之前停，b 不该执行")
	}
	if err := run(); err != nil {
		t.Fatalf("再续跑放行，实际 %v", err)
	}
	if bRuns.Load() != 1 {
		t.Errorf("b 执行一次，实际 %d", bRuns.Load())
	}
}

// 已经开始执行过的超步（成员中断了）续跑时不再判它的 before 断点——否则答完问题续跑又停在同一个超步
// 前面，再续跑才执行。
//
// sanity-revert：Run 里不再 markFrontierStarted ⟹ 中断档的 FrontierStarted 为假 ⟹ 带着答案续跑时又停在
// b 前面，本用例红。
func TestBreakpoint_StartedRestoredBoundaryNotRejudged(t *testing.T) {
	var bRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("bp_restored_started")
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"}); err != nil {
			return nil, err
		}
		bRuns.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "bp_restored_started_sid"
	run := func(answers map[string]*schema.Message) error {
		opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
			compose_v2.WithInterruptBefore("b")}
		if answers != nil {
			opts = append(opts, compose_v2.WithResumeAnswers(answers))
		}
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	}
	mustBreakpoint(t, run(nil)) // 停在 b 之前
	err = run(nil)              // 放行：b 开始执行、提问
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.AllQueries()[0].Breakpoint != nil {
		t.Fatalf("放行之后该停在 b 的提问上，实际 %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	if err := run(answers); err != nil {
		t.Fatalf("带着答案续跑该跑完（b 已经开始执行过，before 断点不再判），实际 %v", err)
	}
	if bRuns.Load() != 1 {
		t.Errorf("b 执行完一次，实际 %d", bRuns.Load())
	}
}
