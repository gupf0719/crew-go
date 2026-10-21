package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// recordingCp 包 InMemoryCheckpointer：数 SaveNS 调用 + 捕获末次写的 cp（marshal 成
// bytes 避免引用 mutation）+ 可注入 saveErr 测写失败。其余 9 method 走嵌入委托。
type recordingCp struct {
	*compose_v2.InMemoryCheckpointer
	mu         sync.Mutex
	saveCount  int
	lastCpData []byte
	allCpData  [][]byte  // 每次 SaveNS 的 cp marshal（按写入顺序——async ordering 测试用）
	caps       []capInfo // 每次 SaveNS 捕获的 cp 结构字段
	saveErr    error     // 非 nil 时 SaveNS 直接返它（测写失败 abort）
}

// capInfo 是每次 SaveNS 捕获的 cp 关键字段（验每超步写的 cp 结构正确）。
type capInfo struct {
	isPending    bool
	graphID      string
	frontierLen  int
	pendingCount int // len(PendingInterrupts)——验中断 cp 走 defer、每超步 cp 干净
}

func (r *recordingCp) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	r.mu.Lock()
	r.saveCount++
	r.caps = append(r.caps, capInfo{
		isPending:    cp.Status == compose_v2.CPStatusPending,
		graphID:      cp.GraphID,
		frontierLen:  len(cp.CurrentFrontier),
		pendingCount: len(cp.PendingInterrupts),
	})
	if data, err := compose_v2.MarshalCheckpoint(cp); err == nil {
		r.lastCpData = data
		r.allCpData = append(r.allCpData, data)
	}
	r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (r *recordingCp) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saveCount
}

// capsCopy 返回每超步捕获 cp 字段的快照副本（线程安全——async 后台写从 worker goroutine
// 调 SaveNS，必须加锁读）。
func (r *recordingCp) capsCopy() []capInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]capInfo, len(r.caps))
	copy(out, r.caps)
	return out
}

// allCpDataCopy 返回按写入顺序的每份 cp marshal 副本（线程安全）。
func (r *recordingCp) allCpDataCopy() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.allCpData))
	copy(out, r.allCpData)
	return out
}

// loopGraph 造一个跑 maxRounds 个超步的 loop（step 节点自环到 counter>=maxRounds）。
// 返回 graph + 两个 channel 供测试 register/读。
func loopGraph(t *testing.T, maxRounds int) (*compose_v2.Graph, *compose_v2.Channel[int], *compose_v2.Channel[int]) {
	t.Helper()
	counterCh := compose_v2.Sum("dur_counter")
	maxCh := compose_v2.LastValue[int]("dur_max")
	b := compose_v2.NewBuilder().
		AddNode("step", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(counterCh, 1)}, nil
		}).
		AddEdge(compose_v2.StartNode, "step").
		AddConditionalEdge("step", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			count, _ := compose_v2.Get(chs, counterCh)
			if count >= compose_v2.MustGet(chs, maxCh) {
				return compose_v2.EndNode, nil
			}
			return "step", nil
		}).
		WithMaxVisit(50)
	b = compose_v2.WithChannel(b, counterCh)
	b = compose_v2.WithChannel(b, maxCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, counterCh, maxCh
}

func newRecordingCp() *recordingCp {
	return &recordingCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
}

// runLoop 跑一次 loop graph（fresh run），返回 final counter。
func runLoop(t *testing.T, g *compose_v2.Graph, maxCh *compose_v2.Channel[int], maxRounds int, opts ...compose_v2.RunOption) error {
	t.Helper()
	cm := compose_v2.NewChannelManager()
	allOpts := append([]compose_v2.RunOption{compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds))}, opts...)
	return compose_v2.Run(context.Background(), g, cm, allOpts...)
}

// TestDurability_ExitDefault_NoPerStepWrites：默认 exit——成功 Run 全程不 SaveNS（只
// defer DeleteSubtree 清）。验证零行为变更。
func TestDurability_ExitDefault_NoPerStepWrites(t *testing.T) {
	g, _, maxCh := loopGraph(t, 5)
	cp := newRecordingCp()
	err := runLoop(t, g, maxCh, 5,
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_exit"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := cp.count(); n != 0 {
		t.Errorf("exit 档成功 Run 不该 SaveNS，实际 %d 次", n)
	}
}

// TestDurability_Sync_WritesPerSuperstep：sync——每超步边界写 cp。与 exit 对比验证。
func TestDurability_Sync_WritesPerSuperstep(t *testing.T) {
	maxRounds := 5
	g, _, maxCh := loopGraph(t, maxRounds)
	cp := newRecordingCp()
	err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_sync"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 每个非末超步（continue 路径）写一次；末超步 isEnd 不写 → ≈ maxRounds-1。
	if n := cp.count(); n < maxRounds-1 {
		t.Errorf("sync 档应每超步写（≥%d 次），实际 %d 次", maxRounds-1, n)
	}
	t.Logf("sync 档 SaveNS %d 次（maxRounds=%d）", cp.count(), maxRounds)
}

// TestDurability_Sync_CleansOnSuccess：sync 成功 Run 后 cleanupOnSuccess 清掉 cp（同 key
// 覆盖无残留）——成功即清语义保持。
func TestDurability_Sync_CleansOnSuccess(t *testing.T) {
	g, _, maxCh := loopGraph(t, 4)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, 4,
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_clean")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cp.count() == 0 {
		t.Fatal("前置：sync 应写过 cp")
	}
	// 成功后底层 store 应无残留 cp（defer DeleteSubtree 清掉每超步写的那份）。
	exists, err := cp.InMemoryCheckpointer.ExistsNS(context.Background(), "s_clean", "")
	if err != nil {
		t.Fatalf("ExistsNS: %v", err)
	}
	if exists {
		t.Error("sync 成功 Run 后 cp 应被 cleanup 清掉，实际残留")
	}
}

// TestDurability_RunOptionAndCtxPrecedence：resolveDurability 优先级——ctx 继承生效 +
// RunOption 显式覆盖 ctx（经"是否每超步写"间接验证）。
func TestDurability_RunOptionAndCtxPrecedence(t *testing.T) {
	maxRounds := 4
	// (a) 仅 ctx Sync（无 RunOption）→ 应每超步写（继承生效）
	g1, _, maxCh1 := loopGraph(t, maxRounds)
	cp1 := newRecordingCp()
	cm1 := compose_v2.NewChannelManager()
	ctxSync := compose_v2.WithDurabilityCtx(context.Background(), compose_v2.DurabilitySync)
	if err := compose_v2.Run(ctxSync, g1, cm1,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh1, maxRounds)),
		compose_v2.WithCheckpointer(cp1), compose_v2.WithSessionID("s_ctx")); err != nil {
		t.Fatalf("ctx Sync Run: %v", err)
	}
	if cp1.count() == 0 {
		t.Error("ctx 继承 Sync 应每超步写，实际 0 次")
	}

	// (b) RunOption Exit 显式覆盖 ctx Sync → 不写（RunOption 优先）
	g2, _, maxCh2 := loopGraph(t, maxRounds)
	cp2 := newRecordingCp()
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctxSync, g2, cm2, // ctx 仍是 Sync
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh2, maxRounds)),
		compose_v2.WithDurability(compose_v2.DurabilityExit), // RunOption 显式 Exit
		compose_v2.WithCheckpointer(cp2), compose_v2.WithSessionID("s_override")); err != nil {
		t.Fatalf("override Run: %v", err)
	}
	if cp2.count() != 0 {
		t.Errorf("RunOption Exit 应覆盖 ctx Sync（不写），实际 %d 次", cp2.count())
	}
}

// TestDurability_Sync_WriteFailureAborts：sync 写失败 → Run 终止返错（对齐 LangGraph
// put 失败传播，不 swallow）。
func TestDurability_Sync_WriteFailureAborts(t *testing.T) {
	g, _, maxCh := loopGraph(t, 5)
	cp := newRecordingCp()
	cp.saveErr = errors.New("disk full boom")
	err := runLoop(t, g, maxCh, 5,
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_fail"))
	if err == nil {
		t.Fatal("sync 写失败应 abort Run")
	}
	if !errcontains(err, "disk full boom") {
		t.Errorf("err 应含写失败原因，实际 %v", err)
	}
}

// TestDurability_Sync_CrashRecovery：sync 的核心价值——捕获末次每超步 cp（模拟"硬崩时
// 盘上的 cp"，defer 不跑），从它 Resume 应续跑不从头重来。
func TestDurability_Sync_CrashRecovery(t *testing.T) {
	maxRounds := 6
	g, counterCh, maxCh := loopGraph(t, maxRounds)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_crash")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cp.lastCpData == nil {
		t.Fatal("应捕获到末次每超步 cp")
	}
	// 末次每超步 cp = 最后一个非末超步后的状态（counter=maxRounds-1，frontier=[step]）。
	snap, err := compose_v2.UnmarshalCheckpoint(cp.lastCpData)
	if err != nil {
		t.Fatalf("Unmarshal 末次 cp: %v", err)
	}
	capturedCount := compose_v2.MustGet(restoreCM(t, snap, counterCh, maxCh).Channels(), counterCh)
	if capturedCount == 0 || capturedCount >= maxRounds {
		t.Fatalf("末次 cp 应是中途进度（0<counter<%d），实际 %d", maxRounds, capturedCount)
	}

	// 从这份 cp Resume（模拟崩溃重启）——应从 counter=capturedCount 续到 maxRounds，
	// 不从头（否则 counter 会超 maxRounds）。
	g2, counterCh2, maxCh2 := loopGraph(t, maxRounds)
	cm2 := restoreCM(t, snap, counterCh2, maxCh2)
	if err := compose_v2.Run(context.Background(), g2, cm2, compose_v2.WithResumeFrom(snap)); err != nil {
		t.Fatalf("Resume Run: %v", err)
	}
	final := compose_v2.MustGet(cm2.Channels(), counterCh2)
	if final != maxRounds {
		t.Errorf("Resume 应续到 counter=%d（不重跑前 %d 轮），实际 %d", maxRounds, capturedCount, final)
	}
	t.Logf("崩溃恢复：捕获 cp counter=%d → Resume 续到 %d", capturedCount, final)
}

// TestDurability_Sync_CheckpointContent：每超步写的 cp 结构正确——不只数对，内容也对：
// Pending 态 + GraphID 非空（防串台）+ 非空 CurrentFrontier（有 Resume 落点）。
func TestDurability_Sync_CheckpointContent(t *testing.T) {
	g, _, maxCh := loopGraph(t, 5)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, 5,
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_content")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	caps := cp.capsCopy()
	if len(caps) == 0 {
		t.Fatal("应捕获到每超步 cp")
	}
	gid := g.ID()
	for i, c := range caps {
		if !c.isPending {
			t.Errorf("cp[%d] Status 应 Pending（挂起态供 Resume）", i)
		}
		if c.graphID == "" || c.graphID != gid {
			t.Errorf("cp[%d] GraphID 应 = g.ID()=%q（防 cp 串台），实际 %q", i, gid, c.graphID)
		}
		if c.frontierLen == 0 {
			t.Errorf("cp[%d] 应有 CurrentFrontier（崩溃从此 frontier Resume），实际空", i)
		}
	}
}

// TestDurability_Sync_NoCheckpointerNoOp：开 sync 但没配 checkpointer → 写点守卫 no-op
// 跑通不崩（崩溃恢复价值要靠业务配持久 Checkpointer，没配也别炸）。
func TestDurability_Sync_NoCheckpointerNoOp(t *testing.T) {
	g, _, maxCh := loopGraph(t, 4)
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, 4)),
		compose_v2.WithDurability(compose_v2.DurabilitySync)) // 无 checkpointer / sessionID
	if err != nil {
		t.Errorf("无 checkpointer 的 sync 应 no-op 跑通，实际 %v", err)
	}
}

// TestDurability_Sync_SingleSuperstep：单超步 run（唯一超步即 isEnd 成功路径，不写）→ 0
// per-step 写。边界：没有"下一超步"要保护时不该多写一份立刻被 cleanup 清的 cp。
func TestDurability_Sync_SingleSuperstep(t *testing.T) {
	g, _, maxCh := loopGraph(t, 1)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, 1,
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_single")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := cp.count(); n != 0 {
		t.Errorf("单超步 run 无中间超步要保护，应 0 次 per-step 写，实际 %d", n)
	}
}

// TestDurability_Async_WritesPerSuperstep：Async 经后台 worker 每超步写 cp（join 在 Run
// 出口 drain，所以 Run 返回后计数已全）。sanity——写点改回 `== Sync` 则 Async 落不写 → 0。
func TestDurability_Async_WritesPerSuperstep(t *testing.T) {
	maxRounds := 5
	g, _, maxCh := loopGraph(t, maxRounds)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := cp.count(); n < maxRounds-1 {
		t.Errorf("Async 应每超步后台写（≥%d），实际 %d", maxRounds-1, n)
	}
}

// TestDurability_Async_CleansOnSuccess：验 **join-before-cleanup**——成功 Run 出口 join defer
// 先 drain 完所有 pending 后台写，cleanup defer 再 DeleteSubtree，所以成功后无残留 cp。
// （若 join 没在 cleanup 前跑，pending 写会在 DeleteSubtree 之后落盘 → 残留。）
func TestDurability_Async_CleansOnSuccess(t *testing.T) {
	g, _, maxCh := loopGraph(t, 4)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, 4,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_clean")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cp.count() == 0 {
		t.Fatal("前置：async 应写过 cp")
	}
	exists, err := cp.InMemoryCheckpointer.ExistsNS(context.Background(), "s_async_clean", "")
	if err != nil {
		t.Fatalf("ExistsNS: %v", err)
	}
	if exists {
		t.Error("async 成功 Run 后 cp 应被 cleanup 清掉（join 先于 DeleteSubtree），实际残留")
	}
}

// TestDurability_Async_CheckpointContent：async 后台 worker 写的 cp 结构也正确——Pending +
// GraphID==g.ID()（防串台）+ 非空 frontier。async 与 sync 共享 snapshotStepCheckpoint，本测试
// 补回归守卫：防将来只改 async 路径漏设 GraphID/Status（sync CheckpointContent 测不到 async）。
func TestDurability_Async_CheckpointContent(t *testing.T) {
	g, _, maxCh := loopGraph(t, 5)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, 5,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_content")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	caps := cp.capsCopy()
	if len(caps) == 0 {
		t.Fatal("应捕获到 async 后台写的 cp")
	}
	gid := g.ID()
	for i, c := range caps {
		if !c.isPending {
			t.Errorf("async cp[%d] 应 Pending", i)
		}
		if c.graphID != gid {
			t.Errorf("async cp[%d] GraphID 应 = g.ID()=%q（防串台），实际 %q", i, gid, c.graphID)
		}
		if c.frontierLen == 0 {
			t.Errorf("async cp[%d] 应有 CurrentFrontier，实际空", i)
		}
	}
}

// TestDurability_Async_Ordering：单 worker + FIFO chan 保证 cp_N 先于 cp_N+1 落盘。restore
// 每份写入的 cp 读 counter，断言**严格递增**——多 worker / 乱序写会破坏此性质。
func TestDurability_Async_Ordering(t *testing.T) {
	maxRounds := 8
	g, counterCh, maxCh := loopGraph(t, maxRounds)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_order")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	datas := cp.allCpDataCopy()
	if len(datas) < 2 {
		t.Fatalf("应有多份 cp 验顺序，实际 %d", len(datas))
	}
	prev := 0
	for i, data := range datas {
		snap, err := compose_v2.UnmarshalCheckpoint(data)
		if err != nil {
			t.Fatalf("cp[%d] unmarshal: %v", i, err)
		}
		got := compose_v2.MustGet(restoreCM(t, snap, counterCh, maxCh).Channels(), counterCh)
		if got <= prev {
			t.Errorf("cp 落盘顺序错：cp[%d] counter=%d 应 > 前一份 %d（单 worker FIFO 保证递增）", i, got, prev)
		}
		prev = got
	}
	t.Logf("async ordering：%d 份 cp counter 严格递增到 %d", len(datas), prev)
}

// TestDurability_Async_WriteFailureAborts：后台写失败 → firstErr → 经下次 submit 或 Run 出口
// join 浮现 → Run 返错（对齐 LangGraph put 失败终止 run）。不 swallow 后台 I/O 失败。
func TestDurability_Async_WriteFailureAborts(t *testing.T) {
	g, _, maxCh := loopGraph(t, 6)
	cp := newRecordingCp()
	cp.saveErr = errors.New("async disk boom")
	err := runLoop(t, g, maxCh, 6,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_fail"))
	if err == nil {
		t.Fatal("async 后台写失败应让 Run abort")
	}
	if !errcontains(err, "async disk boom") {
		t.Errorf("err 应含后台写失败原因，实际 %v", err)
	}
}

// TestDurability_Async_CrashRecovery：async 崩溃恢复——末次后台写 cp（模拟硬崩盘上态，defer
// 不跑）可 Resume 续跑。同 sync 但写经后台 worker，验后台路径的 cp 也完整可恢复。
func TestDurability_Async_CrashRecovery(t *testing.T) {
	maxRounds := 6
	g, counterCh, maxCh := loopGraph(t, maxRounds)
	cp := newRecordingCp()
	if err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_crash")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cp.lastCpData == nil {
		t.Fatal("应捕获到末次后台写 cp")
	}
	snap, err := compose_v2.UnmarshalCheckpoint(cp.lastCpData)
	if err != nil {
		t.Fatalf("Unmarshal 末次 cp: %v", err)
	}
	capturedCount := compose_v2.MustGet(restoreCM(t, snap, counterCh, maxCh).Channels(), counterCh)
	if capturedCount == 0 || capturedCount >= maxRounds {
		t.Fatalf("末次 cp 应是中途进度（0<counter<%d），实际 %d", maxRounds, capturedCount)
	}
	g2, counterCh2, maxCh2 := loopGraph(t, maxRounds)
	cm2 := restoreCM(t, snap, counterCh2, maxCh2)
	if err := compose_v2.Run(context.Background(), g2, cm2, compose_v2.WithResumeFrom(snap)); err != nil {
		t.Fatalf("Resume Run: %v", err)
	}
	if final := compose_v2.MustGet(cm2.Channels(), counterCh2); final != maxRounds {
		t.Errorf("Resume 应续到 %d，实际 %d", maxRounds, final)
	}
}

// panicCp 的 SaveNS panic——验 async worker per-task recover 转 firstErr 不让进程崩。
type panicCp struct {
	*compose_v2.InMemoryCheckpointer
}

func (c *panicCp) SaveNS(_ context.Context, _, _ string, _ *compose_v2.Checkpoint) error {
	panic("checkpointer SaveNS boom")
}

// TestDurability_Async_WorkerPanicRecovers：业务 checkpointer.SaveNS panic 时 async worker
// recover 转 firstErr → Run 返 error，不 crash 进程（对齐 sync 经 Run 入口 outer recover）。
// 这是 audit FINDING 1 修复——worker 在独立 goroutine，outer recover 够不着，必须自己兜。
func TestDurability_Async_WorkerPanicRecovers(t *testing.T) {
	g, _, maxCh := loopGraph(t, 5)
	cp := &panicCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	err := runLoop(t, g, maxCh, 5,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_panic"))
	if err == nil {
		t.Fatal("worker SaveNS panic 应转 error 返回（不 crash 进程）")
	}
	if !errcontains(err, "panic") {
		t.Errorf("err 应含 panic 信息，实际 %v", err)
	}
}

// failOnNthCp 只让第 failAt 次 SaveNS 失败——制造"只有最后一个后台写失败"场景。
type failOnNthCp struct {
	*compose_v2.InMemoryCheckpointer
	mu     sync.Mutex
	count  int
	failAt int
}

func (c *failOnNthCp) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	c.mu.Lock()
	c.count++
	fail := c.count == c.failAt
	c.mu.Unlock()
	if fail {
		return errors.New("last write boom")
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

// TestDurability_Async_LastWriteFailureSurfacesViaJoin：只让最后一个后台写失败——此时无后续
// submit 捕获 firstErr，只能靠 Run 出口 closeAndJoin 浮现（错误传播 path b）。验"最后一次写
// 失败也不被吞"（audit Agent 指出的覆盖缺口）。maxRounds=6 → 5 次 per-superstep 写，failAt=5。
func TestDurability_Async_LastWriteFailureSurfacesViaJoin(t *testing.T) {
	maxRounds := 6
	g, _, maxCh := loopGraph(t, maxRounds)
	cp := &failOnNthCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), failAt: maxRounds - 1}
	err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_lastfail"))
	if err == nil {
		t.Fatal("最后一个后台写失败应经 join 浮现让 Run abort（不被吞）")
	}
	if !errcontains(err, "last write boom") {
		t.Errorf("err 应含最后写失败原因，实际 %v", err)
	}
	if !errcontains(err, "async durability writer") {
		t.Errorf("err 应经 join defer wrap（path b 经 closeAndJoin 浮现），实际 %v", err)
	}
}

// TestDurability_Async_InterruptResume：async + HITL interrupt——中断超步在写点前 return（中断
// cp 走 defer 不经后台 writer），Resume（async + auto-load + 答案）后继续跑完。验 async 与
// interrupt/Resume 不冲突（Resume 后续超步的 async 写 PendingInterrupts 干净由 clearInterrupts 保证）。
func TestDurability_Async_InterruptResume(t *testing.T) {
	maxRounds, interruptAt := 5, 2
	g, counterCh, maxCh := interruptLoopGraph(t, maxRounds, interruptAt)
	cp := newRecordingCp()
	sid := "s_async_int"
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm1,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("Run 1 应中断，实际 %v", err)
	}
	err2 := compose_v2.Run(context.Background(), g, registerCM(t, counterCh, maxCh),
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"dur_int_ask": schema.UserMessage("yes"),
		}))
	if err2 != nil {
		t.Fatalf("Run 2（async Resume）应跑完，实际 %v", err2)
	}
}

// joinOrderCp 包 InMemoryCheckpointer：SaveNS 故意 sleep 放大窗口 + 记录 save_done / delete
// 事件顺序——验 join-before-cleanup（成功 Run 出口 join 必须 drain 完所有后台写才 DeleteSubtree）。
// in-memory 太快测不出这个时序——慢 SaveNS 让"缺 join 时 delete 抢在后台写前"成为确定性失败。
type joinOrderCp struct {
	*compose_v2.InMemoryCheckpointer
	mu     sync.Mutex
	events []string
}

func (c *joinOrderCp) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	time.Sleep(3 * time.Millisecond) // 放大窗口
	err := c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
	c.mu.Lock()
	c.events = append(c.events, "save_done")
	c.mu.Unlock()
	return err
}

func (c *joinOrderCp) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.mu.Lock()
	c.events = append(c.events, "delete")
	c.mu.Unlock()
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

// TestDurability_Async_JoinBeforeCleanup：async 成功 Run——所有后台写（save_done）必须在
// cleanup（delete）**之前**完成。慢 SaveNS 让缺 join 时 delete 抢先成为确定性失败。
func TestDurability_Async_JoinBeforeCleanup(t *testing.T) {
	maxRounds := 5
	g, _, maxCh := loopGraph(t, maxRounds)
	cp := &joinOrderCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	if err := runLoop(t, g, maxCh, maxRounds,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_async_join")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cp.mu.Lock()
	evs := append([]string(nil), cp.events...)
	cp.mu.Unlock()
	saveCount, deleteIdx := 0, -1
	for i, e := range evs {
		if e == "delete" {
			deleteIdx = i
		} else {
			saveCount++
		}
	}
	if deleteIdx < 0 {
		t.Fatalf("应有 delete 事件（成功 cleanup），序列=%v", evs)
	}
	// join drain 完后台写才 cleanup → 所有 maxRounds-1 次后台写都在 delete 前完成。
	// 缺 join 时 Run 不等后台（慢 SaveNS），读到的 save_done 数会不足 → 确定性失败。
	if saveCount != maxRounds-1 {
		t.Errorf("应有 %d 次后台写都在 cleanup 前 drain 完，实际 save_done=%d（join 没等？）序列=%v",
			maxRounds-1, saveCount, evs)
	}
	for i := deleteIdx + 1; i < len(evs); i++ {
		if evs[i] == "save_done" {
			t.Errorf("save_done 出现在 delete 之后——join 没 drain 完就 cleanup（残留风险）序列=%v", evs)
			break
		}
	}
}

// TestDurability_PropagatedToNodeCtx：Run 入口解析出的档经 ctx 透传到节点——子图据此自动
// 继承父档。直验 graph_loop 的 ctx push（节点内 GetDurability 读到的是 Run 入口归一后的值）。
func TestDurability_PropagatedToNodeCtx(t *testing.T) {
	var seen compose_v2.Durability
	probe := compose_v2.NewBuilder().
		AddNode("probe", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			seen = compose_v2.GetDurability(ctx)
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "probe").
		AddEdge("probe", compose_v2.EndNode)
	g, err := probe.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// (a) Sync 档 → 节点 ctx 读到 Sync（子图会据此继承）。
	seen = ""
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithDurability(compose_v2.DurabilitySync)); err != nil {
		t.Fatalf("Run sync: %v", err)
	}
	if seen != compose_v2.DurabilitySync {
		t.Errorf("节点 ctx 应继承 Sync，实际 %q", seen)
	}

	// (b) 不设档 → 节点读到**归一后**的 Exit（非空）——证明 Run 入口先 resolveDurability
	// 再 push，子图继承的是具体档不是空串。
	seen = ""
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run default: %v", err)
	}
	if seen != compose_v2.DurabilityExit {
		t.Errorf("默认档节点 ctx 应读到归一后的 Exit（非空），实际 %q", seen)
	}
}

// interruptLoopGraph 造一个跑到 interruptAt 轮中断一次的 loop——除非 ctx 带答案
// （Resume 时）。验 durability 与 HITL interrupt 的交互。
func interruptLoopGraph(t *testing.T, maxRounds, interruptAt int) (*compose_v2.Graph, *compose_v2.Channel[int], *compose_v2.Channel[int]) {
	t.Helper()
	counterCh := compose_v2.Sum("dur_int")
	maxCh := compose_v2.LastValue[int]("dur_int_max")
	b := compose_v2.NewBuilder().
		AddNode("step", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			count, _ := compose_v2.Get(chs, counterCh)
			if count == interruptAt {
				// 首跑无答案 → 中断；Resume 带答案 → 放行。InterruptID 自填覆盖框架 5 维。
				if _, ok := compose_v2.GetResumeAnswerText(ctx, "dur_int_ask"); !ok {
					return nil, &compose_v2.InterruptError{
						Query: &compose_v2.AskQuery{InterruptID: "dur_int_ask", Question: "continue?"},
					}
				}
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(counterCh, 1)}, nil
		}).
		AddEdge(compose_v2.StartNode, "step").
		AddConditionalEdge("step", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			count, _ := compose_v2.Get(chs, counterCh)
			if count >= compose_v2.MustGet(chs, maxCh) {
				return compose_v2.EndNode, nil
			}
			return "step", nil
		}).
		WithMaxVisit(50)
	b = compose_v2.WithChannel(b, counterCh)
	b = compose_v2.WithChannel(b, maxCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, counterCh, maxCh
}

// registerCM 建 cm 只 register channel 不 Restore（auto-load Resume 路径——Run 内部幂等
// 自动 Restore，caller 只需保证 channel 已注册可反序列化）。
func registerCM(t *testing.T, chs ...*compose_v2.Channel[int]) *compose_v2.ChannelManager {
	t.Helper()
	cm := compose_v2.NewChannelManager()
	for _, ch := range chs {
		if err := compose_v2.RegisterChannel(cm, ch); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	return cm
}

// TestDurability_Sync_InterruptViaDefer：durability 与 HITL interrupt 交互——中断超步在写点
// 之前 return，所以中断 cp 由 **defer 独家**持久化（带 PendingInterrupts），每超步写点只在
// 正常 continue 路径触发（PendingInterrupts 恒空）。两条 SaveNS 路径**每超步互斥**。
// 且验：durability 写的 cp 能被 auto-load Resume（同 sid，无显式 WithResumeFrom）跑完。
func TestDurability_Sync_InterruptViaDefer(t *testing.T) {
	maxRounds, interruptAt := 5, 2
	g, counterCh, maxCh := interruptLoopGraph(t, maxRounds, interruptAt)
	cp := newRecordingCp()
	sid := "s_int"

	// Run 1：sync，跑到 interruptAt 中断。
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm1,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("Run 1 应中断，实际 err=%v", err)
	}
	caps := cp.capsCopy()
	if len(caps) < 2 {
		t.Fatalf("应有 ≥1 每超步写 + 1 defer 中断 cp，实际 %d", len(caps))
	}
	// 最后一份（defer 中断 cp）带 PendingInterrupts；之前的（每超步 continue 写）恒不带。
	last := caps[len(caps)-1]
	if last.pendingCount == 0 {
		t.Errorf("末份 cp（defer 中断）应带 PendingInterrupts，实际 0")
	}
	for i, c := range caps[:len(caps)-1] {
		if c.pendingCount != 0 {
			t.Errorf("每超步 cp[%d] 不该带 PendingInterrupts（中断走 defer 不走写点），实际 %d", i, c.pendingCount)
		}
	}

	// Run 2：auto-load（同 sid + cper，无 WithResumeFrom）+ 带答案 → 应跑完不再中断。
	err2 := compose_v2.Run(context.Background(), g, registerCM(t, counterCh, maxCh),
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"dur_int_ask": schema.UserMessage("yes"),
		}))
	if err2 != nil {
		t.Fatalf("Run 2（auto-load Resume + 答案）应跑完，实际 %v", err2)
	}
}

// restoreCM 建 cm + register 两 channel + Restore snapshot（Resume 前置）。
func restoreCM(t *testing.T, snap *compose_v2.Checkpoint, chs ...*compose_v2.Channel[int]) *compose_v2.ChannelManager {
	t.Helper()
	cm := compose_v2.NewChannelManager()
	for _, ch := range chs {
		if err := compose_v2.RegisterChannel(cm, ch); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	if err := cm.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return cm
}

func errcontains(err error, sub string) bool {
	return err != nil && strings.Contains(err.Error(), sub)
}
