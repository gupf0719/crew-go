package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/schema"
)

// 扇出仲裁：子调用档没写成、槽属于别人（不带 ErrRunAborted）时，也要排在兄弟的中断之前。
func TestPickFanoutError_NotPersistedBeatsInterrupt(t *testing.T) {
	ie := &InterruptError{Query: &AskQuery{InterruptID: "q1", Question: "?"}}
	np := &checkpointNotPersistedError{
		exit:  &InterruptError{Query: &AskQuery{InterruptID: "q2", Question: "?"}},
		cause: fmt.Errorf("root cp save failed: %w", ErrGraphIDConflict),
	}
	for _, errs := range [][]error{{ie, np}, {np, ie}} {
		got := PickFanoutError(errs)
		if !errors.Is(got, ErrCheckpointNotPersisted) {
			t.Fatalf("应报档没写成，实际 %v", got)
		}
		if _, isInt := IsInterrupt(got); isInt {
			t.Fatalf("档没写成的错误链上不该有中断，实际 %v", got)
		}
	}
}

// 以下是出口记账的三条不变量（见 exit_checkpoint.go）。

func setExitGrace(t *testing.T, d time.Duration) {
	t.Helper()
	old := exitBookkeepingGrace
	exitBookkeepingGrace = d
	t.Cleanup(func() { exitBookkeepingGrace = old })
}

// exitProbeCP 的读写都尊重 ctx（ctx 已结束就报 ctx 错、不写，同持久后端）。各开关都可以在 Run 跑着的时候改：
//   - saveDelay / loadDelay：写档 / 读档先等这么久；
//   - hangSave / hangLoad / hangDelete：一直挂到 ctx 结束；
//   - failAfter：写档与删除先等这么久，再报一个跟 ctx 无关的错（客户端自己的超时，比宽限短）；
//   - lateCommit：写档不理 ctx，等这么久之后照样写进去，再看 ctx（写进去了、回包时调用方已经走了）；
//   - loadFail：读档报错。
type exitProbeCP struct {
	*InMemoryCheckpointer
	saveDelay  atomic.Int64
	loadDelay  atomic.Int64
	failAfter  atomic.Int64
	lateCommit atomic.Int64
	hangSave   atomic.Bool
	hangLoad   atomic.Bool
	hangDelete atomic.Bool
	loadFail   atomic.Bool
	saves      atomic.Int32
	deletes    atomic.Int32
}

func newExitProbeCP() *exitProbeCP {
	return &exitProbeCP{InMemoryCheckpointer: NewInMemoryCheckpointer()}
}

func probeWait(ctx context.Context, d time.Duration) error {
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	}
	return ctx.Err()
}

func probeClientTimeout(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return errors.New("probe backend: i/o timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *exitProbeCP) LoadNS(ctx context.Context, sid, ns string) (*Checkpoint, error) {
	if c.loadFail.Load() {
		return nil, errors.New("probe backend: read replica unavailable")
	}
	if c.hangLoad.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := probeWait(ctx, time.Duration(c.loadDelay.Load())); err != nil {
		return nil, err
	}
	return c.InMemoryCheckpointer.LoadNS(ctx, sid, ns)
}

func (c *exitProbeCP) SaveNS(ctx context.Context, sid, ns string, cp *Checkpoint) error {
	c.saves.Add(1)
	if c.hangSave.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	if d := time.Duration(c.failAfter.Load()); d > 0 {
		return probeClientTimeout(ctx, d)
	}
	if d := time.Duration(c.lateCommit.Load()); d > 0 {
		time.Sleep(d)
		if err := c.InMemoryCheckpointer.SaveNS(context.Background(), sid, ns, cp); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err := probeWait(ctx, time.Duration(c.saveDelay.Load())); err != nil {
		return err
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *exitProbeCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.deletes.Add(1)
	if c.hangDelete.Load() {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if d := time.Duration(c.failAfter.Load()); d > 0 {
		return 0, probeClientTimeout(ctx, d)
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

func exitCompile(t *testing.T, b *GraphBuilder) *Graph {
	t.Helper()
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// blockUntilDone 的节点一直等到 ctx 结束；started 非 nil 时进节点先发一个信号。
func blockUntilDone(started chan<- struct{}) NodeFunc {
	return func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		if started != nil {
			started <- struct{}{}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

// nestedNS 是第 k 层嵌套的 ns（第 0 层是根，ns 为空）。
func nestedNS(k int) string {
	ns := ""
	for i := 1; i <= k; i++ {
		ns = JoinNS(ns, fmt.Sprintf("l%d", i))
	}
	return ns
}

// nestedBlockGraph 造 depth 层嵌套：每层的 host 节点跑下一层，最内层的节点一直等到 ctx 结束（进节点时发 started）。
func nestedBlockGraph(t *testing.T, depth int, started chan<- struct{}) *Graph {
	var inner *Graph
	for k := depth - 1; k >= 0; k-- {
		b := NewBuilder().WithName(fmt.Sprintf("exit_l%d", k))
		if k == depth-1 {
			b.AddNode("block", blockUntilDone(started))
			b.AddEdge(StartNode, "block")
			b.AddEdge("block", EndNode)
		} else {
			child, childNS := inner, nestedNS(k+1)
			b.AddNode("host", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
				return nil, Run(ctx, child, NewChannelManager(), WithRunNS(childNS))
			})
			b.AddEdge(StartNode, "host")
			b.AddEdge("host", EndNode)
		}
		inner = exitCompile(t, b)
	}
	return inner
}

// runCanceledNested 跑 depth 层嵌套，最内层进节点时先 beforeCancel()、再取消调用方；返回取消之后 Run 又过了多久
// 才返回，以及 Run 的错误。
func runCanceledNested(t *testing.T, depth int, cp Checkpointer, sid string, beforeCancel func(), opts ...RunOption) (time.Duration, error) {
	t.Helper()
	started := make(chan struct{}, 1)
	g := nestedBlockGraph(t, depth, started)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var canceledAt atomic.Int64
	go func() {
		<-started
		if beforeCancel != nil {
			beforeCancel()
		}
		canceledAt.Store(time.Now().UnixNano())
		cancel()
	}()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, g, NewChannelManager(), append([]RunOption{WithCheckpointer(cp), WithSessionID(sid)}, opts...)...)
	}()
	select {
	case err := <-done:
		return time.Since(time.Unix(0, canceledAt.Load())), err
	case <-time.After(30 * time.Second):
		t.Fatal("Run 没有返回")
		return 0, nil
	}
}

// ---- 不变量 3：宽限从每次调用自己的起点算；调用方放弃之后每一类调用的失败预算一个宽限 ----

// 内层自己超时（工具超时、钩子给的截止时间）先退出，过了不止一个宽限调用方才取消：外层出口照样有宽限，
// 健康的后端写得进去。曾经整个任务共用一个截止时间、由最先结束的那一层定下——内层那次就把它定了，外层出口
// 拿到的截止时间早已过去，档写不进去，任务丢了（审计实测）。
func TestExitGrace_InnerTimeoutDoesNotConsumeCallerGrace(t *testing.T) {
	setExitGrace(t, 200*time.Millisecond)
	cp := newExitProbeCP()
	cp.saveDelay.Store(int64(20 * time.Millisecond)) // 写档要一点时间（宽限到点的取消才赶得上它）
	bi := NewBuilder().WithName("grace_inner")
	bi.AddNode("block", blockUntilDone(nil))
	bi.AddEdge(StartNode, "block")
	bi.AddEdge("block", EndNode)
	inner := exitCompile(t, bi)

	started := make(chan struct{}, 1)
	b := NewBuilder().WithName("grace_outer")
	b.AddNode("host", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		ictx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if err := Run(ictx, inner, NewChannelManager(), WithRunNS("inner")); !errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("前置：内层应因自己的超时退出：%v", err)
		}
		return nil, nil
	})
	b.AddNode("wait", blockUntilDone(started))
	b.AddEdge(StartNode, "host")
	b.AddEdge("host", "wait")
	b.AddEdge("wait", EndNode)
	g := exitCompile(t, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		time.Sleep(2 * exitBookkeepingGrace)
		cancel()
	}()
	const sid = "grace_inner_timeout"
	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid))
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("内层自己超时不该耗掉调用方的宽限，外层出口应写成：%v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应照常报调用方取消（可以续）：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatal("外层的档应已写成")
	}
}

// 内层自己超时退出时后端恰好卡了一下（写档等满宽限、内层档没写成），业务把内层的超时当成可以继续；后端随后恢复，
// 过了很久调用方取消：外层出口照样写成。调用方还在等时某一层自己超时，那一层的失败不计进调用方放弃之后的预算。
// 曾经一次等满宽限就把整个任务的写档类记成「挂住」、一直记着，外层写档直接不发，任务丢了（审计实测）。
func TestExitGrace_EarlierInnerStallNotCharged(t *testing.T) {
	setExitGrace(t, 100*time.Millisecond)
	cp := newExitProbeCP()
	bi := NewBuilder().WithName("grace_stall_inner")
	bi.AddNode("block", blockUntilDone(nil))
	bi.AddEdge(StartNode, "block")
	bi.AddEdge("block", EndNode)
	inner := exitCompile(t, bi)

	started := make(chan struct{}, 1)
	b := NewBuilder().WithName("grace_stall_outer")
	b.AddNode("host", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		cp.saveDelay.Store(int64(time.Second)) // 后端卡一下
		ictx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		err := Run(ictx, inner, NewChannelManager(), WithRunNS("inner"))
		cp.saveDelay.Store(0) // 后端恢复
		if !errors.Is(err, ErrCheckpointNotPersisted) {
			return nil, fmt.Errorf("前置：内层的档应因后端卡住没写成：%v", err)
		}
		return nil, nil
	})
	b.AddNode("wait", blockUntilDone(started))
	b.AddEdge(StartNode, "host")
	b.AddEdge("host", "wait")
	b.AddEdge("wait", EndNode)
	g := exitCompile(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		time.Sleep(5 * exitBookkeepingGrace)
		cancel()
	}()
	const sid = "grace_stall"
	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid))
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("后端早已恢复，外层出口应写成：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatalf("外层的档应已写成：%v", err)
	}
}

// 后端慢但每次都在宽限内写完（三层嵌套、每次写档 0.6 个宽限）：调用方取消之后三层都写成。宽限按整个任务
// 共用的话，第二层写到一半就到点了。
func TestExitGrace_SlowBackendStillCompletes(t *testing.T) {
	setExitGrace(t, 200*time.Millisecond)
	cp := newExitProbeCP()
	cp.saveDelay.Store(int64(120 * time.Millisecond))
	const sid = "grace_slow"
	_, err := runCanceledNested(t, 3, cp, sid, nil)
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("每次写档都在宽限内完成，不该报档没写成：%v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应照常报调用方取消：%v", err)
	}
	for k := 0; k < 3; k++ {
		if ok, _ := cp.ExistsNS(context.Background(), sid, nestedNS(k)); !ok {
			t.Errorf("ns %q 的档应已写成", nestedNS(k))
		}
	}
}

// 开着陈旧写回守卫、后端每次调用都在宽限内（0.6 个宽限）完成：检查与写档各是一次调用、各有一段宽限，档写成。
// 曾经检查与写档共用一段宽限，两次调用加起来超了，写档被截断、任务终局（审计实测）。
func TestExitGrace_SlowBackendWithGuardCompletes(t *testing.T) {
	setExitGrace(t, 200*time.Millisecond)
	cp := newExitProbeCP()
	const sid = "grace_slow_guard"
	_, err := runCanceledNested(t, 1, cp, sid, func() {
		cp.saveDelay.Store(int64(120 * time.Millisecond))
		cp.loadDelay.Store(int64(120 * time.Millisecond))
	}, WithStaleWriteGuard())
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("检查与写档各在宽限内完成，档应写成：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatalf("档应已写成：%v", err)
	}
}

// 后端整个挂住（写档、删除都挂到 ctx 结束）、四层嵌套、调用方取消：Run 在约两个宽限之后返回（最内层写档一个、
// 它清档一个，外面几层的清档不再发）。每次调用各等一个的话层数越多越久。
func TestExitGrace_HungBackendBoundIndependentOfDepth(t *testing.T) {
	setExitGrace(t, 150*time.Millisecond)
	cp := newExitProbeCP()
	waited, err := runCanceledNested(t, 4, cp, "grace_hung_depth", func() {
		cp.hangSave.Store(true)
		cp.hangDelete.Store(true)
	})
	if !errors.Is(err, ErrCheckpointNotPersisted) || !errors.Is(err, ErrRunAborted) {
		t.Fatalf("后端挂住应报档没写成（终局）：%v", err)
	}
	if waited > 3*exitBookkeepingGrace {
		t.Fatalf("调用方取消之后等了 %v，超过三个宽限（%v）：等待跟层数成正比", waited, exitBookkeepingGrace)
	}
}

// 后端每次调用都在宽限内返回、但返回的是错误（客户端自己的超时比宽限短，0.6 个宽限）、十层嵌套：调用方取消之后
// 写档、删除失败所花的时间各合计不超过一个宽限，Run 在两个宽限内返回。曾经只有「等满宽限」才算挂住，这种后端
// 永远等不满，每一层的清档都再等一次，等待跟层数成正比（审计实测）。
func TestExitGrace_FastFailingBackendBoundIndependentOfDepth(t *testing.T) {
	setExitGrace(t, 100*time.Millisecond)
	cp := newExitProbeCP()
	waited, err := runCanceledNested(t, 10, cp, "grace_fast_fail", func() {
		cp.failAfter.Store(int64(60 * time.Millisecond))
	})
	if !errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("后端一直报错应报档没写成：%v", err)
	}
	if waited > 2*exitBookkeepingGrace+exitBookkeepingGrace/2 {
		t.Fatalf("调用方取消之后等了 %v，超过两个半宽限（宽限 %v）：等待跟层数成正比", waited, exitBookkeepingGrace)
	}
}

// 开着陈旧写回守卫、后端的读挂住而写、删健康、四层嵌套：每一层的检查读不出来就放行，读挂住的预算用完之后检查
// 不再发；各层照常写档，Run 在约一个宽限之后返回。曾经检查读到宽限到点却被放行吞掉、从不计入，每一层都再等
// 一个宽限（审计实测）。
func TestExitGrace_StaleCheckReadHangBoundIndependentOfDepth(t *testing.T) {
	setExitGrace(t, 100*time.Millisecond)
	cp := newExitProbeCP()
	const sid = "grace_read_hang"
	waited, err := runCanceledNested(t, 4, cp, sid, func() { cp.hangLoad.Store(true) }, WithStaleWriteGuard())
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("只有读挂住，写档应照常写成：%v", err)
	}
	if waited > 2*exitBookkeepingGrace {
		t.Fatalf("调用方取消之后等了 %v，超过两个宽限（宽限 %v）：每一层的检查都再等一次", waited, exitBookkeepingGrace)
	}
	if ok, _ := cp.InMemoryCheckpointer.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatal("根的档应已写成")
	}
}

// 写档挂住、删除正常：写档等满宽限之后，清档照样做，回答之前那份旧档删掉（不删的话再续一次续进那份，
// 已经答过的问题重问、副作用重做）。各类调用的预算分开记，就是为这个。
func TestExitGrace_WriteHangStillPurges(t *testing.T) {
	setExitGrace(t, 150*time.Millisecond)
	cp := newExitProbeCP()
	b := NewBuilder().WithName("grace_write_hang")
	ask := func(q string) NodeFunc {
		return func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
			_, err := Ask(ctx, &AskQuery{Question: q})
			return nil, err
		}
	}
	b.AddNode("ask1", ask("批准吗"))
	b.AddNode("ask2", ask("再确认一次"))
	b.AddEdge(StartNode, "ask1")
	b.AddEdge("ask1", "ask2")
	b.AddEdge("ask2", EndNode)
	g := exitCompile(t, b)
	const sid = "grace_write_hang"
	ie, ok := IsInterrupt(Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 ask1")
	}
	cp.hangSave.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid),
		WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if !errors.Is(err, ErrCheckpointNotPersisted) || !errors.Is(err, ErrRunAborted) {
		t.Fatalf("写档挂住应报档没写成（终局）：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatalf("写档挂住不该连累清档，回答之前那份档应已删掉：%v", err)
	}
}

// 调用方 ctx 已结束、后端又挂住：出口写档与清档各最多等宽限期，Run 照样返回，报档没写成。
func TestExitCheckpoint_GraceBoundsHungBackend(t *testing.T) {
	setExitGrace(t, 50*time.Millisecond)
	cp := newExitProbeCP()
	cp.hangSave.Store(true)
	cp.hangDelete.Store(true)
	started := make(chan struct{}, 1)
	b := NewBuilder().WithName("grace_probe")
	b.AddNode("a", blockUntilDone(started))
	b.AddEdge(StartNode, "a")
	b.AddEdge("a", EndNode)
	g := exitCompile(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, g, NewChannelManager(), WithCheckpointer(cp), WithSessionID("grace"))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCheckpointNotPersisted) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("宽限期到了应报档没写成（原因是宽限期超时）：%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("后端挂住时 Run 没有在宽限期之后返回")
	}
}

// ---- 不变量 2：写档结果不明时，清档前的检查认得这次写的那一代 ----

// 开着陈旧写回守卫、后端在宽限到点之后才提交（客户端看到的是 ctx 错，提交与取消竞争、结果不明）：写档按没写成
// 处理，清档前的检查认出槽里是这次写的那一代，照常清掉——不报 ErrCheckpointStale，也不留一份可续的档配一个终局
// 结论。曾经把自己晚到的那次提交当成别的执行推进了槽：跳过清档、报陈旧写回（审计实测）。
func TestExitCheckpoint_LateCommitNotReportedAsStale(t *testing.T) {
	setExitGrace(t, 100*time.Millisecond)
	cp := newExitProbeCP()
	const sid = "late_commit"
	_, err := runCanceledNested(t, 1, cp, sid, func() { cp.lateCommit.Store(int64(150 * time.Millisecond)) }, WithStaleWriteGuard())
	if errors.Is(err, ErrCheckpointStale) {
		t.Fatalf("自己晚到的提交不该被当成别的执行推进了槽：%v", err)
	}
	if !errors.Is(err, ErrCheckpointNotPersisted) || !errors.Is(err, ErrRunAborted) {
		t.Fatalf("写档结果不明应按没写成处理（终局）：%v", err)
	}
	if saved, _ := cp.InMemoryCheckpointer.LoadNS(context.Background(), sid, ""); saved != nil && saved.HasResumableWork() {
		t.Fatal("结论是终局，槽里不该留着一份可续的档")
	}
}

// ---- 不变量 1：存储调用之外的代码在结论定下之后才调 ----

type panicOnStaleWarnHandler struct{ slog.Handler }

func (h panicOnStaleWarnHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "stale-write guard") {
		panic("log handler bug")
	}
	return h.Handler.Handle(ctx, r)
}

// 开着陈旧写回守卫、出口检查的读失败（放行并记一条告警），业务的日志 handler 在这条告警上 panic：档照常写成、照常
// 报中断，handler 的 panic 当场兜住、不进错误。曾经这条告警在兜存储 panic 的那段里记，handler 一 panic 写档就不做了、
// 结论变成终局失败（审计实测）；后来改成挂在错误上。
func TestExitCheckpoint_LogHandlerPanicKeepsVerdict(t *testing.T) {
	oldLogger, oldW, oldF := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(panicOnStaleWarnHandler{slog.NewTextHandler(io.Discard, nil)}))
	defer func() {
		slog.SetDefault(oldLogger)
		log.SetOutput(oldW)
		log.SetFlags(oldF)
	}()
	cp := newExitProbeCP()
	b := NewBuilder().WithName("log_panic")
	b.AddNode("ask", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		cp.loadFail.Store(true)
		_, err := Ask(ctx, &AskQuery{Question: "批准吗"})
		return nil, err
	})
	b.AddEdge(StartNode, "ask")
	b.AddEdge("ask", EndNode)
	const sid = "log_panic"
	err := Run(context.Background(), exitCompile(t, b), NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid), WithStaleWriteGuard())
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("日志 handler 的 panic 不该决定档写没写成：%v", err)
	}
	if _, isInt := IsInterrupt(err); !isInt {
		t.Fatalf("档写成了，应照常报中断：%v", err)
	}
	var pe *PanicError
	if errors.As(err, &pe) {
		t.Errorf("日志 handler 的 panic 不该进错误：%v", err)
	}
	if ok, _ := cp.InMemoryCheckpointer.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatal("档应已写成")
	}
}

// ---- 失败预算的计法：墙钟并集、只计调用方放弃之后、ctx 活着的不拦；清档前检查没做成照常清 ----

// nsProbeCP 按 ns 控制的测试后端（读写都尊重 ctx）：saveFail 立刻报错、saveHang 挂到 ctx 结束、delStall 先等这么久
// 再报错（客户端自己的超时）、loadHang 所有读挂到 ctx 结束；记下每个 ns 的写、删次数。
type nsProbeCP struct {
	*InMemoryCheckpointer
	mu        sync.Mutex
	saveFail  map[string]bool
	saveHang  map[string]bool
	delStall  map[string]time.Duration
	loadHang  atomic.Bool
	loadPanic atomic.Bool
	saveCount map[string]int
	delCount  map[string]int
}

func newNSProbeCP() *nsProbeCP {
	return &nsProbeCP{InMemoryCheckpointer: NewInMemoryCheckpointer(), saveFail: map[string]bool{}, saveHang: map[string]bool{},
		delStall: map[string]time.Duration{}, saveCount: map[string]int{}, delCount: map[string]int{}}
}

func (c *nsProbeCP) set(f func()) { c.mu.Lock(); f(); c.mu.Unlock() }

func (c *nsProbeCP) count(m map[string]int, ns string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return m[ns]
}

func (c *nsProbeCP) LoadNS(ctx context.Context, sid, ns string) (*Checkpoint, error) {
	if c.loadPanic.Load() {
		panic("probe backend: load panicked")
	}
	if c.loadHang.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.InMemoryCheckpointer.LoadNS(ctx, sid, ns)
}

func (c *nsProbeCP) SaveNS(ctx context.Context, sid, ns string, cp *Checkpoint) error {
	c.mu.Lock()
	c.saveCount[ns]++
	fail, hang := c.saveFail[ns], c.saveHang[ns]
	c.mu.Unlock()
	if fail {
		return errors.New("probe backend: write rejected")
	}
	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *nsProbeCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.mu.Lock()
	c.delCount[ns]++
	stall := c.delStall[ns]
	c.mu.Unlock()
	if stall > 0 {
		if err := probeClientTimeout(ctx, stall); err != nil {
			return 0, err
		}
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

func blockGraphNamed(t *testing.T, name string, started chan<- struct{}) *Graph {
	b := NewBuilder().WithName(name)
	b.AddNode("block", blockUntilDone(started))
	b.AddEdge(StartNode, "block")
	b.AddEdge("block", EndNode)
	return exitCompile(t, b)
}

// 两个并行分支同时退出、各自清档同时卡 0.6 个宽限后报错（之后后端恢复）：调用方放弃之后删除失败在墙钟上只占了
// 0.6 个宽限，根的终局清档照常发、回答之前那份旧档删掉。曾经按每次失败各自的耗时相加（0.6 + 0.6 = 1.2），根的
// 清档不发，那份可续的旧档留在槽里（审计实测）。
func TestExitGrace_ConcurrentFailuresCountedOnce(t *testing.T) {
	setExitGrace(t, 200*time.Millisecond)
	cp := newNSProbeCP()
	started := make(chan struct{}, 2)
	inner1 := blockGraphNamed(t, "conc_inner1", started)
	inner2 := blockGraphNamed(t, "conc_inner2", started)
	b := NewBuilder().WithName("conc_root")
	b.AddNode("gate", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		_, err := Ask(ctx, &AskQuery{Question: "批准吗"})
		return nil, err
	})
	b.AddNode("b1", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		return nil, Run(ctx, inner1, NewChannelManager(), WithRunNS("b1"))
	})
	b.AddNode("b2", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		return nil, Run(ctx, inner2, NewChannelManager(), WithRunNS("b2"))
	})
	b.AddEdge(StartNode, "gate")
	b.AddEdge("gate", "b1")
	b.AddEdge("gate", "b2")
	b.AddEdge("b1", EndNode)
	b.AddEdge("b2", EndNode)
	g := exitCompile(t, b)
	const sid = "conc_failures"
	ie, ok := IsInterrupt(Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 gate")
	}
	cp.set(func() {
		cp.saveFail["b1"], cp.saveFail["b2"] = true, true
		cp.delStall["b1"], cp.delStall["b2"] = 6*exitBookkeepingGrace/10, 6*exitBookkeepingGrace/10
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		<-started
		cancel()
	}()
	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid),
		WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if !errors.Is(err, ErrRunAborted) {
		t.Fatalf("前置：子层档没写成，根应终局：%v", err)
	}
	if left, _ := cp.InMemoryCheckpointer.LoadNS(context.Background(), sid, ""); left != nil && left.HasResumableWork() {
		t.Fatalf("并发失败在墙钟上只占 0.6 个宽限，根的终局清档应照常发（发了 %d 次），回答之前那份旧档不该还在：%v",
			cp.count(cp.delCount, ""), err)
	}
}

// 内层自己超时（调用方还在等）时开始写档、后端挂住；写到 0.8 个宽限时调用方取消，内层写档等满宽限失败；后端随后
// 恢复：外层出口写档照常发、写成。只计调用方放弃之后的那 0.2 个宽限。曾经从内层超时那一刻记满一个宽限，外层写档
// 不发、任务丢了（审计实测）。
func TestExitGrace_StraddlingCallChargedOnlyAfterGiveUp(t *testing.T) {
	setExitGrace(t, 200*time.Millisecond)
	cp := newNSProbeCP()
	inner := blockGraphNamed(t, "straddle_inner", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := NewBuilder().WithName("straddle_outer")
	b.AddNode("host", func(hctx context.Context, _ Channels) ([]ChannelWrite, error) {
		cp.set(func() { cp.saveHang["inner"] = true })
		ictx, icancel := context.WithTimeout(hctx, 30*time.Millisecond)
		defer icancel()
		go func() {
			time.Sleep(30*time.Millisecond + 8*exitBookkeepingGrace/10) // 内层写档挂到 0.8 个宽限时调用方取消
			cancel()
		}()
		err := Run(ictx, inner, NewChannelManager(), WithRunNS("inner"))
		cp.set(func() { cp.saveHang["inner"] = false }) // 后端恢复
		if !errors.Is(err, ErrCheckpointNotPersisted) {
			return nil, fmt.Errorf("前置：内层档应没写成：%v", err)
		}
		return nil, nil // 业务把内层超时当成可以继续
	})
	b.AddNode("wait", blockUntilDone(nil))
	b.AddEdge(StartNode, "host")
	b.AddEdge("host", "wait")
	b.AddEdge("wait", EndNode)
	const sid = "straddle"
	err := Run(ctx, exitCompile(t, b), NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid))
	if errors.Is(err, ErrCheckpointNotPersisted) {
		t.Fatalf("调用方放弃之后写档失败只占约 0.2 个宽限、后端已恢复，外层写档应照常写成（发了 %d 次）：%v",
			cp.count(cp.saveCount, ""), err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatalf("外层的档应已写成：%v", err)
	}
}

// 节点用 context.WithoutCancel 跑的嵌套 Run（它自己的 ctx 一直活着、后端健康）：同一任务里别的分支在调用方放弃
// 之后把写档预算用完了，它的出口写档照样发、写成。ctx 活着的调用不等宽限，拦下它只会丢档（审计实测）。
func TestExitGrace_LiveCtxCallNotRefused(t *testing.T) {
	setExitGrace(t, 100*time.Millisecond)
	cp := newNSProbeCP()
	started := make(chan struct{}, 1)
	innerA := blockGraphNamed(t, "live_innerA", started)
	bq := NewBuilder().WithName("live_innerB")
	bq.AddNode("ask", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		_, err := Ask(ctx, &AskQuery{Question: "补偿要执行吗"})
		return nil, err
	})
	bq.AddEdge(StartNode, "ask")
	bq.AddEdge("ask", EndNode)
	innerB := exitCompile(t, bq)
	var innerBErr error
	b := NewBuilder().WithName("live_root")
	b.AddNode("a", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		return nil, Run(ctx, innerA, NewChannelManager(), WithRunNS("a"))
	})
	b.AddNode("b", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		<-ctx.Done()
		time.Sleep(3 * exitBookkeepingGrace) // 等分支 a 的写档等满宽限、记进预算
		innerBErr = Run(context.WithoutCancel(ctx), innerB, NewChannelManager(), WithRunNS("b"))
		return nil, nil
	})
	b.AddEdge(StartNode, "a")
	b.AddEdge(StartNode, "b")
	b.AddEdge("a", EndNode)
	b.AddEdge("b", EndNode)
	cp.set(func() { cp.saveHang["a"] = true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		cancel()
	}()
	// 根层随后因分支 a 终局、清掉整棵子树——这里看的是 b 的嵌套 Run 自己那次出口：写档发了、写成了（照常报中断）
	_ = Run(ctx, exitCompile(t, b), NewChannelManager(), WithCheckpointer(cp), WithSessionID("live_ctx"))
	if n := cp.count(cp.saveCount, "b"); n == 0 {
		t.Fatalf("ctx 一直活着的嵌套 Run 的出口写档应照常发（后端健康），被同一任务的预算拦下了：%v", innerBErr)
	}
	if _, isInt := IsInterrupt(innerBErr); !isInt || errors.Is(innerBErr, ErrCheckpointNotPersisted) {
		t.Fatalf("它的挂起档应写成、照常报中断：%v", innerBErr)
	}
}

// 开着陈旧写回守卫：本执行（调用方已放弃）的嵌套层检查读挂住、把读预算用完；之后读恢复，另一个执行把槽推进成
// 自己的挂起档；本执行根层终局清档时检查没做成——照常清。这道检查是尽力型的，拿不到结论时跟写档一样放行：代价是
// 同一个 sid 上并发跑两个任务（违反 RunID 契约）时，另一个执行的挂起档跟着清掉（本用例的场景）；拦下的代价落在
// 合法调用上——成功收尾时旧的挂起档留着、同样的答案再续跑一次会重放节点（见 SuccessPurgeCheckNotDoneStillCleans）。
func TestExitCheckpoint_PurgeCheckNotDoneStillCleans(t *testing.T) {
	setExitGrace(t, 100*time.Millisecond)
	cp := newNSProbeCP()
	started := make(chan struct{}, 1)
	inner := blockGraphNamed(t, "blind_inner", started)
	const sid = "blind_purge"
	b := NewBuilder().WithName("blind_root")
	b.AddNode("gate", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		_, err := Ask(ctx, &AskQuery{Question: "开始吗"})
		return nil, err
	})
	b.AddNode("work", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		_ = Run(ctx, inner, NewChannelManager(), WithRunNS("inner")) // 嵌套层出口：检查读挂住、读预算用完
		cp.loadHang.Store(false)                                     // 读恢复
		other, _ := cp.InMemoryCheckpointer.LoadNS(context.Background(), sid, "")
		if other == nil {
			return nil, errors.New("前置：槽里应有第一段的档")
		}
		cpy := *other // 另一个执行推进了槽：写成自己的挂起档（换了一代）
		cpy.Epoch = "epoch-of-other"
		cpy.PendingInterrupts = append([]*AskQuery{{InterruptID: "other-question", Question: "那边的用户正在答的问题"}}, cpy.PendingInterrupts...)
		if err := cp.InMemoryCheckpointer.SaveNS(context.Background(), sid, "", &cpy); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("业务终局：%w", ErrRunAborted)
	})
	b.AddEdge(StartNode, "gate")
	b.AddEdge("gate", "work")
	b.AddEdge("work", EndNode)
	g := exitCompile(t, b)
	ie, ok := IsInterrupt(Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid), WithStaleWriteGuard()))
	if !ok {
		t.Fatal("前置：第一段应停在 gate")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		cp.loadHang.Store(true) // 读挂住
		cancel()
	}()
	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid), WithStaleWriteGuard(),
		WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("开始")}))
	if left, _ := cp.InMemoryCheckpointer.LoadNS(context.Background(), sid, ""); left != nil {
		t.Fatalf("清档前的检查没做成时应照常清，槽里还有 epoch=%s 的档：%v", left.Epoch, err)
	}
	if !errors.Is(err, ErrRunAborted) || strings.Contains(fmt.Sprint(err), "purge failed") {
		t.Fatalf("清成了，应只报业务的终局失败：%v", err)
	}
}

// 成功收尾时清档前的检查没做成（读档 panic）：照常清档。曾经不删，旧的挂起档留着——同样的答案再续跑一次（客户端
// 重发），auto-load 捡起它、已经执行过的节点再跑一遍（审计实测）。
func TestExitCheckpoint_SuccessPurgeCheckNotDoneStillCleans(t *testing.T) {
	cp := newNSProbeCP()
	var work atomic.Int32
	const sid = "blind_success"
	b := NewBuilder().WithName("blind_success")
	b.AddNode("gate", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		_, err := Ask(ctx, &AskQuery{Question: "执行吗"})
		return nil, err
	})
	b.AddNode("work", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		work.Add(1)
		cp.loadPanic.Store(true) // 出口清档前的检查读档 panic
		return nil, nil
	})
	b.AddEdge(StartNode, "gate")
	b.AddEdge("gate", "work")
	b.AddEdge("work", EndNode)
	g := exitCompile(t, b)
	ie, ok := IsInterrupt(Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid), WithStaleWriteGuard()))
	if !ok {
		t.Fatal("前置：第一段应停在 gate")
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("执行")}
	if err := Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid), WithStaleWriteGuard(),
		WithResumeAnswers(answers)); err != nil {
		t.Fatalf("应成功收尾：%v", err)
	}
	cp.loadPanic.Store(false)
	if ok, _ := cp.InMemoryCheckpointer.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("检查没做成也应照常清档：旧的挂起档还在")
	}
	_ = Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid), WithStaleWriteGuard(),
		WithResumeAnswers(answers))
	if n := work.Load(); n != 1 {
		t.Fatalf("同样的答案再续跑一次，work 应只执行过 1 次，实际 %d 次（旧挂起档被 auto-load 捡起、节点重放）", n)
	}
}

type panicOnPurgedInfoHandler struct{ slog.Handler }

func (h panicOnPurgedInfoHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "cp subtree purged") {
		panic("log handler bug")
	}
	return h.Handler.Handle(ctx, r)
}

// 成功收尾、清档之后记「cp subtree purged」时日志 handler panic：Run 照常成功、档已清（同 cp 事件钩子，见不变量 1）。
func TestExitCheckpoint_LogHandlerPanicAfterSuccessPurgeKeepsSuccess(t *testing.T) {
	oldLogger, oldW, oldF := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(panicOnPurgedInfoHandler{slog.NewTextHandler(io.Discard, nil)}))
	defer func() {
		slog.SetDefault(oldLogger)
		log.SetOutput(oldW)
		log.SetFlags(oldF)
	}()
	cp := NewInMemoryCheckpointer()
	b := NewBuilder().WithName("log_panic_purged")
	b.AddNode("ask", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		_, err := Ask(ctx, &AskQuery{Question: "批准吗"})
		return nil, err
	})
	b.AddEdge(StartNode, "ask")
	b.AddEdge("ask", EndNode)
	g := exitCompile(t, b)
	const sid = "log_panic_purged"
	ie, ok := IsInterrupt(Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 ask")
	}
	err := Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cp), WithSessionID(sid),
		WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if err != nil {
		t.Fatalf("清档之后日志 handler panic 不该让 Run 报错：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("档应已清掉")
	}
}
