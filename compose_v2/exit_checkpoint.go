package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"crew-go/internal/panicerr"
)

// 出口记账：Run 退出时写档、清档。三条不变量：
//
//  1. 结论只看存储调用的结果。档写成了照原样报告（中断照报），没写成报 [ErrCheckpointNotPersisted]。存储调用
//     之外的代码——cp 事件（业务的 OnNodeEvent 钩子）、日志（handler 也是业务代码）——都在结论赋给 runErr、
//     该清的档清完之后才调，而且只作观测：它们的 panic 当场兜住、记一条带栈的日志，不进返回的错误，不改变
//     写没写成、清没清（见 deliverCpEvent、flushNote）。成功收尾时发 cp.cleanup 的那一刻档已经清掉，让 Run
//     因此报错只会引来从头重跑、副作用重放。曾经写档成功之后 cp.save 事件的钩子 panic 被当成写档失败、刚写好
//     的档被清掉；陈旧写回检查里的告警日志 panic 也能让写档不做、结论变成终局失败；后来改成追加到错误上，成功
//     收尾的 Run 因此报错。
//  2. 调用方取消或超时不截断出口的存储调用：每次调用用不跟着取消的 ctx，调用方结束之后再给一段宽限；写档只
//     发一次。陈旧写回检查（开了守卫才有）跟写档、清档各是一次调用、各有各的宽限。检查拿不到结论（读报错、
//     宽限到点、预算用完没发、读档 panic）时写档、清档都放行：这道检查是尽力型的（先读后写、不是原子的），
//     拦下的代价落在合法调用上——拦写档丢自己的档，拦清档留下旧的挂起档、同样的答案再续跑一次会重放节点（曾经
//     清档不删，审计实测）；放行的代价只在同一个 sid 上并发跑两个任务（违反 RunID 一任务一枚的契约）时出现。
//     写档结果不明（宽限到点，可能已经写进去）时，
//     清档前的检查把这次写的那一代认作自己的——只认得出检查之前已经落地的那次提交：后端在客户端放弃之后、清档
//     之后才提交时（非事务后端的固有竞态），槽里会留下这次写的档，而结论是档没写成。曾经写到一半调用方到点就
//     用宽限再写一次，第二次的检查把自己刚写的档当成别人写的；检查与写档共用一段宽限时，慢但每次都在宽限内的
//     后端照样写不进去。
//  3. 宽限从每次调用自己的起点算，不会被更早的事件预先耗掉。调用方放弃（顶层 Run 的 ctx 结束）之后，每一类
//     调用（读 / 写 / 删）失败所占的墙钟时间——几个调用重叠的部分只算一次，调用方放弃之前的部分不算——累计
//     不超过一个宽限：用完之后，自己的 ctx 已经结束的同类调用不再发；没用完时，调用最多等剩下的那部分。成功的
//     调用、自己的 ctx 一直活着的调用（例如用 WithoutCancel 跑的嵌套 Run）不计也不拦。于是调用方放弃之后 Run
//     在出口多等的时间有上界（每一类最多约两个宽限：用完时还在途的调用等完它开始时剩下的那部分），跟嵌套层数、
//     调用次数无关；后端慢但每次都在宽限内完成时照常写完、清完。
//     曾经整个任务共用一个截止时间：内层自己超时那次就把它定下了，之后调用方取消时外层出口已经没有宽限，健康
//     的后端也写不进去；改成「等满过一次宽限才不再等」之后，后端自己很快报错（客户端超时比宽限短）时永远等不
//     满，每一层的清档都再等一次；改成按每次失败各自的耗时相加之后，并行分支同时失败时同一段墙钟被记了几遍、
//     横跨放弃时刻的调用连放弃之前的部分也记上，预算提前用完，该清的旧档没清、该写的档没写（都是审计实测）。
//
// 宽限只对尊重 ctx 的后端生效：不看 ctx 的后端挂住时，调用照样不返回。

// exitBookkeepingGrace 是出口存储调用的宽限：调用方 ctx 结束之后，一次调用最多再等多久；也是调用方放弃之后
// 每一类调用失败所花时间的总预算（见不变量 3）。
//
// 有上限是为了后端挂住时 Run 照样能返回（database/sql 默认不设语句超时）；10 秒对一次存档读写
// 足够宽，健康的后端是毫秒级。是变量只为白盒测试能调小，生产代码不改它。
var exitBookkeepingGrace = 10 * time.Second

// exitOpKind 是出口存储调用的类别：失败预算按类别分开记（见不变量 3）。写档带着整份档、清档是一条小语句、
// 读挂住不代表写也挂住——分开记，一类挂住不连累别的类。
type exitOpKind int

const (
	// exitOpLocal 不碰存储后端（快照）：只兜 panic，不计预算。
	exitOpLocal exitOpKind = iota
	// exitOpRead 陈旧写回检查读当前档。
	exitOpRead
	// exitOpWrite 写档。
	exitOpWrite
	// exitOpDelete 清档。
	exitOpDelete
	exitOpKinds
)

// exitGrace 是一个任务的出口存储调用共用的状态：顶层 Run 放，嵌套的各层沿用，脱离执行时剥掉（独立任务各算
// 各的）。
type exitGrace struct {
	// callerDone 是顶层 Run 入口 ctx 的 Done：关了就是调用方放弃了。只有放弃之后的失败才计预算——某一层自己
	// 超时（调用方还在等）不算。
	callerDone <-chan struct{}

	mu sync.Mutex
	// goneAt 是调用方放弃的时刻（顶层 Run 入口挂的 AfterFunc 记下）。
	goneAt time.Time
	// failed 按类别记调用方放弃之后失败的调用占的时段，预算按它们的墙钟并集算。
	failed [exitOpKinds][]exitSpan
}

// exitSpan 是一次失败调用在调用方放弃之后占的时段。
type exitSpan struct{ from, to time.Time }

// exitGraceKey 是 exitGrace 在 ctx 上的键（执行状态）。
type exitGraceKey struct{}

// withExitGrace 在 Run 入口调：ctx 上已有（外层 Run 放的）就沿用，没有就放一份新的——这一层就是任务的顶层，
// 它的 ctx 结束就是调用方放弃了，放弃的时刻由挂在它上面的 AfterFunc 记下。返回的 release 解开那个 AfterFunc，
// Run 返回时调（嵌套的各层拿到的是空操作）。
func withExitGrace(ctx context.Context) (context.Context, func()) {
	if _, ok := ctx.Value(exitGraceKey{}).(*exitGrace); ok {
		return ctx, func() {}
	}
	g := &exitGrace{callerDone: ctx.Done()}
	stop := context.AfterFunc(ctx, g.markGone)
	return context.WithValue(ctx, exitGraceKey{}, g), func() { stop() }
}

// markGone 记下调用方放弃的时刻（只记第一次）。
func (g *exitGrace) markGone() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.goneAt.IsZero() {
		g.goneAt = time.Now()
	}
}

// callerGone 报调用方是不是已经放弃了（顶层 Run 的 ctx 结束了）。
func (g *exitGrace) callerGone() bool {
	if g.callerDone == nil {
		return false
	}
	select {
	case <-g.callerDone:
		return true
	default:
		return false
	}
}

// goneSince 返回调用方放弃的时刻（还没放弃时为零值）。AfterFunc 还没来得及跑时取现在：略晚于真实时刻，少计不多计。
func (g *exitGrace) goneSince() time.Time {
	if !g.callerGone() {
		return time.Time{}
	}
	g.markGone()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.goneAt
}

// remaining 返回这一类调用的失败预算还剩多少（grace 是这次调用开始时读到的宽限）。
func (g *exitGrace) remaining(kind exitOpKind, grace time.Duration) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	return grace - spanUnion(g.failed[kind])
}

// charge 把一次失败调用在调用方放弃之后占的时段记进这一类的预算。
func (g *exitGrace) charge(kind exitOpKind, from, to time.Time) {
	if !to.After(from) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failed[kind] = append(g.failed[kind], exitSpan{from: from, to: to})
}

// spanUnion 返回几个时段的墙钟并集长度（重叠的部分只算一次）。
func spanUnion(spans []exitSpan) time.Duration {
	if len(spans) == 0 {
		return 0
	}
	sorted := append([]exitSpan(nil), spans...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].from.Before(sorted[j].from) })
	var total time.Duration
	cur := sorted[0]
	for _, sp := range sorted[1:] {
		if sp.from.After(cur.to) {
			total += cur.to.Sub(cur.from)
			cur = sp
			continue
		}
		if sp.to.After(cur.to) {
			cur.to = sp.to
		}
	}
	return total + cur.to.Sub(cur.from)
}

// errExitGraceExpired 挂在宽限到点（或预算用完、不再发）的那次出口存储调用返回的错误上，算一次超时
// （errors.Is(_, context.DeadlineExceeded) 为真）。
var errExitGraceExpired error = exitGraceExpiredError{}

type exitGraceExpiredError struct{}

func (exitGraceExpiredError) Error() string { return "compose_v2: exit bookkeeping grace expired" }

// Is 让它算一次超时。
func (exitGraceExpiredError) Is(target error) bool { return target == context.DeadlineExceeded }

// exitStorageOp 在出口做一次存储调用。op 里只放存储调用本身（不调钩子、不记日志，见不变量 1）：ctx 不跟着
// 调用方取消，调用方结束之后再等一段宽限；调用方放弃之后按类别计失败预算（不变量 2、3）。op 本身 panic 转成
// 错误，记一条带栈的日志（panicerr.Log 在自己的 goroutine 里调 handler，handler panic 掐不断这里）。
func exitStorageOp(ctx context.Context, kind exitOpKind, what string, op func(context.Context) error) (err error) {
	g, _ := ctx.Value(exitGraceKey{}).(*exitGrace)
	if g == nil {
		g = &exitGrace{}
	}
	grace := exitBookkeepingGrace
	budgeted := kind != exitOpLocal
	// ctx 还活着的调用不拦：它不等宽限（调用方结束不会截断它），拦下只会丢档
	if budgeted && ctx.Err() != nil && g.callerGone() && g.remaining(kind, grace) <= 0 {
		return fmt.Errorf("compose_v2: %s not attempted: %w (calls of this kind kept failing after the caller's context ended)",
			what, errExitGraceExpired)
	}
	opCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	var (
		mu       sync.Mutex
		timer    *time.Timer
		finished bool
		armedAt  time.Time     // 开始计宽限的时刻（ctx 结束的时刻）
		waitFor  time.Duration // 这次实际给的宽限
	)
	// ctx 结束时开始计宽限（已经结束的话立刻开始）。回调在自己的 goroutine 里跑，可能在本函数返回之后才拿到锁：
	// 那时 finished 已置位，它直接返回。
	stopWatch := context.AfterFunc(ctx, func() {
		wait := grace
		if budgeted && g.callerGone() {
			if r := g.remaining(kind, grace); r < wait {
				wait = max(r, 0)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if finished {
			return
		}
		armedAt, waitFor = time.Now(), wait
		timer = time.AfterFunc(wait, func() { cancel(errExitGraceExpired) })
	})
	defer func() {
		stopWatch()
		mu.Lock()
		finished = true
		if timer != nil {
			timer.Stop()
		}
		armed, waited := armedAt, waitFor
		mu.Unlock()
		if err != nil && budgeted && !armed.IsZero() {
			// 只计调用方放弃之后的那段：内层自己超时之后、调用方放弃之前等的不算
			if gone := g.goneSince(); !gone.IsZero() {
				g.charge(kind, maxTime(armed, gone), time.Now())
			}
		}
		if err != nil && errors.Is(context.Cause(opCtx), errExitGraceExpired) {
			err = fmt.Errorf("%w (%w: waited %v after the caller's context ended)", err, errExitGraceExpired, waited)
		}
		cancel(nil)
	}()
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicerr.Log(ctx, slog.LevelError, "compose_v2: Run 出口的存储调用 panic，已转成错误", pe)
			err = panicerr.WrapOpaque("compose_v2: "+what+" panicked", pe)
		}
	}()
	return op(opCtx)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// exitNotes 收出口存储调用期间要记的日志，结论定下之后再记（见不变量 1）。
type exitNotes []func(context.Context)

func (n *exitNotes) add(f func(context.Context)) {
	if f != nil {
		*n = append(*n, f)
	}
}

// flush 按顺序记下收着的日志。日志 handler 是业务代码：它的 panic 每条当场兜住、记一条带栈的日志，不往上冒
// （见不变量 1）。
func (n exitNotes) flush(ctx context.Context) {
	for _, f := range n {
		flushNote(ctx, f)
	}
}

func flushNote(ctx context.Context, f func(context.Context)) {
	defer recoverObserver(ctx, "compose_v2: Run 出口记日志时 handler panic，已兜住、不影响这次 Run")
	f(ctx)
}

// exitStaleCheck 是 Run 出口的陈旧写回检查（开了守卫才有）：读当前档是一次 read 类出口存储调用，判断在读之外，
// 要记的日志收进 notes。读报错（含宽限到点）按读失败放行（judgeStale）；检查没做成（预算用完没发、读档 panic）
// 时返回 notDone，调用方记一条告警、照常写档或清档（见不变量 2）。ownEpoch 非空时，槽里是这一代就认作
// 自己写的（见 judgeStale）。
func exitStaleCheck(ctx context.Context, cfg *runOptions, cm *ChannelManager, op, ownEpoch string, notes *exitNotes) (staleErr, notDone error) {
	need, note := staleProbe(cfg, cm, op)
	notes.add(note)
	if !need {
		return nil, nil
	}
	var cur *Checkpoint
	var loadErr error
	loaded := false
	err := exitStorageOp(ctx, exitOpRead, "checkpoint stale check", func(c context.Context) error {
		cur, loadErr = cfg.checkpointer.LoadNS(c, cfg.sessionID, cfg.graphNS)
		loaded = true
		return loadErr
	})
	if err != nil && !loaded {
		return nil, err // 没发、或 LoadNS panic 了
	}
	if err != nil {
		cur, loadErr = nil, err // 读报错（带着宽限到点的标注）
	}
	staleErr, note = judgeStale(cfg, cm, op, cur, loadErr, ownEpoch)
	notes.add(note)
	return staleErr, nil
}

// attachPanicOf 在 src 带着兜住的 panic（[PanicError]）时把它并列挂到 err 上。src 只以文本进 err 的地方用：
// 不让 src 整条进链（它可能带着后端返回的 ctx 错），panic 的记录仍能 errors.As 取到。
func attachPanicOf(err, src error) error {
	var pe *PanicError
	if src == nil || !errors.As(src, &pe) {
		return err
	}
	return panicerr.Attach(err, pe)
}

// exitPurge 是出口清档（成功收尾、终局失败、档没写成）的结果。先做存储调用、把结果记在这里，结论定下之后再
// report 记日志、发事件（不变量 1）。
type exitPurge struct {
	reason string
	// notes 是清档前那次陈旧写回检查要记的日志。
	notes exitNotes
	// staleErr 是陈旧写回检查发现槽已被别的执行推进：这时不删（别人的档）。
	staleErr error
	// deleted 为真时做了删除：删掉的条数、耗时、删除失败的原因。
	deleted  bool
	count    int32
	duration time.Duration
	delErr   error
}

// failure 返回「旧档可能还在」的原因（删除失败、预算用完没删、后端 panic）；没有就返 nil。槽被别人推进
// （staleErr）不算。
func (p *exitPurge) failure() error {
	return p.delErr
}

// purgeExitSlot 清掉这一层的存档子树（只做存储调用）：先做陈旧写回检查（staleOp 进 StaleWriteError.Op），槽被
// 别的执行推进时不删。ownEpoch 是这次出口刚尝试写的那一代（写档结果不明时，槽里可能就是它），没有就传空。
// 调用前置：cfg.checkpointer != nil 且 cfg.sessionID != ""。
func purgeExitSlot(ctx context.Context, cfg *runOptions, cm *ChannelManager, reason, staleOp, ownEpoch string) *exitPurge {
	p := &exitPurge{reason: reason}
	staleErr, notDone := exitStaleCheck(ctx, cfg, cm, staleOp, ownEpoch, &p.notes)
	if staleErr != nil {
		p.staleErr = staleErr
		return p
	}
	if notDone != nil {
		// 检查没做成：照常清（见不变量 2），记一条告警
		p.notes.add(func(c context.Context) {
			slog.WarnContext(c, "compose_v2: 出口清档前的陈旧写回检查没做成，照常清档",
				"sid", cfg.sessionID, "ns", cfg.graphNS, "reason", reason, "err", notDone)
		})
	}
	p.deleted = true
	p.delErr = exitStorageOp(ctx, exitOpDelete, "checkpoint purge", func(c context.Context) error {
		var err error
		p.count, p.duration, err = deleteCpSubtree(c, cfg)
		return err
	})
	return p
}

// report 记清档前检查的日志，发清档的 cp 事件、记清档日志。在结论定下之后调（见 exitPurge）。
func (p *exitPurge) report(ctx context.Context, cfg *runOptions) {
	p.notes.flush(ctx)
	switch {
	case p.staleErr != nil:
		emitCpCleanupRejected(ctx, cfg, p.reason, p.staleErr)
	case p.deleted:
		reportCpPurge(ctx, cfg, p.reason, p.count, p.duration, p.delErr)
	}
}

// newCheckpointNotPersisted 造「出口要写的档没写成」的结论（见 [ErrCheckpointNotPersisted]）。exitErr 是这次
// 退出原本的错误，cause 是写档（或快照）失败的原因。
//
// 挂起类的退出（[shouldStayPending] 为真：中断、软取消、ctx 错……）不把原错误留在错误链上——它在说
// 「可以续」，档却没写成。业务错留着。槽不属于别人时带 ErrRunAborted，调用方接着 purgeExitSlot 清这一层
// 的存档子树、把结果记进来（recordPurge）。
func newCheckpointNotPersisted(exitErr, cause error) *checkpointNotPersistedError {
	return &checkpointNotPersistedError{
		exit:     exitErr,
		keepExit: !shouldStayPending(exitErr),
		cause:    cause,
		// 槽属于别人（别的执行推进了它 / 它是另一张图的档）时不删：那份档不是这次执行的。
		aborted: !errors.Is(cause, ErrCheckpointStale) && !errors.Is(cause, ErrGraphIDConflict),
	}
}

// recordPurge 把出口清档的结果记进结论。
func (e *checkpointNotPersistedError) recordPurge(p *exitPurge) {
	e.purgeSkipped = p.staleErr
	e.purgeErr = p.failure()
}
