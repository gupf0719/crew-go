package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"crew-go/internal/panicerr"
)

// Durability 控制 checkpoint 在一次 Run 中的**写入时机**（exit / sync / async 三档）。
//
// 默认 DurabilityExit（= 历史行为：仅 Run 出口 defer 在 error/interrupt 时 SaveNS、成功
// DeleteSubtree）——短任务零额外 I/O、零行为变更。需要**硬崩溃恢复**的长/贵 Run 显式开
// Sync/Async：框架在**每个超步边界**把全量 cp 写到持久后端，进程硬崩（kill -9 / OOM /
// 断电——defer 不跑、状态只在内存）后能从最近超步 Resume，不从头重跑。
//
// **前提**：崩溃恢复价值只在配了**持久** Checkpointer（redis / sql 等，非 InMemory）时兑现——
// InMemory 随进程死，每超步写也救不回。
//
// **同 sid 复用注意（Sync/Async 特有）**：开 Sync/Async 后，硬崩（defer 不跑）或成功但
// cleanupOnSuccess 的 DeleteSubtree 失败，都会在 (sid, graphNS) 留下 Status=Pending 的 cp。
// 下次用**同 sid** 跑（没传 WithFreshRun）会被 auto-load 当"未完成任务"自动 Resume——这正是
// 崩溃恢复要的行为。但若你想用同 sid 跑一个**全新任务**，必须显式 WithFreshRun 或换新 sid，
// 否则会静默续跑上次残留状态（用旧 channel state 起跑）。Exit 档硬崩不留 cp，所以这是
// Sync/Async 把"sid 即任务身份"语义放大后才需注意的点（同 sid 自动续跑残留状态）。
//
// **嵌套子图**：durability 经 ctx 透传，子图自动继承父档（各写自己 ns）；见 WithDurabilityCtx。
type Durability string

const (
	// DurabilityExit（默认）：执行中不写 cp，只在 Run 出口 defer 按 error→SaveNS /
	// success→DeleteSubtree。= crew-go 历史行为，零额外 I/O；硬崩溃 mid-run 丢全部进度。
	DurabilityExit Durability = "exit"

	// DurabilitySync：每超步边界**同步阻塞**写全量 cp（写完才进下一超步）。最可靠，
	// 代价是每步一次后端 I/O 延迟串进执行时间线。
	DurabilitySync Durability = "sync"

	// DurabilityAsync：每超步边界**后台异步**写全量 cp——Run goroutine 同步 snapshot
	// 捕获一致态后交给单 worker 后台 SaveNS，不阻塞下一超步执行（重叠 compute 与 I/O）。
	// 单 worker + FIFO 队列保证 cp_N 先于 cp_N+1 落盘。快 + 扛崩；后台写失败经下次 submit
	// 或 Run 出口 join 浮现并 abort（cp 写失败必须终止 run，不 swallow）。见 asyncCpWriter。
	// **跨 goroutine 共享态有两处**：firstErr（自带 mu）+ cm.cpEpochBaseline（受 cm.mu，
	// 由 guardedSaveNS 在 worker 里更新）——cp 本身仍是不可变快照。
	DurabilityAsync Durability = "async"
)

// durabilityCtxKey 是 durability 跨 Run / 子图透传的私有 ctx key（私有类型防碰撞）。
type durabilityCtxKey struct{}

// WithDurabilityCtx 把 durability 档塞进 ctx——嵌套子图自动继承父档（子图 Run 沿用父
// ctx，读到父 durability，各写自己 ns）。业务一般用 WithDurability RunOption；框架在 Run
// 入口解析出实际档后用本函数 push 给下游子图。
func WithDurabilityCtx(ctx context.Context, d Durability) context.Context {
	return context.WithValue(ctx, durabilityCtxKey{}, d)
}

// GetDurability 从 ctx 读 durability，未设返 ""（caller 兜底默认 Exit）。
func GetDurability(ctx context.Context) Durability {
	if d, ok := ctx.Value(durabilityCtxKey{}).(Durability); ok {
		return d
	}
	return ""
}

// WithDurability 设本次 Run 的 cp 写入时机（RunOption）。优先级高于 ctx 继承；都没设默认 Exit。
func WithDurability(d Durability) RunOption {
	return func(o *runOptions) { o.durability = d }
}

// staleGuardCtxKey 是守卫开关的 ctx 注入键。
type staleGuardCtxKey struct{}

// WithStaleWriteGuardCtx 把守卫开关塞进 ctx——**嵌套子图自动继承**（子图 Run 沿用父
// ctx，各守自己 ns 的槽），也是 `agent_v2` 9 编排下业务唯一的开启方式（它们内部自己
// 拼 RunOption，不透传业务的 option）。
//
// 不透传的后果实测过：父守卫报 stale 说"本次结果未写入"，但子 ns 的 cp 早在父节点
// 执行期间就被本次 Run 覆盖了——父槽留着别人的状态、子槽是我的，**父子 cp 撕裂**。
func WithStaleWriteGuardCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, staleGuardCtxKey{}, true)
}

// GetStaleWriteGuard 从 ctx 读守卫开关，未设返 false。
func GetStaleWriteGuard(ctx context.Context) bool {
	on, _ := ctx.Value(staleGuardCtxKey{}).(bool)
	return on
}

// resolveDurability 定 Run 实际用哪档：RunOption 显式 > 父 ctx 继承 > 默认 Exit。
func resolveDurability(ctx context.Context, optVal Durability) Durability {
	if optVal != "" {
		return optVal
	}
	if d := GetDurability(ctx); d != "" {
		return d
	}
	return DurabilityExit
}

// saveStepCheckpoint 每超步持久化（**仅 Sync 档**调用——Async 走 snapshotStepCheckpoint +
// asyncCpWriter 后台写、不经此函数）——写当前 cm 全量 Snapshot 到 (sid, graphNS) **覆盖同
// key**（不建 cp 历史链）。崩溃恢复用：进程硬崩时盘上有最近超步状态。成功 Run 退出时
// cleanupOnSuccess DeleteSubtree 清掉（同 key 覆盖无残留）。
//
// 写失败返 err——Sync caller abort（cp 写失败传播终止 run，不 swallow）。
//
// **写失败的双写语义（仅 Sync）**：写点返 err 后 Run 命名返回 runErr 非 nil → Run-defer 会
// **再** Snapshot+SaveNS 一次同 key（兜底；对 transient 失败是恢复机会，同 key 幂等无害）。
// 所以一次逻辑写失败可能产生 2 次物理 SaveNS + 2 个 cp.save event（都 Ok=false）——业务
// 用 cp.save event 算 metric 时按 Ok 字段处理，或视作"2 次物理写尝试"（语义上确实两次）。
// **Async 浮现/双写语义不同**（别套用 sync 的固定 2 次）：失败在 worker，经 submit（mid-run
// 失败、frontier 非空 → cleanup defer 仍兜底写）或 closeAndJoin（最后写失败、主循环已
// isEnd → frontier 空 → cleanup 跳过）浮现，物理写次数随浮现路径而变。详见 asyncCpWriter。
//
// **调用前置**：caller 保证 cfg.durability != Exit 且 cfg.checkpointer != nil 且
// cfg.sessionID != ""（见 graph_loop 写点守卫）。
func saveStepCheckpoint(ctx context.Context, g *Graph, cm *ChannelManager, cfg *runOptions) error {
	cp, err := snapshotStepCheckpoint(g, cm)
	if err != nil {
		return err
	}
	return guardedSaveNS(ctx, cfg, cm, cp)
}

// snapshotStepCheckpoint 同步捕获当前超步边界的全量**不可变** cp（snapshot 序列化 channel +
// Status=Pending + GraphID），不写盘。Sync 档接着同步 SaveNS；Async 档把这份 cp 交给后台
// writer——cp 与 cm **完全解耦**，worker 后台写它时 Run goroutine 改 cm 跑下一超步互不影响
// （-race clean 的前提）。解耦靠四项，**前三项是结构保证、第四项靠一条隐式约定**：
//
//  1. channel 值已序列化成 taggedValue bytes；
//  2. frontier / visits 全 fresh copy；
//  3. continue 写点路径无 PendingInterrupts；
//  4. **nodeStates**——`Snapshot` 只拷 map、不深拷 `taggedValue.Data`，安全性依赖
//     「`marshalValue` 每次新分配、从不原地改写或复用 buffer」。这条一破（比如给它加
//     buffer pool），后台 writer 就会读到被后续超步改掉的字节、静默写出撕裂的 cp。
//     完整说明和破坏它的具体动作见 `serialization.go` 里 `Snapshot` 那段注释。
//
// **调用前置**：caller 保证 cfg.durability != Exit（见 graph_loop 写点守卫）。
func snapshotStepCheckpoint(g *Graph, cm *ChannelManager) (*Checkpoint, error) {
	cp, err := cm.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("durability step checkpoint snapshot failed: %w", err)
	}
	cp.Status = CPStatusPending
	cp.GraphID = g.ID() // 同 Run-defer：防 cp 串台（SaveNS 冲突守卫据此 fail-fast）
	return cp, nil
}

// emitCpSaveEvent fire cp.save event（opt-in，emitCpEvent 才发）——Run-defer 与每超步写
// 共用，避免重复。node ID 用 "" 标"非节点 framework 级 event"。
func emitCpSaveEvent(ctx context.Context, cfg *runOptions, cp *Checkpoint, saveStart time.Time, saveErr error) {
	if !cfg.emitCpEvent || cfg.hooks == nil {
		return
	}
	saveAction := &CpSaveAction{
		SessionID:              cfg.sessionID,
		NS:                     cfg.graphNS,
		ChannelsCount:          len(cp.Channels),
		PendingInterruptsCount: len(cp.PendingInterrupts),
		DurationMs:             time.Since(saveStart).Milliseconds(),
		Ok:                     saveErr == nil,
	}
	if saveErr != nil {
		saveAction.Err = saveErr.Error()
	}
	deliverCpEvent(ctx, cfg, Event{
		Type:    CpEventType,
		Content: string(CpOperationSave),
		Metadata: map[string]any{
			"action":   &CpAction{Type: CpOperationSave, Save: saveAction},
			"graph_ns": cfg.graphNS,
		},
	})
}

// emitCpCleanupRejected 在**清理被守卫拒绝**时补发一条 cp.cleanup event（Ok=false）。
//
// 不发的话业务订阅 cp 事件的观测面是瞎的——只能从 Run 返回值捞。save 路径已有同款
// 处理（`guardedSaveNS` 里 checkStale 拒绝后补 emitCpSaveEvent），两条 purge 路径
// 当时漏了，这里补齐对称性。
//
// **形态 B（成功清理被拒）最该被感知**：一个跑成功、无中断、无业务错的 Run 被降级
// 成失败，业务不订阅就完全不知道发生了什么。
func emitCpCleanupRejected(ctx context.Context, cfg *runOptions, reason string, staleErr error) {
	if !cfg.emitCpEvent || cfg.hooks == nil {
		return
	}
	action := &CpCleanupAction{
		SessionID: cfg.sessionID,
		NS:        cfg.graphNS,
		Reason:    reason,
		Ok:        false,
		Err:       staleErr.Error(),
	}
	deliverCpEvent(ctx, cfg, Event{
		Type:    CpEventType,
		Content: string(CpOperationCleanup),
		Metadata: map[string]any{
			"action":   &CpAction{Type: CpOperationCleanup, Cleanup: action},
			"graph_ns": cfg.graphNS,
		},
	})
}

// deleteCpSubtree 删掉 (sid, graphNS) 子树的全部 cp（只做存储读写，事件与日志由 reportCpPurge 发）。
//
// Run 出口清档用（见 purgeExitSlot）：成功收尾（cp 不该留，sid lifetime = 根 Run lifetime）、终局失败
// （ErrRunAborted——**主动删而非跳过写**：单槽覆盖式 cp 下「跳过写」只是不覆盖，先前 HITL 存的档 /
// durability 中途 cp 会幸存且 Status=Pending → auto-load 捡起 → 重跑已成功节点 / 重问已答问题 /
// 子图 livelock，详见 ErrRunAborted godoc）、出口档没写成。三者本质都是「这个槽里的档不该留」，共用一份
// 逻辑避免行为漂移；删除失败的处置由调用方决定（成功路径只记日志，另外两条写进错误）。
//
// **调用前置**：caller 保证 cfg.checkpointer != nil 且 cfg.sessionID != ""。
func deleteCpSubtree(ctx context.Context, cfg *runOptions) (int32, time.Duration, error) {
	start := time.Now()
	deleted, delErr := cfg.checkpointer.DeleteSubtree(ctx, cfg.sessionID, cfg.graphNS)
	return deleted, time.Since(start), delErr
}

// reportCpPurge 发一次出口清档的 cp.cleanup 事件（opt-in）并记日志。reason 是 "success" / "abort"。
func reportCpPurge(ctx context.Context, cfg *runOptions, reason string, deleted int32, duration time.Duration, delErr error) {
	if cfg.emitCpEvent && cfg.hooks != nil {
		action := &CpCleanupAction{
			SessionID:    cfg.sessionID,
			NS:           cfg.graphNS,
			Reason:       reason, // "success" / "abort" —— 业务订阅可区分两种清理动因
			DeletedCount: deleted,
			DurationMs:   duration.Milliseconds(),
			Ok:           delErr == nil,
		}
		if delErr != nil {
			action.Err = delErr.Error()
		}
		// node ID 用 "" 标"非节点 framework 级 event"（同 emitCpSaveEvent）
		deliverCpEvent(ctx, cfg, Event{
			Type:    CpEventType,
			Content: string(CpOperationCleanup),
			Metadata: map[string]any{
				"action":   &CpAction{Type: CpOperationCleanup, Cleanup: action},
				"graph_ns": cfg.graphNS,
			},
		})
	}
	logCpPurge(ctx, cfg, reason, deleted, delErr)
}

// logCpPurge 记出口清档的日志。日志 handler 是业务代码：它的 panic 兜住、不往上冒（见 exit_checkpoint.go 不变量 1）。
func logCpPurge(ctx context.Context, cfg *runOptions, reason string, deleted int32, delErr error) {
	defer recoverObserver(ctx, "compose_v2: Run 出口记日志时 handler panic，已兜住、不影响这次 Run")
	if delErr != nil {
		slog.WarnContext(ctx, "compose_v2: cp DeleteSubtree failed",
			"sid", cfg.sessionID, "ns", cfg.graphNS, "reason", reason, "err", delErr)
	}
	if delErr == nil && deleted > 0 {
		// **无条件日志**：cp.cleanup event 带的信息更全（条数 / 耗时 / 原因），但它是
		// opt-in（`WithEmitCpEvent`），默认不发 ⟹ 默认配置下「谁把我的存档删了」无从回答。
		// 而删除是不可逆的、成功路径又恰恰是「数据凭空消失」的来源——失败侧已经有
		// Warn，成功侧不能是哑的。
		slog.InfoContext(ctx, "compose_v2: cp subtree purged",
			"session", cfg.sessionID, "ns", cfg.graphNS, "reason", reason, "deleted", deleted)
	}
}

// asyncCpWriterBuffer 是后台 writer 提交队列 buffer——允许 Run 领先后台写最多这么多个超步
// （重叠 compute 与 I/O）；满则 submit 阻塞（背压，防快超步 + 慢 I/O 时 cp 无限堆积内存）。
const asyncCpWriterBuffer = 8

// asyncCpWriter 是 DurabilityAsync 的单 worker 后台 cp 写入器。
//
// **有序**：单 worker + FIFO chan 保证 cp_N 先于 cp_N+1 落盘（异步写不能乱序，否则旧状态
// 覆盖新状态）。Run goroutine 同步 snapshot 捕获一致态后 submit 不可变 cp，worker 后台做慢
// I/O 的 SaveNS——不阻塞下一超步执行。
//
// **生命周期** Run 独占（"谁启动谁负责退出"）：进超步循环前 newAsyncCpWriter 启 worker，出口
// defer closeAndJoin **drain 所有 pending 写 + 等 worker 退**。该 join defer 必须在 cleanup
// defer（成功 DeleteSubtree / 失败兜底 SaveNS）**之前**跑——靠 Go defer LIFO（join defer 后
// 注册先执行）——否则 DeleteSubtree 可能先于某 pending 写落盘 → 残留 cp / 顺序错乱。
//
// **并发**：submit 与 closeAndJoin 都只在 Run goroutine 调（超步循环 + 出口 defer 时序串行，
// 无 send-on-closed-channel race）；唯一跨 goroutine 是与 worker——cp 不可变（snapshot 已序列化
// 解耦 cm）+ firstErr 加锁，-race clean。**注意**：emitCpEvent 开时 worker 从后台 goroutine 调
// OnNodeEvent（cp.save event），业务 OnNodeEvent 实现需 thread-safe（async 特有）。
type asyncCpWriter struct {
	tasks    chan *Checkpoint
	ctx      context.Context // Run 的 ctx（worker SaveNS + event 用）——join 在 Run 返回前，ctx 仍有效
	cfg      *runOptions     // checkpointer/sid/ns/hooks——per Run 固定
	cm       *ChannelManager // 世代基线载体（guardedSaveNS 读写）——per Run 固定
	wg       sync.WaitGroup
	mu       sync.Mutex
	firstErr error // 首个后台写失败——submit / closeAndJoin 返它让 Run abort（对齐 put 失败终止）
}

// newAsyncCpWriter 启动单 worker 后台 writer。caller 保证 cfg.checkpointer != nil 且
// cfg.sessionID != ""（同写点守卫）。
func newAsyncCpWriter(ctx context.Context, cfg *runOptions, cm *ChannelManager) *asyncCpWriter {
	w := &asyncCpWriter{
		tasks: make(chan *Checkpoint, asyncCpWriterBuffer),
		ctx:   ctx,
		cfg:   cfg,
		cm:    cm}
	w.wg.Add(1)
	go w.loop()
	return w
}

// loop 是后台 worker：顺序消费队列、逐个 writeOne 写盘。已有 firstErr 后 writeOne 跳过
// SaveNS——但 loop 仍继续 range（drain 让 submit 不阻塞），channel 关闭时干净退出。
func (w *asyncCpWriter) loop() {
	defer w.wg.Done()
	// **worker 猝死检测**（与 agent_v2 的会话历史 worker 同款——那边先做对了、
	// 注释也点了名，这边是漏网的那个）：`writeOne` 的 per-task recover 兜得住业务
	// checkpointer 的 panic，**兜不住 `runtime.Goexit`**（zap.WithFatalHook 下业务
	// SaveNS 里一句 logger.Fatal 就是这形态）——deferred 照跑、goroutine 消失，而
	// `firstErr` 永不置位。之后两种后果都静默：
	//
	//   - 队列没满：submit 照收，全部中途 durability cp **静默蒸发**，Run 报完全成功
	//     ——而 async 档买的就是「硬崩可恢复」，那个保证无声消失了；
	//   - 队列满了：submit 阻塞在死 receiver 上，配了 deadline 的归因成 ctx 超时、
	//     没配的永久卡住。`closeAndJoin` 因 wg 已归零立即返 nil，也不报。
	//
	// 判据是 `drained` 旗（正常 range 结束才置位）——不能靠 firstErr（Goexit 下它恒 nil），
	// 也不能靠队列长度（channel 已被 range 取走的那份在局部变量里）。
	drained := false
	defer func() {
		if r := recover(); r != nil {
			// 理论上到不了（writeOne 自己 recover），留一手防它被改坏
			panicerr.Log(w.ctx, slog.LevelError, "compose_v2: async durability worker 因 panic 退出，后续 cp 写入将静默堆积",
				panicerr.New(r))
			return
		}
		if !drained {
			slog.ErrorContext(w.ctx, "compose_v2: async durability worker 提前退出（非正常收摊）——"+
				"未落盘的 cp 已丢失，后续写入无人消费且不会有警告；"+
				"若你的 Checkpointer 实现里调了 runtime.Goexit / t.Fatal（zap 的 WithFatalHook 是常见来源），那就是原因",
				"session_id", w.cfg.sessionID, "ns", w.cfg.graphNS)
			// 让 Run 出口看得见：firstErr 是 submit / closeAndJoin 的返回源
			w.mu.Lock()
			if w.firstErr == nil {
				w.firstErr = errors.New("compose_v2: async durability worker 异常终止（runtime.Goexit）——" +
					"该 Run 的中途 checkpoint 未全部落盘")
			}
			w.mu.Unlock()
		}
	}()
	for cp := range w.tasks {
		w.writeOne(cp)
	}
	drained = true
}

// writeOne 写单份 cp，**panic-safe**：业务 checkpointer.SaveNS 实现 panic 时 recover
// 转 firstErr（cp 事件钩子的 panic 在 deliverCpEvent 里就兜住了，不算写档失败）。worker 跑在**独立 goroutine**——Run 出口那两道都够不着它，不自己兜则
// panic 逃逸 crash 整进程。sync 档的写在主体里、归 Run 出口那道，async 必须对齐，
// 否则就是一条进程崩溃 regression。（分工见 panic_recovery.go）
//
// **per-task**（非 loop-level）recover：panic 后 loop 继续 drain 剩余 cp（firstErr 已置，后续
// writeOne 走 stop 跳过 SaveNS）——若 recover 在 loop level，worker 会退出，buffer 满时阻塞中的
// submit 因无 receiver 永久死锁。
func (w *asyncCpWriter) writeOne(cp *Checkpoint) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			perr := panicerr.WrapOpaque("async durability writer panic", pe)
			panicerr.Log(w.ctx, slog.LevelError, "compose_v2: async durability 写档 panic，已转成错误", pe)
			w.mu.Lock()
			if w.firstErr == nil {
				w.firstErr = perr
			}
			w.mu.Unlock()
		}
	}()
	w.mu.Lock()
	stop := w.firstErr != nil
	w.mu.Unlock()
	if stop {
		return
	}
	saveErr := guardedSaveNS(w.ctx, w.cfg, w.cm, cp)
	if saveErr != nil {
		w.mu.Lock()
		w.firstErr = saveErr
		w.mu.Unlock()
	}
}

// submit 把同步 snapshot 好的不可变 cp 入队（非阻塞，除非 buffer 满 → 背压）。已有后台写失败
// 时返 firstErr 让 Run abort、不再入队。只在 Run goroutine 调。
func (w *asyncCpWriter) submit(cp *Checkpoint) error {
	w.mu.Lock()
	priorErr := w.firstErr
	w.mu.Unlock()
	if priorErr != nil {
		return priorErr
	}
	// buffer 满阻塞 send 加 ctx 逃逸：业务 backend SaveNS 慢/卡时 worker 来不及消费、buffer
	// 填满、submit 阻塞；ctx 取消时主动从 submit 逃出返 ctx.Err()，让 Run 不卡在入队这步、
	// 能回到循环顶的 ctx.Err() 检查。**注意**：这只解 submit；出口 closeAndJoin 仍同步
	// wg.Wait 等 worker drain（保 cp 落盘顺序的 join-before-cleanup 不变量）——业务 backend
	// 必须尊重 ctx，SaveNS 完全忽略 ctx 且永不返回时该 join 仍会阻塞（属 backend 违约；框架
	// 靠 mandated ctx deadline + ctx-respecting backend 兜底，不在此处提前逃逸破坏写序）。
	select {
	case w.tasks <- cp:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

// closeAndJoin 关队列 + 等 worker drain 完所有 pending 写并退出，返首个写错误。Run 出口 defer
// 调（cleanup defer 之前）——保证所有 per-superstep 写落盘后才 DeleteSubtree / 兜底 SaveNS。
// 只在 Run goroutine 调一次（defer 单次，与 submit 串行，无 double-close / send-on-closed race）。
func (w *asyncCpWriter) closeAndJoin() error {
	close(w.tasks)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstErr
}

// checkStale 判本 Run 的世代基线是否仍与槽里一致——**写和删共用**。
//
// 返非 nil 即「这个槽已经不归本次 Run 负责了」，caller 应放弃本次操作。
//
// 判据只有一条——**「我载入时看到的那一代，现在还在吗」**，写和删一视同仁：
//   - 守卫未开 ⟹ 恒 nil（opt-in，默认零成本）
//   - **有**基线：拿槽里当前 cp 比世代，不同就拒。`cur == nil` 也算冲突——基线非空
//     证明载入时槽里有东西、现在没了 ⟹ 有人删过，那正是「被推进」的一种形态（且删比
//     覆盖更具破坏性）
//   - **无**基线：本次比不了 ⟹ 放行。注意「载入时槽是空的」**不算**无基线——那种情况
//     下「槽变非空」本身就是冲突信号，照样走比对（守卫能不能保护会话第一轮全看这个区分）
//
// ⚠️ 无基线时曾对**删除**单独拒绝过，已撤——那条极性假设「这个槽可能不归我」，而
// `RunSession.RunID` 的契约（一枚 sid 只服务一个任务）把它定死成「归我」。理由与实测
// 见函数体里那段和 `TestStaleWriteGuard_NoBaselineDoesNotBlockCleanup`。
//
// **非原子**：读与写/删之间有窗口，两个执行者可能都通过。见 WithStaleWriteGuard。
func checkStale(ctx context.Context, cfg *runOptions, cm *ChannelManager, op string) error {
	need, note := staleProbe(cfg, cm, op)
	if note != nil {
		note(ctx)
	}
	if !need {
		return nil
	}
	cur, curErr := cfg.checkpointer.LoadNS(ctx, cfg.sessionID, cfg.graphNS)
	staleErr, note := judgeStale(cfg, cm, op, cur, curErr, "")
	if note != nil {
		note(ctx)
	}
	return staleErr
}

// staleProbe 判陈旧写回检查要不要读当前档：没开守卫、或本 Run 没有基线可比时不读（放行）。note 非 nil 时是
// 要记的日志，由调用方决定什么时候记（Run 出口要等结论定下之后，见 exit_checkpoint.go 不变量 1）。
func staleProbe(cfg *runOptions, cm *ChannelManager, op string) (needLoad bool, note func(context.Context)) {
	if !cfg.staleWriteGuard {
		return false, nil
	}
	if cm.CpEpochBaseline() == "" && !cm.CpSlotWasEmpty() {
		// **没有基线 ⟹ 本次比不了，写和删都放行。**
		//
		// 没基线 = 本 Run 既没载过这个槽、也没往里写过。这在正常路径上是**常态不是异常**：
		// `agent_v2` 的新任务判定让每个顶层 Run 恒 `WithFreshRun`（任务隔离
		// 要的），而 fresh run 刻意不载入。
		//
		// ⚠️ **这里曾经对删除单独拒绝过（「没凭据就别动别人的东西」），已撤**——那条
		// 极性建立在「这个槽可能不归我」上，而 `RunSession.RunID` 的契约把这件事定死了：
		// **一枚 sid 只服务一个任务，槽就是归本次 Run 的**。于是那道拒绝拒掉的全是合法
		// 调用：实测开了守卫之后，同一个 sess 上但凡有过一次被放弃的中断，此后**每一个
		// 完全跑成功的任务**都返 `ErrCheckpointStale`，而且不自愈。
		//
		// 撤掉它还有两条独立理由：
		//   - **它在自己声称的用例上就不成立**。它要防的是「别抹掉另一个执行者正等着的
		//     挂起存档」，可同一处境下的**写**是 fail-open 的——实测守卫开着时，A 的挂起
		//     提问照样被 B 的写覆盖掉。拦住删、拦不住写，保不了任何东西。
		//   - **它是「你的意图 vs 我发现的状态」型守卫**。调用方要让它不报错，得先知道
		//     「这个 sid 底下有没有别人的挂起档」——那是框架的私有知识，而它给的出路
		//     （换 sid / 别开守卫）调用方在开守卫那一刻并不知道该走哪条。
		//
		// 剩下的暴露面是「两个任务共用一枚 sid」，那是**违反契约**，框架分辨不出来
		// （合法的崩溃恢复长得一模一样）。真解在契约那一侧：`sess.NextTask()`。
		//
		// **注意这里不含「载入时槽是空的」**——那种情况下「槽变非空」本身就是冲突信号，
		// 走下面的比对分支（守卫能不能保护会话第一轮，全看这个区分）。
		if cfg.checkpointer == nil || cfg.sessionID == "" {
			return false, nil
		}
		return false, func(ctx context.Context) {
			slog.DebugContext(ctx, "compose_v2: stale-write guard 无基线可比，本次跳过检测",
				"session", cfg.sessionID, "ns", cfg.graphNS, "op", op,
				"hint", "WithFreshRun 属正常；若是 Resume 则可能是旧 cp 无 Epoch 或 backend 漏序列化该字段")
		}
	}
	return true, nil
}

// judgeStale 判读到的当前档（cur、curErr 是那次 LoadNS 的结果）是不是被别的执行推进过。ownEpoch 非空时，槽里
// 是这一代就认作自己写的：Run 出口写档结果不明（宽限到点，可能已经写进去）之后清档前的检查用它，不然自己刚写的
// 档会被当成别人写的。note 同 staleProbe。
func judgeStale(cfg *runOptions, cm *ChannelManager, op string, cur *Checkpoint, curErr error, ownEpoch string) (staleErr error, note func(context.Context)) {
	if curErr != nil {
		// **fail-open 并 log**：读不到当前状态不代表有冲突。守卫是护栏不是主链路，
		// 因一次读抖动把正常 Run 拦下来（HITL 存档直接丢）比放行更糟。
		return nil, func(ctx context.Context) {
			slog.WarnContext(ctx, "compose_v2: stale-write guard 读当前 cp 失败，放行本次操作",
				"session", cfg.sessionID, "ns", cfg.graphNS, "op", op, "err", curErr)
		}
	}
	baseline := cm.CpEpochBaseline()
	slotWasEmpty := cm.CpSlotWasEmpty()
	if cur == nil {
		if slotWasEmpty {
			return nil, nil // 载入时空、现在还空——本 Run 是这个槽的第一个写入者，正常
		}
		if cm.CpSlotUnknown() {
			// 探测槽状态时读失败过，"载入时它空不空"没问出来 ⟹ 不能断言是别人删的。
			// 同 fail-open 理由：一次读抖动就拦掉写回，会让 fork 出的会话存档不落盘
			// 且永久不可 Resume——比放行糟得多。
			return nil, func(ctx context.Context) {
				slog.WarnContext(ctx, "compose_v2: stale-write guard 槽为空但载入时状态未知，放行",
					"session", cfg.sessionID, "ns", cfg.graphNS, "op", op)
			}
		}
		return &StaleWriteError{
			Op: op, SessionID: cfg.sessionID, NS: cfg.graphNS, Baseline: baseline,
			Detail: "槽已空 — 另一个执行者删掉了它（跑完清理或终局失败清子树）",
		}, nil
	}
	if ownEpoch != "" && cur.Epoch == ownEpoch {
		return nil, nil // 槽里是这次出口刚尝试写的那一代：写进去了，是自己的
	}
	if slotWasEmpty {
		// 我载入时槽是空的，现在有东西了 ⟹ 在我执行期间有人写进来了。
		// **这一条守的是"会话第一轮"**：两个副本同时收到同一个新 sid 时，双方基线
		// 都空，靠比对 Epoch 谁也发现不了对方。
		return &StaleWriteError{
			Op: op, SessionID: cfg.sessionID, NS: cfg.graphNS, Current: cur.Epoch,
			Detail: "载入时槽为空，现已被写入 — 另一个执行者在本次 Run 期间创建了它；" +
				"本次结果未落盘，副作用可能已重复发生",
		}, nil
	}
	if cur.Epoch != "" && cur.Epoch != baseline {
		return &StaleWriteError{
			Op: op, SessionID: cfg.sessionID, NS: cfg.graphNS,
			Baseline: baseline, Current: cur.Epoch,
			Detail: "槽已被推进 — 另一个执行者在本次 Run 期间写过；" +
				"本次结果未落盘，副作用可能已重复发生",
		}, nil
	}
	return nil, nil
}

// guardedSaveNS（与它的写入那一半 writeCp）是**所有** cp 写入的统一入口——负责世代标记的判-换-写-记。
//
// **每个写点都要判，不只出口**：Durability=Sync/Async 每超步都写，中途那些写同样
// 会覆盖别人的 cp。只在出口判的话，最后一次超步写已经把别人的成果盖掉了，出口那次
// 变成"自己跟自己比"恒通过——守卫形同虚设。
//
// 写完（含被守卫拒写）发 cp.save event。Run 出口不用它：检查与写各是一次出口存储调用（exitStaleCheck + writeCp），
// 结论定下之后再发事件（见 exit_checkpoint.go 不变量 1、2）。
func guardedSaveNS(ctx context.Context, cfg *runOptions, cm *ChannelManager, cp *Checkpoint) error {
	if err := checkStale(ctx, cfg, cm, "save"); err != nil {
		// **被守卫拒写也要发 cp.save event**：CpSaveAction 的 Ok/Err 正是为"写失败"
		// 设的，而"因陈旧被拒"是最该让业务感知的一种写失败。不发的话业务只能从 Run
		// 返回值捞，订阅 cp 事件的观测面是瞎的。
		emitCpSaveEvent(ctx, cfg, cp, time.Now(), err)
		return err
	}
	saveStart, err := writeCp(ctx, cfg, cm, cp)
	emitCpSaveEvent(ctx, cfg, cp, saveStart, err)
	return err
}

// writeCp 是 guardedSaveNS 的写入那一半：mint 世代 + SaveNS + 更新基线，不做陈旧写回检查、不发事件。返回写入
// 开始的时刻（发事件算耗时用）。cp.Epoch 在调用 SaveNS 之前就铸好了：写档结果不明时，槽里是不是这一代说明它
// 写没写进去（见 judgeStale 的 ownEpoch）。
func writeCp(ctx context.Context, cfg *runOptions, cm *ChannelManager, cp *Checkpoint) (time.Time, error) {
	cp.Epoch = newEpoch()
	// **任务代号原样带过去，不重铸**——它标的是「哪一次任务」，而 Epoch 标的是
	// 「第几次写」。混淆两者会让中断 ID 在同一次 Run 内跨超步漂（sync 档每超步写一次）。
	if cp.TaskID == "" {
		cp.TaskID = cm.TaskID()
	}
	// 时间戳跟世代标记一起盖。`Cleanup`（保留最近 N 个会话）按 CreatedAt 排序，
	// 而 `Snapshot` 产的 cp **从不带时间**——不在这盖的话它在框架路径下退化成排序
	// 键恒等，落到 sid 字典序 tie-break；而 sid 通常是 `RunSession.RunID`（UUIDv4），
	// 跟时间毫无关系。实测 `Cleanup(2)` 保留的是最先创建的两个、删掉最新三个——GC 语义整个
	// 反过来，业务会删掉正在跑的新会话、留下早就死掉的旧会话。
	//
	// **只在 caller 没给时盖**：conformance 靠显式 CreatedAt 验 backend 的排序
	// 实现（它故意让 sid 字典序反着来防假绿），业务想自己控时间线的也照旧生效。
	// 单槽覆盖式下每次 SaveNS 传的都是新 Snapshot（恒 0），所以框架路径上它实际
	// 记的是"最后一次写入"——这正是那两个 API 想要的（GC 该删最久不动的，不是最
	// 早创建的）。
	if cp.CreatedAt == 0 {
		cp.CreatedAt = time.Now().UnixNano()
	}
	saveStart := time.Now()
	saveErr := cfg.checkpointer.SaveNS(ctx, cfg.sessionID, cfg.graphNS, cp)
	if saveErr == nil {
		cm.setCpEpochBaseline(cp.Epoch) // 我写的这代成为新基线
	}
	return saveStart, saveErr
}
