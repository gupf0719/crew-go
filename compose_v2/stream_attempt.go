package compose_v2

import (
	"context"
	"sync"
)

// 交给前端的流在执行作废时的撤回。流是 chunk 事件（[Event] 的 Type 为 "chunk"，Metadata["run_id"] 标它出自哪次模型调用，
// 前端与对端按它把片段拼成一条消息）；撤回是对那个 run_id 发一个 reset_stream 事件。
//
// 不变量：一次执行被取代（重试的下一次尝试、失败 / 被条件边否决之后同一个节点的下一次执行）时，它交出、取代它的那次没再
// 登记的流各撤一次；取代它的那次拿回了说不清对应哪些流的东西时（从存档续跑、命中节点私有态、命中没声明流的步骤记录），
// 一个都不撤——宁可留着作废的，也不删掉被采用的。被取代的那次是暂停（中断、被叫停）结束的不撤：续跑的语义是接着办，进度
// 可能存在框架看不见的地方（业务自研执行体自己的字段、外部存储、远端），框架看不到拿回不等于没有采用（曾经照撤，续跑沿用的
// 草稿在前端消失，审计实测）。代价：节点体续跑时自己重新生成一份（不记步骤记录、直接再调一次模型）的，上一份留在前端——那种
// 写法本身就不对：用户在暂停时看到并批准的是上一份，执行的却是新的一份；记住它（[SaveStepResult] 加 [WithStreams]）续跑时
// 原样拿回，也就不会重复显示。暂停按处置意图判（中断、软取消、陈旧写回、调用方的 ctx 结束了，同 [IsRetryFutile]），后端自己
// 的超时是一次普通的失败。拿回时说得清的照样登记：嵌套调用的完成记录声明了自己交出的流
// （[WithStreams]），拿回时登记回来。
//
// 两层用同一套：一组互相取代的尝试（编排成员的重试、[WithNodeRetry]）用 [StreamAttempts]；同一个节点先后两次执行
// （失败、中断、被否决之后的续跑——可能跨请求）由引擎在节点执行结束时处置，上一次的流记在这个节点执行的私有态里、随存档走。
//
// 登记收在片段的出口上：节点的 EventSink 发出 chunk 时记进发送那一刻 ctx 上的记录（不经编排钩子链——曾经靠挂在链上的记录
// 钩子登记，链没接上某个来源、业务钩子返回别的 ctx 或 panic 时都会漏，审计实测）。用调用方节点的 sink 发片段的执行体
// （业务自研 agent、RemoteAgent）由尝试开始时包在 sink 外面的一层登记。同一个片段因此可能记在两层，撤过的连同外面各层的
// 登记一起去掉（forgetUp），一个 run_id 只撤一次。

// streamRecord 是一段执行（一次尝试、一次嵌套调用）交出的流的 run_id，挂在外层那段的记录下面。
type streamRecord struct {
	mu     sync.Mutex
	parent *streamRecord
	runIDs []string
	seen   map[string]bool
	// uncertain：这段执行（或它里面的一段）拿回了说不清对应哪些流的东西，被它取代的那次交出的流一个都不撤（见本文件开头）
	uncertain bool
}

// markUncertain 给 s 与它外面各层标上 uncertain（s 为 nil 时什么也不做）。
func (s *streamRecord) markUncertain() {
	for ; s != nil; s = s.parent {
		s.mu.Lock()
		s.uncertain = true
		s.mu.Unlock()
	}
}

func (s *streamRecord) isUncertain() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uncertain
}

// noteStateWrite 记下 key 这条节点私有态是 ctx 上所在那段执行写的。
func (cm *ChannelManager) noteStateWrite(ctx context.Context, key string) {
	rec, _ := ctx.Value(streamRecordKey{}).(*streamRecord)
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.nodeStateWriters == nil {
		cm.nodeStateWriters = make(map[string]*streamRecord, 1)
	}
	cm.nodeStateWriters[key] = rec
}

// noteRecall 处置一次节点私有态命中：写它的那段执行跟读它的不在同一条记录链上（另一次尝试写的、从存档恢复的）就是从别的
// 执行拿回来的进度，它对应哪些流框架说不清，读它的那段执行及外面各层标上 uncertain。这次执行自己刚写、自己读的不算——
// 工具账就是同一批里先记后读（曾经一律算拿回，从头再跑的尝试也被当成拿回、作废那次的流一个都不撤，实测）。
func (cm *ChannelManager) noteRecall(ctx context.Context, key string) {
	reader, _ := ctx.Value(streamRecordKey{}).(*streamRecord)
	if reader == nil {
		return
	}
	cm.mu.RLock()
	writer, ok := cm.nodeStateWriters[key]
	cm.mu.RUnlock()
	if ok && writer != nil && onSameChain(writer, reader) {
		return
	}
	reader.markUncertain()
}

// onSameChain 报 a、b 是不是同一条记录链上的（一个是另一个本身或外层）。
func onSameChain(a, b *streamRecord) bool {
	for s := a; s != nil; s = s.parent {
		if s == b {
			return true
		}
	}
	for s := b; s != nil; s = s.parent {
		if s == a {
			return true
		}
	}
	return false
}

// markStreamsUncertain 给 ctx 上所在那段执行及外面各层标上 uncertain。
func markStreamsUncertain(ctx context.Context) {
	if s, _ := ctx.Value(streamRecordKey{}).(*streamRecord); s != nil {
		s.markUncertain()
	}
}

type streamRecordKey struct{}

// withStreamRecord 给一段执行开一个记录，挂在 ctx 上已有的那个下面。
func withStreamRecord(ctx context.Context) (context.Context, *streamRecord) {
	parent, _ := ctx.Value(streamRecordKey{}).(*streamRecord)
	s := &streamRecord{parent: parent}
	return context.WithValue(ctx, streamRecordKey{}, s), s
}

// RecordStreams 把这些流的 run_id 记进 ctx 上所在那段执行（非流式的模型调用、被拿回的嵌套调用当初交出的）；不在任何
// 尝试或嵌套调用里时什么也不做。经节点 EventSink 发出的 chunk 不用调它，发的时候已经记下了。
func RecordStreams(ctx context.Context, runIDs ...string) {
	if s, _ := ctx.Value(streamRecordKey{}).(*streamRecord); s != nil && len(runIDs) > 0 {
		s.add(runIDs...)
	}
}

// ForgetStream 从 ctx 上所在那段执行及外面各层去掉 runID：调用方已经当场撤回了它（如 OnModelEnd 钩子拒掉答复时），
// 所在的尝试作废时不再撤一次。
func ForgetStream(ctx context.Context, runID string) {
	if s, _ := ctx.Value(streamRecordKey{}).(*streamRecord); s != nil {
		s.forgetUp([]string{runID})
	}
}

func (s *streamRecord) add(runIDs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range runIDs {
		if id == "" || s.seen[id] {
			continue
		}
		if s.seen == nil {
			s.seen = map[string]bool{}
		}
		s.seen[id] = true
		s.runIDs = append(s.runIDs, id)
	}
}

func (s *streamRecord) has(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[runID]
}

func (s *streamRecord) remove(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.seen[runID] {
		return
	}
	delete(s.seen, runID)
	for i, id := range s.runIDs {
		if id == runID {
			s.runIDs = append(s.runIDs[:i], s.runIDs[i+1:]...)
			break
		}
	}
}

// forgetUp 从 s 与它外面各层去掉这些 run_id（s 为 nil 时什么也不做）。
func (s *streamRecord) forgetUp(runIDs []string) {
	for ; s != nil; s = s.parent {
		for _, id := range runIDs {
			s.remove(id)
		}
	}
}

func (s *streamRecord) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.runIDs
	s.runIDs, s.seen = nil, nil
	return ids
}

// keep 把这段记下的并进外层那段。
func (s *streamRecord) keep() {
	if ids := s.take(); s.parent != nil && len(ids) > 0 {
		s.parent.add(ids...)
	}
}

// recordChunk 把一个 chunk 事件的 run_id 记进 ctx 上所在那段执行。
func recordChunk(ctx context.Context, ev Event) {
	if ev.Type != "chunk" {
		return
	}
	if id, _ := ev.Metadata["run_id"].(string); id != "" {
		RecordStreams(ctx, id)
	}
}

// recordingSink 包住一段执行开始时 ctx 上的 EventSink：经它发的 chunk 先记进这段的记录再原样转发。那个 sink 可能是调用方
// 节点的（业务自研 agent、RemoteAgent 直接用它），它进节点时就定下了 ctx，记不进这段执行。
type recordingSink struct {
	inner EventSink
	rec   *streamRecord
}

func (s recordingSink) Send(ev Event) {
	if ev.Type == "chunk" {
		if id, _ := ev.Metadata["run_id"].(string); id != "" {
			s.rec.add(id)
		}
	}
	s.inner.Send(ev)
}

// beginStreamRecord 开一段执行的记录，并给 ctx 上的 EventSink 包一层 recordingSink。
func beginStreamRecord(ctx context.Context) (context.Context, *streamRecord) {
	ctx, rec := withStreamRecord(ctx)
	if inner := GetEventSink(ctx); inner != nil {
		ctx = WithEventSink(ctx, recordingSink{inner: inner, rec: rec})
	}
	return ctx, rec
}

// StreamScope 记下一段执行（一次嵌套调用）交出的流：调用方在它结束时取走（[StreamScope.Take]），记进完成记录（[WithStreams]）、
// 登记回自己那一层——这次调用之后被原样拿回时，那些流属于拿回它的那次执行。
type StreamScope struct{ rec *streamRecord }

// BeginStreamScope 开一段执行的流记录，返回带着它的 ctx。
func BeginStreamScope(ctx context.Context) (context.Context, *StreamScope) {
	ctx, rec := beginStreamRecord(ctx)
	return ctx, &StreamScope{rec: rec}
}

// markNodeStreamsPaused 把 nodeKey 这次节点执行记成暂停结束的：节点体成功之后在出边上被叫停（软取消、调用方 ctx 结束了），
// 续跑重跑它时是接着办（见 settleNodeStreams）。
func (cm *ChannelManager) markNodeStreamsPaused(nodeKey string) {
	tv, ok := cm.getNodeState(nodeKey + nodeStreamsSuffix)
	if !ok {
		return
	}
	v, err := unmarshalValue(tv)
	if err != nil {
		return
	}
	ns, ok := v.(nodeExecStreams)
	if !ok || ns.Paused {
		return
	}
	ns.Paused = true
	if tv, err = marshalValue(ns); err == nil {
		cm.putNodeState(nodeKey+nodeStreamsSuffix, tv)
	}
}

// Take 取走这段记下的 run_id（按交出顺序）。
func (s *StreamScope) Take() []string { return s.rec.take() }

// Uncertain 报这段执行里有没有拿回说不清对应哪些流的东西（从存档续跑、命中节点私有态……）：那样的话 Take 交出的不是这段
// 执行最终采用的全部流（续跑之前交出、已提交的那部分不会再登记），完成记录不该声明它（见 [WithStreams]）。
func (s *StreamScope) Uncertain() bool { return s.rec.isUncertain() }

// StreamAttempts 处置一组互相取代的执行尝试交给前端的流（不变量见本文件开头）：
//
//   - 成功的、不会再有下一次的（重试用完、panic）尝试，它记下的并进外层。
//   - 失败的那次，它记下的等取代它的尝试跑完再定：取代它的那次没再登记的撤回（被拿回的嵌套调用会登记回来）；取代它的
//     那次拿回了说不清的东西（从存档续跑、命中节点私有态）时一个都不撤、并进外层；取代它的那次自己也失败了时留到再下一次
//     （它可能在拿回之前就失败了），到有一次尝试成功、或者重试结束（Finish）时才定。
//
// 曾经按「有没有 checkpointer」猜下一次是不是从存档续：有就一律不撤，尝试里直接交出、下一次重新生成的片段（节点体自己
// 发的、不存档的自研执行体发的）留在前端。
//
// 用法：每次尝试 Begin → 跑 → End（panic 时 Abandon），重试结束后 Finish。尝试一次接一次地跑，不并发。
type StreamAttempts struct {
	sink   EventSink // 发 reset_stream 用：这组尝试所在那段执行的 sink
	source string    // reset_stream 的 agent 字段
	outer  *streamRecord

	cur     *streamRecord // 正在跑的这次
	stale   []string      // 被这次取代的尝试交出的
	pending *streamRecord // 上一次失败、还没定的
	carry   []string
}

// NewStreamAttempts 给一组尝试建处置器。ctx 是这组尝试所在那段执行的（取 EventSink 与外层的记录），source 写进撤回事件的
// agent 字段。
func NewStreamAttempts(ctx context.Context, source string) *StreamAttempts {
	outer, _ := ctx.Value(streamRecordKey{}).(*streamRecord)
	return &StreamAttempts{sink: GetEventSink(ctx), source: source, outer: outer}
}

// Begin 开一次尝试，返回带着这次尝试记录的 ctx。上一次失败的尝试记下的等这次跑完再定。
func (a *StreamAttempts) Begin(ctx context.Context) context.Context {
	a.stale = append(a.carry, a.settle(false)...)
	a.carry = nil
	ctx, a.cur = beginStreamRecord(ctx)
	return ctx
}

// End 收下这次尝试的结局（err 为 nil 即成功）。
func (a *StreamAttempts) End(err error) {
	a.voidStale(err)
	if a.cur == nil {
		return
	}
	if err == nil {
		a.cur.keep()
	} else {
		a.pending = a.cur
	}
	a.cur = nil
}

// Abandon 处置 panic 的那次尝试：panic 越过重试往上走，不会再有下一次，当场定。
func (a *StreamAttempts) Abandon() {
	a.voidStale(nil)
	if a.cur != nil {
		a.cur.keep()
	}
	a.cur = nil
}

// Finish 在重试结束之后调：最后那次失败尝试记下的并进外层，没定的撤掉——不会再有尝试拿回它们了（没有存档时最后那次失败也
// 不留可续的档）。
func (a *StreamAttempts) Finish() {
	a.settle(true)
	a.retract(a.carry)
	a.carry = nil
}

// voidStale 处置被这次取代的尝试交出、这次没再登记的流：这次拿回了说不清的东西就留着（并进外层），这次也失败了就留到
// 下一次（carry），否则撤。
func (a *StreamAttempts) voidStale(err error) {
	var gone []string
	for _, id := range a.stale {
		if a.cur == nil || !a.cur.has(id) {
			gone = append(gone, id)
		}
	}
	a.stale = nil
	switch {
	case a.cur != nil && a.cur.isUncertain():
		if a.outer != nil {
			a.outer.add(gone...)
		}
	case err != nil:
		a.carry = gone
	default:
		a.retract(gone)
	}
}

// settle 处置上一次失败的尝试：final 时并进外层，否则交还它记下的（等取代它的跑完再定）。
func (a *StreamAttempts) settle(final bool) (stale []string) {
	if a.pending == nil {
		return nil
	}
	if final {
		a.pending.keep()
	} else {
		stale = a.pending.take()
	}
	a.pending = nil
	return stale
}

// retract 撤回这些流，并从外面各层的记录里去掉。
func (a *StreamAttempts) retract(ids []string) { retractStreams(a.sink, a.source, a.outer, ids) }

// retractStreams 对每个 run_id 经 sink 发一个 reset_stream，并从 outer 及更外层的记录里去掉。
func retractStreams(sink EventSink, source string, outer *streamRecord, ids []string) {
	if len(ids) == 0 {
		return
	}
	if sink != nil {
		for _, id := range ids {
			sink.Send(Event{
				Type:    "reset_stream",
				Content: "attempt discarded and rerun from scratch",
				Metadata: map[string]any{
					"reason": "attempt_discarded",
					"run_id": id,
					"agent":  source,
				},
			})
		}
	}
	outer.forgetUp(ids)
}

// nodeStreams 是步骤记录声明的流（[WithStreams]）的存法。
type nodeStreams []string

// nodeExecStreams 是一次节点执行交出、还没定的流（存在这次节点执行的私有态里，随存档走）：下一次执行同一个节点时据此处置
// （见 settleNodeStreams）。
type nodeExecStreams struct {
	// Own 是这次执行自己交出的
	Own []string `json:"own,omitempty"`
	// Kept 是更早的执行交出、之后某次执行说不清有没有采用而留着的：此后一直留着，不再撤
	Kept []string `json:"kept,omitempty"`
	// Paused 报这次执行是暂停（中断、被叫停）结束的：下一次执行是接着办，不撤 Own
	Paused bool `json:"paused,omitempty"`
}

func init() {
	RegisterValueType[nodeStreams]("compose_v2.nodeStreams")
	RegisterValueType[nodeExecStreams]("compose_v2.nodeExecStreams")
}

const nodeStreamsSuffix = ":streams"

// beginNodeStreams 给一次节点执行开流记录，并读出同一个节点上一次执行留下、还没定的流。
func beginNodeStreams(ctx context.Context, cm *ChannelManager, nodeKey string) (context.Context, *streamRecord, nodeExecStreams) {
	ctx, rec := withStreamRecord(ctx)
	var prev nodeExecStreams
	if tv, ok := cm.getNodeState(nodeKey + nodeStreamsSuffix); ok {
		if v, err := unmarshalValue(tv); err == nil {
			if ns, ok := v.(nodeExecStreams); ok {
				prev = ns
			}
		}
	}
	return ctx, rec, prev
}

// settleNodeStreams 在一次节点执行结束时（不论成败，err 是节点体的结局）处置流：上一次执行自己交出、这次没再登记的撤回——
// 上一次是暂停结束的、或这次拿回了说不清的东西时留着（此后一直留着）；这次交出的连同留着的记进这个节点执行的私有态——这次
// 随后没提交成（失败、中断、被条件边否决）时下一次执行读回，提交成了的话访问序号前进，那一格不再有人读。这次交出的同时并进
// 外层（外层的尝试被取代时一起处置）。
func settleNodeStreams(ctx context.Context, cm *ChannelManager, nodeKey string, rec *streamRecord, prev nodeExecStreams, err error, sink EventSink, source string) {
	ids := rec.take()
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	var gone []string
	for _, id := range prev.Own {
		if !seen[id] {
			gone = append(gone, id)
		}
	}
	kept := prev.Kept
	if prev.Paused || rec.isUncertain() {
		kept = append(kept, gone...)
		if rec.parent != nil {
			rec.parent.add(gone...)
		}
	} else {
		retractStreams(sink, source, rec.parent, gone)
	}
	if rec.parent != nil {
		rec.parent.add(ids...)
	}
	// 暂停按处置意图判（中断、软取消、陈旧写回、调用方的 ctx 结束了），不看错误链上有没有 ctx 错：后端自己的超时（pgx、
	// http.Client{Timeout}）是一次普通的失败，按暂停记的话重新生成时旧片段不撤（审计实测）
	next := nodeExecStreams{Own: ids, Kept: kept, Paused: err != nil && IsRetryFutile(ctx, err)}
	if len(next.Own) == 0 && len(next.Kept) == 0 && len(prev.Own) == 0 && len(prev.Kept) == 0 {
		return
	}
	tv, merr := marshalValue(next)
	if merr != nil {
		return // 类型在本包 init 里注册、只装字符串，走不到这里
	}
	cm.putNodeState(nodeKey+nodeStreamsSuffix, tv)
}

// AttemptDiscarded 报一次以 err 结束的执行，它的进度是不是整个作废了：没有 checkpointer（一点没存下，下一次只能从头再来），
// 或者终局失败（[ErrRunAborted]，存档已清）。成功、或者留下了可续的存档（下一次从存档续）时不算。cper 是这次执行交给
// 引擎的 checkpointer。
func AttemptDiscarded(cper Checkpointer, err error) bool {
	return err != nil && (cper == nil || !LeavesResumableCheckpoint(err))
}
