package compose_v2

import (
	"context"
	"sync"
)

// RecordingHooks 是测试 / 调试用的 OrchestrationHooks 实现——把所有回调收集成
// 切片供断言。
//
// **典型用法**：
//
//	rec := &compose_v2.RecordingHooks{}
//	err := compose_v2.Run(ctx, g, cm, compose_v2.WithHooks(rec))
//	starts := rec.SnapshotNodeStarts()
//	events := rec.SnapshotEvents()
//
// **并发安全**：fan-out 多 goroutine 并发触发 hook 时内部用 mutex 保护切片写入。
// 读（Snapshot* 系列）应在测试断言阶段串行调用——与写并发会读到不一致快照。
type RecordingHooks struct {
	mu          sync.Mutex
	graphStarts int
	graphEnds   int
	nodeStarts  []NodeID
	nodeEnds    []NodeID
	events      []recordedEvent
}

type recordedEvent struct {
	NodeID NodeID
	Event  Event
}

// 编译期接口检查
var _ OrchestrationHooks = (*RecordingHooks)(nil)

// OnGraphStart 计数 +1.
func (r *RecordingHooks) OnGraphStart(ctx context.Context, _ Channels) (context.Context, error) {
	r.mu.Lock()
	r.graphStarts++
	r.mu.Unlock()
	return ctx, nil
}

// OnGraphEnd 计数 +1.
func (r *RecordingHooks) OnGraphEnd(ctx context.Context, _ Channels, _ error) context.Context {
	r.mu.Lock()
	r.graphEnds++
	r.mu.Unlock()
	return ctx
}

// OnNodeStart 记录 nodeID.
func (r *RecordingHooks) OnNodeStart(ctx context.Context, id NodeID, _ Channels) (context.Context, error) {
	r.mu.Lock()
	r.nodeStarts = append(r.nodeStarts, id)
	r.mu.Unlock()
	return ctx, nil
}

// OnNodeEnd 记录 nodeID.
func (r *RecordingHooks) OnNodeEnd(ctx context.Context, id NodeID, _ Channels, _ error) context.Context {
	r.mu.Lock()
	r.nodeEnds = append(r.nodeEnds, id)
	r.mu.Unlock()
	return ctx
}

// OnNodeEvent 记录 (nodeID, event).
func (r *RecordingHooks) OnNodeEvent(ctx context.Context, id NodeID, ev Event) context.Context {
	r.mu.Lock()
	r.events = append(r.events, recordedEvent{NodeID: id, Event: ev})
	r.mu.Unlock()
	return ctx
}

// SnapshotEvents 返所有 OnNodeEvent 调用的 Event 快照（按时序）。
func (r *RecordingHooks) SnapshotEvents() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	for i, e := range r.events {
		out[i] = e.Event
	}
	return out
}

// SnapshotNodeStarts 返所有 OnNodeStart 调用的 nodeID 顺序快照。
func (r *RecordingHooks) SnapshotNodeStarts() []NodeID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeID, len(r.nodeStarts))
	copy(out, r.nodeStarts)
	return out
}

// SnapshotNodeEnds 返所有 OnNodeEnd 调用的 nodeID 顺序快照。
func (r *RecordingHooks) SnapshotNodeEnds() []NodeID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeID, len(r.nodeEnds))
	copy(out, r.nodeEnds)
	return out
}

// SnapshotGraphLifecycle 返 (graphStarts, graphEnds) 计数。
func (r *RecordingHooks) SnapshotGraphLifecycle() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.graphStarts, r.graphEnds
}
