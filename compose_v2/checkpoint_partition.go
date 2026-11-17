package compose_v2

import (
	"context"
	"fmt"
	"reflect"
)

// CheckpointPartitionResolver 为多分区 Checkpointer 显式选择物理存档域。返回的
// Checkpointer 必须固定在选中的域，读写、租约和清档均不得随钩子的上下文重新路由。
// 同一物理分区应返回相等的固定句柄，或实现 [TaskLeaseProvider] 并返回相等的租约存储值。
// 单分区后端直接实现 [Checkpointer] 即可。
type CheckpointPartitionResolver interface {
	ResolveCheckpointPartition(ctx context.Context) (Checkpointer, error)
}

// ResolveCheckpointPartition 将配置的 Checkpointer 解析成固定域的存储句柄。
func ResolveCheckpointPartition(ctx context.Context, store Checkpointer) (Checkpointer, error) {
	if isFrameworkNil(store) {
		return nil, nil
	}
	resolver, ok := store.(CheckpointPartitionResolver)
	if !ok {
		return store, nil
	}
	partition, err := resolver.ResolveCheckpointPartition(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve checkpoint partition: %w", err)
	}
	if isFrameworkNil(partition) {
		return nil, fmt.Errorf("resolved checkpoint partition is nil")
	}
	return partition, nil
}

// SameCheckpointPartition 判断两个已解析的句柄是否访问同一存档及租约域。
func SameCheckpointPartition(a, b Checkpointer) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if reflect.ValueOf(a).Comparable() && reflect.ValueOf(b).Comparable() && a == b {
		return true
	}
	left, leftLeased := a.(TaskLeaseProvider)
	right, rightLeased := b.(TaskLeaseProvider)
	if !leftLeased || !rightLeased {
		return false
	}
	lease, other := left.CheckpointTaskLeases(), right.CheckpointTaskLeases()
	return !isFrameworkNil(lease) && !isFrameworkNil(other) && reflect.ValueOf(lease).Comparable() &&
		reflect.ValueOf(other).Comparable() && lease == other
}

// CheckpointDeleteCondition 指定一次清理所见的完整子树。空清单要求该范围没有存档。
type CheckpointDeleteCondition struct {
	Expected []NSCheckpoint
}

// CheckCheckpointDeleteCondition 核对已锁定的子树和删除预期。调用方只能删除匹配的槽，
// 并保持核对到删除的原子性。
func CheckCheckpointDeleteCondition(current []NSCheckpoint, condition CheckpointDeleteCondition) error {
	if len(current) != len(condition.Expected) {
		return ErrCheckpointStale
	}
	expected := make(map[string]*Checkpoint, len(condition.Expected))
	for _, slot := range condition.Expected {
		expected[slot.NS] = slot.Checkpoint
	}
	for _, slot := range current {
		before := expected[slot.NS]
		if before == nil || slot.Checkpoint == nil || !sameStoredCheckpoint(before, slot.Checkpoint) {
			return ErrCheckpointStale
		}
		delete(expected, slot.NS)
	}
	return nil
}

func sameStoredCheckpoint(before, after *Checkpoint) bool {
	if before == nil || after == nil {
		return before == after
	}
	return before.Status == after.Status && sameCheckpointSnapshot(before, after)
}

// loadCheckpointSubtree 保留当前分区内子树的快照，兄弟执行不参与核对。
func loadCheckpointSubtree(ctx context.Context, cp Checkpointer, sid, ns string) ([]NSCheckpoint, error) {
	slots, err := cp.LoadAllInSession(ctx, sid)
	if leaseErr := CheckCheckpointTaskLease(ctx); leaseErr != nil {
		return nil, leaseErr
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint subtree %q: %w", ns, err)
	}
	current := make([]NSCheckpoint, 0, len(slots))
	for _, slot := range slots {
		if IsNSInSubtree(ns, slot.NS) {
			if slot.Checkpoint == nil {
				return nil, fmt.Errorf("checkpoint subtree %q returned nil checkpoint for %q", ns, slot.NS)
			}
			current = append(current, slot)
		}
	}
	return current, nil
}
