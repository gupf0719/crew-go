package compose_v2

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// ErrRunIDHasResumableCheckpoint 表示新任务引用了仍有可续存档的 RunID。
var ErrRunIDHasResumableCheckpoint error = &freshTaskCheckpointConflict{}

type freshTaskCheckpointConflict struct{}

func (*freshTaskCheckpointConflict) Error() string { return "run id has a resumable checkpoint" }
func (*freshTaskCheckpointConflict) NonRetryable() {}

// freshTaskCheckpoints 记录本次新任务已经检查的后端。下层选用另一个存储域时，
// 先取得它的租约，再检查整个 RunID。每个通过检查的后端可保存本次任务自己的进度。
type freshTaskCheckpoints struct {
	mu      sync.Mutex
	checked map[freshCheckpointPartition]string
}

type freshCheckpointPartition struct {
	checkpointer Checkpointer
	owner        string
}

func (f *freshTaskCheckpoints) check(ctx context.Context, cp Checkpointer, sid string) error {
	if cp == nil {
		return nil
	}
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	token := ""
	if provider, ok := cp.(TaskLeaseProvider); ok {
		if store := provider.CheckpointTaskLeases(); !isFrameworkNil(store) {
			token = CheckpointTaskLeaseToken(ctx, store, sid)
		}
	}
	comparable := reflect.ValueOf(cp).Comparable()
	partition := freshCheckpointPartition{checkpointer: cp, owner: GetOwner(ctx)}
	if comparable {
		f.mu.Lock()
		checkedToken, ok := f.checked[partition]
		f.mu.Unlock()
		if ok && checkedToken == token {
			return nil
		}
	}
	if err := checkFreshTaskCheckpoints(ctx, cp, sid); err != nil {
		return err
	}
	if comparable {
		f.mu.Lock()
		f.checked[partition] = token
		f.mu.Unlock()
	}
	return nil
}

// checkFreshTaskCheckpoints 检查实际存储上下文。钩子可用私有上下文值路由分区，
// 即使 Checkpointer 和 owner 都没变，也不能借用入口的检查结果。
func checkFreshTaskCheckpoints(ctx context.Context, cp Checkpointer, sid string) error {
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	saved, loadErr := cp.LoadAllInSession(ctx, sid)
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	if loadErr != nil {
		return fmt.Errorf("check checkpoints for fresh run %q: %w", sid, loadErr)
	}
	taskID := getRunTaskID(ctx)
	for _, slot := range saved {
		if !checkpointRequiresTaskOwnership(slot.Checkpoint) {
			continue
		}
		if taskID != "" && slot.Checkpoint.TaskID == taskID {
			continue
		}
		if slot.Checkpoint.Status == CPStatusAborted {
			return RejectCheckpointIdentity(slot.Checkpoint, fmt.Sprintf("run id %q has a terminal checkpoint at %q", sid, slot.NS))
		}
		return fmt.Errorf("run id %q (checkpoint namespace %q): %w (call Resume or use a new RunID)",
			sid, slot.NS, ErrRunIDHasResumableCheckpoint)
	}
	return nil
}

func hasFreshTaskAdmission(ctx context.Context, sid string) bool {
	binding, _ := ctx.Value(taskActivityKey{}).(*taskActivityBinding)
	taskActivities.Lock()
	defer taskActivities.Unlock()
	for ; binding != nil; binding = binding.parent {
		if a := binding.activity; a.sid == sid && a.fresh != nil {
			return true
		}
	}
	return false
}
