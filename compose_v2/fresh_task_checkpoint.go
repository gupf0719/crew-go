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
	checked map[Checkpointer]string
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
	if comparable {
		f.mu.Lock()
		checkedToken, ok := f.checked[cp]
		f.mu.Unlock()
		if ok && checkedToken == token {
			return nil
		}
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
		if !slot.Checkpoint.HasResumableWork() {
			continue
		}
		if taskID != "" && slot.Checkpoint.TaskID == taskID {
			continue
		}
		return fmt.Errorf("run id %q (checkpoint namespace %q): %w (call Resume or use a new RunID)",
			sid, slot.NS, ErrRunIDHasResumableCheckpoint)
	}
	if comparable {
		f.mu.Lock()
		f.checked[cp] = token
		f.mu.Unlock()
	}
	return nil
}
