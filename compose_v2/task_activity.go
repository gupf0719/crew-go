package compose_v2

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

type taskActivityState struct {
	running    int
	abandoning bool
}

var taskActivities = struct {
	sync.Mutex
	byID map[string]taskActivityState
}{byID: make(map[string]taskActivityState)}

// TaskActivity 持有一次调用的执行登记与放弃许可。由取得它的调用串行释放。
type TaskActivity struct {
	sid            string
	abandoned      []string
	sharedRun      *checkpointTaskLease
	sharedAbandons []*checkpointTaskLease
	cancel         context.CancelCauseFunc
}

// BeginTaskActivity 在进程内原子登记 sid 的执行和 abandoned 中任务的放弃许可。执行可嵌套，放弃与执行或另一次放弃互斥。
// sid 为空时只申请放弃许可。支持 [TaskLeaseProvider] 的 Checkpointer 还逐个任务申请跨进程独占租约。
// 登记或申请冲突时返回 [ErrTaskBusy]，释放本次已取得的登记与租约。
//
// 给图引擎和 Agent 入口使用：在读写存档或历史之前调用，结束时 Release。放弃操作完成后可先调用 FinishAbandon。
func BeginTaskActivity(ctx context.Context, sid string, abandoned []string) (context.Context, *TaskActivity, error) {
	return beginTaskActivity(ctx, GetCheckpointer(ctx), sid, abandoned)
}

func beginTaskActivity(ctx context.Context, cp Checkpointer, sid string, abandoned []string) (context.Context, *TaskActivity, error) {
	executionCtx := ctx
	if sid != "" && !WithinNodeScope(ctx) {
		executionCtx = withoutDetachedFrom(ctx, sid)
	}
	abandoned = slices.Clone(abandoned)
	taskActivities.Lock()
	if taskActivities.byID[sid].abandoning {
		taskActivities.Unlock()
		return ctx, nil, fmt.Errorf("cannot execute task %q: %w", sid, ErrTaskBusy)
	}
	for _, id := range abandoned {
		if id == "" || id == sid {
			taskActivities.Unlock()
			return ctx, nil, fmt.Errorf("invalid task to abandon %q for session %q", id, sid)
		}
		if _, busy := taskActivities.byID[id]; busy {
			taskActivities.Unlock()
			return ctx, nil, fmt.Errorf("cannot abandon task %q: %w", id, ErrTaskBusy)
		}
	}
	if sid != "" {
		state := taskActivities.byID[sid]
		state.running++
		taskActivities.byID[sid] = state
	}
	for _, id := range abandoned {
		taskActivities.byID[id] = taskActivityState{abandoning: true}
	}
	taskActivities.Unlock()
	activity := &TaskActivity{sid: sid, abandoned: abandoned}
	var ok bool
	defer func() {
		if !ok {
			activity.Release()
		}
	}()
	executionCtx, err := activity.acquireCheckpointLeases(executionCtx, cp)
	if err != nil {
		return ctx, nil, err
	}
	ok = true
	return executionCtx, activity, nil
}

// FinishAbandon 释放放弃许可。调用方应在删档和历史收尾结束后调用。
func (a *TaskActivity) FinishAbandon() {
	if len(a.abandoned) == 0 {
		return
	}
	for _, lease := range a.sharedAbandons {
		lease.release()
	}
	a.sharedAbandons = nil
	taskActivities.Lock()
	for _, id := range a.abandoned {
		delete(taskActivities.byID, id)
	}
	taskActivities.Unlock()
	a.abandoned = nil
}

// Release 释放本次调用持有的许可和执行登记。
func (a *TaskActivity) Release() {
	a.FinishAbandon()
	if a.sharedRun != nil {
		a.sharedRun.release()
		a.sharedRun = nil
	}
	if a.cancel != nil {
		a.cancel(nil)
		a.cancel = nil
	}
	if a.sid == "" {
		return
	}
	taskActivities.Lock()
	state := taskActivities.byID[a.sid]
	if state.running <= 1 {
		delete(taskActivities.byID, a.sid)
	} else {
		state.running--
		taskActivities.byID[a.sid] = state
	}
	taskActivities.Unlock()
	a.sid = ""
}

func taskExecuting(sid string) bool {
	taskActivities.Lock()
	defer taskActivities.Unlock()
	return taskActivities.byID[sid].running > 0
}

// TaskExecuting 报本进程中 sid 是否有已登记的执行，包括内建 Agent 的入口、钩子和历史收尾。
// 它提供查询快照。要与执行互斥地放弃任务，使用 [BeginTaskActivity] 取得许可。
func TaskExecuting(sid string) bool {
	return sid != "" && taskExecuting(sid)
}
