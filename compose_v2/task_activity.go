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
	exclusive  *TaskActivity
}

var taskActivities = struct {
	sync.Mutex
	byID map[string]taskActivityState
}{byID: make(map[string]taskActivityState)}

// TaskActivity 持有一次调用的执行登记与放弃许可。由取得它的调用串行释放。
type TaskActivity struct {
	sid            string
	fresh          *freshTaskCheckpoints
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
	return beginConfiguredTaskActivity(ctx, sid, abandoned, false)
}

// BeginFreshTaskActivity 保护新任务对存档的独占使用。沿同一 ctx 派生的执行可借用登记，
// 其他存档调用在本次任务退出前得到 [ErrTaskBusy]。每个实际使用的后端在持有租约时检查该 sid 下的可续存档。
func BeginFreshTaskActivity(ctx context.Context, sid string, abandoned []string) (context.Context, *TaskActivity, error) {
	return beginConfiguredTaskActivity(ctx, sid, abandoned, true)
}

func beginConfiguredTaskActivity(ctx context.Context, sid string, abandoned []string, fresh bool) (context.Context, *TaskActivity, error) {
	partition, err := ResolveCheckpointPartition(ctx, GetCheckpointer(ctx))
	if err != nil {
		return ctx, nil, err
	}
	return beginTaskActivity(ctx, partition, sid, abandoned, fresh)
}

type taskActivityKey struct{}

func (taskActivityKey) ExecutionState() {}

type taskActivityBinding struct {
	parent   *taskActivityBinding
	activity *TaskActivity
}

func beginTaskActivity(ctx context.Context, cp Checkpointer, sid string, abandoned []string, fresh bool) (context.Context, *TaskActivity, error) {
	executionCtx := ctx
	if sid != "" && !WithinNodeScope(ctx) {
		executionCtx = withoutDetachedFrom(ctx, sid)
	}
	abandoned = slices.Clone(abandoned)
	parent, _ := ctx.Value(taskActivityKey{}).(*taskActivityBinding)
	activity := &TaskActivity{sid: sid, abandoned: abandoned}
	taskActivities.Lock()
	state := taskActivities.byID[sid]
	inherited, freshOwner, ownsExclusive := inheritedActivity(state, parent, sid)
	claimsStorage := cp != nil && (fresh || (freshOwner != nil && state.exclusive == nil))
	if state.abandoning || (cp != nil && !ownsExclusive) || (claimsStorage && inherited != state.running) {
		taskActivities.Unlock()
		return ctx, nil, &TaskBusyError{TaskID: sid, operation: "cannot execute task"}
	}
	for _, id := range abandoned {
		if id == "" || id == sid {
			taskActivities.Unlock()
			return ctx, nil, fmt.Errorf("invalid task to abandon %q for session %q", id, sid)
		}
		if _, busy := taskActivities.byID[id]; busy {
			taskActivities.Unlock()
			return ctx, nil, &TaskBusyError{TaskID: id, operation: "cannot abandon task"}
		}
	}
	if sid != "" {
		if fresh {
			activity.fresh = &freshTaskCheckpoints{checked: make(map[freshCheckpointPartition]string)}
			freshOwner = activity
		}
		if claimsStorage {
			state.exclusive = freshOwner
		}
		state.running++
		taskActivities.byID[sid] = state
		executionCtx = context.WithValue(executionCtx, taskActivityKey{}, &taskActivityBinding{parent: parent, activity: activity})
	}
	for _, id := range abandoned {
		taskActivities.byID[id] = taskActivityState{abandoning: true}
	}
	taskActivities.Unlock()
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
	if freshOwner != nil {
		if err := freshOwner.fresh.check(executionCtx, cp, sid); err != nil {
			return ctx, nil, err
		}
	}
	ok = true
	return executionCtx, activity, nil
}

// inheritedActivity 只认可当前 ctx 链上仍有效的同任务登记。调用方持有 taskActivities 的锁。
func inheritedActivity(state taskActivityState, parent *taskActivityBinding, sid string) (count int, freshOwner *TaskActivity, ownsExclusive bool) {
	ownsExclusive = state.exclusive == nil
	if sid == "" || state.running == 0 {
		return
	}
	for current := parent; current != nil; current = current.parent {
		activity := current.activity
		if activity.sid != sid {
			continue
		}
		count++
		if freshOwner == nil && activity.fresh != nil {
			freshOwner = activity
		}
		ownsExclusive = ownsExclusive || activity == state.exclusive
	}
	return
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
		if state.exclusive == a {
			state.exclusive = nil
		}
		taskActivities.byID[a.sid] = state
	}
	a.sid = ""
	taskActivities.Unlock()
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
