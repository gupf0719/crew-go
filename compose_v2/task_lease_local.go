package compose_v2

import (
	"context"
	"reflect"
	"sync"
)

type localCheckpointTask struct {
	cp  Checkpointer
	sid string
}

type localCheckpointLease struct {
	owner *TaskActivity
	users int
}

var localCheckpointTasks = struct {
	sync.Mutex
	byTask map[localCheckpointTask]localCheckpointLease
}{byTask: make(map[localCheckpointTask]localCheckpointLease)}

// acquireLocalCheckpointLease 按已解析的实际后端协调本进程执行。沿同一执行树的子图
// 可共享分区占用，最后一个该分区的登记者负责释放。
func (a *TaskActivity) acquireLocalCheckpointLease(ctx context.Context, cp Checkpointer) error {
	// 含 NaN 的可比较值也无法作为可释放的键。
	if a.sid == "" || !reflect.ValueOf(cp).Comparable() || cp != cp {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := localCheckpointTask{cp: cp, sid: a.sid}
	localCheckpointTasks.Lock()
	defer localCheckpointTasks.Unlock()
	lease := localCheckpointTasks.byTask[key]
	if lease.owner != nil && lease.owner != a.localOwner {
		return &TaskBusyError{TaskID: a.sid, operation: "cannot execute task"}
	}
	lease.owner = a.localOwner
	lease.users++
	localCheckpointTasks.byTask[key] = lease
	a.releaseLocal = func() {
		localCheckpointTasks.Lock()
		defer localCheckpointTasks.Unlock()
		lease := localCheckpointTasks.byTask[key]
		if lease.users == 1 {
			delete(localCheckpointTasks.byTask, key)
		} else {
			lease.users--
			localCheckpointTasks.byTask[key] = lease
		}
	}
	return nil
}
