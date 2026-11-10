package compose_v2

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync/atomic"
	"time"
)

// TaskLeaseProvider 是支持多进程协调的 Checkpointer 扩展。同一存储域的嵌套调用需复用可比较且相等的租约存储值。
type TaskLeaseProvider interface {
	CheckpointTaskLeases() TaskLeaseStore
}

// TaskLeaseStore 按存档存储域和任务 ID 原子协调执行和放弃。租约有效期使用存储端时间，各方法遵守 ctx 取消。
// 过期的持有者不能续约或释放新持有者的租约。SaveNS 和 DeleteSubtree 需将 [CheckpointTaskLeaseToken]
// 给出的 token 与有效租约的比较和实际变更放在同一个原子操作中。
type TaskLeaseStore interface {
	TryAcquireTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error)
	RenewTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error)
	ReleaseTaskLease(ctx context.Context, sid, token string) error
}

const (
	taskLeaseTTL       = 30 * time.Second
	taskLeaseRenewal   = 10 * time.Second
	taskLeaseIOTimeout = 5 * time.Second
)

type checkpointTaskLeaseKey struct{}

type checkpointTaskLease struct {
	parent   *checkpointTaskLease
	store    TaskLeaseStore
	sid      string
	token    string
	stop     context.CancelFunc
	cancel   context.CancelCauseFunc
	done     chan struct{}
	expires  atomic.Pointer[time.Time]
	lost     atomic.Bool
	released atomic.Bool
}

func taskLeaseFrom(ctx context.Context, store TaskLeaseStore, sid string) *checkpointTaskLease {
	lease, _ := ctx.Value(checkpointTaskLeaseKey{}).(*checkpointTaskLease)
	for ; lease != nil; lease = lease.parent {
		if lease.sid == sid && lease.store == store {
			return lease
		}
	}
	return nil
}

// CheckpointTaskLeaseToken 返回该调用的任务租约令牌。即使租约已失效也保留令牌，
// 让存储端能拒绝过期持有者的写档与删档。
func CheckpointTaskLeaseToken(ctx context.Context, store TaskLeaseStore, sid string) string {
	if lease := taskLeaseFrom(ctx, store, sid); lease != nil {
		return lease.token
	}
	return ""
}

// CheckCheckpointTaskLease 在执行边界检查续约失败及保守的本地有效期。
// 存档变更仍由存储端在同一原子操作中校验 token。
func CheckCheckpointTaskLease(ctx context.Context) error {
	lease, _ := ctx.Value(checkpointTaskLeaseKey{}).(*checkpointTaskLease)
	if lease == nil {
		return nil
	}
	if cause := context.Cause(ctx); errors.Is(cause, ErrTaskLeaseLost) {
		return cause
	}
	for ; lease != nil; lease = lease.parent {
		if !lease.released.Load() && (lease.lost.Load() || !time.Now().Before(*lease.expires.Load())) {
			return lease.markLost()
		}
	}
	return nil
}

// WithCheckpointLeases 将这次登记绑定到 cp。用于已在应用锁下做进程内登记、现在可以访问后端的集成层。
// 调用方在成功和失败路径都要 Release。执行与放弃只能绑定一次。
func (a *TaskActivity) WithCheckpointLeases(ctx context.Context, cp Checkpointer) (context.Context, error) {
	return a.acquireCheckpointLeases(ctx, cp)
}

func (a *TaskActivity) acquireCheckpointLeases(ctx context.Context, cp Checkpointer) (context.Context, error) {
	if a.cancel != nil {
		return ctx, errors.New("compose_v2: task activity already has checkpoint leases")
	}
	provider, ok := cp.(TaskLeaseProvider)
	if !ok || (a.sid == "" && len(a.abandoned) == 0) {
		return ctx, nil
	}
	store := provider.CheckpointTaskLeases()
	if isFrameworkNil(store) {
		return ctx, nil
	}
	if !reflect.ValueOf(store).Comparable() {
		return ctx, errors.New("compose_v2: checkpoint task lease store must be comparable")
	}
	if err := CheckCheckpointTaskLease(ctx); err != nil {
		return ctx, err
	}
	existing := taskLeaseFrom(ctx, store, a.sid)
	acquireRun := a.sid != "" && (existing == nil || existing.released.Load())
	if !acquireRun && len(a.abandoned) == 0 {
		return ctx, nil
	}
	ctx, a.cancel = context.WithCancelCause(ctx)
	if acquireRun {
		var err error
		ctx, a.sharedRun, err = acquireCheckpointTaskLease(ctx, store, a.sid, a.cancel)
		if err != nil {
			return ctx, err
		}
	}
	for _, id := range a.abandoned {
		if existing := taskLeaseFrom(ctx, store, id); existing != nil && !existing.released.Load() {
			if !slices.Contains(a.sharedAbandons, existing) {
				return ctx, fmt.Errorf("cannot abandon task %q: %w", id, ErrTaskBusy)
			}
			continue // 同一次调用里重复声明同一个旧任务。
		}
		next, lease, err := acquireCheckpointTaskLease(ctx, store, id, a.cancel)
		if err != nil {
			return ctx, err
		}
		ctx = next
		a.sharedAbandons = append(a.sharedAbandons, lease)
	}
	return ctx, nil
}

func acquireCheckpointTaskLease(ctx context.Context, store TaskLeaseStore, sid string, cancel context.CancelCauseFunc) (context.Context, *checkpointTaskLease, error) {
	token := rand.Text()
	expires := time.Now().Add(taskLeaseTTL)
	ioCtx, stopIO := context.WithTimeout(ctx, taskLeaseIOTimeout)
	defer stopIO()
	ok, err := store.TryAcquireTaskLease(ioCtx, sid, token, taskLeaseTTL)
	if err != nil {
		// 请求可能已在服务端执行，按 token 尽力归还。
		releaseCheckpointTaskLease(store, sid, token)
		return ctx, nil, fmt.Errorf("acquire checkpoint lease for task %q: %w", sid, err)
	}
	if !ok {
		return ctx, nil, fmt.Errorf("cannot acquire checkpoint lease for task %q: %w", sid, ErrTaskBusy)
	}

	parent, _ := ctx.Value(checkpointTaskLeaseKey{}).(*checkpointTaskLease)
	renewCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	lease := &checkpointTaskLease{parent: parent, store: store, sid: sid, token: token, stop: stop, cancel: cancel, done: make(chan struct{})}
	lease.expires.Store(&expires)
	go lease.renew(renewCtx, taskLeaseRenewal)
	return context.WithValue(ctx, checkpointTaskLeaseKey{}, lease), lease, nil
}

func (l *checkpointTaskLease) renew(ctx context.Context, interval time.Duration) {
	defer close(l.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expires := time.Now().Add(taskLeaseTTL)
			ioCtx, stop := context.WithTimeout(ctx, taskLeaseIOTimeout)
			ok, err := l.store.RenewTaskLease(ioCtx, l.sid, l.token, taskLeaseTTL)
			stop()
			if ctx.Err() != nil {
				return
			}
			if err != nil || !ok {
				slog.WarnContext(ctx, "compose_v2: 存档任务租约丢失", "sid", l.sid, "err", err)
				l.markLost()
				return
			}
			l.expires.Store(&expires)
		}
	}
}

func (l *checkpointTaskLease) markLost() error {
	err := fmt.Errorf("checkpoint lease for task %q: %w", l.sid, ErrTaskLeaseLost)
	l.lost.Store(true)
	l.cancel(err)
	return err
}

func (l *checkpointTaskLease) release() {
	l.released.Store(true)
	l.stop()
	<-l.done
	releaseCheckpointTaskLease(l.store, l.sid, l.token)
}

func releaseCheckpointTaskLease(store TaskLeaseStore, sid, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), taskLeaseIOTimeout)
	defer cancel()
	if err := store.ReleaseTaskLease(ctx, sid, token); err != nil {
		slog.WarnContext(ctx, "compose_v2: 归还存档任务租约失败", "sid", sid, "err", err)
	}
}
