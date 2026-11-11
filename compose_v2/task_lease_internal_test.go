package compose_v2

import (
	"context"
	"errors"
	"testing"
	"time"
)

type rejectedRenewal struct {
	TaskLeaseStore
}

func (rejectedRenewal) RenewTaskLease(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}

func TestCheckpointTaskLease_RenewalLossSurvivesDetachedCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	l := &checkpointTaskLease{store: rejectedRenewal{}, sid: "task", cancel: cancel, done: make(chan struct{})}
	expires := time.Now().Add(time.Minute)
	l.expires.Store(&expires)
	ctx = bindCheckpointTaskLease(ctx, l.store, l)
	renewCtx, stop := context.WithCancel(t.Context())
	defer func() {
		stop()
		<-l.done
	}()
	go l.renew(renewCtx, time.Millisecond)
	select {
	case <-l.done:
	case <-time.After(time.Second):
		t.Fatal("续约被拒后应退出")
	}
	if !errors.Is(context.Cause(ctx), ErrTaskLeaseLost) {
		t.Fatalf("失租应取消执行，实际 %v", context.Cause(ctx))
	}
	if err := CheckCheckpointTaskLease(context.WithoutCancel(ctx)); !errors.Is(err, ErrTaskLeaseLost) {
		t.Fatalf("取消分离的存档收尾也应识别失租，实际 %v", err)
	}
}
