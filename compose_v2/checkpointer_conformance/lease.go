package checkpointer_conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// RunTaskLeaseTests 验证同一存储域的两个 Checkpointer 客户端的互斥、续约、换手及存档围栏。
// expire 在测试存储端让指定任务的租约立即到期。
func RunTaskLeaseTests(t *testing.T, cp, peer compose_v2.Checkpointer, expire func(string)) {
	t.Helper()
	ctx := t.Context()
	primary := cp.(compose_v2.TaskLeaseProvider).CheckpointTaskLeases()
	remote := peer.(compose_v2.TaskLeaseProvider).CheckpointTaskLeases()
	const sid, ns, next = "task", "child", "next-owner"
	const ttl = time.Minute
	ownedCtx, activity, err := compose_v2.BeginTaskActivity(compose_v2.WithCheckpointerCtx(ctx, cp), sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer activity.Release()
	first := compose_v2.CheckpointTaskLeaseToken(ownedCtx, primary, sid)
	if first == "" {
		t.Fatal("执行入口没有持有租约")
	}
	if acquired, err := remote.TryAcquireTaskLease(ctx, sid, next, ttl); err != nil || acquired {
		t.Fatalf("其他客户端不应取得活跃任务的租约，实际 acquired=%t err=%v", acquired, err)
	}
	if renewed, err := primary.RenewTaskLease(ctx, sid, first, ttl); err != nil || !renewed {
		t.Fatalf("当前持有者应能续约，实际 renewed=%t err=%v", renewed, err)
	}
	if err := cp.SaveNS(ownedCtx, sid, ns, &compose_v2.Checkpoint{CreatedAt: 1}); err != nil {
		t.Fatal(err)
	}

	expire(sid)
	if renewed, err := primary.RenewTaskLease(ctx, sid, first, ttl); err != nil || renewed {
		t.Fatalf("过期持有者不能续约，实际 renewed=%t err=%v", renewed, err)
	}
	if acquired, err := remote.TryAcquireTaskLease(ctx, sid, next, ttl); err != nil || !acquired {
		t.Fatalf("过期后应允许接管，实际 acquired=%t err=%v", acquired, err)
	}
	t.Cleanup(func() {
		if err := remote.ReleaseTaskLease(context.Background(), sid, next); err != nil {
			t.Error(err)
		}
	})
	if err := peer.SaveNS(ctx, sid, ns, &compose_v2.Checkpoint{CreatedAt: 2}); err != nil {
		t.Fatal(err)
	}
	if err := cp.SaveNS(ownedCtx, sid, ns, &compose_v2.Checkpoint{CreatedAt: 3}); !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
		t.Fatalf("失租者不能覆盖新档，实际 %v", err)
	}
	if count, err := cp.DeleteSubtree(ownedCtx, sid, ""); count != 0 || !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
		t.Fatalf("失租者不能删新档，实际 count=%d err=%v", count, err)
	}
	activity.Release()
	if renewed, err := remote.RenewTaskLease(ctx, sid, next, ttl); err != nil || !renewed {
		t.Fatalf("旧持有者释放后新租约应仍有效，实际 renewed=%t err=%v", renewed, err)
	}
	if got, err := peer.LoadNS(ctx, sid, ns); err != nil || got == nil || got.CreatedAt != 2 {
		t.Fatalf("新持有者的存档应保留，实际 cp=%v err=%v", got, err)
	}
}
