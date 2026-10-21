package checkpointer_conformance

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// testListPending 验证 ListPending 只返 Status=Pending 的 cp.
func testListPending(t *testing.T, factory CheckpointerFactory) {
	t.Run("ListPending_Filters_By_Status", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 1 Unset + 2 Pending + 1 Failed
		_ = cp.SaveNS(ctx, "sid", "ns_unset", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns_p1", newCheckpoint("g", compose_v2.CPStatusPending))
		_ = cp.SaveNS(ctx, "sid", "ns_p2", newCheckpoint("g", compose_v2.CPStatusPending))
		// **失败现场不算 pending**：上次 Run 带着业务错退出留下的存档，存着完整 frontier。
		// `ListPending` 是「谁在等答案」的入口，失败现场不属于这一类。⚠️ 别把这条读成
		// 「失败档不会被续跑」——2026-08-13 起续跑闸门按内容判、不读 Status，它照样被捡起。
		_ = cp.SaveNS(ctx, "sid", "ns_failed", newCheckpoint("g", compose_v2.CPStatusFailed))

		pending, err := cp.ListPending(ctx, "sid")
		if err != nil {
			t.Fatalf("ListPending: %v", err)
		}
		if len(pending) != 2 {
			t.Errorf("expected 2 pending, got %d", len(pending))
		}
		// 验证返回的全是 Pending 状态
		for _, nc := range pending {
			if nc.Checkpoint.Status != compose_v2.CPStatusPending {
				t.Errorf("ListPending returned non-pending cp: ns=%q status=%v"+
					"（Failed 也不行——它续得上，但不属于「在等答案」这一类）",
					nc.NS, nc.Checkpoint.Status)
			}
		}
		// 失败存档必须还在（只是不进 pending 列表），否则业务显式 Resume 无从续起
		if got, err := cp.LoadNS(ctx, "sid", "ns_failed"); err != nil || got == nil {
			t.Errorf("失败存档应当保留：LoadNS(ns_failed) = %v, err=%v", got, err)
		} else if got.Status != compose_v2.CPStatusFailed {
			t.Errorf("失败存档 Status round-trip 丢了: got %v, want failed——"+
				"业务自研 backend 漏序列化这一档时，失败现场会被当成挂起自动捡起", got.Status)
		}
	})

	t.Run("ListPending_NonExistent_Session_Returns_Empty", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		pending, err := cp.ListPending(ctx, "no_such_sid")
		if err != nil {
			t.Errorf("ListPending non-existent: %v", err)
		}
		if len(pending) != 0 {
			t.Errorf("expected 0 pending for non-existent sid, got %d", len(pending))
		}
	})

	// audit 视角 1 H2: 业务 PG `SELECT ... WHERE status='pending'` 漏 `WHERE sid=?`
	// → ListPending(sid_a) 返回 sid_b 的 pending cp → SaaS 多租户**数据泄露**.
	// conformance 强制业务隔离 contract.
	t.Run("ListPending_Cross_SID_Isolation", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// sid_1: 2 pending + 1 unset
		_ = cp.SaveNS(ctx, "sid_1", "ns_p1", newCheckpoint("g", compose_v2.CPStatusPending))
		_ = cp.SaveNS(ctx, "sid_1", "ns_p2", newCheckpoint("g", compose_v2.CPStatusPending))
		_ = cp.SaveNS(ctx, "sid_1", "ns_u1", newCheckpoint("g", compose_v2.CPStatusUnset))

		// sid_2: 1 pending
		_ = cp.SaveNS(ctx, "sid_2", "ns_other", newCheckpoint("g", compose_v2.CPStatusPending))

		// ListPending(sid_1) 必须恰返 sid_1 的 2 个 pending, 不混 sid_2 的
		pending, err := cp.ListPending(ctx, "sid_1")
		if err != nil {
			t.Fatalf("ListPending sid_1: %v", err)
		}
		if len(pending) != 2 {
			t.Fatalf("expected 2 pending in sid_1, got %d", len(pending))
		}
		// 验返回的 ns 都是 sid_1 的, sid_2 的 ns_other 不能出现
		seen := map[string]bool{}
		for _, nc := range pending {
			seen[nc.NS] = true
			if nc.NS == "ns_other" {
				t.Errorf("ListPending(sid_1) leaked sid_2 cp (ns=%q) — 跨 sid 数据泄露", nc.NS)
			}
		}
		if !seen["ns_p1"] || !seen["ns_p2"] {
			t.Errorf("ListPending(sid_1) missing expected ns, got %v", seen)
		}

		// 反向验 sid_2 只返自己的 1 个
		pending2, _ := cp.ListPending(ctx, "sid_2")
		if len(pending2) != 1 {
			t.Errorf("expected 1 pending in sid_2, got %d", len(pending2))
		}
	})
}
