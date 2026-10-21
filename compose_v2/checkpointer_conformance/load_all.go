package checkpointer_conformance

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// testLoadAllInSession 验证 LoadAllInSession 返 session 下全部 ns cp.
func testLoadAllInSession(t *testing.T, factory CheckpointerFactory) {
	t.Run("LoadAllInSession_Returns_All_NS", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid_a", "ns1", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_a", "ns2", newCheckpoint("g", compose_v2.CPStatusPending))
		_ = cp.SaveNS(ctx, "sid_a", "ns3", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_b", "ns1", newCheckpoint("g", compose_v2.CPStatusUnset))

		all, err := cp.LoadAllInSession(ctx, "sid_a")
		if err != nil {
			t.Fatalf("LoadAllInSession: %v", err)
		}
		if len(all) != 3 {
			t.Errorf("expected 3 cps in sid_a, got %d", len(all))
		}

		// sid_b 不混入
		seen := map[string]bool{}
		for _, nc := range all {
			seen[nc.NS] = true
		}
		if !seen["ns1"] || !seen["ns2"] || !seen["ns3"] {
			t.Errorf("expected ns1+ns2+ns3, got %v", seen)
		}
	})

	t.Run("LoadAllInSession_NonExistent_Returns_Empty", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		all, err := cp.LoadAllInSession(ctx, "no_such_sid")
		if err != nil {
			t.Errorf("LoadAllInSession non-existent: %v", err)
		}
		if len(all) != 0 {
			t.Errorf("expected 0 for non-existent sid, got %d", len(all))
		}
	})

	// audit 视角 1 H2 + 视角 3 M1: 业务 backend SQL 漏 sid filter → LoadAllInSession
	// 返跨 sid 数据. 加强 sid_b 不混入断言 + 返回 cp 的 ID 字段必匹配 sid_a.
	t.Run("LoadAllInSession_Cross_SID_Isolation", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid_a", "ns1", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_a", "ns2", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_b", "ns1", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_b", "ns3", newCheckpoint("g", compose_v2.CPStatusUnset))

		all, _ := cp.LoadAllInSession(ctx, "sid_a")
		if len(all) != 2 {
			t.Fatalf("expected 2 cps in sid_a, got %d", len(all))
		}
		// 每个返回的 cp.ID 必须是 sid_a (SaveNS 自动填 ID=sid)
		for _, nc := range all {
			if nc.Checkpoint == nil {
				t.Error("LoadAllInSession returned nil Checkpoint")
				continue
			}
			if nc.Checkpoint.ID != "sid_a" {
				t.Errorf("LoadAllInSession(sid_a) leaked sid=%q cp (ns=%q) — 跨 sid 数据泄露",
					nc.Checkpoint.ID, nc.NS)
			}
		}
	})
}
