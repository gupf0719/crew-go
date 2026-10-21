package checkpointer_conformance

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// testGraphIDConflict 验证 GraphID 冲突 fail-fast (ISSUE-50 P0).
func testGraphIDConflict(t *testing.T, factory CheckpointerFactory) {
	t.Run("SaveNS_Same_GraphID_OK", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 同 GraphID 重复 SaveNS — 应允许 (覆盖语义)
		if err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_a", compose_v2.CPStatusUnset)); err != nil {
			t.Fatalf("first SaveNS: %v", err)
		}
		if err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_a", compose_v2.CPStatusPending)); err != nil {
			t.Errorf("second SaveNS same GraphID should succeed (overwrite), got: %v", err)
		}
	})

	t.Run("SaveNS_Different_GraphID_Returns_ErrGraphIDConflict", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_a", compose_v2.CPStatusUnset))
		err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_b", compose_v2.CPStatusUnset))
		if err == nil {
			t.Error("SaveNS with different GraphID should return ErrGraphIDConflict")
		}
		if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
			t.Errorf("expected ErrGraphIDConflict, got %v", err)
		}
	})

	t.Run("SaveNS_Empty_GraphID_Skip_Conflict_Check", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// existing.GraphID == "" 或 cp.GraphID == "" 时跳过 conflict check
		// (兼容旧 cp / 业务不设 GraphID 场景)
		_ = cp.SaveNS(ctx, "sid", "ns", newCheckpoint("", compose_v2.CPStatusUnset))
		if err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_a", compose_v2.CPStatusUnset)); err != nil {
			t.Errorf("SaveNS with prior empty GraphID should succeed, got: %v", err)
		}
	})

	// 判冲突以存档里最新那份 GraphID 为准：放行的写入整份替换存档，GraphID 跟着换；被拒的写入
	// 不动存档。判下一次写入冲不冲突与 LoadNS 读出来的必须是同一份——Run / Resume 入口按 LoadNS
	// 读出的 GraphID 放行，写回时却按另一份判的话，节点带着用户的答复已经跑过、进度存不下来。
	// sql 后端曾把 GraphID 另存一列、只在第一次写入时落下：'' → A → B 三次都成功，存档里是 B、
	// 列里一直是空。
	t.Run("SaveNS_GraphID_Follows_Latest_Accepted_Write", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		if err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("", compose_v2.CPStatusUnset)); err != nil {
			t.Fatalf("SaveNS empty GraphID: %v", err)
		}
		if err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_a", compose_v2.CPStatusPending)); err != nil {
			t.Fatalf("SaveNS graph_a over empty: %v", err)
		}
		err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint("graph_b", compose_v2.CPStatusUnset))
		if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
			t.Fatalf("SaveNS graph_b over graph_a: want ErrGraphIDConflict, got %v", err)
		}
		got, err := cp.LoadNS(ctx, "sid", "ns")
		if err != nil || got == nil {
			t.Fatalf("LoadNS: cp=%v err=%v", got, err)
		}
		if got.GraphID != "graph_a" || got.Status != compose_v2.CPStatusPending {
			t.Errorf("rejected write must leave the stored checkpoint as it was: got GraphID=%q Status=%v, want graph_a / pending",
				got.GraphID, got.Status)
		}
	})

	// 空 GraphID 的写入也是一次放行的写入：存档里的 GraphID 换成空，之后任何图都能写。sql 后端
	// 曾在这里拒掉 B——列里还是 A，而 LoadNS 读出来是空。
	t.Run("SaveNS_Empty_GraphID_Write_Replaces_Stored_GraphID", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		for _, g := range []string{"graph_a", "", "graph_b"} {
			if err := cp.SaveNS(ctx, "sid", "ns", newCheckpoint(g, compose_v2.CPStatusUnset)); err != nil {
				t.Fatalf("SaveNS GraphID=%q: %v", g, err)
			}
		}
		got, err := cp.LoadNS(ctx, "sid", "ns")
		if err != nil || got == nil {
			t.Fatalf("LoadNS: cp=%v err=%v", got, err)
		}
		if got.GraphID != "graph_b" {
			t.Errorf("stored GraphID = %q, want graph_b", got.GraphID)
		}
	})
}
