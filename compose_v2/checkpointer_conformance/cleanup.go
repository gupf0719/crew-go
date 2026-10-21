package checkpointer_conformance

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// testCleanup 验证 Cleanup(keepLastN) 按 sid 维度排序保留最新 N 个 sid.
//
// crew-go 协议 (checkpointer.go:77-81):
//   - 按 sid 维度排序 (用 sid 下所有 ns cp 中**最大** CreatedAt 当 sid 排序键)
//   - 保留最新 keepLastN 个 sid, 其余整 sid 删除
//   - keepLastN <= 0 等价 0: 删全部 sid
//   - 返删除的 cp 总数
//
// audit 视角 1 + 视角 3 H1: Cleanup 早期不在 conformance 覆盖范围内, 但 keepLastN
// 排序逻辑业务 PG/MySQL 实现易写错 (e.g., SELECT sid ORDER BY MAX(created_at)
// 漏 GROUP BY) — 故补了本 contract 测试, 现已在 RunCheckpointerTests 内跑.
func testCleanup(t *testing.T, factory CheckpointerFactory) {
	t.Run("Cleanup_KeepLastN_Sorted_By_CreatedAt", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// audit iter-2 视角 1 HIGH-1 + 视角 2 HIGH-3: **sid 字典序故意反 CreatedAt**
		// 防 5 类炸弹 #1 (业务 backend `ORDER BY sid DESC LIMIT N` 跟 `ORDER BY
		// CreatedAt DESC LIMIT N` 数据布局相同时假绿).
		//
		// sid_aaa CreatedAt=500 (字典序最小, CreatedAt 最大)
		// sid_bbb CreatedAt=400
		// sid_ccc CreatedAt=300
		// sid_ddd CreatedAt=200
		// sid_eee CreatedAt=100 (字典序最大, CreatedAt 最小)
		//
		// keepLastN=3 期望保留 (CreatedAt 顶 3): sid_aaa/bbb/ccc (字典序前 3)
		// 业务 backend 用字典序 desc 会拿到 sid_eee/ddd/ccc → my test 抓到 ✅
		fixtures := []struct {
			sid       string
			createdAt int64
		}{
			{"sid_aaa", 500},
			{"sid_bbb", 400},
			{"sid_ccc", 300},
			{"sid_ddd", 200},
			{"sid_eee", 100},
		}
		for _, f := range fixtures {
			c := newCheckpoint("g", compose_v2.CPStatusUnset)
			c.CreatedAt = f.createdAt
			if err := cp.SaveNS(ctx, f.sid, "", c); err != nil {
				t.Fatalf("SaveNS %s: %v", f.sid, err)
			}
		}

		// keepLastN=3 保留 CreatedAt 顶 3 = sid_aaa/bbb/ccc, 删 sid_ddd/eee (2 cp)
		deleted, err := cp.Cleanup(ctx, 3)
		if err != nil {
			t.Fatalf("Cleanup: %v", err)
		}
		if deleted != 2 {
			t.Errorf("expected 2 cps deleted (CreatedAt 底 2: sid_ddd+sid_eee), got %d", deleted)
		}

		// 验留下的 3 个 sid (CreatedAt 顶 3, 字典序前 3 — 业务实现按 CreatedAt 才对)
		for _, sid := range []string{"sid_aaa", "sid_bbb", "sid_ccc"} {
			ex, _ := cp.ExistsNS(ctx, sid, "")
			if !ex {
				t.Errorf("%s should be kept (CreatedAt in top 3), got deleted (业务 backend 可能按字典序排序而非 CreatedAt)", sid)
			}
		}
		// 验删了的 2 个 (CreatedAt 底 2)
		for _, sid := range []string{"sid_ddd", "sid_eee"} {
			ex, _ := cp.ExistsNS(ctx, sid, "")
			if ex {
				t.Errorf("%s should be deleted (CreatedAt below top 3), still exists", sid)
			}
		}
	})

	t.Run("Cleanup_KeepLastN_Zero_Deletes_All", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid_a", "", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_b", "", newCheckpoint("g", compose_v2.CPStatusUnset))

		// keepLastN=0 删全部
		deleted, err := cp.Cleanup(ctx, 0)
		if err != nil {
			t.Fatalf("Cleanup(0): %v", err)
		}
		if deleted != 2 {
			t.Errorf("expected 2 cps deleted, got %d", deleted)
		}
	})

	t.Run("Cleanup_KeepLastN_Negative_Equivalent_To_Zero", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid_a", "", newCheckpoint("g", compose_v2.CPStatusUnset))

		// keepLastN<0 等价 0 (crew-go 协议 checkpointer.go:80)
		deleted, _ := cp.Cleanup(ctx, -5)
		if deleted != 1 {
			t.Errorf("expected 1 cp deleted with keepLastN=-5 (≡0), got %d", deleted)
		}
	})

	t.Run("Cleanup_Multiple_NS_Per_SID_Sorted_By_Max_CreatedAt", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// sid_old: 2 ns 最大 CreatedAt=100; sid_new: 2 ns 最大 CreatedAt=500
		c1a := newCheckpoint("g", compose_v2.CPStatusUnset)
		c1a.CreatedAt = 50
		_ = cp.SaveNS(ctx, "sid_old", "ns_a", c1a)
		c1b := newCheckpoint("g", compose_v2.CPStatusUnset)
		c1b.CreatedAt = 100
		_ = cp.SaveNS(ctx, "sid_old", "ns_b", c1b)

		c2a := newCheckpoint("g", compose_v2.CPStatusUnset)
		c2a.CreatedAt = 400
		_ = cp.SaveNS(ctx, "sid_new", "ns_a", c2a)
		c2b := newCheckpoint("g", compose_v2.CPStatusUnset)
		c2b.CreatedAt = 500
		_ = cp.SaveNS(ctx, "sid_new", "ns_b", c2b)

		// keepLastN=1 保留 sid_new (最大 500 > 100), 删 sid_old 整 2 个 ns
		deleted, _ := cp.Cleanup(ctx, 1)
		if deleted != 2 {
			t.Errorf("expected 2 cps deleted (整 sid_old), got %d", deleted)
		}

		// sid_new 全保留
		ex, _ := cp.ExistsNS(ctx, "sid_new", "ns_a")
		if !ex {
			t.Error("sid_new/ns_a should be kept")
		}
		ex, _ = cp.ExistsNS(ctx, "sid_new", "ns_b")
		if !ex {
			t.Error("sid_new/ns_b should be kept")
		}

		// sid_old 全删
		ex, _ = cp.ExistsNS(ctx, "sid_old", "ns_a")
		if ex {
			t.Error("sid_old/ns_a should be deleted")
		}
	})
}

// testCleanupNodeStates Cleanup 出局的 sid，其 NodeStates 也必须读不到。
//
// 主路径（cp 整行删）天然没有遗漏可能；这条防的是**业务 backend 把 `node_states` 拆成
// 独立表**的形态——那时「删 cp 忘了删附属表」不会报错，只是留一堆孤儿行，且下次同
// sid 复用时可能被读回来（读到别人的产出）。
func testCleanupNodeStates(t *testing.T, factory CheckpointerFactory) {
	t.Run("Cleanup_Removes_NodeStates_Too", func(t *testing.T) {
		ctx := context.Background()
		cper := factory()
		src := cpWithNodeStates(t, "conf_cleanup_ns_graph", "sid-ns-src")

		// 两个 sid，只保留最新那个 ⟹ 带 memo 的这个出局
		older := src
		older.CreatedAt = 1
		if err := cper.SaveNS(ctx, "sid-old", "", older); err != nil {
			t.Fatalf("SaveNS old: %v", err)
		}
		newer := newCheckpoint(src.GraphID, compose_v2.CPStatusPending)
		newer.CreatedAt = 2
		if err := cper.SaveNS(ctx, "sid-new", "", newer); err != nil {
			t.Fatalf("SaveNS new: %v", err)
		}
		if _, err := cper.Cleanup(ctx, 1); err != nil {
			t.Fatalf("Cleanup: %v", err)
		}

		got, err := cper.LoadNS(ctx, "sid-old", "")
		if err != nil {
			t.Fatalf("LoadNS: %v", err)
		}
		if got != nil {
			t.Fatalf("出局的 sid 还能载入（NodeStates %d 条）——Cleanup 没删干净。\n"+
				"  backend 把 node_states 拆独立表时最容易漏：删 cp 行忘了删附属行，\n"+
				"  留下的孤儿行在同 sid 复用时可能被读回来（读到别人的产出）",
				len(got.NodeStates))
		}
	})
}
