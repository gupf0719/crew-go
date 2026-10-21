package checkpointer_conformance

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// identityTwins 是逐字节不同、但在常见的非二进制排序规则下相等的成对字符串——身份用例拿它
// 验「(sid, ns) 逐字节就是身份」。
var identityTwins = []struct{ name, a, b string }{
	{"尾随空格", "s ", "s"},        // PAD SPACE（MySQL utf8mb4_bin）
	{"大小写", "Run1", "run1"},    // 不区分大小写（MySQL 8 默认 utf8mb4_0900_ai_ci）
	{"重音", "résumé", "resume"}, // 不区分重音（同上）
	{"组合字符", "é", "é"},        // 规范等价：NFD 的 e + 组合尖音符 与 NFC 的 é
}

// testKeyIdentity 验证 (sid, ns) 逐字节就是存档的身份。
//
// 逐字节不同的两个 sid（或两个 ns）是两份档，存、取、列、删都互不影响。键列用非二进制排序
// 规则的后端会在这里静默串档：MySQL 的 utf8mb4_bin 是 PAD SPACE（比较时忽略尾随空格），
// MySQL 8 的默认排序规则 utf8mb4_0900_ai_ci 不区分大小写和重音、按规范等价比较组合字符——主键
// 把 identityTwins 里的每一对当成同一行，后存的覆盖先存的、续跑载入别人的档、删一个会话连带
// 删掉另一个会话的档，全程 err == nil（sql 后端的 MySQL 建表因此用 VARBINARY）。
func testKeyIdentity(t *testing.T, factory CheckpointerFactory) {
	// 两份档同 GraphID，用 CreatedAt 区分——GraphID 不同的话撞行会先被冲突守卫拦成错误，
	// 看不到静默覆盖这个形态。
	saved := func(createdAt int64) *compose_v2.Checkpoint {
		c := newCheckpoint("g", compose_v2.CPStatusPending)
		c.CreatedAt = createdAt
		return c
	}
	mustLoad := func(t *testing.T, cp compose_v2.Checkpointer, sid, ns string, want int64) {
		t.Helper()
		got, err := cp.LoadNS(context.Background(), sid, ns)
		if err != nil {
			t.Fatalf("LoadNS(%q, %q): %v", sid, ns, err)
		}
		if got == nil {
			t.Fatalf("LoadNS(%q, %q) 返 nil——这份档不见了", sid, ns)
		}
		if got.CreatedAt != want {
			t.Errorf("LoadNS(%q, %q) 拿到 CreatedAt=%d，应为 %d——逐字节不同的两份档落进了同一行",
				sid, ns, got.CreatedAt, want)
		}
	}

	t.Run("SID_Is_Byte_Exact", func(t *testing.T) {
		for _, tw := range identityTwins {
			t.Run(tw.name, func(t *testing.T) {
				cp := factory()
				ctx := context.Background()
				if err := cp.SaveNS(ctx, tw.a, "n", saved(101)); err != nil {
					t.Fatalf("SaveNS(%q): %v", tw.a, err)
				}
				if err := cp.SaveNS(ctx, tw.b, "n", saved(202)); err != nil {
					t.Fatalf("SaveNS(%q): %v", tw.b, err)
				}
				mustLoad(t, cp, tw.a, "n", 101)
				mustLoad(t, cp, tw.b, "n", 202)

				for _, sid := range []string{tw.a, tw.b} {
					all, err := cp.LoadAllInSession(ctx, sid)
					if err != nil {
						t.Fatalf("LoadAllInSession(%q): %v", sid, err)
					}
					if len(all) != 1 {
						t.Errorf("LoadAllInSession(%q) 应只有自己那 1 份，实际 %d 份", sid, len(all))
					}
					pending, err := cp.ListPending(ctx, sid)
					if err != nil {
						t.Fatalf("ListPending(%q): %v", sid, err)
					}
					if len(pending) != 1 {
						t.Errorf("ListPending(%q) 应只有自己那 1 份，实际 %d 份", sid, len(pending))
					}
				}

				// 删整个会话 a，会话 b 的档必须还在
				if _, err := cp.DeleteSubtree(ctx, tw.a, ""); err != nil {
					t.Fatalf("DeleteSubtree(%q): %v", tw.a, err)
				}
				mustLoad(t, cp, tw.b, "n", 202)
			})
		}
	})

	t.Run("NS_Is_Byte_Exact", func(t *testing.T) {
		for _, tw := range identityTwins {
			t.Run(tw.name, func(t *testing.T) {
				cp := factory()
				ctx := context.Background()
				if err := cp.SaveNS(ctx, "s", tw.a, saved(101)); err != nil {
					t.Fatalf("SaveNS(%q): %v", tw.a, err)
				}
				if err := cp.SaveNS(ctx, "s", tw.b, saved(202)); err != nil {
					t.Fatalf("SaveNS(%q): %v", tw.b, err)
				}
				mustLoad(t, cp, "s", tw.a, 101)
				mustLoad(t, cp, "s", tw.b, 202)

				all, err := cp.LoadAllInSession(ctx, "s")
				if err != nil {
					t.Fatalf("LoadAllInSession: %v", err)
				}
				if len(all) != 2 {
					t.Errorf("LoadAllInSession(s) 应有 2 份（%q 与 %q），实际 %d 份", tw.a, tw.b, len(all))
				}

				// b 不在 a 的子树里，删 a 不能带走它
				n, err := cp.DeleteSubtree(ctx, "s", tw.a)
				if err != nil {
					t.Fatalf("DeleteSubtree(%q): %v", tw.a, err)
				}
				if n != 1 {
					t.Errorf("DeleteSubtree(s, %q) 应删 1 份，实际 %d 份", tw.a, n)
				}
				mustLoad(t, cp, "s", tw.b, 202)
			})
		}
	})
}
