package conformance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"crew-go/store"
)

// testNamespace 验证 namespace 校验 + 跨 namespace 隔离.
func testNamespace(t *testing.T, factory StoreFactory) {
	t.Run("NS_With_Dot_Should_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// 段含 "." 应失败 (防 ns 路径分隔符冲突)
		err := st.Put(ctx, store.Namespace{"valid", "has.dot"}, "k", "v")
		if err == nil {
			t.Error("namespace segment with dot should return error")
		}
		if !errors.Is(err, store.ErrInvalidNamespace) {
			t.Errorf("expected ErrInvalidNamespace, got %v", err)
		}
	})

	t.Run("NS_With_NUL_Byte_Should_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// audit 视角 1 L2 + store.go:71-73 godoc: NUL byte 是 joinNS 内部分隔符,
		// 业务输入含外部用户串时这是真实安全 bug — 不拒绝会 ns 注入
		err := st.Put(ctx, store.Namespace{"valid", "has\x00nul"}, "k", "v")
		if err == nil {
			t.Error("namespace segment with NUL byte should return error (安全协议)")
		}
		if !errors.Is(err, store.ErrInvalidNamespace) {
			t.Errorf("expected ErrInvalidNamespace, got %v", err)
		}
	})

	t.Run("NS_With_Empty_Segment_Should_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// 段为空字符串应失败
		err := st.Put(ctx, store.Namespace{"valid", "", "inner"}, "k", "v")
		if err == nil {
			t.Error("namespace with empty segment should return error")
		}
		if !errors.Is(err, store.ErrInvalidNamespace) {
			t.Errorf("expected ErrInvalidNamespace, got %v", err)
		}
	})

	t.Run("NS_Isolation_CrossNamespace_NotLeak", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		_ = st.Put(ctx, store.NS("u1"), "k", "u1_value")
		_ = st.Put(ctx, store.NS("u2"), "k", "u2_value")

		// u1 看不到 u2 的数据
		item1, _ := st.Get(ctx, store.NS("u1"), "k")
		item2, _ := st.Get(ctx, store.NS("u2"), "k")

		if item1 == nil || item2 == nil {
			t.Fatal("expected both items present")
		}
		if item1.Value == item2.Value {
			t.Errorf("expected isolated values across NS, got both %v", item1.Value)
		}
	})

	// audit iter-3 视角 3 H3: Store 同款 sibling prefix false-positive 测试 —
	// checkpointer 有 `DeleteSubtree_Sibling_Prefix_Not_Affected` 但 store 0 测.
	// 业务 PG/MySQL 用 `LIKE 'users.%'` 实现 Search ns prefix 会**误命中**
	// `users_admin` / `users_extra` (共字符前缀但非 NS 后代) → SaaS 跨租户数据
	// 泄露 (PG `LIKE` prefix 实现的经典坑).
	t.Run("NS_Sibling_Prefix_Not_Affected_In_Search", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// "users" 跟 "users_admin" / "users_other" 是 sibling (共字符前缀但非 NS
		// 祖先). 真后代是 "users/u1" / "users/u2" (用 / 分隔, crew-go joinNS
		// 实际用 \x00 内部存储).
		_ = st.Put(ctx, store.NS("users", "u1"), "k", "alice")
		_ = st.Put(ctx, store.NS("users", "u2"), "k", "bob")
		_ = st.Put(ctx, store.NS("users_admin"), "k", "secret_admin")
		_ = st.Put(ctx, store.NS("users_other"), "k", "leaked")

		// Search("users") 应只返 users/u1 + users/u2 共 2 条, 不返 users_admin /
		// users_other (共字符前缀 sibling)
		items, err := st.Search(ctx, store.NS("users"))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(items) != 2 {
			t.Errorf("expected 2 items under users/* (u1 + u2), got %d (业务 backend 可能 LIKE 'users%%' 误命中 sibling)",
				len(items))
		}
		// 验内容: 不能含 admin / leaked
		for _, item := range items {
			v, _ := item.Value.(string)
			if v == "secret_admin" || v == "leaked" {
				t.Errorf("Search NS('users') leaked sibling namespace data (value=%q) — SaaS 跨租户数据泄露", v)
			}
		}
	})

	// 不变量：namespace 与 key 逐字节就是身份——逐字节不同的两个就是两份数据，读、写、删、列
	// 都互不影响。
	//
	// 列用非二进制排序规则的后端会在这里静默串行：MySQL 的 utf8mb4_bin 是 PAD SPACE（比较时
	// 忽略尾随空格），MySQL 8 的默认排序规则 utf8mb4_0900_ai_ci 不区分大小写和重音、按规范
	// 等价比较组合字符——主键把 identityTwins 里的每一对当成同一行，后写的覆盖先写的、读一个
	// 拿到另一个的值、删一个带走另一个，全程 err == nil（sql 后端的 MySQL 建表因此用 VARBINARY）。
	// namespace 段或 key 来自外部输入时，这就是两个 namespace 互相覆盖、互相读到。
	t.Run("Identity_Is_Byte_Exact", func(t *testing.T) {
		for _, tw := range identityTwins {
			t.Run(tw.name, func(t *testing.T) {
				t.Run("namespace", func(t *testing.T) {
					st := factory()
					ctx := context.Background()
					nsA, nsB := store.NS("p", tw.a), store.NS("p", tw.b)
					if err := st.Put(ctx, nsA, "k", "A"); err != nil {
						t.Fatalf("Put(%q): %v", nsA, err)
					}
					if err := st.Put(ctx, nsB, "k", "B"); err != nil {
						t.Fatalf("Put(%q): %v", nsB, err)
					}
					for _, c := range []struct {
						ns   store.Namespace
						want string
					}{{nsA, "A"}, {nsB, "B"}} {
						it, err := st.Get(ctx, c.ns, "k")
						if err != nil {
							t.Fatalf("Get(%q): %v", c.ns, err)
						}
						if it == nil || it.Value != c.want {
							t.Errorf("Get(%q) 应拿到 %q，实际 %v——逐字节不同的两个 namespace 落进了同一行", c.ns, c.want, it)
						}
						items, err := st.Search(ctx, c.ns)
						if err != nil {
							t.Fatalf("Search(%q): %v", c.ns, err)
						}
						if len(items) != 1 || items[0].Value != c.want {
							t.Errorf("Search(%q) 应只返自己那 1 条 %q，实际 %d 条——读到了另一个 namespace", c.ns, c.want, len(items))
						}
					}
					// 只看两个都在；次序由 ListNamespaces_Sorted_By_String_Bytes 钉
					nss, err := st.ListNamespaces(ctx, store.WithListPrefix(store.NS("p")))
					if err != nil {
						t.Fatalf("ListNamespaces: %v", err)
					}
					if got, want := nsSet(nss), nsSet([]store.Namespace{nsA, nsB}); got != want {
						t.Errorf("ListNamespaces(p) 应包含且只包含 %s，实际 %s", want, got)
					}
					if err := st.Delete(ctx, nsB, "k"); err != nil {
						t.Fatalf("Delete(%q): %v", nsB, err)
					}
					if it, err := st.Get(ctx, nsA, "k"); err != nil || it == nil || it.Value != "A" {
						t.Errorf("删 %q 之后 Get(%q) 应仍拿到 %q，实际 %v（err %v）——删除越过了 namespace 边界", nsB, nsA, "A", it, err)
					}
				})

				t.Run("key", func(t *testing.T) {
					st := factory()
					ctx := context.Background()
					ns := store.NS("q")
					if err := st.Put(ctx, ns, tw.a, "A"); err != nil {
						t.Fatalf("Put(%q): %v", tw.a, err)
					}
					if err := st.Put(ctx, ns, tw.b, "B"); err != nil {
						t.Fatalf("Put(%q): %v", tw.b, err)
					}
					for _, c := range []struct{ key, want string }{{tw.a, "A"}, {tw.b, "B"}} {
						it, err := st.Get(ctx, ns, c.key)
						if err != nil {
							t.Fatalf("Get(%q): %v", c.key, err)
						}
						if it == nil || it.Value != c.want {
							t.Errorf("Get(%q) 应拿到 %q，实际 %v——逐字节不同的两个 key 落进了同一行", c.key, c.want, it)
						}
					}
					items, err := st.Search(ctx, ns)
					if err != nil {
						t.Fatalf("Search: %v", err)
					}
					keys := map[string]bool{}
					for _, it := range items {
						keys[it.Key] = true
					}
					if len(items) != 2 || !keys[tw.a] || !keys[tw.b] {
						t.Errorf("Search(q) 应返 %q 与 %q 两条，实际 %d 条", tw.a, tw.b, len(items))
					}
					if err := st.Delete(ctx, ns, tw.b); err != nil {
						t.Fatalf("Delete(%q): %v", tw.b, err)
					}
					if it, err := st.Get(ctx, ns, tw.a); err != nil || it == nil || it.Value != "A" {
						t.Errorf("删 %q 之后 Get(%q) 应仍拿到 %q，实际 %v（err %v）", tw.b, tw.a, "A", it, err)
					}
				})
			})
		}
	})

	t.Run("NS_Hierarchical_Search_Returns_All_Descendants", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// 写到不同层级
		_ = st.Put(ctx, store.NS("root", "a"), "k1", "v1")
		_ = st.Put(ctx, store.NS("root", "a", "deep"), "k2", "v2")
		_ = st.Put(ctx, store.NS("root", "b"), "k3", "v3")

		// Search root 应返全部 3 条 (含 root/a/deep 也算 root 后代)
		items, _ := st.Search(ctx, store.NS("root"))
		if len(items) != 3 {
			t.Errorf("expected 3 items under root/*, got %d", len(items))
		}

		// Search root/a 应返 2 条 (root/a + root/a/deep)
		items, _ = st.Search(ctx, store.NS("root", "a"))
		if len(items) != 2 {
			t.Errorf("expected 2 items under root/a/*, got %d", len(items))
		}
	})

	// **namespace 段里的模式元字符不得获得通配能力**。
	//
	// `Namespace.Validate()` 只拒 `.` 与 NUL——`_` `%` `*` `?` `[` 全是合法段字符。
	// 而 backend 普遍把段拼成一个串再做前缀匹配（SQL `LIKE`、Redis `SCAN MATCH`），
	// 那些字符在模式语言里是通配符：
	//
	//   - SQL LIKE：`_` 匹配任意单字符 ⟹ 前缀 `a_b` 的子树查询命中 `a.b.secret`
	//     （`_` 匹配了段分隔符本身）——**跨 namespace 读**，不是多返几条
	//   - Redis glob：`*` / `?` / `[` 同理 ⟹ `NS("a*")` 读到 `alpha` 的数据
	//
	// 两类都 `err == nil`，静默。namespace 是多租户隔离边界，所以这是越权不是瑕疵。
	//
	// ⚠️ 这跟上面 `NS_Sibling_Prefix_Not_Affected_In_Search` 是**两个不同的缺陷**：
	// sibling（`acme` vs `acmex`）靠「模式里带上分隔符」修；元字符靠**转义**修。
	// 只覆盖前者的话，业务自研 backend 能全绿通过 conformance 同时带着越权读。
	t.Run("NS_Pattern_Metachars_Are_Literal", func(t *testing.T) {
		cases := []struct {
			name     string
			attacker store.Namespace // 段含元字符，会越界的那个
			victim   store.Namespace // 不该被它读到
		}{
			// SQL LIKE：`_` 匹配任意单字符，含段分隔符
			{"下划线", store.NS("a_b"), store.NS("a", "b", "secret")},
			// SQL LIKE：`%` 匹配任意长
			{"百分号", store.NS("a%"), store.NS("alpha")},
			// Redis glob：`*` 匹配任意长
			{"星号", store.NS("g*"), store.NS("gamma")},
			// Redis glob：`?` 匹配单字符
			{"问号", store.NS("d?f"), store.NS("def")},
			// Redis glob：`[` 开字符类
			{"字符类", store.NS("h[ij]k"), store.NS("hik")},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				st := factory()
				ctx := context.Background()
				if err := st.Put(ctx, c.victim, "vk", "受害者私有"); err != nil {
					t.Fatalf("put victim: %v", err)
				}
				if err := st.Put(ctx, c.attacker, "ak", "自己的"); err != nil {
					t.Fatalf("put attacker: %v", err)
				}

				items, err := st.Search(ctx, c.attacker)
				if err != nil {
					t.Fatalf("Search(%v): %v", c.attacker, err)
				}
				// ① 不得越界读到别人
				for _, it := range items {
					if it.Namespace.String() == c.victim.String() {
						t.Errorf("越权：Search(%v) 读到了 %v 的数据——段里的元字符获得了通配能力",
							c.attacker, c.victim)
					}
				}
				// ② **正向断言**：自己的数据必须读得到（防「转义过头，谁都读不到」的退化——
				// 只有 ① 的话，一个恒返空的实现也能全绿）
				var sawOwn bool
				for _, it := range items {
					if it.Key == "ak" {
						sawOwn = true
					}
				}
				if !sawOwn {
					t.Errorf("Search(%v) 读不到自己的数据（转义过头）, got %d 条", c.attacker, len(items))
				}
			})
		}
	})

}

// identityTwins 是逐字节不同、但在常见的非二进制排序规则下相等的成对字符串——身份用例拿它
// 验「逐字节就是身份」。
var identityTwins = []struct{ name, a, b string }{
	{"尾随空格", "a ", "a"},           // PAD SPACE（MySQL utf8mb4_bin）
	{"大小写", "Review", "review"},   // 不区分大小写（MySQL 8 默认 utf8mb4_0900_ai_ci）
	{"重音", "résumé", "resume"},    // 不区分重音（同上）
	{"组合字符", "e\u0301", "\u00e9"}, // 规范等价：NFD 的 e + 组合尖音符 与 NFC 的 é
}

// nsSet 把 namespace 列表格式化成排好序的集合文本，供不关心次序的比较用。
func nsSet(nss []store.Namespace) string {
	out := make([]string, 0, len(nss))
	for _, ns := range nss {
		out = append(out, fmt.Sprintf("%q", []string(ns)))
	}
	sort.Strings(out)
	return fmt.Sprint(out)
}
