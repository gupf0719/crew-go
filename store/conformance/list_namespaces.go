package conformance

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/store"
)

// testListNamespaces 验证 ListNamespaces 的 prefix/suffix/maxDepth 过滤.
func testListNamespaces(t *testing.T, factory StoreFactory) {
	t.Run("ListNamespaces_Returns_All_Used", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		_ = st.Put(ctx, store.NS("users", "u1"), "k", 1)
		_ = st.Put(ctx, store.NS("users", "u2"), "k", 2)
		_ = st.Put(ctx, store.NS("admins"), "k", 3)

		nss, err := st.ListNamespaces(ctx)
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		// 至少 3 个 distinct namespace
		if len(nss) < 3 {
			t.Errorf("expected ≥3 namespaces, got %d: %v", len(nss), nss)
		}
	})

	t.Run("ListNamespaces_WithPrefix_Filter", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		prefix := store.NS("users", "acme")
		_ = st.Put(ctx, store.NS("users", "acme", "u1"), "k", 1)
		_ = st.Put(ctx, store.NS("users", "acme", "u2"), "k", 2)
		_ = st.Put(ctx, store.NS("admins"), "k", 3)
		// **深层 sibling**：跟 prefix 只差最后一段的一个字符。拼串前缀匹配的后端
		// （PG `LIKE 'users/acme%'` / Redis `KEYS users:acme*`）会把它一起捞回来。
		_ = st.Put(ctx, store.NS("users", "acmex", "u9"), "k", 4)
		// 首段相同但第二段不同——同上，只比第一段的实现看不出区别
		_ = st.Put(ctx, store.NS("users", "other", "u8"), "k", 5)

		nss, err := st.ListNamespaces(ctx, store.WithListPrefix(prefix))
		if err != nil {
			t.Fatalf("ListNamespaces WithListPrefix: %v", err)
		}
		// **按段比，不是按首段比、更不是按字符串前缀比。**
		//
		// ⚠️ 这条断言一度写成 `ns[0] != "users"`，那只抓得到首段不同的（admins），
		// **抓不到深层 sibling**——而深层 sibling 正是拼串前缀匹配后端的经典坑
		// （Search 那侧早有专门用例钉它，这侧漏了）。放过的后果不是「多几条」：
		// 上层按写入者分片取长期记忆时，这些 ns 会被当成本 agent 的写入者去查，
		// **别人的内容进 prompt**。
		for _, ns := range nss {
			if !prefix.IsPrefixOf(ns) {
				t.Errorf("WithListPrefix(%v) 返回了不在该前缀之下的 %v——"+
					"前缀过滤是后端的义务，按段比不是按字符串比", prefix, ns)
			}
		}
		// **正向计数**——上面那条是否定式断言，**空列表下循环体一次都不跑**。
		//
		// 「prefix 分支没写完 / 索引没建 / 翻译成了永假的 WHERE」是自研后端最常见的形态，
		// 而它们一律返空。实测一个「带 prefix 就返空」的后端能**通过全套 conformance**，
		// 接进框架后让上层退化成只查基准 ns。Search 那侧早就正负两向都钉（用精确计数），
		// 这侧只钉了「不该多」没钉「不该少」。
		if len(nss) != 2 {
			t.Errorf("WithListPrefix(%v) 应返回恰好 2 个（u1/u2），实际 %d 个：%v——"+
				"返空同样满足「没有越界项」，但它不是过滤，是没实现", prefix, len(nss), nss)
		}
	})

	t.Run("ListNamespaces_WithListLimit_Caps", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		for i := 0; i < 12; i++ {
			_ = st.Put(ctx, store.NS("cap", fmt.Sprintf("n%02d", i)), "k", i)
		}
		nss, err := st.ListNamespaces(ctx, store.WithListPrefix(store.NS("cap")), store.WithListLimit(5))
		if err != nil {
			t.Fatalf("ListNamespaces WithListLimit: %v", err)
		}
		// 上层拿这个 limit 当**成本的界**用（对返回的每个 ns 各发一次查询）。后端不遵守
		// 时那个界就不存在——实测一个无视它的后端能让上层发出 20001 次调用。
		if len(nss) > 5 {
			t.Errorf("WithListLimit(5) 返回了 %d 个——调用方拿它当成本上界用，"+
				"不遵守等于那个界不存在", len(nss))
		}
	})

	// 不传 WithListLimit 时默认最多 100 个（同 Search 的默认 10）：调用方不传 limit 时拿它当成本上界，后端「不限」的话
	// 那个界不存在，跨后端行为也会漂。
	t.Run("ListNamespaces_Default_Limit_Is_100", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		for i := 0; i < 105; i++ {
			_ = st.Put(ctx, store.NS("dflt", fmt.Sprintf("n%03d", i)), "k", i)
		}
		nss, err := st.ListNamespaces(ctx, store.WithListPrefix(store.NS("dflt")))
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		if len(nss) != 100 {
			t.Errorf("不传 WithListLimit 应默认返回 100 个（写了 105 个），got %d", len(nss))
		}
	})

	// 不变量：结果按 Namespace.String() 的字节序排列，分页窗口是这个次序里的连续一段。
	//
	// 把排序交给数据库的后端，次序跟着列的排序规则走：PG 库的默认规则常是 en-US 一类
	// 的语言规则（大小写在第一层不区分、标点先于字母），ICU en-US 的库里 "o.a" 排在
	// "o.B" 前面、"o._c" 排在最前，与字节序相反——同一个 (offset, limit) 在不同后端上
	// 拿到的是不同的几项。夹具用大小写、'_'（0x5F，介于大写与小写字母之间）和 '-'
	// （0x2D，小于分隔符 '.'）把字节序与语言规则区分开。
	t.Run("ListNamespaces_NegativeOffset_Returns_ErrNegativeOffset", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		_ = st.Put(ctx, store.NS("neg_list"), "a", 1)
		for _, opts := range [][]store.ListOption{
			{store.WithListOffset(-1)},
			{store.WithListOffset(-1), store.WithListMaxDepth(1)}, // 带 maxDepth 的那条路（有的后端在 Go 里分页）
		} {
			if _, err := st.ListNamespaces(ctx, opts...); !errors.Is(err, store.ErrNegativeOffset) {
				t.Errorf("ListNamespaces(offset=-1) err = %v, want store.ErrNegativeOffset", err)
			}
		}
	})

	t.Run("ListNamespaces_Sorted_By_String_Bytes", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		want := []store.Namespace{
			store.NS("o", "B"),
			store.NS("o", "Z9"),
			store.NS("o", "_c"),
			store.NS("o", "a"),
			store.NS("o", "a-b"),
			store.NS("o", "a", "x"),
		}
		// 交错写：写入次序既不是字节序也不是它的倒序，「按首次写入正序」「按最近写入倒序」返回
		// 的实现都对不上（只倒着写的话，后一种恰好对上）。
		for _, i := range []int{2, 5, 0, 3, 1, 4} {
			if err := st.Put(ctx, want[i], "k", i); err != nil {
				t.Fatalf("Put(%q): %v", want[i], err)
			}
		}
		prefix := store.WithListPrefix(store.NS("o"))
		full, err := st.ListNamespaces(ctx, prefix)
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		if !equalNamespaces(full, want) {
			t.Errorf("ListNamespaces 应按 String() 字节序返回 %q，实际 %q", want, full)
		}
		for offset := 0; offset <= len(want)+1; offset++ {
			for limit := 1; limit <= len(want)+1; limit++ {
				opts := []store.ListOption{prefix, store.WithListLimit(limit)}
				if offset > 0 {
					opts = append(opts, store.WithListOffset(offset))
				}
				got, err := st.ListNamespaces(ctx, opts...)
				if err != nil {
					t.Fatalf("ListNamespaces offset=%d limit=%d: %v", offset, limit, err)
				}
				lo, hi := min(offset, len(want)), min(offset+limit, len(want))
				if !equalNamespaces(got, want[lo:hi]) {
					t.Errorf("offset=%d limit=%d 应返回 %q，实际 %q——分页窗口要取在字节序上", offset, limit, want[lo:hi], got)
				}
			}
		}
	})

	t.Run("ListNamespaces_WithMaxDepth_Truncates", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		_ = st.Put(ctx, store.NS("a", "b", "c"), "k", 1)
		_ = st.Put(ctx, store.NS("a", "b", "d"), "k", 2)

		nss, err := st.ListNamespaces(ctx, store.WithListMaxDepth(2))
		if err != nil {
			t.Fatalf("ListNamespaces WithListMaxDepth(2): %v", err)
		}
		// 截断到 2 段, 所以 a/b/c + a/b/d 都变 a/b — dedup 后只 1 个
		for _, ns := range nss {
			if len(ns) > 2 {
				t.Errorf("expected max depth 2, got depth %d: %v", len(ns), ns)
			}
		}
		// **截断出来的那一层必须真的出现**——只断言 "深度不超过 2" 的话, 把 maxDepth
		// 实现成「**过滤掉**更深的」而不是「**截断**成更浅的」的后端会返回空列表, 照样
		// 通过。两种语义对上层是天壤之别: 框架用它列「基准 ns 的直接子层」(记忆注入按
		// 写入者分配配额), 过滤语义下那个列表恒空 ⟹ 整套分配静默退化, 不报错。
		var found bool
		for _, ns := range nss {
			if len(ns) == 2 && ns[0] == "a" && ns[1] == "b" {
				found = true
			}
		}
		if !found {
			t.Errorf("WithListMaxDepth 是**截断**不是过滤: a/b/c 与 a/b/d 应截断成 a/b 出现在结果里, 实际 %v", nss)
		}
	})

	// 不变量：带 maxDepth 时，任何 (offset, limit) 的结果都等于「截断、去重、按
	// Namespace.String() 字节序排好」的完整列表的 [offset, offset+limit) 那一段——
	// 分页取在截断之后的列表上，不是取在原始的完整 ns 上。
	//
	// 先对原始行分页、再截断去重的后端三样都错，且全程 err == nil：一页里被截成同一个
	// 前缀的几行只剩一条（少返）；下一页从原始行接着数，截出来的又是上一页那个（跨页
	// 重复）；截断不保序（见下面 m/w2-x），照原始行的次序边读边截，结果是乱序的。
	// 框架按写入者扇出取长期记忆时正是这样调它的：前缀 = 基准 ns、maxDepth = 深一层、
	// limit = 写入者上限——前缀下的完整 ns 一多，排在后面的写入者就被挤出列表。
	t.Run("ListNamespaces_MaxDepth_Pages_Over_Truncated_List", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		for _, ns := range []store.Namespace{
			store.NS("m"),       // 前缀自身
			store.NS("m", "w1"), // maxDepth=2 时要和下一行截出来的 m/w1 去重
			store.NS("m", "w1", "a"),
			store.NS("m", "w2", "a"), // m/w2 本身没有行，maxDepth=2 时只能由截断产生
			store.NS("m", "w2", "b", "c"),
			// 拼成串之后 "m.w2-x" 排在 "m.w2.a" 前面（'-' 是 0x2D、'.' 是 0x2E），截到两段
			// 之后 "m.w2" 却排在 "m.w2-x" 前面：截断不保序。
			store.NS("m", "w2-x"),
			store.NS("m", "w3", "a"),
			store.NS("n", "w1", "a"), // 前缀之外
			store.NS("mm", "w1"),     // 字符前缀相同、段不同，也在前缀之外
		} {
			if err := st.Put(ctx, ns, "k", 1); err != nil {
				t.Fatalf("Put(%q): %v", ns, err)
			}
		}

		for _, tc := range []struct {
			depth int
			want  []store.Namespace
		}{
			// 不深于前缀：全部截成前缀自身
			{1, []store.Namespace{store.NS("m")}},
			// 框架记忆扇出的形状：前缀 + 深一层
			{2, []store.Namespace{
				store.NS("m"), store.NS("m", "w1"), store.NS("m", "w2"), store.NS("m", "w2-x"), store.NS("m", "w3"),
			}},
			{3, []store.Namespace{
				store.NS("m"), store.NS("m", "w1"), store.NS("m", "w1", "a"), store.NS("m", "w2-x"),
				store.NS("m", "w2", "a"), store.NS("m", "w2", "b"), store.NS("m", "w3", "a"),
			}},
			// 不浅于最深的 ns：等于不截断
			{4, []store.Namespace{
				store.NS("m"), store.NS("m", "w1"), store.NS("m", "w1", "a"), store.NS("m", "w2-x"),
				store.NS("m", "w2", "a"), store.NS("m", "w2", "b", "c"), store.NS("m", "w3", "a"),
			}},
		} {
			base := []store.ListOption{store.WithListPrefix(store.NS("m")), store.WithListMaxDepth(tc.depth)}

			// 不传 limit / offset：默认上限 100 装得下，完整列表本身先得对。
			full, err := st.ListNamespaces(ctx, base...)
			if err != nil {
				t.Fatalf("ListNamespaces maxDepth=%d: %v", tc.depth, err)
			}
			if !equalNamespaces(full, tc.want) {
				t.Errorf("WithListMaxDepth(%d) 的完整列表应为 %q（截断、去重后按 String() 字节序排），实际 %q",
					tc.depth, tc.want, full)
			}

			// 每一个窗口都对一遍；limit / offset 为 0 表示不传那个选项（limit 走默认 100）。
			for offset := 0; offset <= len(tc.want)+1; offset++ {
				for limit := 0; limit <= len(tc.want)+1; limit++ {
					opts := append([]store.ListOption(nil), base...)
					if limit > 0 {
						opts = append(opts, store.WithListLimit(limit))
					}
					if offset > 0 {
						opts = append(opts, store.WithListOffset(offset))
					}
					got, err := st.ListNamespaces(ctx, opts...)
					if err != nil {
						t.Fatalf("ListNamespaces maxDepth=%d offset=%d limit=%d: %v", tc.depth, offset, limit, err)
					}
					lo, hi := min(offset, len(tc.want)), len(tc.want)
					if limit > 0 {
						hi = min(offset+limit, len(tc.want))
					}
					if !equalNamespaces(got, tc.want[lo:hi]) {
						t.Errorf("WithListMaxDepth(%d) offset=%d limit=%d 应返回 %q，实际 %q——"+
							"分页要取在截断、去重、排好序之后的列表上；先对原始 ns 分页再截断会少返、跨页重复",
							tc.depth, offset, limit, tc.want[lo:hi], got)
					}
				}
			}
		}
	})
}

// equalNamespaces 逐项、逐段比较两个 namespace 列表（顺序敏感）。
func equalNamespaces(got, want []store.Namespace) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			return false
		}
	}
	return true
}
