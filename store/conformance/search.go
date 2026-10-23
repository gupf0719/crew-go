package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"crew-go/store"
)

// testSearch 验证 Search 在 namespace prefix 下的检索 + filter + 分页.
func testSearch(t *testing.T, factory StoreFactory) {
	t.Run("Search_ByNamespacePrefix_Returns_All_Matching", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// 写 3 条到 users/u1, 2 条到 users/u2, 1 条到 unrelated
		_ = st.Put(ctx, store.NS("users", "u1"), "k1", "v1")
		_ = st.Put(ctx, store.NS("users", "u1"), "k2", "v2")
		_ = st.Put(ctx, store.NS("users", "u1"), "k3", "v3")
		_ = st.Put(ctx, store.NS("users", "u2"), "k1", "u2_v1")
		_ = st.Put(ctx, store.NS("users", "u2"), "k2", "u2_v2")
		_ = st.Put(ctx, store.NS("unrelated"), "k", "x")

		// prefix=users 应返 5 条 (u1 + u2 全部)
		items, err := st.Search(ctx, store.NS("users"))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(items) != 5 {
			t.Errorf("expected 5 items under users/*, got %d", len(items))
		}

		// prefix=users/u1 应返 3 条
		items, err = st.Search(ctx, store.NS("users", "u1"))
		if err != nil {
			t.Fatalf("Search u1: %v", err)
		}
		if len(items) != 3 {
			t.Errorf("expected 3 items under users/u1, got %d", len(items))
		}
	})

	// Search 是**前缀递归**的（上一条已经钉了：查 users 返回 users/u1 + users/u2 全部），
	// 所以每一条结果都得说清自己**到底在哪个 namespace**——那个字段是调用方唯一能区分
	// 「u1 的 k1」和「u2 的 k1」的东西。
	//
	// ⚠️ 这条一度整个没人测，而框架里有承重的消费者：长期记忆注入按写入者分片各查一遍，
	// 用 `(Namespace, Key)` 去重。两种都很自然的后端实现各自静默坏一个方向，**都不报错**：
	//
	//   - 不填 Namespace ⟹ 不同写入者的同 key 记忆被当成同一条，注入时静默少一半
	//   - 填成「查询用的那个前缀」而非条目自身 ⟹ 去重失效，同一条记忆重复注入、prompt 膨胀
	//
	// Get 那边早就钉了 `item.Namespace.Equal(ns)`，Search 这边漏了——而恰恰是 Search
	// 会跨 namespace 返回，这个字段在 Get 上几乎没有信息量、在 Search 上才承重。
	t.Run("Search_Item_Namespace_Is_Item_Own_Not_Query_Prefix", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		_ = st.Put(ctx, store.NS("users", "u1"), "k", "u1 的值")
		_ = st.Put(ctx, store.NS("users", "u2"), "k", "u2 的值")

		items, err := st.Search(ctx, store.NS("users"))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("fixture 没搭对：两个子 namespace 各一条，应返 2 条，实际 %d", len(items))
		}
		seen := map[string]bool{}
		for _, it := range items {
			if it.Namespace.String() == "users" {
				t.Errorf("key=%q 的 Namespace 是查询用的前缀 %q，不是它自己的——"+
					"调用方按 (Namespace,Key) 去重时两条会合并成一条",
					it.Key, it.Namespace.String())
				continue
			}
			if len(it.Namespace) != 2 {
				t.Errorf("key=%q 的 Namespace=%v，期望是条目自身的两段路径（users/uN）",
					it.Key, it.Namespace)
				continue
			}
			seen[it.Namespace.String()] = true
		}
		if !seen["users.u1"] || !seen["users.u2"] {
			t.Errorf("两条应分别落在 users/u1 和 users/u2 上，实际认出 %v", seen)
		}
	})

	t.Run("Search_Empty_Namespace_Returns_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		_, err := st.Search(ctx, store.Namespace{})
		if err == nil {
			t.Error("Search with empty namespace should return error (禁全店扫)")
		}
		if !errors.Is(err, store.ErrEmptyNamespace) && !errors.Is(err, store.ErrInvalidNamespace) {
			t.Errorf("expected ErrEmptyNamespace or ErrInvalidNamespace, got %v", err)
		}
	})

	t.Run("Search_WithLimit_Caps_Result_Count", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("limited")

		// 写 5 条 key=a/b/c/d/e
		for i := 0; i < 5; i++ {
			_ = st.Put(ctx, ns, string(rune('a'+i)), i)
		}

		// limit=2 — 加 stable order 联合验证 (audit 视角 2 H2: 业务 backend 返
		// [d,e] 跳过 a/b 也 PASS 是 5 类炸弹 #1)
		items, err := st.Search(ctx, ns, store.WithLimit(2))
		if err != nil {
			t.Fatalf("Search limit=2: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("expected 2 items with limit=2, got %d", len(items))
		}
		// limit + stable order 联合验证：**必须是全序的 head N，不能是任意 N 条**
		// （业务 backend 返 [d,e] 跳过前两条也 PASS 是 fixture 陷阱 #1）。
		//
		// ⚠️ 这里不写死具体是哪两条 key——顺序契约是**时新优先**（见
		// Search_StableOrder_RecentFirst），而这条用例验的是「limit 截的是 head 不是
		// 随机」，两件事分开。写死 key 会让这条用例跟着排序契约一起漂。
		full, err := st.Search(ctx, ns)
		if err != nil {
			t.Fatalf("Search 全量: %v", err)
		}
		if len(full) < 2 {
			t.Fatalf("fixture 前提破了：全量应有 5 条，实际 %d", len(full))
		}
		if items[0].Key != full[0].Key || items[1].Key != full[1].Key {
			t.Errorf("limit=2 应返全序的前 2 条 [%s,%s]，实际 [%s,%s]"+
				"（业务 backend 把 limit 实现成倒序/随机取时会红）",
				full[0].Key, full[1].Key, items[0].Key, items[1].Key)
		}
	})

	t.Run("Search_Default_Limit_Is_10", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("defaults")

		// 写 15 条, 不传 limit
		for i := 0; i < 15; i++ {
			_ = st.Put(ctx, ns, fmt.Sprintf("k_%02d", i), i)
		}

		items, err := st.Search(ctx, ns) // 不传 WithLimit
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		// crew-go 协议 (memory.go:172): 默认 limit=10, 业务 backend 不可
		// "不限"返 15 条 (跨 backend 行为漂移)
		if len(items) > 10 {
			t.Errorf("expected default limit ≤ 10, got %d (业务 backend 违反默认 limit 协议)",
				len(items))
		}
	})

	t.Run("Search_WithOffset_Skips_First_N", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("offset")

		for i := 0; i < 5; i++ {
			_ = st.Put(ctx, ns, string(rune('a'+i)), i)
		}

		// offset=2 应跳过前 2 条, 返剩余
		items, err := st.Search(ctx, ns, store.WithOffset(2))
		if err != nil {
			t.Fatalf("Search offset=2: %v", err)
		}
		if len(items) > 3 {
			t.Errorf("expected at most 3 items with offset=2 over 5, got %d", len(items))
		}
	})

	t.Run("Search_NegativeOffset_Returns_ErrNegativeOffset", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("neg_offset")
		_ = st.Put(ctx, ns, "a", 1)
		// 负 offset 是调用方算错了：入口报错（不 panic、不当 0、不透传给数据库各报各的）
		if _, err := st.Search(ctx, ns, store.WithOffset(-1)); !errors.Is(err, store.ErrNegativeOffset) {
			t.Errorf("Search(offset=-1) err = %v, want store.ErrNegativeOffset", err)
		}
	})

	t.Run("Search_WithFilter_ExactMatch", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("filtered")

		_ = st.Put(ctx, ns, "a", map[string]any{"tag": "important"})
		_ = st.Put(ctx, ns, "b", map[string]any{"tag": "normal"})
		_ = st.Put(ctx, ns, "c", map[string]any{"tag": "important"})

		items, err := st.Search(ctx, ns, store.WithFilter(map[string]any{"tag": "important"}))
		if err != nil {
			t.Fatalf("Search filter: %v", err)
		}
		if len(items) != 2 {
			t.Errorf("expected 2 items with tag=important, got %d", len(items))
		}
	})

	t.Run("Search_WithFilter_ComparisonOps", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("ops_test")

		// 写 3 条 age=10/50/100, 让 6 个比较 op 各能区分
		_ = st.Put(ctx, ns, "young", map[string]any{"age": 10})
		_ = st.Put(ctx, ns, "mid", map[string]any{"age": 50})
		_ = st.Put(ctx, ns, "old", map[string]any{"age": 100})

		cases := []struct {
			op   string
			val  int
			want int // 期望 match 数
			desc string
		}{
			{"$eq", 50, 1, "$eq 50 只匹配 mid"},
			{"$ne", 50, 2, "$ne 50 匹配 young + old"},
			{"$gt", 50, 1, "$gt 50 只匹配 old"},
			{"$gte", 50, 2, "$gte 50 匹配 mid + old"},
			{"$lt", 50, 1, "$lt 50 只匹配 young"},
			{"$lte", 50, 2, "$lte 50 匹配 young + mid"},
		}
		for _, c := range cases {
			t.Run(c.op, func(t *testing.T) {
				items, err := st.Search(ctx, ns, store.WithFilter(map[string]any{
					"age": map[string]any{c.op: c.val},
				}))
				if err != nil {
					t.Fatalf("Search %s: %v", c.op, err)
				}
				if len(items) != c.want {
					t.Errorf("%s: expected %d matches (%s), got %d", c.op, c.want, c.desc, len(items))
				}
			})
		}
	})

	// 范围 op 的 filter 值必须是数值——这是调用方手上就有的信息，静态判定：**空 namespace
	// 也要报**（等扫到第一行才报的后端在空 ns 上会静默放过，跨后端就漂了）。
	t.Run("Search_RangeOp_NonNumericFilterValue_Returns_ErrFilterTypeMismatch", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("typemis")

		for _, v := range []any{"abc", "10", true, nil, []any{1}, map[string]any{"x": 1}} {
			_, err := st.Search(ctx, ns, store.WithFilter(map[string]any{
				"field": map[string]any{"$gt": v},
			}))
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("$gt %T(%v) on empty ns: expected ErrFilterTypeMismatch, got %v", v, v, err)
			}
		}
		// 有数值行也一样报——判据是 filter 值本身，不是「能不能比」
		_ = st.Put(ctx, ns, "n", map[string]any{"field": 50})
		for _, op := range []string{"$gt", "$gte", "$lt", "$lte"} {
			_, err := st.Search(ctx, ns, store.WithFilter(map[string]any{
				"field": map[string]any{op: "10"}, // 数字字符串也不是数
			}))
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("%s \"10\": expected ErrFilterTypeMismatch, got %v", op, err)
			}
		}
	})

	// type bracketing：存的值类型对不上就是「这一行不命中」，不是错误——一条异构行不能
	// 毒化整个 Search。fixture 必须异构（N=2 以上），同一字段一行数、一行串、一行数字串、
	// 一行 bool、一行 null、一行缺失、一行数组、一行对象。
	//
	// ⚠️ `$lt 10` 那条专门钉三种「反命中」形态：把非数值文本 CAST 成 0（LangGraph SQLite
	// 后端的写法）、把 JSON true 当成 1（SQLite `json_extract`）、把 JSON null 排在所有数之下
	// （MySQL 类型优先级）——三种在裸 SQL 下推下都会让 `$lt 10` 多返回行。
	t.Run("Search_RangeOp_TypeBracketing_SkipsNonNumericRows", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("bracket")
		put := func(k string, v map[string]any) {
			if err := st.Put(ctx, ns, k, v); err != nil {
				t.Fatalf("Put %s: %v", k, err)
			}
		}
		put("str", map[string]any{"f": "string_val"})
		put("numstr", map[string]any{"f": "9"})
		put("bool", map[string]any{"f": true})
		put("null", map[string]any{"f": nil})
		put("missing", map[string]any{"other": 1})
		put("arr", map[string]any{"f": []any{1, 2}})
		put("obj", map[string]any{"f": map[string]any{"x": 1}})
		put("n5", map[string]any{"f": 5})
		put("n50", map[string]any{"f": 50})
		put("big", map[string]any{"f": json.Number("9007199254740993")}) // MySQL 把它标成 UNSIGNED INTEGER——守卫列表漏它就丢行

		cases := []struct {
			op   string
			val  any
			want []string
		}{
			{"$gt", 10, []string{"n50", "big"}},
			{"$gte", 50, []string{"n50", "big"}},
			{"$lt", 10, []string{"n5"}},
			{"$lte", 5, []string{"n5"}},
			{"$gt", 0, []string{"n5", "n50", "big"}}, // true 不是 1，"9" 不是 9
			{"$lt", 100, []string{"n5", "n50"}},

			// ⚠️ **阈值必须贴着 2^53，上面那些 10/50/0/100 不承重**：客户端过滤若把
			// 数值统一转 float64，9007199254740993 会被舍成 ...992——于是 `$gt ...992`
			// 漏命中、`$lte ...992` 反命中，两个方向都错，而三个 SQL 引擎（PG numeric /
			// MySQL BIGINT / SQLite INTEGER）都算得对。阈值离 2^53 远时舍入后答案不变，
			// 有损实现和精确实现**都能过**，那条 fixture 对精度这一维是盲的。
			{"$gt", int64(9007199254740992), []string{"big"}},
			{"$gte", int64(9007199254740993), []string{"big"}},
			{"$lte", int64(9007199254740992), []string{"n5", "n50"}},
			{"$lt", int64(9007199254740993), []string{"n5", "n50"}},
		}
		for _, c := range cases {
			t.Run(fmt.Sprintf("%s_%v", c.op, c.val), func(t *testing.T) {
				items, err := st.Search(ctx, ns, store.WithLimit(100), store.WithFilter(map[string]any{
					"f": map[string]any{c.op: c.val},
				}))
				if err != nil {
					t.Fatalf("%s %v: expected no error (type mismatch is a non-match, not an error), got %v", c.op, c.val, err)
				}
				assertKeys(t, items, c.want)
			})
		}
	})

	// `$eq` / `$ne` / 直值：JSON 语义相等，类型严格——数值互通（10 == 10.0）、bool 不是 1、
	// 数字字符串不是数、null 只等于 null、数组逐元素相等（**不是包含**）、对象逐键相等；
	// `$ne` = 字段存在且不等（JSON null 算存在，缺失不算）。
	//
	// ⚠️ 数组用字符串元素：JSON 往返后数值元素的 Go 类型会变（float64 / json.Number），
	// 这条用例钉的是「相等 vs 包含」，不让元素类型的另一维混进来。
	t.Run("Search_Eq_Ne_JSONEquality_TypeStrict", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("eqne")
		put := func(k string, v map[string]any) {
			if err := st.Put(ctx, ns, k, v); err != nil {
				t.Fatalf("Put %s: %v", k, err)
			}
		}
		put("i10", map[string]any{"f": 10})
		put("i1", map[string]any{"f": 1}) // 与 btrue 互为对照：bool 与 1 两个方向都不相等
		put("f10", map[string]any{"f": json.Number("10.0")})
		put("s10", map[string]any{"f": "10"})
		put("btrue", map[string]any{"f": true})
		put("null", map[string]any{"f": nil})
		put("missing", map[string]any{"g": 1})
		put("tags", map[string]any{"f": []any{"a", "b"}})
		put("obj", map[string]any{"f": map[string]any{"a": "1", "b": "2"}})

		cases := []struct {
			name   string
			filter map[string]any
			want   []string
		}{
			{"eq_10_numeric_equality", map[string]any{"f": map[string]any{"$eq": 10}}, []string{"i10", "f10"}},
			{"direct_10_same_as_eq", map[string]any{"f": 10}, []string{"i10", "f10"}},
			{"direct_string_10_only_string_row", map[string]any{"f": "10"}, []string{"s10"}},
			{"direct_true_is_not_1", map[string]any{"f": true}, []string{"btrue"}},
			{"direct_1_is_not_true", map[string]any{"f": 1}, []string{"i1"}},
			{"direct_nil_only_json_null_row", map[string]any{"f": nil}, []string{"null"}},
			{"array_equality_not_containment", map[string]any{"f": []any{"a"}}, nil},
			{"array_equal", map[string]any{"f": []any{"a", "b"}}, []string{"tags"}},
			{"eq_object_equal_key_order_free", map[string]any{"f": map[string]any{"$eq": map[string]any{"b": "2", "a": "1"}}}, []string{"obj"}},
			{"ne_10_exists_and_not_equal", map[string]any{"f": map[string]any{"$ne": 10}}, []string{"i1", "s10", "btrue", "null", "tags", "obj"}},
			{"ne_nil", map[string]any{"f": map[string]any{"$ne": nil}}, []string{"i10", "i1", "f10", "s10", "btrue", "tags", "obj"}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				items, err := st.Search(ctx, ns, store.WithLimit(100), store.WithFilter(c.filter))
				if err != nil {
					t.Fatalf("Search %v: %v", c.filter, err)
				}
				assertKeys(t, items, c.want)
			})
		}
	})

	// Put 的值是具体 Go 类型（map[string]string、struct）时，filter 按它的 JSON 形状匹配：字段照常命中，对象相等不看
	// struct 字段的声明序。曾经内存后端原样存业务的值、只认 map[string]any，`map[string]string{"z":"1"}` 配 filter
	// `{"z":"1"}` 在内存后端上 0 条、在 SQLite 上 1 条；struct 字段按声明序编码，SQLite 按 JSON 文本比对象时跟 filter
	// 的 map（键排序）对不上——两种都 `err == nil`。
	t.Run("Search_WithFilter_ConcreteGoTypes_MatchByJSONShape", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("gotypes")
		type meta struct {
			Z int
			A int
		}
		type memo struct {
			Kind string `json:"kind"`
			Meta meta   `json:"meta"`
		}
		if err := st.Put(ctx, ns, "ss", map[string]string{"z": "1"}); err != nil {
			t.Fatalf("Put ss: %v", err)
		}
		if err := st.Put(ctx, ns, "st", memo{Kind: "pref", Meta: meta{Z: 1, A: 2}}); err != nil {
			t.Fatalf("Put st: %v", err)
		}
		cases := []struct {
			name   string
			filter map[string]any
			want   []string
		}{
			{"map_string_string_field", map[string]any{"z": "1"}, []string{"ss"}},
			{"struct_tagged_field", map[string]any{"kind": "pref"}, []string{"st"}},
			{"struct_object_equal_declaration_order_free", map[string]any{"meta": map[string]any{"$eq": map[string]any{"A": 2, "Z": 1}}}, []string{"st"}},
			// 比较值本身是具体 Go 类型：同样按它的 JSON 形状比（曾经客户端过滤的后端 0 条、SQLite 1 条，struct
			// 比较值按声明序编码在 SQLite 上也对不上）
			{"operand_struct", map[string]any{"meta": map[string]any{"$eq": meta{Z: 1, A: 2}}}, []string{"st"}},
			{"operand_map_string_int", map[string]any{"meta": map[string]any{"$eq": map[string]int{"Z": 1, "A": 2}}}, []string{"st"}},
			{"operand_ne_struct", map[string]any{"meta": map[string]any{"$ne": meta{Z: 9, A: 9}}}, []string{"st"}},
			{"operand_raw_string", map[string]any{"z": map[string]any{"$eq": json.RawMessage(`"1"`)}}, []string{"ss"}},
			// 直值编成 JSON 是对象：等于这个对象（不是嵌套 filter）
			{"direct_struct", map[string]any{"meta": meta{Z: 1, A: 2}}, []string{"st"}},
			{"direct_map_string_int", map[string]any{"meta": map[string]int{"A": 2, "Z": 1}}, []string{"st"}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				items, err := st.Search(ctx, ns, store.WithLimit(100), store.WithFilter(c.filter))
				if err != nil {
					t.Fatalf("Search %v: %v", c.filter, err)
				}
				assertKeys(t, items, c.want)
			})
		}
	})

	// 多键 AND + 同一 key 上多个 op。
	//
	// ⚠️ **这条是给下推型 backend 用的**：把 filter 翻成 SQL 时，参数编号（PG 的 `$N`）
	// 一旦错位就会读到隔壁条件的值。而单 key 单 op 的用例**看不见**这种错——占位符只有
	// 一个，错不了。MySQL / SQLite 用 `?` 位置无关、天然免疫，所以「跑 SQLite 绿了」对
	// 参数编号这一维零信息量。
	//
	// fixture 的区间**故意互相重叠**（三个条件各自都能筛掉一部分行），错位就会换答案。
	t.Run("Search_WithFilter_MultiKey_And_MultiOp", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("multi")
		put := func(k string, v map[string]any) {
			if err := st.Put(ctx, ns, k, v); err != nil {
				t.Fatalf("Put %s: %v", k, err)
			}
		}
		//              age  score  tag
		put("hit", map[string]any{"age": 30, "score": 50, "tag": "x"})
		put("age_low", map[string]any{"age": 10, "score": 50, "tag": "x"})  // age 出界
		put("age_high", map[string]any{"age": 90, "score": 50, "tag": "x"}) // age 出界（上界）
		put("score_out", map[string]any{"age": 30, "score": 5, "tag": "x"}) // score 出界
		put("tag_out", map[string]any{"age": 30, "score": 50, "tag": "y"})  // tag 不等

		items, err := st.Search(ctx, ns, store.WithLimit(100), store.WithFilter(map[string]any{
			"age":   map[string]any{"$gt": 20, "$lt": 60}, // 同一 key 两个 op
			"score": map[string]any{"$gte": 50},
			"tag":   "x", // 直值路径也掺进来：它和 op-map 走的是不同分支
		}))
		if err != nil {
			t.Fatalf("multi-key filter: %v", err)
		}
		assertKeys(t, items, []string{"hit"})
	})

	t.Run("Search_UnsupportedFilterOp_Returns_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("a")

		_ = st.Put(ctx, ns, "k", map[string]any{"x": 1})
		// 嵌套使用未支持 op (如 $regex) — InMemoryStore 仅支持
		// $eq/$ne/$gt/$gte/$lt/$lte 6 个比较 op, 业务后端约定一致拒
		_, err := st.Search(ctx, ns, store.WithFilter(map[string]any{
			"x": map[string]any{"$regex": "pattern"},
		}))
		if err == nil {
			t.Error("Search with unsupported op $regex should return ErrUnsupportedFilterOp")
		}
		if !errors.Is(err, store.ErrUnsupportedFilterOp) {
			t.Errorf("expected ErrUnsupportedFilterOp, got %v", err)
		}
		// filter 值本身过不了 JSON（NaN / ±Inf / chan / func）——**所有后端必须同一时机
		// 报同一个哨兵**。不拒的话真库实测是四个后端三种答案且全 err == nil：客户端两个
		// 方向都不命中、MySQL `$gt NaN` 命中、PG `$lt NaN` 命中（它的 numeric 把 NaN 排在
		// 所有数之上、且自等）。「NaN 在引擎里怎么排序」我们改不动，只能在入口静态拒。
		for _, bad := range []struct {
			name string
			val  any
		}{
			{"NaN", math.NaN()},
			{"+Inf", math.Inf(1)},
			{"-Inf", math.Inf(-1)},
		} {
			_, err = st.Search(ctx, ns, store.WithFilter(map[string]any{"x": map[string]any{"$gt": bad.val}}))
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("$gt %s: 期望 ErrFilterTypeMismatch，得到 %v", bad.name, err)
			}
			_, err = st.Search(ctx, ns, store.WithFilter(map[string]any{"x": map[string]any{"$eq": bad.val}}))
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("$eq %s: 期望 ErrFilterTypeMismatch，得到 %v", bad.name, err)
			}
			_, err = st.Search(ctx, ns, store.WithFilter(map[string]any{"x": bad.val}))
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("直值 %s: 期望 ErrFilterTypeMismatch，得到 %v", bad.name, err)
			}
		}
		// 不可序列化的 Go 类型同款
		_, err = st.Search(ctx, ns, store.WithFilter(map[string]any{"x": map[string]any{"$eq": make(chan int)}}))
		if !errors.Is(err, store.ErrFilterTypeMismatch) {
			t.Errorf("$eq chan: 期望 ErrFilterTypeMismatch，得到 %v", err)
		}

		// 顶级逻辑操作符形态 `{"$or": [...]}`——同样是不支持的 op，不能当成叫 "$or" 的字段
		// 做等值（那样零行、err == nil，业务以为没数据；曾经 in-memory 就是这样静默放过的）
		_, err = st.Search(ctx, ns, store.WithFilter(map[string]any{
			"$or": []any{map[string]any{"x": 1}, map[string]any{"x": 2}},
		}))
		if !errors.Is(err, store.ErrUnsupportedFilterOp) {
			t.Errorf("top-level $or: expected ErrUnsupportedFilterOp, got %v", err)
		}
		// 嵌套 filter 与「op map 里混进裸键」——两种都拒。
		//
		// 修前这两种是**跨后端分歧 + 客户端静默**：`{"x":{"g":1}}` 客户端按嵌套部分匹配算、
		// sql 报错；`{"x":{"$eq":1,"g":1}}` 客户端把 $eq 整个丢掉返零行且 err == nil——
		// 业务明明写了 $eq，得到的是「没数据」而不是「你写错了」。
		for _, f := range []map[string]any{
			{"x": map[string]any{"g": 1}},
			{"x": map[string]any{"$eq": 1, "g": 1}},
		} {
			if _, err := st.Search(ctx, ns, store.WithFilter(f)); !errors.Is(err, store.ErrUnsupportedFilterOp) {
				t.Errorf("nested/mixed %v: expected ErrUnsupportedFilterOp, got %v", f, err)
			}
		}
	})

	// 非标识符字段名——`user-id` / `a.b` / CJK / `*` 都是**完全合法的 JSON 字段名**，
	// 必须在所有后端上一致地能用。
	//
	// 这条曾经是一处真实的跨后端分歧：sql 后端把字段名拼进 SQL 字面量、靠一道
	// `^[a-zA-Z_][a-zA-Z0-9_]*$` 白名单防注入，于是这些 key 在 in-memory / redis 上
	// 正常匹配、在三个 sql 方言上返 `invalid identifier`（而且是裸 error 不带哨兵，
	// 业务 errors.Is 判不出来）。改成绑参之后注入面消失、限制随之取消。
	//
	// ⚠️ `a.b` 与 `*` 是**承重用例**，不是凑数：下推型后端要把字段名放进 JSON path，
	// 那里 `.` 是层级分隔符、`*` 是通配符。`{"a.b": ...}` 必须命中**字面字段 `a.b`**
	// 而不是嵌套的 a→b；`{"*": ...}` 必须只命中真有 `*` 这个字段的行，不能匹配全部。
	t.Run("Search_WithFilter_NonIdentifierFieldNames", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("filter", "oddkeys")
		// 每条记录只带一个奇怪字段名；另加一条**嵌套** a→b 用来抓「path 把 . 当层级」
		fixtures := map[string]map[string]any{
			"hyphen": {"user-id": "u1"},
			"dotted": {"a.b": "flat"},
			"nested": {"a": map[string]any{"b": "nested"}},
			"cjk":    {"用户": "张三"},
			"star":   {"*": "literal-star"},
			"space":  {"first name": "ada"},
		}
		for k, v := range fixtures {
			if err := st.Put(ctx, ns, k, v); err != nil {
				t.Fatalf("Put(%s): %v", k, err)
			}
		}
		for _, c := range []struct {
			field string
			want  any
			key   string // 期望命中的那一条
		}{
			{"user-id", "u1", "hyphen"},
			{"a.b", "flat", "dotted"}, // ⚠️ 必须是 dotted 不是 nested
			{"用户", "张三", "cjk"},
			{"*", "literal-star", "star"}, // ⚠️ 只此一条，不是全部
			{"first name", "ada", "space"},
		} {
			items, err := st.Search(ctx, ns, store.WithFilter(map[string]any{c.field: c.want}))
			if err != nil {
				t.Errorf("filter {%q: %v}: 合法 JSON 字段名不该报错，得到 %v", c.field, c.want, err)
				continue
			}
			if len(items) != 1 || items[0].Key != c.key {
				got := make([]string, 0, len(items))
				for _, it := range items {
					got = append(got, it.Key)
				}
				t.Errorf("filter {%q: %v}: 期望恰好命中 [%s]，得到 %v", c.field, c.want, c.key, got)
			}
		}
	})

	// Search 的顺序契约是**时新优先**（UpdatedAt 降序），不是 key 字典序。
	//
	// # 为什么不是字典序
	//
	// 字典序跟「这条数据对调用方有没有用」**零相关**，而 `Search` 恒带 limit（后端
	// 默认或调用方给），所以顺序**就是**「截断时留下谁」。字典序意味着截断按 key 的
	// 首字母挑——一条 key 排在末尾的记录，无论多相关都进不了结果集，而调用方看到的是
	// 一个正常返回的列表，没有任何信号说「还有更该给你的没返」。
	//
	// 时新性不完美，但它是**单调有用**的先验：一条刚写的记录比一年前那条更可能相关。
	// 而且它跟真实持久后端的天然实现一致（时间列上本来就有索引）。
	//
	// # 这不解除上层按相关性重排的责任
	//
	// 有语义检索能力的后端应当按相关性返回并自报 [store.RelevanceRanker]；没有的
	// 后端返时新序，由上层（如长期记忆注入）在候选池里自己打分重排。**时新序的作用
	// 是保证候选池不被一个无关维度提前砍掉。**
	t.Run("Search_StableOrder_RecentFirst", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		// ⚠️ **fixture 必须让两种契约的期望互斥**，否则测不出区别。
		// 按 a→b→c 的顺序写入：时新序是 [c,b,a]，字典序是 [a,b,c]，**正好相反**。
		// （第一版写成了 c→b→a——那时两种排法都得 [a,b,c]，用例恒绿、分不开。）
		_ = st.Put(ctx, store.NS("z"), "a", 1)
		time.Sleep(2 * time.Millisecond) // 拉开时间戳，防同毫秒回落 key 序
		_ = st.Put(ctx, store.NS("z"), "b", 2)
		time.Sleep(2 * time.Millisecond)
		_ = st.Put(ctx, store.NS("z"), "c", 3)

		items, _ := st.Search(ctx, store.NS("z"))
		// 先卡数量 — 业务 backend 漏写数据时 Fatalf 让 sanity-revert 精确指向
		if len(items) != 3 {
			t.Fatalf("expected 3 items inserted, got %d (业务 backend Search 实现漏数据)", len(items))
		}
		// 最近写的排最前。返 [a,b,c] 说明是字典序不是时新序。
		if items[0].Key != "c" || items[1].Key != "b" || items[2].Key != "a" {
			t.Errorf("expected recent-first order [c,b,a] (写入顺序 a→b→c), got [%s,%s,%s]"+
				" —— 返 [a,b,c] 说明后端按 key 字典序排，那跟「这条对调用方有没有用」零相关",
				items[0].Key, items[1].Key, items[2].Key)
		}
		// 时间戳相同时才回落 key 序——那只是为了结果可预测，不是主序。
	})
}

// assertKeys 断言 items 的 key 集合恰好等于 want（无序）——多返、少返都要红，并把两边
// 都打出来让 sanity-revert 的失败信息精确指向多了哪行 / 少了哪行。
func assertKeys(t *testing.T, items []*store.SearchItem, want []string) {
	t.Helper()
	got := make([]string, 0, len(items))
	for _, it := range items {
		got = append(got, it.Key)
	}
	sort.Strings(got)
	w := append([]string(nil), want...)
	sort.Strings(w)
	if fmt.Sprint(got) != fmt.Sprint(w) {
		t.Errorf("matched keys mismatch:\n  want %v\n  got  %v", w, got)
	}
}
