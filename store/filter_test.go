package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"crew-go/store"
)

// TestValidateFilter_RangeOp_FilterValue_Is_Static 验范围 op 的 filter 值校验是**静态**的：
// 不看数据、不看 namespace——空 namespace 也报。等扫到第一行才报的实现在空 ns 上会静默
// 放过，跨后端就漂了（sql 后端在构 SQL 前就拦，客户端过滤后端必须同一时机）。
func TestValidateFilter_RangeOp_FilterValue_Is_Static(t *testing.T) {
	for _, op := range []string{"$gt", "$gte", "$lt", "$lte"} {
		for _, v := range []any{"abc", "10", true, nil, []any{1}, map[string]any{"x": 1}} {
			err := store.ValidateFilter(map[string]any{"f": map[string]any{op: v}})
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("%s %T(%v): expected ErrFilterTypeMismatch, got %v", op, v, v, err)
			}
		}
		for _, v := range []any{10, int64(10), uint8(1), 3.5, float32(1), json.Number("42"), json.Number("1.5")} {
			if err := store.ValidateFilter(map[string]any{"f": map[string]any{op: v}}); err != nil {
				t.Errorf("%s %T: expected ok, got %v", op, v, err)
			}
		}
	}
	// $eq / $ne / 直值不限类型
	for _, f := range []map[string]any{
		{"f": "x"}, {"f": nil}, {"f": []any{1}}, {"f": map[string]any{"$eq": "x"}}, {"f": map[string]any{"$ne": nil}},
	} {
		if err := store.ValidateFilter(f); err != nil {
			t.Errorf("%v: expected ok, got %v", f, err)
		}
	}
	if err := store.ValidateFilter(map[string]any{"f": map[string]any{"$in": []any{1}}}); !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("$in: expected ErrUnsupportedFilterOp, got %v", err)
	}
	// 顶级逻辑操作符形态：不是「叫 $or 的字段」，是不支持的 op（曾经零行 err==nil 静默放过）
	if err := store.ValidateFilter(map[string]any{"$or": []any{map[string]any{"f": 1}}}); !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("top-level $or: expected ErrUnsupportedFilterOp, got %v", err)
	}
	// 空 namespace 上 Search 也报（InMemoryStore 走的就是 ValidateFilter）
	st := store.NewInMemoryStore()
	_, err := st.Search(context.Background(), store.NS("empty"),
		store.WithFilter(map[string]any{"f": map[string]any{"$gt": "abc"}}))
	if !errors.Is(err, store.ErrFilterTypeMismatch) {
		t.Errorf("Search on empty ns with $gt \"abc\": expected ErrFilterTypeMismatch, got %v", err)
	}
}

// TestValueEqual_JSONSemantics 钉 JSON 语义相等的每一条：数值互通且递归到容器、bool 不是数、
// 数字字符串不是数、null 只等 null、数组逐元素（不是包含）且 slice 类型互比、对象逐键。
func TestValueEqual_JSONSemantics(t *testing.T) {
	cases := []struct {
		name string
		a, b any
		want bool
	}{
		{"int vs float", 10, 10.0, true},
		{"int vs json.Number", 10, json.Number("10.0"), true},
		{"json.Number vs json.Number", json.Number("1e1"), json.Number("10"), true},
		{"bool vs 1", true, 1, false},
		{"numeric string vs number", "10", 10, false},
		{"string vs string", "x", "x", true},
		{"nil vs nil", nil, nil, true},
		{"nil vs 0", nil, 0, false},
		{"nil vs false", nil, false, false},
		{"array elements numeric tolerant", []any{1, 2}, []any{float64(1), json.Number("2")}, true},
		{"array is equality not containment", []any{"a"}, []any{"a", "b"}, false},
		{"[]string vs []any", []string{"a", "b"}, []any{"a", "b"}, true},
		{"array order matters", []any{"a", "b"}, []any{"b", "a"}, false},
		{"object key order free", map[string]any{"a": 1, "b": "x"}, map[string]any{"b": "x", "a": 1.0}, true},
		{"object subset is not equal", map[string]any{"a": 1}, map[string]any{"a": 1, "b": 2}, false},
		{"nested", map[string]any{"a": []any{1, map[string]any{"k": 2}}}, map[string]any{"a": []any{1.0, map[string]any{"k": json.Number("2")}}}, true},
		{"[]byte vs string never equal", []byte("x"), "x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := store.ValueEqual(c.a, c.b); got != c.want {
				t.Errorf("ValueEqual(%#v, %#v) = %v, want %v", c.a, c.b, got, c.want)
			}
			if got := store.ValueEqual(c.b, c.a); got != c.want {
				t.Errorf("ValueEqual(%#v, %#v) (swapped) = %v, want %v", c.b, c.a, got, c.want)
			}
		})
	}
}

// TestMatchFilter_TypeBracketing_And_Ne 钉客户端过滤的单行语义（redis / 业务后端直接复用
// 这个函数）：存值非数值对范围 op 是「不命中」不是错误；`$ne` 是「存在且不等」，JSON null
// 算存在、缺失不算。
func TestMatchFilter_TypeBracketing_And_Ne(t *testing.T) {
	gt10 := map[string]any{"f": map[string]any{"$gt": 10}}
	for _, v := range []any{"string_val", "50", true, nil, []any{1, 2}, map[string]any{"x": 1}} {
		ok, err := store.MatchFilter(map[string]any{"f": v}, gt10)
		if err != nil || ok {
			t.Errorf("$gt 10 on stored %T(%v): want (false, nil), got (%v, %v)", v, v, ok, err)
		}
	}
	if ok, err := store.MatchFilter(map[string]any{"f": 50}, gt10); err != nil || !ok {
		t.Errorf("$gt 10 on 50: want (true, nil), got (%v, %v)", ok, err)
	}
	if ok, err := store.MatchFilter(map[string]any{"g": 50}, gt10); err != nil || ok {
		t.Errorf("$gt 10 on missing field: want (false, nil), got (%v, %v)", ok, err)
	}
	// filter 值侧仍报错（兜底；静态校验在 ValidateFilter）
	if _, err := store.MatchFilter(map[string]any{"f": 50}, map[string]any{"f": map[string]any{"$gt": "abc"}}); !errors.Is(err, store.ErrFilterTypeMismatch) {
		t.Errorf("$gt \"abc\": expected ErrFilterTypeMismatch, got %v", err)
	}

	ne10 := map[string]any{"f": map[string]any{"$ne": 10}}
	for v, want := range map[string]bool{"json null": true, "string": true, "missing": false, "equal": false} {
		var value map[string]any
		switch v {
		case "json null":
			value = map[string]any{"f": nil}
		case "string":
			value = map[string]any{"f": "10"}
		case "missing":
			value = map[string]any{"g": 1}
		case "equal":
			value = map[string]any{"f": json.Number("10.0")}
		}
		ok, err := store.MatchFilter(value, ne10)
		if err != nil || ok != want {
			t.Errorf("$ne 10 on %s: want (%v, nil), got (%v, %v)", v, want, ok, err)
		}
	}
	// 不可比较类型不 panic（曾用 a == b 触发 runtime panic）
	if ok, _ := store.MatchFilter(map[string]any{"f": []any{"a"}}, map[string]any{"f": []any{"a"}}); !ok {
		t.Error("slice equality should match without panic")
	}
}

// TestValueEqual_And_RangeOps_ExactIntegers 钉「2^53 之外的整数按精确整数比」。
//
// 修前 toFloat 一律转 float64：存 9007199254740993 时 `$eq ...992` 反命中、`$gt ...992`
// 漏命中，两个方向都错，而三个 SQL 引擎都算得对——客户端过滤是唯一错的那个。
//
// sanity-revert：把 compareNumeric 的 toBigInt 分支删掉（退回纯 float64）→ 本测试红。
func TestValueEqual_And_RangeOps_ExactIntegers(t *testing.T) {
	const big93 = "9007199254740993" // 2^53+1，float64 存不下
	got := map[string]any{"f": json.Number(big93)}

	// $eq：不该等于 2^53
	for _, probe := range []any{int64(9007199254740992), json.Number("9007199254740992")} {
		ok, err := store.MatchFilter(got, map[string]any{"f": map[string]any{"$eq": probe}})
		if err != nil {
			t.Fatalf("$eq %v: %v", probe, err)
		}
		if ok {
			t.Errorf("$eq %v 不该命中 %s（float64 会把两者舍成同一个值）", probe, big93)
		}
	}
	// 等于自己
	if ok, err := store.MatchFilter(got, map[string]any{"f": map[string]any{"$eq": json.Number(big93)}}); err != nil || !ok {
		t.Errorf("$eq 自身应命中，得到 ok=%v err=%v", ok, err)
	}
	// 范围 op 两个方向
	for _, c := range []struct {
		op   string
		want bool
	}{{"$gt", true}, {"$gte", true}, {"$lt", false}, {"$lte", false}} {
		ok, err := store.MatchFilter(got, map[string]any{
			"f": map[string]any{c.op: int64(9007199254740992)}})
		if err != nil {
			t.Fatalf("%s: %v", c.op, err)
		}
		if ok != c.want {
			t.Errorf("%s 9007199254740992 打在 %s 上：得到 %v，应为 %v", c.op, big93, ok, c.want)
		}
	}
	// 小数仍走 float64（三个引擎自己就不一致，不在这里假装精确）——10 == 10.0 不能坏
	if !store.ValueEqual(10, 10.0) {
		t.Error("10 == 10.0 的 JSON 语义相等被整数路径破坏了")
	}
	if !store.ValueEqual(json.Number("10"), 10.0) {
		t.Error("json.Number(\"10\") == 10.0 被破坏了")
	}
}

// TestValidateFilter_NestedAndMixed_Rejected 钉「嵌套 / 混用 $ 与非 $ 键一律拒」。
//
// 修前：{"f":{"g":1}} 客户端按嵌套部分匹配算、sql 报错（同一写法两种语义）；
// {"f":{"$eq":10,"g":1}} 客户端把 $eq 整个丢掉、返零行且 err == nil。
func TestValidateFilter_NestedAndMixed_Rejected(t *testing.T) {
	for _, f := range []map[string]any{
		{"f": map[string]any{"g": 1}},            // 纯嵌套
		{"f": map[string]any{"$eq": 10, "g": 1}}, // 混用
	} {
		if err := store.ValidateFilter(f); !errors.Is(err, store.ErrUnsupportedFilterOp) {
			t.Errorf("%v: 期望 ErrUnsupportedFilterOp，得到 %v", f, err)
		}
		// 直接调 MatchFilter（自定义 backend 可能不先 Validate）同样要拒，不能静默算。
		// 两种 got 都试：一种让 $eq 会失败、一种让 $eq 会通过——**出口必须都是报错**，
		// 否则就是「先撞到哪个键」决定结果（map 迭代序不定）。
		for _, got := range []map[string]any{
			{"f": map[string]any{"g": 1}}, // $eq 10 打在 map 上会失败
			{"f": 10},                     // $eq 10 会通过
		} {
			for i := 0; i < 50; i++ { // 迭代序是随机的，多跑几轮才敢说出口确定
				if _, err := store.MatchFilter(got, f); !errors.Is(err, store.ErrUnsupportedFilterOp) {
					t.Fatalf("MatchFilter(got=%v, %v): 期望 ErrUnsupportedFilterOp，得到 %v", got, f, err)
				}
			}
		}
	}
}

// TestCompareNumeric_FastPath_Agrees_With_BigInt 钉「int64 快路与 big.Int 慢路结论一致」。
//
// 两条路判定的是同一件事（这是不是精确整数），只是表示范围不同——快路的存在纯为省分配，
// 一旦两者在某个值上分叉，业务看到的就是「同一个 filter 时对时错」。uint64 超 MaxInt64
// 的那一段正是分界，必须两边都覆盖。
func TestCompareNumeric_FastPath_Agrees_With_BigInt(t *testing.T) {
	vals := []any{
		int64(0), int64(-1), int64(1), int64(-9007199254740993), int64(9007199254740993),
		int64(math.MinInt64), int64(math.MaxInt64),
		uint64(math.MaxInt64), uint64(math.MaxUint64), // 后者只有 big.Int 路走得了
		json.Number("9007199254740993"), json.Number("18446744073709551615"),
		json.Number("-18446744073709551616"), // 超 int64 两个方向
		uint8(7), int32(-7),
	}
	for _, a := range vals {
		for _, b := range vals {
			// 用 $eq / $gt 两个方向过一遍 MatchFilter，确保没有 panic、结论自洽
			eq, err := store.MatchFilter(map[string]any{"f": a}, map[string]any{"f": map[string]any{"$eq": b}})
			if err != nil {
				t.Fatalf("$eq(%v,%v): %v", a, b, err)
			}
			gt, err := store.MatchFilter(map[string]any{"f": a}, map[string]any{"f": map[string]any{"$gt": b}})
			if err != nil {
				t.Fatalf("$gt(%v,%v): %v", a, b, err)
			}
			lt, err := store.MatchFilter(map[string]any{"f": a}, map[string]any{"f": map[string]any{"$lt": b}})
			if err != nil {
				t.Fatalf("$lt(%v,%v): %v", a, b, err)
			}
			// 三分律：恰好一个成立
			n := 0
			for _, v := range []bool{eq, gt, lt} {
				if v {
					n++
				}
			}
			if n != 1 {
				t.Errorf("三分律破了：a=%v(%T) b=%v(%T) eq=%v gt=%v lt=%v", a, a, b, b, eq, gt, lt)
			}
		}
	}
}

// TestCompareNumeric_LoadBearing_Magnitudes 钉**具体大小关系**，不是三分律。
//
// ⚠️ 上面那条叉积测试的值集是对的，断言却选在了不承重的维度上——独立审计实测：撤掉
// big.Int 慢路、撤掉 uint64 溢出守卫，**四套测试全绿**，因为精度塌缩和符号翻转都**保持**
// 三分律（恰好一个成立，只是成立的那个变了）。这正是本仓记的「断言选在不承重的维度上，
// 等于没测」。承重的断言必须是「谁大谁小」本身。
func TestCompareNumeric_LoadBearing_Magnitudes(t *testing.T) {
	cases := []struct {
		name     string
		got      any // 存的值
		op       string
		val      any // filter 值
		want     bool
		revealed string // 这条断言是为了抓哪个退化
	}{
		// 撤掉 big.Int 慢路 → 两个值都塌成同一个 float64 → $eq 变 true
		{"MaxInt64+1 不等于 MaxInt64", json.Number("9223372036854775808"), "$eq",
			int64(math.MaxInt64), false, "big.Int 慢路"},
		{"MaxInt64+1 大于 MaxInt64", json.Number("9223372036854775808"), "$gt",
			int64(math.MaxInt64), true, "big.Int 慢路"},
		// 撤掉 toInt64 的 uint64 守卫 → MaxUint64 被当成 int64(-1) → 符号翻转
		{"MaxUint64 大于 0", uint64(math.MaxUint64), "$gt", 0, true, "uint64 溢出守卫"},
		{"MaxUint64 不小于 0", uint64(math.MaxUint64), "$lt", 0, false, "uint64 溢出守卫"},
		{"MaxUint64 不等于 -1", uint64(math.MaxUint64), "$eq", int64(-1), false, "uint64 溢出守卫"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := store.MatchFilter(map[string]any{"f": c.got},
				map[string]any{"f": map[string]any{c.op: c.val}})
			if err != nil {
				t.Fatalf("%v: %v", c.name, err)
			}
			if got != c.want {
				t.Errorf("%v %s %v = %v，应为 %v（这条断言钉的是 %s）",
					c.got, c.op, c.val, got, c.want, c.revealed)
			}
		})
	}
}

// TestNaN_IsUnordered_NotEqual 钉「NaN 不可比，不是相等」。
//
// 回归实录：`compareNumeric` 的 float 尾部曾是 `<` / `>` 两分支 + 兜底 `return 0`——
// 而 IEEE-754 下 NaN 的两个比较都是 false，于是「无序」被兜底成了「相等」。后果是**一个
// NaN 行会命中任何数值 filter**：`$eq 10` 把它返回来、`$ne 10` 反而不返，五个算子全翻，
// 且 err == nil。in-memory 中招是因为它不做 JSON 往返；redis / sql 因 json.Marshal 拒
// NaN 而免疫，所以这条只能在这一层钉。
//
// 语义取「不命中 + 不报错」，与同类库一致（它们的用例原话：NaN never equals anything,
// including itself, so should return 0 results；且 should not raise exception）。
func TestNaN_IsUnordered_NotEqual(t *testing.T) {
	nan := math.NaN()
	// 存的是 NaN：任何数值 filter 都不该命中它
	for _, op := range []string{"$eq", "$gt", "$gte", "$lt", "$lte"} {
		ok, err := store.MatchFilter(map[string]any{"f": nan},
			map[string]any{"f": map[string]any{op: 10}})
		if err != nil {
			t.Errorf("%s 打在 NaN 上不该报错，得到 %v", op, err)
		}
		if ok {
			t.Errorf("存的是 NaN，%s 10 不该命中", op)
		}
	}
	// $ne 反过来：NaN 确实不等于 10
	if ok, err := store.MatchFilter(map[string]any{"f": nan},
		map[string]any{"f": map[string]any{"$ne": 10}}); err != nil || !ok {
		t.Errorf("NaN $ne 10 应命中，得到 ok=%v err=%v", ok, err)
	}
	// filter 值是 NaN：同样不命中、不报错
	for _, op := range []string{"$eq", "$gt", "$lt"} {
		ok, err := store.MatchFilter(map[string]any{"f": 10},
			map[string]any{"f": map[string]any{op: nan}})
		if err != nil {
			t.Errorf("filter 值为 NaN 时 %s 不该报错，得到 %v", op, err)
		}
		if ok {
			t.Errorf("filter 值为 NaN 时 %s 不该命中", op)
		}
	}
	// NaN 不等于自己
	if store.ValueEqual(nan, nan) {
		t.Error("ValueEqual(NaN, NaN) 应为 false（IEEE-754：NaN 与自己也不相等）")
	}
	if store.ValueEqual(nan, 10) {
		t.Error("ValueEqual(NaN, 10) 应为 false")
	}
}

// TestValidateFilter_EmptyOpMap_Rejected 钉「空 map 不是合法 filter」。
//
// `{"f": {}}` 既不是 op map 也不是「等于空对象」：放行的话新的「嵌套/混用一律拒」规则
// 被**空洞满足**——实测 in-memory 返「有 f 字段的行」、SQLite 返**全表**，正是这条规则
// 本来要消灭的「一个写法两种语义」。
func TestValidateFilter_EmptyOpMap_Rejected(t *testing.T) {
	f := map[string]any{"f": map[string]any{}}
	if err := store.ValidateFilter(f); !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("ValidateFilter: 期望 ErrUnsupportedFilterOp，得到 %v", err)
	}
	if _, err := store.MatchFilter(map[string]any{"f": 1}, f); !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("MatchFilter: 期望 ErrUnsupportedFilterOp，得到 %v", err)
	}
	// 顶级 $ 键：MatchFilter 要和 ValidateFilter 对称（不对称的话直接调 MatchFilter 的
	// 自定义 backend 又回到「零行且 err == nil」那个坑）
	or := map[string]any{"$or": []any{map[string]any{"f": 1}}}
	if _, err := store.MatchFilter(map[string]any{"f": 1}, or); !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("MatchFilter 顶级 $or: 期望 ErrUnsupportedFilterOp，得到 %v", err)
	}
	// 「等于空对象」的正确写法仍然可用
	ok, err := store.MatchFilter(map[string]any{"f": map[string]any{}},
		map[string]any{"f": map[string]any{"$eq": map[string]any{}}})
	if err != nil || !ok {
		t.Errorf("$eq 空对象应命中，得到 ok=%v err=%v", ok, err)
	}
}
