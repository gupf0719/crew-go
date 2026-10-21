package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"crew-go/store"
)

// ── Namespace 测试 ─────────────────────────────────────────────────────────

func TestNamespace_Validate(t *testing.T) {
	tests := []struct {
		name    string
		ns      store.Namespace
		wantErr error
	}{
		{"empty namespace", store.NS(), store.ErrEmptyNamespace},
		{"nil namespace", store.Namespace(nil), store.ErrEmptyNamespace},
		{"empty segment", store.NS("users", "", "prefs"), store.ErrInvalidNamespace},
		{"segment with dot", store.NS("users", "ali.ce"), store.ErrInvalidNamespace},
		{"segment with NUL byte", store.NS("users", "a\x00b"), store.ErrInvalidNamespace},
		{"valid", store.NS("users", "alice", "prefs"), nil},
		{"single segment", store.NS("shared"), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ns.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Validate() = %v, want errors.Is %v", err, tc.wantErr)
			}
		})
	}
}

func TestNamespace_IsPrefixOf(t *testing.T) {
	tests := []struct {
		name   string
		n      store.Namespace
		other  store.Namespace
		expect bool
	}{
		{"prefix matches", store.NS("users"), store.NS("users", "alice"), true},
		{"exact equal", store.NS("users"), store.NS("users"), true},
		{"empty prefix matches all", store.NS(), store.NS("users", "alice"), true},
		{"longer than other", store.NS("users", "alice"), store.NS("users"), false},
		{"different first seg", store.NS("groups"), store.NS("users", "alice"), false},
		{"different middle seg", store.NS("users", "bob"), store.NS("users", "alice"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.n.IsPrefixOf(tc.other); got != tc.expect {
				t.Errorf("IsPrefixOf() = %v, want %v", got, tc.expect)
			}
		})
	}
}

func TestNamespace_Equal(t *testing.T) {
	if !store.NS("a", "b").Equal(store.NS("a", "b")) {
		t.Error("NS(a,b).Equal(NS(a,b)) should be true")
	}
	if store.NS("a", "b").Equal(store.NS("a")) {
		t.Error("different len should not equal")
	}
	if store.NS("a").Equal(store.NS("b")) {
		t.Error("different content should not equal")
	}
}

func TestNamespace_String(t *testing.T) {
	if got := store.NS("users", "alice", "prefs").String(); got != "users.alice.prefs" {
		t.Errorf("String() = %q, want %q", got, "users.alice.prefs")
	}
}

// ── Get/Put/Delete 基础 ────────────────────────────────────────────────────

func TestInMemoryStore_PutGet(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("users", "alice", "prefs")

	// Put 新值
	if err := st.Put(ctx, ns, "theme", map[string]any{"mode": "dark"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get 拿出来
	item, err := st.Get(ctx, ns, "theme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if item == nil {
		t.Fatal("Get returned nil item after Put")
	}
	got := item.Value.(map[string]any)
	if got["mode"] != "dark" {
		t.Errorf("Value mode = %v, want dark", got["mode"])
	}
	if !item.Namespace.Equal(ns) {
		t.Errorf("Namespace = %v, want %v", item.Namespace, ns)
	}
	if item.Key != "theme" {
		t.Errorf("Key = %q, want theme", item.Key)
	}
	if item.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
	if !item.CreatedAt.Equal(item.UpdatedAt) {
		t.Error("CreatedAt should equal UpdatedAt on first Put")
	}
}

func TestInMemoryStore_GetNotFound(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	item, err := st.Get(ctx, store.NS("users", "alice"), "nonexistent")
	if err != nil {
		t.Fatalf("Get not-found: %v", err)
	}
	if item != nil {
		t.Errorf("Get not-found returned %v, want nil", item)
	}
}

func TestInMemoryStore_PutOverwrite(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("users", "alice")

	if err := st.Put(ctx, ns, "k", "v1"); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	first, _ := st.Get(ctx, ns, "k")
	firstCreated := first.CreatedAt

	// 等够时间让 UpdatedAt 跟 CreatedAt 区分（time.Now 单调时钟精度）
	time.Sleep(2 * time.Millisecond)

	// 覆盖
	if err := st.Put(ctx, ns, "k", "v2"); err != nil {
		t.Fatalf("overwrite Put: %v", err)
	}
	second, _ := st.Get(ctx, ns, "k")
	if second.Value != "v2" {
		t.Errorf("Value after overwrite = %v, want v2", second.Value)
	}
	if !second.CreatedAt.Equal(firstCreated) {
		t.Errorf("CreatedAt should preserve across overwrite: first=%v, second=%v", firstCreated, second.CreatedAt)
	}
	if !second.UpdatedAt.After(firstCreated) {
		t.Errorf("UpdatedAt should advance: %v vs CreatedAt %v", second.UpdatedAt, firstCreated)
	}
}

func TestInMemoryStore_Delete(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("users", "alice")

	_ = st.Put(ctx, ns, "k", "v")
	if err := st.Delete(ctx, ns, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	item, _ := st.Get(ctx, ns, "k")
	if item != nil {
		t.Error("Get after Delete should return nil")
	}
}

func TestInMemoryStore_DeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	// 删不存在的 key 不报错
	if err := st.Delete(ctx, store.NS("nonexistent"), "k"); err != nil {
		t.Errorf("Delete nonexistent should be idempotent, got %v", err)
	}
}

func TestInMemoryStore_PutNilEqualsDelete(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("users", "alice")

	_ = st.Put(ctx, ns, "k", "v")
	// Put(value=nil) 等价 Delete（对齐 LangGraph 语义）
	if err := st.Put(ctx, ns, "k", nil); err != nil {
		t.Fatalf("Put(nil): %v", err)
	}
	item, _ := st.Get(ctx, ns, "k")
	if item != nil {
		t.Error("Get after Put(nil) should return nil")
	}
}

// TestInMemoryStore_GetReturnsCopy 验证 Get 返 shallow copy 防 outer field 篡改。
//
// **shallow copy 边界**（PR 1 audit 视角 1 HIGH-1 文档化）：
//   - ✅ 防业务改 got.Namespace / got.Key / got.Value 替换为新值
//   - ❌ **不**防业务 mutate got.Value 内部 map/slice——业务约定不可变
//
// 第二组断言 verify "不防 Value 内 map 篡改"是已知/契约行为而非未发现 bug。
func TestInMemoryStore_GetReturnsCopy(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("users", "alice")
	val := map[string]any{"mode": "dark"}
	_ = st.Put(ctx, ns, "theme", val)

	// ─── 防 outer field 篡改 ───
	got, err := st.Get(ctx, ns, "theme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Value = "tampered" // 替换 Value 引用
	got.Namespace = store.NS("evil")
	got.Key = "evil"

	again, _ := st.Get(ctx, ns, "theme")
	againMap, ok := again.Value.(map[string]any)
	if !ok {
		t.Fatalf("Value type changed (outer field tamper leaked): got %T", again.Value)
	}
	if againMap["mode"] != "dark" {
		t.Errorf("internal Value tampered: %v", againMap)
	}
	if again.Key != "theme" {
		t.Errorf("Key tampered: %v", again.Key)
	}

	// ─── 不防 Value 内 map 篡改（契约行为）───
	again.Value.(map[string]any)["mode"] = "intruded"
	third, _ := st.Get(ctx, ns, "theme")
	if third.Value.(map[string]any)["mode"] != "intruded" {
		t.Errorf("expected shallow-copy contract (Value inner mutation visible to store), got %v", third.Value)
	}
}

// ── 入参 namespace 校验 ────────────────────────────────────────────────────

func TestInMemoryStore_GetInvalidNS(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	cases := []struct {
		name string
		ns   store.Namespace
	}{
		{"empty", store.NS()},
		{"empty segment", store.NS("users", "", "k")},
		{"dot segment", store.NS("users", "ali.ce")},
		{"NUL segment", store.NS("users", "a\x00b")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.Get(ctx, tc.ns, "k")
			if err == nil {
				t.Error("Get with invalid ns should return error")
			}
		})
	}
}

// TestInMemoryStore_NULCollisionRejected 验证 NUL 字节 namespace 段被 Validate 拒
// —— 防 NS("a\x00b","c") 跟 NS("a","b\x00c") 共享内部 bucket key 互相覆盖的安全 bug。
//
// 历史：PR 1 audit 视角 3 HIGH-1 揭露 InMemoryStore.joinNS 用 "\x00" 作分隔符但
// Validate 没禁 NUL → 业务输入含外部用户串可读/覆盖别人数据。修复加 Validate 禁 NUL。
func TestInMemoryStore_NULCollisionRejected(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()

	// 两个含 NUL 的 namespace 在错误实现下会共享 bucket：
	ns1 := store.NS("a\x00b", "c") // joinNS = "a\x00b\x00c"
	ns2 := store.NS("a", "b\x00c") // joinNS = "a\x00b\x00c" — collision

	// Validate 应该拒绝 NUL 段
	err1 := st.Put(ctx, ns1, "k", "v1")
	err2 := st.Put(ctx, ns2, "k", "v2")
	if !errors.Is(err1, store.ErrInvalidNamespace) {
		t.Errorf("ns1 Put with NUL segment should fail: %v", err1)
	}
	if !errors.Is(err2, store.ErrInvalidNamespace) {
		t.Errorf("ns2 Put with NUL segment should fail: %v", err2)
	}
}

// ── TTL ────────────────────────────────────────────────────────────────────

func TestInMemoryStore_WithTTLNotSupported(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	err := st.Put(ctx, store.NS("u"), "k", "v", store.WithTTL(5*time.Minute))
	if !errors.Is(err, store.ErrTTLNotSupported) {
		t.Errorf("Put with WithTTL should return ErrTTLNotSupported, got %v", err)
	}
}

func TestInMemoryStore_NotTTLAware(t *testing.T) {
	st := store.NewInMemoryStore()
	// InMemoryStore 不实现 TTLAware——业务 type-assert 应该 false
	if _, ok := any(st).(store.TTLAware); ok {
		t.Error("InMemoryStore should NOT implement TTLAware (PoC)")
	}
}

// ── Search ────────────────────────────────────────────────────────────────

func TestInMemoryStore_SearchBasic(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("users", "alice"), "k1", map[string]any{"v": 1})
	_ = st.Put(ctx, store.NS("users", "alice"), "k2", map[string]any{"v": 2})
	_ = st.Put(ctx, store.NS("users", "bob"), "k3", map[string]any{"v": 3})

	// Search prefix=("users",) 拿到 3 条
	items, err := st.Search(ctx, store.NS("users"))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("Search users got %d items, want 3", len(items))
	}

	// Search prefix=("users", "alice",) 拿到 2 条
	items, _ = st.Search(ctx, store.NS("users", "alice"))
	if len(items) != 2 {
		t.Errorf("Search users.alice got %d items, want 2", len(items))
	}
}

func TestInMemoryStore_SearchEmptyPrefix(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_, err := st.Search(ctx, store.NS())
	if !errors.Is(err, store.ErrEmptyNamespace) {
		t.Errorf("Search empty prefix should return ErrEmptyNamespace, got %v", err)
	}
}

func TestInMemoryStore_SearchFilterEq(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("data"), "k1", map[string]any{"type": "A", "n": 1})
	_ = st.Put(ctx, store.NS("data"), "k2", map[string]any{"type": "B", "n": 2})
	_ = st.Put(ctx, store.NS("data"), "k3", map[string]any{"type": "A", "n": 3})

	// 精确匹配
	items, err := st.Search(ctx, store.NS("data"),
		store.WithFilter(map[string]any{"type": "A"}))
	if err != nil {
		t.Fatalf("Search filter: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("filter type=A got %d, want 2", len(items))
	}

	// $eq 显式
	items, _ = st.Search(ctx, store.NS("data"),
		store.WithFilter(map[string]any{"type": map[string]any{"$eq": "B"}}))
	if len(items) != 1 {
		t.Errorf("filter $eq B got %d, want 1", len(items))
	}
}

func TestInMemoryStore_SearchFilterComparison(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("data"), "k1", map[string]any{"n": 1})
	_ = st.Put(ctx, store.NS("data"), "k2", map[string]any{"n": 5})
	_ = st.Put(ctx, store.NS("data"), "k3", map[string]any{"n": 10})

	cases := []struct {
		op   string
		val  float64
		want int
	}{
		{"$gt", 5, 1},
		{"$gte", 5, 2},
		{"$lt", 5, 1},
		{"$lte", 5, 2},
		{"$ne", 5, 2},
		{"$eq", 5, 1},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			items, err := st.Search(ctx, store.NS("data"),
				store.WithFilter(map[string]any{"n": map[string]any{tc.op: tc.val}}))
			if err != nil {
				t.Fatalf("%s: %v", tc.op, err)
			}
			if len(items) != tc.want {
				t.Errorf("%s 5: got %d items, want %d", tc.op, len(items), tc.want)
			}
		})
	}
}

func TestInMemoryStore_SearchFilterUnsupportedOp(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("data"), "k1", map[string]any{"v": 1})

	for _, op := range []string{"$or", "$nor", "$in", "$nin", "$and"} {
		t.Run(op, func(t *testing.T) {
			_, err := st.Search(ctx, store.NS("data"),
				store.WithFilter(map[string]any{"v": map[string]any{op: 1}}))
			if !errors.Is(err, store.ErrUnsupportedFilterOp) {
				t.Errorf("filter %s should return ErrUnsupportedFilterOp, got %v", op, err)
			}
		})
	}
}

func TestInMemoryStore_SearchFilterTypeMismatch(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("data"), "k1", map[string]any{"name": "alice"})

	// filter 值侧：范围 op 的值不是数——调用方自己给错，报 sentinel
	_, err := st.Search(ctx, store.NS("data"),
		store.WithFilter(map[string]any{"name": map[string]any{"$gt": "alice"}}))
	if !errors.Is(err, store.ErrFilterTypeMismatch) {
		t.Errorf("$gt with a string filter value should return ErrFilterTypeMismatch, got %v", err)
	}

	// 存值侧：filter 值是数、存的是字符串——类型对不上是这一行不命中，不是错误
	// （type bracketing；一条异构行不能毒化整个 Search）
	items, err := st.Search(ctx, store.NS("data"),
		store.WithFilter(map[string]any{"name": map[string]any{"$gt": 10}}))
	if err != nil {
		t.Fatalf("$gt 10 on a string-valued field should not error, got %v", err)
	}
	if len(items) != 0 {
		t.Errorf("string-valued row must not match $gt 10, got %d items", len(items))
	}
}

func TestInMemoryStore_SearchLimitOffset(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	for i := 0; i < 20; i++ {
		_ = st.Put(ctx, store.NS("data"), keyN(i), map[string]any{"i": i})
	}

	// 默认 limit 10
	items, _ := st.Search(ctx, store.NS("data"))
	if len(items) != 10 {
		t.Errorf("default limit got %d, want 10", len(items))
	}

	// WithLimit(5)
	items, _ = st.Search(ctx, store.NS("data"), store.WithLimit(5))
	if len(items) != 5 {
		t.Errorf("WithLimit(5) got %d, want 5", len(items))
	}

	// WithOffset(15) WithLimit(10) 应拿 5 条（剩余）
	items, _ = st.Search(ctx, store.NS("data"), store.WithLimit(10), store.WithOffset(15))
	if len(items) != 5 {
		t.Errorf("WithOffset(15)+Limit(10) got %d, want 5", len(items))
	}

	// 超大 offset
	items, _ = st.Search(ctx, store.NS("data"), store.WithOffset(100))
	if len(items) != 0 {
		t.Errorf("WithOffset(100) got %d, want 0", len(items))
	}
}

// ── ListNamespaces ────────────────────────────────────────────────────────

func TestInMemoryStore_ListNamespaces(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("users", "alice", "prefs"), "k", "v")
	_ = st.Put(ctx, store.NS("users", "bob", "prefs"), "k", "v")
	_ = st.Put(ctx, store.NS("shared", "knowledge"), "k", "v")
	_ = st.Put(ctx, store.NS("actions", "completed"), "k", "v")

	all, err := st.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(all) != 4 {
		t.Errorf("ListNamespaces all got %d, want 4", len(all))
	}
}

func TestInMemoryStore_ListNamespacesPrefix(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("users", "alice"), "k", "v")
	_ = st.Put(ctx, store.NS("users", "bob"), "k", "v")
	_ = st.Put(ctx, store.NS("shared"), "k", "v")

	all, _ := st.ListNamespaces(ctx, store.WithListPrefix(store.NS("users")))
	if len(all) != 2 {
		t.Errorf("prefix users got %d, want 2", len(all))
	}
	for _, ns := range all {
		if !store.NS("users").IsPrefixOf(ns) {
			t.Errorf("returned ns %v not under prefix users", ns)
		}
	}
}

func TestInMemoryStore_ListNamespacesSuffix(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("users", "alice", "prefs"), "k", "v")
	_ = st.Put(ctx, store.NS("groups", "team1", "prefs"), "k", "v")
	_ = st.Put(ctx, store.NS("users", "alice", "memories"), "k", "v")

	all, _ := st.ListNamespaces(ctx, store.WithListSuffix(store.NS("prefs")))
	if len(all) != 2 {
		t.Errorf("suffix prefs got %d, want 2", len(all))
	}
}

func TestInMemoryStore_ListNamespacesMaxDepth(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("users", "alice", "prefs"), "k", "v")
	_ = st.Put(ctx, store.NS("users", "alice", "memories"), "k", "v")
	_ = st.Put(ctx, store.NS("users", "bob", "prefs"), "k", "v")

	// maxDepth=2 截断到 ("users", "alice") + ("users", "bob") 2 条去重
	all, _ := st.ListNamespaces(ctx, store.WithListMaxDepth(2))
	if len(all) != 2 {
		t.Errorf("maxDepth 2 got %d, want 2 (dedup), all=%v", len(all), all)
	}

	// maxDepth=1 截断到 ("users",) 1 条
	all, _ = st.ListNamespaces(ctx, store.WithListMaxDepth(1))
	if len(all) != 1 {
		t.Errorf("maxDepth 1 got %d, want 1, all=%v", len(all), all)
	}
}

// ── Batch ─────────────────────────────────────────────────────────────────

func TestInMemoryStore_BatchMixed(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	_ = st.Put(ctx, store.NS("users", "alice"), "theme", "dark")

	ops := []store.Op{
		store.GetOp{Namespace: store.NS("users", "alice"), Key: "theme"},
		store.PutOp{Namespace: store.NS("users", "alice"), Key: "lang", Value: "zh"},
		store.SearchOp{NamespacePrefix: store.NS("users", "alice")},
	}
	results, err := st.Batch(ctx, ops)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Batch results len = %d, want 3", len(results))
	}

	// GetOp 返 Item
	if results[0].Err != nil {
		t.Errorf("GetOp err = %v", results[0].Err)
	}
	if results[0].Item == nil || results[0].Item.Value != "dark" {
		t.Errorf("GetOp Item = %v, want theme=dark", results[0].Item)
	}

	// PutOp 应成功
	if results[1].Err != nil {
		t.Errorf("PutOp err = %v", results[1].Err)
	}

	// SearchOp 拿 2 条（theme + lang）
	if results[2].Err != nil {
		t.Errorf("SearchOp err = %v", results[2].Err)
	}
	if len(results[2].Items) != 2 {
		t.Errorf("SearchOp Items len = %d, want 2", len(results[2].Items))
	}
}

func TestInMemoryStore_BatchPerOpErrorIsolation(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()

	ops := []store.Op{
		store.PutOp{Namespace: store.NS("valid"), Key: "k1", Value: "v1"},
		store.GetOp{Namespace: store.NS(), Key: "k"}, // invalid ns 触发错误
		store.PutOp{Namespace: store.NS("valid"), Key: "k2", Value: "v2"},
	}
	results, err := st.Batch(ctx, ops)
	if err != nil {
		t.Fatalf("Batch should not fail overall: %v", err)
	}
	// 守卫断言：先确认 results 长度——sanity-revert "Batch 首 Op 失败立即 return" 时
	// fail 信息精确指向 "results truncated" 而非后续 index panic
	if len(results) != 3 {
		t.Fatalf("per-Op isolation broken: results len = %d, want 3 (Batch returned early on first failure?)", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("op[0] PutOp should succeed: %v", results[0].Err)
	}
	if !errors.Is(results[1].Err, store.ErrEmptyNamespace) {
		t.Errorf("op[1] GetOp should fail with ErrEmptyNamespace: %v", results[1].Err)
	}
	if results[2].Err != nil {
		t.Errorf("op[2] PutOp should succeed (per-Op isolation): %v", results[2].Err)
	}

	// 验证 op[2] 真的执行了
	item, _ := st.Get(ctx, store.NS("valid"), "k2")
	if item == nil {
		t.Fatal("op[2] PutOp didn't actually execute (per-Op isolation broken): Get returned nil")
	}
	if item.Value != "v2" {
		t.Errorf("op[2] Value = %v, want v2", item.Value)
	}
}

// ── ctx 注入 ──────────────────────────────────────────────────────────────

func TestStore_WithContextFromContext(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)

	got := store.FromContext(ctx)
	if got != st {
		t.Errorf("FromContext = %v, want %v", got, st)
	}
}

func TestStore_FromContextEmpty(t *testing.T) {
	got := store.FromContext(context.Background())
	if got != nil {
		t.Errorf("FromContext empty ctx = %v, want nil", got)
	}
}

func TestStore_WithContextTypedNil(t *testing.T) {
	// 业务陷阱：typed nil pointer
	var nilSt *store.InMemoryStore
	ctx := store.WithContext(context.Background(), nilSt)

	// FromContext 应该返 untyped nil（业务 if st == nil 守卫生效）
	got := store.FromContext(ctx)
	if got != nil {
		t.Errorf("FromContext typed nil should be untyped nil, got %v", got)
	}
}

// ── 并发 ──────────────────────────────────────────────────────────────────

// TestInMemoryStore_ConcurrentReadWrite 验证 sync.RWMutex 真防 race。
//
// **sanity-revert 设计**（PR 1 audit 视角 1 MED-2）：
// Value 用 map[string]any 让 race detector 能抓到通过 *Item 字段写的真 race；
// 50 routine 撞同 26 个 key（i%26）让 Put 覆盖路径 `existing.Value=...; existing.UpdatedAt=...`
// 多次并发写同 *Item 触发 race。把 s.mu 去掉跑此测试应 race detector 立即报错。
func TestInMemoryStore_ConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("users", "alice")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		// Put 覆盖路径并发——i%26 让多 routine 撞同 key，race detector 抓 *Item 字段写竞争
		go func(i int) {
			defer wg.Done()
			_ = st.Put(ctx, ns, keyN(i), map[string]any{"n": i, "tag": "v1"})
		}(i)
		go func(i int) {
			defer wg.Done()
			_ = st.Put(ctx, ns, keyN(i), map[string]any{"n": i + 100, "tag": "v2"})
		}(i)
		// Get 并发读——撞同 key 的 Put 时读跟写 *Item.Value 真 race
		go func(i int) {
			defer wg.Done()
			_, _ = st.Get(ctx, ns, keyN(i))
		}(i)
	}
	wg.Wait()
	// race detector 在 go test -race 下抓违规——这里只验无 panic + 无 race report
}

// ── helper ────────────────────────────────────────────────────────────────

func keyN(i int) string {
	return string(rune('a' + i%26))
}
