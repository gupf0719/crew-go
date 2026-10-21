package store_test

import (
	"context"
	"testing"

	"crew-go/store"
)

// TestSearch_SliceFilterValue_NoPanic 验证 N1——filter value 为 slice（不可比较类型）时
// ValueEqual 不 panic。matchValue → ValueEqual(got, want)，got/want 皆 slice 时原 `a == b`
// 触发 runtime panic（comparing uncomparable type []interface {}）。
//
// sanity：revert N1（DeepEqual 改回 a==b）后本测试 panic（ValueEqual slice 比较崩溃）。
func TestSearch_SliceFilterValue_NoPanic(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := context.Background()
	ns := store.NS("app", "u1")
	if err := st.Put(ctx, ns, "k1", map[string]any{"tags": []any{"a", "b"}}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// filter value 是 slice——精确匹配走 ValueEqual(slice, slice)
	items, err := st.Search(ctx, ns, store.WithFilter(map[string]any{"tags": []any{"a", "b"}}))
	if err != nil {
		t.Fatalf("Search slice filter 不应 error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("N1: slice filter DeepEqual 应匹配 1 item, got %d", len(items))
	}
	// 不同 slice 不匹配（DeepEqual 语义正确）
	items2, err := st.Search(ctx, ns, store.WithFilter(map[string]any{"tags": []any{"a", "c"}}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(items2) != 0 {
		t.Fatalf("N1: 不同 slice 不应匹配, got %d", len(items2))
	}
}

// TestGet_NamespaceElementWrite_NoPoison 验证 N2——Get 返回的 Item.Namespace 元素写不污染
// store 内部（原 shallow copy `*it` 只复制 slice header，got.Namespace[i]=x 改底层数组穿透）。
//
// sanity：revert N2（删 Get 的 Namespace deep-copy）后 got2.Namespace[0]=="hacked" FAIL。
func TestGet_NamespaceElementWrite_NoPoison(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := context.Background()
	ns := store.NS("app", "u1")
	if err := st.Put(ctx, ns, "k1", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := st.Get(ctx, ns, "k1")
	if err != nil || got == nil {
		t.Fatalf("Get: err=%v got=%v", err, got)
	}
	got.Namespace[0] = "hacked" // 元素写——原 shallow copy 会穿透污染 store 内部
	got2, err := st.Get(ctx, ns, "k1")
	if err != nil || got2 == nil {
		t.Fatalf("Get2: err=%v", err)
	}
	if got2.Namespace[0] != "app" {
		t.Fatalf("N2(Get): Namespace 元素写污染 store, got2.Namespace[0]=%q (want app)", got2.Namespace[0])
	}
}

// TestSearch_NamespaceElementWrite_NoPoison 验证 N2——Search 返回的 SearchItem.Namespace
// 同 Get 路径深 copy 防元素写污染。
//
// sanity：revert N2（删 Search 的 Namespace deep-copy）后二次 Search 结果被污染 FAIL。
func TestSearch_NamespaceElementWrite_NoPoison(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := context.Background()
	ns := store.NS("app", "u1")
	if err := st.Put(ctx, ns, "k1", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	items, err := st.Search(ctx, store.NS("app"))
	if err != nil || len(items) != 1 {
		t.Fatalf("Search: err=%v n=%d", err, len(items))
	}
	items[0].Namespace[1] = "hacked" // 元素写 u1→hacked
	items2, err := st.Search(ctx, store.NS("app"))
	if err != nil || len(items2) != 1 {
		t.Fatalf("Search2: err=%v n=%d", err, len(items2))
	}
	if items2[0].Namespace[1] != "u1" {
		t.Fatalf("N2(Search): Namespace 元素写污染 store, items2[0].Namespace[1]=%q (want u1)", items2[0].Namespace[1])
	}
}
