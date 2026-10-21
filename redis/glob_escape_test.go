package redis_test

// SCAN MATCH glob 转义哨兵（redis/key.go 的 escapeGlobPattern）。
//
// 钉的缺陷形态：namespace 段被原样嵌进 SCAN MATCH 模式，而 glob 的 `*` / `?` / `[`
// 是元字符、`store.Namespace.Validate` 只拒 `.` 与 NUL ⟹ `NS("a*")` 的 Search
// 模式变成 `...:item:a*:*`，**读到 `alpha` 的数据**。这一层没有二次筛兜底
// （解出的 ns 直接进结果），namespace 又是多租户隔离边界 ⟹ 越权读。
//
// sanity-revert：把 storeItemPatternForNS 里的 escapeGlobPattern 摘掉，本测试红在
// 「越界：读到别的 namespace」。

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"crew-go/redis"
	"crew-go/store"
)

func newProbeStore(t *testing.T) (*redis.Store, context.Context) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return redis.NewStore(rdb), context.Background()
}

// TestGlobEscape_NamespaceWithMetachars：含 glob 元字符的 namespace 只读到自己的数据。
func TestGlobEscape_NamespaceWithMetachars(t *testing.T) {
	cases := []struct {
		name      string
		attacker  store.Namespace // 名字含元字符，会越界的那个
		victim    store.Namespace // 不该被它读到
		victimSeg string
	}{
		{"星号", store.NS("a*"), store.NS("alpha"), "alpha"},
		{"问号", store.NS("a?c"), store.NS("abc"), "abc"},
		{"字符类", store.NS("a[bc]d"), store.NS("abd"), "abd"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, ctx := newProbeStore(t)
			if err := st.Put(ctx, c.victim, "vk", map[string]any{"v": "受害者私有"}); err != nil {
				t.Fatalf("put victim: %v", err)
			}
			if err := st.Put(ctx, c.attacker, "ak", map[string]any{"v": "自己的"}); err != nil {
				t.Fatalf("put attacker: %v", err)
			}
			got, err := st.Search(ctx, c.attacker)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			// 正向断言：自己的数据必须读得到（防「转义过头，谁都读不到」的退化）
			var sawOwn bool
			for _, it := range got {
				if it.Key == "ak" {
					sawOwn = true
				}
				if len(it.Namespace) > 0 && it.Namespace[0] == c.victimSeg {
					t.Errorf("越界：读到别的 namespace %q 的数据", c.victimSeg)
				}
			}
			if !sawOwn {
				t.Errorf("转义过头：自己的数据读不到了, got %d 条", len(got))
			}
		})
	}
}

// TestGlobEscape_DescendantStillWorks：转义不能破坏正常的后代匹配语义。
func TestGlobEscape_DescendantStillWorks(t *testing.T) {
	st, ctx := newProbeStore(t)
	if err := st.Put(ctx, store.NS("team", "sub"), "k", map[string]any{"v": 1}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := st.Put(ctx, store.NS("team"), "own", map[string]any{"v": 2}); err != nil {
		t.Fatalf("%v", err)
	}
	got, err := st.Search(ctx, store.NS("team"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(got) != 2 {
		t.Fatalf("自身 + 后代该都返回，got %d 条", len(got))
	}
}
