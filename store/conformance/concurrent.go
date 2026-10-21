package conformance

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/store"
)

// testConcurrent 验证并发安全 — 多 goroutine 同时 Put/Get/Delete 不数据丢失 / 不
// race / 不 panic.
func testConcurrent(t *testing.T, factory StoreFactory) {
	t.Run("Concurrent_Put_Different_Keys_No_Loss", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("concurrent_put")
		const N = 50

		// audit iter-2 视角 3 H1: CLAUDE.md "并发场景需用 WaitGroup 屏障 + 短超时
		// 强制真正并发, 否则 Go scheduler 会自然串行化掉 thunder-herd 测试"
		// 修前实测 0.00s 完成 = 100% 伪并发. 加 barrier 让 N goroutine 同瞬间释放.
		start := make(chan struct{})

		var wg sync.WaitGroup
		var errCount atomic.Int64
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-start // 等 barrier 释放
				key := fmt.Sprintf("k_%d", idx)
				if err := st.Put(ctx, ns, key, idx); err != nil {
					errCount.Add(1)
					t.Errorf("Put k_%d: %v", idx, err)
				}
			}(i)
		}
		// N 个 goroutine 都到 <-start 后一次性释放
		close(start)
		wg.Wait()

		if errCount.Load() > 0 {
			t.Fatalf("concurrent Put encountered %d errors", errCount.Load())
		}

		// 验证全部 N 条都写入
		items, err := st.Search(ctx, ns, store.WithLimit(N+10))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(items) != N {
			t.Errorf("expected %d items after concurrent Put, got %d", N, len(items))
		}
	})

	t.Run("Concurrent_Put_Same_Key_Some_Value_Visible", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("concurrent_same_key")
		const N = 20

		// audit iter-5 视角 1 H2 修: 用 string 值跨 backend safe (避 json.Number
		// 跟 int type assert 漂移 — sql backend JSON unmarshal 数字走 json.Number,
		// InMemory 不序列化保留 int, 跨 backend `.(int)` panic).
		// fixture 改用 "value_<idx>" string, 业务 type assert string 跨 backend OK.
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-start // barrier 强制真并发
				_ = st.Put(ctx, ns, "shared_key", fmt.Sprintf("value_%d", idx))
			}(i)
		}
		close(start)
		wg.Wait()

		// 最终值是 "value_0".."value_N-1" 中的某个
		item, err := st.Get(ctx, ns, "shared_key")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if item == nil {
			t.Fatal("expected item after concurrent Put, got nil")
		}
		// string type assert 跨 backend safe
		v, ok := item.Value.(string)
		if !ok {
			t.Fatalf("expected string value, got %T: %v", item.Value, item.Value)
		}
		// 验在并发写入范围内
		found := false
		for i := 0; i < N; i++ {
			if v == fmt.Sprintf("value_%d", i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected value in [value_0, value_%d), got %q (业务 backend 可能写错值)", N, v)
		}

		// 显式 overwrite 验证最后写入可见
		if err := st.Put(ctx, ns, "shared_key", "explicit_value"); err != nil {
			t.Fatalf("explicit Put: %v", err)
		}
		item, _ = st.Get(ctx, ns, "shared_key")
		if item == nil {
			t.Fatal("expected item after explicit Put")
		}
		if v, ok := item.Value.(string); !ok || v != "explicit_value" {
			t.Errorf("explicit Put should be visible, got %v (type %T)", item.Value, item.Value)
		}
	})

	t.Run("Concurrent_Read_Write_No_Race", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("rw_race")
		const N = 30

		// 预置数据
		for i := 0; i < N; i++ {
			_ = st.Put(ctx, ns, fmt.Sprintf("k_%d", i), i)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		// 一半 goroutine 读, 一半写 — barrier 强制真并发
		for i := 0; i < N; i++ {
			wg.Add(2)
			go func(idx int) {
				defer wg.Done()
				<-start
				_, _ = st.Get(ctx, ns, fmt.Sprintf("k_%d", idx))
			}(i)
			go func(idx int) {
				defer wg.Done()
				<-start
				_ = st.Put(ctx, ns, fmt.Sprintf("k_%d", idx), idx*2)
			}(i)
		}
		close(start)
		wg.Wait()
		// 通过 `go test -race` 验证无 data race
	})
}
