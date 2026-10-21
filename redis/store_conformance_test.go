package redis_test

import (
	"sync/atomic"
	"testing"
	"time"

	"crew-go/redis"
	"crew-go/store"
	"crew-go/store/conformance"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestRedisStore_Conformance 跑 store/conformance 套件验 redis.Store 语义合规.
//
// **miniredis**: 内存 Redis fake, 跑测试零依赖. 协议兼容 ~95% (纯原语
// GET/SET/SADD/SCAN/EXPIRE 完整支持).
//
// **TTL_Expires 子测试**: miniredis 用 fake clock 不响应真实 time.Sleep,
// 后台 goroutine 定期 `mr.FastForward(50ms)` 推时间, 让 conformance TTL_Expires
// 子测试 (200ms TTL + 500ms sleep) 在 miniredis 下也能验证 EXPIRE 协议.
//
// 业务真接 Redis 容器跑测试: 用 `REDIS_ADDR` env 驱动切换 (Phase 2 可加).
func TestRedisStore_Conformance(t *testing.T) {
	conformance.RunStoreTests(t, func() store.Store {
		mr := miniredis.RunT(t)
		rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })

		// 后台 goroutine 推 miniredis fake clock 让 TTL_Expires 子测试能过.
		// goroutine 在 t.Cleanup 关时退出 (避免 leak).
		stop := make(chan struct{})
		var stopped atomic.Bool
		t.Cleanup(func() {
			if stopped.CompareAndSwap(false, true) {
				close(stop)
			}
		})
		go func() {
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					mr.FastForward(50 * time.Millisecond)
				}
			}
		}()

		return redis.NewStore(rdb)
	})
}

// TestRedisStore_TTLAware_SupportsTTL_True 验 redis.Store 实现 TTLAware 返 true.
// Redis 原生 EXPIRE 支持 TTL.
func TestRedisStore_TTLAware_SupportsTTL_True(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	st := redis.NewStore(rdb)
	ta, ok := store.Store(st).(store.TTLAware)
	if !ok {
		t.Fatal("redis.Store should implement store.TTLAware")
	}
	if !ta.SupportsTTL() {
		t.Error("redis.Store.SupportsTTL() should return true")
	}
}
