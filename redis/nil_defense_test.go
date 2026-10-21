package redis_test

import (
	"testing"

	agentRedis "crew-go/redis"
)

// TestNewStore_Nil_Panics 验 NewStore(nil) fail-fast panic (iter-5-J 根 D).
//
// **CLAUDE.md "公开 API 防御"**: 业务传 nil rdb 后第一次 Get/SaveNS 远栈
// panic 在 go-redis 包内部, 业务调用栈完全看不到根因. NewStore 顶部加 panic
// 让业务 init 时 fail-fast 友好.
//
// **sanity-revert 验证**: 删 NewStore 的 nil panic → 此测试 fail (期望 panic 但
// NewStore 正常返).
func TestNewStore_Nil_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewStore(nil) should panic, got no panic")
		}
	}()
	agentRedis.NewStore(nil)
}

// TestNewCheckpointer_Nil_Panics 验 NewCheckpointer(nil) fail-fast panic (iter-5-J 根 D).
func TestNewCheckpointer_Nil_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewCheckpointer(nil) should panic, got no panic")
		}
	}()
	agentRedis.NewCheckpointer(nil)
}
