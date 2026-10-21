package redis_test

import (
	"testing"

	"crew-go/compose_v2"
	"crew-go/compose_v2/checkpointer_conformance"
	"crew-go/redis"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestRedisCheckpointer_Conformance 跑 compose_v2/checkpointer_conformance 套件
// 验 redis.Checkpointer 语义合规.
//
// **覆盖**: 9 子测试 (SaveLoad / Exists / Delete / ListPending / LoadAllInSession /
// SaveBatch / GraphIDConflict / Cleanup / Concurrent).
func TestRedisCheckpointer_Conformance(t *testing.T) {
	checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
		mr := miniredis.RunT(t)
		rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return redis.NewCheckpointer(rdb)
	})
}
