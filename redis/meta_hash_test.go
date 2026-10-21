package redis_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	agentRedis "crew-go/redis"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestSaveNS_Writes_Meta_Hash_All_Fields 验 iter-5-K 加的 meta hash 写入路径 —
// 直接 HGETALL 断言 SaveNS 后 meta hash 3 字段全部正确写入.
//
// **iter-5-M round audit 视角 4 HIGH-1 修** (5 类炸弹 #4 双层防护遮蔽): 原 meta
// hash 只靠 DeleteSubtree 间接覆盖. 加正向 HGETALL 断言让 meta hash 写入
// regression 直接抓.
//
// **sanity-revert 验证**: saveNSScript 删 HSET meta_key 任一字段行 → 此测试 fail
// 精确指向缺字段.
func TestSaveNS_Writes_Meta_Hash_All_Fields(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	cp := agentRedis.NewCheckpointer(rdb)
	ctx := context.Background()

	err := cp.SaveNS(ctx, "sid_test", "ns_test", &compose_v2.Checkpoint{
		GraphID:   "g_alpha",
		Status:    compose_v2.CPStatusPending,
		CreatedAt: 12345,
	})
	if err != nil {
		t.Fatalf("SaveNS: %v", err)
	}

	// 直接 HGETALL 验 meta hash 3 字段
	metaKey := "crew:cp:meta:sid_test:ns_test"
	meta, err := rdb.HGetAll(ctx, metaKey).Result()
	if err != nil {
		t.Fatalf("HGETALL meta: %v", err)
	}

	expectations := map[string]string{
		"graph_id":   "g_alpha",
		"status":     "1", // CPStatusPending = 1
		"created_at": "12345",
	}
	for k, want := range expectations {
		got, ok := meta[k]
		if !ok {
			t.Errorf("meta hash missing field %q (业务 sanity-revert 删 HSET %q 时此处 fail 精确指向)", k, k)
			continue
		}
		if got != want {
			t.Errorf("meta[%q] = %q, want %q", k, got, want)
		}
	}
}

// TestDeleteSubtree_Removes_Meta_Hash 验 DeleteSubtree 删 cp 时同步删 meta hash.
//
// **iter-5-K**: DeleteSubtree Lua 脚本除了 DEL item key 也 DEL meta key (line 86).
// **sanity-revert**: 删 deleteSubtreeScript 的 `redis.call("DEL", meta_key)` 行 →
// 此测试 fail 验残留 meta hash.
func TestDeleteSubtree_Removes_Meta_Hash(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	cp := agentRedis.NewCheckpointer(rdb)
	ctx := context.Background()

	err := cp.SaveNS(ctx, "sid_d", "ns_d", &compose_v2.Checkpoint{
		GraphID:   "g",
		Status:    compose_v2.CPStatusPending,
		CreatedAt: 100,
	})
	if err != nil {
		t.Fatalf("SaveNS: %v", err)
	}

	metaKey := "crew:cp:meta:sid_d:ns_d"

	// 删前 meta 存在
	if n, _ := rdb.Exists(ctx, metaKey).Result(); n != 1 {
		t.Fatalf("meta key should exist before DeleteSubtree, got Exists=%d", n)
	}

	// DeleteSubtree
	if _, err := cp.DeleteSubtree(ctx, "sid_d", "ns_d"); err != nil {
		t.Fatalf("DeleteSubtree: %v", err)
	}

	// 删后 meta key 不存在 (iter-5-K Lua 内 DEL meta_key)
	if n, _ := rdb.Exists(ctx, metaKey).Result(); n != 0 {
		t.Errorf("meta key should be deleted after DeleteSubtree, got Exists=%d (sanity-revert 删 Lua DEL meta_key 时此处 fail)", n)
	}
}
