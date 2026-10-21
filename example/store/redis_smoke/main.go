// redis_smoke 演示 crew-go/redis 业务接 Redis backend 跑 Store + Checkpointer
// 最小闭环.
//
// **场景**: miniredis fake (生产用真 Redis localhost:6379, env 切换):
//
//	go run ./example/store/redis_smoke                            # 默认 miniredis
//	REDIS_ADDR=localhost:6379 go run ./example/store/redis_smoke  # 接真 Redis
//
// **演示**:
//  1. store.Put / Get / Search (跨 thread 长期 KV)
//  2. Checkpointer.SaveNS / LoadNS / ListPending (HITL Resume cp)
//  3. GraphID 冲突 ErrGraphIDConflict 防御
//  4. TTL (Redis 原生 EXPIRE)
//
// **不依赖真 LLM** — 纯 store/cp API 调用.
//
// 全场景 PASS + 退出码 0 表示通过.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"crew-go/compose_v2"
	agentRedis "crew-go/redis"
	"crew-go/store"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("redis_smoke PASS")
}

func run() error {
	ctx := context.Background()

	// 1. 创建 Redis client — env REDIS_ADDR 切换真/fake
	rdb, cleanup, err := makeRedisClient()
	if err != nil {
		return fmt.Errorf("makeRedisClient: %w", err)
	}
	defer cleanup()

	// 2. Demo Store: Put / Get / Search
	if err := demoStore(ctx, rdb); err != nil {
		return fmt.Errorf("demoStore: %w", err)
	}

	// 3. Demo Checkpointer: SaveNS / LoadNS / ListPending
	if err := demoCheckpointer(ctx, rdb); err != nil {
		return fmt.Errorf("demoCheckpointer: %w", err)
	}

	// 4. Demo GraphID conflict 防御
	if err := demoGraphIDConflict(ctx, rdb); err != nil {
		return fmt.Errorf("demoGraphIDConflict: %w", err)
	}

	// 5. Demo TTL (Redis 原生 EXPIRE)
	if err := demoTTL(ctx, rdb); err != nil {
		return fmt.Errorf("demoTTL: %w", err)
	}

	return nil
}

// makeRedisClient 创建 Redis client — env REDIS_ADDR 切换:
//   - 默认: miniredis fake (零部署本地跑 demo)
//   - REDIS_ADDR=host:port: 真 Redis (业务联调用)
func makeRedisClient() (*goredis.Client, func(), error) {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rdb := goredis.NewClient(&goredis.Options{Addr: addr})
		// 验连接
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			return nil, nil, fmt.Errorf("ping real Redis %s: %w", addr, err)
		}
		fmt.Printf("[redis_smoke] connected to real Redis at %s\n", addr)
		return rdb, func() { _ = rdb.Close() }, nil
	}

	mr, err := miniredis.Run()
	if err != nil {
		return nil, nil, fmt.Errorf("miniredis.Run: %w", err)
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	fmt.Printf("[redis_smoke] using miniredis fake (set REDIS_ADDR=host:port to use real Redis)\n")
	return rdb, func() {
		_ = rdb.Close()
		mr.Close()
	}, nil
}

func demoStore(ctx context.Context, rdb *goredis.Client) error {
	st := agentRedis.NewStore(rdb, agentRedis.WithStorePrefix("smoke"))

	ns := store.NS("users", "alice", "preferences")

	// Put
	if err := st.Put(ctx, ns, "lang", "Go"); err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	if err := st.Put(ctx, ns, "ide", "vscode"); err != nil {
		return fmt.Errorf("Put: %w", err)
	}

	// Get
	item, err := st.Get(ctx, ns, "lang")
	if err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	if item == nil || item.Value != "Go" {
		return fmt.Errorf("expected lang=Go, got %v", item)
	}
	fmt.Printf("[demoStore] Get(lang) = %q (CreatedAt=%v)\n", item.Value, item.CreatedAt.Format(time.RFC3339))

	// Search ns prefix
	items, err := st.Search(ctx, store.NS("users", "alice"))
	if err != nil {
		return fmt.Errorf("Search: %w", err)
	}
	if len(items) != 2 {
		return fmt.Errorf("expected 2 items, got %d", len(items))
	}
	fmt.Printf("[demoStore] Search(users/alice) returned %d items\n", len(items))

	// Delete + 验幂等
	if err := st.Delete(ctx, ns, "lang"); err != nil {
		return fmt.Errorf("Delete: %w", err)
	}
	if err := st.Delete(ctx, ns, "lang"); err != nil {
		return fmt.Errorf("Delete idempotent: %w", err)
	}
	fmt.Printf("[demoStore] Delete + 幂等 OK\n")

	return nil
}

func demoCheckpointer(ctx context.Context, rdb *goredis.Client) error {
	cp := agentRedis.NewCheckpointer(rdb, agentRedis.WithCheckpointerPrefix("smoke"))

	// SaveNS
	err := cp.SaveNS(ctx, "sid_demo", "ns1", &compose_v2.Checkpoint{
		GraphID:   "demo_graph",
		Status:    compose_v2.CPStatusPending,
		CreatedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("SaveNS: %w", err)
	}

	// LoadNS
	loaded, err := cp.LoadNS(ctx, "sid_demo", "ns1")
	if err != nil {
		return fmt.Errorf("LoadNS: %w", err)
	}
	if loaded == nil || loaded.GraphID != "demo_graph" {
		return fmt.Errorf("expected GraphID=demo_graph, got %v", loaded)
	}
	fmt.Printf("[demoCheckpointer] LoadNS(sid_demo, ns1).GraphID = %q\n", loaded.GraphID)

	// ListPending
	pending, err := cp.ListPending(ctx, "sid_demo")
	if err != nil {
		return fmt.Errorf("ListPending: %w", err)
	}
	if len(pending) != 1 {
		return fmt.Errorf("expected 1 pending, got %d", len(pending))
	}
	fmt.Printf("[demoCheckpointer] ListPending(sid_demo) = %d pending cp\n", len(pending))

	// DeleteSubtree
	n, err := cp.DeleteSubtree(ctx, "sid_demo", "")
	if err != nil {
		return fmt.Errorf("DeleteSubtree: %w", err)
	}
	fmt.Printf("[demoCheckpointer] DeleteSubtree(sid_demo) deleted %d cp\n", n)

	return nil
}

func demoGraphIDConflict(ctx context.Context, rdb *goredis.Client) error {
	cp := agentRedis.NewCheckpointer(rdb, agentRedis.WithCheckpointerPrefix("smoke_conflict"))

	// 写 cp.GraphID=A
	err := cp.SaveNS(ctx, "sid_x", "ns_x", &compose_v2.Checkpoint{
		GraphID:   "graph_A",
		Status:    compose_v2.CPStatusPending,
		CreatedAt: 100,
	})
	if err != nil {
		return fmt.Errorf("SaveNS graph_A: %w", err)
	}

	// 改 cp.GraphID=B 再写 — 应返 ErrGraphIDConflict
	err = cp.SaveNS(ctx, "sid_x", "ns_x", &compose_v2.Checkpoint{
		GraphID:   "graph_B",
		Status:    compose_v2.CPStatusPending,
		CreatedAt: 200,
	})
	if err == nil {
		return fmt.Errorf("expected ErrGraphIDConflict, got nil")
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		return fmt.Errorf("expected ErrGraphIDConflict, got %v", err)
	}
	fmt.Printf("[demoGraphIDConflict] graph_B vs existing graph_A → %v ✓\n", err)
	return nil
}

func demoTTL(ctx context.Context, rdb *goredis.Client) error {
	st := agentRedis.NewStore(rdb, agentRedis.WithStorePrefix("smoke_ttl"))

	// 验 TTLAware capability
	ta, ok := store.Store(st).(store.TTLAware)
	if !ok || !ta.SupportsTTL() {
		return fmt.Errorf("redis.Store should implement TTLAware + SupportsTTL=true")
	}

	ns := store.NS("ttl_demo")
	err := st.Put(ctx, ns, "transient", "expires_soon", store.WithTTL(200*time.Millisecond))
	if err != nil {
		return fmt.Errorf("Put WithTTL: %w", err)
	}

	// 立即 Get OK
	item, _ := st.Get(ctx, ns, "transient")
	if item == nil {
		return fmt.Errorf("expected item before TTL expire")
	}
	fmt.Printf("[demoTTL] Put WithTTL(200ms) + 立即 Get → %q\n", item.Value)

	// 真 Redis 等 500ms 后 Get nil; miniredis 需 FastForward — demo 跳过过期验证
	// (miniredis 时钟需手动推, 真 Redis 跑 conformance TTL_Expires 子测试已覆盖)
	fmt.Printf("[demoTTL] TTL expire 验证留 conformance / 真 Redis 部署测\n")
	return nil
}
