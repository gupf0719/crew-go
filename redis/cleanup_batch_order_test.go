package redis_test

// 分批 Cleanup（`WithCleanupBatchSize`）必须**先删最旧的**。
//
// # 为什么这条不删掉就会漏
//
// 分批的场景是"sid 太多、一次删不完，配合多轮 cron 收敛"。若每轮删的是可删集合里
// **最新**的那批，最旧的会永远排在末尾——持续负载下（新会话不断进来、可删集合不断
// 补充）它们跨多少轮都收不掉，存储只增不减。
//
// 这个 bug 在框架给 cp 盖时间戳之前是**看不出来的**：那时所有 sid 的 createdAt 全等，
// "最旧先删"只是一句空话，排序退化成 sid 字典序。盖章之后排序第一次有了语义，方向
// 错了才显现出来。

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	agentRedis "crew-go/redis"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestCleanup_BatchDeletesOldestFirst(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	ctx := context.Background()

	// batchSize=2：一轮只删两个
	cper := agentRedis.NewCheckpointer(rdb, agentRedis.WithCleanupBatchSize(2))

	// sid_0 最旧 → sid_5 最新（CreatedAt 显式递增，不依赖墙钟粒度）
	const n = 6
	for i := 0; i < n; i++ {
		sid := string(rune('0'+i)) + "_sid"
		err := cper.SaveNS(ctx, "sid_"+sid[:1], "", &compose_v2.Checkpoint{
			GraphID: "g", Status: compose_v2.CPStatusPending,
			CreatedAt: int64(1000 + i*10),
		})
		if err != nil {
			t.Fatalf("SaveNS %d: %v", i, err)
		}
	}

	// keepLastN=2 → 可删 4 个（sid_0..sid_3），batchSize=2 → 本轮删 2 个
	deleted, err := cper.Cleanup(ctx, 2)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("batchSize=2 应删 2 个 cp，实际 %d", deleted)
	}

	var alive []string
	for i := 0; i < n; i++ {
		sid := "sid_" + string(rune('0'+i))
		ok, err := cper.ExistsNS(ctx, sid, "")
		if err != nil {
			t.Fatalf("ExistsNS %s: %v", sid, err)
		}
		if ok {
			alive = append(alive, sid)
		}
	}

	// 保留最新 2 个（sid_4 sid_5，keepLastN 语义）+ 本轮没轮到的 2 个。
	// 被删的应该是**最旧**的 sid_0 sid_1。
	want := []string{"sid_2", "sid_3", "sid_4", "sid_5"}
	if len(alive) != len(want) {
		t.Fatalf("存活数不对: got %v, want %v", alive, want)
	}
	for i := range want {
		if alive[i] != want[i] {
			t.Errorf("分批删的不是最旧的那批: 存活 %v, 期望 %v\n"+
				"  scores 是 createdAt DESC，取头部 = 删可删集合里**最新**的 →\n"+
				"  最旧的永远排在末尾，持续负载下跨多少轮 cron 都收不掉（存储只增不减）",
				alive, want)
			break
		}
	}
}
