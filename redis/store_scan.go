package redis

import (
	"context"
	"fmt"
)

// mgetChunkSize 是 MGET 单批最大 key 数. Redis MGET 协议无硬上限但单大命令
// 在 Redis server 单线程跑期间 block 其他 client; 1000 是常见生产经验值
// (业务 1000 key MGET ~10ms server-side, 可接受).
const mgetChunkSize = 1000

// scanAll 跑 SCAN MATCH pattern 拿全部 key. 用于 Search / ListNamespaces.
//
// **iter-5-K ctx.Done 早退** (verification audit M3 同根): 主循环每 SCAN 一批
// 检 ctx.Err(), 业务 deadline 时不再继续拉.
//
// **生产警告**: SCAN 在大集合 (>10万 key) 性能差 (O(N) iteration). PoC 接受;
// 业务大规模建议接 RediSearch 后端 / mem0.
//
// COUNT 默认 100 (每次 SCAN 拿 100 key 减少 round-trip).
func (s *Store) scanAll(ctx context.Context, pattern string) ([]string, error) {
	var (
		cursor uint64
		all    []string
	)
	for {
		if err := ctx.Err(); err != nil {
			return all, fmt.Errorf("scan ctx: %w", err)
		}
		keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan %q: %w", pattern, err)
		}
		all = append(all, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return all, nil
}

// mgetChunked 批量 MGET 大集合 key 拆 mgetChunkSize 一批避免 Redis server-side
// block. 跑期间检 ctx.Err() 早退.
//
// **iter-5-K 修** (verification audit M2 + M3 同根): 原 MGET keys[]... 一次性
// 传任意长度 — 100万 key 单命令 buffer ~50MB+ Redis server 单线程 block 全部
// client. 拆 1000 一批 + 检 ctx 早退.
func (s *Store) mgetChunked(ctx context.Context, keys []string) ([]any, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) <= mgetChunkSize {
		return s.rdb.MGet(ctx, keys...).Result()
	}

	all := make([]any, 0, len(keys))
	for i := 0; i < len(keys); i += mgetChunkSize {
		if err := ctx.Err(); err != nil {
			return all, fmt.Errorf("mget ctx: %w", err)
		}
		end := i + mgetChunkSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk, err := s.rdb.MGet(ctx, keys[i:end]...).Result()
		if err != nil {
			return all, fmt.Errorf("mget chunk %d-%d: %w", i, end, err)
		}
		all = append(all, chunk...)
	}
	return all, nil
}
