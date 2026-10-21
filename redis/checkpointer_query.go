package redis

import (
	"context"
	"fmt"
	"sort"

	"crew-go/compose_v2"
)

// ListPending 实现 compose_v2.Checkpointer.ListPending.
//
// 返 sessionID 下 Status=CPStatusPending 的全部 cp.
//
// **跨 sid 隔离严格** (audit iter-3 H4 协议) — `pending:{sid}` SET 按 sid 隔离.
func (c *Checkpointer) ListPending(ctx context.Context, sessionID string) ([]compose_v2.NSCheckpoint, error) {
	pendingNS, err := c.rdb.SMembers(ctx, cpPendingKey(c.prefix)+":"+sessionID).Result()
	if err != nil {
		return nil, fmt.Errorf("redis cp ListPending smembers: %w", err)
	}
	if len(pendingNS) == 0 {
		return []compose_v2.NSCheckpoint{}, nil
	}

	sort.Strings(pendingNS)
	return c.loadCheckpoints(ctx, sessionID, pendingNS)
}

// LoadAllInSession 实现 compose_v2.Checkpointer.LoadAllInSession.
func (c *Checkpointer) LoadAllInSession(ctx context.Context, sessionID string) ([]compose_v2.NSCheckpoint, error) {
	allNS, err := c.rdb.SMembers(ctx, cpNSIndexKey(c.prefix, sessionID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis cp LoadAllInSession smembers: %w", err)
	}
	if len(allNS) == 0 {
		return []compose_v2.NSCheckpoint{}, nil
	}

	sort.Strings(allNS)
	return c.loadCheckpoints(ctx, sessionID, allNS)
}

// loadCheckpoints 共享 helper — 给 ListPending / LoadAllInSession 用.
// 批量 MGET 拿 payload, 反序列化为 NSCheckpoint.
//
// **stale 容忍**: SMEMBERS 拿的 ns 可能在并发场景下 cp 已被 DEL — MGET 返 nil
// 跳过 (跟 sql ORDER BY 同款 best-effort).
//
// **iter-5-K MGET chunk** (verification audit M2): 大 sid (>1000 ns) 时拆
// `cpMGetChunkSize` 一批避 Redis server-side block.
func (c *Checkpointer) loadCheckpoints(ctx context.Context, sessionID string, nsList []string) ([]compose_v2.NSCheckpoint, error) {
	if len(nsList) == 0 {
		return []compose_v2.NSCheckpoint{}, nil
	}

	keys := make([]string, len(nsList))
	for i, ns := range nsList {
		keys[i] = cpItemKey(c.prefix, sessionID, ns)
	}

	rawValues, err := c.mgetChunked(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("redis cp mget: %w", err)
	}

	result := make([]compose_v2.NSCheckpoint, 0, len(rawValues))
	for i, raw := range rawValues {
		if raw == nil {
			// 并发 delete — 跳过
			continue
		}
		rawStr, ok := raw.(string)
		if !ok {
			continue
		}
		cp, err := compose_v2.UnmarshalCheckpoint([]byte(rawStr))
		if err != nil {
			return nil, fmt.Errorf("redis cp unmarshal ns=%q: %w", nsList[i], err)
		}
		result = append(result, compose_v2.NSCheckpoint{NS: nsList[i], Checkpoint: cp})
	}
	return result, nil
}

// cpMGetChunkSize 是 cp MGET 单批最大 key 数 (跟 store.mgetChunkSize 同款 1000).
const cpMGetChunkSize = 1000

// mgetChunked 批量 MGET 大集合 key 拆 cpMGetChunkSize 一批 + ctx.Done 早退.
//
// **iter-5-K 修** (verification audit M2 + M3): 大 session (1000+ ns) 时一次性
// MGET 占 Redis server-side block 全部 client. 拆 chunk + 早退.
func (c *Checkpointer) mgetChunked(ctx context.Context, keys []string) ([]any, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) <= cpMGetChunkSize {
		return c.rdb.MGet(ctx, keys...).Result()
	}

	all := make([]any, 0, len(keys))
	for i := 0; i < len(keys); i += cpMGetChunkSize {
		if err := ctx.Err(); err != nil {
			return all, fmt.Errorf("mget ctx: %w", err)
		}
		end := i + cpMGetChunkSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk, err := c.rdb.MGet(ctx, keys[i:end]...).Result()
		if err != nil {
			return all, fmt.Errorf("mget chunk %d-%d: %w", i, end, err)
		}
		all = append(all, chunk...)
	}
	return all, nil
}
