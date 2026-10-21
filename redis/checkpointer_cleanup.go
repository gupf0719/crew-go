package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
)

// Cleanup 实现 compose_v2.Checkpointer.Cleanup.
//
// **协议**:
//   - 按 sid 维度排序 (sid 下所有 ns cp 中最大 CreatedAt 当 sid 排序键)
//   - 保留最新 keepLastN 个 sid, 其余整 sid 删除
//   - keepLastN <= 0 等价 0: 删全部 sid
//   - 返删除的 cp 总数
//
// **iter-5-M 分批断点续传** (round audit 视角 3 HIGH-2 修): 业务 `WithCleanupBatchSize`
// 设非 0 时单次 Cleanup 最多删该数量个 sid 避业务 ctx timeout 时跑不完, 业务多轮
// cron 跑收敛; 默认 0=不限 (一次删完). 删 sid 顺序: createdAt ASC (最旧的先删) +
// sid ASC 二次排序保稳定.
func (c *Checkpointer) Cleanup(ctx context.Context, keepLastN int32) (int32, error) {
	if keepLastN <= 0 {
		return c.cleanupAll(ctx)
	}

	// 拿全部 sid
	allSids, err := c.rdb.SMembers(ctx, c.cpSidsKey()).Result()
	if err != nil {
		return 0, fmt.Errorf("redis cp Cleanup smembers sids: %w", err)
	}
	if int32(len(allSids)) <= keepLastN {
		return 0, nil
	}

	// 对每 sid 计算 max CreatedAt (HGET meta 单字段查)
	type sidScore struct {
		sid       string
		createdAt int64
	}
	scores := make([]sidScore, 0, len(allSids))
	for _, sid := range allSids {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("redis cp Cleanup ctx: %w", err)
		}
		maxCreated, err := c.maxCreatedAtForSid(ctx, sid)
		if err != nil {
			return 0, fmt.Errorf("redis cp Cleanup max created sid=%q: %w", sid, err)
		}
		scores = append(scores, sidScore{sid: sid, createdAt: maxCreated})
	}

	// 排序: createdAt DESC, sid ASC tie-break
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].createdAt != scores[j].createdAt {
			return scores[i].createdAt > scores[j].createdAt
		}
		return scores[i].sid < scores[j].sid
	})

	// 保 keepLastN, 删其余 — 业务 WithCleanupBatchSize 设非 0 时单次最多删 N 个
	// (业务大 sid 数 ctx timeout 短时配合多轮 cron 跑收敛)
	toDelete := scores[keepLastN:]
	if c.cleanupBatchSize > 0 && len(toDelete) > c.cleanupBatchSize {
		// **取尾部**: scores 是 createdAt DESC, 可删集合的**尾部**才是最旧的。取头部
		// 会让每轮都删掉"可删集合里最新的那批", 最旧的那些永远排在末尾 —— 持续负载下
		// 跨多少轮 cron 都收不掉 (实测 6 sid / keep=2 / batch=2 → 存活
		// [sid_0 sid_1 sid_4 sid_5], 最旧的两个反而活下来)。
		toDelete = toDelete[len(toDelete)-c.cleanupBatchSize:]
	}

	var totalDeleted int32
	for _, sd := range toDelete {
		if err := ctx.Err(); err != nil {
			return totalDeleted, fmt.Errorf("redis cp Cleanup ctx: %w", err)
		}
		n, err := c.DeleteSubtree(ctx, sd.sid, "")
		if err != nil {
			return totalDeleted, fmt.Errorf("redis cp Cleanup delete sid=%q: %w", sd.sid, err)
		}
		totalDeleted += n
	}
	return totalDeleted, nil
}

// cleanupAll 删全部 sid 的 cp.
//
// **iter-5-K ctx.Done 早退**: 长 op 主循环每迭代检 ctx.Err().
func (c *Checkpointer) cleanupAll(ctx context.Context) (int32, error) {
	allSids, err := c.rdb.SMembers(ctx, c.cpSidsKey()).Result()
	if err != nil {
		return 0, fmt.Errorf("redis cp cleanupAll smembers: %w", err)
	}

	var total int32
	for _, sid := range allSids {
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("redis cp cleanupAll ctx: %w", err)
		}
		n, err := c.DeleteSubtree(ctx, sid, "")
		if err != nil {
			return total, fmt.Errorf("redis cp cleanupAll sid=%q: %w", sid, err)
		}
		total += n
	}
	return total, nil
}

// maxCreatedAtForSid 拿 sid 下所有 cp 的最大 CreatedAt (用于 Cleanup 按 sid 排序).
//
// **iter-5-K 修** (verification audit HIGH-2 同根 + 性能优化): 改用 HGET meta
// created_at 单字段查 (几十字节), 不再 MGET 整 payload + 反序列化 (可能 MB 级).
// 单字段查避免解析整 payload.
func (c *Checkpointer) maxCreatedAtForSid(ctx context.Context, sid string) (int64, error) {
	nsList, err := c.rdb.SMembers(ctx, cpNSIndexKey(c.prefix, sid)).Result()
	if err != nil {
		return 0, err
	}
	if len(nsList) == 0 {
		return 0, nil
	}

	// pipeline 批 HGET meta created_at — 减少 round-trip
	pipe := c.rdb.Pipeline()
	cmds := make([]*pipelineHGet, len(nsList))
	for i, ns := range nsList {
		cmds[i] = &pipelineHGet{
			cmd: pipe.HGet(ctx, cpMetaKey(c.prefix, sid, ns), "created_at"),
		}
	}
	// best-effort: 部分 HGET 返 nil (ns 已被 DeleteSubtree 并发删) — 不影响主路径
	_, _ = pipe.Exec(ctx)

	var maxCreated int64
	for _, c := range cmds {
		val, err := c.cmd.Result()
		if err != nil {
			continue // key 不存在 / 并发 delete — 跳过
		}
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			continue
		}
		if v > maxCreated {
			maxCreated = v
		}
	}
	return maxCreated, nil
}

// pipelineHGet wraps redis.StringCmd 让 maxCreatedAtForSid 命令缓存.
type pipelineHGet struct {
	cmd interface {
		Result() (string, error)
	}
}
