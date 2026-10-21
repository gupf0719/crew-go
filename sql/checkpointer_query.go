package sql

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
)

// ListPending 实现 compose_v2.Checkpointer.ListPending.
//
// 返 sessionID 下 Status=CPStatusPending 的全部 cp. 跨 sid 隔离严格 (audit
// iter-3 H4 cross-sid isolation 协议 — 业务 PG 漏 WHERE sid=? 时 SaaS 数据
// 泄露).
func (c *Checkpointer) ListPending(ctx context.Context, sessionID string) ([]compose_v2.NSCheckpoint, error) {
	ph := c.dialect.Placeholder
	q := fmt.Sprintf(
		`SELECT ns, payload FROM %s WHERE sid = %s AND status = %s ORDER BY ns`,
		c.table, ph(1), ph(2),
	)
	return c.queryNSCheckpoints(ctx, q, sessionID, int(compose_v2.CPStatusPending))
}

// LoadAllInSession 实现 compose_v2.Checkpointer.LoadAllInSession.
func (c *Checkpointer) LoadAllInSession(ctx context.Context, sessionID string) ([]compose_v2.NSCheckpoint, error) {
	ph := c.dialect.Placeholder
	q := fmt.Sprintf(
		`SELECT ns, payload FROM %s WHERE sid = %s ORDER BY ns`,
		c.table, ph(1),
	)
	return c.queryNSCheckpoints(ctx, q, sessionID)
}

// Cleanup 实现 compose_v2.Checkpointer.Cleanup.
//
// **协议** (checkpointer.go:77-81):
//   - 按 sid 维度排序 (sid 下所有 ns cp 中**最大** CreatedAt 当 sid 排序键)
//   - 保留最新 keepLastN 个 sid, 其余整 sid 删除
//   - keepLastN <= 0 等价 0: 删全部 sid
//   - 返删除的 cp 总数
//
// **SQL 实现**: 子查询 `SELECT sid GROUP BY sid ORDER BY MAX(created_at) DESC
// LIMIT keepLastN` 拿"保留 sid 集合", 然后 `DELETE WHERE sid NOT IN (...)`.
// 跨 backend 同款 SQL (PG/MySQL 都支持 GROUP BY + MAX + ORDER BY 子查询).
//
// **生产警告 (audit iter-5 视角 4 H6)**:
//
//   - 大表 (>100K sid / >1M cp 行) Cleanup 会全表扫 + 长事务 + 索引锁,
//     执行时长 5-30s, 期间 SaveNS/LoadNS 阻塞业务雪崩
//   - DELETE WHERE NOT IN (大集合) 业务 keepLastN=10000 时 IN 子句 10K 参数,
//     PG 内存 spike / MySQL `max_allowed_packet` 64MB 默认会爆
//
// **生产部署建议**:
//   - 低峰时间跑 (e.g., 凌晨 cron)
//   - 业务自分 batch (每批 1000 sid, 间隔 60s) 避锁表雪崩
//   - 业务 keepLastN 不要太大 (推荐 < 1000)
//   - 大表场景考虑业务自管 Cleanup (TRUNCATE PARTITION / 分区表), 不调本 API
func (c *Checkpointer) Cleanup(ctx context.Context, keepLastN int32) (int32, error) {
	ph := c.dialect.Placeholder

	if keepLastN <= 0 {
		// 删全部
		result, err := c.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, c.table))
		if err != nil {
			return 0, fmt.Errorf("sql cp Cleanup all: %w", err)
		}
		n, _ := result.RowsAffected()
		return int32(n), nil
	}

	// 保留最新 keepLastN 个 sid (按 MAX(created_at) DESC)
	// SQL: DELETE WHERE sid NOT IN (SELECT sid FROM table GROUP BY sid ORDER BY MAX(created_at) DESC LIMIT N)
	//
	// MySQL 5.7 不支持子查询 LIMIT in DELETE — 用 IN 临时表 workaround.
	// PG 直接支持子查询 LIMIT in DELETE.
	//
	// PoC 简化: 先 SELECT 拿保留 sid 列表, 再 DELETE WHERE sid NOT IN (列表) —
	// 两步 SQL, 跨 backend 兼容性最好.
	q := fmt.Sprintf(
		// 二级 sid ASC: created_at 撞值时 (同 sid 下多 ns 并发写常见, 纳秒时间戳在
		// darwin 上实测只有微秒粒度) 保哪个 sid 不能由 DB 心情决定 —— 显式 tie-break
		// 保跨 backend 确定性.
		`SELECT sid FROM %s GROUP BY sid ORDER BY MAX(created_at) DESC, sid ASC LIMIT %s`,
		c.table, ph(1),
	)
	rows, err := c.db.QueryContext(ctx, q, keepLastN)
	if err != nil {
		return 0, fmt.Errorf("sql cp Cleanup select keep: %w", err)
	}
	defer rows.Close()

	keepSids := []string{}
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return 0, fmt.Errorf("sql cp Cleanup scan: %w", err)
		}
		keepSids = append(keepSids, sid)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sql cp Cleanup rows: %w", err)
	}

	// 如果 keepLastN >= 现有 sid 数, 不删任何
	if len(keepSids) == 0 {
		return 0, nil
	}

	// DELETE WHERE sid NOT IN (keepSids...)
	args := make([]any, len(keepSids))
	for i, sid := range keepSids {
		args[i] = sid
	}
	deleteQ := fmt.Sprintf(
		`DELETE FROM %s WHERE sid NOT IN (%s)`,
		c.table, placeholders(c.dialect, 1, len(keepSids)),
	)
	result, err := c.db.ExecContext(ctx, deleteQ, args...)
	if err != nil {
		return 0, fmt.Errorf("sql cp Cleanup delete: %w", err)
	}
	n, _ := result.RowsAffected()
	return int32(n), nil
}
