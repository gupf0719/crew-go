package sql

import (
	"context"
	dbsql "database/sql"
	"fmt"

	"crew-go/compose_v2"
)

// ClaimResume 使用条件写入所在的事务锁保护存档世代和任务租约。
func (c *Checkpointer) ClaimResume(ctx context.Context, sid, ns string, expected, source *compose_v2.Checkpoint) (*compose_v2.Checkpoint, error) {
	tx, err := c.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("sql checkpoint resume begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 已提交时回滚不改变结果。
	if token := compose_v2.CheckpointTaskLeaseToken(ctx, c.CheckpointTaskLeases(), sid); token != "" {
		if err := c.checkTaskLease(ctx, tx, sid, token); err != nil {
			return nil, fmt.Errorf("sql checkpoint resume lease: %w", err)
		}
	}
	if err := c.lockCheckpointRow(ctx, tx, sid, ns); err != nil {
		return nil, fmt.Errorf("sql checkpoint resume lock: %w", err)
	}
	ph := c.dialect.Placeholder
	var raw []byte
	query := fmt.Sprintf("SELECT payload FROM %s WHERE sid = %s AND ns = %s", c.table, ph(1), ph(2))
	if err := tx.QueryRowContext(ctx, query, sid, ns).Scan(&raw); err != nil {
		return nil, fmt.Errorf("sql checkpoint resume read: %w", err)
	}
	current, err := compose_v2.UnmarshalCheckpoint(raw)
	if err != nil {
		return nil, err
	}
	if current.ID == "" {
		current = nil // 只有刚锁住的空槽占位档没有 sid。
	}
	claimed, err := compose_v2.PrepareResumeCheckpoint(sid, ns, current, expected, source)
	if err != nil {
		return nil, err
	}
	payload, err := compose_v2.MarshalCheckpoint(claimed)
	if err != nil {
		return nil, err
	}
	if err := c.writeCheckpointRow(ctx, tx, sid, ns, claimed, payload); err != nil {
		return nil, fmt.Errorf("sql checkpoint resume write: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sql checkpoint resume commit: %w", err)
	}
	return claimed, nil
}
