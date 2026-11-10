package sql

import (
	"context"
	"crypto/sha256"
	dbsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"crew-go/compose_v2"
)

// CheckpointTaskLeases 返回内建 SQL 方言的跨进程任务租约存储。
func (c *Checkpointer) CheckpointTaskLeases() compose_v2.TaskLeaseStore {
	if c.taskLeaseMillisSQL() == "" {
		return nil
	}
	return *c
}

func (c *Checkpointer) taskLeaseTable() string {
	table := c.table + "_task_lease"
	if len(table) <= 63 {
		return table
	}
	sum := sha256.Sum256([]byte(strings.ToLower(c.table)))
	return fmt.Sprintf("crew_cp_task_lease_%x", sum[:16])
}

func (c *Checkpointer) taskLeaseMillisSQL() string {
	switch c.dialect.(type) {
	case Postgres:
		return "(FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)"
	case MySQL:
		return "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)"
	case SQLite:
		return "(CAST(strftime('%s','now') AS INTEGER) * 1000 + CAST(substr(strftime('%f','now'),4,3) AS INTEGER))"
	default:
		return ""
	}
}

// TryAcquireTaskLease 尝试占用空闲任务，已过期的租约可被接管。
func (c Checkpointer) TryAcquireTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error) {
	return c.updateTaskLease(ctx, sid, token, ttl, true)
}

// RenewTaskLease 延长当前仍有效且属于 token 的租约。
func (c Checkpointer) RenewTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error) {
	return c.updateTaskLease(ctx, sid, token, ttl, false)
}

func (c *Checkpointer) updateTaskLease(ctx context.Context, sid, token string, ttl time.Duration, acquire bool) (bool, error) {
	millis := c.taskLeaseMillisSQL()
	if millis == "" || sid == "" || token == "" || ttl.Milliseconds() <= 0 {
		return false, errors.New("sql cp: invalid task lease request")
	}
	tx, err := c.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }() // 已提交时无操作，否则回滚失败的事务。
	ph := c.dialect.Placeholder
	if acquire {
		// UPSERT 占住任务行。后续读时才取数据库时间，避免用锁等待前的时间判租约。
		q := fmt.Sprintf("INSERT INTO %s (sid, owner_token, expires_at) VALUES (%s, '', 0) %s",
			c.taskLeaseTable(), ph(1), c.dialect.UpsertSuffix([]string{"sid"}, []string{"sid"}))
		if _, err := tx.ExecContext(ctx, q, sid); err != nil {
			return false, err
		}
	} else if err := c.lockTaskLeaseRow(ctx, tx, sid); err != nil {
		return false, err
	}
	owner, err := c.activeTaskLeaseOwner(ctx, tx, sid)
	if err != nil || (owner != token && (!acquire || owner != "")) {
		return false, err
	}
	q := fmt.Sprintf("UPDATE %s SET owner_token = %s, expires_at = %s + %s WHERE sid = %s",
		c.taskLeaseTable(), ph(1), millis, ph(2), ph(3))
	if _, err := tx.ExecContext(ctx, q, token, ttl.Milliseconds(), sid); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ReleaseTaskLease 只移除属于 token 的租约记录。
func (c Checkpointer) ReleaseTaskLease(ctx context.Context, sid, token string) error {
	ph := c.dialect.Placeholder
	q := fmt.Sprintf("DELETE FROM %s WHERE sid = %s AND owner_token = %s", c.taskLeaseTable(), ph(1), ph(2))
	_, err := c.db.ExecContext(ctx, q, sid, token)
	return err
}

func (c *Checkpointer) lockTaskLeaseRow(ctx context.Context, tx *dbsql.Tx, sid string) error {
	// 空更新在各方言上都取得行的写锁。SQLite 先取得写事务，再查当前租约。
	q := fmt.Sprintf("UPDATE %s SET owner_token = owner_token WHERE sid = %s", c.taskLeaseTable(), c.dialect.Placeholder(1))
	_, err := tx.ExecContext(ctx, q, sid)
	return err
}

func (c *Checkpointer) activeTaskLeaseOwner(ctx context.Context, tx *dbsql.Tx, sid string) (string, error) {
	q := fmt.Sprintf("SELECT owner_token FROM %s WHERE sid = %s AND expires_at > %s",
		c.taskLeaseTable(), c.dialect.Placeholder(1), c.taskLeaseMillisSQL())
	var owner string
	if err := tx.QueryRowContext(ctx, q, sid).Scan(&owner); err != nil && !errors.Is(err, dbsql.ErrNoRows) {
		return "", err
	}
	return owner, nil
}

func (c *Checkpointer) checkTaskLease(ctx context.Context, tx *dbsql.Tx, sid, token string) error {
	if err := c.lockTaskLeaseRow(ctx, tx, sid); err != nil {
		return err
	}
	owner, err := c.activeTaskLeaseOwner(ctx, tx, sid)
	if err != nil {
		return err
	}
	if owner != token {
		return compose_v2.ErrTaskLeaseLost
	}
	return nil
}
