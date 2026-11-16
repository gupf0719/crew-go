package sql

import (
	"context"
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"crew-go/agent_v2/background"
	"crew-go/compose_v2"
)

// BackgroundTaskStore 把子任务状态和存档放在同一 SQL 数据库。
type BackgroundTaskStore struct {
	*Checkpointer
	taskTable string
}

var _ background.Store = (*BackgroundTaskStore)(nil)

// NewBackgroundTaskStore 为 cp 创建后台任务存储。使用前调用 [BackgroundTaskStore.Setup]。
func NewBackgroundTaskStore(cp *Checkpointer) *BackgroundTaskStore {
	if cp == nil {
		panic("sql: NewBackgroundTaskStore requires a checkpointer")
	}
	table := cp.table + "_background_task"
	if len(table)+len("_ready_idx") > 63 {
		base := cp.table
		if _, mysql := cp.dialect.(MySQL); !mysql {
			base = strings.ToLower(base)
		}
		hash := sha256.Sum256([]byte(base))
		table = fmt.Sprintf("crew_bg_%x", hash[:16])
	}
	return &BackgroundTaskStore{Checkpointer: cp, taskTable: table}
}

// Setup 创建存档、租约和任务表。可重复调用。
func (s *BackgroundTaskStore) Setup(ctx context.Context) error {
	if err := s.Checkpointer.Setup(ctx); err != nil {
		return err
	}
	keyType, suffix, inline := "TEXT", "", ""
	switch s.dialect.(type) {
	case Postgres:
		keyType = `TEXT COLLATE "C"`
	case MySQL:
		keyType, suffix = "VARBINARY(96)", " ENGINE=InnoDB"
		inline = ", INDEX ready_idx (ready, sid)"
	}
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		sid %s NOT NULL PRIMARY KEY, revision BIGINT NOT NULL, ready SMALLINT NOT NULL, payload %s NOT NULL%s
	)%s`, s.taskTable, keyType, s.dialect.BlobType(), inline, suffix)
	if _, err := s.db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("sql background task setup: %w", err)
	}
	if inline == "" {
		q = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_ready_idx ON %s (ready, sid)", s.taskTable, s.taskTable)
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("sql background task ready index: %w", err)
		}
	}
	return nil
}

// backgroundReadyState 是从任务状态投影的调度索引。终态与承诺唤醒父任务的标记一同保存。
func backgroundReadyState(task *background.Task) int {
	if task.Terminal() {
		if !task.Notified {
			return 2
		}
		return 0
	}
	if task.CancelRequested || task.Status == background.Queued || task.Status == background.Running {
		return 1
	}
	return 0
}

// CreateBackgroundTask 向同一个任务 ID 原子插入一次，重复派单读取原状态。
func (s *BackgroundTaskStore) CreateBackgroundTask(ctx context.Context, task *background.Task) (*background.Task, error) {
	if task == nil || task.ID == "" || task.Revision != 1 || task.Status != background.Queued {
		return nil, errors.New("sql background task: create requires a new queued task")
	}
	data, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("sql background task encode: %w", err)
	}
	ph := s.dialect.Placeholder
	conflict := "ON CONFLICT (sid) DO NOTHING"
	if _, ok := s.dialect.(MySQL); ok {
		conflict = "ON DUPLICATE KEY UPDATE sid = sid"
	}
	q := fmt.Sprintf("INSERT INTO %s (sid, revision, ready, payload) VALUES (%s, 1, 1, %s) %s",
		s.taskTable, ph(1), ph(2), conflict)
	if _, err := s.db.ExecContext(ctx, q, task.ID, data); err != nil {
		return nil, fmt.Errorf("sql background task create: %w", err)
	}
	return s.LoadBackgroundTask(ctx, task.ID)
}

// LoadBackgroundTask 读取任务快照。无记录时返回 (nil, nil)。
func (s *BackgroundTaskStore) LoadBackgroundTask(ctx context.Context, id string) (*background.Task, error) {
	q := fmt.Sprintf("SELECT revision, payload FROM %s WHERE sid = %s", s.taskTable, s.dialect.Placeholder(1))
	var rev int64
	var data []byte
	if err := s.db.QueryRowContext(ctx, q, id).Scan(&rev, &data); err != nil {
		if errors.Is(err, dbsql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("sql background task load: %w", err)
	}
	var task background.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("sql background task decode: %w", err)
	}
	if task.ID != id || task.Revision != rev {
		return nil, errors.New("sql background task: state disagrees with task row")
	}
	return &task, nil
}

// ListReadyBackgroundTasks 只读取待处理 ID。分页按 ID 字节序推进。
func (s *BackgroundTaskStore) ListReadyBackgroundTasks(ctx context.Context, after string, limit int, notify bool) ([]string, error) {
	if limit <= 0 {
		return nil, errors.New("sql background task: positive page size required")
	}
	ready := "ready = 1"
	if notify {
		ready = "ready IN (1, 2)"
	}
	ph := s.dialect.Placeholder
	q := fmt.Sprintf("SELECT sid FROM %s WHERE %s AND sid > %s ORDER BY sid LIMIT %s",
		s.taskTable, ready, ph(1), ph(2))
	rows, err := s.db.QueryContext(ctx, q, after, limit)
	if err != nil {
		return nil, fmt.Errorf("sql background task list: %w", err)
	}
	defer rows.Close() // rows.Err 报告查询迭代和隐式关闭错误。
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("sql background task scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql background task iterate: %w", err)
	}
	return ids, nil
}

// UpdateBackgroundTask 校验版本，替换任务记录。持有租约的执行者在同一事务内校验令牌。
func (s *BackgroundTaskStore) UpdateBackgroundTask(ctx context.Context, task *background.Task, clearCheckpoint bool) (bool, error) {
	if task == nil || task.ID == "" || task.Revision <= 0 || (clearCheckpoint && !task.Terminal()) {
		return false, errors.New("sql background task: invalid update")
	}
	token := compose_v2.CheckpointTaskLeaseToken(ctx, s.CheckpointTaskLeases(), task.ID)
	if clearCheckpoint && token == "" {
		return false, compose_v2.ErrTaskLeaseLost
	}
	next := *task
	next.Revision++
	data, err := json.Marshal(next)
	if err != nil {
		return false, fmt.Errorf("sql background task encode update: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("sql background task begin update: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交后回滚为空操作。
	if token != "" {
		if err := s.checkTaskLease(ctx, tx, task.ID, token); err != nil {
			return false, fmt.Errorf("sql background task lease: %w", err)
		}
	}
	ph := s.dialect.Placeholder
	q := fmt.Sprintf("UPDATE %s SET revision = revision + 1, ready = %s, payload = %s WHERE sid = %s AND revision = %s",
		s.taskTable, ph(1), ph(2), ph(3), ph(4))
	res, err := tx.ExecContext(ctx, q, backgroundReadyState(&next), data, task.ID, task.Revision)
	if err != nil {
		return false, fmt.Errorf("sql background task update: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return false, err
	}
	if clearCheckpoint {
		if _, err := s.deleteSubtree(ctx, tx, task.ID, ""); err != nil {
			return false, fmt.Errorf("sql background task checkpoint cleanup: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("sql background task commit: %w", err)
	}
	return true, nil
}
