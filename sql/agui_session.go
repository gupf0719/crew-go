package sql

import (
	"context"
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"crew-go/agui"
	"crew-go/compose_v2"
)

// AGUISessionStore 在 Checkpointer 所用的数据库中另存 AG-UI 会话记录。
type AGUISessionStore struct {
	*Checkpointer
	sessionTable string
	cancelTable  string
}

var _ agui.SessionStore = (*AGUISessionStore)(nil)

// NewAGUISessionStore 为 cp 建立共享会话存储。使用前调 [AGUISessionStore.Setup]。
func NewAGUISessionStore(cp *Checkpointer) *AGUISessionStore {
	if cp == nil || cp.CheckpointTaskLeases() == nil {
		panic("sql: NewAGUISessionStore requires a checkpointer with task leases")
	}
	table := cp.table + "_agui_session"
	if len(table) > 63 {
		sum := sha256.Sum256([]byte(strings.ToLower(cp.table)))
		table = fmt.Sprintf("crew_agui_session_%x", sum[:16])
	}
	index := cp.table + "_agui_cancel"
	if len(index) > 63 {
		sum := sha256.Sum256([]byte(strings.ToLower(cp.table)))
		index = fmt.Sprintf("crew_agui_cancel_%x", sum[:16])
	}
	return &AGUISessionStore{Checkpointer: cp, sessionTable: table, cancelTable: index}
}

// Setup 创建存档、租约、会话和待清理索引，首次执行时导入旧取消记录。可重复调用。
func (s *AGUISessionStore) Setup(ctx context.Context) error {
	if err := s.Checkpointer.Setup(ctx); err != nil {
		return err
	}
	var keyType, payloadType, suffix string
	switch s.dialect.(type) {
	case Postgres:
		keyType, payloadType = `TEXT COLLATE "C"`, "BYTEA"
	case MySQL:
		keyType, payloadType, suffix = "VARBINARY(128)", "LONGBLOB", " ENGINE=InnoDB"
	case SQLite:
		keyType, payloadType = "TEXT", "BLOB"
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (sid %s NOT NULL PRIMARY KEY, payload %s NOT NULL)%s",
		s.sessionTable, keyType, payloadType, suffix)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("sql agui session setup: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (sid %s NOT NULL PRIMARY KEY, ready SMALLINT NOT NULL)%s", s.cancelTable, keyType, suffix)); err != nil {
		return fmt.Errorf("sql agui cancellation index setup: %w", err)
	}
	return s.backfillCancellations(ctx)
}

func (s *AGUISessionStore) backfillCancellations(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // 提交成功后回滚为空操作。
	// 保留空键作为初始化标记，同一存储域的 Setup 在此串行。
	marker := fmt.Sprintf("INSERT INTO %s (sid, ready) VALUES ('', 0) %s", s.cancelTable, s.dialect.UpsertSuffix([]string{"sid"}, []string{"sid"}))
	if _, err := tx.ExecContext(ctx, marker); err != nil {
		return fmt.Errorf("sql agui cancellation migration lock: %w", err)
	}
	var ready int
	if err := tx.QueryRowContext(ctx, "SELECT ready FROM "+s.cancelTable+" WHERE sid = ''").Scan(&ready); err != nil {
		return err
	}
	if ready == 0 {
		for cursor := ""; ; {
			keys, next, err := s.scanLegacyCancellations(ctx, tx, cursor, 128)
			if err != nil {
				return err
			}
			for _, key := range keys {
				if err := s.backfillCancellation(ctx, tx, key); err != nil {
					return err
				}
			}
			if next == "" {
				break
			}
			cursor = next
		}
		if _, err := tx.ExecContext(ctx, "UPDATE "+s.cancelTable+" SET ready = 1 WHERE sid = ''"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// 先锁会话行再更新索引，与 SaveSession 的写入次序相同。
func (s *AGUISessionStore) backfillCancellation(ctx context.Context, tx *dbsql.Tx, key string) error {
	q := fmt.Sprintf("SELECT payload FROM %s WHERE sid = %s", s.sessionTable, s.dialect.Placeholder(1))
	switch s.dialect.(type) {
	case Postgres, MySQL:
		q += " FOR UPDATE"
	}
	var payload []byte
	err := tx.QueryRowContext(ctx, q, key).Scan(&payload)
	if errors.Is(err, dbsql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if hasCancellation(ctx, key, payload) {
		return s.indexCancellation(ctx, tx, key, true)
	}
	return nil
}

func hasCancellation(ctx context.Context, key string, payload []byte) bool {
	var header struct {
		Cancellation *json.RawMessage `json:"cancellation"`
	}
	if err := json.Unmarshal(payload, &header); err != nil {
		slog.WarnContext(ctx, "sql: 无法识别待补偿会话的记录", "session", key, "err", err)
		return false
	}
	return header.Cancellation != nil
}

func (s *AGUISessionStore) indexCancellation(ctx context.Context, tx *dbsql.Tx, key string, pending bool) error {
	ph := s.dialect.Placeholder(1)
	query := fmt.Sprintf("DELETE FROM %s WHERE sid = %s", s.cancelTable, ph)
	if pending {
		query = fmt.Sprintf("INSERT INTO %s (sid, ready) VALUES (%s, 0) %s", s.cancelTable, ph, s.dialect.UpsertSuffix([]string{"sid"}, []string{"sid"}))
	}
	if _, err := tx.ExecContext(ctx, query, key); err != nil {
		return fmt.Errorf("sql agui update cancellation index: %w", err)
	}
	return nil
}

// ListCancellations 只读取待清理索引。游标为上一页最后一个键。
func (s *AGUISessionStore) ListCancellations(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("sql agui cancellation page size must be positive")
	}
	ph := s.dialect.Placeholder
	q := fmt.Sprintf("SELECT sid FROM %s WHERE sid > %s ORDER BY sid LIMIT %s", s.cancelTable, ph(1), ph(2))
	rows, err := s.db.QueryContext(ctx, q, cursor, limit)
	if err != nil {
		return nil, "", fmt.Errorf("sql agui list cancellations: %w", err)
	}
	defer rows.Close() // rows.Err 报告迭代及自动关闭时的错误。
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, "", err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(keys) == limit {
		return keys, keys[len(keys)-1], nil
	}
	return keys, "", nil
}

// 带版本的任务使用单独的持久字段。旧二进制看到空 task_id 会在加载会话时拒绝，
// 因而无法拿旧工厂执行新版本任务，也无法覆盖版本字段。
type storedAGUISession struct {
	agui.SessionRecord
	VersionedTaskID string `json:"versioned_task_id,omitempty"`
}

// LoadSession 读会话记录，找不到时返回 (nil, nil)。
func (s *AGUISessionStore) LoadSession(ctx context.Context, key string) (*agui.SessionRecord, error) {
	query := fmt.Sprintf("SELECT payload FROM %s WHERE sid = %s", s.sessionTable, s.dialect.Placeholder(1))
	var data []byte
	err := s.db.QueryRowContext(ctx, query, key).Scan(&data)
	if errors.Is(err, dbsql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sql agui session load: %w", err)
	}
	var stored storedAGUISession
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("sql agui session decode: %w", err)
	}
	if stored.VersionedTaskID != "" {
		if stored.WorkflowVersion == "" || stored.TaskID != "" {
			return nil, errors.New("sql agui session has inconsistent workflow version fields")
		}
		stored.TaskID = stored.VersionedTaskID
	}
	return &stored.SessionRecord, nil
}

func (s *AGUISessionStore) scanLegacyCancellations(ctx context.Context, tx *dbsql.Tx, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("sql agui cancellation page size must be positive")
	}
	ph := s.dialect.Placeholder
	query := fmt.Sprintf("SELECT sid, payload FROM %s WHERE sid > %s ORDER BY sid LIMIT %s",
		s.sessionTable, ph(1), ph(2))
	rows, err := tx.QueryContext(ctx, query, cursor, limit)
	if err != nil {
		return nil, "", fmt.Errorf("sql agui list cancellations: %w", err)
	}
	defer rows.Close() // rows.Err 报告迭代及自动关闭时的错误。
	var keys []string
	var last string
	count := 0
	for rows.Next() {
		var key string
		var payload dbsql.RawBytes
		if err := rows.Scan(&key, &payload); err != nil {
			return nil, "", fmt.Errorf("sql agui read cancellation key: %w", err)
		}
		last = key
		count++
		if hasCancellation(ctx, key, payload) {
			keys = append(keys, key)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("sql agui list cancellations: %w", err)
	}
	if count == limit {
		return keys, last, nil
	}
	return keys, "", nil
}

// SaveSession 持有 key 的有效租约时整条替换，record 为 nil 时删除。
func (s *AGUISessionStore) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	token := compose_v2.CheckpointTaskLeaseToken(ctx, s.CheckpointTaskLeases(), key)
	if key == "" || token == "" {
		return 0, compose_v2.ErrTaskLeaseLost
	}
	ph := s.dialect.Placeholder
	query := fmt.Sprintf("DELETE FROM %s WHERE sid = %s", s.sessionTable, ph(1))
	args := []any{key}
	if record != nil {
		stored := storedAGUISession{SessionRecord: *record}
		if stored.WorkflowVersion != "" {
			stored.VersionedTaskID, stored.TaskID = stored.TaskID, ""
		}
		data, err := json.Marshal(stored)
		if err != nil {
			return 0, fmt.Errorf("sql agui session encode: %w", err)
		}
		query = fmt.Sprintf("INSERT INTO %s (sid, payload) VALUES (%s, %s) %s",
			s.sessionTable, ph(1), ph(2), s.dialect.UpsertSuffix([]string{"sid"}, []string{"payload"}))
		args = append(args, data)
	}
	tx, err := s.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("sql agui session begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交成功后回滚为空操作，失败则释放事务。
	if err := s.checkTaskLease(ctx, tx, key, token); err != nil {
		return 0, fmt.Errorf("sql agui session lease: %w", err)
	}
	var recordTask string
	if record != nil {
		recordTask = record.TaskID
		if recordTask == "" {
			return 0, errors.New("sql agui session requires a task id")
		}
	}
	for _, sid := range []string{recordTask, dropTask} {
		if sid == "" {
			continue
		}
		taskToken := compose_v2.CheckpointTaskLeaseToken(ctx, s.CheckpointTaskLeases(), sid)
		if taskToken == "" {
			return 0, compose_v2.ErrTaskLeaseLost
		}
		if err := s.checkTaskLease(ctx, tx, sid, taskToken); err != nil {
			return 0, fmt.Errorf("sql agui session task lease: %w", err)
		}
	}
	var deleted int32
	if dropTask != "" {
		deleted, err = s.deleteSubtree(ctx, tx, dropTask, "")
		if err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return 0, fmt.Errorf("sql agui session save: %w", err)
	}
	// 取消标记与其索引在同一事务提交。普通更新从不产生索引条目。
	if err := s.indexCancellation(ctx, tx, key, record != nil && record.Cancellation != nil); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sql agui session commit: %w", err)
	}
	return deleted, nil
}
