package sql

import (
	"context"
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"crew-go/agui"
	"crew-go/compose_v2"
)

// AGUISessionStore 在 Checkpointer 所用的数据库中另存 AG-UI 会话记录。
type AGUISessionStore struct {
	*Checkpointer
	sessionTable string
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
	return &AGUISessionStore{Checkpointer: cp, sessionTable: table}
}

// Setup 创建存档、租约和 AG-UI 会话表，可重复调用。
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
	return nil
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
	var record agui.SessionRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("sql agui session decode: %w", err)
	}
	return &record, nil
}

// SaveSession 持有 key 的有效租约时整条替换，record 为 nil 时删除。
func (s *AGUISessionStore) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	token := compose_v2.CheckpointTaskLeaseToken(ctx, s.CheckpointTaskLeases(), key)
	if token == "" {
		return 0, compose_v2.ErrTaskLeaseLost
	}
	ph := s.dialect.Placeholder
	query := fmt.Sprintf("DELETE FROM %s WHERE sid = %s", s.sessionTable, ph(1))
	args := []any{key}
	if record != nil {
		data, err := json.Marshal(record)
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
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sql agui session commit: %w", err)
	}
	return deleted, nil
}
