package sql

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"crew-go/agui"
)

// AGUIRunStore 将一次 HTTP Run 的回执和事件保存在 SQL 中。
// 从 [NewAGUIRunStore] 构造并调用 [AGUIRunStore.Setup] 后可传给 [agui.WithRunStore]。
type AGUIRunStore struct {
	db      *dbsql.DB
	dialect Dialect
	runs    string
	events  string
}

var _ agui.RunStore = (*AGUIRunStore)(nil)

// NewAGUIRunStore 复用 cp 的连接、方言和表名命名空间。日志有独立的生命周期。
func NewAGUIRunStore(cp *Checkpointer) *AGUIRunStore {
	if cp == nil {
		panic("sql: NewAGUIRunStore requires a checkpointer")
	}
	prefix := cp.table
	if len(prefix)+len("_agui_events") > 63 {
		sum := sha256.Sum256([]byte(strings.ToLower(prefix)))
		prefix = fmt.Sprintf("crew_%x", sum[:16])
	}
	return &AGUIRunStore{db: cp.db, dialect: cp.dialect, runs: prefix + "_agui_runs", events: prefix + "_agui_events"}
}

// Setup 创建回执和事件表，可重复调用。
func (s *AGUIRunStore) Setup(ctx context.Context) error {
	keyType, suffix := "TEXT", ""
	switch s.dialect.(type) {
	case Postgres:
		keyType = `TEXT COLLATE "C"`
	case MySQL:
		keyType, suffix = "VARBINARY(64)", " ENGINE=InnoDB"
	}
	blob := s.dialect.BlobType()
	queries := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			run_key %s NOT NULL PRIMARY KEY, token %s NOT NULL UNIQUE, digest %s NOT NULL,
			deadline_ms BIGINT NOT NULL, status VARCHAR(16) NOT NULL, last_event_id BIGINT NOT NULL,
			http_status INTEGER NOT NULL, finished_ms BIGINT NOT NULL, terminal %s)%s`,
			s.runs, keyType, keyType, keyType, blob, suffix),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			token %s NOT NULL, event_id BIGINT NOT NULL, data %s NOT NULL,
			PRIMARY KEY (token, event_id))%s`, s.events, keyType, blob, suffix),
	}
	for _, query := range queries {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("sql agui run setup: %w", err)
		}
	}
	return nil
}

// ClaimRun 按 key 原子认领，重复请求读到已存在的回执。
func (s *AGUIRunStore) ClaimRun(ctx context.Context, key, digest string, deadline time.Time) (*agui.RunRecord, bool, error) {
	token := rand.Text()
	ph := s.dialect.Placeholder
	conflict := "ON CONFLICT (run_key) DO NOTHING"
	if _, ok := s.dialect.(MySQL); ok {
		conflict = "ON DUPLICATE KEY UPDATE run_key = run_key"
	}
	query := fmt.Sprintf(`INSERT INTO %s (run_key, token, digest, deadline_ms, status, last_event_id, http_status, finished_ms)
		VALUES (%s, %s, %s, %s, %s, 0, 0, 0) %s`, s.runs, ph(1), ph(2), ph(3), ph(4), ph(5), conflict)
	tx, err := s.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return nil, false, fmt.Errorf("sql agui run begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交后回滚为空操作。
	if _, err := tx.ExecContext(ctx, query, key, token, digest, deadline.UnixMilli(), agui.RunRunning); err != nil {
		return nil, false, fmt.Errorf("sql agui run claim: %w", err)
	}
	record, err := s.loadRun(ctx, tx, key)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		// 提交的响应可能丢失。读到本次令牌才能确认自己仍是执行者。
		committed, readErr := s.LoadRun(ctx, key)
		if readErr == nil && committed != nil {
			return committed, committed.Token == token, nil
		}
		return nil, false, fmt.Errorf("sql agui run commit claim: %w", err)
	}
	return record, record != nil && record.Token == token, nil
}

// LoadRun 返回当前回执。找不到时返回 (nil, nil)。
func (s *AGUIRunStore) LoadRun(ctx context.Context, key string) (*agui.RunRecord, error) {
	return s.loadRun(ctx, s.db, key)
}

func (s *AGUIRunStore) loadRun(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *dbsql.Row
}, key string) (*agui.RunRecord, error) {
	query := fmt.Sprintf(`SELECT token, digest, deadline_ms, status, last_event_id, http_status, terminal
		FROM %s WHERE run_key = %s`, s.runs, s.dialect.Placeholder(1))
	var record agui.RunRecord
	var deadline int64
	var terminal []byte
	err := db.QueryRowContext(ctx, query, key).Scan(&record.Token, &record.Digest, &deadline,
		&record.Status, &record.LastEventID, &record.HTTPStatus, &terminal)
	if errors.Is(err, dbsql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sql agui run load: %w", err)
	}
	record.Deadline = time.UnixMilli(deadline)
	record.Terminal = terminal
	return &record, nil
}

// AppendRunEvent 在同一事务内分配序号、追加帧并提交终态。令牌不匹配或已收尾时返回 [agui.ErrRunClosed]。
func (s *AGUIRunStore) AppendRunEvent(ctx context.Context, key, token string, data json.RawMessage, status agui.RunStatus, httpStatus int) error {
	if !json.Valid(data) || bytes.ContainsAny(data, "\r\n") {
		return errors.New("sql agui run event must be single-line JSON")
	}
	switch status {
	case agui.RunRunning, agui.RunSucceeded, agui.RunInterrupted, agui.RunCancelled, agui.RunFailed:
		if httpStatus != 0 {
			return errors.New("sql agui run http status requires a rejection")
		}
	case agui.RunRejected:
		if httpStatus < 400 || httpStatus > 599 {
			return errors.New("sql agui run rejection requires a valid http status")
		}
	default:
		return fmt.Errorf("sql agui run invalid status %q", status)
	}

	ph := s.dialect.Placeholder
	bind := &paramBinder{dialect: s.dialect, offset: 1}
	set := "last_event_id = last_event_id + 1"
	if status != agui.RunRunning {
		set += fmt.Sprintf(", status = %s, http_status = %s, terminal = %s, finished_ms = %s",
			bind.Bind(status), bind.Bind(httpStatus), bind.Bind([]byte(data)), bind.Bind(time.Now().UnixMilli()))
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE run_key = %s AND token = %s AND status = %s",
		s.runs, set, bind.Bind(key), bind.Bind(token), bind.Bind(agui.RunRunning))
	tx, err := s.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("sql agui run begin append: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交后回滚为空操作，失败则释放事务。
	result, err := tx.ExecContext(ctx, query, bind.args...)
	if err != nil {
		return fmt.Errorf("sql agui run advance: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sql agui run affected rows: %w", err)
	}
	if affected == 0 {
		return agui.ErrRunClosed
	}
	var sequence int64
	query = fmt.Sprintf("SELECT last_event_id FROM %s WHERE run_key = %s", s.runs, ph(1))
	if err := tx.QueryRowContext(ctx, query, key).Scan(&sequence); err != nil {
		return fmt.Errorf("sql agui run read event id: %w", err)
	}
	query = fmt.Sprintf("INSERT INTO %s (token, event_id, data) VALUES (%s, %s, %s)", s.events, ph(1), ph(2), ph(3))
	if _, err := tx.ExecContext(ctx, query, token, sequence, []byte(data)); err != nil {
		return fmt.Errorf("sql agui run insert event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sql agui run commit event: %w", err)
	}
	return nil
}

// ReadRunEvents 返回与同一回执令牌对应的有序事件页。
func (s *AGUIRunStore) ReadRunEvents(ctx context.Context, key, token string, after, through int64, limit int) ([]agui.RunFrame, error) {
	if limit <= 0 {
		return nil, errors.New("sql agui run event limit must be positive")
	}
	ph := s.dialect.Placeholder
	query := fmt.Sprintf(`SELECT event_id, data FROM %s
		WHERE token = (SELECT token FROM %s WHERE run_key = %s AND token = %s)
		  AND event_id > %s AND event_id <= %s ORDER BY event_id LIMIT %s`,
		s.events, s.runs, ph(1), ph(2), ph(3), ph(4), ph(5))
	rows, err := s.db.QueryContext(ctx, query, key, token, after, through, limit)
	if err != nil {
		return nil, fmt.Errorf("sql agui run query events: %w", err)
	}
	defer rows.Close() // rows.Err 报告迭代和隐式关闭错误。
	var frames []agui.RunFrame
	for rows.Next() {
		var id int64
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, fmt.Errorf("sql agui run scan event: %w", err)
		}
		frames = append(frames, agui.RunFrame{ID: id, Data: data})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql agui run iterate events: %w", err)
	}
	return frames, nil
}

// PruneRuns 删除 before 之前已收尾的回执和全部事件。调用方按业务所需的重试窗口定期调用。
// 已回收的 runId 应退役，重复 POST 会被当成新请求。
func (s *AGUIRunStore) PruneRuns(ctx context.Context, before time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("sql agui run begin prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交后回滚为空操作。
	ph := s.dialect.Placeholder(1)
	query := fmt.Sprintf("DELETE FROM %s WHERE token IN (SELECT token FROM %s WHERE finished_ms > 0 AND finished_ms < %s)",
		s.events, s.runs, ph)
	if _, err := tx.ExecContext(ctx, query, before.UnixMilli()); err != nil {
		return 0, fmt.Errorf("sql agui run prune events: %w", err)
	}
	// 若两个语句之间有新的终态提交，该记录仍有终态帧，留待下一次回收。
	query = fmt.Sprintf(`DELETE FROM %s WHERE finished_ms > 0 AND finished_ms < %s
		AND NOT EXISTS (SELECT 1 FROM %s WHERE %s.token = %s.token)`, s.runs, ph, s.events, s.events, s.runs)
	result, err := tx.ExecContext(ctx, query, before.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("sql agui run prune receipts: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sql agui run pruned rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sql agui run commit prune: %w", err)
	}
	return deleted, nil
}
