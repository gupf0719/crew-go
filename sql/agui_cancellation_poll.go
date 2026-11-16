package sql

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"crew-go/agui"
)

var _ agui.CancellationPollStore = (*AGUISessionStore)(nil)

func (s *AGUISessionStore) cancellationPollTable() string {
	table := s.table + "_agui_cancel_poll"
	if len(table) <= 63 {
		return table
	}
	sum := sha256.Sum256([]byte(strings.ToLower(s.table)))
	return fmt.Sprintf("crew_agui_cancel_poll_%x", sum[:16])
}

func (s *AGUISessionStore) setupCancellationPoll(ctx context.Context, tokenType, suffix string) error {
	table := s.cancellationPollTable()
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SMALLINT NOT NULL PRIMARY KEY, owner_token %s NOT NULL, expires_at BIGINT NOT NULL)%s",
		table, tokenType, suffix)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("sql agui cancellation poll setup: %w", err)
	}
	query = fmt.Sprintf("INSERT INTO %s (id, owner_token, expires_at) VALUES (1, '', 0) %s",
		table, s.dialect.UpsertSuffix([]string{"id"}, []string{"id"}))
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("sql agui cancellation poll initialize: %w", err)
	}
	return nil
}

// TryAcquireCancellationPoll 为本存储域申请固定期限的扫描租约。
func (s *AGUISessionStore) TryAcquireCancellationPoll(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	if token == "" || ttl.Milliseconds() <= 0 {
		return false, errors.New("sql agui: invalid cancellation poll lease")
	}
	ph := s.dialect.Placeholder
	now := s.taskLeaseMillisSQL()
	query := fmt.Sprintf("UPDATE %s SET owner_token = %s, expires_at = %s + %s WHERE id = 1 AND expires_at <= %s",
		s.cancellationPollTable(), ph(1), now, ph(2), now)
	result, err := s.db.ExecContext(ctx, query, token, ttl.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("sql agui acquire cancellation poll lease: %w", err)
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

// ReleaseCancellationPoll 仅释放 token 仍匹配的扫描租约。
func (s *AGUISessionStore) ReleaseCancellationPoll(ctx context.Context, token string) error {
	query := fmt.Sprintf("UPDATE %s SET owner_token = '', expires_at = 0 WHERE id = 1 AND owner_token = %s",
		s.cancellationPollTable(), s.dialect.Placeholder(1))
	if _, err := s.db.ExecContext(ctx, query, token); err != nil {
		return fmt.Errorf("sql agui release cancellation poll lease: %w", err)
	}
	return nil
}
