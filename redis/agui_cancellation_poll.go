package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"crew-go/agui"
)

var _ agui.CancellationPollStore = (*AGUISessionStore)(nil)

func (s *AGUISessionStore) cancellationPollKey() string { return s.cancellationIndex() + ":poll" }

// TryAcquireCancellationPoll 为本存储前缀申请固定期限的扫描租约。
func (s *AGUISessionStore) TryAcquireCancellationPoll(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	if token == "" || ttl.Milliseconds() <= 0 {
		return false, errors.New("redis agui: invalid cancellation poll lease")
	}
	acquired, err := s.rdb.SetNX(ctx, s.cancellationPollKey(), token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis agui acquire cancellation poll lease: %w", err)
	}
	return acquired, nil
}

// ReleaseCancellationPoll 仅释放 token 仍匹配的扫描租约。
func (s *AGUISessionStore) ReleaseCancellationPoll(ctx context.Context, token string) error {
	if err := releaseTaskLeaseScript.Run(ctx, s.rdb, []string{s.cancellationPollKey()}, token).Err(); err != nil {
		return fmt.Errorf("redis agui release cancellation poll lease: %w", err)
	}
	return nil
}
