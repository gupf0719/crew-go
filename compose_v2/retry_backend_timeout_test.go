package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// 默认谓词按「这个错重跑有没有救」判，不按「错误链上有没有 ctx 错」判：后端自己的超时（pgx、`http.Client{Timeout}`
// 会把 context.DeadlineExceeded 原样包进错误）在调用方 ctx 还活着时照常重试；调用方 ctx 真的结束了，RunWithRetry
// 在谓词之前就停下。
//
// 原来默认谓词先调 IsControlFlowSignal，错误链上有 ctx 错就判不可重试：后端内部超时一律只调 1 次（实测）。

func countCalls(t *testing.T, ctx context.Context, cfg compose_v2.RetryConfig, err error) int {
	t.Helper()
	n := 0
	_, _ = compose_v2.RunWithRetry(ctx, cfg, func(context.Context) (struct{}, error) {
		n++
		return struct{}{}, err
	})
	return n
}

func TestIsRetryable_BackendTimeoutRetriedWhileCallerAlive(t *testing.T) {
	backendTimeout := fmt.Errorf("pg query: %w", context.DeadlineExceeded)
	for name, cfg := range map[string]compose_v2.RetryConfig{
		"simple":      compose_v2.SimpleRetry(3),
		"exponential": compose_v2.ExponentialBackoff(3, time.Millisecond, time.Millisecond),
	} {
		t.Run(name, func(t *testing.T) {
			if n := countCalls(t, context.Background(), cfg, backendTimeout); n != 4 {
				t.Errorf("调用方 ctx 活着、后端自己超时：调了 %d 次，want 4（1 + 3 次重试）", n)
			}
		})
	}
	if !compose_v2.IsRetryable(backendTimeout) {
		t.Error("IsRetryable(后端超时) = false，want true")
	}
}

func TestIsRetryable_CallerCanceledStillStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := countCalls(t, ctx, compose_v2.SimpleRetry(3), fmt.Errorf("op: %w", ctx.Err())); n != 1 {
		t.Errorf("调用方 ctx 已取消：调了 %d 次，want 1——取消由 RunWithRetry 的守卫按 ctx 拦下", n)
	}
}

func TestIsRetryable_ErrorOnlySignalsStillNotRetried(t *testing.T) {
	for name, err := range map[string]error{
		"interrupt":   &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "x", Question: "q"}},
		"stale":       fmt.Errorf("save: %w", compose_v2.ErrCheckpointStale),
		"soft_cancel": fmt.Errorf("run: %w", compose_v2.ErrSoftCanceled),
		"aborted":     fmt.Errorf("x: %w", compose_v2.ErrRunAborted),
	} {
		if compose_v2.IsRetryable(err) {
			t.Errorf("%s: IsRetryable = true，want false", name)
		}
	}
	if compose_v2.IsRetryable(errors.New("")) == false {
		t.Error("普通业务错应当可重试")
	}
}
