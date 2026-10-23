package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crew-go/compose_v2"
)

func TestRetry_NoRetriesOnSuccess(t *testing.T) {
	calls := 0
	out, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
		func(_ context.Context) (string, error) {
			calls++
			return "ok", nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "ok" {
		t.Errorf("out=%q", out)
	}
	if calls != 1 {
		t.Errorf("calls=%d, want 1", calls)
	}
}

func TestRetry_RetryUntilSuccess(t *testing.T) {
	calls := 0
	out, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
		func(_ context.Context) (string, error) {
			calls++
			if calls < 3 {
				return "", errors.New("transient")
			}
			return "ok", nil
		})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "ok" || calls != 3 {
		t.Errorf("out=%q calls=%d, want ok/3", out, calls)
	}
}

func TestRetry_MaxRetriesExhausted(t *testing.T) {
	calls := 0
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(2),
		func(_ context.Context) (string, error) {
			calls++
			return "", errors.New("always fail")
		})
	if err == nil || err.Error() != "always fail" {
		t.Errorf("err=%v", err)
	}
	if calls != 3 {
		t.Errorf("calls=%d, want 3 (1 initial + 2 retries)", calls)
	}
}

func TestRetry_InterruptErrorNeverRetried(t *testing.T) {
	calls := 0
	intErr := &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "q1"}}
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(5),
		func(_ context.Context) (string, error) {
			calls++
			return "", intErr
		})
	if !errors.Is(err, intErr) {
		t.Errorf("err=%v, want intErr", err)
	}
	if calls != 1 {
		t.Errorf("calls=%d, want 1 (no retry on InterruptError)", calls)
	}
}

func TestRetry_CtxCancelStopsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	// **必须给谓词**——否则第 1 次失败就被闸门返回，下面 `calls == 2` 的 cancel()
	// 一次都执行不到，两条断言在「什么都没发生」上恒真（本用例被这么废过一次）。
	cfg := compose_v2.SimpleRetry(5)
	cfg.InitialDelay = 10 * time.Millisecond
	_, err := compose_v2.RunWithRetry(ctx, cfg,
		func(_ context.Context) (string, error) {
			calls++
			if calls == 2 {
				cancel()
			}
			return "", errors.New("transient")
		})
	if err == nil {
		t.Fatal("expected ctx err")
	}
	// 正向前提：重试必须真的发生过，cancel() 才有机会执行。
	if calls < 2 {
		t.Fatalf("fixture 没搭对：只跑了 %d 次，cancel() 没被执行到——"+
			"下面「取消后很快停」这句在这种状态下恒真", calls)
	}
	if calls > 3 {
		t.Errorf("calls=%d, expected to stop quickly after cancel", calls)
	}
}

// TestIsControlFlowSignal retry 层判据的直接覆盖（nil / interrupt / wrapped-ctx /
// 两种 ctx 错 / 普通错误）。⚠️ **它不覆盖 fallback**——那一层消费的是 `IsRetryFutile`，
// 另有自己的哨兵清单，两边加哨兵要各加各的。
func TestIsControlFlowSignal(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"interrupt", &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "q1"}}, true},
		{"interrupt_wrapped", fmt.Errorf("outer: %w", &compose_v2.InterruptError{Reason: "hitl"}), true},
		{"ctx_canceled", context.Canceled, true},
		{"ctx_canceled_wrapped", fmt.Errorf("call: %w", context.Canceled), true},
		{"ctx_deadline", context.DeadlineExceeded, true},
		{"plain_error", errors.New("transient 503"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := compose_v2.IsControlFlowSignal(c.err); got != c.want {
				t.Errorf("IsControlFlowSignal(%v)=%v, want %v", c.err, got, c.want)
			}
			// 中断与普通错上 IsRetryable 恰是它的否定。⚠️ **一般情况下不是否定关系**：IsRetryable 另排
			// NonRetryable / ErrRunAborted / ErrCheckpointNotResumable 三类；ctx 错是控制流的**标签**，
			// 但谓词不按它决定要不要再做一次（调用方的 ctx 由 RunWithRetry 的守卫判），所以 ctx 错上两者都为真。
			if c.err != nil {
				wantRetry := !c.want
				if errors.Is(c.err, context.Canceled) || errors.Is(c.err, context.DeadlineExceeded) {
					wantRetry = true
				}
				if compose_v2.IsRetryable(c.err) != wantRetry {
					t.Errorf("IsRetryable(%v)=%v, want %v", c.err, compose_v2.IsRetryable(c.err), wantRetry)
				}
			}
		})
	}
}

func TestRetry_BackoffDelayBasics(t *testing.T) {
	cfg := compose_v2.ExponentialBackoff(3, 100*time.Millisecond, 1*time.Second)
	d1 := compose_v2.BackoffDelay(cfg.BackoffPolicy, 1)
	d2 := compose_v2.BackoffDelay(cfg.BackoffPolicy, 2)
	// d1 ~ 100ms ±25%; d2 ~ 200ms ±25% — 取消 jitter 验范围
	cfg.Jitter = false
	if compose_v2.BackoffDelay(cfg.BackoffPolicy, 1) != 100*time.Millisecond {
		t.Errorf("attempt 1 base != 100ms")
	}
	if compose_v2.BackoffDelay(cfg.BackoffPolicy, 2) != 200*time.Millisecond {
		t.Errorf("attempt 2 base != 200ms")
	}
	if compose_v2.BackoffDelay(cfg.BackoffPolicy, 100) != 1*time.Second {
		t.Errorf("attempt 100 capped != 1s")
	}
	// jitter 版本只验落在 0~2x 范围
	if d1 < 0 || d1 > 200*time.Millisecond {
		t.Errorf("d1=%v out of jitter range", d1)
	}
	if d2 < 0 || d2 > 400*time.Millisecond {
		t.Errorf("d2=%v out of jitter range", d2)
	}
}
