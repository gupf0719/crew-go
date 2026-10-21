package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// retryAfterErr 是实现 compose_v2.RetryAfterProvider 的测试错误。
type retryAfterErr struct {
	d time.Duration
}

func (e *retryAfterErr) Error() string                  { return fmt.Sprintf("rate limited, retry after %v", e.d) }
func (e *retryAfterErr) RetryAfterDelay() time.Duration { return e.d }

// TestRetryAfterFromError_UnwrapsChain 验证沿 Unwrap 链查找——业务 wrap 后仍能识别。
func TestRetryAfterFromError_UnwrapsChain(t *testing.T) {
	base := &retryAfterErr{d: 3 * time.Second}
	wrapped := fmt.Errorf("model call: %w", fmt.Errorf("transport: %w", base))

	if got := compose_v2.RetryAfterFromError(wrapped); got != 3*time.Second {
		t.Fatalf("wrap 两层后应仍能取到 3s，实际 %v——Unwrap 链未被遍历", got)
	}
	if got := compose_v2.RetryAfterFromError(errors.New("plain")); got != 0 {
		t.Fatalf("不实现该接口的错误应返 0，实际 %v", got)
	}
	if got := compose_v2.RetryAfterFromError(nil); got != 0 {
		t.Fatalf("nil 应返 0，实际 %v", got)
	}
	if got := compose_v2.RetryAfterFromError(&retryAfterErr{d: 0}); got != 0 {
		t.Fatalf("provider 返 0 时应视作未提供，实际 %v", got)
	}
}

// TestRunWithRetry_HonorsRetryAfterOverBackoff 验证服务端 Retry-After 覆盖计算退避。
//
// **Bug 场景**：服务端在 429 里明说「等 N 秒」，我们却按指数退避猜——猜早了再吃一次
// 429（部分 provider 因此延长封禁），猜晚了白等。
//
// **测试设计**：配一个 InitialDelay 极大（5s）的退避，错误携带极小的 Retry-After
// （30ms）。fix 后按 30ms 走，整体远快于 5s。
//   - fix 后：总耗时 < 1s
//   - fix 前：第一次重试就要睡 5s（受 MaxDelay 限制也仍是秒级）
func TestRunWithRetry_HonorsRetryAfterOverBackoff(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries: 1,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: 5 * time.Second, // 故意远大于 Retry-After
			MaxDelay:     10 * time.Second,
			Multiplier:   2.0,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	start := time.Now()
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			if calls == 1 {
				return 0, &retryAfterErr{d: 30 * time.Millisecond}
			}
			return 42, nil
		})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("第二次调用应成功，实际 err=%v", err)
	}
	if calls != 2 {
		t.Fatalf("应重试一次共调 2 次，实际 %d 次", calls)
	}
	if elapsed > time.Second {
		t.Fatalf("应按服务端 Retry-After(30ms) 退避，实际等了 %v——说明仍在用计算退避"+
			"(InitialDelay=5s)，服务端明示值被忽略", elapsed)
	}
}

// TestRunWithRetry_FallsBackToBackoffWithoutRetryAfter 验证未提供 Retry-After 时
// 仍走原计算退避——保证上面那条 fix 没把常规路径改掉。
func TestRunWithRetry_FallsBackToBackoffWithoutRetryAfter(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries: 1,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: 40 * time.Millisecond,
			MaxDelay:     time.Second,
			Multiplier:   2.0,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	start := time.Now()
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			if calls == 1 {
				return 0, errors.New("transient")
			}
			return 7, nil
		})
	elapsed := time.Since(start)

	if err != nil || calls != 2 {
		t.Fatalf("应重试一次并成功，实际 calls=%d err=%v", calls, err)
	}
	// 带 ±25% 抖动，下界取 40ms*0.75=30ms
	if elapsed < 30*time.Millisecond {
		t.Fatalf("无 Retry-After 时应按 InitialDelay(40ms±25%%) 退避，实际仅 %v——"+
			"常规退避路径被破坏", elapsed)
	}
}

// bizRetryAfterWrap 模拟业务按 RetryAfterProvider 的邀请包的一层自己的错误类型：
// 它实现了接口，但在「我没意见」时返 0。
type bizRetryAfterWrap struct {
	d     time.Duration
	inner error
}

func (b bizRetryAfterWrap) Error() string                  { return "biz: " + b.inner.Error() }
func (b bizRetryAfterWrap) Unwrap() error                  { return b.inner }
func (b bizRetryAfterWrap) RetryAfterDelay() time.Duration { return b.d }

// raErr 是最小的 RetryAfterProvider 实现（不依赖 model 包——compose_v2 不 import 它）。
type raErr struct{ d time.Duration }

func (e raErr) Error() string                  { return "ra" }
func (e raErr) RetryAfterDelay() time.Duration { return e.d }

// TestRetryAfterFromError_SkipsZeroImplementers 钉住「返 0 的实现者不算数，继续往下找」。
//
// **这条抓的是一个真实退化**：函数原本写成一次 `errors.As` 加一个后置的 `d > 0` 判断，
// 而 `errors.As` 命中第一个可赋值的错误就停了——后置过滤不会让它继续搜索。于是：
//
//   - 业务包一层自己的错误类型、在「我没意见」时返 0 ⟹ 里层服务端明说的 30s 再也取不到
//   - `errors.Join` 上取值**随成员顺序翻**（本仓确实在产多错误树）
//
// 两种都 `err == nil`，唯一症状是「等错了时长」——正是这个特性本来要消灭的那个。
func TestRetryAfterFromError_SkipsZeroImplementers(t *testing.T) {
	const want = 30 * time.Second
	inner := raErr{d: want}
	zero := raErr{d: 0}

	cases := []struct {
		name string
		err  error
		want time.Duration
	}{
		{"业务壳返 0，里层有值", bizRetryAfterWrap{d: 0, inner: inner}, want},
		{"业务壳自己有值时以壳为准", bizRetryAfterWrap{d: 5 * time.Second, inner: inner}, 5 * time.Second},
		{"多错误树：零值在前", errors.Join(zero, inner), want},
		{"多错误树：零值在后", errors.Join(inner, zero), want},
		{"多错误树：一个都没有", errors.Join(errors.New("a"), errors.New("b")), 0},
		{"套两层壳", bizRetryAfterWrap{d: 0, inner: bizRetryAfterWrap{d: 0, inner: inner}}, want},
		{"没有任何实现者", errors.New("plain"), 0},
		{"nil", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := compose_v2.RetryAfterFromError(c.err); got != c.want {
				t.Errorf("RetryAfterFromError = %v, want %v——"+
					"服务端明示的等待时长被中途某个返 0 的实现者吞了，退避会退回瞎猜", got, c.want)
			}
		})
	}
}
