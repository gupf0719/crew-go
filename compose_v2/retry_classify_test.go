package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// statusErr 是实现 compose_v2.HTTPStatusProvider 的测试错误。
type statusErr struct {
	code int
}

func (e *statusErr) Error() string   { return fmt.Sprintf("api error: HTTP %d", e.code) }
func (e *statusErr) HTTPStatus() int { return e.code }

// TestIsRetryable_DeterministicHTTPFailures 验证确定性失败不重试。
//
// **Bug 场景**：修复前 IsRetryable = !IsControlFlowSignal，除控制流外什么都重试——
// 401（API key 错）/ 400（请求格式错）会被重试 N 次 + 指数退避睡眠，每次注定同样失败。
// 影响 7 个编排的 sub-agent 调用（RunWithRetry 包的就是它们）。
func TestIsRetryable_DeterministicHTTPFailures(t *testing.T) {
	tests := []struct {
		name string
		code int
		want bool
	}{
		// 确定性失败——请求本身有问题，重发必然同样失败
		{"400 参数错", 400, false},
		{"401 鉴权失败", 401, false},
		{"403 无权限", 403, false},
		{"404 模型不存在", 404, false},
		{"422 请求体不合法", 422, false},
		// 值得重试
		{"0 非 HTTP 错误（网络/超时/解析）", 0, true},
		{"408 请求超时", 408, true},
		{"409 冲突", 409, true},
		{"429 限流", 429, true},
		{"500 provider 故障", 500, true},
		{"502 网关错误", 502, true},
		{"503 服务不可用", 503, true},
		// 未知 4xx 保守可重试——宁可多试一次，不把瞬时错误钉死成永久失败
		{"418 未知 4xx", 418, true},
		{"451 未知 4xx", 451, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compose_v2.IsRetryable(&statusErr{code: tt.code}); got != tt.want {
				t.Errorf("IsRetryable(HTTP %d) = %v，期望 %v", tt.code, got, tt.want)
			}
		})
	}
}

// TestIsRetryable_UnwrapsChain 验证沿 Unwrap 链识别——业务 wrap 后仍准确分类。
func TestIsRetryable_UnwrapsChain(t *testing.T) {
	wrapped := fmt.Errorf("sub-agent run: %w", fmt.Errorf("model call: %w", &statusErr{code: 401}))
	if compose_v2.IsRetryable(wrapped) {
		t.Fatal("wrap 两层的 401 应仍判不可重试——Unwrap 链未被遍历，业务 wrap 后分类失效")
	}
}

// TestIsRetryable_PlainErrorStillRetryable 验证不带状态码的普通 error 仍可重试。
//
// 防回归：加了 HTTP 分类不能把 tool 业务错、网络库原生 error 这类误判成不可重试。
func TestIsRetryable_PlainErrorStillRetryable(t *testing.T) {
	if !compose_v2.IsRetryable(errors.New("tool: connection reset by peer")) {
		t.Fatal("不带 HTTP 状态码的普通 error 应可重试——分类改动误伤了非 HTTP 错误路径")
	}
	if compose_v2.IsRetryable(nil) {
		t.Fatal("nil 不应可重试")
	}
}

// TestIsRetryable_GraphIDConflictIsDeterministic：身份冲突（两张图共用一个存档槽）重跑必然撞同一个
// 槽，不可重试；换了类型之后 errors.Is 仍认得出它。曾经是 errors.New，被当成可重试——配了重试的
// 编排会把整个成员连同副作用再跑几遍。
func TestIsRetryable_GraphIDConflictIsDeterministic(t *testing.T) {
	err := fmt.Errorf("子图载入子档: %w", compose_v2.ErrGraphIDConflict)
	if compose_v2.IsRetryable(err) {
		t.Fatal("身份冲突重跑必然一样，不该可重试")
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatal("包一层之后 errors.Is 应仍认得出 ErrGraphIDConflict")
	}
	if compose_v2.ErrGraphIDConflict.Error() != "graph_id conflict" {
		t.Errorf("错误正文变了：%q", compose_v2.ErrGraphIDConflict.Error())
	}
}

// TestIsRetryable_ControlFlowStillWins 验证控制流信号优先级不被 HTTP 分类打乱。
//
// InterruptError 即使被包成带状态码的错误也必须判不可重试——HITL 控制流不能被吃掉。
func TestIsRetryable_ControlFlowStillWins(t *testing.T) {
	ie := &compose_v2.InterruptError{
		Reason: "ask user",
		Query:  &compose_v2.AskQuery{Question: "continue?"},
	}
	if compose_v2.IsRetryable(ie) {
		t.Fatal("InterruptError 必须不可重试")
	}
	// 500 本来可重试，但包着 InterruptError 时控制流优先
	wrapped := fmt.Errorf("HTTP 500: %w", ie)
	if compose_v2.IsRetryable(wrapped) {
		t.Fatal("包着 InterruptError 的错误必须判不可重试——控制流优先级被 HTTP 分类打乱")
	}
	if compose_v2.IsRetryable(context.Canceled) {
		t.Fatal("context.Canceled 必须不可重试")
	}
}

// TestIsRateLimited 验证 429 识别。
func TestIsRateLimited(t *testing.T) {
	if !compose_v2.IsRateLimited(&statusErr{code: 429}) {
		t.Error("429 应判限流")
	}
	if compose_v2.IsRateLimited(&statusErr{code: 500}) {
		t.Error("500 不应判限流")
	}
	if compose_v2.IsRateLimited(errors.New("plain")) {
		t.Error("不带状态码的 error 不应判限流")
	}
	// Unwrap 链
	if !compose_v2.IsRateLimited(fmt.Errorf("wrap: %w", &statusErr{code: 429})) {
		t.Error("wrap 后的 429 应仍判限流")
	}
}

// TestRunWithRetry_SkipsDeterministicFailure 验证确定性失败不进重试循环。
//
//   - fix 后：401 只调 1 次，立即返
//   - fix 前：调 MaxRetries+1 次，每次之间还睡指数退避
func TestRunWithRetry_SkipsDeterministicFailure(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries: 3,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: 20 * time.Millisecond,
			Multiplier:   2.0,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	start := time.Now()
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &statusErr{code: 401}
		})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("401 应作为错误返回")
	}
	if calls != 1 {
		t.Fatalf("401 是确定性失败应只调 1 次，实际调了 %d 次——每次重发都注定同样失败，"+
			"纯浪费 quota 且拖长 Run", calls)
	}
	if elapsed > 15*time.Millisecond {
		t.Fatalf("不该有退避睡眠，实际耗时 %v", elapsed)
	}
}

// TestRunWithRetry_StillRetriesTransient 验证瞬时失败仍走重试（防 fix 改坏主路径）。
func TestRunWithRetry_StillRetriesTransient(t *testing.T) {
	cfg := compose_v2.RetryConfig{MaxRetries: 3, BackoffPolicy: compose_v2.BackoffPolicy{InitialDelay: time.Millisecond}, ShouldRetry: compose_v2.IsRetryable}

	var calls int
	out, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			if calls < 3 {
				return 0, &statusErr{code: 503}
			}
			return 42, nil
		})

	if err != nil || out != 42 {
		t.Fatalf("503 应重试到成功，实际 out=%d err=%v", out, err)
	}
	if calls != 3 {
		t.Fatalf("应调 3 次（2 次失败 + 1 次成功），实际 %d 次", calls)
	}
}

// TestRunWithRetry_RateLimitUsesLowerCap 验证 429 走独立的更低上限。
//
// **Bug 场景**：限流等待是秒到分钟级（还常照 Retry-After 等），跟 5xx 毫秒级重试
// 共用一个 MaxRetries 会让一次限流把整个 Run 拖很久。
//
//   - fix 后：429 只重试 RateLimitMaxRetries(1) 次 → 共 2 次调用
//   - fix 前：重试 MaxRetries(5) 次 → 共 6 次调用
func TestRunWithRetry_RateLimitUsesLowerCap(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries:          5,
		RateLimitMaxRetries: 1,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: time.Millisecond,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &statusErr{code: 429}
		})

	if err == nil {
		t.Fatal("持续限流应最终返错")
	}
	if calls != 2 {
		t.Fatalf("429 应只用 RateLimitMaxRetries=1（共 2 次调用），实际 %d 次——"+
			"限流跟 5xx 共用 MaxRetries=5 会把 Run 拖很久", calls)
	}
}

// TestRunWithRetry_RateLimitCapDoesNotAffectOthers 验证限流上限不误伤其他错误。
func TestRunWithRetry_RateLimitCapDoesNotAffectOthers(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries:          3,
		RateLimitMaxRetries: 1,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: time.Millisecond,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &statusErr{code: 503}
		})

	if err == nil {
		t.Fatal("持续 503 应最终返错")
	}
	if calls != 4 {
		t.Fatalf("503 应用 MaxRetries=3（共 4 次调用），实际 %d 次——"+
			"限流专用上限误伤了非限流错误", calls)
	}
}

// TestRunWithRetry_RateLimitCapIgnoredWhenLarger 验证限流上限大于总预算时不反向放大。
//
// RateLimitMaxRetries 设得比 MaxRetries 大时不生效（不能反向放大重试次数）。
// **必须断言 err**：只断言 calls 抓不到 bug——calls 被外层 `for attempt <= cfg.MaxRetries`
// 的循环边界卡死，无论限流预算多大都是 `MaxRetries+1`。真正的故障模式在返回值上：
// 退出条件写错会让循环自然走完落到函数末尾 `return zero, nil`，**持续 429 却返回 nil
// error，失败被吞成成功**。
func TestRunWithRetry_RateLimitCapIgnoredWhenLarger(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries:          1,
		RateLimitMaxRetries: 10,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: time.Millisecond,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &statusErr{code: 429}
		})

	if err == nil {
		t.Fatal("持续 429 耗尽总预算后必须返非 nil err——退出条件写错会让循环自然走完" +
			"落到 return zero, nil，失败被吞成成功")
	}
	if calls != 2 {
		t.Fatalf("RateLimitMaxRetries(10) > MaxRetries(1) 时受总预算约束（共 2 次），"+
			"实际 %d 次——限流上限反向放大了重试预算", calls)
	}
}

// bizStatusWrap 模拟业务按 HTTPStatusProvider 的邀请包的一层自己的错误类型：
// 它实现了接口，但在「这不是 HTTP 错」时返 0。
type bizStatusWrap struct {
	code  int
	inner error
}

func (b bizStatusWrap) Error() string   { return "biz: " + b.inner.Error() }
func (b bizStatusWrap) Unwrap() error   { return b.inner }
func (b bizStatusWrap) HTTPStatus() int { return b.code }

// TestHTTPStatusClassify_SkipsZeroImplementers 钉住「返 0 的实现者不算数，继续往下找」。
//
// **跟 RetryAfterFromError 是同一个形状的坑**：若按「首个实现者」取值，业务包一层
// 自己的错误类型、在「这不是 HTTP 错」时返 0，就会把里层 `model.APIError` 的状态码
// 整个遮掉 —— 400 这类确定性失败重新被重试 N 次（每次之间还照指数退避睡），429 重新
// 跟 5xx 抢同一份预算。两者都 err == nil，唯一症状是「白烧配额 + Run 变慢」。
//
// `errors.Join` 那两条钉的是另一半：取值不能随成员顺序翻。
func TestHTTPStatusClassify_SkipsZeroImplementers(t *testing.T) {
	det := &statusErr{code: 400} // 确定性失败
	rl := &statusErr{code: 429}  // 限流
	zero := &statusErr{code: 0}  // 非 HTTP 层错误
	plain := errors.New("plain") // 压根没实现接口

	t.Run("确定性失败不被外层的 0 遮住", func(t *testing.T) {
		cases := []struct {
			name string
			err  error
		}{
			{"业务壳返 0 包住 400", bizStatusWrap{code: 0, inner: det}},
			{"套两层壳", bizStatusWrap{code: 0, inner: bizStatusWrap{code: 0, inner: det}}},
			{"多错误树：零值在前", errors.Join(zero, det)},
			{"多错误树：零值在后", errors.Join(det, zero)},
		}
		for _, c := range cases {
			if compose_v2.IsRetryable(c.err) {
				t.Errorf("%s：判成了可重试——里层 400 被遮掉了，"+
					"一个重发必然同样失败的请求会被重试到次数用满", c.name)
			}
		}
	})

	t.Run("限流不被外层的 0 遮住", func(t *testing.T) {
		cases := []struct {
			name string
			err  error
		}{
			{"业务壳返 0 包住 429", bizStatusWrap{code: 0, inner: rl}},
			{"多错误树：零值在前", errors.Join(zero, rl)},
		}
		for _, c := range cases {
			if !compose_v2.IsRateLimited(c.err) {
				t.Errorf("%s：没认出限流——它会去跟 5xx 抢同一份重试预算", c.name)
			}
		}
	})

	t.Run("确实没有状态码时不误判", func(t *testing.T) {
		for _, e := range []error{plain, zero, errors.Join(plain, zero), nil} {
			if compose_v2.IsRateLimited(e) {
				t.Errorf("%v 被当成了限流", e)
			}
		}
		// 没有状态码的普通错误仍然可重试（框架不替业务猜不透明的错）
		if !compose_v2.IsRetryable(plain) {
			t.Error("不带状态码的普通错误不该被判成不可重试")
		}
	})
}

// TestRunWithRetry_RateLimitCounterIsIndependent 验证限流计数**独立**于其他错误。
//
// **这是本特性的核心契约**，也是初版实现的 bug：初版用共享的 attempt 下标去比限流
// 专属预算，混合序列下算的是别人消耗的次数。
//
// **测试设计**：`MaxRetries=5, RateLimitMaxRetries=2`，序列 `503, 503, 429, 429, 429`。
//   - fix 后：前两次 503 各消耗 attempt 不碰限流预算；第 3/4 次 429 用掉 2 次限流预算；
//     第 5 次 429 超限退出 → 共 5 次调用
//   - fix 前（共享计数）：第 3 次 429 时 attempt 已到 2 >= RateLimitMaxRetries 直接放弃
//     → 共 3 次调用，**429 一次都没重试**
func TestRunWithRetry_RateLimitCounterIsIndependent(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries:          5,
		RateLimitMaxRetries: 2,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: time.Millisecond,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	seq := []int{503, 503, 429, 429, 429}
	var calls int
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			code := seq[calls]
			calls++
			return 0, &statusErr{code: code}
		})

	if err == nil {
		t.Fatal("预算耗尽应返非 nil err")
	}
	if calls != 5 {
		t.Fatalf("限流计数应独立于 503 消耗的次数：503,503 不碰限流预算，两次 429 用掉 "+
			"RateLimitMaxRetries=2，第三次 429 超限退出 → 共 5 次调用；实际 %d 次。"+
			"若为 3 次说明用了共享 attempt 计数——429 被 503 消耗的次数挤掉，一次都没重试", calls)
	}
}

// TestRunWithRetry_RateLimitCounterRespectsTotalBudget 验证两个上限同时生效（谁先到谁停）。
//
// 纯 429 序列下总预算先耗尽——防独立计数器写成「只看限流预算、无视 MaxRetries」。
func TestRunWithRetry_RateLimitCounterRespectsTotalBudget(t *testing.T) {
	cfg := compose_v2.RetryConfig{
		MaxRetries:          2, // 总预算更小
		RateLimitMaxRetries: 10,
		BackoffPolicy: compose_v2.BackoffPolicy{
			InitialDelay: time.Millisecond,
		},
		ShouldRetry: compose_v2.IsRetryable,
	}

	var calls int
	_, err := compose_v2.RunWithRetry(context.Background(), cfg,
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &statusErr{code: 429}
		})

	if err == nil {
		t.Fatal("预算耗尽应返非 nil err")
	}
	if calls != 3 {
		t.Fatalf("总预算 MaxRetries=2 应先到（共 3 次调用），实际 %d 次——"+
			"独立计数器无视了总预算", calls)
	}
}
