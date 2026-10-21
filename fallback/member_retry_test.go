package fallback_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// 「在同一个候选上重试」与「换下一个候选」是两个判断（见 fallback.go 的 callMemberWithRetry）。

func quotaExhausted() error {
	return &model.APIError{Provider: "dashscope", StatusCode: 403,
		Body: `{"code":"AllocationQuota.FreeTierOnly","message":"The free tier of the model has been exhausted."}`}
}

// 额度耗尽：不在这个候选上重试（等多久都不会恢复），直接换下一个。曾经两个问题共用一个判据，额度耗尽先在
// 第一个候选上按逐成员重试的次数重试、白等退避才换（实测 3 次请求、600 毫秒）。
func TestMemberRetry_QuotaExhaustedSwitchesWithoutRetry(t *testing.T) {
	first := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) { return nil, quotaExhausted() }}
	second := &stubModel{name: "b"}
	fb := fallback.New([]model.ChatModel{first, second}, fallback.WithPerModelRetry(fallback.PerModelRetry{
		MaxRetries: 2, Backoff: compose_v2.BackoffPolicy{InitialDelay: 200 * time.Millisecond}}))
	start := time.Now()
	out, err := fb.Generate(context.Background(), msgs())
	if err != nil {
		t.Fatalf("应换到第二个候选：%v", err)
	}
	if out.Content != "b ok" {
		t.Fatalf("应由第二个候选答复，实际 %q", out.Content)
	}
	if n := first.callCount(); n != 1 {
		t.Fatalf("额度耗尽不该在同一个候选上重试，第一个候选被调了 %d 次", n)
	}
	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("额度耗尽不该等退避，花了 %v", d)
	}
}

// 服务端要等很久（429 + Retry-After 10 分钟）、后面还有能换的候选：不在这儿等，直接换。曾经照着 Retry-After
// 干等，调用方的超时先到，第二个候选一次都没被调用。
func TestMemberRetry_LongRetryAfterSwitchesToNext(t *testing.T) {
	first := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 10 * time.Minute}
	}}
	second := &stubModel{name: "b"}
	fb := fallback.New([]model.ChatModel{first, second}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := fb.Generate(ctx, msgs())
	if err != nil {
		t.Fatalf("服务端要等 10 分钟、后面还有候选，应直接换：%v", err)
	}
	if out.Content != "b ok" {
		t.Fatalf("应由第二个候选答复，实际 %q", out.Content)
	}
	if n := first.callCount(); n != 1 {
		t.Fatalf("第一个候选应只调 1 次，实际 %d 次", n)
	}
}

// 只有一个候选、服务端要等很久、调用方没设期限：照 Retry-After 等（外层边界交给调用方的 ctx），不当成「换不了就
// 放弃」。
func TestMemberRetry_LongRetryAfterOnlyMemberWaits(t *testing.T) {
	only := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 61 * time.Second}
	}}
	fb := fallback.New([]model.ChatModel{only}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	start := time.Now()
	_, err := fb.Generate(ctx, msgs())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("没设期限时应照 Retry-After 等，直到调用方取消：%v", err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("没等就返回了（%v）", d)
	}
}

// 要等的时间不短于调用方剩下的时间：等完也来不及再请求，不等，当场返回服务端的错误（不是等到超时、拿一个 ctx 错误）。
func TestMemberRetry_RetryAfterBeyondDeadlineReturnsAtOnce(t *testing.T) {
	only := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 61 * time.Second}
	}}
	fb := fallback.New([]model.ChatModel{only}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := fb.Generate(ctx, msgs())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 {
		t.Fatalf("等不完时应当场返回服务端的错误，实际 %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("等不完时不该等，花了 %v", d)
	}
	if n := only.callCount(); n != 1 {
		t.Fatalf("应只请求 1 次，实际 %d 次", n)
	}
}

// 后面的候选都熔断了（换不过去）、调用方没设期限：当前候选照 Retry-After 等，不直接返回限流错误（那等于什么都没做）。
func TestMemberRetry_LongRetryAfterWaitsWhenNextIsTripped(t *testing.T) {
	var mode atomic.Value
	mode.Store("fail")
	first := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) {
		switch mode.Load().(string) {
		case "ok":
			return nil, nil
		case "longRA":
			return nil, &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 61 * time.Second}
		default:
			return nil, &model.APIError{Provider: "p", StatusCode: 503}
		}
	}}
	second := &stubModel{name: "b", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "p", StatusCode: 503}
	}}
	fb := fallback.New([]model.ChatModel{first, second}, fallback.WithBreaker(2, time.Hour),
		fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	// 前置：两次都失败的 sweep 让第二个候选熔断，中间一次成功把第一个候选的计数清零
	_, _ = fb.Generate(context.Background(), msgs())
	mode.Store("ok")
	if _, err := fb.Generate(context.Background(), msgs()); err != nil {
		t.Fatalf("前置：第一个候选应成功：%v", err)
	}
	mode.Store("fail")
	_, _ = fb.Generate(context.Background(), msgs())
	secondCalls := second.callCount()

	mode.Store("longRA")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	start := time.Now()
	_, err := fb.Generate(ctx, msgs())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("后面的候选都熔断了，应照 Retry-After 等到调用方取消：%v", err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("后面没有能换的候选，却没等就返回了（%v）", d)
	}
	if n := second.callCount(); n != secondCalls {
		t.Fatalf("熔断的候选不该被调用（%d → %d）", secondCalls, n)
	}
}

// 要等的时间没超过 60 秒、但比调用方剩下的时间还长：等不完，换到下一个候选。曾经只看 60 秒，Retry-After 5 秒对
// 300 毫秒的超时照样干等到超时，第二个候选一次都没被调用（审计实测）。
func TestMemberRetry_RetryAfterBeyondDeadlineSwitches(t *testing.T) {
	first := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 5 * time.Second}
	}}
	second := &stubModel{name: "b"}
	fb := fallback.New([]model.ChatModel{first, second}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	out, err := fb.Generate(ctx, msgs())
	if err != nil || out.Content != "b ok" {
		t.Fatalf("等不完时应换到第二个候选：out=%v err=%v", out, err)
	}
	if n := first.callCount(); n != 1 {
		t.Fatalf("第一个候选应只调 1 次，实际 %d 次", n)
	}
}

// 按时间窗口的配额（「每分钟请求数超了」）是限流，等一会儿就恢复：唯一的候选上照 Retry-After 重试。曾经借了切换判据
// 的额度关键词，"Quota exceeded" 被当成等多久都不会恢复，不重试、直接失败（审计实测）。计费类额度用完照旧不重试。
func TestMemberRetry_PerMinuteQuotaRetried(t *testing.T) {
	perMinute := &model.APIError{Provider: "vertex", StatusCode: 429, RetryAfter: 50 * time.Millisecond,
		Body: `{"error":{"code":429,"message":"Quota exceeded for aiplatform.googleapis.com/generate_content_requests_per_minute_per_project_per_base_model","status":"RESOURCE_EXHAUSTED"}}`}
	only := &stubModel{name: "a", respond: func(n int, _ []*schema.Message) (*schema.Message, error) {
		if n == 0 {
			return nil, perMinute
		}
		return nil, nil
	}}
	fb := fallback.New([]model.ChatModel{only}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	out, err := fb.Generate(context.Background(), msgs())
	if err != nil || out.Content != "a ok" {
		t.Fatalf("按分钟的配额应等 Retry-After 重试成功：out=%v err=%v", out, err)
	}
	if n := only.callCount(); n != 2 {
		t.Fatalf("应请求 2 次，实际 %d 次", n)
	}

	billing := &stubModel{name: "b", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "openai", StatusCode: 429, Body: `{"error":{"code":"insufficient_quota"}}`}
	}}
	fb = fallback.New([]model.ChatModel{billing}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
	if _, err := fb.Generate(context.Background(), msgs()); err == nil {
		t.Fatal("账户额度用完应失败")
	}
	if n := billing.callCount(); n != 1 {
		t.Fatalf("账户额度用完不该重试，实际请求 %d 次", n)
	}
}

// 会自己恢复的错误（503）照旧在同一个候选上重试。
func TestMemberRetry_TransientStillRetried(t *testing.T) {
	first := &stubModel{name: "a", respond: func(n int, _ []*schema.Message) (*schema.Message, error) {
		if n == 0 {
			return nil, &model.APIError{Provider: "p", StatusCode: 503}
		}
		return nil, nil
	}}
	second := &stubModel{name: "b"}
	fb := fallback.New([]model.ChatModel{first, second}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 1}))
	out, err := fb.Generate(context.Background(), msgs())
	if err != nil || out.Content != "a ok" {
		t.Fatalf("503 应在第一个候选上重试成功：out=%v err=%v", out, err)
	}
	if first.callCount() != 2 || second.callCount() != 0 {
		t.Fatalf("第一个候选应调 2 次、第二个 0 次，实际 %d / %d", first.callCount(), second.callCount())
	}
}

// deterministicErr 是业务自己的错误类型，声明了 compose_v2.NonRetryable。
type deterministicErr struct{}

func (deterministicErr) Error() string { return "deterministic failure" }

// NonRetryable 实现 compose_v2.NonRetryable。
func (deterministicErr) NonRetryable() {}

// 声明了 compose_v2.NonRetryable 的错误不在同一个候选上重试；换不换候选照切换判据（不认识的错误默认换）。
func TestMemberRetry_NonRetryableNotRetried(t *testing.T) {
	first := &stubModel{name: "a", respond: func(int, []*schema.Message) (*schema.Message, error) { return nil, deterministicErr{} }}
	second := &stubModel{name: "b"}
	fb := fallback.New([]model.ChatModel{first, second}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
	out, err := fb.Generate(context.Background(), msgs())
	if err != nil || out.Content != "b ok" {
		t.Fatalf("应换到第二个候选：out=%v err=%v", out, err)
	}
	if n := first.callCount(); n != 1 {
		t.Fatalf("声明了不可重试的错误不该在同一个候选上重试，第一个候选被调了 %d 次", n)
	}
}
