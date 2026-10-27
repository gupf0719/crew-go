package fallback_test

// 全部候选都失败时交出的错误，可重试性要等于整次调用的：有候选的失败还值得再来一次时外层照常重试整次调用。

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

// countingModel 第 n 次（从 1 数）调用按 err(n) 失败，返回 nil 时成功。
type countingModel struct {
	n   *atomic.Int32
	err func(n int32) error
}

func (m countingModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	if e := m.err(m.n.Add(1)); e != nil {
		return nil, e
	}
	return schema.AssistantMessage("ok"), nil
}

func (m countingModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m countingModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func billing429() error {
	return &model.APIError{Provider: "p1", StatusCode: 429, Body: `{"error":{"code":"insufficient_quota"}}`}
}

// 主候选额度用完（不可重试）、备用瞬时 503：外层重试整次调用，备用第二次成功（曾经交出主候选的错，外层不再重试，审计实测）。
func TestFinalError_TransientBackupKeepsCallRetryable(t *testing.T) {
	var n1, n2 atomic.Int32
	m1 := countingModel{n: &n1, err: func(int32) error { return billing429() }}
	m2 := countingModel{n: &n2, err: func(k int32) error {
		if k == 1 {
			return &model.APIError{Provider: "p2", StatusCode: 503, Body: "overloaded"}
		}
		return nil
	}}
	fb := fallback.New([]model.ChatModel{m1, m2})
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(2), func(ctx context.Context) (*schema.Message, error) {
		return fb.Generate(ctx, nil)
	})
	if err != nil || n2.Load() != 2 {
		t.Fatalf("外层应重试整次调用、备用第二次成功，实际 err=%v 备用被调 %d 次", err, n2.Load())
	}
}

// 两个候选都是额度用完：交出排得最靠前的那个，外层不重试。
func TestFinalError_AllPermanentReportsFirst(t *testing.T) {
	var n1, n2 atomic.Int32
	m1 := countingModel{n: &n1, err: func(int32) error { return billing429() }}
	m2 := countingModel{n: &n2, err: func(int32) error {
		return &model.APIError{Provider: "p2", StatusCode: 429, Body: `{"error":{"code":"insufficient_quota"}}`}
	}}
	fb := fallback.New([]model.ChatModel{m1, m2})
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(2), func(ctx context.Context) (*schema.Message, error) {
		return fb.Generate(ctx, nil)
	})
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.Provider != "p1" || !errors.Is(err, model.ErrBillingExhausted) || n1.Load() != 1 {
		t.Fatalf("应交出主候选的额度用完、不重试，实际 err=%v 主候选被调 %d 次", err, n1.Load())
	}
}

// 主候选额度用完；备用 429 且 Retry-After 远超调用方期限（这次等不到）：交出主候选的错、立即失败——备用的 429 不算「值得
// 再来一次」（曾经交出它，外层照 Retry-After 睡到期限，原因被换成期限到了，审计实测）。
func TestFinalError_UnreachableBackupNotRetried(t *testing.T) {
	var n1, n2 atomic.Int32
	m1 := countingModel{n: &n1, err: func(int32) error { return billing429() }}
	m2 := countingModel{n: &n2, err: func(int32) error {
		return &model.APIError{Provider: "p2", StatusCode: 429, Body: "rate limited", RetryAfter: 10 * time.Minute}
	}}
	fb := fallback.New([]model.ChatModel{m1, m2})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (*schema.Message, error) {
		return fb.Generate(ctx, nil)
	})
	var apiErr *model.APIError
	if elapsed := time.Since(start); elapsed > time.Second || !errors.As(err, &apiErr) || apiErr.Provider != "p1" {
		t.Fatalf("应立即交出主候选的错，实际耗时 %v、err=%v", elapsed, err)
	}
}

// 主候选 429 且 Retry-After 远超期限（这次等不到）、备用额度用完：没有一个候选值得再来一次，交出主候选的错但外层不再重试——
// 立即失败，不照它的 Retry-After 睡到期限（审计实测曾经睡满期限、原因被换成期限到了）。
func TestFinalError_UnreachablePrimaryNotRetried(t *testing.T) {
	var n1, n2 atomic.Int32
	m1 := countingModel{n: &n1, err: func(int32) error {
		return &model.APIError{Provider: "p1", StatusCode: 429, Body: "rate limited", RetryAfter: 10 * time.Minute}
	}}
	m2 := countingModel{n: &n2, err: func(int32) error {
		return &model.APIError{Provider: "p2", StatusCode: 429, Body: `{"error":{"code":"insufficient_quota"}}`}
	}}
	fb := fallback.New([]model.ChatModel{m1, m2})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (*schema.Message, error) {
		return fb.Generate(ctx, nil)
	})
	var apiErr *model.APIError
	if elapsed := time.Since(start); elapsed > time.Second || !errors.As(err, &apiErr) || apiErr.Provider != "p1" {
		t.Fatalf("应立即交出主候选的错，实际耗时 %v、err=%v", elapsed, err)
	}
}
