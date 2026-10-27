package fallback_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// TestPerModelRetry_ExhaustedNotRetriedAbove：每候选重试用完之后交出的错误带「已在这一层重试过」的标记，外层的重试
// （编排成员重试、BasicAgent 的模型调用重试都走 compose_v2.RunWithRetry）不再重做——一个失败只被重试一层，不然
// 两层各 2 次就是 9 次请求。
func TestPerModelRetry_ExhaustedNotRetriedAbove(t *testing.T) {
	only := &stubModel{name: "only", respond: func(int, []*schema.Message) (*schema.Message, error) { return nil, apiErr(503) }}
	fb := fallback.New([]model.ChatModel{only}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
	_, err := compose_v2.RunWithRetry(context.Background(),
		compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }},
		func(ctx context.Context) (*schema.Message, error) {
			return fb.Generate(ctx, []*schema.Message{schema.UserMessage("hi")})
		})
	if n := only.callCount(); n != 3 {
		t.Fatalf("候选上重试用完之后外层不该再重做：期望 3 次请求，实际 %d 次", n)
	}
	var re *compose_v2.RetriesExhaustedError
	if !errors.As(err, &re) {
		t.Fatalf("交出的错误应带标记，实际 %v", err)
	}
	var ae *model.APIError
	if !errors.As(err, &ae) || ae.StatusCode != 503 {
		t.Fatalf("APIError 应仍能取到，实际 %v", err)
	}
}

// TestPerModelRetry_ExhaustedMarkSurvivesFailover：主候选额度用完（不在候选上重试、直接切换），备用候选重试用完。交出的
// 是排在前面的主候选的错，它本身没带标记——标记仍要跟着这次调用走，外层不再整轮重做。曾经丢了标记，外层重做 2 次，备用
// 被请求 9 次（审计实测）。
func TestPerModelRetry_ExhaustedMarkSurvivesFailover(t *testing.T) {
	primary := &stubModel{name: "primary", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "stub", StatusCode: 429, Body: `{"error":{"code":"insufficient_quota"}}`}
	}}
	backup := &stubModel{name: "backup", respond: func(int, []*schema.Message) (*schema.Message, error) { return nil, apiErr(503) }}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(2),
		func(ctx context.Context) (*schema.Message, error) {
			return fb.Generate(ctx, []*schema.Message{schema.UserMessage("hi")})
		})
	if p, b := primary.callCount(), backup.callCount(); p != 1 || b != 3 {
		t.Fatalf("外层不该再整轮重做：期望 primary=1 backup=3，实际 primary=%d backup=%d", p, b)
	}
	var ae *model.APIError
	if !errors.As(err, &ae) || ae.StatusCode != 429 {
		t.Fatalf("交出的仍应是排在前面的主候选的错，实际 %v", err)
	}
}
