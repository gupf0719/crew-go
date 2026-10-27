package anthropic_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// TestBillingError_RecognizedFromRealClientShape：Anthropic 的 400 credit balance 经真客户端变成的 APIError（HTTP 错误上
// 也带着 SDK 的错误）照样认成计费类额度用完——默认重试判据不重试、fallback 切换候选。判据只在 model 包一处，手写一个
// Err 为空的 APIError 做输入测不出客户端的形状。
func TestBillingError_RecognizedFromRealClientShape(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replyJSON(w, http.StatusBadRequest, map[string]any{"type": "error", "error": map[string]any{
			"type":    "invalid_request_error",
			"message": "Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.",
		}})
	})
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest || apiErr.Err == nil {
		t.Fatalf("fixture：应是带 SDK 错误的 HTTP 400 APIError，got %#v", err)
	}
	if !errors.Is(err, model.ErrBillingExhausted) {
		t.Error("计费错误应认成 model.ErrBillingExhausted")
	}
	if compose_v2.IsRetryable(err) {
		t.Error("计费错误不该被默认重试判据重试")
	}
	if !(fallback.DefaultFailoverPolicy{}).ShouldFailover(context.Background(), err) {
		t.Error("计费错误应切换候选")
	}
}
