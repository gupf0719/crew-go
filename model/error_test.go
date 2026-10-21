package model_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/model"
)

// TestAPIError_ErrorString 验证错误串含 provider + 状态码 + body（HTTP）/ 底层 err（网络）。
// 关键：Body 必须出现在 Error() 里——ReactiveCompact 等靠错误文本匹配 prompt-too-long
// 的上层逻辑依赖此，包装成 APIError 后不能丢 body。
func TestAPIError_ErrorString(t *testing.T) {
	httpErr := &model.APIError{Provider: "openai", StatusCode: 400, Body: `{"error":"prompt is too long"}`}
	got := httpErr.Error()
	for _, want := range []string{"openai", "400", "prompt is too long"} {
		if !strings.Contains(got, want) {
			t.Fatalf("HTTP 错误串缺 %q: %q", want, got)
		}
	}

	netErr := &model.APIError{Provider: "qwen", Err: errors.New("dial tcp: connection refused")}
	if !strings.Contains(netErr.Error(), "connection refused") {
		t.Fatalf("网络错误串缺底层 err: %q", netErr.Error())
	}

	// Provider 缺省时回落，不 panic、仍含状态码
	if got := (&model.APIError{StatusCode: 500, Body: "x"}).Error(); !strings.Contains(got, "500") {
		t.Fatalf("缺 provider 时错误串异常: %q", got)
	}
}

// TestAPIError_Unwrap 验证 errors.Is 穿透到底层——控制流信号（如 context.Canceled）
// 不能被故障切换层误判为"该切模型"。
func TestAPIError_Unwrap(t *testing.T) {
	e := &model.APIError{Provider: "openai", Err: context.Canceled}
	if !errors.Is(e, context.Canceled) {
		t.Fatal("errors.Is 未穿透 APIError.Err 到 context.Canceled")
	}
	// 多层 wrap 仍可穿透（client 实际用 fmt.Errorf 包了一层 "request failed"）
	wrapped := &model.APIError{Provider: "openai", Err: fmt.Errorf("request failed: %w", context.Canceled)}
	if !errors.Is(wrapped, context.Canceled) {
		t.Fatal("errors.Is 未穿透多层 wrap 到 context.Canceled")
	}
}

// TestAPIError_As 验证上层按 errors.As 拿 StatusCode 做故障切换分类。
func TestAPIError_As(t *testing.T) {
	var err error = &model.APIError{Provider: "deepseek", StatusCode: 503, Body: "bad gateway"}
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) {
		t.Fatal("errors.As 未匹配 *APIError")
	}
	if apiErr.StatusCode != 503 {
		t.Fatalf("StatusCode = %d, want 503", apiErr.StatusCode)
	}
}
