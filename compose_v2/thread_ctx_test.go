package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// TestThreadIDCtx 验 WithThreadIDCtx/GetThreadID round-trip + 空值不注入 + 跟
// sessionID 独立 (gap 3: observability session.id 取 thread, request.id 取 sid)。
func TestThreadIDCtx(t *testing.T) {
	ctx := context.Background()
	if got := compose_v2.GetThreadID(ctx); got != "" {
		t.Errorf("空 ctx GetThreadID = %q; want 空", got)
	}
	// 空 threadID 不注入 (返原 ctx, 跟 WithSessionIDCtx 同款)
	if got := compose_v2.GetThreadID(compose_v2.WithThreadIDCtx(ctx, "")); got != "" {
		t.Errorf(`WithThreadIDCtx("") 后 GetThreadID = %q; want 空`, got)
	}
	// 正常 round-trip
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv_7")
	if got := compose_v2.GetThreadID(ctx); got != "conv_7" {
		t.Errorf("GetThreadID = %q; want conv_7", got)
	}
	// thread 与 session 独立 key: 同 ctx 各取各的, 互不串
	ctx = compose_v2.WithSessionIDCtx(ctx, "run_42")
	if tid := compose_v2.GetThreadID(ctx); tid != "conv_7" {
		t.Errorf("push session 后 GetThreadID = %q; want conv_7 (独立)", tid)
	}
	if sid := compose_v2.GetSessionID(ctx); sid != "run_42" {
		t.Errorf("GetSessionID = %q; want run_42", sid)
	}
}
