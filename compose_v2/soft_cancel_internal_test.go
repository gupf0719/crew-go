package compose_v2

import (
	"context"
	"testing"
)

// TestResolveSoftCancel 白盒验优先级：RunOption 显式 > 父 ctx 继承 > nil。
func TestResolveSoftCancel(t *testing.T) {
	bg := context.Background()
	optSig := NewSoftCancelSignal()
	ctxSig := NewSoftCancelSignal()

	// 都无 → nil
	if got := resolveSoftCancel(bg, nil); got != nil {
		t.Errorf("都无应 nil，实际 %p", got)
	}
	// 仅 ctx → 继承 ctx
	if got := resolveSoftCancel(WithSoftCancelCtx(bg, ctxSig), nil); got != ctxSig {
		t.Errorf("应继承 ctx 信号")
	}
	// RunOption 显式 → 优先（忽略 ctx）
	if got := resolveSoftCancel(WithSoftCancelCtx(bg, ctxSig), optSig); got != optSig {
		t.Errorf("RunOption 应优先于 ctx")
	}
}

// TestGetSoftCancel_Unset 未 push 时返 nil。
func TestGetSoftCancel_Unset(t *testing.T) {
	if got := GetSoftCancel(context.Background()); got != nil {
		t.Errorf("未设应 nil，实际 %p", got)
	}
}
