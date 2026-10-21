package compose_v2

import (
	"context"
	"testing"
)

// TestResolveDurability 白盒直测优先级：RunOption 显式 > 父 ctx 继承 > 默认 Exit。
// 集成测试经"是否每超步写"间接验，这里直打函数枚举边界更精确（含空串归一）。
func TestResolveDurability(t *testing.T) {
	bg := context.Background()
	tests := []struct {
		name   string
		ctxVal Durability // "" 表示不 push ctx
		optVal Durability
		want   Durability
	}{
		{"都空→默认 Exit", "", "", DurabilityExit},
		{"仅 RunOption", "", DurabilitySync, DurabilitySync},
		{"仅 ctx 继承", DurabilityAsync, "", DurabilityAsync},
		{"RunOption 覆盖 ctx", DurabilitySync, DurabilityExit, DurabilityExit},
		{"RunOption 覆盖 ctx 反向", DurabilityExit, DurabilitySync, DurabilitySync},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := bg
			if tt.ctxVal != "" {
				ctx = WithDurabilityCtx(bg, tt.ctxVal)
			}
			if got := resolveDurability(ctx, tt.optVal); got != tt.want {
				t.Errorf("resolveDurability(ctx=%q, opt=%q) = %q, want %q", tt.ctxVal, tt.optVal, got, tt.want)
			}
		})
	}
}

// TestGetDurability_Unset 验未 push 时返空串（caller 据此兜底 Exit）。
func TestGetDurability_Unset(t *testing.T) {
	if got := GetDurability(context.Background()); got != "" {
		t.Errorf("未设 ctx 应返空串，实际 %q", got)
	}
}
