package tool_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/tool"
)

// TestAudit_TypedNil_Tool 验 audit²-²⁷ tool 包加 isToolNil 防护 sanity-revert.
//
// 历史: audit⁷ HIGH-4 (Toolkit.Register) + audit¹⁷ HIGH-1 (NewFuncTool).
//
// **sanity-revert**: 删 tool/tool.go::Register isToolNil 或 NewFuncTool nil 检 → 测试 fail.
func TestAudit_TypedNil_Tool(t *testing.T) {
	type myTool struct{ tool.Tool }
	var typedNilTool *myTool

	// Toolkit.Register typed nil tool — audit⁷ HIGH-4
	t.Run("Toolkit.Register typed nil tool", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("audit⁷ HIGH-4: Toolkit.Register typed nil tool 应 panic, got no panic\n" +
					"sanity-revert: 删 tool/tool.go Register isToolNil check → 测试 fail")
				return
			}
			msg := ""
			switch v := r.(type) {
			case string:
				msg = v
			case error:
				msg = v.Error()
			}
			if !strings.Contains(msg, "nil tool") {
				t.Errorf("panic msg 应含 'nil tool', got %q", msg)
			}
		}()
		tk := tool.NewToolkit()
		tk.Register(typedNilTool)
	})

	// NewFuncTool nil fn — audit¹⁷ HIGH-1
	t.Run("NewFuncTool nil fn", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("audit¹⁷ HIGH-1: NewFuncTool nil fn 应 panic, got no panic\n" +
					"sanity-revert: 删 tool/tool.go NewFuncTool nil fn check → 测试 fail")
				return
			}
			msg := ""
			switch v := r.(type) {
			case string:
				msg = v
			case error:
				msg = v.Error()
			}
			if !strings.Contains(msg, "nil fn") {
				t.Errorf("panic msg 应含 'nil fn', got %q", msg)
			}
		}()
		_ = tool.NewFuncTool(
			"x", "desc",
			map[string]any{"type": "object"},
			(func(ctx context.Context, args map[string]any) (*tool.Result, error))(nil),
		)
	})
}
