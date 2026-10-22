package tool_test

import (
	"context"
	"testing"

	"crew-go/tool"
)

type undeclaredTool struct{}

func (undeclaredTool) Name() string                { return "u" }
func (undeclaredTool) Description() string         { return "" }
func (undeclaredTool) InputSchema() map[string]any { return nil }
func (undeclaredTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	return tool.Success(nil), nil
}

// 没声明就按有副作用处理（fail-closed）；FuncTool 经 WithReadOnly 声明。
func TestIsReadOnly(t *testing.T) {
	fn := func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success(nil), nil }
	var typedNil *tool.FuncTool
	for _, tc := range []struct {
		name string
		t    tool.Tool
		want bool
	}{
		{"nil", nil, false},
		{"typed nil", typedNil, false},
		{"没实现接口", undeclaredTool{}, false},
		{"FuncTool 默认", tool.NewFuncTool("f", "", nil, fn), false},
		{"FuncTool WithReadOnly", tool.NewFuncTool("f", "", nil, fn, tool.WithReadOnly()), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tool.IsReadOnly(tc.t); got != tc.want {
				t.Errorf("IsReadOnly = %v, want %v", got, tc.want)
			}
		})
	}
}
