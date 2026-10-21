package tool_test

import (
	"context"
	"slices"
	"testing"

	"crew-go/tool"
)

// TestToolkit_ListKeepsRegistrationOrder：List / ToolInfos 按注册顺序列出，每次都一样；同名重新注册替换实例、
// 保持原位。曾经遍历 map：每次给模型的工具顺序随机，provider 按前缀命中的 prompt cache 每一轮都命不中。
func TestToolkit_ListKeepsRegistrationOrder(t *testing.T) {
	noop := func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }
	names := []string{"m", "c", "x", "a", "q", "b", "z", "d", "k", "e", "y", "f"}
	tk := tool.NewToolkit()
	for _, n := range names {
		tk.Register(tool.NewFuncTool(n, n, map[string]any{"type": "object"}, noop))
	}
	tk.Register(tool.NewFuncTool("x", "x v2", map[string]any{"type": "object"}, noop))

	for i := 0; i < 5; i++ {
		infos := tk.ToolInfos()
		got := make([]string, 0, len(infos))
		for _, info := range infos {
			got = append(got, info.Name)
			if info.Name == "x" && info.Desc != "x v2" {
				t.Fatalf("同名重新注册应替换实例，x 的描述是 %q", info.Desc)
			}
		}
		if !slices.Equal(got, names) {
			t.Fatalf("第 %d 次列出的顺序跟注册顺序不一样：%v", i+1, got)
		}
	}
}
