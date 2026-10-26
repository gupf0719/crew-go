package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestToolOutput_StructuredRenderedAsJSON：工具返回 map 时，模型看到的是 JSON，不是 Go 的 %v 写法
// （`map[items:[a] total:1]`）——MCP 工具的 structuredContent 就是这样交过来的。
func TestToolOutput_StructuredRenderedAsJSON(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("list", "列出", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success(map[string]any{"items": []any{"a"}, "total": 1}), nil
		}))
	m := &fixedCallModel{name: "list", args: "{}"}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("列")); err != nil {
		t.Fatal(err)
	}
	if got, want := m.lastToolResult(), `{"items":["a"],"total":1}`; got != want {
		t.Fatalf("模型看到的工具结果 = %q，期望 %q", got, want)
	}
}
