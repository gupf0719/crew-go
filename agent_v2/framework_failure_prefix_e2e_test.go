package agent_v2_test

// **端到端**双前缀哨兵：断言三条框架合成失败路径喂回 LLM 的 content 里，
// `[code]` 前缀恰好出现一次。
//
// 为什么必须端到端：`failed_tool_result_internal_test.go` 那条只测 `failedToolResult`
// 函数本身、传的是**裸 message**——也就是把要验的那件事当成了前提。而 bug 在**调用点**
// （合成 message 时自己又加了一次前缀）。独立审计实测：把调用点改回旧形态，那条哨兵
// 与全仓 440+ 测试**全绿**，而 LLM 收到的仍是 `[invalid_arguments] [invalid_arguments] ...`。
// 这正是 CLAUDE.md 那条「断言选在不承重的维度上，等于没测」。
//
// 断言的量选在**模型真正看到的那个字符串**上——bug 发生时它一定会变。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// deferredAll 让所有业务工具都是 deferred（未解锁）——框架工具恒豁免。
func deferredAll(tool.Tool) bool { return true }

// runFrameworkFailure 让 LLM 调一次会被框架判失败的工具，返回喂回模型的 tool_result content。
func runFrameworkFailure(t *testing.T, tk *tool.Toolkit, tc schema.ToolCall, opts ...agent_v2.Option) string {
	t.Helper()
	stub := &capturingStub{responses: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
		schema.AssistantMessage("done"),
	}}
	base := []agent_v2.Option{
		agent_v2.WithName("ffp"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
	}
	ag := agent_v2.NewBasicAgent(append(base, opts...)...)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return stub.lastToolContent(t)
}

func assertSinglePrefix(t *testing.T, content, code string) {
	t.Helper()
	want := "[" + code + "]"
	if n := strings.Count(content, want); n != 1 {
		t.Errorf("喂回 LLM 的 content 里 %q 出现 %d 次，该恰好 1 次\n  content = %q", want, n, content)
	}
	if !strings.HasPrefix(content, want+" ") {
		t.Errorf("content 该以 %q 开头, got %q", want+" ", content)
	}
}

// 路径 1：参数非法（LLM 吐了坏 JSON）
func TestFrameworkFailurePrefix_InvalidArguments(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("f", "x", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success("never reached"), nil
		}))
	tc := schema.ToolCall{ID: "tc1", Function: schema.FunctionCall{Name: "f", Arguments: `{bad json`}}
	assertSinglePrefix(t, runFrameworkFailure(t, tk, tc), "invalid_arguments")
}

// 路径 2：工具超时（框架自己设的 deadline 到期）
func TestFrameworkFailurePrefix_ToolTimeout(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("slow", "x", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			<-ctx.Done() // 响应 ctx：返 DeadlineExceeded，由框架统一改写
			return nil, ctx.Err()
		}))
	tc := schema.ToolCall{ID: "tc2", Function: schema.FunctionCall{Name: "slow", Arguments: `{}`}}
	assertSinglePrefix(t, runFrameworkFailure(t, tk, tc, agent_v2.WithToolTimeout(30_000_000)), "tool_timeout")
}

// 路径 3：未解锁的 deferred 工具被硬调（fail-closed 拒执行）。
//
// ⚠️ 这条是补上来的：它正是双前缀那个 bug 修的**两个调用点之一**，而最初的端到端
// 哨兵只写了另外两条——「修了 A 和 B，只给 A、C 配哨兵」是独立审计点出来的缺口。
func TestFrameworkFailurePrefix_ToolNotRevealed(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("secret", "x", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success("never reached"), nil
		}))
	tc := schema.ToolCall{ID: "tc3", Function: schema.FunctionCall{Name: "secret", Arguments: `{}`}}
	content := runFrameworkFailure(t, tk, tc,
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: deferredAll}))
	assertSinglePrefix(t, content, "tool_not_revealed")
}
