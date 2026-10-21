package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestBasicAgent_MaxRoundsBound 钉死 BasicAgent 的 ReAct 轮次硬上限 = maxRounds+1 次
// model 调用（basic_builder.go `WithMaxVisit(maxRounds + 1)`）。
//
// **背景**：BasicAgent 无优雅轮次退出（model 条件边只判 tool_calls），maxVisit 是循环
// 唯一硬约束。compose_v2.maxVisit 是 **per-node**——model 每 ReAct 轮访问 1 次，故 model
// 能成功调用 maxVisit 次、第 maxVisit+1 次访问在执行前触 MaxIterationsError（对齐
// LangGraph GraphRecursionError / eino ErrExceedMaxSteps，均硬报错非优雅 final）。
//
// **修复前 bug**：maxVisit=maxRounds*2（把 per-node 上限误当 global 步数预算：2 节点/轮 ×
// maxRounds）→ model 实际能跑 2*maxRounds 次才报错，与文档"最大 maxRounds 轮"差 2 倍。
//
// **sanity-revert**：把 basic_builder.go 的 `maxRounds + 1` 改回 `maxRounds * 2` 后，
// model 被调用 2*maxRounds=6 次而非 maxRounds+1=4 → 下面 callCount 断言 FAIL 精确指向 2x bug。
func TestBasicAgent_MaxRoundsBound(t *testing.T) {
	const maxRounds = 3

	noop := tool.NewFuncTool("noop", "noop",
		map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil })
	tk := tool.NewToolkit()
	tk.Register(noop)

	// model 永远返 tool_call、永不给无 tool 的 final——强制撞 maxVisit 上限。
	m := &perCallModel{respFn: func(int) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"}},
		})
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bound"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(maxRounds),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))

	// 触顶必须是 MaxIterationsError（硬报错语义，非优雅返回）。
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Fatalf("model 永不终止应触 MaxIterationsError，got err=%v", err)
	}

	// **精确**断言：model 恰好被调用 maxRounds+1 次（修复后），非 2*maxRounds（修复前）。
	if got := m.callCount(); got != maxRounds+1 {
		t.Fatalf("ReAct 轮次硬上限应为 maxRounds+1=%d 次 model 调用，got %d "+
			"(=2*maxRounds 说明 WithMaxVisit 被 revert 回 maxRounds*2 的 2x bug)", maxRounds+1, got)
	}
}
