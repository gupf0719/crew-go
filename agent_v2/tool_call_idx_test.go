package agent_v2_test

// ISSUE-96 步骤3 验收：**tc 下标是独立维度**。
//
// 同一轮 LLM 发 N 个 tool_call → 它们跑在**同一个 tools 节点实例**里（节点内 fanout
// ≠ 图级 fanout）→ 图级实例身份（InstanceID）对它们**全相同**；而每个 tc 有独立的
// interrupt counter（Idx 各自从 0 起）→ 只剩 ToolCallIdx 能区分它们。
//
// 拆维度前 tc 下标借住在 nodeBranchIdxKey 上；若"删掉不稳的 branchIdx"时把它一起
// 删掉 → N 个自算 ID 的工具算出**同一个 ID** → MergeInterrupts 后 N 条 query 同 ID
// → WithResumeAnswers 只配得上一条 → 静默串台。
//
// sanity-revert：删 `WithCurrentToolCallContext` 里的 `toolCallIdxKey` 注入 → 本测试 fail。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// selfIDTool 模拟**业务自算稳定 ID** 的工具（走 CurrentInterruptSite，非 auto-fill）
type selfIDTool struct{ name string }

func (t *selfIDTool) Name() string                { return t.name }
func (t *selfIDTool) Description() string         { return "self-id HITL tool" }
func (t *selfIDTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (t *selfIDTool) Call(ctx context.Context, _ map[string]any) (*tool.Result, error) {
	id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
	if ans, ok := compose_v2.GetResumeAnswerText(ctx, id); ok {
		return tool.Success(t.name + " got: " + ans), nil
	}
	return nil, &compose_v2.InterruptError{
		Reason: t.name,
		Query:  &compose_v2.AskQuery{InterruptID: id, Question: t.name + " asks?"},
	}
}

// TestISSUE96_ToolCallIdxIsIndependentDimension：同轮 3 个并发 tool_call 各自
// **自算** ID，必须全 distinct。
func TestISSUE96_ToolCallIdxIsIndependentDimension(t *testing.T) {
	tk := tool.NewToolkit()
	for _, n := range []string{"ask_a", "ask_b", "ask_c"} {
		tk.Register(&selfIDTool{name: n})
	}
	tcs := []schema.ToolCall{
		{ID: "tc_a", Function: schema.FunctionCall{Name: "ask_a", Arguments: `{}`}},
		{ID: "tc_b", Function: schema.FunctionCall{Name: "ask_b", Arguments: `{}`}},
		{ID: "tc_c", Function: schema.FunctionCall{Name: "ask_c", Arguments: `{}`}},
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tcidx"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage(tcs),
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(tk),
	)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断（3 个工具都抛 ask）, got %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 3 {
		t.Fatalf("应收集 3 条 query, got %d", len(qs))
	}
	seen := map[string]string{}
	for _, q := range qs {
		if prev, dup := seen[q.InterruptID]; dup {
			t.Errorf("**tc 下标维度丢失**：%q 与 %q 算出同一个 InterruptID=%s\n"+
				"  → MergeInterrupts 后 N 条 query 同 ID → WithResumeAnswers 只配得上一条 → 静默串台\n"+
				"  根因：N 个 tool_call 跑在同一个 tools 节点实例里（InstanceID 全同、各自 Idx 从 0 起），"+
				"只有 ToolCallIdx 能区分它们",
				prev, q.Question, q.InterruptID)
		}
		seen[q.InterruptID] = q.Question
	}
	t.Logf("3 个并发 tool_call 的自算 ID 全 distinct ✓")
}
