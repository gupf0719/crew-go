package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// returnDirectModel 记录 Generate 调用次数——return-direct 验证关键：工具声明
// return-direct 后 BasicAgent 应短路终止 ReAct，model 不该被调第二轮。
type returnDirectModel struct {
	calls    atomic.Int32
	firstMsg *schema.Message // 第 1 轮输出（tool_call）
	laterMsg *schema.Message // 第 2 轮+输出（return-direct 生效时不该到达）
}

func (m *returnDirectModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if m.calls.Add(1) == 1 {
		return m.firstMsg, nil
	}
	return m.laterMsg, nil
}

func (m *returnDirectModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *returnDirectModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*returnDirectModel)(nil)

// TestBasicAgent_ReturnDirect 验证工具返 tool.Result.Metadata[ReturnDirectMetaKey]=true
// 后，BasicAgent 执行完该工具立即短路终止 ReAct、不回 model 继续生成（HandoffAgent
// transfer 工具"一调即出"的前置能力，对齐 eino returnDirectly）。
//
// 核心断言：model 只被调 1 次（出 tool_call 那轮）。sanity-revert（还原 tools 边为
// 无条件 AddEdge("tools","model")）时 model 会被调第 2 轮 → calls==2 → 本测试 fail
// 并精确指向"return-direct 短路未生效"。
func TestBasicAgent_ReturnDirect(t *testing.T) {
	// return-direct 工具：返回 Metadata[ReturnDirectMetaKey]=true
	transferTool := tool.NewFuncTool(
		"transfer_like",
		"短路 ReAct 的 transfer 类工具",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return &tool.Result{
				Success:  true,
				Output:   "transferred to billing",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true},
			}, nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(transferTool)

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "transfer_like", Arguments: `{}`}}
	m := &returnDirectModel{
		firstMsg: schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
		laterMsg: schema.AssistantMessage("SHOULD_NOT_REACH: LLM 在 return-direct 工具后被错误继续调用"),
	}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_rd"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("帮我转账单"))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	// 核心断言：return-direct 生效 → model 只调 1 次（tool_call 那轮），不回 model 第 2 轮。
	if got := m.calls.Load(); got != 1 {
		t.Errorf("return-direct 失效：model 被调 %d 次，期望 1 次（return-direct 工具调用后应短路不回 model）", got)
	}

	// 旁证：第 2 轮 model 输出（SHOULD_NOT_REACH）不该出现在 final message。
	final := agent_v2.FinalMessage(events)
	if final != nil && strings.Contains(final.Content, "SHOULD_NOT_REACH") {
		t.Errorf("return-direct 失效：LLM 第 2 轮输出泄漏到 final message")
	}
	// ⚠️ 上面那条是**否定式**断言（「不该出现 X」）。只有它的话，一个「最终答复整个没了」
	// 的退化照样全绿——本仓规约：每组断言必须至少有一条是「它必须产出 X」。
	//
	// 钉住的契约：**业务 return-direct 工具的产出就是本次的最终答复**
	//（`tool.ReturnDirectMetaKey` 的 godoc 承诺的正是这个）。
	//
	// ⚠️ 框架工具（`transfer_to_agent` / `route_to`）**不适用**——它们是控制动作，
	// 后面接手的 agent 才是答复来源，那条路仍是「带 tool_call 的 assistant」。
	// 判据是「短路之后还有没有别人来说话」，不是位置。
	if final == nil {
		t.Fatal("return-direct 之后没有最终答复——commitFinal 静默早退了")
	}
	if final.Content != "transferred to billing" {
		t.Errorf("业务 return-direct 工具的产出该成为最终答复，实际 content=%q tool_calls=%v"+
			"——空的话业务收到一个无错无告警的空答复", final.Content, schema.GetToolCalls(final))
	}

}

// TestBasicAgent_NoReturnDirect_NormalReAct 反向验证：普通工具（不设 ReturnDirectMetaKey）
// 行为完全不变——tool 调用后正常回 model 继续 ReAct（零副作用保证：新增 return-direct
// 分支不影响现有 ReAct 循环）。
func TestBasicAgent_NoReturnDirect_NormalReAct(t *testing.T) {
	// 普通工具：不设 Metadata return-direct
	normalTool := tool.NewFuncTool(
		"normal",
		"普通工具",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("tool done"), nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(normalTool)

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "normal", Arguments: `{}`}}
	m := &returnDirectModel{
		firstMsg: schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
		laterMsg: schema.AssistantMessage("final answer after tool"),
	}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_normal"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	// 普通工具：tool 调用后必须回 model（第 2 轮出 final）→ model 调 2 次。
	if got := m.calls.Load(); got != 2 {
		t.Errorf("普通 ReAct 行为被破坏：model 被调 %d 次，期望 2 次（tool 后回 model 出 final）", got)
	}
}
