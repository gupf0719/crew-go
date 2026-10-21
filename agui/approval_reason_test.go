package agui_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// twoCallsModel 第一轮同批调两个工具，之后给出最终答复。
type twoCallsModel struct{ n int }

func (m *twoCallsModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.n++
	if m.n == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{"x":1}`}},
			{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "notify", Arguments: `{"x":2}`}},
		}), nil
	}
	return schema.AssistantMessage("done"), nil
}

func (m *twoCallsModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *twoCallsModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 同一批两个工具都要审批时，两条审批合成一个中断：传给前端的每条 interrupt 都应是一次工具审批——reason 用 AG-UI 的
// 核心值 tool_call、带着各自的 toolCallId 与答复 schema，不是那句合并说明（「聚合 2 个 InterruptError …」）。通用前端
// 按 reason 区分审批与普通提问，曾经发的 tool_approval 既不是核心值也没加前缀，通用前端认不出这是审批。
func TestHandler_MergedApprovals_EachKeepsItsReason(t *testing.T) {
	ok := func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("refund", "退款", map[string]any{"type": "object"}, ok))
		tk.Register(tool.NewFuncTool("notify", "通知", map[string]any{"type": "object"}, ok))
		return agent_v2.NewBasicAgent(agent_v2.WithName("agui_approval"), agent_v2.WithModel(&twoCallsModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	events := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"退款并通知"}]}`).Body.String())
	var its []map[string]any
	for _, ev := range events {
		if ev["type"] != "RUN_FINISHED" {
			continue
		}
		outcome, _ := ev["outcome"].(map[string]any)
		raw, _ := outcome["interrupts"].([]any)
		for _, it := range raw {
			m, _ := it.(map[string]any)
			its = append(its, m)
		}
	}
	if len(its) != 2 {
		t.Fatalf("应有两条审批中断，实际 %d 条（事件 %v）", len(its), eventTypes(events))
	}
	for i, wantCall := range []string{"c1", "c2"} {
		m := its[i]
		if m["reason"] != "tool_call" || m["toolCallId"] != wantCall {
			t.Errorf("第 %d 条应是 reason=tool_call、toolCallId=%s，实际 reason=%v toolCallId=%v", i+1, wantCall, m["reason"], m["toolCallId"])
		}
		schema, _ := m["responseSchema"].(map[string]any)
		props, _ := schema["properties"].(map[string]any)
		if _, ok := props["approved"]; !ok {
			t.Errorf("第 %d 条应带答复 schema（approved: boolean），实际 %v", i+1, m["responseSchema"])
		}
		meta, _ := m["metadata"].(map[string]any)
		if opts, _ := meta["options"].([]any); len(opts) != 4 {
			t.Errorf("第 %d 条的 metadata 应带四个选项，实际 %v", i+1, m["metadata"])
		}
	}
}
