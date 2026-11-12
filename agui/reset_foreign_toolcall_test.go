package agui_test

// 请求原文里的工具调用装在一条带 parentMessageId 的 assistant 消息下。
// Provider 复用原 ID 时，快照保留原有调用和本次新调用各一份。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

func TestResetSnapshot_ForeignHeldToolCallNotDuplicated(t *testing.T) {
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0:
			send(&schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{Id: "call_1", Name: "lookup", Arguments: `{}`}}}})
		case 1:
			send(text("half-")) // 流中途失败：撤回它、发快照
			return errReset
		default:
			send(text("full"))
		}
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("新结果"), nil }))
		return agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	})
	body := `{"threadId":"t1","runId":"r2","messages":[` +
		`{"id":"u0","role":"user","content":"earlier"},` +
		`{"id":"M","role":"assistant","toolCalls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},` +
		`{"id":"r1/call_1","role":"tool","toolCallId":"call_1","content":"旧结果"},` +
		`{"id":"u1","role":"user","content":"go"}]}`
	events := parseSSE(postAGUI(h, body).Body.String())
	snapshots := 0
	var newID string
	for _, ev := range events {
		if ev["type"] == "TOOL_CALL_START" {
			newID, _ = ev["toolCallId"].(string)
		}
		if ev["type"] != "MESSAGES_SNAPSHOT" {
			continue
		}
		snapshots++
		held := map[string]int{}
		msgs, _ := ev["messages"].([]any)
		for _, raw := range msgs {
			msg, _ := raw.(map[string]any)
			calls, _ := msg["toolCalls"].([]any)
			for _, c := range calls {
				cm, _ := c.(map[string]any)
				id, _ := cm["id"].(string)
				held[id]++
			}
		}
		if newID == "" || newID == "call_1" || len(held) != 2 || held["call_1"] != 1 || held[newID] != 1 {
			t.Fatalf("快照应同时保留旧调用 call_1 和新调用 %q，实际 %v", newID, held)
		}
	}
	if snapshots == 0 {
		t.Fatal("fixture 没搭对：流中途失败应发快照")
	}
}
