package agui_test

// 请求原文里的工具调用装在一条 id 不等于 toolCallId 的 assistant 消息下（外来历史带 parentMessageId），provider 这次又复用了
// 同一个 tool_call ID：官方客户端把这次的调用并进它手上那条，快照里同一个工具调用只能出现一份（曾经另记一条，两份，⑩）。

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
	for _, ev := range events {
		if ev["type"] != "MESSAGES_SNAPSHOT" {
			continue
		}
		snapshots++
		held := 0
		msgs, _ := ev["messages"].([]any)
		for _, raw := range msgs {
			msg, _ := raw.(map[string]any)
			calls, _ := msg["toolCalls"].([]any)
			for _, c := range calls {
				if cm, _ := c.(map[string]any); cm["id"] == "call_1" {
					held++
				}
			}
		}
		if held != 1 {
			t.Fatalf("快照里工具调用 call_1 应只出现一份（并进原文的 M），实际 %d 份：%v", held, msgs)
		}
	}
	if snapshots == 0 {
		t.Fatal("fixture 没搭对：流中途失败应发快照")
	}
}
