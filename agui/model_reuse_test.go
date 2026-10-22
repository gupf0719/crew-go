package agui_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// askOnModelEndHook 在 OnModelEnd 里把答复拿给人批准。
type askOnModelEndHook struct{ hook.NoOpHooks }

func (askOnModelEndHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准这条答复吗：" + out.Message.Content})
	return ctx, err
}

// TestHandler_ReusedModelResponse_NotResent：业务在 OnModelEnd 里问人批准答复，第一次 run 把流式答复交给前端之后中断；
// 续跑沿用那次的答复、不再调模型（见 hook.ModelResponseReusedKey）——适配层不该把同一条答复再发一遍（同一个 messageId
// 的第二条）。
func TestHandler_ReusedModelResponse_NotResent(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	stub := &streamingModel{chunks: []string{"答复", "正文"}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("agui_reuse"),
			agent_v2.WithModel(stub),
			agent_v2.WithHooks(hook.NewChainHooks(hooks, askOnModelEndHook{})),
			agent_v2.WithMaxRounds(2),
		)
	}, agui.WithCheckpointer(cp))

	events1 := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"问"}]}`).Body.String())
	if ot := outcomeType(events1); ot != "interrupt" {
		t.Fatalf("fixture 没搭对：第一次 run 应在 OnModelEnd 里中断，outcome=%q（%v）", ot, eventTypes(events1))
	}
	var msgID, content string
	for _, ev := range events1 {
		switch ev["type"] {
		case "TEXT_MESSAGE_START":
			msgID, _ = ev["messageId"].(string)
		case "TEXT_MESSAGE_CONTENT":
			d, _ := ev["delta"].(string)
			content += d
		}
	}
	if msgID == "" || content != "答复正文" {
		t.Fatalf("fixture 没搭对：第一次 run 应把答复流给前端，messageId=%q 内容=%q", msgID, content)
	}

	id := findInterruptID(events1)
	events2 := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r2","resume":[{"interruptId":"`+id+`","status":"resolved","payload":"批准"}]}`).Body.String())
	if ot := outcomeType(events2); ot != "success" {
		t.Fatalf("续跑应完成，outcome=%q（%v）", ot, eventTypes(events2))
	}
	for _, ev := range events2 {
		typ, _ := ev["type"].(string)
		if mid, _ := ev["messageId"].(string); mid == msgID && (typ == "TEXT_MESSAGE_START" || typ == "TEXT_MESSAGE_CONTENT") {
			t.Fatalf("续跑把上一次 run 已经交给前端的答复又发了一遍（%s，messageId=%s）：%v", typ, msgID, eventTypes(events2))
		}
	}
}
