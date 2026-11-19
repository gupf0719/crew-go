package agent_v2

import (
	"testing"

	"crew-go/schema"
)

func TestHistoryTurnAtWaitsForHostedInput(t *testing.T) {
	prefix := schema.UserMessage("保留")
	hookInput := schema.UserMessage("内部派生输入")
	hookReply := schema.AssistantMessage("提前写下的回复")
	for _, m := range []*schema.Message{hookInput, hookReply} {
		m.Annotations = map[string]any{HistoryTaskAnnotation: "active", hostedHistoryTaskAnnotation: true}
	}
	records := []*schema.Message{prefix, hookInput, hookReply}
	if _, _, ok := HistoryTurnAt(records, 2); ok {
		t.Fatal("根输入尚未落盘，不能选用钩子内部输入")
	}
	user := schema.UserMessage("宿主输入")
	user.Annotations = map[string]any{
		HistoryTaskAnnotation: "active", hostedHistoryTaskAnnotation: true,
		HistoryInputMessageIDAnnotation: "wire",
	}
	start, input, ok := HistoryTurnAt(append(records, user), 2)
	if !ok || start != 1 || input != user {
		t.Fatalf("应回溯到同任务的根输入，并在任务的第一条记录断开，实际 %d %+v %v", start, input, ok)
	}
}
