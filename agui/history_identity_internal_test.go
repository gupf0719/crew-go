package agui

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

func TestHistoryToolWireAliasAvoidsExistingMessageID(t *testing.T) {
	const instance = "batch/tool/0"
	base := toolResultWireID(instance)
	var input AGUIMessage
	wire, err := json.Marshal(map[string]string{"id": base, "role": "assistant", "content": "existing"})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wire, &input); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	sse, ok := NewSSEWriter(rec)
	if !ok {
		t.Fatal("需要 SSE writer")
	}
	a := newSSEAdapter(sse, "run", []AGUIMessage{input})
	out := &schema.ToolOutput{CallId: "provider", Extra: map[string]any{agent_v2.ToolCallInstanceIDExtraKey: instance}}
	a.emitToolResult(out)
	a.emitToolResult(out)

	msg := schema.ToolResultMessage("provider", "done")
	msg.Annotations = map[string]any{agent_v2.ToolCallInstanceIDExtraKey: instance}
	ref := historyReference(msg)
	var sent []ToolCallResultEvent
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		body, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var event ToolCallResultEvent
		if err := json.Unmarshal([]byte(body), &event); err != nil {
			t.Fatal(err)
		}
		sent = append(sent, event)
	}
	if len(sent) != 1 || sent[0].MessageID == base || sent[0].ToolCallID != instance ||
		!matchesHistoryWireID(ref, sent[0].MessageID) {
		t.Fatalf("线上结果应有唯一的可查别名，实际 %+v，历史 %+v", sent, ref)
	}
}
