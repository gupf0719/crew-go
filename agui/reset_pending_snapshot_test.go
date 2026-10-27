package agui_test

// 攒着的撤回快照不能已经含有紧接着要发的那个事件：快照之后再按增量发同一条，前端内容就重复了。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type nodeEventer interface {
	OnNodeEvent(ctx context.Context, nodeID string, ev agent_v2.Event) context.Context
}

// scriptedAgent 直接往适配层喂事件。
type scriptedAgent struct {
	h      hook.Hooks
	script func(ctx context.Context, ne nodeEventer)
}

func (s *scriptedAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ne, _ := s.h.(nodeEventer)
	s.script(ctx, ne)
	return []*agent_v2.AgentEvent{agent_v2.NewOutputEvent("probe", schema.AssistantMessage("done"))}, nil
}
func (s *scriptedAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (s *scriptedAgent) Name() string        { return "probe" }
func (s *scriptedAgent) Description() string { return "probe" }

func chunkEv(run, c string) agent_v2.Event {
	return agent_v2.Event{Type: "chunk", Content: c, Metadata: map[string]any{"run_id": run}}
}

// A 流完被撤（攒快照），紧接着 B 的改写到达、B 从没发过文本：前端的 B 是一份 "FINAL"（曾经是 "FINALFINAL"，审计实测）。
func TestPendingSnapshot_VoidThenReplaceUnemitted(t *testing.T) {
	h := ownedHandler(func(hk hook.Hooks) agent_v2.Agent {
		return &scriptedAgent{h: hk, script: func(ctx context.Context, ne nodeEventer) {
			ne.OnNodeEvent(ctx, "n", chunkEv("A", "partial"))
			ne.OnNodeEvent(ctx, "n", agent_v2.Event{Type: "reset_stream", Content: "x", Metadata: map[string]any{"run_id": "A"}})
			ne.OnNodeEvent(ctx, "n", agent_v2.Event{Type: "reset_stream", Content: "rewrite", Metadata: map[string]any{"run_id": "B", "replacement": "FINAL"}})
		}}
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	evs := parseSSE(rec.Body.String())
	assertNoResendAfterSnapshot(t, evs)
	for _, m := range applyAGUI(nil, evs) {
		if m.id == "B" && m.content != "FINAL" {
			t.Fatalf("前端的 B 应是一份 FINAL，实际 %q", m.content)
		}
		if m.id == "A" {
			t.Fatalf("被撤回的 A 不该留在前端")
		}
	}
}

// 并行分支：X 被撤回（攒快照），兄弟分支接着只发工具调用与结果：快照里不能已经有这个工具调用 / 结果（曾经快照里先有、
// 随后又发 START / ARGS，前端的参数拼成两份，审计实测）。
func TestPendingSnapshot_VoidThenSiblingTool(t *testing.T) {
	h := ownedHandler(func(hk hook.Hooks) agent_v2.Agent {
		return &scriptedAgent{h: hk, script: func(ctx context.Context, ne nodeEventer) {
			ne.OnNodeEvent(ctx, "n", chunkEv("X", "xxx"))
			ne.OnNodeEvent(ctx, "n", agent_v2.Event{Type: "reset_stream", Content: "x", Metadata: map[string]any{"run_id": "X"}})
			_, _ = hk.OnToolStart(ctx, nil, &schema.ToolInput{Name: "lookup", CallId: "c1", ArgumentsJSON: `{"q":1}`})
			_, _ = hk.OnToolEnd(ctx, nil, &schema.ToolOutput{CallId: "c1", Result: &schema.ToolCallResult{Success: true, Output: "R"}})
		}}
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	evs := parseSSE(rec.Body.String())
	assertNoResendAfterSnapshot(t, evs)
	snapshots := 0
	for _, e := range evs {
		if e["type"] == "MESSAGES_SNAPSHOT" {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("X 被撤回后应发一份快照，实际 %d 份", snapshots)
	}
}

// assertNoResendAfterSnapshot：一份快照里已经有的消息，之后不能再用 TEXT_MESSAGE_START / TOOL_CALL_START /
// TOOL_CALL_RESULT 重发——消费方按 id 认、START 幂等、内容追加，重发就是重复。
func assertNoResendAfterSnapshot(t *testing.T, evs []map[string]any) {
	t.Helper()
	inSnap := map[string]bool{}
	for _, e := range evs {
		switch e["type"] {
		case "MESSAGES_SNAPSHOT":
			inSnap = map[string]bool{}
			msgs, _ := e["messages"].([]any)
			for _, m := range msgs {
				if mm, ok := m.(map[string]any); ok {
					if id, _ := mm["id"].(string); id != "" {
						inSnap[id] = true
					}
				}
			}
		case "TEXT_MESSAGE_START", "TOOL_CALL_RESULT":
			if id, _ := e["messageId"].(string); inSnap[id] {
				t.Fatalf("快照里已经有 %s，之后又发了 %v：%v", id, e["type"], evs)
			}
		case "TOOL_CALL_START":
			if id, _ := e["toolCallId"].(string); inSnap[id] {
				t.Fatalf("快照里已经有工具调用 %s，之后又发了 TOOL_CALL_START：%v", id, evs)
			}
		}
	}
}
