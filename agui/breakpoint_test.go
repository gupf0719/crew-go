package agui_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

func TestHandler_BreakpointResumeUsesNamespaceAndCurrentInterrupt(t *testing.T) {
	var prepared, applied atomic.Int32
	child := compose_v2.NewBuilder().WithName("debug_child").
		WithInterruptBefore("prepare", "apply").WithInterruptAfter("prepare")
	child.AddNode("prepare", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		prepared.Add(1)
		return nil, nil
	})
	child.AddNode("apply", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		applied.Add(1)
		return nil, nil
	})
	child.AddEdge(compose_v2.StartNode, "prepare")
	child.AddEdge("prepare", "apply")
	child.AddEdge("apply", compose_v2.EndNode)
	sub, err := child.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := compose_v2.NewBuilder().WithName("debug_parent")
	for _, ns := range []compose_v2.NodeID{"left", "right"} {
		parent.AddSubgraphNode(ns, sub)
		parent.AddEdge(compose_v2.StartNode, ns)
		parent.AddEdge(ns, compose_v2.EndNode)
	}
	graph, err := parent.Compile()
	if err != nil {
		t.Fatal(err)
	}
	h := ownedHandler(func(hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph,
			agent_v2.WithGraphAgentName("debug_parent"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
				return schema.AssistantMessage("done")
			}))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))

	const thread = "debug"
	first := parseSSE(postAGUI(h, `{"threadId":"debug","runId":"start","messages":[{"role":"user","content":"go"}]}`).Body.String())
	onEntry := aguiBreakpoints(t, first, "start", "prepare", "")
	if prepared.Load() != 0 || applied.Load() != 0 {
		t.Fatal("before 断点处两个子图都应尚未执行")
	}
	continueAt := func(id string) map[string]any {
		return map[string]any{"interruptId": id, "status": "resolved"}
	}
	cancelLeft := map[string]any{"interruptId": onEntry["left"], "status": "cancelled"}
	mixed := resumeEntries(t, h, thread, "mixed", cancelLeft, continueAt(onEntry["right"]))
	if findErrorCode(mixed) != "invalid_resume" || prepared.Load() != 0 {
		t.Fatalf("单独取消一个断点时不能继续整个边界，实际 %+v，执行 %d 次", mixed, prepared.Load())
	}

	second := resumeEntries(t, h, thread, "step", continueAt(onEntry["left"]), continueAt(onEntry["right"]))
	atNext := aguiBreakpoints(t, second, "step", "apply", "prepare")
	if atNext["left"] == onEntry["left"] || atNext["right"] == onEntry["right"] ||
		prepared.Load() != 2 || applied.Load() != 0 {
		t.Fatalf("应执行两个 prepare 并停在新边界，前后 ID %+v %+v，次数 %d/%d",
			onEntry, atNext, prepared.Load(), applied.Load())
	}
	stale := resumeEntries(t, h, thread, "stale", continueAt(onEntry["left"]), continueAt(atNext["right"]))
	if findErrorCode(stale) != "invalid_resume" || applied.Load() != 0 {
		t.Fatalf("旧边界的 ID 不能放行新边界，实际 %+v，执行 %d 次", stale, applied.Load())
	}

	done := resumeEntries(t, h, thread, "done", continueAt(atNext["left"]), continueAt(atNext["right"]))
	if outcome := aguiBreakpointOutcome(t, done, "done"); outcome["type"] != "success" || prepared.Load() != 2 || applied.Load() != 2 {
		t.Fatalf("准确放行后两个子图各执行一次 apply，实际 %+v，次数 %d/%d", outcome, prepared.Load(), applied.Load())
	}
}

func aguiBreakpoints(t *testing.T, events []map[string]any, runID, before, after string) map[string]string {
	t.Helper()
	if outcome := aguiBreakpointOutcome(t, events, runID); outcome["type"] != "interrupt" {
		t.Fatalf("断点对应 interrupt 终态，实际 %+v", outcome)
	}
	ids := make(map[string]string, 2)
	for _, item := range interruptsIn(events) {
		metadata, _ := item["metadata"].(map[string]any)
		details, _ := metadata["breakpoint"].(map[string]any)
		ns, _ := details["ns"].(string)
		id, _ := item["id"].(string)
		var wantAfter any
		if after != "" {
			wantAfter = []any{after}
		}
		if item["reason"] != "crew-go:breakpoint" || item["responseSchema"] != nil || id == "" ||
			!reflect.DeepEqual(details["before"], []any{before}) || !reflect.DeepEqual(details["after"], wantAfter) {
			t.Fatalf("断点投影不匹配，实际 %+v", item)
		}
		ids[ns] = id
	}
	if len(ids) != 2 || ids["left"] == "" || ids["right"] == "" || ids["left"] == ids["right"] {
		t.Fatalf("两个同名节点应按所在子图寻址，实际 %+v", ids)
	}
	return ids
}

func aguiBreakpointOutcome(t *testing.T, events []map[string]any, runID string) map[string]any {
	t.Helper()
	if len(events) > 0 {
		last := events[len(events)-1]
		if last["type"] == "RUN_FINISHED" && last["runId"] == runID && last["threadId"] == "debug" {
			outcome, _ := last["outcome"].(map[string]any)
			return outcome
		}
	}
	t.Fatalf("缺少本次 run 的终态，实际 %+v", events)
	return nil
}
