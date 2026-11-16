package agui_test

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

type withdrawToolResultProducer struct{ hook.NoOpHooks }

func (withdrawToolResultProducer) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	want := []string{"discard", "keep"}
	var got []string
	for _, msg := range in.Messages {
		if msg.Role != schema.RoleTool {
			continue
		}
		if id := schema.GetToolResultID(msg); id != "reused" {
			return ctx, fmt.Errorf("unexpected provider call ID %q", id)
		}
		got = append(got, msg.Content)
	}
	if info.Round < 0 || int(info.Round) > len(want) || !slices.Equal(got, want[:info.Round]) {
		return ctx, fmt.Errorf("model round %d received tool results %v", info.Round, got)
	}
	return ctx, nil
}

func (withdrawToolResultProducer) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	modelRunID, _ := out.Extra[agent_v2.ToolModelRunIDExtraKey].(string)
	sink := compose_v2.GetEventSink(ctx)
	if modelRunID == "" || sink == nil {
		return ctx, fmt.Errorf("missing model run identity or event sink")
	}
	event := compose_v2.Event{Type: "reset_stream", Content: "review", Metadata: map[string]any{"run_id": modelRunID}}
	if out.Result.Output == "keep" {
		event.Metadata["replacement"] = "reviewed text"
	}
	// AG-UI 的结束钩子已经登记展示，节点提交前再同步撤回或改写模型答复。
	sink.Send(event)
	return ctx, nil
}

func TestToolResult_CommitAfterModelWithdrawal(t *testing.T) {
	call := func(label string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "reused", Type: "function",
			Function: schema.FunctionCall{Name: "lookup", Arguments: fmt.Sprintf(`{"label":%q}`, label)}}})
	}
	model := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0:
			send(call("discard"))
		case 1:
			send(call("keep"))
		case 2:
			send(text("finished"))
		default:
			return fmt.Errorf("unexpected model turn %d", n)
		}
		return nil
	}}
	var executions atomic.Int64
	h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"}, func(_ context.Context, args map[string]any) (*tool.Result, error) {
			executions.Add(1)
			return tool.Success(args["label"].(string)), nil
		}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("review_outputs"), agent_v2.WithModel(model),
			agent_v2.WithToolkit(tk), agent_v2.WithHooks(hook.NewChainHooks(ui, withdrawToolResultProducer{})))
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	events := parseSSE(rec.Body.String())
	var calls, results []string
	var snapshots, resets int
	for _, ev := range events {
		switch {
		case ev["type"] == "TOOL_CALL_START":
			calls = append(calls, ev["toolCallId"].(string))
		case ev["type"] == "TOOL_CALL_RESULT":
			results = append(results, ev["toolCallId"].(string))
		case ev["type"] == "CUSTOM" && ev["name"] == agui.CustomEventResetStream:
			resets++
		case ev["type"] == "MESSAGES_SNAPSHOT":
			snapshots++
		}
	}
	if outcomeType(events) != "success" || executions.Load() != 2 || resets != 2 || snapshots == 0 || len(calls) != 2 || calls[0] == calls[1] {
		t.Fatalf("应执行两个不同的工具调用，完成撤回与改写：执行 %d 次，调用 %v，事件 %v", executions.Load(), calls, eventTypes(events))
	}
	if !slices.Equal(results, calls[1:]) {
		t.Errorf("节点提交只应发送仍可见的调用结果，实际 %v，调用 %v", results, calls)
	}
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, events)
	var visibleCalls, visibleResults []string
	for _, message := range got {
		visibleCalls = append(visibleCalls, message.toolCalls...)
		if message.role == "tool" {
			visibleResults = append(visibleResults, message.toolResultFor)
			if message.content != "keep" {
				t.Errorf("消费后仍有被撤回的结果：%+v", message)
			}
		}
	}
	if !slices.Equal(visibleCalls, calls[1:]) || !slices.Equal(visibleCalls, visibleResults) || got[len(got)-1].content != "finished" {
		t.Fatalf("前端应只保留有效调用、对应结果和最终答复，实际 %+v", got)
	}
}
