package agui_test

// 一个 toolCallId 在一个 thread 里只收到一条 TOOL_CALL_RESULT（前端按 toolCallId 一个调用一条 tool 消息）：一批 tool_call
// 停在审批上时，框架给已经有结局的调用照样触发 OnToolEnd（带 agent_v2.ToolBatchFailedExtraKey），那个结果不发；续跑时这个
// 调用从工具账重放、或者重新执行，那一次才发。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

var resultOnceSignal = compose_v2.NewSignalKind("agui.test.tool_result_once")

type repeatedToolResultModel struct{}

func (repeatedToolResultModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	var results []string
	for _, message := range messages {
		if message.Role == schema.RoleTool {
			if id := schema.GetToolResultID(message); id != "shared" {
				return nil, fmt.Errorf("unexpected provider call ID %q", id)
			}
			results = append(results, message.Content)
		}
	}
	if len(results) == 0 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "shared", Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{"label":"A"}`}},
			{ID: "shared", Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{"label":"B"}`}},
		}), nil
	}
	if len(results) != 2 || results[0] != "checked A" ||
		!strings.HasPrefix(results[1], "["+agent_v2.SupersededDecisionErrCode+"]") || !strings.Contains(results[1], "checked B") {
		return nil, fmt.Errorf("model did not receive the settled tool results: %q", results)
	}
	return schema.AssistantMessage("done"), nil
}

func (m repeatedToolResultModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(out), nil
}

func (m repeatedToolResultModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

type pauseToolEnd struct{ hook.NoOpHooks }

type scopedToolStart struct{ hook.NoOpHooks }

func (scopedToolStart) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	return compose_v2.EnterPhase(ctx, "lookup-observer"), nil
}

func (pauseToolEnd) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out.Result.Output == "B" {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Question: "检查 B？", Reason: "tool_call", ToolCallID: out.CallId,
		}); err != nil {
			return ctx, err
		}
	}
	out.Result.Output = "checked " + out.Result.Output
	return ctx, nil
}

func TestToolResult_OncePerCall_OnToolEndInterrupt(t *testing.T) {
	var executions atomic.Int32
	h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查询", nil,
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				label := args["label"].(string)
				if !compose_v2.EmitSignal(ctx, resultOnceSignal, compose_v2.Signal{Dest: "selection", Value: label}) {
					return nil, fmt.Errorf("could not record tool decision %s", label)
				}
				executions.Add(1)
				return tool.Success(label), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("check_results"), agent_v2.WithModel(repeatedToolResultModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithHooks(hook.NewChainHooks(scopedToolStart{}, ui, pauseToolEnd{})))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	const thread = "result-review"
	first := parseSSE(postAGUI(h, `{"threadId":"result-review","runId":"first","messages":[{"role":"user","content":"run"}]}`).Body.String())
	var starts []string
	for _, ev := range first {
		switch ev["type"] {
		case "TOOL_CALL_START":
			starts = append(starts, ev["toolCallId"].(string))
		case "TOOL_CALL_RESULT":
			t.Errorf("整批钩子挂起前不能发送工具结果，实际 %v", ev)
		}
	}
	queries := interruptsIn(first)
	if executions.Load() != 2 || len(starts) != 2 || starts[0] == starts[1] || len(queries) != 1 || queries[0]["toolCallId"] != starts[1] {
		t.Fatalf("应有两次实际执行，B 的提问对应自己的调用。执行 %d 次，调用 %v，问题 %v", executions.Load(), starts, queries)
	}
	resumed := resumeEntries(t, h, thread, "resume", resolved(queries[0]["id"].(string), "是"))
	var resultIDs, content []string
	for _, ev := range resumed {
		switch ev["type"] {
		case "TOOL_CALL_START":
			t.Errorf("续跑不应重发已展示的调用，实际 %v", ev)
		case "TOOL_CALL_RESULT":
			resultIDs = append(resultIDs, ev["toolCallId"].(string))
			content = append(content, ev["content"].(string))
		case "TEXT_MESSAGE_START":
			if len(resultIDs) != 2 {
				t.Errorf("后续模型的流开始时应已收到两条工具结果，实际 %v", resultIDs)
			}
		}
	}
	if outcomeType(resumed) != "success" || executions.Load() != 2 || len(resultIDs) != 2 || resultIDs[0] != starts[0] || resultIDs[1] != starts[1] ||
		content[0] != "checked A" || content[1] != "tool error: "+agent_v2.SupersededDecisionErrCode {
		t.Fatalf("应复用两条实际调用，各发送一次定稿结果。执行 %d 次，调用 %v，结果 %v，事件 %v",
			executions.Load(), resultIDs, content, eventTypes(resumed))
	}
}

type pauseFinalReply struct{ askOnModelEndHook }

func (h pauseFinalReply) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if len(schema.GetToolCalls(out.Message)) > 0 {
		return ctx, nil
	}
	return h.askOnModelEndHook.OnModelEnd(ctx, info, out)
}

func TestToolResult_OncePerCall_LaterModelInterrupt(t *testing.T) {
	var calls atomic.Int32
	h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查询", nil, func(context.Context, map[string]any) (*tool.Result, error) {
			calls.Add(1)
			return tool.Success("found"), nil
		}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithToolkit(tk),
			agent_v2.WithModel(&callToolsOnce{names: []string{"lookup"}}), agent_v2.WithHooks(hook.NewChainHooks(ui, pauseFinalReply{})))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	first := parseSSE(postAGUI(h, `{"threadId":"result-then-model","runId":"first","messages":[{"role":"user","content":"run"}]}`).Body.String())
	var accepted int
	for _, ev := range first {
		if ev["type"] == "TOOL_CALL_RESULT" && ev["content"] == "found" {
			accepted++
		}
	}
	id := findInterruptID(first)
	if id == "" || accepted != 1 || calls.Load() != 1 {
		t.Fatalf("挂起发生在工具结果已经提交后，实际调用 %d 次，事件 %v", calls.Load(), first)
	}
	resumed := resumeEntries(t, h, "result-then-model", "resume", resolved(id, "批准"))
	if outcomeType(resumed) != "success" || calls.Load() != 1 || contains(eventTypes(resumed), "TOOL_CALL_RESULT") {
		t.Fatalf("续跑应直接完成且不再投递已提交的结果，实际调用 %d 次，事件 %v", calls.Load(), resumed)
	}
}

type displayToolResult struct {
	hook.Hooks
	hide       bool
	ask        bool
	hideReplay bool
}

func (h displayToolResult) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	display := *in
	return h.Hooks.OnToolStart(compose_v2.EnterPhase(ctx, "display"), info, &display)
}

func (h displayToolResult) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	replayed, _ := out.Extra[agent_v2.ToolReplayedExtraKey].(bool)
	if !h.hide && !(replayed && h.hideReplay) {
		// 前端只看脱敏副本。工具结果本身仍按业务契约交给模型。
		display, result := *out, *out.Result
		result.Output = "[redacted]"
		if h.ask && !replayed {
			result.Output = "[provisional]"
		}
		display.Result = &result
		if _, err := h.Hooks.OnToolEnd(ctx, info, &display); err != nil {
			return ctx, err
		}
	}
	if h.ask {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续处理工具批次？"})
		return ctx, err
	}
	return ctx, nil
}

type displayedToolModel struct {
	wantResult string
}

func (m displayedToolModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, message := range messages {
		if message.Role == schema.RoleTool {
			if message.Content != m.wantResult {
				return nil, fmt.Errorf("model received display content: %q", message.Content)
			}
			return schema.AssistantMessage("done"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "lookup", Type: "function",
		Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}}}), nil
}

func (m displayedToolModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(out), nil
}

func (m displayedToolModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func TestToolResult_RespectsDisplayHook(t *testing.T) {
	for _, tc := range []struct {
		name string
		hook displayToolResult
	}{
		{name: "projected"},
		{name: "hidden", hook: displayToolResult{hide: true}},
		{name: "projected_after_resume", hook: displayToolResult{ask: true}},
		{name: "hidden_after_resume", hook: displayToolResult{ask: true, hideReplay: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool("lookup", "查询", nil, func(context.Context, map[string]any) (*tool.Result, error) {
					calls.Add(1)
					return tool.Success("model-only data"), nil
				}))
				hooks := tc.hook
				hooks.Hooks = ui
				return agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithToolkit(tk),
					agent_v2.WithModel(displayedToolModel{wantResult: "model-only data"}), agent_v2.WithHooks(hooks))
			}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
			first := parseSSE(postAGUI(h, `{"threadId":"display-review","runId":"first","messages":[{"role":"user","content":"run"}]}`).Body.String())
			last := first
			events := first
			if tc.hook.ask {
				id := findInterruptID(first)
				if id == "" || contains(eventTypes(first), "TOOL_CALL_RESULT") {
					t.Fatalf("未提交的批次应挂起且不发结果，实际 %v", eventTypes(first))
				}
				last = resumeEntries(t, h, "display-review", "resumed", resolved(id, "继续"))
				events = append(events, last...)
			}
			var start string
			var resultIDs, contents []string
			for _, ev := range events {
				switch ev["type"] {
				case "TOOL_CALL_START":
					if start != "" {
						t.Fatal("同一次工具调用重复展示")
					}
					start = ev["toolCallId"].(string)
				case "TOOL_CALL_RESULT":
					resultIDs = append(resultIDs, ev["toolCallId"].(string))
					contents = append(contents, ev["content"].(string))
				}
			}
			if start == "" || calls.Load() != 1 || outcomeType(last) != "success" {
				t.Fatalf("测试应展示并执行一次工具，调用 %q，实际执行 %d 次，事件 %v", start, calls.Load(), eventTypes(events))
			}
			wantHidden := tc.hook.hide || tc.hook.hideReplay
			if wantHidden && len(contents) != 0 || !wantHidden && (len(contents) != 1 || contents[0] != "[redacted]" || resultIDs[0] != start) {
				t.Fatalf("展示结果应由工具结束钩子决定，调用 %v，内容 %q", resultIDs, contents)
			}
		})
	}
}

type copyToolDisplay struct {
	hook.Hooks
	independent bool
}

func (h copyToolDisplay) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	display := *out
	if h.independent {
		// 本次结果无需脱敏，前端仍接收自己的结果副本。
		result := *out.Result
		display.Result = &result
	}
	return h.Hooks.OnToolEnd(ctx, info, &display)
}

type addModelToolDetail struct {
	hook.NoOpHooks
	detail string
	ask    bool
}

func (h addModelToolDetail) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	out.Result.Output += h.detail
	if h.ask {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "提交工具批次？"})
		return ctx, err
	}
	return ctx, nil
}

func TestToolResult_KeepsIndependentDisplayCopy(t *testing.T) {
	const publicResult = "public lookup result"
	const modelDetail = " contextual detail for the model only"
	for _, tc := range []struct {
		name       string
		copyOutput bool
		copyResult bool
		resume     bool
	}{
		{name: "independent", copyOutput: true, copyResult: true},
		{name: "independent_after_resume", copyOutput: true, copyResult: true, resume: true},
		{name: "shared_result", copyOutput: true},
		{name: "original_output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool("lookup", "查询", nil, func(context.Context, map[string]any) (*tool.Result, error) {
					calls++
					return tool.Success(publicResult), nil
				}))
				display := ui
				if tc.copyOutput {
					display = copyToolDisplay{Hooks: ui, independent: tc.copyResult}
				}
				hooks := hook.NewChainHooks(display, addModelToolDetail{detail: modelDetail, ask: tc.resume})
				return agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithToolkit(tk),
					agent_v2.WithModel(displayedToolModel{wantResult: publicResult + modelDetail}), agent_v2.WithHooks(hooks))
			}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
			first := parseSSE(postAGUI(h, `{"threadId":"display-copy","runId":"first","messages":[{"role":"user","content":"run"}]}`).Body.String())
			last, events := first, first
			if tc.resume {
				id := findInterruptID(first)
				if id == "" || contains(eventTypes(first), "TOOL_CALL_RESULT") {
					t.Fatalf("提交前应挂起且不发工具结果，实际 %v", eventTypes(first))
				}
				last = resumeEntries(t, h, "display-copy", "resumed", resolved(id, "提交"))
				events = append(events, last...)
			}
			var starts, resultIDs, contents []string
			for _, ev := range events {
				switch ev["type"] {
				case "TOOL_CALL_START":
					starts = append(starts, ev["toolCallId"].(string))
				case "TOOL_CALL_RESULT":
					resultIDs = append(resultIDs, ev["toolCallId"].(string))
					contents = append(contents, ev["content"].(string))
				}
			}
			if outcomeType(last) != "success" || calls != 1 || len(starts) != 1 || len(resultIDs) != 1 || resultIDs[0] != starts[0] {
				t.Fatalf("应成功执行一次并按原调用身份发结果，调用 %d 次，开始 %v，结果 %v，事件 %v",
					calls, starts, resultIDs, eventTypes(events))
			}
			want := publicResult
			if !tc.copyResult {
				want += modelDetail
			}
			if contents[0] != want {
				t.Fatalf("前端收到工具结果 %q，期望 %q", contents[0], want)
			}
		})
	}
}

// resultsAcrossApproval 跑一批 [first（只读）, refund（要审批）]：run1 停在审批上，run2 批准续跑。返回每个 toolCallId 在两次
// run 里收到的 TOOL_CALL_RESULT 内容。
func resultsAcrossApproval(t *testing.T, thread string, first tool.Tool) map[string][]string {
	t.Helper()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(first)
		tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("refunded"), nil }))
		return agent_v2.NewBasicAgent(agent_v2.WithName("once"), agent_v2.WithModel(&callToolsOnce{names: []string{first.Name(), "refund"}}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	run1 := parseSSE(postAGUI(h, `{"threadId":"`+thread+`","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String())
	var ids []string
	for _, ev := range run1 {
		if ev["type"] != "RUN_FINISHED" {
			continue
		}
		outcome, _ := ev["outcome"].(map[string]any)
		its, _ := outcome["interrupts"].([]any)
		for _, it := range its {
			m, _ := it.(map[string]any)
			id, _ := m["id"].(string)
			ids = append(ids, id)
		}
	}
	if len(ids) != 1 {
		t.Fatalf("fixture：run1 应停在一条审批上，实际事件 %v", eventTypes(run1))
	}
	body, err := json.Marshal(map[string]any{"threadId": thread, "runId": "r2",
		"resume": []map[string]any{{"interruptId": ids[0], "status": "resolved", "payload": true}}})
	if err != nil {
		t.Fatal(err)
	}
	run2 := parseSSE(postAGUI(h, string(body)).Body.String())
	results := map[string][]string{}
	for _, ev := range append(run1, run2...) {
		if ev["type"] == "TOOL_CALL_RESULT" {
			tc, _ := ev["toolCallId"].(string)
			c, _ := ev["content"].(string)
			results[tc] = append(results[tc], c)
		}
	}
	return results
}

// 跑完的调用：run1 不发（它在 run1 那批里收尾时这一批没整体成功），续跑时从工具账重放才发——一条。
func TestToolResult_OncePerCall_CompletedSiblingReplayed(t *testing.T) {
	var lookups atomic.Int32
	lookup := tool.NewFuncTool("lookup", "lookup", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookups.Add(1)
			return tool.Success("balance=42"), nil
		}, tool.WithReadOnly())
	results := resultsAcrossApproval(t, "t-once-replay", lookup)
	if lookups.Load() != 1 {
		t.Fatalf("fixture：lookup 应只执行 1 次（续跑从工具账重放），got %d", lookups.Load())
	}
	for tc, cs := range results {
		if len(cs) != 1 {
			t.Errorf("toolCallId %s 收到 %d 条 TOOL_CALL_RESULT：%q", tc, len(cs), cs)
		}
	}
	if len(results) != 2 {
		t.Errorf("两个调用都应各有一条结果，got %q", results)
	}
}

// 硬失败的调用：run1 不发它的失败（续跑会重新执行），续跑重新执行成功后发——一条，内容是成功的那份。
func TestToolResult_OncePerCall_HardFailedSiblingRerun(t *testing.T) {
	var flakies atomic.Int32
	flaky := tool.NewFuncTool("flaky", "flaky", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			if flakies.Add(1) == 1 {
				return nil, fmt.Errorf("upstream 503")
			}
			return tool.Success("fresh-data"), nil
		}, tool.WithReadOnly())
	results := resultsAcrossApproval(t, "t-once-rerun", flaky)
	if flakies.Load() != 2 {
		t.Fatalf("fixture：flaky 应执行 2 次（第一次硬失败、续跑重新执行），got %d", flakies.Load())
	}
	for tc, cs := range results {
		if len(cs) != 1 {
			t.Errorf("toolCallId %s 收到 %d 条 TOOL_CALL_RESULT：%q", tc, len(cs), cs)
		}
		for _, c := range cs {
			if strings.Contains(c, "tool error") {
				t.Errorf("toolCallId %s 收到了续跑会重做的那次失败：%q", tc, cs)
			}
		}
	}
}
