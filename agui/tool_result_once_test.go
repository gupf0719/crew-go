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
	"crew-go/tool"
)

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
