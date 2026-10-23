package agui_test

// AG-UI 1.0 的几条线上约定：开流之前被拒的请求是 HTTP 错误码、没有事件流；RUN_STARTED 声明生产方自己的协议版本，
// 请求声明了不同大版本时开流之前拒绝、同一大版本更新的小版本照常服务；续跑时同一个工具调用的 START / ARGS 只发一次。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

func plainHandler() http.Handler {
	return ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("plain"), agent_v2.WithModel(&callToolsOnce{}),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(2))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
}

func TestHandler_RejectBeforeStream_HTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"bad_json", `{"threadId":`, http.StatusBadRequest, "bad_request"},
		{"resume_without_thread", `{"runId":"r","resume":[{"interruptId":"x","status":"resolved","payload":"y"}]}`, http.StatusBadRequest, "no_thread_id"},
		{"no_user_message", `{"threadId":"t1","runId":"r","messages":[]}`, http.StatusBadRequest, "bad_request"},
		{"no_session", `{"threadId":"t-none","runId":"r","resume":[{"interruptId":"x","status":"resolved","payload":"y"}]}`, http.StatusNotFound, "no_session"},
		{"bad_resume_status", `{"threadId":"t2","runId":"r","resume":[{"interruptId":"x","status":"maybe"}]}`, http.StatusBadRequest, "invalid_resume"},
		{"other_major_version", `{"threadId":"t3","runId":"r","protocolVersion":"2.0","messages":[{"role":"user","content":"hi"}]}`, http.StatusBadRequest, "unsupported_protocol_version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := postAGUI(plainHandler(), tc.body)
			if rec.Code != tc.status {
				t.Errorf("HTTP %d，want %d：%s", rec.Code, tc.status, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); strings.Contains(ct, "event-stream") {
				t.Errorf("开流之前被拒的请求不该开事件流，Content-Type=%q", ct)
			}
			if strings.Contains(rec.Body.String(), "data: ") {
				t.Errorf("不该有任何事件：%s", rec.Body.String())
			}
			var body struct{ Code string }
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Code != tc.code {
				t.Errorf("响应体应是 {\"code\":%q,...}，got %s（%v）", tc.code, rec.Body.String(), err)
			}
		})
	}
}

func TestHandler_RunStartedDeclaresProtocolVersion(t *testing.T) {
	for _, declared := range []string{"", "1.0", "1.5", "garbage"} {
		body := `{"threadId":"tv-` + declared + `","runId":"r","messages":[{"role":"user","content":"hi"}]`
		if declared != "" {
			body += `,"protocolVersion":"` + declared + `"`
		}
		rec := postAGUI(plainHandler(), body+"}")
		if rec.Code != http.StatusOK {
			t.Fatalf("声明 %q 应照常服务，HTTP %d：%s", declared, rec.Code, rec.Body.String())
		}
		evs := parseSSE(rec.Body.String())
		if len(evs) == 0 || evs[0]["type"] != "RUN_STARTED" {
			t.Fatalf("声明 %q：第一个事件应是 RUN_STARTED，got %v", declared, eventTypes(evs))
		}
		if v := evs[0]["protocolVersion"]; v != agui.ProtocolVersion {
			t.Errorf("声明 %q：RUN_STARTED.protocolVersion = %v，want %q（生产方自己的版本，不回显输入）", declared, v, agui.ProtocolVersion)
		}
	}
}

// 一批 [lookup（只读）, refund（要审批）] 停在审批上、批准续跑：两个调用在整个任务里各只有一条 TOOL_CALL_START 与 ARGS。
func TestToolCallStart_OncePerCallAcrossResume(t *testing.T) {
	var lookups atomic.Int32
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "lookup", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				lookups.Add(1)
				return tool.Success("balance=42"), nil
			}, tool.WithReadOnly()))
		tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("refunded"), nil }))
		return agent_v2.NewBasicAgent(agent_v2.WithName("once_start"), agent_v2.WithModel(&callToolsOnce{names: []string{"lookup", "refund"}}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	run1 := parseSSE(postAGUI(h, `{"threadId":"t-start","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String())
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
	body, err := json.Marshal(map[string]any{"threadId": "t-start", "runId": "r2",
		"resume": []map[string]any{{"interruptId": ids[0], "status": "resolved", "payload": true}}})
	if err != nil {
		t.Fatal(err)
	}
	run2 := parseSSE(postAGUI(h, string(body)).Body.String())
	starts, args, results := map[string]int{}, map[string]int{}, map[string]int{}
	for _, ev := range append(run1, run2...) {
		tc, _ := ev["toolCallId"].(string)
		switch ev["type"] {
		case "TOOL_CALL_START":
			starts[tc]++
		case "TOOL_CALL_ARGS":
			args[tc]++
		case "TOOL_CALL_RESULT":
			results[tc]++
		}
	}
	for _, tc := range []string{"c1", "c2"} {
		if starts[tc] != 1 || args[tc] != 1 {
			t.Errorf("toolCallId %s：START %d 次、ARGS %d 次，want 各 1——续跑再发会让前端把参数追加成两份", tc, starts[tc], args[tc])
		}
		if results[tc] != 1 {
			t.Errorf("toolCallId %s：RESULT %d 次，want 1", tc, results[tc])
		}
	}
	if lookups.Load() != 1 {
		t.Errorf("fixture：lookup 应只执行 1 次，got %d", lookups.Load())
	}
}

// reuseIDModel 模拟按批内下标编号的 provider：每一轮都用同一个 toolCallId。
type reuseIDModel struct{}

func (m *reuseIDModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	n := 0
	for _, ms := range msgs {
		if ms.Role == schema.RoleTool {
			n++
		}
	}
	if n >= 2 {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "functions.lookup:0", Type: "function",
		Function: schema.FunctionCall{Name: "lookup", Arguments: fmt.Sprintf(`{"q":"round%d"}`, n+1)}}}), nil
}
func (m *reuseIDModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *reuseIDModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// provider 在不同轮次复用同一个 toolCallId：那是两个调用，各发一组 START/ARGS/END 与 RESULT。跳过只认「同一批的
// 再次派发」（agent_v2.ToolRedispatchedExtraKey），不按 id 记「发过没有」——按 id 记时第二个调用的 START/ARGS 整个
// 丢掉、它的结果挂到第一个头上（审计实测）。
func TestToolCallStart_ReusedIDAcrossRoundsBothSent(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "lookup", map[string]any{"type": "object"},
			func(_ context.Context, a map[string]any) (*tool.Result, error) {
				return tool.Success(fmt.Sprint(a["q"])), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("reuse"), agent_v2.WithModel(&reuseIDModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	evs := parseSSE(postAGUI(h, `{"threadId":"t-reuse","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String())
	var starts int
	var args []string
	for _, ev := range evs {
		switch ev["type"] {
		case "TOOL_CALL_START":
			starts++
		case "TOOL_CALL_ARGS":
			d, _ := ev["delta"].(string)
			args = append(args, d)
		}
	}
	if starts != 2 || len(args) != 2 || args[0] != `{"q":"round1"}` || args[1] != `{"q":"round2"}` {
		t.Errorf("两轮复用同一个 id 的调用应各发一组：START %d 次，ARGS %q", starts, args)
	}
}
