package agui_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

// 工具审批的答复：标准形状（true / false、{"approved": bool}、选项字符串）照答复执行；认不出的报错、这一问
// 保持挂起，一批里有一条认不出就整批不采纳。曾经 payload 先被压成文本交给审批层，认不出的按拒绝处理：
// 前端发 {"approved":true} 或 "Yes."，工具一次都不执行（实测）。

// callToolsOnce 第一轮按 names 同批调这些工具，看到工具结果后给出最终答复。
type callToolsOnce struct{ names []string }

func (m *callToolsOnce) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, ms := range msgs {
		if ms.Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	tcs := make([]schema.ToolCall, 0, len(m.names))
	for i, n := range m.names {
		tcs = append(tcs, schema.ToolCall{ID: fmt.Sprintf("c%d", i+1), Type: "function",
			Function: schema.FunctionCall{Name: n, Arguments: `{}`}})
	}
	return schema.AssistantToolCallMessage(tcs), nil
}

func (m *callToolsOnce) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m *callToolsOnce) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// approvalFixture 搭一个要审批的 agent：names 里每个工具执行一次就记一次数。
type approvalFixture struct {
	h     http.Handler
	calls map[string]*atomic.Int32
}

func newApprovalFixture(names ...string) *approvalFixture {
	f := &approvalFixture{calls: map[string]*atomic.Int32{}}
	for _, n := range names {
		f.calls[n] = &atomic.Int32{}
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	f.h = ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		for _, n := range names {
			c := f.calls[n]
			tk.Register(tool.NewFuncTool(n, n, map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					c.Add(1)
					return tool.Success("ok"), nil
				}))
		}
		return agent_v2.NewBasicAgent(agent_v2.WithName("agui_approval_answers"), agent_v2.WithModel(&callToolsOnce{names: names}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
	}, agui.WithCheckpointer(cp))
	return f
}

// start 发起一次 run，返回各审批中断的 id（按出现顺序）。
func (f *approvalFixture) start(t *testing.T, thread string) []string {
	t.Helper()
	events := parseSSE(postAGUI(f.h, `{"threadId":"`+thread+`","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String())
	var ids []string
	for _, ev := range events {
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
	if len(ids) == 0 {
		t.Fatalf("前置：应停在审批上（事件 %v）", eventTypes(events))
	}
	return ids
}

// resume 按 interruptId → payload 续跑，返回事件。
func (f *approvalFixture) resume(t *testing.T, thread, runID string, payloads map[string]any) []map[string]any {
	t.Helper()
	entries := make([]map[string]any, 0, len(payloads))
	for id, p := range payloads {
		entries = append(entries, map[string]any{"interruptId": id, "status": "resolved", "payload": p})
	}
	body, err := json.Marshal(map[string]any{"threadId": thread, "runId": runID, "resume": entries})
	if err != nil {
		t.Fatal(err)
	}
	return parseSSE(postAGUI(f.h, string(body)).Body.String())
}

func TestHandler_ApprovalAnswers_StandardShapes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload any
		runs    int32
	}{
		{"bool true", true, 1},
		{"approved true", map[string]any{"approved": true}, 1},
		{"选项字符串", "approve", 1},
		{"选项字符串带空白与大写", "  Approve ", 1},
		{"bool false", false, 0},
		{"approved false", map[string]any{"approved": false}, 0},
		{"deny", "deny", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newApprovalFixture("refund")
			ids := f.start(t, "t-"+tc.name)
			events := f.resume(t, "t-"+tc.name, "r2", map[string]any{ids[0]: tc.payload})
			if code := findErrorCode(events); code != "" {
				t.Fatalf("标准形状的答复不该报错，实际 code=%q", code)
			}
			if got := f.calls["refund"].Load(); got != tc.runs {
				t.Fatalf("答复 %v 之后工具应执行 %d 次，实际 %d 次", tc.payload, tc.runs, got)
			}
		})
	}
}

// 认不出的答复（自然语言、对象里没有 approved、数字）报 invalid_resume_payload，这一问保持挂起，重答之后照常执行。
func TestHandler_ApprovalAnswers_UnrecognizedKeepsPending(t *testing.T) {
	for _, bad := range []any{"Yes.", "好的", map[string]any{"ok": true}, 1.0} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			f := newApprovalFixture("refund")
			const thread = "t-bad"
			ids := f.start(t, thread)
			events := f.resume(t, thread, "r2", map[string]any{ids[0]: bad})
			if code := findErrorCode(events); code != "invalid_resume_payload" {
				t.Fatalf("认不出的答复应报 invalid_resume_payload，实际 %q", code)
			}
			if got := f.calls["refund"].Load(); got != 0 {
				t.Fatalf("认不出的答复不该执行工具，实际 %d 次", got)
			}
			events = f.resume(t, thread, "r3", map[string]any{ids[0]: map[string]any{"approved": true}})
			if code := findErrorCode(events); code != "" {
				t.Fatalf("重答应能续上，实际 code=%q", code)
			}
			if got := f.calls["refund"].Load(); got != 1 {
				t.Fatalf("重答批准之后工具应执行 1 次，实际 %d 次", got)
			}
		})
	}
}

// 一批两条审批，一条认得、一条认不出：整批不采纳（两个工具都不执行），都答对之后都执行。
func TestHandler_ApprovalAnswers_BatchRejectedAsAWhole(t *testing.T) {
	f := newApprovalFixture("refund", "notify")
	const thread = "t-batch"
	ids := f.start(t, thread)
	if len(ids) != 2 {
		t.Fatalf("前置：应有两条审批，实际 %d", len(ids))
	}
	events := f.resume(t, thread, "r2", map[string]any{ids[0]: true, ids[1]: "Yes."})
	if code := findErrorCode(events); code != "invalid_resume_payload" {
		t.Fatalf("一条认不出时整批应报 invalid_resume_payload，实际 %q", code)
	}
	if a, b := f.calls["refund"].Load(), f.calls["notify"].Load(); a != 0 || b != 0 {
		t.Fatalf("整批不采纳：两个工具都不该执行，实际 refund=%d notify=%d", a, b)
	}
	events = f.resume(t, thread, "r3", map[string]any{ids[0]: true, ids[1]: map[string]any{"approved": true}})
	if code := findErrorCode(events); code != "" {
		t.Fatalf("都答对之后应能续上，实际 code=%q", code)
	}
	if a, b := f.calls["refund"].Load(), f.calls["notify"].Load(); a != 1 || b != 1 {
		t.Fatalf("都批准之后两个工具各执行 1 次，实际 refund=%d notify=%d", a, b)
	}
}
