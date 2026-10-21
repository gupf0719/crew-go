package agui_test

import (
	"context"
	"encoding/json"
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

// 续跑的 resume 列表与答复：恰好覆盖挂起的中断（AG-UI 不支持只答一部分，也不能答不在其中的、不能重复）；同一个任务
// 里采纳过的答复累积起来，每次续跑整份交给 Resume。

// interruptsIn 取事件里 RUN_FINISHED(interrupt) 带的中断。
func interruptsIn(events []map[string]any) []map[string]any {
	var out []map[string]any
	for _, ev := range events {
		if ev["type"] != "RUN_FINISHED" {
			continue
		}
		outcome, _ := ev["outcome"].(map[string]any)
		raw, _ := outcome["interrupts"].([]any)
		for _, it := range raw {
			m, _ := it.(map[string]any)
			out = append(out, m)
		}
	}
	return out
}

// resumeEntries 按给定的 resume 列表原样续跑（可以有重复、未知的 ID）。
func resumeEntries(t *testing.T, h http.Handler, thread, runID string, entries ...map[string]any) []map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"threadId": thread, "runId": runID, "resume": entries})
	if err != nil {
		t.Fatal(err)
	}
	return parseSSE(postAGUI(h, string(body)).Body.String())
}

func resolved(id string, payload any) map[string]any {
	return map[string]any{"interruptId": id, "status": "resolved", "payload": payload}
}

// seqAskAgent 是一个先后问两问的节点；asked 记每一问被问了几次，got 记节点最后拿到的答案。
func seqAskAgent(asked map[string]*atomic.Int32, got *atomic.Value) agent_v2.Agent {
	inCh := compose_v2.LastValue[*schema.Message]("__seq_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__seq_out__")
	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("ask2", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		var answers []string
		for _, q := range []string{"Q-1?", "Q-2?"} {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
			if err != nil {
				asked[q].Add(1)
				return nil, err
			}
			answers = append(answers, ans.Content)
		}
		got.Store(strings.Join(answers, "+"))
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("done"))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "ask2")
	bld.AddEdge("ask2", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		panic(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("seq_asker"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// 一个节点先后问两问、每次答当时挂着的那一问：两轮答完，每一问只问一次。曾经每次只交这一轮的答复，第二轮续跑时
// 第一问没有答案、又被问一遍，Q-1 / Q-2 交替出现、永远答不完（实测）。
func TestHandler_Resume_SequentialAsksConverge(t *testing.T) {
	asked := map[string]*atomic.Int32{"Q-1?": {}, "Q-2?": {}}
	var got atomic.Value
	h := ownedHandler(func(hook.Hooks) agent_v2.Agent { return seqAskAgent(asked, &got) },
		agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	ans := map[string]string{"Q-1?": "one", "Q-2?": "two"}
	const thread = "t-seq"
	events := parseSSE(postAGUI(h, `{"threadId":"t-seq","runId":"r0","messages":[{"role":"user","content":"go"}]}`).Body.String())
	for round := 1; round <= 4; round++ {
		its := interruptsIn(events)
		if len(its) == 0 {
			break
		}
		entries := make([]map[string]any, 0, len(its))
		for _, it := range its {
			id, _ := it["id"].(string)
			msg, _ := it["message"].(string)
			entries = append(entries, resolved(id, ans[msg]))
		}
		events = resumeEntries(t, h, thread, "r"+string(rune('0'+round)), entries...)
	}
	if code := findErrorCode(events); code != "" || len(interruptsIn(events)) != 0 {
		t.Fatalf("应答完收尾，实际 code=%q 还挂着 %d 问", code, len(interruptsIn(events)))
	}
	if v, _ := got.Load().(string); v != "one+two" {
		t.Fatalf("节点应拿到两问的答案 one+two，实际 %q", v)
	}
	if a, b := asked["Q-1?"].Load(), asked["Q-2?"].Load(); a != 1 || b != 1 {
		t.Fatalf("每一问应只问一次，实际 Q-1=%d Q-2=%d（答过的又被问了）", a, b)
	}
}

// 挂着两条审批、只答其一：报 invalid_resume，两个工具都不执行、会话保持挂起；都答了之后各执行一次。
func TestHandler_Resume_MustCoverEveryOpenInterrupt(t *testing.T) {
	f := newApprovalFixture("refund", "notify")
	const thread = "t-cover"
	ids := f.start(t, thread)
	if len(ids) != 2 {
		t.Fatalf("前置：应有两条审批，实际 %d", len(ids))
	}
	events := resumeEntries(t, f.h, thread, "r2", resolved(ids[0], true))
	if code := findErrorCode(events); code != "invalid_resume" {
		t.Fatalf("只答一部分应报 invalid_resume，实际 %q", code)
	}
	if a, b := f.calls["refund"].Load(), f.calls["notify"].Load(); a != 0 || b != 0 {
		t.Fatalf("只答一部分时工具都不该执行，实际 refund=%d notify=%d", a, b)
	}
	events = resumeEntries(t, f.h, thread, "r3", resolved(ids[0], true), resolved(ids[1], true))
	if code := findErrorCode(events); code != "" {
		t.Fatalf("都答了之后应能续上，实际 code=%q", code)
	}
	if a, b := f.calls["refund"].Load(), f.calls["notify"].Load(); a != 1 || b != 1 {
		t.Fatalf("都批准之后两个工具各执行 1 次，实际 refund=%d notify=%d", a, b)
	}
}

// resume 里有不是挂起中断的 ID、或同一个 ID 出现两次（审批的批准与拒绝相反）：报 invalid_resume，工具不执行。曾经
// 未知 ID 照样交给 Resume，同一个 ID 两条时后一条覆盖前一条，批准还是拒绝取决于数组顺序（审计实测）。
func TestHandler_Resume_RejectsUnknownAndDuplicateIDs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries func(id string) []map[string]any
	}{
		{"未知 ID", func(id string) []map[string]any {
			return []map[string]any{resolved(id, true), resolved("not-an-open-interrupt", true)}
		}},
		{"重复 ID 先拒后批", func(id string) []map[string]any { return []map[string]any{resolved(id, false), resolved(id, true)} }},
		{"重复 ID 先批后拒", func(id string) []map[string]any { return []map[string]any{resolved(id, true), resolved(id, false)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newApprovalFixture("refund")
			thread := "t-ids-" + tc.name
			ids := f.start(t, thread)
			events := resumeEntries(t, f.h, thread, "r2", tc.entries(ids[0])...)
			if code := findErrorCode(events); code != "invalid_resume" {
				t.Fatalf("应报 invalid_resume，实际 %q", code)
			}
			if got := f.calls["refund"].Load(); got != 0 {
				t.Fatalf("resume 列表不合法时工具不该执行，实际 %d 次", got)
			}
			events = resumeEntries(t, f.h, thread, "r3", resolved(ids[0], true))
			if code := findErrorCode(events); code != "" || f.calls["refund"].Load() != 1 {
				t.Fatalf("改对之后应能续上并执行 1 次，实际 code=%q refund=%d", code, f.calls["refund"].Load())
			}
		})
	}
}

// 审批答复对象只认 approved 一个字段：带 editedArgs（带修改的批准）或别的字段时报 invalid_resume_payload、工具不执行。
// 曾经只取 approved，用户改过的参数被丢掉、工具按原参数执行（审计实测）。答复 schema 也写明不收别的字段。
func TestHandler_ApprovalAnswers_ExtraFieldsRejected(t *testing.T) {
	for _, payload := range []map[string]any{
		{"approved": true, "editedArgs": map[string]any{"amount": 100}},
		{"approved": true, "note": "ok"},
	} {
		f := newApprovalFixture("refund")
		const thread = "t-extra"
		events := parseSSE(postAGUI(f.h, `{"threadId":"t-extra","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String())
		its := interruptsIn(events)
		if len(its) != 1 {
			t.Fatalf("前置：应停在一条审批上，实际 %d 条", len(its))
		}
		if s, _ := its[0]["responseSchema"].(map[string]any); s["additionalProperties"] != false {
			t.Fatalf("审批的答复 schema 应写明不收别的字段，实际 %v", its[0]["responseSchema"])
		}
		id, _ := its[0]["id"].(string)
		events = resumeEntries(t, f.h, thread, "r2", resolved(id, payload))
		if code := findErrorCode(events); code != "invalid_resume_payload" {
			t.Fatalf("答复 %v 应报 invalid_resume_payload，实际 %q", payload, code)
		}
		if got := f.calls["refund"].Load(); got != 0 {
			t.Fatalf("答复 %v 不该让工具执行，实际 %d 次", payload, got)
		}
	}
}

// askThenRefundModel 第一轮调 ask_user，拿到答案后调 refund，再之后给出最终答复。
type askThenRefundModel struct{}

func (askThenRefundModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	results := 0
	for _, m := range msgs {
		if m.Role == schema.RoleTool {
			results++
		}
	}
	switch results {
	case 0:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"which order?"}`}}}), nil
	case 1:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Type: "function",
			Function: schema.FunctionCall{Name: "refund", Arguments: `{}`}}}), nil
	default:
		return schema.AssistantMessage("done"), nil
	}
}

func (m askThenRefundModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m askThenRefundModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 先挂一条普通提问、答完再挂一条审批：第二次挂起的审批照样按审批校验（"Yes." 被拦下），批准之后执行一次。挂起的
// 中断每次挂起都换成这次的——沿用上一次的话，这条审批不在其中，答复要么被拒、要么不按审批校验。
func TestHandler_Resume_OpenSetRefreshedOnEachInterrupt(t *testing.T) {
	var runs atomic.Int32
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				runs.Add(1)
				return tool.Success("ok"), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("ask_then_refund"), agent_v2.WithModel(askThenRefundModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithAskUser(), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	const thread = "t-refresh"
	its := interruptsIn(parseSSE(postAGUI(h, `{"threadId":"t-refresh","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String()))
	if len(its) != 1 || its[0]["reason"] == "tool_call" {
		t.Fatalf("前置：第一次应挂一条普通提问，实际 %v", its)
	}
	askID, _ := its[0]["id"].(string)
	its = interruptsIn(resumeEntries(t, h, thread, "r2", resolved(askID, "order-1")))
	if len(its) != 1 || its[0]["reason"] != "tool_call" {
		t.Fatalf("前置：第二次应挂一条审批，实际 %v", its)
	}
	approvalID, _ := its[0]["id"].(string)
	if code := findErrorCode(resumeEntries(t, h, thread, "r3", resolved(approvalID, "Yes."))); code != "invalid_resume_payload" {
		t.Fatalf("第二次挂起的审批答 Yes. 应按审批校验拦下，实际 %q", code)
	}
	if code := findErrorCode(resumeEntries(t, h, thread, "r4", resolved(approvalID, true))); code != "" {
		t.Fatalf("批准之后应能续上，实际 %q", code)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("批准之后 refund 应执行 1 次，实际 %d", got)
	}
}

// 续跑撞上存档不可续（checkpoint_not_resumable）时会话照原样挂着：记下的中断还在，审批答复照样按审批校验，放弃照样
// 走得通。
func TestHandler_Resume_NotResumableKeepsOpenSet(t *testing.T) {
	var runs atomic.Int32
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				runs.Add(1)
				return tool.Success("ok"), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("not_resumable"), agent_v2.WithModel(&callToolsOnce{names: []string{"refund"}}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
	}, agui.WithCheckpointer(&dropFrontierOnLoad{Checkpointer: compose_v2.NewInMemoryCheckpointer()}))
	const thread = "t-nr"
	its := interruptsIn(parseSSE(postAGUI(h, `{"threadId":"t-nr","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String()))
	if len(its) != 1 {
		t.Fatalf("前置：应停在一条审批上，实际 %v", its)
	}
	id, _ := its[0]["id"].(string)
	if code := findErrorCode(resumeEntries(t, h, thread, "r2", resolved(id, true))); code != "checkpoint_not_resumable" {
		t.Fatalf("前置：应撞上存档不可续，实际 %q", code)
	}
	if code := findErrorCode(resumeEntries(t, h, thread, "r3", resolved(id, "Yes."))); code != "invalid_resume_payload" {
		t.Fatalf("会话应照原样挂着：审批答 Yes. 应仍按审批拦下，实际 %q", code)
	}
	cancelled := map[string]any{"interruptId": id, "status": "cancelled"}
	if code := findErrorCode(resumeEntries(t, h, thread, "r4", cancelled)); code != "" {
		t.Fatalf("放弃应走得通，实际 %q", code)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("全程 refund 不该执行，实际 %d", got)
	}
}

// noCallIDModel 第一轮调 refund 但不给 tool_call ID（有的 provider 不回），看到工具结果后给出最终答复。
type noCallIDModel struct{}

func (noCallIDModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, m := range msgs {
		if m.Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{Type: "function",
		Function: schema.FunctionCall{Name: "refund", Arguments: `{}`}}}), nil
}

func (m noCallIDModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m noCallIDModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 没有 tool_call ID 的审批：reason 用 confirmation（tool_call 必须带 toolCallId），批准之后照常执行。
func TestHandler_ApprovalInterrupt_ConfirmationWithoutToolCallID(t *testing.T) {
	var runs atomic.Int32
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				runs.Add(1)
				return tool.Success("ok"), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("no_call_id"), agent_v2.WithModel(noCallIDModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	its := interruptsIn(parseSSE(postAGUI(h, `{"threadId":"t-conf","runId":"r1","messages":[{"role":"user","content":"go"}]}`).Body.String()))
	if len(its) != 1 {
		t.Fatalf("前置：应停在一条审批上，实际 %v", its)
	}
	if its[0]["reason"] != "confirmation" {
		t.Fatalf("没有 tool_call ID 的审批 reason 应为 confirmation，实际 %v", its[0]["reason"])
	}
	if _, has := its[0]["toolCallId"]; has {
		t.Fatalf("没有 tool_call ID 时不该带 toolCallId，实际 %v", its[0]["toolCallId"])
	}
	id, _ := its[0]["id"].(string)
	if code := findErrorCode(resumeEntries(t, h, "t-conf", "r2", resolved(id, true))); code != "" || runs.Load() != 1 {
		t.Fatalf("批准之后应执行 1 次，实际 code=%q runs=%d", code, runs.Load())
	}
}
