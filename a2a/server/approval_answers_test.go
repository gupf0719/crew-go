package server_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// a2a 服务端校验续发的答复：工具审批的文本只认固定的几个词、另认 {"approved": bool}，认不出的（`Yes.`、只有
// DataPart 的结构化答复曾都被当成拒绝）不交给审批层，任务继续停在 input-required；挂多问时只接受按 interruptId
// 的答复。

// callToolsModel 第一轮同批调 names 里的工具，看到工具结果后答 "done"。
type callToolsModel struct{ names []string }

func (m *callToolsModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
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

func (m *callToolsModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m *callToolsModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// approvalServer 起一个要审批的 served agent：names 里每个工具执行一次记一次数。
func approvalServer(t *testing.T, names ...string) (*a2aclient.Client, map[string]*atomic.Int32) {
	t.Helper()
	calls := map[string]*atomic.Int32{}
	tk := tool.NewToolkit()
	for _, n := range names {
		c := &atomic.Int32{}
		calls[n] = c
		tk.Register(tool.NewFuncTool(n, n, map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				c.Add(1)
				return tool.Success("ok"), nil
			}))
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("approver"), agent_v2.WithModel(&callToolsModel{names: names}),
		agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithMaxRounds(4))
	base := serveAgent(t, ag)
	client, err := a2aclient.NewFromCard(context.Background(), server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return client, calls
}

// turn 是一次请求的结局：终态、状态消息文本、挂起清单、任务 ID。
type turn struct {
	state   a2atypes.TaskState
	text    string
	pending []a2a.PendingQuestion
	taskID  a2atypes.TaskID
	ctxID   string
}

func send(t *testing.T, client *a2aclient.Client, msg *a2atypes.Message) turn {
	t.Helper()
	var out turn
	for ev, e := range client.SendStreamingMessage(context.Background(), &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			out.taskID, out.ctxID, out.state = v.ID, v.ContextID, v.Status.State
			if v.Status.Message != nil {
				out.text, out.pending = textOf(v.Status.Message.Parts), a2a.PendingQuestionsFromParts(v.Status.Message.Parts)
			}
		case *a2atypes.TaskStatusUpdateEvent:
			out.taskID, out.ctxID, out.state = v.TaskID, v.ContextID, v.Status.State
			if v.Status.Message != nil {
				out.text, out.pending = textOf(v.Status.Message.Parts), a2a.PendingQuestionsFromParts(v.Status.Message.Parts)
			}
		}
	}
	return out
}

func textOf(parts a2atypes.ContentParts) string {
	var sb strings.Builder
	for _, p := range parts {
		if p != nil {
			sb.WriteString(p.Text())
		}
	}
	return sb.String()
}

func reply(prev turn, parts ...*a2atypes.Part) *a2atypes.Message {
	return a2atypes.NewMessageForTask(a2atypes.MessageRoleUser, a2atypes.TaskInfo{TaskID: prev.taskID, ContextID: prev.ctxID}, parts...)
}

func TestServerApproval_SinglePendingAnswers(t *testing.T) {
	for _, tc := range []struct {
		name string
		part func() *a2atypes.Part
		runs int32
	}{
		{"文本 approve", func() *a2atypes.Part { return a2atypes.NewTextPart("approve") }, 1},
		{"文本 yes", func() *a2atypes.Part { return a2atypes.NewTextPart(" YES ") }, 1},
		{"DataPart approved true", func() *a2atypes.Part { return a2atypes.NewDataPart(map[string]any{"approved": true}) }, 1},
		{"文本 deny", func() *a2atypes.Part { return a2atypes.NewTextPart("deny") }, 0},
		{"DataPart approved false", func() *a2atypes.Part { return a2atypes.NewDataPart(map[string]any{"approved": false}) }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, calls := approvalServer(t, "refund")
			first := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
			if first.state != a2atypes.TaskStateInputRequired || len(first.pending) != 1 || !first.pending[0].Approval {
				t.Fatalf("前置：应停在一条审批上，got state=%s pending=%+v", first.state, first.pending)
			}
			second := send(t, client, reply(first, tc.part()))
			if second.state != a2atypes.TaskStateCompleted {
				t.Fatalf("认得的答复应续上并完成，got state=%s text=%q", second.state, second.text)
			}
			if got := calls["refund"].Load(); got != tc.runs {
				t.Fatalf("工具应执行 %d 次，实际 %d 次", tc.runs, got)
			}
		})
	}
}

// 认不出的审批答复：任务继续停在 input-required、消息里写明能接受哪些答复，工具不执行；重答之后照常执行。
func TestServerApproval_UnrecognizedReasks(t *testing.T) {
	client, calls := approvalServer(t, "refund")
	first := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	if first.state != a2atypes.TaskStateInputRequired {
		t.Fatalf("前置：应停在审批上，got %s", first.state)
	}
	second := send(t, client, reply(first, a2atypes.NewTextPart("Yes.")))
	if second.state != a2atypes.TaskStateInputRequired || !strings.Contains(second.text, "not recognized") {
		t.Fatalf("认不出的答复应再问一次，got state=%s text=%q", second.state, second.text)
	}
	if got := calls["refund"].Load(); got != 0 {
		t.Fatalf("认不出的答复不该执行工具，实际 %d 次", got)
	}
	third := send(t, client, reply(second, a2atypes.NewTextPart("approve")))
	if third.state != a2atypes.TaskStateCompleted || calls["refund"].Load() != 1 {
		t.Fatalf("重答批准之后应执行并完成，got state=%s runs=%d", third.state, calls["refund"].Load())
	}
}

// 挂两条审批：一条文本不能同时答两问（一句 approve 就批准了全部工具）；按 interruptId 分别作答，答了的执行，
// 没答的继续挂着。
func TestServerApproval_MultiPendingNeedsPerIDAnswers(t *testing.T) {
	client, calls := approvalServer(t, "refund", "notify")
	first := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	if first.state != a2atypes.TaskStateInputRequired || len(first.pending) != 2 {
		t.Fatalf("前置：应停在两条审批上，got state=%s pending=%+v", first.state, first.pending)
	}
	second := send(t, client, reply(first, a2atypes.NewTextPart("approve")))
	if second.state != a2atypes.TaskStateInputRequired || calls["refund"].Load()+calls["notify"].Load() != 0 {
		t.Fatalf("一条文本不该批准两条审批，got state=%s refund=%d notify=%d", second.state, calls["refund"].Load(), calls["notify"].Load())
	}
	yes := true
	third := send(t, client, reply(second, a2a.AnswerPart(first.pending[0].InterruptID, &yes, "")))
	if third.state != a2atypes.TaskStateInputRequired {
		t.Fatalf("只答了一问，另一问应继续挂着，got state=%s", third.state)
	}
	if calls["refund"].Load() != 1 || calls["notify"].Load() != 0 {
		t.Fatalf("答了的执行、没答的不执行，got refund=%d notify=%d", calls["refund"].Load(), calls["notify"].Load())
	}
	if len(third.pending) != 1 {
		t.Fatalf("应只剩一问挂着，got %+v", third.pending)
	}
	fourth := send(t, client, reply(third, a2a.AnswerPart(third.pending[0].InterruptID, nil, "approve")))
	if fourth.state != a2atypes.TaskStateCompleted || calls["notify"].Load() != 1 {
		t.Fatalf("剩下那问答完之后应完成，got state=%s notify=%d", fourth.state, calls["notify"].Load())
	}
}

// 按 interruptId 作答时给了不在挂起里的 ID：再问一次，不调 Resume。
func TestServerApproval_UnknownInterruptIDReasks(t *testing.T) {
	client, calls := approvalServer(t, "refund", "notify")
	first := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	yes := true
	second := send(t, client, reply(first, a2a.AnswerPart("not-a-pending-id", &yes, "")))
	if second.state != a2atypes.TaskStateInputRequired || !strings.Contains(second.text, "not one of the questions") {
		t.Fatalf("未知的 interruptId 应再问一次，got state=%s text=%q", second.state, second.text)
	}
	if calls["refund"].Load()+calls["notify"].Load() != 0 {
		t.Fatal("不该执行任何工具")
	}
}

// 客户端 human gate 模式：远端挂两问时宿主这边每一问各是一个提问，分别作答之后各拿各的答案。
func TestServerMultiQuery_HumanGatePerQuestion(t *testing.T) {
	base := serveAgent(t, newAsker(t, "Q-A?", "Q-B?"))
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base, a2a.WithHumanGate())
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_multi")).AddStep("remote", ra)
	hctx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(hctx, sess, schema.UserMessage("ask me"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("期望中断，got %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 2 || !strings.Contains(qs[0].Question, "Q-A?") || !strings.Contains(qs[1].Question, "Q-B?") {
		t.Fatalf("远端两问应各是宿主的一个提问，got %+v", qs)
	}
	events, err := seq.Resume(hctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			qs[0].InterruptID: schema.UserMessage("a-ans"),
			qs[1].InterruptID: schema.UserMessage("b-ans"),
		}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content != "got: a-ans+b-ans" {
		t.Fatalf("每问应拿到自己的答案，got %v", f)
	}
}

// 客户端 human gate 模式：远端只挂一条工具审批时，宿主这边那一问也标成审批、带上选项。
func TestServerApproval_HumanGateMarksApproval(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("approver"), agent_v2.WithModel(&callToolsModel{names: []string{"refund"}}),
		agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithMaxRounds(4))
	base := serveAgent(t, ag)
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base, a2a.WithHumanGate())
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_approval")).AddStep("remote", ra)
	hctx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(hctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query == nil {
		t.Fatalf("期望单问中断，got %v", err)
	}
	if ie.Query.Reason != agent_v2.ApprovalInterruptReason || len(ie.Query.Options) != 4 {
		t.Fatalf("远端是工具审批，宿主这一问应标成审批并带上四个选项，got reason=%q options=%v", ie.Query.Reason, ie.Query.Options)
	}
}
