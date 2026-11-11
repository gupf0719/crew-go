package a2a_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

type continuationRequest struct {
	input, taskID, contextID, assignedTask, assignedContext string
}

type continuationRemote struct {
	mu       sync.Mutex
	requests []continuationRequest
}

func (r *continuationRemote) execute(ec *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
	input := inputText(ec)
	r.mu.Lock()
	r.requests = append(r.requests, continuationRequest{
		input: input, taskID: string(ec.Message.TaskID), contextID: ec.Message.ContextID,
		assignedTask: string(ec.TaskID), assignedContext: ec.ContextID,
	})
	r.mu.Unlock()

	state, reply := a2atypes.TaskStateCompleted, "new:"+input
	switch {
	case ec.StoredTask != nil:
		reply = "continued:" + input
	default:
		if !yield(a2atypes.NewSubmittedTask(ec, ec.Message), nil) {
			return
		}
		switch input {
		case "发起转账":
			state, reply = a2atypes.TaskStateInputRequired, "要转多少？"
		case "订会议室":
			state, reply = a2atypes.TaskStateInputRequired, "要几点？"
		}
	}
	yield(a2atypes.NewStatusUpdateEvent(ec, state,
		a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart(reply))), nil)
}

func (r *continuationRemote) snapshot() []continuationRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]continuationRequest(nil), r.requests...)
}

type continuationTrace struct {
	hook.NoOpHooks
	calls []struct{ runID, ns string }
}

func (h *continuationTrace) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	ns := compose_v2.GetGraphNS(ctx)
	h.calls = append(h.calls, struct{ runID, ns string }{agent_v2.GetSession(ctx).RunID, ns})
	if ns == "weather" {
		in.Messages[len(in.Messages)-1] = schema.UserMessage("查东京天气")
	}
	return ctx, nil
}

func TestRemoteAgent_DefaultSequentialStartsIndependentTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	remote := &continuationRemote{}
	trace := &continuationTrace{}
	ra, err := a2a.NewRemoteAgent(ctx, startServer(t, fnExec(remote.execute)), a2a.WithName("remote"), a2a.WithHooks(trace))
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("two_requests")).
		AddStep("transfer", ra).AddStep("weather", ra)
	sess := agent_v2.NewRunSession()
	events, err := seq.Run(ctx, sess, schema.UserMessage("发起转账"))
	final := agent_v2.FinalMessage(events)
	got := remote.snapshot()
	if err != nil || final == nil || len(got) != 2 || len(trace.calls) != 2 {
		t.Fatalf("应跑完两步，实际请求=%+v，答复=%+v，错误=%v", got, final, err)
	}
	first, second := got[0], got[1]
	if trace.calls[0].runID != sess.RunID || trace.calls[1].runID != sess.RunID ||
		trace.calls[0].ns != "transfer" || trace.calls[1].ns != "weather" ||
		first.input != "发起转账" || first.taskID != "" || first.assignedTask == "" ||
		first.assignedContext == "" || second.input != "查东京天气" {
		t.Fatalf("前置条件不符，调用=%+v，请求=%+v", trace.calls, got)
	}
	if second.taskID != "" || second.assignedTask == first.assignedTask ||
		second.contextID != first.assignedContext || final.Content != "new:查东京天气" {
		t.Fatalf("天气请求应沿用 contextId 并开新任务，实际请求=%+v，答复=%q", got, final.Content)
	}
}

func tokenFromToolResult(content string) string {
	line, _, _ := strings.Cut(content, "\n")
	token, ok := strings.CutPrefix(line, "[input_required] continuation=")
	if !ok {
		return ""
	}
	return token
}

type continuationModel struct{}

func (*continuationModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	last := messages[len(messages)-1]
	switch {
	case last.Role == schema.RoleUser && last.Content == "帮我订会":
		return continuationToolCall("book", "订会议室", ""), nil
	case schema.GetToolResultID(last) == "book":
		token := tokenFromToolResult(last.Content)
		if token == "" || !strings.Contains(last.Content, "要几点？") {
			return nil, fmt.Errorf("tool result is missing its question or continuation: %q", last.Content)
		}
		return continuationToolCall("answer", "下午三点", token), nil
	case schema.GetToolResultID(last) == "answer" && last.Content == "continued:下午三点":
		return schema.AssistantMessage("会议室已预约"), nil
	default:
		return nil, fmt.Errorf("unexpected model input: %q", last.Content)
	}
}

func (*continuationModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *continuationModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func continuationToolCall(id, input, token string) *schema.Message {
	args := map[string]string{"input": input}
	if token != "" {
		args["continuation"] = token
	}
	encoded, _ := json.Marshal(args)
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: id, Type: "function",
		Function: schema.FunctionCall{Name: "remote", Arguments: string(encoded)},
	}})
}

func TestRemoteAgent_DefaultReActContinuesSameTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	remote := &continuationRemote{}
	trace := &continuationTrace{}
	rt, err := a2a.NewRemoteAgentTool(ctx, startServer(t, fnExec(remote.execute)), a2a.WithName("remote"), a2a.WithHooks(trace))
	if err != nil {
		t.Fatal(err)
	}
	tk := tool.NewToolkit()
	tk.Register(rt)
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk),
		agent_v2.WithModel(&continuationModel{}), agent_v2.WithMaxRounds(4))
	sess := agent_v2.NewRunSession()
	events, err := host.Run(ctx, sess, schema.UserMessage("帮我订会"))
	final := agent_v2.FinalMessage(events)
	got := remote.snapshot()
	if err != nil || final == nil || final.Content != "会议室已预约" || len(got) != 2 || len(trace.calls) != 2 {
		t.Fatalf("ReAct 应读到句柄、补答并完成，实际请求=%+v，答复=%+v，错误=%v", got, final, err)
	}
	properties, _ := rt.InputSchema()["properties"].(map[string]any)
	if properties["continuation"] == nil {
		t.Fatal("工具 schema 应向模型声明可选的 continuation 参数")
	}
	first, answer := got[0], got[1]
	if trace.calls[0].runID != sess.RunID || trace.calls[1].runID != sess.RunID || trace.calls[0].ns == trace.calls[1].ns ||
		first.input != "订会议室" || first.taskID != "" || first.assignedTask == "" || first.assignedContext == "" ||
		answer.input != "下午三点" || answer.taskID != first.assignedTask || answer.contextID != first.assignedContext {
		t.Fatalf("跨工具调用补答应发送原 taskId、contextId 和答复正文，实际调用=%+v，请求=%+v", trace.calls, got)
	}
}

func TestRemoteAgent_DefaultAnswersTwoPendingTasksInReverse(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	remote := &continuationRemote{}
	ra, err := a2a.NewRemoteAgent(ctx, startServer(t, fnExec(remote.execute)), a2a.WithName("remote"))
	if err != nil {
		t.Fatal(err)
	}
	rt := agent_v2.AgentAsTool(ra)
	sess := agent_v2.NewRunSession()
	ctx = agent_v2.WithSession(ctx, sess)
	call := func(args map[string]any) string {
		t.Helper()
		out, err := rt.Call(ctx, args)
		if err != nil || out == nil || out.Error != nil {
			t.Fatalf("工具调用失败，结果=%+v，错误=%v", out, err)
		}
		content, ok := out.Output.(string)
		if !ok {
			t.Fatalf("工具输出应为字符串，实际 %T", out.Output)
		}
		return content
	}

	firstResult := call(map[string]any{"input": "发起转账"})
	secondResult := call(map[string]any{"input": "订会议室"})
	got := remote.snapshot()
	if len(got) != 2 || got[0].assignedTask == "" || got[0].assignedContext == "" ||
		got[1].taskID != "" || got[1].assignedTask == got[0].assignedTask || got[1].contextID != got[0].assignedContext {
		t.Fatalf("同一 run 的两次请求应各自开任务并沿用会话，实际 %+v", got)
	}
	first, second := tokenFromToolResult(firstResult), tokenFromToolResult(secondResult)
	if first == "" || second == "" || first == second ||
		!strings.Contains(firstResult, "要转多少？") || !strings.Contains(secondResult, "要几点？") {
		t.Fatalf("两问应各有句柄，实际结果 %q、%q", firstResult, secondResult)
	}
	for _, answer := range []struct {
		token, input string
		request      continuationRequest
	}{
		{second, "下午三点", got[1]},
		{first, "500 元", got[0]},
	} {
		if result := call(map[string]any{"input": answer.input, "continuation": answer.token}); result != "continued:"+answer.input {
			t.Fatalf("补答结果 = %q，输入 = %q", result, answer.input)
		}
		requests := remote.snapshot()
		last := requests[len(requests)-1]
		if last.input != answer.input || last.taskID != answer.request.assignedTask || last.contextID != answer.request.assignedContext {
			t.Fatalf("答复串到其他任务，实际请求=%+v，原请求=%+v", last, answer.request)
		}
	}
	if got := remote.snapshot(); len(got) != 4 {
		t.Fatalf("应只发送两次委派和两次补答，实际 %+v", got)
	}
}

// 同一个句柄只允许一个补答在途，第二次调用应在联系远端之前被拒。
func TestRemoteAgent_ContinuationRejectsConcurrentAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	remote := &continuationRemote{}
	started := make(chan struct{})
	release := make(chan struct{})
	var arrivals atomic.Int32
	var released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	defer unblock()

	base := startServer(t, fnExec(func(ec *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		if ec.StoredTask != nil && arrivals.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
		}
		remote.execute(ec, yield)
	}))
	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	first, err := ra.Run(ctx, sess, schema.UserMessage("订会议室"))
	if err != nil {
		t.Fatal(err)
	}
	token := a2a.Continuation(agent_v2.FinalMessage(first))
	if token == "" {
		t.Fatal("前置条件缺少续接句柄")
	}
	answer := a2a.WithContinuation(schema.UserMessage("下午三点"), token)

	done := make(chan error, 1)
	go func() {
		events, err := ra.Run(ctx, sess, answer)
		if err == nil {
			if m := agent_v2.FinalMessage(events); m == nil || m.Content != "continued:下午三点" {
				err = fmt.Errorf("first answer returned %v", m)
			}
		}
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("第一个补答没有送达远端")
	}
	if _, err := ra.Run(ctx, sess, answer); err == nil || !strings.Contains(err.Error(), "in progress") {
		t.Errorf("第二次补答应被判为同一问在途，实际 %v", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatalf("第一个补答应完成，实际 %v", err)
	}
	if got := remote.snapshot(); len(got) != 2 || got[1].taskID != got[0].assignedTask || arrivals.Load() != 1 {
		t.Fatalf("远端应只收到一次补答，实际 %+v", got)
	}
}
