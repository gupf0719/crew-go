package a2a_test

// 黑盒测试 RemoteAgent.Run 行为，用 in-process a2asrv server 做 fixture（非外部服务，
// 不打 integration tag）。覆盖 Task 完成 / input-required 默认 / F2 事件去重锚点。
// 完整 HITL 闭环（WithHumanGate 嵌 Sequential + Resume）由 example/a2a_hitl_smoke 覆盖。

import (
	"context"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// fnExec 用函数实现 a2asrv.AgentExecutor，让每个测试自定义远程行为。
type fnExec func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool)

func (f fnExec) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) { f(execCtx, yield) }
}

func (fnExec) Cancel(_ context.Context, _ *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {}
}

// startServer 起一个 in-process A2A server（JSON-RPC + AgentCard），返回 base URL。
// 用 NewUnstartedServer 先拿端口再建带正确 URL 的 card——避开"card 需 URL、URL 需 server"鸡蛋问题。
func startServer(t *testing.T, exec a2asrv.AgentExecutor) string {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	baseURL := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name:        "Test Agent",
		Description: "in-process test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(baseURL+"/invoke", a2atypes.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	srv.Start()
	t.Cleanup(srv.Close)
	return baseURL
}

func inputText(execCtx *a2asrv.ExecutorContext) string {
	if execCtx.Message != nil && len(execCtx.Message.Parts) > 0 {
		return execCtx.Message.Parts[0].Text()
	}
	return ""
}

// completedExec：submitted → completed("done: <text>")。
func completedExec(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
	if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
		return
	}
	yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
		a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done: "+inputText(execCtx)))), nil)
}

// askExec：submitted → input-required("need more")。
func askExec(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
	if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
		return
	}
	yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
		a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("need more"))), nil)
}

func TestRemoteAgent_Completed(t *testing.T) {
	base := startServer(t, fnExec(completedExec))
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "done: hi" {
		t.Fatalf("final = %v, want content 'done: hi'", final)
	}
}

// TestRemoteAgent_NoInputEventDup 锚定 F2 fix：RemoteAgent 只 emit 远程输出的 ExitEvent，
// 不 emit input 的 OutputEvent。否则 host Resume 重跑 RemoteAgent.Run 会把 input event
// 重复写进共享父 session（泄漏进下游读 history 的 LLM 输入，同 ISSUE-74 class）。
// **sanity-revert**：恢复 Run 里的 `AddEvent(NewOutputEvent(input))` 后，事件数变 2 → 本测试 fail。
func TestRemoteAgent_NoInputEventDup(t *testing.T) {
	base := startServer(t, fnExec(completedExec))
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	sess := agent_v2.NewRunSession()
	events, err := ra.Run(ctx, sess, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "done: hi" {
		t.Fatalf("Run 应成功返 'done: hi'，got %v", final)
	}
	// 关键断言：恰好 1 个事件（仅 ExitEvent）。若 RemoteAgent 仍 emit input OutputEvent
	// 则为 2 → fail。session 累积同理。
	if len(events) != 1 {
		t.Fatalf("F2: 期望恰好 1 个事件（仅 ExitEvent），got %d——input OutputEvent 泄漏？", len(events))
	}
	if got := len(sess.Events()); got != 1 {
		t.Fatalf("F2: session.Events() 期望 1 条，got %d", got)
	}
}

// recHooks 记录业务级 hook 调用（RemoteAgent WithHooks 观测范式）。
type recHooks struct {
	hook.NoOpHooks
	starts     int
	ends       int
	lastEndErr error
}

func (h *recHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.starts++
	return ctx, nil
}

func (h *recHooks) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.ends++
	h.lastEndErr = agent_v2.GetEndError(ctx)
	return ctx
}

// TestRemoteAgent_Hooks 锚定 audit obs-#3：WithHooks 后 agent 形态 fire OnAgentStart/End 对
// （对齐 8 编排），含中断路径（WithEndError 协议让 observability 数 interrupts）。
// **sanity-revert**：删 Run 里的 hook 火法块 → starts/ends 归 0 → FAIL。
func TestRemoteAgent_Hooks(t *testing.T) {
	ctx := context.Background()

	// 路径 1：completed——start/end 各 1，end 无 error
	base := startServer(t, fnExec(completedExec))
	rec := &recHooks{}
	ra, err := a2a.NewRemoteAgent(ctx, base, a2a.WithHooks(rec))
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	if _, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rec.starts != 1 || rec.ends != 1 {
		t.Fatalf("completed 路径应 start/end 各 1, got starts=%d ends=%d", rec.starts, rec.ends)
	}
	if rec.lastEndErr != nil {
		t.Fatalf("completed 路径 OnAgentEnd 不应带 error, got %v", rec.lastEndErr)
	}

	// 路径 2：humanGate 中断——OnAgentEnd 仍 fire 且 WithEndError 带 InterruptError
	base2 := startServer(t, fnExec(askExec))
	rec2 := &recHooks{}
	ra2, err := a2a.NewRemoteAgent(ctx, base2, a2a.WithHumanGate(), a2a.WithHooks(rec2))
	if err != nil {
		t.Fatalf("NewRemoteAgent(humanGate): %v", err)
	}
	_, runErr := ra2.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("ask me"))
	if _, ok := compose_v2.IsInterrupt(runErr); !ok {
		t.Fatalf("humanGate 应抛 InterruptError, got %v", runErr)
	}
	if rec2.starts != 1 || rec2.ends != 1 {
		t.Fatalf("中断路径也应 start/end 各 1, got starts=%d ends=%d", rec2.starts, rec2.ends)
	}
	if _, ok := compose_v2.IsInterrupt(rec2.lastEndErr); !ok {
		t.Fatalf("中断路径 OnAgentEnd 应经 WithEndError 看到 InterruptError, got %v", rec2.lastEndErr)
	}
}

// TestRemoteAgent_InputRequiredDefault：不开 humanGate 时 input-required 当普通输出返回
// （不抛 error），追问文本透给 host LLM 自动补。
func TestRemoteAgent_InputRequiredDefault(t *testing.T) {
	base := startServer(t, fnExec(askExec))
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base) // 不开 humanGate
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("默认模式 input-required 不应抛 error，got %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "input_required") || !strings.Contains(final.Content, "need more") {
		t.Fatalf("期望 input-required 追问消息（含 'input_required' + 'need more'），got %v", final)
	}
}
