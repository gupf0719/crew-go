package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// stubModel 实现 model.ChatModel——按 sequence 返回固定输出。
type stubModel struct {
	responses []*schema.Message
	idx       int
}

func newStubModel(responses []*schema.Message) *stubModel {
	return &stubModel{responses: responses}
}

func (m *stubModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if m.idx >= len(m.responses) {
		return schema.AssistantMessage("[stub exhausted]"), nil
	}
	out := m.responses[m.idx]
	m.idx++
	return out, nil
}

func (m *stubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *stubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*stubModel)(nil)

// TestBasicAgent_HITLResume 验证 ask_user 中断 + Resume 闭环。
func TestBasicAgent_HITLResume(t *testing.T) {
	askTool := tool.NewFuncTool(
		"ask",
		"ask user something",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("user said: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "what?"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"test"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	resp2 := schema.AssistantMessage("got it")
	m := newStubModel([]*schema.Message{resp1, resp2})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_basic"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: 触发 InterruptError
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("Run 1 expected interrupt")
	}
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 expected InterruptError, got: %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("Run 1 InterruptError missing Query.InterruptID")
	}

	// Run 2: Resume with answer
	events, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("answer_text"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume err: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("Resume no final message")
	}
	if !strings.Contains(final.Content, "got it") {
		t.Fatalf("final content unexpected: %q", final.Content)
	}
}

// hookInterruptStartHook 在 OnAgentStart 抛 InterruptError mock 业务"LLM 调用前用户审批"。
type hookInterruptStartHook struct {
	hook.NoOpHooks
	onErrorCalls atomic.Int32
}

func (h *hookInterruptStartHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return ctx, &compose_v2.InterruptError{
		Reason: "approve_llm_call",
		Query:  &compose_v2.AskQuery{InterruptID: "approve_gate", Question: "approve LLM call?"},
	}
}

func (h *hookInterruptStartHook) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	h.onErrorCalls.Add(1)
	return ctx
}

// TestBasicAgent_R25_HIGH1_HookInterruptNotReportedAsError 是 R25 audit HIGH-1 回归——
// 验证业务 hook（OnAgentStart 等 6 个返 error 的 hook）返 InterruptError 时框架**不**
// 上报 OnError——业务监控 / billing hook 不被 HITL 假阳性增计数。
//
// **Bug 历史**：原 agent_v2/basic_builder.go 7 处 hook err 路径无差别 safeOnError
// 上报 "agent_start_error" 等 → 业务监控把 HITL 当业务错。
// 同样不对称需查 sequential / loop / parallel / reflect / handoff / routing 6 编排
// 共 12 处对称漏修——R25 HIGH-1 一并修共 18 处。
func TestBasicAgent_R25_HIGH1_HookInterruptNotReportedAsError(t *testing.T) {
	hook1 := &hookInterruptStartHook{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
		agent_v2.WithHooks(hook1),
	)

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R25 HIGH-1 回归：hook 抛 InterruptError 应 propagate；实际 nil")
	}
	// 契约更新（2026-08-29）：**顶层** hook 抛中断被拒——图之前无档可落，旧行为是
	// IsInterrupt=true 而挂起档 0、用户答完 Resume 撞 ErrNoPendingCheckpoint 的**假暂停**。
	// 本用例守的真实伤害是下面那半（OnError 不误报，判据看原始 err、仍是中断故不上报）；
	// 中断类型断言随契约反转。嵌套（interruptHook 范式）的 IsInterrupt=true 由
	// TestHookInterrupt_NestedStillLandsCheckpoint 钉。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("顶层 hook 中断该被拒成非中断错误（假暂停→响亮错误），实际仍 IsInterrupt: %v", err)
	}
	if got := hook1.onErrorCalls.Load(); got != 0 {
		t.Errorf("R25 HIGH-1 回归：hook 抛 InterruptError 时 OnError **不**应被调（控制流非业务错）；实际调 %d 次\n"+
			"sanity-revert 删 OnAgentStart err 路径 IsInterrupt 守卫 → OnError 被误上报", got)
	}
}

// ─────────── audit³ D1 回归: OnAgentStart → OnAgentEnd ctx 跨阶段 propagation ───────────

// d1CtxKey is package-level (Go ctx.Value compares by key type, scoped types not equal)
type d1CtxKey struct{}

type ctxPushHookD1 struct {
	hook.NoOpHooks
	capturedAtEnd any
}

func (h *ctxPushHookD1) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	ctx = context.WithValue(ctx, d1CtxKey{}, "pushed-from-init")
	return ctx, nil
}

func (h *ctxPushHookD1) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.capturedAtEnd = ctx.Value(d1CtxKey{})
	return ctx
}

// TestAudit3_D1_BasicAgent_CtxPropagation_StartToEnd 验 audit³ D1 fix:
// OnAgentStart push 的 ctx 在 OnAgentEnd 拿到 (Run top-level fire 同函数 local var 串).
//
// sanity-revert: agent_v2.BasicAgent.Run 把 OnAgentStart 调用挪回 compose_v2 modelNode
// 内 fire → capturedAtEnd == nil (跨闭包 ctx 丢) → 本测 fail.
func TestAudit3_D1_BasicAgent_CtxPropagation_StartToEnd(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})
	hook := &ctxPushHookD1{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("d1_ctx"),
		agent_v2.WithModel(stub),
		agent_v2.WithHooks(hook),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if hook.capturedAtEnd != "pushed-from-init" {
		t.Errorf("audit³ D1 regression: OnAgentStart push ctx, OnAgentEnd 应拿到 'pushed-from-init', got %v\n"+
			"sanity-revert: agent_v2.BasicAgent.Run 把 OnAgentStart 挪回 compose_v2 modelNode 内 → 此测 fail",
			hook.capturedAtEnd)
	}
}

// resumeCtxHook 在 OnAgentStart 推 ctx 值, OnAgentEnd 拿——验证 BasicAgent.Resume
// 入口 fire OnAgentStart 后 push 的 ctx 真传到 defer OnAgentEnd.
//
// audit⁴ C HIGH-1 fix 守护: BasicAgent.Resume 原版无 OnAgentStart fire, Resume
// defer OnAgentEnd 拿不到业务 push 的 ctx. 修后 Resume top-level fire OnAgentStart
// 把返 ctx 赋给 local var, defer OnAgentEnd 同函数 scope 拿到推后的 ctx.
type resumeCtxHook struct {
	hook.NoOpHooks
	startCallsRun     atomic.Int32
	startCallsResume  atomic.Int32
	endCapturedRun    any
	endCapturedResume any
	isResume          bool
}

func (h *resumeCtxHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.isResume {
		h.startCallsResume.Add(1)
		return context.WithValue(ctx, d1CtxKey{}, "resume-pushed"), nil
	}
	h.startCallsRun.Add(1)
	return context.WithValue(ctx, d1CtxKey{}, "run-pushed"), nil
}

func (h *resumeCtxHook) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if h.isResume {
		h.endCapturedResume = ctx.Value(d1CtxKey{})
	} else {
		h.endCapturedRun = ctx.Value(d1CtxKey{})
	}
	return ctx
}

// TestAudit4C_HIGH1_BasicAgent_Resume_OnAgentStartCtxPropagation 验
// audit⁴ C HIGH-1 fix: BasicAgent.Resume 必须 fire OnAgentStart 让业务 OTel
// pattern (init push span, end close span) Resume 段独立 work.
//
// sanity-revert: 删 basic_agent.go Resume 内 audit⁴ C HIGH-1 fix block →
// endCapturedResume == nil → 此测 fail.
func TestAudit4C_HIGH1_BasicAgent_Resume_OnAgentStartCtxPropagation(t *testing.T) {
	// 真 HITL fixture（GA-M2 后 Resume 对无 cp fail-fast 在 OnAgentStart 之前——
	// 本测试要验的是 Resume **正常路径**的 OnAgentStart fire + ctx 传播，须先真
	// interrupt 再 Resume；原 fixture 用 no_such_sid 蹭"无 cp 也往下走"的旧协议，
	// 顺带修掉 stale 注释"返 ResumeNotFound err"——该错误类型从未存在过）。
	askTool := tool.NewFuncTool(
		"ask4c",
		"ask user",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask4c_id"); ok {
				return tool.Success("user said: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask4c_id", Question: "what?"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)
	tc := schema.ToolCall{ID: "tc_4c", Function: schema.FunctionCall{Name: "ask4c", Arguments: `{}`}}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)

	h := &resumeCtxHook{isResume: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("audit4c_resume"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
			schema.AssistantMessage("ok"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(h),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err == nil {
		t.Fatal("expect interrupt on first run")
	}
	h.startCallsResume.Store(0) // 只统计 Resume 段的 fire（Run 段已 fire 过）
	_, _ = ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"ask4c_id": schema.UserMessage("yes")}))

	// R5-K LOW-1 fix: 精确验 1 次而非"≥1"——若 framework 未来 bug 重复 fire 2+ 次
	// （如 audit + Run 入口都 fire），旧 "== 0" assertion silently 通过。
	if got := h.startCallsResume.Load(); got != 1 {
		t.Fatalf("audit⁴ C HIGH-1 regression: BasicAgent.Resume 必须 fire OnAgentStart 恰好 1 次, 实际 %d\n"+
			"sanity-revert: 删 basic_agent.go Resume audit⁴ C HIGH-1 fix → 此测 fail（init=0）；"+
			"重复 fire bug → 此测 fail（init=2+）", got)
	}
	if h.endCapturedResume != "resume-pushed" {
		t.Errorf("audit⁴ C HIGH-1 regression: Resume OnAgentStart push ctx 未传到 OnAgentEnd, got %v\n"+
			"sanity-revert: 删 basic_agent.go Resume audit⁴ C HIGH-1 fix → endCapturedResume == nil",
			h.endCapturedResume)
	}
}
