package agent_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// planOnAgentEndCounter 统计 PlanAgent.OnAgentEnd 触发次数 — ISSUE-83 验证。
type planOnAgentEndCounter struct {
	hook.NoOpHooks
	count atomic.Int32
}

func (h *planOnAgentEndCounter) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.count.Add(1)
	return ctx
}

// TestISSUE83_PlanAgent_OnAgentEnd_TriggeredOnce_Success 验 ISSUE-83 fix —
// PlanAgent 整体生命周期 OnAgentEnd 应 fire 1 次（与其他 8 编排架构对称）。
//
// **bug 历史**: pre-ISSUE-83 PlanAgent 是 9 编排里**唯一**不接业务级 hook.Hooks
// 的 agent — 业务监控/SLO/计费按 agent 整体生命周期统计时漏数据。
//
// **fix**: PlanAgent.Config 加 hooks 字段 + WithPlanHooks option + Run defer
// 直接调 p.hooks.OnAgentEnd 兜底（学 ISSUE-82 BasicAgent 单一所有权; audit² 删
// SafeOnAgentEnd wrapper 跟 eino 对齐 — framework outer recover 兜底 panic）。
//
// **sanity-revert**: 删 PlanAgent.Run defer OnAgentEnd 块 → 测试 fail "got 0 want 1"。
func TestISSUE83_PlanAgent_OnAgentEnd_TriggeredOnce_Success(t *testing.T) {
	counter := &planOnAgentEndCounter{}

	stub := newStubModel([]*schema.Message{schema.AssistantMessage("step done")})
	executor := agent_v2.NewBasicAgent(agent_v2.WithName("exec"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "do"}}, "", nil
		}
		return nil, "completed", nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanHooks(counter), // ISSUE-83 新 option
	).SetExecutor(executor)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	got := counter.count.Load()
	if got != 1 {
		t.Errorf("ISSUE-83 fix: PlanAgent OnAgentEnd 应 fire 1 次 (Run defer 单一所有权), got %d 次\n"+
			"sanity-revert: 删 PlanAgent.Run defer OnAgentEnd 块 → got = 0", got)
	}
}

// TestISSUE83_PlanAgent_OnAgentEnd_TriggeredOnce_BizError 验业务错退出路径
// PlanAgent OnAgentEnd 仍 fire 1 次（defer 兜底）。
func TestISSUE83_PlanAgent_OnAgentEnd_TriggeredOnce_BizError(t *testing.T) {
	counter := &planOnAgentEndCounter{}

	// planner 抛业务错
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "", errors.New("planner business error")
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanHooks(counter),
	)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err == nil {
		t.Fatalf("Run 应抛业务错, got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("应返非 InterruptError 业务错, got InterruptError")
	}

	got := counter.count.Load()
	if got != 1 {
		t.Errorf("ISSUE-83 fix: PlanAgent 业务错退出 OnAgentEnd 应 fire 1 次 (Run defer 兜底), got %d 次", got)
	}
}

// TestISSUE83_PlanAgent_NoHooks_DefaultNoOp 验默认 NoOp hooks 不传 WithPlanHooks
// 时 Run 不崩 (向后兼容)。
func TestISSUE83_PlanAgent_NoHooks_DefaultNoOp(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})
	executor := agent_v2.NewBasicAgent(agent_v2.WithName("exec"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "do"}}, "", nil
		}
		return nil, "completed", nil
	}

	// **关键**: 不传 WithPlanHooks — pre-ISSUE-83 行为应完全等价
	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p")).SetExecutor(executor)

	events, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("ISSUE-83 fix: 默认 NoOp hooks 不传 WithPlanHooks 应不崩, err=%v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil {
		t.Errorf("应返 final message, got nil")
	}
}

// TestISSUE83_PlanAgent_HITLResume_OnAgentEnd_TwoFires 验 audit MED-1 — HITL
// Resume 闭环 OnAgentEnd 应 fire 2 次 (Run 抛 Interrupt 1 次 + Resume 完成 1 次)。
//
// pre-fix bug: PlanAgent.Resume defer 缺 OnAgentEnd → Resume 后 hook 不 fire
// → 业务监控漏 Resume 退出事件 (audit² 删 SafeOnAgentEnd wrapper 跟 eino 对齐 —
// framework outer recover 兜底 panic)。
//
// **sanity-revert**: 删 PlanAgent.Resume defer OnAgentEnd 块 → 测试 fail
// "got 1 want 2"。
func TestISSUE83_PlanAgent_HITLResume_OnAgentEnd_TwoFires(t *testing.T) {
	counter := &planOnAgentEndCounter{}

	const askID = "plan_hitl_id"
	subRunCount := 0
	subStubFn := func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
		subRunCount++
		// 第二次跑 (Resume) 时 ResumeContext 应有答案
		if _, ok := compose_v2.GetResumeAnswerText(ctx, askID); ok {
			return schema.AssistantMessage("sub_done"), nil
		}
		return nil, &compose_v2.InterruptError{
			Reason: "ask user from sub-agent",
			Query:  &compose_v2.AskQuery{InterruptID: askID, Question: "approve?"},
		}
	}
	subAgent := agent_v2.NewBasicAgent(agent_v2.WithName("sub"), agent_v2.WithModel(&fnModel{fn: subStubFn}))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) > 0 {
			return nil, "plan_done", nil
		}
		return []agent_v2.PlanStep{{Description: "sub task", SubAgentName: "sub"}}, "", nil
	}
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("plan"),
		agent_v2.WithPlanHooks(counter), // ISSUE-83 Plan-level hooks
	).AddSubAgent("sub", subAgent)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: PlanAgent.Run 抛 InterruptError → defer fire 1 次
	_, err := pa.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 1 应抛 InterruptError, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("应返 InterruptError, got %v", err)
	}
	gotRun := counter.count.Load()
	if gotRun != 1 {
		t.Fatalf("Run 抛 Interrupt 退出 OnAgentEnd 应 fire 1 次, got %d", gotRun)
	}

	// Resume: PlanAgent.Resume 完成 → defer fire 1 次 (累计 2)
	_, err = pa.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("yes"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}

	gotTotal := counter.count.Load()
	if gotTotal != 2 {
		t.Errorf("ISSUE-83 audit MED-1 fix: HITL Resume 闭环 OnAgentEnd 应 fire 2 次 (Run 1 + Resume 各 1), got %d 次\n"+
			"sanity-revert: 删 PlanAgent.Resume defer OnAgentEnd 块 → got = 1", gotTotal)
	}
}

// TestISSUE83_PlanAgent_OnAgentEnd_NilHooks_NoCrash 验显式传 nil hooks 不崩
// (WithPlanHooks(nil) → 自动 fallback 到 NoOpHooks)
func TestISSUE83_PlanAgent_OnAgentEnd_NilHooks_NoCrash(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})
	executor := agent_v2.NewBasicAgent(agent_v2.WithName("exec"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "do"}}, "", nil
		}
		return nil, "completed", nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanHooks(nil), // 显式 nil — 应 fallback NoOp 不崩
	).SetExecutor(executor)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("ISSUE-83 fix: WithPlanHooks(nil) 应 fallback NoOp 不崩, err=%v", err)
	}
}
