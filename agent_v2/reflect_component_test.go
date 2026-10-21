package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// ────────────────────────────────────────────────────────────────────────
// ReflectAgent evaluator component 退化真端到端测试 (Phase 2 agent 级 component 已删).
//
// **退化背景**: 原 reflect_agent.go subAgentReflector 在调 evaluator/reflector sub-agent
// 前 push WithComponentKind(evCtx, "evaluator") 让 observability 标 evaluator agent span.
// 但 CS-OSS/OTel operation.name 无 evaluator, sub-agent invocation 即 invoke_agent — 已
// 退化删除 push, evaluator sub-agent 走默认 agent span.
//
// **验真实路径 (非模拟)**: 用真 ReflectAgent + 真 BasicAgent evaluator, capture hook 接
// evaluator 验证它 OnAgentStart 收到的 ctx component **为空** (reflect_agent 不再 push).
//
// **防回归**: 若有人加回 reflect_agent.go subAgentReflector 的 evCtx WithComponentKind,
// capture.captured 变 "evaluator" → 此测 FAIL.
// ────────────────────────────────────────────────────────────────────────

// componentCaptureHooks 捕获 OnAgentStart (agent span 层) + OnModelStart (内部 node 层)
// 时 ctx 上的 component kind — 验 evaluator agent span 标 evaluator 且内部 model node 不污染.
type componentCaptureHooks struct {
	hook.NoOpHooks
	captured   string // OnAgentStart 时 component (应=evaluator)
	fired      bool
	modelComp  string // OnModelStart 时 component (应="" — 内部 node 被 graph_loop consume 清除)
	modelFired bool
}

func (c *componentCaptureHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	c.fired = true
	c.captured = compose_v2.GetComponentKind(ctx)
	return ctx, nil
}

func (c *componentCaptureHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	c.modelFired = true
	c.modelComp = compose_v2.GetComponentKind(ctx)
	return ctx, nil
}

func TestReflectAgent_EvaluatorNoComponentTag(t *testing.T) {
	capture := &componentCaptureHooks{}

	// actor: stub 返 "draft" (assistant 无 tool_call → BasicAgent 最终答复).
	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("actor"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("draft")})),
	)
	// evaluator: stub 返 "yes" → parseGoalMatched 识别 → GoalMatched=true (第 1 轮退出).
	// 接 capture hook 验证 evaluator OnAgentStart 收到的 ctx component.
	evaluator := agent_v2.NewBasicAgent(
		agent_v2.WithName("evaluator"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("yes")})),
		agent_v2.WithHooks(capture),
	)
	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("review"),
		agent_v2.WithReflectMaxRounds(3),
		agent_v2.WithEvaluator(evaluator),
	).WithActor(actor)

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !capture.fired {
		t.Fatal("evaluator sub-agent OnAgentStart 应被 fire (ReflectAgent 调 evaluator.Run)")
	}
	// evaluator component 已退化: reflect_agent 不再 push, evaluator sub-agent 收到空 component.
	if capture.captured != "" {
		t.Errorf("evaluator component 已退化, ReflectAgent 不应再 push component, 期望空, got %q\n"+
			"若非空: reflect_agent.go subAgentReflector 又加回了 evCtx WithComponentKind", capture.captured)
	}
	// model node 同样无 component (退化后 evCtx 本就不含 component).
	if capture.modelFired && capture.modelComp != "" {
		t.Errorf("evaluator 内部 model node 不应有 component, got %q", capture.modelComp)
	}
}
