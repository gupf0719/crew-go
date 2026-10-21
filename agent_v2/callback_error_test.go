package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// **3 callback signature 加 error 设计性 API 改动回归测试**：
//
// 修前：LoopAgentExitCondition / EvalFunc / ExecInputBuilder 签名仅返 bool/Message
// 不返 error → 业务想做 HITL gate（"循环退出前用户审批" / "评估通过前用户确认" /
// "敏感任务输入用户审批"）唯一方式是 panic InterruptError。但 panic 路径下框架
// 不在所有调用点 recover；即使 recover 也字符串化丢类型链 → HITL 完全无法表达。
//
// 修后：3 callback 都加 error 返回 → 业务可正常 `return false, &InterruptError{...}`，
// 框架 propagate Interrupt 到主 Run loop 走 cp save + Resume 协议。
//
// 本文件 3 个测试覆盖每个 callback 抛 InterruptError 端到端 propagate：
//   - LoopAgentExitConditionInterrupt: ExitCondition 抛 Interrupt
//   - EvalFuncInterrupt: EvalFunc 抛 Interrupt
//   - ExecInputBuilderInterrupt: ExecInputBuilder 抛 Interrupt

// TestLoopAgent_ExitConditionInterrupt 验证 LoopAgentExitCondition 抛 InterruptError
// 时 LoopAgent.Run 应 propagate（HITL gate "本轮退出前用户审批"）。
func TestLoopAgent_ExitConditionInterrupt(t *testing.T) {
	calls := atomic.Int32{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("body_done")})
	body := agent_v2.NewBasicAgent(agent_v2.WithName("body"), agent_v2.WithModel(stub))

	cond := func(_ context.Context, _ *schema.Message, round int) (bool, error) {
		calls.Add(1)
		// 第 1 轮抛 Interrupt mock"循环退出前用户审批"
		return false, &compose_v2.InterruptError{
			Reason: "approve_loop_exit",
			Query: &compose_v2.AskQuery{
				InterruptID: "loop_exit_gate",
				Question:    "approve loop exit?",
			},
		}
	}

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(5),
		agent_v2.WithLoopExitCondition(cond),
	).WithBody(body)

	_, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("ExitCondition 抛 InterruptError 应 propagate；实际 nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("err 应为 InterruptError；实际 %v (%T)\n"+
			"sanity-revert: 修前签名 `func(...) bool` 业务无法返 Interrupt → 测试编译失败", err, err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("ExitCondition 应被调 1 次（首轮就抛 Interrupt）；实际 %d", got)
	}
}

// TestReflectAgent_EvalFuncInterrupt 验证 EvalFunc 抛 InterruptError 时
// ReflectAgent.Run 应 propagate（HITL gate "评估通过前人工确认"）。
func TestReflectAgent_EvalFuncInterrupt(t *testing.T) {
	calls := atomic.Int32{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("draft")})
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(stub))

	evalFn := func(_ context.Context, _ *schema.Message) (bool, error) {
		calls.Add(1)
		return false, &compose_v2.InterruptError{
			Reason: "approve_eval",
			Query: &compose_v2.AskQuery{
				InterruptID: "eval_gate",
				Question:    "approve evaluation?",
			},
		}
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(3),
		agent_v2.WithEvalFunc(evalFn),
	).WithActor(actor)

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("draft a doc"))
	if err == nil {
		t.Fatalf("EvalFunc 抛 InterruptError 应 propagate；实际 nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("err 应为 InterruptError；实际 %v (%T)\n"+
			"sanity-revert: 修前签名 `func(...) bool` 业务无法返 Interrupt → 测试编译失败", err, err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("EvalFunc 应被调 1 次（首轮就抛 Interrupt）；实际 %d", got)
	}
}

// TestPlanAgent_ExecInputBuilderInterrupt 验证 ExecInputBuilder 抛 InterruptError
// 时 PlanAgent.Run 应 propagate（HITL gate "敏感任务输入用户审批"）。
func TestPlanAgent_ExecInputBuilderInterrupt(t *testing.T) {
	calls := atomic.Int32{}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("sub"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
	)

	// 自定义 Planner：返 1 step 让 ExecInputBuilder 被调
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) > 0 {
			return nil, "done", nil
		}
		return []agent_v2.PlanStep{
			{Description: "process sensitive data", SubAgentName: "sub"},
		}, "", nil
	})

	builder := agent_v2.ExecInputBuilder(func(_ context.Context, _ string, _ []agent_v2.ExecutedStep, _ agent_v2.PlanStep, _ []int) (*schema.Message, error) {
		calls.Add(1)
		return nil, &compose_v2.InterruptError{
			Reason: "approve_input",
			Query: &compose_v2.AskQuery{
				InterruptID: "input_gate",
				Question:    "approve sub-agent input (contains PII)?",
			},
		}
	})

	pa := agent_v2.NewPlanAgent(
		planner,
		agent_v2.PlanName("plan"),
		agent_v2.WithExecInputBuilder(builder),
	).AddSubAgent("sub", subAgent)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("process data"))
	if err == nil {
		t.Fatalf("ExecInputBuilder 抛 InterruptError 应 propagate；实际 nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		// IsInterrupt 透 wrap chain，所以即使 wrap 也能识别
		t.Errorf("err 应为 InterruptError；实际 %v (%T)\n"+
			"sanity-revert: 修前签名 `func(...) *Message` 业务无法返 Interrupt → 测试编译失败", err, err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("ExecInputBuilder 应被调 1 次；实际 %d", got)
	}
	if !strings.Contains(err.Error(), "exec input builder") {
		t.Errorf("err 应含 'exec input builder' wrap；实际 %v", err)
	}
}
