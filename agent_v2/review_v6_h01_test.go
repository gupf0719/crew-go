package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestReviewV6_H01_ExecInputBuilderCtxIsWorkerCtx_SubAgent 验 review-v6 H-01 fix —
// PlanAgent SubAgentName worker 路径的 ExecInputBuilder 应收到 workerCtx (含框架
// 注入的 nodeID), 不是 outer Run 入口 ctx (无 nodeID).
//
// 历史: audit³ LOW-A1 commit 注释承诺改 workerCtx 但代码 3 处全用 outer ctx
// (review-v6 视角 2 HIGH-2 + 视角 3 HIGH-1 跨视角共识同一 bug).
//
// sanity-revert: agent_v2/plan_agent.go SubAgentName worker 改回 `ctx` →
// 测试看到 nodeID 为空 → fail with diagnostic.
func TestReviewV6_H01_ExecInputBuilderCtxIsWorkerCtx_SubAgent(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("step done")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "step1", SubAgentName: "a1"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "step2", SubAgentName: "a2"}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var capturedNodeIDs []string
	custom := func(ctx context.Context, _ string, _ []agent_v2.ExecutedStep, current agent_v2.PlanStep, _ []int) (*schema.Message, error) {
		// fix 后: workerCtx 由 framework runNodeWithHooks push nodeID → 非空
		// revert 后: outer Run 入口 ctx 无 nodeID → 空字符串
		nodeID := string(compose_v2.GetCurrentNodeID(ctx))
		capturedNodeIDs = append(capturedNodeIDs, nodeID)
		return schema.UserMessage(current.Description), nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithExecInputBuilder(custom),
	).AddSubAgent("a1", a1).AddSubAgent("a2", a2)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	if len(capturedNodeIDs) != 2 {
		t.Fatalf("ExecInputBuilder 应被调 2 次, got %d", len(capturedNodeIDs))
	}
	for i, nodeID := range capturedNodeIDs {
		if nodeID == "" {
			t.Errorf("review-v6 H-01 fix: SubAgentName worker 第 %d 次 ExecInputBuilder ctx 应是 workerCtx (含 framework 注入的 nodeID), 实际空字符串 — 说明用了 outer Run ctx\n"+
				"sanity-revert: 改回 p.execInputBuilder(ctx, ...) → 此 fail",
				i)
		}
	}
}

// TestReviewV6_H01_ExecInputBuilderCtxIsWorkerCtx_TmpSubAgent 同 SubAgent 路径
// 但走 TmpSubAgent dispatcher worker.
func TestReviewV6_H01_ExecInputBuilderCtxIsWorkerCtx_TmpSubAgent(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("step done")})

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{
				Description: "step1",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp1", System: "sys1"},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var capturedNodeIDs []string
	custom := func(ctx context.Context, _ string, _ []agent_v2.ExecutedStep, current agent_v2.PlanStep, _ []int) (*schema.Message, error) {
		nodeID := string(compose_v2.GetCurrentNodeID(ctx))
		capturedNodeIDs = append(capturedNodeIDs, nodeID)
		return schema.UserMessage(current.Description), nil
	}

	dummyExec := agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(stub))
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanModel(stub),
		agent_v2.WithExecInputBuilder(custom),
	).SetExecutor(dummyExec)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	if len(capturedNodeIDs) != 1 {
		t.Fatalf("ExecInputBuilder 应被调 1 次, got %d", len(capturedNodeIDs))
	}
	if capturedNodeIDs[0] == "" {
		t.Errorf("review-v6 H-01 fix: TmpSubAgent dispatcher ExecInputBuilder ctx 应是 workerCtx (含 nodeID), 实际空字符串\n" +
			"sanity-revert: 改回 p.execInputBuilder(ctx, ...) → 此 fail")
	}
}

// TestReviewV6_H01_ExecInputBuilderCtxIsWorkerCtx_Executor 同 SubAgent 路径
// 但走 default executor dispatcher (PlanStep 既无 SubAgentName 也无 TmpSubAgent).
func TestReviewV6_H01_ExecInputBuilderCtxIsWorkerCtx_Executor(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("step done")})

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "step1"}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var capturedNodeIDs []string
	custom := func(ctx context.Context, _ string, _ []agent_v2.ExecutedStep, current agent_v2.PlanStep, _ []int) (*schema.Message, error) {
		nodeID := string(compose_v2.GetCurrentNodeID(ctx))
		capturedNodeIDs = append(capturedNodeIDs, nodeID)
		return schema.UserMessage(current.Description), nil
	}

	executor := agent_v2.NewBasicAgent(agent_v2.WithName("exec"), agent_v2.WithModel(stub))
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithExecInputBuilder(custom),
	).SetExecutor(executor)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	if len(capturedNodeIDs) != 1 {
		t.Fatalf("ExecInputBuilder 应被调 1 次, got %d", len(capturedNodeIDs))
	}
	if capturedNodeIDs[0] == "" {
		t.Errorf("review-v6 H-01 fix: executor dispatcher ExecInputBuilder ctx 应是 workerCtx (含 nodeID), 实际空字符串\n" +
			"sanity-revert: 改回 p.execInputBuilder(ctx, ...) → 此 fail")
	}
}
