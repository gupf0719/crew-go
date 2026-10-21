package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestPlan_SetExecutor 验证 SetExecutor 默认执行器路径——PlanStep 不指定 SubAgentName 时使用。
func TestPlan_SetExecutor(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done by exec")})
	executor := agent_v2.NewBasicAgent(agent_v2.WithName("exec"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			// step 不指定 SubAgentName → 走 executor
			return []agent_v2.PlanStep{{Description: "do it"}}, "", nil
		}
		return nil, "all done", nil
	}

	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p")).SetExecutor(executor)

	events, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "all done") {
		t.Errorf("expect 'all done', got %v", final)
	}
}

// TestPlan_AddSubAgent_Fluent 验证 AddSubAgent 链式 API + dispatch by name。
func TestPlan_AddSubAgent_Fluent(t *testing.T) {
	res := agent_v2.NewBasicAgent(agent_v2.WithName("res"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("research done")})))
	wri := agent_v2.NewBasicAgent(agent_v2.WithName("wri"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("writing done")})))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "research", SubAgentName: "researcher"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "write", SubAgentName: "writer"}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p")).
		AddSubAgent("researcher", res).
		AddSubAgent("writer", wri)

	events, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "completed") {
		t.Errorf("expect 'completed', got %v", final)
	}
}

// TestPlan_TmpSubAgent_Basic 验证 TmpSubAgent 当场构造一次性 BasicAgent。
func TestPlan_TmpSubAgent_Basic(t *testing.T) {
	tmpStub := newStubModel([]*schema.Message{schema.AssistantMessage("tmp ran")})

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{
				Description: "use tmp agent",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{
					Name:   "ad_hoc",
					System: "你是临时 agent",
				},
			}}, "", nil
		}
		return nil, "tmp completed", nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanModel(tmpStub),
	)
	// 必须有至少一个 sub-agent 注册——这里 register dummy 让 plan compile 通过
	dummy := agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(tmpStub))
	pa.AddSubAgent("dummy", dummy)

	events, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "tmp completed") {
		t.Errorf("expect 'tmp completed', got %v", final)
	}
}

// TestPlan_NoSubAgentNoExecutor 验证两者都没有时报错。
func TestPlan_NoSubAgentNoExecutor(t *testing.T) {
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "ok", nil
	}
	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p"))
	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "need at least one sub-agent") {
		t.Fatalf("expect 'need at least one sub-agent' err, got %v", err)
	}
}
