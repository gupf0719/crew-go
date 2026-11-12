package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

type plannerTestModel func(context.Context, []*schema.Message) (*schema.Message, error)

func (m plannerTestModel) Generate(ctx context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m(ctx, messages)
}

func (m plannerTestModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m plannerTestModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestWebPlanner_ResumeWithSynchronousCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithDurabilityCtx(compose_v2.WithCheckpointerCtx(ctx, cp), compose_v2.DurabilitySync)
	const task = "审核预算样本"
	const result = "预算样本审核完毕"
	const confirmation = "确认并保留金额明细"
	var plannerNS string
	var planned, replanned, executed int

	planner := agent_v2.NewBasicAgent(agent_v2.WithName("budget_planner"), agent_v2.WithModel(plannerTestModel(
		func(ctx context.Context, messages []*schema.Message) (*schema.Message, error) {
			if len(messages) == 0 || !strings.Contains(messages[len(messages)-1].Content, task) {
				return nil, fmt.Errorf("planner did not receive the task")
			}
			prompt := messages[len(messages)-1].Content
			if strings.Contains(prompt, result) {
				if !strings.Contains(prompt, confirmation) {
					return nil, fmt.Errorf("replanning did not receive the user's instructions")
				}
				replanned++
				return schema.AssistantMessage("[]"), nil
			}
			plannerNS = compose_v2.GetGraphNS(ctx)
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认预算规划？"})
			if err != nil {
				return nil, err
			}
			if answer == nil || answer.Content != confirmation {
				return nil, fmt.Errorf("planner did not receive its answer")
			}
			planned++
			return schema.AssistantMessage(`[{"desc":"审核预算","agent":"reviewer"}]`), nil
		})))
	reviewer := agent_v2.NewAgentFunc("reviewer", "审核预算", func(_ context.Context, input string) (string, error) {
		if !strings.Contains(input, "审核预算") {
			return "", fmt.Errorf("reviewer did not receive the planned step")
		}
		executed++
		return result, nil
	})
	workers := map[string]agent_v2.Agent{"reviewer": reviewer}
	sink := newEventSink()
	defer sink.close()
	plan := agent_v2.NewPlanAgent(newWebPlanner(planner, sink, workers),
		agent_v2.PlanName("budget"), agent_v2.WithSubAgents(workers))
	session := agent_v2.NewRunSession()
	_, err := plan.Run(ctx, session, schema.UserMessage(task))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || interrupt.Query == nil || interrupt.Query.Question != "确认预算规划？" || executed != 0 {
		t.Fatalf("应在执行前保留规划器的问题，执行 %d 次，err=%v", executed, err)
	}
	if plannerNS == "" {
		t.Fatal("规划器应拥有独立于 Plan 图根的存档 namespace")
	}
	for _, ns := range []string{"", plannerNS} {
		snapshot, err := cp.LoadNS(ctx, session.RunID, ns)
		if err != nil || snapshot == nil {
			t.Fatalf("应保留 namespace %q 的存档，err=%v", ns, err)
		}
	}

	interruptedPlannerNS := plannerNS
	events, err := plan.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			interrupt.Query.InterruptID: schema.UserMessage(confirmation),
		}), agent_v2.WithResumeUserText(confirmation))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || !strings.Contains(final.Content, result) || planned != 1 || executed != 1 || replanned != 1 {
		t.Fatalf("续跑应完成规划、执行和重规划各一次，规划=%d 执行=%d 重规划=%d 终答=%v err=%v",
			planned, executed, replanned, final, err)
	}
	if plannerNS != interruptedPlannerNS {
		t.Fatalf("续跑应使用原规划器存档，原 namespace %q，续跑 %q", interruptedPlannerNS, plannerNS)
	}
	if snapshots, err := cp.LoadAllInSession(ctx, session.RunID); err != nil || len(snapshots) != 0 {
		t.Fatalf("完成后应清理整棵存档子树，剩余 %d 份，err=%v", len(snapshots), err)
	}
}

func TestWebPlanner_ReplanAfterPauseAndResumeNewQuestion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithDurabilityCtx(compose_v2.WithCheckpointerCtx(ctx, cp), compose_v2.DurabilitySync)
	const task = "审计年度费用"
	const direction = "补做预算抽查"
	const confirmation = "批准抽查"
	const result = "预算抽查完毕"
	var pauseRequested bool
	var originalNS, directedNS string
	var originalPlans, directedPlans, replans, executions int

	pauser := &interruptHook{requested: func(context.Context) (bool, error) { return pauseRequested, nil }}
	planner := agent_v2.NewBasicAgent(agent_v2.WithName("budget_planner"), agent_v2.WithHooks(pauser),
		agent_v2.WithModel(plannerTestModel(func(ctx context.Context, messages []*schema.Message) (*schema.Message, error) {
			if len(messages) == 0 || !strings.Contains(messages[len(messages)-1].Content, task) {
				return nil, fmt.Errorf("planner did not receive the task")
			}
			prompt := messages[len(messages)-1].Content
			switch {
			case strings.Contains(prompt, result):
				replans++
				return schema.AssistantMessage("[]"), nil
			case strings.Contains(prompt, direction):
				directedNS = compose_v2.GetGraphNS(ctx)
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准预算抽查？"})
				if err != nil {
					return nil, err
				}
				if answer == nil || answer.Content != confirmation {
					return nil, fmt.Errorf("planner did not receive the approval")
				}
				directedPlans++
				return schema.AssistantMessage(`[{"desc":"预算抽查","agent":"reviewer"}]`), nil
			default:
				originalNS = compose_v2.GetGraphNS(ctx)
				originalPlans++
				pauseRequested = true
				return schema.AssistantMessage("[]"), nil
			}
		})))
	reviewer := agent_v2.NewAgentFunc("reviewer", "审计年度费用", func(_ context.Context, input string) (string, error) {
		if !strings.Contains(input, "预算抽查") {
			return "", fmt.Errorf("reviewer did not receive the revised step")
		}
		executions++
		return result, nil
	})
	workers := map[string]agent_v2.Agent{"reviewer": reviewer}
	sink := newEventSink()
	defer sink.close()
	plan := agent_v2.NewPlanAgent(newWebPlanner(planner, sink, workers),
		agent_v2.PlanName("budget"), agent_v2.WithSubAgents(workers))
	session := agent_v2.NewRunSession()
	_, err := plan.Run(ctx, session, schema.UserMessage(task))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || pause.Query == nil || pause.Query.InterruptID != pauseInterruptID || originalPlans != 1 || executions != 0 {
		t.Fatalf("应在原计划生成后暂停，原计划 %d 次，执行 %d 次，err=%v", originalPlans, executions, err)
	}

	pauseRequested = false
	_, err = plan.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session),
		agent_v2.WithResumeAction(agent_v2.ResumeActionReplan), agent_v2.WithResumeUserText(direction),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{pause.Query.InterruptID: schema.UserMessage(direction)}))
	question, ok := compose_v2.IsInterrupt(err)
	if !ok || question.Query == nil || question.Query.Question != "批准预算抽查？" || executions != 0 {
		t.Fatalf("换方向后应在执行前发出新问题，执行 %d 次，err=%v", executions, err)
	}
	if directedNS == "" || directedNS == originalNS {
		t.Fatalf("新方向应使用独立存档，原 namespace %q，新 namespace %q", originalNS, directedNS)
	}

	pendingNS := directedNS
	events, err := plan.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session),
		agent_v2.WithResumeUserText(confirmation),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{question.Query.InterruptID: schema.UserMessage(confirmation)}))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || !strings.Contains(final.Content, result) || originalPlans != 1 || directedPlans != 1 || replans != 1 || executions != 1 {
		t.Fatalf("答复后应执行新计划一次，原规划=%d 新规划=%d 执行=%d 重规划=%d 终答=%v err=%v",
			originalPlans, directedPlans, executions, replans, final, err)
	}
	if directedNS != pendingNS {
		t.Fatalf("答复应返回新问题所在的 namespace，原值 %q，续跑 %q", pendingNS, directedNS)
	}
	if snapshots, err := cp.LoadAllInSession(ctx, session.RunID); err != nil || len(snapshots) != 0 {
		t.Fatalf("完成后应清理原计划及新计划的存档，剩余 %d 份，err=%v", len(snapshots), err)
	}
}
