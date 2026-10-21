package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestReviewV6_H04_TmpSubAgentHITLResume 验 review-v6 H-04 fix —
// PlanAgent TmpSubAgent step 内抛 InterruptError 后 Resume 能找到 cfg 重跑 dispatcher,
// 不报 "no config for description X" 死循环.
//
// 历史: 配置曾放在 PlanAgent 实例上的一张表里，Run defer 清表的时机跟「续跑要读它」互相打架（R32+
// HIGH-2/HIGH-3、review-v6 H-04 来回修过三次）。现在配置随这一步进 planCurrentCh、跟着存档走
// （planExecStep.TmpSubAgent），没有要清的东西。
//
// sanity-revert: 让 plan 节点不把配置写进 planCurrentCh → Resume 报 "no config for description" → fail.
func TestReviewV6_H04_TmpSubAgentHITLResume(t *testing.T) {
	// ask_user tool: 第 1 次抛 Interrupt, 第 2 次 (Resume 后) 拿答案返成功
	askTool := tool.NewFuncTool(
		"ask",
		"ask user",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_h04"); ok {
				return tool.Success("answered: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_h04", Question: "h04_q?"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)

	// stub model 让 TmpSubAgent (动态构造的 BasicAgent) 第 1 轮 emit tool_call ask,
	// 第 2 轮 (Resume 后) 看 ask result emit final assistant
	tc := schema.ToolCall{ID: "tc_h04", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"h04_q"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	resp2 := schema.AssistantMessage("h04 done")
	stub := newStubModel([]*schema.Message{resp1, resp2})

	// PlanAgent: planner 第 1 步返 TmpSubAgent (含 ask tool), 第 2 步 (Resume 完成) 返 done
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{
				Description: "ask user about h04",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{
					Name:      "h04_tmp",
					System:    "you are h04 asker",
					ToolNames: []string{"ask"},
				},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	// PlanAgent 构造时要求至少 1 sub-agent / executor (TmpSubAgent 不算), 加 dummy.
	dummyExec := agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(stub))
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("h04_plan"),
		agent_v2.WithPlanModel(stub), // TmpSubAgent 默认用 PlanModel
		agent_v2.WithPlanToolkit(tk), // TmpSubAgent.ToolNames 从此 toolkit 筛
	).SetExecutor(dummyExec)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: TmpSubAgent 内 ask tool 抛 Interrupt
	_, err := pa.Run(ctx, sess, schema.UserMessage("h04 task"))
	if err == nil {
		t.Fatal("Run 1 期望 Interrupt")
	}
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 期望 InterruptError, got: %v", err)
	}

	// Resume: 用户答 + dispatcher 从存档读回配置 → 不应报 "no config for description"
	events, err := pa.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("user_answer_h04"),
		}),
	)
	if err != nil {
		// review-v6 H-04 fix 前期望 Resume 报 "no config for description X" 死循环
		if strings.Contains(err.Error(), "no config for description") {
			t.Fatalf("review-v6 H-04: Resume 报 'no config' — 临时子 agent 的配置没随存档走\n"+
				"err: %v", err)
		}
		t.Fatalf("Resume err: %v", err)
	}

	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("Resume no final message")
	}
	// H-04 fix 核心断言: Resume 没死循环且产出 final message. final 内容由 planner
	// 第 2 步返的 finalAnswer "completed" 决定 — TmpSubAgent dispatcher 成功 Load
	// 配置 + 跑完 ask tool Resume 后 planner 才能进入 case completed 路径.
	if final.Content == "" {
		t.Errorf("review-v6 H-04 fix: Resume final 应非空 (TmpSubAgent dispatcher 读回了配置), 实际空")
	}
}
