package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// PlanAgent 的 TmpSubAgent 配置随这一步进 planCurrentCh、跟着存档走（见 planExecStep.TmpSubAgent）：续跑从存档
// 读回，不靠 PlanAgent 实例上的状态。
//
// 下面第一条：出口写根档撞上陈旧写回（槽被另一个执行推进、不属于这次执行）之后，槽里那份（另一个执行的）挂起
// 档照样续得上。配置放在实例上的一张表里时，这次退出曾经把它清掉，那个执行续跑时报 no config for description
// （审计实测）。陈旧写回用后端返回 ErrCheckpointStale 模拟（真实形态是同一任务两次并发续跑、后写者被世代守卫
// 拦下）。
type planStaleRootCP struct {
	*compose_v2.InMemoryCheckpointer
	failRoot atomic.Bool
	mu       sync.Mutex
	saved    []string
}

func (c *planStaleRootCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	c.mu.Lock()
	c.saved = append(c.saved, ns)
	c.mu.Unlock()
	if c.failRoot.Load() && ns == "" {
		return fmt.Errorf("simulated concurrent advance: %w", compose_v2.ErrCheckpointStale)
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func TestPlan_TmpConfigSurvivesStaleRootSave(t *testing.T) {
	var askCalls atomic.Int32
	askTool := tool.NewFuncTool(
		"ask",
		"ask user",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			askCalls.Add(1)
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_plan_stale"); ok {
				return tool.Success("answered: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_plan_stale", Question: "zz_q?"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)
	tc := schema.ToolCall{ID: "tc_zz", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"zz"}`}}
	stub := newStubModel([]*schema.Message{schema.AssistantToolCallMessage([]schema.ToolCall{tc}), schema.AssistantMessage("zz done")})
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{
				Description: "ask user plan_stale",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "plan_stale_tmp", System: "asker", ToolNames: []string{"ask"}},
			}}, "", nil
		}
		return nil, "completed", nil
	}
	dummyExec := agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(stub))
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("plan_stale"),
		agent_v2.WithPlanModel(stub),
		agent_v2.WithPlanToolkit(tk),
	).SetExecutor(dummyExec)

	cper := &planStaleRootCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// 1. 首跑：TmpSubAgent 的 ask 中断，存档正常落盘（这就是「另一个执行」等着被续的那份）
	_, err := pa.Run(ctx, sess, schema.UserMessage("zz task"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("首跑应中断：%v", err)
	}
	t.Logf("首跑写过的 ns：%q", cper.saved)

	// 2. 一次并发的续跑（不带答案，照样再问一遍）在出口写根档时撞上陈旧写回
	cper.failRoot.Store(true)
	_, err2 := pa.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	cper.failRoot.Store(false)
	_, isInt2 := agent_v2.IsInterrupt(err2)
	t.Logf("陈旧续跑：isInterrupt=%v stale=%v aborted=%v\nerr=%v", isInt2,
		errors.Is(err2, compose_v2.ErrCheckpointStale), errors.Is(err2, compose_v2.ErrRunAborted), err2)
	if ok, _ := cper.ExistsNS(context.Background(), sess.RunID, ""); !ok {
		t.Fatal("前置：根槽里那份挂起档应还在（槽属于别人，不删）")
	}

	// 3. 正当的那个执行带着答案续跑它的挂起档
	events, err3 := pa.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("yes")}),
	)
	t.Logf("正当续跑：err=%v askCalls=%d", err3, askCalls.Load())
	if err3 != nil {
		if strings.Contains(err3.Error(), "no config for description") {
			t.Fatalf("临时子 agent 的配置丢了，正当的挂起档续不上：%v", err3)
		}
		t.Fatalf("正当续跑失败：%v", err3)
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content == "" {
		t.Fatalf("正当续跑应产出 final")
	}
}

// 换一个 PlanAgent 实例续跑（进程重启之后就是这样）：临时子 agent 的配置从存档读回，续跑照常完成，规划器不重跑
// 这一步。配置放在实例上的表里时，新实例的表是空的，续跑报 no config for description。
func TestPlan_TmpConfigResumesOnFreshInstance(t *testing.T) {
	newAsk := func(calls *atomic.Int32) *tool.Toolkit {
		askTool := tool.NewFuncTool(
			"ask",
			"ask user",
			map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				calls.Add(1)
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
				if err != nil {
					return nil, err
				}
				return tool.Success("answered: " + ans.Content), nil
			},
		)
		tk := tool.NewToolkit()
		tk.Register(askTool)
		return tk
	}
	build := func(planner agent_v2.Planner, stub *stubModel, tk *tool.Toolkit) *agent_v2.PlanAgent {
		return agent_v2.NewPlanAgent(planner,
			agent_v2.PlanName("plan_tmp_restart"),
			agent_v2.WithPlanModel(stub),
			agent_v2.WithPlanToolkit(tk),
		).SetExecutor(agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(stub)))
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// 1. 第一个实例：规划出临时子 agent 那一步，子 agent 的工具问用户
	var calls1 atomic.Int32
	tc := schema.ToolCall{ID: "tc_restart", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"x"}`}}
	first := build(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{
				Description: "问一下用户",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "plan_restart_tmp", System: "asker", ToolNames: []string{"ask"}},
			}}, "", nil
		}
		return nil, "completed", nil
	}, newStubModel([]*schema.Message{schema.AssistantToolCallMessage([]schema.ToolCall{tc})}), newAsk(&calls1))
	_, err := first.Run(ctx, sess, schema.UserMessage("任务"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("前置：首跑应停在子 agent 的提问：%v", err)
	}

	// 2. 第二个实例（规划器要是被叫去重新规划这一步就报错）带着答案续跑
	var calls2 atomic.Int32
	second := build(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return nil, "", errors.New("续跑不该重新规划已经派出去的那一步")
		}
		return nil, "completed", nil
	}, newStubModel([]*schema.Message{schema.AssistantMessage("问完了")}), newAsk(&calls2))
	events, err := second.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("批准")}))
	if err != nil {
		if strings.Contains(err.Error(), "no config for description") {
			t.Fatalf("临时子 agent 的配置应从存档读回，新实例上没有那张表：%v", err)
		}
		t.Fatalf("续跑应完成：%v", err)
	}
	if calls2.Load() != 1 {
		t.Fatalf("续跑时子 agent 的工具应执行 1 次，实际 %d 次", calls2.Load())
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content == "" {
		t.Fatal("续跑应产出 final")
	}
}
