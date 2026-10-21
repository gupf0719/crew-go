package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 本文件补 PlanAgent 3 个审计发现的覆盖缺口——删 compose_v2 plan 测试 时带走的、
// 实现有防御但 agent_v2 黑盒未测的路径。每个测试都用真实 NewPlanAgent + 真实
// BasicAgent sub-agent（mock model.ChatModel），不闭包绕过 Planner 默认路径。

// TestPlanGap_IllegalSubAgentName 验 GAP1 — Planner 返回未注册的 SubAgentName 时
// plan 节点 fail-fast 报 "not registered"（实现 plan_agent.go:707）。
//
// 现有 PlanAgent 测试只覆盖**已注册** name dispatch（plan_subagent_test.go
// TestPlan_AddSubAgent_Fluent）或 NS 校验 panic（issue85 MED-4），没有覆盖
// "Planner 输出 name 不在 AddSubAgent 注册集"的运行期路径。
//
// **sanity-revert**: 注释掉 plan_agent.go:705-708 的 `if _, ok := p.subAgents[...]; !ok`
// not-registered 检查 → plan 节点直接 `target = NodeID("未注册名")` → conditional edge
// 路由到 unknown worker 时 compilePlanGraph 的 `allowedWorkers[target]` false →
// 报 "plan: target ... not registered as worker"（plan_builder.go:114）。**实跑结果**：
// revert 后测试仍 PASS——错误信息仍含 "not registered"（来自 conditional edge 兜底而非
// plan 节点）。改用 plan_agent.go 专属字符串断言（含 PlanAgent name "p" 前缀 + SubAgentName）
// 区分两条路径，revert 后实测 fail（详见报告）。
func TestPlanGap_IllegalSubAgentName(t *testing.T) {
	// 真实 BasicAgent sub-agent，注册名 "researcher"
	res := agent_v2.NewBasicAgent(agent_v2.WithName("res"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("research done")})))

	planner := func(_ context.Context, _ *agent_v2.RunSession, input string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		// 读 input 做断言（mock 不忽略输入）
		if input == "" {
			t.Errorf("planner 应收到非空 original input")
		}
		// 返回未注册的 SubAgentName（"writer" 不在注册集，只注册了 "researcher"）
		return []agent_v2.PlanStep{{Description: "write report", SubAgentName: "writer"}}, "", nil
	}

	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p")).
		AddSubAgent("researcher", res)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("do task"))
	if err == nil {
		t.Fatal("GAP1: Planner 返回未注册 SubAgentName 应返错, got nil")
	}
	// plan_agent.go:707 专属字符串: `plan agent "p": SubAgentName "writer" not registered`
	// 同时断言 PlanAgent name + 具体 SubAgentName 区分于 conditional edge 兜底
	// (`plan: target "writer" not registered as worker`)。
	if !strings.Contains(err.Error(), `SubAgentName "writer" not registered`) {
		t.Fatalf("GAP1: 期望错误含 `SubAgentName \"writer\" not registered` (plan_agent.go:707), got %q\n"+
			"sanity-revert: 注释掉 plan_agent.go:705-708 not-registered 检查 → 错误来自 conditional edge 兜底 "+
			"(`plan: target \"writer\" not registered as worker`) 不含 SubAgentName 字样 → 此断言 fail", err.Error())
	}
}

// TestPlanGap_MaxStepsDefense 验 GAP2 — WithPlanMaxSteps 低上限 + Planner 永不终止时
// Run 触顶返错（实现 plan_builder.go:79 WithMaxVisit(maxSteps)；触顶在 graph_loop
// 抛 compose_v2.MaxIterationsError）。
//
// 现有 WithPlanMaxSteps 引用（issue82_onagentend_single_trigger_test.go）是 hook 触发
// 计数，没有覆盖"maxSteps 防 Planner 死循环"的触顶路径。
//
// **sanity-revert**: 把 plan_builder.go:79 `WithMaxVisit(maxSteps)` 改成传死大值（如
// `WithMaxVisit(100000)`）忽略低 maxSteps → 永不终止的 Planner 会一直 plan↔exec →
// 实测触发 compose_v2 默认 maxVisit 兜底（仍报 visit/iterations）但远晚于 maxSteps=3，
// 或测试因 stub 耗尽进入兜底响应循环但 visit 上限被抬高 → 在合理范围内不再触顶报错 →
// 测试 fail（详见报告 revert 实测）。
func TestPlanGap_MaxStepsDefense(t *testing.T) {
	// 真实 BasicAgent sub-agent 每次返可验证输出
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("w"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("step output")})))

	// Planner 永不终止：每次都返非空 remaining（同一步无限重复）。
	var plannerCalls int
	planner := func(_ context.Context, _ *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		plannerCalls++
		if input == "" {
			t.Errorf("planner 应收到非空 original input")
		}
		// 安全阀：plannerCalls 不该被调超过 maxSteps 太多次——若死循环没被 maxSteps 兜住，
		// 这里硬上限防测试本身挂死。
		if plannerCalls > 50 {
			t.Fatalf("GAP2: Planner 被调 %d 次仍未触顶——maxSteps 防御失效（死循环未被兜住）", plannerCalls)
		}
		return []agent_v2.PlanStep{{Description: "loop step", SubAgentName: "w"}}, "", nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanMaxSteps(2), // 低上限
	).AddSubAgent("w", worker)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("never ending task"))
	if err == nil {
		t.Fatal("GAP2: maxSteps=2 + 永不终止 Planner 应触顶返错, got nil")
	}
	// 触顶抛 MaxIterationsError（可 errors.As 解析）或错误含 "visit"。
	var maxErr *compose_v2.MaxIterationsError
	hitMaxIter := errors.As(err, &maxErr)
	hasVisit := strings.Contains(err.Error(), "visit") || strings.Contains(err.Error(), "iteration")
	if !hitMaxIter && !hasVisit {
		t.Fatalf("GAP2: 期望触顶错误 (MaxIterationsError 或含 visit/iteration), got %q\n"+
			"sanity-revert: plan_builder.go:79 WithMaxVisit(maxSteps) 改传死大值忽略低上限 → "+
			"低 maxSteps=2 不再约束 → 在合理范围内不触顶 → 此断言 fail", err.Error())
	}
}

// TestPlanGap_FinalAnswerFallback 验 GAP3 — Planner 第一步执行后第二次返**空 remaining +
// 空 finalAnswer** 时最终答复走 planFinalAnswer fallback（实现 plan_builder.go:140-145：
// "计划已完成" 开头 + "步骤 N (agent名): result"）。
//
// 现有测试（plan_subagent_test.go）的 Planner 终止时都给非空 finalAnswer（"completed"
// / "all done" / "tmp completed"），走的是 planAnswerCh 非空分支（plan_builder.go:133），
// 从未覆盖 finalAnswer 空 → executed 拼接 fallback 路径。
//
// **sanity-revert**: 把 plan_builder.go:140-147 fallback 分支改成 `return ""`（直接返空
// string 不拼 executed）→ FinalMessage(events).Content == "" → 断言 "计划已完成" + "步骤 1"
// 全 fail（详见报告 revert 实测）。
func TestPlanGap_FinalAnswerFallback(t *testing.T) {
	// 真实 BasicAgent sub-agent 返可验证输出（用于断言 fallback 拼接含此 result）
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker_agent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("WORKER_RESULT_42")})))

	planner := func(_ context.Context, _ *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if input == "" {
			t.Errorf("planner 应收到非空 original input")
		}
		if len(executed) == 0 {
			// 第一次：返 1 步
			return []agent_v2.PlanStep{{Description: "single step", SubAgentName: "worker_agent"}}, "", nil
		}
		// 第二次（执行后）：空 remaining + **空 finalAnswer** → 触发 fallback
		return nil, "", nil
	}

	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p")).
		AddSubAgent("worker_agent", worker)

	events, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("do one step"))
	if err != nil {
		t.Fatalf("GAP3: Run err: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("GAP3: 期望 fallback final message, got nil")
	}
	// fallback 格式: "计划已完成。\n\n步骤 1 (worker_agent): WORKER_RESULT_42"
	if !strings.HasPrefix(final.Content, "计划已完成") {
		t.Errorf("GAP3: 期望 fallback 以 '计划已完成' 开头, got %q\n"+
			"sanity-revert: plan_builder.go:140-147 fallback 改 `return \"\"` → Content 空 → fail", final.Content)
	}
	// 断言 fallback 拼接含真实步骤序号 + agent 名 + sub-agent 结果（mock 输出 load-bearing）
	if !strings.Contains(final.Content, "步骤 1 (worker_agent): WORKER_RESULT_42") {
		t.Errorf("GAP3: 期望 fallback 含 '步骤 1 (worker_agent): WORKER_RESULT_42', got %q\n"+
			"sanity-revert: plan_builder.go fallback 改 `return \"\"` → 不拼 executed → fail", final.Content)
	}
}
