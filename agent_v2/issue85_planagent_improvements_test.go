package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestISSUE85_MED4_AddSubAgent_NSValidation 验 MED-4 fix — AddSubAgent 注册时
// fail-fast panic 含 NS 禁止字符 (`/` `#` 空字符串)。
func TestISSUE85_MED4_AddSubAgent_NSValidation(t *testing.T) {
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "done", nil
	}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("ok")})
	exec := agent_v2.NewBasicAgent(agent_v2.WithName("e"), agent_v2.WithModel(stub))

	cases := []struct {
		name        string
		invalidName string
	}{
		{"slash 字符", "a/b"},
		{"hash 字符", "a#0"},
		{"空字符串", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("ISSUE-85 MED-4 fix: AddSubAgent(%q) 应 panic NS 禁止字符, got 无 panic", c.invalidName)
				}
			}()
			pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p"))
			pa.AddSubAgent(c.invalidName, exec)
		})
	}
}

// TestISSUE85_MED4_AddSubAgent_DuplicatePanic 验 MED-4 fix — 重复注册同 name
// panic 防 silent overwrite。
func TestISSUE85_MED4_AddSubAgent_DuplicatePanic(t *testing.T) {
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "done", nil
	}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("ok")})
	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub))

	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p")).AddSubAgent("foo", a1)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("ISSUE-85 MED-4 fix: AddSubAgent 重复注册同 name 应 panic 防 silent overwrite, got 无 panic")
		} else if !strings.Contains(r.(string), "already registered") {
			t.Errorf("panic 信息应含 'already registered', got %v", r)
		}
	}()
	pa.AddSubAgent("foo", a2)
}

// TestISSUE85_MED3_ExecInputBuilderSeesFullExecuted 验 MED-3 fix — ExecInputBuilder
// 收到的 executed 是全部历史不是 InputRefs 子集。
//
// **ISSUE-85 audit HIGH-1 fix**: 之前测试只覆盖 SubAgentName 路径, TmpSubAgent
// dispatcher 漏修同款 ISSUE-85 MED-3 (commit 谎报"3 处都改"实际只 2 处)。
// 加 TestISSUE85_MED3_ExecInputBuilderSeesFullExecuted_TmpSubAgent + executor
// 子测试覆盖 3 路径全协议对称。
func TestISSUE85_MED3_ExecInputBuilderSeesFullExecuted(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("step done")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub))
	a3 := agent_v2.NewBasicAgent(agent_v2.WithName("a3"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "step1", SubAgentName: "a1"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "step2", SubAgentName: "a2"}}, "", nil
		case 2:
			// step 3 不引用任何前置 (InputRefs=nil) — 但 ExecInputBuilder 应该看到全 2 个历史步
			return []agent_v2.PlanStep{{Description: "step3", SubAgentName: "a3"}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	// 自定义 ExecInputBuilder 校验 executed 含全历史
	var executedLensAtCalls []int
	custom := func(_ context.Context, original string, executed []agent_v2.ExecutedStep, current agent_v2.PlanStep, resolvedRefs []int) (*schema.Message, error) {
		executedLensAtCalls = append(executedLensAtCalls, len(executed))
		return schema.UserMessage(current.Description), nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithExecInputBuilder(custom),
	).AddSubAgent("a1", a1).AddSubAgent("a2", a2).AddSubAgent("a3", a3)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	// 期望: step1 调时 executed=0; step2 调时 executed=1; step3 调时 executed=2
	want := []int{0, 1, 2}
	if len(executedLensAtCalls) != len(want) {
		t.Fatalf("ExecInputBuilder 应被调 3 次, got %d (lens=%v)", len(executedLensAtCalls), executedLensAtCalls)
	}
	for i, w := range want {
		if executedLensAtCalls[i] != w {
			t.Errorf("ISSUE-85 MED-3 fix: ExecInputBuilder 第 %d 次调 executed.len 应是 %d (全历史), got %d (sanity-revert: 改回 highExecuted=refs 子集 → 默认 InputRefs=nil 时 len 永远 0)",
				i, w, executedLensAtCalls[i])
		}
	}
}

// TestISSUE85_MED3_ExecInputBuilderSeesFullExecuted_TmpSubAgent 验 ISSUE-85 audit
// HIGH-1 fix — TmpSubAgent dispatcher 路径同款覆盖 (之前 commit 谎报漏修)。
//
// 业务用 PlanStep.TmpSubAgent (动态构造 BasicAgent) 时 ExecInputBuilder 应同样
// 收到全历史 executed。
func TestISSUE85_MED3_ExecInputBuilderSeesFullExecuted_TmpSubAgent(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("step done")})

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{
				Description: "step1",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp1", System: "sys1"},
			}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{
				Description: "step2",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp2", System: "sys2"},
			}}, "", nil
		case 2:
			return []agent_v2.PlanStep{{
				Description: "step3",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp3", System: "sys3"},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var executedLensAtCalls []int
	custom := func(_ context.Context, original string, executed []agent_v2.ExecutedStep, current agent_v2.PlanStep, resolvedRefs []int) (*schema.Message, error) {
		executedLensAtCalls = append(executedLensAtCalls, len(executed))
		return schema.UserMessage(current.Description), nil
	}

	// PlanAgent 需要至少 1 个 sub-agent 通过 graph 校验 — 加 dummy 但实际走 TmpSubAgent 路径
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

	want := []int{0, 1, 2}
	if len(executedLensAtCalls) != len(want) {
		t.Fatalf("ExecInputBuilder 应被调 3 次 (TmpSubAgent 路径), got %d", len(executedLensAtCalls))
	}
	for i, w := range want {
		if executedLensAtCalls[i] != w {
			t.Errorf("ISSUE-85 audit HIGH-1 fix: TmpSubAgent dispatcher 第 %d 次调 executed.len 应是 %d (全历史), got %d\n"+
				"sanity-revert: 改回 TmpSubAgent dispatcher highExecuted=refs 子集 → executed.len 永远 0",
				i, w, executedLensAtCalls[i])
		}
	}
}

// TestISSUE85_HIGH2_ResolvedRefsReverseLookup 验 ISSUE-85 audit HIGH-2 fix —
// (Step, Agent) 不是唯一 key 时反向映射应**倒序找最近**, 与高层 resolveRefs
// AgentRef 倒序逻辑对称, 防业务 reuse 同 step 时取错位。
//
// 场景: review by reviewer 在 step 0 + step 2 重跑, AgentRef("reviewer")
// 倒序找最近 idx=2, agent_v2 worker wrapper 也应倒序 idx=2 (而非正序 idx=0)。
func TestISSUE85_HIGH2_ResolvedRefsReverseLookup(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("first review"),
		schema.AssistantMessage("edit done"),
		schema.AssistantMessage("second review"),
		schema.AssistantMessage("finalize done"),
	})
	reviewer := agent_v2.NewBasicAgent(agent_v2.WithName("reviewer"), agent_v2.WithModel(stub))
	editor := agent_v2.NewBasicAgent(agent_v2.WithName("editor"), agent_v2.WithModel(stub))
	finalizer := agent_v2.NewBasicAgent(agent_v2.WithName("finalizer"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "review", SubAgentName: "reviewer"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "edit", SubAgentName: "editor"}}, "", nil
		case 2:
			// 重跑 review (同 step desc + 同 agent)
			return []agent_v2.PlanStep{{Description: "review", SubAgentName: "reviewer"}}, "", nil
		case 3:
			// finalize 引用最近 reviewer 结果 (期望 second_review 不是 first_review)
			return []agent_v2.PlanStep{{
				Description:  "finalize",
				SubAgentName: "finalizer",
				InputRefs:    []agent_v2.RefSpec{agent_v2.AgentRef("reviewer")},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	// 验 finalize 步收到的 resolvedRefs 指向 second_review (idx=2)
	var capturedResolvedRefs []int
	custom := func(_ context.Context, original string, executed []agent_v2.ExecutedStep, current agent_v2.PlanStep, resolvedRefs []int) (*schema.Message, error) {
		if current.Description == "finalize" {
			capturedResolvedRefs = resolvedRefs
		}
		return schema.UserMessage(current.Description), nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithExecInputBuilder(custom),
	).AddSubAgent("reviewer", reviewer).AddSubAgent("editor", editor).AddSubAgent("finalizer", finalizer)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	// 期望 resolvedRefs[0] == 2 (倒序找最近 review by reviewer = idx 2)
	if len(capturedResolvedRefs) != 1 || capturedResolvedRefs[0] != 2 {
		t.Errorf("ISSUE-85 audit HIGH-2 fix: AgentRef('reviewer') 应倒序找最近 idx=2, got %v\n"+
			"sanity-revert: 改回 `for j := 0; j < len(allExecuted); j++` 正序 → resolvedRefs[0]=0 (first_review 错位)",
			capturedResolvedRefs)
	}
}

// TestISSUE85_AuditFollowup_HIGH1_IndexRefPreservesExplicit 验 audit follow-up HIGH-1 fix —
// 业务用 IndexRef(0) 显式选 step 0 时, worker 内 resolvedRefs 应严格保留 [0],
// **不应**因 worker 内倒序 (Step, Agent) lookup 错位到最近 idx=N。
//
// 之前 ISSUE-85 audit HIGH-2 fix 在 worker 内做"倒序找 (Step, Agent)" lookup,
// 业务 IndexRef(0) 显式语义被悄悄改写到"最近一个同 (Step, Agent)" — regression。
//
// **sanity-revert**: 把 worker 内 `resolvedRefs := append([]int(nil), current.InputRefs...)`
// 改回原倒序 lookup → resolvedRefs[0] = 2 (second_review 错位) 测试 fail。
func TestISSUE85_AuditFollowup_HIGH1_IndexRefPreservesExplicit(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("first review"),
		schema.AssistantMessage("edit done"),
		schema.AssistantMessage("second review"),
		schema.AssistantMessage("finalize done"),
	})
	reviewer := agent_v2.NewBasicAgent(agent_v2.WithName("reviewer"), agent_v2.WithModel(stub))
	editor := agent_v2.NewBasicAgent(agent_v2.WithName("editor"), agent_v2.WithModel(stub))
	finalizer := agent_v2.NewBasicAgent(agent_v2.WithName("finalizer"), agent_v2.WithModel(stub))

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "review", SubAgentName: "reviewer"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "edit", SubAgentName: "editor"}}, "", nil
		case 2:
			// 重跑 review (同 step desc + 同 agent) — 与 reverseLookup 测试同 fixture
			return []agent_v2.PlanStep{{Description: "review", SubAgentName: "reviewer"}}, "", nil
		case 3:
			// **关键**: 用 IndexRef(0) 显式选 first_review (idx=0), 不用 AgentRef
			return []agent_v2.PlanStep{{
				Description:  "finalize",
				SubAgentName: "finalizer",
				InputRefs:    []agent_v2.RefSpec{agent_v2.IndexRef(0)},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var capturedResolvedRefs []int
	custom := func(_ context.Context, original string, executed []agent_v2.ExecutedStep, current agent_v2.PlanStep, resolvedRefs []int) (*schema.Message, error) {
		if current.Description == "finalize" {
			capturedResolvedRefs = resolvedRefs
		}
		return schema.UserMessage(current.Description), nil
	}

	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithExecInputBuilder(custom),
	).AddSubAgent("reviewer", reviewer).AddSubAgent("editor", editor).AddSubAgent("finalizer", finalizer)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	// 期望 resolvedRefs[0] == 0 (IndexRef(0) 显式语义保留)
	if len(capturedResolvedRefs) != 1 || capturedResolvedRefs[0] != 0 {
		t.Errorf("audit follow-up HIGH-1 fix: IndexRef(0) 显式语义应保留, 期望 [0], got %v\n"+
			"sanity-revert: worker 内改回 `for j := len(allExecuted)-1; j>=0; j--` 倒序 lookup → resolvedRefs[0]=2 (second_review 错位)",
			capturedResolvedRefs)
	}
}

// TestISSUE85_AuditFollowup²_HIGH1_IndexRefPreservesExplicit_TmpSubAgent 验
// IndexRef 显式语义保留在 **TmpSubAgent dispatcher** 路径 (audit² 视角 A HIGH-1
// 揭露原 SubAgentName 路径测试不能区分 fix 在 vs revert 因 3 worker 任意 1 改对
// 测试就 PASS — 5 类炸弹 #4 双层防护遮蔽变体)。
//
// **sanity-revert**: 单独 revert TmpSubAgent dispatcher (plan_agent.go:566-572)
// IndexRef fix → 测试 fail (resolvedRefs[0]=2 second_review 错位 vs 期望 [0])。
func TestISSUE85_AuditFollowup2_HIGH1_IndexRefPreservesExplicit_TmpSubAgent(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("first review"),
		schema.AssistantMessage("edit done"),
		schema.AssistantMessage("second review"),
		schema.AssistantMessage("finalize done"),
	})

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{
				Description: "review",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "reviewer", System: "review"},
			}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{
				Description: "edit",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "editor", System: "edit"},
			}}, "", nil
		case 2:
			// 重跑 review (同 step desc + 同 agent — TmpSubAgent.Name 决定 ExecutedStep.Agent)
			return []agent_v2.PlanStep{{
				Description: "review",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "reviewer", System: "review"},
			}}, "", nil
		case 3:
			// 关键: IndexRef(0) 显式选 first_review (idx=0) — TmpSubAgent dispatcher 应保留
			return []agent_v2.PlanStep{{
				Description: "finalize",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "finalizer", System: "finalize"},
				InputRefs:   []agent_v2.RefSpec{agent_v2.IndexRef(0)},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var capturedResolvedRefs []int
	custom := func(_ context.Context, original string, executed []agent_v2.ExecutedStep, current agent_v2.PlanStep, resolvedRefs []int) (*schema.Message, error) {
		if current.Description == "finalize" {
			capturedResolvedRefs = resolvedRefs
		}
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

	if len(capturedResolvedRefs) != 1 || capturedResolvedRefs[0] != 0 {
		t.Errorf("audit follow-up² HIGH-1 fix (TmpSubAgent dispatcher): IndexRef(0) 显式语义应保留 [0], got %v\n"+
			"sanity-revert: TmpSubAgent dispatcher (plan_agent.go:566) 改回倒序 lookup → resolvedRefs[0]=2",
			capturedResolvedRefs)
	}
}

// TestISSUE85_AuditFollowup²_HIGH1_IndexRefPreservesExplicit_Executor 验 IndexRef
// 显式语义保留在 **executor** worker 路径 (与 SubAgentName / TmpSubAgent 三 worker 对称)。
//
// **sanity-revert**: 单独 revert executor worker (plan_agent.go:609-613) IndexRef
// fix → 测试 fail (resolvedRefs[0]=2 错位 vs 期望 [0])。
func TestISSUE85_AuditFollowup2_HIGH1_IndexRefPreservesExplicit_Executor(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("first review"),
		schema.AssistantMessage("edit done"),
		schema.AssistantMessage("second review"),
		schema.AssistantMessage("finalize done"),
	})

	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			// SubAgentName 留空 + TmpSubAgent nil → 走 executor worker
			return []agent_v2.PlanStep{{Description: "review"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "edit"}}, "", nil
		case 2:
			// 重跑 review (同 step desc — Agent 是 executor 名)
			return []agent_v2.PlanStep{{Description: "review"}}, "", nil
		case 3:
			return []agent_v2.PlanStep{{
				Description: "finalize",
				InputRefs:   []agent_v2.RefSpec{agent_v2.IndexRef(0)},
			}}, "", nil
		default:
			return nil, "completed", nil
		}
	}

	var capturedResolvedRefs []int
	custom := func(_ context.Context, original string, executed []agent_v2.ExecutedStep, current agent_v2.PlanStep, resolvedRefs []int) (*schema.Message, error) {
		if current.Description == "finalize" {
			capturedResolvedRefs = resolvedRefs
		}
		return schema.UserMessage(current.Description), nil
	}

	exec := agent_v2.NewBasicAgent(agent_v2.WithName("exec"), agent_v2.WithModel(stub))
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("p"),
		agent_v2.WithPlanModel(stub),
		agent_v2.WithExecInputBuilder(custom),
	).SetExecutor(exec)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	if len(capturedResolvedRefs) != 1 || capturedResolvedRefs[0] != 0 {
		t.Errorf("audit follow-up² HIGH-1 fix (executor worker): IndexRef(0) 显式语义应保留 [0], got %v\n"+
			"sanity-revert: executor worker (plan_agent.go:609) 改回倒序 lookup → resolvedRefs[0]=2",
			capturedResolvedRefs)
	}
}

// TestISSUE85_AuditFollowup_HIGH2_WithSubAgentsMergesNotOverwrite 验 audit follow-up HIGH-2 fix —
// WithSubAgents 应合并 + dup-check (与 AddSubAgent 协议对称), 而非整 map 直接覆盖。
//
// 验证策略 (无 SubAgents accessor): 间接通过"先 AddSubAgent('a1') + 再 WithSubAgents('a1')
// 应 panic dup" 验证合并语义 — 旧 `p.subAgents = agents` 直接覆盖时 dup 不会触发 panic。
//
// **sanity-revert**: WithSubAgents 改回 `p.subAgents = agents` → 不再 dup-check →
// 测试断言"a1 dup panic" 失败 (没 panic 测试 fail)。
func TestISSUE85_AuditFollowup_HIGH2_WithSubAgentsMergesNotOverwrite(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})
	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub))
	a1Dup := agent_v2.NewBasicAgent(agent_v2.WithName("a1_dup"), agent_v2.WithModel(stub))

	pl := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "ok", nil
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("audit follow-up HIGH-2 fix: WithSubAgents dup name 应 panic\n" +
				"sanity-revert: WithSubAgents 改回 `p.subAgents = agents` → 直接覆盖不检 dup → 测试 fail")
			return
		}
		if !strings.Contains(fmt.Sprint(r), "already registered") {
			t.Errorf("audit follow-up HIGH-2 fix: panic msg should contain 'already registered', got %v", r)
		}
	}()

	pa := agent_v2.NewPlanAgent(pl, agent_v2.PlanName("p"))
	pa.AddSubAgent("a1", a1)
	// WithSubAgents 注册 dup name 'a1' — 应 panic (合并语义), 旧覆盖语义不会 panic
	agent_v2.WithSubAgents(map[string]agent_v2.Agent{"a1": a1Dup})(pa)
}

// TestISSUE85_AuditFollowup2_HIGH2_AddSubAgentTypedNilTrap 验 audit² HIGH-A2 修 —
// AddSubAgent / WithSubAgents 用 reflect.IsNil 检 Go interface "typed nil" 陷阱:
// `var b *BasicAgent` (nil pointer) → 传 Agent interface 时 header 持非 nil type tag
// → `a == nil` false → 旧 fail-fast 漏检 → 业务后续 ag.Run nil deref panic 难定位。
//
// **sanity-revert**: isAgentNil 改回 `if a == nil` → typed nil 不 panic → 测试 fail。
func TestISSUE85_AuditFollowup2_HIGH2_AddSubAgentTypedNilTrap(t *testing.T) {
	pl := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "", nil
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("audit² HIGH-A2 fix: AddSubAgent typed nil interface 应 panic, got no panic\n" +
				"sanity-revert: isAgentNil 改回 `if a == nil` → typed nil 不 panic → 测试 fail")
			return
		}
		if !strings.Contains(fmt.Sprint(r), "typed nil") {
			t.Errorf("panic msg should contain 'typed nil', got %v", r)
		}
	}()
	pa := agent_v2.NewPlanAgent(pl, agent_v2.PlanName("p"))
	var typedNilAgent *agent_v2.BasicAgent // typed nil pointer
	pa.AddSubAgent("x", typedNilAgent)     // 应 panic — 旧 a==nil 漏检
}

// TestISSUE85_AuditFollowup2_HIGH2_WithSubAgentsTypedNilTrap 验 WithSubAgents 同款 typed nil 陷阱修。
func TestISSUE85_AuditFollowup2_HIGH2_WithSubAgentsTypedNilTrap(t *testing.T) {
	pl := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "", nil
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("audit² HIGH-A2 fix: WithSubAgents typed nil interface 应 panic, got no panic")
			return
		}
		if !strings.Contains(fmt.Sprint(r), "typed nil") {
			t.Errorf("panic msg should contain 'typed nil', got %v", r)
		}
	}()
	var typedNilAgent *agent_v2.BasicAgent // typed nil pointer
	pa := agent_v2.NewPlanAgent(pl, agent_v2.PlanName("p"))
	agent_v2.WithSubAgents(map[string]agent_v2.Agent{"x": typedNilAgent})(pa)
}

// TestISSUE85_AuditFollowup2_MED1_WithSubAgentsMergePreservesPrior 验 audit² MED-A1
// 修 — WithSubAgents 合并语义正向: 先 AddSubAgent 后 WithSubAgents (不同 name)
// 应保留 prior 注册不被覆盖。
//
// audit² 揭露 commit 49d3065 测试只覆盖 dup panic 路径, 没正向断言"merge-preserve",
// 若未来 silent revert 合并改回 `p.subAgents = agents` 直接覆盖, dup-check 能保护
// dup name 但**不同 name 的 silent loss** 无测试覆盖。本测试补此 gap。
//
// **sanity-revert**: WithSubAgents 改回 `p.subAgents = agents` 直接赋值 (并删
// dup check) → AddSubAgent('a1') 注册的 a1 silent 丢失 → planner 调 a1 时
// "no SubAgentName" 错 → 测试 fail。
func TestISSUE85_AuditFollowup2_MED1_WithSubAgentsMergePreservesPrior(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("a1 done")})
	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub))

	called := atomicBool{}
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "step1", SubAgentName: "a1"}}, "", nil
		}
		called.set(true)
		return nil, "ok", nil
	}

	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("p"))
	pa.AddSubAgent("a1", a1)
	// WithSubAgents 注册不同 name a2 — 合并应保留 a1
	agent_v2.WithSubAgents(map[string]agent_v2.Agent{"a2": a2})(pa)

	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
	if err != nil {
		t.Errorf("audit² MED-A1 fix: WithSubAgents 应合并保留 prior AddSubAgent('a1') — 期望 planner 调 a1 成功, got Run err=%v\n"+
			"sanity-revert: WithSubAgents 改回 `p.subAgents = agents` → a1 silent 丢失 → planner 调 a1 时 worker 路由失败", err)
	}
	if !called.get() {
		t.Errorf("audit² MED-A1 fix: planner 应被调 2 次 (step1 + 终止), 第 2 次未触发说明 a1 worker 没跑")
	}
}

// atomicBool 简易并发安全 bool helper (避免引入 sync/atomic).
type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (a *atomicBool) set(v bool) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicBool) get() bool  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

// TestISSUE85_AuditFollowup_MED1_AddSubAgentNilCheck 验 audit follow-up MED-1 fix —
// AddSubAgent 加 nil check (与 WithSubAgents fail-fast 协议对称)。
func TestISSUE85_AuditFollowup_MED1_AddSubAgentNilCheck(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("audit follow-up MED-1 fix: AddSubAgent('x', nil) 应 panic, got no panic")
			return
		}
	}()
	pl := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "", nil
	}
	pa := agent_v2.NewPlanAgent(pl, agent_v2.PlanName("p"))
	pa.AddSubAgent("x", nil) // 应 panic
}

// TestISSUE85_AuditFollowup_MED1_WithSubAgentsFailFast 验 WithSubAgents fail-fast 测试覆盖
// (commit 5afb8eb 加 fail-fast 但 0 测试覆盖, audit follow-up 补)。
func TestISSUE85_AuditFollowup_MED1_WithSubAgentsFailFast(t *testing.T) {
	pl := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "", nil
	}

	tests := []struct {
		name    string
		agents  map[string]agent_v2.Agent
		wantMsg string
	}{
		{"NS 禁止字符", map[string]agent_v2.Agent{"foo/bar": agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithModel(newStubModel(nil)))}, "invalid for NS"},
		{"nil agent", map[string]agent_v2.Agent{"x": nil}, "nil agent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("WithSubAgents %s 应 panic, got no panic", tt.name)
					return
				}
				if !strings.Contains(fmt.Sprint(r), tt.wantMsg) {
					t.Errorf("WithSubAgents %s panic msg should contain %q, got %v", tt.name, tt.wantMsg, r)
				}
			}()
			pa := agent_v2.NewPlanAgent(pl, agent_v2.PlanName("p"))
			agent_v2.WithSubAgents(tt.agents)(pa)
		})
	}
}
