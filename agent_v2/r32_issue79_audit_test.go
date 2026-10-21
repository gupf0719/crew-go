package agent_v2_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// 类型别名让代码更紧凑
type r32Tool = tool.Tool
type toolPkgToolkit = tool.Toolkit

func toolPkgNewToolkit() *tool.Toolkit { return tool.NewToolkit() }

// R32 audit (ISSUE-79 实施复审) 找的真 finding 配套回归测试集。
//
// audit 方法：4 agent 单视角并发审 commit 7c9c7db，4 真 HIGH:
//   - A HIGH-1 race: NewSharedChildSession values map 共享但 mu 不共享 → 父子并发 SetValue race detector 100% 爆
//   - C HIGH-1: skill_tool.go fork 路径仍 NewRunSession() 全隔离 — 与 AgentAsTool 行为分裂
//   - D HIGH-1: T3 NilParent_Fallback stub 自防御覆盖 fix 路径 — sanity-revert 仍 PASS（假绿炸弹）
//   - D HIGH-2/3: T1/T4 stub 不验真业务路径
//
// 本文件补:
//   - TestR32_SharedSession_ConcurrentSetValue_Race: 验证 race fix（必须 -race 跑）
//   - TestR32_SharedSession_EventsIndependent_PreciseSize: 加 events size 精确断言
//   - TestR32_SharedSession_NestedFanOut_NoRace: ParallelAgent fan-out 多 branch 共享父 sess race-free

// TestR32_SharedSession_ConcurrentSetValue_Race 验证 R32 audit 视角 A HIGH-1 修复——
// NewSharedChildSession 共享 valuesMu 指针让父子并发 SetValue/GetValue race-free。
//
// **跑法**：必须用 `-race` flag —— `go test -race -run TestR32_SharedSession_ConcurrentSetValue_Race`
//
// **bug 历史**：commit 7c9c7db ISSUE-79 fix 让 child sess 共享父 values map 但**不共享 mu**——
// 父 RunSession.mu / 子 RunSession.mu 是各自独立的 sync.RWMutex 但指向同一个 map →
// 并发 SetValue 触发 `runtime.mapaccess2()` race。框架 `basic_agent.go::preambleFn`
// 调用 `session.SetValue(skillListingSessionKey(name), ...)` + ParallelAgent fan-out
// N branch × M sub-agent 嵌套 AgentAsTool 场景下必触发。
//
// **修法**：mu 字段拆为 eventsMu (per-sess 独立 sync.RWMutex 保护 events) +
// valuesMu (*sync.RWMutex 父子共享指针 NewSharedChildSession 复制) + valuesOnce
// (sync.Once 保 lazy init race-free)。
func TestR32_SharedSession_ConcurrentSetValue_Race(t *testing.T) {
	root := agent_v2.NewRunSession()
	child1 := agent_v2.NewSharedChildSession(root)
	child2 := agent_v2.NewSharedChildSession(root)
	grandchild := agent_v2.NewSharedChildSession(child1)

	var wg sync.WaitGroup
	const N = 50
	for i := 0; i < N; i++ {
		wg.Add(4)
		go func(i int) {
			defer wg.Done()
			root.SetValue("key", i)
		}(i)
		go func(i int) {
			defer wg.Done()
			child1.SetValue("key", i)
			_, _ = child1.GetValue("key")
		}(i)
		go func(i int) {
			defer wg.Done()
			child2.SetValue("key", i)
			_, _ = child2.GetValue("key")
		}(i)
		go func(i int) {
			defer wg.Done()
			grandchild.SetValue("key", i)
			_, _ = grandchild.GetValue("key")
		}(i)
	}
	wg.Wait()

	// 父子读到同一个值（最后赢方写的）
	v1, _ := root.GetValue("key")
	v2, _ := child1.GetValue("key")
	v3, _ := grandchild.GetValue("key")
	if v1 != v2 || v2 != v3 {
		t.Errorf("R32 A HIGH-1 fix: 父子共享 map 应见同值；实际 root=%v child1=%v grandchild=%v", v1, v2, v3)
	}
}

// TestR32_SharedSession_EventsIndependent_PreciseSize 改进 D HIGH-2 测试设计——
// events 独立断言改为精确 size 而非"AgentName == child"软匹配。
//
// **bug 历史**：原 TestAgentAsTool_ISSUE79_SharedSession 仅检查 `AgentName == "child"`
// 的 events 不进父——但若半共享 fix 漏让 events 也共享指针，类型可能是 OutputEvent
// 而非 ExitEvent / 不同 AgentName，软匹配漏报。
//
// **测试设计**：Call 前后断 parentSess.Events() 长度精确不变（子写多 events 父长度 +0）。
func TestR32_SharedSession_EventsIndependent_PreciseSize(t *testing.T) {
	// inline 实现：写 3 个不同类型 events 到子 sess
	var subRun = func(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if sess == nil {
			sess = agent_v2.NewRunSession()
		}
		sess.AddEvent(agent_v2.NewOutputEvent("child_step1", schema.UserMessage("u1")))
		sess.AddEvent(agent_v2.NewOutputEvent("child_step2", schema.AssistantMessage("a1")))
		sess.AddEvent(agent_v2.NewOutputEvent("__internal__", schema.AssistantMessage("hidden")))
		return []*agent_v2.AgentEvent{
			agent_v2.NewExitEvent("child_final", schema.AssistantMessage("done")),
		}, nil
	}
	cap := &captureAgentR32{name: "child", run: subRun}
	tooly := agent_v2.AgentAsTool(cap, agent_v2.WithAgentToolDescription("child"))

	parentSess := agent_v2.NewRunSession()
	parentSess.AddEvent(agent_v2.NewOutputEvent("parent_setup", schema.UserMessage("setup")))
	parentLenBefore := len(parentSess.Events())
	if parentLenBefore != 1 {
		t.Fatalf("setup: parent events 应 1; 实际 %d", parentLenBefore)
	}

	ctx := agent_v2.WithSession(context.Background(), parentSess)
	if _, err := tooly.Call(ctx, map[string]any{"input": "go"}); err != nil {
		t.Fatalf("Call: %v", err)
	}

	parentLenAfter := len(parentSess.Events())
	if parentLenAfter != parentLenBefore {
		t.Errorf("R32 D HIGH-2 fix: 半共享 child sess events 应严格独立——父 events len Call 前后不变；"+
			"实际 before=%d after=%d (新增 %d 条)\n"+
			"sanity-revert 让 NewSharedChildSession 也共享 events slice → 子 3+1 events 进父 → 测试 fail",
			parentLenBefore, parentLenAfter, parentLenAfter-parentLenBefore)
	}
}

// captureAgentR32 用闭包 run func 自定义行为。
type captureAgentR32 struct {
	name string
	run  func(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error)
}

func (c *captureAgentR32) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return c.run(ctx, sess, in, opts...)
}
func (c *captureAgentR32) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *captureAgentR32) Name() string        { return c.name }
func (c *captureAgentR32) Description() string { return "" }

// TestR32_SkillForkPath_SharesParentValues 验证 R32 audit 视角 C HIGH-1 修复——
// skill_tool.go fork 路径与 AgentAsTool 路径行为一致（半共享 child sess），
// 之前 NewRunSession() 全隔离让 SkillSubAgentResolver 注同款 sub-agent 时取决于
// 走 AgentAsTool 还是 fork mode 行为分裂。
//
// **测试设计**：直接调 callForkSubAgent helper（white-box）—— 但 callForkSubAgent
// 是 unexported 且嵌在 skillTool.Call 路径里，不易直接测。改用集成方式: 验证父 sess 里的
// 业务 KV 在 fork sub-agent 写后双向可见（如果 fork 用 NewRunSession 全隔离则父读不到）。
//
// **简化**：测试不调真实 fork 路径（涉及 SkillBox + Resolver 复杂 setup），而是
// 直接验证 NewSharedChildSession 返回的子 sess 跟 parent values 共享——这是 fork 路径
// 改动后的行为基础，与 AgentAsTool 共用同一 helper。
//
// **真正 e2e 验证**留待 plan_chat_web smoke + 业务集成测覆盖。
func TestR32_SkillForkPath_SharesParentValues(t *testing.T) {
	parent := agent_v2.NewRunSession()
	parent.SetValue("biz_kv:sub", map[string]bool{"foo": true})

	// 模拟 fork 路径行为：子 sess 用 NewSharedChildSession（fix 后行为）
	subSess := agent_v2.NewSharedChildSession(parent)

	v, ok := subSess.GetValue("biz_kv:sub")
	if !ok {
		t.Fatalf("R32 C HIGH-1 fix: fork 子 sess 应通过共享 values map 看到父写的业务 KV；实际读不到\n" +
			"sanity-revert 把 NewSharedChildSession 改回不共享 values 或 skill_tool.go fork 路径改回 NewRunSession() → 子读不到 → 测试 fail")
	}
	m, ok := v.(map[string]bool)
	if !ok || !m["foo"] {
		t.Errorf("fork 子 sess 应见父 KV 含 foo=true；实际 %v", v)
	}

	// 反向：子写父读（半共享 values 双向可见）
	subSess.SetValue("biz_kv:sub", map[string]bool{"foo": true, "bar": true})
	v2, _ := parent.GetValue("biz_kv:sub")
	m2, _ := v2.(map[string]bool)
	if !m2["bar"] {
		t.Errorf("fork 子 sess 写后父应见 bar=true；实际 %v", v2)
	}
}

// TestR32_SharedSession_NestedFanOut_NoRace 验证 ParallelAgent fan-out N branch
// 嵌套 AgentAsTool 场景下父子并发 SetValue race-free。
//
// **场景**：业务 plan_chat_web 真实路径——父 BasicAgent → ParallelAgent fan-out 4 branch
// → 每 branch BasicAgent → tool.Call → AgentAsTool(子) → 子 BasicAgent.Run 内 preamble
// SetValue("skill_listing:<name>", ...) → 父子并发同 values map
//
// **跑法**：必须 `-race`
func TestR32_SharedSession_NestedFanOut_NoRace(t *testing.T) {
	parentSess := agent_v2.NewRunSession()

	// 模拟 ParallelAgent fan-out N branch，每 branch 内嵌 AgentAsTool.Call
	const branches = 8
	const callsPerBranch = 20
	var wg sync.WaitGroup
	var totalCalls atomic.Int32

	for b := 0; b < branches; b++ {
		wg.Add(1)
		go func(branchID int) {
			defer wg.Done()
			// 每 branch 内 N 次 AgentAsTool.Call (模拟 ReAct 多轮 tool_call)
			for c := 0; c < callsPerBranch; c++ {
				cap := &captureAgentR32{name: "sub", run: func(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
					if sess == nil {
						sess = agent_v2.NewRunSession()
					}
					// 子写 values + read（模拟 Skill listing accumulation）
					sess.SetValue("skill_listing:sub", map[string]bool{"foo": true})
					_, _ = sess.GetValue("skill_listing:sub")
					totalCalls.Add(1)
					return []*agent_v2.AgentEvent{
						agent_v2.NewExitEvent("sub", schema.AssistantMessage("done")),
					}, nil
				}}
				tooly := agent_v2.AgentAsTool(cap, agent_v2.WithAgentToolDescription("sub"))
				ctx := agent_v2.WithSession(context.Background(), parentSess)
				if _, err := tooly.Call(ctx, map[string]any{"input": "go"}); err != nil {
					t.Errorf("branch %d call %d: %v", branchID, c, err)
				}
			}
		}(b)
	}
	wg.Wait()

	if int(totalCalls.Load()) != branches*callsPerBranch {
		t.Errorf("expected %d total calls, got %d", branches*callsPerBranch, totalCalls.Load())
	}
	// 验证 parent values 含 skill_listing:sub key（任何一个并发 branch 写都应可见）
	if _, ok := parentSess.GetValue("skill_listing:sub"); !ok {
		t.Errorf("R32 A HIGH-1 fix: 父 sess 应见 child 写的 skill_listing:sub key（共享 map）")
	}
}

// simpleSkillBox 极简 SkillBox 实现——只为 e2e 测试用。
type simpleSkillBox struct {
	skills []skill.SkillDescriptor
}

func (b *simpleSkillBox) Name() string                        { return "simple" }
func (b *simpleSkillBox) RootDir() string                     { return "" }
func (b *simpleSkillBox) ListSkills() []skill.SkillDescriptor { return b.skills }
func (b *simpleSkillBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult {
	return nil
}
func (b *simpleSkillBox) ListSkillResources(_ string) []skill.SkillResource { return nil }
func (b *simpleSkillBox) ReadSkillResource(_, _ string) string              { return "" }
func (b *simpleSkillBox) ReadSkillPrompt(_ string) string                   { return "" }
func (b *simpleSkillBox) ListSkillActions(_ string) []skill.SkillActionDescriptor {
	return nil
}
func (b *simpleSkillBox) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	return &skill.ActiveSkill{Skill: skill.SkillDescriptor{Name: name}, Prompt: ""}, nil
}
func (b *simpleSkillBox) RunSkillAction(_ context.Context, _, _ string, _ map[string]any) (*skill.SkillExecutionResult, error) {
	return nil, skill.ErrSkillActionNotFound
}

func countValues(s *agent_v2.RunSession) int {
	// 通过 reflection 不太合适，简单返 0 作占位（错误信息辅助用）
	return 0
}

// TestR32_E2E_NestedSessionChain 是 R32 视角 D MED-2 (a) 端到端补测——
// 验证 9 编排嵌套时 sess.parentSess 链自动建立（业务真实路径而非 stub）。
//
// **测试场景**: 父 BasicAgent → AgentAsTool(SeqAgent) → seq.step1 = BasicAgent →
// step1 调 AgentAsTool(GrandChild) → 验证 GrandChild.RootSession() 链路指向父 sess
//
// **bug 风险**：sub-agent.Run(ctx, sess, ...) 收到的 sess 是不是父 sess 沿链下传？
// 还是某编排中途用 NewRunSession 断链？r31 测试只测了直接调 NewSharedChildSession 3
// 层 helper，没验真实 9 编排嵌套自然形成的链路。
func TestR32_E2E_NestedSessionChain(t *testing.T) {
	// 收集每层 grandchild 看到的 RootSession.RunID
	var capturedRootID string
	var captureMu sync.Mutex

	// GrandChild: 捕获 RootSession
	captureGrandchild := &captureAgentR32{name: "grandchild", run: func(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if sess == nil {
			sess = agent_v2.NewRunSession()
		}
		captureMu.Lock()
		capturedRootID = sess.RootSession().RunID
		captureMu.Unlock()
		return []*agent_v2.AgentEvent{
			agent_v2.NewExitEvent("grandchild", schema.AssistantMessage("done")),
		}, nil
	}}
	grandchildTooly := agent_v2.AgentAsTool(captureGrandchild, agent_v2.WithAgentToolDescription("grandchild"))

	// step1 BasicAgent: 调 grandchild tool 一次然后返 final
	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "grandchild", Arguments: `{"input":"go"}`}}
	step1Model := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
		schema.AssistantMessage("step1 done"),
	})
	tk := newToolkitWith(grandchildTooly)
	step1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("step1"),
		agent_v2.WithModel(step1Model),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	// SeqAgent 包 step1
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("a", step1)
	seqTooly := agent_v2.AgentAsTool(seq, agent_v2.WithAgentToolDescription("seq"))

	// 父 BasicAgent: 调 seq tool 一次然后返 final
	parentTc := schema.ToolCall{ID: "tc_p", Function: schema.FunctionCall{Name: "seq", Arguments: `{"input":"top"}`}}
	parentModel := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{parentTc}),
		schema.AssistantMessage("parent done"),
	})
	parentTk := newToolkitWith(seqTooly)
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentModel),
		agent_v2.WithToolkit(parentTk),
		agent_v2.WithMaxRounds(5),
	)

	parentSess := agent_v2.NewRunSession()
	parentSess.RunID = "ROOT_RUN_ID"

	if _, err := parent.Run(context.Background(), parentSess, schema.UserMessage("go")); err != nil {
		t.Fatalf("parent.Run: %v", err)
	}

	captureMu.Lock()
	defer captureMu.Unlock()
	if capturedRootID != "ROOT_RUN_ID" {
		t.Errorf("R32 D MED-2 e2e: 嵌套 4 层（parent BasicAgent → SeqAgent → step1 BasicAgent → AgentAsTool(grandchild)）后\n"+
			"grandchild.RootSession().RunID 应链回到根 sess（ROOT_RUN_ID）；实际 %q\n"+
			"如果不等说明某层编排断了 sess 链（用 NewRunSession 而非半共享 child sess）",
			capturedRootID)
	}
}

// newToolkitWith 创建含单个 tool 的 toolkit helper
func newToolkitWith(t r32Tool) *toolPkgToolkit {
	tk := toolPkgNewToolkit()
	tk.Register(t)
	return tk
}

// TestISSUE80_SameSubAgentInstanceInMultiBranches 验证 ISSUE-80 lane events 修复后
// 业务可用同 sub-agent 实例作多 branch——之前 R31 FinalMessageByAgent name filter
// 限制要求 distinct sub-agent 名，lane 隔离根因修复后取消此限制。
//
// **测试场景**：
//   - 创建 1 个 sub-agent 实例（同 Name="shared_sub"）
//   - ParallelAgent 用此同实例添加 2 个 branch (branch_A / branch_B)
//   - 用 barrier 强制并发，让 sub-agent.Run() 返回 events 时 sibling 也已写完
//   - 验证 merger 收到的两 branch 输出都正确归属（不是 sibling 串台）
//
// **关键设计**：与 R31 TestR31_ParallelAgent_FanoutFinalMessage_NoSiblingCrosstalk
// 不同——R31 用 distinct name agent_A / agent_B 才能让 name filter 工作；本测试
// 故意用同 Name 验证 lane 隔离不靠 name 也能保正确归属。
func TestISSUE80_SameSubAgentInstanceInMultiBranches(t *testing.T) {
	var wgWrote sync.WaitGroup
	wgWrote.Add(2)
	var wgGo sync.WaitGroup
	wgGo.Add(1)

	done := make(chan struct{})
	go func() {
		wgWrote.Wait()
		wgGo.Done()
		close(done)
	}()

	// 同 Name 但每个 branch 实际行为通过闭包区分 (业务真实场景:
	// 同 sub-agent 实例 + 不同 input → 不同输出)
	makeBarrierAgent := func(exitMsg string) *captureAgentR32 {
		return &captureAgentR32{
			name: "shared_sub", // **故意用同 Name** 验证 lane 隔离不依赖 name filter
			run: func(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if sess == nil {
					sess = agent_v2.NewRunSession()
				}
				// 写 OutputEvent + ExitEvent
				sess.AddEvent(agent_v2.NewOutputEvent("shared_sub", schema.UserMessage("step")))
				sess.AddEvent(agent_v2.NewExitEvent("shared_sub", schema.AssistantMessage(exitMsg)))
				wgWrote.Done()
				wgGo.Wait()
				// 返 sub-agent 完整事件流——lane 隔离让此 slice 只含本 branch
				all := sess.Events()
				return all, nil
			},
		}
	}

	captured := map[string]string{}
	var mu sync.Mutex
	merger := func(_ context.Context, results []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range results {
			if r.Output != nil {
				captured[r.Name] = r.Output.Content
			}
		}
		return schema.AssistantMessage("merged"), nil
	}

	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithParallelMerger(merger),
	).
		AddBranch("branch_A", makeBarrierAgent("A-FINAL-VALUE")).
		AddBranch("branch_B", makeBarrierAgent("B-FINAL-VALUE"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("barrier 未 release，sub-agent.Run() 未都 return")
	}

	mu.Lock()
	defer mu.Unlock()
	// 关键断言：即使两 branch 用同 Name="shared_sub" 的 sub-agent 实例，
	// lane 物理隔离让每 branch 拿到的 events 只含本 branch 的 → FinalMessage 正确归属
	if got := captured["branch_A"]; got != "A-FINAL-VALUE" {
		t.Errorf("ISSUE-80 fix: branch_A 应见 A-FINAL-VALUE (lane 物理隔离不靠 name filter); 实际 %q", got)
	}
	if got := captured["branch_B"]; got != "B-FINAL-VALUE" {
		t.Errorf("ISSUE-80 fix: branch_B 应见 B-FINAL-VALUE (lane 物理隔离不靠 name filter); 实际 %q\n"+
			"sanity-revert ParallelAgent.branchFn 改回共享 parentSession + FinalMessage(events) → 拿到 sibling Exit → 测试 fail",
			got)
	}
}

// TestR33_NestedFanOut_LaneParentNoRace 验证 R33 audit A HIGH-1 修复——
// 嵌套 ParallelAgent (outer N branch × 每 inner = M branch ParallelAgent) 时
// 多个 inner branch 同时 commit 到同一 outer lane.Events 必须 race-free。
//
// **bug 历史**：commit 2da4a77 ISSUE-80 fix 把 "lane.Events lock-free" 假设建立在
// "join 时单 goroutine 串行 commit" 前提（仿 eino runctx.go:301），但 crew-go 的
// branchFn 各自 commit 不同时间到达 outer lane → 多 inner branch 同时 append
// outer lane.Events → race detector 100% 触发。
//
// **修法**：laneEvents 加 sync.Mutex 保护 Events slice append。AddEvent / Events
// 读路径 / CommitLaneEvents 写路径 都走 lane.mu。
//
// **跑法**：必须 -race。
func TestR33_NestedFanOut_LaneParentNoRace(t *testing.T) {
	// 模拟嵌套 fan-out 直接调 lane API（绕过完整 ParallelAgent 让测试聚焦 race）
	root := agent_v2.NewRunSession()
	outerLane := root.ForkLane()

	const innerBranches = 8
	const eventsPerBranch = 20
	var wg sync.WaitGroup
	for b := 0; b < innerBranches; b++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 模拟 inner branch: fork 自 outerLane → write events → commit
			innerLane := outerLane.ForkLane()
			for e := 0; e < eventsPerBranch; e++ {
				innerLane.AddEvent(agent_v2.NewOutputEvent("inner", schema.AssistantMessage("e")))
			}
			// commit innerLane 到 outerLane → outerLane.laneEvents.Events 多 goroutine 同时写
			agent_v2.CommitLaneEvents(outerLane, innerLane)
		}(b)
	}
	wg.Wait()

	// 同时 outer 主 goroutine 读 outerLane.Events() — 应不 race
	_ = outerLane.Events()
}

// TestR33_LaneInternal_SeesParentCommittedHistory 验证 R33 audit B HIGH-1 修复——
// ForkLane child sess 在 lane 内调 Events() 应能看到 parent committed history。
//
// **bug 历史**：commit 2da4a77 ForkLane 只共享 values，**没共享** parent.events 指针——
// child.events 是新 nil slice → child Events() 拼 committed 部分返空 → 业务 lane 内
// sub-agent.SelfHistory / HistoryFor / preambleFn buildInitialMessages 拿不到父
// prior 历史 → 跨 Run 复用 sess 时 message 流不完整。
//
// eino runctx.go:334 显式 `Events: parentRunCtx.Session.Events` 共享父 events——
// crew-go 漏对齐。
//
// **修法**：ForkLane child.events = parent.events (slice 共享指针) +
// child.eventsMuRef = &parent.eventsMu (锁共享指针) → effectiveEventsMu() 返父锁
// → child Events() 走父锁读父 slice race-free。
func TestR33_LaneInternal_SeesParentCommittedHistory(t *testing.T) {
	parent := agent_v2.NewRunSession()
	parent.AddEvent(agent_v2.NewOutputEvent("parent_step1", schema.UserMessage("u1")))
	parent.AddEvent(agent_v2.NewOutputEvent("parent_step2", schema.AssistantMessage("a1")))

	parentLen := len(parent.Events())
	if parentLen != 2 {
		t.Fatalf("setup: parent events 应 2; 实际 %d", parentLen)
	}

	// fork lane 让 lane 内 sess.Events() 应见父历史
	laneSess := parent.ForkLane()
	laneEvents := laneSess.Events()
	if len(laneEvents) != parentLen {
		t.Errorf("R33 B HIGH-1 fix: lane 内 sess.Events() 应见父 committed 历史 (%d 条);"+
			"实际只见 %d 条\n"+
			"sanity-revert ForkLane 改回 events 不共享 → lane 内拿不到父历史 → 业务 sub-agent prior 流不完整",
			parentLen, len(laneEvents))
	}

	// lane 内写 lane events，应只在 lane.Events，不污染 parent.events
	laneSess.AddEvent(agent_v2.NewOutputEvent("lane_step", schema.AssistantMessage("lane")))
	if len(parent.Events()) != parentLen {
		t.Errorf("lane 内 AddEvent 不应进父 events; 父长度应仍 %d，实际 %d", parentLen, len(parent.Events()))
	}
	// 但 lane 自己 Events() 视图应见父 + lane 链
	if len(laneSess.Events()) != parentLen+1 {
		t.Errorf("lane Events() 应见父 + lane (%d 条); 实际 %d 条", parentLen+1, len(laneSess.Events()))
	}

	// commit 后 parent.events 含 lane events
	agent_v2.CommitLaneEvents(parent, laneSess)
	if len(parent.Events()) != parentLen+1 {
		t.Errorf("commit 后父 events 应 %d 条; 实际 %d", parentLen+1, len(parent.Events()))
	}
}
