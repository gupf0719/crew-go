package agent_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// TestPlan_R28_HIGH1_NestedNSPushResume 是 R28 audit HIGH-1 回归——
// 验证 PlanAgent 嵌入父编排（共享 session）内部 sub-agent 抛 InterruptError 时
// cp 物理位置正确隔离 + Resume 闭环成功。
//
// **Bug 历史**：R28 audit HIGH-1 发现 PlanAgent 全链路（runPlanGraph + Resume + 3
// worker wrapper）都**未** push graphNS——其他 7 编排都正确 push，PlanAgent 是
// 单点 regression。后果：
//
//   - 父编排（Sequential / Loop / Reflect 等共享 session 给 sub-agent.Run）push 父
//     graphNS = "step_plan" 给 PlanAgent.Run 但 PlanAgent **不 forward** 给内部
//     compose_v2.WithRunNS → PlanAgent 内部 cp 写到 (sid, "") 而非 (sid, "step_plan")
//   - 父编排自己的 cp 也在 (sid, "")（如 Sequential 顶层 graphNS=""）→ **互覆**
//   - PlanAgent worker 调 sub-agent.Run 时 ctx graphNS=""，sub BasicAgent.runGraph
//     的 cp 又写到 (sid, "") 三方覆盖
//   - Resume 时 cper.LoadNS(sid, "") 拿到最后被覆盖的 cp（GraphID 是 PlanAgent 或
//     sub BasicAgent 的）→ 与父 graph.ID() 不一致 → 框架 fail-fast 返
//     `ErrGraphIDConflict` → HITL 链彻底断
//
// **修法**：PlanAgent.runPlanGraph + Resume 入口 forward 父 graphNS；3 worker wrapper
// push 子 ns（含 step 序号也解决 MED-1 同 sub-agent 多次 step 的 InterruptID 串台）。
//
// **测试设计避坑**（CLAUDE.md 5 类炸弹之 #2 早退兜底盖过 fix 路径）：
//   - **不能用 AgentAsTool 包 PlanAgent**——AgentAsTool 经嵌套调用执行器给被调方 push 了由
//     调用身份派生的子 ns，PlanAgent 的存档本来就跟父的物理分离 → **bug 路径不触发**
//     （R28 audit 误判此场景必踩）。（这里曾写「AgentAsTool 传 nil session、PlanAgent 自建
//     新 sid」——那是更早的实现，现在被调方的 RunID 就是父 sid。）
//   - **必须用共享 session 父编排**（如 SequentialAgent 传 parentSession 给
//     sub.Run）才让 sid 与 ns 都一致、触发 cp 互覆 + GraphID conflict
func TestPlan_R28_HIGH1_NestedNSPushResume(t *testing.T) {
	// 子 BasicAgent：第一次跑抛 ask_user InterruptError，Resume 后取答案返"sub_done"
	const askID = "sub_ask_id"
	subRunCount := atomic.Int32{}
	subStubFn := func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
		subRunCount.Add(1)
		// 检查 ResumeContext 是否含答案
		if _, ok := compose_v2.GetResumeAnswerText(ctx, askID); ok {
			return schema.AssistantMessage("sub_done"), nil
		}
		// 首次抛 InterruptError
		return nil, &compose_v2.InterruptError{
			Reason: "ask user from sub-agent",
			Query: &compose_v2.AskQuery{
				InterruptID: askID,
				Question:    "approve sub-agent execution?",
			},
		}
	}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("sub"),
		agent_v2.WithModel(&fnModel{fn: subStubFn}),
	)

	// PlanAgent：1 step 派给 "sub"
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) > 0 {
			return nil, "plan_done", nil
		}
		return []agent_v2.PlanStep{{Description: "sub task", SubAgentName: "sub"}}, "", nil
	})
	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan")).
		AddSubAgent("sub", subAgent)

	// 父 SequentialAgent：1 step = PlanAgent，**共享 session** 给 sub.Run
	// （Sequential.stepFn line 326 传 parentSession 给 step.Agent.Run）
	parent := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq"),
	).AddStep("plan_step", pa)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// === Run 1: 嵌套 sub-agent 抛 InterruptError ===
	_, err := parent.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R28 HIGH-1 回归：嵌套 sub-agent 抛 InterruptError 应 propagate；实际 nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("R28 HIGH-1 回归：err 应为 InterruptError；实际 %v", err)
	}
	if ie.Query == nil {
		t.Fatalf("R28 HIGH-1 回归：ie.Query 不应 nil")
	}

	// === Run 2: Resume 答 sub-agent 的 ask_user ===
	// 修前：Sequential cp 在 (sid, "")，PlanAgent cp 写到 (sid, "") 互覆，sub BasicAgent
	//       cp 又写到 (sid, "") 三方覆盖。Resume 时 cper.LoadNS(sid, "") 拿被覆盖最后的
	//       cp，GraphID != Sequential.g.ID() → ErrGraphIDConflict 报错；或 cm.Restore
	//       报 "channel not registered"
	// 修后：Sequential cp 在 (sid, "")，PlanAgent cp 在 (sid, "plan_step")（Sequential
	//       push 的 step name 是 "plan_step"，PlanAgent forward 不 push 自己 name 见
	//       ISSUE-85 MED-2），sub cp 在 (sid, "plan_step/sub_step_0")
	//       → 三层物理隔离 → Resume 走父 .Resume → 父 step → PlanAgent.Run（子 cp 续跑）
	//       → PlanAgent worker → sub.Run（孙 cp 续跑）→ sub 拿 ResumeContext 答案
	_, err = parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("yes"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("R28 HIGH-1 回归：嵌套 Resume 应成功；实际 err=%v\n"+
			"sanity-revert 删 PlanAgent 4 处 NS push (runPlanGraph forward + Resume forward + 3 worker wrapper push 子 ns) → "+
			"应 fail 报 'graph_id conflict' 或 'channel ... not registered'", err)
	}

	// 验证 sub-agent 至少跑了 2 次（Run 1 抛 Interrupt + Resume 重跑）
	if got := subRunCount.Load(); got < 2 {
		t.Errorf("R28 HIGH-1 回归：sub-agent 应跑 >= 2 次（Run 1 失败 + Resume 重跑）；实际 %d", got)
	}

	// **ISSUE-85 MED-1 fix**: 删假绿 NS 检查——之前 expectChildNS = "step_plan/plan/sub_step_0"
	// 字面量错 (实际是 "plan_step/sub_step_0"), 用 t.Logf 不 t.Errorf 假绿。Resume
	// 成功后 cleanup 清整 sid, 没 pending cp 可断言。NS 协议正确性靠 Resume 成功
	// (line 116 t.Fatalf) + sub-agent 跑 >= 2 次 (line 122) 端到端验证。
}

// fnModel 是 closure-based ChatModel，让测试用 ctx 检查 ResumeContext。
type fnModel struct {
	fn func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error)
}

func (m *fnModel) Generate(ctx context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.fn(ctx, msgs)
}
func (m *fnModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *fnModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}
