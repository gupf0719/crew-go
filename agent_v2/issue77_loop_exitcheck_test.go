package agent_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// bodyCallCounterAgent 计 body sub-agent.Run 被调次数——用于验证 ISSUE-77 修复后
// Resume 时 body 不重跑（即使 exitCondition 抛 Interrupt）。
type bodyCallCounterAgent struct {
	calls atomic.Int32
	out   string
}

func (a *bodyCallCounterAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.calls.Add(1)
	exit := agent_v2.NewExitEvent("body_counter", schema.AssistantMessage(a.out))
	if sess != nil {
		sess.AddEvent(exit) // 真实 sub-agent.Run 协议: 写 ExitEvent 进 sess
	}
	return []*agent_v2.AgentEvent{exit}, nil
}
func (a *bodyCallCounterAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *bodyCallCounterAgent) Name() string        { return "body_counter" }
func (a *bodyCallCounterAgent) Description() string { return "" }

// TestISSUE77_ExitCondition_Interrupt_BodyNotRerun 验证 ISSUE-77 修复——
// LoopAgent ExitCondition 抛 InterruptError 时 Resume 只重跑 exitCheck 节点，
// body sub-agent 不重跑。
//
// **bug 历史 (ISSUE-77)**：之前 body 节点跑 b.body 后**同步**调 exitCondition，抛
// Interrupt 时 cp 含 body 节点 frontier → Resume 重跑 body → b.body 又调一次
// （重 LLM 调用费 + tool call + OutputEvent 重写 sess.events）。
//
// **fix (ISSUE-77)**：拓扑改 `body → exitCheck → ConditionalEdge`，exitCondition 在
// 独立 exitCheck 节点跑。Interrupt 时 cp 含 exitCheck frontier → Resume 只重跑
// exitCheck（lastOut channel cm.Restore 恢复，不需要 body 再跑）。
//
// **测试设计**:
//   - body sub-agent 用 bodyCallCounterAgent 计调用次数
//   - exitCondition 第一次抛 InterruptError，Resume 后返 true（退出）
//   - Run 1 → InterruptError → 验证 body.calls = 1
//   - Resume → 验证 body.calls **仍 = 1**（fix 后不重跑），ISSUE-77 前 = 2（bug）
func TestISSUE77_ExitCondition_Interrupt_BodyNotRerun(t *testing.T) {
	const askID = "issue77_approve_exit"
	body := &bodyCallCounterAgent{out: "round 1 output"}

	exitCondCalls := atomic.Int32{}
	exitCond := func(ctx context.Context, _ *schema.Message, _ int) (bool, error) {
		exitCondCalls.Add(1)
		// Resume 命中: 用户 approve → 退出循环
		if ans, ok := compose_v2.GetResumeAnswerText(ctx, askID); ok && ans == "approve" {
			return true, nil
		}
		// 首次 / 未命中 → 抛 Interrupt
		return false, &compose_v2.InterruptError{
			Reason: "approve_exit",
			Query: &compose_v2.AskQuery{
				InterruptID: askID,
				Question:    "approve loop exit?",
			},
		}
	}

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(3),
		agent_v2.WithLoopExitCondition(exitCond),
	).WithBody(body)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: body 跑 1 次 → exitCond 抛 Interrupt
	_, err := loop.Run(ctx, sess, schema.UserMessage("start"))
	if err == nil {
		t.Fatal("expected Interrupt from exitCondition")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("expected InterruptError, got %v", err)
	}

	bodyCalls1 := body.calls.Load()
	exitCondCalls1 := exitCondCalls.Load()
	if bodyCalls1 != 1 {
		t.Fatalf("Run 1: body 应被调 1 次，实际 %d", bodyCalls1)
	}
	if exitCondCalls1 != 1 {
		t.Fatalf("Run 1: exitCond 应被调 1 次，实际 %d", exitCondCalls1)
	}

	// Resume: 答 approve → exitCond 返 true → 循环结束
	// **R34 audit MED-2 fix**: 加 WithResumeSession(sess) 让 Resume 续传同 sess,
	// 否则 Resume 内创建新 sess (loop_agent.go:191) 让 events 不进同 slice 掩盖
	// 累积/dup bug。
	answers := map[string]*schema.Message{
		askID: schema.UserMessage("approve"),
	}
	_, err = loop.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(answers),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume err: %v", err)
	}

	// **核心断言** (ISSUE-77 fix)：body 不重跑，calls 仍 = 1
	bodyCalls2 := body.calls.Load()
	exitCondCalls2 := exitCondCalls.Load()
	if bodyCalls2 != 1 {
		t.Errorf("ISSUE-77 fix: Resume 后 body 不应重跑（exitCheck 节点独立 cp）；"+
			"实际 body.calls=%d (Run 后 1, Resume 后应仍 1)\n"+
			"sanity-revert 把 exitCondition 调用合并回 body 节点 → Resume 重跑 body → calls=2",
			bodyCalls2)
	}
	// exitCond 第二次跑成功
	if exitCondCalls2 != 2 {
		t.Errorf("Resume 后 exitCond 应再调 1 次（第二次返 true 退出），实际 calls=%d", exitCondCalls2)
	}

	// **R34 audit follow-up**: 验证 sess.events 含的 body ExitEvent 不重复
	// (body.calls=1 → ExitEvent 也应仅 +1 一次, 不能因 Resume 重跑 body 而 dup)
	bodyExitCount := 0
	for _, e := range sess.Events() {
		if e.AgentName == "body_counter" && e.Action != nil && e.Action.Exit {
			bodyExitCount++
		}
	}
	if bodyExitCount != 1 {
		t.Errorf("R34 audit: body sub-agent ExitEvent 应仅 1 次 (body 不重跑); 实际 %d 次", bodyExitCount)
	}
}

// TestISSUE77_NoExitCondition_NoRegression 验证无 exitCondition 时正常 maxIter 退出。
func TestISSUE77_NoExitCondition_NoRegression(t *testing.T) {
	body := &bodyCallCounterAgent{out: "iter"}

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(3),
	).WithBody(body)

	_, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("start"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	// body 应跑 3 次直到 maxIter
	if got := body.calls.Load(); got != 3 {
		t.Errorf("expected body called 3 times (maxIter); actual %d", got)
	}
}

// TestISSUE77_ExitCondition_NormalExit_NoRegression 验证 exitCondition 返 true
// 时正常退出，body 跑 N 次符合预期。
func TestISSUE77_ExitCondition_NormalExit_NoRegression(t *testing.T) {
	body := &bodyCallCounterAgent{out: "iter"}

	exitCond := func(_ context.Context, _ *schema.Message, round int) (bool, error) {
		return round >= 2, nil // 第 2 轮后退出
	}

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(10),
		agent_v2.WithLoopExitCondition(exitCond),
	).WithBody(body)

	_, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("start"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if got := body.calls.Load(); got != 2 {
		t.Errorf("expected body called 2 times (exitCondition round>=2); actual %d", got)
	}
}
