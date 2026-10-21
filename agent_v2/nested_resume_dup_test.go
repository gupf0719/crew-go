package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// countingWrapAgent 包一层记录 inner.Run 被调次数 + 每次收到的 input content——
// 用于断言 outer.Resume 确实重新经过 inner（agent_v2 wrapper）入口（测试前提）。
type countingWrapAgent struct {
	inner  agent_v2.Agent
	inputs *[]string
}

func (c *countingWrapAgent) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	*c.inputs = append(*c.inputs, input.Content)
	return c.inner.Run(ctx, sess, input, opts...)
}
func (c *countingWrapAgent) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return c.inner.Resume(ctx, sid, opts...)
}
func (c *countingWrapAgent) Name() string        { return c.inner.Name() }
func (c *countingWrapAgent) Description() string { return c.inner.Description() }

var _ agent_v2.Agent = (*countingWrapAgent)(nil)

// TestNestedWrapperResume_InputEventNotDuplicated 端到端回归（audit MED-2）：wrapper 编排
// 嵌套（outer Sequential 嵌 inner Sequential）+ 续传同一 sess 时，inner 中断 → outer.Resume
// 重跑 step 第二次调 inner.Run（agent_v2 嵌套统一走 .Run 非 .Resume）→ inner 的 input
// OutputEvent 不得重复。shouldEmitInputEvent guard：inner cp 已存在 = resume 重入，不重加
// （对齐 BasicAgent input 在 preamble 节点 initial-only 的免疫）。
//
// **关键**：必须 WithResumeSession 续传同一 sess（真实 HITL 业务范式）——否则 Resume 新建空
// session（commonResumePrep），dup 加别处测试 sess 看不到（假绿陷阱）。
//
// **sanity-revert**：把 shouldEmitInputEvent 改成恒 return true（回退 guard，无条件 AddEvent）
// → inner input event count==2，本测试 FAIL 精确指向嵌套 Resume input dup（已实测验证）。
func TestNestedWrapperResume_InputEventNotDuplicated(t *testing.T) {
	var innerCalls []string
	inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner")).
		AddStep("w", &interruptingAgent{name: "w"})
	counted := &countingWrapAgent{inner: inner, inputs: &innerCalls}
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("i", counted)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := outer.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("outer.Run 应中断，实际 err=%v", err)
	}
	qid := ie.AllQueries()[0].InterruptID

	_, _ = outer.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess), // 续传同一 sess（真实 HITL 业务范式）
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			qid: schema.UserMessage("answer"),
		}))

	// 前提断言：outer.Resume 确实重跑 inner.Run 第二次（input "hi"）——否则 dup 不会发生，
	// 测试无意义（防将来 resume 行为变化导致假绿）。
	innerHiRuns := 0
	for _, in := range innerCalls {
		if in == "hi" {
			innerHiRuns++
		}
	}
	if innerHiRuns != 2 {
		t.Fatalf("前提不成立：outer.Resume 应重跑 inner.Run 第二次（input hi），实际 inner.Run(hi) %d 次", innerHiRuns)
	}

	// 核心断言：inner 的 input OutputEvent（Content=="hi"）只 1 个。
	count := 0
	for _, e := range sess.Events() {
		if e.AgentName == "inner" && e.Output != nil && e.Output.Message != nil && e.Output.Message.Content == "hi" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("inner input event 应只 1 个（嵌套 Resume guard 防 dup），实际 %d（audit MED-2）", count)
	}
}
