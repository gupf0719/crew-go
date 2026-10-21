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

// R33 Reflect 拆图回归（actor → evaluate → cond → [End | feedback]）。
//
// RF-H1: 原 evaluator+reflectorAgent 串单 eval 节点——reflectorAgent 中断 Resume
// 必重评 evaluator（LLM 非确定 verdict 翻面 → 用户答案静默丢）。拆后 verdict
// 在 evaluate 超步 commit，Resume 只重跑 feedback 节点。
// RF-M2: evaluator 收 user-role 合成消息（含原始目标）；reflectorAgent 收
// evaluator 批评全文。
// RF-M4: budget 判定在 feedback 之前——maxRounds 耗尽轮 reflectorAgent 不白跑；
// final.Annotations 标 reflect_goal_matched / reflect_rounds。
//
// sanity-revert 说明：拆图是结构性变更（agent_v2/reflect_builder.go 整文件重写 +
// Reflector 签名变），整体 revert 后本文件引用的新 API（ReflectFeedbackFn 等）
// 编译失败——old/new 行为分叉由断言设计保证：旧拓扑下 RF-H1 测试 evaluator
// calls=3（Resume 重评）必 fail、RF-M4 测试 reflectorAgent calls=1（耗尽轮白跑）
// 必 fail（CLAUDE.md "全文件 revert 测试引用新 API 编译失败"既有局限的论证式替代）。

// countingAgent 包装 Agent 计数调用 + 捕获最近一次 input。
type countingAgent struct {
	inner agent_v2.Agent
	calls int
	last  *schema.Message
}

func (c *countingAgent) Name() string        { return c.inner.Name() }
func (c *countingAgent) Description() string { return c.inner.Description() }
func (c *countingAgent) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	c.calls++
	c.last = input
	return c.inner.Run(ctx, sess, input, opts...)
}
func (c *countingAgent) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return c.inner.Resume(ctx, sid, opts...)
}

// seqReplyAgent 按调用次序返回固定回复序列（产 ExitEvent）。
type seqReplyAgent struct {
	name    string
	replies []string
	idx     int
}

func (f *seqReplyAgent) Name() string        { return f.name }
func (f *seqReplyAgent) Description() string { return "seq reply" }
func (f *seqReplyAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	reply := f.replies[len(f.replies)-1]
	if f.idx < len(f.replies) {
		reply = f.replies[f.idx]
	}
	f.idx++
	ev := agent_v2.NewExitEvent(f.name, schema.AssistantMessage(reply))
	sess.AddEvent(ev)
	return []*agent_v2.AgentEvent{ev}, nil
}
func (f *seqReplyAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// buildAskReflectorAgent 真 BasicAgent + ask tool（首跑抛 interrupt，Resume 后
// 把答案拼进改进意见）——走真实嵌套 cp 协议。
func buildAskReflectorAgent() *agent_v2.BasicAgent {
	askTool := tool.NewFuncTool(
		"rf_ask",
		"ask user",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "rf_ask_id"); ok {
				return tool.Success("用户补充：" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "rf_ask_id", Question: "改进方向？"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)
	tc := schema.ToolCall{ID: "rf_tc", Function: schema.FunctionCall{Name: "rf_ask", Arguments: `{}`}}
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("coach"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}), // 轮 0：问用户
			schema.AssistantMessage("建议：ADVICE_WITH_ANSWER"),       // 拿到答案后产意见
		})),
		agent_v2.WithToolkit(tk),
	)
}

// TestR33_ReflectSplit_FeedbackInterrupt_NoReeval 复现 RF-H1：
//
// evaluator 恒"不过"（轮 0）→ feedback 内 reflectorAgent ask_user 中断 → Resume
// 只重跑 feedback 节点：evaluator **不重评**（拆图前 Resume 重跑整 eval 节点 →
// evaluator calls 多 1 次且 LLM 非确定时 verdict 翻面、用户答案被静默跳过）。
func TestR33_ReflectSplit_FeedbackInterrupt_NoReeval(t *testing.T) {
	// evaluator：轮 0 "no"（不过）、轮 1 "yes"（过）——确定性序列
	evaluator := &countingAgent{inner: &seqReplyAgent{name: "judge", replies: []string{"缺监控方案", "yes"}}}
	coach := buildAskReflectorAgent()
	actor := &seqReplyAgent{name: "writer", replies: []string{"DRAFT_1", "DRAFT_2_IMPROVED"}}

	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("r33_split"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectorAgent(coach),
		agent_v2.WithReflectMaxRounds(3),
	).WithActor(actor)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := ra.Run(ctx, sess, schema.UserMessage("写方案"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expect feedback 内 ask_user interrupt, got %v", err)
	}
	if got := evaluator.calls; got != 1 {
		t.Fatalf("中断时 evaluator 应已评 1 次, got %d", got)
	}

	events, err := ra.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("加 Prometheus")}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// RF-H1 核心断言：Resume 只重跑 feedback——evaluator 总计恰 2 次
	//（轮 0 一次 + 轮 1 正常评估一次）。旧拓扑 Resume 重跑整 eval 节点 →
	// evaluator 重评轮 0 → 总计 3 次（或翻面时 2 次但答案被跳过）。
	if got := evaluator.calls; got != 2 {
		t.Fatalf("RF-H1: evaluator 应恰评 2 次（Resume 不重评轮 0），got %d", got)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "DRAFT_2_IMPROVED") {
		t.Fatalf("final 应是改进轮产物（用户答案被消费、循环走完），got %v", final)
	}
}

// TestR33_ReflectSplit_InputProtocol 复现 RF-M2：
//
// evaluator 收到 user-role 合成消息（含原始目标 + actor 输出，非 actor 的
// assistant 消息原样直传）；reflectorAgent 收到 evaluator 批评全文。
func TestR33_ReflectSplit_InputProtocol(t *testing.T) {
	evaluator := &countingAgent{inner: &seqReplyAgent{name: "judge2", replies: []string{"缺少灾备章节", "yes"}}}
	coach := &countingAgent{inner: &seqReplyAgent{name: "coach2", replies: []string{"补灾备"}}}
	actor := &seqReplyAgent{name: "writer2", replies: []string{"DRAFT_A", "DRAFT_B"}}

	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("r33_proto"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectorAgent(coach),
		agent_v2.WithReflectMaxRounds(3),
	).WithActor(actor)

	if _, err := ra.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("GOAL_X 设计方案")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// evaluator 输入：user-role + 含原始目标 + 含 actor 输出
	if evaluator.last == nil {
		t.Fatal("evaluator 没收到输入")
	}
	if evaluator.last.Role != schema.RoleUser {
		t.Errorf("RF-M2: evaluator 输入应为 user-role 合成消息（非 assistant 直传），got %v", evaluator.last.Role)
	}
	if !strings.Contains(evaluator.last.Content, "GOAL_X") {
		t.Errorf("RF-M2: evaluator 应看到原始目标 GOAL_X，got %q", evaluator.last.Content)
	}
	if !strings.Contains(evaluator.last.Content, "DRAFT_") {
		t.Errorf("evaluator 应看到 actor 输出，got %q", evaluator.last.Content)
	}
	// reflectorAgent 输入：含 evaluator 批评全文
	if coach.last == nil {
		t.Fatal("reflectorAgent 没收到输入")
	}
	if !strings.Contains(coach.last.Content, "缺少灾备章节") {
		t.Errorf("RF-M2: reflectorAgent 应收到 evaluator 批评全文，got %q", coach.last.Content)
	}
	if !strings.Contains(coach.last.Content, "GOAL_X") {
		t.Errorf("RF-M2: reflectorAgent 应看到原始目标，got %q", coach.last.Content)
	}
}

// TestR33_ReflectSplit_BudgetBeforeFeedback 复现 RF-M4：
//
// maxRounds=1：轮 0 evaluator 不过 + nextRound>=maxRounds → 直接 exit——
// reflectorAgent **不被调**（旧实现耗尽轮 reflectorAgent 白跑产意见无人消费）；
// final.Annotations 标 reflect_goal_matched=false + reflect_rounds=1（业务可
// 区分"达标退出"vs"预算耗尽兜底"）。
func TestR33_ReflectSplit_BudgetBeforeFeedback(t *testing.T) {
	evaluator := &countingAgent{inner: &seqReplyAgent{name: "judge3", replies: []string{"不行"}}}
	coach := &countingAgent{inner: &seqReplyAgent{name: "coach3", replies: []string{"WASTED_ADVICE"}}}
	actor := &seqReplyAgent{name: "writer3", replies: []string{"ONLY_DRAFT"}}

	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("r33_budget"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectorAgent(coach),
		agent_v2.WithReflectMaxRounds(1),
	).WithActor(actor)

	events, err := ra.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("写"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := coach.calls; got != 0 {
		t.Fatalf("RF-M4: maxRounds 耗尽轮 reflectorAgent 不应被调（budget 判定在 feedback 之前），got %d 次", got)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "ONLY_DRAFT") {
		t.Fatalf("耗尽兜底应返最后 actor 输出，got %v", final)
	}
	if v, ok := final.Annotations["reflect_goal_matched"].(bool); !ok || v {
		t.Errorf("RF-M4: final 应标 reflect_goal_matched=false（耗尽兜底可区分），got %v", final.Annotations)
	}
	if v, ok := final.Annotations["reflect_rounds"].(int); !ok || v != 1 {
		t.Errorf("RF-M4: final 应标 reflect_rounds=1，got %v", final.Annotations)
	}
}

// TestR33_ReflectSplit_GoalMatchedAnnotation 验证达标路径 annotation：
// 轮 0 直接过 → reflect_goal_matched=true + reflect_rounds=1。
func TestR33_ReflectSplit_GoalMatchedAnnotation(t *testing.T) {
	evaluator := &countingAgent{inner: &seqReplyAgent{name: "judge4", replies: []string{"yes"}}}
	actor := &seqReplyAgent{name: "writer4", replies: []string{"GOOD_DRAFT"}}

	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("r33_match"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectMaxRounds(3),
	).WithActor(actor)

	events, err := ra.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("写"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("no final")
	}
	if v, ok := final.Annotations["reflect_goal_matched"].(bool); !ok || !v {
		t.Errorf("达标退出应标 reflect_goal_matched=true，got %v", final.Annotations)
	}
}
