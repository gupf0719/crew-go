package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

// TestReflectAgent_GoalMatchedExitsEarly 验证 Reflector 返 GoalMatched=true 时立即退出。
func TestReflectAgent_GoalMatchedExitsEarly(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("draft1"),
		schema.AssistantMessage("never"),
	})
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(stub))

	reflector := func(_ context.Context, _, out *schema.Message) (*agent_v2.ReflectOpinion, error) {
		// 第一轮就接受
		return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithReflector(reflector),
	).WithActor(actor)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "draft1") {
		t.Errorf("expect 'draft1' (single-round exit), got %v", final)
	}
}

// TestReflectAgent_RetryUntilGoalMatched 验证不达标时按 Opinion 反馈给 actor 重跑。
//
// **R10 跨仓 audit A1 修订（第 5 类炸弹：mock 不读 input）**：
// 旧版用 newStubModel 按 idx 返固定 sequence，actor 第二轮 input 是否含 opinion / 上轮
// outContent 完全不验证 → sanity-revert 改 nextInput 为固定字符串测试仍 PASS（HIGH 假绿）。
//
// 新版用 captureInputStub 捕获 actor 第二轮 user message 内容并断言含：
//   - 业务原始 input "write"（origContent）
//   - reflector opinion "请改进措辞"（feedback）
//   - actor 上轮回答 "draft"（outContent）
//
// → 反 reflect_agent.go::buildNextActorInput 真把这三个拼到 actor input。
// sanity-revert 把 nextInput 改成不含 opinion → captured 不含 "请改进措辞" → fail。
func TestReflectAgent_RetryUntilGoalMatched(t *testing.T) {
	// 用 captureInputStub 捕获 actor 第 2 轮 input；返"final"让 reflector accept
	// stub 自身按 calls 计数返不同响应（第 1 轮 draft / 第 2 轮 final）
	captured := ""
	stub := &reflectActorStub{
		captured: &captured,
		responses: []*schema.Message{
			schema.AssistantMessage("draft"),
			schema.AssistantMessage("final"),
		},
	}
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(stub))

	reflector := func(_ context.Context, _, out *schema.Message) (*agent_v2.ReflectOpinion, error) {
		if strings.Contains(out.Content, "final") {
			return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
		}
		return &agent_v2.ReflectOpinion{GoalMatched: false, Opinion: "请改进措辞"}, nil
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithReflector(reflector),
	).WithActor(actor)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("write"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "final") {
		t.Errorf("expect retry-then-accept 'final', got %v", final)
	}

	// **R10 audit A1 加的关键断言**：actor 第 2 轮 input 必须含 opinion 反馈链路全要素
	// （而不是按 idx 返固定 sequence）。任一断言 fail 都精确指向"reflect 反馈链路断"。
	if !strings.Contains(captured, "请改进措辞") {
		t.Errorf("R10 A1 回归：actor 第 2 轮 input 应含 reflector opinion '请改进措辞'，实际 %q", captured)
	}
	if !strings.Contains(captured, "draft") {
		t.Errorf("R10 A1 回归：actor 第 2 轮 input 应含上轮 outContent 'draft'，实际 %q", captured)
	}
	if !strings.Contains(captured, "write") {
		t.Errorf("R10 A1 回归：actor 第 2 轮 input 应保留原始 origContent 'write'，实际 %q", captured)
	}
}

// reflectActorStub 是 ReflectAgent 测试专用 stub：按 calls 索引返不同响应 + 捕获
// 第 ≥2 次调用的最后 user message 内容（验 actor 拿到 reflect 反馈）。
type reflectActorStub struct {
	captured  *string
	responses []*schema.Message
	calls     int
}

func (s *reflectActorStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.calls++
	// 仅在第 ≥2 次调用时捕获——第 1 次是 actor 第 1 轮，没 reflect 反馈
	if s.calls >= 2 {
		for i := len(msgs) - 1; i >= 0; i-- {
			if msgs[i] != nil && msgs[i].Role == schema.RoleUser {
				*s.captured = msgs[i].Content
				break
			}
		}
	}
	if s.calls-1 < len(s.responses) {
		return s.responses[s.calls-1], nil
	}
	return schema.AssistantMessage("[exhausted]"), nil
}

func (s *reflectActorStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *reflectActorStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

var _ model.ChatModel = (*reflectActorStub)(nil)

// TestReflectAgent_MaxRoundsExitsAnyway 验证 reflector 永不满意时按 maxRounds 兜底退出。
func TestReflectAgent_MaxRoundsExitsAnyway(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("r1"),
		schema.AssistantMessage("r2"),
		schema.AssistantMessage("r3"),
	})
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(stub))

	reflector := func(_ context.Context, _, _ *schema.Message) (*agent_v2.ReflectOpinion, error) {
		// 永远 GoalMatched=false → maxRounds 兜底
		return &agent_v2.ReflectOpinion{GoalMatched: false, Opinion: "再来"}, nil
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(3),
		agent_v2.WithReflector(reflector),
	).WithActor(actor)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "r3") {
		t.Errorf("expect maxRounds exit on 'r3', got %v", final)
	}
}

// TestReflectAgent_EvalFuncFallback 验证未设 Reflector 时退化到 EvalFunc。
func TestReflectAgent_EvalFuncFallback(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("bad"),
		schema.AssistantMessage("good"),
	})
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(stub))

	evalFn := func(_ context.Context, out *schema.Message) (bool, error) {
		return strings.Contains(out.Content, "good"), nil
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithEvalFunc(evalFn),
	).WithActor(actor)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "good") {
		t.Errorf("expect EvalFunc-driven exit on 'good', got %v", final)
	}
}

// TestReflectAgent_NoActorFails 验证 actor 必填。
func TestReflectAgent_NoActorFails(t *testing.T) {
	r := agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect"))
	_, err := r.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "actor required") {
		t.Fatalf("expect 'actor required' err, got %v", err)
	}
}
