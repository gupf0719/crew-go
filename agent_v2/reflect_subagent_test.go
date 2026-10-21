package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestReflect_SubAgent_Evaluator 验证 sub-agent evaluator 路径——按 parseGoalMatched
// 严格匹配语义（switch "yes/ok/approved/true/passed/good" 全等，非 Contains）。
//
// **R11 跨仓 audit A1 修订（第 1 类炸弹变体：fixture 通过元素恰好原位）**：
// 旧 fixture evaluator 第 1 轮返 "no" / 第 2 轮 "yes"——switch 严格匹配 vs Contains
// 多关键词或行为完全一致（"no" 都为 false，"yes" 都为 true）。sanity-revert 把
// parseGoalMatched 改成 Contains 测试仍 PASS（HIGH 假绿）。
//
// 新 fixture 第 1 轮返 "noted: not yes still draft"（含 "yes" 子串但**不全等**）：
//   - parseGoalMatched 严格 switch：strings.ToLower(...) != "yes" → false → 进第 2 轮
//   - 假想 Contains 实现：strings.Contains(..., "yes") → true → 第 1 轮就退出
//
// → fixture 让两实现行为分叉，sanity-revert 真能 catch。
func TestReflect_SubAgent_Evaluator(t *testing.T) {
	// 第 1 轮 actor 输出 "draft"；evaluator 看 → 含 "yes" 但**不全等**于 yes
	//                          ↳ 严格 switch 判 false → 进第 2 轮
	//                          ↳ Contains 实现判 true → 第 1 轮直接退出（错）
	// 第 2 轮 actor 输出 "final"；evaluator 看 → "yes" → 退出
	actorStub := newStubModel([]*schema.Message{
		schema.AssistantMessage("draft"),
		schema.AssistantMessage("final"),
	})
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(actorStub))

	evalCount := 0
	evaluator := &fixedAgentDynamic{
		name: "eval",
		respFn: func() *schema.Message {
			evalCount++
			if evalCount == 1 {
				// **R11 A1 关键诱饵**：含 "yes" 子串但 strings.ToLower trim 后 != "yes"
				return schema.AssistantMessage("noted: not yes still draft")
			}
			return schema.AssistantMessage("yes")
		},
	}
	reflector := &fixedAgent{name: "reflector", resp: schema.AssistantMessage("再清晰些")}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectorAgent(reflector),
	).WithActor(actor)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("write"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "final") {
		t.Errorf("R11 A1 回归（parseGoalMatched 严格 switch）：第 1 轮 evaluator 含 'yes' 子串"+
			"但不全等，应继续第 2 轮拿 'final'。若退化为 Contains 实现会第 1 轮就退 'draft'。"+
			"实际 final=%v", final)
	}
	if evalCount != 2 {
		t.Errorf("R11 A1 回归：evaluator 应被调 2 次（第 1 轮判不全等→重试），实际 %d 次", evalCount)
	}
}

// TestReflect_SubAgent_AnnotationGoalMatched 验证 evaluator 通过 annotation 决定。
func TestReflect_SubAgent_AnnotationGoalMatched(t *testing.T) {
	actorStub := newStubModel([]*schema.Message{schema.AssistantMessage("immediate")})
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"), agent_v2.WithModel(actorStub))

	evalMsg := schema.AssistantMessage("don't trust the content; use annotation")
	evalMsg.Annotations = map[string]any{"goal_matched": true}
	evaluator := &fixedAgent{name: "eval", resp: evalMsg}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithEvaluator(evaluator),
	).WithActor(actor)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "immediate") {
		t.Errorf("expect immediate exit on goal_matched=true, got %v", final)
	}
}

// fixedAgentDynamic 是动态版 fixedAgent——每次 Run 调 respFn() 拿 message。
type fixedAgentDynamic struct {
	name   string
	respFn func() *schema.Message
}

func (a *fixedAgentDynamic) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	resp := a.respFn()
	if sess != nil {
		sess.AddEvent(agent_v2.NewExitEvent(a.name, resp))
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, resp)}, nil
}
func (a *fixedAgentDynamic) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *fixedAgentDynamic) Name() string        { return a.name }
func (a *fixedAgentDynamic) Description() string { return "" }
