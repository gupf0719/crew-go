package tutorial_test

// adv_08 mock：ReflectAgent actor + 确定性 EvalFunc 关键词评分（EvalFunc 档）。
//
// 评估档位互斥（NewReflectAgent fail-fast）：本例展示 EvalFunc 档——关键词命中
// 真 gate 循环（第 1 轮 v1 不过 → 第 2 轮 v2 命中 5/5 通过）。sub-agent 档
//（Evaluator+ReflectorAgent）见 adv_08 真 LLM 孪生。

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

func TestAdv_08_ReflectAgent_Mock(t *testing.T) {
	round := int32(0)
	actorStub := &stubModel{reply: func(_, _ string, _ []*schema.Message) *schema.Message {
		n := atomic.AddInt32(&round, 1)
		if n == 1 {
			return schema.AssistantMessage("v1 简单架构 + 接口" + strings.Repeat("文字", 100))
		}
		return schema.AssistantMessage("v2 完整方案：技术选型 Go + Redis；数据模型 Order/User；接口 RESTful + gRPC；" +
			"性能 QPS 10000 延迟 < 50ms；监控 Prometheus + Grafana；灾备 多活" +
			strings.Repeat("详细描述", 50))
	}}
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("proposal_writer"), agent_v2.WithSystemPrompt("写方案"), agent_v2.WithModel(actorStub))

	requiredKeywords := []string{"选型", "数据模型", "接口", "性能", "监控"}
	sess := agent_v2.NewRunSession()
	evalFunc := agent_v2.EvalFunc(func(_ context.Context, output *schema.Message) (bool, error) {
		hit := 0
		for _, kw := range requiredKeywords {
			if strings.Contains(output.Content, kw) {
				hit++
			}
		}
		passed := hit >= 4 && len([]rune(output.Content)) > 300
		sess.SetValue("eval_hit", fmt.Sprintf("%d/%d", hit, len(requiredKeywords)))
		return passed, nil
	})
	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("proposal_review"),
		agent_v2.WithEvalFunc(evalFunc),
		agent_v2.WithReflectMaxRounds(4),
	).WithActor(actor)
	events, err := ra.Run(ctx60sMock(t), sess, schema.UserMessage("订单系统方案 QPS 10000"))
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("nil")
	}
	hit, _ := sess.GetValue("eval_hit")
	t.Logf("关键词: %v 最终: %.200s", hit, out.Content)
	if !strings.Contains(out.Content, "选型") || !strings.Contains(out.Content, "监控") {
		t.Errorf("最终方案应含完整关键词，实际 %q", out.Content)
	}
}
