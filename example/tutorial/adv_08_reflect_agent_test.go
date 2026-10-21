//go:build integration

package tutorial_test

import (
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 8. ReflectAgent（进阶）— 技术方案评审（Evaluator + ReflectorAgent 双 sub-agent 档）
//
// 评估档位互斥（NewReflectAgent fail-fast）：本例展示 sub-agent 档——真 LLM
// evaluator 判过/不过（输出 APPROVED 命中 parseGoalMatched），reflector 产改进
// 意见。确定性 EvalFunc 档见 adv_08 mock 孪生。
// ──────────────────────────────────────────────────────────────────

func TestAdv_08_ReflectAgent(t *testing.T) {
	provider := requireAPIKey(t)

	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("proposal_writer"),
		agent_v2.WithSystemPrompt(
			"你是高级架构师，负责撰写技术方案。方案必须涵盖：\n"+
				"1. 系统架构图（文字描述）\n2. 核心技术选型（含备选方案）\n"+
				"3. 数据模型设计\n4. 接口定义（RESTful/gRPC）\n"+
				"5. 性能预估和压测方案\n6. 监控告警策略\n7. 灾备方案\n"+
				"如果收到改进意见，在下一版本中针对性修改。",
		),
		agent_v2.WithModel(newLLM(provider)),
	)

	evaluator := agent_v2.NewBasicAgent(
		agent_v2.WithName("tech_reviewer"),
		agent_v2.WithSystemPrompt(
			"你是严格的技术评审员。评估收到的技术方案是否完整，重点检查：\n"+
				"- 是否有具体技术选型（必须包含 '选型' 或 'stack'）\n"+
				"- 是否有性能指标（必须包含 QPS/TPS/延迟等数字）\n"+
				"- 是否有监控方案（必须包含 'Prometheus' 或 '监控'）\n"+
				"通过时只输出一个单词：approved（小写、不要任何其他文字）；"+
				"不通过时列出具体不足项（不要出现 approved 字样）。",
		),
		agent_v2.WithModel(newLLM(provider)),
	)

	reflector := agent_v2.NewBasicAgent(
		agent_v2.WithName("improvement_coach"),
		agent_v2.WithSystemPrompt(
			"你是技术写作教练。根据评审意见，给出 3 条具体可操作的改进建议：\n"+
				"1. [具体缺失项] 如何补充\n"+
				"格式要精准，避免泛泛而谈。",
		),
		agent_v2.WithModel(newLLM(provider)),
	)

	session := agent_v2.NewRunSession()
	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("proposal_review"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectorAgent(reflector),
		agent_v2.WithReflectMaxRounds(4),
	).WithActor(actor)

	events, err := ra.Run(ctx300s(t), session,
		schema.UserMessage("请设计一个高并发订单处理系统的技术方案（目标 QPS 10000）"))
	if err != nil {
		t.Fatalf("ReflectAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)

	t.Logf("最终方案（前 500 字符）: %.500s", out.Content)

	if out.Content == "" {
		t.Error("输出不应为空")
	}
}
