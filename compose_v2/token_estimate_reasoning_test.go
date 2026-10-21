package compose_v2_test

import (
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestEstimateMessageTokens_CountsReasoningContent 验 thinking 模型的 reasoning_content
// 计入 token 估算——多轮回传时它随 assistant 消息上 wire（model/openai convertMessages），
// 不计入会让 AutoCompact 对 reasoning-heavy 历史低估、压缩触发偏晚（ISSUE-89 follow-up）。
//
// sanity-revert：去掉 EstimateMessageTokens 的 ReasoningContent 累加后，base 与
// withReasoning 估算相等，下面断言从 pass 变 fail，精确指向漏算。
func TestEstimateMessageTokens_CountsReasoningContent(t *testing.T) {
	longReasoning := "这是一段较长的推理过程文本，用来确认 token 估算把 reasoning_content 也算进去了。" +
		"deepseek/qwen3 thinking 的推理动辄数千字符，漏算会显著低估真实 prompt 大小。"

	base := []*schema.Message{{Role: schema.RoleAssistant, Content: "最终答案"}}
	withReasoning := []*schema.Message{{Role: schema.RoleAssistant, Content: "最终答案", ReasoningContent: longReasoning}}

	baseTokens := compose_v2.EstimateMessageTokens(base)
	reasoningTokens := compose_v2.EstimateMessageTokens(withReasoning)

	if reasoningTokens <= baseTokens {
		t.Fatalf("ReasoningContent 未计入 token 估算：base=%d withReasoning=%d（应 withReasoning > base）",
			baseTokens, reasoningTokens)
	}
}
