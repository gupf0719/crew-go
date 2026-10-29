package compose_v2_test

import (
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 工具结果的正文同时在 Content 与 tool_result 块里（schema.ToolResultMessage），wire 上只发一份：估算与同样长度的
// assistant 文本相同（曾经两处都算，工具结果估成两倍）。
func TestEstimateMessageTokens_ToolResultCountedOnce(t *testing.T) {
	body := strings.Repeat("x", 3000)
	text := compose_v2.EstimateMessageTokens([]*schema.Message{schema.AssistantMessage(body)})
	tr := compose_v2.EstimateMessageTokens([]*schema.Message{schema.ToolResultMessage("c1", body)})
	if tr != text {
		t.Fatalf("同样 3000 字节，工具结果估 %d、assistant 文本估 %d——正文被算了不止一次", tr, text)
	}
}

// 只有跟 Content 相同的那一份不重复算：同一条消息里第二个 tool_result 在 wire 上单独成一条消息发，内容不同的块
// 照算（宁多估）。
func TestEstimateMessageTokens_OnlyTheRepeatedBlockIsSkipped(t *testing.T) {
	body := strings.Repeat("x", 3000)
	single := compose_v2.EstimateMessageTokens([]*schema.Message{schema.ToolResultMessage("c1", body)})

	two := schema.ToolResultMessage("c1", body)
	two.ContentBlocks = append(two.ContentBlocks, &schema.ContentBlock{Type: "tool_result",
		Toolresult: &schema.ToolresultContentBlock{ToolCallId: "c2", Content: body}})
	if got := compose_v2.EstimateMessageTokens([]*schema.Message{two}); got != 2*single {
		t.Fatalf("两个 tool_result（wire 上两条消息）应估 %d，实际 %d", 2*single, got)
	}

	differs := schema.ToolResultMessage("c1", body)
	differs.ContentBlocks[0].Toolresult.Content = strings.Repeat("y", 3000)
	if got := compose_v2.EstimateMessageTokens([]*schema.Message{differs}); got != 2*single {
		t.Fatalf("块的正文与 Content 不同时两份都该算（%d），实际 %d", 2*single, got)
	}
}
