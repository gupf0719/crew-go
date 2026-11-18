package agent_v2

import (
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestReactiveCompact_ToolMediaWithShortText(t *testing.T) {
	m := schema.ToolResultMessage("image", "图片说明")
	image := m.ContentBlocks[0].Toolresult
	image.Parts = []schema.ToolResultPart{{Text: m.Content, MediaType: "image/png", Data: "QUJD"}}
	voice := &schema.ToolresultContentBlock{ToolCallId: "voice", Content: "这段录音属于另一个调用"}
	m.ContentBlocks = append(m.ContentBlocks, &schema.ContentBlock{Toolresult: voice})
	input := []*schema.Message{m}
	before := compose_v2.EstimateMessageTokens(input)
	out := shrinkOversizedInPlace(input, 0)
	if after := compose_v2.EstimateMessageTokens(out); after >= before {
		t.Fatalf("附件应该计入 token 估算并能被紧急清理，估算 %d -> %d", before, after)
	}
	gotImage, gotVoice := out[0].ContentBlocks[0].Toolresult, out[0].ContentBlocks[1].Toolresult
	if gotImage.ToolCallId != "image" || gotImage.Content != "图片说明" || len(gotImage.Parts) != 0 ||
		gotVoice.ToolCallId != "voice" || gotVoice.Content != voice.Content || len(image.Parts) != 1 {
		t.Fatalf("只移除送模副本的附件并保持各自结果，实际 image=%+v voice=%+v", gotImage, gotVoice)
	}
}

func TestMicroCompact_ToolMediaDroppedWithOldResult(t *testing.T) {
	result := func(id, text, data string) *schema.Message {
		m := schema.ToolResultMessage(id, text)
		m.ContentBlocks[0].Toolresult.Parts = []schema.ToolResultPart{{Text: text, MediaType: "image/png", Data: data}}
		return m
	}
	call := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Function: schema.FunctionCall{Name: "capture"}}})
	}
	old, recent := result("old", "早期截图", "QUJD"), result("recent", "最新截图", "REVG")
	out := MicroCompactRun(t.Context(), []*schema.Message{call("old"), old, call("recent"), recent}, &MicroCompactConfig{
		Enabled: true, CompactableTools: []string{"capture"}, KeepRecent: 1,
	})
	oldResult, recentResult := out[1].ContentBlocks[0].Toolresult, out[3].ContentBlocks[0].Toolresult
	if oldResult.ToolCallId != "old" || len(oldResult.Parts) != 0 || out[1].Content == old.Content ||
		len(recentResult.Parts) != 1 || recentResult.Parts[0].Data != "REVG" || len(old.ContentBlocks[0].Toolresult.Parts) != 1 {
		t.Fatalf("压缩应保留配对、较新图片与历史原件，实际 old=%+v recent=%+v", oldResult, recentResult)
	}
}
