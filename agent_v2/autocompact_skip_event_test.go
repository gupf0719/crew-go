package agent_v2_test

import (
	"context"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 可压缩区太小被跳过的那一轮没有压缩：不发 Before / After（跟 shouldTrigger 的跳过一致）。曾经 Before 在跳过判断之前
// 就发，跳过的轮次各发一对，crew.compactions{crew.outcome=success} 每轮 +1、crew.compaction.ratio 恒 1.0。
func TestAutoCompact_SkippedRoundEmitsNoEvents(t *testing.T) {
	calls := 0
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               &countingSummaryModel{calls: &calls},
		ModelContextWindow:  220,
		SummaryMaxTokens:    30,
		SummaryBufferTokens: 50,
		// MinCompactableTokens 取默认 5000：可压缩区远小于它，静默跳过
	}
	msgs := []*schema.Message{schema.UserMessage("first question")}
	for i := 0; i < 6; i++ {
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("answer round %d with enough content to push token over the threshold of seventy-seven tokens we set", i)))
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("follow up %d also containing extra padding text", i)))
	}
	sink := &capturingSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	out, err := agent_v2.AutoCompactRun(ctx, cfg, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun err: %v", err)
	}
	if calls != 0 || len(out) != len(msgs) {
		t.Fatalf("fixture：应当跳过（摘要模型 %d 次、消息 %d → %d）", calls, len(msgs), len(out))
	}
	if actions := sink.compactionEvents(); len(actions) != 0 {
		t.Errorf("跳过的一轮发了 %d 个压缩事件，want 0：%+v", len(actions), actions)
	}
}

type countingSummaryModel struct{ calls *int }

func (m *countingSummaryModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	*m.calls++
	return schema.AssistantMessage("<summary>compacted</summary>"), nil
}
func (m *countingSummaryModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *countingSummaryModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}
