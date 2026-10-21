package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

type seqStep1Model struct{}

func (m *seqStep1Model) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("S1_PRODUCT_MARK"), nil
}
func (m *seqStep1Model) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *seqStep1Model) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type seqStep2CaptureModel struct {
	msgs []*schema.Message
}

func (m *seqStep2CaptureModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.msgs = append(m.msgs, msgs...)
	return schema.AssistantMessage("step2-done"), nil
}
func (m *seqStep2CaptureModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *seqStep2CaptureModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// TestSequential_StepData_ViaStitchedText 锁定 SequentialAgent step 间数据通路真相（修正
// #1b godoc bug——旧 godoc 称 step2 从 SelfHistoryCompacted 读 step1 OutputEvent）：
// step2 通过**拼接的 user message**（"上一步产出"）拿到 step1 产出，**看不到** step1 的
// assistant OutputEvent（owner-aware SelfHistoryCompacted 只看自己 name）。防 godoc 再 stale。
func TestSequential_StepData_ViaStitchedText(t *testing.T) {
	step1 := agent_v2.NewBasicAgent(agent_v2.WithName("step1"), agent_v2.WithModel(&seqStep1Model{}))
	capModel := &seqStep2CaptureModel{}
	step2 := agent_v2.NewBasicAgent(agent_v2.WithName("step2"), agent_v2.WithModel(capModel))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("step1", step1).AddStep("step2", step2)

	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sawInUser, sawAsAssistant := false, false
	for _, m := range capModel.msgs {
		if strings.Contains(m.Content, "S1_PRODUCT_MARK") {
			switch m.Role {
			case schema.RoleUser:
				sawInUser = true
			case schema.RoleAssistant:
				sawAsAssistant = true
			}
		}
	}
	if !sawInUser {
		t.Error("step2 应通过拼接 user message 拿到 step1 产出（SequentialAgent step 间数据通路 = 拼接文本）")
	}
	if sawAsAssistant {
		t.Error("step2 不应看到 step1 的 assistant OutputEvent（owner-aware SelfHistoryCompacted）——证 godoc 旧'step2 从 SelfHistoryCompacted 看 step1'表述错")
	}
}
