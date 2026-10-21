package agent_v2_test

import (
	"context"
	"strings"

	"crew-go/model"
	"crew-go/schema"
)

// captureMsgsStub 捕获每次 LLM 调用收到的 messages——被多个 skill / history 测试共用。
type captureMsgsStub struct {
	responses []*schema.Message
	calls     int
	allCalls  [][]*schema.Message // 每次 Generate 收到的完整 msgs
	lastTools []string
}

func (m *captureMsgsStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	cloned := make([]*schema.Message, 0, len(msgs))
	for _, x := range msgs {
		if x == nil {
			cloned = append(cloned, nil)
			continue
		}
		c := *x
		cloned = append(cloned, &c)
	}
	m.allCalls = append(m.allCalls, cloned)
	if m.calls < len(m.responses) {
		out := m.responses[m.calls]
		m.calls++
		return out, nil
	}
	m.calls++
	return schema.AssistantMessage("[exhausted]"), nil
}

func (m *captureMsgsStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *captureMsgsStub) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.lastTools = nil
	for _, t := range tools {
		if t != nil {
			m.lastTools = append(m.lastTools, t.Name+":"+t.Desc)
		}
	}
	return m, nil
}

var _ model.ChatModel = (*captureMsgsStub)(nil)

// concatContent 把 msgs 全部 content 串起来（含 nil 跳过），便于 substring 断言。
func concatContent(msgs []*schema.Message) string {
	var sb strings.Builder
	for _, m := range msgs {
		if m == nil {
			continue
		}
		sb.WriteString(string(m.Role))
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteString("\n")
	}
	return sb.String()
}
