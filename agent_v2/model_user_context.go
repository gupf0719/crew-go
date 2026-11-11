package agent_v2

import (
	"context"
	"slices"

	"crew-go/schema"
)

type modelUserContextKey struct{}

func (modelUserContextKey) ExecutionState() {}

// WithModelUserContext 为本次调用补充用户级文本。BasicAgent 和 LLMPlanner 仅在送模时注入，
// 对话消息、历史和存档保持原样。
func WithModelUserContext(ctx context.Context, data string) context.Context {
	return context.WithValue(ctx, modelUserContextKey{}, data)
}

func withModelUserData(ctx context.Context, messages []*schema.Message) []*schema.Message {
	data, _ := ctx.Value(modelUserContextKey{}).(string)
	if data == "" {
		return messages
	}
	for i := len(messages) - 1; i >= 0; i-- {
		if msg := messages[i]; msg != nil && msg.Role == schema.RoleUser && schema.GetToolResultID(msg) == "" {
			out := slices.Clone(messages)
			updated := *msg
			updated.Content += "\n\n" + data
			out[i] = &updated
			return out
		}
	}
	return append(slices.Clone(messages), schema.UserMessage(data))
}
