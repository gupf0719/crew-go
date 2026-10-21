package model

import (
	"context"

	"crew-go/schema"
)

// ChatModel 是核心的 LLM 抽象。实现类包装特定的提供商。
type ChatModel interface {
	// Generate 将消息发送到模型并返回单个响应。
	Generate(ctx context.Context, messages []*schema.Message, opts ...Option) (*schema.Message, error)

	// Stream 发送消息并返回流读取器。
	Stream(ctx context.Context, messages []*schema.Message, opts ...Option) (*schema.StreamReader[*schema.Message], error)

	// WithTools 返回一个与给定工具绑定的新 ChatModel 实例。
	// 返回的实例在每个请求中都包含工具定义。
	WithTools(tools []*schema.ToolInfo) (ChatModel, error)
}
