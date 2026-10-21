package tutorial_test

// adv mock 共享 helper：给 adv_xx_mock_test.go 用的轻量 stub ChatModel。
// 与 adv_helpers_test.go 互补——后者带 //go:build integration tag 仅集成测试编译，
// 本文件不带 tag 默认编译，给 mock 镜像测试用。

import (
	"context"
	"testing"
	"time"

	"crew-go/model"
	"crew-go/schema"
)

// ctx60sMock 给 mock 测试用的 60s 超时 ctx（mock 不调真 LLM 实际<1s 完成，
// 60s 给 fan-out 多分支 + retry 留余量）。adv_helpers_test.go 的 ctx60s 在
// integration tag 里看不到，所以独立定义。
func ctx60sMock(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// stubModel 是按"system prompt 关键词触发响应"模式的简易 ChatModel。
// 每个 adv_xx_mock_test.go 按场景自定义 reply 闭包，避免脚本式响应在多轮场景脆弱。
//
// reply 签名：(systemPrompt, lastUserMsg, allMessages) → response
// 闭包内部按场景判断返 tool_call / final / 多轮策略。
type stubModel struct {
	reply func(systemPrompt, lastUser string, all []*schema.Message) *schema.Message
	// 可选：记录每次调用的 input messages 供测试断言
	calls    int
	lastSeen []*schema.Message
}

func (s *stubModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.calls++
	s.lastSeen = msgs
	sysPrompt := ""
	lastUser := ""
	for _, m := range msgs {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleSystem && sysPrompt == "" {
			sysPrompt = m.Content
		}
		if m.Role == schema.RoleUser {
			lastUser = m.Content
		}
	}
	if s.reply == nil {
		return schema.AssistantMessage(""), nil
	}
	return s.reply(sysPrompt, lastUser, msgs), nil
}

func (s *stubModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := s.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(resp), nil
}

func (s *stubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return s, nil
}
