package main

import (
	"context"
	"fmt"
	"strings"

	"crew-go/model"
	"crew-go/schema"
)

// mockWeatherModel 为 -mock 模式提供脚本化 LLM 响应。
//
// 路由策略（非常简化的关键字匹配）：
//   - input 含 "天气" / "气温" / 城市名 → 返回 get_weather tool_call
//   - input 是 "刚才第一个城市叫什么"等回顾性问题 → 走文本回答
//   - 其它 → 通用占位回复
//
// 多轮 demo 想看真实 LLM 的上下文推断（"明天呢" → 推断仍是上一城市）需要真 LLM，
// mock 只能展示工具调用 + 回复的事件流。
type mockWeatherModel struct {
	tools []*schema.ToolInfo
}

func newMockWeatherModel() *mockWeatherModel {
	return &mockWeatherModel{}
}

func (m *mockWeatherModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mockWeatherModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) == 0 {
		return schema.AssistantMessage("[mock] 请说点什么"), nil
	}
	// 取最后一条 user 消息作为路由依据
	var userText string
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			userText = msgs[i].Content
			break
		}
	}

	// 检查"当前轮次"是否已有 tool_result——倒扫到最近一条 user 之间的 tool_result
	// （只看本轮，不能看历史轮的，否则 round 2 没调工具也会路由到 summarize 分支）
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break // 遇到 user 边界停止——本轮没有 tool_result
		}
		if msgs[i].Role == schema.RoleTool {
			content := msgs[i].Content
			return schema.AssistantMessage(fmt.Sprintf("[mock] 根据查询：%s", content)), nil
		}
	}

	// 检查是否是天气问题
	city := extractCity(userText)
	date := extractDate(userText)
	if city != "" {
		// 返回 tool_call
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:   fmt.Sprintf("call_%s_%s", city, date),
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "get_weather",
				Arguments: fmt.Sprintf(`{"city":"%s","date":"%s"}`, city, date),
			},
		}}), nil
	}

	// 回顾性问题
	if strings.Contains(userText, "第一个城市") || strings.Contains(userText, "刚才") {
		// 简单从历史里找第一个 user 消息提到的城市
		for _, m := range msgs {
			if m.Role != schema.RoleUser {
				continue
			}
			if c := extractCity(m.Content); c != "" {
				return schema.AssistantMessage(fmt.Sprintf("[mock] 第一个询问的城市是 %s", c)), nil
			}
		}
	}

	return schema.AssistantMessage("[mock] 我是天气助手，请问哪里的天气？例如：北京天气如何"), nil
}

func (m *mockWeatherModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

func extractCity(text string) string {
	for _, c := range []string{"北京", "上海", "广州", "深圳", "杭州"} {
		if strings.Contains(text, c) {
			return c
		}
	}
	return ""
}

func extractDate(text string) string {
	switch {
	case strings.Contains(text, "明天"):
		return "tomorrow"
	case strings.Contains(text, "后天"):
		return "tomorrow"
	default:
		return "today"
	}
}

var _ model.ChatModel = (*mockWeatherModel)(nil)
