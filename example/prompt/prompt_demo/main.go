// prompt_demo 演示 prompt 包的轻量提示词模板——变量绑定（text/template）+ 消息列表占位
// （Placeholder）渲染成 []*schema.Message，以及经 WithPromptTemplate 集成进 BasicAgent
// 做「每轮动态 system prompt」。
//
// 全程 mock（不调真 LLM），直接跑:
//
//	go run ./example/prompt/prompt_demo
package main

import (
	"context"
	"fmt"
	"log"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/prompt"
	"crew-go/schema"
)

func main() {
	fmt.Println("=== 场景 1: prompt.Template 直接渲染（变量绑定 + Placeholder 消息占位）===")
	demoDirectFormat()
	fmt.Println("\n=== 场景 2: WithPromptTemplate 集成 BasicAgent（每轮按 input 动态渲染 system prompt）===")
	demoWithAgent()
}

// demoDirectFormat: prompt.Template.Format 独立用——System/User 变量绑定 + Placeholder 插消息列表。
func demoDirectFormat() {
	tmpl := prompt.FromMessages(
		prompt.System("你是{{.role}}，只用{{.lang}}回答，语气{{.tone}}。"),
		prompt.Placeholder("history", true), // optional: 把一段历史消息插进列表（缺省则跳过）
		prompt.User("{{.question}}"),
	)
	history := []*schema.Message{
		schema.UserMessage("我叫小明"),
		schema.AssistantMessage("你好小明，有什么可以帮你？"),
	}
	msgs, err := tmpl.Format(map[string]any{
		"role": "客服助手", "lang": "中文", "tone": "热情",
		"question": "我叫什么名字？",
		"history":  history,
	})
	if err != nil {
		log.Fatalf("Format: %v", err)
	}
	for i, m := range msgs {
		fmt.Printf("  [%d] %-9s %s\n", i, m.Role, m.Content)
	}
	fmt.Println("  → System/User 的 {{.x}} 被变量替换；Placeholder(\"history\") 把 2 条历史消息插进列表")
}

// demoWithAgent: BasicAgent 经 WithPromptTemplate 集成——preamble 每轮用 varsFn 渲染模板得
// system prompt 再 prepend。适合「system prompt 随请求 / 用户 / 时间动态变」的场景。
func demoWithAgent() {
	tmpl := prompt.FromMessages(
		prompt.System("你是{{.persona}}。当前会话语言：{{.lang}}。"),
	)
	// varsFn 可从 ctx / input 动态取（用户画像 / A-B 分组 / 时间 / 语言偏好…）
	varsFn := func(_ context.Context, _ *schema.Message) map[string]any {
		return map[string]any{"persona": "一个惜字如金的助手", "lang": "中文"}
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("prompt_bot"),
		agent_v2.WithModel(&echoModel{}),
		agent_v2.WithPromptTemplate(tmpl, varsFn),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("你好"))
	if err != nil {
		log.Fatalf("Run: %v", err)
	}
	if msg := agent_v2.FinalMessage(events); msg != nil {
		fmt.Printf("  agent> %s\n", msg.Content)
	}
	fmt.Println("  → echoModel 回显收到的 system prompt，可见是模板渲染后的「惜字如金的助手」")
}

// echoModel 把收到的 system prompt 回显的 mock（证明模板渲染生效，不调真 LLM）。
type echoModel struct{}

func (m *echoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *echoModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	var sys string
	for _, msg := range msgs {
		if msg.Role == schema.RoleSystem {
			sys = msg.Content
			break
		}
	}
	return schema.AssistantMessage(fmt.Sprintf("[mock] 我的 system prompt = 「%s」", sys)), nil
}

func (m *echoModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}
