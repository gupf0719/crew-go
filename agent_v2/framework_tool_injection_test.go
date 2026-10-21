package agent_v2_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// boundToolsModel 记每轮绑到的工具名；首轮手上有 final_output 就调它（参数合法），否则答自由文本。
type boundToolsModel struct {
	mu     sync.Mutex
	bound  []string
	rounds [][]string
}

func (m *boundToolsModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	names := append([]string(nil), m.bound...)
	m.rounds = append(m.rounds, names)
	first := len(m.rounds) == 1
	m.mu.Unlock()
	if first && slices.Contains(names, agent_v2.FinalOutputToolName) {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: `{"answer":"结构化"}`},
		}}), nil
	}
	return schema.AssistantMessage("自由文本答复"), nil
}

func (m *boundToolsModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *boundToolsModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		names = append(names, t.Name)
	}
	m.mu.Lock()
	m.bound = names
	m.mu.Unlock()
	return m, nil
}

func (m *boundToolsModel) firstRound(t *testing.T) []string {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rounds) == 0 {
		t.Fatal("fixture 没搭对：模型一次都没被调")
	}
	return m.rounds[0]
}

func answerSchema() map[string]any {
	return map[string]any{"type": "object",
		"properties": map[string]any{"answer": map[string]any{"type": "string"}}, "required": []any{"answer"}}
}

func kitNames(kit *tool.Toolkit) []string {
	out := make([]string, 0, len(kit.List()))
	for _, tl := range kit.List() {
		out = append(out, tl.Name())
	}
	return out
}

// TestFrameworkTools_SharedToolkitNotWritten：两个 agent 共用一个业务 toolkit，A 配了 WithOutputSchema +
// WithAskUser，B 什么都没配——业务的 toolkit 一件都不多，B 的模型看不到 final_output / ask_user，B 照常以
// 自由文本收尾；A 自己照常拿到这两件。曾经两个 option 直接注册进业务的 toolkit：B 也拿到了，模型一调 B 就拿
// A 的 schema JSON 当自己的最终答复收尾，`err == nil`。
func TestFrameworkTools_SharedToolkitNotWritten(t *testing.T) {
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("biz", "业务工具", map[string]any{"type": "object"},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))

	mA := &boundToolsModel{}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithModel(mA),
		agent_v2.WithToolkit(kit), agent_v2.WithOutputSchema("A", answerSchema()), agent_v2.WithAskUser())
	mB := &boundToolsModel{}
	b := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(mB), agent_v2.WithToolkit(kit))

	if got := kitNames(kit); !slices.Equal(got, []string{"biz"}) {
		t.Fatalf("构造之后业务的 toolkit 被写入了框架工具：%v", got)
	}
	eventsB, err := b.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("B Run: %v", err)
	}
	if got := mB.firstRound(t); !slices.Equal(got, []string{"biz"}) {
		t.Errorf("没配结构化输出 / ask_user 的 B 不该看到框架那两件，实际 %v", got)
	}
	if final := agent_v2.FinalMessage(eventsB); final == nil || final.Content != "自由文本答复" {
		t.Errorf("B 应以自己的自由文本收尾，实际 %+v", final)
	}

	if _, err := a.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("A Run: %v", err)
	}
	if got := mA.firstRound(t); !slices.Contains(got, agent_v2.FinalOutputToolName) || !slices.Contains(got, agent_v2.AskUserToolName) {
		t.Errorf("A 应拿到 final_output 与 ask_user，实际 %v", got)
	}
	if got := kitNames(kit); !slices.Equal(got, []string{"biz"}) {
		t.Errorf("A 跑完之后业务的 toolkit 被写入了框架工具：%v", got)
	}
}

// TestFrameworkTools_WithToolkitAfterOptIn：WithToolkit 排在 WithOutputSchema / WithAskUser 之后，而且业务
// toolkit 里有一件自己的 final_output——框架那件照样生效：模型看到的 final_output 是框架的，调了以后按 schema
// 校验、以结构化 JSON 收尾，业务那件一次都没被调。曾经 WithToolkit 整个替换 toolkit，框架那件丢了，而构造期
// 守卫按名字找 final_output、找到的是业务那件就放行了。
func TestFrameworkTools_WithToolkitAfterOptIn(t *testing.T) {
	var bizCalls atomic.Int32
	bizKit := tool.NewToolkit()
	bizKit.Register(tool.NewFuncTool(agent_v2.FinalOutputToolName, "业务自己的同名工具", map[string]any{"type": "object"},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			bizCalls.Add(1)
			return tool.Success("业务那件"), nil
		}))

	m := &boundToolsModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("A", answerSchema()), agent_v2.WithAskUser(), agent_v2.WithToolkit(bizKit))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := m.firstRound(t); !slices.Contains(got, agent_v2.AskUserToolName) {
		t.Errorf("WithToolkit 放在后面，ask_user 也该在，实际 %v", got)
	}
	if n := bizCalls.Load(); n != 0 {
		t.Errorf("框架那件 final_output 应顶掉业务同名的，业务那件被调了 %d 次", n)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != `{"answer":"结构化"}` {
		t.Errorf("应以校验过的结构化 JSON 收尾，实际 %+v", final)
	}
	if got := kitNames(bizKit); !slices.Equal(got, []string{agent_v2.FinalOutputToolName}) {
		t.Errorf("业务的 toolkit 不该被改：%v", got)
	}
}
