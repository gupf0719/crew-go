package agent_v2

import (
	"context"
	"testing"

	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// rdMintedStubModel 首轮返一条调 `route_to` 的 tool_call，之后返纯文本。
type rdMintedStubModel struct{ calls int }

func (m *rdMintedStubModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Function: schema.FunctionCall{Name: "route_to", Arguments: `{}`}}}), nil
	}
	return schema.AssistantMessage("模型自己的话"), nil
}
func (m *rdMintedStubModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(out), nil
}
func (m *rdMintedStubModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestReturnDirect_MintedVsBusinessSameName 钉住**同一个名字、铸造与否，行为必须分叉**。
//
// 「这是不是框架的工具」在本仓曾有两份判据：按名字的那张名单（开放集，已删——
// `route_to` 的构造函数是导出的，`respond`/`plan` 业务更占得到）和按铸造标记的
// `frameworkControlTool`（封闭集，`tool_memo.go` 为修「未授予方触发转交」那个真安全洞
// 立的，godoc 明写「那些名字业务占得到」）。audit F1 实测按名字判的后果：撞名工具
// 「体真跑了、短路真发生了、产出被丢了」，err=nil 无告警。
//
// 现在身份在写侧按**铸造事实**判（`ReturnDirectMintedAnnotation`），本测试钉两半：
//
//   - **业务**注册的同名 return-direct 工具（非铸造）⟹ 产出即答复（F1 修复）。
//   - **框架铸的**同名工具 ⟹ 不取产出（控制动作档；真实承重是 Handoff Rewrite 模式的
//     历史可见性——见 basic_builder.go 守卫处注释，audit F2）。
//
// 白盒原因：`markFrameworkTool` 未导出——铸造能力本来就不该给业务。
func TestReturnDirect_MintedVsBusinessSameName(t *testing.T) {
	mkTool := func() tool.Tool {
		return tool.NewFuncTool("route_to", "撞名工具", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				return &tool.Result{Success: true, Output: "撞名工具的产出",
					Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
			})
	}

	run := func(t *testing.T, tl tool.Tool) *schema.Message {
		t.Helper()
		tk := tool.NewToolkit()
		tk.Register(tl)
		ag := NewBasicAgent(
			WithName("a"), WithToolkit(tk), WithMaxRounds(3),
			WithModel(&rdMintedStubModel{}),
		)
		evs, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi"))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return FinalMessage(evs)
	}

	t.Run("业务撞名_产出即答复", func(t *testing.T) {
		final := run(t, mkTool())
		if final == nil || final.Content != "撞名工具的产出" {
			t.Errorf("业务注册的 route_to（非铸造）产出该成为答复，实际 %+v\n"+
				"——按名字判框架身份会把它当框架工具、把产出丢掉（audit F1 的实测缺陷）", final)
		}
	})

	t.Run("框架铸造_不取产出", func(t *testing.T) {
		final := run(t, markFrameworkTool(mkTool()))
		if final != nil && final.Content == "撞名工具的产出" {
			t.Errorf("框架铸的控制动作工具，产出不该成为答复（真实承重：Rewrite 模式的"+
				"历史可见性），实际 content=%q", final.Content)
		}
	})
}
