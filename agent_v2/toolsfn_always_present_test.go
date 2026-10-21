package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 模型第一轮幻觉出一个未注册的工具名，第二轮改口直接作答。
type hallucinateModel struct{ rounds int }

func (m *hallucinateModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	m.rounds++
	if m.rounds == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Type: "function",
			Function: schema.FunctionCall{Name: "totally_unregistered_tool", Arguments: `{}`},
		}}), nil
	}
	return schema.AssistantMessage("我改口了，直接作答"), nil
}
func (m *hallucinateModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *hallucinateModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 一次工具幻觉不该炸掉整个 Run——**包括「这个 agent 压根没有业务 toolkit」这一档**。
//
// # 这条钉的是一处未声明的行为变化
//
// `makeToolsFn` 曾有一道早退 `if cfg.Toolkit == nil && !skillNeedsTool && !skillHasActions
// { return nil }`，于是没有 toolkit 的 BasicAgent 一旦收到任何 tool_call，tools 节点直接
// 报 `tools called but BasicAgent has no tools fn`——**整个 Run 带着 err 退出，模型没有第二次
// 机会**。转交能力改走签名之后那道早退被删（没被授予的候选也要跑软失败桩），行为随之改变，
// 但**当时的 commit message 没声明**。
//
// **实测的真实范围**（还原早退跑同一探针）：只有「完全没有 toolkit」那一档变了——
// 「有 toolkit 但不含该名字」两版都是软失败回灌。此前记录写成「任意未注册工具名 ×
// 任意 BasicAgent」是**夸大**。
//
// 方向是对的：候选完全没有业务 toolkit 是常态（前台只负责分诊），而一次幻觉或残留的
// tool_call 把整轮对话炸掉，对业务是纯损失。这条用例把它从「碰巧如此」钉成「说好如此」。
func TestToolsFn_HallucinatedToolDoesNotAbortRun(t *testing.T) {
	realToolkit := func() *tool.Toolkit {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("some_real_tool", "真工具",
			map[string]any{"type": "object", "properties": map[string]any{}},
			func(context.Context, map[string]any) (*tool.Result, error) {
				return &tool.Result{Success: true, Output: "ok"}, nil
			}))
		return tk
	}

	for _, tc := range []struct {
		name string
		tk   *tool.Toolkit
	}{
		{"完全没有业务 toolkit（还原早退这一档会红）", nil},
		{"有 toolkit 但不含该名字（两版行为相同）", realToolkit()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &hallucinateModel{}
			opts := []agent_v2.Option{
				agent_v2.WithName("hallu"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(4),
			}
			if tc.tk != nil {
				opts = append(opts, agent_v2.WithToolkit(tc.tk))
			}
			evs, err := agent_v2.NewBasicAgent(opts...).
				Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

			if err != nil {
				t.Fatalf("一次工具幻觉炸掉了整个 Run：err=%v", err)
			}
			// **承重在这两条上**：只断言 err==nil 不够——「什么都没发生」时它也为真。
			// 要的是「模型真的拿到了失败反馈并跑了第二轮」+「第二轮的话真的成了最终答复」。
			if m.rounds < 2 {
				t.Fatalf("模型只被调 %d 轮——它没拿到失败反馈，也就没有改口的机会", m.rounds)
			}
			final := agent_v2.FinalMessage(evs)
			if final == nil || final.Content != "我改口了，直接作答" {
				got := "<nil>"
				if final != nil {
					got = final.Content
				}
				t.Fatalf("最终答复不是模型改口那句：%q", got)
			}
		})
	}
}
