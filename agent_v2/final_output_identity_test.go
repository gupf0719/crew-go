package agent_v2_test

// 「这次调用是不是一次结构化输出的尝试」按派发层盖的事实认，不按工具名认：没配 WithOutputSchema 时
// final_output 这个名字业务占得到，那件工具就是一件普通的业务工具。曾经按名字认——业务那件同名工具成功了
// 也被记成一次失败的尝试（这一轮必须回炉），同批的业务 exit 工具不再短路，产出被丢、模型被多调一次，
// err == nil（审计实测）；它失败时、它自己是 return-direct 工具时同理。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// bizFinalOutputToolkit 是没配结构化输出的业务 toolkit：一件 return-direct 的 exit 工具，加一件业务自己
// 叫 final_output 的普通工具，它的结果由 res 给。
func bizFinalOutputToolkit(res func() *tool.Result) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("do_exit", "给出答复并结束", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return rdMeta(tool.Success("业务产出")), nil
		}))
	tk.Register(tool.NewFuncTool(agent_v2.FinalOutputToolName, "业务自己的记账工具", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return res(), nil }))
	return tk
}

// TestFinalOutputName_BusinessToolDoesNotBlockExit：没配结构化输出，同一批 [业务 exit 工具, 业务自己的
// final_output]——exit 工具照常短路，最终答复是它的产出，模型只被调一次。业务那件同名工具成功、失败都一样。
func TestFinalOutputName_BusinessToolDoesNotBlockExit(t *testing.T) {
	cases := map[string]func() *tool.Result{
		"同名工具成功": func() *tool.Result { return tool.Success("已记账") },
		"同名工具失败": func() *tool.Result { return tool.Failure("ledger_down", "记账服务不可用") },
	}
	for name, res := range cases {
		t.Run(name, func(t *testing.T) {
			m := &structModel{responses: []*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{
					{ID: "tc_exit", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}},
					{ID: "tc_fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: `{}`}},
				}),
				schema.AssistantMessage("模型又说话了"),
			}}
			events, err := agent_v2.NewBasicAgent(agent_v2.WithName("biz"), agent_v2.WithModel(m),
				agent_v2.WithToolkit(bizFinalOutputToolkit(res)), agent_v2.WithMaxRounds(3)).
				Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("办事"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			final := agent_v2.FinalMessage(events)
			if len(m.inputs) != 1 || final == nil || final.Content != "业务产出" {
				t.Errorf("exit 工具应短路收尾：模型被调 %d 次，最终 %+v", len(m.inputs), final)
			}
		})
	}
}

// TestFinalOutputName_BusinessReturnDirectFailureStillExits：没配结构化输出时，业务自己叫 final_output 的
// return-direct 工具以失败收尾，跟别的业务 exit 工具一样短路——失败内容就是它给出的答复。曾经按名字当成
// 「通过校验、又被钩子否决的结构化答复」：不短路，模型被再调一次。
func TestFinalOutputName_BusinessReturnDirectFailureStillExits(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(agent_v2.FinalOutputToolName, "给出答复并结束", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return rdMeta(tool.Failure("abort", "无法继续，请联系人工")), nil
		}))
	m := &structModel{responses: []*schema.Message{finalOutputCall(`{}`), schema.AssistantMessage("模型又说话了")}}
	events, err := agent_v2.NewBasicAgent(agent_v2.WithName("biz"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3)).
		Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("办不了的事"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if len(m.inputs) != 1 || final == nil || !strings.Contains(final.Content, "无法继续") {
		t.Errorf("应以失败内容短路收尾：模型被调 %d 次，最终 %+v", len(m.inputs), final)
	}
}

// TestFinalOutputName_BusinessReturnDirectGetsNoPriority：没配结构化输出时，业务自己叫 final_output 的
// return-direct 工具不享有结构化答复的「恒胜」——跟别的业务 exit 工具同批时按业务工具之间的规则取（目前是
// 排在前面的那个），所以两种次序各取各的第一个。按名字认的时候它在两种次序下都胜出（审计实测的变异）。
func TestFinalOutputName_BusinessReturnDirectGetsNoPriority(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("do_exit", "给出答复并结束", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return rdMeta(tool.Success("业务产出")), nil
		}))
	tk.Register(tool.NewFuncTool(agent_v2.FinalOutputToolName, "业务自己的记账工具", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return rdMeta(tool.Success("记账结果")), nil
		}))
	exit := schema.ToolCall{ID: "tc_exit", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}}
	fo := schema.ToolCall{ID: "tc_fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: `{}`}}
	for _, c := range []struct {
		name  string
		calls []schema.ToolCall
		want  string
	}{
		{"exit 在前", []schema.ToolCall{exit, fo}, "业务产出"},
		{"final_output 在前", []schema.ToolCall{fo, exit}, "记账结果"},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := &structModel{responses: []*schema.Message{
				schema.AssistantToolCallMessage(c.calls), schema.AssistantMessage("模型又说话了"),
			}}
			events, err := agent_v2.NewBasicAgent(agent_v2.WithName("biz"), agent_v2.WithModel(m),
				agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3)).
				Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("办事"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if final := agent_v2.FinalMessage(events); len(m.inputs) != 1 || final == nil || final.Content != c.want {
				t.Errorf("应以排在前面的业务 exit 工具收尾（%q）：模型被调 %d 次，最终 %+v", c.want, len(m.inputs), final)
			}
		})
	}
}

// renameToolHook 在 OnToolStart 里把名为 from 的工具调用改名成 to（名字归一化式的钩子）。
type renameToolHook struct {
	hook.NoOpHooks
	from, to string
}

func (h renameToolHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in.Name == h.from {
		in.Name = h.to
	}
	return ctx, nil
}

// TestFinalOutputName_RenamedIntoFinalOutputCounts：钩子把模型调的 submit_answer 改名成 final_output、参数又
// 过不了校验——这就是一次失败的结构化输出尝试，预算按它算、失败原因按它报。曾经按模型原始的调用名数：预算
// 不生效，模型被一遍遍调到轮次超限，拿不到 StructuredOutputError（审计实测 7 次）。
func TestFinalOutputName_RenamedIntoFinalOutputCounts(t *testing.T) {
	submit := func() *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "s1", Function: schema.FunctionCall{Name: "submit_answer", Arguments: `{"name":"A"}`},
		}})
	}
	m := &structModel{responses: []*schema.Message{submit(), submit(), submit(), submit()}}
	_, soe, err := runStructured(t, m, 0, agent_v2.WithHooks(renameToolHook{from: "submit_answer", to: agent_v2.FinalOutputToolName}))
	if soe == nil {
		t.Fatalf("预算 0、第一次就校验失败，应返回 StructuredOutputError，实际 %v（模型被调 %d 次）", err, len(m.inputs))
	}
	if soe.Attempts != 1 || len(m.inputs) != 1 || !strings.Contains(soe.LastError, "age") {
		t.Errorf("应报 1 次尝试、原因是缺 age、模型只调 1 次：Attempts=%d LastError=%q 模型被调 %d 次",
			soe.Attempts, soe.LastError, len(m.inputs))
	}
}

// TestFinalOutputName_RenamedAwayIsNotAnAttempt：钩子把模型调的 final_output 改名成业务工具 audit_log——框架那件
// 一次也没跑，不算结构化输出的尝试。模型随后用文本收尾，报「没有结构化输出」时尝试次数是 0。曾经按模型原始的
// 调用名数，报「已尝试 1 次」。
func TestFinalOutputName_RenamedAwayIsNotAnAttempt(t *testing.T) {
	var audited int
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("audit_log", "记审计日志", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			audited++
			return tool.Success("已记录"), nil
		}))
	m := &structModel{responses: []*schema.Message{finalOutputCall(`{"name":"A","age":3}`), schema.AssistantMessage("自由文本")}}
	_, soe, err := runStructured(t, m, 1, agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(renameToolHook{from: agent_v2.FinalOutputToolName, to: "audit_log"}))
	if soe == nil {
		t.Fatalf("模型没经框架的 final_output 收尾，应返回 StructuredOutputError，实际 %v", err)
	}
	if audited != 1 || soe.Attempts != 0 {
		t.Errorf("框架那件一次没跑，不该算尝试：audit_log 执行 %d 次，Attempts=%d", audited, soe.Attempts)
	}
}
