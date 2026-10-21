package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestStructuredOutput_NoStaleResultAcrossRuns 钉住一条**已修 HIGH** 的修复。
//
// 缺陷（在当时那版 `structuredFinalMessage` 里——现已换成 `structuredFinalOf`，判据不同）：
// 它倒扫**含 prior 的消息列表**找带 `ReturnDirectAnnotation`
// 的 final_output tool_result。开了会话历史层时 prior 里就有上一轮完整的 tool_result
// （历史存的是逐字完整流），其 call ID 也在 `foIDs` 里 ⟹ **本轮一个 final_output 都没跑
// 时，会捞到上一轮的结果并静默返回**，而那条路径按代码里明写的「绝不静默退化」契约
// 该抛 `StructuredOutputError`。
//
// 实测的样子（修复前）：
//
//	Run 1: 成功产出 {"age":30,"name":"Alice"}
//	Run 2: 模型自由文本终止，本轮零个 final_output
//	     → 静默返回 {"age":30,"name":"Alice"}      ← 另一个问题的答案
//
// 业务 `json.Unmarshal` 会成功、拿到一个**看起来完全正常但属于上一个问题**的结果，
// 没有任何信号。
//
// 修法不是加防御，是换取值来源：判据改看**决策点交出来的本轮产出**（`basicFinalCh`），
// 而 prior 结构上进不去那条 channel。
func TestStructuredOutput_NoStaleResultAcrossRuns(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")

	mk := func(resp []*schema.Message) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("extract"),
			agent_v2.WithModel(&structModel{responses: resp}),
			agent_v2.WithOutputSchema("person", personSchema()),
			agent_v2.WithHistory(hs, "tenant-a"),
			agent_v2.WithMaxRounds(4),
		)
	}

	// Run 1：成功产出结构化结果（它会进历史，成为 Run 2 的 prior）
	ev1, err := mk([]*schema.Message{finalOutputCall(`{"name":"Alice","age":30}`)}).
		Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("extract Alice 30"))
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := agent_v2.FinalMessage(ev1).Content; got != `{"age":30,"name":"Alice"}` {
		t.Fatalf("前提没成立——Run 1 应产出规范 JSON，实际 %q", got)
	}

	// Run 2：模型自由文本终止，**本轮零个 final_output**
	ev2, err := mk([]*schema.Message{schema.AssistantMessage("我不想用工具")}).
		Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("再来一个"))

	// 承重：必须抛 typed 错，绝不能静默返回上一轮的结果
	var soErr *agent_v2.StructuredOutputError
	if !errors.As(err, &soErr) {
		content := ""
		if f := agent_v2.FinalMessage(ev2); f != nil {
			content = f.Content
		}
		t.Fatalf("本轮零个 final_output，应抛 StructuredOutputError；实际 err=%v final=%q\n"+
			"（若 final 是上一轮的 JSON，就是那条 stale 缺陷复现了）", err, content)
	}
}

// TestStructuredOutput_FinalOutputOrderIndependent 钉住「final_output 在本批 return-direct
// 结果里排第几都要生效」。
//
// 缺陷（我引入的）：短路 ReAct 的判断和「本轮答案是不是工具产出」的判断挤在同一个循环里，
// 前者撞见第一个 return-direct 就 `break`，把后者一起带走了 ⟹ `final_output` **只有排在
// 第一个 return-direct 位置时才覆盖最终产出**。
//
// 后果：同一轮里业务自己的 return-direct 工具排在前面时，模型明明产出了合法 JSON、
// `final_output` 也校验通过了，却抛 `StructuredOutputError`「模型未产出结构化输出」。
//
// ⚠️ 这个组合是**允许**的——`validateOutputSchemaConfig` 只拦 transfer_to_agent / route_to
// 跟结构化输出同用，业务自己声明 `tool.ReturnDirectMetaKey` 的工具不在拦截之列。
func TestStructuredOutput_FinalOutputOrderIndependent(t *testing.T) {
	bizRD := tool.NewFuncTool("biz_exit", "业务自己的一调即出工具",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "biz done",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		})

	// 两种顺序都必须拿到结构化结果
	for _, tc := range []struct {
		name  string
		calls []schema.ToolCall
	}{
		{"final_output 排前", []schema.ToolCall{
			{ID: "f1", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.FinalOutputToolName, Arguments: `{"name":"Alice","age":30}`}},
			{ID: "b1", Type: "function", Function: schema.FunctionCall{Name: "biz_exit", Arguments: `{}`}},
		}},
		{"业务 return-direct 排前", []schema.ToolCall{
			{ID: "b1", Type: "function", Function: schema.FunctionCall{Name: "biz_exit", Arguments: `{}`}},
			{ID: "f1", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.FinalOutputToolName, Arguments: `{"name":"Alice","age":30}`}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := tool.NewToolkit()
			tk.Register(bizRD)
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("extract"),
				agent_v2.WithModel(&structModel{responses: []*schema.Message{
					schema.AssistantToolCallMessage(tc.calls)}}),
				agent_v2.WithToolkit(tk),
				agent_v2.WithOutputSchema("person", personSchema()),
				agent_v2.WithMaxRounds(4),
			)
			ev, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("extract"))
			if err != nil {
				t.Fatalf("模型产出了合法 JSON、final_output 也校验通过了，却报错：%v\n"+
					"（多半是 final_output 在本批里的位置影响了取值）", err)
			}
			f := agent_v2.FinalMessage(ev)
			if f == nil || f.Content != `{"age":30,"name":"Alice"}` {
				t.Fatalf("应拿到校验后的规范 JSON，实际 %v", f)
			}
		})
	}
}
