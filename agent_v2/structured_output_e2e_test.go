package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// structModel 捕获 input/tools/tool_choice + 按 sequence 返回——结构化输出端到端 mock，
// 读 input 做断言（下游确实收到 final_output 工具 / 回炉反馈），非忽略 input 的假集成。
type structModel struct {
	responses  []*schema.Message
	idx        int
	boundTools []*schema.ToolInfo
	inputs     [][]*schema.Message // 每次调用 input 快照
	forcedSeen bool                // 是否收到 tool_choice=required
}

func (m *structModel) Generate(_ context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	snap := make([]*schema.Message, len(in))
	copy(snap, in)
	m.inputs = append(m.inputs, snap)
	if o := model.Apply(opts...); o.ToolChoice != nil && *o.ToolChoice == schema.ToolChoiceRequired {
		m.forcedSeen = true
	}
	if m.idx >= len(m.responses) {
		return schema.AssistantMessage("[exhausted free text]"), nil
	}
	out := m.responses[m.idx]
	m.idx++
	return out, nil
}

func (m *structModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil // 降级 Generate
}

func (m *structModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.boundTools = tools
	return m, nil
}

func finalOutputCall(args string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:       "fo_1",
		Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: args},
	}})
}

func personSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
		"required": []string{"name", "age"},
	}
}

// TestStructuredOutput_Success 验纯抽取一次成功：final_output 在 bind list、tool_choice=
// required 被设、最终 Content 是结构化 JSON（A1）。
func TestStructuredOutput_Success(t *testing.T) {
	m := &structModel{responses: []*schema.Message{finalOutputCall(`{"name":"Alice","age":30}`)}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("extract"),
		agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema()),
		agent_v2.WithMaxRounds(5),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("extract Alice 30"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("no final message")
	}
	// A1：最终 Content 直接 unmarshal 成结构
	var got struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if jerr := json.Unmarshal([]byte(final.Content), &got); jerr != nil {
		t.Fatalf("final content 非结构化 JSON: %q (%v)", final.Content, jerr)
	}
	if got.Name != "Alice" || got.Age != 30 {
		t.Errorf("解析错: %+v", got)
	}
	if !hasToolNamed(m.boundTools, agent_v2.FinalOutputToolName) {
		t.Error("final_output 应在 bind list（mock 读 tools 断言）")
	}
	if !m.forcedSeen {
		t.Error("应设 tool_choice=required")
	}
}

// TestStructuredOutput_ReburnOnInvalid 验校验失败经 ReAct 循环回炉：第二轮 input 含
// error tool_result（mock 读 input 断言），最终产出修正后的 JSON。
func TestStructuredOutput_ReburnOnInvalid(t *testing.T) {
	m := &structModel{responses: []*schema.Message{
		finalOutputCall(`{"name":"Bob"}`),          // 缺 age → 无效
		finalOutputCall(`{"name":"Bob","age":25}`), // 有效
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("extract"),
		agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema()),
		agent_v2.WithMaxRounds(5),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if m.idx != 2 {
		t.Errorf("应调 model 2 次（初始失败 + 回炉），实际 %d", m.idx)
	}
	if len(m.inputs) < 2 || !inputContains(m.inputs[1], agent_v2.StructuredOutputInvalidCode) {
		t.Error("第二轮 input 应含 structured_output_invalid 回炉反馈（校验错误回喂 LLM）")
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, `"age":25`) {
		t.Errorf("最终应是修正后的 JSON, got %q", final.Content)
	}
}

// TestStructuredOutput_RetryExhausted 验重试耗尽抛 typed StructuredOutputError（绝不静默）。
func TestStructuredOutput_RetryExhausted(t *testing.T) {
	m := &structModel{responses: []*schema.Message{
		finalOutputCall(`{"name":"A"}`), // 无效（缺 age）
		finalOutputCall(`{"name":"B"}`), // 无效
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("extract"),
		agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema(), agent_v2.WithMaxValidationRetries(1)),
		agent_v2.WithMaxRounds(10),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil {
		t.Fatal("应抛 StructuredOutputError")
	}
	var soe *agent_v2.StructuredOutputError
	if !errors.As(err, &soe) {
		t.Fatalf("应是 StructuredOutputError, got %T: %v", err, err)
	}
	if soe.Attempts != 2 {
		t.Errorf("Attempts = %d; want 2（maxRetries=1 → 初始 + 1 retry 都失败）", soe.Attempts)
	}
	if soe.SchemaName != "person" {
		t.Errorf("SchemaName = %q; want person", soe.SchemaName)
	}
}

func hasToolNamed(tools []*schema.ToolInfo, name string) bool {
	for _, t := range tools {
		if t != nil && t.Name == name {
			return true
		}
	}
	return false
}

func inputContains(msgs []*schema.Message, sub string) bool {
	for _, m := range msgs {
		if m != nil && strings.Contains(m.Content, sub) {
			return true
		}
	}
	return false
}

// TestStructuredOutput_ConstructGuards 验 construct 期 fail-fast（audit：A3 组合 + WithToolkit
// 覆盖顺序陷阱）——silent 数据错误改成构造期 panic。
func TestStructuredOutput_ConstructGuards(t *testing.T) {
	noop := func(_ context.Context, _ map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }

	t.Run("与 transfer_to_agent 组合 panic", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("WithOutputSchema + transfer_to_agent 应 construct 期 panic")
			}
		}()
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool(agent_v2.HandoffTransferToolName, "transfer", map[string]any{"type": "object"}, noop))
		agent_v2.NewBasicAgent(
			agent_v2.WithModel(&structModel{}),
			agent_v2.WithToolkit(tk),
			agent_v2.WithOutputSchema("p", personSchema()),
		)
	})

	// final_output 由框架每次 Run 注入、不写进业务的 toolkit，跟 WithToolkit 的先后无关（行为断言见
	// framework_tool_injection_test.go）。曾经顺序放反就 panic。
	t.Run("WithToolkit 放在后面也不 panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("先后无关，不该 panic: %v", r)
			}
		}()
		agent_v2.NewBasicAgent(
			agent_v2.WithModel(&structModel{}),
			agent_v2.WithOutputSchema("p", personSchema()),
			agent_v2.WithToolkit(tool.NewToolkit()),
		)
	})

	t.Run("WithToolkit 放在前面也不 panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("先后无关，不该 panic: %v", r)
			}
		}()
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("biz", "biz tool", map[string]any{"type": "object"}, noop))
		agent_v2.NewBasicAgent(
			agent_v2.WithModel(&structModel{}),
			agent_v2.WithToolkit(tk),
			agent_v2.WithOutputSchema("p", personSchema()),
		)
	})
}

// TestStructuredOutput_BudgetSurvivesCompaction 回归 A4 HIGH：配 AutoCompact（阈值极低每轮
// 触发、会摘要删掉失败尝试）时，预算仍从**压缩前完整历史** count → 抛 StructuredOutputError，
// 而非 count 低估 → 空转到 MaxIterationsError。sanity-revert（预算移回压缩后）应 fail。
func TestStructuredOutput_BudgetSurvivesCompaction(t *testing.T) {
	m := &structModel{responses: []*schema.Message{
		finalOutputCall(`{"name":"A"}`), // 无效（缺 age）→ 尝试 1
		finalOutputCall(`{"name":"B"}`), // 无效 → 尝试 2
		finalOutputCall(`{"name":"C"}`), // sanity-revert 路径（预算压缩后失效）才会调到
	}}
	// summarizer 必须返 <summary>…</summary> 合规块，否则 formatCompactSummary 解析失败 →
	// AutoCompact 放弃压缩、不删任何消息（纯文本 mock 会让本测试假绿——不删就没验证到交互）。
	summarizer := &structModel{responses: []*schema.Message{
		schema.AssistantMessage("<summary>s1</summary>"), schema.AssistantMessage("<summary>s2</summary>"),
		schema.AssistantMessage("<summary>s3</summary>"), schema.AssistantMessage("<summary>s4</summary>"),
	}}
	// Threshold = ModelContextWindow - SummaryMaxTokens - SummaryBufferTokens = 4-1-1 = 2（小正数）
	// → 每轮对话 token > 2 必触发压缩；第 3 轮起有 >1 组（失败尝试因不同 run_id 分组）→ 摘要
	// 删掉早期失败尝试。若预算在压缩后 count，会低估 → 不抛。
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("extract"),
		agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema(), agent_v2.WithMaxValidationRetries(1)),
		agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{
			Enabled: true, Model: summarizer, ModelContextWindow: 4, SummaryMaxTokens: 1, SummaryBufferTokens: 1,
		}),
		agent_v2.WithMaxRounds(20),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	// fix 生效时：第 3 轮入口预算（压缩**前**）count=2 → 直接抛，AutoCompact 没机会介入
	// （故此路径 summarizer 未必被调，不断言）。sanity-revert（预算移回压缩后）时：压缩会删掉
	// 早期失败尝试 → count 低估 → 不抛 → 自由文本终止 err=nil → 下面 errors.As 失败 → 测试
	// fail，据此验证 fix 真有效（防假绿）。
	var soe *agent_v2.StructuredOutputError
	if !errors.As(err, &soe) {
		t.Fatalf("配 AutoCompact 时预算仍应抛 StructuredOutputError, got %T: %v", err, err)
	}
	if soe.Attempts != 2 {
		t.Errorf("Attempts = %d; want 2（压缩前完整历史 count）", soe.Attempts)
	}
}

// TestStructuredOutput_WithBusinessTool 验场景 A：模型先调业务工具、再 final_output 收尾——
// final_output 与业务 toolkit 共存 + 结构化结果可引用工具数据 + tool_choice=required 多轮。
func TestStructuredOutput_WithBusinessTool(t *testing.T) {
	bizTool := tool.NewFuncTool("get_data", "获取数据",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success(`{"value":42}`), nil
		})
	tk := tool.NewToolkit()
	tk.Register(bizTool)

	getData := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "g1", Function: schema.FunctionCall{Name: "get_data", Arguments: "{}"},
	}})
	m := &structModel{responses: []*schema.Message{
		getData,                          // round1：先调业务工具
		finalOutputCall(`{"result":42}`), // round2：final_output 收尾（引用工具数据）
	}}
	sc := map[string]any{
		"type":       "object",
		"properties": map[string]any{"result": map[string]any{"type": "integer"}},
		"required":   []string{"result"},
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("scenarioA"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithOutputSchema("out", sc),
		agent_v2.WithMaxRounds(5),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查数据后结构化"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if m.idx != 2 {
		t.Errorf("应调 model 2 次（get_data + final_output），实际 %d", m.idx)
	}
	// round2 input 应含 get_data 的结果（mock 读 input 断言：业务工具结果回喂 LLM）
	if len(m.inputs) < 2 || !inputContains(m.inputs[1], "42") {
		t.Error("round2 input 应含 get_data 的 tool_result（value:42）")
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("无最终消息")
	}
	var out struct {
		Result int `json:"result"`
	}
	if err := json.Unmarshal([]byte(final.Content), &out); err != nil {
		t.Fatalf("最终非结构化 JSON: %q (%v)", final.Content, err)
	}
	if out.Result != 42 {
		t.Errorf("result=%d want 42", out.Result)
	}
}

// TestStructuredOutput_HITLResume 验结构化 agent 中途 ask_user 中断 → Resume → final_output
// 收尾——结构化与 HITL Resume 兼容（ask_user 框架工具不与 final_output 冲突；跨 cp round-trip
// 后仍产结构化 JSON）。
func TestStructuredOutput_HITLResume(t *testing.T) {
	askCall := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "a1", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"确认？"}`},
	}})
	m := &structModel{responses: []*schema.Message{
		askCall,                                  // round1：ask_user → 中断
		finalOutputCall(`{"answer":"resolved"}`), // Resume 后：final_output 收尾
	}}
	sc := map[string]any{
		"type":       "object",
		"properties": map[string]any{"answer": map[string]any{"type": "string"}},
		"required":   []string{"answer"},
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("hitlStruct"),
		agent_v2.WithModel(m),
		agent_v2.WithAskUser(), // ask_user 框架工具（不与 final_output 冲突）
		agent_v2.WithOutputSchema("out", sc),
		agent_v2.WithMaxRounds(5),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("处理这个"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 应中断（ask_user），got: %v", err)
	}
	events, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}),
		agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("Resume err: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("Resume 无最终消息")
	}
	var out struct {
		Answer string `json:"answer"`
	}
	if err := json.Unmarshal([]byte(final.Content), &out); err != nil {
		t.Fatalf("Resume 后最终非结构化 JSON: %q (%v)", final.Content, err)
	}
	if out.Answer != "resolved" {
		t.Errorf("answer=%q want resolved", out.Answer)
	}
}

// TestStructuredOutput_UnforcedFreeTextError 验模型自由文本终止不调 final_output（unforced /
// soft-compliance 不遵守 tool_choice=required）→ 抛 StructuredOutputError 而非 silent 回退自由
// 文本（cross-verify CLAIM 5：守"绝不静默退化"契约）。
func TestStructuredOutput_UnforcedFreeTextError(t *testing.T) {
	m := &structModel{responses: []*schema.Message{
		schema.AssistantMessage("我直接回答：这是自由文本，没调 final_output"),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("extract"),
		agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema()),
		agent_v2.WithMaxRounds(5),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	var soe *agent_v2.StructuredOutputError
	if !errors.As(err, &soe) {
		t.Fatalf("模型自由文本终止应抛 StructuredOutputError（不 silent 退化），got %T: %v", err, err)
	}
	if soe.SchemaName != "person" {
		t.Errorf("SchemaName = %q; want person", soe.SchemaName)
	}
}

// TestStructuredOutput_RetryBeatsBusinessShortCircuit 钉住**校验失败的 final_output 在场时，
// 业务 return-direct 的短路给回炉让路**。
//
// ⚠️ audit 抓到的先存缺陷（本条修掉）：短路信号曾在扫描循环里**第一个命中就写**——那时
// 还不知道同批有没有校验失败的 final_output。同批 [业务RD, final_output(缺 required 字段)]
// 时短路抢跑 ⟹ tools→model 的回炉边死 ⟹ 模型第 2 轮永不被调、postamble 抛
// StructuredOutputError，**文案还把它诊断成「自由文本终止」**（模型明明调了 final_output，
// 产物被短路丢了）——预算白烧一次、诊断把作者引向错误方向。
//
// 语义：业务 return-direct 说「这轮成功收尾」，而配了 WithOutputSchema 的 agent「收尾」
// 的定义被 schema 收紧——没有合法 JSON 就不算收尾完。回炉赢。
func TestStructuredOutput_RetryBeatsBusinessShortCircuit(t *testing.T) {
	var bizRuns int
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("do_exit", "业务 exit", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			bizRuns++
			return &tool.Result{Success: true, Output: "业务产出",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	round1 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_biz", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}},
		{ID: "tc_fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName,
			Arguments: `{"name":"张三"}`}}, // 缺 required 的 age ⟹ 校验失败
	})
	round2 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_fo2", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName,
			Arguments: `{"name":"张三","age":30}`}},
	})
	m := newStubModel([]*schema.Message{round1, round2, schema.AssistantMessage("txt")})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5),
		agent_v2.WithOutputSchema("person", personSchema()),
		agent_v2.WithModel(m),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	evs, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("回炉该活着（第 2 轮给出合法 JSON），实际 err=%v", err)
	}
	final := agent_v2.FinalMessage(evs)
	if final == nil || !strings.Contains(final.Content, `"age":30`) {
		t.Fatalf("最终答复该是第 2 轮的合法 JSON，实际 %+v", final)
	}
	if bizRuns != 1 {
		t.Errorf("业务工具执行 %d 次，期望 1（回炉不重放已执行的工具）", bizRuns)
	}
}

// TestStructuredOutput_BizShortCircuitAloneStillWorks 对照组：**没有** final_output 参与时，
// 业务 return-direct 的短路照常工作——上一条的让路逻辑不能把普通短路也让没了。
// ⚠️ 注意这个 agent 没配 WithOutputSchema：让路只发生在「校验失败的 final_output 在场」。
func TestStructuredOutput_BizShortCircuitAloneStillWorks(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("do_exit", "业务 exit", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "业务产出",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "t1", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}}}),
		schema.AssistantMessage("SHOULD_NOT_REACH"),
	})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(m),
	)
	evs, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(evs)
	if final == nil || final.Content != "业务产出" {
		t.Fatalf("业务 return-direct 单独出现该正常短路、产出即答复，实际 %+v", final)
	}
}

// vetoFinalOutput 在 OnToolEnd 里把 final_output 的成功结果否决掉（业务追加的校验）：mode 0 就地改，
// mode 1 整体替换 Result 忘了填 Success（零值 false）。否决次数到 limit 为止（<0 = 一直否决）。
type vetoFinalOutput struct {
	hook.NoOpHooks
	mode, limit, n int
}

func (h *vetoFinalOutput) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out == nil || out.Result == nil || !out.Result.Success || (h.limit >= 0 && h.n >= h.limit) {
		return ctx, nil
	}
	h.n++
	if h.mode == 0 {
		out.Result.Success = false
		out.Result.ErrorCode = "biz_reject"
		out.Result.Error = "年龄不可信，请重给"
	} else {
		out.Result = &schema.ToolCallResult{Output: "年龄不可信，请重给"}
	}
	return ctx, nil
}

// TestStructuredOutput_HookVetoedFinalOutputRetries：final_output 校验通过、却被 OnToolEnd 否决——等同
// 校验失败：回炉、模型看到否决理由重给，最终答复是合法 JSON。就地改与整体替换两种写法一样。
//
// 曾经被否决的那次照样被挑成答复短路收尾：最终答复是失败文本、不是 JSON，`err == nil`（审计实测）。
func TestStructuredOutput_HookVetoedFinalOutputRetries(t *testing.T) {
	for _, mode := range []int{0, 1} {
		name := map[int]string{0: "就地改成失败", 1: "整体替换忘填 Success"}[mode]
		t.Run(name, func(t *testing.T) {
			m := &structModel{responses: []*schema.Message{
				finalOutputCall(`{"name":"A","age":300}`),
				finalOutputCall(`{"name":"A","age":30}`),
			}}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("extract"), agent_v2.WithModel(m),
				agent_v2.WithOutputSchema("person", personSchema()),
				agent_v2.WithHooks(&vetoFinalOutput{mode: mode, limit: 1}), agent_v2.WithMaxRounds(10))
			events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			final := agent_v2.FinalMessage(events)
			var v map[string]any
			if final == nil || json.Unmarshal([]byte(final.Content), &v) != nil || v["age"] != float64(30) {
				t.Errorf("被否决之后应回炉、以第二次的合法 JSON 收尾，实际 %+v", final)
			}
			if m.idx != 2 || !inputContains(m.inputs[len(m.inputs)-1], "年龄不可信") {
				t.Errorf("模型应被调 2 次、第 2 次看到否决理由：调了 %d 次", m.idx)
			}
		})
	}
}

// TestStructuredOutput_HookVetoedFinalOutputCountsAgainstBudget：被钩子否决的 final_output 照样算一次尝试，
// 一直否决时预算耗尽抛 StructuredOutputError——不会无限回炉，也不会静默以失败文本收尾。
func TestStructuredOutput_HookVetoedFinalOutputCountsAgainstBudget(t *testing.T) {
	m := &structModel{responses: []*schema.Message{
		finalOutputCall(`{"name":"A","age":30}`),
		finalOutputCall(`{"name":"B","age":31}`),
		finalOutputCall(`{"name":"C","age":32}`),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("extract"), agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema(), agent_v2.WithMaxValidationRetries(1)),
		agent_v2.WithHooks(&vetoFinalOutput{limit: -1}), agent_v2.WithMaxRounds(10))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	var soe *agent_v2.StructuredOutputError
	if !errors.As(err, &soe) {
		t.Fatalf("一直被否决应耗尽预算、抛 StructuredOutputError，实际 %v", err)
	}
	if m.idx != 2 {
		t.Errorf("maxRetries=1 ⟹ 模型应被调 2 次（初始 + 1 次回炉），实际 %d", m.idx)
	}
}

// vetoCall 只否决指定 call 的成功结果（就地改成失败）。
type vetoCall struct {
	hook.NoOpHooks
	callID string
}

func (h *vetoCall) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil && out.Result.Success && out.CallId == h.callID {
		out.Result.Success = false
		out.Result.ErrorCode = "biz_reject"
		out.Result.Error = "年龄不可信，请重给"
	}
	return ctx, nil
}

// TestStructuredOutput_HookVetoBeatsBusinessShortCircuit：同一批 [业务 exit 工具（return-direct）,
// 合法的 final_output]，钩子否决了 final_output——跟校验失败一样，回炉压过业务工具的短路：第 2 轮给出
// 合法 JSON 收尾，业务工具不重放。
//
// 否决分支只「跳过」不「置回炉」的话，同批的业务工具会抢到收尾：最终答复是业务产出、结构化输出的承诺
// 落空（批里只有 final_output 时两种写法分不出来，这条是那半的哨兵）。
func TestStructuredOutput_HookVetoBeatsBusinessShortCircuit(t *testing.T) {
	var bizRuns int
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("do_exit", "业务 exit", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			bizRuns++
			return &tool.Result{Success: true, Output: "业务产出",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	round1 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_biz", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}},
		{ID: "tc_fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName,
			Arguments: `{"name":"张三","age":30}`}}, // 校验通过，被钩子否决
	})
	round2 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_fo2", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName,
			Arguments: `{"name":"张三","age":31}`}},
	})
	m := newStubModel([]*schema.Message{round1, round2, schema.AssistantMessage("txt")})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5),
		agent_v2.WithOutputSchema("person", personSchema()),
		agent_v2.WithModel(m), agent_v2.WithHooks(&vetoCall{callID: "tc_fo"}),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	evs, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("回炉该活着（第 2 轮给出合法 JSON），实际 err=%v", err)
	}
	final := agent_v2.FinalMessage(evs)
	if final == nil || !strings.Contains(final.Content, `"age":31`) {
		t.Fatalf("最终答复该是第 2 轮的合法 JSON，实际 %+v", final)
	}
	if bizRuns != 1 {
		t.Errorf("业务工具执行 %d 次，期望 1（回炉不重放已执行的工具）", bizRuns)
	}
}

// TestStructuredOutput_BudgetExhaustedByVeto_LastErrorIsLastFailure：重试预算被 OnToolEnd 的否决耗尽时，
// StructuredOutputError.LastError 是最后一次失败的原因。曾经只认 schema 失败的错误码：全被钩子否决时
// LastError 为空（报错里只剩「已尝试 2 次：」）；先 schema 失败、后被否决时指向更早那次 schema 失败。
func TestStructuredOutput_BudgetExhaustedByVeto_LastErrorIsLastFailure(t *testing.T) {
	run := func(t *testing.T, responses []*schema.Message) *agent_v2.StructuredOutputError {
		t.Helper()
		ag := agent_v2.NewBasicAgent(agent_v2.WithName("extract"), agent_v2.WithModel(&structModel{responses: responses}),
			agent_v2.WithOutputSchema("person", personSchema(), agent_v2.WithMaxValidationRetries(1)),
			agent_v2.WithHooks(&vetoFinalOutput{limit: -1}), agent_v2.WithMaxRounds(10))
		_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
		var soe *agent_v2.StructuredOutputError
		if !errors.As(err, &soe) {
			t.Fatalf("应耗尽预算报 StructuredOutputError，实际 %v", err)
		}
		return soe
	}
	t.Run("全部被钩子否决", func(t *testing.T) {
		soe := run(t, []*schema.Message{finalOutputCall(`{"name":"A","age":30}`), finalOutputCall(`{"name":"B","age":31}`)})
		if !strings.Contains(soe.LastError, "年龄不可信") || !strings.Contains(soe.Error(), "年龄不可信") {
			t.Errorf("LastError 应是钩子给的否决理由，实际 LastError=%q Error()=%q", soe.LastError, soe.Error())
		}
	})
	t.Run("先schema失败后被钩子否决", func(t *testing.T) {
		soe := run(t, []*schema.Message{
			finalOutputCall(`{"name":"A"}`),          // 缺 age：schema 校验失败
			finalOutputCall(`{"name":"B","age":31}`), // 校验通过、被钩子否决——这才是最后一次失败
		})
		if !strings.Contains(soe.LastError, "年龄不可信") {
			t.Errorf("LastError 应是最后那次钩子否决，实际 %q", soe.LastError)
		}
	})
}
