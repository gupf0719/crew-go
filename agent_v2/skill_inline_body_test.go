package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// newCallStubToolkit 注入一个固定输出的 tool 用于业务工具路径测试。
func newCallStubToolkit(name, output string) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(
		name,
		"stub tool for test",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success(output), nil
		},
	))
	return tk
}

// TestInlineSkillBody_DefaultDisabled 验证 InlineSkillBody 默认 false 走老路径——
// activate_skill tool_result content 直接是 skill body（没有额外 UserMessage 注入）。
//
// **bomb 防御**：用 `[system_reminder]` 字符串断言新路径是否被走——sanity-revert
// 把默认改成 true → "[system_reminder]" 出现在 LLM msgs → fail。
func TestInlineSkillBody_DefaultDisabled(t *testing.T) {
	box := &fakeSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "翻译"},
		},
		activations: map[string]string{"translate": "你是翻译专家，请把文本翻译成英文。"},
	}
	// stub 序列：round 1 返 activate_skill tool_call，round 2 返 final
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"translate"}`,
		},
	}})
	r2 := schema.AssistantMessage("done")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		// 不设 WithInlineSkillBody → 默认 false
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(stub.allCalls) < 2 {
		t.Fatalf("应至少 2 次 LLM 调用，实际 %d", len(stub.allCalls))
	}
	// 第 2 次 LLM 调用看到 tool_result 含 body 全文
	round2Msgs := concatContent(stub.allCalls[1])
	if !strings.Contains(round2Msgs, "你是翻译专家") {
		t.Errorf("默认模式 tool_result content 应直接含 skill body 全文，实际：%q", round2Msgs)
	}
	// 默认模式不应注入 [system_reminder] UserMessage
	if strings.Contains(round2Msgs, "[system_reminder]") {
		t.Errorf("默认模式不应注入 inline body UserMessage（[system_reminder] 不应出现），实际：%q", round2Msgs)
	}
}

// TestInlineSkillBody_EnabledInjectsSystemReminder 验证启用 InlineSkillBody 后：
//  1. tool_result content 退化为 terse 激活 ack（不含 body）
//  2. 额外 emit `[system_reminder] Skill 'X' content:` UserMessage 含 body
//  3. body UserMessage 标 SkillBodyAnnotation marker（用 message.Annotations 检测）
//
// **bomb 防御（5 类炸弹·第 5 类——mock 不读 input）**：
// captureMsgsStub.allCalls 记录每次完整 msgs；断言 body 内容**真在 UserMessage**
// 而不是 tool_result 内（用 ToolResultMessage 不带"[system_reminder]"前缀）。
// sanity-revert 删 inline 注入路径 → body 仍在 tool_result → "[system_reminder]"
// 不出现 → fail。
func TestInlineSkillBody_EnabledInjectsSystemReminder(t *testing.T) {
	box := &fakeSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "翻译"},
		},
		activations: map[string]string{"translate": "你是翻译专家，请把文本翻译成英文。"},
	}
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"translate"}`,
		},
	}})
	r2 := schema.AssistantMessage("done")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithInlineSkillBody(true), // 启用 P2.D
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(stub.allCalls) < 2 {
		t.Fatalf("应至少 2 次 LLM 调用，实际 %d", len(stub.allCalls))
	}
	round2Msgs := stub.allCalls[1]

	// 找 tool_result 和 UserMessage(body) 的具体位置
	var toolResult, bodyUserMsg *schema.Message
	for _, m := range round2Msgs {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool {
			// ToolCallID 存在 ContentBlocks 的 Toolresult.ToolCallId
			tcID := ""
			for _, b := range m.ContentBlocks {
				if b != nil && b.Toolresult != nil {
					tcID = b.Toolresult.ToolCallId
					break
				}
			}
			if tcID == "tc1" {
				toolResult = m
			}
		}
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "[system_reminder]") {
			bodyUserMsg = m
		}
	}

	if toolResult == nil {
		t.Fatalf("应有 tool_result message，实际 msgs：%q", concatContent(round2Msgs))
	}
	if bodyUserMsg == nil {
		t.Fatalf("应有 [system_reminder] UserMessage 含 body，实际 msgs：%q", concatContent(round2Msgs))
	}
	// tool_result 应是 terse ack（不含 body 全文）
	if strings.Contains(toolResult.Content, "你是翻译专家") {
		t.Errorf("inline mode tool_result 不应含 body 全文（应 terse ack），实际：%q", toolResult.Content)
	}
	if !strings.Contains(toolResult.Content, "activated") {
		t.Errorf("inline mode tool_result 应含 'activated' ack，实际：%q", toolResult.Content)
	}
	// body UserMessage 应含 body 全文
	if !strings.Contains(bodyUserMsg.Content, "你是翻译专家") {
		t.Errorf("inline mode UserMessage 应含 body 全文，实际：%q", bodyUserMsg.Content)
	}
	// body UserMessage 应标 SkillBodyAnnotation marker
	if bodyUserMsg.Annotations == nil {
		t.Errorf("inline mode body UserMessage 应有 Annotations 含 SkillBodyAnnotation，实际 nil")
	} else if v, ok := bodyUserMsg.Annotations[agent_v2.SkillBodyAnnotation].(bool); !ok || !v {
		t.Errorf("inline mode body UserMessage 应标 SkillBodyAnnotation marker，实际 Annotations=%v", bodyUserMsg.Annotations)
	}
	// **R19 audit HIGH-1 fix 后断言变化**：原 P2.D 设计 marker 只在 bodyMsg 上不在
	// terse ack 上——但 R19 发现这破坏 4 层压缩协议（splitProtectedGroups 让 asst
	// tool_call group 进 summarize → 配对上下文丢失）。修后 ack toolMsg **也带**
	// SkillBodyAnnotation marker 让两 group 都被识别为 skill group 整组保留。
	if toolResult.Annotations == nil {
		t.Errorf("R19 HIGH-1 fix 后 inline mode tool_result 也应有 SkillBodyAnnotation marker，实际 Annotations=nil")
	} else if v, ok := toolResult.Annotations[agent_v2.SkillBodyAnnotation].(bool); !ok || !v {
		t.Errorf("R19 HIGH-1 fix 后 inline mode tool_result 应标 SkillBodyAnnotation marker，实际 Annotations=%v", toolResult.Annotations)
	}
}

// TestInlineSkillBody_OnlyForFrameworkActivateSkill 验证 inline 协议**只对** framework
// 注入的 activate_skill 生效——业务自定义同名 tool 或其他 tool 不触发。
//
// **bomb 防御（5 类炸弹·第 4 类——双层防护遮蔽）**：
// 用业务工具 `my_tool` 触发，验证不走 inline 路径——sanity-revert 把 isFrameworkActivateSkill
// 检查删了 → 业务工具结果也被替换为 terse ack + 注入 [system_reminder] body → fail。
func TestInlineSkillBody_OnlyForFrameworkActivateSkill(t *testing.T) {
	// 业务自定义 toolkit + 不设 WithSkills → skillNeedsTool=false
	tk := newCallStubToolkit("my_tool", "biz output content")

	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "my_tool",
			Arguments: `{}`,
		},
	}})
	r2 := schema.AssistantMessage("done")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithInlineSkillBody(true), // 即使开启也不应影响业务工具
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(stub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(stub.allCalls))
	}
	round2Msgs := concatContent(stub.allCalls[1])
	// 业务工具结果应直接在 tool_result 内（不替换为 terse ack）
	if !strings.Contains(round2Msgs, "biz output content") {
		t.Errorf("业务工具的 tool_result 应直接含 output（不走 inline 协议），实际：%q", round2Msgs)
	}
	// 不应注入 [system_reminder] UserMessage
	if strings.Contains(round2Msgs, "[system_reminder]") {
		t.Errorf("业务工具不应触发 inline body 协议，实际：%q", round2Msgs)
	}
}

// TestInlineSkillBody_R16_HIGH1_ParallelToolCallSequence 是 R16 audit HIGH-1 回归测试——
// 验证 P2.D + LLM parallel tool_call 时所有 [system_reminder] UserMessage 全部
// **后置在 tool_result 之后**，不破坏 OpenAI/Claude API 的 tool sequence 约束。
//
// **Bug 历史**：原实现 makeToolsFn 循环内 [toolMsg, bodyMsg, toolMsg, bodyMsg] 顺序
// append——LLM parallel 调 2 个 activate_skill 时 user role 中断 tool_result 序列，
// API 422（"tool messages must immediately follow assistant tool_calls"）。
// 修法：循环内只 append toolMsg，bodyMsg 收集到 deferredBodyMsgs；循环结束统一 append。
//
// **bomb 防御**：检查 LLM 看到的 round 2 messages 序列——assistant 之后所有 tool
// role messages 必须连续，不被 user role 中断。sanity-revert 改回 inline append →
// user role 在 tool 之间出现 → fail。
func TestInlineSkillBody_R16_HIGH1_ParallelToolCallSequence(t *testing.T) {
	box := &fakeSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "skill_a", Description: "a"},
			{Name: "skill_b", Description: "b"},
		},
		activations: map[string]string{
			"skill_a": "body A",
			"skill_b": "body B",
		},
	}
	// LLM r1: parallel call 2 个 activate_skill
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"skill_a"}`}},
		{ID: "tc2", Type: "function", Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"skill_b"}`}},
	})
	r2 := schema.AssistantMessage("done")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithInlineSkillBody(true),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(stub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(stub.allCalls))
	}
	round2 := stub.allCalls[1]

	// 关键断言：找到 assistant tool_calls message 后，所有 tool role messages 必须连续，
	// 不能出现 [tool, user[system_reminder], tool] 这种序列——OpenAI/Claude API 约束
	// "tool messages must immediately follow assistant with tool_calls"，user 中间会 422。
	foundAsstToolCall := false
	sawToolAfterAsst := false     // 经历过至少一个 tool message
	sawReminderAfterTool := false // tool 之后出现 user [system_reminder]
	for _, m := range round2 {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleAssistant && len(schema.GetToolCalls(m)) > 0 {
			foundAsstToolCall = true
			sawToolAfterAsst = false
			sawReminderAfterTool = false
			continue
		}
		if !foundAsstToolCall {
			continue
		}
		if m.Role == schema.RoleTool {
			if sawReminderAfterTool {
				// 触发 BUG 序列：tool, user[reminder], tool
				t.Errorf("R16-HIGH-1 回归：parallel tool_call 序列含 [tool, user[system_reminder], tool] 中断模式（API 422）\n"+
					"完整 sequence：%s", dumpRoles(round2))
				return
			}
			sawToolAfterAsst = true
			continue
		}
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "[system_reminder]") && sawToolAfterAsst {
			sawReminderAfterTool = true
		}
	}
}

// TestSkillTool_R24_HIGH1_ActivateSkillPropagatesInterruptError 是 R24 audit HIGH-1
// 回归——验证 toolCall 模式 box.ActivateSkill 抛 InterruptError 时 propagate
// 而非 wrap 成 tool.Failure (str-ize 后 IsInterrupt 解不出 → HITL 链断)。
//
// **bomb 防御**：业务自定义 SkillBox.ActivateSkill 抛 InterruptError → 父 Run
// 应返 InterruptError 类型；sanity-revert 删 propagate 路径 → tool.Failure 字符串化
// → IsInterrupt 解不出 → fail。
func TestSkillTool_R24_HIGH1_ActivateSkillPropagatesInterruptError(t *testing.T) {
	box := &interruptingActivateBox{
		skills: []skill.SkillDescriptor{{Name: "secret", Description: "PII skill"}},
	}
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"secret"}`},
	}})
	r2 := schema.AssistantMessage("ok")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("use skill"))
	if err == nil {
		t.Fatalf("R24 HIGH-1 回归：toolCall 模式 box.ActivateSkill 抛 InterruptError 时" +
			"BasicAgent.Run 应 propagate 不 wrap 字符串；实际 nil → silent 跑下去 HITL 链断")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("R24 HIGH-1 回归：err 应为 InterruptError 类型——实际 %v (%T)\n"+
			"sanity-revert 删 IsInterrupt propagate 路径 → 字符串化进 tool.Failure → IsInterrupt 解不出",
			err, err)
	}
}

// interruptingActivateBox 是 R24 测试用 SkillBox——ActivateSkill 抛 InterruptError。
type interruptingActivateBox struct {
	skills []skill.SkillDescriptor
}

func (b *interruptingActivateBox) Name() string                        { return "intbox" }
func (b *interruptingActivateBox) RootDir() string                     { return "" }
func (b *interruptingActivateBox) ListSkills() []skill.SkillDescriptor { return b.skills }
func (b *interruptingActivateBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult {
	return nil
}
func (b *interruptingActivateBox) ListSkillResources(_ string) []skill.SkillResource {
	return nil
}
func (b *interruptingActivateBox) ReadSkillResource(_, _ string) string { return "" }
func (b *interruptingActivateBox) ReadSkillPrompt(_ string) string      { return "" }
func (b *interruptingActivateBox) ListSkillActions(_ string) []skill.SkillActionDescriptor {
	return nil
}
func (b *interruptingActivateBox) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "approve_pii_skill",
		Query:  &compose_v2.AskQuery{InterruptID: "ai1", Question: "approve " + name + "?"},
	}
}
func (b *interruptingActivateBox) RunSkillAction(_ context.Context, _, _ string, _ map[string]any) (*skill.SkillExecutionResult, error) {
	return nil, skill.ErrSkillActionNotFound
}

var _ skill.SkillBox = (*interruptingActivateBox)(nil)

// TestInlineSkillBody_R22_HIGH1_BodyTruncatedBySkillBodyMaxChars 是 R22 audit HIGH-1
// 回归——验证 P2.D inline body 路径走 SkillBodyMaxChars 截断（与 adaptive/fork 对称）。
//
// **bomb 防御**：用极大 body (5000 chars) + 业务设小 SkillBodyMaxChars=500，
// 断言 LLM 看到的 body UserMessage 被截断（含 "…" + 长度 ≤ 500 + 含"system_reminder"前缀）。
// sanity-revert 删 P2.D 截断分支 → body 长度 == 5000+ → fail。
func TestInlineSkillBody_R22_HIGH1_BodyTruncatedBySkillBodyMaxChars(t *testing.T) {
	hugeBody := strings.Repeat("a", 5000)
	box := &fakeSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "huge", Description: "huge body skill"},
		},
		activations: map[string]string{"huge": hugeBody},
	}
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"huge"}`},
	}})
	r2 := schema.AssistantMessage("done")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithInlineSkillBody(true),
		agent_v2.WithSkillBodyMaxChars(500),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(stub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(stub.allCalls))
	}
	round2 := stub.allCalls[1]

	// 找 [system_reminder] body UserMessage
	var bodyMsg *schema.Message
	for _, m := range round2 {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "[system_reminder]") {
			bodyMsg = m
			break
		}
	}
	if bodyMsg == nil {
		t.Fatalf("应见 [system_reminder] body UserMessage")
	}
	// 关键断言：body 内容（剥前缀后）长度应 ≤ 500
	// 完整 content 是 "[system_reminder] Skill \"huge\" content:\n<body>"
	// 加 prefix 约 50 chars + 截断后 body 500 chars = 总长 ≤ ~600
	if len(bodyMsg.Content) > 700 {
		t.Errorf("R22 HIGH-1 回归：P2.D inline body 应被 SkillBodyMaxChars=500 截断，"+
			"实际 body UserMessage 长度 %d > 700——可能截断逻辑漏写", len(bodyMsg.Content))
	}
	if !strings.Contains(bodyMsg.Content, "…") {
		t.Errorf("R22 HIGH-1 回归：截断应带 ellipsis '…'，实际 body 末尾：%q",
			bodyMsg.Content[max(0, len(bodyMsg.Content)-30):])
	}
}

// TestInlineSkillBody_R19_HIGH1_AckMsgHasMarker 是 R19 audit HIGH-1 回归测试——
// 验证 P2.D terse ack toolMsg **必须**带 SkillBodyAnnotation marker，否则 4 层
// 压缩 splitProtectedGroups 会让 asst tool_call group 进 summarize input → 配对
// 上下文丢失 + bodyMsg 顺序倒置在 summary 之前。
//
// **bomb 防御**：直接断言 toolMsg.Annotations[SkillBodyAnnotation] == true。
// sanity-revert 删 line 851 (toolMsg.Annotations 设置) → tool_result 无 marker
// → splitProtectedGroups 切到 summarize → 测试 fail。
func TestInlineSkillBody_R19_HIGH1_AckMsgHasMarker(t *testing.T) {
	box := &fakeSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "翻译"},
		},
		activations: map[string]string{"translate": "你是翻译"},
	}
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"translate"}`},
	}})
	r2 := schema.AssistantMessage("done")
	stub := &captureMsgsStub{responses: []*schema.Message{r1, r2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithInlineSkillBody(true),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(stub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(stub.allCalls))
	}
	round2 := stub.allCalls[1]

	// 找 terse ack toolMsg
	var ackMsg *schema.Message
	for _, m := range round2 {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool && schema.GetToolResultID(m) == "tc1" {
			ackMsg = m
			break
		}
	}
	if ackMsg == nil {
		t.Fatalf("应找到 terse ack tool_result")
	}
	// R19 关键断言：terse ack 必须带 SkillBodyAnnotation marker
	if ackMsg.Annotations == nil {
		t.Errorf("R19 HIGH-1 回归：terse ack toolMsg 应有 Annotations 含 SkillBodyAnnotation，实际 nil")
	} else if v, ok := ackMsg.Annotations[agent_v2.SkillBodyAnnotation].(bool); !ok || !v {
		t.Errorf("R19 HIGH-1 回归：terse ack toolMsg 应标 SkillBodyAnnotation marker（保 4 层压缩 "+
			"splitProtectedGroups 不切 asst tool_call group 到 summarize），实际 Annotations=%v", ackMsg.Annotations)
	}
}

// dumpRoles 把 messages 序列化成 role 列表方便 debug。
func dumpRoles(msgs []*schema.Message) string {
	var sb strings.Builder
	for i, m := range msgs {
		if m == nil {
			continue
		}
		preview := m.Content
		if len(preview) > 30 {
			preview = preview[:30] + "..."
		}
		sb.WriteString("\n  [")
		sb.WriteString(string(rune('0' + i)))
		sb.WriteString("] ")
		sb.WriteString(string(m.Role))
		sb.WriteString(" ")
		sb.WriteString(preview)
	}
	return sb.String()
}
