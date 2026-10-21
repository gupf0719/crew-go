package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/skill"
)

// fakeSkillBox 测试用最小 SkillBox 实现——只给 Name/ListSkills/ActivateSkill 真值，其他返空。
type fakeSkillBox struct {
	skills      []skill.SkillDescriptor
	activations map[string]string // skill name → prompt
}

func (b *fakeSkillBox) Name() string                        { return "fake" }
func (b *fakeSkillBox) RootDir() string                     { return "" }
func (b *fakeSkillBox) ListSkills() []skill.SkillDescriptor { return b.skills }
func (b *fakeSkillBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult {
	return nil
}
func (b *fakeSkillBox) ListSkillResources(_ string) []skill.SkillResource { return nil }
func (b *fakeSkillBox) ReadSkillResource(_, _ string) string              { return "" }
func (b *fakeSkillBox) ReadSkillPrompt(name string) string                { return b.activations[name] }
func (b *fakeSkillBox) ListSkillActions(_ string) []skill.SkillActionDescriptor {
	return nil
}
func (b *fakeSkillBox) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	p, ok := b.activations[name]
	if !ok {
		return nil, errors.New("skill not found: " + name)
	}
	return &skill.ActiveSkill{
		Skill:  skill.SkillDescriptor{Name: name},
		Prompt: p,
	}, nil
}
func (b *fakeSkillBox) RunSkillAction(_ context.Context, _, _ string, _ map[string]any) (*skill.SkillExecutionResult, error) {
	return nil, skill.ErrSkillActionNotFound
}

var _ skill.SkillBox = (*fakeSkillBox)(nil)

// TestSkill_ActivatedViaTool 验证 LLM tool_call(activate_skill) → 框架返 prompt → 第二轮 LLM 用 prompt 回答。
func TestSkill_ActivatedViaTool(t *testing.T) {
	box := &fakeSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "把文本翻译成英文"},
		},
		activations: map[string]string{
			"translate": "你是一名翻译，把用户输入的中文翻译成英文。",
		},
	}

	// stub round 1: tool_call(activate_skill, {skill: "translate"})
	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"translate"}`,
		},
	}})
	// round 2: 普通 final 答复（没 tool_call）
	r2 := schema.AssistantMessage("translated done")
	stub := newStubModel([]*schema.Message{r1, r2})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("translator"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("翻译你好"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "translated done") {
		t.Errorf("expect final 'translated done', got %v", final)
	}
}

// capturingHook 捕获 OnModelStart round=0 看到的 enriched 消息列表——用于断言
// system prompt 实际内容 (含 framework preamble 注入的 sys + skill body + memory).
//
// 8-method: 改用 OnModelStart 替代 OnAgentStart — 后者整个调用 1 次在 Run top-level
// fire, 此时仅 raw user input. 业务想看 enriched 用 OnModelStart (modelNode 闭包内 fire).
//
// 记的是 `in.Preamble` 不是 `in.Messages`——agent 的 system prompt（含 skill hint /
// skill body）走前言那条流，不进被压缩的消息流。见 schema.ModelInput.Preamble。
type capturingHook struct {
	hook.NoOpHooks
	initPreamble []*schema.Message
}

func (h *capturingHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	if info.Round == 0 {
		h.initPreamble = append([]*schema.Message{}, in.Preamble...)
	}
	return ctx, nil
}

// TestSkill_DefaultSystemPromptHint 验证 WithSkills 启用 + 不设 WithSkillSystemPrompt
// 时框架追加默认中文 hint。
func TestSkill_DefaultSystemPromptHint(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}},
		activations: map[string]string{"translate": "翻译指南"},
	}
	captureHook := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithSystemPrompt("You are an assistant."),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithHooks(captureHook),
	)

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 找 system message
	if len(captureHook.initPreamble) == 0 {
		t.Fatal("hook 未捕获到 messages")
	}
	sysMsg := captureHook.initPreamble[0]
	if sysMsg.Role != schema.RoleSystem {
		t.Fatalf("first message 应是 system, got role=%v", sysMsg.Role)
	}
	if !strings.Contains(sysMsg.Content, "You are an assistant.") {
		t.Errorf("system prompt 应保留业务原文，实际 %q", sysMsg.Content)
	}
	if !strings.Contains(sysMsg.Content, "activate_skill") {
		t.Errorf("system prompt 应含默认 hint 提到 activate_skill，实际 %q", sysMsg.Content)
	}
	if !strings.Contains(sysMsg.Content, "专项技能") {
		t.Errorf("system prompt 应含默认中文 hint，实际 %q", sysMsg.Content)
	}
}

// TestSkill_CustomSystemPrompt 验证 WithSkillSystemPrompt 覆盖默认 hint。
func TestSkill_CustomSystemPrompt(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate", Description: "translate"}},
		activations: map[string]string{"translate": "translate playbook"},
	}
	captureHook := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	customHint := "When the user request matches a specialized skill, " +
		"first call activate_skill to load the skill's playbook."

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithSystemPrompt("You are an assistant."),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSystemPrompt(customHint),
		agent_v2.WithHooks(captureHook),
	)

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(captureHook.initPreamble) == 0 {
		t.Fatal("hook 未捕获到 messages")
	}
	sysMsg := captureHook.initPreamble[0]
	if !strings.Contains(sysMsg.Content, customHint) {
		t.Errorf("system prompt 应含自定义 hint，实际 %q", sysMsg.Content)
	}
	// 关键：默认中文 hint 必须不出现（覆盖语义）
	if strings.Contains(sysMsg.Content, "专项技能") {
		t.Errorf("自定义 hint 应覆盖默认中文，实际仍含「专项技能」: %q", sysMsg.Content)
	}
}

// TestSkill_CustomSystemPrompt_EmptyFallsBackToDefault 验证传空字符串时
// 回退到默认 hint（向后兼容）。
func TestSkill_CustomSystemPrompt_EmptyFallsBackToDefault(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}},
		activations: map[string]string{"translate": "翻译指南"},
	}
	captureHook := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithSystemPrompt("base prompt"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSystemPrompt(""), // 显式传空
		agent_v2.WithHooks(captureHook),
	)

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sysMsg := captureHook.initPreamble[0]
	if !strings.Contains(sysMsg.Content, "专项技能") {
		t.Errorf("空自定义应回退到默认中文 hint，实际 %q", sysMsg.Content)
	}
}

// TestSkill_DefaultHint_EmptySystemPrompt 验证业务**不设** WithSystemPrompt
// 时 hint 不带 leading `\n\n`——sysPrompt = "" + "\n\n" + hint 这种语义不洁
// 输出已被 conditional separator 修复。
func TestSkill_DefaultHint_EmptySystemPrompt(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}},
		activations: map[string]string{"translate": "翻译指南"},
	}
	captureHook := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		// 故意不设 WithSystemPrompt
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithHooks(captureHook),
	)

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(captureHook.initPreamble) == 0 {
		t.Fatal("hook 未捕获到 messages")
	}
	sysMsg := captureHook.initPreamble[0]
	if sysMsg.Role != schema.RoleSystem {
		t.Fatalf("first message 应是 system, got role=%v", sysMsg.Role)
	}
	if strings.HasPrefix(sysMsg.Content, "\n") {
		t.Errorf("空 SystemPrompt 时 hint 不应带 leading `\\n`，实际 %q", sysMsg.Content[:min(20, len(sysMsg.Content))])
	}
	if !strings.HasPrefix(sysMsg.Content, "当你判断") {
		t.Errorf("空 SystemPrompt + 默认 hint 应直接以 hint 开头，实际 %q", sysMsg.Content[:min(30, len(sysMsg.Content))])
	}
}

// TestSkill_MultipleWithSkillSystemPrompt_LastWins 验证多次调 WithSkillSystemPrompt
// 后调覆盖前调（option 累积语义）。
func TestSkill_MultipleWithSkillSystemPrompt_LastWins(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}},
		activations: map[string]string{"translate": "翻译指南"},
	}
	captureHook := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSystemPrompt("FIRST hint should be overridden"),
		agent_v2.WithSkillSystemPrompt("SECOND hint should win"),
		agent_v2.WithHooks(captureHook),
	)

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sysMsg := captureHook.initPreamble[0]
	if !strings.Contains(sysMsg.Content, "SECOND hint should win") {
		t.Errorf("后调 WithSkillSystemPrompt 应覆盖前调，实际 %q", sysMsg.Content)
	}
	if strings.Contains(sysMsg.Content, "FIRST hint") {
		t.Errorf("前调 hint 应被覆盖，实际仍含 FIRST: %q", sysMsg.Content)
	}
}

// TestSkill_CustomSystemPrompt_NoSkillBox 验证未启用 skill 时
// SkillSystemPrompt 完全无效（不污染 system prompt）。
func TestSkill_CustomSystemPrompt_NoSkillBox(t *testing.T) {
	captureHook := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithSystemPrompt("base prompt only"),
		agent_v2.WithModel(stub),
		// 不调 WithSkills
		agent_v2.WithSkillSystemPrompt("this should not appear"),
		agent_v2.WithHooks(captureHook),
	)

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sysMsg := captureHook.initPreamble[0]
	if strings.Contains(sysMsg.Content, "this should not appear") {
		t.Errorf("未启用 skill 时 SkillSystemPrompt 不应注入，实际 %q", sysMsg.Content)
	}
	if strings.Contains(sysMsg.Content, "activate_skill") {
		t.Errorf("未启用 skill 时不应注入 skill hint，实际 %q", sysMsg.Content)
	}
}

// ── 黑盒测公开 API（package agent_v2_test）：WithSkillSelector / WithSkillSelectionHook /
// WithSkillBodyMaxChars 公开 Option 业务从外部 import 用，必须有黑盒覆盖确保 import
// 路径可达 + API 行为符合 godoc（R8 audit M5 fix）。
//
// adaptive 模式 + selectorRouter LLM 选 skill + selectionHook 业务订阅 + bodyMaxChars 截断
// 全链路黑盒覆盖。

// blackboxAdaptiveBox 黑盒测试用 SkillBox（外包 _test 包，无 internal_test 类型可用）
type blackboxAdaptiveBox struct {
	skills  []skill.SkillDescriptor
	matches []skill.SkillMatchResult
	prompts map[string]string
}

func (b *blackboxAdaptiveBox) Name() string                        { return "bb" }
func (b *blackboxAdaptiveBox) RootDir() string                     { return "" }
func (b *blackboxAdaptiveBox) ListSkills() []skill.SkillDescriptor { return b.skills }
func (b *blackboxAdaptiveBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult {
	return b.matches
}
func (b *blackboxAdaptiveBox) ListSkillResources(_ string) []skill.SkillResource { return nil }
func (b *blackboxAdaptiveBox) ReadSkillResource(_, _ string) string              { return "" }
func (b *blackboxAdaptiveBox) ReadSkillPrompt(_ string) string                   { return "" }
func (b *blackboxAdaptiveBox) ListSkillActions(_ string) []skill.SkillActionDescriptor {
	return nil
}
func (b *blackboxAdaptiveBox) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	if p, ok := b.prompts[name]; ok {
		return &skill.ActiveSkill{Skill: skill.SkillDescriptor{Name: name}, Prompt: p}, nil
	}
	return nil, errors.New("not found: " + name)
}
func (b *blackboxAdaptiveBox) RunSkillAction(_ context.Context, _, _ string, _ map[string]any) (*skill.SkillExecutionResult, error) {
	return nil, skill.ErrSkillActionNotFound
}

// TestSkill_BlackboxAdaptiveAndHook 黑盒覆盖 P1.B + R7 H2 公开 API：
// adaptive 模式 + WithSkillSelectionHook 同时启用，业务 hook 拿到 SkillSelection。
func TestSkill_BlackboxAdaptiveAndHook(t *testing.T) {
	box := &blackboxAdaptiveBox{
		skills: []skill.SkillDescriptor{{Name: "translate"}},
		matches: []skill.SkillMatchResult{
			{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.9},
		},
		prompts: map[string]string{"translate": "BLACKBOX_PROMPT"},
	}
	captured := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	var hookCalled int
	var hookSelection agent_v2.SkillSelection
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		agent_v2.WithSkillSelectionHook(func(_ context.Context, sel agent_v2.SkillSelection) {
			hookCalled++
			hookSelection = sel
		}),
		agent_v2.WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("translate hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 hook 被调 + 拿到 selection
	if hookCalled != 1 {
		t.Errorf("hook 应被调 1 次，实际 %d", hookCalled)
	}
	if hookSelection.Skill == nil || hookSelection.Skill.Name != "translate" {
		t.Errorf("hook 应拿到 translate selection，实际 %v", hookSelection.Skill)
	}
	if hookSelection.Strategy != agent_v2.SkillSelectionStrategySingleCandidate {
		t.Errorf("hook strategy 应=single_candidate（公开常量），实际 %q", hookSelection.Strategy)
	}

	// 验证 sysPrompt 含 skill body
	if len(captured.initPreamble) == 0 || !strings.Contains(captured.initPreamble[0].Content, "BLACKBOX_PROMPT") {
		t.Errorf("sysPrompt 应含 skill body，实际 %v", captured.initPreamble)
	}
}

// TestSkill_BlackboxBodyMaxChars 黑盒覆盖 R7 H1 公开 API：WithSkillBodyMaxChars 截断
// 大 SKILL.md body。
func TestSkill_BlackboxBodyMaxChars(t *testing.T) {
	hugeBody := strings.Repeat("x", 50000)
	box := &blackboxAdaptiveBox{
		skills: []skill.SkillDescriptor{{Name: "huge"}},
		matches: []skill.SkillMatchResult{
			{Skill: skill.SkillDescriptor{Name: "huge"}, Score: 0.9},
		},
		prompts: map[string]string{"huge": hugeBody},
	}
	captured := &capturingHook{}
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		agent_v2.WithSkillBodyMaxChars(5000),
		agent_v2.WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sysMsg := captured.initPreamble[0]
	if len(sysMsg.Content) > 5000 {
		t.Errorf("WithSkillBodyMaxChars(5000) 应截断 sysPrompt ≤ 5000，实际 %d char", len(sysMsg.Content))
	}
}

// TestSkill_UnknownSkillReturnsError 验证 LLM 调 activate_skill 但 skill name 不存在时
// 返 tool error message（不 panic / 不 abort）。
func TestSkill_UnknownSkillReturnsError(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate"}},
		activations: map[string]string{"translate": "翻译技能"},
	}

	r1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"nonexistent_skill"}`,
		},
	}})
	r2 := schema.AssistantMessage("recovered final")
	stub := newStubModel([]*schema.Message{r1, r2})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "recovered final") {
		t.Errorf("expect final 'recovered final' after unknown skill error, got %v", final)
	}
}
