package agent_v2

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
)

// adaptiveMockBox 是 adaptive 测试用的 SkillBox——MatchSkills 返预设结果。
type adaptiveMockBox struct {
	mockSkillBox
	matches []skill.SkillMatchResult
}

func (b *adaptiveMockBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult {
	return b.matches
}

// adaptiveMockBoxWithActivate 同上但能 ActivateSkill 返 prompt。
type adaptiveMockBoxWithActivate struct {
	adaptiveMockBox
	prompts map[string]string
}

func (b *adaptiveMockBoxWithActivate) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	p, ok := b.prompts[name]
	if !ok {
		return nil, errors.New("not found")
	}
	return &skill.ActiveSkill{
		Skill:  skill.SkillDescriptor{Name: name},
		Prompt: p,
	}, nil
}

// routerStubModel 实现 model.ChatModel 返预设 router decision JSON。
type routerStubModel struct {
	response string // LLM 输出原文
	calls    int    // 被调次数
}

func (m *routerStubModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls++
	return schema.AssistantMessage(m.response), nil
}
func (m *routerStubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *routerStubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*routerStubModel)(nil)

// ── selectSkillAdaptive 单元测 ──

// TestSelectSkillAdaptive_NoMatch 验证 box.MatchSkills 返空 → strategy="none"
func TestSelectSkillAdaptive_NoMatch(t *testing.T) {
	box := &adaptiveMockBox{matches: nil}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, nil, false)
	if sel.Strategy != "none" {
		t.Errorf("应 strategy=none，实际 %q", sel.Strategy)
	}
	if sel.Skill != nil {
		t.Errorf("无候选不应激活，实际 skill=%v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_MinScoreFilter 验证 top1 score < MinScore → strategy="none"
func TestSelectSkillAdaptive_MinScoreFilter(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "low"}, Score: 0.1},
	}}
	cfg := &SkillSelectorConfig{MinScore: 0.5}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if sel.Strategy != "none" {
		t.Errorf("score 低应不激活，实际 %q", sel.Strategy)
	}
}

// TestSelectSkillAdaptive_SingleCandidate 验证 1 个候选直接激活
func TestSelectSkillAdaptive_SingleCandidate(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.9},
	}}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, nil, false)
	if sel.Strategy != "single_candidate" {
		t.Errorf("应 strategy=single_candidate，实际 %q", sel.Strategy)
	}
	if sel.Skill == nil || sel.Skill.Name != "translate" {
		t.Errorf("应激活 translate，实际 %v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_PreMatchSkipsLLM 验证 pre_match 模式多候选直接 top1（即使配 Model）
func TestSelectSkillAdaptive_PreMatchSkipsLLM(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	stub := &routerStubModel{response: `{"skill_name":"b","reason":"prefer b"}`}
	cfg := &SkillSelectorConfig{Model: stub}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, true /* pre_match */)
	if sel.Strategy != "skipped_pre_match" {
		t.Errorf("pre_match 应 strategy=skipped_pre_match，实际 %q", sel.Strategy)
	}
	if sel.Skill == nil || sel.Skill.Name != "a" {
		t.Errorf("pre_match 应选 top1=a，实际 %v", sel.Skill)
	}
	if stub.calls > 0 {
		t.Errorf("pre_match 不应调 LLM，实际被调 %d 次", stub.calls)
	}
}

// TestSelectSkillAdaptive_AdaptiveNoModel_FallbackTop1 验证 adaptive + 无 Model → top1
func TestSelectSkillAdaptive_AdaptiveNoModel_FallbackTop1(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	cfg := &SkillSelectorConfig{} // Model nil
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if sel.Strategy != "fallback_top1" {
		t.Errorf("adaptive 无 Model 应 strategy=fallback_top1，实际 %q", sel.Strategy)
	}
	if sel.Skill.Name != "a" {
		t.Errorf("应选 top1=a，实际 %v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_AdaptiveLLMRouter 验证 adaptive + 有 Model → LLM 仲裁
func TestSelectSkillAdaptive_AdaptiveLLMRouter(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	stub := &routerStubModel{response: `{"skill_name":"b","reason":"b matches better"}`}
	cfg := &SkillSelectorConfig{Model: stub}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if sel.Strategy != "llm_router" {
		t.Errorf("adaptive 多候选 + LLM 应 strategy=llm_router，实际 %q", sel.Strategy)
	}
	if sel.Skill == nil || sel.Skill.Name != "b" {
		t.Errorf("LLM 选 b 应激活 b，实际 %v", sel.Skill)
	}
	if sel.Reason != "b matches better" {
		t.Errorf("应保留 LLM reason，实际 %q", sel.Reason)
	}
	if stub.calls != 1 {
		t.Errorf("LLM 应被调 1 次，实际 %d", stub.calls)
	}
}

// TestSelectSkillAdaptive_LLMReturnsInvalidName_FallbackTop1 验证 LLM 选了不在候选 → top1
func TestSelectSkillAdaptive_LLMReturnsInvalidName_FallbackTop1(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	stub := &routerStubModel{response: `{"skill_name":"nonexistent","reason":"x"}`}
	cfg := &SkillSelectorConfig{Model: stub}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if sel.Strategy != "fallback_top1" {
		t.Errorf("LLM 选不在候选应 fallback_top1，实际 %q", sel.Strategy)
	}
	if sel.Skill.Name != "a" {
		t.Errorf("应 fallback 到 top1=a，实际 %v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_LLMReturnsGarbage_FallbackTop1 验证 LLM 输出无效 JSON → top1
func TestSelectSkillAdaptive_LLMReturnsGarbage_FallbackTop1(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	stub := &routerStubModel{response: `not json at all`}
	cfg := &SkillSelectorConfig{Model: stub}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if sel.Strategy != "fallback_top1" {
		t.Errorf("LLM 输出无效应 fallback_top1，实际 %q", sel.Strategy)
	}
}

// TestSelectSkillAdaptive_MaxCandidatesTruncates 验证 MaxCandidates 截断候选给 LLM
func TestSelectSkillAdaptive_MaxCandidatesTruncates(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
		{Skill: skill.SkillDescriptor{Name: "c"}, Score: 0.7},
		{Skill: skill.SkillDescriptor{Name: "d"}, Score: 0.6},
	}}
	stub := &routerStubModel{response: `{"skill_name":"d","reason":"x"}`}
	cfg := &SkillSelectorConfig{Model: stub, MaxCandidates: 2}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	// MaxCandidates=2 截断到 [a, b]，LLM 选 d（不在候选）→ fallback top1=a
	if sel.Strategy != "fallback_top1" {
		t.Errorf("LLM 选被截断的候选应 fallback，实际 %q", sel.Strategy)
	}
	if sel.Skill.Name != "a" {
		t.Errorf("应 fallback top1=a，实际 %v", sel.Skill)
	}
}

// ── parseRouterDecision 单元测 ──

func TestParseRouterDecision(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantSkill string
		wantOK    bool
	}{
		{"纯 JSON", `{"skill_name":"x","reason":"y"}`, "x", true},
		{"前后带说明", `Sure, my choice: {"skill_name":"x","reason":"y"} done.`, "x", true},
		{"代码块包裹", "```json\n{\"skill_name\":\"x\",\"reason\":\"y\"}\n```", "x", true},
		{"无效 JSON", `not json`, "", false},
		{"缺 skill_name", `{"reason":"y"}`, "", false},
		{"空字符串", ``, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			name, _ := parseRouterDecision(c.input)
			if c.wantOK && name != c.wantSkill {
				t.Errorf("expect %q, got %q", c.wantSkill, name)
			}
			if !c.wantOK && name != "" {
				t.Errorf("expect empty (parse fail), got %q", name)
			}
		})
	}
}

// ── 端到端测：BasicAgent + adaptive 模式 ──

// TestBasicAgent_AdaptiveMode_PreActivatesSkillIntoSysPrompt 验证 adaptive 模式
// 真把 skill body 注入 system prompt（而不是注 activate_skill 工具）
func TestBasicAgent_AdaptiveMode_PreActivatesSkillIntoSysPrompt(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.9},
			},
		},
		prompts: map[string]string{
			"translate": "PRE_ACTIVATED_TRANSLATE_PLAYBOOK",
		},
	}
	captured := &capturingHookInternal{}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	ag := NewBasicAgent(
		WithName("ag"),
		WithSystemPrompt("base"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("translate hello")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(captured.initPreamble) == 0 {
		t.Fatal("hook 未捕获到 messages")
	}
	sysMsg := captured.initPreamble[0]
	if !strings.Contains(sysMsg.Content, "PRE_ACTIVATED_TRANSLATE_PLAYBOOK") {
		t.Errorf("adaptive 模式应把 skill body 注入 sysPrompt，实际 %q", sysMsg.Content)
	}
	// 不应有 toolCall hint（"activate_skill" 字符串）
	if strings.Contains(sysMsg.Content, "activate_skill") {
		t.Errorf("adaptive 模式不应注 toolCall hint，实际含 activate_skill: %q", sysMsg.Content)
	}
	// 不应注 activate_skill 工具——验证 stub 看到的工具表
	for _, tn := range stub.lastTools {
		if tn == activateSkillToolName {
			t.Errorf("adaptive 模式不应注 activate_skill 工具，实际 stub 看到工具 %v", stub.lastTools)
		}
	}
}

// errorRouterModel 模拟 router LLM 返特定 error——验 H2 propagate 行为。
type errorRouterModel struct {
	err error
}

func (m *errorRouterModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, m.err
}
func (m *errorRouterModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *errorRouterModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestSelectSkillAdaptive_InterruptErrorPropagates 验证 综合 audit H1：router LLM 抛
// InterruptError 时 selectSkillAdaptive 必须 propagate err 给 caller（不能 swallow 走
// fallback top1 丢 HITL 信号；也不能塞进 SkillSelection.Strategy 让 caller silent 继续）。
func TestSelectSkillAdaptive_InterruptErrorPropagates(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	intErr := &compose_v2.InterruptError{
		Query: &compose_v2.AskQuery{InterruptID: "test_id", Question: "需要补充信息？"},
	}
	cfg := &SkillSelectorConfig{Model: &errorRouterModel{err: intErr}}
	sel, err := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if err == nil {
		t.Fatal("InterruptError 必须 propagate（返非 nil err），实际 err=nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("propagated err 应是 InterruptError，实际 %v", err)
	}
	if sel.Skill != nil {
		t.Errorf("control flow err 时 sel.Skill 应 nil（语义无效），实际 %v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_CtxCancelledPropagates 验证 综合 audit H1：ctx.Err 必须 propagate。
func TestSelectSkillAdaptive_CtxCancelledPropagates(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 提前 cancel
	cfg := &SkillSelectorConfig{Model: &errorRouterModel{err: ctx.Err()}}
	_, err := selectSkillAdaptive(ctx, "x", box, cfg, false)
	if err == nil {
		t.Fatal("ctx.Cancel 必须 propagate（返非 nil err），实际 err=nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("propagated err 应是 context.Canceled，实际 %v", err)
	}
}

// TestBasicAgent_AdaptiveMode_InterruptPropagatesToRun 端到端验证 综合 audit H1：
// router LLM 抛 InterruptError → BasicAgent.Run 必须返这个 err（让上层 SaveNS cp）。
// 这是 H1 的关键 sanity test——单独测 selectSkillAdaptive 返 err 不够，必须验
// buildInitialMessages 真把它 propagate 出去。
func TestBasicAgent_AdaptiveMode_InterruptPropagatesToRun(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
				{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
			},
		},
		prompts: map[string]string{"a": "A_PROMPT", "b": "B_PROMPT"},
	}
	intErr := &compose_v2.InterruptError{
		Query: &compose_v2.AskQuery{InterruptID: "router_paused", Question: "需要业务确认"},
	}

	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(&simpleStub{response: schema.AssistantMessage("won't reach")}),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithSkillSelector(SkillSelectorConfig{
			Model: &errorRouterModel{err: intErr},
		}),
	)
	_, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("Run 必须返 InterruptError（综合 audit H1：buildInitialMessages 应 propagate），实际 err=nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("Run 返的 err 应是 InterruptError，实际 %v", err)
	}
}

// TestSelectSkillAdaptive_NormalLLMErrorFallsBackTop1 验证非控制流 error 仍 fallback top1。
func TestSelectSkillAdaptive_NormalLLMErrorFallsBackTop1(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.8},
	}}
	cfg := &SkillSelectorConfig{Model: &errorRouterModel{err: errors.New("rate limit")}}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if sel.Strategy != "fallback_top1" {
		t.Errorf("普通 LLM 错应 fallback_top1，实际 %q", sel.Strategy)
	}
	if sel.Skill.Name != "a" {
		t.Errorf("应 fallback top1=a，实际 %v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_DoesNotMutateMatchesSlice 验证 audit M1：filter 不污染
// 业务自定义 SkillBox 返的 slice 底层数组。
//
// **R8 audit H1 修订 fixture**：旧 fixture `[a:0.9, b:0.1, c:0.05]` + MinScore=0.5
// 只有 a 通过，in-place filter 第 1 次 append 写 matches[0]="a"（恰好原位无变化）→
// 测试假绿（sanity-revert M1 fix 仍 PASS）。新 fixture 让 c 也通过 → in-place filter
// 第 2 次 append 写 matches[1]="c" 污染 → 真能 catch 退化。
func TestSelectSkillAdaptive_DoesNotMutateMatchesSlice(t *testing.T) {
	// 业务自定义 box 返一个内部缓存共享的 slice
	// 关键：c 也通过过滤（Score 0.8 ≥ 0.5）让 in-place filter 第 2 次 append 写到 matches[1]
	cached := []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a"}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b"}, Score: 0.1}, // 低分被过滤
		{Skill: skill.SkillDescriptor{Name: "c"}, Score: 0.8}, // 高分通过 → 暴露 in-place 污染
	}
	box := &adaptiveMockBox{matches: cached}
	cfg := &SkillSelectorConfig{MinScore: 0.5}
	_, _ = selectSkillAdaptive(context.Background(), "x", box, cfg, false)

	// 验证 cached 底层数组未被改写——
	// fix 前（in-place `matches[:0]`）：cached[1] 被覆盖为 c（污染）
	// fix 后（`make([]..., 0, len)` 复制）：cached[1] 仍是 b
	if cached[1].Skill.Name != "b" {
		t.Fatalf("M1 回归：业务 box 返的 slice 被污染！cached[1] 应是 'b' 实际 %q\n  full cached=%+v",
			cached[1].Skill.Name, cached)
	}
	if cached[2].Skill.Name != "c" {
		t.Fatalf("M1 回归：cached[2] 不应被改 实际 %q", cached[2].Skill.Name)
	}
}

// TestSelectSkillAdaptive_EmptyChosenName_FallbackTop1 验证 M5 + R17 audit LOW-3：
//
// 业务自定义 SkillBox 返 Name="" 脏数据时不能影响选择路径。
//
// **R17 audit LOW-3 加强**：早期 enabled filter 直接 skip Name=="" 候选——LLM 路径
// 看不到 empty Name 候选，不会被选中。只剩"real" 单候选 → strategy=single_candidate。
//
// **M5 历史防御**（仍守住）：即使 enabled filter 漏过（理论不可能但防御性），
// LLM 返 chosen="" 时不能误命中 Name="" 的 entry。M5 + R17 双保险——R17 是上游
// 过滤，M5 是下游兜底。
func TestSelectSkillAdaptive_EmptyChosenName_FallbackTop1(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: ""}, Score: 0.9},     // 业务 box 数据 bug：name 空
		{Skill: skill.SkillDescriptor{Name: "real"}, Score: 0.8}, // 正常 name
	}}
	// LLM 返不可解析输出 → chosen = ""
	stub := &routerStubModel{response: `garbage`}
	cfg := &SkillSelectorConfig{Model: stub}
	sel, _ := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	// R17 后预期：empty Name 候选被早期 enabled filter 过滤 → 只剩 1 候选 "real"
	// → 走 single_candidate 路径不进 LLM 仲裁
	if sel.Strategy != SkillSelectionStrategySingleCandidate {
		t.Errorf("R17 audit LOW-3：empty Name 候选应被 enabled filter 早过滤 → "+
			"剩 1 候选 'real' → strategy=single_candidate，实际 %q", sel.Strategy)
	}
	if sel.Skill == nil || sel.Skill.Name != "real" {
		t.Errorf("应选剩余 'real'，实际 %v", sel.Skill)
	}
}

// TestBasicAgent_AdaptiveMode_ActivateSkillFails_NoSkillInjection 验证 audit H1：
// adaptive 模式 ActivateSkill 失败时 sysPrompt **不**含 skill body（log warn + 跳过注入）
type failingActivateBox struct {
	adaptiveMockBox
}

func (b *failingActivateBox) ActivateSkill(_ context.Context, _ string) (*skill.ActiveSkill, error) {
	return nil, errors.New("simulated activation failure")
}

func TestBasicAgent_AdaptiveMode_ActivateSkillFails_NoSkillInjection(t *testing.T) {
	box := &failingActivateBox{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.9},
			},
		},
	}
	captured := &capturingHookInternal{}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	ag := NewBasicAgent(
		WithName("ag"),
		WithSystemPrompt("base"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(captured.initPreamble) == 0 {
		t.Fatal("hook 未捕获到 messages")
	}
	sysMsg := captured.initPreamble[0]
	// 应保留 base sysPrompt + 不含任何 skill body
	if sysMsg.Content != "base" {
		t.Errorf("ActivateSkill 失败时 sysPrompt 应只保留 'base'，实际 %q", sysMsg.Content)
	}
}

// TestBasicAgent_AdaptiveMode_BodyTruncated 验证 R7 audit H1 fix：超长 skill body
// 注入 sysPrompt 时按 SkillBodyMaxChars 截断（防止打满 ctx window）。
func TestBasicAgent_AdaptiveMode_BodyTruncated(t *testing.T) {
	hugeBody := strings.Repeat("x", 50000) // 50K char，超默认 20K 上限
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "huge"}, Score: 0.9},
			},
		},
		prompts: map[string]string{"huge": hugeBody},
	}
	captured := &capturingHookInternal{}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithSkillBodyMaxChars(10000), // 显式 10K 上限
		WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sysMsg := captured.initPreamble[0]
	if len(sysMsg.Content) > 10000 {
		t.Errorf("sysPrompt 应被截断 ≤ 10000 char，实际 %d char", len(sysMsg.Content))
	}
	if !strings.HasSuffix(sysMsg.Content, "…") {
		t.Errorf("截断后应带 trailing '…'，实际尾部 %q", sysMsg.Content[max(0, len(sysMsg.Content)-30):])
	}
}

// TestBasicAgent_AdaptiveMode_BodyDefaultLimit 验证不设 WithSkillBodyMaxChars 时
// 用 DefaultSkillBodyMaxChars 默认值。
func TestBasicAgent_AdaptiveMode_BodyDefaultLimit(t *testing.T) {
	hugeBody := strings.Repeat("x", DefaultSkillBodyMaxChars+5000)
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "huge"}, Score: 0.9},
			},
		},
		prompts: map[string]string{"huge": hugeBody},
	}
	captured := &capturingHookInternal{}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		// 不设 WithSkillBodyMaxChars → 用 DefaultSkillBodyMaxChars (20000)
		WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sysMsg := captured.initPreamble[0]
	if len(sysMsg.Content) > DefaultSkillBodyMaxChars {
		t.Errorf("默认 SkillBodyMaxChars=%d 应截断，实际 %d char",
			DefaultSkillBodyMaxChars, len(sysMsg.Content))
	}
}

// TestBasicAgent_AdaptiveMode_TinyMaxChars_NoEmptyArtifact 验证 R10 audit HIGH fix：
// 极小 WithSkillBodyMaxChars（< 3，truncateWithEllipsis 边界）使 body 截断为 "" 时，
// 不应在 sysPrompt 末尾留下 "\n\n" dangling delimiter（silent 丢 skill 注入信号）。
//
// **R10 sanity-revert PASS**：去掉 `if body == ""` 守卫后，本测试 FAIL 报
// `sysPrompt = "base\n\n"` 精确指向 bug。
func TestBasicAgent_AdaptiveMode_TinyMaxChars_NoEmptyArtifact(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.9},
			},
		},
		prompts: map[string]string{"translate": "REAL_SKILL_BODY_HERE_LONG_ENOUGH"},
	}
	captured := &capturingHookInternal{}
	ag := NewBasicAgent(
		WithName("ag"),
		WithSystemPrompt("base"),
		WithModel(&simpleStub{response: schema.AssistantMessage("done")}),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithSkillBodyMaxChars(2), // 极小值——truncateWithEllipsis(body, 2) 返 ""
		WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(captured.initPreamble) == 0 {
		t.Fatal("hook 未捕获 messages")
	}
	sysContent := captured.initPreamble[0].Content
	// 期望 sysPrompt 仅 "base"（empty body 跳过拼接），不应留 "\n\n" artifact
	if strings.HasSuffix(sysContent, "\n\n") {
		t.Errorf("R10 HIGH 回归：sysPrompt 末尾不应留 '\\n\\n' artifact，实际 %q", sysContent)
	}
	if sysContent != "base" {
		t.Errorf("R10 HIGH 回归：sysPrompt 应为 'base'（empty body 跳过拼接），实际 %q", sysContent)
	}
}

// TestBasicAgent_AdaptiveMode_SkillSelectionHookCalled 验证 R7 audit H2 fix：
// 业务 SkillSelectionHook 被调用并能拿到 SkillSelection（之前业务完全不可达）。
func TestBasicAgent_AdaptiveMode_SkillSelectionHookCalled(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.95},
			},
		},
		prompts: map[string]string{"translate": "T_PROMPT"},
	}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	var capturedSel SkillSelection
	hookCalls := 0

	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithSkillSelectionHook(func(_ context.Context, sel SkillSelection) {
			capturedSel = sel
			hookCalls++
		}),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("translate this")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if hookCalls != 1 {
		t.Errorf("SkillSelectionHook 应被调 1 次，实际 %d", hookCalls)
	}
	if capturedSel.Skill == nil || capturedSel.Skill.Name != "translate" {
		t.Errorf("hook 应拿到 selection.Skill=translate，实际 %v", capturedSel.Skill)
	}
	if capturedSel.Strategy != SkillSelectionStrategySingleCandidate {
		t.Errorf("hook strategy 应=single_candidate，实际 %q", capturedSel.Strategy)
	}
}

// TestBasicAgent_AdaptiveMode_SkillSelectionHookCalledOnNoMatch 验证 selection.Skill nil
// （无候选）也调用 hook，业务能知道"没匹配上"原因。
func TestBasicAgent_AdaptiveMode_SkillSelectionHookCalledOnNoMatch(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{matches: []skill.SkillMatchResult{}}, // 无匹配
		prompts:         map[string]string{},
	}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	var capturedSel SkillSelection
	hookCalls := 0
	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithSkillSelectionHook(func(_ context.Context, sel SkillSelection) {
			capturedSel = sel
			hookCalls++
		}),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if hookCalls != 1 {
		t.Errorf("hook 即使 no_match 也应调用 1 次，实际 %d", hookCalls)
	}
	if capturedSel.Skill != nil {
		t.Errorf("无匹配时 selection.Skill 应 nil，实际 %v", capturedSel.Skill)
	}
	if capturedSel.Strategy != SkillSelectionStrategyNone {
		t.Errorf("无匹配 strategy 应=none，实际 %q", capturedSel.Strategy)
	}
}

// TestBasicAgent_AdaptiveMode_SkillSelectionHookPanicRecovered 验证业务 hook panic
// 不阻断 Run（recover 转 slog.Warn）。
func TestBasicAgent_AdaptiveMode_SkillSelectionHookPanicRecovered(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "x"}, Score: 0.9},
			},
		},
		prompts: map[string]string{"x": "X_PROMPT"},
	}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithSkillSelectionHook(func(_ context.Context, _ SkillSelection) {
			panic("business hook bug")
		}),
	)
	// 不应 panic，Run 正常完成
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run 应成功（hook panic 被 recover），实际 err=%v", err)
	}
}

// TestBasicAgent_AdaptiveMode_PreservesUserToolkit 验证 audit M6：adaptive + 业务 toolkit
// 共存时业务工具仍在 toolkit 里（不被 adaptive 模式破坏）。
func TestBasicAgent_AdaptiveMode_PreservesUserToolkit(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{
				{Skill: skill.SkillDescriptor{Name: "translate"}, Score: 0.9},
			},
		},
		prompts: map[string]string{"translate": "T_PROMPT"},
	}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	// 用 stubUserTool 让 BasicAgent 看到一个用户工具
	captured := &capturingHookInternal{}
	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
		}),
		WithHooks(captured),
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("translate hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// adaptive 模式：activate_skill 不应出现；业务工具空时 stub.lastTools 也应空
	for _, tn := range stub.lastTools {
		if tn == activateSkillToolName {
			t.Errorf("adaptive 模式不应注 activate_skill 工具，实际 %v", stub.lastTools)
		}
	}
}

// TestBasicAgent_ToolCallMode_StillInjectsTool 验证 toolCall 模式仍注 activate_skill 工具
// （回归测试——确保 P1.B 改动没破 V1 行为）
func TestBasicAgent_ToolCallMode_StillInjectsTool(t *testing.T) {
	box := &adaptiveMockBoxWithActivate{
		adaptiveMockBox: adaptiveMockBox{
			matches: []skill.SkillMatchResult{},
		},
		prompts: map[string]string{"translate": "PROMPT"},
	}
	box.skills = []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}}
	stub := &simpleStub{response: schema.AssistantMessage("done")}

	ag := NewBasicAgent(
		WithName("ag"),
		WithModel(stub),
		WithSkills(skill.SkillSource{SkillBox: box}), // 不设 Options → toolCall 默认
	)
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	found := false
	for _, tn := range stub.lastTools {
		if tn == activateSkillToolName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("toolCall 模式应注 activate_skill 工具，实际 stub 看到工具 %v", stub.lastTools)
	}
}

// ── 测试 helper ──

// capturingHookInternal 内部测试用, 记录 OnModelStart round=0 看到的 enriched messages.
//
// 8-method: OnAgentStart 在 Run top-level fire (整个调用 1 次), 此时 messages 是
// raw user input (没经过 framework preamble: sys/memory/skill listing).
// 业务想看 enriched messages 应用 OnModelStart (modelNode 闭包内 fire 已 enriched).
// capture 只在 round=0 时记 (对齐 enriched 首轮视图).
//
// 记的是 `in.Preamble`——skill body 拼进 sysPrompt，而 sysPrompt 走前言那条流。
type capturingHookInternal struct {
	hook.NoOpHooks
	initPreamble []*schema.Message
}

func (h *capturingHookInternal) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	if info.Round == 0 {
		h.initPreamble = append([]*schema.Message{}, in.Preamble...)
	}
	return ctx, nil
}

// simpleStub 类似 stubModel 但能记录 WithTools 被传入的工具名
type simpleStub struct {
	response  *schema.Message
	lastTools []string
}

func (m *simpleStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.response, nil
}
func (m *simpleStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *simpleStub) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.lastTools = nil
	for _, t := range tools {
		if t != nil {
			m.lastTools = append(m.lastTools, t.Name)
		}
	}
	return m, nil
}

var _ model.ChatModel = (*simpleStub)(nil)

// ── DisableModelInvocation 在 adaptive selection 路径的 filter 测试 ──

// TestSelectSkillAdaptive_FiltersDisabledModelInvocation 验证 disabled skill 被排除。
//
// **bomb 防御（5 类炸弹·第 1 类——过滤后元素恰好原位）**：
// 旧 fixture 若 disabled idx 0、enabled idx 1，filter 完后 enabled 仍居 idx 1
// 看不出"过滤是否生效"。这里把 disabled top1 (score 0.9) + enabled top2 (score 0.8)，
// 过滤后 enabled 应升 idx 0 + Strategy=single_candidate（filter 把"3 候选"压成"1
// 候选"路径切换）。sanity-revert 删 filter → 仍 multi candidate → strategy="fallback_top1"
// + skill name=disabled_top1 → fail（含名字 + strategy 双重断言）。
func TestSelectSkillAdaptive_FiltersDisabledModelInvocation(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "disabled_top1", DisableModelInvocation: true}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "enabled_top2"}, Score: 0.8},
		{Skill: skill.SkillDescriptor{Name: "disabled_top3", DisableModelInvocation: true}, Score: 0.7},
	}}
	sel, err := selectSkillAdaptive(context.Background(), "x", box, nil, false)
	if err != nil {
		t.Fatalf("selectSkillAdaptive: %v", err)
	}
	// 过滤后剩 1 候选 → strategy 应切到 single_candidate
	if sel.Strategy != SkillSelectionStrategySingleCandidate {
		t.Errorf("过滤掉 2 disabled 后剩 1 候选应 strategy=single_candidate，实际 %q（filter 可能未跑）", sel.Strategy)
	}
	if sel.Skill == nil || sel.Skill.Name != "enabled_top2" {
		t.Errorf("应激活 enabled_top2，实际 %v", sel.Skill)
	}
}

// TestSelectSkillAdaptive_AllDisabled 验证全 disabled 时 strategy=none + Reason 准确。
//
// **bomb 防御（5 类炸弹·第 3 类——错误字符串不匹配）**：
// 用 strings.Contains "disable-model-invocation" 精确匹配 fix 路径专属 Reason 字符串
// （不是通用的 "no candidate"）。sanity-revert 删 enabled 检查后会进 MinScore 路径
// Reason="top1 score X < min Y"——不含 "disable" 关键词 → fail。
func TestSelectSkillAdaptive_AllDisabled(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "a", DisableModelInvocation: true}, Score: 0.9},
		{Skill: skill.SkillDescriptor{Name: "b", DisableModelInvocation: true}, Score: 0.8},
	}}
	sel, err := selectSkillAdaptive(context.Background(), "x", box, nil, false)
	if err != nil {
		t.Fatalf("selectSkillAdaptive: %v", err)
	}
	if sel.Strategy != SkillSelectionStrategyNone {
		t.Errorf("全 disabled 应 strategy=none，实际 %q", sel.Strategy)
	}
	if sel.Skill != nil {
		t.Errorf("全 disabled 不应激活，实际 %v", sel.Skill)
	}
	// 关键：Reason 必须含 "disable-model-invocation"——证明 fix 路径专属代码段执行
	if !strings.Contains(sel.Reason, "disable-model-invocation") {
		t.Errorf("Reason 应含 'disable-model-invocation' 关键词（fix 路径专属），实际 %q", sel.Reason)
	}
}

// TestSelectSkillAdaptive_PreMatchModeFiltersDisabled 验证 pre_match 模式也尊重
// DisableModelInvocation——pre_match 不调 LLM 但仍是 LLM-side execution，必须过滤。
func TestSelectSkillAdaptive_PreMatchModeFiltersDisabled(t *testing.T) {
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "disabled_top", DisableModelInvocation: true}, Score: 0.95},
		{Skill: skill.SkillDescriptor{Name: "enabled_a"}, Score: 0.85},
		{Skill: skill.SkillDescriptor{Name: "enabled_b"}, Score: 0.75},
	}}
	// pre_match 模式（isPreMatch=true）
	sel, err := selectSkillAdaptive(context.Background(), "x", box, nil, true)
	if err != nil {
		t.Fatalf("selectSkillAdaptive: %v", err)
	}
	// 过滤掉 disabled_top 后剩 enabled_a + enabled_b → pre_match 选 top1=enabled_a
	if sel.Skill == nil || sel.Skill.Name != "enabled_a" {
		t.Errorf("pre_match 应选过滤后 top1 (enabled_a)，实际 %v", sel.Skill)
	}
	if sel.Strategy != SkillSelectionStrategySkippedPreMatch {
		t.Errorf("应 strategy=skipped_pre_match，实际 %q", sel.Strategy)
	}
}

// TestSelectSkillAdaptive_DisabledFilterBeforeMinScore 验证两阶段过滤顺序：
// disable filter 先 → MinScore 后。混合 case 验证两层都生效。
func TestSelectSkillAdaptive_DisabledFilterBeforeMinScore(t *testing.T) {
	// disabled top1 (0.95) + enabled mid (0.6) + enabled low (0.2)；MinScore=0.5
	// → disable 过滤剩 enabled mid + enabled low → MinScore 过滤剩 enabled mid
	// → strategy=single_candidate + skill=enabled mid
	box := &adaptiveMockBox{matches: []skill.SkillMatchResult{
		{Skill: skill.SkillDescriptor{Name: "disabled_high", DisableModelInvocation: true}, Score: 0.95},
		{Skill: skill.SkillDescriptor{Name: "enabled_mid"}, Score: 0.6},
		{Skill: skill.SkillDescriptor{Name: "enabled_low"}, Score: 0.2},
	}}
	cfg := &SkillSelectorConfig{MinScore: 0.5}
	sel, err := selectSkillAdaptive(context.Background(), "x", box, cfg, false)
	if err != nil {
		t.Fatalf("selectSkillAdaptive: %v", err)
	}
	if sel.Strategy != SkillSelectionStrategySingleCandidate {
		t.Errorf("应 strategy=single_candidate (disable + minScore 双过滤后剩 1)，实际 %q", sel.Strategy)
	}
	if sel.Skill == nil || sel.Skill.Name != "enabled_mid" {
		t.Errorf("应激活 enabled_mid，实际 %v", sel.Skill)
	}
}
