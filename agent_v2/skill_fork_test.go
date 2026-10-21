package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
)

// forkBox 是支持 SkillDescriptor 含 Context/Agent/Model 字段的 fakeSkillBox。
//
// **关键测试假设**（audit C LOW-2 文档化）：`MatchSkills` 永远返 nil → 测试故意
// force toolCall 默认模式跑 fork 路径,**不**触发 adaptive / pre_match 路径。
//
// 若 BasicAgent 未来默认 ActivationMode 改为 adaptive,使用 forkBox 的测试会
// 静默不触发 fork → 假绿。届时需要加 ActivationMode 显式配置或扩 MatchSkills
// 返非 nil match 结果。
type forkBox struct {
	skills      []skill.SkillDescriptor
	activations map[string]string
}

func (b *forkBox) Name() string                                                     { return "fork" }
func (b *forkBox) RootDir() string                                                  { return "" }
func (b *forkBox) ListSkills() []skill.SkillDescriptor                              { return b.skills }
func (b *forkBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult { return nil }
func (b *forkBox) ListSkillResources(_ string) []skill.SkillResource                { return nil }
func (b *forkBox) ReadSkillResource(_, _ string) string                             { return "" }
func (b *forkBox) ReadSkillPrompt(name string) string                               { return b.activations[name] }
func (b *forkBox) ListSkillActions(_ string) []skill.SkillActionDescriptor          { return nil }
func (b *forkBox) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	for _, s := range b.skills {
		if s.Name == name {
			return &skill.ActiveSkill{Skill: s, Prompt: b.activations[name]}, nil
		}
	}
	return nil, errors.New("skill not found")
}
func (b *forkBox) RunSkillAction(_ context.Context, _, _ string, _ map[string]any) (*skill.SkillExecutionResult, error) {
	return nil, skill.ErrSkillActionNotFound
}

var _ skill.SkillBox = (*forkBox)(nil)

// TestForkMode_DefaultBasicAgentFallback 验证 skill 标 context=fork 但 Agent 未指定时，
// 框架默认创建 BasicAgent（用 skill body 作 sysPrompt + parent model 兜底）。
//
// **bomb 防御（5 类炸弹·第 5 类——mock 不读 input）**：
// 用 forkSubModelStub 捕获子 agent LLM 看到的 messages——必须含 skill body
// （证明 sysPrompt 注入生效）+ query 字符串（证明 query 透传到子 agent）。
func TestForkMode_DefaultBasicAgentFallback(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork"},
		},
		activations: map[string]string{"research": "你是调研专家，请调研给定话题。"},
	}
	// 调用顺序（共享 stub）：
	//   1. parent r1: 返 activate_skill tool_call → responses[0]
	//   2. sub-agent r1: 返 final answer → responses[1]
	//   3. parent r2: 返 综合 final → responses[2]
	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research","query":"AI 趋势"}`,
		},
	}})
	subFinal := schema.AssistantMessage("AI 趋势：LLM + Agent + Multi-modal")
	parentR2 := schema.AssistantMessage("综合调研结果")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, subFinal, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("帮我调研 AI 趋势")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证：sub-agent LLM 至少被调一次，看到 skill body 作为 sysPrompt + query 作为 user input
	if len(parentStub.allCalls) < 3 {
		t.Fatalf("应至少 3 次 LLM 调用（parent r1 + sub-agent r1 + parent r2），实际 %d", len(parentStub.allCalls))
	}
	subCallMsgs := concatContent(parentStub.allCalls[1])
	if !strings.Contains(subCallMsgs, "你是调研专家") {
		t.Errorf("默认 fork sub-agent 应看到 skill body 作 sysPrompt，实际 msgs：%q", subCallMsgs)
	}
	if !strings.Contains(subCallMsgs, "AI 趋势") {
		t.Errorf("默认 fork sub-agent 应收到 query 作 user input，实际 msgs：%q", subCallMsgs)
	}

	// 验证：parent 第二轮收到 sub-agent final answer 作 tool_result
	parentR2Msgs := concatContent(parentStub.allCalls[2])
	if !strings.Contains(parentR2Msgs, "AI 趋势：LLM + Agent + Multi-modal") {
		t.Errorf("parent 第二轮应在 tool_result 内见 sub-agent final answer，实际：%q", parentR2Msgs)
	}
}

// TestForkMode_QueryRequired 验证 fork skill 调用时 LLM 必传 query 参数——
// 缺 query 返 ErrCodeInvalidArguments 业务级失败让 LLM 重试。
//
// **bomb 防御（5 类炸弹·第 3 类——error 字符串不匹配）**：
// 用 fix 路径专属字符串"requires 'query' argument"断言。sanity-revert 删 query
// 检查 → fork mode 进入但因 model 兜底缺失也会 fail，不含 'query' 关键词 → fail。
func TestForkMode_QueryRequired(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork"},
		},
		activations: map[string]string{"research": "你是调研专家。"},
	}
	// LLM 调 fork skill 但忘了传 query
	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("done")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("调研")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// parent 第二轮 tool_result content 应含 "requires 'query' argument"
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(parentStub.allCalls))
	}
	round2 := concatContent(parentStub.allCalls[1])
	if !strings.Contains(round2, "requires 'query' argument") {
		t.Errorf("fork 缺 query 应返业务级 'requires 'query' argument' 错误，实际：%q", round2)
	}
}

// TestForkMode_SubAgentResolverUsed 验证业务通过 WithSkillSubAgentResolver 注册的
// sub-agent 被实际调用（不走默认 BasicAgent 兜底路径）。
//
// **bomb 防御（5 类炸弹·第 1 类——过滤后元素恰好原位）**：
// resolverInvoked 计数器 + sub-agent 返"resolver_marker"独特字符串——sanity-revert
// 删 resolver 检查 → 走默认 BasicAgent 路径 → resolverInvoked=0 + final 不含 marker → fail。
func TestForkMode_SubAgentResolverUsed(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork", Agent: "biz_research_agent"},
		},
		activations: map[string]string{"research": "skill body"},
	}

	// 业务自定义 sub-agent，返独特字符串
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("biz_research_agent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("[resolver_marker] biz custom answer"),
		})),
	)

	resolverCalls := 0
	resolver := func(name string) (agent_v2.Agent, error) {
		resolverCalls++
		if name == "biz_research_agent" {
			return subAgent, nil
		}
		return nil, errors.New("unknown agent: " + name)
	}

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research","query":"调研子任务"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("综合")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("调研")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if resolverCalls != 1 {
		t.Errorf("SkillSubAgentResolver 应被调 1 次，实际 %d", resolverCalls)
	}

	// parent 第二轮 tool_result 应含业务 sub-agent 返的 resolver_marker
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(parentStub.allCalls))
	}
	round2 := concatContent(parentStub.allCalls[1])
	if !strings.Contains(round2, "[resolver_marker]") {
		t.Errorf("应见业务 sub-agent 返的 resolver_marker，实际：%q", round2)
	}
}

// TestForkMode_PerSkillModelResolved 验证 P2.B per-skill model：skill.Model 非空 +
// modelResolver 配了时，默认 BasicAgent 用解析后的 model 跑（不用 parent model）。
//
// **bomb 防御**：用独立 perSkillModelStub vs parentStub 区分——sub-agent 调的应是
// perSkillModelStub.Generate。sanity-revert 删 modelResolver 检查 → sub-agent 用
// parentStub → perSkillModelCalls=0 → fail。
func TestForkMode_PerSkillModelResolved(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "翻译", Context: "fork", Model: "haiku"},
		},
		activations: map[string]string{"translate": "你是翻译专家。"},
	}

	// 独立 model 实例——sub-agent 应调它（不调 parentStub）
	perSkillStub := newStubModel([]*schema.Message{
		schema.AssistantMessage("[per_skill_model] translated"),
	})

	resolverCalls := 0
	modelResolver := func(name string) (model.ChatModel, error) {
		resolverCalls++
		if name == "haiku" {
			return perSkillStub, nil
		}
		return nil, errors.New("unknown model: " + name)
	}

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"translate","query":"hello world"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("done")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillModelResolver(modelResolver),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("translate")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if resolverCalls != 1 {
		t.Errorf("SkillModelResolver 应被调 1 次（fork 默认 BasicAgent 路径解析 skill.Model='haiku'），实际 %d", resolverCalls)
	}

	// parent 第二轮应见 per-skill model stub 的输出
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(parentStub.allCalls))
	}
	round2 := concatContent(parentStub.allCalls[1])
	if !strings.Contains(round2, "[per_skill_model]") {
		t.Errorf("应见 per-skill model 返的 [per_skill_model] 标记（证明用 perSkillStub 不用 parentStub）：%q", round2)
	}
}

// TestForkMode_InlineDefaultUnchanged 验证 skill 未声明 context（默认 inline）时
// 走 P1.A 老路径——返 active.Prompt 作 tool_result content 不 spawn sub-agent。
func TestForkMode_InlineDefaultUnchanged(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "翻译"}, // Context 未声明 → 默认 inline
		},
		activations: map[string]string{"translate": "你是翻译专家。"},
	}
	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"translate"}`, // 不传 query，inline 模式不需要
		},
	}})
	parentR2 := schema.AssistantMessage("done")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("翻译")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(parentStub.allCalls))
	}
	round2 := concatContent(parentStub.allCalls[1])
	// inline 模式 tool_result 直接含 skill body
	if !strings.Contains(round2, "你是翻译专家") {
		t.Errorf("inline mode tool_result 应含 skill body 全文，实际：%q", round2)
	}
}

// TestForkMode_UnknownContextRejected 验证无效 context 值（如 "weird_mode"）
// 返 SkillForkErrCode 让 LLM 改用 inline。
//
// **bomb 防御（5 类炸弹·第 4 类——双层防护遮蔽）**：
// 用 SkillForkErrCode 专属字符串断言。sanity-revert 删 default switch 分支让 fork
// 路径处理未知 mode → 子 agent 跑空 sysPrompt → 不返 SkillForkErrCode → fail。
func TestForkMode_UnknownContextRejected(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "weird", Description: "奇怪 skill", Context: "weird_mode"},
		},
		activations: map[string]string{"weird": "x"},
	}
	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"weird"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("done")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("x")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(parentStub.allCalls))
	}
	round2 := concatContent(parentStub.allCalls[1])
	if !strings.Contains(round2, agent_v2.SkillForkErrCode) {
		t.Errorf("未知 context 应返 SkillForkErrCode (%q)，实际：%q", agent_v2.SkillForkErrCode, round2)
	}
}

// interruptModelStub 是返 InterruptError 的 ChatModel——用于 R14 HIGH-2 回归测试。
type interruptModelStub struct{}

func (m *interruptModelStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "fork_sub_interrupt",
		Query:  &compose_v2.AskQuery{InterruptID: "fsi1", Question: "fork sub asks user"},
	}
}
func (m *interruptModelStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *interruptModelStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*interruptModelStub)(nil)

// TestForkMode_R14_HIGH2_InterruptErrorPropagates 是 R14 audit HIGH-2 回归测试——
// 验证 fork sub-agent 抛 InterruptError 时父 Run 应 propagate 而非 wrap 成 tool error。
//
// **Bug 历史**：原 runForkMode 把 sub-agent 的 InterruptError 一律 wrap 成
// SkillForkErrCode tool.Failure 返 LLM——破坏 HITL 控制流（框架不存 cp / parent
// 不挂起 / Resume 找不到入口）。修法：先判 IsInterrupt + ctx.Err()，是控制流必
// propagate；否则才 wrap 成业务级 SkillForkErrCode。
//
// **bomb 防御**：用 compose_v2.IsInterrupt(err) 显式断言——sanity-revert 删
// propagate 路径 → 父 Run 返 nil err（被 wrap）→ IsInterrupt 检查 fail。
func TestForkMode_R14_HIGH2_InterruptErrorPropagates(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "ask_skill", Description: "ask user", Context: "fork", Agent: "ask_agent"},
		},
		activations: map[string]string{"ask_skill": "ask user something"},
	}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_agent"),
		agent_v2.WithModel(&interruptModelStub{}),
	)
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "ask_agent" {
			return subAgent, nil
		}
		return nil, errors.New("unknown")
	}

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"ask_skill","query":"q"}`,
		},
	}})
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
	)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("p"))
	if err == nil {
		t.Fatalf("R14 HIGH-2 回归：fork sub-agent InterruptError 应 propagate 给父 Run，实际 nil（被 wrap 进 tool result）")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("R14 HIGH-2 回归：父 Run 应返 InterruptError，实际 %v (%T)", err, err)
	}
}

// TestForkMode_R14_MED1_BodyMaxCharsTruncation 是 R14 audit MED-1 回归测试——
// 验证 fork 默认 BasicAgent 路径的 active.Prompt 受 SkillBodyMaxChars 截断。
//
// **bomb 防御**：用极大 SKILL.md body（远超默认 20K）+ 业务设小 SkillBodyMaxChars
// 让截断必走；断言 sub-agent LLM 看到的 sysPrompt 长度 ≤ 上限。sanity-revert 删
// 截断分支 → 长度 == 原 body 长度 → fail。
func TestForkMode_R14_MED1_BodyMaxCharsTruncation(t *testing.T) {
	hugeBody := strings.Repeat("a", 5000) // 5K char body
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "huge_skill", Description: "huge", Context: "fork"},
		},
		activations: map[string]string{"huge_skill": hugeBody},
	}

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"huge_skill","query":"do something"}`,
		},
	}})
	subFinal := schema.AssistantMessage("done")
	parentR2 := schema.AssistantMessage("综合")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, subFinal, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillBodyMaxChars(500), // 业务设小上限——远小于 5000 char body
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应至少 2 次 LLM 调用（parent + sub-agent），实际 %d", len(parentStub.allCalls))
	}
	// 找 sub-agent LLM 调用的 system message——应是被截断的版本（含 "…"）
	subCallMsgs := parentStub.allCalls[1]
	var sysMsg *schema.Message
	for _, m := range subCallMsgs {
		if m != nil && m.Role == schema.RoleSystem {
			sysMsg = m
			break
		}
	}
	if sysMsg == nil {
		t.Fatalf("sub-agent 应见 system message，实际 msgs：%q", concatContent(subCallMsgs))
	}
	if len(sysMsg.Content) > 500 {
		t.Errorf("R14 MED-1 回归：fork 默认 BasicAgent sysPrompt 应被 SkillBodyMaxChars=500 截断，"+
			"实际 sysPrompt 长度 %d > 500", len(sysMsg.Content))
	}
	if !strings.HasSuffix(sysMsg.Content, "…") {
		t.Errorf("R14 MED-1 回归：截断应带 ellipsis '…'，实际 sysPrompt 末尾：%q",
			sysMsg.Content[max(0, len(sysMsg.Content)-20):])
	}
}

// max helper（Go 1.21+ 内置但保兼容）
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestForkMode_R21_HIGH1_PrefixSubstringNotRecursive 是 R21 audit HIGH-1 回归——
// 验证前缀同名 skill (`r` vs `research`) 不被误判 recursive。
//
// **Bug 历史**：R19 fix 用 strings.Contains(parentNS, subAgentNSName) 检 recursive——
// 业务两个 skill 名前缀相同时（"r" vs "research"）误判：parentNS="__skillfork_research"
// 含 "__skillfork_r" 子串 → 拒绝。R21 修：改 SplitNS segment match。
//
// **bomb 防御**：fixture 配 2 fork skill r + research，sub-agent (research_agent) 内
// 调 fork "r"。修后正常完成；sanity-revert 改回 strings.Contains → sub round 2
// tool_result 含 "recursive fork detected" → fail。
func TestForkMode_R21_HIGH1_PrefixSubstringNotRecursive(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "r", Description: "short", Context: "fork", Agent: "r_agent"},
			{Name: "research", Description: "long", Context: "fork", Agent: "research_agent"},
		},
		activations: map[string]string{"r": "you r", "research": "you research"},
	}

	rSubAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("r_agent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("r done")})),
	)
	researchSubLLM := &captureMsgsStub{
		responses: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "research_tc", Type: "function",
				Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"r","query":"sub"}`},
			}}),
			schema.AssistantMessage("research done"),
		},
	}

	resolver := func(name string) (agent_v2.Agent, error) {
		switch name {
		case "r_agent":
			return rSubAgent, nil
		case "research_agent":
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("research_agent"),
				agent_v2.WithModel(researchSubLLM),
				agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
				agent_v2.WithSkillSubAgentResolver(func(n string) (agent_v2.Agent, error) {
					if n == "r_agent" {
						return rSubAgent, nil
					}
					return nil, errors.New("unknown")
				}),
				agent_v2.WithMaxRounds(3),
			), nil
		}
		return nil, errors.New("unknown")
	}

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "p_tc1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"research","query":"main"}`},
	}})
	parentR2 := schema.AssistantMessage("parent final")
	parentStub := newStubModel([]*schema.Message{parentR1, parentR2})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithMaxRounds(3),
	)

	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("research"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(researchSubLLM.allCalls) < 2 {
		t.Fatalf("research sub-agent LLM 应被调 ≥2 次, 实际 %d", len(researchSubLLM.allCalls))
	}
	round2 := concatContent(researchSubLLM.allCalls[1])
	if strings.Contains(round2, "recursive fork detected") {
		t.Errorf("R21 HIGH-1 回归：'r' 不是 'research' 的递归调用（segment match 应区分），"+
			"实际见 'recursive fork detected'：%s", round2)
	}
}

// TestForkMode_R19_MED1_RecursiveForkRejected 是 R19 audit MED-1 回归——
// 验证 sub-agent 又调同 fork skill 时 runForkMode 拒绝（防 recursive fork 无限嵌套）。
//
// **bomb 防御**：fixture 配 skill A 的 SubAgentResolver 返一个**也带 WithSkills + 同 box**
// 的 sub-agent；sub LLM 调 activate_skill("A") → fork → runForkMode 检 parentNS
// 已含 "skill_A" → 返 SkillForkErrCode。sanity-revert 删检查 → 真死循环 → maxRounds
// 兜底但 cper.ListPending 会含 N 层 cp leak。
func TestForkMode_R19_MED1_RecursiveForkRejected(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "recur", Description: "recursive", Context: "fork", Agent: "recur_specialist"},
		},
		activations: map[string]string{"recur": "you recurse"},
	}

	// sub-agent 也调同 skill "recur"——LLM 第一次调 activate_skill("recur",...)
	subParentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "sub_tc1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"recur","query":"sub"}`},
	}})
	subParentR2 := schema.AssistantMessage("sub final")
	subStub := &captureMsgsStub{responses: []*schema.Message{subParentR1, subParentR2}}

	// sub-agent 自指 (WithSkills 同 box + WithSkillSubAgentResolver 让 sub 也能 fork)
	var subAgent agent_v2.Agent
	subResolver := func(name string) (agent_v2.Agent, error) {
		if name == "recur_specialist" {
			return subAgent, nil
		}
		return nil, errors.New("unknown")
	}
	subAgent = agent_v2.NewBasicAgent(
		agent_v2.WithName("recur_specialist"),
		agent_v2.WithModel(subStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(subResolver),
		agent_v2.WithMaxRounds(5),
	)

	// 父 LLM 调 fork skill recur
	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "p_tc1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"recur","query":"start"}`},
	}})
	parentR2 := schema.AssistantMessage("parent final")
	parentStub := newStubModel([]*schema.Message{parentR1, parentR2})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(subResolver),
		agent_v2.WithMaxRounds(3),
	)

	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("trigger recur"))
	// Run 应正常完成（不 panic / 不死循环）——sub 内的 recursive call 被 runForkMode 拒
	if err != nil {
		t.Fatalf("Run 应正常完成（recursive 在 sub 层被拒），实际 %v", err)
	}
	// 父收到 tool_result 后给 final，sub 也收到 tool_result 含 SkillForkErrCode 关键词
	// （sub 第二次 LLM 调用时看到的 tool_result）
	if len(subStub.allCalls) < 2 {
		t.Fatalf("sub LLM 应被调 2 次（r1 调 recur + r2 看 tool_result）, 实际 %d", len(subStub.allCalls))
	}
	subRound2 := concatContent(subStub.allCalls[1])
	if !strings.Contains(subRound2, "recursive fork detected") {
		t.Errorf("R19 MED-1 回归：recursive call 应被 runForkMode 拒返 'recursive fork detected'，"+
			"实际 sub round 2 tool_result：%q", subRound2)
	}
}

// captureGraphNSStub 是验证 R15-HIGH-1 用的 ChatModel——在 Generate 时捕获 ctx 中的 graphNS。
type captureGraphNSStub struct {
	capturedNS string
	response   *schema.Message
}

func (m *captureGraphNSStub) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.capturedNS = compose_v2.GetGraphNS(ctx)
	if m.response != nil {
		return m.response, nil
	}
	return schema.AssistantMessage("ok"), nil
}
func (m *captureGraphNSStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *captureGraphNSStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*captureGraphNSStub)(nil)

// TestForkMode_R15_HIGH1_SubAgentNSIsolation 是 R15 audit HIGH-1 回归测试——
// 验证 fork sub-agent 收到的 ctx 含 child graphNS（不是 parent ns）。
//
// **Bug 历史**：原 runForkMode 直接 subAgent.Run(ctx, ...) 没 push graphNS——
// cper 启用时 sub-agent cp 写到 (parent sid, parent ns) 与父 cp 冲突
// ErrGraphIDConflict / DeleteSubtree 误清父 cp。修法对照 agent_tool.go:51-53
// 标准协议：ValidateNodeIDForNS + JoinNS + WithGraphNS 三件套。
//
// **bomb 防御**：用 captureGraphNSStub 捕获 sub-agent LLM 收到的 graphNS——
// sanity-revert 删 ns push → capturedNS == "" (parent ns 默认 root) → fail。
func TestForkMode_R15_HIGH1_SubAgentNSIsolation(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork", Agent: "research_specialist"},
		},
		activations: map[string]string{"research": "你是调研专家"},
	}
	subStub := &captureGraphNSStub{response: schema.AssistantMessage("done")}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("research_specialist"),
		agent_v2.WithModel(subStub),
	)
	resolver := func(_ string) (agent_v2.Agent, error) { return subAgent, nil }

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research","query":"AI"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("综合")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("调研")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 关键断言：sub-agent LLM 收到的 graphNS 应是 "__skillfork_research"（child ns），
	// 不是空字符串（parent root ns）
	if subStub.capturedNS == "" {
		t.Errorf("R15 HIGH-1 回归：fork sub-agent 应收到 child graphNS，实际 capturedNS=\"\" " +
			"（parent root，cper 启用时父子 cp 串台）")
	}
	if !strings.Contains(subStub.capturedNS, "__skillfork_research") {
		t.Errorf("R15 HIGH-1 回归：fork sub-agent 的 graphNS 应含 '__skillfork_research'，"+
			"实际 capturedNS=%q", subStub.capturedNS)
	}
}

// TestForkMode_R15_HIGH2_NoP2DWrappingForForkAnswer 是 R15 audit HIGH-2 回归测试——
// 验证 fork final answer **不被** P2.D InlineSkillBody 协议套 [system_reminder] 包装。
//
// **Bug 历史**：原 makeToolsFn 仅检 isFrameworkActivateSkill && cfg.InlineSkillBody
// 触发 P2.D——fork 模式 res.Output 是 sub-agent final answer 不是 skill body，被
// 误套 [system_reminder] Skill "X" content 包装 → LLM 困惑 + AutoCompact 错误整组保留。
// 修法：runForkMode 在 res.Metadata 标 SkillForkExecutedMetaKey=true，makeToolsFn
// 检到此 flag 跳过 P2.D。
//
// **bomb 防御**：fork 模式启用 + P2.D 启用，断言 LLM 看到的 round 2 messages
// **不含** "[system_reminder] Skill" 前缀（fork 答案应直接进 tool_result）。
// sanity-revert 删 isForkExecuted 检查 → P2.D 触发 → LLM 看到 [system_reminder] Skill → fail。
func TestForkMode_R15_HIGH2_NoP2DWrappingForForkAnswer(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork", Agent: "research_specialist"},
		},
		activations: map[string]string{"research": "你是调研专家"},
	}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("research_specialist"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("[fork_unique_marker] sub-agent answer text"),
		})),
	)
	resolver := func(_ string) (agent_v2.Agent, error) { return subAgent, nil }

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research","query":"AI"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("综合")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithInlineSkillBody(true), // P2.D 同时启用
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("调研")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("应 2 次 LLM 调用，实际 %d", len(parentStub.allCalls))
	}
	round2 := concatContent(parentStub.allCalls[1])
	// 关键 1：fork 答案应直接进 tool_result（不被 P2.D 套 [system_reminder] 包装）
	if strings.Contains(round2, "[system_reminder] Skill") {
		t.Errorf("R15 HIGH-2 回归：fork 模式不应触发 P2.D inline body 包装"+
			"（fork final answer 不是 skill body），实际见 [system_reminder] Skill 包装：%q", round2)
	}
	// 关键 2：tool_result 含 fork 独特标记（fork 答案直接到 tool_result）
	if !strings.Contains(round2, "[fork_unique_marker]") {
		t.Errorf("R15 HIGH-2 回归：fork 答案应在 tool_result 内，实际：%q", round2)
	}
}

// TestForkMode_R30D_LOW1_SplitNSErrPropagates 是 R30D LOW-1 回归——
// 验证 skill_tool::runForkMode 内 SplitNS err 时 propagate 为 SkillForkErrCode
// tool result + slog.Warn 业务可观测，不再 silent 跳过递归保护降级。
//
// **Bug 历史**：原 `parentSegs, _ := compose_v2.SplitNS(parentNS)` 静默吞 err
// → segs=nil → 递归 fork 检测 + maxSkillForkDepth 检测全失效。NS 异常等价"递归
// 保护失效"是不安全默认——业务上游 NS 污染（如 R30D MED-1 描述场景）或自定义编排
// push 不规范 ns 时漏报。
//
// **修法**：SplitNS err 时 propagate 为 SkillForkErrCode + slog.Warn。
func TestForkMode_R30D_LOW1_SplitNSErrPropagates(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork"},
		},
		activations: map[string]string{"research": "你是调研专家。"},
	}
	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research","query":"do it"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("done")
	parentStub := &captureMsgsStub{responses: []*schema.Message{parentR1, parentR2}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
	)

	// 父 ctx 已含 broken NS（含 leading "/"，触发 SplitNS err: "leading/trailing"）
	// 业务自定义编排或测试 mock 上游 push 不规范 NS 时此场景触发
	brokenCtx := compose_v2.WithGraphNS(context.Background(), "/broken_ns")

	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(brokenCtx, sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证：parent 第 2 轮 LLM 应收到含 "fork: parent ns" 错误的 tool_result
	if len(parentStub.allCalls) < 2 {
		t.Fatalf("parent LLM 应被调 ≥2 次（r1 fork + r2 final），实际 %d", len(parentStub.allCalls))
	}
	r2Content := concatContent(parentStub.allCalls[1])
	if !strings.Contains(r2Content, "fork: parent ns") {
		t.Errorf("R30D LOW-1 回归：fork 工具应返 SkillForkErrCode 含 'fork: parent ns invalid'\n"+
			"实际 r2 messages 不含该字符串：%q\n"+
			"sanity-revert 把 SplitNS err propagate 改回 silent 吞掉 → 测试 fail（fork 沉默通过+空 segs，不报 invalid NS 错）",
			r2Content)
	}
	if !strings.Contains(r2Content, "invalid") {
		t.Errorf("R30D LOW-1 回归：tool result 应含 'invalid' 关键词；实际 %q", r2Content)
	}
}
