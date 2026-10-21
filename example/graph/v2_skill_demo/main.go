// v2_skill_demo 演示 agent_v2.WithSkills + skill.DefaultSkillBox 的最小闭环：
//
// **P1 baseline（toolCall 模式默认）**：
// 1. 用 SKILL.md frontmatter 定义 skill（translate / code-review）
// 2. agent_v2.WithSkills(SkillSource{SkillBox: box}) 注入 agent
// 3. 框架在 modelNode 内部自动给 toolkit 注入内置 activate_skill 工具
// 4. mock LLM 模拟 ReAct 流，验证 WithSkills 端到端 wire-up
//
// **P2 增强场景**（4 features 各自最小例）：
//   - P2.A: `disable-model-invocation: true` 让 skill 不出现在 LLM listing
//   - P2.D: `WithInlineSkillBody(true)` skill body 走 [system_reminder] UserMessage
//   - P2.C+B: `context: fork` 让 skill spawn 独立 sub-agent（WithSkillSubAgentResolver）
//
// 跑法：
//
//	go run ./example/graph/v2_skill_demo
//
// 全 P1 + 4 P2 场景成功 + 退出码 0 表示通过。
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
)

func main() {
	if err := runDemo(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL P1 baseline: %v\n", err)
		os.Exit(1)
	}
	if err := runP2Demo(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL P2 enhancements: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n=== ALL SCENARIOS PASSED (P1 baseline + P2 A/D/C+B) ===")
}

func runDemo() error {
	// ── 1. 加载 skill 目录 ──
	// P1 baseline 用 AllowList 只加载 translate + code-review 两个（避开 P2 demo 用的 skill）
	skillsDir := skillsDir()
	box, err := skill.NewDefaultSkillBox(skillsDir,
		skill.WithSkillBoxName("demo-box"),
		skill.WithAllowList([]string{"translate", "code-review"}),
	)
	if err != nil {
		return fmt.Errorf("加载 skill box: %w", err)
	}
	skills := box.ListSkills()
	if len(skills) != 2 {
		return fmt.Errorf("expected 2 skills loaded, got %d (skillsDir=%s)", len(skills), skillsDir)
	}
	fmt.Printf("📦 已加载 skill box %q (rootDir=%s)\n", box.Name(), skillsDir)
	for _, s := range skills {
		fmt.Printf("   - %s: %s\n", s.Name, s.Description)
	}

	// ── 2. 构造 BasicAgent + WithSkills ──
	llm := newMockSkillLLM()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("skill_demo_bot"),
		agent_v2.WithSystemPrompt(
			`你是一个专家路由 bot。当用户请求需要专项技能（翻译 / code review）时，
调用 activate_skill 工具激活对应 skill，然后用激活返回的 prompt 内容回答。
不需要技能时直接答。`),
		agent_v2.WithModel(llm),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithHooks(traceHook{}),
		agent_v2.WithMaxRounds(5),
	)

	// ── 3. 跑 3 个场景 ──
	scenarios := []struct {
		name        string
		userInput   string
		expectFinal []string // final 必含的子串
		expectSkill string   // 期望被激活的 skill 名（""表示无）
	}{
		{
			name:        "scenario 1: 翻译请求 → activate translate",
			userInput:   "请翻译：你好世界",
			expectFinal: []string{"Hello"},
			expectSkill: "translate",
		},
		{
			name:        "scenario 2: code review 请求 → activate code-review",
			userInput:   "review 这段代码: func Add(a, b int) int { return a + b }",
			expectFinal: []string{"Add", "改进建议"},
			expectSkill: "code-review",
		},
		{
			name:        "scenario 3: 普通闲聊 → 无 skill 激活",
			userInput:   "今天星期几？",
			expectFinal: []string{"星期"},
			expectSkill: "",
		},
	}

	for _, sc := range scenarios {
		fmt.Println("\n┌──────────────────────────────────────────────────┐")
		fmt.Printf("│  %s\n", sc.name)
		fmt.Println("└──────────────────────────────────────────────────┘")
		fmt.Printf("👤 user> %s\n", sc.userInput)

		llm.reset() // 每场景独立计数
		sess := agent_v2.NewRunSession()
		events, err := ag.Run(context.Background(), sess, schema.UserMessage(sc.userInput))
		if err != nil {
			return fmt.Errorf("[%s] Run: %w", sc.name, err)
		}

		final := agent_v2.FinalMessage(events)
		if final == nil {
			return fmt.Errorf("[%s] no final message", sc.name)
		}
		fmt.Printf("🤖 agent> %s\n", final.Content)

		// 校验 final 文本
		for _, sub := range sc.expectFinal {
			if !strings.Contains(final.Content, sub) {
				return fmt.Errorf("[%s] final 应含 %q，实际 %q", sc.name, sub, final.Content)
			}
		}

		// 校验 skill 激活与否（看 mock LLM 内部记录）
		if sc.expectSkill != "" && llm.state.activatedSkill != sc.expectSkill {
			return fmt.Errorf("[%s] 应激活 skill %q，实际 %q", sc.name, sc.expectSkill, llm.state.activatedSkill)
		}
		if sc.expectSkill == "" && llm.state.activatedSkill != "" {
			return fmt.Errorf("[%s] 不应激活任何 skill，实际激活了 %q", sc.name, llm.state.activatedSkill)
		}

		fmt.Printf("✅ 场景通过；history size=%d；激活 skill=%q\n",
			len(sess.History()), llm.state.activatedSkill)
	}

	// ── 总结 ──
	fmt.Println("\n┌──────────────────────────────────────────────────┐")
	fmt.Println("│  WithSkills 端到端 wire-up 验证通过              │")
	fmt.Println("└──────────────────────────────────────────────────┘")
	fmt.Printf("✓ DefaultSkillBox 扫描 SKILL.md → 加载 2 个 skill\n")
	fmt.Printf("✓ WithSkills 注入 agent → 框架自动注 activate_skill 工具\n")
	fmt.Printf("✓ LLM 在 tool description 看到候选 → tool_call 激活\n")
	fmt.Printf("✓ ActivateSkill 返 prompt → LLM 第二轮用 prompt 给最终答\n")
	fmt.Printf("✓ 普通对话不强制激活——activate_skill 是工具不是劫持\n")

	return nil
}

// skillsDir 返本 demo 同目录下的 ./skills 绝对路径——`go run` 不论 cwd 都能定位。
func skillsDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "skills"
	}
	return filepath.Join(filepath.Dir(file), "skills")
}

// ── mock LLM ──
//
// mockSkillLLM 是脚本化 ChatModel——按"是否已激活 skill"二态决策：
//
//   - 第 1 轮（msgs 末尾是 user）：根据用户文本路由
//     · 含"翻译" → tool_call(activate_skill, {skill:"translate"})
//     · 含"review" → tool_call(activate_skill, {skill:"code-review"})
//     · 其他 → 直接 final 答（不激活 skill）
//
//   - 第 2 轮（msgs 末尾是 tool result，是 activate_skill 返的 prompt body）：
//     用激活的 skill 类型生成对应的 final 答（mock 只取关键字证明 prompt 被收到）
//
// 不模拟真 LLM 的语义理解——只演示 tool flow，业务接真 LLM 时 LLM 自己读 prompt 决定怎么答。
//
// **state 通过 *skillState 共享**——框架每轮调 WithTools 返新 copy，跨轮可变状态
// 必须放共享指针上才不丢；直接把 activatedSkill 放结构体字段会被 copy 截断（这是
// 写 mock 时容易踩的坑，本 demo 故意保留这个 pattern 给业务参考）。
type mockSkillLLM struct {
	state *skillState        // 跨轮共享的可变状态
	tools []*schema.ToolInfo // 框架按 toolkit 注入的工具表
}

type skillState struct {
	activatedSkill string // 本场景被激活的 skill 名（""=未激活）
}

func newMockSkillLLM() *mockSkillLLM {
	return &mockSkillLLM{state: &skillState{}}
}

func (m *mockSkillLLM) reset() {
	m.state.activatedSkill = ""
}

func (m *mockSkillLLM) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m // shallow copy → state 指针 + tools 都浅拷贝，state 跨轮共享 ✓
	cp.tools = tools
	return &cp, nil
}

func (m *mockSkillLLM) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) == 0 {
		return schema.AssistantMessage("[mock] 空输入"), nil
	}

	// 取最后一条消息分类：tool result 还是 user
	last := msgs[len(msgs)-1]

	// 第 2 轮：拿到 activate_skill 返的 prompt → 给最终答
	if last.Role == schema.RoleTool {
		// last.Content 是激活的 skill body（SKILL.md frontmatter 后的 prompt 文本）
		// 真 LLM 会读这个 prompt 然后给答案；mock 直接根据已记录的 activatedSkill 模拟
		switch m.state.activatedSkill {
		case "translate":
			// 真 LLM 会按 prompt 翻译"你好世界"→"Hello World"，mock 直接返
			return schema.AssistantMessage("Hello World"), nil
		case "code-review":
			return schema.AssistantMessage(
				"## ✅ 优点\n- Add 函数命名清晰，职责单一\n\n" +
					"## ⚠️ 问题\n- LOW: 无 doc comment\n\n" +
					"## 🔧 改进建议\n- 加上 godoc 注释",
			), nil
		default:
			return schema.AssistantMessage("[mock] 收到未识别 skill prompt: " +
				truncate(last.Content, 60)), nil
		}
	}

	// 第 1 轮：根据 user 文本路由
	if last.Role != schema.RoleUser {
		return schema.AssistantMessage("[mock] unexpected role at end: " + string(last.Role)), nil
	}
	text := last.Content

	switch {
	case strings.Contains(text, "翻译") || strings.Contains(text, "translate"):
		m.state.activatedSkill = "translate"
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:   "call_translate_1",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "activate_skill",
				Arguments: `{"skill":"translate"}`,
			},
		}}), nil

	case strings.Contains(text, "review") || strings.Contains(text, "审查"):
		m.state.activatedSkill = "code-review"
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:   "call_review_1",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "activate_skill",
				Arguments: `{"skill":"code-review"}`,
			},
		}}), nil

	default:
		// 普通闲聊：不激活 skill，直接答
		return schema.AssistantMessage("[mock] 我不知道今天星期几，但你可以查日历~"), nil
	}
}

func (m *mockSkillLLM) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var _ model.ChatModel = (*mockSkillLLM)(nil)

// ── P2 demo 场景 ──
//
// 4 场景各自配独立 BasicAgent + 独立 mock LLM——展示 4 个 P2 feature 的最小用法。
func runP2Demo() error {
	fmt.Println("\n\n╔══════════════════════════════════════════════════╗")
	fmt.Println("║  P2 增强场景演示                                  ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")

	if err := scenarioP2A(); err != nil {
		return fmt.Errorf("P2.A: %w", err)
	}
	if err := scenarioP2D(); err != nil {
		return fmt.Errorf("P2.D: %w", err)
	}
	if err := scenarioP2CB(); err != nil {
		return fmt.Errorf("P2.C+B: %w", err)
	}
	return nil
}

// scenarioP2A 演示 disable-model-invocation：
// box 含 3 个 skill (translate / code-review / legacy-disabled)，但 LLM listing 只见 2 个。
func scenarioP2A() error {
	fmt.Println("\n┌──────────────────────────────────────────────────┐")
	fmt.Println("│  P2.A: disable-model-invocation 二维 skill 控制   │")
	fmt.Println("└──────────────────────────────────────────────────┘")

	// 加载 3 个 skill（含 legacy-disabled）
	box, err := skill.NewDefaultSkillBox(skillsDir(),
		skill.WithSkillBoxName("p2a-box"),
		skill.WithAllowList([]string{"translate", "code-review", "legacy-disabled"}),
	)
	if err != nil {
		return fmt.Errorf("load box: %w", err)
	}
	all := box.ListSkills()
	fmt.Printf("📦 box.ListSkills 返 %d 个 skill (含 disabled):\n", len(all))
	for _, s := range all {
		marker := ""
		if s.DisableModelInvocation {
			marker = "  ⚠️ disable-model-invocation=true (LLM 看不到)"
		}
		fmt.Printf("   - %s%s\n", s.Name, marker)
	}

	// 用 captureToolsLLM 捕获 framework 注入的 activate_skill tool description
	llm := newCaptureToolsLLM()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("p2a_agent"),
		agent_v2.WithModel(llm),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithMaxRounds(1),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("test")); err != nil {
		return fmt.Errorf("Run: %w", err)
	}

	desc := llm.state.activateSkillDesc
	fmt.Printf("🔍 LLM 看到的 activate_skill description:\n%s\n", indent(desc, "   "))

	// 验证：legacy-disabled 不在 listing；translate / code-review 在
	if strings.Contains(desc, "legacy-disabled") {
		return fmt.Errorf("disabled skill 不应在 listing，实际 %q", desc)
	}
	if !strings.Contains(desc, "translate") || !strings.Contains(desc, "code-review") {
		return fmt.Errorf("translate / code-review 应在 listing，实际 %q", desc)
	}
	fmt.Println("✅ P2.A 验证通过：disable-model-invocation skill 不出现在 LLM 端 listing")
	return nil
}

// scenarioP2D 演示 inline body：tool_result 退化为 terse ack，body 走 [system_reminder] UserMessage。
func scenarioP2D() error {
	fmt.Println("\n┌──────────────────────────────────────────────────┐")
	fmt.Println("│  P2.D: inline body 走 [system_reminder]          │")
	fmt.Println("└──────────────────────────────────────────────────┘")

	box, err := skill.NewDefaultSkillBox(skillsDir(),
		skill.WithSkillBoxName("p2d-box"),
		skill.WithAllowList([]string{"translate"}),
	)
	if err != nil {
		return err
	}

	// 用 captureMsgsLLM 捕获 LLM 看到的 round 2 messages
	llm := newCaptureMsgsLLM([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "p2d_tc1", Type: "function",
			Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"translate"}`},
		}}),
		schema.AssistantMessage("done with inline body"),
	})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("p2d_agent"),
		agent_v2.WithModel(llm),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithInlineSkillBody(true), // ★ P2.D 启用
		agent_v2.WithMaxRounds(3),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("translate something")); err != nil {
		return err
	}

	if len(llm.state.allCalls) < 2 {
		return fmt.Errorf("应 ≥2 次 LLM 调用，实际 %d", len(llm.state.allCalls))
	}
	round2 := llm.state.allCalls[1]
	hasReminderUser := false
	hasTerseToolResult := false
	for _, m := range round2 {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "[system_reminder]") {
			hasReminderUser = true
		}
		if m.Role == schema.RoleTool && strings.Contains(m.Content, "activated") {
			hasTerseToolResult = true
		}
	}
	if !hasReminderUser {
		return fmt.Errorf("P2.D 应注 [system_reminder] UserMessage 含 body，实际未见")
	}
	if !hasTerseToolResult {
		return fmt.Errorf("P2.D tool_result 应退化为 terse ack（含 'activated'），实际未见")
	}
	fmt.Println("✅ P2.D 验证通过：tool_result terse + body 走 [system_reminder] UserMessage")
	return nil
}

// scenarioP2CB 演示 fork mode（WithSkillSubAgentResolver 提供预建 sub-agent，自带 subLLM）：
// research-fork skill (context: fork) → spawn sub-agent → 返答案给父。
func scenarioP2CB() error {
	fmt.Println("\n┌──────────────────────────────────────────────────┐")
	fmt.Println("│  P2.C+B: fork 模式 + per-skill model 切换        │")
	fmt.Println("└──────────────────────────────────────────────────┘")

	box, err := skill.NewDefaultSkillBox(skillsDir(),
		skill.WithSkillBoxName("p2cb-box"),
		skill.WithAllowList([]string{"research-fork"}),
	)
	if err != nil {
		return err
	}

	// 业务自定义 sub-agent（用 stub model）
	subLLM := newCaptureMsgsLLM([]*schema.Message{
		schema.AssistantMessage("调研结果：AI Agent 主流框架 2025 - LangGraph / crew-go / Agno"),
	})
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("research_specialist"),
		agent_v2.WithSystemPrompt("你是调研专家"),
		agent_v2.WithModel(subLLM),
		agent_v2.WithMaxRounds(2),
	)
	subResolver := func(name string) (agent_v2.Agent, error) {
		if name == "research_specialist" {
			return subAgent, nil
		}
		return nil, fmt.Errorf("unknown sub-agent: %s", name)
	}

	// 父 LLM：r1 调 fork skill，r2 final
	parentLLM := newCaptureMsgsLLM([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "p2cb_tc1", Type: "function",
			Function: schema.FunctionCall{
				Name:      "activate_skill",
				Arguments: `{"skill":"research-fork","query":"AI Agent 框架现状"}`,
			},
		}}),
		schema.AssistantMessage("综合 sub-agent 结果生成报告"),
	})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("p2cb_parent"),
		agent_v2.WithModel(parentLLM),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(subResolver), // ★ P2.C
		agent_v2.WithMaxRounds(3),
	)

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("帮我调研 AI Agent"))
	if err != nil {
		return err
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		return fmt.Errorf("no final msg")
	}
	fmt.Printf("🤖 父 agent final: %s\n", final.Content)

	// 验证 sub-agent 真被调（看 subLLM.state.allCalls）
	if len(subLLM.state.allCalls) < 1 {
		return fmt.Errorf("sub-agent 应被 fork 调，实际 LLM 调 0 次")
	}
	subCallMsgs := concatRoles(subLLM.state.allCalls[0])
	if !strings.Contains(subCallMsgs, "AI Agent 框架现状") {
		return fmt.Errorf("sub-agent 应收到 query 'AI Agent 框架现状'，实际 %q", subCallMsgs)
	}

	// 验证父 round 2 tool_result 含 sub-agent answer
	if len(parentLLM.state.allCalls) < 2 {
		return fmt.Errorf("父 LLM 应 ≥2 次调用，实际 %d", len(parentLLM.state.allCalls))
	}
	parentRound2 := concatRoles(parentLLM.state.allCalls[1])
	if !strings.Contains(parentRound2, "LangGraph / crew-go / Agno") {
		return fmt.Errorf("父 round 2 应见 sub-agent answer，实际 %q", parentRound2)
	}
	fmt.Println("✅ P2.C+B 验证通过：fork sub-agent 跑 query → 返答案给父")
	return nil
}

// ── P2 mock helpers ──

// captureToolsLLM 捕获 framework 注入的 activate_skill 工具 description——
// 用于 P2.A 验证 LLM 看到的 listing 不含 disabled skill。
//
// **shared state 指针**：WithTools 框架按 toolkit 注入返新 copy；跨 copy 共享
// 必须放 *captureToolsState 上才不丢（同 mockSkillLLM pattern）。
type captureToolsLLM struct {
	state *captureToolsState
}

type captureToolsState struct {
	activateSkillDesc string
}

func newCaptureToolsLLM() *captureToolsLLM {
	return &captureToolsLLM{state: &captureToolsState{}}
}

func (m *captureToolsLLM) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("[noop]"), nil
}
func (m *captureToolsLLM) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *captureToolsLLM) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m // shallow copy，state 指针共享
	for _, t := range tools {
		if t != nil && t.Name == "activate_skill" {
			cp.state.activateSkillDesc = t.Desc
		}
	}
	return &cp, nil
}

var _ model.ChatModel = (*captureToolsLLM)(nil)

// captureMsgsLLM 捕获每次 LLM 调用收到的完整 messages——
// 用于 P2.D / P2.C+B 验证 conversation flow。
//
// **shared state 指针**：同 captureToolsLLM——WithTools 返新 copy 必须共享 state。
type captureMsgsLLM struct {
	responses []*schema.Message
	state     *captureMsgsState
}

type captureMsgsState struct {
	calls    int
	allCalls [][]*schema.Message
}

func newCaptureMsgsLLM(responses []*schema.Message) *captureMsgsLLM {
	return &captureMsgsLLM{
		responses: responses,
		state:     &captureMsgsState{},
	}
}

func (m *captureMsgsLLM) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	cloned := make([]*schema.Message, 0, len(msgs))
	for _, x := range msgs {
		if x == nil {
			cloned = append(cloned, nil)
			continue
		}
		c := *x
		cloned = append(cloned, &c)
	}
	m.state.allCalls = append(m.state.allCalls, cloned)
	if m.state.calls < len(m.responses) {
		out := m.responses[m.state.calls]
		m.state.calls++
		return out, nil
	}
	m.state.calls++
	return schema.AssistantMessage("[exhausted]"), nil
}

func (m *captureMsgsLLM) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *captureMsgsLLM) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m // state 指针共享
	return &cp, nil
}

var _ model.ChatModel = (*captureMsgsLLM)(nil)

// concatRoles 把 messages 序列化为可读字符串便于断言。
func concatRoles(msgs []*schema.Message) string {
	var sb strings.Builder
	for _, m := range msgs {
		if m == nil {
			continue
		}
		sb.WriteString(string(m.Role))
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteString("\n")
	}
	return sb.String()
}

// indent 给多行字符串每行加前缀（demo 输出美化用）。
func indent(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n")
}

// ── trace hook ──
//
// traceHook 输出 ReAct 每一步的关键信号——便于直观看到：
//   - LLM 第几轮被调用、看到几条消息（in.Tools 框架恒 nil，不打印）
//   - 哪个工具被 call、参数和返回是什么
type traceHook struct{ hook.NoOpHooks }

func (traceHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	fmt.Printf("  [llm→] agent=%s round=%d msgs=%d\n",
		info.Name, info.Round, len(in.Messages))
	return ctx, nil
}

func (traceHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Message == nil {
		return ctx, nil
	}
	tcs := schema.GetToolCalls(out.Message)
	if len(tcs) > 0 {
		fmt.Printf("  [llm←] tool_call x%d → %s(%s)\n",
			len(tcs), tcs[0].Function.Name, tcs[0].Function.Arguments)
	} else {
		fmt.Printf("  [llm←] final: %s\n", truncate(out.Message.Content, 80))
	}
	return ctx, nil
}

func (traceHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	fmt.Printf("  [tool→] %s args=%s\n", in.Name, in.ArgumentsJSON)
	return ctx, nil
}

func (traceHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out.Result == nil {
		return ctx, nil
	}
	if out.Result.Success {
		fmt.Printf("  [tool←] ok: %s\n", truncate(out.Result.Output, 100))
	} else {
		fmt.Printf("  [tool←] fail(%s): %s\n", out.Result.ErrorCode, out.Result.Error)
	}
	return ctx, nil
}
