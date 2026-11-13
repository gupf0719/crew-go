package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// makeCompactInput 构造多轮对话，长答复使消息总量超过测试配置的阈值。
func makeCompactInput(n int) []*schema.Message {
	msgs := make([]*schema.Message, 0, n*2)
	for i := 0; i < n; i++ {
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("user msg %d", i)))
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("assistant resp %d %s", i, strings.Repeat("payload data ", 500))))
	}
	return msgs
}

// applyWithAutoCompact 为独立调用 AutoCompactRun 装配带断路器表的配置。
func applyWithAutoCompact(cfg *agent_v2.AutoCompactConfig) *agent_v2.AutoCompactConfig {
	tmpCfg := &agent_v2.Config{}
	agent_v2.WithAutoCompact(cfg)(tmpCfg)
	return tmpCfg.AutoCompact
}

// alwaysFailModel 业务 ChatModel——Generate 总是返 prompt-too-long error
// 让 ReactiveCompact retry 触发。原定义在 PR 3.3 删的 Memory 测试文件之一，
// 迁来此文件让 compaction_test.go 跨 ReactiveCompact 多 test 共享。
type alwaysFailModel struct {
	calls atomic.Int64
}

func (m *alwaysFailModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	// 通用 model error 让 AutoCompact summarize RecordFailure 走 breaker 计数
	// （非 prompt-too-long 避免命中 ReactiveCompact retry 路径）
	return nil, errors.New("simulated summarize llm failure")
}

func (m *alwaysFailModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *alwaysFailModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// ── MicroCompact 触发验证 ──

// TestMicroCompact_TriggersOnExcessToolResults 验证：白名单 tool 的旧 tool_result
// 在超过 KeepRecent 时被清成占位符（保结构）。
func TestMicroCompact_TriggersOnExcessToolResults(t *testing.T) {
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:    true,
		KeepRecent: 2,
		// 用默认白名单 ["Read","Bash","Grep","Glob","WebSearch","WebFetch","Edit","Write"]
	}

	// 5 次 Read tool_call + 对应 tool_result
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read", Arguments: `{"path":"/tmp/x"}`}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "原始大文件内容 row 1\nrow 2\n... 很长"))
	}

	out := agent_v2.MicroCompactRun(context.Background(), msgs, cfg)

	// 应只保留最近 2 次 tool_result 原文，前 3 次被替换为占位符
	clearedCount := 0
	for _, m := range out {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool && strings.Contains(m.Content, "cleared by microcompact") {
			clearedCount++
		}
	}
	if clearedCount != 3 {
		t.Fatalf("expected 3 tool_result cleared, got %d", clearedCount)
	}
}

// TestMicroCompact_NoTriggerForNonWhitelist 验证非白名单 tool 不被清。
func TestMicroCompact_NoTriggerForNonWhitelist(t *testing.T) {
	cfg := &agent_v2.MicroCompactConfig{Enabled: true, KeepRecent: 2}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "ask_user", Arguments: `{}`}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "user said yes"))
	}
	out := agent_v2.MicroCompactRun(context.Background(), msgs, cfg)
	for _, m := range out {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool && strings.Contains(m.Content, "cleared") {
			t.Fatalf("ask_user tool_result should NOT be cleared (not in whitelist), got: %q", m.Content)
		}
	}
}

// ── AutoCompact 触发验证 ──

// summaryStubModel 实现 model.ChatModel——AutoCompact 调它生成摘要时返预设字符串。
type summaryStubModel struct {
	summary string
	called  int
}

func (m *summaryStubModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.called++
	return schema.AssistantMessage(m.summary), nil
}
func (m *summaryStubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *summaryStubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*summaryStubModel)(nil)

// TestAutoCompact_TriggersOnLowThreshold 验证：低 ContextWindow + 长 message 历史
// 触发 AutoCompactRun 调 Model 生成摘要。
func TestAutoCompact_TriggersOnLowThreshold(t *testing.T) {
	// AutoCompact 期 LLM 输出 9 段结构化（含 <summary>...</summary>）；formatCompactSummary
	// 提取 <summary> 块——stub 必须按此格式返回否则被识别为"empty summary block"
	stub := &summaryStubModel{summary: `<analysis>think...</analysis>
<summary>
## Conversation Summary
这是 AutoCompact 摘要内容（覆盖前 N 轮）
</summary>`}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  500,
		SummaryMaxTokens:    100,
		SummaryBufferTokens: 80,
		// 微缩配置（阈值 320 token）——按同比例关掉可压缩区尺寸守卫，
		// 否则 early 达不到 MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
		// Threshold = 500 - 100 - 80 = 320 tokens
	}
	// 用 WithAutoCompact 复制 cfg 让 breakers 字段正确初始化
	tmpAg := agent_v2.NewBasicAgent(agent_v2.WithName("tmpAg"), agent_v2.WithAutoCompact(cfg))
	_ = tmpAg

	// 构造长消息历史：每条 ~200 token estimated（×4/3 chars / 4）
	bigPayload := strings.Repeat("超长内容 ABCDEFGH ", 100) // ~1500 字符
	msgs := []*schema.Message{}
	for i := 0; i < 6; i++ {
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("第 %d 轮 user 输入：%s", i, bigPayload)))
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("第 %d 轮 assistant 回复：%s", i, bigPayload)))
	}

	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun: %v", err)
	}
	if stub.called == 0 {
		t.Fatalf("AutoCompact 应触发 LLM 调用（threshold=%d, current=%d msgs），实际未调用", cfg.Threshold(), len(msgs))
	}

	// 验证 out 含摘要消息（带 marker）
	hasSummary := false
	for _, m := range out {
		if m == nil || m.Annotations == nil {
			continue
		}
		if v, ok := m.Annotations[agent_v2.AutoCompactSummaryAnnotation].(bool); ok && v {
			hasSummary = true
			break
		}
	}
	if !hasSummary {
		t.Fatalf("AutoCompact 应在 out 中标记 summary 消息（带 AutoCompactSummaryAnnotation），实际 out 长度=%d 无 marker", len(out))
	}
	// out 应短于 in（早期 messages 被 summary 替换）
	if len(out) >= len(msgs) {
		t.Logf("WARN: out len=%d >= in len=%d（可能 tail 单组场景仅替换不缩短，或所有消息合并到一组）", len(out), len(msgs))
	}
}

// TestAutoCompact_NoTriggerBelowThreshold 验证：消息少 token 不超阈值时不触发。
func TestAutoCompact_NoTriggerBelowThreshold(t *testing.T) {
	stub := &summaryStubModel{summary: "should not be called"}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  100000,
		SummaryMaxTokens:    20000,
		SummaryBufferTokens: 13000,
	}
	_ = agent_v2.NewBasicAgent(agent_v2.WithName("ag2"), agent_v2.WithAutoCompact(cfg))

	msgs := []*schema.Message{
		schema.UserMessage("hi"),
		schema.AssistantMessage("hello"),
	}
	_, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun: %v", err)
	}
	if stub.called != 0 {
		t.Fatalf("AutoCompact 不应触发（far below threshold），实际 LLM 被调 %d 次", stub.called)
	}
}

// ── ReactiveCompact 触发验证 ──

// TestReactiveCompact_TriggersOnPromptTooLong 验证：LLM 报 prompt-too-long 时
// ReactiveCompactRun 删旧消息组+返压缩后 messages（不返 nil）。
func TestReactiveCompact_TriggersOnPromptTooLong(t *testing.T) {
	cfg := &agent_v2.ReactiveCompactConfig{
		Enabled:     true,
		MaxAttempts: 3,
	}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("msg %d", i)))
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("ans %d", i)))
	}
	// 模拟 OpenAI 风格 prompt-too-long 错（含可解析 token gap）
	err := errors.New(`{"error":{"code":"context_length_exceeded","message":"prompt is too long: 50000 tokens > 8192 maximum"}}`)
	if !agent_v2.IsPromptTooLongError(err) {
		t.Fatalf("test setup: error 应被识别为 prompt-too-long，实际不是")
	}
	out, retryErr := agent_v2.ReactiveCompactRun(context.Background(), cfg, msgs, err)
	if retryErr != nil {
		t.Fatalf("ReactiveCompactRun: %v", retryErr)
	}
	if out == nil {
		t.Fatalf("ReactiveCompact 应返压缩后的 messages，实际 nil")
	}
	if len(out) >= len(msgs) {
		t.Fatalf("ReactiveCompact 应删旧消息组缩短，实际 in=%d out=%d", len(msgs), len(out))
	}
}

// TestReactiveCompact_NoTriggerForUnrelatedError 验证：非 prompt-too-long 错不触发。
func TestReactiveCompact_NoTriggerForUnrelatedError(t *testing.T) {
	err := errors.New("rate limit exceeded")
	if agent_v2.IsPromptTooLongError(err) {
		t.Fatalf("rate-limit 错不应被识别为 prompt-too-long")
	}
}

// TestAutoCompact_BreakerIsolatedAcrossSessions 验证同一份配置的断路器按 RunID 隔离。
func TestAutoCompact_BreakerIsolatedAcrossSessions(t *testing.T) {
	stub := &alwaysFailModel{}
	rawCfg := &agent_v2.AutoCompactConfig{
		Enabled:                true,
		Model:                  stub,
		ModelContextWindow:     2000,
		SummaryMaxTokens:       100,
		SummaryBufferTokens:    100,
		MaxConsecutiveFailures: 2,
	}
	cfg := applyWithAutoCompact(rawCfg)
	msgs := makeCompactInput(5)

	// sessionA 反复触发 → 拉爆 breaker（2 次失败后 trip）
	ctxA := agent_v2.WithSession(context.Background(), &agent_v2.RunSession{RunID: "sessionA"})
	for i := 0; i < 5; i++ {
		_, _ = agent_v2.AutoCompactRun(ctxA, cfg, msgs)
	}
	callsAfterA := stub.calls.Load()
	if callsAfterA > 3 { // 2 次失败 + 容忍 1 次 race
		t.Fatalf("sessionA breaker 未跳：5 次调用 LLM 被打 %d 次（期望 ≤3）", callsAfterA)
	}

	// sessionB 同样的 cfg 但全新 sessionID → 应该有独立 breaker entry，不受 A 拉爆影响
	ctxB := agent_v2.WithSession(context.Background(), &agent_v2.RunSession{RunID: "sessionB"})
	_, _ = agent_v2.AutoCompactRun(ctxB, cfg, msgs)
	callsAfterB := stub.calls.Load()
	delta := callsAfterB - callsAfterA
	if delta == 0 {
		t.Fatalf("sessionB breaker 被 sessionA 污染：sessionB 跑 1 次但 LLM 未被调（应正常调用）\n"+
			"  callsAfterA=%d callsAfterB=%d delta=%d",
			callsAfterA, callsAfterB, delta)
	}
}

// ── P1.C: skill body 跨 compaction 保鲜测试 ──

// TestAutoCompact_PreservesSkillBodyGroups 验证 P1.C：含 SkillBodyAnnotation marker
// 的整组在 AutoCompact 后被保留（不被 summarize 进摘要），跟 memory_injected 类似但
// 按 group 抽（保 tool_call/tool_result pair 完整）。
//
// **场景**：toolCall 模式下 LLM 调 activate_skill → tool_result 含 SKILL.md body +
// SkillBodyAnnotation marker。长会话触发 AutoCompact → skill body 应保留让下轮 LLM 仍
// 能用其指导。
func TestAutoCompact_PreservesSkillBodyGroups(t *testing.T) {
	stub := &summaryStubModel{summary: `<analysis>...</analysis>
<summary>summary text</summary>`}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  500,
		SummaryMaxTokens:    100,
		SummaryBufferTokens: 80,
		// 微缩配置（阈值 320 token）——按同比例关掉可压缩区尺寸守卫，
		// 否则 early 达不到 MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
	}
	cfgApplied := applyWithAutoCompact(cfg)

	// 构造场景：3 组 user → asst → tool_result
	//  组 1: user("查天气") → asst(tool_call=get_weather) → tool_result(plain) ← 普通工具
	//  组 2: user("翻译") → asst(tool_call=activate_skill) → tool_result(SKILL_BODY) ← skill body
	//  组 3: user("最新提问") → ... ← tail 保留
	bigPayload := strings.Repeat("超长内容 abcdefgh ", 50)
	skillBodyResult := schema.ToolResultMessage("tc_skill", "SKILL_BODY_翻译指南")
	skillBodyResult.Annotations = map[string]any{
		agent_v2.SkillBodyAnnotation: true,
	}

	msgs := []*schema.Message{
		// 组 1: 普通工具（应被 summarize）
		schema.UserMessage("查天气：" + bigPayload),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_weather", Type: "function",
			Function: schema.FunctionCall{Name: "get_weather", Arguments: `{}`},
		}}),
		schema.ToolResultMessage("tc_weather", "晴 12°C"),
		// 组 2: skill 激活（应整组保留）
		schema.UserMessage("翻译这段：" + bigPayload),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_skill", Type: "function",
			Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"translate"}`},
		}}),
		skillBodyResult,
		// 组 3: tail（user 触发 LLM 调用前的最新轮）
		schema.UserMessage("最新提问"),
	}

	out, err := agent_v2.AutoCompactRun(context.Background(), cfgApplied, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun: %v", err)
	}
	if stub.called == 0 {
		t.Fatalf("AutoCompact 应触发（msgs 超 threshold），实际未触发")
	}

	// out 必含 SKILL_BODY_翻译指南（保鲜）
	hasSkillBody := false
	for _, m := range out {
		if m != nil && strings.Contains(m.Content, "SKILL_BODY_翻译指南") {
			hasSkillBody = true
			break
		}
	}
	if !hasSkillBody {
		t.Errorf("P1.C 回归：AutoCompact 后 skill body 应保留，实际 out 找不到 SKILL_BODY_翻译指南")
	}

	// out 必含对应的 assistant tool_call message（pair 完整）
	hasSkillToolCall := false
	for _, m := range out {
		if m == nil {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			if tc.ID == "tc_skill" {
				hasSkillToolCall = true
			}
		}
	}
	if !hasSkillToolCall {
		t.Errorf("P1.C 回归：skill body 的 tool_result 必须保留配对的 assistant tool_call message（防 OpenAI 422 orphan tool_result），实际 out 找不到 tc_skill assistant 消息")
	}

	// 普通工具的内容应被 summarize 不在 out 中
	hasWeatherResult := false
	for _, m := range out {
		if m != nil && strings.Contains(m.Content, "晴 12°C") {
			hasWeatherResult = true
			break
		}
	}
	if hasWeatherResult {
		t.Errorf("普通工具结果应被 summarize 进摘要，实际 out 仍含 '晴 12°C'")
	}
}

// TestAutoCompact_SkillBodyAnnotationNoMarker_NotPreserved 验证未打 marker 的 tool_result
// 不会被特殊处理（防 marker 误用范围扩大破坏 AutoCompact 语义）。
func TestAutoCompact_SkillBodyAnnotationNoMarker_NotPreserved(t *testing.T) {
	stub := &summaryStubModel{summary: `<summary>summary</summary>`}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  500,
		SummaryMaxTokens:    100,
		SummaryBufferTokens: 80,
		// 微缩配置（阈值 320 token）——按同比例关掉可压缩区尺寸守卫，
		// 否则 early 达不到 MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
	}
	cfgApplied := applyWithAutoCompact(cfg)

	bigPayload := strings.Repeat("超长内容 abcdefgh ", 50)
	// 注：tool_result 但**不**打 SkillBodyAnnotation
	msgs := []*schema.Message{
		// 多组让 AutoCompact 触发
		schema.UserMessage("Q1: " + bigPayload),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Function: schema.FunctionCall{Name: "activate_skill"},
		}}),
		schema.ToolResultMessage("tc1", "FAKE_BODY_未打 marker"),
		schema.UserMessage("Q2: " + bigPayload),
		schema.AssistantMessage("answer 2"),
		schema.UserMessage("最新提问"),
	}

	out, err := agent_v2.AutoCompactRun(context.Background(), cfgApplied, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun: %v", err)
	}
	if stub.called == 0 {
		t.Skip("test setup: AutoCompact 未触发（threshold 没超），无法验证；调大 bigPayload 重跑")
	}

	// 未打 marker 的 fake body 应被 summarize 掉（不在 out 中）
	for _, m := range out {
		if m != nil && strings.Contains(m.Content, "FAKE_BODY_未打 marker") {
			t.Errorf("未打 SkillBodyAnnotation 的 tool_result 不应被特殊保留（marker 才是 protect 信号），实际 out 仍含此内容")
		}
	}
}

// TestReactiveCompact_ProtectsSkillBodyGroups 验证 P1.C audit H1 fix：
// ReactiveCompact emergencyDropOldestGroups 不能删带 SkillBodyAnnotation 的组——
// 否则 AutoCompact 刚保鲜的 skill body 下一轮被第 3 层删掉前功尽弃。
//
// **R9 audit 修订**：旧 fixture error 字符串 `prompt is too long: ... > ... maximum`
// 不匹配 promptTooLongOpenAIRe / promptTooLongClaudeRe 任何一个 → ParsePromptTooLongGap
// 返 0 → emergencyDrop 走 dropPercent 兜底（20% × 3 droppable = 0 → 兜底 1）→
// 永远只删最旧 1 组（恰巧 g0 不是 skill 组）→ 即使删 SkillBody 检查测试也 PASS（**假绿**）。
//
// 修订：用 Claude 真实格式 `Input is too long: 50000 input tokens (max 8192)` 让
// ParsePromptTooLongGap 解析 gap=41808 → 算法循环用尽 droppableIdx → dropCount 拉满。
// fix 前 droppable=[0,1,2] → toDrop=[0,1] → 删 g0+g1（含 skill body）；
// fix 后 droppable=[0,2]（保护 g1）→ toDrop=[0] → 仅删 g0。这才能区分 old/new。
func TestReactiveCompact_ProtectsSkillBodyGroups(t *testing.T) {
	cfg := &agent_v2.ReactiveCompactConfig{
		Enabled:     true,
		MaxAttempts: 3,
	}
	skillBodyResult := schema.ToolResultMessage("tc_skill", "SKILL_BODY_保鲜")
	skillBodyResult.Annotations = map[string]any{
		agent_v2.SkillBodyAnnotation: true,
	}
	msgs := []*schema.Message{
		// 多个普通组让 emergencyDrop 有目标
		schema.UserMessage("Q1: " + strings.Repeat("a ", 100)),
		schema.AssistantMessage("A1: " + strings.Repeat("a ", 100)),
		// skill 组（应被保护）
		schema.UserMessage("translate"),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_skill", Function: schema.FunctionCall{Name: "activate_skill"},
		}}),
		skillBodyResult,
		// 更多普通组
		schema.UserMessage("Q3: " + strings.Repeat("c ", 100)),
		schema.AssistantMessage("A3: " + strings.Repeat("c ", 100)),
	}
	// R9 修：用 Claude 真实格式让 ParsePromptTooLongGap 解析出 gap=41808 → dropCount 拉满
	err := errors.New(`Input is too long: 50000 input tokens (max 8192)`)
	out, retryErr := agent_v2.ReactiveCompactRun(context.Background(), cfg, msgs, err)
	if retryErr != nil {
		t.Fatalf("ReactiveCompactRun: %v", retryErr)
	}

	// out 必含 skill body（保护）
	hasSkillBody := false
	for _, m := range out {
		if m != nil && strings.Contains(m.Content, "SKILL_BODY_保鲜") {
			hasSkillBody = true
			break
		}
	}
	if !hasSkillBody {
		t.Errorf("P1.C audit H1 回归：ReactiveCompact 删了带 SkillBodyAnnotation 的组——必须 isProtectedGroup 加 SkillBody 检查")
	}
}

// 注：P1.C audit H2（adaptive 模式 + 业务自带同名 activate_skill 工具不被错误打 marker）
// 通过 makeToolsFn skillNeedsTool 闭包标志在**编译期**保证——marker 注入条件
// `if skillNeedsTool && tc.Function.Name == activateSkillToolName && ...`，
// adaptive 模式下 skillNeedsTool=false 必跳过 marker 注入。无需端到端测覆盖。

// TestAutoCompact_DoesNotCountSystemTokens 验证综合 audit H2 fix：
// shouldTrigger 估算 token 时跳过 system messages，避免 adaptive 注入大 SKILL.md
// body 进 system 时持续反复触发 AutoCompact 死循环（业务付 N 倍 LLM 费用 +
// 断路器跳）。
//
// **R9 audit 修订**：旧 fixture conv 只 1 组，AutoCompactRun 内 `len(groups) <= 1`
// 直接 bail 不调 stub —— stub.called=0 在 fix 前后都成立，**测试无法区分 old/new**。
// 修订：conv 必须 ≥ 2 组，让 fix 前 trigger 后真能调到 stub；fix 后 shouldTrigger
// 直接 false 跳过，stub.called=0 才是真有差异的断言。
//
// **sanity 设计**：sys 巨大（~2250 token）+ conv 2 组：
//   - fix 前（shouldTrigger 算 system）：~2250 + conv > threshold(2000) → trigger
//     → AutoCompactRun 进入主流程 → groups=2 通过 bail → 调 stub 1 次
//   - fix 后（shouldTrigger 跳 system）：仅算 conv 几 token < threshold → 不 trigger
//     → stub.called=0
func TestAutoCompact_DoesNotCountSystemTokens(t *testing.T) {
	stub := &summaryStubModel{summary: "<summary>...</summary>"}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               stub,
		ModelContextWindow:  3000, // 紧 budget
		SummaryMaxTokens:    500,
		SummaryBufferTokens: 500,
		// Threshold = 3000 - 500 - 500 = 2000 token
	}
	cfgApplied := applyWithAutoCompact(cfg)

	// sys 巨大 + conv 2 组（让 AutoCompactRun bail `len(groups) <= 1` 失效）
	bigSysContent := strings.Repeat("ABCDEFGH ", 1000) // ~9000 char ~3000 token
	msgs := []*schema.Message{
		schema.SystemMessage(bigSysContent),
		schema.UserMessage("Q1"),
		schema.AssistantMessage("A1"),
		schema.UserMessage("Q2"), // 新一组
		schema.AssistantMessage("A2"),
	}

	out, err := agent_v2.AutoCompactRun(context.Background(), cfgApplied, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun: %v", err)
	}
	if stub.called > 0 {
		t.Fatalf("综合 audit H2 回归：sys 巨大但 conv 小，shouldTrigger 应只算 conv 不应触发，"+
			"实际 LLM 被调 %d 次", stub.called)
	}
	// out 应原 messages 不变
	if len(out) != len(msgs) {
		t.Errorf("不该 compact，msgs len 应不变，实际 %d → %d", len(msgs), len(out))
	}
}

// TestAutoCompact_BreakerIsolatedAcrossCfgInstances 验证直接调用 AutoCompactRun 时，
// 两次应用 WithAutoCompact 得到的配置即使传入同一 RunID，也会分别熔断。
func TestAutoCompact_BreakerIsolatedAcrossCfgInstances(t *testing.T) {
	stubParent := &alwaysFailModel{}
	stubChild := &alwaysFailModel{}
	rawCfgParent := &agent_v2.AutoCompactConfig{
		Enabled:                true,
		Model:                  stubParent,
		ModelContextWindow:     2000,
		SummaryMaxTokens:       100,
		SummaryBufferTokens:    100,
		MaxConsecutiveFailures: 2,
	}
	rawCfgChild := &agent_v2.AutoCompactConfig{
		Enabled:                true,
		Model:                  stubChild,
		ModelContextWindow:     2000,
		SummaryMaxTokens:       100,
		SummaryBufferTokens:    100,
		MaxConsecutiveFailures: 2,
	}
	cfgParent := applyWithAutoCompact(rawCfgParent)
	cfgChild := applyWithAutoCompact(rawCfgChild)

	msgs := makeCompactInput(5)
	// 故意用同一 sessionID（最严格的隔离测试——只靠 cfg.breakers 物理分离区分）
	ctx := agent_v2.WithSession(context.Background(), &agent_v2.RunSession{RunID: "shared_sid"})

	// 父：跑 5 次拉爆 breaker（MaxConsecutiveFailures=2，N+1 次后 trip）
	const rounds = 5
	for i := 0; i < rounds; i++ {
		_, _ = agent_v2.AutoCompactRun(ctx, cfgParent, msgs)
	}
	parentCalls := stubParent.calls.Load()
	const maxAllowed = 3 // N=2 失败 + 容忍 1 次 race
	if parentCalls > maxAllowed {
		t.Fatalf("父 cfg breaker 未跳：%d 次调用 LLM 被打 %d 次（max %d）", rounds, parentCalls, maxAllowed)
	}

	// 子配置沿用同一个 RunID，独立记录摘要失败并熔断。
	for i := 0; i < rounds; i++ {
		_, _ = agent_v2.AutoCompactRun(ctx, cfgChild, msgs)
	}
	childCalls := stubChild.calls.Load()
	if childCalls > maxAllowed {
		t.Fatalf("子 cfg breaker 被父污染或自身未生效：%d 次调用 LLM 被打 %d 次（max %d）\n"+
			"  parentCalls=%d childCalls=%d",
			rounds, childCalls, maxAllowed, parentCalls, childCalls)
	}
}

// TestAutoCompact_R23_HIGH1_BasicAgentRunPropagatesInterrupt 是 R23 audit HIGH-1
// 真 e2e 回归——验证 BasicAgent.Run 内 AutoCompact 触发 + cfg.Model 抛
// InterruptError 时**整个 Run propagate**，不被 `_` 吞掉。
//
// **Bug 历史**：basic_agent.go line 658 原 `compacted, _ := AutoCompactRun(...)` 用
// `_` 吞 err——AutoCompact LLM 抛 InterruptError 后 caller 完全不知 → 框架不存
// cp / 父不挂起 / Resume 找不到入口 → HITL 控制流断链。
//
// **关键**：直接测 AutoCompactRun 不够（wrap 用 %w 后 IsInterrupt 仍能解 chain
// 检测）；必须**端到端**测 BasicAgent.Run 整个返回——`_` 吞掉 err 导致 Run 仍
// 走主流 LLM 调用 + 主链路 mock model 自己输出 final → 业务感知 OK 但实际 HITL
// 断链 silent。
//
// **bomb 防御**：parent stub 配 final response，AutoCompact stub 抛 Interrupt——
// 期望 ag.Run 整个返 InterruptError；sanity-revert 改回 `_` 吞 → ag.Run 返 nil
// + 主流 final → 断言 IsInterrupt fail 精确指向 bug。
func TestAutoCompact_R23_HIGH1_BasicAgentRunPropagatesInterrupt(t *testing.T) {
	// AutoCompact 用专门抛 Interrupt 的 model
	acStub := &interruptingAutoCompactModel{}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               acStub,
		ModelContextWindow:  500,
		SummaryMaxTokens:    100,
		SummaryBufferTokens: 80,
		// 微缩配置（阈值 320 token）——按同比例关掉可压缩区尺寸守卫，
		// 否则 early 达不到 MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
	}

	// 主链路 LLM 用普通 stub（mock 业务 model）
	parentStub := &summaryStubModel{summary: "<summary>main reply</summary>"}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithAutoCompact(cfg),
		agent_v2.WithMaxRounds(2),
	)

	// 构造长 input 触发 AutoCompact
	bigPayload := strings.Repeat("ABCDEFGH ", 200)
	sess := agent_v2.NewRunSession()
	// 先跑几次 Run 累积历史超阈值
	for i := 0; i < 5; i++ {
		_, _ = ag.Run(context.Background(), sess,
			schema.UserMessage(fmt.Sprintf("u%d %s", i, bigPayload)))
	}

	// 第 6 次 Run，AutoCompact 应触发 → cfg.Model 抛 Interrupt → 父 Run 应 propagate
	_, err := ag.Run(context.Background(), sess,
		schema.UserMessage("trigger autocompact"))
	if err == nil {
		t.Fatalf("R23 HIGH-1 回归：AutoCompact LLM 抛 InterruptError 时 BasicAgent.Run 应" +
			"propagate（不被 caller `_` 吞）；实际 nil → 业务 silent 跑下去无控制流信号")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("R23 HIGH-1 回归：BasicAgent.Run 应返 InterruptError 类型——实际 %v (%T)\n"+
			"sanity-revert 改回 `_` 吞掉 → Run 不返 InterruptError silent 跑下去", err, err)
	}
}

// interruptingAutoCompactModel 是 R23 测试用 ChatModel——Generate 抛 InterruptError。
type interruptingAutoCompactModel struct {
	calls int
}

func (m *interruptingAutoCompactModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls++
	return nil, &compose_v2.InterruptError{
		Reason: "autocompact_user_review",
		Query:  &compose_v2.AskQuery{InterruptID: "ac1", Question: "approve summary?"},
	}
}
func (m *interruptingAutoCompactModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *interruptingAutoCompactModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*interruptingAutoCompactModel)(nil)
