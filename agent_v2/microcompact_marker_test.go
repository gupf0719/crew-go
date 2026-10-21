package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestMicroCompact_R17_MED2_SkipsSkillBodyMarker 是 R17 audit MED-2 回归测试——
// 验证业务把 activate_skill 加进 CompactableTools 白名单时，MicroCompact 跳过带
// SkillBodyAnnotation marker 的 tool_result（不清内容），保跨层契约：
//   - AutoCompact splitProtectedGroups 仍能正确按 marker 整组保留 + 内容真有 body
//   - 不会"marker 在但内容是占位符"导致 LLM 看 N 个 placeholder 找不到真 body
//
// **bomb 防御**：fixture 用业务自定义 CompactableTools=["activate_skill"] + KeepRecent=1
// + 5 个 activate_skill tool_result 全标 SkillBodyAnnotation marker。预期：5 条
// tool_result content 都保留原 body 不被 placeholder 替换（因 marker 防御）。
// sanity-revert 删 SkillBodyAnnotation 检查 → 前 4 条 content 被替换为 placeholder
// → 断言 "body content N" 出现次数 < 5 → fail。
func TestMicroCompact_R17_MED2_SkipsSkillBodyMarker(t *testing.T) {
	// 构造 5 个带 SkillBodyAnnotation marker 的 tool_result
	msgs := []*schema.Message{
		schema.SystemMessage("sys"),
	}
	for i := 1; i <= 5; i++ {
		// assistant tool_call
		tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: tcID(i), Type: "function",
			Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"x"}`},
		}})
		msgs = append(msgs, tc)
		// tool_result 含 body marker
		tr := schema.ToolResultMessage(tcID(i), contentBodyN(i))
		tr.Annotations = map[string]any{agent_v2.SkillBodyAnnotation: true}
		msgs = append(msgs, tr)
	}

	// 业务危险配置：把 activate_skill 加进 CompactableTools 白名单 + KeepRecent=1
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:          true,
		KeepRecent:       1,
		CompactableTools: []string{"activate_skill"},
	}

	out := agent_v2.MicroCompactRun(context.Background(), msgs, cfg)

	// 验证：所有 5 条 body content 都保留（marker 防御让 MicroCompact 跳过 clear）
	allContent := concatContent(out)
	for i := 1; i <= 5; i++ {
		if !strings.Contains(allContent, contentBodyN(i)) {
			t.Errorf("R17-MED-2 回归：body %d 应被 SkillBodyAnnotation marker 保护不被 MicroCompact 清，"+
				"实际未在输出找到\nout:\n%s", i, allContent)
		}
	}
}

func tcID(i int) string {
	return "tc_" + string(rune('a'+i-1))
}
func contentBodyN(i int) string {
	return "BODY_CONTENT_" + string(rune('A'+i-1))
}

// errSimulatedPromptTooLong 是测试用错误——含 IsPromptTooLongError 识别的关键词。
type errSimulatedPromptTooLong struct{}

func (errSimulatedPromptTooLong) Error() string {
	return "prompt is too long: this is a test simulated prompt-too-long error"
}

// TestReactiveCompact_R21_HIGH2_AllProtectedReturnsUnchanged 验证全 protected 时
// 兜底**不删** groups[0]——返原 messages 让 retry 走外层失败信号。
//
// **R21 audit HIGH-2 重评（false positive but 代码改进）**：
// audit 报"全 protected 兜底删 groups[0] 破 SkillBody pair"——但原代码已有
// `dropCount >= len(droppableIdx) { dropCount = len-1 }` 保护，单元素 droppableIdx
// 时 dropCount 被 cap 到 0 → 早返回 → 实际不删。**所以原代码 functionally 安全**。
// 但代码可读性差（隐式 cap 才避免 bug）——R21 改 explicit early return 让意图清晰。
//
// 本测试验证 explicit fix 不引入回归（全 protected 时返原 messages）。
func TestReactiveCompact_R21_HIGH2_AllProtectedReturnsUnchanged(t *testing.T) {
	// 全 protected 场景：4 个 group 全标 SkillBody marker
	msgs := []*schema.Message{schema.SystemMessage("sys")}
	for i := 1; i <= 4; i++ {
		u := schema.UserMessage("user " + string(rune('a'+i-1)))
		u.Annotations = map[string]any{agent_v2.SkillBodyAnnotation: true}
		msgs = append(msgs, u)
		a := schema.AssistantMessage("ans " + string(rune('a'+i-1)))
		a.Annotations = map[string]any{agent_v2.SkillBodyAnnotation: true}
		msgs = append(msgs, a)
	}
	originalLen := len(msgs)

	cfg := &agent_v2.ReactiveCompactConfig{
		Enabled:              true,
		EmergencyDropPercent: 50,
	}
	out, err := agent_v2.ReactiveCompactRun(context.Background(), cfg, msgs, errSimulatedPromptTooLong{})
	if err != nil {
		t.Fatalf("ReactiveCompactRun: %v", err)
	}

	// R21 HIGH-2 关键断言：全 protected 时应返原 messages（不删 groups[0] 破 pair）
	if len(out) != originalLen {
		t.Errorf("R21 HIGH-2 回归：全 protected 时应返原 messages 让外层 retry 失败，"+
			"实际 len 从 %d 变 %d——可能仍删 groups[0] 破坏 SkillBody pair", originalLen, len(out))
	}
	// 所有原 user a/b/c/d 都应保留
	all := concatContent(out)
	for _, name := range []string{"user a", "user b", "user c", "user d"} {
		if !strings.Contains(all, name) {
			t.Errorf("R21 HIGH-2 回归：%s 应保留（全 protected 兜底不删）, 实际未在输出找到\n%s", name, all)
		}
	}
}
