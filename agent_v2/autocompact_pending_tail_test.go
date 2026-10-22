package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// withRunID 给一条 assistant 打上模型调用的 RunID：分组按它把每次模型调用分成一组。
func withRunID(m *schema.Message, runID string) *schema.Message {
	if m.Annotations == nil {
		m.Annotations = map[string]any{}
	}
	m.Annotations[compose_v2.RunIDAnnotationKey] = runID
	return m
}

func pendingUser(text, annotation string) *schema.Message {
	m := schema.UserMessage(text)
	m.Annotations = map[string]any{annotation: true}
	return m
}

// 消息流末尾是刚接进来、模型还没回应的插话 / 闸门反馈时，AutoCompact 原文保留的那一段要从它前面那一轮算起：
// 插话说的是刚才那批工具结果（「只看 2026 年的」），闸门反馈说的是刚才那条答复（「漏了价格」）——那一轮被摘要
// 掉，模型就只能对着摘要回应。
//
// sanity-revert：tail 从「最后一条 user 所在的组」起算、不往前带 ⟹ 末尾那条单独成组的插话把最近一轮挤进摘要，
// 输出里没有那批结果的原文，本用例红。
func TestAutoCompact_PendingUserInputKeepsPrecedingRound(t *testing.T) {
	big := strings.Repeat("超长内容 ABCDEFGH ", 100)
	for _, tc := range []struct {
		name    string
		tail    []*schema.Message
		keepRaw string // 那一轮的标记
		kept    bool   // 它该不该原文留在输出里
	}{
		{
			name: "插话接在工具结果后面",
			tail: []*schema.Message{
				withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Function: schema.FunctionCall{Name: "search", Arguments: `{}`}}}), "r2"),
				schema.ToolResultMessage("c2", "LATEST_RESULT "+big),
				pendingUser("只看 2026 年的", agent_v2.InterjectionAnnotation),
			},
			keepRaw: "LATEST_RESULT",
			kept:    true,
		},
		{
			name: "闸门反馈加上最后一次的那句",
			tail: []*schema.Message{
				withRunID(schema.AssistantMessage("VETOED_ANSWER "+big), "r2"),
				pendingUser("漏了价格", agent_v2.TurnGateFeedbackAnnotation),
				pendingUser("这是最后一次被要求继续", agent_v2.TurnGateFeedbackAnnotation),
			},
			keepRaw: "VETOED_ANSWER",
			kept:    true,
		},
		{
			// 对照：用户真正的下一个问题照旧是新一轮的起点，它前面那一轮照常进摘要（只往前带「待回应」的那两类）
			name: "用户真正的下一个问题",
			tail: []*schema.Message{
				withRunID(schema.AssistantMessage("PREV_ROUND "+big), "r2"),
				schema.UserMessage("第二个问题"),
			},
			keepRaw: "PREV_ROUND",
			kept:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := &capturingSummaryModel{}
			cfg := &agent_v2.AutoCompactConfig{
				Enabled: true, Model: sm, ModelContextWindow: 500, SummaryMaxTokens: 100, SummaryBufferTokens: 80,
				MinCompactableTokens: 1,
			}
			msgs := append([]*schema.Message{
				schema.UserMessage("查一下 " + big),
				withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Function: schema.FunctionCall{Name: "search", Arguments: `{}`}}}), "r1"),
				schema.ToolResultMessage("c1", "OLD_RESULT "+big),
			}, tc.tail...)
			out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
			if err != nil {
				t.Fatal(err)
			}
			if len(sm.captured) == 0 {
				t.Fatal("没触发压缩（阈值构造失误），本用例测不到什么")
			}
			kept := false
			for _, m := range out {
				if strings.HasPrefix(m.Content, tc.keepRaw) {
					kept = true
				}
			}
			if kept != tc.kept {
				t.Errorf("%s 那一轮原文保留 = %v，该是 %v（%d 条输出）", tc.keepRaw, kept, tc.kept, len(out))
			}
			if orphans := orphanToolResults(out); len(orphans) > 0 {
				t.Errorf("输出含孤儿 tool_result %v", orphans)
			}
		})
	}
}

// 前面有一组受保护的记忆（业务以 user 消息注入、打 StoreInjectedAnnotation）、只有一轮真实的问答、末尾是待回应的
// 插话：往前带到那一轮之后，「前面全是受保护组 ⟹ 把 tail 重置成最后一组」那条兜底不能把它又顶回去。
//
// sanity-revert：往前带放在那条兜底之前 ⟹ tail 被重置成只剩插话，那一轮的工具结果被摘要掉，本用例红。
func TestAutoCompact_PendingTailSurvivesProtectedLeadingGroup(t *testing.T) {
	big := strings.Repeat("超长内容 ABCDEFGH ", 100)
	sm := &capturingSummaryModel{}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled: true, Model: sm, ModelContextWindow: 500, SummaryMaxTokens: 100, SummaryBufferTokens: 80,
		MinCompactableTokens: 1,
	}
	mem := schema.UserMessage("记忆：用户偏好简洁")
	mem.Annotations = map[string]any{agent_v2.StoreInjectedAnnotation: true}
	msgs := []*schema.Message{
		mem,
		schema.UserMessage("查一下 " + big),
		withRunID(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Function: schema.FunctionCall{Name: "search", Arguments: `{}`}}}), "r1"),
		schema.ToolResultMessage("c1", "LATEST_RESULT "+big),
		pendingUser("只看 2026 年的", agent_v2.InterjectionAnnotation),
	}
	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatal(err)
	}
	kept := false
	for _, m := range out {
		if strings.HasPrefix(m.Content, "LATEST_RESULT") {
			kept = true
		}
	}
	if !kept {
		t.Errorf("插话前面那一轮的工具结果该原文保留，实际被摘要掉了（摘要调用 %d 次，输出 %d 条）", len(sm.captured), len(out))
	}
	if orphans := orphanToolResults(out); len(orphans) > 0 {
		t.Errorf("输出含孤儿 tool_result %v", orphans)
	}
}
