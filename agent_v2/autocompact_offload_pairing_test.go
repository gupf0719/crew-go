package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

// capturingSummaryModel 返回合法 <summary> 块 + 捕获 summary 调用收到的 input（验配对）。
type capturingSummaryModel struct {
	captured [][]*schema.Message
}

func (m *capturingSummaryModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	cp := make([]*schema.Message, len(msgs))
	copy(cp, msgs)
	m.captured = append(m.captured, cp)
	return schema.AssistantMessage("<analysis>t</analysis>\n<summary>\n早期轮次摘要。\n</summary>"), nil
}
func (m *capturingSummaryModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *capturingSummaryModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// orphanToolResults 返回无前置配对 assistant tool_call 的 RoleTool 消息 callID（孤儿检查）。
func orphanToolResults(msgs []*schema.Message) []string {
	callIDs := make(map[string]bool)
	for _, m := range msgs {
		if m == nil {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			callIDs[tc.ID] = true
		}
	}
	var orphans []string
	for _, m := range msgs {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if id := schema.GetToolResultID(m); !callIDs[id] {
			orphans = append(orphans, id)
		}
	}
	return orphans
}

// danglingAssistantTCs 返回同流内无配对 tool_result 的 assistant tool_call ID（悬空检查）。
func danglingAssistantTCs(msgs []*schema.Message) []string {
	resultIDs := make(map[string]bool)
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleTool {
			resultIDs[schema.GetToolResultID(m)] = true
		}
	}
	var dangling []string
	for _, m := range msgs {
		if m == nil || m.Role != schema.RoleAssistant {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			if !resultIDs[tc.ID] {
				dangling = append(dangling, tc.ID)
			}
		}
	}
	return dangling
}

// TestAutoCompact_OffloadedGroupKeepsPairing 验证 audit HIGH（AutoCompact 逐条抽 offloaded
// tool_result 造成配对断裂）：MicroCompact offload 后的 tool_result（RoleTool + offloaded
// marker）落在 early 组里，AutoCompact 压缩时——
//  1. 输出不得含孤儿 tool_result（占位符被单独拼到 summary 前、无配对 assistant → 主 LLM 422）
//  2. summary model 收到的 input 不得含悬空 assistant tool_call（配对 tool_result 被抽走 →
//     摘要调用 400 → AutoCompact 静默失效）
//
// 修法与 skill body（P1.C splitProtectedGroups 整组保留）同款：offloaded 所在 API 轮整组
// 保留，不逐条抽。
func TestAutoCompact_OffloadedGroupKeepsPairing(t *testing.T) {
	sm := &capturingSummaryModel{}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               sm,
		ModelContextWindow:  500,
		SummaryMaxTokens:    100,
		SummaryBufferTokens: 80, // threshold = 320 tokens
		// 微缩配置——按同比例关掉可压缩区尺寸守卫，否则 early 达不到
		// MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
	}

	big := strings.Repeat("超长内容 ABCDEFGH ", 100) // ~1500 字符，确保超阈值触发压缩

	// offload 后的占位 tool_result（MicroCompact 产物形态：RoleTool + 双 marker + 占位 content）
	offloaded := schema.ToolResultMessage("tc_off_1", "", "[offloaded:tc_off_1] preview: 原全文已卸载到 store")
	offloaded.Annotations = map[string]any{
		agent_v2.MicroCompactOffloadedAnnotation:  true,
		agent_v2.MicroCompactOffloadKeyAnnotation: "tc_off_1",
	}

	msgs := []*schema.Message{
		schema.SystemMessage("你是助手。"),
		// Group A（early）：含 offload 配对（assistant tool_call + 占位 tool_result）
		schema.UserMessage("第 1 轮：" + big),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       "tc_off_1",
			Function: schema.FunctionCall{Name: "read_file", Arguments: `{"path":"a.txt"}`},
		}}),
		offloaded,
		// Group B（early）：普通文本轮堆 token
		schema.UserMessage("第 2 轮：" + big),
		schema.AssistantMessage("第 2 轮回复：" + big),
		// Group C（tail）：最后 user 组
		schema.UserMessage("第 3 轮：现在总结"),
	}

	out, err := agent_v2.AutoCompactRun(context.Background(), cfg, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun: %v（若为 summary 输入非法导致，正是本 bug 的 400 面）", err)
	}
	if len(sm.captured) == 0 {
		t.Fatal("未触发压缩（阈值构造失误），测试无效")
	}

	// 断言 1：输出无孤儿 tool_result
	for i, m := range out {
		t.Logf("out[%d] role=%s tcs=%d resultID=%q content=%.30q", i, m.Role, len(schema.GetToolCalls(m)), schema.GetToolResultID(m), m.Content)
	}
	if orphans := orphanToolResults(out); len(orphans) > 0 {
		t.Errorf("❌ 输出含孤儿 tool_result %v（无配对 assistant tool_call，真实 API 422）", orphans)
	}

	// 断言 2：summary input 无悬空 assistant tool_call
	for _, cap := range sm.captured {
		if dangling := danglingAssistantTCs(cap); len(dangling) > 0 {
			t.Errorf("❌ summary input 含悬空 assistant tool_call %v（配对 tool_result 被抽走，真实摘要调用 400）", dangling)
		}
	}
}
