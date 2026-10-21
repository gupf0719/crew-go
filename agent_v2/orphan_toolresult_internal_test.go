package agent_v2

import (
	"testing"

	"crew-go/schema"
)

// TestEmergencyDrop_NoOrphanToolResult 钉住「按组丢完之后不留孤儿 tool_result」。
//
// **bug 历史（我这次改动放大出来的）**：分组规则里 `RoleSystem` 恒开新组，于是
// `[user][assistant tool_call][system][assistant][tool_result]` 被切成三组——中间那条
// system 把一次 tool_call 往返劈成两半。丢掉最旧那组之后剩下一条孤儿 `tool_result`，
// **真实 API 直接 400/422**，而框架一声不吭。
//
// 让 system 单独成组那次改动把它从「潜伏」变成「必然」：改前 droppable 只有 1 组、
// 被「至少留 1 组」夹住不动，改后 droppable 变 2 组、越过那道夹子。
//
// ⚠️ 修法不是「不许在工具轮中间插 system」——那是个会漂的前提。是**丢完再数一遍**。
func TestEmergencyDrop_NoOrphanToolResult(t *testing.T) {
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"},
	}})
	in := []*schema.Message{
		schema.UserMessage("u1"),
		tc,
		schema.SystemMessage("MID"),
		schema.AssistantMessage("中间产出"),
		schema.ToolResultMessage("c1", "f", "结果"),
	}
	out := emergencyDropOldestGroups(in, 50, 0, "[MARKER]")

	calls := map[string]bool{}
	for _, m := range out {
		for _, c := range schema.GetToolCalls(m) {
			calls[c.ID] = true
		}
	}
	for _, m := range out {
		if m.Role != schema.RoleTool {
			continue
		}
		if id := schema.GetToolResultID(m); id != "" && !calls[id] {
			t.Errorf("留下孤儿 tool_result（id=%s）——发起它的 assistant 已经被丢了，"+
				"真实 API 会 400/422，而框架这边一声不吭", id)
		}
	}
	// 前提：确认这次真的丢了东西，否则「没有孤儿」可以由「什么都没丢」廉价满足。
	if len(out) >= len(in) {
		t.Fatalf("前提没搭对：一条都没丢（%d → %d），被测路径没跑到", len(in), len(out))
	}
}
