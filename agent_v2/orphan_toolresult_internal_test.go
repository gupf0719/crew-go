package agent_v2

import (
	"fmt"
	"strings"
	"testing"

	"crew-go/compose_v2"
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
// ⚠️ 修法不是「不许在工具轮中间插 system」——那是个会漂的前提（qwen 照收这种输入）。现在两道：分组时调用还在等结果就
// 不切组（compose_v2.GroupMessagesByAPIRound），丢完再数一遍（`dropOrphanToolResults`，兜别的来源的孤儿）。断言调用与
// 结果**都还在**——只断言「没有孤儿」的话，把调用丢了、再把结果当孤儿清掉也满足，而那样结果的内容就丢了。
func TestEmergencyDrop_NoOrphanToolResult(t *testing.T) {
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"},
	}})
	in := []*schema.Message{
		schema.UserMessage("u0"),
		schema.AssistantMessage("最旧那一轮"),
		schema.UserMessage("u1"),
		tc,
		schema.SystemMessage("MID"),
		schema.AssistantMessage("中间产出"),
		schema.ToolResultMessage("c1", "结果"),
		schema.UserMessage("u2"),
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
	if !calls["c1"] {
		t.Errorf("夹着 system 的那次调用被丢了——它的结果要么成了孤儿、要么跟着被清掉（内容丢失）")
	}
	hasResult := false
	for _, m := range out {
		if schema.GetToolResultID(m) == "c1" {
			hasResult = true
		}
	}
	if !hasResult {
		t.Errorf("c1 的结果不见了")
	}
	// 前提：确认这次真的丢了东西，否则「没有孤儿」可以由「什么都没丢」廉价满足。
	if len(out) >= len(in) {
		t.Fatalf("前提没搭对：一条都没丢（%d → %d），被测路径没跑到", len(in), len(out))
	}
}

// 钩子每一轮都在调用与结果之间插 system 时，紧急压缩照样丢得动：混着 system 的组可以丢，丢的时候 system 原地留下。
// 曾经含 system 的组整组受保护——一条也丢不掉，原样返回、prompt-too-long 照旧（审计实测 10241 → 10241）。
func TestEmergencyDrop_SystemInEveryToolRoundStillDrops(t *testing.T) {
	payload := strings.Repeat("y", 1500)
	in := []*schema.Message{schema.UserMessage("查一下所有日志")}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("c%d", i)
		// 每次模型调用的产出带自己的 run_id（真实运行里就是这样；分组按它区分一轮轮的工具调用）
		call := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"}}})
		call.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "run-" + id}
		in = append(in, call, schema.SystemMessage("以下是不可信输出"), schema.ToolResultMessage(id, payload))
	}
	out := emergencyDropOldestGroups(in, 50, 0, "[MARKER]")
	if len(out) >= len(in) {
		t.Fatalf("应丢掉一部分工具轮，%d 条还是 %d 条", len(in), len(out))
	}
	sysIn, sysOut := 0, 0
	for _, m := range in {
		if m.Role == schema.RoleSystem {
			sysIn++
		}
	}
	calls := map[string]int{}
	for _, m := range out {
		if m.Role == schema.RoleSystem {
			sysOut++
		}
		for _, c := range schema.GetToolCalls(m) {
			calls[c.ID]++
		}
	}
	if sysOut != sysIn {
		t.Fatalf("丢组时 system 指令应原地留下：输入 %d 条、输出 %d 条", sysIn, sysOut)
	}
	// 留下的 system 排在前面时，第一条不是 system 的消息仍要是 user（API 要求 user-first）
	for _, m := range out {
		if m.Role == schema.RoleSystem {
			continue
		}
		if m.Role != schema.RoleUser {
			t.Fatalf("丢组之后第一条不是 system 的消息是 %s，要补 user 占位", m.Role)
		}
		break
	}
	for _, m := range out {
		if m.Role == schema.RoleTool && calls[schema.GetToolResultID(m)] == 0 {
			t.Fatalf("留下孤儿 tool_result %s", schema.GetToolResultID(m))
		}
	}
}
