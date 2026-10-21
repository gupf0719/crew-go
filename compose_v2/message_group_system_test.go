package compose_v2_test

import (
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func rolesOf(g []*schema.Message) string {
	var b strings.Builder
	for _, m := range g {
		b.WriteString(string(m.Role) + " ")
	}
	return strings.TrimSpace(b.String())
}

// TestGroupMessagesByAPIRound_SystemIsAlwaysAlone 钉住「system 消息真的单独成组」。
//
// **这条注释以前是假的**：代码只让 system **开**新组、没让它**关**，而 `RoleAssistant`
// 只在 RunID 变化时才开新组 ⟹「system 紧跟一条无 RunID 的 assistant」混成同一组。
//
// **为什么要它真成立**：压缩层按「组」决定丢不丢，含 system 的组受保护 ⟹ 混组意味着
// 一条 system 把整轮 assistant + 全部 tool_result 一起变成永久不可压缩。实测过那个代价：
// 摘要跑完**反而变大**（入 30016 字节、出 30059），还白烧一次 LLM，且下一轮再触发。
//
// 断言选在「那一组的长度」上——只断言「system 开了新组」抓不到「它没关上」。
func TestGroupMessagesByAPIRound_SystemIsAlwaysAlone(t *testing.T) {
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"},
	}})
	cases := map[string][]*schema.Message{
		"system 后跟无 RunID 的 assistant": {
			schema.UserMessage("u1"), schema.SystemMessage("MID"),
			schema.AssistantMessage("紧跟其后"), schema.UserMessage("u2"),
		},
		"system 后跟 tool_result": {
			schema.UserMessage("u1"), tc, schema.SystemMessage("MID"),
			schema.ToolResultMessage("c1", "结果"),
		},
		"两条相邻 system": {
			schema.UserMessage("u1"), schema.SystemMessage("A"),
			schema.SystemMessage("B"), schema.UserMessage("u2"),
		},
		"前导 system": {
			schema.SystemMessage("LEAD"), schema.UserMessage("u1"),
			schema.AssistantMessage("a1"),
		},
	}
	for name, msgs := range cases {
		for i, g := range compose_v2.GroupMessagesByAPIRound(msgs) {
			hasSys := false
			for _, m := range g {
				if m.Role == schema.RoleSystem {
					hasSys = true
				}
			}
			if hasSys && len(g) != 1 {
				t.Errorf("[%s] 组%d 含 system 却不是单独成组：[%s]——"+
					"它会把同组的其他消息一起变成永久不可压缩", name, i, rolesOf(g))
			}
		}
	}
}

// TestGroupMessagesByAPIRound_NonSystemGroupingUnchanged 钉住「让 system 关组不影响别的分组规则」。
//
// 改动只在「上一条是 system」这个条件上加了一刀，其余规则（user 开新组、assistant 按 RunID、
// tool 跟随）必须一个字没变——否则会静默破坏 tool_call ↔ tool_result 的同组性。
func TestGroupMessagesByAPIRound_NonSystemGroupingUnchanged(t *testing.T) {
	tc1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"},
	}})
	msgs := []*schema.Message{
		schema.UserMessage("u1"),
		tc1,
		schema.ToolResultMessage("c1", "r1"),
		schema.UserMessage("u2"),
		schema.AssistantMessage("a2"),
	}
	got := compose_v2.GroupMessagesByAPIRound(msgs)
	want := []string{"user assistant tool", "user assistant"}
	if len(got) != len(want) {
		t.Fatalf("组数变了：期望 %d 组，实际 %d 组 %v", len(want), len(got), func() []string {
			var out []string
			for _, g := range got {
				out = append(out, rolesOf(g))
			}
			return out
		}())
	}
	for i := range want {
		if rolesOf(got[i]) != want[i] {
			t.Errorf("组%d 形态变了：期望 [%s]，实际 [%s]——tool_result 必须跟它的 assistant 同组",
				i, want[i], rolesOf(got[i]))
		}
	}
}
