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
//
// 例外：system 夹在一次调用与它的结果之间时跟这一对同组（见 TestGroupMessagesByAPIRound_PairNeverSplit）——切开会把
// 调用与结果分到两组。
func TestGroupMessagesByAPIRound_SystemIsAlwaysAlone(t *testing.T) {
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"},
	}})
	cases := map[string][]*schema.Message{
		"system 后跟无 RunID 的 assistant": {
			schema.UserMessage("u1"), schema.SystemMessage("MID"),
			schema.AssistantMessage("紧跟其后"), schema.UserMessage("u2"),
		},
		"system 后跟 tool_result（调用的结果已经到齐）": {
			schema.UserMessage("u1"), tc, schema.ToolResultMessage("c1", "结果"),
			schema.SystemMessage("MID"), schema.ToolResultMessage("c9", "没有对应调用的结果"),
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

// 一次调用与它的结果总在同一组：业务把 system / user 插进它们之间时（qwen 照收，DeepSeek / OpenAI 拒），曾在 system 旁边
// 切开——压缩层整组拎出、摘要、丢弃时把调用与结果分到两边，输出里留下孤儿 tool_result、摘要输入里带着悬空的调用。结果到齐
// 之后照常切组；没收到结果的调用不拖住后面的分组；同一个 ID 被后面重新声明时，后面的结果配给后面那次。
func TestGroupMessagesByAPIRound_PairNeverSplit(t *testing.T) {
	tcOf := func(ids ...string) *schema.Message {
		calls := make([]schema.ToolCall, 0, len(ids))
		for _, id := range ids {
			calls = append(calls, schema.ToolCall{ID: id, Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"}})
		}
		return schema.AssistantToolCallMessage(calls)
	}
	tr := func(id string) *schema.Message { return schema.ToolResultMessage(id, "结果") }
	// multi：一条 tool 消息里放几个结果块（业务照 Anthropic 的样子合并结果；openai 转换时拆成几条）
	multi := func(ids ...string) *schema.Message {
		m := &schema.Message{Role: schema.RoleTool}
		for _, id := range ids {
			m.ContentBlocks = append(m.ContentBlocks, &schema.ContentBlock{Type: "tool_result",
				Toolresult: &schema.ToolresultContentBlock{ToolCallId: id, Content: "结果"}})
		}
		return m
	}
	u, sys := schema.UserMessage, schema.SystemMessage
	for _, tc := range []struct {
		name string
		msgs []*schema.Message
		want []string
	}{
		{"system 夹在调用与结果之间", []*schema.Message{u("u1"), tcOf("c1"), sys("MID"), tr("c1"), u("u2")},
			[]string{"user assistant system tool", "user"}},
		{"user 夹在调用与结果之间", []*schema.Message{u("u1"), tcOf("c1"), u("插话"), tr("c1"), u("u2")},
			[]string{"user assistant user tool", "user"}},
		{"两个调用的结果被 system 隔开", []*schema.Message{u("u1"), tcOf("a", "b"), tr("a"), sys("MID"), tr("b"), u("u2")},
			[]string{"user assistant tool system tool", "user"}},
		{"没收到结果的调用不拖住后面", []*schema.Message{u("u1"), tcOf("c1"), u("u2"), tcOf("c2"), tr("c2")},
			[]string{"user assistant", "user assistant tool"}},
		{"复用 ID：后面的结果配给后面那次", []*schema.Message{u("u1"), tcOf("c1"), u("u2"), tcOf("c1"), tr("c1")},
			[]string{"user assistant", "user assistant tool"}},
		{"结果块不在第一块", []*schema.Message{u("u1"), tcOf("a", "b"), tr("a"), sys("MID"), multi("x", "b"), u("u2")},
			[]string{"user assistant tool system tool", "user"}},
		{"合法形态不受影响", []*schema.Message{u("u1"), tcOf("c1"), tr("c1"), sys("MID"), u("u2")},
			[]string{"user assistant tool", "system", "user"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := compose_v2.GroupMessagesByAPIRound(tc.msgs)
			got := make([]string, 0, len(groups))
			for _, g := range groups {
				got = append(got, rolesOf(g))
			}
			if strings.Join(got, " | ") != strings.Join(tc.want, " | ") {
				t.Fatalf("分组 %q，want %q", got, tc.want)
			}
		})
	}
}
