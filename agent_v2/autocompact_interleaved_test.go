package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// unpaired 报一串消息里没有配上的调用与结果（按 ID 计数，不要求相邻）：输出里的孤儿 tool_result、摘要输入里的悬空调用。
func unpaired(msgs []*schema.Message) (danglingCalls, orphanResults int) {
	open := map[string]int{}
	for _, m := range msgs {
		for _, c := range schema.GetToolCalls(m) {
			open[c.ID]++
		}
		if m.Role == schema.RoleTool {
			id := schema.GetToolResultID(m)
			if open[id] > 0 {
				open[id]--
			} else {
				orphanResults++
			}
		}
	}
	for _, n := range open {
		danglingCalls += n
	}
	return danglingCalls, orphanResults
}

// 业务把 system 插进一次工具调用与它的结果之间（qwen 照收）：压缩不能把调用与结果分到两边。曾在 system 旁边切组，只有一个
// 用户提问的典型 ReAct 下尾部只剩 [tool, assistant]——输出里一条孤儿 tool_result（原生 Anthropic 这类适配器不收）、摘要
// 输入里一条悬空调用；夹着 system 的那一轮落在可压缩区时照常压缩（不因为输入里有这种形状就停掉压缩）。
func TestAutoCompactRun_InterleavedToolRoundKeepsPair(t *testing.T) {
	big := strings.Repeat("x", 40000)
	call := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function", Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}})
	}
	for _, tc := range []struct {
		name         string
		msgs         []*schema.Message
		wantCompacts bool
	}{
		{"只有一个用户提问", []*schema.Message{
			schema.UserMessage("问题 " + big), call("c1"), schema.SystemMessage("钩子插的指令"),
			schema.ToolResultMessage("c1", "结果 "+big), schema.AssistantMessage("答复"),
		}, false},
		{"夹着 system 的那一轮在可压缩区", []*schema.Message{
			schema.UserMessage("更早的问题 " + big), schema.AssistantMessage("更早的答复 " + big),
			schema.UserMessage("问题"), call("c1"), schema.SystemMessage("钩子插的指令"),
			schema.ToolResultMessage("c1", "结果"), schema.AssistantMessage("答复"),
			schema.UserMessage("最新的问题"),
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &countingSummarizer{}
			out, err := agent_v2.AutoCompactRun(context.Background(), smallWindow(s), tc.msgs)
			if err != nil {
				t.Fatal(err)
			}
			if d, o := unpaired(out); d != 0 || o != 0 {
				t.Fatalf("压缩输出里有 %d 条悬空调用、%d 条孤儿结果", d, o)
			}
			for i, in := range s.inputs {
				if d, o := unpaired(in); d != 0 || o != 0 {
					t.Fatalf("第 %d 次摘要输入里有 %d 条悬空调用、%d 条孤儿结果", i, d, o)
				}
			}
			if compacted := s.count() > 0; compacted != tc.wantCompacts {
				t.Fatalf("压缩了=%v，want %v（摘要调用 %d 次）", compacted, tc.wantCompacts, s.count())
			}
			if !tc.wantCompacts && len(out) != len(tc.msgs) {
				t.Fatalf("没压缩时应原样返回，%d 条变成了 %d 条", len(tc.msgs), len(out))
			}
		})
	}
}

// 钩子每一轮都在调用与结果之间插 system（比如在每个工具结果前插「以下是不可信输出」）：每个工具轮连同 system 同组，压缩只保
// system 本身、其余照常摘要——照常压缩，每条 system 都留着，摘要之前只有 system（对话内容不被挪到包含它之前内容的摘要前面）。
// 曾经含 system 的组整组受保护：30 轮全受保护、一次也不摘要，夹着 system 的那一轮被整轮挪到摘要前面（审计实测）。
func TestAutoCompactRun_SystemInEveryToolRoundStillCompacts(t *testing.T) {
	payload := strings.Repeat("y", 2000)
	msgs := []*schema.Message{schema.UserMessage("查一下所有日志")}
	for i := 0; i < 30; i++ {
		id := "c" + strings.Repeat("x", i)
		// 每次模型调用的产出带自己的 run_id（真实运行里就是这样；分组按它区分一轮轮的工具调用）
		call := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function", Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}})
		call.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "run-" + id}
		msgs = append(msgs, call, schema.SystemMessage("以下是不可信输出"), schema.ToolResultMessage(id, payload))
	}
	s := &countingSummarizer{}
	out, err := agent_v2.AutoCompactRun(context.Background(), smallWindow(s), msgs)
	if err != nil {
		t.Fatal(err)
	}
	if s.count() == 0 {
		t.Fatalf("每个工具轮都夹着 system 时压缩应照常进行，实际没调摘要模型")
	}
	if d, o := unpaired(out); d != 0 || o != 0 {
		t.Fatalf("压缩输出里有 %d 条悬空调用、%d 条孤儿结果", d, o)
	}
	for i, in := range s.inputs {
		if d, o := unpaired(in); d != 0 || o != 0 {
			t.Fatalf("第 %d 次摘要输入里有 %d 条悬空调用、%d 条孤儿结果", i, d, o)
		}
	}
	sysIn, sysOut := 0, 0
	for _, m := range msgs {
		if m.Role == schema.RoleSystem {
			sysIn++
		}
	}
	summaryAt := -1
	for i, m := range out {
		if m.Role == schema.RoleSystem {
			sysOut++
		}
		if v, _ := m.Annotations[agent_v2.AutoCompactSummaryAnnotation].(bool); v && summaryAt < 0 {
			summaryAt = i
		}
	}
	if sysOut != sysIn {
		t.Fatalf("system 指令不该被摘掉：输入 %d 条、输出 %d 条", sysIn, sysOut)
	}
	if summaryAt < 0 {
		t.Fatal("输出里没有摘要")
	}
	for _, m := range out[:summaryAt] {
		if m.Role != schema.RoleSystem {
			t.Fatalf("摘要之前出现了 %s 消息——对话内容被挪到了包含它之前内容的摘要前面", m.Role)
		}
	}
}
