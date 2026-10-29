package agent_v2

// `pairToolResults` / `danglingToolCalls` 的判定回归。
//
// 这两个是「视图重建」和「历史修复」共用的**唯一**一份配对实现——全仓另有 6 份各自
// 独立的（截断守卫、切点调整、三层压缩），共用这份的理由是：两套规则一旦分叉，视图层
// 会自愈（重建时多余的被丢），而历史层不会（append-only 删不掉）。
//
// **判据是「结果排在声明之后」不要求紧跟**，这是刻意的：合成结果只能 append 到流
// 末尾、跟声明之间可能隔着别的消息，改严会让它永远算不上应答 ⟹ 每次修复再补一条 ⟹
// 历史无限增长且完全静默。下面「隔着消息也算应答」那条就是这个不变量的哨兵。

import (
	"strings"
	"testing"

	"crew-go/schema"
)

func tcMsg(tcs ...schema.ToolCall) *schema.Message {
	return schema.AssistantToolCallMessage(tcs)
}
func tc(id, name string) schema.ToolCall {
	return schema.ToolCall{ID: id, Function: schema.FunctionCall{Name: name}}
}

func TestDanglingToolCalls(t *testing.T) {
	cases := []struct {
		name string
		in   []*schema.Message
		want []string // 期望悬空的 tool_call ID，按顺序
		why  string
	}{
		{
			name: "全配对",
			in: []*schema.Message{
				tcMsg(tc("a", "f")), schema.ToolResultMessage("a", "r"),
			},
			want: nil,
		},
		{
			name: "单个悬空",
			in:   []*schema.Message{tcMsg(tc("a", "f"))},
			want: []string{"a"},
		},
		{
			name: "部分配对：两个声明只有一个结果",
			in: []*schema.Message{
				tcMsg(tc("a", "f"), tc("b", "g")),
				schema.ToolResultMessage("a", "r"),
			},
			want: []string{"b"},
			why:  "只有 b 悬空，a 不能被误报",
		},
		{
			name: "隔着别的消息也算应答",
			in: []*schema.Message{
				tcMsg(tc("a", "f")),
				schema.UserMessage("插一句"),
				schema.ToolResultMessage("a", "r"),
			},
			want: nil,
			why: "**承重哨兵**：判据不要求紧跟。改成「必须紧跟」时这条会红——" +
				"而那正是会让历史无限增长的那个改动（合成结果 append 在末尾，永远不紧跟）",
		},
		{
			name: "结果排在声明前面 = 错位，不算应答",
			in: []*schema.Message{
				schema.ToolResultMessage("a", "r"),
				tcMsg(tc("a", "f")),
			},
			want: []string{"a"},
			why:  "孤儿结果不能顶替应答，否则那条声明会被当成已完成",
		},
		{
			name: "id 跨轮复用：各认各的",
			in: []*schema.Message{
				tcMsg(tc("same", "f")), schema.ToolResultMessage("same", "r1"),
				tcMsg(tc("same", "f")), schema.ToolResultMessage("same", "r2"),
			},
			want: nil,
			why:  "队列语义：第二轮认领第二条结果。只记第一条的话第二轮会被误报悬空",
		},
		{
			name: "id 跨轮复用但第二轮没结果",
			in: []*schema.Message{
				tcMsg(tc("same", "f")), schema.ToolResultMessage("same", "r1"),
				tcMsg(tc("same", "f")),
			},
			want: []string{"same"},
			why:  "第二轮悬空——按 map[id]bool 判会漏报（第一轮的结果顶了）",
		},
		{
			name: "同 id 多结果：多出来的不算数，后续声明认领不到",
			in: []*schema.Message{
				tcMsg(tc("a", "f")),
				schema.ToolResultMessage("a", "r1"),
				schema.ToolResultMessage("a", "r2"), // ← 多出来的
				tcMsg(tc("a", "f")),                 // ← 第二条声明
			},
			want: []string{"a"},
			why: "**承重**：r2 排在第二条声明**之前**、不是对它的应答。去掉 " +
				"`declared[id] > len(resultsOf[id])` 那道上限守卫后 r2 会被收进队列、" +
				"让第二条声明误认领到它 ⟹ 悬空漏报。\n" +
				"  （原先那版没有第二条声明，删守卫也测不出差异——tautological）",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := danglingToolCalls(c.in)
			var ids []string
			for _, x := range got {
				ids = append(ids, x.ID)
			}
			if len(ids) != len(c.want) {
				t.Fatalf("悬空数 %d != %d（got %v, want %v）%s", len(ids), len(c.want), ids, c.want, c.why)
			}
			for i := range c.want {
				if ids[i] != c.want[i] {
					t.Errorf("[%d] got %q want %q %s", i, ids[i], c.want[i], c.why)
				}
			}
		})
	}
}

// TestDanglingToolCalls_AgreesWithRebuild 判定与视图重建必须一致。
//
// 两处共用 `pairToolResults`，所以**配对判据**一致。
// ⚠️ **一致的是判据，不是产出结论**：「那条 assistant 一个真结果都没有」时
// `dropDanglingToolCalls` 整批剥掉 tool_calls、一个占位都不补，而 `danglingToolCalls`
// 照报悬空——那是下面那条已修的独立缺陷，不是判据分叉。所以本用例限定在「每轮至少一个真
// 结果」的范围内验一致性（穷举下约 83% 的输入落在缺陷那一档，fixture 的规避是承重的）。
//
// ⚠️ **fixture 里每条带 tool_call 的 assistant 都至少有一个真结果**——这是有意的。
// 「一个真结果都没有」的那条走的是另一条路（把 tool_calls 整批剥掉，见下面那个
// 已知缺陷的哨兵），不产生占位，跟判据一不一致无关。
func TestDanglingToolCalls_AgreesWithRebuild(t *testing.T) {
	in := []*schema.Message{
		schema.UserMessage("go"),
		tcMsg(tc("a", "f"), tc("b", "g")),
		schema.ToolResultMessage("a", "ra"),
		schema.UserMessage("再来"),
		tcMsg(tc("c", "h"), tc("d", "i")),
		schema.ToolResultMessage("c", "rc"),
	}
	dangling := danglingToolCalls(in)
	if len(dangling) != 2 || dangling[0].ID != "b" || dangling[1].ID != "d" {
		t.Fatalf("判定结果不对: %+v", dangling)
	}

	out := dropDanglingToolCalls(in)
	placeholders := 0
	for _, m := range out {
		if m.Role == schema.RoleTool && m.Content == unfinishedToolResultText {
			placeholders++
		}
	}
	if placeholders != len(dangling) {
		t.Errorf("重建补了 %d 条占位，判定说有 %d 个悬空——两处判据分叉了\n"+
			"  它们共用 pairToolResults 就是为了防这个；分叉后历史层会积垃圾（append-only 删不掉）",
			placeholders, len(dangling))
	}
}

// TestDropDangling_PureToolCallRoundKeepsTheRecord 钉住阶段 4 修完后的行为：
// 纯 tool_call 轮（Content 为空）一个真结果都没有时，**assistant 不再整条消失**——
// 它被换成一条文本记录，写明提过什么。
//
// 修的是什么：原来走「保文本」分支，而没有文本可保 ⟹ 整条 assistant 连同它声明过的
// tool_call 一起没了。后果是模型对**自己提过这个动作**的记录被静默抹掉——审批挂起被
// 用户放弃、转头说「先别转了」时，模型看到的历史里没有任何转账痕迹。
//
// 为什么是文本而不是补一条占位 tool_result：见 dropDanglingToolCalls 里那段注释。
// 一句话——这里分不出「本就不会有结果」和「那一轮没跑完」，而占位文案必须二选一。
func TestDropDangling_PureToolCallRoundKeepsTheRecord(t *testing.T) {
	in := []*schema.Message{
		schema.UserMessage("转 5000"),
		tcMsg(tc("x", "transfer_money")), // Content 为空
		schema.UserMessage("先别转了"),
	}
	out := dropDanglingToolCalls(in)

	if len(out) != 3 {
		t.Fatalf("应保留三条（含记录那条），实际 %d 条：%v", len(out), contentsOf(out))
	}
	rec := out[1]
	if rec.Role != schema.RoleAssistant {
		t.Fatalf("中间那条应是 assistant，实际 %s", rec.Role)
	}
	// 承重：工具名必须在，否则「记得提过什么」就没兑现
	if !strings.Contains(rec.Content, "transfer_money") {
		t.Errorf("记录里没有工具名，模型仍然不知道自己提过什么：%q", rec.Content)
	}
	// **这是回归守卫，不是安全保证**——别把它当后者。它只挡几个**已知**踩过的字面量；
	// 同义词一换就绕过去了，而且绕过去的可能更危险：实测「重新提交」重放 5/16，比被这个
	// 黑名单挡下的「重新发起」（2/16）还高，全套测试却零反应。
	//
	// 安全保证只能由真 LLM 给：`go test -tags=integration ./agent_v2/ -run TestRecordWording`
	// （生产档必须 0 重放 + 反例档必须复现出效应 + 阳性对照必须成立）。**改文案前跑它。**
	for _, bad := range []string{"请重新调用", "重新发起", "重新提交", "重新调用", "再次调用"} {
		if strings.Contains(rec.Content, bad) {
			t.Errorf("记录里不该把「重新发起」摆上桌（实测会招致副作用重放）：命中 %q，全文 %q",
				bad, rec.Content)
		}
	}
	// ⚠️ 这里**不再**断言「必须包含『用户』」。那条断言曾经存在，理由是「决定权要交回
	// 上下文」——听着周全，实测是把唯一不安全的文案锁在了原地：满足它的那一版重放 3/6，
	// 而被它判为不合格的两版（纯事实 / 禁止式）都是 0/6。**没有实测支撑的措辞偏好不该
	// 写成承重断言。**
	// 承重：不引入 tool_call，否则会牵动结构化输出按 final_output 调用计的重试预算
	if len(schema.GetToolCalls(rec)) != 0 {
		t.Errorf("记录那条不该带 tool_call：%v", schema.GetToolCalls(rec))
	}
}

// multiResultMsg 造一条带多个结果块的 tool 消息（业务照 Anthropic 的样子把几个调用的结果合成一条）。
func multiResultMsg(content string, results ...[2]string) *schema.Message {
	m := &schema.Message{ID: "merged", Role: schema.RoleTool, Content: content,
		Annotations: map[string]any{HistoryAgentAnnotation: "x"}}
	for _, r := range results {
		m.ContentBlocks = append(m.ContentBlocks, &schema.ContentBlock{Type: "tool_result",
			Toolresult: &schema.ToolresultContentBlock{ToolCallId: r[0], Content: r[1]}})
	}
	return m
}

// 一条 tool 消息带两个结果块时，两个调用都算有结果：读侧补救逐块配对、把它拆成每块一条（同 openai 适配层发请求时的拆法），
// 不再给第二个调用补占位。曾经只认第一块：b 被判成没有结果、多补一条占位，发出去 b 有两条结果（审计实测）。
func TestDropDangling_MultiResultBlockMessageAnswersEveryCall(t *testing.T) {
	merged := multiResultMsg("", [2]string{"a", "ra"}, [2]string{"b", "rb"})
	in := []*schema.Message{schema.UserMessage("q"), tcMsg(tc("a", "f"), tc("b", "g")), merged}
	if got := danglingToolCalls(in); len(got) != 0 {
		t.Errorf("两个调用都有结果，不该报悬空：%v", got)
	}
	out := dropDanglingToolCalls(in)
	if len(out) != 4 {
		t.Fatalf("应是 user + assistant + 两条单块结果，实际 %d 条：%v", len(out), out)
	}
	for i, want := range []struct{ id, content string }{{"a", "ra"}, {"b", "rb"}} {
		r := out[2+i]
		if r.Role != schema.RoleTool || schema.GetToolResultID(r) != want.id || r.Content != want.content || len(toolResultIDs(r)) != 1 {
			t.Errorf("第 %d 条结果应是 %s 的单块消息、内容 %q，实际 id=%q content=%q 块数=%d", i, want.id, want.content,
				schema.GetToolResultID(r), r.Content, len(toolResultIDs(r)))
		}
		if strings.Contains(r.Content, "未完成") {
			t.Errorf("%s 有真结果，不该补占位", want.id)
		}
	}
	if out[2].ID == out[3].ID {
		t.Errorf("拆出来的两条消息 ID 应不同，都是 %q", out[2].ID)
	}
	if len(merged.ContentBlocks) != 2 || merged.Content != "" {
		t.Error("拆块不该改原消息")
	}
}

// 读 prior 时判「这个窗口够不够」：合并消息里有一块的调用不在窗口里声明（b 的声明更早、在窗口外），窗口不够，要再往前读。
// 曾经只看第一块：a 有声明就算够，b 那块成了孤儿。
func TestPriorWindowSatisfied_EveryResultBlockNeedsItsCall(t *testing.T) {
	win := []*schema.Message{schema.UserMessage("u0"), tcMsg(tc("a", "f")),
		multiResultMsg("", [2]string{"a", "ra"}, [2]string{"b", "rb"})}
	if priorWindowSatisfied(win, 2) {
		t.Error("b 的结果块在窗口里没有声明它的调用，窗口不该算够")
	}
	ok := []*schema.Message{schema.UserMessage("u0"), tcMsg(tc("a", "f"), tc("b", "g")),
		multiResultMsg("", [2]string{"a", "ra"}, [2]string{"b", "rb"})}
	if !priorWindowSatisfied(ok, 2) {
		t.Error("两块都有声明时窗口够")
	}
}

// 拆法同 openai 适配层：第一块带着原消息的文本（原消息有文本时），其余各取自己块的文本；标注各拷一份。
func TestSplitToolResultBlocks_MirrorsOpenAIAdapter(t *testing.T) {
	merged := multiResultMsg("合并的说明", [2]string{"a", "ra"}, [2]string{"b", "rb"})
	pieces := splitToolResultBlocks(merged)
	if len(pieces) != 2 {
		t.Fatalf("应拆成 2 条，实际 %d", len(pieces))
	}
	if pieces[0].Content != "合并的说明" || pieces[1].Content != "rb" {
		t.Errorf("第一条应带原消息的文本、第二条取自己块的，实际 %q / %q", pieces[0].Content, pieces[1].Content)
	}
	pieces[1].Annotations["k"] = "v"
	if _, leaked := merged.Annotations["k"]; leaked {
		t.Error("拆出来的消息改标注不该写回原消息")
	}
	if single := schema.ToolResultMessage("a", "r"); splitToolResultBlocks(single)[0] != single {
		t.Error("只有一个结果块时应原样返回")
	}
}
