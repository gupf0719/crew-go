package agent_v2

// 清洗层的消息形状矩阵——**正例反例一起穷举**。
//
// # 为什么要一张表
//
// 这个清洗函数被改了四轮，每一轮都是"上一轮的修法不够"：先只清悬空 tool_call，再加孤儿
// tool_result，再统一两遍判定的顺序规则，最后发现真实约束是「紧跟」、改成按声明重建配对块。
// 每一轮的测试都只盯着当轮那个形状，于是每一轮都留下了下一轮才发现的洞。
//
// 表的价值不在于"多测几个 case"，而在于**把出口契约写死成一条对所有输入都成立的性质**：
//
//	不管喂进去什么形状，出来的流必须是 API 合法的——
//	每条 role=tool 都紧跟在声明它的 assistant 之后，按声明顺序排列，一个不多一个不少。
//
// 有了这条性质，下次再改清洗逻辑时，不需要预判"这次会漏哪个形状"——表里任何一行挂了都是
// 契约破了。
//
// # 断言强度
//
// `assertWireLegal` 验的是**紧跟**（真实 API 的硬约束），不是「声明在前」。后者宽一档，
// 分离态会漏过去——这轮真栽过：结果都在、声明也在前，但中间夹了消息，严格 provider 照样 400。

import (
	"fmt"
	"strings"
	"testing"

	"crew-go/schema"
)

// ── 构造 helper：让每个 case 的形状一眼可读 ──

func mUser(s string) *schema.Message { return schema.UserMessage(s) }
func mAsst(s string) *schema.Message { return schema.AssistantMessage(s) }

// mCall 造一条声明了若干 tool_call 的 assistant（content 可空）。
func mCall(content string, ids ...string) *schema.Message {
	tcs := make([]schema.ToolCall, 0, len(ids))
	for _, id := range ids {
		tcs = append(tcs, schema.ToolCall{
			ID: id, Type: "function",
			Function: schema.FunctionCall{Name: "f_" + id, Arguments: "{}"},
		})
	}
	m := schema.AssistantToolCallMessage(tcs)
	m.Content = content
	return m
}

func mResult(id string) *schema.Message {
	return schema.ToolResultMessage(id, "f_"+id, "result_of_"+id)
}

// mResultWith 造内容可区分的结果——用来验「认领了哪一条」，而不只是「认领了某一条」。
func mResultWith(id, content string) *schema.Message {
	return schema.ToolResultMessage(id, "f_"+id, content)
}

// assertWireLegal 验出口契约：每条带 tool_call 的 assistant 之后，必须**紧跟**回应它全部
// tool_call 的 tool 消息，且顺序与声明一致；流里不得出现其他位置的 role=tool。
//
// 这条比「每个 tool_result 的声明者在它前面」严——后者允许中间夹东西，而真实 API 不允许。
func assertWireLegal(msgs []*schema.Message) error {
	// 一果一用：同一条结果消息不能被两条 assistant 共用。
	//
	// **块内合法 ≠ 整体合法**：一个结果被两条 assistant 共用时，两个块各自看都是「紧跟且
	// id 对得上」，只有跨块看才发现那是**同一条消息**被摆了两遍——后一次调用并没有真的拿到
	// 结果，是捏造出来的。这条断言是变异测试补出来的：去掉认领判断后整张表全绿，正是因为
	// 原来只查块内。
	//
	// 判据是**消息身份**（指针）而不是 tool_call_id：id 会跨轮复用，两轮各自调一次、各自
	// 拿到自己的结果时，同一个 id 出现两次是对的——那是两条不同的消息，不是一条被用两遍。
	// 出口重建时真结果直接引用输入里那条，所以指针相同就是一果多用。
	seen := map[*schema.Message]int{}
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleTool {
			seen[m]++
		}
	}
	for m, n := range seen {
		if n > 1 {
			return fmt.Errorf("同一条结果消息（id=%s 内容=%q）被摆了 %d 次——"+
				"一个结果只能应答一次调用，重复摆放等于给后一次调用捏造了答案",
				schema.GetToolResultID(m), m.Content, n)
		}
	}

	i := 0
	for i < len(msgs) {
		m := msgs[i]
		if m == nil {
			return fmt.Errorf("[%d] nil 消息不该出现在出口", i)
		}
		if m.Role == schema.RoleTool {
			return fmt.Errorf("[%d] 孤立的 tool_result(id=%s)——它前面没有声明它的 assistant",
				i, schema.GetToolResultID(m))
		}
		tcs := schema.GetToolCalls(m)
		if len(tcs) == 0 {
			i++
			continue
		}
		// 紧跟检查
		for j, tc := range tcs {
			k := i + 1 + j
			if k >= len(msgs) {
				return fmt.Errorf("[%d] assistant 声明了 %d 个 tool_call，后面只有 %d 条消息——不够紧跟",
					i, len(tcs), len(msgs)-i-1)
			}
			nxt := msgs[k]
			if nxt == nil || nxt.Role != schema.RoleTool {
				var got string
				if nxt == nil {
					got = "nil"
				} else {
					got = string(nxt.Role)
				}
				return fmt.Errorf("[%d] 的 tool_call 块被打断：第 %d 条应是 tool_result(%s)，实际是 %s",
					i, k, tc.ID, got)
			}
			if id := schema.GetToolResultID(nxt); id != tc.ID {
				return fmt.Errorf("[%d] 的第 %d 个 tool_call 是 %s，但紧跟的结果 id=%s——顺序对不上",
					i, j, tc.ID, id)
			}
		}
		i += 1 + len(tcs)
	}
	return nil
}

// contentsOf 便于断言"哪些内容活下来了"。
func contentsOf(msgs []*schema.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			out = append(out, "<nil>")
			continue
		}
		tag := string(m.Role)
		if tcs := schema.GetToolCalls(m); len(tcs) > 0 {
			ids := make([]string, 0, len(tcs))
			for _, tc := range tcs {
				ids = append(ids, tc.ID)
			}
			tag += "(tc:" + strings.Join(ids, ",") + ")"
		}
		if m.Role == schema.RoleTool {
			tag += "(" + schema.GetToolResultID(m) + ")"
		}
		out = append(out, tag+":"+m.Content)
	}
	return out
}

// TestCleanup_ShapeMatrix：喂各种形状，出口必须恒合法。
//
// 每行还带一个 `keep` 断言，验"不该被误杀的内容确实留下来了"——只验合法性不够，全删光也
// 是合法的。
func TestCleanup_ShapeMatrix(t *testing.T) {
	cases := []struct {
		name string
		in   []*schema.Message
		keep []string // 这些子串必须出现在输出里
		drop []string // 这些子串必须不出现
		why  string
	}{
		// ── 正例：本来就合法的，不能被改坏 ──
		{
			name: "纯文本对话",
			in:   []*schema.Message{mUser("问"), mAsst("答")},
			keep: []string{"问", "答"},
			why:  "没有 tool_call，应原样通过",
		},
		{
			name: "单 tool_call 全配对",
			in:   []*schema.Message{mUser("问"), mCall("", "A"), mResult("A"), mAsst("答")},
			keep: []string{"问", "result_of_A", "答"},
			why:  "最常见的一轮工具调用",
		},
		{
			name: "多 tool_call 全配对且顺序一致",
			in:   []*schema.Message{mCall("", "A", "B"), mResult("A"), mResult("B")},
			keep: []string{"result_of_A", "result_of_B"},
			why:  "fanout 的正常形态（ISSUE-81 起支持 N>=2）",
		},
		{
			name: "多轮全配对",
			in: []*schema.Message{
				mUser("问1"), mCall("", "A"), mResult("A"), mAsst("答1"),
				mUser("问2"), mCall("", "B"), mResult("B"), mAsst("答2"),
			},
			keep: []string{"问1", "result_of_A", "答1", "问2", "result_of_B", "答2"},
			why:  "跨轮不能互相干扰",
		},
		{
			name: "assistant 带文本又带 tool_call",
			in:   []*schema.Message{mCall("我来查一下", "A"), mResult("A")},
			keep: []string{"我来查一下", "result_of_A"},
			why:  "文本和 tool_call 并存是合法的，文本不能丢",
		},

		// ── 反例：坏形状，出口必须修成合法 ──
		{
			name: "孤儿 tool_result（没有任何声明者）",
			in:   []*schema.Message{mUser("问"), mResult("X"), mAsst("答")},
			keep: []string{"问", "答"},
			drop: []string{"result_of_X"},
			why:  "声明者不在流里，留着必 400",
		},
		{
			name: "悬空 tool_call（无结果，有文本）",
			in:   []*schema.Message{mCall("我打算查一下", "A"), mAsst("答")},
			keep: []string{"我打算查一下", "答", "f_A"},
			why: "整轮没落地：文本留着，同时补一条「提过 f_A」的记录——" +
				"只剥 tool_call 的话，动作记录一样丢，只是丢得没那么显眼",
		},
		{
			name: "悬空 tool_call（无结果，无文本）",
			in:   []*schema.Message{mUser("问"), mCall("", "A"), mUser("换个话题")},
			keep: []string{"问", "换个话题", "f_A"},
			drop: []string{"重新调用"},
			why: "纯 tool_call 轮没落地：没有文本可留，但**不能整条丢**——" +
				"那样模型对自己提过这个动作的记录被静默抹掉。换成文本记录，" +
				"且不发重试指令（这类调用可能带副作用、也可能真执行过）",
		},
		{
			name: "部分配对：两个声明只有一个结果",
			in:   []*schema.Message{mCall("", "A", "B"), mResult("A")},
			keep: []string{"result_of_A", "[未完成]"},
			why:  "已拿到的 A 不能被连坐删除，缺的 B 补占位",
		},
		{
			name: "顺序错位：结果排在声明前面",
			in:   []*schema.Message{mResult("A"), mCall("", "A")},
			why:  "错位的结果不算应答（marker 提升 / 业务压缩器都能造出这形状）",
		},
		{
			name: "错位结果与真结果并存——必须认领真的那条",
			in: []*schema.Message{
				mResultWith("A", "上一轮残留的孤儿"),
				mCall("", "A"),
				mResultWith("A", "本轮的真结果"),
			},
			keep: []string{"本轮的真结果"},
			drop: []string{"上一轮残留的孤儿"},
			why: "两种认领方式产出的流都「合法」，差别在认领了谁——" +
				"排在声明前面的那条跟这次调用没有因果关系，用它就是捏造。" +
				"这个 case 是变异测试补的：没有内容差异时，索引不看顺序也能全绿",
		},
		{
			name: "结果被隔开：中间夹了别的消息",
			in:   []*schema.Message{mCall("", "A", "B"), mResult("A"), mAsst("插话"), mResult("B")},
			keep: []string{"result_of_A", "result_of_B"},
			why:  "真实 API 要紧跟；重建配对块后两个结果都该被收拢到 assistant 后面",
		},
		{
			name: "id 复用：两轮用同一个 id，后一轮没结果",
			in:   []*schema.Message{mCall("", "A"), mResult("A"), mCall("", "A")},
			keep: []string{"result_of_A"},
			why: "业务 stub / 固定 id 的 mock 会造出来；第二条 A 没有自己的结果。" +
				"第一轮的声明和结果是真的、必须留住——只验合法性的话，全删光也合法",
		},
		{
			name: "id 复用：两轮各有自己的真结果",
			in: []*schema.Message{
				mCall("", "A"), mResultWith("A", "第一轮的结果"),
				mCall("", "A"), mResultWith("A", "第二轮的结果"),
			},
			keep: []string{"第一轮的结果", "第二轮的结果"},
			drop: []string{"[未完成]"},
			why: "两轮各自调了一次、各自拿到了结果，两条都是真的。" +
				"索引若按 id 只记一条，第二轮的声明就认领不到自己的结果 → 那条 assistant" +
				"「一个真结果都没有」→ 纯 tool_call 轮没有文本可保 → **整条连同它的真结果一起丢**，" +
				"静默、合法、无报错。上一行（后一轮没结果）恰好绕开了这个形状",
		},
		{
			name: "同 id 被两条 assistant 声明",
			in:   []*schema.Message{mCall("先说", "A"), mCall("再说", "A"), mResult("A")},
			keep: []string{"先说", "再说", "result_of_A"},
			why: "一个结果不能同时认领两个声明。但两条 assistant 各有文本、结果也是真的，" +
				"三样都得留——去重时把带文本的那条连同结果一起丢是静默失内容",
		},
		{
			name: "同 id 多个结果",
			in: []*schema.Message{
				mCall("", "A"), mResultWith("A", "先到的结果"), mResultWith("A", "后到的结果"),
			},
			keep: []string{"先到的结果"},
			drop: []string{"后到的结果"},
			why: "重复响应本身不合法，只认第一条。**两条内容必须可区分**——都用同一个 " +
				"result_of_A 的话，实现改成认第二条照样绿，「只认第一条」这句契约根本没被验证",
		},
		{
			name: "空 assistant（无文本无 tool_call）",
			in:   []*schema.Message{mUser("问"), mAsst(""), mUser("再问")},
			keep: []string{"问", "再问"},
			why:  "空 assistant 对模型零信息，但保留不违法——只验不崩不违规",
		},
		{
			name: "nil 元素混入",
			in:   []*schema.Message{mUser("问"), nil, mCall("", "A"), nil, mResult("A")},
			keep: []string{"问", "result_of_A"},
			why:  "上游任何一层漏了 nil 检查都可能带进来，不能 panic",
		},
		{
			name: "空流",
			in:   []*schema.Message{},
			why:  "边界：不能 panic",
		},
		{
			name: "只有一条孤儿",
			in:   []*schema.Message{mResult("X")},
			drop: []string{"result_of_X"},
			why:  "边界：清完变空流",
		},
		{
			name: "多个 assistant 连续声明各自的 tool_call",
			in: []*schema.Message{
				mCall("", "A"), mResult("A"),
				mCall("", "B"), mResult("B"),
			},
			keep: []string{"result_of_A", "result_of_B"},
			why:  "连续两轮工具调用，块与块不能串",
		},
		{
			name: "三个 tool_call 只中间那个有结果",
			in:   []*schema.Message{mCall("", "A", "B", "C"), mResult("B")},
			keep: []string{"result_of_B"},
			why:  "占位要补在正确的位置上（A 和 C 各一条），顺序按声明",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dropDanglingToolCalls(tc.in)

			if err := assertWireLegal(got); err != nil {
				t.Errorf("出口不合法：%v\n  场景：%s\n  输入：%v\n  输出：%v",
					err, tc.why, contentsOf(tc.in), contentsOf(got))
			}

			joined := strings.Join(contentsOf(got), " | ")
			for _, k := range tc.keep {
				if !strings.Contains(joined, k) {
					t.Errorf("%q 被误杀了\n  场景：%s\n  输出：%v", k, tc.why, contentsOf(got))
				}
			}
			for _, d := range tc.drop {
				if strings.Contains(joined, d) {
					t.Errorf("%q 本该被清掉\n  场景：%s\n  输出：%v", d, tc.why, contentsOf(got))
				}
			}
		})
	}
}

// TestCleanup_Idempotent：清洗必须幂等。
//
// 出口流会被再次喂进来（Resume 重跑 preamble、多轮对话把上轮输出当 prior 的一部分）。
// 第二遍如果还改动，说明第一遍没到不动点——那意味着某些形状会在反复清洗中不断退化。
func TestCleanup_Idempotent(t *testing.T) {
	inputs := [][]*schema.Message{
		{mCall("", "A", "B"), mResult("A")},                           // 补过占位的
		{mResult("A"), mCall("", "A")},                                // 错位过的
		{mCall("", "A", "B"), mResult("A"), mAsst("插"), mResult("B")}, // 被隔开过的
		{mCall("先", "A"), mCall("后", "A"), mResult("A")},              // 多重声明
		{mUser("问"), mCall("", "A"), mResult("A"), mAsst("答")},        // 本来就干净的
		{mUser("问"), mCall("", "A"), mUser("换话题")},                    // 零真结果、无文本 → 换成文本记录
		{mCall("有文本", "A"), mAsst("答")},                               // 零真结果、有文本 → 文本后追加记录
	}
	for i, in := range inputs {
		once := dropDanglingToolCalls(in)
		twice := dropDanglingToolCalls(once)
		if len(once) != len(twice) {
			t.Errorf("[%d] 不幂等：第一遍 %d 条、第二遍 %d 条\n  一：%v\n  二：%v",
				i, len(once), len(twice), contentsOf(once), contentsOf(twice))
			continue
		}
		for j := range once {
			if once[j] != twice[j] && contentsOf(once)[j] != contentsOf(twice)[j] {
				t.Errorf("[%d] 不幂等：第 %d 条变了\n  一：%v\n  二：%v",
					i, j, contentsOf(once), contentsOf(twice))
				break
			}
		}
	}
}
