package agent_v2

import (
	"strings"
	"testing"

	"crew-go/schema"
)

// TestUnresolvedRecord_AllCallsListed 钉住「N 个声明要全部出现在记录里」。
//
// **为什么单列**：这是本机制要救的**主形态**——`ApprovalAsk` 下 fanout N 个工具由
// `MergeInterrupts` 一次问完，用户放弃 ⟹ N 个 tool_call 全都没有结果。只报第一个的话，
// 剩下 N-1 个动作照旧被静默抹掉，正是本机制要修的病。
//
// 变异验证：把 `describeUnresolvedCalls(tcs)` 改成 `(tcs[:1])`，**全套测试 0 FAIL**
// ——这条用例补的就是那个洞。
func TestUnresolvedRecord_AllCallsListed(t *testing.T) {
	in := []*schema.Message{
		schema.UserMessage("转 5000 并且发通知"),
		tcMsg(
			tc("c1", "transfer_money"),
			tc("c2", "send_notice"),
			tc("c3", "write_audit_log"),
		),
		schema.UserMessage("先别转了"),
	}
	out := dropDanglingToolCalls(in)

	var rec *schema.Message
	for _, m := range out {
		if IsUnresolvedCallsRecord(m) {
			rec = m
			break
		}
	}
	if rec == nil {
		t.Fatal("没有生成记账文本")
	}
	for _, name := range []string{"transfer_money", "send_notice", "write_audit_log"} {
		if !strings.Contains(rec.Content, name) {
			t.Errorf("记录漏了 %q——那个动作对模型来说等于没发生过：%q", name, rec.Content)
		}
	}
}

// TestUnresolvedRecord_ArgsRenderedAndTruncated 钉住参数渲染与截断。
//
// **参数是承重的**：用户说「先别转了」时，模型要能答上「刚才那笔是 5000 到 acct-9」。
// 只有工具名没有参数，记录就退化成「提过某个动作」，答不出是哪一笔。
//
// 截断同样承重：参数由模型产出、长度不设限，而这个分支恰恰出现在历史被压缩截断之后
// （上下文最紧张时）。按**字符**不按字节切——按字节会把多字节字符劈开，写进 prompt 是乱码。
//
// 变异验证：整块删掉参数渲染（连 `unresolvedCallArgMaxChars` 一起），**全套 0 FAIL**。
func TestUnresolvedRecord_ArgsRenderedAndTruncated(t *testing.T) {
	t.Run("参数进记录", func(t *testing.T) {
		got := describeUnresolvedCalls([]schema.ToolCall{
			tc2("c1", "transfer_money", `{"to":"acct-9","amount":5000}`),
		})
		for _, want := range []string{"transfer_money", "acct-9", "5000"} {
			if !strings.Contains(got, want) {
				t.Errorf("记录里缺 %q，模型答不出「刚才那笔是哪一笔」：%q", want, got)
			}
		}
	})

	t.Run("超长参数按字符截断且不劈开多字节", func(t *testing.T) {
		long := `{"note":"` + strings.Repeat("中", 500) + `"}`
		got := describeUnresolvedCalls([]schema.ToolCall{tc2("c1", "f", long)})

		if !strings.Contains(got, "…") {
			t.Fatalf("超长参数没被截断：%q", got)
		}
		// 承重：按字节切会劈开多字节字符 → 出现 U+FFFD 替换符
		if strings.ContainsRune(got, '�') {
			t.Error("截断劈开了多字节字符（按字节切而非按字符切），写进 prompt 是乱码")
		}
		// 承重：确实截到了上限附近，而不是原样透出
		if n := len([]rune(got)); n > unresolvedCallArgMaxChars+200 {
			t.Errorf("截断没生效，记录长 %d 字符", n)
		}
	})

	t.Run("空参数不产生空括号", func(t *testing.T) {
		got := describeUnresolvedCalls([]schema.ToolCall{tc2("c1", "noargs", "")})
		if strings.Contains(got, "()") {
			t.Errorf("空参数不该渲染成空括号：%q", got)
		}
		if !strings.Contains(got, "noargs") {
			t.Errorf("工具名丢了：%q", got)
		}
	})
}

// tc2 是带自定义参数的 tool_call 构造器（tc 的参数恒为 "{}"）。
func tc2(id, name, args string) schema.ToolCall {
	return schema.ToolCall{
		ID:       id,
		Type:     "function",
		Function: schema.FunctionCall{Name: name, Arguments: args},
	}
}
