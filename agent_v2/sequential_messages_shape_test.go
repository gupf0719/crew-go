package agent_v2_test

// SequentialAgent 累积消息流的形状：**只记实际发生的事，不记派生的中间输入**。
//
// # 为什么钉这条
//
// step 1+ 的输入是框架用模板拼的（"原始目标：…\n\n上一步（X）的产出：…"）。把它一起
// 写进累积流会产生两个后果：
//
//  1. **每条产出被复述两遍** —— 一次作为本步的 assistant 产出、一次嵌在下一步的 user
//     模板里。N 步的链上，前 N-1 条产出各有一份逐字副本。
//  2. **角色错位** —— 派生模板是 user 角色，于是"上一步 agent 说的话"以用户的口吻在流里
//     再现一次。消费这份流的人（OnAgentEnd 的 out.Messages、以及将来的历史层）会读到一段
//     用户从没说过的话。
//
// 派生输入随时能从"原始目标 + 上一步产出"重算，不该占据消息流。
//
// step 0 是**例外且必须保留**：它的输入就是 origMsg 本身，是真实的用户输入不是派生物。
// 丢了它，整条流就没有开头——谁都不知道这轮在回答什么问题。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// seqShapeCapture 抓 OnAgentEnd 的 out.Messages —— 那正是 seqAgentMessagesCh 的全量
// （sequential_agent.go:232/326 `finalMessagesRef = allMsgs`），是这条 channel 唯一
// 流向业务的出口。
type seqShapeCapture struct {
	hook.NoOpHooks
	msgs []*schema.Message
}

func (c *seqShapeCapture) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	if out != nil {
		c.msgs = append(c.msgs, out.Messages...)
	}
	return ctx
}

func TestSequential_Messages_OnlyProducts_NotDerivedInputs(t *testing.T) {
	cap := &seqShapeCapture{}
	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("shape_seq"),
		agent_v2.WithSequentialHooks(cap),
	).
		AddStep("step1", agent_v2.NewBasicAgent(
			agent_v2.WithName("step1"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantMessage("S1_OUT_MARK"),
			})))).
		AddStep("step2", agent_v2.NewBasicAgent(
			agent_v2.WithName("step2"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantMessage("S2_OUT_MARK"),
			}))))

	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("ORIGINAL_GOAL_MARK")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 期望形状恰好三条：原始 user 输入 + 两步产出
	want := []struct {
		role schema.MessageRole
		mark string
	}{
		{schema.RoleUser, "ORIGINAL_GOAL_MARK"},
		{schema.RoleAssistant, "S1_OUT_MARK"},
		{schema.RoleAssistant, "S2_OUT_MARK"},
	}
	if len(cap.msgs) != len(want) {
		t.Fatalf("累积消息数 = %d, want %d —— 多出来的几乎肯定是派生的 stepInput 模板\n%s",
			len(cap.msgs), len(want), dumpSeqMsgs(cap.msgs))
	}
	for i, w := range want {
		got := cap.msgs[i]
		if got.Role != w.role || !strings.Contains(got.Content, w.mark) {
			t.Errorf("[%d] = (%s, %q), want role=%s 且含 %q\n%s",
				i, got.Role, got.Content, w.role, w.mark, dumpSeqMsgs(cap.msgs))
		}
	}

	// 派生模板的特征串一次都不该出现 —— 它是 fmt.Sprintf 的字面前缀，
	// 业务产出里不可能自然含有
	for i, m := range cap.msgs {
		if strings.Contains(m.Content, "上一步（") {
			t.Errorf("[%d] 混进了派生的 stepInput 模板：%q", i, m.Content)
		}
	}

	// step1 的产出**恰好出现一次**。写 stepInput 时它会出现两次：
	// 一次作为 step1 的 assistant 产出、一次嵌在 step2 的 user 模板里。
	if n := countSeqMark(cap.msgs, "S1_OUT_MARK"); n != 1 {
		t.Errorf("S1_OUT_MARK 出现 %d 次, want 1 —— >1 说明产出被复述了\n%s",
			n, dumpSeqMsgs(cap.msgs))
	}
}

// TestSequential_Messages_SingleStep_KeepsUserInput：单步链上，唯一的 stepInput
// 就是真实用户输入。若"不记 stepInput"被实现成无条件跳过，这条流会丢掉开头，
// 只剩一条孤零零的 assistant 产出——没人知道它在回答什么。
func TestSequential_Messages_SingleStep_KeepsUserInput(t *testing.T) {
	cap := &seqShapeCapture{}
	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("shape_seq_one"),
		agent_v2.WithSequentialHooks(cap),
	).AddStep("only", agent_v2.NewBasicAgent(
		agent_v2.WithName("only"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("ONLY_OUT_MARK"),
		}))))

	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("SOLE_GOAL_MARK")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(cap.msgs) != 2 {
		t.Fatalf("累积消息数 = %d, want 2（user 输入 + 产出）\n%s",
			len(cap.msgs), dumpSeqMsgs(cap.msgs))
	}
	if cap.msgs[0].Role != schema.RoleUser || !strings.Contains(cap.msgs[0].Content, "SOLE_GOAL_MARK") {
		t.Errorf("首条应是真实用户输入，got (%s, %q) —— 它不是派生物，不该被一起跳过",
			cap.msgs[0].Role, cap.msgs[0].Content)
	}
}

func countSeqMark(msgs []*schema.Message, mark string) int {
	n := 0
	for _, m := range msgs {
		if m != nil && strings.Contains(m.Content, mark) {
			n++
		}
	}
	return n
}

func dumpSeqMsgs(msgs []*schema.Message) string {
	var b strings.Builder
	b.WriteString("实际累积：\n")
	for i, m := range msgs {
		if m == nil {
			b.WriteString("  [")
			b.WriteString(string(rune('0' + i)))
			b.WriteString("] <nil>\n")
			continue
		}
		b.WriteString("  [")
		b.WriteString(string(rune('0' + i)))
		b.WriteString("] ")
		b.WriteString(string(m.Role))
		b.WriteString(": ")
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	return b.String()
}
