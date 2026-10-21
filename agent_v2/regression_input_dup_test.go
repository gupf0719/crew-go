package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

// captureMsgsModel 捕获 LLM 每次调用收到的 messages — 让测试断言 input 出现次数。
type captureMsgsModel struct {
	mu       sync.Mutex
	allMsgs  [][]*schema.Message
	response *schema.Message
}

func (m *captureMsgsModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := make([]*schema.Message, len(msgs))
	copy(clone, msgs)
	m.allMsgs = append(m.allMsgs, clone)
	if m.response != nil {
		return m.response, nil
	}
	return schema.AssistantMessage("ok"), nil
}
func (m *captureMsgsModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *captureMsgsModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// countUserContent 数 messages 中含特定 user content 的次数。
func countUserContent(msgs []*schema.Message, content string) int {
	n := 0
	for _, m := range msgs {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleUser && strings.Contains(m.Content, content) {
			n++
		}
	}
	return n
}

// TestBasicAgent_R29_HIGH1_NoInputDup 是 R29 audit HIGH-1 (ISSUE-74 regression)
// 回归——验证 BasicAgent 跑 N 轮时 user input 在 LLM messages 中只出现 1 次（不重复）。
//
// **Bug 历史**：ISSUE-74 commit b07c140 把 buildInitialMessages 移到 preamble 节点
// （runGraph 内）跑——但忘了 BasicAgent.Run 入口 line 153 的 AddEvent(input) 仍在
// runGraph **之前**：
//   - line 153: session.AddEvent(NewOutputEvent(name, input)) — sess.events 已含 input
//   - line 549 (in buildInitialMessages, in preamble node): prior = SelfHistoryCompacted — prior 含 input
//   - line 573-574: msgs = append(msgs, prior...) + msgs = append(msgs, input) — input 重复 2 次
//
// LLM 看到 user input 重复 2 次——除浪费 token 外，跨 Run prior 含 listing 时还会
// 把 listing 错位到 user/asst 之间不在 sys 段，破坏 4 层压缩 splitProtectedGroups 配对语义。
//
// **修法**：AddEvent(input) 推迟到 buildInitialMessages 末尾（与 R14 listing 推迟同模式）。
//
// **诊断 sanity-revert**：把 line 153 AddEvent 还原 → 测试应 fail 报"input 出现 2 次"。
func TestBasicAgent_R29_HIGH1_NoInputDup(t *testing.T) {
	stub := &captureMsgsModel{response: schema.AssistantMessage("ans")}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(2),
	)

	sess := agent_v2.NewRunSession()

	// Run 1: input "q1"
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("q1"))
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if len(stub.allMsgs) < 1 {
		t.Fatalf("stub 应被调 >= 1 次")
	}
	got := countUserContent(stub.allMsgs[0], "q1")
	if got != 1 {
		t.Errorf("R29 HIGH-1 回归：Run 1 user input \"q1\" 应只出现 1 次；实际 %d 次（messages: %d 条）\n"+
			"sanity-revert 还原 line 153 AddEvent(input) 提前 → 应 fail 报 2 次", got, len(stub.allMsgs[0]))
	}

	// Run 2 同 sess: input "q2"，prior 应含 q1+ans，input q2 仅 1 次
	_, err = ag.Run(context.Background(), sess, schema.UserMessage("q2"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if len(stub.allMsgs) < 2 {
		t.Fatalf("stub 应被调 >= 2 次")
	}
	gotQ1 := countUserContent(stub.allMsgs[1], "q1")
	gotQ2 := countUserContent(stub.allMsgs[1], "q2")
	if gotQ1 != 1 {
		t.Errorf("R29 HIGH-1 回归：Run 2 prior 应含 q1 1 次；实际 %d 次", gotQ1)
	}
	if gotQ2 != 1 {
		t.Errorf("R29 HIGH-1 回归：Run 2 当前 input q2 应 1 次；实际 %d 次", gotQ2)
	}
}
