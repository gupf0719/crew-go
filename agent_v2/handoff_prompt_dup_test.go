package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

type dupProbeModel struct {
	reply    string
	captured *[][]*schema.Message
}

func (m *dupProbeModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if m.captured != nil {
		cp := make([]*schema.Message, len(msgs))
		copy(cp, msgs)
		*m.captured = append(*m.captured, cp)
	}
	if m.reply == "TRANSFER" {
		// 走内置 transfer 工具（annotation 通道已删）——本 fixture 的 front 必须是
		// BasicAgent（要捕获它每轮看到的 prompt），所以走 LLM 那条路而不是 HandoffTo。
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_dup", Type: "function",
			Function: schema.FunctionCall{
				Name:      agent_v2.HandoffTransferToolName,
				Arguments: `{"agent_name":"billing"}`,
			},
		}}), nil
	}
	return schema.AssistantMessage(m.reply), nil
}
func (m *dupProbeModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *dupProbeModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 转交之后，后接 agent 收到的 prompt 里用户的问题只该出现一次。
//
// 断言的是**喂给模型的那份**，不是 store。这个区分是要害：nonce 认领保证了 store 里
// 只有一份，所以所有按 store 断言的测试都绿——而模型实际收到的是两份。bug 藏在
// 「读出来的 prior」和「append 上去的 input」之间，两边各自都对。
//
// 机制：`buildInitialMessages` 靠「prior 恒不含 input」（写入点在读之后）决定要不要
// append，而那个不变量只在单 agent 内成立——转交第 2 跳时前一跳已经把同一条 input
// 写进共享历史了。
//
// **sanity-revert**：把 `&& !alreadyInPrior` 去掉，本测试 FAIL 并报出现 2 次。
func TestHandoff_SharedHistory_InputNotDuplicatedInPrompt(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	var billingPrompts [][]*schema.Message

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(&dupProbeModel{reply: "TRANSFER"}),
		agent_v2.WithHandoffTransfer(), // 走工具路径必须显式开

		agent_v2.WithHistory(hs, "acme"), agent_v2.WithMaxRounds(3))
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(&dupProbeModel{reply: "已受理", captured: &billingPrompts}),
		agent_v2.WithHistory(hs, "acme"), agent_v2.WithMaxRounds(3))

	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-dup")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我要退款")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(billingPrompts) == 0 {
		t.Fatal("billing 没被调用——fixture 没造出转交")
	}
	p := billingPrompts[0]
	n := 0
	for _, m := range p {
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "我要退款") {
			n++
		}
	}
	t.Logf("billing 收到的 prompt %d 条：", len(p))
	for i, m := range p {
		t.Logf("  [%d] %-9s %q", i, m.Role, m.Content)
	}
	if n != 1 {
		t.Errorf("后接收到的 prompt 里用户的问题出现 %d 次，want 1——模型会以为用户问了两遍。"+
			"\n  完整 prompt 见上方 log；注意 store 里是对的，所以按 store 断言的测试抓不到", n)
	}

	stored, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "acme", ConvID: "conv-dup"})
	sn := 0
	for _, m := range stored {
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "我要退款") {
			sn++
		}
	}
	// 对照组：store 一直是对的（nonce 认领生效）。两个数字不一致才说明 bug 在读写之间。
	if sn != 1 {
		t.Errorf("store 里存了 %d 份，want 1——这条要是也坏了，说明 nonce 认领那层出了问题", sn)
	}
}
