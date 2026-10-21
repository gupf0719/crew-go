package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAnswerConsumption_WithinOneExecution 守兑付记账的**第二个来源**：本次执行自己
// 刚兑付的那些。
//
// # 为什么必须单独守
//
// 兑付并入 cm 的时刻是**超步的提交点**（「这一步成了」才算），所以一个节点在**同一次
// 执行内**问两次时，第一问的兑付**还不在 cm 里**。只查 cm 的话，第二问撞了同一个 ID
// 会被同一张票放行——实测：用户只批准「转账 5000」，紧接着的「删除全部历史」跟着执行、
// `err == nil`。
//
// 而这恰恰是自填 ID 最容易撞的形态：一个工具体里前后两个确认，业务顺手写了同一个常量。
//
// 两个来源的生命周期不同、缺一不可：cm 那份跨超步跨 Run 活着；本次这份随执行结束而
// 丢弃——所以重跑时账本是新的，节点能重新读到它上次读过的答案（由
// TestAnswerConsumption_LegitimateRerunStillGetsAnswer 守那一半）。
//
// sanity-revert：把 answerLedger.consumed 里扫 l.ids 那段去掉（只查 cm）→ 本用例红。
func TestAnswerConsumption_WithinOneExecution(t *testing.T) {
	const shared = "SAME"
	var asked, did []string

	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("within_exec")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 第一件事
			if a1, ok := compose_v2.GetResumeAnswer(ctx, shared); ok && a1 != nil {
				did = append(did, "转账 5000")
			} else {
				asked = append(asked, "确认转账 5000")
				return nil, &compose_v2.InterruptError{
					Query: &compose_v2.AskQuery{InterruptID: shared, Question: "确认转账 5000"}}
			}
			// 第二件事——**同一次执行内**，同一个 ID
			if a2, ok := compose_v2.GetResumeAnswer(ctx, shared); ok && a2 != nil {
				did = append(did, "删除全部历史")
			} else {
				asked = append(asked, "确认删除全部历史")
				return nil, &compose_v2.InterruptError{
					Query: &compose_v2.AskQuery{InterruptID: shared, Question: "确认删除全部历史"}}
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "within_sid"

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))); !ok {
		t.Fatal("Run1 该中断在第一件事")
	}
	err2 := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{shared: schema.UserMessage("批准")}))

	t.Logf("问过: %v / 执行过: %v", asked, did)

	// **承重 1**：第二件事没有被同一张票放行
	if len(did) != 1 || did[0] != "转账 5000" {
		t.Errorf("用户只批准了转账，实际执行了 %v\n"+
			"  含「删除全部历史」= 同一次执行内的第二问被第一问的票放行了——"+
			"兑付账本漏了「本次执行自己刚兑付的」这个来源", did)
	}
	// **承重 2**：而且是响的
	if !errors.Is(err2, compose_v2.ErrRunAborted) {
		t.Errorf("落在已兑付位上的新提问应终局失败，实际 err=%v", err2)
	}
	// **承重 3（正向）**：第二问确实被问出来了（而不是整个没走到）
	if len(asked) != 2 {
		t.Errorf("两件事都该被问出来（第二件问出来才谈得上被拦），实际问过 %v", asked)
	}
}
