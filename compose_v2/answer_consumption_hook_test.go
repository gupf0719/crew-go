package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// hookAskHooks 在 `OnNodeStart` 相位提问——**框架里唯一还强制业务自填 ID 的地方**
// （那一层框架一维身份都不给，业务只能编常量），也就是最容易撞 ID 的地方。
type hookAskHooks struct {
	compose_v2.NoOpOrchestrationHooks
	id    string
	qs    []string // 每次进节点问哪一个（按调用序）
	calls int
	asked *[]string
	did   *[]string
}

func (h *hookAskHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id != "n" {
		return ctx, nil
	}
	i := h.calls
	h.calls++
	if i >= len(h.qs) {
		return ctx, nil
	}
	q := h.qs[i]
	if ans, ok := compose_v2.GetResumeAnswer(ctx, h.id); ok && ans != nil {
		*h.did = append(*h.did, q)
		return ctx, nil
	}
	*h.asked = append(*h.asked, q)
	return ctx, &compose_v2.InterruptError{
		Query: &compose_v2.AskQuery{InterruptID: h.id, Question: q},
	}
}

// TestAnswerConsumption_HookPhaseCovered 钉住「兑付记账在 **OnNodeStart 相位**同样生效」。
//
// # 为什么这条要单独钉
//
// 账本挂在 ctx 上，而 ctx 是**一路构造下来**的——注入点晚一步，hook 那条路就整条绕过：
// 记不上（兑付无人知道）、也拦不住（撞了不报错）。审计实测过绕过时的样子：用户只批准
// 「确认转账 5000」，「确认删除全部历史」跟着执行、`err == nil`。
//
// 而 hook 相位恰恰是**最需要这道兜底**的地方：框架在那一层不给任何身份维，业务只能自填
// 常量 ID（仓里的例子就是 `"pause"` 这种），撞 ID 的概率最高。
//
// **这是同一个教训的第二次**：同一个函数、同一条早退路径上，上一轮空 ID 校验漏的也是
// OnNodeStart。所以这条哨兵守的不只是「这次修对了」，是「下次谁再往节点 ctx 上加东西，
// 别又忘了 hook 相位在它前面」。
//
// sanity-revert：把 graph_loop 里 `withAnswerLedger` 的注入挪回 `nodeCtx` 构造那一段
// （即 OnNodeStart 之后）→ 本用例红，且红的样子正是「两件事都执行了、err=nil」。
func TestAnswerConsumption_HookPhaseCovered(t *testing.T) {
	var asked, did []string
	const shared = "pause" // 业务在 hook 相位只能编常量——仓里的例子就是这个形态

	mk := func() (*compose_v2.Graph, *hookAskHooks) {
		b := compose_v2.NewBuilder().WithName("hook_ask")
		loop := compose_v2.Sum("__hook_loop__")
		compose_v2.WithChannel(b, loop)
		b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(loop, 1)}, nil
		})
		b.AddNode("tail", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("tail", compose_v2.EndNode)
		b.AddConditionalEdge("n", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			if v, _ := compose_v2.Get(chs, loop); v >= 2 {
				return "tail", nil
			}
			return "n", nil // 回边：节点被第二次访问，hook 问第二件事
		})
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g, &hookAskHooks{
			id:    shared,
			qs:    []string{"确认转账 5000", "确认删除全部历史"},
			asked: &asked, did: &did,
		}
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "hook_ask_sid"

	g1, h1 := mk()
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g1, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithHooks(h1))); !ok {
		t.Fatal("Run1 该在 hook 相位中断")
	}

	// 用户只答了第一件事
	g2, h2 := mk()
	err2 := compose_v2.Run(ctx, g2, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithHooks(h2),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{shared: schema.UserMessage("批准")}))

	t.Logf("问过: %v / 执行过: %v", asked, did)
	t.Logf("err = %v", err2)

	// **承重 1**：第二件事没有被同一张票放行
	if len(did) > 1 {
		t.Errorf("用户只批准了第一件事，hook 相位却放行了 %d 件：%v\n"+
			"  = 账本的注入晚于 OnNodeStart，hook 那条路整条绕过了兑付记账", len(did), did)
	}
	// **承重 2**：而且是响的
	if !errors.Is(err2, compose_v2.ErrRunAborted) {
		t.Errorf("hook 相位落在已兑付位上的新提问应终局失败，实际 err=%v", err2)
	}
}
