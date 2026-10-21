package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAnswerConsumption_NestedBindsChildLayer 钉住「嵌套子图里，账本绑的是**子层的 cm**」。
//
// # 为什么要钉
//
// 账本挂在 ctx 上，而 ctx 会被子图继承——绑错层的两种后果都真实：绑父层的话，子层的
// 兑付记进父的账本（归属错），而子层从父那儿继承来的历史又会让子图的**合法首问**被
// 误判成已兑付。每层各自一份、跟 cp 的 `(sid, ns)` 分层对齐，才是对的。
//
// 判别式用「子图内同一次执行问两次同一个自填 ID」：绑对层的话第二问在**本层**被拦，
// 错误里带得出 `ns "kid"`；绑错层的话行为会跟父层历史耦合、定位信息也指错。
//
// ⚠️ **顺带记下一个已知边界**（不是本用例守的）：兑付集合**按层分开** ⟹ 同一个自填常量
// ID 在父层和子层能**各兑付一次**。走 `Ask` 的身份含 graphNS、跨层不可能撞；只有业务
// 自填常量 ID 且父子层用了同一个常量才踩得到。见 doc/code/todo.md。
func TestAnswerConsumption_NestedBindsChildLayer(t *testing.T) {
	const shared = "CHILD_SLOT"
	var childAsked, childDid int

	// **两问必须落在两次不同的执行里**——这是本用例承重的那一维。
	//
	// 放在同一次执行内的话，扛事的是账本里「本次执行刚兑付的」那一半（`l.ids`），
	// 跟账本绑哪层 cm **毫无关系**——那样这条哨兵就是假绿的（写过一版，独立审计实测
	// 证明它在「账本绑父层 cm」这个变异下照样绿）。用子图内的回边把第二问推到下一次
	// 执行，才逼它去查**并入 cm 的那一半**，也才谈得上「绑的是哪一层的 cm」。
	round := compose_v2.Sum("__nl_round__")
	sub := compose_v2.NewBuilder().WithName("nl_child")
	compose_v2.WithChannel(sub, round)
	sub.AddNode("c", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		r, _ := compose_v2.Get(chs, round)
		q := []string{"子图第一件事", "子图第二件事"}[min(r, 1)]
		if a, ok := compose_v2.GetResumeAnswer(ctx, shared); ok && a != nil {
			childDid++
			return []compose_v2.ChannelWrite{compose_v2.Write(round, 1)}, nil
		}
		childAsked++
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: shared, Question: q}}
	})
	sub.AddNode("ctail", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "c")
	sub.AddEdge("ctail", compose_v2.EndNode)
	sub.AddConditionalEdge("c", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if r, _ := compose_v2.Get(chs, round); r >= 2 {
			return "ctail", nil
		}
		return "c", nil // 回边：第二问落在**下一次执行**
	})
	subG, err := sub.Compile()
	if err != nil {
		t.Fatal(err)
	}

	b := compose_v2.NewBuilder().WithName("nl_parent")
	b.AddSubgraphNode("kid", subG)
	b.AddEdge(compose_v2.StartNode, "kid")
	b.AddEdge("kid", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "nl_sid"

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))); !ok {
		t.Fatal("Run1 该中断")
	}
	err2 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{shared: schema.UserMessage("批准")}))

	t.Logf("子图问过 %d 次 / 兑付 %d 次", childAsked, childDid)
	t.Logf("err = %v", err2)
	// **承重 1**：同一张票在子图内只兑付一次
	if childDid != 1 {
		t.Errorf("同一张票在子图内被兑付了 %d 次（应为 1）——同一次执行内的第二问没被拦", childDid)
	}
	// **承重 2**：两件事都被问出来了（第二件问出来才谈得上被拦）
	if childAsked != 2 {
		t.Errorf("子图该问出 2 件事，实际 %d", childAsked)
	}
	// **承重 3**：错误必须是终局失败——静默放行（err=nil）正是账本在跨执行这一维失效的样子
	if !errors.Is(err2, compose_v2.ErrRunAborted) {
		t.Errorf("第二问落在已兑付位上应终局失败，实际 err=%v\n"+
			"  err=nil 且执行了两件 = 跨执行那一半失效：ids 写进一层 cm、consumed 读另一层，两边错开", err2)
	}
	// 子层的 consumed 应落在子层 cp
	kid, _ := cper.LoadNS(ctx, sid, "kid")
	root, _ := cper.LoadNS(ctx, sid, "")
	if kid != nil {
		t.Logf("子 cp(ns=kid).ConsumedAnswers = %v", kid.ConsumedAnswers)
	}
	if root != nil {
		t.Logf("父 cp(ns=\"\").ConsumedAnswers = %v", root.ConsumedAnswers)
	}
}
