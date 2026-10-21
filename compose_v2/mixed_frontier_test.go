package compose_v2_test

// 混合 frontier：同一个 NodeID 在一个超步里**同时**有静态边实例（身份空）和
// SendFrom 派生实例（带 token）。
//
// ISSUE-96 用「有 InstanceID ⟹ 忽略 visit」修掉了 fan-out 实例的 ID 漂移，但那条
// 规则的判据是**中断成员自己**有没有身份——而 `visitCount` 是 per-NodeID **共享**的：
// 只要同 NodeID 有任何 fan-out 兄弟成功，计数器就被推高，此时**空身份成员**照样走
// visit 维度，照样漂。

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestMixedFrontier_StaticInstanceVisitNotPollutedBySiblings：X 同时是静态边目标
// 和 SendFrom 目标 → frontier = [{X,""}, {X,i1}, {X,i2}]。静态那个中断、两个兄弟
// 成功，Resume 时静态实例算出的 InterruptID 必须与 Run1 一致。
//
// **为什么要屏障**：静态实例在节点入口读 visit、兄弟的推进发生在本超步的提交点，
// 谁先谁后由调度决定——不加屏障时静态实例可能恰好读到已被推高的值而"碰巧对上"，
// 测试变 flaky。屏障强制"静态先读、兄弟后增"这个必然会发生的时序。
//
// sanity-revert：把 advanceVisitsLocked（channel_manager.go）里那个 visit 自增循环里的 instanceID
// 守卫去掉（退回无条件递增）→ Run1 的 visit=0 在 Resume 时变成 2 → 两次算出不同 ID
// → 答案永远配不上，HITL 死循环。
//
// ⚠️ **改动 visit 推进点时这条会跟着松紧**：推进点曾短暂被放在「本超步全员成功」那段，
// 那时部分失败超步（正是本用例的形态）根本走不到它 ⟹ 守卫在不在都测不出差别，本用例
// 退化成恒真（审计变异实测 PASS）。推进点回到「写入提交成功那一刻」后它重新承重。
// 结论：**这条哨兵的有效性依赖推进点位于「失败 return 之前」**——挪动它时一并重跑本条。
func TestMixedFrontier_StaticInstanceVisitNotPollutedBySiblings(t *testing.T) {
	staticRead := make(chan struct{}) // 静态实例读完 visit 的信号
	var closeOnce atomic.Bool
	var siblingRuns atomic.Int32

	b := compose_v2.NewBuilder().WithName("mixed")
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "X"},
			compose_v2.SendDirective{Target: "X"},
		)
		return nil, nil
	})
	b.AddNode("X", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if compose_v2.GetCurrentInstanceID(ctx) != "" {
			<-staticRead // 兄弟等静态实例先读完 visit
			siblingRuns.Add(1)
			return nil, nil
		}
		site := compose_v2.CurrentInterruptSite(ctx)
		if closeOnce.CompareAndSwap(false, true) {
			defer close(staticRead)
		}
		id := site.InterruptID()
		if _, answered := compose_v2.GetResumeAnswer(ctx, id); answered {
			return nil, nil
		}
		return nil, &compose_v2.InterruptError{
			Reason: "mixed",
			Query:  &compose_v2.AskQuery{InterruptID: id, Question: "?"},
		}
	})
	b.AddEdge(compose_v2.StartNode, "A")
	b.AddEdge("A", "X") // X 既是静态边目标、又是 SendFrom 目标
	b.AddEdge("X", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	err1 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("mixed_sid"))
	ie, ok := compose_v2.IsInterrupt(err1)
	if !ok {
		t.Fatalf("Run1 应中断在静态实例, got %v", err1)
	}
	askedID := ie.Query.InterruptID
	if got := siblingRuns.Load(); got != 2 {
		t.Fatalf("两个 fan-out 兄弟都该跑完, 实际 %d", got)
	}

	// 带着 Run1 问的那个 ID 的答案 Resume——ID 若漂了就配不上
	err2 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("mixed_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			askedID: schema.UserMessage("go"),
		}))
	if ie2, isInt := compose_v2.IsInterrupt(err2); isInt {
		t.Fatalf("答案配不上——Run1 问 %s，Resume 又问 %s\n"+
			"  visitCount 是 per-NodeID 共享的：fan-out 兄弟成功后把它推高，\n"+
			"  空身份成员再读就是另一个值 → ID 漂移 → 每次 Resume 都重新提问，HITL 永不收敛",
			askedID, ie2.Query.InterruptID)
	}
	if err2 != nil {
		t.Fatalf("Resume 应成功, got %v", err2)
	}
}
