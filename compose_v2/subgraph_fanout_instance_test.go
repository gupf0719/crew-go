package compose_v2_test

// ISSUE-96 侧面 2 正面回归：同一个 subgraph 节点经**同-Target SendFrom** fan-out 成
// N 个实例 + 配 Checkpointer 时，各实例的子 cp 必须**物理分离**。
//
// # 本文件曾经验的是相反的事
//
// 原 TestSubgraphFanoutSameTargetRejectedWithCheckpointer（已删）验的是 MED-3 fail-fast
// **拒绝**这个用法——因为当年 childNS 由 branchIdx（frontier 里的位置）派生，Resume 时
// frontier 缩成失败子集、位置重排 → 子 cp namespace 对不上 → 静默丢进度。
//
// ISSUE-96 后 childNS 改由**实例身份**派生（fresh-mint、随 cp 持久化、跨 Resume 稳定），
// 那个理由不再成立，guard 已拆、能力放开。本文件相应从"验拒绝"反转为"验隔离"。
//
// 顺带记：那个 guard 实测**根本没拦住**（侧面 5）——① 只拦 branchIdx>0，实例 0 照跑；
// ② guard 的错只进 FailedBranches、会被兄弟的 InterruptError 顶掉；③ Resume 时 frontier
// 缩成 1 → 走 executeFrontier fast path（硬编码 branchIdx=0）→ 不触发。净效果是 Run1 报
// 一次、一次 Resume 就走过去、最终 err==nil。**拆掉它不损失任何现有保证。**
//
// sanity-revert：删 subgraph.go 里 `if fanoutIdx, ok := instanceFanoutIndex(...)` 那个
// 分支（退回只用 nodeID 派生 childNS）→ 两个实例撞同一个 ns → 本文件两条都 fail。

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestSubgraphFanoutSameTarget_InstancesGetSeparateCheckpoints：2 个 subgraph 实例
// 各自中断 → 必须存下**两份**子 cp（各带自己的待答问题），而非互相覆盖只剩一份。
func TestSubgraphFanoutSameTarget_InstancesGetSeparateCheckpoints(t *testing.T) {
	var innerRuns atomic.Int32

	// 子图：内部中断走 `compose_v2.Ask`——它按执行位置算 ID，实例身份那一维会把两个
	// 并发实例分开（这正是本用例要验的「实例各自独立」的另一面）。
	//
	// ⚠️ 原先是「不自填 ID 让框架 auto-fill」，那条路已被拒。
	sb := compose_v2.NewBuilder().WithName("i96_sub")
	sb.AddNode("inner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		innerRuns.Add(1)
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Reason: "sub asks", Question: "sub question?",
		})
		return nil, err
	})
	sb.AddEdge(compose_v2.StartNode, "inner")
	sb.AddEdge("inner", compose_v2.EndNode)
	sub, err := sb.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	// 父图：split 把同一个 subgraph 节点投递两次 → 2 个并发实例
	b := compose_v2.NewBuilder().WithName("i96_parent")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "sub"},
			compose_v2.SendDirective{Target: "sub"},
		)
		return nil, nil
	})
	b.AddSubgraphNode("sub", sub)
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("sub", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("i96_sub_sid"))
	if _, ok := compose_v2.IsInterrupt(runErr); !ok {
		t.Fatalf("两个子图实例都该中断, got %v", runErr)
	}
	if n := innerRuns.Load(); n != 2 {
		t.Fatalf("两个实例都该跑（能力已放开、不再 fail-fast 拒绝）, got %d 次", n)
	}

	// 核心断言：两个实例各存一份子 cp（ns 不同）
	all, err := cper.LoadAllInSession(ctx, "i96_sub_sid")
	if err != nil {
		t.Fatalf("LoadAllInSession: %v", err)
	}
	subNS := map[string]int{}
	for _, c := range all {
		if strings.Contains(c.NS, "sub") {
			subNS[c.NS] = len(c.Checkpoint.PendingInterrupts)
		}
	}
	if len(subNS) != 2 {
		t.Errorf("2 个 subgraph 实例应各存一份子 cp（ns 物理分离），实际 %d 份: %v\n"+
			"  → childNS 不含实例维度时两个实例撞同一个 ns、后写的覆盖先写的，"+
			"一个实例的进度和它的 HITL 问题**静默蒸发**（ISSUE-96 侧面 2）",
			len(subNS), subNS)
	}
	for ns, pending := range subNS {
		if pending != 1 {
			t.Errorf("子 cp %q 应带 1 条待答 query, got %d", ns, pending)
		}
	}
	t.Logf("2 个实例的子 cp: %v", subNS)
}

// TestSubgraphFanoutSameTarget_BothInstancesResumable：两个实例的问题都能被答完——
// 验隔离不只是"存下来了"，而是真能各自续跑（此前一个实例的档被覆盖 → 永远回不来）。
func TestSubgraphFanoutSameTarget_BothInstancesResumable(t *testing.T) {
	var innerCompleted atomic.Int32

	sb := compose_v2.NewBuilder().WithName("i96_sub2")
	sb.AddNode("inner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 一次 interrupt 只取一次 site（内部 NextInterruptIndex 每调递增）
		id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
		if _, ok := compose_v2.GetResumeAnswer(ctx, id); ok {
			innerCompleted.Add(1)
			return nil, nil
		}
		return nil, &compose_v2.InterruptError{
			Reason: "sub asks",
			Query:  &compose_v2.AskQuery{InterruptID: id, Question: "sub question?"},
		}
	})
	sb.AddEdge(compose_v2.StartNode, "inner")
	sb.AddEdge("inner", compose_v2.EndNode)
	sub, err := sb.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	b := compose_v2.NewBuilder().WithName("i96_parent2")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "sub"},
			compose_v2.SendDirective{Target: "sub"})
		return nil, nil
	})
	b.AddSubgraphNode("sub", sub)
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("sub", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)

	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("i96_sub2_sid"))
	if _, ok := compose_v2.IsInterrupt(runErr); !ok {
		t.Fatalf("应中断, got %v", runErr)
	}
	// 从各子 cp 收集全部待答 ID（父 ie 只带得到冒泡上来的那部分）
	answers := map[string]*schema.Message{}
	all, _ := cper.LoadAllInSession(ctx, "i96_sub2_sid")
	for _, c := range all {
		for _, q := range c.Checkpoint.PendingInterrupts {
			answers[q.InterruptID] = schema.UserMessage("ok")
		}
	}
	t.Logf("收集到 %d 个待答 ID", len(answers))
	if len(answers) != 2 {
		t.Fatalf("2 个实例应各有 1 个待答问题, got %d —— 少了说明有实例的档被覆盖", len(answers))
	}

	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("i96_sub2_sid"),
		compose_v2.WithResumeAnswers(answers)); err != nil {
		t.Fatalf("带全部答案 Resume 应成功, got %v", err)
	}
	if n := innerCompleted.Load(); n != 2 {
		t.Errorf("两个实例都该认领到自己的答案并完成, got %d", n)
	}
}
