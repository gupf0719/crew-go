package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 业务 hook 返回的 ctx 会**顶替**框架的 ctx——这两条钉住「框架自己的 ctx 值不由业务
// hook 决定存亡」。
//
// # 为什么要钉
//
// 有一批东西框架必须在 `OnNodeStart` **之前**注入（观测层要在 hook 里读实例身份给
// span 分槽；答案兑付账本要覆盖 hook 相位的提问——那一层框架一维身份都不给、业务只能
// 自填常量 ID，最容易撞）。而 hook 返回的 ctx 直接赋回主链，于是业务只要返一个**不是
// 从入参派生**的 ctx，这些就**全部静默消失**：
//
//   - 实例身份没了 ⟹ N 个并发实例落同一个 span 槽 / 子 cp 互覆 / InterruptID 撞车
//   - 兑付账本没了 ⟹ 「一个答案只能满足一个提问」对这个节点整条失效
//
// 而业务这么写一点都不离奇：从自己的池子里取 ctx、或者干脆 `context.Background()`。
//
// sanity-revert：把 graph_loop 里 OnNodeStart 那次 CallHook 改回直接调钩子、直接换成它返回的 ctx
// → FreshCtx 那条红（instanceID 变空）。
type freshCtxHooks struct {
	compose_v2.NoOpOrchestrationHooks
}

func (freshCtxHooks) OnNodeStart(_ context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	// 业务完全可能这么写：从自己的池子里拿一个 ctx，或者干脆 Background
	return context.Background(), nil
}

// nilCtxHooks 返 (nil, nil)——业务忘了返 ctx 的常见写法
type nilCtxHooks struct {
	compose_v2.NoOpOrchestrationHooks
}

func (nilCtxHooks) OnNodeStart(context.Context, compose_v2.NodeID, compose_v2.Channels) (context.Context, error) {
	return nil, nil
}

func TestHookCtx_NilReturnFallsBackToInput(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("nilctx")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if ctx == nil {
			t.Error("❌ 节点体收到 nil ctx")
			return nil, nil
		}
		_ = ctx.Err()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(nilCtxHooks{}))
	if err != nil {
		t.Errorf("hook 返 (nil, nil) 该沿用框架自己的 ctx 正常跑完，实际 err=%v\n"+
			"  nil pointer panic = 那个 nil 被直接传给了节点体，而真正的原因"+
			"（hook 返了 nil）在错误里一个字都看不到", err)
	}
}

func TestHookCtx_FrameworkValuesSurviveNonDerivedCtx(t *testing.T) {
	var sawInstance, sawTaskID string
	var sawSite compose_v2.InterruptSite

	b := compose_v2.NewBuilder().WithName("hookctx")
	b.AddNode("spawner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "n"})
		return nil, nil
	})
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sawInstance = compose_v2.GetCurrentInstanceID(ctx)
		sawSite = compose_v2.CurrentInterruptSite(ctx)
		sawTaskID = sawSite.TaskID
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "spawner")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(freshCtxHooks{})); err != nil {
		t.Fatalf("run: %v", err)
	}
	t.Logf("hook 返新 ctx 后，节点体看到：instanceID=%q taskID=%q nodeID=%q visit=%d",
		sawInstance, sawTaskID, sawSite.NodeID, sawSite.Visit)
	// **承重**：业务 hook 返了个新 ctx，框架的实例身份仍在
	if sawInstance == "" {
		t.Errorf("实例身份被业务 hook 返回的 ctx 顶掉了（fan-out 派生实例应有非空 InstanceID）\n" +
			"  丢了的后果：N 个并发实例落同一个 span 槽 / 子 cp 互覆 / InterruptID 撞车")
	}
	if sawSite.NodeID == "" {
		t.Error("节点身份丢了 —— 中断寻址会算出空 ID")
	}
}

// TestHookCtx_NonDerivedCtx_NSAndAnswersKept：OnNodeStart 返回 context.Background() 时，节点体照样读得到
// 自己的 ns、续跑时拿得到答案——执行状态一律取钩子之前的那个（CallHook 采纳）。曾经只重铺几样：ns 变成
// ""、答案进不来，节点体再问一遍，续跑永远停在同一个中断上（实测续跑 4 次问了 5 遍）。
func TestHookCtx_NonDerivedCtx_NSAndAnswersKept(t *testing.T) {
	asks := 0
	var seenNS []string
	b := compose_v2.NewBuilder().WithName("fresh_ctx_ask")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		seenNS = append(seenNS, compose_v2.GetGraphNS(ctx))
		asks++
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx := compose_v2.WithGraphNS(context.Background(), "outer/x")
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("fresh-ask"), compose_v2.WithHooks(freshCtxHooks{})}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问上，实际 %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil {
		t.Fatalf("续跑应拿到答案、跑完，实际 %v", err)
	}
	if asks != 2 {
		t.Errorf("节点应只执行两次（首跑问、续跑拿到答案），实际 %d 次", asks)
	}
	for i, ns := range seenNS {
		if ns != "outer/x" {
			t.Errorf("第 %d 次执行节点体读到的 ns 应是 outer/x，实际 %q", i+1, ns)
		}
	}
}
