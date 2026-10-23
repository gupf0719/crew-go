package compose_v2_test

// maxVisit 语义：限制的是**迭代深度**（同一节点被反复回边访问多少轮），不是
// fan-out **宽度**（一个超步里同一 NodeID 有多少个并发实例）。
//
// 两者混淆的后果不是报错了事——业务把 worker 并发数调到 maxVisit 以上，整张图
// 在第一个超步就假触顶，worker 一次都没跑。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// TestVisitCount_FanoutWidthIsNotIterationDepth：单超步内 N 个并发实例不该被当作
// N 次迭代。
//
// sanity-revert：把 graph_loop 的 distinctNodeIDs(frontier) 换回 range frontier
// → visits[worker] 一超步跳到 5 > maxVisit(3) → worker 跑 0 次 + 返
// MaxIterationsError。
func TestVisitCount_FanoutWidthIsNotIterationDepth(t *testing.T) {
	const width = 5
	var ranWorker atomic.Int32

	b := compose_v2.NewBuilder().WithName("width").WithMaxVisit(3)
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ds := make([]compose_v2.SendDirective, 0, width)
		for i := 0; i < width; i++ {
			ds = append(ds, compose_v2.SendDirective{Target: "worker"})
		}
		compose_v2.SendFrom(ctx, ds...)
		return nil, nil
	})
	b.AddNode("worker", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranWorker.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("worker", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		var mie *compose_v2.MaxIterationsError
		if errors.As(err, &mie) {
			t.Fatalf("fan-out 宽度 %d 被当成迭代深度触顶（maxVisit=3）：%v\n"+
				"  maxVisit 限的是同一节点被回边反复访问的**轮数**，不是一超步里的并发**份数**",
				width, err)
		}
		t.Fatalf("Run: %v", err)
	}
	if got := ranWorker.Load(); got != width {
		t.Errorf("worker 应跑 %d 次（fan-out 宽度），实际 %d 次", width, got)
	}
}

// TestVisitCount_LoopDepthStillCapped 守另一半：改成按 distinct NodeID 计数后，
// 真正的迭代深度（循环回边反复访问同一节点）**仍然**要被 maxVisit 拦住。
//
// 没有这条，上面那个测试可以靠"干脆不计数"作弊通过——防护就被拆没了。
func TestVisitCount_LoopDepthStillCapped(t *testing.T) {
	var rounds atomic.Int32

	b := compose_v2.NewBuilder().WithName("depth").WithMaxVisit(3)
	b.AddNode("loop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		rounds.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "loop")
	// 无条件回边——只能靠 maxVisit 收敛
	b.AddEdge("loop", "loop")

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// ctx deadline：防护若失效，失败模式是挂死烧光 -timeout 预算、断言那行永远
	// 执行不到（整包 panic: test timed out）。有 deadline 就退化成干净的断言失败。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Fatalf("无限回边应触 MaxIterationsError（否则 maxVisit 防护形同虚设），got %v", err)
	}
	if got := rounds.Load(); got > 4 {
		t.Errorf("loop 跑了 %d 轮，maxVisit=3 应在第 4 轮拦住", got)
	}
}

// TestVisitCount_CompoundSelfFanoutIsBounded 守 fan-out 宽度的兜底。
//
// maxVisit 计的是深度、一个超步至多 +1 —— 它拦不住宽度。节点在自己的 NodeFunc 里
// SendFrom 自己 N(≥2) 次时宽度**逐超步相乘**：业务终止条件写错就是 N^超步数。等
// maxVisit 触顶时进程早已 OOM（实测 N=2/maxVisit=6 执行 63 次 = 2^6−1；默认
// maxVisit=100 是 2^100）。
//
// 用 ctx deadline 而不是裸跑：防护若失效，失败模式是挂死烧光 -timeout 预算，
// 断言那行永远执行不到。有 deadline 就退化成干净的断言失败。
func TestVisitCount_CompoundSelfFanoutIsBounded(t *testing.T) {
	var ran atomic.Int32

	b := compose_v2.NewBuilder().WithName("compound").
		WithMaxVisit(60).          // 深度给够，逼宽度那道防线出场
		WithMaxFrontierWidth(2000) // 宽度上限调小让测试快
	b.AddNode("W", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ran.Add(1)
		// 每个实例再投 2 个自己——终止条件写错的典型形态
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "W"},
			compose_v2.SendDirective{Target: "W"},
		)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "W")

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	// 钉的**行为契约不变**：宽度指数爆炸必须被拦住。换的只是类型——宽度与深度是两个正交维度，
	// 曾经共用 MaxIterationsError（`Iterations` 一个装宽度一个装深度，调用方 errors.As 之后分不出拿到
	// 的是哪个量），现在宽度有自己的 [compose_v2.MaxWidthError]。用专属类型断言比原来**更准**：
	// 深度触顶再也不会误命中这条。
	var mwe *compose_v2.MaxWidthError
	if !errors.As(err, &mwe) {
		t.Fatalf("宽度指数爆炸应被 MaxWidthError 拦住，got %v（执行了 %d 次）\n"+
			"  没拦住 = 宽度无界，默认 maxVisit=100 时是 2^100 次执行，OOM 先到",
			err, ran.Load())
	}
	if mwe.Width <= 2000 {
		t.Errorf("MaxWidthError.Width=%d 应大于宽度上限 2000——它要装的是触顶时的宽度", mwe.Width)
	}
	// 宽度失控不该被当成深度触顶：两个维度各有自己的类型，调用方据此分流处置
	// （宽度 ⟹ 查业务的终止条件；深度 ⟹ 预算耗尽）。
	var mie *compose_v2.MaxIterationsError
	if errors.As(err, &mie) {
		t.Errorf("宽度失控不该同时命中 MaxIterationsError（Iterations=%d）——那正是两个维度共用一个类型"+
			"的老问题", mie.Iterations)
	}
	// 拦在宽度上限量级，而不是等指数涨到天上
	if got := ran.Load(); got > 8000 {
		t.Errorf("拦得太晚：已执行 %d 次（宽度上限 2000，应在同量级拦住）", got)
	}
	t.Logf("宽度防线在 %d 次执行后拦住", ran.Load())
}

// TestWidthOverflow_IsTerminalAndLeavesNoPoisonedCheckpoint：宽度失控是**终局**的——档被清掉、
// 各吞错点不吞。
//
// 不清档的后果实测过：存进档的 `CurrentFrontier` 就是那个超限的 frontier，而续跑闸门按**内容**判
// （`HasResumableWork`，不读 Status）⟹ 同 sid 再 Run 时 auto-load 捡起它，**节点一次没跑**就在同一道
// 闸上以同一个 Width 再爆，那个 (sid, ns) 槽从此是死的，且每次重爆都往持久后端写一遍上万条 frontier。
//
// ⚠️ **断言选在承重的维度上**：不能只断言「errors.Is(ErrRunAborted)」——`MaxWidthError` 本来就实现
// `NonRetryable`，把 `%w ErrRunAborted` 撤掉后 `IsRetryable` 仍是 false（双层防护遮蔽，CLAUDE.md
// 第 4 类炸弹）。真正只有 abort 才给的是这三样：**档被清掉**、**第二次 Run 从头跑而不是 ran==0**、
// **MustPropagate 为真**。
func TestWidthOverflow_IsTerminalAndLeavesNoPoisonedCheckpoint(t *testing.T) {
	var ran atomic.Int32
	b := compose_v2.NewBuilder().WithName("poison").WithMaxVisit(60).WithMaxFrontierWidth(64)
	b.AddNode("W", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ran.Add(1)
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "W"},
			compose_v2.SendDirective{Target: "W"})
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "W")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := compose_v2.NewInMemoryCheckpointer()
	const sid = "poison-sid"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)

	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid))
	var mwe *compose_v2.MaxWidthError
	if !errors.As(err, &mwe) {
		t.Fatalf("fixture 没搭对：第一次 Run 应以宽度失控收尾，got %v（执行 %d 次）", err, ran.Load())
	}
	if ran.Load() == 0 {
		t.Fatal("fixture 没搭对：第一次 Run 应真的执行过节点")
	}
	if !compose_v2.MustPropagate(ctx, err) {
		t.Error("宽度失控应 MustPropagate——否则经 AgentAsTool 等吞错点会被吞成工具失败，模型换条路再试，" +
			"最终调用方拿到的是别的错，这次区分宽度与深度的改动被抹掉")
	}

	ok, exErr := cp.ExistsNS(ctx, sid, "")
	if exErr != nil {
		t.Fatalf("ExistsNS: %v", exErr)
	}
	if ok {
		ckpt, _ := cp.LoadNS(ctx, sid, "")
		n := 0
		if ckpt != nil {
			n = len(ckpt.CurrentFrontier)
		}
		t.Errorf("宽度失控应清掉 cp 子树，档还在（frontier %d 条）——留着它 auto-load 会捡起来、"+
			"节点一次没跑就在同一道闸再爆，那个 sid 从此是死的", n)
	}

	// 承重的那条：同 sid 再 Run 必须**从头跑**（节点真执行），而不是捡起毒档 ran==0 再爆。
	ran.Store(0)
	err2 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid))
	if !errors.As(err2, &mwe) {
		t.Fatalf("第二次 Run 也该以宽度失控收尾（图没变），got %v", err2)
	}
	if ran.Load() == 0 {
		t.Error("第二次 Run 节点一次没跑 ⟹ auto-load 捡起了上次的毒档、直接在闸上再爆；" +
			"清了档的话它是 fresh run，节点会从 StartNode 真跑一遍")
	}
}
