package compose_v2_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// 端到端测试：覆盖 Step 3 主循环 + computeNextTasks 与 channel-based 模型设计原则。

// ── 基础端到端 ──

func TestGraph_SimpleLinear(t *testing.T) {
	counterCh := compose_v2.Sum("counter")
	b := compose_v2.NewBuilder().
		AddNode("inc", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(counterCh, 1)}, nil
		}).
		AddEdge(compose_v2.StartNode, "inc").
		AddEdge("inc", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, counterCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if v := compose_v2.MustGet(cm.Channels(), counterCh); v != 1 {
		t.Fatalf("counter 应 1 实际 %d", v)
	}
}

// ── 关键测试：channel-based 自然消除 plan_chat_web bug 类 ──

// TestGraph_DoneAfterCtxDeadline_StillSuccess 验证 channel-based 模型下"已完成
// graph 不被 ctx 拦下"是模型自带属性，不需要 graphLoop 手动调换检查顺序。
//
// **场景**：
//   - graph: Start → consumer → End
//   - consumer 节点 sleep 直到 ctx 真正 deadline 后才 return
//   - return 后 computeNextTasks 算出 frontier=[End] → isEnd=true → 立即 return success
//   - **关键**：不会进入下一帧 loop 顶部 ctx 检查，所以 ctx 状态完全无关
//
// **对照 main 分支 bug**：commit 0bfd568 临时 fix 调换顺序——本测试在 channel
// 模型下根本不需要那个 fix，因为 isEnd 检测在 computeNextTasks 内合并完成。
//
// sanity-revert 提示：把 graph_loop.go::Run 内 `if isEnd { return nil }` 删掉
// 改为 `frontier = nextFrontier; continue`，本测试应能复现 deadline exceeded fail。
func TestGraph_DoneAfterCtxDeadline_StillSuccess(t *testing.T) {
	resultCh := compose_v2.LastValue[string]("result")
	b := compose_v2.NewBuilder().
		AddNode("consumer", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 等到 ctx 真正 done + 多 2ms
			<-ctx.Done()
			time.Sleep(2 * time.Millisecond)
			if ctx.Err() == nil {
				t.Fatalf("test setup bug: ctx should be done")
			}
			// **故意**：节点内部不检查 ctx，正常 return success
			// 类似 plan_chat_web 中 plan node 在 ctx 即将死时跑完返 Done=true
			return []compose_v2.ChannelWrite{compose_v2.Write(resultCh, "done")}, nil
		}).
		AddEdge(compose_v2.StartNode, "consumer").
		AddEdge("consumer", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, resultCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(ctx, g, cm)

	// 修复后（channel-based 模型自然行为）：consumer 跑完 → computeNextTasks
	// 立即返 isEnd=true → return nil，不进入下次 loop 顶部 ctx 检查
	if runErr != nil {
		if errors.Is(runErr, context.DeadlineExceeded) {
			t.Fatalf("已完成的 graph 不应被 ctx deadline 拦下：%v\n"+
				"sanity-revert：删 Run 内 'if isEnd { return nil }' 改 continue，本测试应复现此 fail", runErr)
		}
		t.Fatalf("意外错误：%v", runErr)
	}
	if v := compose_v2.MustGet(cm.Channels(), resultCh); v != "done" {
		t.Fatalf("result 应 'done' 实际 %q", v)
	}
}

// TestGraph_CtxErrDuringRunningStillFails 反向验证：ctx 检查在"还要继续干"
// 场景仍生效——修复不应让"真正进行中的 graph 也忽略 ctx"。
func TestGraph_CtxErrDuringRunningStillFails(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("slow", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			<-ctx.Done()
			return nil, nil
		}).
		AddNode("next", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			t.Fatalf("should not reach next node")
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "slow").
		AddEdge("slow", "next"). // slow → next，不是直接 End
		AddEdge("next", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(ctx, g, cm)

	if runErr == nil {
		t.Fatal("还要继续干时 ctx 超时应报错，实际 nil")
	}
	if !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("期望 deadline exceeded，实际 %v", runErr)
	}
}

// ── 条件边 + channel 读写 ──

// TestGraph_ConditionalEdge_ReadsChannel 验证条件边可读 channel 决定路由。
func TestGraph_ConditionalEdge_ReadsChannel(t *testing.T) {
	flagCh := compose_v2.LastValue[string]("decision")
	pathCh := compose_v2.LastValue[string]("path_taken")

	b := compose_v2.NewBuilder().
		AddNode("decide", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(flagCh, "left")}, nil
		}).
		AddNode("left_branch", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(pathCh, "took_left")}, nil
		}).
		AddNode("right_branch", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(pathCh, "took_right")}, nil
		}).
		AddEdge(compose_v2.StartNode, "decide").
		AddConditionalEdge("decide", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			f, _ := compose_v2.Get(chs, flagCh)
			if f == "left" {
				return "left_branch", nil
			}
			return "right_branch", nil
		}).
		AddEdge("left_branch", compose_v2.EndNode).
		AddEdge("right_branch", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, flagCh)
	b = compose_v2.WithChannel(b, pathCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if v := compose_v2.MustGet(cm.Channels(), pathCh); v != "took_left" {
		t.Fatalf("应走 left_branch（flag='left'），实际 %q", v)
	}
}

// ── 跨超步累加（PoC §4 场景 B）──

// TestGraph_CrossSuperstepAccumulation 验证 channel reducer 自动跨超步累加，
// 不需节点手动维护 state + 锁。
//
// **撞 audit 价值验证**：channel-based 在跨超步累加场景显著更简洁。
func TestGraph_CrossSuperstepAccumulation(t *testing.T) {
	tokenUsage := compose_v2.Sum("token_usage")
	roundCount := compose_v2.Sum("rounds")

	// 3 轮循环：每轮 +tokens / +1 round
	b := compose_v2.NewBuilder().
		AddNode("step", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{
				compose_v2.Write(tokenUsage, 100),
				compose_v2.Write(roundCount, 1),
			}, nil
		}).
		AddEdge(compose_v2.StartNode, "step").
		AddConditionalEdge("step", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			rounds, _ := compose_v2.Get(chs, roundCount)
			if rounds >= 3 {
				return compose_v2.EndNode, nil
			}
			return "step", nil // 循环回 step
		}).
		WithMaxVisit(10)
	b = compose_v2.WithChannel(b, tokenUsage)
	b = compose_v2.WithChannel(b, roundCount)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if r := compose_v2.MustGet(cm.Channels(), roundCount); r != 3 {
		t.Fatalf("rounds 应 3 实际 %d", r)
	}
	if t2 := compose_v2.MustGet(cm.Channels(), tokenUsage); t2 != 300 {
		t.Fatalf("token_usage 应 300 实际 %d", t2)
	}
}

// TestGraph_MaxVisit_Defense 验证 maxVisit 防死循环。
func TestGraph_MaxVisit_Defense(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("loop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "loop").
		AddEdge("loop", "loop"). // 死循环
		WithMaxVisit(5).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("死循环应被 maxVisit 拦下")
	}
	if !strings.Contains(err.Error(), "visited") {
		t.Fatalf("error 应含 'visited'，实际 %q", err.Error())
	}
}

// TestGraph_NodeFuncPanicRecovered 验证 NodeFunc panic 被 framework 转 error。
//
// **R10 跨仓 audit A2 修订（第 5 类炸弹变体：fixture 用 string panic 不触发 fix 路径）**：
// 旧版 `panic("intentional test panic")` 是 string，graph_loop.go::safeNodeRun 的
// `if rerr, ok := r.(error); ok` 永远 false → 走 `%v` 分支 → audit Round 10 Bug D-N3
// 修复 `%w` 保 errors.Is/As chain 完全没被触发 → sanity-revert 删 `%w` 分支测试仍 PASS。
//
// 新版 panic(io.EOF)（标准库 error 实现）+ 断言 errors.Is(err, io.EOF) → 真验 chain。
func TestGraph_NodeFuncPanicRecovered(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("panicker", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			panic(io.EOF) // R10 A2 改：用 error 接口实现而非 string，触发 %w chain 路径
		}).
		AddEdge(compose_v2.StartNode, "panicker").
		AddEdge("panicker", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("NodeFunc panic 应被 recover 转 error")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error 应含 'panic'，实际 %q", err.Error())
	}
	// **R10 A2 关键断言**：errors.Is chain 必须保留——audit Round 10 Bug D-N3 修复的
	// `%w` wrap 行为。sanity-revert 改回 `%v` → errors.Is 返 false → fail。
	if !errors.Is(err, io.EOF) {
		t.Errorf("R10 A2 回归：panic value 是 io.EOF，errors.Is(err, io.EOF) 必须真——audit D-N3 fix 用 %%w 保 chain，实际 err=%v", err)
	}
}
