package compose_v2_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// PoC §4.3 场景 C：动态 fan-in + 多分支并发写 channel 自动 reduce 验证。
//
// **目标**：撞 channel 模型相对 frontier 模型的核心价值场景——多分支并发写
// 同 channel，reducer 在统一同步点（ApplyAll）自动合并；frontier 模型必须
// 硬编码 ParallelResults 聚合节点 + 业务自己写聚合逻辑。

// ── 多 worker fan-out + Append reducer 自动聚合 ──

// TestFanOut_AppendReducer_AutoMerge 验证多 worker 并发写同 Append channel
// 时 reducer 自动合并所有结果。
//
// **撞设计文档 §3.1 价值场景**：channel-based 在多分支聚合场景显著优于
// frontier 模型——后者必须硬编码"fan-out 节点 + 聚合节点"双节点结构
// （见现有 agent/parallel.go:281-290）。
func TestFanOut_AppendReducer_AutoMerge(t *testing.T) {
	resultsCh := compose_v2.Append[string]("worker_results")

	// 3 个 worker 节点，并发执行，每个写自己的 result
	worker := func(name string) compose_v2.NodeFunc {
		return func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 模拟工作
			time.Sleep(10 * time.Millisecond)
			return []compose_v2.ChannelWrite{
				compose_v2.Write(resultsCh, []string{fmt.Sprintf("result_from_%s", name)}),
			}, nil
		}
	}

	b := compose_v2.NewBuilder().
		AddNode("dispatcher", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil // 仅作 fan-out 起点
		}).
		AddNode("worker_a", worker("a")).
		AddNode("worker_b", worker("b")).
		AddNode("worker_c", worker("c")).
		AddNode("aggregator", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// aggregator 在所有 worker 完成后自动 trigger（frontier 推进语义）
			results := compose_v2.MustGet(chs, resultsCh)
			t.Logf("aggregator 看到 %d 个 results: %v", len(results), results)
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "dispatcher").
		AddEdge("dispatcher", "worker_a"). // 静态 fan-out
		AddEdge("dispatcher", "worker_b").
		AddEdge("dispatcher", "worker_c").
		AddEdge("worker_a", "aggregator").
		AddEdge("worker_b", "aggregator").
		AddEdge("worker_c", "aggregator").
		AddEdge("aggregator", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, resultsCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	results := compose_v2.MustGet(cm.Channels(), resultsCh)
	if len(results) != 3 {
		t.Fatalf("应有 3 个 worker results，实际 %d (%v)", len(results), results)
	}

	// 顺序非确定性（goroutine 完成顺序），但内容一定全到
	sort.Strings(results)
	expected := []string{"result_from_a", "result_from_b", "result_from_c"}
	for i, r := range results {
		if r != expected[i] {
			t.Fatalf("results[%d] 应是 %q 实际 %q", i, expected[i], r)
		}
	}
}

// TestFanOut_TrueParallelism 验证多 worker 真并发（goroutine 同时跑），
// 不是顺序 + 假并发。
//
// 关键测试设计：3 个 worker 都阻塞在 barrier，所有 3 个都进入后才一起放行。
// 如果是顺序执行（fast path 之类），永远不会全到，测试 timeout fail。
func TestFanOut_TrueParallelism(t *testing.T) {
	resultsCh := compose_v2.Append[string]("results")

	var inFlight atomic.Int32
	var allArrived = make(chan struct{})
	var arriveOnce sync.Once

	worker := func(name string) compose_v2.NodeFunc {
		return func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 进入 barrier 计数
			n := inFlight.Add(1)
			if n == 3 {
				arriveOnce.Do(func() { close(allArrived) })
			}
			// 等所有 3 个都到——证明真并发
			select {
			case <-allArrived:
			case <-time.After(2 * time.Second):
				t.Errorf("worker %s timeout waiting for siblings", name)
				return nil, fmt.Errorf("timeout")
			}
			return []compose_v2.ChannelWrite{
				compose_v2.Write(resultsCh, []string{name}),
			}, nil
		}
	}

	b := compose_v2.NewBuilder().
		AddNode("dispatcher", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddNode("w1", worker("w1")).
		AddNode("w2", worker("w2")).
		AddNode("w3", worker("w3")).
		AddNode("agg", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "dispatcher").
		AddEdge("dispatcher", "w1").
		AddEdge("dispatcher", "w2").
		AddEdge("dispatcher", "w3").
		AddEdge("w1", "agg").
		AddEdge("w2", "agg").
		AddEdge("w3", "agg").
		AddEdge("agg", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, resultsCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	results := compose_v2.MustGet(cm.Channels(), resultsCh)
	if len(results) != 3 {
		t.Fatalf("应 3 个 workers，实际 %d", len(results))
	}
}

// TestFanOut_FailedBranchWritesDiscarded 验证业务错时失败分支的 writes 丢弃，
// **成功分支 writes 仍保留**（per-branch 部分提交语义）。
//
// 对照 LangGraph task 失败 pending_writes 不 commit 语义——crew-go 实际是
// per-branch delta（superstep_snapshot 模型）：失败分支单独标记 writes 丢弃，
// 成功分支独立 commit。
func TestFanOut_FailedBranchWritesDiscarded(t *testing.T) {
	resultsCh := compose_v2.Append[string]("results")

	b := compose_v2.NewBuilder().
		AddNode("d", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddNode("ok_branch", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(resultsCh, []string{"ok"})}, nil
		}).
		AddNode("fail_branch", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 失败前先写——这条 write 应该被丢弃
			return []compose_v2.ChannelWrite{
				compose_v2.Write(resultsCh, []string{"discarded"}),
			}, fmt.Errorf("intentional failure")
		}).
		AddEdge(compose_v2.StartNode, "d").
		AddEdge("d", "ok_branch").
		AddEdge("d", "fail_branch").
		AddEdge("ok_branch", compose_v2.EndNode).
		AddEdge("fail_branch", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, resultsCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm)
	if runErr == nil {
		t.Fatal("fail_branch 应让 Run 报错")
	}

	// **R10 跨仓 audit B1 修订（无断言 → 严格断言）**：旧版只 t.Logf 不断言，
	// 注释还停留在"全有或全无 PoC 简化"过时描述——R10 加 t.Fatal 后才暴露：
	// 实际行为已经是 **per-branch delta**（fail_branch 的 "discarded" 丢弃，
	// ok_branch 的 "ok" 保留）。
	//
	// **断言**：results 必须含 "ok"（成功分支保留）+ 必须不含 "discarded"
	// （失败分支丢弃）。任何一条 fail 都精确指向 fan-out 提交语义改变。
	results, _ := compose_v2.Get(cm.Channels(), resultsCh)
	hasOK := false
	for _, r := range results {
		if r == "discarded" {
			t.Errorf("fail_branch 的 writes 必须丢弃，实际 results 含 'discarded'：%v", results)
		}
		if r == "ok" {
			hasOK = true
		}
	}
	if !hasOK {
		t.Errorf("ok_branch 的 writes 必须保留（per-branch 提交语义），实际 results=%v", results)
	}
}
