package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// ── SendFrom 动态 fan-out ──

// TestSendFrom_DynamicDispatchToNWorkers 验证：split 节点根据运行时数据
// SendFrom([]Send) 投递 N 个 worker，下一超步并发执行。
func TestSendFrom_DynamicDispatchToNWorkers(t *testing.T) {
	resultCh := compose_v2.Append[string]("results")

	splitNode := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 运行时决定派发 3 个 worker
		var ds []compose_v2.SendDirective
		for _, item := range []string{"a", "b", "c"} {
			ds = append(ds, compose_v2.SendDirective{
				Target: "worker",
				Payload: []compose_v2.ChannelWrite{
					compose_v2.Write(resultCh, []string{"processed:" + item}),
				},
			})
		}
		compose_v2.SendFrom(ctx, ds...)
		return nil, nil
	}

	workerNode := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// worker 不再追加 ChannelWrite——SendDirective.Payload 已注入 results channel
		return nil, nil
	}

	b := compose_v2.NewBuilder().
		AddNode("split", splitNode).
		AddNode("worker", workerNode).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", compose_v2.EndNode). // split 静态边到 EndNode（被 SendFrom 动态 fan-out 覆盖）
		AddEdge("worker", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, resultCh)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := compose_v2.Get(cm.Channels(), resultCh)
	if len(got) != 3 {
		t.Fatalf("expected 3 results from 3 workers, got %d: %v", len(got), got)
	}
	// 顺序非保（并发执行）但内容应包含三项
	have := map[string]bool{}
	for _, r := range got {
		have[r] = true
	}
	for _, want := range []string{"processed:a", "processed:b", "processed:c"} {
		if !have[want] {
			t.Fatalf("missing %q in results: %v", want, got)
		}
	}
}

// TestSendFrom_AllInvalidTargetsNotEnd 验证 caller 错调 SendFrom 全是 EndNode/空
// 时图能优雅终止——不报"frontier is empty but isEnd was false"。
//
// 实现细节：原 graph_loop.go 已通过 `continue` 保证 isEnd=false 只对 valid Target
// 生效（自审 Bug A 误判，重读代码后撤回）。本测试作 regression guard 防未来
// 重构破坏此不变量。
func TestSendFrom_AllInvalidTargetsNotEnd(t *testing.T) {
	splitNode := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 错调：所有 Target 都是 EndNode → 框架 filter 后 nextFrontier 空
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: compose_v2.EndNode},
			compose_v2.SendDirective{Target: ""}, // 空也被 filter
		)
		return nil, nil
	}

	g, _ := compose_v2.NewBuilder().
		AddNode("split", splitNode).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", compose_v2.EndNode). // 静态边到 EndNode 让 isEnd 自然 true
		Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm)
	if err != nil {
		t.Fatalf("Run should succeed (all invalid SendFrom Targets ignored), got %v\n"+
			"sanity-revert：graph_loop.go 去掉 validDirCount 守卫，isEnd 错置为 false → "+
			"\"frontier is empty but isEnd was false\"",
			err)
	}
}

// TestSendFrom_FailedNodeDirectivesDropped 验证失败分支的 SendFrom directives
// 被自然丢弃（与 ChannelWrite 同语义——失败 writes 不 commit）。
func TestSendFrom_FailedNodeDirectivesDropped(t *testing.T) {
	resultCh := compose_v2.Append[string]("results_failed")

	failingNode := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 先 SendFrom 派 worker，再返 error——directives 应被丢弃
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target: "worker",
			Payload: []compose_v2.ChannelWrite{
				compose_v2.Write(resultCh, []string{"should_not_happen"}),
			},
		})
		return nil, context.Canceled
	}

	b := compose_v2.NewBuilder().
		AddNode("fail", failingNode).
		AddNode("worker", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "fail").
		AddEdge("fail", compose_v2.EndNode).
		AddEdge("worker", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, resultCh)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("expected error from failing node")
	}

	got, _ := compose_v2.Get(cm.Channels(), resultCh)
	if len(got) != 0 {
		t.Fatalf("expected 0 results (directives dropped on failure), got %d: %v", len(got), got)
	}
}
