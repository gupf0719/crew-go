package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// separateCheckpointClient 模拟另一个客户端，比较实例不同，存档仍落在同一后端。
type separateCheckpointClient struct {
	*compose_v2.InMemoryCheckpointer
}

type switchFanoutCheckpointer struct {
	compose_v2.NoOpOrchestrationHooks
	cp compose_v2.Checkpointer
}

func (h switchFanoutCheckpointer) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id == "left" || id == "right" {
		ctx = compose_v2.WithCheckpointerCtx(ctx, h.cp)
	}
	return ctx, nil
}

func TestRunAllowsSiblingSubgraphsOnOneAlternateCheckpointer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	sub, err := compose_v2.NewBuilder().WithName("sibling_work").
		AddNode("write", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			calls.Add(1)
			arrived <- struct{}{}
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).AddEdge(compose_v2.StartNode, "write").AddEdge("write", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent, err := compose_v2.NewBuilder().WithName("sibling_parent").
		AddSubgraphNode("left", sub).AddSubgraphNode("right", sub).
		AddEdge(compose_v2.StartNode, "left").AddEdge(compose_v2.StartNode, "right").
		AddEdge("left", compose_v2.EndNode).AddEdge("right", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}

	outer, alternate := compose_v2.NewInMemoryCheckpointer(), compose_v2.NewInMemoryCheckpointer()
	finished := make(chan error, 1)
	go func() {
		finished <- compose_v2.Run(compose_v2.WithOrchHooks(ctx, switchFanoutCheckpointer{cp: alternate}),
			parent, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(outer), compose_v2.WithSessionID("siblings"))
	}()
	defer func() {
		close(release)
		if err := <-finished; err != nil && !t.Failed() {
			t.Errorf("并行子图失败: %v", err)
		}
	}()
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatalf("两个子图应可在同一备选分区执行，实际 %d 次", calls.Load())
		}
	}
	if err := compose_v2.Run(ctx, sub, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(alternate), compose_v2.WithSessionID("siblings")); !errors.Is(err, compose_v2.ErrTaskBusy) {
		t.Fatalf("独立根任务抢到并行子图的分区: %v", err)
	}
}

func TestTaskActivityHoldsAlternateCheckpointUntilLastSiblingReleases(t *testing.T) {
	ctx := t.Context()
	cp := compose_v2.NewInMemoryCheckpointer()
	const sid = "alternate"
	rootCtx, root, err := compose_v2.BeginTaskActivity(ctx, sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Release()
	siblingCtx := compose_v2.WithCheckpointerCtx(rootCtx, cp)
	stale, first, err := compose_v2.BeginTaskActivity(siblingCtx, sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	_, second, err := compose_v2.BeginTaskActivity(siblingCtx, sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	independent := compose_v2.WithCheckpointerCtx(ctx, cp)
	first.Release()
	assertBusy := func(ctx context.Context) {
		t.Helper()
		_, activity, err := compose_v2.BeginTaskActivity(ctx, sid, nil)
		if activity != nil {
			activity.Release()
		}
		if !errors.Is(err, compose_v2.ErrTaskBusy) {
			t.Fatalf("未持有分区的调用应被拒绝: %v", err)
		}
	}
	assertBusy(independent)
	second.Release()
	_, next, err := compose_v2.BeginTaskActivity(independent, sid, nil)
	if err != nil {
		t.Fatalf("最后一支释放后仍不可进入: %v", err)
	}
	defer next.Release()
	assertBusy(stale)
}

func TestRunRejectsConcurrentResumeOnSameCheckpointer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var writes atomic.Int32
	g, err := compose_v2.NewBuilder().WithName("resume_same_checkpointer").
		AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, ok := compose_v2.GetResumeAnswer(ctx, "q"); !ok {
				return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "q", Question: "?"}}
			}
			if writes.Add(1) == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	run := func(opts ...compose_v2.RunOption) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			append([]compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("resume")}, opts...)...)
	}
	if _, ok := compose_v2.IsInterrupt(run()); !ok {
		t.Fatal("首次运行未挂起")
	}
	answer := compose_v2.WithResumeAnswers(map[string]*schema.Message{"q": schema.UserMessage("继续")})
	first := make(chan error, 1)
	go func() { first <- run(answer) }()
	defer func() {
		close(release)
		if err := <-first; err != nil && !t.Failed() {
			t.Errorf("原续跑失败: %v", err)
		}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("首个续跑未执行")
	}
	if err := run(answer); !errors.Is(err, compose_v2.ErrTaskBusy) || writes.Load() != 1 {
		t.Fatalf("重叠续跑应在副作用前被拒绝，err=%v，执行 %d 次", err, writes.Load())
	}
}

func TestRunRejectsConcurrentFreshExecutionOnSameCheckpointer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	b := compose_v2.NewBuilder().WithName("same_checkpointer")
	b.AddNode("write", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "write")
	b.AddEdge("write", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	run := func(cp compose_v2.Checkpointer) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("task"))
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	first := make(chan error, 1)
	go func() { first <- run(cp) }()
	select {
	case <-entered:
	case err := <-first:
		t.Fatalf("首个执行未到达副作用节点: %v", err)
	case <-ctx.Done():
		t.Fatal("首个执行未进入节点")
	}
	defer func() {
		close(release)
		if err := <-first; err != nil {
			t.Errorf("首个执行失败: %v", err)
		}
	}()
	if err := run(cp); !errors.Is(err, compose_v2.ErrTaskBusy) || calls.Load() != 1 {
		t.Fatalf("同实例的重叠执行应在节点前拒绝，err=%v，节点执行 %d 次", err, calls.Load())
	}
	if err := run(compose_v2.NewInMemoryCheckpointer()); err != nil || calls.Load() != 2 {
		t.Fatalf("独立后端应能执行相同任务 ID，err=%v，节点执行 %d 次", err, calls.Load())
	}
}
