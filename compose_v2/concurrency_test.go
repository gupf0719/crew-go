package compose_v2_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

func TestMaxConcurrentNodes_CancelQueuedAndResume(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 4*time.Second)
	defer stop()
	activeCtx, cancel := context.WithCancel(compose_v2.WithMaxConcurrentNodes(ctx, 2))
	defer cancel()
	entered := make(chan compose_v2.NodeID, 4)
	release := make(chan struct{})
	var calls [4]atomic.Int32
	order := compose_v2.Append[string]("order")
	b := compose_v2.NewBuilder().WithName("bounded").WithMaxFrontierWidth(4)
	compose_v2.WithChannel(b, order)
	b.AddNode("split", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "split")
	for i, id := range []compose_v2.NodeID{"a", "b", "c", "d"} {
		b.AddNode(id, func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			calls[i].Add(1)
			if c.Done() == activeCtx.Done() {
				entered <- id
				<-release
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(order, []string{string(id)})}, nil
		})
		b.AddEdge("split", id)
		b.AddEdge(id, compose_v2.EndNode)
	}
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	options := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("budget-cancel")}
	done := make(chan error, 1)
	go func() { done <- compose_v2.Run(activeCtx, g, compose_v2.NewChannelManager(), options...) }()
	var admitted []compose_v2.NodeID
	for range 2 {
		select {
		case id := <-entered:
			admitted = append(admitted, id)
		case <-ctx.Done():
			close(release)
			t.Fatal("did not reach concurrency budget")
		}
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("scheduler did not return after cancellation")
	}
	slices.Sort(admitted)
	if !slices.Equal(admitted, []compose_v2.NodeID{"a", "b"}) || calls[2].Load() != 0 || calls[3].Load() != 0 {
		t.Fatalf("unexpected dispatch: admitted=%v c=%d d=%d", admitted, calls[2].Load(), calls[3].Load())
	}
	checkpoint, err := cp.LoadNS(ctx, "budget-cancel", "")
	if err != nil || checkpoint == nil || len(checkpoint.CompletedFrontier) != 2 || len(checkpoint.CurrentFrontier) != 2 {
		t.Fatalf("expected two completed and two queued members: checkpoint=%v err=%v", checkpoint, err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(compose_v2.WithMaxConcurrentNodes(ctx, 1), g, cm, options...); err != nil {
		t.Fatal(err)
	}
	for i := range calls {
		if got := calls[i].Load(); got != 1 {
			t.Fatalf("node %d executed %d times", i, got)
		}
	}
	got, _ := compose_v2.Get(cm.Channels(), order)
	if !slices.Equal(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("writes out of declaration order: %v", got)
	}
}
