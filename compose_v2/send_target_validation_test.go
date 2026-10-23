package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// SendFrom 投递到图里没有的节点（或起点 sentinel）是图定义错，重跑必然一样：跟条件边返回坏目标同一个处置——
// 发出它的成员以 ErrRunAborted 失败，不重试，不留下原地再炸的存档。
//
// 原来坏目标进了下一超步的 frontier 才炸在「node 未注册」上：普通错误（默认可重试），外层重试白跑几遍，存档里带着
// 这个目标，每次续跑原地再炸。

func runSendToTarget(t *testing.T, target compose_v2.NodeID, fanout bool) (attempts int32, err error, pending int) {
	t.Helper()
	split := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: target})
		return nil, nil
	}
	noop := func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }
	b := compose_v2.NewBuilder().AddNode("split", split).AddEdge("split", compose_v2.EndNode)
	if fanout {
		// 起点扇出成两个成员：坏目标出自其中一个，走并发收集那条路径
		b = b.AddNode("sibling", noop).AddEdge(compose_v2.StartNode, "split").AddEdge(compose_v2.StartNode, "sibling").
			AddEdge("sibling", compose_v2.EndNode)
	} else {
		b = b.AddEdge(compose_v2.StartNode, "split")
	}
	g, cerr := b.Compile()
	if cerr != nil {
		t.Fatalf("Compile: %v", cerr)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	var n atomic.Int32
	_, err = compose_v2.RunWithRetry(ctx, compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: compose_v2.IsRetryable},
		func(ctx context.Context) (struct{}, error) {
			n.Add(1)
			return struct{}{}, compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("sid-send"))
		})
	ps, lerr := cp.ListPending(ctx, "sid-send")
	if lerr != nil {
		t.Fatalf("ListPending: %v", lerr)
	}
	return n.Load(), err, len(ps)
}

func TestSendFrom_UnregisteredTargetIsTerminal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target compose_v2.NodeID
		fanout bool
	}{
		{"ghost", "ghost", false},
		{"start", compose_v2.StartNode, false},
		{"ghost_fanout", "ghost", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts, err, pending := runSendToTarget(t, tc.target, tc.fanout)
			if !errors.Is(err, compose_v2.ErrRunAborted) {
				t.Fatalf("坏投递目标应终局失败（ErrRunAborted），got %v", err)
			}
			if attempts != 1 {
				t.Errorf("外层重试跑了 %d 次，want 1——确定性的图定义错不该重试", attempts)
			}
			if pending != 0 {
				t.Errorf("留下 %d 份可续的存档，want 0——续跑只会原地再炸", pending)
			}
		})
	}
}
