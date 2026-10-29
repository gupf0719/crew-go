package compose_v2_test

// 钩子链上一个 End / Event 钩子 panic，排在它后面的钩子照样调到：End 钩子能做上行信号决定，节点体成功时 panic 又被就地兜住，
// 跳过后面的钩子就是静默丢掉它们的决定（曾经如此，审计实测：节点体做 from_body、钩子做 from_hook，panic 在前时收到
// from_body、err == nil）。调完整条链之后第一个 panic 原样交给调用点，原值与 panic 现场的栈帧都在。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
)

// panickyEndHooks 在 OnNodeEnd / OnGraphEnd / OnNodeEvent 上 panic。
type panickyEndHooks struct {
	compose_v2.NoOpOrchestrationHooks
}

func (panickyEndHooks) OnNodeEnd(context.Context, compose_v2.NodeID, compose_v2.Channels, error) context.Context {
	panic("boom-node-end")
}

func (panickyEndHooks) OnGraphEnd(context.Context, compose_v2.Channels, error) context.Context {
	panic("boom-graph-end")
}

func (panickyEndHooks) OnNodeEvent(context.Context, compose_v2.NodeID, compose_v2.Event) context.Context {
	panic("boom-event")
}

// endRecorder 记下 End / Event 钩子被调了几次。
type endRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	nodeEnd, graphEnd, events int
}

func (r *endRecorder) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	r.nodeEnd++
	return ctx
}

func (r *endRecorder) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	r.graphEnd++
	return ctx
}

func (r *endRecorder) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Event) context.Context {
	r.events++
	return ctx
}

// 节点体成功、链里排在前面的观测钩子在 OnNodeEnd 里 panic：排在后面的钩子在 OnNodeEnd 里做的决定照常生效（后做的为准）。
func TestChainOrchHooks_EndPanicKeepsLaterHookDecision(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("chain_end_panic")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "from_body")
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)
	hooks := compose_v2.NewChainOrchHooks(panickyEndHooks{}, &nodeHookHooks{node: "n", value: "from_hook"})
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks))
	})
	if err != nil {
		t.Fatalf("节点体已经成功、OnNodeEnd 是观测钩子，Run 应成功：%v", err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "from_hook" {
		t.Fatalf("排在后面的钩子在 OnNodeEnd 里的决定应生效（from_hook），实际 %v", got)
	}
}

// 节点里发事件、图成功跑完：排在 panic 钩子后面的钩子照样收到事件、OnNodeEnd 与 OnGraphEnd。
func TestChainOrchHooks_EventAndGraphEndReachLaterHooks(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("chain_event_panic")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: "x"})
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)
	rec := &endRecorder{}
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(compose_v2.NewChainOrchHooks(panickyEndHooks{}, rec)))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rec.events == 0 || rec.nodeEnd != 1 || rec.graphEnd != 1 {
		t.Fatalf("排在后面的钩子应收到事件、OnNodeEnd、OnGraphEnd，实际 events=%d nodeEnd=%d graphEnd=%d",
			rec.events, rec.nodeEnd, rec.graphEnd)
	}
}

// 直接调链：每个钩子都调到，交给调用点的是第一个 panic——原值不变、调用点取的栈看得到它 panic 的地方。
func TestChainOrchHooks_RethrowsFirstPanicWithOriginalStack(t *testing.T) {
	rec := &endRecorder{}
	chain := compose_v2.NewChainOrchHooks(panickyEndHooks{}, panickyEndHooks{}, rec)
	cases := []struct {
		name  string
		value string
		call  func()
	}{
		{"OnNodeEnd", "boom-node-end", func() { chain.OnNodeEnd(context.Background(), "n", compose_v2.Channels{}, nil) }},
		{"OnGraphEnd", "boom-graph-end", func() { chain.OnGraphEnd(context.Background(), compose_v2.Channels{}, nil) }},
		{"OnNodeEvent", "boom-event", func() { chain.OnNodeEvent(context.Background(), "n", compose_v2.Event{}) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pe := recoverPanicError(t, c.call)
			if pe.Value != c.value {
				t.Fatalf("应抛出第一个 panic 的原值 %q，实际 %v", c.value, pe.Value)
			}
			if !strings.Contains(pe.Stack, "panickyEndHooks."+c.name) {
				t.Fatalf("栈应是钩子当初 panic 的地方，实际：\n%s", pe.Stack)
			}
		})
	}
	if rec.nodeEnd != 1 || rec.graphEnd != 1 || rec.events != 1 {
		t.Fatalf("排在两个 panic 后面的钩子每个方法应各被调一次，实际 nodeEnd=%d graphEnd=%d events=%d",
			rec.nodeEnd, rec.graphEnd, rec.events)
	}
}

// 节点体失败、链里的 OnNodeEnd panic：排在后面的钩子照样收到 OnNodeEnd，错误照旧是节点那个，日志里记的栈指向钩子
// （panic 现场的栈帧在调用点取栈时还在）。
func TestChainOrchHooks_FailurePathPanicLoggedWithOriginalStack(t *testing.T) {
	logs := psCaptureLogs(t)
	b := compose_v2.NewBuilder().WithName("chain_fail_panic")
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, errors.New("body failed")
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)
	rec := &endRecorder{}
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(compose_v2.NewChainOrchHooks(panickyEndHooks{}, rec)))
	if err == nil || err.Error() != `node "n": body failed` {
		t.Fatalf("OnNodeEnd / OnGraphEnd 的 panic 不改变结局，应照旧返回节点的错误，实际 %v", err)
	}
	if n := logs.countStack("panickyEndHooks.OnNodeEnd"); n != 1 {
		t.Fatalf("应记一条栈指向钩子的日志，实际 %d 条", n)
	}
	if rec.nodeEnd != 1 {
		t.Fatalf("排在后面的钩子应收到 OnNodeEnd，实际 %d 次", rec.nodeEnd)
	}
}

// recoverPanicError 调 call、兜住它抛出的 panic，按框架兜底点的写法（panicerr.New）还原成 PanicError。
func recoverPanicError(t *testing.T, call func()) (pe *compose_v2.PanicError) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("应抛出 panic")
		}
		pe = panicerr.New(r)
	}()
	call()
	return nil
}
