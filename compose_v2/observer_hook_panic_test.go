package compose_v2_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// 观测钩子出错不改变执行结果（不变量：钩子出错不得让已经做完的工作再做一次）。节点体成功之后的 OnNodeEnd、节点体
// 中途的 OnNodeEvent、图成功跑完之后的 OnGraphEnd，返回值都不影响执行；它们 panic 时就地兜住、带栈记一条 ERROR，
// 结果照旧。曾经一律转成这个节点 / 这次 Run 的错误：失败档被续跑或重试捡起，已经做完的节点体重跑、副作用重放
// （实测同一个节点体跑 3～4 次）。

// ohpHooks 在指定位置 panic 一次。
type ohpHooks struct {
	compose_v2.NoOpOrchestrationHooks
	at   string
	done atomic.Bool
}

func (h *ohpHooks) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	if h.at == "OnNodeEnd" && id == "a" && err == nil && h.done.CompareAndSwap(false, true) {
		ohpNodeEndExplodes()
	}
	return ctx
}

func (h *ohpHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	if ev.Type == compose_v2.CpEventType {
		// Run 级的存档事件：at 写成 "cp.<操作>"。清档只认成功收尾那次。
		if h.at == "cp."+ev.Content && !ohpIsAbortCleanup(ev) && h.done.CompareAndSwap(false, true) {
			ohpCpEventExplodes()
		}
		return ctx
	}
	if h.at == "OnNodeEvent" && h.done.CompareAndSwap(false, true) {
		ohpNodeEventExplodes()
	}
	return ctx
}

func ohpIsAbortCleanup(ev compose_v2.Event) bool {
	a, ok := ev.Metadata["action"].(*compose_v2.CpAction)
	return ok && a.Cleanup != nil && a.Cleanup.Reason != "success"
}

func (h *ohpHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	if h.at == "OnGraphEnd" && err == nil && h.done.CompareAndSwap(false, true) {
		ohpGraphEndExplodes()
	}
	return ctx
}

//go:noinline
func ohpNodeEndExplodes() { panic("node end boom") }

//go:noinline
func ohpNodeEventExplodes() { panic("node event boom") }

//go:noinline
func ohpGraphEndExplodes() { panic("graph end boom") }

//go:noinline
func ohpCpEventExplodes() { panic("cp event boom") }

// ohpGraph：a（做一次副作用、发一个事件、写 x）→ b（读 x）。返回 a 做副作用的次数与 b 读到的 x。
func ohpGraph(t *testing.T) (*compose_v2.Graph, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	return ohpGraphWith(t, false)
}

// ohpGraphWith 同 ohpGraph；bFailsOnce 时 b 第一次报一个可重试的业务错（留下失败档，下一次 Run 从 b 续）。
func ohpGraphWith(t *testing.T, bFailsOnce bool) (*compose_v2.Graph, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var sideEffects, bSaw atomic.Int64
	var bFailed atomic.Bool
	x := compose_v2.LastValue[int]("ohp_x")
	b := compose_v2.NewBuilder().WithName("ohp")
	compose_v2.WithChannel(b, x)
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		n := sideEffects.Add(1) // 扣款这类副作用：做了就不该再做
		if sink := compose_v2.GetEventSink(ctx); sink != nil {
			sink.Send(compose_v2.Event{Type: "progress", Content: "已扣款"})
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(x, int(n))}, nil
	})
	b.AddNode("b", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if bFailsOnce && bFailed.CompareAndSwap(false, true) {
			return nil, errors.New("b transient")
		}
		v, _ := compose_v2.Get(chs, x)
		bSaw.Store(int64(v))
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g, &sideEffects, &bSaw
}

// ohpErrorStacks 数带 site 栈的 ERROR 日志条数。
func ohpErrorStacks(r *psLogRecorder, site string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.Level != slog.LevelError {
			continue
		}
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "stack" && strings.Contains(a.Value.String(), "compose_v2_test."+site) {
				n++
			}
			return true
		})
	}
	return n
}

// TestObserverHookPanic_CompletedWorkNotRedone：钩子 panic 一次。调用方照常续跑（同 sid 再跑，最多两次）、或用 RunWithRetry 包住——
// a 的副作用只做一次，b 读到 a 那次的写入，Run 成功，那次 panic 记成一条带栈的 ERROR。
func TestObserverHookPanic_CompletedWorkNotRedone(t *testing.T) {
	for _, tc := range []struct{ at, site string }{
		{"OnNodeEnd", "ohpNodeEndExplodes"},
		{"OnNodeEvent", "ohpNodeEventExplodes"},
		{"OnGraphEnd", "ohpGraphEndExplodes"},
	} {
		t.Run(tc.at+"/续跑", func(t *testing.T) {
			logs := psCaptureLogs(t)
			g, sideEffects, bSaw := ohpGraph(t)
			h := &ohpHooks{at: tc.at}
			cp := compose_v2.NewInMemoryCheckpointer()
			run := func() error {
				return compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
					compose_v2.WithHooks(h), compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("ohp-"+tc.at))
			}
			err := run()
			for i := 0; i < 2 && err != nil; i++ {
				err = run()
			}
			if err != nil {
				t.Fatalf("续跑之后仍失败：%v", err)
			}
			if !h.done.Load() {
				t.Fatal("fixture 没搭对：钩子没 panic")
			}
			if n := sideEffects.Load(); n != 1 {
				t.Errorf("a 已经做完的副作用被重做了：做了 %d 次", n)
			}
			if v := bSaw.Load(); v != 1 {
				t.Errorf("b 应读到 a 那次的写入 1，实际 %d", v)
			}
			if n := ohpErrorStacks(logs, tc.site); n != 1 {
				t.Errorf("钩子那次 panic 应恰好记一条带栈的 ERROR，实际 %d 条", n)
			}
		})
		t.Run(tc.at+"/重试", func(t *testing.T) {
			g, sideEffects, _ := ohpGraph(t)
			h := &ohpHooks{at: tc.at}
			_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3), func(ctx context.Context) (struct{}, error) {
				return struct{}{}, compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithHooks(h))
			})
			if err != nil {
				t.Fatalf("重试之后仍失败：%v", err)
			}
			if n := sideEffects.Load(); n != 1 {
				t.Errorf("a 已经做完的副作用被重试重做了：做了 %d 次", n)
			}
		})
	}
}

// TestObserverHookPanic_CpEventDoesNotRedo：Run 级的存档事件（写档、载入、成功收尾的清档）同样只作观测，它的钩子
// panic 一次——续跑或重试之后 a 的副作用只做一次，Run 成功。曾经直接调钩子不兜：成功收尾清完档之后才 panic，Run
// 报错、档已经没了，重试从头再跑一遍。
func TestObserverHookPanic_CpEventDoesNotRedo(t *testing.T) {
	for _, tc := range []struct {
		at         string
		durability compose_v2.Durability
		bFailsOnce bool
	}{
		{at: "cp.cleanup"},
		{at: "cp.save", durability: compose_v2.DurabilitySync},
		{at: "cp.load", bFailsOnce: true},
	} {
		opts := func(h *ohpHooks, cp compose_v2.Checkpointer) []compose_v2.RunOption {
			o := []compose_v2.RunOption{compose_v2.WithHooks(h), compose_v2.WithCheckpointer(cp),
				compose_v2.WithSessionID("ohp-" + tc.at), compose_v2.WithEmitCpEvent(true)}
			if tc.durability != "" {
				o = append(o, compose_v2.WithDurability(tc.durability))
			}
			return o
		}
		t.Run(tc.at+"/续跑", func(t *testing.T) {
			logs := psCaptureLogs(t)
			g, sideEffects, bSaw := ohpGraphWith(t, tc.bFailsOnce)
			h := &ohpHooks{at: tc.at}
			cp := compose_v2.NewInMemoryCheckpointer()
			run := func() error {
				return compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts(h, cp)...)
			}
			runs := 1
			err := run()
			for i := 0; i < 3 && err != nil; i++ {
				runs++
				err = run()
			}
			if err != nil {
				t.Fatalf("续跑之后仍失败：%v", err)
			}
			want := 1
			if tc.bFailsOnce {
				want = 2 // b 自己失败那一次
			}
			if runs != want {
				t.Errorf("钩子 panic 让 Run 多失败了：跑了 %d 次才成功，应为 %d 次", runs, want)
			}
			if !h.done.Load() {
				t.Fatal("fixture 没搭对：钩子没 panic")
			}
			if n := sideEffects.Load(); n != 1 {
				t.Errorf("a 已经做完的副作用被重做了：做了 %d 次", n)
			}
			if v := bSaw.Load(); v != 1 {
				t.Errorf("b 应读到 a 那次的写入 1，实际 %d", v)
			}
			if n := ohpErrorStacks(logs, "ohpCpEventExplodes"); n != 1 {
				t.Errorf("钩子那次 panic 应恰好记一条带栈的 ERROR，实际 %d 条", n)
			}
		})
		t.Run(tc.at+"/重试", func(t *testing.T) {
			g, sideEffects, _ := ohpGraphWith(t, tc.bFailsOnce)
			h := &ohpHooks{at: tc.at}
			cp := compose_v2.NewInMemoryCheckpointer()
			_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3), func(ctx context.Context) (struct{}, error) {
				return struct{}{}, compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts(h, cp)...)
			})
			if err != nil {
				t.Fatalf("重试之后仍失败：%v", err)
			}
			if !h.done.Load() {
				t.Fatal("fixture 没搭对：钩子没 panic")
			}
			if n := sideEffects.Load(); n != 1 {
				t.Errorf("a 已经做完的副作用被重试重做了：做了 %d 次", n)
			}
		})
	}
}
