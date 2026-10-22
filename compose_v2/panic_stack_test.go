package compose_v2_test

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// 兜住 panic 时的栈（compose_v2.PanicError）与日志——每个用例都断言三件事（Goexit 那条只断言结论照样记下）：
//
//   - errors.As 取得到 PanicError，栈里有 panic 发生处的函数名（各 ps* 函数都 noinline，名字不随编译器变）；
//   - 错误文本不含栈；
//   - 恰好记一条带这个栈的日志（同一次 panic 不在嵌套的每一层各记一次）。
//
// 另有两条钉住「挂上记录不改错误的判定」：原来 %w 接上 panic 值的兜底点 errors.Is 仍认得出，原来只取文本的
// （OnGraphStart / OnNodeStart 钩子）仍认不出。

var errPSSentinel = errors.New("ps sentinel boom")

//go:noinline
func psNodeBodyExplodes(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	panic(errPSSentinel)
}

//go:noinline
func psBranchAExplodes(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	panic("branch a boom")
}

//go:noinline
func psBranchBExplodes(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	panic("branch b boom")
}

//go:noinline
func psCondExplodes(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
	panic("cond boom")
}

//go:noinline
func psReducerExplodes(_, _ int) int { panic("reducer boom") }

func psNoop(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }

// psHooks 在指定的钩子里 panic。
type psHooks struct {
	compose_v2.NoOpOrchestrationHooks
	at string
}

func (h psHooks) OnGraphStart(ctx context.Context, chs compose_v2.Channels) (context.Context, error) {
	if h.at == "OnGraphStart" {
		psGraphStartExplodes()
	}
	return ctx, nil
}

func (h psHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, chs compose_v2.Channels) (context.Context, error) {
	if h.at == "OnNodeStart" {
		psNodeStartExplodes()
	}
	return ctx, nil
}

func (h psHooks) OnGraphEnd(ctx context.Context, chs compose_v2.Channels, err error) context.Context {
	if h.at == "OnGraphEnd" {
		psGraphEndExplodes()
	}
	return ctx
}

//go:noinline
func psGraphStartExplodes() { panic(errPSSentinel) }

//go:noinline
func psNodeStartExplodes() { panic(errPSSentinel) }

//go:noinline
func psGraphEndExplodes() { panic("graph end boom") }

func psSingleNodeGraph(t *testing.T, fn compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("ps_single")
	b.AddNode("n", fn)
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// psAssertPanicStack 断言 err 上挂着带 site 的栈、错误文本不含栈。
func psAssertPanicStack(t *testing.T, err error, site string) *compose_v2.PanicError {
	t.Helper()
	if err == nil {
		t.Fatal("该返回错误")
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As 取不到 PanicError：%v", err)
	}
	if !strings.Contains(pe.Stack, "compose_v2_test."+site) {
		t.Fatalf("栈里没有 panic 发生处 %s：\n%s", site, pe.Stack)
	}
	if strings.Contains(err.Error(), "goroutine ") {
		t.Errorf("错误文本不该带栈：%q", err.Error())
	}
	return pe
}

func TestPanicStack_NodeBody(t *testing.T) {
	logs := psCaptureLogs(t)
	err := compose_v2.Run(context.Background(), psSingleNodeGraph(t, psNodeBodyExplodes), compose_v2.NewChannelManager())

	psAssertPanicStack(t, err, "psNodeBodyExplodes")
	if !strings.Contains(err.Error(), "node panic recovered: "+errPSSentinel.Error()) {
		t.Errorf("错误文本该与原来相同，实际 %q", err.Error())
	}
	// 节点体兜底点原来用 %w 接上 panic 值：挂上记录之后仍认得出
	if !errors.Is(err, errPSSentinel) {
		t.Error("errors.Is 该认得出 panic 值（这个兜底点原来就用 %w）")
	}
	if n := logs.countStack("psNodeBodyExplodes"); n != 1 {
		t.Errorf("该恰好记一条带栈的日志，实际 %d 条", n)
	}
}

// TestPanicStack_FanOutEachBranchLogged：两个并发成员都 panic，Run 只返其中一个的错误——另一个的栈只在
// 日志里，所以两个都要各记一条。
func TestPanicStack_FanOutEachBranchLogged(t *testing.T) {
	logs := psCaptureLogs(t)
	b := compose_v2.NewBuilder().WithName("ps_fanout")
	b.AddNode("a", psBranchAExplodes)
	b.AddNode("b", psBranchBExplodes)
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge(compose_v2.StartNode, "b")
	b.AddEdge("a", compose_v2.EndNode)
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())

	var pe *compose_v2.PanicError
	if !errors.As(runErr, &pe) {
		t.Fatalf("errors.As 取不到 PanicError：%v", runErr)
	}
	// Run 返回的是其中一个分支的错误（哪一个由失败分支的仲裁定），它上面的栈是那个分支的
	if !strings.Contains(pe.Stack, "compose_v2_test.psBranchAExplodes") && !strings.Contains(pe.Stack, "compose_v2_test.psBranchBExplodes") {
		t.Errorf("错误上的栈里没有任何一个分支的 panic 发生处：\n%s", pe.Stack)
	}
	if strings.Contains(runErr.Error(), "goroutine ") {
		t.Errorf("错误文本不该带栈：%q", runErr.Error())
	}
	for _, site := range []string{"psBranchAExplodes", "psBranchBExplodes"} {
		if n := logs.countStack(site); n != 1 {
			t.Errorf("%s：该恰好记一条带栈的日志，实际 %d 条", site, n)
		}
	}
}

func TestPanicStack_Cond(t *testing.T) {
	logs := psCaptureLogs(t)
	b := compose_v2.NewBuilder().WithName("ps_cond")
	b.AddNode("n", psNoop)
	b.AddNode("x", psNoop)
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddConditionalEdge("n", psCondExplodes)
	b.AddEdge("x", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())

	psAssertPanicStack(t, runErr, "psCondExplodes")
	if n := logs.countStack("psCondExplodes"); n != 1 {
		t.Errorf("该恰好记一条带栈的日志，实际 %d 条", n)
	}
}

// TestPanicStack_HooksKeepOpaqueJudgement 钉住「挂上记录不改错误的判定」：OnGraphStart / OnNodeStart 钩子的
// 兜底点原来只取 panic 值的文本（%v），panic 一个 error 时 errors.Is 认不出它——挂上记录之后也认不出。
// 记录要是露出了 panic 值，这里 panic 的若是中断或 ctx 错，就会被当成真中断、真取消。
func TestPanicStack_HooksKeepOpaqueJudgement(t *testing.T) {
	for _, tc := range []struct{ at, site string }{
		{"OnGraphStart", "psGraphStartExplodes"},
		{"OnNodeStart", "psNodeStartExplodes"},
	} {
		t.Run(tc.at, func(t *testing.T) {
			logs := psCaptureLogs(t)
			err := compose_v2.Run(context.Background(), psSingleNodeGraph(t, psNoop), compose_v2.NewChannelManager(),
				compose_v2.WithHooks(psHooks{at: tc.at}))

			psAssertPanicStack(t, err, tc.site)
			if errors.Is(err, errPSSentinel) {
				t.Error("这个兜底点原来只取文本：errors.Is 不该认出 panic 值")
			}
			if n := logs.countStack(tc.site); n != 1 {
				t.Errorf("该恰好记一条带栈的日志，实际 %d 条", n)
			}
		})
	}
}

// TestPanicStack_OnGraphEnd：图失败时 OnGraphEnd panic 落在 Run 入口那道兜底，错误上挂着栈、只记一条日志。（图成功时它的 panic
// 就地兜住、不改结果，见 observer_hook_panic_test.go。）
func TestPanicStack_OnGraphEnd(t *testing.T) {
	logs := psCaptureLogs(t)
	err := compose_v2.Run(context.Background(), psSingleNodeGraph(t, func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, errors.New("业务错")
	}), compose_v2.NewChannelManager(),
		compose_v2.WithHooks(psHooks{at: "OnGraphEnd"}))

	psAssertPanicStack(t, err, "psGraphEndExplodes")
	if !strings.Contains(err.Error(), "compose_v2.Run panic recovered") {
		t.Errorf("该落在 Run 入口那道兜底，实际 %q", err.Error())
	}
	if n := logs.countStack("psGraphEndExplodes"); n != 1 {
		t.Errorf("该恰好记一条带栈的日志，实际 %d 条", n)
	}
}

// TestPanicStack_ReducerPanicLoggedAtRunExit：reducer 在提交时 panic，兜底点持着 cm 写锁、当场不记日志，由
// Run 出口补记——嵌在外层图的节点里跑时也只记一次（外层 Run 出口不再补记）。
func TestPanicStack_ReducerPanicLoggedAtRunExit(t *testing.T) {
	ch := compose_v2.Custom[int]("ps_acc", psReducerExplodes, 0)
	inner := func(t *testing.T) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("ps_reducer")
		b.AddNode("w", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(ch, 1)}, nil
		})
		b.AddEdge(compose_v2.StartNode, "w")
		b.AddEdge("w", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	innerCM := func(t *testing.T) *compose_v2.ChannelManager {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.RegisterChannel(cm, ch); err != nil {
			t.Fatal(err)
		}
		return cm
	}

	t.Run("top", func(t *testing.T) {
		logs := psCaptureLogs(t)
		err := compose_v2.Run(context.Background(), inner(t), innerCM(t))
		psAssertPanicStack(t, err, "psReducerExplodes")
		if !strings.Contains(err.Error(), "ApplyAll: reducer panic") {
			t.Fatalf("该是提交时（持锁区）兜住的，实际 %q", err.Error())
		}
		if n := logs.countStack("psReducerExplodes"); n != 1 {
			t.Errorf("该恰好补记一条带栈的日志，实际 %d 条", n)
		}
	})

	t.Run("nested", func(t *testing.T) {
		logs := psCaptureLogs(t)
		ig := inner(t)
		outer := psSingleNodeGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, compose_v2.Run(ctx, ig, innerCM(t))
		})
		err := compose_v2.Run(context.Background(), outer, compose_v2.NewChannelManager())
		psAssertPanicStack(t, err, "psReducerExplodes")
		if n := logs.countStack("psReducerExplodes"); n != 1 {
			t.Errorf("嵌套时也该只记一条，实际 %d 条", n)
		}
	})
}

// TestPanicStack_NestedNodePanicLoggedOnce：内层图的节点 panic，外层节点把错误原样返回——兜底点记一条，
// 两层 Run 出口都不再记。
func TestPanicStack_NestedNodePanicLoggedOnce(t *testing.T) {
	logs := psCaptureLogs(t)
	ig := psSingleNodeGraph(t, psNodeBodyExplodes)
	outer := psSingleNodeGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.Run(ctx, ig, compose_v2.NewChannelManager())
	})
	err := compose_v2.Run(context.Background(), outer, compose_v2.NewChannelManager())

	psAssertPanicStack(t, err, "psNodeBodyExplodes")
	if n := logs.countStack("psNodeBodyExplodes"); n != 1 {
		t.Errorf("该恰好记一条带栈的日志，实际 %d 条", n)
	}
}

// TestPanicStack_RestoreUnmarshalPanic：续跑恢复时业务 UnmarshalJSON panic（Restore 持着 cm 写锁）。经 Run 续跑时
// 由 Run 出口补记一条；业务直接调 cm.Restore 时没有 Run 出口，错误上照样挂着栈，日志由调用方记。
func TestPanicStack_RestoreUnmarshalPanic(t *testing.T) {
	chBad := compose_v2.LastValue[*panickyJSONValue]("ps_bad")
	src := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(src, chBad); err != nil {
		t.Fatal(err)
	}
	if err := src.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(chBad, &panickyJSONValue{X: 1})}); err != nil {
		t.Fatal(err)
	}
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	fresh := func(t *testing.T) *compose_v2.ChannelManager {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.RegisterChannel(cm, chBad); err != nil {
			t.Fatal(err)
		}
		return cm
	}
	const site = "(*panickyJSONValue).UnmarshalJSON"

	t.Run("via Run", func(t *testing.T) {
		logs := psCaptureLogs(t)
		err := compose_v2.Run(context.Background(), psSingleNodeGraph(t, psNoop), fresh(t), compose_v2.WithResumeFrom(snap))
		psAssertPanicStack(t, err, site)
		if !strings.Contains(err.Error(), "Restore panic (rolled back)") {
			t.Fatalf("该是 Restore（持锁区）兜住的，实际 %q", err.Error())
		}
		if n := logs.countStack(site); n != 1 {
			t.Errorf("该由 Run 出口补记一条，实际 %d 条", n)
		}
	})

	t.Run("direct", func(t *testing.T) {
		logs := psCaptureLogs(t)
		err := fresh(t).Restore(snap)
		psAssertPanicStack(t, err, site)
		if n := logs.countStack(site); n != 0 {
			t.Errorf("直接调 Restore 没有 Run 出口，框架不该记日志（持锁区里记不了），实际 %d 条", n)
		}
	})
}

// psPanicCp 的 SaveNS 每次都 panic——async 写档在后台 goroutine 里兜住它；calls 数它被调了几次。
type psPanicCp struct {
	*compose_v2.InMemoryCheckpointer
	calls atomic.Int32
}

func (c *psPanicCp) SaveNS(context.Context, string, string, *compose_v2.Checkpoint) error {
	c.calls.Add(1)
	psSaveNSExplodes()
	return nil
}

//go:noinline
func psSaveNSExplodes() { panic("ps SaveNS boom") }

func TestPanicStack_AsyncDurabilityWriter(t *testing.T) {
	logs := psCaptureLogs(t)
	g, _, maxCh := loopGraph(t, 3)
	cp := &psPanicCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	err := runLoop(t, g, maxCh, 3,
		compose_v2.WithDurability(compose_v2.DurabilityAsync),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s_ps_async"))

	psAssertPanicStack(t, err, "psSaveNSExplodes")
	// 每次 SaveNS 都 panic、每次 panic 记一条。后台写档出错之后后续的写档会跳过，但 Run 收尾时可能还会同步写一次
	// （看时序），那是另一次 panic、由 Run 入口那道兜住另记一条——所以按调用次数比，不写死 1
	calls := int(cp.calls.Load())
	if n := logs.countStack("psSaveNSExplodes"); n != calls || calls == 0 {
		t.Errorf("每次 SaveNS 的 panic 该恰好记一条：调了 %d 次，记了 %d 条", calls, n)
	}
}

// psGoexitHandler 在 ERROR 记录上调 runtime.Goexit——测试里「一有错误日志就 t.FailNow」的 handler 就是这个形状。
type psGoexitHandler struct{}

func (psGoexitHandler) Enabled(context.Context, slog.Level) bool { return true }
func (psGoexitHandler) Handle(_ context.Context, rec slog.Record) error {
	if rec.Level >= slog.LevelError {
		runtime.Goexit()
	}
	return nil
}
func (h psGoexitHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h psGoexitHandler) WithGroup(string) slog.Handler      { return h }

// psEndHook 记下 OnGraphEnd 收到的错误。
type psEndHook struct {
	compose_v2.NoOpOrchestrationHooks
	called *bool
	err    *error
}

func (h psEndHook) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	*h.called, *h.err = true, err
	return ctx
}

// TestPanicStack_HandlerGoexitKeepsOutcome：兜底点记日志时 handler 调了 runtime.Goexit，兜底点要记下的结论照样
// 记下——成员的失败不变成幽灵成功、图主体的 panic 照样让 OnGraphEnd 看到错误、Run 照常返回。
func TestPanicStack_HandlerGoexitKeepsOutcome(t *testing.T) {
	psCaptureLogs(t) // 只为了在用例结束时连 log 包输出一起还原；下面换成会 Goexit 的 handler
	slog.SetDefault(slog.New(psGoexitHandler{}))

	t.Run("fan-out member", func(t *testing.T) {
		b := compose_v2.NewBuilder().WithName("ps_goexit_fanout")
		b.AddNode("a", psNoop)
		b.AddNode("b", psNoop)
		b.AddNode("x", psNoop)
		b.AddEdge(compose_v2.StartNode, "a")
		b.AddEdge(compose_v2.StartNode, "b")
		// 条件边返回一个 Unwrap 会 panic 的错误：panic 发生在框架对它做链式检查时，由成员 goroutine 顶上那道兜住
		b.AddConditionalEdge("a", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
			return "", &unwrapPanicErr{}
		})
		b.AddEdge("b", compose_v2.EndNode)
		b.AddEdge("x", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		var runErr error
		returned := false
		done := make(chan struct{})
		go func() {
			defer close(done)
			runErr = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
			returned = true
		}()
		<-done
		if !returned {
			t.Fatal("Run 没有返回：日志 handler 的 Goexit 掐断了 Run 所在的 goroutine")
		}
		if runErr == nil || !strings.Contains(runErr.Error(), "out-edge panic recovered") {
			t.Fatalf("成员的失败该照样记下，实际 err=%v（nil 就是幽灵成员被当成功）", runErr)
		}
	})

	t.Run("graph body", func(t *testing.T) {
		b := compose_v2.NewBuilder().WithName("ps_goexit_body")
		b.AddNode("n", psNoop)
		b.AddNode("x", psNoop)
		b.AddEdge(compose_v2.StartNode, "n")
		// 单成员走 fast path：出边计算在 Run 的 goroutine 上，panic 由 Run 出口那道兜住
		b.AddConditionalEdge("n", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
			return "", &unwrapPanicErr{}
		})
		b.AddEdge("x", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		var endCalled, returned bool
		var endErr, runErr error
		done := make(chan struct{})
		go func() {
			defer close(done)
			runErr = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
				compose_v2.WithHooks(psEndHook{called: &endCalled, err: &endErr}))
			returned = true
		}()
		<-done
		if !endCalled || endErr == nil {
			t.Fatalf("OnGraphEnd 该看到图主体 panic 转成的错误，called=%v err=%v", endCalled, endErr)
		}
		if !returned || runErr == nil {
			t.Fatalf("Run 该照常返回错误，returned=%v err=%v", returned, runErr)
		}
	})
}

// ── 日志捕获 ──

type psLogRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *psLogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *psLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.recs = append(r.recs, rec.Clone())
	r.mu.Unlock()
	return nil
}
func (r *psLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *psLogRecorder) WithGroup(string) slog.Handler      { return r }

// countStack 数 stack 字段里含 site 的日志条数。
func (r *psLogRecorder) countStack(site string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "stack" && strings.Contains(a.Value.String(), "compose_v2_test."+site) {
				n++
			}
			return true
		})
	}
	return n
}

// psCaptureLogs 换掉 slog 的默认 handler 收日志。log 包的输出要一起还原：slog.SetDefault 换成自定义 handler
// 时会把 log 包的输出改道过去，换回原来的默认 logger 时不会改回来。
func psCaptureLogs(t *testing.T) *psLogRecorder {
	t.Helper()
	rec := &psLogRecorder{}
	oldDefault := slog.Default()
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})
	return rec
}
