package compose_v2_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

// softCancelGraph 造一个跑 maxRounds 超步的 loop；step 节点在 counter==triggerAt 时经 ctx 拿
// 软取消信号 Trigger（triggerAt<0 = 不触发）。节点经 GetSoftCancel(ctx) 拿信号也顺带验 ctx 透传。
func softCancelGraph(t *testing.T, maxRounds, triggerAt int) (*compose_v2.Graph, *compose_v2.Channel[int], *compose_v2.Channel[int]) {
	t.Helper()
	counterCh := compose_v2.Sum("sc_counter")
	maxCh := compose_v2.LastValue[int]("sc_max")
	b := compose_v2.NewBuilder().
		AddNode("step", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			count, _ := compose_v2.Get(chs, counterCh)
			if count == triggerAt {
				if s := compose_v2.GetSoftCancel(ctx); s != nil {
					s.Trigger("node reached trigger point")
				}
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(counterCh, 1)}, nil
		}).
		AddEdge(compose_v2.StartNode, "step").
		AddConditionalEdge("step", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			count, _ := compose_v2.Get(chs, counterCh)
			if count >= compose_v2.MustGet(chs, maxCh) {
				return compose_v2.EndNode, nil
			}
			return "step", nil
		}).
		WithMaxVisit(50)
	b = compose_v2.WithChannel(b, counterCh)
	b = compose_v2.WithChannel(b, maxCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, counterCh, maxCh
}

// TestSoftCancel_StopsAndResumes：核心闭环——软取消触发 → 当前超步善终后返 ErrSoftCanceled +
// 存档；新信号 Run 2 从 cp auto-load 续跑到 max（证「优雅停 + 可 Resume」）。
func TestSoftCancel_StopsAndResumes(t *testing.T) {
	maxRounds, triggerAt := 6, 3
	g, counterCh, maxCh := softCancelGraph(t, maxRounds, triggerAt)
	cp := compose_v2.NewInMemoryCheckpointer()
	sid := "sc_resume"
	sig := compose_v2.NewSoftCancelSignal()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithSoftCancel(sig),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	if !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("应返 ErrSoftCanceled，实际 %v", err)
	}
	// 当前超步善终：触发那一超步的写已 apply → counter = triggerAt+1（不是 triggerAt，证不是
	// 中途掐而是边界停）。
	if got := compose_v2.MustGet(cm.Channels(), counterCh); got != triggerAt+1 {
		t.Errorf("触发超步应善终，counter 应 %d，实际 %d", triggerAt+1, got)
	}

	// Run 2：新信号（不触发）+ 同 sid → auto-load 该 Pending cp 续跑到 max。
	g2, counterCh2, maxCh2 := softCancelGraph(t, maxRounds, -1)
	cm2 := registerCM(t, counterCh2, maxCh2)
	if err := compose_v2.Run(context.Background(), g2, cm2,
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)); err != nil {
		t.Fatalf("Resume 应跑完，实际 %v", err)
	}
	if final := compose_v2.MustGet(cm2.Channels(), counterCh2); final != maxRounds {
		t.Errorf("Resume 应续到 %d，实际 %d", maxRounds, final)
	}
}

// TestSoftCancel_NotTriggered_Completes：建了信号但没触发 → Run 正常跑完（不影响热路径）。
func TestSoftCancel_NotTriggered_Completes(t *testing.T) {
	maxRounds := 5
	g, counterCh, maxCh := softCancelGraph(t, maxRounds, -1) // 不触发
	sig := compose_v2.NewSoftCancelSignal()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithSoftCancel(sig)); err != nil {
		t.Fatalf("未触发应正常跑完，实际 %v", err)
	}
	if final := compose_v2.MustGet(cm.Channels(), counterCh); final != maxRounds {
		t.Errorf("应跑到 %d，实际 %d", maxRounds, final)
	}
}

// TestSoftCancel_DistinctFromCtxDone：ErrSoftCanceled 与 context.Canceled 是不同哨兵——业务能区分
// 「优雅停（可 Resume）」vs「硬停」。
func TestSoftCancel_DistinctFromCtxDone(t *testing.T) {
	maxRounds, triggerAt := 5, 2
	g, _, maxCh := softCancelGraph(t, maxRounds, triggerAt)
	sig := compose_v2.NewSoftCancelSignal()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithSoftCancel(sig))
	if !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("应是 ErrSoftCanceled，实际 %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Error("软取消不应是 context.Canceled（两者须可区分）")
	}
}

// TestSoftCancel_TerminalSuperstep_NoOp：Trigger 到最后一个超步(isEnd)是 no-op——Run 正常
// 成功完成(err==nil)，软取消丢弃（任务本就要结束）。固化「边界丢弃语义」。
func TestSoftCancel_TerminalSuperstep_NoOp(t *testing.T) {
	maxRounds := 5
	// triggerAt=maxRounds-1：触发那超步写 +1 → counter=maxRounds → isEnd return nil（早于下轮软取消检查）
	g, counterCh, maxCh := softCancelGraph(t, maxRounds, maxRounds-1)
	sig := compose_v2.NewSoftCancelSignal()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithSoftCancel(sig)); err != nil {
		t.Fatalf("末超步触发应正常成功完成(err==nil)，实际 %v", err)
	}
	if final := compose_v2.MustGet(cm.Channels(), counterCh); final != maxRounds {
		t.Errorf("应跑完到 %d，实际 %d", maxRounds, final)
	}
}

// TestSoftCancel_NoCheckpointer：软取消触发但没配 Checkpointer → 返 ErrSoftCanceled 不崩、无 cp
// （优雅停 ≠ 一定能 Resume，Resume 需 cp）。
func TestSoftCancel_NoCheckpointer(t *testing.T) {
	maxRounds, triggerAt := 5, 2
	g, _, maxCh := softCancelGraph(t, maxRounds, triggerAt)
	sig := compose_v2.NewSoftCancelSignal()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithInitialWrites(compose_v2.Write(maxCh, maxRounds)),
		compose_v2.WithSoftCancel(sig)) // 无 WithCheckpointer
	if !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("无 checkpointer 软取消应返 ErrSoftCanceled，实际 %v", err)
	}
}

// TestSoftCancel_NestedViaCtx：嵌套子图经 ctx 继承父信号——子图节点触发，子图在自己的超步边界
// 停并把 ErrSoftCanceled 抛回父（顶层 Trigger 停整棵树的核心）。
func TestSoftCancel_NestedViaCtx(t *testing.T) {
	maxRounds, triggerAt := 5, 2
	child, _, childMaxCh := softCancelGraph(t, maxRounds, triggerAt)
	var childErr error
	pb := compose_v2.NewBuilder().
		AddNode("runner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 沿用父 ctx（含软取消信号）调子图 → 子图 resolveSoftCancel 继承同一信号
			childErr = compose_v2.Run(ctx, child, compose_v2.NewChannelManager(),
				compose_v2.WithInitialWrites(compose_v2.Write(childMaxCh, maxRounds)))
			return nil, childErr // 子图 ErrSoftCanceled 往上抛
		}).
		AddEdge(compose_v2.StartNode, "runner").
		AddEdge("runner", compose_v2.EndNode)
	parent, err := pb.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}
	sig := compose_v2.NewSoftCancelSignal()
	ctx := compose_v2.WithSoftCancelCtx(context.Background(), sig) // 顶层 push 信号
	perr := compose_v2.Run(ctx, parent, compose_v2.NewChannelManager())
	if !errors.Is(childErr, compose_v2.ErrSoftCanceled) {
		t.Fatalf("子图应继承父软取消信号并触发，childErr=%v", childErr)
	}
	if !errors.Is(perr, compose_v2.ErrSoftCanceled) {
		t.Errorf("父 Run 应传播子图 ErrSoftCanceled（%%w 链完整），实际 %v", perr)
	}
}

// TestSoftCancel_ReasonVisibility：并发下读到 triggered=true 时 reason 一定已可见（验 Trigger 里
// 「先 Store reason 后 Store triggered」的 happens-before，-race 加固）。
func TestSoftCancel_ReasonVisibility(t *testing.T) {
	for iter := 0; iter < 1000; iter++ {
		s := compose_v2.NewSoftCancelSignal()
		done := make(chan struct{})
		go func() {
			s.Trigger("must-be-visible")
			close(done)
		}()
		for {
			if tr, reason := s.Triggered(); tr {
				if reason != "must-be-visible" {
					t.Fatalf("读到 triggered 时 reason 应已可见，实际 %q", reason)
				}
				break
			}
		}
		<-done
	}
}

// TestSoftCancel_Signal：信号 Trigger/Triggered 基本语义。
func TestSoftCancel_Signal(t *testing.T) {
	s := compose_v2.NewSoftCancelSignal()
	if tr, _ := s.Triggered(); tr {
		t.Fatal("新信号不应已触发")
	}
	s.Trigger("boom")
	if tr, reason := s.Triggered(); !tr || reason != "boom" {
		t.Errorf("触发后应 (true, boom)，实际 (%v, %q)", tr, reason)
	}
}

// TestSoftCancel_Signal_Concurrent：并发 Trigger + Triggered（-race 验 atomic 无竞态）。
func TestSoftCancel_Signal_Concurrent(t *testing.T) {
	s := compose_v2.NewSoftCancelSignal()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Trigger("g")
			_, _ = s.Triggered()
		}()
	}
	wg.Wait()
	if tr, _ := s.Triggered(); !tr {
		t.Fatal("并发 Trigger 后应已触发")
	}
}
