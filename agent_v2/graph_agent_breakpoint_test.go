package agent_v2_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// newBreakpointGraphAgent 是 a → b 两节点图包成的 GraphAgent：a 把输入加上前缀交给 b，b 产出最终答复。
func newBreakpointGraphAgent(t *testing.T, name string, aRuns, bRuns *atomic.Int32, opts ...agent_v2.GraphAgentOption) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("__bp_ga_in_" + name + "__")
	midCh := compose_v2.LastValue[*schema.Message]("__bp_ga_mid_" + name + "__")
	outCh := compose_v2.LastValue[*schema.Message]("__bp_ga_out_" + name + "__")
	bld := compose_v2.NewBuilder().WithName("bp_ga_" + name)
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, midCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("a", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		aRuns.Add(1)
		in, _ := compose_v2.Get(chs, inCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(midCh, schema.UserMessage("a:"+in.Content))}, nil
	})
	bld.AddNode("b", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		bRuns.Add(1)
		mid, _ := compose_v2.Get(chs, midCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("b:"+mid.Content))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "a")
	bld.AddEdge("a", "b")
	bld.AddEdge("b", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := []agent_v2.GraphAgentOption{
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	}
	return agent_v2.NewGraphAgent(g, append(base, opts...)...)
}

func requireBreakpoint(t *testing.T, err error) *compose_v2.AskQuery {
	t.Helper()
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("该在断点处停下，实际 %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 1 || qs[0].Breakpoint == nil {
		t.Fatalf("该是一条断点挂起，实际 %+v", qs)
	}
	return qs[0]
}

// GraphAgent 构造期声明的断点：Run 停在 b 之前，Resume（不带答案）放行跑完。
func TestGraphAgent_InterruptBefore_StopsAndResumes(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	ga := newBreakpointGraphAgent(t, "bp_ga_before", &aRuns, &bRuns, agent_v2.WithGraphInterruptBefore("b"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ga.Run(ctx, sess, schema.UserMessage("hi"))
	q := requireBreakpoint(t, err)
	if !slices.Equal(q.Breakpoint.Before, []compose_v2.NodeID{"b"}) {
		t.Errorf("Breakpoint = %+v，want Before=[b]", q.Breakpoint)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 0 {
		t.Fatalf("停在 b 之前，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
	events, err := ga.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("Resume 就是放行，该跑完，实际 %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "b:a:hi" {
		t.Errorf("最终答复该是 b:a:hi，实际 %+v", final)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Errorf("a 不重跑、b 跑一次，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// 嵌在别的编排里：断点挂起照常冒到最外层，最外层 Resume 放行。
func TestGraphAgent_Breakpoint_NestedInSequential(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	ga := newBreakpointGraphAgent(t, "bp_ga_nested", &aRuns, &bRuns, agent_v2.WithGraphInterruptAfter("a"))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("bp_seq")).AddStep("g", ga)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("hi"))
	q := requireBreakpoint(t, err)
	if !slices.Equal(q.Breakpoint.After, []compose_v2.NodeID{"a"}) || q.Breakpoint.NS == "" {
		t.Errorf("Breakpoint = %+v，want After=[a] 且 NS 是 GraphAgent 那张图的", q.Breakpoint)
	}
	events, err := seq.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("外层 Resume 该放行跑完，实际 %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "b:a:hi" {
		t.Errorf("最终答复该是 b:a:hi，实际 %+v", final)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Errorf("a 不重跑、b 跑一次，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// 闸门拦下不走：Resume 带 Abort，b 不执行、存档清掉。
func TestGraphAgent_Breakpoint_AbortStops(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	ga := newBreakpointGraphAgent(t, "bp_ga_abort", &aRuns, &bRuns, agent_v2.WithGraphInterruptBefore("b"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ga.Run(ctx, sess, schema.UserMessage("hi"))
	requireBreakpoint(t, err)
	if _, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		t.Fatalf("Abort 该干净返回，实际 %v", err)
	}
	if bRuns.Load() != 0 {
		t.Errorf("拦下之后 b 不该执行，实际 %d", bRuns.Load())
	}
	if _, err := ga.Resume(ctx, sess.RunID); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Errorf("Abort 之后再 Resume 该报 ErrNoPendingCheckpoint，实际 %v", err)
	}
}

// 断点里的节点名不在图里：构造期 panic。
func TestGraphAgent_Breakpoint_UnknownNodePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("节点名不在图里时 NewGraphAgent 该 panic")
		}
	}()
	var a, b atomic.Int32
	newBreakpointGraphAgent(t, "bp_ga_unknown", &a, &b, agent_v2.WithGraphInterruptBefore("nope"))
}

// 逐步执行：Run 与每次 Resume 都带上断点——Resume 停在下一个边界，不是一路跑完。
//
// sanity-revert：GraphAgent.Resume 不带 breakpointOptions ⟹ 第一次 Resume 直接跑完，本用例红。
func TestGraphAgent_Breakpoint_StepThroughCarriedOnResume(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	ga := newBreakpointGraphAgent(t, "bp_ga_step", &aRuns, &bRuns, agent_v2.WithGraphInterruptBefore(compose_v2.AllNodes))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ga.Run(ctx, sess, schema.UserMessage("hi"))
	if q := requireBreakpoint(t, err); !slices.Equal(q.Breakpoint.Before, []compose_v2.NodeID{"a"}) {
		t.Fatalf("第一次停在 a 之前，实际 %+v", q.Breakpoint)
	}
	_, err = ga.Resume(ctx, sess.RunID)
	if q := requireBreakpoint(t, err); !slices.Equal(q.Breakpoint.Before, []compose_v2.NodeID{"b"}) {
		t.Fatalf("第二次停在 b 之前，实际 %+v", q.Breakpoint)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 0 {
		t.Fatalf("一次 Resume 只越过一个边界，实际 a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
	if _, err := ga.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("最后一次 Resume 该跑完，实际 %v", err)
	}
	if bRuns.Load() != 1 {
		t.Errorf("b 跑一次，实际 %d", bRuns.Load())
	}
}

// 配了断点、ctx 上却没有 checkpointer：停下之后 Resume 续不了（续跑要存档）、重跑又停在同一个断点，这个任务
// 永远过不去——入口直接报错，一个节点都不执行；错不可重试（配置错，重跑必然一样）。
//
// sanity-revert：入口不查 ⟹ Run 照样停在断点上、返回中断，本用例红。
func TestGraphAgent_BreakpointWithoutCheckpointerRejected(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	ga := newBreakpointGraphAgent(t, "bp_nocp", &aRuns, &bRuns, agent_v2.WithGraphInterruptBefore("b"))
	_, err := ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil {
		t.Fatal("配了断点又没有 checkpointer，该报错")
	}
	if _, ok := compose_v2.IsInterrupt(err); ok {
		t.Fatalf("不该停在断点上（停下了也续不了），实际 %v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("配置错重跑必然一样，不该可重试：%v", err)
	}
	if aRuns.Load() != 0 {
		t.Errorf("入口就该拒绝、一个节点都不执行，实际 a 执行了 %d 次", aRuns.Load())
	}
}

// 断点是图编译期声明的（GraphBuilder.WithInterruptAfter）也一样。
func TestGraphAgent_CompiledBreakpointWithoutCheckpointerRejected(t *testing.T) {
	inCh := compose_v2.LastValue[*schema.Message]("__bp_ga_compiled_in__")
	var runs atomic.Int32
	bld := compose_v2.NewBuilder().WithName("bp_ga_compiled")
	compose_v2.WithChannel(bld, inCh)
	bld.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		runs.Add(1)
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "a")
	bld.AddEdge("a", compose_v2.EndNode)
	bld.WithInterruptAfter("a")
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("bp_ga_compiled"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("done") }))
	_, err = ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil {
		t.Fatal("图里声明了断点又没有 checkpointer，该报错")
	}
	if _, ok := compose_v2.IsInterrupt(err); ok {
		t.Fatalf("不该停在断点上，实际 %v", err)
	}
	if runs.Load() != 0 {
		t.Errorf("入口就该拒绝，实际 a 执行了 %d 次", runs.Load())
	}
}
