package compose_v2_test

// 「有人接得住中断」的章（withinNodeScope）哨兵。
//
// 钉的缺陷形态：这个章曾与「中断身份从哪来」共用同一个 key（nodeID）——而两者的
// 盖章时机要求相反：身份章必须晚到 OnNodeStart 之后（hook 相位框架一维身份都不给，
// 是刻意的），落档能力却在进节点那一刻就成立。合一的代价：OnNodeStart 里跑的子运行，
// 其 OnGraphStart 抛的中断被判成「没人接得住」而拒绝，可外层早退路径明明会校验并
// 冒泡落档——白拒一次、挂起档 0。
//
// sanity-revert：把 graph_loop.go 的 enteredFromNodeBody 换回 GetCurrentNodeID(ctx)!=""，
// 本测试红在「内层被误拒」。

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

type nsInnerPause struct {
	compose_v2.NoOpOrchestrationHooks
}

func (nsInnerPause) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return ctx, &compose_v2.InterruptError{
		Reason: "inner pause",
		Query:  &compose_v2.AskQuery{InterruptID: "inner-1", Question: "?"},
	}
}

type nsOuterHooks struct {
	compose_v2.NoOpOrchestrationHooks
	inner *error
}

func (h nsOuterHooks) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	sb := compose_v2.NewBuilder()
	sb.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	sb.AddEdge(compose_v2.StartNode, "x")
	sg, _ := sb.Compile()
	err := compose_v2.Run(ctx, sg, compose_v2.NewChannelManager(), compose_v2.WithHooks(nsInnerPause{}))
	*h.inner = err
	return ctx, err // hook 把它返出去——早退路径会校验并冒泡
}

func TestNodeScopeStamp_InnerRunFromNodeStartPhaseIsCatchable(t *testing.T) {
	var innerErr error
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "n")
	g, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	outerErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(nsOuterHooks{inner: &innerErr}),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("gapa2"))

	_, innerIsInt := compose_v2.IsInterrupt(innerErr)
	_, outerIsInt := compose_v2.IsInterrupt(outerErr)
	t.Logf("内层 Run 返回中断: %v (%v)", innerIsInt, innerErr)
	t.Logf("外层 Run 返回中断: %v", outerIsInt)
	pending, _ := cper.ListPending(context.Background(), "gapa2")
	t.Logf("挂起档: %d", len(pending))
	if !innerIsInt {
		t.Errorf("内层被误拒——它其实接得住（外层 OnNodeStart 早退路径会校验+冒泡落档）")
	}
	// 正向断言：放行不算数，得真有档可续
	if len(pending) == 0 {
		t.Errorf("放行了却没落档——那就是假暂停，跟拒绝要消灭的是同一个东西")
	}
	_ = outerIsInt
}
