package compose_v2_test

// OnGraphStart 中断拒绝判据哨兵（graph_loop.go 的 enteredFromNodeBody）。
//
// 钉的缺陷形态：判据曾是 `cfg.graphNS == ""`（ns 字符串空不空）——业务在**顶层**
// 自己 push 一个 ns（租户前缀）时被误判成嵌套 ⟹ 中断放行 ⟹ IsInterrupt=true 但
// 挂起档 0，前端照常渲染问题、用户答完 Resume 撞 ErrNoPendingCheckpoint。
//
// 判据换成引擎盖的**节点执行章**（在 clearInheritedNodePosition 之前抠出）：
// 「我是不是从某个节点体里被调进来的」——嵌套子图为真（中断冒给父节点路径落父档），
// 顶层无论带不带 ns 都为假。
//
// sanity-revert：把 enteredFromNodeBody 换回 `cfg.graphNS == ""`，形态 B 会红。

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// graphStartInterruptHooks 在**第 fireOn 次** OnGraphStart 抛中断。
// 嵌套用例要打的是**子图**那次（第 2 次），打父图那次等于又测了一遍顶层。
type graphStartInterruptHooks struct {
	compose_v2.NoOpOrchestrationHooks
	calls  *int
	fireOn int
}

func (h graphStartInterruptHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	*h.calls++
	if *h.calls != h.fireOn {
		return ctx, nil
	}
	return ctx, &compose_v2.InterruptError{
		Reason: "graph pause",
		Query:  &compose_v2.AskQuery{InterruptID: "gs-1", Question: "暂停?"},
	}
}

func buildNoopGraph(t *testing.T) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// 形态 A：顶层、ns 为空 —— 拒（既有行为，防回归）
func TestGraphStartInterrupt_TopLevelBareIsRejected(t *testing.T) {
	calls := 0
	err := compose_v2.Run(context.Background(), buildNoopGraph(t), compose_v2.NewChannelManager(),
		compose_v2.WithHooks(graphStartInterruptHooks{calls: &calls, fireOn: 1}))
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("顶层该拒（没有能落档的地方），却放行成中断: %v", err)
	}
	if !strings.Contains(err.Error(), "没有可落的存档") {
		t.Fatalf("错误正文该说明原因, got %v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("接线错误重试必然一样，不该判为可重试")
	}
}

// 形态 B：顶层、但业务自己 push 了 ns —— 仍该拒（判据换章前这里放行成假暂停）
func TestGraphStartInterrupt_TopLevelWithNSStillRejected(t *testing.T) {
	calls := 0
	cper := compose_v2.NewInMemoryCheckpointer()
	err := compose_v2.Run(context.Background(), buildNoopGraph(t), compose_v2.NewChannelManager(),
		compose_v2.WithHooks(graphStartInterruptHooks{calls: &calls, fireOn: 1}),
		compose_v2.WithRunNS("tenant-a"),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("gs-sid"))
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("顶层带 ns 仍该拒——ns 是寻址串不代表有父节点接得住: %v", err)
	}
	// 承重维度：假暂停的实质是「说挂起了、却没有任何档」——直接数档
	pending, lerr := cper.ListPending(context.Background(), "gs-sid")
	if lerr != nil {
		t.Fatalf("ListPending: %v", lerr)
	}
	if len(pending) != 0 {
		t.Fatalf("被拒后不该留下挂起档, got %d", len(pending))
	}
}

// 形态 C：嵌套子图 —— 放行（中断冒给父节点路径落父档），且真的落了档
func TestGraphStartInterrupt_NestedIsAllowedAndCheckpointed(t *testing.T) {
	calls := 0
	sub := buildNoopGraph(t)

	parent := compose_v2.NewBuilder()
	parent.AddSubgraphNode("sub", sub)
	parent.AddEdge(compose_v2.StartNode, "sub")
	pg, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	// hooks 经 ctx 透传才会被子图继承（subgraph.go 的 sub Run 走 GetOrchHooks，
	// 不转发 RunOption）——只挂 WithHooks 的话子图那次 OnGraphStart 根本不 fire。
	// fireOn=2：第 1 次是父图（顶层），第 2 次才是子图——要打的是后者。
	hooks := graphStartInterruptHooks{calls: &calls, fireOn: 2}
	ctx := compose_v2.WithOrchHooks(context.Background(), hooks)
	runErr := compose_v2.Run(ctx, pg, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(hooks),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("gs-nested"))
	if _, isInt := compose_v2.IsInterrupt(runErr); !isInt {
		t.Fatalf("嵌套该放行（父节点路径接得住）, got %v", runErr)
	}
	// 正向断言：放行不算数，得真有档可续
	pending, lerr := cper.ListPending(ctx, "gs-nested")
	if lerr != nil {
		t.Fatalf("ListPending: %v", lerr)
	}
	if len(pending) == 0 {
		t.Fatalf("放行了却没落档 —— 那就是假暂停，跟拒绝要消灭的是同一个东西")
	}
}
