package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// GraphAgent 显式编排路径 — node 组件类型透传端到端测试 (Phase 2).
//
// ────────────────────────────────────────────────────────────────────────
// 验完整链路: AddNode(WithNodeComponent("retrieval")) → Compile 拷贝 nodeComponents
// → graph_loop runNodeWithHooks 在 fire OnNodeStart 前 WithComponentKind push →
// OnNodeStart ctx 含 component (observability 据此标 node span operation.name=retrieval).
//
// **黑盒真跑 graph** (非模拟 ctx): 用 WithHooks 接捕获 hook, 在 OnNodeStart 读
// GetComponentKind(ctx) 验证 graph_loop 真 push 了. 对照: 无声明 node 不带 component.
//
// **sanity-revert**: 删 graph_loop.go runNodeWithHooks 内 `if componentKind != "" {
// ctx = WithComponentKind(...) }` → captured["retrieve"]="" → 此测 FAIL.
// ────────────────────────────────────────────────────────────────────────

// captureComponentHooks 嵌入 NoOp, 覆写 OnNodeStart 捕获 ctx 上的 component kind.
type captureComponentHooks struct {
	compose_v2.NoOpOrchestrationHooks
	captured map[string]string
}

func (c *captureComponentHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	c.captured[string(id)] = compose_v2.GetComponentKind(ctx)
	return ctx, nil
}

func TestNodeComponent_GraphLoopPushesComponentKind(t *testing.T) {
	capHooks := &captureComponentHooks{captured: map[string]string{}}

	g, err := compose_v2.NewBuilder().
		AddNode("retrieve", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}, compose_v2.WithNodeComponent("retrieval")).
		AddNode("plain", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}). // 无 WithNodeComponent 声明 — 对照组
		AddEdge(compose_v2.StartNode, "retrieve").
		AddEdge("retrieve", "plain").
		AddEdge("plain", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(capHooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 核心: retrieve node 声明 component=retrieval, graph_loop 应在 fire OnNodeStart 前 push.
	if capHooks.captured["retrieve"] != "retrieval" {
		t.Errorf("graph_loop 应 push component=retrieval 到 retrieve node OnNodeStart ctx, got %q\n"+
			"链路: AddNode WithNodeComponent → Compile 拷贝 nodeComponents → graph_loop runNodeWithHooks push",
			capHooks.captured["retrieve"])
	}
	// 对照: plain node 无声明, ctx 不应有 component.
	if c := capHooks.captured["plain"]; c != "" {
		t.Errorf("无声明 node 不应有 component, got %q", c)
	}
}

// TestNodeComponent_NoLeakToInnerGraph — component 不应跨 graph 边界泄漏到子流程.
//
// **Phase 2 跨层泄漏回归**: graph_loop push component 到 ctx 后, ctx 流给 NodeFunc.
// 若 NodeFunc 内启动子 graph / sub-agent (如 ReflectAgent 的 evaluator sub-agent —
// 它内部有 model/tools node), 子流程的 OnNodeStart/OnAgentStart 会**继承**读到父 node
// 的 component → 子流程所有 node 被误标. evaluator 场景必然触发 (evaluator 内部 model
// node 全标 evaluator).
//
// 修复: graph_loop 进入 NodeFunc 前应 consume/清除 component (无声明 node 也 set 空,
// 覆盖继承的父值), 让 component 只作用于声明它的那一层.
//
// 此测: outer node 声明 retrieval, NodeFunc 内跑子 graph (inner 无声明). 修复前 inner
// 继承 retrieval (FAIL), 修复后 inner 不继承 (PASS).
func TestNodeComponent_NoLeakToInnerGraph(t *testing.T) {
	innerHooks := &captureComponentHooks{captured: map[string]string{}}

	subG, err := compose_v2.NewBuilder().
		AddNode("inner", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "inner").
		AddEdge("inner", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("Compile subG: %v", err)
	}

	parentG, err := compose_v2.NewBuilder().
		AddNode("outer", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// outer NodeFunc 内跑子 graph (模拟 node 内启动子流程, 如 sub-agent.Run).
			// ctx 是 graph_loop 传入的 (含 outer 声明的 component).
			return nil, compose_v2.Run(ctx, subG, compose_v2.NewChannelManager(), compose_v2.WithHooks(innerHooks))
		}, compose_v2.WithNodeComponent("retrieval")).
		AddEdge(compose_v2.StartNode, "outer").
		AddEdge("outer", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("Compile parentG: %v", err)
	}

	if err := compose_v2.Run(context.Background(), parentG, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// inner node 不应继承 outer 的 retrieval component.
	if c := innerHooks.captured["inner"]; c == "retrieval" {
		t.Errorf("component 跨 graph 泄漏: outer 的 retrieval 污染了子 graph inner node (got %q)\n"+
			"graph_loop 应在进入 NodeFunc 前 consume/清除 component, 防 sub-graph/sub-agent 继承误标", c)
	}
}
