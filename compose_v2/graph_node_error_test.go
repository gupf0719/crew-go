package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// TestGraph_NodeError_WrappedAsGraphNodeError 验证业务节点 error 被包成 typed
// *GraphNodeError，caller 可 errors.As 定位 NodeID + errors.Is 穿透原错。
//
// sanity-revert：把 runNodeWithHooks 业务错路径的 `&GraphNodeError{...}` 改回
// `fmt.Errorf("node %q: %w", id, err)` 时，errors.As(&GraphNodeError) 应 fail。
func TestGraph_NodeError_WrappedAsGraphNodeError(t *testing.T) {
	bizErr := errors.New("biz failure")
	b := compose_v2.NewBuilder().
		AddNode("failer", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, bizErr
		}).
		AddEdge(compose_v2.StartNode, "failer").
		AddEdge("failer", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	if runErr == nil {
		t.Fatal("应返业务错")
	}

	var gne *compose_v2.GraphNodeError
	if !errors.As(runErr, &gne) {
		t.Fatalf("业务节点错应包成 *GraphNodeError，实际 %T: %v", runErr, runErr)
	}
	if gne.NodeID != "failer" {
		t.Errorf("GraphNodeError.NodeID 应 'failer'，实际 %q", gne.NodeID)
	}
	if !errors.Is(runErr, bizErr) {
		t.Error("errors.Is 应穿透 GraphNodeError.Unwrap 命中原业务错")
	}
}

// TestGraph_InterruptError_NotWrappedAsGraphNodeError 验证 InterruptError（控制流）
// 不被包成 GraphNodeError——保持 IsInterrupt 可识别。
func TestGraph_InterruptError_NotWrappedAsGraphNodeError(t *testing.T) {
	b := compose_v2.NewBuilder().
		AddNode("asker", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_1", Question: "确认？"},
			}
		}).
		AddEdge(compose_v2.StartNode, "asker").
		AddEdge("asker", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	if runErr == nil {
		t.Fatal("应返 InterruptError")
	}
	if _, ok := compose_v2.IsInterrupt(runErr); !ok {
		t.Fatalf("InterruptError 应被 IsInterrupt 命中，实际 %v", runErr)
	}
	var gne *compose_v2.GraphNodeError
	if errors.As(runErr, &gne) {
		t.Error("InterruptError 是控制流，不应被包成 GraphNodeError")
	}
}

// TestGraph_NodeError_NestedGraphNS 验证嵌套子图（Run 注入 graphNS）的业务错，
// GraphNodeError 携带正确的 GraphNS——补 audit LOW-2 的嵌套回归覆盖。
func TestGraph_NodeError_NestedGraphNS(t *testing.T) {
	bizErr := errors.New("nested biz failure")
	b := compose_v2.NewBuilder().
		AddNode("failer", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, bizErr
		}).
		AddEdge(compose_v2.StartNode, "failer").
		AddEdge("failer", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// WithRunNS 模拟嵌入子图位置——节点 GetGraphNS 应拿到此 ns
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithRunNS("exec/researcher"))
	if runErr == nil {
		t.Fatal("应返业务错")
	}

	var gne *compose_v2.GraphNodeError
	if !errors.As(runErr, &gne) {
		t.Fatalf("应包成 *GraphNodeError，实际 %T: %v", runErr, runErr)
	}
	if gne.GraphNS != "exec/researcher" {
		t.Errorf("嵌套 GraphNS 应 'exec/researcher'，实际 %q", gne.GraphNS)
	}
	if gne.NodeID != "failer" {
		t.Errorf("NodeID 应 'failer'，实际 %q", gne.NodeID)
	}
}
