package compose_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

func noopNode(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	return nil, nil
}

// TestCompile_RejectsStartAndEndAsNodeIDs：起止节点名不能当普通节点 ID。叫 "__end__" 的节点
// 会被当成图的终点、永远不执行，Run 以 err == nil 结束；AddNode 与 AddSubgraphNode 两条路都要拦。
func TestCompile_RejectsStartAndEndAsNodeIDs(t *testing.T) {
	sub, err := compose_v2.NewBuilder().AddNode("inner", noopNode).
		AddEdge(compose_v2.StartNode, "inner").AddEdge("inner", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}
	for _, id := range []compose_v2.NodeID{compose_v2.StartNode, compose_v2.EndNode} {
		viaNode := compose_v2.NewBuilder().AddNode("a", noopNode).AddNode(id, noopNode).
			AddEdge(compose_v2.StartNode, "a").AddEdge("a", compose_v2.EndNode)
		if _, err := viaNode.Compile(); err == nil || !strings.Contains(err.Error(), string(id)) {
			t.Errorf("AddNode(%q) should fail to compile, got %v", id, err)
		}
		viaSubgraph := compose_v2.NewBuilder().AddNode("a", noopNode).
			AddEdge(compose_v2.StartNode, "a").AddEdge("a", compose_v2.EndNode)
		viaSubgraph.AddSubgraphNode(id, sub)
		if _, err := viaSubgraph.Compile(); err == nil || !strings.Contains(err.Error(), string(id)) {
			t.Errorf("AddSubgraphNode(%q) should fail to compile, got %v", id, err)
		}
	}
}

// TestCompile_RejectsStaticEdgeBackToStart：静态边不能指向起点。放过去会在运行时炸在「节点
// 未注册」上，还留下一份 frontier 里带着起点的可续跑存档，每次续跑原地再炸。
func TestCompile_RejectsStaticEdgeBackToStart(t *testing.T) {
	b := compose_v2.NewBuilder().AddNode("a", noopNode).
		AddEdge(compose_v2.StartNode, "a").AddEdge("a", compose_v2.StartNode)
	if _, err := b.Compile(); err == nil {
		t.Fatal("an edge back to the start node should fail to compile")
	}
}
