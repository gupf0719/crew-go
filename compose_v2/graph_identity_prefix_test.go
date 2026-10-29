package compose_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// identityGraph 建一张单节点图：prefix 非空时用 WithIdentityPrefix，withCh 决定多不多一条 channel（结构变化）。
func identityGraph(t *testing.T, prefix string, withCh bool) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().
		AddNode("node1", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }).
		AddEdge(compose_v2.StartNode, "node1").
		AddEdge("node1", compose_v2.EndNode)
	if prefix != "" {
		b = b.WithIdentityPrefix(prefix)
	}
	if withCh {
		b = compose_v2.WithChannel(b, compose_v2.LastValue[string]("identity_prefix_ch"))
	}
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// 身份前缀区分结构相同的图，结构这一维照留：同前缀同结构同身份、前缀不同身份不同、结构变了身份照样变。
func TestGraphIdentityPrefix_QualifiesFingerprint(t *testing.T) {
	plain := identityGraph(t, "", false)
	a1, a2, b := identityGraph(t, "a", false), identityGraph(t, "a", false), identityGraph(t, "b", false)
	if a1.ID() != a2.ID() {
		t.Fatalf("同前缀同结构应同身份：%q vs %q", a1.ID(), a2.ID())
	}
	if a1.ID() == b.ID() {
		t.Fatalf("前缀不同、结构相同应是两个身份，都是 %q", a1.ID())
	}
	if a1.ID() != "a:"+plain.ID() {
		t.Fatalf("身份应是前缀加结构指纹 %q，实际 %q", "a:"+plain.ID(), a1.ID())
	}
	if aCh := identityGraph(t, "a", true); aCh.ID() == a1.ID() {
		t.Fatalf("前缀相同、结构变了应换身份，都是 %q", a1.ID())
	}
}

func TestGraphIdentityPrefix_ExclusiveWithName(t *testing.T) {
	_, err := compose_v2.NewBuilder().
		AddNode("node1", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }).
		AddEdge(compose_v2.StartNode, "node1").
		AddEdge("node1", compose_v2.EndNode).
		WithName("g").WithIdentityPrefix("p").
		Compile()
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("WithName 与 WithIdentityPrefix 同时设应编译报错，实际 %v", err)
	}
}
