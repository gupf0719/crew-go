package compose_v2_test

import (
	"context"
	htmltemplate "html/template"
	"strings"
	"testing"
	texttemplate "text/template"

	"crew-go/compose_v2"
)

func contractNode(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	return nil, nil
}

func compileContractGraph(t *testing.T, b *compose_v2.GraphBuilder) *compose_v2.Graph {
	t.Helper()
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestCheckpointIdentity_TopologyWithPublicIDUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options func(*compose_v2.GraphBuilder) *compose_v2.GraphBuilder
	}{
		{name: "无名", options: func(b *compose_v2.GraphBuilder) *compose_v2.GraphBuilder { return b }},
		{name: "显式名称", options: func(b *compose_v2.GraphBuilder) *compose_v2.GraphBuilder { return b.WithName("workflow") }},
		{name: "相同前缀", options: func(b *compose_v2.GraphBuilder) *compose_v2.GraphBuilder { return b.WithIdentityPrefix("workflow") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			build := func(target compose_v2.NodeID, reverseDeclarations bool) *compose_v2.Graph {
				b := tc.options(compose_v2.NewBuilder())
				if reverseDeclarations {
					b.AddNode("next", contractNode).AddNode("gate", contractNode)
					b.AddEdge("next", compose_v2.EndNode)
					compose_v2.WithChannel(b, compose_v2.Sum("second"))
					compose_v2.WithChannel(b, compose_v2.Sum("first"))
				} else {
					b.AddNode("gate", contractNode).AddNode("next", contractNode)
					compose_v2.WithChannel(b, compose_v2.Sum("first"))
					compose_v2.WithChannel(b, compose_v2.Sum("second"))
					b.AddEdge("next", compose_v2.EndNode)
				}
				b.AddEdge(compose_v2.StartNode, "gate").AddEdge("gate", target)
				return compileContractGraph(t, b)
			}
			first := build("next", false)
			rebuilt := build("next", true)
			changed := build(compose_v2.EndNode, false)
			if first.ID() != rebuilt.ID() || first.ID() != changed.ID() {
				t.Fatalf("公开 ID 应沿用旧规则，实际 %q、%q、%q", first.ID(), rebuilt.ID(), changed.ID())
			}
			if first.CheckpointIdentity() != rebuilt.CheckpointIdentity() {
				t.Fatal("相同图重建须沿用身份，独立声明顺序不应影响身份")
			}
			if first.CheckpointIdentity() == changed.CheckpointIdentity() {
				t.Fatal("只改后继节点也应改变存档身份")
			}
			if identity := first.CheckpointIdentity(); !strings.HasPrefix(identity, "graph-v1:") || len(identity) != len("graph-v1:")+64 {
				t.Fatalf("存档身份须带版本且使用完整摘要，实际 %q", identity)
			}
		})
	}
}

func contractTestBuilder() *compose_v2.GraphBuilder {
	return compose_v2.NewBuilder().WithName("workflow").
		AddNode("gate", contractNode).AddNode("left", contractNode).AddNode("right", contractNode).
		AddEdge(compose_v2.StartNode, "gate").
		AddEdge("left", compose_v2.EndNode).AddEdge("right", compose_v2.EndNode)
}

func TestCheckpointIdentity_EdgeOrderAndConditionalPolicy(t *testing.T) {
	leftFirst := compileContractGraph(t, contractTestBuilder().AddEdge("gate", "left").AddEdge("gate", "right"))
	rightFirst := compileContractGraph(t, contractTestBuilder().AddEdge("gate", "right").AddEdge("gate", "left"))
	if leftFirst.CheckpointIdentity() == rightFirst.CheckpointIdentity() {
		t.Fatal("同源出边顺序影响并发成员执行，顺序改变须区分存档身份")
	}
	chooseLeft := func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) { return "left", nil }
	abort := compileContractGraph(t, contractTestBuilder().AddConditionalEdge("gate", chooseLeft))
	rerun := compileContractGraph(t, contractTestBuilder().AddConditionalEdge("gate", chooseLeft,
		compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom)))
	static := compileContractGraph(t, contractTestBuilder().AddEdge("gate", "left"))
	if abort.CheckpointIdentity() == static.CheckpointIdentity() || abort.CheckpointIdentity() == rerun.CheckpointIdentity() {
		t.Fatal("静态边、条件边及条件失败策略应能区分")
	}
}

func TestCheckpointIdentity_ChannelTypeAndMerge(t *testing.T) {
	sum := compileContractGraph(t, compose_v2.WithChannel(contractTestBuilder(), compose_v2.Sum("value")))
	custom := compileContractGraph(t, compose_v2.WithChannel(contractTestBuilder(),
		compose_v2.Custom("value", func(a, b int) int { return a - b }, 0)))
	if sum.CheckpointIdentity() == custom.CheckpointIdentity() {
		t.Fatal("同类型但使用不同合并契约的 channel 应改变身份")
	}

	// reflect.Type.String() 对两种类型均给出 *template.Template。
	textGraph := compileContractGraph(t, compose_v2.WithChannel(contractTestBuilder(),
		compose_v2.LastValue[*texttemplate.Template]("value")))
	htmlGraph := compileContractGraph(t, compose_v2.WithChannel(contractTestBuilder(),
		compose_v2.LastValue[*htmltemplate.Template]("value")))
	if textGraph.CheckpointIdentity() == htmlGraph.CheckpointIdentity() {
		t.Fatal("同短名的不同包类型应改变身份")
	}

	// register 首次接到同名 channel 时确定 reducer，后续同类型注册不会取代它。
	sumFirst := compose_v2.WithChannel(contractTestBuilder(), compose_v2.Sum("value"))
	compose_v2.WithChannel(sumFirst, compose_v2.LastValue[int]("value"))
	lastFirst := compose_v2.WithChannel(contractTestBuilder(), compose_v2.LastValue[int]("value"))
	compose_v2.WithChannel(lastFirst, compose_v2.Sum("value"))
	if compileContractGraph(t, sumFirst).CheckpointIdentity() == compileContractGraph(t, lastFirst).CheckpointIdentity() {
		t.Fatal("同名 channel 的绑定顺序改变了实际合并契约")
	}
}

func TestCheckpointIdentity_StableOwnerAndRevisions(t *testing.T) {
	base := compileContractGraph(t, contractTestBuilder().WithCheckpointRevision("base-v1"))
	otherBase := compileContractGraph(t, contractTestBuilder().WithCheckpointRevision("base-v2"))
	identity := base.CheckpointIdentity()
	wrapped := base.WithCheckpointOwner("wrapper").WithCheckpointRevision("wrapper-v1")
	rebuilt := compileContractGraph(t, contractTestBuilder().WithCheckpointRevision("base-v1")).
		WithCheckpointRevision("wrapper-v1").WithCheckpointOwner("wrapper")
	if wrapped.CheckpointIdentity() != rebuilt.CheckpointIdentity() {
		t.Fatal("相同所有者与业务版本重建后应沿用身份")
	}
	if wrapped.ID() != base.ID() || base.CheckpointIdentity() != identity {
		t.Fatal("派生所有者和版本不得改写底层原图与公开 ID")
	}
	for _, g := range []*compose_v2.Graph{
		base, base.WithCheckpointOwner("other").WithCheckpointRevision("wrapper-v1"),
		base.WithCheckpointOwner("wrapper").WithCheckpointRevision("wrapper-v2"),
		otherBase.WithCheckpointOwner("wrapper").WithCheckpointRevision("wrapper-v1"),
	} {
		if g.CheckpointIdentity() == wrapped.CheckpointIdentity() {
			t.Fatal("所有者、构建版本和包装版本都应参与身份")
		}
	}
}

func TestCheckpointIdentity_ContextRevisionPreservesWrapper(t *testing.T) {
	base := compileContractGraph(t, contractTestBuilder().WithCheckpointRevision("base"))
	ctx := compose_v2.WithCheckpointRevisionCtx(t.Context(), "deployment")
	wrapped := base.WithCheckpointOwner("wrapper").WithCheckpointRevision("wrapper-v1")
	combined := wrapped.WithCheckpointContext(ctx)
	rebuilt := compileContractGraph(t, contractTestBuilder().WithCheckpointRevision("base")).
		WithCheckpointContext(ctx).WithCheckpointOwner("wrapper").WithCheckpointRevision("wrapper-v1")
	if id := combined.CheckpointIdentity(); combined.WithCheckpointContext(ctx) != combined ||
		rebuilt.CheckpointIdentity() != id {
		t.Fatal("相同的部署版本应可重复应用，并可重建出同一存档身份")
	}
	if combined.ID() != base.ID() || combined.WithCheckpointContext(t.Context()).CheckpointIdentity() != wrapped.CheckpointIdentity() {
		t.Fatal("上下文版本不改变公开 ID，也不污染原图的版本")
	}
	for _, other := range []*compose_v2.Graph{
		wrapped,
		combined.WithCheckpointRevision("wrapper-v2"),
		combined.WithCheckpointContext(compose_v2.WithCheckpointRevisionCtx(t.Context(), "deployment-v2")),
	} {
		if other.CheckpointIdentity() == combined.CheckpointIdentity() {
			t.Fatal("包装器版本和部署版本独立参与存档身份")
		}
	}
}

func TestCheckpointIdentity_StaticSubgraphContract(t *testing.T) {
	value := compose_v2.Sum("value")
	build := func(rev string, options ...compose_v2.SubgraphOption) *compose_v2.Graph {
		child := compileContractGraph(t, contractTestBuilder().WithCheckpointRevision(rev))
		b := compose_v2.NewBuilder().WithName("parent")
		compose_v2.WithChannel(b, value)
		b.AddSubgraphNode("child", child, options...).AddEdge(compose_v2.StartNode, "child").
			AddEdge("child", compose_v2.EndNode)
		return compileContractGraph(t, b)
	}
	old := build("v1", compose_v2.WithImport(value))
	if old.CheckpointIdentity() != build("v1", compose_v2.WithImport(value)).CheckpointIdentity() {
		t.Fatal("同合同子图重建应沿用父图身份")
	}
	for _, changed := range []*compose_v2.Graph{
		build("v2", compose_v2.WithImport(value)),
		build("v1", compose_v2.WithExport(value)),
		build("v1", compose_v2.WithImport(value), compose_v2.WithSubgraphIsolatedResumeContext()),
	} {
		if old.ID() != changed.ID() || old.CheckpointIdentity() == changed.CheckpointIdentity() {
			t.Fatal("子图版本或边界契约变化应改变父图存档身份，公开 ID 保持不变")
		}
	}
}
