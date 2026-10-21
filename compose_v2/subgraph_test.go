package compose_v2_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// PoC §4.4 场景 D：嵌套子图 import/export + audit F5 多层嵌套传递性验证。

// ── 单层子图 import/export ──

// TestSubgraph_ImportFromParent 验证子图能读父图 channel。
func TestSubgraph_ImportFromParent(t *testing.T) {
	parentMsgCh := compose_v2.LastValue[string]("parent_msg")

	// 子图：读 parentMsg，写到自己的 result channel
	subResultCh := compose_v2.LastValue[string]("sub_result")
	subB := compose_v2.NewBuilder().
		AddNode("inner", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			parentVal, _ := compose_v2.Get(chs, parentMsgCh)
			return []compose_v2.ChannelWrite{
				compose_v2.Write(subResultCh, "echo: "+parentVal),
			}, nil
		}).
		AddEdge(compose_v2.StartNode, "inner").
		AddEdge("inner", compose_v2.EndNode)
	subB = compose_v2.WithChannel(subB, subResultCh)
	sub, err := subB.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}

	// 父图：写 parentMsg，嵌入子图，子图 import parentMsg + export sub_result
	parentB := compose_v2.NewBuilder().
		AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(parentMsgCh, "hello")}, nil
		}).
		AddSubgraphNode("sub", sub,
			compose_v2.WithImport(parentMsgCh),
			compose_v2.WithExport(subResultCh),
		).
		AddEdge(compose_v2.StartNode, "seed").
		AddEdge("seed", "sub").
		AddEdge("sub", compose_v2.EndNode)
	parentB = compose_v2.WithChannel(parentB, parentMsgCh)
	parentB = compose_v2.WithChannel(parentB, subResultCh) // 父图也注册 export channel
	parentG, err := parentB.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), parentG, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := compose_v2.MustGet(cm.Channels(), subResultCh)
	if got != "echo: hello" {
		t.Fatalf("export value 应是 'echo: hello' 实际 %q", got)
	}
}

// TestSubgraph_DefaultIsolated 验证默认隔离——未 import/export 的 channel
// 子图看不见、写不到。
func TestSubgraph_DefaultIsolated(t *testing.T) {
	parentSecretCh := compose_v2.LastValue[string]("parent_secret")
	subWroteCh := compose_v2.LastValue[bool]("sub_can_see_secret")

	subB := compose_v2.NewBuilder().
		AddNode("snoop", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 子图试图读 parentSecretCh——未 import，应读不到
			canSee := compose_v2.Has(chs, parentSecretCh)
			return []compose_v2.ChannelWrite{compose_v2.Write(subWroteCh, canSee)}, nil
		}).
		AddEdge(compose_v2.StartNode, "snoop").
		AddEdge("snoop", compose_v2.EndNode)
	subB = compose_v2.WithChannel(subB, subWroteCh)
	sub, err := subB.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}

	parentB := compose_v2.NewBuilder().
		AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(parentSecretCh, "TOP SECRET")}, nil
		}).
		AddSubgraphNode("sub", sub,
			// **故意**不 import parentSecretCh + export subWroteCh
			compose_v2.WithExport(subWroteCh),
		).
		AddEdge(compose_v2.StartNode, "seed").
		AddEdge("seed", "sub").
		AddEdge("sub", compose_v2.EndNode)
	parentB = compose_v2.WithChannel(parentB, parentSecretCh)
	parentB = compose_v2.WithChannel(parentB, subWroteCh)
	parentG, err := parentB.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), parentG, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	canSee := compose_v2.MustGet(cm.Channels(), subWroteCh)
	if canSee {
		t.Fatal("默认隔离时子图不应能读父 channel——但 Has 返回 true")
	}
}

// ── audit F5 [MED]：多层嵌套传递性 ──

// TestSubgraph_3LayerNested_ExplicitImport 验证 3 层嵌套（祖父→父→子）下
// channel 必须**逐层显式**声明 import，不自动透传。
//
// 撞 audit F5：原 PoC 设计文档没说传递性规则。本测试明确：
//   - 默认每层隔离
//   - 跨层 channel 必须每层都 import 才能传递到内层
//   - 不自动透传——避免"祖父图改 channel 类型，所有间接子图破坏"耦合
func TestSubgraph_3LayerNested_ExplicitImport(t *testing.T) {
	grandparentCh := compose_v2.LastValue[string]("from_grandparent")
	innerObservedCh := compose_v2.LastValue[bool]("inner_observed_value")
	innerEchoCh := compose_v2.LastValue[string]("inner_echo")

	// Layer 3: child（最内层）—— 试图读 grandparentCh
	childB := compose_v2.NewBuilder().
		AddNode("read", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			val, ok := compose_v2.Get(chs, grandparentCh)
			return []compose_v2.ChannelWrite{
				compose_v2.Write(innerObservedCh, ok),
				compose_v2.Write(innerEchoCh, val),
			}, nil
		}).
		AddEdge(compose_v2.StartNode, "read").
		AddEdge("read", compose_v2.EndNode)
	childB = compose_v2.WithChannel(childB, innerObservedCh)
	childB = compose_v2.WithChannel(childB, innerEchoCh)
	child, err := childB.Compile()
	if err != nil {
		t.Fatalf("child compile: %v", err)
	}

	// Layer 2: parent—— 嵌入 child，**显式** import grandparentCh + export inner channels
	parentB := compose_v2.NewBuilder().
		AddSubgraphNode("child_sub", child,
			compose_v2.WithImport(grandparentCh), // 透传到 child
			compose_v2.WithExport(innerObservedCh),
			compose_v2.WithExport(innerEchoCh),
		).
		AddEdge(compose_v2.StartNode, "child_sub").
		AddEdge("child_sub", compose_v2.EndNode)
	parentB = compose_v2.WithChannel(parentB, grandparentCh)
	parentB = compose_v2.WithChannel(parentB, innerObservedCh)
	parentB = compose_v2.WithChannel(parentB, innerEchoCh)
	parent, err := parentB.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}

	// Layer 1: grandparent—— 嵌入 parent，**显式** import grandparentCh
	grandparentB := compose_v2.NewBuilder().
		AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(grandparentCh, "from_top")}, nil
		}).
		AddSubgraphNode("parent_sub", parent,
			compose_v2.WithImport(grandparentCh), // 必须！否则 parent 看不到，更别说 child
			compose_v2.WithExport(innerObservedCh),
			compose_v2.WithExport(innerEchoCh),
		).
		AddEdge(compose_v2.StartNode, "seed").
		AddEdge("seed", "parent_sub").
		AddEdge("parent_sub", compose_v2.EndNode)
	grandparentB = compose_v2.WithChannel(grandparentB, grandparentCh)
	grandparentB = compose_v2.WithChannel(grandparentB, innerObservedCh)
	grandparentB = compose_v2.WithChannel(grandparentB, innerEchoCh)
	grandparentG, err := grandparentB.Compile()
	if err != nil {
		t.Fatalf("grandparent compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), grandparentG, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 child 真能看到 grandparent 的值（通过 parent 透传）
	if !compose_v2.MustGet(cm.Channels(), innerObservedCh) {
		t.Fatal("3 层显式 import 后 child 应能看到 grandparentCh，实际未观察到")
	}
	if got := compose_v2.MustGet(cm.Channels(), innerEchoCh); got != "from_top" {
		t.Fatalf("child 读到的值应是 'from_top'，实际 %q", got)
	}
}

// TestSubgraph_3LayerNested_MissingMiddleLayerImport 验证传递性的反向：
// 中间层不 import 时，最内层看不到祖父 channel——证明"不自动透传"。
func TestSubgraph_3LayerNested_MissingMiddleLayerImport(t *testing.T) {
	grandparentCh := compose_v2.LastValue[string]("from_grandparent")
	innerObservedCh := compose_v2.LastValue[bool]("inner_observed_value")

	// child 试图读 grandparentCh
	childB := compose_v2.NewBuilder().
		AddNode("read", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ok := compose_v2.Has(chs, grandparentCh)
			return []compose_v2.ChannelWrite{compose_v2.Write(innerObservedCh, ok)}, nil
		}).
		AddEdge(compose_v2.StartNode, "read").
		AddEdge("read", compose_v2.EndNode)
	childB = compose_v2.WithChannel(childB, innerObservedCh)
	child, _ := childB.Compile()

	// parent **故意**不 import grandparentCh——只 export inner result
	parentB := compose_v2.NewBuilder().
		AddSubgraphNode("child_sub", child,
			// 故意省略 WithImport(grandparentCh)
			compose_v2.WithExport(innerObservedCh),
		).
		AddEdge(compose_v2.StartNode, "child_sub").
		AddEdge("child_sub", compose_v2.EndNode)
	parentB = compose_v2.WithChannel(parentB, innerObservedCh)
	parent, _ := parentB.Compile()

	// grandparent 写 grandparentCh + 嵌入 parent（也 import grandparentCh 让 parent 自己能看到）
	grandparentB := compose_v2.NewBuilder().
		AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(grandparentCh, "from_top")}, nil
		}).
		AddSubgraphNode("parent_sub", parent,
			compose_v2.WithImport(grandparentCh), // parent 自己能看，但不会自动透传给 child
			compose_v2.WithExport(innerObservedCh),
		).
		AddEdge(compose_v2.StartNode, "seed").
		AddEdge("seed", "parent_sub").
		AddEdge("parent_sub", compose_v2.EndNode)
	grandparentB = compose_v2.WithChannel(grandparentB, grandparentCh)
	grandparentB = compose_v2.WithChannel(grandparentB, innerObservedCh)
	grandparentG, _ := grandparentB.Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), grandparentG, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// **关键验证**：parent 没显式 import grandparentCh 给 child，所以 child 看不到
	if compose_v2.MustGet(cm.Channels(), innerObservedCh) {
		t.Fatal("中间层不 import 时 child 不应能看到 grandparentCh，实际能看到——传递性自动透传是 bug")
	}
}

// TestSubgraph_MultiSubgraphs_AppendExport 验证多个子图 export 同 channel
// 时父 reducer 自动合并——典型 fan-out 子图聚合场景。
func TestSubgraph_MultiSubgraphs_AppendExport(t *testing.T) {
	resultsCh := compose_v2.Append[string]("all_results")

	// 复用同一子图模板（每个子图 export 自己的标记）
	makeSub := func(label string) *compose_v2.Graph {
		b := compose_v2.NewBuilder().
			AddNode("work", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return []compose_v2.ChannelWrite{
					compose_v2.Write(resultsCh, []string{"sub_" + label}),
				}, nil
			}).
			AddEdge(compose_v2.StartNode, "work").
			AddEdge("work", compose_v2.EndNode)
		b = compose_v2.WithChannel(b, resultsCh)
		g, _ := b.Compile()
		return g
	}

	parentB := compose_v2.NewBuilder().
		AddNode("d", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddSubgraphNode("sub_a", makeSub("a"), compose_v2.WithExport(resultsCh)).
		AddSubgraphNode("sub_b", makeSub("b"), compose_v2.WithExport(resultsCh)).
		AddSubgraphNode("sub_c", makeSub("c"), compose_v2.WithExport(resultsCh)).
		AddEdge(compose_v2.StartNode, "d").
		AddEdge("d", "sub_a").
		AddEdge("d", "sub_b").
		AddEdge("d", "sub_c").
		AddEdge("sub_a", compose_v2.EndNode).
		AddEdge("sub_b", compose_v2.EndNode).
		AddEdge("sub_c", compose_v2.EndNode)
	parentB = compose_v2.WithChannel(parentB, resultsCh)
	parent, err := parentB.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), parent, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	results := compose_v2.MustGet(cm.Channels(), resultsCh)
	if len(results) != 3 {
		t.Fatalf("应聚合 3 个子图 export，实际 %d (%v)", len(results), results)
	}
	sort.Strings(results)
	expected := []string{"sub_a", "sub_b", "sub_c"}
	for i, r := range results {
		if r != expected[i] {
			t.Fatalf("results[%d] 应是 %q 实际 %q", i, expected[i], r)
		}
	}
}

// TestSubgraph_ExportToUnregisteredParentChannel_Error 验证父图未注册同名 channel
// 时 export 报错——避免静默 drop。
func TestSubgraph_ExportToUnregisteredParentChannel_Error(t *testing.T) {
	subResultCh := compose_v2.LastValue[string]("sub_result")
	subB := compose_v2.NewBuilder().
		AddNode("inner", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(subResultCh, "hi")}, nil
		}).
		AddEdge(compose_v2.StartNode, "inner").
		AddEdge("inner", compose_v2.EndNode)
	subB = compose_v2.WithChannel(subB, subResultCh)
	sub, _ := subB.Compile()

	parentB := compose_v2.NewBuilder().
		AddSubgraphNode("sub", sub, compose_v2.WithExport(subResultCh)).
		AddEdge(compose_v2.StartNode, "sub").
		AddEdge("sub", compose_v2.EndNode)
	// **故意**不在父图 WithChannel(subResultCh) → export 时找不到 reducer
	parent, _ := parentB.Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), parent, cm)
	if err == nil {
		t.Fatal("父图未注册 export channel 应报错")
	}
	if !strings.Contains(err.Error(), "sub_result") || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error 应含 channel 名 + 'not registered'，实际 %q", err.Error())
	}
}

// 子图 export 撞上「父图没声明这个 channel」时，错误要指出**是父图缺声明**——
// 泛泛一句「没注册」不告诉业务该去哪儿加那一行。
//
// ⚠️ **这句话直到 ApplyAll 拆成规划/提交才第一次可达**：在那之前有一趟统一的
// 预校验跑在所有 apply 之前，对三种 write 一视同仁地用泛化措辞，于是 exportWrite
// 自己那句是死代码（审计端到端实测两棵树的输出确实不同）。既然现在它是特性了，
// 就得有人钉着——原有用例只断言 Contains("not registered")，新旧措辞都满足。
func TestSubgraph_ExportUnregistered_ErrorNamesParentGraph(t *testing.T) {
	sub := compose_v2.NewBuilder().WithName("exp_sub")
	out := compose_v2.LastValue[string]("exp_only_in_child")
	sub = compose_v2.WithChannel(sub, out)
	sub.AddNode("w", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(out, "v")}, nil
	})
	sub.AddEdge(compose_v2.StartNode, "w")
	sub.AddEdge("w", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	// 父图**故意不声明** exp_only_in_child
	b := compose_v2.NewBuilder().WithName("exp_parent")
	b.AddSubgraphNode("s", subG, compose_v2.WithExport(out))
	b.AddEdge(compose_v2.StartNode, "s")
	b.AddEdge("s", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	if runErr == nil {
		t.Fatal("父图没声明 export channel，该报错")
	}
	msg := runErr.Error()
	if !strings.Contains(msg, "in parent graph") {
		t.Errorf("错误该指出是父图缺声明，实际 %q", msg)
	}
	if !strings.Contains(msg, "exp_only_in_child") {
		t.Errorf("错误该带上 channel 名，实际 %q", msg)
	}
}
