package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// 在「没有执行位置」的地方取中断身份，必须拿到空串而不是一个看着像真的值。
//
// # 修的是什么
//
// 中断身份由执行位置派生。`OnNodeStart` / `OnGraphStart` / 顶层 `OnAgentStart` 触发时
// 那些位置维度还没建立，各维取零值 ⟹ hash 出一个**同 ns 下人人共用的常量**（实测根图
// 是 `a5f04c5e3163b86f`、`ns=sub` 是 `37b9ed664e90a903`）：同一个 ns 里两张不同的图、
// 不同 nodeID、不同 sid 拿到同一个值，而且照样落进存档（`Status=pending`）。
//
// 它比空 ID 更难发现——空一眼看得出坏了，这个是一串看着很正常的十六进制。

type siteIDRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	ids  map[compose_v2.NodeID]string
	lock chan struct{}
}

func (h siteIDRecorder) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	<-h.lock
	h.ids[id] = compose_v2.CurrentInterruptSite(ctx).InterruptID()
	h.lock <- struct{}{}
	return ctx, nil
}

// TestHookSite_NoPositionMeansNoID 图外取身份必须返空。
//
// ⚠️ **必须同时跑根图和子图两格**：判据是 `NodeID == ""`，而根图里 `GraphNS` 也恰好是
// 空——只跑根图的话，「判 NodeID」和「判 NodeID 且判 GraphNS」这两种实现**不可区分**。
// 实测过那个漏网：把判据收窄成 `NodeID=="" && GraphNS==""`，本文件三条全绿，而子图里
// 两张不同的图、不同 nodeID、不同 sid 照样共用 `37b9ed664e90a903`。
//
// ⚠️ 断言点是「**空**」而不是「两次不同」：后者在只有一维接上时也能过（比如 nodeID
// 接上了、任务代号还是空的 ⟹ 同图内不撞、跨任务照撞）。空是唯一不会漏的判据。
func TestHookSite_NoPositionMeansNoID(t *testing.T) {
	for _, ns := range []string{"", "sub"} {
		name := "根图"
		if ns != "" {
			name = "子图 ns=" + ns
		}
		t.Run(name, func(t *testing.T) {
			ids := map[compose_v2.NodeID]string{}
			lock := make(chan struct{}, 1)
			lock <- struct{}{}

			b := compose_v2.NewBuilder().WithName("hook_site_" + ns)
			for _, n := range []compose_v2.NodeID{"alpha", "beta"} {
				b.AddNode(n, func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					return nil, nil
				})
				b.AddEdge(compose_v2.StartNode, n)
				b.AddEdge(n, compose_v2.EndNode)
			}
			g, err := b.Compile()
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			opts := []compose_v2.RunOption{
				compose_v2.WithHooks(siteIDRecorder{ids: ids, lock: lock}),
				compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
				compose_v2.WithSessionID("hook_site_" + ns),
			}
			if ns != "" {
				opts = append(opts, compose_v2.WithRunNS(ns))
			}
			if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...); err != nil {
				t.Fatalf("run: %v", err)
			}
			// 前置：钩子真被调过。缺了这道，「全是空」在「钩子根本没跑」时也成立。
			if len(ids) != 2 {
				t.Fatalf("应记到 2 个节点，实际 %d：%v", len(ids), ids)
			}
			for node, id := range ids {
				if id != "" {
					t.Errorf("节点 %q 的 OnNodeStart 里取到了非空身份 %q——那个位置还没建立，"+
						"这个值是同 ns 下所有图所有节点共用的，业务拿它提问会跨任务串台", node, id)
				}
			}
		})
	}
}

// TestHookSite_FanoutInstanceAlsoNoID 扇出实例的钩子里同样不能有身份。
//
// ⚠️ **单独一格是因为实例身份的 push 早于钩子**（`WithNodeInstanceID` 在
// `hooks.OnNodeStart` 之前几行）。静态边节点的实例身份恒空，所以只跑静态边时，
// 「判 NodeID」和「判 NodeID 且判 InstanceID」不可区分——实测过那个漏网：把判据收窄成
// `NodeID=="" && InstanceID==""`，本文件其余格全绿，而扇出实例的钩子照样拿到非空身份。
func TestHookSite_FanoutInstanceAlsoNoID(t *testing.T) {
	var got []string
	lock := make(chan struct{}, 1)
	lock <- struct{}{}

	b := compose_v2.NewBuilder().WithName("hook_site_fanout")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "worker"},
			compose_v2.SendDirective{Target: "worker"})
		return nil, nil
	})
	b.AddNode("worker", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("worker", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(fanoutSiteRecorder{got: &got, lock: lock}),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("hook_fanout")); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 前置：两个 worker 实例的钩子都跑过（split 那次不算）
	if len(got) != 2 {
		t.Fatalf("应记到 2 个 worker 实例，实际 %d：%v", len(got), got)
	}
	for i, id := range got {
		if id != "" {
			t.Errorf("第 %d 个扇出实例的 OnNodeStart 里取到了非空身份 %q——那时节点身份还没建立，"+
				"这个值只由实例序号区分，跨图跨任务照撞", i, id)
		}
	}
}

type fanoutSiteRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	got  *[]string
	lock chan struct{}
}

func (h fanoutSiteRecorder) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id != "worker" {
		return ctx, nil
	}
	<-h.lock
	*h.got = append(*h.got, compose_v2.CurrentInterruptSite(ctx).InterruptID())
	h.lock <- struct{}{}
	return ctx, nil
}

// TestHookSite_EmptyTaskIDStillHasIdentity 节点内 TaskID 为空时，身份仍须非空。
//
// 这一格钉的是「守卫**别改成判 TaskID**」：TaskID 可以合法为空——续跑时框架原样沿用
// 存档里那枚，哪怕它是空的（升级期旧存档 / 业务 backend 漏序列化 / 业务手拼
// `&Checkpoint{}` 三种来源）。拿它当判据的话，那些续跑会整条 HITL 静默返空身份。
//
// ⚠️ 首跑的节点体里 TaskID 恒非空，所以**只跑首跑覆盖不到这条**——实测过那个漏网：
// 把判据放宽成 `NodeID=="" || TaskID==""`，本文件其余格全绿。
func TestHookSite_EmptyTaskIDStillHasIdentity(t *testing.T) {
	var idInNode string
	cper := dropTaskIDCheckpointer{compose_v2.NewInMemoryCheckpointer()}
	ctx := context.Background()

	b := compose_v2.NewBuilder().WithName("empty_task_g")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		idInNode = compose_v2.CurrentInterruptSite(ctx).InterruptID()
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	run := func() error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("empty_task"))
	}
	if _, isInt := compose_v2.IsInterrupt(run()); !isInt {
		t.Fatal("首跑应中断")
	}
	// 续跑：存档里的 TaskID 已被抹掉，节点体读到空——身份仍须非空
	idInNode = ""
	_ = run()
	if idInNode == "" {
		t.Fatal("续跑时节点体取不到身份——存档里 TaskID 为空是合法状态" +
			"（旧存档 / backend 漏序列化 / 业务手拼），守卫不该拿它当判据")
	}
}

// TestHookSite_InNodeStillWorks 守卫不能误伤真正在节点里的那些。
//
// 承重量：`OnModelStart/End`、`OnToolStart/End`、节点体、工具体都跑在节点执行内，
// 框架自带的三处 HITL（ask_user / 工具审批 / a2a）全靠这条路算身份。守卫若判宽了，
// 它们会一起返空 ⟹ 整个 HITL 停摆。
func TestHookSite_InNodeStillWorks(t *testing.T) {
	var got string
	var site compose_v2.InterruptSite
	b := compose_v2.NewBuilder().WithName("in_node_g")
	b.AddNode("worker", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		site = compose_v2.CurrentInterruptSite(ctx)
		got = site.InterruptID()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "worker")
	b.AddEdge("worker", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("in_node")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got == "" {
		t.Fatalf("节点体里取不到身份（site=%+v）——两种可能：守卫的判据判宽了"+
			"（interrupt.go 的 InterruptID）；或者 nodeID 没进 ctx"+
			"（graph_loop.go 的 withNodeInterruptCounter 调用点）", site)
	}
	// 守卫为什么选 NodeID 而不选 TaskID：**TaskID 可以合法为空**。续跑时框架原样沿用
	// 存档里那枚，哪怕它是空的（升级期旧存档 / 业务 backend 漏序列化 / 业务手拼
	// `&Checkpoint{}` 三种来源，见 graph_loop.go 里 auto-load 那段）。拿它当判据的话，
	// 那些续跑会整条 HITL 静默返空。这一格就是钉住「别改成判 TaskID」。
	if site.TaskID == "" {
		t.Errorf("首跑的节点体里 TaskID 为空（site=%+v）——它是「哪一次任务」的唯一来源，"+
			"空了跨任务的身份会撞", site)
	}
}

// TestHookSite_NestedGraphAlsoNoID 嵌套图里同样不能有身份。
//
// # 为什么单独一格
//
// 位置维度是普通 ctx value，**跨图边界继承**：子图的 `Run` 收到的就是父节点的 ctx，
// 而写它们的那处排在 `OnNodeStart` 之后 ⟹ 子图每个节点的钩子读到的是**父节点的**
// NodeID，非空 ⟹ 守卫够不着。
//
// **而嵌套是常态**——7 个编排全是「父节点体内跑子 agent 的图」，所以只跑根图等于
// 只覆盖了最少见的那种形状。实测过泄漏时的样子：
//
//	ns=""         钩子在 subnode → ctxNodeID=""        → id=""              （守卫生效）
//	ns="subnode"  钩子在 s1      → ctxNodeID="subnode" → id=e3f0ba040e9a2f08（守卫失效）
//
// 危害不是值不好看，是 livelock：那个身份的 idx 维读的是**父节点的计数器**，tick 次数
// 随 Resume 时子图 frontier 缩水而变 ⟹ 身份变 ⟹ 用户答的那条永远配不上。
func TestHookSite_NestedGraphAlsoNoID(t *testing.T) {
	var hookIDs []string
	var bodyID string
	lock := make(chan struct{}, 1)
	lock <- struct{}{}

	sb := compose_v2.NewBuilder().WithName("nested_sub")
	sb.AddNode("s1", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		bodyID = compose_v2.CurrentInterruptSite(ctx).InterruptID()
		return nil, nil
	})
	sb.AddEdge(compose_v2.StartNode, "s1")
	sb.AddEdge("s1", compose_v2.EndNode)
	sub, err := sb.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	hooks := nestedSiteRecorder{got: &hookIDs, lock: lock}
	pb := compose_v2.NewBuilder().WithName("nested_parent")
	pb.AddNode("subnode", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.Run(ctx, sub, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(hooks), compose_v2.WithRunNS("subnode"))
	})
	pb.AddEdge(compose_v2.StartNode, "subnode")
	pb.AddEdge("subnode", compose_v2.EndNode)
	pg, err := pb.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}
	if err := compose_v2.Run(context.Background(), pg, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(hooks),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("nested_site")); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 前置：父子两层的钩子都跑过
	if len(hookIDs) != 2 {
		t.Fatalf("应记到父子两层各一次，实际 %d：%v", len(hookIDs), hookIDs)
	}
	for i, id := range hookIDs {
		if id != "" {
			t.Errorf("第 %d 层的 OnNodeStart 里取到了非空身份 %q——"+
				"子图那层读到的是父节点的位置，跨 Resume 会漂", i, id)
		}
	}
	// 承重：**子图节点体自己的身份必须非空**——清继承时若把子图自己那份也清掉，
	// 整个嵌套 HITL 会停摆，而上面那两条断言照样绿。
	if bodyID == "" {
		t.Fatal("子图节点体取不到身份——清继承时把子图自己那份也清掉了，嵌套 HITL 会整个停摆")
	}
}

type nestedSiteRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	got  *[]string
	lock chan struct{}
}

func (h nestedSiteRecorder) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	<-h.lock
	*h.got = append(*h.got, compose_v2.CurrentInterruptSite(ctx).InterruptID())
	h.lock <- struct{}{}
	return ctx, nil
}
