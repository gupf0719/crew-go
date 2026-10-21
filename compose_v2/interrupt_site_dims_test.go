package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// 中断寻址的每一维都必须真的进 hash，且必须真的从 ctx 读回来。
//
// # 为什么要有这一格
//
// 审计逐维实测：把 `interruptSiteFromCtx`（协议**唯一**的维度枚举点）里的维度整行删掉，
// 6 维里 **4 维 compose_v2 全绿**，`GraphNS` 更是**全仓零覆盖**。
//
// 而本该守 `GraphNS` 的那条用例（两个子图 ns 各算一个 ID、断言不等）**被后加的任务代号
// 遮住了**：每个 ns 各铸各的代号 ⟹ 删掉 GraphNS 两个 ID 照样不同。这是「双层防护遮蔽」
// 的实例，而且遮蔽源正是新加的那一维。
//
// 所以这里分两层钉：
//   - **hash 层**：纯函数，逐维改一个值、ID 必须变（防「某一维没进 hash」）
//   - **ctx 层**：从 ctx 真读一遍（防「进了 hash 但没从 ctx 读」）

// TestInterruptSiteDims_EachDimensionEntersHash hash 层：逐维单独变一个值，ID 必须跟着变。
//
// 这是纯函数断言，不经 ctx——它只保证「维度在 hash 里」，不保证「维度从 ctx 读得到」。
// 后半由下面那条管。
func TestInterruptSiteDims_EachDimensionEntersHash(t *testing.T) {
	base := compose_v2.InterruptSite{
		GraphNS: "p/c", NodeID: "n1", InstanceID: "i1",
		Visit: 1, ToolCallID: "tc1", ToolCallIdx: 1, Idx: 1, TaskID: "t1",
	}
	baseID := base.InterruptID()

	cases := []struct {
		dim   string
		muton func(s *compose_v2.InterruptSite)
		why   string
	}{
		{"GraphNS", func(s *compose_v2.InterruptSite) { s.GraphNS = "p/other" },
			"不同子图的同名节点会算出同一个 ID，嵌套场景下答案跨子图串台"},
		{"NodeID", func(s *compose_v2.InterruptSite) { s.NodeID = "n2" },
			"同图不同节点的中断会撞"},
		{"InstanceID", func(s *compose_v2.InterruptSite) { s.InstanceID = "i2" },
			"SendFrom 派生的 N 个并发实例会撞（ISSUE-96 那一维）"},
		// ⚠️ Visit 单列——它跟 InstanceID **互斥**（有实例身份时 visit 归零，见
		// `InterruptID()`），所以基准里 InstanceID 非空时改 Visit 本来就不该变 ID。
		// 这条互斥本身在下面单独钉。
		{"ToolCallID", func(s *compose_v2.InterruptSite) { s.ToolCallID = "tc2" },
			"「在某个 tool_call 里」和「在 tools 节点但不在任何 tool_call 里（钩子相位）」会撞——" +
				"下标那一维分不开这两者，因为 int 的 0 同时表示「不存在」和「第 0 个」"},
		{"ToolCallIdx", func(s *compose_v2.InterruptSite) { s.ToolCallIdx = 2 },
			"同一节点内 N 个并发 tool_call 会撞"},
		{"Idx", func(s *compose_v2.InterruptSite) { s.Idx = 2 },
			"同一处多次中断会撞"},
		{"TaskID", func(s *compose_v2.InterruptSite) { s.TaskID = "t2" },
			"同 sid 下跨任务会撞 ⟹ 用户批的是这件事、系统执行的是另一件"},
	}
	for _, c := range cases {
		t.Run(c.dim, func(t *testing.T) {
			s := base
			c.muton(&s)
			if s.InterruptID() == baseID {
				t.Fatalf("%s 这一维没进 hash——%s", c.dim, c.why)
			}
		})
	}

	// Visit 只在**没有实例身份**时承担代际——静态边节点靠它区分跨轮次重访。
	t.Run("Visit（无实例身份时）", func(t *testing.T) {
		st := compose_v2.InterruptSite{GraphNS: "p/c", NodeID: "n1", Visit: 1}
		st2 := st
		st2.Visit = 2
		if st.InterruptID() == st2.InterruptID() {
			t.Fatal("Visit 这一维没进 hash——静态边节点跨轮次重访会撞（ReAct 多轮 / Loop 重访）")
		}
	})

	// **互斥本身也是承重的**：有实例身份时 visit 必须被归零，否则同一个实例在
	// 「部分失败重跑」里 visit 变了就会算出新 ID，用户手里的答案全部失配。
	t.Run("有实例身份时 Visit 被归零", func(t *testing.T) {
		st := compose_v2.InterruptSite{GraphNS: "p/c", NodeID: "n1", InstanceID: "i1", Visit: 1}
		st2 := st
		st2.Visit = 99
		if st.InterruptID() != st2.InterruptID() {
			t.Fatal("有实例身份时 visit 没被归零——同一实例重跑时 ID 会漂，答案全部失配")
		}
	})
}

// TestInterruptSiteDims_GraphNSReadFromCtx ctx 层：`GraphNS` 必须真的从 ctx 读回来。
//
// ⚠️ **必须把任务代号钉死成同一个**，否则这条会被它遮住：两个 ns 各铸各的代号 ⟹ 就算
// `GraphNS` 完全不读，两个 ID 也不同。用显式 `WithResumeFrom` 传同一份带固定代号的存档
// 来消掉那个干扰项。
func TestInterruptSiteDims_GraphNSReadFromCtx(t *testing.T) {
	ids := map[string]string{}
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ns := compose_v2.GetGraphNS(ctx)
		ids[ns] = compose_v2.CurrentInterruptSite(ctx).InterruptID()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// 同一份存档（同一个 TaskID）喂给两个不同 ns——差异只剩 GraphNS
	run := func(ns string) {
		cp := &compose_v2.Checkpoint{
			TaskID:          "FIXED_TASK",
			CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "n"}},
		}
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithRunNS(ns), compose_v2.WithResumeFrom(cp)); err != nil {
			t.Fatalf("ns=%s: %v", ns, err)
		}
	}
	run("p/childX")
	run("p/childY")

	if len(ids) != 2 {
		t.Fatalf("fixture 没搭对：只收到 %d 个 ns 的 ID：%v", len(ids), ids)
	}
	if ids["p/childX"] == ids["p/childY"] {
		t.Fatalf("两个不同 ns 算出同一个中断 ID（%s）——`GraphNS` 没从 ctx 读回来。\n"+
			"后果：嵌套场景下答案跨子图串台。\n"+
			"（这条特意把任务代号钉成同一个，否则会被「每个 ns 各铸各的代号」遮住）",
			ids["p/childX"])
	}
}

// TestInterruptSiteDims_ToolCallIDReadFromCtx ctx 层：`ToolCallID` 必须真从 ctx 读出来。
//
// ⚠️ **hash 层那条覆盖不到这个**——它是纯函数断言，手工填满字段。少了这一条，把
// `interruptSiteFromCtx` 里读 `ToolCallID` 那行删掉，全仓照样绿。
//
// 承重的是「有 tool-call 上下文」与「没有」必须算出不同身份：`int` 的 0 同时表示
// 「不在任何 tool_call 里」和「这就是 tool_call #0」，所以只靠下标那一维，钩子相位的
// 整个地址空间会**别名到 tool_call #0 上**。空串是可区分的「不存在」，int 的 0 不是。
func TestInterruptSiteDims_ToolCallIDReadFromCtx(t *testing.T) {
	var noTC, withTC string
	b := compose_v2.NewBuilder().WithName("tc_dim_g")
	b.AddNode("tools", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 相位一：在 tools 节点里，但还没进任何 tool_call（钩子就跑在这个位置）
		noTC = compose_v2.CurrentInterruptSite(ctx).InterruptID()
		// 相位二：进了 tool_call #0
		callCtx := compose_v2.WithCurrentToolCallContext(ctx, "tc_0", 0)
		withTC = compose_v2.CurrentInterruptSite(callCtx).InterruptID()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "tools")
	b.AddEdge("tools", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("tc_dim")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if noTC == "" || withTC == "" {
		t.Fatalf("两个相位都该算出非空身份：noTC=%q withTC=%q", noTC, withTC)
	}
	if noTC == withTC {
		t.Fatalf("「不在任何 tool_call 里」与「在 tool_call #0 里」算出同一个身份 %q——\n"+
			"钩子相位的整条计数线会叠在 tool_call #0 上：用户答钩子那一问，"+
			"工具体会把它当成自己那一问的答案消费掉，而那个问题从未被问过。\n"+
			"sanity-revert: 删 interruptSiteFromCtx 里读 ToolCallID 那行 → 本用例 fail", noTC)
	}
}
