package compose_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// Phase 1.2.c → 5.1.a Step 4 改造测试：嵌套子图独立 cp 物理分离 +
// Resume 从子图断点继续。
//
// 旧版（Phase 1.2.c）：用 cm.SubgraphSnapshots 嵌入式 map 存子 cp。
// 新版（Phase 5.1.a Step 4）：用 Checkpointer.SaveNS(sid, childNS) 物理分离。

// TestSubgraph_NestedInterrupt_ResumeFromSubFrontier 是 Phase 1.2.c 旗舰测试。
//
// 场景：父 graph 中嵌入子图，子图内分 3 步：step1 → step2 (ask_user) → step3
//   - Run 1: 子图 step1 完成，step2 抛 ask_user → 父 graph 中断
//   - 子 cp 写到 (sessionID, "sub_node") 物理分离 ns
//   - Run 2 with answer: 子图从 step2 重跑（不重跑 step1）→ step3 完成
//   - 子图成功完成 → DeleteSubtree 清子 cp（避免下次 Resume 误读）
func TestSubgraph_NestedInterrupt_ResumeFromSubFrontier(t *testing.T) {
	step1Runs := atomicCounterReset()
	step3Runs := atomicCounterReset()
	subProgressCh := compose_v2.LastValue[string]("nested_progress")

	makeSubgraph := func() *compose_v2.Graph {
		subB := compose_v2.NewBuilder().
			AddNode("step1", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				step1Runs.Add(1)
				return []compose_v2.ChannelWrite{compose_v2.Write(subProgressCh, "step1_done")}, nil
			}).
			AddNode("step2_with_ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_nested"); ok {
					return []compose_v2.ChannelWrite{
						compose_v2.Write(subProgressCh, "step2_done_with:"+ans),
					}, nil
				}
				return nil, &compose_v2.InterruptError{
					Query: &compose_v2.AskQuery{InterruptID: "ask_nested", Question: "嵌套确认？"},
				}
			}).
			AddNode("step3", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				step3Runs.Add(1)
				return []compose_v2.ChannelWrite{compose_v2.Write(subProgressCh, "step3_done")}, nil
			}).
			AddEdge(compose_v2.StartNode, "step1").
			AddEdge("step1", "step2_with_ask").
			AddEdge("step2_with_ask", "step3").
			AddEdge("step3", compose_v2.EndNode)
		subB = compose_v2.WithChannel(subB, subProgressCh)
		sub, err := subB.Compile()
		if err != nil {
			t.Fatalf("sub compile: %v", err)
		}
		return sub
	}

	makeParent := func() *compose_v2.Graph {
		sub := makeSubgraph()
		parentB := compose_v2.NewBuilder().
			AddSubgraphNode("sub_node", sub, compose_v2.WithExport(subProgressCh)).
			AddEdge(compose_v2.StartNode, "sub_node").
			AddEdge("sub_node", compose_v2.EndNode)
		parentB = compose_v2.WithChannel(parentB, subProgressCh)
		parent, err := parentB.Compile()
		if err != nil {
			t.Fatalf("parent compile: %v", err)
		}
		return parent
	}

	// === Run 1: 子图 step1 完成、step2 抛 ask_user → 父中断 ===
	cper := compose_v2.NewInMemoryCheckpointer()
	const sid = "sid_nested_1"

	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makeParent(), cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("应中断，实际 %v", err)
	}

	if step1Runs.Load() != 1 {
		t.Fatalf("Run 1 step1 应跑 1 次，实际 %d", step1Runs.Load())
	}
	if step3Runs.Load() != 0 {
		t.Fatalf("Run 1 step3 不应跑（中断在 step2），实际跑了 %d 次", step3Runs.Load())
	}

	// 验证子 cp 写到 (sid, "sub_node") 物理分离 ns
	subCp, err := cper.LoadNS(context.Background(), sid, "sub_node")
	if err != nil {
		t.Fatalf("LoadNS sub_node: %v", err)
	}
	if subCp == nil {
		t.Fatal("子图中断后 cp 应写到 (sid, sub_node)，实际未找到")
	}
	if subCp.Status != compose_v2.CPStatusPending {
		t.Fatalf("子 cp Status 应 Pending，实际 %v", subCp.Status)
	}
	if len(subCp.CurrentFrontier) != 1 || subCp.CurrentFrontier[0].NodeID != "step2_with_ask" {
		t.Fatalf("子 cp frontier 应是 [step2_with_ask]，实际 %v", subCp.CurrentFrontier)
	}
	if len(subCp.PendingInterrupts) != 1 {
		t.Fatalf("子 cp 应含 1 个 pending interrupt，实际 %d", len(subCp.PendingInterrupts))
	}

	// === Run 2: 父 Resume + answer，子图从 step2 重跑 ===
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm2, subProgressCh); err != nil {
		t.Fatalf("cm2 register: %v", err)
	}

	if err := compose_v2.Run(context.Background(), makeParent(), cm2,
		compose_v2.WithCheckpointer(cper), // 同 cper 让 LoadNS 找回子 cp
		compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"ask_nested": schema.UserMessage("yes")}),
	); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	// **关键验证 1**：step1 总共还是只跑 1 次（子图 Resume 不重跑 step1）
	if step1Runs.Load() != 1 {
		t.Fatalf("Resume 后 step1 总共应只跑 1 次（不重跑），实际 %d", step1Runs.Load())
	}

	// **关键验证 2**：step3 跑了 1 次（Resume 后子图从 step2 续到 step3）
	if step3Runs.Load() != 1 {
		t.Fatalf("Resume 后 step3 应跑 1 次，实际 %d", step3Runs.Load())
	}

	// **关键验证 3**：progress 最终是 step3_done（导出到父 cm）
	if v := compose_v2.MustGet(cm2.Channels(), subProgressCh); v != "step3_done" {
		t.Fatalf("progress 应是 step3_done，实际 %q", v)
	}

	// **关键验证 4**：子图成功完成后 (sid, "sub_node") cp 已被 DeleteSubtree 清掉
	subCp2, _ := cper.LoadNS(context.Background(), sid, "sub_node")
	if subCp2 != nil {
		t.Fatalf("子图成功完成后子 cp 应被清掉，实际仍存 %+v", subCp2)
	}
}

// TestSubgraph_NormalCompletion_NoCp 验证子图正常完成时 (sid, childNS) cp
// **不残留**——避免下次 Resume 误读。
func TestSubgraph_NormalCompletion_NoCp(t *testing.T) {
	doneCh := compose_v2.LastValue[string]("normal_sub_done")

	subB := compose_v2.NewBuilder().
		AddNode("inner", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(doneCh, "ok")}, nil
		}).
		AddEdge(compose_v2.StartNode, "inner").
		AddEdge("inner", compose_v2.EndNode)
	subB = compose_v2.WithChannel(subB, doneCh)
	sub, _ := subB.Compile()

	parentB := compose_v2.NewBuilder().
		AddSubgraphNode("sub", sub, compose_v2.WithExport(doneCh)).
		AddEdge(compose_v2.StartNode, "sub").
		AddEdge("sub", compose_v2.EndNode)
	parentB = compose_v2.WithChannel(parentB, doneCh)
	parent, _ := parentB.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), parent, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_normal"),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 子图正常完成后 (sid, "sub") 路径不应有 cp
	subCp, _ := cper.LoadNS(context.Background(), "sid_normal", "sub")
	if subCp != nil {
		t.Fatalf("子图正常完成不应留 cp，实际 %+v", subCp)
	}
}

// atomicCounterReset 简化的 test helper——返新的 atomic counter。
func atomicCounterReset() *atomicCounterT {
	return &atomicCounterT{}
}

type atomicCounterT struct {
	v atomic.Int64
}

func (c *atomicCounterT) Add(n int64) int64 { return c.v.Add(n) }
func (c *atomicCounterT) Load() int64       { return c.v.Load() }
