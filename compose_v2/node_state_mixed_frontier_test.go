package compose_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type mixedMemo struct{ Done []string }

func init() { compose_v2.RegisterValueType[*mixedMemo]("*compose_v2_test.mixedMemo") }

// TestNodeState_MixedFrontier_MemoSurvives 钉住**混合 frontier** 下节点私有态不掉档。
//
// # 混合 frontier 是什么、为什么它专门坑这一维
//
// 同一个 NodeID 可以**同时**是静态边目标和 `SendFrom` 目标——于是一个超步的 frontier 里
// 会有 `{X,""}`（静态那位）和 `{X,i1}`（派生实例）并存。
//
// 而 `visitCount` 是 **per-NodeID 共享**的：静态那位成功 ⟹ 提交点把它推高。于是实例那位
//
//   - 在节点入口写 memo 用的是推高**前**的值
//   - 存档投影按推高**后**的值去找
//
// ⟹ **miss，memo 整条掉出存档**，Resume 时它已经做完的带副作用的事（下单 / 扣款 / 发消息）
// 照样重放一遍。
//
// # 修法与它守的那句话
//
// memo 键对非空 `InstanceID` 把 visit 置 0——跟 `InterruptSite.InterruptID()` **同一条规则**。
// 两处对同一组维度必须做同一件事；曾经一处互斥、一处三维全用。
//
// 这条同时守 serialization.go 里那句「投影谓词 = 读谓词，逐字相同」——它成立的前提正是
// 「导出时读到的 visitCount 跟节点入口读到的一样」，而混合 frontier 会打破它。
//
// sanity-revert：去掉 `nodeStateKey` 里 `if instanceID != "" { visit = 0 }` → 本用例红，
// 且红成「memo 掉档、副作用执行 2 次」。
func TestNodeState_MixedFrontier_MemoSurvives(t *testing.T) {
	var charged atomic.Int32

	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("mixed_frontier")
		// A：静态边指向 X，同时再 SendFrom 一个 X 实例 ⟹ 混合 frontier
		b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "X"})
			return nil, nil
		})
		b.AddNode("X", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			inst := compose_v2.GetCurrentInstanceID(ctx)
			if inst == "" {
				return nil, nil // 静态那位：直接成功，把共享计数器推高
			}
			// 实例那位：先做带副作用的事并记进 memo，再提问
			if prev, ok := compose_v2.GetNodeState[*mixedMemo](ctx); !ok || prev == nil {
				charged.Add(1)
				_ = compose_v2.SaveNodeState(ctx, &mixedMemo{Done: []string{"charged"}})
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认收货？"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "A")
		b.AddEdge("A", "X") // 静态边——跟 SendFrom 的实例并存
		b.AddEdge("X", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "mixed_sid"

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 该中断在实例那位")
	}
	cp, _ := cper.LoadNS(ctx, sid, "")
	t.Logf("cp.CurrentFrontier=%+v NodeVisits=%v len(NodeStates)=%d",
		cp.CurrentFrontier, cp.NodeVisits, len(cp.NodeStates))

	// **承重 1**：memo 必须进存档——掉了的话 Resume 无从跳过已完成的副作用
	if len(cp.NodeStates) == 0 {
		t.Errorf("memo 没进存档（混合 frontier 下投影按被兄弟推高的 visit 去找 ⟹ miss）\n"+
			"  NodeVisits=%v ——静态那位成功把共享计数器推高了，而实例那位写 memo 用的是推高前的值",
			cp.NodeVisits)
	}

	// Resume
	if err := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
		})); err != nil {
		t.Fatalf("Resume 该跑通: %v", err)
	}

	// **承重 2**：副作用恰一次——memo 的全部意义
	if got := charged.Load(); got != 1 {
		t.Errorf("扣款该恰 1 次，实际 %d 次——memo 掉档导致 Resume 重放了已完成的副作用", got)
	}
}
