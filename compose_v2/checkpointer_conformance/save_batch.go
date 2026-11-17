package checkpointer_conformance

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// testSaveBatch 验证 SaveBatch 一次写多个 (ns, cp).
func testSaveBatch(t *testing.T, factory CheckpointerFactory) {
	t.Run("SaveBatch_Multiple_NS_All_Persisted", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		batch := map[string]*compose_v2.Checkpoint{
			"ns_a": newCheckpoint("g", compose_v2.CPStatusUnset),
			"ns_b": newCheckpoint("g", compose_v2.CPStatusPending),
			"ns_c": newCheckpoint("g", compose_v2.CPStatusUnset),
		}
		if err := cp.SaveBatch(ctx, "sid", batch); err != nil {
			t.Fatalf("SaveBatch: %v", err)
		}

		// 3 个全持久化
		for _, ns := range []string{"ns_a", "ns_b", "ns_c"} {
			loaded, _ := cp.LoadNS(ctx, "sid", ns)
			if loaded == nil {
				t.Errorf("ns=%q expected non-nil after SaveBatch, got nil", ns)
			}
		}
	})

	t.Run("SaveBatch_Empty_Map_No_Error", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		if err := cp.SaveBatch(ctx, "sid", map[string]*compose_v2.Checkpoint{}); err != nil {
			t.Errorf("SaveBatch empty map should not error, got: %v", err)
		}
	})

	t.Run("SaveBatch_MiddleFailureKeepsSuccessfulPrefix", func(t *testing.T) {
		store := factory()
		ctx := t.Context()
		const sid, first, conflict, last = "batch", "A", "a", "a "
		for _, ns := range []string{conflict, last} {
			if err := store.SaveNS(ctx, sid, ns, newCheckpoint("existing", compose_v2.CPStatusUnset)); err != nil {
				t.Fatal(err)
			}
		}
		batch := make(map[string]*compose_v2.Checkpoint)
		batch[last] = newCheckpoint("existing", compose_v2.CPStatusPending)
		rejected := newCheckpoint("different", compose_v2.CPStatusPending)
		rejected.ID, rejected.NS = "caller", "input"
		batch[conflict] = rejected
		batch[first] = newCheckpoint("new", compose_v2.CPStatusPending)

		if err := store.SaveBatch(ctx, sid, batch); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
			t.Fatalf("首个失败须保留 GraphID 冲突错误链：%v", err)
		}
		if rejected.ID != "caller" || rejected.NS != "input" || rejected.ParentNS != "" {
			t.Fatalf("失败项的输入也应保持原样：id=%q ns=%q parent=%q", rejected.ID, rejected.NS, rejected.ParentNS)
		}
		for _, want := range []struct {
			ns, graph string
			status    compose_v2.CheckpointStatus
		}{
			{first, "new", compose_v2.CPStatusPending},
			{conflict, "existing", compose_v2.CPStatusUnset},
			{last, "existing", compose_v2.CPStatusUnset},
		} {
			cp, err := store.LoadNS(ctx, sid, want.ns)
			if err != nil || cp == nil || cp.GraphID != want.graph || cp.Status != want.status {
				t.Fatalf("槽 %q 的预期 graph=%q status=%v，当前=%+v err=%v", want.ns, want.graph, want.status, cp, err)
			}
		}
	})
}

// cpWithNodeStates 跑一次真图产出一份**带 NodeStates 的** cp，供批量 / 清理路径复用。
//
// 不能直接构造：`taggedValue` 未导出。走真图是唯一拿得到的路子。
func cpWithNodeStates(t *testing.T, graphName, sid string) *compose_v2.Checkpoint {
	t.Helper()
	ctx := context.Background()
	compose_v2.RegisterValueType[confChannelVal]("checkpointer_conformance.confChannelVal")

	tmp := compose_v2.NewInMemoryCheckpointer()
	b := compose_v2.NewBuilder().WithName(graphName)
	b.AddNode("worker", func(nodeCtx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if err := compose_v2.SaveNodeState(nodeCtx, confChannelVal{V: "done-step-1"}); err != nil {
			return nil, err
		}
		// 派生键的两种形态也各记一条（按 key 的步骤、可重做的步骤）：它们跟上面那一条是同一个
		// 字段里的不同键，后端若只在批量路径上按键形态挑着写（比如丢掉带冒号的键），只比条数
		// 的话也能发现，但得先有这几条
		if err := compose_v2.SaveStepResult(nodeCtx, confStepKey, confChannelVal{V: "charged"}); err != nil {
			return nil, err
		}
		if err := compose_v2.SaveStepResult(nodeCtx, "planner", confChannelVal{V: "plan-v1"}, compose_v2.Rerunnable()); err != nil {
			return nil, err
		}
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "ns_batch_q", Question: "?"},
		}
	})
	b.AddEdge(compose_v2.StartNode, "worker")
	b.AddEdge("worker", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("cpWithNodeStates Compile: %v", err)
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(tmp), compose_v2.WithSessionID(sid)); err == nil {
		t.Fatal("cpWithNodeStates: 应中断")
	}
	got, err := tmp.LoadNS(ctx, sid, "")
	if err != nil || got == nil {
		t.Fatalf("cpWithNodeStates LoadNS: err=%v cp=%v", err, got)
	}
	if len(got.NodeStates) < 3 {
		t.Fatalf("cpWithNodeStates: 前提不成立，产出的 cp 里 NodeStates 只有 %d 条（应有节点那一条 + 两条步骤记录）",
			len(got.NodeStates))
	}
	return got
}

// testSaveBatchNodeStates 批量路径也得带上 NodeStates。
//
// 单独一条是因为**业务自研 backend 最典型的写法就是两条路各写各的**：`SaveNS` 走一条
// 完整序列化、`SaveBatch` 自己拼一条批量 SQL/pipeline。后者漏了 `node_states` **不报错**
// ——只是 Resume 后已成功的副作用全部重放一遍（安全侧，但静默）。
func testSaveBatchNodeStates(t *testing.T, factory CheckpointerFactory) {
	t.Run("SaveBatch_NodeStates_Persisted", func(t *testing.T) {
		ctx := context.Background()
		cper := factory()
		src := cpWithNodeStates(t, "conf_batch_ns_graph", "sid-src")
		wantN := len(src.NodeStates)

		batch := map[string]*compose_v2.Checkpoint{
			"ns_plain": newCheckpoint(src.GraphID, compose_v2.CPStatusPending),
			"ns_memo":  src,
		}
		if err := cper.SaveBatch(ctx, "sid-batch", batch); err != nil {
			t.Fatalf("SaveBatch: %v", err)
		}

		loaded, err := cper.LoadNS(ctx, "sid-batch", "ns_memo")
		if err != nil || loaded == nil {
			t.Fatalf("LoadNS: err=%v cp=%v", err, loaded)
		}
		if len(loaded.NodeStates) != wantN {
			t.Fatalf("SaveBatch 路径丢了 NodeStates：want %d 条, got %d 条\n"+
				"  漏这个字段**不报错**——只是节点 Resume 时读不到「上次已经做完哪些」，\n"+
				"  已成功的副作用（下单/扣款/发消息）重放一遍。\n"+
				"  典型成因：SaveNS 走完整序列化，而 SaveBatch 自己拼了一条批量语句漏了列",
				wantN, len(loaded.NodeStates))
		}
		for k := range src.NodeStates {
			if _, ok := loaded.NodeStates[k]; !ok {
				t.Fatalf("SaveBatch 路径丢了 NodeStates 里的键 %q（条数对上了但键变了）\n"+
					"  派生键形如 `<节点执行键>:step:<哈希>`，按键形态挑着写 / 改写键都会让节点读不回"+
					"已经做成的步骤，已完成的嵌套调用在续跑时重跑一遍", k)
			}
		}
	})
}
