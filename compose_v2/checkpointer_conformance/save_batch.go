package checkpointer_conformance

import (
	"context"
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

	// audit iter-3 视角 3 H4: SaveBatch partial failure contract — checkpointer.go:71-75
	// godoc 写 "best-effort 失败时返 error, caller 需自行处理部分成功状态".
	// 业务 backend 实现可能 silent rollback 全部 (PG TX) vs best-effort 部分成功
	// (Redis pipeline)— conformance 强制业务 backend 返 err 非 nil + 业务 caller
	// 必判 err. 不强制 atomic vs best-effort 选择 (godoc 允业务自定).
	t.Run("SaveBatch_Partial_Failure_Returns_Error", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 第 2 cp GraphID 跟已存在的不同 → SaveNS 应返 ErrGraphIDConflict
		// (业务 backend 必触发 partial failure 路径)
		_ = cp.SaveNS(ctx, "sid", "ns_a", newCheckpoint("graph_x", compose_v2.CPStatusUnset))

		// batch 含: ns_b OK + ns_a 跟现有 GraphID 冲突 (graph_x vs graph_y)
		batch := map[string]*compose_v2.Checkpoint{
			"ns_b": newCheckpoint("graph_y", compose_v2.CPStatusUnset),
			"ns_a": newCheckpoint("graph_y", compose_v2.CPStatusUnset), // conflict!
		}
		err := cp.SaveBatch(ctx, "sid", batch)
		if err == nil {
			t.Error("SaveBatch with GraphID conflict in any cp should return error (not silent skip)")
		}
		// 不强制 atomic vs best-effort — 业务 backend 自选, 但必须返 err 非 nil
		// 让 caller 知道 batch 不完全成功. 业务 silent return nil 是协议违反.
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
