package sql_test

// `sql/` 把 cp 的全部业务字段塞进单个 `payload` 列（JSON 类型），只有元数据
// （sid/ns/status/graph_id/created_at）有独立列。
//
// # 为什么需要这条测试
//
// 本模块此前唯一的测试手段是 sqlmock（`b1a9434` 起已接真 SQLite 引擎跑 conformance），而它对 payload 传的是 `sqlmock.AnyArg()`
// （`checkpointer_sqlmock_test.go`）——**payload 内容从不被检查**。于是对"某个字段
// 没进 wire"这类退化结构性失明：实测给 `Checkpoint.Epoch` 打 `json:"-"`，既有
// `sql/` 测试**全绿**。
//
// 那些字段漏了都不报错、只是静默退化：
//   - `FrontierMember.InstanceID`（两组 frontier 里）→ 多实例 Resume 后实例互相冒充
//   - `SerializedSend.OwnerInstanceID` / `.InstanceID` → 旧 send 清不掉 → Target 双发
//   - `Epoch` → `WithStaleWriteGuard` 静默失效（业务以为开了保护其实没有）
//   - `NodeStates` → 节点读不到「上次已做完哪些」→ 已成功的副作用 Resume 时重放一遍
//     （`taggedValue` 未导出、构造不出来，所以它单独一条走**真图**跑出 cp——
//     见 TestPayloadRoundTrip_NodeStates。原先只写「覆盖在 checkpointer_conformance」，
//     可 `sql/` 从不跑那套 ⟹ 本模块对这个字段实际零覆盖）
//
// 本测试只验 `MarshalCheckpoint` ↔ `UnmarshalCheckpoint` 这一层——**它正是 `sql/`
// 存取 payload 的唯一路径**（全模块 1 个 marshal 点、3 个 unmarshal 点，没有任何
// 按列重建 Checkpoint 的路径）。挡不住 SQL 层的问题，但挡得住"字段掉出 wire"。
//
// **本模块没有真库级 conformance 覆盖**——全靠 sqlmock，所以这条尤其是唯一防线。

import (
	"bytes"
	"context"
	"testing"

	"crew-go/compose_v2"
)

func TestPayloadRoundTrip_AllIdentityFields(t *testing.T) {
	original := &compose_v2.Checkpoint{
		ID: "sid-1", NS: "parent/child", ParentNS: "parent",
		Status: compose_v2.CPStatusPending, GraphID: "g1",
		SchemaVer: compose_v2.CheckpointSchemaVer,

		Epoch: "epoch-token-42",
		// 空 InstanceID = 静态边成员，不是"没值"——那条记录也得整条活下来
		CurrentFrontier:   []compose_v2.FrontierMember{{NodeID: "a"}, {NodeID: "b", InstanceID: "i7"}},
		CompletedFrontier: []compose_v2.FrontierMember{{NodeID: "c"}, {NodeID: "d", InstanceID: "i9"}},
		NodeVisits:        map[compose_v2.NodeID]int{"a": 3, "z": 0}, // 零值也要活下来
		PendingInterrupts: []*compose_v2.AskQuery{
			{InterruptID: "q1", Question: "?", Options: []string{"x", "y"}},
		},
		DeferredSends: []compose_v2.SerializedSend{
			{Target: "t", Owner: "d", OwnerInstanceID: "i9", InstanceID: "i12"},
		},
	}

	data, err := compose_v2.MarshalCheckpoint(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Epoch != "epoch-token-42" {
		t.Errorf("Epoch 丢了: %q — WithStaleWriteGuard 会静默失效（守卫读到空 Epoch 就跳过比对）", got.Epoch)
	}
	assertMembers(t, "CurrentFrontier", got.CurrentFrontier,
		[]compose_v2.FrontierMember{{NodeID: "a"}, {NodeID: "b", InstanceID: "i7"}},
		"漏 node_id 无从寻址；漏 instance_id 多实例 Resume 后互相冒充")
	assertMembers(t, "CompletedFrontier", got.CompletedFrontier,
		[]compose_v2.FrontierMember{{NodeID: "c"}, {NodeID: "d", InstanceID: "i9"}},
		"重跑成员的旧记录清不掉 → Target 双发")
	if len(got.DeferredSends) != 1 {
		t.Fatalf("DeferredSends 长度: want 1, got %d", len(got.DeferredSends))
	}
	if d := got.DeferredSends[0]; d.OwnerInstanceID != "i9" || d.InstanceID != "i12" || d.Owner != "d" {
		t.Errorf("send 身份字段丢了: %+v — purge 匹配键退化成 {owner,\"\"}，旧 send 一条都清不掉", d)
	}
	// NodeVisits 的零值项：dropZero 型序列化会把它吃掉，InterruptID 的 visit 维度就漂了
	if v, ok := got.NodeVisits["z"]; !ok || v != 0 {
		t.Errorf("NodeVisits 零值项丢了: %v", got.NodeVisits)
	}
	if len(got.PendingInterrupts) != 1 || got.PendingInterrupts[0].InterruptID != "q1" ||
		len(got.PendingInterrupts[0].Options) != 2 {
		t.Errorf("PendingInterrupts 丢字段: %+v — HITL 答案配对靠 InterruptID 路由", got.PendingInterrupts)
	}
}

// assertMembers 逐条比 frontier 成员——身份内联后**两个字段一起比**，
// 不再是「两条数组各自比、还得自己盯着等长」。
func assertMembers(t *testing.T, name string, got, want []compose_v2.FrontierMember, consequence string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s 长度 %d != %d（%+v）— %s\n  注意空 InstanceID 的那条也必须保留，"+
			"它表达「该成员是静态边节点」不是「没值」", name, len(got), len(want), got, consequence)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %+v, want %+v — %s", name, i, got[i], want[i], consequence)
		}
	}
}

// nodeStateProbe 是走真图产出 NodeStates 用的载荷类型。
type nodeStateProbe struct{ Done []string }

func init() { compose_v2.RegisterValueType[*nodeStateProbe]("*sql_test.nodeStateProbe") }

// TestPayloadRoundTrip_NodeStates 补上 `NodeStates` 的 wire 覆盖。
//
// 上面那条测试构造不出它——`taggedValue` 未导出。这条改走**真图**：节点调
// `SaveNodeState` 记进度后抛中断 → 框架存 cp（`NodeStates` 非空）→ 过 `sql/` 存取
// payload 的那唯一一条路（Marshal ↔ Unmarshal）→ Restore 回一个新 cm 让节点读回来。
//
// 漏了这个字段不报错，只是节点读不到「上次已做完哪些」⟹ 已成功的副作用（下单 / 扣款 /
// 发消息）Resume 时重放一遍。
func TestPayloadRoundTrip_NodeStates(t *testing.T) {
	const nodeID = "worker"
	b := compose_v2.NewBuilder().WithName("ns_wire")
	b.AddNode(nodeID, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		st := &nodeStateProbe{}
		if prev, ok := compose_v2.GetNodeState[*nodeStateProbe](ctx); ok && prev != nil {
			st = prev
		}
		if len(st.Done) == 0 {
			st.Done = append(st.Done, "step-1")
			if err := compose_v2.SaveNodeState(ctx, st); err != nil {
				return nil, err
			}
			// 记完进度就中断——cp 里必须留下这份 memo
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "wire_q", Question: "?"},
			}
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, nodeID)
	b.AddEdge(nodeID, compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sid-ns"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("首跑应中断, got %v", err)
	}
	saved, lerr := cper.LoadNS(ctx, "sid-ns", "")
	if lerr != nil || saved == nil {
		t.Fatalf("应有 cp: %v", lerr)
	}
	if len(saved.NodeStates) == 0 {
		t.Fatal("前提不成立：cp 里没 NodeStates，本测试测不到东西")
	}

	// 过 sql/ 存取 payload 的那条唯一路径
	blob, merr := compose_v2.MarshalCheckpoint(saved)
	if merr != nil {
		t.Fatalf("MarshalCheckpoint: %v", merr)
	}
	got, uerr := compose_v2.UnmarshalCheckpoint(blob)
	if uerr != nil {
		t.Fatalf("UnmarshalCheckpoint: %v", uerr)
	}
	if len(got.NodeStates) != len(saved.NodeStates) {
		t.Fatalf("NodeStates 没过 wire：want %d 条, got %d 条\n"+
			"  漏这个字段不报错——节点读不到「上次已做完哪些」⟹ 已成功的副作用重放一遍",
			len(saved.NodeStates), len(got.NodeStates))
	}

	// 逐字比对：再 Marshal 一次，两次 blob 必须相同（taggedValue 含 json.RawMessage
	// 不可直接 ==，round-trip 幂等是等价的、且更严——键和 Data 任一漂了都会露）
	blob2, m2err := compose_v2.MarshalCheckpoint(got)
	if m2err != nil {
		t.Fatalf("二次 MarshalCheckpoint: %v", m2err)
	}
	if !bytes.Equal(blob, blob2) {
		t.Errorf("round-trip 不幂等——NodeStates 的键或 taggedValue 没逐字过 wire")
	}

	// 让节点自己把进度读回来：Restore 进新 cm 后 GetNodeState 必须拿到 step-1
	cm2 := compose_v2.NewChannelManager()
	if rerr := cm2.Restore(got); rerr != nil {
		t.Fatalf("Restore: %v", rerr)
	}
}
