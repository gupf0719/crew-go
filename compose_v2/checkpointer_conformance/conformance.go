// Package checkpointer_conformance 提供 compose_v2.Checkpointer 接口的一致性测试
// 套件——业务实现自定义 backend (PostgresCheckpointer / RedisCheckpointer 等) 后,
// 在自己的 _test.go 里 import 本包 + 调 `RunCheckpointerTests` 验证语义合规.
//
// 用法示例:
//
//	package mybackend_test
//
//	import (
//	    "testing"
//	    "crew-go/compose_v2"
//	    "crew-go/compose_v2/checkpointer_conformance"
//	    "myorg/mybackend"
//	)
//
//	func TestMyPGCheckpointer_Conformance(t *testing.T) {
//	    checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
//	        cp := mybackend.NewPGCheckpointer("postgres://...")
//	        t.Cleanup(func() { _ = cp.Close() })
//	        return cp
//	    })
//	}
//
// **覆盖**: 9 类 (SaveLoad / Exists / Delete / ListPending / LoadAllInSession /
// SaveBatch / GraphIDConflict / Cleanup / Concurrent) 对照
// InMemoryCheckpointer 行为锚定语义.
//
// # 业务必实现 capability
//
// 所有 8 个 Checkpointer 方法都是 mandatory (不像 store.TTLAware 有 opt-in
// skip 路径). 业务 backend 必实现:
//   - SaveNS / LoadNS / ExistsNS / DeleteSubtree
//   - ListPending / LoadAllInSession / SaveBatch
//   - Cleanup (keepLastN 按 sid 维度排序)
//
// 业务**必保 round-trip** 的字段 (audit cross-verify 强 contract):
//   - GraphID / Status / ParentNS / SchemaVer / CreatedAt
//     (CreatedAt 还有一条**反向**契约: caller 给 0 时 backend **不许自己填**——
//     框架拿 0 当"还没盖章"的判据, backend 抢填会让 Cleanup 按错误时间线排序)
//   - **CurrentFrontier []FrontierMember** (frontier 在 Resume 时寻址; 每条记录
//     自带 node_id + instance_id, **身份内联**不是平行数组)
//   - **NodeVisits map[NodeID]int** (InterruptID 5 维稳定 ID 编入 visit 维度)
//   - **PendingInterrupts []*AskQuery** (HITL 答案配对靠 InterruptID 路由)
//   - **ConsumedAnswers []string** (已兑付的答案位——漏了不报错, 只是「一个答案只能满足
//     一个提问」那道兜底跨 Run 失效: 身份算错时旧答案又会静默满足新提问)
//   - **Signals []SignalRecord** (已提交的上行信号——漏了不报错, 只是做出转交 / 路由决定的
//     那一步续跑时不再执行, 决定静默丢失) + **CompletedSignals** (部分失败超步里成功成员先记在
//     一边的信号, 同一族) + **RecipientKey []byte** (铸收件人的密钥——漏了续跑时重铸、收件人
//     变了, 停下前已提交的决定同样静默丢失)
//   - **CompletedTargets []NodeID** (部分失败超步里**成功成员当时算好的出边目标**——
//     漏了不报错, 只是发散 fan-out HITL Resume 后成功分支的后继静默丢失、graph 假成功。
//     出边计算归成员之后**承载后继的是本字段**, 不是 CompletedFrontier)
//   - **CompletedFrontier []FrontierMember** (同一批成功成员的**身份**; 与
//     CompletedTargets 分工: 身份用于 cp 记账与 sends 归属, 出边由 CompletedTargets 承载)
//   - **DeferredSends []SerializedSend** (全字段含 **Owner** + **OwnerInstanceID**
//   - **InstanceID**——漏任一个都不报错: 前两个漏 → 重跑目标的旧 send 清不掉 →
//     Target 双发; 第三个漏 → 重放出的 N 个实例全空身份彼此无法区分 → HITL
//     InterruptID 撞车 / 子 cp 互覆 / 观测 span 共槽)
//   - **Epoch string** (世代标记, 每次写换新——漏了不报错, 只是
//     `WithStaleWriteGuard` 静默失效: 守卫读到空 Epoch 就跳过比对)
//   - **PendingSendWrites []SerializedWrite** (已被回滚、待 Resume 补 apply 的
//     SendFrom payload——漏了 target 在 Resume 后静默读到零值)
//   - 两组 frontier 里的 **FrontierMember.InstanceID** (branch 实例身份, ISSUE-96——
//     漏了多实例场景 Resume 后实例互相冒充、重跑成员的旧记录清不掉;
//     **InstanceID 为空的那条记录也要整条保留**, 空表达「该成员是静态边节点」
//     不是「没值」)
//
// **Channels 字段** (内部 taggedValue 类型) 业务 backend 通常 marshal 整 cp
// 为 bytes, 不直构. `SaveNS_Channels_Roundtrip` 用 framework helper 构造 fixture
// 验它能穿过 backend 的 wire; 至于 backend 内部用什么 wire format, 业务自测.
//
// # 业务必遵守的 sentinel error
//
//   - `ErrGraphIDConflict` — SaveNS 不同 GraphID 共享 (sid, ns) 必返此
package checkpointer_conformance

import (
	"testing"

	"crew-go/compose_v2"
)

// CheckpointerFactory 工厂函数 — 每次调用返新 Checkpointer instance.
//
// 业务 backend 自带 Close 在 factory 内 t.Cleanup 注册:
//
//	factory := func() compose_v2.Checkpointer {
//	    cp := NewMyPGCheckpointer(...)
//	    t.Cleanup(func() { _ = cp.Close() })
//	    return cp
//	}
type CheckpointerFactory func() compose_v2.Checkpointer

// RunCheckpointerTests 跑全套 Checkpointer 一致性测试.
//
// 子测试用 t.Run 分组 — 业务 `go test -run TestMyCP/SaveLoad` 可只跑某组.
func RunCheckpointerTests(t *testing.T, factory CheckpointerFactory) {
	t.Helper()
	t.Run("SaveLoad", func(t *testing.T) { testSaveLoad(t, factory) })
	t.Run("Exists", func(t *testing.T) { testExists(t, factory) })
	t.Run("KeyIdentity", func(t *testing.T) { testKeyIdentity(t, factory) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, factory) })
	t.Run("ListPending", func(t *testing.T) { testListPending(t, factory) })
	t.Run("LoadAllInSession", func(t *testing.T) { testLoadAllInSession(t, factory) })
	t.Run("SaveBatch", func(t *testing.T) { testSaveBatch(t, factory) })
	t.Run("SaveBatchNodeStates", func(t *testing.T) { testSaveBatchNodeStates(t, factory) })
	t.Run("GraphIDConflict", func(t *testing.T) { testGraphIDConflict(t, factory) })
	t.Run("Cleanup", func(t *testing.T) { testCleanup(t, factory) })
	t.Run("CleanupNodeStates", func(t *testing.T) { testCleanupNodeStates(t, factory) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, factory) })
}

// newCheckpoint 构造测试用 *Checkpoint **最小** helper (仅 GraphID + Status +
// CreatedAt=1). 业务关心的全字段 round-trip 不靠 helper 默认填字段实现 (audit
// iter-3 教训 R29 第 3 次实证: helper 默认非空让 SaveNS auto-fill 路径丢测试
// 覆盖 — `InMemoryCheckpointer.SaveNS` 的 auto-fill 协议
// `if cp.ParentNS == "" && ns != "" { cp.ParentNS = ParentNS(ns) }`), 而是在
// `SaveNS_Preserves_All_Plain_Fields` 显式构造全字段 fixture + 加
// `SaveNS_AutoFills_ParentNS_From_NS` 验 auto-fill 协议.
//
// **iter-5-H Option D**: SchemaVer 不再 fail-fast, 业务 ad-hoc
// cp 漏填 SchemaVer (默认 0) 也能 roundtrip — helper 不填 SchemaVer 让 `SaveNS_*`
// 测试同时覆盖业务 ad-hoc 路径 (Unmarshal 不强制对齐 SchemaVer, 容忍默认 0).
//
// 业务必保 round-trip 字段 (audit cross-verify 强 contract, 见 godoc 表):
// 见 `SaveNS_Preserves_All_Plain_Fields` 显式构造.
//
// Channels 字段框架内部 taggedValue 类型业务侧不直构, 留 nil — 业务 backend
// 通常 marshal 整 cp 为 bytes, Channels 序列化走 framework reducer.
// `SaveNS_Channels_Roundtrip` 单独用 framework helper 构造 Channels fixture,
// 验它能穿过 backend 的 wire.
func newCheckpoint(graphID string, status compose_v2.CheckpointStatus) *compose_v2.Checkpoint {
	return &compose_v2.Checkpoint{
		GraphID:   graphID,
		Status:    status,
		CreatedAt: 1,
	}
}
