package checkpointer_conformance

import (
	"testing"

	"crew-go/compose_v2"
)

// TestNewCheckpoint_Marshal_Unmarshal_Roundtrip 验 conformance helper 构造的
// cp 走 Marshal/Unmarshal 路径 round-trip 成功 — 业务真接 sql/Redis/mem0 backend
// 时不踩任何反序列化 fail.
//
// **iter-5-H Option D 对照 LangGraph 决策**: helper 故意不填 SchemaVer 让测试
// 同时覆盖业务 ad-hoc `&Checkpoint{}` (默认 SchemaVer=0) 路径. 跟 LangGraph
// `BaseCheckpointSaver.serde` 路径**不在反序列化时 fail-fast** version mismatch
// 同款 — 业务零负担, ad-hoc cp 走通完整 roundtrip.
//
// **sanity-revert 验证**: 把 UnmarshalCheckpoint 加回 SchemaVer fail-fast 检查 →
// 此测试 fail (默认 0 != 6 直接 Unmarshal 报错). 业务真 backend 跑 conformance
// 同款 fail 路径.
func TestNewCheckpoint_Marshal_Unmarshal_Roundtrip(t *testing.T) {
	cp := newCheckpoint("g_test", compose_v2.CPStatusPending)
	cp.ID = "sid_test"
	cp.NS = "ns_test"
	// 故意不填 SchemaVer — 业务 ad-hoc 漏填路径

	// Marshal (业务 sql backend SaveNS 路径)
	data, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		t.Fatalf("MarshalCheckpoint: %v", err)
	}

	// Unmarshal (业务 sql backend LoadNS 路径)
	loaded, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("UnmarshalCheckpoint failed: %v (Option D 后应不 fail-fast SchemaVer mismatch)", err)
	}

	// 验关键字段 round-trip
	if loaded.GraphID != "g_test" {
		t.Errorf("GraphID round-trip: expected g_test, got %q", loaded.GraphID)
	}
	if loaded.Status != compose_v2.CPStatusPending {
		t.Errorf("Status round-trip: expected Pending, got %v", loaded.Status)
	}
	// SchemaVer 默认 0 透传不被框架改写 — 业务跨版本兼容自负
	if loaded.SchemaVer != 0 {
		t.Errorf("SchemaVer should not be auto-filled by framework (Option D 不 mutate 入参 + 不 fail-fast): got %d", loaded.SchemaVer)
	}
}

// TestCheckpoint_SchemaVer_Roundtrip_When_Set 验业务显式填 SchemaVer 时 marshal/
// unmarshal 路径正确 round-trip 该字段 (跨版本兼容业务可按 cp.SchemaVer 分支处理).
func TestCheckpoint_SchemaVer_Roundtrip_When_Set(t *testing.T) {
	cp := newCheckpoint("g", compose_v2.CPStatusPending)
	cp.SchemaVer = compose_v2.CheckpointSchemaVer

	data, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		t.Fatalf("MarshalCheckpoint: %v", err)
	}
	loaded, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("UnmarshalCheckpoint: %v", err)
	}
	if loaded.SchemaVer != compose_v2.CheckpointSchemaVer {
		t.Errorf("SchemaVer round-trip: expected %d, got %d",
			compose_v2.CheckpointSchemaVer, loaded.SchemaVer)
	}
}

// TestMarshalCheckpoint_Does_Not_Mutate_Input 验 Option D 后 MarshalCheckpoint
// 是纯函数 — 业务持有的 cp 实例字段不被改写 (跟 LangGraph 一样 marshal 不 mutate
// 入参).
//
// **sanity-revert**: 在 MarshalCheckpoint 加回 `if cp.SchemaVer == 0 { cp.SchemaVer = ... }`
// → 此测试 fail (业务 cp 被 side-effect 改写).
func TestMarshalCheckpoint_Does_Not_Mutate_Input(t *testing.T) {
	cp := &compose_v2.Checkpoint{
		GraphID: "g",
		Status:  compose_v2.CPStatusPending,
		// 不填 SchemaVer (默认 0)
	}
	_, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		t.Fatalf("MarshalCheckpoint: %v", err)
	}
	if cp.SchemaVer != 0 {
		t.Errorf("MarshalCheckpoint should not mutate input cp.SchemaVer (LangGraph 一样 marshal 是纯函数): got %d", cp.SchemaVer)
	}
}

// TestCheckpointSchemaVer_Exported 验 CheckpointSchemaVer 是 exported const.
// 业务跨版本兼容 fixture 用 `compose_v2.CheckpointSchemaVer` 访问 framework
// 当前版本.
func TestCheckpointSchemaVer_Exported(t *testing.T) {
	if compose_v2.CheckpointSchemaVer <= 0 {
		t.Errorf("CheckpointSchemaVer should be > 0, got %d", compose_v2.CheckpointSchemaVer)
	}
}
