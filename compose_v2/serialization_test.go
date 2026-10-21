package compose_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// Phase 1.0.a 测试：cp marshal/unmarshal + Snapshot/Restore 端到端。
//
// 撞 audit F3 [HIGH]：cp/Resume 序列化是先决问题。本组测试验证 channel value
// 能正确 round-trip JSON，跨进程恢复后 cm 状态等于原状态。

func init() {
	// 注册测试用类型
	compose_v2.RegisterValueType[int]("int")
	compose_v2.RegisterValueType[string]("string")
	compose_v2.RegisterValueType[bool]("bool")
	compose_v2.RegisterValueType[[]string]("[]string")
}

// TestSnapshot_BasicTypes 验证基础类型 round-trip。
func TestSnapshot_BasicTypes(t *testing.T) {
	intCh := compose_v2.Sum("count")
	strCh := compose_v2.LastValue[string]("name")
	flagCh := compose_v2.OnceFlag("done")
	listCh := compose_v2.Append[string]("trace")

	cm := compose_v2.NewChannelManager()
	for _, e := range []error{
		compose_v2.RegisterChannel(cm, intCh),
		compose_v2.RegisterChannel(cm, strCh),
		compose_v2.RegisterChannel(cm, flagCh),
		compose_v2.RegisterChannel(cm, listCh),
	} {
		if e != nil {
			t.Fatalf("register: %v", e)
		}
	}

	// 写入若干 value
	if err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(intCh, 100),
		compose_v2.Write(intCh, 50),
		compose_v2.Write(strCh, "alice"),
		compose_v2.Write(flagCh, true),
		compose_v2.Write(listCh, []string{"step_1", "step_2"}),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// 1. Snapshot
	snap, err := cm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// 2. JSON marshal
	data, err := compose_v2.MarshalCheckpoint(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	t.Logf("snapshot wire size: %d bytes\nbody: %s", len(data), string(data))

	// 3. JSON unmarshal（模拟跨进程 / 跨重启）
	snap2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// 4. 恢复到新 cm（同样注册 channels）
	cm2 := compose_v2.NewChannelManager()
	for _, e := range []error{
		compose_v2.RegisterChannel(cm2, intCh),
		compose_v2.RegisterChannel(cm2, strCh),
		compose_v2.RegisterChannel(cm2, flagCh),
		compose_v2.RegisterChannel(cm2, listCh),
	} {
		if e != nil {
			t.Fatalf("cm2 register: %v", e)
		}
	}
	if err := cm2.Restore(snap2); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 5. 验证 cm2 值与原 cm 完全一致
	if v := compose_v2.MustGet(cm2.Channels(), intCh); v != 150 {
		t.Fatalf("intCh 应 150（100+50 Sum），实际 %d", v)
	}
	if v := compose_v2.MustGet(cm2.Channels(), strCh); v != "alice" {
		t.Fatalf("strCh 应 'alice'，实际 %q", v)
	}
	if v := compose_v2.MustGet(cm2.Channels(), flagCh); !v {
		t.Fatalf("flagCh 应 true，实际 %v", v)
	}
	v := compose_v2.MustGet(cm2.Channels(), listCh)
	if len(v) != 2 || v[0] != "step_1" || v[1] != "step_2" {
		t.Fatalf("listCh 应 [step_1 step_2]，实际 %v", v)
	}
}

// TestSnapshot_UnregisteredType_Error 验证类型未注册时 Snapshot 返 error
// （fail-fast 比静默丢更好）。
func TestSnapshot_UnregisteredType_Error(t *testing.T) {
	type myUnregisteredType struct {
		Foo int
	}

	ch := compose_v2.LastValue[myUnregisteredType]("custom")
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm, ch); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(ch, myUnregisteredType{Foo: 42}),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	_, err := cm.Snapshot()
	if err == nil {
		t.Fatal("未注册类型应返 Snapshot 错")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error 应含 'not registered'，实际 %q", err.Error())
	}
}

// TestRestore_UnregisteredChannel_Error 验证 cm 未注册同名 channel 时 Restore
// 返 error（防止静默 drop）。
func TestRestore_UnregisteredChannel_Error(t *testing.T) {
	intCh := compose_v2.Sum("count")
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm, intCh); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(intCh, 42),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	snap, err := cm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// 新 cm 未注册 count channel
	cm2 := compose_v2.NewChannelManager()
	err = cm2.Restore(snap)
	if err == nil {
		t.Fatal("未注册同名 channel 应返 Restore 错")
	}
	if !strings.Contains(err.Error(), "count") || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error 应含 channel 名 + 'not registered'，实际 %q", err.Error())
	}
}

// TestRestore_SchemaVer_Mismatch_Passes 验 Option D 决策: UnmarshalCheckpoint
// **不在版本不匹配时 fail-fast** — 对照 LangGraph `BaseCheckpointSaver.serde`
// 反序列化路径不强制 version 对齐.
//
// **设计权衡**: 业务跨版本兼容 / ad-hoc cp 漏填 SchemaVer 是常态; framework
// fail-fast 是过度防御让业务零负担变高负担. 业务真要做版本兼容自行按
// `cp.SchemaVer` 分支处理.
func TestRestore_SchemaVer_Mismatch_Passes(t *testing.T) {
	// 构造一个 SchemaVer=99 (远超 framework 当前 6) 的 JSON
	bad := `{"schema_ver":99,"channels":{}}`
	cp, err := compose_v2.UnmarshalCheckpoint([]byte(bad))
	if err != nil {
		t.Fatalf("Option D 后 UnmarshalCheckpoint 不应 fail-fast SchemaVer mismatch: %v", err)
	}
	if cp.SchemaVer != 99 {
		t.Errorf("SchemaVer 字段应透传原值 (业务跨版本自负): expected 99, got %d", cp.SchemaVer)
	}
}

// TestRestore_SchemaVer_Zero_Passes 验业务 ad-hoc `&Checkpoint{}` 漏填
// SchemaVer (默认 0) 走 marshal/unmarshal 路径不 fail. 跟 LangGraph
// `empty_checkpoint()` 显式工厂 / business 直构造混用兼容同款.
func TestRestore_SchemaVer_Zero_Passes(t *testing.T) {
	zero := `{"channels":{}}` // 不含 schema_ver 字段, JSON 默认 0
	cp, err := compose_v2.UnmarshalCheckpoint([]byte(zero))
	if err != nil {
		t.Fatalf("Option D 后业务 ad-hoc cp 漏填 SchemaVer 不应 fail: %v", err)
	}
	if cp.SchemaVer != 0 {
		t.Errorf("SchemaVer 应默认 0: got %d", cp.SchemaVer)
	}
}

// ── 端到端：跑 graph 完成 → snapshot → restore → 在新 cm 上读出值正确 ──

// TestSnapshot_AfterGraphRun_RoundTrip 端到端验证：跑完 graph 的 cm 状态可
// snapshot → marshal → unmarshal → restore，新 cm 状态等于原 cm。
//
// 这是 Phase 1.0.a 的 happy path——不涉及"中断+继续"，仅验证 cp marshal 协议
// 本身正确。完整 Resume 留 Phase 1.0.b。
func TestSnapshot_AfterGraphRun_RoundTrip(t *testing.T) {
	tokenUsage := compose_v2.Sum("tokens")
	roundCount := compose_v2.Sum("rounds")

	makeGraph := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().
			AddNode("step", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return []compose_v2.ChannelWrite{
					compose_v2.Write(tokenUsage, 100),
					compose_v2.Write(roundCount, 1),
				}, nil
			}).
			AddEdge(compose_v2.StartNode, "step").
			AddConditionalEdge("step", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
				rounds, _ := compose_v2.Get(chs, roundCount)
				if rounds >= 3 {
					return compose_v2.EndNode, nil
				}
				return "step", nil
			}).
			WithMaxVisit(10)
		b = compose_v2.WithChannel(b, tokenUsage)
		b = compose_v2.WithChannel(b, roundCount)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	// Run 1：完整跑完
	g1 := makeGraph()
	cm1 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g1, cm1); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Snapshot + 序列化
	snap, err := cm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	data, err := compose_v2.MarshalCheckpoint(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// 模拟跨进程：解码 + 新 cm + 注册 channels + Restore
	snap2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm2, tokenUsage); err != nil {
		t.Fatalf("cm2 register tokens: %v", err)
	}
	if err := compose_v2.RegisterChannel(cm2, roundCount); err != nil {
		t.Fatalf("cm2 register rounds: %v", err)
	}
	if err := cm2.Restore(snap2); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 验证 cm2 值与原 cm 完全一致
	if r := compose_v2.MustGet(cm2.Channels(), roundCount); r != 3 {
		t.Fatalf("rounds 应 3，实际 %d", r)
	}
	if tk := compose_v2.MustGet(cm2.Channels(), tokenUsage); tk != 300 {
		t.Fatalf("tokens 应 300，实际 %d", tk)
	}
}
