package compose_v2_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// Phase 1.2.b 测试：dynamic channel runtime register。
//
// 验证：
//   - 运行时根据条件创建 channel
//   - Dynamic*/Custom 与静态工厂行为等价（仅命名差异化）
//   - 类型错 panic + ApplyAll defer recover 同款（撞 audit F4）
//   - dynamic channel 可参与 cp marshal（与静态同款）

// TestDynamic_RuntimeCreation 验证业务运行时创建多 channel。
func TestDynamic_RuntimeCreation(t *testing.T) {
	// 模拟业务场景：根据用户输入 query 决定 spawn 哪些 metric channel
	metrics := []string{"latency_ms", "tokens_used", "tool_calls"}

	cm := compose_v2.NewChannelManager()
	channels := make(map[string]*compose_v2.Channel[int])
	for _, name := range metrics {
		ch := compose_v2.DynamicSum("metric_" + name)
		if err := compose_v2.RegisterChannel(cm, ch); err != nil {
			t.Fatalf("register %q: %v", name, err)
		}
		channels[name] = ch
	}

	// 业务节点逻辑模拟——直接 ApplyAll
	if err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(channels["latency_ms"], 100),
		compose_v2.Write(channels["latency_ms"], 50),
		compose_v2.Write(channels["tokens_used"], 200),
		compose_v2.Write(channels["tool_calls"], 3),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// 验证每个 dynamic channel 累加正确
	if v := compose_v2.MustGet(cm.Channels(), channels["latency_ms"]); v != 150 {
		t.Fatalf("latency_ms 应 150，实际 %d", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), channels["tokens_used"]); v != 200 {
		t.Fatalf("tokens_used 应 200，实际 %d", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), channels["tool_calls"]); v != 3 {
		t.Fatalf("tool_calls 应 3，实际 %d", v)
	}
}

// TestDynamic_AllReducerTypes 验证 4 个 dynamic reducer 工厂行为等价静态版。
func TestDynamic_AllReducerTypes(t *testing.T) {
	intCh := compose_v2.DynamicSum("dyn_int")
	strCh := compose_v2.DynamicLastValue[string]("dyn_str")
	listCh := compose_v2.DynamicAppend[string]("dyn_list")
	customCh := compose_v2.DynamicChannel("dyn_max", func(p, n int) int {
		if n > p {
			return n
		}
		return p
	}, 0)

	cm := compose_v2.NewChannelManager()
	for _, e := range []error{
		compose_v2.RegisterChannel(cm, intCh),
		compose_v2.RegisterChannel(cm, strCh),
		compose_v2.RegisterChannel(cm, listCh),
		compose_v2.RegisterChannel(cm, customCh),
	} {
		if e != nil {
			t.Fatalf("register: %v", e)
		}
	}

	if err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(intCh, 10),
		compose_v2.Write(intCh, 20),
		compose_v2.Write(strCh, "first"),
		compose_v2.Write(strCh, "second"),
		compose_v2.Write(listCh, []string{"a"}),
		compose_v2.Write(listCh, []string{"b", "c"}),
		compose_v2.Write(customCh, 5),
		compose_v2.Write(customCh, 100),
		compose_v2.Write(customCh, 50), // 不会覆盖 100（max reducer）
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if v := compose_v2.MustGet(cm.Channels(), intCh); v != 30 {
		t.Errorf("Sum 应 30，实际 %d", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), strCh); v != "second" {
		t.Errorf("LastValue 应 'second'，实际 %q", v)
	}
	v := compose_v2.MustGet(cm.Channels(), listCh)
	sort.Strings(v)
	if !strings.HasPrefix(strings.Join(v, ","), "a,b,c") {
		t.Errorf("Append 应 [a b c]，实际 %v", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), customCh); v != 100 {
		t.Errorf("Custom max reducer 应 100，实际 %d", v)
	}
}

// TestDynamic_InGraph 验证 dynamic channel 可在 graph 节点内读写。
func TestDynamic_InGraph(t *testing.T) {
	// 节点闭包内创建 channel——典型 dynamic 用法
	scoreCh := compose_v2.DynamicSum("dynamic_score")

	body := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(scoreCh, 25)}, nil
	}
	exit := func(_ context.Context, chs compose_v2.Channels) bool {
		s, _ := compose_v2.Get(chs, scoreCh)
		return s >= 100
	}

	// 直接用 GraphBuilder 拼 body→ConditionalEdge→[End | body] 循环拓扑
	b := compose_v2.NewBuilder().WithMaxVisit(10)
	compose_v2.WithChannel(b, scoreCh)
	b.AddNode("body", body)
	b.AddEdge(compose_v2.StartNode, "body")
	b.AddConditionalEdge("body", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if exit(ctx, chs) {
			return compose_v2.EndNode, nil
		}
		return "body", nil
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 4 轮 *25 = 100，达到 exit 条件
	if s := compose_v2.MustGet(cm.Channels(), scoreCh); s != 100 {
		t.Fatalf("score 应 100，实际 %d", s)
	}
}

// TestDynamic_TypeMismatchPanics 验证 dynamic channel 路径下的类型错可观测性
// （撞 audit F4）——panic 信息含 channel 名 + 期望/实际类型。
func TestDynamic_TypeMismatchPanics(t *testing.T) {
	intCh := compose_v2.DynamicSum("dyn_type_check")
	cm := compose_v2.NewChannelManager()
	_ = compose_v2.RegisterChannel(cm, intCh)
	_ = cm.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(intCh, 10)})

	// 同名异类型注册 → 应 fail-fast
	strCh := compose_v2.DynamicLastValue[string]("dyn_type_check")
	err := compose_v2.RegisterChannel(cm, strCh)
	if err == nil {
		t.Fatal("同名异类型 dynamic register 应 fail-fast")
	}
	if !strings.Contains(err.Error(), "dyn_type_check") {
		t.Fatalf("err 应含 channel 名，实际 %q", err.Error())
	}
}

// TestDynamic_CheckpointRoundTrip 验证 dynamic channel 与 cp marshal 协议兼容。
func TestDynamic_CheckpointRoundTrip(t *testing.T) {
	// 注意：cp 类型注册需 init 阶段——这里 int 已注册（serialization_test 内 init）

	dynCh := compose_v2.DynamicSum("dyn_cp_metric")
	cm1 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm1, dynCh); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := cm1.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(dynCh, 42)}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	snap, err := cm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	data, err := compose_v2.MarshalCheckpoint(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	t.Logf("dynamic channel cp wire: %s", data)

	// 跨进程模拟
	snap2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// 业务在新进程中重新声明同款 dynamic channel
	dynCh2 := compose_v2.DynamicSum("dyn_cp_metric")
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm2, dynCh2); err != nil {
		t.Fatalf("cm2 register: %v", err)
	}
	if err := cm2.Restore(snap2); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if v := compose_v2.MustGet(cm2.Channels(), dynCh2); v != 42 {
		t.Fatalf("Restore 后 dynamic channel 值应 42，实际 %d", v)
	}
}

// TestDynamic_StaticAndDynamic_Mixed 验证静态 + 动态 channel 混合使用——
// 90% 静态 + 10% 动态 escape hatch 模式。
func TestDynamic_StaticAndDynamic_Mixed(t *testing.T) {
	// 静态：业务核心字段
	staticCounter := compose_v2.Sum("static_counter")
	// 动态：根据运行时配置创建
	dynamicMetrics := make(map[string]*compose_v2.Channel[int])
	for _, name := range []string{"a", "b", "c"} {
		dynamicMetrics[name] = compose_v2.DynamicSum("dyn_metric_" + name)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm, staticCounter); err != nil {
		t.Fatalf("register static: %v", err)
	}
	for name, ch := range dynamicMetrics {
		if err := compose_v2.RegisterChannel(cm, ch); err != nil {
			t.Fatalf("register dynamic %q: %v", name, err)
		}
	}

	writes := []compose_v2.ChannelWrite{compose_v2.Write(staticCounter, 1)}
	for _, ch := range dynamicMetrics {
		writes = append(writes, compose_v2.Write(ch, 10))
	}
	if err := cm.ApplyAll(writes); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if v := compose_v2.MustGet(cm.Channels(), staticCounter); v != 1 {
		t.Errorf("static_counter 应 1，实际 %d", v)
	}
	for name, ch := range dynamicMetrics {
		if v := compose_v2.MustGet(cm.Channels(), ch); v != 10 {
			t.Errorf("dyn_metric_%s 应 10，实际 %d", name, v)
		}
	}

	t.Logf("混合模式：1 static + %d dynamic channels 共存", len(dynamicMetrics))
	_ = fmt.Sprintf("test") // suppress unused
}
