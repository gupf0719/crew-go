package registry_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/a2a/registry"
	"crew-go/a2a/registry/conformance"
)

// TestInMemory_Conformance 跑协议合规套——InMemory 作为参考实现必须全过。
func TestInMemory_Conformance(t *testing.T) {
	conformance.RunDirectoryTests(t, func() registry.Directory {
		return registry.NewInMemory()
	})
}

func TestEligible(t *testing.T) {
	insts := []registry.ServiceInstance{
		{InstanceID: "serving", Status: registry.StatusServing, Weight: registry.WeightPtr(100)},
		{InstanceID: "draining", Status: registry.StatusDraining, Weight: registry.WeightPtr(100)},
		{InstanceID: "oos", Status: registry.StatusOutOfService, Weight: registry.WeightPtr(100)},
		{InstanceID: "zeroweight", Status: registry.StatusServing, Weight: registry.WeightPtr(0)},
	}
	got := registry.Eligible(insts)
	if len(got) != 1 || got[0].InstanceID != "serving" {
		t.Fatalf("Eligible 应只返 SERVING+weight>0, got %+v", got)
	}
}

// TestWeightZeroPreserved 验证 W1 fix：显式 weight=0（不参与选择）不被默认成 100；nil（未给）
// 才默认 100。**sanity-revert**：把 WithDefaults 的 `if s.Weight == nil` 改回也吃显式 0
// （如 `|| *s.Weight == 0`）后，"显式 0 保留"断言 fail。
func TestWeightZeroPreserved(t *testing.T) {
	base := registry.ServiceInstance{
		SchemaVersion: 1, ServiceName: "s", InstanceID: "i",
		AgentCardURL: "http://x/c", Status: registry.StatusServing,
	}
	// nil Weight → WithDefaults → 100 → eligible
	nilW := base.WithDefaults()
	if nilW.Weight == nil || *nilW.Weight != registry.DefaultWeight {
		t.Fatalf("nil weight 应默认 100, got %v", nilW.Weight)
	}
	if len(registry.Eligible([]registry.ServiceInstance{nilW})) != 1 {
		t.Fatal("默认 weight=100 实例应 eligible")
	}
	// 显式 0 → WithDefaults 保留 0 → 不 eligible（§3.1 不参与选择）
	zeroW := base
	zeroW.Weight = registry.WeightPtr(0)
	zeroW = zeroW.WithDefaults()
	if zeroW.Weight == nil || *zeroW.Weight != 0 {
		t.Fatalf("显式 weight=0 应保留 0（不该被默认成 100）, got %v", zeroW.Weight)
	}
	if len(registry.Eligible([]registry.ServiceInstance{zeroW})) != 0 {
		t.Fatal("weight=0 实例应被 Eligible 排除")
	}
}

func TestRoundRobinSelector(t *testing.T) {
	insts := []registry.ServiceInstance{
		{InstanceID: "a", Status: registry.StatusServing, Weight: registry.WeightPtr(1)},
		{InstanceID: "b", Status: registry.StatusServing, Weight: registry.WeightPtr(1)},
	}
	s := registry.NewRoundRobinSelector()
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		inst, ok := s.Select(insts, nil)
		if !ok {
			t.Fatal("Select 应成功")
		}
		seen[inst.InstanceID]++
	}
	if seen["a"] != 2 || seen["b"] != 2 {
		t.Fatalf("轮询应均匀分布, got %+v", seen)
	}
}

// TestSelector_NoRepeatTried 验证 §6"同一次 failover 不重选已试过实例"。
func TestSelector_NoRepeatTried(t *testing.T) {
	insts := []registry.ServiceInstance{
		{InstanceID: "a", Status: registry.StatusServing, Weight: registry.WeightPtr(1)},
		{InstanceID: "b", Status: registry.StatusServing, Weight: registry.WeightPtr(1)},
	}
	for _, s := range []registry.Selector{
		registry.NewRoundRobinSelector(),
		registry.NewRandomSelector(),
		registry.NewWeightedSelector(),
	} {
		tried := map[string]struct{}{registry.InstanceIdentity(insts[0]): {}}
		inst, ok := s.Select(insts, tried)
		if !ok || inst.InstanceID != "b" {
			t.Fatalf("%T: 应跳过 tried=a 选 b, got ok=%v inst=%q", s, ok, inst.InstanceID)
		}
		tried[registry.InstanceIdentity(insts[1])] = struct{}{}
		if _, ok := s.Select(insts, tried); ok {
			t.Fatalf("%T: 全 tried 应返 ok=false", s)
		}
	}
}

// TestSelector_FailoverReachesSiblingAgent 验证 4 层后同 instanceId 不同 agentName 是不同实例
// （同端口多 Agent Card 场景，§ZK 节点模型）——failover 去重身份用 InstanceIdentity（agentName+
// instanceId）而非裸 instanceId，否则 sibling agent 被误判已试漏选。
// **sanity-revert**：把 selector untried 的 tried key 从 InstanceIdentity(in) 改回裸 in.InstanceID 后，
// 第三次"全试过应选不出"断言 fail（untried 用 instanceId 查 InstanceIdentity-key 的 tried 永远 miss → 永远能选）。
func TestSelector_FailoverReachesSiblingAgent(t *testing.T) {
	a := registry.ServiceInstance{ServiceName: "svc", AgentName: "agent-a", InstanceID: "10.0.0.1:8080", Status: registry.StatusServing, Weight: registry.WeightPtr(1)}
	b := registry.ServiceInstance{ServiceName: "svc", AgentName: "agent-b", InstanceID: "10.0.0.1:8080", Status: registry.StatusServing, Weight: registry.WeightPtr(1)}
	all := []registry.ServiceInstance{a, b}
	s := registry.NewRoundRobinSelector()
	tried := map[string]struct{}{}

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		inst, ok := s.Select(all, tried)
		if !ok {
			t.Fatalf("第 %d 次 failover 应选到实例（两个同 instanceId 不同 agentName 都该可达）, got ok=false", i+1)
		}
		seen[inst.AgentName] = true
		tried[registry.InstanceIdentity(inst)] = struct{}{}
	}
	if !seen["agent-a"] || !seen["agent-b"] {
		t.Fatalf("failover 应遍历到两个不同 agentName 的实例（同 instanceId 不该互相屏蔽）, got seen=%v", seen)
	}
	// 两个都试过 → 第三次选不出（裸 instanceId 去重会让此断言 fail）
	if _, ok := s.Select(all, tried); ok {
		t.Fatal("两个实例都试过后应选不出（tried 身份用 InstanceIdentity 才能正确判全试过）")
	}
}

// TestParseDiscoveredJSON 验证 binding 层 raw JSON 安全解析（W4）+ weight=0 经 JSON wire
// round-trip 保留（W1 跨语言关键——这是不靠真 ZK 的 wire 级验证）。
func TestParseDiscoveredJSON(t *testing.T) {
	// 显式 weight=0 → 序列化为 "weight":0 → 解析回保留 0 → Eligible 排除
	in := registry.ServiceInstance{
		SchemaVersion: 1, ServiceName: "s", InstanceID: "i",
		AgentCardURL: "http://h/c", Status: registry.StatusServing, Weight: registry.WeightPtr(0),
	}
	data, _ := json.Marshal(in)
	if !strings.Contains(string(data), `"weight":0`) {
		t.Fatalf(`weight=0 应序列化为 "weight":0（W1），got %s`, data)
	}
	got, err := registry.ParseDiscoveredJSON(data)
	if err != nil {
		t.Fatalf("解析合法 payload: %v", err)
	}
	if got.Weight == nil || *got.Weight != 0 {
		t.Fatalf("weight=0 应 wire round-trip 保留 0, got %v", got.Weight)
	}
	if len(registry.Eligible([]registry.ServiceInstance{got})) != 0 {
		t.Fatal("wire 读回的 weight=0 应被 Eligible 排除（跨语言 weight=0 不路由）")
	}

	// 未给 weight → omitempty 不序列化 → 解析回默认 100
	in2 := registry.ServiceInstance{SchemaVersion: 1, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://h/c", Status: registry.StatusServing}
	data2, _ := json.Marshal(in2)
	if strings.Contains(string(data2), "weight") {
		t.Fatalf("未给 weight 不该序列化, got %s", data2)
	}
	got2, _ := registry.ParseDiscoveredJSON(data2)
	if got2.Weight == nil || *got2.Weight != registry.DefaultWeight {
		t.Fatalf("未给 weight 应默认 100, got %v", got2.Weight)
	}

	// malformed payload → error（binding 应 continue 跳过该节点）
	bad := map[string]string{
		"JSON 语法错":            `{`,
		"metadata 非字符串 value": `{"schemaVersion":1,"serviceName":"s","instanceId":"i","agentCardUrl":"http://h/c","metadata":{"k":123}}`,
		"缺 schemaVersion":     `{"serviceName":"s","instanceId":"i","agentCardUrl":"http://h/c"}`,
		"缺 serviceName":       `{"schemaVersion":1,"instanceId":"i","agentCardUrl":"http://h/c"}`,
		"相对 agentCardUrl":     `{"schemaVersion":1,"serviceName":"s","instanceId":"i","agentCardUrl":"relative/path"}`,
	}
	for name, b := range bad {
		if _, err := registry.ParseDiscoveredJSON([]byte(b)); err == nil {
			t.Errorf("malformed [%s] 应返 error 让跳过: %s", name, b)
		}
	}
}

// TestParseDiscovered_SkipsBadSchema 验证读侧 schemaVersion 缺失(0)/未知(2,99) 都返 error
// 让发现端跳过（W6）。**sanity-revert**：把 ParseDiscovered 改回 `!= 0 && != Current` 后
// sv=0 这条 fail（缺失被放行）。
func TestParseDiscovered_SkipsBadSchema(t *testing.T) {
	for _, sv := range []int{0, 2, 99} { // 0=缺失（必填），2/99=未知版本
		_, err := registry.ParseDiscovered(registry.ServiceInstance{
			SchemaVersion: sv, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://x/c",
		})
		if err == nil {
			t.Errorf("schemaVersion=%d 应返 error 让发现端跳过该节点", sv)
		}
	}
}

// TestInMemory_ConcurrentMetadataRace 是 W2 的 -race 锚点：并发 Discover（改返回的 Metadata）
// + Register（刷新），验证返回的 map 与存储无别名共享。**sanity-revert**：去掉 InMemory 的
// cloneMetadata 后 `go test -race` 报 DATA RACE（发现方写返回 map vs Discover 经 Validate 读存储 map）。
func TestInMemory_ConcurrentMetadataRace(t *testing.T) {
	dir := registry.NewInMemory()
	ctx := context.Background()
	mk := func(id string) registry.ServiceInstance {
		return registry.ServiceInstance{
			SchemaVersion: registry.CurrentSchemaVersion, ServiceName: "svc", InstanceID: id,
			AgentCardURL: "http://x/c", Status: registry.StatusServing, Weight: registry.WeightPtr(100),
			Metadata: map[string]string{"k": "v"},
		}
	}
	if _, err := dir.Register(ctx, registry.DefaultNamespace, mk("i0")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{}) // 屏障：让所有 goroutine 尽量同时冲，强制真并发
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, _ := dir.Discover(ctx, registry.DefaultNamespace, "svc")
			for _, g := range got {
				if g.Metadata != nil {
					g.Metadata["mutated"] = "x" // 并发改返回的 map——若与存储别名则 race
				}
			}
		}()
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			_, _ = dir.Register(ctx, registry.DefaultNamespace, mk(fmt.Sprintf("i%d", n+1)))
		}(i)
	}
	close(start)
	wg.Wait()
}
