// Package conformance 提供 registry.Directory 实现的协议合规测试套。
//
// 业务自实现 backend（ZooKeeper / Nacos / etcd / Consul）时跑：
//
//	func TestMyZKDirectory(t *testing.T) {
//	    conformance.RunDirectoryTests(t, func() registry.Directory {
//	        d := myzk.New("zk://localhost:2181")
//	        t.Cleanup(func() { _ = d.Close() })
//	        return d
//	    })
//	}
//
// 覆盖 Directory 接口级语义（注册/发现 round-trip、缺省值、namespace/service 隔离、
// deregister、update-status、非法拒绝、默认 namespace 归一）。**不覆盖** binding 特有细节
// （malformed 节点跳过、ZK 路径↔payload 一致性、Base64url 编码、ephemeral 断连消失）——
// 那些需 binding 自测能注入裸节点，见各 binding 自己的测试。
package conformance

import (
	"context"
	"errors"
	"testing"

	"crew-go/a2a/registry"
)

// RunDirectoryTests 对一个 registry.Directory 实现跑全部合规子测试。
// factory 每次调用返回一个**干净**的 Directory（含 t.Cleanup 注册的资源释放）。
func RunDirectoryTests(t *testing.T, factory func() registry.Directory) {
	t.Helper()
	t.Run("RegisterDiscoverRoundTrip", func(t *testing.T) { testRoundTrip(t, factory()) })
	t.Run("Defaults", func(t *testing.T) { testDefaults(t, factory()) })
	t.Run("EmptyServiceReturnsEmpty", func(t *testing.T) { testEmpty(t, factory()) })
	t.Run("NamespaceIsolation", func(t *testing.T) { testNamespaceIsolation(t, factory()) })
	t.Run("ServiceIsolation", func(t *testing.T) { testServiceIsolation(t, factory()) })
	t.Run("MultipleInstances", func(t *testing.T) { testMultipleInstances(t, factory()) })
	t.Run("MultipleAgentNames", func(t *testing.T) { testMultipleAgentNames(t, factory()) })
	t.Run("SameInstanceDifferentAgentName", func(t *testing.T) { testSameInstanceDifferentAgentName(t, factory()) })
	t.Run("Deregister", func(t *testing.T) { testDeregister(t, factory()) })
	t.Run("UpdateStatus", func(t *testing.T) { testUpdateStatus(t, factory()) })
	t.Run("RejectsInvalid", func(t *testing.T) { testRejectsInvalid(t, factory()) })
	t.Run("DefaultNamespaceNormalization", func(t *testing.T) { testDefaultNamespace(t, factory()) })
	t.Run("MetadataSnapshotIsolation", func(t *testing.T) { testMetadataSnapshotIsolation(t, factory()) })
	t.Run("DuplicateRegisterRejected", func(t *testing.T) { testDuplicateRegister(t, factory()) })
	t.Run("InvalidStatusRejected", func(t *testing.T) { testInvalidStatus(t, factory()) })
}

const ns = "prod"

func sample(name, id string) registry.ServiceInstance {
	return registry.ServiceInstance{
		SchemaVersion: registry.CurrentSchemaVersion,
		ServiceName:   name,
		InstanceID:    id,
		AgentCardURL:  "http://" + id + "/.well-known/agent-card.json",
		BaseURL:       "http://" + id,
		Transport:     registry.TransportJSONRPC,
		Status:        registry.StatusServing,
		Weight:        registry.WeightPtr(100),
		Metadata:      map[string]string{"zone": "az1", "language": "go"},
	}
}

func mustRegister(t *testing.T, d registry.Directory, namespace string, inst registry.ServiceInstance) registry.Registration {
	t.Helper()
	reg, err := d.Register(context.Background(), namespace, inst)
	if err != nil {
		t.Fatalf("Register(%s/%s) 失败: %v", namespace, inst.InstanceID, err)
	}
	return reg
}

func discover(t *testing.T, d registry.Directory, namespace, svc string) []registry.ServiceInstance {
	t.Helper()
	got, err := d.Discover(context.Background(), namespace, svc)
	if err != nil {
		t.Fatalf("Discover(%s/%s) 失败: %v", namespace, svc, err)
	}
	return got
}

func findByID(insts []registry.ServiceInstance, id string) (registry.ServiceInstance, bool) {
	for _, in := range insts {
		if in.InstanceID == id {
			return in, true
		}
	}
	return registry.ServiceInstance{}, false
}

func testRoundTrip(t *testing.T, d registry.Directory) {
	in := sample("research-agent", "10.0.0.12:8080")
	mustRegister(t, d, ns, in)
	got := discover(t, d, ns, "research-agent")
	if len(got) != 1 {
		t.Fatalf("期望 1 实例, got %d", len(got))
	}
	g := got[0]
	if g.ServiceName != in.ServiceName || g.InstanceID != in.InstanceID ||
		g.AgentCardURL != in.AgentCardURL || g.BaseURL != in.BaseURL ||
		g.Transport != in.Transport || g.Status != in.Status {
		t.Fatalf("字段 round-trip 不一致:\n in=%+v\nout=%+v", in, g)
	}
	// Weight 是 *int——按值比（指针比在 InMemory 假绿、wire backend 必 fail）
	if g.Weight == nil || in.Weight == nil || *g.Weight != *in.Weight {
		t.Fatalf("weight round-trip 不一致: in=%v out=%v", in.Weight, g.Weight)
	}
	if g.Metadata["zone"] != "az1" || g.Metadata["language"] != "go" {
		t.Fatalf("metadata round-trip 丢失: %+v", g.Metadata)
	}
	// agentName 缺省 = serviceName（sample 未设 AgentName，WithDefaults 补）
	if g.AgentName != in.ServiceName {
		t.Fatalf("agentName 缺省应 = serviceName(%q), got %q", in.ServiceName, g.AgentName)
	}
}

func testDefaults(t *testing.T, d registry.Directory) {
	// 只给必填字段，transport/status/weight/schemaVersion 全缺省。
	in := registry.ServiceInstance{
		ServiceName:  "svc",
		InstanceID:   "i1",
		AgentCardURL: "http://i1/.well-known/agent-card.json",
	}
	mustRegister(t, d, ns, in)
	got := discover(t, d, ns, "svc")
	if len(got) != 1 {
		t.Fatalf("期望 1 实例, got %d", len(got))
	}
	g := got[0]
	if g.SchemaVersion != registry.CurrentSchemaVersion {
		t.Errorf("schemaVersion 默认应为 %d, got %d", registry.CurrentSchemaVersion, g.SchemaVersion)
	}
	if g.Transport != registry.DefaultTransport {
		t.Errorf("transport 默认应为 %q, got %q", registry.DefaultTransport, g.Transport)
	}
	if g.Status != registry.DefaultStatus {
		t.Errorf("status 默认应为 %q, got %q", registry.DefaultStatus, g.Status)
	}
	if g.Weight == nil || *g.Weight != registry.DefaultWeight {
		t.Errorf("weight 默认应为 %d, got %v", registry.DefaultWeight, g.Weight)
	}
}

func testEmpty(t *testing.T, d registry.Directory) {
	got, err := d.Discover(context.Background(), ns, "nonexistent")
	if err != nil {
		t.Fatalf("发现不存在的服务应返 nil error, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("发现不存在的服务应返空列表, got %d", len(got))
	}
}

func testNamespaceIsolation(t *testing.T, d registry.Directory) {
	mustRegister(t, d, "ns-a", sample("svc", "i1"))
	if got := discover(t, d, "ns-b", "svc"); len(got) != 0 {
		t.Fatalf("ns-b 不应看到 ns-a 的实例, got %d", len(got))
	}
	if got := discover(t, d, "ns-a", "svc"); len(got) != 1 {
		t.Fatalf("ns-a 应看到自己的实例, got %d", len(got))
	}
}

func testServiceIsolation(t *testing.T, d registry.Directory) {
	mustRegister(t, d, ns, sample("svc1", "i1"))
	mustRegister(t, d, ns, sample("svc2", "i1")) // 同 instanceId 不同 service
	got := discover(t, d, ns, "svc1")
	if len(got) != 1 || got[0].ServiceName != "svc1" {
		t.Fatalf("svc1 发现应只返 svc1 实例, got %+v", got)
	}
}

func testMultipleInstances(t *testing.T, d registry.Directory) {
	for _, id := range []string{"i1", "i2", "i3"} {
		mustRegister(t, d, ns, sample("svc", id))
	}
	got := discover(t, d, ns, "svc")
	if len(got) != 3 {
		t.Fatalf("期望 3 实例, got %d", len(got))
	}
	for _, id := range []string{"i1", "i2", "i3"} {
		if _, ok := findByID(got, id); !ok {
			t.Errorf("缺实例 %q", id)
		}
	}
}

// testMultipleAgentNames 验证同一 serviceName 下多个 agentName 各自实例聚合发现（§ZK 节点模型
// list(serviceName) 返回该 service 下所有 agentName 的实例）+ agentName 字段显式 round-trip。
func testMultipleAgentNames(t *testing.T, d registry.Directory) {
	a1 := sample("commerce-service", "10.0.0.1:8080")
	a1.AgentName = "research"
	a2 := sample("commerce-service", "10.0.0.2:8080")
	a2.AgentName = "refund"
	mustRegister(t, d, ns, a1)
	mustRegister(t, d, ns, a2)

	got := discover(t, d, ns, "commerce-service")
	if len(got) != 2 {
		t.Fatalf("同 service 下 2 个 agentName 应聚合发现 2 实例, got %d", len(got))
	}
	byAgent := map[string]string{}
	for _, g := range got {
		byAgent[g.AgentName] = g.InstanceID
	}
	if byAgent["research"] != "10.0.0.1:8080" || byAgent["refund"] != "10.0.0.2:8080" {
		t.Fatalf("聚合发现的 agentName→instance 映射错（agentName round-trip 失败）: %+v", byAgent)
	}
}

// testSameInstanceDifferentAgentName 验证 §4.3 重复判定是 4 元组（ns+service+agentName+instanceId）：
// 同 serviceName+同 instanceId 但不同 agentName 可共存；同 4 元组才算重复返 ErrInstanceExists。
// **sanity-revert**：若重复判定漏掉 agentName（只按 service+instanceId），a2 注册会误报 ErrInstanceExists 致本测试 fail。
func testSameInstanceDifferentAgentName(t *testing.T, d registry.Directory) {
	a1 := sample("svc", "10.0.0.1:8080")
	a1.AgentName = "agent-a"
	a2 := sample("svc", "10.0.0.1:8080") // 同 instanceId
	a2.AgentName = "agent-b"             // 不同 agentName
	mustRegister(t, d, ns, a1)
	if _, err := d.Register(context.Background(), ns, a2); err != nil {
		t.Fatalf("同 instanceId 不同 agentName 应可共存（§4.3 4 元组判定）, got %v", err)
	}
	if got := discover(t, d, ns, "svc"); len(got) != 2 {
		t.Fatalf("同 instanceId 不同 agentName 应聚合发现 2 实例, got %d", len(got))
	}
	// 同 4 元组（service+agentName+instanceId）重复 → ErrInstanceExists
	if _, err := d.Register(context.Background(), ns, a1); !errors.Is(err, registry.ErrInstanceExists) {
		t.Fatalf("同 4 元组重复注册应返 ErrInstanceExists, got %v", err)
	}
}

func testDeregister(t *testing.T, d registry.Directory) {
	reg := mustRegister(t, d, ns, sample("svc", "i1"))
	if err := reg.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister 失败: %v", err)
	}
	if got := discover(t, d, ns, "svc"); len(got) != 0 {
		t.Fatalf("反注册后应发现不到, got %d", len(got))
	}
	// 幂等：重复反注册不报错
	if err := reg.Deregister(context.Background()); err != nil {
		t.Errorf("重复 Deregister 应幂等返 nil, got %v", err)
	}
}

func testUpdateStatus(t *testing.T, d registry.Directory) {
	reg := mustRegister(t, d, ns, sample("svc", "i1"))
	if err := reg.UpdateStatus(context.Background(), registry.StatusDraining); err != nil {
		t.Fatalf("UpdateStatus 失败: %v", err)
	}
	got := discover(t, d, ns, "svc")
	// DRAINING 实例仍可被 Discover 返回（§6 末），但状态应已更新。
	g, ok := findByID(got, "i1")
	if !ok {
		t.Fatalf("UpdateStatus 后实例不应消失")
	}
	if g.Status != registry.StatusDraining {
		t.Fatalf("status 应为 DRAINING, got %q", g.Status)
	}
}

func testRejectsInvalid(t *testing.T, d registry.Directory) {
	cases := map[string]registry.ServiceInstance{
		"empty serviceName":  {SchemaVersion: 1, InstanceID: "i", AgentCardURL: "http://x/c"},
		"empty instanceId":   {SchemaVersion: 1, ServiceName: "s", AgentCardURL: "http://x/c"},
		"empty agentCardUrl": {SchemaVersion: 1, ServiceName: "s", InstanceID: "i"},
		"relative cardUrl":   {SchemaVersion: 1, ServiceName: "s", InstanceID: "i", AgentCardURL: "foo/bar"},
		"relative baseUrl":   {SchemaVersion: 1, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://x/c", BaseURL: "foo"},
		"unknown transport":  {SchemaVersion: 1, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://x/c", Transport: "SOAP"},
		"unknown status":     {SchemaVersion: 1, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://x/c", Status: "UP"},
		"negative weight":    {SchemaVersion: 1, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://x/c", Weight: registry.WeightPtr(-1)},
		"bad schemaVersion":  {SchemaVersion: 2, ServiceName: "s", InstanceID: "i", AgentCardURL: "http://x/c"},
	}
	for name, inst := range cases {
		if _, err := d.Register(context.Background(), ns, inst); err == nil {
			t.Errorf("Register 应拒绝 [%s]，却成功了", name)
		}
	}
}

// testMetadataSnapshotIsolation 验证 Discover 返回 deep-copy 快照（W2）——注册方改原 map /
// 发现方改返回 map 都不污染存储。**sanity-revert**：去掉 InMemory 的 cloneMetadata（任一侧）
// 本测试 fail。
func testMetadataSnapshotIsolation(t *testing.T, d registry.Directory) {
	in := sample("svc", "i1") // Metadata{zone:az1, language:go}
	mustRegister(t, d, ns, in)

	// 注册方改自己持有的原始 map —— 不应穿透污染存储。
	in.Metadata["zone"] = "MUTATED-BY-REGISTRANT"
	got := discover(t, d, ns, "svc")
	if len(got) != 1 {
		t.Fatalf("期望 1 实例, got %d", len(got))
	}
	if got[0].Metadata["zone"] != "az1" {
		t.Fatalf("注册方改原 map 污染了存储（Register 侧未 clone）: zone=%q", got[0].Metadata["zone"])
	}

	// 发现方改返回的 map —— 不应穿透污染存储。
	got[0].Metadata["zone"] = "MUTATED-BY-DISCOVERER"
	got2 := discover(t, d, ns, "svc")
	if got2[0].Metadata["zone"] != "az1" {
		t.Fatalf("发现方改返回 map 污染了存储（Discover 侧未 clone）: zone=%q", got2[0].Metadata["zone"])
	}
}

// testDuplicateRegister 验证 §4.3：目标实例已被持有 → 注册失败（errors.Is ErrInstanceExists）；
// Deregister 后可重新注册（重注册流程 = 先关旧句柄）。
func testDuplicateRegister(t *testing.T, d registry.Directory) {
	reg := mustRegister(t, d, ns, sample("svc", "i1"))
	if _, err := d.Register(context.Background(), ns, sample("svc", "i1")); !errors.Is(err, registry.ErrInstanceExists) {
		t.Fatalf("重复注册应返 ErrInstanceExists（§4.3 已被持有不得覆盖）, got %v", err)
	}
	if err := reg.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, err := d.Register(context.Background(), ns, sample("svc", "i1")); err != nil {
		t.Fatalf("Deregister 后重新注册应成功, got %v", err)
	}
}

// testInvalidStatus 验证 UpdateStatus 写侧校验（非法 status 写进目录会让节点被所有发现端
// 按 malformed 跳过——实例静默消失，必须在写侧拒绝）。
func testInvalidStatus(t *testing.T, d registry.Directory) {
	reg := mustRegister(t, d, ns, sample("svc", "i1"))
	if err := reg.UpdateStatus(context.Background(), registry.Status("bogus")); err == nil {
		t.Fatal("UpdateStatus 应拒绝未知 status")
	}
	got := discover(t, d, ns, "svc")
	g, okFound := findByID(got, "i1")
	if !okFound || g.Status != registry.StatusServing {
		t.Fatalf("非法 UpdateStatus 被拒后实例应保持原状态 SERVING, got %+v", got)
	}
}

func testDefaultNamespace(t *testing.T, d registry.Directory) {
	// 空 namespace 注册 → "default" 发现可见（归一一致）。
	mustRegister(t, d, "", sample("svc", "i1"))
	if got := discover(t, d, registry.DefaultNamespace, "svc"); len(got) != 1 {
		t.Fatalf("空 ns 注册应在 default ns 可见, got %d", len(got))
	}
	// 反向：default 注册 → 空 ns 发现可见。
	mustRegister(t, d, registry.DefaultNamespace, sample("svc", "i2"))
	if got := discover(t, d, "", "svc"); len(got) != 2 {
		t.Fatalf("default ns 实例应在空 ns 查询可见, got %d", len(got))
	}
}
