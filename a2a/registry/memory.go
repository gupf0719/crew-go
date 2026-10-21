package registry

import (
	"context"
	"fmt"
	"maps"
	"sync"
)

// InMemory 是进程内 Directory 实现——测试 / 单进程多 agent 场景。跨进程跨语言互通需
// ZooKeeper Binding（a2a/zk）。"ephemeral" 语义在进程内退化为"Deregister 或进程退出即消失"。
type InMemory struct {
	mu        sync.RWMutex
	instances map[string]map[string]ServiceInstance // svcKey(ns,svc) → instKey(agentName,instanceId) → instance
}

var _ Directory = (*InMemory)(nil)

// NewInMemory 构造空的进程内目录。
func NewInMemory() *InMemory {
	return &InMemory{instances: make(map[string]map[string]ServiceInstance)}
}

// cloneShared 深拷贝 ServiceInstance 的引用/指针字段（Metadata map + Weight *int）——
// InMemory 存的是 Go struct，引用字段不拷会在注册方 / 存储 / 发现方间别名共享：改
// Metadata（W2）或 *Weight 都会污染存储 + data race。
func cloneShared(inst ServiceInstance) ServiceInstance {
	if inst.Metadata != nil {
		inst.Metadata = maps.Clone(inst.Metadata)
	}
	if inst.Weight != nil {
		w := *inst.Weight
		inst.Weight = &w
	}
	return inst
}

// svcKey 用 NUL 分隔 namespace 与 serviceName 避免拼接歧义。
func svcKey(namespace, serviceName string) string {
	return NormalizeNamespace(namespace) + "\x00" + serviceName
}

// instKey 是 bucket 内一条实例的唯一 key——agentName + instanceId（§4.3 重复判定是
// namespace+serviceName+agentName+instanceId 4 元组；svcKey 已含 ns+svc，此处补后两段）。
// 同 serviceName 下不同 agentName 的相同 instanceId 因此不互踩（对齐 zk 路径含 agentName 段）。
func instKey(agentName, instanceID string) string {
	return agentName + "\x00" + instanceID
}

// Register 见 Directory.Register。
func (m *InMemory) Register(_ context.Context, namespace string, inst ServiceInstance) (Registration, error) {
	inst = inst.WithDefaults()
	if err := inst.Validate(); err != nil {
		return nil, err
	}
	// W2 fix: clone 引用字段（Metadata/Weight）切断与注册方的别名——否则注册方后续改
	// 自己的 map / *Weight 会反向污染存储（struct 值拷贝只拷 header/指针）。
	inst = cloneShared(inst)
	key := svcKey(namespace, inst.ServiceName)
	ik := instKey(inst.AgentName, inst.InstanceID)
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.instances[key]
	if bucket == nil {
		bucket = make(map[string]ServiceInstance)
		m.instances[key] = bucket
	}
	if _, exists := bucket[ik]; exists {
		// §4.3：目标实例（serviceName+agentName+instanceId 4 元组）已被持有 → 注册失败
		//（重注册流程 = 先 Deregister 旧句柄）。静默覆盖会让两个注册方互踩（与 zk ErrNodeExists 对齐）。
		return nil, fmt.Errorf("%w: %s/%s/%s", ErrInstanceExists, inst.ServiceName, inst.AgentName, inst.InstanceID)
	}
	bucket[ik] = inst
	return &inMemoryRegistration{dir: m, key: key, agentName: inst.AgentName, instanceID: inst.InstanceID}, nil
}

// Discover 见 Directory.Discover。
func (m *InMemory) Discover(_ context.Context, namespace, serviceName string) ([]ServiceInstance, error) {
	key := svcKey(namespace, serviceName)
	m.mu.RLock()
	defer m.mu.RUnlock()
	bucket := m.instances[key]
	if len(bucket) == 0 {
		return nil, nil // 服务路径不存在 → 空列表 + nil error（§6.1）
	}
	out := make([]ServiceInstance, 0, len(bucket))
	for _, inst := range bucket {
		// 走读侧解析（缺省+校验）；InMemory 存的本已合规，此处对齐协议读侧路径，
		// malformed 跳过（§6.6）。
		parsed, err := ParseDiscovered(inst)
		if err != nil {
			continue
		}
		// W2 fix: clone 引用字段切断与存储的别名——否则发现方改返回实例的 Metadata/*Weight
		// 会污染存储 + 与并发 Discover/Register race（Directory godoc 承诺返回"快照"）。
		out = append(out, cloneShared(parsed))
	}
	return out, nil
}

// Close 见 Directory.Close（InMemory no-op）。
func (m *InMemory) Close() error { return nil }

// inMemoryRegistration 是 InMemory 的注册句柄。
type inMemoryRegistration struct {
	dir        *InMemory
	key        string
	agentName  string // §4.3 重复判定含 agentName（与 instanceID 一起算 bucket instKey）
	instanceID string
}

var _ Registration = (*inMemoryRegistration)(nil)

// UpdateStatus 见 Registration.UpdateStatus。
func (r *inMemoryRegistration) UpdateStatus(_ context.Context, status Status) error {
	if !status.Known() {
		return fmt.Errorf("registry: unknown status %q", status)
	}
	r.dir.mu.Lock()
	defer r.dir.mu.Unlock()
	bucket := r.dir.instances[r.key]
	if bucket == nil {
		return fmt.Errorf("registry: instance %q gone (cannot update status)", r.instanceID)
	}
	ik := instKey(r.agentName, r.instanceID)
	inst, ok := bucket[ik]
	if !ok {
		return fmt.Errorf("registry: instance %q gone (cannot update status)", r.instanceID)
	}
	inst.Status = status
	bucket[ik] = inst
	return nil
}

// Deregister 见 Registration.Deregister（幂等）。
func (r *inMemoryRegistration) Deregister(_ context.Context) error {
	r.dir.mu.Lock()
	defer r.dir.mu.Unlock()
	bucket := r.dir.instances[r.key]
	if bucket != nil {
		delete(bucket, instKey(r.agentName, r.instanceID))
		if len(bucket) == 0 {
			delete(r.dir.instances, r.key)
		}
	}
	return nil
}
