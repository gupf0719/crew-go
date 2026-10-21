package registry

import (
	"math/rand"
	"sync"
)

// Eligible 过滤出可参与默认路由的实例（§6 默认选择规则）：status=SERVING 且 weight>0。
// DRAINING / OUT_OF_SERVICE / weight=0 排除（它们可被 Discover 返回供观测，但默认不路由）。
func Eligible(instances []ServiceInstance) []ServiceInstance {
	out := make([]ServiceInstance, 0, len(instances))
	for _, in := range instances {
		if in.Status == StatusServing && effectiveWeight(in) > 0 {
			out = append(out, in)
		}
	}
	return out
}

// effectiveWeight 返回参与选择的权重：Weight 为 nil（未给）按默认 100，否则用显式值
// （含显式 0 = 不参与选择）。
func effectiveWeight(in ServiceInstance) int {
	if in.Weight == nil {
		return DefaultWeight
	}
	return *in.Weight
}

// Selector 从一组实例里挑一个。instances 应是 Eligible 过滤后的列表；tried 是本次 failover
// 已试过的实例身份集合（key = InstanceIdentity(inst) = agentName+instanceId）——选择器不得重复
// 选已试过的（§6）。挑不出（全试过 / 空）返 ok=false。
//
// **去重身份用 InstanceIdentity 而非裸 instanceId**：4 层后同 instanceId 不同 agentName 是不同
// 实例（同端口多 Agent Card），裸 instanceId 去重会让 sibling agent 被误判已试漏选。业务自定义
// Selector 比对 tried 时也必须用 InstanceIdentity(inst) 作 key。
type Selector interface {
	Select(instances []ServiceInstance, tried map[string]struct{}) (inst ServiceInstance, ok bool)
}

// untried 过滤掉 tried 里已试过的实例（按 InstanceIdentity 去重）。
func untried(instances []ServiceInstance, tried map[string]struct{}) []ServiceInstance {
	if len(tried) == 0 {
		return instances
	}
	out := make([]ServiceInstance, 0, len(instances))
	for _, in := range instances {
		if _, ok := tried[InstanceIdentity(in)]; !ok {
			out = append(out, in)
		}
	}
	return out
}

// roundRobinSelector 轮询（默认策略）。跨调用持有游标，并发安全。
type roundRobinSelector struct {
	mu  sync.Mutex
	idx int
}

// NewRoundRobinSelector 返回轮询选择器（§6 列举策略之一；发现 glue 的 nil-selector 默认是随机，见 a2a.NewRemoteAgentFromService）。
func NewRoundRobinSelector() Selector { return &roundRobinSelector{} }

func (r *roundRobinSelector) Select(instances []ServiceInstance, tried map[string]struct{}) (ServiceInstance, bool) {
	avail := untried(instances, tried)
	if len(avail) == 0 {
		return ServiceInstance{}, false
	}
	r.mu.Lock()
	i := r.idx % len(avail)
	r.idx++
	r.mu.Unlock()
	return avail[i], true
}

// randomSelector 等概率随机。
type randomSelector struct{}

// NewRandomSelector 返回随机选择器。
func NewRandomSelector() Selector { return randomSelector{} }

func (randomSelector) Select(instances []ServiceInstance, tried map[string]struct{}) (ServiceInstance, bool) {
	avail := untried(instances, tried)
	if len(avail) == 0 {
		return ServiceInstance{}, false
	}
	return avail[rand.Intn(len(avail))], true //nolint:gosec // 负载均衡选实例非加密用途
}

// weightedSelector 按 weight 加权随机。
type weightedSelector struct{}

// NewWeightedSelector 返回加权随机选择器（按 ServiceInstance.Weight）。
func NewWeightedSelector() Selector { return weightedSelector{} }

func (weightedSelector) Select(instances []ServiceInstance, tried map[string]struct{}) (ServiceInstance, bool) {
	avail := untried(instances, tried)
	if len(avail) == 0 {
		return ServiceInstance{}, false
	}
	total := 0
	for _, in := range avail {
		total += effectiveWeight(in)
	}
	if total <= 0 {
		// Eligible 已保证 weight>0，理论到不了；兜底返第一个。
		return avail[0], true
	}
	r := rand.Intn(total) //nolint:gosec // 负载均衡非加密用途
	for _, in := range avail {
		r -= effectiveWeight(in)
		if r < 0 {
			return in, true
		}
	}
	return avail[len(avail)-1], true
}
