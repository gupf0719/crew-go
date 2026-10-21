package a2a

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"crew-go/a2a/registry"
	"crew-go/agent_v2"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
)

// ErrNoEligibleInstance 表示服务发现到了但**无可路由实例**（无 SERVING+weight>0）——与
// "全部实例 card 拉取失败"区分：业务 errors.Is 命中本哨兵时走"等注册/告警"，否则走
// "瞬态重试/降级"。
var ErrNoEligibleInstance = errors.New("a2a: no eligible instance (SERVING with weight>0)")

// NewRemoteAgentFromService 通过服务目录发现 serviceName 的一个实例并建 RemoteAgent。
//
// 流程（发现层在调用层之上）：Directory.Discover → Eligible 过滤（SERVING + weight>0，§6）
// → selector 选实例 → 拉其 AgentCard 建 client（= NewRemoteAgent）。**card 拉取失败按
// §8 failover 换下一实例重试**；一旦换成功返回绑定该实例的 RemoteAgent（之后 message/send
// 发出就不在此层重试——避免远程已建 task 的重复执行）。
//
// selector 为 nil 时用随机（单次调用即分散负载）。要跨多次调用做**轮询**负载均衡，传一个
// **复用的** registry.NewRoundRobinSelector()（轮询游标需在调用间持久）。opts 透传 NewRemoteAgent
// （WithHumanGate / WithClientOptions 等）。
func NewRemoteAgentFromService(ctx context.Context, dir registry.Directory, namespace, serviceName string, sel registry.Selector, opts ...RemoteOption) (*RemoteAgent, error) {
	if isNilIface(dir) {
		return nil, fmt.Errorf("a2a: NewRemoteAgentFromService requires a non-nil Directory")
	}
	instances, err := dir.Discover(ctx, namespace, serviceName)
	if err != nil {
		return nil, fmt.Errorf("a2a: discover service %q: %w", serviceName, err)
	}
	// agentName 过滤（WithDiscoveryAgentName）：同一 serviceName 注册多个 Agent Card 时精确路由；
	// 不传则用 Discover 聚合的所有 agentName 实例。opts 在此解析一遍提取过滤条件（option 是纯函数，
	// 下面 NewRemoteAgentFromCard 再解析一遍建 agent 无副作用）。
	var dc remoteConfig
	for _, o := range opts {
		o(&dc)
	}
	if dc.discoverAgentName != "" {
		instances = filterByAgentName(instances, dc.discoverAgentName)
	}
	eligible := registry.Eligible(instances)
	if len(eligible) == 0 {
		if dc.discoverAgentName != "" {
			return nil, fmt.Errorf("%w: service %q agent %q in namespace %q", ErrNoEligibleInstance, serviceName, dc.discoverAgentName, registry.NormalizeNamespace(namespace))
		}
		return nil, fmt.Errorf("%w: service %q in namespace %q", ErrNoEligibleInstance, serviceName, registry.NormalizeNamespace(namespace))
	}
	if isNilIface(sel) {
		sel = registry.NewRandomSelector()
	}
	tried := make(map[string]struct{}, len(eligible))
	var lastErr error
	for {
		inst, ok := sel.Select(eligible, tried)
		if !ok {
			break // 全部 eligible 实例都试过了
		}
		// 去重身份用 InstanceIdentity（agentName+instanceId）而非裸 instanceId——4 层后同 instanceId
		// 不同 agentName 是不同实例（同端口多 Agent Card），裸 id 去重会漏试 sibling agent。
		identity := registry.InstanceIdentity(inst)
		if _, seen := tried[identity]; seen {
			// 防业务自定义 Selector 违约（§6 不得重复选已试过的）——否则 tried 不再增长死循环
			break
		}
		tried[identity] = struct{}{}
		card, cardErr := resolveCardFromURL(ctx, inst.AgentCardURL)
		if cardErr != nil {
			lastErr = fmt.Errorf("instance %q card %s: %w", inst.InstanceID, inst.AgentCardURL, cardErr)
			continue // §8: card 拉取失败可 failover 换实例
		}
		ra, raErr := NewRemoteAgentFromCard(ctx, card, opts...)
		if raErr != nil {
			lastErr = fmt.Errorf("instance %q (%s): %w", inst.InstanceID, inst.AgentCardURL, raErr)
			continue
		}
		return ra, nil
	}
	return nil, fmt.Errorf("a2a: all %d eligible instance(s) failed for service %q: %w", len(eligible), serviceName, lastErr)
}

// filterByAgentName 过滤出 AgentName 匹配的实例（WithDiscoveryAgentName 精确路由用）。
func filterByAgentName(instances []registry.ServiceInstance, agentName string) []registry.ServiceInstance {
	out := make([]registry.ServiceInstance, 0, len(instances))
	for _, in := range instances {
		if in.AgentName == agentName {
			out = append(out, in)
		}
	}
	return out
}

// resolveCardFromURL 按**完整** agentCardUrl（spec §3 通常是 .../.well-known/agent-card.json）
// 解析 AgentCard。Resolver 默认对 base 追加 well-known 路径，故拆出 base + path 用 WithPath
// 精确命中完整 URL；若 URL 只有 host 无路径（退化成 base 形式）则走默认 well-known 路径。
//
// **鉴权不要放 card URL**（W5）：保留 userinfo（basic-auth）但 **query string 会被丢弃**——
// 这是刻意的：A2A 鉴权模型是 AgentCard.securitySchemes + client 注 header（crew-go 走
// WithClientOptions 的 a2aclient.AuthInterceptor / 自定义 RoundTripper），card URL 不该带
// query-token（spec §9 也禁目录存 token）。要给拉 card 加鉴权请用 WithClientOptions。
func resolveCardFromURL(ctx context.Context, cardURL string) (*a2atypes.AgentCard, error) {
	u, err := url.Parse(cardURL)
	if err != nil {
		return nil, fmt.Errorf("parse card url: %w", err)
	}
	if !u.IsAbs() {
		return nil, fmt.Errorf("agentCardUrl %q is not absolute", cardURL)
	}
	base := u.Scheme + "://"
	if u.User != nil {
		base += u.User.String() + "@" // 保留 basic-auth userinfo
	}
	base += u.Host
	if u.Path == "" || u.Path == "/" {
		return agentcard.DefaultResolver.Resolve(ctx, base)
	}
	return agentcard.DefaultResolver.Resolve(ctx, base, agentcard.WithPath(u.Path))
}

// NewRemoteAgentToolFromService 是 NewRemoteAgentFromService 的 tool 形态便利包装
// （= AgentAsTool(发现到的 RemoteAgent)），注册进 host BasicAgent toolkit 让 LLM 调度。
func NewRemoteAgentToolFromService(ctx context.Context, dir registry.Directory, namespace, serviceName string, sel registry.Selector, opts ...RemoteOption) (tool.Tool, error) {
	ra, err := NewRemoteAgentFromService(ctx, dir, namespace, serviceName, sel, opts...)
	if err != nil {
		return nil, err
	}
	return agent_v2.AgentAsTool(ra), nil
}
