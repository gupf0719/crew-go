// Package registry 实现 A2A 多语言注册发现协议的服务目录抽象——保存"如何找到一个
// 远程 A2A agent 服务"的最小指针信息（serviceName → 实例 → agentCardUrl），不碰 A2A
// 任务协议本身（拉 card + message/send/stream 由上层 a2a.RemoteAgent 完成）。
//
// 核心是 Directory 接口（注册 + 发现）+ ServiceInstance 模型 + Selector 选择策略。
// InMemory 是进程内实现（测试 / 单进程多 agent 场景）；跨进程跨语言互通需 ZooKeeper
// Binding v1（a2a/zk 子模块），业务自实现其它 backend 时跑 registry/conformance 套验收。
//
// 协议规范见 doc/description/a2a-registry-discovery-spec.md（§N 引用均指该文档）。
package registry

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// CurrentSchemaVersion 是服务目录 JSON schema 版本（§3，当前固定 1）。
// 注意：这是目录 schema 编号，与 ServiceInstance.Version（服务/agent 自身版本）不同。
const CurrentSchemaVersion = 1

// DefaultNamespace 是未显式传 namespace 时的默认值（§4.1）——所有语言 SDK 必须一致，
// 否则同一服务被写进不同目录互相发现不到。
const DefaultNamespace = "default"

// Transport 是远端 A2A 调用传输方式（§3.2）。目录里只作路由 hint，最终 transport 以
// Agent Card 的 preferredTransport / supportedInterfaces 为准（§7）。
type Transport string

const (
	TransportJSONRPC  Transport = "JSONRPC"
	TransportGRPC     Transport = "GRPC"
	TransportHTTPJSON Transport = "HTTP+JSON"
	// TransportCUSTOM 是 crew-go 扩展值：目录只能表达"自定义 transport"而无法写具体
	// vendor binding 时用。
	TransportCUSTOM Transport = "CUSTOM"
)

// DefaultTransport 是 transport 缺省值（§3.1）。
const DefaultTransport = TransportJSONRPC

func (t Transport) known() bool {
	switch t {
	case TransportJSONRPC, TransportGRPC, TransportHTTPJSON, TransportCUSTOM:
		return true
	}
	return false
}

// Status 是实例服务状态（§3.2）。
type Status string

const (
	// StatusServing 可接收新请求。
	StatusServing Status = "SERVING"
	// StatusDraining 实例正在下线 / 排空，不参与默认路由。
	StatusDraining Status = "DRAINING"
	// StatusOutOfService 实例不可服务，不参与默认路由。
	StatusOutOfService Status = "OUT_OF_SERVICE"
)

// DefaultStatus 是 status 缺省值（§3.1）。
const DefaultStatus = StatusServing

// Known 校验 status 是否协议已知枚举值（§3.2）——backend（zk 等）写侧校验用，
// 与 InMemory 对称（写入非法 status 会让节点被所有发现端按 malformed 跳过，实例静默消失）。
func (s Status) Known() bool {
	switch s {
	case StatusServing, StatusDraining, StatusOutOfService:
		return true
	}
	return false
}

// DefaultWeight 是 weight 缺省值（§3.1）。
const DefaultWeight = 100

// WeightPtr 构造 ServiceInstance.Weight（*int）的便利 helper——要显式权重（含 0=不参与选择）
// 时用，如 registry.WeightPtr(0)；留 Weight 为 nil 则注册时默认补 100。
func WeightPtr(n int) *int { return &n }

// ServiceInstance 是服务目录里一条 A2A agent 实例记录（§3 通用服务实例模型）。
// JSON 字段名严格对齐协议规范——跨语言（Java/Python/Go）必须用同一 payload 格式。
type ServiceInstance struct {
	// SchemaVersion 当前固定 1（§3.1）。发现端遇未知版本跳过该节点。
	SchemaVersion int `json:"schemaVersion"`
	// ServiceName 逻辑服务名（必填），调用方按该名发现实例。
	ServiceName string `json:"serviceName"`
	// AgentName 区分同一 serviceName 下的多个 Agent Card（§ZK 节点模型）。未显式设置时
	// WithDefaults 补 = ServiceName（单 agent 服务把 agentName 与 serviceName 用同一值）。
	// 4 层路径 {agentName} 段必须 == 本字段。项目统一 4 层，不读旧 3 层节点。
	AgentName string `json:"agentName"`
	// InstanceID 同一 serviceName 下唯一实例 ID（必填），如 "10.0.0.12:8080"。
	InstanceID string `json:"instanceId"`
	// BaseURL 服务基础地址（可选），提供时必须绝对 URI。
	BaseURL string `json:"baseUrl,omitempty"`
	// AgentCardURL Agent Card 绝对 URI（必填）——发现端用它拉 Card。
	AgentCardURL string `json:"agentCardUrl"`
	// Transport A2A transport hint（可选，默认 JSONRPC）。
	Transport Transport `json:"transport,omitempty"`
	// Version 服务 / agent 自身版本（可选）。
	Version string `json:"version,omitempty"`
	// Status 实例服务状态（可选，默认 SERVING）。
	Status Status `json:"status,omitempty"`
	// Weight 非负路由权重（可选）。nil = 未给 → 默认 100；显式 0 = 不参与默认选择（§3.1，
	// 与 Status 正交：SERVING 但 weight=0 = 健康但临时零权重）。用 *int 而非 int 是为区分
	// "未给"（→默认 100）与"显式 0"（→不路由）——int 零值无法区分，会让跨语言对端的
	// weight=0 被 Go 误当默认 100 路由（违反 §3.1）。nil 经 omitempty 不序列化（对端补默认
	// 100）；&0 序列化为 "weight":0（对端读到显式 0）。
	Weight *int `json:"weight,omitempty"`
	// Metadata 字符串到字符串的非敏感元数据（可选）。禁存密钥 / 请求正文 / 运行态（§9）。
	Metadata map[string]string `json:"metadata,omitempty"`
}

// WithDefaults 返回填充了缺省值的副本（§3.1 / §10：缺省可选字段必须用本文定义的默认值）。
// 注册前 / 发现读出后都应规整，保证跨语言行为一致。
func (s ServiceInstance) WithDefaults() ServiceInstance {
	if s.SchemaVersion == 0 {
		s.SchemaVersion = CurrentSchemaVersion
	}
	// agentName 缺省 = serviceName（§ZK 节点模型：未显式设置时默认 = serviceName，单 agent
	// 服务把 agentName 与 serviceName 用同一值）。
	if s.AgentName == "" {
		s.AgentName = s.ServiceName
	}
	if s.Transport == "" {
		s.Transport = DefaultTransport
	}
	if s.Status == "" {
		s.Status = DefaultStatus
	}
	// weight 缺省 100：仅当真未给（nil）时填；显式 &0（不参与选择）原样保留（§3.1）。
	if s.Weight == nil {
		w := DefaultWeight
		s.Weight = &w
	}
	return s
}

// Validate 是注册端写入前的校验（§3.3 注册端规则）。
func (s ServiceInstance) Validate() error {
	if s.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("registry: schemaVersion must be %d, got %d", CurrentSchemaVersion, s.SchemaVersion)
	}
	if strings.TrimSpace(s.ServiceName) == "" {
		return fmt.Errorf("registry: serviceName must be non-empty")
	}
	if strings.TrimSpace(s.InstanceID) == "" {
		return fmt.Errorf("registry: instanceId must be non-empty")
	}
	// agentName WithDefaults 后应非空（缺省补 = serviceName）；仍空说明 serviceName 也空
	//（上面已拦），此处作为 4 层路径 {agentName} 段的不变量声明。
	if strings.TrimSpace(s.AgentName) == "" {
		return fmt.Errorf("registry: agentName must be non-empty")
	}
	if err := requireAbsURI("agentCardUrl", s.AgentCardURL); err != nil {
		return err
	}
	if s.BaseURL != "" {
		if err := requireAbsURI("baseUrl", s.BaseURL); err != nil {
			return err
		}
	}
	if !s.Transport.known() {
		return fmt.Errorf("registry: unknown transport %q", s.Transport)
	}
	if !s.Status.Known() {
		return fmt.Errorf("registry: unknown status %q", s.Status)
	}
	if s.Weight != nil && *s.Weight < 0 {
		return fmt.Errorf("registry: weight must be >= 0, got %d", *s.Weight)
	}
	for k := range s.Metadata {
		if k == "" {
			return fmt.Errorf("registry: metadata key must be non-empty")
		}
		// value 是 Go string，天然非 null；空串允许（协议只禁 null）。
	}
	return nil
}

// requireAbsURI 校验字符串是非空绝对 URI。
func requireAbsURI(field, raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("registry: %s must be non-empty", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("registry: %s %q is not a valid URI: %w", field, raw, err)
	}
	if !u.IsAbs() {
		return fmt.Errorf("registry: %s %q must be an absolute URI", field, raw)
	}
	return nil
}

// ParseDiscovered 是发现端读出 payload 后的解析+校验（§3.3 发现端规则 + §10）：先填缺省
// 值再校验，**malformed 返 error 让调用方跳过该节点**（不污染同服务其它健康实例）。
// 用于 Directory 实现的 Discover 内部逐节点解析。
func ParseDiscovered(s ServiceInstance) (ServiceInstance, error) {
	// W6 fix: schemaVersion 是必填字段（§3.1）。缺失(0) / 未知值都按 §3.3"缺失必填字段 /
	// 未知 schemaVersion 跳过该节点"处理——**不补默认**（必填字段补默认会架空 §10"写入端
	// 必须写 schemaVersion:1"，让没写的 malformed 节点被当合法）。写侧 Register 经 WithDefaults
	// 给 Go caller 补 1 是另一回事（框架代写，满足 §10）。
	if s.SchemaVersion != CurrentSchemaVersion {
		return ServiceInstance{}, fmt.Errorf("registry: missing or unknown schemaVersion %d (skip node)", s.SchemaVersion)
	}
	out := s.WithDefaults()
	if err := out.Validate(); err != nil {
		return ServiceInstance{}, err
	}
	return out, nil
}

// ParseDiscoveredJSON 是 binding（ZK / Nacos / etcd 等）从 wire 读出 raw JSON 节点后的**统一
// 安全解析入口**：json.Unmarshal + ParseDiscovered（缺省+校验）。任何 malformed——JSON 语法错 /
// metadata 含非字符串 value（Go map[string]string 解析失败，§3.3"含非字符串 value 跳过"）/
// 缺失必填 / 未知枚举——都返 error 让 binding **跳过该节点 continue**。
//
// binding 必须对单节点解析失败 continue 而非上抛整体 discovery 错（§3.3 末："单个 malformed
// node 不得导致同一 serviceName 下其它健康实例发现失败"）。统一走本 helper 避免各 binding
// 自己 Unmarshal 时踩"一个坏节点炸全服务"的坑（W4）。
func ParseDiscoveredJSON(data []byte) (ServiceInstance, error) {
	var s ServiceInstance
	if err := json.Unmarshal(data, &s); err != nil {
		return ServiceInstance{}, fmt.Errorf("registry: unmarshal instance payload: %w", err)
	}
	return ParseDiscovered(s)
}

// NormalizeNamespace 把空 namespace 归一到 DefaultNamespace（§4.1 / §10）。
func NormalizeNamespace(ns string) string {
	if strings.TrimSpace(ns) == "" {
		return DefaultNamespace
	}
	return ns
}

// InstanceIdentity 返回实例在同一 serviceName 下的唯一去重身份——agentName + instanceId
// （§4.3 4 元组的后两段，ns+serviceName 由发现路径确定）。**Selector 的 tried 集合 / failover
// 去重必须用本身份而非裸 instanceId**：4 层后同 instanceId 不同 agentName 是不同实例（同端口多
// Agent Card 场景，§ZK 节点模型），裸 instanceId 去重会让 sibling agent 被误判已试而漏选。
// 与 InMemory 内部 bucket key 同一身份算法（instKey）。
func InstanceIdentity(s ServiceInstance) string {
	return instKey(s.AgentName, s.InstanceID)
}
