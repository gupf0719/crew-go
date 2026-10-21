// Package zk 是 A2A 服务注册发现协议的 ZooKeeper Binding v1 实现（规范 §4）——crew-go 跨
// 进程跨语言（Java/Python/Go）互通的服务目录后端。实现 registry.Directory，跑
// registry/conformance 验收语义合规。
//
// 独立子模块隔离 go-zookeeper 依赖：core（crew-go 主模块）不引 ZK，业务按需引本模块。
//
// **协议要点**：实例节点是 ephemeral（注册进程退出 / session 丢失即消失，§4.2）；路径
// {root}/{ns}/{svc}/{base64url(instanceId)}（§4.1）；payload 是 ServiceInstance JSON（§3）；
// 发现端校验 path↔payload 一致性 + 跳过 malformed（§6）。
package zk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"crew-go/a2a/registry"

	gozk "github.com/go-zookeeper/zk"
)

// Directory 是 ZooKeeper Binding v1 的 registry.Directory 实现。
type Directory struct {
	conn *gozk.Conn
	root string // ZK 根路径，默认 DefaultRootPath；测试经 WithRootPath 隔离
}

var _ registry.Directory = (*Directory)(nil)

// Option 配置 Directory。
type Option func(*Directory)

// WithRootPath 覆盖 ZK 根路径。**生产不要用**——协议 §4.1 规定根为 /crew/a2a，改了
// 跨语言 / 跨实现就发现不到。仅供测试隔离（每次用独立 root 子树避免 conformance 多 subtest 撞）。
func WithRootPath(root string) Option {
	return func(d *Directory) { d.root = root }
}

// New 连接 ZooKeeper 集群返回 Directory。servers 如 []string{"127.0.0.1:2181"}。
// Directory.Close 会关闭此连接。
func New(servers []string, sessionTimeout time.Duration, opts ...Option) (*Directory, error) {
	conn, _, err := gozk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("zk: connect %v: %w", servers, err)
	}
	d := &Directory{conn: conn, root: DefaultRootPath}
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// Register 见 registry.Directory.Register（§4.2/§4.3/§5）。
func (d *Directory) Register(_ context.Context, namespace string, inst registry.ServiceInstance) (registry.Registration, error) {
	inst = inst.WithDefaults()
	if err := inst.Validate(); err != nil {
		return nil, err
	}
	ns := registry.NormalizeNamespace(namespace)
	if err := validateSegment("namespace", ns); err != nil {
		return nil, err
	}
	if err := validateSegment("serviceName", inst.ServiceName); err != nil {
		return nil, err
	}
	if err := validateSegment("agentName", inst.AgentName); err != nil {
		return nil, err
	}

	data, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("zk: marshal instance: %w", err)
	}
	if err := d.ensureParents(ns, inst.ServiceName, inst.AgentName); err != nil {
		return nil, err
	}

	path := instancePath(d.root, ns, inst.ServiceName, inst.AgentName, inst.InstanceID)
	_, err = d.conn.Create(path, data, gozk.FlagEphemeral, gozk.WorldACL(gozk.PermAll))
	if err == gozk.ErrNodeExists {
		// §4.3：目标实例路径已被其它进程 / 注册器持有 → 不删，返注册失败。
		// %w 包 registry.ErrInstanceExists 让业务 errors.Is 分流（conformance 校验本语义）。
		return nil, fmt.Errorf("%w: zk node %s held by another registrant", registry.ErrInstanceExists, path)
	}
	if err != nil {
		return nil, fmt.Errorf("zk: create %s: %w", path, err)
	}
	return &zkRegistration{conn: d.conn, path: path, inst: inst}, nil
}

// Discover 见 registry.Directory.Discover（§6 + §ZK 节点模型，4 层）。
//
// 4 层：service → {agentName} 中间层 → {encodedInstanceId} 实例叶子。遍历 service 下每个 agentName
// 子树收集实例，逐叶子校验 path↔payload 一致（serviceName + agentName==中间层节点名 + 叶子名==
// encode(instanceId)），任一不符按 malformed 跳过。返回该 serviceName 下**所有 agentName** 的实例
// （聚合；调用方按 serviceName 找服务，agentName 过滤交给上层 selector / NewRemoteAgentFromService）。
//
// 只认 4 层：service 直挂的旧 3 层实例叶子不再被发现（项目统一 4 层）；单 agent 服务注册时 agentName
// 缺省 = serviceName，路径 service/{=serviceName}/{instanceId} 仍是 4 层。
func (d *Directory) Discover(ctx context.Context, namespace, serviceName string) ([]registry.ServiceInstance, error) {
	ns := registry.NormalizeNamespace(namespace)
	svcPath := servicePath(d.root, ns, serviceName)
	children, _, err := d.conn.Children(svcPath)
	if err == gozk.ErrNoNode {
		return make([]registry.ServiceInstance, 0), nil // §6.1 服务路径不存在 → 空列表
	}
	if err != nil {
		return nil, fmt.Errorf("zk: list children %s: %w", svcPath, err) // §6.7 顶层整体读取失败
	}
	out := make([]registry.ServiceInstance, 0, len(children))
	for _, agentName := range children {
		// service 的每个子节点都是 agentName 中间层 → 下挖一层收其下实例叶子。
		d.collectAgentInstances(ctx, svcPath+"/"+agentName, serviceName, agentName, &out)
	}
	return out, nil
}

// collectAgentInstances 收集 agentNodePath（{service}/{agentName} 中间层）下的实例叶子（§ZK 节点
// 模型 4 层）。4 层一致性：payload.serviceName==传入逻辑名 + payload.agentName==中间层节点名 +
// 叶子名==encode(instanceId)，任一不符按 malformed 跳过（§6.6）。中间层读取失败 / 列举后消失
// best-effort 跳过（不炸整服务，§3.3 末）。
func (d *Directory) collectAgentInstances(ctx context.Context, agentNodePath, serviceName, agentName string, out *[]registry.ServiceInstance) {
	children, _, err := d.conn.Children(agentNodePath)
	if err != nil {
		return // 中间层读取失败 / 列举后消失 → best-effort 跳过
	}
	for _, leaf := range children {
		leafPath := agentNodePath + "/" + leaf
		data, _, getErr := d.conn.Get(leafPath)
		if getErr != nil {
			continue // §6.3
		}
		inst, parseErr := registry.ParseDiscoveredJSON(data) // §6.6 malformed 跳过
		if parseErr != nil {
			slog.WarnContext(ctx, "zk: skip malformed instance leaf", "path", leafPath, "err", parseErr)
			continue
		}
		// §ZK 节点模型一致性：serviceName + agentName==中间层节点名 + 叶子名=encode(instanceId)。
		// 对端漏写 payload.agentName 时 WithDefaults 补成 serviceName，与中间层节点名不符 → 跳过
		//（暴露对端不合规，Inspect 可诊断），不静默纠偏。
		if inst.ServiceName == serviceName && inst.AgentName == agentName && encodeInstanceID(inst.InstanceID) == leaf {
			*out = append(*out, inst)
		} else {
			slog.WarnContext(ctx, "zk: skip 4-layer leaf with path/payload mismatch", "path", leafPath,
				"payloadService", inst.ServiceName, "payloadAgent", inst.AgentName, "payloadInstanceID", inst.InstanceID)
		}
	}
}

// ── 调试 / 运维：遍历 ZK 树看注册全貌（registry.Directory 接口只有 Discover(ns,service)，
// 下面是 zk binding 特有的 raw 遍历，不进接口）──

// ListNamespaces 列出 root 下所有 namespace 节点名（调试：看 zk 里注册了哪些 namespace）。
func (d *Directory) ListNamespaces() ([]string, error) {
	children, _, err := d.conn.Children(d.root)
	if err == gozk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("zk: list namespaces %s: %w", d.root, err)
	}
	return children, nil
}

// ListServiceNames 列出 namespace 下所有 service 名（调试：看某 namespace 注册了哪些 agent）。
func (d *Directory) ListServiceNames(namespace string) ([]string, error) {
	ns := registry.NormalizeNamespace(namespace)
	nsPath := d.root + "/" + ns
	children, _, err := d.conn.Children(nsPath)
	if err == gozk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("zk: list services %s: %w", nsPath, err)
	}
	return children, nil
}

// InstanceNode 是 Inspect 返回的原始实例叶子诊断：解析成功则 Parsed 非 nil，否则 ParseError
// 给出被 Discover 跳过的原因。
type InstanceNode struct {
	NodeName   string                    // 实例叶子的 ZK 节点名（= encodeInstanceID(instanceId)）
	AgentName  string                    // 所属 agentName 中间层节点名（4 层）
	Parsed     *registry.ServiceInstance // ParseDiscovered + 一致性校验成功则非 nil
	ParseError string                    // 失败原因（空 = 成功）——Discover 会静默跳过这种节点
}

// Inspect 遍历 ns/service 下所有实例叶子返回 raw 解析诊断（4 层）。Discover 静默跳过 malformed /
// path↔payload 不一致的节点（只返健康实例），Inspect **连被跳过的也返 + 原因**——排查"zk 里明明
// 有节点但 Discover 返空"（看是 parse 失败、还是 serviceName/agentName/编码对不上）。
//
// 4 层：service → {agentName} 中间层 → instance 叶子（条目 AgentName=中间层名）。agentName 中间层
// 节点本身（无 payload）不作为条目；其下无叶子时该 agentName 不产生任何条目（= 该 agent 当前无在线
// 实例，可用 DumpSubtree 看分组层全貌）。
func (d *Directory) Inspect(namespace, serviceName string) ([]InstanceNode, error) {
	ns := registry.NormalizeNamespace(namespace)
	svcPath := servicePath(d.root, ns, serviceName)
	children, _, err := d.conn.Children(svcPath)
	if err == gozk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("zk: list children %s: %w", svcPath, err)
	}
	out := make([]InstanceNode, 0, len(children))
	for _, agentName := range children {
		// service 的每个子节点都是 agentName 中间层 → 诊断其下每个实例叶子（条目 AgentName=中间层名）。
		agentNodePath := svcPath + "/" + agentName
		leaves, _, lerr := d.conn.Children(agentNodePath)
		if lerr != nil {
			continue // 中间层读取失败 best-effort 跳过
		}
		for _, leaf := range leaves {
			out = append(out, d.inspectLeaf(agentNodePath+"/"+leaf, serviceName, agentName, leaf))
		}
	}
	return out, nil
}

// inspectLeaf 诊断单个实例叶子节点：解析 + path↔payload 一致性（serviceName + agentName + 编码）。
// agentNameSeg 是 path 里的 agentName 中间层段；leafName 是叶子节点名（应 = encodeInstanceID(instanceId)）。
func (d *Directory) inspectLeaf(leafPath, serviceName, agentNameSeg, leafName string) InstanceNode {
	node := InstanceNode{NodeName: leafName, AgentName: agentNameSeg}
	data, _, getErr := d.conn.Get(leafPath)
	if getErr != nil {
		node.ParseError = fmt.Sprintf("get node failed: %v", getErr)
		return node
	}
	inst, parseErr := registry.ParseDiscoveredJSON(data)
	if parseErr != nil {
		node.ParseError = parseErr.Error()
		return node
	}
	if encodeInstanceID(inst.InstanceID) != leafName {
		node.ParseError = fmt.Sprintf("path/payload mismatch: encodeInstanceID(%q)=%q != 节点名 %q",
			inst.InstanceID, encodeInstanceID(inst.InstanceID), leafName)
		return node
	}
	if inst.ServiceName != serviceName {
		node.ParseError = fmt.Sprintf("payload serviceName %q != 路径段 %q", inst.ServiceName, serviceName)
		return node
	}
	// 4 层：校验 agentName 段与 payload 一致。
	if inst.AgentName != agentNameSeg {
		node.ParseError = fmt.Sprintf("payload agentName %q != 路径段 %q", inst.AgentName, agentNameSeg)
		return node
	}
	instCopy := inst
	node.Parsed = &instCopy
	return node
}

// dumpMaxDepth 限 DumpSubtree 递归深度（防意外深树 / 环——服务子树标准 2~3 层，留余量到 8）。
const dumpMaxDepth = 8

// SubtreeNode 是 DumpSubtree 返回的原始子树节点：完整 ZK 路径 + raw data 原文（不解析、
// 不校验、不假定层级）。
type SubtreeNode struct {
	Path    string // 完整 ZK 路径（从 root 起，如 /crew/a2a/default/research-service/agent_1/MTAuMTkz）
	Depth   int    // 相对 service 节点的深度（service 节点本身=0，其子=1，再下一层=2……）
	RawData string // 节点 data 原文（实例 payload JSON / 中间节点多为空串）
	HasData bool   // data 非空——区分中间分组节点（无 data）vs 叶子实例节点（payload JSON）
	Err     string // 读取该节点失败原因（空 = 成功）
}

// DumpSubtree 递归遍历 {namespace}/{serviceName} 下整棵子树，照实返回每个节点的完整路径 +
// raw data 原文——**不解析、不校验、不假定层级**。Discover/Inspect 按 §4.1 标准层级（service
// 直挂 instance）解读节点；DumpSubtree 不假定任何层级，专用于排查"跨语言对端注册的真实树形
// 与 payload 格式"：对端若在 service↔instance 间多塞分组层（如 service/{agentName}/{instanceId}），
// Inspect 只看一层会把分组层误报 malformed，DumpSubtree 照实 dump 全貌——拿到对端真实 wire
// 再决定如何对齐协议层级（避免基于假设改路径）。
func (d *Directory) DumpSubtree(namespace, serviceName string) ([]SubtreeNode, error) {
	ns := registry.NormalizeNamespace(namespace)
	svcPath := servicePath(d.root, ns, serviceName)
	out := make([]SubtreeNode, 0)
	d.walkSubtree(svcPath, 0, &out)
	return out, nil
}

// walkSubtree 深度优先收集 path 下所有节点（含自身）的 raw 视图。读取失败 best-effort 记录
// 不上抛（诊断工具尽量多 dump）；子节点名排序保证输出稳定；超 dumpMaxDepth 停止防深树/环。
func (d *Directory) walkSubtree(path string, depth int, out *[]SubtreeNode) {
	if depth > dumpMaxDepth {
		return
	}
	node := SubtreeNode{Path: path, Depth: depth}
	data, _, err := d.conn.Get(path)
	if err == gozk.ErrNoNode {
		return // 列举后消失 / 不存在 → 不记录
	}
	if err != nil {
		node.Err = err.Error()
		*out = append(*out, node)
		return
	}
	node.RawData = string(data)
	node.HasData = len(data) > 0
	*out = append(*out, node)

	children, _, err := d.conn.Children(path)
	if err != nil {
		return // 读子节点失败 → best-effort 停在此层
	}
	sort.Strings(children)
	for _, c := range children {
		d.walkSubtree(path+"/"+c, depth+1, out)
	}
}

// Close 关闭 ZK 连接（New 创建的）。
func (d *Directory) Close() error {
	d.conn.Close()
	return nil
}

// ensureParents 自底向上确保 root→ns→svc→agentName 的持久父节点存在（§4.2 父路径用
// persistent node；agentName 中间层也是持久节点，仅 instance 叶子是 ephemeral）。
func (d *Directory) ensureParents(ns, serviceName, agentName string) error {
	for _, p := range ancestorPaths(d.root, ns, serviceName, agentName) {
		_, err := d.conn.Create(p, nil, 0, gozk.WorldACL(gozk.PermAll)) // flags=0 → 持久节点
		if err != nil && err != gozk.ErrNodeExists {
			return fmt.Errorf("zk: ensure parent %s: %w", p, err)
		}
	}
	return nil
}

// zkRegistration 是 ZK 的注册句柄。
type zkRegistration struct {
	conn *gozk.Conn
	path string
	mu   sync.Mutex // 保护 inst（UpdateStatus 改写 + marshal）
	inst registry.ServiceInstance
}

var _ registry.Registration = (*zkRegistration)(nil)

// UpdateStatus 见 registry.Registration.UpdateStatus（§5.7 排空时置 DRAINING）。
func (r *zkRegistration) UpdateStatus(_ context.Context, status registry.Status) error {
	// 写侧校验与 InMemory 对称：非法 status 写进 wire 会让节点被所有发现端按 malformed
	// 跳过——实例静默消失且无日志，排障盲区。
	if !status.Known() {
		return fmt.Errorf("zk: unknown status %q", status)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inst.Status = status
	data, err := json.Marshal(r.inst)
	if err != nil {
		return fmt.Errorf("zk: marshal: %w", err)
	}
	if _, err := r.conn.Set(r.path, data, -1); err != nil { // version=-1 → 任意版本
		return fmt.Errorf("zk: update status %s: %w", r.path, err)
	}
	return nil
}

// Deregister 见 registry.Registration.Deregister（幂等）。
func (r *zkRegistration) Deregister(_ context.Context) error {
	err := r.conn.Delete(r.path, -1)
	if err == gozk.ErrNoNode {
		return nil // 幂等：节点已消失（如 session 已断）
	}
	if err != nil {
		return fmt.Errorf("zk: deregister %s: %w", r.path, err)
	}
	return nil
}
