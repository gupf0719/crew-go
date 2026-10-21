package registry

import "context"

// Directory 是 A2A 服务目录抽象——注册（写 ephemeral 实例）+ 发现（按 namespace+serviceName 读）。
//
// 接口定义在使用方（crew-go）。InMemory 是进程内实现；跨进程跨语言互通用 ZooKeeper
// Binding v1（a2a/zk 子模块）；业务自实现其它 backend（Nacos/etcd/Consul）跑
// registry/conformance 验收语义合规。
//
// namespace 是路径坐标（§4.1，**不在 ServiceInstance payload 里**），故 Register / Discover
// 都显式带 namespace；空值归一到 "default"。
type Directory interface {
	// Register 在 namespace 下写入一条服务实例（ephemeral 语义：注册进程退出 / 连接断开 /
	// 显式反注册后实例消失）。inst 内部会 WithDefaults + Validate，非法返 error。返回
	// Registration 句柄用于更新状态 / 反注册。
	Register(ctx context.Context, namespace string, inst ServiceInstance) (Registration, error)

	// Discover 按 namespace + serviceName 返回通过校验的实例快照。服务路径不存在返空列表
	// + nil error（§6.1）；单个 malformed 节点跳过不报错（§3.3 / §6.6）；整体读取失败才
	// 返 error（§6.7）。返回的实例已 WithDefaults 规整。
	//
	// **实现约定（W2）**：返回的必须是 deep-copy 快照——引用类型字段（Metadata map）不可
	// 与内部存储别名，否则调用方改返回值会污染存储 + 与并发 Discover/Register data race。
	// InMemory 直存 Go struct 故显式 clone Metadata；ZK 等从 wire JSON 解码的 backend 天然
	// 每次新建 map 无此问题。
	Discover(ctx context.Context, namespace, serviceName string) ([]ServiceInstance, error)

	// Close 释放底层资源（ZK session 等）。InMemory 为 no-op。
	Close() error
}

// Registration 是一次注册的句柄——更新状态 / 反注册。
type Registration interface {
	// UpdateStatus 更新实例状态（如排空时置 DRAINING，§5.7）。
	UpdateStatus(ctx context.Context, status Status) error
	// Deregister 显式反注册删除实例节点（§11）。幂等：重复调用 / 节点已消失返 nil。
	Deregister(ctx context.Context) error
}
