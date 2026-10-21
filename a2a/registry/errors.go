package registry

import "errors"

// ErrInstanceExists 表示注册目标实例路径已被持有（§4.3：不得删除他人节点，应返注册失败）。
// 业务 errors.Is 命中时走"先 Deregister 旧句柄再注册"或"换 instanceId"；backend（zk 等）
// 的 Register 冲突路径必须用 %w 包装本哨兵（conformance 套校验）。
var ErrInstanceExists = errors.New("registry: instance already registered")
