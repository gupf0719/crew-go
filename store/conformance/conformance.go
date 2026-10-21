// Package conformance 提供 store.Store 接口的一致性测试套件——业务实现自定义
// backend (PostgresStore / RedisStore / MySQLStore 等) 后, 在自己的 _test.go 里
// import 本包 + 调用 `RunStoreTests` 验证语义合规.
//
// 用法示例:
//
//	package mybackend_test
//
//	import (
//	    "testing"
//	    "crew-go/store"
//	    "crew-go/store/conformance"
//	    "myorg/mybackend"
//	)
//
//	func TestMyRedisStore_Conformance(t *testing.T) {
//	    conformance.RunStoreTests(t, func() store.Store {
//	        st := mybackend.NewRedisStore("redis://localhost:6379")
//	        t.Cleanup(func() { _ = st.Close() })
//	        return st
//	    })
//	}
//
// **设计哲学**: 业务每个子测试拿全新 store instance (factory 模式), 子测试间状态
// 隔离. 业务 backend 自带 Close 时在 factory 内 t.Cleanup 注册即可.
//
// **覆盖范围**: 7 类子测试 (GetPutDelete / Search / Namespace / Batch / TTL /
// Concurrent / ListNamespaces), 对照 InMemoryStore 行为锚定语义.
//
// # 业务可选 capability 自动 skip 协议
//
// crew-go 协议允许业务 backend "**部分实现**"——某些 capability 是
// `optional`, 业务不实现时 conformance 自动 skip 子测试不算 fail:
//
//   - **TTL** (`store.TTLAware` interface) — 业务 backend 不实现 TTLAware
//     interface 时, `Put + WithTTL` 子测试自动检测并 skip "expire" 路径,
//     转走"返 ErrTTLNotSupported" 路径. 两路径都合规.
//
// **业务**必**实现的 capability** (不可 skip, 实现错 conformance 直接 fail):
//   - Get / Put / Delete / Search / ListNamespaces / Batch 6 个 method
//   - namespace 校验 (空 / 含 `.` / 空段 拒绝)
//   - 跨 namespace 隔离
//   - 并发安全
//   - 6 比较 op ($eq / $ne / $gt / $gte / $lt / $lte) 任一不实现/实现错 → fail
//   - **范围 op 的 type bracketing**: 存的值不是数值 (字符串 / bool / null / 数组 /
//     对象) 或字段缺失 ⟹ 那一行**不命中、不报错**——一条异类型的行不能毒化整个 Search
//   - **$eq / $ne / 直值是 JSON 语义相等**: 10 == 10.0、true != 1、"10" != 10、
//     数组逐元素相等 (不是包含)、对象逐键相等；整数按**精确整数**比 (2^53 之外不能
//     被 float64 舍成相等)
//   - **嵌套 / 混用 op map 与顶级 $ 键一律拒**: `{k:{g:1}}` / `{k:{$eq:1,g:1}}` /
//     `{"$or":[...]}` 都返 ErrUnsupportedFilterOp，不能静默返零行
//   - sentinel error 类型一致 (ErrEmptyNamespace / ErrInvalidNamespace /
//     ErrUnsupportedFilterOp / ErrFilterTypeMismatch / ErrTTLNotSupported)
//
// **不覆盖的部分** (业务自负):
//   - 语义搜索 (`WithQuery`) — InMemoryStore 忽略, 业务实现 mem0/向量后端时
//     自行测试 query 准确性
//   - 跨进程序列化 (业务 PG/Redis 后端的 wire format) — 业务自测 round-trip
//   - 跨 backend 性能 — 业务自测吞吐 / 延迟
package conformance

import (
	"testing"

	"crew-go/store"
)

// StoreFactory 工厂函数——每次调用返回新 store instance 给子测试用.
//
// 业务自行管理 backend lifecycle (在 factory 内调 t.Cleanup 关连接):
//
//	factory := func() store.Store {
//	    st := NewMyRedisStore(...)
//	    t.Cleanup(func() { _ = st.Close() })
//	    return st
//	}
type StoreFactory func() store.Store

// RunStoreTests 跑全套 Store 一致性测试.
//
// 子测试用 t.Run 分组——业务 `go test -run TestMyStore/GetPutDelete` 可只跑某组.
//
// 业务后端不支持某类语义 (如 TTL 后端未实现 TTLAware) 时子测试自动跳过 —— 不算
// fail.
func RunStoreTests(t *testing.T, factory StoreFactory) {
	t.Helper()
	t.Run("GetPutDelete", func(t *testing.T) { testGetPutDelete(t, factory) })
	t.Run("Search", func(t *testing.T) { testSearch(t, factory) })
	t.Run("Namespace", func(t *testing.T) { testNamespace(t, factory) })
	t.Run("Batch", func(t *testing.T) { testBatch(t, factory) })
	t.Run("TTL", func(t *testing.T) { testTTL(t, factory) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, factory) })
	t.Run("ListNamespaces", func(t *testing.T) { testListNamespaces(t, factory) })
}
