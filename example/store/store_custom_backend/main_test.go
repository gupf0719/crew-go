package main

import (
	"testing"

	"crew-go/store"
	"crew-go/store/conformance"
)

// TestMapStore_Conformance 跑 store/conformance 全套验证业务自定义 backend 语义合规。
//
// CLAUDE.md "业务自实现 backend 必跑 conformance 套"——业务接真实 PG/Redis/mem0 时
// 同款写法：factory 每次返新 backend instance，conformance 子测试间状态隔离。
//
// 覆盖 7 类（GetPutDelete / Search / Namespace / Batch / TTL / Concurrent /
// ListNamespaces）。mapStore 不实现 TTLAware，TTL 子测试自动走"返 ErrTTLNotSupported"
// 路径（不算 fail）。
func TestMapStore_Conformance(t *testing.T) {
	conformance.RunStoreTests(t, func() store.Store {
		return newMapStore()
	})
}
