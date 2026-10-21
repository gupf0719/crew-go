package conformance_test

import (
	"testing"

	"crew-go/store"
	"crew-go/store/conformance"
)

// TestInMemoryStore_Conformance 让 InMemoryStore 跑全套 conformance 验证 — 既验证
// InMemoryStore 自身合规, 也验证 conformance 套设计本身可用 (业务 import 套用同款
// 范式).
func TestInMemoryStore_Conformance(t *testing.T) {
	conformance.RunStoreTests(t, func() store.Store {
		return store.NewInMemoryStore()
	})
}
