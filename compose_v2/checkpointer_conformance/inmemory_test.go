package checkpointer_conformance_test

import (
	"testing"

	"crew-go/compose_v2"
	"crew-go/compose_v2/checkpointer_conformance"
)

// TestInMemoryCheckpointer_Conformance 让 InMemoryCheckpointer 跑全套 conformance
// 验证 — 既验证 InMemoryCheckpointer 自身合规, 也验证 conformance 套设计本身可用
// (业务 import 套用同款范式).
func TestInMemoryCheckpointer_Conformance(t *testing.T) {
	checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
		return compose_v2.NewInMemoryCheckpointer()
	})
}
