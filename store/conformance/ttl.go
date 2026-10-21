package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/store"
)

// testTTL 验证 TTL 支持. backend 不实现 TTLAware 时 (如 InMemoryStore) 走"返
// ErrTTLNotSupported" 路径; 实现 TTLAware 时走"实际 expire" 路径.
func testTTL(t *testing.T, factory StoreFactory) {
	t.Run("TTLAware_TypeAssert", func(t *testing.T) {
		st := factory()
		ta, ok := st.(store.TTLAware)
		if !ok {
			t.Log("backend does not implement TTLAware (合规, e.g., InMemoryStore)")
			return
		}
		// 实现了 TTLAware 必须返 true (false 没意义)
		if !ta.SupportsTTL() {
			t.Error("if backend implements TTLAware interface, SupportsTTL() must return true")
		}
	})

	t.Run("Put_WithTTL_NotSupported_Returns_ErrTTLNotSupported", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("ttl_test")

		ta, ok := st.(store.TTLAware)
		if ok && ta.SupportsTTL() {
			t.Skip("backend supports TTL, this test verifies non-TTL backend path")
		}

		err := st.Put(ctx, ns, "k", "v", store.WithTTL(5*time.Second))
		if err == nil {
			t.Error("non-TTL backend should return ErrTTLNotSupported with WithTTL")
		}
		if !errors.Is(err, store.ErrTTLNotSupported) {
			t.Errorf("expected ErrTTLNotSupported, got %v", err)
		}
	})

	t.Run("Put_WithTTL_Supported_Eventually_Expires", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("ttl_expire")

		ta, ok := st.(store.TTLAware)
		if !ok || !ta.SupportsTTL() {
			t.Skip("backend does not support TTL — skip expire test")
		}

		// 写一条 TTL=200ms
		if err := st.Put(ctx, ns, "transient", "v", store.WithTTL(200*time.Millisecond)); err != nil {
			t.Fatalf("Put WithTTL: %v", err)
		}

		// 立即 Get 应能拿到
		item, _ := st.Get(ctx, ns, "transient")
		if item == nil {
			t.Fatal("expected item before TTL expire")
		}

		// 等 500ms (TTL + 缓冲)
		time.Sleep(500 * time.Millisecond)

		// expired 后应返 nil
		item, _ = st.Get(ctx, ns, "transient")
		if item != nil {
			t.Errorf("expected nil after TTL expire (200ms), got %+v", item)
		}
	})
}
