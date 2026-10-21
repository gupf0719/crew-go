package checkpointer_conformance

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// testExists 验证 ExistsNS true/false 判定.
func testExists(t *testing.T, factory CheckpointerFactory) {
	t.Run("ExistsNS_NonExistent_Returns_False", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		exists, err := cp.ExistsNS(ctx, "sid", "ns")
		if err != nil {
			t.Errorf("ExistsNS: %v", err)
		}
		if exists {
			t.Error("ExistsNS for non-existent should return false")
		}
	})

	t.Run("ExistsNS_After_Save_Returns_True", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid", "ns", newCheckpoint("g", compose_v2.CPStatusUnset))

		exists, err := cp.ExistsNS(ctx, "sid", "ns")
		if err != nil {
			t.Errorf("ExistsNS: %v", err)
		}
		if !exists {
			t.Error("ExistsNS after SaveNS should return true")
		}
	})

	t.Run("ExistsNS_Different_NS_Returns_False", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid", "ns_a", newCheckpoint("g", compose_v2.CPStatusUnset))

		exists, _ := cp.ExistsNS(ctx, "sid", "ns_b")
		if exists {
			t.Error("ExistsNS for different ns should return false")
		}
	})
}
