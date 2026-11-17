package sql

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"crew-go/compose_v2"
)

// SaveBatch 按 namespace 顺序写入，遇到第一个错误即返回。
func (c *Checkpointer) SaveBatch(ctx context.Context, sessionID string, batch map[string]*compose_v2.Checkpoint) error {
	for _, ns := range slices.Sorted(maps.Keys(batch)) {
		if err := c.SaveNS(ctx, sessionID, ns, batch[ns]); err != nil {
			return fmt.Errorf("save batch checkpoint sid=%q ns=%q: %w", sessionID, ns, err)
		}
	}
	return nil
}
