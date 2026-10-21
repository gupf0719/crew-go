package redis

import (
	"context"

	"crew-go/compose_v2"
)

// SaveBatch 实现 compose_v2.Checkpointer.SaveBatch.
//
// **PoC 实现**: 顺序调 SaveNS (跟 InMemoryCheckpointer.SaveBatch / sql 同款语义).
// 任一失败立即返 error, 不继续. best-effort partial success state caller 自处理.
//
// **后续优化**: Phase 2 可加 `WithSaveBatchAtomic(true)` opt-in — 用 Lua 脚本
// 真原子写多 ns. PoC 不做.
func (c *Checkpointer) SaveBatch(ctx context.Context, sessionID string, batch map[string]*compose_v2.Checkpoint) error {
	for ns, cp := range batch {
		if err := c.SaveNS(ctx, sessionID, ns, cp); err != nil {
			return err
		}
	}
	return nil
}
