package sql

import (
	"context"

	"crew-go/compose_v2"
)

// SaveBatch 实现 compose_v2.Checkpointer.SaveBatch.
//
// **协议** (checkpointer.go:71-75):
//   - 一次原子写多个 cp (同 sessionID 下不同 ns)
//   - 内存实现走顺序 SaveNS
//   - 事务后端可优化为真原子, best-effort 失败时返 error
//
// **PoC 实现**: 顺序调 SaveNS, 任一失败立即返 error 不继续 (跟 InMemoryCheckpointer.
// SaveBatch 的 best-effort 顺序语义一致). 业务想要 SQL TX
// 原子写, 可 wrap *sql.DB.BeginTx 自管 — 但跟接口 best-effort 协议偏离.
//
// 后续 Phase 2 可加 `WithSaveBatchTransaction(true)` opt-in atomic 模式 (业务
// 真有诉求时驱动加).
func (c *Checkpointer) SaveBatch(ctx context.Context, sessionID string, batch map[string]*compose_v2.Checkpoint) error {
	for ns, cp := range batch {
		if err := c.SaveNS(ctx, sessionID, ns, cp); err != nil {
			return err
		}
	}
	return nil
}
