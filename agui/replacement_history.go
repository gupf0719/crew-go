package agui

import (
	"context"
	"time"

	"crew-go/agent_v2"
)

// bindLocalHistory 固定旧任务实际使用的历史目标。替换任务独立解析自身目标。
func (e *sessionEntry) bindLocalHistory(ctx context.Context, sid string) context.Context {
	return agent_v2.WithTaskHistoryBinding(ctx, sid, e.history, func(_ context.Context, selected agent_v2.TaskHistoryDestination) error {
		e.history = selected
		return nil
	})
}

// closeReplacementHistory 在放弃许可范围内补齐已清档任务的历史。
func closeReplacementHistory(ctx context.Context, entry *sessionEntry, deadline time.Time) error {
	if deadline.IsZero() {
		deadline = time.Now().Add(dropCPTimeout)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return agent_v2.CloseAbandonedTaskHistory(ctx, entry.history, entry.sess.RunID)
}
