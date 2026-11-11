package compose_v2

import (
	"context"
	"log/slog"

	"crew-go/internal/panicerr"
)

type checkpointRestoreObserverKey struct{}

func (checkpointRestoreObserverKey) Environment() {}

// WithCheckpointRestoreObserver 让观测方在可执行存档实际恢复后收到通知。
// 回调与 [WithEmitCpEvent] 的开关独立。
func WithCheckpointRestoreObserver(ctx context.Context, observe func(context.Context)) context.Context {
	return context.WithValue(ctx, checkpointRestoreObserverKey{}, observe)
}

func notifyCheckpointRestored(ctx context.Context) {
	observe, _ := ctx.Value(checkpointRestoreObserverKey{}).(func(context.Context))
	if observe == nil {
		return
	}
	defer func() {
		if value := recover(); value != nil {
			panicerr.Log(ctx, slog.LevelError, "compose_v2: 恢复存档的观察回调 panic", panicerr.New(value))
		}
	}()
	observe(ctx)
}
