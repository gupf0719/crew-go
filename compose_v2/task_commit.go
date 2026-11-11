package compose_v2

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
)

// NewTaskCommitSignalKind 注册一类随节点和存档传递的任务提交信号。
// 外层图成功退出时调用 apply。处理器应使用任务入口的配置，并能幂等处理同一值。
// apply 返回的错误只记日志。存档恢复后的成功出口可以处理存档中的信号。
func NewTaskCommitSignalKind(name string, apply func(context.Context, Signal) error) *SignalKind {
	if apply == nil {
		panic("compose_v2: task commit signal requires an apply function")
	}
	return registerSignalKind(name, apply)
}

type taskCommitContextKey struct{}

func (taskCommitContextKey) ExecutionState() {}

type taskCommitScope struct {
	base    context.Context
	graphID string
}

func taskCommitBase(ctx context.Context) *taskCommitScope {
	base, _ := ctx.Value(taskCommitContextKey{}).(*taskCommitScope)
	return base
}

// TaskCommitContext 返回任务最外层图进入时的 ctx，供提交信号的写入者核对外层持有的配置。
// 图外调用返回传入的 ctx。
func TaskCommitContext(ctx context.Context) context.Context {
	if base := taskCommitBase(ctx); base != nil {
		return base.base
	}
	return ctx
}

type taskCommitBarrierKey struct{}

func (taskCommitBarrierKey) ExecutionState() {}

type taskCommitBarrier struct {
	mu    sync.Mutex
	apply func()
}

// WithTaskCommitBarrier 让图的宿主在业务结果生成成功后确认任务提交。用于图退出后还会调用业务代码的入口。
func WithTaskCommitBarrier(ctx context.Context) (context.Context, func()) {
	if WithinNodeScope(ctx) {
		return ctx, func() {}
	}
	barrier := &taskCommitBarrier{}
	return context.WithValue(ctx, taskCommitBarrierKey{}, barrier), func() {
		barrier.mu.Lock()
		apply := barrier.apply
		barrier.apply = nil
		barrier.mu.Unlock()
		if apply != nil {
			apply()
		}
	}
}

func finishTaskCommit(ctx context.Context, cm *ChannelManager) {
	if barrier, _ := ctx.Value(taskCommitBarrierKey{}).(*taskCommitBarrier); barrier != nil {
		barrier.mu.Lock()
		barrier.apply = func() { applyTaskCommitSignals(ctx, cm) }
		barrier.mu.Unlock()
		return
	}
	applyTaskCommitSignals(ctx, cm)
}

func sealCommitSignal(key []byte, scope *taskCommitScope, s SignalRecord) string {
	base := scope.base
	payload, _ := json.Marshal([7]string{s.Kind, s.Dest, s.Value,
		GetSessionID(base), GetGraphNS(base), scope.graphID, GetThreadID(base)}) // 字符串数组总能编码。
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload) // HMAC 写入不会失败。
	return hex.EncodeToString(mac.Sum(nil))
}

func applyTaskCommitSignals(ctx context.Context, cm *ChannelManager) {
	if cm == nil {
		return
	}
	key := taskRecipientKeyFrom(ctx)
	scope := taskCommitBase(ctx)
	if len(key) == 0 || scope == nil {
		return
	}
	base := scope.base
	for _, record := range cm.signalsSnapshot() {
		if record.Seal == "" || !hmac.Equal([]byte(record.Seal), []byte(sealCommitSignal(key, scope, record))) {
			continue
		}
		signalKindsMu.Lock()
		reg, ok := signalKinds[record.Kind]
		signalKindsMu.Unlock()
		if !ok || reg.kind.commit == nil {
			continue
		}
		signal := Signal{Dest: record.Dest, Value: record.Value, Origin: record.Origin}
		if err := exitStorageOp(base, exitOpWrite, record.Kind, func(c context.Context) error {
			return reg.kind.commit(c, signal)
		}); err != nil {
			slog.WarnContext(base, "compose_v2: 任务提交信号写入失败", "kind", record.Kind, "err", err)
		}
	}
}
