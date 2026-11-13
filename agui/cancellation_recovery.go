package agui

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
)

// Server 是 AG-UI Handler 及其共享会话补偿的生命周期。
type Server struct {
	handler *handler
	stop    context.CancelFunc
	done    <-chan struct{}
}

// ServeHTTP 处理 AG-UI 请求。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func newServer(h *handler) *Server {
	s := &Server{handler: h}
	if h.sessionStore != nil {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		s.stop, s.done = cancel, done
		go func() {
			defer close(done)
			h.runCancellationRecovery(ctx)
		}()
	}
	return s
}

// Close 停止后台取消补偿并等待在途尝试退出。可重复调用。
// HTTP 服务由应用关闭。在途取消保留持久进度，其他实例或重启后可继续。
// Close 会等待同步的 AgentFactory 返回。
func (s *Server) Close() error {
	if s.stop != nil {
		s.stop()
		<-s.done
	}
	return nil
}

// WithCancellationHistory 为已接受的取消按保存的历史分区提供 HistoryStore。
// 仅在 HistoryStore 来自 HTTP 中间件时需要，工厂已有历史配置时可沿用工厂。
// 回调使用服务凭据，并遵守 ctx 取消。返回错误时保留清理记录，后续继续尝试。
func WithCancellationHistory(resolve func(ctx context.Context, key agent_v2.HistoryKey) (agent_v2.HistoryStore, error)) Option {
	return func(h *handler) { h.cancellationHistory = resolve }
}

const (
	cancellationPollInterval = 5 * time.Second
	cancellationPageSize     = 64
)

func (h *handler) runCancellationRecovery(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := h.recoverCancellationPass(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "agui: 列举待补偿的取消任务失败", "err", err)
			}
			timer.Reset(cancellationPollInterval)
		}
	}
}

func (h *handler) recoverCancellationPass(ctx context.Context) error {
	for cursor := ""; ctx.Err() == nil; {
		pageCtx, cancel := context.WithTimeout(ctx, dropCPTimeout)
		keys, next, err := h.listCancellations(pageCtx, cursor)
		cancel()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := h.recoverCancellation(ctx, key); err != nil && ctx.Err() == nil && !errors.Is(err, compose_v2.ErrTaskBusy) {
				slog.WarnContext(ctx, "agui: 后台取消补偿未完成", "session", key, "err", err)
			}
		}
		if next == "" {
			return nil
		}
		if next == cursor {
			return errors.New("agui: cancellation listing did not advance")
		}
		cursor = next
	}
	return ctx.Err()
}

func (h *handler) listCancellations(ctx context.Context, cursor string) (keys []string, next string, err error) {
	defer func() {
		if v := recover(); v != nil {
			p := panicerr.New(v)
			panicerr.Log(ctx, slog.LevelError, "agui: 后台列举取消任务捕获 panic", p)
			err = panicerr.WrapOpaque("agui: list cancellations", p)
		}
	}()
	return h.sessionStore.ListCancellations(ctx, cursor, cancellationPageSize)
}

func (h *handler) recoverCancellation(ctx context.Context, key string) (err error) {
	if !isSharedSessionID(key) {
		return nil
	}
	defer func() {
		if v := recover(); v != nil {
			p := panicerr.New(v)
			panicerr.Log(ctx, slog.LevelError, "agui: 后台取消补偿捕获 panic", p)
			err = panicerr.WrapOpaque("agui: recover cancellation", p)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, dropCPTimeout)
	defer cancel()
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, h.cp)
	lifecycle := ctx
	ctx, activity, err := h.loadSharedSession(ctx, key)
	if err != nil {
		return err
	}
	defer activity.Release()
	s := sharedSessionFrom(ctx)
	if s.record == nil || s.record.Cancellation == nil {
		return nil
	}
	intent := s.record.Cancellation
	thread := intent.ThreadID
	if thread != "" && sharedSessionID(sessionKey{owner: intent.Owner, threadID: thread}) != key {
		return errors.New("agui: cancellation identity does not match the session key")
	}
	if thread == "" && intent.HistoryKey != nil {
		// 旧记录只保存了历史会话 ID，直接沿用其已绑定的历史归属。
		thread = intent.HistoryKey.ConvID
	}
	if history := intent.HistoryKey; history != nil && (history.Owner == "" || history.ConvID == "" || history.ConvID != thread) {
		return errors.New("agui: cancellation history does not match the session thread")
	}
	ctx = compose_v2.WithThreadIDCtx(ctx, thread)
	ctx = compose_v2.WithOwner(ctx, intent.Owner)
	return h.reconcileSharedCancellation(ctx, s, agent_v2.WithResumeAbortContext(lifecycle))
}
