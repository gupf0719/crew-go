package agui

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
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

// 租约随 Checkpointer 的存储域隔离，跨轮保留以免多个实例串行重复扫描。
const cancellationWorkerID = "agui:cancel-recovery"

func (h *handler) runCancellationRecovery(ctx context.Context) {
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, h.cp)
	for ctx.Err() == nil {
		leaderCtx, lease, err := compose_v2.BeginTaskActivity(ctx, cancellationWorkerID, nil)
		if err == nil {
			h.runCancellationLeader(leaderCtx, lease)
			// 交还后给候补至少一个轮询周期，避免原持有者立即重新抢占。
			if !waitCancellationPoll(ctx) {
				return
			}
		} else if ctx.Err() == nil && !errors.Is(err, compose_v2.ErrTaskBusy) {
			slog.WarnContext(ctx, "agui: 申请取消补偿租约失败", "err", err)
		}
		if !waitCancellationPoll(ctx) {
			return
		}
	}
}

func (h *handler) runCancellationLeader(ctx context.Context, lease *compose_v2.TaskActivity) {
	defer lease.Release()
	for ctx.Err() == nil {
		if err := h.recoverCancellationPass(ctx); err != nil && ctx.Err() == nil {
			if !errors.Is(err, errCancellationIncomplete) {
				slog.WarnContext(ctx, "agui: 列举待补偿的取消任务失败", "err", err)
			}
			// 本实例缺旧工厂或后端配置时，让其他实例接手。
			return
		}
		if !waitCancellationPoll(ctx) {
			return
		}
	}
}

func waitCancellationPoll(ctx context.Context) bool {
	timer := time.NewTimer(cancellationPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var errCancellationIncomplete = errors.New("agui: some cancellations could not be recovered")

func (h *handler) recoverCancellationPass(ctx context.Context) error {
	var incomplete atomic.Bool
	// 慢项有独立的清理时限。分页之间共用并发预算，失败项留到下一轮。
	var wg sync.WaitGroup
	slots := make(chan struct{}, 4)
	defer wg.Wait()
	for cursor := ""; ctx.Err() == nil; {
		pageCtx, cancel := context.WithTimeout(ctx, dropCPTimeout)
		keys, next, err := h.listCancellations(pageCtx, cursor)
		cancel()
		if err != nil {
			return err
		}
		for _, key := range keys {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			wg.Go(func() {
				defer func() { <-slots }()
				if ctx.Err() != nil {
					return
				}
				if err := h.recoverCancellation(ctx, key); err != nil && ctx.Err() == nil && !errors.Is(err, compose_v2.ErrTaskBusy) {
					incomplete.Store(true)
					slog.WarnContext(ctx, "agui: 后台取消补偿未完成", "session", key, "err", err)
				}
			})
		}
		if next == "" {
			wg.Wait()
			if incomplete.Load() {
				return errCancellationIncomplete
			}
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
