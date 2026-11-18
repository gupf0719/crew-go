package agui

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
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

// WithCancellationHistory 按已绑定的历史分区解析取消或替换所需的 HistoryStore。
// 共享任务使用请求选择的历史后端，或旧记录中未注明后端来源时，替换需要此回调。
// 回调使用服务凭据，并遵守 ctx 取消。替换在清档前解析，已有清理记录可继续重试。
func WithCancellationHistory(resolve func(ctx context.Context, key agent_v2.HistoryKey) (agent_v2.HistoryStore, error)) Option {
	return func(h *handler) { h.cancellationHistory = resolve }
}

const (
	cancellationPollInterval = 5 * time.Second
	cancellationListTimeout  = 10 * time.Second
	cancellationPageSize     = 64
	cancellationConcurrency  = 4
)

// CancellationPollStore 可选地协调同一 SessionStore 存储域内的取消扫描。
// TryAcquireCancellationPoll 在空闲时原子取得固定期限的租约，使用后端时间判断过期。
// ReleaseCancellationPoll 仅释放 token 对应的租约。操作使用独立于任务 ID 的存储位置。
// 未实现时，每个 Handler 独立发现候选，仍由会话及任务租约保护清理。
type CancellationPollStore interface {
	TryAcquireCancellationPoll(ctx context.Context, token string, ttl time.Duration) (bool, error)
	ReleaseCancellationPoll(ctx context.Context, token string) error
}

type cancellationRecovery struct {
	cursor    string
	pending   []string
	active    map[string]struct{}
	completed chan cancellationResult
	failed    bool
	workers   sync.WaitGroup
}

type cancellationResult struct {
	key    string
	failed bool
}

func (h *handler) runCancellationRecovery(ctx context.Context) {
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, h.cp)
	store, _ := h.sessionStore.(CancellationPollStore)
	recovery := &cancellationRecovery{
		active:    make(map[string]struct{}, cancellationConcurrency),
		completed: make(chan cancellationResult, cancellationConcurrency),
	}
	defer recovery.workers.Wait()
	for ctx.Err() == nil {
		var token string
		var err error
		recovery.failed = false
		if len(recovery.active) < cancellationConcurrency {
			token, err = h.scanCancellations(ctx, store, recovery)
			if err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "agui: 发现待补偿的取消任务失败", "err", err)
			}
		}
		proceed := recovery.waitPoll(ctx)
		if token != "" {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancellationPollInterval)
			if releaseErr := releaseCancellationPoll(releaseCtx, store, token); releaseErr != nil {
				slog.WarnContext(ctx, "agui: 释放取消补偿轮询租约失败", "err", releaseErr)
			}
			cancel()
		}
		if !proceed {
			return
		}
		// 失败或长时间未返回时，给其他实例至少一个轮询间隔。
		if token != "" && (err != nil || recovery.failed || len(recovery.active) > 0) && !recovery.waitPoll(ctx) {
			return
		}
	}
}

func (r *cancellationRecovery) waitPoll(ctx context.Context) bool {
	timer := time.NewTimer(cancellationPollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case result := <-r.completed:
			r.complete(result)
		}
	}
}

func (r *cancellationRecovery) complete(result cancellationResult) {
	delete(r.active, result.key)
	r.failed = r.failed || result.failed
}

func (h *handler) scanCancellations(ctx context.Context, store CancellationPollStore, r *cancellationRecovery) (token string, err error) {
	defer catchCancellationPollPanic(ctx, "agui: scan cancellations", &err)
	// 新页准入从申请前计时。轮询租约有固定期限，期间无续约。
	scanCtx, cancel := context.WithTimeout(ctx, cancellationPollInterval)
	defer cancel()
	if store != nil {
		token = rand.Text()
		acquired, err := store.TryAcquireCancellationPoll(scanCtx, token, cancellationPollInterval)
		if err != nil {
			// 返回丢失时后端可能已经取得租约，退出前按 token 尝试释放。
			return token, err
		}
		if !acquired {
			return "", nil
		}
	}
	return token, h.discoverCancellations(ctx, scanCtx, r)
}

func releaseCancellationPoll(ctx context.Context, store CancellationPollStore, token string) (err error) {
	defer catchCancellationPollPanic(ctx, "agui: release cancellation poll lease", &err)
	return store.ReleaseCancellationPoll(ctx, token)
}

func catchCancellationPollPanic(ctx context.Context, operation string, err *error) {
	if v := recover(); v != nil {
		p := panicerr.New(v)
		panicerr.Log(ctx, slog.LevelError, "agui: 取消扫描捕获 panic", p, "operation", operation)
		*err = panicerr.WrapOpaque(operation, p)
	}
}

func (h *handler) discoverCancellations(ctx, scanCtx context.Context, r *cancellationRecovery) error {
	for scanCtx.Err() == nil {
		if len(r.pending) == 0 {
			keys, next, err := h.listCancellations(ctx, r.cursor)
			if err != nil {
				return err
			}
			if next != "" && next == r.cursor {
				return errors.New("agui: cancellation listing did not advance")
			}
			r.pending, r.cursor = keys, next
		}
		// 迟于轮询窗口返回的页仍可用空闲名额。候选在补偿时会在会话租约下重读。
		// 满载等待与开始新页受轮询窗口约束，未派发项保留到下一轮。
		for len(r.pending) > 0 && ctx.Err() == nil {
			key := r.pending[0]
			if _, busy := r.active[key]; busy {
				r.pending = r.pending[1:]
				continue
			}
			if len(r.active) == cancellationConcurrency {
				select {
				case <-scanCtx.Done():
					return nil
				case result := <-r.completed:
					r.complete(result)
				}
				continue
			}
			r.pending = r.pending[1:]
			r.active[key] = struct{}{}
			r.workers.Go(func() {
				result := cancellationResult{key: key, failed: true}
				defer func() { r.completed <- result }()
				err := h.recoverCancellation(ctx, key)
				result.failed = err != nil && ctx.Err() == nil && !errors.Is(err, compose_v2.ErrTaskBusy)
				if result.failed {
					slog.WarnContext(ctx, "agui: 后台取消补偿未完成", "session", key, "err", err)
				}
			})
		}
		if r.cursor == "" {
			return nil
		}
	}
	return nil
}

func (h *handler) buildCancellationAgent(version string) (agent_v2.Agent, error) {
	factory := h.workflowFactory(version)
	if factory == nil {
		return nil, errWorkflowUnavailable
	}
	ag := factory(hook.NoOpHooks{})
	if agent_v2.IsNilAgent(ag) {
		return nil, errors.New("agui: AgentFactory returned nil agent")
	}
	return ag, nil
}

func (h *handler) listCancellations(ctx context.Context, cursor string) (keys []string, next string, err error) {
	defer func() {
		if v := recover(); v != nil {
			p := panicerr.New(v)
			panicerr.Log(ctx, slog.LevelError, "agui: 后台列举取消任务捕获 panic", p)
			err = panicerr.WrapOpaque("agui: list cancellations", p)
		}
	}()
	listCtx, cancel := context.WithTimeout(ctx, cancellationListTimeout)
	defer cancel()
	keys, next, err = h.sessionStore.ListCancellations(listCtx, cursor, cancellationPageSize)
	if err == nil {
		err = listCtx.Err()
	}
	return keys, next, err
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
	// 构造工厂前只读工作流版本，未持有会话租约。其他实例可正常完成同一取消或替换。
	candidate, err := h.sessionStore.LoadSession(ctx, key)
	if err != nil || candidate == nil || candidate.Cancellation == nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ag, err := h.buildCancellationAgent(candidate.WorkflowVersion)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 以持有会话租约时读到的进度和归属为准，只清理同一个任务。
	lifecycle := ctx
	ctx, activity, err := h.loadSharedSession(ctx, key)
	if err != nil {
		return err
	}
	defer activity.Release()
	s := sharedSessionFrom(ctx)
	if !sameCancellationTask(s.record, candidate) {
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
	return cancelSharedSession(ctx, s, ag, sharedCancellationRequest{}, h.cancellationHistory, agent_v2.WithResumeAbortContext(lifecycle))
}
