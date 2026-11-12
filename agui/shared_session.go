package agui

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"reflect"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// SessionRecord 保存一个 thread 当前任务的请求前文和已采纳答复。挂起问题以根存档为准。
type SessionRecord struct {
	TaskID    string                     `json:"task_id"`
	Prior     []*schema.Message          `json:"prior,omitempty"`
	Closing   bool                       `json:"closing,omitempty"`
	ModelData string                     `json:"model_data,omitempty"`
	Answers   map[string]*schema.Message `json:"answers,omitempty"`
}

// SessionStore 在同一共享后端保存会话记录、任务租约及图存档。key 由 Handler 生成，代表
// (owner, threadId)。LoadSession 在无记录时返回 (nil, nil)，返回值由调用方持有。
type SessionStore interface {
	compose_v2.Checkpointer
	compose_v2.TaskLeaseProvider
	LoadSession(ctx context.Context, key string) (*SessionRecord, error)
	// SaveSession 整体替换记录，record 为 nil 时删除。dropTask 非空时还原子删除该任务的
	// 全部存档，并返回删除数量。写入时核对 [compose_v2.CheckpointTaskLeaseToken]
	// 对应的会话租约、record.TaskID 和 dropTask 的任务租约，缺失或过期时拒绝写入。
	SaveSession(ctx context.Context, key string, record *SessionRecord, dropTask string) (int32, error)
}

// WithSessionStore 启用共享会话和 GET 挂起查询。store 同时提供 Checkpointer；
// [WithSessionTTL] 和 [WithMaxSessions] 只作用于默认的进程内会话表。
func WithSessionStore(store SessionStore) Option {
	return func(h *handler) {
		if nilInterface(store) || nilInterface(store.CheckpointTaskLeases()) {
			panic("agui: WithSessionStore requires a shared session store with task leases")
		}
		h.sessionStore = store
	}
}

func nilInterface(v any) bool {
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

type sharedSessionKey struct{}

func (sharedSessionKey) ExecutionState() {}

// sharedSession 只在持有会话租约的请求内使用。
type sharedSession struct {
	store  SessionStore
	key    string
	record *SessionRecord
}

func (h *handler) openSharedSession(ctx context.Context, key sessionKey) (context.Context, *compose_v2.TaskActivity, error) {
	// owner 长度先编码，保留 (owner, threadId) 的两个身份维度。
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s%s", len(key.owner), key.owner, key.threadID))
	id := fmt.Sprintf("agui-thread:%x", sum)
	ctx, activity, err := compose_v2.BeginTaskActivity(ctx, id, nil)
	if err != nil {
		return ctx, nil, err
	}
	var opened bool
	defer func() {
		if !opened {
			activity.Release()
		}
	}()
	record, err := h.sessionStore.LoadSession(ctx, id)
	if err != nil {
		return ctx, nil, err
	}
	if record != nil && record.TaskID == "" {
		return ctx, nil, errors.New("agui: shared session has an empty task id")
	}
	shared := &sharedSession{store: h.sessionStore, key: id, record: record}
	if record != nil && record.Closing {
		if err := h.finishSharedSession(ctx, shared); err != nil {
			return ctx, nil, err
		}
	}
	opened = true
	return context.WithValue(ctx, sharedSessionKey{}, shared), activity, nil
}

func sharedSessionFrom(ctx context.Context) *sharedSession {
	s, _ := ctx.Value(sharedSessionKey{}).(*sharedSession)
	return s
}

func (s *sharedSession) save(ctx context.Context, record *SessionRecord, dropTask string) error {
	_, err := s.write(ctx, record, dropTask)
	return err
}

func (s *sharedSession) write(ctx context.Context, record *SessionRecord, dropTask string) (int32, error) {
	// SSE 断开时仍需收尾。原租约令牌保留在 ctx，由后端在写入时核对。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropCPTimeout)
	defer cancel()
	deleted, err := s.store.SaveSession(ctx, s.key, record, dropTask)
	if err == nil {
		s.record = record
	}
	return deleted, err
}

func (s *sharedSession) session() *agent_v2.RunSession {
	sess := agent_v2.NewRunSession()
	sess.RunID = s.record.TaskID
	return sess
}

func (s *sharedSession) pending(ctx context.Context) (interruptBatch, error) {
	if s.record == nil {
		return interruptBatch{}, nil
	}
	cp, err := s.store.LoadNS(ctx, s.record.TaskID, "")
	if err != nil {
		return interruptBatch{}, err
	}
	if cp == nil {
		return interruptBatch{}, nil
	}
	if cp.ID != s.record.TaskID {
		return interruptBatch{}, errors.New("agui: shared session checkpoint has a different session id")
	}
	batch, err := projectInterrupts(cp.PendingInterrupts, "")
	batch.epoch = cp.Epoch
	return batch, err
}

var (
	errAnswerAccepted = errors.New("a different answer has already been accepted for an interrupt")
	errPendingChanged = errors.New("pending interrupts advanced before the task could be discarded")
)

func (s *sharedSession) accept(ctx context.Context, answers map[string]*schema.Message) (map[string]*schema.Message, error) {
	next := *s.record
	next.Answers = make(map[string]*schema.Message, len(s.record.Answers)+len(answers))
	maps.Copy(next.Answers, s.record.Answers)
	for id, answer := range answers {
		if prior := next.Answers[id]; prior != nil && prior.Content != answer.Content {
			return nil, errAnswerAccepted
		}
		next.Answers[id] = answer
	}
	// 答复先于图执行落盘。图推进后即使响应丢失，后续请求仍可重放同一节点的早期答复。
	if err := s.save(ctx, &next, ""); err != nil {
		return nil, err
	}
	return next.Answers, nil
}

func (h *handler) finishSharedSession(ctx context.Context, s *sharedSession) error {
	if s.record == nil {
		return nil
	}
	if err := compose_v2.CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	sid := s.record.TaskID
	if compose_v2.CheckpointTaskLeaseToken(ctx, s.store.CheckpointTaskLeases(), sid) == "" {
		dropCtx, activity, err := h.beginDropCP(ctx, sid)
		if err != nil {
			return err
		}
		defer activity.Release()
		ctx = dropCtx
	}
	if !s.record.Closing {
		next := *s.record
		next.Closing = true
		if err := s.save(ctx, &next, ""); err != nil {
			return err
		}
	}
	return s.save(ctx, nil, sid)
}

func (h *handler) completeSession(ctx context.Context, key sessionKey) error {
	if s := sharedSessionFrom(ctx); s != nil {
		return h.finishSharedSession(ctx, s)
	}
	h.removeEntry(key)
	return nil
}

func (h *handler) servePending(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	if threadID == "" {
		_ = writeReject(w, http.StatusBadRequest, "no_thread_id", "pending query requires threadId")
		return
	}
	ctx, key := h.requestContext(r, threadID)
	ctx, activity, err := h.openSharedSession(ctx, key)
	if err != nil {
		status, code, message := sharedSessionError(ctx, err)
		_ = writeReject(w, status, code, message)
		return
	}
	defer activity.Release()
	s := sharedSessionFrom(ctx)
	if s.record != nil {
		taskCtx, lease, err := compose_v2.BeginTaskActivity(ctx, s.record.TaskID, nil)
		if err != nil {
			status, code, message := sharedSessionError(ctx, err)
			_ = writeReject(w, status, code, message)
			return
		}
		defer lease.Release()
		ctx = taskCtx
	}
	batch, err := s.pending(ctx)
	if err != nil {
		status, code, message := sharedSessionError(ctx, err)
		_ = writeReject(w, status, code, message)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if batch.events == nil {
		batch.events = []RunInterrupt{}
	}
	var accepted []string
	if s.record != nil {
		for _, q := range batch.events {
			if s.record.Answers[q.ID] != nil {
				accepted = append(accepted, q.ID)
			}
		}
	}
	err = json.NewEncoder(w).Encode(struct {
		ThreadID   string         `json:"threadId"`
		Interrupts []RunInterrupt `json:"interrupts"`
		Accepted   []string       `json:"acceptedInterruptIds,omitempty"`
	}{threadID, batch.events, accepted})
	if err != nil {
		slog.DebugContext(ctx, "agui: 挂起查询响应未送达", "err", err)
	}
}

func sharedSessionError(ctx context.Context, err error) (int, string, string) {
	if errors.Is(err, compose_v2.ErrTaskBusy) {
		return http.StatusConflict, "busy", "an operation on this thread is in progress"
	}
	if errors.Is(err, errAnswerAccepted) {
		return http.StatusConflict, "answer_already_accepted", errAnswerAccepted.Error()
	}
	slog.WarnContext(ctx, "agui: 共享会话操作失败", "err", err)
	return http.StatusServiceUnavailable, "session_store_failed", "the shared session could not be read or updated"
}

func rejectSharedSession(ctx context.Context, sse *SSEWriter, err error) {
	status, code, message := sharedSessionError(ctx, err)
	_ = sse.Reject(status, code, message)
}
