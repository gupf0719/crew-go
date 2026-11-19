package agui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"reflect"
	"strings"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// SessionRecord 保存一个 thread 当前任务的请求前文和已采纳答复。挂起问题以根存档为准。
type SessionRecord struct {
	WorkflowVersion string                     `json:"workflow_version,omitempty"`
	TaskID          string                     `json:"task_id"`
	Prior           []*schema.Message          `json:"prior,omitempty"`
	Closing         bool                       `json:"closing,omitempty"`
	Cancellation    *SessionCancellation       `json:"cancellation,omitempty"`
	HistoryKey      agent_v2.HistoryKey        `json:"history_key,omitzero"`
	HistorySource   agent_v2.TaskHistorySource `json:"history_source,omitempty"`
	ModelData       string                     `json:"model_data,omitempty"`
	Answers         map[string]*schema.Message `json:"answers,omitempty"`
}

// SessionCancellation 保存已接受的取消或替换、请求身份与历史归属。清理成功前随原 TaskID 保留。
type SessionCancellation struct {
	Owner              string               `json:"owner,omitempty"`
	ThreadID           string               `json:"thread_id,omitempty"`
	InterruptIDs       []string             `json:"interrupt_ids"`
	HistoryKey         *agent_v2.HistoryKey `json:"history_key,omitempty"`
	CheckpointsDeleted bool                 `json:"checkpoints_deleted,omitempty"`
	Replacement        bool                 `json:"replacement,omitempty"`
}

// SessionStore 在同一共享后端保存会话记录、任务租约及图存档。key 由 Handler 生成，代表
// (owner, threadId)。LoadSession 在无记录时返回 (nil, nil)，返回值由调用方持有。
// Handler 的后台补偿可与 HTTP 请求并发调用这些方法。
type SessionStore interface {
	compose_v2.Checkpointer
	compose_v2.TaskLeaseProvider
	LoadSession(ctx context.Context, key string) (*SessionRecord, error)
	// ListCancellations 分页列举含取消意图的会话键。空 cursor 从头开始，空 next 表示本轮结束。
	// 一轮应枚举其间始终存在的取消意图，允许空页和重复键。游标独立于记录是否被删除。
	// limit 控制一页扫描量，调用方会在持有租约后重读记录。
	ListCancellations(ctx context.Context, cursor string, limit int) (keys []string, next string, err error)
	// SaveSession 整体替换记录，record 为 nil 时删除。dropTask 非空时还原子删除该任务的
	// 全部存档，并返回删除数量。写入时核对 [compose_v2.CheckpointTaskLeaseToken]
	// 对应的会话租约、record.TaskID 和 dropTask 的任务租约，缺失或过期时拒绝写入。
	SaveSession(ctx context.Context, key string, record *SessionRecord, dropTask string) (int32, error)
}

// WithSessionStore 启用共享会话、GET 挂起查询和后台取消补偿。store 同时提供 Checkpointer；
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

// sharedSession 只在持有会话租约的执行范围内使用。
type sharedSession struct {
	store              SessionStore
	key                string
	record             *SessionRecord
	settledReplacement string // 本次请求完成收尾的旧任务，供新 Run 进场复核。
}

const sharedSessionPrefix = "agui-thread:"

func sharedSessionID(key sessionKey) string {
	// owner 长度先编码，保留 (owner, threadId) 的两个身份维度。
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s%s", len(key.owner), key.owner, key.threadID))
	return sharedSessionPrefix + hex.EncodeToString(sum[:])
}

func isSharedSessionID(id string) bool {
	digest, ok := strings.CutPrefix(id, sharedSessionPrefix)
	if !ok || len(digest) != sha256.Size*2 {
		return false
	}
	var decoded [sha256.Size]byte
	_, err := hex.Decode(decoded[:], []byte(digest))
	return err == nil
}

func (h *handler) loadSharedSession(ctx context.Context, id string) (context.Context, *compose_v2.TaskActivity, error) {
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
	if record != nil && record.Cancellation != nil && !record.Closing {
		return ctx, nil, errors.New("agui: shared cancellation is missing its closing state")
	}
	shared := &sharedSession{store: h.sessionStore, key: id, record: record}
	opened = true
	return context.WithValue(ctx, sharedSessionKey{}, shared), activity, nil
}

func (h *handler) openSharedSession(ctx context.Context, key sessionKey) (context.Context, *compose_v2.TaskActivity, error) {
	ctx, activity, err := h.loadSharedSession(ctx, sharedSessionID(key))
	if err != nil {
		return ctx, nil, err
	}
	var opened bool
	defer func() {
		if !opened {
			activity.Release()
		}
	}()
	shared := sharedSessionFrom(ctx)
	if shared.record != nil && shared.record.Closing && shared.record.Cancellation == nil {
		if err := h.finishSharedSession(ctx, shared); err != nil {
			return ctx, nil, err
		}
	}
	opened = true
	return ctx, activity, nil
}

func sharedSessionFrom(ctx context.Context) *sharedSession {
	s, _ := ctx.Value(sharedSessionKey{}).(*sharedSession)
	return s
}

func (s *sharedSession) save(ctx context.Context, record *SessionRecord, dropTask string) error {
	// SSE 断开时仍需收尾。原租约令牌保留在 ctx，由后端在写入时核对。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropCPTimeout)
	defer cancel()
	_, err := s.write(ctx, record, dropTask)
	return err
}

func (s *sharedSession) write(ctx context.Context, record *SessionRecord, dropTask string) (int32, error) {
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

func (s *sharedSession) bindHistory(ctx context.Context, hosted agent_v2.TaskHistoryDestination) context.Context {
	sid, key := s.record.TaskID, s.record.HistoryKey
	if key != (agent_v2.HistoryKey{}) {
		destination := agent_v2.TaskHistoryDestination{Key: key, Source: s.record.HistorySource}
		if destination.Source == agent_v2.TaskHistoryHost {
			destination.Store = hosted.Store
		}
		return agent_v2.WithTaskHistoryBinding(ctx, sid, destination, nil)
	}
	return agent_v2.WithTaskHistoryBinding(ctx, sid, hosted, func(ctx context.Context, destination agent_v2.TaskHistoryDestination) error {
		if s.record == nil || s.record.TaskID != sid || s.record.Closing {
			return errPendingChanged
		}
		next := *s.record
		next.HistoryKey = destination.Key
		next.HistorySource = destination.Source
		return s.save(ctx, &next, "")
	})
}

func (s *sharedSession) pending(ctx context.Context) (interruptBatch, error) {
	if s.record == nil || s.record.Closing {
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
	batch.resumeBlocked = cp.Status == compose_v2.CPStatusResuming
	return batch, err
}

var (
	errAnswerAccepted          = errors.New("a different answer has already been accepted for an interrupt")
	errPendingChanged          = errors.New("pending interrupts advanced before the task could be discarded")
	errWorkflowUnavailable     = errors.New("the task's workflow version is not available on this instance")
	errHistoryResolverRequired = errors.New("a resolver is required for the task's original history store")
)

type sharedCancellationRequest struct {
	entries     []ResumeEntry
	epoch       string
	replacement bool
}

// cancelSharedSession 由会话租约保护，Abort 回调中的变更还持有旧任务的放弃许可。
func cancelSharedSession(ctx context.Context, s *sharedSession, ag agent_v2.Agent, request sharedCancellationRequest,
	resolveHistory func(context.Context, agent_v2.HistoryKey) (agent_v2.HistoryStore, error),
	options ...agent_v2.ResumeOption,
) error {
	sess := s.session()
	opts := append(options, agent_v2.WithResumeSession(sess), agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
	if saved := s.record.Cancellation; saved != nil {
		opts = append(opts, agent_v2.WithResumeAbortHistoryKey(saved.HistoryKey))
	} else if key := s.record.HistoryKey; key != (agent_v2.HistoryKey{}) {
		opts = append(opts, agent_v2.WithResumeAbortHistoryKey(&key))
	}
	if s.record.HistorySource == agent_v2.TaskHistoryHost || request.replacement || s.record.Cancellation != nil && s.record.Cancellation.Replacement {
		// 中间件提供的存储会覆盖 Agent 配置。未知来源的旧记录也需要宿主定位原存储。
		if resolveHistory == nil && s.record.HistorySource != agent_v2.TaskHistoryAgent {
			return errHistoryResolverRequired
		}
		opts = append(opts, agent_v2.WithResumeAbortAgentHistory())
	}
	if resolveHistory != nil {
		opts = append(opts, agent_v2.WithResumeAbortHistoryStore(resolveHistory))
	}
	cleanup := func(ctx context.Context, sid, ns string, key *agent_v2.HistoryKey) (int32, error) {
		if sid != sess.RunID || ns != "" || s.record == nil || s.record.TaskID != sid {
			return 0, errors.New("agui: shared abort requires the root of the current task")
		}
		if s.record.Cancellation == nil {
			if !request.replacement {
				batch, err := s.pending(ctx)
				if err != nil {
					return 0, err
				}
				if len(batch.open) == 0 || batch.epoch != request.epoch || coverageProblem(request.entries, batch.open) != "" {
					return 0, errPendingChanged
				}
			}
			next := *s.record
			next.Closing = true
			next.Cancellation = &SessionCancellation{
				Owner: compose_v2.GetOwner(ctx), ThreadID: compose_v2.GetThreadID(ctx),
				HistoryKey: key, InterruptIDs: make([]string, len(request.entries)),
				Replacement: request.replacement,
			}
			for i, e := range request.entries {
				next.Cancellation.InterruptIDs[i] = e.InterruptID
			}
			next.Prior, next.Answers, next.ModelData = nil, nil, ""
			if request.replacement {
				// 删除失败时原任务仍可续跑。删除成功后保留历史收尾进度，禁止回复原问题。
				next.Cancellation.CheckpointsDeleted = true
				return s.write(ctx, &next, sid)
			}
			if _, err := s.write(ctx, &next, ""); err != nil {
				return 0, err
			}
		}
		if s.record.Cancellation.CheckpointsDeleted {
			return 0, nil
		}
		// 与删档原子提交，历史收尾可从该状态重试。
		next := *s.record
		progress := *next.Cancellation
		progress.CheckpointsDeleted = true
		next.Cancellation = &progress
		return s.write(ctx, &next, sid)
	}
	finalize := func(ctx context.Context, sid string) error {
		if s.record == nil || s.record.TaskID != sid || s.record.Cancellation == nil || !s.record.Cancellation.CheckpointsDeleted {
			return errors.New("agui: cannot confirm cancellation before checkpoint cleanup")
		}
		// 即使没有存档，仍让后端原子核对会话及原任务的租约。
		_, err := s.write(ctx, nil, sid)
		return err
	}
	_, err := ag.Resume(ctx, sess.RunID, append(opts, agent_v2.WithResumeAbortCleanup(cleanup, finalize))...)
	if err == nil && s.record != nil {
		return errors.New("the agent did not finalize the cancelled shared session")
	}
	return err
}

// openSharedSessionAfterCancellation 让新 Run 和查询协助已接受的取消。
// 工厂构造释放会话租约，重新取得租约后只处理原任务。原记录已变化时，新 Run 交由调用方重试。
func (h *handler) openSharedSessionAfterCancellation(ctx context.Context, key sessionKey, beforeRun bool) (context.Context, *compose_v2.TaskActivity, error) {
	locked, activity, err := h.openSharedSession(ctx, key)
	if err != nil {
		return locked, nil, err
	}
	candidate := sharedSessionFrom(locked).record
	if candidate == nil || candidate.Cancellation == nil {
		return locked, activity, nil
	}
	activity.Release()
	ag, buildErr := h.buildCancellationAgent(candidate.WorkflowVersion)
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}

	locked, activity, err = h.openSharedSession(ctx, key)
	if err != nil {
		return locked, nil, err
	}
	var opened bool
	defer func() {
		if !opened {
			activity.Release()
		}
	}()
	s := sharedSessionFrom(locked)
	switch {
	case s.record == nil:
		// 别的实例可能已接管、启动并完成替换任务，空记录无法证明线程在这段时间没有推进。
		if beforeRun {
			return locked, nil, compose_v2.ErrTaskBusy
		}
	case sameCancellationTask(s.record, candidate):
		if buildErr != nil {
			return locked, nil, buildErr
		}
		if err := cancelSharedSession(locked, s, ag, sharedCancellationRequest{}, h.historyForCancellation(s)); err != nil {
			return locked, nil, err
		}
		if beforeRun && candidate.Cancellation.Replacement {
			s.settledReplacement = candidate.TaskID
		}
	case beforeRun || s.record.Cancellation != nil:
		return locked, nil, compose_v2.ErrTaskBusy
	}
	opened = true
	return locked, activity, nil
}

// openSharedRunSession 为有历史的旧任务准备原工厂。工厂可以阻塞，释放会话
// 租约后必须确认仍是准备处理的任务，才能执行替换。
func (h *handler) openSharedRunSession(ctx context.Context, key sessionKey) (context.Context, *compose_v2.TaskActivity, agent_v2.Agent, error) {
	locked, activity, err := h.openSharedSessionAfterCancellation(ctx, key, true)
	if err != nil {
		return locked, nil, nil, err
	}
	candidate := sharedSessionFrom(locked).record
	if candidate == nil || candidate.HistoryKey == (agent_v2.HistoryKey{}) {
		return locked, activity, nil, nil
	}
	activity.Release()
	ag, buildErr := h.buildCancellationAgent(candidate.WorkflowVersion)
	if err := ctx.Err(); err != nil {
		return ctx, nil, nil, err
	}
	locked, activity, err = h.openSharedSession(ctx, key)
	if err != nil {
		return locked, nil, nil, err
	}
	current := sharedSessionFrom(locked).record
	if current == nil || current.TaskID != candidate.TaskID || current.WorkflowVersion != candidate.WorkflowVersion ||
		current.Cancellation != nil || current.HistoryKey != candidate.HistoryKey || current.HistorySource != candidate.HistorySource {
		activity.Release()
		return locked, nil, nil, compose_v2.ErrTaskBusy
	}
	if buildErr != nil {
		activity.Release()
		return locked, nil, nil, buildErr
	}
	return locked, activity, ag, nil
}

func sameCancellationTask(record, candidate *SessionRecord) bool {
	return record != nil && record.Cancellation != nil && record.TaskID == candidate.TaskID &&
		record.WorkflowVersion == candidate.WorkflowVersion
}

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
	ctx, activity, err := h.openSharedSessionAfterCancellation(ctx, key, false)
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
	var version string
	if s.record != nil {
		version = s.record.WorkflowVersion
		for _, q := range batch.events {
			if s.record.Answers[q.ID] != nil {
				accepted = append(accepted, q.ID)
			}
		}
	}
	err = json.NewEncoder(w).Encode(struct {
		ThreadID      string         `json:"threadId"`
		Interrupts    []RunInterrupt `json:"interrupts"`
		Accepted      []string       `json:"acceptedInterruptIds,omitempty"`
		Version       string         `json:"workflowVersion,omitempty"`
		ResumeBlocked bool           `json:"resumeBlocked,omitempty"`
	}{threadID, batch.events, accepted, version, batch.resumeBlocked})
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
	if errors.Is(err, errWorkflowUnavailable) {
		return http.StatusConflict, "workflow_version_unavailable", errWorkflowUnavailable.Error()
	}
	slog.WarnContext(ctx, "agui: 共享会话操作失败", "err", err)
	return http.StatusServiceUnavailable, "session_store_failed", "the shared session could not be read or updated"
}

func rejectSharedSession(ctx context.Context, sse eventWriter, err error) {
	status, code, message := sharedSessionError(ctx, err)
	_ = sse.Reject(status, code, message)
}
