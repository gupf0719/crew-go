package agui

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"crew-go/internal/panicerr"
)

// RunStatus 是一次 HTTP Run 或 Resume 的状态。一次任务可经多次 HTTP Run 挂起和续跑。
type RunStatus string

const (
	RunRunning     RunStatus = "running"
	RunSucceeded   RunStatus = "success"
	RunInterrupted RunStatus = "interrupt"
	RunCancelled   RunStatus = "cancelled"
	RunFailed      RunStatus = "error"
	RunRejected    RunStatus = "rejected"
)

// ErrRunClosed 表示事件日志已收尾，或写入者的令牌不再对应当前记录。
var ErrRunClosed = errors.New("agui: run journal is closed")

// RunRecord 是一次 HTTP 请求的回执。Deadline 是允许写入终态的截止时间。
type RunRecord struct {
	Token       string
	Digest      string
	Deadline    time.Time
	Status      RunStatus
	LastEventID int64
	HTTPStatus  int             // 开流前拒绝时保存原 HTTP 状态码。
	Terminal    json.RawMessage // 终态 AG-UI 事件。
}

// RunFrame 是日志中的一帧，ID 在同一 Run 内从 1 起连续递增。
type RunFrame struct {
	ID   int64
	Data json.RawMessage
}

// RunStore 保存回执和事件。key 由 Handler 按 (owner, threadId, runId) 派生。
// 各实例必须共享同一份记录。返回的记录和帧由调用方持有。
type RunStore interface {
	// ClaimRun 原子创建 running 回执。已有记录时返回原回执和 false。
	ClaimRun(ctx context.Context, key, digest string, deadline time.Time) (*RunRecord, bool, error)
	// LoadRun 找不到时返回 (nil, nil)。
	LoadRun(ctx context.Context, key string) (*RunRecord, error)
	// AppendRunEvent 核对令牌与 running 状态，原子分配序号、保存帧并更新回执。
	// 非终态用 RunRunning；开流前拒绝用 RunRejected 并传入原 HTTP 状态码。
	// 终态是最后一帧，提交后所有写入返回 ErrRunClosed。
	AppendRunEvent(ctx context.Context, key, token string, data json.RawMessage, status RunStatus, httpStatus int) error
	// ReadRunEvents 按序号升序返回 (after, through] 区间内至多 limit 帧。
	ReadRunEvents(ctx context.Context, key, token string, after, through int64, limit int) ([]RunFrame, error)
}

// WithRunStore 启用持久 Run 回执和 SSE 续传。需同时配置 [WithSessionStore]。
// 相同请求用原 threadId 和 runId 重试时从日志读结果。日志包含原始 AG-UI 事件。
// timeout 限制单次 Run 或 Resume 的执行时间，零值采用五分钟。订阅断开后执行继续。
func WithRunStore(store RunStore, timeout time.Duration) Option {
	if nilInterface(store) || timeout < 0 {
		panic("agui: WithRunStore requires a run store and non-negative timeout")
	}
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	return func(h *handler) {
		h.runStore = store
		h.runTimeout = timeout
	}
}

type eventWriter interface {
	WriteEvent(Event) error
	Reject(status int, code, message string) error
}

// journalWriter 串行提交事件。HTTP 订阅独立读取日志，写入故障通过取消通知执行方。
type journalWriter struct {
	mu       sync.Mutex
	append   func(json.RawMessage, RunStatus, int) error
	cancel   context.CancelFunc
	opened   bool
	terminal *journalTerminal
	err      error
}

type journalTerminal struct {
	data       json.RawMessage
	status     RunStatus
	httpStatus int
}

func (w *journalWriter) WriteEvent(ev Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	status := RunRunning
	switch terminal := ev.(type) {
	case *RunFinishedEvent:
		status = RunSucceeded
		if terminal.Outcome != nil {
			status = RunStatus(terminal.Outcome.Type)
		}
	case *RunErrorEvent:
		status = RunFailed
	}
	return w.write(ev, status, 0)
}

func (w *journalWriter) Reject(status int, code, message string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.opened {
		return w.write(NewRunError(message, code), RunFailed, 0)
	}
	return w.write(NewRunError(message, code), RunRejected, status)
}

// write 由持锁方调用。
func (w *journalWriter) write(ev Event, status RunStatus, httpStatus int) error {
	if w.terminal != nil {
		return ErrRunClosed
	}
	if w.err != nil {
		return w.err
	}
	data, err := encodeEvent(ev)
	if err == nil {
		if status != RunRunning {
			// 让 serveRequest 的 defer 先释放会话和任务租约，客户端收到终帧便可提交下一次 Resume。
			w.terminal = &journalTerminal{data, status, httpStatus}
			return nil
		}
		err = w.append(data, status, httpStatus)
	}
	if err != nil {
		w.err = err
		w.cancel()
		return err
	}
	w.opened = true
	return nil
}

func (w *journalWriter) finish(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if errors.Is(w.err, ErrRunClosed) {
		return
	}
	terminal := w.terminal
	if terminal == nil {
		ev := NewRunError("run exited without a terminal event", "agent_error")
		if w.err != nil {
			slog.WarnContext(ctx, "agui: Run 事件保存失败", "err", w.err)
			ev = NewRunError("run events could not be saved", "run_store_failed")
		}
		data, err := encodeEvent(ev)
		if err != nil {
			slog.WarnContext(ctx, "agui: Run 错误事件编码失败", "err", err)
			return
		}
		terminal = &journalTerminal{data: data, status: RunFailed}
	}
	if err := w.append(terminal.data, terminal.status, terminal.httpStatus); err != nil && !errors.Is(err, ErrRunClosed) {
		slog.WarnContext(ctx, "agui: Run 终态保存失败", "err", err)
	}
}

func runJournalKey(key sessionKey, runID string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s%d:%s%s", len(key.owner), key.owner, len(key.threadID), key.threadID, runID))
	return fmt.Sprintf("%x", sum)
}

// 摘要只忽略 JSON 空白，保留成员顺序、媒体和显式 null。
func runDigest(raw json.RawMessage) (string, error) {
	var body bytes.Buffer
	if err := json.Compact(&body, raw); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(body.Bytes())), nil
}

func runCursor(r *http.Request) (int64, error) {
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		raw = r.URL.Query().Get("after")
	}
	if raw == "" {
		return 0, nil
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 0 {
		return 0, errors.New("event cursor must be a non-negative integer")
	}
	return id, nil
}

func (h *handler) serveTrackedRun(ctx context.Context, sse *SSEWriter, r *http.Request, in *RunAgentInput, session sessionKey, raw json.RawMessage) {
	after, err := runCursor(r)
	if err != nil {
		_ = sse.Reject(http.StatusBadRequest, "invalid_cursor", err.Error())
		return
	}
	if in.RunID == "" {
		in.RunID = rand.Text()
	}
	key := runJournalKey(session, in.RunID)
	digest, err := runDigest(raw)
	if err != nil {
		_ = sse.Reject(http.StatusBadRequest, "bad_request", "run body could not be fingerprinted")
		return
	}
	deadline := time.Now().Add(h.runTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}

	var record *RunRecord
	var claimed bool
	if after > 0 {
		record, err = h.loadRun(ctx, key)
	} else {
		if ctx.Err() != nil {
			return
		}
		// 从认领开始，断开订阅不影响已接收的工作。收尾期限留给租约清理和终态写入。
		claimCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), dropCPTimeout)
		record, claimed, err = h.runStore.ClaimRun(claimCtx, key, digest, deadline.Add(2*dropCPTimeout))
		stop()
	}
	if err != nil {
		rejectRunStore(ctx, sse, err)
		return
	}
	if record == nil {
		_ = sse.Reject(http.StatusNotFound, "run_not_found", "run was not found")
		return
	}
	if record.Digest != digest {
		_ = sse.Reject(http.StatusConflict, "run_id_conflict", "run id already belongs to a different request")
		return
	}
	if !claimed {
		h.replayRun(ctx, sse, key, record.Token, after, true, nil)
		return
	}

	runCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	writer := &journalWriter{
		cancel: cancel,
		append: func(data json.RawMessage, status RunStatus, httpStatus int) error {
			// 运行超时后，adapter 仍要写完消息结束帧，再提交终态。
			writeCtx, stop := context.WithTimeout(context.WithoutCancel(runCtx), dropCPTimeout)
			defer stop()
			return h.runStore.AppendRunEvent(writeCtx, key, record.Token, data, status, httpStatus)
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		defer func() {
			if rec := recover(); rec != nil {
				panicerr.Log(runCtx, slog.LevelError, "agui: Run 日志生产者异常", panicerr.New(rec))
			}
		}()
		defer writer.finish(runCtx)
		h.serveRequest(runCtx, writer, in, session)
	}()
	// HTTP Server 的优雅关闭仍会等待原始 Handler 收尾，订阅只管理自己的连接。
	defer func() { <-done }()
	h.replayRun(ctx, sse, key, record.Token, after, true, done)
}

func (h *handler) loadRun(ctx context.Context, key string) (*RunRecord, error) {
	record, err := h.runStore.LoadRun(ctx, key)
	if err != nil || record == nil || record.Status != RunRunning || time.Now().Before(record.Deadline) {
		return record, err
	}
	data, err := encodeEvent(NewRunError("execution stopped reporting before a terminal result was saved", "run_interrupted"))
	if err != nil {
		return nil, err
	}
	if err := h.runStore.AppendRunEvent(ctx, key, record.Token, data, RunFailed, 0); err != nil && !errors.Is(err, ErrRunClosed) {
		return nil, err
	}
	return h.runStore.LoadRun(ctx, key)
}

func (h *handler) serveRunQuery(w http.ResponseWriter, r *http.Request, runID string) {
	threadID := r.URL.Query().Get("threadId")
	if threadID == "" {
		_ = writeReject(w, http.StatusBadRequest, "no_thread_id", "run query requires threadId")
		return
	}
	ctx, session := h.requestContext(r, threadID)
	key := runJournalKey(session, runID)
	record, err := h.loadRun(ctx, key)
	if err != nil {
		_ = writeReject(w, http.StatusServiceUnavailable, "run_store_failed", "run journal could not be read")
		slog.WarnContext(ctx, "agui: Run 回执读取失败", "err", err)
		return
	}
	if record == nil {
		_ = writeReject(w, http.StatusNotFound, "run_not_found", "run was not found")
		return
	}
	if acceptsEventStream(r.Header.Get("Accept")) || r.Header.Get("Last-Event-ID") != "" || r.URL.Query().Has("after") {
		sse, ok := NewSSEWriter(w)
		if !ok {
			_ = writeReject(w, http.StatusInternalServerError, "streaming_unsupported", "agui: streaming unsupported")
			return
		}
		after, err := runCursor(r)
		if err != nil {
			_ = sse.Reject(http.StatusBadRequest, "invalid_cursor", err.Error())
			return
		}
		h.replayRun(ctx, sse, key, record.Token, after, false, nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(struct {
		ThreadID    string          `json:"threadId"`
		RunID       string          `json:"runId"`
		Status      RunStatus       `json:"status"`
		LastEventID int64           `json:"lastEventId"`
		HTTPStatus  int             `json:"httpStatus,omitempty"`
		Terminal    json.RawMessage `json:"terminal,omitempty"`
	}{threadID, runID, record.Status, record.LastEventID, record.HTTPStatus, record.Terminal}); err != nil {
		slog.DebugContext(ctx, "agui: Run 回执响应未送达", "err", err)
	}
}

func acceptsEventStream(accept string) bool {
	for part := range strings.SplitSeq(accept, ",") {
		mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err == nil && mediaType == "text/event-stream" {
			return true
		}
	}
	return false
}

func (h *handler) replayRun(ctx context.Context, sse *SSEWriter, key, token string, after int64, post bool, producer <-chan struct{}) {
	const pageSize = 128
	sse.replay = true
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	producerDone := false
	for ctx.Err() == nil {
		record, err := h.loadRun(ctx, key)
		if err != nil {
			rejectRunStore(ctx, sse, err)
			return
		}
		if record == nil || record.Token != token {
			_ = sse.Reject(http.StatusNotFound, "run_not_found", "run was not found")
			return
		}
		if after > record.LastEventID {
			_ = sse.Reject(http.StatusBadRequest, "invalid_cursor", "event cursor is ahead of the run journal")
			return
		}
		if post && record.HTTPStatus != 0 {
			var ev RunErrorEvent
			if err := json.Unmarshal(record.Terminal, &ev); err != nil {
				rejectRunStore(ctx, sse, err)
				return
			}
			_ = sse.Reject(record.HTTPStatus, ev.Code, ev.Message)
			return
		}
		if after < record.LastEventID {
			frames, err := h.runStore.ReadRunEvents(ctx, key, token, after, record.LastEventID, pageSize)
			if err != nil {
				rejectRunStore(ctx, sse, err)
				return
			}
			if len(frames) == 0 {
				rejectRunStore(ctx, sse, fmt.Errorf("missing run events after %d", after))
				return
			}
			for _, frame := range frames {
				if frame.ID != after+1 {
					rejectRunStore(ctx, sse, fmt.Errorf("non-contiguous run event %d after %d", frame.ID, after))
					return
				}
				if err := sse.writeFrame(strconv.FormatInt(frame.ID, 10), frame.Data); err != nil {
					return
				}
				after = frame.ID
			}
			continue
		}
		if record.Status != RunRunning {
			sse.openStream()
			return
		}
		if producerDone {
			rejectRunStore(ctx, sse, errors.New("producer exited before a terminal event was saved"))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-producer:
			producerDone = true
			producer = nil
		}
	}
}

func rejectRunStore(ctx context.Context, sse *SSEWriter, err error) {
	if ctx.Err() != nil {
		return
	}
	slog.WarnContext(ctx, "agui: Run 日志操作失败", "err", err)
	_ = sse.Reject(http.StatusServiceUnavailable, "run_store_failed", "the run journal could not be read or updated")
}
