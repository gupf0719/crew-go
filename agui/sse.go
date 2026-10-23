package agui

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// errRejected：这次请求在开流之前已经按 HTTP 错误码拒掉，不再写事件。
var errRejected = errors.New("agui: request already rejected before the stream opened")

// SSEWriter 把 AG-UI Event 编码成 Server-Sent Events 写入 http.ResponseWriter，
// 每次 WriteEvent 后 flush 让前端 EventSource 实时收到。
//
// **并发安全**——内部 mu 串行化写 frame + flush，多 goroutine 可安全并发调 WriteEvent
// （handler 发 run 边界 + adapter 并发发 chunk 等场景不依赖 caller 保证单写者，self-safe）。
//
// 事件流在写出第一个事件时才开（那时才发响应头）：开流之前还能用 [SSEWriter.Reject] 回 HTTP 错误码。
type SSEWriter struct {
	mu       sync.Mutex // 串行化写 w + flush
	w        http.ResponseWriter
	flusher  http.Flusher
	opened   bool // 响应已开始（事件流已开，或已经按 HTTP 错误码拒掉）
	rejected bool // 开流之前已经拒掉：之后不再写事件
}

// NewSSEWriter 构造 SSEWriter，设好 SSE 响应头（text/event-stream + no-cache）。响应头随第一个事件一起发出：
// 开流之前被拒的请求（坏输入、会话正忙……）要回 HTTP 错误码、不开事件流（AG-UI HTTP + SSE 绑定：「开始之前被拒
// 的输入是 HTTP 错误码，没有事件流」），见 [SSEWriter.Reject]。
//
// 若 w 不支持 http.Flusher（SSE 流式必需），返回 (nil, false)——caller 应回 500。
func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, false
	}
	return &SSEWriter{w: w, flusher: flusher}, true
}

// openLocked 发出 SSE 响应头、开流。调用方须持锁。
func (s *SSEWriter) openLocked() {
	if s.opened {
		return
	}
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // 防 nginx 等反代缓冲流
	s.w.WriteHeader(http.StatusOK)
	s.opened = true
}

// rejectBody 是开流之前拒绝请求时的响应体。
type rejectBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Reject 拒绝这次请求：还没开流时回 status 与 JSON 体 {"code", "message"}、不开事件流（这次 run 没有开始）；已经
// 开流时（响应码早已发出、改不了）退化成流内的 RUN_ERROR。
func (s *SSEWriter) Reject(status int, code, message string) error {
	s.mu.Lock()
	if !s.opened {
		defer s.mu.Unlock()
		s.opened = true // 响应已定：之后的事件不再写（见 WriteEvent）
		s.rejected = true
		return writeReject(s.w, status, code, message)
	}
	s.mu.Unlock()
	return s.WriteEvent(NewRunError(message, code))
}

// writeReject 写开流之前的拒绝：status + JSON 体 {"code", "message"}（还没有 SSEWriter 的那几处也用它，形状一致）。
func writeReject(w http.ResponseWriter, status int, code, message string) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(rejectBody{Code: code, Message: message}); err != nil {
		return fmt.Errorf("agui: write reject body: %w", err)
	}
	return nil
}

// WriteEvent 把一个 AG-UI 事件编码成 SSE frame 写出并 flush。
//
// wire 格式（对齐 ag-ui SSE 编码）：
//
//	id: {TYPE}_{TIMESTAMP_MS}
//	data: {JSON}
//	<空行>
//
// data 是单行 JSON——encoding/json 保证输出不含裸换行（字符串里的 \n 被编码成 `\n`
// 字面），故 SSE data 行安全。事件未设时间戳时补当前毫秒（AG-UI 约定 Unix ms）。
func (s *SSEWriter) WriteEvent(ev Event) error {
	if ev.timestampMs() == 0 {
		ev.setTimestampMs(time.Now().UnixMilli())
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("agui: marshal event: %w", err)
	}
	// 锁只护共享的 w + flusher；timestamp/marshal 作用于独立 ev，放锁外减小临界区。
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejected {
		return errRejected
	}
	s.openLocked()
	if _, err := fmt.Fprintf(s.w, "id: %s_%d\ndata: %s\n\n", ev.Type(), ev.timestampMs(), payload); err != nil {
		return fmt.Errorf("agui: write sse frame: %w", err)
	}
	s.flusher.Flush()
	return nil
}
