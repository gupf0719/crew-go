package agui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// SSEWriter 把 AG-UI Event 编码成 Server-Sent Events 写入 http.ResponseWriter，
// 每次 WriteEvent 后 flush 让前端 EventSource 实时收到。
//
// **并发安全**——内部 mu 串行化写 frame + flush，多 goroutine 可安全并发调 WriteEvent
// （handler 发 run 边界 + adapter 并发发 chunk 等场景不依赖 caller 保证单写者，self-safe）。
type SSEWriter struct {
	mu      sync.Mutex // 串行化写 w + flush
	w       http.ResponseWriter
	flusher http.Flusher
}

// NewSSEWriter 构造 SSEWriter 并写出 SSE 响应头（text/event-stream + no-cache），
// 立即 flush 让前端 onopen 触发。
//
// 若 w 不支持 http.Flusher（SSE 流式必需），返回 (nil, false)——caller 应回 500。
func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, false
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // 防 nginx 等反代缓冲流
	flusher.Flush()                  // 立即刷响应头
	return &SSEWriter{w: w, flusher: flusher}, true
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
	if _, err := fmt.Fprintf(s.w, "id: %s_%d\ndata: %s\n\n", ev.Type(), ev.timestampMs(), payload); err != nil {
		return fmt.Errorf("agui: write sse frame: %w", err)
	}
	s.flusher.Flush()
	return nil
}
