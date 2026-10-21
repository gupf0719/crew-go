package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// uiEvent 是推送到浏览器的单条事件。Type 固定小写 snake_case，Data 内部字段
// 由各 event kind 自定义（见 hooks.go / main.go 的 push 调用点）。
type uiEvent struct {
	Type string         `json:"type"`
	TS   int64          `json:"ts"`
	Data map[string]any `json:"data,omitempty"`
}

// eventSink 是 hook 层与 SSE writer 的单向通道。
// 容量 256 已足够单次 PlanAgent 运行——hook 端满了就 drop 并记 warn（见 send），
// 保证 demo 不会因为浏览器慢消费而阻塞后端。
type eventSink struct {
	ch     chan uiEvent
	closed chan struct{}
	once   sync.Once
}

func newEventSink() *eventSink {
	return &eventSink{
		ch:     make(chan uiEvent, 256),
		closed: make(chan struct{}),
	}
}

// send 非阻塞投递一条事件。通道满或已关闭时返回 false（调用方无需处理，
// demo 场景下只会在极端背压下触发）。
func (s *eventSink) send(kind string, data map[string]any) bool {
	ev := uiEvent{Type: kind, TS: time.Now().UnixNano() / 1e6, Data: data}
	select {
	case <-s.closed:
		return false
	case s.ch <- ev:
		return true
	default:
		// 通道满——丢弃并吞掉，避免阻塞 agent 主流程。
		return false
	}
}

// close 幂等关闭。SSE handler 完整写出完后调用。
func (s *eventSink) close() {
	s.once.Do(func() { close(s.closed) })
}

// writeSSE 把事件持续写入 http response，直到 ctx 取消或 sink 关闭。
// 单 writer 单 response，调用方负责保证不被并发调用。
func writeSSE(w http.ResponseWriter, r *http.Request, sink *eventSink) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // 防 nginx 缓冲
	// 立即刷出响应头，让前端 EventSource.onopen 马上触发
	flusher.Flush()

	ctx := r.Context()
	// 心跳，防中间件因为长时间没数据误判连接超时。demo 粒度 15s 够用。
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sink.closed:
			// 把 sink 里剩余已入队事件排空再退出
			for {
				select {
				case ev := <-sink.ch:
					writeOne(w, flusher, ev)
				default:
					return
				}
			}
		case ev := <-sink.ch:
			writeOne(w, flusher, ev)
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func writeOne(w http.ResponseWriter, f http.Flusher, ev uiEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		// 编码错误不对外抛，写个 server-level 错误事件以便前端看得到
		payload, _ = json.Marshal(uiEvent{Type: "error", TS: ev.TS, Data: map[string]any{"message": "marshal: " + err.Error()}})
	}
	// SSE 协议：每条事件以 "data: <json>\n\n" 结尾。type 字段由 JSON 自身携带，
	// 不用 SSE 原生的 event: 字段——前端统一 onmessage 就能拿到。
	fmt.Fprintf(w, "data: %s\n\n", payload)
	f.Flush()
}
