package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/mcp"
)

const (
	sseSessionID = "sse-test-session"
	sseVersion   = "2025-11-25"
)

type sseWireRequest struct {
	rpcReq
	Result json.RawMessage `json:"result"`
	Error  *mcp.RPCError   `json:"error"`
}

func newSSEClient(t *testing.T, serve func(http.ResponseWriter, *http.Request, sseWireRequest), opts ...mcp.HTTPOption) *mcp.StreamableHTTPClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req sseWireRequest
		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		if req.Method == "initialize" {
			w.Header().Set("Mcp-Session-Id", sseSessionID)
			writeResult(w, req.ID, map[string]any{"protocolVersion": sseVersion})
			return
		}
		if r.Header.Get("Mcp-Session-Id") != sseSessionID || r.Header.Get("MCP-Protocol-Version") != sseVersion {
			t.Error("请求未带本次握手的会话和协议版本")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch {
		case req.Method == "notifications/initialized", r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)
		case req.Method == "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "write"}}})
		default:
			serve(w, r, req)
		}
	}))
	t.Cleanup(srv.Close)
	c, err := mcp.NewStreamableHTTPClient("events", srv.URL, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() }) // 测试已断言业务出口，清理不覆盖失败原因。
	return c
}

func TestStreamableHTTP_ResumesToolReplyWithoutReposting(t *testing.T) {
	var writes, gets atomic.Int32
	var requestID atomic.Int64
	c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, req sseWireRequest) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		switch r.Method {
		case http.MethodPost:
			writes.Add(1)
			requestID.Store(req.ID)
			if req.Method != "tools/call" || r.Header.Get("Last-Event-ID") != "" {
				t.Errorf("工具 POST 含错误的方法或旧游标，method=%s", req.Method)
			}
			_, _ = fmt.Fprint(w, "id: write-received\r\nretry: 1\r\ndata:\r\n\r\n")
			_, _ = fmt.Fprint(w, "id: incomplete\ndata: {\"jsonrpc\":\"2.0\"\nretry: 10000")
		case http.MethodGet:
			if r.Header.Get("Accept") != "text/event-stream" || r.Header.Get("Authorization") != "Bearer test-auth" {
				t.Error("重连 GET 未携带正确的 Accept 和认证头")
			}
			switch n := gets.Add(1); n {
			case 1:
				if r.Header.Get("Last-Event-ID") != "write-received" {
					t.Error("游标必须停在空 data 引导事件，不能越过未完成的帧")
				}
				_, _ = fmt.Fprint(w, "id: poll-finished\ndata: {\"id\":0,\"result\":{\"content\":[]}}\n\n")
			case 2:
				if r.Header.Get("Last-Event-ID") != "poll-finished" {
					t.Error("第二次续订未使用同一流的最新游标")
				}
				_, _ = fmt.Fprintf(w, "data: {\"id\":%d,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"saved\"}]}}\n\n", requestID.Load())
			default:
				t.Errorf("多余的续订 GET %d", n)
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			t.Errorf("意外请求 %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}, mcp.WithHTTPHeader("Authorization", "Bearer test-auth"), mcp.WithHTTPHeader("Last-Event-ID", "old-stream"))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() }) // 业务断言在关闭前进行。
	res, err := h.Toolkit().Call(ctx, "mcp_events_write", nil)
	if err != nil || res == nil || !res.Success || res.Output != "saved" || writes.Load() != 1 || gets.Load() != 2 {
		t.Fatalf("应取回唯一一次写入的响应，实际 result=%+v err=%v writes=%d gets=%d", res, err, writes.Load(), gets.Load())
	}
}

func TestStreamableHTTP_ResumeRejectionDoesNotClaimToolWasSkipped(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var writes, gets atomic.Int32
			c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, req sseWireRequest) {
				if r.Method == http.MethodGet {
					gets.Add(1)
					http.Error(w, "resume rejected", status)
					return
				}
				if req.Method != "tools/call" {
					t.Errorf("意外 RPC %s", req.Method)
				}
				writes.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "id: write-received\nretry: 1\ndata:\n\n")
			})
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			h := mcp.NewHub(mcp.WithClient(c))
			if err := h.Start(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() }) // 业务断言在关闭前进行。
			res, err := h.Toolkit().Call(ctx, "mcp_events_write", nil)
			if err != nil || res == nil || res.Success || res.Error == nil || writes.Load() != 1 || gets.Load() != 1 {
				t.Fatalf("续订失败应保留一次已发出的调用，实际 result=%+v err=%v writes=%d gets=%d", res, err, writes.Load(), gets.Load())
			}
			if !strings.Contains(res.Error.Message, "不确定是否已执行") || strings.Contains(res.Error.Message, "没有执行") {
				t.Fatalf("GET 被拒不能证明工具 POST 未执行，模型收到 %+v", res.Error)
			}
		})
	}
}

func TestStreamableHTTP_SubscribeAndToolReplyUseSeparateCursors(t *testing.T) {
	toolStarted := make(chan struct{})
	firstPush := make(chan struct{})
	stop := errors.New("two pushes received")
	var toolID atomic.Int64
	var writes, pushes, pings atomic.Int32
	c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, req sseWireRequest) {
		if r.Method == http.MethodPost {
			if req.Method == "" && req.ID == 9001 {
				if req.JSONRPC != "2.0" || string(req.Result) != "{}" || req.Error != nil {
					t.Errorf("ping 应答不是 JSON-RPC success: %+v", req)
				}
				pings.Add(1)
				w.WriteHeader(http.StatusAccepted)
				return
			}
			if req.Method != "tools/call" {
				t.Errorf("意外 RPC %q", req.Method)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			toolID.Store(req.ID)
			if writes.Add(1) == 1 {
				close(toolStarted)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "id: tool-cursor\nretry: 1\ndata:\n\n")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		switch cursor := r.Header.Get("Last-Event-ID"); cursor {
		case "":
			if pushes.Add(1) != 1 {
				t.Error("订阅在断线后丢失游标")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprint(w, "\xef\xbb\xbfretry: 1\rid: push-cursor\rdata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\",\"params\":{\"revision\":1}}\r\r")
			w.(http.Flusher).Flush()
			select {
			case <-toolStarted:
			case <-r.Context().Done():
			}
		case "tool-cursor":
			_, _ = fmt.Fprintf(w, "data: {\"id\":%d,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"tool reply\"}]}}\n\n", toolID.Load())
		case "push-cursor":
			pushes.Add(1)
			_, _ = fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"id\":9001,\"method\":\"ping\"}\n\n")
			_, _ = fmt.Fprint(w, "id: second-push\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"revision\":2}}\n\n")
		default:
			t.Errorf("订阅使用了错误的游标 %q", cursor)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	if err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	notifications := make(chan mcp.Notification, 4)
	ended := make(chan error, 1)
	go func() {
		ended <- c.Subscribe(ctx, func(_ context.Context, notice mcp.Notification) error {
			notifications <- notice
			if notice.Method == "notifications/tools/list_changed" {
				close(firstPush)
				return nil
			}
			return stop
		})
	}()
	select {
	case <-firstPush:
	case err := <-ended:
		t.Fatalf("等首条推送时订阅结束: %v", err)
	case <-ctx.Done():
		t.Fatal("独立 GET 首条推送未在流保持打开时交付")
	}
	res, err := c.CallTool(ctx, "write", nil)
	if err != nil || res == nil || mcp.FirstText(res.Content) != "tool reply" || writes.Load() != 1 {
		t.Fatalf("并发的工具回复应走自己的流，实际 res=%+v err=%v writes=%d", res, err, writes.Load())
	}
	select {
	case err := <-ended:
		if !errors.Is(err, stop) {
			t.Fatalf("订阅应返回回调的停止原因，实际 %v", err)
		}
	case <-ctx.Done():
		t.Fatal("订阅未退出")
	}
	if len(notifications) != 2 || pings.Load() != 1 || pushes.Load() != 2 {
		t.Fatalf("应依序收到两条推送并确认一次 ping，实际 notifications=%d pings=%d gets=%d", len(notifications), pings.Load(), pushes.Load())
	}
	first, second := <-notifications, <-notifications
	if first.Method != "notifications/tools/list_changed" || string(first.Params) != `{"revision":1}` ||
		second.Method != "notifications/message" || string(second.Params) != `{"revision":2}` {
		t.Fatalf("推送的方法或参数未按原顺序保留: %+v / %+v", first, second)
	}
}

func TestStreamableHTTP_OptionalGETCanBeRejected(t *testing.T) {
	c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, req sseWireRequest) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeResult(w, req.ID, map[string]any{"content": []map[string]any{{"type": "text", "text": "ok"}}})
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, func(context.Context, mcp.Notification) error {
		t.Error("拒绝 GET 时不应交付通知")
		return nil
	}); err == nil || !strings.Contains(err.Error(), "405") {
		t.Fatalf("订阅应报告 HTTP 405，实际 %v", err)
	}
	res, err := c.CallTool(ctx, "write", nil)
	if err != nil || res == nil || mcp.FirstText(res.Content) != "ok" {
		t.Fatalf("POST 应仍可使用，实际 %+v, %v", res, err)
	}
}

func TestStreamableHTTP_CloseCancelsIdleGET(t *testing.T) {
	started, disconnected := make(chan struct{}), make(chan struct{})
	var gets atomic.Int32
	c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, _ sseWireRequest) {
		if gets.Add(1) != 1 {
			t.Error("Close 后不得为旧流重试")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "id: idle-stream\ndata:\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(disconnected)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ended := make(chan error, 1)
	go func() {
		ended <- c.Subscribe(ctx, func(context.Context, mcp.Notification) error { return nil })
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("GET 流未启动")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) || gets.Load() != 1 {
			t.Fatalf("Close 应打断原订阅且不重试，实际 err=%v GET=%d", err, gets.Load())
		}
	case <-ctx.Done():
		t.Fatal("关闭后订阅还在等待")
	}
	select {
	case <-disconnected:
	case <-ctx.Done():
		t.Fatal("关闭后服务端仍持有 GET 流")
	}
}

func TestStreamableHTTP_ResumeWaitUsesOriginalDeadline(t *testing.T) {
	var writes, gets atomic.Int32
	c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, _ sseWireRequest) {
		if r.Method == http.MethodGet {
			gets.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writes.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "id: write-received\nretry: 10000\ndata:\n\n")
	})
	if err := c.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	_, err := c.CallTool(ctx, "write", nil)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, mcp.ErrNotSent) || writes.Load() != 1 || gets.Load() != 0 {
		t.Fatalf("原调用 deadline 结束时还在等 retry，实际 err=%v writes=%d GET=%d", err, writes.Load(), gets.Load())
	}
}

func TestStreamableHTTP_SubscribeStopInsideBatch(t *testing.T) {
	for _, stop := range []string{"cancel", "close"} {
		t.Run(stop, func(t *testing.T) {
			c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, _ sseWireRequest) {
				if r.Method != http.MethodGet {
					t.Errorf("意外请求 %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "data: [{\"method\":\"notifications/first\"},{\"method\":\"notifications/second\"}]\n\n")
			})
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var seen []string
			err := c.Subscribe(ctx, func(_ context.Context, notification mcp.Notification) error {
				seen = append(seen, notification.Method)
				if stop == "close" {
					return c.Close()
				}
				cancel()
				return nil
			})
			if !errors.Is(err, context.Canceled) || len(seen) != 1 || seen[0] != "notifications/first" {
				t.Fatalf("中止后同一帧不得继续派发，实际 seen=%v err=%v", seen, err)
			}
		})
	}
}

func TestStreamableHTTP_SubscribeCallbackToolErrorStopsDelivery(t *testing.T) {
	var pushes, writes atomic.Int32
	c := newSSEClient(t, func(w http.ResponseWriter, r *http.Request, _ sseWireRequest) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.Method == http.MethodGet {
			if pushes.Add(1) > 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprint(w, "id: before-notice\nretry: 1\ndata:\n\n")
			_, _ = fmt.Fprint(w, "id: notice\ndata: {\"method\":\"notifications/write\"}\n\n")
			return
		}
		writes.Add(1)
		// 工具已写入且没有提供它的响应游标，本次工具调用无法续订。
		_, _ = fmt.Fprint(w, ": tool stream has no cursor\n\n")
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := c.Subscribe(ctx, func(ctx context.Context, _ mcp.Notification) error {
		_, err := c.CallTool(ctx, "write", nil)
		return err
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) || pushes.Load() != 1 || writes.Load() != 1 {
		t.Fatalf("回调错误必须直接结束订阅，实际 err=%v pushes=%d writes=%d", err, pushes.Load(), writes.Load())
	}
}
