package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/mcp"
)

func TestStreamableHTTP_SSEAndSessionLifecycle(t *testing.T) {
	const sessionID = "fake-session-token"
	const version = "2025-06-18"
	var initialized, deleted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if accept := r.Header.Get("Accept"); !strings.Contains(accept, "application/json") || !strings.Contains(accept, "text/event-stream") {
			t.Errorf("Accept 缺少 JSON 或 SSE，实际 %q", accept)
		}
		if r.Method == http.MethodDelete {
			deleted.Store(r.Header.Get("Mcp-Session-Id") == sessionID && r.Header.Get("MCP-Protocol-Version") == version)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var req struct {
			Method string          `json:"method"`
			ID     json.RawMessage `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Method == "initialize" {
			if r.Header.Get("Mcp-Session-Id") != "" {
				t.Error("initialize 不应带旧 session")
			}
			w.Header().Set("Mcp-Session-Id", sessionID)
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"protocolVersion\":%q}}\n\n", req.ID, version)
			return
		}
		if r.Header.Get("Mcp-Session-Id") != sessionID || r.Header.Get("MCP-Protocol-Version") != version {
			t.Error("后续请求应携带协商版本和会话")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch req.Method {
		case "notifications/initialized":
			if len(req.ID) != 0 {
				t.Error("通知应无 JSON-RPC id")
			}
			initialized.Store(true)
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			if !initialized.Load() {
				t.Error("调用前必须通知服务端已初始化")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, ": heartbeat\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n")
			_, _ = fmt.Fprintf(w, "event: message\ndata: [{\"id\":0,\"result\":{}},\ndata: {\"id\":%s,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}}]\n\n", req.ID)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client, err := mcp.NewStreamableHTTPClient("session", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.CallTool(ctx, "lookup", nil)
	if err != nil || res == nil || len(res.Content) != 1 || res.Content[0].Text != "ok" {
		t.Fatalf("应从 SSE 取得匹配的响应并及时关闭连接，实际 %+v，错误 %v", res, err)
	}
	if err := client.Close(); err != nil || !deleted.Load() {
		t.Fatalf("Close 应终止正确的远端会话，实际 %v", err)
	}
}

func TestStreamableHTTP_ExpiredSessionReinitializesWithoutReplayingTool(t *testing.T) {
	var handshakes, calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		id := r.Header.Get("Mcp-Session-Id")
		switch req.Method {
		case "initialize":
			if id != "" {
				t.Errorf("重建会话时仍发送旧 ID %q", id)
			}
			w.Header().Set("Mcp-Session-Id", fmt.Sprintf("session-%d", handshakes.Add(1)))
			writeResult(w, req.ID, map[string]any{"protocolVersion": "2025-03-26"})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			calls.Add(1)
			if id == "session-1" {
				http.Error(w, "expired session-1", http.StatusNotFound)
				return
			}
			if id != "session-2" {
				t.Errorf("工具调用应使用新会话，实际 %q", id)
			}
			writeResult(w, req.ID, map[string]any{"content": []any{}})
		}
	}))
	t.Cleanup(srv.Close)
	client, err := mcp.NewStreamableHTTPClient("session", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CallTool(t.Context(), "write", nil)
	if err == nil || !strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "session-1") || calls.Load() != 1 {
		t.Fatalf("过期请求应报告 404、脱敏且只尝试一次，calls=%d，错误 %v", calls.Load(), err)
	}
	if _, err := client.CallTool(t.Context(), "write", nil); err != nil || handshakes.Load() != 2 || calls.Load() != 2 {
		t.Fatalf("下次请求应创建新会话并正常运行，handshakes=%d calls=%d，错误 %v", handshakes.Load(), calls.Load(), err)
	}
}

func TestStreamableHTTP_InitializationFailureReleasesServerSession(t *testing.T) {
	var deleted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted.Store(r.Header.Get("Mcp-Session-Id") == "session-to-close")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "session-to-close")
			writeResult(w, req.ID, map[string]any{"protocolVersion": "2025-03-26"})
		case "notifications/initialized":
			http.Error(w, "rejected readiness notification", http.StatusBadRequest)
		default:
			t.Errorf("握手失败后不应发出工具调用，收到 %q", req.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	client, err := mcp.NewStreamableHTTPClient("session", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Initialize(t.Context()); err == nil || !strings.Contains(err.Error(), "rejected readiness notification") || !deleted.Load() {
		t.Fatalf("初始化失败应保留通知错误并关闭已分配会话，deleted=%v，错误 %v", deleted.Load(), err)
	}
}
