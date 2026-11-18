package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/mcp"
	"crew-go/tool"
)

type sseRoundTrip func(*http.Request) (*http.Response, error)

func (f sseRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newSSETransportClient(t *testing.T, rt sseRoundTrip) *mcp.StreamableHTTPClient {
	t.Helper()
	c, err := mcp.NewStreamableHTTPClient("events", "https://mcp.invalid", mcp.WithHTTPClient(&http.Client{Transport: rt}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() }) // 测试断言使用显式观察点。
	return c
}

func sseWireResponse(code int, media, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": {media}}, Body: io.NopCloser(strings.NewReader(body))}
}

func sseWireJSON(id int64, result string) *http.Response {
	return sseWireResponse(http.StatusOK, "application/json", fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, id, result))
}

func sseCallBody(r *http.Request) (rpcReq, error) {
	defer r.Body.Close()
	var req rpcReq
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

func TestStreamableHTTP_InitializeCanResumeBeforeNegotiation(t *testing.T) {
	var id int64
	var notified bool
	c := newSSETransportClient(t, func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodGet:
			if r.Header.Get("Last-Event-ID") != "handshake-start" || r.Header.Get("Mcp-Session-Id") != sseSessionID || r.Header.Get("MCP-Protocol-Version") != "" {
				t.Error("握手续订应带受校验的候选会话和游标，版本待协商")
			}
			return sseWireResponse(http.StatusOK, "text/event-stream", fmt.Sprintf("data: {\"id\":%d,\"result\":{\"protocolVersion\":%q}}\n\n", id, sseVersion)), nil
		case http.MethodDelete:
			return sseWireResponse(http.StatusNoContent, "", ""), nil
		}
		req, err := sseCallBody(r)
		if err != nil {
			return nil, err
		}
		if req.Method == "initialize" {
			id = req.ID
			resp := sseWireResponse(http.StatusOK, "text/event-stream", "id: handshake-start\nretry: 1\ndata:\n\n")
			resp.Header.Set("Mcp-Session-Id", sseSessionID)
			return resp, nil
		}
		if req.Method != "notifications/initialized" || r.Header.Get("MCP-Protocol-Version") != sseVersion || r.Header.Get("Mcp-Session-Id") != sseSessionID {
			t.Error("初始化通知应带续订协商的会话和版本")
		}
		notified = true
		return sseWireResponse(http.StatusAccepted, "", ""), nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := c.Initialize(ctx); err != nil || !notified {
		t.Fatalf("续订后应完成握手，实际 notified=%v err=%v", notified, err)
	}
}

func TestStreamableHTTP_CloseCancelsHandshakeResume(t *testing.T) {
	started := make(chan struct{})
	var deletes atomic.Int32
	c := newSSETransportClient(t, func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodGet:
			close(started)
			<-r.Context().Done()
			return nil, r.Context().Err()
		case http.MethodDelete:
			if r.Header.Get("Mcp-Session-Id") != sseSessionID {
				t.Error("取消握手应释放原候选会话")
			}
			deletes.Add(1)
			return sseWireResponse(http.StatusNoContent, "", ""), nil
		}
		req, err := sseCallBody(r)
		if err != nil {
			return nil, err
		}
		if req.Method != "initialize" {
			t.Errorf("握手被取消后收到 RPC %s", req.Method)
		}
		resp := sseWireResponse(http.StatusOK, "text/event-stream", "id: handshake-start\nretry: 1\ndata:\n\n")
		resp.Header.Set("Mcp-Session-Id", sseSessionID)
		return resp, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	initialized := make(chan error, 1)
	go func() { initialized <- c.Initialize(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("没有开始握手续订 GET")
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close 仍在等待握手续订")
	}
	select {
	case err := <-initialized:
		if !errors.Is(err, context.Canceled) || deletes.Load() != 1 {
			t.Fatalf("应取消握手并释放候选会话，实际 err=%v deletes=%d", err, deletes.Load())
		}
	case <-ctx.Done():
		t.Fatal("握手续订未取消")
	}
}

type observedWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestStreamableHTTP_HandshakeLeaderCancelKeepsOtherCaller(t *testing.T) {
	started := make(chan struct{})
	var count atomic.Int32
	c := newSSETransportClient(t, func(r *http.Request) (*http.Response, error) {
		req, err := sseCallBody(r)
		if err != nil {
			return nil, err
		}
		if req.Method != "initialize" {
			return sseWireResponse(http.StatusAccepted, "", ""), nil
		}
		if count.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return sseWireJSON(req.ID, `{"protocolVersion":"2025-11-25"}`), nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	first, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstErr, secondErr := make(chan error, 1), make(chan error, 1)
	go func() { firstErr <- c.Initialize(first) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("第一轮握手未开始")
	}
	waiter := &observedWaitContext{Context: ctx, waiting: make(chan struct{})}
	go func() { secondErr <- c.Initialize(waiter) }()
	select {
	case <-waiter.waiting:
	case <-ctx.Done():
		t.Fatal("另一个调用方未等待握手")
	}
	stopFirst()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("首个调用应取消，实际 %v", err)
	}
	if err := <-secondErr; err != nil || count.Load() != 2 {
		t.Fatalf("另一调用应以自己的 ctx 完成握手，实际 err=%v handshakes=%d", err, count.Load())
	}
}

func TestStreamableHTTP_ExpiredGETKeepsNewSessionOnlineDespiteLatePOST(t *testing.T) {
	firstWrite, allowLate := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(allowLate) })
	defer release()
	var handshakes, writes atomic.Int32
	c := newSSETransportClient(t, func(r *http.Request) (*http.Response, error) {
		sid := r.Header.Get("Mcp-Session-Id")
		switch r.Method {
		case http.MethodGet:
			if sid != "generation-1" {
				t.Errorf("GET 应使用原会话，实际 %q", sid)
			}
			return sseWireResponse(http.StatusNotFound, "", ""), nil
		case http.MethodDelete:
			return sseWireResponse(http.StatusNoContent, "", ""), nil
		}
		req, err := sseCallBody(r)
		if err != nil {
			return nil, err
		}
		switch req.Method {
		case "initialize":
			if sid != "" || r.Header.Get("Last-Event-ID") != "" {
				t.Error("新握手不能沿用旧会话与流游标")
			}
			resp := sseWireJSON(req.ID, `{"protocolVersion":"2025-11-25"}`)
			resp.Header.Set("Mcp-Session-Id", fmt.Sprintf("generation-%d", handshakes.Add(1)))
			return resp, nil
		case "notifications/initialized":
			return sseWireResponse(http.StatusAccepted, "", ""), nil
		case "tools/list":
			return sseWireJSON(req.ID, `{"tools":[{"name":"write"}]}`), nil
		case "tools/call":
			writes.Add(1)
			if sid == "generation-1" {
				close(firstWrite)
				<-r.Context().Done()
				<-allowLate
				return nil, r.Context().Err()
			}
			if sid != "generation-2" {
				t.Errorf("写请求未切到正确的会话，实际 %q", sid)
			}
			return sseWireJSON(req.ID, `{"content":[{"type":"text","text":"saved on new session"}]}`), nil
		}
		return sseWireResponse(http.StatusBadRequest, "", ""), nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	h := mcp.NewHub(mcp.WithClientFactory("events", func(context.Context) (mcp.Client, error) { return c, nil }), mcp.WithReconnectBackoff(time.Hour, time.Hour))
	if err := h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() }) // 收尾在原调用已退出之后。
	old := make(chan *tool.Result, 1)
	go func() {
		res, err := h.Toolkit().Call(ctx, "mcp_events_write", nil)
		if err != nil {
			t.Error(err)
		}
		old <- res
	}()
	select {
	case <-firstWrite:
	case <-ctx.Done():
		t.Fatal("旧写入未开始")
	}
	if err := c.Subscribe(ctx, func(context.Context, mcp.Notification) error { return nil }); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("应由 GET 404 撤销旧会话，实际 %v", err)
	}
	newResult, err := h.Toolkit().Call(ctx, "mcp_events_write", nil)
	if err != nil || newResult == nil || !newResult.Success || newResult.Output != "saved on new session" || handshakes.Load() != 2 {
		t.Fatalf("应在原 Hub 连接上用新会话写入，实际 res=%+v err=%v sessions=%d", newResult, err, handshakes.Load())
	}
	release()
	select {
	case res := <-old:
		if res == nil || res.Success || res.Error == nil || !strings.Contains(res.Error.Message, "不确定是否已执行") {
			t.Fatalf("旧会话结果应保留执行不确定性，实际 %+v", res)
		}
	case <-ctx.Done():
		t.Fatal("旧工具调用未返回")
	}
	if st := statusOf(t, h, "events"); st.State != mcp.ServerOnline || writes.Load() != 2 {
		t.Fatalf("迟到错误不能让新会话掉线或重发工具，实际 state=%v writes=%d", st.State, writes.Load())
	}
}

func TestStreamableHTTP_ResumeTransportNotSentIsNotToolNotSent(t *testing.T) {
	var writes, gets int
	c := newSSETransportClient(t, func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodGet:
			gets++
			return nil, errors.Join(mcp.ErrNotSent, context.DeadlineExceeded)
		case http.MethodDelete:
			return sseWireResponse(http.StatusNoContent, "", ""), nil
		}
		req, err := sseCallBody(r)
		if err != nil {
			return nil, err
		}
		switch req.Method {
		case "initialize":
			return sseWireJSON(req.ID, `{"protocolVersion":"2025-11-25"}`), nil
		case "notifications/initialized":
			return sseWireResponse(http.StatusAccepted, "", ""), nil
		default:
			writes++
			return sseWireResponse(http.StatusOK, "text/event-stream", "id: written\nretry: 1\ndata:\n\n"), nil
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err := c.CallTool(ctx, "write", nil)
	if errors.Is(err, mcp.ErrNotSent) || !errors.Is(err, context.DeadlineExceeded) || writes != 1 || gets == 0 {
		t.Fatalf("只应保留后续 GET 的超时原因，实际 err=%v writes=%d GET=%d", err, writes, gets)
	}
}
