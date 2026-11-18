package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"
)

// Notification 是服务端推送的 JSON-RPC 通知。Params 保留 JSON 原文，缺省时为 nil。
type Notification struct {
	Method string
	Params json.RawMessage
}

// Subscribe 在当前会话上打开独立 GET 流，并按到达顺序调用 handle。
// 它阻塞到 ctx 结束、会话关闭或收到终止错误。回调错误经包装原样返回，由调用方控制回调的退出。
// 断线后在同一会话尝试重连。有游标时续订原流，无游标时打开新的 GET 流。
// HTTP 405 表示服务端不提供该流。订阅的整体时限由 ctx 和会话决定。
func (c *StreamableHTTPClient) Subscribe(ctx context.Context, handle func(context.Context, Notification) error) error {
	if handle == nil {
		return errors.New("mcp subscribe: notification handler is required")
	}
	session, err := c.activeSession(ctx)
	if err != nil {
		return fmt.Errorf("mcp subscribe: %w", err)
	}
	ctx, release := session.requestContext(ctx)
	defer release()
	client := *c.httpClient
	client.Timeout = 0 // 流由订阅 ctx 结束，避免空闲但健康的会话周期性断开。
	dispatch := func(msg sseRPCMessage) (bool, error) {
		if err := session.lifetime.Err(); err != nil {
			return false, err
		}
		if msg.Method == "" {
			return false, nil
		}
		if len(msg.ID) != 0 {
			return false, c.replyToServer(ctx, session, msg)
		}
		if err := handle(ctx, Notification{Method: msg.Method, Params: msg.Params}); err != nil {
			return false, fmt.Errorf("notification handler: %w", err)
		}
		return false, nil
	}

	var cursor sseCursor
	body, err := c.openEventStream(ctx, &client, session, "")
	for err == nil {
		_, err = readEventStream(ctx, body, &cursor, nil, dispatch)
		if _, disconnected := err.(*sseReadError); !disconnected {
			break
		}
		body, err = c.reconnectEventStream(ctx, &client, session, cursor)
	}
	if err != nil {
		return fmt.Errorf("mcp subscribe: %w", err)
	}
	return nil
}

func isEventStream(resp *http.Response) bool {
	typ, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")) // 仅精确匹配媒体类型。
	return typ == "text/event-stream"
}

func (c *StreamableHTTPClient) openEventStream(ctx context.Context, client *http.Client, session *httpSession, eventID string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return nil, c.redactTransportError(err, session.id, eventID)
	}
	c.setRequestHeaders(req, session)
	req.Header.Set("Accept", "text/event-stream")
	if eventID != "" {
		req.Header.Set(lastEventIDHeader, eventID)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, c.redactTransportError(err, session.id, eventID)
	}
	if resp.StatusCode/100 != 2 {
		_ = resp.Body.Close() // 按 GET 状态处理，错误体可能未结束。
		status := &httpStatusError{code: resp.StatusCode, afterSend: true}
		c.expireSession(session, status)
		return nil, status
	}
	if !isEventStream(resp) {
		_ = resp.Body.Close() // 媒体类型错误优先。
		return nil, &badResponseError{errors.New("expected text/event-stream from MCP GET")}
	}
	return resp.Body, nil
}

func (c *StreamableHTTPClient) reconnectEventStream(ctx context.Context, client *http.Client, session *httpSession, cursor sseCursor) (io.ReadCloser, error) {
	delay := cursor.retry
	if delay == 0 {
		delay = 250 * time.Millisecond
	}
	var err error
	for range 3 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		var stream io.ReadCloser
		stream, err = c.openEventStream(ctx, client, session, cursor.id)
		if err == nil {
			return stream, nil
		}
		var status *httpStatusError
		var transport *url.Error
		if !errors.As(err, &transport) && !(errors.As(err, &status) && status.code/100 == 5) {
			return nil, err
		}
	}
	return nil, err
}

func (c *StreamableHTTPClient) readSSEReply(ctx context.Context, session *httpSession, body io.ReadCloser, id string) (rpcResponse, error) {
	var cursor sseCursor
	var reply rpcResponse
	budget := int64(maxResponseBytes)
	dispatch := func(msg sseRPCMessage) (bool, error) {
		if msg.Method != "" {
			if len(msg.ID) != 0 {
				return false, c.replyToServer(ctx, session, msg)
			}
			return false, nil
		}
		if responseIDKey(msg.ID) == id && (msg.Result != nil || msg.Error != nil) {
			reply = msg.rpcResponse
			return true, nil
		}
		return false, nil
	}
	for {
		sentCursor := cursor.id
		done, err := readEventStream(ctx, body, &cursor, &budget, dispatch)
		if done {
			if reply.Error != nil && sentCursor != "" {
				reply.Error = c.redactRPC(reply.Error, sentCursor)
			}
			return reply, nil
		}
		if _, disconnected := err.(*sseReadError); !disconnected {
			return rpcResponse{}, err
		}
		if cursor.id == "" {
			return rpcResponse{}, fmt.Errorf("SSE closed before request %s received a response: %w", id, err)
		}
		body, err = c.reconnectEventStream(ctx, c.httpClient, session, cursor)
		if err != nil {
			return rpcResponse{}, fmt.Errorf("resume SSE response for request %s: %w", id, err)
		}
	}
}

func readEventStream(ctx context.Context, body io.ReadCloser, cursor *sseCursor, budget *int64, dispatch func(sseRPCMessage) (bool, error)) (bool, error) {
	defer body.Close()
	if budget == nil {
		return cursor.read(ctx, body, dispatch)
	}
	limited := &io.LimitedReader{R: body, N: *budget + 1}
	done, err := cursor.read(ctx, limited, dispatch)
	*budget = limited.N - 1
	if !done && *budget < 0 {
		return false, oversizedHTTPReply()
	}
	return done, err
}

// replyToServer 应答 MCP ping。其余服务端请求需要客户端能力，为未声明的能力返回 method not found。
func (c *StreamableHTTPClient) replyToServer(ctx context.Context, session *httpSession, msg sseRPCMessage) error {
	if string(msg.ID) == "null" {
		return nil
	}
	reply := rpcResponse{JSONRPC: "2.0", ID: msg.ID}
	if msg.Method == "ping" {
		reply.Result = json.RawMessage(`{}`)
	} else {
		reply.Error = &RPCError{Code: -32601, Message: "method not found"}
	}
	data, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("mcp encode server request reply: %w", err)
	}
	err = c.postSessionMessage(ctx, session, data)
	if status, ok := err.(*httpStatusError); ok {
		status.afterSend = true
		c.expireSession(session, status)
	}
	return err
}
