package anthropic_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"crew-go/model/anthropic"
)

// captured 是桩服务收到的一次请求。
type captured struct {
	header http.Header
	body   map[string]any
}

// recorder 记下桩服务收到的全部请求，测试据此断言「发出去的是什么」。
type recorder struct {
	mu   sync.Mutex
	reqs []captured
}

func (r *recorder) add(c captured) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, c)
	return len(r.reqs)
}

// at 返回第 i 次请求（从 0 计）。
func (r *recorder) at(t *testing.T, i int) captured {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.reqs) {
		t.Fatalf("桩服务只收到 %d 次请求，要第 %d 次", len(r.reqs), i+1)
	}
	return r.reqs[i]
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs)
}

// newServer 起一个桩服务：记下每次请求，再交给 respond 回应（n 是第几次请求，从 1 计）。
func newServer(t *testing.T, respond func(w http.ResponseWriter, r *http.Request, n int)) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("读请求体: %v", err)
		}
		var body map[string]any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			t.Errorf("请求体不是 JSON 对象: %v\n%s", err, raw)
		}
		n := rec.add(captured{header: r.Header.Clone(), body: body})
		respond(w, r, n)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// newClient 建一个指向桩服务的 Client。
func newClient(srv *httptest.Server, opts ...anthropic.ClientOption) *anthropic.Client {
	base := []anthropic.ClientOption{anthropic.WithBaseURL(srv.URL), anthropic.WithHTTPClient(srv.Client())}
	return anthropic.NewClient("test-key", "", append(base, opts...)...)
}

// message 构造一条完整的响应消息。usage 为 nil 时给一组默认用量。
func message(content []any, stopReason string, usage map[string]any) map[string]any {
	if usage == nil {
		usage = map[string]any{"input_tokens": 10, "output_tokens": 5}
	}
	return map[string]any{
		"id":            "msg_test",
		"type":          "message",
		"role":          "assistant",
		"model":         "claude-opus-5-5",
		"content":       content,
		"stop_reason":   stopReason,
		"stop_sequence": nil,
		"usage":         usage,
	}
}

// replyJSON 回一个 JSON 响应。
func replyJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // 写给测试客户端，失败时客户端那边会报
}

// sse 是一个 SSE 事件。
type sse struct {
	event string
	data  any
}

// replySSE 回一串 SSE 事件。
func replySSE(w http.ResponseWriter, events ...sse) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	for _, e := range events {
		raw, err := json.Marshal(e.data)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, raw)
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// 下面几个构造流式事件。
func evMessageStart(usage map[string]any) sse {
	if usage == nil {
		usage = map[string]any{"input_tokens": 10, "output_tokens": 1}
	}
	return sse{"message_start", map[string]any{"type": "message_start", "message": map[string]any{
		"id": "msg_test", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
		"content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": usage,
	}}}
}

func evBlockStart(i int, block map[string]any) sse {
	return sse{"content_block_start", map[string]any{"type": "content_block_start", "index": i, "content_block": block}}
}

func evDelta(i int, delta map[string]any) sse {
	return sse{"content_block_delta", map[string]any{"type": "content_block_delta", "index": i, "delta": delta}}
}

func evBlockStop(i int) sse {
	return sse{"content_block_stop", map[string]any{"type": "content_block_stop", "index": i}}
}

func evMessageDelta(stopReason string, usage map[string]any) sse {
	return sse{"message_delta", map[string]any{"type": "message_delta",
		"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil}, "usage": usage}}
}

func evMessageStop() sse {
	return sse{"message_stop", map[string]any{"type": "message_stop"}}
}

// at 按路径取 JSON 值：字符串走对象键，整数走数组下标。取不到时返回 nil。
func at(v any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			s, ok := v.([]any)
			if !ok || k >= len(s) {
				return nil
			}
			v = s[k]
		}
	}
	return v
}

// str 按路径取字符串（json.Number 也按原文）。
func str(v any, path ...any) string {
	switch x := at(v, path...).(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}

// num 按路径取整数。
func num(t *testing.T, v any, path ...any) int {
	t.Helper()
	s := str(v, path...)
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("路径 %v 不是整数: %q", path, s)
	}
	return n
}

// canon 把 JSON 值编码成规范字符串（对象键排序），用于比较两份 JSON 语义上是否相同。
func canon(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("编码: %v", err)
	}
	var x any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&x); err != nil {
		t.Fatalf("解码: %v", err)
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false) // 期望串里直接写 <、>，不写 \u003c
	if err := enc.Encode(x); err != nil {
		t.Fatalf("编码: %v", err)
	}
	return strings.TrimSuffix(out.String(), "\n")
}

// betas 返回请求的 anthropic-beta 取值。
func betas(h http.Header) []string {
	var out []string
	for _, v := range h.Values("Anthropic-Beta") {
		for _, b := range strings.Split(v, ",") {
			if b = strings.TrimSpace(b); b != "" {
				out = append(out, b)
			}
		}
	}
	return out
}
