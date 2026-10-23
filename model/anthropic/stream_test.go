package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

// jsonOf 把 JSON 文本解成值（数字保留原文）。
func jsonOf(t *testing.T, s string) any {
	t.Helper()
	var v any
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("解 JSON %q: %v", s, err)
	}
	return v
}

// sseFor 把一条完整响应的 content 拆成流式事件：text 拆成两片 delta，thinking 发 thinking_delta 与
// signature_delta，tool_use 的 input 以 input_json_delta 发，其余块在 content_block_start 里整块给出。
func sseFor(content []any, stopReason string) []sse {
	events := []sse{evMessageStart(nil)}
	for i, raw := range content {
		b := raw.(map[string]any)
		switch b["type"] {
		case "text":
			events = append(events, evBlockStart(i, map[string]any{"type": "text", "text": ""}))
			r := []rune(b["text"].(string))
			for _, p := range []string{string(r[:len(r)/2]), string(r[len(r)/2:])} {
				if p != "" {
					events = append(events, evDelta(i, map[string]any{"type": "text_delta", "text": p}))
				}
			}
		case "thinking":
			events = append(events, evBlockStart(i, map[string]any{"type": "thinking", "thinking": "", "signature": ""}))
			if s, _ := b["thinking"].(string); s != "" {
				events = append(events, evDelta(i, map[string]any{"type": "thinking_delta", "thinking": s}))
			}
			events = append(events, evDelta(i, map[string]any{"type": "signature_delta", "signature": b["signature"]}))
		case "tool_use":
			events = append(events, evBlockStart(i, map[string]any{"type": "tool_use", "id": b["id"], "name": b["name"], "input": map[string]any{}}))
			raw, err := json.Marshal(b["input"])
			if err != nil {
				panic(err)
			}
			if string(raw) != "{}" {
				events = append(events, evDelta(i, map[string]any{"type": "input_json_delta", "partial_json": string(raw)}))
			}
		default:
			events = append(events, evBlockStart(i, b))
		}
		events = append(events, evBlockStop(i))
	}
	return append(events, evMessageDelta(stopReason, map[string]any{"output_tokens": 5}), evMessageStop())
}

// collect 读完整个流，返回全部片段与结束时的错误（正常结束是 io.EOF）。
func collect(t *testing.T, reader *schema.StreamReader[*schema.Message]) ([]*schema.Message, error) {
	t.Helper()
	defer reader.Close()
	var chunks []*schema.Message
	for {
		chunk, err := reader.Recv(context.Background())
		if err != nil {
			return chunks, err
		}
		chunks = append(chunks, chunk)
	}
}

// aggregate 按 agent_v2 流式聚合的规则把片段拼成一条消息：Content / ReasoningContent 拼接，工具调用按
// ID 累加参数，标注逐片合并（后写的赢），元数据取最后一个非空的。
func aggregate(chunks []*schema.Message) *schema.Message {
	out := &schema.Message{Role: schema.RoleAssistant}
	var content, reasoning strings.Builder
	byID := map[string]*schema.ContentBlock{}
	for _, c := range chunks {
		content.WriteString(c.Content)
		reasoning.WriteString(c.ReasoningContent)
		for _, cb := range c.ContentBlocks {
			if cb == nil || cb.Toolcall == nil {
				continue
			}
			if prev, ok := byID[cb.Toolcall.Id]; ok {
				prev.Toolcall.Arguments += cb.Toolcall.Arguments
				continue
			}
			cp := &schema.ContentBlock{Type: cb.Type, Toolcall: &schema.ToolcallContentBlock{
				Id: cb.Toolcall.Id, Name: cb.Toolcall.Name, Arguments: cb.Toolcall.Arguments}}
			byID[cb.Toolcall.Id] = cp
			out.ContentBlocks = append(out.ContentBlocks, cp)
		}
		for k, v := range c.Annotations {
			if out.Annotations == nil {
				out.Annotations = map[string]any{}
			}
			out.Annotations[k] = v
		}
		if c.Metadata != nil {
			out.Metadata = c.Metadata
		}
	}
	out.Content, out.ReasoningContent = content.String(), reasoning.String()
	return out
}

// streamedContent 用流式跑一遍 content，返回聚合出的 Content。
func streamedContent(t *testing.T, content []any) string {
	t.Helper()
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replySSE(w, sseFor(content, "end_turn")...)
	})
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunks, err := collect(t, reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("流结束于 %v，期望 io.EOF", err)
	}
	return aggregate(chunks).Content
}

// TestStream_DeltasThenFinalChunk 钉住流式的交付形状：文本与 thinking 文本逐片交出；工具调用、
// 标注、用量、结束原因只在最后一片；聚合出的消息下一轮按 API 原样回放（块的顺序、签名、参数）。
func TestStream_DeltasThenFinalChunk(t *testing.T) {
	content := []any{
		map[string]any{"type": "thinking", "thinking": "想一想", "signature": "sig-s"},
		map[string]any{"type": "text", "text": "你好呀"},
		map[string]any{"type": "tool_use", "id": "toolu_s", "name": "lookup", "input": map[string]any{"q": "x"}},
	}
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			events := sseFor(content, "tool_use")
			// 换成带 thinking 用量的 message_delta
			events[len(events)-2] = evMessageDelta("tool_use", map[string]any{
				"output_tokens": 40, "output_tokens_details": map[string]any{"thinking_tokens": 10}})
			replySSE(w, events...)
			return
		}
		textReply(w, nil, n)
	})
	c := newClient(srv)
	reader, err := c.Stream(context.Background(), []*schema.Message{schema.UserMessage("查 x")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunks, err := collect(t, reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("流结束于 %v，期望 io.EOF", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("只收到 %d 片", len(chunks))
	}
	for i, ch := range chunks[:len(chunks)-1] {
		if len(ch.ContentBlocks) > 0 || ch.Annotations != nil || ch.Metadata != nil {
			t.Errorf("第 %d 片提前带了工具调用 / 标注 / 元数据：%+v", i, ch)
		}
	}
	last := chunks[len(chunks)-1]
	if last.Content != "" || last.ReasoningContent != "" {
		t.Errorf("最后一片不该重复文本：%q / %q", last.Content, last.ReasoningContent)
	}
	msg := aggregate(chunks)
	if msg.Content != "你好呀" || msg.ReasoningContent != "想一想" {
		t.Errorf("聚合出 Content=%q ReasoningContent=%q", msg.Content, msg.ReasoningContent)
	}
	calls := schema.GetToolCalls(msg)
	if len(calls) != 1 || calls[0].ID != "toolu_s" || calls[0].Function.Arguments != `{"q":"x"}` {
		t.Errorf("工具调用 = %+v", calls)
	}
	md := last.Metadata
	if md == nil || md.FinishReason != "tool_calls" || md.Usage == nil ||
		md.Usage.InputTokens != 10 || md.Usage.OutputTokens != 40 || md.Usage.ThinkTokens != 10 {
		t.Errorf("Metadata = %+v / Usage = %+v", md, md.Usage)
	}

	// 下一轮：聚合出的消息原样回放
	if _, err := c.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("查 x"), msg, toolResult("toolu_s", "结果", false),
	}); err != nil {
		t.Fatalf("第二轮 Generate: %v", err)
	}
	want := `[{"signature":"sig-s","thinking":"想一想","type":"thinking"},{"text":"你好呀","type":"text"},` +
		`{"id":"toolu_s","input":{"q":"x"},"name":"lookup","type":"tool_use"}]`
	if got := canon(t, at(rec.at(t, 1).body, "messages", 1, "content")); got != want {
		t.Errorf("回放的 assistant content =\n%s\n期望\n%s", got, want)
	}
}

// TestStream_EndsWithoutMessageStop：没收到 message_stop 就断开是截断，必须报错——只闭流的话消费方
// 拿到 io.EOF，半截内容会被当成完整答复。
func TestStream_EndsWithoutMessageStop(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replySSE(w, evMessageStart(nil), evBlockStart(0, map[string]any{"type": "text", "text": ""}),
			evDelta(0, map[string]any{"type": "text_delta", "text": "半截"}))
	})
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunks, err := collect(t, reader)
	if err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "without message_stop") {
		t.Fatalf("流结束于 %v，期望「without message_stop」错误", err)
	}
	if len(chunks) != 1 || chunks[0].Content != "半截" {
		t.Errorf("已到达的片段 = %+v", chunks)
	}
}

// TestStream_ErrorEvent：流中途的错误帧作为流的错误交出，StatusCode 为 0（不是 HTTP 错误，SDK 给它
// 填的 200 不能当状态码），默认可重试。
func TestStream_ErrorEvent(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replySSE(w, evMessageStart(nil), sse{"error", map[string]any{"type": "error",
			"error": map[string]any{"type": "overloaded_error", "message": "Overloaded"}}})
	})
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	_, err = collect(t, reader)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 0 || !strings.Contains(err.Error(), "overloaded_error") {
		t.Fatalf("err = %v（%+v），期望 StatusCode 0、带 overloaded_error 的 *model.APIError", err, apiErr)
	}
	if !compose_v2.IsRetryable(err) {
		t.Errorf("过载错误帧应可重试：%v", err)
	}
}

// TestStream_HTTPErrorReturnedSynchronously：建流时的 HTTP 错误由 Stream 直接返回（不进流），带状态码
// 与 Retry-After，且只发一次请求。
func TestStream_HTTPErrorReturnedSynchronously(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("retry-after-ms", "1500")
		replyJSON(w, 529, map[string]any{"type": "error", "error": map[string]any{"type": "overloaded_error", "message": "busy"}})
	})
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if reader != nil {
		reader.Close()
		t.Fatalf("HTTP 错误时不该返回 reader")
	}
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 529 || apiErr.RetryAfter != 1500*time.Millisecond {
		t.Fatalf("err = %v（%+v）", err, apiErr)
	}
	if n := rec.count(); n != 1 {
		t.Errorf("发了 %d 次请求，期望 1 次", n)
	}
}

// TestStream_Refusal：流到一半被拒，已流出的文本作废，流以内容过滤错误结束。
func TestStream_Refusal(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		events := sseFor([]any{map[string]any{"type": "text", "text": "我来写"}}, "refusal")
		replySSE(w, events...)
	})
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	_, err = collect(t, reader)
	if !errors.Is(err, model.ErrContentFiltered) {
		t.Fatalf("err = %v，期望内容过滤错误", err)
	}
	var refusal *anthropic.RefusalError
	if !errors.As(err, &refusal) {
		t.Errorf("err = %v，期望 *anthropic.RefusalError", err)
	}
}

// TestStream_FallbackDropsDeclinedBlocks：服务端回退在中途切换模型时，切换点之前被拒模型的 thinking 与
// 工具调用作废——工具调用不交出（不会被执行），回放也不带它们；文本照常保留；切换记在标注里。
func TestStream_FallbackDropsDeclinedBlocks(t *testing.T) {
	content := []any{
		map[string]any{"type": "text", "text": "开头 "},
		map[string]any{"type": "thinking", "thinking": "", "signature": "sig-declined"},
		map[string]any{"type": "tool_use", "id": "toolu_bad", "name": "f", "input": map[string]any{"a": 1}},
		map[string]any{"type": "fallback", "from": map[string]any{"model": "claude-opus-5-5"}, "to": map[string]any{"model": "claude-opus-5"}},
		map[string]any{"type": "thinking", "thinking": "", "signature": "sig-new"},
		map[string]any{"type": "text", "text": "结尾"},
		map[string]any{"type": "tool_use", "id": "toolu_good", "name": "f", "input": map[string]any{"a": 2}},
	}
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			replySSE(w, sseFor(content, "tool_use")...)
			return
		}
		textReply(w, nil, n)
	})
	c := newClient(srv)
	reader, err := c.Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunks, err := collect(t, reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("流结束于 %v", err)
	}
	msg := aggregate(chunks)
	if calls := schema.GetToolCalls(msg); len(calls) != 1 || calls[0].ID != "toolu_good" {
		t.Fatalf("工具调用 = %+v，期望只有 toolu_good", calls)
	}
	if msg.Content != "开头 结尾" {
		t.Errorf("Content = %q", msg.Content)
	}
	if hops := anthropic.Fallbacks(msg); len(hops) != 1 || hops[0].From != "claude-opus-5-5" || hops[0].To != "claude-opus-5" {
		t.Errorf("Fallbacks = %+v", hops)
	}
	if msg.Metadata == nil || msg.Metadata.Model != "claude-opus-5" {
		t.Errorf("Metadata.Model 应是接着答的模型：%+v", msg.Metadata)
	}
	if _, err := c.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("q"), msg, toolResult("toolu_good", "ok", false),
	}); err != nil {
		t.Fatalf("第二轮 Generate: %v", err)
	}
	want := `[{"text":"开头 ","type":"text"},{"signature":"sig-new","thinking":"","type":"thinking"},` +
		`{"text":"结尾","type":"text"},{"id":"toolu_good","input":{"a":2},"name":"f","type":"tool_use"}]`
	if got := canon(t, at(rec.at(t, 1).body, "messages", 1, "content")); got != want {
		t.Errorf("回放的 assistant content =\n%s\n期望\n%s", got, want)
	}
}

// slowStream 先发一片文本，然后挂住直到请求被取消（或超时），取消时关闭 canceled。
func slowStream(canceled chan struct{}) func(w http.ResponseWriter, r *http.Request, n int) {
	return func(w http.ResponseWriter, r *http.Request, _ int) {
		replySSE(w, evMessageStart(nil), evBlockStart(0, map[string]any{"type": "text", "text": ""}),
			evDelta(0, map[string]any{"type": "text_delta", "text": "第一片"}))
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-time.After(5 * time.Second):
		}
	}
}

// TestStream_ConsumerCloseCancelsRequest：消费方 Close 之后请求被取消、连接释放——不能等到下一个事件
// 到达（可能永远不来）才发现没人要了。
func TestStream_ConsumerCloseCancelsRequest(t *testing.T) {
	canceled := make(chan struct{})
	srv, _ := newServer(t, slowStream(canceled))
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if chunk, err := reader.Recv(context.Background()); err != nil || chunk.Content != "第一片" {
		t.Fatalf("第一片 = %+v, %v", chunk, err)
	}
	reader.Close()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("消费方 Close 之后请求没被取消")
	}
}

// TestStream_CtxCancel：调用方 ctx 取消要作为流的错误交出，交出的就是 ctx 的错误本身，不包成 provider
// 错误（*model.APIError）——按错误类型统计 provider 失败的消费方不该把调用方取消算进去。Recv 用的是另一个
// 没取消的 ctx，错误只能来自生产方。
func TestStream_CtxCancel(t *testing.T) {
	canceled := make(chan struct{})
	srv, _ := newServer(t, slowStream(canceled))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := newClient(srv).Stream(ctx, []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer reader.Close()
	if _, err := reader.Recv(context.Background()); err != nil {
		t.Fatalf("第一片: %v", err)
	}
	cancel()
	recvCtx, recvCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer recvCancel()
	_, err = reader.Recv(recvCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv = %v，期望 context.Canceled", err)
	}
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		t.Errorf("调用方取消被包成了 provider 错误：%v", err)
	}
}

// TestStream_ExtraBodyKeepsStreamFlag：带 ExtraBody 时请求体由本包自己编码，stream:true 不能丢。
func TestStream_ExtraBodyKeepsStreamFlag(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replySSE(w, sseFor([]any{map[string]any{"type": "text", "text": "ok"}}, "end_turn")...)
	})
	reader, err := newClient(srv).Stream(context.Background(), []*schema.Message{schema.UserMessage("q")},
		anthropic.WithEffortOption(anthropic.EffortLow))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := collect(t, reader); !errors.Is(err, io.EOF) {
		t.Fatalf("流结束于 %v", err)
	}
	body := rec.at(t, 0).body
	if got := canon(t, body["stream"]); got != "true" {
		t.Errorf("stream = %s", got)
	}
	if got := str(body, "output_config", "effort"); got != "low" {
		t.Errorf("output_config.effort = %q", got)
	}
}
