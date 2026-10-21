package openai_test

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

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

// 这组测试钉的是：流式响应只有收到 finish_reason 才算输出完整，其余结束方式都要作为错误
// 交给消费方，不能让消费方读到 io.EOF、把半截内容当成完整答复。

// sseFrame 把一个 data 值包成一帧（data 行 + 空行）。
func sseFrame(data string) string { return "data: " + data + "\n\n" }

// contentChunk 是一帧只带文本增量的 chunk。
func contentChunk(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return `{"choices":[{"delta":{"content":` + string(b) + `}}]}`
}

// finishChunk 是一帧带结束原因的 chunk。
func finishChunk(reason string) string {
	return `{"choices":[{"delta":{},"finish_reason":"` + reason + `"}]}`
}

const usageChunk = `{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`

// sseServer 起一个服务端，把 body 原样作为 200 的流式响应写回。
func sseServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// cutServer 起一个服务端：响应头声明的 Content-Length 比实际写的多，写完 body 就断开连接，
// 客户端读到半路收到 io.ErrUnexpectedEOF——模拟连接断在流中途。
func cutServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\n\r\n", len(body)+1000)
		_, _ = buf.WriteString(body)
		_ = buf.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newStreamClient(srv *httptest.Server, opts ...openai.ClientOption) model.ChatModel {
	base := []openai.ClientOption{openai.WithBaseURL(srv.URL), openai.WithSystem("qwen")}
	return openai.NewClient("k", "m", append(base, opts...)...)
}

// drainStream 读完整条流，返回拼起来的内容与流的结论（正常结束为 nil）。Recv 用独立的
// Background ctx，结论只取决于生产方交出了什么。
func drainStream(t *testing.T, ctx context.Context, c model.ChatModel) (string, error) {
	t.Helper()
	sr, err := c.Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	defer sr.Close()
	var b strings.Builder
	for {
		m, err := sr.Recv(context.Background())
		if errors.Is(err, io.EOF) {
			return b.String(), nil
		}
		if err != nil {
			return b.String(), err
		}
		b.WriteString(m.Content)
	}
}

func TestStream_FinishReasonThenDone_Completes(t *testing.T) {
	body := sseFrame(`{"choices":[{"delta":{"role":"assistant","content":"你"}}]}`) +
		sseFrame(contentChunk(t, "好")) +
		sseFrame(finishChunk("stop")) +
		sseFrame(usageChunk) +
		sseFrame("[DONE]")
	got, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err != nil {
		t.Fatalf("完整的流不该报错：%v", err)
	}
	if got != "你好" {
		t.Fatalf("内容 = %q，want 你好", got)
	}
}

// 结束原因已经收到、连接随后正常关闭（没有 [DONE]）：输出完整。
func TestStream_FinishReasonWithoutDone_Completes(t *testing.T) {
	body := sseFrame(contentChunk(t, "你好")) + sseFrame(finishChunk("stop"))
	if _, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body))); err != nil {
		t.Fatalf("收到了 finish_reason 就算完整，不该报错：%v", err)
	}
}

func TestStream_EndsWithoutFinishReason_IsError(t *testing.T) {
	for _, tc := range []struct {
		name, body string
	}{
		{"收到 [DONE]", sseFrame(`{"choices":[{"delta":{"content":"你好"}}]}`) + sseFrame("[DONE]")},
		{"连接正常关闭", sseFrame(`{"choices":[{"delta":{"content":"你好"}}]}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, tc.body)))
			if err == nil {
				t.Fatalf("没收到 finish_reason 的流被当成了完整输出（内容 %q）", got)
			}
			var apiErr *model.APIError
			if !errors.As(err, &apiErr) || apiErr.Provider != "qwen" {
				t.Fatalf("错误应是 provider=qwen 的 *model.APIError，实际 %T %v", err, err)
			}
			if !strings.Contains(err.Error(), "finish_reason") {
				t.Fatalf("错误文本应点出没收到 finish_reason：%v", err)
			}
			if got != "你好" {
				t.Fatalf("报错之前已收到的内容应照常交出，实际 %q", got)
			}
		})
	}
}

// provider 不带 error 键、只回一帧 {"code","message"} 就结束的：错误里带上那一帧，好排查。
func TestStream_EndsWithoutFinishReason_ReportsLastUnrecognizedChunk(t *testing.T) {
	body := sseFrame(`{"code":"Throttling","message":"too many requests"}`)
	_, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err == nil || !strings.Contains(err.Error(), "too many requests") {
		t.Fatalf("错误里应带上最后那一帧无法识别的内容，实际 %v", err)
	}
}

func TestStream_OptionalFinishReason(t *testing.T) {
	body := sseFrame(contentChunk(t, "你好")) + sseFrame("[DONE]")
	t.Run("ClientOption", func(t *testing.T) {
		c := newStreamClient(sseServer(t, body), openai.WithOptionalFinishReason())
		if _, err := drainStream(t, context.Background(), c); err != nil {
			t.Fatalf("声明了不发 finish_reason 的 provider，正常结束不该报错：%v", err)
		}
	})
	t.Run("Preset", func(t *testing.T) {
		srv := sseServer(t, body)
		c := openai.NewCompatible(openai.Preset{System: "p", BaseURL: srv.URL, OptionalFinishReason: true}, "k", "m")
		if _, err := drainStream(t, context.Background(), c); err != nil {
			t.Fatalf("Preset.OptionalFinishReason 没生效：%v", err)
		}
	})
	t.Run("错误帧照样报错", func(t *testing.T) {
		c := newStreamClient(sseServer(t, sseFrame(`{"error":{"message":"boom"}}`)), openai.WithOptionalFinishReason())
		if _, err := drainStream(t, context.Background(), c); err == nil {
			t.Fatal("声明了不发 finish_reason 不该吞掉错误帧")
		}
	})
}

// 声明了不支持流式：Stream 不发请求，直接返回 ErrStreamUnsupported（ClientOption 与 Preset 两条路）。
func TestStream_StreamUnsupportedDeclared(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
	}))
	defer srv.Close()
	for _, tc := range []struct {
		name string
		c    model.ChatModel
	}{
		{"ClientOption", newStreamClient(srv, openai.WithStreamUnsupported())},
		{"Preset", openai.NewCompatible(openai.Preset{System: "p", BaseURL: srv.URL, StreamUnsupported: true}, "k", "m")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sr, err := tc.c.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
			if sr != nil || !errors.Is(err, model.ErrStreamUnsupported) {
				t.Fatalf("应直接返回 ErrStreamUnsupported，实际 reader=%v err=%v", sr, err)
			}
		})
	}
	if n := reqs.Load(); n != 0 {
		t.Fatalf("声明了不支持流式就不该发请求，实际发了 %d 次", n)
	}
}

// 错误帧后面都跟一帧 finish：只靠「没收到 finish_reason」兜底也会报错，那样测不出错误帧本身
// 有没有被认出来。
func TestStream_ErrorFrames(t *testing.T) {
	finish := sseFrame(finishChunk("stop"))
	for _, tc := range []struct {
		name, body, want string
	}{
		{"error 是对象", sseFrame(contentChunk(t, "你")) + sseFrame(`{"error":{"message":"rate limited","code":"429"}}`) + finish, "rate limited"},
		{"error 是字符串", sseFrame(`{"error":"overloaded"}`) + finish, "overloaded"},
		{"event: error", "event: error\ndata: {\"message\":\"boom\"}\n\n" + finish, "boom"},
		{"结束原因之后的错误帧", finish + sseFrame(`{"error":{"message":"late failure"}}`), "late failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, tc.body)))
			if err == nil {
				t.Fatal("错误帧被当成了正常内容")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误文本应带上 provider 给的说明 %q：%v", tc.want, err)
			}
		})
	}
}

// error 字段为 null 的帧不是错误（官方 SDK 按真值判）。
func TestStream_NullErrorFieldIsNotAnError(t *testing.T) {
	for _, v := range []string{"null", "false", "0", "0.0", `""`, "[ ]", "{ }"} {
		t.Run(v, func(t *testing.T) {
			body := sseFrame(`{"error":` + v + `,"choices":[{"delta":{"content":"你好"},"finish_reason":"stop"}]}`)
			got, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
			if err != nil || got != "你好" {
				t.Fatalf("error:%s 的帧应当照常处理，实际内容 %q err %v", v, got, err)
			}
		})
	}
}

// 输入超出上下文窗口（智谱的结束原因）：错误文本要能被上下文压缩的第 3 层认出来。
func TestStream_ContextWindowExceededLooksLikePromptTooLong(t *testing.T) {
	body := sseFrame(contentChunk(t, "你")) + sseFrame(finishChunk("model_context_window_exceeded"))
	_, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err == nil || !strings.Contains(err.Error(), "context length exceeded") {
		t.Fatalf("错误文本应含 context length exceeded，实际 %v", err)
	}
}

// 截断的帧多半是模型输出，可能含用户的个人信息；错误文本会进 span、交给前端与对端，不能带原文。
func TestStream_InvalidChunkErrorOmitsContent(t *testing.T) {
	body := `data: {"choices":[{"delta":{"content":"用户身份证号 110101199003071234 已核对`
	_, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err == nil || !strings.Contains(err.Error(), "invalid stream chunk") {
		t.Fatalf("截断的帧应报 invalid stream chunk，实际 %v", err)
	}
	if strings.Contains(err.Error(), "110101199003071234") {
		t.Fatalf("错误文本带出了模型输出的原文：%v", err)
	}
}

// 结束原因已经收到之后，再来解析不了的帧（类型对不上的 usage、心跳）不影响结果：输出是完整的。
func TestStream_UnparsableChunkAfterFinishReason_Ignored(t *testing.T) {
	body := sseFrame(contentChunk(t, "完整")) + sseFrame(finishChunk("stop")) +
		sseFrame(`{"choices":[],"usage":{"prompt_tokens":"3"}}`) + sseFrame("ping") + sseFrame("[DONE]")
	got, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err != nil || got != "完整" {
		t.Fatalf("结束原因之后解析不了的帧应只告警，实际内容 %q err %v", got, err)
	}
}

// 读出错之前，结束帧那一行已经连同行尾完整收到、只差派发它的空行：照常处理，输出完整。
// 那一行没有行尾就断了：不知道这一行收全了没有，按读错误处理。
func TestStream_ReadErrorAfterCompleteFinishLine(t *testing.T) {
	head := sseFrame(contentChunk(t, "完整答案"))
	got, err := drainStream(t, context.Background(), newStreamClient(cutServer(t, head+"data: "+finishChunk("stop")+"\n")))
	if err != nil || got != "完整答案" {
		t.Fatalf("结束帧那一行已完整收到，应按完整处理，实际内容 %q err %v", got, err)
	}
	_, err = drainStream(t, context.Background(), newStreamClient(cutServer(t, head+"data: "+finishChunk("stop"))))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("结束帧那一行没有行尾就断了，应报读错误，实际 %v", err)
	}
}

func TestStream_InvalidJSONFrame_IsError(t *testing.T) {
	body := sseFrame(contentChunk(t, "你")) + sseFrame("<html>bad gateway</html>") + sseFrame(finishChunk("stop"))
	_, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err == nil || !strings.Contains(err.Error(), "invalid stream chunk") {
		t.Fatalf("data 不是 JSON 应报错，实际 %v", err)
	}
}

func TestStream_ConnectionCutMidStream_IsError(t *testing.T) {
	body := sseFrame(contentChunk(t, "你好"))
	got, err := drainStream(t, context.Background(), newStreamClient(cutServer(t, body)))
	if err == nil {
		t.Fatalf("连接断在流中途被当成了完整输出（内容 %q）", got)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("错误链上应保留底层的读错误，实际 %v", err)
	}
}

// 结束原因已经到了才断开：输出完整，断掉的只可能是后面那一帧 usage。
func TestStream_ConnectionCutAfterFinishReason_Completes(t *testing.T) {
	body := sseFrame(contentChunk(t, "你好")) + sseFrame(finishChunk("stop"))
	got, err := drainStream(t, context.Background(), newStreamClient(cutServer(t, body)))
	if err != nil || got != "你好" {
		t.Fatalf("收到 finish_reason 之后断开应按完整处理，实际内容 %q err %v", got, err)
	}
}

// 调用方取消 ctx：消费方必须拿到取消错误。这里 Recv 用的是另一个不会取消的 ctx，所以
// 结论只能来自生产方——只闭流不报错的话，消费方必然读到 io.EOF。
func TestStream_ContextCanceled_ReportedAsError(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, sseFrame(`{"choices":[{"delta":{"content":"你"}}]}`))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	sr, err := newStreamClient(srv).Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	defer sr.Close()
	if _, err := sr.Recv(context.Background()); err != nil {
		t.Fatalf("第一帧应正常收到：%v", err)
	}
	cancel()
	recvCtx, recvCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer recvCancel()
	_, err = sr.Recv(recvCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("取消之后消费方应拿到 context.Canceled，实际 %v", err)
	}
}

func TestStream_FinishReasonOutcomes(t *testing.T) {
	for _, tc := range []struct {
		reason    string
		wantErr   bool
		filtered  bool
		exceeded  bool
		retryable bool
	}{
		{reason: "stop"},
		{reason: "length"},
		{reason: "tool_calls"},
		{reason: "function_call"},
		{reason: "some_new_reason"},
		{reason: "content_filter", wantErr: true, filtered: true},
		{reason: "sensitive", wantErr: true, filtered: true},
		{reason: "model_context_window_exceeded", wantErr: true, exceeded: true},
		{reason: "insufficient_system_resource", wantErr: true, retryable: true},
		{reason: "aborted", wantErr: true, retryable: true},
		{reason: "network_error", wantErr: true, retryable: true},
		{reason: "error", wantErr: true, retryable: true},
		{reason: "abort", wantErr: true, retryable: true},
		{reason: "repetition", wantErr: true, retryable: true},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			body := sseFrame(contentChunk(t, "你好")) + sseFrame(finishChunk(tc.reason)) + sseFrame("[DONE]")
			_, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("finish_reason=%s 应算完整，实际报错 %v", tc.reason, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("finish_reason=%s 表示输出不完整，不该当成完整输出", tc.reason)
			}
			if got := errors.Is(err, model.ErrContentFiltered); got != tc.filtered {
				t.Fatalf("errors.Is(ErrContentFiltered) = %v，want %v（err %v）", got, tc.filtered, err)
			}
			if got := errors.Is(err, model.ErrContextWindowExceeded); got != tc.exceeded {
				t.Fatalf("errors.Is(ErrContextWindowExceeded) = %v，want %v（err %v）", got, tc.exceeded, err)
			}
			if got := compose_v2.IsRetryable(err); got != tc.retryable {
				t.Fatalf("IsRetryable = %v，want %v（err %v）", got, tc.retryable, err)
			}
		})
	}
}

// Generate 跟流式按同一套结束原因判断：内容被过滤、生成被打断的输出不当成正常结果。
func TestGenerate_FinishReasonOutcomes(t *testing.T) {
	for _, tc := range []struct {
		reason   string
		wantErr  bool
		filtered bool
		exceeded bool
	}{
		{reason: ""},
		{reason: "stop"},
		{reason: "length"},
		{reason: "content_filter", wantErr: true, filtered: true},
		{reason: "model_context_window_exceeded", wantErr: true, exceeded: true},
		{reason: "insufficient_system_resource", wantErr: true},
	} {
		t.Run("reason="+tc.reason, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"你好"},"finish_reason":"`+tc.reason+`"}]}`)
			}))
			defer srv.Close()
			out, err := newStreamClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
			if !tc.wantErr {
				if err != nil || out.Content != "你好" {
					t.Fatalf("finish_reason=%q 应算完整，实际 %v err %v", tc.reason, out, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("finish_reason=%s 表示输出不完整，Generate 不该当成正常结果", tc.reason)
			}
			if got := errors.Is(err, model.ErrContentFiltered); got != tc.filtered {
				t.Fatalf("errors.Is(ErrContentFiltered) = %v，want %v（err %v）", got, tc.filtered, err)
			}
			if got := errors.Is(err, model.ErrContextWindowExceeded); got != tc.exceeded {
				t.Fatalf("errors.Is(ErrContextWindowExceeded) = %v，want %v（err %v）", got, tc.exceeded, err)
			}
		})
	}
}

// 同一条逻辑流的几种合法写法（SSE 规范允许的行尾、空格、注释、多行 data 等），结论都应一样。
func TestStream_SSEFramingVariants(t *testing.T) {
	content := contentChunk(t, "你好")
	finish := finishChunk("stop")
	for _, tc := range []struct {
		name, body string
	}{
		{"LF", sseFrame(content) + sseFrame(finish) + sseFrame("[DONE]")},
		{"CRLF", "data: " + content + "\r\n\r\ndata: " + finish + "\r\n\r\ndata: [DONE]\r\n\r\n"},
		{"单独的 CR", "data: " + content + "\r\rdata: " + finish + "\r\r"},
		{"冒号后没有空格", "data:" + content + "\n\ndata:" + finish + "\n\n"},
		{"注释与其他字段", ": keep-alive\n\nid: 1\nevent: message\nretry: 100\ndata: " + content + "\n\n: ping\ndata: " + finish + "\n\n"},
		{"多行 data", "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"},\ndata: \"finish_reason\":\"stop\"}]}\n\n"},
		{"开头带 BOM", "\uFEFFdata: " + content + "\n\ndata: " + finish + "\n\n"},
		{"最后一帧后面没有空行", "data: " + content + "\n\ndata: " + finish},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, tc.body)))
			if err != nil || got != "你好" {
				t.Fatalf("内容 %q err %v，want 你好 / nil", got, err)
			}
		})
	}
}

// 一帧里塞了很大一段内容（超过 bufio.Scanner 默认的 64KB 单行上限）照样能读。
func TestStream_LargeChunk(t *testing.T) {
	big := strings.Repeat("字", 100_000)
	body := sseFrame(contentChunk(t, big)) + sseFrame(finishChunk("stop"))
	got, err := drainStream(t, context.Background(), newStreamClient(sseServer(t, body)))
	if err != nil || got != big {
		t.Fatalf("大帧读取失败：len=%d err=%v", len(got), err)
	}
}
