package openai_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

// 流式响应的空闲超时：provider 发了一部分之后停住、既不发也不关连接时，流要以 ErrStreamIdleTimeout 结束，
// 不能一直挂着等调用方的 ctx。计时只算在等数据的时间，消费方慢、keep-alive 注释都不能误触发。

// stallServer 起一个服务端：按顺序写 parts（每段之间停 gap 并 Flush），写完之后不关连接，挂到测试结束。
func stallServer(t *testing.T, gap time.Duration, parts ...string) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for i, p := range parts {
			if i > 0 && gap > 0 {
				time.Sleep(gap)
			}
			_, _ = io.WriteString(w, p)
			fl.Flush()
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	return srv
}

// drain 读完流，返回拼起来的文本与结束时的错误（io.EOF 记成 nil）。
func drain(t *testing.T, sr *schema.StreamReader[*schema.Message]) (string, error) {
	t.Helper()
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

func TestStreamIdleTimeout_StalledStreamEndsWithIdleError(t *testing.T) {
	srv := stallServer(t, 0, sseFrame(contentChunk(t, "半截")))
	c := newStreamClient(srv, openai.WithStreamIdleTimeout(100*time.Millisecond))
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 兜底：空闲超时失效时测试不会挂死
	defer cancel()
	sr, err := c.Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	text, err := drain(t, sr)
	if !errors.Is(err, openai.ErrStreamIdleTimeout) {
		t.Fatalf("停住的流应以 ErrStreamIdleTimeout 结束，实际 err=%v（文本 %q）", err, text)
	}
	if text != "半截" {
		t.Fatalf("停住之前的片段应照常交出，实际 %q", text)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("应在空闲超时后不久结束，实际用了 %s", el)
	}
	var ae *model.APIError
	if !errors.As(err, &ae) || ae.StatusCode != 0 {
		t.Fatalf("应包在 StatusCode 为 0 的 APIError 里（按网络错误重试 / 切换），实际 %#v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("空闲超时不是调用方的期限，错误链上不该有 context.DeadlineExceeded")
	}
}

// 消费方迟迟不取：流的缓冲满了、生产方在等消费方，这段时间不算空闲。每帧 1KB、共 64KB——一次 Read 读不完，
// 生产方等消费方的那段时间里还有数据没读，计时器若在两次 Read 之间也走，就会把连接关掉。
func TestStreamIdleTimeout_SlowConsumerNotIdle(t *testing.T) {
	const frames = 64
	parts := make([]string, 0, frames+2)
	for range frames {
		parts = append(parts, sseFrame(contentChunk(t, strings.Repeat("x", 1024))))
	}
	parts = append(parts, sseFrame(finishChunk("stop")), sseFrame("[DONE]"))
	srv := sseServer(t, strings.Join(parts, ""))
	c := newStreamClient(srv, openai.WithStreamIdleTimeout(100*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 兜底：空闲超时失效时测试不会挂死
	defer cancel()
	sr, err := c.Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	time.Sleep(400 * time.Millisecond) // 缓冲早已满，生产方一直在等
	text, err := drain(t, sr)
	if err != nil || len(text) != frames*1024 {
		t.Fatalf("消费方慢不该触发空闲超时：err=%v 收到 %d 字节", err, len(text))
	}
}

// keep-alive 注释也是收到的数据：provider 排队时用它保活。
func TestStreamIdleTimeout_KeepAliveCommentsCount(t *testing.T) {
	parts := []string{": keep-alive\n\n", ": keep-alive\n\n", ": keep-alive\n\n", ": keep-alive\n\n",
		sseFrame(contentChunk(t, "好")), sseFrame(finishChunk("stop")), sseFrame("[DONE]")}
	srv := stallServer(t, 80*time.Millisecond, parts...)
	c := newStreamClient(srv, openai.WithStreamIdleTimeout(200*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 兜底：空闲超时失效时测试不会挂死
	defer cancel()
	sr, err := c.Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	text, err := drain(t, sr)
	if err != nil || text != "好" {
		t.Fatalf("keep-alive 每 80ms 一次、超时 200ms，不该超时：err=%v 文本 %q", err, text)
	}
}

// 结束原因已经收到、停在等末尾帧上：输出是完整的，不报错。
func TestStreamIdleTimeout_AfterFinishReasonOutputComplete(t *testing.T) {
	srv := stallServer(t, 0, sseFrame(contentChunk(t, "完整")), sseFrame(finishChunk("stop")))
	c := newStreamClient(srv, openai.WithStreamIdleTimeout(100*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 兜底：空闲超时失效时测试不会挂死
	defer cancel()
	sr, err := c.Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	text, err := drain(t, sr)
	if err != nil || text != "完整" {
		t.Fatalf("收到结束原因之后的空闲不影响结果：err=%v 文本 %q", err, text)
	}
}

// 默认就有空闲超时；<= 0 关闭，停住的流只能等调用方的 ctx。
func TestStreamIdleTimeout_DefaultOnAndDisable(t *testing.T) {
	if openai.DefaultStreamIdleTimeout <= 0 {
		t.Fatal("默认应开启空闲超时")
	}
	srv := stallServer(t, 0, sseFrame(contentChunk(t, "半截")))
	c := newStreamClient(srv, openai.WithStreamIdleTimeout(0))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	sr, err := c.Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("建流失败：%v", err)
	}
	_, err = drain(t, sr)
	if errors.Is(err, openai.ErrStreamIdleTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("关掉空闲超时后应由调用方 ctx 结束，实际 %v", err)
	}
}
