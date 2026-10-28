package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

// drain 读完一个流，返回最后的错误（正常结束为 nil）。
func drain(t *testing.T, sr *schema.StreamReader[*schema.Message]) error {
	t.Helper()
	ctx := context.Background()
	for {
		_, err := sr.Recv(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// TestStreamIdleTimeout_StalledStreamFails：provider 发了 message_start 之后停住、不关连接，超过空闲超时流以可重试的
// ErrStreamIdleTimeout 结束，不是一直挂到调用方的 ctx。
func TestStreamIdleTimeout_StalledStreamFails(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		replySSE(w, evMessageStart(nil))
		<-r.Context().Done() // 停住，直到客户端关连接
	})
	// 兜底期限：没有空闲超时时这条测试以 ctx 到期失败，而不是一直挂着
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sr, err := newClient(srv, anthropic.WithStreamIdleTimeout(100*time.Millisecond)).
		Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	start := time.Now()
	err = drain(t, sr)
	if !errors.Is(err, anthropic.ErrStreamIdleTimeout) {
		t.Fatalf("停住的流应以 ErrStreamIdleTimeout 结束，got %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("空闲超时 100ms，流过了 %v 才结束", d)
	}
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 0 {
		t.Errorf("应包在 StatusCode 为 0 的 APIError 里，got %#v", err)
	}
	if !compose_v2.IsRetryable(err) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("空闲超时应可重试、且不带 DeadlineExceeded，got %v", err)
	}
}

// TestStreamIdleTimeout_DataRenewsTimer：每次收到数据都续期——ping 间隔短于空闲超时、总时长长于它的流照常读完。
func TestStreamIdleTimeout_DataRenewsTimer(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f, _ := w.(http.Flusher)
		write := func(e sse) {
			replyEvent(w, e)
			f.Flush()
		}
		write(evMessageStart(nil))
		for range 6 {
			time.Sleep(50 * time.Millisecond)
			write(sse{"ping", map[string]any{"type": "ping"}})
		}
		for _, e := range []sse{evBlockStart(0, map[string]any{"type": "text", "text": ""}),
			evDelta(0, map[string]any{"type": "text_delta", "text": "好"}), evBlockStop(0),
			evMessageDelta("end_turn", map[string]any{"output_tokens": 1}), evMessageStop()} {
			write(e)
		}
	})
	sr, err := newClient(srv, anthropic.WithStreamIdleTimeout(150*time.Millisecond)).
		Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if err := drain(t, sr); err != nil {
		t.Errorf("持续有数据的流不该超时，got %v", err)
	}
}

// TestStreamIdleTimeout_Disabled：d <= 0 关闭空闲超时，停住的流只受调用方 ctx 约束。
func TestStreamIdleTimeout_Disabled(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		replySSE(w, evMessageStart(nil))
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	sr, err := newClient(srv, anthropic.WithStreamIdleTimeout(0)).Stream(ctx, []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if err := drain(t, sr); errors.Is(err, anthropic.ErrStreamIdleTimeout) || err == nil {
		t.Errorf("关掉空闲超时后应由调用方 ctx 结束，got %v", err)
	}
}

// replyEvent 写一个 SSE 事件（不 flush）。
func replyEvent(w io.Writer, e sse) {
	raw, err := json.Marshal(e.data)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, raw)
}
