//go:build integration

package anthropic_smoke_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

type timingTransport struct{ body *timedStreamBody }

func (t *timingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err == nil && resp.StatusCode == http.StatusOK && strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.body = &timedStreamBody{ReadCloser: resp.Body, lastRead: time.Now()}
		resp.Body = t.body
	}
	return resp, err
}

type timedStreamBody struct {
	io.ReadCloser
	lastRead, lastPing time.Time
	longestGap         time.Duration
	longestPing        time.Duration
	bytes, pings       int
	line               [64]byte
	lineBytes          int
}

func (b *timedStreamBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n == 0 {
		return n, err
	}
	now := time.Now()
	b.bytes += n
	b.longestGap = max(b.longestGap, now.Sub(b.lastRead))
	b.lastRead = now
	for _, ch := range p[:n] {
		if ch == '\n' {
			if b.lineBytes <= len(b.line) && strings.TrimSpace(string(b.line[:b.lineBytes])) == "event: ping" {
				b.pings++
				if !b.lastPing.IsZero() {
					b.longestPing = max(b.longestPing, now.Sub(b.lastPing))
				}
				b.lastPing = now
			}
			b.lineBytes = 0
			continue
		}
		if b.lineBytes < len(b.line) {
			b.line[b.lineBytes] = ch
		}
		if b.lineBytes <= len(b.line) {
			b.lineBytes++
		}
	}
	return n, err
}

func TestSmoke_StreamIdleAndBilling(t *testing.T) {
	transport := &timingTransport{}
	client, _ := newClient(t, anthropic.WithHTTPClient(&http.Client{Transport: transport}),
		anthropic.WithEffort(anthropic.EffortHigh), anthropic.WithThinkingDisplay(anthropic.DisplayOmitted))
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	stream, err := client.Stream(ctx, []*schema.Message{schema.UserMessage(
		"Compare the correctness trade-offs of replaying a checkpointed parallel workflow after a crash. Consider commit ordering and external side effects before answering. Give a concise answer.")},
		model.WithMaxTokens(2048))
	if err != nil {
		checkLiveStreamError(t, err)
	}
	defer stream.Close()
	var answer strings.Builder
	for {
		msg, err := stream.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			checkLiveStreamError(t, err)
		}
		if msg != nil {
			answer.WriteString(msg.Content)
		}
	}
	body := transport.body
	if body == nil || body.bytes == 0 || strings.TrimSpace(answer.String()) == "" {
		t.Fatal("真服务端没有返回可测量的 SSE 正文")
	}
	t.Logf("SSE %d bytes，最长字节间隔 %s，ping %d 次，最长 ping 间隔 %s",
		body.bytes, body.longestGap, body.pings, body.longestPing)
	if body.longestGap >= anthropic.DefaultStreamIdleTimeout {
		t.Fatalf("观察到的字节间隔超过默认空闲超时 %s", anthropic.DefaultStreamIdleTimeout)
	}
	t.Log("本次请求未遇到计费拒绝")
}

func checkLiveStreamError(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, model.ErrBillingExhausted) {
		t.Log("真服务端的计费拒绝已归类为 model.ErrBillingExhausted")
		t.Skip("当前凭据没有额度继续验证流式间隔")
	}
	t.Fatal(err)
}
