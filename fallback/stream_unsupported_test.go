package fallback_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// stubMember 是一个候选：Stream / Generate 各返回配置的错误，都没配错误时 Stream 给一条正常的流、
// Generate 给一条正常的答复；streamUnsupported 为 true 时 Stream 返回 model.ErrStreamUnsupported。
type stubMember struct {
	name              string
	streamUnsupported bool
	err               error
	streamCalls       atomic.Int32
	genCalls          atomic.Int32
}

func (m *stubMember) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.genCalls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return schema.AssistantMessage(m.name + " generate"), nil
}

func (m *stubMember) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.streamCalls.Add(1)
	if m.streamUnsupported {
		return nil, model.ErrStreamUnsupported
	}
	if m.err != nil {
		return nil, m.err
	}
	return schema.NewSingleChunkReader(schema.AssistantMessage(m.name + " stream")), nil
}

func (m *stubMember) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// readAll 读完流，返回拼起来的内容。
func readAll(t *testing.T, sr *schema.StreamReader[*schema.Message]) string {
	t.Helper()
	defer sr.Close()
	var out string
	for {
		m, err := sr.Recv(context.Background())
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("读流出错：%v", err)
		}
		if m == nil {
			t.Fatal("流里交出了 nil 消息")
		}
		out += m.Content
	}
}

// 不支持流式的候选在流式里就地用它的 Generate：不切到备选、不记断路器失败。跑的次数超过默认
// 断路器阈值（3），主候选也不会被熔断。
func TestFallback_StreamUsesGenerateOfNonStreamingMember(t *testing.T) {
	primary := &stubMember{name: "primary", streamUnsupported: true}
	backup := &stubMember{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})
	for i := 0; i < 5; i++ {
		sr, err := fb.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
		if err != nil {
			t.Fatalf("第 %d 次：不支持流式的主候选应就地用 Generate，实际报错 %v", i+1, err)
		}
		if got := readAll(t, sr); got != "primary generate" {
			t.Fatalf("第 %d 次：应由主候选的 Generate 回答，实际 %q", i+1, got)
		}
	}
	if s, g := backup.streamCalls.Load(), backup.genCalls.Load(); s != 0 || g != 0 {
		t.Fatalf("备选不该被调用，实际 Stream %d 次、Generate %d 次", s, g)
	}
}

// 主候选支持流式但限流，备选不支持流式：切到备选后就地用备选的 Generate，不从主候选重新走。
// 从头重走的话，主候选在一次调用里被打两次（不看 Retry-After），断路器也一次记两次失败——
// 断路器阈值设成 2，第二次调用时主候选还应该被尝试。
func TestFallback_NonStreamingBackupDoesNotRestartFromPrimary(t *testing.T) {
	primary := &stubMember{name: "primary", err: &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 30 * time.Second}}
	backup := &stubMember{name: "backup", streamUnsupported: true}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(2, time.Hour))
	for i := 0; i < 2; i++ {
		sr, err := fb.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
		if err != nil {
			t.Fatalf("第 %d 次：应由备选经 Generate 回答，实际报错 %v", i+1, err)
		}
		if got := readAll(t, sr); got != "backup generate" {
			t.Fatalf("第 %d 次：应由备选的 Generate 回答，实际 %q", i+1, got)
		}
	}
	if s, g := primary.streamCalls.Load(), primary.genCalls.Load(); s != 2 || g != 0 {
		t.Fatalf("主候选每次调用只该被打一次（Stream），实际 Stream %d 次、Generate %d 次", s, g)
	}
}
