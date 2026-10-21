package fallback_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// genOnlyMember 不支持流式；Generate 返回配置的消息与错误，并计数。
type genOnlyMember struct {
	msg   *schema.Message
	err   error
	calls atomic.Int32
}

func (m *genOnlyMember) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	return m.msg, m.err
}

func (m *genOnlyMember) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *genOnlyMember) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// callBoth 分别经 Generate 与 Stream 调一次 fb，返回两条路各自的答复与错误。
func callBoth(t *testing.T, fb model.ChatModel) (genOut string, genErr error, streamOut string, streamErr error) {
	t.Helper()
	in := []*schema.Message{schema.UserMessage("hi")}
	msg, genErr := fb.Generate(context.Background(), in)
	if msg != nil {
		genOut = msg.Content
	}
	sr, streamErr := fb.Stream(context.Background(), in)
	if sr != nil {
		streamOut = readAll(t, sr)
	}
	return genOut, genErr, streamOut, streamErr
}

// provider 在响应里报的上下文超限、内容过滤，跟 HTTP 400 报的同一类错误同一个答案：不重试、不切换。
// 这两个哨兵包在 StatusCode 为 0 的 APIError 里（建流成功之后才知道），按状态码会被当成网络错误，
// 每个候选重试再切换。
func TestFallback_InputFaultsReportedInResponse_NoRetryNoSwitch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		sentinel error
	}{
		{
			name:     "context_window",
			err:      &model.APIError{Provider: "p", Err: fmt.Errorf("%w (finish_reason=model_context_window_exceeded)", model.ErrContextWindowExceeded)},
			sentinel: model.ErrContextWindowExceeded,
		},
		{
			name:     "content_filter",
			err:      &model.APIError{Provider: "p", Err: model.ErrContentFiltered},
			sentinel: model.ErrContentFiltered,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := &genOnlyMember{err: tc.err}
			backup := &genOnlyMember{msg: schema.AssistantMessage("backup")}
			fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
			_, genErr, _, streamErr := callBoth(t, fb)
			if !errors.Is(genErr, tc.sentinel) || !errors.Is(streamErr, tc.sentinel) {
				t.Fatalf("两条路都应原样交出主候选的错误，实际 Generate %v / Stream %v", genErr, streamErr)
			}
			if n := primary.calls.Load(); n != 2 {
				t.Fatalf("主候选每条路只该被请求一次（共 2 次），实际 %d 次", n)
			}
			if n := backup.calls.Load(); n != 0 {
				t.Fatalf("不该切到备选，实际备选被请求 %d 次", n)
			}
		})
	}
}

// 对照：同样包在 StatusCode 为 0 的 APIError 里的网络错误照常重试、切换——上面那条不是因为
// 「StatusCode 为 0 一律不切」才成立的。
func TestFallback_NetworkErrorStillRetriesAndSwitches(t *testing.T) {
	primary := &genOnlyMember{err: &model.APIError{Provider: "p", Err: errors.New("connection reset")}}
	backup := &genOnlyMember{msg: schema.AssistantMessage("backup")}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
	genOut, genErr, streamOut, streamErr := callBoth(t, fb)
	if genErr != nil || streamErr != nil || genOut != "backup" || streamOut != "backup" {
		t.Fatalf("网络错误应切到备选，实际 Generate %q %v / Stream %q %v", genOut, genErr, streamOut, streamErr)
	}
	if n := primary.calls.Load(); n != 6 {
		t.Fatalf("主候选每条路应请求 1+2 次（共 6 次），实际 %d 次", n)
	}
}

// 候选的 Generate 返回 nil 消息、又没报错：按候选故障处理，Generate 与 Stream 两条路一样切到备选。
// 曾经 Stream 那条切换、Generate 那条把 nil 当成功交给调用方。
func TestFallback_NilGenerateMessage_SwitchesOnBothPaths(t *testing.T) {
	primary := &genOnlyMember{}
	backup := &genOnlyMember{msg: schema.AssistantMessage("backup")}
	fb := fallback.New([]model.ChatModel{primary, backup})
	genOut, genErr, streamOut, streamErr := callBoth(t, fb)
	if genErr != nil || genOut != "backup" {
		t.Fatalf("Generate：主候选返回 nil 消息应切到备选，实际 %q %v", genOut, genErr)
	}
	if streamErr != nil || streamOut != "backup" {
		t.Fatalf("Stream：主候选返回 nil 消息应切到备选，实际 %q %v", streamOut, streamErr)
	}
}
