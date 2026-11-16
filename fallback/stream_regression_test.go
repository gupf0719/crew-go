package fallback_test

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

func TestFallback_PeekHandsOffAtPrefixLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		frames := make([]*schema.Message, 64)
		for i := range frames {
			frames[i] = &schema.Message{Role: schema.RoleAssistant,
				Metadata: &schema.MessageMetadata{Usage: &schema.MessageUsage{InputTokens: int32(i + 1)}}}
		}
		primary := &prefixStreamMember{stubModel: stubModel{name: "primary"}, prefix: frames,
			closed: make(chan struct{}), blocking: true}
		backup := &stubModel{name: "backup"}
		fb := fallback.New([]model.ChatModel{primary, backup})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		sr, err := fallback.New([]model.ChatModel{fb}).Stream(ctx, msgs())
		if err != nil {
			t.Fatalf("达到预读上限时应交付主候选，实际 %v", err)
		}
		defer sr.Close()
		for i, want := range frames {
			if got, err := sr.Recv(ctx); err != nil || got != want {
				t.Fatalf("元数据帧 %d 应原样交付，实际 %v，%v", i, got, err)
			}
		}
		if backup.callCount() != 0 {
			t.Fatal("预读上限应允许消费原流")
		}
	})
}

type scriptedStreamTerminal struct {
	stubModel
	ends []error
}

func (m *scriptedStreamTerminal) Stream(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	end := m.ends[m.record(in)]
	r, send := schema.NewStreamReader[*schema.Message](1)
	send(schema.AssistantMessage("主候选正文"))
	if end != nil {
		schema.SendError(r, end)
	}
	schema.CloseSend(r)
	return r, nil
}

func openPrimaryFlow(t *testing.T, fb model.ChatModel) *schema.StreamReader[*schema.Message] {
	t.Helper()
	ctx := t.Context()
	r, err := fb.Stream(ctx, msgs())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	if chunk, err := r.Recv(ctx); err != nil || chunk.GetContent() != "主候选正文" {
		t.Fatalf("应先收到主候选正文，实际 %v，%v", chunk, err)
	}
	return r
}

type transparentStreamMember struct {
	model.ChatModel
}

func TestFallback_EstablishedStream429GatesSubsequentRequest(t *testing.T) {
	for _, name := range []string{"直接调用", "直接嵌套", "经包装嵌套"} {
		t.Run(name, func(t *testing.T) {
			limited := &model.APIError{Provider: "stub", StatusCode: 429, RetryAfter: 90 * time.Second}
			primary := &scriptedStreamTerminal{stubModel: stubModel{name: "primary"}, ends: []error{limited}}
			backup := &stubModel{name: "backup"}
			var fb model.ChatModel = fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(3, time.Hour))
			outerBackup := &stubModel{name: "outer backup"}
			switch name {
			case "直接嵌套":
				fb = fallback.New([]model.ChatModel{fb, outerBackup})
			case "经包装嵌套":
				fb = fallback.New([]model.ChatModel{transparentStreamMember{fb}, outerBackup})
			}
			sr := openPrimaryFlow(t, fb)
			if _, err := sr.Recv(t.Context()); err != limited || backup.callCount() != 0 {
				t.Fatalf("已输出正文的请求应保留原始 429，实际 %v，备用调用 %d 次", err, backup.callCount())
			}
			msg, err := fb.Generate(t.Context(), msgs())
			if err != nil || msg.GetContent() != "backup ok" || primary.callCount() != 1 || outerBackup.callCount() != 0 {
				t.Fatalf("下一请求在 Retry-After 内应跳过主候选，实际 %q，%v，主候选累计 %d 次",
					msg.GetContent(), err, primary.callCount())
			}
		})
	}
}

func TestFallback_OldStreamCannotUndoTrip(t *testing.T) {
	broken := apiErr(503)
	primary := &scriptedStreamTerminal{stubModel: stubModel{name: "primary"}, ends: []error{nil, broken}}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))
	old := openPrimaryFlow(t, fb)
	failed := openPrimaryFlow(t, fb)
	if _, err := failed.Recv(t.Context()); err != broken {
		t.Fatalf("第二条流应保留服务端错误，实际 %v", err)
	}
	if msg, err := fb.Generate(t.Context(), msgs()); err != nil || msg.GetContent() != "backup ok" {
		t.Fatalf("503 后应先进入熔断，实际 %q，%v", msg.GetContent(), err)
	}
	if _, err := old.Recv(t.Context()); err != io.EOF {
		t.Fatalf("旧流应正常结束，实际 %v", err)
	}
	if msg, err := fb.Generate(t.Context(), msgs()); err != nil || msg.GetContent() != "backup ok" {
		t.Fatalf("旧流结束不能解除后来的熔断，实际 %q，%v", msg.GetContent(), err)
	}
}
