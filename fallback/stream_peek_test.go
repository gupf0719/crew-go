package fallback_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

type prefixStreamMember struct {
	stubModel
	prefix   []*schema.Message
	end      error
	closed   chan struct{}
	blocking bool
}

func (m *prefixStreamMember) Stream(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.record(in)
	sr, send := schema.NewStreamReaderWithCancel[*schema.Message](len(m.prefix), func() { close(m.closed) })
	for _, msg := range m.prefix {
		send(msg)
	}
	if !m.blocking {
		schema.SendError(sr, m.end)
		schema.CloseSend(sr)
	}
	return sr, nil
}

func TestFallback_SwitchesBeforeVisibleStreamOutput(t *testing.T) {
	primary := &prefixStreamMember{stubModel: stubModel{name: "primary"},
		prefix: []*schema.Message{
			{Role: schema.RoleAssistant, Metadata: &schema.MessageMetadata{Usage: &schema.MessageUsage{InputTokens: 999}}},
			{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{nil}},
		}, end: apiErr(503), closed: make(chan struct{})}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))
	sr, err := fb.Stream(t.Context(), msgs())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	msg, err := sr.Recv(t.Context())
	if err != nil || msg == nil || msg.Content != "backup stream" || backup.callCount() != 1 {
		t.Fatalf("first read should come only from the backup: %v, %v, backup calls=%d", msg, err, backup.callCount())
	}
	if _, err := sr.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("backup stream should finish: %v", err)
	}
	select {
	case <-primary.closed:
	default:
		t.Fatal("failed primary stream was not closed")
	}
	if _, err := fb.Generate(t.Context(), msgs()); err != nil || primary.callCount() != 1 {
		t.Fatalf("first-read failure should have tripped primary: %v, calls=%d", err, primary.callCount())
	}
}

func TestFallback_PeekPreservesSelectedFrames(t *testing.T) {
	chunks := []*schema.Message{{Role: schema.RoleAssistant}, {Role: schema.RoleAssistant, ReasoningContent: "think"}, schema.AssistantMessage("answer")}
	primary := &prefixStreamMember{stubModel: stubModel{name: "primary"}, prefix: chunks, closed: make(chan struct{})}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})
	sr, err := fb.Stream(t.Context(), msgs())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	for i, want := range chunks {
		if msg, err := sr.Recv(t.Context()); err != nil || msg != want {
			t.Fatalf("chunk %d was reordered or replaced: %v %v", i, msg, err)
		}
	}
	if _, err := sr.Recv(t.Context()); !errors.Is(err, io.EOF) || backup.callCount() != 0 {
		t.Fatalf("expected one complete stream: %v, backup calls=%d", err, backup.callCount())
	}
}

func TestFallback_PeekHonorsCancellation(t *testing.T) {
	primary := &prefixStreamMember{stubModel: stubModel{name: "primary"}, closed: make(chan struct{}), blocking: true}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if sr, err := fb.Stream(ctx, msgs()); sr != nil || !errors.Is(err, context.DeadlineExceeded) || backup.callCount() != 0 {
		t.Fatalf("deadline should close the first candidate without a switch: %v %v", sr, err)
	}
	select {
	case <-primary.closed:
	default:
		t.Fatal("cancelled stream was not closed")
	}
}

func TestFallback_NestedPeekLeavesTheTerminalForTheConsumer(t *testing.T) {
	primary := &prefixStreamMember{stubModel: stubModel{name: "primary"},
		prefix: []*schema.Message{schema.AssistantMessage("first")}, end: apiErr(503), closed: make(chan struct{})}
	backup := &stubModel{name: "backup"}
	inner := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))
	outer := fallback.New([]model.ChatModel{inner})
	sr, err := outer.Stream(t.Context(), msgs())
	if err != nil {
		t.Fatal(err)
	}
	sr.Close()
	msg, err := inner.Generate(t.Context(), msgs())
	if err != nil || msg.Content != "primary ok" || backup.callCount() != 0 {
		t.Fatalf("unused stream should release the inner probe without failure: %v %v", msg, err)
	}
}

func TestFallback_NilOnlyStreamSwitches(t *testing.T) {
	primary := &prefixStreamMember{stubModel: stubModel{name: "primary"},
		prefix: []*schema.Message{nil}, closed: make(chan struct{})}
	backup := &stubModel{name: "backup"}
	sr, err := fallback.New([]model.ChatModel{primary, backup}).Stream(t.Context(), msgs())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	if msg, err := sr.Recv(t.Context()); err != nil || msg == nil || msg.Content != "backup stream" || backup.callCount() != 1 {
		t.Fatalf("nil-only stream should fail over: %v %v", msg, err)
	}
}
