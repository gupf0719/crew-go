package fallback_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

type terminalStreamMember struct {
	stubModel
	terminalErr error
}

func (m *terminalStreamMember) Stream(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.record(in)
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(schema.AssistantMessage(m.name + ": " + in[0].Content))
	schema.SendError(sr, m.terminalErr)
	schema.CloseSend(sr)
	return sr, nil
}

func TestFallback_StreamFailureTripsCandidateAfterPartialOutput(t *testing.T) {
	ctx := t.Context()
	broken := netErr(errors.New("connection lost"))
	primary := &terminalStreamMember{stubModel: stubModel{name: "primary"}, terminalErr: broken}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))
	sr, err := fb.Stream(ctx, msgs())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	if msg, err := sr.Recv(ctx); err != nil || msg == nil || msg.Content != "primary: q" {
		t.Fatalf("主候选应交出已有片段，实际 %v, %v", msg, err)
	}
	if _, err := sr.Recv(ctx); !errors.Is(err, broken) {
		t.Fatalf("原流错误应直接交给消费方，实际 %v", err)
	}
	if backup.callCount() != 0 {
		t.Fatal("已建立的流中途失败不应切换候选")
	}
	next, err := fb.Stream(ctx, msgs())
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, next); got != "backup stream" || primary.callCount() != 1 {
		t.Fatalf("下次请求应跳过已熔断的主候选，实际 %q，主候选调用 %d 次", got, primary.callCount())
	}
}

func TestFallback_StreamCloseDoesNotTripCandidate(t *testing.T) {
	ctx := t.Context()
	primary := &terminalStreamMember{stubModel: stubModel{name: "primary"}, terminalErr: apiErr(503)}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))
	sr, err := fb.Stream(ctx, msgs())
	if err != nil {
		t.Fatal(err)
	}
	sr.Close()
	if msg, err := fb.Generate(ctx, msgs()); err != nil || msg.Content != "primary ok" || backup.callCount() != 0 {
		t.Fatalf("提前关流不应计候选失败，实际 %v, %v", msg, err)
	}
}
