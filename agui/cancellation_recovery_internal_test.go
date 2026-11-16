package agui

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/internal/panicerr"
)

type panickingCancellationPollStore struct{}

func (panickingCancellationPollStore) TryAcquireCancellationPoll(context.Context, string, time.Duration) (bool, error) {
	panic("acquire cancellation poll")
}

func (panickingCancellationPollStore) ReleaseCancellationPoll(context.Context, string) error {
	panic("release cancellation poll")
}

func TestCancellationPollStorePanicsAreIsolated(t *testing.T) {
	ctx := t.Context()
	store := panickingCancellationPollStore{}
	h := &handler{}
	token, err := h.scanCancellations(ctx, store, &cancellationRecovery{})
	var panicRecord *panicerr.Error
	if token == "" || !errors.As(err, &panicRecord) || panicRecord.Value != "acquire cancellation poll" {
		t.Fatalf("acquire should retain the token for a conditional release, token=%q err=%v", token, err)
	}
	err = releaseCancellationPoll(ctx, store, token)
	if !errors.As(err, &panicRecord) || panicRecord.Value != "release cancellation poll" {
		t.Fatalf("release panic should be reported as an error, got %v", err)
	}
}
