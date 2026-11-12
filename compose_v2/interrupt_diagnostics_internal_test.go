package compose_v2

import (
	"bytes"
	"log"
	"log/slog"
	"strings"
	"testing"

	"crew-go/schema"
)

func TestResumeWarningDistinguishesReplayedAndNewAnswers(t *testing.T) {
	var output bytes.Buffer
	previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(writer)
		log.SetFlags(flags)
	})
	ctx := WithReplayedResumeAnswersCtx(t.Context(), []string{"already-answered"})
	cp := &Checkpoint{PendingInterrupts: []*AskQuery{{InterruptID: "current"}}}
	answers := map[string]*schema.Message{
		"already-answered": schema.UserMessage("old answer"),
		"current":          schema.UserMessage("new answer"),
		"typo":             schema.UserMessage("unexpected answer"),
	}
	warnUnmatchedAnswerIDs(ctx, cp, answers)
	got := output.String()
	if !strings.Contains(got, "unmatched=[typo]") || strings.Contains(got, "already-answered") {
		t.Fatalf("warning should only report the unknown new answer, got %s", got)
	}
}
