package schema_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"crew-go/schema"
)

func TestStreamReadAheadPreservesPrefetchedItemAndDefersObserver(t *testing.T) {
	sr, send := schema.NewStreamReader[string](2)
	send("")
	send("ready")
	schema.SendError(sr, io.ErrUnexpectedEOF)
	schema.CloseSend(sr)
	var terminal error
	sr.ObserveEnd(func(err error) { terminal = err })
	if count, err := sr.ReadAhead(t.Context(), func(s string) bool { return s == "ready" }); count != 2 || err != nil {
		t.Fatalf("read ahead: %d %v", count, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, want := range []string{"", "ready"} {
		if item, err := sr.Recv(ctx); item != want || err != nil || terminal != nil {
			t.Fatalf("prefetched %q: %q %v, observer=%v", want, item, err, terminal)
		}
	}
	if _, err := sr.Recv(t.Context()); !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(terminal, io.ErrUnexpectedEOF) {
		t.Fatalf("terminal error was lost: %v observer=%v", err, terminal)
	}
}

func TestStreamReadAheadTerminalIsObservedByConsumer(t *testing.T) {
	sr := schema.NewSingleChunkReader("metadata")
	var terminal error
	sr.ObserveEnd(func(err error) { terminal = err })
	count, err := sr.ReadAhead(t.Context(), func(string) bool { return false })
	if count != 1 || !errors.Is(err, io.EOF) || terminal != nil {
		t.Fatalf("only prefetch should see EOF: %d %v observer=%v", count, err, terminal)
	}
	sr.Close()
	if !errors.Is(terminal, schema.ErrStreamCanceled) {
		t.Fatalf("unused stream should record consumer close: %v", terminal)
	}
}
