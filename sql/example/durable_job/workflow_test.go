package main

import (
	"context"
	dbsql "database/sql"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
)

type earlyCallbackService struct {
	buildService
	notify func(ctx context.Context, id string) error
}

func (s earlyCallbackService) submit(ctx context.Context, id, input string) (string, error) {
	handle, err := s.buildService.submit(ctx, id, input)
	if err != nil {
		return "", err
	}
	return handle, s.notify(ctx, id)
}

func TestDurableJob_RecoverAfterSubmitBeforeHandleWrite(t *testing.T) {
	a, b, provider := fixture(t)
	ctx := t.Context()
	if err := a.create(ctx, "first", "request", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	handle, err := provider.submit(ctx, "first", "request")
	if err != nil {
		t.Fatal(err)
	}
	j, err := a.load(ctx, "first")
	if err != nil || j.handle != "" {
		t.Fatalf("fixture already saved the handle: %+v, %v", j, err)
	}
	if err := a.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcile(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := provider.complete(ctx, "first", "artifact-first"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if status := sendCallback(ctx, b, "first"); status != http.StatusAccepted {
			t.Fatalf("callback status %d", status)
		}
	}
	assertJob(t, b, "first", handle, completed, "artifact-first")
	assertBuildCount(t, provider.db, 1)
}

func TestDurableJob_CallbackBeforeCheckpointWakesOriginalTask(t *testing.T) {
	a, b, provider := fixture(t)
	ctx := t.Context()
	if err := a.create(ctx, "early", "request", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	early := 0
	a.service = earlyCallbackService{provider, func(nodeCtx context.Context, id string) error {
		if cp, err := b.cp.LoadNS(ctx, sidPrefix+id, ""); err != nil || cp != nil {
			t.Errorf("callback should precede checkpoint: cp=%v err=%v", cp, err)
		}
		if err := provider.complete(nodeCtx, id, "early-artifact"); err != nil {
			return err
		}
		early = sendCallback(ctx, b, id) // 模拟独立 HTTP 请求，不沿用图的租约。
		return nil
	}}
	if err := a.drive(ctx, "early"); err != nil {
		t.Fatal(err)
	}
	if early != http.StatusAccepted {
		t.Fatalf("early callback status %d", early)
	}
	assertJob(t, b, "early", "", completed, "early-artifact")
	assertBuildCount(t, provider.db, 1)
}

func TestDurableJob_CompletionTimeoutCancelHaveOneOutcome(t *testing.T) {
	a, b, provider := fixture(t)
	ctx := t.Context()
	now := time.Now()
	for _, id := range []string{"alpha", "beta"} {
		deadline := now.Add(time.Hour)
		if id == "alpha" {
			deadline = now
		}
		if err := a.create(ctx, id, "request-"+id, deadline); err != nil {
			t.Fatal(err)
		}
		if err := a.drive(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := provider.complete(ctx, id, "artifact-"+id); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		if status := sendCallback(ctx, a, "alpha"); status != http.StatusAccepted {
			t.Errorf("callback status %d", status)
		}
	})
	wg.Go(func() {
		<-start
		if err := b.choose(ctx, "alpha", cancelled, ""); err != nil {
			t.Error(err)
		}
	})
	wg.Go(func() {
		<-start
		if err := b.choose(ctx, "alpha", timedOut, ""); err != nil {
			t.Error(err)
		}
	})
	close(start)
	wg.Wait()
	if err := b.reconcile(ctx, now); err != nil {
		t.Fatal(err)
	}
	winner, err := b.load(ctx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	switch winner.decision {
	case completed:
		assertJob(t, b, "alpha", winner.handle, completed, "artifact-alpha")
	case cancelled, timedOut:
		assertJob(t, b, "alpha", winner.handle, winner.decision, "")
	default:
		t.Fatalf("invalid outcome %+v", winner)
	}
	if status := sendCallback(ctx, b, "beta"); status != http.StatusAccepted {
		t.Fatalf("callback status %d", status)
	}
	assertJob(t, b, "beta", "", completed, "artifact-beta")
	if status := sendCallback(ctx, a, "alpha"); status != http.StatusAccepted {
		t.Fatalf("duplicate callback status %d", status)
	}
	if err := a.choose(ctx, "alpha", cancelled, ""); err != nil {
		t.Fatal(err)
	}
	assertJob(t, b, "alpha", winner.handle, winner.decision, winner.result)
	assertBuildCount(t, provider.db, 2)
}

func TestDurableJob_ScanRecoversMissedNotificationBeforeTimeout(t *testing.T) {
	a, b, provider := fixture(t)
	ctx := t.Context()
	now := time.Now()
	if err := a.create(ctx, "missed", "request", now); err != nil {
		t.Fatal(err)
	}
	if err := a.drive(ctx, "missed"); err != nil {
		t.Fatal(err)
	}
	if err := provider.complete(ctx, "missed", "recovered-artifact"); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcile(ctx, now); err != nil {
		t.Fatal(err)
	}
	assertJob(t, b, "missed", "", completed, "recovered-artifact")
	assertBuildCount(t, provider.db, 1)
}

func TestDurableJob_ExpiredIntentDoesNotSubmit(t *testing.T) {
	a, b, provider := fixture(t)
	ctx := t.Context()
	now := time.Now()
	if err := a.create(ctx, "expired", "request", now); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcile(ctx, now); err != nil {
		t.Fatal(err)
	}
	j, err := b.load(ctx, "expired")
	if err != nil || !j.delivered || j.decision != timedOut || j.handle != "" {
		t.Fatalf("expired intent should terminate without a handle: %+v %v", j, err)
	}
	assertBuildCount(t, provider.db, 0)
}

func TestDurableJob_LateNotificationAfterCancellationAcknowledged(t *testing.T) {
	a, b, provider := fixture(t)
	ctx := t.Context()
	if err := a.create(ctx, "cancel", "request", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := a.drive(ctx, "cancel"); err != nil {
		t.Fatal(err)
	}
	if err := b.choose(ctx, "cancel", cancelled, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.drive(ctx, "cancel"); err != nil {
		t.Fatal(err)
	}
	assertJob(t, b, "cancel", "", cancelled, "")
	remote, err := provider.lookup(ctx, "cancel")
	if err != nil || remote == nil || remote.completed {
		t.Fatalf("fixture should leave a cancelled remote job: %+v %v", remote, err)
	}
	if status := sendCallback(ctx, a, "cancel"); status != http.StatusAccepted {
		t.Fatalf("late callback status %d", status)
	}
	assertJob(t, a, "cancel", remote.handle, cancelled, "")
	assertBuildCount(t, provider.db, 1)
}

func fixture(t *testing.T) (*runner, *runner, buildService) {
	t.Helper()
	dir := t.TempDir()
	open := func(name string) *dbsql.DB {
		db, err := openDatabase(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() }) // A 可能已经主动断开。
		return db
	}
	provider, err := newBuildService(t.Context(), open("builds.db"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := newRunner(t.Context(), open("workflow.db"), provider)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newRunner(t.Context(), open("workflow.db"), provider)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, provider
}

func assertJob(t *testing.T, r *runner, id, handle, decision, result string) {
	t.Helper()
	j, err := r.load(t.Context(), id)
	if err != nil || !j.delivered || j.decision != decision || j.result != result || j.handle == "" || (handle != "" && j.handle != handle) {
		t.Fatalf("unexpected delivered job %+v, err=%v", j, err)
	}
	all, err := r.cp.LoadAllInSession(compose_v2.WithCheckpointerCtx(t.Context(), r.cp), sidPrefix+id)
	if err != nil || len(all) != 0 {
		t.Fatalf("job %q left %d checkpoints: %v", id, len(all), err)
	}
}

func assertBuildCount(t *testing.T, db *dbsql.DB, want int) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM builds").Scan(&n); err != nil || n != want {
		t.Fatalf("external system created %d builds, want %d: %v", n, want, err)
	}
}
