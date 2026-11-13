package sql_test

import (
	"context"
	dbsql "database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	crewsql "crew-go/sql"
)

type failingCancelSessionStore struct {
	*crewsql.AGUISessionStore
	key    string
	before func(context.Context, string, *agui.SessionRecord, string) error
}

func testIdleCancellationShutdown(t *testing.T, f aguiFixture) {
	t.Helper()
	history := agent_v2.NewInMemoryHistoryStore()
	fixture := &questions{}
	factory := fixture.factory(t)
	store := &failingCancelSessionStore{AGUISessionStore: f.a}
	store.before = func(_ context.Context, _ string, r *agui.SessionRecord, drop string) error {
		if r != nil && r.Cancellation != nil && drop != "" {
			return errors.New("delete unavailable")
		}
		return nil
	}
	base := sharedHandler(factory, store)
	withHistory := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base.ServeHTTP(w, r.WithContext(agent_v2.WithHistoryCtx(r.Context(), history, "history-tenant")))
	})
	aguiPost(t, withHistory, "tenant", aguiStart())
	id := onlyAGUIQuestion(t, aguiPending(t, withHistory, "tenant"), "q1")
	sid := fixture.id.Load().(string)
	if r := aguiPost(t, withHistory, "tenant", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), "abort_failed") {
		t.Fatal(r.Body)
	}
	store.before = nil
	started := make(chan struct{}, 1)
	released := make(chan struct{})
	a := agui.Handler(factory, agui.WithSessionStore(f.a), agui.WithOwnerResolver(func(context.Context) string { return "tenant" }),
		agui.WithCancellationHistory(func(ctx context.Context, _ agent_v2.HistoryKey) (agent_v2.HistoryStore, error) {
			started <- struct{}{}
			<-ctx.Done()
			close(released)
			return nil, ctx.Err()
		}))
	t.Cleanup(func() { _ = a.Close() })
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("idle recovery did not start history finalization")
	}
	checkCancelCheckpoint(t, f.b, sid, false)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_ = a.Close()
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("closing server did not cancel and join background recovery")
	}
	select {
	case <-released:
	default:
		t.Fatal("close returned before the resolver exited")
	}
	saved, err := f.b.LoadSession(t.Context(), store.key)
	if err != nil || saved == nil || saved.Cancellation == nil || !saved.Cancellation.CheckpointsDeleted {
		t.Fatalf("close lost durable progress: %+v %v", saved, err)
	}
	b := agui.Handler(factory, agui.WithSessionStore(f.b), agui.WithOwnerResolver(func(context.Context) string { return "tenant" }),
		agui.WithCancellationHistory(func(context.Context, agent_v2.HistoryKey) (agent_v2.HistoryStore, error) { return history, nil }))
	defer b.Close()
	waitIdleCancel(t, func() (bool, error) {
		saved, err := f.b.LoadSession(t.Context(), store.key)
		return saved == nil, err
	})
	if fixture.second.Load() != 0 {
		t.Fatal("background recovery ran a suspended node")
	}
}

func (s *failingCancelSessionStore) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, drop string) (int32, error) {
	s.key = key
	if s.before != nil {
		if err := s.before(ctx, key, record, drop); err != nil {
			return 0, err
		}
	}
	return s.AGUISessionStore.SaveSession(ctx, key, record, drop)
}

func testDurableCancelWorkflowRecovery(t *testing.T, fixture func(*testing.T) aguiFixture) {
	t.Helper()
	for _, entrance := range []string{"get", "new_run", "cancel"} {
		t.Run(entrance, func(t *testing.T) {
			f := fixture(t)
			oldQ, newQ := &questions{}, &questions{}
			oldFactory, newFactory := oldQ.factory(t), newQ.factory(t)
			store := &failingCancelSessionStore{AGUISessionStore: f.a}
			store.before = func(_ context.Context, _ string, r *agui.SessionRecord, drop string) error {
				if r != nil && r.Cancellation != nil && drop != "" {
					return errors.New("delete unavailable")
				}
				return nil
			}
			a := sharedHandler(oldFactory, store, agui.WithWorkflowVersions("old", nil))
			aguiPost(t, a, "alice", aguiStart())
			id := onlyAGUIQuestion(t, aguiPending(t, a, "alice"), "q1")
			sid := oldQ.id.Load().(string)
			if r := aguiPost(t, a, "alice", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), "abort_failed") {
				t.Fatal(r.Body)
			}
			stored, err := f.b.LoadSession(t.Context(), store.key)
			if err != nil || stored == nil || !stored.Closing || stored.Cancellation == nil || stored.Cancellation.CheckpointsDeleted {
				t.Fatalf("intent not retained: %+v %v", stored, err)
			}
			missing := sharedHandler(newFactory, f.b, agui.WithWorkflowVersions("new", nil))
			for _, how := range []string{"get", "new_run", "cancel"} {
				var response *httptest.ResponseRecorder
				if how == "get" {
					response = httptest.NewRecorder()
					missing.ServeHTTP(response, aguiRequest(t, "alice", http.MethodGet, nil))
				} else if how == "new_run" {
					response = aguiPost(t, missing, "alice", aguiStart())
				} else {
					response = aguiPost(t, missing, "alice", aguiAnswer(id, "cancelled", nil))
				}
				if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "workflow_version_unavailable") {
					t.Fatalf("missing version %s: %d %s", how, response.Code, response.Body)
				}
			}
			if newQ.first.Load() != 0 || oldQ.second.Load() != 0 {
				t.Fatal("missing definition executed business")
			}
			store.before = nil
			recovery := sharedHandler(newFactory, f.b, agui.WithWorkflowVersions("new", map[string]agui.AgentFactory{"old": oldFactory}))
			switch entrance {
			case "get":
				if p := aguiPending(t, recovery, "alice"); len(p.Interrupts) != 0 {
					t.Fatalf("pending=%+v", p)
				}
			case "cancel":
				if r := aguiPost(t, recovery, "alice", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
					t.Fatal(r.Body)
				}
			case "new_run":
				if r := aguiPost(t, recovery, "alice", aguiStart()); !strings.Contains(r.Body.String(), `"type":"interrupt"`) {
					t.Fatal(r.Body)
				}
				newID := onlyAGUIQuestion(t, aguiPending(t, recovery, "alice"), "q1")
				newSID := newQ.id.Load().(string)
				if newID == id || newSID == sid {
					t.Fatal("replacement reused old identity")
				}
				if r := aguiPost(t, recovery, "alice", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), "invalid_resume") {
					t.Fatalf("old cancel touched replacement: %s", r.Body)
				}
				checkCancelCheckpoint(t, f.b, newSID, true)
				if got := onlyAGUIQuestion(t, aguiPending(t, recovery, "alice"), "q1"); got != newID {
					t.Fatal("old cancel replaced new question")
				}
			}
			checkCancelCheckpoint(t, f.b, sid, false)
			if oldQ.second.Load() != 0 {
				t.Fatal("cancelled business resumed")
			}
		})
	}
}

func testDurableCancelFences(t *testing.T, fixture func(*testing.T) aguiFixture) {
	t.Helper()
	for _, phase := range []string{"delete", "finalize"} {
		for _, lose := range []string{"thread", "task"} {
			t.Run(phase+"_"+lose, func(t *testing.T) {
				f := fixture(t)
				q := &questions{}
				factory := q.factory(t)
				store := &failingCancelSessionStore{AGUISessionStore: f.a}
				a := sharedHandler(factory, store)
				aguiPost(t, a, "alice", aguiStart())
				id := onlyAGUIQuestion(t, aguiPending(t, a, "alice"), "q1")
				sid := q.id.Load().(string)
				var hijacked string
				store.before = func(ctx context.Context, key string, r *agui.SessionRecord, drop string) error {
					if hijacked != "" || drop == "" || (phase == "finalize") != (r == nil) {
						return nil
					}
					leaseID := sid
					if lose == "thread" {
						leaseID = key
					}
					leases := f.b.CheckpointTaskLeases()
					if err := leases.ReleaseTaskLease(t.Context(), leaseID, compose_v2.CheckpointTaskLeaseToken(ctx, f.a.CheckpointTaskLeases(), leaseID)); err != nil {
						return err
					}
					ok, err := leases.TryAcquireTaskLease(t.Context(), leaseID, "successor", time.Minute)
					if err != nil {
						return err
					}
					if !ok {
						return errors.New("takeover fixture failed")
					}
					hijacked = leaseID
					return nil
				}
				if r := aguiPost(t, a, "alice", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), "abort_failed") {
					t.Fatalf("stale mutation succeeded: %d %s", r.Code, r.Body)
				}
				if hijacked == "" {
					t.Fatal("takeover fixture never ran")
				}
				if ok, err := f.b.CheckpointTaskLeases().RenewTaskLease(t.Context(), hijacked, "successor", time.Minute); err != nil || !ok {
					t.Fatalf("old release revoked successor lease: %v %v", ok, err)
				}
				stored, err := f.b.LoadSession(t.Context(), store.key)
				if err != nil || stored == nil || stored.Cancellation == nil || stored.Cancellation.CheckpointsDeleted != (phase == "finalize") {
					t.Fatalf("stale request advanced cancellation: %+v %v", stored, err)
				}
				checkCancelCheckpoint(t, f.b, sid, phase == "delete")
				if err := f.b.CheckpointTaskLeases().ReleaseTaskLease(t.Context(), hijacked, "successor"); err != nil {
					t.Fatal(err)
				}
				if r := aguiPost(t, sharedHandler(factory, f.b), "alice", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
					t.Fatal(r.Body)
				}
				checkCancelCheckpoint(t, f.b, sid, false)
				if q.second.Load() != 0 {
					t.Fatal("cancelled business resumed")
				}
			})
		}
	}
}

func TestSQLiteAGUIRecoveryLeaderYieldsAndSlowItemDoesNotBlockPeer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leadership.db")
	fixture := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
	q := &questions{}
	factory := q.factory(t)
	makeIntent := func(owner, version string) string {
		t.Helper()
		store := &failingCancelSessionStore{AGUISessionStore: fixture.a, before: func(_ context.Context, _ string, _ *agui.SessionRecord, drop string) error {
			if drop != "" {
				return errors.New("hold cancellation")
			}
			return nil
		}}
		h := sharedHandler(factory, store, agui.WithWorkflowVersions(version, nil))
		aguiPost(t, h, owner, aguiStart())
		id := onlyAGUIQuestion(t, aguiPending(t, h, owner), "q1")
		if r := aguiPost(t, h, owner, aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), "abort_failed") {
			t.Fatal(r.Body)
		}
		return store.key
	}
	unavailable := makeIntent("legacy", "v1")
	slow := makeIntent("slow", "slow")
	peer := makeIntent("peer", "v2")
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	primary := agui.Handler(factory, agui.WithSessionStore(fixture.a), agui.WithOwnerResolver(func(context.Context) string { return "unused" }),
		agui.WithWorkflowVersions("v2", map[string]agui.AgentFactory{"slow": func(h hook.Hooks) agent_v2.Agent {
			close(reached)
			<-release
			return factory(h)
		}}))
	defer func() { once.Do(func() { close(release) }); _ = primary.Close() }()
	select {
	case <-reached:
	case <-time.After(3 * time.Second):
		t.Fatal("primary did not start slow cancellation")
	}
	waitIdleCancel(t, func() (bool, error) { r, e := fixture.b.LoadSession(t.Context(), peer); return r == nil, e })
	if r, e := fixture.b.LoadSession(t.Context(), slow); e != nil || r == nil {
		t.Fatalf("slow fixture unexpectedly finished: %v %v", r, e)
	}
	follower := agui.Handler(factory, agui.WithSessionStore(fixture.b), agui.WithOwnerResolver(func(context.Context) string { return "unused" }),
		agui.WithWorkflowVersions("v2", map[string]agui.AgentFactory{"v1": factory, "slow": factory}))
	defer follower.Close()
	once.Do(func() { close(release) })
	waitIdleCancel(t, func() (bool, error) { r, e := fixture.b.LoadSession(t.Context(), unavailable); return r == nil, e })
}
