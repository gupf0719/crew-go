package sql_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

func TestSQLiteAGUIResumeKeepsExecutionHistoryPartition(t *testing.T) {
	_, cp, history := (sharedApprovalBackend{driver: "sqlite", dsn: "file:" + filepath.Join(t.TempDir(), "history.db") + "?_pragma=busy_timeout(3000)"}).open(t)
	st := crewsql.NewAGUISessionStore(cp)
	if err := st.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := history.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	runs := 0
	base := sharedHandler(func(hook.Hooks) agent_v2.Agent { return sharedApprovalAgent(history, func() { runs++ }) }, st)
	historyOwner := "execution-partition"
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), cancelHistoryOwnerKey{}, historyOwner)
		ctx = agent_v2.WithHistoryCtx(ctx, history, "", agent_v2.WithHistoryOwnerFunc(func(ctx context.Context) string { return ctx.Value(cancelHistoryOwnerKey{}).(string) }))
		base.ServeHTTP(w, r.WithContext(ctx))
	})
	aguiPost(t, h, "same-auth-owner", map[string]any{"threadId": "shared", "messages": []map[string]any{{"role": "user", "content": "转账"}}})
	pending := aguiPending(t, h, "same-auth-owner")
	if len(pending.Interrupts) != 1 {
		t.Fatal(pending)
	}
	historyOwner = "resume-request-partition"
	if r := aguiPost(t, h, "same-auth-owner", aguiAnswer(pending.Interrupts[0].ID, "resolved", true)); !strings.Contains(r.Body.String(), `"type":"success"`) {
		t.Fatal(r.Body)
	}
	final, err := agent_v2.LoadAllHistory(t.Context(), history, agent_v2.HistoryKey{Owner: "execution-partition", ConvID: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	results := 0
	for _, msg := range final {
		if schema.GetToolResultID(msg) == "transfer" {
			results++
		}
	}
	other, err := agent_v2.LoadAllHistory(t.Context(), history, agent_v2.HistoryKey{Owner: "resume-request-partition", ConvID: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	if results != 1 || runs != 1 || len(other) != 0 {
		t.Fatalf("resume split one task across history partitions: original results=%d tool runs=%d other records=%d", results, runs, len(other))
	}
}

func TestSQLiteAGUIResumeKeepsPendingWhenHistoryStoreUnavailable(t *testing.T) {
	_, cp, history := (sharedApprovalBackend{driver: "sqlite", dsn: "file:" + filepath.Join(t.TempDir(), "history.db")}).open(t)
	st := crewsql.NewAGUISessionStore(cp)
	if err := st.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := history.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	q := &questions{}
	base := sharedHandler(q.factory(t), st)
	withHistory := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base.ServeHTTP(w, r.WithContext(agent_v2.WithHistoryCtx(r.Context(), history, "tenant")))
	})
	aguiPost(t, withHistory, "tenant", aguiStart())
	id := onlyAGUIQuestion(t, aguiPending(t, withHistory, "tenant"), "q1")
	sid := q.id.Load().(string)
	r := aguiPost(t, base, "tenant", aguiAnswer(id, "resolved", "one"))
	checkCancelCheckpoint(t, cp, sid, true)
	if !strings.Contains(r.Body.String(), "session_store_failed") {
		t.Fatalf("resume should wait for the original history store: %d %s", r.Code, r.Body)
	}
	pending := aguiPending(t, withHistory, "tenant")
	if got := onlyAGUIQuestion(t, pending, "q1"); got != id || q.second.Load() != 0 || len(pending.Accepted) != 1 {
		t.Fatalf("failed history binding advanced the task or lost its answer: %+v", pending)
	}
	if r := aguiPost(t, withHistory, "tenant", aguiAnswer(id, "resolved", "one")); !strings.Contains(r.Body.String(), `"type":"interrupt"`) {
		t.Fatalf("resume after restoring history: %d %s", r.Code, r.Body)
	}
	onlyAGUIQuestion(t, aguiPending(t, withHistory, "tenant"), "q2")
}

func TestSQLiteAGUICancelRemainsClosedWhenHistoryStoreUnavailable(t *testing.T) {
	_, cp, history := (sharedApprovalBackend{driver: "sqlite", dsn: "file:" + filepath.Join(t.TempDir(), "history.db")}).open(t)
	st := crewsql.NewAGUISessionStore(cp)
	if err := st.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := history.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	q := &questions{}
	factory := q.factory(t)
	base := sharedHandler(factory, st)
	withHistory := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base.ServeHTTP(w, r.WithContext(agent_v2.WithHistoryCtx(r.Context(), history, "tenant")))
	})
	aguiPost(t, withHistory, "tenant", aguiStart())
	id := onlyAGUIQuestion(t, aguiPending(t, withHistory, "tenant"), "q1")
	sid := q.id.Load().(string)
	if r := aguiPost(t, base, "tenant", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), "abort_failed") {
		t.Fatalf("cancellation should wait for history finalization: %d %s", r.Code, r.Body)
	}
	checkCancelCheckpoint(t, cp, sid, false)
	newHandler := sharedHandler(factory, st)
	if r := aguiPost(t, newHandler, "tenant", aguiAnswer(id, "resolved", "one")); r.Code != http.StatusConflict ||
		!strings.Contains(r.Body.String(), "cleanup_pending") || q.second.Load() != 0 {
		t.Fatalf("new instance drove a cancelled task: %d %s", r.Code, r.Body)
	}
	w := httptest.NewRecorder()
	newHandler.ServeHTTP(w, aguiRequest(t, "tenant", http.MethodGet, nil))
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "session_store_failed") {
		t.Fatalf("missing history configuration finalized the cancellation: %d %s", w.Code, w.Body)
	}
	if r := aguiPost(t, withHistory, "tenant", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
		t.Fatalf("retry after restoring history: %d %s", r.Code, r.Body)
	}
	if pending := aguiPending(t, newHandler, "tenant"); len(pending.Interrupts) != 0 {
		t.Fatalf("cancelled task remained pending: %+v", pending)
	}
}
