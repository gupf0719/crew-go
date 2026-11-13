package sql_test

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

func TestSQLiteAGUICancelSurvivesProcessExit(t *testing.T) {
	for _, scenario := range []string{"delete_failed", "intent_ack_lost", "killed_after_intent", "history_failed", "finalize_failed"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			for _, phase := range []string{"cancel", "restart"} {
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAGUICancelProcessWorker$")
				cmd.Env = append(os.Environ(), "CREW_TEST_CANCEL_DIR="+dir, "CREW_TEST_CANCEL_PHASE="+phase, "CREW_TEST_CANCEL_FAULT="+scenario)
				out, err := cmd.CombinedOutput()
				cancel()
				if err != nil || !strings.Contains(string(out), "verified-"+phase) {
					t.Fatalf("%s worker: %v\n%s", phase, err, out)
				}
			}
		})
	}
}

type cancelRecordStore struct {
	*crewsql.AGUISessionStore
	fault string
}

func (s cancelRecordStore) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	if s.fault == "finalize_failed" && record == nil {
		return 0, errors.New("injected failure confirming finalization")
	}
	count, err := s.AGUISessionStore.SaveSession(ctx, key, record, dropTask)
	if err != nil || record == nil || record.Cancellation == nil || dropTask != "" {
		return count, err
	}
	switch s.fault {
	case "intent_ack_lost":
		return 0, errors.New("injected loss of intent acknowledgement")
	case "killed_after_intent":
		fmt.Println("verified-cancel")
		os.Exit(0)
	}
	return count, nil
}

type cancelHistoryOwnerKey struct{}

type cancelProcessRef struct {
	SID    string
	Cancel map[string]any
}

func TestAGUICancelProcessWorker(t *testing.T) {
	dir := os.Getenv("CREW_TEST_CANCEL_DIR")
	if dir == "" {
		t.Skip("由跨进程取消测试启动")
	}
	db, cp, history := (sharedApprovalBackend{driver: "sqlite", dsn: "file:" + filepath.Join(dir, "cancel.db") + "?_pragma=busy_timeout(3000)"}).open(t)
	ctx := t.Context()
	st := crewsql.NewAGUISessionStore(cp)
	runs := crewsql.NewAGUIRunStore(cp)
	for _, setup := range []func(context.Context) error{st.Setup, history.Setup, runs.Setup} {
		if err := setup(ctx); err != nil {
			t.Fatal(err)
		}
	}
	phase, fault := os.Getenv("CREW_TEST_CANCEL_PHASE"), os.Getenv("CREW_TEST_CANCEL_FAULT")
	historyOwner := "recovery-caller"
	var toolCalls atomic.Int32
	var sessionStore agui.SessionStore = st
	if phase == "cancel" {
		historyOwner = "history-tenant"
		sessionStore = cancelRecordStore{AGUISessionStore: st, fault: fault}
	}
	base := sharedHandler(func(hook.Hooks) agent_v2.Agent {
		return sharedApprovalAgent(history, func() { toolCalls.Add(1) })
	}, sessionStore, agui.WithRunStore(runs, 0))
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCtx := context.WithValue(r.Context(), cancelHistoryOwnerKey{}, historyOwner)
		reqCtx = agent_v2.WithHistoryCtx(reqCtx, history, "", agent_v2.WithHistoryOwnerFunc(func(ctx context.Context) string {
			return ctx.Value(cancelHistoryOwnerKey{}).(string)
		}))
		base.ServeHTTP(w, r.WithContext(reqCtx))
	})
	refPath := filepath.Join(dir, "ref.json")
	switch phase {
	case "cancel":
		start := aguiPost(t, h, "tenant", map[string]any{"threadId": "shared", "runId": "start",
			"messages": []map[string]any{{"role": "user", "content": "转账"}}})
		pending := aguiPending(t, h, "tenant")
		if len(pending.Interrupts) != 1 || pending.Interrupts[0].ToolCallID != "transfer" {
			t.Fatalf("fixture did not suspend for approval: %s", start.Body)
		}
		records, err := agent_v2.LoadAllHistory(ctx, history, agent_v2.HistoryKey{Owner: historyOwner, ConvID: "shared"})
		if err != nil || len(records) < 2 {
			t.Fatalf("initial history missing: %d %v", len(records), err)
		}
		sid, _ := records[len(records)-1].Annotations[agent_v2.HistoryTaskAnnotation].(string)
		if sid == "" {
			t.Fatal("fixture did not record task identity")
		}
		ref := cancelProcessRef{SID: sid, Cancel: aguiAnswer(pending.Interrupts[0].ID, agui.ResumeStatusCancelled, nil)}
		writeCancelRef(t, refPath, ref)
		if toolCalls.Load() != 0 {
			t.Fatal("transfer executed before approval")
		}
		// 取消请求与执行请求的业务历史分区不同。必须使用写入者原先绑定的分区。
		historyOwner = "cancel-caller"
		switch fault {
		case "delete_failed":
			execCancelFixture(t, db, "CREATE TRIGGER refuse_cancel BEFORE DELETE ON "+crewsql.DefaultCheckpointTable+
				" BEGIN SELECT RAISE(ABORT, 'injected checkpoint deletion outage'); END")
		case "history_failed":
			execCancelFixture(t, db, "CREATE TRIGGER refuse_cancel BEFORE INSERT ON "+crewsql.DefaultHistoryTable+
				" BEGIN SELECT RAISE(ABORT, 'injected history append outage'); END")
		}
		response := aguiPost(t, h, "tenant", ref.Cancel)
		if !strings.Contains(response.Body.String(), "abort_failed") {
			t.Fatalf("fault did not interrupt cancellation: %d %s", response.Code, response.Body)
		}
		wantCheckpoint := fault == "delete_failed" || fault == "intent_ack_lost"
		checkCancelCheckpoint(t, cp, sid, wantCheckpoint)
		if toolCalls.Load() != 0 {
			t.Fatal("cancelled transfer executed before process exit")
		}
		fmt.Println("verified-cancel")
		os.Exit(0)
	case "restart":
		execCancelFixture(t, db, "DROP TRIGGER IF EXISTS refuse_cancel")
		ref := readCancelRef(t, refPath)
		checkCancelCheckpoint(t, cp, ref.SID, fault == "delete_failed" || fault == "intent_ack_lost" || fault == "killed_after_intent")
		ids, ok := ref.Cancel["resume"].([]any)
		if !ok || len(ids) != 1 {
			t.Fatalf("invalid reference: %+v", ref)
		}
		id := ids[0].(map[string]any)["interruptId"].(string)
		if r := aguiPost(t, h, "other", aguiAnswer(id, agui.ResumeStatusCancelled, nil)); r.Code != http.StatusNotFound {
			t.Fatalf("wrong owner reached cleanup: %d %s", r.Code, r.Body)
		}
		if fault == "killed_after_intent" {
			if r := aguiPost(t, h, "tenant", aguiAnswer(id, agui.ResumeStatusResolved, true)); !strings.Contains(r.Body.String(), `"code":"busy"`) {
				t.Fatalf("an unexpired lease should still fence the dead worker: %d %s", r.Code, r.Body)
			}
			// 推进测试库中的租约期限，模拟经过服务端 TTL。
			execCancelFixture(t, db, "UPDATE "+crewsql.DefaultCheckpointTable+"_task_lease SET expires_at = 0")
		}
		if r := aguiPost(t, h, "tenant", aguiAnswer(id, agui.ResumeStatusResolved, true)); !strings.Contains(r.Body.String(), "cleanup_pending") {
			t.Fatalf("restart did not fence off the cancelled task: %d %s", r.Code, r.Body)
		}
		if fault != "killed_after_intent" {
			r := aguiPost(t, h, "tenant", ref.Cancel)
			if !strings.Contains(r.Body.String(), "abort_failed") {
				t.Fatalf("original run receipt changed: %d %s", r.Code, r.Body)
			}
		}
		switch fault {
		case "history_failed":
			if p := aguiPending(t, h, "tenant"); len(p.Interrupts) != 0 {
				t.Fatalf("pending query did not reconcile cancellation: %+v", p)
			}
		case "finalize_failed":
			start := aguiStart()
			start["runId"] = "after-cleanup"
			r := aguiPost(t, h, "tenant", start)
			if !strings.Contains(r.Body.String(), `"type":"success"`) {
				t.Fatalf("new run did not reconcile old cancellation: %d %s", r.Code, r.Body)
			}
		default:
			r := aguiPost(t, h, "tenant", aguiAnswer(id, agui.ResumeStatusCancelled, nil))
			if !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
				t.Fatalf("cleanup retry failed: %d %s", r.Code, r.Body)
			}
		}
		checkCancelCheckpoint(t, cp, ref.SID, false)
		if p := aguiPending(t, h, "tenant"); len(p.Interrupts) != 0 {
			t.Fatalf("cancelled task remained pending: %+v", p)
		}
		records, err := agent_v2.LoadAllHistory(ctx, history, agent_v2.HistoryKey{Owner: "history-tenant", ConvID: "shared"})
		if err != nil {
			t.Fatal(err)
		}
		results := 0
		for _, record := range records {
			if schema.GetToolResultID(record) == "transfer" && strings.Contains(record.Content, "被放弃") {
				results++
			}
		}
		if results != 1 {
			t.Fatalf("want one cancelled result in the original history partition, got %d", results)
		}
		if toolCalls.Load() != 0 {
			t.Fatal("cancelled transfer executed after restart")
		}
		fmt.Println("verified-restart")
	default:
		t.Fatal("unknown worker phase")
	}
}

func TestAGUIRequiresSharedSessionForSQLCheckpointer(t *testing.T) {
	_, cp, _ := (sharedApprovalBackend{driver: "sqlite", dsn: ":memory:"}).open(t)
	defer func() {
		if got := recover(); !strings.Contains(fmt.Sprint(got), "use WithSessionStore") {
			t.Fatalf("expected to reject mixed lifetimes, got %v", got)
		}
	}()
	agui.Handler(func(hook.Hooks) agent_v2.Agent { return nil }, agui.WithCheckpointer(cp),
		agui.WithOwnerResolver(func(context.Context) string { return "tenant" }))
}

func TestAGUIRejectsRequestScopedSQLCheckpointerWithoutSharedSession(t *testing.T) {
	_, cp, _ := (sharedApprovalBackend{driver: "sqlite", dsn: "file:" + filepath.Join(t.TempDir(), "middleware.db")}).open(t)
	if err := cp.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	q := &questions{}
	h := agui.Handler(q.factory(t), agui.WithOwnerResolver(func(context.Context) string { return "tenant" }))
	r := aguiRequest(t, "tenant", http.MethodPost, aguiStart())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r.WithContext(compose_v2.WithCheckpointerCtx(r.Context(), cp)))
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "invalid_checkpointer") {
		t.Fatalf("request checkpointer bypassed handler configuration: %d %s", w.Code, w.Body)
	}
	if q.id.Load() != nil || q.first.Load() != 0 {
		t.Fatal("started a task with persistent checkpoints and no shared session")
	}
}

type checkpointOverrideHook struct {
	hook.NoOpHooks
	cp compose_v2.Checkpointer
}

func (h checkpointOverrideHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return compose_v2.WithCheckpointerCtx(ctx, h.cp), nil
}

func TestAGUIHookKeepsSessionCheckpointer(t *testing.T) {
	_, cp, _ := (sharedApprovalBackend{driver: "sqlite", dsn: "file:" + filepath.Join(t.TempDir(), "hook.db")}).open(t)
	if err := cp.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	q := &questions{}
	factory := q.factory(t)
	local := compose_v2.NewInMemoryCheckpointer()
	h := agui.Handler(func(hook.Hooks) agent_v2.Agent { return factory(checkpointOverrideHook{cp: cp}) },
		agui.WithCheckpointer(local), agui.WithOwnerResolver(func(context.Context) string { return "tenant" }))
	response := aguiPost(t, h, "tenant", aguiStart())
	sid, _ := q.id.Load().(string)
	if sid == "" || q.first.Load() != 1 {
		t.Fatalf("task did not suspend: %d %s", response.Code, response.Body)
	}
	checkCancelCheckpoint(t, cp, sid, false)
	checkCancelCheckpoint(t, local, sid, true)
	pending, err := local.LoadNS(t.Context(), sid, "")
	if err != nil || pending == nil || len(pending.PendingInterrupts) != 1 {
		t.Fatalf("local pending checkpoint: %+v %v", pending, err)
	}
	id := pending.PendingInterrupts[0].InterruptID
	if r := aguiPost(t, h, "tenant", aguiAnswer(id, "cancelled", nil)); !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
		t.Fatalf("handler could not cancel its suspended task: %d %s", r.Code, r.Body)
	}
	checkCancelCheckpoint(t, local, sid, false)
}

func execCancelFixture(t *testing.T, db *dbsql.DB, query string) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query); err != nil {
		t.Fatal(err)
	}
}

func writeCancelRef(t *testing.T, path string, ref cancelProcessRef) {
	t.Helper()
	data, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func readCancelRef(t *testing.T, path string) cancelProcessRef {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ref cancelProcessRef
	if err := json.Unmarshal(data, &ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func checkCancelCheckpoint(t *testing.T, cp compose_v2.Checkpointer, sid string, want bool) {
	t.Helper()
	checkpoints, err := cp.LoadAllInSession(t.Context(), sid)
	if err != nil || (len(checkpoints) != 0) != want {
		t.Fatalf("task %s: want checkpoint present=%v, got %d err=%v", sid, want, len(checkpoints), err)
	}
}
