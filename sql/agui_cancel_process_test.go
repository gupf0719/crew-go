package sql_test

import (
	"context"
	"crypto/sha256"
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
	runSQLiteAGUICancelProcesses(t, "restart", "delete_failed", "intent_ack_lost", "killed_after_intent", "history_failed", "finalize_failed")
}

func TestSQLiteAGUICancelRecoversWithoutRequests(t *testing.T) {
	runSQLiteAGUICancelProcesses(t, "idle", "delete_failed", "killed_after_intent", "history_failed", "legacy")
}

func runSQLiteAGUICancelProcesses(t *testing.T, restart string, faults ...string) {
	t.Helper()
	for _, fault := range faults {
		t.Run(fault, func(t *testing.T) {
			dir := t.TempDir()
			for _, phase := range []string{"cancel", restart} {
				ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAGUICancelProcessWorker$")
				cmd.Env = append(os.Environ(), "CREW_TEST_CANCEL_DIR="+dir, "CREW_TEST_CANCEL_PHASE="+phase, "CREW_TEST_CANCEL_FAULT="+fault)
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
	refPath := filepath.Join(dir, "ref.json")
	if phase == "idle" {
		verifyIdleCancelRecovery(t, db, cp, st, history, readCancelRef(t, refPath), fault)
		fmt.Println("verified-idle")
		return
	}
	historyOwner := "recovery-caller"
	var toolCalls atomic.Int32
	var sessionStore agui.SessionStore = st
	if phase == "cancel" {
		historyOwner = "history-tenant"
		sessionStore = cancelRecordStore{AGUISessionStore: st, fault: fault}
	}
	base := sharedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return sharedApprovalAgent(history, func() { toolCalls.Add(1) }, agent_v2.WithHooks(hooks))
	}, sessionStore, agui.WithRunStore(runs, 0))
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCtx := context.WithValue(r.Context(), cancelHistoryOwnerKey{}, historyOwner)
		reqCtx = agent_v2.WithHistoryCtx(reqCtx, history, "", agent_v2.WithHistoryOwnerFunc(func(ctx context.Context) string {
			return ctx.Value(cancelHistoryOwnerKey{}).(string)
		}))
		base.ServeHTTP(w, r.WithContext(reqCtx))
	})
	switch phase {
	case "cancel":
		start := aguiPost(t, h, "tenant", map[string]any{"threadId": "shared", "runId": "start",
			"messages": []map[string]any{{"role": "user", "content": "转账"}}})
		pending := aguiPending(t, h, "tenant")
		starts := aguiToolEventIDs(t, start.Body.String())[agui.EventTypeToolCallStart]
		if len(starts) != 1 || starts[0] == "" || starts[0] == "transfer" ||
			len(pending.Interrupts) != 1 || pending.Interrupts[0].ToolCallID != starts[0] {
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
		case "delete_failed", "legacy":
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
		wantCheckpoint := fault == "delete_failed" || fault == "intent_ack_lost" || fault == "legacy"
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

func verifyIdleCancelRecovery(t *testing.T, db *dbsql.DB, cp *crewsql.Checkpointer, st *crewsql.AGUISessionStore,
	history agent_v2.HistoryStore, ref cancelProcessRef, fault string,
) {
	t.Helper()
	ctx := t.Context()
	// 与上一进程共用文件数据库，直接读存储，重启实例始终不接收 HTTP 请求。
	key := fmt.Sprintf("agui-thread:%x", sha256.Sum256([]byte("6:tenantshared")))
	record, err := st.LoadSession(ctx, key)
	if err != nil || record == nil || record.TaskID != ref.SID || record.Cancellation == nil ||
		record.Cancellation.CheckpointsDeleted != (fault == "history_failed") {
		t.Fatalf("cancellation was not durably recorded before restart: %+v %v", record, err)
	}
	if fault == "legacy" {
		// 模拟没有待清理索引的旧库，保留历史分区与取消记录。
		table := crewsql.DefaultCheckpointTable + "_agui_session"
		if _, err := db.ExecContext(ctx, "UPDATE "+table+" SET payload = json_remove(CAST(payload AS TEXT), '$.cancellation.owner', '$.cancellation.thread_id') WHERE sid = ?", key); err != nil {
			t.Fatal(err)
		}
		execCancelFixture(t, db, "DROP TABLE "+crewsql.DefaultCheckpointTable+"_agui_cancel")
		if err := st.Setup(ctx); err != nil {
			t.Fatal(err)
		}
		execCancelFixture(t, db, "DROP TRIGGER refuse_cancel")
	}
	checkCancelCheckpoint(t, cp, ref.SID, fault != "history_failed")
	var tools atomic.Int32
	var historyResolved atomic.Bool
	var firstAttempt atomic.Bool
	var attempts atomic.Int32
	if fault == "killed_after_intent" {
		if ok, err := st.CheckpointTaskLeases().TryAcquireTaskLease(ctx, key, "probe", time.Minute); err != nil || ok {
			t.Fatalf("old process did not leave a live lease: %v %v", ok, err)
		}
	}
	store := &idleCancelSessionStore{AGUISessionStore: st, firstPassDone: make(chan struct{}, 1), fault: fault}
	h := agui.Handler(func(hook.Hooks) agent_v2.Agent {
		return sharedApprovalAgent(history, func() { tools.Add(1) })
	}, agui.WithOwnerResolver(func(context.Context) string { return "unused" }), agui.WithSessionStore(store),
		agui.WithCancellationHistory(func(ctx context.Context, key agent_v2.HistoryKey) (agent_v2.HistoryStore, error) {
			wantOwner := "tenant"
			if fault == "legacy" {
				wantOwner = ""
			}
			if key != (agent_v2.HistoryKey{Owner: "history-tenant", ConvID: "shared"}) || compose_v2.GetThreadID(ctx) != "shared" ||
				compose_v2.GetOwner(ctx) != wantOwner {
				return nil, fmt.Errorf("incorrect recovery history identity: %+v", key)
			}
			attempts.Add(1)
			historyResolved.Store(true)
			if fault == "history_failed" && !firstAttempt.Swap(true) {
				return nil, errors.New("injected missing history store configuration")
			}
			return history, nil
		}))
	defer h.Close()
	// 第一次尝试时故障或旧租约仍在，恢复后只能靠后台轮询重试。
	if fault == "killed_after_intent" {
		select {
		case <-store.firstPassDone:
		case <-time.After(3 * time.Second):
			t.Fatal("background recovery did not encounter the old lease")
		}
		checkCancelCheckpoint(t, cp, ref.SID, true)
		execCancelFixture(t, db, "UPDATE "+crewsql.DefaultCheckpointTable+"_task_lease SET expires_at = 0")
	} else if fault != "legacy" {
		waitIdleCancel(t, func() (bool, error) { return store.failedDelete.Load() || historyResolved.Load(), nil })
		execCancelFixture(t, db, "DROP TRIGGER refuse_cancel")
	}
	waitIdleCancel(t, func() (bool, error) {
		record, err := st.LoadSession(ctx, key)
		return record == nil, err
	})
	checkCancelCheckpoint(t, cp, ref.SID, false)
	records, err := agent_v2.LoadAllHistory(ctx, history, agent_v2.HistoryKey{Owner: "history-tenant", ConvID: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	results := 0
	for _, message := range records {
		if schema.GetToolResultID(message) == "transfer" && strings.Contains(message.Content, "被放弃") {
			results++
		}
	}
	if results != 1 || tools.Load() != 0 || !historyResolved.Load() {
		t.Fatalf("idle recovery did not settle original history: results=%d tools=%d", results, tools.Load())
	}
	if fault == "history_failed" && attempts.Load() != 2 {
		t.Fatalf("expected resolver failure and retry, got %d attempts", attempts.Load())
	}
}

type idleCancelSessionStore struct {
	*crewsql.AGUISessionStore
	firstPassDone chan struct{}
	failedDelete  atomic.Bool
	fault         string
}

func (s *idleCancelSessionStore) ListCancellations(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	keys, next, err := s.AGUISessionStore.ListCancellations(ctx, cursor, limit)
	if s.fault == "killed_after_intent" && err == nil && cursor == "" && len(keys) > 0 && next == "" {
		// 强制续读一次，让测试在后台尝试持有旧租约失败之后再推进数据库时间。
		next = keys[len(keys)-1]
	}
	if s.fault == "killed_after_intent" && cursor != "" {
		select {
		case s.firstPassDone <- struct{}{}:
		default:
		}
	}
	return keys, next, err
}

func (s *idleCancelSessionStore) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, drop string) (int32, error) {
	n, err := s.AGUISessionStore.SaveSession(ctx, key, record, drop)
	if err != nil && s.fault == "delete_failed" && drop != "" {
		s.failedDelete.Store(true)
	}
	return n, err
}

func waitIdleCancel(t *testing.T, check func() (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		done, err := check()
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("idle handler did not advance cancellation")
		}
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
