package sql_test

import (
	"bytes"
	"context"
	"crypto/rand"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type aguiOwnerKey struct{}

type aguiFixture struct {
	a, b *crewsql.AGUISessionStore
	dbA  *dbsql.DB
}

func setupAGUI(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) aguiFixture {
	t.Helper()
	table := "agui_" + strings.ToLower(rand.Text())
	dbA, dbB := open(), open()
	t.Cleanup(func() {
		for _, tbl := range []string{table + "_agui_events", table + "_agui_runs", table + "_agui_session", table + "_task_lease", table} {
			if _, err := dbB.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
				t.Error(err)
			}
		}
		_ = dbA.Close() // A 可能已经模拟退出。
		_ = dbB.Close() // 测试结束后释放连接。
	})
	makeStore := func(db *dbsql.DB) *crewsql.AGUISessionStore {
		return crewsql.NewAGUISessionStore(crewsql.NewCheckpointer(db, dialect, crewsql.WithCheckpointerTable(table)))
	}
	f := aguiFixture{a: makeStore(dbA), b: makeStore(dbB), dbA: dbA}
	if err := f.a.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestSQLiteAGUISessions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agui.db")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=busy_timeout(5000)&_txlock=immediate"}).String()
	runAGUISessionScenarios(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
}

// 宿主进程真正退出后，新进程只凭共享 SQL 和线程号找回同一次工具审批。
func TestSQLiteAGUIApprovalSurvivesProcessExit(t *testing.T) {
	dsn := (&url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "agui-process.db"),
		RawQuery: "_pragma=busy_timeout(5000)&_txlock=immediate"}).String()
	run := func(step string) string {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAGUIApprovalProcessWorker$")
		cmd.Env = append(os.Environ(), "CREW_TEST_AGUI_DSN="+dsn, "CREW_TEST_AGUI_STEP="+step)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s worker: %v\n%s", step, err, out)
		}
		return string(out)
	}
	if out := run("start"); !strings.Contains(out, "approval-persisted") || strings.Contains(out, "transfer-executed") {
		t.Fatalf("start worker did not suspend before execution: %s", out)
	}
	if out := run("resume"); strings.Count(out, "transfer-executed") != 1 {
		t.Fatalf("replacement worker must execute the approved tool once: %s", out)
	}
}

func TestAGUIApprovalProcessWorker(t *testing.T) {
	dsn := os.Getenv("CREW_TEST_AGUI_DSN")
	if dsn == "" {
		t.Skip("由跨进程审批测试启动")
	}
	backend := sharedApprovalBackend{driver: "sqlite", dsn: dsn}
	_, cp, history := backend.open(t)
	st := crewsql.NewAGUISessionStore(cp)
	if err := st.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := history.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	h := sharedHandler(func(hook.Hooks) agent_v2.Agent {
		return sharedApprovalAgent(history, func() { fmt.Println("transfer-executed") })
	}, st)
	switch os.Getenv("CREW_TEST_AGUI_STEP") {
	case "start":
		r := aguiPost(t, h, "tenant", map[string]any{"threadId": "shared", "runId": "start",
			"messages": []map[string]any{{"role": "user", "content": "转账"}}})
		if !strings.Contains(r.Body.String(), `"type":"interrupt"`) {
			t.Fatalf("start failed to reach tool approval: %s", r.Body)
		}
		fmt.Println("approval-persisted")
	case "resume":
		p := aguiPending(t, h, "tenant")
		if len(p.Interrupts) != 1 || p.Interrupts[0].ToolCallID != "transfer" {
			t.Fatalf("tool approval was lost: %+v", p)
		}
		answer := aguiAnswer(p.Interrupts[0].ID, "resolved", true)
		if r := aguiPost(t, h, "tenant", answer); !strings.Contains(r.Body.String(), `"type":"success"`) {
			t.Fatalf("resume failed: %s", r.Body)
		}
		if r := aguiPost(t, h, "tenant", answer); r.Code != http.StatusNotFound {
			t.Fatalf("approval replayed after completion: %d %s", r.Code, r.Body)
		}
	default:
		t.Fatal("unknown worker step")
	}
}

type viewModel struct {
	input, document atomic.Value
}

func (m *viewModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	var text []string
	var documents []schema.FileData
	for _, msg := range msgs {
		text = append(text, msg.Content)
		for _, block := range msg.ContentBlocks {
			if block != nil && block.File != nil && block.File.File != nil {
				documents = append(documents, *block.File.File)
			}
		}
	}
	m.input.Store(strings.Join(text, "|"))
	m.document.Store(documents)
	return schema.AssistantMessage("done"), nil
}

func (m *viewModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *viewModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type questions struct {
	id, consumed  atomic.Value
	first, second atomic.Int32
	beforeSecond  func(context.Context) error
	model         viewModel
}

func (q *questions) factory(t *testing.T) agui.AgentFactory {
	t.Helper()
	out := compose_v2.LastValue[*schema.Message]("out")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, out)
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		q.id.Store(compose_v2.GetSessionID(ctx))
		a, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "q1"})
		if err != nil {
			q.first.Add(1)
			return nil, err
		}
		if q.beforeSecond != nil {
			if err := q.beforeSecond(ctx); err != nil {
				return nil, err
			}
		}
		b, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Question: "q2", ResponseSchema: json.RawMessage(`{"type":"string","minLength":2}`),
		})
		if err != nil {
			q.second.Add(1)
			return nil, err
		}
		q.consumed.Store(a.Content + "+" + b.Content)
		sub := agent_v2.NewBasicAgent(agent_v2.WithName("finish"), agent_v2.WithModel(&q.model))
		events, err := agent_v2.RunSubAgent(ctx, "finish", sub, schema.UserMessage("下游输入"))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(events))}, nil
	}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return func(h hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("shared-ask"),
			agent_v2.WithGraphAgentHooks(h),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(channels compose_v2.Channels) *schema.Message {
				msg, _ := compose_v2.Get(channels, out) // 收尾节点写入。
				return msg
			}))
	}
}

func sharedHandler(factory agui.AgentFactory, st agui.SessionStore, opts ...agui.Option) http.Handler {
	base := []agui.Option{agui.WithSessionStore(st), agui.WithOwnerResolver(func(ctx context.Context) string {
		owner, _ := ctx.Value(aguiOwnerKey{}).(string)
		return owner
	})}
	return agui.Handler(factory, append(base, opts...)...)
}

func aguiRequest(t *testing.T, owner, method string, body any) *http.Request {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequestWithContext(context.WithValue(t.Context(), aguiOwnerKey{}, owner),
		method, "/agui?threadId=shared", bytes.NewReader(data))
}

func aguiStart() map[string]any {
	return map[string]any{"threadId": "shared", "runId": "start", "state": map[string]any{"initial": "state"},
		"messages": []map[string]any{
			{"role": "user", "content": "原前文"},
			aguiDocument("file-original"),
			{"role": "user", "content": "开始"},
		}}
}

func aguiDocument(fileID string) map[string]any {
	return map[string]any{"role": "user", "content": []map[string]any{
		{"type": "document", "source": map[string]any{
			"type": "file", "value": fileID, "provider": "openai", "mimeType": "application/pdf",
		}},
	}}
}

func aguiAnswer(id, status string, payload any) map[string]any {
	return map[string]any{"threadId": "shared", "runId": rand.Text(), "resume": []agui.ResumeEntry{{
		InterruptID: id, Status: status, Payload: payload,
	}}, "state": map[string]any{"injected": "state"}}
}

func aguiPost(t *testing.T, h http.Handler, owner string, body any) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, aguiRequest(t, owner, http.MethodPost, body))
	return r
}

type pendingAGUI struct {
	Interrupts []agui.RunInterrupt `json:"interrupts"`
	Accepted   []string            `json:"acceptedInterruptIds"`
	Version    string              `json:"workflowVersion"`
}

func aguiPending(t *testing.T, h http.Handler, owner string) pendingAGUI {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, aguiRequest(t, owner, http.MethodGet, nil))
	var p pendingAGUI
	if r.Code != http.StatusOK || json.Unmarshal(r.Body.Bytes(), &p) != nil {
		t.Fatalf("query pending: %d %s", r.Code, r.Body)
	}
	return p
}

func onlyAGUIQuestion(t *testing.T, p pendingAGUI, want string) string {
	t.Helper()
	if len(p.Interrupts) != 1 || p.Interrupts[0].Message != want || p.Interrupts[0].ID == "" {
		t.Fatalf("want one pending question %q, got %+v", want, p)
	}
	return p.Interrupts[0].ID
}

type brokenSSE struct {
	*httptest.ResponseRecorder
}

func (w brokenSSE) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte(`"type":"RUN_FINISHED"`)) {
		return 0, io.ErrClosedPipe
	}
	return w.ResponseRecorder.Write(data)
}

type faultSessions struct {
	*crewsql.AGUISessionStore
	replaceFailure, ackFailure, cleanupFailure bool
}

func (s *faultSessions) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	if (s.replaceFailure && record != nil && dropTask != "") || (s.cleanupFailure && record == nil) {
		return 0, errors.New("injected session write failure")
	}
	deleted, err := s.AGUISessionStore.SaveSession(ctx, key, record, dropTask)
	if err == nil && s.ackFailure && record != nil && len(record.Answers) > 0 {
		return deleted, io.ErrUnexpectedEOF
	}
	return deleted, err
}

func runAGUISessionScenarios(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) {
	t.Helper()
	checkRunJournal(t, dialect, open)
	checkWorkflowVersions(t, dialect, open)
	fixture := func(t *testing.T) aguiFixture { return setupAGUI(t, dialect, open) }
	t.Run("cancel_recovery_with_old_workflow", func(t *testing.T) { testDurableCancelWorkflowRecovery(t, fixture) })
	t.Run("cancel_transitions_require_both_fences", func(t *testing.T) { testDurableCancelFences(t, fixture) })
	t.Run("restart_disconnect_busy_owner_stale", func(t *testing.T) {
		f := setupAGUI(t, dialect, open)
		reached, proceed := make(chan struct{}), make(chan struct{})
		var once sync.Once
		q := &questions{beforeSecond: func(ctx context.Context) error {
			var err error
			once.Do(func() {
				close(reached)
				select {
				case <-proceed:
				case <-ctx.Done():
					err = ctx.Err()
				}
			})
			return err
		}}
		factory := q.factory(t)
		a, b := sharedHandler(factory, f.a), sharedHandler(factory, f.b)
		lost := brokenSSE{httptest.NewRecorder()}
		a.ServeHTTP(lost, aguiRequest(t, "alice", http.MethodPost, aguiStart()))
		if strings.Contains(lost.Body.String(), "RUN_FINISHED") {
			t.Fatal("fixture did not drop the interrupt frame")
		}
		if err := f.dbA.Close(); err != nil {
			t.Fatal(err)
		}
		first := onlyAGUIQuestion(t, aguiPending(t, b, "alice"), "q1")
		if p := aguiPending(t, b, "bob"); len(p.Interrupts) != 0 {
			t.Fatalf("cross-owner projection: %+v", p)
		}
		if r := aguiPost(t, b, "bob", aguiAnswer(first, "resolved", "foreign")); r.Code != 404 {
			t.Fatalf("cross-owner resume: %d %s", r.Code, r.Body)
		}
		done := make(chan *httptest.ResponseRecorder, 1)
		resume := aguiRequest(t, "alice", http.MethodPost, aguiAnswer(first, "resolved", "one"))
		go func() {
			r := httptest.NewRecorder()
			b.ServeHTTP(r, resume)
			done <- r
		}()
		select {
		case <-reached:
		case <-time.After(3 * time.Second):
			close(proceed)
			t.Fatalf("resume did not reach q2: %s", (<-done).Body)
		}
		peer := sharedHandler(factory, f.b)
		conflict := aguiPost(t, peer, "alice", aguiAnswer(first, "resolved", "wrong"))
		close(proceed)
		if r := <-done; !strings.Contains(r.Body.String(), "q2") || conflict.Code != 409 {
			t.Fatalf("parallel resume outcome: %s, concurrent request %d %s", r.Body, conflict.Code, conflict.Body)
		}
		pending := aguiPending(t, b, "alice")
		second := onlyAGUIQuestion(t, pending, "q2")
		if pending.Interrupts[0].ResponseSchema["minLength"] != float64(2) {
			t.Fatalf("schema was lost across SQL round trip: %+v", pending)
		}
		if r := aguiPost(t, b, "alice", aguiAnswer(second, "resolved", 42)); r.Code != 400 || q.consumed.Load() != nil {
			t.Fatalf("invalid payload should preserve the pending task: %d %s", r.Code, r.Body)
		}
		if first == second {
			t.Fatal("two questions have the same identity")
		}
		if r := aguiPost(t, b, "alice", aguiAnswer(first, "resolved", "wrong")); r.Code != 400 {
			t.Fatalf("late q1 answer accepted: %d %s", r.Code, r.Body)
		}
		answer := aguiAnswer(second, "resolved", "two")
		answer["messages"] = []map[string]any{aguiDocument("file-resume")}
		r := aguiPost(t, b, "alice", answer)
		if !strings.Contains(r.Body.String(), `"type":"success"`) || q.consumed.Load() != "one+two" || q.first.Load() != 1 || q.second.Load() != 1 {
			t.Fatalf("resume failed to replay answers once: %s (%v, %d/%d)", r.Body, q.consumed.Load(), q.first.Load(), q.second.Load())
		}
		view, _ := q.model.input.Load().(string)
		if !strings.Contains(view, "原前文") || !strings.Contains(view, `"initial":"state"`) || strings.Contains(view, "injected") {
			t.Fatalf("downstream model did not see the original run view: %q", view)
		}
		docs, _ := q.model.document.Load().([]schema.FileData)
		if len(docs) != 1 || docs[0].ID != "file-original" || docs[0].Provider != "openai" {
			t.Fatalf("shared resume changed the original user documents: %+v", docs)
		}
		if p := aguiPending(t, b, "alice"); len(p.Interrupts) != 0 {
			t.Fatalf("completed task is still open: %+v", p)
		}
	})
	t.Run("failed_replacement_and_accepted_answer_recovery", func(t *testing.T) {
		f := setupAGUI(t, dialect, open)
		q := &questions{}
		factory := q.factory(t)
		good := sharedHandler(factory, f.b)
		aguiPost(t, good, "alice", aguiStart())
		id := onlyAGUIQuestion(t, aguiPending(t, good, "alice"), "q1")
		bad := sharedHandler(factory, &faultSessions{AGUISessionStore: f.a, replaceFailure: true, ackFailure: true})
		if r := aguiPost(t, bad, "alice", aguiStart()); !strings.Contains(r.Body.String(), "session_store_failed") || q.first.Load() != 1 {
			t.Fatalf("failed replacement ran the new task: %s, calls=%d", r.Body, q.first.Load())
		}
		if kept := onlyAGUIQuestion(t, aguiPending(t, good, "alice"), "q1"); kept != id {
			t.Fatalf("failed replacement lost old task: %q vs %q", kept, id)
		}
		if r := aguiPost(t, bad, "alice", aguiAnswer(id, "resolved", "one")); !strings.Contains(r.Body.String(), "session_store_failed") || q.second.Load() != 0 {
			t.Fatalf("answer write ambiguity drove the graph: %s", r.Body)
		}
		p := aguiPending(t, good, "alice")
		if len(p.Accepted) != 1 || p.Accepted[0] != id {
			t.Fatalf("reconnect cannot identify previously accepted answer: %+v", p)
		}
		if r := aguiPost(t, good, "alice", aguiAnswer(id, "resolved", "changed")); !strings.Contains(r.Body.String(), "answer_already_accepted") {
			t.Fatalf("previous answer was overwritten: %s", r.Body)
		}
		aguiPost(t, good, "alice", aguiAnswer(id, "resolved", nil))
		id = onlyAGUIQuestion(t, aguiPending(t, good, "alice"), "q2")
		failedCleanup := sharedHandler(factory, &faultSessions{AGUISessionStore: f.a, cleanupFailure: true})
		if r := aguiPost(t, failedCleanup, "alice", aguiAnswer(id, "resolved", "two")); !strings.Contains(r.Body.String(), "session_store_failed") || q.consumed.Load() != "one+two" {
			t.Fatalf("fixture did not complete the graph before failing cleanup: %s", r.Body)
		}
		if p := aguiPending(t, good, "alice"); len(p.Interrupts) != 0 {
			t.Fatalf("completed task returned old questions: %+v", p)
		}
		aguiPost(t, good, "alice", aguiStart())
		newID := onlyAGUIQuestion(t, aguiPending(t, good, "alice"), "q1")
		if q.first.Load() != 2 {
			t.Fatalf("new request did not run after cleanup: %d", q.first.Load())
		}
		if r := aguiPost(t, good, "alice", aguiAnswer(newID, "cancelled", nil)); !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
			t.Fatalf("current cancellation did not finish: %s", r.Body)
		}
	})
	t.Run("stale_cancel_cannot_delete_new_question", func(t *testing.T) {
		f := setupAGUI(t, dialect, open)
		q := &questions{}
		factory := q.factory(t)
		a := sharedHandler(factory, f.a)
		aguiPost(t, a, "alice", aguiStart())
		first := onlyAGUIQuestion(t, aguiPending(t, a, "alice"), "q1")
		sid := q.id.Load().(string)
		var advanced bool
		b := sharedHandler(func(h hook.Hooks) agent_v2.Agent {
			ctx := compose_v2.WithCheckpointerCtx(t.Context(), f.a)
			_, err := factory(hook.NoOpHooks{}).Resume(ctx, sid, agent_v2.WithResumeUserInputs(
				map[string]*schema.Message{first: schema.UserMessage("one")}))
			ie, ok := compose_v2.IsInterrupt(err)
			advanced = ok && ie.AllQueries()[0].Question == "q2"
			return factory(h)
		}, f.b)
		r := aguiPost(t, b, "alice", aguiAnswer(first, "cancelled", nil))
		if !advanced || !strings.Contains(r.Body.String(), "invalid_resume") {
			t.Fatalf("cancellation against the previous epoch: %s (advanced=%v)", r.Body, advanced)
		}
		second := onlyAGUIQuestion(t, aguiPending(t, a, "alice"), "q2")
		if r := aguiPost(t, a, "alice", aguiAnswer(second, "cancelled", nil)); !strings.Contains(r.Body.String(), `"type":"cancelled"`) {
			t.Fatalf("valid cancel failed: %s", r.Body)
		}
	})
	t.Run("shared_write_requires_both_fences", func(t *testing.T) {
		for _, lose := range []string{"thread", "task"} {
			t.Run(lose, func(t *testing.T) {
				f := setupAGUI(t, dialect, open)
				ctx := compose_v2.WithCheckpointerCtx(t.Context(), f.a)
				ctx, threadLease, err := compose_v2.BeginTaskActivity(ctx, "thread", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer threadLease.Release()
				ctx, taskLease, err := compose_v2.BeginTaskActivity(ctx, "task", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer taskLease.Release()
				if _, err := f.a.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "task", ModelData: "original"}, ""); err != nil {
					t.Fatal(err)
				}
				if err := f.a.SaveNS(ctx, "task", "", &compose_v2.Checkpoint{GraphID: "original"}); err != nil {
					t.Fatal(err)
				}
				leaseStore := f.b.CheckpointTaskLeases()
				token := compose_v2.CheckpointTaskLeaseToken(ctx, f.a.CheckpointTaskLeases(), lose)
				if err := leaseStore.ReleaseTaskLease(t.Context(), lose, token); err != nil {
					t.Fatal(err)
				}
				ok, err := leaseStore.TryAcquireTaskLease(t.Context(), lose, "winner", time.Minute)
				if err != nil || !ok {
					t.Fatalf("failed to simulate lease takeover: %v %v", ok, err)
				}
				if lose == "thread" {
					_, err = f.a.SaveSession(ctx, "thread", nil, "task")
				} else {
					_, err = f.a.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "task", ModelData: "stale"}, "")
				}
				if !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
					t.Fatalf("unfenced mutation: %v", err)
				}
				record, err := f.b.LoadSession(t.Context(), "thread")
				if err != nil || record == nil || record.ModelData != "original" {
					t.Fatalf("old request changed session record: %+v %v", record, err)
				}
				cp, err := f.b.LoadNS(t.Context(), "task", "")
				if err != nil || cp == nil || cp.GraphID != "original" {
					t.Fatalf("old request discarded checkpoint: %+v %v", cp, err)
				}
			})
		}
	})
}
