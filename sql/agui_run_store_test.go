package sql_test

import (
	"bytes"
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/tool"
)

func setupRunJournal(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) (aguiFixture, *crewsql.AGUIRunStore, *crewsql.AGUIRunStore) {
	t.Helper()
	f := setupAGUI(t, dialect, open)
	a, b := crewsql.NewAGUIRunStore(f.a.Checkpointer), crewsql.NewAGUIRunStore(f.b.Checkpointer)
	for _, s := range []*crewsql.AGUIRunStore{a, b} {
		if err := s.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	return f, a, b
}

type journalModel struct{}

func (journalModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool && schema.GetToolResultID(msg) == "commit-1" && strings.Contains(msg.Content, "saved") {
			return schema.AssistantMessage("operation completed"), nil
		}
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].Content != "persist the operation" {
		return nil, errors.New("journal fixture did not receive the expected input")
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "commit-1", Type: "function", Function: schema.FunctionCall{Name: "commit", Arguments: `{"label":"checked"}`},
	}}), nil
}

func (m journalModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m journalModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	if len(tools) != 1 || tools[0].Name != "commit" {
		return nil, errors.New("expected exactly the commit tool")
	}
	return m, nil
}

type disconnectRun struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w disconnectRun) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte(`"type":"RUN_STARTED"`)) {
		w.cancel()
		// 浏览器只收到第一帧，换实例时从该帧的 ID 继续。
		n, _ := w.ResponseRecorder.Write(data)
		return n, io.ErrClosedPipe
	}
	return w.ResponseRecorder.Write(data)
}

func journalQuery(t *testing.T, h http.Handler, owner, runID, cursor string) *httptest.ResponseRecorder {
	t.Helper()
	req := aguiRequest(t, owner, http.MethodGet, nil)
	query := req.URL.Query()
	query.Set("runId", runID)
	req.URL.RawQuery = query.Encode()
	if cursor != "" {
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Last-Event-ID", cursor)
	}
	r := httptest.NewRecorder()
	h.ServeHTTP(r, req)
	return r
}

func checkRunJournal(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) {
	t.Helper()
	t.Run("journal_transport_faults", func(t *testing.T) { testRunJournalTransportFaults(t, dialect, open) })
	t.Run("disconnect_replay_and_deduplicate", func(t *testing.T) {
		f, storeA, storeB := setupRunJournal(t, dialect, open)
		var calls atomic.Int32
		started, proceed := make(chan struct{}, 1), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(proceed) }) }
		defer release()
		factory := func(h hook.Hooks) agent_v2.Agent {
			kit := tool.NewToolkit()
			kit.Register(tool.NewFuncTool("commit", "save a record", nil,
				func(ctx context.Context, args map[string]any) (*tool.Result, error) {
					if args["label"] != "checked" {
						return nil, errors.New("unexpected tool arguments")
					}
					calls.Add(1)
					select {
					case started <- struct{}{}:
					default:
					}
					select {
					case <-proceed:
						return &tool.Result{Success: true, Output: "saved"}, ctx.Err()
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}))
			return agent_v2.NewBasicAgent(agent_v2.WithModel(journalModel{}), agent_v2.WithHooks(h), agent_v2.WithToolkit(kit))
		}
		a := sharedHandler(factory, f.a, agui.WithRunStore(storeA, time.Minute))
		b := sharedHandler(factory, f.b, agui.WithRunStore(storeB, time.Minute))
		body := map[string]any{"threadId": "shared", "runId": "once", "messages": []map[string]any{{
			"id": "m1", "role": "user", "content": "persist the operation",
		}}}
		req := aguiRequest(t, "alice", http.MethodPost, body)
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		lost := disconnectRun{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			a.ServeHTTP(lost, req.WithContext(ctx))
		}()
		defer func() { release(); <-finished }()
		for _, done := range []<-chan struct{}{started, ctx.Done()} {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("tool did not start or first subscriber did not disconnect")
			}
		}
		if r := journalQuery(t, b, "alice", "once", ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"status":"running"`) {
			t.Fatalf("run should survive disconnect while the tool is active: %d %s", r.Code, r.Body)
		}
		if r := journalQuery(t, b, "other", "once", ""); r.Code != http.StatusNotFound {
			t.Fatalf("another owner can read the run: %d %s", r.Code, r.Body)
		}
		changed := map[string]any{"threadId": "shared", "runId": "once", "messages": []map[string]any{{
			"role": "user", "content": "different operation",
		}}}
		if r := aguiPost(t, b, "alice", changed); r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), "run_id_conflict") {
			t.Fatalf("same ID with a different request should conflict: %d %s", r.Code, r.Body)
		}

		attached := make(chan *httptest.ResponseRecorder, 1)
		go func() { attached <- journalQuery(t, b, "alice", "once", "1") }()
		release()
		<-finished
		var replay *httptest.ResponseRecorder
		select {
		case replay = <-attached:
		case <-time.After(5 * time.Second):
			t.Fatal("second subscription did not finish")
		}
		if replay.Code != 200 || strings.Contains(replay.Body.String(), "RUN_STARTED") ||
			!strings.Contains(replay.Body.String(), "operation completed") || !strings.Contains(replay.Body.String(), `"type":"success"`) {
			t.Fatalf("reconnect should deliver only the remaining events: %d %s", replay.Code, replay.Body)
		}
		next := int64(2)
		for _, line := range strings.Split(replay.Body.String(), "\n") {
			if id, ok := strings.CutPrefix(line, "id: "); ok {
				if id != strconv.FormatInt(next, 10) {
					t.Fatalf("wanted event %d, got %s", next, id)
				}
				next++
			}
		}
		if next <= 3 || !strings.Contains(lost.Body.String(), "id: 1\n") || strings.Contains(lost.Body.String(), "RUN_FINISHED") {
			t.Fatalf("first connection did not lose the terminal frame: %s", lost.Body)
		}

		r := aguiPost(t, b, "alice", body)
		if r.Code != 200 || !strings.HasSuffix(r.Body.String(), replay.Body.String()) || calls.Load() != 1 {
			t.Fatalf("retry must replay the original output with one tool execution: calls=%d response=%d %s", calls.Load(), r.Code, r.Body)
		}
		status := journalQuery(t, b, "alice", "once", "")
		var receipt struct {
			Status      agui.RunStatus  `json:"status"`
			LastEventID int64           `json:"lastEventId"`
			Terminal    json.RawMessage `json:"terminal"`
		}
		if err := json.Unmarshal(status.Body.Bytes(), &receipt); err != nil || receipt.Status != agui.RunSucceeded ||
			receipt.LastEventID != next-1 || !bytes.Contains(receipt.Terminal, []byte("RUN_FINISHED")) {
			t.Fatalf("terminal receipt should survive session cleanup: %v %s", err, status.Body)
		}
		if r := journalQuery(t, b, "alice", "once", strconv.FormatInt(next, 10)); r.Code != 400 {
			t.Fatalf("out of range cursor: %d %s", r.Code, r.Body)
		}
		if count, err := storeB.PruneRuns(t.Context(), time.Now().Add(time.Second)); err != nil || count != 1 {
			t.Fatalf("prune completed run: %d %v", count, err)
		}
		if r := journalQuery(t, b, "alice", "once", ""); r.Code != http.StatusNotFound {
			t.Fatalf("pruned run is still visible: %d %s", r.Code, r.Body)
		}
	})

	t.Run("resume_and_rejection_are_replayable", func(t *testing.T) {
		f, storeA, storeB := setupRunJournal(t, dialect, open)
		q := &questions{}
		factory := q.factory(t)
		a := sharedHandler(factory, f.a, agui.WithRunStore(storeA, 0))
		b := sharedHandler(factory, f.b, agui.WithRunStore(storeB, 0))
		if r := aguiPost(t, a, "alice", aguiStart()); !strings.Contains(r.Body.String(), `"type":"interrupt"`) {
			t.Fatalf("failed to start the task: %d %s", r.Code, r.Body)
		}
		id := onlyAGUIQuestion(t, aguiPending(t, b, "alice"), "q1")
		answer := aguiAnswer(id, "resolved", "one")
		first := aguiPost(t, b, "alice", answer)
		again := aguiPost(t, a, "alice", answer)
		if first.Code != 200 || !strings.Contains(first.Body.String(), `"message":"q2"`) ||
			again.Body.String() != first.Body.String() || q.second.Load() != 1 {
			t.Fatalf("same Resume must replay its new pending state: %s / %s / calls=%d", first.Body, again.Body, q.second.Load())
		}
		bad := aguiAnswer(id, "resolved", "stale")
		rejected := aguiPost(t, a, "alice", bad)
		retry := aguiPost(t, b, "alice", bad)
		if rejected.Code != http.StatusBadRequest || retry.Code != rejected.Code || retry.Body.String() != rejected.Body.String() {
			t.Fatalf("pre-stream rejection must replay its original status and body: %d %s / %d %s", rejected.Code, rejected.Body, retry.Code, retry.Body)
		}
		_ = onlyAGUIQuestion(t, aguiPending(t, b, "alice"), "q2")
	})
}

type journalProbeStore struct {
	agui.RunStore
	afterClaim     func(context.Context) error
	beforeTerminal func()
	readPage       func(after int64) (int, error)
}

func (s journalProbeStore) ClaimRun(ctx context.Context, key, digest string, deadline time.Time) (*agui.RunRecord, bool, error) {
	record, claimed, err := s.RunStore.ClaimRun(ctx, key, digest, deadline)
	if err == nil && s.afterClaim != nil {
		err = s.afterClaim(ctx)
	}
	return record, claimed, err
}

func (s journalProbeStore) AppendRunEvent(ctx context.Context, key, token string, data json.RawMessage, status agui.RunStatus, httpStatus int) error {
	if status != agui.RunRunning && s.beforeTerminal != nil {
		s.beforeTerminal()
	}
	return s.RunStore.AppendRunEvent(ctx, key, token, data, status, httpStatus)
}

func (s journalProbeStore) ReadRunEvents(ctx context.Context, key, token string, after, through int64, limit int) ([]agui.RunFrame, error) {
	if s.readPage != nil {
		size, err := s.readPage(after)
		if err != nil {
			return nil, err
		}
		limit = min(limit, size)
	}
	return s.RunStore.ReadRunEvents(ctx, key, token, after, through, limit)
}

func testRunJournalTransportFaults(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) {
	t.Helper()
	f, storeA, storeB := setupRunJournal(t, dialect, open)
	q := &questions{}
	factory := q.factory(t)
	req := aguiRequest(t, "alice", http.MethodPost, aguiStart())
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	var failed atomic.Bool
	reader := journalProbeStore{RunStore: storeB, readPage: func(after int64) (int, error) {
		if after > 0 && failed.CompareAndSwap(false, true) {
			return 0, io.ErrUnexpectedEOF
		}
		return 1, nil
	}}
	b := sharedHandler(factory, f.b, agui.WithRunStore(reader, 0))
	beforeTerminal := make(chan *httptest.ResponseRecorder, 1)
	a := sharedHandler(factory, f.a, agui.WithRunStore(journalProbeStore{
		RunStore: storeA,
		afterClaim: func(claimCtx context.Context) error {
			cancel() // 认领事务成功，订阅在收到任何事件之前断开。
			return claimCtx.Err()
		},
		beforeTerminal: func() {
			r := httptest.NewRecorder()
			b.ServeHTTP(r, aguiRequest(t, "alice", http.MethodGet, nil))
			beforeTerminal <- r
		},
	}, 0))
	a.ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
	select {
	case r := <-beforeTerminal:
		if r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `"message":"q1"`) {
			t.Fatalf("terminal publication raced with the session lease: %d %s", r.Code, r.Body)
		}
	default:
		t.Fatal("claim did not start a producer after the subscriber disconnected")
	}
	if q.first.Load() != 1 {
		t.Fatalf("expected the claimed workflow to execute once, got %d", q.first.Load())
	}
	first := journalQuery(t, b, "alice", "start", "0")
	if first.Code != 200 || !strings.Contains(first.Body.String(), "id: 1\n") ||
		strings.Contains(first.Body.String(), "RUN_ERROR") || !failed.Load() {
		t.Fatalf("subscription read failure must preserve the cursor: %d %s", first.Code, first.Body)
	}
	again := journalQuery(t, b, "alice", "start", "1")
	if again.Code != 200 || !strings.Contains(again.Body.String(), `"type":"interrupt"`) ||
		strings.Contains(again.Body.String(), "RUN_STARTED") || q.first.Load() != 1 {
		t.Fatalf("reconnect should finish the original journal: %d %s", again.Code, again.Body)
	}
}
