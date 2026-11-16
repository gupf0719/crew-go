package sql_test

import (
	"context"
	dbsql "database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/hook"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type sharedReplacementHistory struct {
	agent_v2.HistoryStore
	fail atomic.Bool
}

func (s *sharedReplacementHistory) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	if s.fail.Load() {
		for _, msg := range msgs {
			if schema.GetToolResultID(msg) == "transfer" {
				return errors.New("old history unavailable")
			}
		}
	}
	return s.HistoryStore.Append(ctx, key, msgs)
}

func TestSQLiteAGUIReplacementFinishesOriginalHistoryAfterRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agui-replacement.db")
	f := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
	oldHistory := crewsql.NewHistoryStore(f.dbA, crewsql.SQLite{}, crewsql.WithHistoryTable("old_history"))
	newHistory := crewsql.NewHistoryStore(f.dbA, crewsql.SQLite{}, crewsql.WithHistoryTable("new_history"))
	for _, history := range []*crewsql.HistoryStore{oldHistory, newHistory} {
		if err := history.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	oldSink := &sharedReplacementHistory{HistoryStore: oldHistory}
	var executed atomic.Int32
	factory := func(history agent_v2.HistoryStore, owner string) agui.AgentFactory {
		return func(h hook.Hooks) agent_v2.Agent {
			return sharedApprovalAgent(history, func() { executed.Add(1) }, agent_v2.WithHooks(h),
				agent_v2.WithHistory(history, owner, agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return owner })))
		}
	}
	oldFactory := factory(oldSink, "old-partition")
	newFactory := factory(newHistory, "new-partition")
	oldKey := agent_v2.HistoryKey{Owner: "old-partition", ConvID: "shared"}
	newKey := agent_v2.HistoryKey{Owner: "new-partition", ConvID: "shared"}
	load := func(store agent_v2.HistoryStore, key agent_v2.HistoryKey) []*schema.Message {
		t.Helper()
		msgs, err := agent_v2.LoadAllHistory(t.Context(), store, key)
		if err != nil {
			t.Fatal(err)
		}
		return msgs
	}
	var sessionKey string
	store := &failingCancelSessionStore{AGUISessionStore: f.a, before: func(_ context.Context, key string, _ *agui.SessionRecord, _ string) error {
		sessionKey = key
		return nil
	}}
	a := sharedHandler(oldFactory, store, agui.WithWorkflowVersions("old", nil))
	run := func(id, content string) map[string]any {
		return map[string]any{"threadId": "shared", "runId": id, "messages": []map[string]any{{"role": "user", "content": content}}}
	}
	start := aguiPost(t, a, "owner", run("old", "转账"))
	pending := aguiPending(t, a, "owner")
	if !strings.Contains(start.Body.String(), `"type":"interrupt"`) || len(pending.Interrupts) != 1 {
		t.Fatalf("旧任务应挂起审批，实际 %s %+v", start.Body, pending)
	}
	previous, err := f.a.LoadSession(t.Context(), sessionKey)
	if err != nil || previous == nil || previous.HistoryKey != oldKey {
		t.Fatalf("旧任务应绑定已持久化分区，实际 %+v 错误 %v", previous, err)
	}
	oldMessages := load(oldHistory, oldKey)
	hasCall := false
	for _, msg := range oldMessages {
		for _, call := range schema.GetToolCalls(msg) {
			hasCall = hasCall || call.ID == "transfer"
		}
	}
	if !hasCall {
		t.Fatal("原分区中没有待配对的调用")
	}

	failDelete := &failingCancelSessionStore{AGUISessionStore: f.b, before: func(_ context.Context, _ string, _ *agui.SessionRecord, drop string) error {
		if drop == previous.TaskID {
			return errors.New("old checkpoint temporarily unavailable")
		}
		return nil
	}}
	versions := agui.WithWorkflowVersions("new", map[string]agui.AgentFactory{"old": oldFactory})
	replacementHandler := func(store agui.SessionStore) http.Handler {
		base := sharedHandler(newFactory, store, versions)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 新工作流的中间件已切到新存储，不能覆盖旧任务工厂的历史去向。
			ctx := agent_v2.WithHistoryCtx(r.Context(), newHistory, newKey.Owner,
				agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return newKey.Owner }))
			base.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	first := replacementHandler(failDelete)
	if r := aguiPost(t, first, "owner", run("delete-fails", "查余额")); !strings.Contains(r.Body.String(), "session_store_failed") {
		t.Fatalf("清档失败须阻止替换，实际 %s", r.Body)
	}
	checkCancelCheckpoint(t, f.b, previous.TaskID, true)
	if open := aguiPending(t, first, "owner"); len(open.Interrupts) != 1 || open.Interrupts[0].ID != pending.Interrupts[0].ID {
		t.Fatalf("清档失败丢失旧问题，实际 %+v", open)
	}

	oldSink.fail.Store(true)
	b := replacementHandler(f.b)
	if r := aguiPost(t, b, "owner", run("append-fails", "查余额")); !strings.Contains(r.Body.String(), "cleanup_pending") {
		t.Fatalf("旧历史补写失败须阻止替换，实际 %s", r.Body)
	}
	checkCancelCheckpoint(t, f.b, previous.TaskID, false)
	progress, err := f.a.LoadSession(t.Context(), sessionKey)
	if err != nil || progress == nil || progress.TaskID != previous.TaskID || progress.Cancellation == nil ||
		!progress.Closing || !progress.Cancellation.CheckpointsDeleted || progress.Cancellation.HistoryKey == nil ||
		*progress.Cancellation.HistoryKey != oldKey || len(load(newHistory, newKey)) != 0 {
		t.Fatalf("应保留原任务的持久收尾进度，新任务不得写历史，实际 %+v 错误 %v", progress, err)
	}

	oldSink.fail.Store(false)
	c := replacementHandler(f.a)
	if r := aguiPost(t, c, "owner", run("retry", "查余额")); !strings.Contains(r.Body.String(), `"type":"success"`) {
		t.Fatalf("另一实例应补齐旧历史后成功替换，实际 %s", r.Body)
	}
	results := 0
	for _, msg := range load(oldHistory, oldKey) {
		if schema.GetToolResultID(msg) == "transfer" {
			results++
			if msg.Annotations[agent_v2.HistoryTaskAnnotation] != previous.TaskID {
				t.Fatalf("收尾记录的任务身份错误：%+v", msg.Annotations)
			}
		}
	}
	newMessages := load(newHistory, newKey)
	for _, msg := range newMessages {
		if schema.GetToolResultID(msg) != "" || len(schema.GetToolCalls(msg)) != 0 {
			t.Fatalf("新分区不该接收旧调用及收尾结果，实际 %+v", msg)
		}
	}
	if results != 1 || executed.Load() != 0 || len(newMessages) == 0 {
		t.Fatalf("原调用须配对一次、零执行，新任务须写入新历史，结果数=%d 执行数=%d 新历史数=%d", results, executed.Load(), len(newMessages))
	}
}

func TestSQLiteAGUIReplacementKeepsOldTaskWhenHistoryCannotBeResolved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agui-unresolved-history.db")
	f := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
	history := crewsql.NewHistoryStore(f.dbA, crewsql.SQLite{}, crewsql.WithHistoryTable("middleware_history"))
	fallback := crewsql.NewHistoryStore(f.dbA, crewsql.SQLite{}, crewsql.WithHistoryTable("agent_history"))
	for _, store := range []*crewsql.HistoryStore{history, fallback} {
		if err := store.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	var executions atomic.Int32
	factory := func(h hook.Hooks) agent_v2.Agent {
		return sharedApprovalAgent(fallback, func() { executions.Add(1) }, agent_v2.WithHooks(h))
	}
	withHistory := func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			base.ServeHTTP(w, r.WithContext(agent_v2.WithHistoryCtx(r.Context(), history, "original")))
		})
	}
	store := &failingCancelSessionStore{AGUISessionStore: f.a}
	original := withHistory(sharedHandler(factory, store, agui.WithWorkflowVersions("old", nil)))
	run := func(id, content string) map[string]any {
		return map[string]any{"threadId": "shared", "runId": id, "messages": []map[string]any{{"role": "user", "content": content}}}
	}
	start := aguiPost(t, original, "tenant", run("original", "转账"))
	pending := aguiPending(t, original, "tenant")
	if !strings.Contains(start.Body.String(), `"type":"interrupt"`) || len(pending.Interrupts) != 1 {
		t.Fatalf("旧任务应挂起，实际 %s %+v", start.Body, pending)
	}
	record, err := f.a.LoadSession(t.Context(), store.key)
	if err != nil || record == nil || record.HistoryKey == (agent_v2.HistoryKey{}) || record.HistorySource != agent_v2.TaskHistoryRequest {
		t.Fatalf("应记录实际选中的中间件历史，即使工厂也配置了历史。实际 %+v 错误 %v", record, err)
	}

	versions := agui.WithWorkflowVersions("new", map[string]agui.AgentFactory{"old": factory})
	next := sharedHandler(factory, f.b, versions)
	reply := aguiPost(t, next, "tenant", run("replacement", "查余额"))
	if !strings.Contains(reply.Body.String(), "session_store_failed") || executions.Load() != 0 {
		t.Fatalf("旧共享历史后端不可解析时应拒绝替换，实际 %s", reply.Body)
	}
	checkCancelCheckpoint(t, f.b, record.TaskID, true)
	id := pending.Interrupts[0].ID
	active := aguiPending(t, next, "tenant")
	if len(active.Interrupts) != 1 || active.Interrupts[0].ID != id {
		t.Fatalf("拒绝替换必须保留原挂起身份，实际 %+v", active)
	}
	resume := aguiPost(t, withHistory(next), "tenant", aguiAnswer(id, "resolved", true))
	if !strings.Contains(resume.Body.String(), `"type":"success"`) || executions.Load() != 1 {
		t.Fatalf("另一实例应继续原问题并执行一次已批准工具，实际次数 %d 响应 %s", executions.Load(), resume.Body)
	}
	checkCancelCheckpoint(t, f.b, record.TaskID, false)

	// 旧版存档只有历史 key，来源未知。无法用相同的 key 推断原存储。
	store.before = func(_ context.Context, _ string, record *agui.SessionRecord, _ string) error {
		if record != nil {
			record.HistorySource = ""
		}
		return nil
	}
	start = aguiPost(t, original, "tenant", run("legacy", "转账"))
	if !strings.Contains(start.Body.String(), `"type":"interrupt"`) {
		t.Fatalf("旧版任务应挂起，实际 %s", start.Body)
	}
	legacy, err := f.a.LoadSession(t.Context(), store.key)
	if err != nil || legacy == nil || legacy.HistorySource != "" || legacy.HistoryKey != record.HistoryKey {
		t.Fatalf("fixture 应保存同 key 和未知来源，实际 %+v 错误 %v", legacy, err)
	}
	if r := aguiPost(t, next, "tenant", run("legacy-unresolved", "查余额")); !strings.Contains(r.Body.String(), "session_store_failed") {
		t.Fatalf("来源未知的旧档应保留原任务，实际 %s", r.Body)
	}
	checkCancelCheckpoint(t, f.b, legacy.TaskID, true)
	resolved := 0
	withResolver := sharedHandler(factory, f.b, versions, agui.WithCancellationHistory(
		func(_ context.Context, key agent_v2.HistoryKey) (agent_v2.HistoryStore, error) {
			resolved++
			if key != legacy.HistoryKey {
				return nil, errors.New("wrong history key")
			}
			return history, nil
		}))
	if r := aguiPost(t, withResolver, "tenant", run("legacy-resolved", "查余额")); !strings.Contains(r.Body.String(), `"type":"success"`) {
		t.Fatalf("原后端可解析后应成功替换，实际 %s", r.Body)
	}
	checkCancelCheckpoint(t, f.b, legacy.TaskID, false)
	msgs, err := agent_v2.LoadAllHistory(t.Context(), history, legacy.HistoryKey)
	if err != nil {
		t.Fatal(err)
	}
	closed := 0
	for _, msg := range msgs {
		if schema.GetToolResultID(msg) == "transfer" && msg.Annotations[agent_v2.HistoryTaskAnnotation] == legacy.TaskID &&
			strings.Contains(msg.Content, "任务已被放弃") {
			closed++
		}
	}
	newMessages, err := agent_v2.LoadAllHistory(t.Context(), fallback, legacy.HistoryKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range newMessages {
		if msg.Annotations[agent_v2.HistoryTaskAnnotation] == legacy.TaskID {
			t.Fatalf("旧任务记录泄漏到 Agent 的另一个历史后端，实际 %+v", msg)
		}
	}
	if closed != 1 || executions.Load() != 1 || resolved != 1 || len(newMessages) == 0 {
		t.Fatalf("未知来源经 resolver 只补一次旧历史、零额外工具执行并写新历史，实际补写 %d 执行 %d 解析 %d 新历史长度 %d",
			closed, executions.Load(), resolved, len(newMessages))
	}
}
