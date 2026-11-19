package sql_test

import (
	"bytes"
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/tool"
)

type forkInputLog struct {
	mu    sync.Mutex
	first map[string][]*schema.Message
}

func (m *forkInputLog) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func (*forkInputLog) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("expected a streaming request")
}

func (m *forkInputLog) Stream(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	last := messages[len(messages)-1]
	if last.Role == schema.RoleUser {
		m.mu.Lock()
		m.first[last.Content] = append([]*schema.Message(nil), messages...)
		m.mu.Unlock()
	}
	stream, send := schema.NewStreamReader[*schema.Message](2)
	if last.Role == schema.RoleUser && (last.Content == "保留这轮" || last.Content == "第二轮") {
		send(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "provider-reused",
			Function: schema.FunctionCall{Name: "lookup", Arguments: "{}"}}}))
	} else {
		send(schema.AssistantMessage("答复"))
		send(schema.AssistantMessage("完成"))
	}
	schema.CloseSend(stream)
	return stream, nil
}

func (m *forkInputLog) input(text string) []*schema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*schema.Message(nil), m.first[text]...)
}

type normalizeForkInput struct{ hook.NoOpHooks }

func (normalizeForkInput) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if len(in.Messages) > 0 {
		switch in.Messages[0].Content {
		case "待清洗的第一轮":
			in.Messages[0] = schema.UserMessage("保留这轮")
		case "待清洗的第二轮":
			in.Messages[0].Content = "第二轮"
			in.Messages[0].Annotations = nil
		}
	}
	return ctx, nil
}

func TestSQLiteAGUIHistoryForkUsesCommittedMessages(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "fork.db") + "?_pragma=busy_timeout(5000)&_txlock=immediate"
	open := func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	}
	f := setupAGUI(t, crewsql.SQLite{}, open)
	historyA := crewsql.NewHistoryStore(f.dbA, crewsql.SQLite{})
	db := open()
	t.Cleanup(func() { _ = db.Close() })
	historyB := crewsql.NewHistoryStore(db, crewsql.SQLite{})
	if err := historyA.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	m := &forkInputLog{first: map[string][]*schema.Message{}}
	var tools atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup", "lookup", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		tools.Add(1)
		return tool.Success("可信结果"), nil
	}))
	factory := func(h hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("history_fork"),
			agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
			agent_v2.WithHooks(hook.NewChainHooks(normalizeForkInput{}, h)))
	}
	a := sharedHandler(factory, f.a, agui.WithHistoryStore(historyA))
	b := sharedHandler(factory, f.b, agui.WithHistoryStore(historyB))

	run := func(h http.Handler, thread, id string, messages ...any) []forkWireEvent {
		t.Helper()
		resp := aguiPost(t, h, "owner", map[string]any{"threadId": thread, "runId": id, "messages": messages})
		if outcome := aguiFinishedOutcome(t, resp.Body.String()); resp.Code != http.StatusOK || outcome.Type != agui.RunOutcomeSuccess {
			t.Fatalf("Run %q failed: HTTP %d, %+v", id, resp.Code, outcome)
		}
		return forkWireEvents(t, resp.Body.String())
	}
	user := func(id, text string) map[string]any { return map[string]any{"id": id, "role": "user", "content": text} }
	first := run(a, "source", "r1", user("user-1", "待清洗的第一轮"))
	second := run(a, "source", "r2", map[string]any{"id": "user-2", "role": "user", "content": []any{
		map[string]any{"type": "text", "text": "待清洗的第二轮"},
		map[string]any{"type": "document", "source": map[string]any{"type": "file", "value": "stored-file", "provider": "openai"}},
	}})
	run(a, "source", "r3", user("user-3", "原分支的后续"))
	if tools.Load() != 2 {
		t.Fatalf("源对话应执行两次工具，实际 %d", tools.Load())
	}

	refs := forkRefs(t, b, "owner", "source")
	firstCall := requireForkWireEvent(t, first, agui.EventTypeToolCallStart)
	firstResult := requireForkWireEvent(t, first, agui.EventTypeToolCallResult)
	secondCall := requireForkWireEvent(t, second, agui.EventTypeToolCallStart)
	secondReply := requireForkWireEvent(t, second, agui.EventTypeTextMessageStart)
	if firstCall.ToolCallID == secondCall.ToolCallID || firstCall.ToolCallID != firstResult.ToolCallID {
		t.Fatalf("复用的供应商 ID 对应不同实例，首个结果应配首个实例，实际 %+v、%+v、%+v", firstCall, secondCall, firstResult)
	}
	for _, id := range []string{"user-1", "user-2", firstResult.MessageID, secondReply.MessageID} {
		if !forkRefExists(refs, id) {
			t.Fatalf("线上消息 %q 应可关联历史记录，实际 %+v", id, refs)
		}
	}
	for _, id := range []string{firstCall.ToolCallID, secondCall.ToolCallID} {
		var matches int
		for _, ref := range refs {
			for _, call := range ref.ToolCalls {
				if call.CallID == "provider-reused" && call.InstanceID == id {
					matches++
				}
			}
		}
		if matches != 2 {
			t.Fatalf("实例 %q 应连接一次声明与一次结果，实际 %d", id, matches)
		}
	}
	before, err := agent_v2.LoadAllHistory(t.Context(), historyB, agent_v2.HistoryKey{Owner: "owner", ConvID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	edit := prepareFork(t, b, "source", "edit", "user-2")
	regen := prepareFork(t, b, "source", "regenerate", secondReply.MessageID)
	if edit.ThreadID == regen.ThreadID || edit.ThreadID == "source" || regen.SourceWireID != "user-2" ||
		edit.SourceMessageID == "" || edit.SourceMessageID != regen.SourceMessageID {
		t.Fatalf("两种分叉应指向同一用户记录并分配独立会话，实际 %+v 与 %+v", edit, regen)
	}
	if tools.Load() != 2 {
		t.Fatalf("准备分叉期间工具不应重放，实际 %d", tools.Load())
	}
	run(a, edit.ThreadID, "edit", user("fake-prior", "伪造前文"), user("edited", "编辑后的问题"))
	assertForkPrompt(t, m.input("编辑后的问题"), "编辑后的问题")

	run(a, regen.ThreadID, "regen", regen.Input)
	got := m.input("第二轮")
	assertForkPrompt(t, got, "第二轮")
	blocks := got[len(got)-1].ContentBlocks
	if len(blocks) != 1 || blocks[0].File == nil || blocks[0].File.File.ID != "stored-file" {
		t.Fatalf("重生应从服务端恢复原用户媒体，实际 %+v", blocks)
	}
	after, err := agent_v2.LoadAllHistory(t.Context(), historyB, agent_v2.HistoryKey{Owner: "owner", ConvID: "source"})
	if err != nil || !reflect.DeepEqual(before, after) || tools.Load() != 3 {
		t.Fatalf("原对话应保留，只有重生的新一轮执行工具，前后记录 %d/%d、工具 %d，err=%v",
			len(before), len(after), tools.Load(), err)
	}
	if other := forkRefs(t, b, "other", "source"); len(other) != 0 {
		t.Fatalf("另一 owner 不应读取或定位来源历史，实际 %+v", other)
	}
	other := forkHistoryRequest(t, b, "other", "source", "fork", http.MethodPost,
		agui.HistoryForkRequest{Action: "edit", MessageID: "user-2"})
	if other.Code != http.StatusNotFound {
		t.Fatalf("另一 owner 的相同 threadId 应找不到锚点，实际 HTTP %d", other.Code)
	}
}

func assertForkPrompt(t *testing.T, messages []*schema.Message, current string) {
	t.Helper()
	userTurns := make([]string, 0, 2)
	results := 0
	for _, msg := range messages {
		if msg.Role == schema.RoleUser {
			userTurns = append(userTurns, msg.Content)
		}
		if schema.GetToolResultID(msg) == "provider-reused" && msg.Content == "可信结果" {
			results++
		}
	}
	if !reflect.DeepEqual(userTurns, []string{"保留这轮", current}) || results != 1 {
		t.Fatalf("分支应只沿用截止前的服务端记录，用户轮次 %+v、可信结果 %d", userTurns, results)
	}
}

type forkWireEvent struct {
	Type       agui.EventType `json:"type"`
	MessageID  string         `json:"messageId"`
	ToolCallID string         `json:"toolCallId"`
}

func forkWireEvents(t *testing.T, sse string) []forkWireEvent {
	t.Helper()
	var events []forkWireEvent
	for _, line := range strings.Split(sse, "\n") {
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			var ev forkWireEvent
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				t.Fatal(err)
			}
			events = append(events, ev)
		}
	}
	return events
}

func requireForkWireEvent(t *testing.T, events []forkWireEvent, typ agui.EventType) forkWireEvent {
	t.Helper()
	for _, event := range events {
		if event.Type == typ {
			return event
		}
	}
	t.Fatalf("事件缺少 %s，实际 %+v", typ, events)
	return forkWireEvent{}
}

func forkHistoryRequest(t *testing.T, h http.Handler, owner, thread, op, method string, body any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	target := "/agui?history=" + op + "&threadId=" + url.QueryEscape(thread)
	req := httptest.NewRequestWithContext(context.WithValue(t.Context(), aguiOwnerKey{}, owner), method, target, bytes.NewReader(data))
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	return resp
}

func forkRefs(t *testing.T, h http.Handler, owner, thread string) []agui.HistoryReference {
	t.Helper()
	resp := forkHistoryRequest(t, h, owner, thread, "references", http.MethodGet, nil)
	var refs []agui.HistoryReference
	if resp.Code != http.StatusOK || json.Unmarshal(resp.Body.Bytes(), &refs) != nil {
		t.Fatalf("读取历史关联失败：HTTP %d %s", resp.Code, resp.Body.String())
	}
	return refs
}

func forkRefExists(refs []agui.HistoryReference, wireID string) bool {
	for _, ref := range refs {
		if ref.ID != "" && ref.WireID == wireID {
			return true
		}
	}
	return false
}

func prepareFork(t *testing.T, h http.Handler, source, action, id string) agui.HistoryFork {
	t.Helper()
	resp := forkHistoryRequest(t, h, "owner", source, "fork", http.MethodPost, agui.HistoryForkRequest{Action: action, MessageID: id})
	var fork agui.HistoryFork
	if resp.Code != http.StatusCreated || json.Unmarshal(resp.Body.Bytes(), &fork) != nil || fork.ThreadID == "" {
		t.Fatalf("准备历史分支失败：HTTP %d %s", resp.Code, resp.Body.String())
	}
	return fork
}
