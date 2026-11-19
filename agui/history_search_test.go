package agui_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

type historySearchHost struct {
	agent_v2.HistoryStore
	mu   sync.Mutex
	keys []agent_v2.HistoryKey
}

func (s *historySearchHost) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if q.Agent == "clerk" && q.Limit == 128 {
		s.mu.Lock()
		s.keys = append(s.keys, key)
		s.mu.Unlock()
		q.Agent = "" // 旧 backend 可能忽略新过滤条件，检索入口仍须逐条核对归属。
	}
	return s.HistoryStore.Load(ctx, key, q)
}

type historySearchModel struct{}

func (historySearchModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		if schema.GetToolResultID(m) == "lookup" {
			return schema.AssistantMessage(m.Content), nil
		}
		if m.Role == schema.RoleUser {
			break
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "lookup", Function: schema.FunctionCall{
		Name: "search_history", Arguments: `{"query":"anchor","owner":"tenant-a","conv_id":"other","agent":"sibling","limit":10000}`,
	}}}), nil
}

func (historySearchModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m historySearchModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestHistorySearchUsesAuthorizedHostedConversation(t *testing.T) {
	const thread = "same-thread"
	host := &historySearchHost{HistoryStore: agent_v2.NewInMemoryHistoryStore()}
	alternate := agent_v2.NewInMemoryHistoryStore()
	appendMessage := func(s agent_v2.HistoryStore, owner, conv, member, id, body string, marker string, flag any) {
		t.Helper()
		m := schema.UserMessage(body)
		m.ID = id
		m.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: member, agent_v2.HistoryTaskAnnotation: "previous-task"}
		if marker != "" {
			m.Annotations[marker] = flag
		}
		if err := s.Append(t.Context(), agent_v2.HistoryKey{Owner: owner, ConvID: conv}, []*schema.Message{m}); err != nil {
			t.Fatal(err)
		}
	}
	appendMessage(host, "tenant-a", thread, "clerk", "host-a", strings.Repeat("旧", 200)+"anchor own-a "+strings.Repeat("末", 220), "", nil)
	appendMessage(host, "tenant-a", thread, "sibling", "sibling-private", "anchor sibling-secret", "", nil)
	appendMessage(host, "tenant-a", thread, "clerk", "tool-record", "anchor background-secret", agent_v2.BackgroundTaskNotificationAnnotation, "finished-call")
	appendMessage(host, "tenant-a", thread, "clerk", "derived-record", "anchor derived-secret", agent_v2.DerivedInputAnnotation, true)
	appendMessage(host, "tenant-a", "other", "clerk", "other-record", "anchor other-secret", "", nil)
	appendMessage(host, "tenant-b", thread, "clerk", "host-b", "anchor own-b", "", nil)
	appendMessage(alternate, "tenant-a", thread, "clerk", "option-record", "anchor fallback-secret", "", nil)
	var allowed atomic.Bool
	handler := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("clerk"), agent_v2.WithHooks(hooks), agent_v2.WithModel(historySearchModel{}),
			agent_v2.WithHistory(alternate, "static-owner"), agent_v2.WithMaxHistoryPairs(2),
			agent_v2.WithHistorySearch(func(ctx context.Context, key agent_v2.HistoryKey) bool {
				return allowed.Load() && key.Owner == compose_v2.GetOwner(ctx) && key.ConvID == thread
			}))
	}, agui.WithHistoryStore(host))
	run := func(owner, runID string) string {
		t.Helper()
		body := runBody(t, thread, runID, msg("assistant", "anchor client-secret"), msg("user", "查询 anchor current-secret"))
		response := postAs(handler, owner, body).Body.String()
		if outcomeType(parseSSE(response)) != "success" {
			t.Fatalf("AG-UI 终态异常: %s", response)
		}
		return response
	}
	if response := run("tenant-a", "denied"); !strings.Contains(response, "history_search_denied") || strings.Contains(response, "own-a") {
		t.Fatalf("未获授权的调用返回了历史: %s", response)
	}
	if len(host.keys) != 0 {
		t.Fatalf("拒绝授权前已查询 store: %v", host.keys)
	}
	allowed.Store(true)
	for _, tc := range []struct{ owner, own, other, id string }{
		{"tenant-a", "own-a", "own-b", "host-a"},
		{"tenant-b", "own-b", "own-a", "host-b"},
	} {
		response := run(tc.owner, "authorized")
		for _, want := range []string{tc.own, tc.id, "conversation_history", "previous-task"} {
			if !strings.Contains(response, want) {
				t.Fatalf("%s 的授权结果缺少 %q: %s", tc.owner, want, response)
			}
		}
		for _, forbidden := range []string{tc.other, "sibling-secret", "fallback-secret", "client-secret", "background-secret", "derived-secret", "other-secret"} {
			if strings.Contains(response, forbidden) {
				t.Fatalf("%s 收到了不属于授权结果的数据 %q: %s", tc.owner, forbidden, response)
			}
		}
	}
	if len(host.keys) != 2 || host.keys[0] != (agent_v2.HistoryKey{Owner: "tenant-a", ConvID: thread}) ||
		host.keys[1] != (agent_v2.HistoryKey{Owner: "tenant-b", ConvID: thread}) {
		t.Fatalf("实际历史来源不是当前宿主会话: %v", host.keys)
	}
}
