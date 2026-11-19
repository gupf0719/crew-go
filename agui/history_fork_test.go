package agui_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type failBranchAppend struct {
	*agent_v2.InMemoryHistoryStore
	fail atomic.Bool
}

func (s *failBranchAppend) Append(ctx context.Context, key agent_v2.HistoryKey, messages []*schema.Message) error {
	if s.fail.Load() && key.ConvID != "source" {
		return errors.New("branch storage unavailable")
	}
	return s.InMemoryHistoryStore.Append(ctx, key, messages)
}

func TestHistoryForkPreservesSourceOnAppendFailure(t *testing.T) {
	store := &failBranchAppend{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	key := agent_v2.HistoryKey{Owner: "test-owner", ConvID: "source"}
	user := historyUser("target", "old wire", "task", "old input")
	prefix := schema.AssistantMessage("earlier")
	if err := store.Append(t.Context(), key, []*schema.Message{prefix, user}); err != nil {
		t.Fatal(err)
	}
	handler := historyOnlyHandler(t, store)
	store.fail.Store(true)
	resp := requestHistoryFork(t, handler, "source", "edit", "old wire")
	if resp.Code != http.StatusServiceUnavailable || !strings.Contains(resp.Body.String(), "history_fork_failed") {
		t.Fatalf("初始化失败应显式返回，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
	source, err := agent_v2.LoadAllHistory(t.Context(), store, key)
	if err != nil || len(source) != 2 || source[0].ID != prefix.ID || source[1].ID != user.ID {
		t.Fatalf("原档应保持原顺序和身份，实际 %+v，err=%v", source, err)
	}
	store.fail.Store(false)
	resp = requestHistoryFork(t, handler, "source", "edit", "old wire")
	var fork agui.HistoryFork
	if resp.Code != http.StatusCreated || json.Unmarshal(resp.Body.Bytes(), &fork) != nil {
		t.Fatalf("存储恢复后应可以重试，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
	history, err := agent_v2.LoadAllHistory(t.Context(), store, agent_v2.HistoryKey{Owner: key.Owner, ConvID: fork.ThreadID})
	if err != nil || len(history) != 1 || history[0].ID != prefix.ID {
		t.Fatalf("重试应只写入截取后的前缀，实际 %+v，err=%v", history, err)
	}
}

func TestHistoryForkRejectsAmbiguousWireIDAndUsesCanonicalID(t *testing.T) {
	store := agent_v2.NewInMemoryHistoryStore()
	first := historyUser("first stored", "reused", "one", "first input")
	copyOfFirst := *first
	copyOfFirst.Annotations = map[string]any{
		agent_v2.HistoryTaskAnnotation: "one", agent_v2.HistoryAgentAnnotation: "another-slot",
		agent_v2.HistoryInputMessageIDAnnotation: "reused",
	}
	second := historyUser("second stored", "reused", "two", "second input")
	collision := historyUser("third stored", first.ID, "three", "third input")
	if err := store.Append(t.Context(), agent_v2.HistoryKey{Owner: "test-owner", ConvID: "source"},
		[]*schema.Message{first, &copyOfFirst, second, collision}); err != nil {
		t.Fatal(err)
	}
	handler := historyOnlyHandler(t, store)
	if resp := requestHistoryFork(t, handler, "source", "edit", "reused"); resp.Code != http.StatusConflict {
		t.Fatalf("重复引用应明确拒绝，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
	resp := requestHistoryFork(t, handler, "source", "edit", second.ID)
	var fork agui.HistoryFork
	if resp.Code != http.StatusCreated || json.Unmarshal(resp.Body.Bytes(), &fork) != nil || fork.SourceMessageID != second.ID {
		t.Fatalf("服务端 ID 应精确定位，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
	resp = requestHistoryFork(t, handler, "source", "edit", first.ID)
	if resp.Code != http.StatusCreated || json.Unmarshal(resp.Body.Bytes(), &fork) != nil || fork.SourceMessageID != first.ID {
		t.Fatalf("同一输入的多份记录可合并，服务端 ID 优先于后来的线上别名，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
}

func TestHistoryForkRegeneratesLegacyTaskFromItsUser(t *testing.T) {
	store := agent_v2.NewInMemoryHistoryStore()
	older := schema.UserMessage("older")
	user := schema.UserMessage("legacy turn")
	reply := schema.AssistantMessage("legacy reply")
	user.Annotations = map[string]any{agent_v2.HistoryTaskAnnotation: "legacy"}
	reply.Annotations = map[string]any{agent_v2.HistoryTaskAnnotation: "legacy", compose_v2.RunIDAnnotationKey: "old stream"}
	if err := store.Append(t.Context(), agent_v2.HistoryKey{Owner: "test-owner", ConvID: "source"}, []*schema.Message{older, user, reply}); err != nil {
		t.Fatal(err)
	}
	resp := requestHistoryFork(t, historyOnlyHandler(t, store), "source", "regenerate", "old stream")
	var fork agui.HistoryFork
	var input agui.AGUIMessage
	if resp.Code != http.StatusCreated || json.Unmarshal(resp.Body.Bytes(), &fork) != nil ||
		json.Unmarshal(fork.Input, &input) != nil || input.Content != "legacy turn" || fork.SourceMessageID != user.ID {
		t.Fatalf("旧的助手流应按任务章找到原用户消息，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
}

func TestHistoryForkLeavesOriginalSuspensionResumable(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	store := agent_v2.NewInMemoryHistoryStore()
	var settled atomic.Int32
	b := compose_v2.NewBuilder().WithName("fork_pause")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		input, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "go?"})
		if err != nil {
			return nil, err
		}
		if input.Content == "yes" {
			settled.Add(1)
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	handler := ownedHandler(func(hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("fork_pause"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("ok") }))
	}, agui.WithHistoryStore(store), agui.WithCheckpointer(cp))
	r := postAGUI(handler, `{"threadId":"source","runId":"old","messages":[{"id":"orig","role":"user","content":"start"}]}`)
	pending := interruptsIn(parseSSE(r.Body.String()))
	if len(pending) != 1 {
		t.Fatalf("应先得到原任务挂起，实际 HTTP %d %s", r.Code, r.Body.String())
	}
	if resp := requestHistoryFork(t, handler, "source", "edit", "orig"); resp.Code != http.StatusCreated || settled.Load() != 0 {
		t.Fatalf("分叉准备应保留原挂起，实际 HTTP %d %s", resp.Code, resp.Body.String())
	}
	resumeEntries(t, handler, "source", "resume", map[string]any{
		"interruptId": pending[0]["id"], "status": "resolved", "payload": "yes",
	})
	if settled.Load() != 1 {
		t.Fatalf("原断点地址应仍能续跑一次，实际 %d", settled.Load())
	}
}

func historyUser(id, wire, task, content string) *schema.Message {
	msg := schema.UserMessage(content)
	msg.ID = id
	msg.Annotations = map[string]any{agent_v2.HistoryInputMessageIDAnnotation: wire, agent_v2.HistoryTaskAnnotation: task}
	return msg
}

func requestHistoryFork(t *testing.T, handler http.Handler, thread, action, id string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(agui.HistoryForkRequest{Action: action, MessageID: id})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/agui?history=fork&threadId="+thread, strings.NewReader(string(data)))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}

func historyOnlyHandler(t *testing.T, store agent_v2.HistoryStore) http.Handler {
	t.Helper()
	return ownedHandler(func(hook.Hooks) agent_v2.Agent {
		t.Error("准备历史分支不应构造 Agent")
		return nil
	}, agui.WithHistoryStore(store))
}
