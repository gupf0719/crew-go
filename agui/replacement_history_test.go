package agui_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

type localReplacementHistory struct {
	agent_v2.HistoryStore
	attempts  atomic.Int32
	failFirst int32
}

func (s *localReplacementHistory) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	if hasToolResult(msgs, "c1") && s.attempts.Add(1) <= s.failFirst {
		return errors.New("history append unavailable")
	}
	return s.HistoryStore.Append(ctx, key, msgs)
}

func TestHandler_ReplacementRetriesOriginalHistoryDestination(t *testing.T) {
	oldHistory := &localReplacementHistory{HistoryStore: agent_v2.NewInMemoryHistoryStore(), failFirst: 1}
	newHistory := agent_v2.NewInMemoryHistoryStore()
	cp := &replacementDeleteFailure{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	var executed atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			executed.Add(1)
			return tool.Success("已转账"), nil
		}))
	base := ownedHandler(func(hook hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("transfer-agent"), agent_v2.WithModel(transferModel{}),
			agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(hook))
	}, agui.WithCheckpointer(cp))
	withHistory := func(store agent_v2.HistoryStore, owner string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := agent_v2.WithHistoryCtx(r.Context(), store, owner,
				agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return owner }))
			base.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	old := withHistory(oldHistory, "old-partition")
	replacement := withHistory(newHistory, "new-partition")
	oldKey := agent_v2.HistoryKey{Owner: "old-partition", ConvID: "replace"}
	newKey := agent_v2.HistoryKey{Owner: "new-partition", ConvID: "replace"}
	load := func(store agent_v2.HistoryStore, key agent_v2.HistoryKey) []*schema.Message {
		t.Helper()
		msgs, err := agent_v2.LoadAllHistory(t.Context(), store, key)
		if err != nil {
			t.Fatal(err)
		}
		return msgs
	}
	suspended := postAGUI(old, `{"threadId":"replace","runId":"old","messages":[{"role":"user","content":"转账"}]}`)
	id := findInterruptID(parseSSE(suspended.Body.String()))
	sid := cp.lastRoot
	if id == "" || sid == "" || !hasToolCall(load(oldHistory, oldKey), "c1") {
		t.Fatalf("旧任务应落盘工具声明并挂起审批，实际 %s", suspended.Body)
	}
	failed := postAGUI(replacement, `{"threadId":"replace","runId":"replace-attempt","messages":[{"role":"user","content":"查余额"}]}`)
	events := parseSSE(failed.Body.String())
	if len(events) != 2 || events[0]["type"] != "RUN_STARTED" || events[1]["code"] != "cleanup_pending" || oldHistory.attempts.Load() != 1 {
		t.Fatalf("旧历史第一次补写失败应阻止新任务，实际 %s", failed.Body)
	}
	if exists, err := cp.ExistsNS(t.Context(), sid, ""); err != nil || exists {
		t.Fatalf("历史补写失败时仍应保留已清档状态，exists=%v err=%v", exists, err)
	}
	if len(load(newHistory, newKey)) != 0 || hasToolResult(load(oldHistory, oldKey), "c1") {
		t.Fatal("首次补写失败时旧调用应保持未配对，新历史应为空")
	}
	resumed := resumeEntries(t, replacement, "replace", "late-answer", resolved(id, true))
	if outcomeType(resumed) == "success" || !strings.Contains(strings.Join(eventTypes(resumed), ","), "RUN_ERROR") {
		t.Fatalf("旧任务已清档，不能接受迟到审批，实际 %v", resumed)
	}
	done := postAGUI(replacement, `{"threadId":"replace","runId":"replace-retry","messages":[{"role":"user","content":"查余额"}]}`)
	if outcomeType(parseSSE(done.Body.String())) != "success" {
		t.Fatalf("重试应补齐旧记录后执行新任务，实际 %s", done.Body)
	}
	results := 0
	for _, msg := range load(oldHistory, oldKey) {
		if schema.GetToolResultID(msg) == "c1" {
			results++
			if msg.Annotations[agent_v2.HistoryTaskAnnotation] != sid {
				t.Fatalf("收尾结果的任务身份错误：%+v", msg.Annotations)
			}
		}
	}
	newMessages := load(newHistory, newKey)
	if results != 1 || executed.Load() != 0 || len(newMessages) == 0 ||
		hasToolResult(newMessages, "c1") || hasToolCall(newMessages, "c1") {
		t.Fatalf("旧分区应有一个配对结果且新分区只记录自己的运行，结果数=%d 工具执行=%d 新历史=%+v", results, executed.Load(), newMessages)
	}
}

func TestHandler_ReplacementHistorySurvivesCapacityWithoutCheckpointer(t *testing.T) {
	history := &localReplacementHistory{HistoryStore: agent_v2.NewInMemoryHistoryStore(), failFirst: 2}
	var executed atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			executed.Add(1)
			return tool.Success("已转账"), nil
		}))
	handler := ownedHandler(func(hook hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("transfer-agent"), agent_v2.WithModel(transferModel{}),
			agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHistory(history, "partition"), agent_v2.WithHooks(hook))
	}, agui.WithMaxSessions(1))

	suspended := postAGUI(handler, `{"threadId":"previous","runId":"old","messages":[{"role":"user","content":"转账"}]}`)
	if id := findInterruptID(parseSSE(suspended.Body.String())); id == "" {
		t.Fatalf("未配存档的旧任务仍应生成待补写工具记录，实际 %s", suspended.Body)
	}
	replacement := `{"threadId":"previous","runId":"replace","messages":[{"role":"user","content":"查余额"}]}`
	first := postAGUI(handler, replacement)
	if !strings.Contains(first.Body.String(), "cleanup_pending") || history.attempts.Load() != 1 {
		t.Fatalf("第一次收尾应失败一次并保持旧条目，实际尝试 %d 响应 %s", history.attempts.Load(), first.Body)
	}

	other := postAGUI(handler, `{"threadId":"other","runId":"other","messages":[{"role":"user","content":"查余额"}]}`)
	if outcomeType(parseSSE(other.Body.String())) != "success" || history.attempts.Load() != 2 {
		t.Fatalf("容量回收应尝试且保留失败的收尾，实际尝试 %d 响应 %s", history.attempts.Load(), other.Body)
	}
	retry := postAGUI(handler, replacement)
	if outcomeType(parseSSE(retry.Body.String())) != "success" || history.attempts.Load() != 3 || executed.Load() != 0 {
		t.Fatalf("同线程重试应完成原历史收尾并开始新任务，尝试 %d 工具执行 %d 响应 %s", history.attempts.Load(), executed.Load(), retry.Body)
	}
	msgs, err := agent_v2.LoadAllHistory(t.Context(), history, agent_v2.HistoryKey{Owner: "test-owner", ConvID: "previous"})
	if err != nil || !hasToolResult(msgs, "c1") {
		t.Fatalf("成功补写的原调用结果缺失，历史 %v 错误 %v", msgs, err)
	}
}
