package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

type noteWriteCounter struct {
	store.Store
	writes atomic.Int32
}

func (s *noteWriteCounter) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	s.writes.Add(1)
	return s.Store.Put(ctx, ns, key, value, opts...)
}

func noteAnswer(t *testing.T, err error, text string) agent_v2.ResumeOption {
	t.Helper()
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		t.Fatalf("预期一个挂起问题: %v", err)
	}
	return agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interrupt.AllQueries()[0].InterruptID: schema.UserMessage(text),
	})
}

func TestNoteToolsRecoverAfterCompactionWithoutCrossingWriterOrOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	ctx = compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, "team.alpha/member"), "planning")
	const (
		writerName = "planner"
		writerID   = "agent/planner"
		key        = "release.17"
		marker     = "raw-history-body"
	)
	body := "NOTE-C12 " + strings.Repeat("details ", 180)
	hs := agent_v2.NewInMemoryHistoryStore()
	notes := &noteWriteCounter{Store: store.NewInMemoryStore()}
	callArgs := fmt.Sprintf(`{"key":%q,"text":%q,"writer":"agent/other","owner":"team/alpha.member"}`, key, body)
	writer := agent_v2.NewBasicAgent(agent_v2.WithName(writerName), agent_v2.WithHistory(hs, "fallback"), agent_v2.WithNoteTools(notes),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
		agent_v2.WithModel(&fnModel{fn: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
			if saved := recallResult(messages, "save"); saved != "" {
				if !strings.Contains(saved, writerID) || strings.Contains(saved, body) {
					return nil, fmt.Errorf("incorrect write acknowledgement: %q", saved)
				}
				return schema.AssistantMessage("saved"), nil
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{recallCall("save", "write_note", callArgs)}), nil
		}}),
		agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
			return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
				result, err := next(ctx, name, args)
				if err != nil || result == nil || !result.Success {
					return result, err
				}
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "保存后继续？"}); err != nil {
					return nil, err
				}
				return result, nil
			}
		}))
	session := agent_v2.NewRunSession()
	_, err := writer.Run(ctx, session, schema.UserMessage(marker+strings.Repeat(" inventory for this environment", 2200)))
	approve := noteAnswer(t, err, agent_v2.ApprovalOptionApprove)
	if notes.writes.Load() != 0 {
		t.Fatalf("用户授权前已写入 %d 次", notes.writes.Load())
	}
	_, err = writer.Resume(ctx, session.RunID, approve)
	confirm := noteAnswer(t, err, "继续")
	if notes.writes.Load() != 1 {
		t.Fatalf("批准后实际写入 %d 次", notes.writes.Load())
	}
	if _, err := writer.Resume(ctx, session.RunID, confirm); err != nil || notes.writes.Load() != 1 {
		t.Fatalf("端点之后续跑不应再写: writes=%d err=%v", notes.writes.Load(), err)
	}
	// 增加未被压缩的近期轮次，旧的大段内容进入可压缩前缀。
	filler := agent_v2.NewBasicAgent(agent_v2.WithName(writerName), agent_v2.WithHistory(hs, "fallback"),
		agent_v2.WithModel(&fnModel{fn: func(context.Context, []*schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("近期记录"), nil
		}}))
	for _, text := range []string{"复核优先级", "确认最近安排", "检查当前状态"} {
		if _, err := filler.Run(ctx, nil, schema.UserMessage(text)); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := notes.Search(ctx, store.NS("__crew_notes__"), store.WithLimit(8))
	if err != nil || len(stored) != 1 || len(stored[0].Namespace) != 3 || stored[0].Key != key {
		t.Fatalf("便签的存储分区异常: %v err=%v", stored, err)
	}
	decoy := append(append(store.Namespace(nil), stored[0].Namespace...), "descendant")
	if err := notes.Store.Put(ctx, decoy, "wrong-namespace", "decoy"); err != nil {
		t.Fatal(err)
	}
	summary := &countingSummarizer{}
	spills := store.NewInMemoryStore()
	reader := agent_v2.NewBasicAgent(agent_v2.WithName(writerName), agent_v2.WithHistory(hs, "fallback"), agent_v2.WithNoteTools(notes),
		agent_v2.WithAutoCompact(smallWindow(summary)),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 256, Store: spills}),
		agent_v2.WithModel(&fnModel{fn: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
			if content := recallResult(messages, "read"); content != "" {
				var result struct{ Key, Text, Writer, Source string }
				if err := json.Unmarshal([]byte(content), &result); err != nil || result.Text != body || result.Writer != writerID || result.Source != "model_note" {
					return nil, fmt.Errorf("incorrect note after compaction: %s", content)
				}
				return schema.AssistantMessage("取回完成"), nil
			}
			if listed := recallResult(messages, "list"); listed != "" {
				if !strings.Contains(listed, key) || strings.Contains(listed, "wrong-namespace") || strings.Contains(listed, "NOTE-C12") {
					return nil, fmt.Errorf("list should contain only exact partition keys: %s", listed)
				}
				return schema.AssistantToolCallMessage([]schema.ToolCall{recallCall("read", "read_note", `{"key":"release.17"}`)}), nil
			}
			seen := concatContent(messages)
			if !strings.Contains(seen, "SUMMARY-S") || strings.Contains(seen, marker) || strings.Contains(seen, "NOTE-C12") {
				return nil, errors.New("expected a real compacted view with no raw or pre-injected note")
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{recallCall("list", "list_notes", `{}`)}), nil
		}}))
	if events, err := reader.Run(ctx, nil, schema.UserMessage("取回决定")); err != nil || summary.count() == 0 || agent_v2.FinalMessage(events) == nil {
		t.Fatalf("真实压缩后 notes 无法取回: summary calls=%d err=%v", summary.count(), err)
	}
	if exported, err := spills.Search(ctx, store.NS("_tool_spill")); err != nil || len(exported) != 0 {
		t.Fatalf("授权便签生成了通用取回副本: count=%d err=%v", len(exported), err)
	}
	for _, tc := range []struct {
		owner, want string
		readable    []string
	}{
		{"team.alpha/member", "NOTE-C12", []string{writerID}},
		{"team.alpha/member", "notes_unavailable", nil},
		{"team/alpha.member", "note_not_found", []string{writerID}},
	} {
		other := agent_v2.NewBasicAgent(agent_v2.WithName("reviewer"), agent_v2.WithNoteTools(notes, tc.readable...),
			agent_v2.WithModel(&fnModel{fn: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
				if got := recallResult(messages, "check"); got != "" {
					return schema.AssistantMessage(got), nil
				}
				return schema.AssistantToolCallMessage([]schema.ToolCall{recallCall("check", "read_note",
					`{"key":"release.17","writer":"agent/planner","owner":"team.alpha/member"}`)}), nil
			}}))
		request := compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, tc.owner), "independent-thread")
		events, err := other.Run(request, nil, schema.UserMessage("读计划"))
		final := agent_v2.FinalMessage(events)
		if err != nil || final == nil || !strings.Contains(final.Content, tc.want) ||
			(tc.want != "NOTE-C12" && strings.Contains(final.Content, "NOTE-C12")) {
			t.Fatalf("用户或写入者的读取边界错误: owner=%s final=%v err=%v", tc.owner, final, err)
		}
	}
}

func TestNoteApprovalIsScopedToTheWriterPosition(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithOwner(ctx, "review-team"), compose_v2.NewInMemoryCheckpointer())
	notes := &noteWriteCounter{Store: store.NewInMemoryStore()}
	member := agent_v2.NewBasicAgent(agent_v2.WithName("reviewer"), agent_v2.WithNoteTools(notes), agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
		agent_v2.WithModel(&fnModel{fn: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
			if len(messages) > 0 && schema.GetToolResultID(messages[len(messages)-1]) == "write" {
				return schema.AssistantMessage("saved"), nil
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{recallCall("write", "write_note", `{"key":"decision","text":"ok"}`)}), nil
		}}))
	flow := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("two-writers")).AddStep("first", member).AddStep("second", member)
	session := agent_v2.NewRunSession()
	_, err := flow.Run(ctx, session, schema.UserMessage("保存复核结果"))
	first := noteAnswer(t, err, agent_v2.ApprovalOptionApproveAlwaysTool)
	_, err = flow.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session), first)
	second := noteAnswer(t, err, agent_v2.ApprovalOptionApprove)
	if notes.writes.Load() != 1 {
		t.Fatalf("首个写入者的长期批准串到了另一分区: %d 次写入", notes.writes.Load())
	}
	if _, err := flow.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session), second); err != nil || notes.writes.Load() != 2 {
		t.Fatalf("第二个分区独立批准后写入次数=%d err=%v", notes.writes.Load(), err)
	}
	rows, err := notes.Search(ctx, store.NS("__crew_notes__"))
	if err != nil || len(rows) != 2 || rows[0].Namespace.Equal(rows[1].Namespace) {
		t.Fatalf("两成员应有独立分区: %v err=%v", rows, err)
	}
}
