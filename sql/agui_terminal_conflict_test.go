package sql_test

import (
	"bytes"
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
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type terminalConflictSessions struct {
	*crewsql.AGUISessionStore
	failUnbind bool
	sealWrites atomic.Int32
}

func (s *terminalConflictSessions) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if ns == "" && cp.Status == compose_v2.CPStatusAborted {
		s.sealWrites.Add(1)
		return errors.New("injected terminal checkpoint write failure")
	}
	return s.AGUISessionStore.SaveNS(ctx, sid, ns, cp)
}

func (s *terminalConflictSessions) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	if s.failUnbind && record == nil && dropTask == "" {
		return 0, errors.New("injected session unbind failure")
	}
	return s.AGUISessionStore.SaveSession(ctx, key, record, dropTask)
}

func TestSQLiteAGUITerminalConflictClosesResume(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failUnbind bool
		wantCode   string
	}{
		{"terminal_checkpoint_write_failed", false, "graph_id_conflict"},
		{"session_unbind_also_failed", true, "session_store_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "terminal-conflict.db")
			f, journal, peerJournal := setupRunJournal(t, crewsql.SQLite{}, func() *dbsql.DB {
				db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				return db
			})
			store := &terminalConflictSessions{AGUISessionStore: f.a, failUnbind: tc.failUnbind}
			factory, replaceChild, writes, sid := terminalConflictWorkflow(t)
			h := sharedHandler(factory, store, agui.WithRunStore(journal, 0))
			peer := sharedHandler(factory, f.b, agui.WithRunStore(peerJournal, 0))
			const owner = "alice"
			first := aguiPost(t, h, owner, aguiStart())
			pending := aguiPending(t, peer, owner)
			if first.Code != http.StatusOK || len(pending.Interrupts) != 2 || writes.Load() != 0 {
				t.Fatalf("应先挂起两个分支，实际 %d %s，问题 %+v，写入 %d 次", first.Code, first.Body, pending, writes.Load())
			}
			taskID := sid.Load().(string)
			ctx := compose_v2.WithOwner(t.Context(), owner)
			checkpoint := func(ns string) []byte {
				t.Helper()
				cp, err := f.b.LoadNS(ctx, taskID, ns)
				if err != nil || cp == nil {
					t.Fatalf("应保留 namespace %q 的存档，实际 %v", ns, err)
				}
				wire, err := compose_v2.MarshalCheckpoint(cp)
				if err != nil {
					t.Fatal(err)
				}
				return wire
			}
			root, child := checkpoint(""), checkpoint("child")
			replaceChild()
			answers := make([]agui.ResumeEntry, 0, len(pending.Interrupts))
			for _, question := range pending.Interrupts {
				answers = append(answers, agui.ResumeEntry{InterruptID: question.ID, Status: agui.ResumeStatusResolved, Payload: "yes"})
			}
			const runID = "terminal-conflict"
			body := map[string]any{"threadId": "shared", "runId": runID, "resume": answers}
			result := aguiPost(t, h, owner, body)
			wantCode := `"code":"` + tc.wantCode + `"`
			receipt := journalQuery(t, peer, owner, runID, "")
			if result.Code != http.StatusOK || !strings.Contains(result.Body.String(), wantCode) ||
				receipt.Code != http.StatusOK || !strings.Contains(receipt.Body.String(), `"status":"error"`) ||
				!strings.Contains(receipt.Body.String(), wantCode) || store.sealWrites.Load() != 1 || writes.Load() != 1 {
				t.Fatalf("终局应只执行一次且留下对应错误回执，实际 %d %s，回执 %d %s，封存尝试 %d，写入 %d",
					result.Code, result.Body, receipt.Code, receipt.Body, store.sealWrites.Load(), writes.Load())
			}
			if replay := aguiPost(t, peer, owner, body); !strings.Contains(replay.Body.String(), wantCode) || writes.Load() != 1 {
				t.Fatalf("相同 runId 应重放终态，实际 %d %s，写入 %d 次", replay.Code, replay.Body, writes.Load())
			}
			if !tc.failUnbind {
				pending = aguiPending(t, peer, owner)
				body["runId"] = "retry-terminal-conflict"
				retry := aguiPost(t, peer, owner, body)
				if len(pending.Interrupts) != 0 || retry.Code != http.StatusNotFound ||
					!strings.Contains(retry.Body.String(), `"code":"no_session"`) || writes.Load() != 1 {
					t.Fatalf("原挂起档仍在时也必须禁止换 runId 重放，实际问题 %v，重试 %d %s，写入 %d 次",
						pending.Interrupts, retry.Code, retry.Body, writes.Load())
				}
			}
			if !bytes.Equal(root, checkpoint("")) || !bytes.Equal(child, checkpoint("child")) {
				t.Fatal("解绑会话不应改写根档或冲突子档")
			}
		})
	}
}

func terminalConflictWorkflow(t *testing.T) (agui.AgentFactory, func(), *atomic.Int32, *atomic.Value) {
	t.Helper()
	buildChild := func(name string) *compose_v2.Graph {
		g, err := compose_v2.NewBuilder().WithName(name).
			AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准子流程？"})
				return nil, err
			}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	original, incompatible := buildChild("original-child"), buildChild("incompatible-child")
	child := original
	var writes atomic.Int32
	var sid atomic.Value
	parent, err := compose_v2.NewBuilder().WithName("parent").
		AddNode("child", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sid.Store(compose_v2.GetSessionID(ctx))
			return nil, compose_v2.Run(ctx, child, compose_v2.NewChannelManager(), compose_v2.WithRunNS("child"))
		}).
		AddNode("terminal", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "开始写入？"}); err != nil {
				return nil, err
			}
			writes.Add(1)
			return nil, compose_v2.ErrRunAborted
		}).
		AddEdge(compose_v2.StartNode, "child").AddEdge(compose_v2.StartNode, "terminal").
		AddEdge("child", compose_v2.EndNode).AddEdge("terminal", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	factory := func(h hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(parent, agent_v2.WithGraphAgentName("parent"), agent_v2.WithGraphAgentHooks(h),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil }))
	}
	return factory, func() { child = incompatible }, &writes, &sid
}
