package sql_test

import (
	"context"
	dbsql "database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type cancelAfterAGUIRun struct {
	agent_v2.Agent
	cancel context.CancelFunc
}

func (a cancelAfterAGUIRun) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message,
	opts ...agent_v2.RunOption,
) ([]*agent_v2.AgentEvent, error) {
	events, err := a.Agent.Run(ctx, sess, input, opts...)
	a.cancel()
	return events, err
}

type leaseLossDuringPendingRead struct {
	*crewsql.AGUISessionStore
	lose func()
}

func (s leaseLossDuringPendingRead) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	cp, err := s.AGUISessionStore.LoadNS(ctx, sid, ns)
	if err == nil && cp != nil && len(cp.PendingInterrupts) > 0 {
		s.lose()
	}
	return cp, err
}

func TestSQLiteAGUIRunOutcome_PendingAfterConnectionOrLeaseLoss(t *testing.T) {
	for _, tc := range []struct {
		name      string
		loseLease bool
	}{
		{"caller_disconnect_preserves_interrupt", false},
		{"lease_loss_fences_pending_projection", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pending-outcome.db")
			f := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
				db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				return db
			})
			const owner = "alice"
			q := &questions{}
			factory := q.factory(t)
			request := aguiRequest(t, owner, http.MethodPost, aguiStart())
			ctx, cancel := context.WithCancelCause(request.Context())
			defer cancel(nil)
			var store agui.SessionStore = f.a
			runFactory := factory
			if tc.loseLease {
				store = leaseLossDuringPendingRead{AGUISessionStore: f.a, lose: func() { cancel(compose_v2.ErrTaskLeaseLost) }}
			} else {
				runFactory = func(hooks hook.Hooks) agent_v2.Agent {
					return cancelAfterAGUIRun{Agent: factory(hooks), cancel: func() { cancel(nil) }}
				}
			}

			resp := httptest.NewRecorder()
			sharedHandler(runFactory, store).ServeHTTP(resp, request.WithContext(ctx))
			body := resp.Body.String()
			interrupt := strings.Contains(body, `"outcome":{"type":"interrupt"`)
			if tc.loseLease {
				if interrupt || !strings.Contains(body, `"code":"checkpoint_stale"`) || context.Cause(ctx) != compose_v2.ErrTaskLeaseLost {
					t.Fatalf("读取时丢权应拒绝旧投影，实际 %s，ctx %v", body, context.Cause(ctx))
				}
			} else if !interrupt || strings.Contains(body, "session_store_failed") {
				t.Fatalf("已落盘的中断应在断开竞争后仍能投影，实际 %s", body)
			}

			peer := sharedHandler(factory, f.b)
			id := onlyAGUIQuestion(t, aguiPending(t, peer, owner), "q1")
			if (!tc.loseLease && !strings.Contains(body, id)) || q.first.Load() != 1 {
				t.Fatalf("原挂起问题应保留，实际问题 %q，节点执行 %d 次", id, q.first.Load())
			}
			if next := aguiPost(t, peer, owner, aguiAnswer(id, "resolved", "first")); !strings.Contains(next.Body.String(), `"message":"q2"`) {
				t.Fatalf("另一实例应能继续原任务，实际 %s", next.Body)
			}
		})
	}
}
