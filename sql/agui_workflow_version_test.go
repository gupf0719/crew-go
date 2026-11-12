package sql_test

import (
	"context"
	dbsql "database/sql"
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
	crewsql "crew-go/sql"
)

func versionFactory(t *testing.T, next compose_v2.NodeID, oldCalls, newCalls *atomic.Int32) agui.AgentFactory {
	t.Helper()
	// 两版故意共用 GraphID、节点和 channel，只改审批后的边。
	out := compose_v2.LastValue[*schema.Message]("out")
	b := compose_v2.NewBuilder().WithName("approval")
	compose_v2.WithChannel(b, out)
	b.AddNode("gate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve revision"})
		if err != nil {
			return nil, err
		}
		if answer.Content != "yes" {
			return nil, errors.New("expected the original approval answer")
		}
		return nil, nil
	})
	for _, node := range []struct {
		id    compose_v2.NodeID
		calls *atomic.Int32
	}{{"old", oldCalls}, {"new", newCalls}} {
		b.AddNode(node.id, func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			node.calls.Add(1)
			return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage(string(node.id)))}, nil
		}).AddEdge(node.id, compose_v2.EndNode)
	}
	b.AddEdge(compose_v2.StartNode, "gate").AddEdge("gate", next)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return func(h hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("approval"),
			agent_v2.WithGraphAgentHooks(h),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(channels compose_v2.Channels) *schema.Message {
				msg, _ := compose_v2.Get(channels, out) // 后继节点负责写入。
				return msg
			}))
	}
}

func checkWorkflowVersions(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) {
	t.Helper()
	for _, recorded := range []string{"v1", ""} {
		name := recorded
		if name == "" {
			name = "unversioned_record"
		}
		t.Run("workflow_revision_"+name, func(t *testing.T) {
			f, journalA, journalB := setupRunJournal(t, dialect, open)
			var oldCalls, newCalls atomic.Int32
			oldFactory := versionFactory(t, "old", &oldCalls, &newCalls)
			newFactory := versionFactory(t, "new", &oldCalls, &newCalls)
			startOpts := []agui.Option{agui.WithRunStore(journalA, 0)}
			if recorded != "" {
				startOpts = append(startOpts, agui.WithWorkflowVersions(recorded, nil))
			}
			a := sharedHandler(oldFactory, f.a, startOpts...)
			noOld := sharedHandler(newFactory, f.b, agui.WithRunStore(journalB, 0), agui.WithWorkflowVersions("v2", nil))
			b := sharedHandler(newFactory, f.b, agui.WithRunStore(journalB, 0),
				agui.WithWorkflowVersions("v2", map[string]agui.AgentFactory{recorded: oldFactory}))

			if r := aguiPost(t, a, "alice", aguiStart()); !strings.Contains(r.Body.String(), "interrupt") {
				t.Fatalf("old workflow did not suspend: %d %s", r.Code, r.Body)
			}
			if err := f.dbA.Close(); err != nil {
				t.Fatal(err)
			}
			p := aguiPending(t, b, "alice")
			id := onlyAGUIQuestion(t, p, "approve revision")
			if p.Version != recorded {
				t.Fatalf("pending workflow revision: got %q want %q", p.Version, recorded)
			}

			r := aguiPost(t, noOld, "alice", aguiAnswer(id, "resolved", "yes"))
			if r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), "workflow_version_unavailable") ||
				oldCalls.Load()+newCalls.Load() != 0 {
				t.Fatalf("missing version must fail before running: %d %s (%d/%d)", r.Code, r.Body, oldCalls.Load(), newCalls.Load())
			}
			p = aguiPending(t, b, "alice")
			if got := onlyAGUIQuestion(t, p, "approve revision"); got != id || len(p.Accepted) != 0 || p.Version != recorded {
				t.Fatalf("version rejection changed the pending approval: %+v", p)
			}

			r = aguiPost(t, b, "alice", aguiAnswer(id, "resolved", "yes"))
			if r.Code != 200 || !strings.Contains(r.Body.String(), `"type":"success"`) || oldCalls.Load() != 1 || newCalls.Load() != 0 {
				t.Fatalf("old task ran the wrong version: %d %s (%d/%d)", r.Code, r.Body, oldCalls.Load(), newCalls.Load())
			}
			start := aguiStart()
			start["runId"] = "second-run"
			if r := aguiPost(t, b, "alice", start); !strings.Contains(r.Body.String(), "interrupt") {
				t.Fatalf("new workflow did not suspend: %d %s", r.Code, r.Body)
			}
			p = aguiPending(t, b, "alice")
			id = onlyAGUIQuestion(t, p, "approve revision")
			r = aguiPost(t, b, "alice", aguiAnswer(id, "resolved", "yes"))
			if r.Code != 200 || !strings.Contains(r.Body.String(), `"type":"success"`) ||
				p.Version != "v2" || oldCalls.Load() != 1 || newCalls.Load() != 1 {
				t.Fatalf("new task did not use v2: %d %s (%s, %d/%d)", r.Code, r.Body, p.Version, oldCalls.Load(), newCalls.Load())
			}
		})
	}
}
