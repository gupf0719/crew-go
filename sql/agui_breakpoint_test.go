package sql_test

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
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

func TestSQLiteAGUIBreakpointQueryAndResumeOnSecondHandler(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "breakpoint.db") + "?_pragma=busy_timeout(5000)&_txlock=immediate"
	f := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
	var writes atomic.Int32
	b := compose_v2.NewBuilder().WithName("debug_shared").WithInterruptBefore("write")
	b.AddNode("write", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		writes.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "write")
	b.AddEdge("write", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	factory := func(hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph,
			agent_v2.WithGraphAgentName("debug_shared"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
				return schema.AssistantMessage("saved")
			}))
	}
	a, bServer := sharedHandler(factory, f.a), sharedHandler(factory, f.b)
	start := aguiPost(t, a, "owner", aguiStart())
	event := aguiFinishedOutcome(t, start.Body.String())
	if start.Code != http.StatusOK || event.Type != agui.RunOutcomeInterrupt || len(event.Interrupts) != 1 || writes.Load() != 0 {
		t.Fatalf("第一个 Handler 应在写入前挂起，实际 HTTP %d %+v，写入 %d 次", start.Code, event, writes.Load())
	}
	q := event.Interrupts[0]
	if q.ID == "" || q.Reason != "crew-go:breakpoint" ||
		!reflect.DeepEqual(q.Metadata["breakpoint"], map[string]any{"before": []any{"write"}}) {
		t.Fatalf("顶层断点应提供节点名和可续跑 ID，实际 %+v", q)
	}
	pending := aguiPending(t, bServer, "owner")
	if !reflect.DeepEqual(pending.Interrupts, event.Interrupts) {
		t.Fatalf("另一个 Handler 应从 SQLite 根档投影同一个断点，实际 %+v 与 %+v", pending.Interrupts, event.Interrupts)
	}
	if other := aguiPending(t, bServer, "other"); len(other.Interrupts) != 0 {
		t.Fatalf("别的 owner 不应看见断点，实际 %+v", other)
	}
	resume := aguiAnswer(q.ID, agui.ResumeStatusResolved, nil)
	done := aguiPost(t, bServer, "owner", resume)
	if outcome := aguiFinishedOutcome(t, done.Body.String()); done.Code != http.StatusOK || outcome.Type != agui.RunOutcomeSuccess || writes.Load() != 1 {
		t.Fatalf("断点 ID 应在第二个 Handler 准确放行，实际 HTTP %d %+v，写入 %d 次", done.Code, outcome, writes.Load())
	}
	if current := aguiPending(t, a, "owner"); len(current.Interrupts) != 0 {
		t.Fatalf("续跑完成后应清理挂起视图，实际 %+v", current)
	}
}

type diagnosticGraphAgent struct {
	*agent_v2.GraphAgent
	runOptions    []agent_v2.RunOption
	resumeOptions []agent_v2.ResumeOption
}

func (a diagnosticGraphAgent) Run(ctx context.Context, session *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return a.GraphAgent.Run(ctx, session, in, append(opts, a.runOptions...)...)
}

func (a diagnosticGraphAgent) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return a.GraphAgent.Resume(ctx, sid, append(opts, a.resumeOptions...)...)
}

func TestSQLiteAGUICallBreakpointsStepAcrossHandlers(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "diagnostics.db") + "?_pragma=busy_timeout(5000)&_txlock=immediate"
	f := setupAGUI(t, crewsql.SQLite{}, func() *dbsql.DB {
		db, err := dbsql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	})
	var prepared, written atomic.Int32
	b := compose_v2.NewBuilder().WithName("diagnostics")
	b.AddNode("prepare", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		prepared.Add(1)
		return nil, nil
	})
	b.AddNode("write", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		written.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "prepare")
	b.AddEdge("prepare", "write")
	b.AddEdge("write", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	factory := func(run []agent_v2.RunOption, resume []agent_v2.ResumeOption) agui.AgentFactory {
		return func(hook.Hooks) agent_v2.Agent {
			ag := agent_v2.NewGraphAgent(graph,
				agent_v2.WithGraphAgentName("diagnostics"),
				agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
				agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
					return schema.AssistantMessage("saved")
				}))
			return diagnosticGraphAgent{GraphAgent: ag, runOptions: run, resumeOptions: resume}
		}
	}
	a := sharedHandler(factory([]agent_v2.RunOption{agent_v2.WithRunInterruptBefore("prepare")}, nil), f.a)
	bServer := sharedHandler(factory(nil, []agent_v2.ResumeOption{
		agent_v2.WithResumeInterruptAfter("prepare"), agent_v2.WithResumeInterruptBefore("write"),
	}), f.b)

	start := aguiPost(t, a, "owner", aguiStart())
	first := aguiFinishedOutcome(t, start.Body.String())
	if start.Code != http.StatusOK || first.Type != agui.RunOutcomeInterrupt ||
		len(first.Interrupts) != 1 || prepared.Load() != 0 || written.Load() != 0 {
		t.Fatalf("首次调用应停在 prepare 前，实际 HTTP %d %+v，次数 %d/%d", start.Code, first, prepared.Load(), written.Load())
	}
	initial := first.Interrupts[0]
	if initial.ID == "" || !reflect.DeepEqual(initial.Metadata["breakpoint"], map[string]any{"before": []any{"prepare"}}) ||
		!reflect.DeepEqual(aguiPending(t, bServer, "owner").Interrupts, first.Interrupts) {
		t.Fatalf("第一次挂起应从 SQLite 重建原来的地址和位置，实际 %+v", initial)
	}

	step := aguiPost(t, bServer, "owner", aguiAnswer(initial.ID, agui.ResumeStatusResolved, nil))
	second := aguiFinishedOutcome(t, step.Body.String())
	if step.Code != http.StatusOK || second.Type != agui.RunOutcomeInterrupt ||
		len(second.Interrupts) != 1 || prepared.Load() != 1 || written.Load() != 0 {
		t.Fatalf("第二个 Handler 应执行 prepare 并停在 write 前，实际 HTTP %d %+v，次数 %d/%d",
			step.Code, second, prepared.Load(), written.Load())
	}
	current := second.Interrupts[0]
	want := map[string]any{"before": []any{"write"}, "after": []any{"prepare"}}
	if current.ID == "" || current.ID == initial.ID || !reflect.DeepEqual(current.Metadata["breakpoint"], want) {
		t.Fatalf("续跑的新断点应保持两侧位置及新地址，实际 %+v", current)
	}
	stale := aguiPost(t, a, "owner", aguiAnswer(initial.ID, agui.ResumeStatusResolved, nil))
	if !strings.Contains(stale.Body.String(), "invalid_resume") || written.Load() != 0 ||
		!reflect.DeepEqual(aguiPending(t, a, "owner").Interrupts, second.Interrupts) {
		t.Fatalf("旧地址应被拒且保留新挂起，实际 HTTP %d %s，write %d 次", stale.Code, stale.Body.String(), written.Load())
	}
	done := aguiPost(t, a, "owner", aguiAnswer(current.ID, agui.ResumeStatusResolved, nil))
	if outcome := aguiFinishedOutcome(t, done.Body.String()); done.Code != http.StatusOK ||
		outcome.Type != agui.RunOutcomeSuccess || prepared.Load() != 1 || written.Load() != 1 {
		t.Fatalf("当前地址应仅执行一次 write，实际 HTTP %d %+v，次数 %d/%d", done.Code, outcome, prepared.Load(), written.Load())
	}
}

func aguiFinishedOutcome(t *testing.T, stream string) *agui.RunFinishedOutcome {
	t.Helper()
	for _, line := range strings.Split(stream, "\n") {
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var event struct {
			Type    agui.EventType           `json:"type"`
			Outcome *agui.RunFinishedOutcome `json:"outcome"`
		}
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			t.Fatal(err)
		}
		if event.Type == agui.EventTypeRunFinished && event.Outcome != nil {
			return event.Outcome
		}
	}
	t.Fatalf("SSE 缺少 RUN_FINISHED，实际 %s", stream)
	return nil
}
