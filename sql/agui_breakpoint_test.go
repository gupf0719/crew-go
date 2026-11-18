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
