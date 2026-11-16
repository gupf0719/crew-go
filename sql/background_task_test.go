package sql_test

import (
	"context"
	"crypto/rand"
	dbsql "database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agent_v2/background"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/tool"
)

func openBackgroundStore(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) (*crewsql.BackgroundTaskStore, *crewsql.BackgroundTaskStore) {
	t.Helper()
	name := "bg_" + strings.ToLower(rand.Text())
	a, b := open(), open()
	t.Cleanup(func() {
		for _, table := range []string{name + "_background_task", name + "_task_lease", name} {
			if _, err := b.Exec("DROP TABLE IF EXISTS " + table); err != nil {
				t.Error(err)
			}
		}
		_ = a.Close() // 测试可能已模拟断开。
		_ = b.Close() // 释放测试连接。
	})
	makeStore := func(db *dbsql.DB) *crewsql.BackgroundTaskStore {
		return crewsql.NewBackgroundTaskStore(crewsql.NewCheckpointer(db, dialect, crewsql.WithCheckpointerTable(name)))
	}
	one, two := makeStore(a), makeStore(b)
	for _, st := range []*crewsql.BackgroundTaskStore{one, two} {
		if err := st.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	return one, two
}

func backgroundManager(t *testing.T, store background.Store, cfg background.Config) *background.Manager {
	t.Helper()
	m, err := background.New(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func openBackgroundSQLite(t *testing.T, path string) *dbsql.DB {
	t.Helper()
	db, err := dbsql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func TestSQLiteBackgroundTasks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.sqlite")
	runBackgroundTaskScenarios(t, crewsql.SQLite{}, func() *dbsql.DB { return openBackgroundSQLite(t, path) })
}

type backgroundModel struct{}

func (backgroundModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool && schema.GetToolResultID(msg) == "call_asset" && strings.Contains(msg.Content, "booked oak") {
			return schema.AssistantMessage("confirmed " + msg.Content), nil
		}
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].Content != "reserve oak" {
		return nil, errors.New("background model did not get the task input or result")
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "call_asset", Type: "function",
		Function: schema.FunctionCall{Name: "reserve", Arguments: `{"asset":"oak"}`}}}), nil
}

func (m backgroundModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(out), nil
}

func (m backgroundModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	for _, tool := range tools {
		if tool.Name == "reserve" {
			return m, nil
		}
	}
	return nil, errors.New("background model was not bound to reserve")
}

func runBackgroundTaskScenarios(t *testing.T, dialect crewsql.Dialect, open func() *dbsql.DB) {
	t.Helper()
	t.Run("approval_deduplication_and_notification", func(t *testing.T) {
		storeA, storeB := openBackgroundStore(t, dialect, open)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		parent := background.Parent{Owner: "alice", TaskID: "parent-task", ThreadID: "parent-thread"}
		var calls, notices atomic.Int32
		factory := func(ctx context.Context, id string) (agent_v2.Agent, error) {
			if compose_v2.GetOwner(ctx) != parent.Owner || compose_v2.GetThreadID(ctx) != id {
				return nil, errors.New("unexpected owner or child thread")
			}
			kit := tool.NewToolkit()
			kit.Register(tool.NewFuncTool("reserve", "Reserve an asset", nil,
				func(ctx context.Context, args map[string]any) (*tool.Result, error) {
					calls.Add(1)
					if args["asset"] != "oak" || compose_v2.GetSessionID(ctx) != id {
						return nil, errors.New("tool did not receive isolated task and arguments")
					}
					return tool.Success("booked oak"), nil
				}))
			return agent_v2.NewBasicAgent(agent_v2.WithName("reservation"),
				agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithModel(backgroundModel{}), agent_v2.WithToolkit(kit)), nil
		}
		cfg := background.Config{Workers: map[string]background.Factory{"reservation@v1": factory}}
		a, b := backgroundManager(t, storeA, cfg), backgroundManager(t, storeB, cfg)
		task, err := a.Submit(ctx, parent, "reserve", "reservation@v1", schema.UserMessage("reserve oak"))
		if err != nil {
			t.Fatal(err)
		}
		retry, err := b.Submit(ctx, parent, "reserve", "reservation@v1", schema.UserMessage("reserve oak"))
		if err != nil || retry.ID != task.ID || retry.Input.ID != task.Input.ID {
			t.Fatalf("duplicate submission should preserve the original input: %+v %v", retry, err)
		}
		if _, err := b.Submit(ctx, parent, "reserve", "reservation@v1", schema.UserMessage("reserve elm")); !errors.Is(err, background.ErrRequestConflict) {
			t.Fatalf("different input under the same idempotency key: %v", err)
		}
		if err := a.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		waiting, err := b.Get(ctx, parent, task.ID)
		if err != nil || waiting.Status != background.Waiting || len(waiting.Pending) != 1 || waiting.Pending[0].ToolName != "reserve" || calls.Load() != 0 {
			t.Fatalf("tool executed before durable approval: %+v %v calls=%d", waiting, err, calls.Load())
		}
		pendingID := waiting.Pending[0].InterruptID
		for _, foreign := range []background.Parent{
			{Owner: "bob", TaskID: parent.TaskID, ThreadID: parent.ThreadID},
			{Owner: parent.Owner, TaskID: "different-task", ThreadID: parent.ThreadID},
		} {
			if _, err := b.Answer(ctx, foreign, task.ID, map[string]string{pendingID: "approve"}); !errors.Is(err, background.ErrNotFound) {
				t.Fatalf("foreign parent saw the task: %v", err)
			}
		}
		if _, err := b.Answer(ctx, parent, task.ID, map[string]string{"old": "approve"}); !errors.Is(err, background.ErrNotWaiting) {
			t.Fatalf("stale answer accepted: %v", err)
		}
		for range 2 {
			if _, err := b.Answer(ctx, parent, task.ID, map[string]string{pendingID: "approve"}); err != nil {
				t.Fatal(err)
			}
		}
		cfg.Notify = func(ctx context.Context, completed background.Task) error {
			if completed.ID != task.ID || completed.Parent != parent || completed.Result == nil ||
				!strings.Contains(completed.Result.Content, "booked oak") || compose_v2.GetThreadID(ctx) != parent.ThreadID {
				return fmt.Errorf("unexpected notification: %+v", completed)
			}
			if notices.Add(1) == 1 {
				return io.ErrUnexpectedEOF
			}
			completed.Result.Content = "callback changed its copy"
			return nil
		}
		b = backgroundManager(t, storeB, cfg)
		if err := b.RunOnce(ctx); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("failed notification should remain pending: %v", err)
		}
		finished, err := a.Get(ctx, parent, task.ID)
		if err != nil || finished.Status != background.Succeeded || finished.Notified || calls.Load() != 1 {
			t.Fatalf("expected one committed success before notification retry: %+v %v calls=%d", finished, err, calls.Load())
		}
		if cp, err := storeA.LoadNS(ctx, task.ID, ""); err != nil || cp != nil {
			t.Fatalf("terminal transaction did not clean the child checkpoint: %+v %v", cp, err)
		}
		peer := backgroundManager(t, storeA, cfg)
		if err := peer.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if err := peer.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		finished, err = peer.Get(ctx, parent, task.ID)
		if err != nil || !finished.Notified || !strings.Contains(finished.Result.Content, "booked oak") ||
			notices.Load() != 2 || calls.Load() != 1 {
			t.Fatalf("notification retry changed result or reran task: %+v %v notices=%d calls=%d", finished, err,
				notices.Load(), calls.Load())
		}
	})

	t.Run("current_answer_batch", func(t *testing.T) {
		storeA, storeB := openBackgroundStore(t, dialect, open)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		parent := background.Parent{Owner: "alice", TaskID: "batch-parent"}
		out := compose_v2.LastValue[*schema.Message]("batch_result")
		builder := compose_v2.NewBuilder()
		for _, id := range []compose_v2.NodeID{"first_a", "first_b"} {
			builder.AddNode(id, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: string(id)})
				if err != nil {
					return nil, err
				}
				if answer.Content != string(id) {
					return nil, fmt.Errorf("wrong answer at %s: %q", id, answer.Content)
				}
				return nil, nil
			}).AddEdge(compose_v2.StartNode, id).AddEdge(id, "final")
		}
		builder.AddNode("final", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "final answer"})
			if err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage("received "+answer.Content))}, nil
		}).AddEdge("final", compose_v2.EndNode)
		agent, err := backgroundGraph("batch", out, builder)
		if err != nil {
			t.Fatal(err)
		}
		cfg := background.Config{Workers: map[string]background.Factory{
			"batch@v1": func(context.Context, string) (agent_v2.Agent, error) { return agent, nil },
		}}
		a, b := backgroundManager(t, storeA, cfg), backgroundManager(t, storeB, cfg)
		task, err := a.Submit(ctx, parent, "questionnaire", "batch@v1", schema.UserMessage("ask"))
		if err != nil {
			t.Fatal(err)
		}
		if err := a.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		first, err := b.Get(ctx, parent, task.ID)
		if err != nil || first.Status != background.Waiting || len(first.Pending) != 2 {
			t.Fatalf("expected two pending questions: %+v %v", first, err)
		}
		full := make(map[string]string, 2)
		for _, q := range first.Pending {
			full[q.InterruptID] = q.Question
		}
		accepted, err := b.Answer(ctx, parent, task.ID, full)
		if err != nil || accepted.Status != background.Running {
			t.Fatalf("could not accept the complete batch: %+v %v", accepted, err)
		}
		retry, err := a.Answer(ctx, parent, task.ID, full)
		if err != nil || retry.Revision != accepted.Revision {
			t.Fatalf("identical retry modified the task: %+v %v", retry, err)
		}
		q := first.Pending[0]
		if _, err := a.Answer(ctx, parent, task.ID, map[string]string{q.InterruptID: q.Question}); !errors.Is(err, background.ErrNotWaiting) {
			t.Fatalf("partial retry was accepted as the whole batch: %v", err)
		}
		if err := b.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		next, err := a.Get(ctx, parent, task.ID)
		if err != nil || next.Status != background.Waiting || len(next.Pending) != 1 || next.Pending[0].Question != "final answer" {
			t.Fatalf("expected the next question: %+v %v", next, err)
		}
		old := map[string]string{q.InterruptID: q.Question}
		if _, err := a.Answer(ctx, parent, task.ID, old); !errors.Is(err, background.ErrNotWaiting) {
			t.Fatalf("an earlier answer was accepted as current: %v", err)
		}
		if _, err := a.Answer(ctx, parent, task.ID, map[string]string{next.Pending[0].InterruptID: "done"}); err != nil {
			t.Fatal(err)
		}
		if err := a.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		done, err := b.Get(ctx, parent, task.ID)
		if err != nil || done.Status != background.Succeeded || done.Result == nil || done.Result.Content != "received done" {
			t.Fatalf("the new answer did not reach the agent: %+v %v", done, err)
		}
	})

	t.Run("active_cancel_and_exclusive_worker", func(t *testing.T) {
		storeA, storeB := openBackgroundStore(t, dialect, open)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		parent := background.Parent{Owner: "alice", TaskID: "parent-a"}
		started := make(chan struct{})
		var runs atomic.Int32
		var once sync.Once
		factory := func(_ context.Context, _ string) (agent_v2.Agent, error) {
			g, err := compose_v2.NewBuilder().WithName("long-work").
				AddNode("block", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					runs.Add(1)
					once.Do(func() { close(started) })
					<-ctx.Done()
					return nil, ctx.Err()
				}).
				AddEdge(compose_v2.StartNode, "block").AddEdge("block", compose_v2.EndNode).Compile()
			if err != nil {
				return nil, err
			}
			return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("long-work"),
				agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
				agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil })), nil
		}
		cfg := background.Config{Workers: map[string]background.Factory{"long@v1": factory}, PollInterval: 10 * time.Millisecond}
		a, b := backgroundManager(t, storeA, cfg), backgroundManager(t, storeB, cfg)
		task, err := a.Submit(ctx, parent, "long", "long@v1", schema.UserMessage("go"))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- a.RunOnce(ctx) }()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("worker did not start")
		}
		if err := b.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Cancel(ctx, parent, task.ID); err != nil {
			t.Fatal(err)
		}
		var runErr error
		select {
		case runErr = <-done:
			done <- runErr // 清理等到同一个工作进程退出。
		case <-ctx.Done():
			t.Fatal("running worker did not observe cancellation")
		}
		finished, err := b.Get(ctx, parent, task.ID)
		if runErr != nil || err != nil || finished.Status != background.Cancelled || runs.Load() != 1 {
			t.Fatalf("expected one cancelled execution: %+v %v run=%v runs=%d", finished, err, runErr, runs.Load())
		}
		if cp, err := storeB.LoadNS(ctx, task.ID, ""); err != nil || cp != nil {
			t.Fatalf("cancel left a checkpoint: %+v %v", cp, err)
		}
	})
}
