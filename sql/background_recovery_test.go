package sql_test

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agent_v2/background"
	"crew-go/compose_v2"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

func backgroundGraph(name string, out *compose_v2.Channel[*schema.Message], builder *compose_v2.GraphBuilder) (agent_v2.Agent, error) {
	compose_v2.WithChannel(builder, out)
	graph, err := builder.WithName(name).Compile()
	if err != nil {
		return nil, err
	}
	return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
			msg, _ := compose_v2.Get(ch, out) // 节点成功时写入结果。
			return msg
		})), nil
}

func TestSQLiteBackgroundTaskWakesParentAfterEarlyCompletion(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	db := openBackgroundSQLite(t, filepath.Join(t.TempDir(), "wake.sqlite"))
	defer db.Close() // 测试结束释放连接。
	cp := crewsql.NewCheckpointer(db, crewsql.SQLite{})
	st := crewsql.NewBackgroundTaskStore(cp)
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE wake_receipt (sid TEXT PRIMARY KEY, message TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	scope := background.Parent{Owner: "operator", TaskID: "parent", ThreadID: "conversation"}
	ctx = compose_v2.WithThreadIDCtx(compose_v2.WithOwner(compose_v2.WithCheckpointerCtx(ctx, cp), scope.Owner), scope.ThreadID)

	var jobs *background.Manager
	var actualParentCalls, actualChildCalls atomic.Int32
	var early = errors.New("parent has not parked yet")
	parentOut := compose_v2.LastValue[*schema.Message]("parent_result")
	parent, err := backgroundGraph("parent", parentOut, compose_v2.NewBuilder().
		AddNode("delegate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			task, err := jobs.Submit(ctx, background.ParentFromContext(ctx), "look-up", "lookup@v1", schema.UserMessage("lookup"))
			if err != nil {
				return nil, err
			}
			task, err = jobs.Await(ctx, task.ID)
			if err != nil {
				return nil, err
			}
			if task.Status != background.Succeeded || task.Result == nil || task.Result.Content != "asset ready" {
				return nil, fmt.Errorf("parent got wrong result: %+v", task)
			}
			actualParentCalls.Add(1)
			if _, err := db.ExecContext(ctx, "INSERT INTO wake_receipt (sid, message) VALUES (?, ?)", task.ID, task.Result.Content); err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(parentOut, schema.AssistantMessage("received "+task.Result.Content))}, nil
		}).AddEdge(compose_v2.StartNode, "delegate").AddEdge("delegate", compose_v2.EndNode))
	if err != nil {
		t.Fatal(err)
	}
	worker := func(_ context.Context, _ string) (agent_v2.Agent, error) {
		out := compose_v2.LastValue[*schema.Message]("child_result")
		in := compose_v2.LastValue[*schema.Message]("child_input")
		b := compose_v2.NewBuilder().WithName("lookup")
		compose_v2.WithChannel(b, in)
		compose_v2.WithChannel(b, out)
		graph, err := b.AddNode("lookup", func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			msg, _ := compose_v2.Get(ch, in) // 图入口写入输入。
			if msg == nil || msg.Content != "lookup" {
				return nil, errors.New("child input was lost")
			}
			actualChildCalls.Add(1)
			return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage("asset ready"))}, nil
		}).AddEdge(compose_v2.StartNode, "lookup").AddEdge("lookup", compose_v2.EndNode).Compile()
		if err != nil {
			return nil, err
		}
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("lookup"),
			agent_v2.WithGraphInputWriter(func(msg *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(in, msg)}
			}), agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
				msg, _ := compose_v2.Get(ch, out) // 成功节点写入结果。
				return msg
			})), nil
	}
	notify := func(ctx context.Context, child background.Task) error {
		if background.ParentFromContext(ctx).ThreadID != scope.ThreadID {
			return errors.New("parent notification lost thread scope")
		}
		var message string
		err := db.QueryRowContext(ctx, "SELECT message FROM wake_receipt WHERE sid = ?", child.ID).Scan(&message)
		if err == nil {
			if message != "asset ready" {
				return fmt.Errorf("unexpected delivery receipt %q", message)
			}
			return nil
		}
		if !errors.Is(err, dbsql.ErrNoRows) {
			return err
		}
		checkpoint, err := cp.LoadNS(ctx, child.Parent.TaskID, "")
		if err != nil {
			return err
		}
		if checkpoint == nil {
			return early
		}
		for _, question := range checkpoint.PendingInterrupts {
			if question.Reason == background.CompletionReason(child.ID) {
				events, err := parent.Resume(ctx, child.Parent.TaskID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
					question.InterruptID: schema.UserMessage(child.ID),
				}))
				if err != nil {
					return err
				}
				final := agent_v2.FinalMessage(events)
				if final == nil || final.Content != "received asset ready" {
					return fmt.Errorf("unexpected final parent message: %+v", final)
				}
				return io.ErrUnexpectedEOF // 父任务已执行，但通知确认丢失。
			}
		}
		return early
	}
	cfg := background.Config{Workers: map[string]background.Factory{"lookup@v1": worker}, Notify: notify}
	jobs = backgroundManager(t, st, cfg)
	task, err := jobs.Submit(ctx, scope, "look-up", "lookup@v1", schema.UserMessage("lookup"))
	if err != nil {
		t.Fatal(err)
	}
	if err := jobs.RunOnce(ctx); !errors.Is(err, early) {
		t.Fatalf("early notification should remain available: %v", err)
	}
	sess := agent_v2.NewRunSession()
	sess.RunID = scope.TaskID
	_, err = parent.Run(ctx, sess, schema.UserMessage("delegate now"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("parent did not wait on the durable completion: %v", err)
	}
	jobs = backgroundManager(t, st, cfg)
	if err := jobs.RunOnce(ctx); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("parent should finish even if notification ACK is lost: %v", err)
	}
	if err := jobs.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := jobs.Get(ctx, scope, task.ID)
	if err != nil || !result.Notified || actualParentCalls.Load() != 1 || actualChildCalls.Load() != 1 {
		t.Fatalf("expected one dispatch and one parent delivery: %+v %v (%d/%d)", result, err,
			actualParentCalls.Load(), actualChildCalls.Load())
	}
	for _, id := range []string{scope.TaskID, task.ID} {
		if checkpoint, err := cp.LoadNS(ctx, id, ""); err != nil || checkpoint != nil {
			t.Fatalf("successful task %q still has a checkpoint: %v %v", id, checkpoint, err)
		}
	}
}

type backgroundWriteFault struct {
	*crewsql.BackgroundTaskStore
	status    background.Status
	committed bool
	fired     bool
}

func (s *backgroundWriteFault) UpdateBackgroundTask(ctx context.Context, task *background.Task, clear bool) (bool, error) {
	if task.Status == s.status && !s.fired {
		s.fired = true
		if s.committed {
			if ok, err := s.BackgroundTaskStore.UpdateBackgroundTask(ctx, task, clear); !ok || err != nil {
				return ok, err
			}
		}
		return false, io.ErrUnexpectedEOF
	}
	return s.BackgroundTaskStore.UpdateBackgroundTask(ctx, task, clear)
}

func TestSQLiteBackgroundTaskWriteRetry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    background.Status
		committed bool
	}{
		{"claim_ack_lost", background.Running, true},
		{"terminal_write_failed", background.Succeeded, false},
		{"terminal_ack_lost", background.Succeeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "retry.sqlite")
			store, peer := openBackgroundStore(t, crewsql.SQLite{}, func() *dbsql.DB { return openBackgroundSQLite(t, path) })
			fault := &backgroundWriteFault{BackgroundTaskStore: store, status: tc.status, committed: tc.committed}
			out := compose_v2.LastValue[*schema.Message]("result")
			var effects atomic.Int32
			agent, err := backgroundGraph("complete", out, compose_v2.NewBuilder().
				AddNode("prepare", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					return nil, nil
				}).AddNode("effect", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				result := schema.AssistantMessage(fmt.Sprintf("executed %d", effects.Add(1)))
				return []compose_v2.ChannelWrite{compose_v2.Write(out, result)}, nil
			}).AddEdge(compose_v2.StartNode, "prepare").AddEdge("prepare", "effect").AddEdge("effect", compose_v2.EndNode))
			if err != nil {
				t.Fatal(err)
			}
			cfg := background.Config{Workers: map[string]background.Factory{
				"write@v1": func(context.Context, string) (agent_v2.Agent, error) { return agent, nil },
			}}
			a, b := backgroundManager(t, fault, cfg), backgroundManager(t, peer, cfg)
			parent := background.Parent{Owner: "operator", TaskID: "parent"}
			task, err := a.Submit(ctx, parent, "request", "write@v1", schema.UserMessage("perform effect"))
			if err != nil {
				t.Fatal(err)
			}
			if err := a.RunOnce(ctx); err != nil {
				t.Errorf("first worker did not finish persisting the known result: %v", err)
			}
			if err := b.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			done, err := b.Get(ctx, parent, task.ID)
			if err != nil || done.Status != background.Succeeded || done.Result == nil ||
				done.Result.Content != "executed 1" || !fault.fired || effects.Load() != 1 {
				t.Fatalf("known outcome was lost or repeated: %+v %v effects=%d", done, err, effects.Load())
			}
		})
	}
}

func TestSQLiteBackgroundTaskSurvivesHardProcessExit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.sqlite")
	db := openBackgroundSQLite(t, path)
	defer db.Close() // 测试结束释放连接。
	ctx, stop := context.WithTimeout(t.Context(), 15*time.Second)
	defer stop()
	cp := crewsql.NewCheckpointer(db, crewsql.SQLite{})
	st := crewsql.NewBackgroundTaskStore(cp)
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE transition_count (value INTEGER NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	const workerName = "restart@v1"
	scope := background.Parent{Owner: "operator", TaskID: "parent"}
	m := backgroundManager(t, st, background.Config{})
	task, err := m.Submit(ctx, scope, "restart", workerName, schema.UserMessage("run"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBackgroundTaskCrashProcess$")
	cmd.Env = append(os.Environ(), "CREW_BG_RESTART_SQLITE="+path)
	var exit *exec.ExitError
	if out, err := cmd.CombinedOutput(); !errors.As(err, &exit) || exit.ExitCode() != 43 {
		t.Fatalf("first worker did not stop at the persisted boundary: %v %s", err, out)
	}

	checkpoint, err := cp.LoadNS(ctx, task.ID, "")
	if err != nil || checkpoint == nil || len(checkpoint.CurrentFrontier) != 1 || checkpoint.CurrentFrontier[0].NodeID != "verify" {
		t.Fatalf("first worker did not persist its progress: %+v %v", checkpoint, err)
	}
	replacement := backgroundManager(t, st, background.Config{Workers: map[string]background.Factory{
		workerName: func(_ context.Context, _ string) (agent_v2.Agent, error) { return restartAgent(db, false) },
	}})
	if err := replacement.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// 模拟等到已退出的进程租约过期，测试无需等待真实 TTL。
	res, err := db.ExecContext(ctx, "UPDATE "+crewsql.DefaultCheckpointTable+"_task_lease SET expires_at = 0 WHERE sid = ?", task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("expected the exited worker's lease to persist: %d %v", n, err)
	}
	if err := replacement.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := replacement.Get(ctx, scope, task.ID)
	if err != nil || result.Status != background.Succeeded || result.Result.Content != "prior output restored" {
		t.Fatalf("replacement did not recover prior progress: %+v %v", result, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transition_count").Scan(&count); err != nil || count != 1 {
		t.Fatalf("completed node was rerun: count=%d %v", count, err)
	}
}

func TestBackgroundTaskCrashProcess(t *testing.T) {
	path := os.Getenv("CREW_BG_RESTART_SQLITE")
	if path == "" {
		t.Skip("由跨进程恢复测试启动")
	}
	db := openBackgroundSQLite(t, path)
	defer db.Close() // 未触发模拟退出时释放连接。
	st := crewsql.NewBackgroundTaskStore(crewsql.NewCheckpointer(db, crewsql.SQLite{}))
	m := backgroundManager(t, st, background.Config{Workers: map[string]background.Factory{
		"restart@v1": func(_ context.Context, _ string) (agent_v2.Agent, error) { return restartAgent(db, true) },
	}})
	if err := m.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("worker returned without simulated process exit")
}

func restartAgent(db *dbsql.DB, crash bool) (agent_v2.Agent, error) {
	prior := compose_v2.LastValue[string]("persisted_progress")
	out := compose_v2.LastValue[*schema.Message]("result")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, prior)
	b.AddNode("prepare", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := db.ExecContext(ctx, "INSERT INTO transition_count (value) VALUES (1)"); err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(prior, "prepared")}, nil
	}).AddNode("verify", func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if value, ok := compose_v2.Get(ch, prior); !ok || value != "prepared" {
			return nil, errors.New("lost the completed node's output")
		}
		if crash {
			os.Exit(43)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage("prior output restored"))}, nil
	}).AddEdge(compose_v2.StartNode, "prepare").AddEdge("prepare", "verify").AddEdge("verify", compose_v2.EndNode)
	return backgroundGraph("restart", out, b)
}
