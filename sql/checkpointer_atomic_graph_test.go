package sql_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

type insertBeforeCheckpointDelete struct {
	*crewsql.Checkpointer
	insert func(context.Context, string) error
}

func (c *insertBeforeCheckpointDelete) DeleteSubtree(ctx context.Context, sid, ns string, conditions ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	if err := c.insert(ctx, sid); err != nil {
		return 0, err
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns, conditions...)
}

func TestSQLiteGraphAgentConditionalCleanupPreservesNewOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "checkpoint.db")
	first := openBackgroundSQLite(t, path)
	peer := openBackgroundSQLite(t, path)
	cp := crewsql.NewCheckpointer(first, crewsql.SQLite{})
	foreign := crewsql.NewCheckpointer(peer, crewsql.SQLite{})
	if err := cp.Setup(ctx); err != nil {
		t.Fatal(err)
	}

	writes := 0
	graph := leaseTestGraph(t, "cleanup-race", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准写入？"}); err != nil {
			return nil, err
		}
		writes++
		return nil, nil
	})
	agent := agent_v2.NewGraphAgent(graph,
		agent_v2.WithGraphAgentName("cleanup-race"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("完成") }),
	)
	session := agent_v2.NewRunSession()
	base := compose_v2.WithCheckpointerCtx(ctx, cp)
	_, err := agent.Run(base, session, schema.UserMessage("准备写入"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 || writes != 0 {
		t.Fatalf("首轮应挂在审批前，实际 %v writes=%d", err, writes)
	}
	const lateNS = "new-child"
	injected := false
	racing := &insertBeforeCheckpointDelete{Checkpointer: cp, insert: func(_ context.Context, sid string) error {
		injected = true
		// 独立客户端在引擎读出清单后放入另一份有业务归属的档。
		return foreign.SaveNS(ctx, sid, lateNS, &compose_v2.Checkpoint{
			Status: compose_v2.CPStatusPending, Epoch: "new-generation", TaskID: "another-task", GraphID: "another-graph",
			CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "work"}},
		})
	}}
	resume := compose_v2.WithCheckpointerCtx(ctx, racing)
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		pause.AllQueries()[0].InterruptID: schema.UserMessage("批准"),
	})
	_, err = agent.Resume(resume, session.RunID, answer)
	if !injected || !errors.Is(err, compose_v2.ErrCheckpointStale) || writes != 1 {
		t.Fatalf("候选集合变化应拒绝整棵清理，实际 err=%v writes=%d injected=%v", err, writes, injected)
	}
	root, err := foreign.LoadNS(ctx, session.RunID, "")
	if err != nil || root == nil || root.Status != compose_v2.CPStatusResuming {
		t.Fatalf("条件不满足时根档也要保留，实际 %v %v", root, err)
	}
	next, err := foreign.LoadNS(ctx, session.RunID, lateNS)
	if err != nil || next == nil || next.Epoch != "new-generation" {
		t.Fatalf("新所有者的档应保留，实际 %v %v", next, err)
	}
	if _, err := agent.Resume(base, session.RunID, answer); !errors.Is(err, compose_v2.ErrCheckpointResumeUnconfirmed) || writes != 1 {
		t.Fatalf("清理被拒后不能重新派发已执行的写入，实际 %v writes=%d", err, writes)
	}
}
