package sql_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	crewsql "crew-go/sql"
)

type leaseGraphStart struct {
	compose_v2.NoOpOrchestrationHooks
	start func(context.Context) (context.Context, error)
}

func (h leaseGraphStart) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return h.start(ctx)
}

func leaseTestGraph(t *testing.T, name string, fn compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("work", fn)
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestSQLiteCheckpointer_ForeignHookCannotReplaceLease(t *testing.T) {
	ctx := t.Context()
	db := newSQLiteDB(t)
	cp := crewsql.NewCheckpointer(db, crewsql.SQLite{})
	if err := cp.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	cachedCtx, cached, err := compose_v2.BeginTaskActivity(ctx, "other-task", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Release()
	const sid, ns, token = "expired-task", "successor", "successor-owner"
	store := cp.CheckpointTaskLeases()
	hooks := leaseGraphStart{start: func(oldCtx context.Context) (context.Context, error) {
		if _, err := db.ExecContext(oldCtx, "UPDATE crew_checkpoint_task_lease SET expires_at = 0 WHERE sid = ?", sid); err != nil {
			return nil, err
		}
		ok, err := store.TryAcquireTaskLease(ctx, sid, token, time.Minute)
		if err != nil || !ok {
			t.Fatalf("接管者应取得租约，实际 ok=%t err=%v", ok, err)
		}
		if err := cp.SaveNS(ctx, sid, ns, &compose_v2.Checkpoint{CreatedAt: 2}); err != nil {
			return nil, err
		}
		return cachedCtx, nil
	}}
	g := leaseTestGraph(t, "foreign-hook", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	err = compose_v2.Run(compose_v2.WithOrchHooks(ctx, hooks), g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid))
	if !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
		t.Fatalf("使用旧租约的出口应被拒绝，实际 %v", err)
	}
	if cp, err := cp.LoadNS(ctx, sid, ns); err != nil || cp == nil || cp.CreatedAt != 2 {
		t.Fatalf("应保留接管者存档，实际 cp=%v err=%v", cp, err)
	}
}

type observedSubgraphCheckpointer struct {
	*crewsql.Checkpointer
	t            *testing.T
	loads, drops int
}

func (c *observedSubgraphCheckpointer) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	c.loads++
	c.requireLease(ctx, sid)
	return c.Checkpointer.LoadNS(ctx, sid, ns)
}

func (c *observedSubgraphCheckpointer) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.drops++
	c.requireLease(ctx, sid)
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

func (c *observedSubgraphCheckpointer) requireLease(ctx context.Context, sid string) {
	c.t.Helper()
	if compose_v2.CheckpointTaskLeaseToken(ctx, c.CheckpointTaskLeases(), sid) == "" {
		c.t.Error("子图的存档操作应携带其存储域的租约")
	}
	if ok, err := c.CheckpointTaskLeases().TryAcquireTaskLease(ctx, sid, "other-worker", time.Minute); err != nil || ok {
		c.t.Errorf("预读和清理期间另一进程不能接管，实际 ok=%t err=%v", ok, err)
	}
}

func TestSQLiteCheckpointer_SubgraphLeasesItsOwnBackend(t *testing.T) {
	ctx := t.Context()
	db := newSQLiteDB(t)
	root := crewsql.NewCheckpointer(db, crewsql.SQLite{}, crewsql.WithCheckpointerTable("root_cp"))
	child := crewsql.NewCheckpointer(db, crewsql.SQLite{}, crewsql.WithCheckpointerTable("child_cp"))
	for _, cp := range []*crewsql.Checkpointer{root, child} {
		if err := cp.Setup(ctx); err != nil {
			t.Fatal(err)
		}
	}
	observed := &observedSubgraphCheckpointer{Checkpointer: child, t: t}
	hooks := leaseGraphStart{start: func(ctx context.Context) (context.Context, error) {
		return compose_v2.WithCheckpointerCtx(ctx, observed), nil
	}}
	childRuns := 0
	sub := leaseTestGraph(t, "subgraph", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		childRuns++
		return nil, nil
	})
	b := compose_v2.NewBuilder().WithName("parent")
	b.AddSubgraphNode("child", sub)
	b.AddEdge(compose_v2.StartNode, "child")
	b.AddEdge("child", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx = compose_v2.WithOrchHooks(compose_v2.WithCheckpointerCtx(ctx, root), hooks)
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID("shared-subgraph"))
	if err != nil || childRuns != 1 || observed.loads == 0 || observed.drops == 0 {
		t.Fatalf("子图应读档、执行并清理，实际 err=%v runs=%d loads=%d drops=%d", err, childRuns, observed.loads, observed.drops)
	}
}

func TestSQLiteCheckpointer_NestedLeaseRejectsDifferentOwner(t *testing.T) {
	ctx := t.Context()
	db := newSQLiteDB(t)
	root := crewsql.NewCheckpointer(db, crewsql.SQLite{}, crewsql.WithCheckpointerTable("root_lease_cp"))
	other := crewsql.NewCheckpointer(db, crewsql.SQLite{}, crewsql.WithCheckpointerTable("other_lease_cp"))
	for _, cp := range []*crewsql.Checkpointer{root, other} {
		if err := cp.Setup(ctx); err != nil {
			t.Fatal(err)
		}
	}
	const sid, remoteToken = "shared-sid", "other-owner"
	ownedCtx, activity, err := compose_v2.BeginTaskActivity(compose_v2.WithCheckpointerCtx(ctx, root), sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer activity.Release()

	remote := other.CheckpointTaskLeases()
	ok, err := remote.TryAcquireTaskLease(ctx, sid, remoteToken, time.Minute)
	if err != nil || !ok {
		t.Fatalf("另一存储域应能持有同名任务，实际 ok=%t err=%v", ok, err)
	}
	t.Cleanup(func() {
		if err := remote.ReleaseTaskLease(context.Background(), sid, remoteToken); err != nil {
			t.Error(err)
		}
	})

	_, nested, err := compose_v2.BeginTaskActivity(compose_v2.WithCheckpointerCtx(ownedCtx, other), sid, nil)
	if nested != nil {
		nested.Release()
	}
	if !errors.Is(err, compose_v2.ErrTaskBusy) {
		t.Fatalf("不同令牌的租约冲突应返回 ErrTaskBusy，实际 %v", err)
	}
	if renewed, err := remote.RenewTaskLease(ctx, sid, remoteToken, time.Minute); err != nil || !renewed {
		t.Fatalf("被拒的子调用应保留另一持有者的租约，实际 renewed=%t err=%v", renewed, err)
	}
}

func TestSQLiteCheckpointer_ExpiredParentCannotAcquireViaAlias(t *testing.T) {
	ctx := t.Context()
	db := newSQLiteDB(t)
	cp := crewsql.NewCheckpointer(db, crewsql.SQLite{})
	if err := cp.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	alias := crewsql.NewCheckpointer(db, crewsql.SQLite{},
		crewsql.WithCheckpointerTable(strings.ToUpper(crewsql.DefaultCheckpointTable)))
	const sid, ns = "expired-parent", "successor"
	current := compose_v2.WithCheckpointerCtx(ctx, cp)
	parent, activity, err := compose_v2.BeginTaskActivity(current, sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer activity.Release()

	if _, err := db.ExecContext(ctx, "UPDATE crew_checkpoint_task_lease SET expires_at = 0 WHERE sid = ?", sid); err != nil {
		t.Fatal(err)
	}
	newOwner, next, err := compose_v2.BeginTaskActivity(current, sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	if err := cp.SaveNS(newOwner, sid, ns, &compose_v2.Checkpoint{CreatedAt: 2}); err != nil {
		t.Fatal(err)
	}
	next.Release()
	if err := cp.SaveNS(parent, sid, ns, &compose_v2.Checkpoint{CreatedAt: 3}); !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
		t.Fatalf("存储已拒绝父调用的旧令牌，实际 %v", err)
	}

	nestedCtx, nested, err := compose_v2.BeginTaskActivity(compose_v2.WithCheckpointerCtx(parent, alias), sid, nil)
	if nested != nil {
		defer nested.Release()
		deleted, delErr := alias.DeleteSubtree(nestedCtx, sid, "")
		t.Fatalf("失租的父调用经别名获得了新租约，删档 count=%d err=%v", deleted, delErr)
	}
	if !errors.Is(err, compose_v2.ErrTaskLeaseLost) || !errors.Is(context.Cause(parent), compose_v2.ErrTaskLeaseLost) {
		t.Fatalf("存储确认失租后应拒绝子调用并通知父调用，实际 err=%v cause=%v", err, context.Cause(parent))
	}
	if got, err := cp.LoadNS(ctx, sid, ns); err != nil || got == nil || got.CreatedAt != 2 {
		t.Fatalf("接管者的存档应保留，实际 cp=%v err=%v", got, err)
	}
	_, fresh, err := compose_v2.BeginTaskActivity(current, sid, nil)
	if err != nil {
		t.Fatalf("被拒的子调用应归还刚取得的租约，实际 %v", err)
	}
	fresh.Release()
}
