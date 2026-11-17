package compose_v2_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type checkpointScopeStore struct {
	compose_v2.Checkpointer
	partitions map[string]*compose_v2.InMemoryCheckpointer
	scope      func(context.Context) string
	saves      atomic.Int32
	deletes    atomic.Int32
}

func (s *checkpointScopeStore) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	s.saves.Add(1)
	return s.partitions[s.scope(ctx)].SaveNS(ctx, sid, ns, cp)
}

func (s *checkpointScopeStore) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	return s.partitions[s.scope(ctx)].LoadNS(ctx, sid, ns)
}

func (s *checkpointScopeStore) ExistsNS(ctx context.Context, sid, ns string) (bool, error) {
	return s.partitions[s.scope(ctx)].ExistsNS(ctx, sid, ns)
}

func (s *checkpointScopeStore) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	s.deletes.Add(1)
	return s.partitions[s.scope(ctx)].DeleteSubtree(ctx, sid, ns)
}

type checkpointScopeHook struct {
	compose_v2.NoOpOrchestrationHooks
	selectScope func(context.Context, string) context.Context
	starts      int
	ends        int
	endErr      error
}

func (h *checkpointScopeHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	h.starts++
	return h.selectScope(ctx, "B"), nil
}

func (h *checkpointScopeHook) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	h.ends++
	h.endErr = err
	return ctx
}

type checkpointPartitionKey struct{}

func setCheckpointPartition(ctx context.Context, scope string) context.Context {
	return context.WithValue(ctx, checkpointPartitionKey{}, scope)
}

func getCheckpointPartition(ctx context.Context) string {
	scope, _ := ctx.Value(checkpointPartitionKey{}).(string)
	return scope
}

func TestT503CheckpointScopeAtGraphStart(t *testing.T) {
	for _, tt := range []struct {
		name      string
		scope     func(context.Context) string
		selectAt  func(context.Context, string) context.Context
		explicit  bool
		allowLate bool
	}{
		{name: "reject/owner/auto", scope: compose_v2.GetOwner, selectAt: compose_v2.WithOwner},
		{name: "reject/owner/explicit", scope: compose_v2.GetOwner, selectAt: compose_v2.WithOwner, explicit: true},
		{name: "reject/custom_partition/auto", scope: getCheckpointPartition, selectAt: setCheckpointPartition},
		{name: "reject/custom_partition/explicit", scope: getCheckpointPartition, selectAt: setCheckpointPartition, explicit: true},
		{name: "allow/owner/late_slot", scope: compose_v2.GetOwner, selectAt: compose_v2.WithOwner, allowLate: true},
		{name: "allow/custom_partition/late_slot", scope: getCheckpointPartition, selectAt: setCheckpointPartition, allowLate: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testCheckpointHookScope(t, tt.scope, tt.selectAt, tt.explicit, tt.allowLate)
		})
	}
}

func testCheckpointHookScope(t *testing.T, scope func(context.Context) string, selectScope func(context.Context, string) context.Context, explicit, allowLate bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	partitions := map[string]*compose_v2.InMemoryCheckpointer{
		"A": compose_v2.NewInMemoryCheckpointer(),
		"B": compose_v2.NewInMemoryCheckpointer(),
	}
	cp := &checkpointScopeStore{Checkpointer: partitions["A"], partitions: partitions, scope: scope}
	tenant := compose_v2.LastValue[string]("tenant")
	var nodeCalls int
	var completed string
	builder := compose_v2.NewBuilder().WithName("persisted-workflow")
	compose_v2.WithChannel(builder, tenant)
	builder.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		nodeCalls++
		got, _ := compose_v2.Get(chs, tenant)
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续任务 " + got + "？"})
		if err != nil {
			return nil, err
		}
		completed = got + "/" + answer.Content
		return nil, nil
	}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	const sid = "same-run-id"
	run := func(partition string, opts ...compose_v2.RunOption) error {
		base := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
		return compose_v2.Run(selectScope(ctx, partition), graph, compose_v2.NewChannelManager(), append(base, opts...)...)
	}
	snapshots := make(map[string]*compose_v2.Checkpoint, len(partitions))
	before := make(map[string][]byte, len(partitions))
	answers := make(map[string]compose_v2.RunOption, len(partitions))
	for _, partition := range []string{"A", "B"} {
		if allowLate && partition == "A" {
			continue
		}
		err := run(partition, compose_v2.WithInitialWrites(compose_v2.Write(tenant, partition)))
		if _, ok := compose_v2.IsInterrupt(err); !ok {
			t.Fatalf("分区 %s 首跑应挂起，实际 %v", partition, err)
		}
		saved, wire := checkpointScopeSnapshot(t, ctx, partitions[partition], sid)
		if saved == nil || len(saved.PendingInterrupts) != 1 {
			t.Fatalf("分区 %s 应有一条存档问题，实际 %+v", partition, saved)
		}
		q := saved.PendingInterrupts[0]
		if q.InterruptID == "" || q.Question != "继续任务 "+partition+"？" {
			t.Fatalf("分区 %s 存档问题不匹配，实际 %+v", partition, q)
		}
		snapshots[partition], before[partition] = saved, wire
		answers[partition] = compose_v2.WithResumeAnswers(map[string]*schema.Message{
			q.InterruptID: schema.UserMessage("批准 " + partition),
		})
	}
	if !allowLate && (snapshots["A"].GraphID != snapshots["B"].GraphID ||
		snapshots["A"].PendingInterrupts[0].InterruptID == snapshots["B"].PendingInterrupts[0].InterruptID) {
		t.Fatal("测试需要同图、同 RunID、不同待答任务")
	}

	saves, deletes, calls := cp.saves.Load(), cp.deletes.Load(), nodeCalls
	hook := &checkpointScopeHook{selectScope: selectScope}
	options := []compose_v2.RunOption{compose_v2.WithHooks(hook), answers["B"]}
	if explicit {
		options = append(options, compose_v2.WithResumeFrom(snapshots["A"]))
	}
	err = run("A", options...)
	if allowLate {
		if err != nil || completed != "B/批准 B" || nodeCalls != calls+1 {
			t.Errorf("入口槽为空时应使用钩子选择的分区恢复原任务，完成=%q 节点=%d err=%v", completed, nodeCalls-calls, err)
		}
		if hook.starts != 1 || hook.ends != 1 || hook.endErr != nil || cp.saves.Load() != saves || cp.deletes.Load() != deletes+1 {
			t.Errorf("晚选有效分区应只调用配对钩子并清档一次，start=%d end=%d 写=%d 删=%d err=%v",
				hook.starts, hook.ends, cp.saves.Load()-saves, cp.deletes.Load()-deletes, hook.endErr)
		}
		if saved, _ := checkpointScopeSnapshot(t, ctx, partitions["B"], sid); saved != nil {
			t.Error("分区 B 完成后应清理存档")
		}
		return
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Errorf("切换存档分区应返回身份冲突，实际 %v", err)
	}
	if hook.starts != 1 || hook.ends != 1 || !errors.Is(hook.endErr, compose_v2.ErrGraphIDConflict) {
		t.Errorf("钩子应完成一次配对并看到冲突，start=%d end=%d err=%v", hook.starts, hook.ends, hook.endErr)
	}
	if nodeCalls != calls || completed != "" || cp.saves.Load() != saves || cp.deletes.Load() != deletes {
		t.Errorf("拒绝时应跳过执行、消费与写删，节点=%d 完成=%q 写=%d 删=%d",
			nodeCalls-calls, completed, cp.saves.Load()-saves, cp.deletes.Load()-deletes)
	}
	for _, partition := range []string{"A", "B"} {
		_, after := checkpointScopeSnapshot(t, ctx, partitions[partition], sid)
		if !bytes.Equal(before[partition], after) {
			t.Errorf("分区 %s 原档被改写", partition)
		}
	}
	if t.Failed() {
		return
	}
	for _, partition := range []string{"A", "B"} {
		if err := run(partition, answers[partition]); err != nil || completed != partition+"/批准 "+partition {
			t.Errorf("分区 %s 应能使用原答案继续，完成=%q err=%v", partition, completed, err)
		}
		if saved, _ := checkpointScopeSnapshot(t, ctx, partitions[partition], sid); saved != nil {
			t.Errorf("分区 %s 成功后应清理自己的档", partition)
		}
		if partition == "A" {
			_, after := checkpointScopeSnapshot(t, ctx, partitions["B"], sid)
			if !bytes.Equal(before["B"], after) {
				t.Error("分区 A 清档应保留分区 B 的原档")
			}
		}
	}
}

func checkpointScopeSnapshot(t *testing.T, ctx context.Context, store compose_v2.Checkpointer, sid string) (*compose_v2.Checkpoint, []byte) {
	t.Helper()
	saved, err := store.LoadNS(ctx, sid, "")
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil {
		return nil, nil
	}
	wire, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	return saved, wire
}

func TestCheckpointLowLevelRestartWithGraphStartContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	f := newKeyDurFixture(t, "restart-with-graph-context", askTail)
	cp := &topFailOnceCP{keyDurCP: newKeyDurCP("")}
	const sid = "restart-with-graph-context"
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
	err := compose_v2.Run(ctx, f.g, compose_v2.NewChannelManager(), opts...)
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !cp.failedDeleteTop.Load() || !cp.savedNS("cg/inner") {
		t.Fatalf("测试需要根写删失败后留下真实子档，实际 %v", err)
	}

	// 装饰上下文的钩子增加业务值，后端仍读写原来的分区。
	hooks := &checkpointScopeHook{selectScope: setCheckpointPartition}
	opts = append(opts, compose_v2.WithHooks(hooks))
	err = compose_v2.Run(ctx, f.g, compose_v2.NewChannelManager(), opts...)
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 || f.wRuns.Load() != 2 || hooks.starts != 1 {
		t.Fatalf("空根槽应从头执行并重新提问，实际 err=%v runs=%d hooks=%d", err, f.wRuns.Load(), hooks.starts)
	}
	answers := map[string]*schema.Message{interrupt.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	err = compose_v2.Run(ctx, f.g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
	if err != nil || f.last() != "took:billing" {
		t.Fatalf("重开任务应能用新问题续跑，实际 err=%v result=%q", err, f.last())
	}
}
