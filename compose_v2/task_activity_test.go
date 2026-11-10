package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

func TestRun_RejectsTaskBeingAbandoned(t *testing.T) {
	const sid = "abandoning-graph-task"
	ctx, activity, err := compose_v2.BeginTaskActivity(context.Background(), "", []string{sid})
	if err != nil {
		t.Fatal(err)
	}
	defer activity.Release()
	runs := 0
	b := compose_v2.NewBuilder().WithName("cleanup-conflict")
	b.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		runs++
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g := rsCompile(t, b)
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid)); !errors.Is(err, compose_v2.ErrTaskBusy) || runs != 0 {
		t.Fatalf("放弃期间图应在运行节点前被拒，实际 runs=%d err=%v", runs, err)
	}
	activity.FinishAbandon()
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid)); err != nil || runs != 1 {
		t.Fatalf("放弃许可释放后图应能正常运行，实际 runs=%d err=%v", runs, err)
	}
}

func TestBeginTaskActivity_AbandonConflictLeavesBatchUnclaimed(t *testing.T) {
	ctx := t.Context()
	const running, idle, next = "running-task", "idle-task", "next-task"
	_, active, err := compose_v2.BeginTaskActivity(ctx, running, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Release()
	_, rejected, err := compose_v2.BeginTaskActivity(ctx, next, []string{idle, running})
	if rejected != nil || !errors.Is(err, compose_v2.ErrTaskBusy) || compose_v2.TaskExecuting(next) {
		t.Fatalf("整批登记应被拒，实际 activity=%v err=%v", rejected, err)
	}
	_, allowed, err := compose_v2.BeginTaskActivity(ctx, idle, nil)
	if err != nil {
		t.Fatalf("批量放弃失败后空闲任务仍应可执行，实际 %v", err)
	}
	allowed.Release()
}
