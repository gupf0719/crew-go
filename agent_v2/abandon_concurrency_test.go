package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

type pausedTaskDeletion struct {
	compose_v2.Checkpointer
	task             string
	entered, release chan struct{}
	once             sync.Once
	loads            atomic.Int32
}

func (p *pausedTaskDeletion) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	if sid == p.task {
		p.loads.Add(1)
	}
	return p.Checkpointer.LoadNS(ctx, sid, ns)
}

func (p *pausedTaskDeletion) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if sid == p.task {
		p.once.Do(func() { close(p.entered) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return p.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

type pausedResumeTools struct {
	agent_v2.NoOpOrchestrationHooks
	entered, release chan struct{}
}

func (h pausedResumeTools) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if nodeID == "tools" {
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx, ctx.Err()
		}
	}
	return ctx, nil
}

func TestWithAbandonedTasks_BlocksResumeDuringDeletion(t *testing.T) {
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	saved, err := f.cp.LoadNS(ctx, old, "")
	if err != nil || saved == nil || len(saved.PendingInterrupts) != 1 {
		t.Fatalf("应有一项挂起的审批，实际 cp=%v err=%v", saved, err)
	}
	cp := &pausedTaskDeletion{Checkpointer: f.cp, task: old, entered: make(chan struct{}), release: make(chan struct{})}
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	abandoned := make(chan error, 1)
	go func() {
		_, err := f.newAgent().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old))
		abandoned <- err
	}()
	select {
	case <-cp.entered:
	case <-ctx.Done():
		t.Fatal("放弃操作未进入删档")
	}

	tools := pausedResumeTools{entered: make(chan struct{}), release: make(chan struct{})}
	resumed := make(chan error, 1)
	go func() {
		answers := map[string]*schema.Message{saved.PendingInterrupts[0].InterruptID: schema.UserMessage("approve")}
		_, err := f.newAgent().Resume(ctx, old, agent_v2.WithResumeSession(f.pending),
			agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeOrchHooks(tools))
		resumed <- err
	}()
	var resumeErr error
	loaded := false
	select {
	case resumeErr = <-resumed:
	case <-tools.entered:
		loaded = true
	case <-ctx.Done():
		t.Fatal("续跑应被拒或进入工具节点")
	}
	close(cp.release)
	select {
	case err := <-abandoned:
		if err != nil {
			t.Errorf("新任务应照常完成，实际 %v", err)
		}
	case <-ctx.Done():
		t.Fatal("放弃操作未完成")
	}
	close(tools.release)
	if loaded {
		select {
		case resumeErr = <-resumed:
		case <-ctx.Done():
			t.Fatal("续跑未完成")
		}
	}
	var results []string
	for _, m := range f.history(t) {
		if schema.GetToolResultID(m) == "c1" && m.Annotations[agent_v2.HistoryTaskAnnotation] == old {
			results = append(results, m.Content)
		}
	}
	if loaded || !errors.Is(resumeErr, compose_v2.ErrTaskBusy) || cp.loads.Load() != 0 ||
		f.executed.Load() != 0 || len(results) != 1 || !strings.Contains(results[0], "放弃") {
		t.Fatalf("删档期间应在读档前拒绝续跑，且历史只补一条放弃结果；实际 err=%v 读档=%d 执行=%d 结果=%q",
			resumeErr, cp.loads.Load(), f.executed.Load(), results)
	}
	if _, err := f.newAgent().Resume(ctx, old); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("放弃完成后应报告没有可续存档，实际 %v", err)
	}
}

type pausedResumeLoad struct {
	compose_v2.Checkpointer
	task             string
	entered, release chan struct{}
	once             sync.Once
}

func (p *pausedResumeLoad) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	first := false
	if sid == p.task {
		p.once.Do(func() { first = true })
	}
	if first {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.Checkpointer.LoadNS(ctx, sid, ns)
}

func TestResume_PreflightPreventsConcurrentAbandon(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop func(context.Context, *abandonFixture, string) error
	}{
		{"WithAbandonedTasks", func(ctx context.Context, f *abandonFixture, sid string) error {
			_, err := f.newAgent().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(sid))
			return err
		}},
		{"ResumeActionAbort", func(ctx context.Context, f *abandonFixture, sid string) error {
			_, err := f.newAgent().Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAbandonFixture(t, nil)
			old := f.pending.RunID
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			saved, err := f.cp.LoadNS(ctx, old, "")
			if err != nil || saved == nil || len(saved.PendingInterrupts) != 1 {
				t.Fatalf("应有一项挂起的审批，实际 cp=%v err=%v", saved, err)
			}
			cp := &pausedResumeLoad{Checkpointer: f.cp, task: old, entered: make(chan struct{}), release: make(chan struct{})}
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			resumed := make(chan error, 1)
			go func() {
				answers := map[string]*schema.Message{saved.PendingInterrupts[0].InterruptID: schema.UserMessage("approve")}
				_, err := f.newAgent().Resume(ctx, old, agent_v2.WithResumeSession(f.pending), agent_v2.WithResumeUserInputs(answers))
				resumed <- err
			}()
			select {
			case <-cp.entered:
			case <-ctx.Done():
				t.Fatal("续跑未进入预读存档")
			}
			if err := tc.drop(ctx, f, old); !errors.Is(err, compose_v2.ErrTaskBusy) {
				t.Errorf("预读存档期间应拒绝放弃，实际 %v", err)
			}
			if pending, err := f.cp.ListPending(ctx, old); err != nil || len(pending) != 1 {
				t.Errorf("放弃被拒后存档应仍可续，实际 %v err=%v", pending, err)
			}
			close(cp.release)
			select {
			case err := <-resumed:
				if err != nil {
					t.Fatalf("续跑应完成，实际 %v", err)
				}
			case <-ctx.Done():
				t.Fatal("续跑未完成")
			}
			var results []string
			for _, m := range f.history(t) {
				if schema.GetToolResultID(m) == "c1" && m.Annotations[agent_v2.HistoryTaskAnnotation] == old {
					results = append(results, m.Content)
				}
			}
			if f.executed.Load() != 1 || len(results) != 1 || results[0] != "已转账" {
				t.Fatalf("工具应执行一次且历史只记真实结果，实际执行=%d 结果=%q", f.executed.Load(), results)
			}
		})
	}
}

func TestResumeAbort_SubtreeLeavesOtherCheckpointHistoryOpen(t *testing.T) {
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	ctx := f.ctx
	saved, err := f.cp.LoadNS(ctx, old, "")
	if err != nil || saved == nil || len(saved.PendingInterrupts) != 1 {
		t.Fatalf("应有一项挂起的审批，实际 cp=%v err=%v", saved, err)
	}
	const dropped = "abandoned-subtree"
	if err := f.cp.SaveNS(ctx, old, dropped, saved); err != nil {
		t.Fatal(err)
	}
	if _, err := f.newAgent().Resume(compose_v2.WithGraphNS(ctx, dropped), old, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		t.Fatalf("局部放弃应成功，实际 %v", err)
	}
	if got, err := f.cp.LoadNS(ctx, old, dropped); err != nil || got != nil {
		t.Fatalf("指定子树应已删除，实际 cp=%v err=%v", got, err)
	}
	answers := map[string]*schema.Message{saved.PendingInterrupts[0].InterruptID: schema.UserMessage("approve")}
	if _, err := f.newAgent().Resume(ctx, old, agent_v2.WithResumeSession(f.pending), agent_v2.WithResumeUserInputs(answers)); err != nil {
		t.Fatalf("根任务仍应能续跑，实际 %v", err)
	}
	var results []string
	for _, m := range f.history(t) {
		if schema.GetToolResultID(m) == "c1" && m.Annotations[agent_v2.HistoryTaskAnnotation] == old {
			results = append(results, m.Content)
		}
	}
	if f.executed.Load() != 1 || len(results) != 1 || results[0] != "已转账" {
		t.Fatalf("根任务的历史应只保留一次真实执行，实际执行=%d 结果=%q", f.executed.Load(), results)
	}
}
