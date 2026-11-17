package agent_v2

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func approvalRetirementGraph(t *testing.T, name string, fn compose_v2.NodeFunc) *GraphAgent {
	t.Helper()
	g, err := compose_v2.NewBuilder().WithName(name).
		AddNode("work", fn).AddEdge(compose_v2.StartNode, "work").AddEdge("work", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	return NewGraphAgent(g, WithGraphAgentName(name),
		WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("完成") }))
}

func TestTaskApprovalRulesRetiredAfterRootResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer()), 5*time.Second)
	defer cancel()
	session := NewRunSession()
	old := NewSharedChildSession(session)
	key := approvalRulesKey(old.RunID)
	call := schema.ToolCall{Function: schema.FunctionCall{Name: "write", Arguments: "{}"}}
	cfg := &Config{approvalOwner: "test-agent"}
	member := approvalRetirementGraph(t, "approval-member", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		markToolApproved(GetSession(ctx), cfg, call, "", false)
		return nil, nil
	})
	gate := approvalRetirementGraph(t, "approval-gate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !isToolApproved(GetSession(ctx), cfg, call, "") {
			return nil, errors.New("任务内的审批规则提前被回收")
		}
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "任务继续？"})
		return nil, err
	})
	ag := NewSequentialAgent(WithSequentialName("approval-owner")).AddStep("member", member).AddStep("gate", gate)
	_, err := ag.Run(ctx, session, schema.UserMessage("开始"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 || !isToolApproved(old, cfg, call, "") {
		t.Fatalf("子图成功后，挂起的父任务仍需保留审批规则：%v", err)
	}

	session.NextTask()
	markToolApproved(session, cfg, call, "", false)
	answer := WithResumeUserInputs(map[string]*schema.Message{pause.AllQueries()[0].InterruptID: schema.UserMessage("继续")})
	_, err = ag.Resume(ctx, old.RunID, WithResumeSession(old), answer)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := session.GetValue(key); found {
		t.Fatal("根任务结算后应删除旧 RunID 的审批键")
	}
	if !isToolApproved(session, cfg, call, "") {
		t.Fatal("不能删除同一 KV 中其他任务的规则")
	}
}

type approvalDeleteFault struct {
	compose_v2.Checkpointer
	fail bool
}

func (c *approvalDeleteFault) DeleteSubtree(ctx context.Context, sid, ns string, cond ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	if c.fail {
		return 0, errors.New("清档失败")
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns, cond...)
}

func TestTaskApprovalRulesRetiredOnlyAfterConfirmedAbandonment(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "abort"
		if replace {
			name = "replace"
		}
		t.Run(name, func(t *testing.T) {
			testTaskApprovalAbandonment(t, replace)
		})
	}
}

func testTaskApprovalAbandonment(t *testing.T, replace bool) {
	t.Helper()
	store := &approvalDeleteFault{Checkpointer: compose_v2.NewInMemoryCheckpointer(), fail: true}
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), store), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithGraphNS(ctx, "tenant/tasks")
	session := NewRunSession()
	call := schema.ToolCall{Function: schema.FunctionCall{Name: "write", Arguments: "{}"}}
	cfg := &Config{approvalOwner: "test-agent"}
	ag := approvalRetirementGraph(t, "approval-abandon", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		markToolApproved(GetSession(ctx), cfg, call, "", false)
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "任务继续？"})
		return nil, err
	})
	if _, err := ag.Run(ctx, session, schema.UserMessage("开始")); err == nil {
		t.Fatal("任务应该挂起")
	}
	abort := func(ctx context.Context) error {
		_, err := ag.Resume(ctx, session.RunID, WithResumeSession(session), WithResumeAction(ResumeActionAbort))
		return err
	}
	otherPartition := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	if err := abort(otherPartition); err != nil || !isToolApproved(session, cfg, call, "") {
		t.Fatalf("放弃另一分区的同名槽时应保留原任务审批：%v", err)
	}
	drop := abort
	if replace {
		next := approvalRetirementGraph(t, "replacement", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		drop = func(ctx context.Context) error {
			replacement := NewSharedChildSession(session)
			replacement.NextTask()
			_, err := next.Run(ctx, replacement, schema.UserMessage("新任务"), WithAbandonedTasks(session.RunID))
			return err
		}
	}
	err := drop(ctx)
	if (err == nil) != replace || !isToolApproved(session, cfg, call, "") {
		t.Fatalf("放弃失败时，原任务仍需保留审批：%v", err)
	}
	store.fail = false
	if err := drop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found := session.GetValue(approvalRulesKey(session.RunID)); found {
		t.Fatal("确认放弃后应删除审批键")
	}
}

func TestTaskApprovalRulesRootResumeWaitsForActiveChild(t *testing.T) {
	rootStore := compose_v2.NewInMemoryCheckpointer()
	childStore := compose_v2.NewInMemoryCheckpointer()
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), rootStore), 5*time.Second)
	defer cancel()
	session := NewRunSession()
	call := schema.ToolCall{Function: schema.FunctionCall{Name: "write", Arguments: "{}"}}
	cfg := &Config{approvalOwner: "test-agent"}
	entered, release := make(chan struct{}), make(chan struct{})
	child := approvalRetirementGraph(t, "approval-child", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "子任务继续？"})
		if err != nil {
			return nil, err
		}
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			if !isToolApproved(GetSession(ctx), cfg, call, "") {
				return nil, errors.New("在途子图丢失审批规则")
			}
			return nil, nil
		}
	})
	root := approvalRetirementGraph(t, "approval-root", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if action, _ := GetResumePhase(ctx); action == ResumeActionReplan {
			return nil, nil
		}
		markToolApproved(GetSession(ctx), cfg, call, "", false)
		_, err := RunSubAgent(ctx, "child", child, schema.UserMessage("子任务"))
		return nil, err
	})
	_, err := root.Run(ctx, session, schema.UserMessage("任务"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 {
		t.Fatalf("首次 Run 需留下一项子图中断：%v", err)
	}
	slots, err := rootStore.LoadAllInSession(ctx, session.RunID)
	if err != nil || len(slots) != 2 {
		t.Fatalf("应有父子两个存档：%v (%v)", slots, err)
	}
	var childNS string
	for _, slot := range slots {
		if slot.NS != "" {
			childNS = slot.NS
			if err := childStore.SaveNS(ctx, session.RunID, childNS, slot.Checkpoint); err != nil {
				t.Fatal(err)
			}
		}
	}
	if childNS == "" {
		t.Fatal("未找到子图存档")
	}
	childCtx := compose_v2.WithGraphNS(compose_v2.WithCheckpointerCtx(ctx, childStore), childNS)
	childSession := NewSharedChildSession(session)
	answer := WithResumeUserInputs(map[string]*schema.Message{pause.AllQueries()[0].InterruptID: schema.UserMessage("继续")})
	done := make(chan struct{})
	var childErr error
	go func() {
		defer close(done)
		_, childErr = child.Resume(childCtx, childSession.RunID, WithResumeSession(childSession), answer)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-entered:
	case <-done:
		t.Fatalf("子图尚未进入恢复节点就退出：%v", childErr)
	case <-ctx.Done():
		t.Fatal("等不到子图进入恢复节点")
	}
	replan := func() error {
		_, err := root.Resume(ctx, session.RunID, WithResumeSession(session), WithResumeAction(ResumeActionReplan))
		return err
	}
	if err := replan(); !errors.Is(err, compose_v2.ErrTaskBusy) {
		t.Errorf("子图仍在执行时，根任务续跑应等许可：%v", err)
	}
	next := NewSharedChildSession(session)
	next.NextTask()
	if _, err := root.Run(ctx, next, schema.UserMessage("新任务"), WithAbandonedTasks(session.RunID)); !errors.Is(err, compose_v2.ErrTaskBusy) {
		t.Errorf("子图仍在执行时，新任务不能放弃旧任务：%v", err)
	}
	close(release)
	select {
	case <-done:
		if childErr != nil {
			t.Fatal(childErr)
		}
	case <-ctx.Done():
		t.Fatal("子图续跑未完成")
	}
	if !isToolApproved(session, cfg, call, "") {
		t.Fatal("独立子图结算后应保留根任务审批")
	}
	if err := replan(); err != nil {
		t.Fatal(err)
	}
	if _, found := session.GetValue(approvalRulesKey(session.RunID)); found {
		t.Fatal("根任务结算后应删除审批键")
	}
}

type failingApprovalGraphStart struct {
	compose_v2.NoOpOrchestrationHooks
	err error
}

func (hooks failingApprovalGraphStart) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return ctx, hooks.err
}

func TestTaskApprovalRulesDoNotAllocateForGraphStartFailure(t *testing.T) {
	store := compose_v2.NewInMemoryCheckpointer()
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), store), 5*time.Second)
	defer cancel()
	wantErr := errors.New("开始钩子失败")
	ctx = compose_v2.WithOrchHooks(ctx, failingApprovalGraphStart{err: wantErr})
	ctx = compose_v2.WithGraphNS(ctx, "tenant/failed-start")
	session := NewRunSession()
	ag := approvalRetirementGraph(t, "approval-start-failure", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		t.Error("开始钩子已拒绝，节点不能执行")
		return nil, nil
	})
	for range 3 {
		id := session.RunID
		_, err := ag.Run(ctx, session, schema.UserMessage("请求"))
		if !errors.Is(err, wantErr) {
			t.Fatalf("应返回开始钩子的错误：%v", err)
		}
		if cp, err := store.LoadNS(ctx, id, "tenant/failed-start"); err != nil || cp != nil {
			t.Fatalf("开始钩子失败后应无存档：cp=%v err=%v", cp, err)
		}
		if _, found := session.GetValue(approvalRulesKey(id)); found {
			t.Fatal("没有审批或可续存档的任务不应在 session 中留下审批键")
		}
		session.NextTask()
	}
}

type approvalTaskRouter struct {
	compose_v2.Checkpointer
	oldID string
	old   compose_v2.Checkpointer
}

func (r approvalTaskRouter) ResolveCheckpointPartition(ctx context.Context) (compose_v2.Checkpointer, error) {
	return r.partition(compose_v2.GetSessionID(ctx)), nil
}

func (r approvalTaskRouter) DeleteSubtree(ctx context.Context, sid, ns string, cond ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	return r.partition(sid).DeleteSubtree(ctx, sid, ns, cond...)
}

func (r approvalTaskRouter) partition(sid string) compose_v2.Checkpointer {
	if sid == r.oldID {
		return r.old
	}
	return r.Checkpointer
}

func TestTaskApprovalRulesRetiredAfterRoutedAbandonment(t *testing.T) {
	old := NewRunSession()
	store := approvalTaskRouter{Checkpointer: compose_v2.NewInMemoryCheckpointer(),
		oldID: old.RunID, old: compose_v2.NewInMemoryCheckpointer()}
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), store), 5*time.Second)
	defer cancel()
	call := schema.ToolCall{Function: schema.FunctionCall{Name: "write", Arguments: "{}"}}
	cfg := &Config{approvalOwner: "test-agent"}
	waiting := approvalRetirementGraph(t, "waiting", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		markToolApproved(GetSession(ctx), cfg, call, "", false)
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
		return nil, err
	})
	if _, err := waiting.Run(ctx, old, schema.UserMessage("旧任务")); !compose_v2.LeavesResumableCheckpoint(err) {
		t.Fatalf("旧任务应留下挂起存档：%v", err)
	}
	if cp, err := store.old.LoadNS(ctx, old.RunID, ""); err != nil || cp == nil || !isToolApproved(old, cfg, call, "") {
		t.Fatalf("旧分区应持有存档且规则已写入：cp=%v err=%v", cp, err)
	}
	next := NewSharedChildSession(old)
	next.NextTask()
	replacement := approvalRetirementGraph(t, "replacement", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	if _, err := replacement.Run(ctx, next, schema.UserMessage("新任务"), WithAbandonedTasks(old.RunID)); err != nil {
		t.Fatal(err)
	}
	if cp, err := store.old.LoadNS(ctx, old.RunID, ""); err != nil || cp != nil {
		t.Fatalf("应按旧 RunID 清理旧分区：cp=%v err=%v", cp, err)
	}
	if _, found := old.GetValue(approvalRulesKey(old.RunID)); found {
		t.Fatal("确认旧分区清档后应回收规则")
	}
}
