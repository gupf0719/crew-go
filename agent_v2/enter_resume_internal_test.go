package agent_v2

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestEnterResumePreservesTaskApprovalAndAnswerIdentity(t *testing.T) {
	session := NewRunSession()
	other := NewRunSession()
	call := schema.ToolCall{Function: schema.FunctionCall{Name: "write", Arguments: "{}"}}
	cfg := &Config{approvalOwner: "custom"}
	markToolApproved(session, cfg, call, "", false)
	answer := schema.UserMessage("批准")
	answer.Annotations = map[string]any{"proof": []string{"source"}}
	answers := map[string]*schema.Message{"current": answer}
	ctx := compose_v2.WithOwner(WithSession(t.Context(), other), "owner")
	ctx = compose_v2.WithThreadIDCtx(ctx, "thread")
	ctx = compose_v2.WithResumeContextValue(ctx, &compose_v2.ResumeContext{
		UserInputs: map[string]*schema.Message{"stale": schema.UserMessage("其他任务")},
	})
	ctx = withNodeGrants(ctx, memberGrants{route: &routeGrant{}, handoff: &handoffGrant{}})

	entered, request, err := EnterResume(ctx, session.RunID, "custom", WithResumeSession(session),
		WithResumeAction(ResumeActionReplan), WithResumeUserInputs(answers), WithResumeUserText("改变方向"))
	if err != nil {
		t.Fatal(err)
	}
	if !isToolApproved(session, cfg, call, "") || isToolApproved(other, cfg, call, "") {
		t.Fatal("原任务的审批应保留，其他任务不应共享")
	}
	if request.Session != session || GetSession(entered) != session ||
		compose_v2.GetSessionID(entered) != session.RunID || compose_v2.IsFreshRunCtx(entered) {
		t.Fatalf("续跑身份不一致：session=%v sid=%q", request.Session == session, compose_v2.GetSessionID(entered))
	}
	grants, _ := entered.Value(nodeGrantsKey{}).(*memberGrants)
	if grants == nil || grants.route != nil || grants.handoff != nil {
		t.Fatal("续跑上下文携带了外层授予")
	}
	if action, ok := GetResumePhase(entered); !ok || action != ResumeActionReplan ||
		request.Action != action || request.UserText != "改变方向" {
		t.Fatalf("续跑动作或用户指示丢失：%+v", request)
	}
	if _, hasOldAnswers := compose_v2.GetResumeContext(entered); compose_v2.GetOwner(entered) != "owner" ||
		compose_v2.GetThreadID(entered) != "thread" || hasOldAnswers {
		t.Fatal("续跑应保留请求归属并隔离旧答复")
	}
	if len(request.UserInputs) != 1 || request.UserInputs["current"].Content != "批准" {
		t.Fatalf("答案寻址丢失：%v", request.UserInputs)
	}
	request.UserInputs["current"].Annotations["proof"].([]string)[0] = "modified"
	delete(request.UserInputs, "current")
	if answers["current"] != answer || answer.Annotations["proof"].([]string)[0] != "source" {
		t.Fatal("续跑处理改写了调用方的答案")
	}
}

func TestEnterResumeRejectsNestedAndDetachedSelfFromRawGraph(t *testing.T) {
	session := NewRunSession()
	other := NewRunSession()
	g, err := compose_v2.NewBuilder().
		AddNode("check", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			_, _, err := EnterResume(ctx, other.RunID, "custom", WithResumeAction(ResumeActionAbort))
			if !errors.Is(err, compose_v2.ErrRunAborted) {
				return nil, fmt.Errorf("图节点内的独立放弃应提前被拒，实际 %v", err)
			}
			detached := DetachTask(ctx)
			_, _, err = EnterResume(detached, session.RunID, "custom")
			if !errors.Is(err, compose_v2.ErrRunAborted) {
				return nil, fmt.Errorf("脱离后不能续跑正在执行的任务，实际 %v", err)
			}
			newCtx, req, err := EnterResume(detached, other.RunID, "custom")
			if err != nil {
				return nil, err
			}
			if req.Session == nil || req.Session.RunID != other.RunID || GetSession(newCtx) != req.Session ||
				compose_v2.GetThreadID(newCtx) != "" || compose_v2.GetGraphNS(newCtx) != "" ||
				compose_v2.GetOwner(newCtx) != "owner" {
				return nil, errors.New("独立续跑串用了父图的位置或会话")
			}
			return nil, nil
		}).AddEdge(compose_v2.StartNode, "check").AddEdge("check", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx := compose_v2.WithThreadIDCtx(compose_v2.WithOwner(t.Context(), "owner"), "parent-thread")
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithSessionID(session.RunID), compose_v2.WithRunNS("parent")); err != nil {
		t.Fatal(err)
	}
}

type rawGraphResumeHooks struct {
	compose_v2.NoOpOrchestrationHooks
	check func(context.Context, string)
}

func (h rawGraphResumeHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	h.check(ctx, "start")
	return ctx, nil
}

func (h rawGraphResumeHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	h.check(ctx, "end")
	return ctx
}

func TestEnterResumeRejectsRawGraphHooks(t *testing.T) {
	session := NewRunSession()
	other := NewRunSession()
	var fromHook context.Context
	calls := make(map[string]int)
	h := rawGraphResumeHooks{check: func(ctx context.Context, phase string) {
		calls[phase]++
		ctx = compose_v2.WithSessionIDCtx(ctx, other.RunID)
		if _, _, err := EnterResume(ctx, other.RunID, "custom"); !errors.Is(err, compose_v2.ErrRunAborted) {
			t.Errorf("%s: 裸图钩子中的独立续跑没有被拒绝：%v", phase, err)
		}
		fromHook = compose_v2.DetachExecution(ctx)
		if _, _, err := EnterResume(fromHook, session.RunID, "custom"); !errors.Is(err, compose_v2.ErrRunAborted) {
			t.Errorf("%s: 脱离之后误续正在运行的原任务：%v", phase, err)
		}
		if _, _, err := EnterResume(fromHook, other.RunID, "custom"); err != nil {
			t.Errorf("%s: 脱离后应允许续跑独立任务：%v", phase, err)
		}
	}}
	g, err := compose_v2.NewBuilder().AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}).AddEdge(compose_v2.StartNode, "work").AddEdge("work", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	err = compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(session.RunID),
		compose_v2.WithFreshRun(), compose_v2.WithHooks(h))
	if err != nil || calls["start"] != 1 || calls["end"] != 1 {
		t.Fatalf("需要实际经过两个图钩子：calls=%v err=%v", calls, err)
	}
	if _, req, err := EnterResume(fromHook, session.RunID, "custom"); err != nil || req.Session.RunID != session.RunID {
		t.Fatalf("图退出后应允许外层检查原任务的续跑请求：%+v err=%v", req, err)
	}
}

func TestEnterResumeRejectsAllAttachedGraphTasksAfterDetach(t *testing.T) {
	tasks := []string{NewRunSession().RunID, NewRunSession().RunID, NewRunSession().RunID}
	check := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		detached := DetachTask(ctx)
		for _, sid := range tasks {
			if _, _, err := EnterResume(detached, sid, "custom"); !errors.Is(err, compose_v2.ErrRunAborted) {
				return nil, fmt.Errorf("脱离丢失仍在运行的任务 %q：%v", sid, err)
			}
		}
		return nil, nil
	}
	var nested *compose_v2.Graph
	for level := len(tasks) - 1; level >= 0; level-- {
		fn := check
		if nested != nil {
			child, childTask := nested, tasks[level+1]
			fn = func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, compose_v2.Run(ctx, child, compose_v2.NewChannelManager(),
					compose_v2.WithSessionID(childTask), compose_v2.WithRunNS(compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "child")))
			}
		}
		g, err := compose_v2.NewBuilder().AddNode("step", fn).
			AddEdge(compose_v2.StartNode, "step").AddEdge("step", compose_v2.EndNode).Compile()
		if err != nil {
			t.Fatal(err)
		}
		nested = g
	}
	if err := compose_v2.Run(t.Context(), nested, compose_v2.NewChannelManager(),
		compose_v2.WithSessionID(tasks[0])); err != nil {
		t.Fatal(err)
	}
}

func TestEnterResumeRejectsInvalidAndHostedOptions(t *testing.T) {
	session := NewRunSession()
	cleanupCalls := 0
	cases := []struct {
		name string
		sid  string
		opts []ResumeOption
	}{
		{name: "空任务", sid: ""},
		{name: "空选项", sid: session.RunID, opts: []ResumeOption{nil}},
		{name: "会话不符", sid: session.RunID, opts: []ResumeOption{WithResumeSession(NewRunSession())}},
		{name: "未知操作", sid: session.RunID, opts: []ResumeOption{WithResumeAction(ResumeAction(99))}},
		{name: "图断点", sid: session.RunID, opts: []ResumeOption{WithResumeInterruptBefore("node")}},
		{name: "模型覆盖", sid: session.RunID, opts: []ResumeOption{WithResumeMaxRounds(3)}},
		{name: "宿主放弃", sid: session.RunID, opts: []ResumeOption{
			WithResumeAction(ResumeActionAbort),
			WithResumeAbortCleanup(func(context.Context, string, string, *HistoryKey) (int32, error) {
				cleanupCalls++
				return 1, nil
			}, nil),
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			got, req, err := EnterResume(ctx, tt.sid, "custom", tt.opts...)
			var nonRetryable compose_v2.NonRetryable
			if got != ctx || req.Session != nil || !errors.As(err, &nonRetryable) ||
				errors.Is(err, compose_v2.ErrRunAborted) {
				t.Fatalf("顶层非法请求应保持原上下文并可纠正：req=%+v err=%v", req, err)
			}
		})
	}
	if cleanupCalls != 0 {
		t.Fatalf("宿主的删档回调被执行 %d 次", cleanupCalls)
	}
	ctx, req, err := EnterResume(t.Context(), session.RunID, "custom", WithResumeAction(ResumeActionAbort))
	if action, ok := GetResumePhase(ctx); err != nil || !ok || action != ResumeActionAbort || req.Action != action {
		t.Fatalf("由应用管理的 Abort 应转交原请求：request=%+v err=%v", req, err)
	}
}
