package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 这组哨兵钉的不变量：**影子档对路由不可见**——配了影子档时，选中哪个分支、后面各档收到的请求、
// 续跑与失败重判时哪些档重放哪些重新调用、构造期约束，都与把影子档整个拿掉时相同；影子档唯一的
// 产出是事件（与 RouteUndecidedError）里它自己那条如实的记录。

func newShadowSupportAgent(opts ...agent_v2.RoutingOption) *agent_v2.RoutingAgent {
	opts = append([]agent_v2.RoutingOption{
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingDescriptions(supportDescriptions),
	}, opts...)
	return agent_v2.NewRoutingAgent(opts...).
		AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))
}

// TestRouteDecider_ShadowRecordsButNeverDecides：影子档每种结果都如实记下（够门槛的记 shadow，
// 其余记平常的原因），路由永远由后面的档决定。
func TestRouteDecider_ShadowRecordsButNeverDecides(t *testing.T) {
	cases := []struct {
		name   string
		dec    agent_v2.RouteDecision
		err    error
		reason string
	}{
		{"would adopt", agent_v2.RouteDecision{Target: "billing", Confidence: 0.95,
			Scores: map[string]float64{"billing": 0.95, "tech": 0.05}, Source: "shadow-model-1"}, nil, agent_v2.RouteReasonShadow},
		{"low confidence", agent_v2.RouteDecision{Target: "billing", Confidence: 0.5, Source: "shadow-model-1"}, nil, agent_v2.RouteReasonLowConfidence},
		{"abstain", agent_v2.RouteDecision{}, nil, agent_v2.RouteReasonAbstain},
		{"unknown target", agent_v2.RouteDecision{Target: "nope", Confidence: 0.99}, nil, agent_v2.RouteReasonUnknownTarget},
		{"error", agent_v2.RouteDecision{}, errors.New("shadow service down"), agent_v2.RouteReasonError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
				calls++
				return tc.dec, tc.err
			}
			cls := &recordingClassifier{target: "tech"}
			r := newShadowSupportAgent(
				agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()),
				agent_v2.WithRoutingClassifier(cls),
			)
			rec := &routeEventRecorder{}
			events, err := r.Run(agent_v2.WithCtxOrchHooks(context.Background(), rec), agent_v2.NewRunSession(), schema.UserMessage("发票开错了"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if out := finalContent(t, events); out != "from tech" {
				t.Fatalf("a shadow decider must never decide the route; want the classifier's tech, got %q", out)
			}
			if calls != 1 || len(cls.calls()) != 1 {
				t.Fatalf("shadow decider and classifier should each run once, got %d / %d", calls, len(cls.calls()))
			}
			a := rec.only(t)
			if a.Stage != agent_v2.RouteStageClassifier || a.Target != "tech" {
				t.Fatalf("the decision should come from the classifier, got %+v", a)
			}
			if len(a.Escalations) != 1 {
				t.Fatalf("the event should carry the shadow record, got %+v", a.Escalations)
			}
			e := a.Escalations[0]
			if !e.Shadow || e.DeciderIndex != 0 || e.Reason != tc.reason {
				t.Fatalf("shadow record should be flagged with reason %q, got %+v", tc.reason, e)
			}
			if tc.err == nil && (e.Target != tc.dec.Target || e.Source != tc.dec.Source) {
				t.Fatalf("shadow record should keep the shadow judgment as given, got %+v", e)
			}
			if tc.reason == agent_v2.RouteReasonShadow && (e.Confidence != 0.95 || e.Scores["tech"] != 0.05) {
				t.Fatalf("shadow record should keep confidence and scores, got %+v", e)
			}
			if tc.err != nil && e.Err != "shadow service down" {
				t.Fatalf("shadow error should be recorded, got %+v", e)
			}
		})
	}
}

// TestRouteDecider_ShadowHiddenFromLaterDeciders：后面的档收到的 Escalations 里没有影子记录，
// 由它决定路由时，事件里照样带着影子那条。
func TestRouteDecider_ShadowHiddenFromLaterDeciders(t *testing.T) {
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.95, Scores: map[string]float64{"billing": 0.95}}, nil
	}
	var seen [][]agent_v2.RouteEscalation
	real := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		seen = append(seen, req.Escalations)
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.9}, nil
	}
	r := newShadowSupportAgent(
		agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()),
		agent_v2.WithRouteDecider(real, 0.85),
		agent_v2.WithRoutingFallback(branchAgent("fallback")),
	)
	rec := &routeEventRecorder{}
	events, err := r.Run(agent_v2.WithCtxOrchHooks(context.Background(), rec), agent_v2.NewRunSession(), schema.UserMessage("登不上去"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("the real decider should decide, got %q", out)
	}
	if len(seen) != 1 || len(seen[0]) != 0 {
		t.Fatalf("a later decider must not see shadow records in its request, got %+v", seen)
	}
	a := rec.only(t)
	if a.Stage != agent_v2.RouteStageDecider || a.DeciderIndex != 0 || a.Target != "tech" {
		t.Fatalf("decision should come from decider 0 (shadow tiers do not take a decider index), got %+v", a)
	}
	if len(a.Escalations) != 1 || !a.Escalations[0].Shadow || a.Escalations[0].Reason != agent_v2.RouteReasonShadow {
		t.Fatalf("the event should still carry the shadow record, got %+v", a.Escalations)
	}
}

// TestRouteDecider_ShadowErrorDoesNotMoveRetryPoint：影子档报错不算「有档报错」——重判起点与 LastErr
// 都和没有影子档时一样。没有真实档报错，重试时从头判。
func TestRouteDecider_ShadowErrorDoesNotMoveRetryPoint(t *testing.T) {
	firstCalls := 0
	first := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		firstCalls++
		return agent_v2.RouteDecision{}, nil
	}
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, errors.New("shadow service down")
	}
	last := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, nil
	}
	r := newShadowSupportAgent(
		agent_v2.WithRouteDecider(first, 0),
		agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()),
		agent_v2.WithRouteDecider(last, 0),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("an undecided chain should return RouteUndecidedError, got %v", err)
	}
	if ue.LastErr != nil {
		t.Fatalf("LastErr is the last erroring real tier; a shadow tier's error must not show up there, got %v", ue.LastErr)
	}
	if len(ue.Escalations) != 3 || !ue.Escalations[1].Shadow || ue.Escalations[1].Reason != agent_v2.RouteReasonError {
		t.Fatalf("the error should still carry the shadow record for diagnosis, got %+v", ue.Escalations)
	}
	if strings.Contains(err.Error(), "shadow") {
		t.Fatalf("the error text may reach a model (as a tool result); it must not carry the shadow tier, got %q", err.Error())
	}

	_, err = r.Resume(ctx, sess.RunID)
	if !errors.As(err, &ue) {
		t.Fatalf("the retry is still undecided, got %v", err)
	}
	if firstCalls != 2 {
		t.Fatalf("no real tier errored, so the retry must decide from scratch and call the first decider again "+
			"(the shadow error must not move the retry point); got %d calls", firstCalls)
	}
}

// TestRouteDecider_ShadowReplayedOnResume：影子档跑完之后后面的档问用户，Resume 时影子档原样重放、
// 不再调用，路由按用户的回答走。
func TestRouteDecider_ShadowReplayedOnResume(t *testing.T) {
	shadowCalls := 0
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		shadowCalls++
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.95}, nil
	}
	askUser := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您是想办理哪一类业务？", Options: []string{"billing", "tech"}})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1, Source: "user"}, nil
	}
	r := newShadowSupportAgent(
		agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()),
		agent_v2.WithRouteDecider(askUser, 0),
	)
	rec := &routeEventRecorder{}
	ctx := agent_v2.WithCtxOrchHooks(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), rec)
	sess := agent_v2.NewRunSession()

	_, err := r.Run(ctx, sess, schema.UserMessage("钱的事"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the real tier should ask the user, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("tech"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("should route to the branch the user picked, got %q", out)
	}
	if shadowCalls != 1 {
		t.Fatalf("the shadow tier finished before the interrupt and must be replayed, not called again; got %d calls", shadowCalls)
	}
	a := rec.only(t)
	if a.DeciderIndex != 0 || len(a.Escalations) != 1 || !a.Escalations[0].Shadow || a.Escalations[0].Target != "billing" {
		t.Fatalf("the event should come from decider 0 and carry the replayed shadow record, got %+v", a)
	}
}

// TestRouteDecider_ShadowQuestionStillInterrupts：影子档里提问照常中断——框架不吞控制流信号，影子档
// 也不例外。影子档不该提问，这条钉住的是「提了会发生什么」。
func TestRouteDecider_ShadowQuestionStillInterrupts(t *testing.T) {
	shadow := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "shadow asks"}); err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{}, nil
	}
	r := newShadowSupportAgent(
		agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()),
		agent_v2.WithRoutingClassifier(&recordingClassifier{target: "tech"}),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil || !isInterruptErr(err) {
		t.Fatalf("an interrupt raised inside a shadow decider must propagate, got %v", err)
	}
}

// TestRouteDecider_ShadowConstructionGuards：构造期与 Run 入口的约束只看非影子档；和 Router 同时配
// 仍然 panic（Router 总会自己决定，影子档永远不会被调用）。
func TestRouteDecider_ShadowConstructionGuards(t *testing.T) {
	noDecision := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, nil
	}
	expectPanic := func(t *testing.T, want string, fn func()) {
		t.Helper()
		defer func() {
			rec := recover()
			if rec == nil || !strings.Contains(fmt.Sprint(rec), want) {
				t.Fatalf("expected panic containing %q, got %v", want, rec)
			}
		}()
		fn()
	}
	t.Run("shadow after a zero-threshold tier needs no escalation target", func(t *testing.T) {
		agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"),
			agent_v2.WithRouteDecider(noDecision, 0),
			agent_v2.WithRouteDecider(noDecision, 0.85, agent_v2.WithShadow()))
	})
	t.Run("last real tier can still escalate with nowhere to go", func(t *testing.T) {
		expectPanic(t, "没有地方可升级", func() {
			agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"),
				agent_v2.WithRouteDecider(noDecision, 0.85),
				agent_v2.WithRouteDecider(noDecision, 0, agent_v2.WithShadow()))
		})
	})
	t.Run("shadow tiers alone are no decision source", func(t *testing.T) {
		r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"),
			agent_v2.WithRouteDecider(noDecision, 0.85, agent_v2.WithShadow()),
			agent_v2.WithRoutingFallback(branchAgent("fallback")),
		).AddBranch("billing", branchAgent("billing"))
		_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
		if err == nil || !strings.Contains(err.Error(), "decider or classifier required") {
			t.Fatalf("shadow tiers alone should not count as a decision source, got %v", err)
		}
	})
	t.Run("shadow with a router", func(t *testing.T) {
		expectPanic(t, "WithRouter", func() {
			agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"),
				agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) { return "billing", nil }),
				agent_v2.WithRouteDecider(noDecision, 0.85, agent_v2.WithShadow()))
		})
	})
}

func isInterruptErr(err error) bool {
	_, ok := compose_v2.IsInterrupt(err)
	return ok
}

// flakyAt 第一次调用给 billing 0.8（低于 0.85 的门槛），之后给 tech 0.9：重新调用就会越过门槛。
func flakyAt(calls *int) agent_v2.RouteDecider {
	return func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		*calls++
		if *calls == 1 {
			return agent_v2.RouteDecision{Target: "billing", Confidence: 0.8}, nil
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.9}, nil
	}
}

func askBranch(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "哪一类？", Options: []string{"billing", "tech"}})
	if err != nil {
		return agent_v2.RouteDecision{}, err
	}
	return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1}, nil
}

// TestRouteDecider_ShadowAddedWhileTaskPaused：任务停在「问用户」时部署挂上了影子档，续跑仍按用户的回答走——
// 影子档不占真实档的下标，后面各档的重放与调用身份不平移。
func TestRouteDecider_ShadowAddedWhileTaskPaused(t *testing.T) {
	flakyCalls := 0
	flaky := flakyAt(&flakyCalls)
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.99}, nil
	}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	before := newShadowSupportAgent(agent_v2.WithRouteDecider(flaky, 0.85), agent_v2.WithRouteDecider(askBranch, 0))
	_, err := before.Run(ctx, sess, schema.UserMessage("钱的事"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the chain should stop to ask the user, got %v", err)
	}
	after := newShadowSupportAgent(
		agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()),
		agent_v2.WithRouteDecider(flaky, 0.85), agent_v2.WithRouteDecider(askBranch, 0))
	events, err := after.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("billing"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" || flakyCalls != 1 {
		t.Fatalf("adding a shadow tier to a paused task must not change its route: want the user's billing with flaky replayed (1 call), got %q / %d calls", out, flakyCalls)
	}
}

// TestRouteDecider_ShadowRemovedWhileTaskPaused：任务停在「问用户」时部署摘掉了影子档，续跑仍按用户的回答走。
func TestRouteDecider_ShadowRemovedWhileTaskPaused(t *testing.T) {
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.99}, nil
	}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	before := newShadowSupportAgent(agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()), agent_v2.WithRouteDecider(askBranch, 0))
	_, err := before.Run(ctx, sess, schema.UserMessage("钱的事"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the real tier should ask the user, got %v", err)
	}
	after := newShadowSupportAgent(agent_v2.WithRouteDecider(askBranch, 0))
	events, err := after.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("tech"),
	}))
	if err != nil {
		t.Fatalf("removing a shadow tier from a paused task must not lose the user's answer, got %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("should route to the branch the user picked, got %q", out)
	}
}

// TestRouteDecider_ShadowPanicDoesNotBreakRouting：影子档 panic 记为 error，路由照常由后面的档决定。
func TestRouteDecider_ShadowPanicDoesNotBreakRouting(t *testing.T) {
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		var m map[string]int
		m["boom"] = 1 // 写 nil map：未验证的新决策方里的典型 bug
		return agent_v2.RouteDecision{}, nil
	}
	real := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9}, nil
	}
	r := newShadowSupportAgent(agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()), agent_v2.WithRouteDecider(real, 0))
	rec := &routeEventRecorder{}
	events, err := r.Run(agent_v2.WithCtxOrchHooks(context.Background(), rec), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("a panicking shadow decider must not fail the routing, got %v", err)
	}
	if out := finalContent(t, events); out != "from billing" {
		t.Fatalf("routing should be decided by the real tier, got %q", out)
	}
	e := rec.only(t).Escalations
	if len(e) != 1 || !e[0].Shadow || e[0].Reason != agent_v2.RouteReasonError || !strings.Contains(e[0].Err, "panicked") {
		t.Fatalf("the panic should be recorded as the shadow tier's error, got %+v", e)
	}
}

// TestRouteDecider_ShadowErrorReplayedKeepsLastErr：影子档报错之后真实档问了用户，Resume 时影子档的报错原样重放，
// 也不进 LastErr。
func TestRouteDecider_ShadowErrorReplayedKeepsLastErr(t *testing.T) {
	shadowCalls := 0
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		shadowCalls++
		return agent_v2.RouteDecision{}, errors.New("shadow 503")
	}
	askThenAbstain := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "哪一类？"}); err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{}, nil // 用户答了也定不下来
	}
	r := newShadowSupportAgent(agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()), agent_v2.WithRouteDecider(askThenAbstain, 0))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the real tier should ask, got %v", err)
	}
	_, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("不知道"),
	}))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("the chain is still undecided, got %v", err)
	}
	if ue.LastErr != nil {
		t.Fatalf("a replayed shadow error must not become LastErr, got %v", ue.LastErr)
	}
	if shadowCalls != 1 {
		t.Fatalf("the shadow tier should be replayed on Resume, got %d calls", shadowCalls)
	}
}

// TestRouteDecider_ShadowKeepsRealDeciderIndices：后面的档按 DeciderIndex 找前一档时，结果与没有影子档时相同。
func TestRouteDecider_ShadowKeepsRealDeciderIndices(t *testing.T) {
	j := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.6}, nil
	}
	k := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		for _, e := range req.Escalations {
			if e.DeciderIndex == 0 {
				return agent_v2.RouteDecision{Target: e.Target, Confidence: 1}, nil
			}
		}
		return agent_v2.RouteDecision{}, nil
	}
	shadow := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.99}, nil
	}
	run := func(opts ...agent_v2.RoutingOption) string {
		opts = append(opts, agent_v2.WithRouteDecider(j, 0.85), agent_v2.WithRouteDecider(k, 0),
			agent_v2.WithRoutingFallback(branchAgent("fallback")))
		events, err := newShadowSupportAgent(opts...).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return finalContent(t, events)
	}
	without := run()
	with := run(agent_v2.WithRouteDecider(shadow, 0.85, agent_v2.WithShadow()))
	if without != "from billing" || with != without {
		t.Fatalf("a later tier looking up decider 0 must see the same record with or without a shadow tier: without=%q with=%q", without, with)
	}
}
