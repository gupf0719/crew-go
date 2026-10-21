package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// routeEventRecorder 收集 OnNodeEvent 里的路由决策事件。
type routeEventRecorder struct {
	agent_v2.NoOpOrchestrationHooks
	mu      sync.Mutex
	actions []*agent_v2.RouteDecisionAction
}

func (h *routeEventRecorder) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	a, ok := agent_v2.RouteDecisionOf(ev)
	if !ok {
		return ctx
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.actions = append(h.actions, a)
	return ctx
}

// only 返回唯一一条路由决策事件；条数不是 1 时让测试失败。
func (h *routeEventRecorder) only(t *testing.T) *agent_v2.RouteDecisionAction {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.actions) != 1 {
		t.Fatalf("expected exactly one route_decision event, got %d", len(h.actions))
	}
	return h.actions[0]
}

// recordingClassifier 是读入参的 classifier 桩：记下收到的每条输入，回一个 route_to 调用。
type recordingClassifier struct {
	target string
	mu     sync.Mutex
	inputs []string
}

func (c *recordingClassifier) Run(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	c.mu.Lock()
	c.inputs = append(c.inputs, in.Content)
	c.mu.Unlock()
	ev := agent_v2.NewExitEvent("classifier", makeClassifierMsg(c.target))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}
func (c *recordingClassifier) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *recordingClassifier) Name() string        { return "classifier" }
func (c *recordingClassifier) Description() string { return "" }

func (c *recordingClassifier) calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.inputs...)
}

// branchAgent 返回一个产出固定为 "from <name>" 的分支，用最终消息判断实际跑到了哪个分支。
func branchAgent(name string) agent_v2.Agent {
	return &fixedAgent{name: name, resp: schema.AssistantMessage("from " + name)}
}

// supportDescriptions 是两个候选分支的描述，决策方与 classifier 都该拿到它们。
var supportDescriptions = map[string]string{
	"billing": "账单、扣费、发票",
	"tech":    "故障、报错、使用问题",
}

func finalContent(t *testing.T, events []*agent_v2.AgentEvent) string {
	t.Helper()
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("RoutingAgent produced no final message")
	}
	return final.Content
}

// TestRouteDecider_ConfidentDecisionSkipsClassifier：决策方置信度达到门槛就直接采纳，
// classifier 一次都不调；决策方收到原始输入和按注册顺序排好的候选。
func TestRouteDecider_ConfidentDecisionSkipsClassifier(t *testing.T) {
	var got []agent_v2.RouteRequest
	decider := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		got = append(got, req)
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9, Source: "stub-1", Scores: map[string]float64{"billing": 0.9, "tech": 0.1}}, nil
	}
	cls := &recordingClassifier{target: "tech"}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.85),
		agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingDescriptions(supportDescriptions),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	rec := &routeEventRecorder{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("发票开错了"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" {
		t.Fatalf("confident decision should route to billing, got %q", out)
	}
	if n := len(cls.calls()); n != 0 {
		t.Fatalf("classifier must not run when the decider is confident, ran %d times", n)
	}

	if len(got) != 1 {
		t.Fatalf("decider should be called once, got %d", len(got))
	}
	req := got[0]
	if req.Input == nil || req.Input.Content != "发票开错了" {
		t.Fatalf("decider should receive the original input, got %+v", req.Input)
	}
	wantCands := []agent_v2.RoutingChoice{
		{Name: "billing", Description: supportDescriptions["billing"]},
		{Name: "tech", Description: supportDescriptions["tech"]},
	}
	if fmt.Sprint(req.Candidates) != fmt.Sprint(wantCands) {
		t.Fatalf("candidates should follow AddBranch order with descriptions, got %v", req.Candidates)
	}
	if len(req.Escalations) != 0 {
		t.Fatalf("the first decider should see no escalations, got %v", req.Escalations)
	}

	a := rec.only(t)
	if a.Stage != agent_v2.RouteStageDecider || a.Target != "billing" || a.DeciderIndex != 0 ||
		a.Confidence == nil || *a.Confidence != 0.9 || a.Source != "stub-1" || a.Scores["tech"] != 0.1 || len(a.Escalations) != 0 {
		t.Fatalf("unexpected route_decision event: %+v", a)
	}
}

// TestRouteDecider_LowConfidenceEscalatesToClassifier：置信度低于门槛时交给 classifier，
// classifier 收到的是原始输入，事件记下被跳过的那一档。
func TestRouteDecider_LowConfidenceEscalatesToClassifier(t *testing.T) {
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.6, Scores: map[string]float64{"billing": 0.6, "tech": 0.4}}, nil
	}
	cls := &recordingClassifier{target: "tech"}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.85),
		agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingDescriptions(supportDescriptions),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	rec := &routeEventRecorder{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("登不上去"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("low confidence should escalate to the classifier's choice, got %q", out)
	}
	calls := cls.calls()
	if len(calls) != 1 || !strings.Contains(calls[0], "登不上去") {
		t.Fatalf("classifier should run once on the original input, got %q", calls)
	}

	a := rec.only(t)
	if a.Stage != agent_v2.RouteStageClassifier || a.Target != "tech" || a.Confidence != nil {
		t.Fatalf("final decision should come from the classifier, got %+v", a)
	}
	if len(a.Escalations) != 1 {
		t.Fatalf("expected one escalation, got %+v", a.Escalations)
	}
	e := a.Escalations[0]
	if e.DeciderIndex != 0 || e.Reason != agent_v2.RouteReasonLowConfidence || e.Target != "billing" ||
		e.Confidence != 0.6 || e.Scores["tech"] != 0.4 {
		t.Fatalf("escalation should carry the rejected decision, got %+v", e)
	}
}

// TestRouteDecider_UndecidedOutcomesEscalateToFallback：六种「这一档没决定」都交给下一档，
// 不让整次路由失败。这里下一档是兜底分支。
func TestRouteDecider_UndecidedOutcomesEscalateToFallback(t *testing.T) {
	cases := []struct {
		name   string
		dec    agent_v2.RouteDecision
		err    error
		reason string
	}{
		{"low confidence", agent_v2.RouteDecision{Target: "billing", Confidence: 0.2}, nil, agent_v2.RouteReasonLowConfidence},
		{"abstain", agent_v2.RouteDecision{Confidence: 0.99}, nil, agent_v2.RouteReasonAbstain},
		{"unknown target", agent_v2.RouteDecision{Target: "sales", Confidence: 0.99}, nil, agent_v2.RouteReasonUnknownTarget},
		{"confidence above 1", agent_v2.RouteDecision{Target: "billing", Confidence: 1.5}, nil, agent_v2.RouteReasonInvalidConfidence},
		{"confidence NaN", agent_v2.RouteDecision{Target: "billing", Confidence: math.NaN()}, nil, agent_v2.RouteReasonInvalidConfidence},
		{"decider error", agent_v2.RouteDecision{}, errors.New("decision service unavailable"), agent_v2.RouteReasonError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
				return tc.dec, tc.err
			}
			r := agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("support"),
				agent_v2.WithRouteDecider(decider, 0.5),
				agent_v2.WithRoutingFallback(branchAgent("human")),
			).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

			rec := &routeEventRecorder{}
			ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
			events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
			if err != nil {
				t.Fatalf("an undecided stage must escalate, not fail the run: %v", err)
			}
			if out := finalContent(t, events); out != "from human" {
				t.Fatalf("should end at the fallback branch, got %q", out)
			}
			a := rec.only(t)
			if a.Stage != agent_v2.RouteStageFallback || a.Target != "__fallback__" {
				t.Fatalf("final decision should be the fallback, got %+v", a)
			}
			if len(a.Escalations) != 1 || a.Escalations[0].Reason != tc.reason {
				t.Fatalf("escalation reason should be %q, got %+v", tc.reason, a.Escalations)
			}
			if tc.err != nil && !strings.Contains(a.Escalations[0].Err, "unavailable") {
				t.Fatalf("escalation should keep the decider error text, got %+v", a.Escalations[0])
			}
		})
	}
}

// TestRouteDecider_ServiceTimeoutEscalates：决策服务自己的请求超时带着
// context.DeadlineExceeded，但调用方的 ctx 还活着——这正是该换下一档的情形。
// 按「错误链上有没有 ctx 错」判的话，这里会让整次路由失败。
func TestRouteDecider_ServiceTimeoutEscalates(t *testing.T) {
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, fmt.Errorf("decision service: %w", context.DeadlineExceeded)
	}
	cls := &recordingClassifier{target: "tech"}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.85),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("a decider-side timeout must escalate, got %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("should be routed by the classifier, got %q", out)
	}
	if len(cls.calls()) != 1 {
		t.Fatalf("classifier should take over after the decider timed out")
	}
}

// TestRouteDecider_CallerCancelPropagates：调用方的 ctx 取消了就原样往上抛，不升级，
// 也不调 classifier。
func TestRouteDecider_CallerCancelPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	decider := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		cancel()
		return agent_v2.RouteDecision{}, ctx.Err()
	}
	cls := &recordingClassifier{target: "tech"}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.85),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	_, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation should propagate, got %v", err)
	}
	if n := len(cls.calls()); n != 0 {
		t.Fatalf("classifier must not run after the caller cancelled, ran %d times", n)
	}
}

// TestRouteDecider_CascadeOrderAndIsolation：多档决策方按挂载顺序执行，后一档看得到
// 前一档没被采纳的结果；前一档改了候选清单不影响后一档。
func TestRouteDecider_CascadeOrderAndIsolation(t *testing.T) {
	var secondReq agent_v2.RouteRequest
	first := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		req.Candidates[0].Name = "tampered"
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.5, Source: "fast"}, nil
	}
	second := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		secondReq = req
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.7, Source: "slow"}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(first, 0.9),
		agent_v2.WithRouteDecider(second, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	rec := &routeEventRecorder{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("the second decider should decide, got %q", out)
	}
	if len(secondReq.Candidates) != 2 || secondReq.Candidates[0].Name != "billing" {
		t.Fatalf("a decider mutating its candidates must not affect the next one, got %v", secondReq.Candidates)
	}
	if len(secondReq.Escalations) != 1 || secondReq.Escalations[0].Target != "billing" ||
		secondReq.Escalations[0].Source != "fast" || secondReq.Escalations[0].Reason != agent_v2.RouteReasonLowConfidence {
		t.Fatalf("the second decider should see the first one's rejected decision, got %+v", secondReq.Escalations)
	}
	a := rec.only(t)
	if a.Stage != agent_v2.RouteStageDecider || a.DeciderIndex != 1 || a.Source != "slow" || len(a.Escalations) != 1 {
		t.Fatalf("event should credit the second decider, got %+v", a)
	}
}

// TestRouteDecider_AskUserWhenUnsure：低置信时由第二档问用户，只列第一档概率最高的
// 两个候选；Resume 带上答案后路由到用户选的分支。
//
// Resume 时 classify 节点整个重跑，但第一档已经跑完，它的结果从节点私有状态重放，
// 不会被再调一次；问用户的那一档会被再调一次，这次拿到答案。
func TestRouteDecider_AskUserWhenUnsure(t *testing.T) {
	var fastCalls int
	fast := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		fastCalls++
		return agent_v2.RouteDecision{
			Target: "billing", Confidence: 0.4,
			Scores: map[string]float64{"billing": 0.45, "tech": 0.4, "sales": 0.15},
		}, nil
	}
	var asked []string
	askUser := func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		prev := req.Escalations[len(req.Escalations)-1]
		options := topTwo(prev.Scores)
		asked = options
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您是想办理哪一类业务？", Options: options})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1, Source: "user"}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(fast, 0.85),
		agent_v2.WithRouteDecider(askUser, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech")).AddBranch("sales", branchAgent("sales"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("钱的事"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("an unsure route should become a question to the user, got %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 1 || fmt.Sprint(qs[0].Options) != fmt.Sprint([]string{"billing", "tech"}) {
		t.Fatalf("the question should offer the first decider's top two candidates, got %+v", qs)
	}

	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		qs[0].InterruptID: schema.UserMessage("tech"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("should route to the branch the user picked, got %q", out)
	}
	if fastCalls != 1 {
		t.Fatalf("the first decider already finished before the interrupt and must be replayed on Resume, not called again; got %d calls", fastCalls)
	}
	if fmt.Sprint(asked) != fmt.Sprint([]string{"billing", "tech"}) {
		t.Fatalf("the rerun should ask about the same candidates, got %v", asked)
	}
}

// topTwo 按分数从高到低取前两个候选名。
func topTwo(scores map[string]float64) []string {
	type kv struct {
		k string
		v float64
	}
	all := make([]kv, 0, len(scores))
	for k, v := range scores {
		all = append(all, kv{k, v})
	}
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].v > all[i].v || (all[j].v == all[i].v && all[j].k < all[i].k) {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	out := make([]string, 0, 2)
	for i := 0; i < len(all) && i < 2; i++ {
		out = append(out, all[i].k)
	}
	return out
}

// TestRouteDecider_NoEscalationTargetReturnsError：最后一档门槛为 0 时可以不配 classifier
// 和兜底；这时决策方报错就让 Run 失败，返回 RouteUndecidedError，原始错误在 LastErr 里，
// 外层编排不再整体重试。
func TestRouteDecider_NoEscalationTargetReturnsError(t *testing.T) {
	sentinel := errors.New("decision service unavailable")
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, sentinel
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0),
	).AddBranch("billing", branchAgent("billing"))

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("with nowhere to escalate the run should fail with RouteUndecidedError, got %v", err)
	}
	if ue.LastErr != sentinel {
		t.Fatalf("LastErr should be the decider's own error, got %v", ue.LastErr)
	}
	if ue.AgentName != "support" || len(ue.Escalations) != 1 || ue.Escalations[0].Reason != agent_v2.RouteReasonError {
		t.Fatalf("the error should record why each decider was passed over, got %+v", ue)
	}
	if !strings.Contains(err.Error(), "no decider produced an acceptable decision") {
		t.Fatalf("error should say why routing failed, got %v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Fatal("outer orchestrations must not retry a routing agent whose deciders all passed")
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatal("a failure that a retry may fix must not end the run for good")
	}
}

// TestRouteDecider_UndecidedWithoutErrorsRetriesFromScratch：每一档都弃权、没有一档报错时，
// 整条链没拿定主意。这是任务层面的失败，不是终局：存档留着，重试从头判，输出不完全确定
// 的决策方这次可能就有把握了。
func TestRouteDecider_UndecidedWithoutErrorsRetriesFromScratch(t *testing.T) {
	var calls int
	flaky := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		calls++
		if calls == 1 {
			return agent_v2.RouteDecision{}, nil
		}
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(flaky, 0),
	).AddBranch("billing", branchAgent("billing"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) || errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("an undecided chain should fail with a non-terminal RouteUndecidedError, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("the retry should call the decider again, got %v", err)
	}
	if out := finalContent(t, events); out != "from billing" || calls != 2 {
		t.Fatalf("the retry should decide from scratch: routed %q, decider called %d times", out, calls)
	}
}

// TestRouteDecider_UndecidedRouterAsToolLeavesParentRunning：RoutingAgent 被包成工具时，
// 决策方都拿不准只是这个工具失败了，模型拿到失败后自己换条路，父 Run 不受影响。
func TestRouteDecider_UndecidedRouterAsToolLeavesParentRunning(t *testing.T) {
	abstain := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, nil
	}
	router := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingDescription("把问题转给合适的专员"),
		agent_v2.WithRouteDecider(abstain, 0),
	).AddBranch("billing", branchAgent("billing"))

	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(router))
	tc := schema.ToolCall{ID: "tc_1", Type: "function", Function: schema.FunctionCall{Name: "router", Arguments: `{"input":"随便聊聊"}`}}
	var seen []*schema.Message
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("assistant"),
		agent_v2.WithModel(newStubModelCapturing([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
			schema.AssistantMessage("转接没成功，我直接回答你"),
		}, &seen)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	events, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("随便聊聊"))
	if err != nil {
		t.Fatalf("an undecided router used as a tool must not end the parent run: %v", err)
	}
	if out := agent_v2.FinalMessage(events); out == nil || out.Content != "转接没成功，我直接回答你" {
		t.Fatalf("the model should answer after the tool failed, got %+v", out)
	}
	// 模型看到的工具结果要是这次路由失败的原因，不能是一次静默的空结果。
	var toolResult string
	for _, m := range seen {
		if m != nil && m.Role == schema.RoleTool {
			toolResult = m.Content
		}
	}
	if !strings.Contains(toolResult, "no decider produced an acceptable decision") {
		t.Fatalf("the model should see why routing failed, got tool result %q", toolResult)
	}
}

// TestRouteDecider_SequentialRetriesOnlyTheRoutingStep：Sequential 的第一步问过用户、
// 带副作用地完成了，第二步的路由没拿定主意。重试只重跑路由这一步，第一步不再问、副作用
// 不重做。
func TestRouteDecider_SequentialRetriesOnlyTheRoutingStep(t *testing.T) {
	var sideEffects, deciderCalls int
	confirm := &runFnAgent{name: "confirm", runFn: func(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认下单吗？"})
		if err != nil {
			return nil, err
		}
		sideEffects++
		ev := agent_v2.NewExitEvent("confirm", schema.AssistantMessage("已下单："+ans.Content))
		if sess != nil {
			sess.AddEvent(ev)
		}
		return []*agent_v2.AgentEvent{ev}, nil
	}}
	flaky := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		deciderCalls++
		if deciderCalls == 1 {
			return agent_v2.RouteDecision{}, nil
		}
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9}, nil
	}
	router := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRouteDecider(flaky, 0),
	).AddBranch("billing", branchAgent("billing"))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("confirm", confirm).AddStep("route", router)

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(ctx, sess, schema.UserMessage("买一台"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the first step should ask, got %v", err)
	}
	if _, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
	})); err == nil {
		t.Fatal("the routing step should fail the first time")
	}
	events, err := seq.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("retrying the failed routing step should work, got %v", err)
	}
	if out := agent_v2.FinalMessage(events); out == nil || out.Content != "from billing" {
		t.Fatalf("expected the retried routing step to reach billing, got %+v", out)
	}
	if sideEffects != 1 || deciderCalls != 2 {
		t.Fatalf("only the routing step should run again: side effects %d, decider calls %d", sideEffects, deciderCalls)
	}
}

// TestRouteDecider_ClassifierQuestionSurvivesEarlierTransientError：前面一档临时报错、
// classifier 在问用户时，Resume 要原样重放前面各档，把用户的回答兑现给 classifier。
// 重新调用那一档的话，它这次若拿定了主意就绕过了 classifier，用户的回答被悄悄丢掉。
func TestRouteDecider_ClassifierQuestionSurvivesEarlierTransientError(t *testing.T) {
	var calls int
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		calls++
		if calls == 1 {
			return agent_v2.RouteDecision{}, errors.New("decision service returned 503")
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.99}, nil
	}
	cls := &askingClassifier{}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the classifier to ask, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("billing"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" || calls != 1 {
		t.Fatalf("the classifier's question must be honored: routed %q, decider called %d times", out, calls)
	}
}

// TestRouteDecider_ClassifierContradictionFirstTargetWins：classifier 同一批里给出两个不同目标时，
// 排在前面的生效，链照常收尾——不算 classifier 失败，决策方也不因此重新调用。理由见
// TestRouteSignal_MultipleRouteCalls_FirstWins。决策方低置信猜的是排在后面的那个目标，
// 「取最后一个」与「回落到决策方的猜测」都会路由到 billing。
func TestRouteDecider_ClassifierContradictionFirstTargetWins(t *testing.T) {
	var deciderCalls int
	unsure := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		deciderCalls++
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.2}, nil
	}
	conflict := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"tech"}`}},
		{ID: "r2", Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"billing"}`}},
	})
	cls := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{conflict})),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(3),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(unsure, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("two targets in one batch should not fail the chain: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" || deciderCalls != 1 {
		t.Fatalf("the first route_to should win: routed %q, decider called %d times", out, deciderCalls)
	}
}

// TestRouteDecider_RetryAfterFailureKeepsAnsweredStages：以失败收尾后重试，只重新调用报错
// 的那一档和它后面的档。第 0 档已经问过用户、用户也答了，重试时不能再问一遍。
func TestRouteDecider_RetryAfterFailureKeepsAnsweredStages(t *testing.T) {
	var askCalls, flakyCalls int
	ask := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		askCalls++
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if strings.TrimSpace(ans.Content) == "不确定" {
			return agent_v2.RouteDecision{}, nil
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1}, nil
	}
	flaky := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		flakyCalls++
		if flakyCalls == 1 {
			return agent_v2.RouteDecision{}, errors.New("decision service returned 503")
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.9}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(ask, 0.5),
		agent_v2.WithRouteDecider(flaky, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the first decider to ask, got %v", err)
	}
	_, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("不确定"),
	}))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("expected the chain to fail on the 503, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("the retry should re-call only the failed decider, got %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("expected the retried decider to route to tech, got %q", out)
	}
	if askCalls != 2 || flakyCalls != 2 {
		t.Fatalf("the answered decider must be replayed, not asked again: ask called %d times, flaky %d times", askCalls, flakyCalls)
	}
}

// TestRouteDecider_ClassifierFailureRetriesFailedDeciders：classifier 没给出候选分支让链失败时，
// 前面报错的决策方在重试时也要重新调用，不能把它的报错当成已完成来重放。
func TestRouteDecider_ClassifierFailureRetriesFailedDeciders(t *testing.T) {
	var deciderCalls int
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		deciderCalls++
		if deciderCalls == 1 {
			return agent_v2.RouteDecision{}, errors.New("decision service returned 503")
		}
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9}, nil
	}
	single := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r3", Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"tech"}`}},
	})
	cls := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("我也说不准"), single})),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(3),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	var undecided *agent_v2.RouteUndecidedError
	if _, err := r.Run(ctx, sess, schema.UserMessage("hi")); !errors.As(err, &undecided) {
		t.Fatalf("a classifier that names no branch should fail the run, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" || deciderCalls != 2 {
		t.Fatalf("the failed decider should be called again and decide: routed to %q, decider called %d times", out, deciderCalls)
	}
}

// TestRouteDecider_UndecidedErrorHidesDeciderTimeout：决策服务自己的请求超时带着
// context.DeadlineExceeded。RouteUndecidedError 不让它出现在错误链上，否则按错误链判断的
// 地方会当成调用方的 ctx 到期了。
func TestRouteDecider_UndecidedErrorHidesDeciderTimeout(t *testing.T) {
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, fmt.Errorf("decision service: %w", context.DeadlineExceeded)
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0),
	).AddBranch("billing", branchAgent("billing"))

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) || compose_v2.IsControlFlowSignal(err) {
		t.Fatalf("the decider's own timeout must not look like the caller's deadline, got %v", err)
	}
	if !errors.Is(ue.LastErr, context.DeadlineExceeded) {
		t.Fatalf("the original error should stay available in LastErr, got %v", ue.LastErr)
	}
}

// TestRouteDecider_CheckpointNotResumablePropagates：存档不可续是框架判定的结果，决策方
// 把它带出来时原样往上抛，不当成这一档的普通失败升级到下一档。
func TestRouteDecider_CheckpointNotResumablePropagates(t *testing.T) {
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, fmt.Errorf("helper agent: %w", compose_v2.ErrCheckpointNotResumable)
	}
	cls := &recordingClassifier{target: "billing"}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing"))

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if !errors.Is(err, compose_v2.ErrCheckpointNotResumable) {
		t.Fatalf("ErrCheckpointNotResumable from a decider should propagate, got %v", err)
	}
	if n := len(cls.calls()); n != 0 {
		t.Fatalf("the chain must stop instead of escalating to the classifier, classifier called %d times", n)
	}
}

// TestRoutingClassifier_CheckpointNotResumablePropagates：classifier 那一档同理，不降级到兜底。
func TestRoutingClassifier_CheckpointNotResumablePropagates(t *testing.T) {
	cls := &fixedAgent{name: "classifier", err: fmt.Errorf("classifier graph: %w", compose_v2.ErrCheckpointNotResumable)}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingFallback(branchAgent("human")),
	).AddBranch("billing", branchAgent("billing"))

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if !errors.Is(err, compose_v2.ErrCheckpointNotResumable) {
		t.Fatalf("ErrCheckpointNotResumable from the classifier should propagate, got %v", err)
	}
}

// TestRouteDecider_RequestCopiesAreIsolated：决策方改了收到的请求，既不影响分支收到的
// 输入，也不影响后面的档与路由决策事件看到的前面各档结果。
func TestRouteDecider_RequestCopiesAreIsolated(t *testing.T) {
	first := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		req.Input.Content = "tampered by decider 0"
		req.Input.Annotations["tag"] = "tampered"
		req.Input.Metadata.System = "tampered"
		req.Input.ContentBlocks[0].Type = "tampered"
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.3, Scores: map[string]float64{"billing": 0.6, "tech": 0.4}}, nil
	}
	second := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		req.Escalations[0].Scores["billing"] = 99
		return agent_v2.RouteDecision{}, nil
	}
	var seen float64
	third := func(_ context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		seen = req.Escalations[0].Scores["billing"]
		return agent_v2.RouteDecision{Target: "billing", Confidence: 1}, nil
	}
	var got *schema.Message
	billing := &runFnAgent{name: "billing", runFn: func(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		got = in
		ev := agent_v2.NewExitEvent("billing", schema.AssistantMessage("from billing"))
		if sess != nil {
			sess.AddEvent(ev)
		}
		return []*agent_v2.AgentEvent{ev}, nil
	}}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(first, 0.85),
		agent_v2.WithRouteDecider(second, 0.85),
		agent_v2.WithRouteDecider(third, 0),
	).AddBranch("billing", billing).AddBranch("tech", branchAgent("tech"))

	rec := &routeEventRecorder{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	in := schema.UserMessage("发票开错了")
	in.Annotations = map[string]any{"channel": "app"}
	in.Metadata = &schema.MessageMetadata{System: "origin"}
	in.ContentBlocks = []*schema.ContentBlock{{Type: "text", Text: &schema.TextContentBlock{Type: "text", Text: "发票开错了"}}}
	if _, err := r.Run(ctx, agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got.Content != "发票开错了" {
		t.Fatalf("a decider's change to the input content leaked into the branch: %q", got.Content)
	}
	if _, leaked := got.Annotations["tag"]; leaked || got.Metadata.System != "origin" || got.ContentBlocks[0].Type != "text" {
		t.Fatalf("a decider's change to the input annotations, metadata or content blocks leaked into the branch: %+v %+v %+v",
			got.Annotations, got.Metadata, got.ContentBlocks[0])
	}
	if seen != 0.6 {
		t.Fatalf("a later decider saw an earlier decider's change to the escalation scores: %v", seen)
	}
	if a := rec.only(t); a.Escalations[0].Scores["billing"] != 0.6 {
		t.Fatalf("the event should keep the scores decider 0 returned, got %v", a.Escalations[0].Scores)
	}
}

// TestRouteDecider_SubAgentNamespacesAreDistinct：决策方里用 RunSubAgent 跑子 agent 时，每一档
// 的子 agent 拿到各自的存档命名空间，也不与 classifier 那一档相同。节点名故意取
// "classifier"，与 classifier 那一档的命名空间后缀一样。
func TestRouteDecider_SubAgentNamespacesAreDistinct(t *testing.T) {
	var mu sync.Mutex
	var helperNS []string
	helper := &runFnAgent{name: "helper", runFn: func(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		mu.Lock()
		helperNS = append(helperNS, compose_v2.GetGraphNS(ctx))
		mu.Unlock()
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("helper", schema.AssistantMessage("ok"))}, nil
	}}
	viaHelper := func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "classifier", helper, req.Input); err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{}, nil
	}
	var classifierNS string
	cls := &runFnAgent{name: "classifier", runFn: func(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		classifierNS = compose_v2.GetGraphNS(ctx)
		ev := agent_v2.NewExitEvent("classifier", makeClassifierMsg("billing"))
		if sess != nil {
			sess.AddEvent(ev)
		}
		return []*agent_v2.AgentEvent{ev}, nil
	}}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(viaHelper, 0.5),
		agent_v2.WithRouteDecider(viaHelper, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing"))

	if _, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(helperNS) != 2 || helperNS[0] == helperNS[1] {
		t.Fatalf("each decider's sub-agent needs its own namespace, got %q", helperNS)
	}
	for _, ns := range helperNS {
		if ns == classifierNS {
			t.Fatalf("a decider's sub-agent shares the classifier's namespace %q", ns)
		}
	}
}

// TestRouteDecider_ClassifierFailureIsRecorded：classifier 没给出可用结果、转去兜底时，路由
// 决策事件记下原因，区分「classifier 挂了转兜底」与「决策方都拿不准、没有 classifier 直接
// 转兜底」。没有兜底时是报错，见 TestRouteDecider_ClassifierUnknownTargetWithoutFallbackIsAnError。
func TestRouteDecider_ClassifierFailureIsRecorded(t *testing.T) {
	t.Run("error_with_fallback", func(t *testing.T) {
		cls := &fixedAgent{name: "classifier", err: errors.New("llm quota exhausted")}
		r := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("support"),
			agent_v2.WithRoutingClassifier(cls),
			agent_v2.WithRoutingFallback(branchAgent("human")),
		).AddBranch("billing", branchAgent("billing"))

		rec := &routeEventRecorder{}
		ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
		if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		a := rec.only(t)
		if a.Stage != agent_v2.RouteStageFallback || !strings.Contains(a.ClassifierFailure, "llm quota exhausted") {
			t.Fatalf("the event should record why the classifier was passed over, got %+v", a)
		}
	})
	t.Run("unknown_target_with_fallback", func(t *testing.T) {
		r := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("support"),
			agent_v2.WithRoutingClassifier(&recordingClassifier{target: "refunds"}),
			agent_v2.WithRoutingFallback(branchAgent("human")),
		).AddBranch("billing", branchAgent("billing"))

		rec := &routeEventRecorder{}
		ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
		events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		a := rec.only(t)
		if a.Stage != agent_v2.RouteStageFallback || !strings.Contains(a.ClassifierFailure, `"refunds"`) {
			t.Fatalf("the event should record the classifier's unknown target, got %+v", a)
		}
		if out := finalContent(t, events); out != "from human" {
			t.Fatalf("an unknown target should go to the fallback branch, got %q", out)
		}
	})
	t.Run("long_unknown_target_is_truncated", func(t *testing.T) {
		r := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("support"),
			agent_v2.WithRoutingClassifier(&recordingClassifier{target: strings.Repeat("我觉得应该转给", 200)}),
			agent_v2.WithRoutingFallback(branchAgent("human")),
		).AddBranch("billing", branchAgent("billing"))

		rec := &routeEventRecorder{}
		ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
		if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if n := len([]rune(rec.only(t).ClassifierFailure)); n == 0 || n > 300 {
			t.Fatalf("the failure reason should be bounded, got %d characters", n)
		}
	})
	t.Run("success_has_no_failure", func(t *testing.T) {
		r := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("support"),
			agent_v2.WithRoutingClassifier(&recordingClassifier{target: "billing"}),
			agent_v2.WithRoutingFallback(branchAgent("human")),
		).AddBranch("billing", branchAgent("billing"))

		rec := &routeEventRecorder{}
		ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
		if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if a := rec.only(t); a.ClassifierFailure != "" || a.Target != "billing" {
			t.Fatalf("a successful classifier decision has no failure, got %+v", a)
		}
	})
}

// TestRouteDecisionOf：只认路由决策事件，别的事件类型与载荷缺失都返回 false。
func TestRouteDecisionOf(t *testing.T) {
	action := &agent_v2.RouteDecisionAction{AgentName: "support", Target: "billing"}
	if a, ok := agent_v2.RouteDecisionOf(agent_v2.Event{Type: agent_v2.RouteDecisionEventType, Metadata: map[string]any{"action": action}}); !ok || a != action {
		t.Fatalf("expected the payload back, got %v %v", a, ok)
	}
	if _, ok := agent_v2.RouteDecisionOf(agent_v2.Event{Type: "chunk", Metadata: map[string]any{"action": action}}); ok {
		t.Fatal("a different event type must not be read as a route decision")
	}
	if _, ok := agent_v2.RouteDecisionOf(agent_v2.Event{Type: agent_v2.RouteDecisionEventType}); ok {
		t.Fatal("an event without the payload must return false")
	}
	var typedNil *agent_v2.RouteDecisionAction
	if _, ok := agent_v2.RouteDecisionOf(agent_v2.Event{Type: agent_v2.RouteDecisionEventType, Metadata: map[string]any{"action": typedNil}}); ok {
		t.Fatal("a nil payload must return false")
	}
}

// TestRouteDecider_ConstructionGuards：只看调用方自己给的配置就能判定的错误，构造期 panic。
func TestRouteDecider_ConstructionGuards(t *testing.T) {
	ok := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, nil
	}
	mustPanic := func(t *testing.T, want string, fn func()) {
		t.Helper()
		defer func() {
			rec := recover()
			if rec == nil {
				t.Fatalf("expected panic containing %q", want)
			}
			if !strings.Contains(fmt.Sprint(rec), want) {
				t.Fatalf("panic should contain %q, got %v", want, rec)
			}
		}()
		fn()
	}
	t.Run("nil decider", func(t *testing.T) {
		mustPanic(t, "nil decider", func() { agent_v2.WithRouteDecider(nil, 0.5) })
	})
	for _, bad := range []float64{-0.1, 1.1, math.NaN()} {
		t.Run(fmt.Sprintf("threshold %v", bad), func(t *testing.T) {
			mustPanic(t, "within [0, 1]", func() { agent_v2.WithRouteDecider(ok, bad) })
		})
	}
	t.Run("positive threshold with nowhere to escalate", func(t *testing.T) {
		mustPanic(t, "没有地方可升级", func() {
			agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"), agent_v2.WithRouteDecider(ok, 0.85))
		})
	})
	t.Run("zero threshold needs no escalation target", func(t *testing.T) {
		agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"), agent_v2.WithRouteDecider(ok, 0))
	})
	t.Run("positive threshold then zero threshold", func(t *testing.T) {
		agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"),
			agent_v2.WithRouteDecider(ok, 0.85), agent_v2.WithRouteDecider(ok, 0))
	})
}

// TestRoutingClassifier_LLMTimeoutDegradesToFallback：classifier 自己的 LLM 请求超时、
// 调用方 ctx 还活着时，按 best-effort 降级到兜底分支，和其它 LLM 调用失败同样处理。
func TestRoutingClassifier_LLMTimeoutDegradesToFallback(t *testing.T) {
	cls := &fixedAgent{name: "classifier", err: fmt.Errorf("llm request: %w", context.DeadlineExceeded)}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingFallback(branchAgent("human")),
	).AddBranch("billing", branchAgent("billing"))

	rec := &routeEventRecorder{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("a classifier-side timeout should degrade to the fallback, got %v", err)
	}
	if out := finalContent(t, events); out != "from human" {
		t.Fatalf("should end at the fallback branch, got %q", out)
	}
	if a := rec.only(t); a.Stage != agent_v2.RouteStageFallback {
		t.Fatalf("event should record the fallback stage, got %+v", a)
	}
}

// TestRoutingClassifier_RunAbortedPropagates：classifier 返回框架判定的终局失败时原样往上抛，
// 不降级到兜底——吞掉它等于让本该停下的运行继续走下去。
func TestRoutingClassifier_RunAbortedPropagates(t *testing.T) {
	cls := &fixedAgent{name: "classifier", err: fmt.Errorf("classifier graph: %w", compose_v2.ErrRunAborted)}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingFallback(branchAgent("human")),
	).AddBranch("billing", branchAgent("billing"))

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("ErrRunAborted from the classifier should propagate, got %v", err)
	}
}

// TestRouteDecider_BranchThresholdOverridesDefault：高风险分支单独要求更高的置信度，
// 其余分支仍按这一档的默认门槛。
func TestRouteDecider_BranchThresholdOverridesDefault(t *testing.T) {
	cases := []struct {
		name   string
		dec    agent_v2.RouteDecision
		want   string
		reason string
	}{
		{"risky branch below its own threshold", agent_v2.RouteDecision{Target: "refund", Confidence: 0.8}, "from human", agent_v2.RouteReasonLowConfidence},
		{"risky branch above its own threshold", agent_v2.RouteDecision{Target: "refund", Confidence: 0.96}, "from refund", ""},
		{"ordinary branch uses the default", agent_v2.RouteDecision{Target: "tech", Confidence: 0.6}, "from tech", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
				return tc.dec, nil
			}
			r := agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("support"),
				agent_v2.WithRouteDecider(decider, 0.5, agent_v2.WithBranchMinConfidence("refund", 0.95)),
				agent_v2.WithRoutingFallback(branchAgent("human")),
			).AddBranch("refund", branchAgent("refund")).AddBranch("tech", branchAgent("tech"))

			rec := &routeEventRecorder{}
			ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
			events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if out := finalContent(t, events); out != tc.want {
				t.Fatalf("got %q, want %q", out, tc.want)
			}
			a := rec.only(t)
			if tc.reason == "" && len(a.Escalations) != 0 {
				t.Fatalf("decision should be accepted, got escalations %+v", a.Escalations)
			}
			if tc.reason != "" && (len(a.Escalations) != 1 || a.Escalations[0].Reason != tc.reason) {
				t.Fatalf("expected one %q escalation, got %+v", tc.reason, a.Escalations)
			}
		})
	}
}

// TestRouteDecider_BranchThresholdMisconfiguration：分支门槛的配置错误各自在能发现的最早
// 时刻报出来。
func TestRouteDecider_BranchThresholdMisconfiguration(t *testing.T) {
	ok := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "tech", Confidence: 1}, nil
	}
	panics := func(t *testing.T, want string, fn func()) {
		t.Helper()
		defer func() {
			rec := recover()
			if rec == nil || !strings.Contains(fmt.Sprint(rec), want) {
				t.Fatalf("expected panic containing %q, got %v", want, rec)
			}
		}()
		fn()
	}
	t.Run("out of range", func(t *testing.T) {
		panics(t, "within [0, 1]", func() { agent_v2.WithBranchMinConfidence("refund", 1.2) })
	})
	t.Run("empty branch", func(t *testing.T) {
		panics(t, "empty branch", func() { agent_v2.WithBranchMinConfidence("", 0.9) })
	})
	t.Run("same branch twice", func(t *testing.T) {
		panics(t, "set twice", func() {
			agent_v2.WithRouteDecider(ok, 0,
				agent_v2.WithBranchMinConfidence("refund", 0.9), agent_v2.WithBranchMinConfidence("refund", 0.95))
		})
	})
	t.Run("branch threshold with nowhere to escalate", func(t *testing.T) {
		panics(t, "没有地方可升级", func() {
			agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("support"),
				agent_v2.WithRouteDecider(ok, 0, agent_v2.WithBranchMinConfidence("refund", 0.9)))
		})
	})
	t.Run("unknown branch fails at Run", func(t *testing.T) {
		r := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("support"),
			agent_v2.WithRouteDecider(ok, 0.5, agent_v2.WithBranchMinConfidence("refnud", 0.9)),
			agent_v2.WithRoutingFallback(branchAgent("human")),
		).AddBranch("refund", branchAgent("refund")).AddBranch("tech", branchAgent("tech"))
		_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
		if err == nil || !strings.Contains(err.Error(), `unknown branch "refnud"`) {
			t.Fatalf("a threshold for an unregistered branch should fail the run, got %v", err)
		}
	})
}

// flakyDecider 模拟判定不完全确定的决策模型：第一次对 billing 拿不准，第二次对 tech
// 很有把握。Resume 时若把它重新调一次，就会拿到和第一次不一样的结论。
func flakyDecider(calls *int) agent_v2.RouteDecider {
	return func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		*calls++
		if *calls == 1 {
			return agent_v2.RouteDecision{Target: "billing", Confidence: 0.8, Scores: map[string]float64{"billing": 0.55, "tech": 0.45}}, nil
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.9}, nil
	}
}

// TestRouteDecider_ResumeHonorsTheUsersAnswer：第一档拿不准、第二档问了用户，Resume 时
// 必须按用户的回答路由。第一档若被重新调用而这次越过了门槛，用户的回答会被悄悄丢掉。
func TestRouteDecider_ResumeHonorsTheUsersAnswer(t *testing.T) {
	var calls int
	askUser := func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "哪一类？", Options: []string{"billing", "tech"}})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(flakyDecider(&calls), 0.85),
		agent_v2.WithRouteDecider(askUser, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the second stage to ask the user, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("billing"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" {
		t.Fatalf("the user answered billing, but the request went elsewhere: %q (first decider called %d times)", out, calls)
	}
}

// askingClassifier 是一个会问用户的 classifier：第一次调用抛中断，拿到答案后把答案当路由目标。
type askingClassifier struct{ runs int }

func (c *askingClassifier) Run(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	c.runs++
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "哪一类？", Options: []string{"billing", "tech"}})
	if err != nil {
		return nil, err
	}
	ev := agent_v2.NewExitEvent("classifier", makeClassifierMsg(strings.TrimSpace(ans.Content)))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}
func (c *askingClassifier) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *askingClassifier) Name() string        { return "classifier" }
func (c *askingClassifier) Description() string { return "" }

// TestRouteDecider_ResumeKeepsTheClassifiersQuestion：决策方拿不准、交给 classifier，
// classifier 问了用户。Resume 时 classifier 的提问必须得到兑现，不能因为前面的决策方
// 这次越过了门槛就被绕过。
func TestRouteDecider_ResumeKeepsTheClassifiersQuestion(t *testing.T) {
	var calls int
	cls := &askingClassifier{}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(flakyDecider(&calls), 0.85),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the classifier to ask the user, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("billing"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" {
		t.Fatalf("the classifier's question was answered with billing, but the request went elsewhere: %q (first decider called %d times, classifier ran %d times)",
			out, calls, cls.runs)
	}
}

// TestRouteDecider_RetryAfterFailureCallsDecidersAgain：决策方一时报错、又无处升级时整次
// 路由失败；之后重试失败的节点（Resume 不带答案，或外层编排的自动重试），决策方必须被
// 重新调用。报错的档不算跑完——把报错当成已完成来重放，会让一次临时故障变成永久失败。
func TestRouteDecider_RetryAfterFailureCallsDecidersAgain(t *testing.T) {
	var calls int
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		calls++
		if calls == 1 {
			return agent_v2.RouteDecision{}, errors.New("decision service returned 503")
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.9}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := r.Run(ctx, sess, schema.UserMessage("hi")); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("the first attempt should fail with the decider error, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("retrying after a transient decider failure should succeed, got %v (decider called %d times)", err, calls)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("retry should route by the decider's fresh answer, got %q", out)
	}
	if calls != 2 {
		t.Fatalf("the decider must be called again on retry, called %d times", calls)
	}
}

// TestRouteDecider_AnswerDoesNotLeakAcrossDeciders：每一档决策方的提问要有各自的身份。
//
// 第 0 档问「转账吗？」，用户答「是」。Resume 时第 0 档这次在提问之前就失败了（比如远端
// 超时），升级到第 1 档，第 1 档问的是另一件事「删除账户吗？」。如果两档共用同一个提问
// 计数器，第 1 档算出的 ID 与上一问相同，会直接拿到「是」而把请求送去删账户。
func TestRouteDecider_AnswerDoesNotLeakAcrossDeciders(t *testing.T) {
	var firstCalls int
	transfer := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		firstCalls++
		if firstCalls > 1 {
			return agent_v2.RouteDecision{}, errors.New("remote check timed out")
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "转账吗？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if strings.TrimSpace(ans.Content) == "是" {
			return agent_v2.RouteDecision{Target: "transfer", Confidence: 1}, nil
		}
		return agent_v2.RouteDecision{}, nil
	}
	deleteAccount := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "删除账户吗？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if strings.TrimSpace(ans.Content) == "是" {
			return agent_v2.RouteDecision{Target: "delete_account", Confidence: 1}, nil
		}
		return agent_v2.RouteDecision{Target: "chat", Confidence: 1}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(transfer, 0.5),
		agent_v2.WithRouteDecider(deleteAccount, 0),
	).AddBranch("transfer", branchAgent("transfer")).AddBranch("delete_account", branchAgent("delete_account")).AddBranch("chat", branchAgent("chat"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.AllQueries()[0].Question != "转账吗？" {
		t.Fatalf("expected the first decider to ask about the transfer, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("是"),
	}))
	if err == nil {
		t.Fatalf("the answer to the transfer question must not be reused for a different question; routed to %q", finalContent(t, events))
	}
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.AllQueries()[0].Question != "删除账户吗？" {
		t.Fatalf("the second decider should ask its own question, got %v", err)
	}
}

// TestRouteDecider_RetryDoesNotBindOldAnswersToNewQuestions：以失败收尾后重新调用的档要问
// 新问题。第 0 档第一次建议了不存在的 refunds，第 1 档请用户确认、用户答「是」，链以
// unknown_target 失败；重试时第 0 档改口 delete_account，第 1 档问的已经是另一件事。
// 调用方照常带着上次的答案 Resume，那个「是」不能被套到新问题上把请求送去注销账户。
func TestRouteDecider_RetryDoesNotBindOldAnswersToNewQuestions(t *testing.T) {
	var suggestCalls int
	suggest := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		suggestCalls++
		if suggestCalls == 1 {
			return agent_v2.RouteDecision{Target: "refunds", Confidence: 0.5}, nil
		}
		return agent_v2.RouteDecision{Target: "delete_account", Confidence: 0.5}, nil
	}
	confirm := func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		prev := req.Escalations[len(req.Escalations)-1]
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "要为您转到 " + prev.Target + " 吗？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if strings.TrimSpace(ans.Content) == "是" {
			return agent_v2.RouteDecision{Target: prev.Target, Confidence: 1}, nil
		}
		return agent_v2.RouteDecision{}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(suggest, 0.9),
		agent_v2.WithRouteDecider(confirm, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("delete_account", branchAgent("delete_account"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("帮我处理一下"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the confirming decider to ask, got %v", err)
	}
	answers := map[string]*schema.Message{first.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	var ue *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &ue) {
		t.Fatalf("confirming a branch that does not exist should fail the chain, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err == nil {
		t.Fatalf("the old yes must not confirm a different question: routed to %q", finalContent(t, events))
	}
	second, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the retry should ask the new question, got %v", err)
	}
	q := second.AllQueries()[0]
	if q.InterruptID == first.AllQueries()[0].InterruptID || !strings.Contains(q.Question, "delete_account") {
		t.Fatalf("the retry should ask a new question with a new id, got %+v", q)
	}
}

// flakyEchoModel 第一次调用报错；之后把最后一条 user 消息里「user said: 」后面的内容当答复。
type flakyEchoModel struct {
	mu     sync.Mutex
	calls  int
	inputs []string
}

func (m *flakyEchoModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	var last string
	for _, msg := range msgs {
		if msg != nil && msg.Role == schema.RoleUser {
			last = msg.Content
		}
	}
	m.inputs = append(m.inputs, last)
	if m.calls == 1 {
		return nil, errors.New("model service returned 503")
	}
	return schema.AssistantMessage(strings.TrimPrefix(last, "user said: ")), nil
}

func (m *flakyEchoModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *flakyEchoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestRouteDecider_RetriedDeciderSubAgentStartsFresh：决策方问用户、把回答交给 RunSubAgent
// 跑的子 agent；子 agent 第一次报错让整条链失败。重试时决策方被重新调用、问到了用户的新
// 回答，子 agent 必须按新回答从头跑，不能从上一次的失败现场带着旧回答续跑。
func TestRouteDecider_RetriedDeciderSubAgentStartsFresh(t *testing.T) {
	pick := &flakyEchoModel{}
	picker := agent_v2.NewBasicAgent(agent_v2.WithName("picker"), agent_v2.WithModel(pick))
	decider := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		events, err := agent_v2.RunSubAgent(ctx, "picker", picker, schema.UserMessage("user said: "+strings.TrimSpace(ans.Content)))
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(agent_v2.FinalMessage(events).Content), Confidence: 1}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the decider to ask, got %v", err)
	}
	var ue *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		first.AllQueries()[0].InterruptID: schema.UserMessage("billing"),
	})); !errors.As(err, &ue) {
		t.Fatalf("the failing sub-agent should fail the chain, got %v", err)
	}
	_, err = r.Resume(ctx, sess.RunID)
	second, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the retried decider should ask again, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		second.AllQueries()[0].InterruptID: schema.UserMessage("tech"),
	}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" {
		t.Fatalf("the sub-agent should act on the new answer, routed to %q (model saw %q)", out, pick.inputs)
	}
}

// TestRouteDecider_ClassifierFailureRetryAsksAgain：classifier 没给出候选分支让链失败后，
// 重新调用的提问档也要问新问题，不能拿上一次的答案直接往下走。
func TestRouteDecider_ClassifierFailureRetryAsksAgain(t *testing.T) {
	broken := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, errors.New("decision service returned 503")
	}
	ask := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if strings.TrimSpace(ans.Content) == "不确定" {
			return agent_v2.RouteDecision{}, nil
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1}, nil
	}
	single := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r3", Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"tech"}`}},
	})
	cls := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("我也说不准"), single})),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(3),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(broken, 0.5),
		agent_v2.WithRouteDecider(ask, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the asking decider to ask, got %v", err)
	}
	answers := map[string]*schema.Message{first.AllQueries()[0].InterruptID: schema.UserMessage("不确定")}
	var undecided *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &undecided) {
		t.Fatalf("a classifier that names no branch should fail the chain, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	second, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the re-called asking decider should ask again instead of reusing the old answer; err %v, events %d", err, len(events))
	}
	if second.AllQueries()[0].InterruptID == first.AllQueries()[0].InterruptID {
		t.Fatal("the question after the retry needs a new id")
	}
}

// TestRouteDecider_RetryGenerationSurvivesResume：以失败收尾后重试，重新调用的档又被中断
// 时，续跑要沿用这一次的身份，用户对这次提问的回答才能兑现。
func TestRouteDecider_RetryGenerationSurvivesResume(t *testing.T) {
	var flakyCalls int
	flaky := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		flakyCalls++
		if flakyCalls == 1 {
			return agent_v2.RouteDecision{}, errors.New("decision service returned 503")
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.2}, nil
	}
	ask := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{Target: strings.TrimSpace(ans.Content), Confidence: 1}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(flaky, 0.5),
		agent_v2.WithRouteDecider(ask, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the asking decider to ask, got %v", err)
	}
	var ue *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		first.AllQueries()[0].InterruptID: schema.UserMessage("refunds"),
	})); !errors.As(err, &ue) {
		t.Fatalf("an answer that is not a branch should fail the chain, got %v", err)
	}
	_, err = r.Resume(ctx, sess.RunID)
	second, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the retry should ask again, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		second.AllQueries()[0].InterruptID: schema.UserMessage("billing"),
	}))
	if err != nil {
		t.Fatalf("the answer to the retried question must be honored, got %v", err)
	}
	if out := finalContent(t, events); out != "from billing" {
		t.Fatalf("expected billing, got %q", out)
	}
}

// TestRouteDecider_ClassifierUnknownTargetWithoutFallbackIsAnError：classifier 没给出候选
// 分支、又没有兜底时返回 RouteUndecidedError，不发路由决策事件、不跑任何分支。之前这里
// 路由到 End 且 err == nil，只能从事件里看出 classifier 失败了。
func TestRouteDecider_ClassifierUnknownTargetWithoutFallbackIsAnError(t *testing.T) {
	for _, target := range []string{"refunds", strings.Repeat("我觉得应该转给", 200)} {
		r := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("support"),
			agent_v2.WithRoutingClassifier(&recordingClassifier{target: target}),
		).AddBranch("billing", branchAgent("billing"))

		rec := &routeEventRecorder{}
		ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
		events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
		var undecided *agent_v2.RouteUndecidedError
		if !errors.As(err, &undecided) {
			t.Fatalf("a classifier without a usable target and no fallback should fail the run, got err=%v events=%d", err, len(events))
		}
		if !strings.Contains(undecided.ClassifierFailure, "unknown target") {
			t.Fatalf("the error should say why the classifier failed, got %q", undecided.ClassifierFailure)
		}
		if n := len([]rune(undecided.ClassifierFailure)); n > 300 {
			t.Fatalf("the failure reason should be bounded, got %d characters", n)
		}
		if len(rec.actions) != 0 {
			t.Fatalf("a failed route is not a routing decision, got %d route_decision events", len(rec.actions))
		}
		if compose_v2.IsRetryable(err) {
			t.Fatal("the default retry predicate should not retry an undecided route")
		}
	}
}

// TestRouteDecider_ClassifierTimeoutWithoutFallbackStaysOffTheErrorChain：classifier 自己的
// 请求超时（调用方的 ctx 还活着）导致失败时，超时不能漏到错误链上：按错误链判断的地方会
// 当成调用方的 ctx 到期，存档被标成挂起而不是失败。它只在 LastErr 里。
func TestRouteDecider_ClassifierTimeoutWithoutFallbackStaysOffTheErrorChain(t *testing.T) {
	cls := &fixedAgent{name: "classifier", err: fmt.Errorf("classifier request: %w", context.DeadlineExceeded)}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing"))

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	var undecided *agent_v2.RouteUndecidedError
	if !errors.As(err, &undecided) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("the classifier's own timeout leaked onto the error chain")
	}
	if !errors.Is(undecided.LastErr, context.DeadlineExceeded) || !strings.Contains(undecided.ClassifierFailure, "classifier request") {
		t.Fatalf("the classifier's error should be kept in the error, got LastErr=%v failure=%q", undecided.LastErr, undecided.ClassifierFailure)
	}
	saved, lerr := cp.LoadNS(ctx, sess.RunID, "")
	if lerr != nil || saved == nil || saved.Status != compose_v2.CPStatusFailed {
		t.Fatalf("the checkpoint should record a failure, not a pause: cp=%+v err=%v", saved, lerr)
	}
}

// TestRouteDecider_ClassifierFailureWithoutFallbackReplaysDeciders：classifier 没兜底失败后
// 重试时，决策方原样重放、只重判 classifier。
func TestRouteDecider_ClassifierFailureWithoutFallbackReplaysDeciders(t *testing.T) {
	var deciderCalls int
	unsure := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		deciderCalls++
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.2}, nil
	}
	single := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"tech"}`}},
	})
	cls := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("我也说不准"), single})),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(3),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(unsure, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	var undecided *agent_v2.RouteUndecidedError
	if _, err := r.Run(ctx, sess, schema.UserMessage("hi")); !errors.As(err, &undecided) {
		t.Fatalf("a classifier that names no branch should fail the run, got %v", err)
	}
	if len(undecided.Escalations) != 1 || undecided.Escalations[0].Reason != agent_v2.RouteReasonLowConfidence {
		t.Fatalf("the error should carry the decider's result, got %+v", undecided.Escalations)
	}
	events, err := r.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" || deciderCalls != 1 {
		t.Fatalf("only the classifier should run again: routed %q, decider called %d times", out, deciderCalls)
	}
}

// TestRouteDecider_ClassifierFailureWithoutFallbackAsksAgainWithReusedToolCallID：classifier
// 问过用户、拿到答案后没给出候选分支让链失败，重试时它换了问题，用户必须被重新问到。
// 模型复用同一个 tool_call ID 时，提问身份只剩命名空间这一维；这条出口若不加失败重试代数，
// 上一次的「是」会被套到这次的问题上。
func TestRouteDecider_ClassifierFailureWithoutFallbackAsksAgainWithReusedToolCallID(t *testing.T) {
	askRefund := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "q1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"您是要退款吗？"}`}},
	})
	askDelete := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "q1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"您是要注销账户吗？"}`}},
	})
	routeDelete := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"delete_account"}`}},
	})
	cls := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{askRefund, schema.AssistantMessage("我也说不准"), askDelete, routeDelete})),
		agent_v2.WithAskUser(),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(5),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("delete_account", branchAgent("delete_account"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("帮我处理一下"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the classifier to ask, got %v", err)
	}
	answers := map[string]*schema.Message{first.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	var undecided *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &undecided) {
		t.Fatalf("a classifier that names no branch should fail the chain, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err == nil {
		t.Fatalf("the old yes must not answer the new question: routed to %q", finalContent(t, events))
	}
	second, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the retried classifier should ask again, got %v", err)
	}
	q := second.AllQueries()[0]
	if q.InterruptID == first.AllQueries()[0].InterruptID || !strings.Contains(q.Question, "注销账户") {
		t.Fatalf("the retried classifier should ask its new question with a new id, got %+v", q)
	}
}

// scriptedModel 按顺序执行脚本：*schema.Message 照常返回，error 让这次调用失败。脚本用完后
// 返回一条固定文本。
type scriptedModel struct {
	mu    sync.Mutex
	steps []any
	calls int
}

func (m *scriptedModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls >= len(m.steps) {
		return schema.AssistantMessage("[script exhausted]"), nil
	}
	step := m.steps[m.calls]
	m.calls++
	if err, ok := step.(error); ok {
		return nil, err
	}
	return step.(*schema.Message), nil
}

func (m *scriptedModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *scriptedModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *scriptedModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func askMsg(id, question string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: id, Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: fmt.Sprintf(`{"question":%q}`, question)}},
	})
}

func routeMsg(id, target string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: id, Type: "function", Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: fmt.Sprintf(`{"target":%q}`, target)}},
	})
}

// TestRouteDecider_ClassifierFailureRetriesTheErroredDeciderFirst：决策方报错、classifier 又没
// 给出目标时，重试从报错的决策方开始：它换新身份重新调用，这次拿定了主意就不再走到 classifier。
// LastErr 是决策方的错误——classifier 没报错，只是没给目标。
func TestRouteDecider_ClassifierFailureRetriesTheErroredDeciderFirst(t *testing.T) {
	deciderErr := errors.New("decision service returned 503")
	var ids []string
	decider := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ids = append(ids, compose_v2.GetCurrentToolCallID(ctx))
		if len(ids) == 1 {
			return agent_v2.RouteDecision{}, deciderErr
		}
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9}, nil
	}
	m := &scriptedModel{steps: []any{schema.AssistantMessage("我也说不准"), routeMsg("r1", "tech")}}
	cls := agent_v2.NewBasicAgent(agent_v2.WithName("classifier"), agent_v2.WithModel(m), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	var undecided *agent_v2.RouteUndecidedError
	if !errors.As(err, &undecided) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	if !errors.Is(undecided.LastErr, deciderErr) {
		t.Fatalf("only the decider errored, so LastErr should be its error, got %v", undecided.LastErr)
	}
	events, err := r.Resume(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out := finalContent(t, events); out != "from billing" || m.callCount() != 1 {
		t.Fatalf("the errored decider should be called again and decide: routed %q, classifier model called %d times", out, m.callCount())
	}
	if strings.Join(ids, ",") != "route_decider_0,route_decider_0_retry1" {
		t.Fatalf("the re-called decider should carry a new identity, got %v", ids)
	}
}

// TestRouteDecider_ClassifierErrorIsTheLastErr：决策方与 classifier 都报错时，LastErr 是最后
// 一个报错的档，也就是 classifier 的错误。
func TestRouteDecider_ClassifierErrorIsTheLastErr(t *testing.T) {
	deciderErr := errors.New("decider sentinel")
	classifierErr := errors.New("classifier sentinel")
	decider := func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, fmt.Errorf("decision service: %w", deciderErr)
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.5),
		agent_v2.WithRoutingClassifier(&fixedAgent{name: "classifier", err: fmt.Errorf("llm 429: %w", classifierErr)}),
	).AddBranch("billing", branchAgent("billing"))

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var undecided *agent_v2.RouteUndecidedError
	if !errors.As(err, &undecided) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	if !errors.Is(undecided.LastErr, classifierErr) || errors.Is(undecided.LastErr, deciderErr) {
		t.Fatalf("LastErr should be the classifier's error, the last stage that errored, got %v", undecided.LastErr)
	}
}

// TestRouteDecider_RetryGenerationKeepsIncreasing：连续以失败收尾时，失败重试代数逐次递增，
// 每一次重试的 classifier 都在新的命名空间里提问。代数若不再增长（比如被写成「重试过没有」
// 的旗标），第三次的问题会和第二次撞同一个提问 ID，第二次的「是」就把请求送去注销账户。
func TestRouteDecider_RetryGenerationKeepsIncreasing(t *testing.T) {
	m := &scriptedModel{steps: []any{
		askMsg("q1", "您是要退款吗？"), schema.AssistantMessage("我也说不准"),
		askMsg("q1", "您是要查账单吗？"), schema.AssistantMessage("还是说不准"),
		askMsg("q1", "您是要注销账户吗？"), routeMsg("r1", "delete_account"),
	}}
	cls := agent_v2.NewBasicAgent(agent_v2.WithName("classifier"), agent_v2.WithModel(m),
		agent_v2.WithAskUser(), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(5))
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("delete_account", branchAgent("delete_account"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	seen := map[string]bool{}
	_, err := r.Run(ctx, sess, schema.UserMessage("帮我处理一下"))
	for round, want := range []string{"退款", "查账单", "注销账户"} {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("round %d: the classifier should ask about %s, got err=%v", round, want, err)
		}
		q := ie.AllQueries()[0]
		if seen[q.InterruptID] || !strings.Contains(q.Question, want) {
			t.Fatalf("round %d: expected a new question about %s with a new id, got %+v", round, want, q)
		}
		seen[q.InterruptID] = true
		// 调用方把历次答案都带着，旧答案不该满足新问题。
		answers[q.InterruptID] = schema.UserMessage("是")
		if round == 2 {
			break
		}
		var undecided *agent_v2.RouteUndecidedError
		if _, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &undecided) {
			t.Fatalf("round %d: the classifier names no branch, the chain should fail, got %v", round, err)
		}
		events, rerr := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
		if rerr == nil {
			t.Fatalf("round %d retry: an old answer satisfied the new question, routed to %q", round, finalContent(t, events))
		}
		err = rerr
	}
}

// askThenFailClassifier 是自己提问、自己不留存档的 classifier，形如业务自研或远程的 classifier：
// 前两次调用问「退款吗」、拿到答案后报错；之后问「注销账户吗」，答「是」就路由到 delete_account。
type askThenFailClassifier struct {
	mu   sync.Mutex
	runs int
}

func (c *askThenFailClassifier) Run(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	c.mu.Lock()
	c.runs++
	firstAttempt := c.runs <= 2
	c.mu.Unlock()
	if firstAttempt {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您是要退款吗？"}); err != nil {
			return nil, err
		}
		return nil, errors.New("refund service returned 503")
	}
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您是要注销账户吗？"})
	if err != nil {
		return nil, err
	}
	target := "billing"
	if strings.TrimSpace(ans.Content) == "是" {
		target = "delete_account"
	}
	ev := agent_v2.NewExitEvent("classifier", makeClassifierMsg(target))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}

func (c *askThenFailClassifier) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *askThenFailClassifier) Name() string        { return "classifier" }
func (c *askThenFailClassifier) Description() string { return "" }

// TestRouteDecider_ClassifierErrorWithoutFallbackAsksAgain：classifier 问过用户、拿到答案后
// 报错让链失败，重试时它换新身份：换了的问题拿新的提问 ID，不会被上一次的「是」满足。与
// 「没给出目标」那条子路径同一个出口，这里专测报错那条。
//
// classifier 用自己不留存档的那种，不用 BasicAgent：BasicAgent 会从自己的失败存档接着跑，
// 第二个问题落在工具节点的下一次访问上，提问 ID 天然不同，换不换身份都测不出来。自研或
// 远程的 classifier 没有这层进度，命名空间里的代数是它唯一的区分。
func TestRouteDecider_ClassifierErrorWithoutFallbackAsksAgain(t *testing.T) {
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRoutingClassifier(&askThenFailClassifier{}),
	).AddBranch("billing", branchAgent("billing")).AddBranch("delete_account", branchAgent("delete_account"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("帮我处理一下"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the classifier to ask, got %v", err)
	}
	answers := map[string]*schema.Message{first.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	var undecided *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &undecided) || undecided.LastErr == nil {
		t.Fatalf("a classifier error without fallback should fail the chain with the error kept, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err == nil {
		t.Fatalf("the old yes must not answer the new question: routed to %q", finalContent(t, events))
	}
	second, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("the retried classifier should ask again, got %v", err)
	}
	if q := second.AllQueries()[0]; q.InterruptID == first.AllQueries()[0].InterruptID || !strings.Contains(q.Question, "注销账户") {
		t.Fatalf("the retried classifier should ask its new question with a new id, got %+v", q)
	}
}

// TestRouteDecider_ClassifierErrorDoesNotReaskAnsweredDecider：决策方问过用户、拿到答案后
// 拿不准，classifier 调用又报错让链失败。重试时决策方原样重放，用户不会被再问一遍；只重新
// 调用 classifier。
func TestRouteDecider_ClassifierErrorDoesNotReaskAnsweredDecider(t *testing.T) {
	var deciderCalls int
	decider := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		deciderCalls++
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		_ = ans // 答案只用来让决策方走完这一档；它照样拿不准，交给 classifier。
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.2}, nil
	}
	m := &scriptedModel{steps: []any{errors.New("llm 503"), routeMsg("r1", "tech")}}
	cls := agent_v2.NewBasicAgent(agent_v2.WithName("classifier"), agent_v2.WithModel(m), agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decider, 0.5),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected the decider to ask, got %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("账单")}
	var undecided *agent_v2.RouteUndecidedError
	if _, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &undecided) {
		t.Fatalf("a classifier error without fallback should fail the chain, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out := finalContent(t, events); out != "from tech" || deciderCalls != 2 {
		t.Fatalf("the answered decider should be replayed, not asked again: routed %q, decider called %d times", out, deciderCalls)
	}
}

// TestRouteDecider_DeciderGenerationKeepsIncreasing：决策方提问的链连续以失败收尾时，决策方的
// 调用身份也逐代递增，每一次重试的提问都拿新的 ID。只钉 classifier 那一侧不够：决策方的代数
// 若封顶，第三问会和第二问撞同一个 ID，第二次的「是」把请求送去注销账户。
func TestRouteDecider_DeciderGenerationKeepsIncreasing(t *testing.T) {
	suggestions := []string{"refunds", "refunds_v2", "delete_account"}
	var suggestCalls int
	suggest := func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		s := suggestions[min(suggestCalls, len(suggestions)-1)]
		suggestCalls++
		return agent_v2.RouteDecision{Target: s, Confidence: 0.5}, nil
	}
	confirm := func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		prev := req.Escalations[len(req.Escalations)-1]
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "要为您转到 " + prev.Target + " 吗？"})
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if strings.TrimSpace(ans.Content) == "是" {
			return agent_v2.RouteDecision{Target: prev.Target, Confidence: 1}, nil
		}
		return agent_v2.RouteDecision{}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(suggest, 0.9),
		agent_v2.WithRouteDecider(confirm, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("delete_account", branchAgent("delete_account"))

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	_, err := r.Run(ctx, sess, schema.UserMessage("帮我处理一下"))
	for round, want := range []string{"refunds", "refunds_v2", "delete_account"} {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("round %d: expected the confirming decider to ask about %s, got err=%v", round, want, err)
		}
		q := ie.AllQueries()[0]
		if _, seen := answers[q.InterruptID]; seen || !strings.Contains(q.Question, want) {
			t.Fatalf("round %d: expected a new question about %s with a new id, got %+v", round, want, q)
		}
		answers[q.InterruptID] = schema.UserMessage("是")
		if round == 2 {
			return
		}
		var ue *agent_v2.RouteUndecidedError
		if _, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); !errors.As(err, &ue) {
			t.Fatalf("round %d: confirming a branch that does not exist should fail the chain, got %v", round, err)
		}
		events, rerr := r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
		if rerr == nil {
			t.Fatalf("round %d retry: an old yes answered the new question: routed to %q", round, finalContent(t, events))
		}
		err = rerr
	}
}

// TestRouteDecider_RetryStartsFromTheFirstErroredDecider：两档决策方都报错时，重试从第一个
// 报错的档开始重新调用。若从后一个报错的档开始，前一档的临时故障会被当成已完成重放，请求按
// 后一档的选择路由且不报错。
func TestRouteDecider_RetryStartsFromTheFirstErroredDecider(t *testing.T) {
	var d0Calls, d1Calls int
	d0 := func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		d0Calls++
		if d0Calls == 1 {
			return agent_v2.RouteDecision{}, errors.New("d0 503")
		}
		return agent_v2.RouteDecision{Target: "billing", Confidence: 0.9}, nil
	}
	d1 := func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		d1Calls++
		if d1Calls == 1 {
			return agent_v2.RouteDecision{}, errors.New("d1 429")
		}
		return agent_v2.RouteDecision{Target: "tech", Confidence: 0.9}, nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(d0, 0.5),
		agent_v2.WithRouteDecider(d1, 0),
	).AddBranch("billing", branchAgent("billing")).AddBranch("tech", branchAgent("tech"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	var ue *agent_v2.RouteUndecidedError
	if _, err := r.Run(ctx, sess, schema.UserMessage("hi")); !errors.As(err, &ue) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	events, err := r.Resume(ctx, sess.RunID)
	out := ""
	if err == nil {
		out = finalContent(t, events)
	}
	if err != nil || out != "from billing" || d0Calls != 2 {
		t.Fatalf("retry should re-call the first errored decider and route by it: err=%v routed=%q d0=%d d1=%d", err, out, d0Calls, d1Calls)
	}
}

// TestRouteDecider_ReplayedDeciderErrorStaysInLastErr：前一档报错、后一档问过用户后弃权时，
// 续跑时那个报错的档是重放出来的，它的错误仍要回填进 LastErr，而且是原错误的文本（godoc 写明
// 重放出来的只剩文本），不是原因码。
func TestRouteDecider_ReplayedDeciderErrorStaysInLastErr(t *testing.T) {
	d0 := func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, errors.New("jev 503")
	}
	d1 := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"}); err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{}, nil // 弃权
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(d0, 0.5),
		agent_v2.WithRouteDecider(d1, 0),
	).AddBranch("billing", branchAgent("billing"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected d1 to ask, got %v", err)
	}
	_, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("不知道"),
	}))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	if ue.LastErr == nil || ue.LastErr.Error() != "jev 503" {
		t.Fatalf("decider 0 errored in this routing (replayed on resume); LastErr should carry its text, got %v", ue.LastErr)
	}
}

// TestRouteDecider_LastErrIsTheLatestErroredStage：续跑时前一档的报错是重放出来的、后一档又新
// 报了错，LastErr 是后一档的原始错误——godoc 承诺的是「最后一个报错的档」。重放的错误若在循环
// 结束后才回填，会盖掉后一档新报的错。
func TestRouteDecider_LastErrIsTheLatestErroredStage(t *testing.T) {
	d1Err := errors.New("decider 1 overloaded")
	d0 := func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{}, errors.New("jev 503")
	}
	d1 := func(ctx context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "您要办哪类业务？"}); err != nil {
			return agent_v2.RouteDecision{}, err
		}
		return agent_v2.RouteDecision{}, fmt.Errorf("decider 1: %w", d1Err)
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(d0, 0.5),
		agent_v2.WithRouteDecider(d1, 0),
	).AddBranch("billing", branchAgent("billing"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected decider 1 to ask, got %v", err)
	}
	_, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("账单"),
	}))
	var ue *agent_v2.RouteUndecidedError
	if !errors.As(err, &ue) {
		t.Fatalf("expected RouteUndecidedError, got %v", err)
	}
	if !errors.Is(ue.LastErr, d1Err) {
		t.Fatalf("LastErr should be the error of the last stage that errored (decider 1), got %v", ue.LastErr)
	}
}
