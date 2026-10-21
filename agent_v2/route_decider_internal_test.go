package agent_v2

import (
	"context"
	"testing"

	"crew-go/schema"
)

// routeDeciderTestAgent 是只为凑够 AddBranch 参数的最小 Agent，这里不会真的跑它。
type routeDeciderTestAgent struct{}

func (routeDeciderTestAgent) Run(context.Context, *RunSession, *schema.Message, ...RunOption) ([]*AgentEvent, error) {
	return nil, nil
}
func (routeDeciderTestAgent) Resume(context.Context, string, ...ResumeOption) ([]*AgentEvent, error) {
	return nil, nil
}
func (routeDeciderTestAgent) Name() string        { return "branch" }
func (routeDeciderTestAgent) Description() string { return "" }

// TestDecisionRouter_DeciderRunsWithoutHistory：决策方是内部决策，框架调它时必须已经
// 摘掉会话历史——WithoutHistoryCtx 的 godoc 把它列为「框架已经摘过」的一处，业务据此
// 不再自己摘。决策方里若跑子 agent 且框架没摘，决策过程会混进用户的会话历史。
func TestDecisionRouter_DeciderRunsWithoutHistory(t *testing.T) {
	var suppressed, called bool
	decider := func(ctx context.Context, _ RouteRequest) (RouteDecision, error) {
		called = true
		suppressed = historySuppressed(ctx)
		return RouteDecision{Target: "a", Confidence: 1}, nil
	}
	r := NewRoutingAgent(WithRoutingName("support"), WithRouteDecider(decider, 0))
	r.AddBranch("a", routeDeciderTestAgent{})

	route := r.decisionRouter(NewRunSession())
	target, err := route(context.Background(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if target != "a" {
		t.Fatalf("target = %q, want a", target)
	}
	if !called {
		t.Fatal("decider was not called")
	}
	if !suppressed {
		t.Fatal("the framework must call deciders with history suppressed")
	}
}
