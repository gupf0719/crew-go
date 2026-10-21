package decision_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/decision"
	"crew-go/decision/jev"
	"crew-go/schema"
)

// fakeJev 是读入参的假 System One 服务：记下每次收到的请求，按 reply 回一个 choice 答案。
type fakeJev struct {
	mu       sync.Mutex
	requests []map[string]any
	reply    func(req map[string]any) string
}

func (f *fakeJev) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req map[string]any
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()
	w.Write([]byte(f.reply(req))) //nolint:errcheck // 测试服务，写失败时客户端那边会报
}

func (f *fakeJev) last(t *testing.T) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		t.Fatal("fake Jev received no request")
	}
	return f.requests[len(f.requests)-1]
}

func choiceReply(choice string, confidence float64) func(map[string]any) string {
	return func(map[string]any) string {
		return fmt.Sprintf(`{"model":"jev-1.13.0","answers":{"route":{"type":"choice","choice":%q,"confidence":%v,"probabilities":{"billing":0.1,"tech":0.9}}},"usage":{"input_tokens":120,"output_tokens":20}}`,
			choice, confidence)
	}
}

// TestRouteDecider_MapsCandidatesAndAnswer：候选分支变成 choice 选项，没描述的用分支名，
// 默认用输入文本作 state、用默认问题描述；答案映射成带置信度与概率的决定。
func TestRouteDecider_MapsCandidatesAndAnswer(t *testing.T) {
	fake := &fakeJev{reply: choiceReply("tech", 0.93)}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	decide := decision.NewRouteDecider(jev.NewClient("k", decision.WithBaseURL(srv.URL)))
	dec, err := decide(context.Background(), agent_v2.RouteRequest{
		Input: schema.UserMessage("app 一直闪退"),
		Candidates: []agent_v2.RoutingChoice{
			{Name: "billing", Description: "账单与发票"},
			{Name: "tech"},
		},
	})
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if dec.Target != "tech" || dec.Confidence != 0.93 || dec.Scores["tech"] != 0.9 || dec.Source != "jev-1.13.0" {
		t.Fatalf("decision mapped wrong: %+v", dec)
	}

	req := fake.last(t)
	if req["state"] != "app 一直闪退" {
		t.Errorf("state should default to the input text, got %v", req["state"])
	}
	q := req["questions"].(map[string]any)["route"].(map[string]any)
	if q["type"] != "choice" || q["instructions"] != decision.DefaultRouteInstructions {
		t.Errorf("route question wrong: %v", q)
	}
	crit := q["criteria"].(map[string]any)
	if len(crit) != 2 || crit["billing"] != "账单与发票" || crit["tech"] != "tech" {
		t.Errorf("criteria should list every candidate, using the name when there is no description; got %v", crit)
	}
}

// TestRouteDecider_CustomInstructionsAndState：自定义问题描述与结构化 state 原样发给 Jev。
func TestRouteDecider_CustomInstructionsAndState(t *testing.T) {
	fake := &fakeJev{reply: choiceReply("billing", 0.99)}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	decide := decision.NewRouteDecider(jev.NewClient("k", decision.WithBaseURL(srv.URL)),
		decision.WithInstructions("这条消息该交给哪个处理方？"),
		decision.WithStateFunc(func(_ context.Context, req agent_v2.RouteRequest) (any, error) {
			return map[string]string{"order_status": "已签收", "message": req.Input.Content}, nil
		}),
	)
	if _, err := decide(context.Background(), agent_v2.RouteRequest{
		Input:      schema.UserMessage("怎么还没到"),
		Candidates: []agent_v2.RoutingChoice{{Name: "billing"}, {Name: "tech"}},
	}); err != nil {
		t.Fatalf("decide: %v", err)
	}
	req := fake.last(t)
	state, ok := req["state"].(map[string]any)
	if !ok || state["order_status"] != "已签收" || state["message"] != "怎么还没到" {
		t.Errorf("structured state not sent as an object: %v", req["state"])
	}
	q := req["questions"].(map[string]any)["route"].(map[string]any)
	if q["instructions"] != "这条消息该交给哪个处理方？" {
		t.Errorf("custom instructions not used: %v", q["instructions"])
	}
}

// TestRouteDecider_EmptyStateAbstainsWithoutCalling：没有可判断的内容时这一档弃权，
// 不发请求。
func TestRouteDecider_EmptyStateAbstainsWithoutCalling(t *testing.T) {
	fake := &fakeJev{reply: choiceReply("tech", 0.99)}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	client := jev.NewClient("k", decision.WithBaseURL(srv.URL))
	stateOf := func(v any) decision.DeciderOption {
		return decision.WithStateFunc(func(context.Context, agent_v2.RouteRequest) (any, error) { return v, nil })
	}
	type label string
	blank := "   "

	cases := map[string]struct {
		decide agent_v2.RouteDecider
		input  *schema.Message
	}{
		"blank text":             {decision.NewRouteDecider(client), schema.UserMessage("  \n")},
		"nil input":              {decision.NewRouteDecider(client), nil},
		"state func returns nil": {decision.NewRouteDecider(client, stateOf(nil)), schema.UserMessage("hi")},
		"typed nil map":          {decision.NewRouteDecider(client, stateOf(map[string]string(nil))), schema.UserMessage("hi")},
		"named blank string":     {decision.NewRouteDecider(client, stateOf(label(" "))), schema.UserMessage("hi")},
		"pointer to blank":       {decision.NewRouteDecider(client, stateOf(&blank)), schema.UserMessage("hi")},
		"empty object":           {decision.NewRouteDecider(client, stateOf(map[string]any{})), schema.UserMessage("hi")},
	}
	for name, tc := range cases {
		dec, err := tc.decide(context.Background(), agent_v2.RouteRequest{
			Input:      tc.input,
			Candidates: []agent_v2.RoutingChoice{{Name: "billing"}, {Name: "tech"}},
		})
		if err != nil || dec.Target != "" {
			t.Errorf("%s: expected an abstention, got %+v, %v", name, dec, err)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.requests) != 0 {
		t.Fatalf("nothing to judge should not be sent to Jev, got %d requests", len(fake.requests))
	}
}

// TestRouteDecider_BoundedDeciderEscalatesWhenItsTimeRunsOut：按文档推荐，给这一档派生一个
// 带超时的 ctx。Jev 慢到超时时这一档记为报错、升级到兜底，Run 正常完成；Run 自己的 ctx
// 还有富余，不受影响。
func TestRouteDecider_BoundedDeciderEscalatesWhenItsTimeRunsOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(400 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("retry-after-ms", "50")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	decide := decision.NewRouteDecider(jev.NewClient("k", decision.WithBaseURL(srv.URL)))
	bounded := func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		ctx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
		defer cancel()
		return decide(ctx, req)
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(bounded, 0.85),
		agent_v2.WithRoutingFallback(replyAgent("human")),
	).AddBranch("billing", replyAgent("billing")).AddBranch("tech", replyAgent("tech"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("闪退"))
	if err != nil {
		t.Fatalf("the decider that ran out of time should escalate, got %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "from human" {
		t.Fatalf("expected the fallback branch to answer, got %+v", final)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("the decider should give up at its own deadline, took %v", time.Since(start))
	}
}

// TestRouteDecider_BadAnswersAreErrors：答案缺选项或缺置信度都当作这一档失败，交给
// RoutingAgent 升级，不能当成置信度 0 的有效判断。
func TestRouteDecider_BadAnswersAreErrors(t *testing.T) {
	const usage = `"usage":{"input_tokens":1,"output_tokens":1}`
	replies := map[string]string{
		"no answer":                 `{"model":"jev-1.13.0","answers":{},` + usage + `}`,
		"no confidence":             `{"model":"jev-1.13.0","answers":{"route":{"type":"choice","choice":"tech"}},` + usage + `}`,
		"route answered noul":       `{"model":"jev-1.13.0","answers":{"route":{"type":"noul","noul":0.9}},` + usage + `}`,
		"noul with a stray choice":  `{"model":"jev-1.13.0","answers":{"route":{"type":"noul","noul":0.9,"choice":"tech"}},` + usage + `}`,
		"score with a stray choice": `{"model":"jev-1.13.0","answers":{"route":{"type":"score","score":1,"confidence":0.99,"legend":{"0":"a"},"probabilities":{"0":1},"choice":"tech"}},` + usage + `}`,
	}
	for name, body := range replies {
		t.Run(name, func(t *testing.T) {
			body := body
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("x-typesafe-request-id", "req_route")
				w.Write([]byte(body)) //nolint:errcheck // 测试服务
			}))
			defer srv.Close()
			decide := decision.NewRouteDecider(jev.NewClient("k", decision.WithBaseURL(srv.URL)))
			_, err := decide(context.Background(), agent_v2.RouteRequest{
				Input:      schema.UserMessage("hi"),
				Candidates: []agent_v2.RoutingChoice{{Name: "billing"}, {Name: "tech"}},
			})
			if err == nil {
				t.Fatalf("%s should be an error", name)
			}
			if !strings.Contains(err.Error(), "req_route") {
				t.Fatalf("%s: the error should carry the request id for troubleshooting, got %v", name, err)
			}
		})
	}
}

// TestRouteDecider_WithRoutingAgent：接到 RoutingAgent 上端到端跑，高置信直达目标分支，
// 低置信落到兜底分支，候选描述来自 WithRoutingDescriptions。
func TestRouteDecider_WithRoutingAgent(t *testing.T) {
	cases := []struct {
		confidence float64
		want       string
	}{
		{0.95, "from tech"},
		{0.5, "from human"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.confidence), func(t *testing.T) {
			fake := &fakeJev{reply: choiceReply("tech", tc.confidence)}
			srv := httptest.NewServer(fake)
			defer srv.Close()

			r := agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("support"),
				agent_v2.WithRouteDecider(decision.NewRouteDecider(jev.NewClient("k", decision.WithBaseURL(srv.URL))), 0.85),
				agent_v2.WithRoutingFallback(replyAgent("human")),
				agent_v2.WithRoutingDescriptions(map[string]string{"billing": "账单", "tech": "故障"}),
			).AddBranch("billing", replyAgent("billing")).AddBranch("tech", replyAgent("tech"))

			events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("闪退"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			final := agent_v2.FinalMessage(events)
			if final == nil || final.Content != tc.want {
				t.Fatalf("got %v, want %q", final, tc.want)
			}
			crit := fake.last(t)["questions"].(map[string]any)["route"].(map[string]any)["criteria"].(map[string]any)
			if crit["tech"] != "故障" {
				t.Fatalf("descriptions should reach Jev as option text, got %v", crit)
			}
		})
	}
}

// replyAgent 是产出固定为 "from <name>" 的分支，用最终消息判断实际跑到了哪个分支。
type replyAgentT struct{ name string }

func replyAgent(name string) agent_v2.Agent { return &replyAgentT{name: name} }

func (a *replyAgentT) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ev := agent_v2.NewExitEvent(a.name, schema.AssistantMessage("from "+a.name))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}
func (a *replyAgentT) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *replyAgentT) Name() string        { return a.name }
func (a *replyAgentT) Description() string { return "" }

// TestRouteDecider_RateLimitWithRunDeadlineDoesNotEscalate 钉住一个有意的取舍：不给决策方
// 单独派生 ctx、而是让它用 Run 的 ctx 时，限流撞上 Run 的 deadline 会让整个 Run 失败，不会
// 升级到兜底。客户端只拦「等待本身就放不下」——等完之后那次请求要多久估不准，估错两个
// 方向都有代价（见 decision.WithRetryBudget 与 doc/v2/route-decider.md §4.1）。要保证升级就按
// 文档包一层派生 ctx，见上一条用例。
func TestRouteDecider_RateLimitWithRunDeadlineDoesNotEscalate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(300 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("retry-after-ms", "600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decision.NewRouteDecider(jev.NewClient("k", decision.WithBaseURL(srv.URL))), 0.85),
		agent_v2.WithRoutingFallback(replyAgent("human")),
	).AddBranch("billing", replyAgent("billing")).AddBranch("tech", replyAgent("tech"))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("闪退"))
	if err == nil {
		t.Fatalf("expected the run to fail on the caller deadline, routed to %v", agent_v2.FinalMessage(events))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller deadline in the error chain, got %v", err)
	}
}
