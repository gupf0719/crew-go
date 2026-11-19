package decision_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/decision"
	"crew-go/model/openai"
	"crew-go/schema"
)

func TestLLMRouteDeciderUsesCalibratedConfidence(t *testing.T) {
	for _, tc := range []struct {
		name, snapshot, wantReason string
		logprob, confidence, min   float64
	}{
		{name: "校准后通过", snapshot: "score-v1", logprob: -0.4, confidence: 0.91, min: 0.8},
		{name: "原始概率高而校准不足", snapshot: "score-v1", logprob: -0.01, confidence: 0.42, min: 0.8,
			wantReason: agent_v2.RouteReasonLowConfidence},
		{name: "换了未校准的模型", snapshot: "score-v2", logprob: -0.01, confidence: 0.99,
			wantReason: agent_v2.RouteReasonAbstain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observed atomic.Int32
			action := runLLMRoute(t, routeScoreReply(tc.snapshot, "stop", `{"role":"assistant","content":" B"}`,
				fmt.Sprintf(`{"content":[{"token":" B","bytes":[32,66],"logprob":%v}]}`, tc.logprob)), http.StatusOK,
				tc.min, tc.wantReason, func(s decision.LLMRouteSample) (float64, bool) {
					observed.Add(1)
					if s.Provider != "calibrated-provider" || s.Model != tc.snapshot || s.Target != "billing" ||
						s.Logprob != tc.logprob || len(s.Candidates) != 2 ||
						s.Candidates[0] != (agent_v2.RoutingChoice{Name: "tech", Description: "排查技术故障"}) ||
						s.Candidates[1] != (agent_v2.RoutingChoice{Name: "billing", Description: "账单、扣费"}) {
						t.Errorf("校准收到的证据或候选顺序有误，得到 %+v", s)
					}
					return tc.confidence, s.Model == "score-v1"
				})
			if observed.Load() != 1 {
				t.Fatalf("校准调用次数 = %d，期望 1", observed.Load())
			}
			if tc.wantReason == "" {
				if action.Source != tc.snapshot || action.Confidence == nil || *action.Confidence != tc.confidence ||
					len(action.Scores) != 1 || action.Scores["billing"] != math.Exp(tc.logprob) {
					t.Fatalf("应同时看见校准置信度、原始分数与来源，得到 %+v", action)
				}
				return
			}
			esc := action.Escalations[0]
			if esc.Source != tc.snapshot || len(esc.Scores) != 1 || esc.Scores["billing"] != math.Exp(tc.logprob) {
				t.Fatalf("升级应保留原始分数和实际模型，得到 %+v", esc)
			}
			if tc.wantReason == agent_v2.RouteReasonLowConfidence && (esc.Target != "billing" || esc.Confidence != tc.confidence) {
				t.Fatalf("低置信度应记录经过校准的值，得到 %+v", esc)
			}
		})
	}
}

func TestLLMRouteDeciderRejectsUnscoredOrMismatchedOutput(t *testing.T) {
	const valid = `{"content":[{"token":"B","logprob":0}]}`
	const picked = `{"role":"assistant","content":"B"}`
	for _, tc := range []struct {
		name, snapshot, finish, message, logprobs string
	}{
		{"缺少概率字段", "score-v1", "stop", picked, "null"},
		{"缺少单词元数值", "score-v1", "stop", picked, `{"content":[{"token":"B"}]}`},
		{"空概率", "score-v1", "stop", picked, `{"content":[{"token":"B","logprob":null}]}`},
		{"供应商极小占位值", "score-v1", "stop", picked, `{"content":[{"token":"B","logprob":-9999}]}`},
		{"非法正对数概率", "score-v1", "stop", picked, `{"content":[{"token":"B","logprob":0.1}]}`},
		{"文本对应另一词元", "score-v1", "stop", picked, `{"content":[{"token":"A","logprob":0}]}`},
		{"词元字节对应另一标签", "score-v1", "stop", picked, `{"content":[{"token":"B","bytes":[65],"logprob":0}]}`},
		{"仍有缺失位置", "score-v1", "stop", picked, `{"content":[null,{"token":"B","logprob":0}]}`},
		{"未知标签", "score-v1", "stop", `{"role":"assistant","content":"C"}`, `{"content":[{"token":"C","logprob":0}]}`},
		{"模型选择弃权", "score-v1", "stop", `{"role":"assistant","content":"0"}`, `{"content":[{"token":"0","logprob":0}]}`},
		{"生成遭截断", "score-v1", "length", picked, valid},
		{"附带拒答", "score-v1", "stop", `{"role":"assistant","content":"B","refusal":"cannot classify"}`, valid},
		{"服务端未标识模型", "", "stop", picked, valid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action := runLLMRoute(t, routeScoreReply(tc.snapshot, tc.finish, tc.message, tc.logprobs), http.StatusOK,
				0, agent_v2.RouteReasonAbstain, uncalledRouteCalibration(t))
			if len(action.Escalations[0].Scores) != 0 {
				t.Fatalf("缺少有效证据时不应产生分数，得到 %+v", action.Escalations[0])
			}
		})
	}
}

func TestLLMRouteDeciderEscalatesOnProviderError(t *testing.T) {
	runLLMRoute(t, `{"error":{"message":"this model does not support logprobs"}}`, http.StatusBadRequest,
		0, agent_v2.RouteReasonError, uncalledRouteCalibration(t))
}

func TestLLMRouteDeciderStopsWhenCalibrationCancels(t *testing.T) {
	for _, tc := range []struct {
		name    string
		accept  bool
		options []agent_v2.RouteDeciderOption
	}{
		{name: "本可采纳", accept: true},
		{name: "本可升级"},
		{name: "影子档", accept: true, options: []agent_v2.RouteDeciderOption{agent_v2.WithShadow()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scores, calibrations, next atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				scores.Add(1)
				_, _ = io.WriteString(w, routeScoreReply("score-v1", "stop", `{"role":"assistant","content":"A"}`,
					`{"content":[{"token":"A","logprob":-0.1}]}`))
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			m := openai.NewClient("k", "score-request", openai.WithBaseURL(server.URL))
			scored := decision.NewLLMRouteDecider(m, func(s decision.LLMRouteSample) (float64, bool) {
				calibrations.Add(1)
				if s.Target != "billing" || s.Logprob != -0.1 {
					t.Errorf("校准应收到真实标签证据，得到 %+v", s)
				}
				cancel()
				return 0.95, tc.accept
			})
			router := agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("support"),
				agent_v2.WithRouteDecider("llm-scored", scored, 0.8, tc.options...),
				agent_v2.WithRouteDecider("next", func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
					next.Add(1)
					return agent_v2.RouteDecision{Target: "billing", Confidence: 1}, nil
				}, 0),
			).AddBranch("billing", agent_v2.NewBasicAgent(agent_v2.WithName("billing"), agent_v2.WithModel(m)))
			record := llmRouteRecorder{actions: make(chan *agent_v2.RouteDecisionAction, 8)}
			_, err := router.Run(agent_v2.WithCtxOrchHooks(ctx, record), agent_v2.NewRunSession(), schema.UserMessage("查账单"))
			if !errors.Is(err, context.Canceled) || scores.Load() != 1 || calibrations.Load() != 1 ||
				next.Load() != 0 || len(record.actions) != 0 {
				t.Fatalf("校准期间取消应终止交接，err=%v，模型=%d，校准=%d，下一档=%d，事件=%d",
					err, scores.Load(), calibrations.Load(), next.Load(), len(record.actions))
			}
		})
	}
}

func uncalledRouteCalibration(t *testing.T) func(decision.LLMRouteSample) (float64, bool) {
	return func(s decision.LLMRouteSample) (float64, bool) {
		t.Errorf("无有效证据时不得调用校准，得到 %+v", s)
		return 1, true
	}
}

type llmRouteRecorder struct {
	agent_v2.NoOpOrchestrationHooks
	actions chan *agent_v2.RouteDecisionAction
}

func (r llmRouteRecorder) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if action, ok := agent_v2.RouteDecisionOf(ev); ok {
		r.actions <- action
	}
	return ctx
}

func routeScoreReply(snapshot, finish, message, logprobs string) string {
	return fmt.Sprintf(`{"model":%q,"choices":[{"message":%s,"finish_reason":%q,"logprobs":%s}]}`,
		snapshot, message, finish, logprobs)
}

// runLLMRoute 用真实 HTTP 适配器、路由图、内置 route_to 工具和分支，按最终模型调用核对路由。
func runLLMRoute(t *testing.T, scoringReply string, status int, threshold float64, reason string,
	calibrate func(decision.LLMRouteSample) (float64, bool),
) *agent_v2.RouteDecisionAction {
	t.Helper()
	const input = "帮我核对这笔扣款"
	var scorer, classifier, tech, billing atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model       string
			Messages    []struct{ Role, Content string }
			Tools       []struct{ Function struct{ Name string } }
			Stream      bool
			Logprobs    bool
			MaxTokens   int `json:"max_tokens"`
			Temperature *float64
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || r.URL.Path != "/chat/completions" {
			t.Errorf("模型请求异常，地址 %s，err=%v", r.URL.Path, err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(req.Messages) == 0 || !strings.Contains(req.Messages[len(req.Messages)-1].Content, input) {
			t.Errorf("模型应收到本轮输入，得到 %+v", req)
		}
		var reply string
		switch req.Model {
		case "score-request":
			scorer.Add(1)
			if req.Stream || !req.Logprobs || req.MaxTokens != 16 || req.Temperature == nil || *req.Temperature != 0 ||
				len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Role != "user" ||
				req.Messages[1].Content != input ||
				!strings.Contains(req.Messages[0].Content, "A: \"tech\" \"排查技术故障\"\nB: \"billing\" \"账单、扣费\"") {
				t.Errorf("打分请求应有受控标签、文本和 logprobs，得到 %+v", req)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, scoringReply) // 网络写失败时测试会在调用端报错。
			return
		case "classifier":
			classifier.Add(1)
			if req.Logprobs || len(req.Tools) != 1 || req.Tools[0].Function.Name != agent_v2.RouteTool {
				t.Errorf("应使用原来的 route_to classifier，得到 %+v", req)
			}
			reply = `{"choices":[{"index":0,"finish_reason":"tool_calls","delta":{"role":"assistant","tool_calls":[{"index":0,"id":"route-1","type":"function","function":{"name":"route_to","arguments":"{\"target\":\"tech\"}"}}]}}]}`
		case "tech":
			tech.Add(1)
			reply = `{"choices":[{"index":0,"delta":{"role":"assistant","content":"实际执行技术分支"},"finish_reason":"stop"}]}`
		case "billing":
			billing.Add(1)
			reply = `{"choices":[{"index":0,"delta":{"role":"assistant","content":"实际执行账单分支"},"finish_reason":"stop"}]}`
		default:
			t.Errorf("未知测试模型 %q", req.Model)
			http.Error(w, "unexpected test model", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+reply+"\n\ndata: [DONE]\n\n") // 网络写失败时测试会在调用端报错。
	}))
	defer server.Close()
	client := func(name string) *openai.Client {
		return openai.NewClient("k", name, openai.WithBaseURL(server.URL), openai.WithSystem("calibrated-provider"))
	}
	legacy := agent_v2.NewBasicAgent(agent_v2.WithName("classifier"), agent_v2.WithModel(client("classifier")), agent_v2.WithRouteTool())
	router := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider("llm-scored", decision.NewLLMRouteDecider(client("score-request"), calibrate), threshold),
		agent_v2.WithRoutingClassifier(legacy),
		agent_v2.WithRoutingDescriptions(map[string]string{"tech": "排查技术故障", "billing": "账单、扣费"}),
	).AddBranch("tech", agent_v2.NewBasicAgent(agent_v2.WithName("tech"), agent_v2.WithModel(client("tech")))).
		AddBranch("billing", agent_v2.NewBasicAgent(agent_v2.WithName("billing"), agent_v2.WithModel(client("billing"))))
	record := llmRouteRecorder{actions: make(chan *agent_v2.RouteDecisionAction, 8)}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	events, err := router.Run(agent_v2.WithCtxOrchHooks(ctx, record), agent_v2.NewRunSession(), schema.UserMessage(input))
	if err != nil || len(record.actions) != 1 || scorer.Load() != 1 {
		t.Fatalf("应在一次打分后发出唯一的路由事件，err=%v，事件数=%d，打分调用=%d", err, len(record.actions), scorer.Load())
	}
	action := <-record.actions
	final := agent_v2.FinalMessage(events)
	if reason == "" {
		if action.Target != "billing" || action.Stage != agent_v2.RouteStageDecider || action.DeciderName != "llm-scored" ||
			final == nil || final.Content != "实际执行账单分支" || billing.Load() != 1 || tech.Load() != 0 || classifier.Load() != 0 {
			t.Fatalf("应直接执行账单分支，事件=%+v，结果=%+v，调用=%d/%d/%d", action, final, classifier.Load(), tech.Load(), billing.Load())
		}
	} else if action.Target != "tech" || action.Stage != agent_v2.RouteStageClassifier || action.Confidence != nil ||
		len(action.Escalations) != 1 || action.Escalations[0].Reason != reason || action.Escalations[0].DeciderName != "llm-scored" ||
		final == nil || final.Content != "实际执行技术分支" || billing.Load() != 0 || tech.Load() != 1 || classifier.Load() != 1 {
		t.Fatalf("应因 %q 升级并实际执行技术分支，事件=%+v，结果=%+v，调用=%d/%d/%d", reason, action, final, classifier.Load(), tech.Load(), billing.Load())
	}
	return action
}
