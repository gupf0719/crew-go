// client_smoke 对真实的决策模型服务跑一遍 decision 包：一个请求同时问选择题、打分题、
// 是非题，逐个核对答案的形状；故意请求一个不存在的模型，核对服务端报错能被认出来；再让
// RoutingAgent 挂决策模型的路由决策方完整路由几条输入。
//
// 单测都走 httptest 桩，桩是照对服务端协议的理解写的——理解与真服务端不一致时，客户端会
// 拒掉真实响应而单测照样全绿。改了请求校验、响应解析或某一家的接入之后跑一次这个。
//
// 用 -provider 选服务方：
//
//	TYPESAFE_API_KEY=... go run ./example/decision/client_smoke -provider jev
//	go run ./example/decision/client_smoke -provider bailian -workspace llm-xxxx [-region ap-southeast-1]
//
// 走百炼时 key 取 DASHSCOPE_API_KEY，没有就取 QWEN_API_KEY（两者是同一种百炼 API key；项目
// 根目录的 .env 会自动加载），业务空间 ID 也可以经环境变量 BAILIAN_WORKSPACE_ID 给。
//
// 一次运行约 5 个请求；直连 TypeSafe 按当前标价不到千分之一美元。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/decision"
	"crew-go/decision/bailian"
	"crew-go/decision/jev"
	"crew-go/example/envloader"
	"crew-go/model"
	"crew-go/schema"
)

func main() {
	envloader.Load()
	provider := flag.String("provider", "jev", "服务方：jev（直连 TypeSafe）或 bailian（阿里云百炼）")
	workspace := flag.String("workspace", os.Getenv("BAILIAN_WORKSPACE_ID"), "百炼业务空间 ID，形如 llm-xxxx")
	region := flag.String("region", string(bailian.Beijing), "百炼地域：cn-beijing 或 ap-southeast-1")
	flag.Parse()

	c, err := newClient(*provider, *workspace, *region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	failed := false
	for _, step := range []struct {
		name string
		run  func(context.Context, *decision.Client) error
	}{
		{"一个请求问三种题型", threeQuestionTypes},
		{"不存在的模型报 4xx 且不重试", unknownModelIsClientError},
		{"RoutingAgent 挂决策模型的路由决策方", routeThroughDecider},
	} {
		if err := step.run(ctx, c); err != nil {
			fmt.Printf("✗ %s: %v\n", step.name, err)
			failed = true
			continue
		}
		fmt.Printf("✓ %s\n", step.name)
	}
	if failed {
		os.Exit(1)
	}
}

// newClient 按 -provider 构造客户端，缺 key 或业务空间时返回错误。
func newClient(provider, workspace, region string) (*decision.Client, error) {
	switch provider {
	case "jev":
		key := os.Getenv("TYPESAFE_API_KEY")
		if key == "" {
			return nil, errors.New("需要环境变量 TYPESAFE_API_KEY")
		}
		return jev.NewClient(key), nil
	case "bailian":
		key := os.Getenv("DASHSCOPE_API_KEY")
		if key == "" {
			key = os.Getenv("QWEN_API_KEY")
		}
		if key == "" {
			return nil, errors.New("需要环境变量 DASHSCOPE_API_KEY 或 QWEN_API_KEY")
		}
		if workspace == "" {
			return nil, errors.New("需要 -workspace 或环境变量 BAILIAN_WORKSPACE_ID")
		}
		return bailian.NewClient(key, bailian.Region(region), workspace), nil
	default:
		return nil, fmt.Errorf("未知的 -provider %q，可选 jev、bailian", provider)
	}
}

// threeQuestionTypes 在同一份 State 上问三道题，核对每个答案只填了它那一类的字段、
// 取值落在合法范围里。
func threeQuestionTypes(ctx context.Context, c *decision.Client) error {
	levels := []string{"平静", "有些不满", "非常愤怒"}
	req := &decision.Request{
		State: "你们客服也太慢了，我等了半个小时都没人理，再这样我就退订不用了。",
		Questions: map[string]decision.Question{
			"intent": decision.Choice("用户这条消息的主要诉求是什么", map[string]string{
				"complaint": "投诉服务质量",
				"refund":    "要求退款",
				"question":  "咨询产品用法",
			}),
			"anger": decision.Score("用户的不满程度", levels),
			"churn": decision.Noul("用户是否表示可能会停止使用或退订"),
		},
	}
	resp, err := c.Decide(ctx, req)
	if err != nil {
		return err
	}
	fmt.Printf("  模型 %s，请求 ID %s，token %+v\n", resp.Model, resp.RequestID, resp.Usage)
	if resp.Model == "" || resp.RequestID == "" {
		return fmt.Errorf("model or request id is empty: model=%q id=%q", resp.Model, resp.RequestID)
	}

	intent, ok := resp.Answers["intent"]
	if !ok || intent.Type != decision.TypeChoice {
		return fmt.Errorf("intent: missing or wrong type: %+v", intent)
	}
	if !slices.Contains([]string{"complaint", "refund", "question"}, intent.Choice) {
		return fmt.Errorf("intent: choice %q is not one of the options", intent.Choice)
	}
	if err := checkDistribution("intent", intent.Probabilities, []string{"complaint", "refund", "question"}); err != nil {
		return err
	}
	if err := checkUnit("intent confidence", intent.Confidence); err != nil {
		return err
	}
	fmt.Printf("  intent = %s（置信度 %.3f，概率 %v）\n", intent.Choice, *intent.Confidence, intent.Probabilities)

	anger, ok := resp.Answers["anger"]
	if !ok || anger.Type != decision.TypeScore {
		return fmt.Errorf("anger: missing or wrong type: %+v", anger)
	}
	if anger.Score == nil || math.IsNaN(*anger.Score) || *anger.Score < 0 || *anger.Score > float64(len(levels)-1) {
		return fmt.Errorf("anger: score %v is outside the level range [0, %d]", anger.Score, len(levels)-1)
	}
	levelKeys := make([]string, len(levels))
	for i := range levels {
		levelKeys[i] = strconv.Itoa(i)
	}
	if err := checkDistribution("anger", anger.Probabilities, levelKeys); err != nil {
		return err
	}
	if len(anger.Legend) != len(levels) {
		return fmt.Errorf("anger: legend should echo %d levels, got %v", len(levels), anger.Legend)
	}
	if err := checkUnit("anger confidence", anger.Confidence); err != nil {
		return err
	}
	fmt.Printf("  anger = %.3f（置信度 %.3f，概率 %v）\n", *anger.Score, *anger.Confidence, anger.Probabilities)

	churn, ok := resp.Answers["churn"]
	if !ok || churn.Type != decision.TypeNoul {
		return fmt.Errorf("churn: missing or wrong type: %+v", churn)
	}
	if err := checkUnit("churn", churn.Noul); err != nil {
		return err
	}
	if churn.Choice != "" || churn.Score != nil || len(churn.Probabilities) != 0 {
		return fmt.Errorf("churn: a yes/no answer should only carry Noul, got %+v", churn)
	}
	fmt.Printf("  churn = %.3f\n", *churn.Noul)
	return nil
}

// unknownModelIsClientError 故意请求一个不存在的模型：服务端报 4xx，客户端返回
// *model.APIError、不重试；错误文本带服务端的请求 ID（服务端给了的话）。
func unknownModelIsClientError(ctx context.Context, c *decision.Client) error {
	start := time.Now()
	_, err := c.Decide(ctx, &decision.Request{
		Model:     "decision-smoke-no-such-model",
		State:     "这条请求用来核对报错",
		Questions: map[string]decision.Question{"q": decision.Noul("是否紧急")},
	})
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) {
		return fmt.Errorf("expected *model.APIError, got %v", err)
	}
	if apiErr.StatusCode < http.StatusBadRequest || apiErr.StatusCode >= http.StatusInternalServerError {
		return fmt.Errorf("expected a 4xx status, got %v", err)
	}
	fmt.Printf("  HTTP %d，用时 %v：%v\n", apiErr.StatusCode, time.Since(start).Round(time.Millisecond), err)
	return nil
}

// routeThroughDecider 让 RoutingAgent 只挂一档决策模型的路由决策方（门槛 0，总是采纳），
// 核对请求路由到了预期的分支、路由决策事件带着决策模型给的置信度与各分支概率。
func routeThroughDecider(ctx context.Context, c *decision.Client) error {
	descriptions := map[string]string{
		"billing": "账单、扣费、发票",
		"tech":    "故障、报错、无法使用",
		"refund":  "退款、退货",
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("support"),
		agent_v2.WithRouteDecider(decision.NewRouteDecider(c), 0),
		agent_v2.WithRoutingDescriptions(descriptions),
	)
	for _, name := range []string{"billing", "tech", "refund"} {
		r.AddBranch(name, &echoAgent{name: name})
	}

	cases := []struct{ input, want string }{
		{"这个月被多扣了一笔钱，发票抬头也开错了", "billing"},
		{"App 一打开就闪退，提示错误代码 502", "tech"},
		{"买的东西不想要了，怎么把钱退回来", "refund"},
	}
	for _, tc := range cases {
		rec := &decisionRecorder{}
		runCtx := agent_v2.WithCtxOrchHooks(ctx, rec)
		events, err := r.Run(runCtx, agent_v2.NewRunSession(), schema.UserMessage(tc.input))
		if err != nil {
			return fmt.Errorf("%q: %w", tc.input, err)
		}
		final := agent_v2.FinalMessage(events)
		if final == nil || final.Content != "from "+tc.want {
			return fmt.Errorf("%q: routed to %v, want %s", tc.input, final, tc.want)
		}
		a := rec.only()
		if a == nil {
			return fmt.Errorf("%q: expected exactly one route_decision event, got %d", tc.input, rec.count())
		}
		if a.Stage != agent_v2.RouteStageDecider || a.Target != tc.want {
			return fmt.Errorf("%q: decision event should come from the decider, got %+v", tc.input, a)
		}
		if err := checkUnit("confidence", a.Confidence); err != nil {
			return fmt.Errorf("%q: %w", tc.input, err)
		}
		if err := checkDistribution("scores", a.Scores, []string{"billing", "tech", "refund"}); err != nil {
			return fmt.Errorf("%q: %w", tc.input, err)
		}
		fmt.Printf("  %q → %s（置信度 %.3f，来源 %s）\n", tc.input, a.Target, *a.Confidence, a.Source)
	}
	return nil
}

// checkUnit 核对一个概率型的值存在、有限且在 [0, 1] 里。
func checkUnit(name string, v *float64) error {
	if v == nil || math.IsNaN(*v) || *v < 0 || *v > 1 {
		return fmt.Errorf("%s: %v is not a value in [0, 1]", name, v)
	}
	return nil
}

// checkDistribution 核对概率表的键都在 allowed 里、值都在 [0, 1]、加起来约等于 1。
func checkDistribution(name string, probs map[string]float64, allowed []string) error {
	if len(probs) == 0 {
		return fmt.Errorf("%s: probabilities are empty", name)
	}
	sum := 0.0
	for k, p := range probs {
		if !slices.Contains(allowed, k) {
			return fmt.Errorf("%s: unexpected key %q in probabilities %v", name, k, probs)
		}
		if math.IsNaN(p) || p < 0 || p > 1 {
			return fmt.Errorf("%s: probability %v for %q is outside [0, 1]", name, p, k)
		}
		sum += p
	}
	if math.Abs(sum-1) > 0.02 {
		return fmt.Errorf("%s: probabilities sum to %.4f, not 1", name, sum)
	}
	return nil
}

// echoAgent 是产出固定为 "from <name>" 的分支，用最终消息判断实际跑到了哪个分支。
type echoAgent struct{ name string }

func (a *echoAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ev := agent_v2.NewExitEvent(a.name, schema.AssistantMessage("from "+a.name))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}

func (a *echoAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

func (a *echoAgent) Name() string        { return a.name }
func (a *echoAgent) Description() string { return "" }

// decisionRecorder 收集路由决策事件。
type decisionRecorder struct {
	agent_v2.NoOpOrchestrationHooks
	mu      sync.Mutex
	actions []*agent_v2.RouteDecisionAction
}

func (h *decisionRecorder) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if a, ok := agent_v2.RouteDecisionOf(ev); ok {
		h.mu.Lock()
		h.actions = append(h.actions, a)
		h.mu.Unlock()
	}
	return ctx
}

func (h *decisionRecorder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.actions)
}

// only 返回唯一一条路由决策事件，条数不是 1 时返回 nil。
func (h *decisionRecorder) only() *agent_v2.RouteDecisionAction {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.actions) != 1 {
		return nil
	}
	return h.actions[0]
}
