package decision_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/decision"
	"crew-go/decision/jev"
	"crew-go/model"
)

// sampleResponse 是官方 quickstart 里的响应，三种问题各一个答案。
const sampleResponse = `{
  "model": "jev-1.13.0",
  "answers": {
    "department": {"type": "choice", "choice": "technical", "confidence": 0.78,
                   "probabilities": {"technical": 0.85, "sales": 0.0, "billing": 0.15}},
    "frustration": {"type": "score", "score": 1.0, "confidence": 1.0,
                    "legend": {"0": "calm", "1": "frustrated", "2": "angry"},
                    "probabilities": {"0": 0.0, "1": 1.0, "2": 0.0}},
    "is_urgent": {"type": "noul", "noul": 1.0}
  },
  "usage": {"input_tokens": 392, "output_tokens": 65}
}`

// fastRetry 让重试测试不必等真实的退避时间。
func fastRetry() decision.Option {
	return decision.WithBackoff(time.Millisecond, 2*time.Millisecond)
}

func sampleRequest() *decision.Request {
	return &decision.Request{
		State: "Stripe integration keeps failing, please help ASAP.",
		Questions: map[string]decision.Question{
			"department":  decision.Choice("Which team should handle this", map[string]string{"billing": "Payments", "technical": "Bugs", "sales": "Pricing"}),
			"frustration": decision.Score("How frustrated the customer appears", []string{"calm", "frustrated", "angry"}),
			"is_urgent":   decision.Noul("The message conveys urgency"),
		},
	}
}

// TestSystemOne_RequestShapeAndAnswers：请求打到官方路径、带鉴权头，请求体按官方
// 格式序列化，模型为空时补默认模型；三种答案都解析出来。
func TestSystemOne_RequestShapeAndAnswers(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/systemone" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization header = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type = %q", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("request body is not JSON: %v", err)
		}
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := jev.NewClient("test-key", decision.WithBaseURL(srv.URL+"/"))
	resp, err := c.Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	if body["model"] != jev.Model {
		t.Errorf("empty request model should fall back to %q, got %v", jev.Model, body["model"])
	}
	if body["state"] != "Stripe integration keeps failing, please help ASAP." {
		t.Errorf("state not sent as-is: %v", body["state"])
	}
	qs := body["questions"].(map[string]any)
	dept := qs["department"].(map[string]any)
	if dept["type"] != "choice" || dept["instructions"] != "Which team should handle this" ||
		dept["criteria"].(map[string]any)["technical"] != "Bugs" {
		t.Errorf("choice question serialized wrong: %v", dept)
	}
	if levels := qs["frustration"].(map[string]any)["criteria"].([]any); len(levels) != 3 || levels[2] != "angry" {
		t.Errorf("score levels serialized wrong: %v", levels)
	}
	if _, has := qs["is_urgent"].(map[string]any)["criteria"]; has {
		t.Errorf("noul question should omit criteria")
	}

	if resp.Model != "jev-1.13.0" || resp.Usage.InputTokens != 392 {
		t.Errorf("model/usage not parsed: %+v", resp)
	}
	d := resp.Answers["department"]
	if d.Choice != "technical" || d.Confidence == nil || *d.Confidence != 0.78 || d.Probabilities["billing"] != 0.15 {
		t.Errorf("choice answer parsed wrong: %+v", d)
	}
	f := resp.Answers["frustration"]
	if f.Score == nil || *f.Score != 1.0 || f.Legend["2"] != "angry" {
		t.Errorf("score answer parsed wrong: %+v", f)
	}
	u := resp.Answers["is_urgent"]
	if u.Noul == nil || *u.Noul != 1.0 || u.Confidence != nil {
		t.Errorf("noul answer parsed wrong: %+v", u)
	}
}

// TestSystemOne_ModelSelection：请求里给了模型就用请求的，否则用客户端配置的默认模型。
func TestSystemOne_ModelSelection(t *testing.T) {
	var models []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body) // 解析失败时 models 记空串，下面的断言会报出来
		models = append(models, body.Model)
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), decision.WithModel("jev-1.13.0"))
	req := sampleRequest()
	if _, err := c.Decide(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.Model = "jev-preview"
	if _, err := c.Decide(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || models[0] != "jev-1.13.0" || models[1] != "jev-preview" {
		t.Fatalf("models sent = %v", models)
	}
}

// TestSystemOne_RetriesRateLimitHonoringRetryAfterMs：429 带 retry-after-ms 时按它等待
// 后重试，第二次成功。
func TestSystemOne_RetriesRateLimitHonoringRetryAfterMs(t *testing.T) {
	var hits atomic.Int32
	var firstAt, secondAt time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			firstAt = time.Now()
			w.Header().Set("retry-after-ms", "80")
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"detail":{"message":"slow down"}}`)
			return
		}
		secondAt = time.Now()
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), fastRetry())
	if _, err := c.Decide(context.Background(), sampleRequest()); err != nil {
		t.Fatalf("a rate-limited request should succeed after retrying: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", hits.Load())
	}
	if gap := secondAt.Sub(firstAt); gap < 80*time.Millisecond {
		t.Fatalf("retry should wait retry-after-ms (80ms) instead of the tiny backoff, waited %v", gap)
	}
}

// TestSystemOne_NoRetryOnAuthError：401 重试必然同样失败，只打一次；错误里能取到
// 状态码，文本带服务端请求 ID。
func TestSystemOne_NoRetryOnAuthError(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("x-typesafe-request-id", "req_abc")
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"detail":{"error_type":"authentication_error"}}`)
	}))
	defer srv.Close()

	c := jev.NewClient("bad-key", decision.WithBaseURL(srv.URL), fastRetry())
	_, err := c.Decide(context.Background(), sampleRequest())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized || apiErr.Provider != "typesafe" {
		t.Fatalf("expected a typesafe 401 APIError, got %v", err)
	}
	if !strings.Contains(err.Error(), "req_abc") {
		t.Fatalf("error should carry the request id, got %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("401 must not be retried, got %d attempts", hits.Load())
	}
}

// TestSystemOne_RetriesOverloadedThenGivesUp：529 可重试，重试两次后仍失败就返回错误。
func TestSystemOne_RetriesOverloadedThenGivesUp(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(529)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), fastRetry())
	_, err := c.Decide(context.Background(), sampleRequest())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 529 {
		t.Fatalf("expected a 529 APIError, got %v", err)
	}
	if hits.Load() != 3 {
		t.Fatalf("expected 1 attempt + 2 retries, got %d", hits.Load())
	}
}

// TestSystemOne_AttemptTimeoutIsRetried：单次尝试超时算网络层失败，会重试；调用方的
// ctx 没结束，不能因为错误链上有 DeadlineExceeded 就放弃。
func TestSystemOne_AttemptTimeoutIsRetried(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			select {
			case <-time.After(2 * time.Second):
			case <-r.Context().Done():
			}
			return
		}
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), decision.WithAttemptTimeout(100*time.Millisecond), fastRetry())
	if _, err := c.Decide(context.Background(), sampleRequest()); err != nil {
		t.Fatalf("a timed-out attempt should be retried: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", hits.Load())
	}
}

// TestSystemOne_ValidatesRequestBeforeSending：明显非法的请求当场报错，不发 HTTP。
func TestSystemOne_ValidatesRequestBeforeSending(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	c := jev.NewClient("k", decision.WithBaseURL(srv.URL))

	noul := map[string]decision.Question{"q": decision.Noul("x")}
	rawState := []byte(`{"message":"hi"}`)
	cases := map[string]*decision.Request{
		"nil request":                   nil,
		"nil state":                     {Questions: noul},
		"typed nil state":               {State: map[string]string(nil), Questions: noul},
		"number state":                  {State: 42, Questions: noul},
		"bool state":                    {State: true, Questions: noul},
		"no questions":                  {State: "s"},
		"empty question name":           {State: "s", Questions: map[string]decision.Question{"": decision.Noul("x")}},
		"choice with no opts":           {State: "s", Questions: map[string]decision.Question{"q": decision.Choice("x", nil)}},
		"choice criteria not an object": {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeChoice, Criteria: []string{"a"}}}},
		"score with no level":           {State: "s", Questions: map[string]decision.Question{"q": decision.Score("x", nil)}},
		"score criteria not an array":   {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeScore, Criteria: map[string]string{"0": "low"}}}},
		"noul criteria not an object":   {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeNoul, Criteria: "yes"}}},
		"noul criteria unknown key":     {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeNoul, Criteria: map[string]string{"yes": "spam"}}}},
		"choice numeric description":    {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeChoice, Criteria: map[string]any{"a": 1}}}},
		"score null level":              {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeScore, Criteria: []any{"low", nil}}}},
		"score numeric level":           {State: "s", Questions: map[string]decision.Question{"q": {Type: decision.TypeScore, Criteria: []any{0, 1}}}},
		"state is raw bytes":            {State: []byte(`{"message":"hi"}`), Questions: noul},
		"state is pointer to raw bytes": {State: &rawState, Questions: noul},
		"choice empty option name":      {State: "s", Questions: map[string]decision.Question{"q": decision.Choice("x", map[string]string{"": "说不清"})}},
		"unknown type":                  {State: "s", Questions: map[string]decision.Question{"q": {Type: "rank", Instructions: "x"}}},
	}
	for name, req := range cases {
		_, err := c.Decide(context.Background(), req)
		if err == nil {
			t.Errorf("%s: expected a validation error", name)
			continue
		}
		if compose_v2.IsRetryable(err) {
			t.Errorf("%s: a request the server must reject should not be retried by outer orchestrations", name)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("invalid requests must not reach the server, got %d calls", hits.Load())
	}
}

// echoServer 按服务端的约定作答：choice 选第一个选项，score 把 criteria 按档位编号
// 回显进 legend，noul 答 0.5。它读请求体，用来验证请求形状与响应解码能对上。
func echoServer(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		var req struct {
			Questions map[string]struct {
				Type     string          `json:"type"`
				Criteria json.RawMessage `json:"criteria"`
			} `json:"questions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		answers := map[string]any{}
		for name, q := range req.Questions {
			switch q.Type {
			case "choice":
				var opts map[string]json.RawMessage
				json.Unmarshal(q.Criteria, &opts) //nolint:errcheck // 客户端已校验过形状
				probs := map[string]float64{}
				first := ""
				for k := range opts {
					if first == "" || k < first {
						first = k
					}
					probs[k] = 0
				}
				probs[first] = 1
				answers[name] = map[string]any{"type": "choice", "choice": first, "confidence": 1.0, "probabilities": probs}
			case "score":
				var levels []json.RawMessage
				json.Unmarshal(q.Criteria, &levels) //nolint:errcheck // 同上
				legend := map[string]json.RawMessage{}
				probs := map[string]float64{}
				for i, l := range levels {
					legend[fmt.Sprint(i)] = l
					probs[fmt.Sprint(i)] = 0
				}
				probs["0"] = 1
				answers[name] = map[string]any{"type": "score", "score": 0.0, "confidence": 1.0, "legend": legend, "probabilities": probs}
			case "noul":
				answers[name] = map[string]any{"type": "noul", "noul": 0.5}
			}
		}
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck // 测试服务
			"model": "jev-1.13.0", "answers": answers, "usage": map[string]int{"input_tokens": 1, "output_tokens": 1},
		})
	}))
}

// TestSystemOne_AcceptsJSONShapedCriteria：criteria 按编成 JSON 之后的形状校验，不限定
// Go 类型。选项描述可以是对象或 null，档位可以是对象，是非题的 criteria 可以为 null。
// 服务端把 score 的档位原样回显在 legend 里，对象形状也要解得开。
func TestSystemOne_AcceptsJSONShapedCriteria(t *testing.T) {
	var hits atomic.Int32
	srv := echoServer(t, &hits)
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL))
	resp, err := c.Decide(context.Background(), &decision.Request{
		State: json.RawMessage(`["第一条消息", {"role": "user", "text": "第二条"}]`),
		Questions: map[string]decision.Question{
			"department":  {Type: decision.TypeChoice, Criteria: map[string]any{"billing": map[string]any{"covers": []string{"发票"}}, "other": nil}},
			"frustration": {Type: decision.TypeScore, Criteria: []any{"calm", map[string]any{"label": "angry"}}},
			"is_urgent":   {Type: decision.TypeNoul, Criteria: map[string]any{"true": "需要马上处理", "false": nil}},
			"is_spam":     {Type: decision.TypeNoul, Criteria: map[string]string(nil)},
		},
	})
	if err != nil {
		t.Fatalf("JSON-shaped criteria should be accepted and the echoed legend decoded: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected the request to be sent once, got %d", hits.Load())
	}
	level, err := json.Marshal(resp.Answers["frustration"].Legend["1"])
	if err != nil || string(level) != `{"label":"angry"}` {
		t.Fatalf("legend should echo the object-shaped level, got %s (%v)", level, err)
	}
}

// TestSystemOne_RetryBudgetSkipsLongServerWait：服务端要求的等待超出默认 30 秒预算时
// 不再重试，立刻返回那次 429，而不是真去等。超出 time.Duration 范围的值也一样。
func TestSystemOne_RetryBudgetSkipsLongServerWait(t *testing.T) {
	headers := map[string][2]string{
		"one hour":             {"retry-after", "3600"},
		"saturating seconds":   {"retry-after", "1e300"},
		"saturating ms":        {"retry-after-ms", "1e300"},
		"far future http date": {"retry-after", "Fri, 31 Dec 9999 23:59:59 GMT"},
	}
	for name, h := range headers {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set(h[0], h[1])
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer srv.Close()

			c := jev.NewClient("k", decision.WithBaseURL(srv.URL))
			done := make(chan error, 1)
			go func() {
				_, err := c.Decide(context.Background(), sampleRequest())
				done <- err
			}()
			select {
			case err := <-done:
				var apiErr *model.APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests {
					t.Fatalf("expected the 429 back right away, got %v", err)
				}
				if hits.Load() != 1 {
					t.Fatalf("a wait beyond the budget must not be taken, got %d attempts", hits.Load())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("still waiting after 2s: the budget did not stop the retry")
			}
		})
	}
}

// TestSystemOne_DoesNotWaitPastCallerDeadline：服务端要求的等待比 ctx 剩余时间还长时不去
// 等，立刻返回那次错误，调用方还来得及用剩下的时间走别的路。
func TestSystemOne_DoesNotWaitPastCallerDeadline(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("retry-after", "5")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := jev.NewClient("k", decision.WithBaseURL(srv.URL))
	start := time.Now()
	_, err := c.Decide(ctx, sampleRequest())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected the 429 back, got %v", err)
	}
	if ctx.Err() != nil || time.Since(start) > 500*time.Millisecond || hits.Load() != 1 {
		t.Fatalf("should return right away instead of sleeping into the deadline: took %v, %d attempts", time.Since(start), hits.Load())
	}
}

// TestSystemOne_SaturatedWaitWithoutBudgetReturnsAtOnce：不设预算、只靠 ctx 兜底时，服务端
// 要求的超大等待也不能让客户端睡到 deadline。
func TestSystemOne_SaturatedWaitWithoutBudgetReturnsAtOnce(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("retry-after", "1e300")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), decision.WithRetryBudget(0))
	start := time.Now()
	_, err := c.Decide(ctx, sampleRequest())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected the 429 back right away, got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond || hits.Load() != 1 {
		t.Fatalf("should not sleep into the deadline: took %v, %d attempts", time.Since(start), hits.Load())
	}
}

// TestSystemOne_StateWithTextMarshalerBytesIsAccepted：自带文本编码的字节切片（比如 net.IP）
// 编出来是字符串，是合法的 state。
func TestSystemOne_StateWithTextMarshalerBytesIsAccepted(t *testing.T) {
	var got any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck // 解不开时 got 为 nil，断言会报
		got = req["state"]
		io.WriteString(w, `{"model":"m","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL))
	req := &decision.Request{State: net.ParseIP("10.0.0.1"), Questions: map[string]decision.Question{"q": decision.Noul("x")}}
	if _, err := c.Decide(context.Background(), req); err != nil {
		t.Fatalf("a byte slice that encodes itself as text should be accepted: %v", err)
	}
	if got != "10.0.0.1" {
		t.Fatalf("the server should receive the IP as text, got %#v", got)
	}
}

// TestSystemOne_RetryBudgetCountsElapsedTime：预算按累计耗时算，已经等过的时间也算在内；
// 设 0 表示不限，重试次数用完为止。
func TestSystemOne_RetryBudgetCountsElapsedTime(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("retry-after-ms", "100")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), decision.WithRetryBudget(150*time.Millisecond))
	if _, err := c.Decide(context.Background(), sampleRequest()); err == nil {
		t.Fatal("an always-failing server should end in an error")
	}
	if hits.Load() != 2 {
		t.Fatalf("with a 150ms budget and 100ms waits only one retry fits, got %d attempts", hits.Load())
	}

	hits.Store(0)
	c = jev.NewClient("k", decision.WithBaseURL(srv.URL), decision.WithRetryBudget(0))
	if _, err := c.Decide(context.Background(), sampleRequest()); err == nil {
		t.Fatal("an always-failing server should end in an error")
	}
	if hits.Load() != 3 {
		t.Fatalf("without a budget every retry should run, got %d attempts", hits.Load())
	}
}

// TestSystemOne_CallerCancelStopsWaiting：等待重试时调用方取消，立即返回；错误既能判断
// 是取消，也看得到上一次失败的原因。
func TestSystemOne_CallerCancelStopsWaiting(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("retry-after-ms", "5000")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	c := jev.NewClient("k", decision.WithBaseURL(srv.URL))
	start := time.Now()
	_, err := c.Decide(ctx, sampleRequest())
	var apiErr *model.APIError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected a cancellation that still carries the last failure, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("cancellation should stop the wait, took %v", time.Since(start))
	}
}

// TestSystemOne_MalformedResponseIsNotRetried：成功状态但响应体不是 JSON，重试拿到的会
// 一样。只打一次，错误带请求 ID，外层编排也不重试。
func TestSystemOne_MalformedResponseIsNotRetried(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("x-typesafe-request-id", "req_bad")
		io.WriteString(w, "<html>gateway page</html>")
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), fastRetry())
	_, err := c.Decide(context.Background(), sampleRequest())
	if err == nil || !strings.Contains(err.Error(), "req_bad") {
		t.Fatalf("expected a decode error with the request id, got %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("a malformed response must not be retried, got %d attempts", hits.Load())
	}
	if compose_v2.IsRetryable(err) {
		t.Fatal("outer orchestrations must not retry a malformed response either")
	}
}

// TestSystemOne_IncompleteResponseIsAnError：成功状态但缺必填字段的响应不当成功返回，
// 也不重试。
func TestSystemOne_IncompleteResponseIsAnError(t *testing.T) {
	const usage = `"usage":{"input_tokens":1,"output_tokens":1}`
	bodies := map[string]string{
		"null":                      `null`,
		"empty object":              `{}`,
		"array":                     `[]`,
		"null answers":              `{"model":"jev-1.13.0","answers":null,` + usage + `}`,
		"no model":                  `{"answers":{"q":{"type":"noul","noul":0.5}},` + usage + `}`,
		"no usage":                  `{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}}}`,
		"answer without type":       `{"model":"jev-1.13.0","answers":{"q":{"noul":0.5}},` + usage + `}`,
		"choice without confidence": `{"model":"jev-1.13.0","answers":{"q":{"type":"choice","choice":"a","probabilities":{"a":1}}},` + usage + `}`,
		"score without legend":      `{"model":"jev-1.13.0","answers":{"q":{"type":"score","score":1,"confidence":1,"probabilities":{"0":1}}},` + usage + `}`,
		"noul without noul":         `{"model":"jev-1.13.0","answers":{"q":{"type":"noul"}},` + usage + `}`,
		"null probability":          `{"model":"jev-1.13.0","answers":{"q":{"type":"choice","choice":"a","confidence":1,"probabilities":{"a":1,"b":null}}},` + usage + `}`,
		"null legend value":         `{"model":"jev-1.13.0","answers":{"q":{"type":"score","score":0,"confidence":1,"legend":{"0":null},"probabilities":{"0":1}}},` + usage + `}`,
		"numeric legend value":      `{"model":"jev-1.13.0","answers":{"q":{"type":"score","score":0,"confidence":1,"legend":{"0":3},"probabilities":{"0":1}}},` + usage + `}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set("x-typesafe-request-id", "req_partial")
				io.WriteString(w, body)
			}))
			defer srv.Close()

			c := jev.NewClient("k", decision.WithBaseURL(srv.URL), fastRetry())
			resp, err := c.Decide(context.Background(), sampleRequest())
			if err == nil {
				t.Fatalf("an incomplete response must be an error, got %+v", resp)
			}
			if !strings.Contains(err.Error(), "req_partial") || compose_v2.IsRetryable(err) || hits.Load() != 1 {
				t.Fatalf("expected a non-retryable error with the request id after one attempt, got %v (%d attempts)", err, hits.Load())
			}
		})
	}
}

// TestSystemOne_AcceptsSchemaValidEdgeResponses：schema 允许的边界值不能被当成缺字段。
// 服务端以后新增的答案类型被跳过，其余答案照常返回；model 可以是空串，answers 可以没有。
func TestSystemOne_AcceptsSchemaValidEdgeResponses(t *testing.T) {
	const usage = `"usage":{"input_tokens":1,"output_tokens":1}`
	cases := map[string]struct {
		body string
		want []string
	}{
		"noul zero":                   {`{"model":"m","answers":{"q":{"type":"noul","noul":0}},` + usage + `}`, []string{"q"}},
		"score zeros":                 {`{"model":"m","answers":{"q":{"type":"score","score":0,"confidence":0,"legend":{},"probabilities":{}}},` + usage + `}`, []string{"q"}},
		"choice empty probs":          {`{"model":"m","answers":{"q":{"type":"choice","choice":"a","confidence":0.5,"probabilities":{}}},` + usage + `}`, []string{"q"}},
		"unknown type skipped":        {`{"model":"m","answers":{"spam":{"type":"noul","noul":0.9},"mystery":{"type":"aurora","value":3}},` + usage + `}`, []string{"spam"}},
		"empty model":                 {`{"model":"","answers":{"q":{"type":"noul","noul":0.5}},` + usage + `}`, []string{"q"}},
		"answers omitted":             {`{"model":"m",` + usage + `}`, nil},
		"nested legend values":        {`{"model":"m","answers":{"q":{"type":"score","score":1,"confidence":1,"legend":{"0":{"label":"calm"},"1":["a","b"]},"probabilities":{"0":0,"1":1}}},` + usage + `}`, []string{"q"}},
		"stray fields of other types": {`{"model":"m","answers":{"n":{"type":"noul","noul":0.5,"confidence":{"x":1}},"c":{"type":"choice","choice":"a","confidence":1,"probabilities":{"a":1},"legend":[1,2]}},` + usage + `}`, []string{"c", "n"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			resp, err := jev.NewClient("k", decision.WithBaseURL(srv.URL)).Decide(context.Background(), sampleRequest())
			if err != nil {
				t.Fatalf("a schema-valid response must be accepted: %v", err)
			}
			got := make([]string, 0, len(resp.Answers))
			for k := range resp.Answers {
				got = append(got, k)
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("answers = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSystemOne_OversizedResponseIsReported：响应体超过 1 MiB 时如实报超过上限，不截断
// 后当成解析失败，也不重试。
func TestSystemOne_OversizedResponseIsReported(t *testing.T) {
	var hits atomic.Int32
	big := `{"model":"` + strings.Repeat("a", 1<<20) + `"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.WriteString(w, big)
	}))
	defer srv.Close()

	c := jev.NewClient("k", decision.WithBaseURL(srv.URL), fastRetry())
	_, err := c.Decide(context.Background(), sampleRequest())
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected an oversized-response error, got %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("an oversized response must not be retried, got %d attempts", hits.Load())
	}
}

// TestNewClient_NormalizesKeyAndBaseURL：key 前后的空白去掉，比如从文件读来带的换行；
// 地址可以带路径前缀和结尾斜杠。成功响应带回服务端的请求 ID。
func TestNewClient_NormalizesKeyAndBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer k-123" {
			t.Errorf("Authorization = %q", got)
		}
		if r.URL.Path != "/gw/v1/systemone" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("x-typesafe-request-id", "req_ok")
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := jev.NewClient(" k-123\n", decision.WithBaseURL(" "+srv.URL+"/gw/ "))
	resp, err := c.Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if resp.RequestID != "req_ok" {
		t.Fatalf("RequestID = %q", resp.RequestID)
	}
}

// TestClientOptions_RejectInvalidConfig：只看配置本身就能判定的错误，构造期 panic。
func TestClientOptions_RejectInvalidConfig(t *testing.T) {
	cases := map[string]func(){
		"empty key":               func() { jev.NewClient("  ") },
		"control char in key":     func() { jev.NewClient("ab\x00cd") },
		"base url without scheme": func() { decision.WithBaseURL("api.typesafe.ai") },
		"base url not http":       func() { decision.WithBaseURL("ftp://api.typesafe.ai") },
		"base url with query":     func() { decision.WithBaseURL("https://api.typesafe.ai/?region=us") },
		"base url empty query":    func() { decision.WithBaseURL("https://api.typesafe.ai?") },
		"base url empty fragment": func() { decision.WithBaseURL("https://api.typesafe.ai#") },
		"empty base url":          func() { decision.WithBaseURL("") },
		"empty model":             func() { decision.WithModel(" ") },
		"nil http client":         func() { decision.WithHTTPClient(nil) },
		"zero attempt timeout":    func() { decision.WithAttemptTimeout(0) },
		"negative retries":        func() { decision.WithMaxRetries(-1) },
		"backoff initial > max":   func() { decision.WithBackoff(2*time.Second, time.Second) },
		"negative backoff":        func() { decision.WithBackoff(-time.Second, time.Second) },
		"negative budget":         func() { decision.WithRetryBudget(-time.Second) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s should panic", name)
				}
			}()
			fn()
		})
	}
}

// TestSystemOne_UnknownAnswerTypeIsLogged：被跳过的未知类型答案记一条 warn 日志，带问题名、
// 类型与请求 ID，便于找回。
func TestSystemOne_UnknownAnswerTypeIsLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-typesafe-request-id", "req_future")
		io.WriteString(w, `{"model":"m","answers":{"mystery":{"type":"aurora","value":3}},"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	before := log.Writer()
	var logged string
	func() {
		var buf bytes.Buffer
		prev, prevWriter, prevFlags := slog.Default(), log.Writer(), log.Flags()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		// slog.SetDefault 顺手改了标准库 log 的输出与 flag，换回去时它不管，得自己还原，
		// 否则这之后所有测试的日志都写进这个已经没人看的 buffer。
		defer func() {
			slog.SetDefault(prev)
			log.SetOutput(prevWriter)
			log.SetFlags(prevFlags)
		}()

		resp, err := jev.NewClient("k", decision.WithBaseURL(srv.URL)).Decide(context.Background(), sampleRequest())
		if err != nil || len(resp.Answers) != 0 {
			t.Fatalf("the unknown answer should be skipped without failing the call: %+v, %v", resp, err)
		}
		logged = buf.String()
	}()

	if log.Writer() != before {
		t.Fatal("swapping the default logger must leave the standard log output as it was")
	}
	for _, want := range []string{"mystery", "aurora", "req_future"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("the warn log should mention %q, got %q", want, logged)
		}
	}
}

// TestSystemOne_SlowAttemptStillLeavesRoomToRetry 钉住「等待判定只看这一次等多久」的收益面：
// 一次长尾请求之后剩余时间仍然放得下退避时，照样再试一次。改成把这次的耗时也算进去（估
// 下一次要多久）就会提前收手——那个估值两个方向都错，失败的那次常是网关秒回的 429，成功
// 的请求却要几百毫秒。姊妹用例 TestRouteDecider_RateLimitWithRunDeadlineDoesNotEscalate
// 钉的是同一处判定的代价面（等完之后那次请求可能撞上 deadline）。
func TestSystemOne_SlowAttemptStillLeavesRoomToRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 第一次拖 600ms 再返可重试的错，第二次立刻返。
		if hits.Add(1) == 1 {
			time.Sleep(600 * time.Millisecond)
		}
		w.Header().Set("retry-after-ms", "200")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// 900ms 预算：长尾吃掉 600ms，剩 300ms 仍放得下 200ms 的退避。
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	c := jev.NewClient("k", decision.WithBaseURL(srv.URL))
	_, err := c.Decide(ctx, sampleRequest())

	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected the 503 back, got %v", err)
	}
	if got := hits.Load(); got < 2 {
		t.Fatalf("a slow attempt must not cancel the retry: 600ms tail left 300ms and the backoff only needs 200ms, got %d attempts", got)
	}
}
