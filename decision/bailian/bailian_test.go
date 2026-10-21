package bailian_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/decision"
	"crew-go/decision/bailian"
	"crew-go/model"
	"crew-go/schema"
)

// bailianResponse 是百炼决策模型一次真实调用的响应（只换了请求 ID）：请求 ID 在响应体里，
// usage 只有 input_tokens，另带 latency_ms。
const bailianResponse = `{"model":"decision-model-preview","request_id":"req-bl-1","answers":{` +
	`"department":{"type":"choice","choice":"billing","confidence":0.38,"probabilities":{"billing":0.69,"technical":0.31}},` +
	`"escalate":{"type":"noul","noul":0.9},` +
	`"severity":{"type":"score","score":2.59,"legend":{"0":"轻微","1":"部分受影响","2":"核心不可用","3":"严重影响"},` +
	`"probabilities":{"0":0.01,"1":0.07,"2":0.24,"3":0.68},"confidence":0.86}},` +
	`"usage":{"input_tokens":91},"latency_ms":70.7}`

// roundTripFunc 把函数当成 http.RoundTripper，用来在不联网的情况下看请求发到了哪里。
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// reply 构造一个带 x-request-id 头的响应，与百炼网关一致。
func reply(r *http.Request, status int, requestID, body string) *http.Response {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("x-request-id", requestID)
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func ticketRequest() *decision.Request {
	return &decision.Request{
		State: map[string]string{"content": "订单支付后超过 24 小时仍未到账"},
		Questions: map[string]decision.Question{
			"department": decision.Choice("应该由哪个团队处理？", map[string]string{"billing": "支付与账单", "technical": "产品故障"}),
			"escalate":   decision.Noul("是否需要立即通知值班人员？"),
			"severity":   decision.Score("这个问题有多严重？", []string{"轻微", "部分受影响", "核心不可用", "严重影响"}),
		},
	}
}

// TestNewClient_WorkspaceEndpoint：请求发到业务空间自己的地址，地域与业务空间 ID 前后的
// 空白去掉；带 Bearer key，没指定模型时用 decision-model-preview。
func TestNewClient_WorkspaceEndpoint(t *testing.T) {
	cases := []struct {
		region    bailian.Region
		workspace string
		wantURL   string
	}{
		{bailian.Beijing, "llm-test01", "https://llm-test01.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone"},
		{bailian.Singapore, "llm-test01", "https://llm-test01.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/systemone"},
		{" cn-beijing ", " llm-test01\n", "https://llm-test01.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone"},
	}
	for _, tc := range cases {
		t.Run(string(tc.region)+"/"+tc.workspace, func(t *testing.T) {
			var gotURL, gotAuth, gotModel string
			hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotURL = r.URL.String()
				gotAuth = r.Header.Get("Authorization")
				var body struct {
					Model string `json:"model"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body) // 解析失败时记空串，下面的断言会报出来
				gotModel = body.Model
				return reply(r, http.StatusOK, "req-bl-1", bailianResponse), nil
			})}
			c := bailian.NewClient("sk-test", tc.region, tc.workspace, decision.WithHTTPClient(hc))
			if _, err := c.Decide(context.Background(), ticketRequest()); err != nil {
				t.Fatalf("Decide: %v", err)
			}
			if gotURL != tc.wantURL {
				t.Errorf("request went to %q, want %q", gotURL, tc.wantURL)
			}
			if gotAuth != "Bearer sk-test" {
				t.Errorf("Authorization = %q", gotAuth)
			}
			if gotModel != bailian.Model {
				t.Errorf("model = %q, want %q", gotModel, bailian.Model)
			}
		})
	}
}

// TestNewClient_ParsesBailianResponse：百炼的响应三种答案都解析出来，请求 ID 从 x-request-id
// 读，usage 只有输入 token。
func TestNewClient_ParsesBailianResponse(t *testing.T) {
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return reply(r, http.StatusOK, "req-bl-1", bailianResponse), nil
	})}
	c := bailian.NewClient("sk-test", bailian.Beijing, "llm-test01", decision.WithHTTPClient(hc))
	resp, err := c.Decide(context.Background(), ticketRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if resp.Model != bailian.Model || resp.RequestID != "req-bl-1" {
		t.Errorf("model/request id = %q/%q", resp.Model, resp.RequestID)
	}
	if resp.Usage.InputTokens != 91 || resp.Usage.OutputTokens != 0 {
		t.Errorf("usage = %+v", resp.Usage)
	}
	d := resp.Answers["department"]
	if d.Choice != "billing" || d.Confidence == nil || *d.Confidence != 0.38 || d.Probabilities["technical"] != 0.31 {
		t.Errorf("choice answer parsed wrong: %+v", d)
	}
	e := resp.Answers["escalate"]
	if e.Noul == nil || *e.Noul != 0.9 {
		t.Errorf("noul answer parsed wrong: %+v", e)
	}
	s := resp.Answers["severity"]
	if s.Score == nil || *s.Score != 2.59 || s.Confidence == nil || *s.Confidence != 0.86 || s.Legend["3"] != "严重影响" {
		t.Errorf("score answer parsed wrong: %+v", s)
	}
}

// TestNewClient_GatewayErrorCarriesRequestID：网关拒掉的请求（这里是 key 无效）只带
// x-request-id 头。错误能取到状态码，标识是 bailian，文本带请求 ID；401 不重试。
func TestNewClient_GatewayErrorCarriesRequestID(t *testing.T) {
	var hits atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		hits.Add(1)
		return reply(r, http.StatusUnauthorized, "req-bl-401",
			`{"error":{"message":"Invalid API-key provided.","id":"req-bl-401","type":"invalid_request_error","code":"invalid_api_key"}}`), nil
	})}
	c := bailian.NewClient("sk-bad", bailian.Beijing, "llm-test01", decision.WithHTTPClient(hc),
		decision.WithBackoff(time.Millisecond, 2*time.Millisecond))
	_, err := c.Decide(context.Background(), ticketRequest())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized || apiErr.Provider != "bailian" {
		t.Fatalf("expected a bailian 401 APIError, got %v", err)
	}
	if !strings.Contains(err.Error(), "bailian request req-bl-401") {
		t.Fatalf("error should carry the request id, got %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("401 must not be retried, got %d attempts", hits.Load())
	}
}

// TestNewClient_OptionsOverrideDefaults：选项在默认值之后应用，能换地址与模型；换了地址，
// 请求 ID 仍按百炼的响应头读。
func TestNewClient_OptionsOverrideDefaults(t *testing.T) {
	var path, gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body) // 解析失败时记空串，下面的断言会报出来
		gotModel = body.Model
		w.Header().Set("x-request-id", "req-gw")
		io.WriteString(w, bailianResponse)
	}))
	defer srv.Close()

	c := bailian.NewClient("sk-test", bailian.Beijing, "llm-test01",
		decision.WithModel("decision-model-x"), decision.WithBaseURL(srv.URL+"/compatible-mode"))
	resp, err := c.Decide(context.Background(), ticketRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if path != "/compatible-mode/v1/systemone" || gotModel != "decision-model-x" {
		t.Errorf("path/model = %q/%q", path, gotModel)
	}
	if resp.RequestID != "req-gw" {
		t.Errorf("RequestID = %q", resp.RequestID)
	}
}

// TestNewClient_RejectsHostInjection：地域与业务空间 ID 要拼进域名，混进点、斜杠、@ 等
// 字符会把请求连同 key 发到别的主机，构造期 panic。
func TestNewClient_RejectsHostInjection(t *testing.T) {
	cases := map[string]func(){
		"empty workspace":         func() { bailian.NewClient("k", bailian.Beijing, " ") },
		"dot in workspace":        func() { bailian.NewClient("k", bailian.Beijing, "evil.com") },
		"slash in workspace":      func() { bailian.NewClient("k", bailian.Beijing, "evil.com/x") },
		"at in workspace":         func() { bailian.NewClient("k", bailian.Beijing, "user@evil") },
		"leading hyphen":          func() { bailian.NewClient("k", bailian.Beijing, "-llm") },
		"trailing hyphen":         func() { bailian.NewClient("k", bailian.Beijing, "llm-") },
		"workspace over 63 chars": func() { bailian.NewClient("k", bailian.Beijing, strings.Repeat("a", 64)) },
		"empty region":            func() { bailian.NewClient("k", "", "llm-test01") },
		"dot in region":           func() { bailian.NewClient("k", "cn-beijing.evil", "llm-test01") },
		"empty key":               func() { bailian.NewClient(" ", bailian.Beijing, "llm-test01") },
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

// TestRouteDecider_BailianAnswerMissingNamesBailian：百炼的响应里没有路由那一问的答案时，
// 路由决策方报错，错误标明是百炼的请求并带请求 ID。
func TestRouteDecider_BailianAnswerMissingNamesBailian(t *testing.T) {
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return reply(r, http.StatusOK, "req-bl-route",
			`{"model":"decision-model-preview","request_id":"req-bl-route","answers":{},"usage":{"input_tokens":5}}`), nil
	})}
	decide := decision.NewRouteDecider(bailian.NewClient("sk-test", bailian.Beijing, "llm-test01", decision.WithHTTPClient(hc)))
	_, err := decide(context.Background(), agent_v2.RouteRequest{
		Input:      schema.UserMessage("退款"),
		Candidates: []agent_v2.RoutingChoice{{Name: "billing"}, {Name: "tech"}},
	})
	if err == nil || !strings.Contains(err.Error(), "bailian request req-bl-route") {
		t.Fatalf("expected an error naming the bailian request, got %v", err)
	}
}
