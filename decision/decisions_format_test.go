package decision_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/decision"
	"crew-go/schema"
)

// decisionsResponse 照一次真实的 OpenAI Decisions 格式响应改写（2026-10-16），问题换成
// sampleRequest 那三道：answers 是按请求顺序的数组；打分题的候选值是数字，同时给了 legend；usage
// 只有输入 token。
const decisionsResponse = `{"model":"decision-model-preview","request_id":"req-d-1","answers":[
 {"name":"department","type":"choice","confidence":0.78,"choice":"technical","probabilities":[
   {"value":"billing","probability":0.15},{"value":"sales","probability":0.0},{"value":"technical","probability":0.85}]},
 {"name":"frustration","type":"score","confidence":1.0,"score":1.0,"probabilities":[
   {"value":0,"label":"calm","probability":0.0},{"value":1,"label":"frustrated","probability":1.0},
   {"value":2,"label":"angry","probability":0.0}],
  "legend":{"0":"calm","1":"frustrated","2":"angry"}},
 {"name":"is_urgent","type":"predicate","probability":1.0}],
 "usage":{"input_tokens":76},"latency_ms":40.1}`

// decisionsServer 起一个按 OpenAI Decisions 格式回复的桩，记下请求路径与请求体，统计请求次数。
func decisionsServer(t *testing.T, reply string) (url string, path *string, body *map[string]any, hits *atomic.Int32) {
	t.Helper()
	var gotPath string
	var gotBody map[string]any
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody) // 解析失败时为 nil，下面的断言会报出来
		w.Header().Set("x-acme-id", "acme_ok")
		io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &gotPath, &gotBody, &n
}

func decisionsClient(url string) *decision.Client {
	return decision.NewClient("k", decision.Preset{
		Provider:        "acme",
		BaseURL:         url,
		DefaultModel:    "acme-1",
		RequestIDHeader: "x-acme-id",
		Protocol:        decision.ProtocolOpenAIDecisions,
	}, fastRetry())
}

// questionsByName 把请求体里的 questions 数组按 name 取出来，同时返回名字的出现顺序。
func questionsByName(t *testing.T, body map[string]any) (map[string]map[string]any, []string) {
	t.Helper()
	list, ok := body["questions"].([]any)
	if !ok {
		t.Fatalf("questions should be an array, got %T", body["questions"])
	}
	byName := map[string]map[string]any{}
	var order []string
	for _, item := range list {
		q := item.(map[string]any)
		name := q["name"].(string)
		byName[name] = q
		order = append(order, name)
	}
	return byName, order
}

// TestDecisionsProtocol_RequestShape：请求打到 /v1/decisions；对象形的 State 编成 JSON 文本作 input；
// 问题按名字排序；是非题叫 predicate，criteria 里的是与否拼到 instructions 后面（null 的那一项不拼）；
// 选项与档位变成数组，说明不是文本时编成 JSON 文本、为 null 时不带；instructions 为空时用问题名。
func TestDecisionsProtocol_RequestShape(t *testing.T) {
	url, path, body, _ := decisionsServer(t, `{"model":"acme-1","answers":[],"usage":{"input_tokens":1}}`)
	c := decisionsClient(url)
	_, err := c.Decide(context.Background(), &decision.Request{
		State: map[string]any{"content": "退款还没到账", "vip": true},
		Questions: map[string]decision.Question{
			"route":   decision.Choice("交给哪个团队", map[string]string{"tech": "故障", "billing": "账单"}),
			"urgency": decision.Score("", []string{"不急", "紧急"}),
			"angry": {Type: decision.TypeNoul, Instructions: "用户是否生气",
				Criteria: map[string]any{"true": "语气激烈", "false": nil}},
			"detail": {Type: decision.TypeChoice, Instructions: "看哪一类",
				Criteria: map[string]any{"a": map[string]any{"k": "v"}, "b": nil}},
		},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if *path != "/v1/decisions" {
		t.Errorf("path = %q, want /v1/decisions", *path)
	}
	if (*body)["model"] != "acme-1" {
		t.Errorf("model = %v", (*body)["model"])
	}
	if (*body)["input"] != `{"content":"退款还没到账","vip":true}` {
		t.Errorf("input = %#v, want the state as JSON text", (*body)["input"])
	}
	if _, ok := (*body)["state"]; ok {
		t.Errorf("the Decisions format has no state field: %v", *body)
	}
	qs, order := questionsByName(t, *body)
	if strings.Join(order, ",") != "angry,detail,route,urgency" {
		t.Errorf("questions should be sorted by name, got %v", order)
	}
	if qs["angry"]["type"] != "predicate" || qs["angry"]["instructions"] != "用户是否生气\nTrue: 语气激烈" {
		t.Errorf("noul question encoded wrong: %v", qs["angry"])
	}
	wantChoices := `[{"description":"账单","value":"billing"},{"description":"故障","value":"tech"}]`
	if got, _ := json.Marshal(qs["route"]["choices"]); qs["route"]["type"] != "choice" || string(got) != wantChoices {
		t.Errorf("choice question encoded wrong: %v (choices %s)", qs["route"], got)
	}
	wantDetail := `[{"description":"{\"k\":\"v\"}","value":"a"},{"value":"b"}]`
	if got, _ := json.Marshal(qs["detail"]["choices"]); string(got) != wantDetail {
		t.Errorf("non-text and null descriptions encoded wrong: %s", got)
	}
	wantLevels := `[{"label":"不急"},{"label":"紧急"}]`
	if got, _ := json.Marshal(qs["urgency"]["levels"]); qs["urgency"]["type"] != "score" || string(got) != wantLevels {
		t.Errorf("score question encoded wrong: %v (levels %s)", qs["urgency"], got)
	}
	if qs["urgency"]["instructions"] != "urgency" {
		t.Errorf("empty instructions should fall back to the question name, got %v", qs["urgency"]["instructions"])
	}
}

// TestDecisionsProtocol_DecodesAnswers：文本形的 State 原样作 input；答案按 name 放回 Answers，
// predicate 的概率填进 Noul、类型记成 noul，打分题的数字候选值记成 "0"、"1"……，与 System One 格式
// 解出来的一样；请求 ID 照常从响应头读。
// TestDecisionsProtocol_ObjectTextKeepsCharacters：对象形的 State、选项说明与档位转成文本时 < > & 原样
// 保留。encoding/json 默认把它们编成 < 这样的转义，而这份 JSON 本身就是交给模型读的文本，转义会
// 原样到达模型（实测模型读到转义字样时输入 token 变多、置信度也跟着变）。
func TestDecisionsProtocol_ObjectTextKeepsCharacters(t *testing.T) {
	url, _, body, _ := decisionsServer(t, `{"model":"acme-1","answers":[],"usage":{"input_tokens":1}}`)
	_, err := decisionsClient(url).Decide(context.Background(), &decision.Request{
		State: map[string]string{"message": "价格<100 & 包邮 > 运费"},
		Questions: map[string]decision.Question{
			"level": {Type: decision.TypeChoice, Criteria: map[string]any{"low": map[string]string{"desc": "<低>"}}},
			"tier":  {Type: decision.TypeScore, Criteria: []any{map[string]string{"desc": "a&b"}}},
		},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if got := (*body)["input"]; got != `{"message":"价格<100 & 包邮 > 运费"}` {
		t.Errorf("input = %#v, want < > & kept as is", got)
	}
	qs, _ := questionsByName(t, *body)
	var desc, label any
	if cs, _ := qs["level"]["choices"].([]any); len(cs) == 1 {
		if m, ok := cs[0].(map[string]any); ok {
			desc = m["description"]
		}
	}
	if desc != `{"desc":"<低>"}` {
		t.Errorf("choice description = %#v, want < > kept as is", desc)
	}
	if ls, _ := qs["tier"]["levels"].([]any); len(ls) == 1 {
		if m, ok := ls[0].(map[string]any); ok {
			label = m["label"]
		}
	}
	if label != `{"desc":"a&b"}` {
		t.Errorf("score level = %#v, want & kept as is", label)
	}
}

func TestDecisionsProtocol_DecodesAnswers(t *testing.T) {
	url, _, body, _ := decisionsServer(t, decisionsResponse)
	resp, err := decisionsClient(url).Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if (*body)["input"] != sampleRequest().State {
		t.Errorf("input = %#v, want the text state as is", (*body)["input"])
	}
	if resp.Model != "decision-model-preview" || resp.RequestID != "acme_ok" || resp.Usage.InputTokens != 76 {
		t.Errorf("model/request id/usage = %q/%q/%+v", resp.Model, resp.RequestID, resp.Usage)
	}
	d := resp.Answers["department"]
	if d.Type != decision.TypeChoice || d.Choice != "technical" || d.Confidence == nil || *d.Confidence != 0.78 ||
		d.Probabilities["technical"] != 0.85 || len(d.Probabilities) != 3 {
		t.Errorf("choice answer decoded wrong: %+v", d)
	}
	f := resp.Answers["frustration"]
	if f.Type != decision.TypeScore || f.Score == nil || *f.Score != 1.0 || f.Probabilities["1"] != 1.0 ||
		f.Legend["2"] != "angry" || len(f.Probabilities) != 3 {
		t.Errorf("score answer decoded wrong: %+v", f)
	}
	u := resp.Answers["is_urgent"]
	if u.Type != decision.TypeNoul || u.Noul == nil || *u.Noul != 1.0 || u.Refused {
		t.Errorf("predicate answer decoded wrong: %+v", u)
	}
}

// TestDecisionsProtocol_LegendFromLabels：打分题的答案没带 legend 时，按概率数组里每档的 label 补上。
func TestDecisionsProtocol_LegendFromLabels(t *testing.T) {
	reply := `{"model":"m","usage":{"input_tokens":1},"answers":[
	 {"name":"frustration","type":"score","confidence":0.5,"score":0.4,"probabilities":[
	   {"value":0,"label":"calm","probability":0.6},{"value":1,"label":"frustrated","probability":0.3},
	   {"value":2,"label":"angry","probability":0.1}]}]}`
	url, _, _, _ := decisionsServer(t, reply)
	resp, err := decisionsClient(url).Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	f := resp.Answers["frustration"]
	if f.Legend["0"] != "calm" || f.Legend["2"] != "angry" || f.Probabilities["0"] != 0.6 {
		t.Errorf("legend should come from the labels: %+v", f)
	}
}

// TestDecisionsProtocol_RefusalAbstains：服务端对路由那一问拒答时，答案记为 Refused、类型是请求里的
// choice，路由决策方弃权（Target 为空、不报错），RoutingAgent 会把它交给下一档。
func TestDecisionsProtocol_RefusalAbstains(t *testing.T) {
	url, _, _, _ := decisionsServer(t, `{"model":"decision-model-preview","usage":{"input_tokens":5},
	 "answers":[{"name":"route","type":"refusal","refusal":"cannot decide"}]}`)
	c := decisionsClient(url)
	decide := decision.NewRouteDecider(c)
	dec, err := decide(context.Background(), agent_v2.RouteRequest{
		Input:      schema.UserMessage("帮我看看这个"),
		Candidates: []agent_v2.RoutingChoice{{Name: "billing"}, {Name: "tech"}},
	})
	if err != nil {
		t.Fatalf("a refusal should make the decider abstain, got error %v", err)
	}
	if dec.Target != "" || dec.Source != "decision-model-preview" {
		t.Errorf("a refusal should abstain with the model as source, got %+v", dec)
	}

	resp, err := c.Decide(context.Background(), &decision.Request{
		State:     "x",
		Questions: map[string]decision.Question{"route": decision.Choice("哪个", map[string]string{"a": "", "b": ""})},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if a := resp.Answers["route"]; !a.Refused || a.Type != decision.TypeChoice || a.Choice != "" {
		t.Errorf("refusal decoded wrong: %+v", a)
	}
}

// TestDecisionsProtocol_SkipsUnknownAnswerType：认不得的答案类型跳过，别的答案照常返回，与 System One
// 格式一致。
func TestDecisionsProtocol_SkipsUnknownAnswerType(t *testing.T) {
	url, _, _, _ := decisionsServer(t, `{"model":"m","usage":{"input_tokens":1},"answers":[
	 {"name":"is_urgent","type":"future_kind","value":3},
	 {"name":"department","type":"choice","confidence":0.9,"choice":"billing","probabilities":[{"value":"billing","probability":0.95}]}]}`)
	resp, err := decisionsClient(url).Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if _, ok := resp.Answers["is_urgent"]; ok {
		t.Errorf("an unknown answer type should be skipped: %+v", resp.Answers)
	}
	if resp.Answers["department"].Choice != "billing" {
		t.Errorf("the other answers should still be returned: %+v", resp.Answers)
	}
}

// TestDecisionsProtocol_RejectsMalformedAnswers：响应形状不对时报错，而且只请求一次（重试拿到的
// 一样）。
func TestDecisionsProtocol_RejectsMalformedAnswers(t *testing.T) {
	cases := []struct {
		name    string
		answers string
		want    string
	}{
		{"answers 不是数组", `{"is_urgent":{"type":"predicate","probability":1}}`, "decode response answers"},
		{"答案没有 name", `[{"type":"predicate","probability":1}]`, "has no name"},
		{"答的是没问过的问题", `[{"name":"other","type":"predicate","probability":1}]`, "was not asked"},
		{"同一问答了两次", `[{"name":"is_urgent","type":"predicate","probability":1},{"name":"is_urgent","type":"predicate","probability":0}]`, "more than one answer"},
		{"predicate 缺概率", `[{"name":"is_urgent","type":"predicate"}]`, "missing probability"},
		{"choice 的概率为 null", `[{"name":"department","type":"choice","confidence":0.5,"choice":"sales","probabilities":[{"value":"sales","probability":null}]}]`, "null probability"},
		{"score 既没有 legend 也没有 label", `[{"name":"frustration","type":"score","confidence":0.5,"score":1,"probabilities":[{"value":0,"probability":1}]}]`, "missing legend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, _, _, hits := decisionsServer(t, `{"model":"m","usage":{"input_tokens":1},"answers":`+tc.answers+`}`)
			_, err := decisionsClient(url).Decide(context.Background(), sampleRequest())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
			if hits.Load() != 1 {
				t.Fatalf("a malformed response must not be retried, got %d attempts", hits.Load())
			}
		})
	}
}

// TestNewClient_RejectsUnknownProtocol：Preset 的 Protocol 不是已知的两种时构造期 panic。
func TestNewClient_RejectsUnknownProtocol(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for an unknown protocol")
		}
	}()
	decision.NewClient("k", decision.Preset{
		Provider: "acme", BaseURL: "https://example.com", DefaultModel: "m", Protocol: decision.Protocol(99),
	})
}
