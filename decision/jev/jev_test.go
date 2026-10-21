package jev_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"crew-go/decision"
	"crew-go/decision/jev"
)

// roundTripFunc 把函数当成 http.RoundTripper，用来在不联网的情况下看请求发到了哪里。
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestNewClient_DefaultsToTypeSafe：请求发到 TypeSafe 的官方地址、带 Bearer key，没指定
// 模型时用 jev-latest；请求 ID 从 x-typesafe-request-id 读。
func TestNewClient_DefaultsToTypeSafe(t *testing.T) {
	var gotURL, gotAuth, gotModel string
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotURL = r.URL.String()
		gotAuth = r.Header.Get("Authorization")
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body) // 解析失败时记空串，下面的断言会报出来
		gotModel = body.Model
		h := http.Header{}
		h.Set("x-typesafe-request-id", "req_ts")
		h.Set("x-request-id", "req_proxy")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     h,
			Body: io.NopCloser(strings.NewReader(
				`{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.7}},"usage":{"input_tokens":3,"output_tokens":1}}`)),
			Request: r,
		}, nil
	})}

	c := jev.NewClient("ts-key", decision.WithHTTPClient(hc))
	resp, err := c.Decide(context.Background(), &decision.Request{
		State:     "hi",
		Questions: map[string]decision.Question{"q": decision.Noul("is it urgent")},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if gotURL != "https://api.typesafe.ai/v1/systemone" {
		t.Errorf("request went to %q", gotURL)
	}
	if gotAuth != "Bearer ts-key" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotModel != jev.Model {
		t.Errorf("model = %q, want %q", gotModel, jev.Model)
	}
	if resp.RequestID != "req_ts" {
		t.Errorf("RequestID should come from x-typesafe-request-id, got %q", resp.RequestID)
	}
}
