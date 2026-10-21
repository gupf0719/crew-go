package decision_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crew-go/decision"
	"crew-go/model"
)

// TestNewClient_PresetDecidesProviderAndRequestIDHeader：服务方标识与请求 ID 的响应头取自
// Preset。成功响应的 RequestID 读 Preset 指定的头，错误的 Provider 与错误文本用 Preset 的
// 标识；请求没指定模型时用 Preset 的默认模型。
func TestNewClient_PresetDecidesProviderAndRequestIDHeader(t *testing.T) {
	var models []string
	fail := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body) // 解析失败时记空串，下面的断言会报出来
		models = append(models, body.Model)
		if fail {
			w.Header().Set("x-acme-id", "acme_err")
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"error":"bad"}`)
			return
		}
		w.Header().Set("x-acme-id", "acme_ok")
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := decision.NewClient("k", decision.Preset{
		Provider:        "acme",
		BaseURL:         srv.URL,
		DefaultModel:    "acme-1",
		RequestIDHeader: "x-acme-id",
	})
	_, err := c.Decide(context.Background(), sampleRequest())
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.Provider != "acme" || apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected an acme 400 APIError, got %v", err)
	}
	if !strings.Contains(err.Error(), "acme request acme_err") {
		t.Fatalf("error should name the provider and carry its request id, got %v", err)
	}

	fail = false
	resp, err := c.Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if resp.RequestID != "acme_ok" {
		t.Fatalf("RequestID should come from the preset's header, got %q", resp.RequestID)
	}
	if len(models) != 2 || models[0] != "acme-1" || models[1] != "acme-1" {
		t.Fatalf("requests without a model should use the preset's default model, sent %v", models)
	}
}

// TestNewClient_EmptyRequestIDHeaderReadsNothing：Preset 没给请求 ID 的响应头时 RequestID
// 为空，不去读别的头。
func TestNewClient_EmptyRequestIDHeaderReadsNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-request-id", "proxy_id")
		w.Header().Set("x-typesafe-request-id", "other_id")
		io.WriteString(w, sampleResponse)
	}))
	defer srv.Close()

	c := decision.NewClient("k", decision.Preset{Provider: "acme", BaseURL: srv.URL, DefaultModel: "acme-1"})
	resp, err := c.Decide(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if resp.RequestID != "" {
		t.Fatalf("a preset without a request id header should leave RequestID empty, got %q", resp.RequestID)
	}
}

// TestNewClient_RejectsInvalidPreset：只看 Preset 本身就能判定的错误，构造期 panic。
func TestNewClient_RejectsInvalidPreset(t *testing.T) {
	valid := decision.Preset{Provider: "acme", BaseURL: "https://acme.example", DefaultModel: "acme-1"}
	cases := map[string]func(){
		"empty key":             func() { decision.NewClient(" ", valid) },
		"empty provider":        func() { p := valid; p.Provider = " "; decision.NewClient("k", p) },
		"empty default model":   func() { p := valid; p.DefaultModel = ""; decision.NewClient("k", p) },
		"empty base url":        func() { p := valid; p.BaseURL = ""; decision.NewClient("k", p) },
		"base url not http":     func() { p := valid; p.BaseURL = "ftp://acme.example"; decision.NewClient("k", p) },
		"base url with query":   func() { p := valid; p.BaseURL = "https://acme.example?x=1"; decision.NewClient("k", p) },
		"base url without host": func() { p := valid; p.BaseURL = "https:///path"; decision.NewClient("k", p) },
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
