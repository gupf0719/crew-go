package openai_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

// fakeChatServer 返回一个最小合法的 chat completion mock，并把收到的请求体解析进 capture
// （mock 读 input 做断言——验"Preset 是否真的落进 wire 请求"，而非固定响应）。
func fakeChatServer(t *testing.T, capture *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m := map[string]any{}
		_ = json.Unmarshal(body, &m)
		*capture = m
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
}

// TestNewCompatible_AppliesPreset 验 Preset 的 BaseURL / DefaultModel / ExtraBody / System
// 都真的生效（打到 mock server + 写进请求体 + 回填 Metadata）。
func TestNewCompatible_AppliesPreset(t *testing.T) {
	var got map[string]any
	srv := fakeChatServer(t, &got)
	defer srv.Close()

	p := openai.Preset{
		System:       "zhipu",
		BaseURL:      srv.URL,
		DefaultModel: "glm-test",
		ExtraBody:    map[string]any{"enable_thinking": false},
	}
	m := openai.NewCompatible(p, "key", "") // modelName 空 → 用 DefaultModel

	resp, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if resp == nil || resp.Content != "ok" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	if got["model"] != "glm-test" {
		t.Errorf("model = %v, want glm-test (DefaultModel 未生效)", got["model"])
	}
	if got["enable_thinking"] != false {
		t.Errorf("enable_thinking = %v, want false (ExtraBody 未 merge)", got["enable_thinking"])
	}
	if resp.Metadata == nil || resp.Metadata.System != "zhipu" {
		t.Errorf("metadata.System = %+v, want zhipu (System 未回填)", resp.Metadata)
	}
}

// TestNewCompatible_OptsOverridePreset 验后置 opts 覆盖 Preset：显式 modelName 覆盖
// DefaultModel、WithBaseURL 覆盖 Preset.BaseURL、WithExtraBody 同 key 后值覆盖。
func TestNewCompatible_OptsOverridePreset(t *testing.T) {
	var got map[string]any
	srv := fakeChatServer(t, &got)
	defer srv.Close()

	p := openai.Preset{
		System:       "deepseek",
		BaseURL:      "http://preset-base-would-be-wrong.invalid",
		DefaultModel: "deepseek-default",
		ExtraBody:    map[string]any{"thinking": map[string]any{"type": "disabled"}},
	}
	m := openai.NewCompatible(p, "key", "override-model",
		openai.WithBaseURL(srv.URL), // 覆盖 Preset.BaseURL（否则打到 .invalid）
		openai.WithExtraBody(map[string]any{"thinking": map[string]any{"type": "enabled"}}),
	)
	if _, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatalf("generate: %v", err)
	}
	if got["model"] != "override-model" {
		t.Errorf("model = %v, want override-model (显式 modelName 未覆盖 DefaultModel)", got["model"])
	}
	th, _ := got["thinking"].(map[string]any)
	if th == nil || th["type"] != "enabled" {
		t.Errorf("thinking = %v, want type=enabled (后置 ExtraBody 未覆盖 Preset 同 key)", got["thinking"])
	}
}

// TestWithExtraBody_PerRequestMergesAndOverrides 验 per-request model.WithExtraBody
// 叠在构造期 openai.WithExtraBody 之上：同 key per-request 赢、构造期独有 key 保留。
// 这是"按次切 thinking"的底层机制（B1）。
func TestWithExtraBody_PerRequestMergesAndOverrides(t *testing.T) {
	var got map[string]any
	srv := fakeChatServer(t, &got)
	defer srv.Close()

	// 构造期默认：thinking disabled + 一个只在构造期设的 key。
	m := openai.NewClient("key", "m",
		openai.WithBaseURL(srv.URL),
		openai.WithExtraBody(map[string]any{
			"thinking":  map[string]any{"type": "disabled"},
			"ctor_only": true,
		}),
	)
	// per-request：把 thinking 覆盖成 enabled（同 key 赢）；ctor_only 不传（应保留）。
	_, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")},
		model.WithExtraBody(map[string]any{"thinking": map[string]any{"type": "enabled"}}),
	)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	th, _ := got["thinking"].(map[string]any)
	if th == nil || th["type"] != "enabled" {
		t.Errorf("thinking = %v, want type=enabled (per-request 未覆盖构造期同 key)", got["thinking"])
	}
	if got["ctor_only"] != true {
		t.Errorf("ctor_only = %v, want true (构造期独有 key 未保留——per-request 不该整体替换)", got["ctor_only"])
	}
}
