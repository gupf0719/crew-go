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

// TestWithAllowedTools_FiltersRequestTools 验证 model.WithAllowedTools 让 openai client
// 只把白名单工具发进 request.tools（其余裁掉）。httptest 捕获真实出站请求体断言。
//
// bomb 防御：bind [alpha,beta] 但白名单只给 ["beta"]——若 client 未消费 AllowedTools，
// 捕获到的 tools 会是 [alpha,beta]（长度 2），断言长度 1 + 名字 beta 必 fail 指向 bug。
func TestWithAllowedTools_FiltersRequestTools(t *testing.T) {
	var capturedTools []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("解析请求体: %v", err)
		}
		for _, tl := range req.Tools {
			capturedTools = append(capturedTools, tl.Function.Name)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","object":"chat.completion","created":0,"model":"m",`+
			`"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer srv.Close()

	c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL))
	bound, err := c.WithTools([]*schema.ToolInfo{
		{Name: "alpha", Desc: "a", Params: map[string]any{"type": "object"}},
		{Name: "beta", Desc: "b", Params: map[string]any{"type": "object"}},
	})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	if _, err := bound.Generate(context.Background(),
		[]*schema.Message{schema.UserMessage("hi")},
		model.WithAllowedTools("beta"),
	); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(capturedTools) != 1 || capturedTools[0] != "beta" {
		t.Fatalf("AllowedTools 应只发 [beta]，实际 %v", capturedTools)
	}
}
