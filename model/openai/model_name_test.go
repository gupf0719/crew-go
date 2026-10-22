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

// 客户端报构造时配的模型名，WithTools 派生的实例也报同一个——框架拿它当请求模型交给钩子。
func TestClient_ModelName(t *testing.T) {
	c := openai.NewClient("k", "gpt-x")
	if c.ModelName() != "gpt-x" {
		t.Fatalf("ModelName=%q", c.ModelName())
	}
	bound, err := c.WithTools([]*schema.ToolInfo{{Name: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bound.(model.ModelNamer); !ok || n.ModelName() != "gpt-x" {
		t.Errorf("WithTools 之后应仍报 gpt-x，ok=%v", ok)
	}
}

// TestClient_EmptyModelOverrideIgnored：按次的 model.WithModel("") 不算覆盖，照发构造时的模型名——跟 model.RequestConfig
// 报给钩子的一致（曾经照发空串）。
func TestClient_EmptyModelOverrideIgnored(t *testing.T) {
	var sent []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req) // 解不出来时 Model 为空，下面的断言会报
		sent = append(sent, req.Model)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","object":"chat.completion","created":0,"model":"m",`+
			`"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer srv.Close()
	c := openai.NewClient("k", "base-model", openai.WithBaseURL(srv.URL))
	msgs := []*schema.Message{schema.UserMessage("hi")}
	for _, opts := range [][]model.Option{{model.WithModel("")}, {model.WithModel("other")}} {
		if _, err := c.Generate(context.Background(), msgs, opts...); err != nil {
			t.Fatalf("Generate: %v", err)
		}
	}
	if len(sent) != 2 || sent[0] != "base-model" || sent[1] != "other" {
		t.Errorf("空覆盖应发 base-model、非空覆盖发 other，got %v", sent)
	}
	if got := model.RequestConfig(c, model.WithModel("")); got == nil || got.ModelName != sent[0] {
		t.Errorf("RequestConfig 报的应跟实际发的一致，got %+v", got)
	}
}
