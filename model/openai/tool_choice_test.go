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

// toolChoiceServer 记下每次请求体里有没有 tool_choice、是什么。
func toolChoiceServer(t *testing.T, got *[]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("解析请求体: %v", err)
		}
		*got = append(*got, req["tool_choice"])
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","object":"chat.completion","created":0,"model":"m",`+
			`"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
}

// TestToolChoice_DroppedWhenNoTools：请求里没有工具时不发 tool_choice——「必须调工具」配「没有工具」provider 直接
// 拒绝（required 返 400）。没绑工具、AllowedTools 把工具滤光两种都算；有工具时照发（对照）。
func TestToolChoice_DroppedWhenNoTools(t *testing.T) {
	var got []any
	srv := toolChoiceServer(t, &got)
	defer srv.Close()
	ctx := context.Background()
	in := []*schema.Message{schema.UserMessage("hi")}
	required := model.WithToolChoice(schema.ToolChoiceRequired)

	c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL))
	if _, err := c.Generate(ctx, in, required); err != nil {
		t.Fatalf("没绑工具: %v", err)
	}
	bound, err := c.WithTools([]*schema.ToolInfo{{Name: "alpha", Desc: "a", Params: map[string]any{"type": "object"}}})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	if _, err := bound.Generate(ctx, in, required, model.WithAllowedTools("beta")); err != nil {
		t.Fatalf("白名单滤光: %v", err)
	}
	if _, err := bound.Generate(ctx, in, required); err != nil {
		t.Fatalf("有工具: %v", err)
	}

	if len(got) != 3 || got[0] != nil || got[1] != nil || got[2] != "required" {
		t.Fatalf("没有工具时不该发 tool_choice、有工具时照发 required，实际 %v", got)
	}
}

// TestToolChoice_NoneSentWithTools：none 跟 required 走同一条规则——请求里有工具时原样发 "none"，没有工具时不发。
// BasicAgent 的软降落轮靠它：工具照旧绑着，只用 none 关掉工具调用。
func TestToolChoice_NoneSentWithTools(t *testing.T) {
	var got []any
	srv := toolChoiceServer(t, &got)
	defer srv.Close()
	ctx := context.Background()
	in := []*schema.Message{schema.UserMessage("hi")}
	none := model.WithToolChoice(schema.ToolChoiceNone)

	c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL))
	if _, err := c.Generate(ctx, in, none); err != nil {
		t.Fatalf("没绑工具: %v", err)
	}
	bound, err := c.WithTools([]*schema.ToolInfo{{Name: "alpha", Desc: "a", Params: map[string]any{"type": "object"}}})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	if _, err := bound.Generate(ctx, in, none); err != nil {
		t.Fatalf("有工具: %v", err)
	}

	if len(got) != 2 || got[0] != nil || got[1] != "none" {
		t.Fatalf("没有工具时不该发 tool_choice、有工具时照发 none，实际 %v", got)
	}
}

// TestToolChoice_ExtraBodyToolsDecide：「有没有工具」看合并 ExtraBody 之后的请求体——ExtraBody 带了非空 tools
// （provider 内置工具常走这条）时照发 tool_choice；ExtraBody 用空 tools 把绑定的工具清掉时不发。曾经只看绑定的
// 工具，前一种静默丢了业务强制的 tool_choice。
func TestToolChoice_ExtraBodyToolsDecide(t *testing.T) {
	var got []any
	srv := toolChoiceServer(t, &got)
	defer srv.Close()
	ctx := context.Background()
	in := []*schema.Message{schema.UserMessage("hi")}
	required := model.WithToolChoice(schema.ToolChoiceRequired)

	c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL))
	builtin := model.WithExtraBody(map[string]any{"tools": []any{map[string]any{"type": "web_search"}}})
	if _, err := c.Generate(ctx, in, required, builtin); err != nil {
		t.Fatalf("ExtraBody 带工具: %v", err)
	}
	bound, err := c.WithTools([]*schema.ToolInfo{{Name: "alpha", Desc: "a", Params: map[string]any{"type": "object"}}})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	if _, err := bound.Generate(ctx, in, required, model.WithExtraBody(map[string]any{"tools": []any{}})); err != nil {
		t.Fatalf("ExtraBody 清空工具: %v", err)
	}

	if len(got) != 2 || got[0] != "required" || got[1] != nil {
		t.Fatalf("ExtraBody 带非空 tools 时应发 required、清空 tools 时不发，实际 %v", got)
	}
}
