package openai_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"crew-go/model/openai"
	"crew-go/schema"
)

// TestReasoningContent_ParsedFromResponse 验响应的 reasoning_content 被解析进
// schema.Message.ReasoningContent（thinking 模型多轮回传的前提，ISSUE-89）。
func TestReasoningContent_ParsedFromResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"答案","reasoning_content":"先想一下"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()

	m := openai.NewClient("k", "deepseek-reasoner", openai.WithBaseURL(srv.URL))
	resp, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if resp.Content != "答案" {
		t.Errorf("content = %q, want 答案", resp.Content)
	}
	if resp.ReasoningContent != "先想一下" {
		t.Errorf("ReasoningContent = %q, want 先想一下 (响应 reasoning_content 未解析)", resp.ReasoningContent)
	}
}

// TestReasoningContent_PassedBackInRequest 验带 ReasoningContent 的 assistant message
// 拼下一轮请求时把 reasoning_content 原样附回 wire——DeepSeek/Qwen3 thinking 多轮不 400
// 的关键（ISSUE-89）。同时验非 thinking 消息不附该字段（omitempty）。
func TestReasoningContent_PassedBackInRequest(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = map[string]any{}
		_ = json.Unmarshal(body, &got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()

	m := openai.NewClient("k", "deepseek-reasoner", openai.WithBaseURL(srv.URL))
	// 多轮历史：user → assistant(带 reasoning_content) → user
	history := []*schema.Message{
		schema.UserMessage("1+1?"),
		{Role: schema.RoleAssistant, Content: "2", ReasoningContent: "一加一等于二"},
		schema.UserMessage("再+1?"),
	}
	if _, err := m.Generate(context.Background(), history); err != nil {
		t.Fatalf("generate: %v", err)
	}
	msgs, _ := got["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3", len(msgs))
	}
	// assistant 轮附回 reasoning_content
	asst, _ := msgs[1].(map[string]any)
	if asst["reasoning_content"] != "一加一等于二" {
		t.Errorf("assistant.reasoning_content = %v, want 一加一等于二 (多轮回传丢失)", asst["reasoning_content"])
	}
	// user 轮不该有 reasoning_content（omitempty）
	usr, _ := msgs[0].(map[string]any)
	if _, has := usr["reasoning_content"]; has {
		t.Errorf("user 轮不该带 reasoning_content，got %v", usr["reasoning_content"])
	}
}
