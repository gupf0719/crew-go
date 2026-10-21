package prompt_test

import (
	"strings"
	"testing"

	"crew-go/prompt"
	"crew-go/schema"
)

// TestFormat_VariableBinding 验证文本段变量绑定 + 角色。
func TestFormat_VariableBinding(t *testing.T) {
	tmpl := prompt.FromMessages(
		prompt.System("你是 {{.role}}，用 {{.lang}} 回答。"),
		prompt.User("{{.q}}"),
	)
	msgs, err := tmpl.Format(map[string]any{"role": "助手", "lang": "中文", "q": "你好"})
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("应 2 条消息，实际 %d", len(msgs))
	}
	if msgs[0].Role != schema.RoleSystem || msgs[0].Content != "你是 助手，用 中文 回答。" {
		t.Fatalf("system 渲染错: %+v", msgs[0])
	}
	if msgs[1].Role != schema.RoleUser || msgs[1].Content != "你好" {
		t.Fatalf("user 渲染错: %+v", msgs[1])
	}
}

// TestFormat_Placeholder 验证消息列表占位按位插入。
func TestFormat_Placeholder(t *testing.T) {
	examples := []*schema.Message{
		schema.UserMessage("例1问"),
		schema.AssistantMessage("例1答"),
	}
	tmpl := prompt.FromMessages(
		prompt.System("sys"),
		prompt.Placeholder("examples", false),
		prompt.User("{{.q}}"),
	)
	msgs, err := tmpl.Format(map[string]any{"examples": examples, "q": "真问题"})
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	// 期望: [sys, 例1问, 例1答, 真问题]
	if len(msgs) != 4 {
		t.Fatalf("占位应插入 2 条 → 共 4 条，实际 %d: %+v", len(msgs), msgs)
	}
	if msgs[1].Content != "例1问" || msgs[2].Content != "例1答" || msgs[3].Content != "真问题" {
		t.Fatalf("占位插入位置错: %+v", msgs)
	}
}

// TestFormat_OptionalPlaceholderMissing 验证 optional 占位 key 缺失时跳过不报错。
func TestFormat_OptionalPlaceholderMissing(t *testing.T) {
	tmpl := prompt.FromMessages(
		prompt.System("sys"),
		prompt.Placeholder("history", true), // optional
		prompt.User("{{.q}}"),
	)
	msgs, err := tmpl.Format(map[string]any{"q": "x"}) // 无 history
	if err != nil {
		t.Fatalf("optional 缺失不应报错: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("optional 缺失应跳过 → 2 条，实际 %d", len(msgs))
	}
}

// TestFormat_MissingVariableErrors 验证缺变量报错（missingkey=error）。
func TestFormat_MissingVariableErrors(t *testing.T) {
	tmpl := prompt.FromMessages(prompt.System("hi {{.name}}"))
	_, err := tmpl.Format(map[string]any{}) // 无 name
	if err == nil {
		t.Fatal("缺变量应报错")
	}
}

// TestFormat_RequiredPlaceholderMissingErrors 验证非 optional 占位缺失报错。
func TestFormat_RequiredPlaceholderMissingErrors(t *testing.T) {
	tmpl := prompt.FromMessages(prompt.Placeholder("must", false))
	_, err := tmpl.Format(map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "must") {
		t.Fatalf("非 optional 占位缺失应报错且含 key 名，实际 err=%v", err)
	}
}
