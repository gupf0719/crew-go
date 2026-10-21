// Package prompt 提供轻量提示词模板——变量绑定（标准库 text/template）+ 消息列表占位
// （Placeholder），把模板渲染成 []*schema.Message。
//
// 用途：替代手写字符串拼接 system prompt，支持 {{.var}} 变量绑定 + 把一段消息列表
// （如 few-shot 示例 / 历史）按占位符插入。不做版本管理（交 git / 配置系统）。
//
// 示例：
//
//	tmpl := prompt.FromMessages(
//	    prompt.System("你是 {{.role}}，用 {{.lang}} 回答。"),
//	    prompt.Placeholder("examples", true), // few-shot，可选
//	    prompt.User("{{.question}}"),
//	)
//	msgs, err := tmpl.Format(map[string]any{
//	    "role": "助手", "lang": "中文", "question": "你好",
//	    "examples": []*schema.Message{ /* ... */ },
//	})
package prompt

import (
	"fmt"
	"strings"
	"text/template"

	"crew-go/schema"
)

// Part 是模板的一段：文本段（role + 含变量的文本）或消息列表占位段。用
// System / User / Assistant / Placeholder 构造，不直接构造。
type Part struct {
	role        schema.MessageRole
	text        string
	placeholder string // 非空 = 占位段，值是 vars 里的 key
	optional    bool   // 占位 key 缺失 / 类型不符时：true 跳过、false 报错
}

// System 系统消息模板段（text 可含 {{.var}} 变量）。
func System(text string) Part { return Part{role: schema.RoleSystem, text: text} }

// User 用户消息模板段。
func User(text string) Part { return Part{role: schema.RoleUser, text: text} }

// Assistant assistant 消息模板段。
func Assistant(text string) Part { return Part{role: schema.RoleAssistant, text: text} }

// Placeholder 消息列表占位段——Format 时从 vars[key] 取 []*schema.Message 原样插入。
// optional=true 时 key 缺失 / 类型不符则跳过，false 时报错。用于 few-shot / 历史注入。
func Placeholder(key string, optional bool) Part {
	return Part{placeholder: key, optional: optional}
}

// Template 是一组有序消息模板段，Format(vars) 渲染成具体 []*schema.Message。
type Template struct {
	parts []Part
}

// FromMessages 用有序段构造模板。
func FromMessages(parts ...Part) *Template {
	return &Template{parts: parts}
}

// Format 用 vars 渲染模板：文本段经 text/template 绑定变量（缺变量报错），占位段从
// vars 取消息列表插入。返回渲染后的消息列表。
func (t *Template) Format(vars map[string]any) ([]*schema.Message, error) {
	if t == nil {
		return nil, nil
	}
	out := make([]*schema.Message, 0, len(t.parts))
	for i, p := range t.parts {
		if p.placeholder != "" {
			msgs, err := resolvePlaceholder(p, vars)
			if err != nil {
				return nil, err
			}
			out = append(out, msgs...)
			continue
		}
		content, err := renderText(p.text, vars)
		if err != nil {
			return nil, fmt.Errorf("prompt: render part %d (%s): %w", i, p.role, err)
		}
		out = append(out, &schema.Message{Role: p.role, Content: content})
	}
	return out, nil
}

// renderText 用 text/template 渲染单段文本，缺变量报错（missingkey=error）。
func renderText(text string, vars map[string]any) (string, error) {
	tmpl, err := template.New("prompt").Option("missingkey=error").Parse(text)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, vars); err != nil {
		return "", fmt.Errorf("execute: %w", err)
	}
	return sb.String(), nil
}

// resolvePlaceholder 从 vars 取占位段对应的消息列表；缺失 / 类型不符按 optional 处理。
func resolvePlaceholder(p Part, vars map[string]any) ([]*schema.Message, error) {
	raw, ok := vars[p.placeholder]
	if !ok {
		if p.optional {
			return nil, nil
		}
		return nil, fmt.Errorf("prompt: placeholder %q missing in vars", p.placeholder)
	}
	msgs, ok := raw.([]*schema.Message)
	if !ok {
		if p.optional {
			return nil, nil
		}
		return nil, fmt.Errorf("prompt: placeholder %q must be []*schema.Message, got %T", p.placeholder, raw)
	}
	return msgs, nil
}
