package a2a

// 白盒测试未导出 helper（sanitizeName / firstNonEmpty / a2aPartsText / buildMessage）——
// 纯输入→输出，无需远程 server。

import (
	"testing"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

func TestSanitizeName(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"Hello World Agent", "hello_world_agent"}, // 空格 → _ + 小写
		{"currency-agent", "currency_agent"},       // 连字符 → _
		{"Agent.v2", "agent_v2"},                   // 点 → _
		{"ALLCAPS", "allcaps"},                     // 全大写 → 小写
		{"keep_underscore", "keep_underscore"},     // 下划线保留
		{"", "a2a_remote_agent"},                   // 空 → fallback
		{"!@#", "___"},                             // 全非法 → 全 _
	} {
		if got := sanitizeName(c.in); got != c.want {
			t.Errorf("sanitizeName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", "fb"); got != "fb" {
		t.Errorf(`firstNonEmpty("", "fb") = %q, want "fb"`, got)
	}
	if got := firstNonEmpty("x", "fb"); got != "x" {
		t.Errorf(`firstNonEmpty("x", "fb") = %q, want "x"`, got)
	}
	if got := firstNonEmpty("   ", "fb"); got != "fb" {
		t.Errorf(`firstNonEmpty("   ", "fb") = %q（全空白应回退）, want "fb"`, got)
	}
}

func TestA2APartsText(t *testing.T) {
	parts := a2atypes.ContentParts{
		a2atypes.NewTextPart("chunk-A "),
		a2atypes.NewTextPart("chunk-B"),
	}
	if got := a2aPartsText(parts); got != "chunk-A chunk-B" {
		t.Errorf("a2aPartsText = %q, want %q", got, "chunk-A chunk-B")
	}
	// nil 元素安全跳过（consume 收远程流时 Part 可能为 nil）
	withNil := a2atypes.ContentParts{a2atypes.NewTextPart("x"), nil, a2atypes.NewTextPart("y")}
	if got := a2aPartsText(withNil); got != "xy" {
		t.Errorf("a2aPartsText(含 nil) = %q, want %q", got, "xy")
	}
	if got := a2aPartsText(nil); got != "" {
		t.Errorf("a2aPartsText(nil) = %q, want empty", got)
	}
}

func TestBuildMessage(t *testing.T) {
	// 无 task/context → NewMessage（让 server 新建 task）
	m := buildMessage("hi", "", "")
	if m.Role != a2atypes.MessageRoleUser {
		t.Errorf("role = %v, want user", m.Role)
	}
	if got := a2aPartsText(m.Parts); got != "hi" {
		t.Errorf("parts text = %q, want hi", got)
	}
	if m.TaskID != "" {
		t.Errorf("TaskID = %q, want empty（新消息不带 task）", m.TaskID)
	}

	// 有 task/context → NewMessageForTask（续跑同一远程 task）
	m2 := buildMessage("more", "t1", "c1")
	if m2.TaskID != a2atypes.TaskID("t1") {
		t.Errorf("TaskID = %q, want t1", m2.TaskID)
	}
	if m2.ContextID != "c1" {
		t.Errorf("ContextID = %q, want c1", m2.ContextID)
	}
	if got := a2aPartsText(m2.Parts); got != "more" {
		t.Errorf("parts text = %q, want more", got)
	}
}
