package agent_v2

import (
	"strings"
	"testing"
)

// 双 [code] 前缀哨兵：content 恰好一个前缀，annotation 存裸 message。
// 钉的缺陷：两处合成的 message 自带前缀 ⟹ annotation 也带 ⟹ OnToolEnd 改写路径
// 按 "[code] message" 重拼时成了 "[x] [x] ...".
func TestFailedToolResult_SinglePrefix(t *testing.T) {
	for _, c := range []struct{ code, msg string }{
		{"tool_timeout", "工具调用超时"},
		{"invalid_arguments", "解析失败"},
		{"tool_not_revealed", "未解锁"},
	} {
		m := failedToolResult("tc1", "f", c.code, c.msg)
		want := "[" + c.code + "] " + c.msg
		if m.Content != want {
			t.Errorf("content = %q, want %q", m.Content, want)
		}
		if n := strings.Count(m.Content, "["+c.code+"]"); n != 1 {
			t.Errorf("前缀出现 %d 次，该恰好 1 次: %q", n, m.Content)
		}
		// annotation 存裸 message（不含前缀）——改写路径靠它重拼
		if got, _ := m.Annotations[toolErrorMessageAnnotation].(string); got != c.msg {
			t.Errorf("annotation message = %q, want 裸文本 %q", got, c.msg)
		}
	}
}
