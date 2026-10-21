package agent_v2_test

import (
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
)

// TestIsPromptTooLongError_WrappedAPIError 防回归（P0 结构化错误引入）：provider 的
// prompt-too-long 现在包成 *model.APIError，IsPromptTooLongError / ParsePromptTooLongGap
// 必须仍能从其 Error() 文本识别——否则第 3 层 ReactiveCompact 静默失效（CLAUDE.md
// "error 字符串不匹配 regex" 炸弹）。
func TestIsPromptTooLongError_WrappedAPIError(t *testing.T) {
	cases := []struct {
		name string
		body string
		gap  int
	}{
		{"openai_maximum_context", "This model's maximum context length is 8192 tokens. However, your messages resulted in 9123 tokens", 9123 - 8192},
		{"claude_input_too_long", "Input is too long: 234156 input tokens (max 200000)", 234156 - 200000},
		{"qwen_range", "range of input length should be [1, 6000]", 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// 模拟 client 现在的返回：400 + provider 错误体包成 APIError
			err := &model.APIError{Provider: "openai", StatusCode: 400, Body: tc.body}
			if !agent_v2.IsPromptTooLongError(err) {
				t.Fatalf("包成 APIError 后漏判 prompt-too-long: %q", err.Error())
			}
			if got := agent_v2.ParsePromptTooLongGap(err); got != tc.gap {
				t.Fatalf("ParsePromptTooLongGap = %d, want %d", got, tc.gap)
			}
		})
	}
}
