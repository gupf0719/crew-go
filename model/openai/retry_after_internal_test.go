package openai

import (
	"testing"
	"time"
)

// TestParseRetryAfter 覆盖 RFC 9110 两种合法形式 + 各类无效输入。
//
// 纯输入→输出，用表驱动。now 固定注入让 HTTP-date 分支可稳定断言。
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"空头", "", 0},
		{"仅空白", "   ", 0},
		{"秒数", "120", 2 * time.Minute},
		{"秒数带空白", "  15  ", 15 * time.Second},
		{"秒数为 0", "0", 0},
		{"秒数为负", "-5", 0},
		{"HTTP-date 未来 30 秒", "Mon, 20 Jul 2026 12:00:30 GMT", 30 * time.Second},
		{"HTTP-date 已过期", "Mon, 20 Jul 2026 11:59:00 GMT", 0},
		{"HTTP-date 恰好当前", "Mon, 20 Jul 2026 12:00:00 GMT", 0},
		{"无法解析", "soon-ish", 0},
		{"小数秒不合法", "1.5", 0},

		// 整数溢出带：`time.Duration(secs) * time.Second` 在 int64 上乘 1e9 会绕回。
		// ⚠️ **第二条是这组里最要紧的**——它绕回成**正的约 49 年**，三个消费点的
		// `> 0` 闸门全放行、`SleepWithContext` 真的睡下去，只有 ctx deadline 捞得回来。
		// 第一条只绕成负数（各闸门挡得住），但会把负值塞进公开字段 APIError.RetryAfter，
		// 业务写 `if e.RetryAfter != 0` 就会当成「服务端给了」。
		{"溢出：绕回负数", "10000000000", 0},
		{"溢出：绕回正数（约 49 年）", "20000000000", 0},
		{"溢出：MaxInt64", "9223372036854775807", 0},
		{"上限内的最大值仍照收", "9223372036", 9223372036 * time.Second},
		{"刚过上限即拒", "9223372037", 0},

		// 合法的长等待**不拒**——上面那道上限挡的是溢出，不是「等太久」的策略。
		// 日配额重置常发 86400，它必须照收（见 maxRetryAfterSeconds 的 godoc）。
		{"一天（日配额重置，合法常见）", "86400", 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseRetryAfter(tt.header, now); got != tt.want {
				t.Errorf("parseRetryAfter(%q) = %v，期望 %v", tt.header, got, tt.want)
			}
		})
	}
}
