package model_test

import (
	"net/http"
	"testing"
	"time"

	"crew-go/model"
)

// TestParseRetryAfter 覆盖两个响应头的优先级、RFC 9110 两种形式与各类无效输入。纯输入→输出，表驱动；
// now 固定注入让 HTTP-date 分支稳定可断言。
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{"没有响应头", nil, 0},
		{"空值", map[string]string{"Retry-After": "  "}, 0},
		{"秒数", map[string]string{"Retry-After": "120"}, 2 * time.Minute},
		{"秒数带空白", map[string]string{"Retry-After": " 15 "}, 15 * time.Second},
		{"小数秒", map[string]string{"Retry-After": "1.5"}, 1500 * time.Millisecond},
		{"秒数为 0", map[string]string{"Retry-After": "0"}, 0},
		{"秒数为负", map[string]string{"Retry-After": "-5"}, 0},
		{"HTTP-date 未来 30 秒", map[string]string{"Retry-After": "Mon, 20 Jul 2026 12:00:30 GMT"}, 30 * time.Second},
		{"HTTP-date 已过期", map[string]string{"Retry-After": "Mon, 20 Jul 2026 11:59:00 GMT"}, 0},
		{"无法解析", map[string]string{"Retry-After": "soon-ish"}, 0},
		{"毫秒头优先于秒数头", map[string]string{"retry-after-ms": "250", "Retry-After": "9"}, 250 * time.Millisecond},
		{"毫秒头不是数字时看秒数头", map[string]string{"retry-after-ms": "x", "Retry-After": "9"}, 9 * time.Second},
		// 溢出带：按坏数据处理，不封顶成最大值——封顶等于睡到 ctx 超时。
		{"溢出：绕回正数那一档", map[string]string{"Retry-After": "20000000000"}, 0},
		{"溢出：毫秒头", map[string]string{"retry-after-ms": "1e300"}, 0},
		{"上限内的最大值照收", map[string]string{"Retry-After": "9223372036"}, time.Duration(9223372036 * float64(time.Second))},
		// 合法的长等待不拒：上面那道闸挡的是溢出，不是「等太久」。
		{"一天（日配额重置）", map[string]string{"Retry-After": "86400"}, 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h http.Header
			if tt.headers != nil {
				h = http.Header{}
				for k, v := range tt.headers {
					h.Set(k, v)
				}
			}
			if got := model.ParseRetryAfter(h, now); got != tt.want {
				t.Errorf("ParseRetryAfter(%v) = %v，期望 %v", tt.headers, got, tt.want)
			}
		})
	}
}
