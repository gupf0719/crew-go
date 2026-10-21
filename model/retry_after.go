package model

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter 从 HTTP 响应头解析服务端建议的重试等待时长，给 [APIError.RetryAfter] 用；
// 没给、解析不出或已经过期时返回 0（重试层走常规退避）。
//
// 顺序跟各家官方 SDK 一致：先 `retry-after-ms`（毫秒，非标准，Anthropic / OpenAI 都会发），
// 再 RFC 9110 的 `Retry-After`（delta-seconds 或 HTTP-date）。数值允许带小数，官方 SDK 同样接受。
//
// 换算成 [time.Duration] 会溢出的数值按「解析不出」返回 0，不封顶成最大值：那种数值只可能是
// 坏数据，照它睡下去等于挂到 ctx 超时为止（`Retry-After: 20000000000` 在 int64 上乘 1e9 会绕回
// 成正的约 49 年）。合法的长等待照收：日配额重置常发 86400，返回 24h，这里不设「等太久」的上限。
//
// now 由调用方传入：HTTP-date 形式要算相对时长，注入时钟才能稳定测试。
//
// ⚠️ model/openai 与 decision 包各有一份早于本函数的私有实现，溢出时的取值不同（openai 返回 0、
// decision 封顶成最大值）；新代码用本函数，两份旧实现的统一另行处理。
func ParseRetryAfter(h http.Header, now time.Time) time.Duration {
	if h == nil {
		return 0
	}
	if v := strings.TrimSpace(h.Get("retry-after-ms")); v != "" {
		if d, ok := retryDelay(v, time.Millisecond); ok {
			return d
		}
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if d, ok := retryDelay(v, time.Second); ok {
		return d
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// retryDelay 把数字串按 unit 换成时长。ok 为 false 表示 v 不是数字（调用方接着按 HTTP-date 解析）；
// 是数字但不可用（不是正数、NaN、换算溢出）时返回 (0, true)。
func retryDelay(v string, unit time.Duration) (time.Duration, bool) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(f) || f <= 0 {
		return 0, true
	}
	ns := f * float64(unit)
	if ns >= float64(math.MaxInt64) { // 含 +Inf
		return 0, true
	}
	return time.Duration(ns), true
}
