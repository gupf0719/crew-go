package observability

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// CS-OSS Monitor TraceID/SpanID 字符串格式 (spec OTHER_LANGUAGE_SDK_INTEGRATION.md
// §Trace ID / Span ID, line 183-217):
//
// Trace ID:
//
//	{appname}^^{trace_random_32hex}|{epoch_millis}
//
//	示例: order-service^^4bf92f3577b34da6a3ce929d0e0e4736|1715750400123
//
// Span ID:
//
//	root span:   {index}|{16hex}                              通常 "1|{16hex}"
//	sync child:  {parent_catalogue}.{index}|{16hex}           e.g., "1.2|{16hex}"
//	async child: {parent_catalogue}^{index}|{16hex}           e.g., "1.2^3|{16hex}"
//	rpc child:   {parent_service}:{parent_catalogue}.{index}|{16hex}
//
// "catalogue" 是 parent span id 去掉末尾 "|{16hex}" 的部分, 用作 child 拼前缀.
//
// **关键不变量** (spec line 185-187):
//   - 字符串形态, 直接 ASCII/UTF-8 bytes 写入 OTLP `bytes trace_id/span_id` 字段
//   - **不要**把 32hex / 16hex 解码成 16/8 bytes (current Collector 需完整字符串)

// GenTraceID 生成新 Trace ID. 用于 root span 入口 (业务 Run 起始).
//
// 格式: `{appname}^^{32hex}|{epoch_millis}`
//
// 用 crypto/rand 而非 math/rand — 跨进程 trace ID 全局唯一性更强.
func GenTraceID(appname string) string {
	return appname + "^^" + randHex(32) + "|" + strconv.FormatInt(time.Now().UnixMilli(), 10)
}

// GenRootSpanID 生成 root span ID. 通常用于业务入口 (一次 Run 的第一个 span).
//
// 格式: `1|{16hex}` — index 固定 1 是 spec 推荐 (line 224).
func GenRootSpanID() string {
	return "1|" + randHex(16)
}

// GenChildSpanID 派生子 span ID.
//
//   - parentSpanID: 父 span ID (e.g., "1|abc..." 或 "1.2|def...")
//   - index: 子节点编号 (parent 第几个 child, 1-based)
//   - sync: true=同步子 span (用 "."), false=异步子 span (用 "^")
//
// 返回: `{parent_catalogue}{sep}{index}|{16hex}`
//
//   - parent_catalogue 从 parentSpanID 抽出 (去掉末尾 "|{16hex}")
//   - parent_catalogue 含 service 前缀 (e.g., "order-service:1.2") 时透传
//
// 示例:
//
//	GenChildSpanID("1|abc...", 2, true)  → "1.2|{16hex}"
//	GenChildSpanID("1.2|def...", 3, false) → "1.2^3|{16hex}"
//	GenChildSpanID("order-svc:1.2|x...", 1, true) → "order-svc:1.2.1|{16hex}"
func GenChildSpanID(parentSpanID string, index int, sync bool) string {
	catalogue := extractCatalogue(parentSpanID)
	if catalogue == "" {
		// parent 格式异常 (无 "|{hex}" 后缀) — 退化成 root span 不 panic (业务无感知
		// 子级断链比 panic 友好, 上报时仍能看到孤儿 span 在 UI 排查).
		// review P0 audit LOW-1 fix: 加 slog.Warn 让业务可观测断链 (生产端 misbehaving
		// upstream 大量退化 silent 无信号).
		slog.Warn("observability.GenChildSpanID: invalid parent spanID, degrade to root", "parent", parentSpanID)
		return strconv.Itoa(index) + "|" + randHex(16)
	}
	sep := "."
	if !sync {
		sep = "^"
	}
	return catalogue + sep + strconv.Itoa(index) + "|" + randHex(16)
}

// GenRPCChildSpanID 派生 RPC 调用子 span ID — 下游服务收到 propagation header
// 后生成自己的第一个 span 时用. 前缀加 service 让 cross-service trace 可识别归属.
//
//   - parentSpanID: 上游传过来的 span ID (e.g., "1.2|abc...")
//   - parentService: 上游服务名 (e.g., "order-service") — 从 X-ROOT-MESSAGE-ID
//     header 或 tracestate appname 取
//   - index: 本服务内的子节点序号 (1-based)
//
// 格式: `{parentService}:{parent_catalogue}.{index}|{16hex}`
//
// 示例:
//
//	GenRPCChildSpanID("1.2|abc...", "order-service", 1) → "order-service:1.2.1|{16hex}"
//
// review P0 audit HIGH-1 fix: parentService 字符集校验. 上游 header 含中文/空格/
// `:` 等会让 spanID 不匹配 spec 正则被 collector 拒, 整跨服务调用链断. parentService
// 不合法时退化回本进程 sync child (`GenChildSpanID(parent, idx, true)`) 不带 service
// 前缀, 链路 degrade 但仍可追.
func GenRPCChildSpanID(parentSpanID, parentService string, index int) string {
	if !validAppNameRe.MatchString(parentService) {
		// 上游 appname 含非法字符 — 退化本进程 sync child 不带 service 前缀, 仍可上报
		slog.Warn("observability.GenRPCChildSpanID: invalid parentService, degrade to sync child without service prefix",
			"parentService", parentService, "parentSpanID", parentSpanID)
		return GenChildSpanID(parentSpanID, index, true)
	}
	catalogue := extractCatalogue(parentSpanID)
	if catalogue == "" {
		// parent 格式异常但 service 合法时只保 service 前缀
		slog.Warn("observability.GenRPCChildSpanID: invalid parent spanID, degrade with service prefix only",
			"parentService", parentService, "parentSpanID", parentSpanID)
		return parentService + ":" + strconv.Itoa(index) + "|" + randHex(16)
	}
	return parentService + ":" + catalogue + "." + strconv.Itoa(index) + "|" + randHex(16)
}

// 注: GenChildSpanID / GenRPCChildSpanID 退化路径目前用全局 slog.Warn (无 ctx).
// P2 hooks.go 实施时若需 ctx 关联可加 GenChildSpanIDContext(ctx, ...) variant
// 用 slog.WarnContext(ctx, ...) 让退化 log 含 traceID 字段方便排查.

// extractCatalogue 抽 span ID 的 catalogue 部分 (去掉末尾 "|{16hex}").
//
//	"1|abc..."         → "1"
//	"1.2|abc..."       → "1.2"
//	"1.2^3|abc..."     → "1.2^3"
//	"service:1.2|abc"  → "service:1.2"
//	"|abc"             → ""  (退化: 无 catalogue)
//	"no_pipe"          → ""  (退化: 格式异常)
func extractCatalogue(spanID string) string {
	idx := strings.LastIndex(spanID, "|")
	if idx <= 0 {
		return ""
	}
	return spanID[:idx]
}

// ParseTraceIDApp 从 Trace ID 抽 appname 部分.
//
//	"order-service^^4bf92...|1715750400123" → "order-service"
//	"" / 格式异常 → ""
//
// 业务下游 inject propagation header 时 X-ROOT-MESSAGE-ID 需写源 appname,
// 从 trace_id 抽即可.
func ParseTraceIDApp(traceID string) string {
	idx := strings.Index(traceID, "^^")
	if idx <= 0 {
		return ""
	}
	return traceID[:idx]
}

// randHex 生成 n 字符 lowercase hex 字符串 (n 必须偶数, 否则 panic).
//
// 用 crypto/rand 让跨进程跨机器 span ID 实际唯一 (math/rand 默认共享 seed 易撞).
func randHex(n int) string {
	if n%2 != 0 {
		panic(fmt.Sprintf("observability.randHex: n must be even, got %d", n))
	}
	b := make([]byte, n/2)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 在主流 OS 上不应失败 (urandom 总可用). 退化用 time-based
		// 让业务不 crash 但 ID 唯一性下降 — 极端边缘场景兜底.
		return fmt.Sprintf("%0*x", n, time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
