package observability

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// 跟 monitor SDK span.go:22-23 同款正则, 用作 ExtractFromHeaders 严格校验
// 上游 header 内 TraceID/SpanID 真合法 (业务上游传 "invalid" 字面值时降级 CAT).
//
// review P1 follow-up audit HIGH-1 fix: hex 接受大小写 (monitor SDK 用
// `[0-9a-fA-F]`) + spanID 含 `|` 分隔符 (`[\.|\^]` 三选一). 跟 SDK 完全对齐
// 让跨进程接收 SDK 上游 ID 不被误降级.
var (
	validTraceIDRe = regexp.MustCompile(`^[\w-]+\^\^[0-9a-fA-F]{32}\|\d{13}$`)
	validSpanIDRe  = regexp.MustCompile(`^([\w-]+\:)*\d+([\.|\^]\d+)*\|[0-9a-fA-F]{16}$`)
)

// IsValidTraceID 严格校验 CS-OSS Trace ID 格式 (review P1 audit MED-2 fix).
func IsValidTraceID(s string) bool { return s != "" && validTraceIDRe.MatchString(s) }

// IsValidSpanID 严格校验 CS-OSS Span ID 格式.
func IsValidSpanID(s string) bool { return s != "" && validSpanIDRe.MatchString(s) }

// CS-OSS Monitor trace 跨进程 propagation header (spec OTHER_LANGUAGE_SDK_INTEGRATION.md
// §Trace Header Propagation, line 240-266).
//
// 5 个 header (注意: 不用 W3C `traceparent` / `tracestate`):
//
//	x-traceid          = TraceID 字符串
//	x-spanid           = SpanID 字符串
//	x-tracestate       = "sampled={00|01};appname={cur_app};index={next_child_idx};"
//	X-ROOT-MESSAGE-ID  = CAT 兼容字段, 值同 x-traceid (老监控系统兼容)
//	X-ROOT-PARENT-ID   = CAT 兼容字段, 值同 x-spanid
//
// 抽取优先级 (spec line 262-263):
//  1. 优先 x-traceid / x-spanid
//  2. 缺失时降级 X-ROOT-MESSAGE-ID / X-ROOT-PARENT-ID
const (
	HeaderTraceID       = "x-traceid"
	HeaderSpanID        = "x-spanid"
	HeaderTraceState    = "x-tracestate"
	HeaderRootMessageID = "X-ROOT-MESSAGE-ID"
	HeaderRootParentID  = "X-ROOT-PARENT-ID"

	traceStateSampledKey = "sampled"
	traceStateAppNameKey = "appname"
	traceStateIndexKey   = "index"

	sampledYes = "01"
	sampledNo  = "00"
)

// TraceContext 是跨进程 propagation 的 trace 上下文.
//
// SpanCtx 内表示当前 span 状态 (业务 server 收到 header → 抽 ctx → 派生 RPC child
// span → 传给业务 handler).
type TraceContext struct {
	TraceID string // 上游 trace ID (spec 格式 `{app}^^{32hex}|{millis}`)
	SpanID  string // 上游 span ID (本 service 派生 child 时作 parent)
	Sampled bool   // 上游采样决策 (spec line 264: sampled=01 采, sampled=00 不采)
	AppName string // 上游服务名 (用作下游 RPC child span 的 service 前缀, spec line 265)
	NextIdx int    // 下游初始 child index (spec line 266: 缺失默认 1)
}

// IsValid 判 TraceContext 是否含合法 TraceID/SpanID (业务能否复用作 parent).
func (tc *TraceContext) IsValid() bool {
	return tc != nil && tc.TraceID != "" && tc.SpanID != ""
}

// ExtractFromHeaders 从 HTTP header 抽 TraceContext.
//
// 按 spec line 262-266 抽取规则:
//  1. 优先读 x-traceid / x-spanid
//  2. 缺失时降级 X-ROOT-MESSAGE-ID / X-ROOT-PARENT-ID
//  3. 解析 x-tracestate 得 Sampled / AppName / NextIdx
//  4. 都没拿到 → 返 zero-value TraceContext (IsValid() = false, 业务应自起新 trace)
//
// 业务用法 (HTTP server 入口):
//
//	func handler(w http.ResponseWriter, r *http.Request) {
//	    tc := observability.ExtractFromHeaders(r.Header)
//	    if tc.IsValid() {
//	        // 派生本服务的 RPC child span
//	        spanID := observability.GenRPCChildSpanID(tc.SpanID, tc.AppName, tc.NextIdx)
//	    } else {
//	        // 新 trace
//	        traceID := observability.GenTraceID(myAppName)
//	        spanID := observability.GenRootSpanID()
//	    }
//	}
func ExtractFromHeaders(h http.Header) TraceContext {
	if h == nil {
		return TraceContext{NextIdx: 1}
	}
	tc := TraceContext{}

	// 优先 x-traceid / x-spanid — review P1 audit MED-2 fix: 加严格正则校验,
	// 上游传 "invalid" 字面值时降级 CAT, 不让非法 ID 一路传到 collector 拒.
	tc.TraceID = h.Get(HeaderTraceID)
	if !IsValidTraceID(tc.TraceID) {
		tc.TraceID = ""
	}
	tc.SpanID = h.Get(HeaderSpanID)
	if !IsValidSpanID(tc.SpanID) {
		tc.SpanID = ""
	}

	// 降级 X-ROOT-MESSAGE-ID / X-ROOT-PARENT-ID (spec line 263)
	if tc.TraceID == "" {
		cat := h.Get(HeaderRootMessageID)
		if IsValidTraceID(cat) {
			tc.TraceID = cat
		}
	}
	if tc.SpanID == "" {
		cat := h.Get(HeaderRootParentID)
		if IsValidSpanID(cat) {
			tc.SpanID = cat
		}
	}

	// 解析 x-tracestate
	state := h.Get(HeaderTraceState)
	tc.Sampled, tc.AppName, tc.NextIdx = parseTraceState(state)
	if tc.NextIdx == 0 {
		tc.NextIdx = 1 // spec line 266 缺失默认 1
	}
	return tc
}

// InjectToHeaders 把 TraceContext 写到下游 HTTP header.
//
// 5 个 header 全写 (spec 强制): x-traceid + x-spanid + x-tracestate + 2 CAT 兼容.
//
//   - traceID/spanID 必填 (空字符串时本函数不写任何 header, 业务应自查 IsValid)
//   - currentAppName 必填 (放进 x-tracestate.appname 让下游 RPC 派生 span 时拿)
//   - nextChildIndex 必填 (放进 x-tracestate.index 让下游知道从几开始派生)
//   - sampled 控制 x-tracestate.sampled (true → "01", false → "00")
//
// 业务用法 (HTTP client 出口):
//
//	req, _ := http.NewRequest(...)
//	observability.InjectToHeaders(req.Header, spanID, traceID, myAppName, nextIdx, sampled)
func InjectToHeaders(h http.Header, traceID, spanID, currentAppName string, nextChildIndex int, sampled bool) {
	if h == nil || traceID == "" || spanID == "" {
		return
	}
	// review P1 follow-up audit MED-1 fix: currentAppName 含 ; / = 时
	// buildTraceState 内会 escape 成 `_` 但 round-trip 单向变形. 加 slog.Warn 让
	// 业务可观测 (业务侧应在 Resource.validate 阶段 fail-fast 但 Inject 是公开
	// API 业务可能直接调跳过).
	if currentAppName != "" && strings.ContainsAny(currentAppName, ";=") {
		slog.Warn("observability.InjectToHeaders: appname 含 ';' 或 '=' 会被 escape, round-trip 单向变形",
			"appname", currentAppName)
	}
	h.Set(HeaderTraceID, traceID)
	h.Set(HeaderSpanID, spanID)
	h.Set(HeaderRootMessageID, traceID) // CAT 兼容
	h.Set(HeaderRootParentID, spanID)   // CAT 兼容
	h.Set(HeaderTraceState, buildTraceState(currentAppName, nextChildIndex, sampled))
}

// ── A2A Message.metadata propagation（跨语言 a2a trace 串联协议）──
//
// 与 HTTP header 版（InjectToHeaders/ExtractFromHeaders）的唯一区别：载体是
// A2A Message.metadata（map[string]any）而非 http.Header。两个关键差异：
//  1. **key 大小写原样**：http.Header.Set 会 canonical 化（X-Traceid），破坏
//     协议要求的 x-traceid 小写 / X-ROOT-MESSAGE-ID 大写；map 直接读写保原样。
//  2. **value 是 any**：metadata 是 map[string]any，trace value 必须是 string
//     （协议要求），提取时类型断言失败 fail-open 返空。
//
// 协议要求 trace context 走 params.message.metadata（不是 HTTP header / JSON-RPC
// 顶层 / params.metadata / Task / Artifact metadata）。

// InjectToMetadata 把 trace context 写到 A2A Message.metadata（client 出站前调）。
//
// 只 Set 这 5 个 trace key，不清空 md 已有业务 metadata（协议：trace 与业务 metadata
// 共存）。traceID/spanID 任一空时不注入（无合法父——fail-open，业务自查 IsValid）。
func InjectToMetadata(md map[string]any, traceID, spanID, currentAppName string, nextChildIndex int, sampled bool) {
	if md == nil || traceID == "" || spanID == "" {
		return
	}
	if currentAppName != "" && strings.ContainsAny(currentAppName, ";=") {
		slog.Warn("observability.InjectToMetadata: appname 含 ';' 或 '=' 会被 escape, round-trip 单向变形",
			"appname", currentAppName)
	}
	// map 直读直写——保 key 原样大小写（x-traceid 小写 / X-ROOT-MESSAGE-ID 大写）。
	md[HeaderTraceID] = traceID
	md[HeaderSpanID] = spanID
	md[HeaderRootMessageID] = traceID // CAT 兼容
	md[HeaderRootParentID] = spanID   // CAT 兼容
	md[HeaderTraceState] = buildTraceState(currentAppName, nextChildIndex, sampled)
}

// ExtractFromMetadata 从 A2A Message.metadata 抽 trace context（server 入站后调）。
//
// fail-open（协议要求）：缺失 / value 非 string / 格式非法 → 对应字段留空，最终
// IsValid()=false（server 应自起新 trace 而非 fail）。抽取优先级同 header 版：
// 优先 x-traceid/x-spanid，缺失降级 X-ROOT-MESSAGE-ID/X-ROOT-PARENT-ID。
func ExtractFromMetadata(md map[string]any) TraceContext {
	if md == nil {
		return TraceContext{NextIdx: 1}
	}
	tc := TraceContext{}

	tc.TraceID = mdString(md, HeaderTraceID)
	if !IsValidTraceID(tc.TraceID) {
		tc.TraceID = ""
	}
	tc.SpanID = mdString(md, HeaderSpanID)
	if !IsValidSpanID(tc.SpanID) {
		tc.SpanID = ""
	}

	if tc.TraceID == "" {
		if cat := mdString(md, HeaderRootMessageID); IsValidTraceID(cat) {
			tc.TraceID = cat
		}
	}
	if tc.SpanID == "" {
		if cat := mdString(md, HeaderRootParentID); IsValidSpanID(cat) {
			tc.SpanID = cat
		}
	}

	tc.Sampled, tc.AppName, tc.NextIdx = parseTraceState(mdString(md, HeaderTraceState))
	if tc.NextIdx == 0 {
		tc.NextIdx = 1
	}
	return tc
}

// mdString 安全取 map[string]any 里的 string（非 string / 不存在返 ""）——
// 协议 fail-open：trace value 非字符串时忽略非法字段继续业务。
func mdString(md map[string]any, k string) string {
	if v, ok := md[k].(string); ok {
		return v
	}
	return ""
}

// buildTraceState 拼 x-tracestate 字符串 (spec line 251 格式).
//
// 格式: `sampled={00|01};appname={cur_app};index={next_child_idx};`
// 示例: `sampled=01;appname=order-service;index=2;`
//
// review P1 audit MED-3 fix: appname 含 `;` 或 `=` 会污染 tracestate
// (下游 parse 把后续 segment 当独立 key 解析错位). 业务 typo 时静默 escape
// 这俩字符成 `_` (不抛 panic — InjectToHeaders 是热路径不该 fail-fast).
// 业务侧应在 Resource.validate 时已经 fail-fast 非法字符 (P0 audit HIGH-2 fix),
// 这里是 defense in depth.
func buildTraceState(appname string, nextIdx int, sampled bool) string {
	sFlag := sampledNo
	if sampled {
		sFlag = sampledYes
	}
	var b strings.Builder
	b.Grow(64)
	b.WriteString(traceStateSampledKey)
	b.WriteByte('=')
	b.WriteString(sFlag)
	b.WriteByte(';')
	if appname != "" {
		b.WriteString(traceStateAppNameKey)
		b.WriteByte('=')
		b.WriteString(escapeTraceStateValue(appname))
		b.WriteByte(';')
	}
	if nextIdx > 0 {
		b.WriteString(traceStateIndexKey)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(nextIdx))
		b.WriteByte(';')
	}
	return b.String()
}

// escapeTraceStateValue 把值内的分隔符 `;` `=` 替换 `_` 防注入污染.
func escapeTraceStateValue(s string) string {
	if !strings.ContainsAny(s, ";=") {
		return s
	}
	return strings.NewReplacer(";", "_", "=", "_").Replace(s)
}

// parseTraceState 解析 x-tracestate. 跟 monitor SDK `span.go::parseXState` 同款逻辑.
//
// 输入: `sampled=01;appname=svc;index=2;`
// 输出: (true, "svc", 2)
//
// 容错: 格式异常 (缺 `=` / 缺值) 时该字段跳过用 zero-value.
func parseTraceState(state string) (sampled bool, appname string, idx int) {
	if state == "" {
		return false, "", 0
	}
	for _, kv := range strings.Split(state, ";") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		eq := strings.Index(kv, "=")
		if eq <= 0 || eq == len(kv)-1 {
			continue // 缺 `=` 或 `=` 后空
		}
		k := kv[:eq]
		v := kv[eq+1:]
		switch k {
		case traceStateSampledKey:
			if v == sampledYes {
				sampled = true
			}
		case traceStateAppNameKey:
			appname = v
		case traceStateIndexKey:
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				idx = n
			}
		}
	}
	return
}

// ctx propagation — framework 内部 hook 之间通过 ctx 透传 TraceContext.
//
// 跟 HTTP header propagation 互补: HTTP header 跨进程, ctx 跨函数调用.

type traceContextKey struct{}

// Environment 让 traceContextKey 满足 `compose_v2.EnvironmentKey`（按方法集匹配，本包不必为它引用
// 接口）：trace 上下文是这次请求的（a2a/server 按请求推），钩子在派生的 ctx 上透传照收；钩子返回
// 别处存下的 ctx 时取这次请求的，不让新会话按上一个请求的 trace 播种。
func (traceContextKey) Environment() {}

// WithTraceContext 把 TraceContext 注入 ctx 让下游 hook / 业务代码取.
func WithTraceContext(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, tc)
}

// GetTraceContext 从 ctx 取 TraceContext. 不存在时返 zero-value (IsValid() = false).
func GetTraceContext(ctx context.Context) TraceContext {
	if ctx == nil {
		return TraceContext{NextIdx: 1}
	}
	if tc, ok := ctx.Value(traceContextKey{}).(TraceContext); ok {
		if tc.NextIdx == 0 {
			tc.NextIdx = 1
		}
		return tc
	}
	return TraceContext{NextIdx: 1}
}
