package observability

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestExtractFromHeaders_PrimaryHeaders(t *testing.T) {
	h := http.Header{}
	h.Set(HeaderTraceID, "order-svc^^aabbccddeeff00112233445566778899|1700000000000")
	h.Set(HeaderSpanID, "1.2|abcdef0123456789")
	h.Set(HeaderTraceState, "sampled=01;appname=order-svc;index=3;")

	tc := ExtractFromHeaders(h)
	if !tc.IsValid() {
		t.Fatal("IsValid 应 true")
	}
	if tc.TraceID != "order-svc^^aabbccddeeff00112233445566778899|1700000000000" {
		t.Errorf("TraceID 错: %q", tc.TraceID)
	}
	if tc.SpanID != "1.2|abcdef0123456789" {
		t.Errorf("SpanID 错: %q", tc.SpanID)
	}
	if !tc.Sampled {
		t.Errorf("Sampled 应 true (sampled=01)")
	}
	if tc.AppName != "order-svc" {
		t.Errorf("AppName 错: %q", tc.AppName)
	}
	if tc.NextIdx != 3 {
		t.Errorf("NextIdx 应 3, got %d", tc.NextIdx)
	}
}

// TestExtractFromHeaders_FallbackToCAT 验 spec line 263 降级路径 — primary
// x-traceid/x-spanid 缺失时读 X-ROOT-MESSAGE-ID / X-ROOT-PARENT-ID.
func TestExtractFromHeaders_FallbackToCAT(t *testing.T) {
	h := http.Header{}
	h.Set(HeaderRootMessageID, "cat-svc^^0123456789abcdef0123456789abcdef|1700000000000")
	h.Set(HeaderRootParentID, "1|ffeeddccbbaa9988")

	tc := ExtractFromHeaders(h)
	if !tc.IsValid() {
		t.Fatal("IsValid 应 true (CAT 降级路径)")
	}
	if tc.TraceID == "" {
		t.Error("TraceID 降级应取 X-ROOT-MESSAGE-ID 值")
	}
	if tc.SpanID == "" {
		t.Error("SpanID 降级应取 X-ROOT-PARENT-ID 值")
	}
}

// TestExtractFromHeaders_InvalidPrimaryDegradesToCAT 验 review P1 audit MED-2 fix —
// primary header 非空但格式异常 (上游传 "invalid" 字面值) 时降级 CAT, 不让非法
// ID 一路传到 collector 拒.
func TestExtractFromHeaders_InvalidPrimaryDegradesToCAT(t *testing.T) {
	h := http.Header{}
	h.Set(HeaderTraceID, "this-is-not-a-valid-trace-id") // 不匹配 {app}^^{32hex}|{millis}
	h.Set(HeaderSpanID, "broken-span")                   // 不匹配 spec 正则
	// CAT 降级值合法
	h.Set(HeaderRootMessageID, "fallback-svc^^abcdef0123456789abcdef0123456789|1700000000000")
	h.Set(HeaderRootParentID, "1|0123456789abcdef")

	tc := ExtractFromHeaders(h)
	if !tc.IsValid() {
		t.Fatal("IsValid 应 true (primary 无效降级 CAT)")
	}
	if !strings.HasPrefix(tc.TraceID, "fallback-svc^^") {
		t.Errorf("primary 无效时 TraceID 应取 CAT 值, got %q\n"+
			"sanity-revert: 删 IsValidTraceID 校验 → 测试拿到 'this-is-not-a-valid-trace-id' fail",
			tc.TraceID)
	}
	if tc.SpanID != "1|0123456789abcdef" {
		t.Errorf("primary 无效时 SpanID 应取 CAT 值, got %q", tc.SpanID)
	}
}

// TestIsValidTraceID_AcceptsUppercaseHex 验 review P1 follow-up audit HIGH-1 fix —
// 跟 monitor SDK 正则一致接受大小写 hex (上游 SDK 可能传大写).
//
// sanity-revert: 改回 `[0-9a-f]{32}` 只小写 → 测试 fail.
func TestIsValidTraceID_AcceptsUppercaseHex(t *testing.T) {
	cases := []string{
		"svc^^4BF92F3577B34DA6A3CE929D0E0E4736|1700000000000", // 全大写
		"svc^^4bf92f3577B34DA6A3CE929D0E0E4736|1700000000000", // 混合
		"svc^^4bf92f3577b34da6a3ce929d0e0e4736|1700000000000", // 全小写
	}
	for _, id := range cases {
		if !IsValidTraceID(id) {
			t.Errorf("review P1 audit HIGH-1 fix: 应接受大小写 hex, got invalid for %q\n"+
				"sanity-revert: 改回 [0-9a-f] 只小写 → 此 fail", id)
		}
	}
}

// TestIsValidSpanID_AcceptsPipeSeparator 验 SpanID 含 `|` 分隔符 (rpc child)
// 跟 monitor SDK `[\.|\^]` 三选一同款.
//
// sanity-revert: 改回 `[.^]` 漏 `|` → 测试 fail.
func TestIsValidSpanID_AcceptsPipeSeparator(t *testing.T) {
	// monitor SDK 允许的 spanID 形态 (含 `|` 分隔符 + 大小写 hex)
	cases := []string{
		"1|ABCDEF0123456789",             // 大写 hex
		"1.2|abcdef0123456789",           // sync child
		"1^2|abcdef0123456789",           // async child
		"order-svc:1.2|aBcDeF01abcdef89", // rpc child + 混合 hex
	}
	for _, id := range cases {
		if !IsValidSpanID(id) {
			t.Errorf("review P1 audit HIGH-1 fix: SpanID 应接受大小写 hex 和分隔符变体, got invalid for %q", id)
		}
	}
}

// TestExtractFromHeaders_InvalidPrimaryAndCATBothInvalid 同时无效 → IsValid false
func TestExtractFromHeaders_InvalidPrimaryAndCATBothInvalid(t *testing.T) {
	h := http.Header{}
	h.Set(HeaderTraceID, "invalid")
	h.Set(HeaderRootMessageID, "also-invalid")

	tc := ExtractFromHeaders(h)
	if tc.IsValid() {
		t.Error("primary + CAT 都无效, IsValid 应 false")
	}
}

func TestExtractFromHeaders_EmptyHeaderReturnsZero(t *testing.T) {
	tc := ExtractFromHeaders(http.Header{})
	if tc.IsValid() {
		t.Error("空 header IsValid 应 false")
	}
	if tc.NextIdx != 1 {
		t.Errorf("NextIdx 缺失应默认 1 (spec line 266), got %d", tc.NextIdx)
	}
}

func TestExtractFromHeaders_NilSafe(t *testing.T) {
	tc := ExtractFromHeaders(nil)
	if tc.IsValid() {
		t.Error("nil header 应返 invalid")
	}
}

func TestInjectToHeaders_AllFiveHeaders(t *testing.T) {
	h := http.Header{}
	InjectToHeaders(h, "trace-id-xx", "span-id-yy", "order-svc", 5, true)

	// 5 header 全写
	checks := map[string]string{
		HeaderTraceID:       "trace-id-xx",
		HeaderSpanID:        "span-id-yy",
		HeaderRootMessageID: "trace-id-xx", // CAT 兼容值同 TraceID
		HeaderRootParentID:  "span-id-yy",  // CAT 兼容值同 SpanID
	}
	for k, want := range checks {
		if got := h.Get(k); got != want {
			t.Errorf("header %q = %q, want %q", k, got, want)
		}
	}
	state := h.Get(HeaderTraceState)
	for _, sub := range []string{"sampled=01", "appname=order-svc", "index=5"} {
		if !strings.Contains(state, sub) {
			t.Errorf("x-tracestate 缺 %q, got %q", sub, state)
		}
	}
}

func TestInjectToHeaders_NilSafe(t *testing.T) {
	// nil header / 空 trace/span ID 不应 panic
	InjectToHeaders(nil, "t", "s", "a", 1, true)
	h := http.Header{}
	InjectToHeaders(h, "", "spanonly", "a", 1, true)
	if h.Get(HeaderTraceID) != "" {
		t.Error("traceID 空时不应写任何 header")
	}
}

func TestInjectThenExtract_RoundTrip(t *testing.T) {
	// 业务最常见用法: server 收 header 抽 ctx, client 出 header 注 ctx → 应 round-trip 一致
	// 用真合法 traceID/spanID (review P1 audit MED-2 fix 后 Extract 加 IsValid 校验).
	traceID := "order-svc^^abcdef0123456789abcdef0123456789|1700000000000"
	spanID := "1.2|abcdef0123456789"
	h := http.Header{}
	InjectToHeaders(h, traceID, spanID, "order-svc", 7, false)

	tc := ExtractFromHeaders(h)
	if tc.TraceID != traceID {
		t.Errorf("round-trip TraceID 错: %q", tc.TraceID)
	}
	if tc.SpanID != spanID {
		t.Errorf("round-trip SpanID 错: %q", tc.SpanID)
	}
	if tc.AppName != "order-svc" {
		t.Errorf("round-trip AppName 错: %q", tc.AppName)
	}
	if tc.NextIdx != 7 {
		t.Errorf("round-trip NextIdx 错: %d", tc.NextIdx)
	}
	if tc.Sampled {
		t.Error("round-trip Sampled 应 false (sampled=00)")
	}
}

func TestParseTraceState_Variants(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		wantSampled bool
		wantApp     string
		wantIdx     int
	}{
		{"empty", "", false, "", 0},
		{"all 3", "sampled=01;appname=svc;index=5;", true, "svc", 5},
		{"sampled 00", "sampled=00;appname=svc;index=1;", false, "svc", 1},
		{"缺末尾 ;", "sampled=01;appname=svc;index=5", true, "svc", 5},
		{"乱序", "index=9;sampled=01;appname=x;", true, "x", 9},
		{"含空格", "  sampled=01 ; appname=svc ; index=2 ", true, "svc", 2},
		{"非法 key", "foo=bar;sampled=01;", true, "", 0},
		{"index=0 视为缺失 (spec 默认 1, 由 ExtractFromHeaders 兜底)", "sampled=01;index=0;", true, "", 0},
		{"= 后空", "sampled=;appname=svc;", false, "svc", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, a, i := parseTraceState(tc.in)
			if s != tc.wantSampled {
				t.Errorf("sampled = %v, want %v", s, tc.wantSampled)
			}
			if a != tc.wantApp {
				t.Errorf("appname = %q, want %q", a, tc.wantApp)
			}
			if i != tc.wantIdx {
				t.Errorf("index = %d, want %d", i, tc.wantIdx)
			}
		})
	}
}

func TestBuildTraceState_Format(t *testing.T) {
	s := buildTraceState("order-svc", 3, true)
	want := "sampled=01;appname=order-svc;index=3;"
	if s != want {
		t.Errorf("buildTraceState = %q, want %q", s, want)
	}

	// sampled=false
	s2 := buildTraceState("svc", 1, false)
	if !strings.HasPrefix(s2, "sampled=00;") {
		t.Errorf("sampled=false 应以 'sampled=00;' 开头, got %q", s2)
	}
}

// TestBuildTraceState_EscapesInjection 验 review P1 audit MED-3 fix —
// AppName 含 `;` 或 `=` 时 escape 不污染下游 parse.
func TestBuildTraceState_EscapesInjection(t *testing.T) {
	// AppName 含 `;` (典型 injection: "order;evil")
	s := buildTraceState("order;evil", 2, true)
	// `;` 应被 escape 成 `_`, 让下游 parse 把 "order_evil" 当完整 appname 值
	if strings.Contains(s, "order;evil") {
		t.Errorf("appname 含 ';' 应 escape 防注入, got %q", s)
	}
	// round-trip 验证: parse 后 appname 应是 escape 后的值, 不是被切碎
	_, app, _ := parseTraceState(s)
	if strings.Contains(app, ";") {
		t.Errorf("parse 后 appname 含 ';' 说明 escape 失效, got %q", app)
	}

	// AppName 含 `=`
	s2 := buildTraceState("a=b", 1, true)
	if strings.Contains(s2, "a=b") {
		t.Errorf("appname 含 '=' 应 escape, got %q", s2)
	}
}

func TestCtxPropagation(t *testing.T) {
	ctx := context.Background()
	got := GetTraceContext(ctx)
	if got.IsValid() {
		t.Error("空 ctx 应返 invalid TraceContext")
	}

	tc := TraceContext{TraceID: "t", SpanID: "s", AppName: "a", NextIdx: 4}
	ctx2 := WithTraceContext(ctx, tc)
	got2 := GetTraceContext(ctx2)
	if !got2.IsValid() {
		t.Error("注入后应返 valid")
	}
	if got2.NextIdx != 4 {
		t.Errorf("NextIdx 不一致: %d", got2.NextIdx)
	}
}

func TestGetTraceContext_NilSafe(t *testing.T) {
	got := GetTraceContext(nil) //nolint:staticcheck // 故意测 nil ctx
	if got.IsValid() {
		t.Error("nil ctx 应返 invalid")
	}
	if got.NextIdx != 1 {
		t.Errorf("nil ctx NextIdx 应默认 1, got %d", got.NextIdx)
	}
}
