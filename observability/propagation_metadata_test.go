package observability_test

import (
	"testing"

	"crew-go/observability"
)

// A2A Message.metadata propagation 单测（跨语言 a2a trace 串联协议第 1 步）。
// 验证 metadata 版与 header 版逻辑等价 + 协议特有约束（key 原样大小写 / value 必须
// string fail-open / 业务 metadata 共存）。

func TestInjectExtractMetadata_RoundTrip(t *testing.T) {
	traceID := observability.GenTraceID("client-app")
	spanID := observability.GenRootSpanID()
	md := map[string]any{"biz_key": "keep-me"} // 业务 metadata，注入不应清掉

	observability.InjectToMetadata(md, traceID, spanID, "client-app", 2, true)

	// 协议：trace 与业务 metadata 共存
	if md["biz_key"] != "keep-me" {
		t.Errorf("业务 metadata 被清掉了")
	}
	// 协议：key 大小写原样（map 不 canonical 化）
	if _, ok := md["x-traceid"]; !ok {
		t.Errorf("x-traceid 小写 key 缺失，got keys %v", keysOf(md))
	}
	if _, ok := md["X-ROOT-MESSAGE-ID"]; !ok {
		t.Errorf("X-ROOT-MESSAGE-ID 大写 key 缺失（canonical 化破坏跨语言），got keys %v", keysOf(md))
	}

	tc := observability.ExtractFromMetadata(md)
	if !tc.IsValid() {
		t.Fatal("round-trip 后 IsValid=false")
	}
	if tc.TraceID != traceID || tc.SpanID != spanID {
		t.Errorf("round-trip ID 不符: trace got %q want %q / span got %q want %q", tc.TraceID, traceID, tc.SpanID, spanID)
	}
	if tc.AppName != "client-app" || tc.NextIdx != 2 || !tc.Sampled {
		t.Errorf("round-trip tracestate 不符: %+v", tc)
	}
}

func TestExtractFromMetadata_FailOpen(t *testing.T) {
	invalid := func(name string, md map[string]any) {
		tc := observability.ExtractFromMetadata(md)
		if tc.IsValid() {
			t.Errorf("%s 应 invalid（fail-open），got %+v", name, tc)
		}
	}
	invalid("nil md", nil)
	invalid("空 md", map[string]any{})
	// value 非 string（协议：忽略非法字段继续业务）
	invalid("非 string value", map[string]any{"x-traceid": 12345, "x-spanid": []byte("x")})
	// 非法格式（不符 CS-OSS 正则）
	invalid("非法格式", map[string]any{"x-traceid": "not-a-valid-trace", "x-spanid": "also-bad"})
	// 只写 W3C traceparent（协议验收 5：不该被误读为 CS-OSS）
	invalid("只有 W3C traceparent", map[string]any{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
}

func TestExtractFromMetadata_CATFallback(t *testing.T) {
	traceID := observability.GenTraceID("app")
	spanID := observability.GenRootSpanID()
	// 只有 CAT 兼容字段（无 x-traceid / x-spanid）——应降级提取
	md := map[string]any{
		"X-ROOT-MESSAGE-ID": traceID,
		"X-ROOT-PARENT-ID":  spanID,
	}
	tc := observability.ExtractFromMetadata(md)
	if !tc.IsValid() {
		t.Fatal("只有 CAT 兼容字段时应能降级提取")
	}
	if tc.TraceID != traceID || tc.SpanID != spanID {
		t.Errorf("CAT 降级 ID 不符: %+v", tc)
	}
}

func TestInjectToMetadata_NoParentNoInject(t *testing.T) {
	// 无合法父（traceID/spanID 空）时不注入（协议：fail-open）
	md := map[string]any{"biz": "x"}
	observability.InjectToMetadata(md, "", "", "app", 1, true)
	if len(md) != 1 {
		t.Errorf("无合法父时不应注入任何 trace key，got %v", keysOf(md))
	}
}

func keysOf(md map[string]any) []string {
	out := make([]string, 0, len(md))
	for k := range md {
		out = append(out, k)
	}
	return out
}
