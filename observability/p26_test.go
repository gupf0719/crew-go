package observability

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// TestP26_SamplingRatioZero 验 SamplingRatio<1.0 真生效 — ratio=0.0001 让
// 100 个 session 大概率全 drop, mock collector 收 0 span (或 ≤2).
//
// sanity-revert: 删 startSpan 内 sampled 短路 → 收 200 span (100 graph + 100 agent).
func TestP26_SamplingRatioZero(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		SamplingRatio:        0.0001, // 万分之一
	})
	defer obs.Close()

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	for i := 0; i < 100; i++ {
		ctx := compose_v2.WithGraphNS(context.Background(), "g"+string(rune('a'+i%26)))
		_, _ = orch.OnGraphStart(ctx)
		_, _ = ah.OnAgentStart(ctx, &hook.AgentInfo{Name: "ag"}, &hook.AgentInput{})
		ah.OnAgentEnd(ctx, &hook.AgentInfo{Name: "ag"}, &hook.AgentOutput{})
		orch.OnGraphEnd(ctx, nil)
	}
	time.Sleep(500 * time.Millisecond)

	got := srv.count.Load()
	if got > 50 { // 概率上 100*0.0001=0.01 → 几乎 0, ≤50 安全
		t.Errorf("review P2.6 SamplingRatio fix: ratio=0.0001 应 drop ~所有 span, mock 收 %d (>50 反预期)\n"+
			"sanity-revert: 删 startSpan 内 sess.sampled 短路 → 收 200 span", got)
	}
}

// TestP26_SamplingRatioFull 验 1.0 全采.
func TestP26_SamplingRatioFull(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		SamplingRatio:        1.0,
	})
	defer obs.Close()
	orch := obs.OrchHooks()
	for i := 0; i < 5; i++ {
		ctx := compose_v2.WithGraphNS(context.Background(), "g"+string(rune('a'+i)))
		_, _ = orch.OnGraphStart(ctx)
		orch.OnGraphEnd(ctx, nil)
	}
	time.Sleep(500 * time.Millisecond)
	if got := srv.count.Load(); got != 5 {
		t.Errorf("SamplingRatio=1.0 全采: 应收 5 span, got %d", got)
	}
}

// TestP26_RecordMessageContent_True 验 opt-in 后 gen_ai_content event 真采.
func TestP26_RecordMessageContent_True(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		RecordMessageContent: true,
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{
		Messages: []*schema.Message{schema.UserMessage("secret-content-xyz")},
	})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{
		Message: schema.AssistantMessage("response-content-abc"),
	})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("没找 chat span")
	}
	var promptContent, responseContent string
	for _, e := range chat.Events {
		for _, a := range e.Attributes {
			if a.Key != "gen_ai_content" {
				continue
			}
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
				if e.Name == "prompt" {
					promptContent = sv.StringValue
				} else if e.Name == "response" {
					responseContent = sv.StringValue
				}
			}
		}
	}
	if !strings.Contains(promptContent, "secret-content-xyz") {
		t.Errorf("review P2.6 RecordMessageContent fix: prompt event 应含 messages 内容, got %q", promptContent)
	}
	if !strings.Contains(responseContent, "response-content-abc") {
		t.Errorf("review P2.6 RecordMessageContent fix: response event 应含 message 内容, got %q", responseContent)
	}
}

// TestP26_RecordMessageContent_False 验默认关时不采.
func TestP26_RecordMessageContent_False(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		// RecordMessageContent 默认 false
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{
		Messages: []*schema.Message{schema.UserMessage("PII-data")},
	})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{Message: schema.AssistantMessage("ok")})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("没找 chat span")
	}
	for _, e := range chat.Events {
		for _, a := range e.Attributes {
			if a.Key == "gen_ai_content" {
				t.Errorf("RecordMessageContent=false 不应有 gen_ai_content attr, got event %q with content", e.Name)
			}
		}
	}
}

// TestP26_RedactPatterns 验 PII regex 真应用 (RecordMessageContent=true + redact).
func TestP26_RedactPatterns(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		RecordMessageContent: true,
		RedactPatterns: []*regexp.Regexp{
			regexp.MustCompile(`\d{11}`), // 手机号
		},
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{
		Messages: []*schema.Message{schema.UserMessage("call me at 13800138000 please")},
	})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{Message: schema.AssistantMessage("ok")})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("没找 chat span")
	}
	for _, e := range chat.Events {
		if e.Name != "prompt" {
			continue
		}
		for _, a := range e.Attributes {
			if a.Key != "gen_ai_content" {
				continue
			}
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
				if strings.Contains(sv.StringValue, "13800138000") {
					t.Errorf("review P2.6 RedactPatterns fix: 手机号应被 redact, got %q", sv.StringValue)
				}
				if !strings.Contains(sv.StringValue, "[REDACTED]") {
					t.Errorf("应含 '[REDACTED]', got %q", sv.StringValue)
				}
			}
		}
	}
}

// TestP26_MaxSpanAttributes 验业务设小 cap 时 attr 超量真 drop + DroppedAttributesCount.
func TestP26_MaxSpanAttributes(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		MaxSpanAttributes:    5, // 极小 cap, 框架内置 4 attr 占用, endSpan +status 触发 drop
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag", Type: "base"} // type 触发额外 agent.kind attr 超 cap
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	ag := findSpanByOp(srv.Spans(), "agent")
	if ag == nil {
		t.Fatal("没找 agent span")
	}
	if len(ag.Attributes) > 5 {
		t.Errorf("MaxSpanAttributes=5 应 cap, got %d attrs", len(ag.Attributes))
	}
	if ag.DroppedAttributesCount == 0 {
		t.Errorf("review P2.6 MaxSpanAttributes fix: 应有 DroppedAttributesCount > 0 让业务可观测 drop")
	}
}

// TestP26_MaxAttributeValueChars 验长 value 真 truncate.
func TestP26_MaxAttributeValueChars(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	longName := strings.Repeat("x", 1000) // 1000 char agent name
	obs, _ := New(Config{
		Resource:               Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:          addr,
		TraceExportInterval:    100 * time.Millisecond,
		TraceExportBatchSize:   100,
		MaxAttributeValueChars: 50, // 极小让 1000 char 必截
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: longName, Type: "base"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	ag := findSpanByOp(srv.Spans(), "agent")
	if ag == nil {
		t.Fatal("没找 agent span")
	}
	for _, a := range ag.Attributes {
		if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
			if len(sv.StringValue) > 50 {
				t.Errorf("review P2.6 MaxAttributeValueChars fix: key=%q value 超 50 char, got len=%d (应 truncate)",
					a.Key, len(sv.StringValue))
			}
		}
	}
}

// TestP26Audit_GenAIContentNoDoubleTruncate (HIGH-1 回归):
// 之前 addEvent 内 limitedAttrs 循环对所有 attr 走 MaxAttributeValueChars truncate
// → gen_ai_content 被双截断 (renderContent 按 MaxInlineEventBytes 截到 N → addEvent
// 再按 MaxAttributeValueChars 默认 512 截). 业务设 RecordMessageContent=true 实际拿到
// 远小于 spec 1MB 上限.
//
// sanity-revert: 删 addEvent 内 `a.Key != attrGenAIContent` 条件 → gen_ai_content
// value 被截到 50 char 而非 1000.
func TestP26Audit_GenAIContentNoDoubleTruncate(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	long := strings.Repeat("x", 1000) // 1000 char content
	obs, _ := New(Config{
		Resource:               Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:          addr,
		TraceExportInterval:    100 * time.Millisecond,
		TraceExportBatchSize:   100,
		RecordMessageContent:   true,
		MaxAttributeValueChars: 50,   // 极小, 验 gen_ai_content **不** 被这个 cap 约束
		MaxInlineEventBytes:    2000, // > 1000 保 renderContent 不截
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{
		Messages: []*schema.Message{schema.UserMessage(long)},
	})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{Message: schema.AssistantMessage("ok")})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("没找 chat span")
	}
	var got string
	for _, e := range chat.Events {
		if e.Name != "prompt" {
			continue
		}
		for _, a := range e.Attributes {
			if a.Key == attrGenAIContent {
				if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
					got = sv.StringValue
				}
			}
		}
	}
	if got == "" {
		t.Fatal("没找 gen_ai_content attr")
	}
	// 1000 char "x"...JSON 序列化为 ["{\"role\":\"user\",\"content\":\"xxxx...\"}"] 包裹 > 1000 char.
	// 关键验证: len(got) >> MaxAttributeValueChars=50 — 证 fix 真生效 (没被双截断).
	if len(got) <= 50 {
		t.Errorf("P2.6 audit HIGH-1 双截断 regression: gen_ai_content 应跳过 MaxAttributeValueChars=50 走 1MB 上限, got len=%d (≤50 说明仍被双截)\n"+
			"sanity-revert: 删 addEvent 内 `a.Key != attrGenAIContent` 条件 → 应观察到 len=50", len(got))
	}
}

// TestP26Audit_SweepInFlightErrMsgTruncate (HIGH-2 回归):
// sweepInFlight 之前直接 entry.span.Attributes = append(...) 跳 applyAttrLimit → long
// errMsg (LLM 几 KB error) 不 truncate, 超 MaxAttributeValueChars. 改走 applyAttrLimit
// 后真 truncate + MaxSpanAttributes cap 也生效.
//
// sanity-revert: sweepInFlight 内还原直接 append → error.message attr len > 100.
func TestP26Audit_SweepInFlightErrMsgTruncate(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	longErr := strings.Repeat("E", 2000) // 2KB 错误信息
	obs, _ := New(Config{
		Resource:               Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:          addr,
		TraceExportInterval:    100 * time.Millisecond,
		TraceExportBatchSize:   100,
		MaxAttributeValueChars: 100,
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	// 开 model span 但**不** OnModelEnd, 触发 sweepInFlight 兜底
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	// OnError 把 longErr 写入 sess.lastErrors 标 model span
	ah.OnError(ctx, info, &schema.CommError{Code: "LongErrCode", Message: longErr})
	// OnAgentEnd → sweepInFlight 抓 in-flight model span + lastErrors 长 errMsg
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("没找 chat span (sweepInFlight 应 enqueue)")
	}
	var errMsgLen int
	for _, a := range chat.Attributes {
		if a.Key == "error.message" {
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
				errMsgLen = len(sv.StringValue)
			}
		}
	}
	if errMsgLen == 0 {
		t.Fatal("没找 error.message attr")
	}
	if errMsgLen > 100 {
		t.Errorf("P2.6 audit HIGH-2 sweep truncate regression: error.message 应 ≤ MaxAttributeValueChars=100, got len=%d\n"+
			"sanity-revert: sweepInFlight 还原直接 append 不走 applyAttrLimit → errMsgLen=2000", errMsgLen)
	}
}

// TestP26Audit_UnsampledSessionSkipsExpensiveIDs (MED-2 回归):
// !sampled 时跳过贵的 GenTraceID / GenRootSpanID 派生, 减一半 session() 成本.
// ratio=1e-9 几乎全 drop, 100 session 应大多 traceID/rootSpanID 都为空.
//
// sanity-revert: session() 内还原"无论 sampled 都算 traceID/rootSpanID" → 所有 session
// 都有非空 traceID.
func TestP26Audit_UnsampledSessionSkipsExpensiveIDs(t *testing.T) {
	obs, _ := New(Config{
		Resource:      Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint: "", // 关 exporter, 只测 session() 行为
		SamplingRatio: 1e-9,
	})
	defer obs.Close()
	unsampled := 0
	sampledHasTrace := 0
	for i := 0; i < 100; i++ {
		sess := obs.sessionByID("s" + strconv.Itoa(i))
		if !sess.sampled {
			unsampled++
			if sess.traceID != "" {
				t.Errorf("MED-2 regression: !sampled session %d 仍有 traceID=%q (应跳过 GenTraceID)", i, sess.traceID)
			}
			if sess.rootSpanID != "" {
				t.Errorf("MED-2 regression: !sampled session %d 仍有 rootSpanID=%q", i, sess.rootSpanID)
			}
		} else if sess.traceID != "" {
			sampledHasTrace++
		}
	}
	if unsampled < 90 {
		t.Errorf("ratio=1e-9 100 session 应 ≥90 unsampled, got %d (概率异常或 sampling 失效)", unsampled)
	}
	t.Logf("unsampled=%d sampled-with-trace=%d", unsampled, sampledHasTrace)
}

// TestP26Audit_RenderContentUTF8Boundary (LOW-2 回归):
// 多字节字符 (中文 3 byte) 在 MaxInlineEventBytes 边界被 byte 切 → 无效 UTF-8.
// 修后用 utf8.RuneStart 回退到 rune 完整边界.
//
// sanity-revert: 删 utf8.RuneStart 循环 → utf8.ValidString(got) false.
func TestP26Audit_RenderContentUTF8Boundary(t *testing.T) {
	obs, _ := New(Config{
		Resource:            Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		MaxInlineEventBytes: 20, // 极小让中文 (3 byte) 必触边界
	})
	defer obs.Close()
	// 8 个中文 = 24 byte, MaxInlineEventBytes=20 → cut=17 (max-3) 落"中"中间
	big := "中文测试边界字符"
	got := obs.renderContent(big)
	if !utf8.ValidString(got) {
		t.Errorf("P2.6 audit LOW-2 UTF-8 regression: 截断后 invalid UTF-8 (byte 切到中文中间), got %q (bytes=%v)\n"+
			"sanity-revert: 删 hooks.go renderContent 内 utf8.RuneStart 循环 → 此测试 fail", got, []byte(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("应以 ... 结尾, got %q", got)
	}
}

// ─────────────── audit² 6 视角回归测试 ───────────────

// TestP26Audit2_NoAgentSweepInOnGraphEnd (C-HIGH-1 回归):
// OnGraphEnd 在 OnAgentEnd 之前 fire — 之前 sweep "agent|" 会把 success 路径 agent
// span 标 error InFlightSweep + LoadAndDelete, 之后 OnAgentEnd LoadAndDelete miss
// silent drop. 业务监控按 status=error 看 100% 假失败率.
//
// sanity-revert: OnGraphEnd 加回 "agent|" 到 sweep kindPrefix list → agent span
// status=error 而非 success.
func TestP26Audit2_NoAgentSweepInOnGraphEnd(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "success_g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	// 模拟 BasicAgent.Run 真实顺序: compose_v2.Run 退出 → OnGraphEnd → defer OnAgentEnd
	orch.OnGraphEnd(ctx, nil)
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	time.Sleep(300 * time.Millisecond)

	ag := findSpanByOp(srv.Spans(), "agent")
	if ag == nil {
		t.Fatal("agent span 没上报")
	}
	status, _ := findAttr(ag, "gen_ai.status")
	if status != "success" {
		t.Errorf("audit² C-HIGH-1 regression: success Run agent span status 应 'success', got %q (业务监控全红)\n"+
			"sanity-revert: OnGraphEnd kindPrefix list 加回 'agent|' → status='error' InFlightSweep", status)
	}
}

// TestP26Audit2_NodeIDKindAware (C-HIGH-2 回归):
// node.id 之前一律 `graphNS/name`, agent + chat 同 name 时 node.id 重叠 → 前端
// 按 attr 构树 dedup 丢一个; chat 跟 agent 不嵌套显示扁平.
//
// sanity-revert: startSpan 内 nodeID 改回 `graphNS+"/"+name` 不分 kind → agent
// 跟 chat node.id 相同 fail.
func TestP26Audit2_NodeIDKindAware(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "tree_g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{Message: schema.AssistantMessage("ok")})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	ag := findSpanByOp(srv.Spans(), "agent")
	chat := findSpanByOp(srv.Spans(), "chat")
	if ag == nil || chat == nil {
		t.Fatal("agent / chat span 缺")
	}
	agNodeID, _ := findAttr(ag, "gen_ai.node.id")
	chatNodeID, _ := findAttr(chat, "gen_ai.node.id")
	if agNodeID == chatNodeID {
		t.Errorf("audit² C-HIGH-2 regression: agent + chat node.id 不能重叠 (前端构树扁平), 都是 %q", agNodeID)
	}
	if agNodeID != "tree_g/agent/ag" {
		t.Errorf("agent node.id 应 'tree_g/agent/ag' kind-aware, got %q", agNodeID)
	}
	expectChat := "tree_g/agent/ag/model/r0"
	if chatNodeID != expectChat {
		t.Errorf("chat node.id 应 %q 嵌入 agent 下, got %q", expectChat, chatNodeID)
	}
	// chat parent 应是 agent node.id
	chatParent, _ := findAttr(chat, "gen_ai.node.parent_id")
	if chatParent != agNodeID {
		t.Errorf("chat.parent_id 应 = agent.node.id (%q), got %q", agNodeID, chatParent)
	}
}

// TestP26Audit2_OnErrorRedactPII (B-HIGH-B1 回归):
// 业务 RecordMessageContent=true + RedactPatterns + OnError 含 PII (e.g., API key) —
// 之前 lastError.message 跳过 redact 直接上 trace 明文; 修后走 redactString.
//
// sanity-revert: 删 OnError 内 redactedMsg := h.o.redactString(err.Message) 改回
// 直接用 err.Message → trace 内 error.message 含 'sk-xxx' fail.
func TestP26Audit2_OnErrorRedactPII(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		RedactPatterns: []*regexp.Regexp{
			regexp.MustCompile(`sk-[\w-]{6,}`), // API key (含 dash)
		},
	})
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "pii_g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{})
	// OnError 含 API key, 之后 OnAgentEnd sweep 把 lastError 写 model span error.message
	ah.OnError(ctx, info, &schema.CommError{Code: "Unauthorized", Message: "401 Unauthorized: API key sk-proj-abc123def invalid"})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("chat span (sweep 兜底) 缺")
	}
	errMsg, _ := findAttr(chat, "error.message")
	if strings.Contains(errMsg, "sk-proj-abc123def") {
		t.Errorf("audit² B-HIGH-B1 regression: error.message 应 redact API key, got %q (PII 明文上 trace)", errMsg)
	}
	if !strings.Contains(errMsg, "[REDACTED]") {
		t.Errorf("error.message 应含 [REDACTED] marker, got %q", errMsg)
	}
}

// TestP26Audit2_OnErrorPrefixNoCollision (D-HIGH-1 回归):
// 之前 OnError prefix `agent|ns|chat` 用 startsWith 误命中 `agent|ns|chat_v2`,
// chat_v2 span 被污染. 修后 agent 用 exact, model 用 +"|" 严格.
//
// sanity-revert: OnError 改回 for-loop + startsWith(key, prefix) → chat_v2 span
// status=error 而非 success.
func TestP26Audit2_OnErrorPrefixNoCollision(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "collide_g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	infoA := &hook.AgentInfo{Name: "chat"}
	infoB := &hook.AgentInfo{Name: "chat_v2"}
	_, _ = ah.OnAgentStart(ctx, infoA, &hook.AgentInput{})
	_, _ = ah.OnAgentStart(ctx, infoB, &hook.AgentInput{})
	// OnError 给 chat — 不应污染 chat_v2 的 lastError
	ah.OnError(ctx, infoA, &schema.CommError{Code: "OnlyForChat", Message: "err for chat"})
	ah.OnAgentEnd(ctx, infoA, &hook.AgentOutput{})
	ah.OnAgentEnd(ctx, infoB, &hook.AgentOutput{}) // chat_v2 应 success
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	var chatV2 *tracepb.Span
	for _, s := range srv.Spans() {
		nameAttr, _ := findAttr(s, "gen_ai.node.name")
		if nameAttr == "chat_v2" {
			chatV2 = s
			break
		}
	}
	if chatV2 == nil {
		t.Fatal("chat_v2 span 缺")
	}
	status, _ := findAttr(chatV2, "gen_ai.status")
	if status != "success" {
		t.Errorf("audit² D-HIGH-1 regression: chat_v2 不应被 chat OnError 污染, status 应 'success' got %q", status)
	}
}

// TestP26Audit2_InterruptErrorStatusBlocked (D-HIGH-6 回归):
// graph err 是 InterruptError 时, OnGraphEnd 算 status='blocked'. 之前 sweep 硬编码
// 'error' 让 in-flight model/tool span 全红, 跟 graph 'blocked' 不对称.
//
// sanity-revert: sweepInFlight 改回硬编码 strKV("gen_ai.status", "error") →
// model span status='error' 而非 'blocked' fail.
func TestP26Audit2_InterruptErrorStatusBlocked(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()
	ctx := compose_v2.WithGraphNS(context.Background(), "hitl_g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{}) // in-flight model span
	// graph 出 InterruptError — OnGraphEnd 算 status='blocked' 传给 sweep
	ie := &compose_v2.InterruptError{Reason: "ask_user", Query: &compose_v2.AskQuery{Question: "?"}}
	orch.OnGraphEnd(ctx, ie)
	time.Sleep(300 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("chat span (sweep 兜底) 缺")
	}
	status, _ := findAttr(chat, "gen_ai.status")
	if status != "blocked" {
		t.Errorf("audit² D-HIGH-6 regression: InterruptError 路径 in-flight model status 应 'blocked' (跟 graph 对称), got %q", status)
	}
}

// TestP26Audit2_UTF8SafeTruncString (A-MED-2/D-HIGH-3 回归):
// truncString head + tail byte 切让中文 / emoji 在边界处 invalid UTF-8. 修后用
// safeHeadCut + safeTailCut 回退 rune 边界.
//
// sanity-revert: truncString 内 s[:half] / s[len-half:] 改回 byte 切 → 中文 attr value
// invalid UTF-8 fail.
func TestP26Audit2_UTF8SafeTruncString(t *testing.T) {
	// 8 个中文 = 24 byte, max=12 → half=4 → head/tail 都 4 byte 必落中文中间
	long := "中文测试边界字符示例"
	got := truncString(long, 12)
	if !utf8.ValidString(got) {
		t.Errorf("audit² A-MED-2 regression: truncString head/tail 应 UTF-8 safe, got %q (bytes=%v)\n"+
			"sanity-revert: 删 safeHeadCut/safeTailCut, s[:half]/s[len-half:] 直接 byte 切 → 此测 fail", got, []byte(got))
	}
}

// TestP26Audit2_ResourceFieldValidation (B-MED-B1 回归):
// 之前 IP/Host/Zone 长度无上限, 业务恶意 100KB Host 让 Resource attr 持续浪费带宽.
// 修后 validate 加上限校验 fail-fast.
func TestP26Audit2_ResourceFieldValidation(t *testing.T) {
	cases := []struct {
		name string
		r    Resource
	}{
		{"IPTooLong", Resource{AppName: "a", EnvName: "t", IP: strings.Repeat("1", 60), SDKVersion: "v"}},
		{"HostTooLong", Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v", Host: strings.Repeat("h", 300)}},
		{"ZoneTooLong", Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v", Zone: strings.Repeat("z", 100)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(Config{Resource: c.r})
			if err == nil {
				t.Errorf("audit² B-MED-B1 regression: %s 应 fail-fast, got nil err", c.name)
			}
		})
	}
}
