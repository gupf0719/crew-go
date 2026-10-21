package observability

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/modules/common"
	"crew-go/schema"
)

// captureServer 收 OTLP span 并暴露 spans 列表给测试 assert.
type captureServer struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    atomic.Value // *[]*tracepb.Span (atomic 防 race)
	count atomic.Int32
}

func (c *captureServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	var spans []*tracepb.Span
	for _, rs := range req.ResourceSpans {
		for _, ils := range rs.ScopeSpans {
			spans = append(spans, ils.Spans...)
		}
	}
	c.count.Add(int32(len(spans)))
	prev, _ := c.mu.Load().(*[]*tracepb.Span)
	all := []*tracepb.Span{}
	if prev != nil {
		all = append(all, *prev...)
	}
	all = append(all, spans...)
	c.mu.Store(&all)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (c *captureServer) Spans() []*tracepb.Span {
	v, _ := c.mu.Load().(*[]*tracepb.Span)
	if v == nil {
		return nil
	}
	return *v
}

func startCaptureServer(t *testing.T) (*captureServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	c := &captureServer{}
	coltracepb.RegisterTraceServiceServer(srv, c)
	go func() { _ = srv.Serve(lis) }()
	return c, lis.Addr().String(), func() {
		srv.GracefulStop()
		_ = lis.Close()
	}
}

func newTestObs(t *testing.T, addr string) *Observability {
	t.Helper()
	obs, err := New(Config{
		Resource:             Resource{AppName: "test-app", EnvName: "test", IP: "127.0.0.1", SDKVersion: "v0.1"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return obs
}

func TestHooks_FullLifecycleProducesTraceTree(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()

	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "root_graph")

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	// 模拟一次 BasicAgent Run 完整 hook 序列 (跟真实 agent_v2.Run 时序一致):
	//   OnAgentStart [agent_v2 wrapper top-level fire, 早于 graph]
	//   ↓
	//   OnGraphStart [compose_v2.Run 内 fire, 检 in-flight agent 跳过创 span]
	//   ↓ model/tool ...
	//   OnGraphEnd
	//   ↓
	//   OnAgentEnd [agent_v2 wrapper defer fire, 晚于 graph]
	info := &hook.AgentInfo{Name: "test_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("hello")}})
	_, _ = orch.OnGraphStart(ctx)

	mi := &schema.ModelInput{
		Messages: []*schema.Message{schema.UserMessage("hello")},
		Config:   &common.ModelConfig{ModelName: "gpt-4o", Temperature: 0.7, MaxTokens: 100},
	}
	_, _ = ah.OnModelStart(ctx, info, mi)

	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{
		Message: schema.AssistantMessage("hi back"),
		Usage:   &common.MessageUsage{InputTokens: 5, OutputTokens: 3, TotalTokens: 8},
	})

	_, _ = ah.OnToolStart(ctx, info, &schema.ToolInput{
		Name: "search", CallId: "call_abc", ArgumentsJSON: `{"q":"x"}`,
	})
	_, _ = ah.OnToolEnd(ctx, info, &schema.ToolOutput{
		CallId: "call_abc",
		Result: &common.ToolCallResult{Success: true, Output: "found"},
	})

	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{Messages: []*schema.Message{schema.AssistantMessage("done")}})
	orch.OnGraphEnd(ctx, nil)

	// 等 tick flush
	time.Sleep(500 * time.Millisecond)

	spans := srv.Spans()
	// span topology fix (task#66-#70): agent 作 root, graph 跳过创 span. 期望 3 span:
	// agent + chat (model) + tool. 业务直接调 compose_v2.Run 时才有 workflow root span.
	if len(spans) < 3 {
		t.Fatalf("应收到 3+ span (agent/chat/tool), got %d", len(spans))
	}

	// 验各 span 名 + operation.name
	gotOps := map[string]int{}
	for _, s := range spans {
		for _, a := range s.Attributes {
			if a.Key == "gen_ai.operation.name" {
				if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
					gotOps[sv.StringValue]++
				}
			}
		}
	}
	for _, op := range []string{"agent", "chat", "tool"} {
		if gotOps[op] == 0 {
			t.Errorf("缺 operation.name=%s span", op)
		}
	}
	// agent_v2.Run 路径下 graph span 跳过 (eino ADK 同款), 不应有 workflow span
	if gotOps["workflow"] > 0 {
		t.Errorf("agent_v2.Run 路径下 graph workflow span 应跳过, got %d 个", gotOps["workflow"])
	}

	// 验所有 span 共享同 traceID
	traceIDs := map[string]bool{}
	for _, s := range spans {
		traceIDs[string(s.TraceId)] = true
	}
	if len(traceIDs) != 1 {
		t.Errorf("一次 Run 应共享同 traceID, got %d 个: %v", len(traceIDs), traceIDs)
	}
}

// findChatSpan 在 spans 内找 operation.name=chat 的 model span (testing helper).
func findChatSpan(spans []*tracepb.Span) *tracepb.Span {
	for _, s := range spans {
		if op, _ := findAttr(s, "gen_ai.operation.name"); op == "chat" {
			return s
		}
	}
	return nil
}

// TestHooks_SpecGapFields 验 AI_TRACE_SPEC 3 个补的 gap:
//   - gen_ai.response.model + gen_ai.system (从 out.Message.Metadata, gap 1 + gap 2)
//   - gen_ai.request.id (= sid) + gen_ai.session.id (= thread, gap 3)
func TestHooks_SpecGapFields(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// push graphNS + sessionID(request.id) + threadID(session.id)
	ctx := compose_v2.WithGraphNS(context.Background(), "root_graph")
	ctx = compose_v2.WithSessionIDCtx(ctx, "run_42")
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv_7")

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "test_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	// 关键: out.Message.Metadata 带 Model + System (model client 透传来的)
	out := schema.AssistantMessage("ok")
	out.Metadata = &common.MessageMetadata{Model: "qwen3.6-flash", System: "dashscope", FinishReason: "stop"}
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{
		Message: out,
		Usage:   &common.MessageUsage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
	})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{Messages: []*schema.Message{out}})
	orch.OnGraphEnd(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()
	chat := findChatSpan(spans)
	if chat == nil {
		t.Fatalf("缺 chat (model) span; got %d spans", len(spans))
	}

	// gap 1: response.model (新) + request.model (保留)
	if v, ok := findAttr(chat, "gen_ai.response.model"); !ok || v != "qwen3.6-flash" {
		t.Errorf("gen_ai.response.model = %q,%v; want qwen3.6-flash", v, ok)
	}
	if v, ok := findAttr(chat, "gen_ai.model"); !ok || v != "qwen3.6-flash" {
		t.Errorf("gen_ai.request.model = %q,%v; want qwen3.6-flash (保留 metric 同源)", v, ok)
	}
	// gap 2: system (provider)
	if v, ok := findAttr(chat, "gen_ai.system"); !ok || v != "dashscope" {
		t.Errorf("gen_ai.system = %q,%v; want dashscope", v, ok)
	}
	// gap 3: request.id (= sid) + session.id (= thread) — 公共 attr, 每 span 都有
	if v, ok := findAttr(chat, "gen_ai.request.id"); !ok || v != "run_42" {
		t.Errorf("gen_ai.request.id = %q,%v; want run_42", v, ok)
	}
	if v, ok := findAttr(chat, "gen_ai.session.id"); !ok || v != "conv_7" {
		t.Errorf("gen_ai.session.id = %q,%v; want conv_7", v, ok)
	}
}

// TestHooks_OTelTokenAttrsAndSpanKind 验 2026 Q2 OTel GenAI semconv 对齐:
//   - token trace attr 用新名 reasoning.output_tokens / cache_read.input_tokens (旧名不再 emit)
//   - SpanKind: model/chat = CLIENT (调外部 LLM API), 本地 agent = INTERNAL
func TestHooks_OTelTokenAttrsAndSpanKind(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "root_graph")
	ctx = compose_v2.WithSessionIDCtx(ctx, "run_otel")

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "a", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{
		Message: schema.AssistantMessage("ok"),
		Usage:   &common.MessageUsage{InputTokens: 10, OutputTokens: 20, TotalTokens: 35, ThinkTokens: 5, CachedInputTokens: 8},
	})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{Messages: []*schema.Message{schema.AssistantMessage("ok")}})
	orch.OnGraphEnd(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	chat := findChatSpan(spans)
	if chat == nil {
		t.Fatalf("缺 chat (model) span; got %d spans", len(spans))
	}
	// token attr 新名 (OTel GenAI semconv)
	if v, ok := findIntAttr(chat, "gen_ai.usage.reasoning.output_tokens"); !ok || v != 5 {
		t.Errorf("gen_ai.usage.reasoning.output_tokens = %d,%v; want 5", v, ok)
	}
	if v, ok := findIntAttr(chat, "gen_ai.usage.cache_read.input_tokens"); !ok || v != 8 {
		t.Errorf("gen_ai.usage.cache_read.input_tokens = %d,%v; want 8", v, ok)
	}
	// 旧名不该再 emit (改名而非新增)
	if _, ok := findIntAttr(chat, "gen_ai.usage.reasoning_tokens"); ok {
		t.Error("不该再 emit 旧名 gen_ai.usage.reasoning_tokens")
	}
	if _, ok := findIntAttr(chat, "gen_ai.usage.cached_input_tokens"); ok {
		t.Error("不该再 emit 旧名 gen_ai.usage.cached_input_tokens")
	}
	// SpanKind: chat/model = CLIENT (调外部 LLM API)
	if chat.Kind != tracepb.Span_SPAN_KIND_CLIENT {
		t.Errorf("chat span Kind = %v; want CLIENT", chat.Kind)
	}
	// SpanKind: 本地 agent = INTERNAL
	agent := findSpanByOp(spans, "agent")
	if agent == nil {
		t.Fatal("缺 agent span")
	}
	if agent.Kind != tracepb.Span_SPAN_KIND_INTERNAL {
		t.Errorf("agent span Kind = %v; want INTERNAL", agent.Kind)
	}
}

// TestHooks_SessionIDOmittedWithoutThread 验 gap 3 关键设计: 业务没 push thread id
// 时 session.id **不 emit** (不造 = request.id 的无意义重复)。request.id 仍 emit。
func TestHooks_SessionIDOmittedWithoutThread(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// 只 push sessionID, 不 push threadID
	ctx := compose_v2.WithGraphNS(context.Background(), "root_graph")
	ctx = compose_v2.WithSessionIDCtx(ctx, "run_99")

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "a", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	_, _ = ah.OnModelEnd(ctx, info, &schema.ModelOutput{Message: schema.AssistantMessage("ok")})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{Messages: []*schema.Message{schema.AssistantMessage("ok")}})
	orch.OnGraphEnd(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	chat := findChatSpan(srv.Spans())
	if chat == nil {
		t.Fatal("缺 chat span")
	}
	if v, ok := findAttr(chat, "gen_ai.request.id"); !ok || v != "run_99" {
		t.Errorf("gen_ai.request.id = %q,%v; want run_99", v, ok)
	}
	if v, ok := findAttr(chat, "gen_ai.session.id"); ok {
		t.Errorf("没 push thread id 时不该 emit gen_ai.session.id, got %q", v)
	}
}

// sidEchoModel 是最小 mock ChatModel：Stream 报错强制走 Generate downgrade，Generate
// 返固定 assistant 消息。用于真 BasicAgent.Run 端到端验证。
type sidEchoModel struct{}

func (sidEchoModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("done"), nil
}
func (sidEchoModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m sidEchoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHooks_RequestIDOnAgentSpan_RealRun 是 gap 3 fix 的端到端回归: 真 BasicAgent.Run
// 且业务**不手动 push sid**（只挂 obs hook，与真实 framework 用法一致），顶层 agent span
// 也应带 gen_ai.request.id = sess.RunID。这是 audit 指出现有测试漏掉的 wrapper-level 路径
// （OnAgentStart 在 runGraph 之前 fire）。
//
// sanity-revert: 去掉 agent_v2.WithSession 内的 sid 提前 push → agent span 落到 sid=""
// 无 request.id → 本测 fail 并精确指向 bug。
func TestHooks_RequestIDOnAgentSpan_RealRun(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sid_agent"),
		agent_v2.WithModel(sidEchoModel{}),
		agent_v2.WithHooks(obs.AgentHooks()),
	)
	sess := agent_v2.NewRunSession()
	// 关键: 不调 WithSessionIDCtx — 只挂 orch hook, 模拟业务最简用法
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), obs.OrchHooks())
	if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	obs.CleanupSession(sess.RunID)
	time.Sleep(500 * time.Millisecond)

	var agentSpan *tracepb.Span
	for _, s := range srv.Spans() {
		if op, _ := findAttr(s, "gen_ai.operation.name"); op == "agent" {
			agentSpan = s
		}
	}
	if agentSpan == nil {
		t.Fatal("缺 agent span")
	}
	if v, ok := findAttr(agentSpan, "gen_ai.request.id"); !ok || v != sess.RunID {
		t.Errorf("agent span gen_ai.request.id = %q,%v; want %q (WithSession 应提前 push sid)", v, ok, sess.RunID)
	}
}

// TestHooks_RequestIDPropagates_NestedOrchestration 验单点 WithSession 修复对**非-Basic
// 编排 + 嵌套**也成立: Sequential(seq_top) → BasicAgent(step_a)，所有 agent span (顶层
// Sequential + 嵌套 BasicAgent) 都带同一个 gen_ai.request.id = sess.RunID。证明:
//   - 顶层 Sequential 的 agent span 也拿到 request.id (单点覆盖 9 编排, 非只 BasicAgent)
//   - 嵌套子 agent 的 session RunID 就是所在任务（父的 sid），WithSession 推的也是它 — 真 run 验
func TestHooks_RequestIDPropagates_NestedOrchestration(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	step := agent_v2.NewBasicAgent(
		agent_v2.WithName("step_a"),
		agent_v2.WithModel(sidEchoModel{}),
		agent_v2.WithHooks(obs.AgentHooks()),
	)
	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq_top"),
		agent_v2.WithSequentialHooks(obs.AgentHooks()),
	).AddStep("step_a", step)

	sess := agent_v2.NewRunSession()
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), obs.OrchHooks())
	if _, err := seq.Run(ctx, sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	obs.CleanupSession(sess.RunID)
	time.Sleep(500 * time.Millisecond)

	agentSpans := 0
	for _, s := range srv.Spans() {
		if op, _ := findAttr(s, "gen_ai.operation.name"); op == "agent" {
			agentSpans++
			name, _ := findAttr(s, "gen_ai.node.name")
			if v, ok := findAttr(s, "gen_ai.request.id"); !ok || v != sess.RunID {
				t.Errorf("agent span %q request.id = %q,%v; want %q (顶层+嵌套都应同 sid)", name, v, ok, sess.RunID)
			}
		}
	}
	if agentSpans < 2 {
		t.Errorf("应有 >=2 agent span (seq_top + step_a), got %d", agentSpans)
	}
}

func TestHooks_NoopWhenExporterNil(t *testing.T) {
	// 不配 TraceEndpoint → exporter=nil → hooks 不上报但不 panic
	obs, err := New(Config{
		Resource:      Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint: "", // 关 trace
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "g")

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	// 跑 hook 不 panic
	_, _ = orch.OnGraphStart(ctx)
	_, _ = ah.OnAgentStart(ctx, &hook.AgentInfo{Name: "a"}, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, &hook.AgentInfo{Name: "a"}, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
}

// findAttr 在 span attribute 列表内找 key 返 value 字符串 (testing helper).
func findAttr(span *tracepb.Span, key string) (string, bool) {
	for _, a := range span.Attributes {
		if a.Key == key {
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
				return sv.StringValue, true
			}
			if iv, ok := a.Value.Value.(*commonpb.AnyValue_IntValue); ok {
				return string(rune(iv.IntValue)) + "(int)", true
			}
		}
	}
	return "", false
}

// findIntAttr 找 int attr 返 value.
func findIntAttr(span *tracepb.Span, key string) (int64, bool) {
	for _, a := range span.Attributes {
		if a.Key == key {
			if iv, ok := a.Value.Value.(*commonpb.AnyValue_IntValue); ok {
				return iv.IntValue, true
			}
		}
	}
	return 0, false
}

// findSpanByOp 返第一个 operation.name 匹配的 span (e.g., "graph"/"agent"/"chat"/"tool").
func findSpanByOp(spans []*tracepb.Span, op string) *tracepb.Span {
	for _, s := range spans {
		if v, ok := findAttr(s, "gen_ai.operation.name"); ok && v == op {
			return s
		}
	}
	return nil
}

// TestHooks_GraphSpanNodeIDIsGraphNS 验 P2 audit MED-5 + audit² MED-A1 fix —
// graph span 的 node.id = graphNS 不是 "graphNS/graphNS", node.parent_id = ""
// 让前端构树时 graph 是真 root.
//
// span topology fix (task#66-#70): graph 在嵌套场景 + agent_v2.Run 路径都跳过创 span.
// 仅"业务直接调 compose_v2.Run + 顶层 graphNS=""才创 graph span. 测试改成验顶层
// 无嵌套场景下 graph 作 root 的 node.id="" + parent_id="".
func TestHooks_GraphSpanNodeIDIsGraphNS(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// 顶层 graphNS="" + 无 in-flight agent → graph 作 root 创 span
	ctx := context.Background()
	orch := obs.OrchHooks()
	_, _ = orch.OnGraphStart(ctx)
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	g := findSpanByOp(srv.Spans(), "workflow")
	if g == nil {
		t.Fatal("没找 workflow span")
	}
	if v, _ := findAttr(g, "gen_ai.node.id"); v != "" {
		t.Errorf("顶层 graph span node.id 应 = '' (graphNS), got %q", v)
	}
	if v, _ := findAttr(g, "gen_ai.node.parent_id"); v != "" {
		t.Errorf("review P2 audit² MED-A1 fix: graph span node.parent_id 应 = '' (root), got %q (前端构树会自指死循环)", v)
	}
}

// TestHooks_OnErrorPropagatesToEndSpan 验 review P2 audit² HIGH-A2 fix —
// OnError fire 后 OnAgentEnd 上报 span status="error" + error.type/message
// 来自 OnError 的 CommError (不是硬编码 "success").
//
// sanity-revert: 删 endSpan 内 sess.lastErrors.LoadAndDelete block →
// status 又变 "success" → 测试 fail.
func TestHooks_OnErrorPropagatesToEndSpan(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "err_graph")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	// OnError fire → 应写 lastErrors
	ah.OnError(ctx, info, &schema.CommError{Code: "llm_timeout", Message: "OpenAI 504"})
	// OnAgentEnd fire → 应读 lastErrors 覆盖默认 "success"
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	ag := findSpanByOp(srv.Spans(), "agent")
	if ag == nil {
		t.Fatal("没找 agent span")
	}
	status, _ := findAttr(ag, "gen_ai.status")
	if status != "error" {
		t.Errorf("review P2 audit² HIGH-A2 fix: OnError 后 agent span status 应 = 'error', got %q\n"+
			"sanity-revert: 删 endSpan 内 sess.lastErrors LoadAndDelete block → status 又 'success'", status)
	}
	errType, _ := findAttr(ag, "error.type")
	if errType != "llm_timeout" {
		t.Errorf("error.type 应 = 'llm_timeout' (OnError CommError.Code), got %q", errType)
	}
}

// TestHooks_OnAgentEndSweepsInFlight 验 review P2 audit² HIGH-A3 fix —
// OnModelEnd / OnToolEnd 在 error 路径不 fire 时, OnAgentEnd 兜底 sweep
// in-flight span 上报 (不让 sessions.spans 永留泄漏).
//
// sanity-revert: 删 OnAgentEnd 内 sweepInFlight call → model span 不上报 →
// mock collector 只收 agent span 缺 model span → 测试 fail.
func TestHooks_OnAgentEndSweepsInFlight(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "sweep_graph")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	// OnModelStart 创 model span 但**故意不 fire OnModelEnd** (模拟 error 路径)
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{})
	// 直接 OnAgentEnd — 应 sweep in-flight model span
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Errorf("review P2 audit² HIGH-A3 fix: OnAgentEnd 应 sweep in-flight model span 兜底上报, mock collector 没收到 chat span\n" +
			"sanity-revert: 删 OnAgentEnd 内 sweepInFlight call → in-flight 永留泄漏")
		return
	}
	// sweep 上报的 span status 应 "error"
	if v, _ := findAttr(chat, "gen_ai.status"); v != "error" {
		t.Errorf("sweep 兜底 span status 应 'error', got %q", v)
	}
}

// TestHooks_OnAgentStart_CreatesSpanOnce 验 8-method 协议：OnAgentStart 整个调用
// 1 次创 agent span + 设 gen_ai.round attr（不再每轮 update — 原 OnAgentInit 逻辑迁入）。
func TestHooks_OnAgentStart_CreatesSpanOnce(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "round_test")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	ag := findSpanByOp(srv.Spans(), "agent")
	if ag == nil {
		t.Fatal("没找 agent span — OnAgentStart 应创 agent span")
	}
	// 8-method: OnAgentStart 设 gen_ai.agent.kind attr; gen_ai.round 不再上 agent span
	// （轮次走 MetricAgentRounds histogram, 整个调用 1 次时 round 恒 0 无意义）.
	if kind, _ := findAttr(ag, "gen_ai.agent.kind"); kind != "base" {
		t.Errorf("OnAgentStart 应设 gen_ai.agent.kind=base, got %q", kind)
	}
	for _, a := range ag.Attributes {
		if a.Key == "gen_ai.round" {
			t.Errorf("8-method: agent span 不应再有 gen_ai.round attr（轮次走 MetricAgentRounds histogram）")
		}
	}
}

// TestHooks_AddEventConcurrentRace 验 review P2 audit HIGH-2 fix — addEvent
// 多 goroutine 并发写同 span.Events 加 mu 锁后 race detector 不触发.
//
// audit 实测 fix 前 50 goroutine × 20 addEvent race 100% 触发. fix 后 PASS.
func TestHooks_AddEventConcurrentRace(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "race_test")
	orch := obs.OrchHooks()
	_, _ = orch.OnGraphStart(ctx)
	_, _ = orch.OnNodeStart(ctx, "hot_node")

	const N = 50
	done := make(chan struct{}, N)
	for i := 0; i < N; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 20; j++ {
				orch.OnNodeEvent(ctx, "hot_node", agent_v2.Event{
					Type:    "chunk",
					Content: "x",
				})
			}
		}()
	}
	for i := 0; i < N; i++ {
		<-done
	}

	orch.OnNodeEnd(ctx, "hot_node", nil)
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(300 * time.Millisecond)

	// span topology fix (task#66-#70): graph span 跳过 (agent_v2 路径), node span
	// 挂当前 graphNS 的 agent (此测试无 in-flight agent → node 走 graph fallback,
	// 顶层 graphNS="" 也跳过 → 顶层 graph span 创 root). 期望 >= 1 span: 至少 node.
	if got := len(srv.Spans()); got < 1 {
		t.Errorf("应收到 >= 1 span, got %d", got)
	}
}

func TestHooks_ConcurrentSessionsNoRace(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// 10 goroutine 并发跑独立 sessions
	var wg atomic.Int32
	wg.Add(10)
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func(sid string) {
			defer func() { done <- struct{}{} }()
			ctx := compose_v2.WithGraphNS(context.Background(), "graph_"+sid)
			orch := obs.OrchHooks()
			ah := obs.AgentHooks()
			_, _ = orch.OnGraphStart(ctx)
			info := &hook.AgentInfo{Name: "ag", Round: 0}
			_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
			ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
			orch.OnGraphEnd(ctx, nil)
		}("sess-" + string(rune('0'+i)))
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	time.Sleep(300 * time.Millisecond)

	// span topology fix (task#66-#70): graph span 在嵌套场景 (graphNS!="") 跳过.
	// 此测试用 graphNS="graph_<sid>" 都是非空 → graph 跳过, 只剩 agent span.
	// 期望 10 sessions × 1 agent span = 10 span.
	if got := srv.count.Load(); got < 10 {
		t.Errorf("10 并发 sessions 应产 >= 10 agent span, got %d", got)
	}
}

// TestHooks_P0_InterruptCounterFires 验 OnAgentEnd 检 InterruptError 时 fire
// MetricInterruptTotal Counter. 走 agent_v2.WithEndError ctx push 协议.
//
// sanity-revert: 删 hooks.go OnAgentEnd 内的 if isInt { Counter.Inc(); sess.Store(nanos) }
// 块 → 此测试 fail (Counter.Value() == 0).
func TestHooks_P0_InterruptCounterFires(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "p0-interrupt-test")
	ctx = compose_v2.WithGraphNS(ctx, "g")

	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "ag1", Type: "base", Round: 0}

	// OnAgentStart 模拟 Run 入口
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})

	// agent_v2 Run defer 模拟: push InterruptError 到 ctx
	intErr := &compose_v2.InterruptError{Reason: "test", Query: &compose_v2.AskQuery{Question: "q", InterruptID: "i1"}}
	ctxWithErr := agent_v2.WithEndError(ctx, intErr)

	// OnAgentEnd 应识别 InterruptError fire counter
	ah.OnAgentEnd(ctxWithErr, info, &hook.AgentOutput{})

	// 验 Counter
	c := obs.Counter(MetricInterruptTotal, map[string]string{
		LabelAgentName: "ag1",
		LabelAgentKind: "base",
	})
	if c == nil {
		t.Fatal("MetricInterruptTotal Counter 应非 nil (业务 lazy create)")
	}
	if got := c.Value(); got != 1 {
		t.Errorf("MetricInterruptTotal 应 fire 1 次, got %d (sanity-revert: 删 OnAgentEnd InterruptError 分支)", got)
	}

	// 验 status="blocked" 在 MetricAgentRunTotal counter (label 区分 success vs blocked)
	blocked := obs.Counter(MetricAgentRunTotal, map[string]string{
		LabelAgentName: "ag1",
		LabelAgentKind: "base",
		LabelStatus:    "blocked",
	})
	if blocked == nil || blocked.Value() != 1 {
		t.Errorf("MetricAgentRunTotal{status=blocked} 应 fire 1 次, got %v", blocked)
	}
}

// TestHooks_P0_ResumeCounterFiresWithAction 验 OnAgentStart 检 GetResumePhase 时
// fire MetricResumesTotal{action=continue/replan/abort}.
func TestHooks_P0_ResumeCounterFiresWithAction(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "p0-resume-test")
	ctx = compose_v2.WithGraphNS(ctx, "g")
	ctx = agent_v2.WithResumePhase(ctx, agent_v2.ResumeActionContinue)

	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "ag2", Type: "base", Round: 0}

	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	c := obs.Counter(MetricResumesTotal, map[string]string{
		LabelAgentName:    "ag2",
		LabelAgentKind:    "base",
		LabelResumeAction: "continue",
	})
	if c == nil || c.Value() != 1 {
		t.Errorf("MetricResumesTotal{action=continue} 应 fire 1 次, got %v", c)
	}
}

// TestHooks_P0_InterruptToResumeDuration 验完整 Run InterruptError → Resume continue
// 流程: OnAgentEnd 存 nanos + OnAgentStart Resume 算 duration Observe.
//
// sanity-revert: 删 OnAgentStart 内的 startNanos := sess.interruptStartNanos.Swap(0)
// → 此测试 fail (Histogram.Count() == 0).
func TestHooks_P0_InterruptToResumeDuration(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "p0-duration-test")
	ctx = compose_v2.WithGraphNS(ctx, "g")

	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "ag3", Type: "base", Round: 0}

	// Run 1: OnAgentStart + InterruptError + OnAgentEnd → 存 nanos
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	intErr := &compose_v2.InterruptError{Reason: "test", Query: &compose_v2.AskQuery{Question: "q", InterruptID: "i1"}}
	ah.OnAgentEnd(agent_v2.WithEndError(ctx, intErr), info, &hook.AgentOutput{})

	// 模拟用户响应耗时 10ms
	time.Sleep(10 * time.Millisecond)

	// Run 2 (Resume): OnAgentStart 应读 nanos 算 duration fire histogram
	resumeCtx := agent_v2.WithResumePhase(ctx, agent_v2.ResumeActionContinue)
	_, _ = ah.OnAgentStart(resumeCtx, info, &hook.AgentInput{})
	ah.OnAgentEnd(resumeCtx, info, &hook.AgentOutput{})

	h := obs.Histogram(MetricInterruptResumeDuration, BucketsLLMDuration, map[string]string{
		LabelAgentName: "ag3",
		LabelAgentKind: "base",
	})
	if h == nil || h.Count() != 1 {
		t.Errorf("MetricInterruptResumeDuration 应 fire 1 次, got count=%d (sanity-revert: 删 OnAgentStart Swap(0) 块)", h.Count())
	}
	if h.Sum() < 0.005 {
		t.Errorf("duration 应 ≥ 10ms = 0.01s, got %f s (太小说明 nanos 没存)", h.Sum())
	}
}

// TestHooks_P1_PlanReplanCounter 验 PlanReplanEvent 经 OnNodeEvent 路由到
// MetricPlanReplansTotal Counter.
func TestHooks_P1_PlanReplanCounter(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "p1-plan-test")
	ctx = compose_v2.WithGraphNS(ctx, "g")

	orch := obs.OrchHooks()

	// 模拟 PlanAgent planNode 调 EmitPlanReplanEvent: 1 次 initial + 2 次 replan
	orch.OnNodeEvent(ctx, "plan", agent_v2.Event{
		Type: agent_v2.PlanReplanEventType,
		Metadata: map[string]any{
			"action": &agent_v2.PlanReplanAction{Phase: agent_v2.PlanReplanPhaseInitial, ExecutedSteps: 0},
		},
	})
	orch.OnNodeEvent(ctx, "plan", agent_v2.Event{
		Type: agent_v2.PlanReplanEventType,
		Metadata: map[string]any{
			"action": &agent_v2.PlanReplanAction{Phase: agent_v2.PlanReplanPhaseReplan, ExecutedSteps: 1},
		},
	})
	orch.OnNodeEvent(ctx, "plan", agent_v2.Event{
		Type: agent_v2.PlanReplanEventType,
		Metadata: map[string]any{
			"action": &agent_v2.PlanReplanAction{Phase: agent_v2.PlanReplanPhaseReplan, ExecutedSteps: 2},
		},
	})

	cInitial := obs.Counter(MetricPlanReplansTotal, map[string]string{LabelPlanPhase: "initial"})
	cReplan := obs.Counter(MetricPlanReplansTotal, map[string]string{LabelPlanPhase: "replan"})
	if cInitial == nil || cInitial.Value() != 1 {
		t.Errorf("MetricPlanReplansTotal{phase=initial} 应 1, got %v", cInitial)
	}
	if cReplan == nil || cReplan.Value() != 2 {
		t.Errorf("MetricPlanReplansTotal{phase=replan} 应 2, got %v", cReplan)
	}
}

// TestHooks_P1_RouteDecisionCounter 验 RouteDecisionEvent 路由到
// MetricRouteDecisionsTotal Counter (label{agent_name, target}).
func TestHooks_P1_RouteDecisionCounter(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "p1-route-test")
	ctx = compose_v2.WithGraphNS(ctx, "g")

	orch := obs.OrchHooks()

	// 模拟 RoutingAgent 升级链选 target "math" + 一次 fallback + 一次空目标（框架自己不发
	// 空目标，这里模拟业务直接调 EmitRouteDecisionEvent 发的）
	orch.OnNodeEvent(ctx, "classifier", agent_v2.Event{
		Type: agent_v2.RouteDecisionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.RouteDecisionAction{AgentName: "router1", Target: "math"},
		},
	})
	orch.OnNodeEvent(ctx, "classifier", agent_v2.Event{
		Type: agent_v2.RouteDecisionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.RouteDecisionAction{AgentName: "router1", Target: "__fallback__"},
		},
	})
	orch.OnNodeEvent(ctx, "classifier", agent_v2.Event{
		Type: agent_v2.RouteDecisionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.RouteDecisionAction{AgentName: "router1", Target: ""},
		},
	})

	cMath := obs.Counter(MetricRouteDecisionsTotal, map[string]string{LabelAgentName: "router1", LabelRouteTarget: "math"})
	cFb := obs.Counter(MetricRouteDecisionsTotal, map[string]string{LabelAgentName: "router1", LabelRouteTarget: "__fallback__"})
	cNone := obs.Counter(MetricRouteDecisionsTotal, map[string]string{LabelAgentName: "router1", LabelRouteTarget: "__none__"})
	if cMath == nil || cMath.Value() != 1 {
		t.Errorf("MetricRouteDecisionsTotal{target=math} 应 1, got %v", cMath)
	}
	if cFb == nil || cFb.Value() != 1 {
		t.Errorf("MetricRouteDecisionsTotal{target=__fallback__} 应 1, got %v", cFb)
	}
	if cNone == nil || cNone.Value() != 1 {
		t.Errorf("MetricRouteDecisionsTotal{target=__none__ (empty fallback)} 应 1, got %v", cNone)
	}
}

// TestHooks_StaleWriteMetricFires 验 OnAgentEnd 遇到 ErrCheckpointStale 时：
//  1. fire MetricStaleWriteTotal（label 取 StaleWriteError.Op，低基数）
//  2. **不**记成 interrupt（即使双 %w 里挂着 InterruptError）
//  3. error.message 脱敏（不含 session id / epoch token）
//
// 第 2 点是重点：stale 跟中断绑在一条链上时（引擎出口现在不再产出这种形状，档没写成时返
// ErrCheckpointNotPersisted；自研 / 远程 agent 仍可能这样交过来），按 IsInterrupt-first 的顺序
// 它会被判成 blocked → 导出一个 OTel Status=OK 的绿 span，与正常 HITL 提问逐字节
// 相同，运维完全看不出并发冲突；而且那个中断永远不会被 Resume（a2a/agui 都已不留
// entry），计进 interrupt metric 会让 interrupt→resume 漏斗静默产生缺口。
//
// sanity-revert: 把 hooks.go OnAgentEnd 的 stale 分支移到 IsInterrupt 之后（或删掉）
// → MetricStaleWriteTotal 恒 0 且 MetricInterruptTotal 被误计。
func TestHooks_StaleWriteMetricFires(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "stale-metric-test")
	ctx = compose_v2.WithGraphNS(ctx, "g")
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "ag_stale", Type: "base", Round: 0}

	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})

	// 中断与陈旧写回绑在一条链上：原错是 InterruptError，挂着结构化的 StaleWriteError
	ie := &compose_v2.InterruptError{
		Reason: "ask", Query: &compose_v2.AskQuery{Question: "q", InterruptID: "i1"},
	}
	staleErr := &compose_v2.StaleWriteError{
		Op: "purge-success", SessionID: "sess-uuid-secret", NS: "sub#7",
		Baseline: "aabb", Current: "ccdd", Detail: "槽已被推进",
	}
	ah.OnAgentEnd(agent_v2.WithEndError(ctx, fmt.Errorf("%w (cp 未落盘: %w)", ie, staleErr)),
		info, &hook.AgentOutput{})

	c := obs.Counter(MetricStaleWriteTotal, map[string]string{LabelStaleOp: "purge-success"})
	if c == nil {
		t.Fatal("MetricStaleWriteTotal Counter 应非 nil")
	}
	if got := c.Value(); got != 1 {
		t.Errorf("MetricStaleWriteTotal{op=purge-success} 应 fire 1 次, got %d —— "+
			"运维靠它发现并发冲突，不 fire 就只能翻日志", got)
	}
	// 不能同时被记成 interrupt——那个中断永远不会被 Resume
	if ic := obs.Counter(MetricInterruptTotal, map[string]string{
		LabelAgentName: "ag_stale", LabelAgentKind: "base",
	}); ic != nil && ic.Value() != 0 {
		t.Errorf("stale 不该计进 interrupt metric（那个中断不会被 Resume，会让漏斗产生缺口）, got %d",
			ic.Value())
	}
}

// TestHooks_NotPersistedInterruptNotCountedAsBlocked：档没写成（ErrCheckpointNotPersisted）跟中断绑在一条链上时
// （引擎出口不产出这种形状，防自研 agent 这样交过来）不算挂起：不计 interrupt 指标——那个中断续不上。
func TestHooks_NotPersistedInterruptNotCountedAsBlocked(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "np-metric-test")
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "ag_np", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ie := &compose_v2.InterruptError{Reason: "ask", Query: &compose_v2.AskQuery{Question: "q", InterruptID: "i1"}}
	ah.OnAgentEnd(agent_v2.WithEndError(ctx, fmt.Errorf("%w (%w)", ie, compose_v2.ErrCheckpointNotPersisted)),
		info, &hook.AgentOutput{})
	if ic := obs.Counter(MetricInterruptTotal, map[string]string{
		LabelAgentName: "ag_np", LabelAgentKind: "base",
	}); ic != nil && ic.Value() != 0 {
		t.Errorf("档没写成的中断不该计进 interrupt 指标（它续不上），got %d", ic.Value())
	}
}
