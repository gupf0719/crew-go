package a2a_test

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// a2a 跨进程 trace 串联端到端验收：真 HTTP wire + 两个 Observability（client / server 各一个
// TracerProvider，模拟两个进程）→ 验跨进程父子链
// host agent → a2a client span → a2a server span → 远端内部 agent，全程同一 trace ID。
// trace context 经 W3C traceparent 写进 Message.metadata 过 wire。

// traceFixedModel 确定性 ChatModel（server agent 用，不依赖外部 LLM）。
type traceFixedModel struct{ answer string }

func (m *traceFixedModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.answer), nil
}
func (m *traceFixedModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return schema.NewSingleChunkReader(schema.AssistantMessage(m.answer)), nil
}
func (m *traceFixedModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// newRecordingObs 建一个 span 记进内存的 Observability（一个「进程」一个）。
func newRecordingObs(t *testing.T) (*observability.Observability, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return observability.New(observability.WithTracerProvider(tp),
		observability.WithMeterProvider(metricnoop.NewMeterProvider())), rec
}

// startServer 把 agent 暴露成 A2A server，返回 baseURL。
func startTraceServer(t *testing.T, ag agent_v2.Agent, obs *observability.Observability) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen svc: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	opts := []server.ExecutorOption{testOwnerOpt()}
	if obs != nil {
		opts = append(opts, server.WithExecutorObservability(obs))
	}
	httpSrv := &http.Server{Handler: server.Handler(ag, baseURL, opts...)}
	go func() { _ = httpSrv.Serve(ln) }()
	t.Cleanup(func() { _ = httpSrv.Close() })
	return baseURL
}

// findSpan 按名字与 kind 找一个已结束的 span。
func findSpan(spans []sdktrace.ReadOnlySpan, name string, kind trace.SpanKind) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name && s.SpanKind() == kind {
			return s
		}
	}
	return nil
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name()+"/"+s.SpanKind().String())
	}
	return out
}

func TestA2ATraceChain_CrossProcess(t *testing.T) {
	serverObs, serverRec := newRecordingObs(t)
	clientObs, clientRec := newRecordingObs(t)

	svcAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("svc_agent"),
		agent_v2.WithModel(&traceFixedModel{answer: "内部代号 SECRET-7741"}),
		agent_v2.WithHooks(serverObs.AgentHooks()), // 远端内部 agent 的 span 挂在 server span 下
	)
	baseURL := startTraceServer(t, svcAgent, serverObs)

	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, baseURL, a2a.WithObservability(clientObs))
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}

	// client 侧的宿主 agent span 作活跃父，client span 挂在它下面。
	sess := agent_v2.NewRunSession()
	hctx := compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	ah := clientObs.AgentHooks()
	hostInfo := &hook.AgentInfo{Name: "host_agent", Type: "base", Round: 0}
	hctx, _ = ah.OnAgentStart(hctx, hostInfo, &hook.AgentInput{})
	events, err := ra.Run(hctx, sess, schema.UserMessage("查代号"))
	if err != nil {
		t.Fatalf("ra.Run: %v", err)
	}
	ah.OnAgentEnd(hctx, hostInfo, &hook.AgentOutput{})

	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "SECRET-7741") {
		t.Fatalf("远程应答应含 SECRET-7741（A2A wire 打通），got %v", final)
	}

	cs, ss := clientRec.Ended(), serverRec.Ended()
	host := findSpan(cs, "invoke_agent host_agent", trace.SpanKindInternal)
	client := findSpan(cs, "invoke_agent svc_agent", trace.SpanKindClient)
	srvSpan := findSpan(ss, "a2a.server message_stream", trace.SpanKindServer)
	inner := findSpan(ss, "invoke_agent svc_agent", trace.SpanKindInternal)
	if host == nil || client == nil || srvSpan == nil || inner == nil {
		t.Fatalf("缺 span：host=%v client=%v server=%v inner=%v；client 侧 %v，server 侧 %v",
			host != nil, client != nil, srvSpan != nil, inner != nil, spanNames(cs), spanNames(ss))
	}

	// 全程同一 trace ID（client 的 trace context 经 metadata 过 wire）。
	tid := host.SpanContext().TraceID()
	for _, s := range []sdktrace.ReadOnlySpan{client, srvSpan, inner} {
		if s.SpanContext().TraceID() != tid {
			t.Errorf("跨进程 trace ID 不一致：%s=%s，应=%s（metadata 没过 wire 或 server 没取出）",
				s.Name(), s.SpanContext().TraceID(), tid)
		}
	}
	// 父子链 host ← client ← server ← 内部 agent。
	if client.Parent().SpanID() != host.SpanContext().SpanID() {
		t.Errorf("a2a client span 应挂在 host agent span 下，parent=%s 应=%s", client.Parent().SpanID(), host.SpanContext().SpanID())
	}
	if srvSpan.Parent().SpanID() != client.SpanContext().SpanID() || !srvSpan.Parent().IsRemote() {
		t.Errorf("a2a server span 应以远端 client span 为父，parent=%s remote=%v 应=%s",
			srvSpan.Parent().SpanID(), srvSpan.Parent().IsRemote(), client.SpanContext().SpanID())
	}
	if inner.Parent().SpanID() != srvSpan.SpanContext().SpanID() {
		t.Errorf("server 内部 agent span 应挂在 a2a server span 下，parent=%s 应=%s", inner.Parent().SpanID(), srvSpan.SpanContext().SpanID())
	}
}

// TestA2ATraceChain_FailOpen：client 不注入 trace context（没接 observability）时，server 照常完成业务，
// 自己起一条新 trace（server span 没有父）。
func TestA2ATraceChain_FailOpen(t *testing.T) {
	serverObs, serverRec := newRecordingObs(t)
	svcAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("svc_agent2"),
		agent_v2.WithModel(&traceFixedModel{answer: "代号 SECRET-7741"}),
		agent_v2.WithHooks(serverObs.AgentHooks()),
	)
	baseURL := startTraceServer(t, svcAgent, serverObs)

	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, baseURL) // 无 WithObservability：不注入 trace context
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查代号"))
	if err != nil {
		t.Fatalf("没有 trace context 时 server 应正常完成业务，got %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "SECRET-7741") {
		t.Fatalf("应正常返回答案，got %v", final)
	}

	srvSpan := findSpan(serverRec.Ended(), "a2a.server message_stream", trace.SpanKindServer)
	if srvSpan == nil {
		t.Fatalf("server span 应照常记录，got %v", spanNames(serverRec.Ended()))
	}
	if srvSpan.Parent().IsValid() {
		t.Errorf("没有入站 trace context 时 server span 应是新 trace 的根，got parent %s", srvSpan.Parent().SpanID())
	}
}
