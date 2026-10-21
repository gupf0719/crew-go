package a2a_test

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

// a2a 跨语言 trace 串联端到端验收（第 6 步）：真 HTTP wire + 双 obs（client/server 各
// 一个 Observability，模拟两进程）→ 抓两侧上报 span → 验跨进程父子链
// host agent → a2a_client → a2a_server → 内部 agent，全程同一 traceID。
// 覆盖协议验收 1-3（server span/内部 span 挂 client span 下 + 同 trace）+ metadata 过 wire。

// capSrv 是收 OTLP span 的最小 grpc capture server（两 obs 都 export 到这）。
type capSrv struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []*tracepb.Span
}

func (c *capSrv) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			c.spans = append(c.spans, ss.Spans...)
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (c *capSrv) all() []*tracepb.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*tracepb.Span(nil), c.spans...)
}

// traceFixedModel 确定性 ChatModel（server agent 用，不依赖外部 LLM）。
type traceFixedModel struct{ answer string }

func (m *traceFixedModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.answer), nil
}
func (m *traceFixedModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return schema.NewSingleChunkReader(schema.AssistantMessage(m.answer)), nil
}
func (m *traceFixedModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestA2ATraceChain_CrossProcess(t *testing.T) {
	// ── OTLP capture grpc server（两 obs 共用）──
	lnCap, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen cap: %v", err)
	}
	gsrv := grpc.NewServer()
	cap := &capSrv{}
	coltracepb.RegisterTraceServiceServer(gsrv, cap)
	go func() { _ = gsrv.Serve(lnCap) }()
	defer gsrv.Stop()

	mkObs := func(app string) *observability.Observability {
		o, e := observability.New(observability.Config{
			Resource:             observability.Resource{AppName: app, EnvName: "test", IP: "127.0.0.1", SDKVersion: "v"},
			TraceEndpoint:        lnCap.Addr().String(),
			TraceExportInterval:  100 * time.Millisecond,
			TraceExportBatchSize: 100,
		})
		if e != nil {
			t.Fatalf("obs %s: %v", app, e)
		}
		return o
	}
	serverObs := mkObs("svc-app")
	defer serverObs.Close()
	clientObs := mkObs("host-app")
	defer clientObs.Close()

	// ── server 进程：确定性 agent 暴露 A2A server（WithExecutorObservability）──
	svcAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("svc_agent"),
		agent_v2.WithModel(&traceFixedModel{answer: "内部代号 SECRET-7741"}),
		agent_v2.WithHooks(serverObs.AgentHooks()), // 让内部 agent span fire（挂 a2a_server span 下）
	)
	lnSvc, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen svc: %v", err)
	}
	baseURL := "http://" + lnSvc.Addr().String()
	httpSrv := &http.Server{Handler: server.Handler(svcAgent, baseURL, server.WithExecutorObservability(serverObs), testOwnerOpt())}
	go func() { _ = httpSrv.Serve(lnSvc) }()
	defer func() { _ = httpSrv.Close() }()

	// ── client 进程：RemoteAgent（WithObservability）──
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, baseURL, a2a.WithObservability(clientObs))
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}

	// host agent span（client 侧活跃父，让 client span 挂它）+ ra.Run 用同 session
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

	// 返回正确（A2A wire 真打通）
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "SECRET-7741") {
		t.Fatalf("远程应答应含 SECRET-7741（A2A wire 打通），got %v", final)
	}

	time.Sleep(700 * time.Millisecond) // 等两 obs export tick

	// ── 抓 span 验跨进程父子链 ──
	byName := map[string]*tracepb.Span{}
	for _, s := range cap.all() {
		byName[s.Name] = s
	}
	host := byName["gen_ai.agent.host_agent"]
	client := byName["gen_ai.a2a_client.svc_agent"]
	srvSpan := byName["gen_ai.a2a_server.message_stream"]
	inner := byName["gen_ai.agent.svc_agent"]

	if host == nil || client == nil || srvSpan == nil || inner == nil {
		names := make([]string, 0)
		for n := range byName {
			names = append(names, n)
		}
		t.Fatalf("缺 span：host=%v client=%v server=%v inner=%v；抓到 %v", host != nil, client != nil, srvSpan != nil, inner != nil, names)
	}

	// 协议核心①：全程同一 traceID（client 的 traceID 经 metadata 过 wire seed 到 server）
	t1 := string(host.TraceId)
	for _, s := range []*tracepb.Span{client, srvSpan, inner} {
		if string(s.TraceId) != t1 {
			t.Errorf("跨进程 traceID 不一致：%s traceID=%q 应=host %q（metadata 没过 wire 或 server 没 seed）",
				s.Name, string(s.TraceId), t1)
		}
	}
	// 协议核心②：父子链 host ← a2a_client ← a2a_server ← 内部 agent
	if string(client.ParentSpanId) != string(host.SpanId) {
		t.Errorf("a2a_client span 应挂 host agent span 下，parent=%q 应=%q", string(client.ParentSpanId), string(host.SpanId))
	}
	if string(srvSpan.ParentSpanId) != string(client.SpanId) {
		t.Errorf("a2a_server span 应挂 a2a_client span 下（跨进程），parent=%q 应=%q（client spanID 经 wire 传到 server）", string(srvSpan.ParentSpanId), string(client.SpanId))
	}
	if string(inner.ParentSpanId) != string(srvSpan.SpanId) {
		t.Errorf("server 内部 agent span 应挂 a2a_server span 下，parent=%q 应=%q", string(inner.ParentSpanId), string(srvSpan.SpanId))
	}
}

// TestA2ATraceChain_FailOpen 协议验收 6：client 不注入 trace metadata（无 obs）时，
// server 仍正常完成业务（fail-open）+ 自起本地 trace（不 panic、不挂 remote）。
func TestA2ATraceChain_FailOpen(t *testing.T) {
	lnCap, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen cap: %v", err)
	}
	gsrv := grpc.NewServer()
	cap := &capSrv{}
	coltracepb.RegisterTraceServiceServer(gsrv, cap)
	go func() { _ = gsrv.Serve(lnCap) }()
	defer gsrv.Stop()

	serverObs, err := observability.New(observability.Config{
		Resource:             observability.Resource{AppName: "svc-app2", EnvName: "test", IP: "127.0.0.1", SDKVersion: "v"},
		TraceEndpoint:        lnCap.Addr().String(),
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("obs: %v", err)
	}
	defer serverObs.Close()

	svcAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("svc_agent2"),
		agent_v2.WithModel(&traceFixedModel{answer: "代号 SECRET-7741"}),
		agent_v2.WithHooks(serverObs.AgentHooks()),
	)
	lnSvc, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen svc: %v", err)
	}
	baseURL := "http://" + lnSvc.Addr().String()
	httpSrv := &http.Server{Handler: server.Handler(svcAgent, baseURL, server.WithExecutorObservability(serverObs), testOwnerOpt())}
	go func() { _ = httpSrv.Serve(lnSvc) }()
	defer func() { _ = httpSrv.Close() }()

	// client 不接 obs → 不注入 trace metadata → server 收到空 metadata
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, baseURL) // 无 WithObservability
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查代号"))
	if err != nil {
		t.Fatalf("fail-open：无 trace metadata 时 server 应正常完成业务（协议验收 6），got %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "SECRET-7741") {
		t.Fatalf("fail-open 应正常返回答案，got %v", final)
	}

	time.Sleep(500 * time.Millisecond)
	// server 自起本地 trace（svc-app2），不挂任何 remote、不 panic
	for _, s := range cap.all() {
		if s.Name == "gen_ai.a2a_server.message_stream" && !strings.HasPrefix(string(s.TraceId), "svc-app2") {
			t.Errorf("fail-open：server 应自起本地 trace（svc-app2），got traceID %q", string(s.TraceId))
		}
	}
}

// TestA2ATraceChain_ServerSessionCleanedByExecutor 验 R33 audit HIGH 修复：a2a server
// session 由 executor 在 conclude 自动清理——business 裸接 AgentHooks（**不**在 served
// agent OnAgentEnd 调 CleanupSession，遵守新 godoc）也不 leak。修复前两难：business 调则
// a2a_server span 被提前删 session 丢、不调则 session leak；修复后 executor 接管（srvSpan.End
// 之后清，LIFO defer），business 安心不调。sanity-revert：删 conclude 的 cleanupSession
// defer → ActiveSessions()!=0 → 本测 FAIL。
func TestA2ATraceChain_ServerSessionCleanedByExecutor(t *testing.T) {
	lnCap, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen cap: %v", err)
	}
	gsrv := grpc.NewServer()
	cap := &capSrv{}
	coltracepb.RegisterTraceServiceServer(gsrv, cap)
	go func() { _ = gsrv.Serve(lnCap) }()
	defer gsrv.Stop()

	serverObs, err := observability.New(observability.Config{
		Resource:             observability.Resource{AppName: "svc-cleanup", EnvName: "test", IP: "127.0.0.1", SDKVersion: "v"},
		TraceEndpoint:        lnCap.Addr().String(),
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("obs: %v", err)
	}
	defer serverObs.Close()

	// served agent 裸接 AgentHooks（不调 CleanupSession，遵守新 godoc——executor 接管）
	svcAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("svc_agent_cleanup"),
		agent_v2.WithModel(&traceFixedModel{answer: "done"}),
		agent_v2.WithHooks(serverObs.AgentHooks()),
	)
	lnSvc, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen svc: %v", err)
	}
	baseURL := "http://" + lnSvc.Addr().String()
	httpSrv := &http.Server{Handler: server.Handler(svcAgent, baseURL, server.WithExecutorObservability(serverObs), testOwnerOpt())}
	go func() { _ = httpSrv.Serve(lnSvc) }()
	defer func() { _ = httpSrv.Close() }()

	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, baseURL)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	// 跑两个独立 task——验证 server 端 session 不累积（每 task executor 清各自 session）
	for i := 0; i < 2; i++ {
		if _, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("ra.Run #%d: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond) // 等 server 端 conclude defer 跑完（srvSpan.End → cleanupSession）

	if n := serverObs.ActiveSessions(); n != 0 {
		t.Errorf("executor 应在 conclude 自动清 a2a server session（business 不调 CleanupSession），残留 %d 个（leak）", n)
	}
	// a2a_server span 仍正常 export（清理在 srvSpan.End 之后，LIFO defer 保证）
	var serverSpanSeen bool
	for _, s := range cap.all() {
		if s.Name == "gen_ai.a2a_server.message_stream" {
			serverSpanSeen = true
		}
	}
	if !serverSpanSeen {
		t.Error("a2a_server span 应正常 export（LIFO defer：srvSpan.End 先于 CleanupSession）")
	}
}
