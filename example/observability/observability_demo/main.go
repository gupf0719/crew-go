// observability_demo 演示 crew-go framework 的 observability 集成 — 业务侧
// 把 obs.AgentHooks() + obs.OrchHooks() 接入 BasicAgent 后, 框架内置 metric
// (LLM tokens / call duration / tool duration / agent rounds / node execution
// / runs in progress / agent run total / active sessions / fanout / skill /
// compact / cp 等) 自动 fire 到 OTLP MetricsService.
//
// **demo 设计**:
//   - in-process mock OTLP server (tcp 127.0.0.1:0 随机端口) 收 trace + metric
//   - obs 配 mock addr (TraceEndpoint + MetricsEndpoint 同 addr 复用)
//   - 用 mock ChatModel 跑 BasicAgent 3 sessions (不依赖真 LLM key)
//   - 每 session 跑 2 轮 ReAct (1 tool_call + 1 final answer)
//   - obs.Close() 触发 flush
//   - 打印 mock server 收到的 trace span 数 + 各 metric 名称/值/labels
//
// **跑法**:
//
//	go run ./example/observability/observability_demo
//
// **真业务接入** (替换 mock):
//
//	obs := observability.New(Config{
//	    TraceEndpoint:   "collector:4317",
//	    MetricsEndpoint: "collector:4318",
//	    Resource: Resource{AppName: "my-app", EnvName: "prod", ...},
//	})
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithName("researcher"),
//	    agent_v2.WithModel(deepseek.NewClient(apiKey, "")),
//	    agent_v2.WithToolkit(tk),
//	    agent_v2.WithHooks(obs.AgentHooks()),    // ★ 业务 hook 接 obs
//	)
//	ctx := agent_v2.WithCtxOrchHooks(context.Background(), obs.OrchHooks()) // ★ orch hook
//	events, _ := ag.Run(ctx, sess, schema.UserMessage("北京天气"))
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"
)

// ─── mock OTLP collector (trace + metric 同 server) ───

type traceCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	parent *mockState
}

func (t *traceCollector) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	// iter-5-R OTLP v1 升级: InstrumentationLibrarySpans → ScopeSpans.
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			t.parent.spanCount.Add(int32(len(ss.GetSpans())))
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type metricsCollector struct {
	colmetricspb.UnimplementedMetricsServiceServer
	parent *mockState
}

func (m *metricsCollector) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	m.parent.mu.Lock()
	defer m.parent.mu.Unlock()
	// iter-5-R OTLP v1 升级: InstrumentationLibraryMetrics → ScopeMetrics.
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			m.parent.metricsRecv = append(m.parent.metricsRecv, sm.GetMetrics()...)
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type mockState struct {
	mu          sync.Mutex
	spanCount   atomic.Int32
	metricsRecv []*metricspb.Metric
}

func startMockCollector() (*mockState, string, func(), error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", nil, err
	}
	srv := grpc.NewServer()
	mc := &mockState{}
	coltracepb.RegisterTraceServiceServer(srv, &traceCollector{parent: mc})
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsCollector{parent: mc})
	go func() { _ = srv.Serve(lis) }()
	return mc, lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}, nil
}

// ─── mock ChatModel (跟 basic_chat/mock.go 同款 pattern) ───

type mockChat struct {
	tools []*schema.ToolInfo
}

func (m *mockChat) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mockChat) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	time.Sleep(20 * time.Millisecond) // 模拟 LLM 耗时让 LLMCallDuration 可观测
	// 倒扫: 本轮 user 之后有 tool_result → 最终回答; 否则 → tool_call
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			return schema.AssistantMessage(fmt.Sprintf("根据查询: %s", msgs[i].Content)), nil
		}
	}
	// tool_call get_weather(Beijing)
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "call_1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "get_weather",
			Arguments: `{"city":"Beijing"}`,
		},
	}}), nil
}

func (m *mockChat) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

var _ model.ChatModel = (*mockChat)(nil)

// ─── 业务 demo flow ───

func main() {
	// -real: 用真 LLM 跑完整 ReAct (实际业务集成验证); 默认 mock (CI 无 key).
	// -provider: 真 LLM provider (deepseek / qwen), 仅 -real 生效 — 跨 provider
	// observability 验证 (验 token metric / trace 在不同 provider 都 work).
	realLLM := flag.Bool("real", false, "用真 LLM 跑 (需 .env 对应 key)")
	provider := flag.String("provider", "deepseek", "真 LLM provider: deepseek (需 DEEPSEEK_API_KEY) / qwen (dashscope, 需 QWEN_API_KEY)")
	flag.Parse()

	// 选 model: -real 真 LLM (按 provider) / 默认 mock
	var chatModel model.ChatModel = &mockChat{}
	if *realLLM {
		envloader.Load()
		switch *provider {
		case "qwen":
			// qwen dashscope 测试配置 (key 从 QWEN_API_KEY 读)
			if os.Getenv("QWEN_API_KEY") == "" {
				log.Fatal("-real -provider qwen 需 .env QWEN_API_KEY")
			}
			chatModel, _ = llmtrace.NewQwenFallback() // 候选取 qwen.TestModelCandidates，额度耗尽自动切
			fmt.Print("→ 真 qwen dashscope LLM 模式 (跨 provider observability 验证)\n\n")
		case "deepseek":
			apiKey := os.Getenv("DEEPSEEK_API_KEY")
			if apiKey == "" {
				log.Fatal("-real -provider deepseek 需 .env DEEPSEEK_API_KEY")
			}
			chatModel = deepseek.NewClient(apiKey, "")
			fmt.Print("→ 真 deepseek LLM 模式 (实际业务集成验证)\n\n")
		default:
			log.Fatalf("未知 -provider %q (支持 deepseek / qwen)", *provider)
		}
	}

	// 1) 启 mock collector
	mc, addr, cleanup, err := startMockCollector()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()
	fmt.Printf("→ mock OTLP collector started @ %s\n\n", addr)

	// 2) 业务侧配 observability
	obs, err := observability.New(observability.Config{
		TraceEndpoint:       addr,
		MetricsEndpoint:     addr,
		TraceExportInterval: 200 * time.Millisecond,
		Resource: observability.Resource{
			AppName:    "observability-demo",
			EnvName:    "demo",
			IP:         "127.0.0.1",
			SDKVersion: "v0.1",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// 3) 业务侧组装 BasicAgent + obs hooks
	weatherTool := tool.NewFuncTool(
		"get_weather",
		"查询城市天气",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city": map[string]any{"type": "string"},
			},
			"required": []string{"city"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			time.Sleep(15 * time.Millisecond) // 模拟工具耗时
			city, _ := args["city"].(string)
			return &tool.Result{
				Success: true,
				Output:  fmt.Sprintf("%s 晴 25°C", city),
			}, nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(weatherTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("demo_agent"),
		agent_v2.WithDescription("天气问询 demo"),
		agent_v2.WithSystemPrompt("用 get_weather 工具查询."),
		agent_v2.WithModel(chatModel), // -real 真 deepseek / 默认 mock
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(obs.AgentHooks()), // ★ 关键: 接 observability hooks
		agent_v2.WithMaxRounds(5),
	)

	// 4) 跑 3 sessions 验 ActiveSessions Gauge + 各 metric
	//
	// **关键接入步骤** (audit Sub-5 MED-3 fix): observability 端 ActiveSessions Gauge
	// 按 ctx 内的 sessionID 区分 — 业务必须 push sess.RunID 到 ctx 让 obs.session(ctx)
	// 拿到唯一 sid. 否则 3 个 NewRunSession() 都映射到 sid="" → Gauge 永远 = 1.
	//
	// 业务真实路径: 一般 Run 入口 framework 已 push sid (compose_v2 内部 withSessionID),
	// 但 demo 直接调 ag.Run() 没经过 framework 全链路 — 需用 WithSessionIDCtx 显式 push.
	fmt.Println("→ running 3 sessions...")
	for i := 0; i < 3; i++ {
		sess := agent_v2.NewRunSession()
		ctx := compose_v2.WithSessionIDCtx(context.Background(), sess.RunID) // ★ 关键: push sid → gen_ai.request.id
		// gap 3 (thread/run 模型): 3 次 Run 同属一个会话 "demo-conversation"
		// → push thread id 让 trace 的 gen_ai.session.id 把它们聚成一个会话 (类比
		// plan_chat_web 推 HTTP session_id). 业务不 push 时 session.id 不 emit.
		ctx = compose_v2.WithThreadIDCtx(ctx, "demo-conversation") // ★ gen_ai.session.id (跨 run 聚类)
		ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())      // orch hook
		events, runErr := ag.Run(ctx, sess, schema.UserMessage("北京天气如何?"))
		if runErr != nil {
			log.Printf("Run %d err: %v", i, runErr)
			continue
		}
		if final := agent_v2.FinalMessage(events); final != nil {
			fmt.Printf("  Run %d final: %s\n", i, final.Content)
		}
		// 业务完成 Run 后调 CleanupSession 防 observability sessions map leak.
		// 长跑服务 (高 QPS 不同 sessionID) 必须配合 sweeper 定期清孤儿.
		obs.CleanupSession(sess.RunID) // ★ 关键: 防 leak
	}

	// 5) Close 触发 flush
	fmt.Println("\n→ closing observability (flush metric/trace)...")
	_ = obs.Close()
	time.Sleep(500 * time.Millisecond) // 等 RPC drain

	// 6) 打印 mock collector 收到的数据
	mc.mu.Lock()
	defer mc.mu.Unlock()

	fmt.Printf("\n═══ Trace 收到 ═══\n")
	fmt.Printf("总 span: %d\n", mc.spanCount.Load())

	fmt.Printf("\n═══ Metric 收到 (按 metric name 排列) ═══\n")
	type entry struct {
		name, kind, details string
	}
	rows := []entry{}
	// iter-5-R OTLP v1 升级: Metric_IntSum/DoubleHistogram/IntGauge → Metric_Sum/Histogram/Gauge
	// (统一 double 类型). DataPoint.GetValue() → GetAsDouble() (oneof getter).
	// DataPoint.GetLabels() → GetAttributes() (StringKeyValue → KeyValue + AnyValue 包装).
	for _, m := range mc.metricsRecv {
		switch data := m.GetData().(type) {
		case *metricspb.Metric_Sum:
			for _, dp := range data.Sum.GetDataPoints() {
				rows = append(rows, entry{m.GetName(), "Counter",
					fmt.Sprintf("value=%.0f %s", dp.GetAsDouble(), formatAttrs(dp.GetAttributes()))})
			}
		case *metricspb.Metric_Histogram:
			for _, dp := range data.Histogram.GetDataPoints() {
				rows = append(rows, entry{m.GetName(), "Histogram",
					fmt.Sprintf("count=%d sum=%.4f %s",
						dp.GetCount(), dp.GetSum(), formatAttrs(dp.GetAttributes()))})
			}
		case *metricspb.Metric_Gauge:
			for _, dp := range data.Gauge.GetDataPoints() {
				rows = append(rows, entry{m.GetName(), "Gauge",
					fmt.Sprintf("value=%.0f %s", dp.GetAsDouble(), formatAttrs(dp.GetAttributes()))})
			}
		}
	}
	for _, r := range rows {
		fmt.Printf("  [%s] %-32s %s\n", r.kind, r.name, r.details)
	}
	fmt.Printf("\n总 metric data point 数: %d\n", len(rows))
	fmt.Printf("\n✓ 业务接 obs.AgentHooks() + obs.OrchHooks() 后 framework 内置 metric 自动 fire (零侵入；本 demo 触发部分，全量见 CLAUDE.md metric 协议).\n")
}

// formatAttrs 解 OTLP v1 KeyValue (含 AnyValue 包装) — 跟 v0.7 StringKeyValue 不同.
func formatAttrs(kvs []*commonpb.KeyValue) string {
	if len(kvs) == 0 {
		return ""
	}
	out := "{"
	for i, kv := range kvs {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s=%s", kv.GetKey(), kv.GetValue().GetStringValue())
	}
	return out + "}"
}
