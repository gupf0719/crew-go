// observability_demo 演示 crew-go 的 OpenTelemetry 集成：应用配好 OTel SDK，observability 把
// agent / 节点 / 模型调用 / 工具调用记成 span，并记 GenAI 语义约定的指标与 crew.* 指标。
//
// 默认不需要 collector：span 进内存里的 SpanRecorder、指标由 ManualReader 读出，跑完打印 span 树与指标。
// 想发给真实 collector 时传 -otlp 127.0.0.1:4317（OTLP gRPC），span 与指标照常导出。
//
//	go run ./example/observability/observability_demo                  # mock 模型，打印 span 树
//	go run ./example/observability/observability_demo -real -provider qwen
//	go run ./example/observability/observability_demo -otlp 127.0.0.1:4317
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"

	"go.opentelemetry.io/otel/attribute"
)

// ─── mock ChatModel：第一轮调 get_weather，拿到结果后给最终答复 ───

type mockChat struct{ tools []*schema.ToolInfo }

func (m *mockChat) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mockChat) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	time.Sleep(20 * time.Millisecond)
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			out := schema.AssistantMessage(fmt.Sprintf("根据查询: %s", msgs[i].Content))
			out.Metadata = &schema.MessageMetadata{Model: "mock-model", System: "mock",
				Usage: &schema.MessageUsage{InputTokens: 40, OutputTokens: 12, TotalTokens: 52}}
			return out, nil
		}
	}
	out := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: "get_weather", Arguments: `{"city":"Beijing"}`},
	}})
	out.Metadata = &schema.MessageMetadata{Model: "mock-model", System: "mock",
		Usage: &schema.MessageUsage{InputTokens: 30, OutputTokens: 8, TotalTokens: 38}}
	return out, nil
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

func main() {
	realLLM := flag.Bool("real", false, "用真 LLM 跑（需 .env 对应 key）")
	provider := flag.String("provider", "qwen", "真 LLM provider：qwen（需 QWEN_API_KEY）/ deepseek（需 DEEPSEEK_API_KEY），仅 -real 生效")
	otlp := flag.String("otlp", "", "OTLP gRPC 地址（如 127.0.0.1:4317）；给了就把 span 也发过去")
	content := flag.Bool("content", false, "采集模型 / 工具的输入输出内容（gen_ai.input.messages 等）")
	flag.Parse()

	var chatModel model.ChatModel = &mockChat{}
	if *realLLM {
		envloader.Load()
		switch *provider {
		case "qwen":
			if os.Getenv("QWEN_API_KEY") == "" {
				log.Fatal("-real -provider qwen 需 .env QWEN_API_KEY")
			}
			chatModel, _ = llmtrace.NewQwenFallback()
		case "deepseek":
			key := os.Getenv("DEEPSEEK_API_KEY")
			if key == "" {
				log.Fatal("-real -provider deepseek 需 .env DEEPSEEK_API_KEY")
			}
			chatModel = deepseek.NewClient(key, "")
		default:
			log.Fatalf("未知 -provider %q（支持 qwen / deepseek）", *provider)
		}
	}

	// 1) 应用配 OTel SDK：span 进内存（顺带可发 OTLP），指标由 ManualReader 读。
	ctx := context.Background()
	recorder := tracetest.NewSpanRecorder()
	res := resource.NewSchemaless(attribute.String("service.name", "observability-demo"))
	topts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res), sdktrace.WithSpanProcessor(recorder)}
	if *otlp != "" {
		exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(*otlp), otlptracegrpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
		}
		topts = append(topts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(topts...)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))

	// 2) observability 接到这两个 provider 上。
	obs := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(mp),
		observability.WithContentRecording(*content))

	weather := tool.NewFuncTool("get_weather", "查询城市天气",
		map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}, "required": []string{"city"}},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			time.Sleep(15 * time.Millisecond)
			city, _ := args["city"].(string)
			return &tool.Result{Success: true, Output: fmt.Sprintf("%s 晴 25°C", city)}, nil
		})
	tk := tool.NewToolkit()
	tk.Register(weather)

	// 3) Install 一次接上两条钩子链。
	runCtx, opts := obs.Install(ctx,
		agent_v2.WithName("demo_agent"),
		agent_v2.WithSystemPrompt("用 get_weather 工具查询。"),
		agent_v2.WithModel(chatModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	ag := agent_v2.NewBasicAgent(opts...)

	fmt.Println("→ running 2 sessions...")
	for i := 0; i < 2; i++ {
		sess := agent_v2.NewRunSession()
		c := compose_v2.WithThreadIDCtx(runCtx, "demo-conversation") // gen_ai.conversation.id：两次 Run 属于同一场对话
		events, err := ag.Run(c, sess, schema.UserMessage("北京天气如何?"))
		if err != nil {
			log.Printf("Run %d err: %v", i, err)
			continue
		}
		if final := agent_v2.FinalMessage(events); final != nil {
			fmt.Printf("  Run %d final: %s\n", i, final.Content)
		}
	}

	// 4) 打印 span 树与指标。
	printSpanTree(recorder.Ended())
	printMetrics(ctx, reader)

	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = tp.Shutdown(sctx)
	_ = mp.Shutdown(sctx)
}

func printSpanTree(spans []sdktrace.ReadOnlySpan) {
	children := map[string][]sdktrace.ReadOnlySpan{}
	ids := map[string]bool{}
	for _, s := range spans {
		ids[s.SpanContext().SpanID().String()] = true
	}
	var roots []sdktrace.ReadOnlySpan
	for _, s := range spans {
		p := s.Parent().SpanID().String()
		if s.Parent().IsValid() && ids[p] {
			children[p] = append(children[p], s)
		} else {
			roots = append(roots, s)
		}
	}
	byStart := func(xs []sdktrace.ReadOnlySpan) {
		sort.Slice(xs, func(i, j int) bool { return xs[i].StartTime().Before(xs[j].StartTime()) })
	}
	var walk func(s sdktrace.ReadOnlySpan, depth int)
	walk = func(s sdktrace.ReadOnlySpan, depth int) {
		var keys []string
		for _, a := range s.Attributes() {
			switch a.Key {
			case observability.AttrOperationName, observability.AttrOutcome, observability.AttrResponseModel,
				observability.AttrUsageInputTokens, observability.AttrUsageOutputTokens, observability.AttrToolCallID:
				keys = append(keys, fmt.Sprintf("%s=%s", a.Key, a.Value.Emit()))
			}
		}
		fmt.Printf("%s%s [%s] %s\n", strings.Repeat("  ", depth), s.Name(), s.SpanKind(), strings.Join(keys, " "))
		kids := children[s.SpanContext().SpanID().String()]
		byStart(kids)
		for _, k := range kids {
			walk(k, depth+1)
		}
	}
	byStart(roots)
	fmt.Printf("\n═══ span 树（%d 个 span）═══\n", len(spans))
	for _, r := range roots {
		walk(r, 0)
	}
}

func printMetrics(ctx context.Context, reader *sdkmetric.ManualReader) {
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		log.Printf("collect metrics: %v", err)
		return
	}
	fmt.Println("\n═══ 指标 ═══")
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					fmt.Printf("  %-46s sum=%d %s\n", m.Name, dp.Value, dp.Attributes.Encoded(attribute.DefaultEncoder()))
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					fmt.Printf("  %-46s count=%d sum=%.3f %s\n", m.Name, dp.Count, dp.Sum, dp.Attributes.Encoded(attribute.DefaultEncoder()))
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					fmt.Printf("  %-46s count=%d sum=%d %s\n", m.Name, dp.Count, dp.Sum, dp.Attributes.Encoded(attribute.DefaultEncoder()))
				}
			}
		}
	}
}
