package observability_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"
)

// ─── 测试夹具 ───

// harness 是一套接在内存 SDK 上的 Observability：span 进 SpanRecorder，指标由 ManualReader 读。
type harness struct {
	obs    *observability.Observability
	rec    *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
}

func newHarness(t *testing.T, opts ...observability.Option) *harness {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})
	all := append([]observability.Option{observability.WithTracerProvider(tp), observability.WithMeterProvider(mp)}, opts...)
	return &harness{obs: observability.New(all...), rec: rec, reader: reader}
}

func (h *harness) spans() []sdktrace.ReadOnlySpan { return h.rec.Ended() }

// span 找唯一一个指定名字的 span，找不到或多于一个都算失败。
func (h *harness) span(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, s := range h.spans() {
		if s.Name() == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("应有且只有一个 %q span，找到 %d 个；全部 span：%v", name, len(found), names(h.spans()))
	}
	return found[0]
}

func (h *harness) spansNamed(name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range h.spans() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// assertAllEnded：开过的 span 全部结束了（错误 / 中断路径上没有漏收尾的）。
func (h *harness) assertAllEnded(t *testing.T) {
	t.Helper()
	if st, en := len(h.rec.Started()), len(h.rec.Ended()); st != en {
		var open []string
		ended := map[trace.SpanID]bool{}
		for _, s := range h.rec.Ended() {
			ended[s.SpanContext().SpanID()] = true
		}
		for _, s := range h.rec.Started() {
			if !ended[s.SpanContext().SpanID()] {
				open = append(open, s.Name())
			}
		}
		t.Fatalf("开了 %d 个 span、只结束了 %d 个，没收尾的：%v", st, en, open)
	}
}

func names(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}

func attrOf(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, a := range s.Attributes() {
		if a.Key == key {
			return a.Value, true
		}
	}
	return attribute.Value{}, false
}

func strAttr(t *testing.T, s sdktrace.ReadOnlySpan, key attribute.Key) string {
	t.Helper()
	v, ok := attrOf(s, key)
	if !ok {
		t.Fatalf("%s 缺属性 %s；有的：%v", s.Name(), key, s.Attributes())
	}
	return v.Emit()
}

func hasEvent(s sdktrace.ReadOnlySpan, name string) (sdktrace.Event, bool) {
	for _, e := range s.Events() {
		if e.Name == name {
			return e, true
		}
	}
	return sdktrace.Event{}, false
}

func (h *harness) collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func matches(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.Emit() != v {
			return false
		}
	}
	return true
}

// sumInt 累加整型计数器（含 UpDownCounter）里属性匹配 want 的数据点。
func sumInt(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) int64 {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		return 0
	}
	d, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s 不是整型 Sum：%T", name, m.Data)
	}
	var n int64
	for _, dp := range d.DataPoints {
		if matches(dp.Attributes, want) {
			n += dp.Value
		}
	}
	return n
}

// histCount 数直方图里属性匹配 want 的样本数。
func histCount(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) uint64 {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		return 0
	}
	var n uint64
	switch d := m.Data.(type) {
	case metricdata.Histogram[float64]:
		for _, dp := range d.DataPoints {
			if matches(dp.Attributes, want) {
				n += dp.Count
			}
		}
	case metricdata.Histogram[int64]:
		for _, dp := range d.DataPoints {
			if matches(dp.Attributes, want) {
				n += dp.Count
			}
		}
	default:
		t.Fatalf("%s 不是直方图：%T", name, m.Data)
	}
	return n
}

// scriptModel 按「最后一条是不是工具结果」走两步：先发一个 tool_call，拿到结果后给最终答复。
// fail 非 nil 时直接返错；block 为 true 时一直等到 ctx 结束。
type scriptModel struct {
	toolName string
	toolArgs string
	fail     error
	block    bool
	calls    atomic.Int32
}

func (m *scriptModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *scriptModel) Generate(ctx context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if m.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if m.fail != nil {
		return nil, m.fail
	}
	last := msgs[len(msgs)-1]
	if last.Role == schema.RoleTool || m.toolName == "" {
		out := schema.AssistantMessage("最终答复：" + last.Content)
		out.Metadata = &schema.MessageMetadata{Model: "mock-model", System: "mock", FinishReason: "stop",
			Usage: &schema.MessageUsage{InputTokens: 40, OutputTokens: 12, TotalTokens: 52}}
		return out, nil
	}
	out := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: fmt.Sprintf("call_%d", m.calls.Load()), Type: "function",
		Function: schema.FunctionCall{Name: m.toolName, Arguments: m.toolArgs},
	}})
	out.Metadata = &schema.MessageMetadata{Model: "mock-model", System: "mock", FinishReason: "tool_calls",
		Usage: &schema.MessageUsage{InputTokens: 30, OutputTokens: 8, TotalTokens: 38}}
	return out, nil
}

func (m *scriptModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(resp), nil
}

func weatherToolkit(result func(city string) *tool.Result) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("get_weather", "查询城市天气",
		map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			city, _ := args["city"].(string)
			return result(city), nil
		}))
	return tk
}

func sunny(city string) *tool.Result {
	return &tool.Result{Success: true, Output: city + " 晴 25°C"}
}

// runWeatherAgent 用 Install 装好两条钩子链，跑一次「查天气」ReAct。
func runWeatherAgent(t *testing.T, h *harness, m model.ChatModel, tk *tool.Toolkit, input string) ([]*agent_v2.AgentEvent, error) {
	t.Helper()
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("weather_agent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4),
	)
	ag := agent_v2.NewBasicAgent(opts...)
	sess := agent_v2.NewRunSession()
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv-1")
	return ag.Run(ctx, sess, schema.UserMessage(input))
}

// ─── span 树与属性 ───

// TestSpanTree_ReActRun：一次带工具调用的 ReAct 跑出 invoke_agent → 节点 → chat / execute_tool 的
// span 树，全程一个 trace，身份与用量属性按 GenAI 约定填。
func TestSpanTree_ReActRun(t *testing.T) {
	h := newHarness(t)
	m := &scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}
	if _, err := runWeatherAgent(t, h, m, weatherToolkit(sunny), "北京天气如何"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h.assertAllEnded(t)

	agent := h.span(t, "invoke_agent weather_agent")
	tools := h.span(t, "execute_tool get_weather")
	chats := h.spansNamed("chat mock-model")
	if len(chats) != 2 {
		t.Fatalf("两轮模型调用应有 2 个 chat span，got %d；全部：%v", len(chats), names(h.spans()))
	}

	if agent.Parent().IsValid() {
		t.Errorf("agent span 应是根，got parent %s", agent.Parent().SpanID())
	}
	tid := agent.SpanContext().TraceID()
	for _, s := range h.spans() {
		if s.SpanContext().TraceID() != tid {
			t.Errorf("%s 不在 agent 的 trace 里", s.Name())
		}
	}
	// 模型 / 工具 span 挂在各自的节点 span 下，节点挂在 agent 下。
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range h.spans() {
		byID[s.SpanContext().SpanID()] = s
	}
	parentName := func(s sdktrace.ReadOnlySpan) string {
		if p, ok := byID[s.Parent().SpanID()]; ok {
			return p.Name()
		}
		return "<none>"
	}
	for _, c := range chats {
		if got := parentName(c); got != "node model" {
			t.Errorf("chat span 的父应是 node model，got %s", got)
		}
	}
	if got := parentName(tools); got != "node tools" {
		t.Errorf("execute_tool span 的父应是 node tools，got %s", got)
	}
	if got := parentName(h.spansNamed("node model")[0]); got != "invoke_agent weather_agent" {
		t.Errorf("node span 的父应是 agent span，got %s", got)
	}

	if s := strAttr(t, agent, observability.AttrOperationName); s != "invoke_agent" {
		t.Errorf("agent operation.name=%s", s)
	}
	if s := strAttr(t, agent, observability.AttrConversationID); s != "conv-1" {
		t.Errorf("agent conversation.id=%s 应=conv-1", s)
	}
	if _, ok := attrOf(agent, observability.AttrRunID); !ok {
		t.Error("agent span 应带 crew.run.id")
	}
	if s := strAttr(t, agent, "crew.agent.inference_calls"); s != "2" {
		t.Errorf("agent 下模型调用次数=%s 应=2", s)
	}
	if s := strAttr(t, agent, "crew.agent.tool_calls"); s != "1" {
		t.Errorf("agent 下工具调用次数=%s 应=1", s)
	}

	first := chats[0]
	if chats[1].StartTime().Before(first.StartTime()) {
		first = chats[1]
	}
	if first.SpanKind() != trace.SpanKindClient {
		t.Errorf("chat span 应是 CLIENT，got %s", first.SpanKind())
	}
	for key, want := range map[attribute.Key]string{
		observability.AttrOperationName:         "chat",
		observability.AttrProviderName:          "mock",
		observability.AttrResponseModel:         "mock-model",
		observability.AttrUsageInputTokens:      "30",
		observability.AttrUsageOutputTokens:     "8",
		observability.AttrResponseFinishReasons: `["tool_calls"]`,
		observability.AttrOutcome:               "success",
	} {
		if got := strAttr(t, first, key); got != want {
			t.Errorf("首轮 chat span %s=%s 应=%s", key, got, want)
		}
	}
	if s := strAttr(t, tools, observability.AttrToolCallID); s != "call_1" {
		t.Errorf("tool call.id=%s 应=call_1", s)
	}
	if s := strAttr(t, tools, observability.AttrToolName); s != "get_weather" {
		t.Errorf("tool name=%s", s)
	}
}

// aliasModel 报请求用的模型别名（mock-alias），响应里带回的是具体版本（scriptModel 写的 mock-model）。
type aliasModel struct{ *scriptModel }

func (aliasModel) ModelName() string { return "mock-alias" }

// TestRequestModel_FromClient：客户端报了模型名时，chat span 按请求模型命名、记 gen_ai.request.model，响应模型另记；
// 推理与 agent 指标都带请求模型维度。
func TestRequestModel_FromClient(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("req"), agent_v2.WithModel(aliasModel{&scriptModel{}}))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	chat := h.span(t, "chat mock-alias")
	if s := strAttr(t, chat, observability.AttrRequestModel); s != "mock-alias" {
		t.Errorf("gen_ai.request.model=%s 应=mock-alias", s)
	}
	if s := strAttr(t, chat, observability.AttrResponseModel); s != "mock-model" {
		t.Errorf("gen_ai.response.model=%s 应=mock-model", s)
	}
	rm := h.collect(t)
	if n := histCount(t, rm, observability.MetricInferenceDuration, map[string]string{
		"gen_ai.request.model": "mock-alias", "gen_ai.response.model": "mock-model"}); n != 1 {
		t.Errorf("推理时长应带请求与响应两个模型维度，got %d", n)
	}
	if n := histCount(t, rm, observability.MetricInvokeAgentDuration, map[string]string{"gen_ai.request.model": "mock-alias"}); n != 1 {
		t.Errorf("agent 时长应带请求模型维度，got %d", n)
	}
}

// TestMetrics_ReActRun：同一次 ReAct 产出 GenAI 约定的推理 / token / 工具 / agent 指标与 crew.* 指标。
func TestMetrics_ReActRun(t *testing.T) {
	h := newHarness(t)
	m := &scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}
	if _, err := runWeatherAgent(t, h, m, weatherToolkit(sunny), "北京天气如何"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rm := h.collect(t)
	text := map[string]string{"gen_ai.token.modality": "text", "gen_ai.provider.name": "mock"}
	if n := sumInt(t, rm, observability.MetricUsageInputTokens, text); n != 70 {
		t.Errorf("输入 token 合计=%d 应=70（30+40）", n)
	}
	if n := sumInt(t, rm, observability.MetricUsageOutputTokens, text); n != 20 {
		t.Errorf("输出 token 合计=%d 应=20（8+12）", n)
	}
	if n := histCount(t, rm, observability.MetricInferenceDuration, map[string]string{"gen_ai.operation.name": "chat"}); n != 2 {
		t.Errorf("推理耗时样本=%d 应=2", n)
	}
	if n := histCount(t, rm, observability.MetricExecuteToolDuration, map[string]string{"gen_ai.tool.name": "get_weather"}); n != 1 {
		t.Errorf("工具耗时样本=%d 应=1", n)
	}
	if n := histCount(t, rm, observability.MetricInvokeAgentDuration, map[string]string{"gen_ai.agent.name": "weather_agent"}); n != 1 {
		t.Errorf("agent 耗时样本=%d 应=1", n)
	}
	if n := sumInt(t, rm, observability.MetricAgentInvocations, map[string]string{"crew.outcome": "success"}); n != 1 {
		t.Errorf("成功的 agent 调用=%d 应=1", n)
	}
	if n := sumInt(t, rm, observability.MetricAgentActive, nil); n != 0 {
		t.Errorf("跑完之后在途 agent 数=%d 应=0", n)
	}
	if n := sumInt(t, rm, observability.MetricNodeExecutions, map[string]string{"crew.node.id": "model"}); n != 2 {
		t.Errorf("model 节点执行次数=%d 应=2", n)
	}
}

// TestTokenUsage_GenAISemantics：token 按 GenAI 约定的口径记——输出 token 包含推理 token（MessageUsage.OutputTokens 本来
// 就含，ThinkTokens 是其中那部分，不另外相加），推理 / 缓存读 / 缓存写各自是输出或输入的子集。
func TestTokenUsage_GenAISemantics(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "a", Type: "base"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	mctx, _ := ah.OnModelStart(ctx, info, &schema.ModelInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	out := schema.AssistantMessage("ok")
	out.Metadata = &schema.MessageMetadata{Model: "claude-x", System: "anthropic"}
	ah.OnModelEnd(mctx, info, &schema.ModelOutput{Message: out, Usage: &schema.MessageUsage{
		InputTokens: 100, CachedInputTokens: 60, CacheCreationInputTokens: 30, OutputTokens: 25, ThinkTokens: 5, TotalTokens: 125}})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	chat := h.span(t, "chat claude-x")
	for key, want := range map[attribute.Key]string{
		observability.AttrUsageInputTokens:      "100",
		observability.AttrUsageOutputTokens:     "25",
		observability.AttrUsageReasoningTokens:  "5",
		observability.AttrUsageCacheReadTokens:  "60",
		observability.AttrUsageCacheWriteTokens: "30",
	} {
		if got := strAttr(t, chat, key); got != want {
			t.Errorf("%s=%s 应=%s", key, got, want)
		}
	}
	rm := h.collect(t)
	for name, want := range map[string]int64{
		observability.MetricUsageInputTokens:      100,
		observability.MetricUsageOutputTokens:     25,
		observability.MetricUsageReasoningTokens:  5,
		observability.MetricUsageCacheReadTokens:  60,
		observability.MetricUsageCacheWriteTokens: 30,
	} {
		if got := sumInt(t, rm, name, map[string]string{"gen_ai.provider.name": "anthropic"}); got != want {
			t.Errorf("%s=%d 应=%d", name, got, want)
		}
	}
}

// ─── 失败、中断、超时 ───

// TestModelError_ChatSpanEndsWithError：模型报错时框架不调 OnModelEnd，chat span 由 OnError 以失败收尾，
// agent span 也是失败；没有 span 漏收尾。
func TestModelError_ChatSpanEndsWithError(t *testing.T) {
	h := newHarness(t)
	m := &scriptModel{fail: errors.New("upstream 503")}
	if _, err := runWeatherAgent(t, h, m, weatherToolkit(sunny), "hi"); err == nil {
		t.Fatal("模型报错时 Run 应返错")
	}
	h.assertAllEnded(t)
	chats := h.spansNamed("chat")
	if len(chats) == 0 {
		t.Fatalf("应有 chat span；全部：%v", names(h.spans()))
	}
	for _, c := range chats {
		if c.Status().Code != codes.Error {
			t.Errorf("出错的 chat span 状态应是 Error，got %v", c.Status())
		}
		if s := strAttr(t, c, observability.AttrOutcome); s != "error" {
			t.Errorf("chat span crew.outcome=%s 应=error", s)
		}
	}
	agent := h.span(t, "invoke_agent weather_agent")
	if agent.Status().Code != codes.Error || strAttr(t, agent, observability.AttrOutcome) != "error" {
		t.Errorf("agent span 应以失败收尾，status=%v outcome=%s", agent.Status(), strAttr(t, agent, observability.AttrOutcome))
	}
	if _, ok := attrOf(agent, observability.AttrErrorType); !ok {
		t.Error("失败的 agent span 应带 error.type")
	}
}

// TestToolFailure_ErrorType：工具返回业务失败（Success=false）时工具 span 以失败结束，error.type 取工具给的错误码。
func TestToolFailure_ErrorType(t *testing.T) {
	h := newHarness(t)
	m := &scriptModel{toolName: "get_weather", toolArgs: `{"city":"火星"}`}
	tk := weatherToolkit(func(string) *tool.Result {
		return tool.Failure("city_not_found", "没有这个城市")
	})
	if _, err := runWeatherAgent(t, h, m, tk, "火星天气"); err != nil {
		t.Fatalf("工具失败是业务失败，Run 不应返错：%v", err)
	}
	h.assertAllEnded(t)
	ts := h.span(t, "execute_tool get_weather")
	if ts.Status().Code != codes.Error {
		t.Errorf("失败工具的 span 状态应是 Error，got %v", ts.Status())
	}
	if s := strAttr(t, ts, observability.AttrErrorType); s != "city_not_found" {
		t.Errorf("error.type=%s 应=city_not_found", s)
	}
	if n := histCount(t, h.collect(t), observability.MetricExecuteToolDuration, map[string]string{"error.type": "city_not_found"}); n != 1 {
		t.Errorf("失败工具的耗时样本应带 error.type，got %d", n)
	}
}

// TestTimeout_OutcomeTimeout：ctx 到期时 agent / chat span 记 timeout（失败），而不是笼统的 error。
func TestTimeout_OutcomeTimeout(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("slow_agent"), agent_v2.WithModel(&scriptModel{block: true}))
	ag := agent_v2.NewBasicAgent(opts...)
	ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("超时应返错")
	}
	h.assertAllEnded(t)
	agent := h.span(t, "invoke_agent slow_agent")
	if s := strAttr(t, agent, observability.AttrOutcome); s != "timeout" {
		t.Errorf("agent crew.outcome=%s 应=timeout", s)
	}
	if agent.Status().Code != codes.Error {
		t.Errorf("超时算失败，status 应是 Error，got %v", agent.Status())
	}
}

// TestCancel_NotAnError：调用方取消不是失败——crew.outcome=cancelled、span 状态不是 Error。
func TestCancel_NotAnError(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("cancel_agent"), agent_v2.WithModel(&scriptModel{block: true}))
	ag := agent_v2.NewBasicAgent(opts...)
	ctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("取消应返错")
	}
	h.assertAllEnded(t)
	agent := h.span(t, "invoke_agent cancel_agent")
	if s := strAttr(t, agent, observability.AttrOutcome); s != "cancelled" {
		t.Errorf("agent crew.outcome=%s 应=cancelled", s)
	}
	if agent.Status().Code == codes.Error {
		t.Errorf("取消不该标成 Error，got %v", agent.Status())
	}
}

// TestInterruptResume：ask_user 中断时 agent span 记 blocked（不是失败）并带 crew.interrupt 事件；
// 续跑时新的 agent span 带 crew.resume 事件与等待时长，等待时长与续跑次数进指标，挂起记录被取走。
func TestInterruptResume(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()),
		agent_v2.WithName("asker"),
		agent_v2.WithModel(&scriptModel{toolName: agent_v2.AskUserToolName, toolArgs: `{"question":"要查哪个城市?"}`}),
		agent_v2.WithAskUser(),
	)
	ag := agent_v2.NewBasicAgent(opts...)
	sess := agent_v2.NewRunSession()
	runCtx := compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	_, err := ag.Run(runCtx, sess, schema.UserMessage("查天气"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在 ask_user 中断，got %v", err)
	}
	h.assertAllEnded(t)
	first := h.span(t, "invoke_agent asker")
	if s := strAttr(t, first, observability.AttrOutcome); s != "blocked" {
		t.Errorf("中断的 agent span crew.outcome=%s 应=blocked", s)
	}
	if first.Status().Code == codes.Error {
		t.Errorf("中断不是失败，status 不该是 Error，got %v", first.Status())
	}
	ev, ok := hasEvent(first, "crew.interrupt")
	if !ok {
		t.Fatal("中断的 agent span 应有 crew.interrupt 事件")
	}
	if !strings.Contains(fmt.Sprint(ev.Attributes), ie.Query.InterruptID) {
		t.Errorf("crew.interrupt 事件应带 InterruptID %s，got %v", ie.Query.InterruptID, ev.Attributes)
	}
	if n := h.obs.PendingInterrupts(); n != 1 {
		t.Errorf("中断后应留 1 条挂起记录，got %d", n)
	}
	// 抛出中断的那次工具调用等不到 OnToolEnd，由 agent 收尾：它是被挂起的，不是失败。
	askSpan := h.span(t, "execute_tool ask_user")
	if s := strAttr(t, askSpan, observability.AttrOutcome); s != "blocked" || askSpan.Status().Code == codes.Error {
		t.Errorf("挂起的 ask_user span 应记 blocked 且不标 Error，got outcome=%s status=%v", s, askSpan.Status())
	}

	time.Sleep(20 * time.Millisecond)
	if _, err := ag.Resume(runCtx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("北京")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	h.assertAllEnded(t)
	agents := h.spansNamed("invoke_agent asker")
	if len(agents) != 2 {
		t.Fatalf("Run + Resume 应各一个 agent span，got %d", len(agents))
	}
	second := agents[1]
	if second.StartTime().Before(agents[0].StartTime()) {
		second = agents[0]
	}
	rev, ok := hasEvent(second, "crew.resume")
	if !ok {
		t.Fatal("续跑的 agent span 应有 crew.resume 事件")
	}
	var wait float64
	for _, a := range rev.Attributes {
		if a.Key == "crew.interrupt.wait_seconds" {
			wait = a.Value.AsFloat64()
		}
	}
	if wait < 0.02 {
		t.Errorf("续跑事件的等待时长=%.3fs，应 ≥ 中断到续跑之间睡的 20ms", wait)
	}
	if s := strAttr(t, second, observability.AttrOutcome); s != "success" {
		t.Errorf("续跑完成的 agent span crew.outcome=%s 应=success", s)
	}
	if n := h.obs.PendingInterrupts(); n != 0 {
		t.Errorf("续跑之后挂起记录应被取走，残留 %d", n)
	}
	rm := h.collect(t)
	if n := sumInt(t, rm, observability.MetricInterrupts, map[string]string{"gen_ai.agent.name": "asker"}); n != 1 {
		t.Errorf("crew.interrupts=%d 应=1", n)
	}
	if n := sumInt(t, rm, observability.MetricResumes, map[string]string{"gen_ai.agent.name": "asker"}); n != 1 {
		t.Errorf("crew.resumes=%d 应=1", n)
	}
	if n := histCount(t, rm, observability.MetricInterruptWait, map[string]string{"gen_ai.agent.name": "asker"}); n != 1 {
		t.Errorf("等待时长样本=%d 应=1", n)
	}
	if n := sumInt(t, rm, observability.MetricAgentInvocations, map[string]string{"crew.outcome": "blocked"}); n != 1 {
		t.Errorf("blocked 的 agent 调用=%d 应=1", n)
	}
}

// TestCleanupSession_DropsPendingRecord：放弃挂起的任务时 CleanupSession 丢掉它的挂起记录，别的会话的不动。
func TestCleanupSession_DropsPendingRecord(t *testing.T) {
	h := newHarness(t)
	cp := compose_v2.NewInMemoryCheckpointer()
	run := func() string {
		ctx, opts := h.obs.Install(compose_v2.WithCheckpointerCtx(context.Background(), cp),
			agent_v2.WithName("asker"),
			agent_v2.WithModel(&scriptModel{toolName: agent_v2.AskUserToolName, toolArgs: `{"question":"?"}`}),
			agent_v2.WithAskUser(),
		)
		sess := agent_v2.NewRunSession()
		_, err := agent_v2.NewBasicAgent(opts...).Run(compose_v2.WithSessionIDCtx(ctx, sess.RunID), sess, schema.UserMessage("x"))
		if _, ok := compose_v2.IsInterrupt(err); !ok {
			t.Fatalf("应中断，got %v", err)
		}
		return sess.RunID
	}
	a, _ := run(), run()
	if n := h.obs.PendingInterrupts(); n != 2 {
		t.Fatalf("两个会话各挂起一次，应有 2 条记录，got %d", n)
	}
	h.obs.CleanupSession(a)
	if n := h.obs.PendingInterrupts(); n != 1 {
		t.Errorf("清掉一个会话之后应剩 1 条（另一个会话的不动），got %d", n)
	}
}

// ─── 内容采集 ───

// TestContent_OffByDefault：默认不采集输入输出内容——span 上没有任何内容属性。
func TestContent_OffByDefault(t *testing.T) {
	h := newHarness(t)
	m := &scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}
	if _, err := runWeatherAgent(t, h, m, weatherToolkit(sunny), "北京天气如何"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, s := range h.spans() {
		for _, k := range []attribute.Key{observability.AttrInputMessages, observability.AttrOutputMessages,
			observability.AttrToolCallArguments, observability.AttrToolCallResult, observability.AttrSystemInstructions} {
			if _, ok := attrOf(s, k); ok {
				t.Errorf("默认不该采集内容，%s 上有 %s", s.Name(), k)
			}
		}
	}
}

// TestContent_RecordedAndRedacted：打开内容采集时模型 / 工具 / agent 的输入输出按 GenAI 消息格式记下，
// 并经脱敏规则处理。
func TestContent_RecordedAndRedacted(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true),
		observability.WithRedactPatterns(regexp.MustCompile(`1[3-9]\d{9}`)))
	m := &scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}
	if _, err := runWeatherAgent(t, h, m, weatherToolkit(sunny), "我的手机 13812345678，北京天气如何"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	agent := h.span(t, "invoke_agent weather_agent")
	in := strAttr(t, agent, observability.AttrInputMessages)
	if strings.Contains(in, "13812345678") {
		t.Errorf("手机号应被脱敏，got %s", in)
	}
	if !strings.Contains(in, "北京天气如何") || !strings.Contains(in, `"role":"user"`) {
		t.Errorf("agent 输入应是 GenAI 消息格式且含原文（手机号以外），got %s", in)
	}
	if out := strAttr(t, agent, observability.AttrOutputMessages); !strings.Contains(out, "最终答复") {
		t.Errorf("agent 输出应含最终答复，got %s", out)
	}
	ts := h.span(t, "execute_tool get_weather")
	if args := strAttr(t, ts, observability.AttrToolCallArguments); !strings.Contains(args, "北京") {
		t.Errorf("工具参数应记下，got %s", args)
	}
	if res := strAttr(t, ts, observability.AttrToolCallResult); !strings.Contains(res, "晴") {
		t.Errorf("工具结果应记下，got %s", res)
	}
	for _, c := range h.spansNamed("chat mock-model") {
		if s := strAttr(t, c, observability.AttrInputMessages); strings.Contains(s, "13812345678") {
			t.Errorf("chat 输入里的手机号也应脱敏，got %s", s)
		}
	}
}

// TestContent_Truncated：超过上限的内容被截断，不会把整段大文本塞进 span。
func TestContent_Truncated(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true), observability.WithMaxContentChars(64))
	m := &scriptModel{}
	if _, err := runWeatherAgent(t, h, m, tool.NewToolkit(), strings.Repeat("长", 500)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	in := strAttr(t, h.span(t, "invoke_agent weather_agent"), observability.AttrInputMessages)
	if strings.Count(in, "长") > 64 || !strings.Contains(in, "truncated") {
		t.Errorf("超长内容应截到 64 个字符并标注截断，got %d 个「长」：%.120s", strings.Count(in, "长"), in)
	}
}

// TestMultimodalInput_ModalityAndParts：输入里有图片 / 音频块时，供应商给的输入 token 总数里混着别的模态，
// 记 gen_ai.token.modality=unknown（输出仍是 text）；打开内容采集时这些块按 GenAI 消息格式记成 uri / blob part。
func TestMultimodalInput_ModalityAndParts(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true))
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("vision"), agent_v2.WithModel(&scriptModel{}))
	in := schema.UserMessage("这张图里是什么")
	in.ContentBlocks = []*schema.ContentBlock{
		{Type: string(schema.ContentBlockTypeImageUrl), ImageUrl: &schema.ImageUrlContentBlock{
			Type: schema.ContentBlockTypeImageUrl, ImageUrl: &schema.ImageUrl{Url: "https://example.com/cat.png"}}},
		{Type: string(schema.ContentBlockTypeInputAudio), InputAudio: &schema.InputAudioContentBlock{
			Type: schema.ContentBlockTypeInputAudio, InputAudio: &schema.InputAudio{Format: "wav", Data: "UklGRg=="}}},
	}
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rm := h.collect(t)
	if n := sumInt(t, rm, observability.MetricUsageInputTokens, map[string]string{"gen_ai.token.modality": "unknown"}); n != 40 {
		t.Errorf("含图片 / 音频的输入 token 应记在 modality=unknown 下，got %d", n)
	}
	if n := sumInt(t, rm, observability.MetricUsageInputTokens, map[string]string{"gen_ai.token.modality": "text"}); n != 0 {
		t.Errorf("含图片 / 音频的输入不该记成 text，got %d", n)
	}
	if n := sumInt(t, rm, observability.MetricUsageOutputTokens, map[string]string{"gen_ai.token.modality": "text"}); n != 12 {
		t.Errorf("纯文本输出应记 modality=text，got %d", n)
	}
	chat := h.span(t, "chat mock-model")
	msgs := strAttr(t, chat, observability.AttrInputMessages)
	for _, want := range []string{`"type":"uri"`, `"modality":"image"`, `"uri":"https://example.com/cat.png"`,
		`"type":"blob"`, `"modality":"audio"`, `"mime_type":"audio/wav"`, `"content":"UklGRg=="`} {
		if !strings.Contains(msgs, want) {
			t.Errorf("chat 输入消息应含 %s，got %s", want, msgs)
		}
	}
}

// TestSkillActivation_SpanAndMetric：activate_skill 的工具调用按 GenAI 约定命名为
// execute_tool {工具名} {skill 名}，带 gen_ai.skill.name，并记 crew.skill.activations。
func TestSkillActivation_SpanAndMetric(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "skilled", Type: "base"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	tctx, _ := ah.OnToolStart(ctx, info, &schema.ToolInput{Name: agent_v2.ActivateSkillToolName, CallId: "c1",
		ArgumentsJSON: `{"skill":"pdf","query":"拆表格"}`})
	ah.OnToolEnd(tctx, info, &schema.ToolOutput{CallId: "c1", Result: &schema.ToolCallResult{Success: true, Output: "ok"}})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	h.assertAllEnded(t)

	ts := h.span(t, "execute_tool "+agent_v2.ActivateSkillToolName+" pdf")
	if s := strAttr(t, ts, observability.AttrSkillName); s != "pdf" {
		t.Errorf("gen_ai.skill.name=%s 应=pdf", s)
	}
	if s := strAttr(t, ts, observability.AttrToolCategory); s != "skill" {
		t.Errorf("crew.tool.category=%s 应=skill", s)
	}
	if n := sumInt(t, h.collect(t), observability.MetricSkillActivations, map[string]string{"gen_ai.skill.name": "pdf"}); n != 1 {
		t.Errorf("crew.skill.activations=%d 应=1", n)
	}
}

// ─── 装配 ───

// recordingHooks 只数 OnAgentStart / OnModelStart 被调了几次。
type recordingHooks struct {
	hook.NoOpHooks
	agentStarts, modelStarts atomic.Int32
}

func (r *recordingHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	r.agentStarts.Add(1)
	return ctx, nil
}

func (r *recordingHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	r.modelStarts.Add(1)
	return ctx, nil
}

// TestInstall_KeepsBusinessHooks：业务自己配了 WithHooks 时 Install 不会把它顶掉，两边都触发；
// 编排那半也接上了（有节点 span）。
func TestInstall_KeepsBusinessHooks(t *testing.T) {
	h := newHarness(t)
	biz := &recordingHooks{}
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("biz_agent"), agent_v2.WithModel(&scriptModel{}), agent_v2.WithHooks(biz))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if biz.agentStarts.Load() != 1 || biz.modelStarts.Load() != 1 {
		t.Errorf("业务钩子应照常触发，agentStarts=%d modelStarts=%d", biz.agentStarts.Load(), biz.modelStarts.Load())
	}
	h.span(t, "invoke_agent biz_agent")
	if len(h.spansNamed("node model")) != 1 {
		t.Errorf("Install 应同时接上编排钩子（node span），全部：%v", names(h.spans()))
	}
}

// TestHalfInstall_WarnsOnce：只接了业务级钩子时，第一次 OnAgentEnd 喊一次，之后不再喊。
func TestHalfInstall_WarnsOnce(t *testing.T) {
	var buf bytes.Buffer
	prevLogger, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() {
		slog.SetDefault(prevLogger)
		// 还原 slog 默认 logger 不会把 log 包的输出改回去，要单独还原。
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})

	h := newHarness(t)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("half"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(h.obs.AgentHooks()))
	for i := 0; i < 2; i++ {
		if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	if n := strings.Count(buf.String(), "编排级钩子没接上"); n != 1 {
		t.Errorf("只接一半时应恰好告警 1 次，got %d；日志：%s", n, buf.String())
	}
}

// TestHalfInstall_NoWarnWhenGraphNeverRan：开始一侧就被拒绝（图没跑）、没有图的远端 agent 正常结束，编排钩子本来
// 就不会跑，不能报「编排级钩子没接上」。
func TestHalfInstall_NoWarnWhenGraphNeverRan(t *testing.T) {
	var buf bytes.Buffer
	prevLogger, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() {
		slog.SetDefault(prevLogger)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})

	h := newHarness(t)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rejected"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(h.obs.AgentHooks(&rejectHooks{agent: errors.New("quota exceeded")})))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("被拒绝时 Run 应返错")
	}
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "remote", Type: "a2a_remote"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	if strings.Contains(buf.String(), "编排级钩子没接上") {
		t.Errorf("图没跑过时不该告警只装一半，日志：%s", buf.String())
	}
}

// TestNodeComponent_RetrievalSpan：GraphAgent 的节点声明了组件类型时，节点 span 按 GenAI 操作名命名。
func TestNodeComponent_RetrievalSpan(t *testing.T) {
	h := newHarness(t)
	in := compose_v2.LastValue[*schema.Message]("in")
	out := compose_v2.LastValue[*schema.Message]("out")
	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, in)
	compose_v2.WithChannel(bld, out)
	bld.AddNode("fetch", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		m, _ := compose_v2.Get(chs, in)
		return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage("docs for "+m.Content))}, nil
	}, compose_v2.WithNodeComponent("retrieval"))
	bld.AddEdge(compose_v2.StartNode, "fetch")
	bld.AddEdge("fetch", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("rag"),
		agent_v2.WithGraphAgentHooks(h.obs.AgentHooks()),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(in, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			m, _ := compose_v2.Get(chs, out)
			return m
		}),
	)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), h.obs.OrchHooks())
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h.assertAllEnded(t)
	node := h.span(t, "retrieval fetch")
	if s := strAttr(t, node, observability.AttrOperationName); s != "retrieval" {
		t.Errorf("retrieval 节点 operation.name=%s", s)
	}
	if s := strAttr(t, node, observability.AttrNodeComponent); s != "retrieval" {
		t.Errorf("crew.node.component=%s", s)
	}
	if len(h.spansNamed("invoke_workflow rag")) != 1 {
		t.Errorf("GraphAgent 应有一个 invoke_workflow span，且外层 agent 已建 span 时图不再重复建；全部：%v", names(h.spans()))
	}
}

// ─── 审计修复的哨兵 ───

// TestNestedGraph_WorkflowNameWithoutInstanceSuffix：子图 / 扇出实例的 ns 带 #N、@N，实例号每次投递都新铸。
// 工作流名只取最后一段的节点 ID，完整 ns 记在 crew.graph.ns——否则 span 名与 gen_ai.workflow.name 指标维度
// 的取值没有上限。
func TestNestedGraph_WorkflowNameWithoutInstanceSuffix(t *testing.T) {
	h := newHarness(t)
	oh := h.obs.OrchHooks()
	for _, ns := range []string{"outer/sub#7", "outer/sub#8", "outer/sub@2"} {
		ctx := compose_v2.WithGraphNS(context.Background(), ns)
		ctx, _ = oh.OnGraphStart(ctx)
		oh.OnGraphEnd(ctx, nil)
	}
	h.assertAllEnded(t)
	spans := h.spansNamed("invoke_workflow sub")
	if len(spans) != 3 {
		t.Fatalf("三次子图执行都应叫 invoke_workflow sub，got %v", names(h.spans()))
	}
	got := map[string]bool{}
	for _, sp := range spans {
		if s := strAttr(t, sp, observability.AttrWorkflowName); s != "sub" {
			t.Errorf("gen_ai.workflow.name=%s 应=sub", s)
		}
		got[strAttr(t, sp, observability.AttrGraphNS)] = true
	}
	if !got["outer/sub#7"] || !got["outer/sub#8"] || !got["outer/sub@2"] {
		t.Errorf("完整 ns 应记在 crew.graph.ns，got %v", got)
	}
	if n := histCount(t, h.collect(t), observability.MetricInvokeWorkflowDuration, map[string]string{"gen_ai.workflow.name": "sub"}); n != 3 {
		t.Errorf("三次执行应落在同一个 gen_ai.workflow.name=sub 维度下，got %d", n)
	}
}

// TestRemoteAgentKind_IsAgentNotWorkflow：a2a.RemoteAgent 触发钩子时 Type=a2a_remote，它是单个 agent，
// 记 invoke_agent 而不是 invoke_workflow。
func TestRemoteAgentKind_IsAgentNotWorkflow(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "remote_svc", Type: "a2a_remote"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	sp := h.span(t, "invoke_agent remote_svc")
	if s := strAttr(t, sp, observability.AttrAgentName); s != "remote_svc" {
		t.Errorf("gen_ai.agent.name=%s", s)
	}
	if n := histCount(t, h.collect(t), observability.MetricInvokeAgentDuration, map[string]string{"gen_ai.agent.name": "remote_svc"}); n != 1 {
		t.Errorf("远端 agent 应记 gen_ai.invoke_agent.duration，got %d", n)
	}
}

// TestErrorMessage_RedactedBeforeTruncated：错误信息先脱敏再截断。反过来的话，跨在截断点上的手机号被截成
// 前几位、正则匹配不上，前几位原样漏进 span。
func TestErrorMessage_RedactedBeforeTruncated(t *testing.T) {
	h := newHarness(t, observability.WithRedactPatterns(regexp.MustCompile(`1[3-9]\d{9}`)))
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "a", Type: "base"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	mctx, _ := ah.OnModelStart(ctx, info, &schema.ModelInput{})
	msg := strings.Repeat("x", 1018) + " 13812345678 trailing"
	ah.OnError(mctx, info, &schema.CommError{Code: "upstream_error", Message: msg})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	chat := h.span(t, "chat")
	ev, ok := hasEvent(chat, "exception")
	if !ok {
		t.Fatal("失败的 chat span 应有 exception 事件")
	}
	all := chat.Status().Description + fmt.Sprint(ev.Attributes)
	if strings.Contains(all, "13812") {
		t.Errorf("跨截断点的手机号前几位漏进了 span：%.80s…", all[len(all)-80:])
	}
}

// TestCompactionEventError_Redacted：压缩事件里的错误串跟别的错误信息一样脱敏。
func TestCompactionEventError_Redacted(t *testing.T) {
	h := newHarness(t, observability.WithRedactPatterns(regexp.MustCompile(`sk-[A-Za-z0-9]+`)))
	oh := h.obs.OrchHooks()
	ctx, _ := oh.OnNodeStart(context.Background(), "model")
	oh.OnNodeEvent(ctx, "model", agent_v2.Event{Type: agent_v2.CompactionEventType, Metadata: map[string]any{
		"action": &agent_v2.CompactionAction{Layer: agent_v2.CompactionLayerAuto, Type: agent_v2.CompactionActionAfter,
			After: &agent_v2.CompactionAfterAction{Err: "summarize failed: key sk-ABCDEF123 rejected"}},
	}})
	oh.OnNodeEnd(ctx, "model", nil)
	ev, ok := hasEvent(h.span(t, "node model"), "crew.compaction")
	if !ok {
		t.Fatal("节点 span 应有 crew.compaction 事件")
	}
	if s := fmt.Sprint(ev.Attributes); strings.Contains(s, "sk-ABCDEF123") || !strings.Contains(s, "[REDACTED]") {
		t.Errorf("crew.compact.error 应脱敏，got %s", s)
	}
}

// TestToolFailure_OutputOnlyWithContentRecording：工具失败但没给错误说明时，工具输出只在打开内容采集时才
// 拿来当错误信息——输出是内容，默认不进 span。
func TestToolFailure_OutputOnlyWithContentRecording(t *testing.T) {
	run := func(h *harness) sdktrace.ReadOnlySpan {
		ah := h.obs.AgentHooks()
		info := &hook.AgentInfo{Name: "a", Type: "base"}
		ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
		tctx, _ := ah.OnToolStart(ctx, info, &schema.ToolInput{Name: "lookup", CallId: "c1"})
		ah.OnToolEnd(tctx, info, &schema.ToolOutput{CallId: "c1",
			Result: &schema.ToolCallResult{Success: false, Output: "客户张三的余额 9999", ErrorCode: "lookup_failed"}})
		ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
		return h.span(t, "execute_tool lookup")
	}
	off := run(newHarness(t))
	if s := off.Status().Description + fmt.Sprint(off.Events()); strings.Contains(s, "张三") {
		t.Errorf("内容采集关着时工具输出不该进 span，got %s", s)
	}
	if s := strAttr(t, off, observability.AttrErrorType); s != "lookup_failed" {
		t.Errorf("error.type=%s 应=lookup_failed", s)
	}
	on := run(newHarness(t, observability.WithContentRecording(true)))
	if s := on.Status().Description; !strings.Contains(s, "张三") {
		t.Errorf("打开内容采集时工具输出可以当错误信息，got %q", s)
	}
}

// TestOnError_HandledErrorNotRecordedOnAgent：OnError 报到 agent / 节点上的错误要是被处理掉了（agent 最后成功），
// agent span 不带 exception 事件、也不算失败（OTel recording-errors：处理掉的错误不记）。
func TestOnError_HandledErrorNotRecordedOnAgent(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "par", Type: "parallel"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	ah.OnError(ctx, info, &schema.CommError{Code: "branch_error", Message: "branch b failed"})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	sp := h.span(t, "invoke_workflow par")
	if _, ok := hasEvent(sp, "exception"); ok {
		t.Error("处理掉的错误不该在 agent span 上留 exception 事件")
	}
	if s := strAttr(t, sp, observability.AttrOutcome); s != "success" || sp.Status().Code == codes.Error {
		t.Errorf("agent 最后成功，outcome=%s status=%v", s, sp.Status())
	}
}

// TestA2AClientSpan_Outcomes：client span 的结局按发请求用的 ctx 判——ctx 取消不是失败、ctx 到期记 timeout；
// ctx 还活着时错误链上的 DeadlineExceeded 也是 timeout，Canceled 不算取消（别处被连带取消，这次调用失败了）；
// 其余失败记 a2a_remote_error，远端错误原文不进 span。
func TestA2AClientSpan_Outcomes(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	live := context.Background()
	cases := []struct {
		name    string
		ctx     context.Context
		err     error
		outcome string
		errType string
		isError bool
	}{
		{"ctx 取消", canceled, fmt.Errorf("stream: %w", context.Canceled), "cancelled", "", false},
		{"ctx 到期", expired, fmt.Errorf("stream: %w", context.DeadlineExceeded), "timeout", "timeout", true},
		{"错误链上的到期", live, fmt.Errorf("http: %w", context.DeadlineExceeded), "timeout", "timeout", true},
		{"错误链上的取消", live, fmt.Errorf("sibling: %w", context.Canceled), "error", "a2a_remote_error", true},
		{"远端失败", live, errors.New("remote said: token sk-SECRET invalid"), "error", "a2a_remote_error", true},
		{"成功", live, nil, "success", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			ctx, cs := h.obs.StartA2AClientSpan(c.ctx, "svc", "message_stream", "jsonrpc", "", "")
			cs.End(ctx, c.err)
			sp := h.span(t, "invoke_agent svc")
			if s := strAttr(t, sp, observability.AttrOutcome); s != c.outcome {
				t.Errorf("crew.outcome=%s 应=%s", s, c.outcome)
			}
			if (sp.Status().Code == codes.Error) != c.isError {
				t.Errorf("status=%v，应 Error=%v", sp.Status(), c.isError)
			}
			if c.errType != "" {
				if s := strAttr(t, sp, observability.AttrErrorType); s != c.errType {
					t.Errorf("error.type=%s 应=%s", s, c.errType)
				}
			}
			if s := sp.Status().Description + fmt.Sprint(sp.Events()); strings.Contains(s, "SECRET") {
				t.Errorf("远端错误原文不该进 span：%s", s)
			}
		})
	}
}

// TestCanceledInErrorChain_IsFailure：调用方 ctx 还活着、结束错误链上却有 context.Canceled 时，这次执行是失败，
// 不能记成 cancelled 把失败藏起来。
func TestCanceledInErrorChain_IsFailure(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "g", Type: "graph"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	ah.OnAgentEnd(agent_v2.WithEndError(ctx, fmt.Errorf("node fetch: %w", context.Canceled)), info, &hook.AgentOutput{})
	sp := h.span(t, "invoke_workflow g")
	if s := strAttr(t, sp, observability.AttrOutcome); s != "error" || sp.Status().Code != codes.Error {
		t.Errorf("ctx 活着时错误链上的取消应记失败，outcome=%s status=%v", s, sp.Status())
	}
}

// ─── 包装层：里面的钩子开始失败时当场收尾 ───

// rejectHooks 在指定的开始钩子里返回 err（模拟鉴权 / 限额拒绝，或 HITL 闸门抛中断）。
type rejectHooks struct {
	hook.NoOpHooks
	agent, model, tool error
}

func (r *rejectHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return ctx, r.agent
}

func (r *rejectHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	return ctx, r.model
}

func (r *rejectHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in != nil && in.Name == "lookup" {
		return ctx, r.tool
	}
	return ctx, nil
}

// TestInstall_BusinessAgentStartRejected：业务钩子在 OnAgentStart 拒绝（返错）时，框架不再调 OnAgentEnd；
// observability 包在外面，当场把刚开的 agent span 以失败收尾、在途数退回 0。
func TestInstall_BusinessAgentStartRejected(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("gated"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(&rejectHooks{agent: errors.New("quota exceeded")}))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("业务钩子拒绝时 Run 应返错")
	}
	h.assertAllEnded(t)
	sp := h.span(t, "invoke_agent gated")
	if s := strAttr(t, sp, observability.AttrOutcome); s != "error" {
		t.Errorf("被拒绝的调用 crew.outcome=%s 应=error", s)
	}
	if n := sumInt(t, h.collect(t), observability.MetricAgentActive, nil); n != 0 {
		t.Errorf("被拒绝之后在途 agent 数=%d 应=0", n)
	}
}

// TestAgentHooks_InnerInterruptOnStart：里面的钩子在 OnAgentStart 抛中断（HITL 闸门）时，agent span 记 blocked、
// 留一条挂起记录（续跑时据此算等待时长）。
func TestAgentHooks_InnerInterruptOnStart(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks(&rejectHooks{agent: &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "gate-1", Question: "放行吗"}}})
	info := &hook.AgentInfo{Name: "member", Type: "base"}
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "sid-1")
	if _, err := ah.OnAgentStart(ctx, info, &hook.AgentInput{}); err == nil {
		t.Fatal("里面的钩子抛中断时 OnAgentStart 应返回它")
	}
	h.assertAllEnded(t)
	sp := h.span(t, "invoke_agent member")
	if s := strAttr(t, sp, observability.AttrOutcome); s != "blocked" || sp.Status().Code == codes.Error {
		t.Errorf("闸门挂起应记 blocked 且不标 Error，outcome=%s status=%v", s, sp.Status())
	}
	if n := h.obs.PendingInterrupts(); n != 1 {
		t.Errorf("应留 1 条挂起记录，got %d", n)
	}
}

// gateOrchHooks 在节点 / 图开始时拒绝。
type gateOrchHooks struct {
	agent_v2.NoOpOrchestrationHooks
	node, graph error
}

func (g *gateOrchHooks) OnNodeStart(ctx context.Context, _ string) (context.Context, error) {
	return ctx, g.node
}

func (g *gateOrchHooks) OnGraphStart(ctx context.Context) (context.Context, error) {
	return ctx, g.graph
}

// TestOrchHooks_InnerNodeGate：里面的编排钩子在 OnNodeStart 抛中断（节点前的 HITL 闸门）时，节点 span 当场以
// blocked 收尾——框架随后拿开始之前的 ctx 调 OnNodeEnd，那里取不到这个 span。
func TestOrchHooks_InnerNodeGate(t *testing.T) {
	h := newHarness(t)
	ie := &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "n-1", Question: "执行这个节点吗"}}
	oh := h.obs.OrchHooks(&gateOrchHooks{node: ie})
	in := compose_v2.WithGraphNS(context.Background(), "g")
	if _, err := oh.OnNodeStart(in, "tools"); err == nil {
		t.Fatal("闸门抛中断时 OnNodeStart 应返回它")
	}
	oh.OnNodeEnd(in, "tools", ie) // 框架的约定：开始失败时用开始之前的 ctx 调结束
	h.assertAllEnded(t)
	sp := h.span(t, "node tools")
	if s := strAttr(t, sp, observability.AttrOutcome); s != "blocked" {
		t.Errorf("节点前的闸门挂起应记 blocked，got %s", s)
	}
}

// TestOrchHooks_InnerGraphStartRejected：里面的编排钩子在 OnGraphStart 拒绝时，工作流 span 当场以失败收尾。
func TestOrchHooks_InnerGraphStartRejected(t *testing.T) {
	h := newHarness(t)
	oh := h.obs.OrchHooks(&gateOrchHooks{graph: errors.New("denied")})
	if _, err := oh.OnGraphStart(context.Background()); err == nil {
		t.Fatal("OnGraphStart 应返回里面钩子的错")
	}
	oh.OnGraphEnd(context.Background(), errors.New("denied"))
	h.assertAllEnded(t)
	if s := strAttr(t, h.span(t, "invoke_workflow root"), observability.AttrOutcome); s != "error" {
		t.Errorf("被拒绝的图 crew.outcome=%s 应=error", s)
	}
}

// TestAgentHooks_InnerModelAndToolStartRejected：里面的钩子拒绝一次模型 / 工具调用时，chat / execute_tool span
// 当场以失败收尾；调用没有发生，不记时长样本、不计入 agent 的调用次数。
func TestAgentHooks_InnerModelAndToolStartRejected(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks(&rejectHooks{model: errors.New("model blocked"), tool: errors.New("tool blocked")})
	info := &hook.AgentInfo{Name: "a", Type: "base"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	if _, err := ah.OnModelStart(ctx, info, &schema.ModelInput{}); err == nil {
		t.Fatal("OnModelStart 应返回里面钩子的错")
	}
	if _, err := ah.OnToolStart(ctx, info, &schema.ToolInput{Name: "lookup", CallId: "c1"}); err == nil {
		t.Fatal("OnToolStart 应返回里面钩子的错")
	}
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	h.assertAllEnded(t)
	if s := strAttr(t, h.span(t, "chat"), observability.AttrErrorType); s != "model_start_error" {
		t.Errorf("被拒绝的模型调用 error.type=%s 应=model_start_error", s)
	}
	if s := strAttr(t, h.span(t, "execute_tool lookup"), observability.AttrErrorType); s != "tool_start_error" {
		t.Errorf("被拒绝的工具调用 error.type=%s 应=tool_start_error", s)
	}
	agent := h.span(t, "invoke_agent a")
	if s := strAttr(t, agent, "crew.agent.inference_calls"); s != "0" {
		t.Errorf("没发生的模型调用不该计数，inference_calls=%s", s)
	}
	if s := strAttr(t, agent, "crew.agent.tool_calls"); s != "0" {
		t.Errorf("没发生的工具调用不该计数，tool_calls=%s", s)
	}
	rm := h.collect(t)
	if n := histCount(t, rm, observability.MetricInferenceDuration, nil); n != 0 {
		t.Errorf("没发生的模型调用不该记推理时长，got %d", n)
	}
	if n := histCount(t, rm, observability.MetricExecuteToolDuration, nil); n != 0 {
		t.Errorf("没发生的工具调用不该记工具时长，got %d", n)
	}
}

// batchModel 第一轮一次发两个 tool_call（lookup_a、lookup_b），拿到结果后给最终答复。
type batchModel struct{}

func (batchModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return batchModel{}, nil }

func (batchModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "a1", Type: "function", Function: schema.FunctionCall{Name: "lookup_a", Arguments: `{}`}},
		{ID: "b1", Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}},
	}), nil
}

func (m batchModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(resp), nil
}

// TestToolStartRejected_BatchSpansEndPromptly：同一批里第 2 个调用的开始钩子拒绝时，框架当场让这批失败、前面的
// 调用不再执行。两个 span 都当场以 tool_start_error 收尾，而不是拖到 agent 结束时按 agent 的结局收。
func TestToolStartRejected_BatchSpansEndPromptly(t *testing.T) {
	h := newHarness(t)
	tk := tool.NewToolkit()
	for _, name := range []string{"lookup_a", "lookup"} {
		tk.Register(tool.NewFuncTool(name, name, map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				return &tool.Result{Success: true, Output: "ok"}, nil
			}))
	}
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("batch"), agent_v2.WithModel(batchModel{}),
		agent_v2.WithToolkit(tk), agent_v2.WithHooks(&rejectHooks{tool: errors.New("lookup forbidden")}))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err == nil {
		t.Fatal("工具开始钩子拒绝时 Run 应返错")
	}
	h.assertAllEnded(t)
	for _, name := range []string{"execute_tool lookup_a", "execute_tool lookup"} {
		sp := h.span(t, name)
		if s := strAttr(t, sp, observability.AttrErrorType); s != "tool_start_error" {
			t.Errorf("%s 应当场以 tool_start_error 收尾，got error.type=%s", name, s)
		}
	}
}

// foreignCtxHooks 在开始钩子里返回 context.Background()（违约，但业务会这么写：缓存 / 丢掉了入参 ctx）。
type foreignCtxHooks struct{ hook.NoOpHooks }

func (foreignCtxHooks) OnAgentStart(context.Context, *hook.AgentInfo, *hook.AgentInput) (context.Context, error) {
	return context.Background(), nil
}

func (foreignCtxHooks) OnModelStart(context.Context, *hook.AgentInfo, *schema.ModelInput) (context.Context, error) {
	return context.Background(), nil
}

// TestInnerForeignCtx_HandlesKept：里面的钩子返回外来 ctx 时，observability 把句柄挂回去：span 照常收尾、在途数
// 回到 0、成功的模型调用记成功。
func TestInnerForeignCtx_HandlesKept(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("fa"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(foreignCtxHooks{}))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h.assertAllEnded(t)
	if s := strAttr(t, h.span(t, "chat mock-model"), observability.AttrOutcome); s != "success" {
		t.Errorf("成功的模型调用应记 success，got %s", s)
	}
	if n := sumInt(t, h.collect(t), observability.MetricAgentActive, nil); n != 0 {
		t.Errorf("在途 agent 数=%d 应=0", n)
	}
}

// TestDoubleInstall_RecordedOnce：同一个 Observability 装了两层（Install 两次 / Install 之外又手动 WithHooks / ctx 上
// 已有它的编排钩子再 Install）时只记一份：一个 agent span、每个节点一个 span、指标不翻倍，全部收尾。
func TestDoubleInstall_RecordedOnce(t *testing.T) {
	cases := map[string]func(h *harness) (context.Context, []agent_v2.Option){
		"Install 两次": func(h *harness) (context.Context, []agent_v2.Option) {
			ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("dup"), agent_v2.WithModel(&scriptModel{}))
			return h.obs.Install(ctx, opts...)
		},
		"手动 WithHooks 加 Install": func(h *harness) (context.Context, []agent_v2.Option) {
			return h.obs.Install(context.Background(), agent_v2.WithName("dup"), agent_v2.WithModel(&scriptModel{}),
				agent_v2.WithHooks(h.obs.AgentHooks()))
		},
		"ctx 上已有编排钩子再 Install": func(h *harness) (context.Context, []agent_v2.Option) {
			ctx := agent_v2.WithCtxOrchHooks(context.Background(), h.obs.OrchHooks())
			return h.obs.Install(ctx, agent_v2.WithName("dup"), agent_v2.WithModel(&scriptModel{}))
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			ctx, opts := setup(h)
			if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			h.assertAllEnded(t)
			h.span(t, "invoke_agent dup")
			h.span(t, "chat mock-model")
			if n := len(h.spansNamed("node model")); n != 1 {
				t.Errorf("model 节点只执行了一次，应只有 1 个 span，got %d", n)
			}
			rm := h.collect(t)
			if n := sumInt(t, rm, observability.MetricNodeExecutions, map[string]string{"crew.node.id": "model"}); n != 1 {
				t.Errorf("crew.node.executions{model}=%d 应=1", n)
			}
			if n := histCount(t, rm, observability.MetricInferenceDuration, nil); n != 1 {
				t.Errorf("推理时长样本=%d 应=1", n)
			}
			if n := sumInt(t, rm, observability.MetricAgentActive, nil); n != 0 {
				t.Errorf("在途 agent 数=%d 应=0", n)
			}
		})
	}
}

// TestDoubleInstall_EventsOnce：同一个 Observability 装了两层时，同一次事件投递只处理一次——片段计数、span 事件、
// 框架指标不翻倍。
func TestDoubleInstall_EventsOnce(t *testing.T) {
	h := newHarness(t)
	oh := h.obs.OrchHooks(h.obs.OrchHooks())
	ctx, _ := oh.OnNodeStart(context.Background(), "tools")
	oh.OnNodeEvent(ctx, "tools", agent_v2.Event{Type: "chunk", Content: "x"})
	oh.OnNodeEvent(ctx, "tools", agent_v2.Event{Type: agent_v2.FanoutEventType,
		Metadata: map[string]any{"action": &agent_v2.FanoutAction{Kind: agent_v2.FanoutKindToolCalls, Branches: 2}}})
	oh.OnNodeEnd(ctx, "tools", nil)
	h.assertAllEnded(t)
	sp := h.span(t, "node tools")
	if s := strAttr(t, sp, observability.AttrStreamChunks); s != "1" {
		t.Errorf("一个片段应只计一次，crew.stream.chunks=%s", s)
	}
	n := 0
	for _, e := range sp.Events() {
		if e.Name == "crew."+agent_v2.FanoutEventType {
			n++
		}
	}
	if n != 1 {
		t.Errorf("一次扇出事件应只记一条 span 事件，got %d", n)
	}
	if c := histCount(t, h.collect(t), observability.MetricFanoutBranches, nil); c != 1 {
		t.Errorf("crew.fanout.branches 样本=%d 应=1", c)
	}
}

// TestTwoInstancesStacked_EachComplete：两个 Observability（各接一个 provider）叠在一起时各记各的，互不收掉对方的 span。
func TestTwoInstancesStacked_EachComplete(t *testing.T) {
	a, b := newHarness(t), newHarness(t)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), a.obs.OrchHooks(b.obs.OrchHooks()))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("both"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(a.obs.AgentHooks(b.obs.AgentHooks())))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for name, h := range map[string]*harness{"外层": a, "里层": b} {
		h.assertAllEnded(t)
		if len(h.spansNamed("invoke_agent both")) != 1 || len(h.spansNamed("node model")) != 1 {
			t.Errorf("%s 实例应各有一份完整的 span，got %v", name, names(h.spans()))
		}
	}
}

// TestNestedAgentStartRejected_ParentToolUntouched：工具里跑的子 agent 开始被拒时，框架拿开始之前的 ctx 报
// agent_start_error——那里最内层是父 agent 的工具 span，不能被收成失败。
func TestNestedAgentStartRejected_ParentToolUntouched(t *testing.T) {
	h := newHarness(t)
	parent := h.obs.AgentHooks()
	child := h.obs.AgentHooks(&rejectHooks{agent: errors.New("quota")})
	pinfo, cinfo := &hook.AgentInfo{Name: "parent", Type: "base"}, &hook.AgentInfo{Name: "child", Type: "base"}
	pctx, _ := parent.OnAgentStart(context.Background(), pinfo, &hook.AgentInput{})
	tctx, _ := parent.OnToolStart(pctx, pinfo, &schema.ToolInput{Name: "delegate", CallId: "d1"})
	childIn := compose_v2.WithGraphNS(tctx, "delegate")
	if _, err := child.OnAgentStart(childIn, cinfo, &hook.AgentInput{}); err == nil {
		t.Fatal("子 agent 开始应被拒")
	}
	child.OnError(childIn, cinfo, &schema.CommError{Code: "agent_start_error", Message: "quota"})
	parent.OnToolEnd(tctx, pinfo, &schema.ToolOutput{CallId: "d1", Result: &schema.ToolCallResult{Success: true, Output: "fallback"}})
	parent.OnAgentEnd(pctx, pinfo, &hook.AgentOutput{})
	h.assertAllEnded(t)
	if s := strAttr(t, h.span(t, "execute_tool delegate"), observability.AttrOutcome); s != "success" {
		t.Errorf("父工具自己兜住了错误、成功结束，got outcome=%s", s)
	}
	if s := strAttr(t, h.span(t, "invoke_agent child"), observability.AttrOutcome); s != "error" {
		t.Errorf("被拒绝的子 agent 应记 error，got %s", s)
	}
}

// rewriteHooks 在 [消费] 字段上做业务改写：OnModelStart 过滤掉输入里的社保号，OnToolEnd 把结果脱敏并改成失败。
type rewriteHooks struct{ hook.NoOpHooks }

func (rewriteHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	filtered := make([]*schema.Message, 0, len(in.Messages))
	for _, m := range in.Messages {
		cp := *m
		cp.Content = strings.ReplaceAll(cp.Content, "123-45-6789", "***")
		filtered = append(filtered, &cp)
	}
	in.Messages = filtered
	return ctx, nil
}

func (rewriteHooks) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	out.Result = &schema.ToolCallResult{Success: false, Output: "redacted", ErrorCode: "policy_blocked", Error: "blocked by policy"}
	return ctx, nil
}

// TestContent_RecordsBusinessRewrite：span 记的是业务钩子改写之后、真正交下去 / 被框架用上的那份——PII 过滤后的输入、
// 改写后的工具结局与结果。
func TestContent_RecordsBusinessRewrite(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true))
	m := &scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("rw"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(weatherToolkit(sunny)), agent_v2.WithHooks(rewriteHooks{}))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("my ssn 123-45-6789, 北京天气")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, c := range h.spansNamed("chat mock-model") {
		if in := strAttr(t, c, observability.AttrInputMessages); strings.Contains(in, "123-45-6789") {
			t.Errorf("chat 输入应是业务过滤之后的，got %s", in)
		}
	}
	ts := h.span(t, "execute_tool get_weather")
	if s := strAttr(t, ts, observability.AttrOutcome); s != "error" {
		t.Errorf("业务把结果改成失败，工具 span 应记 error，got %s", s)
	}
	if s := strAttr(t, ts, observability.AttrErrorType); s != "policy_blocked" {
		t.Errorf("error.type=%s 应=policy_blocked", s)
	}
	if s := strAttr(t, ts, observability.AttrToolCallResult); s != "redacted" {
		t.Errorf("工具结果应是改写之后的，got %s", s)
	}
}

// TestAbort_DropsNestedPendingRecords：放弃续跑只在最外层触发开始 / 结束钩子，里面各层的挂起记录也要一起丢掉，
// 否则 PendingInterrupts 一直多报。
func TestAbort_DropsNestedPendingRecords(t *testing.T) {
	h := newHarness(t)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx, opts := h.obs.Install(compose_v2.WithCheckpointerCtx(context.Background(), cp),
		agent_v2.WithName("asker"),
		agent_v2.WithModel(&scriptModel{toolName: agent_v2.AskUserToolName, toolArgs: `{"question":"?"}`}),
		agent_v2.WithAskUser(),
	)
	asker := agent_v2.NewBasicAgent(opts...)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialHooks(h.obs.AgentHooks())).AddStep("ask", asker)
	sess := agent_v2.NewRunSession()
	runCtx := compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	if _, err := seq.Run(runCtx, sess, schema.UserMessage("x")); err == nil {
		t.Fatal("应停在中断")
	}
	if n := h.obs.PendingInterrupts(); n < 2 {
		t.Fatalf("外层与里层各应留一条挂起记录，got %d", n)
	}
	if _, err := seq.Resume(runCtx, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if n := h.obs.PendingInterrupts(); n != 0 {
		t.Errorf("放弃之后挂起记录应全部丢掉，残留 %d", n)
	}
}

// TestToolArguments_EscapedJSONRedacted：模型常把中文写成 \uXXXX 转义；脱敏要在还原之后做，不然正则匹配不到、
// 读的人解码后看到明文。
func TestToolArguments_EscapedJSONRedacted(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true), observability.WithRedactPatterns(regexp.MustCompile(`张三`)))
	ah := h.obs.AgentHooks()
	info := &hook.AgentInfo{Name: "a", Type: "base"}
	ctx, _ := ah.OnAgentStart(context.Background(), info, &hook.AgentInput{})
	tctx, _ := ah.OnToolStart(ctx, info, &schema.ToolInput{Name: "lookup", CallId: "c1",
		ArgumentsJSON: `{"name":"\u5f20\u4e09"}`})
	ah.OnToolEnd(tctx, info, &schema.ToolOutput{CallId: "c1", Result: &schema.ToolCallResult{Success: true,
		Output: `{"owner":"\u5f20\u4e09","balance":1}`}})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	ts := h.span(t, "execute_tool lookup")
	for _, k := range []attribute.Key{observability.AttrToolCallArguments, observability.AttrToolCallResult} {
		v := strAttr(t, ts, k)
		if strings.Contains(v, `\u5f20`) || strings.Contains(v, "张三") || !strings.Contains(v, "[REDACTED]") {
			t.Errorf("%s 应还原转义后脱敏，got %s", k, v)
		}
	}
}

// TestAgentEnd_IgnoresForeignHandle：框架在某些路径上拿开始之前的 ctx 调结束钩子（放弃续跑时开始钩子报错），
// 那里最内层的 agent 句柄是外层的。别的 agent 的结束不能把外层的 span 收掉。
func TestAgentEnd_IgnoresForeignHandle(t *testing.T) {
	h := newHarness(t)
	ah := h.obs.AgentHooks()
	outer := &hook.AgentInfo{Name: "outer", Type: "sequential"}
	octx, _ := ah.OnAgentStart(context.Background(), outer, &hook.AgentInput{})
	ah.OnAgentEnd(compose_v2.WithGraphNS(octx, "step"), &hook.AgentInfo{Name: "inner", Type: "base"}, &hook.AgentOutput{})
	if len(h.spansNamed("invoke_workflow outer")) != 0 {
		t.Fatal("别的 agent 的结束钩子把外层 span 收掉了")
	}
	ah.OnAgentEnd(octx, outer, &hook.AgentOutput{})
	h.assertAllEnded(t)
}

// TestNodeEnd_IgnoresSameNameOuterNode：嵌套图里的同名节点开始失败时，框架用开始之前的 ctx 调 OnNodeEnd，
// 那里最内层的句柄是外层的同名节点，靠 ns 区分，不能收掉外层的。
func TestNodeEnd_IgnoresSameNameOuterNode(t *testing.T) {
	h := newHarness(t)
	oh := h.obs.OrchHooks()
	actx, _ := oh.OnNodeStart(compose_v2.WithGraphNS(context.Background(), "a"), "x")
	oh.OnNodeEnd(compose_v2.WithGraphNS(actx, "a/sub"), "x", errors.New("inner start failed"))
	if len(h.spansNamed("node x")) != 0 {
		t.Fatal("内层同名节点的结束钩子把外层节点 span 收掉了")
	}
	oh.OnNodeEnd(actx, "x", nil)
	h.assertAllEnded(t)
	if s := strAttr(t, h.span(t, "node x"), observability.AttrOutcome); s != "success" {
		t.Errorf("外层节点自己的结局应是 success，got %s", s)
	}
}

// TestNestedOrchestrations_SpansComplete：真实嵌套编排（Sequential 套 Parallel，分支里各有一轮工具调用）下，每次
// agent / 编排调用恰好一个 span、全部收尾、在途数归零——重复进入的识别不误伤正常嵌套。
func TestNestedOrchestrations_SpansComplete(t *testing.T) {
	h := newHarness(t)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), h.obs.OrchHooks())
	leaf := func(name string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(name),
			agent_v2.WithModel(&scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}),
			agent_v2.WithToolkit(weatherToolkit(sunny)), agent_v2.WithHooks(h.obs.AgentHooks()))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par"), agent_v2.WithParallelHooks(h.obs.AgentHooks())).
		AddBranch("b1", leaf("worker")).AddBranch("b2", leaf("worker"))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialHooks(h.obs.AgentHooks())).
		AddStep("first", leaf("planner")).AddStep("fan", par)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h.assertAllEnded(t)
	for name, want := range map[string]int{
		"invoke_workflow seq": 1, "invoke_workflow par": 1, "invoke_agent planner": 1, "invoke_agent worker": 2,
		"execute_tool get_weather": 3,
	} {
		if got := len(h.spansNamed(name)); got != want {
			t.Errorf("%s span 应有 %d 个，got %d；全部：%v", name, want, got, names(h.spans()))
		}
	}
	if n := sumInt(t, h.collect(t), observability.MetricAgentActive, nil); n != 0 {
		t.Errorf("在途调用数=%d 应=0", n)
	}
}

// ─── 跨进程传播 ───

// TestPropagation_RoundTrip：InjectToMetadata 写进去的 trace context，ExtractFromMetadata 取出来是同一个
// 远端父；空 / 不相干的 metadata 原样返回 ctx。
func TestPropagation_RoundTrip(t *testing.T) {
	h := newHarness(t)
	ctx, span := h.obs.Tracer().Start(context.Background(), "parent")
	defer span.End()
	md := map[string]any{"other": 1}
	h.obs.InjectToMetadata(ctx, md)
	if _, ok := md["traceparent"].(string); !ok {
		t.Fatalf("应写入 W3C traceparent，got %v", md)
	}
	got := trace.SpanContextFromContext(h.obs.ExtractFromMetadata(context.Background(), md))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("取出的远端父应等于注入方的 span，got %+v", got)
	}

	h.obs.InjectToMetadata(ctx, nil) // nil metadata 不 panic
	base := context.Background()
	if c := h.obs.ExtractFromMetadata(base, map[string]any{"traceparent": 42}); trace.SpanContextFromContext(c).IsValid() {
		t.Error("traceparent 不是字符串时不该取出 span context")
	}
	if c := h.obs.ExtractFromMetadata(base, nil); c != base {
		t.Error("空 metadata 应原样返回 ctx")
	}
}

// TestTraceIDFromContext：有 span 时返回 32 位十六进制 trace ID，没有时返回空串。
func TestTraceIDFromContext(t *testing.T) {
	h := newHarness(t)
	if id := observability.TraceIDFromContext(context.Background()); id != "" {
		t.Errorf("没有 span 时应返回空串，got %q", id)
	}
	ctx, span := h.obs.Tracer().Start(context.Background(), "x")
	defer span.End()
	id := observability.TraceIDFromContext(ctx)
	if len(id) != 32 || id != span.SpanContext().TraceID().String() {
		t.Errorf("trace ID=%q 应等于当前 span 的", id)
	}
}

// TestNilObservability_NoPanic：A2A 两侧的入口对 nil 接收者安全（没接 observability 时调用方不用判空）。
func TestNilObservability_NoPanic(t *testing.T) {
	var o *observability.Observability
	ctx := context.Background()
	o.InjectToMetadata(ctx, map[string]any{})
	_ = o.ExtractFromMetadata(ctx, map[string]any{"traceparent": "x"})
	_, cs := o.StartA2AClientSpan(ctx, "r", "m", "t", "", "")
	cs.End(ctx, errors.New("x"))
	_, ss := o.StartA2AServerSpan(ctx, "op", "", "")
	ss.End(observability.OutcomeError, "")
	o.CleanupSession("s")
	if o.PendingInterrupts() != 0 {
		t.Error("nil 实例的挂起记录数应是 0")
	}
}
