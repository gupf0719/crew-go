package observability

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Sub-5g type aliases — 让 mockMetricsServer 内 grpc 接口签名简短可读.
type (
	colmetricsService  = colmetricspb.UnimplementedMetricsServiceServer
	colmetricsRequest  = colmetricspb.ExportMetricsServiceRequest
	colmetricsResponse = colmetricspb.ExportMetricsServiceResponse
)

// registerMetricsServer 别名 — 测试侧 grpc.RegisterMetricsServiceServer 调用.
func registerMetricsServer(s *grpc.Server, srv colmetricspb.MetricsServiceServer) {
	colmetricspb.RegisterMetricsServiceServer(s, srv)
}

// mockTraceServer 接收 ExportTraceServiceRequest 并记录, 让测试验证 exporter
// 真把数据 ship 出去了.
type mockTraceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	called   atomic.Int32
	spanRecv atomic.Int32
}

func (m *mockTraceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	m.called.Add(1)
	for _, rs := range req.GetResourceSpans() {
		for _, ils := range rs.GetScopeSpans() {
			m.spanRecv.Add(int32(len(ils.GetSpans())))
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// startMockGRPCServer 起本地 mock gRPC server (随机端口), 返回 addr + cleanup.
func startMockGRPCServer(t *testing.T) (*mockTraceServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	m := &mockTraceServer{}
	coltracepb.RegisterTraceServiceServer(srv, m)
	go func() { _ = srv.Serve(lis) }()
	return m, lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func TestExporter_BatchTickFlush(t *testing.T) {
	mockSrv, addr, stop := startMockGRPCServer(t)
	defer stop()

	cfg := &Config{
		Resource:             Resource{AppName: "test-app", EnvName: "test", IP: "127.0.0.1", SDKVersion: "v0.1"},
		TraceEndpoint:        addr,
		TraceExportInterval:  200 * time.Millisecond, // 加速 tick 让测试不慢
		TraceExportBatchSize: 100,                    // 大于 enqueue 数, 走 tick 而非满 flush
	}
	cfg.applyDefaults()

	exp, err := newExporter(cfg)
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}
	defer exp.Close()

	for i := 0; i < 3; i++ {
		exp.Enqueue(&tracepb.Span{
			TraceId: []byte("test-app^^abcdef0123456789abcdef0123456789|1700000000000"),
			SpanId:  []byte("1|abcdef0123456789"),
			Name:    "test_span",
		})
	}
	// 等 tick 触发 + RPC 完成
	time.Sleep(500 * time.Millisecond)

	if mockSrv.called.Load() == 0 {
		t.Fatal("mock server 没收到 Export 调用 (tick 没 flush?)")
	}
	if got := mockSrv.spanRecv.Load(); got != 3 {
		t.Errorf("mock server 收到 %d span, want 3", got)
	}
}

func TestExporter_BatchSizeImmediateFlush(t *testing.T) {
	mockSrv, addr, stop := startMockGRPCServer(t)
	defer stop()

	cfg := &Config{
		Resource:             Resource{AppName: "test-app", EnvName: "test", IP: "127.0.0.1", SDKVersion: "v0.1"},
		TraceEndpoint:        addr,
		TraceExportInterval:  10 * time.Second, // tick 很久, 必须靠 batch size 触发
		TraceExportBatchSize: 3,                // enqueue 3 个就 flush
	}
	cfg.applyDefaults()

	exp, err := newExporter(cfg)
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}
	defer exp.Close()

	for i := 0; i < 3; i++ {
		exp.Enqueue(&tracepb.Span{
			TraceId: []byte("test-app^^abc|1700000000000"),
			SpanId:  []byte("1|hex"),
			Name:    "n",
		})
	}
	// batch 满立即异步 flush, 等 RPC
	time.Sleep(300 * time.Millisecond)

	if mockSrv.called.Load() == 0 {
		t.Fatal("batch 满后 Export 没触发立即 flush")
	}
	if got := mockSrv.spanRecv.Load(); got != 3 {
		t.Errorf("收到 %d span, want 3", got)
	}
}

func TestExporter_QueueOverflowDropsOldest(t *testing.T) {
	cfg := &Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        "127.0.0.1:1", // 不连真 server, 测试纯队列行为
		TraceQueueMaxSize:    3,
		TraceExportInterval:  10 * time.Second,
		TraceExportBatchSize: 10000, // 不让 batch 满 flush, 纯测 queue overflow
	}
	cfg.applyDefaults()

	exp, err := newExporter(cfg)
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}
	defer exp.Close()

	// enqueue 5 → 应 drop 2 个老的
	for i := 0; i < 5; i++ {
		exp.Enqueue(&tracepb.Span{Name: "n"})
	}
	if got := exp.DroppedCount(); got != 2 {
		t.Errorf("DroppedCount = %d, want 2 (5 enqueue - 3 queue size)", got)
	}
}

func TestExporter_NilEndpointReturnsNil(t *testing.T) {
	exp, err := newExporter(&Config{TraceEndpoint: ""})
	if err != nil {
		t.Errorf("空 endpoint 不应报错, got %v", err)
	}
	if exp != nil {
		t.Error("空 endpoint 应返 nil exporter (调用方判 nil 跳过)")
	}

	// nil exporter 调 Enqueue / Close / DroppedCount 不 panic
	exp.Enqueue(&tracepb.Span{})
	_ = exp.Close()
	if got := exp.DroppedCount(); got != 0 {
		t.Errorf("nil exporter DroppedCount 应 0, got %d", got)
	}
}

func TestExporter_CloseFlushesRemaining(t *testing.T) {
	mockSrv, addr, stop := startMockGRPCServer(t)
	defer stop()

	cfg := &Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  10 * time.Second, // 不让 tick 触发
		TraceExportBatchSize: 10000,            // 不让 batch 满
	}
	cfg.applyDefaults()

	exp, _ := newExporter(cfg)
	for i := 0; i < 2; i++ {
		exp.Enqueue(&tracepb.Span{Name: "n"})
	}

	// Close 应优雅 flush 剩余
	if err := exp.Close(); err != nil {
		t.Errorf("Close err: %v", err)
	}
	// 给一点时间让 export goroutine 完成 RPC
	time.Sleep(200 * time.Millisecond)

	if got := mockSrv.spanRecv.Load(); got != 2 {
		t.Errorf("Close flush 应推 2 span, got %d", got)
	}
}

// TestExporter_CloseWaitsAsyncBatchFlush 验 review P1 audit HIGH-2 fix —
// Enqueue 内 go export(batch) 加入 wg 后 Close 会等异步 RPC 完成才关 conn.
// 之前漏 wg → Close → conn 关 → 异步 RPC 中跑被中断 → 整 batch 算 drop.
//
// sanity-revert: 删 Enqueue 内 wg.Add/Done → Close 跟 conn close race →
// mock server 收到 0 span (RPC 中断) → 测试 fail.
func TestExporter_CloseWaitsAsyncBatchFlush(t *testing.T) {
	mockSrv, addr, stop := startMockGRPCServer(t)
	defer stop()

	cfg := &Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  10 * time.Second, // 不让 tick 触发
		TraceExportBatchSize: 3,                // enqueue 3 立即触发 async export
	}
	cfg.applyDefaults()

	exp, _ := newExporter(cfg)

	// enqueue 3 个立即触发异步 export goroutine
	for i := 0; i < 3; i++ {
		exp.Enqueue(&tracepb.Span{Name: "async-batch-flush"})
	}

	// 立刻 Close (异步 export 可能还没完成 RPC)
	if err := exp.Close(); err != nil {
		t.Errorf("Close err: %v", err)
	}

	// fix 后: Close wg.Wait 等了异步 export → mock server 应收到 3 span
	// fix 前 (revert): conn 关 → 异步 export 中跑被中断 → mock server 收 0 span + droppedExport=3
	if got := mockSrv.spanRecv.Load(); got != 3 {
		t.Errorf("review P1 audit HIGH-2 fix: Close 应等异步 export 完成, mock 收到 %d span want 3\n"+
			"sanity-revert: 删 Enqueue 内 e.wg.Add/Done block → conn 提前关 → RPC 中断 → 此 fail",
			got)
	}
	if got := exp.DroppedExportCount(); got != 0 {
		t.Errorf("Close 等齐 → 不应有 export drop, got %d", got)
	}
}

// TestExporter_DroppedCountSplit_ExportFailure 验 review P1 audit MED-4 fix —
// export RPC 失败 droppedExport 独立计数 (vs queue overflow 走 droppedQueue).
func TestExporter_DroppedCountSplit_ExportFailure(t *testing.T) {
	cfg := &Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        "127.0.0.1:1", // 拒连让 RPC 失败
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
	}
	cfg.applyDefaults()

	exp, _ := newExporter(cfg)
	defer exp.Close()

	exp.Enqueue(&tracepb.Span{Name: "fail-export-1"})
	exp.Enqueue(&tracepb.Span{Name: "fail-export-2"})

	// 等 tick + RPC 拒连
	time.Sleep(2 * time.Second)

	if got := exp.DroppedExportCount(); got != 2 {
		t.Errorf("export 失败应 droppedExport=2, got %d", got)
	}
	if got := exp.DroppedQueueCount(); got != 0 {
		t.Errorf("queue 没溢出应 droppedQueue=0, got %d", got)
	}
}

// TestExporter_DroppedCountSplit_QueueOverflow 验 queue overflow 走 droppedQueue.
func TestExporter_DroppedCountSplit_QueueOverflow(t *testing.T) {
	cfg := &Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        "127.0.0.1:1",
		TraceQueueMaxSize:    3,
		TraceExportInterval:  10 * time.Second, // 不让 tick 触发
		TraceExportBatchSize: 10000,            // 不让 batch 满 flush, 纯测 queue overflow
	}
	cfg.applyDefaults()

	exp, _ := newExporter(cfg)
	defer exp.Close()

	for i := 0; i < 5; i++ {
		exp.Enqueue(&tracepb.Span{Name: "overflow"})
	}
	if got := exp.DroppedQueueCount(); got != 2 {
		t.Errorf("5 enqueue 超 queue 3 应 droppedQueue=2, got %d", got)
	}
	if got := exp.DroppedExportCount(); got != 0 {
		t.Errorf("没发 RPC 应 droppedExport=0, got %d", got)
	}
	// 总和 = queue + export
	if total := exp.DroppedCount(); total != exp.DroppedQueueCount()+exp.DroppedExportCount() {
		t.Errorf("DroppedCount 应是 queue+export 总和, got %d", total)
	}
}

func TestExporter_DoubleCloseSafe(t *testing.T) {
	cfg := &Config{
		Resource:      Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint: "127.0.0.1:1",
	}
	cfg.applyDefaults()
	exp, _ := newExporter(cfg)
	if err := exp.Close(); err != nil {
		t.Errorf("first Close err: %v", err)
	}
	if err := exp.Close(); err != nil {
		t.Errorf("second Close 应短路不报错, got %v", err)
	}
}

func TestBuildResource_AllFields(t *testing.T) {
	r := &Resource{
		AppName: "my-app", EnvName: "prod", EnvLevel: "online", Zone: "sh",
		IP: "10.0.0.1", Host: "host-1", SDKVersion: "v1.0",
	}
	res := buildResource(r)

	// 必填 + 建议 + OTel 兼容 = 10 个
	got := map[string]string{}
	for _, kv := range res.Attributes {
		if sv, ok := kv.Value.Value.(*commonpb.AnyValue_StringValue); ok {
			got[kv.Key] = sv.StringValue
		}
	}
	wantKeys := []string{"appname", "env_name", "ip", "sdk_version", "env_level", "zone", "host", "service.name", "deployment.environment"}
	for _, k := range wantKeys {
		if _, ok := got[k]; !ok {
			t.Errorf("Resource 缺 key %q, got = %v", k, got)
		}
	}
	if got["service.name"] != "my-app" {
		t.Errorf("service.name 应 = AppName, got %q", got["service.name"])
	}
}

// 静默 unused import (insecure 在测试 mockServer 用; conn keep 用)
var _ = insecure.NewCredentials
var _ = (&mockTraceServer{}).Export

// ─── per-tick recover 测试 (Round 4 L3 决策更新, 学 LangSmith per-batch catch) ───

// panicTraceClient 模拟 export RPC panic (proto/grpc 极端场景) — 验 flushOnce
// per-tick recover 隔离单次 export panic 不杀 exportLoop goroutine.
type panicTraceClient struct{}

func (panicTraceClient) Export(ctx context.Context, in *coltracepb.ExportTraceServiceRequest, opts ...grpc.CallOption) (*coltracepb.ExportTraceServiceResponse, error) {
	panic("simulated export panic (proto.Marshal / grpc 极端场景)")
}

// TestExporter_FlushOnce_PerTickPanicRecover 验单次 export panic 被 flushOnce
// per-tick recover 隔离 — goroutine 不死, 下一 tick 继续 (学 LangSmith SDK
// _tracing_thread_handle_batch per-batch catch 韧性).
//
// sanity-revert: 删 flushOnce 内 defer recover → 此测 panic crash 整个 test 进程.
func TestExporter_FlushOnce_PerTickPanicRecover(t *testing.T) {
	_, addr, stop := startMockGRPCServer(t)
	defer stop()

	cfg := &Config{
		Resource:             Resource{AppName: "t", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  10 * time.Second,
		TraceExportBatchSize: 100,
	}
	cfg.applyDefaults()
	exp, err := newExporter(cfg)
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}
	defer exp.Close()

	// 注入 panic client (替换真 traceClient)
	exp.traceClient = panicTraceClient{}
	exp.Enqueue(&tracepb.Span{Name: "panic_test"}) // 非空 batch 触发 export → panic

	// flushOnce 内 exportTraceSafe(batch) → panicTraceClient.Export panic → per-tick recover.
	// 到达下一行说明 recover 生效 (panic 没传播 crash 测试).
	exp.flushOnce(false)

	// 8f4c741 audit HIGH #3 fix: trace export panic 时 batch 计 droppedExport
	// (跟 Enqueue 异步 export goroutine 一致). 1 span panic → droppedExport >= 1.
	if got := exp.DroppedExportCount(); got < 1 {
		t.Errorf("audit HIGH #3 regression: trace export panic 后 droppedExport 应 >= 1, got %d\n"+
			"sanity-revert: 删 exportTraceSafe recover 内 droppedExport.Add → 此测 fail", got)
	}

	// 再调一次验 goroutine 概念上"继续下一 tick"仍工作 (recover 后 exporter 没坏死)
	exp.Enqueue(&tracepb.Span{Name: "panic_test_2"})
	exp.flushOnce(false)
	if got := exp.DroppedExportCount(); got < 2 {
		t.Errorf("第 2 次 trace export panic droppedExport 应 >= 2, got %d", got)
	}

	t.Log("flushOnce 两次 trace export panic 都被 per-tick recover 隔离 + droppedExport 计数, exporter 未死")
}

// ─── Sub-5g: snapshot OTLP wire format proto.Marshal 验证测 ───
//
// 验 framework 内 metricRegistry.snapshot() 输出经 proto.Marshal/Unmarshal 后
// collector 端能正确反序列化数据 (字段名 / 字段类型 / 值 全对). 防 metricspb v0.7.0
// 我们设计跟 proto schema 偏差 — 之前 IntHistogram → DoubleHistogram fix 就是这类
// schema 不匹配 bug, wire format 验证测可提前抓.

// mockMetricsServer 接收 ExportMetricsServiceRequest 并捕获 Metrics 字段
// (跟 mockTraceServer 同款 pattern). Sub-5g.
type mockMetricsServer struct {
	colmetricspbServer
	called     atomic.Int32
	lastReq    atomic.Pointer[colmetricspbReq]
	metricRecv atomic.Int32
}

// 别名让 grpc 接口签名简短
type colmetricspbServer = colmetricsService
type colmetricspbReq = colmetricsRequest

func (m *mockMetricsServer) Export(ctx context.Context, req *colmetricsRequest) (*colmetricsResponse, error) {
	m.called.Add(1)
	m.lastReq.Store(req)
	for _, rm := range req.GetResourceMetrics() {
		for _, ilm := range rm.GetScopeMetrics() {
			m.metricRecv.Add(int32(len(ilm.GetMetrics())))
		}
	}
	return &colmetricsResponse{}, nil
}

func startMockMetricsGRPCServer(t *testing.T) (*mockMetricsServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	m := &mockMetricsServer{}
	registerMetricsServer(srv, m)
	go func() { _ = srv.Serve(lis) }()
	return m, lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

// TestMetricsExportInterval_IndependentTick 验 metric 按 MetricsExportInterval 独立 tick
// flush——不依赖 TraceExportInterval、不依赖 Close。double ticker 改造后 MetricsExportInterval
// 才从 dead field 变真正生效（旧代码 metric flush 耦合在单 trace tick 上，该字段没被消费）。
//
// 设 MetricsExportInterval=50ms + TraceExportInterval=30s（测试期不 fire）+ 不 Close：唯一
// 能触发 metric flush 的路径就是 metricTick，故 mock server 收到 Export 即证明独立生效。
//
// sanity-revert: exportLoop 里把 `case <-metricTick.C: e.exportMetricsSafe(true)` 删掉、或
// 把 metricTick 建成 NewTicker(e.cfg.TraceExportInterval)（退回耦合）→ 2s 内 metric server
// 收不到 Export → 此测 fail，精确指向 metricTick 未独立驱动 metric flush。
func TestMetricsExportInterval_IndependentTick(t *testing.T) {
	mockSrv, addr, stop := startMockMetricsGRPCServer(t)
	defer stop()

	obs, err := New(Config{
		TraceEndpoint:         addr, // newExporter 要求非空（:76 早退）；TraceExportInterval 30s 不 fire
		MetricsEndpoint:       addr,
		TraceExportInterval:   30 * time.Second,      // 长到测试期不 fire — 隔离出 metricTick
		MetricsExportInterval: 50 * time.Millisecond, // 短 — 独立驱动 metric flush
		Resource:              Resource{AppName: "test-app", EnvName: "test", IP: "1.2.3.4", SDKVersion: "v0.1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = obs.Close() }()

	c := obs.Counter("test.independent.metric.tick", map[string]string{"k": "v"})
	if c == nil {
		t.Fatal("Counter nil")
	}
	c.Inc()

	// 不 Close——轮询等 metricTick 独立触发 flush（最多 2s，50ms interval 下通常 <200ms）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mockSrv.called.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if mockSrv.called.Load() == 0 {
		t.Fatal("metricTick 未独立触发 metric flush（2s 内 mock server 0 次 Export）——" +
			"MetricsExportInterval 未独立于 TraceExportInterval 生效")
	}
}

// TestPhase2Sub5g_OTLPWireFormat_Counter 验 Counter 经 proto.Marshal/Unmarshal
// 后 collector 收到的 Sum / Labels / Name 全字段对.
//
// sanity-revert: 改 snapshot() 把 IntSum 类型搞错 (e.g., 用 Metric_DoubleSum) →
// collector 端反序列化拿不到 IntSum 数据 → 此测 fail.
func TestPhase2Sub5g_OTLPWireFormat_Counter(t *testing.T) {
	mockSrv, addr, stop := startMockMetricsGRPCServer(t)
	defer stop()

	obs, err := New(Config{
		// trace endpoint 设同 addr (mock server 没实 TraceService → trace RPC fail
		// Unimplemented, 但 metric 路径独立不影响). 当前设计要求 TraceEndpoint 非空
		// 才创 exporter (newExporter line 76 早退) — 测试 work-around.
		TraceEndpoint:       addr,
		MetricsEndpoint:     addr,
		TraceExportInterval: 100 * time.Millisecond,
		Resource:            Resource{AppName: "test-app", EnvName: "test", IP: "1.2.3.4", SDKVersion: "v0.1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	c := obs.Counter("test.wire.counter", map[string]string{"k1": "v1", "k2": "v2"})
	if c == nil {
		t.Fatal("Counter nil")
	}
	c.Add(42)
	c.Inc() // 总 = 43

	// Close 触发 flush
	_ = obs.Close()
	// 等 RPC + mock server 收到
	time.Sleep(300 * time.Millisecond)

	if mockSrv.called.Load() == 0 {
		t.Fatal("mock metrics server 没收到 Export 调用")
	}
	req := mockSrv.lastReq.Load()
	if req == nil {
		t.Fatal("lastReq nil")
	}
	// 找到 test.wire.counter metric
	var found bool
	for _, rm := range req.GetResourceMetrics() {
		for _, ilm := range rm.GetScopeMetrics() {
			for _, m := range ilm.GetMetrics() {
				if m.GetName() != "test.wire.counter" {
					continue
				}
				found = true
				// iter-5-P fix: Counter 改用 DoubleSum (Prometheus 兼容)
				dblSum := m.GetSum()
				if dblSum == nil {
					t.Errorf("metric 应是 DoubleSum 类型 (Counter), got %T", m.GetData())
					continue
				}
				if len(dblSum.GetDataPoints()) != 1 {
					t.Errorf("应 1 个 DataPoint (1 series), got %d", len(dblSum.GetDataPoints()))
					continue
				}
				dp := dblSum.GetDataPoints()[0]
				if dp.GetAsDouble() != 43.0 {
					t.Errorf("wire format 后 Counter Value 应 = 43.0, got %v", dp.GetAsDouble())
				}
				// iter-5-R OTLP v1 升级: Labels → Attributes (KeyValue + AnyValue)
				gotLabels := map[string]string{}
				for _, kv := range dp.GetAttributes() {
					gotLabels[kv.GetKey()] = kv.GetValue().GetStringValue()
				}
				if gotLabels["k1"] != "v1" || gotLabels["k2"] != "v2" {
					t.Errorf("labels wire format 错: got %v want {k1:v1, k2:v2}", gotLabels)
				}
			}
		}
	}
	if !found {
		t.Error("collector 端没找到 test.wire.counter metric")
	}
}

// TestPhase2Sub5g_OTLPWireFormat_Histogram 验 Histogram (DoubleHistogram 含 Sum float64)
// 经 wire 反序列化字段对 — 防 audit HIGH-1 IntHistogram → DoubleHistogram 类回归.
func TestPhase2Sub5g_OTLPWireFormat_Histogram(t *testing.T) {
	mockSrv, addr, stop := startMockMetricsGRPCServer(t)
	defer stop()

	obs, err := New(Config{
		// trace endpoint 设同 addr (mock server 没实 TraceService → trace RPC fail
		// Unimplemented, 但 metric 路径独立不影响). 当前设计要求 TraceEndpoint 非空
		// 才创 exporter (newExporter line 76 早退) — 测试 work-around.
		TraceEndpoint:       addr,
		MetricsEndpoint:     addr,
		TraceExportInterval: 100 * time.Millisecond,
		Resource:            Resource{AppName: "t", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	h := obs.Histogram("test.wire.hist", []float64{0.5, 1.0, 5.0}, nil)
	h.Observe(0.3)  // bucket 0
	h.Observe(0.8)  // bucket 1
	h.Observe(2.0)  // bucket 2
	h.Observe(10.0) // bucket 3 (+Inf)
	// Sum = 13.1

	_ = obs.Close()
	time.Sleep(300 * time.Millisecond)

	req := mockSrv.lastReq.Load()
	if req == nil {
		t.Fatal("lastReq nil")
	}
	var found bool
	for _, rm := range req.GetResourceMetrics() {
		for _, ilm := range rm.GetScopeMetrics() {
			for _, m := range ilm.GetMetrics() {
				if m.GetName() != "test.wire.hist" {
					continue
				}
				found = true
				dh := m.GetHistogram()
				if dh == nil {
					t.Errorf("metric 应是 DoubleHistogram (audit HIGH-1 fix), got %T", m.GetData())
					continue
				}
				if len(dh.GetDataPoints()) != 1 {
					t.Errorf("应 1 DataPoint, got %d", len(dh.GetDataPoints()))
					continue
				}
				dp := dh.GetDataPoints()[0]
				if dp.GetCount() != 4 {
					t.Errorf("Count 应 = 4, got %d", dp.GetCount())
				}
				wantSum := 13.1
				if got := dp.GetSum(); got < wantSum-0.01 || got > wantSum+0.01 {
					t.Errorf("Sum 应 ≈ %.2f, got %.4f (audit HIGH-1 regression: int64 截断让此测 fail)", wantSum, got)
				}
				wantCounts := []uint64{1, 1, 1, 1}
				if got := dp.GetBucketCounts(); len(got) != 4 {
					t.Errorf("BucketCounts len 应 4, got %d", len(got))
				} else {
					for i, want := range wantCounts {
						if got[i] != want {
							t.Errorf("BucketCounts[%d] 应 = %d, got %d", i, want, got[i])
						}
					}
				}
				wantBounds := []float64{0.5, 1.0, 5.0}
				if got := dp.GetExplicitBounds(); len(got) != 3 {
					t.Errorf("ExplicitBounds len 应 3, got %d", len(got))
				} else {
					for i, want := range wantBounds {
						if got[i] != want {
							t.Errorf("ExplicitBounds[%d] 应 = %.2f, got %.2f", i, want, got[i])
						}
					}
				}
			}
		}
	}
	if !found {
		t.Error("collector 端没找到 test.wire.hist metric")
	}
}

// TestPhase2Sub5g_OTLPWireFormat_Gauge 验 Gauge (IntGauge) 经 wire 反序列化 Value 对.
func TestPhase2Sub5g_OTLPWireFormat_Gauge(t *testing.T) {
	mockSrv, addr, stop := startMockMetricsGRPCServer(t)
	defer stop()

	obs, err := New(Config{
		// trace endpoint 设同 addr (mock server 没实 TraceService → trace RPC fail
		// Unimplemented, 但 metric 路径独立不影响). 当前设计要求 TraceEndpoint 非空
		// 才创 exporter (newExporter line 76 早退) — 测试 work-around.
		TraceEndpoint:       addr,
		MetricsEndpoint:     addr,
		TraceExportInterval: 100 * time.Millisecond,
		Resource:            Resource{AppName: "t", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	g := obs.Gauge("test.wire.gauge", map[string]string{"x": "1"})
	g.Set(100)
	g.Inc() // 101
	g.Inc() // 102
	g.Dec() // 101

	_ = obs.Close()
	time.Sleep(300 * time.Millisecond)

	req := mockSrv.lastReq.Load()
	if req == nil {
		t.Fatal("lastReq nil")
	}
	var found bool
	for _, rm := range req.GetResourceMetrics() {
		for _, ilm := range rm.GetScopeMetrics() {
			for _, m := range ilm.GetMetrics() {
				if m.GetName() != "test.wire.gauge" {
					continue
				}
				found = true
				// iter-5-P fix: Gauge 改用 DoubleGauge (Prometheus 兼容)
				gauge := m.GetGauge()
				if gauge == nil {
					t.Errorf("metric 应是 DoubleGauge, got %T", m.GetData())
					continue
				}
				if len(gauge.GetDataPoints()) != 1 {
					t.Errorf("应 1 DataPoint, got %d", len(gauge.GetDataPoints()))
					continue
				}
				dp := gauge.GetDataPoints()[0]
				if dp.GetAsDouble() != 101.0 {
					t.Errorf("Gauge wire format 后 Value 应 = 101.0, got %v", dp.GetAsDouble())
				}
			}
		}
	}
	if !found {
		t.Error("collector 端没找到 test.wire.gauge metric")
	}
}
