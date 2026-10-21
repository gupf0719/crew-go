package observability

// Phase 2 Sub-5a: 新加 MetricsService client import (跟 TraceService 平行).
//
// 使用 OTLP v0.7.0 metrics proto — Counter 用 IntSum + IntDataPoint 路径.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip" // 注册 gzip codec

	"crew-go/internal/panicerr"
)

// exporter 是 OTLP gRPC trace 上报客户端 — framework 内部用.
//
// 业务调用链: hook 触发 → 构造 *tracepb.Span → Enqueue(span)
// → batch tick (5s) → Export gRPC RPC → CS-OSS collector
//
// **设计要点**:
//   - 内存 batch slice + sync.Mutex (低开销, 单一 exporter goroutine 消费)
//   - 队列满 (>= TraceQueueMaxSize) 时 drop-oldest (FIFO from front, 丢队头老
//     span 保新流量) + 记 dropped_count (业务可订阅 metric 看 drop 率)
//   - 5s tick 周期 flush + queue 满立即 flush (避免高 QPS 卡死)
//   - Resource (appname/env_name/ip/...) 进程级标签每次 Export 都附 (按 spec 规约)
//   - Close 优雅退出: 关 tick → flush 剩余 → 关 gRPC conn
//
// 不使用 OTel SDK 的原因: 我们要的 CS-OSS Trace ID 是 UTF-8 字符串 bytes (spec
// line 185-187), OTel SDK 默认是 binary 16/8 bytes — 拼装 OTLP Span 时直接走
// proto, 不绕 OTel SDK Span/Tracer 抽象.
type exporter struct {
	cfg         *Config
	traceConn   *grpc.ClientConn
	traceClient coltracepb.TraceServiceClient
	resource    *resourcepb.Resource
	instLib     *commonpb.InstrumentationScope

	mu    sync.Mutex
	queue []*tracepb.Span

	// review P1 audit MED-4 fix: 拆 2 个 drop counter 让业务区分 "本机限流压力"
	// (queue overflow) vs "网络/collector 故障" (export RPC 失败), 业务监控时不
	// 把两种 drop 混算.
	droppedQueue  atomic.Int64 // queue 溢出 (TraceQueueMaxSize 满 drop-oldest)
	droppedExport atomic.Int64 // export RPC 失败 (整 batch 算 drop, 含网络/超时/collector 5xx)

	// Phase 2 Sub-5a: metric 路径 — 独立 conn + client + registry, 同 exporter
	// 生命周期管. 业务 Provider.Counter() 写到 registry, exportLoop 5s tick 转
	// OTLP MetricsData 推到 MetricsService. 不复用 traceClient 是因 RPC 接口不同
	// (TraceService / MetricsService 不同 server endpoint 可能).
	metricConn     *grpc.ClientConn
	metricClient   colmetricspb.MetricsServiceClient
	metricRegistry *metricRegistry
	metricDropped  atomic.Int64 // metric export RPC 失败

	done   chan struct{}
	closed atomic.Bool
	wg     sync.WaitGroup
}

// newExporter 构造 exporter + 起 gRPC 连接 + 起 tick goroutine.
//
// Endpoint 空时返 nil exporter — 调用方判 nil 跳过上报 (业务只想要 metric 不要 trace).
func newExporter(cfg *Config) (*exporter, error) {
	if cfg == nil || cfg.TraceEndpoint == "" {
		return nil, nil
	}
	// gRPC dial — insecure 默认 (内部网络). 业务想 TLS 可后续加 Config.TraceTLS 字段.
	// gzip 压缩默认开 (减网络体量). spec 没强制但 SDK INTEGRATION line 437 推荐.
	// 用 grpc.NewClient (lazy 连接, 首次 RPC 才建链).
	conn, err := grpc.NewClient(cfg.TraceEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: dial trace endpoint %q: %w", cfg.TraceEndpoint, err)
	}

	e := &exporter{
		cfg:         cfg,
		traceConn:   conn,
		traceClient: coltracepb.NewTraceServiceClient(conn),
		resource:    buildResource(&cfg.Resource),
		instLib: &commonpb.InstrumentationScope{
			Name:    "LLM", // 按 AI_TRACE_SPEC.md 强制 "LLM"
			Version: cfg.Resource.SDKVersion,
		},
		queue:          make([]*tracepb.Span, 0, cfg.TraceExportBatchSize),
		metricRegistry: newMetricRegistry(cfg.MaxMetricSeries, cfg.DisabledMetrics),
		done:           make(chan struct{}),
	}

	// Phase 2 Sub-5a: metric gRPC conn — 空 MetricsEndpoint 时业务不上报 metric
	// 但 metric API 仍可调 (registry 内 inc 不出错, 仅 tick 路径 skip RPC).
	if cfg.MetricsEndpoint != "" {
		mConn, mErr := grpc.NewClient(cfg.MetricsEndpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		)
		if mErr != nil {
			// trace 已成功 dial — 关 trace conn 后返 err (避免半启状态)
			_ = conn.Close()
			return nil, fmt.Errorf("observability: dial metrics endpoint %q: %w", cfg.MetricsEndpoint, mErr)
		}
		e.metricConn = mConn
		e.metricClient = colmetricspb.NewMetricsServiceClient(mConn)
	}

	// tick goroutine
	e.wg.Add(1)
	go e.exportLoop()
	return e, nil
}

// Enqueue 把 span 加入 batch queue. 非阻塞.
//
// 队列满时 drop-oldest 老 span 优先 (新业务流量保留体现"最近用户操作的 trace 更
// 重要"假设). dropped count 累加供 metric 查询.
func (e *exporter) Enqueue(span *tracepb.Span) {
	if e == nil || span == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	// **wg 竞态修复**：closed 检查移到 mu 内（与下方 batch-flush 的 wg.Add 同临界区）——防
	// Close 的 wg.Wait 与本函数 wg.Add 在 counter==0 瞬间并发触发 "WaitGroup reused before
	// Wait returned" panic（-race 实证）。Close 也在 mu 内 closed.Store，故 Enqueue 一旦见
	// closed=true 绝不再 wg.Add；mu 串行化了 closed 转换与 wg.Add。
	if e.closed.Load() {
		return
	}

	if len(e.queue) >= e.cfg.TraceQueueMaxSize {
		// drop-oldest (FIFO from front): 丢队头老 span 保留新流量 — 假设"最近用户
		// 操作的 trace 业务价值更高" (跟 OTel SDK BatchSpanProcessor 同款策略).
		e.queue = e.queue[1:]
		e.droppedQueue.Add(1)
	}
	e.queue = append(e.queue, span)

	// 队列达 batch size 立即 flush 不等 tick (避免高 QPS 队列堆积)
	if len(e.queue) >= e.cfg.TraceExportBatchSize {
		// 异步 flush 不阻塞 caller hook (hook 在业务路径上, 不能等网络).
		// review P1 audit HIGH-2 fix: 异步 export 加入 wg, 让 Close 等其完成
		// 才关 gRPC conn (否则 RPC 跑到一半被 conn close 整 batch 算 drop).
		batch := e.drainLocked()
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			// review P0-P2 整体 audit² 视角 D HIGH-D3 fix: 异步 export goroutine
			// 同款加 defer recover 不让 panic 杀 wg 让 Close 永等.
			defer func() {
				if r := recover(); r != nil {
					panicerr.Log(context.Background(), slog.LevelError, "observability: async export goroutine panic recovered",
						panicerr.New(r), "batch_size", len(batch))
					e.droppedExport.Add(int64(len(batch)))
				}
			}()
			e.export(batch, false) // Close 等齐 (wg.Wait), 不 self-cancel 避免 drop
		}()
	}
}

// DroppedQueueCount 返 queue overflow 累计 drop (本机限流压力指标).
func (e *exporter) DroppedQueueCount() int64 {
	if e == nil {
		return 0
	}
	return e.droppedQueue.Load()
}

// DroppedExportCount 返 export RPC 失败累计 drop (网络/collector 故障指标).
func (e *exporter) DroppedExportCount() int64 {
	if e == nil {
		return 0
	}
	return e.droppedExport.Load()
}

// DroppedCount 返两种 drop 总和 (向后兼容). 业务想区分 queue vs export 用
// DroppedQueueCount / DroppedExportCount.
func (e *exporter) DroppedCount() int64 {
	if e == nil {
		return 0
	}
	return e.droppedQueue.Load() + e.droppedExport.Load()
}

// exportLoop tick 5s 周期 flush queue. exporter goroutine.
func (e *exporter) exportLoop() {
	defer e.wg.Done()
	// outer recover 兜底极端场景 (ticker / select / drainLocked panic, 几乎不可能) —
	// 真退出时记 log. 但**单次 export 的 panic 由 flushOnce per-tick recover 隔离**,
	// 不走到这里 (见 flushOnce).
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(context.Background(), slog.LevelError, "observability: exportLoop outer panic (exporter goroutine 退出)",
				panicerr.New(r))
		}
	}()
	// trace / metric 各自独立 tick——上报频率不锁死（对齐 OTel BatchTimeout vs
	// PeriodicReader interval 分离；两路一般同 collector 但频率可不同）。仍单 goroutine
	// 2 ticker，比各起一个 goroutine 省一个。
	traceTick := time.NewTicker(e.cfg.TraceExportInterval)
	defer traceTick.Stop()
	metricTick := time.NewTicker(e.cfg.MetricsExportInterval)
	defer metricTick.Stop()

	for {
		select {
		case <-traceTick.C:
			e.flushTrace(true) // 只 flush trace batch（tick 路径允许 Close cancel 长 RPC）
		case <-metricTick.C:
			e.exportMetricsSafe(true) // 只 flush metric snapshot（独立频率）
		case <-e.done:
			e.flushOnce(false) // 优雅退出: flush 剩余 trace + metric, 不 self-cancel (caller 是 Close)
			return
		}
	}
}

// flushOnce 单次 flush trace batch + metric. **per-tick panic recover** (Round 4 L3
// 决策更新, 学 LangSmith SDK _tracing_thread_handle_batch per-batch catch 韧性):
// 单次 export 的 panic (proto.Marshal / grpc client / 业务自定义 metric 边界等极端
// 场景) 只 drop 本次 + 记 log, **不杀 exportLoop goroutine** — 下一 tick 继续.
//
// 区别于 outer recover (panic → goroutine 死 → 后续 export 全 drop 直到进程重启,
// OTel SDK 同款脆弱). crew-go 自管 export 是业务唯一上报路径, 故做 per-tick
// 韧性: export RPC 失败已 catch 继续 (export/exportMetrics 内部), panic 也
// per-tick 隔离继续.
//
// **trace / metric 独立 recover** (8f4c741 audit HIGH #3 fix): trace export panic
// 算 droppedExport (跟 Enqueue 异步 export goroutine drop 计数一致); metric export
// panic 不算 (metric cumulative, 下次 tick 全量 snapshot 重报不算永久 drop). 两个
// 独立 recover 让 trace panic 不影响 metric flush.
//
// **mu 安全**: drainLocked 在 mu.Lock/Unlock 内完成 (无 recover 区, drainLocked 不
// panic — 仅 slice 取出 + reset), export/exportMetrics 在 Unlock 之后各自 recover.
// flushOnce flush trace + metric（仅优雅退出 done 路径用；tick 路径各走独立 flushTrace /
// exportMetricsSafe）。trace / metric 各自 per-tick recover 见下。
func (e *exporter) flushOnce(cancelOnDone bool) {
	e.flushTrace(cancelOnDone)
	e.exportMetricsSafe(cancelOnDone)
}

// flushTrace 只 flush trace batch（trace tick 路径 / 优雅退出）。drainLocked 在 mu 内取出，
// exportTraceSafe 在 Unlock 后 per-tick recover。
func (e *exporter) flushTrace(cancelOnDone bool) {
	e.mu.Lock()
	batch := e.drainLocked()
	e.mu.Unlock()
	if len(batch) > 0 {
		e.exportTraceSafe(batch, cancelOnDone)
	}
}

// exportTraceSafe wrap export 加 per-tick recover — trace export panic 时 batch
// drop 计 droppedExport (跟 Enqueue 异步 goroutine recover 一致), goroutine 继续.
func (e *exporter) exportTraceSafe(batch []*tracepb.Span, cancelOnDone bool) {
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(context.Background(), slog.LevelError, "observability: trace export panic recovered (batch drop, goroutine 继续下一 tick)",
				panicerr.New(r), "batch_size", len(batch))
			e.droppedExport.Add(int64(len(batch)))
		}
	}()
	e.export(batch, cancelOnDone)
}

// exportMetricsSafe wrap exportMetrics 加 per-tick recover — metric export panic
// 时不计 drop (metric cumulative, 下次 tick 全量 snapshot 重报), goroutine 继续.
func (e *exporter) exportMetricsSafe(cancelOnDone bool) {
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(context.Background(), slog.LevelError, "observability: metric export panic recovered (本次跳过, 下次 tick 全量重报)",
				panicerr.New(r))
		}
	}()
	e.exportMetrics(cancelOnDone)
}

// exportMetrics 把 metricRegistry snapshot 转 OTLP ExportMetricsServiceRequest
// + RPC 调用 MetricsService. metricConn nil 时 skip (业务不配 MetricsEndpoint).
//
// cancelOnDone 跟 trace export 同款 — tick 路径 true 让 Close cancel 长 RPC;
// done 路径 false 让 flush 走完不被 self-cancel.
func (e *exporter) exportMetrics(cancelOnDone bool) {
	if e == nil || e.metricClient == nil || e.metricRegistry == nil {
		return
	}
	metrics := e.metricRegistry.snapshot()
	if len(metrics) == 0 {
		return
	}
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: e.resource,
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope:   e.instLib,
				Metrics: metrics,
			}},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TraceRPCTimeout)
	defer cancel()
	if cancelOnDone {
		go func() {
			select {
			case <-e.done:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	if _, err := e.metricClient.Export(ctx, req); err != nil {
		e.metricDropped.Add(int64(len(metrics)))
		slog.WarnContext(ctx, "observability: metric export RPC failed",
			"endpoint", e.cfg.MetricsEndpoint, "metrics", len(metrics), "err", err)
	}
}

// drainLocked 取出当前 queue + 重置. 调用方必持 e.mu.
func (e *exporter) drainLocked() []*tracepb.Span {
	if len(e.queue) == 0 {
		return nil
	}
	batch := e.queue
	e.queue = make([]*tracepb.Span, 0, e.cfg.TraceExportBatchSize)
	return batch
}

// export 真实 RPC 调用. 失败不重试 (spec 没说 SDK 要重试, 失败丢弃由 collector
// 自己有 retry queue / 业务订阅 dropped metric 看).
//
// review P0-P2 整体 audit² 视角 D MED-D4 fix: ctx 关联 e.done — Close 时 done 立即
// fire 让 export RPC 中跑被 cancel, 不让 Close 阻塞 5-10s 等 RPC 完成 (tick 路径).
// 视角 C MED-2 fix: deadline 用 Config.TraceRPCTimeout 默认 10s (spec line 99).
//
// P2.6 audit follow-up fix: cancelOnDone 参数 — Close 触发的 flush 路径 (exportLoop
// done case) 跟 Enqueue async 路径不能 self-cancel, 否则 close(done) 立即 fire
// → flush RPC 被 cancel → "Close 应推 N span got 0" regression. 只有周期 tick 路径
// 才走 cancel (业务 Close 时正好命中 tick 的长 RPC).
func (e *exporter) export(spans []*tracepb.Span, cancelOnDone bool) {
	if len(spans) == 0 {
		return
	}
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: e.resource,
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: e.instLib,
						Spans: spans,
					},
				},
			},
		},
	}
	// done-aware ctx — timeout 兜底, cancelOnDone=true 时额外监听 e.done
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TraceRPCTimeout)
	defer cancel()
	if cancelOnDone {
		stopWatch := make(chan struct{})
		defer close(stopWatch)
		go func() {
			select {
			case <-e.done:
				cancel()
			case <-stopWatch:
			}
		}()
	}
	_, err := e.traceClient.Export(ctx, req)
	if err != nil {
		// 失败 slog.Warn 不抛 — 不阻塞业务. 业务订阅 metric 看 drop 率排查.
		slog.WarnContext(ctx, "observability: export trace failed",
			"endpoint", e.cfg.TraceEndpoint, "spans", len(spans), "err", err)
		e.droppedExport.Add(int64(len(spans)))
	}
	// 注: spec line 270+ OTLP `ExportTraceServiceResponse.PartialSuccess` 字段
	// 表示 collector 端部分接收, 当前 v0.7.0 proto 该字段定义但 CS-OSS collector
	// 实际是否填未确认. 后续 P3 metric 加 partial_success counter.
}

// Close 优雅关闭: 通知 tick goroutine 退出 → 等 flush 剩余 → 关 gRPC conn.
func (e *exporter) Close() error {
	if e == nil {
		return nil
	}
	// **wg 竞态修复**：closed.Store 在 mu 内（与 Enqueue 的 closed 检查 + wg.Add 同临界区
	// 串行化，防 wg.Add-concurrent-with-Wait panic）。wg.Wait 必须在 mu **外**——否则与
	// exportLoop done 路径 flushTrace 的 mu.Lock 死锁（Close 持 mu 等 exportLoop wg.Done，
	// exportLoop 阻塞在 flushTrace 的 mu.Lock 等 Close 释放）。
	e.mu.Lock()
	if e.closed.Load() {
		e.mu.Unlock()
		return nil // 幂等重复 close 短路
	}
	e.closed.Store(true)
	e.mu.Unlock()
	close(e.done)
	e.wg.Wait()
	// Phase 2 Sub-5a: 同时关 metric conn
	if e.metricConn != nil {
		_ = e.metricConn.Close()
	}
	if e.traceConn != nil {
		return e.traceConn.Close()
	}
	return nil
}

// buildResource 把 Config.Resource 转 OTLP Resource (KeyValue list).
//
// 按 spec line 117-145, 必填字段 + 建议字段都写, 同时兼容 OTel 标准字段
// (service.name / deployment.environment) 让标准 OTel UI 也可查.
func buildResource(r *Resource) *resourcepb.Resource {
	attrs := []*commonpb.KeyValue{
		strKV("appname", r.AppName),        // spec line 117 必填
		strKV("env_name", r.EnvName),       // spec line 118 必填
		strKV("ip", r.IP),                  // spec line 121 必填
		strKV("sdk_version", r.SDKVersion), // spec line 122 必填
	}
	if r.EnvLevel != "" {
		attrs = append(attrs, strKV("env_level", r.EnvLevel))
	}
	if r.Zone != "" {
		attrs = append(attrs, strKV("zone", r.Zone))
	}
	if r.Host != "" {
		attrs = append(attrs, strKV("host", r.Host))
	}
	// OTel 兼容字段 (spec line 125-126 建议)
	//
	// **iter-5-R**: 删 `sdk.language` 字段 — `sdk_version` 已含语言信息
	// (e.g., `crew-go-1.0.0` 字面带 `-go-`), 业务监控控制台直接用
	// sdk_version regex 反查语言, sdk.language 冗余.
	attrs = append(attrs,
		strKV("service.name", r.AppName),
		strKV("deployment.environment", r.EnvName),
	)
	return &resourcepb.Resource{Attributes: attrs}
}

// strKV 构造 string 类型 OTLP KeyValue (Resource / attribute 通用).
//
// 注: 不在这里 truncate — Resource 字段是业务 init 时设的稳定值, 框架内 trace
// span attr 走 truncKV 走 Config-aware truncate. 分开避免 Resource 误截断.
func strKV(key, val string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}},
	}
}
