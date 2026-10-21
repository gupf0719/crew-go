package observability

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// ────────────────────────────────────────────────────────────
// Sub-5a: OTLP metric DataPoint 构造 + 业务 API
//
// 设计源头:
//   - OTLP metric proto (go.opentelemetry.io/proto/otlp/metrics/v1):
//     ResourceMetrics → InstrumentationLibraryMetrics → Metrics{Sum/Gauge/Histogram}
//   - CS-OSS Monitor 规范 30 metric (Sub-5f LLMError + P0/P1 resumes/plan_replans/route_decisions + failover + member.tripped 扩后) (doc/v2/observability-implementation-guide.md §2)
//   - crew-go 自家实现 native metric (OTLP Sum/Gauge/Histogram DataPoint +
//     MetricsService gRPC client), 不要求业务另接 Prom/StatsD
//
// 设计核心:
//   - **业务调 Provider.Counter(name, labels) → 返 *Counter, Inc() 增 1**
//     (无锁 atomic.Int64, hot path 零分配)
//   - **labels 进 metric series key** — 同 name 不同 labels 是独立 series
//     (避免 cardinality 爆炸: 业务用 enum 类 label 不要用 user_id / call_id 高基数)
//   - **5s tick 自动批量推送** (复用 exporter goroutine)
//   - **registry per-Observability** — 业务 Close 时整体 GC
//
// Phase 2 Sub-5a 范围: Counter + MetricsService client + Provider API + 示例 metric
// Phase 2 Sub-5b 范围: Histogram + Gauge + 30 metric (Sub-5f LLMError + P0/P1 resumes/plan_replans/route_decisions + failover + member.tripped 扩后) 全接入
// ────────────────────────────────────────────────────────────

// ─── 30 metric (Sub-5f LLMError + P0/P1 resumes/plan_replans/route_decisions + failover + member.tripped 扩后) 名常量 ───
//
// 业务用 Provider.Counter(MetricAgentRunTotal, ...).Inc() 等. metric name 是 series
// 标识, labels 是 dimension.

// 业务 KPI metric (8 + 2 P0/P1 新增):
// **iter-5-Q 改 CS-OSS AI_METRICS_SPEC 命名** (业务真生产监控后端 silent drop
// 旧名). 协议变更:
//   - metric name 全用 `_` 分隔 (非 `.`)
//   - Counter name **不带** `_total` 后缀 (spec line 66 "后端或展示层按规则处理")
//   - **类型不变** — Counter / Histogram / Gauge 是 OTel 协议层独立类型, 类型
//     语义不能合并 (spec 用 "Timer" 是 Java SDK 封装, OTel 协议层就是 Histogram)
//   - label key 全用 `_` 分隔 (跟 trace span attribute 用 `.` 故意不同)
//
// **跟 spec 偏离的字段**: 我们多 19 个 metric (compact / cp / fanout / skill /
// interrupt / failover 等 crew-go 特有维度), spec 未明示但跟齐 spec 命名风格 (`_` 无
// `_total`).
const (
	// MetricLLMTokensTotal LLM token 消耗 — Counter, framework auto-fire.
	// spec line 132: `gen_ai_tokens` (Counter), 不带 `_total` 后缀.
	// labels (spec line 137-139): {gen_ai_model, gen_ai_token_type=input|output|total}.
	MetricLLMTokensTotal = "gen_ai_tokens"
	// MetricLLMCostUSDTotal LLM 成本 USD — Counter, **业务自填**.
	MetricLLMCostUSDTotal = "gen_ai_llm_cost_usd"
	// MetricAgentRunTotal agent 调用次数 — Counter, framework auto-fire.
	// labels: {gen_ai_agent_name, gen_ai_agent_kind, gen_ai_status}.
	MetricAgentRunTotal = "gen_ai_agent_runs"
	// MetricAgentRounds 每 Run ReAct 轮数 — Histogram.
	// labels: {gen_ai_agent_name, gen_ai_agent_kind}.
	MetricAgentRounds = "gen_ai_agent_rounds"
	// MetricInterruptTotal HITL 中断次数 — Counter, **P0 framework auto-fire** (Run/Resume defer
	// OnAgentEnd 检 GetEndError == InterruptError 时 fire). labels:
	// {gen_ai_agent_name, gen_ai_agent_kind}. 之前 godoc 写 "业务自填" 已过时.
	MetricInterruptTotal = "gen_ai_interrupts"
	// MetricInterruptResumeDuration 中断→Resume 时长 — Histogram, **P0 framework auto-fire**.
	// Run defer 检 InterruptError 时记录 nanos 到 sess.SetValue; Resume 入口
	// OnAgentStart 检 GetResumePhase 算 duration. labels: {gen_ai_agent_name, gen_ai_agent_kind}.
	MetricInterruptResumeDuration = "gen_ai_interrupt_resume_duration"
	// MetricResumesTotal Resume 调用次数 (HITL 用户响应) — Counter, **P0 framework auto-fire**.
	// labels: {gen_ai_agent_name, gen_ai_agent_kind, crew_resume_action=continue|replan|abort}.
	// 业务用此 metric 看 HITL 流程: abort 占比高 → 业务回答 UI 设计差; replan 占比高 → planner
	// 初版质量差用户常重导航; continue 是默认健康路径.
	MetricResumesTotal = "gen_ai_resumes"
	// MetricSkillActivateTotal skill 激活次数 — Counter, framework auto-fire.
	MetricSkillActivateTotal = "gen_ai_skill_activates"
)

// P1 编排级 metric (2 个 — PlanAgent / RoutingAgent 走 event-based 协议):
const (
	// MetricPlanReplansTotal PlanAgent replan 次数 — Counter, **P1 framework auto-fire**
	// (PlanAgent 每次重规划时 fire `plan_replan` event, observability.routeEventToMetric 路由).
	// labels: {gen_ai_agent_name}. 业务用此 metric 看 plan 质量 (replan 次数高 = 初版 plan 质量差).
	MetricPlanReplansTotal = "gen_ai_plan_replans"
	// MetricRouteDecisionsTotal RoutingAgent 路由决策次数 — Counter, **P1 framework auto-fire**
	// （升级链每次定出目标时 fire 一次，不管由决策方、classifier 还是兜底决定；Router 函数路径不 fire）
	// (RoutingAgent classifier 选定 target 后 fire `route_decision` event).
	// labels: {gen_ai_agent_name, crew_route_target}. 业务用此 metric 看流量分布到
	// 哪个 sub-agent 多 (热门 branch / fallback 占比).
	MetricRouteDecisionsTotal = "gen_ai_route_decisions"
)

// SRE metric (11):
//
// auto-fire 状态:
//   - LLMCallDuration / ToolCallDuration / AgentRunDuration / CompactTotal / CompactDuration
//     / CompactRatio / ToolErrorTotal / FailoverTotal — framework auto-fire (Sub-5b/5c/5d + failover sink-gated)
//   - LLMTTFTDuration / LLMTPOTDuration / LLMErrorTotal — **业务自填**
//     (stream wrap reader 复杂业务自定义 ChatModel.Stream 自上报更合理)
const (
	// MetricLLMCallDuration LLM 调用耗时秒 — Histogram (spec line 113: `gen_ai_model_calls` Timer 语义).
	MetricLLMCallDuration = "gen_ai_model_calls"
	// MetricLLMTTFTDuration time-to-first-token 秒 — Histogram, **业务自填**.
	MetricLLMTTFTDuration = "gen_ai_llm_ttft_duration"
	// MetricLLMTPOTDuration time-per-output-token 秒 — Histogram, **业务自填**.
	MetricLLMTPOTDuration = "gen_ai_llm_tpot_duration"
	// MetricLLMErrorTotal LLM 调用错误次数 — Counter, **业务自填**.
	MetricLLMErrorTotal = "gen_ai_llm_errors"
	// MetricToolCallDuration Tool 调用耗时秒 — Histogram (spec line 93: `gen_ai_tool_calls` Timer 语义).
	MetricToolCallDuration = "gen_ai_tool_calls"
	// MetricToolErrorTotal Tool 错误次数 — Counter, framework auto-fire.
	MetricToolErrorTotal = "gen_ai_tool_errors"
	// MetricAgentRunDuration Agent Run 耗时秒 — Histogram (跟 MetricAgentRunTotal Counter 配对一致命名).
	MetricAgentRunDuration = "gen_ai_agent_run_duration"
	// MetricCompactTotal Compact 次数 — Counter, framework auto-fire.
	MetricCompactTotal = "gen_ai_compacts"
	// MetricCompactDuration Compact 耗时秒 — Histogram.
	MetricCompactDuration = "gen_ai_compact_duration"
	// MetricCompactRatio 压缩比 (OutputTokens/InputTokens) — Histogram.
	MetricCompactRatio = "gen_ai_compact_ratio"
	// MetricFailoverTotal fallback 候选切换次数 — Counter, sink-gated auto-fire（业务接
	// observability + 用 fallback 时自动采集，同压缩机制；见 fallback/event.go +
	// fireFailoverMetric）。label{crew_failover_reason, crew_failover_from}。
	MetricFailoverTotal = "gen_ai_failovers"
	// MetricMemberTrippedTotal fallback 候选断路器熔断次数 — Counter, sink-gated auto-fire
	// （同 failover）。区别 failover（成员间切换）：tripped 是单成员 closed→open 熔断边缘，
	// 最后一个成员熔断也计（failover 计不到全员挂光）。见 fallback/event.go +
	// fireMemberTrippedMetric。label{crew_tripped_member, crew_tripped_reason}。
	MetricMemberTrippedTotal = "gen_ai_member_tripped"
	// MetricStaleWriteTotal 陈旧写回被守卫拒绝的次数 — Counter, framework auto-fire
	//（`WithStaleWriteGuard` 开启时才可能非零）。
	//
	// **为什么必须是 framework 档而非业务自填**：业务拿不到这个信号——它埋在框架 Run
	// 出口的错误链里，且正确判定要 `errors.Is` 穿透双 %w（守卫会保留原始 InterruptError）。
	//
	// **运维价值**：它意味着同一会话正被两个副本处理、副作用可能已重复发生。不打这个
	// metric 的话，三种 stale 形态分别混进 `blocked`（冒充正常中断）和 `error`（与任何
	// 业务错同池），运维只能翻日志。label{crew_stale_op}。
	MetricStaleWriteTotal = "gen_ai_stale_writes"
)

// 容量 metric (5):
const (
	MetricActiveSessions = "gen_ai_active_sessions"
	MetricRunsInProgress = "gen_ai_runs_inprogress"
	MetricCpSaveTotal    = "gen_ai_cp_saves"
	MetricCpSizeBytes    = "gen_ai_cp_size_bytes"
	MetricFanoutBranches = "gen_ai_fanout_branches"
)

// 业务体验/质量 metric (3):
const (
	// MetricToolCallsPerRound 每 ReAct 轮 tool call 数 — Histogram, **业务自填**.
	MetricToolCallsPerRound = "gen_ai_tool_calls_per_round"
	// MetricStreamChunksTotal 流式 LLM chunk 总数 — Counter, **业务自填**.
	MetricStreamChunksTotal = "gen_ai_stream_chunks"
	// MetricNodeExecutionTotal 节点执行次数 — Counter, framework auto-fire.
	MetricNodeExecutionTotal = "gen_ai_node_executions"
)

// metric label key 常量 (Round 7 audit Z2 防御性抽取).
//
// framework auto-fire metric 用这些常量作 label key (不散落字符串字面量防 typo);
// 业务自填 metric 时也应引用这些常量保持 label 命名一致 (跟 framework 同款 OTel /
// crew prefix), 让业务 dashboard 跨 framework + 业务 metric join 无歧义.
//
// 命名规约: OTel GenAI semconv 标准字段用 `gen_ai.*`; crew-go 自定义维度用
// `crew.*` prefix (无 OTel 对应); status 是 OTel 通用 attribute 短名.
// **iter-5-Q**: metric label key 全用 `_` 分隔 (跟 spec line 141-149 一致),
// 跟 trace span attribute 用 `.` 故意不同. spec 解释: 下划线在部分 metrics 后端
// (Prometheus) 或查询语言更稳定不产生歧义.
const (
	LabelAgentName    = "gen_ai_agent_name"  // agent 名 (info.Name)
	LabelAgentKind    = "gen_ai_agent_kind"  // agent 类型 (base/sequential/...)
	LabelToolName     = "gen_ai_tool_name"   // tool 名
	LabelRequestModel = "gen_ai_model"       // LLM model name (spec line 138 `gen_ai_model`)
	LabelSystem       = "gen_ai_system"      // model provider (openai/deepseek/dashscope/qwen)
	LabelUsageType    = "gen_ai_token_type"  // token type input/output/total (spec line 139)
	LabelStatus       = "gen_ai_status"      // success/error/timeout/cancelled (spec line 85)
	LabelCompactLayer = "crew_compact_layer" // micro/auto/reactive
	LabelOutcome      = "crew_outcome"       // true/false (compact/cp 成功与否)
	// failover metric label（crew 自定义加 prefix，对齐 LabelCompactLayer 命名）
	LabelFailoverReason = "crew_failover_reason" // rate_limit/timeout/server_error/network/quota_exhausted/api_error/error
	LabelFailoverFrom   = "crew_failover_from"   // 故障候选下标（哪个成员触发切换最多）
	LabelTrippedMember  = "crew_tripped_member"  // 被熔断的候选下标（哪个成员最不健康）
	LabelTrippedReason  = "crew_tripped_reason"  // 熔断的错误分类（复用 failoverReason 口径）
	LabelCpOp           = "crew_cp_op"           // save/load/cleanup
	LabelStaleOp        = "crew_stale_op"        // save/purge-success/purge-abort（复用 checkStale 的 op）
	LabelNodeID         = "crew_node_id"         // graph 节点 ID
	LabelFanoutKind     = "crew_fanout_kind"     // tool_calls/...
	// P0/P1 新增 label (crew_ prefix 标识自定义维度无 OTel 标准对应):
	LabelResumeAction = "crew_resume_action" // continue/replan/abort (Resume 三态)
	LabelRouteTarget  = "crew_route_target"  // RoutingAgent 选中的 branch name
	LabelPlanPhase    = "crew_plan_phase"    // initial/replan (PlanAgent planner 调用阶段)
)

// ─── Histogram bucket boundaries 预设 ───
//
// CS-OSS Monitor spec 3 类场景:
var (
	// BucketsLLMDuration LLM 调用 / agent run 等长耗时 (秒).
	BucketsLLMDuration = []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120}
	// BucketsSmallDuration tool call / compact / cp save 等小耗时 (秒).
	BucketsSmallDuration = []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1, 5}
	// BucketsSize cp / event 体量分布 (字节).
	BucketsSize = []float64{1024, 10240, 102400, 1048576, 10485760}
	// BucketsCount 离散 count 分布 (rounds / fanout branches 等).
	BucketsCount = []float64{1, 2, 5, 10, 20, 50, 100}
)

// ─── Counter ───
//
// 单调累加计数器. hot path 用 atomic.Int64, 无锁.

// Counter 是单调累加计数器 — 业务 Inc() / Add(n) 增加值, 不可减.
//
// 业务侧用法:
//
//	c := o.Counter(observability.MetricAgentRunTotal, map[string]string{
//	    "gen_ai_agent_name": "researcher", "gen_ai_agent_kind": "base", "gen_ai_status": "success",
//	})
//	c.Inc()
type Counter struct {
	name   string
	labels []*commonpb.KeyValue
	value  atomic.Int64
	// firstObservedNanos Counter **实例创建时间** (lazy create 时即设, 非业务首次
	// Inc 时间). OTLP cumulative monotonic Sum 要求 StartTimeUnixNano 跨 export 不变,
	// 用实例创建时间满足 (业务首次 Inc 可能晚于实例创建, 但 cumulative 语义是"从
	// 某固定时间点起累计", 实例创建时间是合理的零点).
	// audit Sub-5a LOW-8 fix: 之前 godoc 写 "首次记录的时间" 误导业务以为是首次 Inc.
	firstObservedNanos uint64
}

// Inc 增加 1. **nil-safe**: c=nil 时 no-op 返回, 业务可直接 chain call
// `obs.Counter("xx", labels).Inc()` — Counter() 返 nil 时 Inc 不 panic (Go nil
// receiver method 调用合法 + 显式 nil check).
//
// Sub-5 Round 4 audit L5 fix: godoc 强化 nil-safe 协议 — 业务 hot path 不需写
// `if c := obs.Counter(...); c != nil { c.Inc() }`, 直接 `obs.Counter(...).Inc()`
// 一行即可 (零开销).
func (c *Counter) Inc() {
	if c == nil {
		return
	}
	c.value.Add(1)
}

// Add 增加 n (n 可为 0, 不允许负数 — Counter 单调).
func (c *Counter) Add(n int64) {
	if c == nil || n < 0 {
		return
	}
	c.value.Add(n)
}

// Value 返回 Counter 当前累计值 — 业务侧测试 / 监控读快照用.
// 生产路径业务用 Counter 是 fire-and-forget (Inc 后等 OTLP 推, 不需 Get).
// audit Sub-5a MED-6 fix: 暴露 Value 让测试 assert 最终累计值 (race detector
// 抓不到 "Inc 实现错" 的 race-free 逻辑 bug, 如 store(load()+1) 替 atomic.Add).
func (c *Counter) Value() int64 {
	if c == nil {
		return 0
	}
	return c.value.Load()
}

// toOTLP 把 Counter 转 OTLP DoubleDataPoint (内嵌在 DoubleSum metric).
//
// **iter-5-P fix** (CS-OSS Monitor 后端兼容): 原用 `IntSum + IntDataPoint` (OTLP
// v0.7.0 deprecated 路径) → 监控后端 (Prometheus collector) 不识别 silent
// drop Counter metric (业务监控控制台只能看到 Histogram 类 metric).
//
// 改用 `DoubleSum + DoubleDataPoint` 跟 Histogram 同款 double 路径 — 业务
// Counter int64 累计值转 float64 (int64 max ~9.2e18, float64 精确表示 2^53 ≈
// 9e15, 业务 Counter 值远小于此, 不会丢精度).
func (c *Counter) toOTLP(nowNanos uint64) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes:        c.labels,
		StartTimeUnixNano: c.firstObservedNanos,
		TimeUnixNano:      nowNanos,
		Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(c.value.Load())},
	}
}

// ─── Histogram (Sub-5b) ───
//
// bucket 累计计数器 — 业务 Observe(v) 把 v 落入对应 bucket. 用于 duration /
// size 分布. OTLP v0.7.0 DoubleHistogram + DoubleHistogramDataPoint (Sum float64),
// 业务 Observe 单位由 buckets 决定 (秒数 / 字节 / count).
//
// audit Sub-5b HIGH-1 fix: 之前误用 IntHistogram + int64(sum) 截断 — 1ms tool call
// 100 次 sum=0.1s 被 int64() 截成 0 → avg=0 完全失真. 改用 DoubleHistogram + float64 sum.

// Histogram bucket 累计计数器.
//
// 业务侧:
//
//	h := o.Histogram(observability.MetricAgentRunDuration,
//	    observability.BucketsLLMDuration,
//	    map[string]string{"gen_ai_agent_name": "researcher", "gen_ai_agent_kind": "base"})
//	startTime := time.Now()
//	// ... agent.Run ...
//	h.Observe(time.Since(startTime).Seconds())
type Histogram struct {
	name   string
	labels []*commonpb.KeyValue
	// buckets boundaries — **caller 责任保证升序无重复** (Round 8 audit CC2):
	// Observe 落桶逻辑 `for i,b: if v<=b { idx=i; break }` 依赖升序, caller 传非升序
	// (e.g., [5,1,10]) 时落桶错乱. framework 预设 BucketsXxx 都升序无重复; 业务自定义
	// buckets 时必须升序 (跟 OTLP ExplicitBounds 规约一致). framework 不运行时校验
	// (热路径 lazy create 不付 sort 开销), caller fail-fast 自负.
	buckets []float64  // boundaries (升序), e.g. [0.1, 0.5, 1, 2, 5]
	mu      sync.Mutex // 保 counts/sum/count 一致 (Observe 单次锁很短)
	counts  []uint64   // 各 bucket 计数 (len = len(buckets)+1, 最后是 +Inf 桶)
	sum     float64    // 所有 Observe 值的累加
	count   uint64     // 总 observe 次数
	// firstObservedNanos Histogram 实例创建时间 (跟 Counter 同款).
	firstObservedNanos uint64
}

// Observe 把 v 加入对应 bucket. v 单位由业务定 (秒 / 字节等, 跟 buckets 一致).
//
// **不允许负 v** — 跟 Counter 同款 (cumulative monotonic 不接受负贡献).
//
// **bucket boundary 语义** (audit LOW-1): v <= b 落 b 桶 (右闭) — 跟 prometheus de-facto
// 一致, OTLP spec 严格定义是右开 (v < b). 业界惯例 + collector 实际渲染按 BucketCounts
// 直接画图, 边界值差 1 实际影响小. e.g. boundary 0.5, v=0.5 落 idx for 0.5 (不是下一桶).
func (h *Histogram) Observe(v float64) {
	if h == nil || v < 0 {
		return
	}
	// 找 v 落入哪个 bucket (升序 boundaries, v <= boundary 即落)
	idx := len(h.buckets) // +Inf 桶 (最后)
	for i, b := range h.buckets {
		if v <= b {
			idx = i
			break
		}
	}
	h.mu.Lock()
	h.counts[idx]++
	h.sum += v
	h.count++
	h.mu.Unlock()
}

// Sum 返累计 Observe 值的和 (test/监控用).
// audit MED-1 fix: Counts/Sum 暴露让测试可断言真值不只 Count.
func (h *Histogram) Sum() float64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

// BucketCounts 返各 bucket 累计计数 (test/监控用, len = len(buckets)+1, 最后 +Inf 桶).
func (h *Histogram) BucketCounts() []uint64 {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]uint64, len(h.counts))
	copy(out, h.counts)
	return out
}

// Count 返累计 Observe 次数 (test/监控用).
func (h *Histogram) Count() uint64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// toOTLP 把 Histogram 转 OTLP DoubleHistogramDataPoint (Sum float64 保精度).
// 业务 Observe 单位由 buckets 决定 (秒数 / 字节 / count 等), framework 不强制单位.
func (h *Histogram) toOTLP(nowNanos uint64) *metricspb.HistogramDataPoint {
	h.mu.Lock()
	countsCopy := make([]uint64, len(h.counts))
	copy(countsCopy, h.counts)
	sum := h.sum
	count := h.count
	h.mu.Unlock()
	bounds := make([]float64, len(h.buckets))
	copy(bounds, h.buckets)
	return &metricspb.HistogramDataPoint{
		Attributes:        h.labels,
		StartTimeUnixNano: h.firstObservedNanos,
		TimeUnixNano:      nowNanos,
		Count:             count,
		Sum:               &sum, // v1 Sum 是 *float64 (oneof, 可选)
		BucketCounts:      countsCopy,
		ExplicitBounds:    bounds,
	}
}

// ─── Gauge (Sub-5b) ───
//
// 当前值 — 业务 Set(n) 直接覆盖, Inc/Dec 加减. 用于 "活跃 session 数" / "in-progress
// Run 数" 等当前快照值.

// Gauge 当前值计数器 (跟 Counter 不同, 可任意 set/inc/dec).
type Gauge struct {
	name               string
	labels             []*commonpb.KeyValue
	value              atomic.Int64
	firstObservedNanos uint64
}

// Set 覆盖当前值.
func (g *Gauge) Set(v int64) {
	if g == nil {
		return
	}
	g.value.Store(v)
}

// Inc 增加 1.
func (g *Gauge) Inc() {
	if g == nil {
		return
	}
	g.value.Add(1)
}

// Dec 减 1.
//
// **可减到负值, by-design 不 clamp** (Round 8 audit CC1 决策): Gauge 语义跟
// Prometheus / OTLP IntGauge 一致允许负值. framework 计数型 Gauge (RunsInProgress /
// ActiveSessions) 设计是 Inc/Dec 严格配对 (OnAgentStart Inc ↔ OnAgentEnd Dec /
// sessionByID Inc ↔ CleanupSession Dec), 正常路径恒 >= 0. **出现负值 = Inc/Dec
// 配对 bug 的诊断信号** (e.g., 业务漏调 OnAgentStart 但调了 OnAgentEnd) — clamp 到 0
// 会掩盖 root cause, 故不 clamp. 业务 dashboard 看到 RunsInProgress < 0 应排查
// hook 接入完整性.
func (g *Gauge) Dec() {
	if g == nil {
		return
	}
	g.value.Add(-1)
}

// Value 返当前值 (test/监控用).
func (g *Gauge) Value() int64 {
	if g == nil {
		return 0
	}
	return g.value.Load()
}

// toOTLP 把 Gauge 转 OTLP DoubleDataPoint (内嵌在 DoubleGauge metric).
//
// **iter-5-P fix**: 同 Counter 改用 Double 路径让 CS-OSS Monitor Prometheus
// collector 识别. Gauge int64 → float64 (业务 Gauge 计数远小于 2^53 不丢精度).
func (g *Gauge) toOTLP(nowNanos uint64) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes:        g.labels,
		StartTimeUnixNano: g.firstObservedNanos,
		TimeUnixNano:      nowNanos,
		Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(g.value.Load())},
	}
}

// ─── metricRegistry ───
//
// per-Observability 实例的 metric 注册表. 按 (name, labels) key 复用实例.

type metricRegistry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	histograms map[string]*Histogram
	gauges     map[string]*Gauge
	// Sub-5 Round 4 audit K2 + Round 5 audit P1/P2 fix: cardinality cap 防业务高基数
	// label OOM.
	//
	// **Round 5 P1 修**: 改 **per-type cap** 不是 cross-type — 单个高基数违规 metric
	// 占满 counter cap 后不影响合规的 histogram/gauge 继续创建. 防"一个违规波及全局".
	// maxSeries 是单 type 上限 (counter / histogram / gauge 各自独立计 cap).
	// 0 = 无限制 (default 10000 via Config.MaxMetricSeries).
	//
	// **Round 5 P2 修**: droppedKeys sync.Map 跟踪已 drop 的 key, 同 key 多次调用
	// 不重复 ++ droppedSeries (业务 100 次调同 metric 失败时 dropped 仍 = 1 不是 100,
	// 业务监控真反映"被拒 series 数"非"被拒调用数").
	maxSeries     int
	droppedSeries atomic.Int64
	// droppedKeys 已 drop 的 metric key 去重 (Round 5 P2 fix), **bounded** 防无界增长
	// (Round 6 U1 fix): droppedKeysCount 跟踪 size, 达 maxSeries 上限后停止跟踪新 key
	// 直接 ++droppedSeries (高基数场景每 key 只 drop 1 次去重无意义, 内存有界优先).
	droppedKeys      sync.Map // key string → struct{}
	droppedKeysCount atomic.Int64
	// disabled 是 metric name 黑名单集（从 Config.DisabledMetrics 构建，per-category 关闭）——
	// 命中的 name 在 counter/histogram/gauge 入口直接返 nil。nil/空 = 不禁用任何 metric。
	// 只读（构造期一次性建，之后不改），无需锁。
	disabled map[string]bool
}

func newMetricRegistry(maxSeries int, disabledMetrics []string) *metricRegistry {
	var disabled map[string]bool
	if len(disabledMetrics) > 0 {
		disabled = make(map[string]bool, len(disabledMetrics))
		for _, n := range disabledMetrics {
			disabled[n] = true
		}
	}
	return &metricRegistry{
		counters:   make(map[string]*Counter),
		histograms: make(map[string]*Histogram),
		gauges:     make(map[string]*Gauge),
		maxSeries:  maxSeries,
		disabled:   disabled,
	}
}

// reachedCapForType 检指定 type 的 series cap 是否达上限 (per-type 独立). caller 必持 r.mu.
// Round 5 P1 fix: 改 per-type 让单 type 违规不波及其他 type.
func (r *metricRegistry) reachedCapForType(typeMapLen int) bool {
	if r.maxSeries <= 0 {
		return false // 0 = 无限制
	}
	return typeMapLen >= r.maxSeries
}

// recordDrop 记 cap drop 事件, 同 key 去重不重复计 (Round 5 P2 fix) + bounded
// 防 droppedKeys 无界增长 (Round 6 U1 fix).
//
// **bounded 协议**: droppedKeys 跟踪上限 = maxSeries (跟 series cap 同量级合理).
//   - droppedKeysCount < maxSeries: LoadOrStore 去重, 首次此 key 才 ++ droppedSeries
//   - droppedKeysCount >= maxSeries: 停止跟踪新 key (防无界 map), 直接 ++ droppedSeries
//     近似计数 (高基数场景每 key 只 drop 1 次去重无意义, 内存有界优先)
func (r *metricRegistry) recordDrop(key string) {
	maxKeys := r.maxSeries
	if maxKeys <= 0 {
		maxKeys = 10000 // maxSeries=0 时 series 无 cap 永不 drop, 此分支理论不到达; 兜底防御
	}
	if r.droppedKeysCount.Load() >= int64(maxKeys) {
		// droppedKeys 已达上限 — 不跟踪新 key (防无界 OOM), 直接近似计数
		r.droppedSeries.Add(1)
		return
	}
	if _, loaded := r.droppedKeys.LoadOrStore(key, struct{}{}); !loaded {
		// 仅首次 drop 此 key 时 ++ droppedSeries + ++ count
		r.droppedKeysCount.Add(1)
		r.droppedSeries.Add(1)
	}
}

// MetricCardinalityDropped 返业务高基数 label 触发 cap 后被 drop 的 series **去重**数.
// (Round 5 P2 fix: 同 key 多次调用失败仅算 1 次)
// 业务监控用 — 持续增长说明业务 metric labels 设计有问题 (e.g., 用 user_id /
// request_id 作 label 导致 cardinality 爆炸).
func (r *metricRegistry) MetricCardinalityDropped() int64 {
	if r == nil {
		return 0
	}
	return r.droppedSeries.Load()
}

// histogram 同 counter() 模式 lazy create + 复用同 series.
// buckets 仅首次创建时设, 后续 lookup 返同实例忽略 buckets 参数 (按 name+labels 复用).
func (r *metricRegistry) histogram(name string, buckets []float64, labels map[string]string) *Histogram {
	if r.disabled[name] {
		return nil // per-category 关闭（复用 nil-safe silent skip）
	}
	key := metricKey(name, labels)
	r.mu.RLock()
	if h, ok := r.histograms[key]; ok {
		r.mu.RUnlock()
		return h
	}
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[key]; ok {
		return h
	}
	// Round 5 P1+P2 fix: per-type cap + 去重 drop 计数
	if r.reachedCapForType(len(r.histograms)) {
		r.recordDrop(key)
		return nil // 业务 .Observe nil-safe
	}
	h := &Histogram{
		name:               name,
		labels:             labelsToKV(labels),
		buckets:            buckets,
		counts:             make([]uint64, len(buckets)+1), // +1 for +Inf bucket
		firstObservedNanos: uint64(time.Now().UnixNano()),
	}
	r.histograms[key] = h
	return h
}

// gauge 同款 lazy create + 复用.
func (r *metricRegistry) gauge(name string, labels map[string]string) *Gauge {
	if r.disabled[name] {
		return nil // per-category 关闭（复用 nil-safe silent skip）
	}
	key := metricKey(name, labels)
	r.mu.RLock()
	if g, ok := r.gauges[key]; ok {
		r.mu.RUnlock()
		return g
	}
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[key]; ok {
		return g
	}
	if r.reachedCapForType(len(r.gauges)) {
		r.recordDrop(key)
		return nil
	}
	g := &Gauge{
		name:               name,
		labels:             labelsToKV(labels),
		firstObservedNanos: uint64(time.Now().UnixNano()),
	}
	r.gauges[key] = g
	return g
}

// counter 按 name+labels lazy create / 复用 Counter 实例 (业务多处 Inc 同 series 同实例).
func (r *metricRegistry) counter(name string, labels map[string]string) *Counter {
	if r.disabled[name] {
		return nil // per-category 关闭：业务黑名单 metric（复用 cap 同款 nil-safe silent skip）
	}
	key := metricKey(name, labels)
	r.mu.RLock()
	if c, ok := r.counters[key]; ok {
		r.mu.RUnlock()
		return c
	}
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	// double-check 防 race
	if c, ok := r.counters[key]; ok {
		return c
	}
	if r.reachedCapForType(len(r.counters)) {
		r.recordDrop(key)
		return nil
	}
	c := &Counter{
		name:               name,
		labels:             labelsToKV(labels),
		firstObservedNanos: uint64(time.Now().UnixNano()),
	}
	r.counters[key] = c
	return c
}

// snapshot 返当前所有 metric (Counter + Histogram + Gauge) 的 OTLP Metric 列表.
// 不重置任何 metric — cumulative aggregation temporality (跟 spec 默认一致).
func (r *metricRegistry) snapshot() []*metricspb.Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.counters) == 0 && len(r.histograms) == 0 && len(r.gauges) == 0 {
		return nil
	}
	nowNanos := uint64(time.Now().UnixNano())
	out := make([]*metricspb.Metric, 0, len(r.counters)+len(r.histograms)+len(r.gauges))

	// Counter → DoubleSum (cumulative monotonic)
	//
	// **iter-5-P fix**: 原 IntSum 是 OTLP v0.7.0 deprecated 类型, CS-OSS Monitor 后端
	// 不识别 silent drop. 改 DoubleSum 跟 Histogram 走同款 double 路径让
	// Prometheus collector 识别. Counter int64 累计值 → float64 (业务量远小于
	// 2^53 不丢精度).
	counterByName := make(map[string][]*metricspb.NumberDataPoint, len(r.counters))
	for _, c := range r.counters {
		counterByName[c.name] = append(counterByName[c.name], c.toOTLP(nowNanos))
	}
	for name, dps := range counterByName {
		out = append(out, &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Sum{
				Sum: &metricspb.Sum{
					DataPoints:             dps,
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					IsMonotonic:            true,
				},
			},
		})
	}

	// Histogram → DoubleHistogram (cumulative, Sum float64 保精度)
	histByName := make(map[string][]*metricspb.HistogramDataPoint, len(r.histograms))
	for _, h := range r.histograms {
		histByName[h.name] = append(histByName[h.name], h.toOTLP(nowNanos))
	}
	for name, dps := range histByName {
		out = append(out, &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Histogram{
				Histogram: &metricspb.Histogram{
					DataPoints:             dps,
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				},
			},
		})
	}

	// Gauge → DoubleGauge (无 temporality, 当前值)
	//
	// **iter-5-P fix**: 改 DoubleGauge 跟 Counter/Histogram 走同款 double 路径让
	// CS-OSS Monitor Prometheus collector 识别 (原 IntGauge deprecated 路径 silent drop).
	gaugeByName := make(map[string][]*metricspb.NumberDataPoint, len(r.gauges))
	for _, g := range r.gauges {
		gaugeByName[g.name] = append(gaugeByName[g.name], g.toOTLP(nowNanos))
	}
	for name, dps := range gaugeByName {
		out = append(out, &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{
					DataPoints: dps,
				},
			},
		})
	}

	return out
}

// ─── helpers ───

// metricKey 按 name + sorted labels 算唯一 series key.
//
// labels 按 key 字母序排 — map 遍历顺序不定, 同 labels 不同遍历顺序应产生同 key
// 让 counter()/histogram() 复用实例.
func metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(name)
	for _, k := range keys {
		sb.WriteByte('|')
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
	}
	return sb.String()
}

// labelsToKV 把 map[string]string 转 OTLP KeyValue slice (DataPoint.Attributes).
//
// **iter-5-R OTLP v1 升级**: v0.7 用 `StringKeyValue` + `DataPoint.Labels`
// (deprecated field 1), v1 用 `KeyValue + AnyValue` + `DataPoint.Attributes`
// (新 field 7). Prometheus collector 按 v1 协议解析, 老 Labels 字段
// silent drop. 用 strKV helper 包 AnyValue.
//
// 同款按 key 字母序排保 OTLP wire 一致.
func labelsToKV(labels map[string]string) []*commonpb.KeyValue {
	if len(labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*commonpb.KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, strKV(k, labels[k]))
	}
	return out
}
