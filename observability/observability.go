package observability

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"unicode/utf8"

	"crew-go/compose_v2"
)

// attrGenAIContent 是 gen_ai_content event attribute key — spec line 141 独立
// 1MB 上限 (MaxInlineEventBytes), 不受 MaxAttributeValueChars 默认 512 约束.
// addEvent 内 special-case 跳过 value truncate.
const attrGenAIContent = "gen_ai_content"

// Observability 是 crew-go 自家 CS-OSS Monitor trace + metric 推送主对象.
//
// 业务 main 调 `New(Config)` 拿一个实例, 注入 agent:
//
//	obs, _ := observability.New(observability.Config{...})
//	defer obs.Close()
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithHooks(obs.AgentHooks()),
//	    agent_v2.WithOrchHooks(obs.OrchHooks()),
//	)
//
// 内部维护:
//   - exporter: OTLP gRPC client + batch + tick (P1 实施)
//   - sessions: 每 sessionID 一个 traceID + in-flight span registry
//     (跨 hook 派生 child span 时查 parent / child index counter)
type Observability struct {
	cfg      *Config
	exporter *exporter
	sessions sync.Map // sessionID(string) → *sessionState

	// orchFired 记「编排那半 hook 到底跑过没有」——由 agentOrchAdapter.OnGraphStart 置位。
	// halfInstallWarned 保证「只装了一半」这句话每个 obs 实例只喊一次。
	//
	// **为什么要在运行时检**：两条 hook 链的安装点**不在同一个地方**（业务级是构造期
	// Option、编排级是 ctx push 或 RunOption），所以「只装一条」在编译期看不出来，而
	// 漏装编排那条会让 8 个 metric 全漏 fire——**没有任何报错，只是数据少了一截**。
	// [Observability.Install] 把常见拓扑收成一次调用，但 a2a/server 那种「agent 在一处
	// 建、ctx 在每个请求 push」的拓扑结构上就是两个安装点，收不进去；这两个字段是给
	// 那一档兜底的。
	orchFired         atomic.Bool
	halfInstallWarned atomic.Bool
}

// sessionState 是单个 session (= 单 RunSession.RunID) 的 trace 状态.
//
// 全 Run 内同 traceID, 每个 in-flight span 按 key 存, hook End 拿出 finalize → enqueue.
type sessionState struct {
	traceID    string
	rootSpanID string
	// a2a 跨语言 trace（CS-OSS metadata propagation）：server 侧从入站 Message.metadata
	// 提取的 remote parent。非空 = 本 session 由远端 trace seed —— 顶层 span 挂
	// rootParentSpanID 下（用 remote traceID 不本地 GenTraceID），让 server 整棵 span
	// 树串到 client 的 trace。空 = 普通本地 session（走 GenTraceID + rootSpanID 原路径，
	// 行为零变化）。
	rootParentSpanID string
	seededAppName    string // remote AppName（顶层 span 派生 RPCChild spanID 的 service 前缀）
	// review P0-P2 整体 audit² 视角 B HIGH (SamplingRatio dead) fix:
	// session-level 采样决策 — 全 session 共享同 sampled flag, 全采或全 drop.
	// 一致性: 同 traceID 内不能局部采样 (前端构树 / 业务 query trace 全或全无).
	sampled bool
	spans   sync.Map // registryKey(string) → *spanEntry (in-flight)
	// childCounters 给每个 parent span ID 维护一个递增 child index —
	// 用于 GenChildSpanID(parent, idx, ...) 派生唯一 sibling spanID.
	childCounters sync.Map // parentSpanID(string) → *atomic.Int32
	// lastAccess 记录 session 最后访问 Unix 时间——**预留给未来 sweeper**（按超时清孤儿
	// session）。**当前只写不读、framework 不内置 sweeper**（见 CleanupSession godoc）：session
	// 清理靠业务显式 CleanupSession，lastAccess 待 sweeper 实现（ISSUE-90 backlog）才被消费。
	lastAccess atomic.Int64
	// review P2 audit² HIGH-A2 fix: lastError 暂存 OnError 触发时的错误信息,
	// 让随后的 *End hook 读出来填 span.status / error.type / error.message.
	// 协议: OnError 写 → 同 spanKey 的 *End 读 → 上报后清.
	// key = spanKey (kind|graphNS|name|round|callID), value = *commonError
	lastErrors sync.Map
	// P0 fix: interruptStartNanos 暂存 OnAgentEnd 检 InterruptError 时的 nanos,
	// Resume 入口 OnAgentStart 读 + 清 算 interrupt→resume duration histogram.
	// 0 = 无中断在等 (Run 正常完成或从未中断). 用 atomic 避锁热路径.
	// 协议: OnAgentEnd 写 (检 GetEndError == InterruptError); OnAgentStart 读 + Reset(0).
	// 多次 HITL Run→Resume→Run→Resume 循环时按最新中断时间覆盖 (历史中断不累计).
	interruptStartNanos atomic.Int64
	// HIGH-3 fix: dottedOrders 记每个 spanID 的 LangSmith-style dotted_order 字符串.
	// 业务 offline 分析时按字符串 sort 一遍还原完整执行 timeline (单字段同时含树路径 +
	// 起始时间). 跟 sess.spans (in-flight) 独立 — span endSpan 后 sess.spans 删但
	// dottedOrders 保留, 让 child span 在 parent end 后才 start 也能拼链 (理论边缘).
	//
	// 格式: parent_dotted + "." + {startMillis}{spanID_short_8hex}
	//   - root: 仅 self 段 (无 parent 前缀)
	//   - startMillis = StartTimeUnixNano / 1e6 (毫秒 13 位字符串可排序)
	//   - spanID_short_8hex = sid 末尾 8 字符 (去重区分同 ms 多 sid)
	//
	// 用 sync.Map 而非 plain map + mu — 读多写少 (startSpan 写, 后续 child startSpan
	// 读), 跟 sess.spans 同款 access pattern.
	//
	// CleanupSession 清 (跟 sessions 一样生命周期).
	dottedOrders sync.Map // spanID(string) → dottedOrder(string)
	// P3+P4 fix: chunkBuffers 暂存 model node 流式 chunks 让 OnNodeEnd flush 成
	// 单个 streaming_response event (不每 chunk 一个 event 造 trace UI 淹没).
	//
	// 协议:
	//   - OnNodeEvent 看 Type="chunk" → Load buffer (lazy create) → append content
	//   - OnNodeEnd → 找 buffer, 若有内容 emit "streaming_response" event 含拼接 content +
	//     chunk_count + first/last nanos, 清 buffer
	//   - CleanupSession → 清整个 sync.Map 防长跑 leak
	//
	// key 协议: registryKey(node, graphNS, nodeID, 0, "") 跟 node span 一致;
	// value: *chunkBuffer (含 mu + strings.Builder + count + nanos).
	//
	// **InterruptError 路径自动 flush**: framework error 路径 OnNodeEnd 仍 fire (传 err),
	// 所以 flush 路径覆盖 normal/interrupt/error 3 种, 不漏 partial response 可见性.
	chunkBuffers sync.Map
}

type commonError struct {
	code    string
	message string
}

// spanEntry 包 *tracepb.Span + mu — review P2 audit HIGH-2/3 fix:
// hook 并发 (e.g., OnNodeEvent fan-out / ISSUE-81 toolsFn fanout) 写
// span.Events / span.Attributes 时无锁是真 race (audit 实证 detector 100%
// 触发). 加 mu 保 Events / Attributes / Status mutate 串行.
type spanEntry struct {
	span *tracepb.Span
	mu   sync.Mutex
	// runStartNanos Phase 2 Sub-5b 加 — agent span 实际开始时间 (OnAgentStart fire 时设).
	// 用于 OnAgentEnd 算 agent.run.duration metric Observe (跟 trace span 自己的
	// StartTimeUnixNano 是同款值, 但 metric 路径不查 span proto 内字段, 单独存简单).
	// 同款下游可扩 model/tool span 各自 startNanos 算各自 duration metric.
	runStartNanos int64
	// kind / name 方案 C 加 — tool 类 span registry key 用 callID 唯一锚点不掺 kind/name
	// (见 registryKey godoc), OnToolEnd 拿到的 ToolOutput 只有 CallId 无 Name/kind, 改从
	// 此处读展示用的真实 kind (tool/skill/retrieval/...) + name (tool/skill name), 消除
	// 之前遍历 sess.spans 反查的脆弱代码.
	kind string
	name string
}

// New 创建 Observability 实例 + 启 exporter goroutine.
//
// 业务需自传 TraceEndpoint / MetricsEndpoint 才上报 (两者各自独立): endpoint 为空则对应
// 通道 exporter=nil, **静默不上报** (本地开发 / 单测).
func New(cfg Config) (*Observability, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	exp, err := newExporter(&cfg)
	if err != nil {
		return nil, err
	}
	return &Observability{
		cfg:      &cfg,
		exporter: exp,
	}, nil
}

// Counter 获取业务 metric Counter 实例 (lazy 创建 + 按 name+labels 复用).
//
// Phase 2 Sub-5a: 业务调 o.Counter(name, labels).Inc() / Add(n) 记录 metric.
//
// **opt-in 设计**: exporter 为 nil (业务无 TraceEndpoint) 时返一个 stub Counter
// (Inc 是 no-op). 业务不区分 instrumented vs 不 instrumented 路径写 hot path 代码.
//
//	c := o.Counter(observability.MetricAgentRunTotal, map[string]string{
//	    "gen_ai_agent_name": "researcher", "gen_ai_agent_kind": "base", "gen_ai_status": "success",
//	})
//	c.Inc()
func (o *Observability) Counter(name string, labels map[string]string) *Counter {
	if o == nil || o.exporter == nil || o.exporter.metricRegistry == nil {
		return nil // Counter.Inc/Add 内 nil-safe (业务无 cfg 时零开销)
	}
	// audit Sub-5a MED-5 fix: Close 后业务调 Counter() 返 nil 跟 trace Enqueue
	// 对称 — 避免业务在 registry 内继续创建 Counter 永不 export 内存泄漏 (exportLoop
	// 已退出, registry 内 Counter 不会被 GC 直到 Observability 自己被 GC).
	if o.exporter.closed.Load() {
		return nil
	}
	return o.exporter.metricRegistry.counter(name, labels)
}

// Histogram 获取 Histogram 实例 (lazy create + 按 name+labels 复用).
//
// buckets 是 Observe 值的 bucket boundaries (升序). 复用同 series 时忽略 buckets
// 参数 (按 name+labels lookup 已存在返同实例).
//
// 业务侧:
//
//	h := o.Histogram(observability.MetricAgentRunDuration,
//	    observability.BucketsLLMDuration,  // 预设 LLM 适用 bucket
//	    map[string]string{"gen_ai_agent_name": "researcher", "gen_ai_agent_kind": "base"})
//	startTime := time.Now()
//	// ... agent.Run ...
//	h.Observe(time.Since(startTime).Seconds())
func (o *Observability) Histogram(name string, buckets []float64, labels map[string]string) *Histogram {
	if o == nil || o.exporter == nil || o.exporter.metricRegistry == nil {
		return nil
	}
	if o.exporter.closed.Load() {
		return nil
	}
	return o.exporter.metricRegistry.histogram(name, buckets, labels)
}

// Gauge 获取 Gauge 实例 (lazy create + 按 name+labels 复用).
//
// 业务侧:
//
//	g := o.Gauge(observability.MetricRunsInProgress, nil)
//	g.Inc()  // Run 开始
//	defer g.Dec()  // Run 结束
func (o *Observability) Gauge(name string, labels map[string]string) *Gauge {
	if o == nil || o.exporter == nil || o.exporter.metricRegistry == nil {
		return nil
	}
	if o.exporter.closed.Load() {
		return nil
	}
	return o.exporter.metricRegistry.gauge(name, labels)
}

// MetricDroppedCount 返 metric export RPC 失败时累计 drop 的 metric data point 数.
// 业务监控时可订阅查 metric backend 健康度.
func (o *Observability) MetricDroppedCount() int64 {
	if o == nil || o.exporter == nil {
		return 0
	}
	return o.exporter.metricDropped.Load()
}

// MetricCardinalityDropped 返业务高基数 label 触发 Config.MaxMetricSeries cap 后
// 被 drop 的 series 数 (Sub-5 Round 4 audit K2 fix).
//
// **计数语义分两阶段** (Round 6 audit Z1 fix 文档化 — bounded droppedKeys 引入):
//   - 被 drop 的 unique key 数 < MaxMetricSeries 时: **精确去重计数** (同 series 反复
//     drop 只算 1 次)
//   - 被 drop 的 unique key 数 >= MaxMetricSeries 时: **近似计数** (droppedKeys 跟踪
//     达上限停止去重防无界增长 OOM, 此后含重复 — 同 series 反复 drop 会多次 ++)
//
// 业务高基数失控场景 (持续不同 user_id) 下后者占主导, dropped 值 ≈ "drop 调用总数"
// 非 "unique series 数". **但对 alerting 用途够用** — 持续增长即"label 设计有问题"信号.
//
// 业务监控用 — 持续增长说明业务 metric labels 设计有问题 (e.g., 用 user_id /
// request_id 作 label 导致 cardinality 爆炸). 建议业务 dashboard 加告警阈值
// (e.g., 跨 5min 增量 > 100 触发 alert).
func (o *Observability) MetricCardinalityDropped() int64 {
	if o == nil || o.exporter == nil || o.exporter.metricRegistry == nil {
		return 0
	}
	return o.exporter.metricRegistry.MetricCardinalityDropped()
}

// Close 优雅退出: flush exporter 剩余 span + 关 gRPC conn.
func (o *Observability) Close() error {
	if o == nil {
		return nil
	}
	if o.exporter != nil {
		return o.exporter.Close()
	}
	return nil
}

// DroppedQueueCount 返累计 queue overflow drop 数 (本机限流压力指标) —
// review P0-P2 整体 audit² 视角 A MED-A2 fix: 业务可订阅此 metric 排查
// "本机 queue 满 drop" vs "网络/collector 故障 drop".
func (o *Observability) DroppedQueueCount() int64 {
	if o == nil || o.exporter == nil {
		return 0
	}
	return o.exporter.DroppedQueueCount()
}

// DroppedExportCount 返累计 export RPC 失败 drop 数 (网络/collector 故障指标).
func (o *Observability) DroppedExportCount() int64 {
	if o == nil || o.exporter == nil {
		return 0
	}
	return o.exporter.DroppedExportCount()
}

// DroppedCount 返两种 drop 总和 (向后兼容).
func (o *Observability) DroppedCount() int64 {
	if o == nil || o.exporter == nil {
		return 0
	}
	return o.exporter.DroppedCount()
}

// session 从 ctx 抽 sessionID, 拿 (或创) 对应 sessionState.
//
// sessionID 取自 ctx (业务调 agent.Run 时 framework 通过 compose_v2.WithSessionID
// 注入). 缺失 (业务直接调 hook 测试场景) 退化空字符串.
//
// **内存生命周期**: 每个 sessionID 都会 Store sessionState (即使 !sampled — 保
// session-level 采样决策一致, 同 sessionID 多次调 session() 返同一 sampled flag).
// 长跑服务 (高 QPS 不同 sessionID) 必须定期调 CleanupSession 防 sessions map leak.
// !sampled 路径已优化: 跳过贵的 GenTraceID/GenRootSpanID (减一半成本), 仅持
// sampled=false flag (~80 byte/session) 跟 lastAccess.
func (o *Observability) session(ctx context.Context) *sessionState {
	// 主路径：带 ctx 的 remote TraceContext seed（a2a server 入口提取 metadata 后 push）。
	// 非 a2a 场景 GetTraceContext 返 IsValid()=false → seed 无效 → 走原 GenTraceID 路径。
	return o.sessionByIDSeeded(compose_v2.GetSessionID(ctx), GetTraceContext(ctx))
}

// sessionByID 是无 seed 变体（TraceID 查询 / 测试场景，无 ctx）。
func (o *Observability) sessionByID(sid string) *sessionState {
	return o.sessionByIDSeeded(sid, TraceContext{})
}

// sessionByIDSeeded 是 session 创建/获取的实际实现。seed 合法（a2a server 收到远端
// trace，见 ExtractFromMetadata）时用 remote traceID + parent seed 本 session，让
// server 整棵 span 树串到 client 的 trace；seed 非法（所有非 a2a 场景）走原 GenTraceID
// 路径，行为零变化。
func (o *Observability) sessionByIDSeeded(sid string, seed TraceContext) *sessionState {
	if v, ok := o.sessions.Load(sid); ok {
		s := v.(*sessionState)
		s.lastAccess.Store(time.Now().Unix())
		return s
	}
	// review P0-P2 整体 audit² 视角 B HIGH-SamplingRatio fix: session 创建时
	// 一次性算采样决策, 全 session 共享 (跟 OTel TraceIdRatioBased 同款 head sampling).
	// SamplingRatio=1.0 → 永远采; SamplingRatio=0.5 → 50% 概率全 session 采.
	sampled := true
	if o.cfg.SamplingRatio < 1.0 {
		sampled = rand.Float64() < o.cfg.SamplingRatio //nolint:gosec // 采样判定不需密码学安全
	}
	// a2a 跨进程：remote 已做采样决策 → 继承（父采子必采，否则父子采样不一致断链）。
	if seed.IsValid() {
		sampled = seed.Sampled
	}
	s := &sessionState{sampled: sampled}
	// P2.6 audit MED-2 fix: !sampled 时跳过贵的 traceID/rootSpanID 派生 (字符串
	// 拼接 + AppName format + crypto/rand 16-byte). 减一半 session() 成本.
	if sampled {
		if seed.IsValid() {
			// a2a server seed：用 remote traceID（不本地生成）+ remote SpanID 作顶层
			// parent。rootSpanID 留空 —— 本进程无独立 root span，顶层 span 挂 rootParentSpanID。
			s.traceID = seed.TraceID
			s.rootParentSpanID = seed.SpanID
			s.seededAppName = seed.AppName
		} else {
			s.traceID = GenTraceID(o.cfg.Resource.AppName)
			s.rootSpanID = GenRootSpanID()
		}
	}
	s.lastAccess.Store(time.Now().Unix())
	actual, loaded := o.sessions.LoadOrStore(sid, s)
	// Phase 2 Sub-5f: gen_ai.active.sessions Gauge ++ 仅新创建路径 (loaded=false).
	// 并发 LoadOrStore 同 sid 时 LoadOrStore 内部 race-free 只一个 win, 我们按 loaded
	// 区分新 / 已存在确保 Gauge ++ 精准.
	if !loaded {
		if g := o.Gauge(MetricActiveSessions, nil); g != nil {
			g.Inc()
		}
	}
	return actual.(*sessionState)
}

// nextChildIdx 给 parent span ID 派生下一个 child index (1-based, atomic 递增).
func (s *sessionState) nextChildIdx(parentSpanID string) int {
	v, _ := s.childCounters.LoadOrStore(parentSpanID, &atomic.Int32{})
	return int(v.(*atomic.Int32).Add(1))
}

// CleanupSession 业务侧主动清孤儿 session state (长跑服务 sweeper 调).
//
// 跟 HandoffAgent.CleanupSession 同款协议 —
// 业务 session 完成或超时后调, 防 sessions map 持续增长.
//
// **⚠️ 长跑服务必调** (Sub-5 Round 4 audit K1 + Round 5 audit P3 fix):
// 业务高 QPS 不同 sessionID 不断涌入时 sessions map **无 sweeper 自动清理**,
// 业务忘调本 helper 会导致内存持续增长.
//
// **单 sessionState 实际大小** (Round 5 P3 修正之前数字夸大): sampled session 含
// 5 个 sync.Map (spans / childCounters / lastErrors / compactStartNanos / cpStartNanos)
// + traceID/rootSpanID string + atomic.Int64 lastAccess. **空 sessionState ~700-1KB**
// (sync.Map shard 8 个 buckets + struct overhead), 业务高 QPS 业务 session 内 in-flight
// span 多时单 session 可达数 KB. unsampled session 跳过 traceID 派生但 sync.Map 仍占
// (~400-600 bytes). 长跑服务 leak 量级跟"业务实际 in-flight span 数"挂钩, 不是
// hardcoded 数字 — 业务侧用 MetricActiveSessions Gauge 观测 leak 进度更准.
//
// **业务监控 leak 进度**: 观察 `MetricActiveSessions` Gauge 持续增长无下降 →
// 业务 CleanupSession 调用漏. 推荐业务 Run/Resume 入口配 `defer obs.CleanupSession(sid)`
// 或独立 sweeper goroutine 按 lastAccess 超时(framework 当前不内置).
func (o *Observability) CleanupSession(sessionID string) {
	if o == nil {
		return
	}
	// Phase 2 Sub-5f: gen_ai.active.sessions Gauge -- 仅 LoadAndDelete 真存在路径
	// (LoadAndDelete 返 loaded=false 时 sid 本就不存在, 不重复 Dec).
	if v, loaded := o.sessions.LoadAndDelete(sessionID); loaded {
		// P3 fix: 清 chunkBuffers 防长跑 / 高 QPS leak. sessionState 删后 chunk buffer
		// 也得清 (各 LLM response 几 KB - 100KB, 高 QPS × N sessions 累积可观).
		ss := v.(*sessionState)
		ss.chunkBuffers.Range(func(k, _ any) bool {
			ss.chunkBuffers.Delete(k)
			return true
		})
		// HIGH-3 fix: 清 dottedOrders 防 leak (每 span 1 string ~32 bytes, 高 QPS × N
		// sessions × M spans/session 累积). sessionState 删后跟着清.
		ss.dottedOrders.Range(func(k, _ any) bool {
			ss.dottedOrders.Delete(k)
			return true
		})
		if g := o.Gauge(MetricActiveSessions, nil); g != nil {
			g.Dec()
		}
	}
}

// ActiveSessions 返当前活跃 session 数（sessions map 当前条目数）。业务监控长跑 server
// 是否 session 泄漏（高 QPS 不同 sid 涌入 + 漏 CleanupSession → map 无限增长 OOM）：
// 对齐 gen_ai.active.sessions Gauge（Gauge 异步推后端，本方法供进程内同步查询 / 告警 /
// 测试）。a2a server 场景由 executor 在 conclude 自动清理，业务无需自管。
func (o *Observability) ActiveSessions() int {
	if o == nil {
		return 0
	}
	n := 0
	o.sessions.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// TraceIDFor 返指定 sessionID 对应的 OTLP TraceID 字符串.
//
// **用途**: 业务 (e.g., plan_chat_web SSE) 想把真 TraceID echo 到前端让用户复制到
// 监控控制台搜 trace, 必须能拿到 OTLP 协议层的真 TraceID (格式
// `{appname}^^{32hex}|{millis}`) — 跟 SessionID (= RunID, UUID) 是两套 ID 不能混.
//
// **生命周期**: TraceID 在 sessionByID(sid) 首次创建 sessionState 时生成, 整个 sid
// 生命周期共享同一 TraceID. CleanupSession 后 sessionState 删, TraceID 永久丢
// (不持久化到磁盘).
//
// **嵌套场景**: nested sub-agent 通过 ctx 继承父 sid → 共享父 sessionState →
// 共享父 TraceID. 业务想拿嵌套子 agent 的"独立 traceID" 没意义 — trace tree 跨子
// agent 共享是设计意图.
//
// **空返值场景**:
//   - o == nil
//   - sessionID == "" (调本 helper 前业务忘填 SID)
//   - sessionState !sampled (SamplingRatio < 1.0 抽到的 0% 路径 traceID 字段为空)
//
// caller 拿到空字符串时 fallback 给业务"trace 未上报 / 已采样掉"提示, 不要把空当
// "出错" 处理.
//
// **lazy create 协议**: 业务想在第一个 hook fire 之前 (e.g., SSE 推 session_started
// event) 拿 traceID 时, 本 helper 调 sessionByID 会主动创建 sessionState +
// 生成 traceID. 后续 hook fire 调 sessionByID 拿同一实例 traceID 一致 — 用户从 SSE
// echo 复制 traceID 到监控平台跳转能命中实际 spans.
func (o *Observability) TraceIDFor(sessionID string) string {
	if o == nil || sessionID == "" {
		return ""
	}
	return o.sessionByID(sessionID).traceID
}

// sweepInFlight 兜底 finalize sessionState 内剩余 in-flight span — 用于
// OnAgentEnd / OnGraphEnd 时清理 model/tool 等 *End 不 fire (error path) 的残留.
//
// review P2 audit² HIGH-A3 fix: model/tool error 路径 compose_v2 `return nil, err`
// 跳过 OnModelEnd/OnToolEnd, 让 sessions.spans 内 span 永留泄漏直到 CleanupSession.
// agent/graph End 时 sweep 兜底 enqueue.
//
//   - keyPrefix 过滤范围 (e.g., "agent|g|" 只清这 agent 下面的, "graph|g|" 清整 graph)
//   - outerStatus/Type/Msg: caller (OnGraphEnd) 已 mapErrToStatus(graph err) — 个体
//     span 若没 lastError (OnError 没 fire) 走 outer; 有 lastError 用个体的.
//
// audit² D-HIGH-6 fix: 之前硬编码 status="error" 让 HITL InterruptError 场景所有
// in-flight model/tool span 都被强标 error (前端 UI 全红), 但 graph span 走
// mapErrToStatus 是 "blocked" — 不对称. 改 outer 参数 caller 算好传.
func (o *Observability) sweepInFlight(ctx context.Context, keyPrefix string, outerStatus, outerErrType, outerErrMsg string) {
	if o == nil || o.exporter == nil {
		return
	}
	sess := o.session(ctx)
	var toEnqueue []*tracepb.Span
	sess.spans.Range(func(k, v any) bool {
		key := k.(string)
		if len(key) < len(keyPrefix) || key[:len(keyPrefix)] != keyPrefix {
			return true
		}
		// 同 key 找 lastError 取错误信息 (per-span OnError 路径)
		var indvErrType, indvErrMsg string
		if errEntry, ok := sess.lastErrors.LoadAndDelete(key); ok {
			ce := errEntry.(*commonError)
			indvErrType = ce.code
			indvErrMsg = ce.message
		}
		// audit² D-HIGH-6 fix: status 优先级 — individual lastError > outer (graph err) > 默认 "error"
		status := outerStatus
		if status == "" {
			status = "error" // 兜底
		}
		errType := coalesce(indvErrType, outerErrType, "InFlightSweep")
		errMsg := coalesce(indvErrMsg, outerErrMsg, "span 未正常 End — *End hook 在 error 路径未 fire, sweep 兜底")
		// **redact GAP 修复**：sweep 独立写 error.message + Status.Message（不经 endSpan），
		// outerErrMsg 来自 mapErrToStatus(OnGraphEnd) 的 err.Error() 不脱敏——统一 redact 一次
		// 覆盖下方两处写入（同 endSpan，redactString 幂等安全）。
		errMsg = o.redactString(errMsg)
		entry := v.(*spanEntry)
		entry.mu.Lock()
		entry.span.EndTimeUnixNano = uint64(time.Now().UnixNano())
		// P2.6 audit HIGH-2 fix: 走 applyAttrLimit (cap + truncate)
		o.applyAttrLimit(entry.span, []*commonpb.KeyValue{
			strKV("gen_ai.status", status),
			strKV("error.type", errType),
			strKV("error.message", errMsg),
		})
		// OTel Status.code: success / blocked 都映 OK, 其他映 ERROR (spec line 74)
		statusCode := tracepb.Status_STATUS_CODE_ERROR
		if status == "success" || status == "blocked" {
			statusCode = tracepb.Status_STATUS_CODE_OK
		}
		entry.span.Status = &tracepb.Status{Code: statusCode, Message: errMsg}
		// review P0-P2 整体 audit² 视角 D HIGH-D1 fix: 同 endSpan 深拷贝防 race
		snap := cloneSpan(entry.span)
		toEnqueue = append(toEnqueue, snap)
		entry.mu.Unlock()
		sess.spans.Delete(k)
		return true
	})
	for _, sp := range toEnqueue {
		o.exporter.Enqueue(sp)
	}
}

// coalesce 返第一个非空 string, 都空返 "".
func coalesce(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// 注: 之前的 recordLastError(kind, name, round, callID, ...) helper 在 P2.5 audit²
// HIGH-D2 fix 后删除——OnError 改成直接遍历 sessions.spans 用真实 spanKey 写
// lastError (避免 callID="" 跟真 callID 不匹配 leak).

// applyAttrLimit 把 newAttrs append 到 span.Attributes, 按 Config 限制:
//   - key 超 MaxAttributeKeyChars 截断
//   - value (string) 超 MaxAttributeValueChars 截断
//   - 总 attr 数超 MaxSpanAttributes 时 drop 后续 + 累加 DroppedAttributesCount
//
// review P0-P2 整体 audit² 视角 B HIGH (6 字段 dead) fix: MaxSpanAttributes /
// MaxAttributeKeyChars / MaxAttributeValueChars 之前完全 dead, 业务设了无效.
// 现统一在此 helper 内 enforce, startSpan/endSpan/addEvent 都走.
//
// **P2.6 audit LOW-3 注意**: 本函数 *mutate* 入参 KeyValue (a.Key / sv.StringValue
// 原地改). 框架内 caller 都用 strKV() 每次新建 KeyValue 不踩. 业务自定义 hook 内
// 若复用同一 *commonpb.KeyValue 实例 (e.g., 缓存复用) 会被悄改 truncated 值. 业务
// 自定义 attr 应每次新建实例传入.
//
// caller 持 entry.mu 才安全调本函数.
func (o *Observability) applyAttrLimit(span *tracepb.Span, newAttrs []*commonpb.KeyValue) {
	if o == nil {
		return
	}
	maxAttrs := o.cfg.MaxSpanAttributes
	maxKey := o.cfg.MaxAttributeKeyChars
	maxVal := o.cfg.MaxAttributeValueChars
	for _, a := range newAttrs {
		if a == nil {
			continue
		}
		// total cap
		if maxAttrs > 0 && len(span.Attributes) >= maxAttrs {
			span.DroppedAttributesCount++
			continue
		}
		// key truncate
		if maxKey > 0 && len(a.Key) > maxKey {
			a.Key = truncString(a.Key, maxKey)
		}
		// value (string) truncate — int/double/bool 没长度限制
		if maxVal > 0 {
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok && len(sv.StringValue) > maxVal {
				sv.StringValue = truncString(sv.StringValue, maxVal)
			}
		}
		span.Attributes = append(span.Attributes, a)
	}
}

// truncString 头尾保留中间截断 (...). 长 string 头尾各保 (max-3)/2 char + "...".
// e.g., max=10, s="abcdefghijklmnop" → "abcd...mnop"
//
// P2.6 audit LOW-1 fix: max < 7 时 half=(max-3)/2 退化到 0/1, 给业务设极小值
// MaxAttributeValueChars=4 时 "..." 全丢信息; max=5/6 也只剩 1-2 字符信息. 改成
// max < 7 直接 s[:max] 头切保留前缀 (不加 "..." 因为 "..." 占 3 char 就剩不下了).
//
// audit² A-MED-2 fix: head + tail 按 byte 切, 多字节字符 (中文 3 byte / emoji 4 byte)
// 在边界处被切成无效 UTF-8 — 业务 agent.Name="数据分析师" 中文 → cap 后 invalid UTF-8
// 前端展示崩. 头切用 safeHeadCut, 尾切用 safeTailCut 回退到 rune 完整边界.
func truncString(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	if max < 7 {
		return safeHeadCut(s, max)
	}
	half := (max - 3) / 2
	return safeHeadCut(s, half) + "..." + safeTailCut(s, half)
}

// safeHeadCut 取 s 前 n byte 保 UTF-8 完整 — 若 n 落在 rune 中间回退到上一 rune 边界.
func safeHeadCut(s string, n int) string {
	if n >= len(s) {
		return s
	}
	if n < 0 {
		return ""
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// safeTailCut 取 s 后 n byte 保 UTF-8 完整 — 若起始落 rune 中间前进到下一 rune 边界.
func safeTailCut(s string, n int) string {
	if n >= len(s) {
		return s
	}
	if n <= 0 {
		return ""
	}
	start := len(s) - n
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

// ─────────────── 内部 span 构造 helper ───────────────

// registryKey 是 in-flight span registry (sess.spans) 的**统一** key 入口 — 所有
// Store / Load / LoadAndDelete / Range 反查都应走它, 不直接拼 key.
//
// **根因设计 (对齐 OTel span 句柄 / run_id 范式)**: callID 是 tool_call
// 全局唯一锚点 (tool_call ↔ tool_result 配对), 非空即足以唯一标识 span — 此时
// kind (tool/skill/retrieval/embedding/guardrail 5 个展示分类) 与 name
// 都**不进 key**, 避免 OnToolStart / OnToolEnd 两处分别算 kind 写不一致导致反查
// 失配 (eaa3fdf HIGH-1 根因: OnToolEnd 硬编码 endSpan(ctx, "tool", ...) 跟 skill/
// retrieval 等 kind 存的 key 不匹配 → span 不被正常 end). kind / name 降级为
// spanEntry 展示字段, OnToolEnd 直接 Load(callID) 拿 entry 读, 不再遍历反查.
//
// callID 为空 (graph/agent/model/node span 都无 callID): 退回 spanKey 用
// (kind, graphNS, name, round) — 这些 span 无唯一 id, 且 agent vs chat 同 name 时
// 靠 kind 区分 (合理, 见 startSpan node.id 派生).
func registryKey(kind, graphNS, name string, round int32, callID, instanceID string) string {
	if isToolKind(kind) {
		// tool 类: **始终**归一 "tool|graphNS|callID" — 不依赖 callID 是否非空, 让 sweep 单
		// "tool|" 前缀的假设恒成立 (callID 空时若回退 spanKey 用真实 kind 前缀如 "retrieval|"
		// 会让 sweep "tool|" 前缀漏清 → 泄漏, f823ff9 回归). callID 非空时 "tool|graphNS|callID"
		// 全局唯一; 空时退化 "tool|graphNS|" (协议违反场景, 多个空 callID 冲突覆盖, 但 sweep
		// 仍能清不泄漏). callID 是 tool_call 唯一锚点, kind/name 不进 key (降级为 spanEntry 字段).
		return "tool|" + graphNS + "|" + callID
	}
	if kind == "node" && instanceID != "" {
		// node 类**且**是 SendFrom 派生的并发实例 (ISSUE-96 侧面 6): nodeID 在一个超步
		// 里可能有 N 份同时在跑 —— 只用 (kind, graphNS, nodeID) 会让 N 个实例落同一个槽,
		// 后开的 span 覆盖先开的 (被覆盖那些永远 end 不掉 → 漏导出 + 占 registry 到 sweep),
		// 先结束的实例还会把后开那个 end 掉 → duration 张冠李戴.
		//
		// **只加在 node 类**: agent/model 靠 childNS 隔离且 OnError sweep 按 exact match /
		// prefix 认它们的 key (加维度会打破匹配); tool 有 callID 全局唯一锚点. 静态边节点
		// instanceID 恒空 → key 形态与引入前完全一致, 不惊动既有消费者.
		return spanKey(kind, graphNS, name, round, "") + "|" + instanceID
	}
	// 非 tool 类 (graph/agent/model/node, 都无 callID): 用 (kind, graphNS, name, round) —
	// 无唯一 id, 且 agent vs chat 同 name 时靠 kind 区分 (见 startSpan node.id 派生).
	return spanKey(kind, graphNS, name, round, "")
}

// isToolKind 判断是否 tool 类 span — registry key 归一到 "tool|" 前缀的 kind 集合.
// tool / skill / retrieval / embedding / guardrail 都由 tool_call 触发,
// 共享 callID 锚点 (OnToolStart/OnToolEnd 配对), span kind 只是 trace UI 展示分类.
func isToolKind(kind string) bool {
	switch kind {
	case "tool", "skill", "retrieval", "embedding", "guardrail":
		return true
	}
	return false
}

// spanKey 派生无 callID 的 span (graph/agent/model/node) registry key — 仅 registryKey
// 在 callID 为空时内部调用. **不要**直接用它做 sess.spans 操作, 统一走 registryKey.
func spanKey(kind, graphNS, name string, round int32, callID string) string {
	var b strings.Builder
	b.WriteString(kind)
	b.WriteByte('|')
	b.WriteString(graphNS)
	b.WriteByte('|')
	b.WriteString(name)
	if round > 0 {
		b.WriteByte('|')
		b.WriteString(fmt.Sprintf("r%d", round))
	}
	if callID != "" {
		b.WriteByte('|')
		b.WriteString(callID)
	}
	return b.String()
}

// startSpan 构造 in-flight span 入 registry, 返回 spanEntry 供 caller 设 attr.
//
//   - kind/name/round/callID 组合成 unique registry key
//   - parentSpanID 决定 child index 派生 (OTel byte-level)
//   - parentNodeID 是 CS-OSS 前端构树用的字符串 parent (跟 OTel ParentSpanId 独立)
//   - operation.name 按 spec line 37-46 必填
//
// review P2 audit MED-5 fix: graph span (kind=graph + name==graphNS) 的
// node.id 应等于 graphNS 不拼 "graphNS/graphNS" (前端构树时 graph 与 children
// 父子关系会断).
//
// audit² C-HIGH-2 fix: node.id 按 kind 区分避免 agent + chat 同 name 时 node.id
// 重叠 (前端构树扁平). spec line 50/65/66 协议:
//   - graph: nodeID = graphNS
//   - node:  nodeID = graphNS+"/"+name[+"#"+instanceID]  (业务自定义节点
//     OnNodeStart; 实例后缀只在 SendFrom 派生的并发实例上出现——同 NodeID 一个超步
//     里可能 N 份同时在跑, spec 要求 node.id 同一 trace 内唯一)
//   - agent: nodeID = graphNS+"/agent/"+name
//   - model: nodeID = parentNodeID+"/model/r"+round   (chat 嵌入 agent 下)
//   - tool:  nodeID = parentNodeID+"/tool/"+name+"/"+callID
func (o *Observability) startSpan(ctx context.Context,
	kind, operationName, spanName, name string, round int32, callID string,
	parentSpanID string, parentNodeID string, attrs []*commonpb.KeyValue,
) *spanEntry {
	if o == nil || o.exporter == nil {
		return nil // exporter 未启时不上报
	}
	sess := o.session(ctx)
	// review P0-P2 整体 audit² 视角 B HIGH-SamplingRatio fix: session 不采样直接返
	// nil entry, 后续 endSpan/addEvent Load miss 短路 (sessions.spans 也不 Store
	// 让 span 不创建零开销).
	if !sess.sampled {
		return nil
	}
	graphNS := compose_v2.GetGraphNS(ctx)

	// 派生 spanID:
	//   - kind=graph 或 kind=agent + 顶层 (parentSpanID=""): 用 rootSpanID 作 trace
	//     的 root span. **互斥不冲突**: agent_v2.Run 路径下 OnGraphStart 跳过创 graph
	//     span (hooks.go OnGraphStart 检 in-flight agent 跳过), 只 agent 用 rootSpanID;
	//     业务直接 compose_v2.Run 不走 agent_v2: 只 graph 用 rootSpanID, 无 agent.
	//     设计: agent 是用户接口, agent 作 root; graph 是 framework
	//     实现细节 trace 不暴露 (顶层时).
	//   - 其他 kind (model/tool/skill/node) parentSpanID="" 兜底: 挂 rootSpanID 下
	//     作 child 派生新 spanId. 正常路径这些 kind 都有显式 parent, 此分支是异常 fallback.
	//   - parentSpanID 非空 (常规): 正常派生 child spanID.
	var sid string
	switch {
	case parentSpanID == "" && (kind == "graph" || kind == "agent"):
		if sess.rootParentSpanID != "" {
			// a2a server seed 场景：顶层 span 挂 remote parent，派生本地 RPCChild spanID
			//（带 remote service 前缀，对应协议"下游收到 propagation 后派生第一个 span"）。
			idx := sess.nextChildIdx(sess.rootParentSpanID)
			sid = GenRPCChildSpanID(sess.rootParentSpanID, sess.seededAppName, idx)
			parentSpanID = sess.rootParentSpanID
		} else {
			sid = sess.rootSpanID // 原路径（非 a2a，本进程独立 root span）
		}
	case parentSpanID == "":
		// fallback: 其他 kind 没显式 parent 时, 挂 rootSpanID 下作 child
		idx := sess.nextChildIdx(sess.rootSpanID)
		sid = GenChildSpanID(sess.rootSpanID, idx, true)
		parentSpanID = sess.rootSpanID
	default:
		idx := sess.nextChildIdx(parentSpanID)
		sid = GenChildSpanID(parentSpanID, idx, true)
	}

	// audit² C-HIGH-2 fix: kind-aware node.id 派生
	// Phase 1 fix: 加 skill / retrieval / embedding / guardrail 4 个新 kind,
	// 跟 langfuse OBSERVATION_TYPE 同款 trace UI 标识. nodeID 路径同款拼接.
	var nodeID string
	switch kind {
	case "graph":
		nodeID = graphNS
	case "agent":
		nodeID = graphNS + "/agent/" + name
	case "model":
		nodeID = parentNodeID + "/model/r" + fmt.Sprintf("%d", round)
	case "tool":
		nodeID = parentNodeID + "/tool/" + name + "/" + callID
	case "skill":
		// activate_skill 工具调用 skill 后的逻辑实体, name=skill name (业务定义).
		nodeID = parentNodeID + "/skill/" + name + "/" + callID
	case "retrieval":
		// RAG 检索 tool, name=tool name (业务自定义 vector_search / faq_lookup 等).
		nodeID = parentNodeID + "/retrieval/" + name + "/" + callID
	case "embedding":
		// 向量化 tool, name=tool name.
		nodeID = parentNodeID + "/embedding/" + name + "/" + callID
	case "guardrail":
		// 安全护栏 tool, name=tool name.
		nodeID = parentNodeID + "/guardrail/" + name + "/" + callID
	default: // "node" (业务自定义) 跟 fallback
		nodeID = graphNS + "/" + name
		// SendFrom 派生的并发实例要带上身份 (ISSUE-96 侧面 6 的导出侧): spec 要求
		// gen_ai.node.id **同一次 trace 内唯一**, 而同一 nodeID 在一个超步里可能有 N
		// 份同时在跑 —— 不加维度时 N 个兄弟 span 的 node.id 全相同, 按 node.id /
		// node.parent_id 字符串建执行树的前端会把它们塌成一个节点 (OTel 原生 SpanId 和
		// crew.dotted_order 各自唯一, 故只影响这一条路径, 无数据丢失).
		//
		// 与 registryKey 的实例维度同款约定: **只在非空时加**, 静态边节点 instanceID
		// 恒空 → 形态与引入前完全一致。此处只是展示 id, 不是配对锚点 —— findParentSpanID
		// 的两个 parentKind 只有 agent/graph, findActiveAgentSpan 只反查 agent 类,
		// 没有任何路径拿 node 的 node.id 做配对。
		if iid := compose_v2.GetCurrentInstanceID(ctx); iid != "" {
			nodeID += "#" + iid
		}
	}
	nodeParentID := parentNodeID

	now := uint64(time.Now().UnixNano())
	// OTel GenAI SpanKind (2026 Q2 对齐): model/chat = CLIENT (调外部 LLM API), 本地
	// agent/graph/tool/node 等 = INTERNAL. AI_TRACE_SPEC 未定义 SpanKind, 加性不破坏
	// CS-OSS Monitor (前端用 operation.name 分类). 注: A2A RemoteAgent (消费远程 agent)
	// 语义应为 CLIENT, 但它走 kind="agent" 暂标 INTERNAL — 待 a2a 层透传 remote 信号再细分.
	spanKindEnum := tracepb.Span_SPAN_KIND_INTERNAL
	if kind == "model" {
		spanKindEnum = tracepb.Span_SPAN_KIND_CLIENT
	}
	span := &tracepb.Span{
		TraceId:           []byte(sess.traceID),
		SpanId:            []byte(sid),
		ParentSpanId:      []byte(parentSpanID),
		Name:              spanName,
		Kind:              spanKindEnum,
		StartTimeUnixNano: now,
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_UNSET},
	}
	// review P0-P2 整体 audit² 视角 B fix: 走 applyAttrLimit 让 MaxSpanAttributes
	// / Key / Value 限制 enforce. 框架内置 attr (operation/node.id/parent_id/name +
	// request.id/session.id) 在最前 — 业务自定义 attrs 后塞, 超 cap 时 drop 业务的.
	builtins := []*commonpb.KeyValue{
		strKV("gen_ai.operation.name", operationName),
		strKV("gen_ai.node.id", nodeID),
		strKV("gen_ai.node.parent_id", nodeParentID),
		strKV("gen_ai.node.name", name),
	}
	// AI_TRACE_SPEC 公共 attr (thread/run 模型): request.id = 一次 run
	// (= GetSessionID = RunID, 总 emit); session.id = 跨 run 会话 (= GetThreadID,
	// 业务 WithThreadIDCtx push 时才 emit, 不造无意义的 = request.id 重复).
	if reqID := compose_v2.GetSessionID(ctx); reqID != "" {
		builtins = append(builtins, strKV("gen_ai.request.id", reqID))
	}
	if sessID := compose_v2.GetThreadID(ctx); sessID != "" {
		builtins = append(builtins, strKV("gen_ai.session.id", sessID))
	}
	// HIGH-3 fix: dotted_order 编码"树路径 + 起始时间", 业务 offline 分析按字符串
	// sort 还原执行 timeline 不需 client 重建树. LangSmith 同款设计 (run_trees.py:272).
	// 格式: parent_dotted + "." + {startMillis}{spanID_short_8hex}
	//   - 根 span (kind=graph + 无 parent): 仅 self 段
	//   - 其他: 拼父 dotted (父已 startSpan 写入 sess.dottedOrders)
	// parent 找不到 (理论边缘) → 退化为 self-only.
	selfSeg := fmt.Sprintf("%013d%s", now/1e6, shortSpanID(sid))
	var dottedOrder string
	if parentSpanID != "" {
		if parentDottedV, ok := sess.dottedOrders.Load(parentSpanID); ok {
			dottedOrder = parentDottedV.(string) + "." + selfSeg
		} else {
			dottedOrder = selfSeg
		}
	} else {
		dottedOrder = selfSeg
	}
	sess.dottedOrders.Store(sid, dottedOrder)
	builtins = append(builtins, strKV("crew.dotted_order", dottedOrder))

	o.applyAttrLimit(span, builtins)
	o.applyAttrLimit(span, attrs)
	entry := &spanEntry{span: span, kind: kind, name: name}
	sess.spans.Store(registryKey(kind, graphNS, name, round, callID, compose_v2.GetCurrentInstanceID(ctx)), entry)
	return entry
}

// shortSpanID 取 spanID 末尾 8 char (跳过 "|" 分隔符前的 catalogue 部分, 只用
// 16hex random 后 8 位区分同 ms 多 sid).
// 输入: "1.2|abcdef0123456789" → 输出: "23456789"
// 边缘: "|abc" / 无 "|" → 退化全用.
func shortSpanID(sid string) string {
	idx := strings.LastIndex(sid, "|")
	if idx < 0 || idx == len(sid)-1 {
		return sid
	}
	suffix := sid[idx+1:]
	if len(suffix) > 8 {
		return suffix[len(suffix)-8:]
	}
	return suffix
}

// endSpan 取出 in-flight span, finalize + enqueue.
//
//   - status: "success" / "error" / "cancelled" / "blocked" / "timeout"
//   - 失败时填 error.type / error.message (errMsg 非空)
//   - extraAttrs: hook 完成时拿到的额外字段 (e.g., OnModelEnd 的 usage tokens)
func (o *Observability) endSpan(ctx context.Context,
	kind, name string, round int32, callID string,
	status, errType, errMsg, fallbackErrType, fallbackErrMsg string, extraAttrs []*commonpb.KeyValue,
) {
	if o == nil || o.exporter == nil {
		return
	}
	sess := o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	key := registryKey(kind, graphNS, name, round, callID, compose_v2.GetCurrentInstanceID(ctx))
	v, ok := sess.spans.LoadAndDelete(key)
	if !ok {
		return // Start 没触发 / 已 End 过
	}
	entry := v.(*spanEntry)

	// review P2 audit² HIGH-A2 fix: 若 OnError 之前 fire 写过 lastError, 优先用
	// lastError 内容覆盖 *End hook 默认 "success" status. 业务 model_call_error /
	// tool_call_error 等场景 OnError fire 但 *End 仍 fire (defer 兜底), 之前硬编码
	// "success" 让 trace UI 全绿掩盖真错误.
	if errEntry, ok := sess.lastErrors.LoadAndDelete(key); ok {
		ce := errEntry.(*commonError)
		if status == "success" { // 仅在 caller 没明示其他 status 时覆盖
			status = "error"
		}
		if errType == "" {
			errType = ce.code
		}
		if errMsg == "" {
			errMsg = ce.message
		}
	}
	// 顶层编排错误兜底: 7 编排非通信主体错误 (MaxIterations / build graph 失败 / 子 agent
	// 冒泡) 不 fire OnError → lastError 空 → 之前 status=error 但 error.message 落空.
	// 优先级 传入 (该层显式错误 graph/node/tool) > lastError (OnError 记的底层精确错误) >
	// fallback (顶层 retErr, OnAgentEnd 经 mapErrToStatus 分类传入). framework 顶层
	// 兜底自记错误, 承担"框架已知错因主动填 message"职责.
	if errType == "" {
		errType = fallbackErrType
	}
	if errMsg == "" {
		errMsg = fallbackErrMsg
	}

	// review P2 audit HIGH-3 fix: 持 mu 序列化对 Events/Attributes/Status 的
	// mutate. addEvent 并发场景 race detector 实证触发.
	entry.mu.Lock()
	span := entry.span
	span.EndTimeUnixNano = uint64(time.Now().UnixNano())

	// review P0-P2 整体 audit² 视角 B fix: 全部走 applyAttrLimit enforce limit
	finalAttrs := []*commonpb.KeyValue{strKV("gen_ai.status", status)}
	if extraAttrs != nil {
		finalAttrs = append(finalAttrs, extraAttrs...)
	}
	if errType != "" {
		// **redact GAP 修复**：error.message 写前统一过 redactString——mapErrToStatus 返回
		// 的 err.Error()（编排级错误 OnAgentEnd/OnGraphEnd/OnNodeEnd 路径）不脱敏，B-HIGH-B1
		// fix 只补了 OnError 通信路径、漏这三条 → 业务配了 RedactPatterns 仍让含 PII 的错误
		// 明文上 trace。此处单点 redact 覆盖 endSpan 所有调用；redactString 幂等（已
		// [REDACTED] 再跑无变化），OnError 路径已 redact 的内容双跑亦安全。
		finalAttrs = append(finalAttrs,
			strKV("error.type", errType),
			strKV("error.message", o.redactString(errMsg)),
		)
	}
	o.applyAttrLimit(span, finalAttrs)

	// OTel Status.code: 非 success / blocked 都是 ERROR (spec line 74 blocked
	// 业务可选 OK/ERROR, 我们默认走 OK 让 trace UI 不标红).
	switch status {
	case "success", "blocked":
		span.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK}
	default:
		span.Status = &tracepb.Status{
			Code:    tracepb.Status_STATUS_CODE_ERROR,
			Message: errMsg,
		}
	}
	// review P0-P2 整体 audit² 视角 D HIGH-D1 fix: 深拷贝 span 给 exporter, 防
	// race window — addEvent 内 spans.Load 后到 entry.mu.Lock 之间, 我们 LoadAndDelete
	// + Unlock + Enqueue 可能并发. exporter goroutine 序列化时 addEvent 仍能 append
	// span.Events. 深拷贝隔离 — 业务后续 addEvent 改原 span 不影响 exporter 副本.
	snap := cloneSpan(span)
	entry.mu.Unlock()

	o.exporter.Enqueue(snap)
}

// cloneSpan 深拷贝 *tracepb.Span (TraceID/SpanID 等 byte slice + Attributes/Events
// slice 都深拷贝). caller 拿 entry.mu 持锁期间调.
func cloneSpan(s *tracepb.Span) *tracepb.Span {
	if s == nil {
		return nil
	}
	c := &tracepb.Span{
		TraceId:                bytesClone(s.TraceId),
		SpanId:                 bytesClone(s.SpanId),
		ParentSpanId:           bytesClone(s.ParentSpanId),
		Name:                   s.Name,
		Kind:                   s.Kind,
		StartTimeUnixNano:      s.StartTimeUnixNano,
		EndTimeUnixNano:        s.EndTimeUnixNano,
		DroppedAttributesCount: s.DroppedAttributesCount,
		DroppedEventsCount:     s.DroppedEventsCount,
		DroppedLinksCount:      s.DroppedLinksCount,
	}
	if s.Status != nil {
		c.Status = &tracepb.Status{Code: s.Status.Code, Message: s.Status.Message}
	}
	if len(s.Attributes) > 0 {
		c.Attributes = make([]*commonpb.KeyValue, len(s.Attributes))
		copy(c.Attributes, s.Attributes) // shallow KeyValue 指针, 内容 immutable after enqueue
	}
	if len(s.Events) > 0 {
		c.Events = make([]*tracepb.Span_Event, len(s.Events))
		copy(c.Events, s.Events) // 同款 shallow
	}
	return c
}

func bytesClone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// addEvent 给 in-flight span 加 event (e.g., OnNodeEvent / OnModelStart 的 prompt).
func (o *Observability) addEvent(ctx context.Context,
	kind, name string, round int32, callID string,
	eventName string, attrs []*commonpb.KeyValue,
) {
	if o == nil || o.exporter == nil {
		return
	}
	sess := o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	v, ok := sess.spans.Load(registryKey(kind, graphNS, name, round, callID, compose_v2.GetCurrentInstanceID(ctx)))
	if !ok {
		return
	}
	entry := v.(*spanEntry)
	// review P2 audit HIGH-2 fix: 多 goroutine 并发 addEvent (fan-out
	// OnNodeEvent / ISSUE-81 toolsFn fanout) 写 span.Events 无锁 race.
	entry.mu.Lock()
	defer entry.mu.Unlock()
	span := entry.span
	// span event 限 MaxSpanEvents — review P2 audit LOW-2 fix: tracepb.Span
	// 已有 DroppedEventsCount 字段, 累加让业务可观测 dropped 数 (spec line 298).
	if o.cfg.MaxSpanEvents > 0 && len(span.Events) >= o.cfg.MaxSpanEvents {
		span.DroppedEventsCount++
		return
	}
	// review P0-P2 整体 audit² 视角 B fix: event attr 也走 limit
	// (event attr 跟 span attr 共享 MaxSpanAttributes 上限做粗暴上限, spec 没说
	// event attr 独立 budget — 简化用 span 总 attr 数算).
	limitedAttrs := make([]*commonpb.KeyValue, 0, len(attrs))
	maxKey := o.cfg.MaxAttributeKeyChars
	maxVal := o.cfg.MaxAttributeValueChars
	for _, a := range attrs {
		if a == nil {
			continue
		}
		if maxKey > 0 && len(a.Key) > maxKey {
			a.Key = truncString(a.Key, maxKey)
		}
		// P2.6 audit HIGH-1 fix: gen_ai_content event attr 走 spec line 141 独立
		// 1MB 上限 (caller hooks.go renderContent 已按 MaxInlineEventBytes 截断).
		// 跳过 MaxAttributeValueChars 默认 512 的二次 truncate 避免 8x 折扣.
		if a.Key != attrGenAIContent && maxVal > 0 {
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok && len(sv.StringValue) > maxVal {
				sv.StringValue = truncString(sv.StringValue, maxVal)
			}
		}
		limitedAttrs = append(limitedAttrs, a)
	}
	span.Events = append(span.Events, &tracepb.Span_Event{
		TimeUnixNano: uint64(time.Now().UnixNano()),
		Name:         eventName,
		Attributes:   limitedAttrs,
	})
}

// findParentSpanID 按 kind 优先级查 in-flight 父 span ID.
//
// 优先级 (按 impl-guide §1 嵌套树):
//
//	tool/model span 的父是 agent span
//	agent span 的父是 node 或 graph span
//	node span 的父是 graph span
//	graph span 的父是 "" (root)
func (s *sessionState) findParentSpanID(graphNS string, parentKind, parentName string, parentRound int32) string {
	v, ok := s.spans.Load(registryKey(parentKind, graphNS, parentName, parentRound, "", ""))
	if !ok {
		return ""
	}
	return string(v.(*spanEntry).span.SpanId)
}

// chunkBuffer 是 streaming chunk content 累积容器 — OnNodeEvent (Type=chunk) 路径
// append, OnNodeEnd 路径 flush 成单个 streaming_response event.
//
// mu 保 builder 写读一致 (OnNodeEvent 并发 append vs OnNodeEnd flush). count + nanos
// 记元数据让前端展示 chunk 数量 / streaming 时长.
type chunkBuffer struct {
	mu         sync.Mutex
	builder    strings.Builder
	count      int
	firstNanos int64
	lastNanos  int64
}

// appendChunk 加 chunk content 到 buffer, 更新 count + last nanos.
// 首次 append 时设 firstNanos.
func (cb *chunkBuffer) appendChunk(content string, nanos int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.count == 0 {
		cb.firstNanos = nanos
	}
	cb.builder.WriteString(content)
	cb.count++
	cb.lastNanos = nanos
}

// snapshot 拿当前累积 content + 元数据 (返字符串拷贝, caller 安全持有).
func (cb *chunkBuffer) snapshot() (content string, count int, firstNanos, lastNanos int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.builder.String(), cb.count, cb.firstNanos, cb.lastNanos
}

// chunkBufferKey 派生 chunkBuffers map key — 跟 spanKey 同款让 OnNodeEvent / OnNodeEnd
// 用相同 key 访问 buffer.
// chunkBufferKey 按 (graphNS, nodeID, 实例身份) 分槽。
//
// 实例维度不能省 (ISSUE-96 侧面 6): SendFrom 同 Target 投 N 次时 N 个实例并发流式
// 输出，落同一个 buffer 会把 N 路 chunk 逐 token 编织成一段乱码。静态边节点
// instanceID 恒空 → key 形态与引入前一致。
func chunkBufferKey(graphNS, nodeID, instanceID string) string {
	if instanceID == "" {
		return graphNS + "|" + nodeID
	}
	return graphNS + "|" + nodeID + "|" + instanceID
}

// findActiveAgentSpan 找当前 graphNS 下 in-flight 的 agent span, 返其 (spanID, nodeID).
//
// P2 fix: workflow node (preamble/model/tools/postamble) 优先挂当前活跃 agent span
// 下作 parent, 让 trace 树层级清晰. agent span 在 OnAgentStart 创建, 整个 Run
// 期间 in-flight, OnNodeStart 触发时通过本 helper 找到挂上.
//
// **多 agent 在同 graphNS 场景** (PlanAgent 同时跑多个 sub-agent 不同 ns 不算; 真
// 同 ns 多 agent 是反模式): 返第一个找到的, 不保证特定 agent. 业务避免设计.
//
// 没 agent in-flight 返 ("", "") — caller fallback 挂 graph span.
func (s *sessionState) findActiveAgentSpan(graphNS string) (spanID, nodeID string) {
	prefix := "agent|" + graphNS + "|"
	s.spans.Range(func(k, v any) bool {
		key := k.(string)
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			return true // 跳非当前 graphNS agent
		}
		entry := v.(*spanEntry)
		spanID = string(entry.span.SpanId)
		// 从 span attrs 反查 nodeID — 跟 startSpan 内 nodeID = graphNS+"/agent/"+name 同款
		for _, a := range entry.span.Attributes {
			if a.Key == "gen_ai.node.id" {
				nodeID = a.Value.GetStringValue()
				break
			}
		}
		return false // 找到第一个就退出
	})
	return spanID, nodeID
}

// findActiveAgentSpanFrom 从 startNS 起逐级沿 ParentNS 上溯，返第一个有活跃 agent
// span 的 (spanID, nodeID)（含 startNS 本身先命中）。跨 ns 找父 agent span 用：
// a2a_client span（RemoteAgent 经 AgentAsTool/RunSubAgent 在 child ns，host agent span
// 在 parent ns，精确匹配 child ns 找不到需上溯）+ OnAgentStart 嵌套 sub-agent 找祖先层
// in-flight agent。无命中返空。
func (s *sessionState) findActiveAgentSpanFrom(startNS string) (spanID, nodeID string) {
	for ns := startNS; ; {
		if id, nid := s.findActiveAgentSpan(ns); id != "" {
			return id, nid
		}
		if ns == "" {
			return "", ""
		}
		next := compose_v2.ParentNS(ns)
		if next == ns { // 防御 ParentNS 不收敛死循环
			return "", ""
		}
		ns = next
	}
}

// intAttr / floatAttr — OTLP attr 构造 helper.
func intAttr(key string, val int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: val}},
	}
}

func floatAttr(key string, val float64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: val}},
	}
}
