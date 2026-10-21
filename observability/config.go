// Package observability 是 crew-go 自家的 CS-OSS Monitor trace + metric 推送
// 模块——按 monitor-agent spec (AI_TRACE_SPEC.md + OTHER_LANGUAGE_SDK_INTEGRATION.md)
// 直接构造 OTLP protobuf + gRPC 推到 CS-OSS Monitor collector.
//
// **为什么不用 monitor-agent 自家 SDK** (`apimachinery/pkg/monitor`):
//   - MaxUserAttrNum=10 写死, 业务 attr 超量 silent drop (我们 chat span 框架内置 17 attr)
//   - attribute value 一刀切 ≤ 512 char silent drop, 不支持 spec `gen_ai_content` event 1MB 路径
//   - Attribute(key, val string) 只 string, `usage.input_tokens=320` 转 string 后端 PromQL 无法 sum
//
// 模块包结构:
//   - config.go (本文件): Config + Resource + Limits
//   - id.go: CS-OSS TraceID/SpanID 生成 + child span 派生
//   - exporter.go: OTLP gRPC client (TraceService + MetricsService) + batch + 5s tick
//   - propagation.go: x-traceid / x-spanid / X-ROOT-MESSAGE-ID inject/extract
//   - hooks.go: hook.Hooks (9) + OrchHooks (5) 实现, hook 触发构造 OTLP Span
//   - metric.go: Counter / Histogram / Gauge OTLP DataPoint 构造
//
// 业务用法 (一行注入):
//
//	obs, err := observability.New(observability.Config{
//	    TraceEndpoint:   "127.0.0.1:5317",
//	    MetricsEndpoint: "127.0.0.1:5318",
//	    Resource: observability.Resource{
//	        AppName: "my-app", EnvName: "prod", IP: "10.0.0.1", SDKVersion: "crew-1.0",
//	    },
//	})
//	defer obs.Close()
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithHooks(obs.AgentHooks()),
//	    agent_v2.WithOrchHooks(obs.OrchHooks()),
//	)
package observability

import (
	"fmt"
	"regexp"
	"time"
)

// validAppNameRe — AppName 字符集校验. 跟 monitor SDK `span.go:22` traceIDReg
// 的 `[\w-]+` 同源 (字母/数字/下划线/dash). 中文/空格/`^^`/`/`/`|` 任一字符都
// 会让 GenTraceID 生成的 traceID 不匹配 spec 正则被 collector 拒.
var validAppNameRe = regexp.MustCompile(`^[\w-]+$`)

// Resource 是 CS-OSS Monitor 上报必带的进程/服务级标签——后端查询和过滤强依赖.
//
// 按 spec OTHER_LANGUAGE_SDK_INTEGRATION.md §Resource:
//   - 必填: AppName / EnvName / IP / SDKVersion (page line 145 "后端查询通常依赖")
//   - 建议: EnvLevel / Zone / Host / SDKLanguage
//   - 兼容 OTel: service.name / deployment.environment (建议同 AppName / EnvName)
type Resource struct {
	AppName    string // 必填. 应用名 (e.g., "my-agent-app")
	EnvName    string // 必填. 环境名 (prod/test/dev)
	EnvLevel   string // 建议. 环境等级 (online/offline)
	Zone       string // 建议. 多活分区 (e.g., "sh")
	IP         string // 必填. 实例 IP — 业务自取 (e.g., 网卡 IP / Pod IP)
	Host       string // 建议. 实例 hostname
	SDKVersion string // 必填. SDK 版本 (e.g., "crew-1.0.0")
}

// Config 是 observability 模块的初始化配置.
//
// 默认值按 CS-OSS spec, 业务可上调 (e.g., MaxSpanAttributes 默认 128 已比 spec 默认 15 宽).
type Config struct {
	// Resource 进程/服务标签 (必填核心字段, 见 Resource 注释)
	Resource Resource

	// TraceEndpoint OTLP gRPC TraceService endpoint (e.g., "127.0.0.1:5317")
	//
	// 空字符串 → 不启 trace 上报 (业务本地开发 / 单测兜底).
	TraceEndpoint string

	// MetricsEndpoint OTLP gRPC MetricsService endpoint (e.g., "127.0.0.1:5318")
	//
	// 空字符串 → 不启 metric 上报.
	MetricsEndpoint string

	// MaxSpanAttributes 单 span attribute 数上限.
	// spec line 168 默认 15, 我们默认 128 对齐 OTel 通用 (CS-OSS collector 端不 enforce).
	// 超量按 OTel `dropped_attributes_count` 上报 (spec line 297).
	MaxSpanAttributes int

	// MaxAttributeKeyChars 单 attribute key 长度上限.
	// spec line 166 默认 255 (non-empty printable ASCII), 我们默认对齐.
	MaxAttributeKeyChars int

	// MaxAttributeValueChars 单 attribute value 长度上限.
	// spec line 169 默认 512 char, 我们默认对齐. 超量头尾保留中间裁.
	// 注: `gen_ai_content` event attribute 是独立 1MB 上限, 走 MaxInlineEventBytes 路径.
	MaxAttributeValueChars int

	// MaxInlineEventBytes `gen_ai_content` event attribute 长度上限.
	// spec line 141 默认 1MB (1024*1024), 业务可调小 (高 QPS 减 trace 体量).
	MaxInlineEventBytes int

	// MaxSpanEvents 单 span event 数上限. spec 未明示, 默认 128.
	MaxSpanEvents int

	// MaxMetricSeries metric registry 内不同 series (name + labels 组合) 数上限.
	// 业务误用高基数 label (e.g., user_id / request_id) 时防 OOM 兜底.
	// 0 = 无限制 (default 10000). 超出 cap 后 Counter/Histogram/Gauge 返 nil,
	// 业务 Inc/Observe/Set nil-safe 不 panic 仅 silent skip.
	//
	// Sub-5 Round 4 audit K2 fix: 之前无 cap, 业务高基数 label 持续 lazy create
	// 新 series → OOM. 加 cap + 标 `MetricCardinalityDropped` Counter 让业务可监控.
	MaxMetricSeries int

	// DisabledMetrics 是 metric name 黑名单（wire 名，如 "gen_ai_compacts" / "gen_ai_failovers"）——
	// per-category 观测关闭：命中的 metric 在 Counter/Histogram/Gauge 入口直接返 nil（复用 cap
	// 同款 nil-safe 路径，业务 Inc/Observe/Set silent skip），省 series/cardinality/上报成本。
	// event emit 仍照发（下层 fallback/compact 零改动，只是不转成 metric）。业务想按类关观测降本
	// 时用（对齐 OTel View DropAggregation 的 name-based drop）。空/nil = 全采（默认行为不变）。
	DisabledMetrics []string

	// TraceQueueMaxSize 内存 batch queue 上限.
	// spec line 420 默认 2048, 超量 LIFO drop 新 span 优先入.
	TraceQueueMaxSize int

	// TraceExportInterval 内存 batch 推送间隔. spec line 419 默认 5s.
	TraceExportInterval time.Duration

	// TraceExportBatchSize 单次推送 max span 数. spec line 421 默认 512.
	TraceExportBatchSize int

	// MetricsExportInterval metric 推送间隔. spec line 422. 默认 5s (applyDefaults).
	//
	// **独立于 TraceExportInterval** — exportLoop 单 goroutine 2 ticker (trace tick /
	// metric tick 各自频率), 对齐 OTel BatchTimeout vs PeriodicReader interval 分离.
	// 业务可设不同频率 (trace / metric 上报频率不锁死); 仍单 goroutine 省一个额外.
	MetricsExportInterval time.Duration

	// TraceRPCTimeout 单次 OTLP Export RPC 超时. spec line 99 默认 10s.
	// review P0-P2 整体 audit² 视角 C MED-2 fix: 之前写死 5s 跟 spec 偏差 1/2.
	TraceRPCTimeout time.Duration

	// SamplingRatio trace 采样率 [0.0, 1.0]. 默认 1.0 全采.
	//
	// **zero-value 语义**: 业务 zero-value 0 跟显式 0 一律视作"未设"走默认 1.0 全采
	// (避免业务忘填字段 silent drop 整 trace — review P0 audit HIGH-3 决策).
	// 想 100% drop trace 推送有两种正解:
	//   - 关闭整推送: TraceEndpoint="" (推荐, 也跳过 exporter goroutine 启动)
	//   - 极小采样:  SamplingRatio=1e-9 (rand.Float64 几乎不命中)
	// 0.5 = 50% session 上报 (session-level head sampling 跟 OTel TraceIdRatioBased 同款).
	// 负数 fail-fast (Validate 拒), >1.0 clamp 到 1.0.
	SamplingRatio float64

	// RecordMessageContent 是否采集 LLM message 内容到 `gen_ai_content` event.
	// 默认 false (PII 风险). 业务 opt-in true 才采.
	RecordMessageContent bool

	// RedactPatterns PII 过滤 regex 列表. 命中替换为 [REDACTED].
	// 业务自配 (e.g., 身份证 / 手机号 / API key 正则).
	// RecordMessageContent=false 时本字段无效.
	RedactPatterns []*regexp.Regexp
}

// applyDefaults 把 zero-value 字段填默认值.
//
// **不验 Resource 必填**——业务可能裸用 csoss adapter 不传 Resource (e.g., 单元测试),
// 上报到 collector 时 Resource 缺会被 server 拒, 业务自负. New() 入口才校验.
func (c *Config) applyDefaults() {
	if c.MaxSpanAttributes <= 0 {
		c.MaxSpanAttributes = 128
	}
	if c.MaxAttributeKeyChars <= 0 {
		c.MaxAttributeKeyChars = 255
	}
	if c.MaxAttributeValueChars <= 0 {
		c.MaxAttributeValueChars = 512
	}
	if c.MaxInlineEventBytes <= 0 {
		c.MaxInlineEventBytes = 1024 * 1024 // spec 1MB
	}
	if c.MaxSpanEvents <= 0 {
		c.MaxSpanEvents = 128
	}
	if c.MaxMetricSeries <= 0 {
		c.MaxMetricSeries = 10000 // 业务高基数 label 兜底 cap (Sub-5 Round 4 audit K2 fix)
	}
	if c.TraceQueueMaxSize <= 0 {
		c.TraceQueueMaxSize = 2048
	}
	if c.TraceExportInterval <= 0 {
		c.TraceExportInterval = 5 * time.Second
	}
	if c.TraceExportBatchSize <= 0 {
		c.TraceExportBatchSize = 512
	}
	if c.MetricsExportInterval <= 0 {
		c.MetricsExportInterval = 5 * time.Second
	}
	if c.TraceRPCTimeout <= 0 {
		c.TraceRPCTimeout = 10 * time.Second // spec line 99 默认
	}
	// review P0 audit HIGH-3 fix: 负数 SamplingRatio silent 改 1.0 反业务预期 (业务
	// typo 写 -0.5 期望 fail-fast, 改成全采是 silent 放大量 trace 上报). 改:
	//   == 0: 走默认 1.0 (默认全采)
	//   < 0:  validate() 入口 fail-fast (Config.Validate 实现)
	//   > 1.0: clamp 到 1.0
	if c.SamplingRatio == 0 {
		c.SamplingRatio = 1.0
	} else if c.SamplingRatio > 1.0 {
		c.SamplingRatio = 1.0
	}
}

// Validate 校验 Config 必填字段 + 字符集 + 范围. New() 入口调.
//
// review P0 audit HIGH-2/HIGH-3 fix: AppName 字符集校验 + 长度上限 +
// SamplingRatio 负数 fail-fast.
func (c *Config) Validate() error {
	if err := c.Resource.validate(); err != nil {
		return err
	}
	if c.SamplingRatio < 0 {
		return fmt.Errorf("observability.Config.SamplingRatio must be >= 0 (0=fall back to default 1.0, >0 actual ratio), got %v", c.SamplingRatio)
	}
	return nil
}

// MaxAppNameChars AppName 长度上限. spec 未明示但 attribute key 上限 255 char (line 166),
// AppName 拼到 traceID 后续, 留余地控制 100 char 内.
const MaxAppNameChars = 100

// validateResource 校验必填字段非空 + AppName 字符集 + 长度. New() 入口调.
//
// review P0 audit HIGH-2 + MED-3 fix:
//   - AppName 加正则校验 `^[\w-]+$` — 中文/空格/`^^` 等让 GenTraceID 生成非法 ID
//     被 collector 拒, 整 trace 链 silent broken. 必须 fail-fast.
//   - AppName 长度上限 MaxAppNameChars=100 防业务超长拼接.
func (r *Resource) validate() error {
	if r.AppName == "" {
		return fmt.Errorf("observability.Config.Resource.AppName required (spec OTHER_LANGUAGE_SDK_INTEGRATION.md line 117)")
	}
	if len(r.AppName) > MaxAppNameChars {
		return fmt.Errorf("observability.Config.Resource.AppName too long (%d > %d), 建议 < 100 char 防 traceID 拼接超长", len(r.AppName), MaxAppNameChars)
	}
	if !validAppNameRe.MatchString(r.AppName) {
		return fmt.Errorf("observability.Config.Resource.AppName %q invalid: 仅允许字母/数字/下划线/dash (`[\\w-]+`), 中文/空格/`^^`/`|`/`/` 等会破坏 traceID 正则 (spec line 192) 被 collector 拒", r.AppName)
	}
	if r.EnvName == "" {
		return fmt.Errorf("observability.Config.Resource.EnvName required (spec line 118)")
	}
	if r.IP == "" {
		return fmt.Errorf("observability.Config.Resource.IP required (spec line 121)")
	}
	if r.SDKVersion == "" {
		return fmt.Errorf("observability.Config.Resource.SDKVersion required (spec line 122)")
	}
	// audit² B-MED-B1 fix: 之前 IP/Host/Zone/EnvLevel/SDKVersion 长度无上限. 业务恶意
	// (或拼错配 100KB Host) 会让每次 OTLP RPC 都附超长 Resource attr, 持续浪费带宽
	// (Resource 每 RPC 都传). 加上限校验 fail-fast (跟 AppName 同款防御).
	if len(r.IP) > 50 { // IPv6 max 39 + 余地
		return fmt.Errorf("observability.Config.Resource.IP too long (%d > 50)", len(r.IP))
	}
	if len(r.Host) > 255 { // FQDN 上限
		return fmt.Errorf("observability.Config.Resource.Host too long (%d > 255)", len(r.Host))
	}
	if len(r.Zone) > 64 {
		return fmt.Errorf("observability.Config.Resource.Zone too long (%d > 64)", len(r.Zone))
	}
	if len(r.EnvLevel) > 64 {
		return fmt.Errorf("observability.Config.Resource.EnvLevel too long (%d > 64)", len(r.EnvLevel))
	}
	if len(r.SDKVersion) > 64 {
		return fmt.Errorf("observability.Config.Resource.SDKVersion too long (%d > 64)", len(r.SDKVersion))
	}
	return nil
}
