package agent_v2

import (
	"context"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// ────────────────────────────────────────────────────────────
// compact event 协议
//
// 设计核心:
//   - **sink-gated (无独立开关)**: fire 与否由 ctx 有无 EventSink 决定 (hasEventSink)——
//     业务接了 observability (sink 在) 就自动观测压缩, 不再单独 opt-in EmitEvents.
//     无 sink 时 caller 不构造 payload + EmitCompactionEvent 内 GetEventSink nil 检双兜底.
//   - **union 结构**: CompactionAction.Type 决定 Before/After/Generate 哪个字段非
//     nil.
//   - **加 Layer 字段**: 区分 4 层 (Micro/Auto/Reactive/Store——Store 留 Phase 2).
//   - **走 compose_v2.EventSink.Send**: 跟现有 chunk/reset_stream 同协议, 不引
//     入新 hook 接口. observability 的 OnNodeEvent 消费它（span 事件 + crew.compaction* 指标）.
//   - **仅观测**: 业务订阅 event 看到压缩前后完整状态, 不能改写. 真改写靠现有
//     Config 内独立钩子 (如 AutoCompactConfig.Model 业务自定义).
//
// 跨 4 层一致 event.Type = CompactionEventType = "compaction".
// 区分靠 Action.Layer (micro/auto/reactive/store) + Action.Type (before/after/generate).
// ────────────────────────────────────────────────────────────

// CompactionEventType 是 compose_v2.Event.Type 字符串. observability 的
// OnNodeEvent 按此值识别 compaction event，把 Action 展开成 span 事件属性.
const CompactionEventType = "compaction"

// hasEventSink 报告 ctx 是否挂了 compose_v2.EventSink。压缩各层用它作为 fire event
// 的门——取代已删的各 *Config.EmitEvents / Config.EmitHistoryCompactEvent 布尔开关。
//
// 「开压缩」与「观测压缩」不再割裂:不再需要单独 opt-in EmitEvents。event 最终去向由
// EventSink 背后的 OrchHooks 决定——业务接了 observability 就自动收到 compact event
// (转成 metric/span),没接则 framework 的 NoOp hooks 丢弃。
//
// 门的意义:BasicAgent 等 graph 执行内 framework 恒 push nodeEventSink(graph_loop.go),
// 故 sink 恒在、compact 恒发(payload 开销小,未接 observability 时 NoOp 丢弃);而
// **直接调用** MicroCompactRun/AutoCompactRun/ReactiveCompactRun 不经 graph 时无 sink,
// 此门让 caller 连 event payload 都不构造(含 offload 时算 value_size 的 json.Marshal;
// offload 的 store.Put 本身在门外恒执行,是压缩副作用不是观测)。
func hasEventSink(ctx context.Context) bool {
	return compose_v2.GetEventSink(ctx) != nil
}

// CompactionLayer 区分 3 层 compact 实际 fire event 的范围.
//
// 注: 第 4 层 StoreMemory (跨 Run 长期记忆 via store.Search/store.Put) 不属
// "compact" 范畴, Phase 2 D2 trace 接入时走独立 store.search / store.put event
// type, 不复用本 CompactionLayer 常量 (audit LOW-A1: 项目"不留兼容路径"规约,
// 不预留 dead constant).
type CompactionLayer string

const (
	// CompactionLayerMicro 第 1 层 MicroCompact——清白名单 tool 旧 tool_result 内容.
	CompactionLayerMicro CompactionLayer = "micro"
	// CompactionLayerAuto 第 2 层 AutoCompact——LLM 摘要早期对话.
	CompactionLayerAuto CompactionLayer = "auto"
	// CompactionLayerReactive 第 3 层 ReactiveCompact——LLM 报 prompt_too_long 时
	// emergency drop oldest groups.
	CompactionLayerReactive CompactionLayer = "reactive"
)

// CompactionActionType 区分压缩生命周期 3 phase.
type CompactionActionType string

const (
	// CompactionActionBefore 触发条件满足后, 实际压缩动作前.
	CompactionActionBefore CompactionActionType = "before"
	// CompactionActionGenerate 仅 AutoCompact LLM 调用之后 fire (每 attempt 1 次,
	// 含 retry/failover). Micro/Reactive 不调 LLM 无此 phase.
	CompactionActionGenerate CompactionActionType = "generate"
	// CompactionActionAfter 压缩完整完成 (含失败 / 成功).
	CompactionActionAfter CompactionActionType = "after"
	// CompactionActionRetry 仅 ReactiveCompact retry 循环每 attempt 之间 fire
	// (caller modelFn 内 LLM 报 prompt_too_long → ReactiveCompactRun 删旧 messages
	// → fire retry event → 重 callLLM). 跟 reset_stream event 并行: reset_stream
	// 是前端清 chunk buffer 用, compact:reactive:retry 是 compact retry 状态观测.
	CompactionActionRetry CompactionActionType = "retry"
)

// CompactionGeneratePhase AutoCompact LLM 调用 phase.
type CompactionGeneratePhase string

const (
	// CompactionGeneratePhasePrimary 主 model 调用 (含 retry).
	// Attempt=1 是 initial call, Attempt>1 是 retry.
	CompactionGeneratePhasePrimary CompactionGeneratePhase = "primary"
	// CompactionGeneratePhaseFailover failover model (未来扩展, 当前不用).
	CompactionGeneratePhaseFailover CompactionGeneratePhase = "failover"
)

// CompactionAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段——
// observability / 业务订阅方按 Type 分发处理 Before/After/Generate/Retry 字段.
//
// union 模式 + Layer 字段区分 4 层.
type CompactionAction struct {
	Layer CompactionLayer      `json:"layer"`
	Type  CompactionActionType `json:"type"`

	// Before 仅 Type=before 时非 nil.
	Before *CompactionBeforeAction `json:"before,omitempty"`
	// After 仅 Type=after 时非 nil.
	After *CompactionAfterAction `json:"after,omitempty"`
	// Generate 仅 Type=generate 时非 nil (仅 AutoCompact 用).
	Generate *CompactionGenerateAction `json:"generate,omitempty"`
	// Retry 仅 Type=retry 时非 nil (仅 ReactiveCompact retry 循环 attempt 之间 fire).
	Retry *CompactionRetryAction `json:"retry,omitempty"`
}

// CompactionBeforeAction 压缩前快照——业务订阅可备份/审计/对比.
type CompactionBeforeAction struct {
	// Messages 压缩前完整 messages 快照 (浅拷贝 slice header, 元素指针共享).
	// 业务侧若要 mutate 需自己 deep clone.
	Messages []*schema.Message `json:"messages,omitempty"`
	// InputTokens shouldTrigger 决策时计的 token 总数 (实测或估算).
	// MicroCompact 不计 token, 此字段为 0.
	InputTokens int `json:"input_tokens"`
	// Trigger 触发原因——"threshold" (AutoCompact 超阈值) / "prompt_too_long"
	// (ReactiveCompact LLM 报错) / "manual" (业务主动调) / "" (MicroCompact 总跑).
	Trigger string `json:"trigger,omitempty"`
}

// CompactionAfterAction 压缩后快照.
//
// **OTel self-contained 设计 (Sub-5d audit fix)**: After event 自带 InputTokens +
// DurationMs 字段让单 event 业务侧自算 ratio + duration metric. 之前设计依赖
// observability 端 sess cache 关联 Before event 数据——改为 self-contained event
// 更优 (无 cache lifecycle 烦恼, 业务订阅单 event 即得完整压缩数据, 跟 cp event
// caller-fills-DurationMs 设计一致).
type CompactionAfterAction struct {
	// Messages 压缩后完整 messages 快照 (浅拷贝 slice header, 元素指针共享).
	// 业务侧若要 mutate 需自己 deep clone (跟 BeforeAction.Messages 同款约束).
	Messages []*schema.Message `json:"messages,omitempty"`
	// InputTokens 压缩前 token 总数 (Sub-5d audit fix 加).
	// 跟 BeforeAction.InputTokens 同值 — 复制到 After 让 event self-contained, 业务侧
	// 订阅单个 After event 算 ratio = OutputTokens / InputTokens 不依赖 Before event 缓存.
	// MicroCompact 不计 token 时为 0.
	InputTokens int `json:"input_tokens"`
	// OutputTokens 压缩后 token 总数 (可选——MicroCompact 不计为 0).
	OutputTokens int `json:"output_tokens"`
	// DurationMs caller 测的压缩耗时 (从 Before fire 到 After fire 之间, 毫秒)
	// (Sub-5d audit fix 加). 跟 cp event CpAction.Save.DurationMs 设计一致 —
	// caller 在 scope 内最准 (event 系统传递延迟不算).
	DurationMs int64 `json:"duration_ms"`
	// Dropped 跨 3 层语义不同, **业务不要跨层对比** (audit MED-A2 文档化):
	//   - Micro: tool_result **内容被替占位符** 的数量 (Messages 长度不变, 仅内容替)
	//   - Auto:  early 部分**被 summary 替**的 message 数 (Messages 长度减少)
	//   - Reactive: emergencyDrop **真删** 的 message 数 (Messages 长度减少)
	// 业务跨层算"压缩率" Dropped/(Dropped+Kept) 时 Micro 报"60% dropped"但
	// Messages 长度跟 Before 一样 — 不要直接对比. 各层独立观测.
	Dropped int `json:"dropped"`
	// Kept 压缩后保留 message 数. 跨层语义:
	//   - Micro:  len(messages) - len(toClear) (Messages 长度 - 内容被替的)
	//   - Auto:   len(out) (压缩后 messages 长度)
	//   - Reactive: len(out) (emergencyDrop 后 messages 长度)
	Kept int `json:"kept"`
	// Err 压缩失败原因 (仅失败时填). 成功时空字符串.
	// MicroCompact 永不失败. AutoCompact 可能失败 (LLM error / breaker tripped
	// / nil resp). ReactiveCompact 可能失败 (exhausted attempts).
	Err string `json:"err,omitempty"`
}

// CompactionRetryAction ReactiveCompact retry 循环 attempt 之间 fire 的细节——
// caller modelFn 收到 prompt_too_long 错 → ReactiveCompactRun 删旧 messages →
// 本 event fire → 重 callLLM. 跟 reset_stream event 并行 fire (语义分离).
//
// **fire 时序** (audit fix): event 在 callLLM 调用**之前** fire (上一 attempt
// 失败后, 新 attempt 调用前). 业务订阅看到本 event 时, 本 attempt 的 callLLM
// 还没跑. 业务想统计 "retry attempt 真完成数" / "成功率" 时:
//   - retry 不再 fire → attempt N 之后 callLLM 成功或 exhausted (区分: 看 OnModelEnd
//     event 是否在末尾 fire)
//   - retry 再 fire (attempt N+1) → attempt N callLLM 仍 prompt_too_long 需再 retry
//
// **跨层统一 sink-gated**: reactive 相关 event 由 hasEventSink(ctx) 门统一控制 fire:
//   - reactive_compact.go 层 ReactiveCompactRun 内 Before/After 2 event
//     (emergencyDrop 单次输入/输出快照)
//   - basic_agent.go modelFn 层 retry 循环 Retry 1 event (caller wrap 层 attempt 通知)
//
// 无独立 EmitEvents 开关——接了 observability 就全收. 业务想精细过滤可订阅 OnNodeEvent
// 按 event.Type + action.Type 分发.
type CompactionRetryAction struct {
	// Attempt 1-based 当前 attempt 序号 (compact 后即将 retry 的次数).
	Attempt int `json:"attempt"`
	// MaxAttempts 最大 attempt 配置 (ReactiveCompactConfig.MaxAttempts 或默认 3).
	MaxAttempts int `json:"max_attempts"`
	// CompactedMsgs ReactiveCompactRun 返的压缩后 messages 数 (将作下次 callLLM input).
	CompactedMsgs int `json:"compacted_msgs"`
	// OrigMsgs 进入 retry 循环**最初** messages 数 (跨 attempt **恒定** — 始终是入
	// retry 循环前的 msgs 长度, 不是上一 attempt curMsgs 长度). 业务可算总压缩率
	// `(OrigMsgs - CompactedMsgs) / OrigMsgs`. 想算 attempt-to-attempt 增量压缩率
	// 需自己 track 跨 event 的 CompactedMsgs 差.
	OrigMsgs int `json:"orig_msgs"`
	// Reason 触发 retry 的原因 (业务订阅可区分多种 retry 触发) — 当前固定
	// "prompt_too_long" (IsPromptTooLongError 判定为 true 才 retry).
	Reason string `json:"reason,omitempty"`
	// RunID 是上一次失败 attempt 的 run_id (跟同 attempt 已 fire 的 reset_stream
	// event 内 run_id 一致). 前端按此 run_id 清失败 stream 的 chunk buffer; 不是
	// 即将开始的 retry attempt 的 run_id (新 attempt 在 callLLM 内重新生成 UUID).
	//
	// audit fix: 之前 godoc 写"当前 LLM 调用的 run_id"误导 — 实际 fire 在 callLLM
	// 前, lastRunID 还是上一失败 attempt 的值. 业务订阅"按 run_id 关联 stream
	// session"应理解关联的是已作废 stream (跟 reset_stream 同语义).
	RunID string `json:"run_id,omitempty"`
}

// CompactionGenerateAction AutoCompact LLM 调用细节——每 attempt 1 个.
type CompactionGenerateAction struct {
	// Attempt 1-based attempt 序号 (含 retry).
	Attempt int `json:"attempt"`
	// Phase primary / failover.
	Phase CompactionGeneratePhase `json:"phase"`
	// Response LLM 返的原始响应——失败时可能为 nil.
	Response *schema.Message `json:"response,omitempty"`
	// Err LLM 调用失败原因 (err.Error()), 成功时空字符串.
	Err string `json:"err,omitempty"`
}

// EmitCompactionEvent 投递 compaction event 到 compose_v2.EventSink.
//
// **sink-gated**: caller 用 hasEventSink(ctx) 门决定是否调本函数 (无独立 EmitEvents
// 开关). 这让 hot path 不付额外 payload 构造开销.
//
// **零开销路径**: ctx 内无 EventSink 时立刻 return (业务不订阅 event 流时不付任何代价).
//
// observability 的 OnNodeEvent 取 Metadata["action"].(*CompactionAction)，按 Layer + Type
// 展开成 span 事件属性，After 事件再记 crew.compaction* 指标；业务也可自行订阅 OnNodeEvent 消费.
func EmitCompactionEvent(ctx context.Context, action *CompactionAction) {
	if action == nil {
		return
	}
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type:    CompactionEventType,
		Content: string(action.Layer) + ":" + string(action.Type),
		Metadata: map[string]any{
			"action": action,
		},
	})
}
