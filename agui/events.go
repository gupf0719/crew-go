// Package agui 把 crew-go 的 agent 执行事件适配成 AG-UI（Agent-User Interaction
// Protocol）标准事件流，经 SSE 推给前端——业务可直接接 CopilotKit 等 AG-UI 生态的
// 现成前端组件（React / Vue / Angular），不必为 crew-go 专写前端。
//
// AG-UI 是 CopilotKit 主导的 agent→前端事件协议（camelCase JSON + Server-Sent
// Events）。本包内联精简了 server 端要用的事件类型——字段名 / json tag 照
// ag-ui-protocol/ag-ui 的 wire 定义扒（保证与前端兼容），但不硬依赖那个单人
// community 维护、无版本 tag、引 logrus 的官方 Go SDK（协议是薄层，内联可控，
// 同 modules/ 拷 spec 的套路）。
//
// 四层：
//   - events.go  —— AG-UI 事件类型（本文件，协议层）
//   - sse.go     —— SSE 编码 writer
//   - adapter.go —— crew-go 事件源（hook.Hooks + OrchestrationHooks）→ AG-UI 事件
//   - handler.go —— HTTP SSE 端点（把 agent.Run 暴露成 AG-UI server）
//
// 决策溯源见 doc/issue/issue-49-frontend-stream-adapter.md「2026-07-01 决策修订」。
package agui

// EventType 是 AG-UI 事件的 wire 类型（大写下划线，如 "RUN_STARTED"）。
type EventType string

// AG-UI 事件类型常量——只收录 crew-go server 端映射用得上的子集
// （AG-UI 1.0 全集 **31 个**——1.0 退役了 5 个 THINKING_*、新增 3 个 SUBAGENT_*；
// Reasoning / Activity / Subagent 等 crew-go 暂无对应源或未接）。
const (
	// Run 生命周期
	EventTypeRunStarted   EventType = "RUN_STARTED"
	EventTypeRunFinished  EventType = "RUN_FINISHED"
	EventTypeRunError     EventType = "RUN_ERROR"
	EventTypeStepStarted  EventType = "STEP_STARTED"
	EventTypeStepFinished EventType = "STEP_FINISHED"

	// 文本消息（流式 Start → Content×N → End）
	EventTypeTextMessageStart   EventType = "TEXT_MESSAGE_START"
	EventTypeTextMessageContent EventType = "TEXT_MESSAGE_CONTENT"
	EventTypeTextMessageEnd     EventType = "TEXT_MESSAGE_END"

	// 工具调用（Start → Args×N → End → Result）
	EventTypeToolCallStart  EventType = "TOOL_CALL_START"
	EventTypeToolCallArgs   EventType = "TOOL_CALL_ARGS"
	EventTypeToolCallEnd    EventType = "TOOL_CALL_END"
	EventTypeToolCallResult EventType = "TOOL_CALL_RESULT"

	// 状态 + 自定义
	EventTypeStateSnapshot EventType = "STATE_SNAPSHOT"
	EventTypeCustom        EventType = "CUSTOM"
)

// Event 是所有 AG-UI 事件的公共接口——SSE writer 用它拿 wire type + 时间戳编 frame。
//
// 接口方法含未导出项，故 Event 只能由本包构造（业务经 New* 构造函数建事件，
// 不自定义实现）——防止 wire 格式被业务写歪与前端不兼容。
type Event interface {
	// Type 返回 wire 事件类型。
	Type() EventType
	// timestampMs 返回事件毫秒时间戳（0 = 未设，writer 编码时补当前时刻）。
	timestampMs() int64
	// setTimestampMs 由 writer 在编码前补时间戳。
	setTimestampMs(ms int64)
}

// baseEvent 是所有事件的公共字段，值嵌入各事件——JSON flatten 到顶级，对齐
// AG-UI wire（type + timestamp）。各事件用 *XxxEvent 指针满足 Event 接口
// （setTimestampMs 是 pointer receiver）。
type baseEvent struct {
	EventType EventType `json:"type"`
	Timestamp int64     `json:"timestamp,omitempty"` // Unix 毫秒（AG-UI 约定，非 RFC3339）
}

func (b *baseEvent) Type() EventType         { return b.EventType }
func (b *baseEvent) timestampMs() int64      { return b.Timestamp }
func (b *baseEvent) setTimestampMs(ms int64) { b.Timestamp = ms }

// ── Run 生命周期 ──

// 这个适配层自己起的名字都带厂商前缀 `crew.`（AG-UI：CUSTOM 事件名与元数据键是应用空间，生产方 SHOULD 给自己
// 起的加厂商或应用前缀，免得两方扩展同一条流时撞名；不带前缀的名字留给协议自己将来用）。
const (
	// CustomEventError 是模型 / 工具层的中间错误（观测用，不是终态；终态是 RUN_ERROR）。value: {code, message}。
	CustomEventError = "crew.error"
	// CustomEventResetStream 通知前端丢弃某条消息已经收到的片段（流中途重试、OnModelEnd 钩子改写了答复）。
	// value: {run_id, reason}。
	CustomEventResetStream = "crew.reset_stream"
	// MetadataKeyApprovalOptions 是工具审批中断 metadata 里的四个选项（approve / approve_always / approve_always_tool / deny）。
	MetadataKeyApprovalOptions = "crew.options"
	// MetadataKeyToolName 是工具审批中断 metadata 里要审批的工具名。
	MetadataKeyToolName = "crew.toolName"
)

// ProtocolVersion 是这个适配层说的 AG-UI 协议版本（`MAJOR.MINOR`），在每个 RUN_STARTED 上声明。
const ProtocolVersion = "1.0"

// RunStartedEvent —— 一次 agent run 开始。
type RunStartedEvent struct {
	baseEvent
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
	// ProtocolVersion 是生产方自己说的协议版本，不是回显输入里的那个（AG-UI 1.0：实现了这个版本的生产方 MUST 在
	// RUN_STARTED 上声明；缺省表示版本化之前的生产方）。
	ProtocolVersion string `json:"protocolVersion,omitempty"`
}

// NewRunStarted 构造 RUN_STARTED。threadID 是会话聚类 ID（对应 AG-UI thread），
// runID 是本次运行 ID；protocolVersion 填 [ProtocolVersion]。
func NewRunStarted(threadID, runID string) *RunStartedEvent {
	return &RunStartedEvent{baseEvent: baseEvent{EventType: EventTypeRunStarted}, ThreadID: threadID, RunID: runID,
		ProtocolVersion: ProtocolVersion}
}

// RunFinishedOutcomeType 区分 run 结束的形态（成功 / 中断 / 取消）。
type RunFinishedOutcomeType string

const (
	RunOutcomeSuccess   RunFinishedOutcomeType = "success"
	RunOutcomeInterrupt RunFinishedOutcomeType = "interrupt"
	// RunOutcomeCancelled 是**跑到一半被停掉、但不是失败**的结局。1.0 定稿时才有
	// （此前判别联合只有 success/interrupt），而它的 schema 描述解释了为什么要赶在
	// 1.0 之前命名：「an outcome a consumer does not recognise is stripped and read
	// as success — a cancellation added later would reach every 1.0 consumer as a
	// completed run」。
	RunOutcomeCancelled RunFinishedOutcomeType = "cancelled"
)

// RunFinishedOutcome 描述 run 结束形态——interrupt 时带 HITL 中断信息。
type RunFinishedOutcome struct {
	Type       RunFinishedOutcomeType `json:"type"`
	Interrupts []RunInterrupt         `json:"interrupts,omitempty"`
}

// RunInterrupt 是一个 HITL 挂起点（对应 crew-go 的 AskQuery）。
type RunInterrupt struct {
	ID      string `json:"id"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
	// ToolCallID 是这一问针对的工具调用（工具审批时填；reason 为 tool_call 时规范要求带上）。
	ToolCallID string `json:"toolCallId,omitempty"`
	// ResponseSchema 是这一问期望的答复形状（JSON Schema），前端据此生成表单。
	ResponseSchema map[string]any `json:"responseSchema,omitempty"`
	// Metadata 是附加信息（工具审批时放四个选项与工具名，给自家前端用）。
	Metadata map[string]any `json:"metadata,omitempty"`
}

// RunFinishedEvent —— 一次 agent run 结束（成功或中断）。
type RunFinishedEvent struct {
	baseEvent
	ThreadID string              `json:"threadId"`
	RunID    string              `json:"runId"`
	Outcome  *RunFinishedOutcome `json:"outcome,omitempty"`
}

// NewRunFinished 构造成功结束的 RUN_FINISHED。
func NewRunFinished(threadID, runID string) *RunFinishedEvent {
	return &RunFinishedEvent{
		baseEvent: baseEvent{EventType: EventTypeRunFinished},
		ThreadID:  threadID,
		RunID:     runID,
		Outcome:   &RunFinishedOutcome{Type: RunOutcomeSuccess},
	}
}

// NewRunInterrupted 构造中断结束的 RUN_FINISHED（HITL 挂起，前端据此渲染追问 UI）。
func NewRunInterrupted(threadID, runID string, interrupts []RunInterrupt) *RunFinishedEvent {
	return &RunFinishedEvent{
		baseEvent: baseEvent{EventType: EventTypeRunFinished},
		ThreadID:  threadID,
		RunID:     runID,
		Outcome:   &RunFinishedOutcome{Type: RunOutcomeInterrupt, Interrupts: interrupts},
	}
}

// NewRunCancelled 构造**取消结束**的 RUN_FINISHED——run 跑到一半被停掉，但不是失败。
//
// ⚠️ **必须带 outcome，不能省**。规范原文（1.0 events/lifecycle）：
//
//	A producer that stops a run before completion, for a reason that is not a failure,
//	MUST close it with RUN_FINISHED carrying the cancelled outcome. It MUST NOT report
//	a stopped run as success — neither with the success outcome nor by omitting the outcome.
//
// 这里**曾经发的正是「省掉 outcome」那一版**，当时的判断是对的（那个快照的判别联合
// 确实只有 success/interrupt，塞 cancelled 会被前端 reject），但 1.0 落地 cancelled 之后
// 它就从「合法 back-compat」变成了违规——因为规范同时规定**缺省 outcome 意味着成功**，
// 于是我们的取消在 1.0 前端那里被读成了一个**成功完成的 run**。
//
// cancelled 分支是 closed 的（`unevaluatedProperties:false`）且只有 `type` 一个必填字段
// ——**不带 interrupts**（「a cancelled run has no interrupts to carry」），靠
// RunFinishedOutcome.Interrupts 的 omitempty 保证。
func NewRunCancelled(threadID, runID string) *RunFinishedEvent {
	return &RunFinishedEvent{
		baseEvent: baseEvent{EventType: EventTypeRunFinished},
		ThreadID:  threadID,
		RunID:     runID,
		Outcome:   &RunFinishedOutcome{Type: RunOutcomeCancelled},
	}
}

// RunErrorEvent —— run 出错。
//
// ⚠️ **没有 runId 字段，这是照规范来的**。1.0 的 RUN_ERROR 只有 type/message/code/usage，
// 且 schema 是 closed 的（`unevaluatedProperties:false`）——多发的字段会被消费者
// **strip 掉并 warn**。这里曾带一个 `runId`：在更早的快照上它靠 passthrough 能到前端，
// 1.0 起到不了了，于是变成「我们以为在发、实际没发出去」。删掉比留着更诚实。
type RunErrorEvent struct {
	baseEvent
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

// NewRunError 构造 RUN_ERROR。
func NewRunError(message, code string) *RunErrorEvent {
	return &RunErrorEvent{baseEvent: baseEvent{EventType: EventTypeRunError}, Message: message, Code: code}
}

// StepStartedEvent —— 一个编排步骤开始（crew-go node 可映射）。
type StepStartedEvent struct {
	baseEvent
	StepName string `json:"stepName"`
}

// NewStepStarted 构造 STEP_STARTED。
func NewStepStarted(name string) *StepStartedEvent {
	return &StepStartedEvent{baseEvent: baseEvent{EventType: EventTypeStepStarted}, StepName: name}
}

// StepFinishedEvent —— 一个编排步骤结束。
type StepFinishedEvent struct {
	baseEvent
	StepName string `json:"stepName"`
}

// NewStepFinished 构造 STEP_FINISHED。
func NewStepFinished(name string) *StepFinishedEvent {
	return &StepFinishedEvent{baseEvent: baseEvent{EventType: EventTypeStepFinished}, StepName: name}
}

// ── 文本消息（流式）──

// TextMessageStartEvent —— 一条流式文本消息开始。
type TextMessageStartEvent struct {
	baseEvent
	MessageID string `json:"messageId"`
	Role      string `json:"role,omitempty"`
}

// NewTextMessageStart 构造 TEXT_MESSAGE_START。role 一般传 "assistant"。
func NewTextMessageStart(messageID, role string) *TextMessageStartEvent {
	return &TextMessageStartEvent{baseEvent: baseEvent{EventType: EventTypeTextMessageStart}, MessageID: messageID, Role: role}
}

// TextMessageContentEvent —— 一段流式文本增量（delta）。
type TextMessageContentEvent struct {
	baseEvent
	MessageID string `json:"messageId"`
	Delta     string `json:"delta"`
}

// NewTextMessageContent 构造 TEXT_MESSAGE_CONTENT。delta 是本次 token 增量。
func NewTextMessageContent(messageID, delta string) *TextMessageContentEvent {
	return &TextMessageContentEvent{baseEvent: baseEvent{EventType: EventTypeTextMessageContent}, MessageID: messageID, Delta: delta}
}

// TextMessageEndEvent —— 一条流式文本消息结束。
type TextMessageEndEvent struct {
	baseEvent
	MessageID string `json:"messageId"`
}

// NewTextMessageEnd 构造 TEXT_MESSAGE_END。
func NewTextMessageEnd(messageID string) *TextMessageEndEvent {
	return &TextMessageEndEvent{baseEvent: baseEvent{EventType: EventTypeTextMessageEnd}, MessageID: messageID}
}

// ── 工具调用 ──

// ToolCallStartEvent —— 一次工具调用开始。
type ToolCallStartEvent struct {
	baseEvent
	ToolCallID   string `json:"toolCallId"`
	ToolCallName string `json:"toolCallName"`
}

// NewToolCallStart 构造 TOOL_CALL_START。
func NewToolCallStart(toolCallID, toolCallName string) *ToolCallStartEvent {
	return &ToolCallStartEvent{baseEvent: baseEvent{EventType: EventTypeToolCallStart}, ToolCallID: toolCallID, ToolCallName: toolCallName}
}

// ToolCallArgsEvent —— 一段工具调用参数增量。
type ToolCallArgsEvent struct {
	baseEvent
	ToolCallID string `json:"toolCallId"`
	Delta      string `json:"delta"`
}

// NewToolCallArgs 构造 TOOL_CALL_ARGS。delta 是参数 JSON 增量（crew-go 非流式参数时
// 一次性把完整 args 作为单个 delta 发）。
func NewToolCallArgs(toolCallID, delta string) *ToolCallArgsEvent {
	return &ToolCallArgsEvent{baseEvent: baseEvent{EventType: EventTypeToolCallArgs}, ToolCallID: toolCallID, Delta: delta}
}

// ToolCallEndEvent —— 一次工具调用（参数）结束。
type ToolCallEndEvent struct {
	baseEvent
	ToolCallID string `json:"toolCallId"`
}

// NewToolCallEnd 构造 TOOL_CALL_END。
func NewToolCallEnd(toolCallID string) *ToolCallEndEvent {
	return &ToolCallEndEvent{baseEvent: baseEvent{EventType: EventTypeToolCallEnd}, ToolCallID: toolCallID}
}

// ToolCallResultEvent —— 一次工具调用的执行结果。
type ToolCallResultEvent struct {
	baseEvent
	MessageID  string `json:"messageId"`
	ToolCallID string `json:"toolCallId"`
	Content    string `json:"content"`
	Role       string `json:"role,omitempty"` // 默认 "tool"
}

// NewToolCallResult 构造 TOOL_CALL_RESULT。messageID 关联产生此结果的消息。
func NewToolCallResult(messageID, toolCallID, content string) *ToolCallResultEvent {
	return &ToolCallResultEvent{
		baseEvent:  baseEvent{EventType: EventTypeToolCallResult},
		MessageID:  messageID,
		ToolCallID: toolCallID,
		Content:    content,
		Role:       "tool",
	}
}

// ── 状态 + 自定义 ──

// StateSnapshotEvent —— 一份完整状态快照（crew-go 超步 state 可映射）。
type StateSnapshotEvent struct {
	baseEvent
	Snapshot any `json:"snapshot"`
}

// NewStateSnapshot 构造 STATE_SNAPSHOT。
func NewStateSnapshot(snapshot any) *StateSnapshotEvent {
	return &StateSnapshotEvent{baseEvent: baseEvent{EventType: EventTypeStateSnapshot}, Snapshot: snapshot}
}

// CustomEvent —— 自定义事件（承载 AG-UI 原生无对应的 crew-go 概念，如 reset_stream /
// node 边界 / ForkLane，前端按 name 自行处理，标准组件忽略）。
type CustomEvent struct {
	baseEvent
	Name string `json:"name"`
	// Value 在 1.0 里是 **required**（可以是 null）——所以**不能加 omitempty**：
	// 业务 NewCustom(name, nil) 时 omitempty 会让整个字段消失，前端按 required 读不到。
	Value any `json:"value"`
}

// NewCustom 构造 CUSTOM。
func NewCustom(name string, value any) *CustomEvent {
	return &CustomEvent{baseEvent: baseEvent{EventType: EventTypeCustom}, Name: name, Value: value}
}
