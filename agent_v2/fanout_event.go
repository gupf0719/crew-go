package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// ────────────────────────────────────────────────────────────
// fanout event 协议——让 observability/业务订阅看 framework 内部 fanout 分支数.
//
// 跟 store / compact / cp event 同款 EventSink fire 设计:
//   - 仅在 fanout 真发生 (Branches >= 2) 时 fire — N=1 是 inline 不算 fanout
//   - observability/hooks.go::routeEventToMetric 识别 fire MetricFanoutBranches Histogram
//   - 业务订阅 OnNodeEvent 可看每超步 fanout 分支分布
//
// **当前 fire 点**: basic_agent.go::toolsFn 入口 (ISSUE-81 N>=2 goroutine 并发 fanout).
// **未来扩展**: ParallelAgent / 其他 fan-out 编排可同款 fire 共享 metric (label kind 区分).
// ────────────────────────────────────────────────────────────

// FanoutEventType 是 compose_v2.Event.Type 字符串. observability/hooks.go OnNodeEvent
// 按此值识别 fanout event 反序列化 Action 后 fire MetricFanoutBranches.
const FanoutEventType = "fanout"

// FanoutKind 区分 fanout 来源 (将来 ParallelAgent / Plan 等也可 fire 不同 kind).
type FanoutKind string

const (
	// FanoutKindToolCalls BasicAgent toolsFn N>=2 tool_call goroutine 并发 fanout (ISSUE-81).
	FanoutKindToolCalls FanoutKind = "tool_calls"
)

// FanoutAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段——业务订阅
// OnNodeEvent 按 Kind 分发处理.
type FanoutAction struct {
	// Kind fanout 来源类型.
	Kind FanoutKind `json:"kind"`
	// Branches fanout 分支数 (N>=2; N=1 inline 不 fire event).
	Branches int `json:"branches"`
}

// EmitFanoutEvent 投递 fanout event 到 compose_v2.EventSink.
//
// **不守 opt-in flag**: fanout 只在 N>=2 时 fire (caller 已判), 频率不高;
// 不像 compact/cp 频繁触发需要 opt-in 控制.
//
// **零开销路径**: ctx 内无 EventSink 时立刻 return (业务不订阅 event 流时不付任何代价).
func EmitFanoutEvent(ctx context.Context, kind FanoutKind, branches int) {
	if branches < 2 {
		return // N=1 inline 不算 fanout, 不 fire 避免 event 流噪音
	}
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type: FanoutEventType,
		Metadata: map[string]any{
			"action": &FanoutAction{Kind: kind, Branches: branches},
		},
	})
}
