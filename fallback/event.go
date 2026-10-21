package fallback

import (
	"context"
	"errors"
	"strconv"

	"crew-go/compose_v2"
	"crew-go/model"
)

// ────────────────────────────────────────────────────────────
// failover event 协议（sink-gated，和压缩 compact event 同款）
//
// 设计核心（对齐 agent_v2/compact_event.go）：
//   - **sink-gated（无独立开关）**：fire 与否由 ctx 有无 compose_v2.EventSink 决定——
//     业务接了 observability（sink 在）就自动观测 failover，不单独 opt-in；无 sink 时
//     立即 return，连 action/reason payload 都不构造（零开销）。
//   - **走 compose_v2.EventSink.Send**：fallback 在 model 层，只依赖已 import 的
//     compose_v2，不反向依赖 observability（那样会循环依赖）。observability 单向
//     import fallback 消费——见 observability/hooks.go::fireFailoverMetric。
//   - **仅观测**：业务订阅看到每次候选切换（from/to + 原因），不改控制流。真正的切换
//     决策在 policy.ShouldFailover。
// ────────────────────────────────────────────────────────────

// FailoverEventType 是 failover 切换事件的 compose_v2.Event.Type——observability 的
// OnNodeEvent 按此值识别并 fire failover metric（跟压缩 CompactionEventType 同款）。
const FailoverEventType = "failover"

// FailoverAction 是 compose_v2.Event.Metadata["action"] 内的结构化载荷——observability /
// 业务订阅方据此 fire metric / 观测每次候选切换。
type FailoverAction struct {
	// FromIdx 故障的候选下标（0 起）。
	FromIdx int `json:"from_idx"`
	// ToIdx 接下来真正去请求的候选下标：通常是 FromIdx+1；跳过熔断中的、或回到之前推迟的候选时不是（可以小于
	// FromIdx）。
	ToIdx int `json:"to_idx"`
	// Reason 触发切换的错误分类（低基数，适合 metric label）：rate_limit / timeout /
	// server_error / network / quota_exhausted / api_error / error。
	Reason string `json:"reason"`
}

// emitFailoverEvent sink-gated 投递 failover event（和压缩 EmitCompactionEvent 同款）。
// ctx 无 EventSink 时立即 return——业务不接 observability 时零开销，不构造 action/reason。
func emitFailoverEvent(ctx context.Context, fromIdx, toIdx int, err error) {
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return // 零开销门：无 sink 不构造 payload（同 compact_event.go hasEventSink 语义）
	}
	action := &FailoverAction{FromIdx: fromIdx, ToIdx: toIdx, Reason: failoverReason(err)}
	sink.Send(compose_v2.Event{
		Type:     FailoverEventType,
		Content:  strconv.Itoa(fromIdx) + "->" + strconv.Itoa(toIdx),
		Metadata: map[string]any{"action": action},
	})
}

// ────────────────────────────────────────────────────────────
// member.tripped event 协议——断路器熔断（closed→open 边缘）观测。
//
// 与 failover 正交：failover 是"从一个候选换到另一个候选去请求"（成员间切换），tripped 是
// "候选 i 的断路器连续失败达阈值熔断"。**候选失败后没有别的候选可请求时只 emit tripped、
// 不 emit failover**（之后再没有请求可切）——这正是 tripped 的独立价值：failover.total 看不到
// "全员挂光"。一次熔断只 emit 一次（RecordFailure 边缘检测，探测失败刷新 cooldown 但不重复 emit）。
// 有推迟时最后一个候选失败之后还会回到推迟的候选（见 tryMembers），那时照样发一次 failover，
// from 是最后那个候选。
// ────────────────────────────────────────────────────────────

// MemberTrippedEventType 是断路器熔断事件的 Event.Type——observability OnNodeEvent 按此
// 值 fire member.tripped metric（同 FailoverEventType 机制）。
const MemberTrippedEventType = "member_tripped"

// MemberTrippedAction 是 Event.Metadata["action"] 内的熔断结构化载荷。
type MemberTrippedAction struct {
	// MemberIdx 被熔断的候选下标（0 起）。
	MemberIdx int `json:"member_idx"`
	// Reason 触发熔断的错误分类（低基数，复用 failoverReason 口径）。
	Reason string `json:"reason"`
}

// emitMemberTrippedEvent sink-gated 投递断路器熔断 event（同 emitFailoverEvent 零开销门）。
func emitMemberTrippedEvent(ctx context.Context, memberIdx int, err error) {
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return // 零开销门：无 sink 不构造 payload
	}
	action := &MemberTrippedAction{MemberIdx: memberIdx, Reason: failoverReason(err)}
	sink.Send(compose_v2.Event{
		Type:     MemberTrippedEventType,
		Content:  "member_" + strconv.Itoa(memberIdx) + "_tripped",
		Metadata: map[string]any{"action": action},
	})
}

// failoverReason 把触发切换的 err 归类成稳定低基数 reason 标签（作 metric label 防基数爆）。
// 复用 policy 的 APIError 结构化分类 + isQuotaExhausted，与 ShouldFailover 判定口径一致。
func failoverReason(err error) string {
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.StatusCode == 429:
			// OpenAI 用 429 表达配额耗尽（insufficient_quota）——先判 quota 再落 rate_limit，
			// 对齐 policy.go isQuotaExhausted 意图（否则 429 quota 被误标 rate_limit 误导运维：
			// rate_limit 退避自愈 vs quota_exhausted 需充值/换账号，runbook 不同）。
			if isQuotaExhausted(apiErr.Body) {
				return "quota_exhausted"
			}
			return "rate_limit"
		case apiErr.StatusCode == 408:
			return "timeout"
		case apiErr.StatusCode >= 500:
			return "server_error"
		case apiErr.StatusCode == 0:
			return "network"
		case isQuotaExhausted(apiErr.Body):
			return "quota_exhausted"
		default:
			return "api_error"
		}
	}
	return "error"
}
