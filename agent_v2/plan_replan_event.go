package agent_v2

import (
	"context"
	"crew-go/compose_v2"
)

// ────────────────────────────────────────────────────────────
// plan_replan event — PlanAgent planNode 每次调 planner 时 fire (含初始 plan + N 次 replan).
//
// 设计源头:
//   - plan loop 计数无现成业界标准 metric, crew-go 自定义
//   - 走 EventSink + observability.routeEventToMetric 路由 fire
//     MetricPlanReplansTotal Counter (label{agent_name, phase=initial|replan})
//
// 设计核心:
//   - **PlanAgent planNode 入口 fire** — 拿 executed 长度区分 initial vs replan
//   - **走 EventSink 协议** (compose_v2.GetEventSink + nodeEventSink push 自动)
//   - **零开销 sink nil**: 业务不订阅 OrchHooks 时不付任何代价
// ────────────────────────────────────────────────────────────

// PlanReplanEventType 是 compose_v2.Event.Type 字符串.
const PlanReplanEventType = "plan_replan"

// PlanReplanPhase 区分初始 plan vs replan (业务可按此过滤).
type PlanReplanPhase string

const (
	// PlanReplanPhaseInitial 首次调 planner (executed 为空).
	PlanReplanPhaseInitial PlanReplanPhase = "initial"
	// PlanReplanPhaseReplan 后续 replan (executed 有内容, planner 看上一步 result 重规划).
	PlanReplanPhaseReplan PlanReplanPhase = "replan"
)

// PlanReplanAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段——业务订阅
// OnNodeEvent 按 Phase 分发处理.
type PlanReplanAction struct {
	// Phase 阶段 (initial / replan).
	Phase PlanReplanPhase `json:"phase"`
	// ExecutedSteps 当前已执行步数 (= len(executed)). 业务想看 plan 复杂度分布.
	ExecutedSteps int `json:"executed_steps"`
}

// emitPlanReplanEvent 投递 plan_replan event 到 compose_v2.EventSink.
//
// **零开销路径**: ctx 内无 EventSink 时立刻 return (业务不订阅 event 流时不付任何代价).
//
// caller (plan_agent.go planNode 入口) 按 executedLen == 0 区分 phase:
//
//	if len(executed) == 0 {
//	    emitPlanReplanEvent(ctx, PlanReplanPhaseInitial, 0)
//	} else {
//	    emitPlanReplanEvent(ctx, PlanReplanPhaseReplan, len(executed))
//	}
func emitPlanReplanEvent(ctx context.Context, phase PlanReplanPhase, executedSteps int) {
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type: PlanReplanEventType,
		Metadata: map[string]any{
			"action": &PlanReplanAction{
				Phase:         phase,
				ExecutedSteps: executedSteps,
			},
		},
	})
}
