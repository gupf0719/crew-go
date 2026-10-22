package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// ────────────────────────────────────────────────────────────
// route_decision event — RoutingAgent 的升级链定出最终目标时 fire，每次路由恰好一条,
// observability 路由到 crew.route.decisions 计数.
//
// 设计源头:
//   - crew-go 自定义 — 走 EventSink + observability 的 recordEventMetric 记指标
//     crew.route.decisions{gen_ai.agent.name, crew.route.target=branch_name/"__fallback__"/"__none__"}
//
// 设计核心:
//   - **升级链定出目标后 fire** — decisionRouter 在返回目标之前发，不管最终由决策方、
//     classifier 还是兜底分支决定
//   - **走 EventSink 协议** (NodeFunc scope 内 framework 自动 push nodeEventSink)
//   - **零开销 sink nil**: 业务不订阅时不付任何代价
//   - **router 路径不 fire** (业务显式 Router 函数业务自负统计 — observability 不能侵入)
//   - **指标只读 AgentName 与 Target**：Stage / Confidence / Escalations 这些字段给订阅
//     OnNodeEvent 的业务看升级率、置信度分布，不进指标标签
// ────────────────────────────────────────────────────────────

// RouteDecisionEventType 是 compose_v2.Event.Type 字符串. observability 的 OnNodeEvent
// 按此值识别 route_decision event，取 Action 记 crew.route.decisions.
const RouteDecisionEventType = "route_decision"

// RouteDecisionAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段——业务订阅
// OnNodeEvent 按 AgentName + Target 分发处理.
type RouteDecisionAction struct {
	// AgentName 当前 RoutingAgent 名 (区分多 RoutingAgent 嵌套场景).
	AgentName string `json:"agent_name"`
	// Target 选中的 branch name; "__fallback__" 表示 fallback 路径;
	// 框架自己不发空目标（没有兜底时路由失败是返回 RouteUndecidedError、不发事件）；
	// 业务自己调 EmitRouteDecisionEvent 发了空目标时，observability 映射为 "__none__".
	Target string `json:"target"`
	// Stage 是做出最终决定的那一档：RouteStageDecider、RouteStageClassifier 或
	// RouteStageFallback。
	Stage string `json:"stage,omitempty"`
	// DeciderIndex 是被采纳的决策方在 WithRouteDecider 挂载顺序里的下标，不算影子档。Stage 不是
	// RouteStageDecider 时恒为 0，没有意义。
	DeciderIndex int `json:"decider_index"`
	// Confidence 是被采纳的决策方给出的置信度，只在 Stage 为 RouteStageDecider 时非 nil。
	Confidence *float64 `json:"confidence,omitempty"`
	// Scores 是被采纳的决策方给出的各候选分数，决策方没给时为空。
	Scores map[string]float64 `json:"scores,omitempty"`
	// Source 是被采纳的决策方给出的来源标识，比如模型版本。
	Source string `json:"source,omitempty"`
	// ClassifierFailure 是 classifier 那一档没给出可用结果、因而转去兜底分支的原因：它返回
	// 的错误，它没给出目标，或它给出的目标不是候选分支，截断到 300 个字符。只在 Stage 为
	// RouteStageFallback 时可能有值；决策方都拿不准又没有 classifier 而直接落到兜底时为空。
	// 没有兜底分支时 classifier 失败不发事件，而是返回 RouteUndecidedError。
	ClassifierFailure string `json:"classifier_failure,omitempty"`
	// Escalations 是这次路由里没被采纳的决策方，按执行顺序，含影子档的记录（Shadow 为 true，
	// 见 [WithShadow]）；一档就定下来、前面也没有影子档时为空。
	Escalations []RouteEscalation `json:"escalations,omitempty"`
}

// RouteDecisionOf 从 [OrchestrationHooks] 的 OnNodeEvent 收到的事件里取出路由决策载荷；
// 不是路由决策事件时返回 false。业务订阅路由决策用它，不必自己对 Metadata 做类型断言。
// 手上是 compose_v2.Event 时先转换：RouteDecisionOf(agent_v2.Event(ev))。
func RouteDecisionOf(ev Event) (*RouteDecisionAction, bool) {
	if ev.Type != RouteDecisionEventType {
		return nil, false
	}
	a, ok := ev.Metadata["action"].(*RouteDecisionAction)
	return a, ok && a != nil
}

// EmitRouteDecisionEvent 投递 route_decision event 到 compose_v2.EventSink.
//
// **零开销路径**: ctx 内无 EventSink 时立刻 return (业务不订阅 event 流时不付任何代价).
// action 为 nil 时不发。
//
// 框架只在 RoutingAgent 的升级链里调它。业务自写的 Router 函数想让自己的决策也进
// crew.route.decisions，可以在返回目标之前自己调一次：
//
//	agent_v2.EmitRouteDecisionEvent(ctx, &agent_v2.RouteDecisionAction{
//	    AgentName: "support", Target: target,
//	})
func EmitRouteDecisionEvent(ctx context.Context, action *RouteDecisionAction) {
	if action == nil {
		return
	}
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type: RouteDecisionEventType,
		Metadata: map[string]any{
			"action": action,
		},
	})
}
