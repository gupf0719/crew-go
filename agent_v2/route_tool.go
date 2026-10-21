package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// RouteTargetInvalidErrCode 是路由目标非法的 tool error code（业务级软失败，
// 反馈 LLM 让其重选——不在 tool.IsFrameworkError 列表，不 abort 整 Run）。
const RouteTargetInvalidErrCode = "route_target_invalid"

// RouteNoCoordinatorErrCode 是 route_to 这次定不了路由时的 tool error code（业务级软失败，模型改成
// 直接作答）：本 agent 没有被授予路由能力（模型硬发 route_to），或被授予了却没有在等的 RoutingAgent
// （例如经 DetachTask 开的独立任务里，交了也没人取）。
const RouteNoCoordinatorErrCode = "route_no_coordinator"

// newRouteTool 返回框架内置 route_to 工具（return-direct）——RoutingAgent
// classifier 专用，四编排 review RT-M3 引入。
//
// **只由路由授予的持有者构造**——BasicAgent 每次 Run 确认手上这份授予是发给自己的（且自己
// opt-in 了 [WithRouteTool]）之后才建它。没被授予的 agent 连这件工具都不存在，产生不出路由信号
// （见 routeGrant）。曾经导出、谁都能 new 一个注册进任意 agent 的 toolkit——那正是「资格是环境」
// 的一条来路。
//
// **为何必须 return-direct**（probe 实证）：classifier 是真实
// BasicAgent 跑 ReAct 循环——(a) 只注入 schema.ToolInfo 无实现 → tools 节点
// 报错被吞静默 fallback；(b) 普通 FuncTool → 它不做出路由决定，工具跑完回 model，
// final 是后续文本，兜底解析也取不到目标；(c) 仅 return-direct
// （否则工具跑完回 model，本轮的产出是后续文本而不是那次决策）可用。本仓 HandoffAgent
// transfer_to_agent 也是框架内置同款——业务手写三种写法仅一种能用，框架直接
// 提供唯一正确形态。
//
// 行为：
//  1. target 空 → RouteTargetInvalidErrCode 软失败回灌（LLM 重选）
//  2. 没有在等的 RoutingAgent（授予在、收件人不在）→ RouteNoCoordinatorErrCode 软失败，不短路
//  3. 否则 → **把目标作为上行信号交给所在的那段执行**（见 side_signal.go），由 RoutingAgent
//     在分类器那次调用成功之后取；并标 tool.ReturnDirectMetaKey 一调即出，ack 文本仅留审计
//     （return-direct 短路后不回 LLM）。目标合法性由 routing 层校验（非法走
//     WithRoutingFallback 兜底——工具内不重复校验，候选集是 RoutingAgent 的属性非工具的）。
//
// **为什么目标走独立通道而不是让下游从 final 消息里解析**（原先是后者）：
// 「这次的答案是什么」和「接下来去哪」是两件事，塞进同一条消息会让下游为了拿
// 控制流被迫理解值的内部结构。而那条消息**是会被改写的**——`OnModelEnd` 的
// `output.Message` 是 [消费] 档 hook 可改、4 层压缩会重写、上层可能重新合成，
// 任何一处动了结构路由就跟着坏，且是静默的（落 fallback，err==nil）。
//
// **为什么是上行信号而不是消费方准备的容器**（曾经写进 session KV）：决定得跟着做出它的那段执行
// 的提交走。写进活容器的话写入当场生效——分类器是两步图、定路由那一步已提交而下一步问人时，续跑
// 不再执行那一步、路由丢失落到兜底；并行两步各定各的时谁后写谁赢（均实测，`err == nil`）。
// 现在先后两步各定一次时后者为准（分类器是 Sequential / Loop，逐步细化）；同一超步的两个节点都定了
// 路由是图的设计错误，当场终局（compose_v2.SignalConflictError 并带 ErrRunAborted，见 side_signal.go）；
// 同一批里两次 route_to 排在前面的生效，后面那次回一条「没有生效」的失败结果（见 tool_batch_signals.go）。
func newRouteTool() *tool.FuncTool {
	return tool.NewFuncTool(
		RouteTool,
		"把用户 query 路由到目标分支。根据各分支职责选择最合适的 target，选定后立即调用本工具，不要自己回答用户问题。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"target": map[string]any{
					"type":        "string",
					"description": "目标分支名（必须是候选分支之一）。",
				},
			},
			"required": []string{"target"},
		},
		routeToolFunc,
	)
}

// routeDestKey 是 ctx-value key——RoutingAgent 的 classify 节点调分类器之前放进自己的收件人，
// route_to 工具读它。
type routeDestKey struct{}

// routeDest 是 RoutingAgent 的收件人，在 classify 节点的执行里铸（compose_v2.SignalRecipient，
// label 是它的名字）。
//
//   - 收件人用来分开**同一条链上**的两个 RoutingAgent（一个嵌在另一个的分类器或分支里）：内层
//     的分类器做出的路由归内层，不能被外层取走。两层各有自己的图（ns 不同）；名字这一维兜底
//     「同一个节点执行里跑着两个」的情形（例如在节点里裸调另一个）；
//   - 续跑时算出同一个值（密钥随存档恢复），所以上一个 Run 写下、存在分类器存档里的决定续跑后仍
//     认得出是发给它的；
//   - 节点外的代码算不出来：曾经是「名字@ns」，被当工具用的孙辈摘了收件人照样拼得出来、手写一份
//     信号交上去（见 side_signal.go）。
func routeDest(ctx context.Context, agentName string) string {
	r := compose_v2.SignalRecipient(ctx, agentName)
	if r == "" {
		return ""
	}
	return "route:" + r
}

// withRouteDest push RoutingAgent 的收件人（存成 *sealedRecipient：打印 ctx 时不露出来，见其 godoc）。
func withRouteDest(ctx context.Context, dest string) context.Context {
	return withRecipient(ctx, routeDestKey{}, dest)
}

// withoutRouteDest 把收件人从 ctx 摘掉——**agent 被当工具用时，它的路由决策不许逃出去**。
//
// 与 [withoutHandoffDest] 是同一条理由的两半：那两处（AgentAsTool / skill fork）把一个 agent
// 降格成「一件能力」，调用方要的是它的**产出**，不是让它替调用方决定这场对话该走哪条分支。
//
// 不摘的后果是**静默劫持**：ctx 天然下传 ⟹ 内层继承的收件人与 classifier 完全相同 ⟹ 它做出的
// 路由正是 classifier 该做的那个。实测 classifier 选了 A、最终路由去了内层选的 B，err 全程为 nil
// （哨兵 TestRouteDest_NotInheritedByAgentAsTool）。
//
// **摘的是「有没有人在等」这一位**（`getRouteDest` 的第二个返回值），不是把收件人置空。
func withoutRouteDest(ctx context.Context) context.Context {
	if _, ok := getRouteDest(ctx); !ok {
		return ctx
	}
	return context.WithValue(ctx, routeDestKey{}, nil)
}

// getRouteDest 返 (收件人, 有没有 RoutingAgent 在等)。
func getRouteDest(ctx context.Context) (string, bool) {
	return recipientFrom(ctx, routeDestKey{})
}

// routeToolFunc 是 route_to 工具执行体。
func routeToolFunc(ctx context.Context, args map[string]any) (*tool.Result, error) {
	target, _ := args["target"].(string)
	if target == "" {
		return tool.Failure(RouteTargetInvalidErrCode, "target 不能为空，请填目标分支名"), nil
	}
	// 把目标交给所在的那段执行（这次工具调用）：它跟着这次调用的提交走——调用成功并提交才算数，
	// 所在的节点续跑时命中工具账、函数体不再跑，账里记着它（见 side_signal.go）。
	//
	// **没有 RoutingAgent 在等就回软失败**：被授予了、收件人却不在（典型是业务经 DetachTask 开的独立
	// 任务、又显式把授予交了过去），交了也没人取。曾经照样回「已路由到 X」并短路收尾——模型以为定了
	// 路由，实际什么也没发生（审计实测）。同 transfer_to_agent 拿不到收件人时的处置。
	dest, ok := getRouteDest(ctx)
	if !ok {
		return tool.Failure(RouteNoCoordinatorErrCode,
			"这儿没有 RoutingAgent 在等这次路由（这次调用不在分类器那段执行里）——请直接作答"), nil
	}
	compose_v2.EmitSignal(ctx, routeSignal, compose_v2.Signal{Dest: dest, Value: target, Origin: signalOrigin(ctx, RouteTool)})
	return &tool.Result{
		Success:  true,
		Output:   fmt.Sprintf("已路由到 %s。", target),
		Metadata: map[string]any{tool.ReturnDirectMetaKey: true},
	}, nil
}

// WithRouteTool 声明「这个 agent 愿意替 RoutingAgent 定路由」：被授予路由能力时（它是
// RoutingAgent 的分类器，或分类器显式转授给它），框架给它注册内置 route_to 工具，让 LLM 经
// tool-call 输出结构化路由决策（信号优先级 #1，避免文本解析歧义）。跟 [WithHandoffTransfer]
// 一样是**两把闸**之一：opt-in 回答「它愿不愿意」，授予回答「这次调用里它有没有资格」（见
// routeGrant），两把都开才有这件工具。
//
// 不往 cfg.Toolkit 里塞东西（工具由框架每次 Run 注入），所以跟 WithToolkit 的先后无关。
//
// **失败模式**：忘配此 option、或分类器是一层业务壳而它没把授予显式转给内层
// （[RegrantRouteTo]）时，内层 LLM 只能输出文字，路由退化到最后一种解析方式（去掉首尾空白后整段
// 文本等于分支名，格式敏感）。
func WithRouteTool() Option {
	return func(c *Config) { c.RouteToolOptIn = true }
}

// newRouteToolStub 是没被授予路由能力时挂在 dispatch 侧的软失败桩（bind 侧没有这件工具，LLM 看
// 不到它）：模型从历史里照着发了一个 route_to 时拿到「没有被授予」、改成直接作答，而不是整个
// tools 节点报 tool-not-found。同 newHandoffTransferStub。
func newRouteToolStub() *tool.FuncTool {
	t := newRouteTool()
	return tool.NewFuncTool(RouteTool, t.Description(), t.InputSchema(),
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Failure(RouteNoCoordinatorErrCode,
				"本 agent 没有被授予路由能力（路由由 RoutingAgent 授予它的分类器）——请直接作答"), nil
		})
}
