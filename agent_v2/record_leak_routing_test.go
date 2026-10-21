package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// routeToolCall 造一条 classifier 的 route_to 决策（return-direct，Content 为空）。
func routeToolCall(callID, target string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: callID, Type: "function",
		Function: schema.FunctionCall{
			Name: agent_v2.RouteTool, Arguments: `{"target":"` + target + `"}`,
		},
	}})
}

// TestRecordLeak_RoutingStillRoutesAcrossRuns 钉住 dbcd524 那条回归在 **RoutingAgent**
// 上的失败面（审计视角一 HIGH-1）——跟 TestRecordLeak_NotUsedAsFinalAnswer 同根因，
// 但表现完全不同，且**业务躲不掉**，所以单列一条。
//
// 机制：classifier 的 Exit 是 `route_to` 的 tool_call assistant（return-direct，Content 空）。
// 第 2 个 Run 里它进 prior 成悬空 → 被换成记账文本 → 而记账文本是「assistant + 零
// tool_call」，正好命中 `lastAssistant` 的首选判据 ⟹ 它**遮蔽了本轮真正的 route_to 决策**。
// `extractRouteTarget` 拿到记账文本：无 tool_call、无 route_target annotation，落到文本
// 关键词兜底，而整段记账文本不在 branchSet ⟹ **从第 2 个 Run 起静默走 fallback**；
// 没配 fallback 时直接把框架内部记账文本返给调用方。err 全程为 nil。
//
// **为什么业务躲不掉**：classifier 走 `WithoutHistoryCtx`（`routing_agent.go`），
// `loadHistory` 对它恒返 (nil,false) ⟹ prior 恒是 session events 源 ⟹ 开不开
// `WithHistory` 都一样。
//
// 修法：最终产出改由决策点写进专用 channel，记账文本在 preamble 侧合成、结构上进不去
// （当年那个倒扫消息列表的 `lastAssistant` 已作为死代码删除）。`UnresolvedCallsRecordAnnotation`
// 仍然要打——但只剩给「还得靠扫消息挑一条代表」的地方用（observability 的 agent_output、
// ToolFilter 的 lastInput、以及业务自己扫 messages 的代码）。
func TestRecordLeak_RoutingStillRoutesAcrossRuns(t *testing.T) {
	// classifier 每轮都正确返回 route_to(math)——路由结果如果变了，只可能是框架的问题
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			routeToolCall("r1", "math"),
			routeToolCall("r2", "math"),
			routeToolCall("r3", "math"),
		})),
		agent_v2.WithRouteTool(),
	)
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("MATH 已处理"),
			schema.AssistantMessage("MATH 已处理"),
			schema.AssistantMessage("MATH 已处理"),
		})),
	)
	fallback := agent_v2.NewBasicAgent(
		agent_v2.WithName("fb"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("FALLBACK 已处理"),
			schema.AssistantMessage("FALLBACK 已处理"),
			schema.AssistantMessage("FALLBACK 已处理"),
		})),
	)

	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(fallback),
	).AddBranch("math", math)

	// **同一个 RunSession 连跑三轮**——events 跨 Run 累积正是触发条件
	sess := agent_v2.NewRunSession()
	for run := 1; run <= 3; run++ {
		ev, err := rt.Run(context.Background(), sess, schema.UserMessage("1+1"))
		if err != nil {
			t.Fatalf("Run %d: %v", run, err)
		}
		final := agent_v2.FinalMessage(ev)
		if final == nil {
			t.Fatalf("Run %d 没有最终消息", run)
		}
		t.Logf("Run %d final=%.80q", run, final.Content)

		// 承重①：绝不能把框架内部记账文本返给调用方
		if strings.Contains(final.Content, "[历史记录]") {
			t.Fatalf("Run %d：框架内部记账文本被当成答复返出：%q", run, final.Content)
		}
		// 承重②：classifier 每轮都说 math，就必须每轮都走 math——
		// 走到 fallback 说明路由决策被记账文本顶掉了（且 err==nil，完全静默）
		if !strings.Contains(final.Content, "MATH") {
			t.Fatalf("Run %d：classifier 明确路由到 math，实际落到别处：%q", run, final.Content)
		}
	}
}
