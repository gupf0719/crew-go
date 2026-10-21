package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// 四编排 review RT-M3 回归：框架内置 return-direct route_to（WithRouteTool + 路由授予）+
// 真实 BasicAgent classifier 端到端——补 priority-1（tool_call 信号）的真实
// ReAct 路径覆盖。此前唯一覆盖是 fixedAgent 直返 tool_call 消息（绕开 ReAct，
// CLAUDE.md "mock 闭包绕开真实路径不算端到端"陷阱实例）：真实 classifier 跑
// ReAct 时只有 return-direct 形态能让 final 是 tool_call 消息。
//
// sanity-revert：去掉 route_to 工具结果的 ReturnDirectMetaKey → 工具跑完回 model →
// final 变 stub 后续文本 → priority-1 不命中 → 路由错 → fail。

func TestRT_M3_RouteTool_RealClassifierEndToEnd(t *testing.T) {
	// classifier：真实 BasicAgent + 内置 route_to（WithRouteTool）。
	// stub model 第 1 轮发 route_to tool_call；若 return-direct 失效会再被调，
	// 返回 "LEAKED_SECOND_ROUND"（文本路由必不中分支名 → 没配兜底时 Run 返回 RouteUndecidedError）。
	tc := schema.ToolCall{ID: "tc_route", Function: schema.FunctionCall{
		Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}}
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("rt_m3_classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
			schema.AssistantMessage("LEAKED_SECOND_ROUND"),
		})),
		agent_v2.WithRouteTool(),
	)

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt_m3_router"),
		agent_v2.WithRoutingClassifier(classifier),
	).
		AddBranch("math", &fixedReplyAgent{name: "math_wk", reply: "MATH_BRANCH_DONE"}).
		AddBranch("coder", &fixedReplyAgent{name: "coder_wk", reply: "CODER_BRANCH_DONE"})

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("1+1?"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("no final（priority-1 未命中时会以 RouteUndecidedError 收尾，走不到这里）")
	}
	if !strings.Contains(final.Content, "MATH_BRANCH_DONE") {
		t.Fatalf("RT-M3: 真实 classifier + route_to 应经 priority-1 路由到 math 分支，got %q", final.Content)
	}
}

// TestRT_M3_RouteTool_EmptyTargetSoftFailure 验证 target 空时软失败回灌
// （RouteTargetInvalidErrCode 业务级失败，LLM 收到错误后重选，不 abort Run）。
func TestRT_M3_RouteTool_EmptyTargetSoftFailure(t *testing.T) {
	tcBad := schema.ToolCall{ID: "tc_bad", Function: schema.FunctionCall{
		Name: agent_v2.RouteTool, Arguments: `{"target":""}`}}
	tcGood := schema.ToolCall{ID: "tc_good", Function: schema.FunctionCall{
		Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}}
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("rt_m3_retry_cls"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tcBad}),  // 第 1 轮空 target → 软失败回灌
			schema.AssistantToolCallMessage([]schema.ToolCall{tcGood}), // 第 2 轮重选成功
		})),
		agent_v2.WithRouteTool(),
	)

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt_m3_retry"),
		agent_v2.WithRoutingClassifier(classifier),
	).AddBranch("math", &fixedReplyAgent{name: "math_wk2", reply: "MATH_OK"})

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "MATH_OK") {
		t.Fatalf("RT-M3: 空 target 软失败应回灌让 LLM 重选后路由成功，got %v", final)
	}
}
