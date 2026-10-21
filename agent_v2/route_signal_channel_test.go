package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestRouteSignal_SurvivesStaleFinalMessage 钉住「路由目标走独立通道，不依赖最终消息的结构」。
//
// 改这条之前，路由目标是从最终消息的 tool_call 里解析的（`extractRouteTarget` priority-1）。
// 而最终消息**不可靠**——当时取最终消息的 `lastAssistant`（已删）扫的列表含 prior 且不区分「本轮产出」和「历史」，
// 于是 classifier 只要**有过一次普通文本答复**（「我不确定」在真实分类器里很常见），
// 此后每一轮的 route_to 决策都被那条旧文本遮蔽 ⟹ 解析不出目标 ⟹ **静默走 fallback，
// err 全程 nil**。
//
// 本用例复现该场景：第 1 轮 classifier 答普通文本，第 2 轮它明确 route_to(math)。
// 改造前第 2 轮落 fallback；改造后照走 math——目标在 route_to 执行那一刻就作为上行信号
// 交给了它所在的那段执行（跟着提交一路交到 RoutingAgent），跟最终消息长什么样无关。
//
// ⚠️ 别改用「hook 剥掉 tool_call」来模拟改写：`OnModelEnd` 是 [消费] 档，剥了之后
// tools 节点根本不执行、`route_to` 压根没跑——那测的是「工具没跑」不是「消息被改写」。
func TestRouteSignal_SurvivesStaleFinalMessage(t *testing.T) {
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("我不确定该给谁"), // 第 1 轮：普通文本
			routeToolCall("r2", "math"),        // 第 2 轮：明确路由
		})),
		agent_v2.WithRouteTool(),
	)
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("MATH 已处理"),
			schema.AssistantMessage("MATH 已处理"),
		})),
	)
	fb := agent_v2.NewBasicAgent(
		agent_v2.WithName("fb"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("FALLBACK 已处理"),
			schema.AssistantMessage("FALLBACK 已处理"),
		})),
	)

	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(fb),
	).AddBranch("math", math)

	// **同一个 session 连跑两轮**——classifier 的 events 跨 Run 累积正是触发条件
	sess := agent_v2.NewRunSession()
	if _, err := rt.Run(context.Background(), sess, schema.UserMessage("随便问问")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	ev, err := rt.Run(context.Background(), sess, schema.UserMessage("1+1"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	final := agent_v2.FinalMessage(ev)
	if final == nil {
		t.Fatal("Run 2 没有最终消息")
	}
	// 承重：classifier 第 2 轮明确 route_to(math)，就必须走 math
	if !strings.Contains(final.Content, "MATH") {
		t.Fatalf("第 2 轮路由失效（落到别处）：%q\n"+
			"classifier 明确路由到 math，说明路由仍依赖最终消息的结构——"+
			"而那条消息被第 1 轮的陈旧答复遮蔽了", final.Content)
	}
}
