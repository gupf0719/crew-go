package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestRouteSignal_NoResidueAcrossRuns 钉住「路由信号绝不残留到下一次 Run」。
//
// 信号在 `route_to` **执行那一刻**就写进 session 了，而 classify 之后有四条出口
// （中断 / ctx 取消 / 业务错走 fallback / 正常），只在成功路径消费的话，同轮 fanout 里
// route_to 跑成功、兄弟工具抛错，信号就永远留下了。下一次 Run 用户问了个全新的问题，
// 却被送去上一次的分支——**而且全程 err == nil**，业务毫无察觉。
//
// 这条缺陷是从转交那侧的「按位置算键 + 读后即清」范式**连同它的洞一起镜像**过来的。当时的修法
// 是在 classify 出口用 `defer` 清 session KV。
//
// 现在路由是上行信号（见 side_signal.go）：它跟着写入者那段执行的提交走，classifier 这一轮以业务错
// 收尾时那段执行没成，信号随它作废——**结构上**留不下残留，不再靠任何一处记得清。用例留着：它盯的
// 是「跨 Run 会不会串」这个行为，跟信号存哪儿无关。
func TestRouteSignal_NoResidueAcrossRuns(t *testing.T) {
	// 兄弟工具：硬报错，让 classifier 那一轮走「业务错」出口（err==nil 走 fallback）
	boom := tool.NewFuncTool("boom", "always fails",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return nil, errors.New("下游 500")
		})
	tk := tool.NewToolkit()
	tk.Register(boom)

	// 第 1 轮：同轮吐 route_to(math) + boom ⟹ route_to 先写了信号，boom 让整轮失败
	round1 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r1", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}},
		{ID: "b1", Type: "function", Function: schema.FunctionCall{
			Name: "boom", Arguments: `{}`}},
	})

	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			round1,
			schema.AssistantMessage("我不确定该给谁"), // 第 2 轮：明确不路由
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(3),
	)
	math := agent_v2.NewBasicAgent(agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("MATH 已处理"), schema.AssistantMessage("MATH 已处理")})))
	fb := agent_v2.NewBasicAgent(agent_v2.WithName("fb"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("FALLBACK 已处理"), schema.AssistantMessage("FALLBACK 已处理")})))

	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(fb),
	).AddBranch("math", math)

	sess := agent_v2.NewRunSession()

	// Run 1：classifier 那轮失败 → 走 fallback。**对业务 err == nil**
	ev1, err := rt.Run(context.Background(), sess, schema.UserMessage("第一个问题"))
	if err != nil {
		t.Fatalf("Run 1 不该报错（classifier 失败是 best-effort 降级）：%v", err)
	}
	if f := agent_v2.FinalMessage(ev1); f == nil || !strings.Contains(f.Content, "FALLBACK") {
		t.Fatalf("前提没成立——Run 1 应降级走 fallback，实际 %v", f)
	}

	// Run 2：**全新的问题**，classifier 明确答普通文本、这轮压根没调 route_to
	ev2, err := rt.Run(context.Background(), sess, schema.UserMessage("完全不相干的新问题"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	final := agent_v2.FinalMessage(ev2)
	if final == nil {
		t.Fatal("Run 2 没有最终消息")
	}
	// 承重：Run 2 没有任何路由决策，必须走 fallback。走到 math = 被上一轮的残留劫持了
	if !strings.Contains(final.Content, "FALLBACK") {
		t.Fatalf("Run 2 被上一次 Run 的残留信号劫持了（err 全程 nil，业务毫无察觉）：%q", final.Content)
	}
}

// TestRouteSignal_WorksWhenNested 验嵌套编排下路由决定端到端可用（分类器做出、RoutingAgent 取到）。
//
// ⚠️ **它不验收件人维度的隔离**，别照名字误解。收件人只在同一条链上有两个 RoutingAgent 时承重
// （内层分类器做出的路由归内层，外层不能取走），由 TestNestedEffects_InnerRoutingAgentWritesAreNotEscapes
// 守。曾经担心的「两个顶层 RoutingAgent 在同一个 session 上并发、只靠名字分开」那格随路由的 session KV
// 一起消失了——路由决定不再落 session，只在做出它的那段执行里往上走。
func TestRouteSignal_WorksWhenNested(t *testing.T) {
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("我不确定该给谁"), // 第 1 轮：不路由
			routeToolCall("r2", "math"),        // 第 2 轮：明确路由
		})),
		agent_v2.WithRouteTool(),
	)
	math := agent_v2.NewBasicAgent(agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("MATH 已处理"), schema.AssistantMessage("MATH 已处理")})))
	fb := agent_v2.NewBasicAgent(agent_v2.WithName("fb"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("FALLBACK 已处理"), schema.AssistantMessage("FALLBACK 已处理")})))

	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(fb),
	).AddBranch("math", math)

	// **套一层 Sequential**——让 RoutingAgent 的 graphNS 非空，ns push 才可能被验到
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("route", rt)

	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(context.Background(), sess, schema.UserMessage("先随便问")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	ev, err := seq.Run(context.Background(), sess, schema.UserMessage("1+1"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	final := agent_v2.FinalMessage(ev)
	if final == nil || !strings.Contains(final.Content, "MATH") {
		t.Fatalf("嵌套下第 2 轮路由失效——ns 维度没生效，信号写/读对不上：%v", final)
	}
}

// TestRouteSignal_MultipleRouteCalls_FirstWins 钉住同轮多个 route_to 的裁决：排在前面的那个生效，
// 而且恒定。
//
// LLM 一轮吐多个**目标不同**的 route_to 是真实存在的。裁决必须恒定——否则同一个 prompt 会在不同进程 /
// 不同轮次路由到不同分支，那是最难复现的一类线上问题。批内顺序来自那条已提交的模型消息，续跑时不变。
//
// ⚠️ 2026-08 曾改成「拒绝」：理由是「替模型挑一个等于把一个任意选择伪装成决定，业务看不到发生过分歧」。
// 那条理由的前半成立、后半可以补上：被盖掉的那个 route_to 的结果换成一条失败结果（模型看得到这一次没有
// 生效），框架记一条告警——分歧看得见。而「拒绝」的代价没被算进去：这批 tool_call 续跑时从工具账原样
// 重放、必然再撞，任务永远过不去（审计实测：续跑 3 次都撞同一个冲突）。同类产品在这一处也都不让 Run 失败
// （取第一个并告诉模型其余被忽略，或者干脆不让模型一次发多个）。
func TestRouteSignal_MultipleRouteCalls_FirstWins(t *testing.T) {
	twoRoutes := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r1", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}},
		{ID: "r2", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.RouteTool, Arguments: `{"target":"coder"}`}},
	})
	// 连跑多次：非确定性裁决会在某几次翻到 coder
	for i := 0; i < 8; i++ {
		classifier := agent_v2.NewBasicAgent(
			agent_v2.WithName("classifier"),
			agent_v2.WithModel(newStubModel([]*schema.Message{twoRoutes})),
			agent_v2.WithRouteTool(),
		)
		math, mathModel := scopeLeaf("math", "MATH 已处理")
		coder, coderModel := scopeLeaf("coder", "CODER 已处理")
		rt := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("router"),
			agent_v2.WithRoutingClassifier(classifier),
		).AddBranch("math", math).AddBranch("coder", coder)

		if _, err := rt.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q")); err != nil {
			t.Fatalf("第 %d 次：同一批两个路由目标不该让 Run 失败：%v", i+1, err)
		}
		if m, c := mathModel.count(), coderModel.count(); m != 1 || c != 0 {
			t.Fatalf("第 %d 次：排在前面的 route_to（math）应生效：math 被调 %d 次、coder %d 次", i+1, m, c)
		}
	}
}
