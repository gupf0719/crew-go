package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestRoutingAgent_BasicDispatch 验证 Router 按 input 选 1 分支跑——math agent 应被命中。
func TestRoutingAgent_BasicDispatch(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("answer: 4")})),
	)
	coder := agent_v2.NewBasicAgent(
		agent_v2.WithName("coder"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("code")})),
	)

	router := func(_ context.Context, input *schema.Message) (compose_v2.NodeID, error) {
		if strings.Contains(input.Content, "math") || strings.Contains(input.Content, "+") {
			return "math", nil
		}
		return "coder", nil
	}

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRouter(router),
	).
		AddBranch("math", math).
		AddBranch("coder", coder)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("solve 2+2 math"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "answer: 4") {
		t.Errorf("expect math branch hit, got %v", final)
	}
}

// TestRoutingAgent_AlternateBranch 验证非匹配走 coder 分支。
func TestRoutingAgent_AlternateBranch(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
	)
	coder := agent_v2.NewBasicAgent(
		agent_v2.WithName("coder"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("hello world")})),
	)

	router := func(_ context.Context, input *schema.Message) (compose_v2.NodeID, error) {
		if strings.Contains(input.Content, "math") {
			return "math", nil
		}
		return "coder", nil
	}

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRouter(router),
	).
		AddBranch("math", math).
		AddBranch("coder", coder)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("write hello world"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "hello world") {
		t.Errorf("expect coder branch hit, got %v", final)
	}
}

// TestRoutingAgent_UnregisteredTargetFails 验证 Router 返未注册分支报错。
func TestRoutingAgent_UnregisteredTargetFails(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x")})),
	)
	router := func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) { return "ghost", nil }

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRouter(router),
	).AddBranch("math", math)

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "unregistered") {
		t.Fatalf("expect unregistered err, got %v", err)
	}
}

// TestRoutingAgent_RouterInterrupt_WorksInNode 业务 Router 可以直接 HITL——因为它跑在
// **classify 节点内**，`compose_v2.Ask` 拿得到执行位置。
//
// # 承重的是「在 node 里跑」这件事
//
// Router 原先在 **cond** 里跑，而 cond 是纯判断、**禁止 interrupt**（判断需要新输入 =
// 上游没尽责，该由 node 去问）——框架一律转非-interrupt error 拒绝。把 Router 整体搬进
// classify 节点是根治：中断走标准 node 路径，ID 按执行位置算、跨 Resume 稳定
// （节点失败不 IncrVisit）。
//
// **sanity-revert**：routing_builder.go 把 `router(ctx,input)` 调用搬回 ConditionalEdge
// → `Ask` 返「不在任何节点/工具里」错、`IsInterrupt=false` → 本测试 fail。
func TestRoutingAgent_RouterInterrupt_WorksInNode(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
	)

	// Router 用 `compose_v2.Ask` 提问——它跑在 classify **节点内**，所以拿得到执行位置。
	//
	// ⚠️ 原先这里是「不填 InterruptID 让框架 auto-fill」，那条路已被拒（框架看不见节点内部
	// 的分支结构）。本用例真正要钉的是「Router 走的是 **node** 路径而不是 cond 路径」
	// ——cond 里抛中断会被协议无条件拒绝，node 里则正常 HITL。换成 `Ask` 之后那条不变量不变。
	router := func(ctx context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Reason: "approve_route", Question: "approve route?",
		}); err != nil {
			return "", err
		}
		return "math", nil
	}

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRouter(router),
	).AddBranch("math", math)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Router 抛 InterruptError 应 propagate, got nil")
	}
	// 承重①：**Router 里的提问真的成为一次 HITL**——搬回 cond 的话这里就不是中断了
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Router 在 classify node 里跑，提问应走标准 node 路径成为 HITL；got %v", err)
	}
	// 承重②：**恰好一条挂起且带得走**（业务据此配答案 Resume）
	qs := ie.AllQueries()
	if len(qs) != 1 || qs[0].InterruptID == "" {
		t.Fatalf("应有恰好一条带 ID 的挂起，got %+v", qs)
	}
}

// TestRoutingAgent_RouterInterrupt_PreservesExplicitID 业务 Router **自填** ID 时框架
// 必须原样保留——与上面那条互补：那条走 `Ask` 让框架算，这条钉「自填这条路仍然通」
// （跨进程预先约定 ID 精确路由的业务依赖它）。
//
// 重构后 Router 在 classify node 内跑，走标准 node interrupt 路径（原 ISSUE-86 D2 的
// 「cond 路径必须自填 ID」强制已废止——自填在 node 路径是**可选**优化，不是义务）。
func TestRoutingAgent_RouterInterrupt_PreservesExplicitID(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
	)

	const customID = "route_approval_step_1"
	router := func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
		return "", &compose_v2.InterruptError{
			Reason: "approve_route",
			Query:  &compose_v2.AskQuery{Question: "approve route?", InterruptID: customID},
		}
	}

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRouter(router),
	).AddBranch("math", math)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Router 抛 InterruptError 时应 propagate, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("应返 InterruptError, got %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID != customID {
		t.Errorf("Router 自填 InterruptID 应保留, got %v", ie.Query)
	}
}

// TestRoutingAgent_NoRouterFails 验证至少要有一个决策来源：Router、决策方或 classifier。
// 只配兜底分支不算，兜底是升级链的终点而不是决策来源。
func TestRoutingAgent_NoRouterFails(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x")})),
	)
	fallbackOnly := agent_v2.NewBasicAgent(
		agent_v2.WithName("fb"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x")})),
	)
	cases := map[string]*agent_v2.RoutingAgent{
		"nothing":       agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("dispatch")),
		"fallback only": agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("dispatch"), agent_v2.WithRoutingFallback(fallbackOnly)),
	}
	for name, r := range cases {
		r.AddBranch("math", math)
		_, err := r.Run(context.Background(), nil, schema.UserMessage("hi"))
		if err == nil || !strings.Contains(err.Error(), "router, decider or classifier required") {
			t.Fatalf("%s: expect 'router, decider or classifier required' err, got %v", name, err)
		}
	}
}

// TestRoutingAgent_AddBranch_RejectsReservedFallbackName 验证 AddBranch 在构造期拒绝框架保留名
// "__fallback__"（配了 WithRoutingFallback 时，业务同名要到 Run 期建图才报重名；没配时一律拒绝，
// 免得以后加上兜底才炸）。sanity-revert：去掉 routing_agent.go 的 reserved name panic 则本测试
// 不再 panic → FAIL。
func TestRoutingAgent_AddBranch_RejectsReservedFallbackName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("AddBranch(\"__fallback__\") 应 panic（框架保留名），实际未 panic")
		}
	}()
	agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("r")).
		AddBranch("__fallback__", &interruptingAgent{name: "x"})
}

// expectPanic 断言 fn 会 panic。
func expectPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s should panic", what)
		}
	}()
	fn()
}

// TestSequentialAgent_AddStep_RejectsGraphNodeNames：步骤名是图的起止节点名时在构造期拒绝。
// 顺序编排拿步骤名当节点 ID；没有这道检查时要到建图才被拒，得到的是 Run 期一个会被外层重试
// 的错误（更早时连建图都不拒，叫 "__end__" 的步骤会被静默跳过）。
func TestSequentialAgent_AddStep_RejectsGraphNodeNames(t *testing.T) {
	for _, name := range []string{string(compose_v2.EndNode), string(compose_v2.StartNode)} {
		expectPanic(t, fmt.Sprintf("AddStep(%q)", name), func() {
			agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).
				AddStep(name, &fixedAgent{name: "x", resp: schema.AssistantMessage("from x")})
		})
	}
}

// TestRoutingAgentBuilder_AddBranch_RejectsEndAsBranchName：低层 builder 同样拒绝 "__end__"。
// 路由目标 "__end__" 在 classify 节点里被当成显式结束，同名分支永远跑不到且不报错。
func TestRoutingAgentBuilder_AddBranch_RejectsEndAsBranchName(t *testing.T) {
	branch := func(context.Context, *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("x"), nil
	}
	expectPanic(t, `RoutingAgentBuilder.AddBranch("__end__")`, func() {
		agent_v2.NewRoutingAgentBuilder().AddBranch(string(compose_v2.EndNode), branch)
	})
}

// TestRoutingAgent_BranchNamedStartRoutesNormally：分支可以叫 "__start__"。它的节点是
// worker___start__、存档命名空间是 branch___start__，都不撞图的起点；只有 "__end__" 作为路由
// 目标会被当成显式结束。以后若把两个名字的检查合并回去，这条会红。
func TestRoutingAgent_BranchNamedStartRoutesNormally(t *testing.T) {
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("r"),
		agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) {
			return compose_v2.StartNode, nil
		}),
	).AddBranch(string(compose_v2.StartNode), branchAgent(string(compose_v2.StartNode))).AddBranch("other", branchAgent("other"))
	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("a branch named %q should route normally, got %v", compose_v2.StartNode, err)
	}
	if out := finalContent(t, events); out != "from "+string(compose_v2.StartNode) {
		t.Fatalf("routed to %q", out)
	}
}

// TestAddStepAndAddBranch_RejectNSInvalidNamesAtConstruction：顺序编排的步骤名、路由编排的
// 分支名不符合 ns 命名规范时在构造期拒绝。以前要到 Run 期建图时才报，得到的是一个会被外层
// 重试的错误，每次续跑再炸一次，错误文案还指向 OnAgentEnd。
func TestAddStepAndAddBranch_RejectNSInvalidNamesAtConstruction(t *testing.T) {
	for _, name := range []string{"a/b", "a#1", "a@1", ""} {
		expectPanic(t, fmt.Sprintf("SequentialAgent.AddStep(%q)", name), func() {
			agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).
				AddStep(name, &fixedAgent{name: "x", resp: schema.AssistantMessage("from x")})
		})
		expectPanic(t, fmt.Sprintf("RoutingAgent.AddBranch(%q)", name), func() {
			agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("r")).AddBranch(name, &interruptingAgent{name: "x"})
		})
	}
}

// TestPlanAgent_RejectsReservedSubAgentNames：子 agent 不能与计划编排图里框架自己用的节点
// 同名，两个注册入口都拦。撞上内部派发 worker 名时，叫 __default_executor__ 又配了 executor
// 的子 agent 会被静默顶掉，规划器选中它时实际跑的是 executor。
func TestPlanAgent_RejectsReservedSubAgentNames(t *testing.T) {
	planner := func(context.Context, *agent_v2.RunSession, string, []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		return nil, "done", nil
	}
	sub := &fixedAgent{name: "x", resp: schema.AssistantMessage("from x")}
	for _, name := range []string{"__default_executor__", "__tmp_subagent__", "plan", string(compose_v2.EndNode)} {
		expectPanic(t, fmt.Sprintf("AddSubAgent(%q)", name), func() {
			agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("p")).AddSubAgent(name, sub)
		})
		expectPanic(t, fmt.Sprintf("WithSubAgents(%q)", name), func() {
			agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("p"),
				agent_v2.WithSubAgents(map[string]agent_v2.Agent{name: sub}))
		})
	}
}

// TestRoutingAgent_AddBranch_RejectsEndAsBranchName：分支名不能是 "__end__"。路由目标
// "__end__" 会被当成图的终点：选中它的请求不跑任何分支，Run 返回 err == nil 且没有产出。
// "__start__" 不受影响（分支节点带 worker_ 前缀），这里不钉它。
func TestRoutingAgent_AddBranch_RejectsEndAsBranchName(t *testing.T) {
	expectPanic(t, `AddBranch("__end__")`, func() {
		agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("r")).AddBranch(string(compose_v2.EndNode), &interruptingAgent{name: "x"})
	})
}
