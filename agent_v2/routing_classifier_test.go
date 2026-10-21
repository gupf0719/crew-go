package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// alias 让测试代码短一点
type compose_v2_NodeID = compose_v2.NodeID

// makeClassifierMsg 构造带 RouteTool tool_call 的 assistant message。
func makeClassifierMsg(targetBranch string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.RouteTool,
			Arguments: `{"target":"` + targetBranch + `"}`,
		},
	}})
}

// fixedAgent 测试用 stub——Run 直接返预设 message，不跑 ReAct。
type fixedAgent struct {
	name string
	resp *schema.Message
	err  error
}

func (a *fixedAgent) Run(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if a.err != nil {
		return nil, a.err
	}
	if sess != nil {
		sess.AddEvent(agent_v2.NewExitEvent(a.name, a.resp))
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, a.resp)}, nil
}
func (a *fixedAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *fixedAgent) Name() string        { return a.name }
func (a *fixedAgent) Description() string { return "" }

// TestRoutingClassifier_BasicDispatch 验证 classifier sub-agent 通过 RouteTool 路由。
func TestRoutingClassifier_BasicDispatch(t *testing.T) {
	// classifier 用 fixedAgent 直接返 RouteTool tool_call message——不走 ReAct loop
	classifier := &fixedAgent{name: "classifier", resp: makeClassifierMsg("math")}
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("answer: 4"),
		})),
	)
	coder := agent_v2.NewBasicAgent(
		agent_v2.WithName("coder"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("never"),
		})),
	)

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"math":  "数学题",
			"coder": "代码题",
		}),
	).
		AddBranch("math", math).
		AddBranch("coder", coder)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("solve 2+2"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "answer: 4") {
		t.Errorf("expect math branch hit via classifier, got %v", final)
	}
}

// capturingClassifier 记录收到的 input.Content——验证 WithRoutingDescriptions 注入
// （**读 input**，区别于 fixedAgent 忽略 input 的假绿 stub）。
type capturingClassifier struct {
	name     string
	resp     *schema.Message
	gotInput string
}

func (a *capturingClassifier) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if input != nil {
		a.gotInput = input.Content
	}
	if sess != nil {
		sess.AddEvent(agent_v2.NewExitEvent(a.name, a.resp))
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, a.resp)}, nil
}
func (a *capturingClassifier) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *capturingClassifier) Name() string        { return a.name }
func (a *capturingClassifier) Description() string { return "" }

// TestRoutingClassifier_DescriptionsInjected 验证 WithRoutingDescriptions 把分支描述
// 注入 classifier 收到的 input（Stage 4 引入但 classifier 注入逻辑曾漏实现——补全后回归）。
// classifier 用读 input 的 capturingClassifier 断言注入，非 fixedAgent 忽略 input 的假绿。
func TestRoutingClassifier_DescriptionsInjected(t *testing.T) {
	cls := &capturingClassifier{name: "classifier", resp: makeClassifierMsg("math")}
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("4")})),
	)
	coder := agent_v2.NewBasicAgent(
		agent_v2.WithName("coder"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRoutingClassifier(cls),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"math":  "数学计算题",
			"coder": "写代码题",
		}),
	).
		AddBranch("math", math).
		AddBranch("coder", coder)

	if _, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("solve 2+2")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// classifier 收到的 input 必须含注入的分支描述（WithRoutingDescriptions 生效）
	if !strings.Contains(cls.gotInput, "math: 数学计算题") {
		t.Errorf("WithRoutingDescriptions 未注入 math 描述；classifier input = %q", cls.gotInput)
	}
	if !strings.Contains(cls.gotInput, "coder: 写代码题") {
		t.Errorf("WithRoutingDescriptions 未注入 coder 描述；classifier input = %q", cls.gotInput)
	}
	// 原始用户输入必须保留
	if !strings.Contains(cls.gotInput, "solve 2+2") {
		t.Errorf("注入后丢失原始 input；classifier input = %q", cls.gotInput)
	}
}

// TestRoutingClassifier_AnnotationFallback 验证 classifier 用 annotations["route_target"] 路由。
func TestRoutingClassifier_AnnotationFallback(t *testing.T) {
	classMsg := schema.AssistantMessage("classified")
	classMsg.Annotations = map[string]any{"route_target": "coder"}

	classifier := &fixedAgent{name: "classifier", resp: classMsg}
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
	)
	coder := agent_v2.NewBasicAgent(
		agent_v2.WithName("coder"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("hello world")})),
	)

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRoutingClassifier(classifier),
	).
		AddBranch("math", math).
		AddBranch("coder", coder)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("write hello"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "hello world") {
		t.Errorf("expect coder branch hit via annotation, got %v", final)
	}
}

// TestRoutingClassifier_FallbackOnUnknownTarget 验证 classifier 返非法 target 时走 fallback。
func TestRoutingClassifier_FallbackOnUnknownTarget(t *testing.T) {
	classifier := &fixedAgent{name: "classifier", resp: makeClassifierMsg("unknown_branch")}
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
	)
	fallback := agent_v2.NewBasicAgent(
		agent_v2.WithName("fb"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("fallback handled")})),
	)

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(fallback),
	).AddBranch("math", math)

	events, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "fallback handled") {
		t.Errorf("expect fallback when classifier returns unknown target, got %v", final)
	}
}

// TestRoutingAgent_RouterExclusiveWithOtherSources 验证 Router 和其余决策来源互斥，
// 同时配构造期 panic。
//
// Router 函数总会给出决定（返空也是「显式路由到 End」），没有「拿不准」这个出口，
// 排在它后面的决策方、classifier、兜底分支永远不会被调用。以前 Router + classifier
// 是「Router 优先、classifier 静默失效」，现在直接拒绝这种配置。
func TestRoutingAgent_RouterExclusiveWithOtherSources(t *testing.T) {
	router := func(_ context.Context, _ *schema.Message) (compose_v2_NodeID, error) { return "coder", nil }
	decider := func(_ context.Context, _ agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		return agent_v2.RouteDecision{Target: "coder", Confidence: 1}, nil
	}
	other := &fixedAgent{name: "other", resp: schema.AssistantMessage("x")}
	combos := map[string]agent_v2.RoutingOption{
		"classifier": agent_v2.WithRoutingClassifier(other),
		"fallback":   agent_v2.WithRoutingFallback(other),
		"decider":    agent_v2.WithRouteDecider(decider, 0),
	}
	for name, opt := range combos {
		t.Run(name, func(t *testing.T) {
			defer func() {
				rec := recover()
				if rec == nil {
					t.Fatalf("WithRouter + %s should panic at construction", name)
				}
				if !strings.Contains(fmt.Sprint(rec), "WithRouter 不能和") {
					t.Fatalf("panic message should name the conflict, got %v", rec)
				}
			}()
			agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("dispatch"), agent_v2.WithRouter(router), opt)
		})
	}
}

// TestRoutingClassifier_R24_HIGH2_PropagatesInterruptError 是 R24 audit HIGH-2 回归——
// 验证 classifier sub-agent.Run 抛 InterruptError 时父 RoutingAgent.Run 应 propagate
// 而非 silent 走 fallback / End。
//
// **Bug 历史**：原 classifierAsRouter line 427-434 错误吞掉返 fallback / "" → 业务
// 配 classifier + WithAskUser() 让 LLM 路由不确定时主动询问用户 → silent 走
// fallback HITL 链断。修法：RoutingAgentRouter 签名加 error + classifierAsRouter
// 区分控制流 vs 业务错。
func TestRoutingClassifier_R24_HIGH2_PropagatesInterruptError(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("math result")})),
	)

	// classifier 抛 InterruptError（mock"用户审批路由"场景）
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("cls"),
		agent_v2.WithModel(&interruptingClassifierModel{}),
	)
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRoutingClassifier(classifier),
	).AddBranch("math", math)

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R24 HIGH-2 回归：classifier 抛 InterruptError 时 RoutingAgent.Run 应 propagate" +
			"而非 silent 走 fallback；实际 nil → HITL 链断")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("R24 HIGH-2 回归：err 应为 InterruptError 类型——实际 %v (%T)\n"+
			"sanity-revert 改 classifierStage 不 propagate err → silent 走 fallback", err, err)
	}
}

// TestRoutingClassifier_HITL_ResumeClosedLoop 验证 classifier 内 ask_user 中断 → Resume
// **完整闭环**：classifier 问用户走哪条分支 → 用户答 → classifier 据答案选分支 → 选中
// worker 跑出结果。
//
// **cond→node 重构的核心收益验证**：classifier 现在在 classify node 里跑，走标准 node
// interrupt 路径——framework auto-fill/透传稳定 ID（nodeID + visit 维度跨 Resume 不漂）
// + cp 存档 + Resume 重跑 classify node 让 classifier 拿答案续跑。重构前 classifier 在
// cond 里跑：ID 靠业务自填（ISSUE-86 D2 强制）、且 cond interrupt 会把 completedFrontier
// 的成功成员拉回重跑（F-A）。
//
// **补覆盖空白**：重构前全库**没有** classifier ask_user→Resume 闭环测试——
// PropagatesInterruptError 只验"中断能传播"，不验"答完能续跑"。
//
// sanity-revert：routing_builder.go 把 `router(ctx,input)` 调用搬回 ConditionalEdge →
// classifier 的 interrupt 走 cond 路径 → graph_loop 禁 cond interrupt 后 fail-fast 返
// 非-interrupt error → Run 1 的 IsInterrupt 断言 fail。
func TestRoutingClassifier_HITL_ResumeClosedLoop(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("math result")})),
	)

	// classifier 的 ask 工具：首次抛 Interrupt 问用户，Resume 拿到答案后返回
	askTool := tool.NewFuncTool(
		"ask_route",
		"ask user which branch to route to",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "route_ask"); ok {
				return tool.Success("user chose: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "route_ask", Question: "which branch?"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)

	tc := schema.ToolCall{ID: "tc_route", Function: schema.FunctionCall{Name: "ask_route", Arguments: `{"q":"which?"}`}}
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("cls"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}), // 首轮：问用户
			schema.AssistantMessage("math"),                        // 拿到答案后：选 math 分支
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("dispatch"),
		agent_v2.WithRoutingClassifier(classifier),
	).AddBranch("math", math)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1：classifier 内 ask 工具抛 Interrupt → 经 classify node 走标准 node interrupt
	_, err := r.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1：classifier 的 ask_user 应经 classify node 抛 InterruptError, got %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("Run 1：InterruptError 应含非空稳定 InterruptID, got %+v", ie.Query)
	}

	// Resume：答"math" → classifier 拿答案返 target "math" → 路由到 worker math → 完成
	events, err := r.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("math"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume 应完成闭环（classifier 拿答案 → 选分支 → worker 跑）, got %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "math result" {
		t.Fatalf("Resume 后应路由到 math worker 并产出 'math result', got %+v", final)
	}
}

type interruptingClassifierModel struct{}

func (m *interruptingClassifierModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "approve_route",
		Query:  &compose_v2.AskQuery{InterruptID: "ai_route", Question: "approve route?"},
	}
}
func (m *interruptingClassifierModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *interruptingClassifierModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*interruptingClassifierModel)(nil)

// TestRoutingClassifier_NoRouterOrClassifierFails 验证两者必填一个。
func TestRoutingClassifier_NoRouterOrClassifierFails(t *testing.T) {
	math := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
	)
	r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("dispatch")).AddBranch("math", math)
	_, err := r.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "router, decider or classifier required") {
		t.Fatalf("expect 'router, decider or classifier required' err, got %v", err)
	}
}
