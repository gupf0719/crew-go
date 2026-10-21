package agent_v2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// jsonUnmarshalRouteArgs is a tiny indirection over json.Unmarshal so the import is exercised.
func jsonUnmarshalRouteArgs(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}

// RouteTool 是 LLM Classifier 路径用的内置工具名——classifier agent 通过 tool-call
// 输出结构化路由决策（避免文本解析歧义）。
//
// classifier 装备方式（四编排 review RT-M3 fix）：用 WithRouteTool() 声明愿意定路由，被
// RoutingAgent 授予路由能力时框架给它注入内置 return-direct 工具（见 route_grant.go）——LLM 调用
// 即终止 ReAct，它做出的路由是交给 RoutingAgent 的上行信号（classify 节点按收件人取，排在一切解析
// 之前）：
//
//	classifier := agent_v2.NewBasicAgent(
//	    agent_v2.WithName("classifier"),
//	    agent_v2.WithModel(myLLM),
//	    agent_v2.WithRouteTool(), // 内置 route_to，一调即出
//	    agent_v2.WithSystemPrompt("你是路由器。根据用户问题调 route_to 选目标分支，不要自己回答。"),
//	)
//
// 注意**不要**只注入裸 schema.ToolInfo（无实现 → tools 节点报错被吞静默
// fallback）或普通 FuncTool（它不做出路由决定；工具跑完回 model，final 是后续文本，
// 兜底解析也取不到目标）——仅框架注入的 return-direct 形态可用（probe 实证）。
//
// 框架按优先级取 classifier 的路由：
//  1. 它做出的路由决定（route_to 工具、它显式转授的组成部分做出的上行信号）
//  2. 兜底解析最终消息：tool_call 名 == RouteTool → 取 args.target
//  3. msg.Annotations["route_target"] string
//  4. msg.Content 去掉首尾空白后整段
const RouteTool = "route_to"

// RoutingChoice 描述单个可路由的目标：分支名与人类可读描述。决策方收到的候选清单
// [RouteRequest.Candidates] 用它，描述来自 [WithRoutingDescriptions]。
type RoutingChoice struct {
	Name        string
	Description string
}

// fallbackBranchName 是兜底分支注册进图里的保留名，业务分支不能用。
const fallbackBranchName = "__fallback__"

// RoutingAgent 按 input 路由到 N 个候选 sub-agent 之一执行。
//
// **路由决策来源**，二选一：
//   - WithRouter 注入的 Router 函数：业务规则自己说了算，框架不做任何升级。返空 =
//     显式路由 EndNode（无产出），返未知名 = fail-fast 报错。它和下面几项互斥。
//   - 升级链：WithRouteDecider 挂的决策方按挂载顺序逐档判，置信度够就采纳，拿不准
//     交给下一档；决策方之后是 WithRoutingClassifier 的 LLM classifier，最后是
//     WithRoutingFallback 的兜底分支。三者可以组合，至少要有决策方或 classifier 之一
//     （影子档永不采纳，不算，见 [WithShadow]）。完整规则见 [RouteDecider]。
//   - 都没有 → Run 时报错。
//
// 拓扑：classify（跑路由决策，写 targetCh）→ ConditionalEdge（纯读 targetCh）
// → 选中 worker(sub-agent.Run) → End
//
// **兜底分支**：classifier 返非法 target / 解析不出 target / 业务错时，或者决策方都
// 拿不准又没有 classifier 时，路由到 fallback agent；没设兜底时返回
// [*RouteUndecidedError]，不会静默结束。
//
// **典型用法**：按 query 类型派发到不同专家 agent（数学题→math agent / 代码题→coder agent）。
//
// 共享 session：被选中的 sub-agent 写 events 到父 session。
//
// **events 共享是设计意图**（R32 audit 视角 B HIGH-1 澄清）：同一 Run 只走一个
// branch（不并存），classifier 和被选 worker 之间共享 events 让 worker 看 classifier
// 路由依据是合理的。**嵌套场景**：`AgentAsTool(RoutingAgent)` 时 RoutingAgent 收到
// 的 child sess 与父独立 events。详见 AgentAsTool godoc。
//
// 嵌套 cp：被选中的分支通过 ctx push graphNS（"branch_<name>"）让子 cp 物理写
// (parentSID, parentNS/branch_<name>)——多次调用不同分支时 cp 相互不覆。
type RoutingAgent struct {
	name         string
	description  string
	branches     []RoutingBranch
	router       RoutingAgentRouter
	deciders     []routeDeciderStage // 升级链里的决策方，按 WithRouteDecider 调用顺序
	classifier   Agent
	fallback     Agent
	descriptions map[string]string // branch name → 人类可读描述（喂 classifier prompt）
	hooks        hook.Hooks
	retryCfg     compose_v2.RetryConfig // 选中分支 sub-agent.Run 的重试策略
}

// RoutingBranch 单分支定义。
type RoutingBranch struct {
	Name  string
	Agent Agent
}

// NewRoutingAgent 创建 RoutingAgent。
func NewRoutingAgent(opts ...RoutingOption) *RoutingAgent {
	r := &RoutingAgent{}
	for _, opt := range opts {
		opt(r)
	}
	// #7 F6 fail-fast: WithRoutingName required (7 编排同款 — F6 fail-fast 范围
	// = Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除)
	if r.name == "" {
		panic("NewRoutingAgent: WithRoutingName required")
	}
	r.validateDecisionSources()
	return r
}

// RoutingOption 配置 RoutingAgent。
type RoutingOption func(*RoutingAgent)

// WithRoutingName 设 RoutingAgent 名。
func WithRoutingName(name string) RoutingOption {
	return func(r *RoutingAgent) { r.name = name }
}

// WithRoutingDescription 设职责描述。
func WithRoutingDescription(desc string) RoutingOption {
	return func(r *RoutingAgent) { r.description = desc }
}

// WithRouter 设 Router 决策函数：业务规则自己说了算，框架不做任何升级。
//
// caller 实现：根据原始 input 决定路由到哪个 branch name。
//
// 与 WithRouteDecider、WithRoutingClassifier、WithRoutingFallback 互斥，同时设时构造期
// panic：Router 总会给出决定，排在它后面的几项永远不会被调用。
func WithRouter(fn RoutingAgentRouter) RoutingOption {
	return func(r *RoutingAgent) {
		// audit³ HIGH-1 fix: typed nil func 也踩 Go interface "typed nil" 陷阱
		// `var fn RoutingAgentRouter; WithRouter(fn)` → reflect.Func.IsNil() true
		if fn == nil {
			panic("WithRouter: nil router function")
		}
		r.router = fn
	}
}

// WithRoutingClassifier 设 LLM classifier sub-agent。配了 WithRouteDecider 时，它是
// 决策方都拿不准之后的下一档；没配决策方时直接由它决定。
//
// classifier agent 推荐用 WithRouteTool() 装备内置 route_to 工具（return-direct，
// 一调即出——见 RouteTool 常量 godoc，裸 ToolInfo / 普通 FuncTool 形态不可用）：
//
//	classifier := agent_v2.NewBasicAgent(
//	    agent_v2.WithName("classifier"),
//	    agent_v2.WithModel(myLLM),
//	    agent_v2.WithRouteTool(),
//	    agent_v2.WithSystemPrompt("你是路由器。调 route_to 选目标分支，不要自己回答。"),
//	)
//
// 框架按以下顺序取路由，取第一个拿得到的：它做出的路由决定（上行信号）→ 最终消息里的
// tool_call(RouteTool) → annotations["route_target"] → 去掉首尾空白后的整段文本。纯文本
// 小模型也可以不配工具，靠最后一种（整段文本等于分支名，格式敏感）。
func WithRoutingClassifier(ag Agent) RoutingOption {
	return func(r *RoutingAgent) {
		// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
		if isAgentNil(ag) {
			panic("WithRoutingClassifier: nil agent (含 Go typed nil interface 陷阱)")
		}
		r.classifier = ag
	}
}

// WithRoutingFallback 设 fallback agent，升级链的最后一档：classifier 返非法 target /
// 解析不出 / 业务错时兜底执行，决策方都拿不准又没有 classifier 时也走这里。不能和
// WithRouter 同用（构造期 panic）。
//
// 不设时 classifier 失败返回 [*RouteUndecidedError]，与决策方都拿不准又无处升级时一样。
func WithRoutingFallback(ag Agent) RoutingOption {
	return func(r *RoutingAgent) {
		// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
		if isAgentNil(ag) {
			panic("WithRoutingFallback: nil agent (含 Go typed nil interface 陷阱)")
		}
		r.fallback = ag
	}
}

// WithRoutingDescriptions 给每个 branch name 关联人类可读描述。
//
// 用途：classifier prompt 自动注入"<branch_name>: <description>"列表，让 LLM
// 按描述选择目标——不再要求 caller 在 system prompt 手抄候选清单。决策方收到的候选
// 清单 [RouteRequest.Candidates] 也带着这些描述，比如 jev 决策方把它们当选项说明。
func WithRoutingDescriptions(descs map[string]string) RoutingOption {
	return func(r *RoutingAgent) { r.descriptions = descs }
}

// WithRoutingHooks 设业务 hook。
func WithRoutingHooks(h hook.Hooks) RoutingOption {
	return func(r *RoutingAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		r.hooks = h
	}
}

// WithRoutingRetryConfig 设选中分支 sub-agent.Run 的重试策略。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithRoutingRetryConfig(cfg compose_v2.RetryConfig) RoutingOption {
	compose_v2.ValidateRetryConfig(cfg, "WithRoutingRetryConfig")
	return func(r *RoutingAgent) { r.retryCfg = cfg }
}

// WithRoutingMaxRetries 简化版：等价 WithRoutingRetryConfig(SimpleRetry(n))。
func WithRoutingMaxRetries(n int) RoutingOption {
	return func(r *RoutingAgent) { r.retryCfg = compose_v2.SimpleRetry(n) }
}

// AddBranch 追加候选分支。链式。
//
// 构造期 panic 的情形：agent 为 nil（含 typed nil）、name 不符合 ns 命名规范（含 "/" "#" "@"
// 或为空）、name 是 "__fallback__"（兜底分支的内部名）或 "__end__"（路由目标 "__end__" 会被
// 当成图的终点，同名分支永远跑不到）、name 重复。
func (r *RoutingAgent) AddBranch(name string, ag Agent) *RoutingAgent {
	// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
	if isAgentNil(ag) {
		panic(fmt.Sprintf("RoutingAgent.AddBranch: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
	}
	// 名字会拼进分支的节点 ID 与存档命名空间，不符合 ns 命名规范的在构造期就报，不等到 Run 期
	// 建图时才发现、还被外层当成可重试的错误。
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("RoutingAgent.AddBranch: name %q invalid for ns: %v", name, err))
	}
	// 拒绝框架保留名 "__fallback__"：配了 WithRoutingFallback 时，它在建图时注册成这个名字的
	// 内部分支，业务同名要到 Run 期建图才报重名。没配兜底时同名分支其实能用，这里一律拒绝，
	// 免得以后加上兜底才炸。
	if name == fallbackBranchName {
		panic(fmt.Sprintf("RoutingAgent.AddBranch: name %q 是框架保留名（fallback 内部用），请换名", name))
	}
	// 路由目标 "__end__" 会被当成图的终点：同名分支永远跑不到，选中它的请求 Run 返回
	// err == nil 且没有产出。只拒这一个名字——分支节点带 worker_ 前缀，"__start__" 不会撞名。
	// 低层 RoutingAgentBuilder.AddBranch 也拦，这里早一步在构造期报出来。
	if name == string(compose_v2.EndNode) {
		panic(fmt.Sprintf("RoutingAgent.AddBranch: name %q 会被当成图的终点，请换名", name))
	}
	// 重名校验：低层 builder 在 Run 期建图时也会报重名，这里提前到构造期。与
	// ParallelAgent.AddBranch / PlanAgent.AddSubAgent 协议对称。
	for _, br := range r.branches {
		if br.Name == name {
			panic(fmt.Sprintf("RoutingAgent.AddBranch: duplicate name %q", name))
		}
	}
	r.branches = append(r.branches, RoutingBranch{Name: name, Agent: ag})
	return r
}

// Name 返回名。NewRoutingAgent 构造期已 fail-fast 防空 name (F6).
func (r *RoutingAgent) Name() string { return r.name }

// Description 返回职责描述。
func (r *RoutingAgent) Description() string { return r.description }

// Run 启动路由执行。
func (r *RoutingAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "RoutingAgent.Run", &retErr)

	if input == nil {
		input = schema.UserMessage("")
	}
	// 影子档永不采纳，不算决策来源（见 WithShadow）。
	if r.router == nil && r.classifier == nil && !r.hasDecidingStage() {
		if len(r.deciders) > 0 {
			return nil, fmt.Errorf("routing agent %q: router, decider or classifier required — every WithRouteDecider here is a shadow decider, which never decides (add a non-shadow decider or WithRoutingClassifier)", r.Name())
		}
		return nil, fmt.Errorf("routing agent %q: router, decider or classifier required (use WithRouter, WithRouteDecider or WithRoutingClassifier)", r.Name())
	}
	if len(r.branches) == 0 {
		return nil, fmt.Errorf("routing agent %q: no branches; call AddBranch", r.Name())
	}
	if err := resolveRunSession(ctx, &session, r.Name()); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)
	// 历史写入器由**编排这一层**拥有：子里可能有不是 BasicAgent 的执行体（远程 / 自研 /
	// 别的框架的壳），它们不会建也不会写。见 startHistoryForOrchestration。
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, r.historySubs(), r.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()

	cfg := applyRunOptions(opts)
	// MED-1 fail-fast：composite 不把 per-call 覆盖透传给 inner step，传了静默 no-op，直接报错。
	if err := rejectCompositeOverride(cfg.overrides()); err != nil {
		return nil, err
	}
	startOffset := len(session.Events())
	hooks := r.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	// Sub-5 audit Round 2/3 fix: RoutingAgent 是单次路由决策 + 转发, 无 "round" 概念.
	// info.Round 保持 0 是 by-design — 业务关心路由选中哪个 agent 走 trace span 看,
	// metric 维度走 MetricNodeExecutionTotal{node=routing}.
	info := &hook.AgentInfo{Name: r.Name(), Type: "routing", Round: 0}
	// audit³ D1 fix: OnAgentStart Run top-level fire (跟 PlanAgent 对齐)
	agentIn := &hook.AgentInput{Messages: []*schema.Message{input}}
	// 「这是不是新任务」判在钩子之前：钩子里就可能发起调用（见 enterRunTask）
	ctx = enterRunTask(ctx, session)
	var startErr error
	ctx, startErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return hooks.OnAgentStart(c, info, agentIn)
	})
	if startErr != nil {
		if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: startErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, startErr)
	}
	if len(agentIn.Messages) > 0 && agentIn.Messages[len(agentIn.Messages)-1] != nil {
		input = agentIn.Messages[len(agentIn.Messages)-1]
	}

	var finalRef *schema.Message
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	defer func() {
		var msgs []*schema.Message
		if finalRef != nil {
			msgs = []*schema.Message{finalRef}
		}
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "RoutingAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		_ = hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: msgs})
	}()

	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(r.Name(), input))
	}

	final, err := r.runGraph(ctx, &cfg, session, input, hooks)
	if err != nil {
		if final != nil {
			finalRef = final
		}
		return collectEvents(session, startOffset), err
	}
	finalRef = final
	if final != nil {
		session.AddEvent(NewExitEvent(r.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。
func (r *RoutingAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "RoutingAgent.Resume", &retErr)

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := r.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	// Sub-5 audit Round 2/3 fix: RoutingAgent 是单次路由决策 + 转发, 无 "round" 概念.
	// info.Round 保持 0 是 by-design — 业务关心路由选中哪个 agent 走 trace span 看,
	// metric 维度走 MetricNodeExecutionTotal{node=routing}.
	info := &hook.AgentInfo{Name: r.Name(), Type: "routing", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "routing agent", r.Name(), sessionID, opts, info, hooks, r.historySubs())
	if err != nil {
		return nil, err
	}
	if prep.AbortDone {
		return nil, nil
	}
	cper, rcfg, session := prep.Cper, prep.Rcfg, prep.Session
	// MED-1 fail-fast：composite Resume 同样不透传 per-call 覆盖给 inner step。
	if err := rejectCompositeOverride(rcfg.overrides()); err != nil {
		return nil, err
	}
	ctx = newCtx
	// 写入器 Run / Resume 两个入口都要建——图重跑到某一步时边界补记要从 ctx 里取它。
	// 只在 Run 里建的话，中断之后 Resume 那一段的历史整段丢（取到 nil 直接 return）。
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, r.historySubs(), r.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	if rcfg.action == ResumeActionReplan {
		return nil, fmt.Errorf("routing agent %q: ResumeActionReplan not supported (no plan node; only PlanAgent / BasicAgent / GraphAgent accept Replan)", r.Name())
	}

	startOffset := len(session.Events())
	// audit³ D1 fix: OnAgentStart Resume top-level fire (跟 PlanAgent 对齐)
	agentInResume := &hook.AgentInput{}
	ctx = enterResumeTask(ctx, session)
	var startErr error
	ctx, startErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return hooks.OnAgentStart(c, info, agentInResume)
	})
	if startErr != nil {
		if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: startErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, startErr)
	}

	var finalRef *schema.Message
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	defer func() {
		var msgs []*schema.Message
		if finalRef != nil {
			msgs = []*schema.Message{finalRef}
		}
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "RoutingAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		_ = hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: msgs})
	}()

	cm := compose_v2.NewChannelManager()
	// Resume 拿不到授予（授予走 RunOption）——顶层直接 Resume 本编排时它的成员没有
	// 转交 / 路由能力。嵌在 HandoffAgent / RoutingAgent 底下的续跑不走这条：外层节点重跑时
	// 仍以 Run 形态带着授予调下来。
	g, err := r.buildComposeGraph(ctx, session, hooks, memberGrants{})
	if err != nil {
		return nil, fmt.Errorf("routing agent resume: build graph: %w", err)
	}
	runOpts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sessionID),
	}
	// **R29 audit 视角4/5 HIGH-1 fix**：与 runGraph 路径对称——嵌套场景下
	// caller 持引用直接调子编排.Resume 时 ctx 中含父推 graphNS——必须 forward
	// 让 LoadNS(sid, ns) 拿正确 ns 下子 cp，避免拿父 cp 报 channel not registered。
	// 对照 BasicAgent.Resume / PlanAgent.Resume 已修同协议。
	if ns := compose_v2.GetGraphNS(ctx); ns != "" {
		runOpts = append(runOpts, compose_v2.WithRunNS(ns))
	}
	if len(rcfg.userInputs) > 0 {
		runOpts = append(runOpts, compose_v2.WithResumeAnswers(rcfg.userInputs))
	}
	// 非寻址型负载单独一条通道——**不能**跟上面合并：两者可以只给一个（Replan 常常
	// 只给文本、不给任何 InterruptID 答案）。
	if rcfg.userText != "" {
		runOpts = append(runOpts, compose_v2.WithResumeUserText(rcfg.userText))
	}
	if hooks := extractOrchHooksOrCtx(ctx, rcfg.orchHooks); hooks != nil {
		runOpts = append(runOpts, compose_v2.WithHooks(adaptHooks(hooks)))
	}
	runErr := compose_v2.Run(ctx, g, cm, runOpts...)
	final := RoutingAgentFinalMessage(cm.Channels())
	finalRef = final
	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}
	if final != nil {
		session.AddEvent(NewExitEvent(r.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

func (r *RoutingAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
	hooks hook.Hooks,
) (*schema.Message, error) {
	cm := compose_v2.NewChannelManager()
	g, err := r.buildComposeGraph(ctx, session, hooks, runCfg.grantsFor(r))
	if err != nil {
		return nil, fmt.Errorf("routing agent %q: build graph: %w", r.Name(), err)
	}
	cper := compose_v2.GetCheckpointer(ctx)
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(
			compose_v2.Write(RoutingAgentInputChannel(), input),
		),
	}
	if cper != nil {
		sid := session.RunID
		runOpts = append(runOpts,
			compose_v2.WithCheckpointer(cper),
			compose_v2.WithSessionID(sid),
			runCfg.inputDigestOption(),
		)
		if ns := compose_v2.GetGraphNS(ctx); ns != "" {
			runOpts = append(runOpts, compose_v2.WithRunNS(ns))
		}
	}
	if hooks := extractOrchHooksOrCtx(ctx, runCfg.orchHooks); hooks != nil {
		runOpts = append(runOpts, compose_v2.WithHooks(adaptHooks(hooks)))
	}
	runErr := compose_v2.Run(ctx, g, cm, runOpts...)
	final := RoutingAgentFinalMessage(cm.Channels())
	return final, runErr
}

// grants 是**已自查过**发给本编排的转交 / 路由授予（空 = 没有）。每个成员显式转授——
// 「这一步是我的组成部分」这个意图只能由编排自己表达，不能靠环境让谁都捡得到。
func (r *RoutingAgent) buildComposeGraph(
	parentCtx context.Context,
	parentSession *RunSession,
	hooks hook.Hooks,
	grants memberGrants,
) (*compose_v2.Graph, error) {
	bld := NewRoutingAgentBuilder().WithName(r.Name())
	if hooks != nil {
		bld = bld.WithAgentHooks(hooks)
	}

	// 装配路由决策：业务 Router 自己说了算；否则把决策方、classifier、兜底串成升级链。
	switch {
	case r.router != nil:
		bld = bld.WithRouter(r.router)
	case r.hasDecidingStage() || r.classifier != nil:
		if err := r.validateBranchThresholds(); err != nil {
			return nil, err
		}
		bld = bld.WithRouter(r.decisionRouter(parentSession))
	default:
		// Run 入口已校验；Resume 走到这里说明 agent 配置不全
		return nil, fmt.Errorf("routing agent %q: no router, decider or classifier", r.Name())
	}

	retryCfg := r.retryCfg
	for _, br := range r.branches {
		br := br
		branchFn := func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
			return runMember(ctx, grants, memberSpec{
				agent: br.Agent,
				sess:  parentSession,
				input: input,
				// ns 带 branch_ 前缀（与同层其它节点的 ns 不撞），归属用裸注册名。
				nsSuffix:    "branch_" + br.Name,
				historySlot: br.Name,
				retryCfg:    retryCfg,
			})
		}
		bld = bld.AddBranch(br.Name, branchFn)
	}

	// fallback 注册成保留名 fallbackBranchName 的特殊分支
	if r.fallback != nil {
		fb := r.fallback
		bld = bld.AddBranch(fallbackBranchName, func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
			return runMember(ctx, grants, memberSpec{
				agent:       fb,
				sess:        parentSession,
				input:       input,
				nsSuffix:    "branch_" + fallbackBranchName,
				historySlot: fallbackBranchName,
				retryCfg:    retryCfg,
			})
		})
	}

	return bld.Compile()
}

// classifierNS 是 classifier 那一档的存档命名空间后缀：失败重试代数为 0 时是 "classifier"，
// 之后带上 "_retry<代数>"。
func classifierNS(retry int) string {
	if retry == 0 {
		return "classifier"
	}
	return fmt.Sprintf("classifier_retry%d", retry)
}

// maxClassifierFailure 是事件里 classifier 失败原因的长度上限（按字符）。classifier 用
// 一段话作答时，那段话会整个落进「未知目标」的原因里。
const maxClassifierFailure = 300

// boundFailure 把失败原因截到 maxClassifierFailure 个字符以内，截断时末尾是省略号。
func boundFailure(s string) string {
	r := []rune(s)
	if len(r) <= maxClassifierFailure {
		return s
	}
	return string(r[:maxClassifierFailure-1]) + "…"
}

// classifierOutcome 是 classifier 那一档的结果：选中的分支名、最终由哪一档决定
// （classifier 自己，或它失败后转去的兜底分支），以及它没给出可用结果时的原因。
// target 为空表示 classifier 失败且没有兜底分支，调用方据此返回 RouteUndecidedError。
type classifierOutcome struct {
	target  string
	stage   string
	failure string
	// err 是 classifier 调用返回的原始错误，只在它报错时有值。
	err error
}

// classifierStageFunc 是升级链里 classifier 这一档。retry 是失败重试代数，见
// routeStageMemo.Retry：它进 classifier 的存档命名空间，重试时这一档从头跑、它问的问题
// 也换新的提问 ID。
type classifierStageFunc func(ctx context.Context, input *schema.Message, retry int) (classifierOutcome, error)

// classifierStage 把 classifier sub-agent 包成升级链里的一档，由 decisionRouter 调用。
//
// **在 classify node 内被调用**（不在 ConditionalEdge）——classifier 跑
// LLM + 可能 ask_user HITL 属「干活」，归 node：nodeID + visit 协议稳定，走标准 node
// interrupt 路径（稳定 ID + cp 存档 + Resume 重跑本节点让 classifier 拿答案续跑）。
// cond 只纯读 classify 写的 targetCh，永不中断（详见 routing_builder.go 拓扑 godoc）。
//
// 流程：
//  1. 用 compose_v2.CaptureSignals 包住 classifier.Run(input)，收它这次调用做出的上行信号
//  2. 取目标：发给本 RoutingAgent 的路由信号（route_to 做出的，第一来源）→ 最终消息里的
//     tool_call(RouteTool).args.target → annotations[route_target] → 去掉首尾空白后整段等于某个分支名
//  3. target 在 branches 集合内 → 返它；否则有兜底 → 返 fallbackBranchName；都没有 → 返空目标，
//     decisionRouter 据此返回 RouteUndecidedError
//
// 路由决策事件不在这里发，由 decisionRouter 统一发，每次路由恰好一条。
//
// **错误处理**：compose_v2.MustPropagate 判为必传的错（中断、调用方喊停、软取消、陈旧写回、终局失败、
// 坏档）原样返回，由 classify 节点往上走——终局失败包括分类器里并发几路都做了路由决定的
// SignalConflictError；理由见函数体里那处判断。R24 audit HIGH-2 的教训是 classifier.Run 的
// InterruptError 必须 propagate——业务装备 WithAskUser() 让 LLM 路由不确定时主动询问用户，吞掉走
// fallback 会让 HITL 链断。其他业务错（如 LLM API 失败、LLM 客户端自己的请求超时）有兜底就降级到兜底；
// 没有兜底时这里也不返 err，把原始错误放进结果，由 decisionRouter 返回 RouteUndecidedError。
func (r *RoutingAgent) classifierStage(parentSession *RunSession, branchSet map[string]bool) classifierStageFunc {
	classifier := r.classifier
	hasFallback := r.fallback != nil
	agentName := r.Name()

	// WithRoutingDescriptions 注入: 预拼分支描述清单前缀（闭包外算一次复用）——让 classifier
	// 的 LLM 按描述选目标，不再要求 caller 在 classifier system prompt 手抄候选清单。按
	// branches 注册顺序遍历保证稳定（非 map 乱序）；只列设了非空 description 的分支。
	var branchDescPrefix string
	if len(r.descriptions) > 0 {
		var sb strings.Builder
		sb.WriteString("可路由的目标分支（branch_name: 描述）：\n")
		for _, br := range r.branches {
			if desc, ok := r.descriptions[br.Name]; ok && desc != "" {
				sb.WriteString("- ")
				sb.WriteString(br.Name)
				sb.WriteString(": ")
				sb.WriteString(desc)
				sb.WriteString("\n")
			}
		}
		sb.WriteString("\n用户输入：\n")
		branchDescPrefix = sb.String()
	}

	return func(ctx context.Context, input *schema.Message, retry int) (classifierOutcome, error) {
		// 本 RoutingAgent 的收件人：分类器那次调用里做出的路由发给它（见 side_signal.go）。在
		// classify 节点的这次执行里铸——续跑时算出同一个值，节点外的代码算不出来。推在 ctx 上而不是
		// classifier 的子 ctx 上：runDecisionAgent 派生的子 ctx 继承本 ctx 的 value，工具那端读到的
		// 是同一个值。
		dest := routeDest(ctx, agentName)
		if dest == "" {
			return classifierOutcome{}, fmt.Errorf("routing_agent %q: 铸不出收件人（classify 不在节点执行里）", agentName)
		}

		// WithRoutingDescriptions: prepend 分支描述前缀到 input.Content（shallow clone
		// 保留原 input 的 role / Annotations）；未设 descriptions 时 branchDescPrefix 为空原样透传。
		classifierInput := input
		if branchDescPrefix != "" && input != nil {
			cloned := *input
			cloned.Content = branchDescPrefix + input.Content
			classifierInput = &cloned
		}

		// 分类器是**内部决策 LLM**：产出的是「走哪个分支」这个控制信号不是对话，
		// 所以走 runDecisionAgent——不进会话历史、永不拿到转交能力。
		//
		// **路由能力在这一刻授予**：受让方就是 classifier 这个对象本身，经 RunOption 进它的签名、
		// 不进 ctx（见 routeGrant）。只有它（和它显式转授的组成部分）有 route_to；它在同一段执行里
		// 请教的别的 agent 没有——曾经资格是环境，被请教的 agent 定的路由成了分类器的路由（实测）。
		//
		// 分类器这次调用做出的路由只在它成功时交上来；失败 / 中断时随它作废。停下时分类器图里
		// 已提交的那些留在它的存档里，续跑跑完时一起交上来——定路由那一步续跑时不再执行也不丢
		// （分类器是两步图、第一步经 RunSubAgent 定路由第二步问人，或同批兄弟工具问人）。
		//
		// 命名空间带失败重试代数：重试时 classifier 从头跑，它问过的问题换新的提问 ID。
		// 不带的话，模型复用 tool_call ID 时，上一次的答案会被套到这次内容不同的问题上
		// （实测：确认「退款」的「是」用在了「注销账户吗」上，路由到注销账户且 err == nil）。
		var events []*AgentEvent
		sigs, err := compose_v2.CaptureSignals(withRouteDest(ctx, dest), func(c context.Context) error {
			var runErr error
			events, runErr = runDecisionAgent(c, classifierNS(retry), classifier, parentSession, classifierInput,
				&routeGrant{grantee: classifier})
			return runErr
		})
		if err != nil {
			// 降级只给「分类没成」（LLM API 失败、分类器自己的超时之类的任务失败）。compose_v2.MustPropagate
			// 的那些不是分类没成，原样往上走：中断（R24 HIGH-2：WithAskUser 让分类器问人，吞掉 HITL 链就
			// 断了）、调用方喊停（问本 ctx 死没死，不看错误链）、软取消、陈旧写回、终局失败、自相矛盾的
			// 存档。这里曾经手写判据、只认中断与 ctx 错：分类器被用户喊停时降级成兜底并随 classify 节点
			// 提交，续跑直接走兜底分支（实测：不停走 math，停下后续跑走兜底）；分类器里的协议违规
			// （ErrRunAborted）被变成一次「没识别出目标」，`err == nil`（实测）。
			//
			// 分类器里并发几路都做了路由决定（同一超步的两个节点——值相同也算；业务经
			// compose_v2.JoinSignals 合并的几路——给出不同目标）报的 SignalConflictError 带 ErrRunAborted，
			// 归在终局失败里：降级会把一次「几路自相矛盾」悄悄变成一次「没识别出目标」，业务看不到发生过
			// 分歧。同一批 route_to 走不到这里：排在前面的生效，后面那次回一条失败结果（见
			// tool_batch_signals.go）。
			if compose_v2.MustPropagate(ctx, err) {
				return classifierOutcome{}, err
			}
			// 其他业务错（LLM API 失败等）：有兜底就降级过去，没有就由调用方报错。
			// 降级时错误被吞掉，只剩事件里的 ClassifierFailure，所以另记一条日志，
			// 否则「classifier 挂了、流量全进兜底」看不出来。LLM 错误本身子 agent 那层
			// 已经 fire 过 OnError，不重复报。
			failure := boundFailure("error: " + err.Error())
			if hasFallback {
				slog.WarnContext(ctx, "routing classifier failed, degrading to fallback",
					"agent", agentName, "err", err)
				return classifierOutcome{target: fallbackBranchName, stage: RouteStageFallback, failure: failure}, nil
			}
			return classifierOutcome{stage: RouteStageClassifier, failure: failure, err: err}, nil
		}
		// **优先取上行信号**：route_to 把目标交给了它所在的那段执行，一路跟着提交交上来。
		// 从 final 消息解析只作兜底——那条消息会被 hook / 4 层压缩 / 上层重新合成改写，
		// 结构一变解析就静默失效（落 fallback 且 err==nil，同款形状实测过）。剩下的（发给更上方
		// 的）交给本节点这次执行，跟着它的提交继续往上走。
		// 一个收件人上至多一条：分类器先后两步各定一次时后者为准（Sequential / Loop 逐步细化）；同一超步
		// 两路都定了路由的在合并点按设计错误终局（compose_v2.SignalConflictError 并带 ErrRunAborted，原样
		// 往上走，不会落到 fallback）；同一批 route_to 排在前面的生效。
		out := FinalMessage(events)
		target, rest := takeRouteTarget(sigs, dest)
		compose_v2.EmitSignals(ctx, rest)
		if target == "" {
			// 兜底：业务自写的 classifier（不用内置 route_to，走 annotation / 文本关键词）
			target = extractRouteTarget(out)
		}
		if target != "" && branchSet[target] {
			return classifierOutcome{target: target, stage: RouteStageClassifier}, nil
		}
		failure := "no route target"
		if target != "" {
			failure = boundFailure(fmt.Sprintf("unknown target %q", target))
		}
		if hasFallback {
			return classifierOutcome{target: fallbackBranchName, stage: RouteStageFallback, failure: failure}, nil
		}
		return classifierOutcome{stage: RouteStageClassifier, failure: failure}, nil
	}
}

// extractRouteTarget 从 classifier 输出按优先级提路由 target。
//
// 优先级：tool_call(RouteTool).args.target > Annotations["route_target"] > trim(Content)
func extractRouteTarget(msg *schema.Message) string {
	if msg == nil {
		return ""
	}
	for _, tc := range schema.GetToolCalls(msg) {
		if tc.Function.Name != RouteTool {
			continue
		}
		var args struct {
			Target string `json:"target"`
		}
		if err := jsonUnmarshalRouteArgs(tc.Function.Arguments, &args); err == nil && args.Target != "" {
			return args.Target
		}
	}
	if msg.Annotations != nil {
		if v, ok := msg.Annotations["route_target"]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return strings.TrimSpace(msg.Content)
}

var _ Agent = (*RoutingAgent)(nil)
