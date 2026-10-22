package agent_v2

import (
	"context"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"fmt"
)

// LoopAgent 循环跑单 sub-agent——每轮 input 是上一轮 output（首轮 input 为原始）。
//
// 拓扑：StartNode → body(sub-agent.Run) → ConditionalEdge → [End | body]
//
// 退出条件三层（优先级递减）：
//  1. body output 打 BreakLoopAnnotation=true（业务主动 break，推荐用 SetBreakLoop helper）
//  2. ExitCondition 回调返 true（caller 自定义）
//  3. round >= MaxIterations
//
// 共享 session：sub-agent 写 events 到父 session（caller 看每轮历史）。
//
// **events 跨轮累积是设计意图**（R32 audit 视角 B HIGH-1 澄清）：body sub-agent
// 第 N+1 轮调用通过 sess.SelfHistoryCompacted 看到第 1..N 轮 events——LoopAgent
// 累积语义的核心（业务用 LoopAgent 期望"每轮基于上轮结果迭代"）。**嵌套场景**：
// `AgentAsTool(LoopAgent)` 时 LoopAgent 收到的 child sess 与父独立 events，但
// LoopAgent 内部跨轮仍共享同一 child sess.events。详见 AgentAsTool godoc。
//
// 嵌套 cp：每轮 sub-agent 拿同一个 session（RunID = 父 sid）+ push graphNS 让子 cp 物理写
// (parentSID, parentNS/round_<N>) ——R5 嵌套 cp 协议。
type LoopAgent struct {
	name          string
	description   string
	body          Agent
	exitCondition LoopAgentExitCondition
	maxIterations int
	hooks         hook.Hooks
	retryCfg      compose_v2.RetryConfig // 每轮 body sub-agent.Run 的重试策略
}

// SetBreakLoop 在 message 上打 break_loop annotation——body sub-agent 想主动结束循环时调，
// 比业务自己写 msg.Annotations["break_loop"]=true 更安全（key 收口常量 + nil map 处理）。
func SetBreakLoop(msg *schema.Message) {
	if msg == nil {
		return
	}
	if msg.Annotations == nil {
		msg.Annotations = map[string]any{}
	}
	msg.Annotations[BreakLoopAnnotation] = true
}

// NewLoopAgent 创建 LoopAgent。
//
// 推荐 builder 风格：
//
//	loop := agent_v2.NewLoopAgent(
//	    agent_v2.WithLoopName("retry_loop"),
//	    agent_v2.WithLoopMaxIterations(5),
//	).WithBody(retryAgent)
func NewLoopAgent(opts ...LoopOption) *LoopAgent {
	l := &LoopAgent{}
	for _, opt := range opts {
		opt(l)
	}
	// #7 F6 fail-fast: WithLoopName required (7 编排同款 — F6 fail-fast 范围
	// = Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除)
	if l.name == "" {
		panic("NewLoopAgent: WithLoopName required")
	}
	return l
}

// LoopOption 配置 LoopAgent。
type LoopOption func(*LoopAgent)

// WithLoopName 设 LoopAgent 名。
func WithLoopName(name string) LoopOption {
	return func(l *LoopAgent) { l.name = name }
}

// WithLoopDescription 设职责描述。
func WithLoopDescription(desc string) LoopOption {
	return func(l *LoopAgent) { l.description = desc }
}

// WithLoopMaxIterations 设最大循环轮数（默认 10）。
func WithLoopMaxIterations(n int) LoopOption {
	return func(l *LoopAgent) {
		if n > 0 {
			l.maxIterations = n
		}
	}
}

// WithLoopExitCondition 设自定义退出回调。caller 按 last output + round 决定退出。
func WithLoopExitCondition(cond LoopAgentExitCondition) LoopOption {
	return func(l *LoopAgent) {
		// audit¹⁰ HIGH-2 fix: isFrameworkNil 检 typed nil func (协议十复证).
		if isFrameworkNil(cond) {
			panic("WithLoopExitCondition: nil cond (含 Go typed nil func 陷阱)")
		}
		l.exitCondition = cond
	}
}

// WithLoopHooks 设业务 hook。
func WithLoopHooks(h hook.Hooks) LoopOption {
	return func(l *LoopAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		l.hooks = h
	}
}

// WithLoopRetryConfig 设每轮 body sub-agent.Run 的重试策略。
//
// 各轮独立计 attempt——不影响 maxIterations 计数。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithLoopRetryConfig(cfg compose_v2.RetryConfig) LoopOption {
	compose_v2.ValidateRetryConfig(cfg, "WithLoopRetryConfig")
	return func(l *LoopAgent) { l.retryCfg = cfg }
}

// WithLoopMaxRetries 简化版：等价 WithLoopRetryConfig(SimpleRetry(n))。
func WithLoopMaxRetries(n int) LoopOption {
	return func(l *LoopAgent) { l.retryCfg = compose_v2.SimpleRetry(n) }
}

// WithBody 设 sub-agent 作循环 body（必填，链式）。
func (l *LoopAgent) WithBody(ag Agent) *LoopAgent {
	// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface 陷阱
	if isAgentNil(ag) {
		panic("LoopAgent.WithBody: nil agent (含 Go typed nil interface 陷阱)")
	}
	l.body = ag
	return l
}

// Name 返回名。NewLoopAgent 构造期已 fail-fast 防空 name (F6).
func (l *LoopAgent) Name() string { return l.name }

// Description 返回职责描述。
func (l *LoopAgent) Description() string { return l.description }

// Run 启动循环执行。
func (l *LoopAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "LoopAgent.Run", &retErr)

	if input == nil {
		input = schema.UserMessage("")
	}
	if l.body == nil {
		return nil, fmt.Errorf("loop agent %q: body required (call WithBody)", l.Name())
	}
	if err := resolveRunSession(ctx, &session, l.Name()); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)
	// 历史写入器由**编排这一层**拥有：子里可能有不是 BasicAgent 的执行体（远程 / 自研 /
	// 别的框架的壳），它们不会建也不会写。见 startHistoryForOrchestration。
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, l.historySubs(), l.Name())
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
	hooks := l.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: l.Name(), Type: "loop", Round: 0}
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
		recoverPanicToErr(ctx, "LoopAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(l.Name(), input))
	}

	final, lastRound, err := l.runGraph(ctx, &cfg, session, input, hooks)
	info.Round = lastRound // Sub-5 audit Round 2 视角 G fix
	if err != nil {
		if final != nil {
			finalRef = final
		}
		return collectEvents(session, startOffset), err
	}
	finalRef = final
	if final != nil {
		session.AddEvent(NewExitEvent(l.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。
func (l *LoopAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "LoopAgent.Resume", &retErr)

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := l.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: l.Name(), Type: "loop", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "loop agent", l.Name(), sessionID, opts, info, hooks, l.historySubs())
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
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, l.historySubs(), l.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	if rcfg.action == ResumeActionReplan {
		return nil, fmt.Errorf("loop agent %q: ResumeActionReplan not supported (no plan node; only PlanAgent / BasicAgent / GraphAgent accept Replan)", l.Name())
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
		recoverPanicToErr(ctx, "LoopAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	cm := compose_v2.NewChannelManager()
	// Resume 拿不到授予（授予走 RunOption）——顶层直接 Resume 本编排时它的成员没有
	// 转交 / 路由能力。嵌在 HandoffAgent / RoutingAgent 底下的续跑不走这条：外层节点重跑时
	// 仍以 Run 形态带着授予调下来。
	g, err := l.buildComposeGraph(ctx, session, hooks, memberGrants{})
	if err != nil {
		return nil, fmt.Errorf("loop agent resume: build graph: %w", err)
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
	final := LoopAgentFinalMessage(cm.Channels())
	finalRef = final
	info.Round = GetLoopAgentRound(cm) // Sub-5 audit Round 2 视角 G fix
	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}
	if final != nil {
		session.AddEvent(NewExitEvent(l.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// runGraph 返 (final, lastRound, err) — lastRound 是已完成 loop iteration 数
// (Sub-5 audit Round 2 视角 G fix).
func (l *LoopAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
	hooks hook.Hooks,
) (*schema.Message, int32, error) {
	cm := compose_v2.NewChannelManager()
	g, err := l.buildComposeGraph(ctx, session, hooks, runCfg.grantsFor(l))
	if err != nil {
		return nil, 0, fmt.Errorf("loop agent %q: build graph: %w", l.Name(), err)
	}
	cper := compose_v2.GetCheckpointer(ctx)
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(
			compose_v2.Write(LoopAgentInputChannel(), input),
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
	final := LoopAgentFinalMessage(cm.Channels())
	lastRound := GetLoopAgentRound(cm)
	return final, lastRound, runErr
}

// buildComposeGraph 把 l.body 包装成 LoopBodyFn 调 sub-agent.Run。
//
// 每轮的 ns 边界：parentNS/round_<N>——让多轮 cp 物理隔离不互覆。
// grants 是**已自查过**发给本编排的转交 / 路由授予（空 = 没有）。每个成员显式转授——
// 「这一步是我的组成部分」这个意图只能由编排自己表达，不能靠环境让谁都捡得到。
func (l *LoopAgent) buildComposeGraph(
	parentCtx context.Context,
	parentSession *RunSession,
	hooks hook.Hooks,
	grants memberGrants,
) (*compose_v2.Graph, error) {
	bld := NewLoopAgentBuilder().WithName(l.Name())
	if hooks != nil {
		bld = bld.WithAgentHooks(hooks)
	}
	if l.maxIterations > 0 {
		bld = bld.WithMaxIterations(l.maxIterations)
	}
	if l.exitCondition != nil {
		bld = bld.WithExitCondition(l.exitCondition)
	}

	bodyAgent := l.body
	retryCfg := l.retryCfg
	bodyFn := func(ctx context.Context, input *schema.Message, round int) (*schema.Message, error) {
		return runMember(ctx, grants, memberSpec{
			agent: bodyAgent,
			sess:  parentSession,
			input: input,
			// ns 每轮一个 round_<N>（cp 要隔离）；归属固定 "body" **剔掉轮次**——body
			// 跑十轮都是同一个位置说的话，带上轮次就跨轮读不回自己的历史。这一对
			// 「ns 逐次唯一 vs 归属跨次稳定」的反向要求正是 memberSpec 分两个字段的原因。
			nsSuffix:    fmt.Sprintf("round_%d", round),
			historySlot: "body",
			retryCfg:    retryCfg,
		})
	}
	bld = bld.WithBody(bodyFn)
	return bld.Compile()
}

var _ Agent = (*LoopAgent)(nil)
