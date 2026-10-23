package agent_v2

import (
	"context"
	"fmt"
	"strings"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// ReflectAgent 实现 actor / reflector 循环——actor 跑→reflector 评→不达标 actor 再跑。
//
// 拓扑：actor (sub-agent.Run) → eval (Reflector / EvalFunc) → [End | actor]
//
// 退出条件三层（优先级递减）：
//  1. Reflector 返 GoalMatched=true（业务认可输出）
//  2. EvalFunc 返 true（简化版 GoalMatched）
//  3. round >= MaxRounds（兜底防死循环，默认 5）
//
// 共享 session：actor sub-agent 写 events 到父 session。
//
// **events 跨 actor/reflector 共享是设计意图**（R32 audit 视角 B HIGH-1 澄清）：
// reflector 看 actor 输出做评估、actor 第 N+1 轮看上轮 reflector opinion 做改进
// ——是 ReflectAgent 反思迭代语义的核心。**嵌套场景**：`AgentAsTool(ReflectAgent)`
// 时 ReflectAgent 收到的 child sess 与父独立 events，但 actor/reflector 之间仍
// 共享同一 child sess.events。详见 AgentAsTool godoc。
//
// 嵌套 cp：每轮 actor 通过 ctx push graphNS（"round_<N>"）让子 cp 物理隔离。
type ReflectAgent struct {
	name           string
	description    string
	actor          Agent
	reflector      Reflector
	evalFn         EvalFunc
	evaluator      Agent // sub-agent 模式：跑后看 output 是 GoalMatched=true 还是 false
	reflectorAgent Agent // sub-agent 模式：跑后输出转 Opinion
	maxRounds      int
	hooks          hook.Hooks
	retryCfg       compose_v2.RetryConfig // 每轮 actor sub-agent.Run 的重试策略
}

// NewReflectAgent 创建 ReflectAgent。
//
// 推荐 builder 风格：
//
//	ref := agent_v2.NewReflectAgent(
//	    agent_v2.WithReflectName("write_with_review"),
//	    agent_v2.WithReflectMaxRounds(3),
//	    agent_v2.WithReflector(myReflector),
//	).WithActor(writerAgent)
func NewReflectAgent(opts ...ReflectOption) *ReflectAgent {
	r := &ReflectAgent{}
	for _, opt := range opts {
		opt(r)
	}
	// #7 F6 fail-fast: WithReflectName required (7 编排同款 — F6 fail-fast 范围
	// = Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除)
	if r.name == "" {
		panic("NewReflectAgent: WithReflectName required")
	}
	// 四编排 review RF-M1 fail-fast：三档评估静默优先级（Reflector > Evaluator >
	// EvalFunc）让低优先档成 dead config——项目自带 adv_08 都踩过（三档同配，
	// EvalFunc 0 调用但标题宣称按它评分）。未上线无存量，多档同配直接 panic 逼二选一。
	configured := 0
	if r.reflector != nil {
		configured++
	}
	if r.evaluator != nil {
		configured++
	}
	if r.evalFn != nil {
		configured++
	}
	if configured > 1 {
		panic("NewReflectAgent: 评估档位互斥，只能配一档（WithReflector / WithEvaluator[+WithReflectorAgent] / WithEvalFunc）")
	}
	// RF-M3 fail-fast：ReflectorAgent 只在 Evaluator 档生效——无 Evaluator 时
	// 三档装配不会用到它 → 配置静默 dead（单配时整体退化单轮直通；与 Reflector
	// 档同配时 reflectorAgent 被忽略）。
	if r.reflectorAgent != nil && r.evaluator == nil {
		panic("NewReflectAgent: WithReflectorAgent 仅在 Evaluator 档生效，需配合 WithEvaluator（否则该配置静默 dead）")
	}
	return r
}

// ReflectOption 配置 ReflectAgent。
type ReflectOption func(*ReflectAgent)

// WithReflectName 设 ReflectAgent 名。
func WithReflectName(name string) ReflectOption {
	return func(r *ReflectAgent) { r.name = name }
}

// WithReflectDescription 设职责描述。
func WithReflectDescription(desc string) ReflectOption {
	return func(r *ReflectAgent) { r.description = desc }
}

// WithReflector 设 Reflector 函数。与 WithEvaluator / WithEvalFunc 互斥——
// 多档同配 NewReflectAgent 构造期 panic（R33 fail-fast，防低优先档静默成 dead config）。
func WithReflector(fn Reflector) ReflectOption {
	return func(r *ReflectAgent) {
		// audit¹⁰ HIGH-6 fix: isFrameworkNil 检 typed nil func (协议十复证).
		if isFrameworkNil(fn) {
			panic("WithReflector: nil reflector (含 Go typed nil func 陷阱)")
		}
		r.reflector = fn
	}
}

// WithEvalFunc 设简化版评估函数（仅返 GoalMatched bool）。
func WithEvalFunc(fn EvalFunc) ReflectOption {
	return func(r *ReflectAgent) {
		// audit¹⁰ HIGH-7 fix: isFrameworkNil 检 typed nil func (协议十复证).
		if isFrameworkNil(fn) {
			panic("WithEvalFunc: nil eval fn (含 Go typed nil func 陷阱)")
		}
		r.evalFn = fn
	}
}

// WithReflectMaxRounds 设最大循环轮数（默认 5）。
func WithReflectMaxRounds(n int) ReflectOption {
	return func(r *ReflectAgent) {
		if n > 0 {
			r.maxRounds = n
		}
	}
}

// WithReflectHooks 设业务 hook。
func WithReflectHooks(h hook.Hooks) ReflectOption {
	return func(r *ReflectAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		r.hooks = h
	}
}

// WithReflectRetryConfig 设每轮 actor sub-agent.Run 的重试策略。
//
// reflector 函数本身不被 retry 包装（业务自管错误传播）。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithReflectRetryConfig(cfg compose_v2.RetryConfig) ReflectOption {
	compose_v2.ValidateRetryConfig(cfg, "WithReflectRetryConfig")
	return func(r *ReflectAgent) { r.retryCfg = cfg }
}

// WithReflectMaxRetries 简化版：等价 WithReflectRetryConfig(SimpleRetry(n))。
func WithReflectMaxRetries(n int) ReflectOption {
	return func(r *ReflectAgent) { r.retryCfg = compose_v2.SimpleRetry(n) }
}

// WithEvaluator 设 evaluator sub-agent——LLM 看 actor 输出判断"达标 / 未达标"。
//
// 优先级：Reflector(fn) > Evaluator(agent) + ReflectorAgent(agent) > EvalFunc。
//
// 解析 evaluator 输出：
//  1. msg.Annotations["goal_matched"] bool 直接用
//  2. msg.Content trim 后小写匹配 "yes" / "ok" / "approved" / "true" → GoalMatched=true
//  3. 其他 → GoalMatched=false（继续循环）
//
// 与 ReflectorAgent 配合：未达标时 reflector_agent 跑产 Opinion 喂下一轮 actor。
func WithEvaluator(ag Agent) ReflectOption {
	return func(r *ReflectAgent) {
		// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
		if isAgentNil(ag) {
			panic("WithEvaluator: nil agent (含 Go typed nil interface 陷阱)")
		}
		r.evaluator = ag
	}
}

// WithReflectorAgent 设 reflector sub-agent——未达标时 LLM 产改进意见反馈给 actor。
//
// reflector_agent.Run 输入是 actor 输出，输出 message.Content 即 Opinion 文本。
//
// 与 Evaluator 配合：仅 Evaluator 返 GoalMatched=false 时调用 reflector_agent。
// 不设 ReflectorAgent 时未达标的 Opinion 留空（actor 看不到具体改进方向）。
func WithReflectorAgent(ag Agent) ReflectOption {
	return func(r *ReflectAgent) {
		// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
		if isAgentNil(ag) {
			panic("WithReflectorAgent: nil agent (含 Go typed nil interface 陷阱)")
		}
		r.reflectorAgent = ag
	}
}

// WithActor 设 actor sub-agent（必填，链式）。
func (r *ReflectAgent) WithActor(ag Agent) *ReflectAgent {
	// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
	if isAgentNil(ag) {
		panic("ReflectAgent.WithActor: nil agent (含 Go typed nil interface 陷阱)")
	}
	r.actor = ag
	return r
}

// Name 返回名。NewReflectAgent 构造期已 fail-fast 防空 name (F6).
func (r *ReflectAgent) Name() string { return r.name }

// Description 返回职责描述。
func (r *ReflectAgent) Description() string { return r.description }

// Run 启动反思循环执行。
func (r *ReflectAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "ReflectAgent.Run", &retErr)

	if input == nil {
		input = schema.UserMessage("")
	}
	input = ownRunInput(input)
	if r.actor == nil {
		return nil, fmt.Errorf("reflect agent %q: actor required (call WithActor)", r.Name())
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
	info := &hook.AgentInfo{Name: r.Name(), Type: "reflect", Round: 0}
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
	input = adoptHookInput(agentIn, input)

	var finalRef *schema.Message
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	defer func() {
		var msgs []*schema.Message
		if finalRef != nil {
			msgs = []*schema.Message{finalRef}
		}
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "ReflectAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(r.Name(), input))
	}

	final, lastRound, err := r.runGraph(ctx, &cfg, session, input, hooks)
	info.Round = lastRound // Sub-5 audit Round 2 视角 G fix
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
func (r *ReflectAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "ReflectAgent.Resume", &retErr)

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := r.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: r.Name(), Type: "reflect", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "reflect agent", r.Name(), sessionID, opts, info, hooks, r.historySubs())
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
		return nil, fmt.Errorf("reflect agent %q: ResumeActionReplan not supported (no plan node; only PlanAgent / BasicAgent / GraphAgent accept Replan)", r.Name())
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
		recoverPanicToErr(ctx, "ReflectAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	cm := compose_v2.NewChannelManager()
	// Resume 拿不到授予（授予走 RunOption）——顶层直接 Resume 本编排时它的成员没有
	// 转交 / 路由能力。嵌在 HandoffAgent / RoutingAgent 底下的续跑不走这条：外层节点重跑时
	// 仍以 Run 形态带着授予调下来。
	g, err := r.buildComposeGraph(ctx, session, hooks, memberGrants{})
	if err != nil {
		return nil, fmt.Errorf("reflect agent resume: build graph: %w", err)
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
	final := ReflectAgentFinalMessage(cm.Channels())
	finalRef = final
	info.Round = GetReflectAgentRound(cm) // Sub-5 audit Round 2 视角 G fix
	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}
	if final != nil {
		session.AddEvent(NewExitEvent(r.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// runGraph 返 (final, lastRound, err) — lastRound 是已完成 reflection 轮数
// (Sub-5 audit Round 2 视角 G fix).
func (r *ReflectAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
	hooks hook.Hooks,
) (*schema.Message, int32, error) {
	cm := compose_v2.NewChannelManager()
	g, err := r.buildComposeGraph(ctx, session, hooks, runCfg.grantsFor(r))
	if err != nil {
		return nil, 0, fmt.Errorf("reflect agent %q: build graph: %w", r.Name(), err)
	}
	cper := compose_v2.GetCheckpointer(ctx)
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(
			compose_v2.Write(ReflectAgentOriginalChannel(), input),
			compose_v2.Write(ReflectAgentInputChannel(), input),
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
	final := ReflectAgentFinalMessage(cm.Channels())
	lastRound := GetReflectAgentRound(cm)
	return final, lastRound, runErr
}

// grants 是**已自查过**发给本编排的转交 / 路由授予（空 = 没有）。每个成员显式转授——
// 「这一步是我的组成部分」这个意图只能由编排自己表达，不能靠环境让谁都捡得到。
func (r *ReflectAgent) buildComposeGraph(
	parentCtx context.Context,
	parentSession *RunSession,
	hooks hook.Hooks,
	grants memberGrants,
) (*compose_v2.Graph, error) {
	bld := NewReflectAgentBuilder().WithName(r.Name())
	if hooks != nil {
		bld = bld.WithAgentHooks(hooks)
	}
	if r.maxRounds > 0 {
		bld = bld.WithMaxRounds(r.maxRounds)
	}
	// 三档互斥（NewReflectAgent 构造期已 panic 拒多档同配）——R33 拆图后
	// Evaluator 档拆两节点：evaluate 跑 evaluator（verdict+critique），feedback
	// 跑 reflectorAgent（opinion）——reflectorAgent 内中断 Resume 只重跑 feedback
	// 节点，evaluator 不重评（RF-H1：重评 LLM 非确定 verdict 翻面时用户答案被跳过）。
	switch {
	case r.reflector != nil:
		bld = bld.WithReflector(r.reflector)
	case r.evaluator != nil:
		bld = bld.WithReflector(r.evaluatorReflector(parentSession))
		if r.reflectorAgent != nil {
			bld = bld.WithFeedbackFn(r.reflectorFeedback(parentSession))
		}
	case r.evalFn != nil:
		bld = bld.WithEvalFunc(r.evalFn)
	}

	actorAgent := r.actor
	retryCfg := r.retryCfg
	actorFn := func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
		return runMember(ctx, grants, memberSpec{
			agent: actorAgent,
			sess:  parentSession,
			input: input,
			// ns 后缀是裸 "actor" 而非带轮次——compose_v2 的 actor 节点已在入口 push
			// round_<N>（R29 audit 视角4 HIGH-2 fix），实际 graphNS 是
			// parent/round_<N>/actor，跨轮天然不同。归属同样固定 "actor"：同一个 actor
			// 第 1 轮和第 3 轮说的话归属相同。
			nsSuffix:    "actor",
			historySlot: "actor",
			retryCfg:    retryCfg,
		})
	}
	bld = bld.WithActor(actorFn)
	return bld.Compile()
}

// evaluatorReflector 把 Evaluator sub-agent 包成 Reflector（R33 拆图——原
// subAgentReflector 把 evaluator+reflectorAgent 串单闭包跑单 eval 节点，
// reflectorAgent 中断 Resume 时 evaluator 必重评：LLM 非确定 verdict 翻面 →
// 用户给 reflectorAgent 的答案静默丢。拆后本函数只跑 evaluator，跑在
// evaluate 节点；reflectorAgent 走 reflectorFeedback 跑在 feedback 节点）。
//
// 输入协议（RF-M2，reflection role-flip 保留原始 request + OBJECTIVE
// 模板）：框架合成 **user-role** 评审请求（含原始
// 目标 + actor 输出）——不再把 actor 的 assistant 消息原样直传（API 语义
// = 评审自己说过的话，且看不到 goal 判不了 "goal matched"）。
//
// **GoalMatched 解析规则**：
//   - msg.Annotations["goal_matched"] bool 直接用
//   - 否则 trim(content).lower() 匹配 "yes" / "ok" / "approved" / "true" / "passed" / "good" → true
//   - 其他 → false（critique = 输出全文，feedback 阶段喂 reflectorAgent）
func (r *ReflectAgent) evaluatorReflector(parentSession *RunSession) Reflector {
	evaluator := r.evaluator
	return func(ctx context.Context, goal, actorOutput *schema.Message) (*ReflectOpinion, error) {
		// evaluator sub-agent (observability 标默认 agent span — evaluator component kind 已
		// 退化删除: OTel GenAI 的 operation.name 没有 evaluator, sub-agent invocation 即 invoke_agent).
		// 评估器是**内部决策 LLM**：产出的是「达标 / 未达标 + 批评」这个判定，不是对话。
		// 下游（下一轮 actor）只需要结论——不进会话历史，也永不拿到转交能力。
		evEvents, err := runDecisionAgent(ctx, "evaluator", evaluator, parentSession,
			synthesizeEvalInput(goal, actorOutput), nil)
		if err != nil {
			return nil, fmt.Errorf("evaluator: %w", err)
		}
		evMsg := FinalMessage(evEvents)
		matched := parseGoalMatched(evMsg)
		critique := ""
		if !matched && evMsg != nil {
			// 评估器批评全文进 Critique（RF-M2：原实现 parse 完 yes/no 即丢，
			// reflectorAgent 盲猜缺陷）——feedback 节点喂 reflectorAgent；无
			// reflectorAgent 时 compose 层 fallback 直接作下一轮评审反馈。
			critique = evMsg.Content
		}
		return &ReflectOpinion{GoalMatched: matched, Critique: critique}, nil
	}
}

// reflectorFeedback 把 ReflectorAgent sub-agent 包成 ReflectFeedbackFn（R33
// 拆图——跑在 feedback 节点，仅"未达标且还有下一轮"可达：maxRounds 耗尽轮
// 不再白跑（RF-M4）；中断时 Resume 只重跑 feedback 节点不重评 evaluator（RF-H1）。
//
// 输入协议（RF-M2）：合成 user-role 消息含原始目标 + actor 输出 + evaluator
// 批评全文——原实现只传 actor 输出，coach 看不到评审意见只能盲猜。
func (r *ReflectAgent) reflectorFeedback(parentSession *RunSession) ReflectFeedbackFn {
	reflectorAgent := r.reflectorAgent
	return func(ctx context.Context, goal, actorOutput *schema.Message, critique string) (string, error) {
		// 反馈教练（reflector）是**内部决策 LLM**：它把评审意见加工成给下一轮 actor 的
		// 改进建议，产出是控制信号不是对话。下游（actor）只需要那句建议本身——不进
		// 会话历史，也永不拿到转交能力。
		rfEvents, err := runDecisionAgent(ctx, "reflector", reflectorAgent, parentSession,
			synthesizeFeedbackInput(goal, actorOutput, critique), nil)
		if err != nil {
			return "", fmt.Errorf("reflector_agent: %w", err)
		}
		rfMsg := FinalMessage(rfEvents)
		if rfMsg == nil {
			return "", nil
		}
		return rfMsg.Content, nil
	}
}

// synthesizeEvalInput 合成 evaluator 的 user-role 评审请求（RF-M2）。
//
// 这条同样是框架拼的，但**不打 DerivedInputAnnotation**：evaluator 和 reflectorAgent
// 是内部决策 LLM，整条分支已由 WithoutHistoryCtx 摘除，到不了历史层的写入点。在这里
// 补一个标记等于死代码，还会在测「摘除是否生效」时形成双重防护——revert 掉摘除，测试
// 因为标记还在而照样绿。职责分开：摘除管「这条分支不进历史」，标记管「这条消息是拼的」。
func synthesizeEvalInput(goal, output *schema.Message) *schema.Message {
	goalContent := ""
	if goal != nil {
		goalContent = goal.Content
	}
	outContent := ""
	if output != nil {
		outContent = output.Content
	}
	return schema.UserMessage(fmt.Sprintf(
		"原始目标：%s\n\n待评估方案：\n%s\n\n请评估该方案是否达成原始目标。",
		goalContent, outContent,
	))
}

// synthesizeFeedbackInput 合成 reflectorAgent 的 user-role 改进请求（RF-M2，
// 含 evaluator 批评全文）。
func synthesizeFeedbackInput(goal, output *schema.Message, critique string) *schema.Message {
	goalContent := ""
	if goal != nil {
		goalContent = goal.Content
	}
	outContent := ""
	if output != nil {
		outContent = output.Content
	}
	return schema.UserMessage(fmt.Sprintf(
		"原始目标：%s\n\n待改进方案：\n%s\n\n评审意见：\n%s\n\n请给出具体可操作的改进建议。",
		goalContent, outContent, critique,
	))
}

// parseGoalMatched 解析 evaluator 输出的 GoalMatched bool。
//
// 优先级：annotations["goal_matched"] bool > content trim 关键字。
func parseGoalMatched(msg *schema.Message) bool {
	if msg == nil {
		return false
	}
	if msg.Annotations != nil {
		if v, ok := msg.Annotations["goal_matched"].(bool); ok {
			return v
		}
	}
	switch strings.TrimSpace(strings.ToLower(msg.Content)) {
	case "yes", "ok", "approved", "true", "passed", "good":
		return true
	}
	return false
}

var _ Agent = (*ReflectAgent)(nil)
