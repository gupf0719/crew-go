package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// SequentialAgent 按顺序运行 sub-agent 链——内部编译为 channel-based 线性 graph。
//
// 拓扑：
//
//	StartNode → step[0].Run → step[1].Run → ... → step[N-1].Run → EndNode
//
// 每步 input 拼装（自动）：
//   - step 0：caller 传入的原始 input
//   - step N (N≥1)：framework 拼装 "原始目标 + 上一步（prev step name）的产出"
//     —— 让下一步 sub-agent 看到实际需要加工的素材
//
// 共享 session：所有 sub-agent.Run 写 events 到父 session（供 caller 看完整流水/审计）。
//
// **step 间数据通路是 stepInput 拼接文本，不是共享 session**（修正 R32 audit 旧表述——
// 该表述称 step2 从 SelfHistoryCompacted 读 step1 OutputEvent，但实际看不到）：各 step 是
// 不同 sub-agent（不同 name），BasicAgent step 缺省走 owner-aware SelfHistoryCompacted——
// step2 只看**自己 name** 的 events，**看不到** step1 的 OutputEvent。所以"前一步产出可被
// 后一步看到"靠的是上方"每步 input 拼装"的"原始目标 + 上一步产出"文本（compose_v2/
// sequential_agent.go），共享 session 仅供 caller 审计 + 各 step 看自己历史。跨多步依赖
// （step3 要 step1 产出）用 PlanAgent 的 InputRefs 结构化引用。
//
// **嵌套场景**：业务 `AgentAsTool(SequentialAgent)` 时 ISSUE-79 半共享让 SeqAgent
// 收到的 child sess 与父 sess events 独立（父 LLM 不见 SeqAgent 内部细节），但
// SeqAgent 内部 step 之间仍共享同一 child sess.events。详见 AgentAsTool godoc。
//
// 嵌套 cp 物理分离：每步 sub-agent 拿同一个 session（RunID = 父 sid）+ push graphNS 让子 cp
// 写 (parentSID, parentNS/stepName) 路径——R5 嵌套 cp 协议。
//
// HITL Resume：sub-agent 抛 InterruptError 时 framework 自动 SaveNS（含 step 编号
// 在 cp.CurrentFrontier）。caller 用同 sessionID 调 SequentialAgent.Resume 续跑——
// 自动从中断的 step 重跑，前面成功的 step 不重跑。
//
// **设计参考**：老 agent.SequentialAgent (290 行)。本实现承袭其 API 形态
// （AddStep + Run + Resume）但底层换成 compose_v2 channel-based graph。
type SequentialAgent struct {
	name        string
	description string
	steps       []SequentialStep
	hooks       hook.Hooks
	retryCfg    compose_v2.RetryConfig // 每步 sub-agent.Run 的重试策略
}

// SequentialStep 是 SequentialAgent 注册的单步——含 step 名 + sub-agent 实例。
type SequentialStep struct {
	Name  string // step 名（用于 cp ns + 日志）
	Agent Agent  // sub-agent 实例
}

// NewSequentialAgent 创建 SequentialAgent。
//
// 推荐 builder 风格（与 BasicAgent 一致）：
//
//	seq := agent_v2.NewSequentialAgent(
//	    agent_v2.WithSequentialName("research_then_write"),
//	).
//	    AddStep("researcher", researcherAgent).
//	    AddStep("writer", writerAgent)
func NewSequentialAgent(opts ...SequentialOption) *SequentialAgent {
	s := &SequentialAgent{}
	for _, opt := range opts {
		opt(s)
	}
	// #7 F6 fail-fast: WithSequentialName required (空 name 让 AgentAsTool / cp NS
	// 嵌套 silent 撞——故构造期 fail-fast. 7 编排同款 — F6 fail-fast 范围 =
	// Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除.)
	if s.name == "" {
		panic("NewSequentialAgent: WithSequentialName required")
	}
	return s
}

// SequentialOption 配置 SequentialAgent。
type SequentialOption func(*SequentialAgent)

// WithSequentialName 设 SequentialAgent 名（用于 hook AgentInfo + 日志 + cp 寻址）。
func WithSequentialName(name string) SequentialOption {
	return func(s *SequentialAgent) { s.name = name }
}

// WithSequentialDescription 设 agent 职责描述（供 LLMPlanner / SupervisorAgent 自动
// 注入 planner prompt）。
func WithSequentialDescription(desc string) SequentialOption {
	return func(s *SequentialAgent) { s.description = desc }
}

// WithSequentialHooks 设业务 hook 实例——SequentialAgent 自身的 OnAgentStart
// 等回调（与 sub-agent 各自 hooks 隔离）。
func WithSequentialHooks(h hook.Hooks) SequentialOption {
	return func(s *SequentialAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		s.hooks = h
	}
}

// WithSequentialRetryConfig 设每步 sub-agent.Run 的重试策略。
//
// 每步独立计 attempt——A 步失败重试 N 次成功后，B 步若也失败仍可重试 N 次。
// InterruptError 永不重试（HITL 控制流不能被吃掉）。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithSequentialRetryConfig(cfg compose_v2.RetryConfig) SequentialOption {
	compose_v2.ValidateRetryConfig(cfg, "WithSequentialRetryConfig")
	return func(s *SequentialAgent) { s.retryCfg = cfg }
}

// WithSequentialMaxRetries 简化版：等价 WithSequentialRetryConfig(SimpleRetry(n))。
func WithSequentialMaxRetries(n int) SequentialOption {
	return func(s *SequentialAgent) { s.retryCfg = compose_v2.SimpleRetry(n) }
}

// AddStep 追加一个执行步骤；name 用作节点 ID、cp ns、会话历史的归属与日志标识，也会写进
// 下一步的输入（「上一步（name）的产出」）。改名会让旧的会话历史对不上。
//
// **fail-fast**：agent 为 nil（含 typed nil）、name 重复、不符合 ns 命名规范（含 "/" "#" "@"
// 或为空）、或是图的起止节点名（`__start__` / `__end__`）时**立即** panic（本函数内）。后两类
// 建图时也会被拒，但那已是 Run 期，得到的是一个会被外层重试的错误。
//
// 必须在 Run 之前调用。返回自身以便链式。
func (s *SequentialAgent) AddStep(name string, ag Agent) *SequentialAgent {
	// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface 陷阱 (与 PlanAgent 协议对称)
	if isAgentNil(ag) {
		panic(fmt.Sprintf("SequentialAgent.AddStep: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
	}
	// review-v6 M-12 + M-17 fix: dup name 校验 — 之前 silent 覆盖 agA 让 step name 错乱
	// (compose_v2 graph AddNode map 写入静默盖 + b.steps[idx-1].Name 用于 "上一步产出"
	// LLM 看到错乱 step name). 与 ParallelAgent.AddBranch / PlanAgent.AddSubAgent 协议对称.
	// 注: 之前注释承诺 "compose_v2 内部已校验" 实际只校验 NS 字符不校验 dup, 误导业务.
	for _, st := range s.steps {
		if st.Name == name {
			panic(fmt.Sprintf("SequentialAgent.AddStep: duplicate name %q", name))
		}
	}
	// 步骤名直接当节点 ID 与存档命名空间：不符合 ns 命名规范的、与图的起止节点同名的都在这里
	// 拒绝。建图时也会拦，但那已是 Run 期，得到的是一个会被外层重试的普通错误。
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("SequentialAgent.AddStep: name %q invalid for ns: %v", name, err))
	}
	if name == string(compose_v2.EndNode) || name == string(compose_v2.StartNode) {
		panic(fmt.Sprintf("SequentialAgent.AddStep: name %q 是图的起止节点名，请换名", name))
	}
	s.steps = append(s.steps, SequentialStep{Name: name, Agent: ag})
	return s
}

// Name 返回 SequentialAgent 名。NewSequentialAgent 构造期已 fail-fast 防空 name (F6).
func (s *SequentialAgent) Name() string { return s.name }

// Description 返回 agent 职责描述。
func (s *SequentialAgent) Description() string { return s.description }

// Run 启动 sequential 执行并返回本次产生的 events。
//
// 流程：
//  1. 编译 compose_v2.SequentialAgent graph（每 step 包装成 SequentialStepFn 调
//     sub-agent.Run）
//  2. compose_v2.Run 驱动 + 注入 cper / sid（session.RunID）/ graphNS（嵌套场景从 ctx 取）
//  3. Run 完成后从 channels 取 final message，emit ExitEvent 到 sess
//
// session 为 nil 时框架补一个：在一个正在执行的任务里时 RunID 就是那个任务，否则沿用 ctx 上的 sid、
// 都没有才新铸（见 sessionForCtx）。RunID 为空的字面量 session、嵌套位置上不属于所在任务的 session
// 入口直接拒（见 resolveRunSession）。
func (s *SequentialAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "SequentialAgent.Run", &retErr)

	if input == nil {
		input = schema.UserMessage("")
	}
	if len(s.steps) == 0 {
		return nil, fmt.Errorf("sequential agent %q: no steps; call AddStep", s.Name())
	}
	if err := resolveRunSession(ctx, &session, s.Name()); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)
	// 历史写入器由**编排这一层**拥有：步骤里可能有不是 BasicAgent 的执行体（远程 /
	// 自研 / 别的框架的壳），它们不会建也不会写，边界补记需要写入器已经在 ctx 里。
	// 见 startHistoryForOrchestration。
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, s.historySubs(), s.Name())
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
	hooks := s.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}

	// 8-method: OnAgentStart 整个调用 1 次, 在 Run top-level fire (跟 OnAgentEnd 对称, 跟
	// PlanAgent 对齐). 在 Run 函数 scope (非 NodeFunc 闭包) fire — ctx 与
	// OnAgentEnd defer 共享同 local var, 业务 push 的 span 真生效.
	info := &hook.AgentInfo{Name: s.Name(), Type: "sequential", Round: 0}
	var finalMessagesRef []*schema.Message
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

	// hook OnAgentEnd 兜底——所有退出路径触发
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	defer func() {
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "SequentialAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: finalMessagesRef}, retErr)
	}()

	// 注入 user input 到 sess.Events 作审计
	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(s.Name(), input))
	}

	// 编译 graph + 跑
	finalMsg, allMsgs, lastStep, err := s.runGraph(ctx, &cfg, session, input, hooks)
	// Sub-5 audit Round 2 视角 G fix: update info.Round 让 OnAgentEnd 拿真 step 数.
	info.Round = lastStep
	if err != nil {
		if allMsgs != nil {
			finalMessagesRef = allMsgs
		}
		return collectEvents(session, startOffset), err
	}
	finalMessagesRef = allMsgs

	// emit final assistant 到 sess
	if finalMsg != nil {
		session.AddEvent(NewExitEvent(s.Name(), finalMsg))
	}

	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。从 ctx 取 Checkpointer 加载 cp。
//
// 自动从中断的 step 重跑，前面已成功的 step 不重跑（compose_v2 ResumeGraph
// 用 cp.CurrentFrontier 续跑，前面 step 的 channel state 已 Restore 不影响）。
func (s *SequentialAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "SequentialAgent.Resume", &retErr)

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper (详 resume_helpers.go)
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := s.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: s.Name(), Type: "sequential", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "sequential agent", s.Name(), sessionID, opts, info, hooks, s.historySubs())
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
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, s.historySubs(), s.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	if rcfg.action == ResumeActionReplan {
		return nil, fmt.Errorf("sequential agent %q: ResumeActionReplan not supported (no plan node; only PlanAgent / BasicAgent / GraphAgent accept Replan)", s.Name())
	}

	startOffset := len(session.Events())
	// **audit⁴ C HIGH-1 fix**: Resume 也 fire OnAgentStart 与 Run 协议对称
	// (跟 PlanAgent / GraphAgent / 6 编排 Resume 同款). Run 抛 Interrupt 时
	// Run defer OnAgentEnd; Resume 入口再 fire OnAgentStart + defer OnAgentEnd —
	// 业务 OTel pattern 可观测 Run / Resume 两段独立 span.
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
	var finalMessagesRef []*schema.Message
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	defer func() {
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "SequentialAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: finalMessagesRef}, retErr)
	}()

	cm := compose_v2.NewChannelManager()
	// Resume 拿不到授予（授予走 RunOption）——顶层直接 Resume 本编排时它的成员没有
	// 转交 / 路由能力。嵌在 HandoffAgent / RoutingAgent 底下的续跑不走这条：外层节点重跑时
	// 仍以 Run 形态带着授予调下来。
	g, err := s.buildComposeGraph(ctx, session, hooks, memberGrants{})
	if err != nil {
		return nil, fmt.Errorf("sequential agent resume: build graph: %w", err)
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
	finalMsg := SequentialAgentFinalMessage(cm.Channels())
	allMsgs := SequentialAgentAllMessages(cm.Channels())
	finalMessagesRef = allMsgs
	// Sub-5 audit Round 2 视角 G fix: Resume 路径同步 update info.Round.
	info.Round = GetSequentialAgentStep(cm)

	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}

	if finalMsg != nil {
		session.AddEvent(NewExitEvent(s.Name(), finalMsg))
	}
	return collectEvents(session, startOffset), nil
}

// runGraph 编译 + 跑 graph（Run 入口路径，非 Resume）。
//
// 嵌套场景同 BasicAgent.runGraph (audit R5 Bug #1 修复)：
//   - sid 取 session.RunID（任务的身份；嵌套时它就是父 sid，见 BasicAgent.runGraph）
//   - 父 push graphNS 时 forward 让子 cp 物理分离写 (parentSID, childNS)
//
// runGraph 返 (final, allMsgs, lastStep, err) — lastStep 是最后完成的 step idx
// (Sub-5 audit Round 2 视角 G fix: 让 caller Run 内 OnAgentEnd defer 之前 update
// info.Round 让业务 hook 拿到真实 step 数不是 0).
func (s *SequentialAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
	hooks hook.Hooks,
) (final *schema.Message, allMsgs []*schema.Message, lastStep int32, err error) {
	cm := compose_v2.NewChannelManager()
	g, err := s.buildComposeGraph(ctx, session, hooks, runCfg.grantsFor(s))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("sequential agent %q: build graph: %w", s.Name(), err)
	}

	cper := compose_v2.GetCheckpointer(ctx)
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(
			compose_v2.Write(SeqAgentOriginalChannel(), input),
		),
	}
	if cper != nil {
		// 任务的身份（嵌套时就是父 sid，见 BasicAgent.runGraph）
		sid := session.RunID
		runOpts = append(runOpts,
			compose_v2.WithCheckpointer(cper),
			compose_v2.WithSessionID(sid),
			runCfg.inputDigestOption(),
		)
		// 嵌套时父已 push graphNS（AgentAsTool.Call）→ forward 让子 cp 物理分离
		if ns := compose_v2.GetGraphNS(ctx); ns != "" {
			runOpts = append(runOpts, compose_v2.WithRunNS(ns))
		}
	}
	if hooks := extractOrchHooksOrCtx(ctx, runCfg.orchHooks); hooks != nil {
		runOpts = append(runOpts, compose_v2.WithHooks(adaptHooks(hooks)))
	}

	runErr := compose_v2.Run(ctx, g, cm, runOpts...)
	final = SequentialAgentFinalMessage(cm.Channels())
	allMsgs = SequentialAgentAllMessages(cm.Channels())
	lastStep = GetSequentialAgentStep(cm)
	return final, allMsgs, lastStep, runErr
}

// buildComposeGraph 把 s.steps 转成 SequentialAgentBuilder + 编译。
//
// 每 step 包装成 SequentialStepFn 调 sub-agent.Run——sub-agent 拿到的 ctx 含
// graphNS（已 push step name），cp 物理写到子 ns；返 final message 喂下一步。
// grants 是**已自查过**发给本编排的转交 / 路由授予（空 = 没有）。每一步显式转授——
// 「这一步是我的组成部分」这个意图只能由编排自己表达，不能靠环境让谁都捡得到。
func (s *SequentialAgent) buildComposeGraph(
	parentCtx context.Context,
	parentSession *RunSession,
	hooks hook.Hooks,
	grants memberGrants,
) (*compose_v2.Graph, error) {
	bld := NewSequentialAgentBuilder().WithName(s.Name())
	if hooks != nil {
		bld = bld.WithAgentHooks(hooks)
	}

	retryCfg := s.retryCfg
	for _, step := range s.steps {
		step := step // capture
		// 包装 sub-agent.Run 成 SequentialStepFn——加 retry 包装：失败 IsRetryable 时
		// 按 retryCfg 重试，InterruptError / ctx cancel 永不重试。
		stepFn := func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
			// 共享父 session：sub-agent 每次尝试的 events 经暂存 lane 提交到父 session（见
			// attemptLanes），step1→step2 互相看得到。
			return runMember(ctx, grants, memberSpec{
				agent: step.Agent,
				sess:  parentSession,
				input: input,
				// ns 与归属都用步骤注册名（AddStep 强制唯一）：一条链上每步只跑一次，
				// 两者的「逐次唯一」与「跨次稳定」在这里不冲突。
				nsSuffix:    step.Name,
				historySlot: step.Name,
				retryCfg:    retryCfg,
			})
		}
		bld = bld.AddStep(step.Name, stepFn)
	}

	return bld.Compile()
}

// 编译期接口实现检查
var _ Agent = (*SequentialAgent)(nil)
