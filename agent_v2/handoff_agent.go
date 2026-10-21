package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
)

// HandoffAgent 多个 sub-agent 间转交控制权——active agent 决定是否 handoff 给谁。
//
// 拓扑：每轮调 active agent.Run；它这次调用做出的转交决定（上行信号：transfer 工具或 HandoffTo）
// 决定下一轮 active——stepFn 取发给本跳的那条、写进 HandoffSignalKey 给条件边（成员产出上带来的
// 这个字段会被清掉，不是转交通道）；没有决定 = done；超 maxHops = exhausted。
//
// 共享 session：每轮 sub-agent 写 events 到父 session。
//
// **events 跨 hop 共享 + 后接看前接**（§9 历史可见性）：hop2 的 active agent **默认通过
// HistoryFor（Rewrite 模式）改写第三人称看到 hop1 的输出**做"接力"决策——是 Handoff 转交
// 语义的核心（"frontdesk → billing → tech_support" 后接看前接对话上下文，对齐业内）。
// 可用 WithHandoffHistoryMode 切 SelfOnly（隔离）/ Raw（保 role；悬空 transfer tool_call
// 由汇合层剥离、换成一条中性记账文本）。**嵌套场景**：`AgentAsTool(HandoffAgent)` 时 HandoffAgent
// 收到的 child sess 与父独立 events，但 hop 之间仍共享同一 child sess.events。
// 详见 AgentAsTool godoc。
//
// 嵌套 cp：每一跳的候选通过 ctx push graphNS（"agent_<name>_hop<N>"）让子 cp 物理写
// (parentSID, parentNS/agent_<name>_hop<N>)——同一个候选在不同跳各有一个 ns，互不覆盖。
type HandoffAgent struct {
	name        string
	description string
	agents      map[string]Agent
	initial     string
	maxHops     int
	historyMode HandoffHistoryMode // 转交后后接看到的历史视图（默认 Rewrite=看前接改写第三人称；§9）
	hooks       hook.Hooks
	retryCfg    compose_v2.RetryConfig // 每步 active sub-agent.Run 的重试策略

	// defaultStore 是持久 active 的兜底载体——模型 B 跨 Run 记住 active agent（业务
	// 未在 ctx push store 时用）。storeOnce 保 lazy init race-free。
	// 见 doc/v2/handoff.md §5.2：业务 push store（Redis/PG）→ 真持久；不 push → 默认
	// InMemoryStore（单进程多 Run 有效）。
	defaultStore store.Store
	storeOnce    sync.Once

	// stateBySession 按 session.RunID 记录最近的 HandoffStateSnapshot，供
	// 外部通过 State(session) 实时查询当前激活 agent / hops / done / exhausted。
	// 由 stepFn 内每次 hop 完成后更新；并发 Run 不同 session 互不影响。
	stateBySession sync.Map
}

// HandoffStateSnapshot 是 HandoffAgent.State() 返回的外部视图——与内部 channel 状态解耦。
type HandoffStateSnapshot struct {
	Current   string // 当前持有控制权的 agent 名（首次 Run 前为 initial）
	Hops      int    // 已发生的 handoff 次数
	Done      bool   // 本会话已正常结束（active agent 不再 handoff）
	Exhausted bool   // 达到 maxHops 上限被动结束
}

// NewHandoffAgent 创建 HandoffAgent。
//
// 推荐 builder 风格：
//
//	h := agent_v2.NewHandoffAgent(
//	    agent_v2.WithHandoffName("router"),
//	    agent_v2.WithHandoffInitial("frontdesk"),
//	    agent_v2.WithHandoffMaxHops(5),
//	).
//	    AddAgent("frontdesk", frontdeskAgent).
//	    AddAgent("billing", billingAgent)
func NewHandoffAgent(opts ...HandoffOption) *HandoffAgent {
	// 默认 historyMode=Rewrite——handoff 默认"后接看前接"但改写第三人称。
	// 理由（悬空剥离 fix 后更新）：Rewrite 把前接 transfer tool_call 拍平成 "For context:
	// [X] called tool..." 文本痕迹，接力语义完整；Raw 下悬空 transfer 被 buildInitialMessages
	// 汇合层剥离（历史上曾原样上 wire 致 qwen/OpenAI API 400——剥离兜底后解除），消息本身保留、
	// 换成一条中性记账文本（「此前发起过 transfer_to_agent(...)」）。默认仍 Rewrite——它是为接力
	// 写的第三人称叙述。见 §9。
	h := &HandoffAgent{agents: make(map[string]Agent), historyMode: HandoffHistoryRewrite}
	for _, opt := range opts {
		opt(h)
	}
	// #7 F6 fail-fast: WithHandoffName required (7 编排同款 — F6 fail-fast 范围
	// = Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除)
	if h.name == "" {
		panic("NewHandoffAgent: WithHandoffName required")
	}
	return h
}

// HandoffOption 配置 HandoffAgent。
type HandoffOption func(*HandoffAgent)

// WithHandoffName 设 HandoffAgent 名。
func WithHandoffName(name string) HandoffOption {
	return func(h *HandoffAgent) { h.name = name }
}

// WithHandoffDescription 设职责描述。
func WithHandoffDescription(desc string) HandoffOption {
	return func(h *HandoffAgent) { h.description = desc }
}

// WithHandoffInitial 设首轮执行 agent name（必填）。
func WithHandoffInitial(name string) HandoffOption {
	return func(h *HandoffAgent) { h.initial = name }
}

// WithHandoffMaxHops 设最大转交次数（默认 10）。
func WithHandoffMaxHops(n int) HandoffOption {
	return func(h *HandoffAgent) {
		if n > 0 {
			h.maxHops = n
		}
	}
}

// WithHandoffHistoryMode 设转交后新接手 agent 看到的历史视图。
//
// 默认 HandoffHistoryRewrite——后接看前接但改写第三人称（拍平前接 tool_call 成文本，
// 保留转交文本痕迹，兑现"后接看前接"接力语义）。
// 其余两档 opt-in：HandoffHistorySelfOnly=隔离（后接只看自己历史，敏感数据不跨 agent）；
// HandoffHistoryRaw=看前接保 role（悬空 transfer tool_call 由汇合层剥离不再 400，换成一条
// 中性记账文本——看重接力叙述用默认 Rewrite）。
// 见 doc/v2/handoff.md §9。
//
// **已知 LOW（ISSUE-93 遗留①）**：若 handoff 候选是非 handoff 编排（Sequential 等）包
// BasicAgent，该 mode 经 ctx 透传会被内层 BasicAgent 继承（信息可见性扩大，不丢数据/不破协议）。
func WithHandoffHistoryMode(mode HandoffHistoryMode) HandoffOption {
	return func(h *HandoffAgent) { h.historyMode = mode }
}

// WithHandoffHooks 设业务 hook。
func WithHandoffHooks(h hook.Hooks) HandoffOption {
	return func(ha *HandoffAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		ha.hooks = h
	}
}

// WithHandoffRetryConfig 设每步 active sub-agent.Run 的重试策略。
//
// 每 hop 独立计 attempt——不影响 maxHops 计数。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithHandoffRetryConfig(cfg compose_v2.RetryConfig) HandoffOption {
	compose_v2.ValidateRetryConfig(cfg, "WithHandoffRetryConfig")
	return func(ha *HandoffAgent) { ha.retryCfg = cfg }
}

// WithHandoffMaxRetries 简化版：等价 WithHandoffRetryConfig(SimpleRetry(n))。
func WithHandoffMaxRetries(n int) HandoffOption {
	return func(ha *HandoffAgent) { ha.retryCfg = compose_v2.SimpleRetry(n) }
}

// AddAgent 注册候选 agent。链式。name 是 transfer 工具 / [HandoffTo] 的目标名，也是
// [WithHandoffInitial] 用的名字。
//
// **R30 视角D MED-1 fix**：name 含 NS 非法字符（ns 分隔符 "/"、"#"、"@"）会让 stepFn 里
// `JoinNS(parentNS, "agent_<name>_hop<N>")` 拼出错位 NS，下游 SplitNS / IsNSInSubtree 解析错乱
// 污染 cp 寻址 + skill fork 递归保护降级。Compile 时再 panic 排查体验差，链式 builder
// 中段 panic-fast 让业务命名错误第一时间暴露。对照 RoutingAgentBuilder.AddBranch 同协议。
func (h *HandoffAgent) AddAgent(name string, ag Agent) *HandoffAgent {
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("HandoffAgent.AddAgent: name %q invalid for ns: %v", name, err))
	}
	// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
	if isAgentNil(ag) {
		panic(fmt.Sprintf("HandoffAgent.AddAgent: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
	}
	// review-v6 M-12 fix: dup name 校验 — 之前 `h.agents[name] = ag` map 写入 silent
	// 覆盖 agA 让业务期望的"billing"用 agA 实际 agB 跑. 与 ParallelAgent.AddBranch /
	// PlanAgent.AddSubAgent 协议对称.
	if _, exists := h.agents[name]; exists {
		panic(fmt.Sprintf("HandoffAgent.AddAgent: duplicate name %q", name))
	}
	h.agents[name] = ag
	return h
}

// Name 返回名。NewHandoffAgent 构造期已 fail-fast 防空 name (F6).
func (h *HandoffAgent) Name() string { return h.name }

// Description 返回职责描述。
func (h *HandoffAgent) Description() string { return h.description }

// State 返指定 session 的最近 HandoffStateSnapshot——业务实时观测当前激活 agent / hops。
//
// session==nil / RunID 为空 / 未跑过此 session → 返 {Current: initial}（默认未启动状态）。
//
// 并发安全：sync.Map 内部 lock，多 goroutine 同时 State() 安全。
//
// **R25 audit MED-2 设计契约**：State() 反映**最后一次成功 hop** 的快照——
// stepFn 抛错（含 InterruptError 中断挂起）时**不更新** stateBySession（出错时直接
// 返回）。语义后果：
//   - HITL 暂停时 State().Current 显示**前一 hop** 的 agent，而非当前正在被中断的 agent
//   - hops 计数也不含中断所在的 hop（仅累计已成功完成的转交次数）
//
// **业务期望对照**：
//   - 想观测"已完成进度" → State() 正确（这是预期语义）
//   - 想观测"当前实时位置含中断态" → State() **不**反映，需配合 cm.PendingInterrupts +
//     业务侧 sub-agent.Run hook 自己跟踪
//
// **不修复理由**：让 InterruptError 路径也写 stateBySession 会引入"中断的 hop 算不算
// 已完成"歧义——业务读 hops 计数时无法分辨"3 hop + 中断"vs"4 hop + 中断"。当前
// 语义"State()=已成功完成的快照"更清晰；HITL 状态另走 Checkpointer + cm.PendingInterrupts
// 路径单独跟踪。
func (h *HandoffAgent) State(session *RunSession) HandoffStateSnapshot {
	if session == nil || session.RunID == "" {
		return HandoffStateSnapshot{Current: h.initial}
	}
	if v, ok := h.stateBySession.Load(session.RunID); ok {
		return v.(HandoffStateSnapshot)
	}
	return HandoffStateSnapshot{Current: h.initial}
}

// CleanupSession 是业务侧主动清理一个 sessionID 的 stateBySession entry —
// review-v6 M-22 fix: stateBySession 永不自动清理, 长跑服务 + 高 session 流转
// 率时 distinct RunID 累积让 map 缓慢膨胀.
//
// 业务 long-running 服务的最佳实践:
//   - 定期 sweeper 扫已超时 session, 调 `ha.CleanupSession(sid)` 主动清
//   - 或 cper 自己有 TTL 时, sweeper 同时清 cp + state
//
// 业务不调时影响: 单 state entry 几 KB, 万级孤儿 MB 量级 — 短跑业务可忽略.
func (h *HandoffAgent) CleanupSession(sessionID string) {
	if sessionID == "" {
		return
	}
	h.stateBySession.Delete(sessionID)
}

// Run 启动 handoff 执行。
func (h *HandoffAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "HandoffAgent.Run", &retErr)

	if input == nil {
		input = schema.UserMessage("")
	}
	if h.initial == "" {
		return nil, fmt.Errorf("handoff agent %q: initial required (use WithHandoffInitial)", h.Name())
	}
	if len(h.agents) == 0 {
		return nil, fmt.Errorf("handoff agent %q: no agents; call AddAgent", h.Name())
	}
	if _, ok := h.agents[h.initial]; !ok {
		return nil, fmt.Errorf("handoff agent %q: initial %q not registered via AddAgent", h.Name(), h.initial)
	}
	if err := resolveRunSession(ctx, &session, h.Name()); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)
	// 历史写入器由**编排这一层**拥有：子里可能有不是 BasicAgent 的执行体（远程 / 自研 /
	// 别的框架的壳），它们不会建也不会写。见 startHistoryForOrchestration。
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, h.historySubs(), h.Name())
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
	hooks := h.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: h.Name(), Type: "handoff", Round: 0}
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
		recoverPanicToErr(ctx, "HandoffAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		_ = hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: msgs})
	}()

	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(h.Name(), input))
	}

	final, hops, err := h.runGraph(ctx, &cfg, session, input, hooks)
	info.Round = hops // Sub-5 audit Round 2 视角 G fix
	if err != nil {
		if final != nil {
			finalRef = final
		}
		return collectEvents(session, startOffset), err
	}
	finalRef = final
	if final != nil {
		session.AddEvent(NewExitEvent(h.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。
func (h *HandoffAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "HandoffAgent.Resume", &retErr)

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := h.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: h.Name(), Type: "handoff", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "handoff agent", h.Name(), sessionID, opts, info, hooks, h.historySubs())
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
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, h.historySubs(), h.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	if rcfg.action == ResumeActionReplan {
		return nil, fmt.Errorf("handoff agent %q: ResumeActionReplan not supported (no plan node; only PlanAgent / BasicAgent / GraphAgent accept Replan)", h.Name())
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
		recoverPanicToErr(ctx, "HandoffAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		_ = hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: msgs})
	}()

	cm := compose_v2.NewChannelManager()
	// Resume 拿不到授予（ResumeOption 是另一套）：顶层直接 Resume 时候选没有转授下来的路由能力。
	g, err := h.buildComposeGraph(ctx, session, hooks, nil)
	if err != nil {
		return nil, fmt.Errorf("handoff agent resume: build graph: %w", err)
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
	final := HandoffAgentFinalMessage(cm.Channels())
	finalRef = final
	info.Round = GetHandoffAgentHops(cm) // Sub-5 audit Round 2 视角 G fix
	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}
	if final != nil {
		session.AddEvent(NewExitEvent(h.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// runGraph 返 (final, hops, err) — hops 是累计 handoff 次数 (Sub-5 audit Round 2 视角 G fix).
func (h *HandoffAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
	hooks hook.Hooks,
) (*schema.Message, int32, error) {
	cm := compose_v2.NewChannelManager()
	// 本 HandoffAgent 收到的路由授予转授给候选（候选是它的组成部分）；转交授予不转——每一跳给候选
	// 新铸一份，地址是本跳的收件人。
	g, err := h.buildComposeGraph(ctx, session, hooks, runCfg.grantsFor(h).route)
	if err != nil {
		return nil, 0, fmt.Errorf("handoff agent %q: build graph: %w", h.Name(), err)
	}
	cper := compose_v2.GetCheckpointer(ctx)
	// **模型 B**：读持久 active 决定起始 agent（无/陈旧 → initial），写入 active channel
	// 让图从该 agent 起跑（而非恒从 initial）。见 doc/v2/handoff.md §7 入口路由。
	startActive := h.readPersistedActive(ctx, h.storeForActive(ctx), handoffThreadID(ctx, session))
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(
			compose_v2.Write(HandoffAgentInputChannel(), input),
			compose_v2.Write(HandoffAgentActiveChannel(), startActive),
		),
	}
	if cper != nil {
		sid := session.RunID
		runOpts = append(runOpts,
			compose_v2.WithCheckpointer(cper),
			compose_v2.WithSessionID(sid),
			// 嵌套调用交来的输入摘要（RunSubAgent 在调用方一侧对输入消息算，不含起始成员——
			// 那个值从持久层读回，转交之后合法地会变）
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
	final := HandoffAgentFinalMessage(cm.Channels())
	hops := GetHandoffAgentHops(cm)
	return final, hops, runErr
}

// handoffAttempt 是一次 sub-agent 调用尝试的产物：本步的产出 + 它决定转交给谁。
//
// **为什么把目标塞进返回值而不是写进外层的捕获变量**：捕获变量能在错误路径上被赋值，
// 而「失败/中断的那次尝试写下的目标不许参与路由」这条约束就只剩注释在守——实测把
// 取目标那一步挪到错误检查之前，整套用例照样全绿（那条约束在捕获变量形态下不可观测，
// 因为 stepFn 遇错就早退、最后一次成功的赋值又会盖掉先前的）。走返回值之后它变成
// 类型上的事：错误路径**构造不出**带目标的 handoffAttempt。
type handoffAttempt struct {
	out *schema.Message // 本步产出（成功路径必非 nil）
	// signals 是**这一次尝试**交上来的上行信号（候选那次调用做出的转交决定在里面），交给
	// 重试**之外**去取。
	//
	// ⚠️ **为什么不在闭包里就取**：取的那一步之后才做的检查一旦报错，落在重试范围里就会把一次
	// **已经成功的 agent 执行**整个重放（含它的工具副作用；曾经「目标互相矛盾」在这里判时实测扣款
	// 跑了 4 次）。冲突现在在合并点就处理了（同一超步报冲突、同一批 tool_call 排在前面的生效），但「ag.Run 成功之后才做
	// 的检查」不该进重试范围这条照旧——放在外面，将来加的这类检查天然不在里面。
	signals compose_v2.Signals
}

func (h *HandoffAgent) buildComposeGraph(
	parentCtx context.Context,
	parentSession *RunSession,
	hooks hook.Hooks,
	route *routeGrant,
) (*compose_v2.Graph, error) {
	bld := NewHandoffAgentBuilder().WithName(h.Name())
	if hooks != nil {
		bld = bld.WithAgentHooks(hooks)
	}
	if h.maxHops > 0 {
		bld = bld.WithMaxHops(h.maxHops)
	}
	bld = bld.WithInitial(h.initial)
	for n := range h.agents {
		bld = bld.AddCandidate(n)
	}

	agents := h.agents
	retryCfg := h.retryCfg
	// 模型 B：候选集（agents keys）——随授予交给候选，transfer 工具与 HandoffTo 据它验证目标合法性（§6.3）。
	candSet := make(map[string]bool, len(agents))
	for n := range agents {
		candSet[n] = true
	}
	stepFn := func(ctx context.Context, input *schema.Message, activeName string) (*schema.Message, error) {
		ag, ok := agents[activeName]
		if !ok {
			return nil, fmt.Errorf("handoff_agent: active %q not registered", activeName)
		}
		// **R30 视角B HIGH-1/2 fix**：从 ctx 读真 hops（compose_v2 stepNode 已 push）——
		// 避免原 hopsCounter 闭包计数器跨 Resume 重置（buildComposeGraph 重建闭包）。
		// 真 hops 持久化在 compose_v2.handoffAgentHopsCh (Sum) → cp.NodeVisits 跨
		// Resume 正确恢复。
		currentHops := GetHandoffHops(ctx)
		// 嵌套 ns 边界：按 agent name + hop 序号 push 让多 hop 同 active agent 子 cp
		// 物理隔离 + ResumeContext 跨 hop 不串台（同 ask_user 算 InterruptID 时
		// graphNS 5 维含 hop_<N> 自然不同 → 多 hop 同 sub-agent ID 不冲突）。
		parentNS := compose_v2.GetGraphNS(ctx)
		childNS := compose_v2.JoinNS(parentNS, fmt.Sprintf("agent_%s_hop%d", activeName, currentHops))
		subCtx := compose_v2.WithGraphNS(ctx, childNS)
		// 归属位置用 activeName（AddAgent 的注册名，强制唯一）——**剔掉 hop**。
		// graphNS 必须带 hop（每跳 cp 要隔离），归属必须不带：hops 存 ctx、每个 Run 从 0
		// 重新计数，同一个 agent 在 Run 1 是 hop1、Run 2 续接时变 hop0，带上它就跨 Run
		// 读不回自己的历史。而同一个 agent 在第 0 跳和第 2 跳说的话，归属本来就该相同。
		subCtx = WithHistorySlot(subCtx, activeName)
		// sub-agent scope（见 store_scope_ctx.go）。**转交这一处不走 runMember**：它是
		// 转交能力的铸造点（建 grant 而非转授）、每次尝试收一次候选做出的转交决定、补记还要按
		// 历史视图模式分档——三样都是这一处独有的。哨兵
		// TestMemberDispatch_AllAgentRunsGoThroughHelper 里登记着这条豁免。
		subCtx = WithSubAgentStoreScope(subCtx, true)
		// 同 runMember：把执行交给候选，它的 agent 级钩子有自己的相位（见 compose_v2.HandOver）
		subCtx = compose_v2.HandOver(subCtx, childNS)
		// 候选拿本跳新铸 / 转授给它的那几份（签名），拿不到上方放给 RegrantFromNode 的那份（见 shadowNodeGrants）。
		subCtx = shadowNodeGrants(subCtx)
		// 本跳的收件人：候选那次调用里做出的转交决定发给它（见 side_signal.go）。在 step 节点的
		// 这次执行里铸——续跑时同一跳算出同一个值，存在候选存档里的决定仍认得出是发给这里的；节点外
		// 的代码算不出来。
		dest := handoffDest(ctx, childNS)
		if dest == "" {
			return nil, fmt.Errorf("handoff_agent: 铸不出本跳的收件人（step 不在节点执行里）")
		}
		subCtx = withHandoffDest(subCtx, dest)
		// 模型 B §9：push 历史视图模式让后接 BasicAgent 的 prior 切换（默认 Rewrite=看前接改写第三人称）。
		subCtx = WithHandoffHistoryModeCtx(subCtx, h.historyMode)

		// 转交的输入守卫按**历史视图模式**分——同一个「限不限归属」开关，两档答案相反：
		// 共享视图（Rewrite/Raw）整场一条流，原始输入任何归属下已在就不重落（per-slot 会
		// 落成两份，成功与 pause 两路**都**中招——成功那半是 pre-existing，第二跳外部执行体
		// 成功时实测 {front:1, billing:1} 两份）；SelfOnly 每条流各自完整，front 那份**不该**
		// 挡 billing 的（any-stream 会让被转交方的流永远缺问，实测 {front:1, billing:0}）。
		// 见 inputAlreadyInAnyStream 的 godoc。
		sharedView := h.historyMode != HandoffHistorySelfOnly
		attempt, err := compose_v2.RunWithRetry(subCtx, retryCfg, func(ctx context.Context) (handoffAttempt, error) {
			// **一次尝试收一次**：候选这次调用做出的决定只在它成功时交上来；失败 / 中断的这次
			// 尝试做出的随它作废（compose_v2.CaptureSignals）。停下时候选图里已提交的那些留在
			// 它的存档里，续跑跑完时一起交上来——做出决定的那一步续跑时不再执行也不丢。
			//
			// **转交能力在这一刻授予**：受让方就是 ag 这个对象本身（不是它的名字——
			// 注册名与 ag.Name() 互相独立，而名字业务还能自己取重）。经 RunOption 进
			// ag 的签名，不进 ctx——进了 ctx 就是环境，孙辈照样读得到（见 handoffGrant）。
			// 每次尝试铸一份新的。
			var events []*AgentEvent
			sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
				var runErr error
				events, runErr = ag.Run(c, parentSession, input,
					withHandoffGrant(&handoffGrant{grantee: ag, candidates: candSet}),
					withRouteGrant(route.regrant(ag)))
				return runErr
			})
			if err != nil {
				if !sharedView || !inputAlreadyInAnyStream(ctx, input) {
					settleHistoryOnPause(ctx, ag.Name(), input, err)
				}
				// 失败/中断的这次尝试**交不出目标**：它做出的决定已随 CaptureSignals 作废
				// （目标只能从成功返回的那条 handoffAttempt 里出来，见该类型 godoc）。
				return handoffAttempt{}, err
			}
			final := FinalMessage(events)
			if final == nil {
				final = schema.AssistantMessage("")
			}
			// 执行体不是 BasicAgent 时它不写历史，边界补记——只补没记的。
			// 共享视图下原始输入已在流里时只补产出（输入传 nil），守卫方向同上。
			settleInput := input
			if sharedView && inputAlreadyInAnyStream(ctx, input) {
				settleInput = nil
			}
			settleHistorySlot(ctx, ag.Name(), settleInput, final)
			// **决定带出去、不在这里取**——冲突检查在重试之外做，见 handoffAttempt.signals。
			return handoffAttempt{out: final, signals: sigs}, nil
		})
		if err != nil {
			return nil, err
		}
		out := attempt.out
		// 取发给本跳的转交——在重试**之外**（理由见 handoffAttempt.signals）。一个收件人上至多一条：
		// 先后做出的已按后者为准归并，并发做出的在合并点就处理了（同一超步两个成员都做了决定报
		// compose_v2.SignalConflictError 并终局、同一批 tool_call 排在前面的生效）。剩下的（发给更上方
		// 的）交给本节点这次执行，跟着它的提交继续往上走。
		toolTarget, rest := takeHandoffTarget(attempt.signals, dest)
		compose_v2.EmitSignals(ctx, rest)

		// **模型 B 转交信号只有上行信号一条来路**（transfer 工具与 HandoffTo 做出的是同一类）。
		//
		// 曾经还有第二条：读 out.Annotations[HandoffSignalKey]。它已删——那条路**绕过
		// 全部三道校验**（有没有被授予 / 目标合不合法 / 这一层有没有人接）：实测一个
		// 连 transfer 工具都没有、`WithHandoffTransfer()` 压根没配的候选，一行
		// `out.Annotations[HandoffSignalKey] = "billing"` 就把对话转走了，err 全程 nil
		// ——**opt-in 开关只挡住了工具路径**。而产出消息还会流经压缩层、hook、业务壳，
		// 那些层没有一个知道自己在改控制流。
		//
		// 程序化成员（业务自己实现的 Agent）现在走 [HandoffTo]：同一套校验，且只有被
		// 授予方本人调得动。哨兵 TestHandoffAnnotation_NoLongerATransferChannel。
		nextTarget := toolTarget

		// **入站的这个字段一律不作数——必须在这里清掉。**
		//
		// `out` 是成员的产出消息，业务写得进；而下游模型 A 的条件边**直接读它**
		// （`handoff_builder.go` 解析 nextTarget 那段）。只删本函数里那条读取是不够的
		// ——不清的话产出带着的 annotation 会原样穿透到条件边，裁决权照样落在任何能改
		// 产出的代码手上（实测：删了本函数的读取，探针里那个未授予的候选**仍然**转走了
		// 对话）。清掉之后，这个字段的唯一写入者是紧接着的那几行：框架按自己裁决的结果写。
		out = clearHandoffTargetAnnotation(out)
		// 写回 out.Annotations[HandoffSignalKey] 让 compose_v2 stepNode 路由（shallow clone +
		// copy map 防污染原 out）；并持久 active 到 store（转交时，§6.3 stepFn 统一持久）。
		if nextTarget != "" {
			cloned := *out
			m := make(map[string]any, len(out.Annotations)+1)
			for k, v := range out.Annotations {
				m[k] = v
			}
			m[HandoffSignalKey] = nextTarget
			cloned.Annotations = m
			out = &cloned
			if _, ok := agents[nextTarget]; ok {
				if st := h.storeForActive(ctx); st != nil {
					if tid := handoffThreadID(ctx, parentSession); tid != "" {
						if perr := st.Put(ctx, handoffActiveNS(h.name, compose_v2.GetOwner(ctx), tid), handoffActiveKey, nextTarget); perr != nil {
							// audit MED: 持久失败不阻断本 Run（路由走 channel 不依赖 store），但
							// 跨 Run active 会丢——slog 让业务可观测（对齐 BasicAgent store.Put pattern）。
							slog.WarnContext(ctx, "handoff persist active failed (跨 Run active 将丢失)",
								"agent", h.Name(), "thread", tid, "target", nextTarget, "err", perr)
						}
					}
				}
			}
		}
		// hops snapshot 用真值（currentHops + 1 表示本 hop 已成功完成）
		newHops := currentHops + 1
		// **audit LOW fix**：用**有效** maxHops 判 Exhausted。h.maxHops 默认 0（WithHandoffMaxHops
		// 仅 n>0 才写），buildComposeGraph 未显式传时 builder 用 defaultHandoffMaxHops(=10) 终止。
		// 原 `h.maxHops > 0` 门槛让默认配置下本分支永不进入——真撞 10 跳耗尽时 builder 正确终止
		// 但 State() 误报 {Exhausted:false, Current:第 11 个永不运行的 agent}，业务靠 Exhausted
		// 检测"撞上限"在默认档收不到信号。对齐 builder 的有效值。
		effectiveMaxHops := h.maxHops
		if effectiveMaxHops <= 0 {
			effectiveMaxHops = defaultHandoffMaxHops
		}
		snap := HandoffStateSnapshot{
			Current: activeName,
			Hops:    newHops,
		}
		if nextTarget == "" {
			snap.Done = true
		} else if _, ok := agents[nextTarget]; !ok {
			// 非法 target → 视为 done（与 compose_v2 内部一致）
			snap.Done = true
		} else if newHops >= effectiveMaxHops {
			snap.Exhausted = true
			snap.Current = nextTarget // 下一会被激活但被截断
		} else {
			snap.Current = nextTarget // 准备 hop 到下一个
		}
		if parentSession != nil && parentSession.RunID != "" {
			h.stateBySession.Store(parentSession.RunID, snap)
		}

		return out, nil
	}
	bld = bld.WithStepFn(stepFn)
	return bld.Compile()
}

const handoffActiveKey = "active"

// handoffActiveNS 算持久 active 的 store namespace（name + threadID 维度）。
// **name 维度防跨实例污染**（audit LOW-2，同 MED-1 pending key 根因）：两个不同
// HandoffAgent 共享同一 ThreadID + 同一外部 store 时，无 name 维度会读写同一 active slot
// 互相覆盖 + readPersistedActive 自愈误清对方 active。加 name 隔离。默认 InMemoryStore 是 per-instance
// 不触发，仅业务共享 Redis/PG 时才需。
func handoffActiveNS(name, owner, threadID string) store.Namespace {
	// `__` 前缀是**约定标记**：告诉读代码的人这条 key 归框架。
	//
	// **它不是访问控制，而且这里不设访问控制。** 那个 `store.Store` 实例是业务构造的
	// （`st := store.NewInMemoryStore()`），`store.WithContext` 只是把它递进来——想改
	// 这条 key 的业务代码闭包里捕获 `st` 就行，不经过任何框架给的句柄。**没有可以放
	// 守卫的窄口**，所以这里不放一个会给人假安全感的。
	//
	// 改了会怎样：下一轮从这条 key 读起始 active（见 readPersistedActive）。**只能在
	// 已注册候选里重选**——写一个不在候选表里的**名字**会被打回 initial 并清掉，
	// 不是任意控制流注入。
	// ⚠️ 精度：空串和非 string 值走的是另一条分支——**打回 initial 但不清**（它在
	// 清理那一步之前就返回了）。所以「凡是非法值都会被清掉」是错的读法。
	//
	// 真边界在两处，都不在这一层：(a) 别给模型暴露能写任意 `store.Store` namespace 的
	// 工具（模型只能经工具动作影响它）；(b) 多租户时按**已认证主体**分区——
	// **(b) 已做（2026-08-24）**：owner 非空时进 ns，见下方。
	//
	// # owner 这一维
	//
	// threadID 来自 wire，是**续接标识**不是授权令牌——任何知道它的人都能递给我们，
	// 而且就该能（前端刷新重连拿的正是同一个）。没有 owner 维时两个租户递同一个
	// threadID 会撞同一个 slot：实测租户 B 的全新对话被租户 A 的 active 直接送进了
	// 另一个候选。
	//
	// owner 由服务端从已认证凭据解析（见 [compose_v2.WithOwner]），**进 ns 而不是当
	// 一道闸**：合法刷新（同 owner + 同 threadID）照常命中，猜中 threadID 但 owner
	// 不同的读到空——不存在 deny 决策，没有合法场景被打断。
	//
	// 空 owner（单租户 / 没配）时 ns 形状跟以前**完全一致**，不是「多一个空段」——
	// 那会让所有既有存档一次性失联。
	if owner == "" {
		return store.NS("__handoff_active__", name, threadID)
	}
	return store.NS("__handoff_active__", name, owner, threadID)
}

// storeForActive 取持久 active 的 store：优先 ctx（业务 push 的 Redis/PG 等），无则
// HandoffAgent 默认 InMemoryStore（单进程多 Run 有效，lazy init race-free）。见 §5.2。
func (h *HandoffAgent) storeForActive(ctx context.Context) store.Store {
	if st := store.FromContext(ctx); st != nil {
		return st
	}
	h.storeOnce.Do(func() { h.defaultStore = store.NewInMemoryStore() })
	return h.defaultStore
}

// handoffThreadID 取持久 active 的**会话维度**：优先 ThreadID，无则退化 session.RunID。
//
// # 退化档给到的是「任务内持久」，不是「跨任务持久」
//
// 持久 active 是**会话级**状态（「你现在在跟账务组说话」），而本框架里声明会话身份的唯一
// 入口是 `compose_v2.WithThreadIDCtx`——cp 这层不认识会话（ISSUE-98）。业务没声明会话时，
// 框架不替它猜，退化成拿 `session.RunID` 当键。
//
// 而 `RunSession.RunID` 的契约是**一枚只服务一个任务**：同任务内（含 Resume）它不变 ⟹
// 转交在任务内持久；业务 `NextTask()` 开新任务 ⟹ 键变了 ⟹ **新任务从 initial 重新开始**。
//
// ⚠️ **这一档曾经「看起来能跨轮」，靠的是业务复用 sess 且不换 RunID**——那是契约钉死之前
// 的偶然。实测三组对照：无 ThreadID + 不换 ID ⟹ 第二轮仍在 billing；无 ThreadID +
// `NextTask` ⟹ 第二轮退回 frontdesk；有 ThreadID + `NextTask` ⟹ 第二轮仍在 billing。
//
// ⟹ **想要跨任务的转交连续性就得声明 ThreadID**，这不是限制而是让「哪些轮次算同一场
// 对话」由业务说了算。退化档也不是坏事：没声明会话时，转交不泄漏进一个无关的新任务
// （跟 approve_always 按任务分是同一条理由——授权/转交不该长于用户能预期的语境）。
//
// 见 doc/v2/handoff.md §5.2 / §8。
func handoffThreadID(ctx context.Context, session *RunSession) string {
	if tid := compose_v2.GetThreadID(ctx); tid != "" {
		return tid
	}
	if session != nil {
		return session.RunID
	}
	return ""
}

// readPersistedActive 读持久 active——无/陈旧（agent 集合变更后残留非法名）→ 回退
// initial（并清陈旧 store，§6.4 入口非法目标处理：不静默死路，非法目标回退 initial
// 而非死路 goto）。
func (h *HandoffAgent) readPersistedActive(ctx context.Context, st store.Store, threadID string) string {
	if st == nil || threadID == "" {
		return h.initial
	}
	item, err := st.Get(ctx, handoffActiveNS(h.name, compose_v2.GetOwner(ctx), threadID), handoffActiveKey)
	if err != nil || item == nil {
		return h.initial
	}
	active, _ := item.Value.(string)
	if active == "" {
		return h.initial
	}
	if _, ok := h.agents[active]; !ok {
		// 陈旧/非法（agent 集合变更后残留）→ fallback initial + 清陈旧
		if perr := st.Put(ctx, handoffActiveNS(h.name, compose_v2.GetOwner(ctx), threadID), handoffActiveKey, nil); perr != nil {
			slog.WarnContext(ctx, "handoff clear stale active failed",
				"agent", h.Name(), "thread", threadID, "stale", active, "err", perr)
		}
		return h.initial
	}
	return active
}

// EndConversation 清理某会话的持久 active——模型 B 跨 Run 持久 active 按 ThreadID
// 存储，会话结束（用户登出 / 超时 sweeper）应主动清理，防 store 按 ThreadID 无限增长。
//
// 业务用 ThreadID（= WithThreadIDCtx push 的值）调用；未用 ThreadID（退化单 Run）时
// 用 session.RunID。
//
// **⚠️ store 来源须与 Run 对称**（audit LOW-MED）：storeForActive 优先用 ctx 里的 store
// （store.WithContext push），无则 HandoffAgent 默认 InMemoryStore。若业务跑 Run 时用
// 外部 store（Redis/PG，经 ctx push），调 EndConversation **必须传同一带 store 的 ctx**——
// 否则退回空的默认 InMemoryStore，清理静默 no-op，外部 store 里的 active 泄漏。
//
// 见 doc/v2/handoff.md §5.2 清理责任。
func (h *HandoffAgent) EndConversation(ctx context.Context, threadID string) error {
	if threadID == "" {
		return nil
	}
	st := h.storeForActive(ctx)
	if st == nil {
		return nil
	}
	return st.Put(ctx, handoffActiveNS(h.name, compose_v2.GetOwner(ctx), threadID), handoffActiveKey, nil) // nil 等价 Delete
}

var _ Agent = (*HandoffAgent)(nil)
