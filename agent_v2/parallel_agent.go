package agent_v2

import (
	"context"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"fmt"
	"sync"
)

// ParallelAgent 并发跑 N 个 sub-agent 链——所有 branch 同 input、结果合并。
//
// 拓扑：split SendFrom N 个 worker → 全并发 → finalize 调 Merger 合并。
//
// 共享 session：所有 sub-agent.Run 最终 events 都进父 session（caller 看完整流水）。
//
// **branch 间 events 物理隔离**（ISSUE-80 修，lane events 模型）：N
// branch 并发跑时每个在开始时拿一条独立 lane child sess（siblingEvents.fork：上文是此刻
// 父 session 的事件，去掉兄弟分支提交上来的）——branch 内 AddEvent 写 lane 私有 slice
// （R33 后 lane.mu 保护，见 session.go），sibling branch 物理不可见。branch 完成时
// siblingEvents.commit 把 lane events 按 **branch 完成顺序整段 append** 合并回父（不跨
// branch 按 TS 重排——单 branch 内时序保持，跨 branch 顺序 = 完成顺序）。
//
// **同一个 agent 实例可以挂多个分支**（best-of-N 采样就是这个形状）。这条能力经历过
// 两次收窄再放开：R31 曾用 FinalMessageByAgent name filter 临时缓解 events 串台，要求
// 业务用 distinct sub-agent **名**；ISSUE-80 用 lane 隔离根治后取消。会话历史层
// （ISSUE-98）早期按 agent 名标归属，同实例的分支归属会撞，一度靠"拒绝同实例"兜底；
// 归属改用**分支注册名**（见 history_slot.go）之后，两个分支天然分属 "br1"/"br2"，
// 限制随之撤销——**根因修掉了，不再需要用限制掩盖**。
//
// ⚠️ **复用同一实例时，实例内的可变状态由业务自负线程安全**：分支是并发跑的，共享
// 实例意味着共享它持有的一切——model、toolkit、以及业务 Agent 实现自己的字段。框架
// 保证的是 events（lane 隔离）和历史归属（分支位置）分得开，不代管业务对象的并发安全。
// 无状态或自带锁的实现可以放心复用；带可变计数器 / 缓存的实现要么加锁，要么每分支一个实例。
//
// **嵌套场景**：`AgentAsTool(ParallelAgent)` 时 ParallelAgent 收到的 child sess
// (来自 NewSharedChildSession) 与父 sess events 独立，但 N branch 之间通过 lane
// 模型物理隔离。多层嵌套 fan-out (ParallelAgent 内嵌 ParallelAgent) 时内层分支 lane 从外层分支
// lane 派生、提交进外层分支 lane，外层分支再往上提交。
//
// 嵌套 cp：每 branch 通过 ctx push graphNS 让子 cp 写 (parentSID, parentNS/branchName)。
//
// **分支不能做转交 / 路由决定**：本编排当 HandoffAgent 的候选、RoutingAgent 的分类器时，收到的
// 能力不转授给分支——分支里的 BasicAgent 没有 transfer / route_to 工具，程序化 [HandoffTo] 返回
// 「没有被授予」。并行的几个分支各自表态，分歧时没有谁更有资格；想「先并行出意见、再定转交」，
// 在 [SequentialAgent] 里把一个决定方接在本编排之后。
type ParallelAgent struct {
	name        string
	description string
	branches    []ParallelBranch
	merger      ParallelMerger
	hooks       hook.Hooks
	retryCfg    compose_v2.RetryConfig // 每分支 sub-agent.Run 的重试策略
}

// ParallelBranch 单分支定义。
type ParallelBranch struct {
	Name  string
	Agent Agent
}

// NewParallelAgent 创建 ParallelAgent。
func NewParallelAgent(opts ...ParallelOption) *ParallelAgent {
	p := &ParallelAgent{}
	for _, opt := range opts {
		opt(p)
	}
	// #7 F6 fail-fast: WithParallelName required (7 编排同款 — F6 fail-fast 范围
	// = Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除)
	if p.name == "" {
		panic("NewParallelAgent: WithParallelName required")
	}
	return p
}

// ParallelOption 配置 ParallelAgent。
type ParallelOption func(*ParallelAgent)

// WithParallelName 设 ParallelAgent 名。
func WithParallelName(name string) ParallelOption {
	return func(p *ParallelAgent) { p.name = name }
}

// WithParallelDescription 设职责描述。
func WithParallelDescription(desc string) ParallelOption {
	return func(p *ParallelAgent) { p.description = desc }
}

// WithParallelMerger 设 Merger 函数。不设时按 branch name 拼接默认文本。
func WithParallelMerger(m ParallelMerger) ParallelOption {
	return func(p *ParallelAgent) {
		// audit²² HIGH-1 fix: nil func 检 (协议二十二复证). 业务 `var m ParallelMerger;
		// WithParallelMerger(m)` typed nil → 后续 merger(ctx, results) nil deref.
		// func type 用 `m == nil` 等价 isFrameworkNil (func 无 interface header type tag 陷阱).
		if m == nil {
			panic("WithParallelMerger: nil merger fn")
		}
		p.merger = m
	}
}

// Aggregator 是 Merger 的简化版——只接收 map[branchName]content，不暴露完整结果结构。
//
// 适合"按名拿内容拼"场景；想拿 BranchResult 元信息（Name/Output 结构体形态）时用
// ParallelMerger。注意 **OrigErr 当前恒 nil**：生产 worker 分支失败（重试耗尽）时
// 整 Run 直接返 FirstErr、merger 根本不跑——失败分支永远不会以 OrigErr 形态出现在
// results 里（"部分失败降级出报告"语义当前不支持，见 ParallelBranchResult godoc）。
type Aggregator func(ctx context.Context, results map[string]string) (*schema.Message, error)

// WithAggregator 设简化版聚合器（不暴露 BranchResult 元信息）。
//
// 与 WithParallelMerger 互斥；同时设时 Merger 优先生效。
func WithAggregator(a Aggregator) ParallelOption {
	return func(p *ParallelAgent) {
		// audit¹⁰ HIGH-1 fix: isFrameworkNil 检 typed nil Aggregator (协议十复证).
		if isFrameworkNil(a) {
			panic("WithAggregator: nil aggregator (含 Go typed nil func 陷阱)")
		}
		if p.merger != nil {
			return // Merger 已设，Aggregator 忽略
		}
		p.merger = AggregatorToMerger(a)
	}
}

// AggregatorToMerger 把 Aggregator 包成 ParallelMerger。
//
// 转换规则：results 按声明顺序遍历，r.Output 非 nil 则取 Content 写入 map[r.Name]
// （nil 写空字符串，error 分支跳过）。
func AggregatorToMerger(a Aggregator) ParallelMerger {
	return func(ctx context.Context, results []ParallelBranchResult) (*schema.Message, error) {
		flat := make(map[string]string, len(results))
		for _, r := range results {
			if r.OrigErr != nil {
				continue
			}
			if r.Output != nil {
				flat[r.Name] = r.Output.Content
			} else {
				flat[r.Name] = ""
			}
		}
		return a(ctx, flat)
	}
}

// WithParallelHooks 设业务 hook。
func WithParallelHooks(h hook.Hooks) ParallelOption {
	return func(p *ParallelAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		p.hooks = h
	}
}

// WithParallelRetryConfig 设每分支 sub-agent.Run 的重试策略。
//
// 各分支独立计 attempt——A 分支失败重试不影响 B 分支的 retry budget。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithParallelRetryConfig(cfg compose_v2.RetryConfig) ParallelOption {
	compose_v2.ValidateRetryConfig(cfg, "WithParallelRetryConfig")
	return func(p *ParallelAgent) { p.retryCfg = cfg }
}

// WithParallelMaxRetries 简化版：等价 WithParallelRetryConfig(SimpleRetry(n))。
func WithParallelMaxRetries(n int) ParallelOption {
	return func(p *ParallelAgent) { p.retryCfg = compose_v2.SimpleRetry(n) }
}

// AddBranch 追加并发分支。链式。
//
// **同名 branch 静默丢失防护**（R31 audit 视角 A HIGH-1 fix）：name 在 branches
// 内重复时 panic——底层 compose_v2 编译图时 `AddNode(NodeID("worker_"+name), fn)`
// map 写入会静默覆盖前一个 fn 让前一个 branch 永远不跑；同时 branchFn 用
// `JoinNS(parentNS, name)` push graphNS 共用同 childNS 让 cp 互覆。fail-fast 让
// 业务立即看到错（vs silent 串台/丢分支）。
func (p *ParallelAgent) AddBranch(name string, ag Agent) *ParallelAgent {
	// 四编排 review LOW-4：对齐 compose 层校验——非法名（含 ns 分隔符 "/"、"#"、"@"）在构造期
	// fail-fast，不等 Run 期 compose AddBranch panic 转 error（fail-fast 应在 setup 期）。
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("ParallelAgent.AddBranch: name %q invalid for ns: %v", name, err))
	}
	for _, existing := range p.branches {
		if existing.Name == name {
			panic(fmt.Sprintf("ParallelAgent.AddBranch: duplicate branch name %q (会让前一个分支静默被覆盖 + cp ns 互覆)", name))
		}
	}
	// audit³ HIGH-1 fix: isAgentNil 检 typed nil interface
	if isAgentNil(ag) {
		panic(fmt.Sprintf("ParallelAgent.AddBranch: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
	}
	p.branches = append(p.branches, ParallelBranch{Name: name, Agent: ag})
	return p
}

// Name 返回名。NewParallelAgent 构造期已 fail-fast 防空 name (F6).
func (p *ParallelAgent) Name() string { return p.name }

// Description 返回职责描述。
func (p *ParallelAgent) Description() string { return p.description }

// Run 启动 parallel 执行。
func (p *ParallelAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "ParallelAgent.Run", &retErr)

	if input == nil {
		input = schema.UserMessage("")
	}
	if len(p.branches) == 0 {
		return nil, fmt.Errorf("parallel agent %q: no branches; call AddBranch", p.Name())
	}
	if err := resolveRunSession(ctx, &session, p.Name()); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)
	// 历史写入器由**编排这一层**拥有：子里可能有不是 BasicAgent 的执行体（远程 / 自研 /
	// 别的框架的壳），它们不会建也不会写。见 startHistoryForOrchestration。
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, p.historySubs(), p.Name())
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
	hooks := p.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	// Sub-5 audit Round 2/3 fix: ParallelAgent 是 branch fan-out 单次执行, 无 "round"
	// 概念 (区别于 BasicAgent.ReAct 循环 / SequentialAgent.step / LoopAgent.iteration).
	// info.Round 保持 0 是 by-design — 业务想看 fanout branch 数走 crew.fanout.branches.
	info := &hook.AgentInfo{Name: p.Name(), Type: "parallel", Round: 0}
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
		recoverPanicToErr(ctx, "ParallelAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(p.Name(), input))
	}

	final, err := p.runGraph(ctx, &cfg, session, input, hooks)
	if err != nil {
		if final != nil {
			finalRef = final
		}
		return collectEvents(session, startOffset), err
	}
	finalRef = final
	if final != nil {
		session.AddEvent(NewExitEvent(p.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。
func (p *ParallelAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "ParallelAgent.Resume", &retErr)

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := p.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	// Sub-5 audit Round 2/3 fix: ParallelAgent 是 branch fan-out 单次执行, 无 "round"
	// 概念 (区别于 BasicAgent.ReAct 循环 / SequentialAgent.step / LoopAgent.iteration).
	// info.Round 保持 0 是 by-design — 业务想看 fanout branch 数走 crew.fanout.branches.
	info := &hook.AgentInfo{Name: p.Name(), Type: "parallel", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "parallel agent", p.Name(), sessionID, opts, info, hooks, p.historySubs())
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
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, p.historySubs(), p.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	if rcfg.action == ResumeActionReplan {
		return nil, fmt.Errorf("parallel agent %q: ResumeActionReplan not supported (no plan node; only PlanAgent / BasicAgent / GraphAgent accept Replan)", p.Name())
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
		recoverPanicToErr(ctx, "ParallelAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	cm := compose_v2.NewChannelManager()
	g, err := p.buildComposeGraph(ctx, session, hooks)
	if err != nil {
		return nil, fmt.Errorf("parallel agent resume: build graph: %w", err)
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
	final := ParallelAgentFinalMessage(cm.Channels())
	finalRef = final
	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}
	if final != nil {
		session.AddEvent(NewExitEvent(p.Name(), final))
	}
	return collectEvents(session, startOffset), nil
}

func (p *ParallelAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
	hooks hook.Hooks,
) (*schema.Message, error) {
	cm := compose_v2.NewChannelManager()
	g, err := p.buildComposeGraph(ctx, session, hooks)
	if err != nil {
		return nil, fmt.Errorf("parallel agent %q: build graph: %w", p.Name(), err)
	}
	cper := compose_v2.GetCheckpointer(ctx)
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(
			compose_v2.Write(ParAgentOriginalChannel(), input),
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
	final := ParallelAgentFinalMessage(cm.Channels())
	return final, runErr
}

// buildComposeGraph 构建本编排的图：每个分支一个并发成员，跑完由 merger 汇总。
//
// **分支不拿转交 / 路由能力**（给 runMember 的恒为空授予，本编排收到的授予不往下转）。一个收件人同一
// 超步里只能有一个决定方，而分支是同时跑的：把能力交给每个分支，就是让几个决定方并发地对同一件事
// 表态，分歧时谁也不比谁更有资格。曾经照常转授，两支各转各的就在合并点报冲突，续跑把整个超步的分支
// 连同副作用重做一遍，决定从记录里拿回时还次次再撞（实测）。想「先并行出意见、再定转交」，在
// Sequential 里把一个决定方接在本编排之后：分支的产出是意见，由那一个决定方来定。
func (p *ParallelAgent) buildComposeGraph(
	parentCtx context.Context,
	parentSession *RunSession,
	hooks hook.Hooks,
) (*compose_v2.Graph, error) {
	bld := NewParallelAgentBuilder().WithName(p.Name())
	if hooks != nil {
		bld = bld.WithAgentHooks(hooks)
	}
	if p.merger != nil {
		bld = bld.WithMerger(p.merger)
	}

	retryCfg := p.retryCfg
	siblings := &siblingEvents{set: make(map[*AgentEvent]struct{})}
	for _, br := range p.branches {
		br := br
		branchFn := func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
			// **ISSUE-80 修**：fork lane child sess——branch 内 events 写到 lane 私有
			// slice，与 sibling branch 物理隔离。完成后 siblings.commit
			// 按原有顺序合并回 parentSession（见 siblingEvents）。彻底解决 R31 视角 D HIGH-1 串台根因 + 删 R31 临时
			// FinalMessageByAgent name filter 限制（业务可同 sub-agent 实例作多 branch）。
			// **这是 7 编排里唯一让成员看不到兄弟成员事件的一处**——扇出分支之间必须互不可见。
			// 分支成员每次尝试的暂存 lane 再从这条分支 lane 派生（见 attemptLanes）。
			laneSess := siblings.fork(parentSession)
			// 无论成功失败都要 commit lane events 到 parent——失败 (含 InterruptError)
			// 时业务 / Resume 路径需要看 lane 内已写的 events（如 OutputEvent 中间步骤）；
			// panic 越过分支往上走时同样（defer 不 recover，panic 现场的栈归引擎的节点兜底）。
			defer siblings.commit(parentSession, laneSess)
			return runMember(ctx, memberGrants{}, memberSpec{
				agent: br.Agent,
				sess:  laneSess,
				input: input,
				// ns 与归属都用分支注册名（AddBranch 强制唯一），不含运行时序号。
				nsSuffix:    br.Name,
				historySlot: br.Name,
				retryCfg:    retryCfg,
			})
		}
		bld = bld.AddBranch(br.Name, branchFn)
	}
	return bld.Compile()
}

var _ Agent = (*ParallelAgent)(nil)

// siblingEvents 记下同一次并行里各分支提交进编排会话的事件，让分支开始时派生的 lane 看不到兄弟。
//
// 分支的上文 = 它开始执行那一刻编排会话的事件，去掉兄弟分支提交上来的。两条都要：
//   - 兄弟的不能看到——分支之间互不可见（同名 agent 会把兄弟的问答当成自己的上文）。只在建图时取一次快照
//     挡得住兄弟，但分支开始之前编排钩子写进会话的事件（OnGraphStart、分支节点的 OnNodeStart）也一起挡掉了，
//     跟 Sequential 的成员不一致（审计实测）。
//   - 其余的照常看到——跟 Sequential 的成员一样，开始之前写进会话的都在上文里。
//
// 按事件指针认兄弟：提交是把 lane 里的事件指针追加到编排会话，同一批指针。只有经分支 lane 提交上来的才算兄弟的：
// 钩子直接写进编排会话的（哪怕是兄弟分支节点的 OnNodeEnd 写的）算会话自己的，开始得晚的分支看得到。fork 的「取上文 + 过滤」与 commit 的
// 「提交 + 记下」在同一把锁里，兄弟不会插在中间——不然取到了它的事件、却还没记下是它的。
type siblingEvents struct {
	mu  sync.Mutex
	set map[*AgentEvent]struct{}
}

// fork 从 parent 派生一条分支 lane：上文是此刻 parent 的事件去掉兄弟提交上来的。
func (s *siblingEvents) fork(parent *RunSession) *RunSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := parent.Events()
	base := make([]*AgentEvent, 0, len(view))
	for _, ev := range view {
		if _, ok := s.set[ev]; !ok {
			base = append(base, ev)
		}
	}
	return parent.forkLaneFrom(base)
}

// commit 把分支 lane 提交进 parent，记下提交了哪些事件。
func (s *siblingEvents) commit(parent, lane *RunSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range commitLane(parent, lane) {
		s.set[ev] = struct{}{}
	}
}
