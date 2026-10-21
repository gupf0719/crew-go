package agent_v2

import (
	"context"
	"fmt"
	"log/slog"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// isAgentNil 已迁移到 agent_v2/agent_helpers.go 让 7 编排 + 4 With* options 全用
// (audit³ 视角 1 HIGH-1: 协议对称性 — 之前只 PlanAgent 用其他编排漏防).

// TmpSubAgentConfig 定义按计划步骤动态生成的一次性 BasicAgent。
//
// 适合规划者根据任务套不同角色 / 工具集，但不需要预先注册到 subAgents 池中的场景。
//
// **解析时优先级**：PlanStep.SubAgentName > PlanStep.TmpSubAgent > 默认 executor。
//
// 它随计划的这一步进存档、续跑时从存档读回（换进程续跑也一样）：字段要导出、能 JSON 序列化，加进来的字段
// 不满足这两条时，带存档的任务读回来就丢了（或者存档整个写不进去）。规划器返回之后再改自己手里的这个对象
// 不影响已经规划好的那一步——框架写进计划的是一份拷贝。
type TmpSubAgentConfig struct {
	Name      string   // 可选；为空时自动生成 "tmp_step_<N>"
	System    string   // systemPrompt
	Prompt    string   // 可选 prefix，拼在 PlanStep.Description 之前作为 LLM 输入
	ToolNames []string // 从 PlanAgent 共享 toolkit 里筛子集；空 = 不带工具
}

// PlanStep 是高层 Plan 编排的一步描述（高层 vs compose_v2.PlanStep 低层）。
//
// 对照 compose/agent.PlanStep，加 InputRefs 用 RefSpec（可按 index 或 agent 名引用）。
//
// **三种执行器选择（按优先级）**：
//  1. SubAgentName 非空：从 subAgents map 查找
//  2. TmpSubAgent 非 nil：用配置当场构造 BasicAgent，跑完即丢
//  3. 都为空：使用默认 executor（PlanAgent.SetExecutor 设的）
type PlanStep struct {
	Description  string
	SubAgentName string             // 对应注册的 sub-agent name
	TmpSubAgent  *TmpSubAgentConfig // 当场构造的一次性 sub-agent
	InputRefs    []RefSpec          // 0-based index 或 agent name 引用
}

// RefSpec 是 PlanStep.InputRefs 的元素——二选一填 Index 或 AgentName。
type RefSpec struct {
	Index     *int   // 0-based executed 下标（取 0 处用 IndexRef helper）
	AgentName string // 倒序找最近匹配 agent 名
}

// IndexRef 0-based index 引用 helper。
func IndexRef(i int) RefSpec {
	return RefSpec{Index: &i}
}

// AgentRef 按 agent 名引用（倒序找最近）。
func AgentRef(name string) RefSpec {
	return RefSpec{AgentName: name}
}

// ExecutedStep 是已执行步骤的记录（对照 compose/agent.ExecutedStep）。
type ExecutedStep struct {
	Step   string // 步骤 description
	Agent  string // 实际执行 agent 名
	Result string // 输出文本
}

// Planner 是高层 Plan 的规划函数——每步执行完后调用决定剩余 / 收尾。
//
// 对照 compose/agent.Planner（保持业务签名相同便于迁移）。
//
//   - sess 让 Planner 读 sess.History()/Events() 作上下文
//   - input 是用户原始目标
//   - executed 是已完成的所有步骤
//
// 返回：
//   - remaining：剩余步骤；空 = 终止
//   - finalAnswer：终止时的最终答复（可空）
//   - err：规划失败
type Planner func(ctx context.Context, sess *RunSession, input string, executed []ExecutedStep) (remaining []PlanStep, finalAnswer string, err error)

// ExecInputBuilder 自定义 sub-agent 的 input 构造（按 step + 已解析的 refs）。
//
// 对照 compose/agent.ExecInputBuilder，删 sess 参数（compose_v2 不依赖 sess）。
//
// 返 (*schema.Message, nil) → 正常注入；(_, err) → 中止本步 propagate err。
//
// **builder 跑在独立的 build 节点里**（plan_build_input，见 compilePlanGraph 拓扑注释）：
// 拼好的输入写 planBuiltInputCh 提交，worker 中断重跑时从 cp 恢复**同一条**消息——
// 不在 worker 内重拼，消息身份（构造期铸造）跨重跑天然稳定。审批中断（本函数抛
// InterruptError）Resume 时只重跑 build 节点，planner 的写已提交不重跑。
//
// **HITL 支持**：业务可抛 `compose_v2.InterruptError` 做"敏感任务输入用户审批"
// 等 HITL gate（如把 PII 数据传给 sub-agent 前请人工确认）。框架按 graph 内
// SaveNS 协议自动存 cp + Resume 重跑 ExecInputBuilder 取 ResumeContext 答案续跑。
//
// 默认 builder 见 defaultExecInputBuilder——caller 不传时用此默认（不抛 err）。
type ExecInputBuilder func(
	ctx context.Context,
	original string,
	executed []ExecutedStep,
	current PlanStep,
	resolvedRefs []int,
) (*schema.Message, error)

// PlanAgent 是高层 Plan 编排——plan↔exec 循环 + Planner 决定每步剩余 + sub-agents
// 池子由 caller 提供。
//
// 实现：
//   - buildPlanGraph inline 构造 plan↔exec 拓扑（compilePlanGraph + 通用 GraphBuilder），
//     不再经低层 plan builder 分层
//   - sub-agents map[string]Agent 由 caller 传，本 PlanAgent 包成 worker 节点
//   - Planner 高层签名（sess + executed），plan 节点直接调用 + resolve 成 planExecStep
type PlanAgent struct {
	name string
	// nameExplicit 记「name 是业务显式设的，不是构造时自动生成的」。
	//
	// 只有显式设的名字才交给 graph 当身份（见 buildPlanGraph）。自动名是
	// `plan_agent_<uuid8>`——**每次构造都不同**，而 `Graph.ID()` 有 name 就直接返回它、
	// 于是它成了 cp 的持久身份：业务不设名时，重建 PlanAgent 对象后 Resume 必撞
	// ErrGraphIDConflict（跨进程 / 重启 / 每请求新建 agent 都会踩，而那是常规写法）。
	// 不显式设名时回落 fingerprint，跟 BasicAgent 一致——它的自动名同样不进 graph。
	nameExplicit     bool
	description      string
	planner          Planner
	subAgents        map[string]Agent
	executor         Agent           // 默认执行器（SubAgentName 空 + TmpSubAgent 空 时用）
	planModel        model.ChatModel // 共享 model，TmpSubAgent 构造 BasicAgent 用
	planToolkit      *tool.Toolkit   // 共享 toolkit，TmpSubAgent.ToolNames 按名筛子集
	execInputBuilder ExecInputBuilder
	maxSteps         int
	retryCfg         compose_v2.RetryConfig // 每步 sub-agent.Run 的重试策略

	// hooks 业务级 hook.Hooks（ISSUE-83 修，与其他 8 编排架构对称）。Run/Resume
	// audit² 改: defer 单一所有权 fire OnAgentEnd, panic 由 Run/Resume outer recover 兜底.
	// 默认 hook.NoOpHooks{} 让业务不传 option 不影响行为。
	hooks hook.Hooks
}

// PlanOption 配置 PlanAgent。
type PlanOption func(*PlanAgent)

// PlanName 设置 PlanAgent 名（用于 hook + 日志 + cp 寻址）。
func PlanName(name string) PlanOption {
	return func(p *PlanAgent) {
		p.name = name
		p.nameExplicit = name != ""
	}
}

// PlanDescription 设置 PlanAgent 职责描述（供 LLMPlanner / SupervisorAgent 抽取）。
func PlanDescription(desc string) PlanOption {
	return func(p *PlanAgent) { p.description = desc }
}

// checkPlanSubAgentName 校验业务给子 agent 起的名字：符合 ns 命名规范，且不撞计划编排图里
// 框架自己用的节点名。撞上内部派发 worker 名的后果不一：叫 __default_executor__ 又配了
// executor 时，子 agent 被同名的内部 worker 静默顶掉，规划器选中它时实际跑的是 executor；
// 叫 __tmp_subagent__ 时运行期报错。都在注册时拒绝。AddSubAgent 与 WithSubAgents 两个注册
// 入口共用它。
func checkPlanSubAgentName(name string) error {
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		return fmt.Errorf("invalid for NS: %w", err)
	}
	switch compose_v2.NodeID(name) {
	case compose_v2.StartNode, compose_v2.EndNode, "plan", planBuildInputNode, tmpDispatcherWorker, executorDispatchWorker:
		return fmt.Errorf("conflicts with a node the plan graph reserves")
	}
	return nil
}

// WithSubAgents 传入 sub-agents 池子——Planner 输出的 SubAgentName 必须在此 map 里。
//
// **ISSUE-85 audit MED-1 fix**: WithSubAgents 现在做与 AddSubAgent 同款 fail-fast
// 校验 (panic on NS 禁止字符 `/` `#` `@` 空字符串 + nil agent + 空字符串 key + 计划编排图自己用的
// 节点名，见 checkPlanSubAgentName)。
// 之前业务用 WithSubAgents (4 examples 都用此入口) 完全 bypass MED-4 fail-fast,
// 错误延迟到 GraphBuilder.Compile 阶段 (Run 时) 才报。修后注册时立即 fail-fast。
//
// **audit follow-up HIGH-2 fix**: 改"合并 + dup check"语义而非整 map 直接覆盖。
// 之前 `p.subAgents = agents` 让业务先 AddSubAgent 后 WithSubAgents 时已注册 silent
// 丢失——与 AddSubAgent panic-on-dup 协议哲学冲突。修后所有注册路径统一: dup 即 panic。
//
// **audit³ LOW-A2 注意**: 中途 panic (NS invalid / typed nil / dup) 时已注册项保留 (部分
// 注册 semantic). 业务应 fatal exit 不要 recover panic — 与 AddSubAgent 单点 panic 协议
// 一致. 旧 `p.subAgents = agents` 整体赋值是 atomic, 改"逐项合并"后引入此 semantic, 业务
// init 阶段 panic 通常不 recover, 可接受.
func WithSubAgents(agents map[string]Agent) PlanOption {
	return func(p *PlanAgent) {
		if p.subAgents == nil {
			p.subAgents = make(map[string]Agent)
		}
		for name, ag := range agents {
			if err := checkPlanSubAgentName(name); err != nil {
				panic(fmt.Sprintf("WithSubAgents: name %q %v", name, err))
			}
			if isAgentNil(ag) {
				panic(fmt.Sprintf("WithSubAgents: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
			}
			if _, exists := p.subAgents[name]; exists {
				panic(fmt.Sprintf("WithSubAgents: name %q already registered (silent overwrite forbidden — 与 AddSubAgent panic-on-dup 协议对称)", name))
			}
			p.subAgents[name] = ag
		}
	}
}

// WithExecInputBuilder 自定义 sub-agent input 构造逻辑。不传走 defaultExecInputBuilder。
func WithExecInputBuilder(b ExecInputBuilder) PlanOption {
	return func(p *PlanAgent) {
		// audit¹⁰ HIGH-3 fix: isFrameworkNil 检 typed nil func (协议十复证).
		if isFrameworkNil(b) {
			panic("WithExecInputBuilder: nil builder (含 Go typed nil func 陷阱)")
		}
		p.execInputBuilder = b
	}
}

// WithPlanMaxSteps 设最大步数防 Planner 死循环（默认 20）。
func WithPlanMaxSteps(n int) PlanOption {
	return func(p *PlanAgent) { p.maxSteps = n }
}

// WithPlanRetryConfig 设每步 sub-agent.Run 的重试策略。
//
// 各步独立计 attempt——某步失败重试不影响其他步的 retry budget。
//
// **`cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填**，不给在这一行就 panic——重试重放的是
// 整个成员调用连同它的副作用，而框架分不出「网络抖了一下」和「参数非法重跑也一样」。
// 省事写法：`compose_v2.SimpleRetry(n)` / `ExponentialBackoff(...)`（自带保守谓词）。
func WithPlanRetryConfig(cfg compose_v2.RetryConfig) PlanOption {
	compose_v2.ValidateRetryConfig(cfg, "WithPlanRetryConfig")
	return func(p *PlanAgent) { p.retryCfg = cfg }
}

// WithPlanMaxRetries 简化版：等价 WithPlanRetryConfig(SimpleRetry(n))。
func WithPlanMaxRetries(n int) PlanOption {
	return func(p *PlanAgent) { p.retryCfg = compose_v2.SimpleRetry(n) }
}

// WithPlanModel 设共享模型，供 PlanStep.TmpSubAgent 动态构造 BasicAgent 使用。
//
// 步骤未使用 TmpSubAgent 时此 option 不生效。
func WithPlanModel(m model.ChatModel) PlanOption {
	return func(p *PlanAgent) {
		// audit⁶ HIGH-2 fix: isFrameworkNil 检 typed nil ChatModel (协议六复证).
		if isFrameworkNil(m) {
			panic("WithPlanModel: typed nil ChatModel (含 Go interface 陷阱)")
		}
		p.planModel = m
	}
}

// WithPlanToolkit 设共享工具池，供 PlanStep.TmpSubAgent.ToolNames 按名称筛子集。
//
// 步骤未使用 TmpSubAgent 时此 option 不生效。
func WithPlanToolkit(tk *tool.Toolkit) PlanOption {
	return func(p *PlanAgent) { p.planToolkit = tk }
}

// WithPlanHooks 设业务 hook 实例——PlanAgent 整体生命周期 OnAgentEnd 在 Run/Resume
// defer 触发（ISSUE-83 修，与其他 8 编排架构对称 + 学 ISSUE-82 BasicAgent 单一
// 所有权）。
//
// 时机：
//   - OnAgentEnd: 整 PlanAgent.Run defer 1 次 + 每次 PlanAgent.Resume defer 1 次
//   - 覆盖所有退出路径（success / InterruptError / panic / ctx.Err）
//   - 业务计费/SLO/审计按"PlanAgent 整体生命周期"统计时不再漏数据
//
// 业务 hook nil 时默认 hook.NoOpHooks{}（不传 option 完全等价 pre-ISSUE-83 行为）。
//
// PlanAgent 内部 sub-agent 各自独立 hooks（业务在 sub-agent 上配 WithHooks 等），
// 与 PlanAgent 自身 hooks 隔离不互相覆盖。
func WithPlanHooks(h hook.Hooks) PlanOption {
	return func(p *PlanAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		p.hooks = h
	}
}

// SetExecutor 设默认执行器（PlanStep.SubAgentName 和 .TmpSubAgent 都为空时用）。
//
// 链式 API：返回 self。
func (p *PlanAgent) SetExecutor(a Agent) *PlanAgent {
	// audit⁴ HIGH-1 fix: isAgentNil 检 typed nil interface 陷阱 (与 7 编排 + 4 With* options 协议对称)
	// audit³ 自报"7/7 编排 + 4 With* options 全用 isAgentNil" 实际漏 SetExecutor — 同 commit 0aa15bf 谎报模式四复证.
	if isAgentNil(a) {
		panic("PlanAgent.SetExecutor: nil agent (含 Go typed nil interface 陷阱)")
	}
	p.executor = a
	return p
}

// AddSubAgent 注册一个可按 PlanStep.SubAgentName 派发的子执行器。
//
// 链式 API；与 WithSubAgents 等价但更适合一次注册一个。
//
// **ISSUE-85 MED-4 fix**: 注册时 fail-fast 校验——
//   - panic 含 NS 禁止字符 (`/` `#` `@` 空字符串) 让 NS 解析不错位
//   - panic 重复注册让业务 silent overwrite 早暴露
//   - **audit follow-up MED-1 fix**: panic on nil agent — 与 WithSubAgents fail-fast 协议对称
//   - panic 撞上计划编排图自己用的节点名（起止节点、`plan`、build 节点、两个内部派发 worker）：
//     其中叫 `__default_executor__` 又配了 executor 时，子 agent 会被同名 worker 静默顶掉
//     （见 checkPlanSubAgentName）
//
// 对照 HandoffAgent.AddAgent / ParallelAgent.AddBranch 协议一致。
func (p *PlanAgent) AddSubAgent(name string, a Agent) *PlanAgent {
	if err := checkPlanSubAgentName(name); err != nil {
		panic(fmt.Sprintf("PlanAgent.AddSubAgent: name %q %v", name, err))
	}
	if isAgentNil(a) {
		panic(fmt.Sprintf("PlanAgent.AddSubAgent: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
	}
	if p.subAgents == nil {
		p.subAgents = make(map[string]Agent)
	}
	if _, exists := p.subAgents[name]; exists {
		panic(fmt.Sprintf("PlanAgent.AddSubAgent: name %q already registered (silent overwrite forbidden)", name))
	}
	p.subAgents[name] = a
	return p
}

// NewPlanAgent 创建 PlanAgent。
//
// 必填：planner（生成 plan steps）+ WithSubAgents（执行 step 的 sub-agent 池）。
//
// **⚠️ description 默认空** (#7 audit follow-up M2): name 默认 "plan_agent_<uuid>"
// 自动 unique 防嵌套撞, **但 description 默认 ""**. 业务调
// `AgentAsTool(planAgent)` 不传 `WithAgentToolDescription` + 没用 `PlanDescription`
// 配置 description 时, AgentAsTool 构造期 **panic** (fail-fast):
//
//	plan := agent_v2.NewPlanAgent(planner)  // OK: name 自动 UUID
//	tool := agent_v2.AgentAsTool(plan)      // ❌ panic: description empty
//
// 修法二选一:
//
//	plan := agent_v2.NewPlanAgent(planner,
//	    agent_v2.PlanDescription("plan-then-execute 编排"))  // 推荐
//	// 或
//	tool := agent_v2.AgentAsTool(plan,
//	    agent_v2.WithAgentToolDescription("..."))           // override per tool
func NewPlanAgent(planner Planner, opts ...PlanOption) *PlanAgent {
	p := &PlanAgent{
		name:     "plan_agent_" + newUUID()[:8],
		planner:  planner,
		maxSteps: 20,
		hooks:    hook.NoOpHooks{}, // ISSUE-83: 默认 NoOp 让业务不传 WithPlanHooks 时无副作用
	}
	for _, o := range opts {
		o(p)
	}
	if p.execInputBuilder == nil {
		p.execInputBuilder = defaultExecInputBuilder
	}
	return p
}

// Name 返回 agent 名。
func (p *PlanAgent) Name() string { return p.name }

// Description 返回 agent 职责描述。
func (p *PlanAgent) Description() string { return p.description }

// Run 启动一次 Plan 编排并返回本次产生的 events。
//
// 流程：
//  1. buildPlanGraph 构造 plan↔exec 图（plan 节点直调 Planner + 每个 sub-agent 包成 worker 节点）
//  2. compose_v2.Run 驱动,注入 cper + sid + adaptHooks + initialWrites（original input）
//  3. Run 完成后从 channels 取 final answer,emit ExitEvent 到 sess
func (p *PlanAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (events []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err 返业务
	defer recoverToErr(ctx, "PlanAgent.Run", &retErr)
	if p.planner == nil {
		return nil, fmt.Errorf("plan agent %q: planner is required", p.name)
	}
	if input == nil {
		return nil, fmt.Errorf("plan agent %q: input message must not be nil", p.name)
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

	// **audit #5 阶段 1 follow-up HIGH-A fix**: PlanAgent.Run hook 顺序对齐 Resume +
	// 其他 8 编排——之前 defer OnAgentEnd (line 357) 在 OnAgentStart (line 389) **之前**
	// 注册, 业务 OnAgentStart 返 err / push trace 失败时 OnAgentEnd 仍 fire → OTel
	// span Start/End 配对断裂. 新顺序对齐 BasicAgent 模板:
	//   1. startOffset + info
	//   2. OnAgentStart fire (跟其他 8 编排同款, 在 buildPlanGraph 之前)
	//   3. defer OnAgentEnd (OnAgentStart 成功后注册)
	//   4. session.AddEvent + buildPlanGraph

	// Sub-5 audit Round 2/3 fix: PlanAgent step counter 在 planner 内部管理 (不在
	// compose_v2 channel), framework 拿不到. info.Round 保持 0 是 by-design — 业务
	// 关心 plan 步数从 sess.Events 内 PlanStep event 计数 (业务自填 metric).
	info := &hook.AgentInfo{Name: p.Name(), Type: "plan", Round: 0}

	// 8-method 协议：OnAgentStart 整个调用 1 次, Run 入口 fire (PlanAgent 非 ReAct 循环).
	// 之前 PlanAgent 仅 fire OnAgentEnd, 业务监控 / SLO / 计费 silently 漏数据.
	//
	// **Hook 消费语义** (CLAUDE.md): hook 返回后须消费指针入参上的修改 — 业务可能在
	// OnAgentStart 改写 agentIn.Messages 做 PII filter / input 重写 / 业务模板包装.
	// 修后从 agentIn.Messages 取末条作 input, 与 BasicAgent 协议对称.
	agentIn := &hook.AgentInput{Messages: []*schema.Message{input}}
	// 「这是不是新任务」判在钩子之前：钩子里就可能发起调用（见 enterRunTask）
	ctx = enterRunTask(ctx, session)
	var startErr error
	ctx, startErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return p.hooks.OnAgentStart(c, info, agentIn)
	})
	if startErr != nil {
		if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
			_ = p.hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: startErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, startErr)
	}

	// **ISSUE-83 fix**: hook OnAgentEnd 兜底——所有退出路径触发（success / Interrupt /
	// panic / ctx.Err / 业务错）,与其他 8 编排架构对称 + 学 ISSUE-82 BasicAgent
	// 单一所有权（PlanAgent.Run defer 是 OnAgentEnd 唯一源,sub-agent 各自独立 hooks）。
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	var finalMessagesRef []*schema.Message
	defer func() {
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "PlanAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		_ = p.hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: finalMessagesRef})
	}()

	// 消费 hook 改写: 业务清空 Messages 时退化空字符串 (与 BasicAgent 同), 业务
	// 改 Content 时新 content 真透传给 runPlanGraph (planner 看到的 original).
	effectiveMsg := input
	effectiveContent := ""
	if len(agentIn.Messages) > 0 && agentIn.Messages[len(agentIn.Messages)-1] != nil {
		effectiveMsg = agentIn.Messages[len(agentIn.Messages)-1]
		effectiveContent = effectiveMsg.Content
	}
	// 注入 user input 审计事件——在消费**之后**用改写后消息（四编排 review GA-M1
	// 波及面：与其余 7 编排同序，hook 替换式改写须进审计，否则审计存原文而
	// planner 用改写版——审计视图 vs LLM 输入视图分叉）。
	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(p.name, effectiveMsg))
	}
	finalMessagesRef = []*schema.Message{effectiveMsg} // hook 退化看到 input 即可（plan 中断时 graph 还没产 final）

	finalAnswer, err := p.runPlanGraph(ctx, &cfg, session, effectiveContent)
	if err != nil {
		// review-v6 H-05 fix: 业务错 fire OnError (非 InterruptError / ctx.Err 控制流)
		if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
			_ = p.hooks.OnError(ctx, info, &schema.CommError{Code: "plan_run_error", Message: err.Error()})
		}
		return collectEvents(session, startOffset), err
	}

	finalMsg := schema.AssistantMessage(finalAnswer)
	session.AddEvent(NewExitEvent(p.name, finalMsg))
	finalMessagesRef = []*schema.Message{finalMsg} // ISSUE-83 HIGH-1 fix: success 路径 hook 看 final assistant msg (与 BasicAgent/ParallelAgent 协议一致)
	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Plan Run。
func (p *PlanAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err 返业务
	defer recoverToErr(ctx, "PlanAgent.Resume", &retErr)
	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// PlanAgent 是 9 编排中**唯一 silent 走 Continue 路径**接受 Replan 的 (review-v6 M-16
	// 文档化) — 业务 Replan 实际行为: UserInputs 通过 ctx ResumeContext 透给 Planner,
	// Planner 调 GetResumeUserText 取用户文本主动重规划. 框架不做强制路由.
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	// Sub-5 audit Round 2/3 fix: PlanAgent step counter 在 planner 内部管理 (不在
	// compose_v2 channel), framework 拿不到. info.Round 保持 0 是 by-design — 业务
	// 关心 plan 步数从 sess.Events 内 PlanStep event 计数 (业务自填 metric).
	info := &hook.AgentInfo{Name: p.Name(), Type: "plan", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "plan agent", p.Name(), sessionID, opts, info, p.hooks, p.historySubs())
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

	// **audit #5 视角 6 F6+F7 fix**: PlanAgent.Resume hook 顺序对齐其他 8 编排——
	// 之前 OnAgentEnd defer 在 OnAgentStart 之前注册 (旧 line 487 vs 531), 让
	// buildPlanGraph err 时 OnAgentEnd 触发但 OnAgentStart 没 fire 过——业务 OTel
	// span Start/End 配对断裂. 新顺序:
	//   1. startOffset + info
	//   2. OnAgentStart fire (跟其他 8 编排同款, 在 buildPlanGraph 之前)
	//   3. defer OnAgentEnd (OnAgentStart 成功后注册)
	//   4. cm + buildPlanGraph

	startOffset := len(session.Events())

	// 8-method 协议：Resume 入口 fire OnAgentStart 与 Run 协议对称（各 1 次）.
	// HITL Resume 闭环: Run 抛 Interrupt 时 Run defer OnAgentEnd; Resume 入口再 fire
	// OnAgentStart + Resume defer OnAgentEnd — 业务可观测 Run / Resume 两段独立.
	// AgentIn.Messages 退化 nil (Resume 没有新 input — 走 cp 重建), 业务 hook 若依赖
	// messages 可从 sess.Events 取 ResumeContext.UserInputs.
	agentIn := &hook.AgentInput{}
	ctx = enterResumeTask(ctx, session)
	var startErr error
	ctx, startErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return p.hooks.OnAgentStart(c, info, agentIn)
	})
	if startErr != nil {
		if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
			_ = p.hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: startErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, startErr)
	}

	// **ISSUE-83 fix**: hook OnAgentEnd 兜底——Resume 退出路径同样触发（与 Run defer
	// 对称）。HITL Resume 闭环算 2 次 OnAgentEnd（Run 抛 Interrupt + Resume 完成）。
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	var finalMessagesRef []*schema.Message
	defer func() {
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "PlanAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		_ = p.hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: finalMessagesRef})
	}()

	cm := compose_v2.NewChannelManager()
	// Resume 拿不到授予（授予走 RunOption）——顶层直接 Resume 本编排时它的成员没有
	// 转交 / 路由能力。嵌在 HandoffAgent / RoutingAgent 底下的续跑不走这条：外层节点重跑时
	// 仍以 Run 形态带着授予调下来。
	g, err := p.buildPlanGraph(ctx, session, memberGrants{})
	if err != nil {
		return collectEvents(session, startOffset), fmt.Errorf("plan agent resume: build graph: %w", err)
	}

	runOpts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sessionID),
	}
	// **R28 audit HIGH-1 fix**：与 runPlanGraph 路径对称——嵌套场景下 forward
	// 父 graphNS 让 LoadNS(sid, ns) 拿到正确 ns 下的 PlanAgent cp（不是父 cp）。
	// 漏 forward 时 LoadNS(sid, "") 拿父 cp 导致 cm.Restore 报 "channel not registered"。
	if ns := compose_v2.GetGraphNS(ctx); ns != "" {
		runOpts = append(runOpts, compose_v2.WithRunNS(ns))
	}
	// audit Round 9 Bug B1 修复：caller userInputs 已是 *schema.Message 直接 forward
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
	if runErr != nil {
		// review-v6 H-05 fix: 业务错 fire OnError (非控制流)
		if _, isInt := compose_v2.IsInterrupt(runErr); !isInt && ctx.Err() == nil {
			_ = p.hooks.OnError(ctx, info, &schema.CommError{Code: "plan_resume_error", Message: runErr.Error()})
		}
		return collectEvents(session, startOffset), runErr
	}

	finalAnswer := planFinalAnswer(cm.Channels())
	finalMsg := schema.AssistantMessage(finalAnswer)
	session.AddEvent(NewExitEvent(p.name, finalMsg))
	finalMessagesRef = []*schema.Message{finalMsg} // ISSUE-83: hook 看到 final 视图
	return collectEvents(session, startOffset), nil
}

// runPlanGraph 编译 + 跑 plan graph（Run 入口路径）。
func (p *PlanAgent) runPlanGraph(ctx context.Context, runCfg *runConfig, session *RunSession, original string) (string, error) {
	cm := compose_v2.NewChannelManager()
	g, err := p.buildPlanGraph(ctx, session, runCfg.grantsFor(p))
	if err != nil {
		return "", fmt.Errorf("plan agent %q: build graph: %w", p.name, err)
	}

	cper := compose_v2.GetCheckpointer(ctx)
	runOpts := []compose_v2.RunOption{
		// **播的是「初始输入」，只该在新开路径 apply**（`WithInitialWrites` 现在就是这个语义）——续跑时它的当前值
		// 可能已被节点改写过，二次 apply 会把它**回退成原始输入**。
		// 实测过一次真退化：反思编排的 actor 输入被 feedback 节点写成「原目标 + 上轮回答
		// + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹ 重跑那轮看不见批评意见、
		// 等于把上一轮原样再做一遍，err 全程 nil。
		compose_v2.WithInitialWrites(planInputWrites(original)...),
	}
	if cper != nil {
		runOpts = append(runOpts, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(session.RunID),
			runCfg.inputDigestOption())
		// **R28 audit HIGH-1 fix**：嵌套场景下父已 push graphNS（如 Sequential 把
		// childNS = step.Name push 给 sub-agent.Run），forward 让 PlanAgent 内部
		// cp 物理写到 (sid, parent_ns/PlanAgent_ns) 不与父 cp 互覆。原代码漏 forward
		// → PlanAgent cp 与父 cp 都写到 (sid, "") → Resume 时 LoadNS(sid, "") 拿
		// 错的 cp → graph_id conflict 错。对照其他 8 编排都已 forward——PlanAgent
		// 是单点 regression。
		if ns := compose_v2.GetGraphNS(ctx); ns != "" {
			runOpts = append(runOpts, compose_v2.WithRunNS(ns))
		}
	}
	if hooks := extractOrchHooksOrCtx(ctx, runCfg.orchHooks); hooks != nil {
		runOpts = append(runOpts, compose_v2.WithHooks(adaptHooks(hooks)))
	}

	runErr := compose_v2.Run(ctx, g, cm, runOpts...)
	if runErr != nil {
		return "", runErr
	}
	return planFinalAnswer(cm.Channels()), nil
}

// 内置 dispatcher worker NodeID 常量——TmpSubAgent / executor 路径共用。
const (
	tmpDispatcherWorker    = compose_v2.NodeID("__tmp_subagent__")
	executorDispatchWorker = compose_v2.NodeID("__default_executor__")
	// planBuildInputNode 是拼 worker 输入的独立节点（见 compilePlanGraph 拓扑注释）。
	planBuildInputNode = compose_v2.NodeID("plan_build_input")
)

// planWorkerLogic 是单个 worker 的执行逻辑（解析 input + 跑 sub-agent）。
//
// 签名保留原 compose_v2.PlanWorkerFn 的全部入参语义——只是 current 从原低层
// compose_v2.PlanStep 改为内化的 planExecStep（字段语义相同：Description / Target /
// InputRefs []int）。makeWorkerNode 把它包成读 channel 的 compose_v2.NodeFunc。
//
//   - workerCtx: 含 graphNS / sess / ResumeContext etc
//   - current: 本步 planExecStep (Description / Target / InputRefs 已 resolve)
//   - allExecuted: **全部**已执行步骤历史 (与 Planner.executed 同语义)
//   - globalIdx: 本步在已执行序列中的全局索引 (= len(allExecuted))
//   - input: build 节点拼好的输入（经 planBuiltInputCh 提交——worker 重跑从 cp 恢复
//     同一条，**不在 worker 内重拼**，消息身份跨重跑稳定）
type planWorkerLogic func(workerCtx context.Context, current planExecStep, allExecuted []ExecutedStep, globalIdx int, input *schema.Message) (result string, err error)

// makeWorkerNode 把 planWorkerLogic 包成 compose_v2.NodeFunc——读
// planOriginalCh / planExecutedCh / planCurrentCh、解析 InputRefs 子集、调 runLogic、
// 写 ExecutedStep 回 planExecutedCh。之前这层是 compose_v2.PlanBuilder 内部的
// makeWorkerWrapper，消除低层后内化进 agent_v2。
func makeWorkerNode(runLogic planWorkerLogic) compose_v2.NodeFunc {
	return func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		original, _ := compose_v2.Get(chs, planOriginalCh)
		executed, _ := compose_v2.Get(chs, planExecutedCh)
		current, ok := compose_v2.Get(chs, planCurrentCh)
		if !ok || current == nil {
			return nil, fmt.Errorf("worker: no current step")
		}
		cur := *current
		_ = original // 原始目标现只在 build 节点消费（拼输入），worker 不再直接用

		// 输入由 build 节点拼好并提交（InputRefs 的越界校验也在那边、拼之前做）
		input, ok := compose_v2.Get(chs, planBuiltInputCh)
		if !ok {
			return nil, fmt.Errorf("worker %q: no built input (build node did not commit)", cur.Target)
		}

		result, err := runLogic(ctx, cur, executed, len(executed), input)
		if err != nil {
			return nil, fmt.Errorf("worker %q: %w", cur.Target, err)
		}

		step := ExecutedStep{
			Step:   cur.Description,
			Agent:  string(cur.Target),
			Result: result,
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(planExecutedCh, []ExecutedStep{step})}, nil
	}
}

// buildPlanGraph 把高层 Planner / sub-agents 编译成 plan↔exec 循环图。
//
// 三种执行器映射（按优先级）：
//  1. PlanStep.SubAgentName 非空 → 路由到 subAgents map 注册的 worker
//  2. PlanStep.TmpSubAgent 非 nil → 路由到 tmpDispatcherWorker，配置随 planCurrentCh
//     （planExecStep.TmpSubAgent）进存档，dispatch 时取出当场构造 BasicAgent
//  3. 都为空 + p.executor 非 nil → 路由到 executorDispatchWorker
//
// 消除低层分层后直接 inline plan 节点 + worker 节点，末尾走 compilePlanGraph
// （通用 compose_v2.GraphBuilder 公开 API）。
// grants 是**已自查过**发给本编排的转交 / 路由授予（空 = 没有）。每个 worker 显式转授——
// 「这一步是我的组成部分」这个意图只能由编排自己表达，不能靠环境让谁都捡得到。
func (p *PlanAgent) buildPlanGraph(ctx context.Context, session *RunSession, grants memberGrants) (*compose_v2.Graph, error) {
	if len(p.subAgents) == 0 && p.executor == nil {
		return nil, fmt.Errorf("plan agent %q: need at least one sub-agent (WithSubAgents/AddSubAgent) or default executor (SetExecutor)", p.name)
	}

	// plan 节点：emitPlanReplanEvent → 调高层 Planner → resolve remaining[0] →
	// 写 planCurrentCh（done 时写 planDoneCh + planAnswerCh）。
	// 之前这层逻辑分散在 compose_v2.PlanBuilder.Compile 的 planNode + agent_v2 的
	// plannerFn adapter 两处，消除低层后合并 inline。
	planNode := func(nodeCtx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		original, _ := compose_v2.Get(chs, planOriginalCh)
		executed, _ := compose_v2.Get(chs, planExecutedCh)

		// P1 observability: fire plan_replan event 让 observability fire
		// MetricPlanReplansTotal Counter — Phase initial (executed 空) vs replan (executed 非空).
		// 业务关心 plan 质量 / planner 调用频率.
		phase := PlanReplanPhaseInitial
		if len(executed) > 0 {
			phase = PlanReplanPhaseReplan
		}
		emitPlanReplanEvent(nodeCtx, phase, len(executed))

		// 规划器是**内部决策 LLM**：产出的是「下面走哪几步」，执行器只需要这份步骤表，
		// 不需要看它是怎么想出来的。内置 LLMPlanner 直接调 Generate（没有图、没有
		// channel），本来就碰不到历史层；这里摘掉是为业务自定义 Planner ——它内部若跑
		// 子 agent，那条链就会经过历史层。
		//
		// 注意只摘这一处：下面 exec 节点跑的 sub-agent 是**真活**，产出该进历史。
		remaining, finalAnswer, err := p.planner(WithoutHistoryCtx(nodeCtx), session, original, executed)
		if err != nil {
			return nil, fmt.Errorf("plan: planner: %w", err)
		}

		if len(remaining) == 0 {
			// 终止信号
			return []compose_v2.ChannelWrite{
				compose_v2.Write(planDoneCh, true),
				compose_v2.Write(planAnswerCh, finalAnswer),
			}, nil
		}

		// replan-每步语义：每次只取 remaining[0] 解析执行。
		s := remaining[0]
		var target compose_v2.NodeID
		switch {
		case s.SubAgentName != "":
			if _, ok := p.subAgents[s.SubAgentName]; !ok {
				return nil, fmt.Errorf("plan agent %q: SubAgentName %q not registered", p.name, s.SubAgentName)
			}
			target = compose_v2.NodeID(s.SubAgentName)
		case s.TmpSubAgent != nil:
			target = tmpDispatcherWorker
		default:
			if p.executor == nil {
				return nil, fmt.Errorf("plan agent %q: step %q has no SubAgentName/TmpSubAgent and no default executor", p.name, s.Description)
			}
			target = executorDispatchWorker
		}
		next := &planExecStep{
			Description: s.Description,
			Target:      target,
			InputRefs:   resolveRefs(s.InputRefs, executed),
		}
		if target == tmpDispatcherWorker {
			// 配置跟着这一步进 channel（见 planExecStep.TmpSubAgent）。拷一份：规划器返回的指针它自己还可能改。
			cfg := *s.TmpSubAgent
			cfg.ToolNames = append([]string(nil), s.TmpSubAgent.ToolNames...)
			next.TmpSubAgent = &cfg
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(planCurrentCh, next)}, nil
	}

	// build 节点：把 worker 输入**拼一次并提交进 channel**（拓扑与动机见 compilePlanGraph
	// godoc）。worker 中断重跑时本节点不重跑，输入从 cp 恢复——消息身份跨重跑稳定，
	// 历史层的去重守卫按身份认得出「这条落过」。
	//
	// ExecInputBuilder 在这里调（节点 ctx 含 nodeID——review-v6 H-01 的契约是「per-step
	// 节点 scope 不是 Run 入口 ctx」，本节点同样满足）。
	buildInputNode := func(nodeCtx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		original, _ := compose_v2.Get(chs, planOriginalCh)
		executed, _ := compose_v2.Get(chs, planExecutedCh)
		current, ok := compose_v2.Get(chs, planCurrentCh)
		if !ok || current == nil {
			return nil, fmt.Errorf("plan build input: no current step")
		}
		cur := *current

		// InputRefs 越界校验（从 makeWorkerNode 移来——builder 会按下标取 executed）
		for _, idx := range cur.InputRefs {
			if idx < 0 || idx >= len(executed) {
				return nil, fmt.Errorf("plan build input for %q: InputRef %d out of range (executed len=%d)",
					cur.Target, idx, len(executed))
			}
		}
		resolvedRefs := append([]int(nil), cur.InputRefs...)

		// 按 Target 类别重建高层 PlanStep（原三个 worker 各自的 highCurrent 逻辑合一）
		var highCurrent PlanStep
		var tmpCfg *TmpSubAgentConfig
		errPrefix := "exec input builder: %w"
		switch cur.Target {
		case tmpDispatcherWorker:
			if cur.TmpSubAgent == nil {
				return nil, fmt.Errorf("plan agent %q: tmp dispatcher: no config for description %q", p.name, cur.Description)
			}
			tmpCfg = cur.TmpSubAgent
			highCurrent = PlanStep{Description: cur.Description, TmpSubAgent: tmpCfg}
			errPrefix = "exec input builder (tmp sub-agent): %w"
		case executorDispatchWorker:
			highCurrent = PlanStep{Description: cur.Description}
			errPrefix = "exec input builder (executor): %w"
		default:
			highCurrent = PlanStep{Description: cur.Description, SubAgentName: string(cur.Target)}
		}

		// **ISSUE-85 MED-3 / audit HIGH-1 协议不变**：highExecuted 用全历史（executed），
		// resolvedRefs 直接复用高层 resolveRefs 已算的下标（不倒序 lookup）。
		input, builderErr := p.execInputBuilder(nodeCtx, original, executed, highCurrent, resolvedRefs)
		if builderErr != nil {
			// HITL / ctx.Err / 业务错统一 propagate（同 sub-agent.Run 错误协议）
			return nil, fmt.Errorf(errPrefix, builderErr)
		}
		// TmpSubAgent.Prompt 作为 input 前缀（模仿 v1 行为）。
		// **加前缀是改写不是重建**：直接 new 一条会把 ExecInputBuilder 挂在原消息上的
		// Annotations 全丢掉——业务按 MarkDerivedInput 的 godoc 标了派生输入会被静默剥掉。
		if tmpCfg != nil && tmpCfg.Prompt != "" && input != nil {
			prefixed := *input
			prefixed.Content = tmpCfg.Prompt + "\n\n" + input.Content
			input = &prefixed
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(planBuiltInputCh, input)}, nil
	}

	retryCfg := p.retryCfg
	workers := make(map[compose_v2.NodeID]compose_v2.NodeFunc)

	// 1. 注册显式 sub-agents 池里的 worker
	for name, ag := range p.subAgents {
		ag := ag
		workers[compose_v2.NodeID(name)] = makeWorkerNode(func(workerCtx context.Context, current planExecStep, allExecuted []ExecutedStep, globalIdx int, input *schema.Message) (string, error) {
			// 输入由 build 节点拼好经 planBuiltInputCh 提交（重跑从 cp 恢复同一条，
			// 身份稳定）——worker 只管跑 sub-agent。
			//
			// **R28 audit HIGH-1 + MED-1 fix + R32+ 视角 1 HIGH-1 fix**：push 子 ns（含
			// step 序号）让 sub-agent 内部 cp 物理写到 (sid, parent/<name>_step_<N>) 与
			// 父 PlanAgent + 同 sub-agent 跨 step 物理隔离。用 globalIdx（真实已执行
			// 步数，跨步严格递增）。
			out, err := runMember(workerCtx, grants, memberSpec{
				agent: ag,
				sess:  session,
				input: input,
				// ns 带 step 序号（同一个 sub-agent 跨步的 cp 要互相隔离，globalIdx 是
				// 真实已执行步数、跨步严格递增）；归属用注册名并**剔掉序号**——它被
				// 调度到第 2 步还是第 5 步，说的话归属都该是它自己。
				nsSuffix:    fmt.Sprintf("%s_step_%d", name, globalIdx),
				historySlot: name,
				retryCfg:    retryCfg,
			})
			if err != nil {
				return "", err
			}
			return out.Content, nil
		})
	}

	// 2. 注册 TmpSubAgent dispatcher——从 planCurrentCh（planExecStep.TmpSubAgent）读配置当场构造 BasicAgent
	workers[tmpDispatcherWorker] = makeWorkerNode(func(workerCtx context.Context, current planExecStep, allExecuted []ExecutedStep, globalIdx int, input *schema.Message) (string, error) {
		// 配置从 planCurrentCh 读（随存档走：中断续跑、换进程续跑都读得到）
		if current.TmpSubAgent == nil {
			return "", fmt.Errorf("plan agent %q: tmp dispatcher: no config for description %q", p.name, current.Description)
		}
		ag, err := p.makeTmpSubAgent(current.TmpSubAgent, current.Description)
		if err != nil {
			return "", fmt.Errorf("tmp sub-agent: %w", err)
		}
		// 输入（含 TmpSubAgent.Prompt 前缀）由 build 节点拼好提交——见 buildInputNode。
		// **R28 audit HIGH-1 fix + R32+ 视角 1 HIGH-1 fix**：用 globalIdx 不是 len(refs)
		// **授予显式传空**：TmpSubAgent 不进自动授予名单——它是 planner（一个 LLM）
		// 当场决定造出来的执行体，构造它的人是模型不是业务，「这一步是我的组成部分」
		// 这句话没有人说过。（ParallelAgent 的分支也拿空授予，理由不同：分支是并发的。）
		out, err := runMember(workerCtx, memberGrants{}, memberSpec{
			agent: ag,
			sess:  session,
			input: input,
			// TmpSubAgent 是 planner 临时造的、**没有注册名**，ns 和归属只能都用步序号。
			// 代价：replan 后同一个临时 agent 落到不同步序时归属会变——但临时 agent 本就
			// 一次性（每次 plan 重新生成），没有「跨 Run 读回自己历史」的诉求。
			nsSuffix:    fmt.Sprintf("tmp_step_%d", globalIdx),
			historySlot: fmt.Sprintf("tmp_step_%d", globalIdx),
			retryCfg:    retryCfg,
		})
		if err != nil {
			return "", err
		}
		return out.Content, nil
	})

	// 3. 注册 default executor dispatcher
	if p.executor != nil {
		exec := p.executor
		workers[executorDispatchWorker] = makeWorkerNode(func(workerCtx context.Context, current planExecStep, allExecuted []ExecutedStep, globalIdx int, input *schema.Message) (string, error) {
			// 输入由 build 节点拼好提交——worker 只管跑 executor。
			// **R28 audit HIGH-1 fix**：push 子 ns（exec_step_<N>）与父 PlanAgent 物理隔离
			out, err := runMember(workerCtx, grants, memberSpec{
				agent: exec,
				sess:  session,
				input: input,
				// ns 带 step 序号与父 PlanAgent 物理隔离；归属固定——executor 是
				// PlanAgent 的**单一**执行器（SetExecutor 设一个），执行第几步都是它。
				//
				// 归属用内部字面量 `__default_executor__` 而不是裸 "executor"：用户注册名
				// （AddSubAgent）和这个位置在**同一个命名空间**里，业务真注册一个叫
				// "executor" 的 sub-agent 时两者会撞、两个不同 agent 的历史合并成一条流
				// ——正是这套改动要消灭的形状。同款做法见 RoutingAgent 对 __fallback__ 的处理。
				nsSuffix:    fmt.Sprintf("exec_step_%d", globalIdx),
				historySlot: string(executorDispatchWorker),
				retryCfg:    retryCfg,
			})
			if err != nil {
				return "", err
			}
			return out.Content, nil
		})
	}

	// 显式名交给 graph 当身份（沿 ISSUE-85 MED-2：避免 fingerprint 在 sub-agents
	// 集合变更时引发 ErrGraphIDConflict）；**自动名不交**——它每次构造都不同，交上去
	// 就成了不稳定的持久身份，业务不设名时重建对象后 Resume 必撞冲突。没有显式名时
	// 回落 fingerprint，跟 BasicAgent 一致。
	graphName := ""
	if p.nameExplicit {
		graphName = p.Name()
	}
	return compilePlanGraph(planNode, buildInputNode, workers, p.maxSteps, graphName)
}

// makeTmpSubAgent 按 cfg 当场构造一次性 BasicAgent。
//
// 必填：p.planModel（共享 model）。可选：p.planToolkit（共享 toolkit，按 cfg.ToolNames 筛子集）。
func (p *PlanAgent) makeTmpSubAgent(cfg *TmpSubAgentConfig, stepDesc string) (Agent, error) {
	if p.planModel == nil {
		return nil, fmt.Errorf("plan agent %q: TmpSubAgent requires WithPlanModel", p.name)
	}
	name := cfg.Name
	if name == "" {
		// **ISSUE-85 LOW-1 fix**: 按 rune 切防多字节 UTF-8 (中文每字 3 字节) 切到中间
		// 产生非法 UTF-8 → 下游 slog/event emit 报错或乱码。
		runes := []rune(stepDesc)
		end := len(runes)
		if end > 8 {
			end = 8
		}
		name = "tmp_step_" + string(runes[:end])
	}
	opts := []Option{
		WithName(name),
		WithModel(p.planModel),
		WithSystemPrompt(cfg.System),
	}
	// 按 ToolNames 筛 toolkit 子集
	if len(cfg.ToolNames) > 0 && p.planToolkit != nil {
		filtered := tool.NewToolkit()
		want := make(map[string]bool, len(cfg.ToolNames))
		for _, n := range cfg.ToolNames {
			want[n] = true
		}
		for _, t := range p.planToolkit.List() {
			if want[t.Name()] {
				filtered.Register(t)
			}
		}
		if len(filtered.List()) > 0 {
			opts = append(opts, WithToolkit(filtered))
		}
	}
	return NewBasicAgent(opts...), nil
}

// resolveRefs 把高层 RefSpec 解析为 0-based 下标——
//   - Index 非 nil：直接用
//   - AgentName 非空：倒序找最近匹配的 executed 步骤
//   - 都为空：跳过
//
// **ISSUE-85 LOW-2 fix**: 失败 ref (Index 越界 / AgentName 未匹配) slog.Warn
// 而非静默跳过——业务调试时能从日志看到 caller 拿到的 resolvedRefs 比 InputRefs
// 短的根因, 不再 silent 漏数据。
func resolveRefs(refs []RefSpec, executed []ExecutedStep) []int {
	out := make([]int, 0, len(refs))
	for _, r := range refs {
		if r.Index != nil {
			if *r.Index >= 0 && *r.Index < len(executed) {
				out = append(out, *r.Index)
			} else {
				slog.Warn("PlanAgent.resolveRefs: IndexRef out of range — ref skipped silently (业务可能漏数据)",
					"index", *r.Index, "executed_len", len(executed))
			}
			continue
		}
		if r.AgentName != "" {
			found := false
			for i := len(executed) - 1; i >= 0; i-- {
				if executed[i].Agent == r.AgentName {
					out = append(out, i)
					found = true
					break
				}
			}
			if !found {
				slog.Warn("PlanAgent.resolveRefs: AgentRef no match in executed — ref skipped silently (业务可能漏数据)",
					"agent_name", r.AgentName, "executed_agents", executedAgentNames(executed))
			}
		}
	}
	return out
}

// executedAgentNames 抽 executed 步骤的 agent name 列表 (用于 slog.Warn 调试上下文)。
func executedAgentNames(executed []ExecutedStep) []string {
	out := make([]string, 0, len(executed))
	for _, e := range executed {
		out = append(out, e.Agent)
	}
	return out
}

// defaultExecInputBuilder 把 original + step + refs 拼成 user message 给 sub-agent。
//
// **不标 DerivedInputAnnotation**（对比 Sequential 的 stepInput）：这条虽然也是拼的，但
// 「你的任务」那段来自 planner 的 PlanStep.Description，而 planner 已被 WithoutHistoryCtx
// 摘除——计划内容本身不在历史里。不记这条，历史里就只剩各步的产出，看不出每步被要求做什么。
//
// 判据是「拼进去的东西是不是已经在历史里」，不是「是不是框架拼的」。
//
// 默认实现不抛 error——caller 想做 HITL gate 应自定义 ExecInputBuilder。
func defaultExecInputBuilder(_ context.Context, original string, executed []ExecutedStep, current PlanStep, resolvedRefs []int) (*schema.Message, error) {
	var b []byte
	b = append(b, []byte("原始目标：")...)
	b = append(b, []byte(original)...)
	b = append(b, []byte("\n\n你的任务：")...)
	b = append(b, []byte(current.Description)...)
	for _, idx := range resolvedRefs {
		if idx < 0 || idx >= len(executed) {
			continue
		}
		e := executed[idx]
		b = append(b, []byte(fmt.Sprintf("\n\n步骤 %d（%s）的产出：\n%s", idx+1, e.Step, e.Result))...)
	}
	return schema.UserMessage(string(b)), nil
}

// 编译期接口实现检查
var _ Agent = (*PlanAgent)(nil)
