package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// GraphAgent 把任意 compose_v2.Graph 包成 Agent——让用户自定义拓扑也能被嵌入到
// 高阶 agent 编排（Sequential / Parallel / Loop / etc）作 step。
//
// **必填**：caller 提供 InputWriter（把 *schema.Message 翻译成 graph 的初始 channel
// writes）和 OutputReader（从 channels 拿 final message）——因为 channel 模型下
// graph 不像 GraphState 那样有约定俗成的"input/output 字段"，每个 graph 的入口
// channel 名称和出口 channel 名称由业务定义。
//
// **嵌套 cp 自动支持**：sid 取 session.RunID（嵌套时就是父 sid，见 BasicAgent.runGraph）、
// graphNS 从 ctx 取，forward 给 compose_v2.Run，与其他编排同协议，HITL Resume 闭环。注意方向：这说的是 GraphAgent **被嵌入**
// 外层编排时自动走嵌套 cp 协议；反方向——业务在自己的 NodeFunc **内部**调
// 子 agent.Run 时，框架不自动 push 父 ns，**必须走 [RunSubAgent]**（一站式 push
// graphNS + child session + storeScope）。
//
// ⚠️ **别手拼 childNS**：一次嵌套调用的身份是位置（实例维三选一：图级 fan-out 实例 /
// 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态重访 visit；实例节点里又起
// tool_call 并发时实例与 tool_call 一起进位置）加上「这是哪一次调用」（输入内容 + 同一份
// 输入的第几次，或调用方给的 key），存档槽由身份哈希而来——这些判定住在 [RunSubAgent] 里。
// 漏实例维 ⟹ N 个实例撞同一个 childNS、子 cp 互覆（ISSUE-96 侧面 2，实测 2 实例只得 1 个
// 子 cp）；漏 visit 维 ⟹ 父循环第 2 轮重进时子层与第 1 轮撞同一个 InterruptID；漏输入内容 ⟹
// 同一个节点里带着新输入的第二次调用接上第一次的存档、新输入没进子 agent。手拼还拿不到
// [RunSubAgent] 的完成记录（父节点重跑时已完成的调用不再执行）。**完全不 push ns 更糟**：
// 子层存档落进 `(sid, "")`——父图自己的槽 ⟹ Resume 撞 `ErrGraphIDConflict`。
// （见 doc/v2/orchestration-patterns.md §10.4）。
//
// **vs 老 agent.GraphAgent**：老版本基于 GraphState（隐含 LastOutput / Messages
// 字段），新版基于 channels 不假设具体字段名，灵活性更高但需要 caller 显式翻译。
type GraphAgent struct {
	name         string
	description  string
	graph        *compose_v2.Graph
	inputWriter  GraphInputWriter
	outputReader GraphOutputReader
	hooks        hook.Hooks
	// interruptBefore / interruptAfter 见 WithGraphInterruptBefore / WithGraphInterruptAfter。
	interruptBefore []compose_v2.NodeID
	interruptAfter  []compose_v2.NodeID
}

// GraphInputWriter 把 input message 翻译成 graph 初始 channel writes。
//
// 典型实现：
//
//	func(input *schema.Message) []compose_v2.ChannelWrite {
//	    return []compose_v2.ChannelWrite{compose_v2.Write(myInputCh, input)}
//	}
type GraphInputWriter func(input *schema.Message) []compose_v2.ChannelWrite

// GraphOutputReader 从 channels 抽 final message。
//
// 典型实现：
//
//	func(chs compose_v2.Channels) *schema.Message {
//	    out, _ := compose_v2.Get(chs, myOutputCh)
//	    return out
//	}
//
// 返 nil 的契约：GraphAgent 不 emit ExitEvent → FinalMessage(events) 为 nil；
// 嵌套消费方各自兜底（AgentAsTool 返空串 tool result、Sequential step 兜
// AssistantMessage("")）——父层无法区分"没产出"与"产出空"，业务想区分请返带
// 标记的空消息而非 nil。
type GraphOutputReader func(chs compose_v2.Channels) *schema.Message

// NewGraphAgent 创建 GraphAgent 包装 compose_v2.Graph。
//
// **必填**：graph + InputWriter + OutputReader。
func NewGraphAgent(graph *compose_v2.Graph, opts ...GraphAgentOption) *GraphAgent {
	g := &GraphAgent{graph: graph}
	for _, opt := range opts {
		opt(g)
	}
	// #7 F6 fail-fast: WithGraphAgentName required (7 编排同款 — F6 fail-fast 范围
	// = Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph; BasicAgent + PlanAgent
	// UUID 默认排除)
	if g.name == "" {
		panic("NewGraphAgent: WithGraphAgentName required")
	}
	if g.graph != nil {
		for _, bp := range []struct {
			opt string
			ids []compose_v2.NodeID
		}{{"WithGraphInterruptBefore", g.interruptBefore}, {"WithGraphInterruptAfter", g.interruptAfter}} {
			for _, id := range bp.ids {
				if id != compose_v2.AllNodes && !g.graph.HasNode(id) {
					panic(fmt.Sprintf("NewGraphAgent: %s 里的节点 %q 不在这张图里", bp.opt, id))
				}
			}
		}
	}
	return g
}

// WithGraphInterruptBefore 给这个 GraphAgent 加 before 断点：图执行到 nodes 中的节点
// （compose_v2.AllNodes = 每一个节点）之前停下，不用改节点代码——调试、逐步执行、「到这儿先让人看一眼」
// 的闸门。语义同 compose_v2.GraphBuilder.WithInterruptBefore：停下时 Run / Resume 返回
// Reason 为 compose_v2.BreakpointInterruptReason 的中断（Query.Breakpoint 写着停在哪），Resume（带不带
// 答案都行）就是放行，恰好越过这一个超步边界；要拦下不走就不 Resume，或 Resume 时
// WithResumeAction(ResumeActionAbort)。
//
// Run 与每次 Resume 都带上这组断点（逐步执行时每次 Resume 停在下一个边界）。只作用于这张图自己的节点：
// 节点里调起的 agent、子图里的节点不受影响（子图的断点在子图的 builder 上设）。节点名不在图里时构造期 panic。
func WithGraphInterruptBefore(nodes ...compose_v2.NodeID) GraphAgentOption {
	return func(g *GraphAgent) { g.interruptBefore = append(g.interruptBefore, nodes...) }
}

// WithGraphInterruptAfter 给这个 GraphAgent 加 after 断点：nodes 中的节点所在的超步整体提交之后停下。
// 其余同 [WithGraphInterruptBefore]；语义见 compose_v2.GraphBuilder.WithInterruptAfter。
func WithGraphInterruptAfter(nodes ...compose_v2.NodeID) GraphAgentOption {
	return func(g *GraphAgent) { g.interruptAfter = append(g.interruptAfter, nodes...) }
}

// errBreakpointNeedsCheckpointer 是 GraphAgent 配了断点、ctx 上却没有 checkpointer。实现
// [compose_v2.NonRetryable]：配置错重跑必然一样；不用 [compose_v2.ErrRunAborted]——它抛在图开始执行之前，
// 也没有存档可清。
type errBreakpointNeedsCheckpointer struct{ name string }

func (e *errBreakpointNeedsCheckpointer) Error() string {
	return fmt.Sprintf("agent_v2: GraphAgent %q 配了断点，但 ctx 上没有 checkpointer：停在断点上要靠存档续跑，"+
		"没有存档时 Resume 续不了、重跑又停在同一个断点。用 compose_v2.WithCheckpointerCtx 配上 checkpointer", e.name)
}

// NonRetryable 实现 [compose_v2.NonRetryable]。
func (e *errBreakpointNeedsCheckpointer) NonRetryable() {}

// checkBreakpointsResumable：配了断点（GraphAgent 自己的，或者图编译期声明的）就得有 checkpointer。停在断点
// 上只能经 Resume 续（Resume 要存档），没有存档时这个任务每次都停在第一个断点、永远过不去——Run 入口就拒。
func (g *GraphAgent) checkBreakpointsResumable(ctx context.Context) error {
	if len(g.interruptBefore)+len(g.interruptAfter) == 0 && !g.graph.HasBreakpoints() {
		return nil
	}
	if compose_v2.GetCheckpointer(ctx) != nil {
		return nil
	}
	return &errBreakpointNeedsCheckpointer{name: g.Name()}
}

// breakpointOptions 把这个 GraphAgent 的断点转成引擎选项（Run 与 Resume 都带）。
func (g *GraphAgent) breakpointOptions() []compose_v2.RunOption {
	var opts []compose_v2.RunOption
	if len(g.interruptBefore) > 0 {
		opts = append(opts, compose_v2.WithInterruptBefore(g.interruptBefore...))
	}
	if len(g.interruptAfter) > 0 {
		opts = append(opts, compose_v2.WithInterruptAfter(g.interruptAfter...))
	}
	return opts
}

// GraphAgentOption 配置 GraphAgent。
type GraphAgentOption func(*GraphAgent)

// WithGraphAgentName 设 GraphAgent 名。
func WithGraphAgentName(name string) GraphAgentOption {
	return func(g *GraphAgent) { g.name = name }
}

// WithGraphAgentDescription 设职责描述。
func WithGraphAgentDescription(desc string) GraphAgentOption {
	return func(g *GraphAgent) { g.description = desc }
}

// WithGraphInputWriter 设 input writer（必填）。
//
// **audit¹⁰ HIGH-4 fix**: isFrameworkNil 检 typed nil func (协议十复证).
func WithGraphInputWriter(w GraphInputWriter) GraphAgentOption {
	return func(g *GraphAgent) {
		if isFrameworkNil(w) {
			panic("WithGraphInputWriter: nil writer (含 Go typed nil func 陷阱)")
		}
		g.inputWriter = w
	}
}

// WithGraphOutputReader 设 output reader（必填）。
//
// **audit¹⁰ HIGH-5 fix**: isFrameworkNil 检 typed nil func (协议十复证).
func WithGraphOutputReader(r GraphOutputReader) GraphAgentOption {
	return func(g *GraphAgent) {
		if isFrameworkNil(r) {
			panic("WithGraphOutputReader: nil reader (含 Go typed nil func 陷阱)")
		}
		g.outputReader = r
	}
}

// WithGraphAgentHooks 设业务 hook。
func WithGraphAgentHooks(h hook.Hooks) GraphAgentOption {
	return func(g *GraphAgent) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil hook (协议六复证 — 8 编排同款)
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		g.hooks = h
	}
}

// Name 返回名。NewGraphAgent 构造期已 fail-fast 防空 name (F6).
func (g *GraphAgent) Name() string { return g.name }

// Description 返回职责描述。
func (g *GraphAgent) Description() string { return g.description }

// Run 启动 graph 执行。
func (g *GraphAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err 返业务
	defer recoverToErr(ctx, "GraphAgent.Run", &retErr)
	if input == nil {
		input = schema.UserMessage("")
	}
	if g.graph == nil {
		return nil, fmt.Errorf("graph agent %q: graph required", g.Name())
	}
	if g.inputWriter == nil {
		return nil, fmt.Errorf("graph agent %q: input writer required (use WithGraphInputWriter)", g.Name())
	}
	if g.outputReader == nil {
		return nil, fmt.Errorf("graph agent %q: output reader required (use WithGraphOutputReader)", g.Name())
	}
	if err := resolveRunSession(ctx, &session, g.Name()); err != nil {
		return nil, err
	}
	if err := g.checkBreakpointsResumable(ctx); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)

	// 会话历史写入器。**subs 传 nil 是实情不是省事**：GraphAgent 没有静态子列表
	//（子 agent 由业务 NodeFunc 经 RunSubAgent 调起，框架编译期看不见），所以它借不到
	// 配置，唯一的来源是运行入口声明 `WithHistoryCtx`——那个出口本来就是为「编排里一个
	// BasicAgent 都没有」这种形状留的。不另开 WithGraphAgentHistory：多一个入口就多一处
	// 要跟 WithHistoryCtx 对齐的语义。
	//
	// ⚠️ **顶层跑 GraphAgent 时配置必须走 `WithHistoryCtx`，挂在叶子上不行**。这是结构性的
	// 不是没做完：业务把 `WithHistory` 配在图里某个 BasicAgent 上时，那个写入器建在它自己
	// 的 Run 里、活在它自己的子 ctx 里，GraphAgent 这一层**根本看不到**。实测该配法下图
	// 自己那层（InputWriter 收的输入 / OutputReader 给的产出）一条都不记，只有叶子自己那
	// 两条。想要完整的一条流就在运行入口声明。
	ownsHistory := historyWriterFrom(ctx) == nil
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, nil, g.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	ownsHistory = ownsHistory && historyWriterFrom(ctx) != nil
	if ownsHistory {
		// **只开范围回执，不往 ctx 推 slot**——推了会把自己的名字变成所有后代归属的根段
		// （详见 settleHistoryTopLevel 的说明：实测两个代价，换来的"一致"只是段数一致）。
		//
		// 回执是 `settleHistoryAt` 判「这次调用范围内有没有产生记录」的依据，必须在范围
		// **开始**时开、不能等结算时补。图里的子 agent 经 RunSubAgent 记的每一笔都顺着
		// 回执链上溯记一笔，于是出口那次结算知道「里面已经有人记过了，别再补一遍输入」。
		ctx = openHistoryReceipt(ctx)
	}

	cfg := applyRunOptions(opts)
	// MED-1 fail-fast：composite 不把 per-call 覆盖透传给 inner step，传了静默 no-op，直接报错。
	if err := rejectCompositeOverride(cfg.overrides()); err != nil {
		return nil, err
	}
	startOffset := len(session.Events())
	hooks := g.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	// Sub-5 audit Round 2/3 fix: GraphAgent 包任意 compose_v2.Graph 为 Agent, 业务自定义
	// DAG 无统一 "round" 语义 (业务可能是 N 节点串行 / fanout / 循环等). info.Round 保持
	// 0 是 by-design — 业务想看节点执行数走 crew.node.executions{crew.node.id=...}.
	info := &hook.AgentInfo{Name: g.Name(), Type: "graph", Round: 0}

	// 8-method 协议：OnAgentStart 整个调用 1 次, Run 入口 fire (GraphAgent 非 ReAct 循环).
	// 之前 GraphAgent 仅 fire OnAgentEnd, 业务监控 / SLO / 计费 silently 漏数据.
	//
	// **Hook 消费语义** (CLAUDE.md): hook 返回后须消费指针入参上的修改 — 业务可改写
	// agentIn.Messages 做 PII filter / 模板包装. 修后从 agentIn.Messages 取末条作 input
	// 透给 runGraph, 与 BasicAgent 协议对称.
	agentIn := &hook.AgentInput{Messages: []*schema.Message{input}}
	// 「这是不是新任务」判在钩子之前：钩子里就可能发起调用（见 enterRunTask）
	ctx = enterRunTask(ctx, session)
	var hookErr error
	ctx, hookErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return hooks.OnAgentStart(c, info, agentIn)
	})
	if hookErr != nil {
		if _, isInt := compose_v2.IsInterrupt(hookErr); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: hookErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, hookErr)
	}
	// 消费 hook 改写: 空 Messages 时保持原 input (避免 inputWriter 拿 nil panic).
	if len(agentIn.Messages) > 0 && agentIn.Messages[len(agentIn.Messages)-1] != nil {
		input = agentIn.Messages[len(agentIn.Messages)-1]
	}

	// 注入 user input 审计事件——在 OnAgentStart 消费**之后**（四编排 review GA-M1 fix，
	// 对齐其余 7 编排 "OnAgentStart → 消费改写 → AddEvent" 序）：hook 改写（如 PII
	// filter 替换消息）须进审计事件，否则审计存原文而 graph 用脱敏版——审计视图 vs
	// LLM 输入视图分叉（嵌套时经 HistoryFor 喂兄弟 step 泄漏原文）；且 OnAgentStart
	// 报错路径不再留"从未跑过的 run"的 input 垃圾事件。
	// 「这是不是新任务」已在钩子之前判定（enterRunTask），下面那个 input 事件判断读的是同一个信号。
	if shouldEmitInputEvent(ctx, session.RunID) { // audit MED-2: 嵌套 Resume 重入不重加
		session.AddEvent(NewOutputEvent(g.Name(), input))
	}

	// **R30 audit C-H1 fix**: defer OnAgentEnd 注册必须在 OnAgentStart 成功**之后**
	// (跟其他 8 编排架构对称 — fef2ca5 PlanAgent Run fix 当时漏改 GraphAgent, 跟 R32
	// isAgentNil 漏 SetExecutor 同款 "修一个漏一个" 模式).
	// 否则 OnAgentStart 报错时 defer 仍 fire → OTel span Start 未发 End 单飞 → 配对断裂.
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	var finalRef *schema.Message
	defer func() {
		var msgs []*schema.Message
		if finalRef != nil {
			msgs = []*schema.Message{finalRef}
		}
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "GraphAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	final, err := g.runGraph(ctx, &cfg, session, input)
	if err != nil {
		if final != nil {
			finalRef = final
		}
		// **可 Resume 的退出都要结算输入**：Resume 入口拿不到原始 input（它被 inputWriter
		// 交给业务 channel 了，框架不知道是哪个），不在这儿记，这条会话就永远没有用户那
		// 句话。其余 7 编排不需要这一手——它们的补记写在图体内，Resume 会连着重跑。
		//
		// **判据是「框架对这个错的处置意图」不是错的类型**。原来只认 `IsInterrupt`，实测
		// 漏两档：软取消返 `ErrSoftCanceled`、`CondFailRerunFrom` 返的是**业务错**，两档
		// cp 都留 Pending 且之后真能 Resume 成功——漏了就落成「有答无问」。纯业务硬错不在
		// 此列（cp 标 Failed，`Resume` 直接返 `ErrNoPendingCheckpoint`，这一层压根不可
		// Resume），跟「报错的外部执行体叶子」一档，不记是站得住的。
		// 嵌套时（ownsHistory=false）不在这里结算：交给调起它的那道边界——编排的成员派发与
		// 嵌套调用执行器在「可 Resume 的退出」时都结算输入（settleHistoryOnPause），嵌套调用
		// 还按调用身份记下「输入已落过」，续跑完成、再次暂停时都不再记。这里曾经也结算一份
		// （先于边界那套落地），调用方每次重跑都现拼一条输入（新的消息 ID）时，它按消息 ID
		// 认不出上一次落过的那条：被调方问两次，输入记两份（实测）。嵌套位置上 Resume 一律
		// 被拒，「直接 Resume 子」只剩顶层，那里 ownsHistory 为真、走上面这一支。
		if compose_v2.LeavesResumableCheckpoint(err) && ownsHistory {
			settleHistoryTopLevel(ctx, g.Name(), input, nil)
		}
		// review-v6 H-05 fix: 业务错 fire OnError (非控制流)
		if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "graph_run_error", Message: err.Error()})
		}
		return collectEvents(session, startOffset), err
	}
	finalRef = final
	if final != nil {
		session.AddEvent(NewExitEvent(g.Name(), final))
	}
	// 输入 + 产出一起结算。判据自己决定记不记：图里的子 agent 已经记过的，输入这条会被
	// 范围回执挡掉、产出这条会被全局登记表挡掉（透传的跳过、加工过的照记）。
	if ownsHistory {
		settleHistoryTopLevel(ctx, g.Name(), input, final)
	}
	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。
func (g *GraphAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err 返业务
	defer recoverToErr(ctx, "GraphAgent.Resume", &retErr)
	// review-v6 H-06 fix: 与 Run 入口 (line 128-136) 对称的 nil 校验.
	// Resume 是公开 API 业务可独立调 (无 invariant 强制 Run 先于 Resume), 业务用
	// builder NewGraphAgent(...) 忘传 WithGraphOutputReader 然后直接 Resume 时,
	// 下游 compose_v2.Run(ctx, g.graph, ...) 远 stack nil deref panic 难定位.
	// inputWriter Resume 不需要 (cm.Restore 走 cp) 故不校验.
	//
	// **audit #5 F14 doc**: nil-graph check 在 OnAgentStart 之前 fire 是 **by design** —
	// init 期 fail-fast (业务忘 WithGraph) 不算业务运行错, 不应进 hook lifecycle.
	// 跟其他 8 编排"业务 nil config" fail-fast 位置一致 (e.g., loop_agent.go 检 body
	// nil, sequential_agent.go 检 steps 空) — hook 仅观测业务运行错.
	if g.graph == nil {
		return nil, fmt.Errorf("graph agent %q: resume requires graph (use NewGraphAgent with WithGraph)", g.Name())
	}
	if g.outputReader == nil {
		return nil, fmt.Errorf("graph agent %q: resume requires output reader (use WithGraphOutputReader)", g.Name())
	}

	// audit #5 阶段 1 补审 MED-2 fix: 抽 commonResumePrep helper
	// **R21 audit MED-1 fix**：GraphAgent 接受 ResumeActionReplan (透传给底层 graph 节点),
	// 不 fail-fast — 跟 BasicAgent / PlanAgent 同协议.
	// P0 observability fix: info + hooks 传 commonResumePrep 让 Abort 路径也 fire 完整 hook 对.
	hooks := g.hooks
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	// Sub-5 audit Round 2/3 fix: GraphAgent 包任意 compose_v2.Graph 为 Agent, 业务自定义
	// DAG 无统一 "round" 语义 (业务可能是 N 节点串行 / fanout / 循环等). info.Round 保持
	// 0 是 by-design — 业务想看节点执行数走 crew.node.executions{crew.node.id=...}.
	info := &hook.AgentInfo{Name: g.Name(), Type: "graph", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "graph agent", g.Name(), sessionID, opts, info, hooks, nil /* GraphAgent 无静态子列表，历史配置只能来自 ctx */)
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

	// 与 Run 入口对称建写入器。**漏了这半整段 Resume 的历史就没有**——图重跑到某一步时
	// 下游去 ctx 里取写入器取到 nil，直接 return（7 编排曾集体漏这半，见 `cd603e1`）。
	ownsHistory := historyWriterFrom(ctx) == nil
	ctx, stopHistory, err := startHistoryForOrchestration(ctx, nil, g.Name())
	if err != nil {
		return nil, err
	}
	defer stopHistory()
	ownsHistory = ownsHistory && historyWriterFrom(ctx) != nil
	if ownsHistory {
		ctx = openHistoryReceipt(ctx) // 与 Run 入口同款，见那处说明
	}

	startOffset := len(session.Events())

	cm := compose_v2.NewChannelManager()
	runOpts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sessionID),
	}
	runOpts = append(runOpts, g.breakpointOptions()...)
	// **R31 audit 视角 C HIGH-1 fix**：与 runGraph 路径对称——嵌套场景下 caller 持
	// GraphAgent 引用直接 .Resume 时 ctx 中含父推 graphNS——必须 forward 让
	// LoadNS(sid, ns) 拿到正确 ns 下子 cp，避免拿父 cp 报 channel not registered。
	// 对照 BasicAgent/PlanAgent/ParallelAgent/Sequential/Loop/Routing/Reflect/Handoff
	// .Resume 均已修同协议——GraphAgent.Resume 漏修。
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

	// 8-method 协议：Resume 入口 fire OnAgentStart 与 Run 协议对称（各 1 次）.
	// AgentIn.Messages 退化 nil (Resume 没新 input — 走 cp 重建).
	agentIn := &hook.AgentInput{}
	ctx = enterResumeTask(ctx, session)
	var hookErr error
	ctx, hookErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return hooks.OnAgentStart(c, info, agentIn)
	})
	if hookErr != nil {
		if _, isInt := compose_v2.IsInterrupt(hookErr); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: hookErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, hookErr)
	}

	// **R30 audit C-H1 fix**: defer OnAgentEnd 注册必须在 OnAgentStart 成功**之后**
	// (跟其他 8 编排架构对称 — fef2ca5 PlanAgent Run fix 当时漏改 GraphAgent
	// Resume 路径). 否则 OnAgentStart 报错时 defer 仍 fire → OTel span Start 未发 End 单飞.
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	var finalRef *schema.Message
	defer func() {
		var msgs []*schema.Message
		if finalRef != nil {
			msgs = []*schema.Message{finalRef}
		}
		// panic 兜底(recover 与 OnAgentEnd 调用同块,见 agent_helpers.go recoverPanicToErr):
		// 闭包内直接 recover() 设 retErr 让 OnAgentEnd 看到框架自身 wrapper panic.
		recoverPanicToErr(ctx, "GraphAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, hooks, info, &hook.AgentOutput{Messages: msgs}, retErr)
	}()

	// Resume 拿不到授予：放一份空的，遮住外层放的那份。
	ctx = withNodeGrants(ctx, memberGrants{})
	runErr := compose_v2.Run(ctx, g.graph, cm, runOpts...)
	final := g.outputReader(cm.Channels())
	finalRef = final
	if runErr != nil {
		// review-v6 H-05 fix: 业务错 fire OnError (非控制流)
		if _, isInt := compose_v2.IsInterrupt(runErr); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "graph_resume_error", Message: runErr.Error()})
		}
		return collectEvents(session, startOffset), runErr
	}
	if final != nil {
		session.AddEvent(NewExitEvent(g.Name(), final))
	}
	// 产出进历史。input 传 nil——Resume 没有新输入，原始那句在 Run 那次（含中断出口）
	// 就结算过了。
	if ownsHistory {
		settleHistoryTopLevel(ctx, g.Name(), nil, final)
	}
	return collectEvents(session, startOffset), nil
}

// runGraph 内部跑 graph — review-v6 M-20 fix: 删除 dead hooks 参数 (函数体内
// 用 extractOrchHooksOrCtx 取 OrchHooks, 业务级 hook.Hooks 由 caller defer 在 wrapper
// 层 fire, runGraph 内部不需要).
func (g *GraphAgent) runGraph(
	ctx context.Context,
	runCfg *runConfig,
	session *RunSession,
	input *schema.Message,
) (*schema.Message, error) {
	cm := compose_v2.NewChannelManager()
	cper := compose_v2.GetCheckpointer(ctx)

	// **不再自查「这次算不算续跑」**——同 basic_agent.go 那处，见那里的完整说明。
	// 一句话：这个判断需要 LoadNS，而 `compose_v2.Run` 进去本来就要 LoadNS；抄一份到
	// 这里，抄本和正本失配就是「两头不着的 fresh run」。**这处正是那个坑的第四次**：
	// 加 ctx 信号那一轮，basic_agent 改了、这里漏了，实测跑出子图节点拿空输入。
	// 现在改成无条件传 `WithInitialWrites`（只在新开路径 apply），判断收回引擎唯一一处。
	sid := ""
	if cper != nil {
		sid = session.RunID
	}

	// `WithInitialWrites` 只在新开路径 apply；「两条都 apply」那个语义已收进框架内部
	// （`withImportOverlay`，子图独家）。⚠️ 这里尤其不能靠「写 LastValue 所以幂等」来省判断
	// ——`inputWriter` 是**业务提供的**，业务完全可以往 Append channel 写。
	runOpts := []compose_v2.RunOption{compose_v2.WithInitialWrites(g.inputWriter(input)...)}
	runOpts = append(runOpts, g.breakpointOptions()...)
	if cper != nil {
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
	// **顶层 Run 恒不 auto-load**。判据不在这里重算——`Run` 入口的 `enterRunTask`
	// 已经声明过一次，这里只是把同一个信号转成引擎的显式 RunOption。
	// 嵌套保持 auto-load（父 Resume 靠它恢复子）。
	if compose_v2.IsFreshRunCtx(ctx) {
		runOpts = append(runOpts, compose_v2.WithFreshRun())
	}
	// 本 GraphAgent 收到的转交 / 路由授予（发给它自己的那几份）放给它的节点：节点函数拿不到 Run 的
	// opts，要把能力交给它调起的 agent 时经 RegrantFromNode 显式转授。不自动往下传——节点里请教的
	// agent 不该凭在这张图里跑就替上层做决定（见 routeGrant）。
	ctx = withNodeGrants(ctx, runCfg.grantsFor(g))
	runErr := compose_v2.Run(ctx, g.graph, cm, runOpts...)
	final := g.outputReader(cm.Channels())
	return final, runErr
}

var _ Agent = (*GraphAgent)(nil)
