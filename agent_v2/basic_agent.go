package agent_v2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/panicerr"
	"crew-go/model"
	specModel "crew-go/modules/model"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/store"
	"crew-go/tool"
)

// pushStoreCtx 是 BasicAgent.Run / Resume 共享的 store ctx 注入 helper——
// 让下游所有节点 / hook / tool 通过 store.FromContext(ctx) 取到 store 实例
// （RFC §5；PR 2 audit HIGH-1 fix：Run/Resume 协议必须对称）。
//
// **业务不挂 store（c.Store nil / typed nil）时 short circuit 不覆盖父 ctx**
// （PR 2 audit 视角 1 HIGH-1 fix）——嵌套 BasicAgent 场景业务直觉是"子未挂
// WithStore = 透传父 ctx 的 store"，若 unconditional push nil 会用 nil 覆盖
// 父 store key value，违反业务直觉。
//
// 配套 audit MED-1 修正：之前 commit `bc506d0` 误判 guard 是 micro-opt 无
// 功能价值并删除——实际 guard **真有功能价值**（防嵌套子 nil 覆盖父 store），
// 视角 1 HIGH-1 揭穿。本 fix 把判断挪到 helper 内部统一处理。
//
// 8 编排 wrapper 调子 BasicAgent.Run 时 ctx 可能已含父 store——子若挂了不同
// 的 WithStore 才覆盖；子未挂 (nil / typed nil) 时透传父。
//
// **为什么 8 编排不调 pushStoreCtx (by design, audit #5 F13 doc)**：
// Store 是 BasicAgent.config 字段, 其他 8 编排 (Sequential/Parallel/Loop/Routing/
// Reflect/Handoff/Plan/Graph) 不持 store config——业务把 BasicAgent 嵌入 8 编排
// 时, store 通过外层 ctx (业务调 `store.WithContext(ctx, st)`) 或父 BasicAgent
// 的 pushStoreCtx 透传到子 BasicAgent. 8 编排 wrapper 自身**不持 / 不 push**
// store 是协议简化——store 是"全局执行栈"的横切关注点 (跨 sub-agent 共享),
// 不是 sub-agent 编排维度的资源.
func pushStoreCtx(ctx context.Context, st store.Store) context.Context {
	if isFrameworkNil(st) {
		// 子未挂 WithStore——透传父 ctx 不覆盖
		return ctx
	}
	return store.WithContext(ctx, st)
}

// skillActivationMode 取 skill source 的 ActivationMode，缺省返 toolCall（V1 默认）。
func skillActivationMode(src *skill.SkillSource) skill.SkillActivationMode {
	if src == nil || src.Options == nil || src.Options.ActivationMode == "" {
		return skill.SkillActivationModeToolCall
	}
	return src.Options.ActivationMode
}

// schemaStreamAdapter 把 *schema.StreamReader[*schema.Message] 适配为
// specModel.MessageStreamReader interface（hook.OnModelEndStream 入参类型）。
//
// 必要性：
//   - schema.StreamReader.Close 不返 error；spec interface 要求 Close error
//   - schema.Message = common.Message alias，元素类型自动兼容
//
// 业务 hook 拿到本 adapter wrap 的 reader 可在 Recv 链路插入观测/转换；
// streamOrGenerate 用 hook 返回的 wrapped reader 继续消费聚合 final message。
type schemaStreamAdapter struct {
	sr *schema.StreamReader[*schema.Message]
}

func (a *schemaStreamAdapter) Recv(ctx context.Context) (*schema.Message, error) {
	return a.sr.Recv(ctx)
}

func (a *schemaStreamAdapter) Close() error {
	a.sr.Close()
	return nil
}

// streamReaderToSchema 反向适配：把 specModel.MessageStreamReader 包装为
// `*schema.StreamReader[*schema.Message]`——业务 hook 返的 wrapped reader 需要
// 走 streamOrGenerate 的 reader.Recv 循环消费。
//
// 实现：spawn 桥接 goroutine 把 hook reader 的 chunks pump 到内部 schema.StreamReader。
func streamReaderToSchema(ctx context.Context, hookReader specModel.MessageStreamReader) *schema.StreamReader[*schema.Message] {
	out, send := schema.NewStreamReader[*schema.Message](16)
	go func() {
		// **无条件收尾**：生产者无论怎么退出（EOF / 错误 / panic / `runtime.Goexit`），
		// `out` 都必须闭合，否则消费方 `Recv` 永久阻塞——Goexit 会跑 defer 但 recover
		// 返 nil，下面那道 SendError 一个字都不发，全靠这一道保证流有终点。
		// `CloseSend` 幂等（sendOnce），EOF 分支的显式关闭不冲突。
		// **注册在最前 ⟹ 跑在最后**（LIFO）：错误先进流（SendError）、流再闭合。
		defer schema.CloseSend(out)
		// **`concluded` 旗：流必须带着一个结论收尾，不许静默截断。**
		//
		// 光有上面那道无条件闭流只解了「消费方永久阻塞」；剩下的一半是**闭流本身没有
		// 信息量**——`io.EOF` 在「上游正常发完」和「生产者被 `runtime.Goexit` 掐断」两种
		// 情况下**一字不差**，于是半截 chunk 被聚合成 assistant 消息、当最终答复写进历史
		// 返给用户。
		//
		// 所以这一支给截断补一个错误，让消费方（`streamOrGenerate`）走错误路径而不是
		// 把半截当完整内容——**跟同函数的 panic 路径处置一致**（那条早就 SendError 了，
		// 只有 Goexit 这条在裸奔）。
		//
		// 判据：`concluded` 只在**结论已经投递**后由普通语句置位（EOF 闭流 / 错误已 Send）。
		// defer 里置位没用——Goexit 也跑 defer。
		//
		// ⚠️ **这个 defer 里「写结论」必须先判 panic，「释放资源」不用**——上面那道
		// `CloseSend` 无条件，这道 SendError 门控。三方一致的规律（eino 的 toStream 同序、
		// LangGraph 的 executor 注释明写「已经在抛异常了就别再抛一个」）。
		concluded := false
		// **audit³ A5 fix**: goroutine 内 panic (hookReader.Recv / send / Close 任一)
		// Go runtime 直接 fatal 全 process 崩——framework outer recover 不 cover
		// 跨 goroutine. defer recover 转 err 通过 schema.SendError 推给消费方,
		// 让 reader.Recv 收到 err 后 streamOrGenerate 走错误路径（抛上去由 modelFn 处理）.
		defer func() {
			if r := recover(); r != nil {
				pe := panicerr.New(r)
				schema.SendError(out, panicerr.WrapOpaque("streamReaderToSchema goroutine panic recovered", pe))
				panicerr.Log(ctx, slog.LevelError, "agent_v2: 流桥接 goroutine panic，已作为流错误交给消费方", pe)
				return
			}
			if !concluded {
				schema.SendError(out, errors.New("streamReaderToSchema: 上游流被截断——"+
					"生产者既没发完也没报错就消失了（runtime.Goexit，典型来源是 zap.WithFatalHook "+
					"配置下的 logger.Fatal）；已收到的 chunk 不完整，不能当作完整输出"))
			}
		}()
		// stream 关闭 error 无意义 (goroutine 退出清理), 显式忽略
		defer func() { _ = hookReader.Close() }()
		for {
			msg, err := hookReader.Recv(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					schema.CloseSend(out)
					concluded = true // 干净收尾：上游确实发完了
					return
				}
				schema.SendError(out, err)
				concluded = true // 结论已投递（业务错），别让上面那支再盖一层
				return
			}
			send(msg)
		}
	}()
	return out
}

// BasicAgent 是经典 ReAct 循环 agent——LLM 调用 → 解析 tool_call → 执行 tools → 回 LLM。
//
// 编译为 BasicAgentBuilder, 强制 4 节点 channel-based Graph + 循环回边
// (preamble + model + tools + postamble, ISSUE-74/75 后, agent_v2 wrapper 生产路径
// 始终注入 preamble/postamble; compose_v2 builder 层 WithPreamble/WithPostamble 可选).
//
// 4 层 context compaction 集成（详见 doc/v2/context-compaction.md）：
//   - 第 1 层 MicroCompact：modelFn 最早段清白名单 tool_result 内容（先于 AutoCompact）
//   - 第 2 层 AutoCompact：modelFn 入口（MicroCompact 之后）token 超阈值时 LLM 摘要
//   - 第 3 层 ReactiveCompact：LLM 调用 wrap 内 retry 拦截 prompt-too-long
//   - 第 4 层 StoreMemory：store.Search 在 Run 入口注入到初始 messages
//
// HITL：业务工具抛 InterruptError 时 compose_v2 graph_loop 自动 SaveNS cp，
// caller 通过 Resume(sessionID, opts...) 续跑。
type BasicAgent struct {
	config Config
}

// NewBasicAgent 创建 BasicAgent。
//
// **不传 WithName** 时 defaultConfig 自动给 `"agent_v2_" + UUID[:8]` unique 默认 name
// (防嵌套 / fanout cp NS 撞). 业务想自定义 name 显式传 WithName(...) override.
// 跟 PlanAgent 同协议 (UUID 默认), 其他 7 编排无默认 fail-fast (#7 F6).
func NewBasicAgent(opts ...Option) *BasicAgent {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	// 身份在选项之后铸：Option 是导出的 func(*Config)，整份覆盖 Config 的模板选项（`*c = tmpl`）会把先铸的身份冲成
	// 模板里的值，按它建出来的几个 agent 共用一份 approve_always 规则（审计实测：模板建的 b 拿到了 a 的「一直允许」）。
	cfg.approvalOwner = newUUID()
	validateOutputSchemaConfig(&cfg)
	warnAskUserOverride(&cfg)
	warnReadToolOutputOverride(&cfg)
	validateHistoryConfig(&cfg)
	validateBackgroundTasksConfig(&cfg)
	return &BasicAgent{config: cfg}
}

// historyCfgForOrchestration 让编排能提前建历史写入器（见 startHistoryForOrchestration）。
func (a *BasicAgent) historyCfgForOrchestration() *historyConfig { return a.config.History }

// historyCfgFor 是这次调用用的会话历史配置：运行入口用 [WithHistoryCtx] 声明了就以它为准，否则用自己的
// [WithHistory]——同编排（见 startHistoryForOrchestration）。只看自己的配置时，只在 ctx 上声明历史的调用一条
// 不写，两者都配时写进自己的那个，跟 WithHistoryCtx 的约定相反；DetachTask 开的独立任务里跑没配 WithHistory
// 的 BasicAgent 也一条不写。
func (a *BasicAgent) historyCfgFor(ctx context.Context) *historyConfig {
	if hc := historyCfgFromCtx(ctx); hc != nil {
		return hc
	}
	return a.config.History
}

// Name 返回 agent 名。
func (a *BasicAgent) Name() string { return a.config.Name }

// Description 返回 agent 职责描述。
func (a *BasicAgent) Description() string { return a.config.Description }

// Run 启动一次 ReAct 循环并返回本次产生的 events。
//
// 流程：
//  1. 构造初始 messages（system + StoreMemory items + sess.SelfHistoryCompacted + input）
//  2. 编译 compose_v2 graph
//  3. compose_v2.Run 驱动，注入 Checkpointer + sessionID + adaptHooks + initialWrites
//  4. Run 完成后从 channels 取 final message，emit ExitEvent 到 sess
//  5. 写 StoreMemory（如有 — postamble 内 store.Put）
//
// session 为 nil 时框架补一个：在一个正在执行的任务里时 RunID 就是那个任务，否则沿用 ctx 上的 sid、
// 都没有才新铸（见 sessionForCtx）。RunID 为空的字面量 session、嵌套位置上不属于所在任务的 session
// 入口直接拒（见 resolveRunSession）。
func (a *BasicAgent) Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic / NodeFunc
	// panic 转 err 返业务, 不让 Run goroutine crash 嵌套场景 (AgentAsTool 子 BasicAgent)
	// panic 直接冒泡父 graph_loop. 替代之前散落 SafeOnXxx helper.
	defer recoverToErr(ctx, "BasicAgent.Run", &retErr)

	cfg := applyRunOptions(opts)
	// 构造期必须有 base model——per-call WithRunModel 是"覆盖已有 model"而非"提供唯一
	// model"。允许"构造期无 model 只靠 per-call"会在 HITL Resume 崩：Resume 不重传
	// per-call override → resolveModel 拿到 nil base → llm.Stream nil deref（audit Finding 1）。
	if a.config.Model == nil {
		return nil, fmt.Errorf("agent %q: model is required", a.config.Name)
	}
	if input == nil {
		return nil, fmt.Errorf("agent %q: input message must not be nil", a.config.Name)
	}
	input = ownRunInput(input)
	if err := resolveRunSession(ctx, &session, a.config.Name); err != nil {
		return nil, err
	}
	ctx = WithSession(ctx, session)
	ctx = pushStoreCtx(ctx, a.config.Store)
	ctx = withOffloadDedup(ctx) // MicroCompact offload 的 per-Run 跨轮去重集合
	// per-call 覆盖（model / systemPrompt / maxRounds）push 进 ctx，让 buildComposeGraph
	// （maxRounds→WithMaxVisit）/ makeModelFn（model）/ buildInitialMessages（systemPrompt）
	// 运行时优先于构造期 a.config。**总是 push**（即使无覆盖）以覆盖父 agent 透传的
	// override，防 per-call 覆盖泄漏到嵌套子 agent（见 withRunOverrides godoc）。
	ctx = withRunOverrides(ctx, cfg.overrides())
	// 会话历史（opt-in）：起写入器 + 出口 drain。ctx 里已有写入器时（嵌套在另一个
	// 开了历史的 agent 下）沿用父的，不另起——同一场会话只该有一条流。
	ctx, stopHistory := startHistory(ctx, a.historyCfgFor(ctx), a.config.Name)
	warnUnnamedAgentInHistory(ctx, &a.config)
	defer stopHistory()
	startOffset := len(session.Events())

	// 8-method: OnAgentStart 整个调用 1 次, Run top-level fire (跟 OnAgentEnd 对称, 跟
	// PlanAgent / GraphAgent 对齐). 在 Run 函数 scope (非 modelNode 闭包) fire —
	// 业务 hook push 的 ctx 与 OnAgentEnd defer 共享同 local var, 业务 OTel pattern
	// `ctx, span := tracer.Start(ctx, info.Name); ...; trace.SpanFromContext(ctx).End()` 真生效.
	info := &hook.AgentInfo{Name: a.config.Name, Type: "base", Round: 0}
	var finalMessagesRef []*schema.Message
	agentIn := &hook.AgentInput{Messages: []*schema.Message{input}}
	// 后台任务只在顶层（不在另一个正在执行的任务里）生效，判在盖任务章之前（见 withBackgroundScope）
	_, nested := enclosingTask(ctx)
	ctx = withBackgroundScope(ctx, &a.config, !nested)
	// 「这是不是新任务」判在钩子之前：钩子里就可能发起调用（见 enterRunTask）
	ctx = enterRunTask(ctx, session)
	var startErr error
	ctx, startErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return a.config.Hooks.OnAgentStart(c, info, agentIn)
	})
	if startErr != nil {
		if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
			_ = a.config.Hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: startErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, startErr)
	}
	// 消费 hook 改写: 业务可改 agentIn.Messages 做 PII filter / 模板包装 (跟 PlanAgent 对齐)
	input = adoptHookInput(agentIn, input)

	// hook OnAgentEnd 兜底——所有退出路径触发 (含 panic recover 路径 — defer 顺序: 内层先跑).
	// audit² 改: hook 调用 panic 由本函数 outer recover defer (line 122)
	// 兜底转 retErr 返业务, 不让 Run goroutine crash.
	// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
	// 记 crew.interrupts 计数 + 记录 interrupt 时间戳到 sess 让 Resume 算 duration.
	defer func() {
		// panic 兜底: 本 defer 比顶部 recoverToErr 先跑,
		// 在闭包内直接 recover() 设 retErr 让 OnAgentEnd/observability 看到框架自身 panic
		// (buildComposeGraph/collectEvents 等 compose_v2.Run recover 之外的 wrapper panic);
		// 顶部 recoverToErr 后跑时 panic 已消费 → no-op. recover() 必须闭包内直接调(传 helper 实参).
		recoverPanicToErr(ctx, "BasicAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, a.config.Hooks, info, &hook.AgentOutput{Messages: finalMessagesRef}, retErr)
	}()

	// **ISSUE-74 修**：buildInitialMessages 不再在 Run 入口同步调用——已重构成
	// preambleFn 注入到 BasicAgentBuilder.WithPreamble，业务回调（store.Search /
	// HistoryCompactor / SkillBox.ActivateSkill）抛 InterruptError 时走 graph 内
	// SaveNS 协议自动存 cp，Resume 自然支持。runGraph 通过 PreambleInputWrites 把
	// user input 写入 preamble channel；preamble 节点拼装完整初始 messages 写到
	// basicMessagesCh，model 节点入口看 enriched 视图。
	//
	// finalMessagesRef 退化为 nil（preamble 中断时 graph 还没产出 messages）——
	// runGraph 后无论成败都从 channel Get 一次，给 OnAgentEnd hook 尽可能完整视图。
	finalMessagesRef = []*schema.Message{input}

	// **R29 audit HIGH-1 (regression of ISSUE-74) fix**：原 Run 入口 AddEvent(input)
	// 在 runGraph **之前**——但 ISSUE-74 后 buildInitialMessages 在 preamble 节点内
	// 跑（runGraph 之内），prior=session.SelfHistoryCompacted 取出时已含刚加的 input
	// → msgs = sys + mem + prior(含 input) + input → input 重复 2 次。
	//
	// 修法：AddEvent(input) 推迟到 buildInitialMessages 末尾（line 583 后），
	// 与 R14 audit 对 listingMsg 的处理思路一致——prior 取出之后再 AddEvent
	// 避免双重。preamble 抛 InterruptError 时 input 不进 sess.events 也合理
	// （input 没真消费 LLM）。
	finalMsgs, lastRound, err := a.runGraph(ctx, &cfg, session, input)
	// Sub-5 audit HIGH-1 fix: update info.Round 让 OnAgentEnd defer 拿到最后一轮真值.
	// 之前 info.Round 永远是 init 时 0 → 业务钩子在 OnAgentEnd 读到的轮次恒为 0.
	// info 指针 defer 内闭包捕获 — 这里 mutate 字段, defer 跑时读到新值.
	info.Round = lastRound
	// **软降落之后仍然报错**——见 [withFinalRound] 的 godoc。
	//
	// **能走到那一轮本身就是信号**——模型把 maxRounds 个 tool 轮全用完了还没收尾，是被
	// 关掉工具调用逼停的。模型自己提前答完的运行根本到不了这个轮次，err 照旧是 nil。
	//
	// 不这么做的话软降落会把 `MaxIterationsError` **吃掉**：模型产出文本 ⟹ 条件边判无
	// tool_call ⟹ 循环自然退出 ⟹ 从没撞 maxVisit。那就是「静默截断」——`maxRounds` 当
	// 保险丝用的调用方再也看不见熔断，比原来那个「拿到 error 但没有答案」更糟。
	// ⚠️ 用 resolveMaxRounds 不用 `cfg.maxRounds`——后者是 **per-call 覆盖值**，没传时是 0。
	// 判据是 `lastRound > maxRounds`：model 节点每轮访问一次，maxVisit = maxRounds+1，
	// 所以「跑满」意味着访问了 maxRounds+1 次。模型自己提前答完的运行到不了这个数。
	//
	// ⚠️ 两条路都要报，而且**文案不同**——曾经只判 `err == nil`，于是下面那条路上报的是裸引擎消息
	// （`node "model" visited 4 times (max 3)`），一个字没提软降落，排查方向指不出去（审计实测）。
	if mr := resolveMaxRounds(ctx, a.config.MaxRounds); mr > 0 && int(lastRound) > mr {
		var mie *compose_v2.MaxIterationsError
		switch {
		case err == nil:
			// 软降落奏效了：模型在那一轮产出了文本（答复在 events 里），只是预算确实耗尽了。
			err = fmt.Errorf("basic agent %q: 轮次预算耗尽，最后一轮关掉了工具调用（tool_choice=none；配了结构化输出时只留 final_output）逼模型收尾"+
				"（答复在 events 里，用 FinalMessage 取）: %w",
				a.config.Name, &compose_v2.MaxIterationsError{Iterations: int(lastRound)})
		case errors.As(err, &mie) && !isInterruptErr(err):
			// ⚠️ **`errors.As` 问的是「链上任何位置有没有 MaxIterationsError」，它不足以断言「是我自己的软降落轮
			// 模型硬调了工具」**。可达的反例（审计实测）：软降落轮的 tools 节点里业务 OnToolStart / OnToolEnd 钩子
			// 对被拦下的那一批照样 fire，钩子返错会让节点失败（见 basic_builder.go 的 `OnToolStart: %w`），而那个错
			// 里可以包着钩子自己调起的子 agent 的 MaxIterationsError——此刻 round 恰好是 mr+1，判据成立。
			// 实测报出 `Iterations=99`（钩子里别人的数字）挂在一句讲本 agent 预算的话下面。
			//
			// 所以正文**只陈述能确定的事**（预算跑满了几轮、这次 Run 以错误收尾、没交付答复），把「provider 不守
			// none」写成**常见原因之一**而不是结论，真正的错误紧跟在后面。曾经直接断言「模型仍硬调了工具」，那在
			// 上面那条路上是一句假陈述。
			//
			// **链上有中断的不碰**：a2a/server 与 agui 会走中断分支把问题呈现给用户请他作答，而同一个错误的头条
			// 说「预算耗尽、FinalMessage 可能为 nil」是自相矛盾的（审计实测业务钩子 errors.Join 两者的形态）。
			// **重铸一个自己的 MaxIterationsError 挂在外层**：errors.As 取链上第一个，于是调用方读到的
			// Iterations 是**本 agent 跑满的轮数**（= maxRounds+1），跟上面正文里那句「跑满 %d 轮」一致。
			// 不重铸的话取到的是引擎 maxVisit 填的 visits[id]（= maxRounds+2，被拒的那次访问），同一个
			// 诊断给两个数。引擎的原错误经 errors.Join 留在链上，诊断不丢。
			//
			// ⚠️ **正文里那句指路要点明「第一行是重铸的那条」**：`errors.Join` 的 Error() 按参数序用 \n 拼，
			// 而重铸的那条必须排**第一**（errors.As 取链上第一个，调序就取回引擎那个数、这次改动白做）。于是
			// 冒号之后紧跟的是「达到最大迭代：N」——对前半句的同义重复，引擎的原错误被挤到第二行。曾经写
			// 「真正的错误见下」，读的人顺着冒号读到的是一句废话（审计实测量过那一行）。
			err = fmt.Errorf("basic agent %q: 轮次预算耗尽（跑满 %d 轮），本次 Run 以错误收尾、没有交付答复"+
				"（FinalMessage 可能为 nil）。软降落轮（tool_choice=none）里模型硬调的工具不执行，模型仍硬调就会走到这里"+
				"——provider 或业务的 ChatModel 不强制执行 none 是常见原因之一；下一行重复本 agent 跑满的轮数"+
				"（errors.As 取的就是它），引擎的原错误在第二行: %w",
				a.config.Name, lastRound,
				errors.Join(&compose_v2.MaxIterationsError{Iterations: int(lastRound)}, err))
		}
	}
	if err != nil {
		// finalMessagesRef 让 OnAgentEnd 看到尽可能完整的 messages
		if finalMsgs != nil {
			finalMessagesRef = finalMsgs
		}
		return collectEvents(session, startOffset), err
	}
	finalMessagesRef = finalMsgs

	// **ISSUE-75 修**：commitFinal 已迁到 graph 内 postamble 节点（buildComposeGraph
	// WithPostamble 注入 a.postambleFn）——Run 入口同步调用 commitFinal 已删除。
	// 修复 commitFinal 抛 InterruptError 时 cp 已被 cleanupOnSuccess 删 → Resume 找
	// 不到 cp 的 HITL 链断 bug。

	return collectEvents(session, startOffset), nil
}

// Resume 续跑被 InterruptError 中断的 Run。从 ctx 取 Checkpointer 加载 cp。
func (a *BasicAgent) Resume(ctx context.Context, sessionID string, opts ...ResumeOption) (retEvents []*AgentEvent, retErr error) {
	// audit² 改: outer defer recover 兜底业务 hook panic 转 err
	// 返业务, 不让 Run goroutine crash 嵌套场景 panic 直接冒泡父 graph_loop.
	defer recoverToErr(ctx, "BasicAgent.Resume", &retErr)

	// **audit #5 阶段 1 补审 MED-2 fix**: 9 编排 Resume 共享 boilerplate 抽
	// commonResumePrep helper (~50 行/编排 → ~10 行/编排), 避免"协议变更四复证"
	// 漏改风险 (CLAUDE.md 教训). 详 agent_v2/resume_helpers.go.
	//
	// **P0 observability fix**: info + hooks 传 commonResumePrep 让 Abort 路径
	// 也 fire OnAgentStart + OnAgentEnd 让 observability 完整记录 resume 计数.
	info := &hook.AgentInfo{Name: a.config.Name, Type: "base", Round: 0}
	prep, newCtx, err := commonResumePrep(ctx, "basic agent", a.config.Name, sessionID, opts, info, a.config.Hooks, []Agent{a})
	if err != nil {
		return nil, err
	}
	if prep.AbortDone {
		return nil, nil
	}
	cper, rcfg, session := prep.Cper, prep.Rcfg, prep.Session
	ctx = newCtx
	// ResumeActionReplan：BasicAgent 自身无 plan 节点，但内嵌 sub-agent（如
	// PlanAgent 包成 tool）可能消费 user 文本主动 replan——所以不拒绝，按 Continue
	// 路径走（UserInputs map 通过 ctx ResumeContext 透传给 sub-agent 的 planner）。
	// caller 通过 `WithResumeUserText` 让下游 planner 用 `GetResumeUserText`
	// 取出 user 文本主动重新规划。
	ctx = pushStoreCtx(ctx, a.config.Store)
	ctx = withOffloadDedup(ctx) // MicroCompact offload 的 per-Run 跨轮去重集合 // PR 2 audit HIGH-1 fix: Resume 路径同款 push 保 HITL Resume 链路 store 协议不断
	// per-call 覆盖（model / systemPrompt / maxRounds）push 进 ctx——Resume 也支持 per-call
	// 重传（对齐 eino/langgraph：resume 是一次正常调用）。总是 push 防嵌套泄漏（同 Run 入口）。
	ctx = withRunOverrides(ctx, rcfg.overrides())
	// 会话历史（opt-in）：Resume 同款起写入器——续跑产生的轮次照样进历史（HITL 答完
	// 之后的对话跟答之前是同一场）。
	ctx, stopHistory := startHistory(ctx, a.historyCfgFor(ctx), a.config.Name)
	warnUnnamedAgentInHistory(ctx, &a.config)
	defer stopHistory()

	startOffset := len(session.Events())
	// **audit⁴ C HIGH-1 fix**: Resume 也 fire OnAgentStart 与 Run 协议对称
	// (跟 PlanAgent / GraphAgent / 6 编排 Resume 同款). HITL Resume 闭环: Run
	// 抛 Interrupt 时 Run defer OnAgentEnd; Resume 入口再 fire OnAgentStart +
	// Resume defer OnAgentEnd — 业务 OTel pattern 可观测 Run / Resume 两段独立 span.
	// AgentIn.Messages 退化 nil (Resume 没有新 input — 走 cp 重建), 业务 hook 若依赖
	// messages 可从 sess.Events 取 ResumeContext.UserInputs.
	agentInResume := &hook.AgentInput{}
	ctx = withBackgroundScope(ctx, &a.config, true) // Resume 只在顶层调（见 commonResumePrep）
	ctx = enterResumeTask(ctx, session)
	var startErr error
	ctx, startErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return a.config.Hooks.OnAgentStart(c, info, agentInResume)
	})
	if startErr != nil {
		if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
			_ = a.config.Hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: startErr.Error()})
		}
		return collectEvents(session, startOffset), WrapAgentStartErr(ctx, startErr)
	}
	var finalMessagesRef []*schema.Message
	defer func() {
		// audit² 改: hook panic 由 Resume outer recover (line 192 defer) 兜底
		// 时 recover 转 slog.Warn 不冒泡，避免子 BasicAgent panic 污染父 cp save 路径
		// P0 observability: push retErr 让 observability OnAgentEnd 识别 InterruptError 路径
		// panic 兜底: 本 defer 比顶部 recoverToErr 先跑,
		// 在闭包内直接 recover() 设 retErr 让 OnAgentEnd/observability 看到框架自身 panic
		// (buildComposeGraph/collectEvents 等 compose_v2.Run recover 之外的 wrapper panic);
		// 顶部 recoverToErr 后跑时 panic 已消费 → no-op. recover() 必须闭包内直接调(传 helper 实参).
		recoverPanicToErr(ctx, "BasicAgent.OnAgentEnd", recover(), &retErr)
		ctxOut := WithEndError(ctx, retErr)
		fireAgentEnd(ctxOut, a.config.Hooks, info, &hook.AgentOutput{Messages: finalMessagesRef}, retErr)
	}()

	cm := compose_v2.NewChannelManager()
	// **Resume 拿不到授予**：授予走 RunOption，而 ResumeOption 是另一套。
	// 直接 `basicAgent.Resume(...)` 时本层没有转交 / 路由能力——这跟直接 `Run` 而不经
	// HandoffAgent / RoutingAgent 是同一件事（都没人授予）。嵌在它们底下的续跑不走这条路：
	// 外层节点在 Resume 时同样重跑，仍以 Run 形态带着授予调下来。
	ctx = withNodeGrants(ctx, memberGrants{})
	g, dispatchToolkit, err := a.buildComposeGraph(ctx, memberGrants{})
	if err != nil {
		return nil, fmt.Errorf("basic agent resume: build graph: %w", err)
	}
	// 工具的 trace 类型（tool.Categorizer）按这次实际派发的那份工具表查——见 pushToolCategorizerCtx。
	ctx = pushToolCategorizerCtx(ctx, dispatchToolkit)

	// Resume 路径：auto-load 从 (sid, ns) 取 saved cp，cm.Restore 恢复 channels
	//
	// **R27a audit MED-2 fix**：嵌套场景下若 caller 直接对子 BasicAgent.Resume
	// (parentSID) 调用（anti-pattern），ctx 中可能含父推下来的 graphNS——必须 forward
	// 让 LoadNS(parentSID, childNS) 拿子 cp 而非父 cp，避免父 cp restore 到子 cm
	// 报 "channel %q in checkpoint not registered" 或静默错位。与 runGraph 路径
	// (line 320-322) 对称。godoc 仍建议业务只对顶层 Agent 调 Resume。
	runOpts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sessionID),
	}
	if ns := compose_v2.GetGraphNS(ctx); ns != "" {
		runOpts = append(runOpts, compose_v2.WithRunNS(ns))
	}
	// HITL 答案：caller 的 UserInputs map 是 map[string]*schema.Message。
	// compose_v2.WithResumeAnswers 签名一致直接透传，保留 ContentBlocks/Annotations
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
	// Phase 2 Sub-3: 透传 EmitCpEvent 让 framework cp 操作 fire event
	if a.config.EmitCpEvent {
		runOpts = append(runOpts, compose_v2.WithEmitCpEvent(true))
	}

	// 这次续跑取走的插话记一本账，续不了时还回收件箱（见 Inbox「取走之后」）
	ctx, ledger := withInterjectionLedger(ctx)
	runErr := compose_v2.Run(ctx, g, cm, runOpts...)
	settleInterjections(ctx, ledger, cper, runErr)
	finalMsgs, _ := compose_v2.Get(cm.Channels(), basicMessagesChan)
	finalMessagesRef = finalMsgs
	// Sub-5 audit HIGH-1 fix: Resume 路径同步 update info.Round 让 OnAgentEnd defer
	// 拿到最后一轮真值 (Resume 接续之前 Run 的 round counter, cp.Channels 已 restore).
	info.Round = GetBasicAgentRound(cm)

	// **软降落之后仍然报错——Resume 路径同款**（判据与理由见 Run 那处 :306 附近）。
	//
	// ⚠️ 漏了这一半就是把熔断从**整条 Resume 路径**上摘掉，而顶层是 BasicAgent 的 HITL
	// 产品（ask_user / 工具审批）走过一次中断之后，`Resume` 就是那个终结调用——从此
	// `maxRounds` 当保险丝用的调用方**再也看不见熔断**。实测：同一 fixture，纯 Run 返
	// `MaxIterationsError`、Run+Resume 返 `nil`，而两边的 LLM 调用次数一模一样。
	//
	// `basicRoundCh` 随 cp 走 ⟹ round 跨 Resume **接着数**，预算是会话总额不是 per-Run，
	// 所以判据在这条路上同样成立（实测 lastRound=4 > maxRounds=3）。
	if mr := resolveMaxRounds(ctx, a.config.MaxRounds); mr > 0 && int(info.Round) > mr {
		var mie *compose_v2.MaxIterationsError
		switch {
		case runErr == nil:
			runErr = fmt.Errorf("basic agent %q: 轮次预算耗尽，最后一轮关掉了工具调用（tool_choice=none；配了结构化输出时只留 final_output）逼模型收尾"+
				"（答复在 events 里，用 FinalMessage 取）: %w",
				a.config.Name, &compose_v2.MaxIterationsError{Iterations: int(info.Round)})
		case errors.As(runErr, &mie) && !isInterruptErr(runErr):
			// 判据与文案同 Run 那处（理由见那里）。续跑时撞的多是 model 节点开头「一个预算至多一个软降落轮」
			// 那道闸，顶层 Run 上撞的是引擎 maxVisit。
			// 重铸同 Run 那处（理由见那里）。
			runErr = fmt.Errorf("basic agent %q: 轮次预算耗尽（跑满 %d 轮），本次续跑以错误收尾、没有交付答复"+
				"（FinalMessage 可能为 nil）。软降落轮（tool_choice=none）里模型硬调的工具不执行，模型仍硬调就会走到这里"+
				"——provider 或业务的 ChatModel 不强制执行 none 是常见原因之一；下一行重复本 agent 跑满的轮数"+
				"（errors.As 取的就是它），引擎的原错误在第二行: %w",
				a.config.Name, info.Round,
				errors.Join(&compose_v2.MaxIterationsError{Iterations: int(info.Round)}, runErr))
		}
	}

	if runErr != nil {
		return collectEvents(session, startOffset), runErr
	}

	// **ISSUE-75 修**：commitFinal 已迁到 graph 内 postamble 节点——Resume 路径下框架
	// 自动 cm.Restore 让 postamble 重跑，业务自定义 Store.Put 读 ResumeContext
	// (UserInputs 通过 WithResumeAnswers 走 ctx 透传到 postamble 节点) 续跑——
	// R30 视角C HIGH-1 之前手动 inject ResumeContext 到 cmtCtx 的代码已不需要
	// （postamble 节点 ctx 由 graph_loop 自动注入完整 ResumeContext）。

	return collectEvents(session, startOffset), nil
}

// runGraph 编译 + 跑 graph（Run 入口路径，非 Resume）。
//
// 嵌套场景（AgentAsTool 把本 BasicAgent 包成 tool
// 让父 BasicAgent 调用）下，子 BasicAgent 的存档必须同时满足：
//  1. 落在父 sid 下——sid 取 session.RunID（任务的身份）：编排成员与并行分支拿 ForkLane 派生的、嵌套调用的被调方拿框架给的（RunID = 所在任务），RunID 都等于父 sid，所以子存档落在父 sid 下；业务在嵌套位置传了别的任务的 session，入口直接拒（见 resolveRunSession）
//  2. 落在父推下来的 graphNS 里（AgentAsTool.Call 经嵌套调用执行器 push 了由调用身份派生的子 ns）
//     ——子 cp 写到 (parentSID, childNS) 物理分离，避免与父 cp (parentSID, "") 互覆盖
//
// 缺任一个：
//   - sid 不对 → 子 cp 孤儿（独立 sid 链），父 Resume 找不到
//   - 缺 graphNS → 子 cp 与父 cp 写同一 (parentSID, "") 槽位互相覆盖（save 顺序：
//     sub defer 先 save → parent defer 后 save 把 sub 覆盖）→ 表面只见 1 cp
//
// ctx graphNS（已 push）原样 forward 给 compose_v2.Run。
// runGraph 编译 + 跑 graph（Run 入口路径，非 Resume）。
//
// **ISSUE-74 修**：参数从 `initial []*schema.Message` 改为 `input *schema.Message`
// ——initial messages 现由 graph 内 preamble 节点拼装。runGraph 把 user input 通过
// PreambleInputWrites 写入 basicPreambleInputCh，preamble 节点 Get → 调业务回调
// （store.Search / HistoryCompactor / Skill 预激活）→ Write 完整 messages 到
// basicMessagesCh，model 节点 Get 看 enriched 视图。
// runGraph 返 (finalMsgs, lastRound, err) — lastRound 是 ReAct 最后一轮 round 数
// (Sub-5 audit HIGH-1 fix: 让 caller Run 内 OnAgentEnd defer 之前 update info.Round
// 让业务 hook 拿到真实 round 不是 0).
func (a *BasicAgent) runGraph(ctx context.Context, runCfg *runConfig, session *RunSession, input *schema.Message) ([]*schema.Message, int32, error) {
	cm := compose_v2.NewChannelManager()
	// 转交 / 路由能力：只认「这份授予的受让方就是我本人」。业务壳把 opts 原样转发给内层时
	// 受让方仍是壳、收到的是内层 ⟹ 不匹配 ⟹ 内层连 transfer_to_agent / route_to 都没有。
	grants := runCfg.grantsFor(a)
	// 本图里的代码（工具函数、钩子）要把能力交给它调起的 agent 时经 RegrantFromNode 显式转授
	// ——放的是收到的那份，不看 opt-in：opt-in 只管框架给本 agent 注册的那件工具。
	ctx = withNodeGrants(ctx, grants)
	g, dispatchToolkit, err := a.buildComposeGraph(ctx, grants)
	if err != nil {
		return nil, 0, fmt.Errorf("agent %q: build graph: %w", a.config.Name, err)
	}
	// 工具的 trace 类型（tool.Categorizer）按这次实际派发的那份工具表查——见 pushToolCategorizerCtx。
	ctx = pushToolCategorizerCtx(ctx, dispatchToolkit)

	cper := compose_v2.GetCheckpointer(ctx)

	// **不再自查「这次算不算续跑」。**
	//
	// 以前这里要自己 `LoadNS` 一次、按 cp 状态判一遍，只为决定「传不传初始输入」——
	// 而 `compose_v2.Run` 进去本来就要 `LoadNS`。同一个决策被抄到另一个包里，抄本和正本
	// 失配时的症状是「两头不着的 fresh run」（那边从 StartNode 起跑、这边不注入输入，
	// preamble 直接 `PreambleInput nil` 失败）。**这个坑本仓踩过四次**，每次都是
	// 「同一份判据的另一个拷贝没跟着改」。
	//
	// 现在改成无条件传 `WithInitialWrites`——它**只在新开路径 apply**，续跑路径引擎自己
	// 不理它。判断收回引擎那唯一一处，这边不需要知道自己在哪条路上。
	//
	// 同一个文件里早有反证：`Resume` 不传播种，于是它一行自查、一次 LoadNS 都没有。
	sid := ""
	graphNS := ""
	if cper != nil {
		sid = session.RunID
		graphNS = compose_v2.GetGraphNS(ctx)
	}

	// **ISSUE-74 修**：写 PreambleInput 到 basicPreambleInputCh（不再写完整 messages
	// 到 basicMessagesCh）——preamble 节点 Get 后调业务 fn 拼装完整 messages。
	//
	// `WithInitialWrites` **只在新开路径 apply**——「两条路径都 apply」那个语义已收进
	// 框架内部（`withImportOverlay`，子图节点独家），不再是公开选项。
	runOpts := []compose_v2.RunOption{compose_v2.WithInitialWrites(
		BasicAgentPreambleInputWrites(&BasicAgentPreambleInput{
			UserInput: input,
		})...,
	)}
	if cper != nil {
		runOpts = append(runOpts, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
			runCfg.inputDigestOption())
		// 嵌套时父已 push graphNS（AgentAsTool.Call）→ forward 让子 cp 物理分离
		if graphNS != "" {
			runOpts = append(runOpts, compose_v2.WithRunNS(graphNS))
		}
	}
	if hooks := extractOrchHooksOrCtx(ctx, runCfg.orchHooks); hooks != nil {
		runOpts = append(runOpts, compose_v2.WithHooks(adaptHooks(hooks)))
	}
	// Phase 2 Sub-3: 透传 EmitCpEvent
	if a.config.EmitCpEvent {
		runOpts = append(runOpts, compose_v2.WithEmitCpEvent(true))
	}

	// **顶层 Run 恒不 auto-load**。
	//
	// 这里是本文件里**唯一**一处判「这次算不算续跑」——以前还有一处 skipInitialWrites
	// 自查跟它成对、必须手工保持一致，那对判据漂移过四次。现在播种改走
	// `WithInitialWrites`（引擎只在新开路径 apply），那处判断整个删掉了。
	// 嵌套（graphNS 非空）保持 auto-load：父 Resume 时子 agent 正是靠「父重跑 → 子的
	// Run 被调 → 子 auto-load 自己的 cp」恢复的，去掉它嵌套 HITL 全断
	// （实测 nested_smoke 12/12 → 3/12）。
	//
	// 判据不在这里重算——Run 入口的 `enterRunTask` 是全仓唯一一处（9 编排共用），它顺带
	// 丢弃 don't-ask-again 规则。这里只把同一个信号转成引擎的显式 RunOption。
	//
	// ⚠️ 读的是**信号**不是 `graphNS == ""`：嵌套时信号可能由顶层那次 Run 继承下来，
	// 那种情况下子层同样该跳过 auto-load（此刻它名下任何 Pending 存档都属于一个已被
	// 放弃的任务）。用 `graphNS` 判就漏掉这半。
	if compose_v2.IsFreshRunCtx(ctx) {
		runOpts = append(runOpts, compose_v2.WithFreshRun())
	}
	// 这次 Run 取走的插话记一本账，续不了时（没配存档、终局失败）还回收件箱（见 Inbox「取走之后」）
	ctx, ledger := withInterjectionLedger(ctx)
	runErr := compose_v2.Run(ctx, g, cm, runOpts...)
	settleInterjections(ctx, ledger, cper, runErr)
	finalMsgs, _ := compose_v2.Get(cm.Channels(), basicMessagesChan)
	lastRound := GetBasicAgentRound(cm)
	return finalMsgs, lastRound, runErr
}

// buildInitialMessages 拼接 system + StoreMemory items + sess.SelfHistoryCompacted + 当前 input。
func (a *BasicAgent) buildInitialMessages(ctx context.Context, session *RunSession, input *schema.Message) ([]*schema.Message, []*schema.Message, error) {
	var sysPreamble []*schema.Message
	var msgs []*schema.Message
	// PromptTemplate（如有）：preamble 渲染出「agent 是什么」的那一段（system + 可选 few-shot）。
	// 与下面 sysPrompt（SystemPrompt + skill hint / skill body）正交——模板消息在前、sysPrompt 在后。
	//
	// 它进 sysPreamble 不进 msgs：**它每轮可由配置重算，因此不该占着被压缩的名额，
	// 更不该被压缩层丢掉**。见本函数返回值的说明。
	if a.config.PromptTemplate != nil {
		var vars map[string]any
		if a.config.PromptTemplateVarsFn != nil {
			vars = a.config.PromptTemplateVarsFn(ctx, input)
		}
		rendered, err := a.config.PromptTemplate.Format(vars)
		if err != nil {
			return nil, nil, fmt.Errorf("prompt template: %w", err)
		}
		sysPreamble = append(sysPreamble, rendered...)
	}
	sysPrompt := resolveSystemPrompt(ctx, a.config.SystemPrompt) // per-call WithRunSystemPrompt 覆盖
	// Skill 集成：按 ActivationMode 分流：
	//   - toolCall（V1 默认）：注入 activate_skill 工具 + system prompt 加 hint，LLM 推理时自决
	//   - adaptive / pre_match：runtime 预激活 → skill body 直接注入 system prompt
	if a.config.Skill != nil && a.config.Skill.SkillBox != nil {
		mode := skillActivationMode(a.config.Skill)
		injected := false // 标记 adaptive 路径是否真注入了 skill body
		switch mode {
		case skill.SkillActivationModeAdaptive, skill.SkillActivationModePreMatch:
			// 预激活：基于 user input 选 skill，激活后把 body 拼到 sysPrompt
			// audit H1 fix: selectSkillAdaptive 返 (sel, err) —— err 必须 propagate
			// 给 BasicAgent.Run（HITL InterruptError / 用户 ctx.Cancel 不能 silent
			// 走"无 skill"路径继续 Run）
			isPreMatch := mode == skill.SkillActivationModePreMatch
			selection, selErr := selectSkillAdaptive(ctx, input.Content, a.config.Skill.SkillBox, a.config.SkillSelector, isPreMatch)
			if selErr != nil {
				return nil, nil, fmt.Errorf("adaptive skill selection: %w", selErr)
			}
			// R7 audit H2 fix: emit selection 给业务订阅（telemetry / dashboard）
			//
			// **R24 audit LOW-1 设计说明**：panic（含 InterruptError 包裹的 panic）一律
			// recover 不冒泡——hook 设计为纯观测，不参与控制流。业务想做 HITL gate 请改用
			// SkillBox.ActivateSkill / SkillSelector 抛 InterruptError 路径（已 propagate）。
			// 详见 SkillSelectionHook 类型注释。
			if a.config.SkillSelectionHook != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							panicerr.Log(ctx, slog.LevelWarn, "SkillSelectionHook panic recovered (intentional swallow; HITL via hook not supported—use SkillBox/SkillSelector InterruptError instead)",
								panicerr.New(r), "agent", a.config.Name)
						}
					}()
					a.config.SkillSelectionHook(ctx, selection)
				}()
			}
			if selection.Skill != nil {
				active, err := a.config.Skill.SkillBox.ActivateSkill(ctx, selection.Skill.Name)
				if err != nil {
					// R5 audit H1 fix: control flow err 必须 propagate（与 selectSkillAdaptive
					// 路径一致）。业务自定义 SkillBox.ActivateSkill 完全可能抛 InterruptError
					// （如"用户选哪个 skill 版本"）或 ctx.Err（用户取消），不能 silent。
					if _, isInt := compose_v2.IsInterrupt(err); isInt {
						return nil, nil, fmt.Errorf("adaptive skill activation: %w", err)
					}
					if ctx.Err() != nil {
						return nil, nil, fmt.Errorf("adaptive skill activation: %w", err)
					}
					slog.WarnContext(ctx, "adaptive skill activation failed, falling back to no skill body in sysPrompt",
						"skill", selection.Skill.Name, "strategy", selection.Strategy, "err", err)
				} else if active == nil || active.Prompt == "" {
					slog.WarnContext(ctx, "adaptive skill activation returned empty prompt, falling back to no skill body",
						"skill", selection.Skill.Name, "strategy", selection.Strategy)
				} else {
					// R7 audit H1 fix: 截断超长 SKILL.md body 防止打满 ctx window
					// （AutoCompact / ReactiveCompact 都跳过 sys，不会有任何防御层压缩）
					body := active.Prompt
					maxChars := a.config.SkillBodyMaxChars
					if maxChars <= 0 {
						maxChars = DefaultSkillBodyMaxChars
					}
					if len(body) > maxChars {
						body = truncateWithEllipsis(body, maxChars)
						slog.WarnContext(ctx, "adaptive skill body truncated to fit SkillBodyMaxChars",
							"skill", selection.Skill.Name,
							"original_chars", len(active.Prompt), "truncated_chars", len(body),
							"max", maxChars)
					}
					// R10 audit HIGH fix: maxChars 极小（<3）时 truncateWithEllipsis 返 ""——
					// 若仍走拼接路径，sysPrompt 末尾会留下 "\n\n" dangling delimiter +
					// silent 丢 skill 注入信号（hook 仍报 single_candidate / injected=true 但
					// LLM 拿不到 body）。空 body 时跳过拼接 + warn 告诉业务"配的 maxChars 太小"。
					if body == "" {
						slog.WarnContext(ctx, "adaptive skill body empty after truncation, skipping sysPrompt injection",
							"skill", selection.Skill.Name, "max", maxChars,
							"original_chars", len(active.Prompt))
					} else {
						if sysPrompt == "" {
							sysPrompt = body
						} else {
							sysPrompt = sysPrompt + "\n\n" + body
						}
						injected = true
					}
				}
			}
			// adaptive 模式 ActivateSkill 失败时不再回退注 toolCall hint——
			// adaptive / pre_match 的语义就是"runtime 预决定"，回退会破坏契约（且
			// LLM 无 activate_skill 工具可调）。日志已 warn 让业务排查 box 实现。
			_ = injected
		default:
			// toolCall（默认）：注入 hint 让 LLM 知道何时调 activate_skill 工具
			hint := a.config.SkillSystemPrompt
			if hint == "" {
				hint = "当你判断当前任务适合使用某个专项技能时，请先调用 activate_skill 工具获取该技能的详细指导，再执行任务。"
			}
			if sysPrompt == "" {
				sysPrompt = hint
			} else {
				sysPrompt = sysPrompt + "\n\n" + hint
			}
		}
	}
	// 审批生效时把当前审批方式写进前言：模型事先知道哪些调用会停下来等人、被拒了怎么办（见 approvalPrompt）
	if seg := approvalPrompt(&a.config); seg != "" {
		if sysPrompt == "" {
			sysPrompt = seg
		} else {
			sysPrompt = sysPrompt + "\n\n" + seg
		}
	}
	if sysPrompt != "" {
		sysPreamble = append(sysPreamble, schema.SystemMessage(sysPrompt))
	}

	// StoreMemory：业务通过 WithStoreMemory 配置长期记忆 Store 后端——preamble
	// 自动 store.Search 注入到 prompt 前部（RFC §6 helper）。
	//
	// **PR 3.3 核心改动**：从旧 Memory 接口 (a.config.Memory.Memory.Read) 路径切换
	// 到 store.Store 接口 (store.FromContext + Search)。Memory 接口已删（参 git log
	// `feat(agent_v2): PR 3.3 删 Memory 接口`）。
	if a.config.StoreMemory != nil {
		st := store.FromContext(ctx)
		if st == nil {
			// helper 隐式要求 WithStore 已挂——fail-fast 让业务明确感知配置错
			return nil, nil, fmt.Errorf("WithStoreMemory: %w", store.ErrStoreNotConfigured)
		}
		// 用最后一条 user input 作 query（mem0 等语义检索后端需 query；InMemoryStore
		// 忽略 query 仅按 namespace + filter 检索）
		var query string
		if input != nil {
			query = input.Content
		}
		// **round audit fix 1 (视角 3 A2)**：业务直接构 &StoreMemoryConfig{} 跳过
		// WithStoreMemory option 兜底时 SearchLimit=0。WithLimit(0) 传业务自定义
		// 后端可能解读 "0=零结果" 或 "0=无限" —— 仅 SearchLimit > 0 时才 append
		// option，让业务后端用自己的默认行为（InMemoryStore 内 cfg.limit <= 0
		// 兜底 10，业务后端按自己语义）。
		var opts []store.SearchOption
		if a.config.StoreMemory.SearchLimit > 0 {
			opts = append(opts, store.WithLimit(a.config.StoreMemory.SearchLimit))
		}
		if query != "" {
			opts = append(opts, store.WithQuery(query))
		}
		// Phase 2 Sub-1: store.search event fire — sink-gated (接了 observability 才发)
		searchStart := time.Now()
		items, oosDropped, err := a.searchMemories(ctx, st, opts, query)
		if hasEventSink(ctx) {
			searchAction := &StoreSearchAction{
				NS:         a.config.StoreMemoryNS.String(),
				DocsCount:  len(items),
				DurationMs: time.Since(searchStart).Milliseconds(),
				Ok:         err == nil,

				OutOfScopeDropped: oosDropped,
			}
			if err != nil {
				searchAction.Err = err.Error()
			}
			EmitStoreEvent(ctx, &StoreAction{Type: StoreOperationSearch, Search: searchAction})
		}
		if err != nil && compose_v2.MustPropagate(ctx, err) {
			// 控制流信号必须 propagate（业务 store 后端做 HITL gate 抛 Interrupt / ctx 取消）
			return nil, nil, err
		}
		if err != nil {
			// 业务持久化错（DB conn / quota）不中断 Run，只 warn。
			// **但已经查到的照常注入**——items 在部分失败时是非空的，丢掉等于让一次 DB
			// 抖动抹掉其余分片做完的工作。观测那边 event 已经带上 Ok=false + Err + 真实
			// DocsCount，「部分失败」在结构化通道上是说得出口的。
			slog.WarnContext(ctx, "agent_v2: store memory search failed",
				"agent", a.config.Name, "ns", a.config.StoreMemoryNS.String(), "err", err)
		}
		{
			// SearchItem → 注入的 system 消息。值是 store 往返契约下的 JSON 形状（见 store.Item）：框架自己写进来的是答复
			// 文本（postamble 的 `st.Put` 存 `last.Content`），走 `case string` 原样注入；业务塞进同一个 namespace 的别的
			// 形状（对象、数组、数字……）注入它的 JSON 文本，在每个后端上都一样。曾经 in-memory 原样存业务的
			// `*schema.Message`、这里按真消息注入，而持久后端读回的是 map、注入的是 JSON 文本：同一份记忆在两类后端上
			// 喂给模型的东西不一样，两边都 `err == nil`。store 改成按 JSON 形状存之后那条分支没有后端能走到，已删。
			cloned := make([]*schema.Message, 0, len(items))
			for _, it := range items {
				if it == nil {
					continue
				}
				var msg *schema.Message
				switch v := it.Value.(type) {
				case string:
					msg = schema.SystemMessage(v)
				default:
					// json.Encoder + SetEscapeHTML(false)：`<` `>` `&` 不被 \uXXXX 化（默认 json.Marshal 会，模型读到的
					// token 变多、可读性差）；Encode 末尾的换行去掉。编不成 JSON 时（不守往返契约的自研后端）退化 fmt.Sprint
					var buf bytes.Buffer
					enc := json.NewEncoder(&buf)
					enc.SetEscapeHTML(false)
					if err := enc.Encode(v); err == nil {
						msg = schema.SystemMessage(strings.TrimRight(buf.String(), "\n"))
					} else {
						msg = schema.SystemMessage(fmt.Sprintf("%v", v))
					}
				}
				msg.Annotations = map[string]any{StoreInjectedAnnotation: true}
				cloned = append(cloned, msg)
			}
			msgs = append(msgs, cloned...)
		}
	}
	// 跨 Run 历史作 prior。**handoff 历史视图**（doc/v2/handoff.md §9）：据 ctx mode 切换——
	// SelfOnly（默认/非 handoff）只看自己；Rewrite（handoff 默认）看前接改写第三人称；
	// Raw（opt-in）看前接保 role。
	//
	// **来源两档**（ISSUE-98）：
	//
	//   - 开了 WithHistory → 从**会话历史层**读，拿到的是逐字完整流：上一轮调过哪些
	//     工具、工具返回了什么，模型都看得见。
	//   - 没开 → 回落到 session events。events 只记首尾，于是模型看到的是
	//     「user 问 → assistant 说还有 3 件」——结论在、来源没了，它无从判断这个数字是
	//     刚查的还是编的，也就无从判断该不该重查。**这是有意的降级**，见 WithHistory godoc。
	//
	// **开了会话历史时只读最近一段**：第 0 层截断留的是最近 MaxHistoryPairs 对，更早的读回来也是扔
	// （见 history_read.go 的实测）。配了 HistoryCompactor 时留多少由它决定，照旧给它整个视图。
	mode := GetHandoffHistoryMode(ctx)
	need := 0
	if a.config.HistoryCompactor == nil && a.config.MaxHistoryPairs > 0 {
		need = a.config.MaxHistoryPairs * 2
	}
	prior, fromHistory := historyPrior(ctx, a.historyCfgFor(ctx), historyOwner(ctx, a.config.Name), mode, need)
	if !fromHistory {
		switch mode {
		case HandoffHistoryRaw:
			prior = session.HistoryForRaw(a.config.Name)
		case HandoffHistoryRewrite:
			prior = session.HistoryFor(a.config.Name)
		default: // HandoffHistorySelfOnly（含非 handoff 场景缺省）——只自己，零回归
			prior = session.SelfHistoryCompacted(a.config.Name)
		}
		// 调用方提供的前文排在最前：events 只有这个进程里发生的，更早的轮次在调用方手里（见 caller_history.go）
		if turns := callerHistory(ctx); len(turns) > 0 {
			prior = append(turns, prior...)
		}
	}

	// **留一份未削减的给去重判据用**。下面的截断/压缩是「这轮喂给模型多少」的有损投影，
	// 而 input 去重问的是「这条输入落进历史没有」——那是持久化状态。拿投影回答持久化
	// 问题会误判：带 nonce 的那条 user 消息一旦被削掉，去重就以为没记过、于是再记一份，
	// 而历史 append-only，这份重复永久留存，下一轮模型会看到用户把同一句话说了两遍。
	// 触发不需要业务写错代码——有损的 HistoryCompactor（那正是它被文档定位的用途）
	// 或默认开启的 MaxHistoryPairs 攒够量，都会削到它。
	//
	// 这一份是读回来的那一段（只读最近一段之后它也不是整场会话了）；比它更早、但属于本任务的记录由
	// claimSharedRootInput 按消息 ID 与任务章再查一遍（见 findHistory）。
	priorForDedup := prior

	// 第 0 层兜底：HistoryCompactor / MaxHistoryPairs 截断 prior（防多轮累积爆 ctx）。
	//
	// 优先级：
	//   - HistoryCompactor 函数 > MaxHistoryPairs 简单截断 > 不动
	//
	// 注意：这一层只动 prior（跨 Run 历史），不动当前 input + system + StoreMemory——
	// 后三者属于本轮必带，截不得。
	if a.config.HistoryCompactor != nil {
		keep := a.config.MaxHistoryPairs
		if keep <= 0 {
			keep = defaultMaxHistoryPairs // 默认 hint（业务显式 WithMaxHistoryPairs(0) 禁用简单截断但配了 HistoryCompactor 时仍用此兜底）
		}
		// Phase 2 Sub-2: history.compact event fire — sink-gated (接了 observability 才发)
		historyStart := time.Now()
		inputCount := len(prior)
		compacted, err := a.config.HistoryCompactor(ctx, prior, keep)
		if hasEventSink(ctx) {
			action := &HistoryCompactAction{
				InputMsgs:  inputCount,
				OutputMsgs: len(compacted),
				KeepHint:   keep,
				DurationMs: time.Since(historyStart).Milliseconds(),
				Ok:         err == nil,
			}
			if err != nil {
				action.Err = err.Error()
			}
			EmitHistoryCompactEvent(ctx, action)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("history compactor: %w", err)
		}
		prior = compacted
	} else if a.config.MaxHistoryPairs > 0 && len(prior) > a.config.MaxHistoryPairs*2 {
		// 简单截断——保留最后 MaxHistoryPairs 对（user/assistant），按 2x 估算。
		// **保护跨层 marker**（对齐 4 层压缩都尊重 SkillBodyAnnotation）：激活过的 skill body
		// 在对话流里，按条数截掉会让 LLM 突然看不到已激活 skill 的内容。
		prior = truncatePriorPreservingMarkers(prior, a.config.MaxHistoryPairs*2)
	}

	msgs = append(msgs, prior...)
	// 本轮 input 要不要再 append 一次，取决于 **prior 里有没有它**——这个问题的答案只跟
	// prior 从哪来有关，跟 handoff 视图无关：
	//
	//   - prior 来自**历史层**：恒不含。写入点就在下面几行（preamble 记 in.UserInput），
	//     而 prior 是在写之前读的——顺序决定的，不是巧合。所以必须 append。
	//   - prior 来自 **session events** 且 handoff 共享历史（Raw/Rewrite）：含。
	//     HandoffAgent 在 Run 入口就把 input 写进 session 了，HistoryForRaw / HistoryFor
	//     捞得到，再 append 就是 input dup（doc/v2/handoff.md §9）。
	//   - prior 来自 session events 且 SelfOnly（默认 / 非 handoff）：不含，要 append。
	//
	// 这里曾经只判 handoff 视图——那在 prior 只有 events 一个来源时是等价的，换源之后
	// 就成了「Handoff + WithHistory 时模型收不到本轮问题」：第一轮拿到空 prompt，第二轮
	// 拿到的是**上一轮**的问题，于是它对着旧问题给出一个看起来挺合理的答案，**错位一轮
	// 且全程无报错**。
	// **「prior 恒不含 input」这个不变量只在单 agent 内成立**：它靠的是「写入点在下面
	// 几行、prior 在写之前读」这个顺序。转交第 2 跳起就不成立了——前一跳的 agent 已经把
	// 同一条 input 写进**共享**历史，这一跳读 prior 时它就在里面，再 append 就是第二遍。
	//
	// 实测（Handoff(front→billing) + WithHistory + Rewrite）：billing 收到的 prompt 是
	// [user 我要退款][user For context: front said ...][user 我要退款]——用户的问题出现
	// 两遍，而 store 里只有 1 份。**错的不是存储是喂给模型的那份**，所以按 store 断言的
	// 测试全绿。
	//
	// 判据就照着上面那个问题问：**扫 prior 找这条输入的身份**。两个维度都靠 prior 自己
	// 带对：
	//
	//   - **流**：SelfOnly 下 `historyPrior` 已经按归属过滤过，兄弟分支 / 前一跳记的那份
	//     根本不在我的 prior 里 ⟹ 我照样 append。换成「这个 Run 里有人记过吗」就错了：
	//     那是全局的，兄弟一记我就被判「有了」，而我的 prior 里一个字都没有——实测
	//     Parallel 三分支里有 1~2 条拿到**空 prompt**，Handoff SelfOnly 第 2/3 跳全空。
	//   - **载体**：用身份不用指针。指针过一次 cp 就换对象，而 Resume 后 prior 是从历史
	//     反序列化出来的——按指针判在所有跨 cp 的路径上结构性失明（实测 Resume 后本轮
	//     问题在 prompt 里出现两遍）。
	//
	// ⚠️ **业务复用同一个消息对象二次提交时会被判成「已经有了」**。这是**契约不是 bug**：
	// 一个消息对象 = 一次出现，复用它就是声明「这是同一条」。要表达「新的一轮」就新建
	// 一条消息（`schema.UserMessage` 每次调用铸新身份）。这跟成熟框架的取法一致——它们
	// 同样按消息身份判，复用对象同样塌成一条。
	//
	// 不能简单去掉 `fromHistory` ——那会让第一跳的 prompt 空掉，正是上面那段注释在防的 bug。
	alreadyInPrior := fromHistory && priorHasRootInputID(prior, rootInputID(input))
	if (fromHistory || mode == HandoffHistorySelfOnly) && !alreadyInPrior {
		msgs = append(msgs, input)
	}

	// **R29 audit HIGH-1 (regression of ISSUE-74) fix**：input AddEvent 推迟到 prior
	// 取出之后——避免本轮 input 在 prior 里重复出现。同 R14 listing 推迟思路。
	// preamble 节点失败（InterruptError）时本行未跑，input 不进 sess.events——
	// 业务期望"input 真消费 LLM 才算审计"语义对称。
	// **handoff 共享历史模式**（Raw/Rewrite）：input 已由 HandoffAgent 写进 session（owner=
	// HandoffAgent），sub-agent 不重复 AddEvent——否则每 hop 写一份让 HistoryForRaw 捞到多份
	// input dup（doc/v2/handoff.md §9）。SelfOnly（默认/非 handoff）照常 AddEvent。
	if GetHandoffHistoryMode(ctx) == HandoffHistorySelfOnly {
		session.AddEvent(NewOutputEvent(a.config.Name, input))
	}

	// 会话历史层记原始 input——放这里而不是 preamble 节点，是因为判据要看 prior（就在上面
	// 几行读的）：
	//
	//   - **只记 user 角色**：编排喂给 sub-agent 的 input 不一定是「这一轮新发生的事」。
	//     LoopAgent 把上轮产出原样转手当下轮 input（那是 assistant 消息），按角色挡得住。
	//   - **跳过派生输入**（DerivedInputAnnotation）：Sequential 拼的「原始目标+上一步产出」
	//     成分全可重算，不记；Reflect/Plan 拼的含被摘除 LLM 的产出、不可重算，照记（编排
	//     在源头决定打不打标）。
	//   - **共享视图下只记一份**：HandoffAgent 每跳 input 恒为同一条原始输入，第一跳记的对
	//     后续所有跳可见。判「记过没」靠 input 自带的 nonce + 扫 prior（跨嵌套/跨进程都对，
	//     见 rootInputID）。SelfOnly 每跳各记自己名下的。
	//
	// 跟上面 events 侧的 AddEvent 是两条独立记录，判据不同：events 侧靠 HandoffAgent 兜底、
	// 只 SelfOnly 写；历史层没有那个兜底写入者，得自己按 nonce 去重。
	// **跟 prompt 侧同一个判据**（`alreadyInPrior`）：append 进 prompt 的才记。两侧各判各的
	// 会必然分叉——实测过两种方向：prompt 说「已经有了」而记录侧照记（历史里同一身份多份，
	// 下一轮 prior 里那句话出现两遍），以及反过来。
	if input.Role == schema.RoleUser && !isDerivedInput(input) && !alreadyInPrior {
		// 去重**只信 history**（fromHistory）。读失败回落 events 时 prior 是 events，
		// 而 events 里有 HandoffAgent 自己 AddEvent 的那份带 nonce input（它不写 history
		// store）——若也认它，会让每一跳都误判「已记过」，front 和 billing 谁都不记，input
		// 在降级路径彻底丢失（实测 0 份）。降级时宁可重复不丢失：回退到每跳记。
		//
		// 扫的是 priorForDedup（截断/压缩**之前**的那份）——见它的声明处。
		//
		// 判据是「读历史 + 抢认领」两段（见 claimSharedRootInput）：只读历史在顺序多跳
		// 成立、在并发扇出不成立——分支同时起跑时谁都还没写完，三个都判「没记过」于是
		// 都记（实测连跑八次记了 1/2/2/3/1/2/2/3 份，每次都不一样）。
		// 读侧那一半已经由 alreadyInPrior 做过（扫的是同一份 prior）。这里只剩**抢认领**——
		// 它管的是同一个 Run 内的并发：扇出分支同时起跑时谁都还没写完，只读历史三个都判
		// 「没记过」于是都记。认领在写入器的锁里做，那是这条流上唯一天然串行的点。
		shared := GetHandoffHistoryMode(ctx) != HandoffHistorySelfOnly
		if !shared || !fromHistory || claimSharedRootInput(ctx, priorForDedup, input) {
			recordHistory(ctx, historyOwner(ctx, a.config.Name), a.config.Name, []*schema.Message{input})
		}
	}

	// **HIGH-1 fix（重建历史悬空 tool_call → API 400）**：session 事件流 / store memory 只存
	// Exit assistant（tool_result 从不进去）；transfer_to_agent / route_to 等 return-direct
	// 控制流工具的 Exit 是带 tool_call 的 assistant → 复访 / 跨 Run 重建时成悬空 tool_call →
	// 真实 API 400（assistant with tool_calls 必须跟 tool messages）。在 msgs 最终汇合处剥离
	// （补占位配平；一个真结果都没有时保留 assistant + 文本记录、剥掉 tool_call），
	// 单点覆盖**所有**来源：prior 三种 handoff 模式 + 非 handoff 跨 Run +
	// StoreMemory 注入（audit 实证 store.Put 存过带 tc 的 Exit，Run 2 新 session 注入也中招，
	// prior 层清洗覆盖不到）。cross-agent transfer 已被 HistoryFor rewrite 成 user context，
	// 此为 owner 侧 + store 侧的统一兜底。in-Run 轮内配对走 channel 不经此函数，不受影响。
	msgs = dropDanglingToolCalls(msgs)
	// sysPreamble 单独洗一遍：它跟 msgs 是两条独立的流，合不到一起洗。业务模板里的
	// few-shot 若留了没有结果的 tool_call，同样是真实 API 400。
	sysPreamble = dropDanglingToolCalls(sysPreamble)
	return sysPreamble, msgs, nil
}

// truncatePriorPreservingMarkers 第 0 层简单截断——保留最后 limit 条 prior，但**保护**带
// SkillBodyAnnotation 的消息不被计数截断（对齐 4 层压缩：autocompact/microcompact/
// reactive_compact 都尊重这个 marker 一致保护关键内容）。
//
// **why**：skill body 是激活后注入对话流的内容，纯计数截断（prior[len-limit:]）会把早期
// 激活的 body 切掉，而 LLM 并不知道自己"曾经看过"——它只会表现得像没激活过那个 skill。
//
// **带 marker 的 tool_result 要连它的 assistant 一起抽**：prior 换源到会话历史层
// （ISSUE-98）之后，历史存的是逐字完整流，activate_skill 的 tool_result 就在里面——只抽
// 结果不抽声明它的 assistant 会造出孤儿，下面那段处理的正是这个形状。
//
// protected 不占截断预算。**注意它不再是「极小」**：每成功激活一次 skill 就会多进 2 条
// （assistant + 它的 result），随激活次数线性增长——第 0 层防 ctx 爆的效果会被稀释。
// 真出现「激活很多次 + 窗口很小」时靠后面几层压缩兜。
func truncatePriorPreservingMarkers(prior []*schema.Message, limit int) []*schema.Message {
	if len(prior) <= limit {
		return prior
	}
	// **带 marker 的 tool_result 要连它的 assistant 一起抽**——单抽一条 role=tool 拼到
	// 最前，就是把它挪到了声明它的 assistant 前面 = 制造孤儿，出口的 dropDanglingToolCalls
	// 会把它整条丢掉，于是这条「保护」反转成了删除，而且完全静默。
	//
	// 而 SkillBodyAnnotation 恰恰打在 activate_skill 的 **tool_result** 上——**两种模式都打**：
	// 非 inline 是全文 tool_result，inline 是 terse-ack tool_result（R19 audit HIGH-1 fix，
	// 见本文件 makeToolsFn 内 P2.D 分支）。换源到历史层之前它进不了 prior（events 不含
	// tool_result），所以这个形状是换源之后才可能出现的。
	//
	// 先扫一遍认亲：哪些带 marker 的 tool_result、以及声明它们的 assistant 在哪。
	declaredBy := make(map[string]int)
	keepIdx := make(map[int]bool)
	for i, m := range prior {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool {
			if !hasSkillMarker(m) {
				continue
			}
			if ai, ok := declaredBy[schema.GetToolResultID(m)]; ok {
				keepIdx[ai] = true // 声明者
				keepIdx[i] = true  // 结果本身
			}
			// 找不到声明者说明它本来就是孤儿，不特殊保护（出口会清掉）
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			declaredBy[tc.ID] = i
		}
	}

	protected := make([]*schema.Message, 0)
	rest := make([]*schema.Message, 0, len(prior))
	for i, m := range prior {
		// 按原下标顺序入 protected ⟹ assistant 恒在它的 tool_result 之前，配对不破。
		if keepIdx[i] {
			protected = append(protected, m)
			continue
		}
		if m != nil && m.Role != schema.RoleTool && isPriorProtected(m) {
			protected = append(protected, m)
			continue
		}
		rest = append(rest, m)
	}
	// protected 不占预算：保留全部 protected + 最后 limit 条 rest。
	//
	// **切点走配对感知**：按条数硬切会落进 assistant(tool_call) 和它的 tool_result 之间
	// ——assistant 被切走、tool_result 留在头部成孤儿。换源前 prior 里没有 tool_result，
	// 这个问题不存在；换源后它是默认配置（MaxHistoryPairs 默认 20 且默认开启）就会踩的。
	// AdjustIndexToPreserveAPIInvariants 把切点**向前**拉到安全位置（保 tool_use↔tool_result
	// 配对 + 同 RunID 块连续），索引单调减小 ⟹ 只会多留几条，不会多丢。
	if len(rest) > limit {
		start := compose_v2.AdjustIndexToPreserveAPIInvariants(rest, len(rest)-limit)
		rest = rest[start:]
	}
	out := make([]*schema.Message, 0, len(protected)+len(rest))
	out = append(out, protected...)
	out = append(out, rest...)
	return out
}

// isPriorProtected 报第 0 层截断要不要保下这条（非 tool 角色的）消息：skill 正文，以及 AutoCompact 的
// 摘要——后者只会经压缩边界的替代历史进 prior（见 compaction_boundary.go），它代表边界之前的全部对话，
// 按条数截掉它等于把那段对话整个丢了。
func isPriorProtected(m *schema.Message) bool {
	return hasSkillMarker(m) || isCompactSummary(m)
}

// hasSkillMarker 报这条消息是否带 skill body 跨层 marker——第 0 层截断据此保护它不被
// 计数截掉。
func hasSkillMarker(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	v, ok := m.Annotations[SkillBodyAnnotation].(bool)
	return ok && v
}

// unfinishedToolResultText 是补给「声明了却没拿到结果」的 tool_call 的占位内容。
//
// 写成模型能理解的一句话而不是空串：它要据此决定重试、换方案、还是当作这一步失败。
// 空占位只能让配对合法，传达不了「这次没成」。
const unfinishedToolResultText = "[未完成] 这次工具调用没有返回结果（被中断、取消，或结果已从历史中丢失）。如需该结果请重新调用。"

// ReturnDirectToolAnnotation 记录「这条 tool_result 是哪个 return-direct 工具产出的」（工具名），给业务
// 看是谁收的尾。
//
// 存在的理由很实际：`schema.ToolResultMessage` **不保存工具名**（tool_result 块里只有
// tool_call ID 与内容）。靠 tool_call ID 反查也能得到名字，但那需要扫消息列表，而消息列表**含 prior**
// ——那正是我们已经踩过的那类坑（在含历史的列表里找本轮的东西）。名字在产出那一刻就是已知的，记下来
// 比事后找回来可靠。
//
// ⚠️ **框架自己不拿它判身份**：名字是开放集，业务占得到。「这是不是框架结构化输出那件 final_output 的
// 调用」看 finalOutputAttemptAnnotation，「这是不是框架铸的转交 / 路由工具」看 [ReturnDirectMintedAnnotation]，
// 两者都由派发层按真实实例盖。按名字认过：没配结构化输出时业务那件同名工具成功了也被当成一次失败的
// 尝试（审计实测）。
const ReturnDirectToolAnnotation = "__return_direct_tool__"

// UnresolvedCallsRecordAnnotation 标记「这条 assistant 是框架合成的记账文本」——
// `describeUnresolvedCalls` 给零结果的工具轮补的那条。
//
// **为什么必须有这个 marker**：那条记录是「assistant + 零 tool_call」，而「倒扫消息列表
// 找最后一条无 tool_call 的 assistant」正是挑最终答复的朴素判据。本轮若以带 tool_call 的
// assistant 收尾（return-direct：transfer_to_agent / route_to / 业务声明
// ReturnDirectMetaKey），倒扫会跳过 tool_result、跳过那条带 tc 的 assistant，
// **命中这条记账文本**——而 return-direct 恰恰**就是**「以 tool_call 收尾」这个触发条件本身。
//
// ⚠️ 框架自己**已经不这么挑了**（最终产出改由决策点写进专用 channel，记账文本在 preamble
// 侧合成、进不了那条 channel，所以那两处的防护从「靠谓词」变成了「结构上不可能」）。
// 谓词仍然必要，是因为还有在扫**含 prior 的消息列表**的地方——`observability` 的
// agent_output event、`ToolFilter` 的 lastInput，以及**业务自己扫 messages 挑答复时**。
//
// 两个独立审计视角各自撞上它，失败面不同但同一根因：
//   - 它成为 `FinalMessage(events)` ⟹ 业务拿到的「答案」是一句内部记账，还会被
//     `commitFinal` 的 `st.Put` 写进长期记忆、被 11 个编排边界写进 append-only 历史；
//   - `RoutingAgent` 的 `extractRouteTarget` 拿到它 ⟹ 无 tool_call 无 annotation，
//     落到文本关键词兜底、整段记账文本不在 branchSet ⟹ 从第 2 个 Run 起静默走 fallback
//     （没配 fallback 时直接把记账文本返给调用方）。classifier 走 `WithoutHistoryCtx`、
//     prior 恒是 events 源，业务开不开 `WithHistory` 都躲不掉。
//
// 业务若自己扫 messages 挑最终答复，同样应跳过带此 marker 的消息。
const UnresolvedCallsRecordAnnotation = "__unresolved_calls_record__"

// unresolvedCallArgMaxChars 是写进文本记录的单个 tool_call 参数上限。
//
// 参数是模型自己产的、长度不设限（一次 patch 就能几十 KB）。这条记录的用途是让模型
// 记起「提过什么」，超过这个长度对该用途没有增益，却会挤占上下文——而这个分支恰恰
// 出现在历史被压缩 / 截断之后，正是上下文最紧张的时候。
const unresolvedCallArgMaxChars = 200

// describeUnresolvedCalls 把「声明了却在历史里找不到结果」的 tool_call 写成一句给模型
// 读的文本记录。
//
// # 措辞：只陈述事实，**一个字的指引都不加**——这是实测出来的，不是推理出来的
//
// 场景：转账提了审批，用户没批、换了话题，随后追问「刚才那个怎么样了？」。
// 「重新调 transfer_money」= 把用户放弃掉的转账真的执行了。真 qwen 实测（每档 N=6）：
//
//	纯事实（当前实现）                       0/96   ✅
//	「是否要重新发起，请依据用户的后续消息判断」   27/96  ❌（28%）
//	「…不要自行重试」                        0/96   ✅
//	（对照）根本没有这条记录                   96/96
//
// （6 轮 × N=16 × 2 个模型；单点 N=6 的观测在 0~2 之间跳，**别拿它当依据**。）
//
// **任何把「重新发起」摆上桌的句子都会招致重放**，哪怕它语法上是个疑问句、把决定权
// 让给了上下文——模型会把「刚才那个怎么样了」读成「用户在要求继续」。那一档的危害
// 比什么都不说还接近（3/6 vs 6/6）。
//
// 反方向的「不要自行重试」实测是安全的（0/6），但它是条无条件指令，会被喂给**职责就是
// 每轮调那个工具**的 agent。**纯事实两头都占**：0 重放，且不含任何指令。
//
// 另一条：**不判定执行与否**。调用点分不出「本就不会有结果」（return-direct 控制流
// 工具）和「那一轮没跑完」（中断 / 放弃 / 被截断），两种结论正好相反。连「无法判断它
// 是否执行过」也不说——对 return-direct 那半是假话（它一定执行了，正是它终止了那一轮）。
//
// ⚠️ **改这句话之前先跑 `go test -tags=integration ./agent_v2/ -run TestRecordWording`**。
// 它是这条平衡的尺子，而这个平衡读码定不了——上一版尾句就是靠推理选的，理由听着周全，
// 实测 28%（96 次采样）重放。
func describeUnresolvedCalls(tcs []schema.ToolCall) string {
	if len(tcs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("[历史记录] 此前发起过以下工具调用，但历史中没有它们的结果：")
	for i, tc := range tcs {
		if i > 0 {
			b.WriteString("；")
		}
		b.WriteString(tc.Function.Name)
		if args := strings.TrimSpace(tc.Function.Arguments); args != "" {
			b.WriteString("(")
			if len(args) > unresolvedCallArgMaxChars {
				// 按字节切会劈开多字节字符，写进 prompt 是乱码
				r := []rune(args)
				if len(r) > unresolvedCallArgMaxChars {
					args = string(r[:unresolvedCallArgMaxChars]) + "…"
				}
			}
			b.WriteString(args)
			b.WriteString(")")
		}
	}
	b.WriteString("。")
	return b.String()
}

// pairToolResults 建立 tool_call ↔ tool_result 的配对索引：每个 id → 它的结果**队列**，
// 按出现顺序。
//
// **判据是「结果排在声明之后」，不要求紧跟**。这一条是刻意的，别改严：会话历史是
// append-only 的，补出来的合成结果只能追加到流末尾、跟它的声明之间可能隔着任意多条
// 消息。用「必须紧跟」判的话，那条合成结果永远算不上应答 ⟹ 每次修复再补一条 ⟹
// **历史无限增长，而且完全静默**（每条都合法、模型也读得懂）。
//
// **为什么是队列不是单条**：id 会跨轮复用——业务 stub 和固定 id 的 mock 天然如此，
// 真实模型也不保证换。只记第一条的话，第二轮的声明认领不到自己的结果。
//
// 一条结果只有在**已经有一个还没配上结果的声明排在它前面**时才算数：排在所有声明
// 前面的是错位（不是应答），声明数用完之后多出来的也不算。
//
// ⚠️ **全仓已有 6 份独立的配对实现**（重建、截断守卫、切点调整、三层压缩各一）。
// 这是给「视图重建」和「历史修复」两处共用的那一份——**别再加第七份**：两套规则一旦
// 分叉，视图层会自愈（重建时多余的被丢），而历史层不会（append-only 删不掉）。
func pairToolResults(msgs []*schema.Message) map[string][]*schema.Message {
	resultsOf := make(map[string][]*schema.Message)
	declared := make(map[string]int)
	for _, m := range msgs {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool {
			id := schema.GetToolResultID(m)
			if declared[id] > len(resultsOf[id]) {
				resultsOf[id] = append(resultsOf[id], m)
			}
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			declared[tc.ID]++
		}
	}
	return resultsOf
}

// danglingToolCalls 返回这批消息里**没有配对结果**的 tool_call 声明，按出现顺序。
//
// 只读——不改 msgs。给「历史修复」用：拿到未配对的声明才知道要给谁补合成结果。
//
// # ⚠️ 硬前置：msgs 必须是**静止**的历史快照
//
// 有 tool_call **正在执行**时调用，会把在飞的那些全判成悬空。窗口是确定存在的、不是
// 竞态巧合：model 节点成功后就把 assistant(tool_calls) 写进历史，而 tools 节点**整
// 节点成功后**才写 results（见 `basic_builder.go` 那两处 `recordHistory` 的注释）——
// 整个 tools 执行期间历史里恒是「N 条声明 + 0 条结果」。
//
// 而并发通路是现成的：ParallelAgent 兄弟分支各自的 `settleHistorySlot` 跟另一分支的
// tools 窗口重叠；嵌套更容易撞（一批里有个 AgentAsTool，子返回时就 settle，此时兄弟
// tc 还在跑、本轮一条结果都没记）。误判的后果是同一个 id 两条结果，而历史 append-only
// **删不掉**。
//
// **`-race` 抓不到这个**——它是语义重复不是数据竞争。护栏只能是调用点纪律：只在
// 「上一批 tool 已定局」的写边界调（Run/Resume 入口、新 input 落笔之前），
// **禁止**在 `makeToolsFn` 内部、读路径、历史 writer 的后台 goroutine 里调。
//
// # 与 dropDanglingToolCalls 的一致性范围
//
// 两处共用 `pairToolResults`，所以**配对判据**（谁被应答了）逐字一致：本函数报悬空的
// 那些，正是 `dropDanglingToolCalls` 会补占位或写进文本记录的那些。
//
// （曾经不一致：零真结果那条 assistant 会被整条丢掉、一个占位都不补，而本函数照报悬空。
// 那是条独立缺陷，已修——见 `TestDropDangling_PureToolCallRoundKeepsTheRecord`。）
func danglingToolCalls(msgs []*schema.Message) []schema.ToolCall {
	resultsOf := pairToolResults(msgs)
	next := make(map[string]int)
	var out []schema.ToolCall
	for _, m := range msgs {
		if m == nil || m.Role == schema.RoleTool {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			if next[tc.ID] < len(resultsOf[tc.ID]) {
				next[tc.ID]++ // 这个声明有自己的结果
				continue
			}
			out = append(out, tc)
		}
	}
	return out
}

// dropDanglingToolCalls 把重建出来的历史消息流修成 API 合法的形状。
//
// **做法是按声明重建配对块**，不是逐条判断去留：遇到带 tool_call 的 assistant 就在它后面
// 立刻排出每个 tool_call 的结果——有真的用真的，没有的补占位（见 unfinishedToolResultText）。
// 所有 `role=tool` 消息都从原位移除，配上的由它的 assistant 紧跟输出。
//
// 于是出口恒满足：**每条 role=tool 都紧跟在声明它的 assistant 之后，按声明顺序排列，
// 一个不多一个不少**。
//
// 有一种情况不排配对块：assistant 的 tool_call **一个真结果都没配上**。那时不补占位——
// 占位文案必须断言「执行了没有」，而这里分不出成因（实测多数命中是 return-direct 控制流
// 工具，它们的结果本就不进 events、调用真的生效了）。改为保留 assistant、把「提过什么」
// 写成文本记录（见 describeUnresolvedCalls），tool_call 剥掉。**不会整条丢**——那样模型对
// 自己提过这个动作的记录会被静默抹掉。
//
// 配对是**顺序敏感**的：结果必须出现在声明它的 assistant **之后**才算数。只看「两边都
// 存在」不够——被搬到队首的 tool_result 跟它后面的 assistant 互相都能「找到对方」，但那个
// 顺序 API 照样拒。
//
// # 为什么两个方向都要修
//
// 悬空那半是 Handoff 复访踩出来的：events 只存 Exit assistant，`transfer_to_agent` /
// `route_to` 这类 return-direct 控制流工具的 Exit 是带 tool_call 的 assistant，跨 Run 重建
// 时就成了悬空 → 真实 API 400。
//
// 孤儿那半是**会话历史层上线后**才可能出现的（ISSUE-98）：events 源的 prior 里根本没有
// `role=tool`（这个函数原先的注释正是这么写的：「paired 恒空、本分支不可达」），而历史层
// 存的是逐字完整流、tool_result 就在里面。于是任何"按条数切"或"把某些消息挪到前面"的处理
// 都可能把一对拆开：
//
//   - 第 0 层 `truncatePriorPreservingMarkers` 保留末尾 N 条——切点落在 assistant 和它的
//     tool_result 之间，assistant 被切走、tool_result 留在头部成孤儿；
//   - 同一函数把带 marker 的消息抽出前置，而 `SkillBodyAnnotation` 恰恰打在 `activate_skill`
//     的 tool_result 上——一抽就搬到了声明它的 assistant 前面；
//   - Raw 视图下父的 tool_call 与 tool_result 之间会夹着子 agent 那一段。
//
// 这些都不是"少看几条历史"，而是**整个 Run 硬失败**：孤儿引发的是 400 而不是
// prompt-too-long，第 3 层 ReactiveCompact 的 retry 根本不进入，且历史只增不减、下一轮切点
// 更靠后 → 会话永久卡死。所以修在这个"msgs 最终汇合处"——无论上游怎么打散，出口保证合法。
func dropDanglingToolCalls(msgs []*schema.Message) []*schema.Message {
	// 一遍扫描建索引，一遍重建——**不做「两个 map 各判各的」**，那样两边对「谁被应答了」
	// 的多重性理解会分歧（同 id 复用、同 id 被两条 assistant 声明时各自给出不同答案）。
	//
	// 索引：每个 tool_call id → 它的结果**队列**，按出现顺序。
	//
	// **为什么是队列不是单条**：id 会跨轮复用——业务 stub 和固定 id 的 mock 天然如此，
	// 真实模型也不保证换。只记第一条的话，第二轮的声明认领不到自己的结果，于是那条
	// assistant「一个真结果都没有」，它那条真结果被当成无人认领的影子丢掉，整轮退化成
	// 一句「没有结果」的文本记录——静默、合法、无报错。
	//
	// 顺序敏感：一条结果只有在**已经有一个还没配上结果的声明排在它前面**时才算数。
	// 排在所有声明前面的是错位（不是应答），声明数用完之后多出来的也不算。
	resultsOf := pairToolResults(msgs)
	nextResult := make(map[string]int) // 该 id 下一个待认领结果的下标，避免一果多用

	out := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}

		if m.Role == schema.RoleTool {
			// 结果统一由它的 assistant 在下面「紧跟」输出；能走到这里的要么已被认领
			// （原位这条是重复的影子），要么压根没有声明者（孤儿）。两种都丢。
			continue
		}

		tcs := schema.GetToolCalls(m)
		if len(tcs) == 0 {
			out = append(out, m)
			continue
		}

		// 有 tool_call：**assistant 与它的全部结果必须连成一块**。
		//
		// 真实 API 要的不是「结果排在声明后面某处」而是「**紧跟**」——中间夹一条 user 或
		// assistant 就 400（实测：全配对但被隔开，报的是同一条 "must be followed by tool
		// messages responding to each tool_call_id"）。所以这里不判断「原位对不对」，而是
		// 直接**按声明重建这一块**：assistant 之后立刻排出每个 tool_call 的结果，缺的补占位。
		//
		// 这样一次解决三种坏形状：结果被截断切走、结果被 marker 提升隔开、结果散落在后面。
		// 也不再需要「至少有一个配对才保留整条」那种部分判据。
		block := make([]*schema.Message, 0, len(tcs)+1)
		block = append(block, m)
		anyReal := false
		for _, tc := range tcs {
			if i := nextResult[tc.ID]; i < len(resultsOf[tc.ID]) {
				nextResult[tc.ID] = i + 1
				block = append(block, resultsOf[tc.ID][i])
				anyReal = true
				continue
			}
			// 这个 id 的结果用完了（或压根没有）→ 补占位，保证这一块自洽
			block = append(block, schema.ToolResultMessage(tc.ID, unfinishedToolResultText))
		}

		if anyReal {
			out = append(out, block...)
			continue
		}
		// 一个真结果都没有。**不补占位、也不整条丢，改成把「提过什么」写成文本留下。**
		//
		// 为什么不补占位：占位文案必然要断言「执行了没有」，而这里**分不出成因**。实测
		// （给本分支装计数器跑全量测试）31 次命中里 22 次是 `transfer_to_agent`——
		// return-direct 控制流工具的结果**本来就从不进 events**，它真的执行了。对它们说
		// 「未完成、如需该结果请重新调用」既是撒谎，又会引出转交环。
		//
		// ⚠️ **那个分布只刻画 events 源**（没开 `WithHistory` 时的 prior 来源）。开了
		// history 之后 return-direct **不再命中本分支**——tools 节点成功后
		// `recordHistory(results)` 把结果写进历史，下一轮 prior 里配得上。而本机制要救的
		// 场景（审批挂起被放弃）**只在 history 路径上可达**：中断时 commitFinal 不跑 ⟹
		// 无 ExitEvent ⟹ events 源下那条 assistant 根本进不了 prior。所以两条路径的主力
		// 成分相反，接手的人别把「多数是 return-direct」当成一般性刻画。
		//
		// 静态也认不出 return-direct：`tool.ReturnDirectMetaKey` 是**运行时**
		// `Result.Metadata` 不是工具的静态声明——`final_output` 只在校验通过时置、
		// `route_to` 只在 target 非空时置，**同一个工具这次 return-direct 下次不是**。
		// 补占位还会牵动两处既有机制：重试预算按 assistant 上的 final_output tool_call 计数，
		// 跨 Run 重建的 `final_output` 有烧掉预算之嫌；合成 tool_result
		// 还要在 4 层压缩的多个 marker 短路点各加一次防护，漏一处即静默。
		//
		// 为什么不整条丢（这是原来的行为，是个缺陷）：那样模型对**自己提过这个动作**的
		// 记录被静默抹掉。审批挂起被用户放弃、转头说「先别转了」时，模型看到的历史里
		// 没有任何转账痕迹，于是它确信地答「我没有发起过转账」。
		//
		// 文本档同时解决两边：记录留下了，又不对结果下任何结论。有文本的也照样补记——
		// 「保文本、剥 tool_call」同样把动作记录丢了，只是丢得没那么显眼。
		text := strings.TrimSpace(m.Content)
		if note := describeUnresolvedCalls(tcs); note != "" {
			if text != "" {
				text += "\n\n" + note
			} else {
				text = note
			}
		}
		if text == "" {
			continue
		}
		am := schema.AssistantMessage(text)
		am.ReasoningContent = m.ReasoningContent
		// **annotations 必须拷贝不能共享**：下面要往里写 marker，而 m 可能是 store 注入的
		// 消息或 events 里的消息——往共享 map 里写会反向污染源。
		am.Annotations = make(map[string]any, len(m.Annotations)+1)
		for k, v := range m.Annotations {
			am.Annotations[k] = v
		}
		am.Annotations[UnresolvedCallsRecordAnnotation] = true
		out = append(out, am)
	}
	return out

}

// searchMemories 取本轮要注入的记忆，并**在框架侧按相关性排序**。
//
// # 问题不是「谁写的」，是「按什么排」
//
// `store.Search` 的跨 backend 契约是按 `(namespace, key)` 字典序返回。把 `SearchLimit`
// 直接下推给它，截出来的就是「key 首字母最小的 N 条」——**跟本轮问题的相关性毫无关系**。
//
// 这件事跟有几个写入者无关：单个 agent 写了 20 条、limit=5，同样是字典序前 5 条
// （实测：唯一相关的那条 key 排在末尾 ⟹ 一次都注入不进去）。多写入者只是让它**看起来**
// 像个公平问题——曾经按写入者分配过配额，那治的是症状：多写入者那半看着好了，单写入者
// 那半原样坏着，而且从此没人会再去查。
//
// # 三段
//
//  1. **后端自己按相关性排** → 原样透传（[store.RelevanceRanker] 自报）。向量库 / 语义
//     后端已经挑好了最相关的 N 条，框架再插手只会把它们换成更差的。
//  2. **后端不排** → 框架排。先取候选（见下），再按 [skill.KeywordScore] 打分。
//  3. **分数为 0 的丢掉**——宁可少注也不注不相关的。没有 query 时不注（那时没有任何
//     排序依据，字典序前 N 条是纯粹的噪声）。
//
// # 为什么取候选要按写入者扇出
//
// 框架侧排序需要**候选池**，而候选池不能被后端的字典序 limit 提前砍掉——直接
// `Search(base, limit)` 拿回来的已经是被字典序砍过的那批，在里面怎么排都救不回来。
// 按写入者子 ns 分头查、每个**超采**，才拿得到一个有代表性的池子。
//
// 单写入者时不扇出（省一次 ListNamespaces），直接对基准 ns 超采。
// searchMemories 取长期记忆并**保证每条都在 StoreMemoryNS 之下**。
//
// 归属过滤是 [store.Store] 的义务（`Search(prefix)` / `ListNamespaces(WithListPrefix)`
// 的契约写明了）。这里再校一遍是**纵深防御**——它挡的是「后端有 bug」，**不挡「后端有
// 恶意」**：Store 实现跟框架同进程同权限，能返越界内容的实现同样能在返回后继续改那些
// 对象、能读 ctx。**别在这道校验上建授权**。
//
// 为什么校在这一层而不是校 ListNamespaces 的返回值：注入 prompt 有四条路，其中三条
// （后端自报按相关性排 / 单写入者早退 / limit 非法兜底）根本不经过 ListNamespaces。
// 实测一个经 `Search` 返越界项的后端，在单写入者和多写入者两条路上**都能把内容送进
// prompt**——只校 ListNamespaces 建立不了任何可陈述的不变量，却会让人以为建立了。
// 校在这个收口上，一处覆盖全部四条。
func (a *BasicAgent) searchMemories(ctx context.Context, st store.Store, opts []store.SearchOption, query string) ([]*store.SearchItem, int, error) {
	got, err := a.searchMemoriesUnscoped(ctx, st, opts, query)
	base := a.config.StoreMemoryNS
	// **不能原地过滤**（`got[:0]`）——`got` 有两条路是后端**原样递出来的那一份**
	// （limit 非法兜底 / 后端自报按相关性排），原地写等于改别人的内存。实测一个留着自己
	// 那份 slice 的后端，两次 Run 之后它的数据从 `other/r, w0/a, w1/b` 变成
	// `w0/a, w1/b, w1/b`（越界项被覆盖、末项重复），而第二次 Run 的 dropped 报 0——
	// **观测报干净、数据已经错了**。并发调用同一后端时 `-race` 直接报 DATA RACE。
	kept := make([]*store.SearchItem, 0, len(got))
	dropped := 0
	for _, it := range got {
		if it == nil {
			continue
		}
		if !base.IsPrefixOf(it.Namespace) {
			dropped++
			continue
		}
		kept = append(kept, it)
	}
	if dropped > 0 {
		// **出声**——静默丢弃会让观测撒谎（同函数 listErr 那条修的就是这个）。
		slog.WarnContext(ctx, "agent_v2: 长期记忆后端返回了不在本 agent namespace 之下的条目，已丢弃",
			"agent", a.config.Name, "ns", base.String(), "dropped", dropped)
	}
	return kept, dropped, err
}

// searchMemoriesUnscoped 是 [BasicAgent.searchMemories] 的取数与排序部分，**不做归属过滤**。
//
// 拆出来只为让归属过滤有唯一收口——名字里的 Unscoped 是**警告**：它的返回值可能含不在
// StoreMemoryNS 之下的条目（后端有 bug 时）。**除了 searchMemories 没有第二个调用方**，
// 加调用方前先想清楚谁来过滤。
//
// ⚠️ **两条路把后端原样递出来的 slice 返出去**（limit 非法兜底 / 后端自报按相关性排）
// ——调用方**不得原地改写它**（`got[:0]` 会写坏后端自己那份，实测两次 Run 之后它的数据
// 就错了，且第二次的 dropped 报 0）。其余路径返的是本函数 `make` 的或 nil。
func (a *BasicAgent) searchMemoriesUnscoped(ctx context.Context, st store.Store, opts []store.SearchOption, query string) ([]*store.SearchItem, error) {
	base := a.config.StoreMemoryNS
	limit := a.config.StoreMemory.SearchLimit
	if limit <= 0 {
		// 不可达（WithStoreMemory 把 <=0 归一成 5），留作业务绕过 option 直构 Config 的防御。
		return searchMemoryShard(ctx, st, base, opts...)
	}

	// ① 后端自报按相关性排 → 别插手。
	if r, ok := st.(store.RelevanceRanker); ok && r.RanksByRelevance() {
		return searchMemoryShard(ctx, st, base, opts...)
	}

	// 没有 query 就没有排序依据——注字典序前 N 条纯属噪声，不如不注。
	if query == "" {
		return nil, nil
	}

	// ② 取候选：超采，且不让后端的字典序 limit 提前砍掉候选池。
	cands, err := a.gatherMemoryCandidates(ctx, st, base, opts, limit)
	if err != nil && compose_v2.MustPropagate(ctx, err) {
		// 控制流信号（业务在 Store 里做 HITL gate / ctx 取消）→ 什么都别带，就是要停。
		return nil, err
	}
	// 普通后端错**不丢已经查到的**：一个分片挂了就整轮零注入，等于让一次 DB 抖动抹掉
	// 其余分片已经做完的工作。err 照样往上返——调用方据它 fire 观测 event
	// （Ok=false + Err + 真实 DocsCount），那是「部分失败」诚实的表达。

	// ③ 框架侧打分排序。形状跟 tool_search.go 的 defaultToolRetriever 一致——同一件事
	// 在两处应当同构。
	type scored struct {
		it    *store.SearchItem
		score float64
	}
	hits := make([]scored, 0, len(cands))
	for _, it := range cands {
		text := memoryItemText(it.Value)
		if text == "" {
			continue // 抽不出文本 = 没法打分，不是「分 0」
		}
		if sc := skill.KeywordScore(query, text); sc > 0 {
			hits = append(hits, scored{it, sc})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		// 同分才回落字典序——它只做 tie-break，不做主序。
		if hits[i].it.Namespace.String() != hits[j].it.Namespace.String() {
			return hits[i].it.Namespace.String() < hits[j].it.Namespace.String()
		}
		return hits[i].it.Key < hits[j].it.Key
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	out := make([]*store.SearchItem, 0, len(hits))
	for _, h := range hits {
		h.it.Score = h.score
		out = append(out, h.it)
	}
	// err 可能非 nil（某个分片挂了 / 列写入者降级过）——**带着结果一起返**。调用方据它
	// fire 观测 event，同时把已经查到的注进去。
	return out, err
}

// maxMemoryWriters 是一次取候选时最多考虑几个写入者。给 ListNamespaces 一个显式上限，
// 免得覆盖面取决于各家后端的默认截断值。超过这个数的写入者本轮不进候选池——那种规模下
// 共享一个基准 ns 本身就该重新分域了。
const maxMemoryWriters = 512

// memoryCandidateFactor 是候选池相对注入配额的超采倍数。
//
// # 这是降级方案，它的局限是可量化的
//
// 框架侧打分只能在**拿到的候选**里挑。后端按时新序返回，所以一条**又老又相关**的记忆
// 在噪声足够多时会被挤出候选池——这是超采改变不了的，它只把边界往后推。
//
// 实测（`SearchLimit=5`）：倍数 3 时，最旧的那条相关记忆在 **14 条**噪声之内还捞得到，
// 超过就丢。那个数字低到会让业务以为记忆功能坏了，所以提到 20（≈100 条候选）。
//
// # 真正的解法是让后端按相关性返回
//
// 接向量库 / mem0 / 配了 [store.IndexConfig] 的 InMemoryStore——它们自报
// [store.RelevanceRanker]，框架就完全不插手，也就没有候选池这回事。**记忆量大的生产
// 部署应当走那条路**；这里这套是给「没有语义检索能力的后端」兜底的。
const memoryCandidateFactor = 20

// gatherMemoryCandidates 取候选池：按写入者分片各查一遍，合并去重后交给上层打分。
//
// # 失败怎么处置
//
// 分两档，**先分类再套策略**——这个顺序是承重的，不是风格问题：
//
//   - **控制流信号**（业务在自己的 Store 实现里抛 InterruptError 做人在回路 gate、
//     或 ctx 被取消）→ 冒出去。它要的就是「停下来」，而降级跑完正好是它要阻止的事。
//   - **普通后端错**（DB 连不上 / 配额 / 列举超时）→ 尽力而为：这个分片跳过，别的
//     照查。候选少一点只影响排序质量，不该让整轮注入失败。
//
// 判据用 [compose_v2.MustPropagate] 这份单一真相源，不在这儿散写 errors.Is/As。
//
// # 第二个返回值
//
// 是**按 [compose_v2.PickFanoutError] 判据挑出来的那一个**（终局失败 > 坏存档 > 档没写成 > 中断 > 第一个业务错），
// 跟图引擎超步部分失败的仲裁共用同一份判据。**不是「第一个遇到的错」**——先到的普通
// 业务错会盖掉后到的中断，那时 IsInterrupt 为 false，上层按「这轮查不到，warn 一下
// 继续」处理，而业务那道 gate 永远等不到人来答。
//
// 错误必须冒到调用方：上层要据它 fire 观测 event 并决定这轮跳不跳过注入。**吞掉它会
// 让 event 报 Ok=true，那是观测在撒谎**，比少注几条记忆严重得多。
//
// ⚠️ 这里的分档是在补一个**接口表达力的缺口**：`store.Store` 的签名是
// `([]*SearchItem, error)`，业务没有位置说「我这个错是可降级的」，于是框架只能按错误
// 种类猜。猜得对（控制流信号确实一律不可降级），但业务想说「我这个配额错也别降级」
// 时没有表达方式。不改签名是有意的——权衡见
// doc/description/three-way-comparison/error-handling.md。
//
// # ⚠️ 逐写入者扇出**不是授权点**，别当它是
//
// 分片列表是 `[base, w1, w2, …]`——**base 排第一，而 `Search(base)` 是前缀递归的**。
// 所以在任何写入者分片被查到之前，全部后代的数据就已经进候选池了。逐写入者再查一遍，
// 在**数据可见性**上完全冗余；它新增的唯一东西是「对每个写入者 ns 发过一次 Search」
// 这个**可观测事件**。
//
// 后果（三条都实测过）：
//   - 业务守卫返**普通 error**（「拒绝这个写入者」）时，被拒写入者的内容**照样进 prompt**，
//     且 `Run` 返 `err=nil`。写入者数 1/2/5/40 全中。
//   - 业务守卫抛**控制流信号**（InterruptError）时内容确实不进——但那是因为整轮被作废了
//     （`return nil, serr`），不是因为扇出提供了隔离。它是 all-or-nothing 的中断触发器。
//   - **单写入者时守卫根本不被调用**（`len(subs) < 2` 早退，只查 base）。
//
// 所以业务照着「拒绝这个写入者、其余照读」实现，拿到的是**静默泄漏 + err=nil**。要真正
// 的 per-writer 隔离，得让每个写入者用**各自的 base**（各配各的 `WithStoreMemory`），
// 而不是共享一个 base 靠子 ns 分。
//
// 这一条也是「为什么这里没有分片数上限」的真正理由：加过一个（超过 N 片退回单次 base
// 查询），撤了——但**不是**因为它「制造了绕过」（数据经 base 那片本来就全读），而是因为
// 它只削掉 gate 调用、换不来任何隔离，纯亏。
//
// # 扇出跑几遍
//
// preamble 只跑首轮的前提是它**成功过**：业务在 Store 里做的 gate 在 preamble 里抛中断时，节点没成功，Resume 重跑
// 整个扇出（base + 每个写入者各一次 Search）。K 道 gate 就是 K+1 遍全量扇出——实测 300 个写入者 + 1 道 gate：Run
// 时 301 次 Search，Resume 时又 301 次。
func (a *BasicAgent) gatherMemoryCandidates(ctx context.Context, st store.Store,
	base store.Namespace, opts []store.SearchOption, limit int,
) ([]*store.SearchItem, error) {
	want := limit * memoryCandidateFactor
	withLimit := func(n int) []store.SearchOption {
		o := make([]store.SearchOption, 0, len(opts)+1)
		o = append(o, opts...)
		return append(o, store.WithLimit(n))
	}

	var listErr error
	nss, err := st.ListNamespaces(ctx,
		store.WithListPrefix(base),
		store.WithListMaxDepth(len(base)+1),
		store.WithListLimit(maxMemoryWriters))
	if err != nil {
		// **不能一律吞**。这里曾是 `if err == nil { ... }`——列不出写入者就当「只有一个」，
		// 静默降级去查基准 ns。而 store.Store 是业务可自实现的接口，框架明确支持业务在
		// 自己的实现里抛 InterruptError 做人在回路 gate：吞掉 = 中断整条消失，没人被问、
		// Run 照常返成功（实测 err 全程 nil）。
		//
		// 分两档，判据用 compose_v2 那份单一真相源，不在这儿散写 errors.Is/As：
		//   - 控制流信号（中断 / ctx 取消）→ 冒出去。它要的就是「停下来」，降级跑完
		//     正好是它要阻止的事。
		//   - 普通后端错（列举超时 / 权限）→ 保持降级：还能查基准 ns，注入变少总好过
		//     整轮不注入（上层对普通错的处置就是 warn + 跳过注入）。
		if compose_v2.MustPropagate(ctx, err) {
			return nil, err
		}
		slog.WarnContext(ctx, "agent_v2: 列举记忆写入者失败，降级为只查基准 namespace",
			"agent", a.config.Name, "ns", base.String(), "err", err)
		listErr = err // 仍要报上去，理由见下面 shardErrs 的注释
	}
	subs := make([]store.Namespace, 0, len(nss)+1)
	for _, ns := range nss {
		if len(ns) == len(base)+1 {
			subs = append(subs, ns)
		}
	}
	// 单写入者（或列不出来）→ 直接对基准 ns 超采，省一次扇出。
	if len(subs) < 2 {
		got, serr := searchMemoryShard(ctx, st, base, withLimit(want)...)
		// **listErr 必须在这条路上也报出去。** 它曾经只在下面的扇出路径进 shardErrs，而
		// `ListNamespaces` 按 Go 常规失败时返 `nil, err` ⟹ subs 空 ⟹ 走的正是这条早退 ⟹
		// 那段「降级也必须报上去」的代码**在它要救的形态上是死的**。实测：列写入者失败时
		// 观测 event 报 `Ok=true`、`Err=""`，而覆盖面已经从全部写入者塌成基准 ns 一个——
		// 本函数 godoc 自己把这叫「观测在撒谎」。
		return got, compose_v2.PickFanoutError([]error{listErr, serr})
	}

	// 多写入者 → 每人超采一份。**这不是配额**：拿回来之后一起按分排，谁相关谁进，
	// 分头查只是为了不让任何一个写入者的记忆被字典序挡在候选池外面。
	seen := make(map[string]struct{}, want)
	out := make([]*store.SearchItem, 0, want)
	// 攒下每个分片的错，最后按**跟引擎层超步同一份**的判据挑一个返出去（中断 > 第一个
	// 业务错）。这里曾是「先到先得」的 firstErr：1 号写入者返普通业务错、5 号返
	// InterruptError 时返出去的是普通错，IsInterrupt 为 false ⟹ 上层按「这轮查不到，
	// warn 一下继续」处理，业务那道 gate 永远等不到人来答。
	//
	// **降级也必须报上去**：列写入者失败后我们只查了基准 ns，覆盖面从「全部写入者」塌成
	// 一个。只 slog.Warn 的话结构化观测通道上一点信号都没有，业务看到的是一片干净的成功
	// ——本函数 godoc 自己把这叫「观测在撒谎」，那条规矩对降级同样成立。
	shardErrs := make([]error, 0, len(subs)+2)
	if listErr != nil {
		shardErrs = append(shardErrs, listErr)
	}
	// 基准 ns 本层也要查——顶层 agent 和业务 seed 的记忆落在那儿，它不是任何写入者的后代。
	shards := append([]store.Namespace{base}, subs...)
	for _, sub := range shards {
		got, serr := searchMemoryShard(ctx, st, sub, withLimit(want)...)
		if serr != nil {
			// **控制流信号当场停**，不接着查别的分片。
			//
			// 一是语义：中断的意思就是「停下来问」，一个分片说了停还继续查本来就不对。
			// 二是不这么做会**把业务的一个提问变成一批里的两条**——业务在自己的 Store 里
			// 用常量 InterruptID 做 gate（`AskQuery.InterruptID` 的 godoc 明写这条路合法），
			// 它无从得知框架会按写入者分片调 N 次 Search。合并成一批后 `validateInterrupt`
			// 的「同批撞 ID 必拒」当场命中 ⟹ `ErrRunAborted` ⟹ **cp 子树被清、Resume 拒**，
			// 一个合法的 HITL gate 被判成协议违规。
			//
			// 停了就没有「一批」，那个坑不存在。代价是多个分片各有 gate 时要问 N 轮而不是
			// 一轮——可接受，而且分片本来就是顺序查的。
			if compose_v2.MustPropagate(ctx, serr) {
				return nil, serr
			}
			shardErrs = append(shardErrs, serr)
			continue
		}
		for _, it := range got {
			k := it.Namespace.String() + "\x00" + it.Key
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, it)
		}
	}
	return out, compose_v2.PickFanoutError(shardErrs)
}

// searchMemoryShard 调一次后端的 Search，去掉结果里的 nil 元素——自研后端预分配了 slice 又没填满时会夹着它们，
// 下游排序、按写入者去重、打分都要解引用（曾经 preamble 直接 panic，实测）。返回的是新 slice：有两条路把结果原样
// 交给调用方，原地改写会写坏后端自己那份（见 searchMemoriesUnscoped）。
func searchMemoryShard(ctx context.Context, st store.Store, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	got, err := st.Search(ctx, ns, opts...)
	nils := 0
	for _, it := range got {
		if it == nil {
			nils++
		}
	}
	if nils == 0 {
		return got, err
	}
	kept := make([]*store.SearchItem, 0, len(got)-nils)
	for _, it := range got {
		if it != nil {
			kept = append(kept, it)
		}
	}
	slog.WarnContext(ctx, "agent_v2: 长期记忆后端的 Search 结果里有 nil 元素，已跳过", "ns", ns.String(), "nil_items", nils)
	return kept, err
}

// memoryItemText 从一条记忆里抽出用于打分的文本。
//
// store 读回的是值的 JSON 形状（`store.NormalizeValue`），所以只有两种形态——漏掉哪种，那种记忆就永远参与不了
// 打分、永远注入不进去，而且不报错：
//
//   - `string`：**框架 commitFinal 写进去的答复文本**（2026-09-07 起），以及业务 seed 的纯文本
//   - `map[string]any`：业务结构化写入（`doc/v2/store.md` 的示例就是这个形状），业务塞进来的 struct、消息对象
//     读回也是它
//
// map 取全部 string 值拼起来，**按 key 排序**——map 迭代顺序随机，不排的话同一条记录
// 每次算出的文本不同、分数跟着抖。
func memoryItemText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k, val := range x {
			if sv, ok := val.(string); ok && sv != "" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, x[k].(string))
		}
		return strings.Join(parts, " ")
	default:
		return ""
	}
}

// storeMemoryWriterID 返回这次写长期记忆的身份，**作为 namespace 段追加在基准 ns 后面**。
//
// 优先业务显式给的 `StoreMemoryConfig.WriterID`；没给就用编排注册位置
// （`HistorySlot`——`AddBranch`/`AddStep`/`AddAgent` 那个名字，框架强制唯一，嵌套拼成
// `outer/inner`，按 `/` 展开成多段）。顶层跑时两者都空，记忆落在基准 ns 本身。
//
// **为什么不用 agent 名**：框架不校验 agent 名唯一，两个分支挂同名 agent（或两个业务
// 各自 New 一个同名的）就又撞回去了。注册位置是编排装配期强制唯一的那个。
//
// ⚠️ 返回值要当 ns 段用，所以受 [store.Namespace] 的字符约束（不能含 `.` / NUL、
// 展开后不能有空段）——而编排注册名那侧**不校验这些**（`ValidateNodeIDForNS` 只拦
// `""` / `/` / `#`）。不合规时 commitFinal 会 fail-fast 并给出路，不静默丢记忆。
func (a *BasicAgent) storeMemoryWriterID(ctx context.Context) string {
	if a.config.StoreMemory != nil && a.config.StoreMemory.WriterID != "" {
		return a.config.StoreMemory.WriterID
	}
	return HistorySlot(ctx)
}

// commitFinal 把最终 assistant 消息写入 sess + StoreMemory（如配置）。
//
// **R24 audit MED-2 fix**：StoreMemory Put 区分 control-flow err 与业务持久化错——
//   - InterruptError：业务自定义 store 后端可能在 Put 抛此错做"持久化前用户审批"
//     等 HITL 场景（敏感对话不能直接进长期记忆，需用户确认）→ propagate 让框架走
//     cp 挂起 + 父 agent.Run 收到 Interrupt + Resume 闭环
//   - ctx.Err()：上层 cancel/deadline → propagate 让 caller 知道"提前结束"语义
//   - 其他业务错（DB conn 断 / 磁盘满 / quota burst）：保留原 slog.Warn 不 propagate
//     语义——本 Run 已成功（finalMsgs 已 emit ExitEvent），让 caller 误以为 Run 失败
//     比"丢一条记忆 + 日志可见"代价更大
//
// **PR 3.3 改动**：旧 Memory.Write 路径切换到 store.Put（参 buildInitialMessages
// StoreMemory Search 同款理由）。Memory 接口已删。
//
// ⚠️ **本路径不跳过 sub-agent 调用**（这段曾写着相反的话）。`IsSubAgentStoreScope`
// 那道闸已删——它的本意是「让顶层唯一写入」，实际效果是**成员配的 WithStoreMemory
// 一条也写不进去**。防重复写改由「写入者维进 ns」解决：顶层写基准 ns、成员写
// `基准ns/<写入者位置>`，本来就不重复。详见本函数内 store.Put 那段的说明。
func (a *BasicAgent) commitFinal(ctx context.Context, session *RunSession, in *BasicAgentPostambleInput) error {
	if in == nil {
		return nil
	}
	// **本次调用的最终产出由决策点交过来**，不再从 in.Messages 倒扫找——那个列表含 prior，
	// 倒扫在 return-direct 收尾时会拿到上一次调用的答复（实测过，三处一起中招：
	// 调用方拿到别人的答案 / st.Put 再存一份导致自我复制 / 写进 append-only 历史）。
	last := in.Final
	// 结构化输出（A1）：`final_output` 是「工具产出即答案」成立的那个特例，所以 tools 节点
	// 用它**校验后的规范 JSON** 覆盖了 basicFinalCh（不是模型给的 raw args）。这里只需判
	// 「本轮收尾的是不是它」。
	//
	// **判据从「倒扫消息列表找带 annotation 的 tool_result」换成了读本轮产出**：前者扫的
	// 列表含 prior，上一轮成功的 final_output 结果同样带那个 annotation、call ID 同样在
	// foIDs 里 ⟹ **本轮零个 final_output 时会捞到上一轮的结果并静默返回**（已实测坐实），
	// 而那正是该抛 StructuredOutputError 的路径。本轮产出里 prior 进不去，洞结构上就没了。
	if a.config.OutputSchema != nil {
		sm := structuredFinalOf(last)
		if sm == nil {
			// 无成功 final_output（unforced 模型自由文本终止 / soft-compliance 不调 final_output）
			// → 抛 typed StructuredOutputError，守"绝不静默退化"契约（cross-verify CLAIM 5）：
			// SupportsToolChoiceRequired=false 的模型不强制 tool_choice，可能自由文本终止；此前
			// silent 回退 lastAssistant（自由文本）让业务 json.Unmarshal 失败且拿不到 typed error。
			return &StructuredOutputError{
				SchemaName: a.config.OutputSchema.name,
				Attempts:   getFinalOutputAttempts(ctx),
				LastError:  "模型未产出结构化输出（可能自由文本终止；不支持 tool_choice=required 的模型需业务确保模型调 final_output）",
			}
		}
		last = sm
	}
	if last == nil {
		return nil
	}
	// **ISSUE-75 R34 audit HIGH-1 fix**：先 StoreMemory Put 后 AddEvent ExitEvent——
	// 之前顺序反让 Resume 重跑 commitFinal → ExitEvent 第二次 AddEvent → dup。
	// 改后 Put 抛 InterruptError 时 ExitEvent 不 add（return err 早退）→ Resume
	// 重跑时 Put 真写入成功后才 add ExitEvent → 全程 +1 一次。idempotent on Resume。
	// **没有「我是不是 sub-agent」这个开关**。
	//
	// 曾经有：`!IsSubAgentStoreScope(ctx)`。它的本意是防「成员和父各写一遍、同一条内容
	// 在同一个 ns 里存两份」，做法是**让成员一条都不写**——于是业务给某个成员配了
	// `WithStoreMemory`，那个配置静默失效（实测：同一个 agent 顶层跑写 1 条、当成员跑
	// 写 0 条，err 全程 nil）。
	//
	// 而它之所以必须存在，是因为**键里没有写入者**——键末位当年是「各自 session 的事件
	// 条数」这个序号，对同一个 Run 里的两个成员逐字相同（实测把开关删掉，并行两分支各写
	// 一条、只存下 1 条，后写静默覆盖）。开关是在拿「不让第二个人写」模拟「两个人写不同
	// 的地方」。
	//
	// ⚠️ 键末位现在是**内容指纹**（`<RunID>_assistant_<内容指纹>`），那个撞键已经消掉；
	// 但写入者仍然只在 ns 这一维上——指纹是内容的函数不是身份的，两个成员产出同一句话
	// 就该是同一条事实。
	//
	// 现在键里有写入者了（见下），两个成员各写各的，开关就没有存在的理由——写不写由
	// 每个 agent 自己的 `WithStoreMemory` 决定，那本来就是 per-agent 的配置。
	if a.config.StoreMemory != nil {
		st := store.FromContext(ctx)
		if st == nil {
			// helper 隐式要求 WithStore 已挂——fail-fast 让业务明确感知
			return fmt.Errorf("WithStoreMemory commitFinal: %w", store.ErrStoreNotConfigured)
		}
		// **写入者进 namespace，不进 key。**
		//
		// 两者都能让多个写入者不互相覆盖，但只有前者还多买到一件事：**事后能只读某一个
		// 写入者**（`Search(ns/写入者)`），因为 Search 的语义是「本 ns + 全部后代」。
		// 写入者只在 key 里的话，`Search(ns)` 永远收窄不到单个写入者——key 不参与检索。
		//
		// 而且放 key 里还有一个实测到的副作用：Search 的跨 backend 契约是按
		// `(ns, key)` **字典序**取前 N 条（`ORDER BY ns, data_key`），键前缀会**支配排序**
		// ——3 个成员各写 3 条、SearchLimit=5 时，名字排最后的那个自己的记忆
		// **一条都注入不进去**（实测 step_c 自己的 0 条 / 别人的 5 条）。挪进 ns 之后
		// key 恢复成 `<RunID>_assistant_<...>` 的形状，排序由 RunID 主导、无系统性偏向。
		//
		// **读侧默认不变**：注入仍搜基准 ns，后代全在里面 ⟹ 同一个基准 ns 下的多个 agent
		// 照旧互相看得见（那是设计意图，见 WithStoreMemory godoc）。这一维只是让「想收窄
		// 的时候收得了」。
		memNS := a.config.StoreMemoryNS
		if w := a.storeMemoryWriterID(ctx); w != "" {
			// 写入者位置可能是嵌套路径（`outer/inner`）——按段展开，让每一层都成为
			// 可收窄的 ns 层级（`Search(ns/outer)` 能捞到 `ns/outer/inner` 的）。
			memNS = append(append(store.Namespace{}, memNS...), strings.Split(w, "/")...)
			// 段里若含 ns 的保留字符（`.` / NUL），Validate 会在 Put 入口拒——那是
			// 业务给的 WriterID 或注册名有问题，让它当场暴露而不是拼出一个歧义 ns。
		}
		// Item key：`<RunID>_assistant_<内容指纹>`。
		//
		// # 为什么是内容派生而不是序号
		//
		// 一度是 `<RunID>_assistant_<session 事件序号>`。那个序号**不是我们的**——它数的是
		// 当前 session 的事件条数，而同一个写入者在一次任务里被调多次时（AgentAsTool 调
		// 两次 / RunSubAgent 循环 / 图级 fanout）那几次**各自新建 child session**，序号都
		// 从头数 ⟹ 键相同 ⟹ **后写静默覆盖先写**。这正是「借来的量」那一类：`RunID` 里
		// 没有「一个任务里第几次写」的意思，事件序号也没有任何唯一性契约。
		//
		// # 为什么不能用随机 ID
		//
		// 本函数**在 Resume 时会重跑**（见上面 commitFinal 的顺序说明：Put 抛中断时
		// ExitEvent 不 add，Resume 重跑 Put 成功后才 add，「idempotent on Resume」）。
		// 那个幂等性**正是靠现在这个键的稳定性**——换成 uuid 会把「N≥2 才错」换成
		// **「每次 Resume 都多写一条」**，从偶发变成必然。
		//
		// 内容派生同时满足两条：不同内容不撞（消灭覆盖）+ 同内容同键（Resume 重跑天然
		// 幂等）。代价是**同一写入者写两条一模一样的内容时会合并成一条**——对「长期记忆」
		// 这个语义那是对的（重复的事实存一份），跟会话历史的 append-only 语义不同，
		// 那一层在 HistoryStore、不走这里。
		// **存的是这条答复的文本，不是整条 `*schema.Message`。**
		//
		// # 为什么
		//
		// 这条路的 value 过业务提供的 store。曾经 in-memory 原样持有 Go 值，而 redis / sql 三方言
		// 存的是 JSON、读回来是 `map[string]any`，于是同一份记忆在两类后端上**两个轴同时
		// 相反**（实测）：类型断言一边成功一边失败 ⟹ 喂给 LLM 的一边是真 assistant 消息、
		// 另一边是**整条消息的 JSON 文本**；`WithFilter{role:...}` 一边 0 命中一边 1 命中。
		// 两边都 `err == nil`。
		//
		// 存文本让六个后端逐字相同——`string` 过 JSON 往返还是 `string`，不需要类型信封、
		// 不需要注册表、不需要保留 namespace、不需要按字段猜。（store 现在按值的 JSON 形状存、每个
		// 后端读回都是 map，存整条消息也一致了；但注入时还得解回消息、按字段取内容，文本仍是最直接的形状。）
		//
		// 顺带修掉 embedding 的输入：`store.DefaultTextExtractor` 对 `map[string]any` 取**所有
		// 顶层 string 值**，所以持久后端此前 embed 进去的是「内容 + 消息 ID + 推理过程 +
		// 角色名」（实测），in-memory 只 embed 内容。存文本后两类都只有内容。
		//
		// 丢掉的 Role / Annotations / ToolCalls / ID 这一层本来就不消费——`DefaultTextExtractor`
		// 对 `*schema.Message` 也只取 `GetContent()`。要逐字完整流（含工具往返）走会话历史层。
		//
		// ⚠️ **空文本跳过**：实测 7 种收尾形态里 2 种 Content 为空（业务 return-direct 工具返
		// 空输出、模型返空文本且无 tool_call）。理由只有一条——**空串不是可用的记忆**，注入
		// 给 LLM 也没有信息量。
		//
		// （这里一度还写着「不跳过会让两条不同收尾撞键」，**那个理由是错的**：键的指纹吃的是
		// 整条消息，两条空内容但 tool_call 不同的收尾实测指纹不同；退一步即使撞了，两边 value
		// 都是 `""`，覆盖也是 no-op。撞键那个 bug 是 [memoryItemFingerprint] 自己的历史——
		// 指纹一度只取 Role+Content 时端到端复现过，那次改成整条序列化就修掉了，跟这道守卫
		// 无关。留着错理由比没理由更坏：下一个人会据它推出错的结论。）
		// ⚠️ **只跳过写入，不能 return**——本函数下游还要 `AddEvent(NewExitEvent(...))`，
		// 那条事件在「Put 成功 / 业务错被吞 / 根本没配 StoreMemory」三种情况下都必须发出
		// （见下面那句注释与 slog.Warn 那档的理由：审计要完整）。这里若 return，空内容收尾
		// 的**退出事件会整个丢掉**——写这条守卫时我就踩了一次。
		if last.Content != "" {
			key := fmt.Sprintf("%s_assistant_%s", session.RunID, memoryItemFingerprint(last))
			// Phase 2 Sub-1: store.put event fire — sink-gated (接了 observability 才发)
			putStart := time.Now()
			err := st.Put(ctx, memNS, key, last.Content)
			if hasEventSink(ctx) {
				// value_size_bytes 报的是**真正存进去的那个值**的大小——上面那行 Put 存的是
				// `last.Content`（答复文本），所以这里就是它的字节数。
				// ⚠️ 这里曾按 `json.Marshal(last)` 算整条消息（那是「存整条消息」时代的算法，
				// audit MED-2 加的）；2026-09-07 写入侧改存文本之后它就高估了一倍上下，
				// 2026-09-20 改回按存入值算。
				valueSize := len(last.Content)
				putAction := &StorePutAction{
					NS:             memNS.String(),
					Key:            key,
					ValueSizeBytes: valueSize,
					DurationMs:     time.Since(putStart).Milliseconds(),
					Ok:             err == nil,
				}
				if err != nil {
					putAction.Err = err.Error()
				}
				EmitStoreEvent(ctx, &StoreAction{Type: StoreOperationPut, Put: putAction})
			}
			if err != nil {
				// control-flow err 必须 propagate（HITL / cancel）
				if _, isInt := compose_v2.IsInterrupt(err); isInt {
					// ISSUE-75 协议：commitFinal 跑在 graph 内 postamble 节点抛 Interrupt 时
					// graph_loop defer SaveNS 自动存 cp，caller Resume 走 cm.Restore 重跑
					// postamble 节点 → GetResumeAnswerText 命中即真写入。HITL 链完整闭环。
					return err
				}
				if ctx.Err() != nil {
					return err
				}
				// **ns 非法是确定性配置错，不能跟瞬时后端错一起吞掉。**
				//
				// 「吞掉只 Warn」那条取舍是给 DB 断 / 磁盘满 / quota 立的——那类重跑一次
				// 可能就好。而 ns 非法（写入者位置含 `.` 或 NUL、或拼出空段）重跑必然一样，
				// 吞掉的结果是**记忆一条不落且 err=nil**，业务什么也看不到。
				//
				// 出路对调用方是现成的（改注册名 / 显式给 WriterID），所以这里 fail-fast
				// 合法。错误正文要给全：ns 本身 + 写入者是从哪儿来的。
				if errors.Is(err, store.ErrInvalidNamespace) {
					return fmt.Errorf("agent %q: 长期记忆的写入者位置 %q 不能用作 namespace 段"+
						"（namespace 不接受 '.' 和 NUL，拼接后也不能有空段）：%w"+
						"；出路：给这个 agent 显式配 StoreMemoryConfig.WriterID，或改掉编排里的注册名",
						a.config.Name, a.storeMemoryWriterID(ctx), err)
				}
				// 业务持久化错保留 slog.Warn 不 propagate——ExitEvent 仍 add 让审计完整
				slog.WarnContext(ctx, "agent_v2: store memory put failed",
					"agent", a.config.Name, "session", session.RunID,
					"ns", memNS.String(), "writer", a.storeMemoryWriterID(ctx), "err", err)
			}
		}
	}
	// 这个任务里接进来的运行中插话进会话事件，排在它回应的那条答复之前：那是用户的原话，没开会话历史时
	// 下一个任务的上文（从事件读）和转交的下一个接手者只能从这里看到它——不记，上文里就剩一条在回应
	// 一句不存在的话的答复。跟 ExitEvent 放在同一处、同样在 StoreMemory 块之后：Put 抛中断时这里不跑，
	// Resume 重跑 postamble 时只记一次。
	for _, m := range in.Interjections {
		if m != nil {
			session.AddEvent(NewOutputEvent(a.config.Name, m))
		}
	}
	// **ExitEvent 恒在这里发**——Put 成功 / 业务错被吞 / Content 为空跳过写入 / 根本没配
	// StoreMemory，四档都要发。⚠️ 它必须留在 StoreMemory 块**之后**：Put 抛中断时本轮不发、
	// Resume 重跑 Put 成功后才发，挪到前面会让 Resume 重跑时第二次 add ⟹ dup（实测中断轮
	// 1 条、Resume 后 2 条）。哨兵 TestCommitFinal_ExitEventAfterStorePut 两个方向都钉。
	session.AddEvent(NewExitEvent(a.config.Name, last))
	return nil
}

// 内部 channel 名（与 BasicAgentBuilder 内部一致——共享底层 channel
// 让 caller 可读 channels 取 messages）
//
// 注意：BasicAgentBuilder 的 basicMessagesCh 是 unexported——
// agent_v2 这里独立声明同名 channel + 通过 InitialWrites 写入。
// Run 后通过同 channel name 取最终 messages。
var basicMessagesChan = compose_v2.Append[*schema.Message]("__basic_agent_messages__")

// init 注册 channel value 类型让 cm.Snapshot 能 marshal——HITL 中断时若类型未注册，
// snapshot 失败 → cp 不持久化 → Resume 拿不到 cp。
//
// 注册所有 compose_v2 内建编排 (8 个低阶 builder: BasicAgentBuilder + 7 高阶
// Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) + agent_v2 业务用到的
// channel value 类型.
//
// 命名按 reflect.TypeOf(zero).String 形态对齐 compose_v2/serialization.go::
// RegisterValueType godoc 示例（"int" / "[]string" / 包名.类型）——业务自定义
// 注册同 Go 类型时按相同名字幂等不 panic（同 type 不同 name 不允许 re-register）。
func init() {
	// BasicAgentBuilder 内部用——typeName 与导出路径对齐 (`schema.Message` 是
	// 业务可见名, `common.Message` 是 spec internal 名) ——
	// **ISSUE-87 LOW-1 fix**: 跨包统一为 "[]*schema.Message" / "*schema.Message"
	// (与 example/channel_chat_demo / channel_smoke 一致), 避免 first-write-wins
	// 取决于 import 顺序 wire format 漂移。
	compose_v2.RegisterValueType[[]*schema.Message]("[]*schema.Message")
	compose_v2.RegisterValueType[int]("int")

	// **ISSUE-74 修**：preamble 节点的 PreambleInput 必须注册——cp marshal 序列化用。
	// 不注册则 preamble 抛 InterruptError 时 Snapshot 失败 → cp 不存 → HITL 链断
	// （讽刺：变成 ISSUE-74 同病）。
	compose_v2.RegisterValueType[*BasicAgentPreambleInput]("*BasicAgentPreambleInput")

	// SequentialAgentBuilder 内部用——seqAgentLastOutputCh / seqAgentOriginalCh
	// 是 LastValue[*schema.Message]，channel value 是 *schema.Message 单 ptr。
	// **ISSUE-87 LOW-1 fix**: typeName 跨包统一 "*schema.Message"
	compose_v2.RegisterValueType[*schema.Message]("*schema.Message")

	// ParallelAgentBuilder 内部用——parAgentResultsCh 是 Append[ParallelBranchResult]
	compose_v2.RegisterValueType[[]ParallelBranchResult]("[]ParallelBranchResult")

	// PlanAgent 内部 channel 用的基础类型（planAnswerCh=string / planDoneCh=bool）；
	// LoopAgent 等其他编排也复用。PlanAgent 专有的 *planExecStep / []ExecutedStep
	// 由 plan_builder.go 自身 init 注册（unexported 类型留在其所属包文件）。
	compose_v2.RegisterValueType[string]("string")
	compose_v2.RegisterValueType[bool]("bool")
	// []string：basicRevealedCh（工具渐进披露的已解锁集，Append[string] ⟹ 值类型是
	// []string）。
	//
	// ⚠️ **漏注册的后果不是「这个 channel 存不下来」，是整份 cp 存不下来**——
	// Snapshot 逐个 marshal channel 值，一个失败整份快照失败。实测：开了 tool_search 的
	// agent 一旦解锁过工具，后续任何一处中断都 ListPending=0、Resume 返
	// ErrNoPendingCheckpoint，HITL 整条链死掉。
	//
	// ⚠️ **注册必须在这里、不能指望业务**：typeNameByType 是 first-write-wins，本包
	// init 先于任何 import 它的包，wire tag 才恒定是 "[]string"；否则业务先注册
	// `RegisterValueType[[]string]("my.tags")` 就会把 canonical name 抢走（同 :1566
	// ISSUE-87 LOW-1 防的那件事）。
	compose_v2.RegisterValueType[[]string]("[]string")
}

// buildComposeGraph 把 a.config 的 model + toolkit + hooks + 4 层压缩组装成
// BasicAgentBuilder.Compile 后的 *Graph。
//
// **ISSUE-74 修**：必注入 preambleFn 到 compose_v2 BasicAgentBuilder.WithPreamble——
// 让业务回调（store.Search / HistoryCompactor / Skill 预激活）跑在 graph 内首段
// preamble 节点而非 Run 入口同步，抛 InterruptError 时走 graph 内 SaveNS 协议
// 自然支持 Resume。
//
// **ISSUE-75 修**：必注入 postambleFn 到 compose_v2 BasicAgentBuilder.WithPostamble——
// 让业务收尾回调（store.Put / AddEvent ExitEvent）跑在 graph 末段 postamble 节点
// 而非 Run 后同步调用，抛 InterruptError 时 cleanupOnSuccess 不触发，cp 保留供
// Resume。修复 commitFinal 阶段 HITL gate 不可 Resume 的对称缺口。
//
// ctx 用于读 per-call 覆盖（resolveMaxRounds 等）——Run 入口经 withRunOverrides push，
// 优先于构造期 a.config；Resume 路径 ctx 无覆盖则用构造期默认。
// grants 是**已自查过**的转交 / 路由授予（caller 传 `runCfg.grantsFor(a)`）——没有的那份表示
// 本次调用没被授予那项能力，modelFn 不注册对应的工具。**注册工具这件事沿签名进闭包，不从 ctx 读**：
// 授予若是普通的 ctx 值就是环境，孙辈照样读得到，整套授予当场退化（见 handoffGrant godoc）。ctx 上只有
// runGraph 放的那一份受控形态（nodeGrants）：只给本图里的代码经 [RegrantFromNode] 显式转授，框架调起
// 别的 agent 时与各编排入口都遮住它。
func (a *BasicAgent) buildComposeGraph(ctx context.Context, grants memberGrants) (*compose_v2.Graph, *tool.Toolkit, error) {
	// **两把闸**：上层授予 + 本 agent 自己 opt-in。缺任一把都不注册对应的工具
	// （transfer_to_agent / route_to）。授予是「这次调用里谁有资格」，opt-in 是「这个 agent
	// 愿不愿意」——后者让业务能把某个候选做成终点，也让框架**自己合成**的执行体（TmpSubAgent
	// 等）天然不在名单里。
	if !a.config.HandoffTransferOptIn {
		grants.handoff = nil
	}
	if !a.config.RouteToolOptIn {
		grants.route = nil
	}
	dispatchToolkit := a.composeDispatchToolkit(grants, backgroundScopeFrom(ctx) != nil)
	modelFn := a.makeModelFn(grants)
	toolsFn := a.makeToolsFn(grants, dispatchToolkit)

	bld := NewBasicAgentBuilder(modelFn, toolsFn).
		WithName(a.config.Name).
		WithMaxRounds(resolveMaxRounds(ctx, a.config.MaxRounds)). // per-call 覆盖（ctx 无则构造期默认）
		WithPreamble(a.preambleFn).                               // ISSUE-74 修
		WithPostamble(a.postambleFn)                              // ISSUE-75 修
	if a.config.Hooks != nil {
		bld = bld.WithAgentHooks(a.config.Hooks)
	}
	// 交给 OnModelStart 的请求配置：跟 modelFn 发请求用的同一个模型（按次 WithRunModel 覆盖构造期的）与同一组按次选项。
	bld = bld.WithModelConfig(func(ctx context.Context) *schema.ModelConfig {
		return model.RequestConfig(resolveModel(ctx, a.config.Model), resolveModelOpts(ctx)...)
	})
	if a.config.BackgroundTasks != nil {
		bld.backgroundNotes = backgroundNotes // 后台任务的结束通知（见 background_task.go）
	}
	if a.config.ToolResultSpill != nil {
		// OnToolEnd 之后按大小处理（转存 / 截断，见 renderToolContent）
		cfg := a.config
		bld.finishToolResult = func(ctx context.Context, toolName string, msg *schema.Message, failed bool, code string) {
			finishPendingToolResult(ctx, &cfg, toolName, msg, failed, code)
		}
	}
	// 这一批处理完之后把结果定进工具账（见 finalizeToolMemo）——跟配没配转存无关。
	bld.finalizeToolMemo = finalizeToolMemo
	if len(a.config.TurnGates) > 0 {
		bld = bld.WithTurnGates(a.config.TurnGates, a.config.TurnGateMaxVetoes)
	}
	g, err := bld.Compile()
	return g, dispatchToolkit, err
}

// postambleFn 是 BasicAgentPostambleFn 实现——graph 末段节点跑业务收尾
// 回调（ISSUE-75 修）。
//
// 实现策略：从 ctx 取 session（Run 入口已 WithSession 注入），调原 commitFinal 复用
// 全部 AddEvent ExitEvent + store.Put 逻辑——但跑在 graph 内（非 Run 之后）。
//
// **idempotent**：postamble 在 Resume 时完整重跑——业务自定义 Store 实现的 Put
// 必须支持多次调用同结果（典型实现走 ResumeContext 区分首次/续跑）。详见
// `BasicAgentPostambleFn` godoc。
func (a *BasicAgent) postambleFn(ctx context.Context, in *BasicAgentPostambleInput) error {
	// 最后一轮送进来的后台任务通知随那一轮提交了，之后不会再有模型节点看到它，在这里记下送达（见 background_task.go；
	// return-direct 收尾也经过这里）
	if in != nil {
		backgroundSeen(ctx, in.Messages)
	}
	session := GetSession(ctx)
	if session == nil {
		// fallback：理论不会发生（Run 入口已 WithSession 注入）；保守路径不 panic
		session = sessionForCtx(ctx)
	}
	return a.commitFinal(ctx, session, in)
}

// preambleFn 是 BasicAgentPreambleFn 实现——graph 首段节点跑业务准备
// 消息（ISSUE-74 修）。
//
// 实现策略：从 ctx 取 session（Run 入口已 WithSession 注入），调原 buildInitialMessages
// 复用全部拼装逻辑（Skill 预激活 / store.Search / HistoryCompactor 等）。
//
// **idempotent**：preamble 在 Resume 时完整重跑——业务自定义 Store 实现的 Search
// 等回调必须支持多次调用同结果（除非业务自己读 ResumeContext 主动改写）。详见
// `BasicAgentPreambleFn` godoc。
func (a *BasicAgent) preambleFn(ctx context.Context, in *BasicAgentPreambleInput) (*BasicAgentPreambleOutput, error) {
	if in == nil || in.UserInput == nil {
		return nil, fmt.Errorf("agent %q: preamble input is nil", a.config.Name)
	}
	session := GetSession(ctx)
	if session == nil {
		// fallback：理论不会发生（Run 入口已 WithSession 注入）；保守路径不 panic
		session = sessionForCtx(ctx)
	}
	sysPreamble, msgs, err := a.buildInitialMessages(ctx, session, in.UserInput)
	if err != nil {
		return nil, err
	}
	// 会话历史记原始 input 的逻辑在 buildInitialMessages 末段——那里有 prior，nonce 去重要
	// 看它（见那处注释）。
	return &BasicAgentPreambleOutput{System: sysPreamble, Messages: msgs}, nil
}

// toolDeadlineExceeded 判断**框架给这次工具调用设的那个 deadline**是否到期。
//
// 只在配了 [WithToolTimeout] 时有意义：`callCtx` 那时是从调用方 ctx 派生出来的，
// 它 `Err() != nil` 有两种来源——自己的 deadline 到了，或调用方的 ctx 死了。
// 后者不该被当成工具超时（那是真的要停），所以用 `errors.Is(..., DeadlineExceeded)`
// 加上「调用方 ctx 还活着」两个条件一起判。
func toolDeadlineExceeded(parentCtx, callCtx context.Context, d time.Duration, err error) bool {
	if d <= 0 || !errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		return false
	}
	// **父还活着**——否则这是「整个 Run 超时」，不是「这个工具超时」。派生 ctx 继承父的
	// deadline，父被 deadline 杀掉时它的 `Err()` 同样是 DeadlineExceeded，只看它会把
	// 调用方的超时写成一句「工具调用超时（超过 10m0s 未返回）」的谎话落进会话历史。
	if parentCtx.Err() != nil {
		return false
	}
	// **这个错能不能吞**——超时收尾是一个吞点，它必须跟别的吞点问同一个问题。
	// 不问的话，工具在超时窗口内抛的中断 / 终局失败 / 坏存档会被换成一条超时
	// tool_result + `err=nil`：实测 HITL 中断被吞后 `cp 挂起数=0`（人再也答不上）、
	// `ErrRunAborted` 被吞后工具被重跑 3 次。
	return !compose_v2.MustPropagate(parentCtx, err)
}

// jsonSafeValue 把 v 变成「一定能 json.Marshal、且内容尽量保留」的等价物，供
// [memoryItemFingerprint] 在整条 marshal 失败时做**局部降级**用。
//
// 逐层先试整块序列化——绝大多数子树走这条原样返回；只有卡住的那一层才拆开递归，
// 最终卡住的叶子换成 `<unmarshalable:类型名>`。**替身只含类型名**：指纹是内容身份，
// 掺进地址就等于每次调用都换一个键。
//
// struct 转成按字段名索引的 map、map 的 key 转成字符串——`json.Marshal` 对
// `map[string]any` 按 key 排序，故输出仍是确定性的。
//
// depth 上限防的是**环**：`Annotations map[string]any` 里可以挂自引用的值，那种值
// `json.Marshal` 会报 cycle（于是每层都失败、每层都递归）。到顶就交替身收手。
func jsonSafeValue(v reflect.Value, depth int) any {
	if !v.IsValid() {
		return nil
	}
	if depth > 32 {
		return "<unmarshalable:depth-exceeded>"
	}
	if v.CanInterface() {
		iv := v.Interface()
		if _, err := json.Marshal(iv); err == nil {
			return iv
		}
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return jsonSafeValue(v.Elem(), depth+1)
	case reflect.Slice, reflect.Array:
		out := make([]any, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			out = append(out, jsonSafeValue(v.Index(i), depth+1))
		}
		return out
	case reflect.Map:
		out := make(map[string]any, v.Len())
		for _, k := range v.MapKeys() {
			out[fmt.Sprint(k.Interface())] = jsonSafeValue(v.MapIndex(k), depth+1)
		}
		return out
	case reflect.Struct:
		t := v.Type()
		out := make(map[string]any, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			if f := t.Field(i); f.PkgPath == "" {
				out[f.Name] = jsonSafeValue(v.Field(i), depth+1)
			}
		}
		return out
	}
	return "<unmarshalable:" + v.Type().String() + ">"
}

// memoryItemFingerprint 算长期记忆条目的内容指纹——同内容同键、不同内容不同键。
//
// 用 FNV-1a 而不是加密哈希：这里要的是**键的唯一性**不是抗碰撞攻击，而 store 的 key 是
// 业务可见字符串、越短越好排查（base36 最长 13 字符）。实测 200 万条零碰撞，而碰撞域还被
// `(ns, RunID)` 双重收窄——一个写入者在一次任务里的写入数现实是个位到百位。
//
// ⚠️ **`Message.ID` 必须不进指纹**——那个每次构造都新铸，进了就等于随机键（Resume 重跑
// 必然多写一条）。序列化前显式清掉。
func memoryItemFingerprint(m *schema.Message) string {
	if m == nil {
		return "nil"
	}
	// ⚠️ **更上游的问题没人问过**（2026-08-28 补查）：同类产品的 store 是
	// `put(namespace, key, value)`——**键由调用方给**，全仓没有内容指纹这个东西。
	// **他们没有这个问题，因为不替业务自动写记忆。** 我们这个指纹是「框架自动持久化
	// 最终答复 + 框架自己编键」的衍生品。在这个前提下内容派生是唯一站得住的选择
	// （自动写就必须幂等），但「要不要允许业务给键函数」是个没被问过的设计问题，
	// 记在 `doc/code/todo.md`。
	//
	// **序列化整条消息再哈希，而不是手挑字段。**
	//
	// ⚠️ 手挑是 allowlist——**新字段默认漏**，而漏掉的后果跟本函数要修的那个 bug 一模一样
	// （不同内容撞键、后写静默覆盖）。一度只取 Role + Content + block 的 Type/Text，实测
	// 至少 6 类会撞，其中一类**端到端复现**了：同一子 agent 两次分别以**不同的 return-direct
	// 工具 + 不同参数**收尾时，两条消息的 Content 都是空、tool_call 载荷不进指纹 ⟹ 键逐字
	// 相同 ⟹ 2 次 Put 落 1 条、`err` 全程 nil。另外 `ReasoningContent` / `Refusal` 也是生产
	// 可达的（provider 会回填）。
	//
	// `json.Marshal` 对 map 的 key 排序、对 struct 按字段声明序，同一个值恒得同一份字节流；
	// 新加字段自动进指纹。序列化失败时退回 Role+Content——那时至少不比手挑更差。
	// **浅拷贝 + 清掉「每次都变的运行时元数据」再序列化。**
	//
	// 拷贝是为了不改调用方那条消息（它随后要进 events / 存储）。清掉的两样都实测过：
	//   - `ID`：每次构造都新铸 ⟹ 带进去就等于随机键，Resume 重跑必然多写一条。
	//   - `Metadata`：里面有 `created` 时间戳和 token usage ⟹ 真 provider 每次都不同，
	//     同一条内容跨 Run 会得到不同指纹，幂等直接失效（改成整条序列化时当场红了）。
	//
	// ⚠️ 这是「整条序列化」的**必要修正**，不是回到手挑字段：默认全带、只显式排除**已知
	// 非内容**的那几个。新加的**内容**字段仍自动进指纹；新加的运行时元数据要记得在这儿排除
	// ——判据是「同一条消息重跑一次，这个字段会不会变」。
	probe := *m
	probe.ID = ""
	probe.Metadata = nil
	if len(probe.Annotations) > 0 {
		// `run_id`（[compose_v2.RunIDAnnotationKey]）**逐次 LLM 调用都换一个新的**
		// （`callLLM` 闭包里 `newUUID()`），所以它进指纹就等于随机键。
		//
		// ⚠️ 别把它跟键前缀那个 `session.RunID` 当成一回事——**同名不同物**：那个是 per-Run
		// 的任务身份，这个是 per-LLM-call 的消息分组标记。所以排除它的理由是「**它逐次变**」
		// 而不是「跟前缀冗余」（一度写成后者，是把两个粒度混了）。
		//
		// 其余 annotation（skill body / 压缩 marker / 归属之类）是内容的一部分，照进。
		filtered := make(map[string]any, len(probe.Annotations))
		for k, v := range probe.Annotations {
			if k == compose_v2.RunIDAnnotationKey {
				continue
			}
			filtered[k] = v
		}
		probe.Annotations = filtered
	}
	h := fnv.New64a()
	b, err := json.Marshal(&probe)
	if err != nil {
		// **降级也必须由内容派生。**
		//
		// ⚠️ 这里一度写 `%#v`，那是错的，而且错得比它要修的 bug 更糟——审计实测坐实：
		// `fmt` 只在 depth 0 解引用指针，`ContentBlocks []*ContentBlock` 的元素在 depth 2
		// ⟹ 打出来的是**分配地址**，载荷根本不出现。后果两个方向都破：同一个对象原地
		// 改 tool_call 参数指纹**一模一样**（`{"amount":1}` → `{"amount":999999}` 不变），
		// 而两条逐字相同的消息因为地址不同**指纹不同**。地址不是内容属性。
		//
		// 现在的做法是**局部降级**：能序列化的子树原样保留，只有真正卡住的那个叶子换成
		// `<unmarshalable:类型名>` 替身（**只含类型名不含地址**，否则同一条消息两次仍会
		// 算出不同指纹）。这样 Role / Content / ContentBlocks / Refusal / ReasoningContent
		// 这些本来就可序列化的字段照常进指纹——`Annotations` 里一个 func 不该把整条消息的
		// 内容全挤出去。
		safe, serr := json.Marshal(jsonSafeValue(reflect.ValueOf(probe), 0))
		if serr != nil {
			// 结构上不可达（jsonSafeValue 的每条出口都是 json 安全的）。真到这儿宁可让
			// 这一条退化也不能 panic。
			_, _ = fmt.Fprintf(h, "fingerprint-degraded:%v", serr)
			return strconv.FormatUint(h.Sum64(), 36)
		}
		b = safe
	}
	_, _ = h.Write(b)
	return strconv.FormatUint(h.Sum64(), 36)
}

// clearToolChoice 摘掉调用方强制的 tool_choice（这一轮没有工具可绑时用，见 modelFn）。
func clearToolChoice(o *model.Options) { o.ToolChoice = nil }

// toolsUsedKey 标「这个任务里模型已经调过工具」（第一批解锁了新工具的 tool_search 不算；modelNode 据 basicToolsUsedCh 判）——调用方
// 强制的 tool_choice=required 只管到这一刻为止。
type toolsUsedKey struct{}

// withToolsUsed 推这一轮的值；v 为 false 也要推，理由同 withFinalRound（父 agent 模型回调里调起的子 agent
// 第 0 轮曾被复位掉自己要求的 required，审计实测）。
func withToolsUsed(ctx context.Context, v bool) context.Context {
	return context.WithValue(ctx, toolsUsedKey{}, v)
}

func toolsUsed(ctx context.Context) bool {
	v, _ := ctx.Value(toolsUsedKey{}).(bool)
	return v
}

// resetRequiredToolChoice 把调用方强制的 tool_choice=required 复位成不指定；其它取值（auto / none）不动。
func resetRequiredToolChoice(o *model.Options) {
	if o.ToolChoice != nil && *o.ToolChoice == schema.ToolChoiceRequired {
		o.ToolChoice = nil
	}
}

// withFrameworkTools 返回挂上框架 opt-in 工具（[WithAskUser] 的 ask_user、[WithOutputSchema] 的
// final_output、[WithToolResultSpill] 的 read_tool_output）之后的工具表；都没配时原样返回 tk。绑定侧
// （modelFn）与派发侧（toolsFn）各调一次，两边看到的是同一组工具。
//
// 不往 cfg.Toolkit 里注册：那是业务传进来的对象，可能被几个 agent 共用——写进去的话没配这些能力的
// agent 也拿到了（实测：没配结构化输出的 agent 拿别的 agent 的 schema JSON 当最终答复收尾，`err == nil`）。
// 要加时拷一份；业务的同名工具在这份里被框架那件顶掉（构造期已告警）。
func withFrameworkTools(cfg *Config, tk *tool.Toolkit, background bool) *tool.Toolkit {
	background = background && cfg.BackgroundTasks != nil
	if !cfg.AskUserOptIn && cfg.OutputSchema == nil && cfg.ToolResultSpill == nil && !background {
		return tk
	}
	out := tool.NewToolkit()
	if tk != nil {
		for _, t := range tk.List() {
			out.Register(t)
		}
	}
	if cfg.AskUserOptIn {
		out.Register(NewAskUserTool())
	}
	if cfg.OutputSchema != nil {
		out.Register(cfg.OutputSchema.tool)
	}
	if cfg.ToolResultSpill != nil {
		out.Register(newReadToolOutputTool(cfg.ToolResultSpill))
	}
	if background {
		out.Register(newBackgroundTaskTool())
	}
	return out
}

// keepOnlyTools 只保留名字在 names 里的工具——**不豁免框架工具**。
//
// 跟 [filterToolsByName] 的差别就在这一句，而那一句是承重的：那个函数服务
// `WithToolFilter`（裁业务工具、框架工具恒保留，防业务漏带把问用户/技能静默关掉），
// 本函数服务配了结构化输出时的软降落轮（只留 `final_output`，要摘的恰恰就是那些框架工具）。
// **同一个词「过滤」，两种相反的语义**——所以是两个函数，不是一个函数加参数。
func keepOnlyTools(infos []*schema.ToolInfo, names ...string) []*schema.ToolInfo {
	keep := make(map[string]bool, len(names))
	for _, n := range names {
		keep[n] = true
	}
	out := make([]*schema.ToolInfo, 0, len(names))
	for _, info := range infos {
		if info != nil && keep[info.Name] {
			out = append(out, info)
		}
	}
	return out
}

// finalRoundKey 标「这是轮次预算的最后一轮」——modelFn 据它关掉这一轮的工具调用、逼模型收尾，工具函数据它不执行
// 这一轮模型硬调的工具。
type finalRoundKey struct{}

// withFinalRound 标记本次 model 调用是**软降落轮**。
//
// # 为什么需要它
//
// `maxVisit = maxRounds + 1` 那个 `+1` 本来就是**为最后一次无 tool 的 final 调用预留的**
// （见 basic_builder 的 godoc）——但它靠模型**自己选择**不再调工具。模型一直调就耗尽，
// 于是调用方拿到一个 `MaxIterationsError` 和一堆没有结论的 events：**模型从没得到把手上
// 的东西收个尾的机会**。
//
// 软降落把那格预留兑现：最后一轮**关掉工具调用**——模型要么直接产文本，要么经 `final_output` 交结构化答复。
//
//   - 没配 WithOutputSchema：工具集**照旧**（跟前几轮同一套），tool_choice 设成 none，盖掉调用方强制的那份。
//     不摘工具：工具集是 Anthropic 给 thinking 块签名时绑定的前缀之一，摘掉就让之前的 thinking 全部作废
//     （理由与代价见 makeModelFn 软降落那段）。
//   - 配了：只留框架那件 `final_output`（结构化输出的唯一投递通道），其余一律摘掉。
//
// 这一轮模型仍硬调的工具不执行，以失败结果交回（框架那件 `final_output` 除外）：none 不是每家 provider 都强制执行，
// 业务自己的 ChatModel 也未必认它。tools 节点从 basicFinalRoundCh 读出同一个标记交给工具函数，见 makeToolsFn。
//
// # 为什么仍然返回 MaxIterationsError
//
// `maxRounds` 既可能是**保险丝**（防跑飞）也可能是**预算**（用户可理解的额度），取决于
// 产品怎么用——框架不该替它决定。所以走标准库那个形状（`strconv.ParseInt` 的
// `(clamped value, ErrRange)`）：**结果照给、error 照返**。
//
//   - 当保险丝用：`if err != nil` ⟹ 跟以前完全一样，触顶就是异常
//   - 当预算用：`errors.As(&mie)` 之后读 `FinalMessage(events)` ⟹ 拿到那条收尾答复
//
// ⚠️ **后半句只对直调 BasicAgent 成立**。成员跑在编排里时，`MaxIterationsError` 经 `%w`
// （出口这一跳是 `errors.Join`——出口重铸一个自己的挂在外层，见 Run 出口那段）一路冒到顶层、
// `errors.As` 照样命中，但 `FinalMessage(events)` 拿到的是**那个打满轮次的
// 成员的**收尾文本——后续步骤根本没跑，而调用方从错误里分辨不出来。编排层要判「哪一步
// 耗尽了」得走 `OnAgentEnd` 或每步各自的返回值。
//
// 一个形状同时服务两种用法，不需要开关。而「错误路径上照常返 events」本来就是本仓的
// 既有风格（Run 的多个早退分支都返 `collectEvents(...)`）。
//
// v 为 false 也要推：model 节点每轮都推自己的值，盖掉从调用方继承来的那份——父 agent 在软降落轮的模型回调
// （OnModelEnd、ToolFilter、ChatModel 本身）里调起的子 agent，ctx 上带着父的标记，只在为真时才推的话子 agent
// 第 0 轮就被摘光工具（审计实测，那时软降落摘工具；现在是第 0 轮就被关掉工具调用）。tools 节点同理每轮都推：
// 不推的话子 agent 调的工具全被当成软降落轮硬调的、一个不执行。
func withFinalRound(ctx context.Context, v bool) context.Context {
	return context.WithValue(ctx, finalRoundKey{}, v)
}

func isFinalRound(ctx context.Context) bool {
	v, _ := ctx.Value(finalRoundKey{}).(bool)
	return v
}

// makeModelFn 构造 BasicAgentModelFn——wrap 4 层压缩 + 真 LLM 调用。
//
// 顺序（每轮入口）：
//  1. 第 1 层 MicroCompact（清白名单内的旧 tool_result 内容，保配对结构）
//  2. 第 2 层 AutoCompact（modelNode 入口估算 token 超阈值时 LLM 摘要）
//  3. ChatModel.WithTools(toolkit) → 真调 LLM
//  4. 第 3 层 ReactiveCompact（LLM 报 prompt-too-long 时 retry 循环 MaxAttempts 次）
//
// MicroCompact 在 modelFn 入口跑（modelFn 看到的 msgs 是累积历史，能正确清旧
// tool_result）。语义：每次 LLM 调用前清，等价于"toolsFn 完成后清"。
//
// ReactiveCompact 循环最多 MaxAttempts 次，每次 compact 后重试，仍 prompt-too-long
// 继续 compact 直到成功 / attempts 耗尽。
func (a *BasicAgent) makeModelFn(grants memberGrants) BasicAgentModelFn {
	cfg := a.config
	// 业务自带同名工具 warn 在构造期一次而非每轮——
	// 避免 modelFn 每轮 LLM 调用都打同样 warn → N 轮会话产生 N 条噪声
	if cfg.Skill != nil && cfg.Skill.SkillBox != nil &&
		skillActivationMode(cfg.Skill) == skill.SkillActivationModeToolCall &&
		cfg.Toolkit != nil && cfg.Toolkit.Get(activateSkillToolName) != nil {
		slog.Warn("framework activate_skill tool overrides user-registered tool with same name",
			"agent", cfg.Name, "mode", "toolCall")
	}
	return func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		// AGENTS.md harness 指令层——本轮加载一次（transient：每次 model 调用时在 callLLM 内
		// prepend 进 in，不进 msgs / 压缩路径）。空串则不注入。
		var agentsMD string
		if len(cfg.AgentsMDPaths) > 0 {
			agentsMD = loadAgentsMD(ctx, cfg.AgentsMDPaths)
		}

		// 结构化输出预算（A4）：数**本次调用**里 final_output 的尝试次数，超 maxRetries 抛
		// typed StructuredOutputError（绝不静默退化）。
		//
		// 计数由 tools 节点按派发层的章写进 `basicFinalOutputAttemptsCh`，**不再扫消息列表**
		// ——那个列表含 prior，开了会话历史层时上几轮的尝试会被算进本轮预算，**模型压根
		// 不被调用就抛错**（实测：Run 1 有过一次校验失败回炉，Run 2 换全新问题、模型第一次
		// 就给合法 JSON，仍报「已尝试 2 次」）。跟「陈旧答复顶替本轮答案」是同一个根因。
		//
		// 原先「数历史而非独立 counter」的动机是「cp 持久、跨 Resume 累积不重置」——
		// 那个顾虑对普通变量成立，对 channel 不成立（channel 本来就在 cp 里）。
		// 顺带也不再受压缩影响：压缩只临时改喂给 LLM 的那份、不写回 channel，而计数
		// 现在根本不看消息列表。
		if cfg.OutputSchema != nil {
			if attempts := getFinalOutputAttempts(ctx); attempts > cfg.OutputSchema.maxRetries {
				return nil, &StructuredOutputError{
					SchemaName: cfg.OutputSchema.name,
					Attempts:   attempts,
					LastError:  lastStructuredError(msgs),
				}
			}
		}

		// 这一轮之前的摘要结果（单次 Run 内复用，见 compaction_memo.go）：消息流的前 Upto 条换成那次的压缩
		// 形态，后面两层只处理之后新增的部分。排在 MicroCompact 之前——备忘的锚点按原始消息流记，
		// MicroCompact 会把旧工具结果换成占位符的克隆，在它之后校验会对不上。
		memoCtx := compactionMemoFrom(ctx)
		origMsgs := msgs
		memoUpto, memoHead := 0, 0
		if memoCtx != nil && cfg.AutoCompact != nil && cfg.AutoCompact.Enabled {
			msgs, memoUpto, memoHead = memoCtx.in.apply(msgs)
		}

		// 第 1 层 MicroCompact——清白名单 tool 旧 tool_result 内容（保结构）
		if cfg.MicroCompact != nil && cfg.MicroCompact.Enabled {
			msgs = MicroCompactRun(ctx, msgs, cfg.MicroCompact)
		}

		// 第 2 层 AutoCompact——超阈值时调 LLM 摘要
		if cfg.AutoCompact != nil && cfg.AutoCompact.Enabled {
			inputLen := len(msgs)
			// 套用了上一次的结果时告诉压缩 Head 在哪、原始消息流是哪份（见 compactCut）。原始流没经过 MicroCompact：
			// 它会清掉的那些由同一个判据算出来交过去，文件重注入跳过它们。
			cut := &compactCut{headLen: memoHead, original: origMsgs, tailLen: -1}
			if cfg.MicroCompact != nil && cfg.MicroCompact.Enabled && cfg.AutoCompact.reinjectRecentFiles() {
				cut.cleared = microCompactToClear(origMsgs, cfg.MicroCompact)
			}
			compacted, ccErr := autoCompactRun(ctx, cfg.AutoCompact, msgs, cut)
			// 真摘要了一次 ⟹ 记成新的备忘，模型节点成功后写回 channel，下一轮从这里接着（同一轮里
			// 模型调用失败 / 中断时这份结果随节点作废，续跑重做——跟消息流同生共死）。
			if ccErr == nil && cut.tailLen >= 0 && memoCtx != nil {
				memoCtx.out = nextCompactionMemo(origMsgs, memoUpto, memoHead, inputLen, compacted, cut.tailLen)
			}
			// **R23 audit HIGH-1 fix**：control flow err 必须 propagate——业务自定义
			// cfg.AutoCompact.Model 抛 InterruptError / ctx.Err() 时 AutoCompactRun
			// 已 propagate（不 wrap），caller 这里也必须直接返。原 `_` 吞掉破坏 HITL。
			// 业务错（非控制流）AutoCompact 内部已 wrap "auto compact: summarize: %w"——
			// caller 走 fallback "忽略压缩" 主链路继续（保 best-effort 优化语义）。
			if ccErr != nil {
				if _, isInt := compose_v2.IsInterrupt(ccErr); isInt {
					return nil, ccErr
				}
				if ctx.Err() != nil {
					return nil, ccErr
				}
				// 业务错——记录但继续主链路（compacted 通常仍是原 messages，下层照走）
				slog.WarnContext(ctx, "auto compact failed, continue with original messages",
					"agent", cfg.Name, "err", ccErr)
			}
			if compacted != nil {
				msgs = compacted
			}
		}

		llm := resolveModel(ctx, cfg.Model) // per-call WithRunModel 覆盖主推理 model（ctx 无则构造期）
		// per-call WithRunModelOptions（thinking 等 provider 参数按次控制，透传到每次 model 调用）。拷一份再往上加：
		// 存的是调用方传进来的那份切片，它有余量时直接 append 会写进调用方的底层数组（调用方在几个 Run 之间
		// 复用同一份选项时就是数据竞争）。
		modelRunOpts := append([]model.Option(nil), resolveModelOpts(ctx)...)
		// 调用方强制的 tool_choice=required 只管到模型第一次调工具（第一批解锁了新工具的 tool_search 不算，见 basicToolsUsedCh）为止：
		// 每轮都强制的话模型永远不能以文本收尾，一路烧到软降落轮。配了 WithAllowedTools 指定工具的，白名单照旧、
		// 只是不再逼着调。框架自己为结构化
		// 输出设的 required 排在这之后、不受影响（那条路的出口就是 final_output，每轮都要）。
		if toolsUsed(ctx) {
			modelRunOpts = append(modelRunOpts, resetRequiredToolChoice)
		}
		// 结构化输出：能力允许时强制 tool_choice=required，逼模型经 final_output 收尾（不自由
		// 文本绕过）。qwen-3-coder 等不支持 required 的变体（capability 显式 false）跳过——靠工具
		// 描述强指示 + 校验回炉兜底。
		if cfg.OutputSchema != nil && supportsForcedToolChoice(llm) {
			modelRunOpts = append(modelRunOpts, model.WithToolChoice(schema.ToolChoiceRequired))
		}

		// Skill 集成：仅 toolCall 模式注入 activate_skill 工具——adaptive / pre_match
		// 模式由 buildInitialMessages 已预激活 skill 进 sysPrompt，无需再让 LLM 调工具。
		toolkit := withFrameworkTools(&cfg, cfg.Toolkit, backgroundScopeFrom(ctx) != nil)
		if cfg.Skill != nil && cfg.Skill.SkillBox != nil && skillActivationMode(cfg.Skill) == skill.SkillActivationModeToolCall {
			// P2.B/C: 注入 fork 模式 resolver + parent model fallback
			actTool := newSkillTool(cfg.Skill.SkillBox).
				withListingBudget(cfg.SkillListingBudgetChars, cfg.SkillListingPerEntryChars).
				withForkResolvers(cfg.SkillSubAgentResolver, cfg.SkillModelResolver, cfg.Model, cfg.SkillBodyMaxChars)
			if toolkit == nil {
				toolkit = tool.NewToolkit()
			} else {
				// shallow clone 避免污染原始 toolkit
				cloned := tool.NewToolkit()
				for _, t := range toolkit.List() {
					cloned.Register(t)
				}
				toolkit = cloned
				// 综合 audit H3 fix（R5 改进）: 同名工具检测 warn 已移到 makeModelFn 闭包外
				// 构造期一次。这里只 Register 不 warn 避免每轮重复噪声。
			}
			toolkit.Register(actTool)
		}

		// run_skill_action：有可调 action 的 skill → 挂触发工具（独立于 activation mode——actions
		// 是单独机制；与 activate_skill 职责正交不冲突）。
		if cfg.Skill != nil && cfg.Skill.SkillBox != nil && boxHasActions(cfg.Skill.SkillBox) {
			if toolkit == nil {
				toolkit = tool.NewToolkit()
			} else if toolkit == cfg.Toolkit {
				cloned := tool.NewToolkit()
				for _, t := range toolkit.List() {
					cloned.Register(t)
				}
				toolkit = cloned
			}
			toolkit.Register(newSkillActionTool(cfg.Skill.SkillBox))
		}

		// 转交 / 路由能力：**被授予**才有 transfer_to_agent / route_to（授予已在 Run 入口自查过
		// 是发给本对象的，见 buildComposeGraph）。没被授予的 agent 连这件工具都不存在——
		// 它产生不出转交 / 路由信号，而不是产生了再被下游拒。
		if grants.handoff != nil || grants.route != nil {
			if toolkit == nil {
				toolkit = tool.NewToolkit()
			} else if toolkit == cfg.Toolkit {
				cloned := tool.NewToolkit()
				for _, t := range toolkit.List() {
					cloned.Register(t)
				}
				toolkit = cloned
			}
			// 绑定侧也带标记：ToolFilter / 渐进披露在这一侧据它豁免框架工具（见 frameworkMintedTool）。
			if grants.handoff != nil {
				toolkit.Register(markFrameworkTool(newHandoffTransferTool(grants.handoff.candidates)))
			}
			if grants.route != nil {
				toolkit.Register(markFrameworkTool(newRouteTool()))
			}
		}

		// 工具级渐进披露：注册 tool_search meta-tool + 把未揭示的 deferred 工具过滤出 bind list
		// （核心强制，业内统一——未揭示工具不喂 LLM）。已解锁集只读 model 节点从 channel 读出来放进 ctx 的那份
		// （见 revealedNames）。
		var tsRevealed revealedNames
		var tsDeferredNames map[string]bool
		if ts := cfg.ToolSearch; ts != nil && ts.Enabled && ts.Deferred != nil {
			if deferred := ts.collectDeferred(toolkit); len(deferred) > 0 {
				if toolkit == cfg.Toolkit { // 还没被 skill clone 过 → clone 防污染原始 toolkit
					cloned := tool.NewToolkit()
					for _, t := range toolkit.List() {
						cloned.Register(t)
					}
					toolkit = cloned
				}
				toolkit.Register(newToolSearchTool(ts, deferred))
				tsRevealed = newRevealedNames(revealedFromChannelCtx(ctx))
				tsDeferredNames = make(map[string]bool, len(deferred))
				for _, d := range deferred {
					tsDeferredNames[d.Name()] = true
				}
			}
		}

		finalRound := isFinalRound(ctx)
		toolsBound := false
		if toolkit != nil {
			tools := toolkit.ToolInfos()
			// 这一轮有没有裁剪（ToolFilter 返回了名单、或软降落）：有的话交给模型的那些就是 tools 节点只执行的那些（见 toolBinding）
			restricted := false
			if tsDeferredNames != nil {
				tools = filterVisibleTools(tools, tsDeferredNames, tsRevealed)
			}
			// 业务工具裁剪：ToolFilter 返本轮允许的工具名（nil = 不裁）。与 tool_search
			// 的 filterVisibleTools 正交、同位置叠加——这里在 bind 前再裁一层。
			if cfg.ToolFilter != nil {
				// 倒着找最后一条**真实**消息——跳过框架合成的记账文本。
				// 它在 Handoff 共享视图（Raw/Rewrite）下会恰好排在末尾：那条路径不 append
				// input（input 已在 prior 里），于是 msgs 以 prior 收尾，而 prior 末条正是
				// 前接那条 transfer tool_call assistant、被洗成记账文本。业务这个回调的典型
				// 用法是「按用户这轮说了什么裁工具」，拿到框架记账等于拿不到信息。
				// 见 IsUnresolvedCallsRecord：凡是框架**挑一条消息代表某件事**的地方都要跳。
				// 框架以 user 角色追加的提示同理（后台任务通知、重复调用提醒、inline skill 正文，见 IsFrameworkNote）：
				// 它们排在用户输入或工具结果之后，通知最常落在下一次 Run 的第一轮——恰好是这个回调唯一看得到用户原话的那一轮。
				var lastInput *schema.Message
				for i := len(msgs) - 1; i >= 0; i-- {
					if msgs[i] != nil && !IsUnresolvedCallsRecord(msgs[i]) && !IsFrameworkNote(msgs[i]) {
						lastInput = msgs[i]
						break
					}
				}
				if allowed := cfg.ToolFilter(ctx, lastInput); allowed != nil {
					tools = filterToolsByName(tools, allowed, toolkit)
					restricted = true
				}
			}
			// **软降落**（轮次预算的最后一轮，见 [withFinalRound]）分两档：
			//
			//   - **没配结构化输出：工具集照旧，tool_choice 设成 none**（在下面定 tool_choice 那处）。工具集要跟前几轮
			//     是同一套：Anthropic 给 thinking 块签名时绑定它产生时的前缀（system、tools、之前的每条消息），回放时
			//     对不上就按 `prefix_mismatch_behavior` 处理——摘掉工具 ⟹ 之前每个 thinking 块都对不上，默认 drop_block
			//     下 API 丢掉第一个对不上的块及其后的全部 thinking（开着会话历史时下一次 Run 工具恢复、这一轮的块又对不上，
			//     之后每次请求都再丢一遍），error 档下这一轮直接 400。tool_choice 不在那个前缀里，Anthropic 的建议就是保留
			//     工具、发 none。prompt cache 也少失效一截：tool_choice 变了只失效 messages 段，摘工具是 tools 段起全部失效。
			//     这一轮模型仍硬调的工具不执行（provider 不强制执行 none、业务自己的 ChatModel 不认它），见 makeToolsFn。
			//   - **配了结构化输出：只留框架那件 `final_output`**，其余（含全部框架工具）一律摘掉——它是结构化答复的唯一
			//     投递通道，none 会把它一起关掉。⚠️ **这对 Anthropic 就是一次工具集变化**，代价同上一档说的：之前的
			//     thinking 块全部作废。要保住前缀得留着整套工具、用 tool_choice 指定只能调 final_output，而 Opus 5.5 对
			//     指定工具（tool / any）返回 400。
			//
			// ⚠️ **配了结构化输出时不能整个 `toolkit = nil`**——那样会把 `final_output` 一起摘掉，而它是
			// `WithOutputSchema` 下**唯一的答案投递通道**。实测那样做的后果有两层：
			//   - 请求体畸形：`tool_choice=required` 更早无条件填上（本函数上方），而 tools
			//     字段没了 —— 「必须调工具」配「没有工具」。
			//   - **熔断信号被替换**：模型只能自由文本 ⟹ postamble 抛 `StructuredOutputError`
			//     ⟹ 补 `MaxIterationsError` 那段带 `err == nil` 前置、根本轮不到 ⟹ 业务
			//     `errors.As(&mie)` 检测预算耗尽，**配了 OutputSchema 就永远检测不到**；
			//     而错误正文写着「已尝试 0 次」，把排查方向指反。软降落还一个答案都没交付。
			//
			// ⚠️ **不能用 [filterToolsByName]**——它带框架工具（isFrameworkMintedIn）的**恒豁免**，
			// 那是给 `WithToolFilter` 用的语义（裁业务工具、框架工具恒保留，防业务白名单漏带
			// 导致问用户/技能静默失效）。结构化输出那档要的**正好相反**：要摘的就是 `activate_skill` /
			// `tool_search` / `run_skill_action` / `ask_user`，而它们全在那道豁免里。
			// 一度直接借它，实测「白名单」一行没生效——软降落轮留下的正是点名要摘的那三个，
			// 模型继续调 `ask_user`，`maxRounds=2` 却问了 12 次不收敛。**这就是「一个函数被
			// 两种语义共用」那条规约的实例，修法是给第二种语义自己的载体、不是给共用的加参数。**
			//
			// ⚠️ **为什么只放行 `final_output`，不放行 `transfer_to_agent` / `route_to`**（没配结构化输出时它们
			// 照旧绑着，但 none 关着、硬调了也不执行；配了时直接摘）：它们确实也是「靠工具收尾」，但**下游不认**——
			// 本次 Run 挂着 `MaxIterationsError`，而 Handoff 见错整个作废那次尝试（转交根本没发生、`State` 停在原
			// agent）、Routing 走降级分支擦掉路由信号（用户拿到 fallback 的答案而 `err == nil`）。放行等于让模型把
			// 最后一格花在一个**注定被扔掉**的动作上。要么修下游让它们认，要么别给——**不该维持「给了又不认」的
			// 中间态**。已登记 backlog。
			if finalRound && cfg.OutputSchema != nil {
				tools = keepOnlyTools(tools, FinalOutputToolName)
			}
			if sink := toolBindingSink(ctx); sink != nil && (restricted || isFinalRound(ctx)) {
				sink.Restricted = true
				sink.Names = make([]string, 0, len(tools))
				for _, ti := range tools {
					sink.Names = append(sink.Names, ti.Name)
				}
			}
			if len(tools) > 0 {
				bound, err := llm.WithTools(tools)
				if err != nil {
					return nil, fmt.Errorf("model.WithTools: %w", err)
				}
				llm = bound
				toolsBound = true
			}
		}
		switch {
		case !toolsBound:
			// toolkit 这条路这一轮没绑上工具（白名单滤光、本来就没配）：调用方强制的 tool_choice 一并摘掉——按次经
			// model.WithTools 传、或模型预先绑好的工具不算，那些工具本 agent 派发不了，强制调也没用。「必须调工具」配
			// 「没有工具」provider 直接拒绝（required 返 400）；各家 client 不都替你兜（openai client 兜了，业务自己的
			// ChatModel 未必）。软降落轮同样不设 none：没有工具可关。
			modelRunOpts = append(modelRunOpts, clearToolChoice)
		case finalRound && cfg.OutputSchema == nil:
			// 软降落轮（没配结构化输出）：工具照旧绑着，用 none 关掉工具调用（理由见上面软降落那段）。排在调用方强制的
			// tool_choice 之后，盖掉它——照发 required 的话模型被逼着调工具，而这一轮调的工具不执行。
			modelRunOpts = append(modelRunOpts, model.WithToolChoice(schema.ToolChoiceNone))
		}

		// callLLM 优先 Stream（chunk 实时投递）；流不支持时 fallback 到 Generate。
		// chunk 通过 EventSink 实时发出，前端按 run_id 累积渲染（type=chunk）；
		// 最终聚合 assistant 消息走正常 modelFn return → ChannelWrite → tools 节点判断。
		//
		// runID 由 modelFn 管理，retry 前 emit reset_stream 时附 run_id
		// 让前端精确清此 stream group buffer（不带 run_id 时前端要么清全部要么漏清）。
		var lastRunID string
		// minimal AgentInfo for hook.OnModelEndStream——modelFn 闭包看不到 round，
		// 业务想要精确 round 通过 OnModelStart/End 配合。
		streamHookInfo := &hook.AgentInfo{Name: cfg.Name, Type: "base"}
		callLLM := func(in []*schema.Message) (*schema.Message, error) {
			runID := newUUID()
			lastRunID = runID
			// 「agent 是什么」那一段（模板渲染 + sysPrompt + skill hint/body）在这里 prepend。
			//
			// **位置是承重的**：必须在 callLLM 内、也就是 MicroCompact / AutoCompact /
			// ReactiveCompact 全跑完之后——三层压缩动的是 msgs / curMsgs，这里拼的东西
			// 它们一条也看不见。挪到 modelFn 早段就等于把它送回被压缩的那条流里，
			// 而那正是 BasicAgentPreambleOutput 要消除的东西。
			//
			// 顺序：AGENTS.md（harness 层）在最前，然后是 agent 自己的那段，再是对话。
			if len(preamble) > 0 {
				in = append(append([]*schema.Message{}, preamble...), in...)
			}
			// AGENTS.md transient 注入：prepend 为首条 system（harness 层，排任务 system 之前）。
			// 在 callLLM 内 = 每次调用（含 ReactiveCompact retry）都注入；压缩路径的 msgs/curMsgs
			// 不含它，故压缩看不到、纯 transient（不持久 / 不进 cp）。
			if agentsMD != "" {
				in = append([]*schema.Message{schema.SystemMessage(agentsMD)}, in...)
			}
			return streamOrGenerate(ctx, llm, in, cfg.Name, runID, cfg.Hooks, streamHookInfo, modelRunOpts...)
		}

		out, err := callLLM(msgs)
		if err == nil {
			return out, nil
		}

		// 第 3 层 ReactiveCompact 兜底——循环最多 MaxAttempts 次
		if cfg.ReactiveCompact == nil || !cfg.ReactiveCompact.Enabled {
			return nil, err
		}
		maxAttempts := cfg.ReactiveCompact.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = defaultReactiveMaxAttempts
		}
		origErr := err
		curMsgs := msgs
		for attempt := 0; attempt < maxAttempts && IsPromptTooLongError(err); attempt++ {
			// **按 token 判进展，不按条数**。
			//
			// 条数判据（`len(compacted) >= len(curMsgs)`）在「只会删组」的世界里够用——删组
			// 必然减条数。但它有两个问题：
			//
			//   - **既有边界**：`emergencyDropOldestGroups` 删完若首条是 assistant 会**补一条
			//     user marker**。丢 1 条补 1 条时条数不变 ⟹ 当场 break，一次**有效**的压缩被丢掉。
			//   - **挡住下一步**：内容级压缩（把工具结果换占位符 / 截断超长单条）的特征恰恰是
			//     **条数不变、体积变小** ⟹ 必然命中 `>=`，接进来也白接，而且静默。
			//
			// 换成 token 之后两条一起解决，且对「只会删组」的现状行为等价（删组必然减 token）。
			beforeTok := compose_v2.EstimateMessageTokens(curMsgs)
			compacted, retryErr := ReactiveCompactRun(ctx, cfg.ReactiveCompact, curMsgs, err)
			if retryErr != nil {
				return nil, fmt.Errorf("ReactiveCompact attempt %d: %w (orig: %v)", attempt+1, retryErr, origErr)
			}
			if compacted == nil || compose_v2.EstimateMessageTokens(compacted) >= beforeTok {
				// 没法再压缩了——退出循环让原 err 返出
				break
			}
			curMsgs = compacted

			// retry 前通知前端清此 run_id chunk buffer——
			// 第一次 attempt 已 emit 部分 chunks（典型 typed Stream 时），第二次
			// Generate 产新流避免"半截答案 + 全新答案"重复渲染。
			//
			// 附 run_id（即将作废的 stream group id）让前端精确按 run_id 丢 buffer。
			if sink := compose_v2.GetEventSink(ctx); sink != nil {
				sink.Send(compose_v2.Event{
					Type:    "reset_stream",
					Content: fmt.Sprintf("ReactiveCompact retry attempt %d/%d", attempt+1, maxAttempts),
					Metadata: map[string]any{
						"reason":      "prompt_too_long",
						"attempt":     attempt + 1,
						"max":         maxAttempts,
						"compact_len": len(curMsgs),
						"orig_len":    len(msgs),
						"run_id":      lastRunID,
						"agent":       cfg.Name,
					},
				})
			}
			// Phase 2 Sub-4: compact:reactive:retry event (跟 reset_stream 并行 —
			// reset_stream 是前端清 chunk buffer, compact retry 是 compact 状态观测).
			// sink-gated——接了 observability 才发.
			if hasEventSink(ctx) {
				EmitCompactionEvent(ctx, &CompactionAction{
					Layer: CompactionLayerReactive,
					Type:  CompactionActionRetry,
					Retry: &CompactionRetryAction{
						Attempt:       attempt + 1,
						MaxAttempts:   maxAttempts,
						CompactedMsgs: len(curMsgs),
						OrigMsgs:      len(msgs),
						Reason:        "prompt_too_long",
						RunID:         lastRunID,
					},
				})
			}
			out, err = callLLM(curMsgs)
			if err == nil {
				return out, nil
			}
		}
		return nil, err
	}
}

// composeDispatchToolkit 拼这次 Run 派发用的工具表：业务的工具 + 框架的 opt-in 工具（见 withFrameworkTools）
// + skill / tool_search 这些框架工具 + 按授予挂的转交 / 路由（没被授予时挂软失败桩）。绑定侧（makeModelFn）
// 每轮另拼一份，两份注册同一组框架工具。类型标注（pushToolCategorizerCtx）也按这一份查——那是实际会被
// 派发的实例。
func (a *BasicAgent) composeDispatchToolkit(grants memberGrants, background bool) *tool.Toolkit {
	cfg := a.config
	// 只要有用户 toolkit 或 toolCall 模式 skill 集成，就需要 toolsFn。
	// adaptive / pre_match 不注 activate_skill 工具，纯靠用户 toolkit 决定是否需要 toolsFn。
	skillNeedsTool := cfg.Skill != nil && cfg.Skill.SkillBox != nil && skillActivationMode(cfg.Skill) == skill.SkillActivationModeToolCall
	// 有 action 的 skill 也需要 toolsFn 来 dispatch run_skill_action（独立于 activation mode）——
	// 否则 adaptive/pre_match + 纯 actions + 无 user toolkit 时 LLM bind 了 run_skill_action 却无处执行。
	skillHasActions := cfg.Skill != nil && cfg.Skill.SkillBox != nil && boxHasActions(cfg.Skill.SkillBox)
	// **toolsFn 恒非 nil**：被授予时要跑真 transfer 工具，没被授予时要跑软失败桩
	// （见 newHandoffTransferStub——没有它，一次幻觉/残留的 transfer_to_agent 会把
	// 整个 Run 炸掉）。候选完全没有业务 toolkit 是常态（前台只负责分诊），旧的早退
	// 恰好覆盖的就是这批。
	//
	// 图拓扑不受影响：tools 节点本来就恒存在，nil 只在**运行到**它时才报错。
	// 内置 skill 工具的 lazy 初始化——modelFn 内 toolkit 是 clone 的，toolsFn
	// 这里独立维护一份 dispatch 用 toolkit。adaptive / pre_match 不注 activate_skill。
	dispatchToolkit := withFrameworkTools(&cfg, cfg.Toolkit, background)
	if skillNeedsTool {
		// 综合 audit H3 fix: 检测业务自带同名工具，warn 提示被框架版覆盖
		// （toolsFn 构造期一次性 warn——modelFn 已 per-Run warn 过同样信号，但这里
		// 是不同的 dispatchToolkit 副本所以也要看一遍）
		if dispatchToolkit != nil {
			if existing := dispatchToolkit.Get(activateSkillToolName); existing != nil {
				slog.Warn("framework activate_skill tool overrides user-registered tool with same name in dispatch toolkit",
					"agent", cfg.Name, "mode", "toolCall")
			}
		}
		// P2.B/C: dispatch toolkit 的 actTool 实际执行 fork 模式（toolkit Call 路径），必须注 resolver
		actTool := newSkillTool(cfg.Skill.SkillBox).
			withListingBudget(cfg.SkillListingBudgetChars, cfg.SkillListingPerEntryChars).
			withForkResolvers(cfg.SkillSubAgentResolver, cfg.SkillModelResolver, cfg.Model, cfg.SkillBodyMaxChars)
		if dispatchToolkit == nil {
			dispatchToolkit = tool.NewToolkit()
		} else {
			cloned := tool.NewToolkit()
			for _, t := range dispatchToolkit.List() {
				cloned.Register(t)
			}
			dispatchToolkit = cloned
		}
		dispatchToolkit.Register(actTool)
	}
	// run_skill_action 也注进 dispatch toolkit 才能执行（与 modelFn bind 那份独立副本；独立于
	// activation mode）。
	if skillHasActions {
		if dispatchToolkit == nil {
			dispatchToolkit = tool.NewToolkit()
		} else if dispatchToolkit == cfg.Toolkit {
			cloned := tool.NewToolkit()
			for _, t := range dispatchToolkit.List() {
				cloned.Register(t)
			}
			dispatchToolkit = cloned
		}
		dispatchToolkit.Register(newSkillActionTool(cfg.Skill.SkillBox))
	}
	// 转交能力：被授予挂真工具，没被授予挂**只在 dispatch 侧存在**的软失败桩
	// （bind 侧那份见 makeModelFn——没被授予时 LLM 压根看不到这件工具）。
	if dispatchToolkit == nil {
		dispatchToolkit = tool.NewToolkit()
	} else if dispatchToolkit == cfg.Toolkit {
		cloned := tool.NewToolkit()
		for _, t := range dispatchToolkit.List() {
			cloned.Register(t)
		}
		dispatchToolkit = cloned
	}
	if grants.handoff != nil {
		// 被授予 ⟹ 框架版必须赢：业务拿一件同名工具把转交挡掉，等于编排的路由被
		// 一个下层配置悄悄改写（base 在 opt-in 时也是框架版覆盖，同款）。
		dispatchToolkit.Register(markFrameworkTool(newHandoffTransferTool(grants.handoff.candidates)))
	} else if cfg.Toolkit == nil || cfg.Toolkit.Get(HandoffTransferToolName) == nil {
		// 没被授予 ⟹ 只在**业务没占用这个名字**时挂软失败桩。
		// 少了这个条件，任何一个恰好叫 transfer_to_agent 的业务工具会被框架静默顶掉
		// ——哪怕这个 agent 跟 handoff 毫无关系（实测 base 调 1 次 / 无条件挂桩后 0 次）。
		dispatchToolkit.Register(markMinted(newHandoffTransferStub()))
	}
	// 路由能力同一个形状：被授予挂真工具（框架版必须赢），没被授予且业务没占这个名字时挂软失败桩。
	if grants.route != nil {
		dispatchToolkit.Register(markFrameworkTool(newRouteTool()))
	} else if cfg.Toolkit == nil || cfg.Toolkit.Get(RouteTool) == nil {
		dispatchToolkit.Register(markMinted(newRouteToolStub()))
	}
	// 放在转交 / 路由之后：延迟名单要在框架工具都挂好之后算——业务同名工具届时已被顶掉，进不了名单
	// （绑定侧同一个顺序）。
	// 工具级渐进披露：tool_search meta-tool 也注进 dispatch toolkit 才能被执行（与 modelFn 注进
	// bind list 那份是两个独立副本）。deferred 工具本身一直在 toolkit 里（执行不受披露限制，受限的
	// 只是「未揭示前不进 LLM bind list」+ 下面 runOne 的 fail-closed）。
	// 到这里 dispatchToolkit 已是本次 Run 自己的副本（上面挂转交 / 路由时必定拷过），直接注册。
	if ts := cfg.ToolSearch; ts != nil && ts.Enabled && ts.Deferred != nil {
		if deferred := ts.collectDeferred(dispatchToolkit); len(deferred) > 0 {
			dispatchToolkit.Register(newToolSearchTool(ts, deferred))
		}
	}
	return dispatchToolkit
}

// makeToolsFn 构造 BasicAgentToolFn——按 toolCalls 调 toolkit 返 tool_result。
//
// **本 fn 不跑任何压缩层**：第 1 层 MicroCompact 唯一的调用点在 modelFn 入口
// （见那边的注释），因为清「跨轮累积的旧 tool_result」需要看到累积历史，而 tools 节点
// 只拿到本批 toolCalls、不知历史——在这里跑等于只清刚产出的那批（永远是新的，no-op）。
// grants 同 makeModelFn：被授予时 dispatch toolkit 也要有 transfer_to_agent / route_to，否则 LLM
// 那侧 bind 得到、执行这侧却报 tool-not-found（modelFn 的 bind toolkit 与本 fn 的
// dispatch toolkit 是两份独立副本，见下面 dispatchToolkit 的注释）。
func (a *BasicAgent) makeToolsFn(grants memberGrants, dispatchToolkit *tool.Toolkit) BasicAgentToolFn {
	cfg := a.config
	// 派发侧「哪些工具被延迟了」只认 composeDispatchToolkit 实际算出来的那份名单（挂在框架那件 tool_search 上），
	// 不重新跑 Deferred 谓词：两边各判一次的话，算名单时的例外（tool_search 这个名字、框架工具）这边抄不全——
	// 业务工具叫 tool_search 又是唯一被延迟的那件时，模型看得到它、调用却一律被拒，一直循环到轮次上限（审计实测）。
	// 框架那件没注册 ⟹ 没有工具被延迟、不拦。
	var dispatchDeferred map[string]bool
	if tst, ok := dispatchToolkit.Get(toolSearchToolName).(*toolSearchTool); ok {
		dispatchDeferred = make(map[string]bool, len(tst.deferred))
		for _, d := range tst.deferred {
			dispatchDeferred[d.Name()] = true
		}
	}
	// **ISSUE-81 fix**：toolsFn fanout 化——N>=2 个 tool_call 时 goroutine 并发跑
	// 各 tc 让总延迟 ~= max(tc_i) 而非 sum(tc_i)。
	//
	// **why now**：3 前置都修完——ISSUE-78 NS hash 隔离 + ISSUE-79 半共享 sess +
	// ISSUE-80 lane events——并发 N 个 sub-agent 时 cp / events / values 都 race-free。
	//
	// **关键不变量**：
	//  1. tool_result 顺序按 toolCalls idx 保留（OpenAI/Claude 要求 tool_call ↔ tool_result 配对）
	//  2. P2.D inline body deferredBodyMsgs 全 tool_result 之后统一 append (R16 audit)
	//  3. 多 InterruptError 用 MergeInterrupts 聚合返单 ie 让 graph_loop record 全部 query
	//  4. 业务 tool 必须 reentrant (tool.Tool godoc 协议)
	//
	// **why N=1 仍 inline**：避免 spawn goroutine 开销，业务最常见场景 (ReAct
	// 单 tc/轮) 零开销。
	return func(ctx context.Context, toolCalls []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error) {
		if len(toolCalls) == 0 {
			return nil, nil
		}
		// 每个调用的执行用它自己的 ctx（见 BasicAgentToolFn）；对不上就是调用方的接线错了，重跑必然一样。
		if len(callCtxs) != len(toolCalls) {
			return nil, fmt.Errorf("tools: %d 个 tool_call 配了 %d 个调用 ctx: %w", len(toolCalls), len(callCtxs), compose_v2.ErrRunAborted)
		}
		// Phase 2 Sub-5f: fire fanout event (N>=2 时, EmitFanoutEvent 内部判).
		// observability 的 OnNodeEvent 识别 → 记 crew.fanout.branches 直方图.
		EmitFanoutEvent(ctx, FanoutKindToolCalls, len(toolCalls))
		// 执行闸认的已解锁集：这一批开始前已提交的那份（tools 节点从 channel 读出来放进 ctx），并发分支之前取一次。
		var revealedAtEntry revealedNames
		if dispatchDeferred != nil {
			revealedAtEntry = newRevealedNames(revealedFromChannelCtx(ctx))
		}
		// 这一批是不是软降落轮的模型调用产出的（tools 节点从 channel 读出来放进 ctx，见 [withFinalRound]）。从节点交进来的
		// ctx 读，不从每个调用的 ctx 读——那份经过 OnToolStart 钩子采纳。
		finalRound := isFinalRound(ctx)

		// outcome 单 tc 处理结果——并发收集后按 idx 排序保 tool_result 配对顺序
		type outcome struct {
			toolMsg *schema.Message // 必有 (除非 err)
			bodyMsg *schema.Message // P2.D inline body UserMessage (可选)
			// signals：这次调用做出的上行信号（转交 / 路由决定，可选）。原样收下、不在这里判作废——
			// 作废看 OnToolEnd 改写之后的结果，见 tool_batch_signals.go。
			signals compose_v2.Signals
			err     error
			// replayed：产出是从工具账重放的（这一批上次没整体成功，这个调用当时已经跑完）。
			replayed bool
		}

		// ── 已跑成功的 tool_call 不重跑（节点私有中断态，见 compose_v2/node_state.go）──
		//
		// tools 节点是原子的：一个 tc 抛中断 → 整节点 writes 作废 → Resume 时 N 个 tc
		// **全部**从头再跑。已成功的那几个被重执行——下单 / 发消息 / 扣款各来第二遍。
		// 框架此前的说法是「业务工具自负幂等」，那是把框架能解的问题推给每个业务工具，
		// 而 HTTP 调用 / 写库 / 发消息天然不幂等。
		//
		// memo 存「本轮哪些 tc 已跑完、产出是什么」，键是节点执行身份（NodeID+实例+visit，
		// 框架算，见 node_state.go）⟹ 跨中断稳定、成功后自然不可达、不需要清除点。
		//
		// ⚠️ **hook 行为不变**：`OnToolStart/End` 仍对全部 tc 各 fire 一次（它们在
		// basic_builder.go 的 toolsFn 外层、看不到 memo）。Resume 时 hook 重复 fire 是
		// **既有行为**（今天所有 tc 都重跑、hook 也都重 fire），memo 只把「工具函数被重
		// 调用」这半消掉。重放的那几个在 OnToolEnd 的 `ToolOutput.Extra` 上标
		// [ToolReplayedExtraKey]（经 toolBatch.ends 交给 tools 节点）：它们在上一次没整体成功
		// 的那一批里已经收过尾（见 endSettledToolCalls），按调用计数或计时的钩子据它跳过。这一批处理完之后定下的
		// 那几个（见 finalizeToolMemo）例外：续跑时不再处理，钩子也不触发（见 fixedToolResults）。
		//
		// ⚠️ **判据是「这次调用的副作用能不能重放」，不是工具名字**（见 shouldMemoize）：
		// memo 的契约原本是「重放产出、不重放副作用」，可有些工具的产出**不止 tool_result**
		// ——`tool_search` 把解锁记在这一批的解锁记录上（tools 节点成功后才写进 channel），那**不进 cp**
		// ⟹ memo 覆盖不到，命中跳过工具体的后果是**静默**的：err=nil、tool_result 齐全，只是这一批
		// 什么都没解锁。这类没做出上行信号的框架元工具仍被排除在 memo 外，靠函数体重跑把那半补回来。
		//
		// `transfer_to_agent` / `route_to` **不在此列**：它们做出的决定是上行信号，每个 tool_call
		// 期间收到的信号随 memo 进 cp、命中时原样交出，于是可以进 memo（契约扩成「重放产出 +
		// 交出当时做出的决定」，见 toolMemoEntry.Signals）。
		// `activate_skill` 不进这份账；它 fork 出去的 sub-agent 已完成的那次，由嵌套调用自己的
		// 完成记录挡住重跑（见 agent_v2.runNestedCall），没完成的续自己的子存档。
		memo := &toolCallMemo{Done: map[string]*toolMemoEntry{}}
		// **只判 prev != nil，不再附加 `prev.Done != nil`**：memo 现在还装着审批记录
		// （`Approved`），而审批发生在工具跑完**之前**——那一刻 `Done` 恰好是空的。
		// 加那个条件会把「只批过、还没跑完」的那份整个丢掉，于是 Resume 时重新要审批
		// （实测：`TestApproval_SelfInterruptingTool_NoReapproval` 直接红）。
		if prev, ok := compose_v2.GetNodeState[*toolCallMemo](ctx); ok && prev != nil {
			memo = prev
			if memo.Done == nil {
				memo.Done = map[string]*toolMemoEntry{}
			}
		}
		// memoKey 三维：`(tc.ID, 批内下标, 工具名)`。
		//
		//   - **下标**：框架别处不假设 tc.ID 唯一（同批两个 tc 用同一个 ID 时，只按 ID 记
		//     会让后跑的命中前一个的条目、静默不执行——业务以为发了两条实际一条，而
		//     tool_result 条数 / 配对 / err 全部正常）。下标的跨 Resume 稳定性有两重依据：
		//     (1) `WithCurrentToolCallContext(ctx, tc.ID, i)` 拿同一个 i 算 InterruptID，
		//     不稳的话 HITL 答案早就配不上了；(2) toolCalls 取自 `basicMessagesCh` 里那条
		//     assistant 消息，而 channel state 就是 cp 的内容——Resume 从 cp 恢复的是同一
		//     条消息、同一个顺序。
		//   - **工具名**：`OnToolStart` 能改写工具名，改写只在这一批第一次交给工具执行时生效、
		//     续跑沿用（见 fixToolCallContent），同一个 (ID, 下标) 在几次执行之间不换工具。名字
		//     仍进键，让「A 的产出不被当成 B 的返回值贴回去」不依赖那份记录——改写曾经在续跑时
		//     生效，那时这一维是唯一的防线（命中就把 A 的产出贴给 B，带上名字后退化成**不命中**、
		//     重跑，错在安全侧）。现在它是冗余的第二道：固定内容之下走不到，没有测试覆盖。
		memoKey := toolMemoKey
		// fanout 下 N 个 goroutine 并发写 memo——map 写必须串行（cm 那侧自己有锁，
		// 这里保护的是 map 本身 + 「改完立刻存」这两步的原子性）。
		//
		// ⚠️ **它实际保护的范围比「这张 map」大**：持锁期间会 `json.Marshal(memo)`
		// （SaveNodeState 内），而那会遍历**整棵对象图**。于是有一条隐式不变量——
		// **凡是被 memo 引用的 *schema.Message，都不许在 fanout 窗口里、memoMu 之外被改**。
		// memo 引用的就是交出去的那条（下面命中分支 → outcomes[i] → 下游），tools 节点在这一批都返回之后会
		// 就地改它（钩子改写、按大小处理，见 basic_builder.go）——那时 fanout 已经结束、没有并发的
		// SaveNodeState，存下的字节也是改之前当场序列化的。哪天有人在 fanout 窗口内给 tool_result 补个
		// annotation（markToolFailure / ReturnDirectAnnotation / SkillBodyAnnotation 都是这种形状），就是一条真 race。
		var memoMu sync.Mutex
		rememberDone := func(tc schema.ToolCall, idx int, oc outcome) {
			// tc.ID 为空（有的 provider 不回 tool_call ID）照样记：memoKey 带批内下标，同一批两个
			// 空 ID 的 tool_call 不会撞，续跑时从存档恢复的是同一批、同一个顺序。
			if oc.err != nil || oc.toolMsg == nil {
				return // 只记产出了 tool_result 的：失败/中断的要重跑
			}
			// **判据是「这次调用的副作用能不能重放」，不是「工具叫什么名字」。**
			//
			// 做出了上行信号的（转交 / 路由决定）：那半随记账进 cp，命中时原样交出 ⟹ 可以进
			// 记账，函数体不必重跑。
			//
			// 没做出上行信号的框架元工具：产出不止 tool_result 而框架无从重放（如 tool_search
			// 把解锁记在这一批的解锁记录上，那不进 cp）⟹ 排除在记账外，靠函数体重跑补回来。
			// 命中跳过的后果是**静默**的：err=nil、tool_result 齐全，只是事没发生。
			//
			// 业务工具一律进记账：它里面经嵌套调用做出的决定随账走；它在工具体外留下的、
			// 不进 cp 的副作用，命中时同样不会再做一次（那正是记账的目的）。
			if !shouldMemoize(isFrameworkMintedIn(dispatchToolkit, tc.Function.Name), oc.signals) {
				return
			}
			memoMu.Lock()
			defer memoMu.Unlock()
			memo.Done[memoKey(tc.ID, idx, tc.Function.Name)] = &toolMemoEntry{
				ToolMsg: oc.toolMsg, BodyMsg: oc.bodyMsg, Signals: oc.signals,
			}
			// **每成功一个就存**，不等整节点 return——中断是 error 路径，等不到。
			if err := compose_v2.SaveNodeState(ctx, memo); err != nil {
				slog.WarnContext(ctx, "agent_v2: 存 tool_call 进度失败，该工具 Resume 时会重跑",
					"tool_call_id", tc.ID, "err", err)
			}
		}

		runOne := func(callCtx context.Context, tc schema.ToolCall, idx int) (oc outcome) {
			// **上次已跑成功的直接返产出，不再调工具**——这是整套 memo 的目的：
			// 不重放副作用。放在最前面（连 panic recover 都不需要，因为不执行任何业务代码）。
			// tc 的内容在这一批第一次执行时就定下了（tools 节点交进来之前，见 fixToolCallContent），
			// 工具账、审批、执行用的都是这一份。
			memoMu.Lock()
			hit := memo.Done[memoKey(tc.ID, idx, tc.Function.Name)]
			memoMu.Unlock()
			if hit != nil && hit.ToolMsg != nil {
				// 函数体被跳过 ⟹ 它当时做出的决定也不会再做一次，这里原样交出。
				compose_v2.EmitSignals(callCtx, hit.Signals)
				return outcome{toolMsg: hit.ToolMsg, bodyMsg: hit.BodyMsg, replayed: true}
			}
			// review-v6 H-02 fix: 业务 Tool.Call panic 转 outcome.err 不冒泡——
			// ISSUE-81 N>=2 fanout 路径下 goroutine 内未 recover 的 panic 让 Go runtime
			// 直接 fatal crash 整进程 (主进程死亡 + 在跑请求全丢 + cp save 全跳过).
			// N=1 inline 路径下 safeNodeRun 兜底但 fanout 路径绕开 — runOne 内统一防护
			// 让两路径一致 (业务 tool panic 等价业务 tool 返 error, LLM 看到错误 ReAct
			// 继续 / 业务 hook OnError 上报).
			defer func() {
				if r := recover(); r != nil {
					pe := panicerr.New(r)
					oc = outcome{err: panicerr.Wrap(fmt.Sprintf("tool %q panic recovered", tc.Function.Name), pe)}
					panicerr.Log(callCtx, slog.LevelError, "agent_v2: 工具 panic，已转成错误", pe, panicerr.KeyError, oc.err.Error())
				}
			}()
			// **软降落轮模型硬调的工具不执行**（见 [withFinalRound]）：那一轮发的是 tool_choice=none（配了结构化输出时
			// 只绑着 final_output），模型照样调了——provider 不强制执行 none、或业务自己的 ChatModel 不认它。执行的话就是
			// 在轮次预算之外多跑一批副作用，而结果再没有一轮可以消化。以失败结果交回（同下面的 tool_not_revealed：调了这一轮
			// 不让调的工具是失败的 tool call）；回到 model 节点时以 MaxIterationsError 结束，不再调模型（顶层 Run 上是引擎的
			// maxVisit 先拦下，续跑时是 model 节点开头「每个任务至多一个软降落轮」那道闸）。
			// 放在审批之前：框架的审批不会去问一个注定不执行的调用。业务的 OnToolStart / OnToolEnd 照常触发（钩子在工具函数
			// 之外、每个调用都触发）——在 OnToolStart 里做 HITL 确认的业务会被问到这次调用。
			//
			// 放行的只有框架那件 final_output：配了结构化输出时那一轮只给它留了它，它是答复的唯一投递通道。按实例认、不按
			// 名字——没配 WithOutputSchema 时 final_output 这个名字是业务工具，跟别的业务工具一样不执行。
			if finalRound && !isFrameworkFinalOutput(dispatchToolkit, tc.Function.Name) {
				return outcome{toolMsg: failedToolResult(tc.ID, "round_budget_exhausted", "轮次预算已用完，这次工具调用没有执行。")}
			}
			// 这次派发的那件是不是框架铸的——下面渐进披露、审批、超时、中间件几道闸都据它豁免框架工具，
			// 判的是实例不是名字（见 frameworkMintedTool）。
			minted := isFrameworkMintedIn(dispatchToolkit, tc.Function.Name)
			args := map[string]any{}
			if tc.Function.Arguments != "" {
				// review-v6 M-09 fix: JSON 解析失败不能 silent 吞 — 返 tool_result 含
				// 错误反馈让 LLM 看到 + 重试 (LLM emit 坏 JSON 罕见但真触发时 tool 拿空
				// args 静默执行错误 input).
				if jerr := json.Unmarshal([]byte(tc.Function.Arguments), &args); jerr != nil {
					// finding 3: 框架级拒绝也是**失败的 tool call**（工具压根没跑起来）→
					// 同样标记让 observability 把工具 span 记成失败。对比 approval 的 tool_denied
					// **不标**——那是用户的主动决定、不是错误，计进错误率会污染告警。
					msg := fmt.Sprintf("tool_call.Arguments JSON 解析失败: %v", jerr)
					return outcome{toolMsg: failedToolResult(tc.ID, "invalid_arguments", msg)}
				}
			}
			// fail-closed：LLM 调了未揭示的 deferred 工具（不在它的 bind list 却硬调=幻觉）→
			// 拒执行、返反馈让它先 tool_search 解锁。判的是派发侧那份延迟名单（见上面 dispatchDeferred）——
			// 框架铸的工具不在名单里（collectDeferred 跳过它们），防宽 Deferred 谓词误配置下框架工具进了
			// bind list 却在 dispatch 侧被拒执行。
			//
			// 闸认的是这一批开始前已提交的解锁集（revealedAtEntry），不认同一批里检索正在记的：同一批里
			// 检索并直调刚解锁的工具一律被拒，从下一批起可调（模型在这一批里本来就看不到它）。曾经认的是
			// 同一批里检索正在往里加的那份，放不放行取决于并发调度。
			if dispatchDeferred[tc.Function.Name] && !revealedAtEntry[tc.Function.Name] {
				// finding 3: 同 invalid_arguments——LLM 硬调未解锁工具是失败的 tool call
				msg := fmt.Sprintf("工具 %q 未解锁——请先调 tool_search(query) 检索解锁后再调用。", tc.Function.Name)
				return outcome{toolMsg: failedToolResult(tc.ID, "tool_not_revealed", msg)}
			}
			// 这一轮没交给模型的工具不执行（WithToolFilter 没放行，见 toolBinding）：模型凭上文记得的名字硬调时拿到失败
			// 结果。判的是产出这一批的那一轮的绑定（channel 里的那份，续跑也是它）；ToolFilter 豁免的框架工具留在绑定里、
			// 照常执行。软降落轮的硬调已经被上面那道闸（round_budget_exhausted）拦下。排在审批之前——不让人去批一个这一轮
			// 不可用的工具。
			if !toolBindingFrom(ctx).allows(tc.Function.Name) {
				msg := fmt.Sprintf("工具 %q 这一轮不可用（没有提供给你），改用提供给你的工具，或者直接回答。", tc.Function.Name)
				return outcome{toolMsg: failedToolResult(tc.ID, ToolNotAvailableErrCode, msg)}
			}
			// **工具审批 gate（harness P1）**：业务工具执行前查审批决策链（ApprovalMode!=Auto
			// 或有 heuristic 时才有实际开销，默认 Auto 路径零开销）。Ask 未 resume → InterruptError
			// （下方 fanout 收集 + MergeInterrupts 多工具一次问完）；拒绝 → 工具级 error 回 LLM
			// （复用 tool_denied 范式，不 abort 整 Run）。
			// 「这次调用的审批结论」与「批过没有」都在本节点的私有中断态里（与 tool memo 同一个
			// 载体、同一把键）——原先「批过没有」按位置 ID 存 session 上，而位置 ID 跨任务重复、
			// session 又活得比任务长 ⟹ 跨任务泄漏。详见 `toolCallMemo.Approved` 的注释。
			//
			// **RMW 必须在 memoMu 里**：fanout 下多 goroutine 并发读改写同一个 memo，
			// 「读 map → 加一项 → 写回」跨 goroutine 会丢条目（跟 memo.Done 同一个理由）。
			ak := memoKey(tc.ID, idx, tc.Function.Name)
			// 同工具钩子的相位名（toolHookCtx）：带工具名、tool_call ID 与批内下标，在这次执行里唯一。
			phaseOf := func(kind string) string {
				return kind + "\x00" + tc.Function.Name + "\x00" + tc.ID + "\x00" + strconv.Itoa(idx)
			}
			callCtx = withCallApproval(callCtx, callApprovalFns{
				heuristicPhase: phaseOf("approval_heuristic"),
				askPhase:       phaseOf("approval_ask"),
				is: func() bool {
					memoMu.Lock()
					defer memoMu.Unlock()
					return memo.Approved[ak]
				},
				mark: func() {
					memoMu.Lock()
					defer memoMu.Unlock()
					if memo.Approved == nil {
						memo.Approved = map[string]bool{}
					}
					memo.Approved[ak] = true
					// **立刻落盘**，不等整节点 return——审批之后工具自身还可能再抛中断，
					// 那是 error 路径、等不到节点成功。同 memo.Done 的理由。
					// 存不下只 warn：退化成「Resume 时重新问一次」，安全侧。
					if err := compose_v2.SaveNodeState(ctx, memo); err != nil {
						slog.WarnContext(ctx, "agent_v2: 存审批记录失败，该工具 Resume 时会重新要审批",
							"tool_call_id", tc.ID, "err", err)
					}
				},
				decided: func() (ApprovalDecision, bool) {
					memoMu.Lock()
					defer memoMu.Unlock()
					d, ok := memo.Decided[ak]
					return d, ok
				},
				decide: func(d ApprovalDecision) {
					memoMu.Lock()
					defer memoMu.Unlock()
					if memo.Decided == nil {
						memo.Decided = map[string]ApprovalDecision{}
					}
					memo.Decided[ak] = d
					// **立刻落盘**：结论是 Ask 时接下来就抛审批中断，是 error 路径、等不到节点成功；
					// 是 Allow 时工具自己也可能中断或失败。存不下只 warn：退化成续跑时按那时的配置
					// 重新形成结论（它若翻转，就是 approvalDecisionOf 注释里那几种事）。
					if err := compose_v2.SaveNodeState(ctx, memo); err != nil {
						slog.WarnContext(ctx, "agent_v2: 存审批结论失败，该工具续跑时会重新形成审批结论",
							"tool_call_id", tc.ID, "err", err)
					}
				},
			})
			// 只读放行档要知道这次派发的那件工具有没有声明只读——按实例认（tool.IsReadOnly），不按名字
			readOnly := tool.IsReadOnly(dispatchToolkit.Get(tc.Function.Name))
			if ar := checkToolApproval(callCtx, &cfg, tc, args, minted, readOnly); ar.err != nil {
				return outcome{err: ar.err}
			} else if !ar.proceed {
				return outcome{toolMsg: ar.denyMsg}
			}
			// 工具体的私有态按工具名再分一层（tool_call 的分域只有 (tc.ID, 批内下标)）：工具名在这一批
			// 第一次执行时定下、续跑不变（见 fixToolCallContent），这一层让「新工具不读旧工具的记录」
			// 不依赖那份记录，跟工具账（memoKey 带名字）同一个理由。改写曾经在续跑时生效，那时新工具
			// 读到旧工具记的「做过了」，事没做（实测）。现在同样是冗余的第二道，没有测试覆盖。
			callCtx = compose_v2.WithNodeStateSealed(callCtx, "tool\x00"+tc.Function.Name)
			// invoke 跑这次调用：中间件链 + 工具。中间件链在这儿——memo / 渐进披露 / 审批**之内**，工具**之外**（理由见
			// ToolMiddleware godoc「顺序」）。框架自带工具不过链：它们的真产出不在 tool_result 里，短路掉是静默失败
			// （同 memo 的判定，别改回去）。
			//
			// c 是交给工具的 ctx；guard 是判「中间件吞掉的错能不能吞」用的调用方 ctx（见 guardControlFlow）——同步执行
			// 时是工具超时派生之前那个，别把框架自己设的 deadline 当成调用方喊停；后台执行时就是后台任务自己的 ctx。
			invoke := func(c, guard context.Context) (*tool.Result, error) {
				callTool := ToolEndpoint(func(ctx context.Context, name string, a map[string]any) (*tool.Result, error) {
					return dispatchToolkit.Call(ctx, name, a)
				})
				if len(cfg.ToolMiddlewares) == 0 || minted {
					return callTool(c, tc.Function.Name, args)
				}
				// 中间件交给 next 的 ctx 跟钩子返回的 ctx 一样采纳（compose_v2.EnterHook，规则同 CallHook）：
				// 交过来的是存下来的 ctx 时，工具体照样在这次调用的位置上跑。曾经原样交给工具体：中间件为了
				// 挂 span 沿用第一次存下的 ctx，工具里的提问每次续跑都换一个 ID、永远答不完（实测）。
				mwCtx, adopt := compose_v2.EnterHook(c)
				adopting := ToolEndpoint(func(ctx context.Context, name string, a map[string]any) (*tool.Result, error) {
					return callTool(adopt(ctx), name, a)
				})
				guarded, check := guardControlFlow(guard, adopting)
				return check(chainToolMiddlewares(cfg.ToolMiddlewares, guarded)(mwCtx, tc.Function.Name, args))
			}
			var res *tool.Result
			var err error
			ran := false
			// **可后台的调用**（见 background_task.go）：在后台 goroutine 里跑，YieldAfter 之内返回就跟同步执行一样往下走；
			// 到点了还没返回就转入后台，这次的 tool_result 换成任务 id。
			if sc := backgroundScopeFrom(callCtx); backgroundEligible(sc, minted, dispatchToolkit.Get(tc.Function.Name)) {
				// 后台任务结束时没有 OnToolEnd 这一步，当场按大小处理（deferSize=false）
				render := func(c context.Context, r *tool.Result) bgRendered {
					rc := renderToolContent(c, &cfg, false, tc.Function.Name, r, false)
					return bgRendered{content: rc.content, spillID: rc.spillID, spillInfo: rc.spillInfo}
				}
				bo, started := sc.cfg.Manager.call(callCtx, sc, tc, cfg.ToolTimeout, invoke, render)
				if started {
					ran = true
					if bo.task != nil {
						return outcome{toolMsg: backgroundHandleMessage(tc, bo.task, sc.cfg.yieldAfter())}
					}
					if bo.timedOut > 0 {
						return outcome{toolMsg: failedToolResult(tc.ID, ToolTimeoutErrCode,
							fmt.Sprintf("工具调用超时（超过 %s 未返回），本次调用已放弃", bo.timedOut))}
					}
					res, err = bo.res, bo.err
				}
			}
			if !ran {
				// **工具级超时**：把 deadline 交给工具，超时后**照样等它返回**，只在它返回时
				// 说一句「你超了」。理由见 [WithToolTimeout]——不假装能掐断一个不看 ctx 的工具。
				// ⚠️ **留住派生前那个 ctx**：超时判定要区分「工具自己的 deadline 到了」和
				// 「调用方的 ctx 死了」，而派生 ctx 的 `Err()` 两种情况下都非 nil
				// （派生 ctx 继承父的 deadline）。手上没有父 ctx 就分不出来。
				parentCtx := callCtx
				if cfg.ToolTimeout > 0 && !minted {
					var cancel context.CancelFunc
					callCtx, cancel = context.WithTimeout(callCtx, cfg.ToolTimeout)
					defer cancel()
				}
				res, err = invoke(callCtx, parentCtx)
				// **框架自己设的 deadline 到期 ⟹ 框架自己收尾**，不指望工具返对东西。
				//
				// 本仓契约是「工具返 error = 硬失败（整节点挂）、返 tool.Failure = 给模型看」。
				// 而超时是**这次工具调用**的失败、不是整轮的失败——该让模型看见并换条路。
				// 响应 ctx 的工具会返 `context.DeadlineExceeded`（那是 error 档，会挂整轮），
				// 不响应的会返一个迟到的成功。**两种都不是我们要的**，所以在这儿统一改写。
				if toolDeadlineExceeded(parentCtx, callCtx, cfg.ToolTimeout, err) {
					if err == nil && res != nil && res.Success {
						// 超了 deadline 还返回成功 ⟹ 这个工具没看 ctx。出声——**一个不生效
						// 又不出声的超时比没有超时更坏**，业务会以为有保护。
						slog.WarnContext(ctx, "agent_v2: 工具超过 WithToolTimeout 仍返回成功——"+
							"它没有响应 ctx 取消，这次调用实际没有被超时打断（结果已按超时丢弃）",
							"tool", tc.Function.Name, "timeout", cfg.ToolTimeout, "tool_call_id", tc.ID)
					}
					// **标失败**：超时是这次工具调用的失败，不标的话 `OnToolEnd` 收到
					// `Success=true`、工具 span 记成 success、`gen_ai.execute_tool.duration` 不带 `error.type`——
					// 最需要被观测到的工具失败反而是唯一不可见的（合成入口收敛见
					// failedToolResult godoc）。
					return outcome{toolMsg: failedToolResult(tc.ID, ToolTimeoutErrCode,
						fmt.Sprintf("工具调用超时（超过 %s 未返回），本次调用已放弃", cfg.ToolTimeout))}
				}
			}
			if err != nil {
				return outcome{err: err}
			}
			// 开了转存时按大小处理留到 OnToolEnd 之后（deferSize=true，见 renderToolContent）：转存的要是钩子改写之后的那份
			rendered := renderToolContent(callCtx, &cfg, minted, tc.Function.Name, res, true)
			content, toolErr := rendered.content, rendered.toolErr

			// **P2.D inline body 协议**：仅 framework activate_skill 成功 + InlineSkillBody 启用时
			_, isSkillTool := dispatchToolkit.Get(tc.Function.Name).(*skillTool) // 按实例认：派发的是框架那件 activate_skill
			isFrameworkActivateSkill := isSkillTool && res != nil && res.Error == nil
			isForkExecuted := res != nil && res.Metadata != nil && res.Metadata[SkillForkExecutedMetaKey] == true
			if isFrameworkActivateSkill && cfg.InlineSkillBody && !isForkExecuted {
				skillName, _ := args["skill"].(string)
				body := content
				// R22 audit HIGH-1 fix: SkillBodyMaxChars 截断
				maxChars := cfg.SkillBodyMaxChars
				if maxChars <= 0 {
					maxChars = DefaultSkillBodyMaxChars
				}
				if len(body) > maxChars {
					body = truncateWithEllipsis(body, maxChars)
					slog.WarnContext(callCtx, "P2.D inline skill body truncated to fit SkillBodyMaxChars",
						"skill", skillName, "original_chars", len(content), "truncated_chars", len(body), "max", maxChars)
				}
				if body == "" {
					slog.WarnContext(callCtx, "P2.D inline skill body empty after truncation, body UserMessage will only contain system_reminder header",
						"skill", skillName, "max", maxChars, "original_chars", len(content))
				}
				ackContent := fmt.Sprintf("Skill %q activated; see next system reminder for content.", skillName)
				toolMsg := schema.ToolResultMessage(tc.ID, ackContent)
				// R19 audit HIGH-1 fix: terse ack toolMsg 也打 SkillBodyAnnotation
				toolMsg.Annotations = map[string]any{SkillBodyAnnotation: true}
				bodyMsg := schema.UserMessage(fmt.Sprintf("[system_reminder] Skill %q content:\n%s", skillName, body))
				bodyMsg.Annotations = map[string]any{SkillBodyAnnotation: true}
				return outcome{toolMsg: toolMsg, bodyMsg: bodyMsg}
			}

			toolMsg := schema.ToolResultMessage(tc.ID, content)
			annotateSizedToolResult(toolMsg, sizedToolContent{origBytes: rendered.origBytes, spillID: rendered.spillID, spillInfo: rendered.spillInfo})
			if rendered.pending {
				if toolMsg.Annotations == nil {
					toolMsg.Annotations = map[string]any{}
				}
				toolMsg.Annotations[toolResultSizePendingAnnotation] = true
			}
			// issue-95 finding 3: 业务工具声明的失败经 annotation 跨层带到 toolsNode 的
			// hook fire 层——否则 OnToolEnd 恒收到 Success=true，工具 span 永远不会记成失败。
			if toolErr != nil {
				// 失败的 final_output 算不算一次失败的尝试由 toolsNode 在 OnToolEnd 之后统一判（见
				// finalOutputRetryAnnotation 的 godoc），这里只带上失败信息。
				markToolFailure(toolMsg, toolErr.Code, toolErr.Message)
			}
			// P1.C: 给成功激活的 activate_skill tool_result 打 marker（派发的是框架那件才打，不含 fork）
			if isFrameworkActivateSkill && !isForkExecuted {
				if toolMsg.Annotations == nil {
					toolMsg.Annotations = map[string]any{}
				}
				toolMsg.Annotations[SkillBodyAnnotation] = true
			}
			// return-direct：工具声明 tool.Result.Metadata[ReturnDirectMetaKey]=true 时打
			// ReturnDirectAnnotation——compose_v2 toolsNode 读它短路 ReAct、不回 model
			// （如 HandoffAgent transfer / exit 类工具）。照 SkillForkExecutedMetaKey 同款 Metadata 信号范式。
			if res != nil && res.Metadata != nil {
				if rd, ok := res.Metadata[tool.ReturnDirectMetaKey].(bool); ok && rd {
					if toolMsg.Annotations == nil {
						toolMsg.Annotations = map[string]any{}
					}
					toolMsg.Annotations[ReturnDirectAnnotation] = true
					// 顺带记下是**哪个**工具一调即出——`schema.ToolResultMessage` 不存工具名
					// （见 ReturnDirectToolAnnotation godoc）。只给业务看是谁收的尾，框架判身份不读它
					// （名字业务占得到）：结构化输出那件看收结果时盖的 finalOutputAttemptAnnotation。
					toolMsg.Annotations[ReturnDirectToolAnnotation] = tc.Function.Name
					// 身份在**这一层**判掉（手上有 toolkit 和真实实例），消费侧读事实
					// 不再查名单——名字是开放集，业务占得到（见该常量 godoc）。
					toolMsg.Annotations[ReturnDirectMintedAnnotation] =
						isFrameworkControlToolIn(dispatchToolkit, tc.Function.Name)
				}
			}
			return outcome{toolMsg: toolMsg}
		}

		// runCall 跑一个 tool_call，并收下它这段执行做出的上行信号（转交 / 路由决定，见
		// side_signal.go）：调用成功时随产出一起记账、批末登记给 builder 的收集器；失败或中断时随它
		// 作废。命中记账时 runOne 交出当时记下的那些，同样在这里收下。
		//
		// 结果是失败的（超时、工具声明的 tool.Failure、中间件改成失败）也照收、不在这里作废：模型最后
		// 看到的是 OnToolEnd 改写之后的结果，作废按那个判（钩子把失败改成成功，决定就算数），见
		// tool_batch_signals.go。超时也不例外——工具调用是同步的，走到超时收尾时工具已经返回，这次
		// 调用期间做出的决定已经收全。记账里记的也是这一份原始的：这一批没处理完就停下、续跑命中时钩子
		// 照样 fire、按同一套规则重判；处理完之后定下的不再交给钩子（见 fixedToolResults）。
		runCall := func(callCtx context.Context, tc schema.ToolCall, idx int) outcome {
			var oc outcome
			sigs, _ := compose_v2.CaptureSignals(callCtx, func(c context.Context) error {
				oc = runOne(c, tc, idx)
				return oc.err
			})
			oc.signals = sigs
			return oc
		}

		outcomes := make([]outcome, len(toolCalls))
		if len(toolCalls) == 1 {
			// 单 tc inline 跑——零 goroutine 开销
			callCtx := compose_v2.WithCurrentToolCallContext(callCtxs[0], toolCalls[0].ID, 0)
			outcomes[0] = runCall(callCtx, toolCalls[0], 0)
			rememberDone(toolCalls[0], 0, outcomes[0])
		} else {
			// **ISSUE-81 fanout**：N>=2 goroutine 并发——各 tc 走独立 ctx 协议
			// （tc.ID 维度 NS + tc 下标维度 InterruptID + 独立 counter）
			var wg sync.WaitGroup
			for i, tc := range toolCalls {
				wg.Add(1)
				go func(i int, tc schema.ToolCall) {
					defer wg.Done()
					// **第三条出路守卫**：`runtime.Goexit` 会跑 defer 但不是 panic——
					// runOne 的 recover（H-02）接不住，`outcomes[i]` 留在零值 ⟹ 收集
					// 循环 `err==nil && toolMsg==nil` 静默跳过 ⟹ 这个 tc **没有配对的
					// tool_result**（下一轮消息流对真 provider 是 400/422），而 Run
					// 报完全成功。旗必须由普通语句置位（defer 里置位 Goexit 也会跑）。
					// 归节点失败档，跟 tool panic 同一路：Resume 重试整轮，memo 保
					// 已成功的不重放。
					//
					// ⚠️ **这一支刻意不先判 panic，跟 compose_v2 那两支不一样**——那边
					// 「写结论前先 recover」是对的（三方一致规律），这里不加是权衡后的：
					// 若 panic 发生在 `runOne` 之外（`rememberDone` 内部），这支会先写一句
					// 「Goexit」误标，然后 panic 继续传播。但**这个 goroutine 之上没有任何
					// recover**（`defer wg.Done()` 只是计数），所以那种 panic 在 Go 里是
					// **进程级 fatal**——`outcomes[i]` 那句误标没有任何读者。
					// 而加 `recover()` 会吞掉 panic（改变行为）或 re-panic（**截断真实的
					// panic 堆栈**，那是唯一的诊断信息）。用一句没人读的误标换掉堆栈，
					// 不划算。实测两版差异确认过：误标存在但不可观测。
					finished := false
					defer func() {
						if !finished {
							outcomes[i] = outcome{err: fmt.Errorf(
								"tool %q (id=%s) 异常终止：既没正常返回也没 panic"+
									"（runtime.Goexit——典型来源是 zap.WithFatalHook、testify 的 require.*）",
								tc.Function.Name, tc.ID)}
						}
					}()
					callCtx := compose_v2.WithCurrentToolCallContext(callCtxs[i], tc.ID, i)
					outcomes[i] = runCall(callCtx, tc, i)
					rememberDone(tc, i, outcomes[i])
					finished = true
				}(i, tc)
			}
			wg.Wait()
		}

		// 收集 results + 每个分支的原始错——按 idx 顺序遍历保 tool_result 配对顺序。
		//
		// **攒的是原始错误链，不是抠出来的裸 ie**：`IsInterrupt` 抠出来的是裸
		// `*InterruptError`，而原始链上可能还挂着**第二个** sentinel（自研 / 远程 agent 挂上去的；
		// 引擎出口曾把 `ErrCheckpointStale` 跟中断一起 `%w`，现在档没写成时返
		// `ErrCheckpointNotPersisted`、中断不在链上）。只留裸 ie 会把它整条剥掉，下游（a2a/server、
		// agui、observability）对它的专属处置就全部失效。
		results := make([]*schema.Message, 0, len(toolCalls))
		var deferredBodyMsgs []*schema.Message
		toolErrs := make([]error, 0, len(toolCalls))

		for i, oc := range outcomes {
			if oc.err != nil {
				toolErrs = append(toolErrs, oc.err)
				continue
			}
			if oc.toolMsg != nil {
				// 框架结构化输出那件 final_output 的调用在这一层认（手上有 toolkit 和真实实例），盖成事实交给
				// toolsNode——它按名字认的话，没配结构化输出时业务那件同名工具也被当成一次尝试（见
				// finalOutputAttemptAnnotation）。参数非法那条路（工具没跑起来）同样盖：那也是一次失败的尝试。
				// 在 wg.Wait() 之后的单个 goroutine 里改；final_output 不进工具账，改的不是记账里共享的那份消息。
				if i < len(toolCalls) && isFrameworkFinalOutput(dispatchToolkit, toolCalls[i].Function.Name) {
					if oc.toolMsg.Annotations == nil {
						oc.toolMsg.Annotations = map[string]any{}
					}
					oc.toolMsg.Annotations[finalOutputAttemptAnnotation] = true
				}
				// 框架那件 tool_search 的调用同样在这一层认、盖章（见 toolSearchCallAnnotation）。它也不进工具账。
				if i < len(toolCalls) {
					if _, ok := dispatchToolkit.Get(toolCalls[i].Function.Name).(*toolSearchTool); ok {
						if oc.toolMsg.Annotations == nil {
							oc.toolMsg.Annotations = map[string]any{}
						}
						oc.toolMsg.Annotations[toolSearchCallAnnotation] = true
					}
				}
				results = append(results, oc.toolMsg)
			}
			if oc.bodyMsg != nil {
				deferredBodyMsgs = append(deferredBodyMsgs, oc.bodyMsg)
			}
			// ── 工具重复调用检测（见 RepeatToolConfig）──
			//
			// **在这儿数，不在执行之前**：这条路径是「工具已经有结果了」，而**被审批
			// 拒掉的调用也走到这儿**（Deny 返的是 outcome{toolMsg: denyMsg}）——
			// 「模型死命捶一个被拒的工具」恰恰是最该打断的循环。
			//
			// **框架工具直接跳过 = 对链透明**：既不计数也不重置。否则模型只要在两次
			// 重复之间插一个 activate_skill 就能把链洗白。
			if cfg.RepeatTool != nil && cfg.RepeatTool.Enabled && i < len(toolCalls) {
				tc := toolCalls[i]
				if !isFrameworkMintedIn(dispatchToolkit, tc.Function.Name) {
					if rm := observeRepeatToolCall(repeatTurnFrom(ctx), cfg.RepeatTool,
						tc.Function.Name, tc.Function.Arguments); rm != nil {
						deferredBodyMsgs = append(deferredBodyMsgs, rm)
					}
				}
			}
		}

		// 挑一个错报上去：**判据走 compose_v2.PickFanoutError，不在这儿再写一份**。
		//
		// 这里曾经是逐字手写的第三份同款三分支仲裁（单中断返原始错 / 多中断 Join /
		// 否则第一个业务错）。行为上完全冗余——接之前做过等价性对拍，全量套件 498 次
		// 调用两边逐字相同（含中断 102 次、走 merge 分支 12 次）。而它跟着漏掉了「终局
		// 失败严格支配中断」那一档：`AgentAsTool` 包的子 agent 返 abort、兄弟工具同时
		// 中断时，人会被问一个注定答不成的问题，答完 Resume 才 abort 并把答案一起清掉。
		//
		// ⚠️ 多中断合并的**株连**在这条路上是真的：`errors.Is` 在 Join 上是 OR，一个中断分支的链上
		// 挂着 ErrCheckpointStale 就整轮判 stale，兄弟那条干净中断被一起判死、pending entry 一并清掉。
		// 这是有意的 fail-closed。子 agent 出口档没写成（`ErrCheckpointNotPersisted`）不靠这个：
		// PickFanoutError 给它单独一档，排在中断之前。
		// 各调用的结局交给 tools 节点（成不成都交）：没整体成功时它据此给已经有结局的调用收尾（OnToolEnd），整体成功时
		// 据此给重放的调用打标记（见 toolBatch.ends）
		if b := toolBatchFrom(ctx); b != nil {
			b.ends = make([]toolCallEnd, len(outcomes))
			for i, oc := range outcomes {
				b.ends[i] = toolCallEnd{msg: oc.toolMsg, err: oc.err, replayed: oc.replayed}
			}
		}
		if err := compose_v2.PickFanoutError(toolErrs); err != nil {
			return nil, err
		}

		// 各 tool_call 做出的上行信号登记给 builder 的收集器：它在 OnToolEnd 之后按钩子改写后的结果判
		// 作废、同一件事上按顺序第一个生效、交给本节点这次执行（见 tool_batch_signals.go）。没有收集器
		// 时（不经 builder 直接调本函数）在这里按工具自己的结果判、就地交出。
		perCall := make([]compose_v2.Signals, len(outcomes))
		for i, oc := range outcomes {
			perCall[i] = oc.signals
		}
		if b := toolBatchFrom(ctx); b != nil {
			b.perCall, b.filled = perCall, true
		} else {
			failed := make([]bool, len(toolCalls))
			for i := range failed {
				failed[i] = i < len(results) && isFailedToolResult(results[i])
			}
			finishToolBatchSignals(ctx, toolCalls, results, perCall, failed)
		}

		// R16 audit HIGH-1 fix: deferredBodyMsgs 全 tool_result 之后统一 append
		results = append(results, deferredBodyMsgs...)
		return results, nil
	}
}

// IsUnresolvedCallsRecord 判断这条消息是不是框架合成的记账文本（见
// UnresolvedCallsRecordAnnotation）。
//
// **凡是「扫 messages 挑最终答复」的地方都必须调它**——记账文本是「assistant + 零
// tool_call」，正好长得像一条最终答复。框架内已有三处这样的扫描，它们必须给出一致的
// 答案，否则同一条消息在 A 处被排除、在 B 处被当成答复：
//
//   - `observability` 的 `lastAssistantMessage` —— agent_output event 的内容
//
// **曾经还有两处**（`lastAssistant` / `BasicAgentFinalMessage`），它们已经不扫消息列表了
// ——最终产出改由决策点写进专用 channel，而记账文本由 `dropDanglingToolCalls` 在 preamble
// 侧合成、进不了那条 channel。也就是说那两处的防护从「靠谓词跳过」升级成了「结构上不可能」。
// 剩下这一处仍在扫 `out.Messages`（业务 hook 的入参），谓词仍然必要。
//
// 另有一处不是「挑答复」但同理的：`ToolFilter` 的 `lastInput`（挑「本轮最后一条真实
// 消息」喂给业务的工具裁剪回调）。**规则是同一条**——凡是框架**挑一条消息代表某件事**
// 的地方，都不能让这条记账文本冒充。
//
// 导出是为了第三处：它在另一个包里，没有导出谓词就只能自己再判一遍，而「同一判据的第
// N 份独立实现」正是这个项目反复栽的坑（配对判据曾有 6 份）。业务自己扫 messages 挑答复
// 时同样应当调它。
func IsUnresolvedCallsRecord(m *schema.Message) bool {
	if m == nil {
		return false
	}
	rec, _ := m.Annotations[UnresolvedCallsRecordAnnotation].(bool)
	return rec
}

// collectEvents 返回 startOffset 之后的 events（本次 Run 的增量）。
func collectEvents(session *RunSession, startOffset int) []*AgentEvent {
	all := session.Events()
	if startOffset >= len(all) {
		return nil
	}
	out := make([]*AgentEvent, len(all)-startOffset)
	copy(out, all[startOffset:])
	return out
}

// IsInterrupt 检查 err 是否含 InterruptError——caller HITL 路径用。
// 透传 compose_v2.IsInterrupt（chain unwrap）。
func IsInterrupt(err error) (*compose_v2.InterruptError, bool) {
	return compose_v2.IsInterrupt(err)
}

// isInterruptErr 是 streamOrGenerate 内 reset_stream metadata 用的 helper——
// 仅返 bool 给 metadata 标识本次错误是否 InterruptError 类型。
func isInterruptErr(err error) bool {
	_, ok := compose_v2.IsInterrupt(err)
	return ok
}

// streamOrGenerate 调 model.Stream 走 token-by-token 流式；模型不支持流式时改调 Generate。
// chunk 通过 ctx EventSink 实时投递让前端 type=chunk 渲染。
//
// **聚合契约**：流式各 chunk 累加到 finalContent + 用最后非 nil 的 ToolCalls/Role
// 作 final message 字段——避免 LLM 在 chunk 中途插入工具调用导致 final 与 chunk
// 表面内容不一致。最后聚合的 schema.Message 通过 modelFn return → 走 BasicAgent
// 正常 ReAct 循环（tools 节点 / 最终回答判断）。
//
// runID 由调用方（modelFn）传入而非内部生成：retry
// 路径需在外层 emit reset_stream 时附此 run_id 精确清前端 buffer。
//
// 流式行为契约：
//   - 模型不支持流式（Stream 返回 model.ErrStreamUnsupported，或返回 nil reader 且没有错误）
//     → 改调 Generate，无 chunk 投递
//   - Stream 的其他错误（限流、5xx、网络错误）原样返回，交给上层的重试与故障切换。改调
//     Generate 等于不看 Retry-After 立刻再打一遍；配了 fallback 时整条候选链会再走一遍
//   - OnModelEndStream 钩子报错时返回这个错，跟 OnModelStart / OnModelEnd 报错一样让这次
//     模型调用失败（改调 Generate 会让模型被调两次）
//   - chunk 投递通过 OrchestrationHooks.OnNodeEvent type=chunk + run_id 让前端去重 + 重组
//   - 最终聚合走正常 return path 不需要 ChannelWrite 路径改动
func streamOrGenerate(ctx context.Context, llm model.ChatModel, in []*schema.Message, agentName, runID string, hooks hook.Hooks, info *hook.AgentInfo, opts ...model.Option) (*schema.Message, error) {
	reader, err := llm.Stream(ctx, in, opts...)
	if errors.Is(err, model.ErrStreamUnsupported) || (err == nil && reader == nil) {
		return generateWithRunID(ctx, llm, in, runID, opts...)
	}
	if err != nil {
		return nil, err
	}

	// **OnModelEndStream hook 接入**：业务可在 Recv 链路插入观测/转换/审计
	// 路径：原始 reader → schemaStreamAdapter 包成 spec interface → hooks 链 wrap →
	// streamReaderToSchema 反向适配为业务消费的 *schema.StreamReader → 继续 reader.Recv 循环
	if hooks != nil {
		adapted := &schemaStreamAdapter{sr: reader}
		// 按 compose_v2.CallHook 的规则采纳（钩子返回 nil 时原样用入参——后续 GetEventSink(ctx) / Recv(ctx)
		// 不能 deref nil；报错时不采纳）：钩子返回别处缓存的 ctx 时，这次 Run 的流式片段照样发给这次执行的
		// EventSink。曾经直接换成钩子的，片段发到了另一次 Run 的连接上（实测）。
		var wrapped specModel.MessageStreamReader
		var hookErr error
		ctx, hookErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
			nctx, w, err := hooks.OnModelEndStream(c, info, adapted)
			wrapped = w
			return nctx, err
		})
		if hookErr != nil {
			if wrapped != nil {
				_ = wrapped.Close() // 钩子报错时它返回的流也不用了；关流的错误没有下文，忽略
			}
			reader.Close()
			return nil, fmt.Errorf("OnModelEndStream: %w", hookErr)
		}
		// 返回值是替换之后的流，nil 表示流没了：协议违例，跟钩子链（hook.ChainHooks）同一个判据。
		// 两处判据不同的话，同一个钩子单独挂能用、一接上观测（链式安装）就让每次模型调用失败。
		if wrapped == nil {
			reader.Close()
			return nil, errors.New("OnModelEndStream: hook returned nil reader without error")
		}
		// hook 返了同一 adapter（NoOp）→ 走原始 reader 直接消费（避免 goroutine 桥接开销）
		// hook 返了新 wrapped reader → 桥接到 schema.StreamReader 让 streamOrGenerate 消费
		if wrapped != specModel.MessageStreamReader(adapted) {
			reader = streamReaderToSchema(ctx, wrapped)
		}
	}
	defer reader.Close()

	sink := compose_v2.GetEventSink(ctx)
	var (
		finalContent   strings.Builder
		finalReasoning strings.Builder // thinking 模型流式 reasoning_content 聚合（ISSUE-89）
		finalRefusal   strings.Builder // 拒答增量（openai 客户端逐片段交出 delta.refusal）
		finalRole      schema.MessageRole
		// tool_call ContentBlocks 增量聚合
		// OpenAI 流式 tool_call 是 incremental——每 chunk delta 含 partial JSON arguments，
		// 按 tool_call.Id 配对累加（按 id concat 增量拼接）。
		toolCallByID = make(map[string]*schema.ContentBlock)
		toolCallIDs  []string // 保插入顺序避免 map 迭代乱序
		// 业务 chunk 的 Annotations 也要 merge 到最终 message——
		// 业务把信号写在 annotation 里时（RoutingAgent 分类器的 route_target、自定义 key）
		// 上层编排能看到。merge 策略 last-write-wins 让最后一 chunk
		// 的 annotation 覆盖之前——业务一般在最终 chunk 设 annotation 表达"提交"。
		mergedAnnotations map[string]any
		// finalUsage Round 8 集成验证 fix: stream 末尾 usage chunk (openai client
		// include_usage 返 choices 空 + Metadata.Usage 非空) 的 token. 聚合循环捕获,
		// finalize 设到 out.Metadata.Usage 让 OnModelEnd / observability token metric
		// 拿到. 之前聚合只取 Content/ToolCalls/Annotations 漏 Usage → stream 模式 token
		// 全失效 (真 deepseek 集成验证暴露).
		finalUsage *schema.MessageUsage
		// finalModel response model name (跨 provider 集成验证 fix) — observability
		// gen_ai.request.model / response.model 用. 聚合捕获, finalize 设 out.Metadata.Model.
		finalModel string
		// finalSystem provider 名 (gen_ai.provider.name) — model client 透传, 聚合捕获,
		// finalize 设 out.Metadata.System 让 OnModelEnd 记 gen_ai.provider.name span attr.
		finalSystem string
		// onlyChunk / chunkCount：流里只有一个片段时整条采用它（见循环之后）。
		onlyChunk  *schema.Message
		chunkCount int
	)
	chunksEmitted := 0 // 跟踪本次流是否已 emit chunks，错误路径用
	for {
		chunk, recvErr := reader.Recv(ctx)
		if recvErr != nil {
			// 只有生产方交出 io.EOF 才算流正常结束。schema.ErrStreamCanceled 表示流被关掉了，而这个
			// 循环读完之前自己不关流——是别人（OnModelEndStream 钩子、它起的超时保护）半路关的，已收到的
			// 片段不完整，按错误处理。曾经把它也当正常结束，半截内容成了最终答复、err == nil。钩子要在
			// 这里截断并把已有内容当完整输出，就在自己的 Recv 里返回 io.EOF。
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if errors.Is(recvErr, schema.ErrStreamCanceled) {
				recvErr = fmt.Errorf("%w (closed before it ended: by a hook, or by a producer that called Close instead of schema.CloseSend)", recvErr)
			}
			// 业务自定义 model wrapper 在
			// reader.Recv 中段抛 *InterruptError 时 chunks 已 emit，cp 不含
			// 对应 message（model fn 失败 writes 不产生）。Resume 后 modelNode
			// 重跑产新 run_id chunks——前端按 run_id 隔离自然不串台，但缺
			// reset_stream 信号让 UI 显式知道老 run_id 的 chunks 已"作废"。
			//
			// 与 R7 ReactiveCompact retry reset_stream 路径对称：err != EOF 时（流被别人关掉
			// 的 ErrStreamCanceled 也算）若已 emit chunks，发 reset_stream 让前端清老 run_id buffer。
			if sink != nil && chunksEmitted > 0 {
				sink.Send(compose_v2.Event{
					Type:    "reset_stream",
					Content: fmt.Sprintf("stream interrupted mid-flight: %v", recvErr),
					Metadata: map[string]any{
						"reason":         "stream_recv_error",
						"chunks_emitted": chunksEmitted,
						"run_id":         runID,
						"agent":          agentName,
						"is_interrupt":   isInterruptErr(recvErr),
					},
				})
			}
			return nil, fmt.Errorf("stream recv: %w", recvErr)
		}
		if chunk == nil {
			continue
		}
		chunkCount++
		if chunkCount == 1 {
			onlyChunk = chunk
		}
		if chunk.Role != "" {
			finalRole = chunk.Role
		}
		if chunk.Content != "" {
			finalContent.WriteString(chunk.Content)
		}
		if chunk.ReasoningContent != "" {
			finalReasoning.WriteString(chunk.ReasoningContent)
		}
		if chunk.Refusal != "" {
			finalRefusal.WriteString(chunk.Refusal)
		}
		// Round 8 集成验证 fix: 捕获 stream chunk 的 token (include_usage) + model name
		// (response model, observability gen_ai.request.model label 用).
		if chunk.Metadata != nil {
			if chunk.Metadata.Usage != nil {
				finalUsage = chunk.Metadata.Usage
			}
			if chunk.Metadata.Model != "" {
				finalModel = chunk.Metadata.Model
			}
			if chunk.Metadata.System != "" {
				finalSystem = chunk.Metadata.System
			}
		}
		// merge chunk Annotations 到最终 message（last-write-wins）——见 mergedAnnotations
		// 注释解释。注：framework 自管 keys（run_id）会在 final 末段 stamp，业务 keys
		// （route_target / 自定义）由此处 merge 保留。
		for k, v := range chunk.Annotations {
			if mergedAnnotations == nil {
				mergedAnnotations = make(map[string]any)
			}
			mergedAnnotations[k] = v
		}
		// 增量聚合 tool_call ContentBlocks（按 ID 配对累加 args）
		//
		// empty tool_call ID 时按出现顺序生成
		// 隐式 key 防止 N 个不同 tool_call 串台合并成 1 个 args 乱码。OpenAI
		// SDK spec 要求首 chunk 必带 id（model/openai/client.go indexMap 已兜底），
		// 但第三方 SDK 直接产 schema.Message stream 时若不回填 id 会触发本路径。
		//
		// 协议（业务接入新 SDK 必读）：
		//   - 首选：producer 保证 tool_call.Id 非空（OpenAI delta 模式首 chunk 必填）
		//   - 兜底：empty id chunks 按"位置"作 key（无法跨 chunk 配对，需 producer
		//     单 chunk 内自包含全 args）
		for _, cb := range chunk.ContentBlocks {
			if cb == nil || cb.Toolcall == nil {
				continue
			}
			id := cb.Toolcall.Id
			if id == "" {
				// 防御 fallback：按当前 toolCallIDs 长度作隐式位置 key
				// 注：此路径下 chunk 间无法配对累加 args——producer 必须单 chunk 内自包含
				id = fmt.Sprintf("__pos_%d__", len(toolCallIDs))
			}
			if existing, ok := toolCallByID[id]; ok {
				// 增量合并：args 拼接（OpenAI delta 模式），name 取最新非空
				if cb.Toolcall.Name != "" {
					existing.Toolcall.Name = cb.Toolcall.Name
				}
				existing.Toolcall.Arguments += cb.Toolcall.Arguments
			} else {
				// 首次见此 ID——深拷贝避免后续 mutation 串到 chunk
				cloned := &schema.ContentBlock{
					Type: cb.Type,
					Toolcall: &schema.ToolcallContentBlock{
						Id:        cb.Toolcall.Id,
						Name:      cb.Toolcall.Name,
						Arguments: cb.Toolcall.Arguments,
					},
				}
				toolCallByID[id] = cloned
				toolCallIDs = append(toolCallIDs, id)
			}
		}
		// 投递 chunk 给前端实时渲染
		if sink != nil && chunk.Content != "" {
			sink.Send(compose_v2.Event{
				Type:    "chunk",
				Content: chunk.Content,
				Metadata: map[string]any{
					"run_id": runID,
					"agent":  agentName,
				},
			})
			chunksEmitted++
		}
	}

	// 只有一个片段时它就是整条消息，整条采用。增量拼接是给多个片段用的：只拼内容、推理、拒答、工具调用、
	// 标注与用量，其余字段（消息 ID、非工具调用的内容块、结束原因……）拼不了就丢，同一个 ID
	// 的两个工具调用还会被并成一个。不支持流式的候选经 fallback 包成一个片段交过来时正是这种情况，
	// 走拼接的话拿到的跟直接调 Generate 不一样（实测两个 ID 都是 call_0 的工具调用并成了一个、
	// 参数首尾相接）。
	if chunkCount == 1 {
		out := withRunID(onlyChunk, runID)
		if out.Role == "" {
			out.Role = schema.RoleAssistant
		}
		return out, nil
	}
	if finalRole == "" {
		finalRole = schema.RoleAssistant
	}
	out := &schema.Message{
		Role:             finalRole,
		Content:          finalContent.String(),
		ReasoningContent: finalReasoning.String(), // thinking 模型多轮回传需要（ISSUE-89）
		Refusal:          finalRefusal.String(),
	}
	// 按插入顺序拼 ContentBlocks（保 LLM 看到的 tool_call 列表顺序确定）
	for _, id := range toolCallIDs {
		out.ContentBlocks = append(out.ContentBlocks, toolCallByID[id])
	}
	// 给最终消息打 run_id annotation——hook OnModelEnd 能取到，前端按 run_id 区分流批
	if out.Annotations == nil {
		out.Annotations = make(map[string]any)
	}
	// 先把 stream 累积的业务 annotations merge 进来——run_id 后写以保证 framework key 不被业务覆盖
	for k, v := range mergedAnnotations {
		out.Annotations[k] = v
	}
	out.Annotations[compose_v2.RunIDAnnotationKey] = runID
	// Round 8 集成验证 fix: finalize 设 out.Metadata 让 OnModelEnd 拿到 stream token +
	// model name (observability gen_ai.llm.tokens.total / gen_ai.request.model label).
	if finalUsage != nil || finalModel != "" || finalSystem != "" {
		out.Metadata = &schema.MessageMetadata{Usage: finalUsage, Model: finalModel, System: finalSystem}
	}
	return out, nil
}

// generateWithRunID 走 Generate 并给结果 stamp run_id annotation——与 streamOrGenerate 的
// Stream 聚合路径（out.Annotations["run_id"]）保持一致。让 OnModelEnd / 前端在非流式降级
// 路径也能按 run_id 区分本轮 message：并发编排（ParallelAgent / fanout）下多个 sub-agent
// 各自 Generate 时，缺 run_id 会让观测无法区分、agui 适配把多条最终答案补发到同一
// messageId 而交错错乱。
func generateWithRunID(ctx context.Context, llm model.ChatModel, in []*schema.Message, runID string, opts ...model.Option) (*schema.Message, error) {
	out, err := llm.Generate(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return withRunID(out, runID), nil
}

// withRunID 返回 msg 的浅拷贝，Annotations 复制一份后打上 run_id。
//
// 不就地 mutate 模型返回的实例——ChatModel 契约未保证每次返回全新 message，业务实现可能跨调用
// 复用同一指针（如缓存/去重 wrapper）；就地写在并发分支（ParallelAgent fanout 各自 Generate）下
// 会 concurrent map write，且会追溯改写已进 history 的别名消息的 run_id（4 层压缩分组键）。流式
// 只有一个片段时同理：那个片段可能是模型缓存着的实例。框架其余 annotation 写点
// （microcompact/session/handoff）同款 clone-then-write。
func withRunID(msg *schema.Message, runID string) *schema.Message {
	cloned := *msg
	cloned.Annotations = make(map[string]any, len(msg.Annotations)+1)
	for k, v := range msg.Annotations {
		cloned.Annotations[k] = v
	}
	cloned.Annotations[compose_v2.RunIDAnnotationKey] = runID
	return &cloned
}

// 编译期接口实现检查
var _ Agent = (*BasicAgent)(nil)
