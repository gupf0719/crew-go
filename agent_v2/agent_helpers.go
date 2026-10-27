package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/panicerr"
)

// recoverToErr 是 9 编排（BasicAgent + 8 编排）Run/Resume 顶部统一的 panic→error 兜底。
//
// audit²：业务 hook / NodeFunc panic 转 err 返业务，不让 Run goroutine
// crash —— 嵌套场景下 panic 直接冒泡父 graph_loop 会掀翻整个执行树。error 用 %w 保留
// 调用链，非 error 用 %v；并挂上 panic 的值与栈（compose_v2.PanicError）、记一条带栈的日志。
//
// 用法（必须 defer 直接调用 —— recover() 只在被 defer 直接调用的函数内生效，Go 语义）：
//
//	func (a *XxxAgent) Run(ctx context.Context, ...) (retEvents []*AgentEvent, retErr error) {
//	    defer recoverToErr(ctx, "XxxAgent.Run", &retErr)
//	    ...
//	}
func recoverToErr(ctx context.Context, what string, retErr *error) {
	if r := recover(); r != nil {
		pe := panicerr.New(r)
		*retErr = panicerr.Wrap(what+" panic recovered", pe)
		panicerr.Log(ctx, slog.LevelError, "agent_v2: 编排兜住 panic，已转成错误", pe, panicerr.KeyError, (*retErr).Error())
	}
}

// taskScopeKey 是「这次调用处在一个已经建立的任务里」的章，值是那个任务的 RunID，见
// [enclosingTask]。
type taskScopeKey struct{}

// withTaskScope 盖章：ctx 下面发起的调用都在 runID 这个任务里。
func withTaskScope(ctx context.Context, runID string) context.Context {
	return context.WithValue(ctx, taskScopeKey{}, runID)
}

// enclosingTask 报「这次调用所在的任务」：ctx 上有没有一个正在执行的任务，有的话它的身份
// （RunID）是什么。**任务的身份是 session 的 RunID**（见 RunSession.RunID：它就是存档的主索引）。
//
// 两个来源，前者更具体、先看：
//
//   - 节点执行范围（[compose_v2.NodeScopeSessionID]）：节点体、它的钩子、从节点体里调进来的嵌套图
//     都在那个节点所在那次 Run 的任务里，任务就是那次 Run 的 sid（业务直接跑 compose_v2 图、节点里
//     再调 agent 时没有编排入口盖章，靠的就是这一条）；
//   - 任务章：编排的 Run / Resume 入口盖（它下面的钩子、节点都继承），嵌套调用的执行器替被调方盖
//     （[runNestedCall]）。
//
// 在节点执行范围里、但那次 Run 没配 session ID（不存档）又没有任务章时，返回 ("", true)：有所在的
// 任务，只是它没有身份——这次调用照样是它的一部分（不新开），只是没有 RunID 可核对。两者都没有
// ⟹ 不在任何任务里，这次 Run 是一个新任务的入口。
//
// ⚠️ 判据只有「ctx 上有没有」，不问「那次执行还活着吗」：从一次执行里带出来的 ctx（钩子里存下
// 的、节点里起的 goroutine 拿着的）照样报那个任务。在这样的 ctx 上开另一个任务要先 [DetachTask]。
func enclosingTask(ctx context.Context) (string, bool) {
	gsid, inNode := compose_v2.NodeScopeSessionID(ctx)
	if inNode && gsid != "" {
		return gsid, true
	}
	if v, ok := ctx.Value(taskScopeKey{}).(string); ok && v != "" {
		return v, true
	}
	return "", inNode
}

// rejectCall 是入口拒绝一次非法调用时返的错误——**恒不可重试**：这是确定性的代码错误，重跑
// 必然一样，配了重试的编排会把它连同成员里已经做过的事重放 N 遍（实测扣款 4 次）。
//
// 这次调用处在一个正在执行的任务里时（嵌套位置，或从一个任务里 [DetachTask] 出来——错误照样
// 流回那个任务的节点）再 wrap [compose_v2.ErrRunAborted]：所在节点失败后留下的失败档也不该被续跑
// 一次又一次地捡起来（所在任务的存档随之清掉）。
//
// 纯顶层不 wrap：什么都还没执行、存档原样在那儿，把调用改对再来一次即可；这时说「终局失败、
// 别再 Resume」是错的。
func rejectCall(ctx context.Context, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	_, nested := enclosingTask(ctx)
	detached := len(compose_v2.DetachedFrom(ctx)) > 0
	if nested || detached {
		return fmt.Errorf("%s: %w", msg, compose_v2.ErrRunAborted)
	}
	return &errCallRejected{msg: msg}
}

// errCallRejected 是纯顶层入口拒绝的错误：实现 [compose_v2.NonRetryable]（同 errUncatchableHookInterrupt，
// 不用 ErrRunAborted——那捆绑「清 cp 子树」语义，而这里什么都还没执行）。
type errCallRejected struct{ msg string }

func (e *errCallRejected) Error() string { return e.msg }

// NonRetryable 实现 [compose_v2.NonRetryable]——调用写错了，重试必然一样。
func (e *errCallRejected) NonRetryable() {}

// stampTask 铺章、「新开」信号（新任务时），以及 ctx 上的 sid：任务的身份就是 session 的 RunID，
// ctx 上的 sid 只是它的副本（见 WithSession）。钩子往 ctx 上推了别的 sid（比如把会话 ID 当 sid 推）
// 时，采纳它的 ctx 若不取回这一份，没配 checkpointer 的编排拿那个 sid 当自己图的 sid，节点里认出的
// 「所在任务」就是它——自己的成员被拒（实测：成员跑不起来、整个 Run 终局失败）。会话 ID 用
// `compose_v2.WithThreadIDCtx`。
//
// 只在入口铺一次：钩子之后经 compose_v2.CallHook 采纳，这几样都是执行状态、取的是这里铺好的。
func stampTask(ctx context.Context, sid string, fresh bool) context.Context {
	ctx = withTaskScope(ctx, sid)
	ctx = compose_v2.WithSessionIDCtx(ctx, sid)
	if fresh {
		ctx = compose_v2.WithFreshRunCtx(ctx)
	}
	return ctx
}

// enterRunTask 在编排的 **Run 入口、`OnAgentStart` 之前**判定「这是不是一个新任务」，并把
// 判定结果往下传。
//
//   - 这次 Run 在一个正在执行的任务里（[enclosingTask]：父编排的成员、节点体或钩子里的调用、
//     `RunSubAgent` 等嵌套调用……）：它是那个任务的一部分，什么也不声明，只把章往下带。session
//     的 RunID 与所在任务对得上这件事，入口已经核对过（[resolveRunSession]，对不上就拒）；
//   - 否则这是一个新任务的入口：丢弃上个任务的 don't-ask-again 规则、推「新开」信号
//     （[compose_v2.WithFreshRunCtx]：这一层和它下面的各层都不捡上个任务留下的存档），然后盖章。
//
// # 为什么在 `OnAgentStart` 之前
//
// 钩子里就可能发起调用（经 `RunSubAgent` 调一个把关的 agent）。判定排在钩子之后时，那一刻
// 新任务还没宣告：session 里还是上个任务的审批规则，那次调用用上一个任务的 approve_always
// 静默执行了工具（实测）；也没有「新开」信号，它接着跑上个任务被放弃时留下的存档。
//
// # 为什么嵌套位置上不能「新开」
//
// 嵌套位置上的 Run 要么是所在任务的一部分，要么是业务有意开的独立任务——框架分不出「有意开
// 旁路任务」和「为了隔离事件随手 new 了一个 session」，而两种读法各有一种静默出错的后果：当新
// 任务读，`inner.Run(ctx, NewRunSession(), in)` 每次父节点重跑都新开一个任务，内层问两个问题
// 就永远答不完（实测活锁、每轮留一份孤儿存档）；当同一个任务读，拿别的任务的 ctx 给另一个
// 会话开任务又会静默并进去。所以不猜：session 的 RunID 对不上所在任务就拒，开独立任务是
// 显式动作（[DetachTask]）。
//
// # 为什么入口先遮住节点授予
//
// 转交 / 路由能力走签名（RunOption），ctx 上那份节点授予只给**被授予的那个 agent 自己图里**的
// 代码用（[RegrantFromNode]，BasicAgent / GraphAgent 在跑图时放进自己收到的那份）。节点里裸调的
// agent 不经框架的四个调起入口，ctx 上还带着调用方那份——不在入口遮住的话，它的 agent 级钩子
// （跑在图外）与非 Basic / Graph 编排自己的回调经 `RegrantFromNode` 拿到的是调用方的能力，被请教
// 的 agent 替调用方定了路由（实测，`err == nil`）。
//
// 「这次算不算新任务」的信息**只有入口有**——子层看自己的存档只能看到「有一份挂起的」，
// 看不到「它属于上一个已被放弃的任务」。所以全仓只在这里判一处，引擎与 `shouldEmitInputEvent`
// 读同一个信号（这个坑本仓踩过五次，形状每次一样：同一个问题在两处各判各的）。
//
// ⚠️ 这个判据不会让「NodeFunc 里裸调 `sub.Run`、不 push ns」这个形态变得可用：传父的 session
// 时子层没有自己的 ns，它的存档落进父图自己的槽 ⟹ 两张图身份不同时 Resume 撞
// `ErrGraphIDConflict`，身份相同时（BasicAgent 的图不带名字，任意两个 BasicAgent 的图身份相同）
// 守卫拦不住、子层直接接着跑父图的存档。出路只有走 `RunSubAgent`。
func enterRunTask(ctx context.Context, session *RunSession) context.Context {
	ctx = shadowNodeGrants(ctx)
	if _, ok := enclosingTask(ctx); ok {
		return stampTask(ctx, session.RunID, false)
	}
	// 同一刻丢弃这个 RunID 名下的 don't-ask-again 规则——「上一个任务结束了」这句话对存档和
	// 对授权必须同时成立（规则按 RunID 分，换了 RunID 本来就是一套新的；这里管的是业务复用同一个
	// RunID 开新任务的写法）。**只清规则集**——「这次调用批过没有」那份已搬进 tools 节点的私有
	// 中断态（见 `toolCallMemo.Approved`），跨任务隔离靠容器不靠这里。
	clearApprovalRulesForNewTask(session)
	return stampTask(ctx, session.RunID, true)
}

// enterResumeTask 是 Resume 入口的对应物：续跑的是 session 那个已有的任务，只盖章、不声明新
// 任务。同样排在 `OnAgentStart` 之前，钩子之后经 compose_v2.CallHook 采纳。Resume 只在顶层调
// （在一个正在执行的任务里调会被 commonResumePrep 拒掉）。
func enterResumeTask(ctx context.Context, session *RunSession) context.Context {
	return stampTask(shadowNodeGrants(ctx), session.RunID, false)
}

// shouldEmitInputEvent 判断 wrapper 编排（8 个非 BasicAgent 编排）Run 入口是否应加 input
// 审计事件 NewOutputEvent(name, input)。
//
// **防嵌套 Resume input 重复**（audit MED-2）：agent_v2 嵌套统一走 sub-agent.Run（非
// Resume），outer.Resume 重跑中断 step 时第二次调本编排 Run → input event 被加两次（审计
// dup + 泄漏到 AgentAsTool fullHistoryInput / HandoffAgent prior 的 LLM 输入）。BasicAgent
// 因 input 在 preamble 节点 initial-only 天然免疫；wrapper 编排 input 在 Run top-level，需
// 此 guard：本编排 cp 已存在（同 sid+graphNS）= resume 重入（input 上次已加且随 session
// 续传），不重加。无 checkpointer（无 Resume 能力）/ 查询失败 → 保守加（宁审计 dup 不丢审计）。
func shouldEmitInputEvent(ctx context.Context, sid string) bool {
	// **这是新任务 ⟹ 一定不是重入**，不必去问存档。
	//
	// 下面那个 `ExistsNS` 探针答的是「这个 (sid, ns) 有没有存档」，而它被当成「这次算不算
	// 续跑重入」在用——**这两件事不等价**：业务复用会话级 sid 时，上一个被放弃任务留下的
	// 存档会让探针误判成重入 ⟹ 新任务的 input 审计事件**一条都不发**（实测 1 → 0，
	// err 全程 nil），而那条事件会流进 AgentAsTool / HandoffAgent 喂给模型的历史——
	// 模型看不见用户这次说了什么。
	//
	// 「算不算新任务」只有入口知道，所以入口说了算：`enterRunTask` 在顶层 Run
	// 声明一次，这里和引擎读同一个信号，不再各判各的。
	if compose_v2.IsFreshRunCtx(ctx) {
		return true
	}
	cper := compose_v2.GetCheckpointer(ctx)
	if cper == nil {
		return true
	}
	exists, err := cper.ExistsNS(ctx, sid, compose_v2.GetGraphNS(ctx))
	if err != nil {
		return true
	}
	return !exists
}

// recoverPanicToErr 把"在 deferred 闭包内**直接** recover() 拿到的值"转 err 写 *retErr
// （仅当 *retErr 还 nil —— 不覆盖正常 return 已设的业务错误）。
//
// 为何不复用 recoverToErr：recoverToErr 必须 `defer recoverToErr(...)` **直接 defer** 才
// 生效（它内部 recover()）。而 OnAgentEnd 收尾要在**同一个 defer 闭包**里"先 recover 设
// retErr 再调 OnAgentEnd"（在同一 defer 闭包内"先记录错误再上抛"，让
// hook/observability 看到框架自身 panic —— buildComposeGraph / collectEvents 等
// compose_v2.Run recover 之外的 wrapper panic）。但闭包内**调用** recoverToErr 时其
// recover() 无效（Go：recover 只在 deferred function 直接调用生效，闭包调的 helper 不算 ——
// 已被 panic_recover_internal_test.go 实证）。故本 helper 不自己 recover，而是接收闭包内
// **直接** recover() 的值：
//
//	defer func() {
//	    recoverPanicToErr(ctx, "XxxAgent.OnAgentEnd", recover(), &retErr) // recover() 在闭包内直接求值，有效
//	    ctxOut := WithEndError(ctx, retErr)
//	    fireAgentEnd(ctxOut, hooks, info, out, retErr)
//	}()
//
// 与顶部 `defer recoverToErr`（最早注册→最后跑）配合：本闭包（后注册→先跑）先消费 panic 设
// retErr，顶部 recoverToErr 后跑时 recover() 返 nil → no-op（不重复 wrap）。
//
// 栈在本函数里取：它在那个 defer 闭包里被调，panic 发生处的栈帧还在。*retErr 已经有错误时不覆盖它，
// 这次 panic 只留一条带栈的日志。
func recoverPanicToErr(ctx context.Context, what string, r any, retErr *error) {
	if r == nil {
		return
	}
	pe := panicerr.New(r)
	if *retErr != nil {
		// 交错误值本身、不在这里调 Error()：已有错误是业务的，它的 Error() 可能 panic（typed-nil），在调用处
		// 求值的话 panic 会逃出收尾闭包、OnAgentEnd 就不调了；交给 Log 格式化则被它兜住
		panicerr.Log(ctx, slog.LevelError, "agent_v2: 编排收尾兜住 panic（已有错误，不覆盖）", pe, panicerr.KeyError, *retErr)
		return
	}
	*retErr = panicerr.Wrap(what+" panic recovered", pe)
	panicerr.Log(ctx, slog.LevelError, "agent_v2: 编排收尾兜住 panic，已转成错误", pe, panicerr.KeyError, (*retErr).Error())
}

// fireAgentEnd 调 OnAgentEnd。它是观测钩子（返回值不往后流、不影响执行结果），它 panic 不改变这次调用的结局：就地兜住、
// 带栈记一条 ERROR，成功照旧算成功，失败照旧交出原来那个错误。
//
// 成功时转成错误的话，一次已经做完的调用被报成失败：它自己的存档已经按成功清掉，编排续跑 / 重试时它从头再跑、做过的副作用
// 重放（实测）。失败时曾经让 panic 交给外层兜底，外层把原来的错误整个换成 panic 的错误：中断认不出来（调用方不知道有问题
// 要答）、终局失败（ErrRunAborted）的标记丢了被当成可重试、错误文本也变了。挂在原错误旁边也不行——错误链上多出一个 PanicError，
// 按「是不是 panic」分档的地方会把一个观测钩子的问题当成这次调用的。
func fireAgentEnd(ctx context.Context, hooks hook.Hooks, info *hook.AgentInfo, out *hook.AgentOutput, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			name := ""
			if info != nil {
				name = info.Name
			}
			panicerr.Log(ctx, slog.LevelError, "agent_v2: OnAgentEnd 钩子 panic（这次调用的结局不变）", panicerr.New(r),
				"agent", name, "call_failed", retErr != nil)
		}
	}()
	_ = hooks.OnAgentEnd(ctx, info, out)
}

// isFrameworkNil 是包级通用 typed nil 检测 — audit⁶ 视角 1 揭露协议六复证后抽包级
// helper 让所有 framework interface (Agent / model.ChatModel / Memory / hook.Hooks /
// skill.SkillBox / tool.Toolkit / *tool.Toolkit) 共用一套 reflect.IsNil 防护.
//
// **业务陷阱**: Go interface "typed nil" 经典 — `var m *MyModel; WithModel(m)` 时 m
// 是 typed nil pointer 但 model.ChatModel interface header 持非 nil type tag → `m == nil`
// false 不触发 fail-fast → 业务后续 m.Generate(...) 远 stack nil deref panic 难定位.
//
// **覆盖 kind**: Ptr / Interface / Map / Slice / Chan / Func / UnsafePointer
// (Go reflect 可 IsNil 的全部 kind). struct value 实现 (非 ptr receiver) zero value
// 不算 nil 是 Go 语义, 业务自负.
func isFrameworkNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return rv.IsNil()
	}
	return false
}

// isAgentNil 检 Agent 是否真 nil — 防 Go interface "typed nil" 经典陷阱:
// `var b *BasicAgent; seq.AddStep("x", b)` 时 b 是 typed nil pointer 但
// `Agent` interface header 持非 nil type tag → `a == nil` 是 false 不触发 fail-fast,
// 业务后续 `ag.Run(...)` 在 worker 远 stack nil deref panic 难定位。
//
// **audit² 视角 A HIGH-2 揭露此陷阱 (PlanAgent) + audit³ 视角 1 HIGH-1 揭露
// 协议反向不对称** (修法只覆盖 PlanAgent, 其他 6 编排 + 4 With* options 漏防).
// 修法: 抽包级 helper 让 7 编排 + 4 With* options 全用 reflect.IsNil.
//
// **业务自定义 Agent 实现注意**: 假设 Agent 实现是 ptr/interface/map/slice/chan/func
// (Go reflect 可 IsNil 的 kind). struct value 实现 (非 ptr receiver) 不在保护范围
// — 业务自负 (struct value 没"nil"概念是 zero value 不是 nil).
func isAgentNil(a Agent) bool {
	if a == nil {
		return true
	}
	v := reflect.ValueOf(a)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}

// errUncatchableHookInterrupt 是「顶层 hook 抛中断被拒」的错误类型。
//
// 实现 [compose_v2.NonRetryable]：这是确定性的接线错误（gate 挂错了层），重试只会
// 原样再拒一遍——实测不标时配了 WithXxxRetryConfig 的编排把它重试了 4 次。
// 不用 ErrRunAborted：那捆绑「清 cp 子树」语义，而本错抛在图执行之前、什么档都没写，
// 清档动作会误伤同 sid 下与本次无关的残档。
type errUncatchableHookInterrupt struct{ msg string }

func (e *errUncatchableHookInterrupt) Error() string { return e.msg }

// NonRetryable 实现 [compose_v2.NonRetryable]——接线错误重试必然一样。
func (e *errUncatchableHookInterrupt) NonRetryable() {}

// WrapAgentStartErr 包装 OnAgentStart 返回的错误——**没人能接住的 hook 中断在这里被拒**。
//
// 导出给同栈的其他 `Agent` 实现（如 a2a.RemoteAgent）复用：这条规矩曾只长在本包 9 个
// 编排的调用点上，第 10 个实现（RemoteAgent）用 `%w` 透传 ⟹ 假暂停原样存活——
// 「立规矩必须列全入口」，收成一个导出入口让新实现有处可接。
//
// # 为什么没人接的 hook 中断必须拒
//
// 中断的契约是「抛出 ⟹ 有档可续」。校验与落档的机器都在图执行循环里，而 OnAgentStart
// 在 wrapper 顶层、图之前——顶层抛中断时**没有任何一层能落档**。实测现状：
// `IsInterrupt=true` 但挂起档为 0，前端照常渲染问题，用户答完 Resume 撞
// `ErrNoPendingCheckpoint`——被误导到最后一步才失败。
//
// 「落一份初始档」试过：引擎自己的不变量当场拒绝（`checkpoint has pending interrupts
// but no frontier to run them`）——那条防线是对的（挂起没人能跑），绕它等于对抗自己。
// 同类产品的同名机制也一致：中断原语第一行就取节点运行时上下文，图外调用直接崩。
//
// # 判据：引擎盖的「在节点执行范围内」章，不是 ns 字符串
//
// [compose_v2.WithinNodeScope] 为真 ⟹ 本次 Run 发生在某个父图节点的执行范围内 ⟹
// 中断冒出 sub.Run 后被**父图的节点路径**接住（校验 + 落父档 ⟹ 可续），照旧放行——
// `plan_chat_web` 的暂停 gate 范式靠它工作。章由引擎在进节点那一刻盖（先于 OnNodeStart）、
// 进新图时不清（嵌套图整张都在外层节点的执行范围里），只有一个铸造点。
//
// 这里**不看**所在的任务（[enclosingTask]）：顶层图的钩子相位里发起的嵌套调用处在任务里、却不在
// 任何节点执行里，它的 OnAgentStart 抛出的中断同样没有一层能落档，照样要拒。「接不接得住中断」与
// 「是不是新任务」是两个问题，后者见 [enterRunTask]。
//
// 曾用 `GetGraphNS(ctx) == ""` 判「顶层」——两侧各有误差：业务顶层自己 push 了 ns
// ⟹ 误判嵌套 ⟹ 假暂停放行；NodeFunc 里裸调 sub.Run 不 push ns ⟹ 误判顶层 ⟹
// 中断被拒。换章后这两侧的**判定**都对了。
//
// ⚠️ **但「判定对了」不等于那个形态可用**：裸调形态续不上——传父的 session 时子层的存档落进
// 父图自己的槽，传新 session 时入口直接拒（见 [enterRunTask] 的同名说明）。
// 所以这里放行只意味着**中断本身不再被误拒**，整条闭环仍要求走 `RunSubAgent`。别把这段读成
// 「裸调形态被支持了」。
//
// # 转出的错误不 `%w` 原中断，且不可重试
//
// cond 拒中断的先例：`%w` 会让 `IsInterrupt` 仍为 true 而档没落 ⟹ 调用方拿去渲染、
// 用户答完 Resume 死锁——正是要修的那个形态。错误类型实现 NonRetryable（接线错误，
// 重试必然一样）。
func WrapAgentStartErr(ctx context.Context, err error) error {
	if ie, isInt := compose_v2.IsInterrupt(err); isInt && !compose_v2.WithinNodeScope(ctx) {
		return &errUncatchableHookInterrupt{msg: fmt.Sprintf(
			"OnAgentStart: 顶层 hook 抛了中断（reason=%q）——这里在图执行之外，"+
				"没有可落的存档，用户答完也续不上。出路二选一：把这个 agent 嵌进编排当成员跑"+
				"（中断会冒进父图的节点路径落档），或者把 gate 挪到图内可落档的位置——"+
				"BasicAgent 用 OnModelEnd（模型层、图内），自带图的编排用第一个节点。"+
				"⚠️ 叶子型 agent（如 a2a.RemoteAgent）没有模型层，只有前一条路可走", ie.Reason)}
	}
	return fmt.Errorf("OnAgentStart: %w", err)
}
