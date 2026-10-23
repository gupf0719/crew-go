package compose_v2

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
)

// DetachExecution 返回一个剥掉了「当前这次执行」留在 ctx 上的全部状态的 ctx，给「在一次执行
// 里开一个跟它无关的独立运行」用。
//
// 执行过程中框架往 ctx 上铺了一层又一层它自己的东西：所在节点的位置（ns、节点 ID、实例、访问
// 序号、tool_call）、这次 Run 的 session ID 与任务代号、「新开」信号、续跑带来的答案、兑付账本、
// 节点私有态的访问器、嵌套调用计数器、收上行信号的那段执行……嵌套在这次执行里的一切都靠继承
// 它们才对得上号。而一个
// **独立**的运行要的恰恰相反：带着这些跑，它的存档落进当前执行的 ns 下、提问身份掺进当前节点的
// 维度、答案账本认当前任务的答案、嵌套调用记在当前节点这次执行名下——跟当前执行缠在一起，
// 却又不属于它。
//
// **剥掉的**（执行状态）：见 isExecutionKey。其中两样容易以为是环境：
//
//   - 会话 ID（[WithThreadIDCtx]）：它标识的是**当前**这场对话，独立运行按定义不是它的一部分。
//     留着的话独立运行写进当前会话的历史、排在用户这一轮的提问之前，下一轮被当成用户说过的话
//     （实测）。要让它进某个会话，在返回的 ctx 上自己推；
//   - 软取消信号（[WithSoftCancelCtx]）：它一个 Run 一个。留着的话，从一个被软取消的 Run 里带出来
//     的 ctx 开独立运行，一个节点都不跑就以上一个 Run 的取消原因停下，还留一份挂起档（实测）。
//
// **留下的**（调用方给整个请求配的环境）：checkpointer、编排钩子、调用主体（[WithOwner]）、
// durability 与陈旧写回守卫的配置、cp 事件开关。
//
// ⚠️ ctx 的取消（Done / 截止时间）照样继承——那不是值，挡不住：独立运行仍在调用方的请求里跑，
// 请求撤了它也该停。从一次**已经结束**的执行里存下来的 ctx（钩子里存的、后台 goroutine 拿着的）
// 往往已经取消了，那时先 `context.WithoutCancel` 再脱离。
//
// 剥是「看不见」不是「删掉」：返回的 ctx 包在原 ctx 外面，只挡住往下查这些 key；在它上面再推
// 的值照常可见。上层包的执行状态（实现了 [ExecutionStateKey] 的 key：agent_v2 的 session、收件人、
// 节点授予……）一并剥掉。
//
// 剥的同时记下**这一路是从哪些任务里脱离出来的**（[DetachedFrom]）：在节点执行范围里脱离时记下那次 Run
// 的 sid（没配 sid 时记空串——有所在的任务，只是它没有身份），接在之前几次脱离记下的后面；连同那次执行
// 「还在不在跑」的标记一起记。上层包据此拒绝在独立运行里再开一遍**仍在执行**的那些任务（agent_v2：Run /
// Resume 的 RunID 是其中之一就拒——曾经只有 agent_v2.DetachTask 记、只记一层，直接用本函数或脱离两层之后拿
// 外层任务的 session 跑 agent，那个 agent 当新任务收尾时把外层任务挂着的存档删了，实测）。「仍在执行」按任务判：
// 那个任务此刻没有执行在跑（挂起了，ctx 是当时存下来的）就不算——用存下来的 ctx 脱离之后续跑它是合法的（曾经
// 一律拒，实测）；它又续跑起来（另一次执行）期间拿当时存下的 ctx 脱离照样算（曾经按那一次执行判、放过了，实测）。
// 不在节点执行范围里的任务（编排的 agent 级钩子跑在图外）引擎这一层看不到，上层用 [DetachExecutionFrom] 补上。
//
// 独立运行挂起时，它的根图抛出的中断带着记号：所在任务的节点把它返回时引擎拒收（那个挂起这个任务答不了，
// 只能在顶层用它自己的 sid 续）——原样返回、取出里面的 *InterruptError 返回、再包一层、合并进别的中断都一样。
func DetachExecution(ctx context.Context) context.Context {
	return detachExecution(ctx, "", false)
}

// DetachExecutionFrom 同 [DetachExecution]，另把 task 记进「这一路是从哪些任务里脱离出来的」：给有自己的任务
// 身份、而此刻不在节点执行范围里的上层包用（`agent_v2.DetachTask`：编排的 agent 级钩子跑在图外，引擎这一层
// 看不到那个任务）。task 是那个任务的 sid（没有身份时传空串）。
func DetachExecutionFrom(ctx context.Context, task string) context.Context {
	return detachExecution(ctx, task, true)
}

func detachExecution(ctx context.Context, task string, withTask bool) context.Context {
	// 先读出之前几次脱离记下的那一串：它是执行状态，下面包上 detachedCtx 之后就看不见了
	from, _ := ctx.Value(detachedFromKey{}).([]detachedOrigin)
	alive := runAliveFrom(ctx)
	if sid, inNode := NodeScopeSessionID(ctx); inNode {
		from = appendDetachedFrom(from, detachedOrigin{task: sid, alive: alive})
	}
	if withTask {
		from = appendDetachedFrom(from, detachedOrigin{task: task, alive: alive})
	}
	d := context.WithValue(detachedCtx{ctx}, detachedKey{}, true)
	if len(from) > 0 {
		d = context.WithValue(d, detachedFromKey{}, from)
	}
	return d
}

// detachedOrigin 是「这一路是从哪个任务里脱离出来的」那一串里的一项：任务（sid），以及脱离那一刻它所在的
// 那次执行还在不在跑（[runAliveKey]；不在节点执行范围里、ctx 上又没有标记时为 nil，按仍在执行算）。
type detachedOrigin struct {
	task  string
	alive *atomic.Bool
}

// stillExecuting 报这一项所指的任务此刻是否仍在执行：脱离那一刻所在的那次执行还在跑（没有标记的按还在跑算），
// 或者这个任务此刻在本进程里有执行在跑（[executingTasks]）。
func (o detachedOrigin) stillExecuting() bool {
	if o.alive == nil || o.alive.Load() {
		return true
	}
	return o.task != "" && taskExecuting(o.task)
}

// appendDetachedFrom 把 o 接到 from 后面（同一个任务、同一个存活标记已经在里面就不重复记）。同一个任务的不同
// 标记都留着：它们是这一路先后从那个任务的哪几次执行（或图外）里脱离出来的，任何一条还算数，这个任务就算仍在
// 执行（曾经按任务去重、先记的留下：用存下的 ctx 脱离、续跑那个任务之后，续跑那次执行里再脱离记下的那条被丢掉，
// 留下的是上一次执行的、已结束的那条 ⟹ 正在续跑的任务被当成不在执行，实测）。返回的是新切片：from 可能还挂在
// 别的 ctx 上。
func appendDetachedFrom(from []detachedOrigin, o detachedOrigin) []detachedOrigin {
	for _, t := range from {
		if t == o {
			return from
		}
	}
	return append(append(make([]detachedOrigin, 0, len(from)+1), from...), o)
}

// withoutDetachedFrom 把来源链里 task 那几次**已经结束**的执行去掉：进入 task 自己的一次执行时用（不在节点执行范围里、
// sid 为 task 的那次 Run 入口）。在它自己的执行里，它不是「脱离出来之前所在的任务」——用存下的 ctx 脱离、续跑它时，
// 那一条记的是这个 ctx 当初从它的哪次执行里脱离出来，那次执行已经结束，现在又回到了它里面：它的成员拿它的 session
// 跑是它的一部分。不去掉的话，任务此刻正在执行（就是这次），成员全被当成「从它脱离出来又跑它」拒掉。
//
// 存活标记为真（那次执行还在跑）或没有标记（图外脱离、判不出）的留着：这个任务此刻还有一次执行在等着，这一次是在
// 它执行的同时又开了它一遍（节点里脱离之后直接 compose_v2.Run 这个任务的 sid），里面的 agent 入口照样该拒（曾经按
// sid 一律去掉，链上那条被删、里面的调用不再被拒，审计实测）。链里没有可去的项时原样返回。
func withoutDetachedFrom(ctx context.Context, task string) context.Context {
	from, _ := ctx.Value(detachedFromKey{}).([]detachedOrigin)
	ended := func(o detachedOrigin) bool { return o.task == task && o.alive != nil && !o.alive.Load() }
	if !slices.ContainsFunc(from, ended) {
		return ctx
	}
	kept := make([]detachedOrigin, 0, len(from))
	for _, o := range from {
		if !ended(o) {
			kept = append(kept, o)
		}
	}
	return context.WithValue(ctx, detachedFromKey{}, kept)
}

// detachedFromKey 记「这一路是从哪些任务里脱离出来的」（见 [DetachedFrom]）。执行状态：再脱离一次时先被
// 剥掉、由 detachExecution 读出旧的接上新的再放回；采纳钩子返回的 ctx 时取框架这一份。
type detachedFromKey struct{}

// DetachedFrom 返回 ctx 这一路是从哪些**仍在执行**的任务里脱离出来的（sid，先脱离的在前，同一个任务只出现
// 一次；那次 Run 没配 sid 时是空串）。没有脱离过、或只在节点执行范围之外用 [DetachExecution] 脱离过时为空
// （[DetachExecutionFrom] 另记它给的任务）。
//
// 「仍在执行」按任务判，满足任一条就算：
//
//   - 脱离那一刻所在的那次执行还在跑（在图外脱离、引擎看不到那次执行的，按还在跑算）；
//   - 这个任务此刻在本进程里有执行在跑：上一次执行结束了、任务又续跑起来（另一次执行）也算。曾经只看那一次
//     执行：同一个任务续跑期间，节点拿上一次执行里存下的 ctx 脱离、再拿这个任务的 session 跑 agent，没被拒，
//     那个 agent 被当成新任务以这个 sid 跑完收尾，把这个任务挂着的存档清掉（审计实测）。
//
// 任务挂起之后、续跑之前，用当时在节点里存下的 ctx 脱离，记下的那一项两条都不满足：拿它续跑这个任务是合法的。
// 续跑起来之后的那次执行就是它自己，链里它那几次已经结束的执行在这次 Run 入口去掉，它的成员拿它的 session 跑照常。
// 在图外（编排的 agent 级钩子里）存下的 ctx 经 [DetachExecutionFrom]（`agent_v2.DetachTask`）脱离，那一项没有
// 存活标记、按还在跑算：挂起之后拿它续跑这个任务也被拒；用 [DetachExecution] 脱离不记这一项。
//
// ⚠️ 判得到的只有本进程里、图在跑的那一段：别的进程在跑这个任务看不到；任务在图外的那一段不登记（编排的
// OnAgentStart 早于图、OnAgentEnd 晚于图，没有图的 agent 如 `a2a.RemoteAgent` 整段都是），那时拿上一次执行存下的
// ctx 脱离不算。链上只有 ctx 自己带着的来源：`context.Background()`、别的任务存下的 ctx 上没有这个任务，拿它们
// 跑一个正在执行的任务，这里认不出。别跨执行留着 ctx；在节点里开独立任务用节点当前的 ctx。
func DetachedFrom(ctx context.Context) []string {
	v, _ := ctx.Value(detachedFromKey{}).([]detachedOrigin)
	var out []string
	for _, o := range v {
		if !slices.Contains(out, o.task) && o.stillExecuting() {
			out = append(out, o.task)
		}
	}
	return out
}

// runAliveKey 携带「这次执行还在跑」的标记：不在任何节点执行范围里的那次 Run（顶层的、脱离出来的根图）在
// 入口铸一个、出口置否，它里面嵌套的 Run 沿 ctx 继承同一个。执行状态：脱离时剥掉，脱离出来的根图铸自己的。
// 从一次执行里存下来的 ctx 带着它，执行结束之后读到的是否；那个任务是不是又在别的执行里跑，另看 [executingTasks]。
// 没配 sid 的执行只能靠它（[executingTasks] 按 sid 登记）。
type runAliveKey struct{}

// executingTasks 登记本进程里此刻有执行在跑的任务（sid → 在跑的执行数）：不在任何节点执行范围里、配了 sid 的
// Run 入口加一、出口减一，嵌套的 Run 属于外面那次执行、不另计。[DetachedFrom] 据此按任务判「仍在执行」。
//
// 进程级、按 sid 计：同一个 sid 同时有两次执行本来就违反「一枚 RunID 只服务一个任务」的契约，这里不区分它们。
// 只有来源链里记着某个 sid 时才查它，没脱离过的执行不读这张表。
var executingTasks = struct {
	mu sync.Mutex
	n  map[string]int
}{n: map[string]int{}}

// enterExecutingTask 登记 sid 有一次执行开始了，返回出口时调的注销函数。
func enterExecutingTask(sid string) (exit func()) {
	executingTasks.mu.Lock()
	executingTasks.n[sid]++
	executingTasks.mu.Unlock()
	return func() {
		executingTasks.mu.Lock()
		defer executingTasks.mu.Unlock()
		if executingTasks.n[sid] <= 1 {
			delete(executingTasks.n, sid)
			return
		}
		executingTasks.n[sid]--
	}
}

// taskExecuting 报 sid 此刻在本进程里有没有执行在跑。
func taskExecuting(sid string) bool {
	executingTasks.mu.Lock()
	defer executingTasks.mu.Unlock()
	return executingTasks.n[sid] > 0
}

func runAliveFrom(ctx context.Context) *atomic.Bool {
	a, _ := ctx.Value(runAliveKey{}).(*atomic.Bool)
	return a
}

// detachedCtx 挡住执行状态的 key，其余原样委托。
type detachedCtx struct{ context.Context }

func (c detachedCtx) Value(key any) any {
	if isExecutionKey(key) {
		return nil
	}
	return c.Context.Value(key)
}

// detachedKey 是「这里往下是一次脱离出来的独立运行」的记号（[DetachExecution] 盖）。它本身也算
// 执行状态：再脱离一次时先被剥掉、再盖一枚新的。
type detachedKey struct{}

// isDetachedRoot 报 ctx 上跑的 Run 是不是一次独立运行的根图：脱离过、而且不在任何节点的执行
// 范围里（独立运行里面那些节点里再跑的嵌套图在节点范围里，它们的中断归它们的外层节点收）。
func isDetachedRoot(ctx context.Context) bool {
	detached, _ := ctx.Value(detachedKey{}).(bool)
	return detached && !WithinNodeScope(ctx)
}

// detachedInterruptError 是独立运行的根图挂起时交出去的错：原中断照样取得到（[IsInterrupt] /
// errors.As 顺着 Unwrap 找得到，调用方可以拿去展示或记下 sid），只是它不属于调用方所在的任务——
// 所在任务的节点把它原样返回时，引擎拒收（validateInterrupt）。
type detachedInterruptError struct {
	err error
	// sid 是那个独立运行的 session ID：它的挂起在顶层用这个 sid 续。
	sid string
	// marked 是原中断的一份拷贝，带着记号（InterruptError.detached）：[IsInterrupt] / errors.As 走到这一层时
	// 交出它，不交原对象——业务把 *InterruptError 当包级变量复用时，改原对象会让它经一次脱离运行之后永远
	// 带着记号，之后在普通任务里返回它也被终局拒收（审计实测），并发用时还是写竞争。原中断为 typed nil 时为 nil。
	marked *InterruptError
}

func (e *detachedInterruptError) Error() string { return e.err.Error() }
func (e *detachedInterruptError) Unwrap() error { return e.err }

// As 让 errors.As 取 *InterruptError 时拿到带记号的那份拷贝（同 asInterrupt）。
func (e *detachedInterruptError) As(target any) bool {
	if p, ok := target.(**InterruptError); ok && e.marked != nil {
		*p = e.marked
		return true
	}
	return false
}

// foreignInterrupt 报节点返回的中断是不是一次独立运行交出的（以及那个运行的 sid）：链上有
// detachedInterruptError 包装（原样返回），或中断对象本身带着记号（取出来返回、再包一层、合并进别的中断）。
func foreignInterrupt(err error, ie *InterruptError) (string, bool) {
	var foreign *detachedInterruptError
	if errors.As(err, &foreign) {
		return foreign.sid, true
	}
	if ie != nil && ie.detached {
		return ie.detachedSID, true
	}
	return "", false
}

// ExecutionStateKey 由「当前这次执行」的状态用的 ctx key 类型实现（方法体留空，只作标记）：
// [DetachExecution] 剥掉它；[CallHook] 采纳钩子返回的 ctx 时，它**不论那个 ctx 是哪来的**
// 都取框架自己的那份——这类状态是框架的身份与位置（session、任务章、收件人、历史写入器……），
// 钩子推了别的值也只会让框架认错（实测：钩子推了别的 sid，成员被判成别的任务的而拒掉）。
//
// 给本包之外的执行状态用——上层包（agent_v2）的执行状态 key 在这里登记不了（依赖方向），标记
// 挂在 key 类型自己身上，两处都按它认。曾经上层包另有一份清单、另有一个采纳函数，引擎这层的
// 采纳点（OnGraphStart / OnNodeStart）只认得本包那份：钩子在那两处返回别处缓存的 ctx 时，上层的
// 历史写入器、转交收件人取的是钩子那份，会话历史与转交照样静默丢（实测）。
//
// 环境 key 实现 [EnvironmentKey]，别实现它。
type ExecutionStateKey interface {
	ExecutionState()
}

// EnvironmentKey 由调用方给整个请求配的环境用的 ctx key 类型实现（方法体留空，只作标记）：
// [DetachExecution] 留下它；[CallHook] 采纳钩子返回的 ctx 时，那个 ctx 派生自交给钩子的那个
// 就听钩子的（钩子可以改配置），是外来的（别处缓存的、`context.Background()`）就取框架这次的。
//
// 给本包之外的环境用（agent_v2 的编排钩子与历史配置、store 的后端……），本包的在
// isConfigurableKey 里登记。曾经采纳时环境一律跟钩子返回的 ctx：钩子返回别处缓存的 ctx 时，这次
// 请求的调用主体、编排钩子、checkpointer、store 全换成了那个请求的——流式片段送到别人的连接上、
// 长期记忆写进别人的 store；返回 `context.Background()` 时调用主体变成空串，所有租户共用一个桶（均实测，
// `err == nil`）。
type EnvironmentKey interface {
	Environment()
}

// isExecutionKey 报 key 是不是「当前这次执行」的状态（[DetachExecution] 要剥的）：本包的在下面
// 登记，上层包的实现 [ExecutionStateKey]。采纳时怎么取另看 isConfigurableKey：软取消是执行状态
// （脱离时剥），却是调用方可配的（钩子在派生的 ctx 上推了照收）。
//
// 新加 ctx key 时要么登记在这里，要么登记成环境（isConfigurableKey）——机械测试扫全包的
// context.WithValue / Value 调用，哪个 key 两边都没登记就红：剥不剥、钩子能不能改，是要想清楚的
// 决定，不能默认。
func isExecutionKey(key any) bool {
	switch key.(type) {
	case sessionIDKey, runTaskIDKey, currentTaskIDKey, freshRunCtxKey, resumeCtxKey, answerLedgerKey,
		graphNSKey, nodeIDKey, nodeVisitKey, nodeInstanceIDKey, nodeInterruptCounterKey,
		toolCallIdxKey, currentToolCallIDKey, inToolCallKey, withinNodeScopeKey,
		nodeStateAccessorKey, subCallCounterKey, subCallScopeKey, abnormalNodeExitKey,
		componentKindKey, eventSinkKey, sendSinkKey, toolCategoryLookupKey, outerRerunGensKey, outerRegensKey,
		threadIDKey, softCancelCtxKey, detachedKey, detachedFromKey, runAliveKey, signalScopeKey, taskRecipientKeyCtxKey, hookCallKey,
		exitGraceKey:
		return true
	}
	_, ok := key.(ExecutionStateKey)
	return ok
}

// isConfigurableKey 报 key 是不是调用方可配的：环境（本包的在下面登记，上层包的实现
// [EnvironmentKey]），加上软取消这一样执行状态。采纳钩子返回的 ctx 时，派生的就听钩子的，外来的
// 就取框架这次的；其余执行状态不论哪种都取框架的，业务 key 都取钩子的。
//
// 软取消算可配：业务本来就经 ctx 给（[WithSoftCancelCtx]），在钩子里给也是正当用法——比如在
// OnAgentStart 里给每个 Run 装一个管理端能触发的软取消。曾经它按执行状态一律取框架的，钩子装的
// 软取消触发了也停不下来（实测）。
//
// 会话 ID（[WithThreadIDCtx]）不算：它是这场对话的身份，在 OnAgentStart 之前就被历史写入器读走了。
// 钩子推一个新的只会让同一个 Run 里出现两个会话 ID——模型看到钩子的、历史写进调用方的（实测）。
func isConfigurableKey(key any) bool {
	switch key.(type) {
	case checkpointerKey, orchHooksKey, ownerKey, durabilityCtxKey, staleGuardCtxKey, emitCpEventCtxKey,
		softCancelCtxKey:
		return true
	}
	_, ok := key.(EnvironmentKey)
	return ok
}
