package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// DetachTask 返回一个跟当前执行无关的 ctx，用来在一个正在执行的任务里**开一个独立的任务**
// （或续跑另一个任务）：
//
//	func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
//	    side := agent_v2.NewRunSession()
//	    events, err := reviewer.Run(agent_v2.DetachTask(ctx), side, input) // 一个独立的任务
//	    ...
//	}
//
// # 什么时候需要它
//
// 在一个正在执行的任务里（节点体、钩子、嵌套调用的被调方……）调 agent，session 的 RunID 必须
// 属于这个任务，否则入口直接拒；Resume 在这样的位置上一律拒。框架不从「RunID 不同」去推断
// 「业务想开一个独立任务」——它分不出这是有意开旁路任务，还是为了隔离事件随手 new 了一个
// session，而后者当成新任务读会活锁（父节点每次重跑都新开一个，内层问两个问题就永远答不完）。
// 所以开独立任务是一个显式动作，就是这个函数。
//
// 嵌套在当前任务里的调用（它是当前任务的一部分：续跑时要续得上、答案要对得上）**不要**用它，
// 用 `RunSubAgent`。
//
// # 剥掉什么、留下什么
//
// 剥掉当前执行留在 ctx 上的一切——当前 session 与任务章、节点位置与 ns、任务代号、「新开」
// 信号、续跑带来的答案与兑付账本、节点私有态访问器、嵌套调用计数与记录器、历史写入器与归属
// 位置（连同内部决策 LLM 那里「不进历史」的禁止）、转交 / 路由的收件人与收信号的那段执行、per-call 覆盖、会话 ID
// （`compose_v2.WithThreadIDCtx`）、软取消信号……剥离本身由 [compose_v2.DetachExecution] 做（agent_v2 的这些
// key 实现了 `compose_v2.ExecutionStateKey`，它一并认得），本函数在这之上记下是从哪个任务脱离出来的。
// 不剥的话，独立任务的存档落进当前执行的 ns 下、提问身份掺进当前节点的维度、答案账本认当前
// 任务的答案、嵌套调用记在当前节点这次执行名下；独立任务写进当前会话的历史、排在用户这一轮的
// 提问之前（实测）。它要进哪个会话，在返回的 ctx 上自己推会话 ID。
//
// 留下调用方给整个请求配的环境：checkpointer、编排钩子（两层）、调用主体、会话历史配置、store、
// durability 与陈旧写回守卫。ctx 的取消（Done / 截止时间）照样继承——从一次已经结束的执行里存下
// 来的 ctx 往往已经取消了，那时先 `context.WithoutCancel`。
//
// # 两条跟当前任务划清界限的规矩
//
//   - 独立任务要有**自己的** RunID：脱离之后传当前任务的 session（或 Run / Resume 当前任务的 RunID）
//     入口直接拒——那不是独立任务，而是在当前任务自己的执行里再开一遍它：清掉它的审批规则、它成功
//     收尾时连当前任务挂着的存档一起删（实测）。续跑当前任务不需要脱离，它就在执行着。这一路上脱离过
//     几层都算（[compose_v2.DetachedFrom]：独立任务里再脱离一次，外层那个任务照样在等，拿它的 session
//     照样拒），直接用 `compose_v2.DetachExecution` 脱离也算。「仍在执行」按任务判：ctx 是任务执行时在节点里
//     存下来的、任务已经挂起，用它（先 `context.WithoutCancel`）脱离之后续跑那个任务是合法的；任务又续跑起来
//     之后再拿那个存下的 ctx 脱离，照样拒。在编排的 agent 级钩子里存下的 ctx 经本函数脱离，判不出那次执行还在
//     不在、按仍在执行算，挂起之后续跑也拒——续跑它用 `compose_v2.DetachExecution`（判得到的范围见
//     [compose_v2.DetachedFrom]）；
//   - 独立任务挂起时，它的中断**不能**从节点里返回：它属于那个任务，这个任务答不了（答案送不进去，
//     续跑时它从头再问一遍，永不收敛——实测）。框架拒收，报 `compose_v2.ErrRunAborted`——原样返回、
//     用 `compose_v2.IsInterrupt` 取出来返回、再包一层都一样。记下它的 RunID 交给该处理它的地方，在
//     顶层续跑它。
//
// # 独立任务的生命周期归调用方
//
// 它有自己的 RunID：存档在它自己名下，当前任务成功收尾时不会清它；观测层按 sid 记的挂起记录也是它
// 自己的一份，放弃它时要自己 `CleanupSession`。
func DetachTask(ctx context.Context) context.Context {
	// 所在的任务记进「这一路是从哪些任务里脱离出来的」（compose_v2.DetachedFrom）：在节点执行范围里时
	// 引擎自己记得到；编排的 agent 级钩子跑在图外，那里的任务只有本包的任务章知道，所以显式交过去
	if from, inTask := enclosingTask(ctx); inTask {
		return compose_v2.DetachExecutionFrom(ctx, from)
	}
	return compose_v2.DetachExecution(ctx)
}

// detachedFromRunning 报 runID 是不是这一路脱离出来之前所在的任务之一（它们都还在执行）。
func detachedFromRunning(ctx context.Context, runID string) bool {
	for _, t := range compose_v2.DetachedFrom(ctx) {
		if t != "" && t == runID {
			return true
		}
	}
	return false
}

// 下面这些 key 类型是 agent_v2 的执行状态，实现 compose_v2.ExecutionStateKey：剥离
// （compose_v2.DetachExecution / DetachTask）与采纳钩子返回的 ctx（compose_v2.CallHook）都按它认，
// 引擎这层的采纳点（OnGraphStart / OnNodeStart）也认得。新加 ctx key 时要么在这里实现它，要么实现
// compose_v2.EnvironmentKey（调用方配的环境，见本组最后两个）——机械测试扫全包的
// context.WithValue / Value 调用，两边都没实现就红。
func (sessionKey) ExecutionState()            {}
func (taskScopeKey) ExecutionState()          {}
func (callApprovalKey) ExecutionState()       {}
func (finalRoundKey) ExecutionState()         {}
func (toolsUsedKey) ExecutionState()          {}
func (foAttemptsKey) ExecutionState()         {}
func (revealedChKey) ExecutionState()         {}
func (revealTurnKey) ExecutionState()         {}
func (repeatTurnKey) ExecutionState()         {}
func (runOverridesCtxKey) ExecutionState()    {}
func (offloadDedupKey) ExecutionState()       {}
func (subAgentStoreScopeKey) ExecutionState() {}
func (historyCtxKey) ExecutionState()         {}
func (historySlotCtxKey) ExecutionState()     {}
func (historyReceiptCtxKey) ExecutionState()  {}
func (handoffHopsCtxKey) ExecutionState()     {}
func (handoffDestKey) ExecutionState()        {}
func (handoffHistoryModeKey) ExecutionState() {}
func (routeDestKey) ExecutionState()          {}
func (endErrCtxKey) ExecutionState()          {}
func (resumePhaseCtxKey) ExecutionState()     {}
func (toolResultPosKey) ExecutionState()      {}
func (toolBatchSignalsKey) ExecutionState()   {}
func (nodeGrantsKey) ExecutionState()         {}

// 调用方给整个请求配的环境：脱离时留下；采纳钩子返回的 ctx 时，派生的听钩子的、外来的取框架这次的
// （compose_v2.EnvironmentKey）。
func (historyCfgCtxKey) Environment() {}
func (orchHooksCtxKey) Environment()  {}
