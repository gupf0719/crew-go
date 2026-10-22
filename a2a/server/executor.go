// Package server 把 crew-go 的 agent_v2.Agent 暴露成 A2A server（a2asrv.AgentExecutor），
// 让其它语言 SDK（Java/Python）通过 A2A 协议发现并调用 crew-go 的 agent——是 a2a.RemoteAgent
// （消费远程）的逆向（暴露本地）。配合 a2a/registry 注册进服务目录实现跨语言全互通。
//
// **支持**：
//   - 单轮：agent.Run → completed + 最终消息。
//   - **server 侧 HITL**：agent 抛 InterruptError（ask_user / 业务工具）→ task 转
//     input-required（问题作 status message）；客户端带同 taskID 续发 → 框架 Resume
//     （taskID ↔ RunSession/cp 映射 in-process 持有），答案按 InterruptID 配对注入。
//     多轮追问天然支持（每次 input-required → 续发 = 一轮 Resume）。
//   - **流式**：agent 的 LLM chunk（OrchestrationHooks.OnNodeEvent Type="chunk"）实时
//     桥接为 A2A TaskArtifactUpdateEvent（首 chunk 建 artifact，后续 append）。
//   - Cancel：取消挂起 task → 清映射 + abort 清 cp 子树 + canceled 状态。
//
// **可观测（对齐 CLAUDE.md observability 协议）**：
//   - 业务级 AgentHooks：构造被服务的 agent 时照常 `agent_v2.WithHooks(obs.AgentHooks())`——
//     executor 不碰，agent.run/llm/tool metric+span 正常 fire。
//   - 编排级 OrchHooks：业务用 HTTP middleware 包 Handler push
//     `agent_v2.WithCtxOrchHooks(r.Context(), obs.OrchHooks())`——a2asrv 的 detached ctx 是
//     context.WithoutCancel（values 保留），一路流到执行；executor 自动把业务 hooks 与内部
//     流式桥 **chain**（不互斥，不会被顶掉）。
//   - **跨进程 trace 串联**：server 用 WithExecutorObservability(obs) 启用——从入站
//     Message.metadata 取出 W3C trace context 作父 + 建 SERVER span，内部 agent / model / tool span
//     挂在它下面、跟 client 同一条 trace（client 侧 a2a.WithObservability 负责注入）。没有 trace
//     metadata 时 server 自起新 trace，不影响业务。
//   - gen_ai.conversation.id：executor 自动 push ThreadID = A2A ContextID（ctx 未设时）——A2A 同
//     context 多 task 的会话在 trace 里聚类。crew.run.id = 每 task 的 RunID（框架自动）。
//     **注意 ThreadID 不止观测**：served HandoffAgent 的持久转交也按 ThreadID 存——同
//     ContextID 的后续 task 续接上次转交后的 active agent（这是 A2A contextID 语义的正确
//     映射，详见 Execute 内注释）。
//   - 挂起记录：每 task 一个 sid，executor 在任务到终态时调 obs.CleanupSession 丢掉这个 sid 还没续跑的
//     挂起记录（input-required 留着，续发 Resume 时用来算等待时长）。
//
// **局限（明确）**：
//   - 跨进程 Resume 不支持：taskID↔RunSession 映射 + cp 都 in-process，server 重启后
//     旧 task 续发返 failed（session values 本就只活在进程内）。
//   - 挂起 task 在进程内常驻直到被续跑 / Cancel / 进程退出——远端永不回复的 task 会
//     占住 session+cp（业务可经 A2A Cancel 释放）。⚠️ **多实例共享一个持久 task store
//     时这条出路不成立**：实例 2 上续发会把 task 写成 `failed` 终态（它没有那份进程内
//     挂起态），而 `failed` 是终态 ⟹ 回实例 1 再续发或 Cancel 都被拒 ⟹ 实例 1 上的
//     session + cp **永久不可回收**，只随进程退出消失。实测确认。
//   - **cluster mode（`a2asrv.WithClusterMode`）不在支持范围**：它要自拼 a2asrv 栈，
//     而 [Handler] 有意不接 `a2asrv.RequestHandlerOption`（理由见 Handler 的 godoc）
//     ⟹ 归属包装层和 [conformanceInterceptor] 两道守卫都装不上。更要紧的是 SDK 的
//     cluster 执行侧从**全新的 `context.Background()`** 起跑（跨队列只传 callCtxCodec
//     编码的那几样），`compose_v2.WithOwner` 是裸 ctx-value、不在其中 ⟹ [WithOwnerResolver]
//     解析出的身份**全程为空**、所有租户落进同一分区。实测跨租户读 + 接管挂起 task。
//     要跨实例分发就自拼栈并自负归属：身份过队列边界得走 `a2asrv.ClusterConfig.ContextCodec`。
//   - 流式 chunk 是 agent 全程 LLM 增量（含中间 tool-call 轮的文本）——聚合 artifact
//     的客户端拿到的是全程文本，可能比最终答案多（中间轮通常无 content，实际影响小）；
//     要精确最终答案用 completed 的 status message（无 artifact 时客户端自动取它）。
//   - 多 query 并发中断（fanout 多 ask_user / 多个工具审批）：问题编号合并进一条 input-required，每一问带上
//     interruptId，状态消息另附挂起清单 DataPart（约定见 a2a 包 pending.go）。对端要按 interruptId 分别作答，
//     一条文本不再灌给全部挂起项（放在审批上，一句 approve 就批准了全部工具），任务继续停在 input-required、
//     请对端重答。工具审批的答复只认固定的几个词与 `{"approved": bool}`，认不出的同样请重答（见 answers.go）。
//     可以只答其中几问：已接受的答复按任务累积，每次续发都整份交给 Resume（见 taskState.answers）。
package server

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/observability"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// Executor 适配 agent_v2.Agent → a2asrv.AgentExecutor（HITL + 流式）。
type Executor struct {
	agent agent_v2.Agent
	cp    compose_v2.Checkpointer      // 挂起 task 的中断 cp（与 tasks 映射同生命周期，in-process）
	obs   *observability.Observability // a2a 跨语言 trace：提取 metadata + server span（nil 时无 a2a trace）

	// ownerResolver 定「这次请求属于谁」。**必配**（NewExecutor 缺失即 panic）。
	ownerResolver compose_v2.OwnerResolver

	// taskStore 是业务指定的 task 存储后端。nil = 用进程内默认。
	//
	// **[Handler] 总会给它包一层归属检查**（见 ownerScopedTaskStore）——业务只声明
	// 「用哪个后端」，包装不由业务负责。这一条是有意的：守卫要是靠调用方记得自己包，
	// 那它就不是守卫。
	taskStore taskstore.Store

	mu    sync.Mutex
	tasks map[taskKey]*taskState // 挂起（input-required）task → 续跑所需状态
}

// taskKey 是挂起 task 表的键：**(owner, taskID) 两维**。
//
// taskID 单独当键不够——A2A 的 taskId **可以由客户端给**，SDK 收到非空 taskId 就拿它
// 去 taskStore 查、查到就续（`agentexec.go` 的 loadExecutionContext），而那个 Get
// 不做归属过滤。于是任何知道 taskId 的人都能续别人挂起的 task：读到里面等着人答的
// 提问、替对方回答。owner 由服务端从已认证凭据解析，跟 taskId 一起构成键。
//
// 空 owner（单租户 / 匿名）时所有请求落同一分区——那是「没有身份」的诚实语义。
type taskKey struct {
	owner  string
	taskID a2atypes.TaskID
}

// ExecutorOption 配置 Executor。
type ExecutorOption func(*Executor)

// WithExecutorObservability 注入 observability 启用 a2a 跨进程 trace：server 侧从入站
// Message.metadata 取出 W3C trace context 作父 + 建 SERVER span，内部 agent / model / tool span
// 挂在它下面（跟 client 同一条 trace）。
func WithExecutorObservability(obs *observability.Observability) ExecutorOption {
	return func(e *Executor) {
		if obs != nil {
			e.obs = obs
		}
	}
}

// WithOwnerResolver 设「这次请求属于谁」的解析函数——**必配**，构造期缺失 panic。
//
// A2A 的 contextId 和 taskId 都可以由客户端给：它们是**续接标识**，不是授权凭据。
// 不带 owner 维时，谁知道 taskId 谁就能续别人挂起的 task（读到等人答的提问、替对方
// 回答），谁知道 contextId 谁就能接手别人会话里已经转交出去的控制权。
//
// owner 从**已认证凭据**解析——认证中间件放进 ctx 的主体，不是请求体里的字段。它进
// 挂起 task 表的键、并 push 给下游（HandoffAgent 的持久转交等按 owner 分区的存储读它）。
// 不是一道闸：同 owner 同 taskId 照常续，异 owner 查不到、报「无可续状态」。
//
// 单租户 / 内网可信：返固定值即可。多租户：从认证中间件的主体取，**别从客户端可控的
// 字段（header / body / metadata）取**，那等于没做。
func WithOwnerResolver(fn compose_v2.OwnerResolver) ExecutorOption {
	return func(e *Executor) { e.ownerResolver = fn }
}

// WithTaskStore 指定 task 存储后端（默认进程内）。
//
// 换持久后端时用它——**别自己拼 a2asrv 栈**：[Handler] 会给传进来的后端自动包一层
// 归属检查，自拼栈的话那层和 CallInterceptor 里的检查会一起消失，跨租户读原样成立
// （实测：租户 B 经 tasks/get 读到 A 的完整历史）。
//
// # 对后端的两个要求
//
//  1. **把 `Task.Metadata` 原样存回来**。归属戳写在那儿，跟 task 一起持久化，多实例横向
//     扩容时各实例读到的归属才是同一个（见 ownerScopedTaskStore）。丢了 metadata 的后端
//     会让**自己建的 task 立刻就读不到**，且日志里有一句指名原因——不是静默放行。
//  2. **自己按调用方身份分区**（`compose_v2.GetOwner(ctx)`），别把过滤全指望外面那层。
//     A2A 1.0 §13.1 把这条定为 MUST：授权检查必须发生在**任何可能泄漏资源存在性的查询
//     之前**。外面那层是「先无差别查、再逐条滤」，正是这条禁止的形状——它挡得住内容，
//     挡不住计数：实测租户 A 30 条、B 3 条、`pageSize=10`，B 沿 `a2atypes.ListTasksResponse.NextPageToken` 翻 4 页，
//     每页的 `PageSize - len(Tasks)` 累加得 27——**A 真实条数的紧下界**（不含末页；末页
//     算进去会越过真值，两种数法都不精确等于，但都足以把量级交出去）。这个通道从外面
//     关不上（补页要自己合成游标，且仍不满足「查询之前」的字面要求）。
//  3. **实现 [OwnerPartitionedTaskStore] 说出第 2 条做到了**（一个返 true 的方法）。
//     不说的话包装层不敢信任你，会把你正确的 per-owner `a2atypes.ListTasksResponse.TotalSize` 降级成本页条数——
//     安全但不准。
//
// **跑 conformance 验第 2、3 条**：`a2a/server/taskstore_conformance.RunTaskStoreTests(t, factory)`
// ——它断言跨 owner 列不到、满页不因过滤变薄、TotalSize 是分页前的 per-owner 总数、以及
// 你声明了分区。SDK 自带的进程内后端（默认值）已经合规：它经 Authenticator 读我们注入的
// owner 自己下推，框架在装配时替它补上第 3 条的声明。
//
// ⚠️ **续跑仍不跨进程**：挂起 task 的 RunSession / cp 是进程内的（见包 godoc 局限），
// 换持久 task store 只让 `tasks/get` / `tasks/list` 这类**读**跨实例可用，不让
// input-required 的 task 在另一个实例上续得起来。
func WithTaskStore(st taskstore.Store) ExecutorOption {
	return func(e *Executor) { e.taskStore = st }
}

// taskState 是一个 task 的续跑状态。**每个在途 run（新 task Run / 续发 Resume）期间都有一个
// busy=true 的条目**——让 Cancel 能感知在途并安全协调（不与在途 Resume 并发跑 abort）。
type taskState struct {
	sess    *agent_v2.RunSession   // Run 时的 session——Resume 必须续用（taskID/cp sid 都锚定 RunID）
	pending []*compose_v2.AskQuery // 待答的中断 query（Resume 时按 InterruptID 配对答案）
	// answers 是这个任务里已经接受过的全部答复（按 InterruptID），每次续发都整份交给 Resume。没进工具账的提问
	// （ask_user、节点里直接 Ask 的）续跑时会重问，框架要 Resume 的调用方把答过的答案带上；只交这一条消息里的，
	// 一个节点先后问两问、或者挂着两问只答其一时，就会来回重问、永远答不完（实测）。只由持有 busy 的那次续发读写。
	answers   map[string]*schema.Message
	busy      bool               // 有在途 run 正跑——防并发续发 + 让 Cancel 协调
	canceled  bool               // tombstone：Cancel 来过——acquire 拒、在途 run 的 conclude 改走清理
	cancelRun context.CancelFunc // 取消在途 run 的 runCtx（Cancel 用它停在途 Resume，避免并发 abort）
}

var _ a2asrv.AgentExecutor = (*Executor)(nil)

// NewExecutor 包装一个 agent_v2.Agent 成 A2A executor。
//
// agent 通常是框架编排（BasicAgent / Sequential / ...）；自定义 Agent 实现也可，但其
// Resume 必须支持框架 ResumeOption（WithResumeSession / WithResumeUserInputs），否则
// HITL 续跑路径不可用（不中断的自定义 agent 不受影响）。
func NewExecutor(agent agent_v2.Agent, opts ...ExecutorOption) *Executor {
	mustAgent(agent)
	e := &Executor{
		agent: agent,
		cp:    compose_v2.NewInMemoryCheckpointer(),
		tasks: make(map[taskKey]*taskState),
	}
	for _, o := range opts {
		o(e)
	}
	if e.ownerResolver == nil {
		panic("a2a/server: WithOwnerResolver 必配且无默认值——" +
			"contextId / taskId 都可以由客户端给，单靠它们谁知道谁就能续别人挂起的 task；" +
			"单租户返一个固定值即可，多租户从认证中间件放进 ctx 的主体取" +
			"（会话历史层的 WithHistoryOwnerFunc、agui 的 WithOwnerResolver 是同一条规矩）")
	}
	return e
}

// Execute 处理一次 A2A 请求：新 task → agent.Run；带已存 task 续发 → agent.Resume。
func (e *Executor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		inputMsg := inputMessage(execCtx)

		// a2a 跨进程 trace：从入站 Message.metadata 取出远端的 trace context（W3C traceparent 等）作父，
		// server 的整棵 span 树串到 client 的 trace 上。缺失或格式不对时原样返回 ctx，server 自起新 trace。
		if e.obs != nil && execCtx.Message != nil {
			ctx = e.obs.ExtractFromMetadata(ctx, execCtx.Message.Metadata)
		}

		// 会话线程身份：A2A ContextID 是跨 task 的会话线程标识——push 成 ThreadID。两个消费者
		// （cross-verify 确认全集）：(1) 观测——trace 带 gen_ai.conversation.id（跨 run 会话聚类）；
		// (2) **HandoffAgent 持久转交**——served HandoffAgent 的
		// active agent 按 ThreadID 存（handoff.md §5.2 会话维度），同 ContextID 的下一个 task
		// 从上次转交后的 agent 续接（不 push 时按 per-task RunID 各自从 initial 起步）。
		// 仅 ctx 未设时 push，业务 middleware 显式设置的优先。
		if execCtx.ContextID != "" && compose_v2.GetThreadID(ctx) == "" {
			ctx = compose_v2.WithThreadIDCtx(ctx, execCtx.ContextID)
		}

		// owner 从已认证凭据解析（认证中间件放进 ctx 的主体），push 给下游——
		// HandoffAgent 的持久转交按 (agent名, owner, ThreadID) 存，没有这一维时两个
		// 租户递同一个 contextId 会撞同一个转交槽。同时它进本层挂起 task 表的键。
		// ctx 里已有就用——经 Handler 进来时 CallInterceptor.Before 已经 push 过
		// （那是唯一能覆盖整条链的位置，task store 的 Create/Get 都在它下游）。
		// Executor 被单独使用（不经 Handler）时才自己解析。
		owner := compose_v2.GetOwner(ctx)
		if owner == "" {
			owner = e.ownerResolver(ctx)
			ctx = compose_v2.WithOwner(ctx, owner)
		}
		tkey := taskKey{owner: owner, taskID: execCtx.TaskID}

		// 续发（客户端带已存 task 的 taskID）→ Resume 挂起的 run
		if execCtx.StoredTask != nil {
			st, err := e.acquire(tkey)
			if err != nil {
				yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed, agentMsg(err.Error())), nil)
				return
			}
			// 按挂起提问校验答复（见 answers.go）：认不出的审批答复、挂多问时没按 interruptId 作答，都不调 Resume，
			// 任务继续停在 input-required、请对端重答。
			inputs, reask := resumeInputs(st.pending, st.answers, inputMsg)
			if reask != "" {
				if e.releaseAfterReask(ctx, tkey, st) {
					yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired, pendingMessage(st.pending, reask)), nil)
				}
				return
			}
			if st.answers == nil {
				st.answers = make(map[string]*schema.Message, len(inputs))
			}
			for id, ans := range inputs {
				st.answers[id] = ans
			}
			all := make(map[string]*schema.Message, len(st.answers))
			for id, ans := range st.answers {
				all[id] = ans
			}
			e.runStreaming(ctx, execCtx, yield, st, func(runCtx context.Context, hooks agent_v2.OrchestrationHooks) ([]*agent_v2.AgentEvent, error) {
				return e.agent.Resume(runCtx, st.sess.RunID,
					agent_v2.WithResumeSession(st.sess),
					agent_v2.WithResumeUserInputs(all),
					agent_v2.WithResumeOrchHooks(hooks))
			})
			return
		}

		// 新 task：先 submitted 注册，再建 busy 条目（让 Cancel 能感知在途、统一状态机）
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		st := &taskState{sess: agent_v2.NewRunSession(), busy: true}
		e.mu.Lock()
		e.tasks[tkey] = st
		e.mu.Unlock()
		e.runStreaming(ctx, execCtx, yield, st, func(runCtx context.Context, hooks agent_v2.OrchestrationHooks) ([]*agent_v2.AgentEvent, error) {
			return e.agent.Run(runCtx, st.sess, inputMsg, agent_v2.WithOrchHooks(hooks))
		})
	}
}

// Cancel 取消 task：发 canceled 状态 + 清 cp。
//
// **与在途 run 的协调（修 audit MED：Cancel vs 在途 Resume 并发跑同 sid abort 的 cp 损坏）**：
//   - 有在途 run（busy）：只打 canceled tombstone + cr() 取消在途 run 的 ctx，**不**自己跑
//     abort——让在途 run 的 conclude 在其 Resume 返回**之后**独占清 cp。保证任一 sid 同时
//     至多一个 agent.Resume，杜绝并发 abort/continue 的 cp 逻辑损坏。
//   - 无在途 run：删条目 + 直接 abort 清 cp（独占）。
func (e *Executor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		// Cancel 也是 wire 入口——同样按 (owner, taskID) 寻址，否则知道 taskId 就能
		// 取消别人的在途任务（那也是一种拒绝服务）。
		owner := compose_v2.GetOwner(ctx)
		if owner == "" {
			owner = e.ownerResolver(ctx)
		}
		tkey := taskKey{owner: owner, taskID: execCtx.TaskID}
		e.mu.Lock()
		st, ok := e.tasks[tkey]
		var (
			busy   bool
			stopFn context.CancelFunc
			sid    string
		)
		if ok {
			st.canceled = true
			busy, stopFn, sid = st.busy, st.cancelRun, st.sess.RunID
			if !busy {
				delete(e.tasks, tkey) // 无在途，立即清条目
			}
		}
		e.mu.Unlock()

		if ok {
			if busy {
				// 停在途 run；其 conclude 见 canceled → 独占清 cp + 清条目（见 conclude）
				if stopFn != nil {
					stopFn()
				}
			} else {
				// 无在途 → 独占清理（见 discardCanceled）。
				e.discardCanceled(ctx, string(execCtx.TaskID), sid)
			}
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

// discardCanceled 清掉一个已取消、没有在途 run 的任务留下的存档与观测会话：abort 清 cp（detached ctx：取消请求
// 本身的 ctx 可能已随客户端断开取消；push sid 让 abort Resume 的 OnAgentStart 复用本 task 的 session，不漏建
// sid-less "" session），再清 session——不经 conclude 的路径（空闲时 Cancel、请重答期间来了 Cancel）由它释放，
// 不清的话 input-required 留下的 session 永久泄漏。框架编排 abort 恒返 nil（DeleteSubtree 失败 helper 内已
// slog.Warn）；自定义 Agent 的 abort 失败在此 warn——取消本身照常完成，但泄漏信号不能静默蒸发。
func (e *Executor) discardCanceled(ctx context.Context, taskID, sid string) {
	rctx := compose_v2.WithSessionIDCtx(compose_v2.WithCheckpointerCtx(context.WithoutCancel(ctx), e.cp), sid)
	if _, err := e.agent.Resume(rctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		slog.WarnContext(ctx, "a2a server: cancel abort cleanup failed (cp/session 可能残留)", "task", taskID, "err", err)
	}
	if e.obs != nil {
		e.obs.CleanupSession(sid)
	}
}

// releaseAfterReask 在请对端重答之后放掉 busy。期间来过 Cancel 的话（busy 时 Cancel 只打标记、把清理留给在途的
// 那次），这里就是那次：删条目、清存档与观测会话，不再发 input-required（Cancel 已经发了 canceled）——曾经
// 直接放掉 busy，条目、存档、观测会话一直留着，之后的续发拿到「已取消」，再来的 Cancel 被 SDK 短路，再也
// 没有路径清掉它们（审计实测）。返回 false 表示任务已经取消。
func (e *Executor) releaseAfterReask(ctx context.Context, k taskKey, st *taskState) bool {
	e.mu.Lock()
	canceled := st.canceled
	st.busy = false
	if canceled {
		delete(e.tasks, k)
	}
	e.mu.Unlock()
	if !canceled {
		return true
	}
	e.discardCanceled(ctx, string(k.taskID), st.sess.RunID)
	return false
}

// runResult 是 agent Run/Resume 的结果对。
type runResult struct {
	events []*agent_v2.AgentEvent
	err    error
}

// runStreaming 在 goroutine 跑 agent（Run 或 Resume），主循环把 chunk 实时桥接为 A2A
// artifact 事件，结束后按结果发终态（completed / input-required / failed）。
func (e *Executor) runStreaming(
	ctx context.Context,
	execCtx *a2asrv.ExecutorContext,
	yield func(a2atypes.Event, error) bool,
	st *taskState,
	runFn func(context.Context, agent_v2.OrchestrationHooks) ([]*agent_v2.AgentEvent, error),
) {
	runCtx, cancel := context.WithCancel(compose_v2.WithCheckpointerCtx(ctx, e.cp))
	defer cancel()
	// push sid（= st.sess.RunID）：agent.Run 内部也会 WithSession 同一个 sid，重复 push 同值无害。
	runCtx = compose_v2.WithSessionIDCtx(runCtx, st.sess.RunID)
	// a2a server span 覆盖整个 agent 执行到终态（conclude 收尾）；agent 拿返回的 runCtx 跑，它的 span 挂在下面。
	// e.obs 为 nil 时返回 no-op 句柄。operation 固定 message_stream：executor 总以流式消费 agent；
	// a2asrv 的 ExecutorContext 不暴露请求方法，同步 message/send 调进来的也标成 message_stream。
	var srvSpan *observability.A2AServerSpan
	runCtx, srvSpan = e.obs.StartA2AServerSpan(runCtx, "message_stream", string(execCtx.ContextID), string(execCtx.TaskID))
	// 注册 cancelRun 让 Cancel 能停本在途 run（避免它自己并发跑 abort）
	e.mu.Lock()
	st.cancelRun = cancel
	e.mu.Unlock()

	fw := &chunkForwarder{ch: make(chan string, 256), stop: make(chan struct{})}
	// **业务 OrchHooks 不被顶掉（audit obs-#1 fix）**：RunOption hooks 优先级高于 ctx hooks
	//（extractOrchHooksOrCtx 不 chain）——若只传 fw，业务经 HTTP middleware push 的
	// WithCtxOrchHooks(obs.OrchHooks()) 会被静默顶掉（node/cp/compact/fanout/skill 等
	// metric+span 全漏 fire）。这里显式 chain：业务 hooks 在前（其 ctx mutation 向下传），
	// 流式桥在后（纯消费不改 ctx）。
	var hooks agent_v2.OrchestrationHooks = fw
	if prev := agent_v2.GetCtxOrchHooks(ctx); prev != nil {
		hooks = agent_v2.NewChainOrchHooks(prev, fw)
	}
	done := make(chan runResult, 1)
	go func() {
		// **异常终止兜底**：`runFn` 里 `runtime.Goexit`（业务节点/工具里一句
		// `logger.Fatal`，zap 的 WithFatalHook 配置下就是这形态）会掐断这条 goroutine
		// 而不 panic ⟹ 下面那行 send 永不执行 ⟹ abandon 路径里的裸 `r := <-done`
		// **永久阻塞**，这个请求的收摊卡死。旗必须由普通语句在 send **之后**置位
		//（defer 里置位没用，Goexit 也跑 defer；置在 send 之前则正常路径会重复 send
		// 把 buffer 填满、defer 自己阻塞）。
		sent := false
		defer func() {
			if !sent {
				done <- runResult{err: errors.New("a2a/server: agent 执行异常终止（runtime.Goexit）——" +
					"未产出结果；典型来源是业务代码里的 logger.Fatal / t.Fatal")}
			}
		}()
		events, err := runFn(runCtx, hooks)
		done <- runResult{events: events, err: err}
		sent = true
	}()

	// 首 chunk 建 artifact，后续 append 同一 artifact
	var artifactID a2atypes.ArtifactID
	emit := func(text string) bool {
		if artifactID == "" {
			ev := a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart(text))
			artifactID = ev.Artifact.ID
			return yield(ev, nil)
		}
		return yield(a2atypes.NewArtifactUpdateEvent(execCtx, artifactID, a2atypes.NewTextPart(text)), nil)
	}

	// abandon：客户端断开 / 父 ctx 取消——停收 chunk + cancel agent + 等 goroutine 退出，
	// 仍走 conclude 把 cp/映射落到一致态（前端收不到，但服务端状态不悬空——修 audit MED）。
	abandon := func() {
		close(fw.stop)
		cancel()
		r := <-done
		e.conclude(ctx, execCtx, yield, st, r, srvSpan)
	}

	for {
		select {
		case c := <-fw.ch:
			if !emit(c) {
				abandon()
				return
			}
		case <-ctx.Done():
			abandon()
			return
		case r := <-done:
			// run 已结束不再有新 send——清掉缓冲里残留的 chunk 再发终态
			for drained := false; !drained; {
				select {
				case c := <-fw.ch:
					if !emit(c) {
						close(fw.stop)
						e.conclude(ctx, execCtx, yield, st, r, srvSpan)
						return
					}
				default:
					drained = true
				}
			}
			close(fw.stop)
			e.conclude(ctx, execCtx, yield, st, r, srvSpan)
			return
		}
	}
}

// conclude 在 run 结束后落定状态：一把锁内决定映射转移，再发终态事件。
//
// **canceled tombstone**：若在途期间被 Cancel（busy 路径只打 tombstone + 停 runCtx，不并发跑
// abort），此处在 Resume 已返回（<-done 之后）独占 abort 清 cp + 清条目，**不**发终态（Cancel
// 已发 canceled）——保证任一 sid 同时至多一个 agent.Resume。
func (e *Executor) conclude(ctx context.Context, execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool, st *taskState, r runResult, srvSpan *observability.A2AServerSpan) {
	// a2a server span 收尾（覆盖 agent 执行到终态）。srvSpan 总非 nil（e.obs 为 nil 时是 no-op 句柄）。
	// 错误详情不写进 span，只记类型。
	spanStatus, spanErrType := observability.OutcomeSuccess, ""
	// st.sess 由 executor 创建，挂起记录也由它清：到终态时丢掉这个 sid 还没续跑的挂起记录；
	// input-required（keepPending）不清——续发 Resume 时用它算等待时长，续发到终态再清。
	cleanupSession := false
	defer func() {
		if cleanupSession && e.obs != nil {
			e.obs.CleanupSession(st.sess.RunID)
		}
	}()
	defer func() { srvSpan.End(spanStatus, spanErrType) }()

	// owner 在 Execute 入口已 push 进 ctx——conclude 恒跑在那条 ctx 链下游。
	tkey := taskKey{owner: compose_v2.GetOwner(ctx), taskID: execCtx.TaskID}
	ie, isInterrupt := compose_v2.IsInterrupt(r.err)
	var qs []*compose_v2.AskQuery
	if isInterrupt {
		qs = ie.AllQueries()
	}

	// 一把锁内决定转移：再中断（有 query）→ 留条目等续发；否则（完成/失败/取消/空中断）→ 清条目。
	e.mu.Lock()
	canceled := st.canceled
	// **陈旧写回 / 身份冲突 / 档没写成不能留条目等续发**：这次的结果没落盘，槽里是另一个执行者（或另一张图）
	// 的档、或者没有档，客户端续发时载入的是别人的状态或更早那份，于是重新问一遍已经答过的问题（实测客户端
	// 侧表现为"一直在问同一个问题"，不收敛）。引擎出口写档失败时返 `ErrCheckpointNotPersisted`、中断不在
	// 链上，本来就不满足 isInterrupt；这里仍先排除，是防自研 agent 把它们跟中断绑在一条链上交过来。
	stale := errors.Is(r.err, compose_v2.ErrCheckpointStale)
	notResumable := errors.Is(r.err, compose_v2.ErrCheckpointNotResumable)
	graphConflict := errors.Is(r.err, compose_v2.ErrGraphIDConflict)
	notPersisted := errors.Is(r.err, compose_v2.ErrCheckpointNotPersisted)
	keepPending := !canceled && !stale && !graphConflict && !notPersisted && isInterrupt && len(qs) > 0
	if keepPending {
		st.pending = qs
		st.busy = false
		st.cancelRun = nil
	} else {
		delete(e.tasks, tkey)
	}
	e.mu.Unlock()
	// 最终态（完成/失败/取消/空中断）清 session；input-required（keepPending）留待续发 Resume 复用同 session
	cleanupSession = !keepPending

	// 被取消：在途 Resume 此刻已返回 → 独占 abort 清 cp（detached ctx：runCtx 可能已被 Cancel 取消），
	// 不发终态（Cancel 已发 canceled）。
	if canceled {
		spanStatus = observability.OutcomeCancelled
		// push sid 让 abort Resume 的 OnAgentStart 复用本 task session（不漏建 sid-less "" session）；
		// 本 task 的 session 由下面 cleanupSession defer 清（canceled → keepPending=false → cleanupSession=true）。
		rctx := compose_v2.WithSessionIDCtx(compose_v2.WithCheckpointerCtx(context.WithoutCancel(ctx), e.cp), st.sess.RunID)
		if _, err := e.agent.Resume(rctx, st.sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
			// 同 Cancel 路径：框架编排恒 nil；自定义 Agent abort 失败 warn 不静默
			slog.WarnContext(ctx, "a2a server: post-cancel abort cleanup failed (cp/session 可能残留)", "task", string(execCtx.TaskID), "err", err)
		}
		return
	}

	if r.err != nil {
		if keepPending {
			spanStatus = observability.OutcomeBlocked // 远程要更多输入，非错误（中间态）
			// HITL：问题（多 query 编号合并）作 input-required 的 status message
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired, pendingMessage(qs, "")), nil)
			return
		}
		if notResumable {
			// 同款脱敏：`ErrCheckpointNotResumable` 的文案含 sid / ns / graphID / 各字段
			// 计数，那是给服务端日志排查用的，不该跨进程回传给远端对端。
			spanStatus, spanErrType = observability.OutcomeError, "a2a_checkpoint_not_resumable"
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed,
				agentMsg("the saved state for this session is inconsistent and cannot be resumed")), nil)
			return
		}
		if stale {
			// 脱敏：`ErrCheckpointStale` 的文案含 session id / ns / epoch token，是内部
			// 信息，不该跨进程回传给远端对端。
			spanStatus, spanErrType = observability.OutcomeError, "a2a_checkpoint_stale"
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed,
				agentMsg("this session was concurrently advanced by another run; result was not persisted")), nil)
			return
		}
		if graphConflict {
			// 同款脱敏：`ErrGraphIDConflict` 的文案含 sid / ns / 两张图的 GraphID。它重跑必然撞同一个
			// 槽（接线错误），对端换一个会话才走得通。
			spanStatus, spanErrType = observability.OutcomeError, "a2a_graph_id_conflict"
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed,
				agentMsg("the saved state for this session belongs to a different agent; start a new session")), nil)
			return
		}
		if notPersisted {
			// 档没写成（存储出错）：没有可以续的东西，引擎已经尽力清掉槽里更早的那份。脱敏：文案里有
			// 后端的错误原文与 sid，不跨进程回传给远端对端。
			spanStatus, spanErrType = observability.OutcomeError, "a2a_checkpoint_not_persisted"
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed,
				agentMsg("this run's state could not be saved, so it cannot be resumed; start a new task")), nil)
			return
		}
		spanStatus, spanErrType = observability.OutcomeError, "a2a_agent_error"
		// 跟 agent span 同一个口径：执行 ctx 已结束时按 ctx 判（到期是超时，取消不是失败）；ctx 还活着时错误链上
		// 的软取消是取消、DeadlineExceeded 是超时，context.Canceled 不算（那是别处被连带取消，这次执行失败了）。
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			spanStatus, spanErrType = observability.OutcomeTimeout, "timeout"
		case ctx.Err() != nil || errors.Is(r.err, compose_v2.ErrSoftCanceled):
			spanStatus, spanErrType = observability.OutcomeCancelled, ""
		case errors.Is(r.err, context.DeadlineExceeded):
			spanStatus, spanErrType = observability.OutcomeTimeout, "timeout"
		}
		// 非中断 error，或中断但 query 列表空（不该发生，AllQueries 保证非空——防御性走 failed）
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed,
			agentMsg(fmt.Sprintf("agent %q run failed: %v", e.agent.Name(), r.err))), nil)
		return
	}

	var msg *a2atypes.Message
	// 出站完整多模态（audit MED）：agent 响应的 Content + ContentBlocks（图片/文件/结构化）都走
	// A2A parts，不只 final.Content text。a2a.MessageToA2AParts 含 nil-data guard。final 可能
	// Content 空 + 仅 blocks（纯图片响应），故判 final != nil 而非 Content != ""。最终消息总是随
	// completed 发：无 artifact 客户端取 status message；流过 artifact 的按 A2A 惯例优先聚合 artifact。
	if final := agent_v2.FinalMessage(r.events); final != nil {
		if parts := a2a.MessageToA2AParts(final); len(parts) > 0 {
			msg = a2atypes.NewMessage(a2atypes.MessageRoleAgent, parts...)
		}
	}
	yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, msg), nil)
}

// acquire 取出 task 状态并标 busy（防同 task 并发续发）。canceled tombstone 拒绝续发。
func (e *Executor) acquire(k taskKey) (*taskState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.tasks[k]
	if !ok {
		// **不区分「不存在」和「不属于你」**——那个区别本身就是信息泄漏（能拿它探测
		// 别人的 taskId 存不存在）。两种情况同一句话。
		return nil, fmt.Errorf("a2a server: task %s has no resumable state (already completed, canceled, or server restarted) — start a new task", k.taskID)
	}
	if st.canceled {
		return nil, fmt.Errorf("a2a server: task %s was canceled", k.taskID)
	}
	if st.busy {
		return nil, fmt.Errorf("a2a server: task %s resume already in progress", k.taskID)
	}
	st.busy = true
	return st, nil
}

// ---- chunk → artifact 桥 ----

// chunkForwarder 实现 agent_v2.OrchestrationHooks，把 OnNodeEvent 的 chunk 转发给
// runStreaming 主循环。send 阻塞提供背压（A2A 客户端慢时反压 LLM 流）；stop 关闭后
// 丢弃（主循环已停止消费，防 OnNodeEvent 永久阻塞挂死 agent goroutine）。
type chunkForwarder struct {
	agent_v2.NoOpOrchestrationHooks
	ch   chan string
	stop chan struct{}
}

func (f *chunkForwarder) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if ev.Type == "chunk" && ev.Content != "" {
		select {
		case f.ch <- ev.Content:
		case <-f.stop:
		}
	}
	return ctx
}

// ---- helpers ----

// inputMessage 把入站 A2A message 转 schema.Message——完整提取所有 part（text 进 Content、
// raw/url/data 进 ContentBlocks），对齐 A2A 新版多模态。复用 a2a 包的 PartsToContent：executor
// 出站已 import a2a（MessageToA2AParts），wire 转换两侧统一一份，不再各持重复（原"两包不互
// import"的重复理由被出站 import 推翻——audit follow-up）。
func inputMessage(execCtx *a2asrv.ExecutorContext) *schema.Message {
	if execCtx.Message == nil {
		return schema.UserMessage("")
	}
	content, blocks := a2a.PartsToContent(execCtx.Message.Parts)
	m := schema.UserMessage(content)
	if len(blocks) > 0 {
		m.ContentBlocks = blocks
	}
	return m
}

// agentMsg 构造 agent 角色的 A2A 文本消息。
func agentMsg(text string) *a2atypes.Message {
	return a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart(text))
}
