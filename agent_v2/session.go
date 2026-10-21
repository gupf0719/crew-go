package agent_v2

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// newUUID 生成 UUID v4 字符串（不引入外部依赖）。
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// RunSession 保存一次对话运行的执行历史和键值存储。
//
//   - 单一 events 列表，加锁保护；并发扇出时分支各写自己的 lane，join 处按确定顺序
//     单点提交（`ForkLane` / `CommitLaneEvents`）
//   - RunID 作 cp 主索引 + Resume 入口
//
// ⚠️ 这里曾经写着「不含 fork lane 机制——parallel 编排走 superstep frontier 直接合并，
// 无需 lane 隔离协议」。那个判断是错的：多分支往同一个 events 列表并发追加会让谁的
// 事件落在谁的区间取决于调度，下一轮各分支读回的上文因此串台。ISSUE-80 补了 lane
// 隔离，注释一直没跟上。
//
// **框架不替本结构跨 Run 持久化**：events 只在内存里，生命周期归持有它的 caller——
// Resume 想接着累积就用 `WithResumeSession` 把同一个对象传回去，想自己存到外部存储就用
// `RestoreEvents` 灌回来。cp 不持有 events。
//
// 要让对话跨 Run 活着走会话历史层（`WithHistory`，**仅 BasicAgent**），按 `Owner` +
// `compose_v2.WithThreadIDCtx` 声明的 ConvID **两个维度**分流——ConvID 通常来自 wire，
// 是**续接标识符不是授权令牌**，只按它分就是谁猜中谁能拉到别人的完整对话。
//
// cp 那边曾经有过一个 `Checkpoint.ThreadID` 字段（ISSUE-50 P1）想补会话这一维，已随
// ISSUE-98 一并删除。它是个闭环孤岛：字段只服务两个按会话列存档的查询（ListByThread / LatestByThread，一并删了），
// 而**那两个 API** 受制于「cp 成功即清」只看得到当前挂起的 run、承诺的却是「历史 run」，
// 名不副实。字段自己更根本的毛病是**没有天然的产生者**——框架产 cp 时并不知道业务的
// 会话边界，`SaveNS` 只能 auto-fill 成 sessionID，于是每个 sid 自成一个 thread，
// 这一维从来没真正立起来过。
//
// **嵌套 child sess 协议**（ISSUE-79 修）：AgentAsTool 默认构造半共享 child sess
// （`NewSharedChildSession(parent)`）—— values map + valuesMu 与父共享指针 / events
// + eventsMu 独立 / parentSess 反向指针。业务自定义 Store 实现的 Search 可用
// `RootSession()` 沿父链上追看跨 sub-agent 视图。
//
// **lane events 协议**（ISSUE-80 修）：ParallelAgent
// fan-out 多 branch 并发跑时，每 branch 通过 `ForkLane()` 获得独立 lane—— branch 内
// `AddEvent` 进 lane 私有 slice (lock-free，每 goroutine 一份) 不污染 parent 共享
// events；branch 完成后 `CommitLaneEvents` 把 lane events 按 TS 排序后合并回 parent。
// 解决 R31 视角 D HIGH-1 sibling 串台根因，删 R31 临时 `FinalMessageByAgent` name filter
// 限制（业务可同 sub-agent 实例作多 branch）。
//
// **并发安全**：父子共享 valuesMu 让 ParallelAgent fan-out N branch × M sub-agent
// 嵌套场景下并发 SetValue/GetValue 仍 race-free。eventsMu 每 sess 独立——子 events
// 写入不会与父 events 竞争。lane events slice 在 branch 内 lock-free（每 goroutine
// 独立 lane 不共享）。
type RunSession struct {
	// RunID 标识**一次任务**——从一次 Run 开始，到终结为止（成功 / 终局失败 / 被放弃）。
	// 它是 checkpointer 的主索引，也是 Resume 的入口。
	//
	// 「一个任务」不等于「一次 Run 调用」：中间可以有零到多次 `Resume`，**也可以有多次
	// `Run`**——典型是远端 A2A agent 返 input-required 时，host 用同一个 sess 再 `Run`
	// 一次把补充信息送过去（`RemoteAgent.Resume` 不支持 standalone，同任务续跑就是这么
	// 写的，见 `example/a2a/hitl_smoke`）。判据是**意图**不是调用次数：还在办同一件事就
	// 是同一个任务，RunID 不该变。
	//
	// # 契约：一枚 RunID 只服务一个任务
	//
	// 开新任务就要换一枚（`NextTask` 一行搞定）。**不换的后果没有任何提示**：任务 B
	// 成功收尾时按 sid 清整棵子树，会把任务 A 还挂着、正等用户回答的存档一起删掉——
	// 实测 `err = nil`、无告警，之后 `A.Resume` 返 `ErrNoPendingCheckpoint`。
	//
	// 框架不替你判「这次算不算新任务」，因为**只有入口知道**：同一个 sid 上再来一次
	// 调用，可能是「上个任务的续跑」也可能是「新任务」，两者在数据里长得一样。同理
	// 框架也不能偷偷帮你换——换了 Resume 就找不到那份存档了。
	//
	// # 跨任务累积状态不需要复用 RunID
	//
	// `values` / `events` 挂在 **RunSession 对象**上，不挂在 RunID 上（两者都不进 cp，
	// 见 doc/v2/store.md 的状态分层表）。所以「一场会话跑多个任务、业务 KV 和事件流
	// 一路攒着」的写法是 **复用对象 + 每个任务换 RunID**，`NextTask` 做的正是这件事。
	//
	// # 会话身份是另一维
	//
	// 想表达「这几个任务属于同一场对话」用 `compose_v2.WithThreadIDCtx` 的 ConvID，
	// 它归历史层（`WithHistory` / `HistoryStore`）和观测层。**cp 这层不认识会话**——
	// 框架产 cp 时并不知道业务的会话边界（ISSUE-98 删掉 `Checkpoint.ThreadID` 就是
	// 这个理由，见本文件上面那段）。别拿 ConvID 当 RunID 使。
	RunID string

	// eventsMu 保护 events slice。**主线 sess 独立**——业务字面量构造的 sess 用此
	// 字段。**ForkLane child sess** 走 eventsMuRef 共享父锁 (R33 B HIGH-1 fix)。
	// AddEvent / Events / RestoreEvents 都通过
	// `effectiveEventsMu()` 获取实际锁——eventsMuRef 非 nil 优先，否则用 &eventsMu。
	eventsMu    sync.RWMutex
	eventsMuRef *sync.RWMutex // R33 B HIGH-1: ForkLane child 共享 parent.eventsMu 指针
	events      []*AgentEvent

	// valuesMu 保护 values map。**父子共享指针**（NewSharedChildSession 让 child
	// 复制父 valuesMu 指针）—— 父子并发 SetValue/GetValue 同 map 走同一个锁
	// race-free。业务直接构造 `&RunSession{RunID:...}` 字面量时 valuesMu nil →
	// valuesOnce 保护 lazy init。
	valuesOnce sync.Once
	valuesMu   *sync.RWMutex
	values     map[string]any

	// parentSess 反向指针：child sess 指向 parent sess（嵌套 AgentAsTool +
	// shared parent 模式）。nil 表示这是根 sess。
	// 业务用 ParentSession() / RootSession() 访问，不要直接读字段。
	parentSess *RunSession

	// laneEvents 非 nil 表示本 sess 是 ParallelAgent fan-out lane 内的 child sess——
	// AddEvent 路由到 laneEvents.Events (lock-free 每 goroutine 一份) 而非 events，
	// CommitLaneEvents 在 branch 完成时把 lane events 按 TS 排序合并回 parent。
	// nil 表示主线 sess，AddEvent 直接写 events + eventsMu。
	// 业务用 ForkLane() 创建，不直接构造。
	laneEvents *laneEvents
}

// laneEvents 是 ParallelAgent fan-out branch 私有 events slice + 父链。
//
// 字段语义：
//   - Events: branch 内 AddEvent 的目标
//   - Parent: 嵌套 fan-out 时父 lane（业务多层 ParallelAgent 嵌套时）
//
// **R33 audit A HIGH-1 fix**：嵌套 fan-out (Parallel 内嵌 Parallel) 时多个 inner
// branch 同时 commit 到同一 outer lane.Events → race detector 100% 触发。lane
// 想"lock-free"需建立在"join 时单 goroutine 串行 commit"前提，crew-go 的
// branchFn 各自 commit 不同时间到达 outer lane → 必须加 mu。同时 Events() 拼接
// 路径读 lane.Events 也走同 mu 防 read-while-write race。
//
// branch 完成时 `CommitLaneEvents(parent, child)` 把 child.laneEvents.Events
// 按 TS 排序后 append 到 parent.events（或 parent.laneEvents.Events 如果 parent
// 也在 lane 内，写时加 mu）。
type laneEvents struct {
	mu     sync.Mutex // R33 A HIGH-1: 嵌套 fan-out 时多 inner branch 同时 append 同 outer Events 必须加锁
	Events []*AgentEvent
	Parent *laneEvents
}

// NewRunSession 创建空 root RunSession，自动生成 RunID。
//
// 业务想自定义 RunID：直接构造结构体
//
//	sess := &agent_v2.RunSession{RunID: "conv_X:run_42"}
//
// ⚠️ **`:run_42` 那一段不是装饰**。RunID 是**一次任务**的身份（见 [RunSession.RunID]
// 的契约），拿会话主键当它使 ⟹ 同一场会话里的第二个任务会在收尾时删掉第一个任务
// 还挂着的存档。要么像上面那样把任务那一维拼进去，要么每个任务 `NextTask` 换一枚。
//
// 字面量构造时 valuesMu nil—— SetValue/GetValue 内 valuesOnce 保护 lazy init。
func NewRunSession() *RunSession {
	return &RunSession{
		RunID:    newUUID(),
		valuesMu: &sync.RWMutex{},
		values:   make(map[string]any),
	}
}

// NextTask 在同一场会话里开始一个新任务：换一枚新 RunID，**保留 values / events**。
// 按任务分的东西（存档、approve_always 规则）都挂在 RunID 名下，换了 RunID 自然就是新的一套。
//
//	for _, q := range questions {
//	    sess.NextTask()                  // 上一个任务到此为止
//	    ag.Run(ctx, sess, schema.UserMessage(q))
//	}
//
// 这是 [RunSession.RunID] 契约「一枚 RunID 只服务一个任务」的执行方式。跨任务要攒的
// 东西（业务 KV、审计事件流）挂在**对象**上，所以复用对象即可，不必也不该复用 RunID。
//
// **就地改，不返新对象**——同一个 sess 指针可能已经交给了别处（`ForkLane` 的父引用、
// 业务自己存的句柄），返新对象会让那些引用悄悄指向上一个任务。
//
// ⚠️ **只在「上一个任务真的结束了」时调**。任务的终结是成功、终局失败、或你决定放弃它；
// **中断不是终结**——那时该调的是 `Resume`，换了 RunID 就再也找不到那份存档了。
// 放弃一个中断中的任务时，存档不会自己消失（`ListPending` 还看得到），要清就在换之前
// 显式 `DeleteSubtree(oldRunID, "")`。
//
// ⚠️ **不是并发安全的**——`RunSession` 的其余方法（`SetValue` / `AddEvent` / `Events` …）
// 都可并发调，这一个不行：它就地改公开字段 `RunID`，与任何并发读者构成 data race
// （`-race` 实测触发）。加锁也没用（`RunID` 是公开字段，读侧锁不住）。
// **正确用法本来就是任务间串行调**——上一个任务已经结束、下一个还没开始，那一刻没有
// 并发读者；进行中的 Run 里调它是误用（Resume 会找不到存档）。
func (s *RunSession) NextTask() {
	s.RunID = newUUID()
}

// NewSharedChildSession 构造"半共享 child sess"——values map / valuesMu 与父共享
// 指针（业务自定义 KV 跨 sub-agent 累积 + 并发 race-free），
// events / eventsMu 独立（审计层不交叉污染），parentSess 反向指针让业务自定义
// Store 实现的 Search 可上追看父对话历史。
//
// **设计要点**：Values 共享 + Events 独立的精细化语义，同时保留 NS 物理分离 cp。
//
// **使用约束**：
//   - parent 可为 nil——fallback 返 NewRunSession()（不破坏调用，但失去半共享语义）
//   - RunID 沿用父 RunID（cp 持久化按 conv 边界对齐 — 父子 cp 同 sid 不同 ns）
//   - 子 sess.values 和父共享指针——业务侧 SetValue 立即可见父；valuesMu 指针共享
//     让父子并发 SetValue race-free
//   - 子 sess.events 独立——子 AddEvent 不污染父审计流，子 eventsMu 独立避竞争
func NewSharedChildSession(parent *RunSession) *RunSession {
	if parent == nil {
		// 防御：业务误传 nil 不应 panic（caller 已防御），fallback 走根 sess
		return NewRunSession()
	}
	// 触发父 valuesOnce 让父 valuesMu 一定非 nil（业务字面量构造父时 nil 直到
	// 首次 SetValue/GetValue/此处才初始化），保 child 共享有效 mu。
	parent.ensureValues()
	return &RunSession{
		RunID:      parent.RunID,
		valuesMu:   parent.valuesMu, // 共享 mu 指针 — race-free 父子并发
		values:     parent.values,   // 共享 map — 业务自管 namespace 避 key 冲突
		parentSess: parent,
		// eventsMu 值类型 + events nil — 独立每 sess
	}
}

// effectiveEventsMu 返回保护 events 的实际锁——eventsMuRef 非 nil 用 ref（ForkLane
// child 共享父锁），否则用本地 eventsMu 地址（主线 sess / 业务字面量构造）。
//
// **why**：ForkLane child 必须与 parent 共享 events + 共享 eventsMu 才能让 read 路径
// race-free（parent 主线 goroutine 可能并发 AddEvent 写 events，child 在 lane 内 Events()
// 读 committed 部分需要走同一 mu）。sync.RWMutex 是值类型不能直接复制，用 ref 字段实现。
func (s *RunSession) effectiveEventsMu() *sync.RWMutex {
	if s.eventsMuRef != nil {
		return s.eventsMuRef
	}
	return &s.eventsMu
}

// ensureValues lazy init valuesMu + values。供 SetValue/GetValue/NewSharedChildSession
// 调用。sync.Once 保 init 自身 race-free。
func (s *RunSession) ensureValues() {
	s.valuesOnce.Do(func() {
		if s.valuesMu == nil {
			s.valuesMu = &sync.RWMutex{}
		}
		if s.values == nil {
			s.values = make(map[string]any)
		}
	})
}

// valuesStore 返回这个 session 的业务 KV 存储的身份：共享 values 的几个 session（父与
// NewSharedChildSession / ForkLane 派生的子）返回同一个值。nil session 返回 nil。
func (s *RunSession) valuesStore() *sync.RWMutex {
	if s == nil {
		return nil
	}
	s.ensureValues()
	return s.valuesMu
}

// ParentSession 返父 sess（NewSharedChildSession 模式构造的 child sess 持有）。
// nil 表示这是根 sess（默认 NewRunSession / 用户直接 new 的）。
func (s *RunSession) ParentSession() *RunSession {
	if s == nil {
		return nil
	}
	return s.parentSess
}

// RootSession 沿 parentSess 链上追到根 sess（无 parent 的）。
// 单层 sess（无父）返自己。
//
// **典型用途**：业务自定义 Store 实现的 Search 想看跨所有 sub-agent 的对话历史时，
// 沿链上追到根再调 sess.HistoryFor(...)。
//
//	func (s *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
//	    sess := agent_v2.GetSession(ctx)
//	    if sess == nil { return nil, nil }
//	    rootHistory := sess.RootSession().HistoryFor("")
//	    // 用 rootHistory 调 semantic search backend
//	    return s.backend.Search(ctx, ns, rootHistory, opts...)
//	}
func (s *RunSession) RootSession() *RunSession {
	if s == nil {
		return nil
	}
	cur := s
	// 防御循环：理论 parentSess 链单向无环，但加 N 级保护
	for i := 0; i < 100; i++ {
		if cur.parentSess == nil {
			return cur
		}
		cur = cur.parentSess
	}
	return cur // 100 层兜底（业务异常嵌套时不死循环）
}

type sessionKey struct{}

// WithSession 把 RunSession 注入 ctx——节点函数 / hook 可用 GetSession(ctx) 取出。
//
// 顺带把 ctx 上的 sid 推成 s.RunID（覆盖已有的；RunID 为空时不推）：**任务的身份是 session 的
// RunID**，ctx 上的 sid 只是它的副本——观测层（OnAgentStart 早于 runGraph，不提前推的话顶层 agent
// span 落到 sid="" 的孤儿 session、缺 gen_ai.request.id）与嵌套图都读它。覆盖而不是「没有才补」：
// 长寿的 ctx 上可能钉着上一个任务的 sid（业务按契约 `NextTask` 换了 RunID），「没有才补」的话框架
// 照旧认那个旧 sid、新任务接着跑被放弃任务的存档（实测）。嵌套位置上的 session 的 RunID 必须等于
// 所在任务（编排成员拿父 session 本身、并行分支拿 `ForkLane` 派生的、嵌套调用的被调方拿框架给的，
// RunID 都是所在任务；业务自己传的对不上时入口直接拒，见 [resolveRunSession]），覆盖不改变它沿用
// 所在任务 sid 的行为。
func WithSession(ctx context.Context, s *RunSession) context.Context {
	ctx = context.WithValue(ctx, sessionKey{}, s)
	if s != nil && s.RunID != "" {
		ctx = compose_v2.WithSessionIDCtx(ctx, s.RunID)
	}
	return ctx
}

// sessionForCtx 给没拿到 session 的一次 Run 补一个：在一个正在执行的任务里（[enclosingTask]）时
// RunID 就是那个任务；否则沿用 ctx 上的 sid（业务在长寿 ctx 上钉的）；都没有才新铸一枚。一律新铸
// 的话，没传 session 的嵌套调用每次都是一个新任务、存档落在随机 sid 下，续跑时找不到。
func sessionForCtx(ctx context.Context) *RunSession {
	s := NewRunSession()
	if task, _ := enclosingTask(ctx); task != "" {
		s.RunID = task
	} else if sid := compose_v2.GetSessionID(ctx); sid != "" {
		s.RunID = sid
	}
	return s
}

// resolveRunSession 是编排 Run 入口拿 session 的唯一写法，**在任何执行之前**：
//
//   - nil ⟹ 按 [sessionForCtx] 补一个（框架自己的对象）。补出来的 RunID 同样过下面「脱离来源」那一道：
//     脱离之后又把所在任务的 sid 推回 ctx（想让独立任务的 span 带上这个请求的 sid）、session 传 nil 时，补出来的
//     就是那个仍在执行的任务（曾经补完直接返回，那个 agent 当新任务收尾、删了所在任务挂着的存档，审计实测）；
//   - RunID 为空 ⟹ 拒：RunID 是任务的身份、存档的主索引，框架替它补就得改业务的对象（同一个
//     字面量 session 被两个 goroutine 同时用时那一写就是 data race，实测两次中断只剩一份存档）；
//   - 在一个正在执行的任务里、RunID 对不上那个任务 ⟹ 拒（见 [enterRunTask]「为什么嵌套位置上
//     不能新开」）。出路：嵌套调用走 `RunSubAgent`（它给被调方一个属于当前任务的 session、推它
//     自己的 ns；传 `GetSession(ctx)` 裸调不行——没有自己的 ns，存档落进父图的槽），要开独立任务
//     先 [DetachTask]；
//   - 脱离之后 RunID 是脱离出来之前所在的、此刻仍在执行的任务（[DetachTask] 或 `compose_v2.DetachExecution`，
//     脱离几层都算，见 compose_v2.DetachedFrom）⟹ 拒：那不是独立任务，是在那个任务执行的同时再开一遍它——
//     清掉它的审批规则、成功收尾时连它挂着的存档一起删（实测）。
//
// 拒绝的错误见 [rejectCall]：恒不可重试，在任务里（含从任务里脱离出来）再 wrap `compose_v2.ErrRunAborted`。
func resolveRunSession(ctx context.Context, s **RunSession, agentName string) error {
	if *s == nil {
		filled := sessionForCtx(ctx)
		if detachedFromRunning(ctx, filled.RunID) {
			return rejectCall(ctx, "agent %q: 脱离之后 session 传了 nil，按 ctx 上的 sid 补出来的 RunID %q 是脱离出来之前所在的"+
				"任务（它此刻仍在执行）；独立任务要有自己的 RunID（传 NewRunSession()，别把这个任务的 sid 推回 ctx）", agentName, filled.RunID)
		}
		*s = filled
		return nil
	}
	runID := (*s).RunID
	if runID == "" {
		return rejectCall(ctx, "agent %q (ns=%q): session 的 RunID 为空（RunID 是任务的身份、存档的主索引）；"+
			"用 NewRunSession() 构造，或给它一枚", agentName, compose_v2.GetGraphNS(ctx))
	}
	if task, _ := enclosingTask(ctx); task != "" && task != runID {
		return rejectCall(ctx, "agent %q (ns=%q): session 的 RunID %q 不属于所在的任务 %q；"+
			"嵌套调用走 RunSubAgent（它给被调方一个属于当前任务的 session 和自己的 ns），"+
			"要开一个独立的任务先 DetachTask(ctx)",
			agentName, compose_v2.GetGraphNS(ctx), runID, task)
	}
	if detachedFromRunning(ctx, runID) {
		return rejectCall(ctx, "agent %q: 脱离之后传的 session 的 RunID %q 是脱离出来之前所在的任务（它此刻仍在"+
			"执行）；独立任务要有自己的 RunID（NewRunSession() / sess.NextTask()）", agentName, runID)
	}
	return nil
}

// EnterRun 给自己实现 [Agent] 的业务（第十个实现，如 `a2a.RemoteAgent`）在 Run 入口调：做本包 9 个
// 编排的 Run 入口做的同一件事，返回接着用的 ctx 与 session。
//
//   - 判 session：nil ⟹ 补一个——在一个正在执行的任务里时 RunID 就是那个任务，否则沿用 ctx 上的
//     sid，都没有才新铸；RunID 为空 ⟹ 拒；在一个正在执行的任务里（节点体、钩子、嵌套调用的被调方……）
//     RunID 对不上那个任务 ⟹ 拒；脱离之后 RunID 是脱离出来之前所在的任务 ⟹ 拒。拒绝发生在任何
//     执行之前，错误不可重试（任务里的再 wrap `compose_v2.ErrRunAborted`）。
//   - 盖上这个任务的章；不在任务里时宣告「这是新任务」：业务钩子里发起的嵌套调用认得出所在任务。
//   - 遮住调用方留在 ctx 上的节点授予：你的 agent 级钩子跑在图外，经 [RegrantFromNode] 拿不到调用方的
//     转交 / 路由能力（同 9 个编排）。
//
// 返回的 ctx 交给业务钩子（经 `compose_v2.CallHook` 调）与后续执行。
//
// 为什么自研实现也要守：session 的 RunID 是任务的身份，实现往往用它分自己的状态（RemoteAgent 的
// 远端 task 句柄按它存）。嵌套位置上随手给一个新 session，父节点每次重跑都是一个新的远端 task、
// 续跑时读不到上一次的句柄——用户的回答被当成新 task 的第一条消息发出去，原来那个远端 task
// 一直挂着（实测）。授予那条同理：这里曾经只判 session，节点里被裸调的 RemoteAgent / 自研 Agent 的
// OnAgentStart 经 RegrantFromNode 拿到了调用方的路由能力，被请教的 agent 替调用方定了路由（实测，
// `err == nil`）。
func EnterRun(ctx context.Context, session *RunSession, agentName string) (context.Context, *RunSession, error) {
	if err := resolveRunSession(ctx, &session, agentName); err != nil {
		return ctx, nil, err
	}
	return enterRunTask(ctx, session), session, nil
}

// GetSession 从 ctx 取 RunSession。nil 表示业务未注入。
func GetSession(ctx context.Context) *RunSession {
	s, _ := ctx.Value(sessionKey{}).(*RunSession)
	return s
}

// **R32+ 视角 4 audit 删**: AddEventInGraph 生产 0 caller, ev.NS 字段无人读 (仅
// godoc 提到), 设计意图"子图嵌套场景按 ns 过滤回放"已被 ParallelAgent ForkLane +
// CommitLaneEvents (ISSUE-80) lane 物理隔离替代。CLAUDE.md "项目未上线不留兼容
// 路径", 删干净比留死代码好。

// AddEvent 追加事件到会话历史。
//
// 防御性过滤：chunk 事件（IsChunk=true）不入 events——chunk 应通过
// OrchestrationHooks.OnNodeEvent 实时投递给前端 SSE，写历史会刷爆 sess.Events。
//
// CreatedAt 为 0 时自动填充 time.Now().UnixNano()——同时作 lane events 合并时的
// 时序排序键（TS）。
// nil event 静默跳过。
//
// **lane events 路由**（ISSUE-80）：sess.laneEvents 非 nil（ForkLane 后的 child
// sess）→ append 到 lane.Events lock-free（每 goroutine 独立 lane）；nil（主线
// sess）→ append 到 events + eventsMu。
func (s *RunSession) AddEvent(e *AgentEvent) {
	if e == nil {
		return
	}
	if e.Action != nil && e.Action.IsChunk {
		return
	}
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().UnixNano()
	}
	// lane 内 → 走 lane.mu 保护 (R33 A HIGH-1: 嵌套 fan-out 时同 lane 多 goroutine append)
	if s.laneEvents != nil {
		s.laneEvents.mu.Lock()
		s.laneEvents.Events = append(s.laneEvents.Events, e)
		s.laneEvents.mu.Unlock()
		return
	}
	// 主线 → 锁保护写共享 events
	mu := s.effectiveEventsMu()
	mu.Lock()
	s.events = append(s.events, e)
	mu.Unlock()
}

// Events 返回所有事件的快照（按追加顺序）。
//
// **lane events 拼接视图**（ISSUE-80）：sess.laneEvents 非 nil 时返 committed events
// + lane 链所有未 commit events——业务读 sess.Events() 在 lane 内时能看到完整视图
// （含父已提交 + 自己 lane 链 in-flight），与主线行为一致。
func (s *RunSession) Events() []*AgentEvent {
	// 主线 sess（无 lane） → 直接返 events 副本
	if s.laneEvents == nil {
		mu := s.effectiveEventsMu()
		mu.RLock()
		defer mu.RUnlock()
		out := make([]*AgentEvent, len(s.events))
		copy(out, s.events)
		return out
	}
	// lane 内 → 拼 committed + lane 链
	mu := s.effectiveEventsMu()
	mu.RLock()
	committed := make([]*AgentEvent, len(s.events))
	copy(committed, s.events)
	mu.RUnlock()

	// 沿 lane 链上追（外层 lane → 内层 lane）—— Parent 指针不可变 (创建后只读) 安全无锁；
	// 但 lane.Events slice 可变 → 走 lane.mu RLock-equivalent (Mutex.Lock + 复制) 防 race
	// (R33 A MED-1: 与 A HIGH-1 同根因)
	var laneSlices [][]*AgentEvent
	totalLaneSize := 0
	for lane := s.laneEvents; lane != nil; lane = lane.Parent {
		lane.mu.Lock()
		if len(lane.Events) > 0 {
			snapshot := make([]*AgentEvent, len(lane.Events))
			copy(snapshot, lane.Events)
			laneSlices = append(laneSlices, snapshot)
			totalLaneSize += len(snapshot)
		}
		lane.mu.Unlock()
	}
	out := make([]*AgentEvent, 0, len(committed)+totalLaneSize)
	out = append(out, committed...)
	// 反序：父 lane events 在前，子 lane events 在后（时序正确）
	for i := len(laneSlices) - 1; i >= 0; i-- {
		out = append(out, laneSlices[i]...)
	}
	return out
}

// ForkLane 创建 ParallelAgent fan-out 用的 lane child sess——共享 values 指针 +
// 共享 valuesMu + 共享 events 主线 (commit 时合并回此) + 独立 laneEvents slice
// (每 branch goroutine 一份；R33 起 lane.mu 保护——branch 内业务可能再 spawn
// goroutine 写同 lane，纯 lock-free 不成立)。
//
// **使用约束**：
//   - 仅由 ParallelAgent.branchFn 调用——业务 NodeFunc 通常不直接用
//   - 调用方负责 branch 完成后调 `CommitLaneEvents(parent, child)` 合并回父
//   - 同时支持嵌套 fan-out (ParallelAgent 内嵌 ParallelAgent)——child.laneEvents.Parent
//     指向 parent.laneEvents 自动建链
//
// **vs NewSharedChildSession 区别**：
//   - NewSharedChildSession (AgentAsTool 用)：events + eventsMu 完全独立 — 子 events
//     不进父，沿 RootSession 链可看父
//   - ForkLane (ParallelAgent 用)：events 共享主线 (committed) + lane 私有 (in-flight)
//     — branch 完成时 lane events commit 回父，业务侧最终全可见
func (s *RunSession) ForkLane() *RunSession {
	s.ensureValues()
	// **R33 audit B HIGH-1 fix**: 共享 parent.events slice + parent.eventsMu 指针
	// 让 lane 内 sess.Events() 拼接 committed 部分能拿到父历史（child 直接引用
	// parent 的 events slice）。之前漏共享 → 子 lane 内
	// sess.Events()/HistoryFor/SelfHistory 拿不到父之前 AddEvent 的历史。
	//
	// child 不通过 events 路径写（lane 路由），所以 events 共享是 read-only 视图。
	// 但 parent 主线 goroutine 可能并发 AddEvent → 共享 eventsMu 让 lane 读时 RLock
	// 防 race。
	child := &RunSession{
		RunID:      s.RunID,
		valuesMu:   s.valuesMu,   // 共享 mu 指针 — race-free 父子并发
		values:     s.values,     // 共享 values map
		parentSess: s.parentSess, // 沿用父链（lane fork 不改变 sess 嵌套层级）
		laneEvents: &laneEvents{
			Events: make([]*AgentEvent, 0),
			Parent: s.laneEvents, // 嵌套 fan-out 时链回父 lane
		},
	}
	// events + eventsMu 共享父的指针 — sync.RWMutex 是值类型不能直接赋值，
	// 用辅助 helper 让 child Events() 走父的 mu。
	child.shareEventsFrom(s)
	return child
}

// shareEventsFrom 让 child sess 的 events 读路径走 parent 的 events + eventsMu。
// 这是 ForkLane 的辅助 helper——通过 events 字段共享指针 + eventsMu 必须用同一 RWMutex 实例。
//
// 实现技巧：sync.RWMutex 是值类型，直接赋值会复制锁状态——用指针字段 (eventsMuRef)
// 让 child 引用 parent 的 mu。由于现有 RunSession 字段 eventsMu 是值类型（兼容 v1
// 业务直接构造），加新 eventsMuRef *sync.RWMutex 字段：非 nil 时优先用，nil 时走
// 自己的 eventsMu。
//
// **并发安全 (R34 ISSUE-87 fix)**：读 parent.events slice header 必须持 parent
// effective mu RLock — parent 主线 goroutine 并发 AddEvent (Lock 写) 或 sibling
// branch CommitLaneEvents (Lock 写) 时无锁读会触发 race detector (TestAggregator_Basic
// 在 ParallelAgent fan-out 各 branch goroutine 并发 ForkLane → shareEventsFrom 时
// ~40% 命中). parent.eventsMuRef 在 parent 自身 ForkLane 内一次写定之后只读,
// goroutine spawn 已建 happens-before, 无并发写不需锁. s 是新 child 尚未对外暴露,
// 写 s.events / s.eventsMuRef 不需锁.
func (s *RunSession) shareEventsFrom(parent *RunSession) {
	mu := parent.effectiveEventsMu()
	mu.RLock()
	s.events = parent.events // 共享 slice 指针 (read-only 视图)
	mu.RUnlock()
	s.eventsMuRef = parent.eventsMuRef // 共享父的 mu 引用 (parent 也可能是 lane)
	if s.eventsMuRef == nil {
		// parent 是主线 sess (无 ref) — 用 parent 自己的 mu 地址
		s.eventsMuRef = &parent.eventsMu
	}
}

// CommitLaneEvents 把 child sess 的 lane events 合并回 parent sess（按 child lane 内原有
// 顺序 append，不跨 branch 排序）。
//
// 用于 ParallelAgent.branchFn 完成时把 lane 内 in-flight events 提交到 parent。各 branch
// 完成时分别调用——父审计流跨 branch 顺序 = branch 完成顺序（各 branch 内部事件连续且 TS 有序）。
//
// **路由规则**：
//   - parent.laneEvents != nil（parent 也在 lane 内，嵌套 fan-out）→ append 到
//     parent.laneEvents.Events（不加锁，单 goroutine）
//   - 主线 parent → events + eventsMu 加锁
//
// child 必须是 parent.ForkLane() 的产物——其他情况行为未定义。
func CommitLaneEvents(parent, child *RunSession) {
	if parent == nil || child == nil || child.laneEvents == nil {
		return
	}
	// 读 child lane.Events 走 lane.mu (R33 A HIGH-1: 防 child branch 内还在写时 commit 读)
	child.laneEvents.mu.Lock()
	laneEvts := make([]*AgentEvent, len(child.laneEvents.Events))
	copy(laneEvts, child.laneEvents.Events)
	child.laneEvents.mu.Unlock()
	if len(laneEvts) == 0 {
		return
	}
	// parent 也在 lane 内（嵌套 fan-out）— 走 parent lane.mu 保护 (R33 A HIGH-1)
	if parent.laneEvents != nil {
		parent.laneEvents.mu.Lock()
		parent.laneEvents.Events = append(parent.laneEvents.Events, laneEvts...)
		parent.laneEvents.mu.Unlock()
		return
	}
	// 主线 parent
	mu := parent.effectiveEventsMu()
	mu.Lock()
	parent.events = append(parent.events, laneEvts...)
	mu.Unlock()
}

// RestoreEvents 用持久化的事件切片填充 events——业务从外部存储（数据库 / KV）
// 加载历史事件后注入新建的 sess 时调用。
//
// **注意**：当前 compose_v2.Checkpoint 不持有 events 字段——agent_v2 框架在
// Resume 时不自动调用本方法（sess.events 由 caller 通过 WithResumeSession 续传）。
// 本方法是 caller-facing API 让业务自管 events 持久化路径。
//
// 直接赋值不触发 hook / 观测回调避免事件重放造成的告警二次触发。
// 调用方应保证 events 切片此后不再被修改（此处不做深拷贝）。
func (s *RunSession) RestoreEvents(events []*AgentEvent) {
	mu := s.effectiveEventsMu()
	mu.Lock()
	s.events = events
	mu.Unlock()
}

// History 从会话事件重建对话消息历史（按时序）。
func (s *RunSession) History() []*schema.Message {
	evs := s.Events()
	msgs := make([]*schema.Message, 0, len(evs))
	for _, e := range evs {
		if e.Output != nil && e.Output.Message != nil {
			msgs = append(msgs, e.Output.Message)
		}
	}
	return msgs
}

// SetValue 在会话 KV 存储中存储任意值——**per-sess 不走 cp 持久化**。
//
// **生命周期**：跟 RunSession 实例——业务复用 sess 跨 Run 累积仍可见, 新 sess
// (典型 HITL Resume / 新对话场景) 看不到上 sess 的 values. cp 序列化结构
// (`compose_v2.Checkpoint`) 不含 values 字段, Resume restore cp 不恢复 values.
//
// **典型用途** (sub-agent 间共享业务 KV, 本 sess 内):
//   - 业务 user_id / tenant_id 注入承载点 (caller middleware 注入, sub-agent GetValue 取)
//   - Plan 当前 step / Routing 路径标记 等编排内部状态
//
// **API 形态**: 只 2 个 method (SetValue/GetValue), 批量场景业务自循环。
//
// **持久化策略**: values 跟 cp 解耦 — 不走 cp 序列化跨 Resume 自动恢复, 跨 thread
// 数据走专用 `store.Store` 后端。
//
// **跨 Run 持久化数据不走这里**——用 `store.Store`(`store.Put(ctx, NS, key, val)`)
// 走业务后端 (Redis/PG/InMemory) 持久化. 4 层 state 边界完整决策见
// [`doc/v2/store.md` §1](../doc/v2/store.md).
//
// **❌ 反模式**: `sess.SetValue("user.theme", "dark")` 期待跨 Run 拿到 — Run 2
// 新 sess 拿 nil. 修法: `st.Put(ctx, store.NS("users", uid), "theme", "dark")`.
//
// **并发安全**: 半共享 child sess (`NewSharedChildSession`) 父子共享 valuesMu
// 指针, 并发 SetValue/GetValue race-free. lazy init valuesMu + values 让业务直接
// 构造字面量 (`&RunSession{RunID: "biz-id"}`) 不用手动初始化 (valuesOnce 保 init
// race-free).
func (s *RunSession) SetValue(key string, val any) {
	s.ensureValues()
	s.valuesMu.Lock()
	defer s.valuesMu.Unlock()
	s.values[key] = val
}

// GetValue 从会话 KV 取值——**per-sess 不走 cp 持久化**, 跟 SetValue 对称.
//
// 详细生命周期 / 反模式 / 跨 Run 走 store 的边界见 SetValue godoc + [`doc/v2/store.md` §1].
func (s *RunSession) GetValue(key string) (any, bool) {
	s.ensureValues()
	s.valuesMu.RLock()
	defer s.valuesMu.RUnlock()
	v, ok := s.values[key]
	return v, ok
}

// CompactionSummaryAnnotationKey 是 AutoCompact 在 summary 消息上打的 annotation
// key——值类型 bool。SelfHistoryCompacted 按此 marker 跨 Run 截断早期事件。
const CompactionSummaryAnnotationKey = "compaction_summary"

// **R32+ 视角 4 audit 删**: SupersededEventAnnotation + SupersedeEventsByMarker
// 生产 0 caller, godoc 自称"AutoCompact 调用"但实际 0 调用; 已被 SelfHistoryCompacted
// 倒扫 + CompactionSummaryAnnotationKey marker 替代 (R7 audit HIGH-1 文档化此事实)。

// SelfHistory 返回本 agent（按 AgentName 匹配）产出的所有消息——按 events 时序。
//
// 用途：BasicAgent 的 buildInitialMessages 跨 Run 复用 session 时，从本 agent
// 自产事件流重建初始 messages 流（不引入跨 agent cross-talk）。
func (s *RunSession) SelfHistory(ownerAgentName string) []*schema.Message {
	evs := s.Events()
	out := make([]*schema.Message, 0, len(evs))
	for _, e := range evs {
		if e.AgentName != ownerAgentName {
			continue
		}
		if e.Output != nil && e.Output.Message != nil {
			out = append(out, e.Output.Message)
		}
	}
	return out
}

// HistoryFor 返回**跨 agent 改写后**的完整对话视图——非本 agent 产出的
// assistant(tool_call) / tool 消息降级为 user("For context: [name] said: ...")。
//
// 跨 agent 消息改写规则：
//   - assistant.Content non-empty → "[X] said: <content>"
//   - assistant.tool_calls → "[X] called tool: `<name>` with arguments: <args>"
//   - role=tool + content non-empty → "[X] `<tool>` tool returned result: <content>"
//
// **vs SelfHistory**：SelfHistory 只返本 agent 自产消息（用于 LLM 输入流，
// 避免跨 agent cross-talk）；HistoryFor 返全量改写视图（用于审计 / 用户 replay /
// sub-agent 看父历史时格式化）。
//
// 空 ownerAgentName 视作"无所属"——所有非本 agent 消息（即所有 assistant/tool
// 消息）都按 cross-agent 改写。
func (s *RunSession) HistoryFor(ownerAgentName string) []*schema.Message {
	evs := s.Events()
	out := make([]*schema.Message, 0, len(evs))
	for _, e := range evs {
		if e.Output == nil || e.Output.Message == nil {
			continue
		}
		msg := e.Output.Message
		if e.AgentName == ownerAgentName {
			out = append(out, msg)
			continue
		}
		// cross-agent 改写：assistant(tool_call) / tool 降级为 user("For context: ...")；
		// 改写后无实质内容（空 content 无 tool_call）返 nil 跳过，不注入 junk。
		if rewritten := rewriteCrossAgentMessage(msg, e.AgentName); rewritten != nil {
			out = append(out, rewritten)
		}
	}
	return out
}

// rewriteCrossAgentMessage 把跨 agent 的 assistant/tool 消息降级为 user("For context: ...")。
// 非 assistant/tool（user/system）直接返原 msg（无需改写）。改写后若无实质内容
// （空 content 且无 tool_call 的 assistant、空 content 的 tool）返 nil——调用方据此跳过，
// 避免注入只有 "For context:" 前缀的 junk user 消息（audit 视角3 LOW，bug class #9）。
func rewriteCrossAgentMessage(msg *schema.Message, fromAgent string) *schema.Message {
	if msg == nil {
		return nil
	}
	if msg.Role != schema.RoleAssistant && msg.Role != schema.RoleTool {
		return msg
	}
	const prefix = "For context:"
	var sb []byte
	sb = append(sb, []byte(prefix)...)
	if msg.Role == schema.RoleAssistant {
		if msg.Content != "" {
			sb = append(sb, []byte(fmt.Sprintf(" [%s] said: %s.", fromAgent, msg.Content))...)
		}
		for _, tc := range schema.GetToolCalls(msg) {
			sb = append(sb, []byte(fmt.Sprintf(" [%s] called tool: `%s` with arguments: %s.",
				fromAgent, tc.Function.Name, tc.Function.Arguments))...)
		}
	} else if msg.Role == schema.RoleTool && msg.Content != "" {
		sb = append(sb, []byte(fmt.Sprintf(" [%s] tool returned result: %s.",
			fromAgent, msg.Content))...)
	}
	if len(sb) == len(prefix) {
		return nil // 改写后无实质内容 → 跳过，不注入 junk "For context:"
	}
	return schema.UserMessage(string(sb))
}

// **R32+ 视角 4 audit 删**: SupersedeEventsByMarker 实现已删 (生产 0 caller +
// 设计被 SelfHistoryCompacted 倒扫替代)。

// HistoryForRaw 返本 sess 全量消息——非 owner agent 消息**不改写**保留原 role
// (assistant/tool), 仅在每条消息 Annotations 标 from-agent name。
//
// **ISSUE-84 MED-3 fix**: HistoryFor("") 把所有非 owner 消息降级 user 让多层
// 嵌套场景 LLM 看"全 user 消息流" 困惑 (cross-agent 改写为 "For context: [name]
// said: ..."). HistoryForRaw 保 role + 用 Annotations[`from_agent`]=name 注 owner,
// 业务想用 normal 对话流给 LLM 时用此。
//
// **vs HistoryFor(ownerName)**: HistoryFor 是 cross-agent 改写降级 user (适合
// 摘要输入, 返新 UserMessage 完全不共享底层指针), HistoryForRaw 保原 role
// (适合 LLM 对话历史)。
//
// **⚠️ ISSUE-84 audit MED-2 警示 — 浅拷贝陷阱**: 返回的 *schema.Message 是
// `newMsg := *msg` 浅拷贝 + Annotations map 一层深拷贝, 但 `Annotations[k]` 的
// `any` value (如 `map[string]any` / slice) **仍共享底层指针**, ContentBlocks /
// Metadata / RunID 等 *schema.Message 内部 ptr 字段也共享。**业务勿 mutate**
// 返回值 (含 Annotations[k].(map).set / ContentBlocks append) — 会污染 sess.events
// 审计层。需修改时业务自己完整深拷贝。
//
// 空 ownerAgentName 视作"无所属"——所有消息原样返 (含 from_agent annotation)。
func (s *RunSession) HistoryForRaw(ownerAgentName string) []*schema.Message {
	evs := s.Events()
	out := make([]*schema.Message, 0, len(evs))
	for _, e := range evs {
		if e.Output == nil || e.Output.Message == nil {
			continue
		}
		msg := e.Output.Message
		if e.AgentName == ownerAgentName {
			out = append(out, msg)
			continue
		}
		// 非 owner agent — 保原 role + 标 from_agent annotation
		newMsg := *msg
		if newMsg.Annotations == nil {
			newMsg.Annotations = map[string]any{}
		} else {
			// 深拷贝 Annotations 防 mutate 原 events
			ann := make(map[string]any, len(newMsg.Annotations)+1)
			for k, v := range newMsg.Annotations {
				ann[k] = v
			}
			newMsg.Annotations = ann
		}
		newMsg.Annotations["from_agent"] = e.AgentName
		out = append(out, &newMsg)
	}
	return out
}

// SelfHistoryCompacted 是 SelfHistory 的"压缩感知"变体：倒扫找最近一条带
// CompactionSummaryAnnotationKey marker 的消息，从该消息起返回——之前的事件认为已被覆盖。
//
// # 这是留给业务的扩展点，框架自己不写这个 marker
//
// 按 4 层压缩设计原则#5（压缩产物只走 channel/cp.Channels，**不写 session.Events**——见
// doc/v2/context-compaction.md），框架的 AutoCompact summary 只活在 modelFn 的局部切片里，
// **框架没有任何路径往 events 里 AddEvent 这个 marker**。所以走框架标准路径时本方法恒等价
// SelfHistory。这是**有意的**：让框架合成的摘要进业务审计流，等于往「用户说过什么」里掺
// 框架编的话。
//
// 但这个截断机制本身是通的，**业务可以自己当那个写入方**——它自己的审计流，它自己决定：
//
//	// 业务 hook：回合收尾时自行摘要并写进事件，下一次 Run 的 prior 从这儿起截断
//	func (h *myPostTurn) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, out *hook.AgentOutput) context.Context {
//		if compose_v2.EstimateMessageTokens(out.Messages) < h.threshold {
//			return ctx
//		}
//		resp, err := h.summarizer.Generate(ctx, append(out.Messages, schema.UserMessage("请用 <summary></summary> 包住摘要")))
//		if err != nil || resp == nil {
//			return ctx // 失败不打断主链路；⚠️ 控制流信号要自行判 compose_v2.IsInterrupt / ctx.Err()
//		}
//		sum := schema.UserMessage("# Previous Conversation Summary\n\n" + resp.Content)
//		sum.Annotations = map[string]any{agent_v2.AutoCompactSummaryAnnotation: true}
//		agent_v2.GetSession(ctx).AddEvent(agent_v2.NewOutputEvent(h.agentName, sum))
//		return ctx
//	}
//
// ⚠️ **三条前提，缺一条这段代码就静默不生效**（框架内置版正是栽在这儿才撤掉的，
// 见 doc/code/todo.md「压缩后置到回合结束」那条的非目标说明）：
//
//   - **只在没开 [WithHistory] 时成立**。开了之后 prior 改从 append-only 的 HistoryStore 读，
//     根本不看 events，这个 marker 一眼都不会被瞧见。
//   - **只在 HandoffHistorySelfOnly 下成立**。Rewrite（转交默认）/ Raw 走 HistoryFor /
//     HistoryForRaw，那两个读**全部** events、不认 marker ⟹ 摘要不但没截断任何东西，
//     还会被当成普通历史**追加进 prompt**（上下文反而变大）。
//   - **调用方必须自己持有并复用 RunSession**（跨任务用 [RunSession.NextTask]）。框架自带的
//     两个 wire 入口 **每轮新建会话**（`agui/handler.go` 的 serveRun、`a2a/server/executor.go`
//     的每个新 task），那两条路上跨轮零历史，写了也没有下一轮来读。
//
// 另：摘要是业务自己产的**合成消息**，会永久留在审计流、之后每轮喂给模型、并被后续摘要
// 反复转述。摘要模型的输出在这里的信任级别等同于对话内容本身——净化与来源框定由业务自负。
//
// 用途：buildInitialMessages 重建下个 Run 的初始 messages。
//
// 找不到 marker → 行为等同 SelfHistory（返回全量 owner 消息）。
func (s *RunSession) SelfHistoryCompacted(ownerAgentName string) []*schema.Message {
	full := s.SelfHistory(ownerAgentName)
	if len(full) == 0 {
		return full
	}
	cutoff := -1
	for i := len(full) - 1; i >= 0; i-- {
		m := full[i]
		if m == nil || m.Annotations == nil {
			continue
		}
		if v, ok := m.Annotations[CompactionSummaryAnnotationKey].(bool); ok && v {
			cutoff = i
			break
		}
	}
	if cutoff < 0 {
		return full
	}
	return full[cutoff:]
}
