package compose_v2

import (
	"fmt"
	"reflect"
	"sync"

	"crew-go/internal/panicerr"
)

// anyChannel 是 Channel[T] 的类型擦除接口，用于 ChannelManager 持有异构 channel
// 的 map。所有方法接收/返回 any，但内部通过 channelHandle[T] 桥到 typed reducer。
//
// **不导出**：用户只见 Channel[T] 泛型类型 + 包级 Get/Write 函数；anyChannel 是
// 框架内部的实现细节，避免用户绕过类型保护。
type anyChannel interface {
	name() string
	typeRep() reflect.Type
	zeroAny() any
	reduceAny(prev, next any) any
	singleWriterPerStep() bool
}

// ChannelManager 管理所有 channel 的注册 + 当前值 + reduce 调度。
//
// **设计决策**（针对 audit F4 异构 fan-in 类型保护 + F9 builder 类型擦除）：
// channels map 必须用 anyChannel 接口（type erasure）才能装异构 Channel[T]，但
// **写入路径 / 读取路径都强制走泛型 helper（Get[T] / Write[T]）**，类型擦除只在
// map 内部，对用户不可见。
type ChannelManager struct {
	mu       sync.RWMutex
	channels map[string]anyChannel // 注册表：name → channel handle
	values   map[string]any        // 当前值：name → reduced value（已写入的）
	// baseline 是**不经 reducer** 摆进来的底层值——子图 import 专用（见 setBaseline）。
	// 读 = reduce(baseline, values)；写只进 values；Snapshot 只存 values。
	// 非子图的 cm 恒为 nil，读路径零开销。
	baseline map[string]any

	// frontierAtExit 是 Run 在 ctx canceled / 中断时记录的当前帧 frontier，
	// 供 Snapshot 持久化 + ResumeFrom 恢复跑。
	// 仅 Run 在 graphLoop 内更新；外部不该直接 set。
	frontierAtExit []FrontierMember

	// cpEpochBaseline 是本 Run 的"世代基线"——**我最后一次看到或写入的那一代 cp**。
	//
	// 载入 cp 时设为它的 Epoch；本 Run 每次成功写 cp 后更新为刚写的那一代。
	// `WithStaleWriteGuard` 拿它跟槽里当前的比：不同 ⟹ 有别人在我不知情时写过。
	//
	// **为什么写后要更新**：不更新的话，Durability=Sync/Async 每超步写完，基线就
	// 停留在 Run 开始那一代，出口比对会把**自己写的**判成别人写的（误报）。
	cpEpochBaseline string

	// taskID 是「这次跑的是哪一个任务」的代号——新开时铸、续跑时从载入的 cp 读回。
	// 喂进中断寻址让跨任务的 ID 不再相等，见 `Checkpoint.TaskID`。
	taskID string

	// inputDigest 是这份进度新开时那次调用的输入摘要——新开时取本次给的、续跑时从载入的
	// cp 读回，写 cp 时原样带上。见 `Checkpoint.InputDigest`。
	inputDigest string

	// cpSlotWasEmpty 记「本 Run 载入时槽里什么都没有」——**与 cpEpochBaseline 空串
	// 不是一回事**，这个区分正是守卫能不能保护"会话第一轮"的关键。
	//
	// 空基线本来同时表达四种状态，行为却各不相同：
	//   - 载入时槽**是空的**  → 槽后来变非空 ⟹ 别人写的 ⟹ 该判 stale（本字段标它）
	//   - 载入到**无 Epoch 的旧 cp** → 无从比对，放行
	//   - `WithFreshRun`（刻意不载入）→ 按 FreshRun 语义放行
	//   - 没配 checkpointer → 无从谈起
	// 合并成一个"放行"的后果：两个副本同时收到同一个**新** sid（消息重复消费 /
	// 客户端重试 / 多副本同投）时双方都检不出，一方的 HITL 存档被静默覆盖——而这
	// 恰是 WithStaleWriteGuard godoc 举的头号场景的第一轮。
	cpSlotWasEmpty bool

	// cpSlotUnknown 记「探测槽状态时读失败了，空不空没问出来」——第三态，与
	// cpSlotWasEmpty 都为 false 时才是"确知载入到了 cp"。
	//
	// 只有 `WithResumeFrom` 路径会置它：那条路径的基线来自 caller 给的 cp，槽当前
	// 状态得单独探一次。探不到时**不能倒向任何一边**——当成"空"会让普通 Resume 在
	// 读抖动时误判成"载入时空、现在非空"（砸掉最常见路径）；当成"非空"（即维持
	// 两个 false）则会在槽真的为空时报"别人删了"，而一次读抖动就足以让 fork 到空
	// 槽的会话拿到 stale + 存档不落盘 + 永久不可 Resume。
	//
	// 处置：`cur == nil` 时放行（fail-open，与 checkStale 读失败时同款、与
	// checkpoint-storage.md 的契约一致），`cur != nil` 时照常比 Epoch。
	cpSlotUnknown bool

	// pendingInterrupts 是 Run 收到 InterruptError 时记录的 AskQuery，
	// 通过 Snapshot 暴露给 caller（caller 收集 InterruptID 后向 user 询问，
	// 然后通过 WithResumeAnswers 传回）。
	pendingInterrupts []*AskQuery

	// nodeStates 节点私有中断态（见 node_state.go）——键是 nodeStateKey(NodeID,
	// InstanceID, Visit)，值是 tagged JSON。**不跟 clearInterrupts 一起清**：那个
	// 清的是「给调用方看的挂起 query」，这里是「节点自用的进度」，Resume 时节点正要
	// 读它来跳过已完成的部分，清掉就等于白存。
	nodeStates map[string]taggedValue

	// entryRejections 记本次 Run 里各次节点执行名下发生过的嵌套图入口否决：节点执行键 → 产出那份
	// 输入的分域（执行级为空串）。成员结束时取走（见 applyNestedEntryRejections）。只活在内存里：
	// 它回答的是「这次执行里发生过什么」，执行结束就没用了。
	entryRejections map[string]map[string]struct{}

	// outerGens 是这张图的外层代际链（见 InterruptSite 的 OuterRerunGens）：Run 入口算好记在这里，
	// 承载在这张图某次节点执行里的嵌套图、交接出去的执行据它接着往下算（outerRerunGensFor）。按
	// 访问器所在的 cm 取、不按 ctx 取：图级钩子相位记在外层节点执行名下，而它拿到的 ctx 上已经是
	// 嵌套图自己的链（含承载它的那个节点的代际）——从那里交接出去时按 ctx 取会把调用方那一层又算
	// 进来，挂着的调用里已答的问题换 ID 再问一遍（审计实测）。只活在内存里，每次 Run 入口重算。
	outerGens []int

	// completedFrontier / deferredSends / completedTargets：PA-H1 部分失败超步的成功
	// 成员 + 其 SendFrom 指令 + **它们当时算好的出边目标**。失败路径 append（二次中断
	// 累积），Resume 重跑失败成员成功后 peekCompletedSiblings 取出——目标并入下一帧
	// frontier、sends 重放，都不重跑 NodeFunc。
	// 经 Snapshot/Restore 持久化到 cp.CompletedFrontier / cp.DeferredSends / cp.CompletedTargets。
	//
	// **出边目标必须存下来，不能 Resume 时重算**：出边是「那个成员成功」的一部分，在它
	// 成功的那一刻就随它的执行算完了（算不出来它当时就不算成功）。重算读到的是它跑完
	// **之后**的状态——含兄弟成员和重跑成员的产出——跟它自己当时看到的不是一份东西。
	completedFrontier []FrontierMember
	deferredSends     []SendDirective
	completedTargets  []NodeID

	// consumedAnswers 是**已经兑付过的答案位**（见 answer_consumption.go）。
	// 在提交点由本超步成功成员的记账并入；读取答案时跳过它们、并让落在其上的新提问
	// 当场终局失败。经 Snapshot/Restore 持久化到 cp.ConsumedAnswers。
	consumedAnswers []string

	// signals 是本图**已提交的上行信号**（见 signal.go）：在提交点由本超步成功成员做出的并入
	// （同一个收件人上后提交的为准），图成功跑完时整批交给调用这张图的那段执行。经 Snapshot/Restore
	// 持久化到 cp.Signals——停下时已提交的那些留在存档里，续跑跑完时一起交上去。
	signals []SignalRecord

	// completedSignals 是**部分失败超步里已成功成员**做出的上行信号——跟 completedFrontier 同一族：
	// 先记在一边、随存档走，等这一超步的其余成员续跑成功时跟它们合并（同一超步是并发的，一个收件人
	// 只能有一个成员做决定）后才并入 signals。直接并入 signals 的话，续跑成功的成员就成了「后做
	// 的」、覆盖掉兄弟的决定——同一张图停没停过结果不同（实测）。经 Snapshot/Restore 持久化到
	// cp.CompletedSignals。
	completedSignals []SignalRecord

	// recipientKey 是本图这次运行铸收件人用的密钥（见 [SignalRecipient]）：第一次用到时随机铸，
	// 经 Snapshot/Restore 持久化到 cp.RecipientKey，续跑时同一个位置算出同一个收件人。
	recipientKey []byte

	// pendingSendWrites 是"已被回滚、待 Resume 补 apply"的 SendDirective payload。
	//
	// **补的是 R27a 的裂缝**：dirWrites（本超步全部 directive 的 payload 汇总）
	// ApplyAll 失败时一条都没提交，但 target 已进 frontier 并被持久化——产 send
	// 的节点已成功、R27a 明确不让它重跑（否则 visit 漂移 + 副作用双跑），payload
	// 遂无从再生 → Resume 时 target 静默读到零值。存下这批 writes 让 Resume 在跑
	// 任何节点之前补 apply，等价于"那次 ApplyAll 成功了"。
	//
	// **与 deferredSends 的分工**（别复用后者）：deferredSends 走"并入下一超步
	// frontier"的重放机制，而这里的 target **已经在 frontier 里**了——塞进
	// deferredSends 会让 target 被调度两次。这里只补 writes，不碰调度。
	//
	// 经 Snapshot/Restore 持久化到 cp.PendingSendWrites。
	pendingSendWrites []ChannelWrite

	// visitCount 跟踪每个节点的成功完成次数（audit Round 3 Bug #1 修复）。
	// 用于 InterruptSite 编入 visit 维度——同节点跨多次访问（BasicAgent ReAct
	// 多轮 / LoopAgent body 重访 / PlanAgent 循环回 plan 节点）InterruptID 不串台。
	//
	// 语义：
	//   - 节点入口取当前值（ID 计算用）
	//   - **超步的提交点**（写入提交 + 出边算出来，全都成了）之后 ++ 让下次访问得新 visit
	//   - 这一步没成（节点失败 / 中断 / cond 算不出出边 / 写入提交失败）时 visit 不变
	//     ——保证重跑时同位置 ID 稳定，命中上次的答案
	//
	// **推进时刻是承重的**：曾经在「节点函数一返回」就 ++，而那时这一步远没定局——出边
	// 还没算、写入还没提交。任一处随后失败就留下「代际已推进、这一步却没成」的窗口，
	// 重跑时中断身份跟着漂（用户答过的问题被重新问）、节点私有态键位对不上。
	visitCount map[NodeID]int
}

// NewChannelManager 创建空的 ChannelManager。
//
// **audit Round 9 Bug C2 文档化**：cm 推荐每次 Run/Resume 创建新实例。同 cm
// 跨多次独立 Run 复用会让 visitCount / pendingInterrupts / frontierAtExit /
// completedFrontier / deferredSends / cpEpochBaseline / cpSlotWasEmpty 等状态累积污染（上行信号
// signals / completedSignals 在新开路径与 Restore 时会重置，见 Run；stale completed 会在下一
// Run 首个成功超步被并入出边——幽灵后继调度）——典型副作用是同节点跨多次 Run 的 InterruptID auto-fill 时
// visit 维度漂移。framework 内置编排（agent_v2 BasicAgent 等）已每次 Run/Resume
// 创新 cm，业务自定义 RunGraph 调用应同遵此约定。
//
// **audit Round 9 Bug C5 文档化**：NodeFunc 内启动的 goroutine 必须在节点
// return 前 join——超出节点生命周期访问 cm（含 Channels view + SendFrom）行为
// 未定义。框架不为业务 spawn 的 background goroutine 同步 cm 生命周期；不 join
// 在 ctx 不 cancel 时会持续读 stale 状态。
func NewChannelManager() *ChannelManager {
	return &ChannelManager{
		channels:   make(map[string]anyChannel),
		values:     make(map[string]any),
		visitCount: make(map[NodeID]int),
	}
}

// GetVisitCount 返回节点当前访问次数——节点入口算稳定 InterruptID 用。
// 节点**第一次**访问返 0，第二次 1，以此类推。
func (cm *ChannelManager) GetVisitCount(nodeID NodeID) int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.visitCount[nodeID]
}

// setCpEpochBaseline 更新世代基线——载入 cp 后、以及本 Run 每次成功写 cp 后调。
func (cm *ChannelManager) setCpEpochBaseline(epoch string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cpEpochBaseline = epoch
	cm.cpSlotWasEmpty = false // 有了具体世代，不再是"空槽"态
	cm.cpSlotUnknown = false  // 也不再是"没问出来"态——写成功后确知槽里是这一代
}

// setTaskID 定下本次跑的任务代号——**引擎在两条载入路径汇合后调一次，只此一处**。
// 续跑时传从 cp 读回的值、新开时传新铸的。见 `Checkpoint.TaskID`。
func (cm *ChannelManager) setTaskID(id string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.taskID = id
}

// TaskID 返本次跑的任务代号（写 cp 时原样带上、算中断寻址 ID 时喂进 hash）。
func (cm *ChannelManager) TaskID() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.taskID
}

// setInputDigest 定下这份进度的输入摘要——与 setTaskID 同处调用、同样只此一处。
func (cm *ChannelManager) setInputDigest(digest string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.inputDigest = digest
}

// markCpSlotEmpty 标记「载入时槽是空的」——auto-load 拿到 nil 时调。
//
// 与 setCpEpochBaseline 互斥：载入到 cp 走那个，载入到空槽走这个。
func (cm *ChannelManager) markCpSlotEmpty() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cpEpochBaseline = ""
	cm.cpSlotWasEmpty = true
	cm.cpSlotUnknown = false
}

// markCpSlotUnknown 标记「槽当前是空还是非空，探测时读失败没问出来」。
//
// **不动 baseline**：caller 给的那代仍是我们要比的基线，只是"载入时槽空不空"这一维
// 缺了信息。见 cpSlotUnknown 字段。
func (cm *ChannelManager) markCpSlotUnknown() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cpSlotUnknown = true
}

// CpSlotUnknown 返「载入时槽的空/非空状态是否未知」（见 cpSlotUnknown 字段）。
func (cm *ChannelManager) CpSlotUnknown() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cpSlotUnknown
}

// CpSlotWasEmpty 返「本 Run 载入时槽是否为空」（见 cpSlotWasEmpty 字段）。
func (cm *ChannelManager) CpSlotWasEmpty() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cpSlotWasEmpty
}

// CpEpochBaseline 返本 Run 的世代基线（见 cpEpochBaseline 字段）。
// 空串 = 没有基线可比（全新 Run，或载入的是引入 Epoch 之前的旧 cp）。
func (cm *ChannelManager) CpEpochBaseline() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cpEpochBaseline
}

// RegisterChannel 是注册 channel 的用户 API（包级泛型函数，绕开 Go 泛型方法
// 不能引入新类型参数的限制）。
//
// 业务经包级泛型 `compose_v2.WithChannel(b, ch)`（graph.go——**不是**方法形态，
// Go 泛型方法不支持额外类型参数）注册 channel；本函数是底层入口，
// builder 路径最终走到这里（API 形式一致）。
//
// 必须在 graph 编译期调用——运行时注册理论上 OK 但 PoC 不保证并发安全。
func RegisterChannel[T any](cm *ChannelManager, ch *Channel[T]) error {
	// audit⁹ HIGH-5 fix: nil 检 — cm nil → 业务 nil deref; ch nil → ch.asAny() nil deref.
	if cm == nil {
		return fmt.Errorf("RegisterChannel: nil ChannelManager")
	}
	if ch == nil {
		return fmt.Errorf("RegisterChannel: nil Channel[T] (含 Go typed nil pointer 陷阱)")
	}
	return cm.register(ch.asAny())
}

// register 是内部 anyChannel 注册——不导出避免用户绕过类型保护。
func (cm *ChannelManager) register(ch anyChannel) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	name := ch.name()
	if name == "" {
		return fmt.Errorf("channel name must not be empty")
	}
	if existing, ok := cm.channels[name]; ok {
		// 同名重复注册：检查类型一致——一致是无害幂等，不一致是 bug
		if !typesEqual(existing.typeRep(), ch.typeRep()) {
			return fmt.Errorf("channel %q already registered with type %v, cannot re-register with type %v",
				name, existing.typeRep(), ch.typeRep())
		}
		return nil // 幂等
	}
	cm.channels[name] = ch
	return nil
}

// read 取 channel 当前值。
// 第二个返回值表示该 channel 是否被写过（false 时第一个返回 nil）。
//
// **不导出**：通过 Get[T] 包级泛型函数对外（见 channel.go 下方）。
func (cm *ChannelManager) read(name string) (any, bool) {
	cm.mu.RLock()
	own, hasOwn := cm.values[name]
	base, hasBase := cm.baseline[name] // nil map 查表安全
	ch := cm.channels[name]
	cm.mu.RUnlock()

	// 两层都有时才需要合成——reducer 是业务代码，**放在锁外跑**（RWMutex 不可重入，
	// 业务 reducer 里再碰 cm 就卡死；提交区那条「锁内不跑业务代码」的纪律这里同样守）。
	if hasOwn && hasBase && ch != nil {
		return composeRead(ch, name, base, own), true
	}
	if hasOwn {
		return own, true
	}
	return base, hasBase
}

// composeRead 算 reduce(baseline, own)——子图读路径上 reducer 的**第三个**调用点
// （另两个：ApplyAll 归并段、出边叠加视图）。
//
// 这里的 recover **不吞 panic，只补归因再抛回去**。不补的话它会以「node "r": node panic
// recovered: <payload>」的样子落在**第一个读者**的栈上——而那个节点只做了一次 Get，
// 报文里既没有 "reducer" 也没有 channel 名，排查的人会去翻一个无辜节点的代码
// （独立审计实测；ApplyAll 那条路的报文是「reducer panic, no writes committed」，
// 这条路原本什么都没标）。原 payload 是 error 时用 %w 接上，errors.Is/As 链不断。
func composeRead(ch anyChannel, name string, base, own any) (v any) {
	defer func() {
		if r := recover(); r != nil {
			if rerr, ok := r.(error); ok {
				panic(fmt.Errorf("reducer panic during read of channel %q (compose baseline+own): %w", name, rerr))
			}
			panic(fmt.Errorf("reducer panic during read of channel %q (compose baseline+own): %v", name, r))
		}
	}()
	return ch.reduceAny(base, own)
}

// has 仅返回该 channel 是否有值（自己写过、或 import 进来的 baseline）——比 read 省合成。
func (cm *ChannelManager) has(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if _, ok := cm.values[name]; ok {
		return true
	}
	_, ok := cm.baseline[name]
	return ok
}

// ownValue 只返回**自己写过**的那部分（不含 baseline）——子图 export 用。
// 没写过返 (nil, false)，哪怕 baseline 里有值：那是 import 进来的，回送给父图就是回声。
func (cm *ChannelManager) ownValue(name string) (any, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	v, ok := cm.values[name]
	return v, ok
}

// setBaseline 把一批值**不经 reducer**摆成底层——子图 import 的唯一实现。
//
// # 为什么不能走 ApplyAll
//
// 「把父图的值复制给子图」曾经实现成「把它经子图的 reducer 写进去」。对 LastValue 那确实
// 是复制；对 Sum / Append 那是**相加**，而续跑时子 cp 里已经有上一次 import 进来的那份
// ⟹ 每续跑一次多算一份（实测：父 5 + 子图自己 3，中断续跑后子图读到 13、父最终 18）。
// 对称的那半在 export 侧：子图没碰过的 channel 被原样回送，父图 Sum 5→10、Append
// [a]→[a a]（实测），因为 read 分不清「子图产出的」和「刚灌进去的」。
//
// 两侧同一个根因——**reducer 写被借去做状态复制**，只在 LastValue 上碰巧成立。所以
// baseline 单独一层：import 只摆这层，子图自己的写只进 values，读 = reduce(baseline, own)，
// export 只送 own。Snapshot 不存 baseline（它每次进入都从父图重新摆，续跑天然拿到父图
// 最新值，且不会把子图已有的进度盖回去）。
//
// 整批替换：调用方每次都给全量（子图 import 列表），不做增量合并。全部名字先校验已注册
// 再替换——要么整批换上，要么一个都不动。
func (cm *ChannelManager) setBaseline(ws []ChannelWrite) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	next := make(map[string]any, len(ws))
	for _, w := range ws {
		if w == nil {
			continue
		}
		name := w.channelName()
		if _, ok := cm.channels[name]; !ok {
			return w.notRegisteredErr()
		}
		next[name] = w.valueAny()
	}
	cm.baseline = next
	return nil
}

// ApplyAll 批量执行 ChannelWrite——graphLoop 在超步合并点调用。
//
// **关键不变量**：这一批**要么整批落地，要么一条都不落地**。
//   - 同一个 channel 被写多次：按调用顺序链式 reduce。批内顺序是确定的（frontier
//     索引序），所以这里只要求**关联律**；交换律只在「扇出 + 只重跑失败分支」那条
//     跨 Resume 路径上才要求，而 LastValue / Append 并不满足它——见 [Reducer] godoc，
//     别在这儿写成「四个内置都满足交换律」（曾经这么写，与那边自相矛盾）
//   - 中途出错（channel 未注册 / reducer panic / reducer 里一句 logger.Fatal
//     触发 runtime.Goexit）：**一条都没落地**——不是「回滚掉了」，是压根没发生。
//     归并跑在一份局部的 staged 上，写回 cm 是循环之后一次做完的纯赋值。
//
// reducer 的 panic 转成 error 返回（`%w` 保链，业务 `errors.As` 拿得到原类型）；
// 被 Goexit 掐断时函数不返回、也就没有 error 可言，但 cm 同样没被碰过。
//
// ⚠️ **reducer 里不许回调 cm 的任何方法**：归并跑在写锁内，而 Go 的 RWMutex 不可
// 重入——`Get` / `Snapshot` / 再来一次 `ApplyAll` 都会当场死锁（实测）。`Reducer[T]`
// 的签名不收 ctx 正是为此：它拿不到 hook / EventSink，够不着框架的其余部分。
func (cm *ChannelManager) ApplyAll(writes []ChannelWrite) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.applyAllLocked(writes)
}

// isSingleWriterChannel 返 name 是否声明「一个超步里只该有一个写入者」。
// 未注册的返 false——「没注册」由 applyAllLocked 那条路报，不在这儿抢答。
func (cm *ChannelManager) isSingleWriterChannel(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	ch, ok := cm.channels[name]
	return ok && ch.singleWriterPerStep()
}

// ApplyPendingSendsAndClear 提交**待补记录里那批写**，并在同一个临界区里清掉记录。
// 提交没成时记录原样留着。
//
// **不收参数是刻意的**：要提交的就是 `pendingSendWrites` 里那批，让调用方再传一次
// 等于给「传了个子集」留门——剩下那些会既没落地又被清掉，正是本方法要消灭的形态的
// 镜像，而且同样静默。调用方的职责只剩「提交之前把这批写摆进记录里」。
//
// **为什么合成一个调用**：提交与清记录之间只要还有一行能被掐断的代码，掐进去存档里
// 就同时有「写已落地」和「这批写还欠着」两份 ⟹ Resume 补 apply 一次就多算一次，
// 实测**每 Resume 一次翻一倍**。原先是两句并排、靠一句注释守着「中间不许有业务可达
// 代码」，那是把不变量托付给后来改代码的人记得读注释。
//
// ⚠️ **合成买到的是「局部」，不是「结构性」**：把同一把刀（`runtime.Goexit`）插进本
// 方法内部——`applyAllLocked` 成功与 `cm.pendingSendWrites = nil` 之间——撕裂照样复现，
// 独立审计实测「写落地=5、待补记录还剩 1 条」，Resume 后 `acc=10`、`err` 全程 nil。
// 持锁挡的是并发读者，挡不住自己这条 goroutine 展开。真正让它掐不进来的是**这段里
// 一个可能跑业务代码的调用都没有**（Goexit 只能从被调函数进来），而那一条由
// `TestCommitRegion_CallsNothingBusinessReachable` 守着。同款措辞见两个 commitSuperstep* 提交方法。
//
// 能这么合，前提是提交阶段不可失败（见 applyAllLocked）：清记录必须发生在写真的
// 落地之后，而「落地」不再是一个会中途失败的过程。
func (cm *ChannelManager) ApplyPendingSendsAndClear() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(cm.pendingSendWrites) == 0 {
		return nil
	}
	if err := cm.applyAllLocked(cm.pendingSendWrites); err != nil {
		return err
	}
	cm.pendingSendWrites = nil
	return nil
}

// applyAllLocked 是 ApplyAll 家族的共同实现——**调用方须已持有 cm.mu 写锁**。
func (cm *ChannelManager) applyAllLocked(writes []ChannelWrite) (err error) {
	// ⚠️ **这个 defer 整个跑在写锁里**：外层 `defer cm.mu.Unlock()` 注册在前、LIFO 下
	// 后跑。所以下面这几行不许碰任何会回调进 cm 的东西——`fmt.Errorf` 已经踩着线了
	// （它会调操作数的 `Error()`，那是业务方法）。往这儿加一句 slog / hook / metric
	// 「记录一下 reducer panic」是很自然的下一步，而那会把业务代码拖进锁内，撞上
	// RWMutex 不可重入（实测卡死）。要记就记在锁外——把 err 带出去再说。
	//
	// reducer 是业务代码，panic 归业务、不该炸掉整张图——转成 error 交给 caller。
	// **这道 recover 只负责报信，不负责修状态**：它下面那个循环一个字节都不写回
	// cm，被掐断时没有半应用可言。原先它还兼着「回滚已提交的 channel」，那份职责
	// 随提交点后移一并消失。
	//
	// 栈在这里取（debug.Stack 只碰运行时，不碰业务代码），日志**不在这里记**——按上面那条，
	// 标成「待补记」，由 Run 出口在锁外补记（见 panicerr.WrapPending）。
	defer func() {
		if r := recover(); r != nil {
			err = panicerr.WrapPending("ApplyAll: reducer panic, no writes committed", panicerr.New(r))
		}
	}()

	// 规划阶段：算出每个 channel 归并后的新值，**全部留在 staged 里**。
	// 这一段可以失败（channel 没注册 / reducer panic / reducer 里一句 logger.Fatal
	// 触发 runtime.Goexit），而失败的代价只是「什么都没发生」——cm 还是进来时那样。
	//
	// 同一个 channel 被写多次时按写入顺序链式归并（prev 取 staged 里的中间值），
	// 与逐条 apply 的语义逐字相同。
	staged := make(map[string]any, len(writes))
	for _, w := range writes {
		if w == nil {
			continue
		}
		name := w.channelName()
		ch, ok := cm.channels[name]
		if !ok {
			return w.notRegisteredErr()
		}
		prev, ok := staged[name]
		if !ok {
			prev = cm.values[name] // 没写过时为 nil，reducer 自己认
		}
		staged[name] = ch.reduceAny(prev, w.valueAny())
	}

	// 提交阶段：纯赋值。没有查表、没有业务代码、没有可返回的错——**不可失败也就
	// 不可能半途而废**，这是整个拆分买到的东西。
	//
	// 原先归并和写回挤在一起，于是「reducer 炸在第 3 条」意味着前 2 条已经落进
	// cm 了，得靠一个回滚栈把它们捞回来；而回滚栈只在 recover 分支里跑，
	// runtime.Goexit 那条路 recover() 返 nil 就整个跳过——半应用直接进存档，
	// Resume 重跑整超步让 Append/Sum 双计。现在那个窗口在结构上不存在。
	for name, v := range staged {
		cm.values[name] = v
	}
	return nil
}

// Channels 返回供 NodeFunc 读 channel 的 typed view（不暴露 ChannelManager 自身）。
//
// **隔离原因**：NodeFunc 只应能读，不应能在节点内部直接 mutate ChannelManager
// （那会破坏"节点输出 → ChannelWrite → 超步合并"的协议）。Channels 是只读视图。
func (cm *ChannelManager) Channels() Channels {
	return Channels{cm: cm}
}

// updateFrontier 是 Run 内部 helper：更新当前帧 frontier 到 cm，便于中断时
// Snapshot 自动持久化。不导出。
func (cm *ChannelManager) updateFrontier(frontier []FrontierMember) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.updateFrontierLocked(frontier)
}

// updateFrontierLocked 是 updateFrontier 的实现——**调用方须已持写锁**。
func (cm *ChannelManager) updateFrontierLocked(frontier []FrontierMember) {
	if len(frontier) == 0 {
		cm.frontierAtExit = nil
		return
	}
	cm.frontierAtExit = append(cm.frontierAtExit[:0], frontier...)
}

// commitSuperstepPartial 是**部分失败**路径的提交：写入落地、frontier 窄化到失败成员、
// 代际推进、答案兑付、成功兄弟记账、成功兄弟的上行信号记在一边（等整超步完成时再并入），
// **六样一起生效**。
//
// # 为什么必须是一个方法
//
// 这四样原先是四条并排的语句。中间只要插进一行能被掐断的代码（一次 hook、一个
// event、任何业务回调），被掐进去就会留下这样一份存档：写入**已经落地**，代际
// **没有推进**，而产出这批写入的节点**还在 frontier 里**——于是 Resume 重跑它，
// 它的写入再算一次。实测：一个 Sum channel 从 5 变成 10，`err` 全程 nil。
//
// 今天那四条之间确实没有业务可达代码，所以掐不进去（`TestCommitRegion_
// CallsNothingBusinessReachable` 守着这一条）。但那是「碰巧没人插」而不是「插不进去」
// ——把不变量托付给后来改代码的人记得读注释，本仓已经栽过一次。
//
// # 覆盖范围（两个方法各盖住自己那条路的全部）
//
// 曾经只有一个 `commitSuperstep` 盖住「四样」，而它外面还留着四个窗口——独立审计逐点
// 注入全部复现过。现在按路径拆成两个方法，每个把自己那条路上的字段改动**全部**收进
// 一个临界区：
//
//   - [ChannelManager.commitSuperstepPartial]：写入落地 / frontier 窄化 / 代际推进 /
//     答案兑付 / **成功兄弟记账**（出边、sends、上行信号）。最后那样原先在方法外面，掐进去成功兄弟的出边与
//     sends 整批丢失——`err` 全程 nil、channel 值看着还对，只是那个后继节点一次都
//     没跑（**比双计隐蔽**：双计至少改了个看得见的数）。注入实测：旧结构下存档里
//     `completed=[] deferredSends=0`，后继节点少跑一次；现在两样都在。
//   - [ChannelManager.commitSuperstepAdvance]：写入落地 / 代际推进 / 答案兑付 /
//     上行信号并入 / **待办清空** / **frontier 前移** / **SendFrom payload 折叠**。后三样原先散在
//     提交之后的 150 行里，那正是最长的那个双计窗口。注入实测：旧结构下存档是
//     「写入已落地 + frontier 还指着刚跑完的节点」，续跑重算一次（Sum 从 5 变 10、
//     `err` 全程 nil）；现在存档里 frontier 已前移，续跑不重跑。
//
// 能这么合，前提是「下一个 frontier 是什么、payload 是哪些」全是**纯计算**——出边由
// 各成员在自己的执行里算完（算不出来的成员根本不算成功），assembleFrontier 只做去重
// 与判 End。所以它们能提到提交之前算好再传进来。
//
// ⚠️ **`dirWrites` 是第二次 apply，不并进第一次**：并进去的话 payload 里一个 reducer
// 失败会把成功成员的写入一起回滚，那些成员重跑时 fan-out 实例身份是新铸的 ⟹ 已兑付
// 的答案绑在旧身份上，变成永远等不到的挂起。
//
// ⚠️ **仍然不是「`Snapshot` 读的全部」**：本方法之外还有别的字段（挂起清单由
// `recordInterruptBatch` 在提交之前单独记，那是有意的——中断得让调用方看见，哪怕本批
// 提交失败）。真要把原子单位提到「快照全部」是状态模型的改动，另立项。
//
// # 提交顺序是承重的
//
// 代际（visit）必须在写入提交**之后**推进：早了的话 reducer 失败整超步回滚重跑，
// 而代际已经变了 ⟹ 重跑时中断身份漂移，用户答过的问题被重新问。答案兑付跟代际
// 同一时刻——判据一样：这一步成了才算数。
//
// 只有空身份成员推进代际（ISSUE-96）：visitCount 是 per-NodeID 共享的，它唯一的
// 用途是给静态边 / cond 边节点（恒单实例）表达「跨完成访问的代际」；fan-out 实例
// 自己有 InstanceID 表达身份，算 InterruptID 时本就忽略 visit。
//
// ⚠️ **narrowTo 必须非空**。这条路只在 `res.FirstErr != nil` 时走，而
// `executeFrontier` 两条路径都保证「置 FirstErr 必同时 append FailedBranches」，
// 所以调用点给的恒非空。传空会**清空** frontier（`updateFrontierLocked` 对 len==0
// 的语义是清空不是保持）——那时整超步的进度既没窄化也没保留，Resume 从空 frontier
// 起跑。旧版这里有一句 `if narrowTo != nil` 守着，拆成两个方法后它没有意义了
// （全员成功那条路走的是另一个方法），但那句 godoc 留了下来、教的语义正好相反。
func (cm *ChannelManager) commitSuperstepPartial(
	writes []ChannelWrite,
	narrowTo []FrontierMember,
	succeeded []FrontierMember,
	consumedAnswers []string,
	signals []SignalRecord,
	deferredSends []SendDirective,
	deferredTargets []NodeID,
) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(writes) > 0 {
		if err := cm.applyAllLocked(writes); err != nil {
			return err
		}
	}
	cm.updateFrontierLocked(narrowTo)
	cm.advanceVisitsLocked(succeeded)
	cm.markAnswersConsumedLocked(consumedAnswers)
	// 成功兄弟做出的上行信号先记在一边：signals 是调用方已经跟此前记下的合并过的整份（同一超步、
	// 并发），所以整体替换。整超步完成时才并入（见 completedSignals 字段说明）。
	cm.completedSignals = append([]SignalRecord(nil), signals...)
	// 成功兄弟的身份 + 出边 + sends 记账——**跟写入提交同一个临界区**。
	// 这里原先在方法外面，两者之间被掐会让成功兄弟的出边与 sends 整批丢失，
	// 那个后继节点一次都没跑而 err 全程 nil（独立审计注入实测）。
	cm.appendCompletedSiblingsLocked(succeeded, deferredSends, deferredTargets)
	return nil
}

// commitSuperstepAdvance 是**全员成功**路径的提交：写入落地、代际推进、答案兑付、上行信号并入、
// 待办记录清空、frontier 前移、SendFrom payload 折叠，**七样在同一个临界区里**。signals 是调用方
// 已经跟部分失败时记在一边的合并过的整份。
//
// nextFrontier 为 nil 表示本超步之后无待执行成员（isEnd，或出边为空的异常图）——
// 两者都是清空 frontier，区分交给调用方（前者返回成功，后者由下一轮的空 frontier 检查报错）。
//
// # dirWrites 为什么是**第二次** applyAllLocked 而不是并进第一次
//
// 并进去的话，SendFrom payload 里一个 reducer 失败会把**成功成员的写入一起回滚**。
// 那些成员随后重跑，而重跑时 fan-out 实例身份是新铸的 ⟹ 已经兑付的答案绑在旧身份上，
// 变成永远等不到的挂起。分成两次 apply 时第一次已经落地、不受第二次失败影响。
//
// 第二次失败时**保留** frontier=nextFrontier 与 pendingSendWrites=dirWrites（两者都在
// 本次调用里已经摆好）——那正是 Resume 补 apply 所需的现场。
func (cm *ChannelManager) commitSuperstepAdvance(
	writes []ChannelWrite,
	succeeded []FrontierMember,
	consumedAnswers []string,
	signals []SignalRecord,
	nextFrontier []FrontierMember,
	dirWrites []ChannelWrite,
) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(writes) > 0 {
		if err := cm.applyAllLocked(writes); err != nil {
			return err
		}
	}
	cm.advanceVisitsLocked(succeeded)
	cm.markAnswersConsumedLocked(consumedAnswers)
	cm.commitSignalsLocked(signals)
	// 待办记录用完即清：它的内容（targets 与 sends）已分别转移进 nextFrontier 与
	// dirWrites、信号已随上一行并入。留着会让同一批 payload 在存档里存两份，Resume 双放。
	cm.completedFrontier, cm.deferredSends, cm.completedTargets = nil, nil, nil
	cm.completedSignals = nil
	cm.updateFrontierLocked(nextFrontier)
	if len(dirWrites) > 0 {
		cm.pendingSendWrites = append([]ChannelWrite(nil), dirWrites...)
		if err := cm.applyAllLocked(dirWrites); err != nil {
			// 保留 frontier + pendingSendWrites 作 Resume 现场。**标记是哪一步失败的**
			// ——调用方对两种失败的处置完全相反（前一次失败要整超步重跑，这一次不能，
			// 成功成员的写入已经落地、frontier 已前移，重跑就是双计）。由知情的这一处
			// 产出信号，别让调用方从 cm 的状态反推（那是第二份会漂的判据）。
			return &sendPayloadApplyErr{err: err}
		}
		cm.pendingSendWrites = nil
	}
	return nil
}

// sendPayloadApplyErr 标记失败发生在 SendFrom payload 那次 apply 上，而不是成功成员
// 写入那次。见 commitSuperstepAdvance 的说明。
type sendPayloadApplyErr struct{ err error }

func (e *sendPayloadApplyErr) Error() string { return e.err.Error() }
func (e *sendPayloadApplyErr) Unwrap() error { return e.err }

// advanceVisitsLocked 推进代际——**调用方须已持写锁**。
//
// 只有空身份成员推进（ISSUE-96）：visitCount 是 per-NodeID 共享的，它唯一的用途是
// 给静态边 / cond 边节点（恒单实例）表达「跨完成访问的代际」；fan-out 实例自己有
// InstanceID 表达身份，算 InterruptID 时本就忽略 visit。
func (cm *ChannelManager) advanceVisitsLocked(succeeded []FrontierMember) {
	for _, m := range succeeded {
		if m.InstanceID == "" {
			cm.visitCount[m.NodeID]++
		}
	}
}

// appendCompletedSiblingsLocked 记录部分失败超步的成功成员 + 其 SendDirectives（PA-H1）。
// **调用方须已持写锁**——它在 [ChannelManager.commitSuperstepPartial] 的临界区里跑。
// append 语义——Resume 重跑后再次部分失败时累积（更早的成功成员仍要保留）。
//
// **node 阶段失败专用。别给它加"按重跑成员过滤"** —— 那会丢数据：
// SucceededBranches / FailedBranches 记的是 **NodeID**，但执行单位是 **branch 实例**
// （SendFrom 同 Target 投 N 次 → frontier 里同 NodeID N 份，各有自己的 InstanceID）。实例 a
// 成功、实例 b 失败时，同一个 NodeID **同时**出现在两个 list 里——按 NodeID 过滤会把
// 实例 a 的记录连同它的 sends 一起丢掉，而 a 已成功、永远不会重跑去重产 → 后继静默
// 丢失（PA-H1 原 bug 复活）。不过滤才对：a 的记录贡献出边、b 由 frontier 重跑，各归各。
//
// # 这里记下的成员**永不重跑**（所以不需要任何 purge）
//
// 本函数只在「有成员失败」这条路上被调用，记的是 `res.SucceededBranches`——而一个成员
// 算成功的条件已经包含「它的出边算得出来」。于是：
//
//   - 出边算不出来的成员进的是 FailedBranches，它的 writes / sends / targets 一并丢弃，
//     根本不会出现在这里；
//   - 唯一会把已成功成员放回去重跑的路径（`ApplyAll` reducer panic 整超步重跑）在调用
//     本函数**之前**就 return 了。
//
// 曾经有一套「重跑成员的旧记录要按实例精确 purge」的机件（mergeCompletedSiblings），
// 它存在的前提是「成功成员可能被拉回来重跑」——那个前提随「出边归成员」一起消失了，
// 机件已删。哨兵见 multi_instance_test.go 的 TestMultiInstance_CondFailedInstanceOnlyRerun。
func (cm *ChannelManager) appendCompletedSiblingsLocked(members []FrontierMember, sends []SendDirective, targets []NodeID) {
	if len(members) == 0 && len(sends) == 0 && len(targets) == 0 {
		return
	}
	cm.completedFrontier = append(cm.completedFrontier, members...)
	cm.deferredSends = append(cm.deferredSends, sends...)
	cm.completedTargets = append(cm.completedTargets, targets...)
}

// peekCompletedSiblings 只读返回部分失败超步的成功成员记录（PA-H1）。
//
// **audit² H1 fix（peek-then-clear，非破坏性 take）**：take 在出边计算之前清空，
// 但 take 与"出边计算 + sends 重放完成"之间存在失败点（cond 业务错 / cond
// InterruptError / unregistered target）——任一触发时 taken 记录已丢，SaveNS
// 持久化空字段 → 下次 Resume 成功兄弟的后继再次静默丢失（PA-H1 原 bug 复活）。
// 改为 peek（失败时 cm 字段原样保留进 cp）+ 出边阶段全部成功后 clear。
// Run 内部 helper，不导出。
func (cm *ChannelManager) peekCompletedSiblings() ([]FrontierMember, []SendDirective, []NodeID) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if len(cm.completedFrontier) == 0 && len(cm.deferredSends) == 0 && len(cm.completedTargets) == 0 {
		return nil, nil, nil
	}
	ids := append([]FrontierMember(nil), cm.completedFrontier...)
	sends := append([]SendDirective(nil), cm.deferredSends...)
	targets := append([]NodeID(nil), cm.completedTargets...)
	return ids, sends, targets
}

// setPendingSendWrites 存档"这批写没能提交、待 Resume 补 apply"的 SendDirective
//
// ⚠️ **生产路径已不调它**——`commitSuperstepAdvance` 在自己的临界区里直接赋值（那一步
// 必须与 payload 的 apply 在同一个锁内）。留给测试观察，同 `peekPendingSendWrites`。
// payload（F-B）。Run 内部 helper，不导出。
func (cm *ChannelManager) setPendingSendWrites(writes []ChannelWrite) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.pendingSendWrites = append([]ChannelWrite(nil), writes...)
}

// peekPendingSendWrites **只读**待补 apply 的 payload（F-B），不清。
//
// **生产路径已不调它**——补 apply 与清记录收进了 [ChannelManager.ApplyPendingSendsAndClear]
// 的同一个临界区。它留着是给测试观察账本用的（「提交成了记录该空、没成该原样留着」
// 那两条哨兵）。
//
// ⚠️ **只读、不清**这条设计仍然承重：它曾是 take（取出即清），而「apply 失败就放
// 回去」那步挂在 `err != nil` 上——`runtime.Goexit` 掐在归并里时那步跑不到，payload
// 从 cm 消失又没 apply 成 ⟹ 存档 `pendingSendWrites=0` ⟹ Resume 时 target 静默读
// 零值。「成了才清」把这条路堵死。
//
// **必须清空**：payload 补 apply 后就进了 cm.values，下次 Snapshot 会烤进
// cp.Channels；不清则二次 Resume 对 Append / Sum 等累积型 reducer 双计。
// Run 内部 helper，不导出。
func (cm *ChannelManager) peekPendingSendWrites() []ChannelWrite {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if len(cm.pendingSendWrites) == 0 {
		return nil
	}
	return append([]ChannelWrite(nil), cm.pendingSendWrites...)
}

// recordInterruptBatch 把**一个超步**产生的全部挂起一次性记进 cm。
//
// # 为什么是「整批」不是「一条一条」
//
// 校验单位必须等于落档单位。原先是「一条一条记、撞 ID 就 first-write-wins 跳过」——
// 而落档单位是「一个超步的全部挂起」，于是同一个伤害有两种待遇：塞进一个
// `InterruptError` 里被 `validateInterrupt` 当场拒，分在两个分支就只有一行 warn。
//
// 实测那行 warn 的后果：两个分支各问一件事、用了同一个 ID ⟹ 调用方看到 2 条、存档里
// 只有 1 条 ⟹ 用户答了看得见的那条，**另一件事也执行了**，整个 Run 成功收尾（err==nil）。
//
// # 为什么撞了只能拒，不能「都留下」
//
// 调用方回答用的就是这个 ID（`WithResumeUserInputs(map[InterruptID]*Message)`）——
// 两条同址挂起在语义上无法分别回答，留着第二条也没有地址能投递给它。
func (cm *ChannelManager) recordInterruptBatch(qs []*AskQuery) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	seen := make(map[string]*AskQuery, len(qs))
	for _, q := range qs {
		if q == nil {
			continue
		}
		if prev, dup := seen[q.InterruptID]; dup {
			return fmt.Errorf("%w: 同一超步里两条挂起用了相同的 InterruptID %q（%q 与 %q）"+
				"——调用方只有这一个地址可用来回答，第二条无从投递。"+
				"每条提问给一个自己的 ID，或都走 compose_v2.Ask（它按执行位置自动区分）",
				ErrRunAborted, q.InterruptID, prev.Question, q.Question)
		}
		seen[q.InterruptID] = q
	}
	cm.pendingInterrupts = append(cm.pendingInterrupts, qs...)
	return nil
}

// putNodeState 存一条节点私有中断态（`SaveNodeState` 的落点）。
//
// **同键覆盖**：节点做完一件就存一次，后一次是前一次的超集（业务传的是累积态），
// 覆盖即最新。
func (cm *ChannelManager) putNodeState(key string, tv taggedValue) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.nodeStates == nil {
		cm.nodeStates = make(map[string]taggedValue, 1)
	}
	cm.nodeStates[key] = tv
}

// getNodeState 取一条节点私有中断态（`GetNodeState` 的读点）。
func (cm *ChannelManager) getNodeState(key string) (taggedValue, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	tv, ok := cm.nodeStates[key]
	return tv, ok
}

// PendingInterrupts 返回当前挂起的 AskQuery 列表（Run 中断后供 caller 读）。
//
// **audit Round 9 Bug B3 文档化**：Resume 启动后框架内部短暂 clearInterrupts
// （graph_loop.go 重新执行节点时清旧 list，由节点重抛 InterruptError 重填），
// 此期间查询可能短暂返 nil；caller 应在 Run 入口前 / 完成后查询，避免 race
// window。单进程 Resume 自然完成后 cm 状态正确，无副作用。
//
// **audit Round 9 Bug C4 修复**：返回深拷贝（不只是 slice copy）让 caller mutate
// 不污染 cm 内部 state。之前浅拷贝（仅 copy slice header）让业务 mutate
// `query.Question` / `query.Options[i]` 直接污染 cm.pendingInterrupts 元素——
// 与 cm.Restore 的深拷贝不对称（serialization.go::Restore 已修过深拷贝）。
//
// 业务 / UI 渲染 / 翻译 / 格式化文本场景：
//
//	pending := cm.PendingInterrupts()
//	for _, q := range pending {
//	    q.Question = i18n.Translate(q.Question)  // 安全 mutate 副本
//	}
func (cm *ChannelManager) PendingInterrupts() []*AskQuery {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if len(cm.pendingInterrupts) == 0 {
		return nil
	}
	out := make([]*AskQuery, 0, len(cm.pendingInterrupts))
	for _, q := range cm.pendingInterrupts {
		if q == nil {
			continue
		}
		// 深拷贝 *AskQuery + Options slice（map 引用类型也需深拷贝避免污染）
		qc := *q
		if q.Options != nil {
			qc.Options = append([]string(nil), q.Options...)
		}
		out = append(out, &qc)
	}
	return out
}

// clearInterrupts 清空挂起列表——Resume 路径**无条件**调用 (graph_loop.go:570).
//
// **audit #5 视角 9 F12 godoc 修正**: 之前注释 "caller 通过 WithResumeAnswers
// 传了答案才清" 不准——code 实际不检查 answers 状态, 任何 Resume 路径 (含纯
// retry / sub-agent 透传 ResumeContext) 都清空旧 list. 节点重跑时若拿不到答案
// 会重新 throw InterruptError 让 cm.pendingInterrupts 重新填充 (deterministic
// re-emit 同 ID).
//
// **race window** (graph_loop.go:568-571): 清空 → 节点重跑之间, 外部观察者
// 调 cm.PendingInterrupts() 会看到空 list. 业务 caller 应在 Run 入口前 / 完成
// 后查询, 不在 Resume 中途 race query (cm.PendingInterrupts godoc line 313-316
// 已说明).
func (cm *ChannelManager) clearInterrupts() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.pendingInterrupts = nil
}

// ── 类型擦除诊断 ──

func typeMismatchError(channelName string, expectedZero any, got any) error {
	return fmt.Errorf("channel %q type mismatch: expected %T, got %T (value=%v)",
		channelName, expectedZero, got, got)
}

func typesEqual(a, b reflect.Type) bool {
	// reflect.Type 是接口，`a == b` 对 nil 也正确（both nil→true / one nil→false）——
	// register 唯一调用点传的 typeRep() 恒非 nil（reflect.TypeOf((*T)(nil)).Elem()），
	// 原 nil 守卫是不可达死代码，删。
	return a == b
}
