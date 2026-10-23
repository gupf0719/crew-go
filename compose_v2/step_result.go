package compose_v2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// 步骤记录的派生键：`<归属前缀>:step:<key 的哈希>`，可重做的那种是
// `<归属前缀>:redo<代际>:<key 的哈希>`（交出去的一段执行里记的是 `:hredo`，见 handedRedoKeySep）。
// 归属前缀是节点执行键（[nodeStateKey]，16 位十六进制），在分域里记的再加分域哈希（tool_call、
// 钩子相位与条件边、[EnterPhase] / [HandOver] 交出去的执行，见 nodeStateAccessor.base）。存档投影
// 按第一个冒号之前那段认归属、把一次节点执行名下的所有条目一起导出（见 serialization.go 的
// NodeStates 投影）。
const (
	stepKeySep = ":step:"
	redoKeySep = ":redo"
	// handedRedoKeySep 标记交出去的一段执行（[HandOver] 封的那一格与它下面的分域）里的可重做记录：
	// 那里的代际从交接那一层数起、不算执行级（见 nodeStateAccessor.handedOver），存档投影不能拿执行级
	// 代际判它过没过期——单独一个标记让投影认得出来。
	handedRedoKeySep = ":hredo"
)

// stepStateKey 是 key 这一步在 base 名下的派生键。
//
// 业务给的 key 不直接进存档，进的是它的哈希：派生键是存档里 NodeStates 的 map 键，要经得起
// 每个后端的 JSON 往返。原样放进去的话，key 里的 NUL 让 PostgreSQL 的 JSONB 拒掉**整份**
// 存档（实测：这一格曾用 NUL 作分隔符，带步骤记录的存档一份都写不进 PG），非法 UTF-8 往返后
// 变成 U+FFFD、续跑时读不回。哈希之后什么字符的 key 都行。
func stepStateKey(base, key string) string {
	return base + stepKeySep + shortHash(key)
}

// redoStepStateKey 是可重做的那种记录（[Rerunnable]）在第 gen 代的派生键。
func redoStepStateKey(base string, gen int, key string) string {
	return base + redoKeySep + strconv.Itoa(gen) + ":" + shortHash(key)
}

// handedRedoStepStateKey 同 redoStepStateKey，给交出去的那一格用（见 handedRedoKeySep）。
func handedRedoStepStateKey(base string, gen int, key string) string {
	return base + handedRedoKeySep + strconv.Itoa(gen) + ":" + shortHash(key)
}

// redoKey 是 a 所在这一格里 key 这一步的可重做记录在当前代际的派生键。
func (a *nodeStateAccessor) redoKey(key string) string {
	gen := a.cm.effectiveRerunGen(a)
	if a.handedOver {
		return handedRedoStepStateKey(a.base(), gen, key)
	}
	return redoStepStateKey(a.base(), gen, key)
}

// redoGenOf 从派生键里取出可重做记录的代际；不是可重做记录时 ok=false。
func redoGenOf(k string) (gen int, ok bool) {
	_, gen, _, ok = redoSlotOf(k)
	return gen, ok
}

// redoSlotOf 拆开可重做记录的派生键：slot 是去掉代际之后的部分（同一个分域里的同一步），gen 是
// 代际，handed 报它是不是交出去的那一格里记的（见 handedRedoKeySep）。不是可重做记录时 ok=false。
// 前面那几段（节点执行键、分域哈希）都是十六进制，不会含 ":redo" / ":hredo"；两个标记互不包含。
func redoSlotOf(k string) (slot string, gen int, handed, ok bool) {
	sep := redoKeySep
	i := strings.LastIndex(k, sep)
	if i < 0 {
		sep, handed = handedRedoKeySep, true
		i = strings.LastIndex(k, sep)
	}
	if i < 0 {
		return "", 0, false, false
	}
	rest := k[i+len(sep):]
	j := strings.IndexByte(rest, ':')
	if j < 0 {
		return "", 0, false, false
	}
	g, err := strconv.Atoi(rest[:j])
	if err != nil {
		return "", 0, false, false
	}
	return k[:i+len(sep)] + rest[j:], g, handed, true
}

// rerunGeneration 是一次节点执行（或它里面一个分域）的「重做代际」：它的产出被条件边按
// [CondFailRerunFrom] 否决、要求重新产出时加一，此前按 [Rerunnable] 记下的步骤随之不可达。
//
// **按分域记**（键 `<节点执行键>:rerun` 是执行级，`<节点执行键>:rerun:<分域哈希>` 是某个分域的）：
//
//   - 节点自己的条件出边否决 ⟹ 执行级加一：整次执行重做，里面每个分域都跟着换代；
//   - 嵌套图的入口条件边否决 ⟹ 只在**产出那份输入的分域**加一（节点体就是执行级；某个
//     tool_call、某个钩子相位、交出去的一段执行各是一个分域；经 [HandOver] 交给被调方的主图，
//     输入是调用方那个分域产的），而且**等承载它的成员以这个错失败时**才加（见
//     [nestedEntryRejected]）：否决当场框架不知道外层会不会因它重做——错可能被兜底吞掉、被重试
//     消化、被转成工具的失败结果。当场推进整次执行的话，同一批里兄弟 tool_call 挂着的提问换了
//     ID 重问、它们里面挂起的可重做调用换槽重来（实测），而那些兄弟根本不会重做。
//
// 一个分域的有效代际 = 执行级 + 从外到里每一层分域的（[ChannelManager.effectiveRerunGen]）：
// 每一项只增不减，和也就只增不减，同一个分域里任何两个时刻只要中间推进过就不相等。交出去的一段
// 执行（[HandOver] 封的那一格与它下面的分域）例外：只算交接那一层往下的，不算调用方那一层（执行级
// 与交接之前的分域）——调用方重做时这次调用是不是同一次，由调用的身份决定（见 HandOver）。
//
// 存在同一张节点私有态表里，跟着这次节点执行一起进存档、一起不可达，不另设载体。
type rerunGeneration int

func init() {
	RegisterValueType[rerunGeneration]("compose_v2.rerunGeneration")
}

// rerunGenKey 是 nodeKey 这次执行里 scope 这个分域的代际键（scope 为空 = 执行级）。
func rerunGenKey(nodeKey, scope string) string {
	if scope == "" {
		return nodeKey + rerunGenSuffix
	}
	return nodeKey + rerunGenSuffix + ":" + shortHash(scope)
}

// rerunGenHeld 读一格代际（没有记录时为 0），调用方已持 cm.mu（读锁或写锁都行——所以不叫
// `…Locked`：本包那个后缀专指「调用方持写锁」，有机械哨兵按它查）。存档投影在 Snapshot 的
// 读锁里调它——再调一个自己上读锁的方法就是递归读锁，Go 的读锁不可重入，有写者排队时会死锁。
func (cm *ChannelManager) rerunGenHeld(nodeKey, scope string) int {
	tv, ok := cm.nodeStates[rerunGenKey(nodeKey, scope)]
	if !ok {
		return 0
	}
	v, err := unmarshalValue(tv)
	if err != nil {
		return 0
	}
	g, _ := v.(rerunGeneration)
	return int(g)
}

// effectiveRerunGen 是访问器 a 所在分域的有效代际：执行级加上链上每一层分域的；a 在一段交接出去的
// 执行里时只加交接那一层往下的（见 nodeStateAccessor.handedOver）。
func (cm *ChannelManager) effectiveRerunGen(a *nodeStateAccessor) int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if a.handedOver {
		g := 0
		for _, s := range a.chain[a.genFrom:] {
			g += cm.rerunGenHeld(a.key, s)
		}
		return g
	}
	g := cm.rerunGenHeld(a.key, "")
	for _, s := range a.chain {
		g += cm.rerunGenHeld(a.key, s)
	}
	return g
}

// bumpRerunGen 把 nodeKey 这次执行里 scope 这个分域的代际加一（读改写在同一把写锁里；scope 为空
// = 执行级）。
//
// 引擎在条件边按 [CondFailRerunFrom] 否决时调（[condRejected]）：成员自己的出边否决时推进
// 成员这次执行的执行级，嵌套图的入口条件边否决时推进产出那份输入的分域（等成员以这个错失败时，
// 见 [applyNestedEntryRejections]）。节点自己判定一格的产出作废时经 [AdvanceRerunGeneration] 调。
func (cm *ChannelManager) bumpRerunGen(nodeKey, scope string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	tv, err := marshalValue(rerunGeneration(cm.rerunGenHeld(nodeKey, scope) + 1))
	if err != nil {
		// 类型在本包 init 里注册，走不到这里
		panic(fmt.Sprintf("compose_v2: bumpRerunGen: %v", err))
	}
	if cm.nodeStates == nil {
		cm.nodeStates = make(map[string]taggedValue, 1)
	}
	cm.nodeStates[rerunGenKey(nodeKey, scope)] = tv
}

// RerunGeneration 返回 ctx 所在那次节点执行（所在分域）的重做代际：它的产出被条件边按
// [CondFailRerunFrom] 否决过几次（不在节点执行里时为 0）。按分域算：同一次执行里，兄弟
// tool_call 里嵌套图的入口被否决、只让那个 tool_call 重做时，别的 tool_call 的代际不变。在交接出去
// 的一段执行里（[HandOver]：嵌套调用的被调方在自己的图之外）从交接那一层数起，调用方那一层的
// 重做不算——那时这次调用是不是同一次调用由调用的身份决定。
//
// 按 [Rerunnable] 记的步骤引擎已经按它分代、提问的身份也已经带上它（见 [InterruptSite]），
// 业务不用管。这里给的是要把它拼进别的身份的调用方：`agent_v2.RunSubAgent` 带
// `agent_v2.WithRerunnable` 的调用把它拼进调用的身份——被否决后重做的那一次换一个存档槽，
// 被调方在它自己的图之外记下的东西、它问的问题也都换成新的。
func RerunGeneration(ctx context.Context) int {
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return 0
	}
	return a.cm.effectiveRerunGen(a)
}

// AdvanceRerunGeneration 宣布 ctx 所在这一格（分域）的产出作废、要重新产出：推进这一格的重做代际，效果同条件边按
// [CondFailRerunFrom] 否决这一格的产出——这一格里按 [Rerunnable] 记的步骤从此不可达，这一格里的提问换新 ID（提问的
// 身份带着代际），普通记录照常拿回（同一个地方的扣费、问过人的把关不跟着重做）。在相位、tool_call 这类分域里调只推进
// 那一格；在节点体里调推进整次执行。
//
// 给「节点自己判定这一格处理过的产出作废」用，而且只在这次执行随后以失败退出时调：代际随这次执行的私有态一起落盘
// （失败出口的存档带着它），执行成功之后连同它一起不可达。典型是 BasicAgent 的 model 节点：OnModelEnd 钩子处理过
// （可能问过人）的那份答复被作废、续跑重新调模型时推进 OnModelEnd 那一格——不推进的话，调用方累积传的、对旧答复
// 的批准按同一个 ID 兑付给用户从没看过的新一份。
//
// 不在节点执行里时是 no-op。
func AdvanceRerunGeneration(ctx context.Context) {
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return
	}
	a.cm.bumpRerunGen(a.key, a.scope)
}

// outerRerunGensKey 携带「承载这张图的每一层外层执行此刻的重做代际」，从外到里（见
// [InterruptSite] 的 OuterRerunGens）。执行状态：进每张图时重铺，DetachExecution 剥掉。
type outerRerunGensKey struct{}

func withOuterRerunGens(ctx context.Context, gens []int) context.Context {
	return context.WithValue(ctx, outerRerunGensKey{}, gens)
}

// outerRerunGensFor 是一张图的外层代际链：承载这张图的那次执行（host 所在分域）所在那张图的链，
// 再加 host 此刻的有效代际。host 为 nil（顶层图）时是空链。
//
// 上一层的链按 host 所在的 cm 取（ChannelManager.outerGens），不按 ctx 取：两者在节点体里相同，在
// 图级钩子相位里不同（见 outerGens 的说明）。
//
// 在进图那一刻取：代际只在成员失败 / 出边否决之后推进（见 rerunGeneration），或由节点在以失败退出之前
// 自己推进（[AdvanceRerunGeneration]）——承载这张图的执行正在跑、它的代际在这期间不变。
func outerRerunGensFor(host *nodeStateAccessor) []int {
	if host == nil {
		return nil
	}
	parent := host.cm.outerGens
	return append(append(make([]int, 0, len(parent)+1), parent...), host.cm.effectiveRerunGen(host))
}

// outerRegensKey 携带外层「重生次数」链，与 outerRerunGensKey 等长（见 ChannelManager.outerRegens）。
type outerRegensKey struct{}

func withOuterRegens(ctx context.Context, regens []int) context.Context {
	return context.WithValue(ctx, outerRegensKey{}, regens)
}

// outerRegensFor 是一张图的重生次数链：承载它的那次执行所在那张图的链，再加这张图在那次执行里重生过的次数。
func outerRegensFor(host *nodeStateAccessor, regen int) []int {
	if host == nil {
		return nil
	}
	parent := host.cm.outerRegens
	return append(append(make([]int, 0, len(parent)+1), parent...), regen)
}

// nestedRegenKey 是外层节点执行名下记「这张嵌套图跑完过几次」的步骤记录键（graphPhaseNS：ns 加同一次执行里的第几遍）。
//
// 节点里直接跑的嵌套图跑完之后节点又失败或挂起（不是条件边否决）：续跑时节点重跑，嵌套图的存档在它成功时已清，于是它从头
// 再跑——是一次**重生**，里面的提问是新内容上的新问题。只按位置算 ID 的话它们落在上一次的 ID 上，调用方累积传的旧答案
// 被直接兑付给新内容（实测：只问过「批准 v1」，执行的是「v2 已批准」）。所以嵌套图每跑完一次就在外层执行名下记一笔，
// 下一次从头跑时按记下的次数换身份；续跑它自己存档的那一遍次数没变、ID 不变。记录随外层节点「没成」的出口存档落盘，
// 外层节点成功之后不可达（同节点私有态）。
//
// 计数存成私有类型 nestedCompletions（compose_v2 自己注册）：存成裸 int 的话只有业务或别的包碰巧注册过 int 时才存得进
// 节点私有态，只用 compose_v2 的业务上每次都存不进去、修复静默失效（审计实测，包内测试文件注册了 int 所以测不出来）。
func nestedRegenKey(graphPhaseNS string) string {
	return "\x00graph-completions\x00" + graphPhaseNS
}

// nestedCompletions 是一张嵌套图在承载它的那次外层执行里跑完过的次数（见 nestedRegenKey）。
type nestedCompletions int

func init() {
	RegisterValueType[nestedCompletions]("compose_v2.nestedCompletions")
}

// outerRerunGensOf 是 ctx 上的外层代际链（ctx 所在那张图的）。
func outerRerunGensOf(ctx context.Context) []int {
	gens, _ := ctx.Value(outerRerunGensKey{}).([]int)
	return gens
}

// outerRerunGensID 把 ctx 上的外层代际链编成 InterruptSite 的那一维：全为 0 时是空串（从没被
// 否决过、从没重生过的外层不改变 ID），否则从外到里用点号连起来（元组，不求和：内层图重跑时自己的代际从 0
// 数起，求和会让「外层没重做、内层重做过一次」与「外层重做过一次、内层没有」撞成同一个值）。每一层的重生次数
// （见 nestedRegenKey）不为 0 时接在那一层后面，写成「代际~次数」。
func outerRerunGensID(ctx context.Context) string {
	gens := outerRerunGensOf(ctx)
	regens, _ := ctx.Value(outerRegensKey{}).([]int)
	zero := true
	for i, g := range gens {
		if g != 0 || (i < len(regens) && regens[i] != 0) {
			zero = false
			break
		}
	}
	if zero {
		return ""
	}
	parts := make([]string, len(gens))
	for i, g := range gens {
		parts[i] = strconv.Itoa(g)
		if i < len(regens) && regens[i] != 0 {
			parts[i] += "~" + strconv.Itoa(regens[i])
		}
	}
	return strings.Join(parts, ".")
}

// StepOption 是 [SaveStepResult] 的选项。
type StepOption func(*stepOptions)

type stepOptions struct {
	rerunnable bool
}

// Rerunnable 声明这条记录是「这一步的**产出**」而不是「这一步做过的**事**」：节点被要求重新
// 产出时它作废，重跑时这一步重新做。
//
// 「被要求重新产出」指这个节点的条件出边按 [CondFailRerunFrom] 否决——那一档的语义就是
// 「上游产出的东西不行，重跑它换一份」；或者这个节点里跑的嵌套图（含经 `agent_v2.RunSubAgent` 调起
// 的 agent 的主图）的入口条件边按它否决、而且节点因此失败了，那时作废的只是产出那张图输入的
// 分域（某个 tool_call、某个钩子相位；节点体就是整次执行）里记的（见 rerunGeneration）。不作废
// 的话，重跑时这一步原样拿回上一份产出，出边照样报错，续跑多少次都一样。
//
// 什么时候用：记的是调模型、调子 agent 之类**产出**会被下游判定好坏的一步。记的是扣款、发消息
// 之类**副作用**时别用——那种记录在重跑时正该拦住重放。两类都有时分成两条记。
// `agent_v2.RunSubAgent` 的完成记录默认是普通记录，调用方用 `agent_v2.WithRerunnable` 点名。
//
// 只有**否决**会作废：条件边被外面停下（软取消、陈旧写回、调用方 ctx 已经死了）不算；中断、
// 失败、软取消之后的重跑，可重做的记录跟普通记录一样照常读回。
func Rerunnable() StepOption {
	return func(o *stepOptions) { o.rerunnable = true }
}

// SaveStepResult 记下本次节点执行里 key 这一步的结果——**立即落进 cm，不等节点 return**。
// 节点中断 / 失败之后重跑时（同一次节点执行的续跑），用 [GetStepResult] 读回，跳过已经做成
// 的那一步。
//
// 「落进 cm」不等于落盘：它随这个节点**以没成的结局退出时**的那份存档落盘（中断、失败、取消），
// 超步中途的 durability 存档不带它（投影只导出 frontier 上节点的条目，见 serialization.go）。
// 所以进程在节点执行中途崩掉时，已经记下的步骤跟着丢——崩溃恢复后那一步会再做一遍。
//
// 跟 [SaveNodeState] 是同一个载体、同一套生命周期（键是「这个节点的这一次执行」，节点成功后
// 自然不可达、不设清除点、只落本层存档），区别只在于**按 key 分条**：节点里有好几件互不相干
// 的事要各自记住时，不必把它们攒进一个值里整体读改写，并发的几条也互不干扰（同一个 key 后写
// 覆盖先写）。[SaveNodeState] 那一个槽仍归节点整体使用，两者不冲突。
//
// 工具函数里调时记在这个 tool_call 名下（按 tool_call 分域，见 [WithCurrentToolCallContext]）：
// 同一个 key 在同批两个 tool_call 里是两条记录。
//
// 图的钩子（`OnNodeStart` / `OnNodeEnd`）与条件边里调时，记在那个节点的这次执行名下、按相位
// 分开（钩子里的 key 跟节点体里的同一个 key 是两条）——节点重跑时钩子也会再触发一次，钩子里
// 做成的步骤同样拿得回来。嵌套图的 `OnGraphStart` / `OnGraphEnd` 记在承载这张图的那个外层
// 节点执行名下。
//
// # 什么时候用
//
// 节点重跑时要**原样拿回上次的结果**、而不是再做一遍的那一步：
//
//   - 有副作用的（下单、扣款、发消息）——再做一遍就是重放副作用；
//   - 每次做结果都不一样的（直接调模型、读当前时间）——下游拿它当输入时，重跑换了输入，
//     下游的嵌套调用会被当成一次新调用重新执行（重新问人）。把它记住，重跑时下游就拿到跟
//     上次一样的输入。
//
// `agent_v2.RunSubAgent` 就是用它记住已完成的嵌套调用的。
//
// 被跳过的那一步里问过人（[Ask]）时，那一问放进它自己的相位里问（[EnterPhase]）：在节点体里问的话，提问按
// 发起次序编号，跳过它会让后面的提问挪到它的 ID 上，调用方累积传答案时拿到它的答案（见 [Ask]
// 「按同样的次序问」）。
//
// # 唯一的硬规约：记的是「已经做成了什么」，不是「打算做什么」
//
// 同 [SaveNodeState]：**写在副作用真正提交之后**，否则软取消 / ctx 取消之后重跑会跳过一件
// 根本没做成的事，而且不报错。
//
// **v 必须已经 `RegisterValueType` 注册**（与 channel value 同一套 tagged JSON 协议），没注册
// 时返 error。没有节点执行可挂时（图外直接调、顶层图的 `OnGraphStart` / `OnGraphEnd` 与入口
// 条件边）是 no-op 返 nil。
func SaveStepResult(ctx context.Context, key string, v any, opts ...StepOption) error {
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return nil
	}
	var o stepOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	tv, err := marshalValue(v)
	if err != nil {
		return fmt.Errorf("SaveStepResult %q: %w", key, err)
	}
	k := stepStateKey(a.base(), key)
	if o.rerunnable {
		k = a.redoKey(key)
	}
	a.cm.putNodeState(k, tv)
	return nil
}

// GetStepResult 读回本次节点执行里 key 这一步记下的结果（见 [SaveStepResult]）。
//
// 记的时候带没带 [Rerunnable] 这里不用重复声明：两种都查，普通记录优先。
//
// 第二个返回值 false：这一步还没做成过 / 不在节点执行里 / 类型对不上（业务换了类型）/ 记的是
// 可重做的那种、而节点已被要求重新产出——都按「没有记录、照常去做」处理。⚠️ 同
// [GetNodeState]：ok==true 不保证值非 nil（T 是 map / slice / 指针时存的若是 nil，读回是
// typed nil）。
func GetStepResult[T any](ctx context.Context, key string) (T, bool) {
	var zero T
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return zero, false
	}
	tv, ok := a.cm.getNodeState(stepStateKey(a.base(), key))
	if !ok {
		tv, ok = a.cm.getNodeState(a.redoKey(key))
	}
	if !ok {
		return zero, false
	}
	v, err := unmarshalValue(tv)
	if err != nil {
		return zero, false
	}
	typed, ok := v.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}
