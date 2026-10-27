package compose_v2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"
)

// 节点私有中断态——**节点自己攒、随 cp 持久化、Resume 时只有它自己读回来**的一块状态。
//
// # 它解决的问题
//
// 节点是原子的：任一部分失败（含抛 `InterruptError`）整节点的 writes 全部作废、Resume 时
// 从头重跑。对「一个节点内做了 N 件带副作用的事」的形状，这意味着已经做成的那几件会被重做
// ——典型是 BasicAgent 的 tools 节点：一轮 N 个 tool_call，其中一个抛中断，Resume 时下单 /
// 发消息 / 扣款各来第二遍。框架此前的说法是「业务工具自负幂等」，那是把框架能解的问题推给
// 每一个业务工具，而 HTTP 调用 / 写库 / 发消息天然不幂等。
//
// 本原语给节点一个地方记「我已经做完了哪些、结果是什么」，重跑时读回来跳过。
//
// # 为什么是 ctx 访问器而不是 InterruptError 字段 / sendSink 惯例
//
// 两条路都被既有结构堵死（读码 + 探针确认，别再走）：
//
//   - **不能挂在 `InterruptError` 上**：`frontierResult.AllInterrupts` 是裸
//     `[]*InterruptError`、**不带成员身份**。框架收到 N 个 ie 时分不出哪个来自哪个节点，
//     没法把 state 路由到正确的键。
//   - **不能复用 `sendSink` 那套「节点 return 后 drain」**：`drainSendSink` 只在**成功
//     路径**跑（`graph_loop.go` 里所有 error 路径都在它上面 return）。中断是 error 路径
//     ——memo 会恰好在唯一需要它的时刻被丢弃。
//
// 所以走**ctx 访问器**：`runNodeWithHooks` 在节点入口注入一个绑定了 (cm, key) 的访问器，
// 节点调 `SaveNodeState` 立即落进 cm（不等 return，中断路径照样活着）。
//
// # 键：(NodeID, InstanceID, Visit)
//
// 三维，与 `cm.visitCount`（`map[NodeID]int`）同 scoping——**cm 就是 ns 边界**，每层图各
// 自一个 cm、cp 按 (sid, ns) 物理分离，所以键不需要 GraphNS 维（探针实测：两个 cm 的同名
// 节点计数互不影响）。
//
//   - `InstanceID` 与 `Visit` 是**互斥两维**（同 `InterruptSite.InterruptID` 的既有语义）：静态边
//     节点恒空身份、靠 visit 区分轮次；`SendFrom` 派生实例每次投递 fresh mint 身份，
//     **由 `nodeStateKey` 把 visit 维置 0**。两者合起来唯一标定「这个节点的这一次执行」。
//
//     ⚠️ 是**我们置 0**，不是它读到的值天生为 0——这里曾经写作「实例的 visit 恒 0」，
//     那是假的：`visitCount` 是 per-NodeID **共享**的，同一个 NodeID 可以同时是静态边目标
//     和 `SendFrom` 目标（混合 frontier），静态那位成功就把它推高了。照那句话推断的后果
//     实测过：写 memo 用推高前的值、导出投影用推高后的值 ⟹ memo 整条掉出存档 ⟹
//     Resume 重放已完成的副作用（扣款 2 次）。哨兵 node_state_mixed_frontier_test.go。
//   - **键在注入点算一次、读写共用**：不给「读的时候按一套算、写的时候按另一套算」留缝。
//
// # 生命周期：不设清除点
//
// `visit` **只在成功路径递增**——推进点在超步的**提交点**（写入提交成功那一刻，
// graph_loop 主循环），这一步没成（节点报错 / 中断 / cond 算不出出边 / 提交失败）就不动。
// 探针实测中断 0→0、成功 0→1。
// 所以节点一旦成功，它这次执行的 state 键就再也不会被算出来——**旧 state 自然不可达**，
// 正确性上不需要在任何时刻显式清除。
//
// 但**空间不是"时机不敏感"的小事**：旧条目一直挂在 cm 上、每超步随 cp 全量重写一遍 ⟹
// 写入字节数随轮数平方增长。实测长 ReAct（6 轮 × 2 tool_call、800B tool_result）下
// `node_states` 约占整份 cp 的一半；20 轮合成循环里 cp 从 4.4KB 涨到 78KB。
// 用 sync / async durability + 真后端时按这个量级留预算（MySQL 默认
// `max_allowed_packet` 才 4MB）。要收的话正解是**在 Snapshot 侧只导出当前 frontier 节点
// 当前 visit 的键**（一次投影，不是清除点）——别改成在 cm 上删。
//
// ⚠️ **这一条是设计的要害**。撤回的那版 tool memo（走 `RunSession` KV）7 条 HIGH 里有 4 条
// 由清除点引出：清整张表把兄弟还挂着的条目清了 / 业务错出口在清空点之前 return 泄漏到下
// 一次 Run / 清空点在闭包出口而 hook 在节点之后抛中断 / 补丁自己的三条回归全红。
// **「让旧状态自然不可达」比「在正确时刻清除」少一整类 bug**——别把清除点加回来。
//
// ## 曾经有一处例外，现在没有了（别把 purge 加回来）
//
// 出边计算还是「提交之后的独立相位」时，一个已成功的 `SendFrom` 实例会被 cond 报错拉回来
// 重跑，而它的键跟上次完全重合（`advanceVisitsLocked` 里那句 visit 自增只由空身份成员执行 ⟹ 实例的 visit 由 `nodeStateKey` 置 0（**不是**它读到的值恒为 0——共享计数器会被兄弟推高），
// 且降级重跑时 InstanceID 复用）⟹ 读到自己上次那份、跳过本该重做的事。那时挂了一道按
// 实例精确清 memo 的 purge 来兜。
//
// 出边归成员之后（cond 排在提交之前、算不出来就是该成员失败），「已成功成员被拉回重跑」
// **不可达** ⟹ 那道 purge 连同它依赖的整套机件一并删除，本条例外随之消失。
//
// 现在两类成员由**同一条规则**保护：这一步成了 ⟹ 提交点推进 visit ⟹ 旧键不可达；
// 没成 ⟹ visit 不动 ⟹ 读得回自己上次的进度（**那正是 memo 的全部意义**——已经下单/扣款
// 的那几件不再重放）。哨兵见 `node_state_internal_test.go` 的
// `TestNodeState_CondFailedInstance_KeepsOwnMemo` 与
// `TestNodeState_StaticMemberCondFailed_KeepsMemoAndVisit`。
//
// # 不向上冒泡
//
// state 写进本层 cm ⟹ 只落本层 cp。这是结构使然不是约束：每层图各自一个 `ChannelManager`
// （`subgraph.go` 的 `subCM := NewChannelManager()`），子 cp 物理分离在 (sid, childNS)。
//
// 对比 `PendingInterrupts`：同一个中断 ID 在父 cp 和子 cp **各存一份**（探针实测），因为外
// 层要知道「下面有人在等」才能把 query 报给调用方。那半是**给调用方看的**；本原语这半是
// **节点自用的**，两半分档、互不搭车。

// nodeStateKey 算「这个节点的这一次执行」的键。
//
// 只在注入点调一次（`runNodeWithHooks`），读写共用其结果。
//
// # InstanceID 与 Visit 互斥——跟中断寻址用同一条规则
//
// 有实例身份时**忽略 visit**。必须忽略：`visitCount` 是 **per-NodeID 共享**的，而同一个
// NodeID 可以**同时**是静态边目标和 `SendFrom` 目标（混合 frontier）。那时静态那位成功
// 会把计数器推高，于是实例那位在节点入口写 memo 用的是推高**前**的值、导出投影按推高
// **后**的值去找——**miss，memo 整条掉出存档**，Resume 时已完成的副作用照样重放。
//
// 这跟 `InterruptSite.InterruptID()` 是同一条规则（ISSUE-96 的结论）。两处对同一组维度
// 必须做同一件事——曾经一处互斥、一处三维全用，实测混合 frontier 下 memo 掉档、扣款重放。
func nodeStateKey(id NodeID, instanceID string, visit int) string {
	// 有实例身份 ⟹ 它已完整表达「哪一个待办」，visit 维置 0（见上）
	if instanceID != "" {
		visit = 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(instanceID))
	_, _ = h.Write([]byte{0})
	_, _ = fmt.Fprintf(h, "%d", visit)
	return fmt.Sprintf("%016x", h.Sum64())
}

// nodeStateAccessor 绑定了「往哪存」和「存在哪个键下」——注入 ctx 供节点读写。
//
// 把 cm 和 key 一起绑住是有意的：节点不需要（也不能）自己算键，读写两侧不可能分叉。
//
// 同一次节点执行名下的所有条目，键都以 key（16 位十六进制）打头、后面若有别的段就用冒号
// 隔开——存档投影按「第一个冒号之前那段」认归属（见 serialization.go 的 NodeStates 投影）。
type nodeStateAccessor struct {
	cm  *ChannelManager
	key string
	// scope 非空时，这是节点执行里的一个分域：它有自己的那一格（[SaveNodeState] /
	// [GetNodeState]）和自己的步骤记录（[SaveStepResult]），跟节点体自己的互不相干。用在
	// 「不是这个节点的节点体、却跑在它的执行范围里」的代码上：
	//
	//   - 某一个 tool_call（[WithCurrentToolCallContext] 派生）：并发的 tool_call 各记各的，也碰
	//     不到 tools 节点自己那一格（它记着哪些 tool_call 已经做完）——没有分域时，工具里的一次
	//     SaveNodeState 就把那份账覆盖了，已执行过的工具在续跑时再执行一遍（实测）；
	//   - 某一个钩子相位 / 条件边（见 [withHookPhase]）；
	//   - 交出去的一段执行（[EnterPhase] / [HandOver]：业务工具钩子；嵌套调用的被调方在自己的图之外
	//     跑它的 agent 级钩子）——没有分域时，那里的一次 SaveNodeState 覆盖调用方节点自己记的进度
	//     （实测）；
	//   - 同一个位置按某个维度再分账（[WithNodeStateSealed]：工具体按工具名）。
	//
	// 分域可以叠（嵌套图的图级钩子跑在外层某个 tool_call 里时两层都在）。
	scope string
	// chain 是这个分域从外到里的每一层（每个元素是叠到那一层为止的完整 scope）：重做代际按分域
	// 记，有效代际要把执行级与链上每一层的加起来（见 [ChannelManager.effectiveRerunGen]；交出去的
	// 那一段例外，见 handedOver）。
	//
	// 不变量：scope 非空时 chain 的最后一项就是 scope——只由 scopedOf 构造。字面量直接填 scope
	// 的话 chain 为空，这个分域被推进的代际谁也读不到（实测：节点级钩子相位里的否决推进了、
	// 钩子自己看不见，续跑永不收敛）。机械哨兵见 node_state_scope_internal_test.go。
	chain []string
	// handover 非 nil 表示这是 [HandOver] 封出来的那一格：这次执行连同输入是从调用方交过来的。
	// 只挂在交接那一层，scopedOf 往下不带。
	handover *handover
	// handedOver 为真表示这一格在一段交接出去的执行里（[HandOver] 封的那一格，以及在它下面叠的
	// 分域，scopedOf 往下照带）：有效代际只算 chain[genFrom:]——交接那一层往下的分域——不算调用方
	// 那一层（执行级与交接之前的分域）。调用方那一层重做时这次调用是不是同一次调用，由调用的身份
	// 决定（见 [HandOver]）；把调用方的代际算进来的话，身份没变、正挂着的调用里问过的问题会换 ID
	// 再问一遍，按 [Rerunnable] 记下的东西也跟着作废（实测）。
	handedOver bool
	genFrom    int
}

// 节点执行键之后的分段标记。节点执行键是十六进制、不含冒号。
const (
	scopeSep       = ":scope:"
	rerunGenSuffix = ":rerun"
)

type nodeStateAccessorKey struct{}

func withNodeStateAccessor(ctx context.Context, cm *ChannelManager, id NodeID, instanceID string, visit int) context.Context {
	if cm == nil {
		return ctx
	}
	return context.WithValue(ctx, nodeStateAccessorKey{}, &nodeStateAccessor{
		cm:  cm,
		key: nodeStateKey(id, instanceID, visit),
	})
}

// withAccessor 把 a 装进 ctx（nil 表示「这里没有可记的地方」）。
func withAccessor(ctx context.Context, a *nodeStateAccessor) context.Context {
	return context.WithValue(ctx, nodeStateAccessorKey{}, a)
}

func nodeStateAccessorFrom(ctx context.Context) *nodeStateAccessor {
	a, _ := ctx.Value(nodeStateAccessorKey{}).(*nodeStateAccessor)
	return a
}

// scopedOf 返回 a 在 scope 这个分域里的版本（叠在 a 已有的分域之下）；a 为 nil 时返回 nil。
func scopedOf(a *nodeStateAccessor, scope string) *nodeStateAccessor {
	if a == nil {
		return nil
	}
	c := *a
	if c.scope == "" {
		c.scope = scope
	} else {
		c.scope = c.scope + "\x00" + scope
	}
	c.chain = append(append(make([]string, 0, len(a.chain)+1), a.chain...), c.scope)
	// 交接信息只属于交接那一层：往下叠的分域（被调方主图的图级钩子、入口条件边……）不是
	// 「交过来的那次执行」本身
	c.handover = nil
	return &c
}

// base 是这个访问器名下所有条目的键前缀：节点执行键，再加分域（有的话）。
func (a *nodeStateAccessor) base() string {
	if a.scope == "" {
		return a.key
	}
	return a.key + scopeSep + shortHash(a.scope)
}

// shortHash 把任意串压成定长的十六进制（sha256 前 16 字节）。派生键要经得起每个后端的
// JSON 往返，原样放进去的串里可能有 NUL / 非法 UTF-8（见 [stepStateKey]）。
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

// nodeStateOwner 是派生键 k 所属的那次节点执行的键——第一个冒号之前那段。
func nodeStateOwner(k string) string {
	if i := strings.IndexByte(k, ':'); i >= 0 {
		return k[:i]
	}
	return k
}

// WithNodeStateSealed 返回只把节点私有态分出一格的 ctx：调用方那一格封住，scope 这个分域里有它
// 自己的一格与自己的步骤记录（[SaveNodeState] / [SaveStepResult] 都记在分域名下）；提问与嵌套
// 调用的计数、相位名都不变。
//
// 给「同一个位置上的代码按某个维度再分账」用——典型是 BasicAgent 的工具体按工具名再分一层，同一个
// tool_call 位置上不同工具记下的互不相认。
//
// 要把一段代码**交出去**（业务钩子、另一个 agent）用 [EnterPhase] / [HandOver]：那段代码还要
// 有自己的计数与相位名，只封私有态不够——钩子里问一次就占掉调用方的一个提问序号（实测：调用
// 被拿回、钩子不再执行时，调用方后面那一问换了 ID 被再问一遍）。
func WithNodeStateSealed(ctx context.Context, scope string) context.Context {
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return ctx
	}
	return withAccessor(ctx, scopedOf(a, "sealed\x00"+scope))
}

// SaveNodeState 记下「本节点这一次执行已经做到哪儿」——**立即落进 cm，不等节点 return**。
//
// # 唯一的硬规约：记的是「已经做成了什么」，不是「打算做什么」
//
// **写 memo 必须在副作用真正提交之后**。反过来（先记后做）在两种非中断退出下会让
// 重跑跳过一件**根本没做成**的事，而且都不报错：
//
//   - **软取消**（`WithSoftCancel`）——在超步边界停，但节点已经写下的那份留在 cp 里；
//   - **ctx 硬取消**——Go 不杀 goroutine，节点自己发现后返错，中途写的那份同样已落 cm，
//     而 `visit` 只在成功路径递增 ⟹ 键完全重合 ⟹ Resume 原样读回。
//
// 这两种退出下「已写 memo 但事情没做成」和「已写 memo 且做成了」在数据上**无法区分**
// ——框架看到的都是同一份 state。区分它们的唯一办法就是这条写入顺序约定。
//
// 进程在节点执行中途崩溃不在此列：memo 只随节点「没成」的那次出口存档落盘，durability
// （sync / async）只在超步边界写档，那一刻这个节点还没开始或已经结束——崩溃时 memo 整个
// 丢了，节点重跑时从头做，副作用会重放。扛这种崩溃只能靠副作用自己的幂等键。
//
// ⚠️ 对端有两个框架把同款 memo 的**写入权收在框架手里**（绑死在「子单元成功返回」那条
// 边上，业务拿不到「先记后做」的口子）。我们把口子敞给了业务换取灵活性——代价就是这条
// 规约得靠人守。详见 `doc/description/three-way-comparison/interrupt-resume.md`。
//
// 典型用法（节点内做 N 件带副作用的事，做完一件记一次）：
//
//	func init() { compose_v2.RegisterValueType[map[string]string]("myapp.done") }
//
//	done := map[string]string{}
//	if prev, ok := compose_v2.GetNodeState[map[string]string](ctx); ok && prev != nil {
//	    done = prev // 重跑：读回上次做完的
//	}
//	for _, item := range items {
//	    if _, already := done[item.ID]; already {
//	        continue // 上次已做完，跳过——不重复副作用
//	    }
//	    out, err := doSideEffect(item)
//	    if err != nil {
//	        return nil, err
//	    }
//	    done[item.ID] = out
//	    if err := compose_v2.SaveNodeState(ctx, done); err != nil { // 每做完一件就记，中断也不丢
//	        return nil, err
//	    }
//	}
//
// ⚠️ 返回的 error 别丢：值的类型没注册时这里报错、什么也没记下，丢掉它就是「以为记了进度，
// 重跑时照样重放」。
//
// 每件事里要问人（[Ask]）时，每件的提问放进它自己的相位里问（`EnterPhase(ctx, "confirm:"+item.ID)`）。在
// 节点体里问的话，提问按发起次序编号，跳过做完的那件会让后面的提问挪到它的 ID 上：调用方累积传答案时，
// 后面那一问拿到的是做完那件的答案，`err == nil`；只传当前那条答案时后面那一问被再问一遍
// （两件都问人时实测）。被跳过的那件问过人、后面还有别的提问就会中，见 [Ask]「按同样的次序问」。
//
// 循环里的事是嵌套调用（`agent_v2.RunSubAgent`）时不必自己记、也**别自己按进度跳过**：
// 已完成的嵌套调用框架会记住（[SaveStepResult]），父节点重跑时直接拿回结果、不再执行，全部
// 照调一遍即可。跳过有两个后果：那次调用当初做出的转交与路由决定（发给上层 HandoffAgent /
// RoutingAgent 的上行信号）只在「再调一次、被拿回」时交出，跳过就丢了（`err == nil`）；**输入完全相同的几次
// 调用**按发起次序区分，跳过其中一次会让后面那次拿到它的记录（那种写法要给每次调用一个 key，
// `agent_v2.WithCallKey`）。
//
// 在工具函数里调时，落的是这一个 tool_call 自己的槽（按 tool_call 分域，见
// [WithCurrentToolCallContext]；BasicAgent 的 tools 节点再按工具名分一层），碰不到 tools 节点
// 自己记的账，也不跟同批别的 tool_call 串。BasicAgent 的工具钩子（OnToolStart / OnToolEnd）
// 同样各有自己的一格。
//
// **v 必须已经 `RegisterValueType` 注册**（与 channel value 同一套 tagged JSON 协议）——
// 没注册时返 error，业务在 init 里注册即可。
//
// 在节点外调（无访问器）是 no-op 返 nil：让业务的 helper 函数在图外单测时不必特殊处理。
//
// 图的钩子相位（`OnNodeStart` / `OnNodeEnd` 等，与条件边）里、交出去的一段执行里（业务工具钩子、
// 嵌套调用的被调方在自己的图之外，见 [EnterPhase] / [HandOver]）调时，记的是那个相位 / 那段执行
// **自己的**一格，不是节点体这一格
// ——那里的代码不是这个节点的节点体，这一格不归它。节点重跑、钩子再触发时读得回来。顶层图的
// `OnGraphStart` / `OnGraphEnd` 与入口条件边没有节点执行可挂，那里是 no-op。
func SaveNodeState(ctx context.Context, v any) error {
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return nil
	}
	tv, err := marshalValue(v)
	if err != nil {
		return fmt.Errorf("SaveNodeState: %w", err)
	}
	a.cm.putNodeState(a.base(), tv)
	a.cm.noteStateWrite(ctx, a.base())
	return nil
}

// GetNodeState 读回本节点上一次执行存下的 state（重跑 / Resume 时）。
//
// 第二个返回值 false 的三种情况业务不必区分，都按「没有上次、从头做」处理：本节点这次是
// 首次执行 / 上次没存过 / 类型不匹配（业务换了 state 类型）。
//
// ⚠️ **ok==true 不保证值非 nil**：T 是 map / slice / 指针时，`SaveNodeState(ctx, nil 的
// 那个 T)` round-trip 回来是 typed nil，断言成功 ⟹ 返 `(nil, true)`。上面那段范式里的
// `&& prev != nil` 不是多余的守卫——少了它，重跑时会 `assignment to entry in nil map`。
func GetNodeState[T any](ctx context.Context) (T, bool) {
	var zero T
	a := nodeStateAccessorFrom(ctx)
	if a == nil {
		return zero, false
	}
	tv, ok := a.cm.getNodeState(a.base())
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
	// 从别的执行拿回的进度：它对应哪些流框架说不清，这次执行取代的流一个都不撤（见 stream_attempt.go）
	a.cm.noteRecall(ctx, a.base())
	return typed, true
}
