package compose_v2

import (
	"context"
	"maps"
	"strconv"
	"sync"
)

// subCallCounterKey 是「本次执行里，同一个计数键的第几次」计数器的 ctx key。
type subCallCounterKey struct{}

// subCallCounter 按计数键分别计数，同一个键上的调用按发起顺序编号。
//
// 按键分开计，不是整个执行共用一个：共用时任何一处调用的增减都会让**别的键**上的编号
// 跟着移。按键分开计之后，编号只受同一个键上的调用影响。
//
// 持有锁，只按指针用、不拷贝。
type subCallCounter struct {
	mu   sync.Mutex
	next map[string]int
}

// withSubCallCounter 注入一个新的计数器。框架在「一次执行的起点」调用：进节点时
// （[withNodeInterruptCounter]）、进每个 tool_call 时（[WithCurrentToolCallContext]）、以及每次
// 调钩子（图级与节点级）、每次求一条条件边时（[withHookPhase]）。进一张新图时则清成 nil
// （[clearInheritedNodePosition]）：新图的执行不能接着外层数。
func withSubCallCounter(ctx context.Context) context.Context {
	return context.WithValue(ctx, subCallCounterKey{}, &subCallCounter{})
}

// subCallScopeKey 是「当前代码跑在哪个相位」的 ctx key，见 [SubCallScope]。
type subCallScopeKey struct{}

// SubCallScope 返回当前代码所在的相位：节点体里是空串；图的钩子（`OnGraphStart` /
// `OnGraphEnd` / `OnNodeStart` / `OnNodeEnd` / `OnNodeEvent`）与条件边里是那个相位的名字。
// 节点级钩子与条件边的相位名含所属的节点（入口条件边是 StartNode）、这是它的第几次执行（或
// 实例身份）与条件边的序号；图级钩子的相位名含这张图的 ns。
//
// ⚠️ 它**只在一张图之内**区分相位：两张嵌套图里同名节点的 `OnNodeStart` 相位名相同。拼身份时
// 要连同 [GetGraphNS] 一起用——`agent_v2.RunSubAgent` 就是这么拼的（ns、节点、相位一起进
// 嵌套调用的身份），于是钩子里的调用跟节点体里的调用、两个节点各自钩子里的调用、同一个节点
// 第 1 次与第 2 次执行时钩子里的调用，都是不同的调用。
//
// 返回值是不透明的串，只用来比较相等、拼进身份。不在任何图的执行里时返回空串。
func SubCallScope(ctx context.Context) string {
	s, _ := ctx.Value(subCallScopeKey{}).(string)
	return s
}

// withHookPhase 给一次图级钩子或一次条件边求值铺好它的相位：一个新计数器、相位名，以及这个
// 相位记东西的地方（a 为 nil 表示没有）。
//
// 计数器每次调用各自从 0 数：钩子在续跑时会再触发一次，同样的调用序列算出同样的编号。不配的
// 话，同一次钩子调用里对同一个位置、同一份输入调两次，两次算出同一个身份。
//
// a 由调用方给成带相位分域的访问器（见 nodeStateAccessor.scope）：钩子不是节点体，节点体那一格
// 不归它，它有它自己的。
func withHookPhase(ctx context.Context, scope string, a *nodeStateAccessor) context.Context {
	ctx = withSubCallCounter(ctx)
	ctx = context.WithValue(ctx, subCallScopeKey{}, scope)
	return withAccessor(ctx, a)
}

// withBodyPhase 把相位清回「节点体」。钩子返回的 ctx 通常派生自钩子相位的那个，节点体
// 不能带着钩子的相位名跑。
func withBodyPhase(ctx context.Context) context.Context {
	return context.WithValue(ctx, subCallScopeKey{}, "")
}

// nodeHookScope 是节点级钩子（OnNodeStart / OnNodeEnd / OnNodeEvent）的相位名。
// 实例身份与 visit 互斥，规则同 [nodeStateKey]。
func nodeHookScope(kind string, id NodeID, instanceID string, visit int) string {
	if instanceID != "" {
		visit = 0
	}
	return kind + "\x00" + string(id) + "\x00" + instanceID + "\x00" + strconv.Itoa(visit)
}

// graphHookScope 是图级钩子（OnGraphStart / OnGraphEnd）与入口条件边的相位名。带上这张图的
// ns（嵌套时再带上外层这次执行里第几次跑这个 ns，见 compose_v2.Run 入口的 graphPhaseNS）：嵌套图
// 的图级相位记在承载它的那个外层节点执行名下，同一次外层节点执行里可能跑好几张嵌套图，也可能把
// 同一张跑好几遍。
func graphHookScope(kind, graphPhaseNS string) string {
	return kind + "\x00" + graphPhaseNS
}

// condScope 是 from 这个成员第 edge 条出边的 cond 的相位名。
func condScope(from NodeID, instanceID string, visit, edge int) string {
	return nodeHookScope("cond", from, instanceID, visit) + "\x00" + strconv.Itoa(edge)
}

// condPhase 是一个成员算出边时用的相位底子：相位名前缀与记东西的地方。
type condPhase struct {
	from       NodeID
	instanceID string
	visit      int
	// acc 是成员这次执行的访问器（forEdge 再按条件边分域）。入口（StartNode）是承载这张图的外层
	// 节点那次执行、按这张图分域；顶层图的入口为 nil。
	acc *nodeStateAccessor
}

// forEdge 返回第 edge 条出边的 cond 的相位 ctx。
func (p condPhase) forEdge(ctx context.Context, edge int) context.Context {
	scope := condScope(p.from, p.instanceID, p.visit, edge)
	return withHookPhase(ctx, scope, scopedOf(p.acc, scope))
}

// subCallMark 是计数器在某一刻的编号，给「同一次调用的重试」用（见 [RunWithRetry]）。
type subCallMark struct {
	c    *subCallCounter
	next map[string]int
}

// markSubCalls 记下 ctx 里计数器此刻的编号。没有计数器时返回零值，rewind 什么也不做。
func markSubCalls(ctx context.Context) subCallMark {
	c, _ := ctx.Value(subCallCounterKey{}).(*subCallCounter)
	if c == nil {
		return subCallMark{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return subCallMark{c: c, next: maps.Clone(c.next)}
}

// rewind 把计数器退回记下的那一刻：重试的每次尝试都从同一个编号数起，里面的调用算出跟
// 第一次尝试相同的身份。
func (m subCallMark) rewind() {
	if m.c == nil {
		return
	}
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	m.c.next = maps.Clone(m.next)
}

// NextSubCallIndex 返回本次执行里 `key` 这个计数键已经出现过几次（从 0 起），并把计数加一。
// `key` 由调用方给。
//
// `agent_v2.RunSubAgent` 用它给嵌套调用定身份的最后一维：同一个位置上**输入完全相同**的几次
// 调用（投票、冗余调用）按发起次序区分——输入不同的调用本来就按内容区分开了，用不到它；
// 同一个 call key 在同一次执行里出现第二次（两次调用会共用一个身份）也靠它发现。
//
// # 跨续跑稳定的前提
//
// 计数器在每次执行起点新建，节点重跑时从 0 重新数。同一个键上的调用只要按同样的次序发起，
// 第 k 次在首跑和续跑里拿到同一个 k——跟 [Ask] 的提问序号是同一个前提。
//
// # 重试同一次调用
//
// [RunWithRetry] 的每次尝试都从进入重试那一刻的编号数起，所以重试里的调用算出跟第一次尝试
// 相同的身份、续上第一次尝试的存档（提问序号也一起退回，见 [RunWithRetry]）。**手写的重试循环
// 没有这个待遇**：计数器分不出「同一次调用再试一遍」和「再调一次」，循环里每次都算新的一次。
//
// # 没有计数器时
//
// 不在任何执行里（业务在框架外直接调）时恒返回 0：那里没有「节点重跑」这回事。
//
// ⚠️ 并发安全，但**同一次执行里并发发起**同一个键上的调用时，谁拿到几号取决于调度，续跑时
// 对不上。要并发请走框架的 fan-out（`SendFrom` / ParallelAgent / tools 节点的并发 tool_call），
// 它们每个实例各有自己的计数器。另一个只在自起 goroutine 时才出现的例外：[RunWithRetry] 的
// 回退作用于**整张**计数表，一个 goroutine 在重试、另一个同时在调别的键时，别处的编号也会被
// 一起退回。
func NextSubCallIndex(ctx context.Context, key string) int {
	c, _ := ctx.Value(subCallCounterKey{}).(*subCallCounter)
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next == nil {
		c.next = make(map[string]int)
	}
	k := c.next[key]
	c.next[key] = k + 1
	return k
}
