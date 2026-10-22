package compose_v2

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// AllNodes 是断点的通配：这张图的每一个节点（[GraphBuilder.WithInterruptBefore] /
// [WithInterruptBefore] 等的参数）。也因此不能拿来当节点名。
const AllNodes NodeID = "*"

// BreakpointInterruptReason 是声明式断点停下时 [InterruptError] 与它的 [AskQuery] 上的 Reason。
const BreakpointInterruptReason = "breakpoint"

// Breakpoint 说明一次声明式断点停在了哪儿——[AskQuery.Breakpoint] 非 nil 即这一问是断点停下的，
// 不是节点在提问：没有问题要回答，续跑（带不带答案都行）就从这里继续。
//
// 一个超步边界上 after 与 before 同时命中时只停一次，两个字段都填。
type Breakpoint struct {
	// Before 是这个边界上即将执行、命中 before 断点的节点（去重、按字典序）。
	Before []NodeID `json:"before,omitempty"`
	// After 是刚整体提交的那个超步里、命中 after 断点的节点（去重、按字典序）。
	After []NodeID `json:"after,omitempty"`
	// NS 是停下的那张图的 ns（顶层图为空）。
	NS string `json:"ns,omitempty"`
}

func (b *Breakpoint) clone() *Breakpoint {
	if b == nil {
		return nil
	}
	c := *b
	c.Before = append([]NodeID(nil), b.Before...)
	c.After = append([]NodeID(nil), b.After...)
	return &c
}

// breakpointSet 是一组断点节点：all 为真时命中每一个节点。零值 = 没有断点。
type breakpointSet struct {
	all   bool
	nodes map[NodeID]bool
}

func (s breakpointSet) empty() bool { return !s.all && len(s.nodes) == 0 }

func (s breakpointSet) with(ids []NodeID) breakpointSet {
	if len(ids) == 0 {
		return s
	}
	out := breakpointSet{all: s.all, nodes: make(map[NodeID]bool, len(s.nodes)+len(ids))}
	for id := range s.nodes {
		out.nodes[id] = true
	}
	for _, id := range ids {
		if id == AllNodes {
			out.all = true
			continue
		}
		out.nodes[id] = true
	}
	return out
}

// match 返回 members 里命中这组断点的节点（去重、按字典序）。
func (s breakpointSet) match(members []FrontierMember) []NodeID {
	if s.empty() || len(members) == 0 {
		return nil
	}
	seen := make(map[NodeID]bool, len(members))
	var out []NodeID
	for _, m := range members {
		if seen[m.NodeID] || !(s.all || s.nodes[m.NodeID]) {
			continue
		}
		seen[m.NodeID] = true
		out = append(out, m.NodeID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// checkBreakpointNodes 校验断点里的节点名都是这张图的节点（或 AllNodes）。nodes 为 nil 时只看是否
// 碰上起止节点。
func checkBreakpointNodes(nodes map[NodeID]NodeFunc, kind string, ids []NodeID) error {
	for _, id := range ids {
		if id == AllNodes {
			continue
		}
		if id == StartNode || id == EndNode {
			return fmt.Errorf("%s 断点不能设在起止节点 %q 上", kind, id)
		}
		if _, ok := nodes[id]; !ok {
			return fmt.Errorf("%s 断点里的节点 %q 不在这张图里（只认这张图自己的节点；子图里的节点在子图的 builder 上设）", kind, id)
		}
	}
	return nil
}

// errInvalidBreakpoint 是断点配置用不了：运行期断点选项里有不认识的节点名，或者断点设在一张没法续跑的
// 嵌套图上（见 breakpointStop）。实现 [NonRetryable]：配置错重试必然一样；不用 [ErrRunAborted]——节点名
// 写错时它抛在图开始执行之前、一个字节都没写过，带上那个哨兵会让出口清掉同一个槽里与这次无关的存档
// （比如这次本来要续的那份）；嵌套图那种本来就没有存档。
type errInvalidBreakpoint struct{ msg string }

func (e *errInvalidBreakpoint) Error() string { return e.msg }

// NonRetryable 实现 [NonRetryable]。
func (e *errInvalidBreakpoint) NonRetryable() {}

// WithInterruptBefore 给本次 Run 加 before 断点：超步边界上即将执行的节点里有 nodes 中的（或传
// [AllNodes]），就在这个超步开始之前停下，这个超步一个节点都不执行。语义与停下之后怎么继续见
// [GraphBuilder.WithInterruptBefore]。
//
// 只作用于这一次 Run、只作用于这一层图（嵌套图、子图里的节点在它们自己的 builder 上设），与编译期
// 声明的取并集——编译期声明的断点关不掉。**不进存档**：逐步执行时每次续跑都要再传一遍，不传就一路跑完。
// 节点名不在这张图里时 Run 直接报错（不碰存档）。
func WithInterruptBefore(nodes ...NodeID) RunOption {
	return func(c *runOptions) { c.interruptBefore = append(c.interruptBefore, nodes...) }
}

// WithInterruptAfter 给本次 Run 加 after 断点：nodes 中的节点（或 [AllNodes]）所在的超步整体提交
// 之后停下。其余同 [WithInterruptBefore]；语义见 [GraphBuilder.WithInterruptAfter]。
func WithInterruptAfter(nodes ...NodeID) RunOption {
	return func(c *runOptions) { c.interruptAfter = append(c.interruptAfter, nodes...) }
}

// WithInterruptBefore 声明 before 断点：执行到 nodes 中的节点（[AllNodes] = 每一个节点）之前停下，
// 不用改节点代码。用于调试、逐步执行、「到这儿先停一下让人看一眼」的闸门。可调用多次，取并集。
//
// # 停在哪儿
//
// 断点判在**超步边界**上：一个超步即将开始、它要执行的节点里有命中的，这个超步就**一个节点都不执行**
// （包括同一超步里不命中的兄弟——停下的必须是一个完整的边界，存档才代表一个确定的位置）。图的入口
// 那一批也判。
//
// 停下时 Run 返回 [*InterruptError]，Reason 是 [BreakpointInterruptReason]，Query.Breakpoint 写着
// 停在哪些节点之前 / 之后、哪一层图。配了 checkpointer 时存档留在可续跑的挂起档，frontier 就是这个
// 边界；**续跑就是放行**（带不带答案都行），恰好越过这一个边界，之后的边界照样判——循环回到同一个
// 节点时会再停，这正是逐步执行要的。顶层图没配 checkpointer 时，用 [WithResumeFrom]（停下那个
// [ChannelManager] 的 Snapshot()）续跑；嵌在别的节点里的图（子图、节点里跑的嵌套图）停在断点上必须有
// checkpointer，没有时报错而不是停（不然续跑从头再跑那张图、停在同一个断点，永远过不去）。
//
// 续跑恢复出来的边界：上次停在它的断点上的，续跑就是放行；这一超步已经开始执行过的（中断 / 失败之后
// 续跑），它的 before 早已过了、不再判；还没开始执行的（软取消、ctx 取消停在边界上、崩溃恢复）照常判这次
// Run 的 before 断点。after 断点只在超步刚整体提交的那一刻判。
//
// # 跟同一边界上别的停止的先后
//
// 断点紧跟着边界算出来就判，排在这个边界上的软取消、ctx 取消、durability 中途写档之前：停在断点上的
// 档带着这条断点挂起，从别的停止留下的档续跑时断点也还在（见上一节）。
//
// # 它不是什么
//
// 断点只说「停在这儿」，不带要人回答的问题，也不能在停下时改状态再继续。要人看着具体内容批准或拒绝某个
// 动作，用节点里的 [Ask]（或 BasicAgent 的工具审批）。闸门要拦下不走时，调用方不续跑即可（agent_v2 的
// Resume 可以 Abort）。
//
// 节点名不在这张图里时 Compile 报错。子图（[AddSubgraphNode] 嵌进来的图）的断点随子图一起生效，在
// 父图里跑时照样停。
func (b *GraphBuilder) WithInterruptBefore(nodes ...NodeID) *GraphBuilder {
	b.interruptBefore = append(b.interruptBefore, nodes...)
	return b
}

// WithInterruptAfter 声明 after 断点：nodes 中的节点（[AllNodes] = 每一个节点）所在的超步**整体提交
// 之后**停下——它们的写入已经生效、下一个超步还没开始。其余同 [GraphBuilder.WithInterruptBefore]。
//
// 几条要知道的：
//
//   - 同一个边界上 after 与 before 都命中时只停一次。
//   - 图在那个超步之后就结束了的话不停（后面没有可以停在前面的东西了，结果就是 Run 的返回）。
//   - 同一超步里有兄弟中断 / 失败时，这个超步没有整体提交，after 断点等续跑把它补完的那一刻才停。
func (b *GraphBuilder) WithInterruptAfter(nodes ...NodeID) *GraphBuilder {
	b.interruptAfter = append(b.interruptAfter, nodes...)
	return b
}

// breakpointStop 判一个超步边界上该不该停：before 看这个边界要启动的 next，after 看刚整体提交的那个
// 超步的成员 done（入口那一批没有 done）。该停时把这一问记进挂起清单并返回中断；不停返回 nil。
//
// 在哪些边界上调：新开路径的入口、每次整体推进之后，以及续跑恢复出来、**还没开始执行**、上次也不是
// 停在它的断点上的那个边界（软取消 / ctx 取消停在边界上、崩溃恢复——那时候没判过这次 Run 的断点）。
// 恢复出来的边界若是停在它的断点上的那一个，续跑就是放行；若已经开始执行过（中断 / 失败之后续跑），
// 它的 before 早已过了，不再判（否则答完问题续跑又停在同一个超步前面）。
//
// unresumable：这张图嵌在别的节点里跑、又没有 checkpointer——停下之后没有存档可续，续跑会从头再跑
// 这张图、停在同一个断点，永远过不去（审计实测，节点副作用每续一次重放一次）。这时报错而不是停。
func breakpointStop(cm *ChannelManager, graphNS string, before, after breakpointSet, next, done []FrontierMember, unresumable bool) error {
	b := before.match(next)
	a := after.match(done)
	if len(b) == 0 && len(a) == 0 {
		return nil
	}
	if unresumable {
		return &errInvalidBreakpoint{msg: fmt.Sprintf("compose_v2: ns %q 上的断点（before=%v after=%v）停不下来："+
			"这张图嵌在别的节点里跑、又没有配 checkpointer，停下之后续跑会从头再跑这张图、又停在同一个断点。"+
			"给整棵图配上 checkpointer（WithCheckpointer + WithSessionID，或经 ctx），或者把断点设在顶层图上", graphNS, b, a)}
	}
	q := &AskQuery{
		InterruptID: breakpointInterruptID(cm, graphNS, next, done, b, a),
		Question:    breakpointQuestion(b, a),
		Reason:      BreakpointInterruptReason,
		Breakpoint:  &Breakpoint{Before: b, After: a, NS: graphNS},
	}
	if err := cm.recordInterruptBatch([]*AskQuery{q}); err != nil {
		return err
	}
	return &InterruptError{Reason: BreakpointInterruptReason, Query: q}
}

// breakpointInterruptID 给一个断点停下的边界一个稳定、在这个任务里唯一的 ID：任务代号 + ns + 边界上的
// 成员（实例身份与此刻的访问次数）+ 命中的节点。访问次数只在节点成功时递增、实例身份每次投递新铸，
// 所以同一个任务里两个不同的边界算不出同一个 ID；ID 只在停下时算一次、随挂起清单进存档，续跑不重算。
func breakpointInterruptID(cm *ChannelManager, graphNS string, next, done []FrontierMember, before, after []NodeID) string {
	h := fnv.New64a()
	w := func(s string) {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}
	w("breakpoint")
	w(cm.TaskID())
	w(graphNS)
	members := func(tag string, ms []FrontierMember) {
		w(tag)
		for _, m := range ms {
			w(string(m.NodeID))
			w(m.InstanceID)
			w(strconv.Itoa(cm.GetVisitCount(m.NodeID)))
		}
	}
	members("next", next)
	members("done", done)
	w("before:" + joinNodeIDs(before, ","))
	w("after:" + joinNodeIDs(after, ","))
	return fmt.Sprintf("%016x", h.Sum64())
}

func breakpointQuestion(before, after []NodeID) string {
	var parts []string
	if len(after) > 0 {
		parts = append(parts, "节点 "+joinNodeIDs(after, "、")+" 已执行完")
	}
	if len(before) > 0 {
		parts = append(parts, "即将执行节点 "+joinNodeIDs(before, "、"))
	}
	return "已在断点处暂停（" + strings.Join(parts, "；") + "），续跑即继续。"
}

// hasBreakpointStop 报挂起清单里有没有断点停下的那一条（续跑就是放行它）。
func hasBreakpointStop(qs []*AskQuery) bool {
	for _, q := range qs {
		if q != nil && q.Breakpoint != nil {
			return true
		}
	}
	return false
}

func joinNodeIDs(ids []NodeID, sep string) string {
	ss := make([]string, len(ids))
	for i, id := range ids {
		ss[i] = string(id)
	}
	return strings.Join(ss, sep)
}
