package compose_v2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// NodeID 是图节点的唯一标识。
type NodeID string

// 约定常量。
//
// **设计决策**（针对 audit F6 EndChannel vs EndNode 关系不清）：
// 删除 EndChannel 概念——graph 终止统一通过出边路由到 EndNode 表达。
// channel 只承担 state 合并职责，不参与控制流。
//
// 理由：
//   - 拓扑边表达终止比"写 channel 即终止"更直观（看 graph 结构即懂）
//   - 不引入两套终止机制（避免 audit F6 的"该写 channel 还是路由 EndNode"困惑）
//   - 用专门的 EndNode 节点表达终止，控制流统一走拓扑边路由
const (
	StartNode NodeID = "__start__"
	EndNode   NodeID = "__end__"
)

// NodeFunc 是节点函数。
//
// 输入：ctx + channels 只读视图
// 输出：[]ChannelWrite（在超步合并点统一 ApplyAll）+ error（业务错走 graphLoop 错误处理）
//
// **关键不变量**：
//   - NodeFunc 不直接 mutate state——所有状态变更通过 ChannelWrite 表达
//   - 错误返回时 ChannelWrite 全部丢弃（任务失败则其待写入全部不 commit）
//   - panic 由框架 recover 转 error（PoC Step 4 加 safeRunNode 等价物）
type NodeFunc func(ctx context.Context, chs Channels) ([]ChannelWrite, error)

// EdgeCondition 是条件边函数：读 channel 决定下一节点。
// 返回空 NodeID 等价于路由到 EndNode（当前分支正常结束）。
//
// **cond 是纯判断**：读 channel state → 返目标节点，不产 writes、不改 state、**不能 interrupt**
// （要 HITL 请在 node 内抛 InterruptError，后接 cond 读 channel 路由——见 [evalOutEdges]
// 的 cond 分支）。cond 报错时的 Resume 语义由 CondFailPolicy 决定（业务经 WithCondFailPolicy 声明）。
//
// **ctx 只用来透传、不做控制流判断**：cond 收 ctx 是为了签名统一 + 传给 observability，
// **不该**在里面读 ctx.Err() 并据此返回错误。cond 是纯判断、不做 IO，本就产不出 ctx 错；
// 取消由引擎在**超步边界统一拦截**（graphLoop 循环顶查 ctx.Err），轮不到 cond 参与。
//
// 真这么写的后果自负：它跟任何业务错一样走 CondFailPolicy，默认档 CondFailAbort 判
// **终局失败**（wrap ErrRunAborted → Run 出口主动清 cp 子树）。想要"报错后能 Resume"
// 直接声明 CondFailRerunFrom——policy 已覆盖这个需求，框架不为违反本契约的写法另开豁免。
type EdgeCondition func(ctx context.Context, chs Channels) (NodeID, error)

// CondFailPolicy 定义 cond fn **报错**（业务错 / ctx.Err / panic-转-error）时的 Resume 语义。
//
// **为什么需要业务来选**：cond 是纯判断，它报错只可能是「判断的前提不成立」——上游 node
// 产出的数据缺失或非法。该重试还是该放弃，取决于**上游 node 是不是确定性的**，而这只有
// 业务知道：上游若是纯计算，重跑必然拿到同样的坏数据（白跑）；上游若调 LLM（planner /
// classifier），重跑很可能就拿到好数据了（值得）。框架无从判断，故交给业务声明。
type CondFailPolicy int

const (
	// CondFailAbort（**默认**）：cond 报错 = 终局失败。wrap `ErrRunAborted` → Run 出口
	// defer 据此**主动 DeleteSubtree 清 cp 子树** → 业务只能修完问题重新跑。
	//
	// **为什么是「主动删」而不是「跳过写」**：cp 是 (sid, ns) **单槽覆盖式**，跳过写只是
	// 不覆盖——先前的 cp（HITL 中断存的档 / durability 中途写的 cp）会**原样幸存且
	// Status=Pending** → 下次 auto-load 照样捡起 → 重跑已成功节点 / 重问已答过的问题 /
	// 子图 livelock。详见 ErrRunAborted godoc。
	//
	// 为什么作默认：cond 报错多是**结构性**问题（上游数据非法/缺失），确定性居多，重跑
	// 只会再烧一次上游、拿同样的错。默认 fail-fast 比默认给业务一个「重试能好」的假象诚实
	//（对齐项目 fail-fast 规约）。
	CondFailAbort CondFailPolicy = iota

	// CondFailRerunFrom：cond 报错 → 只重跑**算不出出边的那个成员** + 存 Pending cp，
	// 业务 Resume 让它重新产出数据、cond 重评。
	//
	// 适用：from 是**非确定性**节点（调 LLM 的 planner / classifier 等），重跑很可能拿到
	// 好数据。代价是再跑一次 from（含其 LLM 调用）。
	//
	// **多实例下的精度**：from 若是 SendFrom 派生的 N 个并发实例，每个实例**各算各的出边**
	// （视图只含它自己的写入），所以归因是精确的——只有 cond 报错的那个实例重跑，成功的
	// 兄弟照常提交写入、记入 completed、**永不重跑**。
	//
	// 这里曾经是「该 NodeID 的**全部** N 个实例一起重跑」，因为那时 cond 排在提交之后、
	// 读的是合并后的 channel，无从归因是哪个实例的产出有问题；代价是已成功实例的副作用
	//（LLM 调用 / 外部 API）跟着再跑一遍。出边归成员之后那个放大消失了。
	//
	// **业务义务**：失败的那个成员会被**整个重跑**（NodeFunc 从头跑一遍），但它这一轮的
	// writes **没有提交过**（出边算不出来 ⟹ 它不算成功 ⟹ 写入不进 channel），所以对
	// Append / Sum 型 channel **不再双计**——那条「须自行保证幂等」的义务随之解除。
	// 节点内已经做过的**外部副作用**（下单 / 扣款）另说：用 [SaveNodeState] 记进度，
	// 重跑时读回来跳过（键含代际，这一步没成 ⟹ 键不漂 ⟹ 读得回）。
	//
	// **产出**则相反，本档要的就是它重新产出：节点里按 [Rerunnable] 记下的步骤在本档**否决**时
	// 作废、重跑时重新做。经 `agent_v2.RunSubAgent` 调的 planner / classifier 要在那次调用上声明
	// `agent_v2.WithRerunnable`（完成记录默认是普通记录——同一个节点里的扣款、问过人的把关不该
	// 跟着重做）；节点自己直接调模型、把结果用 [SaveStepResult] 记住的，标 [Rerunnable]。不点名
	// 的话重跑原样拿回被否决的那份、续跑多少次都是同一个 cond 错。cond 被外面停下（软取消、
	// 陈旧写回、调用方 ctx 已经死了——错误链上有没有 ctx 错都一样）不算否决，那时记录照常拿回。
	//
	// **否决之后重做时问的是新问题**：重做里在同一个位置提的问题换一个 ID（见 [InterruptSite] 的
	// RerunGen；节点里跑的嵌套图里的提问见 OuterRerunGens），调用方累积传的旧答案不会兑付给它。
	//
	// **入口条件边**（从 StartNode 出发）：嵌套图的入口否决时，「上游」是产出这张图输入的那个分域，
	// 等外层成员以这个错失败时推进它的重做代际（错被业务吞掉、被重试消化、成员最终成功就不推进）：
	//
	//   - 节点体 / 某个 tool_call / 某个钩子相位里直接跑的图：就是跑它的那个分域；
	//   - 经 `agent_v2.RunSubAgent`、AgentAsTool 调起的 agent 的主图：是**调用方**那个分域（输入是
	//     调用方给的）——调用方声明了 `agent_v2.WithRerunnable` 的产出随之作废、重跑时重新做；
	//   - [AddSubgraphNode] 的子图、编排成员的主图：宿主只是转交输入，真正产出它的上游节点已经
	//     提交、不会因此重跑——判官是确定性的话续跑永不收敛。校验放在产出那个节点自己的出边上。
	//
	// ⚠️ 用 [RunWithRetry] 包住那次嵌套运行时，否决不在重试之间推进：判官按 [Rerunnable] 记下的判断
	// 依据（经 RunSubAgent 调的 router）在每次尝试里原样拿回，重试改不了判；让错冒到成员失败、
	// 续跑时才重做。**顶层图的入口没有上游可重跑**：此刻一个节点都没执行过、什么也没提交，本档在
	// 那里**不存档**，调用方直接重跑这次 Run 即可（等价于续跑）。
	CondFailRerunFrom
)

// CondOption 配置 AddConditionalEdge 注册的条件边。
type CondOption func(*condConfig)

type condConfig struct {
	failPolicy CondFailPolicy
}

// WithCondFailPolicy 声明该条件边 cond 报错时的 Resume 语义（默认 CondFailAbort）。
//
//	// 上游是 LLM planner，报错时希望能 Resume 重跑让它重新出 plan
//	b.AddConditionalEdge("plan", planCond, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
//
// **不影响 cond 抛 InterruptError 的处理**——那是协议违规（cond 禁止 interrupt），恒按
// Abort 拒绝（重跑同样违规，给 Resume 无意义）。
func WithCondFailPolicy(p CondFailPolicy) CondOption {
	return func(c *condConfig) { c.failPolicy = p }
}

// edge 是图中一条边的内部表示。
type edge struct {
	from NodeID
	to   NodeID        // 静态边目标；条件边时此字段忽略
	cond EdgeCondition // 非 nil 表示条件边，cond 决定 to
	// condFailPolicy cond 报错时的 Resume 语义（仅条件边有意义）。零值 = CondFailAbort。
	condFailPolicy CondFailPolicy
}

// Graph 是编译后的不可变图。
type Graph struct {
	nodes map[NodeID]NodeFunc
	// nodeComponents Phase 2: node 声明的组件类型 (retrieval/embedding/guardrail).
	// graph 执行引擎在 fire OnNodeStart 前据此 WithComponentKind push ctx, observability 把
	// node span operation.name 标对应 kind (GraphAgent 显式编排路径让组件调用在 trace 区分).
	// 无声明的 node 不在 map 内 (读返 "" → 默认 workflow node kind).
	nodeComponents map[NodeID]string
	adj            map[NodeID][]edge // node 出边
	channels       []anyChannel      // 编译期注册的所有 channel
	maxVisit       int
	maxWidth       int

	// ── ISSUE-50 P0 防串台：Graph 身份字段（Phase 5.1.b 引入） ──
	name     string // 业务可选 WithName 设置，空时 ID() 走 fingerprint 兜底
	cachedID string // ID() 首次调用后缓存（fingerprint 计算昂贵，跨 Run 不变）
}

// ID 返回 Graph 身份字符串（ISSUE-50 P0 防串台）。
//
// 优先级：
//  1. 显式 WithName(...) 设置的 name —— 业务推荐，可读性高、跨进程稳定
//  2. fingerprint 兜底：sha256(sorted(nodeIDs) || sorted(channelName:type)) 前 16 hex chars
//
// 为何需要：Checkpointer 按 (sessionID, ns) 二级 key 存 cp；两个不同 Graph 共享同
// (sessionID, ns) 时旧 cp 被新 graph 覆盖、Resume 加载到"别人"的存档但 frontier
// NodeID / channel name 对不上 → typed channel value 串台污染。本 ID 配合
// Checkpoint.GraphID 字段 + Run 入口 fail-fast 校验，把"静默错数据"问题挡在
// Resume 入口。
//
// fingerprint 兜底设计：
//   - sorted nodeIDs 保证拓扑相同的 graph 同 ID（节点 ID 改一个就变）
//   - sorted channel(name + reflect.Type) 保证 channel schema 变 → ID 变（避免
//     "同名 channel 不同 type" 串台，如 LastValue[string] vs Append[int]）
//   - 16 hex = 64 bit collision 空间 ~10^19，业务规模够用
//   - 缓存到 cachedID，跨 Run 不重算
//
// **fingerprint 限制（必读）**：
//  1. **不区分 NodeFunc 行为** —— 同 nodeID 不同 NodeFunc 实现（如一个调 LLM
//     一个调 print）→ fingerprint 相同 → cp 串台风险未防住
//  2. **不哈希 edges 拓扑** —— 仅哈希 nodeIDs + channels；边拓扑变（如分叉/合并
//     改变）但节点 ID 集合 + channel 集合不变 → fingerprint 不变
//
// 这两类"形似神不同"的 graph 必须 `WithName(...)` 显式声明唯一身份，**不要依赖
// fingerprint 兜底**。fingerprint 仅作"业务忘配 WithName 时的最小防御"。
func (g *Graph) ID() string {
	if g.name != "" {
		return g.name
	}
	if g.cachedID != "" {
		return g.cachedID
	}
	// 收集 nodeIDs 排序拼接
	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)

	// 收集 channels（name + type）排序拼接——schema 一致性
	chDescs := make([]string, 0, len(g.channels))
	for _, ch := range g.channels {
		chDescs = append(chDescs, ch.name()+":"+ch.typeRep().String())
	}
	sort.Strings(chDescs)

	h := sha256.Sum256([]byte(strings.Join(ids, ",") + "|" + strings.Join(chDescs, ",")))
	g.cachedID = hex.EncodeToString(h[:8]) // 16 hex chars = 8 bytes
	return g.cachedID
}

// Compile 把 builder 编译为不可变 Graph。
func (b *GraphBuilder) Compile() (*Graph, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	g := &Graph{
		nodes:          make(map[NodeID]NodeFunc, len(b.nodes)),
		nodeComponents: make(map[NodeID]string, len(b.nodeComponents)),
		adj:            make(map[NodeID][]edge, len(b.nodes)+1),
		channels:       append([]anyChannel(nil), b.channels...),
		maxVisit:       b.maxVisit,
		maxWidth:       b.maxWidth,
		name:           b.name, // ISSUE-50 P0 防串台
	}
	if g.maxVisit <= 0 {
		g.maxVisit = 100 // PoC 默认值
	}
	if g.maxWidth <= 0 {
		g.maxWidth = 10000 // 见 WithMaxFrontierWidth
	}
	for id, fn := range b.nodes {
		g.nodes[id] = fn
	}
	for id, comp := range b.nodeComponents {
		g.nodeComponents[id] = comp
	}
	for _, e := range b.edges {
		g.adj[e.from] = append(g.adj[e.from], e)
	}
	// 编译期（单线程）预热 cachedID，兑现 Compile 的"不可变 Graph"承诺。
	// 否则 ID() 的惰性 fingerprint 写 g.cachedID 无锁——GraphAgent 长持单个
	// *Graph 跨 Run 共享，业务未 WithName 时两并发 Run + interrupt 同时走
	// SaveNS defer 的 g.ID() 会并发读写 cachedID（-race 实证 graph.go DATA
	// RACE）。name 非空时 ID() 直接返 name 不碰 cachedID，无需预热。
	if g.name == "" {
		_ = g.ID()
	}
	return g, nil
}

// GraphBuilder 构造 Graph。
type GraphBuilder struct {
	nodes          map[NodeID]NodeFunc
	nodeComponents map[NodeID]string // Phase 2: node→组件类型声明 (见 Graph.nodeComponents)
	edges          []edge
	channels       []anyChannel
	maxVisit       int
	maxWidth       int
	name           string // ISSUE-50 P0：业务可选 WithName 显式声明 graph 身份
}

// NewBuilder 创建空 GraphBuilder。
func NewBuilder() *GraphBuilder {
	return &GraphBuilder{
		nodes:          make(map[NodeID]NodeFunc),
		nodeComponents: make(map[NodeID]string),
	}
}

// NodeOption 配置 AddNode 注册的节点 (Phase 2: 组件类型标注).
type NodeOption func(*nodeConfig)

type nodeConfig struct {
	component string
}

// WithNodeComponent 声明该 node 的组件类型 (retrieval/embedding/guardrail),
// observability 据此把 node span 的 operation.name 标对应 kind — GraphAgent 显式编排路径
// 让业务在 NodeFunc 里直接调的组件 (不经 LLM tool_call) 也能在 trace 区分.
//
// 非法值由 observability 校验自动 fallback 到默认 workflow node kind (宽松语义, 跟
// tool.Categorizer 一致). compose_v2 只透传 string 不耦合 observability 的合法值定义.
//
//	bld.AddNode("retrieve", retrieveFn, compose_v2.WithNodeComponent("retrieval"))
func WithNodeComponent(kind string) NodeOption {
	return func(c *nodeConfig) { c.component = kind }
}

// AddNode 注册节点。可选 NodeOption 声明组件类型 (WithNodeComponent).
//
// **audit⁷ HIGH-2 fix (audit¹² 补漏 — audit⁷ commit 23eebe4 谎报 5 处实改 3 处)**:
// isFrameworkNil 检 typed nil func (协议七复证). 业务 `var fn NodeFunc; AddNode(id, fn)`
// typed nil → graph_loop run 时远 stack nil deref.
func (b *GraphBuilder) AddNode(id NodeID, fn NodeFunc, opts ...NodeOption) *GraphBuilder {
	if isFrameworkNil(fn) {
		panic("GraphBuilder.AddNode: nil fn (含 Go typed nil func 陷阱)")
	}
	b.nodes[id] = fn
	if len(opts) > 0 {
		var cfg nodeConfig
		for _, o := range opts {
			o(&cfg)
		}
		if cfg.component != "" {
			b.nodeComponents[id] = cfg.component
		}
	}
	return b
}

// AddEdge 加静态边 from → to。
//
// # 多前驱节点是 any-of 触发，不是 all-of
//
// 同一个 to 有多条入边时，**每个前驱各自算出边、各自把 to 投进下一帧**（evalOutEdges →
// assembleFrontier，后者只对**本超步**的目标去重、没有「等齐所有前驱」的账本）。于是：
//
//   - 前驱落在**同一超步**（START→A→C、START→B→C）：C 跑一次——去重塌成一次，这就是
//     「超步天然 barrier」的全部含义；ParallelAgent 的 finalize 节点靠的正是它（N 个
//     worker 由一次 SendFrom 投出、恒同超步）。
//   - 前驱**深度不等**（START→A→C、START→B→B2→C）：C 跑**两次**，首跑只看得到 A 的写入，
//     且 err == nil——实测 order=B,A,B2,C,C。join 节点若在首跑就调 LLM / 产生副作用，
//     那一次是拿半份数据跑的。
//
// 要 all-of（「等两个前驱都到了再跑一次」）今天没有声明式写法，两条路：把前驱重排到同一
// 超步；或手写 barrier——前驱各 Write(Sum, 1)，join 节点读到 < N 时不干活、用条件边路由到
// EndNode 空转一趟，== N 才真正执行（代价一次多余 visit：hook 照 fire、ns 的 @visit 维照
// tick）。声明式 AND-join / 收尾节点（defer）列在 doc/code/todo.md，按需做。
func (b *GraphBuilder) AddEdge(from, to NodeID) *GraphBuilder {
	b.edges = append(b.edges, edge{from: from, to: to})
	return b
}

// AddConditionalEdge 加条件边——cond 函数返回的 NodeID 即下一节点。
//
// 可选 CondOption 声明 cond **报错**时的 Resume 语义（WithCondFailPolicy，默认
// CondFailAbort = 终局失败、主动清 cp 子树）。cond 抛 InterruptError 是协议违规，恒被拒绝
// （不受 policy 影响）——HITL 请在 node 内抛。
//
// # cond 跑在哪、看得见什么（三条业务必须知道的）
//
//  1. **它跑在产出这条出边的那个成员的执行里**，排在任何提交之前。它报错就是**那个成员
//     执行失败**：写入不提交、代际不推进、只有它自己重跑，兄弟照常提交并记入 completed。
//
//  2. **它看不到同超步兄弟成员的写入**——视图是「已提交态 + 本成员自己那批还没提交的
//     写入」。要「看合并后的全貌再决定去哪」，做法是**加一个 join 节点、把条件边挂到它
//     身上**：那时全貌已经是上一超步的提交态了。
//
//  3. **它可能被并发调用**：`SendFrom` 同 Target 投 N 次时该节点有 N 个并发实例，每个
//     实例**各算各的出边**（各看各的写入），于是 cond 在 N 个 goroutine 里同时跑。
//     cond 本就要求是纯判断（不产 writes、不改 state），照此写就无需额外同步；若闭包
//     里持有共享状态（计数器之类），**业务自负 thread-safe**。
//
// 前两条合起来是「路由属于成员」这一个决定的两面：正因为可见范围限死到本成员，出边才
// 能排在提交之前；也正因为排在提交之前，才不存在「成员已算成功、写入已提交、代际已推进，
// 却还得回去重跑」这个半成功状态（中断身份漂移 / 节点私有态键位错配 / 写入双计的共同根）。
//
// **audit⁷ HIGH-3 fix (audit¹² 补漏)**: isFrameworkNil 检 typed nil cond (协议七复证).
func (b *GraphBuilder) AddConditionalEdge(from NodeID, cond EdgeCondition, opts ...CondOption) *GraphBuilder {
	if isFrameworkNil(cond) {
		panic("GraphBuilder.AddConditionalEdge: nil cond (含 Go typed nil func 陷阱)")
	}
	var cfg condConfig // 零值 = CondFailAbort
	for _, o := range opts {
		o(&cfg)
	}
	b.edges = append(b.edges, edge{from: from, cond: cond, condFailPolicy: cfg.failPolicy})
	return b
}

// WithChannel 注册 channel。返回的 builder 可链式调用。
//
// **设计决策**（针对 audit F9 builder 类型擦除卖点打折）：
// 用包级泛型函数 WithChannel[T]（不是方法）——绕开 Go 泛型方法不能引入新类型
// 参数的限制，调用点编译期 T 推断，IDE 重命名安全。
func WithChannel[T any](b *GraphBuilder, ch *Channel[T]) *GraphBuilder {
	// audit¹³ HIGH-1 fix: nil 检 — ch nil → ch.asAny() nil deref panic 难定位.
	if b == nil {
		panic("WithChannel: nil GraphBuilder")
	}
	if ch == nil {
		panic("WithChannel: nil Channel[T] (含 Go typed nil pointer 陷阱)")
	}
	b.channels = append(b.channels, ch.asAny())
	return b
}

// WithMaxVisit 设单节点最大访问**轮数**（防死循环）。
//
// 计的是**深度**：一个超步里同一节点有多少并发实例不计入（fan-out 宽度由业务的
// SendFrom 决定，不归它管）。宽度的兜底是 WithMaxFrontierWidth。
func (b *GraphBuilder) WithMaxVisit(n int) *GraphBuilder {
	b.maxVisit = n
	return b
}

// WithMaxFrontierWidth 设单个超步的最大并发成员数（默认 10000）。
//
// **为什么宽度要单独兜底**：maxVisit 计的是深度、一个超步至多 +1，所以它拦不住宽度。
// 节点在自己的 NodeFunc 里 SendFrom 自己 N(≥2) 次时，宽度**逐超步相乘**——业务终止
// 条件写错就是 N^超步数 的指数爆炸，进程会在 MaxIterationsError 冒出来之前先 OOM
// （实测 N=2、maxVisit=6 时执行 63 次；默认 maxVisit=100 则是 2^100）。
//
// 默认 10000 对正常 map-reduce 足够宽松（一个分支通常对应一次 LLM 调用，上万并发已
// 远超合理编排规模），同时能在指数爆炸的第 14 个超步左右拦住它。
func (b *GraphBuilder) WithMaxFrontierWidth(n int) *GraphBuilder {
	b.maxWidth = n
	return b
}

// WithName 设置 Graph 身份名（ISSUE-50 P0 防串台）。
//
// 业务推荐显式调用——稳定、可读、跨进程不变。不调时框架按 fingerprint 兜底
// （`sha256(sorted(nodeIDs) | sorted(channels))` 前 16 hex），但 fingerprint 在
// graph 拓扑或 channel schema 变更时会变，跨进程 Resume 老 cp 会失败。
//
// 关联：Checkpoint.GraphID 持久化本字段，Run 入口 auto-load 加 fail-fast 校验
// `cp.GraphID != "" && cp.GraphID != g.ID()` → 防两个不同 Graph 共享同
// (sessionID, ns) 时 cp 互覆盖造成 channel value 串台污染。
//
// 同名不同 NodeFunc 行为的 graph 必须 WithName 显式声明唯一身份（fingerprint
// 不区分 NodeFunc 实现）。详见 Graph.ID() godoc。
func (b *GraphBuilder) WithName(name string) *GraphBuilder {
	b.name = name
	return b
}

// validate 编译期拓扑校验。
func (b *GraphBuilder) validate() error {
	if len(b.nodes) == 0 {
		return fmt.Errorf("graph has no nodes")
	}
	// audit Round 4 Bug 2 修复：所有 nodeID 必须通过 ns 命名规范校验——不含
	// "/"（父子分隔符）/ "#"（fan-out 实例）/ "@"（重访实例）/ 不为空，否则 ns 拼装后
	// SplitNS 错位 → cp 跨 ns 串台 / 子图寻址错乱
	for id := range b.nodes {
		if err := ValidateNodeIDForNS(string(id)); err != nil {
			return fmt.Errorf("invalid node ID %q: %w", id, err)
		}
		// 起止节点名不能当普通节点用：叫 "__end__" 的节点被当成图的终点、永远不执行；叫
		// "__start__" 的和图的起点混在一起，按拓扑不同表现为反复进入或写冲突。放在这里而不是
		// AddNode：AddSubgraphNode 直接写节点表，所有节点只在这里汇合。
		if id == StartNode || id == EndNode {
			return fmt.Errorf("invalid node ID %q: it is the graph's start or end node", id)
		}
	}
	hasStart := false
	for _, e := range b.edges {
		if e.from == StartNode {
			hasStart = true
		}
		// 静态边不能指向起点：StartNode 是入口 sentinel、不在节点表里，放过去会在运行时进
		// frontier 后炸在「节点未注册」上，存档里还带着这个跑不动的 NodeID。条件边的同类
		// 目标在运行时由 outEdges 拒绝。
		if e.cond == nil && e.to == StartNode {
			return fmt.Errorf("edge %q → %q: an edge cannot lead back to the start node", e.from, e.to)
		}
		// 静态边目标存在性检查（条件边目标运行时才知道）
		if e.cond == nil && e.to != EndNode && e.to != StartNode {
			if _, ok := b.nodes[e.to]; !ok {
				return fmt.Errorf("edge %q → %q: target node not found", e.from, e.to)
			}
		}
		if e.from != StartNode {
			if _, ok := b.nodes[e.from]; !ok {
				return fmt.Errorf("edge %q → %q: source node not found", e.from, e.to)
			}
		}
	}
	if !hasStart {
		return fmt.Errorf("graph has no edge from %s", StartNode)
	}
	return nil
}
