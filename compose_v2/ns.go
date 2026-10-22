package compose_v2

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ISSUE-70 Phase 2: NS 命名规范与解析。
//
// NS（namespace）是 Checkpointer 物理寻址的位置标识，与 sessionID 共同构成
// (sessionID, ns) 二元 key 定位 cp。设计原则：
//
//   - 业务侧不见 string ns —— 业务只见 typed Address（Phase 3 落地）
//   - ns = 静态拓扑位置 + **实例维**。位置只由框架按图拓扑派生；实例维有两种、按
//     成员类型互斥（见 FrontierMember godoc）："#idx"（SendFrom fan-out 实例，
//     ISSUE-96）与 "@visit"（静态/cond 成员的跨完成重访序数——父循环第 N 轮重进同
//     一个子图节点时，子图的中断身份必须与第 N-1 轮不同，否则撞上已兑付的答案位、
//     终局 ErrRunAborted（答案账本跨子图边界之前是旧答案被静默复用）；
//     visit 只在成功路径递增、随 cp 持久化，Resume 重算必然一致）。首轮（visit=0）
//     不加缀，与引入前的 ns 逐字相同；成功即清保证 ns 不随轮次堆积
//   - 单分隔符 "/"（父子）+ "#"（fan-out idx）+ "@"（重访序数），精简易解析
//
// NS 形态：
//
//   ""              根 graph
//   "<nodeID>"      子图（直接挂 root 下）
//   "a/b"           二层嵌套子图
//   "a#0"           fan-out 分支 0
//   "a/b#1"         嵌套子图内的 fan-out 分支 1
//   "a#0/inner"     fan-out 分支内的子图
//   "a@2"           静态成员 a 的第 3 次访问（visit=2；visit=0 不加缀即 "a"）
//   "a@1/inner"     重访实例内的子图
//
// 所有 nodeID 必须经 ValidateNodeIDForNS 校验，禁止含 "/"、"#" 或 "@" 字符——
// 否则 ns 解析会错位。GraphBuilder.Build 在拓扑校验阶段统一调用。

const (
	// nsSepParent 父子 ns 分隔符
	nsSepParent = "/"
	// nsSepFanout fan-out idx 分隔符（紧接 nodeID）
	nsSepFanout = "#"
	// nsSepVisit 重访序数分隔符（紧接 nodeID）。与 "#" 互斥：SendFrom 实例身份
	// fresh-mint 天然带代际（父重跑换新 token），不叠 visit；静态成员反之。
	nsSepVisit = "@"
)

// NSSegment.Kind 的取值。**别在别处写这几个字面量**——`parseSegment` 里那道
// 「visit=0 必须不加缀」的守卫就是按 kind 字符串匹配的，写错/写漂时它静默失效
// （独立审计实测：kind 改成 "revisit" 后 `"a@0"` 不再被拒，而 init/派生哨兵都放行）。
const (
	nsKindNode   = "node"
	nsKindFanout = "fanout"
	nsKindVisit  = "visit"
)

// nsSeparator 是一种 ns 段分隔符及其语义。
type nsSeparator struct {
	// ch 是分隔符字符本身。
	ch string
	// kind 是 [NSSegment.Kind] 里对应的名字。**空 = 层级分隔符**（`/`），
	// 非空 = 实例分隔符（`#`/`@`，表示「同一个 nodeID 的第 N 个实例」）。
	//
	// ⚠️ 这里曾另有一个 `instance bool` 字段，与 `kind != ""` **完全同义反复**——
	// 而当时得靠两条测试断言守着它俩一致。需要写测试来保证两个字段同步，本身就是
	// 它们该合成一个的证据；合并之后那个不变量从「测出来」变成「表示不出来」。
	kind string
}

// isInstance 报这是不是实例分隔符（[ParentNS] 与 [parseSegment] 只认这批；
// [ValidateNodeIDForNS] 认全部；[IsNSInSubtree] 一个都不认，只认层级分隔符）。
func (s nsSeparator) isInstance() bool { return s.kind != "" }

// nsSeparators 是 ns 段分隔符的**唯一定义**——消费者从这里派生，包内不再有第二份枚举。
//
// # 它防的是什么
//
// 这份数据此前散在 6 个函数里各枚举一遍，加第三种（重访段 `@`）时没有任何机械手段
// 提醒还有几处要改。同类系统上这个坑的代价有据可查：另一个 Pregel 类引擎加第三个维度
// 后父 ns 计算没跟上，父链静默写错、**线上跑了 13 个月**才被当成局部 bug 修掉。
//
// # 约束（都由 TestNSSeparatorsTable_WellFormed 校验）
//
// 每个分隔符必须是**单字节、可打印 ASCII**、彼此不重复；实例分隔符必须有唯一的 kind。
// 单字节不是洁癖：Go 侧的 `strings.ContainsAny` / `strings.LastIndexAny` 把串当**字符
// 集合**（实测 `strings.ContainsAny("a:b", "::") = true`），redis 的 Lua 用
// `string.sub(ns, n+1, n+1)` 取一个字符做表查——多字节两边一起静默退化。
//
// # 加新分隔符时改哪儿
//
// **实例分隔符**（`#`/`@` 那一档）：只改这张表 + 配套的 `nsSep*` 常量，消费者全部从表
// 派生（实测只加表、消费者一行不动即可跑通）。
//
// **层级分隔符**（`/`）：表管得住取并集的那个消费者（[ValidateNodeIDForNS]），但**做层级
// 切分的几处仍绑死在 `nsSepParent` 单个常量上**——[JoinNS] 的拼接、[SplitNS] 的
// `strings.Split`、[ParentNS] 的 `strings.LastIndex`、[IsNSInSubtree] 与它给 backend 导出的
// [NSSubtreeSeparator]。想加第二个层级分隔符得连它们一起改。
// 失败是**响亮**的（`TestNSSeparators_AllConsumersDerive` 会红），不是静默的。
//
// ⚠️ 别把这张表的价值写成「为将来加第四种做准备」——同类系统在 3 个角色上稳了 20 个月，
// 没有证据指向第四个。它真正防的是**刚加完第三种时某个消费者没跟上**。
var nsSeparators = []nsSeparator{
	{ch: nsSepParent},
	{ch: nsSepFanout, kind: nsKindFanout},
	{ch: nsSepVisit, kind: nsKindVisit},
}

// nsInstanceSeparatorCharsCache / nsAllSeparatorCharsCache 是从 [nsSeparators] 派生的
// 字符串形态，**包初始化时算一次**。
//
// ⚠️ **不要改回「每次调用现拼」**：`ValidateNodeIDForNS` 与 `ParentNS` 都在热路径上
// （节点入口可能调用），实测现拼让 `ValidateNodeIDForNS` 从 14ns 涨到 42ns（3 倍）。
// 表是包级不可变数据，派生值没有理由重算。
var (
	nsInstanceSeparatorCharsCache = buildSeparatorChars(true)
	nsAllSeparatorCharsCache      = buildSeparatorChars(false)
)

// buildSeparatorChars 把表里的分隔符字符拼成串——`instanceOnly` 为真时只取实例分隔符。
func buildSeparatorChars(instanceOnly bool) string {
	var b strings.Builder
	for _, sep := range nsSeparators {
		if instanceOnly && !sep.isInstance() {
			continue
		}
		b.WriteString(sep.ch)
	}
	return b.String()
}

// nsInstanceSeparatorChars 返回全部**实例**分隔符字符拼成的串——今天唯一的消费者是
// [ParentNS] 里的 `strings.LastIndexAny`。
func nsInstanceSeparatorChars() string { return nsInstanceSeparatorCharsCache }

// nsAllSeparatorChars 返回全部分隔符字符拼成的串。
func nsAllSeparatorChars() string { return nsAllSeparatorCharsCache }

// ErrInvalidNodeIDForNS 表示 nodeID 含 ns 命名规范禁止的字符。
var ErrInvalidNodeIDForNS = errors.New("compose: nodeID contains a forbidden ns segment separator")

// ErrInvalidNS 表示 ns 字符串无法解析（如以分隔符开头/结尾、连续分隔符）。
var ErrInvalidNS = errors.New("compose: invalid ns format")

// JoinNS 拼接 parent + child nodeID 形成嵌套 ns。
//
//	JoinNS("", "a")       = "a"
//	JoinNS("a", "b")      = "a/b"
//	JoinNS("a/b", "c")    = "a/b/c"
//
// 调用方负责保证 nodeID 已通过 ValidateNodeIDForNS 校验——本函数不重复校验，
// 性能敏感（每次节点入口可能调用）。
func JoinNS(parent, nodeID string) string {
	if nodeID == "" {
		return parent
	}
	if parent == "" {
		return nodeID
	}
	return parent + nsSepParent + nodeID
}

// JoinFanoutNS 拼接 fan-out 分支 ns。
//
//	JoinFanoutNS("", "worker", 0)         = "worker#0"
//	JoinFanoutNS("a", "worker", 2)        = "a/worker#2"
//	JoinFanoutNS("a/b", "worker", 5)      = "a/b/worker#5"
//
// 与 JoinNS 一样，nodeID 必须已校验过。
//
// index 是 int64：`agent_v2` 的嵌套调用拿身份哈希的 63 位当下标，跟平台字长无关——用 int
// 的话 32 位进程只装得下 31 位，两种字长的进程共用一个持久后端时算出的槽号不同、续跑找不到
// 存档。
func JoinFanoutNS(parent, branchNodeID string, index int64) string {
	base := JoinNS(parent, branchNodeID)
	return base + nsSepFanout + strconv.FormatInt(index, 10)
}

// JoinVisitNS 拼接静态/cond 成员的重访 ns——visit 是该节点在本图内**已成功完成的
// 访问次数**（首访 0），进入节点时框架经 [GetCurrentNodeVisit] 注入。
//
//	JoinVisitNS("", "sub", 0)    = "sub"      // 首轮不加缀，与 JoinNS 逐字相同
//	JoinVisitNS("a", "sub", 1)   = "a/sub@1"  // 父循环第 2 轮
//	JoinVisitNS("a", "sub", 2)   = "a/sub@2"
//
// # 为什么重访必须换 ns
//
// ns 是子图全部中断身份的位置根。父循环第 2 轮重进同一个子图节点时，子图内部
// 从头执行（fresh cm），所有内部维度（节点、visit、计数器）都与第 1 轮相同——
// ns 不变则第 2 轮的提问算出与第 1 轮**完全相同**的 InterruptID，撞上已兑付的答案位
// ⟹ 终局 `ErrRunAborted`（subgraph_revisit_identity_test.go 钉着这条；答案账本跨子图边界之前，
// 答案 map 沿 ctx 活整个 Run，第 2 轮的提问从未呈现给用户就被第 1 轮的旧答案静默满足）。
//
// # 跨 Resume 稳定性
//
// visit 只在成功路径递增、随 cp 持久化：某轮中断退出 ⟹ visit 未推进 ⟹ Resume
// 重进算出同一个 ns，子图找回自己的档；上一轮成功 ⟹ 其子档已按成功清掉 ⟹ 新
// ns 是干净槽。首轮不加缀对齐「visit=0 即引入前行为」，存量地址不变。
//
// # 与 fan-out 维互斥
//
// SendFrom 派生成员走 [JoinFanoutNS]——实例 token 每次投递 fresh-mint，代际天然
// 在身份里，不需要（也不能）叠 visit——`visitCount` 是 per-NodeID 共享计数器，只由
// **空身份**成员推进（守卫在 `advanceVisitsLocked`），fan-out 实例读它会读到静态兄弟推高的值。调用方按「有无实例身份」二选一。
func JoinVisitNS(parent, nodeID string, visit int) string {
	base := JoinNS(parent, nodeID)
	if visit <= 0 {
		return base
	}
	return base + nsSepVisit + strconv.Itoa(visit)
}

// NSSegment 是 ns 解析后的单段。
//
// Kind:
//   - "node"   普通子图节点（NodeID 有效，Index < 0）
//   - "fanout" fan-out 分支（NodeID 有效，Index >= 0）
//   - "visit"  静态成员重访实例（NodeID 有效，Index >= 1 为重访序数）
type NSSegment struct {
	NodeID string
	Kind   string // "node" / "fanout" / "visit"
	Index  int64  // 实例序数：fanout 下标或重访 visit（按 Kind 区分；"node" 时为 -1）
}

// NSSubtreeSeparator 返回划定子树的那个分隔符（`/`）——[IsNSInSubtree] 只认它。
//
// 给没法调 Go 函数的 backend 用：redis 把「枚举 + 过滤 + 删除」放进一段 Lua 在服务端
// 原子完成，只能复刻判据，分隔符从这里取、不在脚本里手写一份。能直接调 [IsNSInSubtree]
// 的 backend 就别用它（sql 的 `DeleteSubtree` 是先枚举再调权威函数）。
func NSSubtreeSeparator() string {
	return nsSepParent
}

// SplitNS 把 ns 字符串解析成段列表，error 不 panic。
//
//	SplitNS("")          = [], nil
//	SplitNS("a")         = [{a, node, -1}], nil
//	SplitNS("a/b")       = [{a, node, -1}, {b, node, -1}], nil
//	SplitNS("a#0")       = [{a, fanout, 0}], nil
//	SplitNS("a/b#1/c")   = [{a, node, -1}, {b, fanout, 1}, {c, node, -1}], nil
//	SplitNS("/a")        → ErrInvalidNS（不允许前导分隔符）
//	SplitNS("a/")        → ErrInvalidNS（不允许尾部分隔符）
//	SplitNS("a//b")      → ErrInvalidNS（不允许连续分隔符）
//	SplitNS("a#x")       → ErrInvalidNS（fanout idx 必须是合法整数）
//
// 生产里 skill fork 的递归检测用它按段比较、observability 用它取嵌套图的工作流名；存档清理的子树判定走
// [IsNSInSubtree]。
// 解析失败调用方应降级（如 "从 root 重跑"）而非 panic。
//
// ⚠️ v1 时代描述"ResumeRouting 祖先 prefix 推导", v2 已删 IsResumeTarget 自检
// helper (走 map ID 命中, 不依赖 SplitNS 做 Resume 路由).
func SplitNS(ns string) ([]NSSegment, error) {
	if ns == "" {
		return nil, nil
	}
	if strings.HasPrefix(ns, nsSepParent) || strings.HasSuffix(ns, nsSepParent) {
		return nil, fmt.Errorf("%w: leading/trailing %q in %q", ErrInvalidNS, nsSepParent, ns)
	}
	parts := strings.Split(ns, nsSepParent)
	out := make([]NSSegment, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			return nil, fmt.Errorf("%w: empty segment in %q", ErrInvalidNS, ns)
		}
		seg, err := parseSegment(p)
		if err != nil {
			return nil, fmt.Errorf("%w: %v in %q", ErrInvalidNS, err, ns)
		}
		out = append(out, seg)
	}
	return out, nil
}

// parseSegment 解析单段（无 "/"），可能含 fan-out 后缀 "#N" 或重访后缀 "@N"。
// 两种后缀按成员类型互斥（见 nsSepVisit 常量注释），同段双缀是非法形态。
func parseSegment(s string) (NSSegment, error) {
	// 实例分隔符从 nsSeparators 派生，不在这里另枚举一遍（见那张表的 godoc）。
	sep, at, kind := "", -1, ""
	for _, cand := range nsSeparators {
		if !cand.isInstance() {
			continue
		}
		i := strings.Index(s, cand.ch)
		if i < 0 {
			continue
		}
		if at >= 0 {
			// 同段带两种实例后缀 —— 互斥契约被破坏
			return NSSegment{}, fmt.Errorf("segment %q carries both %q and %q markers", s, sep, cand.ch)
		}
		sep, at, kind = cand.ch, i, cand.kind
	}
	if at < 0 {
		return NSSegment{NodeID: s, Kind: nsKindNode, Index: -1}, nil
	}
	if at == 0 {
		return NSSegment{}, fmt.Errorf("missing nodeID before %q", sep)
	}
	nodeID := s[:at]
	idxStr := s[at+1:]
	if idxStr == "" {
		return NSSegment{}, fmt.Errorf("missing index after %q", sep)
	}
	n, err := strconv.ParseInt(idxStr, 10, 64)
	if err != nil {
		return NSSegment{}, fmt.Errorf("invalid %s index %q: %v", kind, idxStr, err)
	}
	if n < 0 {
		return NSSegment{}, fmt.Errorf("negative %s index %d", kind, n)
	}
	if kind == nsKindVisit && n == 0 {
		// visit=0 不加缀（JoinVisitNS 契约），出现 "@0" 只能来自伪造/手拼
		return NSSegment{}, fmt.Errorf("visit index 0 must be unadorned (got %q)", s)
	}
	return NSSegment{NodeID: nodeID, Kind: kind, Index: n}, nil
}

// ValidateNodeIDForNS 校验 nodeID 不含 ns 命名规范禁止的字符。
//
// 拒绝条件：
//   - 包含 "/"（父子分隔符冲突）
//   - 包含 "#"（fan-out idx 分隔符冲突）
//   - 包含 "@"（重访序数分隔符冲突——业务节点叫 "sub@1" 会与节点 "sub" 第 2 次
//     访问的派生 ns 撞同一个槽）
//   - 空字符串（无 ns 段意义）
//
// GraphBuilder.Build 阶段统一调用所有节点 ID。框架内部生成的 nodeID
// （如 "branch:%s#%d" 的 %s）也必须 escape 后再用——nodeID 含特殊字符
// 在 ns 拼接后会让 SplitNS 错位。
func ValidateNodeIDForNS(nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("%w: empty nodeID", ErrInvalidNodeIDForNS)
	}
	if strings.ContainsAny(nodeID, nsAllSeparatorChars()) {
		return fmt.Errorf("%w: nodeID=%q contains forbidden char", ErrInvalidNodeIDForNS, nodeID)
	}
	return nil
}

// IsNSInSubtree 判断 ns 是否属于 root 这次执行的子树：root 自身，或经 `/` 往下的后代。
// root 为空串表示整个 session。`Checkpointer.DeleteSubtree` 删的就是这个范围。
//
//	IsNSInSubtree("", "a#0")       = true（整个 session）
//	IsNSInSubtree("a", "a")        = true
//	IsNSInSubtree("a", "a/b")      = true
//	IsNSInSubtree("a", "a/b#1/c")  = true
//	IsNSInSubtree("a#0", "a#0/b")  = true
//	IsNSInSubtree("a", "a#0")      = false（同一个节点的另一次执行）
//	IsNSInSubtree("a", "a@1")      = false（同上）
//	IsNSInSubtree("a", "ab")       = false（裸字符前缀不算）
//	IsNSInSubtree("a/b", "a")      = false
//
// # `#` 与 `@` 段不算进来
//
// `a#0`（fan-out 实例）、`a@1`（重访实例）与 `a` 是**同一个节点的不同执行**：各有各的
// 存档，各自在成功或终局失败时清自己的子树。[ParentNS] 把它们挂在 `a` 下面，那是为了
// 按节点归组，不是所有权。
//
// 按归组删的话，一次执行的清理会连带删掉兄弟执行还挂着的存档——同一次节点执行里对
// 同一个名字调两次，第一次成功时把第二次的挂起档删了（实测：模型每次换 tool_call ID 时
// 永不收敛）；子图节点同一超步既有静态成员又有 SendFrom 实例时，静态成员成功把实例的
// 挂起档删了。框架的每一处清理要的都是「删我这次执行留下的东西」：Run 成功或终局失败时
// 清自己的 ns、子图节点成功时清自己那次子图运行、`Resume` 取消时清自己那层；清整个任务
// 走 root 为空串。
func IsNSInSubtree(root, ns string) bool {
	if root == "" || ns == root {
		return true
	}
	return strings.HasPrefix(ns, root+nsSepParent)
}

// ParentNS 返回 ns 的父 ns。
//
// 语义：fan-out 分支的 parent 是 fan-out 节点本身，普通子图节点的 parent
// 是上一层 graph ns。
//
//	ParentNS("")            = ""        // 根
//	ParentNS("a")           = ""        // 根级节点
//	ParentNS("a/b")         = "a"       // 普通子图，剥 /b
//	ParentNS("a#0")         = "a"       // 根级 fan-out 分支，剥 #0
//	ParentNS("a@1")         = "a"       // 重访实例，剥 @1（重访实例挂在节点本身之下）
//	ParentNS("a/b#1")       = "a/b"     // 嵌套 fan-out 分支，剥 #1（不剥 /b）
//	ParentNS("a/b#1/c")     = "a/b#1"   // fan-out 分支内子图，剥 /c
//	ParentNS("a/b/c")       = "a/b"     // 三层子图，剥 /c
//
// 用于 Checkpoint.ParentNS 字段自动填充。⚠️ 它是**归组**关系，
// 不是所有权：`a#0` 挂在 `a` 下面，但不属于 `a` 那次执行的子树（见 [IsNSInSubtree]）。
func ParentNS(ns string) string {
	if ns == "" {
		return ""
	}
	// 找最后一段（最后一个 / 之后）
	lastSlash := strings.LastIndex(ns, nsSepParent)
	lastSegStart := lastSlash + 1 // ns[lastSegStart:] 是末段
	lastSeg := ns[lastSegStart:]

	// 末段含 # / @ → 实例段（两者互斥，至多一个），parent 是去后缀的节点本身
	if markIdx := strings.LastIndexAny(lastSeg, nsInstanceSeparatorChars()); markIdx >= 0 {
		return ns[:lastSegStart+markIdx]
	}
	// 末段无实例后缀 → 普通子图节点，parent 剥末段
	if lastSlash >= 0 {
		return ns[:lastSlash]
	}
	// 根级普通节点 → parent 是 ""
	return ""
}
