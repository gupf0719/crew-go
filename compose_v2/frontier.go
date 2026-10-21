package compose_v2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"hash/fnv"
	"math"
	"strconv"
	"sync/atomic"
)

// FrontierMember 是超步 frontier 的一个成员——**一个待执行的节点实例**。
//
// # 为什么 frontier 不能只是 []NodeID（ISSUE-96 根因）
//
// 同一个 NodeID 可以有 **N 个并发实例**：`SendFrom` 同 Target 投递多次时
// `nextFrontier` 逐条 append **不去重**（by-design，`SendDirective` godoc 的头号示例
// 就是 N 个并发 worker）。而静态边 / cond 边走 `evalOutEdges` / `assembleFrontier` 的去重集合
// （`map[NodeID]struct{}`）→ 同 NodeID 恒 1 份。
//
// 只有 NodeID 时，框架无法区分这 N 个实例。此前靠 **branchIdx（实例在 frontier 里的
// 位置）** 当身份——但部分失败时 `updateFrontier(res.FailedBranches)` 把 frontier
// **缩成失败子集**，位置随即重排（{1,2}→{0,1}）。已实证 6 个静默丢数据的侧面，
// 详见 `doc/issue/issue-96-branch-instance-identity.md`。
//
// # InstanceID 的语义
//
//   - **静态边 / cond 边成员**：空串。它们恒单实例，NodeID 本身就够唯一。
//     其"跨完成访问的代际"由 `visit`（`cm.visitCount`）承担。**该计数器只由空身份
//     成员推进**（守卫在 `advanceVisitsLocked`——`runNodeWithHooks` 自己明写「visit 不在这里推进」）——同一个 NodeID 可以同时是静态边
//     目标和 SendFrom 目标，两类成员并存于同一超步；若让 fan-out 实例也推进，静态
//     那个读到的 visit 就会被兄弟推高，跨 Resume 漂移。
//   - **SendFrom 派生成员**：非空，`newInstanceID()` **fresh mint** 并随 frontier 持久化。
//
// # 为什么 fresh-mint 就够（不需要确定性、不需要路径哈希）
//
// 身份**持久化**而非每次重算，所以不要求可复现：
//   - 嵌套 fanout（Z×2 各自 `SendFrom(W)`）→ 各自 mint，天然不撞
//   - 代际（父重跑 ⟹ 旧答案作废）→ 重跑 = 新一次产出 = 新 token，天然带代际
//   - 不含 visit → 躲开共享计数器的竞态；不含位置 → 躲开子集化重排
//
// # 它同时是 cp 的 wire 形态
//
// `cp.CurrentFrontier` / `cp.CompletedFrontier` 直接是 `[]FrontierMember`，**身份内联
// 在记录里**，不是「NodeID 数组 + 平行的身份数组按下标对齐」。
//
// 引入实例身份时先做的是后者，理由是「绝大多数图没有多实例，平行数组让缺字段天然退化
// 成静态边语义」。**退化那半仍然成立**（缺 `instance_id` 键 → 空串 → 静态边语义，与
// 平行数组缺字段等价），但下标对齐那半是**类型系统够不着的外部约束**：两个数组第 i 项
// 必须指同一个东西这件事没有任何东西管得着，只能靠 godoc 写「漏了不报错」靠人记，而错
// 位的后果是实例互相冒充——静默的错误结果。内联之后没有对齐可错，那类约束整个不存在。
//
// # 但它换来了一条新的约束，别只记前半句
//
// 内联之前 `cp.CurrentFrontier` 是 `[]NodeID`、`cm.frontierAtExit` 是 `[]FrontierMember`,
// 两个方向都得过元素类型转换 ⟹ **cp 与引擎共享底层数组在类型层就不可表达**。内联之后
// 两侧同类型，`cm.frontierAtExit = cp.CurrentFrontier` 能干净编译且静默错——这条不变量
// 从编译器保证降级成了 5 处手写 `make`+`copy`。
//
// 危害具体而隐蔽：`updateFrontier` 是 `append(frontierAtExit[:0], ...)` **就地复用底层
// 数组**，而 `make([]T, n)` 给出 cap==len ⟹ 别名只在 frontier **收缩或等长**时才写回原
// 数组、**变长会 realloc 自动躲开**。哨兵见
// `TestRestore_FrontierCopyProtectsCallerCheckpoint`（专门构造收缩）。
type FrontierMember struct {
	// NodeID 待执行的节点。
	NodeID NodeID `json:"node_id"`

	// InstanceID 实例身份。**空串 = 静态边/cond 边成员**（恒单实例，见类型 godoc）。
	// 非空仅来自 SendFrom 派生，值是不透明 token——业务不应解析它的内部结构。
	//
	// 业务自研 backend 漏序列化本字段不报错，多实例场景下会退化成「全部实例身份为空」
	// → 互相冒充（InterruptID 撞 → 后来的 query 被 dedupe 吞 → 那个实例永远等不到
	// 答案）。`checkpointer_conformance` 有断言。
	InstanceID string `json:"instance_id,omitempty"`
}

// instanceCounter 给 SendFrom 派生实例发号。
//
// **进程内单调即可，不需要全局唯一**：InstanceID 的作用域是「单个 (sid, ns) 的 cp」，
// 且随 frontier 一起持久化——跨进程 Resume 时读的是存下来的值，不重新 mint。
// 用计数器而非 UUID：无外部依赖、可读性好（排查时一眼看出投递顺序）。
var instanceCounter atomic.Uint64

// newInstanceID mint 一个新的实例身份。**每次调用都是新值**——这正是"父重跑 ⟹
// 子任务全新 ⟹ 旧答案作废"语义的来源（与 LangGraph 的 fork 换 checkpoint_id、
// eino 的 callID 换新是同一原则：身份自然变，不需要显式作废逻辑）。
func newInstanceID() string {
	return "i" + strconv.FormatUint(instanceCounter.Add(1), 36)
}

// staticMembers 把一组 NodeID 包成静态边成员（InstanceID 空）。
// 供 [assembleFrontier] 的出口 / StartNode 起跑等"恒单实例"路径用。
func staticMembers(ids []NodeID) []FrontierMember {
	if len(ids) == 0 {
		return nil
	}
	out := make([]FrontierMember, 0, len(ids))
	for _, id := range ids {
		out = append(out, FrontierMember{NodeID: id})
	}
	return out
}

// ⚠️ 这里原本有 memberNodeIDs / memberInstanceIDs / restoredFrontierMembers /
// restoredMembers 四个 helper，负责「成员列表 ↔ 两条平行数组」的来回转换与长度错位时
// 的降级。cp 改成内联身份之后四者一并删除：cp.CurrentFrontier 本身就是成员列表，没有
// 转换、也没有对齐可错。
//
// 尤其 restoredMembers 的整个存在理由是「两个数组长度可能对不上」——那是并行数组自带的
// 失败模式，不是需要长期兜的业务场景。它当年的 godoc 自己写着「这是止损不是修复」。
//
//（memberNodeIDs 是「抽出 NodeID 列表喂给按 NodeID 工作的下游」，它仅有的两个调用点都
// 在旧 Snapshot 里、随平行数组一起没了。仍在用的是 staticMembers / distinctNodeIDs，
// 别一起清。）

// instanceFanoutIndex 把 InstanceID 转成 `JoinFanoutNS` 要的非负整数下标——
// **给 cp namespace 派生用**（ISSUE-96 侧面 2）。
//
// **为什么不直接把 token 拼进 ns**：`SplitNS`/`parseSegment` 的既有契约是 `#` 后面
// 必须是十进制整数（`NSSegment.Index` 是导出 API）。塞字符串会让 ns 解析报错。
//
// **为什么无碰撞**：token 形态是 `"i" + base36(单调计数器)`，本函数解回那个计数器值——
// 是双射，不是 hash。（对比 `agent_v2.RunSubAgent` 的槽号：那里是整个调用身份的哈希，身份里
// 有模型给的 tool_call ID、业务给的输入，只能 hash，有理论碰撞面；我们的 token 天生是数字。）
//
// 非预期形态（业务伪造 / 未来换编码）返 (0,false)，caller 退回无 fanout 维度的
// `JoinNS`——退化成本字段引入前的行为，不 panic。
func instanceFanoutIndex(instanceID string) (int, bool) {
	n, ok := instanceTokenValue(instanceID)
	if !ok {
		return 0, false
	}
	if n <= math.MaxInt32 {
		return int(n), true
	}
	// **超出 int32 不能退化成"没有 fanout 维度"**：调用方拿不到下标就退回
	// JoinNS(parentNS, nodeID)，于是该节点的**全部实例撞同一个 childNS、子 cp 互覆**
	// —— 而且一声不吭。计数器是进程级累计、不随 Run 重置（跨重启还会被 Restore 抬着
	// 续上），长跑服务够得着这个数。
	//
	// 故越界时 hash 进 int32 空间：碰撞概率 ~1/2^31，比"保证全部互撞"好几个数量级。代价只是
	// ns 后缀不再等于投递序号。（`agent_v2.RunSubAgent` 的槽号取 63 位，那里的碰撞面是「同一个
	// 父节点下各留着存档的调用数」的生日界，这里的越界本身就极少发生，31 位够用。）
	h := fnv.New32a()
	_, _ = h.Write([]byte(instanceID))
	return int(h.Sum32() & 0x7FFFFFFF), true
}

// instanceTokenValue 解出 token 里的发号器原值。
//
// 与 instanceFanoutIndex 分开：那个要落进 ns 后缀故受 int32 约束，这个只用来比大小
// （抬发号器），必须拿到完整值。
func instanceTokenValue(instanceID string) (uint64, bool) {
	if len(instanceID) < 2 || instanceID[0] != 'i' {
		return 0, false
	}
	n, err := strconv.ParseUint(instanceID[1:], 36, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// bumpInstanceCounterFromCheckpoint 把发号器抬到不低于 cp 里出现过的最大 token。
//
// 扫的是**所有**存了实例身份的地方：两组 frontier 成员 + deferred send 的目标与发出者
// 身份。漏扫一处，那一处的旧 token 就可能被重新发出去（见 Restore 调用点的注释：撞号是
// 静默的错误结果，不是报错）。
func bumpInstanceCounterFromCheckpoint(cp *Checkpoint) {
	if cp == nil {
		return
	}
	var maxVal uint64
	consider := func(id string) {
		if n, ok := instanceTokenValue(id); ok && n > maxVal {
			maxVal = n
		}
	}
	for _, m := range cp.CurrentFrontier {
		consider(m.InstanceID)
	}
	for _, m := range cp.CompletedFrontier {
		consider(m.InstanceID)
	}
	for _, sd := range cp.DeferredSends {
		consider(sd.InstanceID)
		consider(sd.OwnerInstanceID)
	}
	if maxVal == 0 {
		return
	}
	// CAS 循环而非 Store：并发 Resume 多个 sid 时不能把别人刚发的号压回去
	for {
		cur := instanceCounter.Load()
		if cur >= maxVal {
			return
		}
		if instanceCounter.CompareAndSwap(cur, maxVal) {
			return
		}
	}
}

// InstanceFanoutIndex 从 ctx 取当前节点实例身份并转成 fanout ns 下标（ISSUE-96）。
//
// 同一节点被 SendFrom 同 Target 投递多次时，N 个实例各自的子运行必须物理分离，否则子 cp 互相
// 覆盖、进度静默丢失——本函数给出区分它们的那个数。第二个返回值 false = 当前节点无实例身份
// （静态边 / cond 边节点，恒单实例），那种节点靠 visit 区分重访（[JoinVisitNS]）。
//
// ⚠️ 业务侧一般不该拿它自己拼子 ns：`agent_v2.RunSubAgent` 把它当作调用身份的一段，与调用方
// 所在的节点与相位、tool_call、visit、输入内容（或调用方给的 key）一起哈希成槽号，手拼等于
// 复制一份会漂的拷贝。
func InstanceFanoutIndex(ctx context.Context) (int, bool) {
	return instanceFanoutIndex(GetCurrentInstanceID(ctx))
}

// distinctNodeIDs 按首次出现顺序去重取 NodeID —— 同 NodeID 的多个并发实例只算一份。
//
// 顺序稳定（不用 map 遍历）：调用方拿它做 maxVisit 触顶判定，谁先撞顶决定报哪个
// 节点，随机顺序会让同一张图报出不同的错。
func distinctNodeIDs(ms []FrontierMember) []NodeID {
	out := make([]NodeID, 0, len(ms))
	seen := make(map[NodeID]struct{}, len(ms))
	for _, m := range ms {
		if _, dup := seen[m.NodeID]; dup {
			continue
		}
		seen[m.NodeID] = struct{}{}
		out = append(out, m.NodeID)
	}
	return out
}

// newEpoch mint 一个新的 cp 世代标记（`Checkpoint.Epoch`）——框架每次写 cp 前调。
//
// **随机而非递增**：只需判"变没变"、不需比大小。递增计数器跨进程重启会从头开始、
// 与 cp 里存的旧值撞号 → 误判"没变" → 守卫静默失效（实例身份那套踩过这个坑，
// 靠 Restore 时 bump 兜底；这里用随机值从源头免疫，不需要任何恢复期修补）。
//
// 8 字节足够：守卫只比较"本次载入的那一代"与"槽里当前这一代"两个值，不是全局
// 唯一性要求。
//
// 不做失败兜底：Go 1.24 起 `crypto/rand.Read` **保证不失败**（内部失败直接 panic），
// 写个 fallback 分支既是死代码、又只能退回递增计数器——正是上面说要避免的东西。
func newEpoch() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // Go 1.24+ 恒成功，见上方 godoc
	return hex.EncodeToString(b[:])
}

// formatFrontierMember 把成员渲染成可读的定位串——`node` / `node#inst`。
// 只给错误正文用：定位信息给全（光有 NodeID 时 fan-out 的两个实例长得一样）。
func formatFrontierMember(m FrontierMember) string {
	if m.InstanceID == "" {
		return string(m.NodeID)
	}
	return string(m.NodeID) + "#" + m.InstanceID
}
