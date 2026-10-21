# ISSUE-99：状态架构的整体重审（进行中）

> **状态**：调研中，七路并发，**尚未做任何代码改动**。
>
> **缘起**：cp 相关的 bug 一个接一个改不完；会话历史层四轮补丁每轮引出新问题（见
> [issue-98 §9](issue-98-conversation-history.md)）。停止逐个修，先把整个状态设计挖清楚，
> 再一次性决定动什么。
>
> **⚠️ 本文档的证据分两档**，读的时候别混：
> - **[已核实]** = 我自己 grep/跑过的，主要是关于我们自己代码的断言
> - **[待交叉]** = 调研 agent 报告的三方源码结论，等其他路回来交叉比对后才算数
>
> 之前栽过「把 agent 报告当结论」的跟头，这里明确分档。

## 调研矩阵（七路）

| # | 维度 | 核心问题 | 状态 |
|---|---|---|---|
| ① | **会话消息的写入所有权** | 谁写 / 嵌套时谁代写 / 并发怎么归约 / 跟 cp 的分界 | ✅ 已回 |
| ② | **cp 结构与一致性** | 17 字段 + 6 处隐式约束 + 零校验，别人几个字段、靠什么保证 | ✅ 已回 |
| ③ | **状态分层全景** | 各家几套存储？我们四套是不是太多 | ✅ 已回 |
| ④ | **身份体系** | 我们七套 id，哪几套其实是同一个问题的侧面 | ✅ 已回 |
| ⑤ | **上下文构建与压缩** | `buildInitialMessages` 几百行，哪些复杂度是自造的 | ✅ 已回 |
| ⑥ | **我们自己的全景盘点** | 穷举状态处 / 身份 / 交互点 / 隐式约束 + bug 分布统计 | ✅ 已回 |
| ⑦ | **扩展点开放性的代价** | 框架要「静态遍历用户结构」这件事本身是不是就该避免 | ✅ 已回 |

---

## 第 ② 路结论：cp 结构

### 我原来的假说，三处要更正

我提的是「我们是**状态快照 + N 个平行辅助字段**，而写日志式（每个 task 的 writes 单独记，
恢复=重放）可能根本不产生字段间一致性问题」。

**更正 1 — 不是「日志 vs 快照」二选一。** [待交叉] LangGraph 是**快照 + 一个超步的 redo
log**：恢复时不重放历史，而是「加载快照 → 重算 task 集合 → 把本超步已有的行贴回去 → 只跑
没有行的」。五家里只有 CrewAI 真是日志形态，而它连图都没有。

**更正 2 — 字段数不是驱动因素。** [待交叉] OpenAI Agents SDK 的 `RunState` **26 个字段**，
比我们 17 个还多，没有我们这类 bug。MAF 11、eino 8、LangGraph 7+5。数量和病无关。

**更正 3 — 日志不能消灭约束。** [待交叉] LangGraph 自己的 `DeltaChannel`（beta）引入了跨
checkpoint 的链约束，随即在 docstring 里长出 *"silently reconstruct as empty (no error
raised)"* ——跟我们 godoc 里「漏了不报错，只是静默退化」是同一句话，并跟着一串修复 commit。
**只要引入跨记录依赖，同样的病立刻复发。**

### 真正的分界线：两个决定，各自派生一批字段

#### A. 部分失败时，合不合成功分支的 writes

我们现在（`compose_v2/graph_loop.go` 超步提交段）[已核实]：

```go
if len(res.SuccessWrites) > 0 { cm.ApplyAll(res.SuccessWrites) }   // 先合
if res.FirstErr != nil {
    cm.appendCompletedSiblings(res.SucceededBranches, res.SendDirectives)  // 再单独记「刚才是谁」
    return res.exitErr()
}
```

**一旦合进 `Channels`，「哪条 write 是谁产的」就被抹平了。** 此后所有还需要这个信息的地方，
都只能靠一套平行簿记去重建——那就是 `CompletedFrontier` / `CompletedInstanceIDs` /
`DeferredSends` / `PendingSendWrites` / `FrontierInstanceIDs` 的**全部来源**。

这句是整份报告最要紧的一句：**它们不是「memo」，是被抹掉信息的唯一副本**——所以丢了必然
静默出错，而不是「退化一点」。

[待交叉] LangGraph 在这一刻**什么都不合**，行留在 writes 表里带着 `task_id`（主键
`(task_id, idx)`）。于是：

| 我们的字段 | 那边对应什么 |
|---|---|
| `CompletedFrontier` / `CompletedInstanceIDs` | 有行的 task_id 集合（**无字段**）|
| `DeferredSends` | 成功者那批行里 channel==`__pregel_tasks` 的几条（**无字段**）|
| `SerializedSend.Owner` / `OwnerInstanceID` | 行的主键（**无字段**）|
| 「重跑前清掉旧 send」 | `put_writes` 本来就删自己的旧行（**无逻辑**）|
| `PendingSendWrites` | 不存在——writes 从不「先 apply 再 atomic 回滚」|
| `FrontierInstanceIDs` / `InstanceID` | payload 列表的下标（**无字段**）|

**6 个字段 + 6 处隐式约束里的 5 处，全部来自这一个决定。**

#### B. 单槽覆盖 vs 每超步追加

我们 `c.data[sessionID][ns] = cp`（`compose_v2/checkpointer.go:411`）[已核实]——一个
(sid, ns) 一个槽。[待交叉] LangGraph 是 `storage[thread][ns][checkpoint_id]` 追加 + parent
链，id 单调。后果：

- `NodeVisits` —— cp id 不变，所以要额外计数器给 InterruptID 加「第几次访问」维度；追加式下
  `step` + 新 cp id 天然做到
- `Epoch` + `ErrCheckpointStale` —— 因为覆盖是静默的；追加式下并发写产生的是**分叉**而非覆盖
- `CPStatusFailed` vs `CPStatusPending` —— 因为一个槽同时承担「HITL 挂起」「durability 中途
  点」「失败现场」三种角色，只能靠出口打标区分
- `ErrRunAborted` 必须**主动 DeleteSubtree**（我们发现过「跳过写不够」）—— 追加式下不写就是
  不存在

**再 3 个字段来自这一个决定。**

剩下 8 个（`ID/NS/ParentNS/GraphID/CreatedAt/SchemaVer/Channels/PendingInterrupts`）是每家
都有的，不是问题所在。

### C. 退化方向 —— 独立于结构，也是最便宜的一条

[待交叉] 实测让 backend 故意漏存字段：

| 漏存什么 | LangGraph 的后果 |
|---|---|
| `pending_writes` | 结果正确，多跑 3 次 |
| `versions_seen` | 结果正确，多跑 1 次 |

**丢一整个字段的代价是白干活，不是算错**——因为每个辅助字段都是可重算信息的备忘，缺省值
一律被解释成「这活还没干」。

我们的对照（**我们自己 godoc 写的**）[已核实]：`FrontierInstanceIDs` 漏 → 实例互相冒充静默
丢数据；`Owner` 漏 → Target 双发；`InstanceID` 漏 → InterruptID 撞车被 dedupe 吞掉；
`PendingSendWrites` 漏 → target 静默读零值；`CompletedFrontier` 漏 → 成功分支后继永久丢失。
**五个全是静默出错方向。**

**一个当时没想到的第三选项**：`restoredMembers`（`compose_v2/frontier.go:126-138`）长度不
匹配时降级成「全静态身份」+ warn，注释写的是「两个坏结果里挑可观测的那个」[已核实]。但
**当时只在两个坏选项里选**——还有第三个：把这批成员**全部当成没跑过重跑**。那才是「只多干
活、不出错」的方向。

### 顺带发现

[已核实] 我们的 `SchemaVer` **写了（=9）但从不读**——`serialization.go` godoc 明写「仅供观测 /
业务 debug」「业务侧迁移逻辑负责按 cp.SchemaVer 分支」。[待交叉] 五家里 OpenAI 是白名单
fail-fast（且 import 期 assert 强制每个版本有一行摘要）、MAF 是拓扑 hash fail-fast、
LangGraph 按 v 号前向迁移、CrewAI 按版本链式迁移、eino 交给业务回调。**我们是唯一「存了版本
号但没有任何版本行为」的。**

### 三方走过同一条弯路（[待交叉]，但若属实是最有说服力的证据）

- LangGraph `9b6100bc4`(2024-08)：*"Avoid storing Sends twice — Sends are stored through
  put_writes, so we don't need to also store them inside checkpoint object"*
- LangGraph `4e8fbe452`(2025-05)：*"Remove Checkpoint.pending_sends — Instead store sends in
  a Topic channel"*

即：**曾经有过一个跟我们 `DeferredSends` 一模一样的顶层字段，分两轮删掉了。**

- eino `39eabc0`(2026-01) *"persist rerun node inputs in checkpoint"* —— **正是我们 F-B /
  `PendingSendWrites` 那个 bug**（重跑节点的输入丢了，静默读零值）。他们的补法是往按 nodeKey
  键的 **map** 里塞一项，不是新开一个并行数组。

### 建议的形态（报告给的，尚未评估可行性）

1. **`Send` 降级成普通 channel 写**（保留 channel + 有序 Topic reducer）→ target 和 payload
   同体 → `PendingSendWrites` 消失
2. **实例身份 = payload 列表下标** → `FrontierInstanceIDs` / `CompletedInstanceIDs` /
   `SerializedSend.InstanceID` 消失
3. **6 个 frontier 字段换成一张写日志**（`TaskID` 派生不存储；超步只跑「没有行的 task」，
   完成时先删自己的旧行再追加 → `Owner` purge 天然实现）
4. **每超步追加 cp**（可独立于 1-3 先做）→ `NodeVisits`、`Epoch`、`Pending/Failed` 双档、
   abort 主动删 四件事一起消失

净效果：**17 → 约 9 个字段**。⚠️ **「6 处下标对应约束 → 0 处」是虚报**——全仓「按下标一一对应」只有 **2 处**（`FrontierInstanceIDs↔CurrentFrontier`、`CompletedInstanceIDs↔CompletedFrontier`）。我把「6 处隐式约束」和「6 处下标约束」当成了同一个数，它们是两个概念。真实收益是 **2 处下标约束 → 0** + 若干「漏了不报错」的字段被主键强制。

**成本**：`graph_loop.go` 超步提交路径重写 + cp schema 破坏性变更 + 所有业务自研 backend
失效 + conformance 重写。项目未上线，按规约允许，但不是一个下午的活。

### 不做大改也该做的两件（便宜且正交）

- **把退化方向翻过来**：逐个字段过「缺省值意味着什么」，任何缺失必须解释成「这活还没干，
  重跑」。能把上面五个「静默出错」全部转成「多跑一次」。
- **restore 时校验拓扑 + 让 `SchemaVer` 真正被读**（拓扑 hash 抄 MAF、版本白名单抄 OpenAI）。

### 一句要记住的话

**约束不会归零。** 能做到的是把约束从「两个并行数组按下标对应」（无法验证、错了静默）搬到
「日志行的主键」（map/DB 天然强制，错了根本存不进去），外加一条「缺字段只准多干活」的兜底。

---

## 第 ① 路结论：会话消息的写入所有权

### 一句话：五家没有一家有「一场会话多个并发写入者」这个局面

[待交叉] 不是它们解决得好——是**结构上让它不可能发生**。

### 根因：`WithHistory` 挂错了地方

[已核实] 我们的是 `func WithHistory(hs HistoryStore, owner string, ...) Option`
（`agent_v2/history.go:197`）——**构造期 option，挂在 BasicAgent 上**。

[待交叉] 五家**全都在运行入口**：

| | 会话存储配在哪 |
|---|---|
| OpenAI | `Runner.run(..., session=)` |
| MAF | `agent.run(..., session=)` |
| LangGraph | `compile(checkpointer=)` + config 里的 `thread_id` |
| eino | `NewRunner(RunnerConfig{CheckPointStore})` |
| **我们** | **`agent_v2.Option`（BasicAgent 构造期）** |

于是编排层看不到它 → 只能「从 sub-agent 借」（`history_slot.go` 的 `historyConfigProvider`，
注释自己就是这么写的）→ 于是有了「探测多深」这个**本质无解**的问题：直接子不够（多套一层
就分裂），递归会碰到环和未执行分支（栈溢出），而 GraphAgent 那层怎么都穿不过。

**issue-98 §9 的第 5 条是纯粹的配置位置问题，不是探测算法问题。** 换成运行期声明后：

```go
ctx = compose_v2.WithThreadIDCtx(ctx, convID)
ctx = agent_v2.WithHistoryCtx(ctx, store, owner)   // 会话身份和会话存储在同一层声明
```

任何编排 / BasicAgent 的 `Run` 入口一律「ctx 有配置 && ctx 无 writer → 建一个」。
**零探测、零递归、零栈溢出、零中间层重复。**

### 该抄的结构性决策：「谁该记」必须是纯函数

[待交叉] eino 的 `adk/flow.go:509-527` 是全场唯一正面解决「多层 + 并发 + 不认识的执行体」的
写法，只做两件事：

1. **身份戳在事件产生处打一次，先到先得**：`if len(event.RunPath) == 0 { event.RunPath = runCtx.RunPath }`
2. **记不记 = 比对事件自带的戳和我这一层的身份**：`exactRunPathMatch(runCtx.RunPath, event.RunPath)`，**纯函数**

我们是反过来的 [已核实]：戳是**写入时**由 `recordHistory` 顺手盖的
（`history.go:826` `w.markSlotWritten(HistorySlot(ctx))`），所以「这一层记过没」只能事后去查
写入器里的 `wroteSlots` map。

**判据存在写入器里 → 判据是可变共享状态 → 并发下必然要靠锁里的 CAS 补救（issue-98 §9 第 4 条）
→ 而 CAS 只在同一个 Run 内有效，跨 Run 又得读历史补第二道。** 两个判据各管一段，正是补丁的形状。

[待交叉] LangGraph 那条 commit 说的是同一件事，只是场景不同：*"ID assignment has historically
lived in the `add_messages` reducer, but that's the wrong place ... The correct long-term fix is
to assign IDs in `BaseMessage.__init__`"* ⟹ **身份属于对象，不属于归并点。**

### 原始 input 不该进日志

[待交叉] eino 只在 `isRoot()` 时存 `RootInput`（`adk/runctx.go:394-397`），读时前置拼
（`flow.go:275-290`）；MAF handoff 只在 `is_start_agent and not self._full_conversation` 时
广播一次。

**两家都是「让写入只发生在结构上唯一为真的那个点」，而不是「写多次再去重」。**
我们的 nonce + 扫 prior + 原子认领三件套（issue-98 §9 第 3、4 条两轮补丁）解决的是一个
**自找的问题**。

### 并发分支不该直接写全局写入器

[已核实] `RunSession` 已经有 `ForkLane` / `CommitLaneEvents`（`session.go:343`，抄的 eino
lane），**但历史层 0 处引用**——各节点直接 `w.submit()`。让历史写入也走 lane（分支本地攒 →
join 处按确定顺序单点提交）能同时修掉 issue-98 §7 记的那条已知边界（并发分支落盘顺序不定）。

### 归属：位置只做门禁，别做跨 Run 读回键

[待交叉] 五家里**没有一家**用「编排位置」做跨 Run 的读回键：eino 的 `RunPath` 只做记录门禁；
MAF 用 `(agent, conversationId)` 做 **key** + `author_name` 做展示；OpenAI 和 LangGraph 干脆
不表达归属。

MAF 那个形状（**key 里带 agent 维度，一个 agent 一条流**）对编排结构的改动**完全免疫**——而
我们现在的位置路径，`history_slot.go` 的 godoc 自己就写了代价：改分支名 / 加 wrapper → 旧历史
彻底读不回，且症状是完全空。

### 一条要认真对待的信号

[待交叉] eino 自己在四个 API 上挂了 `NOT RECOMMENDED`（`flow.go:68-71/79-82/91-95`、
`interface.go:423-426`），说法是「多 agent 共享全量上下文 + 路径归属 **has not proven to be
more effective empirically**」，推荐退回「主 agent + AgentTool（子会话隔离，父只看到 tool
result）」。OpenAI 的 `as_tool(session=None)` 默认值、MAF 的 `propagate_session=False` 默认值
指向同一方向。

**这不是说我们的目标错了**（业务确实要「列出某会话的历史」），而是：**「所有 agent 的消息在
同一条流上、靠运行期判重保证不重不漏」这个具体形态，是五家里唯一没人采用的那个。**

### 建议的三步（报告给的，尚未评估可行性）

1. `WithHistory` 从构造选项挪到运行入口 / ctx → 消掉 §9 第 5 条（探测 / 递归 / 栈溢出）
2. 「谁该记」从查 map 改成比对数据自带的身份戳 → 消掉第 4 条（并发判重）
3. 原始 input 从日志内容改成单独一份、读时前置 → 消掉第 3 条（nonce + 认领）

各自对应一条补丁，且都有两家以上现成实现可对照。

---

## 第 ⑥ 路结论：我们自己的家底（对照基线）

全部 [已核实]——这一路只读我们自己的代码，结论可直接采信。

### 规模比我说的严重

我立项时说「四套状态存放处、七套身份」。实际盘出来：

| | 我以为 | 实际 |
|---|---|---|
| 状态存放处 | 4 | **35 处** + ctx **34 个私有 key** + 消息内嵌 **18 个 annotation key**。⚠️ **连「4」这个立项数字本身也是错的**——`store.md §1` 列的是 **6 层**（Channel state / RunSession.Events / RunSession.values / Checkpointer / HistoryStore / store.Store），CLAUDE.md 那句「4 层 state 边界」是 HistoryStore 加进来之前的旧数，已修 |
| 身份 | 7 | 远超 7（见下方「同名不同物」）|
| 同一份数据的多处表达 | 没数过 | **18 个交互点** |
| 隐式约束 | 6（只数了 cp 的）| **33 条** |

### 33 条隐式约束，靠什么保证

| 保证手段 | 条数 |
|---|---|
| **类型强制** | **0** |
| 运行时校验 | 8 |
| conformance 测试 | 9 |
| **纯约定（只有 godoc 说一句）** | **16** |

### 反复出 bug 的交互点（按轮次）

| 交互点 | 反复次数 | 现状 |
|---|---|---|
| ① **超步部分失败时「成功成员的信息」怎么留** | **≥12 轮** | ISSUE-46 → 62 → PA-H1 → 95#1 → 照方案实现即翻车 → 修自引回归 → F-B → ISSUE-96 六侧面 → 四路审计再抓 5 个 → 收口后又抓第 7 个 |
| ② **会话历史的写入所有权** | **5 轮，第 5 轮已撤回，1-5 全部仍开着** | 见 [issue-98 §9](issue-98-conversation-history.md) |
| ③ **中断 query 的两份拷贝** | **≥5 轮** | ISSUE-60/61 → 81 → `errors.Join` 断链 → fan-out 只返第一个 → 判据写成「≥2 才合并」→ 单中断被剥 sentinel → typed-nil 吞文案 |
| ④ **同 NodeID 多实例的身份** | **一个 ISSUE 内 12 个** | ISSUE-96：根因是「记录/身份用 NodeID 粒度，执行单位却是 branch 实例」|
| ⑤ **cp 单槽的三重角色** | **4 轮** | 「跳过写 ≠ 没有 cp」→ ISSUE-94 两轮深挖 → 加 `CPStatusFailed` |
| ⑥ **同一 sid 并发写 cp** | **7 轮 fix** | ISSUE-97。**核心危害（副作用双跑）明确防不住** |
| ⑦ **input 的三处表达与读写时序** | **≥4 轮** | 含我自己在 ISSUE-74 引入的 input dup regression |
| ⑧ **嵌套 ns / 子 cp 互覆** | **≥4 轮** | ISSUE-78 → ISSUE-96 侧面 2（**踩的是我们自己文档推荐的路径**）|

### 三个结构性观察（这一路最值钱的部分）

**1. 有 5 个 0-fix 的状态相关文件，它们具备同一组特征。** ⚠️ 我原写「`ns.go` 是唯一」——**错了**，还有 `ns_ctx.go`(5 commit/0 fix)、`soft_cancel.go`(2/0)、`store_scope_ctx.go`(5/0)、`store/store.go`(6/0)。但这**反而加强**论证：5 个全是纯函数 / 纯接口 / 纯 ctx helper，**共同点是无跨记录依赖**，不是某一个的偶然。

共同特征：**无跨记录依赖**（`ns.go` 另有「约束由 `ValidateNodeIDForNS` 运行时强制」）。
⚠️ 但要诚实标注这条证据的限度：`ns.go` 的 3 个 commit 是 1 feat + 2 doc/style——它不是
「改了很多次都没出错」，是**从没被功能性改过**。而且 `ns.go` 零 fix ≠ ns 这件事零 bug：
⑥ 路热点表里「嵌套 ns / 子 cp 互覆 ≥4 轮」的 fix **全落在调用方文件**（`frontier.go` 的
`instanceFanoutIndex` 那段 int32 兜底就是被 `ns.go` 的 `Atoi` 契约逼出来的）。
**这个抽象把复杂度推给了调用方。**

对比同期：`basic_agent.go` 155 commit / 75 个 fix，`graph_loop.go` 103 / 55，
`interrupt.go` 41 / 26，`history.go` 28 / **19**（该文件 2026-07 才建，68% 是修）。

**2. 约束的保证手段与 bug 密度反相关。** 33 条里 16 条是纯约定，而反复出 bug 的
①③④⑤ **全部落在纯约定区**；conformance 覆盖的那 9 条，**都是被 bug 打出来之后才补的断言**。

**3. 失效模式清一色是「静默」。** `checkpointer.go` 一个文件里「漏了不报错 / 静默退化」
出现 6 次；全仓注释含「静默」的行有 **122 处**（compose_v2 + agent_v2，去测试）。

### 两个之前没注意到的坑

**`AgentEvent.RunID` 和 `RunSession.RunID` 同名不同物**：前者是 BasicAgent **每轮 LLM
调用前**生成的批次号（`basic_agent.go:1543`），后者是 per-Run 会话号。`AddEvent` 不会把
后者填进前者。同名让人以为是一个东西。

**`sid` 一值三用**：`RunSession.RunID` 同时是 cp 主索引、observability sessions key、
StoreMemory item key 前缀——改一个用途会波及全部，中间没有隔离层。

### 顺带发现并已修的两处 drift

- `agent_v2/session.go` godoc 说「跨 run 持久化用 `cp.ThreadID` 字段」——该字段已随
  ISSUE-98 删除，全仓无生产引用
- CLAUDE.md 的 `Checkpoint` 速查未列 `CPStatusFailed` 与 `Resumable()`，`CheckpointStatus`
  现为三档而非两档

---

## 第 ④ 路结论：身份体系

### 七套里真正独立的只有四个问题

「图里哪个节点」「哪一次执行」「哪一场对话」「这条消息是不是那条」。

我们的七套之所以膨胀，是因为**「哪一次执行」被拆成了三种编码**，外加一个凑数的代际维度。

### 同一个 token，三种编码，两处持久化 [已核实]

一个实例 token 走三条路：

| 消费者 | 编码 |
|---|---|
| frontier | 存原值 `"i3"` |
| ns | base36 解回整数 → `worker#3`（`frontier.go` 那段**还带「超出 int32 就 fnv32a 兜底」的补丁**）|
| InterruptID | 原值喂 fnv64a |

那段 int32 溢出兜底是**纯粹的意外复杂度**——它是被 `parseSegment` 的 `strconv.Atoi` 契约逼
出来的，不是问题本身需要。放宽那个契约让 `#` 后面接不透明 token，三处就能用同一个字符串。

### `NodeVisits` 和 `InstanceID` 在抢同一个职责 [已核实]

`InterruptSite.InterruptID()` 里有一条**手写的互斥规则**：

```go
visit := s.Visit
if s.InstanceID != "" {
    visit = 0   // 有实例身份时 visit 维度置 0
}
```

存在的理由是 `visitCount` **per-NodeID 共享**，兄弟实例成功会把它推高。但这正是「两套东西
抢同一个职责」的症状——`Visit` 和 `InstanceID` 都在回答「这是第几个待办」。

[待交叉] 五家里**没有一家需要这条规则**：LangGraph 因为 `step` 和 `checkpoint_id` 已经把
「第几轮」编进去了、`idx` 只管「哪一个」，两者正交；eino 因为中断 id 每次新 mint，「第几次」
是白送的。

### 身份的生成时机：我们在「执行完成时」mint，别人在「调度时」派生

[已核实] 我们的 `InstanceID` 是 `runNodeWithHooks` 成功路径上盖章的。直接后果：

1. 必须持久化（四组并行数组）
2. 必须有 `bumpInstanceCounterFromCheckpoint` 在 Restore 时把发号器抬上去（还得 CAS 循环）
3. 业务自研 backend 漏序列化任何一组都**不报错只静默退化**——CLAUDE.md 为此写了整整四段警告

[待交叉] LangGraph 是**调度时**从 payload 下标算，所以什么都不用存、不用发号器、不用恢复
期修补。

**而我们本来就在存 payload**（`cp.DeferredSends` 带完整 `Payload`）——等于**既存了列表，又
额外 mint 一套 token 来给列表里的元素编号**。

### 缺的维度，按价值排

**1. cp 版本轴（最要紧）** —— 我们是 `(sid, ns)` 单槽覆盖，只有 `Epoch` 判「变没变」。
**上面那条互斥规则的根因就在这**：没有版本轴，「第几次」只能靠 `visit` 这种 per-NodeID 共享
计数器硬凑，一有兄弟实例就被污染。有了版本轴，`visit` 和 `InstanceID` 的冲突自然消失。
这跟第 ② 路的「单槽覆盖派生 3 个字段」是**同一条根因的两个侧面**。

**2. 拓扑签名** —— [已核实] `Graph.ID()` 的第一行是 `if g.name != "" { return g.name }`，
**有 name 就完全不看拓扑**，而九个编排全都强制要求设 name（构造期 fail-fast）。
⟹ **改了图结构但名字没变时，`ErrGraphIDConflict` 一定测不出来**——而那正是它要防的场景。
兜底的 fingerprint 也只哈希 nodeIDs + channel 名类型，**不哈希边拓扑**（godoc 自陈）。

**3. 消息 id + 按 id 归并** —— 我们只有转交场景的 nonce 一个特例。「并发扇出各记一份」在
有 reducer 的框架里是免费解决的，在我们这儿是靠「读历史 + 写入器原子认领」两段协议手工兜的。

**4. 工具身份 ≠ 工具名** —— 我们全靠工具名，MCP 多 server 同名工具会撞。

### 归属那一套：五家零支持，我们自己的代码也已经两处自述这条路走不通 [已核实]

```go
// run_subagent.go：**不用 childNS**——fanout 路径的 childNS 带 per-tcID hash 每轮都变，
// 拿它当归属会让同一个 sub-agent 每轮归属不同、跨轮读不回自己的历史。

// history_slot.go：graphNS 带 hop/round，归属路径必须剔掉……跳数/轮次跨 Run 会重置，
// 按它归属就读不回自己上一轮的历史。
```

两段说的是同一件事：**位置路径里凡是带运行时序号的段都必须剔掉才能当读回键**——而剔干净
之后剩下的「编排注册名静态路径」对结构改动是脆的，`history_slot.go` 自己也写明了「失效时是
彻底空」。

两条出路（都有先例，**需要拍板**）：
- **(a)** 读回键收敛成 `(会话 id, 存储实例)`，归属退化成纯展示——我们已经有
  `HistoryAgentDisplayAnnotation` 了，等于只删掉比较用的那一半
- **(b)** 让业务给需要独立历史的 agent 配一个稳定 id（如 `WithHistoryScope("billing")`），
  框架不再从编排结构推导

(b) 保住了「同一个 agent 挂两个位置历史分开」的能力，(a) 更简单。

---

## 第 ③ 路结论：状态分层

### 四套不算多，多的是第三份消息副本

按功能位比：cp 五家全有；`store.Store` 三家有。这两套没争议。

**真正多出来的是 `RunSession.Events` 和会话历史层的关系** [已核实]：同一批消息落两次，
而且**两处的归属用不同的键**。

### 我们用三个不同的键表达同一条消息的归属 [已核实]

| 键 | 形态 | 带不带运行时序号 |
|---|---|---|
| `(sid, graphNS)` | cp 寻址 | **带**（`agent_billing_hop2` / `round_3`）|
| `HistoryAgentAnnotation` | 历史归属 | **故意剔掉**（`history_slot.go` 详述了为什么必须剔）|
| `AgentEvent.AgentName` | 事件流归属 | agent 自己的名字（框架不校验唯一）|

[待交叉] LangGraph 的 `checkpoint_ns` **同样**带 task_id + step、跨 Run 必变，但它
**从不拿 ns 当消息归属**——归属在消息自己的 `id` 上。所以「带序号 vs 不带序号」这个矛盾
在它那儿根本不成立。

### 五家在出现第二份消息副本的当口，都补了消息级身份

[待交叉] 这是这一路最硬的发现：

| | 有第二份副本？ | 怎么对上号 |
|---|---|---|
| LangGraph | 否（消息只住 cp 一处）| `message.id`，reducer 按 id 幂等——**根本不需要游标/认领/回滚** |
| eino | 是（事件流 vs 图状态）| **2026 年加的** `Extra["_eino_msg_id"]` + `ToolMsgIDs` 传递链（产出侧预生成 → 写入侧 pop 出来盖上）|
| OpenAI | 是（`_generated_items` vs `_session_items`，且显式检测分叉）| item `id` + 内容指纹 + 已持久化游标 + 回滚 |
| MAF | 是（executor cp 三份重叠）| **没有消息 id，为此付了代价**——有整篇 ADR 讲「两份历史会分叉、用户无法判断最后一条在不在里面」，应对是把分歧暴露成配置选项让用户选 |
| **我们** | **是（三份）** | **`schema.Message` 无 ID 字段**；只有原始输入有 nonce |

我们的 nonce + CAS 认领 + `wroteSlots` 是同一类机器的**局部版**——只覆盖「原始用户输入」
这一种消息。**这正是「没有通用消息身份，于是每种消息各造一套判重」的症状。**

### 建议（这一路给的，按投入产出排序）

1. **给 `schema.Message` 加 ID —— 根因。** 加上之后三套局部判重可收敛成「按 ID 幂等」，
   并发扇出记几份不再是问题（不需要 CAS，因为写几次结果一样）。
   ⚠️ 这会动 `HistoryStore` 的 append-only 硬约定，要重新定义成「append-only 但同 ID 幂等」。
   ⚠️ **ID 必须在产出侧生成然后透传，不能在写入侧各自生成**（eino 那条传递链就是为这个）。
2. **Events 归并进 History，不要合进 cp。** Events 退化成纯粹的实时投递管道（chunk / SSE /
   hook 观测），不再承担「Run 结束后可回读的消息来源」。
   **不建议**学 LangGraph 把历史合进 cp：我们的 cp 是 `(sid, ns)` 单槽覆盖式且 ns 带执行期
   序号，改成累积式等于重做寻址。
3. **边界判据从「按容器问」改成「按生命周期问」。** 现在 `doc/v2/store.md §1` 是按容器
   组织的，而且自己数出来是「5 层 / 6 行」跟一般说法的「4 套」对不上——**连套数都没有单一
   说法，这本身就是判据不够硬的证据**。

顺带一条值得抄的**硬互斥**：[待交叉] OpenAI 用 `UserError` 拒绝「Session + conversation_id
同时给」——「对话历史每次 Run 只能有一个权威家」不是靠约定，是 fail-fast。我们对应的形状是
「同一场会话同时有 History 和业务自管 `RestoreEvents`」，现在两者可并存且无校验。

### 一条反向提醒

别因为「别家套数少」就急着砍：eino 1 套是因为它**没有跨 thread 存储**，CrewAI 的 Crew 面
0 套是因为它**把会话所有权整个推给调用方**——这两条我们都不打算走。在「要 store 也要框架管
会话历史」的前提下，**2 套是理论下限，而能做到的唯一原因是「消息有 ID 且只住一个地方」**。

---

## 第 ⑤ 路结论：上下文构建与压缩

### `buildInitialMessages` 393 行，但「消息组装」本身只有约 100 行

按职责切开之后 [已核实]：skill 预激活 105 行 + StoreMemory 128 行 = 233 行，那是别家做成
独立 provider / middleware 的东西，我们**内联进了组装函数**。

**这是可读性问题，不是设计复杂度问题。** 剩下的约 100 行跟别家同量级。

### 本质复杂度（换谁来做都躲不掉）

- **三种可见性视图**——不是我们发明的。三个点位每家都覆盖到了，只是散在不同原语里
  （别家是「独立 API + 默认值 + opt-in 开关」三处，我们收进一个 ctx 枚举）
- **压缩分多层**——行业共识，别家 2~4 层
- **审计视图 vs 模型输入视图分离**——**每家都做了**，只是机制不同（两个字段 / 排除标记 /
  两个 state key）
- **出口有个统一的配对兜底**——给定我们的规范表示是 wire 形，这是必需的

### 自己造的（可以不存在）

**1. 第 0 层按「条数」截而不是按「组」截 —— 最具体的一条**

`MaxHistoryPairs` 默认开启，`truncatePriorPreservingMarkers` 保留最后 N **条**，于是必须
引入 `AdjustIndexToPreserveAPIInvariants`（**86 行**）把切点往前拉。

**而我们自己的第 2/3 层早就用 `GroupMessagesByAPIRound` 按组切了** [已核实]。第 0 层用了
不同粒度，是那 86 行 + 一批边界注释的**唯一来源**。

调研做的结构实验：同一段对话（含单 tool_call 和双 tool_call 两轮）
- 按条截 → 非法（孤儿 tool_result）
- **按组截 → 保留最后 1/2/…/7 组全部合法**
- 把带 marker 的 tool_result 抽到最前（我们 `truncatePreservingMarkers` 干的事）→ 非法，
  **悬空和相邻性两类违约同时踩**

**2. nonce 去重（约 40 行 + 一大段注释）** —— 根因是**谁写历史**：我们让每个叶子 BasicAgent
各自写。别家的对应答案是「原始输入放单写者专用槽，不进那条多写者的 append-only 日志」。
**这不是「他们功能少」，是写入责任放在了 runner / orchestrator 层而不是叶子层。**（与第 ①
路结论一致。）

**3. prior 双来源 + 有意降级** —— **没有第二家有这个**。别家是「没配历史就是没历史」。
代价是所有跟 prior 相关的判断都得分 `fromHistory` 两支。这个降级档有真实价值，但它是我们
主动加的一档，不是必需品。

**4. `session.Events` 只记首尾这件事本身溢出到了模型输入路径** —— `dropDanglingToolCalls`
godoc 列的四个成因里第一个就是它。**这不是配对算法的问题，是审计流的粒度选择溢出到了模型
输入路径。**（与第 ③ 路「Events 归并进 History」指向同一处。）

### 如果只动一处

**按组截第 0 层**——唯一一个「改法明确、有自家先例（第 2/3 层）、能直接消掉 86 行 helper
加一批边界注释」的点，且结构实验已经给了回归判据。其余几条都涉及责任层次的搬迁。

### 顺带两条文档 drift（待修）

- `doc/v2/context-compaction.md` §11 表里「eino 配对感知：应该有」——实测**没有**，且不是
  遗漏：那边压缩是全量替换成摘要，**根本没有切点**
- 同表触发阈值「190K」——实测现在是 160000

---

## 第 ⑦ 路结论：扩展点开放性

### 两个把我带偏的判断，被实测推翻

**「`add_node(subgraph)` 会展开进父图，所以天然静态可知」——不成立。** [待交叉] 实测三种
挂法都不展开，子图始终是不透明节点。静态清单靠**扫函数闭包变量**猜，猜不到就 `except:
None` 静默降级。**没有一家把嵌套单元展开进父结构。**

**「返回事件流所以结构上免疫」——推论不成立。** 我们的 `Run` 返回 `[]*AgentEvent`，同样
能看见黑盒的出口，`settleHistorySlot` 吃的就是这个。差异在**会话对象在哪儿建**：那边是
Runner 在进入 agent 树之前无条件建一个塞进 ctx，**没有「是否开启历史」这个开关**。

### Go 的风险容忍度跟 Python 不是一回事 [已核实]

调研跑的对照：

```
Go     静态遍历遇环 → fatal error: stack overflow，recover 兜不住，进程死
Python 同样的代码   → 可捕获 RecursionError，进程活着，可以降级
```

**能照抄「探不到就降级」的语义，不能照抄它的风险容忍度。**（这正是我第 5 轮补丁撞上的
——环停在一条永远不会被选中的分支里时，Run 正常返回而静态探测走进去。）

### 病根一句话

> **开关配在叶子（`WithHistory` 在 BasicAgent 上），而资源必须活在根。**
> 于是框架只能「从根往下找有没有人配了」——四轮补丁都在给这个搜索打补丁。

[待交叉] **没有一家把 enable-flag 放在叶子上再去搜。** 三个已验证的所有权模型：

| 模型 | 谁持有 | 代价 |
|---|---|---|
| **A 调用方传入** | caller | 业务 API 多一个必填项；忘传就是没有（静默）|
| **B 根声明 + 运行时继承 + 显式弃权** | 被当根跑的那个单元 | 需要一个三态槽（继承 / 弃权 / 必须继承）|
| **C 入口无条件建** | 第一个到达的入口 | 没有关闭开关 |

**对我们最贴的是 B**：保住 opt-in、保住可插拔后端、保住嵌套时子配置被忽略的现有语义，
同时把「找」换成「传」。

### 盖章三规则（这是可以直接借的东西）

```
R1  执行体只产出、不记录；记录只发生在包住它的那一层
R2  产出物流过某层：没盖章 → 盖本层位置章；章==本层 → 记一条；章≠本层（来自后代）→ 只转发
R3  谁要把同一件东西分发给多个下游（并发扇出 / 多跳转交 / 重放），必须先在本层盖章再发
```

**R3 是关键**——它把「多个消费者同时判断谁记过」这个竞态从根上消掉：盖章发生在**分发
之前**，是单点写，不是并发的读-改-写。

调研在 Go 里重写了一遍，在 `Handoff[Parallel{Basic, 外部, Sequential[Basic, 外部]}, Basic]`
（失效清单 #3 + #4 叠一起、带并发屏障强制真并发）上 `-race` 连跑 200 次：条数分布
`{7: 200}`、内容逐字一致、原始输入恰好 1 条、外部执行体产出恰好 2 条。

**顺带一个我们代码里的同款坑**：「未盖章」的哨兵值不能跟「顶层位置」撞。我们的
`HistorySlot(ctx)` 顶层返 `""`，跟「没盖章」是同一个值——调研第一版这么写，实测把用户
原话记了 5 份。需要一个独立的 `stamped bool`。

---

# 整体设计

## 一句话诊断

**我们反复把「本可以从数据本身推出来的信息」，变成「必须单独维护的状态」。**

三个具体形态，各自派生一批字段和一串 bug：

| # | 在哪一步丢的 | 丢了什么 | 派生出什么 |
|---|---|---|---|
| **甲** | 部分失败时先 `ApplyAll` 合并成功分支的 writes | 「哪条 write 是谁产的」 | 5 个平行簿记字段——**它们不是 memo，是被抹掉信息的唯一副本**，所以丢了必然静默出错 |
| **乙** | cp 单槽覆盖 | 「第几次执行」这条轴 | `NodeVisits` / `Epoch` / 三档 Status / abort 主动删；以及 `visit` 和 `InstanceID` 抢职责那条手写互斥规则 |
| **丙** | 写入时才盖归属戳 | 「这一层记过没」 | 判据成了可变共享状态 → 并发要 CAS → CAS 只管一个 Run → 跨 Run 再补一道读历史 |

**反证在 ⑥ 路**：`compose_v2/ns.go` 是唯一 0 个 fix 的状态相关文件（同期 `basic_agent.go`
是 155 commit / 75 fix）。它的特征恰好是三条的反面——**纯函数、无跨记录依赖、约束由运行时
强制**。

## 分档实施

按「独立性 × 风险」排。**同一档内的必须一起做**（分开做就是上一轮「修一个引出一个」的
复现）；跨档可以停。

### 第 0 档 · 止血（零架构改动，可立刻做）

| 做什么 | 为什么 | 破坏面 |
|---|---|---|
| **退化方向翻过来** | 逐个字段过「缺省值意味着什么」，任何缺失必须解释成「这活还没干，重跑」。能把 5 个「静默出错」全部转成「多跑一次」 | 无（只改降级分支）|
| **`Restore` 校验拓扑 + 让 `SchemaVer` 真被读** | 我们是唯一「存了版本号但没有任何版本行为」的；`Graph.ID()` 有 name 就不看拓扑，`ErrGraphIDConflict` 防不住它要防的场景 | cp wire 不变，新增 fail-fast 路径 |
| **第 0 层截断改按组切** | 我们自己第 2/3 层早就按组切了，第 0 层用不同粒度是那 86 行切点修正 helper 的**唯一来源** | 截断边界变化，需回归 |

三条互相独立，可单独做单独验。

### 第 1 档 · 写入所有权（消掉四轮补丁的根）

**必须一起做的三件**：

1. `WithHistory` 从 BasicAgent 构造期 option → **所有编排都有的 option**（模型 B）：顶层
   配一次；嵌套不配 = 继承；显式 `WithoutHistory()` = 弃权
2. 写入器在**每个 `Run` 入口**做 `if ctx 里没有 && 本层配了 { 建 }`——首达者持有，**零遍历**
3. 归属从「查 `wroteSlots` map」改成**盖章 + 精确匹配**（R1/R2/R3），带独立的 `stamped` 标志

**一起做的理由**：盖章的前提是「有一个确定的层在盖」，而那正是 1+2 提供的。只做 3 会落回
「章由谁盖」的老问题。

**能消掉的**：`historyConfigProvider` / `startHistoryForOrchestration` / 递归探测整套；
失效清单 #5（写入器分裂）和 #9（GraphAgent 交不出静态子列表）**构造上不存在**；#4（并发
扇出各记一份）由 R3 消掉，`claimRootInput` 那套 CAS 可以删；#6（边界补记仍用读侧判据）
由 R2 消掉。

**破坏面**：`WithHistory` 的挂载位置变了（业务要改一行）；`RunSession` 的 events 语义
收窄成「实时投递管道」。项目未上线，允许。

### 第 2 档 · 消息身份（需要拍板）

给 `schema.Message` 加 id。**这是第 1 档的补充不是替代**——盖章解决「谁该记」，id 解决
「cp / 历史 / events 三处的同一条消息能不能对上号」。

⚠️ 三个必须避开的坑（都是实测出来的）：
- **id 在产出侧生成然后透传，不能在写入侧各自生成**
- **不能放 `Annotations` map**（通用 concat 会把 string 值拼坏且不报错）
- **动 `HistoryStore` 的 append-only 约定**：要重新定义成「append-only 但同 id 幂等」

### 第 3 档 · cp 结构（架构级，最大）

对应诊断的甲和乙，跟前两档**完全独立**（引擎层 vs agent 层）：

1. `Send` 降级成普通 channel 写 → target 和 payload 同体 → `PendingSendWrites` 消失
2. 实例身份 = payload 列表下标 → 三个 InstanceID 字段消失
3. 6 个 frontier 字段 → 一张按 taskID 键的写日志 → `Owner` purge 天然实现
4. **每超步追加 cp**（可独立于 1-3 先做，收益/成本比最高）→ `NodeVisits`/`Epoch`/双档
   Status/abort 主动删 四件事一起消失

净效果：**17 → 约 9 个字段，6 处下标对应约束 → 0 处**。

**破坏面**：`graph_loop` 超步提交路径重写 + cp schema 破坏性变更 + 所有业务自研 backend
失效 + conformance 重写。**不是一个下午的活。**

## 需要拍板的四件 —— 第二轮调研进行中

⚠️ **这四件我一度直接给了"我的倾向"，那是错的。** 前七路挖的是「病在哪」，拍板需要的是
「怎么做对」——两码事。没有针对性证据就给倾向，正是「改来改去」的源头。已针对每一件派了
专门调研，**结论回来之前不拍板、不动代码**。

| # | 问题 | 两个选项 | 第二轮在挖什么 |
|---|---|---|---|
| 1 | **归属键**（第 1 档之后仍在） | (a) 读回键收敛成 `(会话 id, 存储实例)`、归属退化成纯展示（我们已有 `HistoryAgentDisplayAnnotation`，等于只删比较用的那一半）<br>(b) 业务给需要独立历史的 agent 配稳定 id（`WithHistoryScope("billing")`），框架不再从编排结构推导 | 多租户 key 怎么改写（装饰器 vs 加参数）/ 「同一 agent 出现在多个位置」各家怎么处理、撞了怎么办 / 改编排结构后旧历史有没有迁移路径 / **我们要保的「同一 agent 挂两处历史分开」在五家有没有对应物**，没有的话是走不到这个形状还是认为需求不成立 |
| 2 | **消息 id**（第 2 档）| 做 / 不做 | 字段签名 / **生成时机**（有一家明写「放归并处是放错地方」）/ 相等性判断会不会让我们一大批 `reflect.DeepEqual` 测试变红（**要数**）/ 跟 conformance 那条「逐字重复的两条都得留」怎么调和 / wire 会不会漏给 provider / 流式 chunk 聚合时怎么传 |
| 3 | **第 3 档做不做、做几步** | 只做第 4 步（追加式 cp）/ 全做 / 不做 | 存储布局 + **清理策略**（追加式不清理会无限涨）/ 写放大多少 / fork 和 time-travel 是必然产物还是额外实现 / 接口 8 方法怎么变、业务自研 backend 改动面 / **逐个验那 4 件事真能消掉吗**——尤其「追加式下并发写产生分叉而非覆盖，那**分叉之后谁赢、业务怎么知道**」 |
| 4 | **共享历史转交（Raw 视图）要不要继续养** | 保 / 砍 / 砍一半 | 劝退它的理由到底是**效果判断**（多 agent 共享全量上下文没比单 agent + tool 更好）还是**实现判断**——前者是产品取舍、后者才是该跟的技术信号 / 替代方案（agent 当 tool）的具体形态 / 「模型自主决定转给谁」这个能力替代方案还有没有 / 我们 example 和 a2a、agui 里有多少在依赖 |

## 不建议做的

- **把历史合进 cp**（学「消息就是 state 的一个 channel」）：我们的 cp 是 `(sid, ns)` 单槽
  覆盖式且 ns 带执行期序号，改成累积式等于重做寻址。
- **继续给静态遍历打补丁**：`Agent` 是开放接口，任何遍历方案都存在探不到的层，而 Go 的
  栈溢出 `recover` 兜不住。
- **因为「别家套数少」就砍存储层**：他们套数少是因为不要跨 thread 存储、或把会话所有权
  整个推给调用方——这两条我们都不打算走。

## 已经顺手做掉的（不在上面四档里）

调研过程中查实并修掉的：中断优先于兄弟分支的业务错、多分支中断保第二 sentinel、共享历史
下并发扇出的原子认领、`CondFailRerunFrom` 存档档位、Resume 两个闸门判据对齐。

以及记进 backlog 的：`Graph.ID()` 不哈希拓扑、静态 fan-out 错误选取跨次不稳定。


---

# 第二轮：四个拍板点的落地细节

## 拍板点 1 · 归属键 —— 结论：**(b) 业务声明稳定作用域**

### 先修正第一轮的说法

第一轮我写「五家零先例」。**更准确的是**：

> 这个能力五家都能做到，但**没有一家用编排结构推导它**——统一是业务显式声明一个稳定字符串
> （或显式给一个独立容器）。
>
> 而结构性的键五家都有，**清一色只用于 run 内状态，且结构对不上时是响错**（拓扑哈希 /
> `Executor not found`）。**我们踩的不是「用了结构键」，是把只能当 run 内寻址用的东西
> 当成了跨 Run 的身份。**

[待交叉] 有一家甚至也写了「剥掉运行时段拿稳定路径」的函数（跟我们剥 hop/round 同一个动作），
**但它用在「从外部寻址子图 state」，从不用作消息归属**。

### 实测：比 godoc 描述的更糟 [待交叉]

调研跑了三条：

| 场景 | 结果 |
|---|---|
| 同一 agent 实例挂 `draft`/`polish` 两步 | 归属分别是 `draft`/`polish`，两份历史隔离——**要保的能力确实存在** |
| step 名 `draft`→`drafting`，agent 名一字未改 | prior 从 3 条掉到 **1 条**，无 error 无 warn |
| agent 名改，结构不动 | prior 完整读回 |

第 2 条**比我们 godoc 写的还糟一点**：不只是「读不回」，是**库里长出两条互不可见的流，
而且会持续累积**。

### (a) 的真实代价，比我第一轮说的大得多

我第一轮写「等于只删比较用的那一半」——**错了**。

`HandoffHistoryMode` 的定义就是「自己的原样 / 别人的隐藏或改写」，**没有归属比较就没有
「别人」**。删掉之后 `SelfOnly`/`Rewrite` 在历史层整体失效，而 events 回落路径还留着三档
投影 ⟹ **开 History 和不开 History 的可见性语义分叉**。那是新的静默不一致，正好是本文档
全篇在治的病。

### (b) 的落地面积

**唯一的选择点是 `historyOwner`**（`history_slot.go`）：改成读 `config.HistoryScope`、
回落 `config.Name`，读写两侧自动跟上。`WithHistorySlot` 那套 ctx 机制**不用删**——
`settleHistorySlot` 的「这个位置有没有人写过」是 per-Run 簿记，那个用途本来就是位置的、
正确的，只是不再兼任归属。

新 API 两个：

```go
// 显式声明这个 agent 的历史作用域。不配时回落 Agent 名（WithHistory 已强制 WithName，
// 所以回落值总是业务写死的字符串，不会是自动生成名）。
func WithHistoryScope(scope string) Option

// 迁移用：读的时候额外认这些旧作用域，写恒用主作用域。
func WithHistoryScopeAliases(old ...string) Option
```

**顺带修一个现存不一致** [待交叉]：events 回落路径按 `config.Name` 筛，history 路径按位置
筛——**同名 agent 在两个位置时，开不开 History 分区结果不同**。(b) 让两条路用同一个键，
这个洞自动闭合。

### 配套三件（缺了就是把老问题换个地方复发）

1. 回落必须是「业务声明过的名字」——**已满足**（`WithHistory` 构造期强制 `WithName`）
2. **同 scope 被两个不同 agent 认领时要响**：写入器加一张 `scope → agent 指针` 表，运行期
   首次冲突报错。这是把今天「静默合并」那一档彻底关掉的唯一手段
3. `WithHistoryScopeAliases` **从第一版就要有**——没有它，(b) 只是把「改结构丢历史」换成
   「改常量丢历史」

⚠️ **诚实标注**：alias 这条**没有先例背书**。五家里没有一家有归属键的迁移/版本化机制——
一家是拒绝（改了拓扑就 rebuild），一家直接在 ADR 里接受了 orphan。alias 成立的前提
（键是单个业务字符串、可枚举旧值）在 (b) 下天然满足，在今天的位置路径下不满足。

---

## 拍板点 4 · 共享历史转交 —— 结论：**砍一半（删 Raw），不砍 handoff**

### 先修正第一轮的说法

第一轮我写「eino 在四个 API 上挂 NOT RECOMMENDED」。[待交叉] 实际是 **29 处、8 个文件**，
而且：

- **连坐了它的 Sequential/Parallel/Loop**——那三个在那边**就是**建在 transfer 上的（每步
  传 nil input，子 agent 从共享 session 重建全量改写历史）
- 劝退语被贴到了跟 transfer **无关**的字段上（退出工具、输出键）

⟹ **这是批量导流标注，不是对 handoff 逐条做的判断。** 而**我们的 Sequential/Parallel/Loop
各自独立编译成图、传显式派生 input，跟 handoff 零共享机制**——那 8 处 workflow 劝退在我们
这儿根本不适用。

### 劝退理由要分档看

| 档 | 证据 | 强度 |
|---|---|---|
| **效果判断** | 只有一句 "has not proven to be more effective empirically"。**commit body 空、没 issue、没 eval、没设计文档** | 弱——这是产品导流，按纪律不该跟 |
| **实现判断** | 那家明说**不把历史改写移植到新消息模型**（跟旧模型的 role 语义绑死，迁移成本不值）；另一家把「嵌套转交历史」默认**开了又关回去**（"disabled by default while we stabilize"），handoff 目录 31 commit 里 **14 个 fix** | 强 |

⟹ **「历史怎么给后接看」这件事，业内没有一家做稳；劝退的和不劝退的踩同一个坑。**
这指向**「这一层要收窄」**，不是「整个 handoff 要砍」。

### 砍 handoff 省不到钱 [已核实]

它掏钱买的通用能力**已经被别人接手了**：`return-direct` 现在 structured output 的
`final_output` 和 RoutingAgent 的 `route_to` 都在用；`HistoryFor`（Rewrite 引擎）
`AgentAsTool` 在用；`HandoffHistoryMode` 枚举已经**不是 handoff 私有**——历史层投影和
input 去重全靠它。**砍了这三样也得留**（而且要改名，现在这个名字已经名不副实）。

### 删 Raw 的理由

- 生产 **0 引用**（`HistoryForRaw` 唯一非测试 caller 就是那个 switch 分支）
- **它在文档里的唯一存在理由已被证伪**：`handoff.md` §9 说 Raw 的语义是「转交动作在后接
  视野零痕迹」，[待交叉] 实测**开了 `WithHistory` 之后不成立**——tools 节点会把工具结果也
  记进历史，配对齐了剥不掉，Raw 也保痕迹。两档区别塌成「拍平 vs 不拍平」
- 三方**没有一家提供「保 role 原样 + 打 owner 标注」这一档**
- 删掉后组合矩阵从 3×2 降到 2×2

### 已经顺手修掉的

调研跑真实 HandoffAgent 抓 prompt 时发现的 input dup（`bd8a200`）——**store 对、prompt 错**，
所有按 store 断言的测试全绿。

---

## 拍板点 3 · 追加式 cp —— 结论：**拆开做，而且第一步跟存储无关**

### 这一路推翻了我设计里的核心判断

我写「第 4 步（追加式 cp）可独立先做，**一次消掉 4 件事**，收益/成本比最高」。逐条实测：

| 那 4 件 | 实测结论 |
|---|---|
| `NodeVisits` 能删吗 | **能，但不需要追加式**——换成 `cp.Step` 全局超步序号即可 |
| `Epoch` + 陈旧写回能删吗 | **不能，只能改名**（→ `ParentVersionID`），守卫逻辑一行不变 |
| `Status` 三档能收敛吗 | 能降到一个布尔，**但那是策略选择，不是存储布局的产物** |
| `ErrRunAborted` 主动删还要吗 | **不要了——这是 4 件里唯一真被追加式解决的** |

⟹ **字段数净变化 ≈ 0。** 我原来记的「再 3 个字段来自这一个决定」要改。

### 最有操作价值的单点发现 [待交叉]

> **cp 身份每超步换，跟落不落盘无关**——那边 `_put_checkpoint` 每超步都 mint 新 id，而
> 另一个开关才决定这次落不落盘。**「每超步换身份」和「每超步落盘」是两条正交的轴。**

这直接推翻了「`NodeVisits` 要靠追加式才能消」。⟹ **`NodeVisits` → `Step` 今天就能做，
零存储改动**，而且顺带删掉 `InterruptID()` 里那条「有 InstanceID 就把 visit 置 0」的手写
互斥规则（ISSUE-96 踩出来的补丁）。

### 我设计里那个没想清楚的洞，有答案了

我自己标过「追加式下并发写产生分叉而非覆盖，那**分叉之后谁赢、业务怎么知道**」。实测：

- **朴素追加式根本不产生分叉**，还会拼出一条**撒谎的线性链**——后写者把先写者当自己的父，
  两个副本的成果被串成「先后两步」，而它们其实都从同一份跑出来的
- 要真产生分叉，写方必须带「我从哪个 cp 载入的」——**那就是 `Epoch` 基线本身，只是改名**
- 带上之后确实分叉了，但：**谁赢 = 最后写入者；业务怎么知道 = 不知道，另一支静默留在树里**；
  而且**有副作用的节点照样跑 2 次**

⟹ 追加式**不解决**并发 Resume 的核心危害，只是把「静默覆盖」换成「静默分叉」。

### 真正的耦合方不在那 3 步里

是 `cleanupOnSuccess`：并发 Resume 都成功时，追加式下**链最终长度 = 0**（两个副本的活都被
`DeleteSubtree` 清光）——**追加式在这条路径上完全等于没做**。

### 收益重心要重新定位

不是「消字段」，是「**一次坏的写不再摧毁上一个好的恢复点**」——那才是追加式唯一无可替代的
东西，正好是 `checkpoint-storage.md` 里记的那个洞的结构性解（现在是打 round-trip 补丁）。

### 几条实测数据 [待交叉]

| | 结论 |
|---|---|
| 写次数 | 追加式**不增加**，变的只是留存。默认档下成功的 Run 写 0 次、中断的写 1 次，跟单槽一样 |
| 留存量 | 5 轮 HITL 3.5x / 20 轮 10x+（字节 O(n²)，因为消息流每轮变长）|
| 写放大 | 是 `durability` 的成本不是追加式的（8 轮 sync = 9.3x，单槽下一模一样）|
| 内容寻址 | 对我们**只省 7%**——ReAct 是一条不断变长的 LastValue；换成「12 个独立 channel 每超步只动 1 个」才省 90% |
| **接口** | **现有 8 方法对追加式透明**——实测的追加式后端原样通过全部 45 条 conformance，签名一字未改 |

### 分步（第 1 步今天就能做）

1. **`NodeVisits` → `cp.Step`**（独立、零存储改动、白赚删掉那条互斥规则）
2. `Epoch` → `VersionID`/`ParentVersionID`（守卫逻辑照搬，此时仍是单槽，行为不变）
3. 引擎侧串「载入时记下 VersionID」（6 个 LoadNS 点各存一次基线，写点已 funnel 进一处）
4. 换追加式 + tip 语义 + `PruneVersions(keepLastN=1)` 默认（**那正好退化回今天的行为**）
5. `ErrRunAborted` 路径：主动删 → append 终局 tip
6. `sql/` + `redis/` 跟进
7. 冒烟（这次改动全中「cp / 嵌套 / Resume / ns 路径」，两个 binary 都要跑）

**硬约束写进 godoc**：按数量剪只在「每份 cp 自包含全量快照」时安全。我们现在是，所以安全；
**将来若引入内容寻址/增量 channel，这条 prune 立刻变成静默截断**——而实测收益只有 7%，
所以**别做内容寻址**。

### 顺带查实的一处「有意的不对称」

`subgraph.go` 用 `Resumable()`（含失败现场）而根 auto-load 只认 `Pending`。查了语境：
**两者判的不是同一件事**——根图那个在新一次 Run 的入口（跨 Run 边界，失败现场不该被当成
挂起捡起来），子图这个已经在本次执行内部（父图正跑到子图节点，子图上次的失败点正是要
续跑的地方）。不是修漏，是缺注释，已补。

---

## 拍板点 2 · 消息 id —— 结论：**做，但按「补协议」的规格做**

### Go 侧没有那个卡死别人的 blocker [已核实]

有一家至今做不成「构造期生成 id」，唯一原因是 id 进了相等性判断（`Msg("x") != Msg("x")`）。
我们这边实测：

| 检查 | 结果 |
|---|---|
| `reflect.DeepEqual` 用在 `*schema.Message` 上 | **0 处** |
| `==` 比较 Message 值 | **编译不过**（含 slice + map）|
| golden JSON 断言 | 无 |

[待交叉] 调研在 `git archive` 出的**完整仓库副本**上真加了字段 + 构造期生成，`go test ./...`
**28 包全绿**，子模块也全 build OK。**这个窗口是免费的，而且越晚做越贵。**

### 三条已经存在的账（不是脑补的未来需求）

1. **我们已经手搓了一个**：`rootInputNonceAnnotation` 就是消息级 id，只服务转交去重一个
   场景。它的注释把理由讲得很清楚——「状态编码进消息本身，扫消息判断，不查旁边的 KV」。
   有了正式字段，那套能整个删掉。
2. **agui 现在拿 `run_id` 冒充 `messageId`** [已核实]（`adapter.go` 注释原话），工具结果压根
   没有、退回整个 run id。**AG-UI 和 A2A 两个外部协议都要求消息级 id——这是协议要求。**
3. **三处对号今天做不到** [待交叉]：实测 cp / 历史 / events 三处**零共同键**——`run_id` 只在
   模型产出上，而且每次 LLM 调用（含 ReactiveCompact retry）都重新生成，不是消息身份。

### 最大的坑：并发（实测有 race）

`parallel_builder.go` 的 `parAgentOriginalCh` 注释自己写着「原始 input（**所有分支共享**）」
[已核实]。就地补 id → 实测 `-race` 触发 DATA RACE。

⟹ **必须 COW**。而我们自己代码里早就写过这条警告（`basic_agent.go` 的
`generateWithRunID`：「不就地 mutate 模型返回的实例……并发分支下会 concurrent map write」）。

**COW 又带来第二个坑**：caller 手搓 `&schema.Message{}` 时若只有叶子补齐，六个分支各 COW
各的 → 实测**同一条消息裂成 7 个身份**。⟹ **9 个编排入口一个都不能漏**（正是 CLAUDE.md
「协议变更四复证」那类坑）。

### 明确不能高估的三条

- **不给去重能力。** 我们的历史是 append-only，那条铁律不能因为有了 id 就松。[待交叉]
  有一家有 id 但**明确不拿它给带 role 的消息做去重**，落到内容指纹——**ID 是标识不是主键**。
  ⟹ conformance 要**补一条显式反例**：同 ID 的两条 Append 进去必须留两条。否则实现者看到
  id 就会照 upsert 的直觉做。
- **不解决 events 缺中间消息**（events 只记 input + exit）。ID 让两边能对上号，但 events 里
  本来就没有 assistant tool_call 和 tool result。
- **不解决业务手搓消息**：`&schema.Message{}` 直接构造的只能靠框架边界补齐。

### 成本

3 个文件约 15 行（字段 + `NewMessageID` + `EnsureID` COW）+ 9 个编排入口各一行 + 产出侧一处
（**必须在 `OnModelEnd` 之后**——那是 [消费] 档，hook 可以整条换掉）。

另有 **14 个**浅拷贝点要逐个定性（「同一条」保 ID / 「派生」换新），其中 `ensureRootInputNonce`
**整个删掉**改判 `ID`。

⚠️ **我第一版只列了 11 个，漏了 3 个，而且两类都有**——`loop_builder.go:287` 与
`handoff_builder.go:300` 都是 `cleared := *msg`（清 annotation，语义上**必须保 ID**）、
`plan_agent.go:850` 是 `prefixed := *input`（plan worker 的派生输入，**必须换新**）。
漏项不是同质的，正是「协议变更四复证」那类坑。

---

# 最终实施顺序

> ⛔ **本节已作废（2026-07-28），保留只作决策溯源，别照它实施** —— 四条链路审计推翻了它的核心
> 判断（把病因诊断成「写入器有多个」）。以文末「**方案第二版**」为准。

四个拍板点都有结论了。按「零风险 → 必须一起做 → 可选」重排：

## 第 0 步 · 今天就能做（各自独立，都经实测）

| 做什么 | 为什么零风险 |
|---|---|
| **`NodeVisits` → `cp.Step`** | 「cp 身份每超步换 ≠ 每超步落盘」——不需要追加式。顺带删掉 `InterruptID()` 里那条手写互斥规则 |
| **`schema.Message` 加 `ID` + 构造期生成** | 实测 28 包全绿、wire 不漏、cp round-trip 保得住。立刻让 agui/a2a 的 messageId 有正主 |
| ~~退化方向翻过来~~ **已撤**，换成 **`Restore` 期 fail-fast 校验** | ⚠️ 见下方「撤回记录」——那条不成立，而且代码里早有对它的否决 |
| **第 0 层截断改按组切** | 我们自己第 2/3 层早就按组切了，消掉 86 行切点修正 helper |

## 第 1 步 · 写入所有权（**四件必须一起**）

1. `WithHistory` 从**叶子构造期** → **根对象持有 + 运行期继承 + 显式弃权三态**
   ⚠️ **不是「挪到运行入口」**——见下方「立论修正」
2. 写入器首达者持有，**零遍历**
3. 归属改盖章（R1/R2/R3）+ `WithHistoryScope` / `WithHistoryScopeAliases`
4. **消息 id 的编排入口档**

**第 4 件是新发现的交叉**：它跟前三件改的**是同一批 9 个编排入口**——一起做省一次协议
对账，分开做就是两次「四复证」的风险。

做完可以下线 `rootInputNonceAnnotation`（被 `Message.ID` 取代）。

## 第 2 步 · 收窄（独立）

删 Raw 视图（生产 0 引用；它在文档里的唯一存在理由已被实测证伪）。

## 第 3 步 · cp 结构（最大，可选）

### 3a · 甲（**热点第一名，≥12 轮**）—— 我写最终顺序时把它弄丢了

第一轮的第 3 档有四项，前三项（`Send` 降级成 channel 写 / 实例身份 = payload 下标 /
6 个 frontier 字段换写日志）**正是诊断甲的全部内容**。我收口成「最终实施顺序」时只留了
第四项，**甲的三项无声消失，还没写任何一句「为什么不做」**——而甲是 ⑥ 路热点表里 ≥12 轮
的第一名，是第二名的两倍多。「17 → 约 9 个字段、6 处约束 → 0 处」这个被写了两次的卖点，
在丢掉甲之后已经不可能兑现。

**最小切口**（不必一次做完三项）：把 `DeferredSends` / `PendingSendWrites` / `Owner` /
`OwnerInstanceID` / `FrontierInstanceIDs` / `CompletedInstanceIDs` 这一组收敛成**一张按
`(nodeID, instanceID)` 键的记录**——它们本质是同一件东西：「成功成员的 writes 被合并抹平
之后的唯一副本」。用主键取代下标对应，正是那条「把约束从两个并行数组按下标对应，搬到
日志行的主键」。

改动面比「6 个字段 → 写日志」小得多，而收益是 **2 处下标对应约束归零** + 6 处「漏了不报错」里的 4-5 处，外加：
`Owner` purge 天然实现、`restoredMembers` 那条长度不匹配的降级路径整个消失、顺手闭掉
`todo.md` 里「静态 fan-out 错误选取跨次不稳定」（写日志要求确定序）。

### 3b · 乙

`Epoch` → `VersionID`/`ParentVersionID` → 追加式 + tip + `PruneVersions` →
`ErrRunAborted` 改 append 终局 tip → backend 跟进。

**收益重心不是「消字段」（实测净变化 ≈ 0），是「一次坏的写不再摧毁上一个好的恢复点」。**
⚠️ 但 **`keepLastN=1` 这个默认值恰好把这条收益抵消掉**——坏写 append 成新 tip，剪枝把上
一个好的恢复点删了。要拿到收益必须 `≥2`，而那要付「5 轮 3.5x / 20 轮 10x+」的留存代价。
剪枝**时机**（每次写后 / 成功出口 / 定期）也还没定，而它直接决定坏写之后旧版本还在不在。

**别做内容寻址**（实测只省 7%，且会让按数量剪的 prune 变成静默截断）。

---

## ⚠️ 撤回记录：「退化方向翻过来」不成立

第一轮我把它列为第 0 档的零风险项，承诺「能把 5 个静默出错全部转成多跑一次」。**错了，
而且错在一个我本该查出来的地方。**

**它借的是一个不同提交模型的经验。** 那条实测（漏字段只是白干活）成立的前提是**writes
留在写日志里、从不合进 channel**。我们不是 [已核实]：`graph_loop.go:856` 先
`cm.ApplyAll(res.SuccessWrites)`，**成功之后** `:888` 才 `appendCompletedSiblings`。成功
成员的 writes **已经落进 `cp.Channels`** 了。

具体失败场景：

> 图里有个 `Append` channel。超步 N：成员 A 成功写 `"a"` → 已 `ApplyAll` → cp 里
> `Channels=["a"]` + `CompletedFrontier=[A]`；成员 B 中断。业务 backend 漏存
> `CompletedFrontier`。
>
> - **今天**：A 的出边不贡献 → 后继静默永不执行（已知的静默出错）
> - **翻过来之后**：解释成「A 没干过」→ 重跑 A → 再 `ApplyAll(["a"])` → `["a","a"]`，
>   **结果错**，且 A 的副作用（LLM 调用 / 工具 / 扣款）跑两遍

从「静默丢一条分支」换成「**静默双计 + 副作用双跑**」，不是「只多干活」。

**更要命的是：这个选项代码里早就被评估并否决过。** `graph_loop.go:977` 原文——
「产 send 的节点已成功不重跑（**R27a 拒绝重跑：visit 漂移 + 副作用双跑**），payload 无从
再生 → Resume 时 target 静默读到零值。故把这批被回滚的 writes 存档」。**`PendingSendWrites`
这个字段就是因为「重跑」被否决才存在的。** 我把它写成「当时只在两个坏选项里选，还有第三
个没想到」——不是没想到，是想过并写下了否决理由，而我没查就下了结论。

**它也不是「零风险第 0 档项」**：要让「这活没干」成为可判定状态，前提是 writes 不合并
——那正是**甲**。⟹ 它依赖被我弄丢的那一档。

**第 0 步的安全替代**：把静默降级改成 **fail-fast**（缺字段直接报错），而不是改成重跑。
那才真是「只改降级分支」，而且顺手闭掉 `todo.md` 里 `Graph.ID()` 不哈希拓扑那条。


---

# 独立审计的修正记录（三位审计，本轮）

方案定稿后派了三位独立审计：**自有代码断言**（逐条 grep 复核，数字自己数一遍）、
**外部实现对照**、**方案推理本身**。抓到的错误按影响排：

## 影响决策的

| # | 我原来写的 | 实际 | 影响 |
|---|---|---|---|
| 1 | 最终实施顺序第 3 步 | **甲（热点第一名 ≥12 轮）的三项被我弄丢了**，还没写「为什么不做」 | 已放回，见 3a |
| 2 | 「退化方向翻过来」是零风险第 0 档项 | **不成立**：借了另一个提交模型的经验，而我们先 `ApplyAll` 再记 completedSiblings；且代码里早有否决记录（`graph_loop.go:977`「R27a 拒绝重跑：visit 漂移 + 副作用双跑」）| 已撤，换成 `Restore` 期 fail-fast |
| 3 | 「6 处下标对应约束 → 0 处」 | **只有 2 处**——我把「6 处隐式约束」和「6 处下标约束」当成同一个数 | 最贵那档的收益虚报 3 倍，已改 |
| 4 | 「11 个浅拷贝点要定性」 | **14 个**，漏的 3 个还分属两类（2 个必须保 ID、1 个必须换新）| 实施清单不全，已补 |
| 5 | 「九个编排全都强制设 name」 | **错**：name 有自动默认值；且 **BasicAgent 的图根本不设 name、恒走 fingerprint** | 这句已扩散进 `todo.md`，两处都已改；`Graph.ID()` 那条 backlog 的危害分布要重估 |
| 6 | 「`ns.go` 是唯一 0 fix 的状态相关文件」 | **还有 4 个**（`ns_ctx.go` / `soft_cancel.go` / `store_scope_ctx.go` / `store/store.go`）| 「唯一」垮了，但 5 个共享同一特征**反而加强**论证，已改写 |

## 方案层面还没处理的（下一轮）

- **三条诊断只覆盖 §9 九条里的 2 条**。九条真正的共同点是 §9 自己写的那句「多个写入者需要
  互相协调」，而**「多写入者」不在甲/乙/丙里**——诊断表缺第四条：「配置挂在叶子、资源必须
  活在根 ⟹ 一件事有多个所有者」。
- **拍板点 1 选的 (b) 对共享实例做不到它要保的能力**：`HistoryScope` 是 agent 构造期 option，
  同一个 `*BasicAgent` 指针挂两个 step 只能带一个值 ⟹ 两条历史合并。而「同实例挂多分支」是
  ISSUE-80 特意恢复的 best-of-N 能力。且 `settleHistorySlot` 拿不到 scope，外部 agent 会退回
  按 `Name()` 归属 ⟹ §9 #1 复发。
- **第 1 步的「所有编排都有的 option」比第 ① 路原案（ctx 声明）弱**：只在叶子配时
  `Sequential(远程, 本地)` 的远程那段仍然什么都不记（§9 #2 原样），「零遍历」是把责任推给
  业务换来的。
- **§9 #8（认领成功后写失败 → 彻底没有）方案之后冗余更少了**，文档只列了消掉的四条，没有
  残余清单。
- **`keepLastN=1` 抵消第 3 步自陈的唯一收益**（坏写 append 成新 tip、剪枝删掉上一个好的）。
- **三个没派的调研维度里，并发/隔离 和 流式/事件投递 需要补**——前者是热点第 3 名（7 轮 fix、
  自陈防不住）而全案零覆盖；后者是因为第 1 步一句话收窄了 events 语义，而底下压着 `Agent.Run`
  的返回类型 + 两个外部协议适配层 + 「prior 回落 events」这一档的去留。

## 计数微调（不影响方向）

annotation key 18 → **21**；「静默」122 → **119**（顶层）/ **132**（含子包）；「四组并行数组」→
**2 组并行数组 + 2 个结构体字段**；`buildInitialMessages` 里「剩下约 100 行」→ **160 行**
（393−233，我算错了）；「6 个 LoadNS 点」→ **7 个调用点 / 5 个真载入点**。


---

# 立论修正：第 1 档那句「五家全在运行入口」是错的

## 错在哪 [已核实]

我写「五家全都在运行入口（`Runner.run(session=)` / `compile(checkpointer=)` / …），
只有我们是构造期 option」——**这句话自己就自相矛盾**：`compile()` 恰恰是构造期。

逐条核实下来有三个反例：一家的 checkpointer 在 `compile(checkpointer=None, ...)`（构造期）；
一家的历史 provider 挂在**叶子 agent 的构造参数**上（比我们还靠叶子）；一家的
`as_tool(session=...)` 也是构造期绑定。

**真正的共性是另一根轴**：

| 我原来说的轴 | 实际的轴 |
|---|---|
| 运行期 vs 构造期 | **根/入口对象持有 vs 叶子持有** + **继承与弃权的三态** |

会话**身份**（thread_id / session_id）确实一律在运行入口给——但会话**存储**不是。
而「没有一家把 enable-flag 放叶子再去搜」这条**仍然成立**，它才是我们的病根。

## 对方案的影响：结论不变，动作要改

模型 B 依然是对的，而且**有一份完整可抄的参考实现** [已核实]：

```
compile(checkpointer=None)   → None = 当子图时继承父的；False = 完全弃权
运行期                        → CONFIG_KEY_CHECKPOINTER: (自己的 or 继承父的)
连子图扫描都尊重弃权            → "subgraphs that disabled checkpointing are not considered"
```

⟹ 第 1 步 item 1 的动作从「**挪到运行入口**」改成「**挪到根对象 + 三态继承**」。
两者的差别很实：前者要动 9 个 `Run` 签名或改用 ctx，后者只要让 option 可以挂在任意
一层、加一个「继承 / 弃权」的三态，**而 `WithHistory` 本来就是 option 形态**。

**这条如果不改，我会照着一个不存在的共识去改 API。**

---

# 审计发现的第三条路（我整份文档都没列过）

诊断句我写的是「把约束从**两个并行数组按下标对应**，搬到**日志行的主键**」。审计指出中间
还有一档，而且便宜得多：

> **搬到内容派生的摘要，让它自失效。**

[待交叉] 有一家的跨记录链接不是下标、也不是主键，而是**一段由内容算出来的 marker**
（`current_turn + last_response_id + 序列化的 new_items`）——内容一变，marker 自动对不上，
消费时重算再比，不匹配就回落到按内容去重。

**对我们直接适用**：`CompletedFrontier` / `FrontierInstanceIDs` 这几组并行数组的问题是
「下标对应没法自校验」。换成内容派生的 marker 之后，长度不匹配 / 内容漂移**当场可检测**，
而**不需要重写超步提交路径**（那是第 3 档甲的成本）。

⟹ 这是介于「什么都不做」和「重写成写日志」之间的一档，成本低一个数量级。
**下一轮设计要把它作为甲的低成本替代方案评估。**

---

# 其余待处理（审计给的，本轮未动）

| # | 事项 |
|---|---|
| 1 | **「漏字段 = 白干活」不能当安全网**：那边的 writes 表同时装 HITL 答案和 error-handler 标记，漏了是**答案丢失反复重问**；而且它在人家 conformance 里是**必需能力**不是可降级项。⟹ 真正可抄的是**把关键字段设成 conformance 必需项，让漏存在测试期就炸** |
| 2 | **第 ⑤ 路「那家没有配对感知/没有切点」是反向错的**：它有两套压缩，我描述的是另一套。有切点的那套**保留边界就是对齐 tool_call 轮边界的**，还有按 CallID 配对——那是一份可直接对照的实现。连带 `context-compaction.md §11` 那条「实测没有」也要改回 |
| 3 | **「我们是唯一存了版本号却没有版本行为的」不成立**——另一家同样。且那家的真闸门是**拓扑 hash**，即「拓扑校验」与「版本读取」在那边是**替代**不是叠加 |
| 4 | **`NodeVisits → Step` 的引用要换一行**：「每超步换 id」有个例外（退出那次复用 id）；真正干净的依据是 `step` 本来就是独立于 cp id 的超步序号 |
| 5 | **那条「他们也这么补的」不能照抄**：是 feat 不是 fix、只对外部中断生效、且键是 nodeKey——**他们没有同节点多实例，我们有**，照抄键形状会正面回踩 ISSUE-96 |
| 6 | **「唯一 / 零先例」措辞要删**（第二轮已改对的措辞留着）；另一家的分流键默认是常量，跟 agent 名无关 |
| 7 | **人家的 checkpointer conformance 区分必需/可选能力**，`prune` 是一等方法且带独立 conformance（含「keep_latest 必须连 writes 一起保」——我们大概率会漏这条） |
| 8 | **拍板点 3 那条「硬约束」已经有人逐字写过**，还给了三种安全做法（回溯保留祖先 / 剪之前强制快照 / 干脆不剪）。我标成「我们自己推出来的」，实际可以直接引用 |
| 9 | **fork 语义有现成答案**：有显式的分叉来源标记和 `copy_thread` 能力——「如何表示一次有意的分支」被实现过，不是空白 |
| 10 | **`Send` → Topic channel 那次改造带了两个附带不变量**（旧数据迁移 + 父指针恒指向已保存的 cp），我把成本估成「重写 + schema 变更 + backend 失效」，**漏了这类改完才发现的不变量** |
| 11 | **拍板点 4 缺一个反向检查**：没查「其他家是否也在劝退 handoff 本身」。实际是——**不稳的只有「历史怎么给后接看」这一层**，那正是我的结论，但支撑它的关键反证没写进去 |
| 12 | **④ 路漏了唯一一个跟我们 `InstanceID` 同构的对照物**：那家的地址段里有个专门的「同名多实例额外身份」字段，注释写着「有些情况下光有 ID 不够唯一……比如同名但 tool call id 不同的并行工具调用」。⟹ **我们的 `InstanceID` 不是自造复杂度，是同一个问题的同一种解** |

---

# 改动交汇点：为什么这套改法天生会「修 a 出 b」

四位审计各审一条链路，看不到这个——**它只在把所有改动叠到同一张代码图上时才显形**。

## `buildInitialMessages` 一个函数里挤了四个方案改动，而且它们共享同一条数据流

```
:780  prior, fromHistory := historyPrior(ctx, …, historyOwner(ctx, name), mode)
                                                 └─ 拍板点 1（scope 换归属键）改这里
:798  priorForDedup := prior          ← 截断前的快照
:837  prior = truncatePriorPreservingMarkers(prior, MaxHistoryPairs*2)
                └─ 第 0 步「按组切」改这里
:866  alreadyInPrior := fromHistory && priorHasRootInputNonce(priorForDedup, …)
                                                              └─ 拍板点 2（nonce → Message.ID）改这里
:908  if … || claimSharedRootInput(ctx, priorForDedup, input) {
:909      recordHistory(ctx, historyOwner(ctx, name), name, …)
              └─ 第 1 步（盖章）改这里     └─ 拍板点 1 又改这里
```

**`priorForDedup` 这一行是踩过坑之后加的**，它的注释把不变量写得很清楚：

> 投影/截断后的 prior **不能**用来回答「这条输入落进历史没有」——那是持久化状态。拿投影
> 回答持久化问题会误判：带 nonce 的那条 user 消息一旦被削掉，去重就以为没记过、于是再记
> 一份，而历史 append-only，这份重复**永久留存**。

于是四个改动**互为对方的输入**：

| 改动 | 它改了什么 | 谁的输入因此变了 |
|---|---|---|
| 拍板点 1（scope） | `historyOwner` → `prior` 的**内容**（按 scope 投影而非按位置） | `priorForDedup` → `:866` 和 `:908` 两处判据的**基础**变了 |
| 第 0 步（按组切） | `:837` 的截断粒度 | `prior`；`priorForDedup` 名义上不受影响（它在截断前），**但「什么算一组」变了之后压缩器行为也变** |
| 拍板点 2（nonce→ID） | `:866` 的判据本身 | —— |
| 第 1 步（盖章） | `:908/:909` 的写入决策 | —— |

**这就是「修 a 出 b」的结构性来源**：不是运气差，是四个改动落在同一条已经很紧的数据流上，
而这条流的不变量只写在一句注释里、没有任何测试守着。

⟹ **动这个函数之前必须先把这条数据流的不变量提取成显式契约 + 测试**，否则「四件必须一起
做」也不够——一起做只保证不漏，不保证不互相破坏。

## `recordHistory` 有 4 个生产调用点，而 R1 字面上要删掉其中三个

```
basic_agent.go:909      preamble 记 input
basic_builder.go:425    model 节点记产出
basic_builder.go:554    tools 节点记工具结果      ← 历史「逐字完整含工具往返」全靠这三个
history_slot.go:169     编排边界补记
```

R1 说「**执行体只产出、不记录；记录只发生在包住它的那一层**」。前三个**都在 BasicAgent
内部**。字面执行 R1 = 删掉它们 = 历史退回「只记首尾」的粒度——而那正是 `prior` 换源到历史层
要解决的问题（events 只记首尾 ⟹ 结论在、来源没了）。

⟹ **R1 必须先落到具体调用点**：BasicAgent 到底算「执行体」还是「包住节点的那一层」。
这个问题不回答，第 1 步没法动手。

## 相比之下 `InterruptID()` 那个交汇点是干净的

`interrupt.go:717-728` 六个维度里，方案只有 `NodeVisits → Step` 碰 `visit` 那一维（连带删
`InstanceID != "" → visit = 0` 那条互斥规则）。`Message` 加 ID 不碰它。

**单一改动、锚点集中**——这也解释了为什么它可以独立先做，而 `buildInitialMessages` 那四件
不行。

---

# 第 0 步的「零风险」标签是错的 —— 四件全部不成立

审计把方案叠到**测试网**上看，结论很直接：**第 0 步没有一件是零风险的。**

| 第 0 步四件 | 实际 |
|---|---|
| ~~退化方向翻过来~~ | 上一轮已撤（借错提交模型 + 代码里早有否决记录）|
| **`Restore` 期 fail-fast（SchemaVer）** | ⚠️ **在推翻一个已经拍过板的决策，而我不知道** |
| **第 0 层截断改按组切** | ⚠️ 唯一的白盒单测**改完必然绿**（假绿），且依赖的函数**零单测** |
| **`NodeVisits` → `cp.Step`** | ⚠️ 动 HITL 的锚点，而 `InterruptID()` 这个哈希函数**零直接单测** |
| **`Message` 加 ID + 构造期生成** | ⚠️ 会让 conformance 里那条「逐字重复都得留」**造不出反例** |

## SchemaVer 那条：我在推翻既有决策 [已核实]

`compose_v2/checkpointer_conformance/schema_ver_test.go:18-20` 的 sanity-revert 注释逐字写着：

> **sanity-revert 验证**：把 `UnmarshalCheckpoint` 加回 SchemaVer fail-fast 检查 → 此测试
> fail（默认 0 != 6 直接 Unmarshal 报错）。

项目在 iter-5-H 已经拍板走「**不** fail-fast」，理由写在同一段：业务 ad-hoc `&Checkpoint{}`
（默认 `SchemaVer=0`）要能走通完整 round-trip、**业务零负担**。

⟹ 第 0 步那条不是「补一个空白」，是**推翻一个带理由的既有决策**。要做得先回答：那个理由
今天还成不成立？（业务自研 backend 现在有 redis/ 和 sql/ 两个实现，ad-hoc 构造的场景还在吗）

**而且爆炸半径取决于落点，方案没写清**：落 `cm.Restore` 则 conformance 不受影响；落
`UnmarshalCheckpoint` 则 45 条经 `wire_test.go` **全红**（`conformance.go:119-125` 的
`newCheckpoint` 故意不填 SchemaVer）。

## 按组切那条：唯一的网是假绿 [已核实]

`agent_v2/maxhistory_marker_internal_test.go:19-22` 的 fixture 是 **1 条 system + 50 条清一色
user**。而按 API 轮分组时**每条 user 各自成组** ⟹ 51 组、每组 1 条 ⟹ **按组切 ≡ 按条切**，
三个断言数字一个都不会变。

这是标准的「过滤后元素恰好原位」——`CLAUDE.md` 列的五类假绿之一。

更糟的是：`compose_v2/` 下**没有 `message_group_test.go`** [已核实]。改成按组切之后，截断的
全部正确性押在 `GroupMessagesByAPIRound` 上，**而那个函数零单测**。

## 结论：第 0 步要重新定义

原来的想法是「先做几件零风险的止血」。实际上**这一档里没有零风险的东西**——它们都动在
「没有测试守着的不变量」上，而那正是本文档全篇在诊断的病（33 条约束里 16 条纯约定、类型
强制 0 条）。

⟹ **第 0 步应该改成「先补网，再动」**：
1. `InterruptID()` 的 6 维两两 distinctness 穷举单测（现在零直接单测）
2. `GroupMessagesByAPIRound` 的单测（按组切的前提）
3. conformance 补「同 ID 两条 Append 必须留两条」的**显式反例**（构造期生成的 ID 造不出这个形状，必须手工赋值）
4. `testAppendNoMutation` 的 shape 快照加 `id` 字段（一行）

这四条**本身零风险**（纯加测试），而且做完之后上面那四件改动才有验收依据。

---

# 跨 Run 链路审计：拍板点 2 的立论恰恰是它自己的杀手

## 最致命的一条：ID 跨 Run 撞一次，模型从此看不到当前问题

判据在 `basic_agent.go:866`——**这一行没有 mode 门**：

```go
alreadyInPrior := fromHistory && priorHasRootInputNonce(priorForDedup, rootInputNonce(input))
```

**今天无害纯粹因为非转交场景 input 根本没有 nonce**（`priorHasRootInputNonce` 里那句
`if nonce == "" { return false }` 无条件放行）。而 `Message.ID` 是**普遍的** ⟹ 放行没了 ⟹
判据对**每个 Run** 都生效。

实测（单 BasicAgent、SelfOnly、非转交，把 ID 模拟成同值）：

| | 用户问 | 模型看到 | 历史库 |
|---|---|---|---|
| Run 1 | 问题A | `[问题A]` ✓ | 问题A, ok |
| Run 2 | 问题B | `[问题A, ok]` — **问题B 不在** | +问题B, ok |
| Run 3 | 问题C | `[问题A, ok, 问题B, ok]` — **错位一轮** | +问题C, ok |

**store 全程是对的**——又是 `bd8a200` 那个形状：错的不是存储，是喂给模型的那份，所有按
store 断言的测试全绿。

### 三个真实的撞法，其中一个就是拍板点 2 的立论本身

1. **业务复用同一个消息对象**。今天安全靠的是 `ensureRootInputNonce` 返**拷贝**——调用方
   那个对象永远拿不到 nonce，所以每个 Run 都铸新的。**构造期生成 ID 把这层拷贝保护直接拆掉。**
2. **wire 带的 messageId** —— 这恰恰是拍板点 2 的**全部立论**（「两个外部协议都要求消息级
   id，这是协议要求」）。幂等重发 / 客户端重试 / UI 按 thread 复用 id，全都产生同 ID。
3. 业务在边界按内容算确定性 id。

⟹ **拍板点 2 的立论和它的杀手是同一件事。**

### 还有一个反向的坑

`&schema.Message{...}` 字面量（方案承认覆盖不到）ID 为空 → 逃生口还在 → **安全**；
`schema.UserMessage(...)`（**推荐路径**）拿到构造期 ID → **危险**。同样的业务逻辑，因构造
位置不同行为不同。

## SelfOnly「每分支各记一份」确实被误杀，杀在我没盯的那条路

- **BasicAgent 那条不受影响**：`:907` 有 `shared := mode != SelfOnly` 短路
- **`settleHistorySlot` 那条全军覆没**：`history_slot.go:160` 的 `inputAlreadyInHistory`
  **完全没有 mode 门**，而它是**外部执行体的唯一记录点**

实测（Parallel SelfOnly + 1 BasicAgent + 3 外部 agent，2 个 Run）：

| | 历史条数 | 各位置名下的 user 条数 |
|---|---|---|
| 现状 | 16 | `{br1:2, br2:2, br3:2, cfg:2}` — 每条分支流有问有答 |
| 换 ID | 10 | `{cfg:2}` — **br1/br2/br3 一条 user 都没有** |

那三个位置的流变成**无头的**（只有「我说了 X」，没有问题）。

## 文档自相矛盾（我早该发现）

同一份文档里四句话，两两互斥：

| 位置 | 说法 |
|---|---|
| 第 2 档 | 「要重新定义成 **append-only 但同 id 幂等**」 |
| 拍板点 2 | 「**不给去重能力**……ID 是标识不是主键」 |
| 第 1 步 | 「`claimRootInput` 那套 CAS **可以删**」（= 删判据靠盖章）|
| 拍板点 2 | 「`ensureRootInputNonce` **整个删掉改判 ID**」（= 留判据换键）|

**上面两条实测只在「留判据换键」这个读法下成立**，而文档同时背书了两种。
⟹ **这个先定，再谈加不加 ID 字段**——它决定 Run 2 里用户的新问题在不在 prompt 里，
而且**没有任何现有测试会红**。

## 其余（按严重度）

| # | 事项 |
|---|---|
| 1 | **`WithHistoryScopeAliases` 是永久的**：`HistoryStore` 只有 `Append`+`Load`，**没有删、没有改写** ⟹ 旧消息的章永远改不了。「跑几轮能删」的真判据是 per-conversation 的消息数（默认约 10 个带工具的 Run），业务算不出来；带 skill marker 的消息进 `protected` 不占预算，能让它**永远算不出来** |
| 2 | **alias 和 scope 共用字符串空间，而冲突守卫只管写侧**——alias 从不写、进不了那张表。X 用 alias 认领 Y **正在用**的 scope ⟹ 静默把别人的流当自己的读回来（**污染档**）|
| 3 | **归属键和「谁该写」的章必须同键，(b) 让它们分叉**：R2 比**位置**（分发那层只知道位置），读回比 **scope**。实测同一实例挂 `draft`/`polish` 两步时，Run 2 里 draft 把 polish 说过的话当成自己说过的——**不是「两条历史合并」这么轻，是污染** |
| 4 | **`settleHistorySlot` 拿不到 scope**（外部执行体连 provider 都没实现）⟹ 一条会话里**一半 scope 键、一半位置键** |
| 5 | **Run 1 中断没 Resume、Run 2 用同 sid → 新输入静默蒸发**（现存）：9 编排没有一个传 `WithFreshRun`，Run 和 Resume 在 auto-load 眼里没区别。实测新问题既没进历史也没进 prompt，**一条日志都没有**。第 0 步的 `Restore` fail-fast **抓不到**——那条路上 Restore 是成功的 |
| 6 | **prior 无界增长**（现存）：带 `SkillBodyAnnotation` 的消息进 `protected` **不占预算** ⟹ `prior = 2N + 40`，**无天花板**（实测 60 Run → 模型看到 160 条，名义上限 40）|
| 7 | **「按组切」的组 ≠ 一个 Run 也 ≠ 一个对话轮，是一次 LLM 调用**（`run_id` 每次调用换）。实测 3 个 Run 切出 **6 组**。闸门从「有界 prompt / 不定轮数」变成「**有界轮数 / 无界 prompt**」——对一个自陈用途是「防爆 ctx」的兜底，这是**在本职上退化** |
| 8 | **`Load` 没有 limit / cursor / owner 过滤，store 没有任何留存策略**（对比 `Checkpointer.Cleanup(keepLastN)` 和 `store.Store` 的 `WithTTL`）。scope 让一条会话里的流变多，每个 Run 仍全量拉、内存里筛 |

---

# HITL Resume 链路审计：第 0 步和第 1 步各撞上一堵墙

第三条链路（一次带中断的 Run + Resume）审完。**结论跟前两条同型**：致命的不在方案要加的东西，
在方案**没看的既有洞**上——而方案的措辞会把洞固化进新架构。

## 最致命：7 个编排的 `Resume` 入口根本不建历史写入器 [已核实]

机械对账，`startHistoryForOrchestration` 的 7 个生产调用点**全部落在 `Run` 的行号区间内**：

| 编排 | 调用行 | Run 起 | Resume 起 |
|---|---|---|---|
| sequential | 184 | 162 | **255** |
| parallel | 220 | 199 | **284** |
| routing | 265 | 238 | **329** |
| loop | 164 | 147 | **226** |
| handoff | 267 | 236 | **329** |
| plan | 365 | 343 | **470** |
| reflect | 231 | 210 | **293** |

对照组 `BasicAgent` 用的是 `startHistory`，`:207`（Run）**和 `:325`（Resume）两处都建**，
`:324` 还专门写了注释「Resume 同款起写入器」。⟹ **是漏了 7 处，不是设计**。

实测 `Sequential(s0=外部执行体, s1=BasicAgent)`，s0 抛一次中断：

```
无中断基线：3 条  [user USER_Q@s0] [assistant EXT_FINAL@s0] [assistant LEAF_FINAL@s1]
中断+Resume：1 条  [assistant LEAF_FINAL@s1]      ← 用户原话 + 外部产出双双永久缺席
```

走到哪一行：`Resume` → 图重跑 s0 → `settleHistorySlot` → `historyWriterFrom(ctx)` 返
**nil**（本层没建）→ 直接 return。`LEAF_FINAL` 还在，是因为叶子 BasicAgent 在自己的
`Run` 里兜底建了个私有写入器。

**这正是 `settleHistorySlot` godoc 自陈要防的那个形状**——它在无中断路径上防住了，
中断路径上失效。

**对方案的两处直接冲击**：
- 第 1 步 item 2 原话是「写入器在**每个 `Run` 入口**做 if 没有则建」。**照字面实现，
  这个洞原样保留**。
- item 1（`WithHistory` 从叶子挪到根对象）更糟：叶子不再自带配置 ⟹ 今天那条兜底路也没了
  ⟹ **顶层 Resume 期一条历史都不产生**。今天是丢一部分，改完是全丢。

⟹ **动第 1 步之前必须先补齐 7 个 `Resume` 入口 + 加一条「Run 与 Resume 入口对称」的回归**。

**范围**：只在顶层。`outer → inner` 嵌套时 inner 是被 outer 的 node 用 `Agent.Run` 调起的，
走的是有写入器那条路（审计实测确认）。

**顺带查实的更大缺口 [已核实]**：`graph_agent.go` 里 `startHistory*` / `settleHistorySlot`
**一次都没有**——GraphAgent 在 `Run` 和 `Resume` **两个入口都没有**编排级写入器，也没有边界补记。
它没有 `subs` 概念（sub-agent 由业务 NodeFunc 经 `RunSubAgent` 调起），所以这不是「漏了一行」，
是这套机制对 GraphAgent 整体没接。审计的覆盖面里没有它。

## `settleHistorySlot` 的判据粒度是「位置」，而位置跨轮不变 [已核实机制]

`history_slot.go:155` `if slot == "" || w.slotWroteHistory(slot) { return }`。slot 在 Loop 多轮
之间、Handoff 多跳之间是**同一个值**。实测 `Sequential(s0=Loop(外部执行体, 3 轮), s1)`：

```
[0] user      s0/body  USER_Q
[1] assistant s0/body  EXT_ROUND_1
[2] user      s0       USER_Q          ← 用户原话记了两遍
[3] assistant s0       EXT_ROUND_3
[4] assistant s1       X
>>> EXT_ROUND_2 不在历史里
```

一次同时命中**丢失**（第 2 轮产出被 `slotWroteHistory` 挡掉）和**重复**（两个 slot 不同，
而 `inputAlreadyInHistory` 在无 nonce 时恒 false）。R2「章==本层」若仍是 per-layer 记一次，
**原样继承这个粒度错配**；R3 只消并发扇出的 RMW 竞态，不解决「同一层被调 N 次」。
方案说 R1/R2/R3 消掉失效清单 #4/#6 —— **这一条不在清单里**。

## 第 0 步 `NodeVisits → cp.Step`：推进点跟方案给自己写的依据互相矛盾

**事实 [已核实]**：`Checkpoint` **没有 `Step` 字段**（grep 命中的全是注释里的「Phase 5.1.a
Step N」）。最接近的两个都不能用：`graph_loop.go:776 visits := make(map[NodeID]int)` 是
**loop-local**（每次 Run/Resume 从零起，不进 cp）；`cm.visitCount` 进 cp 但语义是**节点成功
完成次数**。

实测超步序号跨 Resume 天然发散（A→B→C，C 中断）：`Run1` 里 C 在第 3 个超步，
`Resume` 里 C 在第 1 个。而 visit 恒 0（C 从没成功过）。

⟹ **朴素实现（循环顶 `step++`）必炸**：Run 存 3、Resume 恢复后自增得 4 → `InterruptID` 变
（实测 visit 0/1/3 → 三个不同 ID）→ `GetResumeAnswer` miss → **用户被重问**。
唯一安全的推进点是「一个超步**完整成功**之后」（所有失败/中断出口都在它之前 return）——
但那样它就不叫「超步序号」，而是「已完成超步数」，跟 `visitCount` 同构、只是 per-node 换 global。

⚠️ 而**撤回记录第 4 条恰恰把依据改写成「`step` 本来就是独立于 cp id 的超步序号」——
这句话直接指向错误的那个推进点**。

**失效档位反而变重**（这条最该拍板）：

| | 今天 `NodeVisits` | 换 global `Step` |
|---|---|---|
| 中断当下的值 | 中断节点从没成功过 ⟹ 恒 0 | 前面的超步已把它推高 |
| backend 漏存后读回 0 | **恰好相等** | **对不上** |
| 第 1 次中断 | 正常（实测一次问完收敛）| **重问** |

而 **BasicAgent 的 tools 中断永远在 step≥2 上** ⟹ 换成 step 后，漏存字段的业务 backend
会在**第一次 HITL 就重问**；今天要跑到第二轮才暴露。

⟹ 这项**不是「零风险 / 零存储改动」**：新 cp 字段 + Restore + conformance 断言 + 两个 binary
冒烟，字段数 1:1 换而不是净减。另：`InterruptSite` 是导出的、godoc 明确推荐业务自己调
`InterruptID()`，加字段后业务手写字面量会拿到 Step 零值。

## 第 0 步 SchemaVer fail-fast：会顶到一条**带 conformance 设计**的既有决策 [已核实]

- `serialization.go:452-455` 明写 Option D：「**不**在反序列化时做 SchemaVer mismatch
  fail-fast——业务 ad-hoc cp / 旧 cp 都能正常加载」
- `checkpointer_conformance/conformance.go` 注释写着 helper **故意不填** SchemaVer，
  «让 `SaveNS_*` 测试同时覆盖业务 ad-hoc 路径»

⟹ 加白名单只有两个出口：**要么 conformance 套自己不过**（所有业务自研 backend 受影响），
**要么白名单放行 0**（那它挡不住任何东西）。而这条的立论（「唯一存了版本号却没有版本行为」）
已在审计记录第 3 条被撤——剩下的只有「fail-fast 更好」这个偏好，不足以推翻带 conformance
设计的决策。

## 立论修正：「随机 name → Resume 撞 GraphID 冲突」不成立 [已核实]

两个事实各自为真，**拼起来的推论是假的**：

- 7 个编排构造函数**全部** `panic("NewXxxAgent: WithXxxName required")`
  （sequential:79 / parallel:67 / loop:69 / routing / handoff:88 / reflect:62 / graph）——
  有随机默认名的只有 **BasicAgent**（`config.go:263` `"agent_v2_"+uuid[:8]`）和 **PlanAgent**
- 而这两个**恰恰不把 name 交给 graph**：`basic_builder.go:584` / `plan_builder.go:79` 都是
  `NewBuilder().WithMaxVisit(...)`，**从不 `.WithName()`**（`BasicAgentBuilder.WithName` 只喂
  `hook.AgentInfo.Name`）

⟹ 随机 name **永远到不了 `Graph.ID()`**。`doc/code/todo.md:28` 的原始表述是准确的，
是我在方案正文里转述时压缩掉了这一层。**本轮第三次「两个真前提拼出假结论」**。

**顺带的副产品（比原命题更值钱）**：`Graph.ID()` 无 name 时只哈希**排序后的 nodeIDs + channel
name:type**，而 BasicAgent 的节点集 `{preamble,model,tools,postamble}` 和 channel 集是**编译期
常量** ⟹ 实测两个 name 和 maxRounds 都不同的 BasicAgent，`Graph.ID()` **完全相同**
（`95720dd1ffc17ee5`）。

1. **GraphID 防串台对 BasicAgent / PlanAgent 恒等于关闭**（`graph.go` 那段 godoc 自己写着
   「这两类形似神不同的 graph 必须 `WithName(...)`」——框架自带的两个编排违反了它自己的要求）
2. 方案「顺手闭掉 `Graph.ID()` 不哈希拓扑」那条 backlog 对这两个编排**几乎零收益**：加了边拓扑
   哈希它们仍然全等，真正区分两个 BasicAgent 的东西（system prompt / toolkit / model / skill）
   **全在闭包里，任何拓扑哈希都够不着**

## `Message.ID`：产出侧 4 个点全是结构体字面量 [已核实]

LLM 产出的 assistant 消息**全部不经 `schema.AssistantMessage()`**：
`model/openai/client.go:343`（非流式）/ `:475`、`:484`（流式 chunk / 聚合）/
`agent_v2/basic_agent.go:2206`（流聚合出口）——四处都是 `&schema.Message{...}`。

而拍板点 2 同时要求把 `HistoryStore` 约定改成「append-only 但**同 id 幂等**」。两条叠起来：
每轮 model 产出的 id 都是 `""` ⟹ **第 2 轮起被当成第 1 轮的重复静默丢弃**，而 model 节点每轮都记。

⟹ 「构造期生成」必须**先**覆盖这 4 个产出点（外加业务自己的字面量）。方案把它写成「注意事项」，
实际是**必须先补的前置改动**。同理 `rootInputNonceAnnotation` 下线有一处语义反转：今天
`claimRootInput` 空 nonce **放行**、`priorHasRootInputNonce` 空 nonce **返 false**，这对不对称
分支永远走不到危险组合，靠的是 `ensureRootInputNonce` 的**显式**保证；换成 `Message.ID` 后
它变成隐式依赖「所有产出点都盖了 ID」。

## 这条链路上无 finding 的两处（省得下轮重审）

- **tools 中断时的记录位置是对的**：`basic_builder.go:491-498` 报错先 return，
  `:554 recordHistory` 在其后 ⟹ 中断当下 input 已记、model 产出已记、工具结果未记；
  Resume 时 frontier 只含 tools，preamble/model 不重跑 ⟹ 不重记。实测 2→4 条，各 1 次
- **cp 侧的中断结构不受 `Message.ID` 影响**：`AskQuery` 只有 4 个字段、**不含 Message**；
  `ResumeContext.UserInputs` 只进 ctx **从不进 cp**（业务每次 Resume 自己重传）；
  答案消息只被 `ask_user_tool.go:113` 取 `.Content`
- **第 0 步「按组切」在中断链路上基本无影响**：`truncatePriorPreservingMarkers` 只在 preamble 内跑，
  而 Resume 的 frontier 不含 preamble ⟹ 一次 HITL 闭环里只执行一次。风险面在下一个**新** Run

## 审计自陈的未覆盖

`a2a/server` 和 `agui` 两个协议适配层的 Resume 入口（推测同样命中 F1，未实测）；
GraphAgent 的 Resume 路径；`ResumeActionReplan` 分支。

---

# 并发嵌套链路审计：第 1 步会把第 5 轮撤回的东西原样重造一遍

第四条链路（并发 + 嵌套）审完。审计**打了方案原型补丁**（`ForceRootHistoryCtx` 模拟「根对象持有
写入器 + ctx 继承」、`AUDIT99_SCOPE=1` 模拟拍板点 1(b)、`schema.Message.ID` + 构造期生成模拟拍板点 2），
六个形状实测对照。**这是四条链路里唯一一条真跑了方案而不只是读代码的**。

## 最重的一条：6→8 / 6→12 跟 issue-98 §9 记的是同一个数

| 形状 | 现状 条数/原始输入份数 | 根持有写入器后 | 增量来源 |
|---|---|---|---|
| A `Handoff(Par(Basic×3))` | 4 / ×1 | 5 / ×1 | Parallel merger 拼接产物记在 `front` 名下（取舍，不算 bug）|
| B `Handoff(Par(Seq(Basic)×3))` | 6 / ×3 | **8** / ×1 | 3 个 Seq 边界各把 final 再记一遍 |
| C `Par(Handoff×3)` | 6 / ×3 | **12** / **×6** | 3 个 Handoff 边界各把 input+final 再记一遍 |
| D `Par(GraphAgent(Basic)×3)` | 6 | **12** | 3 个 Parallel 分支边界各再记一遍 |
| E `Seq(Remote, Basic)` | 3 | 3 | 不变 |
| F `Par(同实例×3)` | 6 | 6 | 不变 |

**B 的 6→8、C 的 6→12 与 [issue-98 §9](issue-98-conversation-history.md) 记的「第 5 轮撤回」5→8 / 6→12
是同一个数。**

⟹ **第 5 轮撤回的那两条不是递归探测的副作用，是「写入器活在根」这个拓扑本身的直接后果。**
方案第 1 步 item 1+2 **在不做任何探测的前提下把它原样复现了**。我当时把病因归给了探测机制，
归错了。D 是文档没测过的新数据点，同样 6→12。

## 形状 B：agent 把自己上一轮的话读成第三人称

根持有写入器后跑两轮，b1 第二轮实测拿到的 prompt：

```
user: 原问题
user: For context: [b3] said: 产出<b3#1>.      ← 兄弟的（Rewrite，预期内）
user: For context: [seq3] said: 产出<b3#1>.    ← 同一句话再来一遍，换个归属
assistant: 产出<b1#1>                          ← 自己的
user: For context: [seq1] said: 产出<b1#1>.    ← ★ 自己刚说的话，被当成别人说的
user: For context: [par] said: [br1]…[br2]…[br3]…
user: 第二问
user: For context: [b3] said: 产出<b3#2>.      ← ★ 兄弟「本轮」的答案漏进来
```

走到哪一行：`sequential_agent.go:448` 的 `settleHistorySlot` 判据是 `history_slot.go:155`
`w.slotWroteHistory(slot)`——**精确匹配**。BasicAgent 记在 `front/br1/s1`，Seq 边界查的是
`front/br1`，查不到 → 再记一份。同一条产出在库里有两个归属，`history.go:704` 的
`owner == ownerAgentName` 对第二份判 false → 走 Rewrite → 变成 `For context: [seq1] said:`。

**不是「多几条」，是给模型喂假上下文**——它会以为有另一个 agent 独立得出了跟自己相同的结论。
按 CLAUDE.md 自己的判词：**污染比丢失重**。

最后一行（本轮兄弟答案）是并发**读**侧竞态：写侧串行了，`historyPrior` 取 prior 那侧**没有屏障**。

## 形状 C：未脱敏原文被写进历史（方案引入，安全级）

叶子挂一个 `OnAgentStart` 改写 hook（[消费] 档，CLAUDE.md 举的例子就是 PII filter）：

| | 历史里 `13800138000` 原文份数 |
|---|---|
| 现状 | **0** |
| 根持有写入器 | **3** |

走到哪：hook 在 Run 入口把消息整条换成新的 `schema.UserMessage`（脱敏版，`rootInputNonce` 随之丢失），
叶子记脱敏版；边界 `handoff_agent.go:529` 的 `settleHistorySlot` 拿的是**闭包里的 `input`**——
hook 之前那条、带原文；`history_slot.go:160` 的 `inputAlreadyInHistory` 按 nonce 扫 prior，
叶子那份 nonce 已被 hook 抹掉 → 判「没记过」→ **把原文写进去**。

现状不炸纯粹因为 C 形状下 Parallel 层没有写入器（`historyConfigProvider` 只有 BasicAgent 实现，
遍历 `[ho1,ho2,ho3]` 全 miss），边界补记整个是 no-op。**「激活原本 no-op 的边界补记」第 5 轮
已经写过，但当时只记了条数，没意识到它同时把 hook 前的原文带出来。**

## 形状 F：拍板点 1(b) 会把 best-of-N 变成互相污染 + **不确定**

`AUDIT99_SCOPE=1`（归属键从位置换成 scope）跑两轮，同一个 `*BasicAgent` 挂 3 分支：

| | 各分支第二轮 prompt |
|---|---|
| 现状（位置）| `[原问题, 产出#N, 第二问]` ×3，三条流各自完整、互不可见 |
| scope | `[原问题, 产出#1, 原问题, 原问题, 产出#2, 产出#3, 第二问, 第二问, 第二问]`，**且三个分支各不相同** |

连跑 5 次，「第二问」在三分支的出现次数：`1/2/3`、`1/3/1`、`1/2/2`、`1/2/3`、`1/2/3`——
且有分支在自己的 prompt 里读到**兄弟本轮刚产出的** `产出<shared#4>`。

⟹ 文档原写「(b) 对共享实例做不到它要保的能力 ⟹ 两条历史合并」——**实测比那句重：不只是合并，
是不确定 + 同轮串台**。形状跟 issue-98 §9 #4 的 `1/2/2/3/1/2/2/3` 一模一样，但这次在**读侧**，
单写入器 + CAS 治不了。**没有任何测试会红**（条数不变、内容都在），只有真跑 best-of-N 的业务会
发现采样不独立了。

## 形状 D：写入器传得下去，但**没有一层能承担 R2**

**Q1 的担心不成立（好消息）**：写入器是 ctx value，`compose_v2/graph_loop.go` 全文无
`context.Background()`、节点 ctx 是派生的，`RunSubAgent` 只叠 value 不换根 ⟹ **传得下去**。
实证：根持有变体里叶子 Basic **完全没配 `WithHistory`**，D 仍写出了 `br1/gsub` 名下的记录。

但问题被这个答案盖住了：**`graph_agent.go:136-233` 通读，零 `startHistory` / 零
`settleHistorySlot` / 零 `WithHistorySlot`**。R2 说「记录发生在包住它的那一层」——D 里包住
Basic 的是**业务的 NodeFunc**，框架在那里没有落点。

⟹ R1 字面执行后 D 的归属从 `br1/gsub` 塌到 `br1`，GraphAgent 内部结构在历史里完全不可见；
而只要业务 NodeFunc 把子 agent 产出**加工过**再写 channel（拼接、格式化、多路合并——
GraphAgent 的常规用法），产出物就是一条全新消息、章跟着丢，Parallel 边界当「没盖章」重新盖
`br1` 记一份，**跟 Basic 那份并存**。

⟹ 方案里「#9 GraphAgent 交不出静态子列表 ⟹ 构造上不存在」这句**只对写入器成立，对记录点
不成立**——#9 的残余在方案后仍然开着。**D 不是最先炸的，但最难修**：前三个能靠调 R2 判据收口，
D 缺的是**框架在业务 NodeFunc 那层根本没有落点**，不是判据问题是结构问题。

## R2 要成立的三个前提，方案一个都没写

1. **章必须骑在返回的产出物上**。今天 `stampHistoryAgent`（`history.go:846-859`）是「**拷贝一份**
   再盖章」，理由写在 `:827-828`——「直接改它们的 Annotations 会污染正在喂给模型的消息」。R2 要求
   的恰好相反：上层必须能从 `FinalMessage(events)` 上读到章。而 `basic_agent.go:1248` 的
   `NewExitEvent(name, last)` 持有的 `last` 就是 `basicMessagesCh` 里那条、下一轮还要喂给 LLM
   的指针。⟹ **R2 要么推翻那条已文档化的不变量（原地盖章），要么让 session event 和边界看到两个
   不同对象。二选一，方案没选。**（缓解：`model/openai` 不序列化 `Annotations`，原地盖章至少不上 wire）
2. **R1/R2/R3 只覆盖「产出物」和「多下游分发」，不覆盖输入侧**。而 `settleHistorySlot` 记的是
   input + final 两样，input 那半今天靠 `isDerivedInput` + `inputAlreadyInHistory`（读侧判据）。
   方案「#6 由 R2 消掉」⟹ **只消掉一半**。
3. **`章 == 本层` 不必然是「本层刚产的」**。LoopAgent 第 N 轮的输入 = 第 N-1 轮的产出，章都是
   `body`；而 **LoopAgent 是唯一没有 `MarkDerivedInput` 的编排**（全仓只有 `agent_tool.go:233` /
   `sequential_builder.go:185` / `plan_agent.go:846` 三处）。今天靠 `slotWroteHistory("body")` 挡住
   ——实测根持有写入器时 Loop 跑 3 轮**只记了第 1 轮**（后两轮静默丢）。**R2 换掉那个 map 之后这条
   护栏没了，会翻成每轮重记。**

## 另外三条

- **`WithHistoryScope` 若做成 `Option`，只能挂 BasicAgent**：另外 8 个编排各有自己的 option 类型
  （`ParallelOption func(*ParallelAgent)` 等）。⟹「`WithHistory` 本来就是 option 形态」这句
  **低估了改动面**：要么加 9 个 `WithXxxHistory` + 9 份三态字段，要么走 ctx 声明——**两者不是同一个
  方案，而文档现在两处各写了一个**
- **`AgentAsTool` 的归属键今天就是 `Agent.Name()`**（`agent_tool.go:238` → `RunSubAgent(ctx,
  t.agent.Name(), …)`，`run_subagent.go:90-93` 注释自己承认）。换 scope 后这条路径不变——
  **§9 #1 在这条路径上从来没被修掉过**
- **框架推荐的中阶 fanout 归属合并**（现状已有，方案不修也不恶化）：`SendFrom` 同 Target 投 N 次时
  N 个实例的 `HistorySlot` 全是同一个 nodeID（`run_subagent.go:86-89` 注释明说「不用 childNS，
  因为 fanout 的 childNS 每轮都变」）。实测 3 实例全 `owner="work"`，三条流合并。**R3 也管不了**：
  分发层只有一个位置，盖一个章

## 我问错的一个问题

我让审计验「#7 由单写入器后 nonce 铸造点问题消失」——**这句在 issue-99 里不存在**。文档第 1 档写的
是「#5 和 #9 构造上不存在；#4 由 R3 消掉；#6 由 R2 消掉」，**四条里没有 #7**。（「独立审计的修正
记录」还专门补过一句「文档只列了消掉的四条，没有残余清单」。）

实际推演：铸造点问题**会**消失，但靠的是拍板点 2（构造期生成 ID）而非单写入器——实测
`schema.UserMessage` 带 ID 时 C 形状 3 个叶子看到 **1 个 ID / 3 个不同指针**（`ensureRootInputNonce`
的 `cp := *msg` 天然带过 ID）。**但 C 的输入重复并不因此消失**：根持有后实测是 **×6**（叶子 3 +
边界 3），边界那 3 份走 `inputAlreadyInHistory` 读侧判据、**不受 ID 影响**。

## Q6 的答案跟我预期的不同，但结论更糟

不会「Parallel COW 一次 + 3 个 Handoff 各 COW 一次 = 4 个 ID」——COW 只在 ID 为空时触发，构造期生成
之后是 no-op。**但 4 个 ID 会以另一条路径发生**：

| 情形 | 叶子看到的不同 ID 数 |
|---|---|
| `schema.UserMessage` 构造（带 ID）| **1** ✅ |
| 业务手搓 `&schema.Message{}`（无 ID）| 只在叶子补 = 3 |
| **叶子挂 `OnAgentStart` 改写 hook** | **4**（3 叶子新铸 + 1 caller 原始）|

`OnAgentStart` 是 [消费] 档、hook 可整条换掉消息，ID 跟着丢、下游重新铸。方案已为**产出侧**注意到
这件事（「必须在 `OnModelEnd` 之后」），**输入侧的 9 个 `OnAgentStart` 消费点没提**——而这正是形状
C 那条 PII 原文泄漏的同一个根因。

## 炸的先后

1. **C `Par(Handoff×3)`** —— 唯一同时踩满四件的（条数 6→12、输入 ×6、PII 原文进库、ID 分裂成 4）。
   **且它是方案自己承认没被任何一条 R 规则覆盖的那条**（#7 不在「消掉的四条」里）。第 1 步落地当天就翻
2. **B** —— 条数 R2 治得了，但「自己的话读成第三人称」要多轮跑起来才看得见，**且不报错**。比 C 晚炸、比 C 难查
3. **F（拍板点 1(b) 落地后）** —— 现状完全正常，落地后 prompt 变不确定，**没有任何测试会红**
4. **D** —— 最难修（结构问题不是判据问题）

⚠️ **A 和 E 在方案下基本不变——而它们恰好是现有测试覆盖的形状。⟹「28 包全绿」在这轮里一点保证
都没有。**A 恰好是「直接子里有 BasicAgent」的形状、现状就已经是单写入器，**正因如此不能拿它当验收样本**。

## 审计自陈的不确定

R1/R2/R3 没有完整实现跑一遍（要改 7 个编排的边界 + BasicAgent 三个记录点）；B/D/Loop 三条 R2 推演是
**读代码得出**的，只有 R3 对 F 的后果做了等价模拟。`Message.ID` 原型只加在 `UserMessage`（不是全部
构造函数），故「28 包全绿」的复证强度**弱于**方案原报告。

---

# 我自己核出的一处审计错误：PlanAgent 的随机名**会**进 graph [已实测]

HITL 链路审计断言「随机 name 永远到不了 `Graph.ID()`」——**对 BasicAgent 成立，对 PlanAgent 不成立**：

- `plan_agent.go:926` → `compilePlanGraph(planNode, workers, p.maxSteps, p.Name())`
- `plan_builder.go:82-84` → `if name != "" { b = b.WithName(name) }`（ISSUE-85 MED-2 特意加的）
- 而 `NewPlanAgent` 的默认名是 **`"plan_agent_" + newUUID()[:8]`**

实测两次构造：

```
PlanAgent#1  name=plan_agent_45d2a637  GraphID=plan_agent_45d2a637
PlanAgent#2  name=plan_agent_917e301a  GraphID=plan_agent_917e301a
```

⟹ **业务不显式设 `PlanName` 时，重建 PlanAgent 对象后 Resume 必撞 `ErrGraphIDConflict`**
（跨进程 / 重启 / 每请求新建 agent 都会踩）。原命题在 PlanAgent 这一支上是**真的**，
是 HITL 审计把 BasicAgent 的结论推广过头了。

同时也印证了它的另一半：BasicAgent 恒走 fingerprint，而 fingerprint 只哈希 nodeIDs + channel、
节点集是编译期常量 ⟹ **两个 BasicAgent 的 GraphID 恒等**（实测 `95720dd1ffc17ee5`）。
两个编排**方向相反**：PlanAgent 太敏感（同一个 agent 重建就冲突），BasicAgent 完全不敏感
（不同 agent 也不冲突）。

---

# 方案第二版 —— 第一版的核心判断被实测推翻

> 第一版（上文「最终实施顺序」）**作废**，保留在文档里只作决策溯源。以本节为准。

## 第一版错在哪，一句话

第一版把病因诊断成「**写入器有多个**」，于是方案是「把所有权收到根」。四条链路实测证明：

> **重复不是「多个写入器」造成的，是「边界补记在每一层都触发」造成的。**

现状之所以不炸，是因为**很多形状下编排层根本没有写入器、边界补记整个是 no-op**——`historyConfigProvider`
只有 BasicAgent 实现，`Par(Handoff×3)` 的 `subs` 是三个 HandoffAgent，遍历全部 miss ⟹ Parallel 层没写入器
⟹ 边界补记不触发 ⟹ 三个叶子各自建私有写入器、各记各的。

**把所有权收到根 = 把这个 no-op 激活 = 第 5 轮撤回的 5→8 / 6→12 原样重现。** 我上个月把病因归给了
递归探测机制，归错了；第一版是在错误的病因上开的药。

## 换判据：静态结构问题 → 动态范围观测

边界补记存在的**唯一**理由：这一步的执行体**可能什么都没记**（远程 agent / 自研 agent / 适配别的框架的壳）。

今天问的是「**位置 X 有没有被标记过**」（`history_slot.go:155` `w.slotWroteHistory(slot)`）。而位置在
**两个方向上同时是错的**：

- **太细**——叶子记在 `front/br1/s1`，边界查 `front/br1`，精确匹配查不到 → 重复（B/C/D 全是这一条）
- **太粗**——Loop 多轮、Handoff 多跳共用同一个位置 → 第 2 轮起被挡掉 → 静默丢

⟹ 正确的问题不是「谁在哪儿标记过」，是「**这次调用的范围内，有没有产生历史记录**」。
**它是运行期观测，不是静态结构判断**——这正是四轮补丁一直没跨过去的那道坎：静态遍历编排树永远
探不到业务自己塞进来的 `Agent` 实现（`Agent` 是开放接口），而运行期计数**不需要探**，业务实现什么都没记
就是计数为 0，那恰恰是**它应该被补记**的正确答案。

### 形态

边界在调用 sub-agent 前往 ctx 里放一张**回执**，`recordHistory` 落笔时给自己 ctx 上那张回执 +1，
并沿父链上溯（外层问的是「我底下有没有发生过」）。sub 返回后边界读自己那张。

- **必须在 enqueue 时同步登记**，不能等异步写入器 drain——边界紧跟着 sub 返回就要读
  （写入器是单 worker FIFO 异步，`history.go:482 submit`）。登记放在写入器的锁里，那是这条流上
  唯一天然串行的点（跟 `claimRootInput` 同一个位置）
- **回执必须 per-ctx**，不能是写入器上的全局计数：Parallel 三分支共享写入器，全局计数会让分支 A
  看见分支 B 的写入
- **不是「递归探测」**：不遍历任何结构，只数实际发生的事

### input 和 final 分开判 —— 这是第二版跟第一版最实质的差别

| | 判据 | 消掉什么 |
|---|---|---|
| `input` | 范围内计数 **== 0** 才记 | 叶子记了脱敏版 → 计数>0 → 边界**不记** hook 之前那条原文 ⟹ **C 的 PII 泄漏自动消** |
| `final` | **指针身份**：边界手上这条 final 有没有被记过 | 透传（同指针）跳过 ⟹ B/C/D 的重复消；执行体**加工过**（新指针）照记 ⟹ D 的加工结果不丢 |

**为什么必须拆**：只用计数，「既委派又加工」的执行体（GraphAgent 的常规用法、业务自研壳）
加工结果会丢；只用指针，PII 那条会漏（hook 之前的原文是不同指针，照记不误）。
第一版和 R1/R2/R3 都只有一个判据，两个失效模式必然占其一。

指针可用是核实过的：`basic_agent.go:1248` 的 `NewExitEvent(name, last)` 持有的就是
`basicMessagesCh` 里那条、也就是 `recordHistory` 收到的那条；`stampHistoryAgent`
（`history.go:846-859`）盖章前**拷贝**，写入器登记的是**收到的原始指针**，不受影响
——**也因此第二版不需要 R2 那个「章要骑在产出物上」的前提**，那个前提跟
`history.go:827-828` 已文档化的「不污染正在喂给模型的消息」互斥，第一版没解决。

### 顺带修掉的

同一套判据同时消掉：B 的第三人称污染（不再有第二份归属 → 不走 Rewrite）、Loop 第 2/3 轮静默丢
（每轮回执重置，外部 body 每轮计数为 0 → 每轮记）、Handoff 多跳同款、`USER_Q` 记两遍
（Loop 边界记过 → Sequential 边界计数>0 → 跳过）。

### ⚠️ 六形状推演是推演，**没实测**

下一步必须按并发那条审计的规格**打原型真跑**（`ForceRootHistoryCtx` 那套还在 scratchpad）。
理由是这一轮自己给的教训：**A 和 E 是现有测试覆盖的形状，而它们在任何方案下都不变** ⟹ 全绿零保证。
验收样本必须是 **B / C / D / F / Loop**，且 C 要带 `OnAgentStart` 改写 hook。

## 明确不做的（连理由一起记账，防以后重新捡起来）

| 不做 | 理由 |
|---|---|
| **写入器所有权移到根**（第一版第 1 步 item 1+2）| **它就是第 5 轮撤回的拓扑**。B 6→8 / C 6→12 实测复现，跟 issue-98 §9 记的数字一致 |
| **R1「执行体只产不记」** | 字面执行会删掉 `recordHistory` 的三个 BasicAgent 内部调用点（`basic_agent.go:909` / `basic_builder.go:425` / `:554`），历史退回 events 的首尾粒度——而 `prior` 换源到历史层的**唯一理由**就是「events 只记首尾，模型看不到结论的来源」（`history.go:166-172`）⟹ ISSUE-98 §6.2 白做 |
| **拍板点 1(b) scope 归属键 + alias** | best-of-N 实测变**不确定**（连跑 5 次 `1/2/3`、`1/3/1`、`1/2/2`…）+ 同轮串台，而 best-of-N 是 ISSUE-80 特意恢复的能力。且 `WithHistoryScope` 做成 `Option` **只能挂 BasicAgent**（另外 8 个编排各有自己的 option 类型）。alias 连带作废（它只为让 scope 扛住重命名）——一并消掉「alias 永久」「alias 和 scope 共用字符串空间而冲突守卫只管写侧」「R2 比位置读回比 scope」三条 |
| **拍板点 2 `Message.ID` 当去重键** | 判据 `basic_agent.go:866` **没有 mode 门**，今天无害只因非转交输入没有 nonce（空 nonce 无条件放行）；`Message.ID` 是普遍的 ⟹ 放行没了 ⟹ 实测 Run 2 的新问题**不进 prompt**、Run 3 错位一轮，而 store 全程正确。且产出侧 4 个点全是结构体字面量、`OnAgentStart` 改写会丢 ID。**保留显式 nonce**——只有 `HandoffAgent.Run` 铸造，那个**构造上的作用域限制正是 `Message.ID` 会毁掉的安全性**。（`Message.ID` 若为别的目的要加是另一件事，但**不能兼作去重键**）|
| **第 0 步四件全部** | `cp.Step`：`Checkpoint` 没有该字段，唯一安全的推进点跟「step 是超步序号」这个依据互斥，且换后 backend 漏存会在**首次** HITL 就重问；SchemaVer fail-fast：顶到 `serialization.go:452-455` Option D + conformance **故意不填**的设计；按组切：唯一的网是假绿，且「组」是一次 LLM 调用（3 个 Run 切出 6 组），闸门从「有界 prompt」退化成「有界轮数/无界 prompt」 |

## 跟换判据独立、可以先行的现存洞

都有实测复现，**跟第二版核心改动无耦合**，先修先受益：

1. **7 个编排的 `Resume` 入口不建历史写入器** —— 中断后 Resume 实测 3 条只剩 1 条。补齐 + 加「Run/Resume 入口对称」回归
2. **跨租户**：`startHistoryForOrchestration` 取 map 迭代的第一个 ⟹ 落哪个租户每 Run 随机翻。改成**多个内容不同的 history config 时 fail-fast**
3. **PlanAgent 随机默认名直接成为 GraphID** ⟹ 重建对象后 Resume 必撞 `ErrGraphIDConflict`
4. **【本轮新发现】边界补记只覆盖 5 个编排**：`settleHistorySlot` 的生产调用点只有
   `handoff_agent.go:529` / `parallel_agent.go:456` / `sequential_agent.go:448` /
   `loop_agent.go:401` / `routing_agent.go:502`+`:529`。而 **PlanAgent（`:795`/`:861`/`:905`）、
   ReflectAgent（`:465`）、`RunSubAgent`（`run_subagent.go:89`）、skill fork（`skill_tool.go:649`）
   都 push 了 `WithHistorySlot` 却从不 settle** ⟹ 这四条路径下的外部执行体**什么都不留**，
   正是 `settleHistorySlot` godoc 自陈要防的形状。PlanAgent 的 `executor` / `TmpSubAgent`
   是一等公民用法，不是边角

## 第二版自己的已知弱点（不藏）

- **没实测**。六形状推演是读代码得出的，验收前不算数
- **D 形状仍不完美**：「既委派又加工」的执行体，指针判据能保住加工结果，但**归属**仍落在
  最近的可落点（Parallel 的 `br1`），GraphAgent 内部结构在历史里不可见。这是结构问题
  （框架在业务 NodeFunc 那层没有落点），第二版只保证**不比今天差**
- **`SendFrom` 同 Target 的 N 实例归属仍合并**（`run_subagent.go:86-89` 注释自陈不用 childNS）
  ——现存问题，第二版不修也不恶化
- **`AgentAsTool` 的归属键仍是 `Agent.Name()`**（`agent_tool.go:238`）——四轮补丁都没覆盖这条路径
- **回执沿父链上溯的深度**：不遍历结构所以没有栈溢出风险（链长 = 实际嵌套层数，且是 ctx 派生链），
  但要验一遍深嵌套下的开销

---

# 实施记录：方案第二版落地 + 四路审计的修正

第二版按「先建测量基线 → 换判据 → 补现存洞 → 四路单视角审计」走完。**审计各抓到一条我自己
引入的真问题**，判据前后一共改了三轮才收敛。

## 落地了什么

| 项 | commit | 关键实测 |
|---|---|---|
| 编排形状矩阵（A/B/C/C′/D/E/F/G + Loop）| `512b62e` | 立表理由：第五轮撤回时当轮测试全绿，因为**有测试的两个形状对写入协调根本不敏感**（A 和 E 在任何改法下都不变）|
| 边界补记判据：位置 → 范围观测 + 指针 | `512b62e` | Loop(外部 body×3 轮) 修前丢第 2 轮产出 + 原话记两遍，修后 5 条干净 |
| 7 个编排 Resume 入口补写入器 | `cd603e1` | `Sequential(外部, 本地)` 外部那步中断：修前 1 条，修后 3 条 |
| 子 agent 历史配置冲突 fail-fast | `fc02cbc` | 原来取 map 迭代第一个 ⟹ 落哪个租户每 Run 随机翻 |
| 边界补记覆盖面（Plan×3 / Reflect / RunSubAgent）| `337baa4` | 那四条路径下外部执行体原来什么也不留 |
| PlanAgent 自动名不当图身份 | `13bda102` | 两次构造 GraphID 不同 ⟹ 重建对象后 Resume 必撞冲突 |
| tool-call 豁免改由调用点给 | `c317583` | 嵌套时内层产出实测 **0 次** |
| 输入按流认领 + role 过滤 + 产出按范围判 | `a6364ed` | 扇出份数修前随调度在 1~4 翻，修后 30 轮恒为 4 |

## 判据的最终形态（三轮才收敛）

```
输入：Role == user  &&  !派生  &&  !这次调用范围内产生过记录
      &&  !历史里已有(nonce)  &&  按【流】原子认领
产出：!(这次调用范围内产生过记录 && 全局登记表里有这条消息)
```

三轮各改掉什么：

1. **第一轮**（`512b62e`）：位置 → 范围 + 全局指针。修掉 Loop 丢中间轮次。
2. **第二轮**（`c317583`）：tool-call 豁免从 ctx 信号换成调用点开关。ctx 信号**没有清除点**，
   问的是「祖先里有没有 tool_call」而不是「我这一层是不是那次」。
3. **第三轮**（`a6364ed`）：输入从全局指针换成**按流**认领 + 加 `Role == user`；产出从全局
   指针换成**范围 + 全局**。

第三轮那三处同源于**一把尺量两件事**：
- 输入用全局判 ⟹ 并发分支共享同一条输入指针，先跑到的剥夺其余分支的归属（实测 40 轮份数
  在 1~4 之间乱跳，撤掉那道 guard 后 200/200 恒为 4——但 guard 是承重的，撤了 Loop 立刻双记）
- 输入只靠指针 ⟹ 中间层**复制一下消息**就穿过去（`HandoffAgent` 入口的 `ensureRootInputNonce`
  正是浅拷贝，`Loop(Handoff(外部))` 第 2 轮把第 1 轮产出当新输入又记一遍）
- 产出用全局判 ⟹ 执行体**每轮返回同一个消息对象**时第 2 轮起静默丢（`var okMsg = ...` 是
  很自然的写法）——正是这套改动要消灭的形状换了个触发条件

## 三次假绿 + 一次误报（全靠真跑撤销抓到，没有一次是推理出来的）

| # | 假绿在哪 | 怎么暴露的 |
|---|---|---|
| 1 | 顺序稳定用例比的是报错文案，而文案里当时不含冲突双方 | 撤掉排序照样绿 → 把冲突双方的 owner 写进报错 |
| 2 | `RunSubAgent` 哨兵被外层 Sequential 的记录顶掉 | 撤掉照样绿 → 让 NodeFunc 加工过再往下写 |
| 3 | 加工写成「【加工】+原文」，子串断言把原文包在里面 | 撤掉照样绿 → 加工结果必须不含原文 |
| 4 | PlanAgent 两个 GraphID 用例**都是空的**——Run 成功后 cp 被清了 | 带着 bug 也绿 → 让 worker 中断留下 cp |
| 误报 | Reflect「产出记了 9 次」 | 实际是评估器 stub 用完后循环 9 轮，每轮一条**不同**的产出 |

## 一处归因更正

`512b62e` 的 commit message 把「输入份数随机 1/2/3」归给「Parallel 借不到中间那层的配置，
三个叶子各建了一个写入器」。**审计证明单个共享写入器下同样随机**——真因是 settle 的输入路径
是「先查后写」不是原子认领。多写入器确实存在，但那是另一个独立因素。

## 还开着的（都记进了 `doc/code/todo.md`）

1. **GraphAgent 没接历史层的任何钩子** —— Run / Resume 两个入口都没建写入器也没有边界补记。
   当前靠外层编排兜住，**顶层直接跑它历史全程为空**。要先定「它的历史配置从哪来」（没有可借
   配置的静态子列表）
2. **tools 节点 fanout + `AgentAsTool` 时 Resume 整段重记**（**非本轮引入**，审计对
   `512b62e~1` 和 `13bda102` 逐字对比输出相同；`13bda102` 是 2026-09-18 按同名提交推断、替换原文里不存在的 `e649c9c`，`512b62e` 在 dev 历史里仍找不到）：Resume 重跑全部 tool_call，而子 agent 在
   Run 1 里是成功的、cp 已清 ⟹ 从头再跑再记。基线 7 条 → Resume 9 条。**边界补记管不着**
   ——重复是子 agent 自己节点内写的
3. **业务外壳改写输入后委派给框架 agent → 用户原话零落点**（本轮判据的**已知取舍**）：范围
   判据让「内部记了东西 ⟹ 输入归它管」，而它记的是改写后的。这道判据**不能改**——它同时挡着
   `OnAgentStart` 脱敏后边界把原文写进历史那条安全问题
4. **形状 B 的输入份数随机 1/2/3**（`Handoff` 在顶时是共享视图该恰好一份）：那条走的是
   BasicAgent 自己的 `claimSharedRootInput`，跟边界这条是**两套认领**，而 Parallel 借不到
   中间那层 Sequential 的配置 ⟹ 三个叶子各建一个写入器 ⟹ 写侧那次 CAS 只在单个写入器内有定论
