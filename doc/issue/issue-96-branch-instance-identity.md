# ISSUE-96 · 根因：记录/身份用 NodeID 粒度，执行单位却是 branch 实例

**优先级**：HIGH（**6 个已实证侧面**，同一根因；其中一个踩的是框架文档**推荐**的用法，一个让框架针对本类的**唯一** fail-fast 形同虚设）
**状态**：✅ **已闭合**（2026-07-20）——6 个侧面全部修复并配哨兵，4 路并发审计再抓 5 个 finding 也已修完。
实施记录见下方「实施结果」；仍未覆盖的面见「覆盖边界」（诚实标注，非遗漏）
**来源**：ISSUE-95 finding 1 修复过程中暴露 → 用户追问「为什么不去挖清楚」→ 深挖三方 + 定向 bug 狩猎

---

## 一句话

`compose_v2` 里**同一个 NodeID 可以有 N 个并发实例**（唯一来源：`SendFrom` 同 Target 投递多次），
但所有记录/身份/过滤都以 **NodeID** 为粒度。凡是「按 NodeID 索引/过滤/寻址」的地方，
多实例下都会退化——**已实证 6 个**，其中 5 个静默（不报错、结果错），1 个报错但文案误导且不可恢复。

## 为什么只有 SendFrom 会多实例

- 静态边 / cond 边：`computeNextTasks` 的 `nextSet` 是 `map[NodeID]struct{}`（`graph_loop.go:882`，
  `:983` 写入）→ 同 NodeID 恒 1 份
- **`SendFrom`**：`graph_loop.go:811` `nextFrontier = append(nextFrontier, d.Target)` 逐条 append
  **不去重**（by-design，`SendDirective` godoc 的头号示例就是 N 个并发 worker）

## 两个身份维度为什么都不稳

| 维度 | 来源 | 为什么不稳 |
|---|---|---|
| **`branchIdx`** | 实例在 frontier 里的**位置**（`branchIdxByID`，`graph_loop.go:1088` 建 / `:1105` 递增）| 部分失败时 `updateFrontier(res.FailedBranches)` 把 frontier **缩成失败子集** → 位置重排（{1,2}→{0,1}）|
| **`visit`** | `cm.visitCount[nodeID]`，**per-NodeID 共享计数器** | 每个成功实例在**自己的 goroutine** 里 `IncrVisitCount`（`:1313`）、入口读（`:1227`）→ ① 成功兄弟推进它 ② **超步内并发竞态**（谁在别人成功后才进入，读到的就更大）|

---

## 已实证的 6 个侧面（全部有探针 + 对照组，输出见下）

> **以下三节（侧面 / 迁移面 / 验收标尺）是实施前的现场记录**：行号与代码形态对应 `4fe7eb6` 之前，其中提到的 `MakeInterruptID` / `GetCurrentNodeBranchIdx` / MED-3 guard 均已在实施中删除。最终形态见「实施结果」。

### 侧面 1（= ISSUE-95 finding 2）：canonical InterruptID 跨 Resume 漂移 + 不收敛

按 `MakeInterruptID` godoc **明文教的**写法（ctx getter 取 visit/branchIdx 重算）：

```
Run1 中断:    visit=0 branchIdx=1 → 029d…     visit=0 branchIdx=2 → f9bd…   ← 用户按这俩答
Resume1 重算: visit=1 branchIdx=0 → 7ef6…     visit=1 branchIdx=1 → 87e4…   ← 重叠 0/2
Resume2 按新 ID 答: 仍只匹配上 1 个 → **不收敛**
```

打破 godoc 的框架承诺「同节点同 visit 同 branchIdx 同 idx 跨 retry/Resume 总是同 ID」。
N 个实例要 ~N 轮 Resume 才收敛，每轮重新问用户。

### 侧面 2：`SendFrom` + `RunSubAgent` → 子 cp 互覆（**踩的是文档推荐用法**）

`agent_v2/run_subagent.go:68` 只认 `GetCurrentToolCallID`，**不认图级 branchIdx** → SendFrom
fanout 的 N 个实例都走 `JoinNS(parentNS, nodeID)` 撞同一个 childNS。

```
worker 实例 #1: branchIdx=1 parentNS=""
worker 实例 #2: branchIdx=0 parentNS=""       ← 2 个实例各跑 1 个子 agent
本 session 下共 2 个 cp:
  ns=""            frontier=[worker worker]  pending=1
  ns="researcher"  frontier=[tools]          pending=1    ← **只有 1 个子 cp**
```

第二个实例覆盖第一个，另一个实例的进度 + 它的 HITL 待答问题**静默蒸发**。

**而 `run_subagent.go:45-47` 正把 SendFrom 列为推荐 fanout 路径**：
> 业务想 fanout 走：高阶 `ParallelAgent` ／ **中阶: framework `compose_v2.SendFrom(ctx, dirs...)`**

同一段还警告别自己 `go RunSubAgent`（理由正是「全 N 个 goroutine 撞同 NS cp 互覆」）——
**推荐的替代路径踩的是同一个坑**，只是换了扇门。MED-3 的 guard 在 `subgraph.go:123` 拦不到
（它只拦 subgraph 节点，这里是裸 AddNode）——**而且那个 guard 本身也不承重，见侧面 5**。

### 侧面 3：多实例 + cond `CondFailRerunFrom` → 兄弟实例产出丢 2/3

```
Run1: Z 跑了 3 次（应为 3）                                  ← 3 个实例全成功，各投 1 个 W
Run1 cp: CurrentFrontier=[Z]  CompletedFrontier=[]  DeferredSends=0
              ↑ 3 缩成 1        ↑ 3 个记录全没      ↑ 3 个 sendW 全丢
Resume err = <nil>                                          ← **静默成功**
>>> Z 总共跑 4 次 / W 跑 1 次（语义上 W 应为 3）
```

三件事叠加，每件单看都"对"：
1. `updateFrontier([]NodeID{from})`（`:946`）—— `from` 是裸 NodeID，3 缩 1
2. audit² M1 的「重跑成员自己不记」按 NodeID 过滤 → 3 个 `id="Z"` 全命中 `rerun={Z}` → 全丢
3. sends 按 `d.owner ∈ rerun` 过滤 → 3 个 `owner="Z"` 全丢

> **归属**：collapse 机制（`updateFrontier([from])` + M1 的 NodeID 过滤）**早于**本轮；但
> `CondFailRerunFrom` 这个入口是本轮 `67e0aa7` 加的。本轮把**默认档**改成 Abort（清 cp 子树
> → 不会静默出错误结果）实际**收窄**了触发面，但给它开了一扇 opt-in 的门。

### 侧面 4：fan-out **宽度**被当成迭代**深度** → 假 `MaxIterationsError` + **不可恢复的毒 cp**

`compose_v2/graph_loop.go:643` + `:663-669`：

```go
visits := make(map[NodeID]int)   // :643
...
for _, id := range frontier {    // ← 遍历 frontier **条目**
    visits[id]++                 // ← key 却是 NodeID
    if visits[id] > g.maxVisit { return MaxIterationsError }
}
```

`SendFrom` 同 Target 投 N 次（N > maxVisit，**默认 100**）→ frontier 里 N 份同 NodeID →
**一个超步**就把 `visits[worker]` 加到 N。而 maxVisit 的语义是「防死循环」——**图里一个环都没有**。

```
width=100 → worker 跑 100 次 | MaxIterations=false | err=<nil>
width=101 → worker 跑   0 次 | MaxIterations=true  | err=node "worker" visited 101 times (max 100)
        ↑ 图拓扑 Start→split→(SendFrom×101)→worker→End，**零环**
对照组：101 个 distinct NodeID → 跑 101 次 | err=<nil>     ← 证明就是 NodeID-vs-实例这一维
```

**比"报个错"更重**：检查在 `executeFrontier` **之前** → worker **一次都没跑**就存了 Pending cp
→ 之后每次 Resume 都在跑任何节点前触顶 → **永久卡死、进度恒 0**，只能 `WithFreshRun` 或手删 cp 脱困。
（对比单实例自环：每 Run 拿新预算能推进 100→200→250，是「预算用完，Resume 续」；多实例宽度超限
是首个超步就超 → `visits` 每 Run 重置也没用。）

**后果**：可见报错，但**文案误导**（"visited 101 times" 暗示死循环）+ **cp 不可恢复**。

### 侧面 5：MED-3 的 fail-fast guard **根本没在拦**（框架针对本类的唯一防线是装饰性的）

`compose_v2/subgraph.go:122-126` 的 `if branchIdx := GetCurrentNodeBranchIdx(ctx); branchIdx > 0`
三个缺陷叠加，**每个都源于 branchIdx = frontier 位置**：

1. **只拦 `branchIdx>0`** → 实例 0 永远照跑（子图副作用照发生），是**部分拒绝**
2. **guard 的错只进 `FailedBranches`，而 `FirstErr` 只返一个** → 兄弟实例若中断，guard 的错
   **被 InterruptError 顶掉**，业务完全看不到
3. **Resume 时 frontier 缩成 1 元素 → 走 `executeFrontier` fast path → `branchIdx` 硬编码传 `0`**
   （`graph_loop.go:1070`）→ **guard 根本不触发**

```
Run1 err = node "S": subgraph node "S" fanned out via same-target SendFrom is unsupported...
  guard 报了吗: true | 子图跑了 1 次        ← guard 若真拦住应为 0（实例 0 照跑）
Resume err = <nil> | 子图累计跑 2 次
>>> 报一次、一次 Resume 就走过去、**最终 err==nil** —— 业务以为 fan-out 成功了
```

**这推翻了本文档初版三处前提**（原以为「guard 已挡住 subgraph 那半」）。两个直接影响：
- 「收益：MED-3 guard 可拆」——**比原想的更划算**，它今天没在保护任何东西，拆掉不损失现有保证
- `subgraph.go:112` 的 childNS 从"迁移时别忘"提到**现存 bug**

### 侧面 6：observability 的**两个** session 级共享 map，key 都没有实例维度

**6a — node span registry key 碰撞**（`observability/observability.go:668-680` `registryKey` + `:695-711` `spanKey`）：
tool span 有 `callID` 当唯一锚点；**node span 什么都没有** → `"node|graphNS|nodeID"`。N 个实例
graphNS 相同（`executeFrontier` 给 N 个 goroutine 传的是**同一个 ctx**，不派生 fanout ns）、
nodeID 相同 → key 完全相同。

```
worker NodeFunc 实际执行 3 次（各睡 40/80/120ms）
导出的 worker span: 1 个（期望 3）
  name="gen_ai.workflow.node.worker"  duration=41.0ms     ← **来自最先结束（40ms）那个实例**
  name="gen_ai.workflow.probe_graph"  duration=120.9ms    ← graph span 老实显示真实宽度
对照组：3 个 distinct NodeID → 导出 3 个 span（期望 3）
```

被覆盖的 2 个 `spanEntry` 无人引用 → **直接 GC，永不 End、永不上报**（不是泄漏，是静默丢失）。
`-race` 干净 → **逻辑身份碰撞，不是 data race**，race detector 抓不到。

**6b — chunk buffer 同病**（`observability.go:1130` `chunkBufferKey(graphNS, nodeID)`）：
**独立的第二个 map**，修 `registryKey` **不会**顺带修好它。

```
event="streaming_response" content="A1 B1 A2 B2 B3 A3 " count=6     ← 两个实例的 token 逐条编织
单实例对照组：content="S1 S2 S3 " count=3                            ← 探针本身没问题
>>> streaming_response 事件总数 = 1（期望 2）
```

同根还波及 TTFT（`hooks.go:1081-1092` 拿幸存 span 的 start 减 first chunk，可能跨实例）。

> **这是第 4 类消费点**（除 InterruptID / RunSubAgent NS / purge 之外），本文档改动面初版没列，
> 且要改**两个** key 函数。
>
> **明确排除的非问题**（不凑数）：metric 的 `LabelNodeID`（Counter 跑 N 次就该 +N，语义正确，
> 加 branchIdx 反而平添 cardinality）；`agui` / `a2a`（node hook 是 no-op，无 `map[nodeID]` 结构）；
> 并发安全（`sync.Map` + `spanEntry.mu` 都对，无 race）。

---

## 同根的历史事故（说明这不是孤例）

| | |
|---|---|
| `90a94d2` 的自引回归 | 按 NodeID 清 purge → 误删已成功兄弟实例的 send → W 从 2 掉到 1（**比原 bug 更坏**）。`7d6b913` 修回 |
| MED-3 fail-fast（`subgraph.go:114-126`）| 早就诊断出同一根因（注释原话：「branchIdx 是"失败子集内的位置"…Resume 重跑时 branchIdx 从 N 漂回 0 → 子 cp namespace 对不上 → 静默丢进度」），当时选择**试图挡掉 subgraph 那半**，并明写「裸 AddNode 同-Target fan-out + 中断的 InterruptID 漂移是**独立议题**、本 guard 不覆盖」——那就是侧面 1。⚠️ **但实测那个 guard 根本没挡住**（侧面 5）——诊断对了、药没起效 |

**每次在这片地基上动手都会再引入一个**——这本身就是"该根治而非打补丁"的证据。

---

## 方案：给 branch 实例一个身份

**fresh mint 的不透明 token，随 frontier 持久化。**

```
drainSendSink（stamp owner 的同一点，料全在手上）：directive.instanceID = 新 mint 的 token
frontier: []NodeID → []frontierMember{NodeID, InstanceID}
```

### 为什么 fresh-mint 就够（四个问题一起消失）

| | 为什么 |
|---|---|
| 嵌套 fanout 碰撞（Z×2 各自 `SendFrom(W)`）| 各自 mint，天然不同 —— **不需要路径哈希** |
| 代际（重跑 ⟹ 旧答案作废）| 重跑 = 新一次产出 = 新 token —— **不需要 visit 当代际号** |
| visit 竞态 | 身份不含 visit |
| branchIdx 漂移 | 身份不含位置 |
| **确定性** | **不需要** —— 只有"每次重算"才要确定性（那是 LangGraph 的约束），我们**持久化** |

### 实施期定的三条细节（文档原本没写到这一层，2026-07-17 回填）

**① `InstanceID` 用 `string` 空串表示"无"，不用 `*string`**（原文档写的是 `*string`）。
零值即"静态边成员"语义，少一层解引用，也少一类 nil panic 面。

**② mint 点在 drain 之后的 stamp 段（`runNodeWithHooks` 内，与 owner stamp 同点），不在「target 进 frontier」那行**
（`graph_loop.go` 的 `nextFrontier = append(nextFrontier, d.Target)`）。

这条**有实质判断**：deferred send 从 cp 重放回来时会再次流过「target 进 frontier」那行，
若在那里 mint，**重放就会换新身份** → Resume 时误触发"代际作废"→ 用户刚答的答案又失效。
身份必须跟着 directive 走、随 cp 持久化；**只有"节点重跑重新产出"才换新 token**。

> 两家印证同一原则：LangGraph 的 `task_id` 编的全是**被冻住的那个 cp** 的输入
> （`checkpoint_id` / `step` / `idx`，`_algo.py:990-997`）——重放算出来还是同一个；
> eino 恢复时 `input = tnState.Input`（`compose/tool_node.go:1062`）**丢弃传入的 input、
> 换成中断时存的那份** → callID 原样不变。

**③ 用进程内单调计数器（`"i" + base36`）而非 UUID**：InstanceID 的作用域是「单个
(sid, ns) 的 cp」，且随 frontier 持久化——跨进程 Resume 读的是存下来的值、不重新 mint，
**不需要全局唯一**。计数器无外部依赖，且排查时一眼看出投递顺序。

### 三方对照（学原则，不搬结构；细节见 [checkpoint-storage.md](../description/three-way-comparison/checkpoint-storage.md)）

两家的**共同铁律**：**待跑列表永不缩水，已完成的留在原位打标跳过，完成集另存 side-map。**

- **LangGraph**：完整 Send 列表进 `checkpoint["channel_values"][TASKS]`（Topic channel），resume 时
  `for idx, _ in enumerate(tasks_channel.get())` **全量重建**，已完成的靠 `_reapply_writes_to_succeeded_nodes`
  贴回 writes 后被 `if not t.writes` 过滤掉。**它敢用位置当身份，前提就是列表不缩水**——而列表冻住
  又是因为 `after_tick()` 原子（中断时 `_runner.py:690` 抛异常 → `main.py:2984` 的 `after_tick()` 永远到不了
  → TASKS 不清、不写新 cp、HEAD 冻在旧 ckpt）。**task_id 从不落盘**，每次现算，所以必须确定性。
- **eino**：图调度层**没有**动态 fanout（4 道结构约束，同 nodeKey 多实例不可能）；但 ToolsNode 内部
  真有同类问题，解法是 **`callID`（内在 key）**，`ExecutedTools map[callID]result` 做完成集，
  `n := len(input.ToolCalls)` 恒全量、已完成的 `executed=true` 空跑。**位置在任何一环都没出现过。**

> ⚠️ **不能照抄 eino 的 Address**：它不含 generation，安全靠「跑完不重跑」+「票一次性」两个不变量。
> **我们有「父节点重跑但下游中断态还在」**（`CondFailRerunFrom` / ISSUE-95#1 就是），eino 那套会把
> **旧答案喂给新任务**（`Address.Equals` 认不出代际）。LangGraph 把 checkpoint_id 编进 task_id 才是
> 更强的保证。我们的 fresh-mint 天然带代际。
>
> ⚠️ **eino 自己的注释撒谎**：3 处（`internal/core/interrupt.go:125-127` / `compose/resume.go:113` /
> `internal/core/address.go:239`）声称 InterruptID 是地址字符串，实际是 `uuid.NewString()`
> （`interrupt.go:101`/`:112`）。照注释调研会得出"天然稳定"的错误结论。

### 「重跑 ⟹ 旧答案作废」与两家一致（已核实）

- **LangGraph**：会发生（time-travel/fork，`test_fork_always_re_runs_nodes`）。双保险 —— ①身份自然变
  （fork 造新 checkpoint_id → task_id 全变）②显式作废（`_loop.py:897-900` 丢 RESUME、`:968-970` 丢 INTERRUPT）
- **eino**：**结构上不会发生**（跑完的节点根本不会被 cp 恢复重跑；`compose/` 里 retry 零命中）

### ⚠️ 身份是**三个正交维度**，不是一个（2026-07-17 穷尽盘点后修正）

初版方案写「删 visit + branchIdx，全换 token」——**错的，会静默串台**。盘点后的真实分工：

| 维度 | 谁用 | 稳不稳 | 处置 |
|---|---|---|---|
| **图级实例 token**（新增）| send 派生实例（**唯一**多实例来源）| fresh-mint + 持久化 → 稳 | **新增** |
| **`visit`** | **静态边节点的跨完成访问代际** | 静态边恒单实例 → 无兄弟污染、无并发竞态 → **稳** | **保留，不删** |
| **`branchIdx`（图级语义 = frontier 位置）** | 只有 MED-3 guard（`subgraph.go:123`）真用它 | 位置 → 不稳 | **删**（guard 一并拆）|
| **`branchIdx`（tc 下标语义）** | ask_user / approval / a2a / handoff_tool | 锚在**持久化的 channel 值**上 → **稳** | **拆成独立 getter** |

**根子**：`nodeBranchIdxKey` **一个 ctx key 背了两个完全不同的语义**——「frontier 位置」与
「tool_call 在 assistant message 里的下标」。`WithCurrentToolCallContext`（`compose_v2/interrupt.go:552`）
把前者**覆盖**成后者。四个消费点想要的全是后者，只是碰巧借用了同一个 key。
**「删 branchIdx」会把好的那半一起删掉。**

**为什么 token 并不掉 tc 下标**（算术，非推测）：N 个并发 tool_call 跑在**同一个 tools 节点实例**里
（`basic_agent.go:1727` 的 goroutine 是**节点内** fanout，不是图级 fanout）→ token 相同；而
`WithCurrentToolCallContext` 给**每个 tc 新建独立 counter**（`interrupt.go:553` `var counter atomic.Int64`）
→ 各自 `NextInterruptIndex` 从 0 起 → `MakeInterruptID(token, idx)` 对 tc0/tc1 **算出同一个 ID** →
`MergeInterrupts` 后 N 条 query 同 ID → `WithResumeAnswers` 只配得上 1 条 → **静默串台**。
（哨兵：`agent_v2/issue81_toolsfn_fanout_test.go:319-328` `TestISSUE81_MultiInterrupts_MergedAndRecorded` 会抓住——它的注释原话就是「验 InterruptID 全 distinct (branchIdx 维度让 5 维 ID 不撞)」——**别在改测试时顺手改绿**。）

**为什么 visit 不能删**：`agent_v2/repro_test.go:30` `TestRepro_AskUserMultiVisitCrosstalk` 钉死拓扑
`start→A→B→A→end`，A 是**静态边驱动的单实例节点**被完成访问 2 次，靠 visit 区分两次 ask_user。
生产同构 = **BasicAgent ReAct 多轮**（第 1 轮问"目的地?"、第 2 轮问"日期?"）。且 `drainSendSink`
全仓**只有 `graph_loop.go:1306` 一处**、静态边节点走 `nextSet`（`:983`）**根本不产 SendDirective**
→ 没有 mint 点 → 静态边节点**只能**靠 visit。

### 改动面（已穷尽盘点）

- **贯穿 plumbing**：`cm.frontierAtExit` / `cp.CurrentFrontier`(**wire**) / `frontierResult.FailedBranches`
  / `SucceededBranches` / `computeNextTasks(current)` / `executeFrontier(frontier)` / `updateFrontier()`
  —— frontier 成员形如 `{NodeID, InstanceID *string}`（静态边成员 InstanceID 为 nil）
- **三处会丢 token**（必须带上）：`:698`（`res.FailedBranches`）、`:946` / `:973`（`[]NodeID{from}`）
- **消费点改用它**：
  - InterruptID：`InstanceID 有 → 用它；无 → 用 visit`（+ 正交的 tc 下标维度）
  - `RunSubAgent` 的 NS 派生（`run_subagent.go:73`，替 tcID-only）
  - **`subgraph.go:112` `childNS := JoinNS(parentNS, string(id))`** ← **初版漏列**：它不含任何实例
    维度。初版以为这是「guard 拆掉后的未来风险」——**实测是今天就活着的 bug**（侧面 5：Resume
    一绕过 guard 就直接走到它）。优先级从"迁移时别忘"提到"现存 bug"）
  - `mergeCompletedSiblings` 的 purge（精确匹配 → **删 `purgePrior` 启发式**）
  - **observability 两个 key 函数**（侧面 6，初版漏列——这是**第 4 类消费点**）：
    `observability.go:668-680` `registryKey` / `:695-711` `spanKey`（node span 无 callID 类的锚点）
    + `:1130` `chunkBufferKey(graphNS, nodeID)`。**是两个独立的 map，修一个不会顺带修好另一个**
  - **`graph_loop.go:663-669` 的 `visits` 记账**（侧面 4）：`for _, id := range frontier { visits[id]++ }`
    遍历条目、按 NodeID 记 → 宽度被当深度。**换 token 不会顺带修好它**（它用的是 Run 局部 map、
    与 `cm.visitCount` 零交集）——需单独处置，正解是按 frontier 里的 **distinct NodeID** 计数
- **拆 tc 下标维度**：`WithCurrentToolCallContext` 的 branchIdx 参数拆出独立 ctx key
  （如 `toolCallIdxKey` + `GetCurrentToolCallIndex`），与图级 token 正交共存；
  `interrupt.go:539-546` 的「3 维度同时注入」godoc 改 4 维
- **`handoff_tool.go:139`**：`handoffPendingKey(ns, hop, idx)` 的 idx **不能迁 token**——它要的是
  tc 下标（`handoff_agent.go:672` 的 `maxToolCallsGuard = 64 // 单轮 tool_call 上限` 直接坐实），
  迁到新的 tc 下标 getter
- **conformance + 业务 backend** wire 同步 + **`checkpointSchemaVer` 必须 bump**（`serialization.go`）
  —— 否则旧 cp 的 unknown field 被 Go 静默忽略、按新语义半恢复
- **收益**：MED-3 的 fail-fast guard 可拆 + `purgePrior` 启发式可删

### 迁移面（穷尽盘点结论）

**干净的部分**：
- `cm.visitCount` / `cp.NodeVisits` **没有任何非身份消费者**——maxVisit 走的是 `graph_loop.go:643`
  的 **Run 局部 `visits := make(map[NodeID]int)`**（不持久化、与 `cm.visitCount` 零交集）；
  LoopAgent 轮次 / HandoffAgent hops 也都不用它（hops 走 `Sum` channel）
- **`example/` 零直接调用**（`grep -rn "GetCurrentNode|MakeInterruptID|NextInterruptIndex|WithCurrentToolCall" example/` = 0）
  —— 全走 auto-fill 或把 ID 当不透明串
- 生产 `MakeInterruptID` 调用点**恰好 4 个**

**静默面（删了编译能过、行为悄悄变）**：
- **auto-fill 的 7 个业务消费点**（`channel_chat_web/hooks.go:24` / `plan_chat_web/hooks.go:48` /
  `channel_chat_demo` / `channel_smoke` ×4 / `parallel_hitl_smoke:141`）一行都不用改，但算出的 ID
  值变了 → **跨版本 Resume 断裂**。`agui` 更严重：ID 经 HTTP/JSON 送前端（`agui/handler.go:32`
  `json:"interruptId"`）由前端 echo 回来 → **前端持有的旧 ID 永远配不上**
- `cp.NodeVisits` 若动 → 旧 cp JSON 的 unknown field 被静默忽略 → 必须 bump schema ver

### `CompletedFrontier` 要不要带 instanceID —— **要，但只为一个消费点**（已验）

| 消费点 | 多实例时 | 要 instanceID？ |
|---|---|---|
| 出边计算（`edgeFrontier` → `computeNextTasks`）| `nextSet` 是 map，重复 NodeID 只让 cond 多评估几次、target 去重 | **不要**（NodeID 够，只是浪费）|
| purge 过滤（`rerun[id]` / `purgePrior[id]` / `rerun[d.owner]`）| 3 个 `id="Z"` 全命中 `rerun={Z}` → 全丢 | **必须**——即侧面 3 |

**净简化**：有了精确实例身份，`7d6b913` 加的 `purgePrior = rerun \ frontier` 启发式**可整个删掉**
（它本就是"没有实例身份时的将就"）。两个场景都验过用 `purgePrior == rerun` 即正确：
- 侧面 3 场景：`rerun={Z#u2}` → prior `[Z#u1, Z#u3]` 不匹配 → 保留 ✓；新批次只滤 u2 ✓
- ISSUE-95#1 原场景（单实例 A）：`rerun={A}` → prior `[A]` 匹配 → 清 ✓

---

## 实施前待办（已全部完成）

- [x] `CompletedFrontier` 是否需要 instanceID —— **要**（仅 purge 消费点；出边计算 NodeID 够，
      `nextSet` 是 map 会去重）。净收益：能删掉 `purgePrior` 启发式
- [x] 旧身份 API 的**穷尽**迁移面盘点 —— **结论推翻了初版方案**：身份是**三个正交维度**
      （token / visit / tc 下标），不是一个。「删 visit + branchIdx 全换 token」会静默串台。
      `ask_user`/`approval` 的迁移答案：**不是迁到 token，是把 `nodeBranchIdxKey` 背的两个语义拆开**。
      详见上方「身份是三个正交维度」+「迁移面」两节。5 条关键声称已逐条独立 grep 复验、0 误判
- [x] 有没有第 4、第 5 个同根侧面 —— **有，找到 3 个**（侧面 4 / 5 / 6，6 含两个独立 map）。
      每个都配了对照组证明是 NodeID-vs-实例这一维、非并发度或环境。**侧面 5 推翻了本文档初版
      三处前提**（原以为 MED-3 guard 挡住了 subgraph 那半，实测没挡住）。全部已独立复验、非采信报告

> **狩猎覆盖边界**（2026-07-20 更新：下面四条当时列为"没过"的面，收尾审计已全部补验）
>
> | 当时没过的面 | 现在的结论 |
> |---|---|
> | `durability=sync/async` 下的多实例 | **已验，无问题**（**仅审计探针，无回归测试**）——两档 cp 均带互异 token；并行数组在 Snapshot 同一把 RLock 下由同一 slice 生成，async worker 拿的是已解耦的不可变 cp，无错位面 |
> | `soft_cancel` + 多实例 | **已验，无问题**（**仅审计探针，无回归测试**）——超步边界停 + 存档后 Resume，三个实例身份原样对上 |
> | 嵌套多实例（Z×2 各自再 `SendFrom(W)`） | **已验，无问题**（**仅审计探针，无回归测试**——在环最接近的 `multi_instance_test.go` 只到各投 1 个 W、断言执行次数而非身份 distinct）——4 个 W 身份全异、childNS 全异；同进程内 fresh-mint 天然不撞（本文档断言成立） |
> | `sql/checkpointer` + conformance wire | **部分**——conformance 断言已补齐四个身份字段，但 `InMemoryCheckpointer` 直存指针不过 JSON，那四条身份断言在默认 `go test ./...` 里不判别。在环补偿是 `TestISSUE96_InstanceIDWireName`（钉四个身份字段的 JSON 键名，改 tag 名即 FAIL）；真后端 wire 往返仍需业务 backend 或显式跑 `./redis/...` |
>
> **本轮新的覆盖边界**（诚实标注，留给下一轮）：多副本**并发** Resume 同一 sid（cp 层无乐观锁，
> 与实例身份正交但会放大发号器重号）；`agent_v2` 9 编排包装层是否有自己的 NodeID 粒度过滤；
> `PendingSendWrites` 与实例身份的组合在 `restoredMembers` 降级路径下的行为。
>
> **已过且确认干净的面**：`pendingSendWrites`（存整批 writes，不按 NodeID 索引）；
> `FailedBranches`/`SucceededBranches` 的**数量**（失败几个重跑几个是对的，身份漂移属侧面 1）；
> `completedFrontier` + `edgeFrontier` 重复评估 cond（实测会多评估 N 次，但 cond 是纯函数 → 无害）；
> `ParallelAgent`（distinct `worker_<name>` + 同名 panic 守卫 → 框架编排不产多实例）；
> `skill_tool`/`agent_tool` 的 `JoinFanoutNS`（走 tcID hash，节点内 fanout → 安全）

## 实施结果（2026-07-20）

### 身份协议最终形态

`FrontierMember{NodeID, InstanceID}` —— 成员 = 节点 ID + 实例身份。InstanceID 由
`newInstanceID()` fresh-mint（`"i" + base36(进程级单调计数器)`），**mint 点在
`graph_loop.go` 的 drain-then-stamp 段**（`runNodeWithHooks` 内，与 owner stamp 同一处；`drainSendSink` 自身只取出清空、不 mint），不在「target 进 frontier」那一刻——deferred
send 从 cp 重放回来时不能换身份。静态边 / cond 边节点恒空串（它们恒单实例，
`computeNextTasks` 的 `nextSet` 是 map 会去重）。

**身份是三个正交维度**，不是一个（见上方同名小节）：图级 token / `visit` / tc 下标。

### cp wire（`CheckpointSchemaVer = 8`）

| 字段 | 语义 | 漏序列化的后果 |
|---|---|---|
| `FrontierInstanceIDs []string` | 与 `CurrentFrontier` 按下标一一对应 | 多实例 Resume 后实例互相冒充 |
| `CompletedInstanceIDs []string` | 与 `CompletedFrontier` 按下标一一对应 | 重跑成员的旧记录清不掉 → Target 双发 |
| `SerializedSend.OwnerInstanceID` | 发出者自己的身份（"谁发的"） | 同上——purge 匹配键恒 `{Owner,""}` 匹配不上 |
| `SerializedSend.InstanceID` | 目标实例的身份（"发给谁"） | 重放出的 N 个实例全空身份 → HITL 撞车 / 子 cp 互覆 / span 共槽 |

四个都是「**漏了不报错，只是静默退化**」，故 `checkpointer_conformance` 全部配了断言。
空串元素必须保留——它表达「该成员是静态边节点」不是「没值」。

### 六个侧面的修法

| 侧面 | 修法 | 哨兵 |
|---|---|---|
| 1 · InterruptID 跨 Resume 漂移 | 身份三维拆开，`InterruptSite` 一站式取；有 InstanceID 时 visit 置 0（两者互斥） | `instance_identity_test.go` |
| 2 · 子 cp 互覆 | childNS 由实例身份派生（`InstanceFanoutIndex` 优先于 tcID hash） | `subgraph_fanout_instance_test.go` |
| 3 · 兄弟记录被误清 → 双发 | 重跑目标带实例身份；purge 按 `FrontierMember` 精确匹配（`purgePrior` 启发式已删） | `multi_instance_test.go` |
| 4 · 宽度被当深度 | 访问计数按 `distinctNodeIDs`（一超步至多 +1） | `visit_width_test.go` |
| 5 · MED-3 guard 装饰性 | guard 拆除，能力放开（原理由已不成立） | 同侧面 2（测试由「验拒绝」反转为「验隔离」） |
| 6 · span / chunk 共槽 | `registryKey` 与 `chunkBufferKey` 加实例维度；身份注入提前到 fire `OnNodeStart` **之前** | `observability/node_instance_test.go` |

### 审计再抓的 5 个（4 路单视角并发审 + 交叉验证）

| # | 问题 | 严重度 | 修法 |
|---|---|---|---|
| 1 | `SendDirective.instanceID` 从未进 wire——三个身份字段改了两个漏第三个 | HIGH | 上 wire + conformance 断言 |
| 2 | 发号器跨进程重启重号：新号逐位撞 cp 里的旧号 | HIGH | `Restore` 时抬过本 cp 最大 token（扫四处） |
| 3 | fan-out 宽度失去唯一兜底（侧面 4 修复的副作用） | HIGH | `WithMaxFrontierWidth`（默认 10000）在超步入口拦宽度 |
| 4 | cond 返未注册 target 的同族分支仍用 `staticMembers` | MED | 与 `CondFailRerunFrom` 分支对齐 |
| 5 | 越界 fanout 下标静默退化成「无 fanout 维度」 | MED | 超 int32 时 hash 进 int32 空间，不再返 false |

**#2 值得单独记**：它是「不报错、结果错」最坏的一档——重启后第二个任务顶着第一个的身份，
**静默吃掉第一个任务的用户答案、自己从不提问、Run 还报成功**。而且 **#1 的修复放大了它**：
上 wire 前重放拿空串（跟旧号好歹不同），上 wire 后拿真 token → 同一份 cp 里两个无关任务
逐位同身份。两个修复必须一起看。

**#3 是修复引入的回归**：宽度不该算深度是对的，但 `maxVisit` 原本是宽度的唯一上限。节点在
自己的 NodeFunc 里 `SendFrom` 自己 N(≥2) 次时宽度**逐超步相乘**，实测 `maxVisit=6` 执行
63 次（2^6−1）、停用宽度防线后执行 4194303 次（2^22−1）直到撞 ctx deadline（一次性 sanity-revert 探针数字；回归测试用更小的宽度上限跑得快）。

### 顺带修的两处测试假绿

- **观测层测试只覆盖 fix 的 1/3**：手工造实例身份直调 `OnNodeStart`，绕开 `runNodeWithHooks`
  → 「注入早于还是晚于 hook」结构上观察不到。改走真 `compose_v2.Run` + `SendFrom` fan-out。
- **改成真实路径后 chunk 那半仍假绿**：没有屏障时 Go scheduler 自然串行化，每个实例发完
  chunk 就立刻 flush，共享 buffer 也看不出问题。加 channel 屏障（带超时防死锁）才抓得到。

### 记录性决策（判断为不改代码，写进注释而非留在对话）

- `CondFailRerunFrom` 在多实例下重跑的是**全部** N 个实例（含上次已成功的），非幂等 reducer
  的放大倍数是 2N 而非 N+1 —— 已写进该 policy 的 godoc。`WithCondFailPolicy` 目前**生产代码**零使用（仅测试覆盖），
  只有业务自建图 opt-in 才命中。
- `frontierResult.SuccessWrites` 注释原承诺「按 frontier 索引序合并——确定性」，实测同一张
  静态 fan-out 图跑 40 次出现 5 种顺序：索引序确实遵守，但 frontier 里静态边成员的次序来自
  对 map 的遍历。**既有行为、非本次引入**，与 reducer 约定（要求满足交换律）一致，已订正注释。
- `restoredMembers` 长度错位时降级为「全空身份」，原注释说这比按错位下标读「更好」——实测
  两个分支都坏（全空身份对同 NodeID 的 N 个成员就是互相冒充）。已改成「止损不是修复」的措辞。

## 已知会被本 issue 推翻/取代的既有决策

- `doc/issue/issue-86-graphloop-cond-improvements.md:154-165` LOW-1「branchIdx 在 cond 路径永远
  写死 0，by design」—— **已由 ISSUE-96 取代**：cond 路径现在按 `instances[from]` 设重跑目标，
  成员各带实例身份；branchIdx **已完全退出**——`InterruptSite` 无此维度，`MakeInterruptID` 与 `GetCurrentNodeBranchIdx`/`nodeBranchIdxKey` 整条链已删（生产零调用，且其 godoc 在教被推翻的用法）
- ISSUE-95 finding 2（`issues.md:39` 「🔴 已实证待修」）—— 本 issue 的侧面 1，修复合并到此
- MED-3 fail-fast（原 `subgraph.go`）+ 其回归 原 guard 回归测试
  —— **已拆除**；测试已由「验拒绝」反转为「验隔离」，文件更名
  `compose_v2/subgraph_fanout_instance_test.go`

## 验收标尺（换 token 后必须仍绿 / 必须反转的测试）

| 测试 | 它钉死的不变量 |
|---|---|
| `agent_v2/repro_test.go:30` `TestRepro_AskUserMultiVisitCrosstalk` | **静态边单实例节点跨完成访问的代际**（visit 维度）——生产同构 = BasicAgent ReAct 多轮 |
| `agent_v2/issue81_toolsfn_fanout_test.go:319-328` | **N 个并发 tc 的 ask_user ID 全 distinct**（tc 下标维度）——注释原话「验 InterruptID 全 distinct (branchIdx 维度让 5 维 ID 不撞)」，会直接抓住「token 并掉 tc 下标」 |
| `agent_v2/sdk_features_test.go:286` | 同节点内多次 ask_user 按 `idx` 区分 |
| `agent_v2/handoff_agent_test.go:203` | 跨 hop 靠 graphNS 区分；**测试设计明写必须用 framework auto-fill 协议否则假绿** |
| `compose_v2/multi_instance_test.go` | issue-95#1 的多实例哨兵，**已扩**：cond 重跑（含未注册 target 同族分支）下的实例整体性 |
| ~~`compose_v2/subgraph_fanout_guard_test.go`~~ | MED-3 guard —— **已反转**为 `subgraph_fanout_instance_test.go`（验隔离而非验拒绝） |

编译会断的（4 处，删 API 时同步改）：`agent_v2/approval_test.go:372-374` /
`agent_v2/r29_resume_ns_forward_test.go:42` / `agent_v2/repro_test.go:193` /
`compose_v2/checkpointer_conformance/save_load.go:128,226-235`（**注意这是 shipped 包不是
`_test.go`，业务自研 backend 也会断**）

> **纪律**：前 3 个侧面全部由探针实证，没有一个是"读代码觉得有问题"。本轮多次栽在
> 「拿没验的断言当决策依据」上（"运行时 N 路是被封的能力" / "finding 2 血溅半径窄" 都是错的），
> 故实施前必须把上面两项**验完**再动手。
