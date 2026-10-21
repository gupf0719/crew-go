# 主题：Checkpoint 存储模型

> 本文档对照 **crew-go v2（channel-based）/ LangGraph 0.6.10 / eino 主分支**。最初为 v1 时代 cp 存储设计研究，channel-based 重设计落地后已更新到 v2 基线。
>
> **v2 cp 完整协议（数据结构 / 写入时机 / Resume / 并发保护）以 [`checkpoint.md`](../../v2/checkpoint.md) 为权威**，嵌套 NS 寻址见 [`channel-engine.md`](../../v2/channel-engine.md) §8；本文档聚焦三方对照速查 + 我们的设计取舍 + 决策记录，不重复实现细节。
>
> 📍 **2026 H1 更新指针**：本文档 LangGraph 列基线是 **0.6.10**（已 stale）。LangGraph 1.2.5 + eino v0.8.0 的 **cp 类新发现**——DeltaChannel 增量 cp / 选择性 resume 机制（eino，与 crew-go 同语义）/ 嵌套 cp 取舍（内嵌 vs 物理分离）/ coordinate-key 寻址分层（#8053）——见 [`eino-langgraph-upgrades.md`](eino-langgraph-upgrades.md) §三，本文档不重复。（注：`durability=sync/async/exit` 是 LangGraph 长期已有；**crew-go 已移植此能力**——见下方速查表 durability 行 + §不足 #2「已做」+ 决策记录「本次」行。）


## 触发场景

需要查阅本文档的设计决策：
- 改 Checkpointer 接口签名（新增 / 删除 / 重命名 method）
- 改 cp 内部存储数据结构（key 设计、嵌套形态）
- 设计 cp 生命周期语义（何时写 / 何时删 / Status 标记）
- 决策"嵌入式 vs 物理分离"的子图 cp 持久化方案
- 评估 batch / 原子写需求

## 三方对照速查表

| 维度 | crew-go v2 | LangGraph | eino |
|---|---|---|---|
| **存储 key** | `(sessionID, ns)` 二级 map | `(thread_id, ns, cp_id)` 三级（另有 blobs 四元组 / writes 五元组两张表）| 裸 `string` |
| **会话身份**（跨执行）| **不在 cp 层**（ISSUE-98 删了 `cp.ThreadID`）——由 `agent_v2` 会话历史层按 `WithThreadIDCtx` 声明的 ThreadID 组织 | `thread_id` = **主键第一段**，调用方每次显式传，核心层缺了 fail-fast | **无此概念**（grep 零命中）|
| **单次执行身份** | `sessionID`——**占了主键位** | `run_id`——只进 metadata（可 filter / 可批删），不进任何 PK | 无 |
| **历史 cp** | 仅最新（成功即清设计） | 全历史链（parent_id 串联，可 time-travel） | 仅最新 |
| **写入 API** | `SaveNS` / `SaveBatch` | `put` + `put_writes`（durability=sync/async/exit 控制） | `Set([]byte)` |
| **durability 写时机** | `WithDurability`：`exit`(默认,仅出口 defer)/`sync`(每超步同步写)/`async`(单 worker 后台写,FIFO+join-before-cleanup)——**per-超步边界**粒度，**完整崩溃恢复**（单节点超步 = per-task） | `exit`/`sync`/`async` + `put_writes` **per-task** 更细（仅 fan-out 超步 mid-execution 硬崩时比 per-超步少重跑已完成分支，见不足 #4）| 无（仅 interrupt 路径写） |
| **读取 API** | `LoadNS` / `ExistsNS` / `ListPending` / `LoadAllInSession` | `get_tuple` / `list`（time-travel） | `Get([]byte)` |
| **Status 字段** | `Pending`（中断 / 失败 / ctx 统一写）/ `Unset`（默认）—— 不设失败态 | 无（每步都写） | 无 |
| **生命周期清理** | `Cleanup(keepLastN)` + `DeleteSubtree` | `delete_thread` 整 thread 删 | 无 |
| **嵌套 cp 持久化** | 同父 store 物理分离写 `(parentSID, childNS)`（详见 channel-engine §8） | 同 store 不同 ns，独立写 | 父结构体 `SubGraphs` 字段嵌子结构 |
| **典型后端** | InMemory（已有）+ 业务自实现 `Checkpointer` 接口 | InMemory / Postgres / SQLite / Redis | InMemory |
| **同会话并发执行** | **opt-in 世代校验**（`WithStaleWriteGuard` / `...Ctx`，默认关闭）——写/删 cp 前比 `Epoch`，被推进过则返 `ErrCheckpointStale`。不阻止副作用双跑、非原子，只让冲突不再静默 | 库内无（曾有 `CheckpointNotLatest`，2025-06 主动删）；真正的控制在闭源 Platform | 无机制，且未进入设计视野 |

## crew-go 设计（v2 概要）

> **当前实现的字段清单 / 生命周期 / 寻址见 [`checkpoint.md`](../../v2/checkpoint.md)（权威篇）**。
> 本篇保留三方对照与**决策记录**——「当时为何这么取舍」留在这里，不搬去权威篇。

### 存储数据结构

`Checkpoint` 存的是 **channel state**（不是 message / event 历史），元数据 + channel 值同结构
（`compose_v2/checkpointer.go`）。**字段清单不在本篇复制**（复制过一份，已漂：曾记成
「Status 只有 Pending / Unset」「`CurrentFrontier []NodeID`」，而实际早已是三态 +
`[]FrontierMember`）——见 [`checkpoint.md §2`](../../v2/checkpoint.md)。

下面只留三方对照需要的**取舍理由**。

**`PendingSendWrites` 补的是"target 与 payload 分裂持久化"的裂缝**（引擎深审 F-B，2026-07-17
`999113a`）：`SendDirective` 的 target 进 `CurrentFrontier`、payload 走 `ApplyAll` 进 `Channels`
——两条路。payload 那批 ApplyAll 失败**一条都没提交**时 target 已进 frontier，而产 send 的
节点已成功、R27a 明确不让它重跑（否则 visit 漂移 + 副作用双跑）⟹ payload 无从再生，Resume
时 target **静默读到零值**。存下这批被回滚的 writes，Resume 在跑任何节点之前补 apply 即可
（target 仍留 frontier，R27a 决策不变）。

> **三方都没有这个裂缝**（学原则、不搬结构）：LangGraph 的 Send **自带 payload**（`packet.arg`），
> target 与 payload 是同一对象物理不可分裂，且 frontier 每步从 channel 现算、不单独落库；
> eino 是 frontier + payload 同 blob 一次原子 Set。我们没照搬任一结构——**持久化单元本来就是
> cp 本身**（`CurrentFrontier` 与新字段由同一次 `SaveNS` 原子写出），故只需补一个字段，
> 不必把 payload 塞回调度流（塞回去会让 target 被调度两次）。

**`SerializedSend.Owner` 是 owner 归属进持久层**（ISSUE-95#1，`90a94d2`）：我们此前只有
per-超步的临时并列 slice，cp round-trip 后就没了 → 重跑节点时它的旧 deferred send 清不掉
→ Target 双发。

> **三方对照（两家的"没这个问题"是两种不同原因，别混为一谈）**：
> - **LangGraph 有同款问题、也是靠 owner 身份解**：持久化单元就是 `(task_id, channel, value)`，
>   saver 主键含 task_id（sqlite / postgres 均 `PRIMARY KEY (thread_id, checkpoint_ns,
>   checkpoint_id, task_id, idx)`）、`put_writes(config, writes, task_id, task_path)`。
>   **我们与它同构**——都得回答"这条旧产物是谁发的"。
> - **eino 没有这个问题，是因为结构不同、不是因为它也带 owner**：eino 图引擎**根本没有
>   owner / task_id 概念**（`compose/` + `internal/core/` 里 `owner` 仅 1 处散文注释、
>   `task_id` 0 处），也**没有 Send / 动态 fan-out 等价物**（0 处），故不存在"并发子任务的
>   持久化产物待归属"这回事。它的 `cp.Inputs[t.nodeKey]` 按 **target 键控**（`graph_run.go:548`）
>   ——正是 owner/生产者的反面，重跑时覆盖而非追加。

⚠️ **owner 是 NodeID 粒度、执行单位却是 branch 实例**（同 Target 投 N 次 → 同 NodeID N 份），
按 NodeID 清会误删兄弟实例的记录——踩坑记录 + 判据见 [issue-95](../../issue/issue-95-graphloop-sendfrom-double-send.md)。
（LangGraph 的 task_id 是真·per-task 身份，不吃这个亏；我们没抄那套身份体系，代价即此。）

`InMemoryCheckpointer` 用 `data[sessionID][ns]` 二级 map（`checkpointer.go`）；**crew-go 已内置持久 Checkpointer：`redis/`（Redis）+ `sql/`（Postgres + MySQL 双方言）独立 module**，业务也可自实现 `Checkpointer`（8 方法接口）。

### 生命周期（成功即清）

`compose_v2.Run` 出口 defer 自动管理：

- **失败 / InterruptError / ctx 取消** → `SaveNS(sid, ns)` 写 cp（Status=Pending）
- **成功** → `DeleteSubtree(sid, ns)` 清本 graph cp 子树（成功路径不留 cp）

cp 仅作"异常状态快照"存在——正常完成不留。**所有 `runErr != nil` 都写 cp，但要标清是哪一种**（`graph_loop.go`，判据是 `shouldStayPending`——**比 `IsControlFlowSignal` 宽**，还含 `CondFailRerunFrom` 与坏存档两类「返业务错但框架要它被重跑」的，`compose_v2/retry.go` 函数体为准）：中断 / ctx 取消 / 软取消 / 陈旧写回写 `Status=Pending`（等答案的挂起），其余业务错 / hook 错 / 基础设施错写 `Status=Failed`（失败现场）。三态 `Unset`/`Pending`/`Failed` + `Status.Resumable()` —— ⚠️ **2026-08-13 起 `Status` 只是报告位，不参与控制流**：三处闸门统一按**内容**判（`Checkpoint.HasResumableWork()`，frontier 或挂起非空），**失败档也被捡起、从失败节点续跑**。所以上面那个判据只决定「标什么位」，不决定「能不能续」——后者另有谓词 `LeavesResumableCheckpoint`。见决策记录。

**durability 每超步写与「成功即清」不冲突**（`WithDurability`，`compose_v2/durability.go`）：`sync`/`async` 档让框架在每个超步边界额外写 cp（`exit` 默认不写、保持上面的纯出口-defer 行为）——但写的是**同 `(sid, ns)` key 覆盖**，成功路径的 `DeleteSubtree`（上面那条，**无 durability 条件**、对所有档都跑）照清。所以 sync/async 只在**执行中途**多一份"硬崩可恢复"的 cp，成功完成后一样不留。崩溃恢复价值需配**持久** Checkpointer（`redis/` / `sql/`，InMemory 随进程死救不回）。详见 §不足 #2 + 决策记录「本次」行。

### 会话身份：主键层级的取舍（2026-07-22 四方调研）

**核心差异不在字段有无，在于「一次执行」和「跨执行的会话」谁占主键位。**

| | 存储主键 | 「一次执行」 | 「跨执行的会话」 | 历史 |
|---|---|---|---|---|
| **crew-go** | `(sessionID, ns)` 二级 | `sessionID`——**占主键位** | 不在 cp 层——`agent_v2` 会话历史层（ISSUE-98）按 ThreadID 组织 | cp 无（成功即清）；历史层 append-only 留全量 |
| **LangGraph** | `(thread_id, ns, checkpoint_id)` 三级 | `run_id`——只进 metadata，可 filter 可批删，不进任何 PK | `thread_id`——**主键第一段** | 全链（parent_id 串联，可 time-travel）|
| **eino** | 裸 `string` 一级 | 无概念 | **无概念**（全仓 grep `threadID\|sessionID\|conversationID` 零命中）| 无（cp 只在挂起时存在）|
| **Grok harness** | `(cwd, session_id)` | `prompt_index`——session 内部索引，不是存储键 | `session_id`——**就是主键** | `updates.jsonl` append-only |

三家外部实现的共同结构是**「会话占主键位，一次执行是它内部的东西」**。crew-go 反过来——最细粒度的东西占了主键位。**曾经**试图在 cp 上加 `ThreadID` 字段补会话维度，但那个字段没有天然的产生者（框架产 cp 时不知道业务的会话边界）。ISSUE-98 的结论是**不在 cp 上补**：会话身份由 `WithThreadIDCtx` 声明、会话历史交给独立 append-only 层，cp 主键保持最细粒度不动。

**LangGraph 试过我们这个形状并撤回**：`ShallowPostgresSaver` 的 `PRIMARY KEY (thread_id, checkpoint_ns)` 正是二级单槽、只留最新，2.0.20 弃用、3.0.0 移除，改判为「键永远三元组，写多写少交给 `durability` 控制」——即**「保留链」和「少写盘」是两个正交旋钮，不该二选一**。

**我们曾经的不一致**：`cleanupOnSuccess` **无条件**清干净跑完的 cp（无开关），所以当年的 `ListByThread` / `LatestByThread` 只能看到**当前挂起**的 run——3 个成功 run + 1 个挂起 run 标同一 ThreadID，只返那 1 个。而两个 API 的 godoc 承诺的是"历史 run"和"接着上次对话继续"。

对照三家：**「清不清历史」和「有没有历史 API」必须一致**——eino 清且无 API ✓、LangGraph 留且有 API ✓、crew-go 当年清却有 API ✗。

**决策（ISSUE-98，已落地）**：**不改 cp 主键、删掉伪历史 API、另起一层做会话历史**。cp 保持「单次 Run 临时态、成功即清」不动（那是防同 sid 二次 Run 载到旧 cp 的正确性机制，不能为了留历史牺牲它）；`cp.ThreadID` 字段 + `ListByThread` / `LatestByThread` + `ErrThreadNotFound` 全部删除。会话历史交给 `agent_v2` 的**独立 append-only 层**（`WithHistory` + `HistoryStore`，按 `compose_v2.WithThreadIDCtx` 声明的会话身份组织，逐字留存消息），opt-in。这样「清不清历史」与「有没有历史 API」在 cp 这一层重新一致（清且无 API ✓），历史能力落在真正 append-only 的层上。设计见 [ISSUE-98](../../issue/issue-98-conversation-history.md)。

> 为什么不学 LangGraph 改主键三级：它的 checkpoint 无消息级归属（`CheckpointMetadata` 只有 source/step/parents/run_id），而我们有 9 编排、owner 归属是核心场景；且「保留全链」要放弃「成功即清」。两条代价都比「另起一层」大。

**三个可直接借鉴的点**（与主键取舍无关，独立成立）：

- **Grok 的双视图物理分离**：`updates.jsonl` append-only 永不重写（审计/重放真相源）+ `chat_history.jsonl` 可整体替换（LLM 输入视图），压缩和 rewind 都只往前者追加 marker。我们记过的"审计视图 vs LLM 输入视图未对齐"类 bug，在这个结构下不可能出现——只有一个真相源，另一个是可重算的缓存。
- **Grok 的并发处置：让写在语义上可交换**，而不是检测冲突——单调字段永不回退（`last_active_at = max(existing, now)`）、计数用增量 patch 而非绝对值、判定放锁内。它明确知道乐观并发检测这条路并选择绕开。**能设计成单调的就不需要检测**——对我们 cp 里的计数/时间戳类字段仍然适用。
- **Grok 的 `last_active_at` 语义**：只有真实内容产生才推进，元数据写和远端同步都不算——让"最近一个会话"的排序不被后台任务扰动。

**LangGraph 的 thread_id 兜底分层**（复核修正）：`pregel`/`checkpointer` 核心**不兜底**（缺了直接 KeyError，且 `checkpoint_ns` / `checkpoint_id` 都有默认值、唯独 thread_id 没有）；官方 SDK 客户端 mint uuid4、`RemoteGraph` 允许缺失走 stateless run。即「核心 fail-fast，兜底上移」。

### Resume 失败不能降级已存恢复点（2026-07-17 调研记录；已修 `09c97527`：`Restore` 补 `frontierAtExit`）

**我们的洞（已实证，backlog 见 todo.md）**：`Restore` 不恢复 `frontierAtExit`（该字段只被
`updateFrontier` 写），而我们的设计是**所有 `runErr != nil` 出口统一 Snapshot+SaveNS**——
Restore 到 updateFrontier 之间的 early return（现存一处：resume import overlay ApplyAll 失败）
会让 Snapshot 拿空 `CurrentFrontier` **覆盖原本好的 cp** → 业务修好问题再 Resume 得 `err=nil`
静默成功、HITL 问题永久丢。

**两家为什么没有这类坏（机制不同，均已源码核实）**：
- **LangGraph 三重免疫**：① cp 按唯一 id **append-only** 存（`create_checkpoint` mint uuid6 +
  saver 按 id update），写入永不改旧行，失败的 fork 只是"新行没写成"；② **frontier 不持久化**
  ——每 tick 从 `(channel_versions, versions_seen)` 现算（`prepare_next_tasks`），不存在
  "存的 frontier 被空值覆盖"这一整类；③ 写被"apply 成功/超步完成"门控，resume 早期失败发生在
  任何 `_put_checkpoint` 之前。
- **eino 双重免疫**：① 写是 **interrupt-gated 而非 error-gated**——`checkPointer.set` 仅在两个
  interrupt handler 里调，业务错 / resume-restore 失败直接 return、无"错误统一落盘"的 defer；
  ② 每次落盘的 cp 从 live 状态 **fresh 重建**（`cp := &checkpoint{...}`，frontier 取自当场算出
  的 nextTasks），从不回存"读进来、可能半初始化的内存结构"。

**我们的落点（学原则不搬结构）**：不学 eino 的 interrupt-gated——「业务错也写 Pending cp」是
我们的**特性**（CondFailRerunFrom / 业务错后 Resume 重试都靠它）；不学 append-only（单槽覆盖
是既有哲学，见上方决策记录）。要学的是两家共同满足的**不变量：失败的 resume 绝不降级已存
恢复点**。在我们的结构下达成它的最小改动 = **Snapshot↔Restore round-trip 完备**（Snapshot 读
的每个字段 Restore 必须填；对账过全部字段，`frontierAtExit` 是唯一缺的）——修在 Restore 一处，
天然覆盖现在和将来的所有 early return。备选结构性升级（暂不做）：error-path Pending cp 与
"已确认恢复点"分槽存（局部借 append-only 的"不覆盖"性质）。

### 嵌套 cp（物理分离）

子图 cp 通过 `SaveNS(parentSID, childNS)` 写到**父 store 的独立 ns 路径**（`childNS` 由 `RunSubAgent` 按调用身份派生：位置（实例维三选一：图级 fanout 实例 / 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态重访 visit；fanout 实例节点里又起 tool_call 并发时两维一起进位置）+ 输入内容（同一份输入再分第几次）或 call key，槽号是身份的哈希 `#…`），不嵌入父 cp。多层嵌套各 ns 物理分离、互不覆盖。详见 channel-engine §8。

## LangGraph 设计（v0.6.10）

### 存储数据结构

三级嵌套 + 拆分 blobs/writes（`langgraph/checkpoint/memory/__init__.py:65-80`）：
```python
storage: defaultdict[str, dict[str, dict[str, tuple[
    tuple[str, bytes],   # (cp_blob_serde, cp_blob)
    tuple[str, bytes],   # (metadata_serde, metadata_blob)
    str | None           # parent_id（链式回放）
]]]]
writes: defaultdict[tuple[str, str, str], dict[tuple[str, int], ...]]
blobs: dict[tuple[str, str, str, str | int | float], tuple[str, bytes]]
```

ns 用 `|` 分级、`:` 拆 task_id（`langgraph/_internal/_constants.py:72-74`）。形如 `parent|child:taskid`。

### 写入接口（双轨）

- `put(config, checkpoint, metadata, new_versions)` 持久化整 cp（`memory/__init__.py:323-367`）
- `put_writes(config, writes, task_id)` 增量写中间结果（`:369-405`），支持 `durability="async"` 异步

```python
def put(self, config, checkpoint, metadata, new_versions) -> RunnableConfig:
    thread_id = config["configurable"]["thread_id"]
    checkpoint_ns = config["configurable"]["checkpoint_ns"]
    values = c.pop("channel_values")
    for k, v in new_versions.items():
        self.blobs[(thread_id, checkpoint_ns, k, v)] = self.serde.dumps_typed(...)
    self.storage[thread_id][checkpoint_ns].update({...parent linkage...})
```

`durability` 三档（`pregel/main.py:2487-2490`）：
- `sync`：每步前持久化（最可靠）
- `async`：边执行边写（性能优先）
- `exit`：仅退出时写（最快但崩溃丢数据）

### 读取接口（含 time-travel）

- `get_tuple(config)` 返回 cp + parent_config + pending_writes（`base/__init__.py:156-168`）
- `list(config, *, filter, before, limit)` 按时序倒查所有历史 cp（`:170-192`），可恢复任意中间 step

### 嵌套 cp 持久化

**关键差异**：父子分离写到同 store 不同 ns（`pregel/_algo.py:533, 543`）：
```python
checkpoint_ns = f"{parent_ns}{NS_SEP}{name}" if parent_ns else name
task_checkpoint_ns = f"{checkpoint_ns}:{task_id}"
```

父 cp 通过 `CONFIG_KEY_CHECKPOINT_MAP: {**parent, parent_ns: checkpoint["id"]}` 索引子 cp id（`pregel/_algo.py:610-612`）——天然多 ns 共享同一 store。

### 优势

- time-travel 调试能力（全历史链）
- durability 模式区分（性能 vs 可靠权衡）—— **crew-go 已移植三档**（见「我们的不足 #2」✅）
- 父子 cp 物理分离 + 同 store 共享：对 PG/Redis 等持久化后端友好

### 缺陷

- 每 super-step 都写 cp，存储开销大（需要事后 GC）
- ns 派生完全刚性（`pregel/_algo.py:520-543`），调用方无法覆盖——sandbox 子图想"用完就扔"只能换 thread_id
- 无 Status 字段，cp 类型不可区分（pending vs done vs failed）
- 没有 RunSession / 事件流分层概念

## eino 设计

### 存储数据结构

极简——裸 KV（`internal/core/interrupt.go:27-30`）：
```go
type CheckPointStore interface {
    Get(ctx context.Context, checkPointID string) ([]byte, bool, error)
    Set(ctx context.Context, checkPointID string, checkPoint []byte) error
}
```

无 sessionID / ns 概念，`checkPointID` 是用户自取的字符串。

### 嵌套 cp 持久化

cp 内含 `SubGraphs map[string]*checkpoint`（`compose/checkpoint.go:106-117`），递归嵌入：
```go
type checkpoint struct {
    State     any
    SubGraphs map[string]*checkpoint
    InterruptID2Addr  map[string]Address
    InterruptID2State map[string]core.InterruptState
}
```

整个 cp 一次性 marshal 进一个 byte value。子图 cp 通过 `forwardCheckPoint` 在 ctx 内一层层 unwrap（`compose/checkpoint.go:157-168`）：
```go
func forwardCheckPoint(ctx context.Context, nodeKey string) context.Context {
    cp := getCheckPointFromCtx(ctx)
    if subCP, ok := cp.SubGraphs[nodeKey]; ok {
        delete(cp.SubGraphs, nodeKey) // only forward once
        return context.WithValue(ctx, checkPointKey{}, subCP)
    }
    return context.WithValue(ctx, checkPointKey{}, (*checkpoint)(nil))
}
```

### 生命周期

完全无状态字段。仅在 interrupt 路径写 cp（`graph_run.go:560-565`）。无 Delete API，业务自管清理。

### 优势

- 接口极简（2 个 method），后端实现门槛低
- 嵌套结构直观，父子关系靠 Go struct 自然表达

### 缺陷

- 全树 marshal 一次性序列化，cp 大时性能差
- 无 sessionID / ns 概念，多 thread / 多 namespace 场景需要业务自己拼 ID
- 无 Status 字段，无生命周期管理
- 无 list / batch / subtree query 接口
- `forwardCheckPoint` 用 `delete(cp.SubGraphs, nodeKey)` 一次性消费，重入逻辑脆弱

## 同一会话并发执行（2026-07-20 调研 / 2026-07-21 拍板方案 A）

两个进程副本同时对同一 sessionID 调 `Resume`，各自从 cp 载入同一状态、各自执行、各自写回。

**我们的实测**（`compose_v2.Run` 直接调用，屏障强制并发）：

```
副本A err=<nil>
副本B err=<nil>
有副作用的 work 节点执行了 2 次（期望 1）
报告成功的副本数 = 2
```

`-race` 干净——这是**逻辑双重执行**，不是数据竞争，race detector 永远抓不到。

复现：`go test -tags concurrentprobe ./compose_v2/ -run TestProbe_ConcurrentResume -v`（探针在 `compose_v2/concurrent_resume_test.go`，build tag 隔离故默认 `go test ./...` 不编译）。

### 五方现状（2026-07-21 全部读本地源码核实）

| 框架 | 存储层 CAS/version/锁 | 责任放在哪一层 | 库内实现过防护吗 |
|---|---|---|---|
| **crew-go** | 无 | 上层 `agui`/`a2a` busy + 业务直调裸奔 | 无（本次拍板加 opt-in 写回校验） |
| **LangGraph** | 无（`ON CONFLICT DO UPDATE` 无 `WHERE version`） | **服务端**（闭源 langgraph-api：`runs` 表 + `multitask_strategy` + `FOR UPDATE SKIP LOCKED`） | ✅ **有过又删了**（`CheckpointNotLatest`，因调度器下线非机制错） |
| **eino** | 无（`CheckPointStore` 只有 `Get`/`Set`，签名无法表达条件写） | 没有任何一层 | 从没有（未进入设计视野） |
| **Grok** | 无（刻意；`compare_and_swap` 全仓 0） | **进程架构层**（单 leader flock + 单 actor 独占状态） | 用架构规避，不做存储协调 |
| **MAF** | 无（Cosmos 读到 `_etag` 直接丢弃、盲 upsert） | 外挂 **durable runtime**（Azure Durable Task） | 故意设计成可被多实例重放 |
| **OpenAI Agents SDK** | 无（Session append-only；RunState 可重复恢复） | **应用层** + 服务端 `conversation_locked` 锁 | 无 |

**五方铁律**：没有任何一家在库/框架的存储层做乐观并发防 double-resume；责任统一上移（服务端 / durable runtime / 进程架构 / 应用层）。**副作用双跑靠乐观并发防不住**——它发生在 Save 之前，任何**写回时才判定**的机制（CAS / 版本比对）都只能在活干完之后拒绝。要真防住得在**执行前独占**（LangGraph 服务端的 `FOR UPDATE SKIP LOCKED` 行锁、Grok 的单 leader 进程都属此类），代价是租约/锁的全套复杂度——库层不做，见决策记录。库层能做的是让冲突**不再静默**。

下面按证据分量排序：LangGraph（唯一库内做过、且能坐实删除原因）→ eino（同构反例）→ MAF / OpenAI / Grok（三种不同的"上移"路线）。

### LangGraph：库内曾有防护，2025-06 主动删除

commit `0aff02e18`（"Remove code paths no longer needed"，2025-06-12）删掉了：

```python
class CheckpointNotLatest(Exception):
    """Raised when the checkpoint is not the latest version (for distributed mode)."""
```

以及 `SyncPregelLoop.__enter__` / `AsyncPregelLoop.__aenter__` 里的执行点——形态是**载入时的陈旧性检查，不是写入时 CAS**：「我被要求从 X 恢复，那 X 必须仍是本 thread 最新的 cp；否则说明别人已经推进过了，拒绝执行」。opt-in（`CONFIG_KEY_ENSURE_LATEST` 开关），只有 Kafka 分布式调度器接。

删除理由是 Kafka 调度器下线——这有仓库事实支撑而非从 commit message 推断：`f8995f234`
（Remove scheduler-kafka library，2025-06-03）比本 commit 早 9 天；且删除前 `CONFIG_KEY_ENSURE_LATEST: True`
的唯一设置点、`CheckpointNotLatest` 的唯一捕获点，都在 `scheduler-kafka/` 里。**无证据表明它因机制本身
被否定**（这是沉默论证，不是正面证据）。同批删掉的还有 `CONFIG_KEY_DEDUPE_TASKS` / `GraphDelegate` 等整套分布式原语；`SCHEDULED = "__scheduled__"` 至今留在 `checkpoint/serde/types.py:13` 并占着 `WRITES_IDX_MAP` 的 -2 槽位，
但 `libs/langgraph/` 里零命中——注意本 commit **同时**从 `libs/langgraph/langgraph/constants.py` 删掉了它，
存活的是另一个包（`langgraph-checkpoint`）里的独立定义，不是"原地没动"。

**这是五方中唯一被官方实现过的库内形态**。但直接照抄对我们只覆盖一半场景——它绑定"显式指定恢复哪个 cp"这个前提，详见下方决策记录。

真正的控制上移到闭源 Platform（**证据分量不齐**：SDK 类型面可在 OSS 直接验；`runs` 表与 `FOR UPDATE SKIP LOCKED` 的具体 SQL 本地无源码，是从 SDK 语义 + 部署行为推断的，标注如下）：`MultitaskStrategy = Literal["reject", "interrupt", "rollback", "enqueue"]`（`libs/sdk-py/langgraph_sdk/schema.py:81`）+ `ThreadStatus = Literal["idle", "busy", "interrupted", "error"]`（同文件 `:34`）+ 服务端内部的 `prevent_insert_if_inflight`（`libs/sdk-py/langgraph_sdk/auth/types.py:580`（`:555` 是 docstring 里的示例，别引错））。库侧的 `RemoteGraph` 客户端 grep `multitask` **零命中**——分层是刻意的。

### eino：未进入设计视野

`CheckPointStore`（`internal/core/interrupt.go:27-30`）只有 `Get` / `Set`，**签名层面无法表达条件写**（无 expected-value 参数、无冲突错误类型）；checkpoint 结构无 status/version/owner。

**checkpoint 相关的四个文件**（`internal/core/interrupt.go` / `compose/checkpoint.go` / `compose/interrupt.go` / `compose/graph_run.go`）里 `sync.` / `atomic.` / `Mutex` 零命中——注意这是**限定到 checkpoint 路径**的结论，eino 核心执行路径该用锁的地方都在用（`compose/state.go:36` `sync.Mutex`、`compose/tool_node.go:994` `WaitGroup`、`internal/core/address.go:91` `RWMutex`），不是"eino 不用并发原语"。

判断它是"未想过"而非"刻意下放"的依据：紧挨着的 `CheckPointDeleter`（同文件 `:32-38`）godoc 明确交待了生命周期责任归属——"The store owner is responsible for managing checkpoint lifecycle in that case (e.g., via TTL, external cleanup...)"。**作者想过的事情就会写清楚**，而对并发全篇沉默。

### MAF：Cosmos 有 etag 却主动丢弃（原以为的正面案例被证伪）

调研前的假设是"MAF 用 Cosmos etag 做乐观并发"——**证伪**。`agent_framework_azure_cosmos/_checkpoint_storage.py:228` 是裸 `upsert_item(body=document)`，无 `match_condition`；`:438-444` 把 Cosmos 系统属性连同 `_etag` 一起 strip 掉（"读出来就扔"）。.NET 侧 `CosmosCheckpointStore.cs:112-125` 连 upsert 都不是，是纯 `CreateItemAsync` + `Guid.NewGuid()`，无 `IfMatchEtag`。

`CheckpointStorage` protocol（`_workflows/_checkpoint.py:119-189`）的 `save` 只吃一个 checkpoint 返 ID，无 expected_version。checkpoint 结构无 status/owner/lease；`version` 字段 docstring 明写是"Checkpoint format version"（schema 版本，恒 `"1.0"`）。

**它是故意允许多实例重放的**——`_checkpoint.py:37-41` 原文说 checkpoint 绑定的是 workflow *definition* 而非 instance，"This allows checkpoints to be shared and restored across different workflow instances"。resume 纯读、不删不标记（`_runner.py:281-341`），唯一并发守卫是 `Workflow` 对象的实例属性 `_active_run` weakref（`_workflow.py:372`），跨进程形同虚设（`:379-381` 自述依赖单 event loop + GIL）。

全库唯一真正保证"同 id 不并发跑"的路径是把 workflow 跑在 Azure Durable Task 上——`agent_framework_durabletask/_workflows/runner_context.py:34` 直言 "Checkpointing is not supported — the orchestrator manages state."：**整个状态 + 并发下放给外部持久执行运行时**。

### OpenAI Agents SDK：RunState 可重复恢复，防重放靠服务端

两条线要分开看：

- **Session**（对话历史）：`memory/session.py:13-54` 的 protocol 四个方法无版本/CAS；`SQLiteSession` 纯 append（`sqlite_session.py:177-200`，唯一的 `INSERT OR IGNORE` 只为去重 session 元数据行）。锁是按 DB **文件**的进程内 `RLock`，护 sqlite 连接不护逻辑。
- **RunState**（HITL 中断后的完整运行状态）：**这才是"副作用双跑"的风险源**。`$schemaVersion` 只管格式兼容（比对静态 `SUPPORTED_SCHEMA_VERSIONS`，不与存储比对）；`from_json` 每次 new 一个全新 RunState、不回写不标记，**同一份 JSON 可反序列化 N 次各自完整恢复**；`approve/reject` 只改内存 context，docstring 明说 "rerun with this state to continue"。grep `consume`/`idempoten`/`already running` 在 `run_state.py` 内零命中（`src/` 全域有命中但全与 RunState 无关）。

唯一的框架级并发处理**只在用 OpenAI 服务端托管会话时生效**：服务端对同一 conversation 加锁返 `conversation_locked`，客户端退避重试 ≤3 次 + best-effort 回退刚写入的历史项（`run_internal/model_retry.py`、`session_persistence.py:416-424`）。它防的是"历史里重复输入"，**不阻止有副作用的步骤在两个 run 里各跑一次**。

（旁证：扩展后端里 Dapr 是唯一带 etag CAS 的（`dapr_session.py:292-329`），但同样只护 append 不丢更新，两个 run 仍各自完整执行。）

### Grok：架构层消灭并发，存储层刻意不做

单 leader 进程（`~/.grok/leader.lock` 上持有至进程结束的 flock）+ 状态归单一 `ChatStateActor` 独占（`xai-chat-state/src/lib.rs` 自标 "State (no locks needed)"，命令串行处理），把所有并发塌缩成单进程内一条 mailbox。存储层因此**刻意不做** CAS/租约/围栏——限定到**会话状态持久化路径**（`session/storage/`）时 `expected_version` / `compare_exchange` 均为 0。
（注意 grep token：Rust 1.50 起 `compare_and_swap` 已废弃改名 `compare_exchange`，拿旧名 grep 会得到"全仓 0"的假象；用新名在 `session/` 下有 22 处命中，但全是内存 atomic flag，不是持久态版本 CAS。）

**反证它懂 CAS**：`expected_version` 确实出现在 prompt queue（`session/acp_session_impl/prompt_queue.rs:390-392`），语义是"版本过期即良性 no-op"。**懂这个模式，只是刻意不用在会话状态上。**

同会话二次载入的语义是**接管而非拒绝**：`drain_old_session_thread` 等旧 actor 退出，**上限 5 秒，超时带警告继续**（`agent_ops.rs:1693-1699` 原文承认 "Session data may be incomplete if the old actor is still writing"）。

**部署形态不可比**：IPC 仅 Unix domain socket / 命名管道、**无 TCP**（`leader/transport.rs:10-12`），状态根在用户 home。它的全部并发安全依赖"全机只有一个进程拥有状态"这个我们作为库给不出的前提。

**但有两点正交手段，即使我们上 CAS 也用得着**：

1. **意图补丁 + 单调字段合并**（`session/storage/summary_write.rs:1-13` 记录了与我们同构的真 bug："a writer holding a stale read overwrites a concurrent writer's field on write-back"）。修法不是 CAS，是提交 `SummaryPatch`（意图/偏序）而非整体快照 + 逐字段单调合并（时间戳取 max、计数器持锁后新读再加）。**能显著减少需要 CAS 重试的场景。**
2. **陈旧锁三层分工**：flock 兜进程猝死（内核自动释放）→ PID 存活性兜残留文件 → 超时 + unlink-to-break 兜"活着但卡死"的持有者（`auth/manager/lock.rs:1-11`，`STALE_LOCK_TIMEOUT_SECS = 60`）。将来若做 lease，第三层必须有。

### 我们相对各家的位置

- **已有落点**：`Checkpoint.Status`（Pending/Unset）+ `SaveNS` 的 `ErrGraphIDConflict` 写时校验——等于已经有"写时拒绝"的先例和一个能放 version/owner 的位置。eino 连表达 Pending/Done 的字段都没有。
- **多 ns 的额外风险**：我们生产路径只用 `SaveNS`，**共三个写点**——出口 defer（`graph_loop.go`）+ `Sync` 档每超步写 + `Async` worker（后两者在 `durability.go`）。**三者统一走 `guardedSaveNS`**（世代比对 + mint + 写 + 更新基线）。嵌套子图每层各自独立写，**无跨 ns 原子性**。`Durability=Sync/Async` 下 cp 每超步落盘，**并发覆盖窗口比 `Exit` 档宽得多**。（`SaveBatch` 按 caller 讲仅 conformance 在用，但 `redis/` `sql/` 两个独立 module 有第一方实现。）理论上并发交错可拼出「ns=A 来自 run1、ns=B 来自 run2」的混合树——两个 run 谁都没经历过的状态。eino 单 key 单 blob 在这条轴上反而更鲁棒。**未实证**（构造特定交错未成功），且它是并发 Resume 的次生症状，根因解决即消失。
- **上层已有 in-process 保护**：`agui`（`handler.go` 的 busy 条目）和 `a2a/server`（`executor.go` 的 `taskState.busy`）都拒同 threadID/taskID 并发。缺口在**业务直接调 `agent_v2` / `compose_v2`** 以及**跨进程**两处。

### 决策记录（2026-07-21 拍板）

**采纳：契约 + opt-in 的世代校验，默认零开销。** 排除完整分布式租约（五家都不在库层面做——那是 workflow engine 的职责边界；这句归类是我们的设计判断，非各家明文声明）。

**实施时修正了一处形态**（值得记，因为它是"照抄会失效"的典型）：LangGraph 那个被删的 `CheckpointNotLatest` 是**载入时**校验，成立前提是它有 **cp_id 链**、Resume 时明确指定"恢复哪个历史点"。

我们有**两条** Resume 路径，对载入时校验的适配性不同：

| 路径 | 语义 | 纯载入时校验有用吗 |
|---|---|---|
| **auto-load**（`WithCheckpointer` + `WithSessionID`）| 框架现场 `LoadNS` 读槽 | ❌ 读的就是最新——两个副本几乎同时载入都读到同一代、都通过 |
| **`WithResumeFrom(cp)`**（业务显式传 cp 对象；`channel_smoke` / `channel_chat_demo` / `subgraph.go:219` 在用）| 业务手里握着一个 cp | ✅ 有用——手里那个可能已被别人推进作废，正是 LangGraph 那个形态 |

所以**照抄"载入时校验"只覆盖一半**（且不是默认那一半）。对单槽 `(sid, ns)` 模型，能覆盖**两条路径**的是**写回时比世代标记**：

```
副本 A/B 载入 pending cp，各记下世代 G0
副本 A 先写回 → 槽里 G0→G1，成功
副本 B 后写回 → 发现槽里已是 G1 ≠ G0 → 返 ErrCheckpointStale（不再静默 err=nil）
```

**它仍然防不住副作用双跑**（A、B 都已执行完），改善的是"最坏的失败模式"——从"两边都报成功、事后无从察觉"变成"拿到明确错误，可补偿/告警"。这一点写在了 option 的 godoc 里，别让业务误以为开了就安全。

**落点**：`SaveNS` 已有 `ErrGraphIDConflict` 的写时校验先例（`existing.GraphID != "" && cp.GraphID != "" && existing.GraphID != cp.GraphID`），世代校验照同一模式加。世代标记用**新字段**——`CreatedAt` 语义是时间戳（`Cleanup` 按它排序）不是世代，`SchemaVer` 是 schema 迁移用。

### 已实现（2026-07-21，cp schema v9）

| API | 作用 |
|---|---|
| `WithStaleWriteGuard()` | RunOption，**只作用于直接调 `compose_v2.Run`** |
| `WithStaleWriteGuardCtx(ctx)` | ctx 开关——`agent_v2` 9 编排下**唯一**的开启方式（编排内部自拼 RunOption 不透传业务的），**嵌套子图靠它继承**，每层各持独立基线 |
| `Checkpoint.Epoch` | 世代标记，随机 token（非递增——跨进程重启会撞号致守卫静默失效） |
| `ErrCheckpointStale` | 哨兵 |
| `guardedSaveNS` / `checkStale` | 统一判-换-写；**写和删共用同一判定** |

**守卫覆盖 5 个判定点，不止"写回"**：三个 cp 写点（Run 出口 + `Durability=Sync` 每超步 + Async worker）各判一次，**外加**成功清理前（`op="purge-success"`）和终局失败清子树前（`op="purge-abort"`）。删比覆盖更不可逆——删掉的可能是另一个执行者的 HITL 存档。

⚠️ **开了 option 后，一个跑成功、无中断、无业务错的 Run 也可能返 `ErrCheckpointStale`**：成功清理时发现槽已被推进，说明本次执行整个建立在过时状态上，它"成功"这件事本身就可疑（很可能重复做了别人已做过的事）。

**`cur == nil` 也算冲突**：基线非空证明载入时槽里有东西，现在没了 ⟹ 有人删过。

**错误消费契约**（业务必看）：三条路径的 wrap 不同，但都保证 `errors.Is(err, ErrCheckpointStale)` 为真。**先判 `IsInterrupt` 再判 stale**——写回被拒时原错（常是 `InterruptError`）用双 `%w` 保住了，直接当冲突处理会把"还在等用户答案"的会话误杀。

**三条静默失效路径**（守卫开着也可能全程不判，都有 log）：
- 基线为空（全新 Run 属正常；**Resume 时则可能是旧 cp 无 Epoch，或业务 backend 漏序列化该字段**——conformance 有专门断言把关）
- 槽里 `Epoch == ""`（对手跑旧代码写的 cp）
- `LoadNS` 报错 → **fail-open**（守卫是护栏不是主链路，因一次读抖动把 HITL 存档拦掉比放行更糟）

**默认行为不变**：不开 option 时**不比对**（世代照写、只是不读）——`Checkpoint.Epoch` 无条件写入，业务 backend 照常序列化即可、不必理解语义。

**为什么总是写而不是 gate 在 option 里**：不写的话，业务中途开启守卫时第一轮载入的 cp 没有 Epoch → 基线为空 → **守卫静默失效一轮**，而"静默"正是它要消灭的东西。代价是 wire 上多一个 16 字符字段（cp 本就有 channels/frontier/interrupts，可忽略）。所以严格讲不是"wire 一字不变"，是"**行为**不变"。

## 我们的优势（保留不动）

1. **显式 Status 字段 + 成功即清的生命周期设计**：cp 仅作"异常状态快照"存在（`compose_v2.Run` 出口 defer：失败 `SaveNS`、成功 `DeleteSubtree`），不像 LangGraph 每步都写、需要事后 GC，也不像 eino 完全不管清理。这是介于两者之间的明确语义。
2. **(sessionID, ns) 二级 key + ParentNS 字段**：O(1) 父子定位，避免字符串解析（LangGraph 的 `parent|child:taskid` 需要 split）。
3. **`ListPending` / `LoadAllInSession` / `DeleteSubtree` 业务直接可用**：HITL Resume 不需要从 metadata 反查，直接寻址。

## 我们的不足（可借鉴 LangGraph）

1. **无 time-travel 历史 cp 链**
   - LangGraph 每 super-step 一个 cp_id 串成 parent 链（`base/__init__.py:108`），可恢复任意中间 step
   - 我们仅留最新一份，丢了"回放调试"能力
   - **借鉴**：可加 `parent_id` 字段；当前不做（项目初期 time-travel 不是核心需求）
2. ~~**无 durability 模式区分**~~ —— ✅ **已做**（移植 LangGraph durability 三档，`compose_v2/durability.go`）
   - `WithDurability(exit/sync/async)`：`exit`(默认 = 历史"仅出口 defer 写")/`sync`(每超步边界同步阻塞写,扛硬崩)/`async`(单 worker 后台 goroutine 写,FIFO 有序 cp_N 先于 cp_N+1 + join-before-cleanup + per-task panic recover,重叠 compute 与 I/O)；经 ctx 透传让嵌套子图继承父档
   - **原决策担心的"sync 破坏成功即清"没发生**：durability 写同 `(sid,ns)` key 中途 scaffolding，成功 defer 的 `DeleteSubtree`（无 durability 条件、对三档都跑）照清——"成功即清"对三档都成立。崩溃恢复价值需配持久 Checkpointer
   - **写粒度**：per-超步边界——对**单节点超步（绝大多数场景）= per-task，完整崩溃恢复**；只有 fan-out 超步执行中途硬崩时与 LangGraph `put_writes` per-task 有重算差异（见 #4，**可选 perf 优化、非崩溃恢复缺口**）
3. ~~嵌套 cp 当前是嵌入式（与 eino 同款）~~ —— ISSUE-70 Phase 2 Step Ⅰ 已切到物理分离（同 store + ns 寻址），与 LangGraph 一致
4. **fan-out 超步执行中途硬崩重跑已完成分支（per-task 重算优化——可选，非崩溃恢复缺口）** (audit iter-2 H2 + R33 PA-H1 + durability)

   > ⚠️ **这不是"durability 没做"**：`WithDurability(sync/async)` 的 per-超步崩溃恢复**已完整**（每超步边界落盘 → 硬崩从最近超步 Resume）。本条说的是 LangGraph 在 durability **之外另有**的 `put_writes` per-task 机制，只在一个**窄场景省算力**——见下。

   - **单节点超步（ReAct 主循环 / Sequential / Loop 等绝大多数场景）：per-超步 = per-task，零差别**，崩溃恢复与 LangGraph 完全等价
   - 唯一差别在 **fan-out 超步**（ParallelAgent / 多 tool_call 的 tools 节点 / Plan 并行步）：
     - LangGraph `aput_writes(config, writes, task_id)`：fan-out 内单 task 跑完即落盘 → 硬崩只重跑**未完成** task（**per-task**）
     - crew-go：durability 写在**超步边界**（整超步完才写）→ fan-out 超步**执行中途**硬崩从上一边界 Resume、重跑**全分支**（含已完成的）。**只是浪费重算、不是状态错**——从干净的上一超步态重跑、channel reducer apply 一次、状态正确（业务工具幂等管副作用，既有契约非 durability 引入）
   - 而且 fan-out 的 **interrupt / 业务错**路径**已覆盖**（`cp.CompletedFrontier` / `DeferredSends`，R33 PA-H1——成功分支不重跑，见 [hitl-resume.md](../../v2/hitl-resume.md) §7.3）；**只有 kill -9 卡 fan-out 超步执行中途**才落到 per-超步粒度
   - `PendingInterrupts` 与 put_writes 正交（HITL 中断载体 vs 节点中间结果增量持久化）
   - **可选优化（非必须）**：长跑 fan-out（几十分支）+ 频繁 mid-超步硬崩诉求强的业务可加 `SaveTaskWrites(sid, ns, taskID, writes)` 分支完成即落盘省重算；当前未加——绝大多数场景 per-超步已是完整崩溃恢复

## 我们已偏离三方但更优（保留不动）

1. **`RunSession` + 事件流分层**：LangGraph 和 eino 都没有 session 概念，所有审计混在一起。我们的 RunSession 让事件流 / 审计 / cp 持久化可独立演进。
2. **嵌套 interrupt 归属清晰**：子图 cp 物理分离（独立 ns）+ 5 维 `InterruptID`（含 graphNS 维度）让嵌套子图的 pending interrupt 各自存在自己 ns 的 `cp.PendingInterrupts`，天然不冒泡到父、不串台。eino 全树序列化把子图 interrupt 嵌在父结构、`forwardCheckPoint` 一次性消费易脆——我们物理分离 + 稳定 InterruptID 寻址更稳。

## 决策记录

> ⚠️ 决策记录是**历史档案**——记录"当时为何这么取舍"。原 Phase 2 Step Ⅰ/Ⅳ/Round 1 那几条的 v1 实现细节（`saveCheckpointOnError` / `cpKeySubgraphCheckpoints` / `cp.Events` 回放 / `addNSAndAncestors` 等具体函数）已随 channel-based 重设计移除——决策方向（物理分离 + 嵌套/fan-out cp 隔离）见下表，v2 实现见 [`checkpoint.md`](../../v2/checkpoint.md) + channel-engine §7-§8。

| 时间 | 决策 | 借鉴 | 不全抄理由 |
|---|---|---|---|
| ISSUE-70 Phase 1 | 加 `Status` / `NS` / `ParentNS` 字段 | LangGraph 的 ns 派生 | 不抄 cp_id 全历史链——time-travel 非核心需求 |
| ISSUE-70 Phase 1 | `LoadAllInSession` / `ListPending` API | LangGraph `list` | 不抄 time-travel filter——过度工程 |
| ISSUE-70 Phase 2 | 嵌套 / fan-out cp **物理分离**：子图 cp 写父 store 独立 ns `(parentSID, childNS)`、fan-out 各分支独立 ns，不嵌入父 cp，多层 / 多分支互不覆盖 | LangGraph 同 store + ns 寻址 | 不抄 ns 完全刚性——保留 sandbox 走独立 store 路径 |
| ISSUE-70 决策 | 不加 `durability` 模式（**后反转**，见末行 Durability port）| LangGraph durability 三档 | 当年判断"成功即清"是核心语义、sync 模式会破坏——**此担心后证伪** |
| ISSUE-70 决策 | 不加 `WithIsolatedCheckpointStore` | （三方都没有） | 同 thread 两个 store 原子性问题真实，不可取 |
| ISSUE-50 P0 | GraphID 防串台：`Graph.ID()` 身份（name 或拓扑+state 类型 fingerprint）+ `cp.GraphID` 字段 + `SaveNS` 冲突守卫 + Resume 入口 fail-fast 校验 | LangGraph / eino 均无 graph 身份概念 | 两家"同 thread 跨 graph"是业务自管未防御；我们 fail-fast 守护，避免不同 Graph 共享 sid 时 cp 静默互覆 + state 串台 |
| ISSUE-50 P1 | thread-aware：`cp.ThreadID` 字段 + `ListByThread` / `LatestByThread` + `ErrThreadNotFound` | LangGraph thread_id 一级 | **已被 ISSUE-98 推翻并删除**——受制于「成功即清」这套 API 只能返当前挂起的 run，名不副实。会话历史改由 `agent_v2` 独立 append-only 层（`WithHistory`）承担，cp 层回归纯「单次 Run 临时态」。见本文 §跨 run 会话 + [ISSUE-98](../../issue/issue-98-conversation-history.md) |
| ~~本次~~ 已推翻 | ~~删 `CPStatusFailed` 死枚举——Status 只留 `Pending` / `Unset`~~ | LangGraph（pending_writes 对账、无 Status）/ eino（业务错不写 cp）都不用"失败态 cp" | 当时的理由是"cp 存在即有挂起中断待续、Failed 态无产品场景"——**那是照抄别人的结论而丢掉了自己的前提**：他们不需要 Failed 态是因为业务错根本不写 cp，而我们所有 `runErr != nil` 都写 |
| ISSUE-94（`2b5c9bc`）| 加回 `CPStatusFailed` + `Status.Resumable()`，判据 `IsControlFlowSignal(runErr)` | 三家都没有对应物（他们靠"失败不写 cp"绕开） | 失败现场和"等人回答的挂起"在存档上长得一模一样时，auto-load 分不出来——一次因下游永久 500 退出的 Run 会被下一次同 sid 调用当成挂起捡起来接着跑。**"能不能恢复"和"要不要自动恢复"是两件事**：两态 `Resumable()` 都为真，auto-load 只捡 `Pending`。⚠️ **2026-08-13 撤**：这个立论建在「失败现场和挂起在存档上长得一样」上，而那是**设计选择**不是必然——把「谁失败了」写成内容（frontier 指着失败分支），档自己就带着区分。撤后闸门按内容判、`Status` 退为报告位 |
| iter-5-H Option D | `UnmarshalCheckpoint` **不在版本不匹配时 fail-fast** + `MarshalCheckpoint` 不 mutate 入参 auto-fill `SchemaVer` | LangGraph `BaseCheckpointSaver.serde` 路径不强制 `v == LATEST_VERSION` 反序列化 fail | LangGraph 实证: fail-fast 让业务跨版本兼容 / ad-hoc `&Checkpoint{}` 漏填 `SchemaVer` 必爆, 价值低于业务负担; 跟 eino `MigrateCheckpointState` (业务自行迁移) 方向一致. crew-go `SchemaVer` 字段保留 informational 供业务跨版本兼容时按字段分支处理 |
| Durability port | **加** `WithDurability(exit/sync/async)`——反转上面 ISSUE-70「不加」决策 | LangGraph durability 三档 | 当年"sync 破坏成功即清"证伪：durability 写同 `(sid,ns)` key 中途 scaffolding、成功 defer `DeleteSubtree`（无 durability 条件）照清，三档都"成功即清"。`exit` 默认保历史零开销；`sync`/`async` 闭合硬崩"跨超步全丢"gap。`async` 单 worker FIFO + join-before-cleanup + per-task panic recover（对齐框架既有 goroutine recover 模式）。per-超步崩溃恢复**已完整**（单节点超步 = per-task）；**未做** LangGraph `put_writes` 的 per-task 粒度——那只是 fan-out 超步 mid-execution 硬崩省重算的可选优化、非崩溃恢复缺口（见不足 #4）|
| ISSUE-95#1（`90a94d2`+`7d6b913`） | `SerializedSend` **加 `Owner`**：deferred send 的归属进持久层，重跑某节点时据此清它的旧 send 防 Target 双发 | **仅 LangGraph**（同构问题、同类解法：持久化单元即 `(task_id, channel, value)`、saver 主键含 task_id、`put_writes(…, task_id, task_path)`）。eino **无 owner / task_id 概念、亦无 Send 等价物**——它是结构上不产生该问题，**不构成佐证** | 不抄 task_id 那套身份体系（要给每个 task 生成 ID + 主键改造）——我们只需回答"这条 deferred 是谁发的"，一个 owner 字段够。**代价是粒度**：task_id 是真·per-task 身份不吃这亏，我们的 owner 只到 NodeID、执行单位却是 branch 实例，故 purge 靠"本超步执行过 ⟹ 记录属兄弟实例"绕开而非引入实例级身份（见 issue-95 陷阱段） |
| 引擎深审 F-B（`999113a`） | `Checkpoint` **加 `PendingSendWrites`**：没能提交的 SendFrom payload 单独存档，Resume 在跑任何节点前补 apply | 两家都没这个裂缝可抄：LangGraph 的 Send 自带 payload（`packet.arg`）物理不可分裂、frontier 每步现算不落库；eino frontier+payload 同 blob 原子 Set | 两家的"不分裂"都靠**结构**保证，我们不搬结构：**持久化单元本来就是 cp 本身**（`CurrentFrontier` 与新字段同一次 `SaveNS` 原子写出），补一个字段即可。**不复用 `DeferredSends`**——那个走"并入下一超步 frontier"的调度重放，而这里 target 已在 frontier 里，塞回去会被调度两次 |
| ISSUE-96 步骤1（`54867f9`）**mint 点选在"产出的那一刻"** | `SendDirective.instanceID` 在 `drainSendSink`（与 owner stamp 同点）mint，**不在「target 进 frontier」那行** | **两家同一原则**：LangGraph 的 `task_id` 编的全是**被冻住的那个 cp** 的输入（`checkpoint_id`/`step`/`idx`，`_algo.py:990-997`）→ 重放算出来还是同一个；eino 恢复时 `input = tnState.Input`（`compose/tool_node.go:1062`）**丢弃传入的 input、换成中断时存的那份** → callID 原样不变 | 我们的判据是第一性的、不依赖两家：身份要回答"这是不是上次那个待办"——**重放**代表的还是同一次投递（答"是"），**重跑**是新一次产出（答"否"）。mint 在"产出那一刻"让两个答案自动成立。若在「target 进 frontier」mint，deferred send 从 cp 重放时会再次流经那行 → 重放换新身份 → Resume 误触发"代际作废"→ 用户刚答的答案又失效 |
| ISSUE-96 步骤2（`a7f6f00`）**wire 用并行数组**（⚠️ **已被 2026-07-29 `963997d` 取代**，本行为历史；现形态是身份内联的 `[]FrontierMember`，见文末「cp 字段线性增长」节） | `FrontierInstanceIDs []string` 与 `CurrentFrontier` 按下标一一对应，**不把后者改成结构体数组** | eino 的 cp 里 `Inputs map[nodeKey]input` 是主单元、`RerunNodes []string` 是并行补充列表，恢复时归一（`graph_run.go:777-826`）——并行字段是成熟做法 | 不是抄形态，是我们自己的**兼容性**要求：改结构体数组会让业务自研 backend 的 wire 解析当场全断，而绝大多数图没有多实例（静态边走 `nextSet` map 恒去重）。并行数组让"没有实例身份"退化成**字段缺失** = 引入前语义；全空时 omitempty 省略 → 静态边图的 cp 形态**完全不变**。**长度不匹配按"全静态"降级不 panic**——宁可退化成"无身份"，也不要按错位下标让实例互相冒充 |

## 开放问题

1. **是否加 `parent_id` 字段做有限历史链**？
   - 候选 A：完全不加（当前）
   - 候选 B：加但默认仅保留 1 层（最近一次中断态可回退）
   - 候选 C：完全抄 LangGraph 全链（重）
   - 暂定 A，等出现 time-travel 调试需求再升 B
2. ~~**持久化后端（PG / Redis）支持**何时做？~~ ✅ **已做**：`redis/`（Redis Checkpointer + Store，独立 module，含 conformance + smoke）+ `sql/`（Postgres + MySQL 双方言 Checkpointer，独立 module，dialect-specific UPSERT + sqlmock/mysql 测试）。InMemoryCheckpointer 仅默认 / 测试用。
   - ~~**遗留 gap**：框架仅在 error 出口 defer 调 `SaveNS`（非每超步）——硬崩溃 mid-run 仍丢~~ ✅ **已闭合**：`WithDurability(sync/async)`（`compose_v2/durability.go`）每超步边界落盘——配持久后端 + sync/async 即可硬崩从最近超步**完整恢复**（单节点超步 = per-task）。fan-out 超步 mid-execution 硬崩的 per-task 重算是**可选优化、非缺口**（见 §不足 #4）
3. **`SaveBatch` 真原子性**怎么保证？（2026-09-18 核验：生产代码零调用 `SaveBatch`；三个实现都是逐条 best-effort，下面「InMemory 持锁全程」不成立；接口 godoc 说「原子」与实现自相矛盾——删掉还是定语义已进中心 backlog）
   - 当前 InMemory 持锁全程算近原子
   - PG 后端要靠事务，Redis 要 Lua 脚本
   - 接口签名预留了，实现细节后端各自处理

---

~~**同会话并发 Resume 的库内形态怎么选**~~（2026-07-20 提出）→ **已决**，见上文「同一会话并发执行 §决策记录」：契约 + opt-in 写回世代校验，默认零开销。

两条附带处置：

- **同进程互斥**（业务直调 `agent_v2`/`compose_v2` 的缺口）→ **非目标**。写回世代校验对同进程并发同样生效（两个 goroutine 各自 `Run` 同 sid，写回时一样撞世代），额外加 per-sid 互斥只是"更早 fail-fast"，收益不抵复杂度——且嵌套子图是同一 Run 树内的多次 `Run`，per-sid 互斥会自锁，需能识别同一 Run 树才安全。
- **`CreatedAt` 框架生产代码零处赋值**（`redis` 拿它当 thread score、`Cleanup` 拿它当 GC 排序键，而框架从不写）→ **已修**：盖章点就落在 mint `Epoch` 的同一个函数（`guardedSaveNS`），只在 caller 没给时盖。修之前 `Cleanup(keepLastN)` 的排序键恒等、落到 sid 字典序，而 sid 是 UUIDv4——实测保留的是**最先创建**的那批，GC 方向整个反过来。


## 致命错（非中断）之后 checkpoint 怎么办（2026-07-27 五家实测）

### 统一的那条：没有任何一家持久化「失败之后的状态」

三家有 cp 的（LangGraph / eino / MAF）落盘的**永远是失败超步之前的边界**，从不是半执行状态：

- **MAF**：cp 写在超步末尾（`InProc/InProcessRunner.cs:332`），executor 抛异常在 `:318` 就逃逸 →
  cp 那行根本不执行。而且 state 提交（`PublishUpdatesAsync`）**唯一两个调用点都在 CheckpointAsync
  内部**——跳过 cp 也就跳过 state 落盘，失败超步在持久层「完全不存在」
- **eino**：写入是 **interrupt-gated 而非 error-gated**——`checkPointer.set` 全仓只有两个调用点，
  都在 interrupt handler 里（`compose/graph_run.go:561` / `:701`）；普通节点错走 `:477` 直接
  `return wrapGraphNodeError`，不写任何 cp
- **LangGraph**：写前置边界 + 把错误**旁挂**成 pending write（`_runner.py:596-603` 往
  `ERROR = "__error__"` channel 写），channel 状态仍是失败超步之前的

**这条我们符合**：ISSUE-94 里那份 cp 也是失败前边界（`CurrentFrontier=[失败节点]`、channel 是
超步前），本身不是脏数据。**所以问题不在「存了坏东西」。**

### 分歧的那条：残骸留不留、留了怎么被认出来

| | 失败时写 cp | cp 有失败标记 | 恢复会捡起失败态 | 清理 | 立场 |
|---|---|---|---|---|---|
| **LangGraph** | 写 | 库层无 status，靠 pending-write 的 channel 名区分（`__error__` vs `__interrupt__`）；平台层才有 `ThreadStatus="error"` | **会，且是设计目标**（测试注释 "Should retry failed node"） | 手工 `delete_thread`，无 TTL/GC | **保留是特性**：失败=可重试断点，错误在 `state.tasks[0].error` 可见 |
| **eino** | **不写** | 不需要——「cp 存在 ⇒ 有中断挂起」是硬不变量 | 不可能 | compose 层推给 store owner（`CheckPointDeleter` godoc 明写不自动清）；**TurnLoop 层非 stop/interrupt 退出即删已载入的 cp**（`adk/turn_loop.go:2028`） | 不留 |
| **MAF** | **不写** | 无 status 字段 | 不会，且**不自动加载**（caller 显式指名 CheckpointInfo，time-travel 模型） | 接口层没有 delete | 不留也不需要清 |

### 我们的位置：三家各自靠一条免疫的，我们三条全占

- LangGraph 敢保留 ← **append-only 历史 + 非自动 resume**（带新 input 再跑会 discard 未完成任务
  并 append 新 head，残骸自然被推过去）+ 错误可见
- MAF 敢不清 ← **不自动加载**
- eino 敢自动加载 ← **写入 gate 在 interrupt**，「cp 存在 ⇒ 等输入」是真不变量

我们是**单槽覆盖 + auto-load + error-gated 写入**三者叠加。ISSUE-94 文档里说的
「GA-M2 设计假设的精神（cp 存在 ≈ 真正等答案的 Pending）」——eino 把它做成了硬不变量，
我们留成了软约定。**这才是结构根因，不是「存不存」本身。**

### backlog 的理由被削弱了

ISSUE-94 当初 backlog 的核心理由是「保留 cp 是为了瞬时错（网络抖动）的 crash-recovery 重试」。
但实测显示**瞬时错这一档在三家都由节点级重试策略覆盖，不由悬空 cp 覆盖**——LangGraph 自己也有
`RetryPolicy`（默认 `max_attempts=3` + 指数退避）。

**而我们同样有**：`compose_v2.RunWithRetry` + `SimpleRetry` / `ExponentialBackoff`，**7 个编排
全接了**（sequential / parallel / loop / routing / reflect / handoff / plan）。

**但只削弱一半，不能推翻**（2026-07-27 更正——先前这里写过「兜不住的恰恰是确定性错」，
那句是错的）：

- **削弱掉的**：大部分瞬时错在节点级重试里就消化了，轮不到 cp 这一层兜。所以「不存 cp
  就丢了瞬时错恢复」这个前提**大幅缩水**。
- **依然成立的**：**重试耗尽 ≠ 确定性错**。网络断 30 秒、重试 3 次全落在这 30 秒里，
  耗尽了，但它仍是瞬时错——过一会儿 Resume 能成，这种情况留着 cp 确实有价值。真正剩下的
  是「重试窗口盖不住的长故障」这一档，触发面比原记录小，但不是零。

**「框架难区分确定性错 vs 瞬时错」这条技术难点没有解决**，实测框架现有分类能力只有两档：

```go
func IsRetryable(err error) bool {
	if IsControlFlowSignal(err) { return false }
	return !errors.Is(err, ErrRunAborted)   // 其余一律当可重试
}
```

只认「控制流信号」和「终局失败」，剩下全归一类。model 层的 `fallback` 稍细（按 HTTP
408/429 之类分类），但只覆盖 model 调用，够不着 tool 错和业务错。

### 若要动，三条候选各有先例

- **A（写入 gate 加条件）**= eino compose 的形状
- **B（出口按错误类型删）**= eino TurnLoop 的形状，且它连「成功」也一并删（`shouldSaveCheckpoint`
  为 false 就删）——跟我们 `cleanupOnSuccess` 同源
- **C（取消自动加载）**= MAF 的形状，代价是改 Resume 语义，比 A/B 都大

我们已有的 `ErrRunAborted` → `DeleteSubtree` 就是 B 的雏形（走它的是「重跑必然同样失败」那一类：cond 的 Abort 档 + 业务抛的中断请求形状非法）。

**推荐方向：让业务声明哪些错算终局**（类比 `CondFailPolicy`）。注意这是**绕开**上面那个
技术难点而不是解决它——框架信息不足（它看到的只是一个 error，判不了下次会不会好），而
业务知道自己的上游是什么。`CondFailPolicy` 当初也是同一个判断：cond 报错该重试还是放弃，
取决于上游是否确定性，只有业务知道，所以框架不猜、让业务声明。

代价是多一个 option 和一份心智负担；收益是不用去解一个框架信息不足的问题。

---

## cp 字段线性增长而约束组合增长：结构上的原因（2026-07-29）

**立项时的假说**：写日志式的 cp（每个 task 的 writes 单独记，恢复 = 重放）是否根本不产生这类问题。

**假说方向对，但理由不是「写日志」**。LangGraph 两样都有——一份已提交状态的快照 + 一份在途超步的写日志。真正的差别在**执行态怎么表达**。

### 字段数对照

`Checkpoint` 本体（`libs/checkpoint/langgraph/checkpoint/base/__init__.py:92`）**7 个字段**，全是状态快照本身：

```
v / id / ts / channel_values / channel_versions / versions_seen / updated_channels
```

**执行态一个都不在里面**——它在 `CheckpointTuple.pending_writes: list[PendingWrite]`（同文件 `:146`），跟 cp **并列存放**，而 `PendingWrite = (task_id, channel, value)` 三元组。

我们的 `Checkpoint`（`compose_v2/checkpointer.go:97`）**17 个字段**，支持的执行特性并不比它多。

### 关键：执行语义靠保留 channel 名表达，不靠新增字段

`WRITES_IDX_MAP = {ERROR: -1, SCHEDULED: -2, INTERRUPT: -3, RESUME: -4}`（同文件 `:795`）——**报错 / 调度 / 中断 / 恢复全部复用同一条记录形状**，只是 channel 名是保留字。加一个执行特性 = 加一个保留 channel 名，**不是加一个顶层字段**。

这就是它字段数不随执行特性增长的原因。

### 真正的差别：自描述记录 vs 平行数组

| | 我们 | 它 |
|---|---|---|
| 执行态怎么表达 | **新增顶层字段**（每个执行特性 +1~3 个）| 复用同一记录形状 + 保留 channel 名 |
| 身份怎么带 | **平行数组按下标对齐** | **内联在记录里**（`task_id` 是三元组第一项）|
| 漏序列化的后果 | **静默退化**（双发 / 读零值 / 实例互相冒充）| 少一条记录 = 那个 task 没跑过 = 重跑，**语义仍自洽** |

最后一行是关键：它的失败模式**退化到安全侧**，我们的退化到静默错误侧。

**根因**：平行数组是**外部约束**——「两个数组第 i 项必须指同一个东西」这件事没有任何类型或编译器管得着，只能靠 godoc 写「漏了不报错，只是静默退化」，靠人记。自描述记录是**内部约束**——身份在记录里，没有对齐可错。

这解释了「cp 的 bug 一个接一个改不完」：不是不小心，是**约束放在了类型系统够不着的位置**。

### 我们已经有一半是对的

同一个 struct 里两种形态并存：

- ✅ **自描述**：`SerializedSend{Target, Payload, Owner, OwnerInstanceID, InstanceID}`（`compose_v2/checkpointer.go:197`）——身份内联，漏不了、错不了对齐
- ~~❌ **平行数组**：`CurrentFrontier []NodeID` ↔ `FrontierInstanceIDs []string`、`CompletedFrontier []NodeID` ↔ `CompletedInstanceIDs []string`——ISSUE-96 加实例身份时选了后一种~~ **已改（2026-07-29，`963997d`，SchemaVer 9→10）**：两组 frontier 都换成 `[]FrontierMember`，两条平行数组删除，`restoredMembers` / `restoredFrontierMembers` / `memberInstanceIDs` 三个 helper 一并删（`restoredMembers` 的整个存在理由就是「两个数组长度可能对不上」）

**由此修法方向变小很多**：不需要「把 cp 改写成写日志」（那是引擎级重构，且跟我们「cp 是单次 Run 临时态、成功即清」的哲学另有冲突，见上文决策记录）。只要**把 frontier 也改成自描述记录**——`[]FrontierMember{NodeID, InstanceID}` 取代两条平行 slice——就能消掉「两组下标一一对应」这整类约束，而这类正是 godoc 里明写「漏了不报错」的大头。

短期止血（Restore 期加机械校验、把静默退化变 fail-fast）仍然有效，但那是在**约束存在的前提下**验证它；改成自描述记录是**让约束不存在**。后者更彻底且改动面可控（序列化 + 读取点，不动调度算法）。

✅ **已实施**（2026-07-29，`963997d`）。实际改动比立项时预估的还小：**内存态本来就已经是 `FrontierMember`**，平行数组只活在 wire 那一层，故只动了序列化边界（4 个生产/消费点 + 发号器扫描点 + 1 个 Resume 重建点）+ conformance 断言。

落地时确认的一件事：ISSUE-96 当初选平行数组给的理由是「缺字段天然退化成静态边语义」，**这半内联后仍然成立**——缺 `instance_id` 键 → 空串 → 静态边语义，与缺整条平行数组等价。所以这次只丢了「按下标对齐」那条约束，没丢那份宽容，这是它敢改的前提。

**⚠️ 内联换来了一条新约束，别只记前半句**（审计实测）：内联之前 `cp.CurrentFrontier` 是
`[]NodeID`、`cm.frontierAtExit` 是 `[]FrontierMember`，两个方向都得过元素类型转换 ⟹
**cp 与引擎共享底层数组在类型层就不可表达**，不变量由编译器守。内联之后两侧同类型，
`cm.frontierAtExit = cp.CurrentFrontier` 能干净编译且静默错——降级成 5 处手写
`make`+`copy`。所以准确的说法是「用一条编译器保证换掉了一条对齐约束」，不是净减少。

危害隐蔽在于 `updateFrontier` 是 `append(frontierAtExit[:0], ...)` **就地复用底层数组**，
而 `make([]T, n)` 给出 cap==len ⟹ 别名只在 frontier **收缩或等长**时才写回原数组、
**变长会 realloc 自动躲开**。库里既有 Resume 用例恰好都是等长或变长，所以注入别名后
整套 `-race` 仍全绿——已补收缩型哨兵 `TestRestore_FrontierCopyProtectsCallerCheckpoint`
（两个动态实例 → 静态边汇同一后继 → 出边去重 2 收缩到 1），注入别名时实测污染成
`[{A i1} {B i2}]` → `[{C } {B i2}]`：一个从未存在过的 frontier。

**另一条落地时才浮出来的边界**（sanity-revert 实测）：把 `node_id` 键名改错，**只有手写的键名 pin 会 FAIL**，round-trip 类测试（conformance JSON wire / `sql/payload_roundtrip`）全绿——marshal 与 unmarshal 走同一个 Go 结构体，键名一起错就自洽。**wire 键名的稳定性只能靠手写 pin 守**，round-trip 测试对它结构性失明。已写进 `checkpointer_conformance/wire_test.go` 的说明。

## 「有挂起存档时又来了新任务」——五家的处置（2026-07-31）

起因是实测的一个静默 bug：任务 A 中断留下 Pending cp，同 sid 来了**全新任务 B**（`Run` 不是
`Resume`）⟹ auto-load 把 A 的 cp 捡起来当续跑 ⟹ **B 的输入压根没进图**、B 从 A 中断的位置
继续跑、还读到 A 的工具产出，全程 err=nil。

判定「这份存档不是你的」之后怎么处置，两个候选：**(甲) 拒绝并保留**（返 sentinel 要调用方
表态，但 A 的存档一直挂着）/ **(乙) 拒绝并接管**（当 fresh 覆盖，但静默丢 A 的挂起态）。

### 结论：(乙) 排除——它靠版本链兜底，我们不满足

LangGraph 是唯一的 (乙) 实践者。它带新输入进来时 `discard any unfinished tasks from
previous checkpoint`（`pregel/_loop.py:976-993`），但 **discard 只影响「下一步调度谁」**：
`discard_tasks` 的 `writes` 是空 deque，进 `apply_writes` 唯一作用是把触发它们的 channel
consume 掉（`_algo.py:284-292`）；**被 discard 的状态和那个中断问题在存储里一行没动**。

它敢这么做的三个前提：

| 前提 | 证据 | 我们 |
|---|---|---|
| 旧 cp 行 append-only、可寻址、可 time-travel | `checkpoint/memory/__init__.py:456-464` 按 cp id 追加 + parent 指针；`tests/test_time_travel.py:391-395` 断言 fork 后两条分支同时存在（"Original branch (preserved)"）| ❌ 单槽覆盖 |
| 中断 payload 跟旧 cp id 一起活着 | `memory/__init__.py:494-497` outer_key 含 checkpoint_id；`test_time_travel.py:497-546` run 早已完成仍能从「当初中断那份」fork 出来、**中断重新触发** | ❌ 覆盖即消失 |
| thread = 累积会话，不是一次性任务槽 | `tests/test_pregel.py:1473-1484`：报错后「we can recover from error by sending new inputs」，A 累积的 channel state 全部保留 | ❌ 我们的 cp 是单次 Run 临时态、成功即清 |

**现成的反面样本**：eino ADK 就是「单槽 + 覆盖」——`Run` 用一个已有挂起 cp 的 id 跑新任务
时不 load（新输入不丢，这点比我们好），但下次中断时往**同一个 id** 覆写
（`adk/runner.go:330-331`），**旧挂起态静默消失、不可恢复**。这就是 (乙) 跑在单槽上的真实
结局。

### (甲) 的前提我们满足，但它在业内只有一个半实践者

MAF 那个 `RuntimeError("Cannot start a new run with 'message' while in-flight executor
messages remain...")`（`_workflows/_workflow.py:828-836`）**判据不是 checkpoint**——
`has_messages()` 查的是**进程内未 drain 的超步消息队列**（`_runner_context.py:312-313`），
重建 Workflow 实例即清。而且它**明确不拦 HITL 挂起态**（`_workflow.py:818-821` 的注释：
带新 message 来回答挂起请求是**正常路径**）。所以它的「甲」是并发保护，不是归属校验。

我们的 `ListPending` 已满足 (甲) 的可见性前提（业务能提前知道某 sid 上挂着东西），比 eino
强（它只有 `Get` 返 bool、cp struct 未导出，业务看不到问了什么），与 LangGraph 的
`get_state_history` / MAF 的 `list_checkpoints` 同档。⚠️ 但那两家能返回**挂起的具体问题**
（interrupt payload / request event），我们若只返 cp 元信息，业务撞上 sentinel 后还要多一跳。

### 第三条路：归属做进 key，让这道题消失

MAF 的 hosting 层实测在用：cp 路径是 `<root>/<user_id>/<context_id>`
（`foundry_hosting/_responses.py:253-287`），**新任务给新 context_id，物理上就读不到旧存档**，
顺带解掉多租户越权（注释原话是一个租户永远无法恢复或观察另一个租户的状态，即使伪造 id）。

⚠️ **它的真门槛**：taskID 从哪来。业务不给就得框架自己生成，那又回到「怎么知道 B 是新任务」
的原点——跟 (甲) 要回答的是同一个问题。

### 挂起存档的 GC：没有一家给 checkpoint 做 TTL

全是「显式 delete + 业务负责调」。eino 白纸黑字承认会堆积
（`internal/core/interrupt.go:32-41`：不实现 `CheckPointDeleter` 时 stale checkpoints
**will NOT** be automatically cleaned up），且有测试钉死这个行为
（`adk/turn_loop_test.go:4520-4524`）。

两个可抄的：
- **LangGraph 的 `delete_for_runs(run_ids)` + `CheckpointMetadata.run_id` 成对设计**
  （`checkpoint/base/__init__.py:331-338` + `:61-62`）——**归属校验和归属清理是同一个字段的
  两个用途**，一起加边际成本几乎为零。
- MAF hosting 的 sweeper（`_responses.py:805-813`，每次 run 后只留最新一份）——我们用
  `ListPending` + `DeleteSubtree` 拼得出，适合当兜底。

### 一个更本质的观察：A 缺的是终态，不是 GC

「A 的存档永远不会成功因而永远不清」本质上不是垃圾回收问题。MAF 那句错误文本
`"...or wait for the prior run to complete"` 隐含的假设是 A 一定会走到某个结局。

我们的 `Status` 已有 `Unset` / `Pending` / `Failed` + `Resumable()` 三档——让「被判定为异
任务而拒绝」这件事**给 A 打一个终态标记**，既让 `ListPending` 不再把它当活的挂起、又保留
现场给业务捞，比单纯 TTL 干净。

### 决策记录

| 决策 | 结论 | 依据 |
|---|---|---|
| (乙) 拒绝并接管 | **排除** | 靠版本链兜底的三条前提我们全不满足；单槽下同一动作 = 静默丢弃且不可恢复，eino ADK 是现成反面样本 |
| (甲) 拒绝并保留 | **可行**，可见性前提已满足 | `ListPending` 与两家同档 |
| 归属做进 cp key | **更干净但门槛在「taskID 从哪来」** | 与 (甲) 要回答的是同一个问题，不是替代 |
| 挂起存档 GC | 归属字段兼做清理索引 + `Status` 打终态 | 五家零 TTL；成对设计边际成本近零 |

---

## per-成员写日志（WAL）：不做（2026-09-01，三轮终审）

**提案**：把每个成员完成时的原始 writes 按 `(NodeID, InstanceID)` 落盘，Resume 时据此
跳过已成功成员——对照 LangGraph 的 `pending_writes`（`_runner.py:574` 每 task 完成即
`put_writes`；`_loop.py:736` `_reapply_writes_to_succeeded_nodes` + `_runner.py:745`
的 `elif next_task.writes:`，注释原文 "if it already ran, return the result"）。

它想解的是两件事，第一件不归它管，第二件不值。

### (一) 提交区那四个窗口跟 WAL 无关

那四个窗口（`graph_loop.go` 的「同款约束曾经还有四处」清单）**100% 在内存**：区间内一次
`SaveNS` 都没有（机械核过），durability 写点在窗口下游。

真正根治它的是**把提交区扩到「`Snapshot` 读的全部」**——`nextFrontier` 在提交点已经算得
出来（`res.Targets` / `peekCompletedSiblings` / `assembleFrontier` 全部就绪，且
`assembleFrontier` 是纯机械的去重 + 判 End）。**已实施**（2026-09-01，`ab39423`）：按路径
拆成 `commitSuperstepPartial` / `commitSuperstepAdvance`，各自把自己那条路上的字段改动
全部收进一个临界区，逐点 Goexit 注入双向验证过。

> ⚠️ **别用「数据在窗口上游所以持久化没用」当理由——那句是错的**。在上游正是它可用的
> 前提，LangGraph 就是这么干的。真正的理由是**提交模型**：我们在提交点把 writes 折进
> `cm`（`applyAllLocked`），而 `Snapshot` 读的就是 `cm` ⟹ **任何到达存储的撕裂态，到达时
> 已经是折好的**。WAL 摆在一个已折叠的快照旁边只有两种下场：快照权威 ⟹ WAL 是死数据；
> WAL 权威 ⟹ 在已折叠的 channel 上二次 apply，`Append`/`Sum` 当场双计。要让 WAL 权威就
> 必须**不折**，那不是加一个字段，是换掉提交模型。
>
> 同一条在 `issue-99-state-architecture.md` 已被独立审计推翻过一次（原话：「它借的是一个
> 不同提交模型的经验。那条实测成立的前提是 writes 留在写日志里、从不合进 channel。我们
> 不是」）。别再走第三遍。

**顺带一个正面证据**：我们**已经有 WAL 了**，就在唯一需要它的那一类载荷上。
`cm.pendingSendWrites` 就是一条 mini-WAL——先记不折，`ApplyPendingSendsAndClear` 在同一个
临界区里折 + 清。它只覆盖 `SendDirective.Payload`，因为那是唯一「丢了没法再生」的东西。
**这个模式在我们这儿被评估过、被采纳过、并且被有意限定了作用域。** 提案是把它推广到全部
成员 writes——而那些 writes 是**可再生的**（重跑成员就有），所以推广买到的不是「不丢」，
是「不重跑」。

### (二) 它真正买到的是「硬崩在超步中途时已完成成员不重跑」——洞是真的，但不值

洞实测确认存在（durability=sync，2 成员扇出，i2 已完成后取存储快照）：盘上
`current_frontier` 仍列着两个成员、i2 的完成与它的 writes 一个字节都没有 ⟹ 续跑重跑两个，
副作用 2→4。**注意 channel 值仍然正确（2 不是 4）——这是纯重放，不是数据损坏。**

四个乘数把它压到不值：

| 乘数 | 实测 |
|---|---|
| 必须已开 durability | `exit` 是默认档，那一档硬崩后**盘上一个 cp 都没有**，WAL 无处可用 |
| durability 全仓生产 caller = 0 | `WithDurability` 只命中 compose_v2 内部 + 一个 example |
| 只有宽度 ≥2 的超步有窗口 | `agent_v2` **框架自带的**编排里 `SendFrom` 扇出只有 ParallelAgent 一处（`parallel_builder.go:192`，全仓唯一真调用点，另一处是注释）；BasicAgent 是线性四节点，宽度恒 1。⚠️ **业务经 GraphAgent / 直接用 `compose_v2` 自定义的图不受此限**——它们想扇多宽扇多宽，所以这个乘数只削框架自带路径，不削业务图 |
| 同维度上有个大一个数量级、且已被接受的重放 | `asyncCpWriterBuffer = 8` ⟹ 恢复点最多陈旧 8~9 个超步。为亚超步那一格立项而接受 8 超步，不自洽 |

**另：LangGraph 的粒度是 task = 一个节点，覆盖不到我们最贵的那格**——一个 tools 节点内的
N 个 tool_call（下单 / 扣款 / 发消息）。那格我们有 `SaveNodeState` / `toolCallMemo`，中断
路径有效、硬崩路径失效，而**他们在这格同样失效**。

**非崩溃路径我们已经等价了**：部分失败时 `CompletedFrontier` + `CompletedTargets` 让已成功
成员一次都不重跑（实测新增副作用恰好 1）；`RunWithRetry` 第 2 次尝试同理。

### 三方位置

| | 崩溃恢复粒度 | 默认 |
|---|---|---|
| eino | **只在中断时写 cp**，无 durability 档 | 无崩溃恢复 |
| MAF | 超步末尾 | = 我们的 sync |
| LangGraph | per-task WAL + 超步 cp | `async` |
| **我们** | 超步末尾（sync/async）+ 出口 | `exit` |

我们不是落后一档，是**落后半档，而且只在硬崩这一列**。eino 在这条线上比我们弱一整档。

### 真要减重放，按性价比顺序

1. **改一个常量**：调小 `asyncCpWriterBuffer`，或让业务用 `DurabilitySync`。先吃掉数量级
   更大的那块，零设计改动。
2. **给 `SaveNodeState` 加一个 opt-in 的立即落盘写点**。这才对准我们最贵的那格（tools
   节点内的扣款/下单），**比 LangGraph 的粒度还细**，而且载体已经有了，只差一个写点。
3. 全套 per-member WAL：要求不折叠 ⟹ 换提交模型 ⟹ 与「cp 是单次 Run 临时态、成功即清」
   正面冲突。

**前两档现在也不做**：没有真实场景在要（durability 生产 caller = 0），做了就是给合成探针
造 API。

### 两条追问预案

**「那把存档改成 append-only 呢？」** 那是另一个提案（`issue-99` 的 3b「乙」：`Epoch` →
`VersionID`/`ParentVersionID` + tip + `PruneVersions`），**它不解决本条**：append-only 换的
是「一次坏的写不再摧毁上一个好的恢复点」，而本条的洞是**根本没有那次写**——超步中途硬崩时
盘上压根没有已完成成员的记录，多留几个版本也留不出一份不存在的记录。

**「存储侧真的从不撕裂？」** 单条记录：shipped 后端原子（`sql` 单行 UPSERT / redis 单键），
但**契约没承诺**（`Checkpointer.SaveNS` godoc：「返 error 时 cp 状态未定义」）——业务自研
后端写多行不带事务就能撕，而 WAL 同样受这条契约、救不了。跨记录：`DeleteSubtree` 明写允许
不原子；父/子 cp 是两次独立 `SaveNS`。**但跨记录不一致的形态是「陈旧」不是「损坏」**——
实测（删掉根 cp 只留子 cp，模拟崩在两次 `SaveNS` 之间）：重启后父 fresh run 铸新任务代号，
子档 `BelongsToTask` 不匹配 ⟹ 不被静默捡起，子图重跑一次、旧孤儿档被同 key 覆盖，不残留。
守这条的是**任务代号维**。陈旧的代价还是重执行，回到 (二)。

### 决策记录

| 决策 | 结论 | 依据 |
|---|---|---|
| per-成员 WAL | **不做** | 提交模型不兼容（折叠后的快照旁放 WAL 只有「死数据」和「二次 apply 双计」两种下场）；默认档下是死代码；窗口只在宽度 ≥2 的超步；覆盖不到最贵的那格 |
| 提交区四窗口 | **已根治**（`ab39423`）| 跟 WAL 无关，是把提交区扩到一个临界区。逐点 Goexit 注入双向验证 |
| 调小 async buffer / SaveNodeState 立即落盘 | **记在这儿，现在不做** | 没有真实场景在要（durability 生产 caller = 0），做了就是给合成探针造 API |

---

## 删除的作用域：五家共同的一条纪律（2026-08-11 实测定稿）

上一节把「归属做进 key」判为「更干净但门槛在 taskID 从哪来」。这轮实测把两件事拆开了：
**归属字段和「成功收尾别误伤别人」是正交的两件事**，而后者有一条更朴素、五家全遵守的纪律。

### 纪律

> **删除的作用域 = 「我读进来的」或「我写出去的」，永远不是「跟我地址长得像的」。**

保证它的结构性质有三种，任一种就够：

| 形态 | 谁 | 证据 |
|---|---|---|
| 纯 append + 每条记录唯一 id ⟹ 成功收尾**零 DELETE** | LangGraph（引擎）/ MAF / Step Functions / Temporal | 实跑：路由到 A 中断、同 thread 换输入路由到 B 成功之后，`A:<task_id>` 那个 ns 的 checkpoint **一条没删**；MAF `CheckpointStorage.delete` 在 `_workflows/` 下 0 个调用点 |
| 只删**自己载入的那一个 key** | eino ADK | `adk/turn_loop.go:2027-2028`：`} else if l.loadCheckpointID != "" { _ = l.deleteTurnLoopCheckpoint(ctx, l.loadCheckpointID) }`——`loadCheckpointID` 仅在调用方显式传了 `config.CheckpointID` 时赋值（`:995`），**没载入过就没有删除动作** |
| 只删**自己刚消费掉的那一行** | crewAI | `DELETE FROM pending_feedback WHERE flow_uuid = ?`（`flow/persistence/sqlite.py:280-294`），调用点在消费完那条反馈之后 |

**crew-go 走的是第四条路，而它是自洽的**：`purgeExitSlot` 在 `Run` 出口的调用点（成功 /
终局失败 / 出口档没写成）都是 `DeleteSubtree(sid, cfg.graphNS)`，删一个**子树**（自身 + 经 `/` 往下的后代；
2026-09-23 起同一个节点的 `#`/`@` 兄弟执行不再算在内）。

⚠️ **不要据此判我们违反了纪律**（2026-08-11 实现时被两条设计哨兵证伪，此段是修正后的表述）：
我们把 **`sid` 定义成所有权单位**——「谁在这个 sid 上跑，谁就拥有它底下的一切」，于是「前缀」在我们
这儿**就等于**「我的」。这不是疏忽，是有测试钉死的设计：

- `compose_v2/checkpointer_integration_test.go` 标着「F4 行为」，验**业务自己**在节点里写到
  `(sid, root/business_ns)` 的 cp，Run 成功后**必须被清掉**；
- ⚠️ 这里原先还列着 `TestStaleWriteGuard_PurgeRefusedWithoutBaseline`（验没基线时就该返
  `ErrCheckpointStale`，语义「这个槽不归我」）——**它连同那条分支已于 2026-08-12 撤掉**：
  它钉的是**另一个**所有权定义，跟上面那条 F4 哨兵正好相反，两条同时在仓里是矛盾没被发现。

⟹ **同一个目的两条路**：他们靠「只删我读进来 / 刚消费的那一条」，我们靠「sid 即所有权边界」。
把删除收窄成「只删我读/写的」会同时打破上面两条哨兵。

2026-09-23 的收窄不改这条：它只把**同一个节点的兄弟执行**（`#` fan-out 实例、`@` 重访、同一
节点里对同一个名字的另一次调用）移出**某一次执行**的删除范围——它们是别的执行，旧语义下一次
执行成功就把兄弟还挂着的存档一起删了。任务级清理（`ns = ""`）照旧删整个 sid，上面那条 F4 哨兵
（业务写在 `root/business_ns` 的档必须被清）照旧成立。

⚠️ **这里原先写着「它要防的危害走支持的用法到不了——上层本来就用 busy 守卫禁止同会话并发」，
2026-08-12 实测证伪，两句都不对**：

- **可达**。`agui` / `a2a/server` 免疫是因为**每个任务现铸一个新 sid**（各自 `NewRunSession()`），
  跟 busy 守卫无关——busy 挡的是**并发**，而这里的危害是**串行两个任务**。
- **而 `agent_v2` 层的 sid 是调用方给的**。实测：同一个 `RunSession`，任务 A 中断后被
  放弃（残留 1 条挂起档）→ 任务 B 跑成功 ⟹ **A 正等着用户答的存档被静默删掉，`err = nil`**。
- ⚠️ **但别把这读成「文档教了危险用法」——文档是自相矛盾的**（2026-08-12 校正）：
  `RunSession.RunID` 字段自己的 godoc 写的是「**单次 run 实例 ID** = cp 主索引 = Resume
  入口」，`store.md` 的 checkpointer 那栏写的是「单 sessionID 的**一次 Run**（成功即清）」
  ——**两处都是任务级**；而散文层（「与业务持久化主键对齐」、`WithFreshRun` godoc 把
  「复用会话级 sid」列作典型场景）邀请的是会话级。`store.md` 里「复用 sess 跨 Run 累积」
  那句挂在 `Events` / `values` 两栏，**两栏都明写「不走 cp」**。
  ⟹ 真正的成因是 CLAUDE.md 已点名的头号形态：**一个字段被两种生命周期共用**——
  checkpointer 要它每任务一枚（cleanup 才安全），events / values 要它每会话一枚
  （累积才有意义）。
- 仓内还有独立佐证：`agent_v2/agent_helpers.go` 的注释明写这个场景真实存在
  （「业务复用会话级 sid 时，上一个被放弃任务留下的存档会让探针误判成重入」）。

**诚实的写法是取舍而不是不可达**：这条路可达，我们**选择让最后一个任务赢**——那是能被讨论的
取舍；「到不了」是已被证伪的事实陈述。对照那几家把破坏性动作要么做成 caller 显式能力、要么
必须点名（如「终止已有执行」那一档默认是拒绝而非覆盖），我们是**默认破坏且不报告**。

**我们这条路真实的代价是：边界靠文档不靠机制。** `graph_loop.go` 的 godoc 写了「业务在同 sid 下
保存的无关 cp 也会被清——跨 Run 持久化必须用不同 sid」，但没有任何东西拦着。而**同一种越界，
读路径检测并拒绝、删路径既不检测也不拒绝**：auto-load 撞上 `cp.GraphID != g.ID()` 会
`ErrGraphIDConflict` fail-fast 并提示「应显式 `WithFreshRun`」，出口的删除一声不吭地毁掉它。
这条不对称跟「是谁的错」无关，是我们自己两条路的判据不一致。

### 孤儿：不删，让它不可寻址

我们的子树删**确实在干必要的活**（一次 Run 里放弃分支 s1 改走 s2，s1 留下的子存档是孤儿），
所以不能简单收窄。两家用完全不同的手法让这件事根本不需要清理：

- **LangGraph 把执行身份编进子 ns**：`_algo.py:616-624`
  ```python
  checkpoint_ns = f"{parent_ns}{NS_SEP}{name}" if parent_ns else name
  task_id = task_id_func(checkpoint_id_bytes, checkpoint_ns, str(step), name, PULL, *triggers)
  task_checkpoint_ns = f"{checkpoint_ns}{NS_END}{task_id}"
  ```
  `checkpoint_id_bytes` 是**父 checkpoint 的 id**（每超步换新 uuid6）。实跑：同一个 thread 上跑两个任务，
  子 ns 分别是 `SUB:40934b2f…` 和 `SUB:aafc479e…`，**跨 Resume 稳定、跨执行必然不同**。
  被放弃的分支于是**没人再能寻址到**（成功 run 之后旧 ns 原样躺着，`get_state(subgraphs=True).tasks == []`）。
- **eino 让子存档没有独立地址**：整棵树一条记录（`compose/checkpoint.go:110-118` 的 `SubGraphs` 字段），
  中断时 cp 是全新构造（`compose/graph_run.go:641-645`），只有本次真的中断了的子图被填进去（`:680-682`）——
  上一轮留下、这轮没再中断的子状态不被复制，下次写入自然消失。

**两条都让清理从「正确性必需」降级成「省空间」。** 这正是上一节那个死结的出口。

### 纠正：归属字段买到的不是「别误伤」

上一节把「归属做进 key」当成 (甲) 的更干净替代。这轮的反证是 **MAF 明文不记执行 id**：

> `_workflows/_checkpoint.py:37-41`：a checkpoint is not tied to a specific workflow instance, but rather
> to a workflow definition … the ID of the workflow instance that created the checkpoint is **not** included

它零误伤，靠的是纯 append。⟹ **加了归属字段而不改删除作用域，得到的是「按 RunID 前缀删」——同一类问题换个判据名。**

归属维度仍然值得加，但它买到的是**可运维 / 可观测**：

| 字段 | 对照 | 语义 |
|---|---|---|
| `RunID` | LangGraph `CheckpointMetadata.run_id`（`checkpoint/base/__init__.py:61-62`）/ Temporal `run_id` / DBOS `workflow_uuid` / Step Functions execution ARN | 本次执行的身份，**根和所有子 ns 盖同一个值**（实跑确认）；配套 API 是 `DeleteForRun(sid, runID)`，**跨 ns、正交于前缀** |
| `Parent` | LangGraph `CheckpointMetadata.parents = {ns: cpID}`（`:56`，父在派子任务时逐层 push，`_algo.py:741-743`）| 血缘指针，**只用于读**（time-travel / 从子往上找父存档，`_loop.py:347-357`）。⚠️ **不驱动级联删除** |

`delete_for_runs` 的 conformance 有一条硬要求值得抄：`test_delete_for_runs_preserves_other_runs`
——**不相关的 run 必须原封不动**。

### 上一节「taskID 从哪来」那个门槛，有答案了

MAF hosting 那条路要业务给 context_id，所以门槛回到原点。LangGraph 的做法绕开了它：
**身份从父存档 id 派生**（见上），框架自己算得出，业务什么都不用给，而且跨 Resume 稳定。

### ⚠️ Temporal 的 `ParentClosePolicy` 不是这题的对照物

它三档（`Abandon` / `Request Cancel` / `Terminate`，默认 `Terminate`）、per-child 声明。但它
**终止的是一个活着的执行**，不是**删一份存档**——被 terminate 的 child 的 history 照样留到 retention 到期
（清理由**时间**触发、作用于那一个 execution，不由执行者触发）。

⟹ **「让子别再跑」和「把子的现场抹掉」是两件事**：前者有默认答案（terminate），后者的默认答案是**留着**。
我们的子树删把这两件合并了。

### 顺带：sid 能不能装两个任务

LangGraph 在这个位置**结构上堵死**：顶层 run 强制把 checkpoint_ns 抹成 `""`（`_loop.py:341-345`），
实跑传 `checkpoint_ns: "taskA"` 想开第二个独立任务——**被无声忽略**，两个任务的中断互相覆盖。
平台层则把「一个 thread 上并发多个 run」建模成队列问题（`MultitaskStrategy = reject|interrupt|rollback|enqueue`）。

我们的 `WithSessionID` godoc 已经写了同样的约束（「业务在同 sid 下保存的无关 cp 也会被清——
跨 Run 持久化必须用不同 sid」），但那是**文档约定，没有任何机制拦着**。

### 并发守卫：存储层 CAS 一家都没有

LangGraph OSS 的 PG 写是 `ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO UPDATE`
（`checkpoint-postgres/.../base.py:137-144`），但 `checkpoint_id` 每超步是新 uuid6 ⟹ 两个并发 run
**不互相覆盖，而是各自分叉出一条 checkpoint 链**；conformance 的能力枚举里没有任何 CAS 项。
并发是在**身份层**解的：Temporal `WorkflowIdConflictPolicy`（默认 `Fail`）/ `WorkflowIdReusePolicy`、
Step Functions 的 `name + input` 幂等、Restate 的 idempotency key、LangGraph 平台的 `MultitaskStrategy`。

**「删一个自己没读过的槽」这个情况在他们那儿不存在**，所以也不需要守卫去判它。

### 决策记录（2026-08-11 补）

| 决策 | 结论 | 依据 |
|---|---|---|
| 成功 / 终局失败收尾的删除作用域 | **不收窄**（2026-08-11 实现时证伪原结论）| `sid` 是所有权单位，两条测试钉死；收窄会打破它们，而要防的危害走支持用法到不了 |
| 读 / 删两路的检测不对称 | **该补**——这是上一行收窄掉之后真正剩下的 | 同一种越界读路径 fail-fast、删路径静默毁掉；难点是 `DeleteSubtree` 是子树语义没有逐条校验的位置，且只认得出「不同 graph」这一种越界 |
| 孤儿清理 | 我们靠**成功时整 sid 清扫**兜住（sid 即所有权的直接推论）；只有「Run 没走到成功」那一档漏网 | 两家各一条路（执行身份进子 ns / 整棵树一条记录）把清理从正确性必需降级成省空间——**我们不需要照抄**，但那一档漏网的（见 backlog「Resume 路径不带新任务信号」）用「让它不可寻址」比用「再加一次清扫」干净 |
| 归属字段（`RunID` / `Parent`）| **值得加，但不是「别误伤」的修法** | MAF 不记执行 id 仍零误伤；`parents` 在对端只用于读 |
| `ParentClosePolicy` 式的父子连坐 | **不可移植到删除** | 它终止的是活执行，不是删存档；后者默认留着 |
| 存储层 CAS / 陈旧写回守卫 | **不在删除路径上加**；已有的那个守卫另有一处交互 bug 要修 | 五家零 CAS，并发在身份层解。另：顶层 Run 恒 `WithFreshRun` 让守卫对着**空槽**触发保护，把每个成功的顶层 Run 降级成错误——修在「没基线且槽里没东西时不报错」|


---

## ns 拼串 + 前缀匹配：元字符转义（2026-08-31 调研）

起因：我们的 ns 是拼接字符串（段分隔符 `/ # @`）存一列，查子树用 `ns = prefix OR
ns LIKE 'prefix<sep>%'`。而段里可以合法出现 LIKE 的 `_` / `%`（校验只拒段分隔符），
于是 `LIKE 'round_1/%'` 误命中 `roundX1/child`——而 `round_<N>` 正是 LoopAgent 自己
生成的段。Redis 侧同款（SCAN MATCH 是 glob，`*` `?` `[` 获得通配能力）。

### 对照

| | 存储形态 | 有没有「查 ns 子树」 | 元字符怎么处理 |
|---|---|---|---|
| **crew-go** | 单列 TEXT，`(sid, ns)` 二级 key | **有**（`DeleteSubtree` 承重：abort 清死档、子图终局性向上传播） | 转义 + `ESCAPE '!'` |
| **LangGraph checkpointer** | 单列 TEXT，进主键 | **没有**——所有 `checkpoint_ns` 访问都是精确等值；清理只有 `delete_thread`（按 thread_id 整抹） | 不需要 |
| **LangGraph store** | `prefix` 单列 + `text_pattern_ops` 索引 | **有** | `_escape_like_literal` + `ESCAPE '\'`（PG）／`_escape_glob_literal` 字符类包裹（SQLite，GLOB 无 ESCAPE 子句） |
| **eino** | 整棵嵌套树序列化成一个 blob、单 key | **结构上没有**——删子树 = 内存里 `delete(cp.SubGraphs, key)` | 问题类不存在 |

### 三条结论

1. **转义是标准解法，不是打补丁。** LangGraph 在 2026-07-30 为一模一样的 bug 提交了
   一模一样形状的修复（PR #8478），docstring 的反例是 `user_1` 命中 `userX1`，与我们的
   `round_1` / `roundX1` 同形。连最容易漏的细节都一致：**只转义 LIKE 那一臂，等值臂不能转**
   （转了 `user_1` 就匹配不上自己）——我们两处独立收敛到同一写法。
   我们选 `!` 而非 `\` 作转义符更严谨一档：MySQL 的 `\` 会随 `NO_BACKSLASH_ESCAPES` 漂移，
   LangGraph 的 `ESCAPE '\'` 是 PG-only 才敢这么写。
2. **权威语义是「段列表逐段比」，拼串只是存储层编码。** LangGraph 的 `InMemoryStore` 直接
   `namespace[:len(prefix)] == prefix` 不拼串，两个 SQL backend 的 docstring 都写着「对齐
   InMemoryStore 的逐段比较」。我们的 `IsNSInSubtree` / `matchesPrefix` 是同一角色。
   ⟹ 转义是这个编码的**解码器契约**的一部分，不是补丁。
3. **eino 的「整树一个 blob」能让问题类消失，但代价是我们当初不选它的理由**：每次存档整树
   读写放大、无法只载入某分支、单 key 写入争用、blob 随嵌套深度×扇出无界增长，且做不到
   `LoadAllInSession` / `ListPending` 这类跨 ns 查询。我们的 `(sid, ns)` 物理分离是为嵌套
   子图各存各的付的设计代价，模式匹配是它的副产品。

### 被证伪的两条备选

- **Base64url 编码段**（想让段内容不含元字符）：**实测证伪**——RFC 4648 base64url 字母表
  本身含 `_`，编码后照样带通配符；标准 Base64 含 `/`，那是我们的父子分隔符，更糟。
  真要中和只能上 hex，代价是 ns 长度翻倍 + **人不可读**（ns 出现在日志、错误正文、
  `IsNSInSubtree` 契约里）。⚠️ 顺带纠正一处前提：`a2a/zk` 用 Base64url 的理由是**跨语言
  路径等价性** + ZK 路径段不能含 `/`，那里做的是结构化树遍历、根本不做模式匹配，
  **不构成本问题的先例**。
- **ltree / 数组列**（原理上能消灭问题，`<@` / 数组切片比较不做模式匹配）：被三条硬约束
  挡住——PG 专有而我们是 PG+MySQL 双方言；ltree 标签字符集受限（`A-Za-z0-9_`）而我们的段
  允许空格/中文/`%`，要用得先编码（问题被搬走不是消灭）；对 Redis 那一半零帮助。
  全生态扫描（langgraph / mem0 / crewAI / openai-agents / agent-framework / langfuse）
  `ltree` 与「数组列做子树查询」均 **0 命中**——层级 namespace 一律拼串 + 前缀匹配。

### 落到我们身上的两件事

**两件都已做掉（2026-09-01/02）**：`sql` 模块接上了 conformance（sqlite 方言真跑）；
`/ # @` 分隔符集的三份拷贝收成一份——cp 层走「枚举 + 直接调判据函数 + 分批 DELETE」
（不走模式语言 ⟹ 不需要转义），`redis` 的 Lua 分隔符集改从 Go 侧注入。
**store 层按原计划保留可下推索引的 LIKE**（namespace 是业务多租户维度、数量无界，拉全表
再 Go 侧过滤会退化成全表扫）——LangGraph 宁可留 GLOB 也不用 UDF 正是这个理由（planner
对 UDF 不透明 ⟹ 每行回调）。

**2026-09-23**：删除范围收窄成「自身 + 经 `/` 往下的后代」（`#`/`@` 是兄弟执行，不删），
判据函数是 `IsNSInSubtree`，redis Lua 从 `NSSubtreeSeparator()` 注入的只剩层级分隔符一个。
当初那条「backend 源码里有没有手抄分隔符集」的扫描随之删除（只剩一个分隔符，「同一行列出
≥2 个」的判据不再有意义；新增实例段也不再需要 backend 跟进）；回归改由 conformance 的
`DeleteSubtree_Sibling_Executions_Survive` 在各后端实跑兜住。
