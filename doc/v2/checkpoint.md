# Checkpoint 与 Resume（断点续跑）

> **本文档定位**：cp 机制的**权威源**——数据结构 / 生命周期 / 寻址 / 并发保护 / Resume 协议。
> 它是 [harness.md](harness.md) 里那个"底座"的详细篇。
>
> **边界**（这三处不在本文重复，改动时也别在本文另写一份）：
> - **HITL 的业务语义**（`InterruptError` 字段、`AllQueries()`、`ResumeAction` 三模式与支持
>   矩阵、业务配对答案的完整流程）→ [hitl-resume.md](hitl-resume.md)
> - **图执行模型本身**（BSP 四阶段、Channel/Reducer、fan-out 不变量、类型注册）→
>   [channel-engine.md](channel-engine.md)
> - **三方对照与历次决策记录** → [checkpoint-storage.md](../description/three-way-comparison/checkpoint-storage.md)
>
> **字段级细节以 godoc 为准**：`compose_v2/checkpointer.go` 的字段注释比本文详尽（每个字段都
> 写了「业务自研 backend 漏了会怎样」），本文负责组织与入口，不逐字复述。

---

## 1. cp 是什么、不是什么

cp 是**一次 Run 的执行状态快照**：停下来的时候（等人回答 / 出错 / 被取消 / 崩溃）把 channel
state、下一帧要跑谁、挂着哪些问题一起存下来，下次从这里接着跑。

**它是单次 Run 的临时态，成功即清**（§3.3）。由此推出两条经常被误解的边界：

- **cp 不是会话历史**。想「跨多次 Run 在同一场会话里留下对话」是另一个维度的需求，归
  `agent_v2` 的会话历史层（`WithHistory` + `HistoryStore`，按 ThreadID 组织，append-only）。
  `Checkpoint` **没有 ThreadID 字段**，`Checkpointer` 接口也**没有任何按 thread 查询的方法**
  （`ListByThread` / `LatestByThread` 那套伪历史 API 已删）——想按会话取历史别在这一层找。
  见 [ISSUE-98](../issue/issue-98-conversation-history.md)。<br>⚠️ 别被文件位置误导：
  `WithThreadIDCtx` / `GetThreadID` 确实**定义在 `compose_v2/checkpointer.go`**，但它们只是
  跨层透传的**会话身份声明**，服务于历史层与观测层，**不参与 cp 寻址**（cp 只认 `(sessionID, ns)`）。
- **cp 不认识"会话"**。它只认 `sessionID`（= 一次任务），`RunSession.RunID` 的契约是**一枚
  只服务一个任务**。想表达「这几个任务属于同一场对话」用 `compose_v2.WithThreadIDCtx` 的
  ConvID，那归历史层与观测层。

本仓一共 6 层 state，cp 是其中一层，边界见 [store.md §1](store.md)。

## 2. 数据结构

### 2.1 Checkpoint 全字段

```go
// compose_v2/checkpointer.go
type Checkpoint struct {
    // ── 元数据 ──
    ID        string           // = sessionID（SaveNS 自动填）
    NS        string           // 本 cp 的 ns 路径（嵌套寻址用）
    ParentNS  string           // 父 ns（O(1) 反查祖先）
    Status    CheckpointStatus // Unset / Pending / Failed —— **只是报告位**，不参与控制流，见 §3.1
    SchemaVer int              // 当前 CheckpointSchemaVer = 18
    GraphID   string           // 防 cp 串台（§4.3）
    CreatedAt int64            // UnixNano，Cleanup 按它排序；框架只在 caller 没给时盖
    Epoch     string           // 写代：每次 SaveNS 换一枚（§2.2）
    TaskID    string           // 任务代：新开时铸一枚，此后一路带下去不换（§2.2）
    InputDigest string         // 新开时那次调用交来的摘要：auto-load 续跑时比对——是调用的身份时对不上即撞槽、拒绝（不清档），只作核对时告警；不决定捡不捡（§4.2）

    // ── 业务 channel state（核心）──
    Channels          map[string]taggedValue // 本层**自己写过**的 channel 值（per-channel typed JSON）
                                            // ⚠️ 子图 cp 里**不含** WithImport 摆进来的 baseline
                                            //    （那层每次进入都从父图重摆、不进 Snapshot）——
                                            //    子图内 Get 读到 5，子 cp 里可能一条都没有
    PendingInterrupts []*AskQuery            // 挂着的问题
    ConsumedAnswers   []string               // 已兑付的答案位（「一个答案只能满足一个提问」的账本）
    Signals           []SignalRecord         // 本图已提交的上行信号（转交 / 路由决定），图成功跑完时交给调用方
    CompletedSignals  []SignalRecord         // 部分失败超步里成功成员先记在一边的上行信号（整超步完成时再合并）
    RecipientKey      []byte                 // 铸收件人的密钥（compose_v2.SignalRecipient），续跑时同一个位置算出同一个收件人
    CurrentFrontier   []FrontierMember       // 下一帧要跑谁
    FrontierStarted   bool                   // 这一超步开没开始执行（成员中断 / 失败之后的档为真；停在超步边界上的——软取消、ctx 取消、断点停下——为假）——续跑时决定判不判 before 断点
    NodeVisits        map[NodeID]int         // 每节点成功跑过几次——中断 ID 的 visit 维度靠它跨 cp 递增

    // ── 部分失败超步的补充（缺一个都是静默丢数据，见 §2.4）──
    CompletedFrontier []FrontierMember  // 中断超步里已成功的成员
    DeferredSends     []SerializedSend  // 成功成员发出的 SendFrom 指令
    PendingSendWrites []SerializedWrite // 没能提交的 SendFrom payload（待 Resume 补 apply）

    // ── 节点私有中断态（§5）──
    NodeStates map[string]taggedValue
}
```

**cp 粒度 = channel 粒度**：`Channels` 是 per-channel 自描述序列化
（`taggedValue{Type, Data}`），业务加新字段不用动序列化协议。

### 2.2 三个"身份"字段别混

这三个都是标识符，但答的是不同问题，混用会出静默错，所以单独列一节：

| 字段 | 答什么 | 换的频率 | 混用的后果 |
|---|---|---|---|
| `Epoch` | 这份存档是**第几次写**的 | **每次 `SaveNS` 都换** | 拿它算中断 ID → 同一次 Run 内跨超步就漂 |
| `TaskID` | 这份存档属于**哪一次任务** | 只在新开路径铸，此后不换 | 不掺进中断 ID → 两个任务走到同一位置时 ID **确定性相等**，给任务 A 的答案被任务 B 的问题消费掉，`err == nil` 全程无告警 |
| `FrontierMember.InstanceID` | 这条记录是**哪一个并发实例** | 随 `SendFrom` 派生 | 只有 NodeID 时框架分不出谁是谁，多实例互相冒充 |

`Epoch` 用**随机 token 而不是递增计数器**：只需判"变没变"、不需要比大小，而递增计数器跨进程
重启会从头开始、与存档里的旧值撞号 → 误判"没变"。

### 2.3 配套类型

```go
type FrontierMember struct {
    NodeID     NodeID
    InstanceID string // 空串 = 静态边/cond 边成员（恒单实例）；非空仅来自 SendFrom 派生
}

type SerializedSend struct {
    Target          NodeID
    Payload         []SerializedWrite
    Owner           NodeID // 谁发的——重跑它时框架据此清掉旧 send，否则 Target 双发
    OwnerInstanceID string // 执行单位是 branch 实例不是 NodeID，故按实例精确匹配
    InstanceID      string // 这条 send 派生出的 target 实例身份（重放时不换新）
}

type SerializedWrite struct {
    Channel string
    Type    string
    Data    json.RawMessage
}

type SignalRecord struct {
    Kind   string // 信号种类的名字（compose_v2.NewSignalKind 铸造时给的）
    Dest   string // 收件人：等它的那个消费方（如 HandoffAgent 的某一跳）
    Value  string // 决定本身（转交目标 / 路由分支）
    Origin string // 写入者的位置（只用于报冲突时定位）
}
```

`Signals` 是本图已提交的上行信号（见 `compose_v2/signal.go`）：节点提交时、以及入口条件边算出来时
并入，一个 (种类, 收件人) 至多一条（先后做出的后者为准）；图**成功**跑完时整批交给调用这张图的那段执行，
停下时留在存档里，所以做出决定的那一步续跑时不再执行也不丢。`CompletedSignals` 是同一族的另一半：
同一超步部分成员失败时，成功成员做出的先记在这里，等其余成员续跑成功时按「同一超步只能有一个成员做决定」跟它们合并
（续跑的成员也做了决定 ⟹ `SignalConflictError` 并带 `ErrRunAborted`）——直接并入 `Signals` 的话续跑成功的就成了「后做的」、覆盖掉兄弟的决定，
同一张图停没停过结果不同。`RecipientKey` 让收件人续跑时算出同一个值、图外的代码经正常入口算不出来（密钥就在存档里，故意读出来自己算的不在防护范围内）。它**一个任务一把**：顶层新开时铸，嵌套图沿用，每张图的档都带一份——中间哪一层的档没写成，续跑时从上层继承回同一把；顶层自己的档没写成、只能新开时，任务代号也换了，子层旧档过不了续跑闸门、不会被复活（曾经每张图各铸各的、随本图下一次存档才落盘：消费方的档没写成时子层已落盘的决定续跑后静默丢失，三个耐久档都中）。

⚠️ **两组 frontier 的身份都内联在记录里**，不是「NodeID 数组 + 平行的身份数组按下标对齐」——
那种对齐约束类型系统够不着，错位之后实例互相冒充**且不报错**。

### 2.4 backend：先看内置的，再谈自研

**框架已内置三个实现**，多数场景不需要自研：

| 实现 | 构造 | 用途 |
|---|---|---|
| `InMemoryCheckpointer` | `compose_v2.NewInMemoryCheckpointer()` | 单进程 / 测试；随进程死 |
| Redis | `redis.NewCheckpointer(rdb, opts...)` | 独立 go module |
| SQL | `sql.NewCheckpointer(db, dialect, opts...)` | Postgres + MySQL 双方言，独立 go module |

⚠️ **`Durability` 的崩溃恢复价值需要配持久 backend 才成立**——InMemory 随进程一起没了，
sync/async 每超步写出来的中途 cp 救不回任何东西。

**真要自研**（Mongo / 自家 KV / 多租户分片）时，这一层最隐蔽的地方是：**漏序列化任何一个字段
都不报错**，只是各自退化成一种静默的错。

| 漏了 | 退化成 |
|---|---|
| `CompletedFrontier` | 发散 fan-out 部分失败时，Resume 后**成功分支的后继永久丢失** |
| `DeferredSends`（或其中 `Owner`）| Target **双发** |
| `PendingSendWrites` | SendFrom 的 target **静默读到零值** |
| `FrontierMember.InstanceID`（含空身份那条记录整条要留）| 多实例 Resume 后**实例互相冒充**，InterruptID 撞 → query 被 dedupe 吞 → 那个实例永远等不到答案 |
| `Epoch` | `WithStaleWriteGuard` 静默失效 |
| `TaskID` | 跨任务 ID 退回必撞；且首跑与续跑算出的 ID 不同，调用方手里的答案配不上 |
| `InputDigest` | 两次不同的调用撞进同一个存档槽时认不出来——后一次续上前一次的进度、`err == nil`；带 key 的调用输入变了的告警也不再出现 |
| `FrontierStarted` | 读回恒假：续跑时把开始过的超步当成没开始，before 断点（编译期 ∪ 运行期）在它前面多停一次——那次续跑带的答案没被兑付（停在节点执行之前），下一次续跑同一个问题以同一个 ID 再问，答案要再带一遍。读回恒真则相反：软取消停在边界上之后、续跑时新挂的 before 断点不停 |
| `PendingInterrupts[i].Breakpoint` | 引擎靠它判「上次是不是停在断点上」：漏了的话，停在 before 断点上的档每次续跑都被重判、在同一个边界再停（第一次续跑的 ID 与原来的不同，之后固定），图永远过不去；停在 after 断点上的档续跑照常放行（恢复出来的边界不判 after）。调用方也从存档里分不出「这是断点、续跑即放行」与「节点在等答案」 |
| `NodeVisits` | visit 不再跨 cp 递增 ⟹ **同一节点不同轮次的提问算出相同中断 ID**（ReAct 多轮 / Loop 重访 / Plan 循环都会命中），上一轮的答案被这一轮消费 |
| `ConsumedAnswers` | 「一个答案只能满足一个提问」这道兜底静默失效——上一条那种 ID 撞车从**当场报错**退回**静默生效**（用户根本没被问到而 `err == nil`）|
| `Signals` | 做出转交 / 路由决定的那一步已经提交、续跑时不再执行，决定静默丢失——转交没发生、路由落到兜底，`err == nil` |
| `CompletedSignals` | 部分失败超步里成功兄弟做出的转交 / 路由决定续跑后静默丢失；续跑的成员也做了决定时（值相同也算），本该报的冲突也报不出来、它的值静默胜出 |
| `RecipientKey` | 续跑时密钥重铸、收件人变了，停下之前已提交的转交 / 路由决定认不出是发给谁的，静默丢失 |
| `NodeStates` | 退化成「全部重跑」（= 引入前行为）：节点里已做成的副作用重放，其中包括已完成的嵌套调用——它们的完成记录也在这里（§5）。问过人的嵌套调用丢了记录之后：调用方只答当前一问时，两个问题轮流出现、永不收敛（实测）；累积传答案时不重问（旧答案被静默复用），但子 agent 整个重跑、副作用重放 |
| `CreatedAt`（绕过 `Run` 直接写 backend 时）| 恒 0，被 `Cleanup` 当成最旧的优先删 |

⚠️ 同一类静默错还有一处不在字段里，在**键怎么比较**：(sid, ns) 逐字节就是存档的身份，逐字节
不同的两个会话 / 两个 ns 是两份档——只差尾随空格、大小写、重音或组合字符写法的也是。键列用了
非二进制的排序规则（MySQL 的 `utf8mb4_bin` 忽略尾随空格，MySQL 8 默认的 `utf8mb4_0900_ai_ci`
还不区分大小写和重音）时它们落进同一行——后存的覆盖先存的、删一个会话连带删掉另一个，
`err == nil`（真 MySQL 8.0.35 实测）。conformance 的 `KeyIdentity` 对这四种差异逐一钉；`sql` 的
`Setup` 因此在 MySQL 上把身份列建成 VARBINARY、PG 上声明 `COLLATE "C"`，业务自管 schema 要照做。

**跑 conformance 套**验语义合规：

```go
import "crew-go/compose_v2/checkpointer_conformance"

func TestMyPGCheckpointer(t *testing.T) {
    checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
        cp := mybackend.NewPGCheckpointer("postgres://...")
        t.Cleanup(func() { _ = cp.Close() })
        return cp
    })
}
```

## 3. 生命周期

### 3.1 Status 三档

```go
const (
    CPStatusUnset   CheckpointStatus = iota // 默认值（旧 cp 反序列化兼容）
    CPStatusPending                         // 中断挂起，等 Resume
    CPStatusFailed                          // 上次 Run 带着错误退出——失败现场，不是等答案的挂起
)

func (s CheckpointStatus) Resumable() bool { return s == CPStatusPending || s == CPStatusFailed }
```

⚠️ **`Status` 只是报告位，不参与控制流。** 「捡不捡这份档」的三处闸门（Run 入口 auto-load /
子图载入 / `agent_v2.Resume`）统一读 **`Checkpoint.HasResumableWork()`**——按**内容**判：
frontier 或挂起清单非空。`agent_v2/resume_helpers.go` 里那道守卫用的就是它，`Status` 只进
错误消息的格式化参数。

**为什么按内容而不按档位**：

1. 对业务自研 backend 漏序列化 `Status` 鲁棒——按档位判时，一次真 HITL 中断会退化成 fresh
   run + 换一批新 `InterruptID`，用户手上那个答案永远配不上
2. 拒得掉「业务把一份跑完的快照存进这个槽」——按「存在与否」判会静默返 nil、零节点跑完，
   存档还被当成功清掉
3. 与引擎出口早退、`SnapshotForGraph` 是同一个谓词，判据只有一份、不会漂

**撤掉的那道档位 gate 挡错了东西**：它曾按「auto-load 只捡 Pending」挡「失败档别被自动捡起」，
立论是「下次同 sid 跑会静默从失败点恢复、再撞同一个错」。但那个担心在**顶层**已由
编排入口的新任务判定（`enterRunTask`：新任务恒推「新开」信号）结构性覆盖；而 gate 在**嵌套层**挡掉的是**合法续跑**——
Parallel + HITL 下 Resume 重跑失败分支时，子图捡不起自己那份失败子档 ⟹ fresh run ⟹
扣款重放（实测 1→2，撤掉后 →0）。

**所以失败档也续得上**：`Resume` 不带答案 = 重试失败分支（成功兄弟不重放）；带答案 = 答问题；
两者可叠加（混合档一次调用把两件事都办了）。

**光看字段分不出档位，必须由出口标**：失败存档和 durability 中途存档长得一样（都有 frontier、
`PendingInterrupts` 为空），区别只有 Run 出口知道。

**出口标什么档位**是另一个判据：`shouldStayPending`，比 `IsControlFlowSignal` **宽**，除控制流
信号外还含两类**返业务错但框架决定存着等重跑**的——`CondFailRerunFrom` 档的 cond 报错、坏存档的
`ErrCheckpointNotResumable`。⚠️ **按类目读、别数个数**（这份名单加过项），`compose_v2/retry.go`
的函数体是真相源。

⚠️ **「标什么位」和「能不能续」从此是两个谓词，别当同义词**：前者 `shouldStayPending`，后者
导出面的 `LeavesResumableCheckpoint`（判「这次退出**留没留下**之后能续上的档」——**普通业务错也留**）。
两者曾经是同一个，gate 撤掉后真值表分了叉。跨包消费者（`agent_v2` 的历史落点）读的是后者，
混用会让「业务错退出、之后 Resume 重试成功」这条链上的用户输入落不进历史。

### 3.2 何时存

| 触发 | 存什么 | Status |
|---|---|---|
| `InterruptError` 抛出（HITL）| Channels + Frontier + PendingInterrupts | Pending |
| 业务节点 return error | Channels + Frontier | Failed；**但 `CondFailRerunFrom` 档的 cond 报错是 Pending**（返的是业务错、框架仍要它被 auto-load 捡起重跑，见 §3.1）|
| `ctx.DeadlineExceeded / Canceled` | 同上 | Pending（控制流信号）；出口写档不跟着调用方取消（§3.5）|
| 软取消 `ErrSoftCanceled` | 同上，当前超步善终 | Pending |
| panic（`safeNodeRun` recover 转 error）| frontier 非空时存 | 按上面判据 |
| `Durability=sync/async` 每超步 | 全量 cp | Pending（崩溃恢复点，必须能被捡起）|
| 正常完成 | ❌ 不存，且主动删（§3.3） | — |

### 3.3 何时清

成功与终局失败都经 `purgeExitSlot` 清（出口档没写成时也用它清旧档）——「这个槽里的档不该留」：

- **成功**：Run 出口 defer `DeleteSubtree(sid, graphNS)`。避免 Run 1 留下的 cp 污染 Run 2。
- **`ErrRunAborted`**（终局失败）：Run 出口**主动删子树**。

⚠️ **必须主动删而不是"跳过写"**：cp 是 `(sid, ns)` **单槽覆盖式**，跳过写只是不覆盖，先前那份
（HITL 档 / durability 中途档）会幸存且 `Status=Pending` → auto-load 捡起 → 重跑已成功的节点 /
**重问已答过的问题** / 子图 **livelock**（子跳过写但父照存 → 反复 Resume 反复问，永不收敛）。
子图的终局性会经 `%w` 冒泡自动向上传播，父子树一并清。

**「子树」只沿 `/` 往下**：`DeleteSubtree(sid, P)` 删 P 自身与 `P/…`，不删 `P#k`、`P@v`——
它们是同一个节点的**其他执行**（fan-out 实例 / 重访 / 同一节点里对同一个名字的另一次调用），
各自在成功或终局失败时清自己的那棵；清整个任务走 `P = ""`。判据是 `compose_v2.IsNSInSubtree`，
三个内置后端与 conformance 同一口径。2026-09-23 之前把 `#`/`@` 也算作后代，于是一次执行的
成功清理会连带删掉兄弟执行还挂着的存档（同一个节点里对同一个名字调两次时第二次的挂起档、
子图节点同超步 SendFrom 实例的挂起档），被删的那次只能从头跑。

业务想留 cp 调试：不传 checkpointer，或在删之前用 hook snapshot（没有 `WithSkipCleanup`——
「成功也保留 cp」不该是一个选项）。

### 3.4 落盘时机 Durability

`DurabilityExit`（默认，仅出口写，硬崩丢进度）/ `DurabilitySync`（每超步同步阻塞写，扛硬崩）/
`DurabilityAsync`（后台单 worker 写，FIFO 有序）。ctx 可透传、嵌套子图继承父档。崩溃恢复的价值
需要配持久 Checkpointer 才成立。详见 godoc `compose_v2/durability.go`。

### 3.5 存不下时：`ErrCheckpointNotPersisted`

Run 出口要写的档没写成（`SaveNS` 或写档前的 `Snapshot` 失败）时返回它。挂起类的退出（中断、软取消、
调用方取消、`CondFailRerunFrom` 的 cond 报错、坏存档）与业务报错退出都算——两者的档按设计都续得上。

**不变量：档没写成，就不能报告成可以续，槽里也不能留着能被续进去的旧档。**

| | 处置 |
|---|---|
| 错误链 | 挂起类信号**不在链上**（`IsInterrupt` 为假，`ErrSoftCanceled` / ctx 错判不出来），只进文本；业务错在链上；写档的原因在链上（`ErrCheckpointStale` / `ErrGraphIDConflict` 照样判得出来） |
| 终局 | 同时带 `ErrRunAborted`：不自动重试、各吞错点不吞（`MustPropagate`）、出口尽力删这一层的子树（删失败写进错误文本），嵌套时上层出口跟着清 |
| 例外 | 写档撞上 `ErrCheckpointStale`（别的执行推进了这个槽）或 `ErrGraphIDConflict`（槽里是另一张图的档）：不带 `ErrRunAborted`、不删——那份档不是这次执行的。照样不自动重试（实现 `NonRetryable`） |
| 扇出仲裁 | 排在中断之前：`终局失败 > 坏存档 > 档没写成 > 中断 > 第一个业务错`（槽属于别人那种不带 ErrRunAborted，靠这一档才不被兄弟的中断盖掉） |
| 前端适配 | agui 给 `checkpoint_save_failed`、a2a 以 failed 收尾，两边都清自己的挂起状态、文案不带后端原文 |

为什么这样：曾经把写档错误跟原错误一起 `%w`，中断仍在错误链上 ⟹ 前端照常请用户回答，答完续跑
找不到档（工具 0 次执行），或续进槽里回答之前那份——已经答过的问题重问，再答一次副作用重做（实测）。
业务报错退出同理：失败档没写成时按「失败档可续跑重试」去续，续进的是更早那份。

**出口记账的三条不变量**（`compose_v2/exit_checkpoint.go`）：

1. **结论只看存储调用的结果**。档写成了照原样报告（中断照报），没写成报 `ErrCheckpointNotPersisted`。
   存储调用之外的代码——cp 事件（业务的 OnNodeEvent 钩子）、日志（handler 也是业务代码）——都在结论定下、
   该清的档清完之后才调，而且只作观测：它们的 panic 当场兜住、记一条带栈的日志，不进返回的错误、不改结论；兜
   panic 的存储调用本身另算（自研后端 panic = 这次调用失败）。成功收尾时发 cp.cleanup 的那一刻档已经清掉，让 Run
   因此报错只会引来从头重跑、副作用重放（曾经追加到错误上）。入口与中途写档发的 cp 事件、节点里经 `EmitCpEvent`
   发的同样只作观测。曾经写档成功之后
   cp.save 事件的钩子 panic 被当成写档失败、刚写好的档被清掉；陈旧写回检查里的告警日志在兜 panic 的那段里记，
   handler 一 panic 写档就不做了；更早时后端 panic 冒到 Run 最外层的 recover、跟中断并列，中断还在错误链上
   （都是审计实测）。
2. **调用方取消或超时不截断出口的存储调用**：每次调用用 `context.WithoutCancel(ctx)`，调用方结束之后再给
   10 秒宽限；写档只发一次。陈旧写回检查（开了守卫才有）跟写档、清档各是一次调用、各有各的宽限。检查拿不到
   结论（读报错、宽限到点、预算用完没发、读档 panic）时写档、清档都放行：检查是尽力型的（先读后写、不是原子的），
   拦下的代价落在合法调用上——拦写档丢自己的档，拦清档留下旧的挂起档、同样的答案再续跑一次会重放节点（曾经清档
   不删，审计实测）；放行的代价只在同一个 sid 上并发跑两个任务（违反 RunID 契约）时出现。写档结果不明（宽限到点，可能已经写进去）时，清档前的检查把这次写的
   那一代认作自己的——只认得出检查之前已经落地的提交：后端在客户端放弃之后、清档之后才提交时（非事务后端的固有
   竞态），槽里会留下这次写的档，而结论是档没写成。曾经用已经取消的 ctx，持久后端一律直接报错（SQLite 实测
   `sql cp SaveNS graphid check: context canceled`），「调用方取消之后可以续跑」只在内存后端上成立；后来改成写到
   一半到点就用宽限再写一次，第二次的检查把自己刚写的档当成别人写的；检查与写档共用一段宽限时，慢但每次都在
   宽限内的后端照样写不进去（审计实测）。
3. **宽限从每次调用自己的起点算**，不会被更早的事件预先耗掉。调用方放弃（顶层 Run 的 ctx 结束）之后，每一类
   调用（读 / 写 / 删）失败所占的墙钟时间——几个调用重叠的部分只算一次，调用方放弃之前的部分不算——累计不超过
   一个宽限：用完之后，自己的 ctx 已经结束的同类调用不再发；没用完时，调用最多等剩下的那部分。成功的调用、自己
   的 ctx 一直活着的调用（例如用 `WithoutCancel` 跑的嵌套 Run）不计也不拦。于是调用方放弃之后 Run 在出口多等的
   时间有上界（每一类最多约两个宽限：用完时还在途的调用等完它开始时剩下的那部分），跟嵌套层数、调用次数无关；
   后端慢但每次都在宽限内完成时照常写完、清完。各类分开记，是因为写档挂住时清档往往还删得掉，删掉才不会留下
   能被续进去的旧档。曾经整个任务共用一个截止时间：内层自己超时那次就把它定下了，之后调用方取消时外层出口已经
   没有宽限，健康的后端也写不进去，任务丢了；改成「等满过一次宽限才不再等」之后，后端自己很快报错（客户端超时
   比宽限短）时永远等不满，每一层的清档都再等一次；改成按每次失败各自的耗时相加之后，并行分支同时失败时同一段
   墙钟被记了几遍、横跨放弃时刻的调用连放弃之前的部分也记上，预算提前用完，该清的旧档没清、该写的档没写（都是
   审计实测）。

宽限只对尊重 ctx 的后端生效：不看 ctx 的后端挂住时，调用照样不返回。

**跟着改的几处**：`RunWithRetry` 的框架守卫不重试它（槽属于别人那种不带 `ErrRunAborted`，只靠类型上的
`NonRetryable` 的话，业务写个宽松谓词就会重跑——审计实测 4 次）；PlanAgent 的临时子 agent 配置随这一步进
存档（`planExecStep.TmpSubAgent`），不再放在实例上的一张表里——表的清理时机跟「续跑要读它」反复打架：按
「是不是中断」清时，陈旧写回那种退出把别的执行还要用的配置清掉了，按「续不续得上」清时，被放弃的任务一直
留在表里，换进程续跑则根本读不到；agui 丢掉会话句柄时（通用错误分支）把档一起清——浏览器断开之后引擎
照常存下了档，agui 却没有续跑入口，持久后端上越积越多。

## 4. 寻址

### 4.1 (sessionID, ns) 二级 key

```go
type Checkpointer interface {
    SaveNS(ctx, sessionID, ns string, cp *Checkpoint) error
    LoadNS(ctx, sessionID, ns string) (*Checkpoint, error)
    ExistsNS(ctx, sessionID, ns string) (bool, error)
    DeleteSubtree(ctx, sessionID, nsPrefix string) (int32, error)
    ListPending(ctx, sessionID string) ([]NSCheckpoint, error)      // 只返 Pending（§3.1）
    LoadAllInSession(ctx, sessionID string) ([]NSCheckpoint, error)
    SaveBatch(ctx, sessionID string, batch map[string]*Checkpoint) error
    Cleanup(ctx, keepLastN int32) (int32, error)                    // 唯一跨 session 的
}
```

### 4.2 嵌套物理分离

每个嵌入运行的 sub-agent 通过 ctx push graphNS，子 cp 物理写到 `(parentSID, parentNS/childName)`
（childName 是编排给成员的 ns 段，如 Plan 的 `<名字>_step_<N>`）——多层嵌套互不覆盖。**存档只按图
写**：节点没有自己的 cp，节点里发生的中断记在它所在那张图的 cp 上。PlanAgent 的 researcher 成员在
tools 节点里 ask_user 挂起时：

```
root_sid:
  "":                    → Plan 图的 cp（Frontier=["researcher"]，即执行这一步的 worker 节点；含冒泡上来的 PendingInterrupts）
  "researcher_step_0":   → researcher（BasicAgent）那张 ReAct 图的 cp（Frontier=["tools"]，含本次 ask_user 的挂起）
```

⚠️ 自定义 `NodeFunc` 里调子 agent **一律走 `agent_v2.RunSubAgent`，别手拼 childNS**（不只是
多实例场景）。一次嵌套调用的**身份**由两部分组成：

- **位置**——调用方在哪（所在节点 + 相位：节点体 / 某个图钩子 / 某条条件边；节点级钩子与条件边的
  相位名含所属节点与它的第几次执行，图级钩子的含这张图的 ns，见 `compose_v2.SubCallScope`）+
  `nodeID` + 实例维三选一：图级
  fanout 实例 / 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态重访 visit（fanout 实例
  节点里又起 tool_call 并发时，实例与 tool_call 一起进位置）；
- **这是哪一次调用**——不带 key 时是**输入内容 + 同一份输入在本次执行里是第几次**
  （`compose_v2.NextSubCallIndex`；计数作用域是一次节点执行 / 一个 tool_call / 一次图钩子或工具钩子调用 /
  被调方在自己图外的那一段，重跑时从 0 重新数）；带 `agent_v2.WithCallKey(k)` 时是 key。**全历史输入（且没有
  自定义 schema）的 `AgentAsTool` 与 skill fork 只看位置**：它们的输入由框架现拼（全历史输入从 session 事件拼，事件不进存档），续跑时拼得
  不一样也还是同一次——按内容定身份的话换槽重问，最后在空输入上算出结果（实测）。声明了
  `agent_v2.WithRerunnable` 的调用身份里再带上调用所在分域的重做代际（见下「产出不合格要
  重来」）。

存档槽是 `JoinFanoutNS(parentNS, nodeID, 身份哈希的低 63 位)`（固定 63 位，与平台字长无关）。这些
判定住在 `RunSubAgent` 里，手拼 `JoinNS(parentNS, nodeID)` 漏掉哪一维都会出事：漏实例维 ⟹
SendFrom 同 Target 投 N 次时 N 个实例算出同一个 childNS、子 cp 互覆；漏 visit 维 ⟹ 父循环第 2 轮
的提问与第 1 轮撞同一个 InterruptID（答案账本跨子图边界后是响亮的 `ErrRunAborted`，之前是静默
复用旧答案）；漏调用方节点 / 相位 ⟹ 两个节点、或同一个节点的两次执行在钩子里用同一份输入调同一个
子 agent 撞成一次（同样 `ErrRunAborted`）；漏输入内容 ⟹ 同一个节点里带着新输入的第二次调用捡起
第一次的失败存档接着跑，新输入没进图（第一次是确定性失败时以旧错收尾，瞬时失败时拿旧输入的结果
当第二次的返回、`err == nil`）。手拼还拿不到下面说的完成记录。

同一个 nodeID 下的几次调用是**兄弟执行**，不是彼此的下级：一次成功时清理自己的子树
（`DeleteSubtree` 只删自身与经 `/` 往下的后代，见 §3.3 与 `compose_v2.IsNSInSubtree`），不碰
别的调用还挂着的存档。

**父节点重跑时**（中断续跑 / 失败续跑 / `RunWithRetry` 重试），每次调用按身份找回自己的东西：

- **已完成的**直接拿回记下的结果、不再执行——整组事件记在父节点这次执行的私有态里
  （`compose_v2.SaveStepResult`，见 §5），随父节点的存档落盘；子 agent 里的副作用不重放、问过的
  问题不重问。它当初交给上层 HandoffAgent / RoutingAgent 的**转交与路由决定**不在事件里（是上行
  信号，见 `compose_v2/signal.go`），随记录一起记下、拿回时原样交出——否则转交 / 路由决定静默丢失、
  `err == nil`（实测）；
- **没完成的**续上自己的存档，只重跑失败 / 挂起的部分；
- **新的**（输入变了、或这次新加的调用）从头跑。

`AgentAsTool` 只记最终那条消息（它只用得到它），而且**不分是谁调起的**：tools 节点的工具账只在整个
tool_call 完成时记，AgentAsTool 返回之后同一个 tool_call 还可能挂起（组合工具接着问人、同名装饰器 /
中间件在转发之后问人），那时只有这份记录挡得住子 agent 重跑（实测：用户批准草稿 v1、执行的是 v2）。
拿回的是**经过存档往返的那一份**：`any` 类型的标注值变成 JSON 对应的类型（数字成 `float64`、切片成 `[]any`……），
几个事件共享同一条消息的关系不保留——读标注按 JSON 类型读。拿回时被调方不执行，它的
`OnAgentStart` / `OnAgentEnd` 也不触发。

**身份与次序**：输入不同的调用之间，身份跟调用的次序无关——按进度跳过已完成的调用、条件分支插进
一次调用都不会让一个答案落到别的调用上，每个问题的答案都留在它自己名下。⚠️ 但**有转交 / 路由信号的
调用别跳过**：那些决定只在「再调一次、被拿回」时交出，跳过就丢了（`err == nil`）。**输入完全相同的几次**（投票、冗余调用）只能
按发起次序区分：那种调用别自己按进度跳过（已完成的框架已经记住，全部照调一遍即可）——跳过其中
一次，后面那次就拿到它的记录；要跳过或重排，给每次一个 key。链式调用（起草 → 审批，审批要问人）
收敛：起草拿回记下的那一版，审批的输入不变、续上自己的存档，用户批的就是自己看过的那一版。

**输入变了就是另一次调用**：父节点重跑时对同一个位置给了不同的输入 ⟹ 新调用、从头跑、自己问
自己的问题，用户对旧输入的回答不会被套到新输入上。代价是输入里带着每次都变的值（请求号、
时间戳、节点体里直接调模型的产出）时，每次重跑都是新调用、永不收敛——用 `WithCallKey` 声明
身份，或用 `SaveStepResult` 把那个会变的值记住。摘要取的是**调用方交来的**那条消息（在被调方
自己的 OnAgentStart 钩子改写之前），按存档往返之后的样子归一——标注表整张、结构化数据块按存档
同样的 JSON 往返（struct 变 map、大整数变 float64，非法 UTF-8 按 JSON 规则替换，含标注键：替换
之后顺序翻转或两个键合并都跟存档一致）——不含消息 ID、响应元数据与每次调模型都新铸的 `run_id`
标注。整体往返不回来时（NaN、函数、`json.Number("1e999")`、嵌套过深……）**逐叶子**走：JSON 形的容器拆开，
往返不回来的叶子按「在哪儿 + 怎么坏的」单独计入，别的叶子照样区分（曾经一个坏值让整个值塌成同一个
标记：父节点重跑时换了工单，工单-1 的批准兑付给工单-2，实测）；结构体按字段展开、键不是字符串的 map
展开、复数记值，只有自定义编码出错的值整体算一个叶子；共享的子容器只走一遍（记一条引用），遍历是
线性的（曾经深 14 的共享结构 20 秒算不完、十万层嵌套 10GB 内存，实测）。
交给被调方的是输入的**独立拷贝**：结构逐层拷，`any` 值**按原类型递归深拷**——切片、数组、map 不管元素
是什么一路拷下去，值结构体逐个拷导出字段（连同嵌入的结构体、含未导出的嵌入类型里的），标量原样，拷完类型不变、nil 与空不混，自己套自己的容器
拷完形状不变；**指针、函数、channel 原样共享**（指针的身份可能就是意义）。被调方的钩子原地改写消息、
标注与这些容器，不会写回调用方的 channel（曾经每次续跑换一个提问 ID、永不收敛，连幂等的脱敏钩子也
多问一次——实测；曾只拷到「元素是标量」那一层，`[]map[string]any` 照样写回、并发扇出时框架读摘要与
兄弟钩子写同一个 map、进程 fatal——实测）；经指针改仍会写回，续跑时换身份重问。曾经把业务值按存档往返一遍（让首跑
看到的形态跟续跑时一样），实测首跑就丢东西：`int64` 超过 2^53 丢精度、`json.Number` 成 `float64`、
只有未导出字段的句柄与 error 成空 map，业务 MarshalJSON 的 panic 直接冒出去。

**产出不合格要重来**：完成记录的语义是「这件事已经做完」。`RunWithRetry` 重试的是**同一次调用**，
节点返错之后 Resume 重试的是**失败的那部分**——两者都会原样拿回已完成的调用，所以「拿到产出、
校验不合格就重来」别靠它们（重试预算全部白花）。要一份新产出：在节点里循环调（同一份输入的第 k
次就是第 k 次调用、各自执行），或把校验放到条件边上声明 `CondFailRerunFrom`，并给产出那一次调用
声明 `agent_v2.WithRerunnable()`——它的完成记录按 `compose_v2.Rerunnable` 记，条件边**否决**时作废、
重跑时重新执行（§5）。重做的是**整次调用**：身份里带上调用所在分域的重做代际，存档槽、被调方在它
自己的图之外记下的东西（它的 agent 级钩子、业务自研 agent 在自己的 Run 里发起的嵌套调用、它的图的
入口条件边）、它问的问题都换新——不然被调方是业务自研 agent 时，外面这次重新执行了、里面真正干活的
那次却原样拿回（实测）。只有父节点直接发起的那一层点名才算数：外层调用是普通记录时整次拿回，里层
的声明走不到。没点名的调用照常拿回：框架分不出哪次是「被判定的产出」、哪次是「顺带做成的事」
（扣款、问过人的把关），一律作废就是副作用重放（实测扣两次款）。条件边被外面停下（软取消、陈旧
写回、调用方 ctx 已经死了——错误链上有没有 ctx 错都一样）不算否决。节点重做时问的问题是新问题（提问的
身份带上重做代际；节点里跑的嵌套图里的提问带上外层代际链）：调用方累积传历次答案时，对 v1 的批准不会
兑付给 v2。

**带 key 的调用**身份就是（位置, key）：输入变了照样认作同一次调用——没完成的续存档（引擎打告警，
`compose_v2.WithInputDigest` 只作核对）、已完成的拿回记录（拿回那一步打告警）。同一次执行（节点
执行 / 一个 tool_call / 一次钩子调用）里、同一个位置上重复用一个 key 报 `ErrRunAborted`
（`RunWithRetry` 的重试不算两次；手写的循环每次都算新调用，带同一个 key 就会撞上）。

**撞槽**：槽号只是身份的哈希。不带 key 的调用把输入摘要记进被调方的存档（`Checkpoint.InputDigest`，
经 `compose_v2.WithInputIdentity`），auto-load 时对不上只可能是另一次输入不同的调用撞进了同一个槽
⟹ 拒绝（不可重试、**不清档**——槽里那份属于另一次调用，清掉它是替别人做了决定），不续别人的进度。
输入相同、只是位置或次序不同的两次调用撞槽时摘要一样、看不出来，靠 63 位槽号让碰撞可以忽略。

**钩子相位**：`OnNodeStart` / `OnNodeEnd` / `OnNodeEvent` / 条件边里发起的调用记在那个节点这次执行
名下（各相位分域），嵌套图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边里的记在承载它的外层节点执行
名下——节点重跑、钩子再触发时拿回，不重问（只答当前一问的前端下，重问会让两个问题轮流出现、永远走
不完）。同一次外层执行里对同一个 ns 跑几遍嵌套图时，图级相位按「第几遍」分开记（只按 ns 分的话第二遍
拿回第一遍的记录）——所以每一遍都照跑、别按进度跳过（跳过前一遍，后一遍的编号前移、拿回前一遍的
记录）；嵌套图亲口声明 `WithFreshRun` 时图级相位什么也不续、不记。只有顶层图的图级钩子
与入口条件边没有节点执行可挂：那里完成的调用在重跑时再执行一遍（没完成的照样续自己的存档）。次序
计数器每次钩子调用各自从 0 数（`OnNodeEvent` 是这次节点执行的所有事件共用一个）。

**被调方属于调用方的任务**：**任务的身份是 session 的 RunID**（ctx 上的 sid 只是它的副本，`WithSession`
在 RunID 非空时把它推成 RunID）。嵌套调用的执行器（`RunSubAgent` / `AgentAsTool` / skill fork）给被调方
一个 RunID 就是**所在任务**的 session（所在任务：节点执行范围记下的图 sid，其次任务章；直接跑
`compose_v2` 图、ctx 里另推了一个 session 时取图的 sid——跟着 session 走的话每个请求换一个 RunID、被调方
续不上还留孤儿存档，实测），并替它盖任务章。被调方的编排入口据此不声明新任务：不推「新开」信号、续
自己的存档、不清审批规则——调用方是九个编排之一、业务自研的 agent、框架外业务自己的循环，还是直接跑
`compose_v2` 图时的图级钩子都一样。

**在一个正在执行的任务里 session 必须属于这个任务**：业务自己 `inner.Run(ctx, s, in)` 时 s 的 RunID 对不上
所在任务 ⟹ 入口拒（`%w ErrRunAborted`，任何执行之前）；RunID 为空的字面量 session 也拒（顶层不 wrap，
什么都还没发生）；Resume 在任务里一律拒，`sessionID` 与 `WithResumeSession` 的 RunID 对不上也拒（放行时一个
任务被劈成两半：嵌套层按另一个 RunID 找存档、从头重放副作用，成功后按它清子树把另一个任务挂着的存档
删掉，实测 `err=nil`）。**开独立任务**（给另一个会话、续跑另一个挂起的任务）先 `agent_v2.DetachTask(ctx)`：
框架不从「RunID 不同」去猜——当新任务读，`inner.Run(ctx, NewRunSession(), in)` 每次父节点重跑都新开一个、
内层问两个问题就永远答不完（实测活锁、每轮一份孤儿存档）；当同一个任务读，拿别的任务的 ctx 给另一个
会话开任务又静默并进去。脱离之后 session / Resume 的 RunID 不能是这一路脱离出来之前所在的任何一个仍在执行的
任务（拒：那是在它自己的执行里再开一遍它——清掉它的审批规则、成功收尾时删掉它挂着的兄弟存档，实测；经 DetachTask
还是直接用 `compose_v2.DetachExecution`、脱离几层都算，见 `compose_v2.DetachedFrom`；「仍在执行」按任务判：任务挂起
之后用当时在节点里存下的 ctx 脱离再续跑它不算，它又续跑起来之后再拿那个 ctx 脱离照样算）；独立任务挂起时，它的中断**不能**
从节点返回——引擎终局拒收（答案送不进去，续跑时它从头再问、永不收敛，实测；原样返回、取出来返回、再包一层都
一样），记下它的 RunID 在顶层续。
DetachTask 连会话 ID 与软取消信号一起剥。入口拒绝恒不可重试。业务在自己的循环里开新任务换 RunID（`sess.NextTask()`），长寿 ctx 上钉着的旧 sid
不碍事。approve_always 规则按 RunID 分，换了 RunID 就是新的一套。

### 4.3 GraphID 防串台

同一个 sessionID 跑两个不同的 Graph 时，两份 cp 都写 `(sid, "")` 会后写覆盖前写。`cp.GraphID`
由框架填（`g.ID()`），`SaveNS` 检测到不一致返 `ErrGraphIDConflict` fail-fast。确需换 graph 用
`WithFreshRun()`。

判冲突以存档里最新那份 GraphID 为准：存档的与本次的都非空且不同才冲突，这份档不动；否则整份
替换，GraphID 跟着换（换成空也照换）。Run / Resume 入口的 fail-fast 读的也是这一份，所以两边对
同一份档给出同一个答案。判定与写入是一个原子步骤：两个带不同 GraphID 的写入同时打同一
(sid, ns) 时恰好一个成功——新键与已有的行都如此（已有的行上没有「插入」替两个写入者排队，全靠
实现先锁住那一行再判）。自研后端把 GraphID 另存一列时，那一列只能是存档的副本、每次与存档
一起写（conformance 的 `SaveNS_GraphID_Follows_Latest_Accepted_Write`、
`Concurrent_SaveNS_Conflicting_GraphID_Exactly_One_Wins` 与
`Concurrent_SaveNS_Conflicting_GraphID_On_Existing_Row_Exactly_One_Wins` 钉这几条）。

## 5. 节点私有中断态（NodeStates）

**解决什么**：节点是原子的——任一部分失败（含抛中断）整节点 writes 作废、Resume 从头重跑。对
「一个节点内做了 N 件带副作用的事」这个形状，意味着已经做成的那几件会被重做。典型是 BasicAgent
的 tools 节点：一轮 N 个 tool_call，其中一个抛中断，Resume 时下单 / 发消息 / 扣款各来第二遍。

```go
func init() { compose_v2.RegisterValueType[map[string]string]("myapp.done") } // 值的类型先注册

done, _ := compose_v2.GetNodeState[map[string]string](ctx)
if done == nil { done = map[string]string{} }
for _, item := range items {
    if _, ok := done[item.ID]; ok { continue }   // 上次已做完，跳过
    out, err := doSideEffect(item)
    if err != nil { return nil, err }
    done[item.ID] = out
    if err := compose_v2.SaveNodeState(ctx, done); err != nil { // 每做完一件就记，中断也不丢
        return nil, err
    }
}
```

⚠️ **每件事里要问人时，每件的提问放进它自己的相位里问**（`compose_v2.EnterPhase(ctx, "confirm:"+item.ID)`）。
在节点体里问的话提问按发起次序编号，跳过做完的那件会让后面的提问挪到它的 ID 上：调用方累积传答案时，
后面那一问拿到的是做完那件的答案，`err == nil`；只传当前那条答案时后面那一问被再问一遍（两件都问人时实测）。
被跳过的那件问过人、后面还有别的提问就会中。按 key 记的 `SaveStepResult` 同理：被跳过的那一步里问过人，那一问
也放进自己的相位。见 `compose_v2.Ask`「按同样的次序问」。

键是 `(NodeID, InstanceID, Visit)`，由框架在节点入口算**一次**、读写共用。

⚠️ **值的类型要先 `RegisterValueType`**（与 channel value 同一套 tagged JSON 协议）——没注册时
`SaveNodeState` 返 error、什么也没记下。**返回的 error 别丢**：丢掉它就是「以为记了进度，重跑时照样
重放」。（框架只注册了自己用到的几个类型，`map[string]string` 这类业务类型得自己注册。）

**按 key 分条**：`SaveStepResult(ctx, key, v, opts...)` / `GetStepResult[T](ctx, key)` 跟
`SaveNodeState` 同一载体、同一生命周期，区别只在按 key 分条——节点里有几件互不相干的事要各自记住
时，不必攒进一个值整体读改写。`agent_v2.RunSubAgent` 用它记已完成的嵌套调用（§4.2）。存档里它是
`NodeStates` 的另一条派生键 `<归属前缀>:step:<key 的哈希>`（可重做的那种是
`<归属前缀>:redo<代际>:<key 的哈希>`；归属前缀是节点执行键，分域里记的再加分域哈希）：业务 key 先
哈希再入档，key 里有 NUL、
非法 UTF-8 也经得起任何后端的 JSON 往返（曾用 NUL 作分隔符，PostgreSQL 的 JSONB 拒收，带步骤记录
的存档一份都写不进去——实测）。

**可重做的记录**（`compose_v2.Rerunnable()`）：记的是这一步的**产出**而不是做过的**事**。被**否决**、
要求重新产出时这类记录作废，重跑时这一步重新做。重做代际**按分域记**：节点的条件出边按
`CondFailRerunFrom` 否决 ⟹ 整次执行换代；它里面跑的嵌套图的入口条件边否决 ⟹ 只让产出那张图输入的分域
（节点体 = 整次执行、某个 tool_call、某个钩子相位；经 `RunSubAgent` / AgentAsTool 调起的 agent 的主图，
输入算调用方那个分域产的）换代，而且等成员以这个错失败时才推进——错被业务吞掉、被重试消化、被转成工具的
失败结果就不推进；标记没随错误传上来（业务 `%v` 包断了链、扇出只报一个错）而成员以非控制流的错失败时，
这次执行里记过的否决照样推进（当场推进整次执行时兄弟 tool_call 挂着的提问换 ID 重问、挂起的
可重做调用换槽重来，实测）。节点自己判定一格处理过的产出作废时用 `compose_v2.AdvanceRerunGeneration(ctx)` 推进 ctx 所在
那一格（效果同条件边否决那一格；节点体里调推进整次执行），只在这次执行随后以失败退出时调——BasicAgent 作废 OnModelEnd
处理过的答复、重新调模型时用它，对新答复的提问换新 ID、那一格的普通记录照常拿回。一个分域的有效代际 = 执行级 + 分域链上每一层之和（交出去的那一段——`HandOver` 封的那一格与它下面的
分域——只算交接那一层往下的）；同一个分域里同一步只导出代际最大的那条（更旧的一定过期），普通的可重做
记录比执行级代际还旧的也不导出（曾经分域级否决每发生一次存档就多带一份整条产出，实测线性增长）。交接那
一格里的记录（键里是 `:hredo`）不按执行级判：它的代际可以比执行级小、仍读得到——按执行级丢过，被调方在图外
记下的方案掉出存档、续跑时重新产出，对旧方案的批准兑付给了新方案（实测）。条件边被外面停下（软取消、陈旧写回、调用方
ctx 已经死了）不算否决，中断、失败之后的续跑也照常拿回。副作用记录（扣款、发消息）别标它——那种
正该在重跑时拦住重放。嵌套调用的完成记录默认是普通记录，调用方用 `agent_v2.WithRerunnable()` 点名的
才按它记。

**分域**：同一次节点执行名下还有几块互不相干的地方，各有自己的一格（`SaveNodeState` /
`GetNodeState`）和自己的步骤记录：

- **工具函数里**按 tool_call 分域（`WithCurrentToolCallContext` 派生；BasicAgent 另按工具名再分一层）：
  并发的 tool_call 各记各的，也碰不到 tools 节点自己那一格——它记着哪些 tool_call 已经做完，没分域时
  工具里一次 `SaveNodeState` 就把那份账覆盖了、已执行过的工具在续跑时再执行一遍（实测）。工具名那一层
  让不同工具的记录互不相认：`OnToolStart` 能改写工具名，但改写只在这一批第一次执行时生效、续跑沿用定下的
  内容（`fixToolCallContent`），同一个 tool_call 正常情况下不会两次执行之间换工具；这一维让「换了工具就不认旧
  记录」不依赖那份记录；BasicAgent 的**工具钩子**（`OnToolStart` /
  `OnToolEnd`）每次调用是一个相位（`compose_v2.EnterPhase`，按钩子名 + 工具名 + 所属 tool_call 命名）：自己的
  一格、自己的提问与嵌套调用计数（两个 tool_call 的钩子带同一个 call key 调同一个 agent 不再撞），采纳钩子
  返回的 ctx 时框架把节点体那一套连同其余执行状态取回来（`compose_v2.CallHook`）；
- **图的钩子相位与条件边**（`OnNodeStart` / `OnNodeEnd` / `OnNodeEvent` / cond）记在那个节点这次
  执行名下、按相位分域；嵌套图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边记在承载它的外层节点
  执行名下；
- **交出去的一段执行**（`compose_v2.HandOver`）：嵌套调用的被调方在自己的图之外还会跑它的
  agent 级钩子，那时 ctx 里是调用方节点的访问器——交接之后它有自己的分域、自己的提问与嵌套调用计数、自己的
  相位名，碰不到调用方节点体那一格（没封口时一次 `SaveNodeState` 覆盖调用方的进度，实测；共用计数时它问一次
  就占掉调用方一个序号，调用被拿回之后调用方后面那一问换 ID 重问，实测）。编排派发成员、内部决策 LLM、转交
  候选同样交接。交接还记下「输入是调用方产的」：被调方主图（落在交接 ns 上）的入口否决作废的是调用方的产出。

节点外、顶层图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边里调是 no-op（记不下、读恒为没有）。

**落盘时机**：记下的东西随这个节点**以没成的结局退出时**的那份存档落盘（中断、失败、取消）；
durability（sync / async）在超步边界写的存档不带它（那一刻这个节点还没开始或已经结束，投影只导出
frontier 上节点当前执行的条目）——进程在节点执行中途崩掉时，已经记下的跟着丢，崩溃恢复后那一步
会再做一遍，配什么 durability 都一样。扛这种崩溃只能靠副作用自己的幂等键。

**两条设计不变量别改**：

1. **不设清除点**——visit 只在成功路径递增 ⟹ 节点成功后旧 state 自然不可达，不需要谁去删它。
   代价是记下的值在本次 Run 结束前一直留在内存里（中断 / 失败时随存档落盘）：嵌套调用的完成
   记录存的是整组事件（AgentAsTool 只存最终那条消息），被调方产出很大、父节点里又调很多次时，内存与那份存档跟着变大（记录在
   本次 Run 结束前都不释放，即使那次节点执行早已成功、它们已经不可达）。
2. **不向上冒泡**——写进本层 cm ⟹ 只落本层 cp。对比 `PendingInterrupts` 父子各存一份：那半是
   给调用方看的，这半是节点自用的。

⚠️ `GetNodeState` 的 `ok==true` **不保证值非 nil**（T 是 map/slice/指针时 round-trip 回来是
typed nil），上面范式里的 `done == nil` 判断不是多余的守卫。

## 6. 并发保护：陈旧写回检测

同一个 sid 被并发 Run/Resume 时，双方各自载入同一份 cp、各自执行、各自写回 → 后写者静默覆盖先
写者，**且两边都 `err == nil`**。`WithStaleWriteGuard()` / `WithStaleWriteGuardCtx(ctx)` 开启后，
写/删 cp 前比对 `Epoch`，被推进过则返 `ErrCheckpointStale`。

**它明确不解决的**（别当成并发安全开关）：

- **不阻止副作用双跑**——检测发生在写回时，那时两边都已经执行完了
- **非原子**——read-then-write 之间有窗口
- **每层各判各的**——子层基线是它刚读到那一代，只覆盖本层的读-写窗口
- **只管 cp**——`store.Store` 的写在图内、早于判定
- **不管带「新开」信号的 Run**——新开的 Run 没有世代基线（agent_v2 顶层 Run 恒是），守卫要等本次首写之后才生效，而首写本身不校验；于是默认 exit 档下，新开的 Run 全程不受保护——成功清理时还会把槽里别人的挂起档一并删掉；sync / async 档从本次第二次写入起受保护（每次写成功都会把基线推到自己写的那一代）。两个新开 Run 并发撞同一 sid——那本身违反「一枚 RunID 只服务一个任务」的契约（见 `RunSession.RunID` godoc）

**默认关闭。** `agent_v2` 的 9 编排下只能用 ctx 那个（编排内部自己拼 RunOption、不透传业务的），
嵌套子图靠 ctx 继承。真正的解在调用方串行化：会话亲和路由 / 分布式锁 / 队列。这道守卫只把最坏的
「静默」变成「可见」。

## 7. Resume 协议

### 7.1 两条路

底层 `compose_v2.Run` **没有独立 Resume 入口**：

- **auto-load**：给了 `WithCheckpointer` + 同 sessionID → 入口 `LoadNS` 命中就续、没有就拿
  `WithInitialWrites` 的种子从头跑。**判据是 `Checkpoint.HasResumableWork()`——按内容判**
  （`len(CurrentFrontier) > 0 || len(PendingInterrupts) > 0`），**不看档位**。
- **`WithResumeFrom(cp)`**：调用方自己把 cp 递进来（跨 sid、或想指定某份特定存档时用）。

⚠️ **别再按「只捡 Pending / 续 Failed 档只能走 `WithResumeFrom`」理解**——那是 2026-08-13
撤掉档位 gate 之前的行为（§3.1 已记）。现在**失败现场在 9 编排上直接续得上**：同 sid 再跑一次
就是从失败点重试，上游已成功的成员不重放。`ListPending` 仍只返 `Pending`，但那个 API 的语义是
「谁在等人回答」，不是「哪些档能续」。

**答案怎么递进去，两层各有入口**：底层 `compose_v2.WithResumeAnswers(map[string]*schema.Message)`
是 RunOption，直接给 `Run`；高层 `agent_v2.WithResumeUserInputs(...)` 给 `Agent.Resume`，封装层
转成前者。两者的 key 都是 `InterruptID`。

高层 `agent_v2.Agent.Resume(ctx, sid, ...)` 封装了 LoadNS + WithResumeFrom + 注入答案，走的是
auto-load 判据。**前提是 ctx 里有 checkpointer**——没有的话更早就在入口被拒（`resume requires
compose_v2.Checkpointer in ctx`，普通 error 非哨兵）。有 checkpointer 但拿不到挂起存档时，统一返
`ErrNoPendingCheckpoint`（**两种**处境返它：从没跑过 / 已被清）。⚠️ 「那份是失败现场」**不在其列**
——Failed 档 `HasResumableWork()` 为真时照常续跑（闸门不读 Status），实测从失败点续、成功兄弟不重放。

⚠️ 入口默认档是「**恢复否则新开**」。种子（`WithInitialWrites`）**只在新开路径 apply**，所以
「有种子 + 存档命中」是**正常形态**（崩溃恢复就长这样），引擎不会因此报错。想开新任务传
`WithFreshRun()`。

### 7.2 配对答案

`ResumeContext` 经 ctx 透传，节点**按自己的 InterruptID 精确取，这是唯一取法**（「取任意一条」
的读法已删，拿到别人的答案现在结构上不可能）。业务提问统一走 `compose_v2.Ask`——它算 ID、查答案、
没有就抛中断，是同一次调用里完成的闭环。

> `InterruptError` 字段 / `AllQueries()` / `MergeInterrupts` / `ResumeAction` 三模式与支持矩阵 /
> 业务完整配对流程 → [hitl-resume.md](hitl-resume.md)，本文不重复。

### 7.3 嵌套 Resume 的路由

不需要专门的"是不是我"判断函数：`Run` 入口按 `(sessionID, ns)` 寻址 `LoadNS` 拿对应 ns 的 cp，
frontier 自然从 `cp.CurrentFrontier` 开始；嵌套时每层各自 `LoadNS`，自然 drill-down。

ResumeContext 默认全 ctx 子树可见（适合"父节点直接是子 agent"）；
`WithSubgraphIsolatedResumeContext()` opt-in 让子图独立处理自己的 Resume。

### 7.4 Resume 恢复的是 state，不是 event

`Checkpoint` **没有 Events 字段**。`cm.Restore(cp)` 恢复 channel state + frontier +
PendingInterrupts，从 frontier 续跑，**不回放 event**。前端 SSE 断线重连要业务自己持久化流
（如 Redis Stream）+ 按 last-event-id 续传。

### 7.5 防御机制

- 给了答案但 ID 跟 cp 里的 `PendingInterrupts` 对不上 → `slog.Warn` 不报错（可能是有意的 partial answer）
- 给了答案但拿不到可续的档 → **分两种处境**：配了 checkpointer + sessionID 时
  **fail-fast** 返 `%w ErrNoPendingCheckpoint`（正文带 sid / ns / 答案 ID / 没捡到的原因），
  图体一步不执行；没配 checkpointer 或 sessionID 时仍是 warn + 照跑首次执行路径
- `LoadNS` 反序列化时业务 `UnmarshalJSON` panic → recover 转 error 且**不污染 cm**（atomic 回滚）
- fan-out 多分支同时中断 → 收集**所有**分支的 PendingInterrupts

## 8. 优雅停止

`SoftCancelSignal` 让外部在另一个 goroutine `Trigger(reason)`，Run 在**超步边界**优雅停：当前
超步先跑完，存档可 Resume，返 `ErrSoftCanceled`。

跟 `ctx.Done()` 硬停的区别是**当前超步善终不丢**——硬停掐正在跑的超步、进度回滚到上个超步边界，
但出口**同样存 Pending cp 可 Resume**（不是"不存档"；出口写档不跟着调用方取消，持久后端上同样存得下，见 §3.5）。信号一次性（每 Run 建新的），末超步
Trigger 是 no-op。

## 9. 关联文档

| 想知道 | 去哪 |
|---|---|
| HITL 业务怎么写（中断、配对、三模式）| [hitl-resume.md](hitl-resume.md) |
| 图执行模型本身（BSP、Channel、fan-out）| [channel-engine.md](channel-engine.md) |
| cp 在 6 层 state 里的位置 | [store.md §1](store.md) |
| 跨 Run 的会话历史 | [ISSUE-98](../issue/issue-98-conversation-history.md) + [usage-guide.md](usage-guide.md) |
| 三方对照 / 历次决策记录 / 开放问题 | [checkpoint-storage.md](../description/three-way-comparison/checkpoint-storage.md) |
| 字段级细节与「漏了会怎样」 | godoc `compose_v2/checkpointer.go`、`frontier.go`、`node_state.go` |
