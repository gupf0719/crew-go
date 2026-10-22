# HITL（Human-In-The-Loop）+ Resume 完整协议

> 节点抛 `InterruptError` 暂停 graph → 框架自动 SaveNS cp → 调用方拿用户答案 → Resume
> 续跑。**错误作为控制流（Error-as-Control-Flow）模式**——HITL 不是单独子系统，是
> channel-based 引擎的一等公民。

> **文档定位**：HITL + Resume 完整协议详细文档。**业务接 ask_user / 用户暂停按钮 /
> 跨进程 Resume 必读**。cp 与 Resume 的引擎层设计见 [`checkpoint.md`](./checkpoint.md)。

## 目录

1. [HITL 是什么 + 四类触发场景](#1-hitl-是什么--四类触发场景)
2. [InterruptError 完整协议](#2-interrupterror-完整协议)
3. [AskQuery + InterruptID 稳定寻址](#3-askquery--interruptid-稳定寻址)
4. [ResumeContext + 取答案的业务侧 API](#4-resumecontext--取答案的业务侧-api)
5. [ResumeAction 三模式（Continue / Replan / Abort）](#5-resumeaction-三模式continue--replan--abort)
6. [多 tool_call 部分中断处理](#6-多-tool_call-部分中断处理)
7. [Fan-out 多分支并发中断](#7-fan-out-多分支并发中断)
8. [嵌套子图 ResumeContext 隔离 opt-in](#8-嵌套子图-resumecontext-隔离-opt-in)
9. [mid-stream Interrupt + reset_stream 协调](#9-mid-stream-interrupt--reset_stream-协调)
10. [9 编排 HITL 矩阵 + nested_smoke 12 场景](#10-9-编排-hitl-矩阵--nested_smoke-12-场景)
11. [plan_chat_web 实战：用户暂停按钮 + Resume Continue](#11-plan_chat_web-实战用户暂停按钮--resume-continue)
12. [BasicAgent preamble HITL（ISSUE-74 修）](#12-basicagent-preamble-hitlissue-74-修)
13. [反模式 + 常见误解](#13-反模式--常见误解)

---

## 1. HITL 是什么 + 四类触发场景

HITL 全称 **Human-In-The-Loop**——agent 跑到某一步需要 user 决策才能继续，挂起 +
持久化 + 等用户答复 + 续跑。

### 1.1 四类触发场景

| 场景 | 谁抛 InterruptError | 典型用途 |
|---|---|---|
| **A. 业务工具 ask_user** | `agent_v2.NewAskUserTool()` 内置工具（业务从 toolkit 调用）| LLM 主动问用户：要不要继续 / 选 A 还是 B / 输入参数 |
| **B. Hook 主动暂停** | 业务自定义 Hook（`OnAgentStart` + `OnModelEnd` 双触发；**顶层 agent 靠 OnModelEnd 产 cp**——OnAgentStart 在 graph 启动前抛不产 cp，此时对该 sid `Resume` 会返 `errors.Is(err, compose_v2.ErrNoPendingCheckpoint)`（R33 GA-M2 fail-fast，旧行为是误导性内部错/垃圾 final），详 §11.1/§11.3）| 用户暂停按钮 / 配额耗尽 / 政策审查 |
| **C. ctx 超时被动** | ctx.DeadlineExceeded（不是真 HITL，但走类似 cp 路径）| 长任务被外部 cancel，希望 Resume 时续跑而非重做 |
| **D. 声明式断点** | 引擎在超步边界上停（`GraphBuilder.WithInterruptBefore/After`、RunOption、GraphAgent `WithGraphInterruptBefore/After`），Reason=`breakpoint`、`AskQuery.Breakpoint` 写停在哪 | 调试 / 逐步执行 / 「到这儿先停一下」的位置闸门；续跑（带不带答案都行）就是放行，详见 [execution-control.md](execution-control.md) §1 |

A/B 是真 HITL（user 提供答案），C 是被动恢复（无 user 答案），D 是位置闸门（不带问题，续跑即放行）。

### 1.2 完整 HITL 时序

```
┌──────────────────────────────────────────────────────────────────┐
│ 1. agent.Run(ctx, sess, input)                                   │
│      ↓                                                            │
│ 2. graph 跑到某节点（model / tools / 业务自定义）                │
│      ↓                                                            │
│ 3. 节点抛 *compose_v2.InterruptError{Query: AskQuery}            │
│      ↓                                                            │
│ 4. 框架收 PendingInterrupts → SaveNS(cp.Status=Pending)           │
│      ↓                                                            │
│ 5. agent.Run 返回 InterruptError 给调用方                         │
│      ↓                                                            │
│ 6. 调用方 compose_v2.IsInterrupt(err) → 走 ie.AllQueries()       │
│      ↓                                                            │
│ 7. 调用方 for q := range ie.AllQueries(): 问用户 q.Question      │
│      ↓                                                            │
│ 8. 用户答复 → 调用方 agent.Resume(ctx, sess.RunID,                │
│       agent_v2.WithResumeUserInputs(map[string]*Message{          │
│           q.InterruptID: schema.UserMessage("yes"),               │
│       }))  // 多 query 时循环累积 answers                          │
│      ↓                                                            │
│ 9. 框架注入 ResumeContext.UserInputs → 再 LoadNS 恢复 cm（注入在前）│
│      ↓                                                            │
│10. 重跑被中断的节点 → 业务调 Ask(ctx, q) 拿答案（身份框架派生）   │
│      ↓                                                            │
│11. 节点 return 正常 ChannelWrite → graph 继续跑                  │
│      ↓                                                            │
│12. 跑完 → defer DeleteSubtree 清 cp                              │
└──────────────────────────────────────────────────────────────────┘
```

### 1.3 关键不变量

- HITL 通过 `InterruptError`（**错误作为控制流**）触发——节点 return error，框架识别。
- `InterruptError.AllQueries()` **严格非空**（v2 fail-fast）——单 query 用 Query 字段，
  多 query 用 Queries 字段（ISSUE-81 toolsFn fanout 多 tc 同时抛 Interrupt 用，与 Query
  互斥 Queries 优先）。业务"暂停按钮"等无真 question 的场景要填虚拟 Query（典型：
  "已挂起，请点继续或换方向。"）。framework 验 `len(AllQueries())==0` fail-fast。
- cp 物理写到 `(sessionID, graphNS)`——嵌套场景 user 答案按 InterruptID 配对，不会因
  ns 不同串台。
- `cp.PendingInterrupts` 含本超步所有挂起的 AskQuery（多分支并发 + toolsFn fanout 多 tc
  时多个，框架走 `AllQueries` 协议 record 全部）。

---

## 2. InterruptError 完整协议

### 2.1 InterruptError 结构

```go
// compose_v2/interrupt.go
type InterruptError struct {
    Reason  string        // 业务自定义原因（如 "ask_user" / "user requested pause"）
    Query   *AskQuery     // 单 query 模式 —— 与 Queries 互斥
    Queries []*AskQuery   // 多 query 模式（ISSUE-81 fanout）—— 与 Query 互斥；Queries 非空时优先
    Data    any           // 业务自定义元数据——**仅同进程透传**（cp 只持久化 *AskQuery、**含其 `Reason` 字段**；不含 `InterruptError` 顶层的 Reason/Data）；跨进程 Resume 业务侧需自管 Data 持久化
}

func (e *InterruptError) Error() string                 // human-readable 错误信息
func (e *InterruptError) AllQueries() []*AskQuery       // 统一 accessor——业务用此 helper 取全部 query 不区分单/多模式
```

**关键字段**：

| 字段 | 必填 | 用途 |
|---|---|---|
| `Reason` | 否 | 业务自定义标识（hook 可按 reason 路由不同处理）。**多 query 模式聚合**：`MergeInterrupts(ies)` helper 聚合 N 个 InterruptError 时 `Reason` 被覆盖为聚合字符串 (实际中文字面量, 含聚合数量 + "ie.Reason 已拷贝到 query.Reason 见 godoc" 提示; 按 `compose_v2/interrupt.go::MergeInterrupts` 符号自查) — 原各 ie 的 `Reason` **自动 propagate 到 q.Reason** (audit #5 视角 8 F3 fix), per-query 语义不丢失 |
| `Query` | 单/多互斥 | **单 query 模式**——99% 场景填 Query 即可（含 InterruptID + Question + Options + Reason）|
| `Queries` | 单/多互斥 | **多 query 模式**——ISSUE-81 toolsFn fanout 多 tc 同时抛 Interrupt 用；与 Query 互斥，Queries 非空时优先 |
| `Data` | 否 | 业务自定义 metadata（如 active agent 名 / round 号 / hint）—— **仅同进程透传**，cp 只存 *AskQuery 不含 Data。**多 query 场景**：业务想区分 per-query Reason 用 `q.Reason` 字段 (audit #5 视角 8 F3 加, ie.Reason 自动 propagate 到 q.Reason); 想跨 ie 边界保 typed metadata 走 `store.Store` 用 InterruptID 作 key |

**业务统一用 `AllQueries()` accessor**（不要直接读 Query / Queries 字段——避免漏处理多模式）：

```go
if ie, ok := compose_v2.IsInterrupt(err); ok {
    for _, q := range ie.AllQueries() {        // ← 单/多模式统一
        answer := askUser(q.Question)
        answers[q.InterruptID] = schema.UserMessage(answer)
    }
    events, err = agent.Resume(ctx, sess.RunID,
        agent_v2.WithResumeUserInputs(answers))
}
```

**`AllQueries()` 严格非空**（v2 fail-fast）：framework 在 **node 路径**不允许 `len(AllQueries())==0`——业务"暂停按钮"等无真 question 的场景必须填**虚拟 Query**（典型："已挂起，请点继续或换方向。"）。详见 §6 多 tool_call 部分中断。（**cond fn 路径不适用**——cond 禁止 interrupt，抛任何 InterruptError 都被拒，见 §10.1 末。）

### 2.2 IsInterrupt + asInterrupt 判定 helper

```go
// compose_v2/interrupt.go
func IsInterrupt(err error) (*InterruptError, bool)
```

**穿透错误链**（对齐 `errors.As` 语义）：`IsInterrupt` 内部 `asInterrupt` 同时走**单链 `Unwrap() error`** 与**多错误树 `Unwrap() []error`**（`errors.Join`，Go 1.20+），DFS 从左到右第一个命中即返。所以业务可放心用 `errors.Join(interruptErr, bizErr)` 组合错误——`IsInterrupt` 仍能穿透检测到中断。这不只是业务便利：framework 内部 graph_loop 部分中断 + 同超步 `ApplyAll` 失败正是走 `errors.Join(res.exitErr(), applyErr)`，**漏 `[]error` 分支会把 HITL 中断误判成致命错、Resume 闭环静默失效**（fan-out 部分分支中断 + 兄弟 reducer 失败时尤其致命）。

业务侧典型用法（**统一走 `AllQueries()` 协议**——单/多模式都安全）：

```go
events, err := agent.Run(ctx, sess, input)
if ie, ok := compose_v2.IsInterrupt(err); ok {
    answers := make(map[string]*schema.Message)
    for _, q := range ie.AllQueries() {  // ← 统一 accessor，fanout 场景不漏 query
        // q.InterruptID / q.Question / ie.Reason / ie.Data
        answer := askUser(q.Question)  // 业务实现
        answers[q.InterruptID] = schema.UserMessage(answer)
    }
    events, err = agent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
}
if err != nil { /* 真错 */ }
// 用 events 拿最终结果
```

> ⚠️ **不要写 `ie.Query.X` 直接读**——99% 单 query 场景能跑，但 ISSUE-81 toolsFn fanout 多 tc 同时抛 Interrupt 时**会漏 N-1 个 query** 让对应 tool_call Resume 续不上。`AllQueries()` 自动覆盖单/多模式（互斥优先 Queries），是唯一安全的 accessor。

### 2.3 框架处理路径

节点抛 `InterruptError` 后，`compose_v2/graph_loop.go` 的处理顺序（步骤标号为教学示意；
`executeFrontier` / `frontierResult.AllInterrupts` / `cm.recordInterruptBatch` / `cm.commitSuperstepPartial` /
`cm.Snapshot` 是真实符号）：

```
1. executeFrontier 收集所有节点的 errs → frontierResult.AllInterrupts 字段直接装填
2. 所有中断分支的 query 合成一批 → cm.recordInterruptBatch(batch)  ← 累加到 cm.PendingInterrupts
   （主循环 `res.FirstErr != nil` 分支**仅此一处**；批内撞 ID 只记事实、到最终 return 处才换成
    `ErrRunAborted`；cond fn 路径已禁 interrupt、不再 record，见 §10.1 末的 cond 限制；
    grep `cm\.recordInterruptBatch` 看实际位置）
2b. cm.commitSuperstepPartial(res.SuccessWrites, res.FailedBranches, res.SucceededBranches, res.ConsumedAnswers, ...)
   ← **一个临界区**里做完：成功兄弟的 writes 提交 + frontier 收窄到失败/中断分支（Resume 只重跑这些）
     + 已兑付答案记账；中断/失败分支自己的 writes **不进 channel**。收窄只在提交**成功之后**落地
     （pre-arm / post-disarm）；提交本身失败则 cm.updateFrontier(frontier) 退回整超步重跑（含成功分支）
3. cp 落盘：默认 `Durability=exit` 时在 Run 出口 defer 内 cm.Snapshot() + cper.SaveNS()；sync / async 档
   另有每超步的写入点（中途档恒 Pending，见 `compose_v2/durability.go`），不在下面这条 defer 路径里
   （`compose_v2/graph_loop.go::Run` defer 内 `rootCp, _ := cm.Snapshot()` + `cfg.checkpointer.SaveNS(...)`；
    grep `cm\.Snapshot\(\)` 看实际位置）
   ├─ cp.Status = CPStatusPending
   ├─ cp.Channels = cm.Snapshot()（per-channel taggedValue）
   ├─ cp.CurrentFrontier = 当前 frontier（不含已完成节点）
   ├─ cp.PendingInterrupts = make+copy(cm.pendingInterrupts)（新 slice 头 + 共享元素 *AskQuery 指针，与 §7.4 一致）；R9 C4 修的是 `cm.PendingInterrupts()` getter 深拷贝防外部 mutate，不在 Snapshot 路径
   ├─ cp.NodeVisits = cm.NodeVisits（跨 Run/Resume 持久化）
   └─ SaveNS(ctx, sid, graphNS, cp)
4. return res.exitErr() 给调用方（**单一 error，不是 multi-error**）
   （`compose_v2/graph_loop.go::Run` 内 `return res.exitErr()` 路径——grep 看实际位置）
```

**关键**：抛 InterruptError 的节点的 `[]ChannelWrite` **不会 apply**——与失败分支同款，避免半完成状态。Resume 时该节点重跑。

### 2.4 多 InterruptError 同超步处理

fan-out 多分支并发各抛 InterruptError 时：
- 每个 ie 都通过 `ie.AllQueries()` 协议 record 到 `cm.PendingInterrupts`
- 单一 cp 含**多个 AskQuery**
- 框架返**单一 `*InterruptError`**（`res.exitErr()`，不是 multi-error）—— 多个分支同时中断时它是 `MergeInterrupts` 合出来的那一个，`ie.AllQueries()` **含本超步全部 query**，跟 cp.PendingInterrupts 的数量一致；不必再 `cper.LoadNS(...)` 去补
- **中断优先于兄弟分支的业务错**：同一超步里 1 个分支中断 + 兄弟分支业务失败时，返回值仍是中断（否则调用方按「Run 失败了」处理不会 Resume，cp 里挂着的问题永远没人答）。被让位的那些业务错不会永久静默——它们在 `FailedBranches` 里，Resume 时一起重跑；要当场看每个失败分支各自的原因，走 `OnNodeEnd(ctx, nodeID, chs, nodeErr)`（每个失败分支各 fire 一次，带分支身份）
- **只有一个分支中断时返的是那条分支的原始 error**，不是抠出来的裸 `*InterruptError`——链上并列挂的第二 sentinel（如 `ErrCheckpointStale`）必须保住，下游 a2a/server、agui、observability 三处都把 stale 判在 `IsInterrupt` 之前
- **toolsFn fanout 多 tc 同时抛 Interrupt**（ISSUE-81）走 `MergeInterrupts(ies)` helper 聚合到单 ie.Queries，graph_loop 走 AllQueries 协议 record 全部 query 不丢

业务 Resume 时一次性传所有 user 答案（map 多 key）：

```go
answers := make(map[string]*schema.Message)
cp, _ := cper.LoadNS(ctx, sess.RunID, "")
for _, q := range cp.PendingInterrupts {
    answers[q.InterruptID] = schema.UserMessage(askUser(q.Question))
}
events, err = agent.Resume(ctx, sess.RunID,
    agent_v2.WithResumeUserInputs(answers))
```

### 2.5 关键不变量

- ✨ **`InterruptError.AllQueries()` 严格非空**（v2 fail-fast）—— 单 query 用 Query 字段；多 query 用 Queries 字段（ISSUE-81 toolsFn fanout 用，与 Query 互斥 Queries 优先）；业务暂停按钮虚拟 Query
- ✨ **`InterruptError.Data` 字段** 业务自定义 metadata（**仅同进程透传**——cp 持久化路径只存 AskQuery 不含 Data, Resume 后 caller 拿到的 ie.Data 是 nil; audit follow-up #5 已修 godoc 警示, 业务跨 Resume 保 typed metadata 走 `RunSession.SetValue/GetValue` 或 `store.Store`）
- ✨ **panic 不算 InterruptError**——`safeNodeRun` recover 转普通 error；Run defer 内仍按 `runErr != nil` 路径 SaveNS（仅 frontier + PendingInterrupts 都空才 skip）。**业务应避免用 panic 表达 HITL**——panic 后 cp 写 Pending 状态，Resume 重跑节点会再次 panic 死循环
- ✨ **抛 InterruptError 的节点 writes 不 apply**——Resume 时重跑

---

## 3. AskQuery + InterruptID 稳定寻址

### 3.1 AskQuery 结构

```go
// compose_v2/interrupt.go
type AskQuery struct {
    Question    string   `json:"question"`               // 给 user 看的问题
    Options     []string `json:"options,omitempty"`      // 可选项；nil = 自由文本
    InterruptID string   `json:"interrupt_id,omitempty"` // 稳定 ID —— **必填**，见 §3.4
    Reason      string   `json:"reason,omitempty"`       // 业务可读的 per-query 中断原因
                                                          // (MergeInterrupts 聚合 N 个 ie 时透传 ie.Reason → query.Reason)
    ToolCallID  string   `json:"tool_call_id,omitempty"` // 可选：这条挂起指向哪次工具调用
    ToolName    string   `json:"tool_name,omitempty"`    // （空 = 纯问答，不指向任何待执行动作）
    Breakpoint  *Breakpoint `json:"breakpoint,omitempty"` // 非 nil = 声明式断点停下的（框架填，Ask 会清掉业务填的）
}
```

**`json:"..."` tag 关键**——AskQuery 整体随 cp 持久化序列化，所有字段必须 JSON-friendly。

### 3.2 身份从执行位置派生

坐标是 `InterruptSite`，`InterruptID()` 把它 hash 成 16 位 hex：

```go
// compose_v2/interrupt.go
type InterruptSite struct {
    GraphNS     string // 嵌套子图位置
    NodeID      string
    InstanceID  string // SendFrom 派生实例的身份
    Visit       int    // 同节点第几次访问
    ToolCallID  string // 模型给的 tool_call ID（可能为空：有的 provider 不回）
    ToolCallIdx int    // 节点内第几个分支（tool_call）
    Idx         int64  // 所在相位自己那个计数器的当前值
    TaskID      string // 任务代号

    InToolCall bool   // 在不在某个 tool_call 里（false 时不计入哈希）
    Phase      string // 所在相位：节点体为空；钩子、交接出去的那一段各有名字（空时不计入）

    RerunGen       int    // 所在分域被条件边否决、重做了几次（0 时不计入哈希）
    OuterRerunGens string // 嵌套图里：承载它的每一层外层执行的重做代际（全 0 时为空、不计入）
}

func (s InterruptSite) InterruptID() string  // fnv64a → 16 hex chars
```

**业务不直接调它**——走 `compose_v2.Ask`（§3.4），ID 在内部算完立刻用。

### 3.3 每一维防的是什么

**少任一维都会 ID 串台**，而串台的症状统一是「用户批的是这件事、系统执行的是另一件」：

| 维度 | 防什么串台 |
|---|---|
| **TaskID** | 同一会话里的**不同任务**（上一个任务被放弃、下一个任务问同一个位置）|
| **GraphNS** | 嵌套子图里同 nodeID 不同位置 |
| **NodeID** | 同 graph 内多节点 |
| **InstanceID** | 同超步同 NodeID 的多个并发实例（`SendFrom` 同 Target 投递多次）|
| **Visit** | 同节点多次访问（Loop / ReAct 多轮 / PlanAgent 循环回 plan）|
| **ToolCallID** | 模型给的 ID——可能为空（有的 provider 不回），所以它不回答「在不在 tool_call 里」|
| **InToolCall** | 「在某个 tool_call 里」与「不在任何 tool_call 里」——ID 为空、下标为 0 时只靠前两维分不开：钩子里的提问与 tool_call #0 里的提问撞成同一个 ID，用户答了前一问、后一问被判「答案已兑付」、整个 Run 终局失败（实测）|
| **Phase** | 相位：图的钩子、业务工具钩子（每次调用一个）、嵌套调用的被调方在自己图外的那一段各有名字（`compose_v2.EnterPhase` / `HandOver`，各有自己的计数器）。被调方在图外提问时 ns 是它的、节点与 tool_call 维却是调用方的——调用方节点恰好叫 "tools" 时跟被调方 tools 节点钩子里的提问撞 ID（实测，不需要空 ID）|
| **ToolCallIdx** | 一轮里第几个 tool_call（节点内并发扇出）|
| **Idx** | 同一个相位里问了第几次（一个工具连问两次）|
| **RerunGen** | 同一次节点执行**被条件边否决之后重做**时在同一个位置问的问题（否决不推进 visit）——没有它的话调用方累积传的「批准 v1」直接兑付给 v2。按分域算：嵌套图的入口否决只让产出那份输入的分域（某个 tool_call、某个钩子相位；经 `RunSubAgent` 调起的 agent 的主图算调用方的）换代，兄弟 tool_call 的提问 ID 不变。被调方在自己的图之外问的（交接出去的那一段，如 a2a RemoteAgent 的 human gate）从交接那一层数起：调用方那一层被否决重做时，身份没变的那次调用里已答的问题不换 ID |
| **OuterRerunGens** | 节点里跑的**嵌套图**里的提问：外层节点被否决、重做时嵌套图从头跑（它上次成功时存档已清），在同一个位置问——嵌套图有自己的 cm，只看 RerunGen 分不开。交接过来的主图（`RunSubAgent` 等调起的 agent）里承载它的那一项是交接那一格的代际，从交接那一层数起、不算调用方那一层：调用的身份已经决定它是不是同一次调用，算上调用方那一层的话，调用方因别的分域被否决而重做时，这次没变的调用里已经答过的问题换 ID 被再问一遍（实测）。调用方所在图之外的各层照带：那几层被否决重做时调用方的图从头跑、这次调用的完成记录跟着没了，被调方重新产出——曾经把整条链清空，对 plan-v1 的批准兑付给了 plan-v2（实测）|

⚠️ **`InstanceID` 与 `Visit` 互斥，不同时编入**：有实例身份时 `Visit` 置 0。
必须如此——`visitCount` 是 **per-NodeID 共享计数器**，成功的兄弟实例会把它推高，
中断实例重跑时读到的值跟中断当时不同 ⟹ ID 漂移（实测 3 实例场景，Run 1 的 `visit=0`
在 Resume 时变成 `visit=2`）。两者各自解决「同一节点的不同待办怎么区分」，场景互补；
同时编入反而让稳的那个被不稳的那个带偏。

### 3.4 业务提问：走 `compose_v2.Ask`

**唯一推荐写法**——算 ID、查答案、抛中断三件事一次做完：

```go
// 业务工具内
ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
if err != nil {
    return nil, err // 中断沿调用链上抛（往上包必须用 %w，否则控制流被降级成普通错误）
}
// 走到这里说明人已经答了，ans 就是答案
```

⚠️ `Ask` **不写回**你传进去的对象——身份在框架自己的记录上。要读 ID 从抛出来的
`InterruptError.Query` 上读。（写回过，后果是复用同一个 `AskQuery` 时第二问带着第一问
的身份、命中已答过的答案、**没问就做了**；审批语境下即静默自动批准。）

#### `InterruptID` 是必填的，框架不替你补

空 ID 会被**当场拒**：`%w ErrRunAborted`（不可重试、存档不留）。同一个 `InterruptError`
里两条 query 用**相同 ID** 同样拒——放过的话第二条会被 `recordInterrupt` 静默丢掉，
调用方看到 2 条、存档里只有 1 条。

这两道校验**所有产生中断的入口共用一份**（节点返回 + `OnNodeStart` hook）。

**为什么不留兜底**（曾经有，已删）：那条兜底读的是**节点级 ctx**，看不见节点内部的分支
结构 ⟹ 分支下标那一维恒 0、序号取合并列表里的位置；而分支自己算的是「本分支计数器的
下一个」。两套坐标只在「一支问了 N 件事」这种最简单形态下碰巧对上。实测（一轮 N 个
tool_call 各自提问、都不自填）：**除第一支外全部答不进去、永不收敛**，后续每轮的新问题
被盖上已经答过的 ID ⟹ 循环里已审批的工具被反复重放。

**也别指望「让扇出方事后替分支盖章」**——试过，实测**比不补还糟**：只要开了工具审批，
审批层自己先 tick 过一次，业务工具拿到的第一个序号就不是 0 ⟹ 全错位；「一个工具问两次」
时第二问被盖上第一问已答过的 ID ⟹ 副作用**无界重放**（实测 16 次仍在涨，不补时只 1 次）。
那条路已删。

根子是：序号那一维是**本分支自己的计数器**，它的当前值**只有正在跑的那段代码知道**——
任何第二处想重建都是猜。`Ask` 把「算 ID」和「用 ID」放进同一次调用，结构上不可能分叉。

⚠️ **`Ask` 不接受自填 `InterruptID`**（2026-08-12）——填了当场拒（返普通 error 不是中断）。
身份的选择权一旦在业务手里，两个**不同的提问**就能落在**同一个答案位**上，实测三种、
全部 `err == nil`：跨超步（用户只见过第一问，第二件事跟着第一件的授权办了）/ 跨 visit
（批一次、之后每轮静默通过）/ 同节点两问。**名字可以业务给，路线只能框架定。**

### 3.5 NextInterruptIndex：业务工具自管 idx

```go
// compose_v2/interrupt.go
func NextInterruptIndex(ctx context.Context) int64
```

ctx 注入的 per-node 单调计数器——节点入口框架自动注入计数器，业务工具每次调用要 ID
时取并 ++ 拿稳定 idx。

**ask_user_tool 完整实现**（v2 内置）：

```go
// agent_v2/ask_user_tool.go::askUserToolFunc
func askUserToolFunc(ctx context.Context, args map[string]any) (*tool.Result, error) {
    question, _ := args["question"].(string)
    options := /* parse args["options"] */

    // 身份与答案**一次取定**：NextAnswerSlot 内部 tick 一次每节点计数器，
    // 返回「本站点下一个提问位」的身份 + 那位上已有的答案。
    // 拆成「先算 ID、再拿它读」两步的话，第二处只能猜那个计数值。
    stableID, ansMsg, ok := compose_v2.NextAnswerSlot(ctx)

    // 二阶段：Resume 后命中 ⟹ 直接返答案（ok 为真时 ansMsg 仍可能是 nil，见其 godoc）
    if ok && ansMsg != nil {
        return tool.Success(ansMsg.Content), nil
    }

    // 首次调用 → 抛 InterruptError，Query.InterruptID 已填稳定 ID
    return nil, &compose_v2.InterruptError{
        Reason: "ask_user",
        Query: &compose_v2.AskQuery{
            Question:    question,
            Options:     options,
            InterruptID: stableID,
        },
    }
}
```

**二阶段语义**：
1. **首次调用**（无 ResumeAnswer）→ 抛 InterruptError 触发 cp save
2. **Resume 后**（NextAnswerSlot 命中）→ 节点重跑，工具直接返答案给 LLM

对齐 LangGraph `interrupt()` 语义：节点 re-run 时直接返 resume value 而非再抛。

### 3.6 自填 InterruptID：只剩「标签」这一种合法用法

2026-08-12 之前这里列了两种「自填覆盖框架自动 ID」的场景，**两种都已不成立**：
跨进程预先约定 ID 精确路由（`Ask` 现在拒）、业务工具内自管 idx（框架自带的
`ask_user` / 工具审批已改走 `NextAnswerSlot`，同一套派生身份）。

**现在唯一合法的自填**是给前端一个自己认得的**标签**，走的是「直接构造
`InterruptError`」那条路——典型是钩子相位的「暂停」按钮，它抛完**不回查答案**
（续跑走 `WithResumeUserText` 那条非寻址通道）：

```go
// 钩子相位：这不是「提问」，是「停一下」——没有答案要取，ID 只作标签
return nil, &compose_v2.InterruptError{
    Query: &compose_v2.AskQuery{Question: "已暂停", InterruptID: "pause"},
}
```

节点里**真的要问人**一律走 `Ask`（身份留空）：

```go
ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "..."})
if err != nil {
    return nil, err // 没答案 ⟹ err 是 InterruptError，原样冒泡
}
```

---

## 4. ResumeContext + 取答案的业务侧 API

### 4.1 ResumeContext 结构

```go
// compose_v2/interrupt.go
type ResumeContext struct {
    UserInputs map[string]*schema.Message  // 按 InterruptID 精确配对
}
```

**audit Round 9 Bug B1 修复**：之前类型 `map[string]string` 只承载文本，agent_v2
conversion 层把 *schema.Message 强转 map[string]string 静默丢 ContentBlocks /
Annotations / Role / Refusal / Metadata 5 个字段（基本可用性破坏）。改为
`map[string]*schema.Message` 端到端保完整字段。

### 4.2 取答案的 API

```go
// compose_v2/interrupt.go

// GetResumeContext 拿整个 ResumeContext
func GetResumeContext(ctx context.Context) (*ResumeContext, bool)

// Ask 是**业务提问的推荐入口**：算身份 → 查答案 → 有就返、没有就抛中断
// （AskQuery.InterruptID 必须留空，填了当场拒）
func Ask(ctx context.Context, q *AskQuery) (*schema.Message, error)

// NextAnswerSlot 取「本站点下一个提问位」的身份 + 那位上已有的答案。
// 给「查答案」和「抛」之间要插逻辑、用不了 Ask 一步到位形态的场景
// （框架自带的 ask_user / 工具审批走的是它；a2a 的 human gate 按位置、远端 taskID 与第几问派生身份，见 a2a.WithHumanGate）
func NextAnswerSlot(ctx context.Context) (id string, ans *schema.Message, ok bool)

// GetResumeAnswer 按任意 InterruptID 取——⚠️ **这是「拿任意 key 去 pull」那条路，
// 实测漏三种（跨超步 / 跨 visit / 同节点两问）**。新代码一律用 Ask。
// 保留它只因 ResumeContext.UserInputs 是公开字段，删掉关不上口子
func GetResumeAnswer(ctx context.Context, interruptID string) (*schema.Message, bool)
func GetResumeAnswerText(ctx context.Context, interruptID string) (string, bool)

// GetResumeUserText 取「用户这轮说了什么」——**非寻址型**负载，不指向任何提问
// ⚠️ 这不是「取答案」的便利写法：节点要拿属于自己的答案走 Ask / NextAnswerSlot
func GetResumeUserText(ctx context.Context) (string, bool)

// WithResumeContextValue 写侧 helper——业务在标准 Run/Resume 入口之外手动注入
// ResumeContext 到 ctx（与上面读侧 Get* 对称）
func WithResumeContextValue(ctx context.Context, rc *ResumeContext) context.Context
```

> **`WithResumeContextValue` 写侧 helper（高级用法）**：标准 HITL 闭环（`agent_v2.WithResumeUserInputs` → `Agent.Resume`）框架内部已自动注入 ResumeContext，业务**通常不需要**直接调本 helper。仅以下高级场景用：业务自管 cp round-trip 后手动喂答案、单测构造 ResumeContext、或自定义编排绕过标准 Resume 入口时。注入后下游节点用上面的 `GetResumeContext` / `GetResumeAnswer` 即可读到。

### 4.3 业务节点取答案模式

```go
// 自定义 NodeFunc 内部
func myNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    // 算自己的 InterruptID
    // ⚠️ 业务通常**不用手写这两段**——`compose_v2.Ask(ctx, q)` 把「算 ID → 查答案 →
    // 没有就抛中断」三步折叠成一次调用，见 §3.4。下面展开写只为说明它内部做了什么。
    myID := compose_v2.CurrentInterruptSite(ctx).InterruptID()

    // 二阶段：Resume 后取答案
    if ans, ok := compose_v2.GetResumeAnswer(ctx, myID); ok && ans != nil {
        // 用 ans.Content / ans.ContentBlocks 决定行为
        return processAnswer(ans), nil
    }

    // 首次 → 抛 InterruptError
    return nil, &compose_v2.InterruptError{
        Query: &compose_v2.AskQuery{
            Question:    "需要确认？",
            InterruptID: myID,
        },
    }
}
```

### 4.4 两条通道：寻址型 vs 非寻址型

续跑负载有两种语义，**走两条独立通道**：

| | 通道 | 语义 | 节点怎么读 |
|---|---|---|---|
| **寻址型** | `WithResumeUserInputs(map[InterruptID]*Message)` | 这条答案是给**某个具体提问**的 | `GetResumeAnswer(ctx, myID)` 按自己的 ID 取 |
| **非寻址型** | `WithResumeUserText(string)` | 用户这轮说了什么，**不指向任何提问** | `GetResumeUserText(ctx)` |

典型的非寻址消费者是 PlanAgent 的 LLMPlanner：它要读用户在 Resume 时补的指示（「换个
方向」）喂回规划 prompt，**但它不在节点里、没有自己的 InterruptID**，按 ID 取不到东西。

```go
// agent_v2/llm_planner.go (节选)
resumeUserMsg, _ := compose_v2.GetResumeUserText(ctx)  // 注入 Planner prompt
```

#### 为什么必须分开（2026-08-06，两条实测代价）

两种语义对「要不要限定给我」的答案**正好相反**——寻址型必须限定（拿错就是「用户批准
的是这件事、系统执行了另一件」），非寻址型不能限定（它压根没有对应的提问）。混在一条
通道里时：

1. 非寻址型没有真 ID 可用，只能拿**假 key** 挤进去（曾约定 `_replan_`）。而那个假 ID
   跟真挂起并存时，`ask_user` 取不到自己的答案、**反复重抛中断死循环**。
2. 为了让非寻址型读得到，出现过一个 `GetResumeAnswerSingle`（取 map 里**任意一条**）。
   它同时也能取到**别人的**答案——实测：同一会话里任务 A 挂起被放弃、任务 B 挂起，
   用户答 A 的 ID，用它取答案的节点执行了 **B** 那件事，`err == nil`。

⚠️ **给它加过滤治不了本**：A 的 ID 合法地在挂起集里，怎么裁都留着。所以删掉那个读法
本身——现在节点只有 `GetResumeAnswer(ctx, myID)` 一条路，**拿到别人的答案在构造上不
可能**（框架里唯一读 `UserInputs` 的地方就是按 ID 精确取那一处）。

#### 同一句话要走两条通道时，调用方显式给两次

`plan_chat_web` 就是这个形态：用户那句话既是给 `ask_user` 的答案，也是 planner 要据此
调整剩余步骤的指示。**拆分之前这半是偶然得到的**（「取任意一条」顺手捞到，而那个读法
同时也能捞到别人的），现在要 planner 看见就得显式传——调用方本来就知道自己的意图。

### 4.5 nil-safety 双重校验（audit Round 10 Bug B3 文档化）

caller 传 `map[id]nil`（前端反序列化失败 / DB 字段 NULL）时 `GetResumeAnswer` 返
`(nil, true)`——key 存在但 value 是 nil。**业务必须 `if ok && msg != nil` 双重校验**
避免 deref-nil panic。

`GetResumeAnswerText` / `ask_user_tool` 已加守卫；业务自定义 NodeFunc 用 `GetResumeAnswer`
时同样需守。

### 4.6 高层业务 API（agent_v2.WithResumeUserInputs）

```go
// agent_v2/options.go
func WithResumeUserInputs(inputs map[string]*schema.Message) ResumeOption
```

业务侧调用（**统一用 `AllQueries()` 协议**，见 §2.1）：

```go
answers := make(map[string]*schema.Message)
for _, q := range ie.AllQueries() {
    answers[q.InterruptID] = schema.UserMessage("yes")  // 业务实现按 q.Question 决定答案
}
events, err = agent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
```

agent_v2 内部转成底层 `compose_v2.WithResumeAnswers` RunOption 注入 `compose_v2.Run`。

---

## 5. ResumeAction 三模式（Continue / Replan / Abort）

### 5.1 三常量定义

```go
// agent_v2/agent.go
type ResumeAction int
const (
    ResumeActionContinue ResumeAction = iota  // 默认：从中断节点续跑
    ResumeActionReplan                        // 跳到上层规划入口（典型 PlanAgent）
    ResumeActionAbort                         // 删 cp 直接终止
)
```

### 5.2 三模式语义

| 模式 | 行为 | 典型场景 |
|---|---|---|
| `Continue` | LoadNS 恢复 cm，UserInputs 配 lastInterruptID 答 ask_user 工具，从中断节点续跑 | 标准 HITL：user 答完 ask_user 继续 |
| `Replan` | 用户指示走 `WithResumeUserText`（**非寻址通道**），下游 Planner 用 `GetResumeUserText` 取出重规划 | PlanAgent 用户中途说"换个方向" |
| `Abort` | DeleteSubtree 清 cp 退出，Resume 返 (nil, nil) 让 caller 区分于"自然跑完"（caller 显式发 aborted 事件）| 用户主动取消任务 |

**Abort 清理范围 (review-v6 H-03 fix 后行为)**：DeleteSubtree 用 `compose_v2.GetGraphNS(ctx)` 作 nsPrefix——
- **顶层调用** (ctx 无父 graphNS, e.g. 业务直接调 `agent.Resume(ctx, sid, Abort)`): nsPrefix="" 清整 sid 所有 ns
- **nested 调用** (ctx 已被父 push graphNS, e.g. parent agent 调 `subAgent.Resume(ctx, sid, Abort)`): 只清本 sub-agent 子树, 父 cp + sibling sub-agent cp 保留. 业务想清整树需调根 agent.Resume 或显式 `compose_v2.WithGraphNS(ctx, "")` 重置

### 5.3 业务侧用法

```go
// Continue（默认，可省）
ag.Resume(ctx, sessionID,
    agent_v2.WithResumeUserInputs(map[string]*schema.Message{
        interruptID: schema.UserMessage("用户答复"),
    }))

// Replan（仅 PlanAgent 主流场景）——用户指示走**非寻址通道**，它不指向任何提问
ag.Resume(ctx, sessionID,
    agent_v2.WithResumeAction(agent_v2.ResumeActionReplan),
    agent_v2.WithResumeUserText("换个方向：先做 X"))

// 同一句话既是某个提问的答案、又要给 planner 看时，**两条都给**（调用方显式表态）
ag.Resume(ctx, sessionID,
    agent_v2.WithResumeUserInputs(map[string]*schema.Message{
        interruptID: schema.UserMessage("批准"),
    }),
    agent_v2.WithResumeUserText("批准"))

// Abort
ag.Resume(ctx, sessionID,
    agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
```

### 5.4 PoC 简化语义 + 已知限制

**Replan 实际支持矩阵**（audit Phase 5.2 Bug #3 后收紧为 fail-fast）：

| 编排 | 接受 Replan | 行为 |
|---|---|---|
| **PlanAgent** | ✅ | 主流场景——LLMPlanner 通过 `GetResumeUserText` 消费 user 文本主动重规划 |
| **BasicAgent** | ✅（透传） | 内嵌 sub-agent（如 PlanAgent 包成 tool）会自然消费 ResumeContext.UserInputs |
| **GraphAgent** | ✅（透传） | R21 audit MED-1 fix `31660b1` 后业务 NodeFunc 内嵌 BasicAgent + skill，Replan 透传给底层；不再 fail-fast |
| Sequential / Parallel / Loop / Routing / Reflect / Handoff | ❌ **直接 return error** | 6 编排显式 fail-fast 返 `"%s agent %q: ResumeActionReplan not supported"`（commit `c213263` Phase 5.2 收紧） |

**～～已知触发场景：Replan 不能与 ask_user pending 并存～～ —— 2026-08-06 已解**。

原症状：Replan 的用户指示只能拿假 key（`_replan_`）挤进按 InterruptID 寻址的答案 map，
而它不匹配 ask_user 自动算的 stableID ⟹ ask_user 取不到自己的答案、**反复重抛
InterruptError 死循环**。当时记的长期修法是「Replan 路径主动 clearInterrupts」。

**没走那条**——走的是把非寻址型负载拆去独立通道（`WithResumeUserText`，见 §4.4），
假 key 整个不再需要，冲突的前提消失。

---

## 6. 多 tool_call 部分中断处理

### 6.1 问题：一轮 LLM 输出多个 tool_call，部分中断

LLM 一轮可能输出多个 tool_call（典型 GPT-4 / Claude 多工具并行）：

```
assistant {
  tool_calls: [
    {id: "tc_1", name: "get_weather", args: {...}},
    {id: "tc_2", name: "ask_user", args: {question: "确认下单？"}},
    {id: "tc_3", name: "search_db", args: {...}},
  ]
}
```

`tc_2` ask_user 抛 InterruptError 时，会发生什么？

### 6.2 v2 实际机制：toolsNode 整体失败 + 节点 memo 让已成功的 tool_call 不重执行

**v2 toolsNode 实际行为**（`agent_v2/basic_builder.go::toolsNode`）：

```go
toolsNode := func(ctx context.Context, chs Channels) ([]ChannelWrite, error) {
    msgs, _ := Get(chs, basicMessagesCh)
    last := msgs[len(msgs)-1]
    toolCalls := schema.GetToolCalls(last)

    // 业务 tools fn 处理 toolCalls（N=1 inline；N>=2 时 makeToolsFn 内 goroutine 并发 fanout，ISSUE-81）
    results, err := tools(ctx, toolCalls)
    if err != nil {
        // 任一 tool_call 失败（含 InterruptError）→ 整 toolsNode return error
        // → 节点 writes 不 apply（tc_1 的 tool_result 不进 channel——但它已进节点 memo，见下）
        return nil, fmt.Errorf("tools: %w", err)
    }
    // ...
}
```

**关键事实**：
- toolsNode 失败时 channel writes **全部作废**（BSP barrier 原则不变）——cp.Channels 里是上轮 modelNode 写的
  messages（assistant + N tool_calls），**不含任何 tool_result**
- 但**已成功 tool_call 的产出不丢**：每个 tool_call 成功那一刻就记进 tools 节点的私有 memo
  （`toolCallMemo.Done`，键 `(tc.ID, 批内下标, 工具名)`，载体 `compose_v2.SaveNodeState` → cp 的 `NodeStates`；
  **每成功一个存一次**、不等节点 return——中断是 error 路径，等不到）。记的是整条 tool_result Message（4 层压缩
  marker / return-direct 标记 / 失败 code 都在）+ inline skill body + 这次调用做出的转交 / 路由决定
  （上行信号，见 [handoff.md](./handoff.md) §6.3）
- Resume 时 toolsNode 重跑 → 逐个 tool_call **先查 memo**：
  - tc_1 **命中** → 不调工具函数体，直接返记下的 tool_result；它当时做出的转交 / 路由决定（`transfer_to_agent` /
    `route_to`，或工具里经 `RunSubAgent` 调的 agent 做出的）原样交出
  - tc_2 拿 user 答案返 tool_result（二阶段语义）
  - tc_3 没跑到 → 执行
- `OnToolStart/End` hook 对这一批没处理完的 tool_call 仍各 fire 一次（这一批中断之后续跑，命中 memo 的也 fire：
  OnToolEnd 还没处理过它们）——观测口径的重复计数是既有行为，memo 只消掉「工具函数被重复调用」这一半。这一批处理完
  之后才作废的不再 fire（§6.5）
- 进不进 memo 的判据是「这次调用的副作用能不能重放」（`shouldMemoize` = 做出了上行信号 **或** 不是框架元工具）：
  没做出信号的框架元工具刻意不进、靠函数体重跑补回那半（`tool_search` 的解锁写 channel 那一步在工具体里，
  跳过函数体就没人写——理由见 `tool_memo.go` godoc，动名单前先看 `tool_search_resume_test.go`）；审批记录
  （`Approved`：批过的不重复问；`Decided`：这次调用的审批结论，续跑重放、不看当时的配置）也在同一份 memo 里。
  调用内容另有一份步骤记录（`fixToolCallContent`）：一批调用在第一次交给工具执行时定下 `OnToolStart` 改写之后的内容，
  续跑按它执行、不按那一次的改写——memo 的命中、审批、工具自己的提问都是对这份内容做的。判据与代码注释见 `agent_v2/basic_agent.go` 的 `memoKey` /
  `rememberDone` 段与 `agent_v2/tool_memo.go`

### 6.3 幂等要求缩到 memo 覆盖不到的地方

节点 memo 落地（2026-07-30，`compose_v2` 节点私有中断态原语）后，**「Resume 重跑全部 tool_call、业务工具
必须自负幂等」不再成立**。仍要业务自己扛的只剩下面几格：

| 场景 | 为什么 memo 覆盖不到 | 应对 |
|---|---|---|
| 进程在 tools 节点执行中途崩溃 | memo 只随节点「没成」的那次出口存档落盘；sync / async durability 只在超步边界写档，那一刻这个节点还没开始或已经结束，带不上 memo——配了也一样丢 | 写操作带 idempotency_key（durability 只保超步边界的进度，保不了节点中途的） |
| `SaveNodeState` 失败（序列化失败等；日志 `agent_v2: 存 tool_call 进度失败，该工具 Resume 时会重跑`）| 那一条没记上 | 看到这条 warn 就知道该工具会重跑；同上 |
| 没做出上行信号的框架元工具（`tool_search` 等）| 它们的那半副作用在工具体里发生（`tool_search` 写解锁 channel），跳过函数体就没人做，刻意不进 memo | 它们本身幂等（解锁是集合并），业务不用管；`activate_skill` 的 fork 模式里已跑完的 sub-agent 由嵌套调用的完成记录挡住重跑（见 [skill.md](./skill.md) §8） |
| 中断的那个 tool_call 自身 | 它没成功，重跑是应有之义 | ask_user 二阶段语义天然幂等（§6.4）；自定义提问工具走 `compose_v2.Ask` 同款 |
| 业务工具在 tool_result 之外、要在续跑时**再生效一次**的产出（写进不随存档走的 session KV……）| 会被记账、命中时那半不再发生（工具里经框架入口做出的转交 / 路由决定除外——它们随账记下、命中时交出） | 那半进 cp（channel / 节点私有态），命中时从那里读回 |

**设计改判记录**：本节曾论证「rerun-all 是 correctness-first 的有意选择」（时效性：人停顿期间外部状态会变；
快照一致性：一轮 N 个 tool_result 应反映同一时刻）。2026-07-30 改判——那套说法把框架能解的问题推给每一个
业务工具，而 HTTP 调用 / 写库 / 发消息天然不幂等（`compose_v2/node_state.go` 包 godoc）；时效性的代价改由
业务在真需要新鲜读的工具上自己重取。现在的形态与 eino `ExecutedTools`（map[callID]output）同形、比它多两维
键（批内下标 + 工具名，理由见 `basic_agent.go` 的 `memoKey` 注释）；三方落点对照见
[interrupt-resume.md「已成功的工作单元 Resume 时不重跑」](../description/three-way-comparison/interrupt-resume.md)。

> **粒度现在对称**：分支级 Resume 只重跑失败/中断分支（`graph_loop.go::frontierResult` 的 `SuccessWrites` /
> `FailedBranches` per-branch delta + `cp.CompletedFrontier/DeferredSends` 保成功成员的出边），tool_call 级只重跑
> 没命中 memo 的。两级都是「已成功的不重放」。

### 6.4 ask_user 二阶段语义的天然幂等性

`ask_user_tool` 的二阶段语义（详见 §3.5）天然解决了 ask_user 自身的幂等问题：

```
Run 1: askUserToolFunc 入口 → GetResumeAnswer(stableID) miss → 抛 InterruptError
Run 2 Resume: askUserToolFunc 入口 → GetResumeAnswer(stableID) hit → 直接返答案 tool.Success(content)
```

**stableID 跨 Run 一致**（`InterruptSite` 各维度都跨 Run 持久化）——所以 ask_user **不会"重新问一次"**，是直接返答案。

### 6.5 关键不变量

- ✨ toolsNode 任一 tool_call 失败/中断 → 整节点失败，channel writes 全部作废；**已成功 tool_call 的产出在节点 memo 里不丢**（随 cp 走）
- ✨ Resume 重跑 toolsNode → memo 命中的**不调函数体**、重放产出（含当时做出的转交 / 路由决定）；未命中的执行；`OnToolStart/End` 照常 fire（这一批处理完之后才作废的除外，见下一条）
- ✨ 这一批处理完（`OnToolEnd`、批内裁决、按大小处理）之后结果**定下**：这次执行之后在提交之前作废（节点成功之后的钩子 Goexit、提交出错）时，续跑**不再处理**这几个调用——工具不执行，`OnToolStart` / `OnToolEnd` 都不触发，交出定下的结果与钩子当时做出的转交 / 路由决定。再交给钩子的只能是处理完的那份（转存说明、钩子改写过的结果），不幂等的钩子在它上面报错或叠加改写，续跑就过不去或结论跟着变（审计实测）；两个钩子一起不触发，钩子开的 span 也不落单。被批内裁决换掉的那几条定下的是替换后的失败结果（续跑时它按失败判、决定作废，生效的仍是首跑那个）；没处理完就停下的照常重新处理、改写照常生效（`agent_v2/tool_memo.go` 的 `finalizeToolMemo` / `fixedToolResults`，哨兵 `tool_memo_fixed_test.go` 六条）
- ✨ model 节点同一条规矩（`agent_v2/model_call_record.go`）：这一轮处理完（`OnModelEnd` 之后）定下，之后作废续跑时不再处理——不调模型，`OnModelStart` / `OnModelEnd` 都不触发，不重记会话历史。模型答复之后、处理完之前停下的看原因：因为等人或被叫停（中断、取消）停下的，续跑沿用这次调用的答复、不再调模型（钩子问过人的，用户答的就是这一份），也不再带用量（一次调用只报一次）——这一遍 `ModelInput.Extra` / `ModelOutput.Extra` 标着 `hook.ModelResponseReusedKey`，观测不再记一次模型调用、agui 适配层不再把答复发一遍（哨兵 `TestModelResponseReused_RecordedOnce` / `TestHandler_ReusedModelResponse_NotResent`）；钩子报错拒掉、把答复置空、panic 的续跑重新调模型换一份——内容触发的钩子 panic 留着原样的话续跑永远过不去；重新调模型时推进 `OnModelEnd` 那一格的重做代际（模型钩子各在自己的相位里跑，`compose_v2.AdvanceRerunGeneration`，同条件边否决那一格）：对新答复的提问换新 ID，累积传的旧批准不会兑付给新的一份；那一格里与答复无关的普通记录（扣费这类嵌套调用）照常拿回、不重做；`OnModelStart` 问的是输入、它那一格不推进，答过的不再问（哨兵 `TestModelCall_RegeneratedAfter*_AsksAgain` / `TestModelCall_RegeneratedAfterVoid_NormalNestedCallRecalled` / `TestModelCall_Regenerated_StartQuestionNotReasked`）。沿用标记只对排在会停下的钩子前面的钩子成立，按调用计数、往外发的钩子要排在前面。作为编排成员时，重放那一轮照首跑那样登记成「这次调用里记过」，边界补记不替它再记一份（`TestHistory_MemberModelVoidedAfterProcessing_FinalRecordedOnce`）。tools 节点这一批已记过会话历史的，重放时不再记（哨兵 `history_voided_window_test.go`）
- ✨ ask_user 二阶段语义自动幂等（GetResumeAnswer 命中即返）
- ✨ 同节点多次 ask_user 用 `NextInterruptIndex` 自管 idx（业务工具）

> v1 时代的「toolsFn 倒扫 messages 跳过已配对 tool_call」和 v2 初期的「rerun-all + 业务写操作自负幂等」
> 都已被节点 memo 取代（2026-07-30，§6.3 改判记录）。

### 6.6 partial answer multi-Resume 协议（audit #5 视角 9 F10 fix）

fanout 多 tool_call 同时抛 Interrupt 时 (toolsFn N tc 并发, ISSUE-81), `cp.PendingInterrupts` 含 N 个 query. 业务 caller `WithResumeUserInputs(map[id]Message)` **可以 partial 回答** (只答 M ≤ N 个), 但**业务用法必须累积传**:

```go
// Run 1: 3 tc 同时抛 Interrupt → cp 含 3 query [q1, q2, q3]
_, err := agent.Run(ctx, sess, input)
ie1, _ := compose_v2.IsInterrupt(err)
// ie1.AllQueries() = [q1, q2, q3]

// Resume 1: 只答 q1 + q2 (业务前端只收集到 2 个)
_, err = agent.Resume(ctx, sid, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
    q1.InterruptID: msg1,
    q2.InterruptID: msg2,
}))
// → tc_x/tc_y 拿到答案返 tool_result, tc_z 重抛 InterruptError 同 InterruptID (ID 由位置派生、跨 Resume 稳定)
// → cp_2 含 1 query [q3]
// → framework slog.Warn "WithResumeAnswers 部分回答, pending 仍含未答 InterruptIDs"

// Resume 2: **累积传所有 3 个答案** (含 q1 + q2 已经在 Resume 1 答过的)
_, err = agent.Resume(ctx, sid, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
    q1.InterruptID: msg1,
    q2.InterruptID: msg2,
    q3.InterruptID: msg3, // 新答
}))
// → 全 tc 拿到答案 → Run 完成
```

**关键设计**:
- `WithResumeUserInputs` 是 **per-Resume-call ctx 注入**, 不持久化跨 Resume——Resume 2 时 framework 不"记住" Resume 1 已答的 q1/q2
- 业务前端必须**累积持有所有已答答案**, 每次 Resume 全传 (跟 LangGraph `Command(resume=map)` 持久化不同)
- 不只是 partial 回答需要累积：**一个节点先后问两问**（第二问在第一问答完之后才出现）时，第二轮续跑节点从头重跑，
  第一问没有答案就会再问一遍——只传这一轮答案的调用方会让两问来回重问、永远答不完
- 框架自带的两个协议适配层替各自的对端做了这件事：`a2a/server` 按任务累积已接受的答复（对端可以只答其中几问），
  `agui` 同样累积（AG-UI 另要求每次 resume 覆盖全部挂起中断，但跨轮的先后两问仍要累积）。两处曾经都只交这一轮的，
  先后两问时来回重问（审计实测，哨兵 `TestServerAnswers_SequentialAsksConverge` / `TestHandler_Resume_SequentialAsksConverge`）
- 未答 query re-raise 同 InterruptID (ID 由位置派生，tc 跨 Resume 拿到同一个), cp 自动收敛 (只剩未答的)
- framework `slog.Warn` 双向报警:
  - **missing** (pending ∖ answers, audit #5 F9 fix): 业务部分回答时 framework 提醒"还有 N 未答, 会自动 re-raise"
  - **unmatched** (answers ∖ pending): 业务累积传时已答 query ID 不在新 cp 中——预期 warn (业务 caller 累积模式正常)

**对比 LangGraph**: LangGraph 把 `scratchpad.resume_map` 持久化到 cp, 多轮 Resume 时 framework 自动 merge——业务只传新答案. crew-go v2 走"业务前端累积"模式更显式 (业务持答案状态 + framework 不存), 对应 example `plan_chat_web` 业务前端 ✓ 持续回放历史回答.

回归测试: `TestISSUE81_PartialAnswer_PartialResume_NarrowsToRemaining` (agent_v2/issue81_toolsfn_fanout_test.go).

---

## 7. Fan-out 多分支并发中断

### 7.1 多分支并发中断收集

fan-out 派生 N 分支并发，多个分支同时抛 InterruptError 时，框架**全部收集**：

```
超步 N: fan-out → frontier=["worker1", "worker2", "worker3"]
worker1: 抛 InterruptError(Query{InterruptID: "id1", Q: "确认 worker1?"})
worker2: 抛 InterruptError(Query{InterruptID: "id2", Q: "确认 worker2?"})
worker3: 正常 return writes
   ↓ executeFrontier 收集 frontierResult.AllInterrupts
   ↓
for _, q := range allInterrupts {
    cm.recordInterrupt(q)  // 累加到 cm.PendingInterrupts
}
   ↓ Run defer 内 cper.SaveNS()
cp.PendingInterrupts = [Query1, Query2]
   ↓ Run 返 res.exitErr() 给 caller（单一 ie，多分支时是 MergeInterrupts 合出来的，AllQueries() 已含全部）
```

**audit Round 6 Bug 3 修复**（commit `e540139`）：之前 `executeFrontier` 只收集第一个
分支的中断（FirstInterrupt 字段），其他被忽略——多分支并发中断时 user 看不到 worker2
的问题。R6 改为 `AllInterrupts` 全部收集，让 caller 用 `cm.recordInterrupt` 注册全部
query。详见 `compose_v2/graph_loop.go` 内 `AllInterrupts` 字段收集逻辑（grep `AllInterrupts` + `recordInterrupt` 看实际位置）。

### 7.2 业务侧 Resume 多 pending

```go
events, err := agent.Run(ctx, sess, input)
if ie, ok := compose_v2.IsInterrupt(err); ok {
    // ie.Query 是其中一个，但 cp.PendingInterrupts 含全部
    cp, _ := cper.LoadNS(ctx, sess.RunID, "")
    answers := make(map[string]*schema.Message)
    for _, q := range cp.PendingInterrupts {
        answers[q.InterruptID] = schema.UserMessage(askUser(q.Question))
    }
    events, err = agent.Resume(ctx, sess.RunID,
        agent_v2.WithResumeUserInputs(answers))
}
```

### 7.3 worker3 已成功的 writes 怎么办

worker3 没抛中断，但因为 worker1/worker2 抛中断，整超步 status=Pending。worker3 的
ChannelWrite **已经在 cm 里 apply**（per-branch incremental）——cp save 时含 worker3
的 writes 进 cp.Channels。Resume 时不重跑 worker3。

这是 channel-based 模型的天然能力——v1 时代用 `superstep_snapshot per-branch delta`
hack 才能实现的"仅重跑失败/中断分支"，v2 是引擎默认行为。

### 7.4 关键不变量

- ✨ 多分支中断**全部收集** PendingInterrupts（R6 Bug 3 全部收集 + R10-D 修 record/apply 顺序）
- ✨ 同超步成功分支 writes 已 apply 进 cm + cp.Channels（per-branch delta 自然继承）
- ✨ Resume 时**仅重跑失败/中断分支**（成功分支已在 cp.Channels）
- ✨ `cm.PendingInterrupts()` getter 返深拷贝防 caller mutate（R9 C4 修）；**Snapshot 写 cp 是浅拷贝**（make+copy 新 slice 装同 `*AskQuery` 指针，mutate 元素 struct 仍回污染 cm——实证 serialization.go:327-328，与 §2.3 / channel-engine.md §7.6 一致）

---

## 8. 嵌套子图 ResumeContext 隔离 opt-in

### 8.1 默认行为：父 ResumeContext 透传到子图

`ResumeContext` 通过 ctx 透传——嵌套子图的 NodeFunc **默认能拿到父 Run 的 UserInputs**。
这适合"父图节点直接是子 agent"的常见场景（用户答案对子树都有效）。

```go
// 父 Run
events, err = agent.Resume(ctx, sid,
    agent_v2.WithResumeUserInputs(map[string]*schema.Message{
        "ans1": schema.UserMessage("yes"),
    }))

// 子 agent NodeFunc 也能拿到（默认透传）
func childNode(ctx context.Context, chs Channels) ([]ChannelWrite, error) {
    if ans, ok := compose_v2.GetResumeAnswer(ctx, "ans1"); ok {
        // 用 ans
    }
}
```

### 8.2 opt-in 隔离：StripResumeContext + WithSubgraphIsolatedResumeContext

**问题场景**（audit Round 9 Bug B2）：业务在寻址通道里用**自定义的固定 key**（不带
graphNS）时，父子可能互相吃对方答案。需要显式隔离。

⚠️ **原先最典型的那个例子（PlanAgent 的 `_replan_`）已经不存在**——Replan 的用户指示
2026-08-06 改走非寻址通道，不再往寻址 map 里塞假 key。本节现在只对「业务自己约定固定
key」这种写法成立。

**ctx 内 strip helper**：

```go
// compose_v2/interrupt.go
func StripResumeContext(ctx context.Context) context.Context
```

业务侧主动调（少用）：

```go
// 自定义 GraphAgent NodeFunc 内
childCtx := compose_v2.StripResumeContext(ctx)
err := childAgent.Run(childCtx, sess, input)  // 子 agent 拿不到父的 UserInputs
```

**Subgraph 节点 opt-in 隔离**：

```go
// compose_v2/subgraph.go
func WithSubgraphIsolatedResumeContext() SubgraphOption

b.AddSubgraphNode("planner", planGraph,
    compose_v2.WithSubgraphIsolatedResumeContext(),  // 子图入口 strip 父 ResumeContext
)
```

### 8.3 框架 auto-ID 路径不受隔离影响

框架 auto-ID 算法把 `graphNS` 编入 InterruptID——父子 ID 不冲突（不同 ns 哈希不同）。

**隔离只防「业务自己约定固定 key」的串台**（那种 key 不带 graphNS）。框架 auto-ID
路径默认就安全，不需要隔离；框架自己曾用过的那个假 key（`_replan_`）已随通道拆分消失。

### 8.4 关于"子图想 opt-in 继承父答案"

**v2 默认行为已经是继承**——不需要单独的 forwarding opt-in。父 ResumeContext 通过 ctx
透传到所有子树（含嵌入 subgraph）。业务想隔离才显式 `WithSubgraphIsolatedResumeContext()`。

> ⚠️ **历史 PoC 残留** (audit follow-up 已修): 早期 PoC 设计稿曾提 `WithSubgraphResumeAnswersForwarding` opt-in 函数, **v2 从未实现** (grep `compose_v2/` `agent_v2/` 0 hits) + 当前 `compose_v2/interrupt.go` godoc 也无此引用 — 默认行为已是 forward, 不需要显式 opt-in.

### 8.5 关键不变量

- ✨ ResumeContext 默认全 ctx 子树透传（嵌套子图继承）
- ✨ 业务 sentinel-key 场景必须显式 `WithSubgraphIsolatedResumeContext()` 隔离
- ✨ ID 编入 graphNS 天然防串台，不需要额外隔离

---

## 9. mid-stream Interrupt + reset_stream 协调

### 9.1 问题：流式输出中途中断

BasicAgent modelFn 流式调 LLM 时，chunk 实时投递给前端 SSE。如果中途抛 InterruptError
（如 ReactiveCompact retry 后第二次 attempt 启新 stream），前端会看到：
- 第一次失败前已 emit 部分 chunks → 失败 → ReactiveCompact 触发
- 第二次 attempt 新 stream 又 emit chunks → 前端"半截答案 + 全新答案"重复渲染

### 9.2 reset_stream 信号 + run_id（audit Round 5 Bug #2 修）

retry 前框架通过 EventSink 发 `reset_stream` 信号 + 当前 run_id：

```go
// agent_v2/basic_agent.go::makeModelFn (节选)
if sink := compose_v2.GetEventSink(ctx); sink != nil {
    sink.Send(compose_v2.Event{
        Type:    "reset_stream",
        Content: fmt.Sprintf("ReactiveCompact retry attempt %d/%d", attempt+1, maxAttempts),
        Metadata: map[string]any{
            "reason":      "prompt_too_long",
            "attempt":     attempt + 1,
            "max":         maxAttempts,
            "compact_len": len(curMsgs),
            "orig_len":    len(msgs),
            "run_id":      lastRunID,  // ← 即将作废的 stream group id
            "agent":       cfg.Name,
        },
    })
}
```

**前端按 run_id 精确丢 buffer**——之前 R5 缺 run_id 前端要么全清要么漏清。

### 9.3 关键不变量

- ✨ ReactiveCompact retry 前 emit `reset_stream` event + run_id（前端按 run_id 丢 buffer）
- ✨ retry 后新 stream 用新 run_id——前端识别新 run_id 启新 buffer
- ✨ chunk 不写 session.Events（只走 OrchHooks.OnNodeEvent）——retry 重 emit chunks 不污染审计

> 另：audit Round 10-A（commit `b940ccf`）修了 mid-stream 收 InterruptError 时 OnModelEnd
> override return Message + emit reset_stream 协议（不同于本节的 ReactiveCompact retry
> 路径）——也是相关 reset_stream emit 点，但属于 Recv path 而非 retry path。

---

## 10. 9 编排 HITL 矩阵 + nested_smoke 12 场景

### 10.1 9 编排 HITL 矩阵

v2 单 `agent_v2.Agent` 接口 + ctx graphNS push 自动支持嵌入——**全 9 编排（含
BasicAgent 顶层）HITL Resume 闭环**，无 fallback 路径。

| 编排 | 嵌入运行 | Checkpointer 自动注入 | Resume API | HITL 可用 |
|---|---|---|---|---|
| BasicAgent | ✅ `agent_v2/basic_agent.go::Run` | ✅ | ✅ `Resume(ctx, sid, opts...)` | ✅ |
| SequentialAgent | ✅ | ✅ | ✅ | ✅ |
| LoopAgent | ✅（per-round ns 隔离）| ✅ | ✅ | ✅ |
| ReflectAgent | ✅（actor + reflector 跨轮 HITL）| ✅ | ✅ | ✅ |
| HandoffAgent | ✅ | ✅ | ✅ | ✅ |
| ParallelAgent | ✅（per-branch delta + 仅重跑失败/中断分支）| ✅ | ✅ | ✅ |
| RoutingAgent | ✅（Router 函数 / LLM Classifier 两路径，均在 classify node 内跑）| ✅ | ✅ | ✅ |
| PlanAgent | ✅（plan↔exec 循环 + Replan 重规划）| ✅ | ✅ | ✅ |
| GraphAgent | ✅（任意 `compose_v2.Graph` 包成 Agent）| ✅ | ✅ | ✅ |

> ⚠️ **GraphAgent 用户责任**：用户自定义 NodeFunc 内调 sub-agent.Run 时框架不自动 push
> 父 ns，**必须走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input)`**——不走的话 sub-agent
> 的存档落进父图自己的槽（ctx 里的 ns 就是父图的），父的写被 GraphID 守卫拒，Resume 撞
> `ErrGraphIDConflict`（哨兵 `agent_v2/hook_interrupt_scope_test.go` 形态 B）。
>
> ⚠️ **别手拼 childNS**（曾教过 `WithGraphNS(ctx, JoinNS(parentNS, "<nodeID>"))`，已撤）：
> 一次嵌套调用的身份是**位置**——调用方所在的节点与相位（节点体 / 某个图钩子 / 某条条件边），加实例
> 维三选一：图级 fanout 实例（`SendFrom` 同 Target 投递出的实例之一，下标是进程级发号器的值、不是
> 第几个）/ 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态、cond 路径的 visit（本节点
> 已成功完成的访问次数）；fanout 实例节点里又起 tool_call 并发时实例与 tool_call 一起进位置——加上
> **这是哪一次调用**：不带 key 时是输入内容 + 同一份输入在本次执行里的第几次，带
> `agent_v2.WithCallKey` 时是 key（全历史输入的 `AgentAsTool` 与 skill fork 只看位置）。存档槽由
> 身份哈希而来。那段判定住在 `RunSubAgent` 里，手拼等于复制一份会漂的拷贝，而漏哪一维都不会在漏的
> 那一刻报错：
>
> - 漏实例维 ⟹ N 个实例撞同一个 childNS、子 cp 互覆、进度蒸发（ISSUE-96 侧面 2，实测 2 实例只得 1 个子 cp）
> - 漏 visit 维 ⟹ 父循环第 2 轮重进时子层算出与第 1 轮**相同的 InterruptID**，被答案账本判为已兑付、终局 `ErrRunAborted`（答案账本跨子图边界之前是第 2 轮的提问从未呈现就被第 1 轮的旧答案静默满足，实测 `gotAnswers=2` 而 `asked=1`）
> - 漏调用方节点 / 相位 ⟹ 两个节点、或一个节点的两次执行在钩子里用同一份输入调同一个子 agent 撞成一次，同样终局 `ErrRunAborted`（哨兵 `agent_v2/nested_call_hook_phase_test.go`）
> - 漏输入内容 ⟹ 同一个节点里带着新输入的第二次调用捡起第一次的失败存档接着跑，新输入没进子 agent（续错存档这一步不报错：第一次是确定性失败时以旧错收尾，瞬时失败时 `err == nil`；哨兵 `agent_v2/nested_call_identity_test.go`）
>
> **父节点重跑时**（中断续跑 / 失败续跑 / `RunWithRetry` 重试）：已完成的嵌套调用直接拿回记下的
> 结果、不再执行（整组事件记在父节点这次执行的私有态，`compose_v2.SaveStepResult`）——子 agent 里的
> 副作用不重放、问过的不重问，它做出的转交与路由决定（发给上层 HandoffAgent / RoutingAgent 的上行信号）原样交出；
> 没完成的续上自己的存档；新的从头跑。输入不同的调用之间身份跟次序无关，按进度跳过已完成的调用、
> 条件分支增减调用、累积传答案都不会让一个答案落到别的调用上（哨兵
> `agent_v2/nested_call_order_drift_test.go`，含框架工具审批）；输入完全相同的几次只能按次序区分，
> 那种调用别自己跳过（全部照调一遍即可），要跳过就给 key。⚠️ 转交与路由决定只在「再调一次、被拿回」
> 时交出，有这种决定的调用别跳过。钩子相位（`OnNodeStart` / 嵌套图的 `OnGraphStart` 等）里的调用同样
> 被记住；只有顶层图的图级钩子与入口条件边没有可挂的节点执行。被调方属于调用方的任务（执行器替它盖
> 任务章），调用方是业务自研的 agent 或框架外的循环时同样续得上自己的存档。
>
> **输入变了就是另一次调用**：新调用、从头跑、自己问自己的问题，用户对旧输入的回答不会被套上来。
> 输入里带着每次都变的值时每次重跑都是新调用、永不收敛——给调用 `agent_v2.WithCallKey(item.ID)`
> （输入变了照样续，打一条告警），或用 `compose_v2.SaveStepResult` 把那个值记住。不带 key 的调用
> 续跑时输入摘要对不上 = 撞槽 ⟹ 拒绝、不清档（`compose_v2.WithInputIdentity`）。交给被调方的是
> 输入的独立拷贝（`any` 值按原类型递归深拷，拷完类型不变、nil 与空不混）：被调方的钩子原地改写消息、
> 标注与里面的容器不会写回调用方、不会让续跑换了身份——**指针、函数、channel 原样共享**，经指针原地改
> 仍会写回（续跑时换身份重问），别这么改。中间件注入的每次都变的值落进了 `AgentAsTool`
> 交给被调方的输入时（自定义 schema 的整个 args，或改写了 `input` 字段本身）用 `agent_v2.WithAgentToolCallKey`
> 声明身份（`OnToolStart` 的注入续跑不变：一批调用的内容在第一次执行时定下）。
>
> `compose_v2.RunWithRetry` 包住 `RunSubAgent` 重试的是**同一次调用**（已完成的拿回、失败的续存档）；
> 「产出不合格要重来」别用它，也别让节点返错再 Resume——在节点里循环调（每一轮是新的一次调用），
> 或把校验放到条件边上声明 `CondFailRerunFrom`，给产出那一次调用声明 `agent_v2.WithRerunnable()`
> （条件边否决时重做的是整次调用，它问的问题也是新问题；没点名的照常拿回，被打断的条件边——软取消、
> 陈旧写回、调用方 ctx 已经死了——不算否决）。fn 里直接 `compose_v2.Ask` 的问题同理：每次尝试从同一个
> 提问序号数起，前一次尝试已经答过的不重问；编排按 retryCfg 重试成员时，成员在自己图外问的也是（前提跟
> 续跑一样：每次尝试在同一个位置问的是同一个问题，哨兵 `compose_v2/retry_ask_slot_test.go` /
> `agent_v2/retry_ask_slot_test.go`）。fn 里直接跑的嵌套图（`compose_v2.Run`）例外：它跑完之后重试从头重跑它时，
> 同一个位置再问当场终局失败，对旧内容的回答不会兑付给重新生成的内容；要越过它见 `RunWithRetry` godoc。
>
> 在一个正在执行的任务里调 agent，session 的 RunID 必须属于这个任务（对不上入口直接拒），Resume 在那样
> 的位置上一律拒；要开独立任务先 `agent_v2.DetachTask(ctx)`。
>
> 详见 CLAUDE.md "GraphAgent 提示" + [`channel-engine.md`](./channel-engine.md) §8.1–§8.2（ns 形态与 API）。

> ⚠️ **cond fn 禁止 interrupt**（2026-07-16 重构，取代原 ISSUE-86 D2「必须自填 ID」）：业务自定义
> `AddConditionalEdge` 的 cond fn 抛**任何** `InterruptError` 都被框架转**非-interrupt** error 拒绝
>（`IsInterrupt(err)==false`）。
>
> **原因（语义）**：cond 是**纯判断**——读 channel → 返目标，不产 writes、不改 state。而**中断的
> 本质是要新输入**；cond 需要新输入才能判断，说明**判断所需的信息还没备齐**，那是**上游 node
> 没尽责**。何况 cond 是纯函数、Resume 后要重新执行，用户答案仍只能从 channel 读——绕一圈还是
> 得让上游把答案备好，不如一开始就让上游 node 去问。**HITL 请在 node 内抛**（nodeID + visit
> 协议稳定，`Ask` 按位置维算稳定 ID；且 node 的 writes 未 commit 就中断，重跑天然自洽），
> 后接 cond 读 channel 路由。
>
> **RoutingAgent 用户无需改动**：Router / classifier 已在 `classify` **node** 内跑（不在 cond），
> 原生支持 HITL——classifier 里 `WithAskUser()` 问用户、业务 `WithRouter` 的函数抛 InterruptError，
> 都走标准 node interrupt + Resume 闭环——**提问走 `compose_v2.Ask`**（它算 ID、查答案、
> 没答案才抛），业务不自填也不能自填。

> ⚠️ **cond fn 报错（≠ 中断）的 Resume 语义由业务声明**（`WithCondFailPolicy`）：cond 是纯判断，
> 它**报错**只可能是「判断的前提不成立」——上游 node 产出的数据缺失/非法。该重试还是该放弃，
> 取决于**上游是不是确定性的**，只有业务知道：
>
> ```go
> // 默认：报错 = 终局失败 → 主动清 cp 子树（适合纯计算上游——重跑必拿同样的坏数据）
> b.AddConditionalEdge("check", cond)
>
> // 显式：报错 → 存 cp，业务 Resume 重跑 from 让它重新产出（适合 LLM 上游——重跑很可能就好）
> b.AddConditionalEdge("plan", cond, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
> ```
>
> **选 `RerunFrom` 的业务义务**：from 会被**整个重跑**，其 writes 必须幂等。框架自带编排的 from
> 只写 `LastValue`/`OnceFlag`（重跑覆盖/锁定，安全）；业务图若 from 写 `Append`/`Sum` 型 channel
> 会**双计**，须自行保证幂等。
>
> 详见 [`interrupt-resume.md`](../description/three-way-comparison/interrupt-resume.md) §cond fn 协议。

### 10.2 nested_smoke 12 场景覆盖矩阵

```bash
go run ./example/orchestration/nested_smoke
```

12 场景 30 秒跑完（无需真 LLM API，全 mock）：

```
=== 场景: BasicAgent 顶层 ask_user (chat_entry / plan_chat_web 核心路径) === PASS
=== 场景: sequential 内嵌 BasicAgent ask_user ===                            PASS
=== 场景: loop body=BasicAgent ask_user ===                                  PASS
=== 场景: reflect actor=BasicAgent ask_user ===                              PASS
=== 场景: handoff initial=BasicAgent ask_user ===                            PASS
=== 场景: sequential 多次 Run 同 sessionID（验证 cleanup 不污染） ===        PASS
=== 场景: sequential Resume Abort（验证 cp.DeleteSubtree 清子树） ===        PASS
=== 场景: loop 多轮 HITL（验证 ns round_<N> 隔离不冲突） ===                 PASS
=== 场景: reflect actor + reflector 跨轮 HITL ===                            PASS
=== 场景: parallel 内嵌 BasicAgent ask_user (一分支中断) ===                 PASS
=== 场景: routing classifier→BasicAgent ask_user ===                         PASS
=== 场景: GraphID mismatch fail-fast (ISSUE-50 P0 防串台) ===                PASS

=== 总结：12/12 通过 ===
```

**12 场景覆盖**：
- 5 基础：BasicAgent / Sequential / Loop / Reflect / Handoff 各跑一次 Run + Resume
- 4 高级：cleanup 不污染 / Resume Abort / loop 多轮 ns 重入 / reflect 跨轮
- 2 补全：Parallel / Routing 嵌入路径
- 1 ISSUE-50：GraphID 防串台 fail-fast

### 10.3 何时跑（CLAUDE.md "交付纪律"）

| 改动类型 | nested_smoke | plan_chat_web mock |
|---|---|---|
| cp 接口 / 协议 | ✅ | ✅ |
| 嵌套子图 / ns push 协议 | ✅ | ⚠️（间接验证）|
| Resume 路径 / cleanup | ✅ | ✅ |
| ns 寻址 / GraphNS 推进 | ✅ | ✅ |
| events 流 / OrchHooks | ⚠️ | ✅ |
| 4 层压缩改动 | N/A | ⚠️（mock 不触发 token 阈值）|

---

## 11. plan_chat_web 实战：用户暂停按钮 + Resume Continue

`example/orchestration/plan_chat_web` 是 v2 HITL 实战完整 demo——Web 服务跑 PlanAgent，用户中途按
"暂停"按钮触发 hook 主动抛 InterruptError，Resume 时 user 答 continue / abort。

> ⚠️ **这个范式只在「hook 挂在嵌套成员上」时成立**。`OnAgentStart` 在**顶层** agent 上抛
> 中断会被**拒**——那一刻图还没开始跑、本层没有能挂这条中断的地方，框架转成一个不可重试
> 的接线错误（`errUncatchableHookInterrupt`，实现 `compose_v2.NonRetryable`；不用
> `ErrRunAborted` 是因为那捆绑「清 cp 子树」，而这个错抛在图执行之前、什么档都没写，
> 清档会误伤同 sid 下无关的残档）。哨兵 `agent_v2/hook_interrupt_scope_test.go`。
>
> plan_chat_web 能用，是因为这个 hook 挂在 PlanAgent 的**子 agent** 链上（见下方「Hook 挂到
> 所有 sub-agent 和 planner 的 hooks 链上」）。照抄到一个顶层 `BasicAgent` 上会拿到错误，
> 不是挂起。要在顶层表达「暂停」，用 `WithSoftCancel`（超步边界优雅停 + 存档可续）。

### 11.1 interruptHook 实现

```go
// example/orchestration/plan_chat_web/hooks.go
type interruptHook struct {
    hook.NoOpHooks
    flag *atomic.Bool  // 用户按"暂停"按钮时 set true
}

// 双触发点（audit-iter-5-N HIGH-2）：OnAgentStart + OnModelEnd 都检 flag。
// 仅 OnAgentStart 时长 LLM 调用（10-30s 真 LLM）期间点暂停要等下一轮才生效；
// 加 OnModelEnd 让 LLM 调用一结束即触发（不等 tools + 下一轮 model）。
func (h *interruptHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
    return h.maybeInterrupt(ctx, info, "OnAgentStart")
}

func (h *interruptHook) OnModelEnd(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
    _, err := h.maybeInterrupt(ctx, info, "OnModelEnd")
    return ctx, err
}

func (h *interruptHook) maybeInterrupt(ctx context.Context, info *hook.AgentInfo, triggerPoint string) (context.Context, error) {
    if h.flag != nil && h.flag.Load() {
        // v2 强制 InterruptError.Query 非 nil——"用户暂停按钮"场景填虚拟 Query，Question 用于前端展示
        return ctx, &compose_v2.InterruptError{
            Reason: "user requested pause",
            Query: &compose_v2.AskQuery{
                Question: "已挂起，请点继续或换方向。",
                // ⚠️ 必须自填：直接构造 InterruptError 时框架**不补** ID，空 ID 当场
                // 被 validateInterrupt 拒（%w ErrRunAborted）。这条路上的 ID 只作前端
                // 标签、不参与取答案；要取答案走 compose_v2.Ask。
                InterruptID: "pause",
            },
            Data: map[string]any{
                "agent":   info.Name,
                "round":   info.Round,
                "trigger": triggerPoint, // SSE 可观测到 OnAgentStart vs OnModelEnd 哪个触发
            },
        }
    }
    return ctx, nil
}
```

**关键设计**：
- **双触发点（OnAgentStart + OnModelEnd）** —— 仅 OnAgentStart 时长 LLM 调用期间点暂停要等下一轮才生效；OnModelEnd 让 LLM 调用一结束即触发，`Data.trigger` 标哪个触发点
- **InterruptID 必须自填** —— 直接构造 `InterruptError` 这条路框架不补 ID（兜底已删，空 ID
  当场 `ErrRunAborted`）。填一个自己认得的常量标签即可，前端从 SSE 拿到它用作 Resume 的 key
- **Data 含 active agent / round / trigger** —— 前端可展示"在 'researcher' agent 第 3 轮（OnModelEnd）被暂停"
- **Hook 挂到所有 sub-agent 和 planner 的 hooks 链上** —— 任何嵌套位置都能响应

### 11.2 完整 HTTP 流程

```bash
# 1. 启服务（mock 模式不依赖真 LLM）
/tmp/web -mock -addr :19999 &

# 2. 启 chat 后台（mode=plan 走 PlanAgent；不传或 mode=normal 走单 BasicAgent chat_entry）
curl -sN "http://localhost:19999/api/chat?q=plan+budget+report&mode=plan" > /tmp/chat_intr.log &
sleep 0.5

# 3. 抓 sessionID
SID=$(grep -o 'session_id":"[^"]*' /tmp/chat_intr.log | head -1 | cut -d'"' -f3)

# 4. 立即触发 hook interrupt（用户按"暂停"按钮）
curl -X POST "http://localhost:19999/api/interrupt?session_id=$SID"

# 5. SSE 流收到 interrupted 事件
grep '"type":"interrupted"' /tmp/chat_intr.log
# data: {"type":"interrupted","data":{"checkpoint":{"frontier":["model"],"status":"pending"},
#        "query":{"question":"已挂起，请点继续或换方向。","interrupt_id":"7cae64d9786ea7cf"},
#        "reason":"user requested pause","session_id":"..."}}

# 6. Resume continue（用户点"继续"按钮）
curl -sN "http://localhost:19999/api/chat?session_id=$SID&action=continue"
# data: {"type":"final","data":{"content":"[mock chat_entry · Plan mode] 已通过工具完成任务：plan budget report"}}
# data: {"type":"done","data":{"history_size":2}}

# 7. cp 已自动清（cleanupOnSuccess）
curl "http://localhost:19999/api/chat?session_id=$SID"
# → "session not found (likely already completed or never started)"
```

### 11.3 关键验证点

- ✅ `interrupted` 事件含 `query.interrupt_id` + `query.question`（v2 严格契约 — `InterruptError.AllQueries()` 必须非空，单 query 填 `Query` 字段或多 query 填 `Queries`）
- ✅ cp 含 `frontier:["model"]` + `status:"pending"`（顶层 BasicAgent 的 `["model"]` cp 来自 **OnModelEnd** 命中——顶层 `OnAgentStart` 抛 Interrupt 在 graph 启动前 `return` 不产 graph cp，故 §11.1 双触发里的 OnModelEnd 才是产 cp 那条；嵌套深处子 agent 的 OnAgentStart 在父 graph 内执行则会产其他节点名 cp）
- ✅ Resume continue → 出 `final` + `done` 事件，history_size 累积
- ✅ 第二次同 sessionID 请求返 "session not found"（cleanupOnSuccess 工作）

---

## 12. BasicAgent preamble HITL（ISSUE-74 修）

### 12.1 背景：preamble 阶段业务回调

`BasicAgent` 在 LLM 调用前需做 3 件准备工作（"preamble"）：

| 业务回调 | 用途 | 注入 option |
|---|---|---|
| `store.Search(ctx, ns, opts...)` (query 走 `store.WithQuery(...)` SearchOption) | 读长期记忆注入 prompt | `WithStore(st) + WithStoreMemory(ns, &StoreMemoryConfig{...})` |
| `HistoryCompactor(ctx, msgs, keep)` | 历史压缩 | `WithHistoryCompactor(fn)` |
| `SkillBox.ActivateSkill(ctx, name)`（adaptive 模式）| 预激活 skill 注入 sysPrompt | `WithSkills(SkillSource{...})` |

所有 3 个回调都是**业务自定义**——业务可在内部抛 `InterruptError` 做 HITL gate
（典型场景："读私密 store 前用户授权" / "敏感 skill 激活前用户审批"）。

> **toolCall 模式 SkillBox.ActivateSkill 也支持 HITL（R24 audit HIGH-1 fix）**：业务用
> `WithSkills(SkillSource{Options: {ActivationMode: SkillActivationModeToolCall}})`
> 时本调用跑在 toolsNode 内（LLM 调 `activate_skill` 工具触发），抛 InterruptError 走
> tools 节点协议自动 SaveNS cp + Resume 重跑 toolsNode 续跑——抛中断的那次 `activate_skill` 自己会再调一次
> （它没成功），同批已成功的兄弟 tool_call 由节点 memo 重放不重执行（详见 §6.2 / §6.3）。本节后续示例 / idempotent 契约同时适用 adaptive + toolCall 两模式。

### 12.2 已修 bug：原 buildInitialMessages 同步调用导致 cp 不存

**修前（R25 audit HIGH-2 发现）**：

```
BasicAgent.Run → buildInitialMessages(同步业务回调) → runGraph → compose_v2.Run
                    ↓ 抛 InterruptError
                propagate to caller（cp 未存——graph 还没启动）
                    ↓
            caller 调 Resume(sid) → cper.LoadNS 拿不到 cp → HITL 链断
```

**修后（ISSUE-74 方案 A）**：preamble 业务回调下沉到 graph 首段独立节点。

```
BasicAgent.Run → runGraph → compose_v2.Run
                                ↓
              graph: StartNode → preamble → model → tools → ...
                                    ↑
                preamble 节点内调 buildInitialMessages
                抛 InterruptError → graph_loop defer SaveNS 自动存 cp
                Resume 重跑 preamble 拿 ResumeContext 答案续跑
```

### 12.3 protocol 关键点

- **preamble 节点只跑首轮**（StartNode → preamble → model；后续 tools→model 不经
  preamble），逻辑保持现状不变
- preamble 节点产物是**完整 messages 列表**（含 SystemMessage + Memory + history +
  user input），通过 `Write(basicMessagesCh, msgs)` **一次性写入** Append channel
  ——避免 Resume 重跑 preamble 时双重 Append 造成 messages 重复（plan §7.3）
- preamble 里提问走 `compose_v2.Ask`（它按位置维算稳定 ID）；直接构造 `InterruptError` 时
  **必须自填 ID**，框架不补。随后 `recordInterruptBatch` + `SaveNS` cp
- Resume 时 `cm.Restore` 恢复 channel state（含 `basicPreambleInputCh`），
  preamble 节点重跑通过 `compose_v2.GetResumeAnswerText(ctx, askID)` 取答案续跑
- 业务级 hook 时机（8-method 协议）：
  - `OnAgentStart` 整个调用 1 次, 在 `agent_v2.{编排}.Run/Resume` top-level fire (preamble 之前) —
    跟 OnAgentEnd 单一所有权对称, 跟 PlanAgent / GraphAgent / eino ADK 对齐；
    OnAgentStart 入参 messages 修改仅影响 initial input, 不见 preamble 加的 Memory/Skill 注入消息
  - `OnModelStart / OnModelEnd` 每轮在 modelNode 触发（preamble 之后）
  - preamble 节点仅触发引擎 `OnNodeStart/End` 观测 hook

### 12.4 业务实现 idempotent 契约（必读）

业务自定义 `store.Store` 实现 / `HistoryCompactor` / `SkillBox.ActivateSkill`
**必须支持多次调用返同样结果**——preamble 节点在 InterruptError 后 Resume 时**完整重跑**，
业务回调会被再次调用：

```go
// ❌ 错误实现：副作用未幂等
func (s *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
    s.audit.Log("store search")  // Resume 重跑会双重 audit
    return s.cached, nil
}

// ✅ 正确实现：副作用业务自己去重
func (s *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
    if !s.audit.WasLogged(getRequestID(ctx)) {
        s.audit.Log("store search")
    }
    return s.cached, nil
}

// ✅ 正确实现：业务想用 ResumeContext 改写
func (s *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
    if ans, ok := compose_v2.GetResumeAnswerText(ctx, "approve_store"); ok {
        // Resume 路径：业务自决用答案做差异化处理
        return s.searchWithApproval(ans), nil
    }
    return nil, &compose_v2.InterruptError{
        Query: &compose_v2.AskQuery{InterruptID: "approve_store", Question: "approve store search?"},
    }
}
```

### 12.5 端到端示例（业务侧用法）

```go
// 业务自定义 store.Store 实现（HITL gate）
type myStore struct{ *store.InMemoryStore }
func (s *myStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
    if ans, ok := compose_v2.GetResumeAnswerText(ctx, "approve_mem"); ok && ans == "yes" {
        return s.InMemoryStore.Search(ctx, ns, opts...)
    }
    return nil, &compose_v2.InterruptError{
        Reason: "approve_memory_read",
        Query: &compose_v2.AskQuery{
            InterruptID: "approve_mem",
            Question:    "approve memory read?",
        },
    }
}

// 配 BasicAgent
st := &myStore{InMemoryStore: store.NewInMemoryStore()}
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("test"),
    agent_v2.WithModel(myLLM),
    agent_v2.WithStore(st),
    agent_v2.WithStoreMemory(store.NS("users", "u1", "memories"),
        &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
)

// Run 1: store.Search 抛 Interrupt
cper := compose_v2.NewInMemoryCheckpointer()
ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
sess := agent_v2.NewRunSession()
_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
ie, _ := compose_v2.IsInterrupt(err) // 用 ie.AllQueries() 取全部 query（§2.1）

// Resume: 业务侧 UI 拿到用户答案后续跑（用 AllQueries 统一 accessor）
answers := make(map[string]*schema.Message)
for _, q := range ie.AllQueries() {
    answers[q.InterruptID] = schema.UserMessage("yes")
}
events, err := ag.Resume(ctx, sess.RunID,
    agent_v2.WithResumeUserInputs(answers),
    agent_v2.WithResumeSession(sess),
)
final := agent_v2.FinalMessage(events) // 正常拿 final
```

### 12.5.1 嵌套 sub-agent + Store + HITL（ISSUE-84）

业务嵌套 sub-agent 场景 (AgentAsTool / SkillTool fork) 配 Store + HITL gate 时:

```go
// sub-agent store.Search 用 RootSession 上追看父对话历史 (ISSUE-79 半共享 sess)
func (s *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
    sess := agent_v2.GetSession(ctx)
    if sess == nil { return nil, nil }
    parentHist := sess.RootSession().HistoryFor("")
    // 业务 HITL gate 例: 父历史含敏感数据时需用户审批
    if hasSensitive(parentHist) {
        if ans, ok := compose_v2.GetResumeAnswerText(ctx, "approve_nested_mem"); ok {
            if ans == "yes" {
                return s.searchFromHistory(parentHist), nil
            }
            return nil, nil // 拒绝则不注入父历史
        }
        return nil, &compose_v2.InterruptError{
            Query: &compose_v2.AskQuery{
                InterruptID: "approve_nested_mem",
                Question:    "sub-agent 想读父历史 (含敏感数据), 同意?",
            },
        }
    }
    return s.searchFromHistory(parentHist), nil
}

// ⚠️ commitFinal 曾经自查这个 flag 跳过 sub-agent 的 store.Put，**那道闸已删**——
// 它让成员配的 WithStoreMemory 一条也写不进去。防重复写现在靠「写入者维进 ns」
// （顶层写基准 ns、成员写 基准ns/<位置>，本来就不重复）。
//
// 业务后端别再写成 `return nil` 整个跳过，那会把框架的修复抵消掉。按位置给不同待遇：
func (s *MyStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
    if agent_v2.IsSubAgentStoreScope(ctx) {
        // 例如：成员的记忆挂短 TTL / 打个来源标记，而不是丢弃
        return s.putWithShortTTL(ctx, ns, key, value)
    }
    return s.InMemoryStore.Put(ctx, ns, key, value, opts...)
}
```

**关键不变量**:
- `sess.RootSession()` 沿 parentSess 链上追到根 sess (ISSUE-79 半共享 sess 协议)
- ParallelAgent + Store N+1 倍 Put 业务陷阱 + lane events 协议见 `orchestration-patterns.md` §4.5
- `agent_v2.IsSubAgentStoreScope(ctx)` 框架在 **4 处派发点**无条件 push（`runMember` /
  `runDecisionAgent` / `runNestedCall`（RunSubAgent + AgentAsTool + SkillTool fork）/ Handoff
  铸造——前两处在 `member_dispatch.go`、第三处在 `run_subagent.go`、第四处在 `handoff_agent.go`）。⚠️ **框架自己不读它**——防重复写改由「写入者维进 ns」解决，
  详见 `store.md` #4
- HITL gate 在 sub-agent 内抛 InterruptError 时框架 SaveNS cp 物理隔离 (ISSUE-78 hash NS)
- Resume 时 ResumeContext 自动透传到 sub-agent (parent/child ctx 链)

详见 usage-guide.md §3 内 "Store 嵌套 sub-agent 协议（ISSUE-84）" 子节决策矩阵。

### 12.6 三方对照（LangGraph + eino）

ISSUE-74 修法与 LangGraph / eino 的设计对齐：

| 框架 | preamble 业务回调位置 | HITL 协议 |
|---|---|---|
| **crew-go**（修后）| graph 首段 `preamble` 节点 | `InterruptError` + `Resume(WithResumeUserInputs)` |
| LangGraph | `pre_model_hook` 编译为 graph 入口节点（`prebuilt/chat_agent_executor.py:744-746`）| `interrupt()` 函数 + `Command(resume=...)` |
| eino | `BeforeModelRewriteState` 中间件在 graph 首节点（`adk/middlewares/summarization/summarization.go:320`）| `compose.Interrupt` + cp + `BatchResumeWithData` |

三方共识：**preamble 业务回调必须在 graph 内**才能享受 cp round-trip 协议。

详见 [ISSUE-74 跟踪文档](../issue/issue-74-preamble-hitl-cp-gap.md)。

---

## 13. 反模式 + 常见误解

### 13.1 反模式

| ❌ 反模式 | ✅ 正确做法 | 原因 |
|---|---|---|
| 抛 `InterruptError{Query: nil, Queries: nil}` | 至少填 `Query: &AskQuery{Question: "..."}`（单 query）或 `Queries: []*AskQuery{...}`（多 query）| v2 fail-fast——framework graph_loop 不允许 `len(ie.AllQueries())==0` 返普通 error |
| 业务工具内多次提问自己拼 ID | 走 `compose_v2.Ask`（每次调用自己 tick 自己用）| 序号那一维只有正在跑的那段代码知道，第二处重建都是猜 |
| `GetResumeAnswer` 不双重校验 | `if ok && msg != nil { ... }` | caller 传 `map{id: nil}` 时 ok=true 但 msg=nil deref panic |
| Resume 时只传部分 InterruptID 答案 | 业务 OK，但要确认其他 pending 是否需重新中断 | 只答部分时其他 pending 节点 Resume 后继续抛 InterruptError |
| 想「取任意一条答案」（那个 API 已删） | 用 `GetResumeAnswer(ctx, myID)` 按自己的 ID 取；要「用户这轮说了什么」用 `GetResumeUserText` | 取任意一条会吃到**别人的**答案——实测用户批准 A、系统执行了 B，`err == nil` |
| 业务自定义固定 key 跨子图不隔离 | `WithSubgraphIsolatedResumeContext()` 显式隔离；或改用非寻址通道 `WithResumeUserText` | 父子串台答案 |
| 用 panic 表达 HITL | 用 `return nil, &InterruptError{...}` | panic 后 cp 仍写 Pending（frontier 非空时），Resume 会重跑导致再次 panic 死循环；正路径必须 InterruptError |
| Hook 内抛 InterruptError 不填 Reason | 填 `Reason: "user requested pause"` 等明确字符串 | 业务 hook / 审计可按 Reason 路由处理 |

### 13.2 常见误解

| ❌ 误解 | ✅ 真相 |
|---|---|
| "InterruptError 是真错误，应该 log error" | 不是——是控制流信号，应 `IsInterrupt(err)` 分流处理 |
| "Resume 必须传所有 PendingInterrupt 的答案" | 不必——只传部分 OK，其他节点 Resume 后会再抛 InterruptError |
| "ask_user 工具的 InterruptID 业务自己生成" | 不是——`ask_user_tool.go` 内部按 `CurrentInterruptSite(ctx).InterruptID()` 算，业务循环 `for _, q := range ie.AllQueries()` 取 `q.InterruptID`（统一 accessor，见 §2.1）|
| "嵌套子图 Resume 答案要分别传给父子" | 不必——默认 ctx 透传，子图 GetResumeAnswer 看父答案；想隔离才显式 opt-in |
| "Replan 模式所有 agent 都支持" | **错** —— 只 PlanAgent + BasicAgent + GraphAgent 接受；Sequential/Parallel/Loop/Routing/Reflect/Handoff 6 编排显式 fail-fast return error（详见 §5.4 支持矩阵）|
| "fan-out 部分中断时所有分支重跑" | 不——v2 仅重跑失败/中断分支（per-branch delta 自然继承）|
| "InterruptID 业务可以随便填字符串" | 可以，但必须**跨 Resume 稳定 + 同一批内互不相同**（撞了会被当场拒）；推荐直接走 `Ask` |
| "cp 里 PendingInterrupts 是 InterruptError 列表" | 不——是 `[]*AskQuery`（提取 `ie.AllQueries()` 的列表）；不持久化的是 `InterruptError.Reason` / `Data`（`AskQuery` 自身的 per-query `Reason` 字段**仍随 cp 持久化**）|

---

## 关联文档

- [`checkpoint.md`](./checkpoint.md) — cp 与 Resume 的引擎层权威篇（数据结构 / 生命周期 / 寻址 / 并发保护）
- [`channel-engine.md`](./channel-engine.md) — Channel-based Graph 引擎完整设计（BSP / Channel / fan-out / NS）
- [`context-compaction.md`](./context-compaction.md) — 4 层上下文压缩（含 ReactiveCompact retry 与 reset_stream 协调）
- [`usage-guide.md`](./usage-guide.md) — 业务侧用法（写一个 BasicAgent / 编排 / 集成 cp）
- [`migration-history.md`](./migration-history.md) — Stage 1-7 commit 索引 + v2 完整 API 速查
- [`why-channel-based.md`](./why-channel-based.md) — 当初为什么转 channel-based
- [`../description/smoke-test-recipes.md`](../description/smoke-test-recipes.md) — 冒烟测试速查（nested_smoke 12 场景 + plan_chat_web mock）
- [`../description/three-way-comparison/interrupt-resume.md`](../description/three-way-comparison/interrupt-resume.md) — crew-go vs LangGraph vs eino HITL/Resume 三方对照
- 实战 demo：`example/orchestration/plan_chat_web/`（用户暂停按钮 + Resume Continue 完整 HTTP）
- 实战 demo：`example/orchestration/nested_smoke/`（12 场景嵌套 HITL Resume 闭环）
