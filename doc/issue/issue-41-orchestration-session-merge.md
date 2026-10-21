# ISSUE-41 · 编排模式 ForkSession 阻断父 session 观测

**优先级**：中
**状态**：PR 1 已完成；PR 2 已**合并进 ISSUE-46**（2026-04-22）
**关联**：ISSUE-40（持久化）已完成；PR 2 的"并发分支 Interrupt + Resume"由 ISSUE-46 超步快照机制统一承载
**来源**：plan_chat demo 冒烟时发现 `[history size: 0]` 恒成立（2026-04-17）

---

## ⚠️ PR 2 的最终归宿

原 PR 2 计划的 `Checkpoint.LaneEvents map[string][]*AgentEvent` 方案**未采纳**。经 2026-04-22 设计审视（见 ISSUE-46 "后续审视 A"），PR 2 的实际需求是"并发分支内抛 InterruptError 时 Resume 仅重跑该分支"——这与 ISSUE-46 的 `superstepSnapshot` + `branchDelta` 机制**是同一个问题不同失败形式**。

最终采纳的合并方案：

1. `branchDelta` 扩展为三态（成功 / `ErrMsg` / `Interrupt`），Interrupt 在其中作为一等公民
2. `runBatch` 对 Interrupt 与业务错误走**同一路径**——不 cancel 兄弟，统一走 `superstep_snapshot` 存档
3. Resume 时 `rerunFailedBranches` 过滤条件扩展为 `ErrMsg != "" || Interrupt != nil`，重跑分支内部靠 `session.Events`（ISSUE-40）恢复上下文
4. ParallelAgentNode / FanOut / SendFrom / Routing 的阻塞型扇出**不在本次合并范围**——仍保留 `checkParallelInterruptResults` 的"暂不支持"包装；它们未实现分支级快照，若未来有需求再单独设计

落地代码：
- [compose/checkpoint.go](../../compose/checkpoint.go) `buildBranchDelta` / `rerunFailedBranches`
- [compose/superstep.go](../../compose/superstep.go) `runBatch`（删除 `checkParallelInterrupt` 兜底块 + Interrupt cancel）
- [compose/superstep_resume_test.go](../../compose/superstep_resume_test.go) `TestSuperstep_PartialFailure_InterruptBranch`

原 PR 2 描述（含 `Checkpoint.LaneEvents` 设想）保留在本文档下方供对照，**不再实施**。

---

## 一句话结论

线性编排（Sequential / Plan / Reflect / Routing / Handoff）在调用子 agent 时都 `ForkSession` 起新子 session，子 agent 的 events 全写到子 session，**父 session 从头到尾没被任何人写过**——即使没 Resume 也拿不到历史。这个"隔离"对并发模式必要，对线性模式是副作用。

修复采用**方向 2 · eino 风格 laneEvents**，一次到位；方向 1（简单 MergeFrom）作为被否决的折衷方案保留在文末供对照。

---

## 设计不变式（整篇设计的指导原则）

> **"并行"在编排层的语义是"声明无依赖"，不是"同时跑"。**

"同时跑"只是运行时实现的一个可能选择——框架还可以串行跑、乱序跑、重试单路跑。这些自由度的前提是**编排者承诺"分支彼此独立"**。

由此推出三条硬约束，API 实现必须主动捍卫：

1. **Lane 单 writer 无锁** —— 写端只能是拥有 lane 的那个 goroutine。读者要么等 join、要么不存在。不靠加锁解决并发读写，靠**禁止并发读写**解决
2. **运行中 sibling 不可见** —— A 无法从自己的 ctx 拿到 B 的 session 句柄。想"A 读 B"连指针都拿不到，在 API 层面就写不出
3. **values 快照而非共享** —— values 是业务变量，fork 时浅拷贝一次。想跨分支传数据必须走 event / return value，让数据流在编排图上明面可见

这三条合起来保证：**"我以为可以偷偷让 A 看 B"这种代码根本写不出来**。想表达依赖，去改编排（Sequential、Graph 拓扑），不要破坏并发契约。

> **关键取舍：加锁是止损方案，禁止依赖才是干净架构。**
> "单 writer 无锁"不是性能优化的结果，是架构选择的结果——我们通过禁止"并发读写"这种场景，根本不需要锁。一旦允许 sibling 互读，就必须加锁保护 events 切片、values map，性能和正确性都要重新论证。不如在 API 层面直接堵死。

### 工业界同类设计的共识

"并发原语的正确性靠独立性保证，而不是靠加锁解决"——这条不是我们发明的，是跨领域的共识：

| 系统 | 并发原语 | 独立性约束 |
|---|---|---|
| MapReduce | map 阶段 | 各 mapper 完全独立，任意 shuffle 顺序 |
| Airflow / DAG 调度 | task 并行 | 必须声明依赖边才看得到上游输出 |
| eino laneEvents | lane 单 writer 无锁 append | 并发分支互相不读 |
| Go errgroup | 并发 goroutine | 共享只能经 channel，不经内存 |
| React concurrent mode | 并发渲染多棵树 | 各树独立 state |

共同模式：**"想共享"→ 走显式通道（channel / message / upstream edge）**，**"想独立"→ 并发原语根本不暴露互访 API**。加锁作为"不独立但硬要并发"的止损出现在个别场景，但不是干净架构的首选。

不变式的负面推论（不支持的场景及替代方案）：

| 想做什么 | 支持吗 | 正确做法 |
|---|---|---|
| A 运行中实时读 B 的 events | ❌ | 改 Sequential(B, A)，或 Graph 里连一条 B→A 的边 |
| A 完成后 C 基于 A 结果启动、同时 B 还在跑 | ❌ | Graph：A→C 连边，B 单独并行 |
| 父节点（join 后）读 A+B 合并 events | ✅ | join 自动合并，下一节点的 session.Events() 就能看到 |
| 并发分支共享 values | ❌ | fork 时快照；想共享结果走 event/message |

---

## 现象

`example/orchestration/plan_chat/main.go:85`：

```go
events, err := ag.Run(ctx, sess, schema.UserMessage(line))
// ... 对话若干轮
fmt.Printf("[history size: %d]\n", len(sess.History()))
// 输出永远是 [history size: 0]
```

同一个 `sess` 跨多轮复用，PlanAgent 内部 planner/executor/critic 都真实产出了消息，但 `sess.History()` 恒空。

影响面不止 plan_chat：所有依赖父 session 做观测、记忆、hook 的场景都失效。

---

## 为什么会这样

### ForkSession 在编排层被一刀切使用

跨全部编排模式的 session fork 点：

| 文件 | 模式 | fork 场景 |
|---|---|---|
| [compose/sequential.go:79](../../compose/sequential.go) | Sequential | 每步子 agent |
| [compose/plan.go:274](../../compose/plan.go) | Plan | planner |
| [compose/plan.go:337](../../compose/plan.go) | Plan | 每步 executor |
| [compose/reflect.go:186](../../compose/reflect.go) | Reflect | actor |
| [compose/reflect.go:252](../../compose/reflect.go) | Reflect | evaluator |
| [compose/reflect.go:296](../../compose/reflect.go) | Reflect | reflector |
| [compose/routing.go:466](../../compose/routing.go) | Routing | 分派到的专家 |
| [compose/fanout.go:168](../../compose/fanout.go) | FanOut | 每个扇出分支 |
| [compose/helpers.go:200](../../compose/helpers.go) | Parallel | 每个并行分支 |
| [compose/send.go:141](../../compose/send.go) | SendFrom | 每次发送 |

统一的注释理由：

> 每个分支使用独立 session（ForkSession），防止并发分支通过 session.History()/AddEvent 相互污染（H1）

### 这个理由对哪些模式成立

| 模式 | 是否并发 | ForkSession 必要性 |
|---|---|---|
| Parallel | ✅ 并发 | 必要——多分支同时写一个 session 会冲突 |
| FanOut | ✅ 并发 | 必要 |
| SendFrom (超步并发) | ✅ 并发 | 必要 |
| Sequential | ❌ 线性 | 不必要——过度保护 |
| Plan (planner/executor) | ❌ 线性 | 不必要 |
| Reflect (actor/evaluator/reflector) | ❌ 线性 | 不必要 |
| Routing | ❌ 选择一路 | 不必要 |
| Handoff | ❌ 一次一个 | 不必要（当前已不 fork，但也没明确 merge） |

当前实现把"防并发污染"的药方开给了"没并发"的病，导致：
- 并发模式：fork 对、但没 merge 回父（events 永远丢失，不仅是父 session 空）
- 线性模式：fork 错、父 session 完全失聪

### ForkSession 做了什么

[agent/session.go](../../agent/session.go) 里 `ForkSession`：

```go
func ForkSession(parent *RunSession) *RunSession {
    s := NewRunSession()
    // parent.values 浅拷贝给 s，但 events 完全不传
    return s
}
```

子 session 拿到的是**全新的、空 events 的 session**。父子之间**从不回流**——子 agent 产生的 events 永远停留在子 session，子 session 被 GC 后就没了。

---

## 参考设计：eino 的 laneEvents

eino `adk/runctx.go` 的三件套（整篇设计的蓝本）：

```go
type runSession struct {
    Values     map[string]any       // values（eino 里共享指针，我们改成快照）
    Events     []*agentEventWrapper // 已 commit 的主干（带锁）
    LaneEvents *laneEvents          // 当前 lane 的 in-flight 事件（无锁）
}

type laneEvents struct {
    Events []*agentEventWrapper     // 单 writer 无锁追加
    Parent *laneEvents              // 嵌套并发的链表
}

type agentEventWrapper struct {
    *AgentEvent
    TS int64                        // AddEvent 时打戳，join 按 TS 稳定排序
}
```

三个关键操作：

| eino 函数 | 作用 | 用在何处 |
|---|---|---|
| `updateRunPathOnly` | 只改 RunPath，session 完全共享 | 线性模式（Sequential/Loop） |
| `forkRunCtx` | 创建 child lane，Parent 指向当前 lane | 并发分支入口 |
| `joinRunCtxs` | 收集 children lanes，按 TS 排序 commit 到父 lane / main | 并发分支出口 |

关键洞察：**父子不是两个 session，而是一个 session 的两个视图**。读取 events 时沿 `Parent` 指针穿透到顶；写入时只写当前 lane。嵌套并发（Plan 里套 Parallel）天然支持——`commitEvents` 自适应父 lane：父还在 lane 里就 append lane，父是 main 就 append main。

---

## 选择方向 2 的理由

### 1. 线性模式的改动两方向一样，不存在"先做方向 1 不白费"

两个方向对线性模式的改法都是"去掉 ForkSession 直接传 state.Session"——只差一行。方向 2 的**增量成本**只在并发分支的 lane 结构。

### 2. 并发+中断+Resume 组合 100% 会来

我们已经有 `ParallelAgent` / `FanOut` / `SendFrom` 和 `InterruptError`。方向 1 的 `MergeFrom` 在**并发分支中断**场景会很难看：
- merge 时机放在"所有分支完成"—— 中断时 child 还没 merge，checkpoint 要存下 child session 本身（多个 session 序列化）
- merge 时机放在"单分支完成"—— child 中断时没完成不该 merge，但怎么区分"正常未完成"和"中断未完成"

方向 2 的 `Checkpoint.LaneEvents map[laneID][]Event` 原生表达"哪些 lane 正在中断、各自到哪里了"。eino 的 `parState.SubAgentEvents`（[workflow.go:526-529](../../../eino/adk/workflow.go)）就是现成方案。

### 3. 嵌套并发场景（Plan 里套 Parallel）

方向 1 每一层都要手动 merge：外层忘调就丢 events，测试能过（父看到一部分）但不易察觉。

方向 2 的 `commitEvents` 自适应父 lane 语义，嵌套多深都对——函数签名就决定了行为，代码写错可能性低。

### 4. eino 已经踩过的坑我们可以直接跨过去

lane 链表、TS 稳定排序、commit 自适应父 lane——自己摸索至少一个 PR 周期；eino 把这几个细节固化在 130 行 `runctx.go` 里，直接抄结构就行。

---

## 实现：PR 1（本 issue 核心闭环）

### agent 包

#### `agent/event.go`

- `AgentEvent` 加 `CreatedAt int64` 字段（JSON tag `created_at,omitempty`）
  - `AddEvent` 时若为 0 自动填 `time.Now().UnixNano()`
  - 保留用户显式设值的可能（便于测试注入时序）

#### `agent/session.go`

结构扩展：

```go
type RunSession struct {
    ID     string
    mu     sync.RWMutex
    events []*AgentEvent       // 主干（committed）
    lane   *laneEvents         // 当前 lane，nil 表示在 main 上
    values map[string]any
}

type laneEvents struct {
    events []*AgentEvent
    parent *laneEvents          // 链表，父 lane 不可变
}
```

三个新 API：

```go
// ForkLane 为并发分支创建 child session。
// 共享 Events 主干（读穿透），lane 链表追加一层；values 浅拷贝（不共享指针）。
// child 单独拥有 lane.events，父 goroutine 不得写。
func ForkLane(parent *RunSession) *RunSession

// JoinLanes 收集多个 child 的 lane.events，按 CreatedAt 稳定排序后
// commit 到父 session。父若在 lane 里就 append 父 lane，在 main 就 append main。
// 多次调用幂等要求：调用方保证每个 child 只被 join 一次。
func JoinLanes(parent *RunSession, children ...*RunSession)

// Events() / History() 读取时沿 lane.parent 链穿透，
// 返回 main + 当前 lane + 祖先 lane 的并集（committed + in-flight）。
```

`AddEvent` 的新语义：

```go
func (s *RunSession) AddEvent(e *AgentEvent) {
    if e.CreatedAt == 0 {
        e.CreatedAt = time.Now().UnixNano()
    }
    if s.lane != nil {
        // 在 lane 里：无锁追加（单 writer 契约）
        s.lane.events = append(s.lane.events, e)
        return
    }
    s.mu.Lock()
    s.events = append(s.events, e)
    s.mu.Unlock()
}
```

**`ForkSession` 保留但标记 Deprecated**——给第三方可能的外部调用留过渡，编排层全部换成 `ForkLane`。

### compose 包

#### 线性模式：直接传 `state.Session`，不 fork

```go
// sequential.go:79 改动前
out, err := retryRunAgentNodeMsg(ctx, agent.ForkSession(state.Session), n.Name, n.Agent, in, retryCfg)

// 改动后
out, err := retryRunAgentNodeMsg(ctx, state.Session, n.Name, n.Agent, in, retryCfg)
```

涉及文件：
- [compose/sequential.go:79](../../compose/sequential.go)
- [compose/plan.go:274,337](../../compose/plan.go)
- [compose/reflect.go:186,252,296](../../compose/reflect.go)

**注**：routing.go 里 classifier / 单 target / fallback 路径本来就传 `state.Session`（238/265/282/383/417/430），无需改动。唯一的 `ForkSession` 在 routing.go:466，但它在 goroutine 里并发 fan-out（多 target 场景），归类属于并发模式——归入下方 PR 1 的"并发模式"改动。

#### 并发模式：`ForkLane` + `JoinLanes`

```go
// helpers.go / fanout.go 伪代码
children := make([]*agent.RunSession, len(branches))
for i, br := range branches {
    children[i] = agent.ForkLane(state.Session)
    go func() {
        defer wg.Done()
        br.Run(agent.WithSession(ctx, children[i]), input)
    }()
}
wg.Wait()
agent.JoinLanes(state.Session, children...)
```

涉及文件：
- [compose/fanout.go:168](../../compose/fanout.go)
- [compose/helpers.go:200](../../compose/helpers.go)
- [compose/send.go:141](../../compose/send.go)
- [compose/routing.go:466](../../compose/routing.go)（fan-out 并发分支，多 target 场景）

#### Parallel + Interrupt 的 PR 1 策略

PR 1 **不做 lane 持久化**。检测到未 join 的 lane 里出现 `InterruptError`，直接返回 `ErrNotSupported`，说明"并发分支内的 interrupt 在 PR 2 支持"。理由：
- ISSUE-40 的 checkpoint 只存 main `Events`，PR 1 不扩这个面
- 现有 example / 测试没有 parallel+interrupt 组合用例，阻断它不影响闭环
- 留给 PR 2 一次性设计 `Checkpoint.LaneEvents`

### 测试

新建 `compose/session_merge_test.go`：

| 测试 | 断言 |
|---|---|
| `TestLinearSharedSession` | Sequential 两步后 `len(sess.History()) == 2`，内容按执行顺序 |
| `TestPlanParentObserves` | Plan 一轮后父 session 含 planner + 所有 step 的产出 |
| `TestParallelMergedByTS` | Parallel 两分支交替 emit，join 后按 CreatedAt 有序 |
| `TestNestedParallelCommitToLane` | Plan 里嵌套 Parallel，内层 join 写到外层 lane 不直接写 main |
| `TestValuesSnapshotIsolation` | A 写 `values[k]`，B 读不到；fork 前值两者都能读 |
| `TestLaneSingleWriterNoRace` | 100 分支 × 10 events，`go test -race` 通过 |
| `TestParallelInterruptReturnsNotSupported` | PR 1 的临时保护：parallel 内 interrupt 返回明确错误 |

`example/orchestration/plan_chat/main.go`：冒烟验证 `[history size: N]` 随轮数单调递增。

### 文档

- `CLAUDE.md`：RunSession 条目补"线性编排共享父 session；并发编排 ForkLane+JoinLanes，events 按 CreatedAt 归并"
- `doc/description/design.md`：新增"并行契约与 lane 合并"章节，正文包含本 issue 的"设计不变式"
- `agent/session.go` godoc：`ForkLane` / `JoinLanes` 必须写明 single-writer 契约

---

## 实现：PR 2（**已废弃——合并进 ISSUE-46**）

> **以下为 2026-04-22 之前的 PR 2 设想，已不再实施。保留供对照，当前实现见顶部"PR 2 的最终归宿"。**

仅当实际出现 parallel+interrupt 用例时再做。改动面：

1. `agent.Checkpoint` 加 `LaneEvents map[string][]*AgentEvent` 字段（laneID 方案见下）
2. `marshalGraphState` 在检测到未 join lane 时遍历所有 child session 收集其 lane.events
3. `ResumeGraph` 按 laneID 灌回对应 child 的 lane
4. Parallel/FanOut 在分支级支持 per-branch resume（已完成的不重跑）

laneID 方案候选：
- `branch_index`（最简单，但嵌套会冲突）
- `runPath + branch_index` 路径字符串（eino 的做法接近这个）
- 显式 UUID（编排层维护映射）

PR 2 启动时再决定，现在只留接口占位。

---

## 潜在陷阱（实施时逐一核查）

### 陷阱 1 · H1 污染是否会回来

原注释说"防止并发分支通过 session.History()/AddEvent 相互污染"。线性模式共享父 session——**多步 Sequential 的后一步能看到前一步的 events**，这是 H1 想避免的吗？

**不是**。H1 针对**并发**多分支同时读/写 session 互踩；线性顺序执行下，后一步本来就该能看到前一步的产出（不然怎么构成"执行历史"）。当前 Sequential 靠 `state.Messages` 透传消息但 session.History() 空——这是实现偏差，不是设计意图。

### 陷阱 2 · ReAct 子 agent 自己也写 session（H5 重复）

`agent/react.go` 的 BasicAgent.executeRound 通过 ctx 里的 session `AddEvent`。线性模式共享 session 后，父 session 直接看到每轮 ReAct 的中间事件——这是**期望行为**（traceHook 依赖这点）。

但要核查：**每个编排模式是否也在手动 AddEvent**？如果有，移除以避免 H5 重复。

已确认的参照点：[compose/handoff.go:159](../../compose/handoff.go) 注释说"不在此处额外 AddEvent：BasicAgent.reactLoop 已写入 OutputEvent + ExitEvent"。其他模式按此原则检查。

### 陷阱 3 · Reflect 的 actor/evaluator 是否该共享 session

Reflect 的 actor 产出 → evaluator 评估 → (不满意则) reflector 反馈。三者是**线性执行**的，按 PR 1 的一刀切策略全部共享父 session。

观察点：
- **actor**：需要看历史，共享合理
- **evaluator**：只关心"这次要评的答案"，共享会看到过多 ReAct 中间噪声——但它的输入本来就应该由编排层裁剪（input 过滤），不该靠 session 隔离解决
- **reflector**：同 evaluator

**决策**：Reflect 三角色统一共享父 session，上下文裁剪交给 input 构造逻辑。若日志噪声确实影响 LLM 表现，再考虑"让 evaluator/reflector 单独 ForkLane 但不 Join"——本质是"只读、不回流"的新语义，留待需求实际出现。

### 陷阱 4 · `ForkSession` 的现有调用者

全仓搜索 `ForkSession` 调用点，确保只在编排层用，且全部迁移到 `ForkLane`。第三方扩展点若有使用，保留 `ForkSession` 旧 API 做兼容层，内部转调 `ForkLane`（行为差异：旧 API 不回流，新 API 回流——需文档标注）。

### 陷阱 5 · Lane 单 writer 契约的运行时守护

契约要求"单 lane 单写 goroutine"。实际可能踩坑的场景：
- 用户自定义 hook 在另一个 goroutine 里调 `AddEvent`（比如异步埋点）
- BasicAgent.reactLoop 里某个 tool 内部起了 goroutine 还持有 session

**策略**：不做运行时强制（加锁会破坏无锁设计的初衷），靠文档 + race test 暴露。在 `AddEvent` godoc 明确写"调用点必须与 lane 拥有者同 goroutine"。

---

## 与 ISSUE-40 的关系

- ISSUE-40（已完成）：events 在父 session 里，但 checkpoint 没存，Resume 后丢——**存储层问题**
- ISSUE-41（本 issue，PR 1）：events 根本没写进父 session——**编排层问题**
- ISSUE-41 PR 2：并发分支的 in-flight lane events 如何持久化——**存储层 × 编排层组合**

两个 issue PR 1 修完之后：
- 运行期 `sess.History()` 有数据（本 issue PR 1）
- Resume 后 `sess.History()` 依然有数据（ISSUE-40）
- 但 Resume 时若中断发生在并发分支内部，lane 里的事件会丢（待 PR 2）

---

## 验收

### PR 1 必过

1. `plan_chat` 冒烟：每轮 `[history size: N]` 随轮数单调递增
2. `TestLinearSharedSession`：Sequential 两步后 `len(sess.History()) >= 2`
3. `TestParallelMergedByTS`：Parallel 分支 join 后按 CreatedAt 有序
4. `TestNestedParallelCommitToLane`：嵌套并发按 lane 穿透 commit
5. `TestValuesSnapshotIsolation`：并发分支 values 互不可见
6. `go test -race ./compose/... ./agent/...` 通过

### PR 1 允许不过（PR 2 目标）

- 并发分支内触发 InterruptError 后的 Resume 恢复

---

## 附录：方向 1（被否决的折衷方案）

方向 1 的做法是：线性模式不 fork；并发模式保留 ForkSession，子分支返回后调用 `parent.MergeFrom(child)` 按 CreatedAt 归并。

被否决原因：

| 维度 | 方向 1 | 方向 2 |
|---|---|---|
| 线性模式改动 | 相同（1 行） | 相同（1 行） |
| 并发正常路径 | MergeFrom 调用 | ForkLane/JoinLanes 调用 |
| 并发+中断 | child session 需独立序列化，或丢进度 | Checkpoint.LaneEvents 原生表达 |
| 嵌套并发 | 每层手动 merge，易漏 | commit 自适应父 lane 自动 |
| 并发读 events | 需对 parent 加锁 | lane 单 writer 无锁 |
| 对 eino 设计的借鉴度 | 低 | 直接复用 |

方向 1 在"闭环 plan_chat"这一目标上**改动更小**，但几乎一定会在下个迭代重写——既然线性改动完全一致，不如一步到位。
