# Channel-based Graph 引擎详细设计

> crew-go 唯一的图执行引擎——**channel-based BSP（Bulk Synchronous Parallel）**，参考
> LangGraph Pregel + Google Pregel 设计。所有 9 编排（BasicAgent / Sequential /
> Parallel / Loop / Routing / Reflect / Handoff / Plan / GraphAgent）都编译为同一
> 个 `compose_v2.Graph` 执行，cp / Resume / HITL / Hooks / 4 层压缩等横切能力一次实
> 现全模式受益。

> **文档定位**：channel-based 引擎完整设计文档。**新 session 看这一份建立完整引擎层
> 认知**；业务侧用法（写一个 BasicAgent / 编排）见 [`usage-guide.md`](./usage-guide.md)；
> 4 层压缩看 [`context-compaction.md`](./context-compaction.md)；HITL Resume 完整协议
> 看 [`hitl-resume.md`](./hitl-resume.md)（待写）。

## 目录

1. [全景](#1-全景)
2. [核心抽象：Channel / Reducer / NodeFunc](#2-核心抽象channel--reducer--nodefunc)
3. [graphLoop 主循环：BSP 4 阶段](#3-graphloop-主循环bsp-4-阶段)
4. [Builder API + 编译产物](#4-builder-api--编译产物)
5. [Checkpoint 协议](#5-checkpoint-协议) → 已迁至 [checkpoint.md](checkpoint.md)
6. [Resume 协议](#6-resume-协议) → 已迁至 [checkpoint.md](checkpoint.md)
7. [Fan-out 完整不变量](#7-fan-out-完整不变量)
8. [嵌套子图：NS 命名空间体系](#8-嵌套子图ns-命名空间体系)
9. [类型注册 + 序列化协议](#9-类型注册--序列化协议)
10. [Hooks 集成（Agent + Orch 两层）](#10-hooks-集成agent--orch-两层)
11. [错误处理 / 重试 / 断路器](#11-错误处理--重试--断路器)
12. [9 编排到 Graph 的编译产物](#12-9-编排到-graph-的编译产物)
13. [关键不变量速查](#13-关键不变量速查)
14. [反模式 + 常见误解](#14-反模式--常见误解)

---

## 1. 全景

### 1.1 一句话定位

crew-go 的图引擎是**channel-based BSP（Bulk Synchronous Parallel）超步执行模型**：
每个超步并发执行一组节点，所有节点写入的 channel 在 barrier 同步点统一 reduce
合并，下一帧从合并后的 channel 状态计算新 frontier。**isEnd 与 next-tasks 计算合并
在同一函数**，根除 v1 时代「已完成 graph 被 ctx 拦下」类 bug。

```
┌─────────────────────────────────────────────────────────────────┐
│  超步 N:                                                         │
│    1. computeNextTasks(g, cm) → (frontier, isEnd)               │
│    2. if isEnd → return（终止条件优先于 ctx 检查）              │
│    3. ctx.Err() check                                           │
│    4. 并发 execute(frontier) → 收集所有 ChannelWrite           │
│    5. cm.ApplyAll(writes) ← BSP barrier，reducer 统一合并       │
│  超步 N+1:                                                       │
│    ... 重复                                                       │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 与 LangGraph / eino 的关系

| | crew-go | LangGraph (Python) | eino (Go) |
|---|---|---|---|
| 核心模型 | Channel + Reducer + BSP | Channel + Reducer + BSP（Pregel）| Channel + 字符串字段映射 |
| 类型安全 | `Channel[T]` Go 泛型，编译期保护 | TypedDict / Pydantic 运行时校验 | `field_mapping{from, to}` 字符串，运行时 cast |
| 节点签名 | `NodeFunc func(ctx, Channels) ([]ChannelWrite, error)` | `node(state) → state_update` | `Runnable[I, O]` 4 模式 |
| cp 寻址 | `(sessionID, ns)` 二级 key | `(thread_id, checkpoint_ns)` | 单 sessionID（无 ns）|
| Fan-out 部分失败 | 仅重跑失败/中断分支（channel 自然 incremental）| **整超步重跑**（详见 three-way-comparison/error-handling.md）| 整超步重跑 |

参考 LangGraph Pregel + 修正 eino 类型擦除妥协，详见 [`why-channel-based.md`](./why-channel-based.md)
+ [三方对照决策记录](../description/three-way-comparison/)。

### 1.3 包内文件分工

```
compose_v2/
├── channel.go              Channel[T] 泛型定义 + 4 reducer 工厂
├── channel_manager.go      ChannelManager + anyChannel 类型擦除
├── graph.go                Graph 编译产物 + 节点/边数据结构 + Graph.ID()（GraphID 防 cp 串台，无独立 ctx 透传文件）
├── graph_loop.go           Run 主入口 + computeNextTasks + ApplyAll
├── send.go                 SendFrom / SendDirective fan-out 协议
├── access.go               Get/Write/Has 包级泛型访问函数
├── interrupt.go            InterruptError + AskQuery + IsInterrupt
├── checkpointer.go         Checkpointer 接口 + InMemoryCheckpointer + Checkpoint 结构
├── ns.go                   NS 操作 API（JoinNS/SplitNS/IsNSInSubtree/ValidateNodeIDForNS）
├── ns_ctx.go               graphNS ctx 透传（WithGraphNS/GetGraphNS）
├── serialization.go        类型注册表 + tagged JSON（业务跨 cp 边界存自定义类型）
├── safe_truncate.go        AdjustIndexToPreserveAPIInvariants（v1 兼容保留，4 层压缩生产路径不调用——见 context-compaction.md §配对感知切割）
├── message_group.go        Token 估算 + GroupMessagesByAPIRound（4 层压缩用）
├── circuit_breaker.go      AutoCompact 用断路器
├── retry.go                RetryConfig + SimpleRetry + ExponentialBackoff + RunWithRetry[T]
├── hooks.go                OrchestrationHooks 接口（5 回调）+ NoOp/ChainOrchHooks + WithOrchHooks ctx 透传（业务级 hook.Hooks 在 hook 包，非此文件）
├── recording_hooks.go      RecordingHooks test helper
├── errors.go               1 sentinel + 3 typed error（ErrNoSubgraphCheckpoint / MaxIterationsError / MaxRoundsError / GraphNodeError）
└── subgraph.go             AddSubgraphNode 头等子图节点

# 7 个低阶编排 builder（BasicAgentBuilder / SequentialAgentBuilder / … / HandoffAgentBuilder）
# 已从 compose_v2 引擎包移入 agent_v2（agent_v2/*_builder.go）；plan 无低层 builder（inline 进 PlanAgent）。
```

依赖方向：`schema ← hook ← compose_v2 ← agent_v2`（compose_v2 实测只 import `hook` + `schema`，不依赖 `model`/`tool`——后两者在 agent_v2 层引入，见 [architecture-overview.md](./architecture-overview.md) §2）。

---

## 2. 核心抽象：Channel / Reducer / NodeFunc

### 2.1 Channel[T]：泛型类型安全的命名 slot

`Channel[T]` 是跨节点共享数据的命名 slot。每个 channel 有：
- **名字**（字符串，cp 序列化 key）
- **类型 T**（编译期）
- **reducer**（多写入合并策略）
- **零值 zero T**（未写入时的默认）

```go
// compose_v2/channel.go
type Channel[T any] struct {
    n  string
    fn Reducer[T]
    z  T
}

type Reducer[T any] func(prev, next T) T
```

**关键设计**：用户拿到的是 `*Channel[T]` **变量引用**（不是字符串），编译期类型保护
+ IDE 重命名安全。对照 eino 用 `field_mapping{from: "tokens", to: "total"}` 字符串
映射的妥协，crew-go 的 `compose_v2.Get(chs, TokenCh)` 编译期类型推断 + 重构无碍。

### 2.2 4 个 Reducer 工厂（覆盖 90% 场景）

```go
// LastValue：覆盖（最常用）
func LastValue[T any](name string) *Channel[T]
//   reducer: (prev, next) → next
//   适用：单写者（**同一超步里两个成员同时写它 ⟹ 引擎当场拒**，
//         因为「后写的赢」在并发下没有良定义的结果）
//   例：当前节点 ID / 路由决策 / 最新摘要
//   真要「谁最后写谁赢、我不在乎是谁」：Custom(name, func(_,next T) T { return next }, zero)

// Append：追加 slice（累加历史）
func Append[T any](name string) *Channel[[]T]
//   reducer: (prev, next) → make 新 backing 后依次拷 prev、next
//            ⚠️ **不是** append(prev, next...)——那个会写进 prev 的底层数组，见 §7.5
//   适用：消息流 / 已执行步骤 / 事件序列
//   例：messages / executed_steps / chunks
//   ⚠️ reducer 有三条调用路径：提交点（持锁串行）+ 条件边的叠加视图（不持锁、按成员并发）
//      + 子图读路径 reduce(baseline, own)（不持锁、N 个扇出实例并发）。
//      纯 reducer（不改 prev/next）两条都安全；就地改 prev 的实现在后者上是 data race。

// Sum：int 累加
func Sum(name string) *Channel[int]
//   reducer: (prev, next) → prev + next
//   适用：计数 / 累计 token / 累计延迟
//   例：token_usage / retry_count

// OnceFlag：单调 set——已 set true 之后写入静默忽略（不 panic）
func OnceFlag(name string) *Channel[bool]
//   reducer: (prev, next) → if prev { return prev }; return next
//   适用：终止信号 / 一次性副作用（多分支并发 set 安全）
//   例：__plan_done__ / __end_signal__

// Custom：自定义 reducer
func Custom[T any](name string, reducer Reducer[T], zero T) *Channel[T]
//   适用：上述 4 种不够时（如 BarrierChannel "等所有上游写完才放下游过"）
```

**何时用哪个 reducer**：

| 业务场景 | 用什么 |
|---|---|
| 当前节点决策结果 | `LastValue[T]` |
| 历史累加（消息 / 步骤）| `Append[T]` |
| 计数器 | `Sum` |
| 终止 / 一次性信号 | `OnceFlag` |
| 复杂合并（去重 / 排序 / barrier）| `Custom[T]` |

### 2.3 类型擦除的内部接口 anyChannel（不导出）

```go
// compose_v2/channel_manager.go
type anyChannel interface {
    name() string
    reduceAny(prev, next any) any
    zeroAny() any
    // ... 内部方法
}
```

ChannelManager 持有异构 channel 用 `map[string]anyChannel`——必须类型擦除（Go 泛型
限制：`Channel[int]` / `Channel[string]` 是不同类型不能放进 `map[string]Channel[T]`）。

**为什么不导出**：用户只见 `Channel[T]` + `Get/Write` 包级泛型函数，类型保护始终在
调用点。`anyChannel` 是框架内部实现细节，避免用户绕过类型保护用 `any` 操作 channel。

### 2.4 NodeFunc 签名

```go
// compose_v2/graph.go
type NodeFunc func(ctx context.Context, chs Channels) ([]ChannelWrite, error)

// compose_v2/access.go（Channels 只读视图 + ChannelWrite 写指令 + 包级 Get/Write/Has）
type Channels struct {
    cm *ChannelManager  // ChannelManager 类型导出，仅 cm 字段不导出（业务通过 Get/Write/Has 包级函数访问）
}

type ChannelWrite interface {
    channelName() string      // 目标 channel 名
    valueAny() any            // 裸 value——喂给 reducer 的 next；也是 cp 序列化路径用的那份（PA-H1 DeferredSends 把 SendDirective.Payload 编码为 (channel, taggedValue) 对）
    notRegisteredErr() error  // 找不到那个 channel 时该报的错——留给实现自己说（子图 export 要指出「父图缺声明」）
}
// 归并本身不在接口里：三种实现曾各自「查表 → 读 prev → reduce → 写回」抄了三份，
// 现在统一在 ApplyAll 里做一次（见不变量 4）。
```

**节点签名只有 4 件事**：
- 入参 `ctx`：取消 / 超时 / 透传值
- 入参 `chs`：读 channel 当前状态
- 出参 `[]ChannelWrite`：要写哪些 channel（实际写入由框架在 barrier 同步点完成）
- 出参 `error`：业务错 / `InterruptError`（HITL）/ ctx 错

**关键不变量**：节点是**纯函数**——不直接 mutate state，所有产出包成 ChannelWrite
返回。框架在 barrier 同步点统一 reduce + 持久化进 cp。这个约束是 channel-based 模型
"无锁 fan-out / 自然 incremental cp / 易测试"的根本前提。

### 2.5 Get / Write / Has：包级泛型访问函数

```go
// compose_v2/access.go

// Get 读 channel 当前值。ok=false 表示未写入过（返 zero）
func Get[T any](chs Channels, ch *Channel[T]) (T, bool)

// MustGet 读 channel，未写入 panic
func MustGet[T any](chs Channels, ch *Channel[T]) T

// Has 判断 channel 是否已写入
func Has[T any](chs Channels, ch *Channel[T]) bool

// Write 构造 ChannelWrite（不立即写入，由框架在 barrier 时调用 .apply）
func Write[T any](ch *Channel[T], value T) ChannelWrite
```

**为什么用包级函数不用方法**：Go 泛型规则——**方法不能引入新类型参数**（[issue#49085](https://github.com/golang/go/issues/49085)），只能用包级泛型函数让 T 在调用点推断：
```go
// ✅ 推断出 T=int
score, ok := compose_v2.Get(chs, scoreCh)  // scoreCh 是 *Channel[int]

// ❌ 不能写：方法泛型不允许
chs.Get[int](scoreCh)  // compile error
```

### 2.6 NodeFunc 完整示例

```go
var (
    MessagesCh = compose_v2.Append[*schema.Message]("messages")
    LastOutCh  = compose_v2.LastValue[*schema.Message]("last_output")
    EndCh      = compose_v2.OnceFlag("__end__")
)

func modelNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    msgs, _ := compose_v2.Get(chs, MessagesCh)
    out, err := llm.Generate(ctx, msgs)
    if err != nil {
        return nil, err
    }
    writes := []compose_v2.ChannelWrite{
        compose_v2.Write(MessagesCh, []*schema.Message{out}),
        compose_v2.Write(LastOutCh, out),
    }
    if len(schema.GetToolCalls(out)) == 0 {
        // 无 tool call → 终止
        writes = append(writes, compose_v2.Write(EndCh, true))
    }
    return writes, nil
}
```

**与 v1 对比**：v1 时代要写 `state.LastOutput = out` + `state.Messages = append(state.Messages, out)`
直接 mutate state（必须握 mutex），然后 ProcessState[T] 包装 typed local state——
~30 行加锁。v2 直接写 `Write(LastOutCh, out)` 返 ChannelWrite，框架自动加锁 reduce。

---

## 3. graphLoop 主循环：BSP 4 阶段

### 3.1 主入口 Run

```go
// compose_v2/graph_loop.go
func Run(ctx context.Context, g *Graph, cm *ChannelManager, opts ...RunOption) error
```

`Run` 是图执行的**唯一入口**——首次 Run + Resume 都走这里（Resume 由
`WithCheckpointer` + 自动 LoadNS 触发）。

**RunOption 组合**：

| Option | 用途 |
|---|---|
| `WithInitialWrites(ws...)` | 初始 channel 状态（业务把 input 写到 channel）|
| `WithCheckpointer(c)` | 注入 cp 后端启用 cp / Resume |
| `WithSessionID(sid)` | 显式 sid（默认空，从 cp 或调用方指定）|
| `WithRunNS(ns)` | 显式 graph ns（默认空——根图）|
| `WithFreshRun()` | 强制忽略已有 cp 从头跑（不 auto-load）|
| `WithHooks(hooks)` | OrchestrationHooks 注入（5 hook 时机）|
| `WithResumeAnswers(map)` | Resume 时塞 user 答案（HITL）— **底层 RunOption**；上层业务用 `agent_v2.WithResumeUserInputs` |
| `WithResumeFrom(cp)` | 显式从某 cp 恢复 Run（替代 auto-load 路径） |

**ctx 透传 helper（不是 RunOption）**：

| Helper | 用途 |
|---|---|
| `WithCheckpointerCtx(ctx, c)` | ctx 透传 cp 后端（嵌套子 agent 自动继承）|
| `Graph.ID()` | graph 身份（防 cp 串台，ISSUE-50 P0）——`cp.GraphID = g.ID()` 直读，框架自动；WithGraphID ctx pair 废弃已删 |
| `WithGraphNS(ctx, ns)` | 显式子图 ns（嵌套自定义 GraphAgent 用）|
| `WithOrchHooks(ctx, h)` | ctx 透传 OrchHooks（嵌套子 agent 自动继承）|

**跨 Run 的会话历史**不在 cp 层（cp 单次 Run 临时态，成功即清）——用 `agent_v2` 的会话历史层（`WithHistory`，按 `compose_v2.WithThreadIDCtx` 声明的会话身份组织，append-only 留存）。详见 §5.8。

**单节点最大访问次数**（防死循环）：编译期通过 `GraphBuilder.WithMaxVisit(n)` 设，不是 RunOption。

### 3.2 主循环时序

```
Run(ctx, g, cm, opts...) {
    // 0. 入口前置：Checkpointer / sessionID / graphNS push ctx；ResumeContext 早就绪
    // 1. Resume 路径：LoadNS → cm.Restore(cp)（恢复 channels + frontier + PendingInterrupts）
    // 2. 应用 initialWrites（首次 Run 注入 input）：cm.ApplyAll(opts.InitialWrites)

    // 3. 初始 frontier 在循环【外】算一次（StartNode 出边）
    frontier, isEnd, _ := computeNextTasks(ctx, g, []NodeID{StartNode}, cm)
    if isEnd { return nil }             // StartNode→EndNode 极端图直接成功
    cm.updateFrontier(frontier)

    // 4. 超步循环（真实顺序——grep graph_loop.go::Run）
    for {
        if err := ctx.Err(); err != nil { return err }    // 帧首：只查 ctx（done 上轮末已判）
        if len(frontier) == 0 { return errFrontierEmpty } // 空 frontier 守卫
        for _, id := range distinctNodeIDs(frontier) {    // visits++ / maxVisit 防御（按 NodeID 去重，
            if visits[id]++; visits[id] > maxVisit { ... } //   否则同 Target 扇出 N 次直接把计数推 N 格）
        }

        res := executeFrontier(ctx, g, frontier, cm, hooks) // 并发执行 frontier（per-branch delta）

        if res.SingleWriterConflictErr != nil { return ... } // 单写入者冲突：**任何提交之前**拦下

        if res.FirstErr != nil {                          // 失败/中断：先整批 record（撞 ID 当场拒）
            cm.recordInterruptBatch(allQueriesOf(res.AllInterrupts))
            narrowFrontierTo = res.FailedBranches
        }

        // ★ 下一帧与 payload **在提交之前纯计算**算好——出边由各成员在自己的执行里算完，
        //   assembleFrontier 只做去重 + 判 End。这是把它们收进一个临界区的前提。
        if res.FirstErr == nil {
            _, deferredSends, completedTargets := cm.peekCompletedSiblings()
            nextFrontier, isEnd = assembleFrontier(res.Targets + completedTargets)
            res.SendDirectives = deferredSends ++ res.SendDirectives   // deferred 前置
            for _, d := range res.SendDirectives {        // Target 并入 nextFrontier
                nextFrontier = append(nextFrontier, member(d))          // payload 攒进 dirWrites
                dirWrites = append(dirWrites, d.Payload...); isEnd = false
            }
        }

        // ★ 提交：按路径拆成两个方法，各自把自己那条路的字段改动**全部**收进一个临界区
        if res.FirstErr != nil {
            // 写入落地 / frontier 窄化 / 代际推进 / 答案兑付 / 成功兄弟记账
            cm.commitSuperstepPartial(res.SuccessWrites, narrowFrontierTo,
                res.SucceededBranches, res.ConsumedAnswers, res.SendDirectives, res.Targets)
            return res.exitErr()   // 终局失败 > 坏存档 > 档没写成 > 中断 > 第一个业务错
        }
        // 写入落地 / 代际推进 / 答案兑付 / 待办清空 / **frontier 前移** / **payload 第二次 apply**
        //   （payload 那次单独 apply：并进第一次的话它失败会把成功成员的写一起回滚）
        cm.commitSuperstepAdvance(res.SuccessWrites, res.SucceededBranches,
            res.ConsumedAnswers, nextFrontier, dirWrites)

        if isEnd { return nil }                           // ★graph 跑完直接返（不进下一轮 ctx 检查）
        frontier = nextFrontier
        cm.updateFrontier(frontier)
    }
}
// cp save / DeleteSubtree 统一在 Run 顶层 cleanup defer 按 runErr 分流（见 checkpoint.md §3.2/§3.3）——
//   循环体内任何 return 只把 error 冒泡，不直接 SaveNS。
// 节点成功完成后 runNodeWithHooks 递增 visit（失败/中断不增）保 InterruptID 稳定。
```

### 3.3 核心不变量

#### 不变量 1：isEnd 优先于 ctx

真实循环结构（`graph_loop.go::Run`）：`computeNextTasks` + `isEnd` 在循环体**末尾**，
`ctx.Err()` 在下一轮循环体**顶部**——二者跨 iteration 边界形成「isEnd 优先 ctx」：

```go
for {
    if err := ctx.Err(); err != nil { return err } // 帧首：只查 ctx（done 上轮末已判）
    // ... executeFrontier → recordInterrupt → ApplyAll（见 §3.2）...
    nextFrontier, isEnd, err := computeNextTasks(...) // 帧末：算下一帧 + isEnd 合并判定
    if isEnd { return nil }   // graph 跑完直接返，不进入下一轮帧首的 ctx 检查
    frontier = nextFrontier
}
```

**机制**：graph 跑完时 `isEnd` 在帧末 `return nil`，**不会**进入下一轮帧首的 ctx 检查
——所以「已完成的 graph 即便 deadline 已过也不被 ctx 拦下」。v1 时代 `plan_chat_web`
bug「已完成 graph 被 ctx 拦下」正是 v1 把 ctx 检查放在 isEnd 之前。
[`why-channel-based.md`](./why-channel-based.md) §1 直接 trigger 详述。

channel-based 模型下 `computeNextTasks` 同时返 (frontier, isEnd)——一处判定，永远
不可能不一致。

#### 不变量 2：BSP barrier（统一同步点 reduce）

```go
// 所有节点的 writes 收集完毕，一次性 ApplyAll
if err := cm.ApplyAll(writes); err != nil { return err }
```

**为什么必须 barrier**：reducer 是合并多写者的中央点——必须等所有 frontier 节点都返
回再合并，否则后写者看不到先写者的产出（违反 BSP 模型）。LangGraph / eino / Pregel
都是这个设计。

#### 不变量 3：节点是纯函数（不直接 mutate 状态）

节点返 `[]ChannelWrite`，不调 `cm.write` 直接写。所有写入由框架 `ApplyAll` 在 barrier
统一执行。这是无锁 fan-out / 自然 incremental cp / 测试友好的根本前提。

#### 不变量 4：一批写要么全落地要么一条都不落地

```go
// compose_v2/channel_manager.go::applyAllLocked
// 规划：跑业务 reducer 算新值，全留在 staged 里——**一个字节都不写回 cm**
staged := make(map[string]any, len(writes))
for _, w := range writes {
    if w == nil {
        continue
    }
    name := w.channelName()
    ch, ok := cm.channels[name]
    if !ok {
        return w.notRegisteredErr() // 走到这儿时 cm 还是进来时那样
    }
    prev, ok := staged[name]
    if !ok {
        prev = cm.values[name] // 同 channel 多写时按顺序链式归并
    }
    staged[name] = ch.reduceAny(prev, w.valueAny())
}

// 提交：纯赋值。没有查表、没有业务代码、没有可返回的错
for name, v := range staged {
    cm.values[name] = v
}
```

**为什么分两段**：归并要跑业务 reducer，它会 panic、也会被 `runtime.Goexit` 掐断
（业务在 reducer 里写一句 `logger.Fatal` 就是）。归并和写回挤在一起时，「炸在第 3 条」
意味着前 2 条已经进了 cm，得靠一个回滚栈捞回来——而回滚栈只在 `recover` 分支里跑，
Goexit 那条路 `recover()` 返 nil 就整个跳过，半批直接进存档、Resume 重跑整超步让
Append/Sum 双计。分开之后那个窗口在结构上不存在：**失败的代价只是「什么都没发生」**。

⚠️ **reducer 里不许回调 cm 的任何方法**：归并跑在写锁内，而 Go 的 RWMutex 不可重入
——`Get` / `Snapshot` / 再来一次 `ApplyAll` 都会当场死锁。`Reducer[T]` 的签名不收 ctx
正是为此。另注：reducer 的调用次数**不保证「每次提交恰好一次」**——整批里靠后那条
不合法时，靠前那些已经归并过了，只是没提交。

#### 不变量 5：ctx 必须有 deadline（业务约束）

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err := compose_v2.Run(ctx, g, cm, opts...)
```

**没 deadline 的死锁场景**：fan-out 派生 N 分支，其中 1 个 NodeFunc 死循环不 return
→ batch path best-effort 等所有兄弟跑完 → 全卡住。**ctx deadline 是唯一外部解药**。
CLAUDE.md "交付纪律" 强制约束。

### 3.4 节点的错误 / panic / ctx / Interrupt 行为表

| 触发 | 框架处理 | cp 是否 save |
|---|---|---|
| `ctx.Done() / DeadlineExceeded` | 走错误路径 + 整 Run return | ✅ Run defer SaveNS |
| `panic` | `safeNodeRun` recover 转 `node panic recovered: ...` 普通 error | ✅ 同业务 error（frontier 非空时 SaveNS，档位同上）——业务**应避免用 panic 表达 HITL**（Resume 拿 panic 时不一致 channel 状态，§13.5 #7）|
| `*compose_v2.InterruptError` | 框架识别 + 收集 PendingInterrupts | ✅ Run defer SaveNS |
| 业务普通 error | 静默上抛 caller | ✅ Run defer SaveNS，**标 `Status=Failed`**——⚠️ **照样会被 auto-load 捡起**：闸门读 `HasResumableWork() && BelongsToTask()`，**不读 Status**（2026-08-13 起 Status 只是报告位）。实测：a→b、b 报业务错，同 sid 再 Run ⟹ a 一次不跑、b 从失败点重跑。判据见 [checkpoint.md §3.1](checkpoint.md)|
| 节点正常 return + writes | ApplyAll reduce 进 channel | （超步成功，cp 在下次失败/中断时 save）|

**关键不变量**：
- `InterruptError` + `ctx.Err()` + 业务 error + **panic（经 `safeNodeRun` 转 error）** 都经
  Run defer 走**同一 SaveNS 路径**——panic 不是特例（旧版"panic 不存 cp"已废：panic 转 error
  后与业务 error 同走 `graph_loop.go::Run` defer）
- 但**仅当 frontier 或 PendingInterrupts 非空才 SaveNS**；都空（无中断点的快速失败）skip 不存
  cp（避免存无意义空 cp）—— 故上表"✅ SaveNS"前提是有中断点
- panic 路径虽存 cp，业务**仍应避免用 panic 表达 HITL**——Resume 拿 panic 时可能不一致的
  channel 状态（§13.5 #7）
- ctx cancel 是**外部主动停**——fan-out 兄弟也收到 cancel 信号（共享 ctx），与业务
  error 不传染兄弟有别

---

## 4. Builder API + 编译产物

### 4.1 GraphBuilder 完整 API

```go
// compose_v2/graph.go

func NewBuilder() *GraphBuilder

// 注册 channel（包级泛型函数，因方法不能加类型参数）
func WithChannel[T any](b *GraphBuilder, ch *Channel[T]) *GraphBuilder

// 节点 / 边
func (b *GraphBuilder) AddNode(id NodeID, fn NodeFunc, opts ...NodeOption) *GraphBuilder
func (b *GraphBuilder) AddEdge(from, to NodeID) *GraphBuilder
func (b *GraphBuilder) AddConditionalEdge(from NodeID, cond EdgeCondition) *GraphBuilder

// 配置
func (b *GraphBuilder) WithName(name string) *GraphBuilder           // 调试用
func (b *GraphBuilder) WithMaxVisit(n int) *GraphBuilder            // 单节点最大访问次数（防死循环）

// 编译
func (b *GraphBuilder) Compile() (*Graph, error)
```

**typed nil 协议防护** (R32+ audit²-²⁷ 71 真 HIGH 修):

`AddNode` / `AddConditionalEdge` / `Worker` / `Custom` / `DynamicChannel` / `RegisterChannel`
/ `WithChannel` / `WithCheckpointer` / `Get` / `Write` / `MustGet` / `Has`
等入口全用 `compose_v2/helpers.go::isFrameworkNil` panic-fast 检 Go interface "typed nil"
陷阱. 业务 `var fn NodeFunc; AddNode(id, fn)` typed nil → 立即 panic 显式 framework msg
(不让远 stack nil deref). 详见 [`usage-guide.md` §8](usage-guide.md).

**保留节点 ID 常量**：

```go
const (
    StartNode NodeID = "__start__"
    EndNode   NodeID = "__end__"
)
```

`StartNode` / `EndNode` 不是真正的节点：不进节点表，也不能拿来 `AddNode`（`Compile` 会拒绝），静态边也不能指回 `StartNode`。`AddEdge(StartNode, "first")`
表示首个真正节点是 `"first"`；`AddEdge("last", EndNode)` 表示 `"last"` 跑完到达终点。

### 4.2 EdgeCondition 签名

```go
type EdgeCondition func(ctx context.Context, chs Channels) (NodeID, error)
```

**典型用法**：

```go
b.AddConditionalEdge("model", func(_ context.Context, chs Channels) (NodeID, error) {
    msg, ok := Get(chs, LastOutCh)
    if !ok || msg == nil {
        return "", fmt.Errorf("model node didn't write LastOutCh")
    }
    if len(schema.GetToolCalls(msg)) == 0 {
        return EndNode, nil  // 无 tool call → 结束
    }
    return "tools", nil      // 有 tool call → 继续
})
```

返 `(EndNode, nil)` 路由到终点节点——graph 终止。（v2 已删除 v1 时代的 `EndChannel` 概念，统一通过 NodeID = `EndNode` 路由表达终止。）

### 4.3 完整最小例：BasicAgent ReAct（手写不依赖 BasicAgentBuilder）

```go
var (
    MessagesCh = compose_v2.Append[*schema.Message]("messages")
    LastOutCh  = compose_v2.LastValue[*schema.Message]("last_output")
)

func modelNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    msgs, _ := compose_v2.Get(chs, MessagesCh)
    out, err := llm.Generate(ctx, msgs)
    if err != nil {
        return nil, err
    }
    return []compose_v2.ChannelWrite{
        compose_v2.Write(MessagesCh, []*schema.Message{out}),
        compose_v2.Write(LastOutCh, out),
    }, nil
}

func toolsNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    last := compose_v2.MustGet(chs, LastOutCh)
    var results []*schema.Message
    for _, tc := range schema.GetToolCalls(last) {
        out := executeTool(ctx, tc)
        results = append(results, out)
    }
    return []compose_v2.ChannelWrite{
        compose_v2.Write(MessagesCh, results),
    }, nil
}

bld := compose_v2.NewBuilder().WithName("react")
bld = compose_v2.WithChannel(bld, MessagesCh)
bld = compose_v2.WithChannel(bld, LastOutCh)
g, err := bld.
    AddNode("model", modelNode).
    AddNode("tools", toolsNode).
    AddEdge(compose_v2.StartNode, "model").
    AddConditionalEdge("model", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
        last, _ := compose_v2.Get(chs, LastOutCh)
        if last == nil || len(schema.GetToolCalls(last)) == 0 {
            return compose_v2.EndNode, nil
        }
        return "tools", nil
    }).
    AddEdge("tools", "model").
    Compile()

cm := compose_v2.NewChannelManager()
err = compose_v2.Run(ctx, g, cm,
    compose_v2.WithInitialWrites(
        compose_v2.Write(MessagesCh, []*schema.Message{schema.UserMessage("hello")}),
    ),
)
```

**v2 vs v1 复杂度对比**：v1 BasicAgent ReAct（`buildBasicAgentGraph`）~150 行 typed
state + ProcessState 锁配合；v2 等价实现 ~50 行 NodeFunc + 4 行 builder。

### 4.4 5 类节点（按用法分类，实现层都是 NodeFunc）

虽然底层只有 `NodeFunc`，但实际场景按功能分 5 类：

| 类型 | 用途 | 例 |
|---|---|---|
| **业务节点** | 跑业务逻辑（LLM 调用 / 工具执行 / 规划）| `modelNode` / `toolsNode` |
| **路由节点** | **跑路由决策**（调 Router/classifier → 写 target channel）；下游 AddConditionalEdge 的 cond **纯读**该 channel 选 NodeID | RoutingAgent 的 `classify` |
| **聚合节点** | 等所有上游写完 channel 再触发（Append + len 判断 / Custom barrier reducer）| Parallel merger / Plan replan |
| **子图节点** | `AddSubgraphNode(id, subGraph, opts...)` 嵌入子图作头等节点 | PlanAgent exec / ReflectAgent actor |
| **Hooks 节点** | 不产生 ChannelWrite 只 emit OrchHooks 事件（如审计 / metric）| audit / metric collection |

### 4.5 子图作头等节点：AddSubgraphNode

```go
// compose_v2/subgraph.go
func (b *GraphBuilder) AddSubgraphNode(id NodeID, sub *Graph, opts ...SubgraphOption) *GraphBuilder
```

子图作为节点嵌入父图——子图独立 channel 命名空间，cp 物理写到父 store 的
`(parentSID, parentNS/childID)` ns 路径。详见 §8 NS 命名空间体系。

---

## 5. Checkpoint 协议

> **本节已迁至 [checkpoint.md](checkpoint.md)**——cp 的数据结构 / 生命周期 / 寻址 / 并发保护由
> 那一篇权威承载。原内容对 `NodeStates` / `Epoch` / `TaskID` / `Status` 三档 / `FrontierMember`
> 等一轮新增语义已 stale（且原文还在论证「不设失败态」），故**整体迁出而不是就地补**——就地补会
> 留下第二份会漂的拷贝。
>
> **引擎侧只需知道**：cp 按 `(sessionID, ns)` 二级 key 存取（嵌套物理分离见 §8）；`cm.Restore(cp)`
> 恢复 channel state + frontier + PendingInterrupts；正常完成时 `Run` 出口自动删子树。

## 6. Resume 协议

> **本节已迁至 [checkpoint.md §7](checkpoint.md)**。
>
> **引擎侧只需知道**：`Run` 没有独立 Resume 入口——给了 checkpointer + 同 sessionID 即 auto-load
> （按**内容**捡：`HasResumableWork() && BelongsToTask()`，Failed 档同样捡），或 `WithResumeFrom(cp)` 显式递入；嵌套时每层各自 `LoadNS`，按
> `(sessionID, ns)` 自然 drill-down，不需要"是不是我"的判断函数。Resume 恢复的是 **state 不是
> event**（`Checkpoint` 没有 Events 字段）。

## 7. Fan-out 完整不变量

### 7.1 Fan-out 范式

v2 fan-out 由 `SendFrom` + `SendDirective` 协议实现——节点内部主动调 `SendFrom(ctx, directives...)`
让框架在下一超步派生 N 分支：

```go
// compose_v2/send.go
type SendDirective struct {
    Target  NodeID          // 目标节点 ID（必须已在 graph 注册）
    Payload []ChannelWrite  // 一批 ChannelWrite，下一超步入口归并进**共享** channel 状态
                            // ——下一超步所有节点都看到，**不是 target 的私有入参**：
                            // 同 target 扇出 N 次时 N 条 payload 塌成一份，而实例拿不到
                            // 自己是第几个 ⟹「同 target + 每实例不同输入」今天没有正确写法
                            //（不同输入用不同 target；取舍见 doc/code/todo.md backlog）
                            // 空 Payload 等价于"只调度不传参"
}

// SendFrom 通过 ctx 把 directives 提交给框架——非返回 ChannelWrite，节点内部直接调
func SendFrom(ctx context.Context, directives ...SendDirective)
```

**业务很少直接用 SendFrom**——9 编排里 ParallelAgent / RoutingAgent / PlanAgent 内部
都封装好了。直接用场景：自定义 GraphAgent 想做动态 fan-out。

**fan-out 分支不分裂 ns/cp**：引擎 `SendFrom`/`executeFrontier` 派生的所有并发分支跑在
**同一个 graph ns、同一个 ChannelManager**——框架给每个派生实例铸一枚 `InstanceID`
（不透明 token，随 frontier 持久化、跨 Resume 稳定；**不是**「出现顺序 0..N-1」，早期的
`branchIdx` 已被它取代，因为部分失败时 frontier 缩成失败子集、位置会重排），用于算
InterruptID 防 ask_user 串台，**不** push 分支 ns、**不**建 per-branch 独立 cp。
部分失败走**单 cp** + `CompletedFrontier`/`DeferredSends`（§7.3）。
（⚠️ `JoinFanoutNS` **不是**「与引擎 fan-out 无关」：`AddSubgraphNode` 就用
`JoinFanoutNS(parentNS, id, fanoutIdx)` 给 SendFrom 派生的子图实例分子 ns——那是**子图**这一
层的 cp 分离，跟上面说的「分支不分裂 ns」不矛盾：分支自己不分，分支里嵌的子图分。）

### 7.2 所有 fan-out 编译为图原生超步 frontier

**统一**：v2 fan-out 不是新概念——`computeNextTasks` 算出 frontier 长度 ≥ 2 即超步并
发；fan-out 只是"业务主动指定 frontier 内容"的一种方式。

**调度器对待 fan-out / 普通超步无区别**：

```
普通超步：frontier=["model"] → 执行 model
fan-out 超步：frontier=["worker1", "worker2", "worker3"] → 并发执行 3 worker
聚合超步：frontier=["aggregator"]（所有 worker 写完 channel 后路由到 aggregator）
```

### 7.3 Per-branch delta + 仅重跑失败/中断分支

v1 时代为支持"fan-out 部分失败 Resume 只重跑失败分支"加了 `superstep_snapshot` per-branch
delta hack——本质上是把 GraphState 整体 marshal 的设计补丁化成 per-branch incremental。

v2 channel-based 模型下**这是天然能力**——每个分支只写自己的 ChannelWrite，框架按
分支 incremental 收集 writes 进 cp。Resume 时：
- 成功分支的 writes 已在 cp.Channels 里（ApplyAll 已合并）→ 不需要重跑
- 失败/中断分支的 writes 没合并 → 重跑该分支

**实现位置**：`compose_v2/graph_loop.go::executeFrontier` 收集 errs 时按分支分类——
成功分支立即 apply 进 cm、失败分支不动 cm（保持 barrier 之前的状态）。

**发散 fan-out 的补充（PA-H1）**：当成功分支与失败分支走【不同出边】（A→X、B→Y）时，仅
"成功分支 writes 在 cp.Channels" 不够——Resume 重跑失败成员 B 成功后，成功成员 A 的后继 X
会永久丢失。故 cp 额外记 `CompletedFrontier`（成功成员 ID）+ `DeferredSends`（其 SendFrom），
Resume 时这些成员参与同超步**出边计算**但不重跑 NodeFunc（见 §5.1）。

### 7.4 Fan-out 失败语义表

| 触发 | 兄弟分支 | 整体超步结果 | cp save |
|---|---|---|---|
| 业务 error | **不 cancel**，继续跑完 | superstep 错（**不聚合**：返 `FirstErr`，按 frontier 索引序取第一个；要看每个失败分支各自的原因走 `OnNodeEnd`）| ✅ Run defer SaveNS（per-branch delta 自然进 cp.Channels） |
| panic（safeNodeRun 转 error） | 不 cancel 继续跑完 | 同上 | ⚠️ panic 后 frontier 非空时仍 SaveNS Pending —— Resume 重跑会再次 panic 死循环；业务避免用 panic 表达 HITL |
| ctx cancel | **兄弟也收到 cancel**（共享 ctx） | superstep 整体错 | ✅ |
| `InterruptError`（HITL） | 不 cancel 继续跑完 | superstep 整体挂起 | ✅（含 PendingInterrupts，**多分支中断全部收集** R10-D 修） |

**Best-effort 等兄弟跑完的设计动机**：
- 已花的 LLM tokens 不浪费——成功分支产出存进 cp，Resume 时直接 replay
- 失败分支可仔细处理（Replan / Continue / Abort），不影响成功分支
- ctx cancel 是**外部主动停**（与业务 error 不同），必须传染兄弟

**避免 best-effort 死锁**：必须设 ctx deadline——业务 NodeFunc 不返回时 deadline 是
唯一外部解药。CLAUDE.md "交付纪律" 强制约束。

### 7.5 Append reducer no aliasing（R9 C3）

**问题**：fan-out 多分支并发写同一 `Append[T]` channel 时，naive 实现可能：

```go
// ❌ 错误实现（aliasing）
func (cm *ChannelManager) appendReduce(prev, next []T) []T {
    return append(prev, next...)  // 可能复用 prev 的底层数组
}
```

prev 是 cp 持久化的状态、`append` 复用底层数组让两个超步共享内存——后续修改 prev 数组
会污染 cp。

**v2 修复**：

```go
// ✅ 正确：no aliasing
func (cm *ChannelManager) appendReduce(prev, next []T) []T {
    out := make([]T, 0, len(prev)+len(next))
    out = append(out, prev...)
    out = append(out, next...)
    return out
}
```

每次 reduce 都新分配 backing array，prev / out 完全独立。

### 7.6 PendingInterrupts 深拷贝（R9 C4）

类似问题：fan-out 多分支同时抛 InterruptError 收集时，浅拷贝让 cp 与 cm 共享同一
`[]*AskQuery` slice——后续修改其一污染另一。

**R9 C4 实际修复**：`PendingInterrupts()` getter 返深拷贝（含 Options slice + map 引用类型）防 caller mutate 污染 cm；`recordInterrupt` 直接 append、Snapshot 是浅拷贝（`make`+`copy` 新 slice 装同 `*AskQuery` 指针，mutate 元素 struct 仍回污染 cm——实证 `serialization.go` `Snapshot` + getter `channel_manager.go` `PendingInterrupts`）。

### 7.7 JoinFanoutNS：给「同一个名字的多个实例」各分一棵 cp 子树

`JoinFanoutNS` **不是引擎 `SendFrom` fan-out 本身的机制**（`SendFrom` 派生的并发分支共享
同一 graph ns + 同一 cm，见 §7.1）。它给**嵌套的子运行**分 cp 子树——当同一个名字在同一个
父 ns 下会有多个实例时，每个实例一棵：

- **子图节点被 `SendFrom` 投递多次**（引擎自己，`compose_v2/subgraph.go`）：按实例下标派生。
- **`agent_v2` 的嵌套调用**（`RunSubAgent` / `AgentAsTool` / skill fork，共用
  `agent_v2/run_subagent.go` 的派生函数）：槽恒为 `JoinFanoutNS(parentNS, name, hash(身份))`
  （哈希的低 63 位，`index` 是 int64，与平台字长无关），身份 = 位置 + 这是哪一次调用：
  - 位置：调用方所在的节点与相位（节点体 / 某个图钩子 / 某条条件边，见
    `compose_v2.SubCallScope`），加实例维——所在节点是 `SendFrom` 实例 → 实例下标；在某个
    tool_call 里 → tcID + 批内下标 + tools 节点访问序号（ISSUE-78；模型给的 tool_call ID 跨轮、
    批内都可能重复，只用 ID 时第 2 轮会撞上第 1 轮的槽）；两者兼有（`SendFrom` 实例节点里业务又
    自己起 tool_call 并发）→ 实例下标与 tool_call 一起，不叠 visit（visit 按节点 ID 共享、会被
    静态兄弟推高，实例续跑时身份会漂）；都不是 → 静态重访 visit；
  - 这是哪一次调用：输入内容 + 同一份输入在本次执行里的第几次；或 `agent_v2.WithCallKey`
    给的 key；全历史输入（且没有自定义 schema）的 `AgentAsTool` 与 skill fork 只看位置（输入由框架现拼、续跑时可能
    不同）；声明了 `agent_v2.WithRerunnable` 的再带上调用所在分域的重做代际。

这些实例是**兄弟**，子树互不包含：`DeleteSubtree` 只沿 `/` 往下删（§8.5），一个实例成功
清理时不碰另一个实例还挂着的存档。

```go
// compose_v2/ns.go
func JoinFanoutNS(parent, branchNodeID string, index int64) string {
    base := JoinNS(parent, branchNodeID); return base + "#" + strconv.FormatInt(index, 10)  // 根 ns 无前导 /
}
// 例：JoinFanoutNS("", "worker", 1) → "worker#1"；SendFrom 实例那种 index 是发号器的值，
// agent_v2 嵌套调用那种是身份的 fnv64a 哈希的低 63 位（与平台字长无关）——都不是出现顺序
```

生产调用点：`compose_v2/subgraph.go`、`agent_v2/run_subagent.go`（skill fork 也经它派生）。

**与引擎 SendFrom fan-out 的区别**：引擎 `SendFrom` 派生的并发分支共享 cm + graph ns，部分
失败靠**单 cp** 的 `CompletedFrontier`/`DeferredSends` 记成功成员（§5.1 / §7.3）；
`JoinFanoutNS` 给的是**嵌套的子运行**（子图节点的实例、每一次嵌套调用）各自一棵独立 cp
子树——两者层级不同、互不冲突。

---

## 8. 嵌套子图：NS 命名空间体系

### 8.1 NS 是什么

NS（namespace）是 cp 物理寻址的位置标识，与 sessionID 共同构成 `(sessionID, ns)`
二级 key 定位 cp。

**设计原则**：
- ns 是层级路径，用 `/` 分隔（如 `"research/researcher_step_0"`：Sequential 的步骤 research 里，
  PlanAgent 第 0 步调成员 researcher）。**只有图占 ns 段，节点不占**——存档只按图写，节点里发生
  的中断记在它所在那张图的 cp 上（§8.4）
- 根图 ns = `""`（空字符串）
- 每段至多带一个**实例维后缀**：`#n` = 同一个名字的另一个实例（fan-out 实例；`agent_v2` 的
  每一次嵌套调用都落 `#身份哈希`，不论静态路径还是 tool_call 里）。
  **n 不是批内序号**：`SendFrom` 那半是进程级发号器的当前值、`agent_v2` 嵌套调用那半是
  哈希（见 §7.7），不连续、哈希那半理论上可能碰撞，别拿 `SplitNS` 给的 `Index` 当「第几个」
  用。`@visit` = 静态/cond 成员的**跨完成重访序数**（父循环第 N 轮重进同一节点。首访
  visit=0 不加缀，故 `"sub"` 与 `"sub@1"` 是同一节点的第 1、2 次访问）
- `#` / `@` 段与不带后缀的那段是**同一个节点的兄弟执行**，不是它的下级：`ParentNS` 把它们
  归到同一个节点下（观测用），删除范围不跨它们（`IsNSInSubtree`，§8.5）
- 节点 ID 不允许含 `/`、`#`、`@`（保留给层级分隔与两种实例维后缀）

### 8.2 NS 操作 API 全表

```go
// compose_v2/ns.go

// JoinNS 拼接子节点 ns
func JoinNS(parent, nodeID string) string
//   JoinNS("", "exec") → "exec"
//   JoinNS("exec", "researcher") → "exec/researcher"

// JoinFanoutNS fan-out 分支 ns
func JoinFanoutNS(parent, branchNodeID string, index int64) string
//   JoinFanoutNS("", "worker", 1) → "worker#1"

// JoinVisitNS 静态/cond 成员的重访 ns（visit = 该节点已成功完成的访问次数）
func JoinVisitNS(parent, nodeID string, visit int) string
//   JoinVisitNS("", "sub", 0) → "sub"     // 首访不加缀 = JoinNS
//   JoinVisitNS("a", "sub", 1) → "a/sub@1" // 父循环第 2 轮
// 缺这一维时：父循环第 2 轮重进的子层从头执行、内部维度与上一轮全同 ⟹ 算出
// 与第 1 轮**相同的 InterruptID** ⟹ 被答案账本判为已兑付、终局 ErrRunAborted
// （答案账本跨子图边界之前是第 2 轮的提问被第 1 轮的旧答案静默满足）

// SplitNS 解析 ns 为段列表（含 NSSegment.Kind 区分普通节点 vs fan-out 分支）
func SplitNS(ns string) ([]NSSegment, error)

// ParentNS 取父 ns（`#`/`@` 段归到同一个节点下：ParentNS("a#0") → "a"，观测按节点归组用）
func ParentNS(ns string) string
//   ParentNS("exec/researcher/tools") → "exec/researcher"

// IsNSInSubtree 判断 ns 是否属于 root 这次执行的子树（DeleteSubtree 的删除范围）
func IsNSInSubtree(root, ns string) bool
//   IsNSInSubtree("exec", "exec/researcher/tools") → true
//   IsNSInSubtree("exec", "exec#0")               → false（同一个节点的另一次执行）
//   IsNSInSubtree("", "exec#0")                   → true（空 root = 整个 session）

// NSSubtreeSeparator 划定子树的层级分隔符 "/"（redis Lua 从它注入，不手写一份）
func NSSubtreeSeparator() string

// ValidateNodeIDForNS 校验 nodeID 不含禁字符 /、# 和 @
func ValidateNodeIDForNS(nodeID string) error
```

### 8.3 NS ctx 透传：WithGraphNS / GetGraphNS

```go
// compose_v2/ns_ctx.go (audit follow-up #6: 删 currentNS 维度 + EnterChildNS, 生产 0 caller)
// ⚠️ 业务不要自己拼 childNS——身份 = 位置（调用方所在的节点与相位 + 实例维三选一；fan-out 实例
//    里又起 tool_call 时两维一起进位置）+ 这是哪一次调用（输入内容 + 同一份输入的第几次，或
//    agent_v2.WithCallKey 给的 key），槽号是身份的哈希，判定住在 agent_v2.RunSubAgent 里
//    （见 orchestration-patterns.md §10.4）
func WithGraphNS(ctx context.Context, ns string) context.Context
func GetGraphNS(ctx context.Context) string
```

子图 NodeFunc 通过 `GetGraphNS(ctx)` 取本图 ns；嵌入 sub-agent 时框架自动 push
graphNS 让子 cp 物理写到 `(parentSID, parentNS/childNodeID#<身份哈希>)`（`RunSubAgent` 这条路
恒带 `#` 后缀；编排派发自己的成员是 `parentNS/<子 ns 后缀>`——Sequential / Parallel 是步骤 / 分支
的注册名，Loop 是 `round_N`，Plan 是 `<名字>_step_N`，Routing 是 `branch_<名字>`，Reflect 是
`actor`，决策 LLM 是 `classifier` / `evaluator` 等）。

**业务使用**：

```go
// 6 个编排调自己的成员统一走 runMember（member_dispatch.go），ctx 自动派生——业务不感知
// （Handoff 的转交铸造点形状不同，单独留在原地）
// (BasicAgent ReAct 无 sub-agent; GraphAgent 业务自定义 NodeFunc 推荐走 RunSubAgent helper)

// 必须走 RunSubAgent helper: 一站式 push 3 件 ctx (graphNS + NewSharedChildSession + storeScope)
nodeFunc := func(ctx context.Context, chs Channels) ([]ChannelWrite, error) {
    events, err := agent_v2.RunSubAgent(ctx, "child", childAgent, input)
    // childNS = JoinFanoutNS(parentNS, "child", hash(身份))：身份含位置（调用方所在的节点与相位 +
    // fan-out 下标 / tool_call / 重访 visit 三选一）+ 输入内容与同一份输入的第几次（或 WithCallKey
    // 给的 key），不是裸 JoinNS。父节点重跑时已完成的调用直接拿回记下的结果（SaveStepResult），
    // 不再执行
}

// ⚠️ 手拼 ns 的「手动控制」配方已撤（会漏实例维）: 原因见 orchestration-patterns.md §10.4
```

### 8.4 嵌套示例：Plan 成员里的中断

```
PlanAgent (sessionID=root_sid, ns="")
  ↓ Plan 图（节点 plan / plan_build_input / 各 worker，worker 以成员名命名）
researcher worker 节点（仍在 ns="" 这张图里）
  ↓ runMember 按 "researcher_step_0" push 子 ns，调 researcher（BasicAgent）
researcher 的 ReAct 图 (ns="researcher_step_0")
  ↓ 图内 preamble / model / tools / postamble 节点——节点没有自己的 ns
tools 节点里发生 ask_user 中断
  → researcher 那张图的 Run 出口 SaveNS：(root_sid, "researcher_step_0")，CurrentFrontier=["tools"]，
    PendingInterrupts 含本次 ask_user 的 AskQuery
  → 中断冒泡到 worker 节点，Plan 图的 Run 出口同样 SaveNS：(root_sid, "")，CurrentFrontier=["researcher"]
```

cp store 状态（中断后）——**存档只按图写**，一张图一份：
```
root_sid:
  "":                    → Plan 图的 cp（CurrentFrontier=["researcher"]，含冒泡上来的 PendingInterrupts）
  "researcher_step_0":   → researcher 那张 ReAct 图的 cp（CurrentFrontier=["tools"]，CPStatusPending + PendingInterrupts）
```

Plan 成员的子 ns 带步骤序号（`<名字>_step_<N>`），同一个成员被调度到不同步骤时存档互相隔离。

Resume 时每层 Run 入口按当前 graphNS（ctx push）独立 LoadNS 拿对应 cp 恢复，自然
drill-down 进嵌套子图。详见 [checkpoint.md §7.3](checkpoint.md)。

> 注：v2 `AskQuery` 字段是 `{Question, Options, InterruptID, Reason}`——**没有 NS 字段**，ns
> 信息存于 `Checkpoint.NS`。InterruptID 由 `InterruptSite` 各维度算出（详见 §13.4 不变量 #32
> + `compose_v2/interrupt.go` 的 `InterruptSite.InterruptID`）。

### 8.5 嵌套子图 DeleteSubtree 顺序（R9 D4）

正常完成时 Run 入口 defer 调 `DeleteSubtree(sid, graphNS)` 清**自己这次执行**的子树：
graphNS 自身与经 `/` 往下的后代，单次删全部（各 ns 是独立 cp key，无父子删除顺序依赖）。
同一个节点的 `#`/`@` 兄弟执行不在范围内——它们各自清自己的，否则一次执行的成功清理会删掉
兄弟执行还挂着的存档（判据 `IsNSInSubtree`，2026-09-23 前曾把兄弟算作后代）。

**R9 D4 真实修复**（`compose_v2/subgraph.go`）：subgraph 节点完成时**先构造 export writes、
再 `DeleteSubtree` 清子 cp**——否则父 reducer panic 导致父那批写一条都没提交、而子 cp 已删，Resume 重建子图副作用不幂等。

### 8.6 Subgraph Hooks 透传

子图节点继承父图的 OrchHooks（默认 ctx 透传，详见 §10）。v2 当前**没有**专门的"hooks
隔离子图选项"——业务想隔离时只能在子图入口前清 ctx 里的 hooks。详见 §10 Hooks 集成。

---

## 9. 类型注册 + 序列化协议

### 9.1 问题：channel value 是 `any`

```go
// cp 序列化
type Checkpoint struct {
    Channels map[string]taggedValue  // taggedValue.Data 是 json.RawMessage（marshal 后 typed JSON）
}
```

`json.Marshal(any)` OK——但 `json.Unmarshal(b, &any)` 默认拿到 `map[string]interface{}`
/ `float64` 等退化类型，cast 回原 T 失败。

### 9.2 解决：tagged JSON + 类型注册表

```go
// compose_v2/serialization.go

type taggedValue struct {
    Type  string          `json:"type"`   // 类型标记（如 "compose_v2.PlanStep"）
    Data json.RawMessage `json:"data"`  // marshal 后的 typed JSON
}

// 注册类型让 cp 反序列化时能恢复原类型
func RegisterValueType[T any](typeName string)

// 反序列化时按 typeName 查 factory
func LookupType(typeName string) (reflect.Type, bool)

// 业务 wrapper（高阶 API）
func MarshalTagged(v any) ([]byte, error)
func UnmarshalTagged(b []byte) (any, error)
```

### 9.3 业务侧用法

**框架内置类型**（`PlanStep` / `ExecutedStep` / `Message` 等）框架自动注册——业务不
感知。

**业务自定义类型**（要塞进 channel 跨 cp 边界存）必须显式注册：

```go
type MyState struct {
    Counter int
    History []string
}

func init() {
    compose_v2.RegisterValueType[MyState]("myapp.MyState")
}

// 之后业务可以放心写
var MyStateCh = compose_v2.LastValue[MyState]("my_state")
```

**不注册的后果**：cp.Channels 序列化 OK（直接 JSON marshal），LoadNS 反序列化时
`UnmarshalTagged` 找不到 typeName 注册→ cast 失败 panic。

### 9.4 R8 audit 修的序列化 bug

audit Round 8 专审 cp wire 序列化发现 5 真 bug：
- 类型注册竞态（多 init 同 typeName）→ 加 mutex
- nil any 序列化路径 → 加 nil 守卫
- empty channel state 反序列化 → 跳过不调 Unmarshal
- typeName 含特殊字符 → 显式 escape
- 业务自定义 UnmarshalJSON panic → atomic 回滚（R10-C 后续修）

详见 commit `1599c7d` + R10-C `a9e859c`。

---

## 10. Hooks 集成（Agent + Orch 两层）

### 10.1 两层 Hooks

| 层 | 接口 | 触发位置 | 业务用途 |
|---|---|---|---|
| **AgentHooks** | `hook.Hooks`（8 回调, ISSUE-82 后 9→8 删 OnAgentInit）| agent_v2 高层（per agent.Run）| 业务级观测：模型调用 / 工具调用 / 子 agent 切换 |
| **OrchestrationHooks** | `compose_v2.OrchestrationHooks`（5 回调）| compose_v2 引擎层（per node / per graph） | 引擎级观测：节点开始/结束 / chunk 实时投递 / 测试 helper |

**业务侧通常用 AgentHooks**——OrchHooks 主要用于 chunk 实时投递（前端 SSE）+ 测试。

### 10.2 OrchestrationHooks 接口

```go
// compose_v2/hooks.go
type OrchestrationHooks interface {
    OnGraphStart(ctx context.Context, chs Channels) (context.Context, error)
    OnGraphEnd(ctx context.Context, chs Channels, runErr error) context.Context
    OnNodeStart(ctx context.Context, id NodeID, chs Channels) (context.Context, error)
    OnNodeEnd(ctx context.Context, id NodeID, chs Channels, nodeErr error) context.Context
    OnNodeEvent(ctx context.Context, id NodeID, ev Event) context.Context  // ★ chunk 投递（返新 ctx 链式 propagate）
}

type Event struct {
    Type     string         // "chunk" / "audit" / 业务自定义
    Content  string         // 简化版（Phase 1.1）：仅文本——Phase 1.2 扩展 typed event 支持
    Metadata map[string]any
}
```

**5 hook 时机**：

| Hook | 何时触发 | chs 状态 |
|---|---|---|
| `OnGraphStart` | Run 入口（ApplyAll initialWrites **之前**）| 空（initialWrites 尚未注入，业务在此读 channel 拿 zero/ok=false）|
| `OnNodeStart` | 每节点 PreHandler 之前 | 含上轮 barrier 后状态 |
| `OnNodeEnd` | 每节点 return 之后 | 节点新写入**未** apply |
| `OnNodeEvent` | 节点内主动 emit（chunk）| 实时（barrier 之外） |
| `OnGraphEnd` | Run 出口（无论成功/失败/中断）| 最终状态 |

### 10.3 ctx-aware OrchHooks（v2 现状）

**当前 compose_v2 引擎层**：`WithOrchHooks(ctx, hooks)` / `GetOrchHooks(ctx)` helper
真存在（`compose_v2/hooks.go`），`compose_v2.Run` **优先用 `RunOption WithHooks`，
未传时 fallback `GetOrchHooks(ctx)`**（`graph_loop.go:276`——嵌套 sub-agent.Run 在引擎层
自动继承父 hooks）。优先级：RunOption > ctx OrchHooks > NoOp。

**agent_v2 高层桥接层**：`agent_v2.WithCtxOrchHooks(ctx, h)` / `agent_v2.GetCtxOrchHooks(ctx)`
（`agent_v2/options.go`，**接口与 compose_v2.OrchestrationHooks 不同**——是高层
agent_v2.OrchestrationHooks），由 `extractOrchHooksOrCtx` 内部 helper 在 `BasicAgent.Run`
等高层 wrapper 入口提取 ctx hooks 转 RunOption 注入 compose_v2.Run。

```go
// 高层路径（agent_v2 wrapper）：嵌套子 agent 自动继承父 hooks
hooks := myHighLevelHooks{}
ctx = agent_v2.WithCtxOrchHooks(ctx, hooks)
events, _ := parentAgent.Run(ctx, sess, input)
//   → 父 wrapper 提取 ctx hooks 转 RunOption；子 agent.Run 同样路径自动继承

// 低层路径（直接 compose_v2.Run）：必须用 RunOption 显式传
err := compose_v2.Run(ctx, g, cm, compose_v2.WithHooks(myEngineHooks{}))
//   → 未传 RunOption 时 fallback GetOrchHooks(ctx)（graph_loop.go:276 自动继承）
```

**优先级（agent_v2 高层路径）**：`RunOption WithHooks > ctx CtxOrchHooks > nil`。

### 10.4 EventSink：节点内主动发 event

```go
// 节点内
sink := compose_v2.GetEventSink(ctx)
if sink != nil {
    sink.Send(compose_v2.Event{
        Type:    "chunk",
        Content: chunkText,
    })
}
```

**典型用途**：BasicAgent modelFn 流式调 LLM 时把 chunk 实时投递给 OrchHooks，不等
节点 return（barrier）。前端 SSE 通过 OrchHooks 实时拿到 chunk。

### 10.5 chunk 不写 session.Events 的关键不变量

```
Streaming chunk → sink.Send(Event{Type:"chunk"}) → OrchHooks.OnNodeEvent
                                                    （前端 SSE 实时投递）
最终聚合 assistant message → AgentEvent (OutputEvent) → session.AddEvent
                                                       （审计持久化）
```

**为什么 chunk 不写 session**：
1. 频次太高（一个 LLM 输出可能产生 100+ chunks），写 session 浪费
2. session 是审计视图，业务只关心最终 message；chunk 是实时投递视图，前端关心
3. Resume 时不重发 chunks（v2 cp 不存 events，重启需业务自己持久化 SSE 流）

**业务用场景**：plan_chat_web SSE 流——chunk 通过 OrchHooks 直接转 SSE，不进 session。

### 10.6 RecordingHooks test helper（Stage 5 C1 补）

```go
// compose_v2/recording_hooks.go
type RecordingHooks struct{ ... }

// 测试用法
rec := &compose_v2.RecordingHooks{}
ctx = compose_v2.WithOrchHooks(ctx, rec)
err := compose_v2.Run(ctx, g, cm, opts...)

// 4 个 Snapshot 方法分别拿不同维度的录制
events := rec.SnapshotEvents()         // []Event（OnNodeEvent 收集）
starts := rec.SnapshotNodeStarts()     // []NodeID（OnNodeStart 收集）
ends   := rec.SnapshotNodeEnds()       // []NodeID（OnNodeEnd 收集）
graphStarts, graphEnds := rec.SnapshotGraphLifecycle()  // (int, int)
```

### 10.7 Hooks panic 兜底 (audit² 改: 跟 eino 对齐 outer recover)

audit Round 7 当时方案: 每个 hook 调用点包 `safeOnXxx` per-handler isolation. audit²
跟 eino (`internal/callbacks/inject.go` 无 per-Handler recover, 只在 task-level outer
recover) 对齐, 改成 **framework 4 层 outer defer recover 兜底**:

1. `compose_v2.Run` 入口 outer recover ([graph_loop.go](../../compose_v2/graph_loop.go))
2. `runNodeWithHooks` 整函数 defer recover (覆盖 fan-out 跨 goroutine — 主 goroutine
   outer recover 不能 cross-goroutine cover, 此处单独加)
3. `agent_v2.BasicAgent.Run / Resume` + 其他 8 编排 Run/Resume 各 outer recover
4. 嵌套 AgentAsTool 子 agent.Run 自带 outer recover 间接保护父 ToolsFn 路径

业务 hook panic 转 wrapped err 返业务:

**`Run` 出口有两个 recover，分工不同**（一个满足不了两件事）：

```go
func Run(ctx context.Context, g *Graph, cm *ChannelManager, opts ...RunOption) (runErr error) {
    // ① 注册最早 ⟹ LIFO 下最后跑：专收 **defer 链自身**抛的 panic
    //    （业务 OnGraphEnd hook / 自定义 checkpointer 的 SaveNS / DeleteSubtree）
    defer func() {
        if r := recover(); r != nil {
            // runErr 已有值时 errors.Join 保双链，不无条件覆盖
            ...  // 前缀 "compose_v2.Run panic recovered: "
        }
    }()
    // ... cleanup defer（注册 1）、OnGraphEnd defer（注册 2）...

    // ② 注册最晚 ⟹ **最先跑**：把**主体**的 panic 先转成 runErr，
    //    好让后面的 cleanup / OnGraphEnd 看到真实结局
    defer func() {
        if r := recover(); r != nil {
            ...  // 前缀 "compose_v2: graph body panic recovered: "
        }
    }()
    // ...
}
```

**为什么必须是两个**：只有 ① 时，cleanup 与 `OnGraphEnd` 都比它先跑，那时 `runErr` 还是
nil ⟹ cleanup 走成功分支 `purgeExitSlot(reason="success")`。实测后果：用户正等着答一个
中断，下一次 Run 的主体 panic ⟹ 框架打印 `reason=success` **删掉挂起档**、告诉 `OnGraphEnd`
一切正常、同时返回 error ⟹ 此后 Resume 恒返 `ErrNoPendingCheckpoint`，会话没了。

**两个前缀刻意不同**：业务据此区分「图主体炸了（存档已正确存成 failed，可续）」与
「存档写 / hook 炸了（存档可能压根没落盘）」——这两件事的处置相反。

**eino 哲学**: hook panic = 业务 bug fail-fast, framework 不做 per-hook silent isolate
(silent isolate 让业务 bug 难发现). chain 内多 hook 时一个 panic 中断剩余 chain (跟
新 ctx-based 协议 fail-fast 一致).

---

## 11. 错误处理 / 重试 / 断路器

### 11.1 错误分层

| 层 | 错误类型 | 框架行为 |
|---|---|---|
| 业务节点 | 普通 `error` | static 上抛 caller，cp save Pending 状态 |
| HITL | `*compose_v2.InterruptError`（含 `AskQuery`）| 收 PendingInterrupts，cp save Pending 状态 |
| ctx | `context.DeadlineExceeded` / `Canceled` | 整 Run return ctx error，cp save |
| panic | `safeNodeRun` recover 转 error | frontier 非空时 SaveNS Pending（同业务 error；§13.5 #7）|
| reducer | `ApplyAll` 归并段 panic → defer recover 转 error | 那一批写**一条都没提交**（归并跑在局部 staged 上）+ return error |

### 11.2 5 类典型错误（Stage 5 C3 补）

4 个 sentinel + 2 个 typed error：

```go
// compose_v2/errors.go
var ErrNoSubgraphCheckpoint = errors.New("...")  // 嵌套子图找不到 cp
type MaxIterationsError struct{ Iterations int }  // `compose_v2/graph_loop.go::Run` LoopAgent / Graph maxVisit 触顶时**实际抛** (按符号自查; errors.As 解析 Iterations)
type MaxRoundsError      struct{ Rounds int }     // ReAct round 耗尽——typed 但生产路径不抛, 预留供业务自定义

// compose_v2/checkpointer.go
var ErrNoPendingCheckpoint = errors.New("...")  // GA-M2：显式 Resume 但 (sid,ns) 无挂起 cp（sid 拼错/cleanupOnSuccess 已清/从未跑过）——9 编排 Resume 统一 fail-fast，Abort 豁免
var ErrGraphIDConflict     = errors.New("...")  // GraphID 串台 fail-fast
```

业务用 `errors.Is(err, compose_v2.ErrXxx)` 判定 sentinel；`errors.As(err, &mie)` 解析
typed error 拿具体值（如 `mie.Iterations`）。

### 11.3 RetryConfig + RunWithRetry[T] 泛型 helper

```go
// compose_v2/retry.go

// 纯退避参数（「等多久再试」），被编排层与模型故障切换层共享——两边都全用
type BackoffPolicy struct {
    InitialDelay time.Duration  // 首次重试前等待时间；0 = 立即重试
    MaxDelay     time.Duration  // 单次等待上限；0 = 不限制
    Multiplier   float64        // 每次重试后等待时间的倍数（建议 2.0；≤1.0 退化为固定间隔）
    Jitter       bool           // 开启 ±25% 随机抖动，防 Thunder Herd
}

type RetryConfig struct {
    MaxRetries          int              // 最大重试次数；0 = 不重试（共执行 1 次）
    RateLimitMaxRetries int              // 限流（429）失败的独立上限；0 = 不单独限（仍受 MaxRetries 约束）
    BackoffPolicy                        // 嵌入：cfg.InitialDelay 这类访问照旧
    ShouldRetry         func(error) bool // 判定「这个错该不该重试」——MaxRetries>0 时必填
}

// 工厂函数（内部填上面的字段，含 ShouldRetry = IsRetryable 这个保守默认）
func SimpleRetry(n int) RetryConfig                                          // 无退避立即重试 n 次
func ExponentialBackoff(n int, initial, max time.Duration) RetryConfig       // 指数退避 + jitter
func ValidateRetryConfig(cfg RetryConfig, optionName string)                 // 装配期校验；缺谓词 panic

> **判据字段为什么不进 `BackoffPolicy`**：「等多久再试」有多个使用者（编排层的成员重试、
> 模型层的单候选重试），而「这个错该不该重试」只在编排层有意义——模型故障切换那层用的是
> 另一个判据（那里「该重试」恰好等于「该切换」）。两者曾放在同一个结构体里，后果是
> `fallback` 只读退避、不读判据，而判据字段的文档写着「必填」——业务照文档给了，谓词
> **一次都没被咨询**，副作用照样重放。拆开之后那句假话无处可说：只需要退避的地方，
> 拿到的类型里**根本没有判据字段**。

> ⚠️ **`ShouldRetry` 在 `MaxRetries > 0` 时必填**（经编排的 `WithXxxRetryConfig` 配置时，
> 装配期 `ValidateRetryConfig` panic）。重试重放的是**整个成员调用连同它的副作用**，
> 而框架在这一层看到的只是 `error`，分不出「网络抖了一下」和「参数非法重跑也一样」
> ——后者在 Go 里的写法就是普通的 `fmt.Errorf`。配重试的人答得上这个问题，框架答不上。
> 省事写法：传 `SimpleRetry(n)` / `ExponentialBackoff(...)`，它们自带保守谓词 `IsRetryable`
> ——那是**显式选择接受默认**，而不是忘了想。

// 泛型 helper：业务函数包一层自动重试
func RunWithRetry[T any](
    ctx context.Context,
    cfg RetryConfig,
    fn func(ctx context.Context) (T, error),
) (T, error)
```

**关键不变量**：控制流信号（中断 / 调用方 ctx 已死 / 软取消 / 陈旧写回）、终局失败
（`ErrRunAborted`）、坏存档（`ErrCheckpointNotResumable`）永不重试——由 `RunWithRetry` 在
业务谓词**之前**的那道 guard 保证，谓词写成什么都吞不掉；ctx 那一档按**调用方 ctx** 判
（`IsRetryFutile`），不按错误链。默认谓词 `IsRetryable` 另外排除什么、限流的独立计数、
`Retry-After` 怎么用，以 `compose_v2/retry.go` 里 `RunWithRetry` 与 `IsRetryable` 的 godoc
为准——这里曾复述过一份 `IsRetryable` 的伪代码，后来谓词加了好几类排除、那份没跟上。

**重试的是同一次调用**：第 2 次尝试起清掉「新开」信号，fn 里的嵌套调用按自己的存档续跑、
只重跑失败的部分；嵌套调用的次序编号退回进入重试那一刻的值，所以输入不变的调用算出跟第一次
尝试相同的身份——第一次尝试里已完成的拿回记下的结果、不再执行，失败的续上第一次尝试的存档；
重试时换了输入的调用是另一次调用、从头跑。手写的循环没有这两样，每次都算新调用、从头跑。

所以「拿到嵌套调用的产出、校验不合格就重来」别用 `RunWithRetry`：已完成的调用每次原样拿回，
重试预算全部白花。要一份新产出就在循环里调，或把校验放到条件边上声明 `CondFailRerunFrom`，
并给产出那一次嵌套调用声明 `agent_v2.WithRerunnable()`（它的完成记录按 `Rerunnable` 记，条件边否决时
作废、重跑时重新执行；没点名的照常拿回）。

### 11.4 7 编排接 RetryConfig

Stage 4 给 7 编排都加了 `WithXxxRetryConfig` / `WithXxxMaxRetries` 选项：

| 编排 | option |
|---|---|
| Plan | `WithPlanRetryConfig(cfg)` / `WithPlanMaxRetries(n)` |
| Loop | `WithLoopRetryConfig(cfg)` / `WithLoopMaxRetries(n)` |
| Sequential | `WithSequentialRetryConfig(cfg)` / `WithSequentialMaxRetries(n)` |
| Parallel | `WithParallelRetryConfig(cfg)` / `WithParallelMaxRetries(n)` |
| Reflect | `WithReflectRetryConfig(cfg)` / `WithReflectMaxRetries(n)` |
| Routing | `WithRoutingRetryConfig(cfg)` / `WithRoutingMaxRetries(n)` |
| Handoff | `WithHandoffRetryConfig(cfg)` / `WithHandoffMaxRetries(n)` |

每个编排内部 sub-agent.Run 包 `RunWithRetry` wrapper——业务 LLM 报 5xx / network
error 自动重试，无感知。

### 11.5 CircuitBreaker（仅 AutoCompact 用）

```go
// compose_v2/circuit_breaker.go
type CircuitBreaker struct { ... }

func (b *CircuitBreaker) Allow() bool       // 跳过条件
func (b *CircuitBreaker) RecordSuccess()
func (b *CircuitBreaker) RecordFailure()
```

**典型场景**：AutoCompact 摘要 LLM 连续失败 3 次后跳过本会话，避免上下文不可恢复时
无限消耗 API（Claude Code 实测：1,279 个会话曾连续失败 50+ 次，最多 3,272 次，每天
浪费约 250K 次 API 调用）。

**自愈**：cooldown 后半开尝试一次，成功则清零计数；否则继续 trip。**断路器一旦 trip
必须能自愈**——CLAUDE.md "公开 API 防御" 强调。

---

## 12. 9 编排到 Graph 的编译产物

每个编排都是 `compose_v2.GraphBuilder` 的高层封装——业务用 `agent_v2.NewXxxAgent`
拿到 Agent，框架内部编译为 channel-based Graph。

| 编排 | 内部拓扑 | 关键 channel（实际 var 名，业务 NodeFunc 不直接接触——内部 `__xxx__` 双下划线约定）|
|---|---|---|
| **BasicAgent** | preamble + model + tools + postamble **四节点** + 循环回边（preamble 只跑首轮 ISSUE-74 / postamble 跑 commitFinal+store.Put ISSUE-75 / tools 节点内 N>=2 tc goroutine 并发 fanout ISSUE-81）| `__basic_agent_messages__` (Append) + `__basic_agent_round__` (Sum) + `__basic_agent_preamble_input__` (LastValue) + `__basic_agent_return_direct__` (OnceFlag) |
| **SequentialAgent** | step_1 → step_2 → ... → step_N | `__seq_agent_messages__` (Append) + `__seq_agent_last_out__` (LastValue) + `__seq_agent_original__` (LastValue) + `__seq_agent_step__` (Sum) |
| **ParallelAgent** | StartNode → fan-out N workers → merger | `__par_agent_results__` (Append[ParallelBranchResult]) + `__par_agent_original__` (LastValue) + `__par_agent_final__` (LastValue) |
| **LoopAgent** | body + ExitCondition 判定 | `__loop_agent_input__` (LastValue) + `__loop_agent_last_out__` (LastValue) + `__loop_agent_round__` (Sum) + `__loop_agent_exited__` (OnceFlag) |
| **RoutingAgent** | classify（跑 Router/classifier 决策 → 写 target）→ conditional（纯读 target）→ branches | `__routing_agent_input__` (LastValue) + `__routing_agent_target__` (LastValue) + `__routing_agent_result__` (LastValue) |
| **HandoffAgent** | initial → conditional → 各 expert（state machine）| `__handoff_agent_active__` (LastValue[string]) + `__handoff_agent_input__` (LastValue) + `__handoff_agent_output__` (LastValue) + `__handoff_agent_hops__` (Sum) + `__handoff_agent_done__` (OnceFlag) + `__handoff_agent_exhausted__` (OnceFlag) |
| **PlanAgent** | plan（跑 Planner，定下一步或写 done / answer 结束）→ plan_build_input（拼 worker 输入，只拼一次）→ 该步的 worker 节点（每个成员一个）→ plan 循环 | `__plan_executed__` (Append[ExecutedStep]) + `__plan_original__` (LastValue) + `__plan_current__` (LastValue) + `__plan_built_input__` (LastValue) + `__plan_final_answer__` (LastValue) + `__plan_done__` (OnceFlag) |
| **ReflectAgent** | actor → evaluate → cond → [End \| feedback] → actor 循环（R33 拆图：verdict/critique 进 channel superstep commit，feedback 中断 Resume 不重评 evaluator）| `__reflect_actor_input__` + `__reflect_actor_out__` + `__reflect_original__` + `__reflect_round__` (Sum) + `__reflect_exited__` (OnceFlag) + `__reflect_final__` + `__reflect_critique__` + `__reflect_opinion__` |
| **GraphAgent** | 业务自定义 Graph 包成 Agent | 业务定义（GraphInputWriter 写入 / GraphOutputReader 读出） |

**详细业务用法见 [`usage-guide.md`](./usage-guide.md)**。

### 12.1 Plan 内部 channel 详解（典型）

```go
// agent_v2/plan_builder.go
var (
    planOriginalCh   = LastValue[string]("__plan_original__")
    planExecutedCh   = Append[ExecutedStep]("__plan_executed__")  // ★ 历史步骤
    planCurrentCh    = LastValue[*planExecStep]("__plan_current__") // 已解析的执行态
    planAnswerCh     = LastValue[string]("__plan_final_answer__")
    planDoneCh       = OnceFlag("__plan_done__")
    planBuiltInputCh = LastValue[*schema.Message]("__plan_built_input__")
)
```

**各 channel 的 reducer**：
- `original` 用 `LastValue`：用户目标只写一次
- `executed` 用 `Append`：每 worker 写一个 ExecutedStep 累加
- `current` 用 `LastValue`：每次 plan 节点决定的下一步覆盖前值
- `built_input` 用 `LastValue`：plan_build_input 节点拼好的 worker 输入，**拼一次、提交进
  channel、worker 只读**——worker 中断重跑时从 cp 恢复同一条消息，不重拼
- `done` 用 `OnceFlag`：终止信号写一次即终止
- `answer` 用 `LastValue`：终止时一次性写

**业务不直接接触这些内部 channel**——包内的 `planInputWrites` / `planFinalAnswer` 两个
helper（不导出）封装了写入与读出。

---

## 13. 关键不变量速查

### 13.1 引擎层不变量

| # | 不变量 | 出处 / 修复 |
|---|---|---|
| 1 | isEnd 优先于 ctx 检查（彻底消除 v1 "已完成 graph 被 ctx 拦下" bug）| graph_loop.go::Run |
| 2 | BSP barrier 统一同步点 reduce（所有 writes 在合并点 apply）| channel_manager.go::ApplyAll |
| 3 | 节点是纯函数（不直接 mutate state，只返 ChannelWrite）| 设计原则 |
| 4 | 一批写要么全落地要么一条都不落地（归并与提交分开，掐断时「压根没发生」）| — |
| 5 | ctx 必须有 deadline（fan-out 死锁的唯一外部解药）| CLAUDE.md "交付纪律" |
| 6 | NodeVisits 跨 Run/Resume 持久化（InterruptID 的 visit 维度用）| audit Round 3 Bug #1 |
| 7 | panic 经 safeNodeRun 转 error；frontier 非空时 Run defer 仍 SaveNS Pending（业务应避免用 panic 表达 HITL，否则 Resume 死循环）| `compose_v2/graph_loop.go::safeNodeRun` (按符号自查) |

### 13.2 cp + Resume 不变量

| # | 不变量 | 出处 / 修复 |
|---|---|---|
| 8 | cp 按 (sessionID, ns) 二级 key 物理分离 | ISSUE-70 Phase 2 |
| 9 | per-channel taggedValue 序列化（cp 粒度 = channel 粒度）| 设计原则 |
| 10 | NodeVisits 跨 Run/Resume 持久化 | audit Round 3 |
| 11 | GraphID 防 cp 串台 fail-fast | ISSUE-50 P0 |
| 13 | Run 正常完成 defer 自动 DeleteSubtree（删 cp 防污染）| 设计原则 |
| 14 | subgraph export writes 构造先于子 cp `DeleteSubtree`（防回滚后子 cp 已删不幂等）| R9 D4 |
| 15 | 嵌套 Resume 按 (sid, graphNS) 二级 key 各层独立 LoadNS（无 IsResumeTarget 函数）| 设计原则 |
| 16 | ResumeContext 默认全 ctx 子树继承（opt-in 隔离用 WithSubgraphIsolatedResumeContext）| 设计原则 |
| 17 | UserInputs 类型 `map[string]*schema.Message`（key=InterruptID）| R9 B1 |
| 18 | 嵌套子图 ResumeContext 隔离 opt-in（默认继承父）| R9 B2 |
| 19 | auto-load Resume unmatched ID warn 不报错 | R10-B |
| 20 | 答案无档：配了 cper+sid 时 fail-fast 返 `ErrNoPendingCheckpoint`（图体不执行）；没配时 warn + 照跑 | R10-B / 后续改 fail-fast |
| 21 | Restore atomic 业务 UnmarshalJSON panic 路径 | R10-C |

### 13.3 Fan-out 不变量

| # | 不变量 | 出处 / 修复 |
|---|---|---|
| 22 | 所有 fan-out 编译为图原生超步 frontier（统一调度）| 设计原则 |
| 23 | per-branch delta 自然继承（channel-based 天然 incremental）| 设计原则 |
| 24 | Resume 仅重跑失败/中断分支 | 设计原则 |
| 25 | best-effort 等兄弟跑完（业务 error / Interrupt 不传染 ctx）| 设计原则 |
| 26 | ctx cancel 必须传染兄弟（外部主动停）| 设计原则 |
| 27 | 多分支中断全部收集 PendingInterrupts | R10-D |
| 28 | Append reducer no aliasing（每次 reduce 新分配 backing array）| R9 C3 |
| 29 | `PendingInterrupts()` getter 返深拷贝（防 caller mutate 污染 cm；非 Snapshot 路径）| R9 C4 |
| 30 | nil Query deadlock 修复 | R10-D |

### 13.4 HITL 不变量

| # | 不变量 | 出处 |
|---|---|---|
| 31 | `InterruptError.AllQueries()` 严格非空（业务暂停按钮虚拟 Query）—— 单 query 用 Query 字段，多 query 用 Queries 字段，两字段互斥 Queries 优先；framework 验 `len(AllQueries())==0` fail-fast | v2 fail-fast / ISSUE-81 |
| 32 | InterruptID 按执行位置稳定派生（`InterruptSite`：TaskID / graphNS / nodeID / 实例身份⊻visit / tc 下标 / idx）| ISSUE-71 + ISSUE-96 |
| 33 | 多 tool_call 部分中断：toolsNode 整节点失败（channel writes 作废）+ 已成功 tool_call 的产出进节点 memo（`SaveNodeState`，随 cp 走），Resume 命中即重放不重执行；ask_user 二阶段语义自动幂等 | hitl-resume.md §6 |
| 34 | mid-stream Interrupt + reset_stream + run_id 协调 | R10-A |
| 35 | OnModelEnd 改返 Message + 字符串拼接 | R10-A |

### 13.5 Hooks 不变量

| # | 不变量 | 出处 |
|---|---|---|
| 36 | chunk 不写 session.Events，只走 OrchHooks.OnNodeEvent | 设计原则 |
| 37 | streamOrGenerate 重组 message 时保留 chunk Annotations | Stage 6.1 修 |
| 38 | agent_v2 高层 OrchHooks ctx-aware（agent_v2.WithCtxOrchHooks 嵌套子 agent 自动继承）；compose_v2 引擎层 Run 优先 RunOption WithHooks、未传 fallback GetOrchHooks(ctx)（graph_loop.go:276） | Stage 5 C1 + R4 修订 |
| 39 | Hooks panic → framework 4 层 outer defer recover 转 wrapped err（fail-fast 非 per-hook 隔离；audit² 跟 eino 对齐删 safeOnXxx）| audit² |

### 13.6 序列化不变量

| # | 不变量 | 出处 |
|---|---|---|
| 40 | tagged JSON + 类型注册表（cp 反序列化原类型恢复）| 设计原则 |
| 41 | 业务自定义类型必须 RegisterValueType 注册 | 设计原则 |
| 42 | nil any 序列化路径加守卫 | R8 |
| 43 | 业务 UnmarshalJSON panic atomic 回滚 | R10-C |

---

## 14. 反模式 + 常见误解

### 14.1 节点级反模式

| ❌ 反模式 | ✅ 正确做法 | 原因 |
|---|---|---|
| 节点内直接 mutate cm（如 `cm.write("x", v)`）| 返 `Write(xCh, v) ChannelWrite` | 违反 BSP——绕过 barrier，多分支 race |
| 节点内 spawn goroutine 写 channel | 把并发逻辑封装成 sub-agent / fan-out | 框架不知道 goroutine 何时完成，barrier 时机错乱 |
| 节点 panic 不 recover 让框架接（依赖框架 recover）| 节点内自己 recover 并转明确 error | `safeNodeRun` 转的 error 只有 "node panic recovered: <panic 值>"（栈挂在错误上的 `compose_v2.PanicError` 里、日志里也有），业务自己 recover 能说清业务含义 |
| 节点内忽略 ctx.Err() | 长循环内 `select { case <-ctx.Done(): return ctx.Err() }` | ctx cancel 不响应 → 死锁兄弟 |
| 节点产生大量 chunk 不用 EventSink | `sink.Send(Event{Type:"chunk"})` | 不写 session 但前端能实时拿到 |

### 14.2 Builder 级反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| Channel 名字硬编码（`Append[T]("messages")` 直接调用每次）| 全局 var 一次定义，所有节点引用 `*Channel[T]` |
| AddConditionalEdge 返 `("", error)` 让框架兜 | 显式返 `(EndNode, nil)` 或 `("specific_node", nil)` |
| AddNode 重复同 id | 不会报错，后加的节点函数静默覆盖先加的（先加的若声明过 `WithNodeComponent`、后加的没声明，组件类型会残留）——业务自己保证 ID 唯一 |
| StartNode / EndNode 当业务 ID 用 | `Compile` 报错：它们是图的起止标记、不是节点，业务用其他 ID |

### 14.3 cp + Resume 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 业务自定义类型不 RegisterValueType | `func init() { compose_v2.RegisterValueType[MyT]("myapp.MyT") }` |
| 同 sessionID 跑两个不同 Graph 想复用 cp | 不同 Graph 的 `g.ID()` 自动不同 → fail-fast `ErrGraphIDConflict`；确需换 graph 用 `WithFreshRun` 忽略旧 cp |
| Resume 时只塞部分 InterruptID 答案不传 ResumeAction | 显式 `WithResumeAction(ResumeActionContinue)` + 传完整 answers |
| 业务依赖失败 cp 长期保留（cleanup 防污染会自动删成功 cp）| 业务侧定期 `cper.Cleanup(ctx, keepLastN)` 控总量 |
| 跨 Run 想累积历史用 sessionID 复用 | 用 `agent_v2` 会话历史层（`WithHistory`，按会话身份 append-only 留存）|

### 14.4 fan-out 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| fan-out 不设 ctx deadline | `ctx, cancel := context.WithTimeout(...)` 必给 |
| 业务 NodeFunc 死循环不 return | 内部加 ctx.Done() select |
| 在 Append reducer 内复用 prev 数组 | 每次 reduce 新分配 backing array（v2 已修，业务自定义 reducer 注意）|
| fan-out 分支共享 mutable struct（不深拷贝）| `cloneStateForFanOut` 协议（v2 channel 模型天然隔离，但业务自定义状态注意）|

### 14.5 常见误解

| ❌ 误解 | ✅ 真相 |
|---|---|
| "channel 是 Go chan" | 不是——是 LangGraph Pregel 的 channel 概念，命名共享 slot + reducer |
| "BSP barrier 让 agent 慢" | 实际不慢——LLM 调用耗时远大于 barrier 同步开销 |
| "fan-out 失败要全部重跑" | 不——v2 仅重跑失败/中断分支（per-branch delta 天然继承）|
| "嵌套子图 cp 会覆盖父 cp" | 不——按 (sid, ns) 二级 key 物理分离 |
| "GraphID 不设也能跑" | 能跑，但同 sid 跑两个 Graph 时静默串台数据混乱（v2 fail-fast 保护）|
| "Memory 跨 Run 用就够，不需要 cp" | Memory 跟 cp 是不同层——Memory 是业务长期记忆，cp 是引擎执行状态快照（HITL 必需）|
| "不开 cp 就没有 Resume 能力" | 对——`WithCheckpointer` 是开 Resume 的开关 |
| "节点 panic 后 Resume 能安全续跑" | panic 确实存 cp（frontier 非空；档位标 `Status=Failed`，但**照样被 auto-load 捡起**——闸门按内容判不读 Status），于是 Resume 重跑会再 panic 死循环——业务避免用 panic 表达 HITL（§13.5 #7）|

---

## 关联文档

- [`channel-based-design.md`](./channel-based-design.md) — channel-based 引擎的 PoC 期设计稿（5 大难点 + API 草案）
- [`why-channel-based.md`](./why-channel-based.md) — 当初为什么转 channel-based（直接 trigger / 5 大根本设计问题 / PoC 决策 / 实际收益对比）
- [`usage-guide.md`](./usage-guide.md) — 业务侧用法（写一个 BasicAgent / 编排 / 集成 cp）
- [`context-compaction.md`](./context-compaction.md) — 4 层上下文压缩完整设计
- [`hitl-resume.md`](./hitl-resume.md) — HITL 中断 + Resume 完整协议（待写）
- [`migration-history.md`](./migration-history.md) — Stage 1-7 commit 索引 + v2 完整 API 速查
- [`../description/three-way-comparison/`](../description/three-way-comparison/) — crew-go vs LangGraph vs eino 三方对照决策记录
