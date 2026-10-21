# compose_v2 业务使用指南

> 目标：让业务能用 compose_v2 写编排，不用读全部源码
> 完整 v2 公开 API 速查见 [`migration-history.md`](./migration-history.md)

本指南覆盖业务用 compose_v2 写编排时**必须知道的约定 + 核心 API + 常见 pattern**。深入设计原理见 `channel-based-design.md`；历史 PoC 决策溯源见 `poc-report.md`。

## 0. 30 秒速览

**业务侧 90% 用高层 `agent_v2` API**（自动管 channel + cp + Resume + 4 层压缩）：

```go
package main

import (
    "context"
    "crew-go/agent_v2"
    "crew-go/model/qwen"
    "crew-go/schema"
)

func main() {
    ag := agent_v2.NewBasicAgent(
        agent_v2.WithName("my_agent"),
        agent_v2.WithModel(qwen.NewClient(apiKey, "")),
        agent_v2.WithSystemPrompt("你是助手。"),
    )

    sess := agent_v2.NewRunSession()
    events, err := ag.Run(context.Background(), sess, schema.UserMessage("hi"))
    if err != nil { panic(err) }
    final := agent_v2.FinalMessage(events)
    println(final.Content)
}
```

**少数自定义场景用低层 `compose_v2` API**（业务自定义 NodeFunc + Channel）：

```go
import "crew-go/compose_v2"

// 1. 定义 channels
var messagesCh = compose_v2.Append[*schema.Message]("messages")

// 2. init() 注册跨 cp 持久化的类型
func init() {
    compose_v2.RegisterValueType[[]*schema.Message]("[]*schema.Message")
}

// 3. 业务自定义 NodeFunc（NodeFunc 签名 != BasicAgentModelFn 签名）
func myNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    msgs, _ := compose_v2.Get(chs, messagesCh)
    out := process(msgs)
    return []compose_v2.ChannelWrite{compose_v2.Write(messagesCh, []*schema.Message{out})}, nil
}

// 4. 拼自定义图
func main() {
    bld := compose_v2.NewBuilder()
    bld = compose_v2.WithChannel(bld, messagesCh)
    g, _ := bld.AddNode("model", myNode).
        AddEdge(compose_v2.StartNode, "model").
        AddEdge("model", compose_v2.EndNode).
        Compile()

    cm := compose_v2.NewChannelManager()
    compose_v2.Run(context.Background(), g, cm,
        compose_v2.WithInitialWrites(compose_v2.Write(messagesCh,
            []*schema.Message{schema.UserMessage("hi")})))
}
```

> **注意签名差异**：`agent_v2.NewBasicAgentBuilder(modelFn, toolsFn)` 接的是 `BasicAgentModelFn = func(ctx, preamble, msgs []*schema.Message) (*schema.Message, error)`（两个消息参数：`preamble` 是 agent 前言、不参与压缩，自己实现时要把它拼到请求最前） + `BasicAgentToolFn = func(ctx, toolCalls []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error)`（`callCtxs[i]` 是第 i 个调用自己的 ctx，即 OnToolStart(i) 采纳之后的那个，执行第 i 个调用要用它；`ctx` 只给这一批整体的工作），**不是** NodeFunc。业务自定义图时用 NodeFunc + `compose_v2.NewBuilder()`。

## 1. 类型注册约定（**必读，否则 cp 失败**）

### 为什么要注册

channel value 类型是 `any`——cp marshal 时 JSON 不知道实际类型，反序列化时 `json.Unmarshal(b, &any)` 默认拿到 `map[string]interface{}` / `float64` 等退化类型，cast 回原 T 失败。

业务在 `init()` 注册——marshal 时附 type tag，unmarshal 按 tag 查表反序列化。

### 怎么注册

```go
func init() {
    // 基础类型
    compose_v2.RegisterValueType[int]("int")
    compose_v2.RegisterValueType[string]("string")
    compose_v2.RegisterValueType[bool]("bool")

    // 切片
    compose_v2.RegisterValueType[[]string]("[]string")
    compose_v2.RegisterValueType[[]*schema.Message]("[]*schema.Message")
    compose_v2.RegisterValueType[*schema.Message]("*schema.Message")

    // 业务自定义类型——typeName 用 "包.类型" 避免冲突
    compose_v2.RegisterValueType[*MyDomainObj]("mypkg.MyDomainObj")
}
```

### 必须注册的场景

- 任何 channel value 类型在调用 `cm.Snapshot()` 之前必须注册
- 中断 + Resume 路径必经 cp marshal——所有 channel 类型都得注册
- 如果只跑完整 graph 不持久化（无 InterruptError、无 cancel），可不注册

### 常见错误

```
snapshot channel "xxx": marshalValue: type *MyType not registered
```
→ 在 init() 加 `compose_v2.RegisterValueType[*MyType]("...")`

## 2. 核心 API 速查

### Channel 工厂

| 函数 | 语义 | 何时用 |
|---|---|---|
| `LastValue[T](name)` | next 覆盖 prev | 节点输出（最近一次 LLM 回复等） |
| `Append[T](name)` | next 追加到 prev | 消息累积、事件流 |
| `Sum(name)` | int 累加 | token 计数、轮次 |
| `OnceFlag(name)` | bool 单调（一旦 true 不变）| 终止标志、不可逆状态 |
| `Custom[T](name, reducer, zero)` | 自定义 reducer | 上述 4 种不够时 |
| `DynamicChannel[T](name, reducer, zero)` | 同 Custom 但语义"runtime 创建" | 业务循环里动态创建 channel |

### 读 / 写 / 注册

```go
// 读（包级泛型函数，编译期类型推断）
val, ok := compose_v2.Get(chs, myCh)
val := compose_v2.MustGet(chs, myCh)        // 未写入 panic
exists := compose_v2.Has(chs, myCh)

// 写
write := compose_v2.Write(myCh, value)       // 类型错编译失败
err := cm.ApplyAll([]compose_v2.ChannelWrite{write1, write2})

// 注册到 cm
err := compose_v2.RegisterChannel(cm, myCh)
```

### 9 编排 Builder

#### 高层 agent_v2 wrapper（业务侧通常用这个）

| 编排 | 构造函数 | 关键 option（`Wit
hXxx*` 函数）+ 链式方法 |
|---|---|---|
| BasicAgent | `agent_v2.NewBasicAgent(opts...)` | options: `WithName / WithDescription / WithSystemPrompt / WithModel / WithToolkit / WithHooks / WithMaxRounds` |
| SequentialAgent | `agent_v2.NewSequentialAgent(opts...)` | options: `WithSequentialName / WithSequentialDescription / WithSequentialHooks / WithSequentialRetryConfig / WithSequentialMaxRetries`<br>链式: `.AddStep(name, agent)` |
| LoopAgent | `agent_v2.NewLoopAgent(opts...)` | options: `WithLoopName / WithLoopDescription / WithLoopMaxIterations / WithLoopExitCondition / WithLoopHooks / WithLoopRetryConfig / WithLoopMaxRetries`<br>链式: `.WithBody(agent)` |
| RoutingAgent | `agent_v2.NewRoutingAgent(opts...)` | options: `WithRoutingName / WithRoutingDescription / WithRouter / WithRouteDecider / WithRoutingClassifier / WithRoutingFallback / WithRoutingDescriptions / WithRoutingHooks / WithRoutingRetryConfig / WithRoutingMaxRetries`（`WithRouteDecider` 见 [route-decider.md](route-decider.md)；决策方用 `decision` 包接 Jev 或百炼的决策模型，可运行演示 `example/decision/routing_demo`）<br>链式: `.AddBranch(name, agent)` |
| ParallelAgent | `agent_v2.NewParallelAgent(opts...)` | options: `WithParallelName / WithParallelDescription / WithParallelMerger / WithAggregator / WithParallelHooks / WithParallelRetryConfig / WithParallelMaxRetries`<br>链式: `.AddBranch(name, agent)` |
| PlanAgent | `agent_v2.NewPlanAgent(planner, opts...)` | options: `PlanName / PlanDescription / WithSubAgents / WithExecInputBuilder / WithPlanMaxSteps / WithPlanRetryConfig / WithPlanMaxRetries / WithPlanModel / WithPlanToolkit / WithPlanHooks` (ISSUE-83 与其他 8 编排架构对称)<br>链式: `.AddSubAgent(name, ag) / .SetExecutor(ag)`<br>注：`WithLLMPlannerSubAgents` 是 `NewLLMPlanner` 的 LLMPlannerOption 不是 PlanOption |
| ReflectAgent | `agent_v2.NewReflectAgent(opts...)` | options: `WithReflectName / WithReflectDescription / WithReflector / WithEvaluator / WithReflectorAgent / WithEvalFunc / WithReflectMaxRounds / WithReflectHooks / WithReflectRetryConfig / WithReflectMaxRetries`<br>链式: `.WithActor(agent)` |
| HandoffAgent | `agent_v2.NewHandoffAgent(opts...)` | options: `WithHandoffName / WithHandoffDescription / WithHandoffInitial / WithHandoffMaxHops / WithHandoffHistoryMode / WithHandoffHooks / WithHandoffRetryConfig / WithHandoffMaxRetries` + sub-agent 侧 `agent_v2.WithHandoffTransfer()`（必配才能 LLM 自主转交）<br>链式: `.AddAgent(name, ag) / .State(sess) / .EndConversation(ctx, threadID)` |
| GraphAgent | `agent_v2.NewGraphAgent(graph, opts...)` | options: `WithGraphAgentName / WithGraphAgentDescription / WithGraphInputWriter / WithGraphOutputReader / WithGraphAgentHooks` |

#### 低阶 builder（自定义场景用）

| Builder | 入口 |
|---|---|
| `compose_v2.NewBuilder()` → `*GraphBuilder` | 通用图构造（业务自定义节点 + 边，留在 compose_v2 引擎包）|
| `agent_v2.NewBasicAgentBuilder(modelFn, toolsFn)` → `*BasicAgentBuilder` | BasicAgent 低阶 |
| `agent_v2.NewSequentialAgentBuilder()` / `NewParallelAgentBuilder()` / `NewLoopAgentBuilder()` / `NewRoutingAgentBuilder()` / `NewReflectAgentBuilder()` / `NewHandoffAgentBuilder()` → `*XxxBuilder` | 加 `NewBasicAgentBuilder` 共 **7 个低阶编排 builder**（已从 compose_v2 移入 agent_v2，跟 `migration-history.md` §核心类型速查统一表述）；plan **无低层 builder**（已 inline 进 `PlanAgent`，commit `856bb6e`）；加 `compose_v2.NewBuilder` 通用图构造 = 8 入口 |

### Run + RunOption

```go
err := compose_v2.Run(ctx, g, cm,
    compose_v2.WithInitialWrites(...),       // 启动时种子写入（user input 等）——**只在新开路径 apply**
    compose_v2.WithResumeFrom(snap),         // Resume 时传入 cp snapshot（与上一行互斥：续跑时种子不注入，值从 cp 恢复）
    compose_v2.WithResumeAnswers(map),       // HITL Resume 时传 user 答案
    compose_v2.WithHooks(orchHooks),         // OrchestrationHooks（graph 调度级）
)
```

### Checkpoint / Resume

```go
// 业务侧通常通过 agent_v2.Agent.Resume 高层 API（封装好底层）：
events, err := agent.Run(ctx, sess, input)
if ie, ok := compose_v2.IsInterrupt(err); ok {
    // **业务统一用 AllQueries() accessor** (不要直接读 ie.Query.X — ISSUE-81 toolsFn
    // fanout 多 tc 同时抛 Interrupt 时漏 N-1 个 query, 详 hitl-resume.md §2.5)
    answers := make(map[string]*schema.Message)
    for _, q := range ie.AllQueries() {
        answers[q.InterruptID] = schema.UserMessage("user answer for " + q.Question)
    }
    events, err = agent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
}

// 跨进程持久化时手动 Marshal/Unmarshal cp:
cp, _ := cper.LoadNS(ctx, sessionID, "")  // 从 store 取 cp
data, err := compose_v2.MarshalCheckpoint(cp)         // ← 序列化（不是 MarshalSnapshot）
// 存到 Redis / Postgres / S3 ...
cp2, err := compose_v2.UnmarshalCheckpoint(data)      // ← 反序列化
cper.SaveNS(ctx, sessionID, "", cp2)

// 或低阶 compose_v2.Run 直接传 cp:
err := compose_v2.Run(ctx, g, newCm,
    compose_v2.WithResumeFrom(cp2),
    compose_v2.WithResumeAnswers(map[string]*schema.Message{    // ← 类型 *schema.Message
        "ask_id": schema.UserMessage("user answer"),
    }),
)

// 检查 cp pending interrupts:
for _, q := range cp.PendingInterrupts {
    fmt.Println("ask:", q.Question, "id:", q.InterruptID)
}
```

### Durability（崩溃恢复，可选）

`WithDurability` 控制 cp **写入时机**（默认 `exit` = 历史行为，零额外 I/O、零行为变更）。长 / 贵 Run 需要**硬崩溃恢复**（kill -9 / OOM / 断电——出口 defer 不跑）时显式开 `sync` / `async`：框架在**每个超步边界**把全量 cp 写到持久后端，崩溃后从最近超步 Resume。

```go
// 低阶 compose_v2.Run 直接传 RunOption：
err := compose_v2.Run(ctx, g, cm,
    compose_v2.WithCheckpointer(cper),       // 崩溃恢复需配持久后端（redis/ 或 sql/，非 InMemory）
    compose_v2.WithSessionID("task-123"),
    compose_v2.WithDurability(compose_v2.DurabilitySync), // exit(默认) / sync / async
)

// agent_v2 编排没暴露 WithDurability——经 ctx 透传（同 WithCheckpointerCtx 一致模式）：
ctx = compose_v2.WithCheckpointerCtx(ctx, cper)
ctx = compose_v2.WithDurabilityCtx(ctx, compose_v2.DurabilitySync)
events, err := agent.Run(ctx, sess, input) // 内部 Run resolveDurability 读 ctx；嵌套子图自动继承父档
```

| 档 | 写时机 | 取舍 |
|---|---|---|
| `exit`（默认）| 仅出口 defer（error/interrupt 写、成功 DeleteSubtree）| 短任务零额外 I/O；硬崩 mid-run 丢全部进度 |
| `sync` | 每超步**同步阻塞**写 | 最可靠；每步一次后端 I/O 延迟串进时间线 |
| `async` | 每超步**后台单 worker**写（FIFO 有序 + 出口 join drain）| 快（重叠 compute/IO）；崩溃丢最近未 flush 的（最坏 buffer 深度，默认 8）|

**关键**：
- 三档都"成功即清"——`sync`/`async` 的每超步 cp 写同 `(sid,ns)` key，成功 Run 出口 `DeleteSubtree` 照清，成功完成不留 cp。
- 崩溃恢复价值只在配**持久** Checkpointer（`redis/` / `sql/`）时兑现——InMemory 随进程死。
- **同 sid 复用注意**：开 sync/async 后硬崩会在 `(sid,ns)` 留 Pending cp。直接调 `compose_v2.Run` 时，下次**同 sid** 跑（没传 `WithFreshRun`）会 auto-load 当未完成任务续跑——正是崩溃恢复要的；想用同 sid 跑**全新任务**必须 `WithFreshRun` 或换新 sid。`agent_v2` 的顶层 `Run` 恒是新任务（推「新开」信号、不捡旧档），崩溃后接着跑走 `Resume`。

### 软取消（优雅停止，可选）

`SoftCancelSignal` 是外部异步「优雅取消」句柄：业务在另一 goroutine `Trigger()`，Run 在**下个超步边界**优雅停下并存档（可 Resume）。区别于 `ctx.Done()`（硬停：正在跑的超步被掐断、进度回滚到上一超步边界）。**两者都存档**——差别在「当前超步善不善终」，不在「存不存档」。

```go
sig := compose_v2.NewSoftCancelSignal()
go func() { <-userStopBtn; sig.Trigger("user requested stop") }() // 任意 goroutine

err := compose_v2.Run(ctx, g, cm,
    compose_v2.WithCheckpointer(cper),   // 存档 + Resume 需配 Checkpointer
    compose_v2.WithSessionID("task-1"),
    compose_v2.WithSoftCancel(sig),
)
if errors.Is(err, compose_v2.ErrSoftCanceled) {
    // 已停在干净的超步边界、状态存档；可 Resume 续跑或 ResumeActionAbort 弃用
}

// agent_v2 编排：经 ctx 透传（嵌套子图自动继承同一信号）
ctx = compose_v2.WithSoftCancelCtx(ctx, sig)
events, err := agent.Run(ctx, sess, input)
```

| | 软取消 `SoftCancelSignal` | 硬停 `ctx.Done()` |
|---|---|---|
| 何时停 | 下个超步边界（当前超步所有分支先跑完）| 立即（ctx 检查点）|
| 状态 | `frontierAtExit` + `SaveNS` 存档，可 Resume | 回滚到上一超步边界后**同样存档**（`shouldStayPending` 含 ctx 错 → Status=Pending），也可 Resume |
| 返回 | `ErrSoftCanceled` | `context.Canceled` |

**关键**：
- **一次性**：每次 Run / Resume 建**新**信号，不跨 Run 复用已触发的（复用已触发信号去 Resume 会在节点重跑前于首个超步边界就停）。
- **边界丢弃**：Trigger 到达**最后一个超步**是 no-op——Run 正常成功（`err==nil`），软取消被丢弃（任务本就要结束）。别依赖「Trigger 后 err 必是 `ErrSoftCanceled`」。
- 存档可 Resume 需配 Checkpointer；不配则停下但无存档。

### 并发写回检测（同 sid 被两个副本处理，可选）

同一 sessionID 被**并发** Run/Resume（会话被路由到两台机器 / 用户狂点两次）时：双方各自载入同一份 cp、各自执行、各自写回 —— 后写者静默覆盖先写者，**两边都返回 `err=nil`**，事后无从察觉。

```go
err := compose_v2.Run(ctx, g, cm,
    compose_v2.WithCheckpointer(cper),
    compose_v2.WithSessionID("task-1"),
    compose_v2.WithStaleWriteGuard(),      // 开启
)

// agent_v2 9 编排：**必须走 ctx**（编排内部自拼 RunOption，不透传业务的）
ctx = compose_v2.WithStaleWriteGuardCtx(ctx)   // 嵌套子图也靠它继承
events, err := agent.Run(ctx, sess, input)
// ⚠️ 顶层 agent.Run 恒是「新开」，没有世代基线：默认 exit 档下这次 Run 全程不受保护
// （成功清理时还会删掉槽里别人的挂起档）；sync / async 档从第二次写入起才受保护。
// 两个 Run 并发撞同一 sid 本身违反 RunID 契约，真解是调用方串行化

// 消费：引擎出口写档撞上陈旧写回时返 ErrCheckpointNotPersisted，中断不在链上——两者不会同时出现
if errors.Is(err, compose_v2.ErrCheckpointStale) {
    // 本次结果没落盘。**副作用可能已经重复发生**——补偿 / 告警 / 人工介入，
    // 不要简单重试（重试只会再跑一遍副作用）
}
```

**它做什么、不做什么**（别当成"开了就安全"）：

| | |
|---|---|
| ✅ 做到 | 让**同一 ns 内**"我读到那代之后被人推进过"不再静默——至少一方拿到明确错误 |
| ❌ 做不到 | **阻止副作用双跑**。检测在写回时，那时两个副本都执行完了 |
| ❌ 做不到 | **原子**。read-then-write 有窗口，两副本可能都通过 |
| ❌ 做不到 | **跨层看见冲突**。多层编排每层各自载入、各自比对，子层的基线就是它**刚读到的那代**——检测窗口只覆盖本层"读到写"这一段。父层被拒时子层已落盘的也不回滚（盘上会留下父子来自不同副本的组合）|
| ❌ 做不到 | **管 cp 以外的写**。判定点全在 cp 上；Handoff 的控制权转交、StoreMemory 的记忆都经 `store.Store` 在**图内**落盘，早于出口判定——Run 返 stale 时它们可能已经生效了 |

**真解在调用方**：会话亲和路由 / 分布式锁 / 串行队列。上层 `agui` 和 `a2a/server` 已各自拒绝同 threadID/taskID 并发，走那两条路通常不需要本 option。

**其它要点**：
- **默认关闭**；`Checkpoint.Epoch` 世代标记**无条件写入**（不开时只写不读）。业务自研 backend 照常序列化即可，**漏了守卫会静默失效**（conformance 有断言把关）。
- **覆盖 5 个判定点**：3 个 cp 写点（出口 + `Durability=Sync` 每超步 + Async worker）+ 成功清理前 + 终局失败清子树前。删比覆盖更不可逆——删掉的可能是另一个执行者的 HITL 存档。
- ⚠️ **开了之后，一个跑成功、无中断、无业务错的 Run 也可能返 stale**（成功清理时发现槽被推进）：本次执行整个建立在过时状态上，"成功"本身就可疑。
- **`WithFreshRun` 会让守卫整个失效**（两者同开时）：freshRun 刻意不载入，没有基线可比，写回一路放行。打在**已占用**的 sid 上就是静默覆盖别人的存档——而那恰恰是守卫号称要防的后果。要"重头跑"又要防冲突，先 `DeleteSubtree` 清干净再跑。
- **`WithResumeFrom` fork 到的目标 sid 必须是干净的**。槽里已经有别人的 cp 时，基线（caller 给的那代）跟槽里那代对不上 → 判 stale + 本次不落盘。fork 前先 `DeleteSubtree`，或换一个没用过的 sid。
- **拿到 stale 之后别 Resume**。`Resume` 会载入盘上那份（也就是**对方**的）状态接着跑，产出一个混合结果而且 `err=nil`——比原来的静默覆盖更难察觉。要么补偿后重开会话，要么人工介入。
- **三条静默失效路径**（都有 log）：无基线（全新 Run 正常；Resume 时可能是旧 cp 无 Epoch 或 backend 漏序列化）/ 槽里 Epoch 为空（对手跑旧代码）/ `LoadNS` 报错时 fail-open（护栏不是主链路，因一次读抖动拦掉 HITL 存档更糟）。

### 工具执行审批（HITL 权限层，BasicAgent，可选）

`WithApprovalMode(ApprovalAsk)` 让每个**业务**工具执行前抛 `InterruptError` 等 caller 审批——复用 HITL Resume 机制（ask_user 同款稳定 ID + cp 挂起）。默认 `ApprovalAuto`：还没形成审批结论的调用直接放行、零开销。框架工具（ask_user / activate_skill / …）豁免。

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithToolkit(tk),
    agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
    // 可选启发式：白名单只读工具直接放行（框架不内置 shell allowlist，规则内容业务自负）
    agent_v2.WithApprovalHeuristic(func(_ context.Context, name string, _ map[string]any) agent_v2.ApprovalDecision {
        if name == "read_file" {
            return agent_v2.ApprovalDecisionAllow
        }
        return agent_v2.ApprovalDecisionDefault // 交回 mode（Ask → 询问）
    }),
)

ctx = compose_v2.WithCheckpointerCtx(ctx, cper) // 审批骑 cp 挂起
events, err := ag.Run(ctx, sess, input)
if ie, ok := compose_v2.IsInterrupt(err); ok {
    // ⚠️ 两处都必须按 **query 粒度** 判，别用 ie 粒度——同一轮 N>=2 个 tool_call 都要审批时，
    // 框架把它们 MergeInterrupts 成一条中断，此时：
    //   1. **ie.Query 为 nil**（问题都在 Queries 容器里）→ 写 ie.Query.X 会 panic
    //   2. **ie.Reason 是一句合并说明**（「聚合 N 个 InterruptError …」；每个 ie 的 Reason 被拷到
    //      各自的 q.Reason 上）→ 写 ie.Reason == "tool_approval" 会漏掉整个 fanout 场景
    // 实测：两个工具同轮调用 → AllQueries() 返 2 条、ie.Query == nil、ie.Reason 不是 "tool_approval"。
    answers := make(map[string]*schema.Message)
    for _, q := range ie.AllQueries() {
        if q.Reason != agent_v2.ApprovalInterruptReason { // == "tool_approval"
            continue // 混合场景：同一批里可能还有 ask_user 等别的中断
        }
        // 把 q.Question + q.Options[approve/approve_always/approve_always_tool/deny] 展示给用户
        answers[q.InterruptID] = schema.UserMessage(agent_v2.ApprovalOptionApprove)
    }
    events, err = ag.Resume(ctx, sess.RunID,
        agent_v2.WithResumeSession(sess), // ← 让 approve_always 规则跨 Resume 持久（见下「关键」）
        agent_v2.WithResumeUserInputs(answers))
}
```

| Resume 答案 | 行为 |
|---|---|
| `approve` | 本次放行 |
| `approve_always` | **这件事**不再询问——这个 agent 的同一工具 + 同一参数指纹（指纹算法可用 `WithApprovalScope` 换）|
| `approve_always_tool` | 这个 agent 的**这个工具**的任何调用都不再询问（工具级通配）|
| `deny`（或空 / 未识别）| 拒绝执行，返工具级 `[tool_denied]` 给 LLM 继续 ReAct（**不 abort 整 Run**）|

**关键**：
- **三样状态、两个载体**：`approve` 的「这次调用已批过」与这次调用的审批结论记在 tools 节点的**私有中断态**里（`toolCallMemo.Approved` / `Decided`，与 tool memo 同一把键、随 cp 走）——跨 Resume 自动带着，不需要额外配置。`approve_always` / `approve_always_tool` 的**规则集**存 RunSession KV（不走 cp），跨 Resume 持久**需 caller 传 `WithResumeSession(sess)` 复用同一 session**——默认 Resume 新建空 session，规则会丢、重新询问。规则集**按任务（RunID）分**：`sess.NextTask()` 换了 RunID 就是新的一套，复用同一个 RunID 开新任务时由编排入口清掉。规则**只对答应它的那个 agent 实例生效**：编排的其他成员、嵌套调用的被调方或调用方同名的工具是另一个工具，照样要问（沙箱子 agent 里答的「一直允许」曾放行父 agent 的同名生产工具，实测）；agent 被重建（每个请求经工厂重建的接入如 agui、每次 fork 都新建实例的 skill resolver）就是新的身份，再问一次。
- **启发式 seam 不内置策略**：框架只给机制 + `WithApprovalHeuristic` 回调，shell allowlist 等具体安全策略业务自负。
- **每次工具调用只判一次，判的就是执行的**：一批工具调用的内容（`OnToolStart` 改写之后的工具名与参数）在这一批第一次交给工具执行时定下，中断续跑、失败续跑时按定下的内容执行（不论审批开没开）；审批结论在第一次形成时记下，续跑原样重放、不再调启发式（需要配 checkpointer）。所以启发式判过的、用户在审批问题里看到并批过的，就是实际执行的那一份——续跑时 `OnToolStart` 再改写（注入时间戳、请求号，或改了工具名）对这次调用不生效；启发式也可以调模型判断、按时间或额度判断，用户的答复不会被一次重新判定推翻。判断本身失败时返回 `ApprovalDecisionAsk` 交给人。结论不会过期：额度用完了，这次调用先前的 Allow 照样作数。启发式拿到的 args 是拷贝，就地改了不影响执行（要改参数用 `OnToolStart`）。例外是工具中间件：它在审批之后、每次执行都跑，它改的参数不经审批（见下方「包装工具调用」那条）。
- **一次调用只准入一次**：经启发式 / 模式默认判 Allow、用户批准、approve_always 规则放行，任何一条路准入之后，续跑都不再问、不再被推翻——调用方续跑时对同一个审批 ID 改答 deny 不推翻（工具可能已经开始执行，推翻就只执行了一半），不带原 session 续跑、规则丢了也不再问。
- **审批配置在 Run 与 Resume 之间变了**（滚动发布时加上或去掉启发式、换 `ApprovalMode`、开关审批）：已经形成结论的调用沿用它——用户答过的拒绝照样作数，判过 Allow、批准过的不会被重新拦下；新配置只管还没形成结论的调用。审批关着时不形成结论：续跑时打开审批，这次调用按新配置要审批，工具体若已开始执行、被拒时就只执行了一半。审批问题在自己的相位里占提问序号，工具自己的提问不随审批问不问挪位，两边的答复不会串。
- **仅业务工具受审批**：框架铸的工具（ask_user / activate_skill / run_skill_action / tool_search / transfer_to_agent / route_to / 结构化输出的 final_output）豁免。判的是**实例**上的封闭标记、不是名字（`agent_v2/framework_tool.go`）：业务自己写一件叫 ask_user / final_output / respond 的工具照常受审批。同一个判定也决定「哪些工具不过 `WithToolMiddleware` 链」、不受工具超时、不截断结果、不进工具账、对重复调用检测透明。
- fanout 多工具同需审批 → `MergeInterrupts` 一次问完；`ApprovalAuto`（默认）且没配启发式时，还没形成结论的调用直接放行、零开销。

### 包装工具调用（观测 / 短路 / 挂起）

`WithToolMiddleware` 包住每次**业务**工具调用，业务不碰 Tool 对象：

```go
agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
    return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
        if hit, ok := cache.Get(name, args); ok {
            return hit, nil          // 短路：不调 next，工具不跑
        }
        res, err := next(ctx, name, args)
        slog.InfoContext(ctx, "tool done", "name", name)
        return res, err              // 错原样返，别吞
    }
})
```

先注册的在最外。顺序：

```
memo 命中 → 渐进披露 gate → 审批 → [ 中间件链 ] → 工具
```

**五条契约**（每条都有实测撑着，违反的后果都是静默的）：

| | |
|---|---|
| **`next` 至多调一次** | 调两次 = 同一个提问换两个身份（中断身份含一维「本次调用里第几次问」，读一次 tick 一次）。实测重试一轮后同一个问题拿到新 ID，用户答过的那条永远配不上。要重试在**工具体内**做 |
| **框架自带工具不过链** | 它们的真产出不在 tool_result 里（`transfer_to_agent` 的决定是交给上层 HandoffAgent 的上行信号、`tool_search` 写的是揭示集），短路掉是静默失败：`err=nil`、tool_result 齐全，只是转交没发生 |
| **持状态要 thread-safe** | 一轮 N≥2 个工具时，同一条链在 N 个 goroutine 里并发跑 |
| **里面调 `Ask` 要么恒调要么恒不调** | 条件调会移动工具体的中断身份（同一维计数器），两次续跑之间条件一变，工具体的 ID 就漂、答案静默错配 |
| **改 args 不重新触发审批** | 审批排在中间件外面。要改参数用 `hook.Hooks.OnToolStart`，那里是消费档且在审批之前 |

**吞掉中断会被框架挡住**——业务写 `if err != nil { return fallback, nil }` 这一句就能让 HITL 静默死掉，框架检测到会把原信号顶回去并报错。

> ⚠️ **别用「嵌入 `tool.Tool` 包一层」的写法**。Go 的可选接口**不穿过嵌入**：
> `StreamableTool`（流式）和 `Categorizer`（trace span 的组件类型）会静默丢掉——框架靠
> 类型断言识别它们，嵌入出来的新类型不实现。实测 `可流式 true→false`、
> `有组件类型 true→false`，没有任何报错。中间件包的是**调用**不是对象，碰不到它们。
>
> ⚠️ 工具审批别在 `OnToolStart` 里自己做，用
> [`WithApprovalMode(ApprovalAsk)`](#工具执行审批hitl-权限层basicagent可选)——那个钩子在
> 工具执行单位**外面**，拿不到 memo / `approve_always` / 批量合并。
>
> ⚠️ **`OnToolStart` 改参数，就是改这次调用的内容**：第一次执行时定下、随中断档 / 失败档落盘，续跑沿用（续跑时
> 钩子照样触发，但改写不生效）。所以**每次执行都要新取的值**（凭证、短时令牌、trace_id）别在这里写进参数——写进去
> 会落盘，短时令牌还会被冻在第一次的值上。放钩子返回的 ctx（只用于这次执行、不落盘），或用 `WithToolMiddleware`
> 在每次执行时注入（也不落盘，但它改的参数不经审批）。代价：改写让这次调用出错时，修好钩子再续跑也不生效——这一批
> 按第一次的内容执行，恢复办法是放弃这个任务、`sess.NextTask()` 重开。

### 工具级渐进披露（BasicAgent，可选）

工具多到全量 schema 喂 LLM 爆 context 时，`WithToolSearch` 让 deferred 工具初始不进 bind list，LLM 先调 `tool_search(query)` 检索解锁再调用。常用 `Deferred: mcp.IsRemoteTool` 延迟所有 MCP 工具。完整设计 / API / 边界见 [`toolsearch.md`](toolsearch.md)。

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithToolkit(tk), // 含大量 MCP 工具
    agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcp.IsRemoteTool}),
)
```

### 会话历史（BasicAgent，可选）

把消息流按**会话**留下来，跨 Run 活着。`WithHistory` 是 opt-in——不配就一行不写、零开销。

```go
hs := agent_v2.NewInMemoryHistoryStore()   // 生产接 Redis / PG，见下
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("assistant"),
    agent_v2.WithModel(m),
    agent_v2.WithHistory(hs, "tenant-a"),  // owner 必填、无默认值
)

ctx = compose_v2.WithThreadIDCtx(ctx, "conv-42")   // 会话身份，必须声明
ag.Run(ctx, sess, schema.UserMessage("A 还有货吗"))
ag.Run(ctx, sess, schema.UserMessage("那给我留一件"))  // 第二轮看得到第一轮的全过程
```

**开不开，差在模型第二轮看到什么。** 不开时喂给模型的「上一轮」读自 `RunSession.Events`，
而 events 只记首尾：

```
[0] user       "A 还有货吗"
[1] assistant  "A 商品还有 3 件"     ← 结论在，来源没了
[2] user       "那给我留一件"
```

上一轮调过 `查库存` 工具、工具返回「库存只剩3件」——这两条模型完全看不到。后果不只是
信息少：它无法判断「3 件」是刚查的还是编的，也就无法判断该不该重查。开了之后拿到的是
逐字完整流，`tool_call` 和 `tool_result` 都在。

**没开是有意的降级**，不是没实现——框架照常跑，只是模型看到的历史残缺。

三条必须知道的：

| 事项 | 说明 |
|---|---|
| **`Owner` 必填** | `ConvID` 通常来自 wire（HTTP `threadId` / A2A `ContextID`），是**续接标识符不是授权令牌**——猜中别人的就能拉到别人的完整对话。单租户填固定值即可，多租户用 `WithHistoryOwnerFunc(fn)` 按请求解析。构造期 fail-fast。 |
| **没声明会话身份就不写** | 没 `WithThreadIDCtx` 就没有「这是哪个会话」，写下去也无从取回。框架每次调用 warn 一次并跳过，`prior` 回落到 events。`a2a/server`（ContextID）和 `agui`（threadId）会自动注入。 |
| **旁路，不影响跑对** | 写失败 / 后端 panic 一律吞 + warn。工具重不重跑、Resume 从哪继续全由 cp 的 frontier 决定，跟历史一条不沾。 |

**归属**：一场会话里所有 agent 的消息在同一条流上，每条盖 `HistoryAgentAnnotation`
（值 = 该 agent 在编排树里的**位置**——分支名 / 步骤名 / 转交目标名，顶层回落 agent 名；另有 `HistoryAgentDisplayAnnotation` 存 agent 名供展示）。嵌套子 agent 沿用父的写入器——一场会话只该有一条流。**内部决策 LLM
不进历史**：routing 分类器 / reflect 评估器与反馈教练 / plan 规划器产出的是给框架看的控制信号，
框架已自动摘除，`NewPlanAgent` 的 planner 参数与 `WithRouteDecider` 挂的决策方同样；业务自己的
Router（`WithRouter`）、Reflector（`WithReflector`）、评估函数（`WithEvalFunc`）、循环退出条件
（`WithLoopExitCondition`）等内部跑子 agent 时，要自己在子 ctx 上调一次
`agent_v2.WithoutHistoryCtx(ctx)`——框架直接调用的业务回调都一样。

**自研 backend**：实现 `Append` / `Load` 两个方法，跑 conformance 验语义——

```go
import "crew-go/agent_v2/history_conformance"

func TestMyRedisHistory(t *testing.T) {
    history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
        hs := mybackend.NewRedisHistory("redis://localhost:6379")
        t.Cleanup(func() { _ = hs.Close() })
        return hs
    })
}
```

**`Append` 必须只追加**——「增量写不需要游标」完全建立在这上面。做了去重 / 改写 /
截断，「上次那轮到底说了什么」就不可信了。这也是它**不复用 `store.Store`** 的原因：
那层为了保持当前认知必须能改写和删除既有条目，写入语义直接冲突。

设计与决策依据见 [`doc/issue/issue-98-conversation-history.md`](../issue/issue-98-conversation-history.md)。

## 3. HITL 模式（人在回路）

### 节点抛 InterruptError

```go
func myNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    // GetResumeAnswer 返 (*schema.Message, bool)——含完整 Role/Content/ContentBlocks/Annotations
    if ans, ok := compose_v2.GetResumeAnswer(ctx, "my_ask_id"); ok && ans != nil {
        // Resume 路径：节点重跑，answer 已注入
        return businessLogicWithAnswer(ans), nil
    }
    // 仅需文本时用便利 helper：
    //   if text, ok := compose_v2.GetResumeAnswerText(ctx, "my_ask_id"); ok { ... }

    // 首次执行：抛中断
    return nil, &compose_v2.InterruptError{
        Reason: "needs user confirm",
        Query: &compose_v2.AskQuery{
            InterruptID: "my_ask_id",  // 业务自定义 ID
            Question:    "确认操作？",
            Options:     []string{"yes", "no"},
        },
    }
}
```

### 关键不变量

- **InterruptID 必填，框架不替你补**——推荐走 `compose_v2.Ask`（它按 `InterruptSite` 算完立刻用）；自填也行，只要保证跨 Resume 稳定 + **同一批内互不相同**（撞了会被当场拒，`ErrRunAborted`）
- **节点重跑安全**：抛中断后节点的 ChannelWrite **全部丢弃**——节点必须能多次跑（幂等）
- **多 ask 并存**（fan-out 多 worker / toolsFn fanout 多 tool_call 各自抛 ask_user）：
  - 单 ie 多 query：`InterruptError.Queries` 容器（与 `Query` 互斥 Queries 优先；MergeInterrupts 聚合 helper, ISSUE-81）
  - Resume 用 `ie.AllQueries()` 拿全部 query；或 `cm.PendingInterrupts()` 拿全 cm 视图
- **OnAgentEnd 协议**（ISSUE-82 后）：Run defer 1 次 + 每次 Resume defer 1 次（HITL 闭环算 2 次）；覆盖所有退出路径（success / Interrupt / panic / ctx.Err）

### toolsFn ctx 协议（ISSUE-78/81）

业务自定义 toolsFn 想让被调工具拿 tc.ID 算 childNS / 算稳定 InterruptID 时：

| Helper | 何时用 |
|---|---|
| `compose_v2.WithCurrentToolCallContext(ctx, tcID, toolCallIdx)` | **并发 toolsFn** — 一站式注六样：「在 tool_call 里」的章（`InToolCall`）+ tc.ID + tc 下标（批内第几个，不是图级分支下标）+ 独立的提问计数器（atomic.Int64）+ 独立的嵌套调用次序计数器（`NextSubCallIndex`）+ 节点私有态的 tool_call 分域 (ISSUE-81 fanout 必用，否则多 tc ask_user InterruptID 撞；并发的嵌套调用输入相同时按调度先后抢编号、续跑时串存档) |

业务自定义 fanout toolsFn 用 `WithCurrentToolCallContext` 后，业务工具抛 InterruptError 提问走 `compose_v2.Ask`（自填 ID 会被拒） (用 ctx 推的 tc 下标 + 计数器算)。

### Store 嵌套 sub-agent 协议（ISSUE-84）

> 历史背景：`agent_v2.Memory` 接口已删（PR 3.3 commit `e6505f9`），统一改走 `store.Store` 接口 + `WithStore` / `WithStoreMemory` helper。

**「双重持久化」这个问题已经不存在了**（2026-08）。它当年的形态是「顶层和 sub-agent
把**同一条内容**写到**同一个键**上」——根因是键里没有写入者维。补上那一维之后，顶层写
基准 ns、成员写 `基准ns/<写入者位置>`，本来就不会重复。

⚠️ **别再照下面这个老写法实现后端**：

```go
// ❌ 已废：这样写会让成员的 WithStoreMemory 一条也落不了盘
func (m *MyMem0) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
    if agent_v2.IsSubAgentStoreScope(ctx) {
        return nil // 「让顶层唯一写入」——那个前提没了
    }
    return m.client.Add(ns, key, value)
}
```

实测代价：Parallel 两分支各配了 `WithStoreMemory` → `Put` 被调 2 次、被丢弃 2 次、
落盘 **0** 条、`Run.err=nil`。业务看不到任何异常，只是记忆一直是空的。

flag 仍然 push，正当用法是**按位置给不同待遇**而不是按位置丢弃：

```go
func (m *MyMem0) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
    if agent_v2.IsSubAgentStoreScope(ctx) {
        return m.client.AddWithTTL(ns, key, value, 24*time.Hour) // 成员的记忆留短一点
    }
    return m.client.Add(ns, key, value)
}
```

agent_v2 调用别的 agent 的每一处都自动 push `WithSubAgentStoreScope(ctx, true)`（详 `store.md` §10.5 #4 producer/consumer 协议节）。**框架自己不读这个 flag**——它曾经守在 `commitFinal` 的 `store.Put` 前面，那道闸让成员的 `WithStoreMemory` 完全失效，已删。**push 点收敛在 4 处**——`runMember`（**6 个**编排调自己的成员，不含 Handoff）/ `runDecisionAgent`（分类器 / 评估器 / 反馈教练）/ `runNestedCall`（嵌套调用的执行器：`RunSubAgent` + `AgentAsTool` + SkillTool fork）/ Handoff 铸造。

**业务 store.Search 看父对话历史**（`sess.RootSession()` + `HistoryFor(...)` 范式）：

ISSUE-79 半共享 child sess 协议下 sub-agent 拿的 sess 含 parentSess 反向指针, `RootSession()` 沿链上追到根 sess。业务自定义 `store.Search` 想看父 BasicAgent 完整对话历史时:

```go
func (m *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
    sess := agent_v2.GetSession(ctx)
    if sess == nil { return nil, nil }
    // 选 1: HistoryFor 改写非 owner 消息为 user (适合摘要输入)
    msgs := sess.RootSession().HistoryFor("")
    // 选 2: HistoryForRaw 保原 role + 标 from_agent annotation (ISSUE-84 MED-3, 适合 LLM 对话历史)
    // ⚠️ 业务把 History* 结果**直接喂 ChatModel** 时注意：重建历史里 return-direct 出口
    // （transfer_to_agent / route_to）的 Exit 是带 tool_call 的 assistant 且恒无配对
    // tool_result（悬空）——直喂真实 API 会 400。框架 BasicAgent 路径已在
    // buildInitialMessages 汇合层自动剥离；业务绕过框架直喂需自行剥离悬空 tool_call。
    // msgs := sess.RootSession().HistoryForRaw("")
    return convertToSearchItems(msgs), nil
}
```

**9 sub-agent 调用入口 sess 协议决策矩阵**（ISSUE-84 MED-2 文档化 + audit MED-3 加 store.Put 列 + audit² ParallelAgent / Routing classifier 协议补齐）：

| 编排 | child sess 行为 | store.Search 看到 | store.Put `IsSubAgentStoreScope` |
|---|---|---|---|
| `BasicAgent`（顶层） | 主 sess | 顶层 sess 完整 events | `false` (顶层入口) |
| `SequentialAgent` / `LoopAgent` / `RoutingAgent` / `ReflectAgent` / `HandoffAgent` / `PlanAgent` | 直接传 parentSession | 全量父历史 + 自动写父 events | `true` (ISSUE-84 audit HIGH-1 修后) |
| `ParallelAgent` | `ForkLane` lane child sess | 共享父读视图 + 写隔离 (lane events 物理隔离 ISSUE-80) | `true` (ISSUE-84 audit HIGH-1 修后) |
| `AgentAsTool` / `SkillTool fork` | `NewSharedChildSession` 半共享 | events 完全独立 + parentSess 反向指针 (业务 store.Search 用 RootSession 上追) | `true` (ISSUE-79 实施时已修) |

**同一 sub-agent 实例放在不同编排里 sess 行为分裂**——业务自定义 Store 实现按
此设计。store.Put 协议 sub-agent 调用方覆盖**完整闭环**（ISSUE-84 audit HIGH-1）。
push 点**曾经是 15 个、散在每个编排各自的派发代码里**，靠注释里一句人肉计数维持——
`5afb8eb` 那次加协议就漏了 Parallel 和 Routing 的 classifier。现在收敛成 4 处：
`runMember` / `runDecisionAgent` / `runNestedCall`（RunSubAgent + AgentAsTool + SkillTool fork）/
Handoff 铸造，哨兵 `TestMemberDispatch_AllAgentRunsGoThroughHelper` 机械盯着有没有第 5 处。
业务 Store 实现内查 `agent_v2.IsSubAgentStoreScope(ctx)` 跳过
sub-agent 调用——⚠️ **别这么做**：框架自己已不读这个 flag（那道闸曾让成员配的 `WithStoreMemory` 一条也落不了盘，已撤）。防重复写现在靠「写入者维进 ns」：顶层写基准 ns、成员写 `基准ns/<写入者位置>`，本来就不重复。这个 flag 只适合用来「按位置给不同待遇」，不适合整个 skip。

**业务陷阱 (audit² 揭露)**: 修前 ParallelAgent fan-out N branch + 共享 Store 实例
(mem0 / Postgres 单例典型) → N+1 倍持久化 (N branch_final + 顶层 ParallelAgent 不
commitFinal 0 + 顶层调用方 1 if BasicAgent). audit² 修 ParallelAgent + Routing
classifier 漏 push——但**结论已经变了**：现在顶层与成员写的是不同 ns，N+1 倍持久化的前提不成立，别再用 `IsSubAgentStoreScope(ctx)` 跳过（会把框架的修复抵消掉）。

## 4. 子图嵌套模式

### 静态拓扑（推荐）

```go
// 子图作为父图节点
parentB := compose_v2.NewBuilder().
    AddSubgraphNode("sub_a", subGraphA,
        compose_v2.WithImport(parentSharedCh),  // 子图能读父 channel
        compose_v2.WithExport(subResultCh),     // 子图写冒泡到父图
    )
```

**关键**：
- 默认完全隔离（不 import/export 的 channel 子图看不见、写不到）
- 跨边界 channel **必须父子图共享同一 *Channel[T] 引用变量**（保证类型一致）
- 多层嵌套（祖父 → 父 → 子）必须**逐层显式声明** import——不自动透传

### import / export 的值语义：baseline 层，不是 reducer 写

- **import 摆的是子图的 baseline**（一层不经 reducer 的底层值），每次进入（新开或续跑）都从父图
  重新摆。子图内 `Get` 读到 **reduce(baseline, 子图自己的写)**：没写过就是父图的值；写过就是
  父图的值叠上自己的——`Append` 看到 父的 ++ 自己的，`Sum` 看到 父的 + 自己的，`LastValue` 看到
  自己的。子图自己的写**不会**在续跑时被父图的值盖回去。
- **export 只送子图自己写的那部分**，经父图 reducer 合并；子图没碰过的 channel **不回送**。
- 因此 **同一个 channel 同时 `WithImport` + `WithExport` 是正当用法**：子图读到父图的值、在上面
  累加、只把增量送回去（父 `Sum` 5、子图 +3 → 父 8）。
- ⚠️ 子图内 reducer 收到的 `prev` **只含子图自己的累积**，不含 baseline。这不等于「结果错」——
  合成读会让 reducer 再跑一遍：「不在 prev 里才追加」那种去重**照样工作**（实测父 `[a]`、子写
  `[a,b]` → 子图读 `[a b]`、父最终 `[a b]`）。真正会分叉的是不满足下面那条前提的形状
  （见反模式节：计次 `p+1`、平均、不判空的拼接）。
- 为什么不是「把值写进去」：写进去会经 reducer，对 `Sum`/`Append` 是**相加**。修之前实测：子图
  没碰的 `Sum` 被原样回送父图 5→10、`Append` [a]→[a a]；带子 cp 续跑时 import 再灌一次，子图读到
  13、父最终 18（正确 8）——全程 `err == nil`。

### 子图独立 cp

子图中断时按 `(sessionID, ns)` 物理分离独立写 cp（v2 已删 v1 时代的 `SubgraphSnapshots` 嵌入式 map）——Resume 时各层独立 LoadNS 恢复，不重跑已完成节点。**对业务透明**——无需特殊处理。

## 5. fan-out per-branch delta（部分失败时）

```go
parB := agent_v2.NewParallelAgent().
    AddBranch("ok1", okAgent).
    AddBranch("ok2", okAgent).
    AddBranch("flaky", flakyAgent)  // 可能 transient fail
```

**行为**（PoC Phase 1.2.a）：
- flaky 失败时 Run 1 返 error
- ok1/ok2 的 writes **保留**（commit 到 cm）
- frontier 仅含 [flaky]——Resume 时只重跑 flaky，**不重跑 ok1/ok2**

**业务侧不需任何改动**——这是框架自动行为。

## 6. 业务级 hook（hook.Hooks）集成

```go
type myHooks struct {
    hook.NoOpHooks
}

func (h *myHooks) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
    // 在 LLM 调用前修改 messages
    in.Messages = append(in.Messages, schema.SystemMessage("追加 system prompt"))
    return ctx, nil
}

func main() {
    g, _ := agent_v2.NewBasicAgentBuilder(model, tools).
        WithName("my_agent").
        WithAgentHooks(&myHooks{}).
        Compile()
}
```

8 个 hook 时机映射对照见 `hook/hooks.go` 顶部 godoc（OnAgentStart/End + OnModelStart/End/EndStream + OnToolStart/End + OnError）。

## 7. 4 层压缩业务用法

**推荐路径**：用 `agent_v2.NewBasicAgent` + `WithMicroCompact / WithAutoCompact /
WithReactiveCompact / WithStore + WithStoreMemory` options，框架自动在 modelFn 入口按顺序触发：

```go
import "crew-go/agent_v2"

ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("chat"),
    agent_v2.WithModel(myLLM),
    agent_v2.WithToolkit(myToolkit),

    // 第 1 层（每轮 modelFn 最早段触发）
    agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true}),

    // 第 2 层（MicroCompact 之后，LLM 调用之前）
    agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{
        Enabled:            true,
        Model:              summaryLLM,
        ModelContextWindow: 200_000,
    }),

    // 第 3 层（LLM 报 prompt-too-long 时 retry 包围触发）
    agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true}),

    // 第 4 层（跨 Run 持久化）
    agent_v2.WithStore(store.NewInMemoryStore()),
    agent_v2.WithStoreMemory(
        store.NS("users", userID, "memories"),
        &agent_v2.StoreMemoryConfig{SearchLimit: 5},
    ),
)
```

**手写 modelFn 的高级用法**（直接用 agent_v2.NewBasicAgentBuilder 拼底层）：可在
modelFn 内直接调 `agent_v2.AutoCompactRun(ctx, cfg, msgs)` /
`agent_v2.ReactiveCompactRun(ctx, cfg, msgs, err)` / `agent_v2.MicroCompactRun(ctx, msgs, cfg)`
等独立函数 (ctx-first；`MicroCompactRun`/`AutoCompactRun` 3 参，`ReactiveCompactRun` 4 参含 callErr，实证 `agent_v2/microcompact.go::MicroCompactRun`)。

```go
import "crew-go/agent_v2"

// ⚠️ 压缩只作用在 msgs，preamble 原样透传——agent 的前言每轮可由配置重算，
// 不该占着被压缩的名额，更不该被压缩层丢掉。把它拼进 msgs 再压缩就白改了。
wrappedModel := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
    msgs = agent_v2.MicroCompactRun(ctx, msgs, microCfg)
    msgs, _ = agent_v2.AutoCompactRun(ctx, autoCfg, msgs)

    out, err := originalModel(ctx, preamble, msgs)
    if err != nil && agent_v2.IsPromptTooLongError(err) {
        msgs, _ = agent_v2.ReactiveCompactRun(ctx, reactCfg, msgs, err)
        return originalModel(ctx, preamble, msgs)
    }
    return out, err
}

g, _ := agent_v2.NewBasicAgentBuilder(wrappedModel, tools).Compile()
```

第 1 层 MicroCompact 同样只在 model 节点入口跑（`makeModelFn` 最早段，先于 AutoCompact），toolsFn 内不调用。

## 8. 常见反模式（**别这么做**）

### ❌ 节点内直接 mutate channel state

```go
func badNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    // 错：节点不应直接 mutate state
    // 应该 return ChannelWrite 让框架在合并点 reduce
}
```

### ❌ 多个分支同时 LastValue 写入同 channel

```go
parB.AddBranch("a", agentA).AddBranch("b", agentB)
// 两个 branch 内部都 Write(myLastValueCh, ...) → 引擎在任何提交之前当场拒
// （errors.Is(err, compose_v2.ErrRunAborted)，cp 子树一并清掉）。
// 真要「谁最后写谁赢」用 Custom(name, func(_, next T) T { return next }, zero) 显式声明
// 应该用 Append 累积或 Custom reducer 合并
```

### ❌ 跨 channel 引用业务字段（应用 InputRefs）

Plan 编排有 `PlanStep.InputRefs []RefSpec` 显式声明依赖（`RefSpec{Index *int}` 按下标 / `RefSpec{AgentName string}` 按 agent 名倒序找）。**别**通过 channel 间接传递——破坏依赖追踪。

### ❌ 忘了 init() 注册类型

最常见的 cp 失败原因。**任何中断 + Resume 路径都会触发 marshal**——必须注册。

### ❌ 给子图 import 一个不满足关联律（或零值不是左单位元）的自定义 reducer

同一 channel 同时 import + export + `Sum` **已经是正当用法**（见 §4「值语义」：import 是
baseline、export 只送子图自己的增量，父 5 + 子 3 = 8）。曾经写在这儿的「会双重累加」是修之前
的行为，别再照它绕路。

现在真正会踩的是**合成读的前提**：子图读到的是 reduce(baseline, own)，它等于「把子图的写
顺序应用在父图值上」当且仅当 reducer 满足关联律、且零值是**左**单位元（`f(zero, x) == x`
——own 从零值起折叠，所以零值在左边）——内置四个都满足；
自定义的计次 `p+1`、平均 `(p+n)/2`、不判空的字符串拼接会分叉（子图内读到的值 ≠ 顺序应用）。
这类 reducer 别 import，或改成满足前提的形状（计次改 `Sum`、拼接判空）。

### ❌ RunSession.SetValue/GetValue 存跨 Run 数据

```go
// 错: Run 1 set, Run 2 拿 nil (新 sess 空 values map)
sess1.SetValue("user.theme", "dark")
// ...
sess2 := agent_v2.NewRunSession()        // 新 sess, values 空
_, ok := sess2.GetValue("user.theme")    // ok=false (业务 if !ok 走 default)
```

**`RunSession.values` 生命周期跟 sess 实例**——业务复用同一 sess 跨 Run 累积仍可见,
但典型 HITL Resume / 新对话场景都是新 sess 实例 → 看不到上 Run set 的值.
跟 eino `runSession.Values` 走 cp 序列化跨 Resume 自动恢复**不同**(crew-go 把 values
跟 cp 解耦, 跨 thread 数据走专用 `store.Store` 后端).

跨 Run 数据走 `store.Store`:

```go
// 对: 跨 Run 持久化用 store
if err := st.Put(ctx, store.NS("users", uid), "theme", "dark"); err != nil {
    return err
}
// 任意新 Run 都可拿到
item, err := st.Get(ctx, store.NS("users", uid), "theme")
if err != nil { return err }
if item == nil { /* miss — 业务走 default */ }
```

6 层 state 完整边界决策见 [`store.md` §1](store.md)（Channel state / RunSession.Events / RunSession.values / Checkpointer / HistoryStore / store.Store）.

### ❌ Go interface "typed nil" 陷阱 — 27 轮 audit 修 71 真 HIGH 的根源

```go
// 错：业务 conditional 传 model
var m model.ChatModel
if cfg.UseGPT { m = gpt4 }  // cfg.UseGPT=false 时 m 是 nil interface (Type=nil, Value=nil)
ag := agent_v2.NewBasicAgent(agent_v2.WithModel(m))  // ✓ panic-fast (m == nil)

// 错: typed nil pointer 漏防
var mp *MyModel  // typed nil pointer (Type=*MyModel, Value=nil)
ag := agent_v2.NewBasicAgent(agent_v2.WithModel(mp))  // ❌ 旧 silent 接受
                                                      // 后续 m.Generate() 远 stack panic
```

**框架防护** (R32+ audit²-²⁷ 累计修 71 真 HIGH, typed-nil 守卫跨 9 包 8 个名字，按名字 grep 找不全):

| 包 | 入口示例 (typed nil fail-fast——多数 panic，compose_v2 少数返 error / 归一化，见表下注) |
|---|---|
| `agent_v2` | `WithModel` / `WithStore` / `WithStoreMemory` / `WithHistory` / `WithHooks` / `WithSkills` / `WithRouter` / `WithEvaluator` / `WithReflector` / `WithLoopExitCondition` / `WithGraphInputWriter` / `WithGraphOutputReader` / `WithExecInputBuilder` / `WithAggregator` / `WithParallelMerger` / `WithSkillSelectionHook` / `WithSkillModelResolver` / `WithSkillSubAgentResolver` / `WithHistoryCompactor` / `WithSubAgents` / `WithLLMPlannerSubAgents` / `WithRoutingClassifier` / `WithRoutingFallback` / `WithReflectorAgent` / `WithAutoCompact (cfg.Model when Enabled)` / `AddStep` / `AddBranch` / `AddAgent` / `AddSubAgent` / `SetExecutor` / `WithActor` / `WithBody` / `AgentAsTool` / `RunnableAsTool` / `WithOrchHooks` / `WithCtxOrchHooks` / `WithResumeOrchHooks` |
| `compose_v2` | `AddNode` / `AddConditionalEdge` / `Custom` / `DynamicChannel` / `RegisterChannel` / `WithChannel` / `WithCheckpointer` / `WithCheckpointerCtx` / `WithOrchHooks` / `Get` / `Write` / `MustGet` / `Has` / `RunWithRetry` |
| `tool` | `Toolkit.Register` / `NewFuncTool` |
| `hook` | `NewChainHooks` (typed nil 元素静默过滤) |
| `mcp` | `WithClient` / `NewRemoteTool` |

> **注**：上表多数入口 typed nil 直接 panic，但失败形式因 API 语义而异（核心是入口处 fail-fast，不让 typed nil 流到远 stack）：
> - **agent_v2 非 panic 例外**：`WithHooks` → NoOp、`WithSkills` → silent skip、`WithOrchHooks` / `WithCtxOrchHooks` / `WithResumeOrchHooks` → silent return（观测类 option 缺省优雅退化）
> - **compose_v2 非 panic 例外**：`RegisterChannel` / `RunWithRetry` 返 **error**，`WithCheckpointer` / `WithCheckpointerCtx` 把 typed nil **归一化**为 untyped nil（"无 cp" idiom），`Has` 返 **false**

**业务推荐 idiom**:
```go
// ✓ 推荐: 用 untyped nil sentinel
var m model.ChatModel  // nil interface
if cfg.EnableLLM { m = realModel }
agent_v2.WithModel(m)  // m=nil 时 panic 明确指 "WithModel: typed nil ChatModel"

// ❌ 不推荐: typed nil pointer + 隐式转 interface
var mp *MyModel
if cfg.EnableLLM { mp = realImpl }
agent_v2.WithModel(mp)  // mp 是 typed nil → panic, msg 仍清晰但更隐蔽

// ✓ 业务防 typed nil 自查 (业务自定义 helper):
import "reflect"
func safe(v any) bool {
    if v == nil { return false }
    rv := reflect.ValueOf(v)
    switch rv.Kind() {
    case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
        return !rv.IsNil()
    }
    return true
}
```

**对比三方**:
- **eino** (Go 同语言): `compose.graph::addNode` **不检** node typed nil → 业务踩 typed nil 远 stack panic 难定位 (crew-go 比 eino 多一层 typed nil panic-fast)
- **langgraph** (Python): `add_node` 检 `if node is None: raise ValueError` — Python None 单一值无 typed nil 陷阱

## 9. 调试技巧

### 看 cp wire 内容

```go
cp, _ := cper.LoadNS(ctx, sessionID, "")
data, _ := compose_v2.MarshalCheckpoint(cp)
fmt.Printf("cp wire: %s\n", data)
```

### 加 OrchHooks 跟踪 graph 调度

```go
type traceHooks struct{ compose_v2.NoOpOrchestrationHooks }
func (traceHooks) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
    fmt.Println("node start:", id)
    return ctx, nil
}

compose_v2.Run(ctx, g, cm, compose_v2.WithHooks(&traceHooks{}))
```

### 端到端 binary 跑 channel_smoke 验证基础设施

```bash
go run ./example/orchestration/channel_smoke
# 5 场景全过证明 PoC 基础设施正确
```

## 10. v2 已知限制 / 设计取舍

| 限制 | 现状 | 业务 workaround |
|---|---|---|
| 类型注册手动 | 业务自定义类型存 cp 必须 `compose_v2.RegisterValueType[T]("...")` | init() 显式注册（详见 `doc/v2/channel-engine.md` §9）|
| `BasicAgent.ResumeActionReplan` + `ask_user` 工具组合 | PoC 已知 dead-loop（Phase 5.2 Bug #3） | **已解**（2026-08-06）：Replan 的用户指示改走独立的非寻址通道 `WithResumeUserText`，不再需要拿假 key（`_replan_`）挤进按 ID 寻址的答案 map，与 ask_user pending 不再冲突 |
| `PlanAgent.WithStepResultMapper` v1 API | v2 删除 | 用 `agent_v2.WithExecInputBuilder` 替代 |
| `RoutingAgent.DescriptorPrompt` v1 API | v2 未导出 | caller 自己组 system prompt |
| `WithRouteMerger` v1 fan-out 路由合并 | v2 RoutingAgent 单目标 | 多目标 fan-out 用 `ParallelAgent` |

**Stage 4-5 已完成的 v2 能力**（v1 时代有的现已全部支持）：
- ✅ skill 系统：`agent_v2.WithSkills(...)` + activate_skill 自动注入；P1（toolCall/adaptive/pre_match 三模式 + listing 预算 + body 跨 compaction 保鲜）+ P2.A（`disable-model-invocation` / `user-invocable` 二维 frontmatter 控制——详见 [`skill.md` §5.3](./skill.md)）+ P2.D（`WithInlineSkillBody(true)` 启用 inline body：tool_result terse + body 作为 `[system_reminder]` UserMessage 注入对话流——详见 [`skill.md` §6.2](./skill.md)）+ P2.C+B（`context: fork` frontmatter + `WithSkillSubAgentResolver` / `WithSkillModelResolver` 业务注入；activate_skill({skill, query}) LLM 必传 query——详见 [`skill.md` §6.3 / §8](./skill.md)）—— **skill 完整权威文档见 [`skill.md`](./skill.md)**
- ✅ 节点级 retry：`compose_v2.RetryConfig` + `RunWithRetry[T]` + 7 编排接 `WithXxxRetryConfig`
- ✅ 4 层压缩：`agent_v2.WithMicroCompact / WithAutoCompact / WithReactiveCompact / WithStore + WithStoreMemory` 框架自动按顺序触发
- ✅ OnModelEndStream hook：业务可 wrap reader 观测/转换/审计每个 chunk (`agent_v2/basic_agent.go::streamOrGenerate` 直接调 raw `hooks.OnModelEndStream`, panic 由 framework outer recover 兜底 — audit² 删 SafeOnModelEndStream wrapper 跟 eino 对齐)
- ✅ ctx-aware OrchHooks：`compose_v2.WithOrchHooks(ctx, h)` 嵌套子 agent 自动继承
- ✅ AgentAsTool / SkillTool fork 默认半共享 child sess (ISSUE-79 破坏性默认行为变化)：values 指针共享 + events 独立 + parentSess 反向指针；业务 `store.Search` 用 `sess.RootSession()` 上追看父对话历史
- ✅ ParallelAgent fan-out lane events 物理隔离 (ISSUE-80)：删 R31 distinct sub-agent name 临时缓解, branch 间 events 严格隔离
- ✅ BasicAgent toolsFn fanout 化 (ISSUE-81)：N=1 inline 零开销 / N>=2 goroutine 并发, 多 InterruptError 用 `MergeInterrupts` 聚合到 `InterruptError.Queries` (业务 Resume 用 `ie.AllQueries()` 拿全部 query)
- ✅ OnAgentEnd 单一所有权 (ISSUE-82)：删双触发 bug, Run/Resume defer 唯一源兜底所有退出路径 (success / Interrupt / panic / ctx.Err)；HITL Resume 闭环算 2 次 (Run 抛 + Resume 完成)
- ✅ PlanAgent hook 协议对称 (ISSUE-83)：`WithPlanHooks(h)` 与其他 8 编排架构对称, 9 编排全接业务级 hook.Hooks

## 10.4 多租户：哪几层按调用方身份分区

**先说结论：把 owner 接上去是业务的责任，但接一次就够——框架内部会沿 ctx 传下去。**

```go
// a2a：不配构造期 panic
server.Handler(ag, baseURL, server.WithOwnerResolver(func(ctx context.Context) string {
    if u, ok := auth.UserFrom(ctx); ok { return u.TenantID }
    return "" // 匿名——所有匿名请求共享一个分区
}))

// agui：同款
agui.Handler(factory, agui.WithOwnerResolver(resolveTenant), agui.WithCheckpointer(cp))
```

⚠️ **resolver 的入参是 `ctx` 不是请求体**，这是有意的：身份要从**已认证凭据**取
（认证中间件放进 `r.Context()` 的主体），不能从 header / body / metadata 这些客户端
能填的字段取——A2A 的 `contextId`、AG-UI 的 `threadId` 本身就是客户端给的，再从客户端
拿一个"身份"只是把同一个问题换了个字段名。

### 逐层现状

| 层 | 按 owner 分区？ | 谁负责 |
|---|---|---|
| A2A 挂起 task 表 / task store | ✅ 自动（键含 owner） | 配 resolver 即可 |
| AG-UI 挂起会话表 + LRU 配额 | ✅ 自动 | 同上 |
| HandoffAgent 持久转交 | ✅ 自动（ns 含 owner） | 同上 |
| 会话历史 HistoryStore | ✅ 自动（owner 从 ctx 流下来） | 同上；要按别的粒度（如"组织"）就显式给 `WithHistoryOwnerFunc` |
| Checkpointer `(sid, ns)` | ➖ 不需要 | sid 是**服务端铸的** RunID，不从 wire 接——见下 |
| `store.Store` 长期记忆 | ❌ **业务自己分** | ns 由业务给，框架不碰 |

### 两条容易踩的

**1. `agent_v2.Resume(ctx, sid)` 没有 owner 维，靠的是「sid 从不从 wire 接」这条不变量。**
框架的 a2a/agui 适配层都是先按 `(owner, wire-ID)` 找到会话、再用**服务端存的** RunID 去
Resume。业务自己写 HTTP 层时**别从请求里取 sid**——那等于把一个能力令牌交给客户端指定。

**2. `StoreMemoryConfig.WriterID` 不是租户维。** 它分的是「**哪个 agent 写的**」，
用来防多个成员撞键 + 支持「只回忆某个 agent 说过的话」。租户隔离要靠 ns：

```go
// ✅ 租户维在 ns 上
agent_v2.WithStoreMemory(store.NS("tenants", tenantID, "memories"), cfg)
// ❌ 这样两个租户的记忆仍在同一个 ns 里，WriterID 只是把 agent 分开了
agent_v2.WithStoreMemory(store.NS("memories"), &agent_v2.StoreMemoryConfig{WriterID: tenantID})
```

## 10.5 易混淆参数速查（SDK 使用者必读）

业务接入时**最常踩坑的参数**，集中放此节供查阅。各项详细背景见交叉链接。

### #2 ID 体系：`SessionID` / `RunID` / `GraphID`（+ 不在 cp 层的 `ThreadID`）

framework 持久化层有 3 个 cp 相关 ID 字段，但**真正独立的概念只有 2 个**：

| ID 字段 | 类型 | 含义 | 关系 |
|---|---|---|---|
| `RunSession.RunID` | string | 一次 Run 实例 ID | == `SessionID` == `cp.ID`（**Resume 路径** business 传 sess.RunID 与 sessionID 不一致时 `slog.WarnContext` 提醒 cp/事件可能错位—— 见 [`resume_helpers.go`](../../agent_v2/resume_helpers.go) `commonResumePrep`（9 编排 Resume 共用，含 `basic_agent.go::Resume` 调用）内 `"session.RunID 与 sessionID 不一致"` `slog.WarnContext`；Run 路径 sess.RunID 自动 == sessionID 无对比需求）|
| `SessionID`（API 参数）| string | `Resume(sessionID, ...)` 入参 / `cp.SaveNS(ctx, sessionID, ns, cp)` 入参 | == `RunSession.RunID` |
| `cp.ID` | string | cp 结构体字段 | `SaveNS` 自动填入 sessionID（[`checkpointer.go::SaveNS`](../../compose_v2/checkpointer.go) 内 `cp.ID = sessionID`）|
| `cp.GraphID` | string | 防 cp 串台 | `SnapshotForGraph` 自动写入 `cp.GraphID = g.ID()`（直读 `Graph.ID()`，框架自动，不走 ctx）；同 sid 不同 graph 抛 `ErrGraphIDConflict` fail-fast 在 **4 处**：`checkpointer.go::SaveNS` / `graph_loop.go::Run` 内 auto-load 路径 / `graph_loop.go::Run` 内 WithResumeFrom 路径 / `subgraph.go` 子图入口（注：`graph_loop.go` 没独立 Resume 函数，两处 fail-fast 都在 Run 内不同分支；grep `ErrGraphIDConflict` 看实际位置）|

**2 个真正独立概念**：

```
SessionID = RunID = cp.ID  ← 一次完整 Run 的主索引（business 感知就 1 个 ID）
        ↓
   cp.GraphID    ← 不同 graph 共享同 sid 时防 cp 串台 fail-fast
```

**跨 Run 会话身份是第三个概念，但不在 cp 层**：那是 `compose_v2.WithThreadIDCtx` 声明的
`ThreadID`，被 observability（会话聚类）、handoff（持久转交）、以及 `agent_v2` 的会话历史
层（`WithHistory`，按会话 append-only 留存消息）消费。它不进 cp——cp 是单次 Run 的临时态、
成功即清，扛不起「跨 Run 累积历史」。见下方场景 2。

**业务典型场景**：

```go
// 场景 1：单次 Run（默认）—— 业务只感知 RunID 一个 ID
sess := agent_v2.NewRunSession()                  // RunID 自动生成 UUID
events, _ := ag.Run(ctx, sess, input)             // sessionID = sess.RunID 自动用
// → cp 存 (sid=sess.RunID, ns=""), cp.ID=sess.RunID

// 场景 2：跨 Run conversation——用会话历史层，不是 cp
//   ctx = compose_v2.WithThreadIDCtx(ctx, "conv_abc")   // 声明会话身份
//   ag := agent_v2.NewBasicAgent(..., agent_v2.WithHistory(hs, "tenant-a"))
//   ag.Run(ctx, sess1, input1)   // 两次 Run 各用各的 sid（cp 各自成功即清）
//   ag.Run(ctx, sess2, input2)   // 但同一 ThreadID，历史层把两轮串成一条会话
// 会话历史层 append-only 留存逐字消息，第二轮 prior 从它读（含上一轮工具往返）。
// 详见「会话历史」一节 + ISSUE-98。

// 场景 3：防串台——框架自动用 Graph.ID()（name 或拓扑+state fingerprint）填 cp.GraphID，业务无需显式设
// 同 sid 第二次 Run 用不同 graph（g.ID() 不同）→ SaveNS / Run 入口 fail-fast errors.Is(err, ErrGraphIDConflict)
// 业务确需同 sid 跑不同 graph（如换版本）→ 用 WithFreshRun 忽略旧 cp
```

详见 [`channel-engine.md` §cp 寻址](./channel-engine.md) + 本文「会话历史」一节。

### #5 3 个限流参数：`MaxRounds` / `MaxHistoryPairs` / `SearchLimit`

三个参数都"控制 prompt 大小 / 防爆"，但**作用维度完全不同**——业务接入时易混：

| 参数 | 维度 | 默认值 | <=0 行为 | 作用对象 |
|---|---|---|---|---|
| `MaxRounds` | **ReAct 循环上限**（轮次）| 10 | `WithMaxRounds(0)` 兜底 1 | 单次 Run 内 model→tools 循环次数 |
| `MaxHistoryPairs` | **跨 Run 历史截断对数** | 20（默认开启，`defaultMaxHistoryPairs`）；配 HistoryCompactor 时 `MaxHistoryPairs<=0` 用 hint 20 | `WithMaxHistoryPairs(0)` 显式禁用（行为：`MaxHistoryPairs>0` 且 `len(prior)>MaxHistoryPairs*2` 时简单截断 prior 保留最后 N 对，**但保护带 `SkillBodyAnnotation` marker 的消息不被截**——grep `basic_agent.go` 内 `truncatePriorPreservingMarkers` 看实际逻辑）| 第 0 层 prior 截断（前 N 对 user/assistant）|
| `SearchLimit` | **StoreMemory.Search Item 数** | 5 | 兜底 5 | preamble 注入的 store 长期记忆 Item 数 |

**精确触发位置**：

```go
// MaxRounds——ReAct 循环 model 节点入口检查
agent_v2.WithMaxRounds(15)                 // n<=0 → 1（看作"至少跑 1 轮"兜底）

// MaxHistoryPairs——buildInitialMessages 跨 Run prior 截断
agent_v2.WithMaxHistoryPairs(10)           // 仅最近 10 对 user/assistant 进 prompt
agent_v2.WithHistoryCompactor(myFn)        // 优先级高于 MaxHistoryPairs（hint 20 fallback）

// SearchLimit——StoreMemory helper 单次 Search 上限
agent_v2.WithStoreMemory(ns,
    &agent_v2.StoreMemoryConfig{SearchLimit: 10})   // <=0 兜底 5
```

**易踩坑**：`SearchLimit` 是 **Item 数不是 message 数**——业务 Value 是富 JSON（含 metadata + content 多字段）时，单 Item 可能展开成多 token——必须实测 query 返 Item 总 token 数再校准。

**优先级**（业务从旧 Memory 迁来注意）：

```
HistoryCompactor 函数 > MaxHistoryPairs 简单截断 > 不动
                          ↑
                  仅当 HistoryCompactor 挂了才用作 hint（默认 20）
                  没挂时 MaxHistoryPairs>0 直接简单截断
```

### #7 `compose_v2.OrchestrationHooks` vs `hook.Hooks`（**两层 hook 别混**）

framework 有**两套 hook 接口**，业务 90% 用后者：

| 维度 | `compose_v2.OrchestrationHooks`（引擎层）| `hook.Hooks`（业务层）|
|---|---|---|
| 视角 | graph 执行 / 节点级 | agent 业务 / model + tool 级 |
| 方法数 | 5 | 8 |
| 方法清单 | `OnGraphStart/End` + `OnNodeStart/End` + `OnNodeEvent` | `OnAgentStart/End` + `OnModelStart/End` + `OnModelEndStream` + `OnToolStart/End` + `OnError` |
| 挂载 API | `compose_v2.WithOrchHooks(ctx, h)` ctx 协议 | `agent_v2.WithHooks(h)` Option |
| 业务感知 | 偶尔（debug graph 执行 / observability backend）| **常用**（业务 LLM 调用 / tool 调用前后做事）|
| ctx 透传 | ✅ 嵌套 sub-agent 自动继承（`WithOrchHooks(ctx, h)`）| ❌ **没有这个协议**——`hook.Hooks` 构造期绑定在各自 agent 上。想观测嵌套子 agent 的 model / tool 调用，得给每个子 agent 各自 `WithHooks(h)`；编排级的 `WithSequentialHooks` 等只 fire 它自己那层的 OnAgentStart/End/OnError |

**业务 99% 场景**用 `hook.Hooks` 即可——观测 LLM / tool 调用、改写 input / output、加 trace。**业务侧实现 pattern**：嵌入 `hook.NoOpHooks` 只覆写关心的方法（无须实现全 8 method）—— 完整示例见本文档 §6 业务级 hook 集成。

**`OrchestrationHooks` 典型场景**：

- 接 OpenTelemetry：监控每个 graph 节点的 latency
- 调试 channel-based 图执行：看每超步 frontier 是什么
- 自定义 observability backend：业务自管全链路 trace

详见 [`compose_v2/hooks.go`](../../compose_v2/hooks.go) + 本文档 §6 业务级 hook 集成。

### #10 业务 hook 触发时机 / 频次（**Resume 是否重触发**）

`hook.Hooks` 8 method 触发时机 **不一致**——业务写 hook 前必查此表：

| Hook | 触发时机 | Run 内频次 | HITL Resume 是否重触发 |
|---|---|---|---|
| `OnAgentStart` | Run/Resume 入口**直接同步调用**（preamble / modelNode 之前；不是 defer——defer 的是 OnAgentEnd）| Run 1 次 + 每次 Resume 1 次 | ✅ 每次 Resume 都触发（8-method 协议：整个调用 1 次，跟 OnAgentEnd 单一所有权对称；与 PlanAgent/GraphAgent/eino ADK 对齐）|
| `OnModelStart` | LLM 调用前 | **每轮 round** | ✅ 同 OnAgentStart |
| `OnModelEnd` | LLM 调用后（聚合后完整 message）| **每轮 round** | ✅ 同上 |
| `OnModelEndStream` | streaming reader 返回（业务可 wrap）| **仅流式路径每轮**（`llm.Stream` 成功返非 nil reader 才触发；模型不支持流式——Stream 返回 `model.ErrStreamUnsupported` 或 nil reader——时改调 `llm.Generate`，不触发它、只触发 `OnModelEnd`；hook 自己报错或返回 nil reader → 这次模型调用失败，`OnModelEnd` 不触发，`OnError`（`model_call_error`）触发 1 次）| ✅ 同上 |
| `OnToolStart` | per tool_call | **每个 tool_call 串行**（在 agent_v2/basic_builder.go for 循环内）| ✅ Resume 重跑 tool 节点时 |
| `OnToolEnd` | per tool_call 返回后 | 同 OnToolStart 串行 | ✅ 同上 |
| `OnError` | 任一阶段错误 | 错误时 1 次 | ✅ |
| `OnAgentEnd` | Run/Resume 入口 defer | Run 1 次 + 每次 Resume 1 次（ISSUE-82 单一所有权）| ✅ **HITL 闭环算 2 次**（Run 抛 Interrupt + Resume 完成）|

**关键不变量**：

1. **OnAgentStart / OnAgentEnd 次数对称**：Run 各 1 次 + 每次 Resume 各 1 次（**OnAgentStart 同步先于图执行、OnAgentEnd 是 defer**——见上表）。业务**OTel ctx-push pattern**（OnAgentStart push trace span，OnAgentEnd pop）会 Run / Resume 两段各 work。
2. **3 种 hook 返回签名分类**（8 method 不统一）：
   - **5 method 返 `(ctx, error)`**：`OnAgentStart` / `OnModelStart` / `OnModelEnd` / `OnToolStart` / `OnToolEnd`——返 error 时 framework abort
   - **1 method 返 `(ctx, reader, error)` 三元组**：`OnModelEndStream`——允许 hook wrap / 拦截 stream reader
   - **2 method 只返 `ctx`**：`OnAgentEnd` / `OnError`——**不能阻止 Run 完成**（已是终态）
3. **tool_call fanout 协议**（ISSUE-81）：N≥2 时**业务 tool 函数本身 goroutine 并发执行**（`agent_v2/basic_agent.go::makeToolsFn` 内 WaitGroup fanout），但 **`OnToolStart` / `OnToolEnd` hook 调用本身保持串行**——`agent_v2/basic_builder.go` 的 tools 节点在调 toolsFn 前 for 循环串行触发所有 `OnToolStart`，toolsFn 返回后 for 循环串行触发所有 `OnToolEnd`（compose_v2 引擎不认识 `hook.Hooks`）。业务 hook 内共享状态写入**无需 thread-safe**（hook 本身串行）；业务自定义 tool 函数内共享状态**需 thread-safe**（tool fanout 并发）。
4. **业务想"每轮观测"用 `OnModelEnd`**——`OnAgentEnd` 只 Run 完成时调，不每轮。

完整签名见 [`hook/hooks.go::Hooks`](../../hook/hooks.go)；ReAct 内部 4 节点（preamble + model + tools + postamble）跟 hook 触发的对应关系见 CLAUDE.md "Hook 选择速查"段。

---

## 11. 学习路径

1. **30 秒速览**（§0）→ 知道整体 shape
2. **跑 example**：`go run ./example/orchestration/channel_smoke` → 看 5 个真实场景；`go run ./example/orchestration/nested_smoke` → 12 场景 / 7 编排的 HITL Resume 嵌套
3. **本文档 §1-§5**：核心约定 + API + HITL + 子图
4. **设计文档三件套**：
   - [`channel-engine.md`](./channel-engine.md) — 引擎层完整设计（核心抽象 / cp / Resume / fan-out / 嵌套 / 9 编排编译产物 / 43 项不变量）
   - [`hitl-resume.md`](./hitl-resume.md) — HITL + Resume 完整协议（InterruptID：8 个维度（7 个位置维 + 任务代号），外加被否决重做之后才计入的重做代际与外层代际链 / 多 tool_call 部分中断 / fan-out 并发中断 / plan_chat_web 实战）
   - [`context-compaction.md`](./context-compaction.md) — 4 层上下文压缩完整设计

## 12. 参考

- 端到端示例：`example/orchestration/channel_smoke/main.go`、`example/orchestration/nested_smoke/main.go`、`example/orchestration/plan_chat_web/`（HITL Web 服务）
- 单元测试（API 用法实例）：`compose_v2/*_test.go`、`agent_v2/*_test.go`
- 决策原理：[`why-channel-based.md`](./why-channel-based.md)（当初为什么转 channel-based）+ [`channel-based-design.md`](./channel-based-design.md)（PoC 期 API 设计草案）
- PoC 报告：[`poc-report.md`](./poc-report.md)
- 三方对照：[`../description/three-way-comparison/`](../description/three-way-comparison/)
