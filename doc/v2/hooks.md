# hooks.md — Hook 系统设计参考（跨语言 SDK 实现指南）


> 类型签名用 Go 表示，但表达的是**语言无关的设计契约**——其他语言照此设计自己的等价签名即可。

## 1. 两套 hook，两个抽象层

crew-go 把 hook 拆成**两套独立接口**，锚定**不同抽象层**——这是核心设计决策，建议其他 SDK 沿用：

- **业务级 `Hooks`（8 方法）**——锚定 Agent 语义层（agent / model / tool），回答"我的 Agent 跑到哪一步了"。典型消费者：观测 / 审计 / billing / 内容改写。
- **编排级 `OrchestrationHooks`（5 方法）**——锚定图引擎层（graph / node / event），回答"执行引擎在调度哪个节点"。典型消费者：streaming chunk 投递 / 节点级 trace / 引擎调试。

**为什么分两套**：业务方关心 ReAct 语义（调了哪个模型 / 工具、第几轮），不关心图拓扑；引擎可观测性 / streaming 关心节点调度，不关心"轮"的概念。两层正交——业务可只接一套，也可都接，框架内部把两套都 fire。

### 三层语义动作

业务级 hook 按**三个语义动作**分层，每层 start/end 严格配对——这套模型直接对应 agentscope 的 `reply` / `reasoning` / `acting`：

| 语义层 | start / end | 频次 | = agentscope |
|---|---|---|---|
| **agent**（一次完整调用）| `OnAgentStart` / `OnAgentEnd` | Run 同步 **1 次** + 每次 Resume 同步 **1 次** (HITL 闭环算 2 次, 详 line 75 脚注) | `pre_reply` / `post_reply` |
| **model**（一轮 LLM 交互）| `OnModelStart` / `OnModelEnd` | 每轮 | `pre_reasoning` / `post_reasoning` |
| **tool**（一次工具调用）| `OnToolStart` / `OnToolEnd` | 每个 tool_call | `pre_acting` / `post_acting` |

外加 `OnModelEndStream`（流式 reader 包装，crew-go 特有的流式钩子——agentscope 输出侧用 `pre/post_print` 逐条消息回调，覆盖面更宽（reasoning 流式 + tool 结果 + summary）、机制也不同，不强行对齐）和 `OnError`（错误观测）。


### 锚定关系（关键）

一个 BasicAgent（ReAct）被编译成 channel-based 图。拓扑如下（`agent_v2.BasicAgent` **始终注入** preamble + postamble，常驻四节点——ISSUE-74/75；底层 `agent_v2.BasicAgentBuilder` 的 `WithPreamble`/`WithPostamble` 才是可选，business 直接用底层 builder 时可不传）：

```
StartNode → [preamble] → model ⇄ tools
                         model ──(无 tool_call)──→ [postamble] → EndNode
```

- **`OnAgentStart` / `OnAgentEnd` 不在任何节点内**——它们在 agent 调用的最外层 wrapper（图执行的外面），对一次 Run 各触发一次（`OnAgentStart` 同步先于图执行、`OnAgentEnd` 图执行后 defer），框住"整个 agent 调用"。
- **`OnModelStart` / `OnModelEnd` 锚在 `model` 节点（每轮）**；`OnToolStart` / `OnToolEnd` 锚在 `tools` 节点（每个 tool_call）。
- **`preamble` / `postamble` 节点只触发编排级 `OnNodeStart` / `OnNodeEnd`，不触发任何业务级 hook**——它们跑的是框架内务（注入历史 / 记忆检索 / 收尾持久化），不属于"agent 语义轮"。

一轮 ReAct 的完整触发时序（业务级 + 编排级穿插）：

```
一次 Agent.Run / Resume
│
├─ OnAgentStart                     业务级 · 同步 · 每次 Run/Resume 1 次（图执行之前）
│
├─ OnGraphStart                     编排级 · 每图 1 次
│   │
│   ├─[preamble 节点]  OnNodeStart → (框架内务) → OnNodeEnd       仅编排级
│   │
│   ├─[model 节点]     OnNodeStart                                编排级
│   │     ├─ OnModelStart                                         业务级 · 每轮
│   │     │    └─（流式路径）OnModelEndStream                     业务级 · 每轮（介于 Start/End 之间）
│   │     └─ OnModelEnd                                           业务级 · 每轮
│   │                  OnNodeEnd                                  编排级
│   │
│   ├─[tools 节点]     OnNodeStart                                编排级（有 tool_call 时）
│   │     └─ OnToolStart / OnToolEnd × N                          业务级 · 每个 tool_call · 串行
│   │                  OnNodeEnd                                  编排级
│   │
│   │   …… 循环回 model 节点直到无 tool_call ……
│   │
│   └─[postamble 节点] OnNodeStart → (框架内务) → OnNodeEnd       仅编排级
│
├─ OnGraphEnd                       编排级 · 每图 1 次（成功 / 失败都调）
│
└─ OnAgentEnd（defer）              业务级 · 每次 Run/Resume 1 次（图执行之后）

任意节点内：节点主动 EventSink.Send(event) → OnNodeEvent   编排级 · 实时投递（典型 streaming chunk）
```

`OnAgentStart`（整个调用开始 1 次）和 `OnModelStart`（每轮）是**不同层级**——前者框住整个 agent 调用、后者标记每一轮 LLM 交互，不会重叠。

> **HITL 场景**：业务工具中断后 `Resume` 会再走一遍这个时序，所以 `OnAgentStart` / `OnAgentEnd` 在"Run 抛中断 + Resume 完成"的闭环里各算 2 次（每个入口 1 次）。

## 2. 业务级 hook.Hooks（8 方法）

```go
type Hooks interface {
    OnAgentStart(ctx, *AgentInfo, *AgentInput)      (context.Context, error)
    OnAgentEnd(ctx, *AgentInfo, *AgentOutput)        context.Context
    OnModelStart(ctx, *AgentInfo, *ModelInput)      (context.Context, error)
    OnModelEnd(ctx, *AgentInfo, *ModelOutput)       (context.Context, error)
    OnModelEndStream(ctx, *AgentInfo, MessageStreamReader) (context.Context, MessageStreamReader, error)
    OnToolStart(ctx, *AgentInfo, *ToolInput)        (context.Context, error)
    OnToolEnd(ctx, *AgentInfo, *ToolOutput)         (context.Context, error)
    OnError(ctx, *AgentInfo, *CommError)             context.Context
}
```

每个方法第一个入参恒为 `ctx`、第二个恒为 `*AgentInfo`（agent 标识：`Name` / `Type` 编排类型如 `"base"` / `Round` 当前轮次 / `RunPath` 从根的嵌套执行路径，后者为 Go SDK 扩展字段）。

> 下表"触发锚点"列以 **BasicAgent（ReAct）** 为范例。其他编排（Sequential / Parallel / Plan 等）的业务级 hook 触发于各自拓扑对应的位置，但 `OnAgentStart` / `OnAgentEnd` 的最外层语义、以及所有 hook 的参数与消费契约保持不变。

| Hook | 触发锚点 / 频次 | 业务入参（关键字段）| 返回 & 返 error 后果 | 主要作用（设计意图）|
|---|---|---|---|---|
| **OnAgentStart** | agent 最外层、图执行**前**（同步）· 每 Run/Resume 1 次 | `AgentInput.Messages` = 本次调用的**原始输入**（Resume 时为空）| `(ctx, error)`；返 error → 中断整个 Run | 一次 agent 调用的开始锚：拿未加工输入 / 建顶层 trace span / 入口级改写 |
| **OnModelStart** | model 节点内、**压缩之前** · 每轮 | **两个并列字段**：`ModelInput.Preamble` = agent 前言（`WithPromptTemplate` 渲染结果 + `WithSystemPrompt` 含 skill hint/body，不进压缩）；`ModelInput.Messages` = 被压缩的对话流（记忆注入 + 历史 + 本轮输入）。⚠️ **它不是发出去的那一份**：四层压缩在这个 hook **之后**跑，AGENTS.md 注入也在之后 prepend。要「模型最终看到什么」请在 model client 那一侧看 | `(ctx, error)`；返 error → 中断本轮 | 观测 / 改写每一轮送 LLM 的输入——**两个字段都是[消费]档**：改 `Preamble` 换 agent 的 persona、改 `Messages` 动对话流，互不误伤（整段替换 `Preamble` 也生效，旧的不会残留）|
| **OnModelEndStream** | model 内部流式路径，介于 ModelStart / End · 每轮（仅模型流式可用时）| 上游 LLM 的 `MessageStreamReader` | `(ctx, reader, error)`；返 error → **降级非流式，不中断** | 包装流 reader 做逐 chunk 观测 / 转换 / 审计 |
| **OnModelEnd** | model 节点内、LLM 返回后 & tool 解析**前** · 每轮 | `ModelOutput.Message` = 聚合后完整响应 | `(ctx, error)`；返 error → 中断本轮 | 观测 / 改写模型输出（`len(toolCalls)==0` 即最终答复）|
| **OnToolStart** | tools 节点内、每个 tool_call **前** · per tool_call · 串行 | `ToolInput.Name` / `ArgumentsJSON`（只在这一批第一次交给工具执行时生效，续跑沿用定下的内容、随中断档落盘；每次都要新取的值走钩子返回的 ctx 或工具中间件）| `(ctx, error)`；返 error → 中断本轮 | 改写工具名 / 参数（参数注入 / 工具重定向）|
| **OnToolEnd** | tools 节点内、每个 tool_call **后** · per tool_call · 串行 | `ToolOutput.Result`（`Output` / `Success` / `Error` / `ErrorCode`）| `(ctx, error)`；返 error → 中断本轮 | 改写工具结果（脱敏 / 注入缓存）写回 LLM 历史 |
| **OnAgentEnd** | agent 最外层、图执行**后**（defer）· 每 Run/Resume 1 次 | `AgentOutput.Messages` = 本 Run 累积消息快照 | `ctx`（**无 error**）；不中断 | 一次调用的收尾：trace span 结束 / 在途计数 -- / 审计 |
| **OnError** | 任一阶段出错时（中断信号 & ctx 取消除外）| `CommError.Code` / `Message` | `ctx`（**无 error**）；不中断 | 错误观测 / 统计 |


## 3. 编排级 OrchestrationHooks（5 方法）

```go
type OrchestrationHooks interface {
    OnGraphStart(ctx, Channels)                     (context.Context, error)
    OnGraphEnd(ctx, Channels, err error)             context.Context
    OnNodeStart(ctx, NodeID, Channels)              (context.Context, error)
    OnNodeEnd(ctx, NodeID, Channels, nodeErr error)  context.Context
    OnNodeEvent(ctx, NodeID, Event)                  context.Context
}
```

`Channels` 是节点状态的**只读视图**；`Event{Type, Content, Metadata}` 是节点主动发的观测事件。

| Hook | 触发锚点 / 频次 | 入参 | 返回 & 返 error 后果 | 主要作用 |
|---|---|---|---|---|
| **OnGraphStart** | 图开始执行**前** · 每图 1 次 | `Channels`（初始状态）| `(ctx, error)`；返 error → **终止图**（OnGraphEnd 仍调）| 建图级 span / 初始化 |
| **OnGraphEnd** | 图执行**后** · 每图 1 次（成功 / 失败都调）| `Channels` + 最终 `err` | `ctx`；不拦截（监听语义）| 图级收尾 / 拿最终 channel 状态 |
| **OnNodeStart** | 每个节点 NodeFunc **前** · 每节点每次访问 | `NodeID` + `Channels` | `(ctx, error)`；返 error → **跳过该节点**（OnNodeEnd 仍配对）| 节点级 span / 调试 |
| **OnNodeEnd** | 每个节点 NodeFunc **后** · 每节点每次访问 | `NodeID` + `Channels` + `nodeErr`（NodeFunc 返回值：业务错 / 中断 / nil）| `ctx`；不拦截（监听语义）| 节点 span 结束 / 拿节点结果 |
| **OnNodeEvent** | 节点内主动 `EventSink.Send` 时 · 任意次 | `NodeID` + `Event` | `ctx`；不拦截（实时投递）| streaming chunk / 进度上报实时投递给前端流 |

### 并发约定（与业务级的关键区别）

- **`OnNodeStart` / `OnNodeEnd` / `OnNodeEvent` 在 fan-out（并行分支 / 同超步多节点）时从多个 goroutine 并发调用**——编排级 hook 实现**必须线程安全**（典型：mutex 保护写入）。
- **业务级 hook 调用始终串行**——即使 `tools` 节点内多个工具并发执行，`OnToolStart` / `OnToolEnd` 的调用本身仍由框架串行 fire（按 tool_call 顺序）。业务级 hook 内的共享状态写入**无需**加锁。

