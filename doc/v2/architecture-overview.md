# crew-go 架构总览

> **定位**：跨 session 快速建立全局心智模型。本文是**导航式总览**——只讲"有哪些层、怎么
> 串起来、数据该放哪"，不堆算法/API 细节（那些在各专题文档，见文末导航表）。
>
> 权威源：依赖方向 + 包定位是 **2026-06 实测 grep**；跨层归属表来自 [`store.md` §1](./store.md)；
> 编排/数据流细节引用 [`channel-engine.md`](./channel-engine.md) + [`orchestration-patterns.md`](./orchestration-patterns.md)。

## 1. 一句话定位

Go 原生 LLM Agent 编排框架。参考 LangGraph Pregel + eino adk + agentdock-spec 设计，**不做跨语言
codegen**，面向 Go 生态保持简洁。所有编排（BasicAgent ReAct + 8 编排）**编译为同一个 channel-based
BSP 图执行引擎**——不为每种编排手写循环。

## 2. 包结构与依赖方向

**实测依赖方向**（2026-06，grep 各包实际 import 的 `crew-go/` 子包）：

```
modules                      ← agentdock-spec 拷贝来的 spec 生成类型（modules/ 目录）
  ↑
schema                       ← modules 的 type alias（不另起炉灶）
  ↑
model   tool   hook   skill   mcp
  │       │      │      │      └─ mcp → tool（RemoteTool 实现 tool.Tool）
  │       │      │      └─ skill：零内部依赖（self-contained）
  │       │      └─ hook → schema + modules
  │       └─ tool → schema
  └─ model → schema
  ↑
compose_v2                   ← 只依赖 hook + schema（纯图引擎，不认识"模型/工具/技能"）
  ↑
agent_v2                     ← 依赖以上全部（compose_v2 + hook + model + skill + store + tool + schema + modules）
```

> **与 CLAUDE.md 简化图的差异**：CLAUDE.md "包依赖方向" 写 `schema ← model/tool/hook/skill/mcp ←
> compose_v2 ← agent_v2`，是**理想化简写**。实测 `compose_v2` 只 import `hook` + `schema`——
> `model`/`tool`/`skill`/`mcp` 是 **agent_v2 层**才引入的。这是有意的：compose_v2 是通用 BSP 图引擎，
> 不绑定"LLM/工具/技能"概念，只认 `NodeFunc` + `Channel` + `hook.Hooks`（业务级回调）。

**包定位表**：

| 包 | 定位 | godoc / 用途 |
|---|---|---|
| `modules` | spec 生成类型 + 我们的 Go 原生优化（不直接依赖外部模块）| 从 agentdock-spec/go-runtime 拷贝 |
| `schema` | `modules` 的 type alias（`Message` / `StreamReader[T]` / `MessageRole`）| 无独立 Package godoc |
| `model` | `ChatModel` 接口（`Generate` + `Stream` + `WithTools`）+ openai/qwen 实现 | 无独立 godoc |
| `tool` | `Tool` / `Toolkit` / `FuncTool` / `StreamableTool` 工具接口与注册表 | 无独立 godoc |
| `hook` | 8 个生命周期回调（`Hooks` 接口）+ `NoOpHooks` / `ChainHooks` | 无独立 godoc |
| `skill` | "定义 skill 核心类型与接口"——SKILL.md 容器 + `activate_skill` | 有 Package godoc |
| `mcp` | "提供 Model Context Protocol 客户端集成"——StdioClient/StreamableHTTP + Hub | 有 Package godoc |
| `compose_v2` | "channel-based 重设计 PoC"——BSP 图引擎核心 | 有 Package godoc |
| `agent_v2` | "基于 compose_v2 channel-based graph 引擎的高层 agent"——9 编排封装 | 有 Package godoc |
| `store` | "跨 thread / 跨用户的长期 KV 持久化抽象"（第三个持久化层）| 有 Package godoc |

## 3. 核心架构：所有编排 → 一个图引擎

9 个编排都实现 `agent_v2.Agent` **单接口**（`Run(ctx, sess, input, opts...)` + `Resume(ctx, sid, opts...)`
+ `Name()` + `Description()`），都被框架**编译为 `compose_v2` channel-based Graph**。可无限嵌套，自动支持
HITL Resume。

| 编排 | 核心逻辑 |
|---|---|
| `BasicAgent` | ReAct 循环——内部 **preamble + model + tools + postamble 四节点** channel-based Graph + 循环回边 |
| `SequentialAgent` | 线性 step 链 |
| `ParallelAgent` | 并发 fan-out + Merger/Aggregator 聚合 |
| `LoopAgent` | 循环 body + ExitCondition + maxIterations |
| `RoutingAgent` | Router 函数，或「决策方 → LLM Classifier → 兜底」升级链 |
| `HandoffAgent` | 多 agent 间转交（transfer 工具或成员经 `HandoffTo` 做出的转交决定，交给 HandoffAgent 的上行信号 + maxHops）|
| `PlanAgent` | replan-每步：`plan ↔ exec` 循环 |
| `ReflectAgent` | actor + Reflector/Evaluator/EvalFunc 三档评估 |
| `GraphAgent` | 包任意 `compose_v2.Graph` 为 Agent |

> 编排细节 + 信号优先级 + HITL 矩阵见 [`orchestration-patterns.md`](./orchestration-patterns.md)；
> 9 编排到 Graph 的编译产物（内部 channel 名）见 [`channel-engine.md` §12](./channel-engine.md)。

**为什么统一编译为图**：循环/分支/嵌套/中断恢复只需在图引擎实现一次，所有编排复用。设计动因见
[`why-channel-based.md`](./why-channel-based.md)。

## 4. 数据流全景（单次 Run + Resume）

```
agent.Run(ctx, sess, input)
  │  ① WithSession(ctx, sess) + WithCheckpointerCtx(ctx, cp)（业务传 cp）
  │  ② OnAgentStart（业务级 hook，Run top-level fire 1 次）
  ▼
compose_v2.Run(ctx, graph, cm)        ← BSP 超步循环
  │  每超步：取 frontier 节点 → 并发跑 NodeFunc → channel barrier 收敛 reducer
  │  NodeFunc 内 emit Event 给 OrchHooks.OnNodeEvent（前端 SSE 实时）
  ▼
  ├─ 节点抛 InterruptError（HITL）→ SaveNS(sid, ns) 写 Pending cp（含 frontier + channels + pending interrupts）
  │                                 → InterruptError 冒泡回 caller
  └─ 跑完 → cleanupOnSuccess：DeleteSubtree(sid, "") 清根 cp 子树（成功不留存档）
  ▼
  ③ OnAgentEnd（defer，所有退出路径：success/Interrupt/panic/ctx.Err）

──── 用户答完 ask_user ────

agent.Resume(ctx, sid, WithResumeUserInputs(map[InterruptID]answer))
  │  LoadNS(sid, ns) 恢复 cm（channel state + frontier）+ 注入 ResumeContext
  ▼
compose_v2.Run（WithResumeFrom）从 cp.frontier 起跑——不重跑已完成节点
```

**关键不变量**（详见 [`channel-engine.md`](./channel-engine.md) + [`hitl-resume.md`](./hitl-resume.md)）：
- **嵌套 cp 物理分离**：子 agent 通过 ctx push `graphNS`，子 cp 写到 `(parentSID, parentNS/childName)`，多层嵌套不互覆
- **fan-out 超步统一**：所有 fan-out 编译为图原生超步 frontier，Resume 默认只重跑失败/中断分支
- **GraphID 防串台**：两个不同 Graph 共享同 sid 时 `SaveNS`/Run 入口 fail-fast `ErrGraphIDConflict`
- **chunk 不写 session.Events**：streaming chunk 经 OrchHooks 实时投递前端，不写审计；最终 assistant 走 OutputEvent/ExitEvent 写审计

## 5. 跨层 state 归属（4 层 state 抽象，决定数据放哪）

放数据前问 3 个判断问题（命中即定，都不命中 → 第 4 项 Channel 兜底）（来自 [`store.md` §1](./store.md)）：
1. 要跨 sessionID（换 thread/用户）看到？→ **Store**
2. 要跨 Run 但同 sessionID 看到？→ **Checkpointer**（框架自动）
3. sub-agent 间共享、生命周期跟 sess 实例（不走 cp）？→ **RunSession.values**
4. 其余 = **Channel state**（框架管的当前对话上下文）

| 抽象 | 作用域 | 谁管理 | 持久化 |
|---|---|---|---|
| **Channel state**（`compose_v2.Channels`）| 单 Run 内 | 框架自动 | **走 cp.Channels**，跨 Run 同 sid 自动 |
| **`RunSession.Events`** | 单 Run 内（业务持引用可跨 Run 复用累积）| 业务/框架混管（AddEvent/RestoreEvents）| **不走 cp**——业务自管 `RestoreEvents` 跨 Run（`Checkpoint` 无 events 字段）|
| **`RunSession.values`**（`SetValue`/`GetValue`）| 单 sess 实例（half-shared child sess 跨 sub-agent 共享）| 业务显式 SetValue/GetValue | **不走 cp**——生命周期跟 sess 实例，新 sess = 新空 map |
| **`compose_v2.Checkpointer`** | 单 sessionID（可跨 Run 接同 thread）| 框架自动 | 业务接 InMemory/Redis/PG |
| **`store.Store`** | 跨 thread / 跨用户 | 业务显式 Get/Put/Search | 业务接 InMemory/Redis/PG/mem0 |

> 4 层边界 + 混用反模式 + eino 对照见 [`store.md` §1](./store.md)。

## 6. v1 → v2 关键差异

v2 是 channel-based 重设计（v1 `agent/` + `compose/` 已删，v2 = 唯一实现）：

| 维度 | v1（GraphState-based）| v2（channel-based）|
|---|---|---|
| 状态载体 | `GraphState` typed struct | `Channel[T]` + 4 种 reducer（LastValue/Append/Sum/OnceFlag）|
| 编排接口 | `NestableAgent` 双接口（Run + RunNested）| `Agent` 单接口（对齐 LangGraph 单 Pregel / eino 单 Agent）|
| 循环/分支 | 各编排手写 | 统一编译为图原生超步 |
| cp 寻址 | 单 sessionID 键 | `(sessionID, ns)` 二级 key 物理分离 |
| ReAct | 单体 `executeRound` 函数 | preamble + model + tools + postamble 四节点图 |

> 完整演化历史 + commit 索引见 [`migration-history.md`](./migration-history.md)；**为什么转 channel-based**
> + 实际收益验证见 [`why-channel-based.md`](./why-channel-based.md)。

## 7. 文档导航

| 想了解 | 看 |
|---|---|
| v2 公开 API 速查 | [`migration-history.md`](./migration-history.md) |
| 业务侧用法 + 4 ID 体系 + 5 限流参数 | [`usage-guide.md`](./usage-guide.md) |
| 图引擎内部（channel/reducer/cp/编译产物）| [`channel-engine.md`](./channel-engine.md) |
| 9 编排模式 + 信号优先级 + HITL 矩阵 | [`orchestration-patterns.md`](./orchestration-patterns.md) |
| HITL 中断 / Resume / InterruptID 寻址 | [`hitl-resume.md`](./hitl-resume.md) |
| 4 层上下文压缩 | [`context-compaction.md`](./context-compaction.md) |
| Store 跨 thread 长期记忆 + 4 层 state 边界 | [`store.md`](./store.md) |
| 设计决策溯源 | [`why-channel-based.md`](./why-channel-based.md) / [`poc-report.md`](./poc-report.md) |
| 架构总入口 + 详细导航 | [`../description/architecture.md`](../description/architecture.md) |
