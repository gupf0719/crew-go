# crew-go 架构总览

> **跨 session 入口**——新人 / 跨 session 上下文建立从这里开始。
> 不是详细 spec，是"问题 → 该读哪份"的导航地图。

## 项目画像

crew-go 是一个 **Go 原生 LLM Agent 编排框架**——用最少抽象把"单个 ReAct Agent"和"多 Agent 协作"统一在 channel-based BSP 图执行引擎里。参考 LangGraph Pregel + eino adk 设计（早期另参考公司 spec，现已不再依赖），不做跨语言 codegen，面向 Go 生态保持简洁。

**核心设计选择**：
- 所有编排（BasicAgent ReAct + Sequential / Parallel / Loop / Plan / Reflect / Routing / Handoff / GraphAgent）**编译为同一个 channel-based Graph** 执行
- Checkpointing / OrchestrationHooks / InterruptError / 4 层上下文压缩等横切能力在 `compose_v2.Run` 实现一次，全模式自动受益
- 包依赖单向：`schema ← model/tool/hook/skill/mcp/prompt ← compose_v2 ← agent_v2 ← a2a / agui / decision`（A2A 接入层、AG-UI 前端适配、决策模型接入，见下图）；`store` 是 agent_v2 消费的叶子（compose_v2 除 internal/panicerr 外只 import schema，不依赖 store）；`observability` 横切层消费 agent_v2 / compose_v2 的 hooks 接口（见可观测文档）；`internal/panicerr` 是只依赖标准库的叶子——兜住 panic 时取栈、把栈作为 `compose_v2.PanicError`（它的类型别名）挂到返回的错误上、记带栈的日志，被 compose_v2 / agent_v2 / a2a / agui / mcp / model/openai / observability / schedule 引

## 包依赖图

```
┌─────────────────────────────────────────────────────────────────┐
│  a2a/         A2A 协议接入（官方 a2a-go SDK）：RemoteAgent 消费  │
│               远程（agent/tool 两形态）+ server/ 暴露本地 agent  │
│               （HITL+流式）+ registry/ 注册发现（§3 模型+Selector│
│               +InMemory+conformance）+ zk/ ZooKeeper Binding v1  │
│               （独立 go module，隔离 go-zookeeper）              │
│  agui/        AG-UI 前端协议适配：agent 事件流转成 AG-UI         │
│               标准事件经 SSE 推前端（含 HITL Resume 闭环）       │
│  decision/    决策模型客户端（System One 协议）+ 路由决策方；    │
│               子包 jev/ 接 TypeSafe 的 Jev，                     │
│               bailian/ 接阿里云百炼的 decision-model-preview     │
└──────┬──────────────────────────────────────────────────────────┘
       │（registry 纯 stdlib 叶子，被 a2a / a2a/server / a2a/zk 引）
       ▼
┌─────────────────────────────────────────────────────────────────┐
│  agent_v2/    9 种 Agent 实现（BasicAgent + 8 编排：Sequential/ │
│               Parallel/Loop/Plan/Reflect/Routing/Handoff/        │
│               GraphAgent）+ 4 层压缩 + Store (跨 thread KV) +     │
│               Skill 集成 (Memory 接口 PR 3.3 已删, store 替代)    │
└──────┬──────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│  compose_v2/  Channel-based BSP 图引擎 + 4 reducer + 5 维       │
│               InterruptID + Checkpointer (NS 物理分离 + thread)  │
│               + Resume + OrchestrationHooks + Retry             │
└──────┬──────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│  model/       ChatModel 接口（Generate / Stream / WithTools）   │
│  tool/        Tool / Toolkit / FuncTool / StreamableTool        │
│  hook/        8 个生命周期回调（Hooks 接口 + NoOp + Chain）     │
│  skill/       SkillBox 关键词匹配 + 激活                        │
│  mcp/         MCP Server 集成（Stdio + HTTP + Hub 聚合）        │
└──────┬──────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│  schema/      Message (alias of modules/) / StreamReader        │
│  modules/     common / model（spec 生成的跨语言一致类型）       │
└─────────────────────────────────────────────────────────────────┘
（internal/panicerr 只依赖标准库：兜住 panic 时取栈、挂 PanicError、记日志，
  被 compose_v2 / agent_v2 / a2a / agui / mcp / model/openai / observability / schedule 引）
```

## 何时读哪份文档

按"我想解决什么问题"导航：

| 我想... | 读哪份 |
|---|---|
| **第一次了解项目，5 分钟建立全景** | 本文档 + [v2/migration-history.md §包依赖 §核心类型速查](../v2/migration-history.md) |
| 看完整 v2 公开 API 类型表 | [v2/migration-history.md §核心类型速查](../v2/migration-history.md#v2-核心类型速查增量于-claudemd-主表) (注: anchor 跟 markdown header `## v2 核心类型速查（增量于 CLAUDE.md 主表）` 全文一致) |
| 看 v2 设计原理（channel + reducer + BSP）| [v2/channel-based-design.md](../v2/channel-based-design.md)（PoC 期设计稿）|
| **引擎层完整设计**（BSP / Channel / fan-out / 嵌套 NS / 9 编排编译产物 / 不变量速查）| **[v2/channel-engine.md](../v2/channel-engine.md)**（~1420 行核心文档）|
| **cp 与断点续跑**（全字段 / 三档 Status / 生命周期 / 寻址 / 节点私有中断态 / 并发保护 / Resume 协议）| **[v2/checkpoint.md](../v2/checkpoint.md)** |
| **HITL + Resume 完整协议**（InterruptID 5 维 / 多 tool_call 部分中断 / fan-out 并发中断 / mid-stream / plan_chat_web 实战）| **[v2/hitl-resume.md](../v2/hitl-resume.md)**（~1220 行）|
| **编排模式深入用法**（9 编排选型决策树 + 适用场景 + 反模式 + 嵌套组合范式 + Store/Skill/Retry/Hooks 横切能力）| **[v2/orchestration-patterns.md](../v2/orchestration-patterns.md)**（~1170 行）|
| 业务侧用法（写一个 BasicAgent / 编排 / 集成 cp）| [v2/usage-guide.md](../v2/usage-guide.md) |
| 4 层上下文压缩怎么联动 | **[v2/context-compaction.md](../v2/context-compaction.md)**（完整设计文档：算法 / 阈值公式 / 9 段结构 / NO_TOOLS_PREAMBLE / 附件重注入 / 断路器 / 跨层 marker / 时序图 / 排错决策树）+ `agent_v2/microcompact.go` / `agent_v2/autocompact.go` / `agent_v2/reactive_compact.go` 的包级 godoc |
| 加 binary 冒烟测试或想知道 commit 前必跑哪些 | [smoke-test-recipes.md](./smoke-test-recipes.md) |
| **路由先交给决策模型判**（带置信度的升级链 / Jev 与百炼两家决策模型接入 / 门槛怎么定 / 中文路由实测）| **[v2/route-decider.md](../v2/route-decider.md)** + `decision` 包 godoc |
| **接入/暴露 A2A 协议 agent**（消费远程 / 注册发现 / 暴露本地 / ZK binding）| `a2a/remote_tool.go` 包 godoc（两形态设计+业内对照）+ `a2a/server/executor.go` 包 godoc（HITL/流式/可观测/局限）+ `a2a/registry` go doc |
| 写测试时不知道用黑/白盒 / 表驱动适不适用 | [testing-conventions.md](./testing-conventions.md) |
| 设计 cp 接口 / 子图隔离 / Interrupt 语义时对照三方 (LangGraph / eino) | [three-way-comparison/](./three-way-comparison/) 20 个主题文档 (README + 19 themes: address-routing / agent-interface / checkpoint-storage / eino-langgraph-upgrades / error-handling / harness-survey / industry-survey-2026h1 / interrupt-resume / observability-{callback,eino,langgraph,trace-spec} / runtime-model-params / sdk-versioning / skill-system / state-model / store / subgraph-isolation / tool-visibility) |
| 找已知 bug 列表 / 历史 ISSUE 决策 | [../issue/issues.md](../issue/issues.md) + 各 `issue-XX-*.md` |
| 看 OKR / 完成度 | [okr.md](./okr.md)（v1 历史快照） |
| 历史：channel-based redesign 怎么从 v1 演化来 / Stage 1-7 commit 索引 | [v2/migration-history.md](../v2/migration-history.md) |
| 历史：PoC 期资料（PoC 报告 / channel-based 设计原始决策）| [v2/poc-report.md](../v2/poc-report.md) |

## 模块速查

| 模块 | 主入口文件 | 关键类型 | godoc |
|---|---|---|---|
| **schema** | `schema/message.go` | `Message`, `StreamReader[T]`, `MessageRole` | go doc |
| **model** | `model/interface.go` | `ChatModel` 接口；openai/qwen 实现；`APIError` 结构化错误 | go doc |
| **fallback** | `fallback/fallback.go` | `FallbackChatModel` 模型故障切换装饰器（`New` / `FailoverPolicy` / `DefaultFailoverPolicy`）；注入 `WithModel` 对 9 编排透明 | [v2/model-fallback.md](../v2/model-fallback.md) |
| **tool** | `tool/tool.go` | `Tool`, `Toolkit`, `FuncTool`, `StreamableTool` | go doc |
| **hook** | `hook/hooks.go` | `Hooks` 接口（8 callback）+ `NoOpHooks` + `ChainHooks` | go doc |
| **skill** | `skill/skillbox.go` | `SkillBox` 接口，`DefaultSkillBox` 关键词匹配实现；`SkillDescriptor.DisableModelInvocation/UserInvocable` 二维 frontmatter 控制（V2 P2.A）；`SkillDescriptor.Context/Agent/Model` fork 模式 + per-skill model 切换（V2 P2.C+B）| go doc |
| **mcp** | `mcp/client.go` / `hub.go` | `Client` 接口（StdioClient/StreamableHTTPClient）+ `Hub` 聚合 | go doc |
| **compose_v2** | `compose_v2/graph.go` | `GraphBuilder`, `Graph`, `NodeFunc`, `Channel[T]`, `Run`, `Checkpointer` | [v2/migration-history.md §核心类型速查](../v2/migration-history.md) |
| **agent_v2** | `agent_v2/agent.go` | `Agent`, `BasicAgent`, 8 编排 + `RunSession` + 4 层压缩 + `WithStore` / `WithStoreMemory`（第 4 层走 `store.Store` 接口）| 同上 |
| **a2a** | `a2a/remote_agent.go` | `RemoteAgent`（`NewRemoteAgent`/`NewRemoteAgentTool` 消费远程 A2A agent；`WithHumanGate` HITL 骑 host cp；`WithHooks` 观测）+ `NewRemoteAgentFromService` 发现 glue | go doc（remote_tool.go 包 godoc 含业内对照）|
| **a2a/registry** | `a2a/registry/instance.go` | `Directory`/`Registration` 接口 + `ServiceInstance`（§3 模型，Weight `*int`）+ `Selector` 三策略 + `NewInMemory` + `conformance.RunDirectoryTests` | go doc |
| **a2a/server** | `a2a/server/executor.go` | `NewExecutor`/`Handler`/`Card`/`Instance`——暴露 `agent_v2.Agent` 成 A2A server（server 侧 HITL taskID↔Resume 映射 + chunk→artifact 流式 + 可观测协议）| go doc（executor.go 包 godoc）|
| **a2a/zk** | `a2a/zk/zk.go`（独立 module）| `zk.New` ZooKeeper Binding v1（ephemeral + Base64url 路径编码 + malformed skip）| go doc |
| **decision** | `decision/client.go` / `decider.go` | `Client`（决策模型客户端，`Decide` 一次调用；System One 与 OpenAI Decisions 两种线上格式，`Protocol` 选）+ `Preset` / `NewClient`（接别的兼容服务）+ `Choice` / `Score` / `Noul` + `NewRouteDecider`（把决策模型包成 RoutingAgent 的决策方）| [v2/route-decider.md](../v2/route-decider.md) |
| **decision/jev** | `decision/jev/jev.go` | `NewClient(apiKey)` 直连 TypeSafe 的 Jev（默认模型 `jev-latest`）| 同上 |
| **decision/bailian** | `decision/bailian/bailian.go` | `NewClient(apiKey, region, workspaceID)` 接阿里云百炼的 `decision-model-preview`（`Beijing` / `Singapore` 两个地域）| 同上 |

## 编排速查

9 编排都实现 `agent_v2.Agent` 单接口，可无限嵌套，自动支持 HITL Resume。

| 编排 | 用途 | godoc |
|---|---|---|
| `BasicAgent` | ReAct 循环（preamble + model + tools + postamble **四节点** channel-based Graph + 循环回边；preamble 跑 StoreMemory/HistoryCompactor/Skill 预激活只跑首轮 ISSUE-74；postamble 跑 commitFinal+store.Put ISSUE-75；tools 节点内 N>=2 tc goroutine 并发 fanout ISSUE-81）| `agent_v2.BasicAgent` |
| `SequentialAgent` | 线性 step 链（前一输出作后一输入） | `agent_v2.SequentialAgent` |
| `ParallelAgent` | 并发 fan-out + Merger / Aggregator 聚合 | `agent_v2.ParallelAgent` |
| `LoopAgent` | 循环 body + ExitCondition + maxIterations | `agent_v2.LoopAgent` |
| `RoutingAgent` | Router 函数，或「决策方 → LLM Classifier → 兜底」升级链（决策方可用 `decision` 包接 Jev 或百炼的决策模型）| `agent_v2.RoutingAgent` |
| `HandoffAgent` | 多 agent 转交（transfer 工具或成员经 `HandoffTo` 做出的转交决定，交给 HandoffAgent 的上行信号 + maxHops） | `agent_v2.HandoffAgent` |
| `PlanAgent` | replan-每步 + sub-agents / TmpSubAgent / executor | `agent_v2.PlanAgent` |
| `ReflectAgent` | actor + Reflector / Evaluator+ReflectorAgent / EvalFunc | `agent_v2.ReflectAgent` |
| `GraphAgent` | 包任意 `compose_v2.Graph` 为 `Agent`（InputWriter / OutputReader） | `agent_v2.GraphAgent` |

## 4 层上下文压缩

> 完整设计文档：**[v2/context-compaction.md](../v2/context-compaction.md)** —— 算法 / 阈值公式 / 9 段结构 / NO_TOOLS_PREAMBLE 双放 / 附件重注入 / 断路器 / 跨层 marker / ReAct 时序图 / 排错决策树 / Round 6 设计权衡。

| 层 | 何时跑 | 何处跑 | 调用 LLM | 用法 |
|---|---|---|---|---|
| **第 1 层 MicroCompact** | model 节点入口（先于 AutoCompact）| `agent_v2.MicroCompactRun` 由 `modelFn` 最早段触发 | ❌ | `WithMicroCompact(&MicroCompactConfig{Enabled: true})` |
| **第 2 层 AutoCompact** | model 节点入口（MicroCompact 之后）| `agent_v2.AutoCompactRun` 由 `modelFn` 早段触发 | ✅ 摘要调用 | `WithAutoCompact(&AutoCompactConfig{Enabled: true, Model: m, ModelContextWindow: 200_000})` |
| **第 3 层 ReactiveCompact** | LLM 报 prompt-too-long 时 | `agent_v2.ReactiveCompactRun` 由 modelFn retry 包围 | ❌ | `WithReactiveCompact(&ReactiveCompactConfig{Enabled: true})` |
| **第 4 层 StoreMemory** | 跨 Run | `RunSession` + `store.Store` 接口（preamble 节点调 `store.Search` 注入；postamble 调 `store.Put` 保存）| ❌ | `WithStore(st) + WithStoreMemory(ns, &StoreMemoryConfig{...})` |

执行顺序（每轮 model 节点入口）：MicroCompact → AutoCompact → LLM 调用（含 ReactiveCompact retry）→ tools。

## 数据流全景（业务一次 Run 完整路径）

```
业务 caller
  │
  │ agent.Run(ctx, sess, input, ...) / agent.Resume(ctx, sid, ...)
  ▼
agent_v2 编排 wrapper (BasicAgent / SequentialAgent / ...)
  │
  │ runGraph: 内部编译为 *compose_v2.Graph + ChannelManager
  ▼
compose_v2.Run(ctx, g, cm, opts...)
  │
  │ 主循环 (`compose_v2/graph_loop.go::Run` 内 for {} 块, 按符号自查):
  │   1. 初始 computeNextTasks (StartNode → 第一 frontier)
  │   2. for { executeFrontier (并发跑节点 + drain SendDirective)
  │                ↓
  │                ApplyAll (合并点 reduce 所有 ChannelWrite)
  │                ↓
  │                computeNextTasks (eval cond + 算下一 frontier)
  │                ↓
  │                isEnd? break : continue }
  │   3. defer SaveNS (cp persistent — 失败/Pending 时)
  ▼
ChannelManager
  │
  │ channels + values (声明态 + 当前值)
  │ visitCount + pendingInterrupts + frontierAtExit (运行态)
  ▼
Checkpointer
  │
  │ SaveNS(sid, ns, *Checkpoint) — 业务自管 store (InMemory / Redis / Postgres)
  │ cp 协议: (sid, ns) 二级 key → 嵌套子图独立 cp 物理分离
  │
  │ Resume:
  │   cm.Restore(cp) → restore Channels + visitCount + frontier + PendingInterrupts
  │   continue from frontier
```

**Run 内部 BSP 4 阶段** (对齐 channel-engine.md §3):
1. **executeFrontier** — 多节点并发跑 (per-branch delta + drain SendDirective)
2. **recordInterrupt** — 失败/中断分支 query 先 record 再 ApplyAll (ISSUE-81 / R10 D-N1)
3. **ApplyAll** — 所有 ChannelWrite 合并点统一 reduce (LastValue / Append / Sum / Custom)
4. **computeNextTasks** — eval cond + 算下一帧 frontier

完整设计见 [channel-engine.md §3 graphLoop 主循环：BSP 4 阶段](../v2/channel-engine.md).

## 跨层 lifetime 总表

业务最易混淆的是 state 究竟存哪 + 何时清. 横向对照:

| 类型 | 持有方 | lifetime | 持久化位置 | 跨 Run | 业务实例化 |
|---|---|---|---|---|---|
| **RunSession** (`agent_v2.RunSession`) | 业务 caller | per-Run (Run/Resume 入参) | ❌ 内存 (events + values + lane) | ❌ 业务自管 RunID 串联 | `NewRunSession()` |
| **Channel values** (`cm.values`) | `*ChannelManager` (per-Run) | per-Run, 失败时 cp 持久化 | ✅ cp.Channels (Restore 时恢复) | ✅ 通过 Resume cp restore | `compose_v2.LastValue/Append/Sum/Custom` 工厂 |
| **Checkpoint** (`*Checkpoint`) | `Checkpointer` 实现 | 跨 Run/Resume (业务自管) | ✅ Checkpointer.SaveNS | ✅ 主索引 (sid, ns) | `cper.SaveNS` (框架自动) |
| **HistoryStore** (`agent_v2.HistoryStore`) | 业务 caller | 跨 Run，按**会话**（Owner+ConvID） | ✅ 业务实现（InMemory / Redis / PG，append-only） | ✅ 同一 ConvID 续接 | `agent_v2.NewInMemoryHistoryStore()` 或业务自实现 + `WithHistory`（仅 BasicAgent，opt-in） |
| **Store** (`store.Store`) | 业务 caller | 跨 Run/Session/thread (业务实现) | ✅ 业务实现 (InMemory / Redis / Postgres / mem0) | ✅ 业务自管 (`Search` 注入 / `Put` 持久化) | `store.NewInMemoryStore()` 或业务自实现接口 + `WithStore` / `WithStoreMemory` |
| **InterruptID** | `cm.pendingInterrupts` | per-中断, cp 持久化 | ✅ cp.PendingInterrupts | ✅ Resume 时 Restore 配 `WithResumeUserInputs` | 业务走 `compose_v2.Ask`（内部按 `InterruptSite` 算）|

**关键不变量**:
- **channel values 是业务唯一跨 Resume 安全持久化的 state** (其他业务 state 要么内存 / 要么业务自管; cp 还一起持久化 `CurrentFrontier`/`PendingInterrupts`/`NodeVisits` 引擎运行态保 Resume 一致, 业务不直接读写)
- **channel 是唯一 state 模型** — v1 typed local state (`ProcessState`) 已删; 业务想跨 Resume 累积值 (如重试次数) 用 channel reducer
- **cp 物理分离按 (sid, ns) 二级 key** — 嵌套子图 cp 不互覆 (ISSUE-50 防串台)
- **业务自定义 Agent / Hooks / Tool / ChatModel 实现传入框架时按 `isFrameworkNil` 协议防 Go interface typed nil 陷阱** (跨 6 包 30+ 入口, 详见 [usage-guide.md §8 反模式](../v2/usage-guide.md))

详见 [checkpoint.md](../v2/checkpoint.md)（cp 权威篇）+ [hitl-resume.md §3 InterruptID 稳定算法](../v2/hitl-resume.md).

## 测试用 LLM

**默认 qwen** `NewTestClient`（`model/qwen`，模型 `qwen3.6-flash`），baseURL = `https://dashscope.aliyuncs.com/compatible-mode/v1`（阿里云 dashscope，兼容 OpenAI API）。API key 走项目根 `.env` 的 `QWEN_API_KEY`（`.gitignore` 已排除，`example/envloader` 自动加载）。**DeepSeek（`model/deepseek`，`deepseek-v4-flash`，`DEEPSEEK_API_KEY`）仍可用作备选**（`observability_demo` 等 `-provider deepseek|qwen` 两路保留）。详 CLAUDE.md "测试用 LLM" §.
