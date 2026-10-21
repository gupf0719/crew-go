# crew-go OKR（v1 期历史快照）

> ⚠️ **历史 OKR 快照**：本文档是 v1 时代（2026-04-20 截止）的全周期 OKR 记录。
> Stage 7 后 v1 已删，文中 `RunGraph` / `ProcessState` / `TypedGraph` / `GraphState`
> 等 v1 API 已不复存在，等价能力在 v2 (`compose_v2/agent_v2`) 中重新实现。
> v2 演化历史与 commit 索引看 [`../v2/migration-history.md`](../v2/migration-history.md)；
> 当前架构总览看 [`./architecture.md`](./architecture.md)。
>
> 本文件保留作 v1 期 OKR 历史记录，新 OKR 周期开始后另起新文件。
>
> 起点：项目立项
> 末次快照：2026-04-20（v1 时代）
> 对标对象：eino · LangChain · LlamaIndex · AutoGen
>
> **权重**（原 `okr-table.md` 表格版已并入本文、删除）：已达成 O1-O6（计 50%）+ 进行中 O7-O10 合计 50%。
> **v2 完成度提醒**：O6-O10 的 🚧 是 v1（2026-04）当时状态——v2 中统一图引擎/流式/StatefulInterrupt（原 O7）+ OTel 可观测/多用户隔离（原 O8）大部分已落地，**当前完成度以 [architecture.md](./architecture.md) / [migration-history.md](../v2/migration-history.md) 为准**；v1 快照后新增的 A2A 接入 + FallbackChatModel 不在本快照范围。

---

## 愿景（北极星）

做 **Go 原生、面向生产的 LLM Agent 编排框架**。以 Graph 为统一底座，把"单 Agent ReAct"与"多 Agent 协作"收敛到同一执行引擎，在保持 Go 生态简洁的前提下，覆盖从 demo 到生产部署的完整链路。

---

# 一、已达成 OKR（立项 → 2026-04-20）

## ✅ O1：统一图执行引擎

> 所有编排模式在内部编译为同一 `Graph`，循环保护、Checkpointing、Hook 等横切能力只实现一次，全模式受益。

| KR | 交付 |
|---|---|
| KR1.1 图构建器 | `GraphBuilder`：`AddNode / AddAgentNode / AddEdge / AddConditionalEdge / AddFanOutEdge / AddChoicer / WithMaxVisit / WithStateFactory` |
| KR1.2 图执行 | `RunGraph(ctx, g, state, GraphRunOptions)` 单一入口（opts 承载 Checkpointer/OrchHooks）/ `ResumeGraph`（从 Checkpoint 恢复） |
| KR1.3 Typed State | `StateFactory` + `ProcessState[S]`（锁保护读写）+ `GetLocalState[S]`（执行后提取）；替换了早期 `map[string]any` 的类型不安全设计（ISSUE-10） |
| KR1.4 条件边与 Fan-out | `EdgeCondition` 支持 error 返回；`AddFanOutEdge` 提供图级真并发；`AddChoicer` 命名路由节点使 `route:<name>` 对 OrchHooks 可见 |
| KR1.5 拓扑校验 | `GraphBuilder.Validate()` 独立 API：循环检测、EndNode 可达性、孤立节点（ISSUE-27 / 39） |
| KR1.6 图级观测 | `OrchestrationHooks`（OnGraphStart / End / OnNodeStart / End）+ `NoOpOrchestrationHooks` |
| KR1.7 类型安全包装 | `TypedGraph[I, O]`：泛型入/出口转换，内部仍走 `GraphState` |
| KR1.8 Superstep 并发 | ISSUE-29 完成：本轮就绪节点一次性并发执行（部分场景） |

---

## ✅ O2：核心 Agent 与横切能力

> 单 Agent 的 ReAct 循环 + 会话 / 检查点 / 路径追踪等横切机制。

| KR | 交付 |
|---|---|
| KR2.1 BasicAgent | 单 ReAct 循环：LLM → tool-call → 执行 → 循环；`WithModel / WithSystemPrompt / WithToolkit / WithMaxRounds / WithHooks / WithMemory / WithSkills`（注：原 `WithMemory` 已由 `WithStore` + `WithStoreMemory` 替代，PR 3.3） |
| KR2.2 RunOption per-call 覆盖 | `WithRunMaxRounds / WithRunSystemPrompt / WithRunModel`（ISSUE-18） |
| KR2.3 Hook 生命周期 | 8 个回调：OnAgentStart / End、OnModelStart / End / EndStream、OnToolStart / End、OnError；`NoOpHooks` 嵌入 + `ChainHooks` 串联 |
| KR2.4 RunSession 与事件 | `RunSession.ID`（UUID，ISSUE-22）、`AddEvent / Events / History`；`AgentEvent{Output, Action, Err, Checkpoint}` |
| KR2.5 AgentAction 控制流 | `{Exit, BreakLoop, CustomizedAction}`；`CustomizedAction` 标注 `json:"-"` 跳过持久化 |
| KR2.6 Checkpointer 接口化 | `Save / Load / List / Delete / Exists / GetLatestCheckpoint / Cleanup / GetMetadata`；`InMemoryCheckpointer` 内置实现（ISSUE-02 / 21） |
| KR2.7 InterruptError | error-as-control-flow：节点返回触发存档挂起，`ResumeGraph` 恢复；超时路径同样保存 checkpoint（ISSUE-26） |
| KR2.8 Memory | `Read / Write` 接口 + `InMemoryMemory` + `MemoryConfig{Read, Write}`（ISSUE-12 / 15）（注：已由 `store.Store` 接口 + `store.NewInMemoryStore()` + `WithStore` / `WithStoreMemory` 替代，PR 3.3） |
| KR2.9 RunPath | `WithRunStep / GetRunPath / RunPathString`，ctx 透传调用链 |
| KR2.10 Agent.Run 签名 | 显式 session 传参 + `[]*AgentEvent` 返回值；`FinalMessage` / `LastAction` 辅助（ISSUE-01） |

---

## ✅ O3：编排模式矩阵（9 种 + 2 种组合器）

> 所有模式实现 `agent.Agent` 接口，可无限嵌套；每种模式有 `XxxConfig{MaxRetries, MaxRounds}` + `WithXxxConfig`（ISSUE-37）。

| KR | 模式 | 核心能力 |
|---|---|---|
| KR3.1 | BasicAgent | ReAct 循环基础 |
| KR3.2 | SequentialAgent | 线性链，前一输出作后一输入 |
| KR3.3 | ParallelAgent | 并发 fan-out/fan-in；`Merger` / `Aggregator`；ISSUE-62 后编译为图原生超步 frontier，失败统一走 `superstep_snapshot` 重跑（兄弟不 cancel，与 LangGraph pending_writes 同模型）；签名对齐 spec（ISSUE-14） |
| KR3.4 | LoopAgent | 条件迭代；`ExitCondition` + `BreakLoop` 信号（ISSUE-07） |
| KR3.5 | RoutingAgent | LLM 分类器 / Router 函数；多路由 fan-out（ISSUE-05）；路由节点展开为独立图节点（ISSUE-08） |
| KR3.6 | HandoffAgent | 多 agent 交接；`MaxHops` 防无限；`State(session)` 暴露激活 agent（ISSUE-38） |
| KR3.7 | PlanAgent | Replan-每步（ISSUE-45）；结构化 `PlanStep`（ISSUE-04）；`Planner` 接口 + 内置 `NewLLMPlanner`；`TmpSubAgent` 动态子 agent（ISSUE-34） |
| KR3.8 | ReflectAgent | Actor-Eval-Reflector 循环；`ReflectOpinion{GoalMatched, Opinion}` 结构化反馈 + 函数式 `Reflector`（ISSUE-35） |
| KR3.9 | GraphAgent | 直接暴露 GraphBuilder 的自定义拓扑 |
| KR3.10 | AgentAsTool | Agent 包成 `tool.Tool`，supervisor 基础 |
| KR3.11 | RunnableAsTool | 普通函数包成 tool.Tool |
| KR3.12 | 统一 builder API | 编排层 `AddAgent(name, a) / SetXxx(a)` 统一接口（ISSUE-36） |

---

## ✅ O4：工具 / Skill / MCP 扩展层

> 三个外部能力接入通道：本地工具函数、本地 Skill 文件、远端 MCP Server。

| KR | 交付 |
|---|---|
| KR4.1 Tool 基础 | `Tool` 接口 + `FuncTool` 包装 + 线程安全 `Toolkit`；框架级 error codes（timeout / tool_not_found / invalid_arguments 等）直接中止 Agent |
| KR4.2 StreamableTool | 可选接口 `StreamCall`，框架 type switch 自动识别（ISSUE-16） |
| KR4.3 节点级重试 | 所有编排模式支持 `MaxRetries` + 指数退避（ISSUE-17 / 25） |
| KR4.4 Skill 选择子系统 | `SkillBox` 接口 + `DefaultSkillBox`（文件系统 `SKILL.md` 扫描 + CJK/英文关键词匹配）+ `SkillDescriptor / ActiveSkill / SkillMatchResult` + V1 `tool_call` 激活模式（ISSUE-19） |
| KR4.5 MCP 集成 | `Client` 接口 + `StdioClient`（子进程）+ `StreamableHTTPClient`（HTTP）+ `Hub` 聚合器 + `RemoteTool` 适配；命名规则 `mcp_<client>_<source>` |
| KR4.6 ChatModel 双通道 | `Generate` + `Stream` + `WithTools` 返回新实例；`model/openai` 兼容实现；`model/qwen` 联调 |

---

## ✅ O5：多 Agent 会话一致性与可恢复性

> ISSUE-40~45 系列：解决跨 agent 共享 session、跨 resume 事件丢失、LLM 输入双注入等隐性数据流问题。

| KR | 交付 |
|---|---|
| KR5.1 事件持久化 | `Checkpoint.Events` 随 session 序列化，Resume 后 `sess.History() / sess.Events()` 可用（ISSUE-40） |
| KR5.2 并发 session 隔离 | `ForkLane` + `JoinLanes`：child lane 单 writer 无锁写，Join 按 `CreatedAt` 稳定排序；重复 Join 直接 panic（ISSUE-41 / 43·M2） |
| KR5.3 跨 agent 消息改写 | `rewriteCrossAgentMessage`：非本 agent 的 `assistant(tool_call) / role=tool` 降级为 `user("For context: [X] ...")`，避免 OpenAI 422；`store.Search` 注入同样降级（ISSUE-42 / 43·L1） |
| KR5.4 AgentName 归属收口 | 默认 Name 碰撞 / 空 owner 退化 / omitempty 丢失 / Sequential Messages[0] 防御等 6 个边界问题修复（ISSUE-43） |
| KR5.5 LLM 输入路径解耦 | 新增 `SelfHistory(name)` 只返回本 agent 自产事件，供 BasicAgent 构造 LLM 请求；`HistoryFor(name)` 降级为审计/查询 API（ISSUE-44） |
| KR5.6 上下文窗口管理（4 层压缩系统） | **ISSUE-65~68 全部落地**：MicroCompact（modelFn 最早段清白名单 tool_result 内容，无 LLM）+ AutoCompact（modelFn 入口 MicroCompact 之后、token 阈值触发 LLM 摘要 + NO_TOOLS_PREAMBLE 双放 + 9 段结构化 + `<analysis>` chain-of-thought + 附件重注入 + 断路器）+ ReactiveCompact（prompt-too-long 兜底 retry + token gap 解析 + 删最旧组保配对）+ StoreMemory（RunSession + `store.Store` 接口，原 Memory 接口已由 store 替代，PR 3.3）；4 个共享 helper（compose_v2 层）：`EstimateTokens` / `GroupMessagesByAPIRound` / `AdjustIndexToPreserveAPIInvariants` / `CircuitBreaker`。设计参考 [doc/v2/context-compaction.md](../v2/context-compaction.md) |

---

## ✅ O6：开发体验与示例

| KR | 交付 |
|---|---|
| KR6.1 多轮对话 example | BasicAgent + PlanAgent 已落地（ISSUE-32）；`example/orchestration/plan_chat/` demo |
| KR6.2 架构文档 | `doc/description/design.md` · `patterns.md` · `eino-reference.md` 三大文档成型；45+ ISSUE 全量记录 |
| KR6.3 跨 session 上下文 | `CLAUDE.md` 架构摘要 + Hook 选择速查 + 持久化约定，跨会话快速建立上下文 |

---

# 二、进行中 / 待启动 OKR（2026 Q2 → Q3）

> 下阶段聚焦：图引擎下一代能力 · 生产化可观测性 · RAG 三大件 · DevOps 与生态。

## 🚧 O7：图引擎下一代（P0）

> 补齐与 eino 的底座差距。

| KR | 目标 | 验收 |
|---|---|---|
| KR7.1 Superstep 调度全面化 | 线性 BFS → Pregel Superstep 全路径；ParallelAgent 改走通用路径而非专用 | 菱形 DAG 各分支真并发；新增 `graph_superstep_test.go` |
| KR7.2 统一流式 Runnable[I,O] | 对齐 eino：Invoke / Stream / Collect / Transform 自动适配；Agent 补 `StreamRun` | BasicAgent 端到端 token 级流式；Sequential 串起 Stream 流转；关联 ISSUE-30 |
| KR7.3 图级多入边 Fan-in | 任意节点声明多入边自动 merge；`MergeStrategy`（concat / reduce / custom）可插拔 | 单节点接收 ≥3 条入边，merge 函数用户可插拔 |
| KR7.4 StatefulInterrupt | `InterruptError` 升级为带节点输入快照；Resume 自动重注入；结构化控制信号从 `CustomizedAction any` 迁出 | HandoffAgent / PlanAgent 跨 resume 不依赖 `CustomizedAction` |
| KR7.5 节点级中间件 | `NodePreHandler / NodePostHandler`（nodeID 粒度） | 为单节点注入 retry / logging 中间件的测试通过 |

---

## 🚧 O8：生产可观测性（P0→P1）

| KR | 目标 | 验收 |
|---|---|---|
| KR8.1 全组件 callback | Hook 从 Agent 层下沉到 ChatModel / Tool / Memory / Retriever | 任一组件调用链可被单一 handler 观测；关联 ISSUE-31 |
| KR8.2 OpenTelemetry | 关键路径注入 OTEL trace；`WithTracer` 选项（默认 NoOp） | PlanAgent 一次执行的 trace 在 Jaeger 完整可见 |
| KR8.3 Token / Cost 计量 | ChatModel 结构化 usage；Session 维度聚合 cost | `RunSession.Usage()` 返回汇总；demo 打印总成本 |
| KR8.4 多用户会话隔离 | Checkpointer 扩展 `userID / sessionID` 命名空间；至少一个持久化实现（SQLite 或文件系统） | 并发不同用户 Resume 互不干扰 |

---

## 🚧 O9：RAG / 提示词 / 工具选择（P1）

> 对标 LangChain / LlamaIndex / eino，目前完全空白。

| KR | 目标 | 验收 |
|---|---|---|
| KR9.1 Retriever / Indexer / Embedding | 新增 `retriever` · `indexer` · `embedding` 包 + `schema.Document`；内置内存向量库（cosine）+ OpenAI embedding 实现 | RAG demo 串起 embedding → indexer → retriever → BasicAgent |
| KR9.2 Document Loader / Splitter | `loader` 包（file / http / markdown）+ `splitter` 包（token / 段落 / 递归） | 本地 md 目录一键向量化入库 |
| KR9.3 PromptTemplate | 替换 `WithSystemPrompt(string)` 字符串拼接；支持变量绑定 / 版本 / MessagesTemplate | PlanAgent 内置 planner/respond prompt 改用模板 |
| KR9.4 ToolChoice | `WithToolkit` 增 `ToolChoice`（auto / required / none / 指定）；per-call 动态收窄 | RoutingAgent 分类后可裁剪下游工具集 |

---

## 🚧 O10：DevOps 与生态（P2）

| KR | 目标 | 验收 |
|---|---|---|
| KR10.1 Agent as Service 脚手架 | `cmd/agentd`：读 YAML → 启 HTTP / SSE 服务；内置 `/run` · `/stream` · `/resume` | 一个 YAML 即可发布任意 compose 图为服务 |
| KR10.2 多轮 example 补齐 | HandoffAgent / ReflectAgent / RoutingAgent 三种模式多轮 demo | 关联 ISSUE-33；`make smoke` 跑通所有 example |
| KR10.3 API 稳定性声明 | 每个核心包 godoc 导读 + 稳定性级别（stable / experimental）；v0.1 tag | 对外 stable 包清单公开 |
| KR10.4 CI / 质量门禁 | GitHub Actions：build + test + race + vet + golangci-lint；核心包覆盖率 ≥75% | 所有 PR 必过门禁；README 挂覆盖率徽章 |

---

# 三、取舍与不做的事

- **不做跨语言 codegen**：继续仅以 Go 为一等公民，spec 只作参考
- **不做内建前端 UI**：可观测性优先对接外部（Jaeger / Prometheus / Grafana）
- **不做 Agent 市场 / 模型路由网关**：聚焦编排框架本身
- **RAG 只做接口 + 参考实现**：不绑定具体向量库，生态实现交由外部适配
- **不绕过 CLAUDE.md 约束**：不改 `agentdock-spec`，不造 `util/common/misc` 万能包

---

# 四、对标一览（当前状态）

| 能力 | crew-go | LangChain | eino | LlamaIndex | AutoGen |
|---|---|---|---|---|---|
| ReAct Agent | ✅ | ✅ | ✅ | ✅ | ✅ |
| 多 Agent 编排（模式数） | ✅ 9+ | ~5 | ~8 | 部分 | ✅ |
| 工具调用 | ✅ | ✅ | ✅ | ✅ | ✅ |
| MCP 集成 | ✅ | 部分 | 部分 | 部分 | ❌ |
| Skill 本地文件集成 | ✅ | ❌ | ❌ | ❌ | ❌ |
| Checkpointing + Resume | ✅ | ✅ | ✅ | 部分 | 部分 |
| 会话事件持久化 | ✅ | ✅ | ✅ | 部分 | ✅ |
| 跨 agent 消息改写 | ✅ | 部分 | ✅ | ❌ | ✅ |
| Superstep 并发 | 部分 | ❌ | ✅ | ❌ | ❌ |
| 统一流式抽象 | 🚧 O7.2 | ✅ | ✅ | ✅ | 部分 |
| StatefulInterrupt | 🚧 O7.4 | ✅ | ✅ | ❌ | ❌ |
| 组件级 callback | 🚧 O8.1 | ✅ | ✅ | ✅ | ❌ |
| OTEL Trace | 🚧 O8.2 | ✅ | 部分 | ✅ | ❌ |
| 多用户会话隔离 | 🚧 O8.4 | ✅ | ✅ | 部分 | ✅ |
| Retriever / Indexer | 🚧 O9.1 | ✅ | ✅ | ✅ | 部分 |
| Document Loader | 🚧 O9.2 | ✅ | ✅ | ✅ | ❌ |
| PromptTemplate | 🚧 O9.3 | ✅ | ✅ | ✅ | ❌ |
| ToolChoice | 🚧 O9.4 | ✅ | 部分 | 部分 | ❌ |
| Agent-as-Service | 🚧 O10.1 | ✅（LangServe） | 部分（ADK） | 部分 | ❌ |

---

# 五、里程碑回顾

| 阶段 | 交付 |
|---|---|
| M1 基础积木 | schema / model / tool / hook / BasicAgent（O2 + O4 部分） |
| M2 图引擎成型 | compose 包 + Sequential/Parallel/Loop/Routing（O1 + O3 前半） |
| M3 编排模式齐备 | Handoff / Plan / Reflect / GraphAgent / TypedGraph（O3 完整） |
| M4 可恢复性 | Checkpointer / InterruptError / Resume（O2·6~7 + O1·KR1.2） |
| M5 扩展能力 | Skill 子系统 / MCP 集成 / Memory（O4·4~5 + O2·8） |
| M6 会话一致性 | ISSUE-40~45 系列（O5 整体） |
| **M7 下一代底座** | **进行中：Superstep 全面化 / 统一流式 / StatefulInterrupt**（O7） |
| M8 生产化 | 待启动：可观测性 + 多用户隔离（O8） |
| M9 RAG 生态 | 待启动：Retriever / Loader / PromptTemplate（O9） |
| M10 DevOps | 待启动：Agent-as-Service / v0.1 稳定（O10） |
