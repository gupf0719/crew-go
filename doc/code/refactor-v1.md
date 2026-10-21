# crew-go 第一轮返工记录（v1 时代历史文档）

> ⚠️ **历史文档**：本文档记录 v1 (`agent/` + `compose/`，GraphState-based) 的初版返工。
> Stage 7 后 v1 已删，v2 (`agent_v2/` + `compose_v2/`，channel-based) 是唯一实现。
> 本文件保留作 v1 期工作历史，新代码与新决策不参考此文档；v2 演化历史看
> [`../v2/migration-history.md`](../v2/migration-history.md)。

参考文档：`crew-implementation-guide.md`，对照 Eino (`/Users/gupengfei/program/go/src/eino/adk`)。

---

## 总体概览

本轮返工分两个阶段完成，共新增/修改 10 个文件，覆盖 Graph 引擎、全部编排 Agent、Checkpoint 机制和历史压缩工具。

| 阶段 | 文件 | 核心内容 |
|---|---|---|
| Phase 1 | `compose/graph.go` | 并发安全 State、循环保护、EndNode 校验 |
| Phase 1 | `compose/helpers.go` | `ParallelAgentNode`、Merger、错误策略 |
| Phase 1 | `compose/graphagent.go` | `GraphAgent`（Graph 包装为 Agent） |
| Phase 1 | `compose/sequential.go` | SequentialAgent → 线性 Graph |
| Phase 1 | `compose/parallel.go` | ParallelAgent → fan-out Graph |
| Phase 1 | `compose/loop.go` | LoopAgent → 条件自循环 Graph |
| Phase 1 | `compose/routing.go` | RoutingAgent → classify/dispatch Graph |
| Phase 1 | `compose/handoff.go` | HandoffAgent → step/条件自循环 Graph |
| Phase 1 | `compose/plan.go` | PlanAgent → plan/exec 循环 Graph |
| Phase 1 | `compose/reflect.go` | ReflectAgent → actor/eval/reflect Graph |
| Phase 2 | `compose/checkpoint.go` | InterruptError 捕获、Checkpointer 存档/恢复、ResumeGraph |
| Phase 2 | `compose/compact.go` | MicroCompact（截断）、AutoCompact（LLM 总结） |
| Phase 3 | `compose/orch_hooks.go` | OrchestrationHooks 接口 + NoOp 实现 |
| Phase 3 | `compose/typed_graph.go` | TypedGraph[I, O] 泛型外壳 |
| Phase 3 | `compose/fanout.go` | AddFanOutEdge 真正 fan-out 边 |
| Phase 3 | `compose/agent_tool.go` | AgentAsTool / RunnableAsTool |
| Phase 3 | `compose/stream.go` | StreamCollect / GenerateAsStream |
| Phase 3 | `compose/routing.go` | SubAgentDescriptor 完整实现（description + DescriptorPrompt） |

**设计原则**：所有编排 Agent 内部统一编译为 Graph 执行，公开 API 保持稳定；控制流通过 `GraphState` 传递，不依赖全局变量。`RunGraph` 单一入口，不启用可选能力时传 `GraphRunOptions{}`，扩展能力（Checkpointer / OrchHooks）通过 `GraphRunOptions` 注入。

---

## 已完成改动

### 1. Graph 引擎（`compose/graph.go` + `helpers.go`）
- `GraphState.Data` 并发安全：`Get/Set/Update` 加 `sync.RWMutex`
- `Compile()` 别名：对齐 ADR-1 术语
- EndNode 可达性校验：Build 阶段若无任何边可达 END 则报错
- `WithMaxVisit()`：允许调用方覆盖循环保护上限
- `AppendMessageSafe()`：供并行 goroutine 安全追加消息
- `ParallelAgentNode(nodes, merger, policy)`：并行 fan-out helper，结果按声明顺序聚合

### 2. SequentialAgent（`compose/sequential.go`）
- 内部编译为线性 Graph
- 新增 `WithHistoryRewriter(fn)` option，可自定义节点间输出→输入转换

### 3. ParallelAgent（`compose/parallel.go`）
- 内部编译为单节点 Graph，节点使用 `ParallelAgentNode`
- 结果有序：`[]ParallelResult` 按声明顺序，消除 map 随机性
- 新增错误策略 `WithErrorPolicy(ContinueOnError | CancelOnError | FailFast)`
- 结果存入 `state.Get("parallel_results")` 供下游消费

### 4. LoopAgent（`compose/loop.go`）
- 内部编译为 Graph：body 节点 + 条件自循环边
- 公开 API 不变，语义与原版一致

### 5. RoutingAgent（`compose/routing.go`）
- 内部编译为 2 节点 Graph：`classify → dispatch`
- 决策信号优先级：① tool-call `route_to` → ② `Annotations["route_target"]` → ③ 文本匹配（fallback）
- 路由键大小写不敏感（先精确，再 `ToLower` 回退）
- 新增 `SubAgentDescriptor` 和 `Descriptors()` 方法

### 6. HandoffAgent（`compose/handoff.go`）
- 内部编译为 Graph：step 节点 + 条件自循环边
- 共享 State：`active_agent` 挂在 `GraphState.Data`，消息历史累积在 `State.Messages`
- Session 历史转移：自动创建/复用 `RunSession`，每轮同步写入，BasicAgent 子节点通过 `session.History()` 可见完整对话
- 信号优先级：① tool-call `transfer_to_agent` → ② `Annotations["handoff_target"]` → ③ `HANDOFF:` 前缀
- 超 maxHops 时返回 `lastOutput + error`，不丢弃中间结果

### 7. PlanAgent（`compose/plan.go`）
- 内部编译为 Graph：plan 节点 + exec 循环节点
- 修复 `DefaultPlanParser`：用 `unicode.IsDigit` 遍历，正确解析 `10.`、`12)` 等多位数步骤

### 8. ReflectAgent（`compose/reflect.go`）
- 内部编译为 3 节点 Graph：`actor → eval → reflect → 条件回 actor`
- 修复多跑一轮 bug：actor 执行次数严格等于 `maxRounds`
- 新增 `WithEvalThreshold(float64)`：evaluator 输出中首个浮点数 >= 阈值即接受
- 修复 isApproval：改为词边界匹配，不再误匹配 "goodbye" 中的 "good"
- 删除手写 `contains/searchString`，改用标准库

---

## Phase 2（已完成）

### 9. InterruptError + Resume 链路（`compose/checkpoint.go`）
- `RunGraph(ctx, g, state, GraphRunOptions)` — 单一执行入口；opts 承载 Checkpointer / OrchHooks
- 当节点返回 `*agent.InterruptError` 时，自动调用 `Checkpointer.Save`，保存当前节点 ID + GraphState
- `ResumeGraph(ctx, g, checkpointer, checkpointID)` — 从 checkpoint 恢复执行，从被中断节点重跑
- 内部 `runGraphFrom` 从任意节点入口执行图，供 Resume 使用
- Checkpoint 存储：`Messages`、`LastOutput`（JSON）、`current_node`、`data`（best-effort JSON）

### 10. InMemoryCheckpointer 接入 Graph（`compose/checkpoint.go`）
- `GraphRunOptions.Checkpointer` 字段接受任意 `agent.Checkpointer` 实现
- `InMemoryCheckpointer`（已在 `agent/session.go` 实现）直接可用
- 存档/恢复逻辑通过 `marshalGraphState` / `unmarshalGraphState` 与 Checkpointer 解耦

### 11. MicroCompact（`compose/compact.go`）
- `MicroCompact(messages, maxPairs)` — 保留所有 system 消息 + 最近 `maxPairs*2` 条对话消息
- maxPairs ≤ 0 视为无限制，直接返回原 slice（零分配）

### 12. AutoCompact（`compose/compact.go`）
- `AutoCompact(ctx, model, messages, maxPairs)` — 用 LLM 总结超出限制的早期历史
- 返回 `[system...] [summary system msg] [recent maxPairs*2 msgs...]`
- 总结失败时返回原 slice + error，调用方可降级到 MicroCompact

## Phase 3（已完成）

### 13. OrchestrationHooks（`compose/orch_hooks.go` + `compose/checkpoint.go`）
- 新增 `OrchestrationHooks` 接口：`OnGraphStart/End`、`OnNodeStart/End`
- `NoOpOrchestrationHooks` 提供空实现，可按需覆盖单个方法
- 通过 `GraphRunOptions.OrchHooks` 注入，`RunGraph` 和 `runGraphFrom`（Resume 路径）均支持
- `OnGraphEnd` / 节点 err 通过 named return + defer 精确传递给 hook，零歧义

### 14. TypedGraph[I, O]（`compose/typed_graph.go`）
- 泛型外壳：`NewTypedGraph(g, toInput, fromOutput)` — 入口/出口各一个转换函数
- 类型安全在 `NewTypedGraph` 调用时由 Go 泛型编译器校验（ADR-1）
- 内部仍走 `GraphState`，向后完全兼容；`Graph()` 方法暴露底层图供 ResumeGraph 使用

### 15. AddFanOutEdge（`compose/fanout.go`）
- `GraphBuilder.AddFanOutEdge(from, targets, to, merger, policy)` — 真正 fan-out 边
- 内部合成 `__fanout_<from>__` 节点，并行跑各 target 的 NodeFunc，隔离 state（`cloneStateForFanOut`）
- 结果存入 `state.Get("fanout_results")`，与 ParallelAgentNode 约定一致
- 支持 `ContinueOnError / CancelOnError / FailFast` 三种错误策略

### 16. AgentAsTool / RunnableAsTool（`compose/agent_tool.go`）
- `AgentAsTool(agent, description)` → `tool.Tool`，单 `input` 字段，错误转 `tool.Failure`
- `RunnableAsTool(name, desc, fn)` → `tool.Tool`，包装任意 `func(ctx, string) (string, error)`
- 两者均实现 `tool.Tool` 接口，可直接 `Toolkit.Register()` 进 supervisor agent

### 17. Stream 适配（`compose/stream.go`）
- `StreamCollect(ctx, sr)` — Stream → Collect（ADR-2）：拼接 Content，非 Content 字段取最后一个 chunk
- `GenerateAsStream(ctx, model, messages, opts...)` — Generate → Stream（ADR-2）：单 chunk 流，供统一流式调用链使用
- context 取消时主动 Close reader，不泄漏

### 18. SubAgentDescriptor 完整实现（`compose/routing.go`）
- 新增 `WithRouteDescriptions(map[string]string)` option
- `Descriptors()` 现在填充 `Description` 字段（而非空字符串）
- 新增 `DescriptorPrompt() string` — 渲染为可直接注入 system prompt 的路由列表文本

---

## spec 对齐 —— 公司 spec 新 commits 同步（2026-04-09）

触发原因：公司 spec 合并了多个新提交，核心变化是 `common` 模块整合、`CommError` 由接口改为结构体、`MessageStreamReader` 移入 `model` 模块、`ToolOutput.Result` 结构化。

### 19. `schema/tool.go` — `ToolInfo` 改为 alias

- 删除本地 `ToolInfo struct` 定义，改为 `type ToolInfo = common.ToolInfo`
- 遵循 CLAUDE.md 原则：`schema` 包只做 type alias，不另起炉灶

### 20. `schema/message.go` — 新增 common 类型 alias

新增以下 alias，统一通过 `schema` 包对外暴露 spec 的共享类型：

| alias | 原类型 |
|---|---|
| `ModelConfig` | `common.ModelConfig` |
| `ModelInput` | `common.ModelInput` |
| `ModelOutput` | `common.ModelOutput` |
| `ToolInput` | `common.ToolInput` |
| `ToolOutput` | `common.ToolOutput` |
| `ToolCallResult` | `common.ToolCallResult` |
| `CommError` | `common.CommError` |

### 21. `hook/hooks.go` — 全面对齐 spec hook 模块

**删除：**
- `SpecError`、`WrapError()`：原为适配 `common.Error` 接口而写的适配层，spec 改为 struct 后不再需要
- `StreamReader` 接口、`schemaStreamAdapter`、`WrapStream()`：spec 将 stream reader 统一移入 `model.MessageStreamReader`
- 本地 `ModelConfig`、`ModelInput`、`ModelOutput`、`ToolInput`、`ToolOutput` 类型定义（改用 `schema.*` alias）

**更新接口签名（对齐 spec `hook.Hooks`）：**
- `OnModelStart/End`：参数改为 `*schema.ModelInput` / `*schema.ModelOutput`
- `OnModelEndStream`：`StreamReader` → `specModel.MessageStreamReader`，stream 每帧携带 `*schema.Message`（Usage 在最后一帧的 `Metadata.Usage` 里，非退化）
- `OnToolStart`：`*hook.ToolInput` → `*schema.ToolInput`
- `OnToolEnd`：`*hook.ToolOutput` → `*schema.ToolOutput`（`Result` 字段由 `string` 改为 `*schema.ToolCallResult`）
- `OnError`：参数由 `specCommon.Error`（接口）改为 `*schema.CommError`（结构体），并新增 `error` 返回值
- `NoOpHooks`、`ChainHooks` 同步更新

**保留 Go SDK 扩展：**
- `AgentInfo.RunPath []string` 字段（spec 无，Go 运行时专属）

### 22. `agent/event.go` — `Err` 字段改为结构化错误

- `Err error` → `Err *schema.CommError`
- 语义：event 里携带的错误是跨语言可传递的结构化对象，不是 Go 本地接口

### 23. `agent/react.go` — 更新 hook 调用点

- `hook.WrapError(err)` 全部替换为 `&schema.CommError{Code: "...", Message: err.Error()}`
- `hook.ModelInput/Output` → `schema.ModelInput/Output`；去掉 `modelOutput.Usage` 冗余赋值行，保留一处（Usage 同时存在于 `message.Metadata.Usage` 和 `ModelOutput.Usage`，明确注释）
- `hook.ToolInput` → `schema.ToolInput`，字段 `CallID` → `CallId`（与 spec JSON 命名对齐）
- `hook.ToolOutput` → `schema.ToolOutput`，`Result string` → `Result *schema.ToolCallResult`
- 新增 `toolResultToSpec(r *tool.Result) *schema.ToolCallResult`：将内部 `tool.Result` 转为 spec 结构化结果供 hook 使用；LLM 侧仍走 `serializeToolResult` 序列化为字符串
