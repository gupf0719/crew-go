# crew-go SDK 代码示例索引

> 找「X 功能怎么写」的导航地图。每行 = 功能 → **可跑 example**（真实代码，复制即用）+ **深度文档**（设计/option/反模式）。
>
> - 第一次用 → 先看 [quickstart.md](quickstart.md)（5 分钟跑通第一个 Agent）
> - 编排选型 + 每编排完整例 → [orchestration-patterns.md](v2/orchestration-patterns.md)（本文不重复其内容，只做索引）
> - example 跑法：`go run ./example/<dir>`（runnable）或 `go test ./example/... -run <Name>`（adv_* 是 test 形式，`*_mock_test.go` 离线 / `*_test.go` 真 LLM）

## 9 编排

| 编排 | 可跑 example | 深度文档 |
|---|---|---|
| **BasicAgent**（ReAct 单 Agent + 工具）| `example/orchestration/basic_chat`（多轮 CLI，runnable）/ `adv_01_basic_agent_test.go` | [orchestration-patterns §2](v2/orchestration-patterns.md) |
| **SequentialAgent**（顺序链）| `adv_02_sequential_agent_test.go` | [§3](v2/orchestration-patterns.md) |
| **ParallelAgent**（并发 fan-out + 聚合）| `adv_03_parallel_agent_test.go` | [§4](v2/orchestration-patterns.md) |
| **LoopAgent**（条件迭代）| `adv_04_loop_agent_test.go` | [§5](v2/orchestration-patterns.md) |
| **RoutingAgent**（Router / LLM Classifier）| `adv_05_routing_agent_test.go` | [§6](v2/orchestration-patterns.md) |
| **RoutingAgent + 决策模型**（Jev / 百炼先判，拿不准交给 LLM；影子档）| `example/decision/routing_demo`（REPL，runnable）| [route-decider.md](v2/route-decider.md) §4 |
| **HandoffAgent**（Agent 间状态机交接）| `adv_06_handoff_agent_test.go` | [§7](v2/orchestration-patterns.md) |
| **PlanAgent**（Plan-Execute）| `example/orchestration/plan_chat`（CLI）/ `example/orchestration/plan_chat_web`（HTTP SSE + HITL）/ `adv_07_plan_agent_test.go` | [§8](v2/orchestration-patterns.md) |
| **ReflectAgent**（Actor-Eval-Reflect）| `adv_08_reflect_agent_test.go` | [§9](v2/orchestration-patterns.md) |
| **GraphAgent**（自定义 Graph 包成 Agent）| `example/graph/v2_graphagent_demo`（runnable）| [§10](v2/orchestration-patterns.md) |

> 多编排无限嵌套 + HITL Resume 闭环：`example/orchestration/nested_smoke`（7 编排嵌套，12 场景）。

## 工具 / MCP

| 功能 | 可跑 example | 文档 |
|---|---|---|
| **FuncTool**（函数包成工具）| `example/orchestration/basic_chat`（get_weather）| [quickstart §2](quickstart.md) |
| **StreamableTool**（流式工具）| — | `tool` 包 godoc（`StreamableTool` 接口）|
| **MCP 集成**（stdio / HTTP Server；按 server 降级、掉线后台重连）| `example/mcp/mcp_demo`（聚合；降级见 `mcp/degrade_test.go`）| [v2/mcp.md](v2/mcp.md)、`mcp` 包 godoc（`Hub` / `StdioClient` / `StreamableHTTPClient`）|
| **Categorizer**（tool span 标 retrieval/embedding/...）| — | [observability-implementation-guide §1](v2/observability-implementation-guide.md) |

## 横切能力

| 功能 | 可跑 example | 文档 |
|---|---|---|
| **多轮会话**（上下文自动继承）| `example/orchestration/basic_chat` | [quickstart §3](quickstart.md) |
| **HITL 中断 + Resume** | `example/orchestration/plan_chat_web`（HTTP + ask_user）/ `nested_smoke` | [hitl-resume.md](v2/hitl-resume.md) |
| **工具调用中间件**（观测 / 短路 / 挂起）| `example/harness/tool_middleware_demo`（mock；含「吞中断会被框架挡住」）| `agent_v2.ToolMiddleware` godoc（5 条契约）|
| **AGENTS.md harness 指令层**（transient prepend）| `example/harness/agents_md_demo`（mock）| `agent_v2.WithAgentsMD` godoc |
| **节点私有中断态**（Resume 不重放已做成的副作用）| `example/harness/node_state_demo`（mock；有 / 无 memo 对照）| `compose_v2.SaveNodeState` godoc |
| **4 层上下文压缩** | —（`WithMicroCompact`/`WithAutoCompact`/... option，见文末速记）| [context-compaction.md](v2/context-compaction.md) |
| **Store 跨 Run 长期记忆** | `example/store/v2_store_demo` / `store_concurrent_smoke`（并发 race）| [store.md](v2/store.md) |
| **Skill 系统**（activate_skill）| `example/graph/v2_skill_demo`（P1 + P2.A/D/E/C+B）| [orchestration-patterns §11](v2/orchestration-patterns.md) |
| **生命周期 Hook**（8 method）| `adv_10_hooks_test.go` | [hooks.md](v2/hooks.md) |
| **流式 SSE 适配** | `example/orchestration/channel_chat_web` | [hooks-and-streaming.md](v2/hooks-and-streaming.md) |
| **可观测**（trace/metric → CS-OSS Monitor）| `example/observability/observability_demo` | [observability-implementation-guide.md](v2/observability-implementation-guide.md) |
| **Checkpoint + Resume**（cp 持久化）| `nested_smoke`（HITL cp 闭环）| [channel-engine.md](v2/channel-engine.md) |
| **Redis backend**（Store + Checkpointer）| `example/store/redis_smoke` | [store.md](v2/store.md) + `store/conformance` |
| **Channel[T] 类型安全**（4 reducer）| `example/graph/v2_typedgraph_demo` | [usage-guide.md](v2/usage-guide.md) |
| **底层 compose_v2 图引擎** | `example/orchestration/channel_chat_demo` / `channel_smoke` | [usage-guide.md](v2/usage-guide.md) + [channel-engine.md](v2/channel-engine.md) |

## 4 层压缩 option 速记

完整设计（算法/阈值/9 段/断路器/时序图/排错树）见 [context-compaction.md](v2/context-compaction.md)。业务侧一站式配置：

```go
agent := agent_v2.NewBasicAgent(
    agent_v2.WithModel(mainModel),
    agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true}),
    agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{
        Enabled:            true,
        Model:              cheapModel,         // 推荐用更便宜 model 做摘要
        ModelContextWindow: 200_000,
    }),
    agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true}),
    agent_v2.WithStore(st),                                      // 第 4 层（Store 后端）
    agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}), // 第 4 层（preamble Search 注入 + postamble Put 保存）
)
```

执行顺序（每轮 model 节点入口）：MicroCompact → AutoCompact → LLM（含 ReactiveCompact retry）→ tools。

## 完整 example 清单（`example/`）

**runnable**（`go run ./example/<dir>`）：`basic_chat` `plan_chat` `plan_chat_web` `channel_chat_demo` `channel_chat_web` `v2_graphagent_demo` `v2_skill_demo` `v2_store_demo` `v2_typedgraph_demo` `observability_demo` `nested_smoke` `channel_smoke` `redis_smoke` `store_concurrent_smoke` `tool_middleware_demo` `agents_md_demo` `node_state_demo` `decision/routing_demo` `decision/client_smoke` `decision/routing_eval`

**test 形式编排示例**（`go test ./example/... -run AdvNN`）：`adv_01`~`adv_08` + `adv_10_hooks`

> 完整 API 速查见 [migration-history.md](v2/migration-history.md)；架构导航见 [architecture.md](description/architecture.md)。
