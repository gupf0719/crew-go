# crew-go

Go 原生 LLM Agent 编排框架。用最少的抽象把「单个 Agent」和「多 Agent 协作」统一在 channel-based BSP 图执行引擎里。

## 特性

- **统一执行引擎**：所有编排模式内部编译为同一 channel-based `Graph` 执行（`compose_v2.GraphBuilder`），Checkpointing / OrchestrationHooks / InterruptError / 4 层上下文压缩等横切能力一次实现、全模式受益
- **无限嵌套**：所有 9 编排都实现 `agent_v2.Agent` 单接口，可任意组合嵌套，HITL Resume 全闭环
- **Channel-based BSP**：`NodeFunc func(ctx, Channels) ([]ChannelWrite, error)` + 4 reducer（LastValue / Append / Sum / OnceFlag）+ 类型安全 `Channel[T]`
- **9 种编排**：覆盖 ReAct / 线性 / 并行 / 循环 / 路由 / 状态机 / 规划 / 反思 / 自定义图
- **MCP 集成**：`mcp.Hub` 接入任意 MCP Server（stdio 子进程 / streamable HTTP），工具自动聚合进 `tool.Toolkit`
- **A2A 协议互通**：消费远程 agent / 暴露本地 agent 为 server / 多语言服务注册发现（含 ZooKeeper binding）—— 详见 [A2A 跨进程协作](doc/v2/a2a.md)
- **AG-UI 前端接入**：agent 事件流适配 AG-UI 标准协议经 SSE 推前端，直接接 CopilotKit 等生态前端组件（React / Vue / Angular）+ HITL Resume 闭环 —— 详见 [AG-UI 前端接入](doc/v2/agui.md)
- **决策模型路由**：RoutingAgent 先交给决策模型判，拿得准直接路由，拿不准再升级给 LLM classifier 或兜底分支；决策模型可选 TypeSafe 的 Jev 或阿里云百炼的 decision-model-preview —— 详见 [带置信度的路由决策](doc/v2/route-decider.md)
- **执行引擎横切**：cp + Resume（NS 物理分离 + ThreadID 跨 run）+ durability（崩溃恢复 cp 写时机）+ 软取消（超步边界优雅停、存档可 Resume）、流式 SSE 适配、OpenTelemetry 可观测
- **Harness 运行时层**：4 层上下文压缩、Store 跨 Run 长期记忆、Skill（fork / inline + action 执行）、工具级渐进披露（toolsearch）、远端沙箱执行（sandbox，可插拔 seam + 输出安全框定）、AGENTS.md 指令注入、工具审批权限——统一见 [harness 运行时层](doc/v2/harness.md)
- **定时任务调度**：`schedule` 独立叶子包（crontab 心智模型：cron 表达式 / 固定间隔 / 一次性 + handler），与 agent 解耦——handler 里跑 agent / 算数据 / 调接口都行

## 编排模式

| 模式 | 适用场景 |
|---|---|
| `BasicAgent` | 单 LLM + 工具调用，ReAct 循环 |
| `SequentialAgent` | 多专家依次处理，前一输出作后一输入 |
| `ParallelAgent` | 同一输入多专家并发处理，结果聚合 |
| `LoopAgent` | 重复执行直到满足退出条件 |
| `RoutingAgent` | Router 函数，或「带置信度的决策方 → LLM Classifier → 兜底」升级链 |
| `HandoffAgent` | 对话控制权在专家间转移——LLM 调内置 `transfer_to_agent` 工具自主转交（return-direct），active agent 跨 Run 持久（store.Store）|
| `PlanAgent` | 先规划步骤再逐步执行（replan-每步 + sub-agents / TmpSubAgent / executor 三档执行器）|
| `ReflectAgent` | Actor 生成 → Evaluator 评估 → ReflectorAgent / EvalFunc 改进 |
| `GraphAgent` | 包装任意 `compose_v2.Graph` 为 Agent（caller 提供 InputWriter / OutputReader）|

## 包结构

```
crew-go/
├── schema/        消息与流类型（Message、StreamReader）
├── model/         LLM 接口（ChatModel）与 OpenAI 兼容实现（openai / qwen / deepseek 适配）
├── tool/          工具接口（Tool、StreamableTool）与注册表
├── hook/          生命周期钩子（Hooks、ChainHooks）
├── prompt/        轻量提示词模板（变量绑定 + 消息列表占位 → []*schema.Message）
├── skill/         Skill 能力包子系统（SkillBox、DefaultSkillBox；选择 / body 注入 / fork / action 执行）
├── mcp/           MCP 客户端（StdioClient、StreamableHTTPClient）与工具聚合（Hub）
├── store/         跨 Run 长期 KV 持久化（Store 接口 + InMemoryStore，业务可自实现后端）
├── sandbox/       远程命令执行沙箱可插拔 seam（skill 脚本 / 命令下沉远端隔离，包成 sandbox_bash 工具）
├── compose_v2/    Channel-based BSP 图引擎、Checkpointer、Resume、OrchHooks、Retry
├── agent_v2/      9 种 Agent 实现 + Store 长期记忆 + 4 层压缩 + Skill 集成 + LLMPlanner
├── fallback/      模型故障切换装饰器（FallbackChatModel：有序候选 + 断路器 + 重试，注入 WithModel 对 9 编排透明）
├── schedule/      通用定时任务调度（crontab：cron 表达式 / 固定间隔 / 一次性 + handler，与 agent 解耦的独立叶子包）
├── a2a/           A2A 协议：消费远程 agent（RemoteAgent）/ 暴露 server / 服务发现（registry）/ ZK binding（独立 module）
├── agui/          AG-UI 前端协议适配（agent 事件流 → SSE 推前端，接 CopilotKit 等生态）
├── decision/      决策模型客户端与路由决策方（子包 jev / bailian 分别接 TypeSafe 的 Jev 与阿里云百炼）
├── observability/ OpenTelemetry trace + GenAI 语义约定指标（obs.Install 一次接上两条钩子链）
├── redis/         Store + Checkpointer 的 Redis 后端实现（独立 go module）
└── sql/           Store + Checkpointer 的 SQL（MySQL/PG）后端实现（独立 go module）
```

## 依赖方向

```
schema ← model / tool / hook / skill / mcp / prompt / store ← compose_v2 ← agent_v2 ← a2a / agui / decision
```

（右依赖左）`compose_v2` 是底层 channel-based 图运行时，`agent_v2` 在其上构建。`a2a`（跨进程协议接入）/ `agui`（前端接入）/ `decision`（决策模型接入）和 `observability`（横切观测）建在 `agent_v2` 之上；`sandbox`（tool 层沙箱 seam）/ `fallback`（model 装饰器）/ `schedule`（定时任务）是框架核心不依赖、业务按需引入的可选能力包；`redis` / `sql` / `a2a/zk` 是隔离外部依赖的独立 go module——完整依赖图（含 a2a 各子包）见 [架构总览](doc/description/architecture.md)。

## 文档

**入门**（新用户从这开始）
- [快速上手 quickstart](doc/quickstart.md)（**5 分钟跑通第一个 Agent** — 安装 / 工具 / 多轮）
- [SDK 代码示例索引 sdk-guide](doc/sdk-guide.md)（找「X 功能怎么写」→ example 真实代码 + 文档）
- [编排模式深入用法 orchestration-patterns](doc/v2/orchestration-patterns.md)（9 编排选型决策树 / 每编排完整最小例 + 关键 option + 反模式 / 嵌套组合 / 横切能力）

**功能用法**
- [Harness 运行时层总览 harness](doc/v2/harness.md)（4 层压缩 / Store / Skill / toolsearch / sandbox / AgentsMD / approval 如何组成 harness + 组合接法）
- [HITL + Resume 完整协议 hitl-resume](doc/v2/hitl-resume.md)（InterruptError / AskQuery / InterruptID 稳定算法 / ResumeAction / 多 tool_call 部分中断 / fan-out 并发中断 / mid-stream / plan_chat_web 实战）
- [4 层上下文压缩 context-compaction](doc/v2/context-compaction.md)（算法 / 阈值 / 9 段 / 附件重注入 / 断路器 / 时序图 / 排错决策树）
- [Store 跨 Run 长期记忆 store](doc/v2/store.md)（Store 接口 / Namespace / InMemoryStore + 内置 redis/sql 后端 + 自实现后端 conformance）
- [Handoff 对话控制权转移 handoff](doc/v2/handoff.md)（transfer 工具 / 模型 B / 历史可见性模式 / 跨 Run 持久 active）
- [Skill 可复用能力包 skill](doc/v2/skill.md)（定义 / 激活 / body / fork / **action 执行** / listing / 压缩协作）
- [远端沙箱接入 sandbox](doc/v2/sandbox.md)（Sandbox seam / sandbox_bash / 输出安全框定 / 接入自己的后端）
- [工具级渐进披露 toolsearch](doc/v2/toolsearch.md)（deferred 工具按需揭示 + 可插拔检索 + fail-closed；工具多 / MCP 聚合爆 context 时用）
- [A2A 跨进程协作 a2a](doc/v2/a2a.md)（消费远程 / 暴露 server / 服务发现 + ZooKeeper binding）
- [AG-UI 前端接入 agui](doc/v2/agui.md)（agent 事件流→AG-UI 标准协议 SSE / 接 CopilotKit 生态前端 / HITL Resume 闭环 / StateSnapshot opt-in）
- [带置信度的路由决策 route-decider](doc/v2/route-decider.md)（RouteDecider 升级链 / Jev 与百炼两家决策模型接入 / 门槛怎么定 / 中文路由实测）
- [生命周期 hook hooks](doc/v2/hooks.md) + [流式 SSE hooks-and-streaming](doc/v2/hooks-and-streaming.md)
- [可观测 observability](doc/v2/observability.md)（OpenTelemetry：span 树 / GenAI 语义约定属性与指标 / A2A 跨进程传播）
- [v2 业务用法 usage-guide](doc/v2/usage-guide.md)（compose_v2 底层图引擎使用）

**设计 / 架构**
- [架构总览（导航）](doc/description/architecture.md)
- [v2 完整 API 速查 + 演化历史 migration-history](doc/v2/migration-history.md)（**新 session 优先读**）
- [为什么转 channel-based why-channel-based](doc/v2/why-channel-based.md)（直接 trigger / 5 大根本设计问题 / PoC 决策 / 收益对比）
- [v2 设计原理 channel-based-design](doc/v2/channel-based-design.md)（PoC 期设计稿 / 5 大难点 / API 草案）
- [Channel-based Graph 引擎详细设计 channel-engine](doc/v2/channel-engine.md)（核心抽象 / graphLoop / cp + Resume / fan-out / 嵌套 / 9 编排编译产物 / 关键不变量速查）

**测试**
- [冒烟测试速查 smoke-test-recipes](doc/description/smoke-test-recipes.md)
- [测试规约 testing-conventions](doc/description/testing-conventions.md)

## 环境要求

- Go 1.26+（`go.mod` 声明 `go 1.26.0`；依赖的 a2a-go v2.7.0 要求）
- A2A ZooKeeper binding（`a2a/zk`）/ Redis（`redis`）/ SQL（`sql`）是独立 go module，按需引入
