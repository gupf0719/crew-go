# crew-go Examples

按功能分目录的可运行示例。API Key 从项目根 `.env` 自动读取（默认 `QWEN_API_KEY`，`DEEPSEEK_API_KEY` 备选）。

## 目录

| 目录 | 内容 |
|---|---|
| `orchestration/` | 编排 REPL demo + 嵌套冒烟（basic_chat / handoff_chat / plan_chat(_web) / reflect_chat / routing_chat / channel_* / nested_smoke / parallel_hitl_smoke）|
| `agent/` | 会话历史层：真模型跨 RunSession 完整流 + 后端违约（写失败 / panic / 读失败 / 卡住）不打断主链路 + 高并发多会话不串台 |
| `a2a/` | A2A 跨进程协作（hitl_smoke / llm_smoke / orch_llm_smoke / plan_smoke / rich_server / zk_* ...）|
| `harness/` | Harness 运行时能力（approval / toolsearch / skill_action / structured / durability _smoke；tool_middleware / agents_md / node_state _demo 三个 mock 离线 demo）|
| `store/` | Store 跨 Run 长期记忆（concurrent / custom_backend + redis_smoke + v2_store_demo）|
| `observability/` | 可观测（observability_demo）|
| `fallback/` | 模型故障切换（fallback_smoke）|
| `decision/` | 决策模型（Jev / 百炼）：routing_demo（RoutingAgent 先交给决策模型判，拿不准交给 LLM，REPL）/ client_smoke（真服务端冒烟）/ routing_eval（458 条中文路由评测）|
| `agui/` | AG-UI 前端接入（agui_smoke）|
| `graph/` | 图引擎 demo（v2_graphagent / v2_typedgraph / v2_skill _demo）|
| `mcp/` | MCP Server 集成（mcp_demo：Hub 聚合多 server 工具 → BasicAgent 调）|
| `schedule/` | 定时任务调度（schedule_demo：Every / Cron / Once + handler 跑 agent）|
| `compact/` | 4 层上下文压缩（compact_demo：触发 AutoCompact + OrchHooks 观测 compaction event）|
| `prompt/` | 提示词模板（prompt_demo：Template 变量绑定 / 占位 + WithPromptTemplate 动态 system prompt）|
| `tutorial/` | 9 编排教学集成测试（adv_*，真 LLM，需 `-tags=integration`）|
| `envloader/` `scenarios/` `llmtrace/` | 共享 helper（被多个 demo import，故留根）|

## 常用命令

**交互式 REPL**（输入 `/exit` 或 Ctrl-D 退出）：

```bash
go run ./example/orchestration/basic_chat     # BasicAgent + 天气工具（跨轮记住城市）
go run ./example/orchestration/plan_chat      # PlanAgent researcher/writer/critic 专家分派
go run ./example/orchestration/handoff_chat   # HandoffAgent 跨 handoff 上下文
go run ./example/orchestration/reflect_chat   # ReflectAgent 用户反馈迭代
go run ./example/orchestration/routing_chat   # RoutingAgent 跨轮路由
go run ./example/decision/routing_demo -provider jev   # 决策模型路由：打印每句话由哪一档决定、置信度与各分支概率（-provider bailian -workspace llm-xxxx 换百炼，-shadow 影子档）
```

**冒烟 smoke**（自包含验证）：

```bash
go run ./example/orchestration/nested_smoke   # 7 编排嵌套 HITL Resume 闭环
go run ./example/a2a/hitl_smoke               # A2A Task 状态机 + HITL 闭环
go run ./example/harness/structured_smoke     # 结构化输出（真 deepseek）
go run ./example/fallback/fallback_smoke      # 模型故障切换
```

**单能力 demo**（mock，无需 key，直接跑）：

```bash
go run ./example/prompt/prompt_demo         # 提示词模板（变量 / 占位 / 动态 system prompt）
go run ./example/mcp/mcp_demo               # MCP Server 集成（Hub 聚合 → agent 调）
go run ./example/schedule/schedule_demo     # 定时任务（Every / Cron / Once，~6s）
go run ./example/compact/compact_demo       # 上下文压缩（AutoCompact 触发 + OrchHooks 观测）
go run ./example/harness/tool_middleware_demo  # 工具调用中间件（观测 / 短路 / 吞中断被框架挡住 / 框架工具不过链）
go run ./example/harness/agents_md_demo        # AGENTS.md harness 指令层（分层 / 多文件 / transient 不进历史）
go run ./example/harness/node_state_demo       # 节点私有中断态（SaveNodeState：Resume 不重放已做成的副作用）
```

**教学测试**（真 LLM，需 `-tags=integration`）：

```bash
go test -tags=integration ./example/tutorial/ -v -run TestAdv   # 9 编排逐个跑
```

完整冒烟清单见 [smoke-test-recipes](../doc/description/smoke-test-recipes.md)，SDK 代码示例索引见 [sdk-guide](../doc/sdk-guide.md)。
