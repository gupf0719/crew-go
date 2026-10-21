# ISSUE-91 · 外部/异构 agent 接入编排 —— AgentFunc 轻量 adapter

**优先级**：中（DX 改进——业务集成外部 agent 时降样板代码）
**状态**：📋 待实施（2026-06-01 讨论记录）
**触发**：用户问"已做好的 agent 想加入编排，业内怎么做，这个要适配的吧"

## 背景

业务已有的 agent —— 别的框架（LangChain / eino）写的、远程 HTTP/gRPC 服务、或黑盒 ——
想接入 crew-go 编排（Sequential / Parallel / Plan 等），当前必须**手写 struct 实现
`agent_v2.Agent` 全部 4 方法**（Run / Resume / Name / Description）+ 事件转换 + Resume
重跑逻辑，样板代码不少。

## 现状 gap（grep 确认）

crew-go 现有适配入口都不直接覆盖"外部 agent → 编排单元"：

- `GraphAgent`（`agent_v2/graph_agent.go` 包 `compose_v2.Graph` 为 Agent）—— 要业务自己建图，重
- `AgentAsTool` / `RunnableAsTool`（`agent_v2/agent_tool.go`）—— 包成 **tool** 给别的 agent 的
  LLM 调用，**不是编排单元 Agent**（不能直接当 Sequential step / Parallel branch）
- **缺**：把"一个外部调用函数"一行包成 `Agent` 的轻量 adapter

## 三方对照

| 框架 | func → 编排单元 的一行 adapter |
|---|---|
| LangGraph | `RunnableLambda(fn)` |
| eino | `compose.Lambda` |
| crew-go | **无**（要手写整个 struct）← 本 issue |

## 提议

加 `agent_v2.AgentFunc(name, description string, fn func(ctx context.Context, input string) (string, error)) Agent`：

- 自动包装 fn 输出 → `[]*AgentEvent`（OutputEvent + ExitEvent）
- Name / Description 直接返回（Description 让 LLMPlanner / AgentAsTool 看到能力）
- Resume = 重跑 fn（幂等性业务负责，见难点 #1）
- typed-nil func 防护（参考 `RunnableAsTool` audit⁵ HIGH-1 同款 `reflect.IsNil`）

## 适配难点（设计时必须想清楚，不是机械包装）

1. **Resume / HITL（最难）**：外部黑盒 agent 无"中断-恢复"概念。crew-go cp 是 **graph 级**
   （存 channel state + frontier，**不存节点内部状态**），所以外部 agent 作为编排里一个节点：
   - Run **成功** → 输出进 channel → Resume **不重跑**（cp 恢复输出）✓
   - Run **失败 / ctx 取消** → Resume **重跑整个 fn**（**幂等性业务负责**，godoc 必须醒目警示）
   - 外部 agent **不能主动暂停 HITL**（不会抛 `InterruptError`）—— 除非 adapter 显式桥接外部
     agent 自己的 HITL 状态（超出本轻量 adapter 范围，业务自写 struct）
   - 行为与 LangGraph 一致（node 黑盒，resume 重跑 node）
2. **流式**：`fn` 同步返回 string，无流式。要流式 → 单独 `StreamAgentFunc`，或 fn 内走
   `compose_v2.GetEventSink(ctx)` 发 ChunkEvent
3. **错误语义**：fn 返 error 如何映射（业务错 vs ctx.Err vs 想触发中断）——需明确约定

## 关联

- [`doc/description/three-way-comparison/agent-interface.md`](../description/three-way-comparison/agent-interface.md)
  —— 现只讲 **crew-go 原生 agent 嵌套**；**本 issue 落地时补"异构/外部 agent 接入"这一层**
- `agent_v2/agent_tool.go::RunnableAsTool` —— 同款 func 包装但目标是 tool，可参考其 typed-nil 防护

## 重审时机

业务真正要集成外部 / 远程 agent 时实施（YAGNI —— 当前可手写 struct 实现 Agent 接口绕过，
有真实诉求再加这层糖）。
