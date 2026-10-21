# ISSUE-83 · PlanAgent 不接业务级 hook.Hooks（架构对称性 gap）

**优先级**：LOW（架构对称性 gap，不影响主流程）
**状态**：✅ 已修（2026-05-15）— PlanAgent.Config 加 `hooks hook.Hooks` 字段 + `WithPlanHooks(h hook.Hooks) PlanOption` option + Run/Resume defer 直接调 `p.hooks.OnAgentEnd(ctx, info, ...)` 兜底（学 ISSUE-82 BasicAgent 单一所有权 + 与其他 8 编排架构对称; audit² 改: `SafeOnAgentEnd` helper 已删, framework Run 入口 outer defer recover 兜底跟 eino 对齐）
**触发**：ISSUE-82 audit 时 grep `OnAgentEnd` 在
agent_v2/plan_agent.go 全文为空——PlanAgent 是 9 编排里**唯一**不接业务级
hook.Hooks 的 agent

## 问题

agent_v2/plan_agent.go::Run + Resume 全文搜不到 `hooks.OnAgentEnd` defer 调用——业务
通过 `WithPlanModel` / `WithPlanToolkit` 等配置 PlanAgent 时无法配业务级
`hook.Hooks` 实例（如 `WithHooks` 在 PlanAgent 不存在）。

业务想观测 Plan 整体生命周期（OnAgentStart / OnAgentEnd; ISSUE-82 后 hook.Hooks 9→8 删 OnAgentInit, OnAgentStart 整 Run 1 次 fire 取代之）需要：
1. 在每个 sub-agent 上单独配 hooks（粒度太细，无法看 Plan 整体）
2. 自包 wrapper agent 在外层 defer fire OnAgentEnd（违反"PlanAgent 是 Agent
   接口"的对称性）

## 对照 9 编排架构

| 编排 | 接业务 hook.Hooks | OnAgentEnd defer fire |
|---|---|---|
| BasicAgent | ✅ WithHooks | ✅ Run/Resume defer |
| SequentialAgent | ✅ WithSequentialHooks | ✅ Run defer |
| ParallelAgent | ✅ WithParallelHooks | ✅ Run defer |
| LoopAgent | ✅ WithLoopHooks | ✅ Run defer |
| RoutingAgent | ✅ WithRoutingHooks | ✅ Run defer |
| ReflectAgent | ✅ WithReflectHooks | ✅ Run defer |
| HandoffAgent | ✅ WithHandoffHooks | ✅ Run defer |
| GraphAgent | ✅ WithGraphAgentHooks | ✅ Run defer |
| **PlanAgent** | **❌ 缺** | **❌ 缺** |

## 影响

- 业务监控 / SLO / 计费按"agent 整体生命周期"统计时 PlanAgent 漏数据
- 多 agent 系统统一 hook 观测时 PlanAgent 是孤岛，业务需特判
- 嵌套 Plan 调 BasicAgent 时父 Plan 无 OnAgentEnd 信号，业务难以确定 Plan
  整体何时结束

## 修法（待实施）

1. PlanAgent.Config 加 `Hooks hook.Hooks` 字段
2. 加 `WithPlanHooks(h hook.Hooks) PlanOption` option
3. PlanAgent.Run / Resume 入口 defer 直接调 `p.hooks.OnAgentEnd(ctx, info,
   &hook.AgentOutput{Messages: finalMsgs})` 兜底 (audit² 删 `SafeOnAgentEnd` wrapper
   跟 eino 对齐 — framework Run 入口 outer defer recover 兜底 panic)
4. 协议对齐 ISSUE-82 fix（单一所有权 Run/Resume defer 唯一源）

## 跟踪

ISSUE-82 主 fix 范围聚焦 BasicAgent 双触发，本 finding 是 audit 衍生发现的架构
对称性问题——非阻塞 ISSUE-82 落地，单独跟踪。
