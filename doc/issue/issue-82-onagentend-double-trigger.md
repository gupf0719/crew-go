# ISSUE-82 · OnAgentEnd hook 双触发（pre-existing bug）

**优先级**：MED（业务可观测语义错乱，业务计费/SLO/审计逻辑依赖触发次数会出错）
**状态**：✅ 已修（2026-05-15）— 方案 A 落地：删 ConditionalEdge cond fn 内 fire OnAgentEnd，agent_v2.BasicAgent.Run defer 唯一源兜底（学 eino flowAgent + ChatModelAgent 单一所有权）
**触发**：R34 ISSUE-75 commit `9886a8a` audit 时 视角 D 发现，但被识别为
pre-existing bug 跟踪到独立 ISSUE 而非阻塞 R34

## 问题

`OnAgentEnd` hook 在 BasicAgent 一次 Run 结束时触发**两次**：

```
agent_v2/basic_agent.go:139 (Run defer 路径)
  + compose_v2/basic_agent.go:478 (graph ConditionalEdge model→terminal 末段)
```

具体路径：
1. BasicAgent ReAct 循环 model 节点产 final message (无 tool_calls)
2. ConditionalEdge cond fn 检测无 tool_calls → 调 `safeOnAgentEnd(ctx, hooks, info, out)`
   → return terminalTarget → graph 退出 → 业务 OnAgentEnd 触发**第 1 次**
3. agent_v2 BasicAgent.Run 函数 defer 块跑 → 直接调 `hooks.OnAgentEnd(ctx, ...)` (audit²
   删 `SafeOnAgentEnd` wrapper 跟 eino 对齐 — framework outer recover 兜底 panic)
   → 业务 OnAgentEnd 触发**第 2 次**

> ⚠️ **行号 stale**: 原 bug 描述含 `basic_agent.go:139` / `compose_v2/basic_agent.go:478` 等 v1 行号定位, 文件 churn 后已 stale. 业务读 reproduction 时按 `git show 9886a8a / b07c140` 还原 fix-pre 行号; 业务读修复方案看 line 4 状态行即可.

## 影响

**业务可观测语义错乱**：
- 业务 OnAgentEnd 内做计费 / SLO 上报 / 审计日志的逻辑会**算 2 次**
- 业务 hook panic 触发 2 次会让日志双倍噪声

**生产风险评估**：
- 业务可在 hook 内自管 sync.Once 去重（workaround）
- OnAgentEnd 是观测性 hook，不应改变 framework 行为——双触发不影响主流程
- **MED 级而非 HIGH**：framework 行为正确，仅 hook 触发次数错

## 历史回溯

| Round | 何时引入 | 原因 |
|---|---|---|
| ISSUE-74 (preamble 节点化) | `b07c140` | model + tools 两节点 → preamble + model + tools 三节点；ConditionalEdge OnAgentEnd 触发不变 |
| ISSUE-75 (postamble 节点化) | `9886a8a` | 加 postamble 节点跑 commitFinal + Memory.Write（PR 3.3 后改名 `store.Put`）；OnAgentEnd 仍在 ConditionalEdge model 段触发（postamble 之前） |
| 当前 | — | 两处触发并存：ConditionalEdge model 段 + Run defer 兜底 |

实际上 Run defer 是历史代码（早于 graph-based BasicAgent 时期），graph 化后
ConditionalEdge 加了 OnAgentEnd 触发但没删 defer 路径——**两处协议未统一**。

## 修法（待评估）

**方案 A（推荐）**：删 ConditionalEdge model 段 OnAgentEnd 触发，统一走 Run defer
- 优点：defer 是 Go 习惯写法，覆盖所有退出路径（含 InterruptError / panic）
- 缺点：postamble 节点跑后 OnAgentEnd 触发会延迟到 graph 完全退出 — 但这本来
  就该是设计意图（"agent 整体完成才触发"），无副作用

**方案 B**：删 Run defer 兜底，仅 ConditionalEdge 触发
- 缺点：InterruptError 抛出时 ConditionalEdge 不跑（model 节点 return error，
  cond fn 不被调用）→ HITL 中断时 OnAgentEnd 不触发 → 业务监控漏 HITL 退出场景

**方案 C**：加去重（不推荐）
- 在某 ctx 维度记 OnAgentEnd 已触发，第二次跳过
- 业务自管 sync.Once 等价方案——framework 不该承担

**结论**：方案 A 最干净。需测：
- 正常退出（model final）→ OnAgentEnd 触发 1 次（不是 2 次）
- HITL InterruptError 退出 → OnAgentEnd 触发 1 次（defer 路径）
- panic 退出 → OnAgentEnd 触发 1 次（defer 路径）
- ctx.Err() / 其他业务错 → OnAgentEnd 触发 1 次（defer 路径）

## 关联

- ISSUE-74: BasicAgent preamble 节点化
- ISSUE-75: BasicAgent postamble 节点化
- ISSUE-81: BasicAgent toolsFn fanout 化（同期改动 — 但不引入此 bug，bug 早于
  ISSUE-81）

## 跨文档同病

待修：业务向 OnAgentEnd godoc 应明示"框架可能调多次，业务自管去重"——但理想
fix 是修 framework 让协议清晰。
