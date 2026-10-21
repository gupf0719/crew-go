# ISSUE-78 · AgentAsTool childNS 缺 tc.ID 维度（fan-out 化时 cp 互覆）

**优先级**：低（当前 toolsNode 串行执行 + 早 return 兜底未触发，未来 fan-out 化时变 HIGH）
**状态**：✅ 已修（2026-05-15）— 预防性修法落地：ctx 加 toolCallID 协议 + AgentAsTool.Call 用 hash → JoinFanoutNS
**发现轮次**：R31 audit 视角 B HIGH-1（2026-05-15，agent self-downgrade 至 MED）

## 问题

`agent_v2/agent_tool.go:51-53` AgentAsTool.Call 的 childNS 派生：

```go
parentNS := compose_v2.GetGraphNS(ctx)
childNS := compose_v2.JoinNS(parentNS, t.agent.Name())
subCtx := compose_v2.WithGraphNS(ctx, childNS)
```

childNS 仅包含 `t.agent.Name()`——**没有 tc.ID（tool_call ID）维度**。

## 当前为什么不出 bug

`compose_v2/basic_agent.go::toolsFn` (line 962-967) 当前**串行执行**多个
tool_call（同步 for 循环），且任一抛 InterruptError 立即 return（line 339）：

- 同 ReAct 轮 LLM emit `[ask_researcher("查 A"), ask_researcher("查 B")]`
- tc1 跑完成功 → graph_loop defer cleanupOnSuccess → 删 `(parentSID, parent/ask_researcher)` cp
- tc2 紧接用同 childNS fresh Run → cp 干净不串台
- tc1 抛 Interrupt → SaveNS(parentSID, "parent/ask_researcher") + return → tc2 根本不跑 → 无串台

**总结**：当前架构兜底，未实际触发。

## 未来何时会真 bug

若 `toolsFn` 升级支持 fan-out 并发执行多 tool_call（compose_v2 已有 fan-out 能力但
BasicAgent 当前没启用——见 ISSUE-72 备选方向 / 性能优化讨论）：

- 同 ReAct 轮 N 个 tool_call 并发 fire
- 多个 AgentAsTool.Call 同时跑 → 共用同 childNS（agent.Name() 相同时）
- N 个并发 SaveNS 同 (parentSID, parent/ask_researcher) → 最后写赢 cp 互覆
- HITL：N 个并发抛 Interrupt → cp 只剩最后一个 → Resume 答 N-1 个 query 永久丢失

## 修法（待 fan-out 落地时实施）

```go
// 拿 toolsNode 透传的 tc.ID（需在 ctx 注入 GetCurrentToolCallID）
tcID := compose_v2.GetCurrentToolCallID(ctx)  // 待实现
childNS := compose_v2.JoinFanoutNS(parentNS, t.agent.Name(), branchIdx)
// 或 childNS := compose_v2.JoinNS(parentNS, t.agent.Name() + "#tc:" + tcID)
```

需要：
1. compose_v2 toolsFn fan-out 化时给每个 tool_call 一个 branchIdx
2. 通过 ctx 暴露 tc.ID / branchIdx 给 AgentAsTool.Call
3. AgentAsTool.Call 用 fanout-aware childNS

## 当前防护

无——但 toolsFn 的 fan-out 化需要专门修 ctx 协议时一并加防护，文档化 AgentAsTool
是"串行调用安全 / fan-out 待审"。

## 关联 ISSUE

- ISSUE-72: BasicAgent fanout / 性能优化讨论
- ISSUE-79: AgentAsTool 子 sess 不可续传（同审计角度）
