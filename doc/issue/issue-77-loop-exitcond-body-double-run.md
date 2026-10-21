# ISSUE-77 · LoopAgent ExitCondition 抛 InterruptError 时 body sub-agent 重复跑

**优先级**：中（HITL 副作用 + 业务幂等假设违反）
**状态**：✅ 已修（2026-05-15）— 方案 A exitCheck 节点化落地
**发现轮次**：R29 audit 视角 5 MED-1（2026-05-14）

## 问题

`compose_v2/loop_agent.go::bodyFn` (line 152-186) 当前节点结构：
```go
out, err := b.body(ctx, actualInput, round)  // sub-agent 跑 + 写 OutputEvent + cleanup 子 cp
if err != nil { return nil, err }
// ... shouldExit 计算
shouldExit := false
if exitCondition != nil {
    shouldExit, err = exitCondition(ctx, out, round)
    if err != nil { return nil, err }  // ← 抛 InterruptError 时 bodyFn return
}
```

**Bug**：当 `b.body` 成功完成（sub-agent.Run 完成 + 写 OutputEvent/ExitEvent 到 parentSession + sub cp cleanupOnSuccess 删除）但 `exitCondition` 抛 InterruptError → bodyFn return err → 整 bodyFn 失败 → 框架 SaveNS cp + propagate。

Resume 时 bodyFn 重跑 → `b.body` 又被调一次：
- sub-agent.Run 拿不到子 cp（已被 cleanupOnSuccess 删）→ fresh run
- 重新调 LLM + 重新写 OutputEvent/ExitEvent 到 parentSession + 重做业务工具调用副作用

业务场景：
- HITL gate "exit loop after round N? approve continue?"——R26 audit (2) 加的 ExitCondition error 返回功能 godoc 承诺的"循环退出前用户审批"模式恰是触发场景
- 用户期望：Resume 后只续 exitCondition 决定，不重跑已完成的 b.body
- 实际：b.body 副作用重做（重 LLM 调用费、重 tool call、重 OutputEvent 写 sess.events）

## 修法（任选一）

### 方案 A：把 exitCondition 提到独立节点（推荐但架构改）

拓扑改为：`StartNode → body → exitCheck → ConditionalEdge → body / EndNode`

- body 节点：跑 sub-agent → 写 lastOut channel
- exitCheck 节点：读 lastOut → 调 exitCondition → 写 exit decision channel
- exitCondition 抛 Interrupt 时只重跑 exitCheck，body 不重跑

**优点**：架构清晰，符合"节点幂等性"原则
**缺点**：拓扑复杂化，要保证 lastOut channel 在 Resume 时正确恢复让 exitCheck 节点能读

### 方案 B：bodyFn 入口 short-circuit（侵入小但破协议）

bodyFn 入口检查 `lastOut channel` 是否已含本轮输出 → 跳 b.body 重跑：
```go
lastOut, _ := Get(chs, loopAgentBodyOutCh)
if lastOut != nil && roundOfLastOut == round {
    out = lastOut  // 跳 b.body 重跑
} else {
    out, err = b.body(...)
}
```

**优点**：节点拓扑不变
**缺点**：需要"roundOfLastOut" 跟踪 channel + 与 retry 机制互动复杂

## 当前缓解

无——业务需自负 b.body 重跑成本：
- LLM tool calls 业务工具自带幂等性（typically OK）
- OutputEvent 重写到 sess.events 会让审计链有重复事件（SupersedeEventsByMarker 不能 catch 这种）
- 多算 1 次 LLM token cost

## 关联

- R26 audit (2) commit `6d1c727`: 给 LoopAgentExitCondition / EvalFunc / ExecInputBuilder 加 error 返回 — 启用本 ISSUE 触发条件
- ISSUE-75 commitFinal HITL cp gap: 类似 "graph 节点之外的回调抛 Interrupt" 模式
- R28 audit MED-1: LoopAgent body sub-agent 跨轮 round_<N> ns 隔离已修（与本 ISSUE 互不重叠）

## 决策

R29 audit 暂不修。优先级中——HITL gate 在 ExitCondition 是 R26 加的功能（业务实际用例较窄）；当前重跑成本对业务可接受（业务工具应已幂等设计）。

长期修法推荐方案 A（节点拓扑改），与 ISSUE-75 同样需要重构。可考虑两个 ISSUE 一起规划。
