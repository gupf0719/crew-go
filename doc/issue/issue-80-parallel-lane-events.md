# ISSUE-80 · ParallelAgent fan-out lane events 物理隔离（彻底修 R31 串台根因）

**优先级**：高（业务 ParallelAgent 是核心编排，R31 name filter 是临时缓解）
**状态**：✅ 已修（2026-05-15）— eino adk lane events 模型 port 落地
**发现轮次**：R32 audit 视角 B HIGH-1 → LangGraph + eino 调研后确认 eino 模型最优

## 问题（R31 视角 D HIGH-1 揭露的根因）

`agent_v2/parallel_agent.go::buildComposeGraph` 之前 branchFn 写法：

```go
events, err := br.Agent.Run(ctx, parentSession, input)  // 共享 parentSession
final := FinalMessage(events)  // ← 反向扫到 sibling Exit 串台
```

N branch 并发跑 → 共享 `parentSession.events` → sub-agent.Run() 返回 events 切片含
sibling 事件 → FinalMessage 反向扫拿到 sibling 的 final → branch A 的 merger 收到
branch B 的 final 内容。

**R31 临时缓解**：用 `FinalMessageByAgent(events, br.Agent.Name())` 按 sub-agent 名
filter——要求业务用 distinct sub-agent name（限制业务用同 sub-agent 实例作多 branch）。

## 三方调研结果

调研 LangGraph + eino adk 同问题怎么处理：

| 方案 | 模式 | 适配 crew-go |
|---|---|---|
| **LangGraph** | Send + per-task writes deque + reducer 合并；无 final message 概念 | 不适合——要求所有 state 字段定义 reducer，与 crew-go channel 引擎正交 |
| **eino adk** | fork ctx + per-lane events slice (lock-free) + join 时按 TS 排序合并回父 | **完美匹配**——保留 crew-go NS cp / 5 维 InterruptID / values mu 等已有优势 |

eino 实现参考：`/Users/gupengfei/program/go/src/eino/adk/runctx.go:31-353`。

## 修法（eino lane events 模型 port）

### `agent_v2/session.go`

新加 `laneEvents` struct + `RunSession.laneEvents` 字段:

```go
type laneEvents struct {
    Events []*AgentEvent  // branch 私有 events slice (lock-free)
    Parent *laneEvents    // 嵌套 fan-out 时父 lane 链
}
```

新公开 API:
- `(*RunSession).ForkLane()` 创建 lane child sess（共享 values + 独立 lane events slice）
- `CommitLaneEvents(parent, child)` 合并 lane events 回父
- `CommitLaneEventsBatch(parent, children...)` 多 child 按 TS 全局排序合并

`AddEvent` 路由：lane 内 → append 自己的 lane.Events（lock-free）；主线 → events + eventsMu。

`Events()` 拼回 committed + lane 链让业务 sess.Events() 在 lane 内仍能看到完整视图。

### `agent_v2/parallel_agent.go::branchFn`

```go
laneSess := parentSession.ForkLane()
result, runErr := compose_v2.RunWithRetry(subCtx, retryCfg, func(ctx) {
    events, err := br.Agent.Run(ctx, laneSess, input)  // ← lane 隔离
    final := FinalMessage(events)  // ← 不再 name filter
    return final, nil
})
CommitLaneEvents(parentSession, laneSess)  // ← branch 完成时合并回父
return result, runErr
```

## 收益

| 项 | R31 现状（name filter） | ISSUE-80 修后（lane events） |
|---|---|---|
| branch events 隔离 | 共享 + name filter | **物理隔离** |
| 同 sub-agent 实例作多 branch | ❌ 不允许 | ✅ 原生支持 |
| 业务侧可见性（看每 branch 全过程）| ✅（共享 events 直接看）| ✅（join 后父见全部，时序正确）|
| race-free | ✅（R32 valuesMu）| ✅ |
| 嵌套 cp / HITL ID / 重名防御 | ✅ | ✅ |
| 嵌套 fan-out (Parallel 内嵌 Parallel)| ⚠ 多层 name filter 仍可能串台 | ✅ lane.Parent 链自动建立 |

## 测试

`agent_v2/r32_issue79_audit_test.go::TestISSUE80_SameSubAgentInstanceInMultiBranches`:
- 2 branch 用**同 Name="shared_sub"** 的 sub-agent 实例
- barrier 强制并发让 sub-agent.Run() 返回时 sibling 也已写完
- 验证 merger 收到的两 branch 输出归属正确（lane 隔离不靠 name filter）

sanity-revert 验证：把 branchFn 改回共享 parentSession + FinalMessage（无 filter）→
TestISSUE80 + TestR31_ParallelAgent_FanoutFinalMessage_NoSiblingCrosstalk 都准确 FAIL
报"branch_B 应见 B-FINAL-VALUE; 实际 A-FINAL-VALUE"。

## 保留 R31 fix（向后兼容 + 业务直接用 helper）

`FinalMessageByAgent` API 保留——业务可能直接用此 helper 在其他场景按 agent name
过滤 events。godoc 标注它是辅助 helper 不是 ParallelAgent 必需协议。

`AddBranch` 重名 panic 保留——它解决的是不同问题（worker NodeID + childNS 互覆），
不依赖 lane events 模型。

## 关联

- R31 视角 D HIGH-1 (commit `4e09efe`): 临时缓解
- R32 视角 B HIGH-1 (commit `d93414d`): godoc 澄清 events 共享语义
- ISSUE-80 (本): 根因修复
