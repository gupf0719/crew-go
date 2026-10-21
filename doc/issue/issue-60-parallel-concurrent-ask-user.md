---
id: ISSUE-60
title: ParallelAgent 并发 ask_user HITL 端到端
status: ✅ 已实现（v1 仅 ParallelAgent；其它 fan-out 路径见 ISSUE-61/62）
priority: 高
created: 2026-04-30
---

# ISSUE-60 · ParallelAgent 并发 ask_user HITL 端到端

## 背景

ISSUE-58 协议层 + ISSUE-59 BasicAgent Graph 化让单 worker 的 ask_user 跑通了
端到端。下一个真实场景：**ParallelAgent 多分支 worker 同时 ask_user**——例如
两个并发 expert 各自需要用户确认参数，框架需要：

1. 持久化 N 个 InterruptID 各自独立的 Query 到外层 cp
2. Resume API 接受 `map[InterruptID]*Message` 精确配对答案
3. 每个分支 worker 内层只看到属于自己的答案（不被兄弟分支污染）

旧版 `compose.checkParallelInterrupt` 直接拒绝并发 InterruptError，明确告诉
caller "暂不支持"。本 issue 是该限制的根因解除。

## 设计

### 寻址：`InterruptID`

借鉴 LangGraph `Interrupt.id = xxh3_128_hexdigest(ns)`：

```go
// compose/runpath.go
func MakeInterruptID(runPath []RunStep, nodeID string, invocation int) string {
    h := fnv.New64a()
    for _, s := range runPath { h.Write([]byte(s.AgentName)); h.Write([]byte{0}) }
    h.Write([]byte(nodeID)); h.Write([]byte{0})
    h.Write(invocationLE(invocation))
    return hex.EncodeToString(h.Sum(nil))
}
```

同一物理位置（runPath + 节点 + 调用计数）跨 retry/Resume 拿到同一 ID。
多并发分支因 runPath 末尾 AgentName 不同而拿到不同 ID。

### 持久化：`cpKeyInterrupts` 通道

cp metadata 用 `cpKeyInterrupts map[interruptID]*AskQuery` 替代旧版单值
`cpKeyInterruptQuery`——同一 cp 可索引多条并发 Query。

`saveCheckpointOnError` 两条来源 union 进 `cpKeyInterrupts`：

1. `state.pendingInterrupts` buffer（fan-out 节点 wg.Wait 后显式收集的）
2. 单层 `ie.Query`（仅当**非嵌套冒泡**——`hasNestedSubgraphCheckpoint` 抑制
   外层重复持久化，让 Query 仅由真正发生处的 cp 持有）

### 分支收集：`state.pendingInterrupts` buffer

`compose.GraphState.pendingInterrupts map[string]*AskQuery` + `AddPendingInterrupt`
/ `PendingInterrupts` 方法：节点级 outgoing 通道，fan-out 节点 wg.Wait 后扫各
分支 InterruptError 把 Query 写入 buffer，再向上抛单一 outer InterruptError。

### Resume API：`UserInputs map`

`ResumeOptions` 双形态：
- `UserInput *Message` 单值：仅当 cp 含 ≤1 pending 时合法；多 pending 强制
  改用 `UserInputs`（抄 LangGraph 防误用）
- `UserInputs map[interruptID]*Message`：按 InterruptID 精确配对，与 UserInput 互斥

`agent` 层暴露 `WithResumeUserInputs(map)`，`ParallelAgent.Resume` 透传。

### 嵌套防污染：`appendResumeMessages` 过滤

```go
// compose/checkpoint.go::appendResumeMessages
for id, msg := range userInputs {
    if id != "" && !pendingInterrupts[id] {
        continue  // 兄弟分支答案 → silently drop
    }
    ...
}
```

外层 cp 同时跳过 Messages append（`hasNestedSubgraphCheckpoint(state) == true`），
让 tool_result 只 append 在真正发生 ask_user 的内层 inner cp 上。

## 实现改动

| 模块 | 文件 | 改动 |
|---|---|---|
| state buffer | `compose/graph.go` | `GraphState.pendingInterrupts` + `AddPendingInterrupt` / `PendingInterrupts` |
| cp 持久化 | `compose/checkpoint.go` | `saveCheckpointOnError` union buffer 进 `cpKeyInterrupts`；`appendResumeMessages` 嵌套防污染 filter |
| 分支调度 | `agent/orch_helpers.go` | `dispatchBranchAgent`（NestableAgent 嵌套 cp 优先 + retry fallback）；`ParallelAgentNode` wg.Wait 后调 `CollectBranchInterrupts` |
| API | `agent/parallel.go` | `Run` 透传 ctx Checkpointer；新增 `Resume(ctx, sessionID, opts...)` |
| API | `agent/resume_options.go` | `UserInputs` 字段 + `WithResumeUserInputs` |
| 信号转发 | `agent/basicagent_graph.go` | nested ResumeSubgraph 二选一转发 UserInput / UserInputs |

## 端到端测试

`agent/parallel_resume_test.go::TestParallel_ConcurrentAskUser_ResumeWithUserInputs`
覆盖 2 分支并发 ask_user → 外层 cp 持有 2 条 cpKeyInterrupts + 2 条 subgraphCheckpoints
→ Resume(UserInputs) → 两 worker 第二轮 LLM 看到正确 tool_result（且不含兄弟分支
答案）→ merger 合并产出。

## 已知局限

- ✅ ISSUE-61：剩余三处 fan-out 路径（compose.fanout / SendFrom / RoutingAgent）已落地
- ✅ ISSUE-62：fan-out 成功分支 Resume 重跑问题已通过超步化方案 B 根因解决
