---
id: ISSUE-61
title: compose.fanout / RoutingAgent fan-out / SendFrom 并发分支 ask_user
status: ✅ 已实现（2026-05-01）
priority: 中
created: 2026-04-30
---

# ISSUE-61 · 剩余三处 fan-out 路径并发 ask_user

## 背景

ISSUE-60 v1 解决了 `agent.ParallelAgentNode` 路径的并发 ask_user，但还有三处
fan-out 路径仍走旧的「拒绝 InterruptError」路径：

1. `compose/fanout.go::AddFanOutEdge` —— 裸 NodeFunc fan-out
2. `compose/send.go::SendFrom` —— 运行时动态 fan-out
3. `agent/routing.go` Router 多目标 fan-out —— Agent fan-out

这三条路径触发并发 InterruptError 时返回 `parallel branch interrupt not yet
supported on this fan-out path` 错误，用户体验断裂。

## 落地方案

复用 ISSUE-60 v1 的核心机制：`state.pendingInterrupts` buffer + `cpKeyInterrupts`
union + `appendResumeMessages` 嵌套防污染。三处分别接入。

### 改动一：`agent/routing.go` 多目标 fan-out

复用 ParallelAgentNode 的 `dispatchBranchAgent` + `state.pendingInterrupts`
buffer 模式：

- 分支用 `route:<name>#idx` 命名
- `NestableAgent` 走嵌套 cp 协议
- wg.Wait 后扫各分支 InterruptError 写 buffer，抛 outer InterruptError

为 RoutingAgent 加：
- `Run` 路径透传 ctx 中 Checkpointer
- 新增 `Resume(ctx, sessionID, opts...)`（仅 Router 路径支持，LLM 分类器路径单
  目标无 fan-out 需求）
- `runWithRouter` 抽出 `buildRouterGraph(input)`，让 Run 与 Resume 共用拓扑

### 改动二：`compose/fanout.go` 与 `compose/send.go`

新增公共 `compose.CollectBranchInterrupts(ctx, state, branchIDs, results) int`
helper：

- 扫描分支 results 把 *InterruptError 携带的 Query 按 InterruptID 写入
  `state.pendingInterrupts` buffer
- **InterruptID 兜底填充**：裸 NodeFunc 不经过 saveCheckpointOnError 自动回填，
  本函数用 `MakeInterruptID(parentRunPath, "<targetNodeID>#<idx>", 1)` 生成稳定
  ID，让 caller 用 UserInputs map 配对
- 同名 (id, idx) 重复出现以最后一次为准（buffer map 覆盖语义）

`makeFanOutFunc` 与 `executeSendDirectives` 都把 `CheckParallelInterruptResults`
拒绝换成 `CollectBranchInterrupts` 收集 + 抛 outer InterruptError。

### 改动三：`compose.executeSendDirectives` 的 cp 持久化路径

ISSUE-61 之前 sendErr 路径只 wrap 错误返回，未触发 saveCheckpointOnError——
SendFrom 分支挂起时 outer cp 完全不写，Resume 立刻 "checkpoint not found"。
修为 sendErr 非 nil 时显式 save 后再返回。

### 死代码清理

删除 `compose.checkParallelInterrupt` / `compose.CheckParallelInterruptResults`，
所有 fan-out 路径不再有"暂不支持"拒绝。

## 裸 NodeFunc Resume 限制（临时决策；根因修在 ISSUE-62 方案 B）

- 裸 NodeFunc 没有 inner cp 落点，Resume 时**所有**分支重新执行——包括上次成功
  完成的分支
- 节点函数必须自己保证 Resume 重跑幂等：通过 `compose.GetResumeContext(ctx)`
  判 `nil`/非 nil 区分"首次执行"vs"被恢复过"，或在 `GraphState` typed state
  里写"已完成"标志
- ISSUE-61 落地时此限制是临时约定（避免 lane-aware cp 大改）；**ISSUE-62 方案 B
  把 fan-out 编译为超步原语后，复用 ISSUE-46 superstep_snapshot 机制，此限制
  与 Agent fan-out 的 ISSUE-62 重跑问题一并消除**

## 端到端测试

- `agent/routing_resume_test.go::TestRouting_FanOut_ConcurrentAskUser_Resume`
- `compose/fanout_resume_test.go::TestFanOut_ConcurrentInterrupt_ResumeRoundtrip`
  （断言裸 NodeFunc Resume 后各分支重跑 1 次）
- `compose/sendfrom_resume_test.go::TestSendFrom_ConcurrentInterrupt_ResumeRoundtrip`
  （断言同节点多次派发的两分支拿到不同 InterruptID）

## 后续

ISSUE-62 方案 B 落地后，本 issue 的「裸 NodeFunc Resume 重跑」临时约定自动消除
（fan-out 成功分支不再被重跑）。
