---
id: ISSUE-62
title: fan-out 成功分支在 Resume 时被重跑（根因：fan-out 不是图原语）
status: ✅ 已实现（2026-05-01）
priority: 中
created: 2026-05-01
---

# ISSUE-62 · fan-out 超步化重写

## 背景

ISSUE-60/61 让所有 fan-out 路径支持并发 ask_user Resume，但暴露了一个深层问题：
**Resume 时所有分支都被重新执行，包括上次已成功完成的**。覆盖：

- `agent.ParallelAgentNode`
- `agent.RoutingAgent` Router 多目标 fan-out
- `compose.AddFanOutEdge`
- `compose.SendFrom`

**症状**：
- LLM 调用 / 工具调用浪费一轮（成本 + 时延）
- 不幂等副作用（DB 写、邮件、外部 API）执行第二次
- 同时也是 ISSUE-61"裸 NodeFunc Resume 重跑所有分支"那条文档约定的根因

**根因**：fan-out 当前实现为「单合成节点内 fork-join」——`__fanout_<from>__`
合成节点 / `ParallelAgentNode` body 节点内部用 `sync.WaitGroup` 并发跑所有分支，
对图引擎是单节点。引擎只有"当前节点 ID"粒度的 cp，无法记录"分支 i 已完成、
分支 j 待重跑"。Resume 时整个合成节点重新执行 → 所有分支重跑。

## 对照设计

### LangGraph `pending_writes`

`libs/checkpoint/.../base/__init__.py:30`：

```python
PendingWrite = tuple[str, str, Any]   # (task_id, channel, value)
```

时序（`_loop.py:393, 565`）：
- 一个 superstep 内多 task 并发，每个 task 完成时调用 `put_writes(task_id, writes)`
- 完成的 task 的 writes 立刻 append 到 `checkpoint_pending_writes`，但 channels 还没 commit
- 若超步内某 task interrupt → checkpoint 保存「超步前的 channels 快照 + 已完成 task 的 pending_writes」
- Resume 时 `prepare_next_tasks` 只把"errored/interrupted"task 放进 next_tasks；成功 task 的 writes 直接应用到 channels（不重跑）

**invariant**：任何 superstep，成功 task 最多执行一次。

### eino 双层

- compose 层（`compose/dag.go::dagChannel.Values` + `compose/graph_run.go:511`）
  做了 LangGraph 同款的事——channels 持久化 + RerunNodes 列表
- adk 层（`adk/workflow.go::parallelWorkflowAgent`）只有 events 重放
  （`SubAgentEvents`），未走 channel 路径——因为 Agent 在 adk 里是 stateful
  黑盒，拿不到结构化 channel writes

我们的处境：ParallelAgent 直接是 `compose.NodeFunc`，没有 adk 那种黑盒边界
——eino adk 不能做的事我们能做。

## 落地方案：方案 B（让 fan-out 编译成真正的超步 frontier）

ISSUE-46 已经实现了核心机制：
- per-branch delta 持久化（成功分支产出存 snapshot）
- 入口快照保 Messages/localState，Resume 时入口与首次一致
- ResumeDirected 模式只重跑失败/中断分支，成功分支从 delta 回放

**fan-out 接入超步路径后此机制自动覆盖两条 ISSUE-62 路径 + ISSUE-61 裸 NodeFunc 限制**。

## 实现：分三步

### Step 1：超步基础扩展（不破现有）

让 ISSUE-46 超步机制能承载 fan-out 场景：

| 改动 | 文件 | 说明 |
|---|---|---|
| 分支 clone 拷贝 subgraphCheckpoints | `compose/fanout.go::cloneStateForFanOut` | 浅拷贝外层 map 给分支，让分支能读到 Resume 上轮自己的 inner cp |
| runBatch 合并分支 subgraphCheckpoints 回外层 | `compose/superstep.go` | 所有分支（含失败/Interrupt）的 subgraphCheckpoints 都合并回外层 state，让 ask_user 中断态随外层 cp 持久化 |
| saveSuperstepCheckpoint 序列化 subgraphCheckpoints + pendingInterrupts | `compose/checkpoint.go` | 让超步路径 cp 与单节点路径 cp 一致 |
| runGraphFromSuperstep 还原 subgraphCheckpoints | `compose/checkpoint.go` | preState 也带 subgraphCheckpoints，rerun 时 dispatchBranchAgent 找得到 inner cp |
| 解除超步 HITL 限制 + 注入 ResumeContext | `compose/checkpoint.go::ResumeGraph` | UserInput / UserInputs 在超步路径放行，分支 NodeFunc 通过 `GetResumeContext` 转发给内层 ResumeSubgraph |

### Step 2：ParallelAgent / RoutingAgent 拓扑改写

- `agent/parallel.go::buildGraph` 改为 START → branch:&lt;name&gt;#i → merge → END 多节点拓扑（不再单合成 NodeFunc 内部 fork-join）
- `agent/routing.go::buildRouterGraph` 用 `compose.SendFrom` 投递到 worker:&lt;name&gt;
  SendTarget 节点（动态分支），加 `finalize` 节点 fan-in

每个分支是真正的图节点：
- 编译期声明的多 START→branch 边触发 graphLoop 进入 superstep 模式
- 超步内 ISSUE-46 superstep_snapshot 自动接管：成功分支产出存入 snap delta
- Resume 时 ResumeDirected（默认）只重跑失败/中断分支，成功分支从 delta 回放
- **解决 ISSUE-62**：成功分支不再被 Resume 重跑

### Step 3：SendFrom sink-driven 调度（对标 LangGraph `Send`）

`compose/send.go` 与 `compose/superstep.go` 重构：

- 新增 `compose/frontier.go::frontierItem{Target, Payload}` 类型，所有调度路径感知 Payload
- `SendFrom` 不再节点尾部同步派发，而是把 directives 写入分支局部 sink
- 节点 return 后，scheduler drain sink → directives 转下一超步 frontierItem
- 失败分支的 directives 自然丢弃（与 LangGraph task 失败时 pending_sends 不 commit 一致）
- 删除 `compose/send.go::executeSendDirectives` 与 `cloneStateForSend`（旧的 sync 路径）
- `compose/fanout.go::AddFanOutEdge` 改为编译期注册图原生 fan-out 边 + 合成 merge 节点（不再 makeFanOutFunc 单合成节点）

`runBatch` 兜底空 InterruptID：裸 NodeFunc 抛 *InterruptError 时 Query.InterruptID
一般为空，用 `MakeInterruptID(parentRunPath, target#idx, 1)` 兜底——同一物理位置
跨 Resume 拿到同一个 ID。

## 端到端测试

- `agent/parallel_resume_test.go::TestParallel_AsymmetricInterrupt_ResumeRerunsSuccessful`
  ——success 分支 Resume 后 calls=1（不再重跑）。**冻结测试反转**：旧 ISSUE-62 期望
  calls=2，新方案 B 落地后改为 calls=1
- `agent/routing_resume_test.go::TestRouting_FanOut_ConcurrentAskUser_Resume`
- `compose/fanout_resume_test.go::TestFanOut_ConcurrentInterrupt_ResumeRoundtrip`
- `compose/sendfrom_resume_test.go::TestSendFrom_ConcurrentInterrupt_ResumeRoundtrip`
  ——splitter 只跑 1 次（Resume 不重跑），worker 跑 4 次（首次 2 interrupt + Resume 2 完成）

## 决策记录

### 为何选方案 B 而非 A（仿 eino adk SubAgentEvents 重放）

之前考虑过：在 outer cp metadata 加 `cpKeyBranchOutcomes` 缓存成功分支的 Output +
Events，dispatchBranchAgent 命中缓存时跳过 RunSubgraph。

**不选 A 的根因**：
- eino adk 走这条路径是被 SDK 黑盒契约逼的，我们没此约束
- 仅适用 Agent fan-out（裸 NodeFunc 没 events 流）；ISSUE-61 的限制还得另开方案
- 与 ISSUE-46 superstep_snapshot 是两套不兼容的 cp 机制，重叠功能

**用户反馈推进了决策**：用户在 review 时明确"别再用 ABC 三选一让我背决策成本，
告诉我哪个是 hack 直接做根因修"——A 本质是 hack（绕开根因），B 是根因修（fan-out
本来就该是图原语，旧合成节点架构本身是历史折中）。

## 后续可改进项（未在本次范围）

- AddFanOutEdge 的 `policy ParallelErrorPolicy` 参数语义变更（超步路径不 cancel 兄弟），
  当前实现忽略。如有 cancel 需求需在超步层支持显式 cancel 信号
- `runBatch` 的 ParallelResults 仅在多分支超步路径填充；单 worker 走 fast path 不填，
  finalize 节点用 LastOutput 兜底（已实现，但语义不对称）
- ISSUE-46 的 `ResumeFullSuperstep` 模式与 fan-out 场景的交互未单独覆盖测试
