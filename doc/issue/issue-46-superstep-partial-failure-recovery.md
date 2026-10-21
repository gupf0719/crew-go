---
id: ISSUE-46
title: 超步内部分失败恢复（成功分支快照 + 只重跑失败节点）
status: ✅ 已实现
priority: 中
created: 2026-04-20
updated: 2026-04-22
depends: ISSUE-30（超步并发）、ISSUE-40（Events 持久化）
references: eino compose/checkpoint.go（Channels per-node 快照 + RerunNodes）、Spark stage task retry、Pregel
---

> **落地状态（2026-04-22）**：核心机制已实现并随 `compose/superstep_resume_test.go` 覆盖；example 演示合并到 `adv_13_superstep_send_from_test.go`。同日合并 ISSUE-41 PR 2（并发分支 InterruptError 恢复）——`branchDelta` 扩为三态，Interrupt 与业务错走同一 rerun 路径。下文"设计"一节保留原始提案；"落地后校正"与"后续审视 A"记录实现偏差与合并成果。


# ISSUE-46 · 超步内部分失败恢复

## 背景

当前 `compose/superstep.go` 的 `runBatch` 在超步内任一节点失败时：

1. 第一个错被捕获 → `cancel()` 取消 ctx，兄弟节点被 ctx 取消
2. `wg.Wait()` 后**直接 return**，跳过成功分支的 `state.Messages` / `ParallelResults` merge
3. `saveCheckpointOnError` 存档的 state 不含任何成功分支产出
4. Resume 时从单个 `current_node` 出发，重跑**整个超步**

**核心 bug**：[compose/superstep.go:201](compose/superstep.go:201) 失败路径早退，丢弃了 `results[]` 里成功分支的产出。

### 问题表现

- **浪费**：已成功分支的 LLM 调用白调（重试时全部重跑，token 重烧）
- **不可复现**：若分支含非幂等副作用（写外部、时间/随机），重跑结果不同
- **对照 eino**：eino 用 `Checkpoint.Channels`（**每个 completed node 存一份自己的 channel 贡献快照**）+ `RerunNodes`（待重跑列表）实现。本质是 **per-node snapshot + rerun 列表**

---

## 目标

超步内部分失败时：

- **成功分支的产出被单独持久化**（Messages 增量 / localState / nexts）
- **失败分支列表被记录**，Resume 时据此识别待重跑
- **Resume 只重跑失败分支**，重跑时分支输入 = 超步入口原始输入（分支间互不可见）
- **Resume 最终 state = 假装从来没失败过**（Resume 等价不变量）

**核心原则**：超步的终态 = 所有分支产出按声明顺序 merge。想"只重跑失败分支"，就必须持久化成功分支的产出。没有第三条路。

**非目标**：
- 自动重试（`RetryConfig` 节点内重试保持原样，层次不同）
- 跨超步回滚（超步是失败恢复最小原子单位）
- 并发分支内 `InterruptError`（`checkParallelInterrupt` panic 兜底保留，单独 ISSUE 跟进）

---

## 设计：超步入口快照 + BranchDelta

### 一、新增结构

失败时的 checkpoint 需要记住两类信息：

1. **超步入口快照**：rerun 失败分支时 clone 出来，保证分支输入 = 原始输入
2. **每个分支的 BranchDelta**：成功分支用于 Resume 时回放其产出；失败分支标记待重跑

```go
// compose/checkpoint.go
type branchDelta struct {
    Name       string            `json:"name"`
    Messages   []*schema.Message `json:"messages,omitempty"`   // 分支产出（Messages 增量）
    LocalState json.RawMessage   `json:"local_state,omitempty"` // 分支写完的 localState（JSON）
    Nexts      []string          `json:"nexts,omitempty"`      // 分支解析出的下一轮目标
    Output     *schema.Message   `json:"output,omitempty"`     // 优先取 newState.LastOutput（对齐 runBatch 合并语义）
    ErrMsg     string            `json:"err,omitempty"`        // 业务失败（Resume 重跑）
    Interrupt  *interruptPayload `json:"interrupt,omitempty"`  // InterruptError 挂起（Resume 重跑，ISSUE-41 PR 2 合并）
}

type interruptPayload struct {
    Reason string          `json:"reason"`
    Data   json.RawMessage `json:"data,omitempty"` // JSON 尽力序列化；any 类型往返不保证类型等价
}

type superstepSnapshot struct {
    PreMsgLen     int             `json:"pre_msg_len"`               // 超步入口 Messages 长度
    PreLocalState json.RawMessage `json:"pre_local_state,omitempty"` // 超步入口 localState 快照
    PreLastOutput *schema.Message `json:"pre_last_output,omitempty"` // 超步入口 LastOutput（可能与 Messages 末条不同）
    Frontier      []string        `json:"frontier"`                  // 原 frontier 声明顺序
    Branches      []branchDelta   `json:"branches"`                  // 与 Frontier 同序
}
```

> `Output` 字段与首次合并语义对齐说明：首次 `runBatch` 按 `r.state.LastOutput` 作 last-writer 合并 `state.LastOutput`；因此 `buildBranchDelta` 填 `d.Output` 时也优先用 `newState.LastOutput`，避免节点显式写非 Messages 末条的 `LastOutput` 时 Resume 终态漂移。`PreLastOutput` 同理——超步入口的 `LastOutput` 未必等于 `Messages[PreMsgLen-1]`，需独立快照，Resume 时失败分支才能看到与首次进入超步一致的输入。

**Checkpoint.Metadata 新增 key**：

```go
const cpKeySuperstepSnapshot = "superstep_snapshot" // JSON(superstepSnapshot)
```

超步失败路径只写 `cpKeySuperstepSnapshot`，不写 `cpKeyCurrentNode`；单节点失败路径仍只写 `cpKeyCurrentNode`。`ResumeGraph` 按 metadata 里存在的 key 分流。项目尚未上线，不考虑向后兼容。

### 二、`runBatch` 失败策略

**当前**：首错 → `cancel()` 取消兄弟 → 丢弃所有成功 merge
**改为**：
- **业务错误**（非 Interrupt / 非 ctx.Err）：**不 cancel**，兄弟跑到自然结束，然后打包超步快照存档
- **Interrupt / ctx 取消**：保留当前行为（cancel 兄弟，按原 `saveCheckpointOnError` 路径存档，不走超步 rerun 路径）

理由：Interrupt 是业务主动挂起信号，让兄弟跑副作用没意义；外部 ctx 取消整个超步都得停。只有"单分支业务失败 + 兄弟能自己跑完"才值得 best-effort 等。

伪码（`runBatch` 内）：

```go
// wg.Wait() 后
// 0) 记录超步入口快照（在 goroutine 启动前就应捕获，此处为示意）
snap := captureSuperstepSnapshot(origMsgLen, origLocalState, frontier)

// 1) JoinLanes 归并事件（保持现状）
agent.JoinLanes(state.Session, branchSessions...)

// 2) Interrupt / ctx.Err：走原路径，不走超步 rerun
if ierr := checkParallelInterrupt(firstErr); ierr != nil {
    saveCheckpointOnError(ctx, cfg.cp, cfg.cpID, string(firstErrNodeID), state, ierr)
    return state, nil, ierr
}

// 3) 一律 merge 成功分支到 state（关键：不论 firstErr 是否非 nil）
for i, r := range results {
    if r.err != nil || r.state == nil { continue }
    // 3a) Messages 增量追加
    for _, msg := range r.state.Messages[origMsgLen:] {
        state.Messages = append(state.Messages, msg)
    }
    // 3b) LastOutput last-writer
    if r.state.LastOutput != nil { state.LastOutput = r.state.LastOutput }
    // 3c) localState last-writer
    if r.state.localState != nil { state.localState = r.state.localState }
}

// 4) 构造完整 ParallelResults（成功 / 失败都填）
pr := make([]ParallelResult, len(frontier))
for i, r := range results {
    pr[i] = ParallelResult{Name: string(frontier[i])}
    if r.err != nil { pr[i].Err = r.err } else if r.state != nil { pr[i].Output = r.state.LastOutput }
}
state.ParallelResults = pr

// 5) 失败路径：捕获每分支 BranchDelta 并存档
if firstErr != nil {
    for i, r := range results {
        snap.Branches[i] = buildBranchDelta(frontier[i], r, origMsgLen)
    }
    saveSuperstepCheckpoint(ctx, cfg.cp, cfg.cpID, snap, state, firstErr)
    return state, nil, firstErr
}

// 6) 成功路径：不存 BranchDelta（多余），其余逻辑不变
```

**best-effort 的含义**：单分支业务失败不杀兄弟。代价是若失败分支永不返回（goroutine 泄漏风险），兄弟也会等。节点内 `RetryConfig` 耗尽才返回，且 ctx 有 deadline 时 runtime 会触发取消——实际风险可控。

### 三、`saveSuperstepCheckpoint`

```go
func saveSuperstepCheckpoint(
    ctx context.Context, cp agent.Checkpointer, cpID string,
    snap *superstepSnapshot, state *GraphState, nodeErr error,
) {
    // 1) 复用 marshalGraphState（成功分支的 Messages / LastOutput / ParallelResults 已在 state 中）
    // 2) Metadata 追加 cpKeySuperstepSnapshot = JSON(snap)
    // 3) 不写 cpKeyCurrentNode（超步 Resume 不依赖单节点入口）
    // 4) 走原 cp.Save 流程
}
```

`buildBranchDelta`：成功分支填 Messages 增量 + localState(JSON) + nexts；失败分支只填 ErrMsg。

### 四、Resume 路径

`ResumeGraph` 按 metadata 里存在的 key 分流：

```go
if snap, ok := readSuperstepSnapshot(cp.Metadata); ok {
    return runGraphFromSuperstep(ctx, g, cp, snap, opts)
}
// 单节点失败 Resume
return runGraphFrom(ctx, g, state, NodeID(currentNode), opts)
```

`runGraphFromSuperstep`：

```go
func runGraphFromSuperstep(ctx, g, cp, snap, opts) {
    // 1) 从入口快照重建 preState（Messages 截到 PreMsgLen，localState 从 PreLocalState 反序列化）
    preState := restorePreSuperstepState(cp, snap, g.stateFactory)

    // 2) 只 rerun 失败分支（ErrMsg != ""），每个分支从 preState clone
    //    - 成功分支不跑，直接用 snap.Branches[i] 的快照
    rerunDeltas := runRerunBranches(ctx, g, preState, snap, cfg)
    if err := rerunErr(rerunDeltas); err != nil {
        // 再次失败：更新 snap.Branches（rerun 分支可能 ErrMsg 变/不变），重新存档
        return preState, err
    }

    // 3) 按声明顺序 merge 所有 branchDelta（原成功 + 新 rerun）到 preState
    //    - Messages 按 Frontier 声明顺序追加
    //    - localState last-writer（声明顺序的最后一个非空写入者）
    //    - nexts 收集合集
    finalState, nextFrontier := mergeAllDeltas(preState, snap, rerunDeltas)

    // 4) 进主循环继续推进
    return continueGraph(ctx, g, finalState, nextFrontier, opts)
}
```

**关键性质**：
- rerun 分支 B 的输入 = preState clone，**不含兄弟产出**——与原执行等价
- A/C 的 nexts 从快照直接读，**不重算条件边**——与原执行等价
- Messages 按声明顺序 merge，顺序确定
- localState last-writer 按**声明顺序**固定，与原执行确定性一致

---

## 边界 / 行为表

| 场景 | 行为 | 理由 |
|---|---|---|
| 条件边 from 并行分支节点 | Resume 与原执行完全等价（nexts 持久化，不重算） | BranchDelta.Nexts 记录了原执行的解析结果 |
| 并行分支内写 typed state | Resume 与原执行等价（merge 按声明顺序，last-writer 确定） | BranchDelta.LocalState per-branch 保存 |
| 分支 Messages 合并顺序 | 按声明顺序，Resume 后一致 | rerun 前先从成功 delta 按序回放，再接 rerun delta |
| session.Events 叠加 | 失败分支的原 partial events + rerun 产生的 new events 共存 | 事实的忠实记录；下游按 AgentName + CreatedAt 自助处理 |
| `sendSink` 指令 | 失败时不执行，rerun 完成后统一 drain | 用户需保证指令幂等（对齐 eino） |
| 失败分支永不返回 | best-effort 等它，配合 ctx deadline 触发取消 | 节点内 `RetryConfig` 有上限，deadline 兜底 |
| 并发分支内 Interrupt | 走原路径（cancel 兄弟，saveCheckpointOnError） | 单独 ISSUE 跟进，不混入超步 rerun 机制 |
| 外部 ctx 取消 | 走原路径 | 整个超步都要停 |

---

## 实现拆分

### Phase 1：超步失败时持久化 BranchDelta
- [x] [compose/checkpoint.go](../../compose/checkpoint.go)：`branchDelta` / `superstepSnapshot` / `cpKeySuperstepSnapshot` 已落地（第 45-70 行）
- [x] [compose/superstep.go](../../compose/superstep.go)：
  - 入口 `preMsgLen` / `origLocalState` 指针快照（第 116-120 行，成功路径零序列化开销）
  - best-effort 策略：业务错不 cancel 兄弟；Interrupt / hook 失败 / clone 失败走原路径（第 83-93 行注释）
  - `wg.Wait()` 后统一 merge 成功分支 + 构造完整 `ParallelResults`
  - `buildBranchDelta(name, newState, nexts, err, origMsgLen)` → `saveSuperstepCheckpoint`
- [x] 单测 [compose/superstep_resume_test.go](../../compose/superstep_resume_test.go)：3 节点超步、A/C 成功 + B 失败，验证快照完整性

### Phase 2：超步级 Resume
- [x] [compose/checkpoint.go](../../compose/checkpoint.go)：
  - `ResumeGraph` 按 `superstep_snapshot` 分流（第 258-261 行）
  - `runGraphFromSuperstep`（第 629 行起）
  - 辅助函数落地为 `rerunFailedBranches` / `applyAllBranchDeltas`（`restorePreSuperstepState` 逻辑 inline 在 `runGraphFromSuperstep` 第 700-721 行）
- [x] rerun 再失败重存档（`runGraphFromSuperstep` 第 758-761 行）
- [x] 单测覆盖：只重跑失败分支、最终 state 等价、B 再次失败仍只跑 B

### Phase 3：观测 + 文档
- [x] example：`example/adv_13_superstep_send_from_test.go` 已覆盖（超步 + SendFrom + Resume 三合一），不再单独做 `adv_14`
- [x] CLAUDE.md：随本次更新补"Graph 超步 ctx deadline 契约"条目；ISSUE-46 已在"核心类型速查"里标注
- [x] runtime.md：已在"四、Checkpoint"章节详细记录两条存档路径 + Resume 分流（见第 195-305 行）
- [~] `OrchestrationHooks.OnSuperstepResume(rerunNodes []NodeID)` —— **不采纳**。`OnNodeStart` + Resume 路径日志已能观测重跑节点集合，再加一层专属 hook 价值边际；且与 ISSUE-41 PR 2 合并后语义还要重定义，目前不引入。

---

## 为什么必须 per-branch 快照

（方案迭代记录，2026-04-20 讨论）

初版方案试图"复用 `ParallelResults` 零新结构"：只存合并后的 state + rerun 列表。被否原因：

超步的终态语义 = **所有分支产出按声明顺序 merge**。这要求 Resume 时能同时：
1. 让 rerun 分支看到"超步入口原始输入"（分支间互不可见的不变量）
2. 让最终 state 包含所有分支（包括已成功的 A/C）的贡献

只存合并态 → 丢了 A/C 的独立产出，rerun B 时要么重算（副作用重复、token 重烧）、要么看到合并态（Messages 污染 / 条件边错算 / localState 顺序不确定）。两条都违反 Resume 等价不变量。

唯一不违反的路径：**持久化每分支产出**（BranchDelta）+ **持久化超步入口快照**。这与 eino `Checkpoint.Channels`（per-node channel 贡献快照）设计哲学一致——不是"全局累积字段"，而是"每个 completed node 存一份自己的快照，rerun 时从当时输入重跑"。

成本权衡：
- **数据冗余**：`BranchDelta.Output` 与 `ParallelResults[i].Output` 字面重复——可接受，UI/观测用 ParallelResults，Resume 用 BranchDelta
- **成功路径不受影响**：成功路径不生成也不存 BranchDelta，运行时开销为 0
- **失败路径存档体积增大**：每分支多一份 Messages/localState 序列化——但失败本就是 exceptional path，可接受

---

## 参考

- eino：`/Users/gupengfei/program/go/src/eino/compose/checkpoint.go:106-117` (Checkpoint.Channels per-node 快照)、`graph_run.go:510-565` (save)、`graph_run.go:390-405` (restore)
- Spark：stage 内 task 失败，DAGScheduler 重新提交失败 task；成功 task 的 shuffle output 保留在 BlockManager（本质也是 per-task 持久化）
- Pregel：vertex 级 checkpoint + 超步级 barrier

---

## 落地后校正（2026-04-22）

实现与原设计的偏差，按重要性排列：

### 1. `saveSuperstepCheckpoint` 不复用 `marshalGraphState`

**原设计**："复用 `marshalGraphState`"。
**实际**：`marshalGraphState` 会强制写入 `cpKeyCurrentNode`，与"超步失败路径只写 `cpKeySuperstepSnapshot`"的分流约定冲突，因此 `saveSuperstepCheckpoint` 自行 inline 了 Metadata 序列化（`compose/checkpoint.go:490-552`）。

**连带影响**：`Checkpoint.Metadata` 的 `current_node` 与 `superstep_snapshot` 严格互斥，`ResumeGraph` 可据此可靠分流。

### 2. `buildBranchDelta` 签名

**原设计**：`buildBranchDelta(frontier[i], r, origMsgLen)`（3 参数）。
**实际**：`buildBranchDelta(name, newState, nexts, err, origMsgLen)`（5 参数，`compose/checkpoint.go:566`）。展开后的显式签名便于调用点传递，也让失败分支的零值路径更清晰。

### 3. Resume 路径的辅助函数命名

**原设计**：`restorePreSuperstepState` / `runRerunBranches` / `mergeAllDeltas`。
**实际**：`rerunFailedBranches`（`compose/checkpoint.go:800`） / `applyAllBranchDeltas`（第 927 行）。`restorePreSuperstepState` 逻辑直接 inline 在 `runGraphFromSuperstep` 第 700-721 行——调用点唯一，抽出反而增加跳转成本。

### 4. Phase 3 `OnSuperstepResume` hook 不采纳

理由见 Phase 3 checklist。

### 5. Phase 3 `adv_14` 合并到 `adv_13`

`adv_13_superstep_send_from_test.go` 已通过 `failOnceAgent` 覆盖"超步内某分支首次失败、二次成功"的完整链路，再单独做 `adv_14` 造成 example 冗余。保留单一入口。

---

## 后续审视（2026-04-22，基于实现后讨论）

落地后识别的新维度，记录作为后续 ISSUE 或协议约定的入口，**本期不改代码**。

### A. 与 ISSUE-41 PR 2 的合并（✅ 已实现 2026-04-22）

ISSUE-46（业务错恢复）和 ISSUE-41 PR 2（并发分支 Interrupt 恢复）是**同一机制的两种失败形态**，已合并为统一实现：

| 失败形态 | 识别字段 | 共享机制 |
|---|---|---|
| 业务错误（分支 return err） | `branchDelta.ErrMsg` | per-branch delta + 入口快照 + rerun 列表 |
| 人类挂起（分支 `InterruptError`） | `branchDelta.Interrupt`（Reason + JSON-序列化 Data） | per-branch delta + 入口快照 + rerun（节点内靠 `session.Events` 恢复上下文，ISSUE-40） |

**落地改动**：

- `branchDelta` 扩为三态（成功 / `ErrMsg` / `Interrupt`），Interrupt 为一等公民——见 [compose/checkpoint.go](../../compose/checkpoint.go) `branchDelta` / `interruptPayload`
- `buildBranchDelta` 通过 `errors.As(err, &ie)` 识别 `*agent.InterruptError`，填入 `Interrupt` 字段
- `runBatch` 的 Interrupt 与业务错误走**同一路径**——**不 cancel 兄弟**（与"业务错不 cancel"对齐），统一走 `superstep_snapshot` 存档。删除了原 `checkParallelInterrupt` 兜底块与 goroutine 内 `cancel()` on Interrupt——见 [compose/superstep.go](../../compose/superstep.go) `runBatch`
- `rerunFailedBranches` 过滤条件扩为 `ErrMsg != "" || Interrupt != nil`——见 [compose/checkpoint.go](../../compose/checkpoint.go) `rerunFailedBranches`
- 测试覆盖 [compose/superstep_resume_test.go](../../compose/superstep_resume_test.go) `TestSuperstep_PartialFailure_InterruptBranch`

**不在合并范围**：ParallelAgentNode / FanOut / SendFrom / Routing 的阻塞型扇出未实现分支级快照，**仍保留** `checkParallelInterruptResults` 的"暂不支持"包装。若未来这些扇出需要支持 Interrupt Resume，按同一模式扩展（为其各自的结果结构加 `Interrupt` 字段 + 分支级快照），本 ISSUE 不处理。

**Interrupt 数据往返约定**：`InterruptError.Data` 是 `any`，JSON 序列化"尽力而为"——用户若要在 Resume 前修改 Data（注入 resume value），**建议用 JSON-friendly 的 struct/map/基本类型**，并在 Resume 前通过 `Checkpoint.Metadata` 修改或在节点内从 `session.Events` 读取人工注入的 event。

### B. ctx deadline 契约（已下沉 CLAUDE.md）

ISSUE-46 的 best-effort 策略（业务错不 cancel 兄弟）依赖**调用方设 ctx deadline** 兜底：

- 若某失败分支 `NodeFunc` 不返回（死循环 / 外部调用挂死 / 节点内 `RetryConfig` 无限），兄弟会一直等
- 节点内 `RetryConfig` 有上限，但用户自定义 `NodeFunc` 不一定遵守
- deadline 是唯一的外部兜底

**约定**：使用 Checkpointer 的 `RunGraph` 调用方**必须**设 ctx deadline。此条已同步到 `CLAUDE.md` 的"交付纪律"章节。

### C. 嵌套 GraphAgent 共享 session 的 cpID 冲突

`cpID = state.Session.ID`（`compose/checkpoint.go:93`），session ID 即 checkpoint 键。

**冲突场景**：外层 GraphAgent 和内层 GraphAgent 共享同一 session 对象 → 共享 cpID → 内层超步快照会**覆盖**外层快照。

**当前处理**：`ParallelAgent` / `LoopAgent` / `PlanAgent` / `ReflectAgent` / `HandoffAgent` 内部不走 `runBatch` 超步路径（走独立的 fanout 机制），不会触发此冲突。只有用户手写嵌套 `GraphAgent` 且共享 session 时才会触发。

**边界约定**：嵌套 `GraphAgent` 使用 Checkpointer 时，**上层编排原语负责 session 隔离**（通过 `agent.NewRunSession()` 创建 child session，或使用不同的 Checkpointer 实例）。未在代码中强制校验；属用户纪律。

### D. 持久化后端数据量影响

InMemoryCheckpointer 下此开销无感，但项目规划上 Postgres / Redis 等生产级后端落地时（见 `doc/issue/issues.md` 中 Checkpointer 后端待办），单次失败快照体积按 `分支数 × 消息大小 × LocalState 大小` 线性膨胀：

- 10 分支 + 每分支 20 条消息 + 1MB typed state → 单次失败快照可达 ~30MB 量级
- 反复失败 Resume 形成快照链：每次 `runGraphFromSuperstep` 失败都会重存一次完整 snapshot
- 成功 Resume 后旧超步快照不再需要，但当前没有 GC 策略（Checkpointer.Delete 靠调用方）

**后端落地时需考虑**：
- Metadata 字段是否走 TOAST / 外部 blob 存储（Postgres）
- 单行大小上限
- 超步快照过期策略（基于 `updated_at` + TTL，或成功 Resume 后自动清理）
- 若同 cpID 多次失败覆盖存档，是否保留历史版本（审计 vs 体积）

作为 Postgres Checkpointer 设计阶段的输入，不在本 ISSUE 范围内处理。

---

## 2026-04-24 增量：`ResumeFullSuperstep` 模式

原始设计锁死了"定向重跑"语义——成功分支 delta 回放、失败分支 rerun。存在一类真实场景做不了：成功分支的产出依赖外部状态（时间 / 随机 / 远程调用），rerun 失败分支时希望所有分支看到**一致的新快照**，而不是"老成功 + 新失败"混合态。

**改动**：`ResumeGraph` 签名加 `opts ResumeOptions`，`ResumeOptions.Mode` 新增枚举：

| Mode | 语义 |
|---|---|
| `ResumeDirected`（默认） | 保持本 ISSUE 原始语义——仅 rerun 失败 / Interrupt 分支 |
| `ResumeFullSuperstep` | 丢弃成功 delta，整个 frontier 全部 rerun |

实现上只动一处：`rerunFailedBranches` 在 FullSuperstep 模式下把 `snap.Branches` 全部加入 `rerunIdxs`。rerun 完成后所有 branch delta 被新结果替换，`applyAllBranchDeltas` 天然算出"全新 delta"合成的 finalState，不需要另起并行路径。

**副作用**：FullSuperstep 下 `session.Events()` 会保留首次归并的事件，rerun 再次 append 新事件——上层若在意事件重复需在 Resume 前自行清理。这是有意的权衡（保持 ISSUE-40 的"日志恢复"语义），未来可考虑在 `superstepSnapshot` 里加 `PreEventsLen` 字段做精确截断。

**测试**：`TestSuperstep_FullSuperstepResume` 覆盖三分支超步、B 首次失败、FullSuperstep Resume 后 C/D 各 rerun 一次、finalState.Messages 中 c:ok / d:ok / b:ok 各仅出现一次（不叠加）。
