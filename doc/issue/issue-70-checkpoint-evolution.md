---
id: ISSUE-70
title: Checkpoint 系统演进——namespace 物理分离 + 成功即清生命周期 + 三方精华融合
status: ✅ Phase 1-3 已完成；Phase 4 推迟到业务踩到 / PG 接入
priority: 高
created: 2026-05-05
completed: 2026-05-05
depends: ISSUE-46, ISSUE-59, ISSUE-62, ISSUE-64, ISSUE-69
---

# ISSUE-70 · Checkpoint 系统演进

> **⚠️ v2 标注 (audit follow-up 2026-05)**: 本 ISSUE 文档是 v1 时代写的, 其中
> `IsResumeTarget(currentNS, ...)` / `ResumeRouting` self/ancestor/sibling 三模式
> 自检 helper 在 v2 重构时**已删除**——v2 走 LangGraph 风格 map ID 命中协议,
> 嵌套靠 `MakeInterruptID(graphNS, nodeID, visit, branchIdx, idx)` graphNS 编入 +
> opt-in `StripResumeContext` / `WithSubgraphIsolatedResumeContext` 隔离.
>
> 本文 7 处 `IsResumeTarget` / `ResumeRouting` 引用是 v1 演化轨迹本身, 保留正文
> 作历史溯源用. v2 现状 + 决策见 `doc/description/three-way-comparison/interrupt-resume.md`
> + `doc/v2/checkpoint.md §7.3`（嵌套 Resume 的节点路由；原 channel-engine §6.5 已迁至该篇）.


> **状态**：Phase 1-3 全部落地（嵌入式 cp → ns 物理分离 + 嵌套 HITL 闭环 +
> 三方对照沉淀 + SSE ns 字段）；Phase 4（StateFactory migrate / SelfHistoryByNS
> / 性能优化 / SaveBatch 真原子）推迟到业务踩到或 PG 后端接入时实施。详见
> [Phase 4 推迟决策](#phase-4-推迟决策) + [收尾总结](#收尾总结)。
>
> **方法论**：5 轮方案迭代 + 6 轮交叉审核（共派 14 个独立 agent + 2 轮 binary
> 冒烟），按 CLAUDE.md "多层系统 bug 审计方法论" + "拒绝短期方案只做根因修复"
> 原则。

## 收尾总结

**实施期**：2026-05-05（单日内完成 Phase 1-3 全部落地，含 6 轮交叉审核）。

**核心架构成果**：

1. **嵌入式 cp → ns 物理分离**（核心架构改动）
   - 抛弃旧 `state.subgraphCheckpoints` 嵌入父 metadata 协议（与 eino 同款）
   - 对齐 LangGraph `(thread_id, ns)` 二级寻址，子图 cp 物理写到父 store 的
     `(parentSID, childGraphNS)` 路径
   - 删除 `cpKeySubgraphCheckpoints` / `hasNestedSubgraphCheckpoint` /
     `state.subgraphCheckpoints` 三件套

2. **嵌套 HITL 闭环**（Resume 端到端）
   - 7 个编排（BasicAgent / Sequential / Loop / Reflect / Handoff / Parallel /
     Routing / Plan）全实现 NestableAgent 接口
   - `query.NS` + `IsResumeTarget` 三模式（self / ancestor / sibling）守门
   - `cleanupOnSuccess` 防 cp 永久残留 + `crossNSPendingInterrupts` 跨 ns 推导
     PendingNSs

3. **三方对照沉淀**（设计决策可追溯）
   - 4 个三方对照文档全 ✅：checkpoint-storage / subgraph-isolation /
     interrupt-resume / address-routing
   - 关键决策："不抄 eino typed Address / InterruptCtx 三态"——保留 string ns
     + IsResumeTarget 三态分立，节省 2 工作日 over-engineering

4. **工作流改进**
   - CLAUDE.md "实施前 checklist" 加第 4 条（cp / 嵌套 / Resume / ns 改动前必读
     对应主题文档）
   - "交付纪律" 加 commit 前必跑两个 binary 冒烟
   - `example/orchestration/nested_smoke` binary 8/8 端到端场景

**完整 commit 列表**（14 commit + 1 收尾）：

| Commit | Phase | 内容 |
|---|---|---|
| `659a220` | Phase 1 | 接口铺路 + 真 bug 修复 + 设计文档落档 |
| `9273e27` | Phase 2 Step 1-3 | ns 基础设施 + GraphBuilder 校验 |
| `d6ae069` | Phase 2 Step 4 | 删 ResumeContext.UserInput 单值通道 |
| `0cf51c9` | Phase 2 Step 5 | MakeInterruptID 改 toolCallID |
| `39faa97` | 跨 commit 审计 | 4 H + 4 M 修补 |
| `0ed6dd2` | Phase 2 波 0.5 | RunSession 字段拆分（ID → ThreadID + LaneID）|
| `c0bbf7f` | Phase 2 波 1.1 | IsResumeTarget 自路由 + 删 ClearResumeContext |
| `7e9969a` | Phase 2 | 三方对照文档落地 |
| `ed42662` | Phase 2 Step Ⅰ | 嵌入式 cp → ns 物理分离（核心架构）|
| `ed58520` | Phase 2 Step Ⅱ | fan-out 路径清理 |
| `43ef7eb` | Phase 2 Step Ⅳ | ResumeFullSuperstep + events ns 过滤 |
| `94ca193` | Phase 2 Round 1 | 4 HIGH bug 修复（二次审核）|
| `f87edf5` | Phase 2 Round 2 | 5 MEDIUM/LOW 清理 |
| `c3e28ac` | Phase 2 Round 3 | 4 编排 NestableAgent + 嵌套 HITL 集成测试 |
| `50b9652` | Phase 2 冒烟 | nested_smoke binary 4/4 |
| `c463341` | 工作流补救 | CLAUDE.md commit 前必跑冒烟 + nested_smoke 8/8 |
| `aa1310c` | Phase 3 | 三方对照 address-routing.md + interrupt-resume.md |
| `8e1727f` | Phase 3 | plan_chat_web SSE adapter 加 ns 字段 |

**关键不变量验证**（端到端 binary 冒烟）：
- ✅ ns 物理分离（query.NS = "executor_step_1/model" 嵌套层级）
- ✅ 嵌套冒泡指纹判断（外层 cp 不重复写 cpKeyInterrupts）
- ✅ cleanupOnSuccess（session 完成后 cp 清理）
- ✅ 跨 ns PendingNSs 推导（外层节点能跨嵌套命中 IsResumeTarget）
- ✅ Events ns 过滤（子图 Resume 时父层 events 不重复回放）
- ✅ Fan-out WithGraphNS 推进（多分支孙图 cp 不互相覆盖）
- ✅ Resume Session 共享（嵌套子图 events 写父 sess.History）

**未完成事项**（推迟原因明确）：
- Phase 4 全部项 → 业务踩到或 PG/Redis 接入时实施（详见 Phase 4 推迟决策）

详细审核 / 修复历程见 git commit 历史 `git log --grep "ISSUE-70"`。


## 背景

当前 Checkpointer 单 sessionID key 模型在多场景暴露不足：

1. **嵌套 cp 覆写**（用户最初触发问题）：PA/RA/Plan + 父编排嵌套时，内层
   InterruptError 写 `cp[sessionID]`；外层 graphLoop 的 `saveCheckpointOnError`
   覆盖。详见 CLAUDE.md "编排 HITL 矩阵 · 剩余 gap"。
2. **cp 体积膨胀**：父 cp 嵌入子 cp 整树（`subgraphCheckpoints` JSON 内嵌），
   嵌套深时 MB 级。
3. **invocation 硬编码 1**：所有 `MakeInterruptID(...,1)`，LoopAgent 内并发
   ask_user 冲突隐患。
4. **`latestCheckpoint` List 末尾 bug**：`compose/subgraph.go:133` 用
   `cp.List(ctx)` 取末尾，多次嵌套 ask_user Resume 错位。
5. **`ResumeContext.UserInput` 单值无 ID 漏洞**：子图无法判断"这个 UserInput
   是给我吗"，靠 `ClearResumeContext` 反向 strip 兜底。
6. **长超步崩溃全丢**：`saveSuperstepCheckpoint` 单点 Save，崩溃前所有分支
   结果丢失。
7. **typed state 无 schema 演进**：改字段所有旧 cp 全废。
8. **ReactiveCompact 无并发串行化**：与 AutoCompact `callMu` 不对称。
9. **SelfHistoryCompacted 跨 ns 倒扫 marker 失效风险**（Bug T 复发）。
10. **手动 `GetResumeContext` 转发**：3 处 orchestrator 重复模板代码 +
    `ClearResumeContext` 反向 strip。
11. **MicroCompact 修改 Messages 不更新 Events**（by design 但未文档化）。
12. **Memory.Write InterruptError 路径不写**（业务侧需自决）。<!-- Memory API 已由 store 体系替代（PR 3.3 commit e6505f9）：Memory.Write → store.Put / Memory.Read → store.Search；本节描述保留历史叙述 -->
13. **fnv64a 64 位 hash**（24h 长跑后才需要升级）。
14. **`RegisterType` 不走 stateFactory 默认值**（语义差异未文档）。

本 issue 的目标：用**最优设计**一次性解决以上 14 项不足，对齐三方
（LangGraph / eino / Claude Code）的设计精华，同时保留我们已经做得更好的
设计不动。

## 方法论：方案演进的 5 轮迭代

| 轮次 | 方案 | 推翻原因 |
|---|---|---|
| v1 | 抄 LangGraph namespace 物理分离 | 上轮交叉审核找到 5 大致命缺陷（C1 superstep 入口快照不能删 / C2 List(sid) O(N) / H3 Loop iter 死循环 / H4 ns 解析无防御 / H5 UserInput 兼容路径断） |
| v2 | 综合 LangGraph + eino + 自创 SaveBatch | 第 2 轮交叉审核 4 agents 推翻 5 大纠正中 3 条（task counter / CheckpointMap value / ClearResumeContext）+ 揪出 8 个 lifecycle 黑洞（D1-D8） |
| v3 | 修正方案 + 成功即清生命周期 | 8 个 lifecycle 黑洞中 4 个不是真 bug（合理设计），4 个真 bug 同根因（child 成功后无人删自己 cp） |
| v4 | 融合方案——以我们设计为本 | 用户校正"借鉴 ≠ 照抄"，以我们设计为本吸收三方概念 |
| **v5** | **完美最终版** | 重新过 46 个审核发现，把遗漏的 5 项精华 / 3 项真 bug / 8 项隐藏不变量 / 3 项设计选择 / 2 项性能优化全补上 |

## 三方借鉴矩阵（精华 vs 糟粕）

### 真精华，借鉴融合（不照抄）

| # | 来源 | 概念 | 我们怎么融合 |
|---|---|---|---|
| 1 | LangGraph | 位置寻址（thread_id, ns, cp_id 三元组） | (sessionID, ns) 二元组 + 不引入 cp_id 多版本（无 time-travel 需求） |
| 2 | LangGraph | 拆分存储（子图独立 cp） | (sessionID, ns) 物理分离 + 父 cp 仅引用 ns（不嵌入整树） |
| 3 | LangGraph | 位置稳定 interrupt_id（hash(ns)） | 保留 `MakeInterruptID(runPath, nodeID, Round, toolCallID)`，比 hash(ns) 更精细 |
| 4 | LangGraph | per-task 持久化（put_writes 即时刷盘） | `SaveBatch` 一次原子写多 cp（实现细节，不破坏 GraphState 模型） |
| 5 | eino | InterruptCtx Parent chain（user-facing 嵌套） | `AskQuery` + `InterruptCtx` 三态分离（持久化扁平 / 业务面链表 / runtime 不需要树） |
| 6 | eino | MigrateCheckpointState 递归迁移 | `StateFactory` 路径加 version + migrator hook（不引入全局 typeRegistry 复杂机制） |
| 7 | eino | ctx 自路由（祖先节点自动标记 resume target） | `IsResumeTarget(ctx)` 简化版用 ns prefix-match（不抄 globalResumeInfo + Used + mutex 全套） |
| 8 | eino | GetNextResumptionPoints（决定下一批跑谁） | `compose.GetPendingChildren(ctx, sid, parentNS)` helper（7 orchestrator 共用） |
| 9 | eino | Address SubID（解 parallel 同名 tool_call） | `MakeInterruptID` 加 toolCallID 后缀，不引入 typed AddressSeg.SubID |
| 10 | 自创 | **成功即清生命周期** | cp 仅在 pending/failed 时存在，成功路径自动 `cleanupNSTree` |

### 确认是糟粕，不抄（10 项）

| 项 | 来源 | 不抄理由 |
|---|---|---|
| `Command(goto, update)` 节点 return 类型 | LangGraph | 破坏 NodeFunc 签名，AgentAction 已 work |
| channel + reducer 模型 | LangGraph | 已论证 ROI 极低（NodeFunc 签名巨变 + 失去 typed state + 强制超步 overhead） |
| `is_time_traveling` 检测 | LangGraph | 4 层嵌套条件脏，不需要 time-travel |
| `Checkpoint` TypedDict 历史包袱 | LangGraph | 我们 from scratch 设计无包袱 |
| `scratchpad.interrupt_counter` 隐式契约 | LangGraph | 我们用 `basicAgentState.Round` 显式持久化更稳定 |
| 多 store/runtime/cache_key/retry_policy 一锅炖 | LangGraph | 我们 ctx 注入分层更干净 |
| `runner` 假设单 interrupt panic | eino | 我们 fan-out 多分支并发，不能这么写 |
| `gob` 序列化（要 RegisterName） | eino | 我们用 JSON，无 gob 反噬包袱 |
| `WorkflowInterruptInfo` 死而不僵（双填 deprecated 字段） | eino | 项目未上线无包袱 |
| `PopulateInterruptState` 每次 ctx 注入 O(N) 遍历 | eino | prefix-match 简化版避免 |

### 我们已偏离三方但更优（保留不动）

| 设计 | 价值 |
|---|---|
| typed state + ProcessState[S] 泛型 | 比 eino mutex shared / LangGraph dict schema-less 更类型安全 |
| `superstep_snapshot` per-branch delta | 入口快照原子 + delta 精准 |
| RunSession + Events first-class | 比 eino 没有 session 概念更利于审计 / SSE |
| Hook 框架级 panic recover (`safeOnNodeEvent`) | LangGraph / eino 都没有 |
| 4 层 Compaction 已落地（ISSUE-65~68） | 已对齐 Claude Code 4 层 |
| `MakeInterruptID(runPath, nodeID, invocation)` 算法 | 位置稳定，比 LangGraph hash(ns) 更精细 |
| sessionID 作 cp 主索引 | 业务侧 API 简洁，ns 作物理消歧 |

## 核心设计

### 1. 数据模型

```go
type Checkpoint struct {
    // 现有字段保留
    ID        string
    Messages  []*schema.Message
    Events    []*AgentEvent       // 仅作 Resume 历史回放，**非真实数据源**
    Metadata  map[string]any
    CreatedAt int64

    // 新增 4 字段
    NS         string             // 自描述（"" = 根；"a/b" = 嵌套；"a/b#0" = fan-out）
    ParentNS   string             // O(1) 父子定位
    Status     CheckpointStatus   // 仅 pending / failed（成功不存 cp）
    SchemaVer  int                // 借鉴 eino migrate
}

type CheckpointStatus int
const (
    CPStatusUnset   CheckpointStatus = iota  // 默认值，向后兼容旧 cp
    CPStatusPending                           // 中断挂起
    CPStatusFailed                            // 业务错（保留供查）
    // **不要 committed —— 成功的根本不存在 cp**
)

type AgentEvent struct {
    // 原有字段
    NS string  // 新增：AddEvent 自动从 ctx 填，跨 ns 排序基础
}
```

### 2. Checkpointer 接口（增不改）

保留所有现有方法签名（旧调用方零感知），追加 ns-aware 方法：

```go
type Checkpointer interface {
    // ── 旧方法保留（默认 ns="" 行为不变） ──
    Save(ctx, *Checkpoint) error                  // 内部转 SaveNS(sid, "")
    Load(ctx, sid string) (*Checkpoint, error)    // 内部转 LoadNS(sid, "")
    List(ctx) ([]string, error)
    Delete(ctx, sid string) error
    Exists(ctx, sid string) (bool, error)
    GetLatestCheckpoint(ctx) (*Checkpoint, error)
    Cleanup(ctx, keepLastN int32) (int32, error)
    GetMetadata(ctx, sid string) (*CheckpointMetadata, error)

    // ── 新增 ns-aware ──
    SaveNS(ctx, sid, ns string, cp *Checkpoint) error
    LoadNS(ctx, sid, ns string) (*Checkpoint, error)
    ExistsNS(ctx, sid, ns string) (bool, error)
    DeleteSubtree(ctx, sid, nsPrefix string) (int32, error)  // 成功即清用
    SaveBatch(ctx, sid string, batch map[string]*Checkpoint) error
    ListPending(ctx, sid string) ([]NSCheckpoint, error)
    LoadAllInSession(ctx, sid string) ([]NSCheckpoint, error) // 业务跨 ns 全景
}

type NSCheckpoint struct {
    NS string
    CP *Checkpoint
}
```

**InMemoryCheckpointer 内部**：`data map[sid]map[ns]*Checkpoint`，per-sid 锁。

### 3. NS 命名规范

```
分隔符:   "/" 父子，"#" fan-out idx
根:       ns = ""
子图:     ns = parent + "/" + nodeID
fan-out:  ns = parent + "/" + nodeID + "#" + index
loop:     不分（成功即清保证 ns 不堆积）
```

**为什么单分隔符**：LangGraph 用 `|`+`:` 双分隔过度复杂，我们单 `/` 够用。
**为什么 loop 不分 iter**：成功即清后，第 N 轮完成 → 删第 N 轮 ns cp →
第 N+1 轮重新创建，无残留。

```go
// compose/ns.go
func JoinNS(parent, segment string) string                    // parent + "/" + segment
func JoinFanoutNS(parent, branchID string, i int) string      // parent + "/" + branchID + "#" + i
func SplitNS(ns string) ([]string, error)                     // **return error**（修 H4）
func ValidateNodeIDForNS(id string) error                     // GraphBuilder 校验：拒绝含 / # 的 id
```

**强校验**：所有内部生成 nodeID（`branch:%s#%d` 的 `%s`）也必须 escape，否则
fail-fast。

### 4. 成功即清生命周期（v3 自创核心机制）

```go
// graphLoop / runBatch / runStepAgent 末段统一逻辑
defer func() {
    if cp := GetCheckpointer(ctx); cp != nil {
        if runErr == nil {
            // 成功即清：删自己 ns cp + 所有 child ns（递归）
            cp.DeleteSubtree(ctx, sid, currentNS)
        }
        // pending/failed 保留 cp
    }
}()
```

**为什么是自创**：结合 LangGraph 的 task lifecycle 思想（pending_writes
apply_writes + clear）+ 我们自己的"cp 是异常状态快照"语义，得到的简洁规则。

**消解的 lifecycle 黑洞**：
- D3 (Loop iter)：第 N 轮完成 → 框架删第 N 轮所有 child ns → 第 N+1 轮重新写 → 无残留
- D5 (串行兄弟)：actor 成功 return → 框架删 actor ns cp → reflector 跑时父 cp 已不含 actor
- D7 (same-ns 重入)：每次重入是新生命周期，前一次成功就清，不存在覆盖语义模糊
- D8 (Hop 切换)：A 跑完转 B → 框架删 A ns cp → 永远只有当前 hop 一个 cp

### 5. ResumeRouting + ctx 自路由（简化版）

```go
type ResumeRouting struct {
    PendingNSs map[string]bool        // 仅 pending 的 ns（成功即清后保证集合干净）
    Answers    map[string]*Message    // ID → 答案
}

// 节点入口自检（prefix-match，替代 eino 全 mutex map）
func IsResumeTarget(ctx, currentNS) bool {
    r := ctx.Value(...)
    for ns := range r.PendingNSs {
        if ns == currentNS || strings.HasPrefix(ns, currentNS+"/") {
            return true
        }
    }
    return false
}

func GetResumeAnswerForToolCall(ctx, toolCallID) (*Message, bool)
func GetResumeAnswerSingle(ctx) (*Message, bool)  // 单 pending 便利 API
```

**为什么不抄 eino globalResumeInfo + Used + mutex 全套**：成功即清后
`PendingNSs` 自然只含真实 pending，不需要复杂 used 标记。

### 6. interrupt_id 改造（用 Round + toolCallID）

```go
// 上版：MakeInterruptID(runPath, nodeID, invocation int)
// 修正：同 Round 多 tool_call 仍冲突 → 加 toolCallID 后缀
func MakeInterruptID(runPath []RunStep, nodeID string, round int, toolCallID string) string
```

**借鉴**：LangGraph "稳定位置 ID" + eino Address SubID。
**实现**：用我们已有的 `basicAgentState.Round`（已显式持久化）+ tool_call_id，
零新机制。

### 7. NestableAgent 接口（保持简单）

```go
type NestableAgent interface {
    Agent
    RunNested(ctx, ownNS string, sess *RunSession, in *schema.Message) error
}
```

**关键决定**：**不**加 lifecycle 钩子（`OnChildCommitted` 等）。成功即清是
框架自动行为，编排不感知。

**实现方扩展（Phase 2）**：BasicAgent (已有) + ParallelAgent + RoutingAgent
+ PlanAgent + SequentialAgent + LoopAgent + ReflectAgent + HandoffAgent。

**调用方责任**：父 orchestrator step 闭包先 `GetGraphNS(ctx)` 拿 parentNS 再
`JoinNS` 拼 ownNS，**调用方负责拼好**——RunNested 内部 fail-fast 校验。

### 8. InterruptCtx 三态分离（取 eino 设计）

```go
// 持久化态：扁平（避免 JSON 循环）
type AskQuery struct {
    InterruptID string
    NS          string             // 新增
    Question    string
    // ... 不带 Parent 指针
}

// 业务面：链表（GetPending API 返回，业务遍历嵌套上下文用）
type InterruptCtx struct {
    ID       string
    Address  []AddressSeg          // typed
    Query    *AskQuery
    Parent   *InterruptCtx         // value 重建（不是 identity）
}

// runtime 不需要 InterruptSignal 树（我们 ns 物理分离不需要这层中介）
```

**业务侧用 EqualsWithoutID 判同棵子树**，不用 `==`（指针 identity 不可靠）。

### 9. typed state migrate（StateFactory 路径加 hook）

```go
type StateMigrator[S any] func(oldJSON []byte, oldVer int) (newState *S, newVer int, err error)

func NewStateFactory[S any](
    factory func(ctx) *S,
    currentVer int,
    migrators ...StateMigrator[S],
) StateFactory
```

**借鉴**：eino MigrateCheckpointState 概念。
**实现**：附在已有 StateFactory 上，不引入全局 typeRegistry。

**关键约束**（来自审核 #36）：必须用 StateFactory 路径，不走 `RegisterType`
路径——后者 unmarshal 时不调用 factory 默认值，用户自定义 init 逻辑会失效。

### 10. SelfHistoryByNS（避免 Bug T 复发）

```go
// 不破坏现有 SelfHistory(name) 签名
// 追加：
func (s *RunSession) SelfHistoryByNS(ns string) []*Message
func (s *RunSession) SelfHistoryCompactedByNS(ns string) []*Message
```

**为什么**：当前 `SelfHistoryCompacted(name)` 倒扫 owner 自己的消息找 marker。
ns 化后 owner ns 与产生 summary 的 ns 不同 → 倒扫找不到 marker → 退化全量
SelfHistory，**直接破坏 ISSUE-69 Bug T 的修复**。

### 11. 嵌套 cp 协议升级（保留嵌入语义但加 ns）

```go
type NestedRefMode int
const (
    NestedRefByNS NestedRefMode = iota  // 默认：父 cp 仅存 child ns 引用列表
    NestedRefInline                      // 可选：父 cp 嵌入 child JSON（小场景兼容）
)
```

业务可选——小场景（嵌套 ≤ 2 层）可继续用 inline 保留原子性；大场景默认
ByNS 控体积。

### 12. SubgraphNodeOption 补全

现有 4 个不变：`WithSubgraphInput / Output / IsolatedSession / WithoutHookPropagation`。
新增 1 个：

```go
func WithIsolatedCheckpointStore() SubgraphNodeOption  // 与 IsolatedSession 正交
```

**为什么正交**：当前 `WithIsolatedSession` 只控 session 隔离（events 流）；
ns 化后 cp 隔离是另一维度——业务可能要 events 隔离但 cp 共享，或反之。

### 13. OnNodeEvent API 升级

```go
// 当前
OnNodeEvent(ctx, nodeID, event)

// 升级（NS 头等概念）
OnNodeEvent(ctx, ns, nodeID, event)
```

破坏性 API 改动，但 ns 是头等概念不能藏。前端 SSE 可按 ns 维度聚合
不同子图的事件流。

### 14. ResumeFullSuperstep mode 在 ns 化下的语义

```go
// ResumeDirected (默认): 仅重跑 pending child ns
// ResumeFullSuperstep:   删整个 superstep 内所有 child ns（DeleteSubtree），
//                         整个 superstep 从父 entry snapshot 重跑
```

实现：`runGraphFromSuperstep` 在 FullSuperstep mode 下先调
`DeleteSubtree(sid, supersteParentNS)` 清子 ns，再走超步重跑路径。

### 15. ResumeGraph 回放 cp.Events 仅本 ns

修复审核 #34：当前 `ResumeGraph` 启动前回放 cp.Events，nodeID 取
`nodeIDFromEvent(ev)`——子 ns events 在父 Resume 时被回放，前端看到孤立的
子 nodeID。

修复方向：ResumeGraph 仅回放本 ns 的 events，子 ns events 由子 ns Resume
时各自回放（ns 隔离的天然好处）。

## 隐藏不变量清单（必须文档化到 doc/description/checkpointer-invariants.md）

<!-- 下面 1-2 描述的 Memory API 已由 store 体系替代（PR 3.3 commit e6505f9）：
     Memory.Read → store.Search / Memory.Write → store.Put / WithMemory → WithStore + WithStoreMemory；
     不变量本身（解耦 sid + 中断分支不写）在 store 体系下等价保留，仅 API 名变 -->
1. **Memory 与 sessionID 解耦**：`Memory.Read` 接口签名无 sid 参数，业务跨
   sid 复用同一 Memory 实例。Cleanup 跨 sid 不动 Memory 数据。文档明示
   "Cleanup 不会清 Memory；业务自决跨 sid 历史保留策略"。
2. **Memory.Write 在 InterruptError 路径不触发**：中断分支 Memory 不写入。
   业务想保留中断历史需自行调 Resume 后再 Write。
3. **inputAdapter Resume 路径不调用，outputAdapter 仍调用**：
   `subgraph_node.go` 已有不变量，ns 化后保持。
4. **`cloneStateForFanOut` 默认共享 `*schema.Message` + LastOutput**：
   分支 NodeFunc 不准 mutate 指向对象，只能赋新指针。
5. **MicroCompact 修改 `state.Messages` 不更新 `session.Events`**：
   审计视图（events 原文）vs LLM input 视图（占位符）差异是 by design
   （ISSUE-44 SelfHistory 同思路）。
6. **AutoCompactSummaryAnnotation 跨 Run 复用按 owner 名字索引**：
   两个不同 ns 的同名 BasicAgent 会相互看见 summary。业务复用 agent 配置
   时需注意。
7. **`UnmarshalTagged` 不调用 stateFactory 默认值**：与 stateFactory 路径
   语义差异。typed state migrate 必须用 StateFactory 路径。
8. **CircuitBreaker 跨 ns 共享**：BasicAgent 实例字段，多 ns 同实例共享同
   一 breaker。fan-out 共享 Model 时 thunder herd 已有 mu 保护
   （ISSUE-67/68）。
9. **events 仍存 RunSession 单 slice，cp.Events 仅作 Resume 历史回放**：
   `cp.Events` **不是真实数据源**，sess.Events() 才是。RestoreEvents 仅在
   Resume 启动时灌回 root.events。
10. **chunk events 在 AddEvent 入口过滤**：`if e.Action != nil &&
    e.Action.IsChunk { return }`，防 SSE 长流爆 sess.Events。
11. **每事件加 NS 字段**：AddEvent 自动从 ctx 填。SelfHistoryByNS 按 ns
    prefix 匹配。
12. **同 ns 任意时刻最多 1 个 pending cp**：成功即清的不变量。
13. **NestableAgent.RunNested 调用方负责拼 ownNS**：内部 fail-fast 校验
    `if ownNS == "" || !IsValidNS(ownNS) return error`。

## 不抄的边界（明确不做）

- 不引入 cp_id 多版本 / time-travel（`Checkpoint.ID` 字段保留以备未来）
- 不业务侧暴露 string ns（业务只见 typed Address）
- 不 ns 分 loop iteration（成功即清替代）
- 不分布式 ns 锁 / ns 级 ACL / ns rename
- 不 cp 跨 ns 引用（每个 ns cp 自包含）
- 不引入 channel + reducer 模型
- 不破坏 NodeFunc 签名（不抄 LangGraph Command 节点 return）

## Phase 实施计划

### Phase 1: 接口铺路 + 真 bug 修复（行为不变）

**改动**：
- `compose/session.go`：Checkpointer 加 ns-aware 方法（旧方法默认 ns=""）
  + `LoadAllInSession` + `ListPending` + `DeleteSubtree` + `SaveBatch`
- `compose/event.go`：Checkpoint 加 NS / ParentNS / Status / SchemaVer
- `compose/event.go`：AgentEvent 加 NS 字段
- `compose/session.go`：AddEvent 自动填 NS + chunk 过滤
- `compose/resume.go`：删 ResumeContext.UserInput + ClearResumeContext
- `compose/subgraph.go`：修 latestCheckpoint List 末尾 bug → 用 LoadNS 精确寻址
- `compose/runpath.go`：MakeInterruptID 加 toolCallID 参数
- `agent/react.go`：MakeInterruptID 调用点用 `basicAgentState.Round`
- `agent/reactive_compact.go`：加 mu（与 AutoCompact 对齐）
- `compose/checkpoint.go`：saveCheckpointOnError 写 Status=pending
- `compose/checkpoint.go`：cp.Events 序列化时过滤 IsChunk
- `doc/description/checkpointer-invariants.md`：13 个不变量文档

**验收**：所有现有测试改完后绿，仅签名变化无行为变化。

**工作量**：3-4 天

### Phase 2: ns 物理分离 + 7 orchestrator NestableAgent + 成功即清（强绑定）

**改动**：
- 新建 `compose/ns.go`：JoinNS / JoinFanoutNS / SplitNS / ValidateNodeIDForNS
- 新建 `compose/ns_ctx.go`：WithGraphNS / GetGraphNS / WithCurrentNS / GetCurrentNS
- 新建 `compose/resume_routing.go`：ResumeRouting + IsResumeTarget +
  GetResumeAnswerForToolCall + GetResumeAnswerSingle
- 新建 `compose/cleanup.go`：cleanupNSTree（成功即清核心逻辑）
- `compose/builder.go`：GraphBuilder.Build 校验 nodeID（含内部生成路径）
- `agent/basicagent_graph.go`：删 NewInMemoryCheckpointer 闭包，改 ns 派生 +
  共享父 cp store
- `agent/parallel.go` `agent/routing.go` `agent/plan.go` `agent/sequential.go`
  `agent/loop.go` `agent/reflect.go` `agent/handoff.go`：各自实现
  NestableAgent + step 闭包识别
- ~~`compose/superstep.go`：saveSuperstepCheckpoint 改 SaveBatch 一次原子写
  entry snapshot + 各 child ns cp~~ — **推迟到 Phase 4 PG/Redis 接入时一并实现**
  ([见决策](#step-iii-推迟决策))
- `compose/checkpoint.go`：graphLoop / runBatch 末段加 cleanupNSTree 逻辑
- `compose/checkpoint.go`：ResumeFullSuperstep mode 实现 DeleteSubtree + 重跑
- `compose/checkpoint.go`：ResumeGraph 回放仅本 ns 的 events
- `compose/subgraph_node.go`：加 WithIsolatedCheckpointStore SubgraphNodeOption
- 新建 `compose/get_pending.go`：GetPendingChildren helper
- 删 `compose/graph.go` 的 subgraphCheckpoints 字段 + Set/Get 方法
- 删 `compose/checkpoint.go` 的 cpKeySubgraphCheckpoints 常量 +
  序列化 / restore / merge 路径
- 同步改：subgraph_test / subgraph_node_test / subgraph_node_combo_test /
  fanout_resume_test / superstep_test / orchestrators_hitl_test

**验收**：
- 所有现有 cp / Resume 测试改完绿
- 新增"3 层嵌套独立 cp"测试
- 新增"loop 重入 ns 不冲突"测试
- 新增"嵌套 HITL 矩阵 PA-in-Seq / RA-in-Loop / Plan-in-Reflect / Plan-in-Plan
  / Seq-in-Par"

**工作量**：6-8 天

### Phase 3: typed Address + InterruptCtx 三态 + OnNodeEvent NS 升级

**改动**：
- 新建 `compose/address.go`：AddressSeg / AddressKind / addressFromNS / nsFromAddress
- 新建 `compose/interrupt_ctx.go`：InterruptCtx with Parent chain（重建版）
- `compose/checkpoint.go`：AskQuery 加 NS 字段
- `compose/orch_hooks.go`：OnNodeEvent API 加 ns 维度
- `compose/event_sink.go`：sink 透传 ns 给 hook
- 业务 Resume API 错误返回升级 typed
- `example/orchestration/plan_chat_web/hooks.go`：SSE adapter 加 ns 字段

**验收**：3 层嵌套场景 Address 链类型正确 + 业务测试不见 string ns。

**工作量**：2 天

### Phase 4: StateFactory migrate + SelfHistoryByNS + 性能优化预留 — **推迟**

**原计划改动**：
- ~~`compose/state.go`：StateFactory 加 version + migrator~~ → 业务踩到 schema 演进时做
- ~~`compose/migrate.go`：Migrate hook 链式调用~~ → 同上
- ~~`compose/session.go`：SelfHistoryByNS / SelfHistoryCompactedByNS~~ → 业务踩到"按 ns 切片事件"时做
- `compose/session.go`：LoadAllInSession 业务 API 实现 ✅ 已做（Step Ⅰ/Ⅳ）
- 性能优化预留点（Events sort / ListPending 索引接口已留）→ PG/Redis 接入时做
- ~~Step Ⅲ SaveBatch 真原子~~ → PG/Redis 接入时做（已落决策记录）

**推迟理由**（[见决策](#phase-4-推迟决策)）：

1. StateFactory migrate / SelfHistoryByNS 都是"将来踩到再做"的预留——按 CLAUDE.md
   "项目未上线、不为不存在场景写代码"原则不该提前加
2. 两者都是**纯增量 API**——后加零破坏性，老用户不调即不变
3. 接口设计简单（`WithMigrator(fromV, fn)` / `SelfHistoryByNS(ns)`），后加时
   设计错的概率低
4. 死代码维护成本 + 脑补场景设计错风险 > 提前加的边际收益

**实际触发条件**：
- StateFactory migrate：业务 typed state schema 真演进 + 长 cp 不能丢弃时
- SelfHistoryByNS：业务出现"同名 agent 在不同嵌套层"或"按 ns 子树审计"需求时
- 性能 / SaveBatch：PG/Redis 后端接入时

**总工作量：13-16 工作日**

实际工作量节流（按 ISSUE-70 实施完结时统计）：
- Phase 1 + Phase 2 + Phase 3：~10 工作日
- Phase 4 推迟项：未来按需 0.5-1 天做完
- typed Address + InterruptCtx 三态：决策不抄，节省 2 工作日
- 总实际花费：**~10 工作日**（vs 原计划 13-16）

## 风险与不变量

### 必须保留的 8 个核心不变量

1. **超步入口快照原子性**：PreMsgLen / PreLastOutput / PreLocalState /
   Frontier 同一时刻读取（superstep.go:136-138）
2. **分支 rerun 幂等性**：同一物理位置 ns 跨 retry/Resume 稳定
3. **`hasNestedSubgraphCheckpoint` 抑制语义**：嵌套冒泡时本层不写
   cpKeyInterrupts（ISSUE-59 P0 #1 修复）—— ns 化后等价语义保持
4. **`appendResumeMessages` 兄弟分支 silent drop**（checkpoint.go:521-524）
5. **`saveCheckpointOnError` 同 ID 覆盖 + 静默失败**（`_ = cp.Save`）
6. **typed state + ProcessState 隔离性**
7. **cp 主线程 wg.Wait 后串行 Save**：fan-out 分支不直接 cp.Save，写到
   parentState 后单线程序列化
8. **倒扫 tool_call 配对**：避免 LLM 重放幂等性陷阱

### 已知风险

| 风险 | 应对 |
|---|---|
| C1 修复后 entry snapshot 与子 ns cp 同步不一致 | SaveBatch 父 entry + 所有子 ns 一次原子写 |
| Phase 2 强绑定改动量大 | Phase 2 内部按"基础设施 → BasicAgent → 7 orchestrator → ResumeRouting"渐进，但同 PR 上线 |
| ns 字符冲突 | GraphBuilder.Build 校验，含特殊字符 fail-fast |
| 跨 ns 一致性（非内存后端） | SaveBatch 接口 + 后端能力文档明示 |
| 现有 13 个集成测试回归 | 每 Phase commit 必须 `go test ./...` 全过 |

### 工作量风险

- 上轮估时 5-7 天 → 第 1 轮审核后 14-19 天 → 修正方案 13-17 天 → 完美最终版
  13-16 天。
- 每一轮收敛因为：(a) 把"伪简化"剔除（task pending_writes 不抄）；(b) 把
  lifecycle 黑洞用"成功即清"统一消解；(c) 不抄不必要的复杂度。

## 实施前还需用户决策的项

- [ ] 方案最终版是否接受？
- [ ] Phase 划分是否调整？
- [ ] 落地时机（用户思考后决定）

## 配套交叉审核记录（4 轮，11 个 agent）

每轮审核都按 CLAUDE.md "9 类 bug 框架" + "独立 agent 复审" 流程：

| 轮次 | Agents | 主要发现 |
|---|---|---|
| 1 | 3 个（LangGraph 真相 / eino 真相 / 当前方案漏洞） | 5 大致命缺陷 + 三方真相纠正 6 处 |
| 2 | 4 个（5 大纠正复审 / 3 角度未审 / 6 细节 / e2e 嵌套场景） | 推翻 3 大纠正 + 8 个 lifecycle 黑洞（D1-D8） |
| 3 | 思维框架校正（用户反馈"借鉴 ≠ 照抄"） | 推翻"修正方案 C 仍偏抄"，转向以我们设计为本 |
| 4 | 完美最终版（重新过 46 个发现） | 补 5 项遗漏精华 + 3 项真 bug + 8 项隐藏不变量 + 3 项设计选择 + 2 项性能优化 |

详细审核记录见 git commit 历史（搜索 "ISSUE-70"）。

## 决策记录

### Phase 4 推迟决策

**原计划**：StateFactory migrate + SelfHistoryByNS + 性能优化预留 + Step Ⅲ
SaveBatch 真原子。

**全部推迟到"业务真踩到"或"PG/Redis 接入"时实施**。

**理由**：

1. **StateFactory migrate / SelfHistoryByNS 是预留，不是当前需求**
   - StateFactory migrate：typed state schema 演进时旧 cp 自动迁移。但项目未上线、
     PlanAgent / LoopAgent / ReflectAgent 等内部 typed state 设计稳定，没有
     "长 cp + schema 演进"组合场景
   - SelfHistoryByNS：按 ns 维度切片事件流。当前 `SelfHistory(name)` 按 agent
     name 过滤已能 work（PlanAgent 内部 agent 名区分明确），加 NS 维度纯属
     另一种切片角度，没业务实际需要

2. **两者都是纯增量 API——后加零破坏性**
   - `WithMigrator(fromV, fn)` / `SelfHistoryByNS(ns)` 都不修改老接口
   - 老用户不调即不变，业务真踩到再加完全不影响已有功能

3. **性能优化 + Step Ⅲ SaveBatch 跟 PG/Redis 后端绑定**
   - 内存索引 / 内存事务都是"伪原子"——真价值在 PG `BEGIN/COMMIT` 或 Redis Lua
   - InMemoryCheckpointer 持锁全程已"近原子" + 全扫已够性能
   - PG/Redis 接入时一并做更经济（接口已留，实现可推迟）

4. **CLAUDE.md "项目未上线、不为不存在场景写代码" 原则**
   - 死代码维护成本 + 脑补场景设计错风险 > 提前加的边际收益
   - 真踩到时实施工作量 0.5-1 天，反正功能不复杂

**实际触发条件**：

| 项 | 触发条件 |
|---|---|
| StateFactory migrate | 业务 typed state schema 演进 + 长 cp 不能丢弃 |
| SelfHistoryByNS | 同名 agent 在不同嵌套层 OR 按 ns 子树审计需求 |
| 性能优化 / Step Ⅲ SaveBatch | PG/Redis 后端接入 |

### Step III 推迟决策

**原计划**（Phase 2）：`saveSuperstepCheckpoint` 改 SaveBatch 一次原子写 entry
snapshot + 各 child ns cp。

**推迟到 Phase 4 PG/Redis 接入时实现**。

**理由**：
- SaveBatch 真正价值 = 多 key 写入的事务原子性（PG `BEGIN/COMMIT`、Redis Lua）
- 当前 `InMemoryCheckpointer.SaveBatch` 实现是"持锁全程 for 循环挨个 SaveNS"，与
  分散调多次 SaveNS 行为完全等价
- 把 superstep 改成 SaveBatch 在 InMemory 场景下**运行时行为零变化**，仅"代码看起来更聚合"
- 改造代价高：`saveCheckpointOnError` 签名要从"立即 Save"改成"返回 cp data"，影响
  graphLoop / runBatch / rerunFailedBranches / saveSuperstepCheckpoint 4 处主路径
- 接入 PG 时改造范围才是必要的：
  - `Checkpointer` 接口显式区分 "atomic batch" vs "individual save" 语义
  - 父框架收集分支 cp data 后 SaveBatch 一次提交
  - 失败语义重新定义（PG 完整 rollback / Redis MULTI 不支持错误回滚要 Lua / SQLite 又不同）
- 这些决策跟具体后端绑——InMemory 提前做反而约束了后端选择，且中间形态价值 = 0

**当前替代实现**：分支 `saveCheckpointOnError` 与父 `saveSuperstepCheckpoint` 都用
SaveNS 写到独立 ns 路径。即使中间某分支 cp 写失败，父 cp 仍持有完整 snapshot.Branches
（Resume 时 ListPending 能找到 pending → rerun 该分支）—— 分支独立写不破坏整体一致性。

**触发条件**：Phase 4 实现 `PostgresCheckpointer` / `RedisCheckpointer` 时同步实现。
单纯 InMemory 永远不需要。

### 方案 A vs B vs C 之争（已选 C+）

- **方案 A**（嵌套 stack 最小修复，3-5 天）：被否决——用户明确"不要临时方案要最优方案"
- **方案 B**（eino-style SubGraphs 整树，6-8 天）：被否决——cp 体积膨胀长期问题，不解决多租户/分布式未来需求
- **方案 C**（namespace 完整化）：选定，但**加成功即清生命周期消解 lifecycle 黑洞**，最终为 13-16 天

### 为什么不抄 channel + reducer

详见对话记录"5 个核心理由"——简言之：我们没 channel reducer 真正解决的问题
（多源并发写同一字段的自动合并），typed state + ProcessState + ParallelMerger
已对 LLM agent 场景足够。强制超步模型 overhead + NodeFunc 签名巨变 + 失去
typed state 类型安全，ROI 极低。

### 为什么用 sessionID + ns 二元而非完整 namespace

LangGraph 是 (thread_id, ns, cp_id) 三元（ns 字符串 + cp_id 多版本支持
time-travel）。我们：
- 保留 sessionID 作业务侧主索引（API 简洁）
- ns 作物理消歧（解决嵌套覆写 + 体积膨胀）
- 不引入 cp_id 多版本（无 time-travel 需求，cp.ID 字段保留以备未来）

### 为什么删 ResumeContext.UserInput 字段

不是兼容字段，是**真设计漏洞**——单值无 ID 通道让子图无法判断"给我吗"，
靠 ClearResumeContext 反向 strip 兜底。删除后所有 Resume 走 UserInputs map
+ 按 InterruptID 路由，统一干净。项目未上线无兼容包袱。
