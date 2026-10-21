# ISSUE-71: PA/RA/Plan 实现 NestableAgent — 解锁深嵌套 fan-out HITL

**状态**：✅ 已完成（v1→v2 channel-based 重构期落地, ISSUE-71 接口合并 commit `d2059b2` / `5cbb159` / `4bb5482` / `45f5e04` / `b5a129b` — 删 NestableAgent / RunNested 双接口, 9 编排统一单接口 + ctx graphNS push; 详 issues.md §ISSUE-71）
**触发**：ISSUE-72 5 路并行交叉审计 Audit 4 HIGH-1
**前置**：ISSUE-70 Phase 2（嵌套 cp 物理分离）+ ISSUE-72（cross-audit 9 bug 全修）

## 问题陈述

当前 3 个 fan-out 派生型编排（ParallelAgent / RoutingAgent / PlanAgent）**不实现 NestableAgent 接口**：
- `agent/parallel.go:284`：`var _ Agent = (*ParallelAgent)(nil)` — 仅 Agent
- `agent/routing.go`、`compose/plan.go` 同样

CLAUDE.md HITL 矩阵明确标 ⚠️：作为子 agent 嵌入到外层编排时，走 fallback 普通 `Agent.Run` 路径，**不调 `WithGraphNS` 推进 ns**——内层节点写 cp 时跳过中间 fan-out 层。

## 真实 bug 触发场景

3 层嵌套：`Parallel-A → branch_0 → Parallel-B (作为 inner agent) → branch_1 → BasicAgent.ask_user`

1. Parallel-A `dispatchBranchAgent` 见 Parallel-B 不是 NestableAgent → fallback `retryRunAgentNodeMsg` → 普通 `Parallel-B.Run`
2. Parallel-B.Run 调 `compose.RunGraph`，**不推 GraphNS**
3. 内层 BasicAgent 在 branch_1：`GetGraphNS` 仅含 Parallel-A 一层（`branch_xxx_0#0`），**完全没有 Parallel-B**
4. ask_user 写 `query.NS = JoinNS("branch_xxx_0#0", "branch_yyy_1#1")` — 跳过 Parallel-B 整层
5. 内层 cp 物理位置 `(sid, "branch_xxx_0#0/branch_yyy_1#1/basicagent")` — 与"如果 Parallel-B 是 NestableAgent"应得的 ns 不一致
6. 多个 Parallel-B 实例若同名 branch_yyy → cp 路径碰撞，SaveNS 末尾一个赢

**配套 HIGH-2**：sid 复用导致 cp 写入错位。Parallel-B 用同 sid 写 root cp 到 `(parentSID, "")` 与外层 root 同 ns 路径，**两个 cp 互相覆盖**。

## 当前缓解

CLAUDE.md HITL 矩阵已警告：嵌套 ≥3 层 + fan-out 派生型作为子 agent 嵌入时不要触发 ask_user。具体要求：
- 不要"PlanAgent 内嵌 ParallelAgent 内嵌 BasicAgent.ask_user"组合
- 想嵌套保留 HITL 用 5 个已实现 NestableAgent 的编排（BasicAgent / Sequential / Loop / Reflect / Handoff）

## 改造方向

让 3 个编排实现 NestableAgent 接口，**协议要求**：
- `RunNested(ctx, parentState, parentNodeID, ...)` 入口推进 GraphNS：`compose.WithGraphNS(ctx, JoinNS(GetGraphNS(ctx), parentNodeID))`
- 嵌套 cp 写到 `(parentSID, childGraphNS)` 路径
- Resume 入口用 `cp.LoadNS(parentSID, childGraphNS) + Status==Pending` 判断走 ResumeXxx

**复杂度**：3 个编排各重构 Run 入口 + 引入 NestableAgent 接口实现。预计 200-400 行核心改动 + 全套回归。

**冲突点**：当前内部 `superstep_snapshot` 协议与 ISSUE-70 Phase 2 嵌套 cp 协议互斥（CLAUDE.md 矩阵明确）。改造需要把两个协议统一，或让 fan-out 派生型也支持嵌套 cp 协议（superstep snapshot 写到子 ns 路径）。

## 触发条件

下列**任一**真实需求出现时启动：
1. 业务场景需要 PlanAgent 内嵌 ParallelAgent + BasicAgent.ask_user（典型：复杂多智能体 ReAct 含人工节点）
2. 用户报告 ≥3 层嵌套 + fan-out 派生型场景的 cp 错位
3. 持久化后端（PG / Redis）落地后跨进程 Resume 暴露 ns 错位

## 设计参考

不教条抄三方，但保留借鉴：
- **LangGraph**: 子图作 `add_node(name, pregel)` 任意 Pregel 实例。每个 task 自动获 `CONFIG_KEY_CHECKPOINT_NS = task_checkpoint_ns`（pregel/_algo.py:615），子图 ns 自动派生。**借鉴**：ns 派生由"task 入口框架自动推进"而非"调用方手填 parentNodeID"
- **eino**: `AddGraphNode(key, AnyGraph, opts)` 头等节点，cp 通过 forwardCheckPoint ctx 派生。**借鉴**：保留我们 ns 物理分离 + 父 store 共享的优势，不抄 forwardCheckPoint 一次性 delete 消费的脆弱模式
- **我们**: 5 个 NestableAgent 实现已落地（BasicAgent + Sequential/Loop/Reflect/Handoff），`runOrchestratorNested` 共享 helper（agent/orch_helpers.go）作为成熟参考模板

## 已知不修

`Audit 4 MED-3/4/5/LOW-6` 在 ISSUE-72 已修：
- MED-3 sibling 多层命中（拆 PendingExact）
- MED-4 Directed rerun 精确清子树
- MED-5 InterruptID 嵌套 fan-out 撞车（RunStep 加 BranchIdx）
- LOW-6 DeleteSubtree ns 边界（实现已正确）

PA/RA/Plan 嵌套问题是**架构层级缺失**，独立处理；不阻塞当前 9 bug 修复落地。

## OKR 排期

待 Phase 4 持久化后端启动 + 实际业务出现深嵌套需求时再纳入。当前未上线，时间盒不合适提前做。
