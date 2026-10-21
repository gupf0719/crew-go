# ISSUE-93 · HandoffAgent 模型 B audit 遗留 3 LOW

> **状态**：📋 记录跟踪（2026-06-05）
> **来源**：HandoffAgent 模型 B 阶段 2（历史可见性 WithHandoffHistoryMode）提交后，
> spawn 4 个独立单视角 audit agent 并发审 + worktree 隔离 cross-verify。
> **已修（同批）**：doc/godoc stale（commit `6bc28bf`）+ MED 级 pending key 碰撞
> （`34a0c87`）+ active store 跨实例污染（`92f9078`）已根因修复。本文件记录**决定暂不修**
> 的 3 个 LOW（理由各异）+ 1 个 by-design 已知风险。

## 背景

阶段 2 audit 4 视角（BasicAgent prior 回归 / stepFn 持久 active / History API / 文档准确性）
并发审 + 2 个独立 cross-verify agent 复现。已修项见上。本文件 3 个 LOW 经评估暂不修。

---

## 遗留① · historyMode ctx-value 泄漏到非 handoff 编排内层 BasicAgent（视角 1）

`HandoffAgent.stepFn` 经 `WithHandoffHistoryModeCtx(subCtx, h.historyMode)` push historyMode
给 active sub-agent。若 active sub-agent 是**非 handoff 编排**（Sequential/Parallel/Loop/
Routing/Reflect/Plan/Graph）包 BasicAgent，这些编排调子 agent 时不清除该 ctx-value →
内层 BasicAgent `GetHandoffHistoryMode(ctx)` 拿到泄漏的 Rewrite/Raw → 走 HistoryFor/
HistoryForRaw（全量改写视图）而非预期的 SelfHistoryCompacted（自己历史）。

**影响**：LOW。信息可见性扩大（内层 BasicAgent 看到 handoff 全链改写历史），**不丢数据、
不破协议、不 crash**（input 是 user role，HistoryFor/HistoryForRaw 保留）。

**当前未触发**：全仓无"非 handoff 编排作 handoff 候选"的代码/测试用例。

**为何暂不修**：根治需在全部 7 个非 handoff 编排的 sub-agent push 点复位 mode 为 SelfOnly
（对齐 component-kind "consume 清除" 范式），churn 大 + 四复证风险（CLAUDE.md 协议变更四复证
教训）。LOW + 未触发，收益不抵 churn。已在 `WithHandoffHistoryMode` option godoc 注明此已知行为。

**修法（将来有用例时）**：方案 A——7 编排 stepFn push subCtx 时
`WithHandoffHistoryModeCtx(subCtx, HandoffHistorySelfOnly)` 复位，使 mode 只作用 HandoffAgent
直接调的一层。grep checklist：`grep -nE "WithSubAgentStoreScope\(subCtx" agent_v2/*.go` 对账
全部 push 点（漏一个即四复证下一轮）。

---

## 遗留② · SelfHistoryCompacted cutoff 扫描实际 dead（既有，非 handoff 引入）（视角 3）

`SelfHistoryCompacted` 倒扫找 `CompactionSummaryAnnotationKey` marker 决定 cutoff。但该 marker
只在 `autocompact.go` 的 in-graph summary 消息上写，该 summary **从不 AddEvent 进
session.events**（AutoCompact 只改 channel）。故 cutoff 永远 -1 → `SelfHistoryCompacted`
退化**等同 SelfHistory**（返 owner 全量）。

**影响**：LOW。godoc 承诺的"跨 Run 复用 session 避免重新 AutoCompact"优化无法兑现（退化但
不报错，godoc 自己也说"找不到 marker → 等同 SelfHistory"）。handoff SelfOnly 模式用
SelfHistoryCompacted，但因退化等同 SelfHistory（返自己全量），对 handoff 行为**无影响**。

**为何暂不修**：既有设计（非 handoff 引入），影响仅"优化失效不报错"。根治需让 AutoCompact
summary AddEvent 带 marker 进 events（涉及 AutoCompact + session 协议），独立评估。

---

## 遗留③ · ~~consumePendingTransfer >64 tool_call 静默丢转交~~（视角 2）— 已随载体更换消失

原文：`consumePendingTransfer` 固定扫 `idx ∈ [0, 64)`（maxToolCallsGuard=64），LLM 单轮 emit
>64 个并行 tool_call 且 transfer 落在 idx≥64 时永远扫不到 → 静默丢转交。

**现状**：转交信号改走 ctx 上的一次性收件槽（`handoffTransferSlot`）之后这条**不存在了**——
槽是一张 map，写多少读多少，没有上界扫描这回事。裁决仍按下标最小者（`take`），跟原语义一致。
换载体的动机是另外四类泄漏（跨 Run / 跨任务 / 跨重试 / 孙辈越权），本条是顺带消失的，
见 [handoff.md §6.3](../v2/handoff.md)。

---

## by-design 已知风险（非遗留 bug）· HandoffHistoryRaw orphan tool_call（视角 3 F1）

`HandoffHistoryRaw`（opt-in）保 role 把前接 transfer tool_call 原样传后接，后接 toolkit 无此
工具 → 真实 API 400。**这是 Raw 的已知约束**（`handoff_tool.go` const godoc + handoff.md §9
均明确"Raw 仅适合前接无 tool_call 的纯对话场景"），默认 Rewrite 已避开。不是遗留 bug——是
opt-in 模式的文档化前提。业务误用 Raw + transfer 链时会 hard fail（无静默错），符合 fail-fast。
