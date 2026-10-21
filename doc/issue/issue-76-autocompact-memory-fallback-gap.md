# ISSUE-76 · AutoCompact 在 Memory-enabled + 单 user input + 多 ReAct 轮场景下静默失效

**优先级**：中（生产长任务场景影响）
**状态**：✅ 已修（2026-05-15）— 方案 A 增强版 allProtectedGroups fallback
**发现轮次**：R29 audit 视角 2 MED-1（2026-05-14）

> **API 变更说明**（PR 3.3 commit e6505f9）：本 issue 标题含 "Memory" 是历史 API 名——Memory 接口已由 store
> 体系替代：`Memory.Read` → `store.Search` / `WithMemory` → `WithStore` + `WithStoreMemory` /
> `MemoryInjectedAnnotation` → `StoreInjectedAnnotation`。本文档保留作历史档案，文件名 / 标题不动；
> AutoCompact 算法 fix 在 store 体系下等价保留（store 注入 message 同样按 marker 跳过摘要）。

## 问题

`agent_v2/autocompact.go::lastUserGroupIndex` (line 528) + `AutoCompactRun` (line 301-308) 算法假设：
- 单 user + 多 agent 轮（典型 ReAct）场景下 user 在 group 0
- 若 `lastUserGroupIndex` 返 0/-1 → 走 fallback 路径 `tailStartIdx = len(groups) - 1`
- early = 所有"前面的 ReAct 轮"，AutoCompact 摘要 early，保留 tail（最后一轮）

**实际**：buildInitialMessages 注入 Memory.Read 的消息会插在 `[sys] ↔ [UserInput]` 之间。`GroupMessagesByAPIRound` 把 Memory（无 runID）独立成 group 0；UserInput 落在 group 1。

后果：
1. `lastUserGroupIndex` 返 1（不是 0/-1）→ 不触发 fallback
2. tailStartIdx=1 → earlyGroups=[[mem]]、tailGroups=所有 ReAct 轮次
3. per-msg filter 把 Memory 抽到 memoryInjected → early=[]
4. `if len(early) == 0` 直接返回——**AutoCompact 永不调用 LLM 摘要**

业务影响：
- Run 1 单用户任务跑 50+ ReAct 轮（多步研究 / 长链 tool_call）时 AutoCompact 完全失效
- 所有压力压到第 3 层 ReactiveCompact 兜底——emergencyDrop 优先丢含 UserInput 的 group → LLM 后续看不到原始用户提问 → 多轮链条断点
- 用户体验"agent 突然忘了我让它干嘛"

注释 line 255-256 写"单 user 历史 + 多 agent 轮（BasicAgent 典型）：每个 LLM 调用一组，tail = 最近一组，early = 之前所有组——能正确触发摘要"——**注释承诺与实现不符**（注释落后于"加入 Memory"的设计扩展）。

## 修法（任选一）

### 方案 A：lastUserGroupIndex 加"全 protected group 算 fallback 触发"语义

若 `[0..tailStartIdx)` 内所有 group 都被 splitSkillBodyGroups + per-msg filter 抽空（仅含 memory/skillBody/skillListing marker），视为"effectively user 在第 1 个真实组"→ 走 fallback `tailStartIdx = len(groups) - 1`。

### 方案 B：AutoCompactRun line 345 `if len(early) == 0` 二次尝试

early empty 时检测 tail 是否过大，若 `tail.tokens > threshold` 则按"用最后 1 组作 tail，其余作 early"重算（mem/skill 仍走 per-msg filter 抽出）。

## 当前缓解

R29 audit 视角 2 LOW-1 已让 `shouldTrigger` 跳 MemoryInjectedAnnotation marker（commit `afa6c33`）——避免反复无效触发 shouldTrigger 浪费 CPU，但 AutoCompactRun 内部仍命中本 ISSUE 的 fallback 失效路径。

## 跨 Run 自然恢复

- Run 1 单 turn 多轮场景始终命中 bug
- Run 2 起 prior 含 Run 1 的 UserInput → lastUserGroupIndex 找到较晚 group → 部分恢复

## 关联

- R22 audit LOW-1: shouldTrigger 跳 SkillBody/SkillListing marker 同模式
- R29 视角 2 LOW-1: shouldTrigger 跳 MemoryInjectedAnnotation 同模式（已修）
- ISSUE-74: preamble 节点化引入 Memory 注入到 buildInitialMessages 路径

## 决策

R29 audit 暂不修，单独 ISSUE 跟踪。优先级中——业务实际触发场景较窄（单 turn 跑 50+ 轮），但触发后 ReactiveCompact emergency drop 让 user input 丢失体验差。

修法方案 A 简洁但要小心 lastUserGroupIndex 调用方依赖（grep 看其他 caller 是否假设 strict semantics）；方案 B 更安全，仅在 early 失败 fallback 路径添加。
