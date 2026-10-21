---
id: ISSUE-69
title: 上下文压缩落地后 9 轮交叉审计找到的 30+ bug 集合（已全修）
status: ✅ 已实现
priority: 高
created: 2026-05-04
depends: ISSUE-65, ISSUE-66, ISSUE-67, ISSUE-68
---

# ISSUE-69 · 上下文压缩落地后 9 轮交叉审计 retrospective

## 背景

ISSUE-65~68 上下文压缩 4 层落地后，按 [CLAUDE.md 多层系统 bug 审计方法论](../../CLAUDE.md)
做了 9 轮交叉审计（自审 + 独立 agent 复审 + 针对性 sanity revert），共找到约
**30 个真 bug** 全部修复 + 配套回归测试。

本 issue 是 retrospective 索引——bug 已全修无需追踪，但作为后续类似大型架构
落地后审计的实证案例和工作量参考。

## 找 bug 数量曲线（实证递减但 ≠ 0）

| 轮次 | 触发方式 | 真 bug 数 | 设计权衡/文档 gap |
|---|---|---|---|
| Round 1 | 自审（实施完毕） | 0 | - |
| Round 2 | 用户追问"有没有 bug" | 3 | - |
| Round 3 | 用户追问"还有没有" | 3 | - |
| Round 4 | 自审 + agent 协作 | 6 | - |
| Round 5 | agent 专审"未看的场景" | 4 + 1 自审 = 5 | - |
| Round 6 | agent 复审 + 自查 fix 副作用 | 1 + 1 自查 = 2 | 5（设计权衡）|
| Round 7 | agent 审 ISSUE-58~64 架构 | 6 | - |
| Round 8 | agent 审 ask_user/cp/Resume 嵌套 | 1 | 1（架构限制）|
| Round 9 | agent 审 (a)(b)(c) 剩余角度 | 1 | 1（架构限制）|
| panic recovery | 自审产线可靠性 | 1 | - |
| **合计** | | **~30** | **7** |

每轮审计都按方法论：sanity revert 验证测试有效（删 fix → fail 信息精确指向 bug
→ 恢复后 PASS），保证测试不假绿。

## bug 分类（按发现轮次顺序）

### 算法层 bug（Round 2~3）
- AutoCompact 把最后 user 消息也 summarize 掉，LLM 失去当前问题
- ReactiveCompact `attempts` 字段恒报 MaxAttempts，掩盖"消息已无法再压缩"
- compose.AdjustIndexToPreserveAPIInvariants 死代码——没接入 truncateHistory
- formatCompactSummary 找不到 `<summary>` 时回退全文当成功
- IsPromptTooLongError 关键词太宽，误匹配业务错误

### 跨层数据流 bug（Round 4）
- MicroCompact 占位符被 AutoCompact extractRecentFiles 当真实文件内容
- ReactiveCompact gs.Messages 持久化 OnModelStart 钩子临时修改
- AutoCompact "last group as tail" 在多 user 场景丢 verbatim user
- Bug J: ReactiveCompact emergencyDropOldestGroups 把 AutoCompact summary 当最旧组删
- Bug O: CircuitBreaker trip 后无法自愈
- Bug P: AutoCompact tail 边界算法在常见场景失效

### 错误路径 + 防御 bug（Round 5）
- emergencyDropOldestGroups 把 system message group 也丢
- AutoCompact panics on `cfg.Model.Generate` 返回 nil resp
- 空 summary 误计 breaker success
- modelNodeFunc 没防 `compacted == nil`
- sysEnd 循环 nil 元素解引用 panic

### 跨 Run / 并发 / Memory bug（Round 5）
- AutoCompact 跨 Run 不持久化 summary（每 Run 重复 summarize）
- thundering herd: N 分支并发触发 AutoCompact 击穿 breaker
- Memory 注入消息被反复 summarize
- MicroCompact 重复 clone 已清消息
- Memory mutation 副作用（Round 5 fix 自身引入）

### 架构重构 regression（Round 7，4 个月前 ISSUE-58~64 引入）
- ISSUE-63 streaming tool_call 多 chunk 聚合丢失（**CRITICAL**：ReAct streaming 必坏）
- ISSUE-64 OrchHooks 跨子图边界丢失（9 处 extractOrchHooks 不读 ctx）
- ISSUE-62 fan-out resume subgraphCheckpoints 不回填
- OnNodeStart 失败不配对 OnNodeEnd
- ResumeContext 泄露到子图首次路径
- chunk Annotations/Metadata 共享指针

### 嵌套 / 防御 bug（Round 6 续审 + 8 + 9）
- AutoCompactRun 缺 panic recover
- ask_user Resume 路径 tool_result 不 stamp 不 emit（HITL 审计证据丢失）
- runBatch 后 ProcessState 读 stale typed state（fast path 与 resume rerun 不对称）
- AutoCompact 状态事件 OrchHooks 不可见（plan_chat_web 集成时发现）

### 框架级防御（panic recovery 轮）
- compose 层 NodeFunc 调用点裸跑无 recover——任何 NodeFunc panic 拖垮 graph

## 已文档化的设计权衡（不是 bug，但用户必须知道）

1. **ParallelAgent fan-out 共享 breaker** —— 摘要 LLM 失败通常是系统性问题，
   N 分支独立重试浪费 quota；shared 让 graceful degradation
2. **AgentAsTool 内层 BasicAgent 完全独立 cfg** —— 内层独立 LLM 摘要 quota，
   外层不可见
3. **MCP / Skill 默认不在 MicroCompact 白名单** —— 远端结果 / SKILL.md 可能是
   策略关键素材；用户需手动加白名单
4. **Streaming + ReactiveCompact retry 时前端 chunk 重启** —— 第二次 attempt
   仍走 Stream，前端可能看到"半截答案 + 全新答案"（应订阅 :reactive_compact
   namespace 清缓冲）
5. **PA/RA + 父编排嵌套时 cp 覆写** —— 内层 InterruptError 写 cp[sessionID]，
   外层 graphLoop saveCheckpointOnError 又覆盖
6. **Sequential / Loop / Reflect / Handoff 完全不支持 HITL** —— 这 4 个编排
   GraphRunOptions 不注入 Checkpointer 且没有 Resume 方法。需要 HITL 时
   选 PA / RA / PlanAgent 作为顶层
7. **Resume rerun 路径 hook 临时修改 messages 不保留** —— ReactiveCompact
   补救路径用 gs.Messages 直接压缩（不含钩子注入）

## 关键教训

按方法论沉淀到 [CLAUDE.md 多层系统 bug 审计方法论](../../CLAUDE.md)：

- **测试可信度**：sanity-revert 才算测试通过；mock 不能返回固定响应忽略 input；
  并发场景需 WaitGroup 屏障 + 短超时强制真正并发，否则 Go scheduler 自然
  串行化掉 thunder-herd 测试
- **审计前画两张图**：完整时序图 + state 所有权图（lifetime）
- **必查 9 类**：跨层数据流 / 触发顺序 / 跨 Run 持久化 / 消息分组语义 /
  错误路径 / 钩子状态所有权 / Resume / Checkpoint 一致性 / 并发共享态 +
  thundering herd / 退出条件 / 边界 / 主流场景
- **交叉审计**：自审 → 独立 agent 复审（专门指它去查"还没系统看过的角度"）→
  针对性 sanity revert（Python script 改关键代码块）→ 自评估 fix 是否引入新 bug
- **审 N 轮 bug 数几何递减但 ≠ 0**：不要承诺"扫干净"，说"已知未发现，可继续审
  X / Y / Z 角度"更诚实

## 留下的长期跟进项

- **HITL 矩阵 gap**：让 SequentialAgent / LoopAgent / ReflectAgent / HandoffAgent
  实现 NestableAgent + 加 Resume 方法。**这是当前实现 gap，不是架构限制**——
  eino adk/workflow.go 的 workflowAgent（同模式 Sequential/Parallel/Loop）
  通过 typed state 字段（`SequentialInterruptIndex` / `LoopIterations` /
  `ParallelInterruptInfo map[index]InterruptInfo`）+ `Resume(ctx, info)` 接口
  完整支持。LangGraph 同思路：checkpointer 持久化任意节点状态，graph 拓扑无关。
  我们参考实现即可，独立 ISSUE 跟踪
- **真实 LLM 端到端验证**：API 网关 503 期间未跑，恢复后跑 13 个 adv_*_test.go
  + plan_chat_web 真实模式验证压缩在 production-like 场景的行为

## 关联 commit（按时间排序）

5e930d0 ~ 26b145d 共约 40 个 commit，覆盖：
- ISSUE-65~68 设计 + 实施
- 9 轮审计 bug 修复
- 文档同步（CLAUDE.md / context-compaction.md / issues.md / okr.md / README.md）
- example 更新（adv_14_compaction_test.go + plan_chat_web 集成）
