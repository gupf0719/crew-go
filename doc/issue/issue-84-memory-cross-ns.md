# ISSUE-84 · Memory 协议跨 ns 边界传播改进

**优先级**：MED（架构改进，影响业务嵌套场景 Memory 行为可预测性）
**状态**：✅ 已修（2026-05-16）— HIGH-1/2 + MED-1/2/3 + LOW-1/2 全修（已由 store 体系替代，PR 3.3 commit e6505f9）
**触发**：R31 视角 B MED-1 触及 → R32+ 视角 5 专审 (2 HIGH + 3 MED + 2 LOW)

> **API 变更说明**（PR 3.3 commit e6505f9）：本 issue 标题与正文涉及的 Memory 接口已由 store 体系替代。
> 本文档保留作历史档案（文件名 / 标题 / 正文 API 名均不动）。当前对应 API：
>
> | 历史 API（本文档使用） | 当前 API |
> |---|---|
> | `Memory.Read` | `store.Search` |
> | `Memory.Write` | `store.Put` |
> | `WithMemory` | `WithStore` + `WithStoreMemory` |
> | `WithSubAgentMemoryScope` | `WithSubAgentStoreScope` |
> | `IsSubAgentMemoryScope` | `IsSubAgentStoreScope` |
> | `MemoryConfig` | `StoreMemoryConfig` |
> | `InMemoryMemory` | `InMemoryStore` |
>
> ISSUE-84 的 9/9 协议覆盖率、isAgentNil typed nil 防护、跨 ns 双重持久化避免等修复语义在 store 体系下
> 等价保留——`WithSubAgentStoreScope` 跨 9 调用入口 / 15 push 点的协议布局完全一致。

## 核心问题

ISSUE-79 修了半共享 child sess 协议（events 独立 + values 共享 + parentSess 反向
指针），但 **Memory 维度没真正闭环**：

### HIGH-1: Memory.Write 嵌套 sub-agent 双重持久化

业务用单例 mem0/Postgres/Redis 后端时, 每个 BasicAgent 完成 Run 都会调
`commitFinal` → `Memory.Write(ctx, [last])`. AgentAsTool / Skill fork 路径下:
- Sub-agent.Memory.Write 写 sub final
- 顶层 BasicAgent.Memory.Write 写顶层 final

两次 Write 同一 mem0 → **持久化 [sub_final, top_final]**. 业务通常期望"sub-agent
是工具,只用顶层 final 写记忆", 但框架默认每个 BasicAgent 都 Write 无信号传递。

### HIGH-2: RootSession 范式 0 文档（主用户文档）+ 0 调用 + 0 测试

ISSUE-79 fix 声称"业务用 sess.RootSession() 上追看父 history"解决了"嵌套
sub-agent Memory.Read 看父对话"诉求, 但:
1. **唯一文档化点是 agent_tool.go 内部 godoc** + issue-79.md, **主用户文档
   (usage-guide / orchestration-patterns) 完全没提**
2. **生产代码 0 调用** RootSession() / ParentSession()
3. **0 测试**覆盖 Memory + AgentAsTool 嵌套场景

API 设计了但用户不知道 + 没验证 → 等于不存在。半共享 sess 修复(ISSUE-79)若
Memory 维度没真正闭环, 文档收益打对折。

### MED-1: ForkLane parentSess 字段语义跳过 fork 边界

lane child sess 的 `parentSess` 字段沿用 `s.parentSess` 跳过 ForkLane 的 parent
自己 — 用户调 lane child.RootSession() 经过的链跳过中间 ParallelAgent sess, 与
业务预期不符。

### MED-2: 8 sub-agent 调用入口走 3 种不同 sess 协议

| 协议 | 编排 | child sess 行为 |
|---|---|---|
| 直接传 parentSession | Sequential / Loop / Routing / Reflect / Handoff | 全量父历史 + 自动写父 events |
| ForkLane | ParallelAgent | lane 共享父读视图 + 写隔离 |
| NewSharedChildSession | AgentAsTool / SkillTool fork | events 完全独立 + parentSess 反向指针 |

**同一 sub-agent 实例 (配 Memory) 放在不同编排里行为分裂**, 业务无法预测
Memory.Read 行为。

### MED-3: HistoryFor("") 多层嵌套 cross-agent 改写格式陷阱

`RootSession().HistoryFor("")` 把所有非 owner agent 的 assistant/tool 消息改写为
user("For context: [name] said: ...") — 多层嵌套时输出"全 user 消息流",
LLM 看到奇怪的 user 自言自语 + 多名 agent 历史 dump。

### LOW-1: InMemoryMemory demo 单 thread 共享, 与文档说"按 RunID 分桶"矛盾

godoc 说"生产实现按 ctx 中 RunSession.RunID 分桶", 但 InMemoryMemory.Read/Write
完全不读 ctx, 所有 thread 共用。业务参考 demo 抄实现时容易漏 thread 分桶。

### LOW-2: Memory.Read HITL gate 文档示例没显示 RootSession 路径

hitl-resume.md Memory.Read HITL 例子完全没提 sess.RootSession 在 HITL 上下文
怎么用。业务嵌套 sub-agent + Memory + HITL gate 时不知 sess 链行为。

## 修法方向（待评估）

1. **Memory 协议加 `ScopeHint`**（`MemoryWriteScope` enum: `RootOnly` / `PerAgent`
   / `BothExplicit`）, 或 ctx 透传 `IsSubAgent` 信号让业务 Memory.Write 自决
2. **3 种协议统一**: 6 编排也走 NewSharedChildSession + ForkLane 一致协议
   (破坏性但语义干净), 或文档化决策矩阵到 usage-guide.md 主表
3. **补 RootSession 范式到 usage-guide / orchestration-patterns 主文档** + 加
   1-2 个 Memory + AgentAsTool 端到端测试 (验证 RootSession() 真拿到父 events)
4. **HistoryFor 加 owner-aware 选项** 让业务选"原 assistant role 保留 + 标
   Annotation 注 from-agent" 而非降级 user
5. **InMemoryMemory demo 加按 sess.RunID 分桶**, 或 godoc 加更醒目警示

## audit follow-up (2026-05-16)

R32+ 视角 5 修完后再 audit (3 agent 单视角并发 + 自审 D, 跨视角共识), 揭露 4 真 HIGH:

### audit HIGH-1: 6 编排 sub.Run 漏 push WithSubAgentMemoryScope (协议覆盖最初 2/8, audit² 后 9/9 见下文)

视角 A + D 双独立发现 — `WithSubAgentMemoryScope` ctx 协议初始**只覆盖 2/8 sub-agent 调用**:
- ✅ AgentAsTool (`agent_tool.go:128`) — 已 push
- ✅ SkillTool fork (`skill_tool.go:745`) — 已 push
- ❌ Sequential / Loop / Routing×2 / Reflect×3 / Handoff / Plan×3 — 11 处漏 push

audit follow-up 修了上面 6 编排 11 push 点, 但**audit² 又揭露**漏 ParallelAgent + Routing classifier (见下方 audit follow-up² 章节). 最终覆盖率见后面 "协议覆盖率重新统计".

**修法**: 6 编排所有 `sub.Run(subCtx, ...)` 入口前 push `subCtx = agent_v2.WithSubAgentMemoryScope(subCtx, true)`, 协议覆盖 2/8 → audit² → **9/9 调用入口 / 15 push 点**.

### audit HIGH-2: hitl-resume.md §12.5.1 协议覆盖范围未明示

文档只举 AgentAsTool 范例, 业务可能误以为 SequentialAgent 等编排不享受此协议而漏注册 `IsSubAgentMemoryScope` 检查。

**修法**: hitl-resume.md §12.5.1 加协议覆盖说明 9 编排全 push (audit² 后修).

### audit MED 改进:

- **WithSubAgentMemoryScope godoc** 加 "协议覆盖 9/9 调用入口 (15 push 点)" 显式列表 + 多层嵌套 boolean 局限说明 (audit² 修后)
- **InMemoryMemory godoc** 加 race 警示 (业务多 goroutine Read/Write 需自加 sync)
- **HistoryForRaw godoc** 加浅拷贝陷阱警示 (返回 *Message 指向 sess.events 内部, 业务改 Annotation 影响原 events)
- **usage-guide 决策矩阵** 加 Memory.Write `IsSubAgentMemoryScope` 列, 表格行展示 9 调用入口全协议覆盖 (audit² 修后)

## audit follow-up² (commit 49d3065 audit, 2026-05-16)

commit 49d3065 落地 (audit follow-up 修 ParallelAgent + Routing classifier push) 后
再 audit (4 agent 单视角并发审 commit), 揭露:

### audit² HIGH-A1 + B1 + D-HIGH2 跨视角共识: 协议保护测试缺口

视角 A + B + D 跨视角共识揭露 — commit 5afb8eb / 49d3065 修了 ParallelAgent + Routing
classifier push **0 unit test 保护**, 后续若再次 silent revert 完全无 CI 守护. 同时
**视角 A** 揭露 IndexRef regression 测试只覆盖 1/3 worker (SubAgentName), TmpSubAgent
+ executor 路径单独 revert 测试仍 PASS — 5 类炸弹 #4 双层防护遮蔽变体.

**修法**: 加 4 sanity-revert 测试:
- `TestISSUE84_AuditFollowup2_HIGH1_ParallelAgentPushesScope` — sanity-revert 真 FAIL ✓
- `TestISSUE84_AuditFollowup2_HIGH2_RoutingClassifierPushesScope` — 同款保护
- `TestISSUE85_AuditFollowup2_HIGH1_IndexRefPreservesExplicit_TmpSubAgent` — 3/3 worker 覆盖
- `TestISSUE85_AuditFollowup2_HIGH1_IndexRefPreservesExplicit_Executor` — 同款

### audit² HIGH-A2 (视角 A 自审): typed nil interface 陷阱

`AddSubAgent` / `WithSubAgents` 用 `if a == nil` 检 — Go interface "typed nil" 经典陷阱:
`var b *BasicAgent; pa.AddSubAgent("x", b)` 时 b 是 typed nil pointer 但 Agent
interface header 持非 nil type tag → `a == nil` false → 旧 fail-fast 漏检 → 业务后续
ag.Run nil deref panic 难定位.

**修法**: 加 `isAgentNil(a Agent) bool` helper 用 `reflect.Value.IsNil()` 检 underlying
value 真 nil. AddSubAgent + WithSubAgents 都改用此 helper. 加 2 测试覆盖.

### audit² 协议覆盖率重新统计: 8/8 → 9/9 (15 push 点)

视角 C 跨文档审揭露 — commit 5afb8eb / 49d3065 修后实际 grep 确证 **15 push 点 / 9 调用入口**
(Sequential 1 + Loop 1 + Routing 3 + Reflect 3 + Handoff 1 + Plan 3 + Parallel 1 + AgentAsTool 1 + SkillTool 1).
之前文档"8/8 / 11 push 点"全 stale (跨 5 文档同病: CLAUDE.md / migration-history.md /
hitl-resume.md / issues.md / 本文档).

**修法**: 跨 6 文档同步改 "9 调用入口 (7 编排 + 2 tool) / 15 push 点" (CLAUDE.md / migration-history / hitl-resume / usage-guide / issues.md / 本文档).

### audit² MED-A1 (视角 A + D 共识): WithSubAgents 合并语义正向测试缺口

commit 49d3065 测试只覆盖 dup panic 路径, 没正向断言"merge-preserve". 若未来 silent
revert 合并改回 `p.subAgents = agents` 直接覆盖, dup-check 能保护 dup name 但
**不同 name 的 silent loss** 无测试覆盖.

**修法**: 加 `TestISSUE85_AuditFollowup2_MED1_WithSubAgentsMergePreservesPrior` 端到端
验"AddSubAgent('a1') + WithSubAgents({'a2': ...}) → planner 调 a1 应成功", sanity-revert
真 FAIL.

## audit follow-up³ (commit a8b42b9 audit, 2026-05-16)

commit a8b42b9 (audit²) 落地后再 audit (4 agent 单视角并发 + cross-verify), 揭露:

### audit³ HIGH-1: typed nil interface 协议反向不对称 (1/7 编排只 PlanAgent 防护)

视角 1 + 视角 4 双独立发现 — audit² 加了 isAgentNil helper 但**只在 PlanAgent 用**.
其他 6 编排 + 4 With* options (Sequential.AddStep / Loop.WithBody / RoutingAgent.AddBranch /
WithRouter / WithRoutingClassifier / WithRoutingFallback / Reflect.WithActor / WithEvaluator /
WithReflectorAgent / Handoff.AddAgent / Parallel.AddBranch) 全 0 nil 防护. 业务任一编排
传 typed nil 仍踩 worker 远 stack nil deref.

**audit⁴ HIGH-1 / HIGH-2 跟进**: audit³ 自报"7 编排 + 4 With*" 后又揭露 PlanAgent.SetExecutor
+ WithLLMPlannerSubAgents 仍漏 — 协议反向不对称四复证. audit⁴ 修补让 SetExecutor +
WithLLMPlannerSubAgents 也用 isAgentNil.

**audit⁵ 跟进** (协议五复证): audit⁴ 又漏 AgentAsTool + SkillSubAgentResolver `subAgent == nil`
+ RunnableAsTool typed nil func. audit⁵ 修后协议覆盖共 **17 isAgentNil 调用点** (实测 grep:
plan_agent 4 含 SetExecutor / llm_planner 1 / handoff 1 / loop 1 / parallel 1 / reflect 3 /
routing 3 含 WithRouter / sequential 1 / agent_tool 1 含 RunnableAsTool nil fn / skill_tool 1)
+ AllOrchs 15 子测试 sanity-revert 守护 (panic msg 断言区分 framework 显式 vs 远 stack runtime
error — audit⁵ HIGH-3 修 WithLLMPlannerSubAgents 假绿).

**自我打脸**: commit message 称 "typed nil 陷阱业务高发 — fail-fast 必用 reflect.IsNil",
但只修 1 处 — 同 commit 0aa15bf "3 处都改"谎报 + audit² IndexRef 测试只覆盖 1/3 worker 重蹈.

**修法**:
- 抽 `isAgentNil` 到 `agent_v2/agent_helpers.go` 包级 helper (含 reflect.UnsafePointer 完整 kind 覆盖)
- 7 编排 + 4 With* options 全用 isAgentNil panic-fast
- 加 `TestISSUE84_AuditFollowup3_HIGH1_TypedNilInterfaceTrap_AllOrchs` 11 子测试覆盖

### audit³ HIGH-2: push 点测试覆盖率 9/11 漏 (跨视角 1 + 2 共识)

11 push 点中只 3 个有 sanity-revert 单测保护 (AgentAsTool / Parallel branch / Routing classifier),
其他 8 (Sequential / Loop / Routing branch+fallback / Reflect×3 / Handoff / Plan×3) 0 守护.
后续若 silent revert 完全无 CI 抓.

**修法 (待评估)**: 加综合 `TestSubAgentMemoryScopeAllPushPoints` 一次 setup 覆盖所有 push.
ROI 中等 (现已加 isAgentNil 防护, push 失效场景比 nil 失效少触发, 留下轮处理).

### audit³ HIGH-3: ISSUE-87 跨 map mutex 仍 0 sanity-revert 守护 (诚实降级)

audit² 已诚实改名 `*_NoDeadlock` + godoc 说明 "本测试不构成 sanity-revert 守护"; mu 真效果
是逻辑层 ordering 单元测试无法可靠验证. **诚实承认局限不假装**, 但 silent revert mu 仍
完全无测试守护. 接受局限留 stress test 补.

## 关联

- ISSUE-79: AgentAsTool 半共享 child sess (前置)
- ISSUE-80: ParallelAgent lane events (前置)
- R32+ 视角 5 audit: 完整 finding 列表
- R32+ audit follow-up (3 agent + 自审 D): 协议覆盖 2/8 → 8/8 修
- R32+ audit follow-up² (4 agent commit-level audit): 8/8 → 9/9 (15 push 点) + ParallelAgent + Routing classifier 协议补齐 + PlanAgent typed nil 陷阱修 + 测试缺口补全
- R32+ audit follow-up³ (4 agent commit-level audit a8b42b9): isAgentNil 抽包级 + 7 编排 + 4 With* options 全协议对称 + AllOrchs typed nil 综合测试
