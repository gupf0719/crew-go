---
id: ISSUE-73
title: v2 文档体系交叉审计跟踪 + 未写文档清单
status: 🟡 进行中（暂停审计待续）
priority: 高
created: 2026-05-11
---

# ISSUE-73 · v2 文档体系交叉审计跟踪 + 未写文档清单

## 背景

Stage 7 完成后建立 v2 文档体系（约 9000 行）。按 CLAUDE.md "文档撰写纪律"
（commit `30b1fcc`）标准流程——**写 → 多轮 audit → 修 → 验证**——对 7 篇核心
文档累计 6 轮 cross-audit，**累计修 142 真 HIGH**。

本 issue 跟踪：
1. 当前 audit 状态（每篇审到第几轮、累计 HIGH 数）
2. 仍需继续 audit 的文档 + 预计剩余 HIGH
3. 未写的 P1-2/P1-3/P2 文档清单
4. 关键教训沉淀链接

**当前状态**（2026-09-07 更新）：7 篇核心文档全部审完，最少的也过了 4 轮。**业务直接照搬基本不会
编译失败 / 行为不符**。R7/R4/R4 这一轮仍找出 14 真 HIGH，说明「轮次够了」不等于「扫干净了」——
但三篇的边际收益已明显下降（migration-history 的 8 条 HIGH 里 6 条是符号被删/改名后没回来清，
属于机械可防的一类）。**下一步不是再加一轮 audit，是让机械层多担一点**：commentaudit 现在只抓
「符号不存在」，抓不到「符号还在但语义变了」「该列的 API 没列」这两类——那正是本轮 HIGH 的大头。

## 当前 audit 进度（按文档）

| 文档 | 行数 | 已审轮次 | 累计 HIGH | 状态 | 建议下轮 |
|---|---|---|---|---|---|
| `channel-engine.md` | 1714 | R1+R2+R3+R4 | 28 | ✅ 接近收敛 | R5 边际收益低，可暂缓 |
| `hitl-resume.md` | 897 | R1+R2+R3+R4 | 14 | ✅ 接近收敛 | R5 边际收益低 |
| **`context-compaction.md`** | 1033 | R1..R7 | **50** | ✅ R7 完成（2 HIGH + 9 MED + 16 LOW 全修）| R8 边际收益低 |
| `orchestration-patterns.md` | 870 | R1+R2+R3+R4 | 20 | ✅ 接近收敛 | R5 边际收益低 |
| `usage-guide.md` | 1139 | R1..R4 | 20 | ✅ R4 完成（4 HIGH + 5 MED + 6 LOW 全修）| R5 边际收益低 |
| `migration-history.md` | 280 | R1..R4 | 16 | ✅ R4 完成（8 HIGH + 8 MED + 4 LOW 全修，命中率 100%）| R5 边际收益低 |
| `CLAUDE.md` (架构摘要) | ~150 | R1+R2+R3 | 8 | ✅ 接近收敛 | R4 边际收益低 |

**累计**：7 文档 × 平均 4.4 轮 audit = 31 audit 轮次，修 **156 真 HIGH**。

**2026-09-07 R7/R4/R4**（三 agent 单视角并发，视角统一为「文档断言 vs 代码事实」）：62 finding
（14 HIGH + 22 MED + 26 LOW），逐条 grep 交叉验证后**全部为真、全部已修**。三条最值钱的：

- **压缩产物不写回 channel、不进 cp**——context-compaction 有 6 处说它「走 channel / 进 cp.Channels /
  单 Run 内多轮不重摘」，与代码相反（`makeModelFn` 里只改局部入参，model 节点写回只有 LLM 输出）。
  照文档理解会以为摘要能跨轮复用，实际每轮都在原始全量上重摘
- **`hook.Hooks` 没有 ctx 透传协议**——usage-guide 的对照表写「✅ 嵌套 sub-agent 自动继承（同协议）」，
  实际只有 `OrchestrationHooks` 有。只在顶层配 `WithSequentialHooks` 指望看到子 agent 的
  `OnModelStart` / `OnToolStart`：一个都不触发、无报错
- **`ctx.Done()` 硬停同样存档**——usage-guide 说「不存档」，而 `shouldStayPending` 含 ctx 错，
  出口照写 Pending cp。读者以为槽是干净的，同 sid 再 Run 会被 auto-load 捡起旧档且 `err == nil`

另有两处**代码 godoc** 同病由文档审计顺带揪出（`approval.go` 的审批状态载体、`CLAUDE.md` 速查表的
`WithCurrentToolCallID` / `LLMPlanner`），一并修。

### 8 篇 P0/P1 文档完成状态

| # | 文档 | 状态 | 备注 |
|---|---|---|---|
| 1 | `why-channel-based.md` | ✅ 完成（Stage 7 + R0）| 决策记录，未做 audit（内容稳定）|
| 2 | `channel-based-design.md` | ✅ 完成（PoC 期，保留）| 历史文档不需 audit |
| 3 | `poc-report.md` | ✅ 完成（Stage 1-3）| 历史文档不需 audit |
| 4 | `migration-history.md` | ✅ R4 完成 | 16 HIGH 修 |
| 5 | `usage-guide.md` | ✅ R4 完成 | 20 HIGH 修 |
| 6 | `channel-engine.md` (P0-2) | ✅ R4 完成 | 28 HIGH 修 |
| 7 | `hitl-resume.md` (P0-3) | ✅ R4 完成 | 14 HIGH 修 |
| 8 | `context-compaction.md` (P0-1) | ✅ R7 完成 | 50 HIGH 修（v1 stale 最多）|
| 9 | `orchestration-patterns.md` (P1-1) | ✅ R4 完成 | 20 HIGH 修 |

## 未写的文档清单（P1-2 / P1-3 / P2）

按 P0/P1/P2 优先级路线图（[`doc/code/todo.md`](../code/todo.md)）：

### P1-2：`hooks-and-streaming.md`（✅ 已写，332 行 12 章节 —— hook 重构期 commit `97f9309` 完成；原"待写"标记 2026-06-01 stale 回填，下方内容范围保留作历史参考）

**内容范围**：
- AgentHooks vs OrchestrationHooks 两层 hook 完整时机表
- ctx-aware OrchHooks（嵌套子 agent 自动继承父 hooks 的 agent_v2 高层路径）
- chunk 不写 session.Events 协议（streaming chunk 实时投递通过 OrchHooks.OnNodeEvent）
- RecordingHooks test helper 完整用法
- mid-stream Interrupt + reset_stream + run_id 协调（R10-A）
- streamOrGenerate annotation 丢失修复（Stage 6.1）
- Hooks panic 隔离（R7）+ ChainHooks 短路语义

**前置依赖**：channel-engine.md §10 已写完，本文档是它的业务侧深入版。

**写作前必做**（按 CLAUDE.md "文档撰写纪律"）：
- grep 所有 `^func With.*Hook|^func New.*Hook|^type.*Hooks` 在 `hook/`、`compose_v2/hooks.go`、`agent_v2/options.go`
- grep 所有 `OnAgentInit / OnAgentStart / OnAgentEnd / OnModelStart / OnModelEnd / OnModelEndStream / OnToolStart / OnToolEnd / OnError` 实际触发位置
- grep `safeOn*` recover 包装函数确认 panic 隔离机制
- grep `Send(Event{Type:"reset_stream"...})` 确认所有 emit 点

### P1-3：`architecture-overview.md`（✅ 已写 commit 39608df —— 156 行总览，doc-writing skill 流程 + 1 轮独立 agent audit 8 维度 grep 实测 1 MED 修；2026-06-01 回填，下方内容范围保留作历史参考）

**内容范围**：
- 包结构 + 包依赖方向（schema ← model/tool/hook/skill/mcp ← compose_v2 ← agent_v2）
- 数据流全景（Run 入口 / NodeFunc / channel barrier / cp save / Resume restore）
- 跨层归属总表（Session / State / cp / Memory 各自的 lifetime + producer/consumer + 持久化位置）
- v1 → v2 关键差异（GraphState→Channel / typed state→reducer / NestableAgent→单接口 等）
- 复用 v1 时代被删的 `design.md` / `runtime.md` 精华（已恢复策略：从 git show + 改写）

**前置依赖**：context-compaction / channel-engine 等已写完，本文档是高度浓缩的总览。

**写作前必做**：
- 从 git history 恢复 v1 `design.md` / `runtime.md`（commit `6cba234^`）作为参考骨架
- grep 所有 `^package |^// Package ` 包顶部 godoc 确认包定位
- grep 所有公开 API（前几轮 audit 累计找到的漏 API 列表见下面）

### P2 系列（可选，价值递减）

| 文档 | 价值 | 备注 |
|---|---|---|
| `decisions-and-conventions.md` | 中 | 沉淀 CLAUDE.md "项目特定约定"等散落决策 |
| `audit-methodology.md` | 高 | 把 R1-R6 audit 经验沉淀（命中率曲线 / 视角分类 / 跨文档同病防治）|
| `quickstart.md` | 中 | 5 分钟入门指南（业务最小可跑示例）|
| `sdk-guide.md` 完整化 | 低 | 当前 33 行占位 + "上下文压缩" 段已展开，其他章节占位 |

## 跨文档同病清单（已修但需持续监控）

下次写新文档 / 改老文档时**优先 grep 这些跨文档同病关键词**确保不再现：

| 同病 | 表现 | 已修文档 |
|---|---|---|
| `cm.RestoreFromCP` 不存在 | 实际 `cm.Restore` | channel-engine R2 |
| `IsResumeTarget` 函数不存在 | 实际通过 `(sid, ns)` 二级 key 各层独立 LoadNS | channel-engine R1, migration R1 |
| `cpKeyInterrupts` map 不存在 | 实际 `cp.PendingInterrupts []*AskQuery` | CLAUDE.md R1 |
| `AskQuery.NS / ToolCallID` 字段不存在 | 实际只有 Question/Options/InterruptID | migration R1 + 多文档 |
| `Builder` 类型不存在 | 实际 `GraphBuilder` | CLAUDE.md R1, migration R1, channel-engine R2 |
| `WithSubgraphResumeAnswersForwarding` 不存在 | 实际默认 forward + opt-in 隔离 | hitl-resume R1 |
| `EndChannel` 已删 | 文档 §1.3 自己说删 §4/§5 仍提及 | channel-engine R2 |
| `OnceFlag 写第二次 panic` 错 | 实际"已 set 静默忽略" | channel-engine R2 |
| `collectInterrupts/saveCpPending/wrapInterruptError` 三伪函数名 | 实际无封装函数 | channel-engine R2, hitl-resume R2 |
| `ProcessState/NewStateFactory 已删` 错 | 实际 v2 仍保留兼容路径 | migration R1, channel-engine R2 |
| `panic 不存 cp` 错 | 实际 frontier 非空时仍 SaveNS Pending | channel-engine R4, hitl-resume R4, context R5+R6 |
| `MaxIterationsError/MaxRoundsError` 死代码 | 类型存在但生产路径不抛 | channel-engine R4, CLAUDE.md R3 |
| `OrchHooks ctx-aware 引擎层失效` | compose_v2.GetOrchHooks 不被 graph_loop 调用 | channel-engine R4 |
| `ResumeActionReplan 所有 agent 接受退化` 错 | 实际 7 编排 fail-fast not supported | hitl-resume R2, orchestration R2, CLAUDE.md R3, agent.go:50 注释仍 stale |
| `toolsFn 倒扫机制` 不存在 | 实际 toolsNode 整节点失败 + Resume 重跑全部 | CLAUDE.md R1, hitl-resume R1, channel-engine §13.4 #33 R2 |
| `ReactiveCompact 同步回 messages` 错 | 实际本地 retry 局部覆盖不写回 channel | CLAUDE.md R2, context R4 |
| `MaxRounds 默认 20` 错 | 实际 agent_v2 默认 10 / compose_v2 默认 20 | orchestration R2 |
| `LangGraph fan-out 仅重跑失败` 错 | 实际 LangGraph 整超步重跑 | channel-engine R4 |
| `compaction:* 事件 emit` 全虚构 | 实际仅 reset_stream | context R3 |
| `AdjustIndexToPreserveAPIInvariants` 4 层压缩用 | 实际 4 层压缩生产路径 0 调用 | context R4 |
| `SubgraphSnapshots` 已删概念残留 | v2 已改为 (sid, ns) 物理分离独立 cp | usage-guide R3 |
| `hook 数 8 vs 9` | 实际 hook.Hooks **8 方法**（含 OnModelEndStream + OnError; ISSUE-82 commit `6bb4c357` 后删 OnAgentInit 9→8）| usage-guide R3 |
| `OnModelEndStream 已实现` 误导 | 接口已含但 PoC 阶段 runtime 暂不调用 | usage-guide R3 |

## 当前漏的公开 API 速查（migration-history R3 找出）

可补到 migration-history.md / CLAUDE.md 速查表：

```
RunOption: WithFreshRun, WithStateFactory（已补 migration R3）
ReflectOption: WithReflector, WithEvalFunc（已补 migration R3）
4 层压缩 helper: EstimateTokens, EstimateMessageTokens, EstimateTokensFromAPIUsage,
                 GroupMessagesByAPIRound, AdjustIndexToPreserveAPIInvariants（已补 migration R3）
ReactiveCompact retry 触发: IsPromptTooLongError, ParsePromptTooLongGap（已补 migration R3）
Channel helper: Has, MustGet（已补 migration R3）
```

## 关键教训沉淀

详见 [`CLAUDE.md` "文档撰写纪律" 章节](../../CLAUDE.md#文档撰写纪律)（commit `30b1fcc` + `0c56fc5`）：

1. **写 vs 审是两种认知模式**——同一 LLM 不同 prompt → 不同行为模式；写时生成压力下
   anchoring + 训练数据偏差导致大量虚构 API；审时怀疑性 + 代码外部锚点 + 单 claim
   聚焦让命中率接近 100%
2. **5 条 grep 必修**：API 名 / functional claim / 算法步骤 / 修复跨文档 / 表格 ✅ 证据
3. **多轮 audit 必需性**：单视角 audit 只能抓 30% 错；4-5 轮多视角才接近收敛
4. **跨文档同病**：每篇独立写错的时候多篇一起错——必须跨文档 grep + "权威源" 机制
5. **不承诺"扫干净"**——多层系统的 bug 总在你以为干净时冒出来

## R30-R36 多视角并发 audit 进度（2026-05 中下旬）

CLAUDE.md "审计纪律" §6+7 落地: **多 agent 单视角并发审 > 单 agent 多视角同时审**.
每轮 spawn 4 视角独立 agent + HIGH 交叉验证 + sanity-revert 加固. R30-R36 累计扫
**164 finding, 113 HIGH + 50 MED + 22 LOW (粗算 +1 真 code bug ~186 真 finding)**.

### 真 code bug (1)

- **R30: GraphAgent.Run+Resume defer OnAgentEnd 顺序倒置** — commit `84025d9` fix + 加
  `agent_v2/r30_graph_agent_hook_order_test.go` 回归 (按 hook 时序图证: defer LIFO
  导致 OnAgentEnd 在 cleanup 之前 fire 应改为 cleanup 后)

### 每轮 audit 视角 + finding 摘要

| R | finding | HIGH | 主视角 / 命中 cluster |
|---|---|---|---|
| R30 | 16 | 11 + 1 真 code bug | 跨 doc 同病 + agent-interface anchoring + GraphAgent hook 顺序 |
| R31 | 17 | 12 | 跨包 godoc + agent-interface 修后引入新 stale |
| R32 | 18 | 10 | 跨包 godoc 3-round + ISSUE-82 OnAgentInit 残余 + DeepSeek→qwen 跨 3 doc |
| R33 | 28 | 14 | doc example 业务可运行性 (新视角反弹) |
| R34 | 30 | 21 | 字面量虚构 + 跨 doc 不一致 |
| R35 | 53 | 34 | observability 端到端实证 (commit `8231a0f` 139 行大改自审引入) |
| R36 | 22 | 11 | observability 跨表 self-conflict + 业务 use case + 历史 doc + 索引 |

### 关键教训沉淀 (R30→R36)

1. **commit msg verify grep query 必严谨** (R30→R36 5 次复证): 单 commit 改多表多段时
   path-limited grep 漏跨表 self-conflict; 必 full-tree grep + `--include` flag
2. **audit ROI 不可预测** (用户纠正): "换新视角是探索性赌博" — 不能预言"R36 后边际
   收益降"; 每轮换视角仍可能找新 stale
3. **R29 反弹现象**: R28 单 agent 多视角 0 finding → R29 多 agent 单视角揭露 6 真
   finding (含我自己引入的 input dup regression); **单 agent 多视角同时审命中率
   显著低于多 agent 单视角并发审**
4. **R32 实证 commit-level vs round-level audit 互补**: 共享状态 commit (sess /
   channel / map / sync) 必 commit 后立即 audit + race test; round-level audit
   抓跨 commit 设计一致性 (如 ISSUE-74 regression 跨 R26→R29 才被发现)
5. **R34 字面量虚构 + R35 端到端 + R36 跨表 self-conflict** = 单 commit 改多段
   stale 的三种高发模式; 跨这 3 视角并发审命中率高
6. **CLAUDE.md "不要承诺扫干净了"**: R30→R36 累计 162 真 finding 后 R36 仍抓 22
   finding; 文档系统不存在"完全收敛"

## 后续续审建议（按优先级）

### 高优先级
1. **R7 audit context-compaction.md**（v1 stale 最多，R6 仍 6 HIGH，预计 R7 < 5 HIGH 但仍非 0）
2. **R4 audit usage-guide.md**（R3 找出 SubgraphSnapshots 等 R1+R2 漏审，R4 可能仍有边角）
3. **R4 audit migration-history.md**（R3 找出导航 + 速查表漏 8 个 API，R4 复审是否补全）

### P0 + P1 代码层进度（2026-05-11）

✅ **P0 全部完成**:
- 7 处 stale godoc 清理（agent_v2 / compose_v2）— commit `0d36651`
- hook/ ChainHooks panic 隔离测试补 4 个 — 同上 commit

✅ **P1 第一批完成**（commit `ba9a576`）:
- OrchHooks ctx-aware 引擎层补完（audit R4 H1）
- MaxIterationsError 真生效（audit R4 H2）

✅ **P1 第二批完成**（ISSUE-63 验收）:
- ISSUE-63 实时 streaming OrchestrationHooks **已完成**（v2 channel-based 重设计
  时已落地，本次正式标完成）
- ChainOrchHooks 多 hook 链 + panic 隔离（ISSUE-63 增量）+ 4 测试

✅ **P1 第三批完成**（OnModelEndStream hook 真接入）:
- 之前 PoC 阶段 hook.Hooks.OnModelEndStream runtime 不调用，业务用 EventSink 替代
- 本次正式接入 streamOrGenerate（agent_v2/basic_agent.go）—— 业务 hook wrap reader
  可观测/转换/审计每个 chunk + final message 仍正确聚合
- 加 schemaStreamAdapter / streamReaderToSchema 双向适配（schema.StreamReader ↔
  specModel.MessageStreamReader）
- 加 SafeOnModelEndStream 导出版（compose_v2/agent_hooks.go）+ 2 测试
  （TestOnModelEndStream_HookWrapsReader / NoHook_Passthrough）

✅ **P1 第四批完成**（v2_skill_demo example + skill 三方对照设计文档）:
- `example/graph/v2_skill_demo/` 新加 mock LLM + 2 SKILL.md（translate / code-review）+ 3 场景全闭环冒烟通过
- `doc/description/three-way-comparison/skill-system.md` 写完 + R1+R2 audit + 修 cross-doc bug
  （`orchestration-patterns.md:765` 把"按关键词匹配返回"改为"直接按 LLM 给的 name 激活"）
- 顺手修 `agent_v2/config.go::WithSkills` godoc 的错误示例（`Builder()` 链 → `NewDefaultSkillBox(rootDir)` 单参数）
- skill 系统已知缺口梳理到文档"crew-go 当前不足"段（V1 仅 toolCall 模式 / 无 fork / 无 per-skill model / `ListSkillActions` 始终返 nil 等 7 项）

✅ **P1 第五批完成**（skill-system.md 基于 cc-local 反混淆源码 R3-R4 深度审计 + 收尾合并）:
- 读 `cc-local/src/` Claude Code 部分泄漏源码 ~3000+ 行（前 R1+R2 共 ~1500 行覆盖只有 50%）
- R3 audit 找出 4 个跨边界持久化机制（`bootstrap/state.ts addInvokedSkill` + `compact.ts createSkillAttachmentIfNeeded` + `conversationRecovery.ts restoreSkillStateFromMessages` + `registerSkillHooks.ts SKILL.md hooks + once 自卸`）
- R4 ROI 定向 audit 找出 4 个工程窍门，**关键 H3 揭示 crew-go 隐患**：
  - Claude `postCompactCleanup.ts:31-51` 用 `querySource.startsWith('repl_main_thread')` 区分 main vs sub-agent compact 防误清父 cache
  - crew-go 的 4 层压缩在嵌入运行时（GraphNS != ""）**可能有同样隐患** —— 写入"crew-go 待审 bug"节作为 P1 必修
- skill-system.md 重写：
  - 三方设计速览表加 Claude 列的 6 个新维度（跨 compaction 保鲜 / 文件热重载 / 学习反馈 / 排序推荐 / 跨 sub-agent 隔离 / per-call contextModifier）
  - 新增 §"Claude Code Skills 完整子系统图谱"：5 维度 + 6 层渐进式披露 + 4 工程窍门
  - 4 张对照表加 Claude 已确认细节（disableModelInvocation / paths / hooks / argumentHint / contextModifier 等）
  - "crew-go 当前不足"加 R2-R4 揭示的 6 个新缺口（#8-#13）
  - 新增 §"crew-go 待审 bug"自审 checklist（ReactiveCompact 断路器 / Memory cache）
  - 决策记录加 R1-R4 三条（不抄 self-improvement / 不抄 DiscoverSkills / **借鉴**跨 compaction + 热重载 + main-subagent 区分）
  - 待办按 ROI 重排（P1 必修 5 项 / P2 应做 5 项 / P3 慎做 6 项 / 战略选择 2 项）
  - 加 cc-local 关键源码定位表 + R1-R4 audit 摘要 + "未审范围"诚实说明
- 4 轮 audit 全 100% 命中率，已知关键发现已合并到设计文档；剩余未审范围（MCP builder / 完整 permission 路径 / coordinator multi-worker / UI / telemetry）按 ROI 不做

**剩余 P1**:
- 2 个 example 补全（v2_store_demo / GraphAgent / TypedGraph 等价; 原 v2_memory_demo 因 PR 3.3 Memory 接口已删改 store）
- ~~**新 P1**: 审 + 修 agent_v2 sub-agent 嵌入运行的 compact cleanup 隔离~~ → ✅ **已审无隐患**（2026-06-01 回填）：双重隔离机制（`config.go:421` per-BasicAgent `cfg.breakers=&sync.Map{}` + `autocompact.go:135` per-sessionID）+ crew-go 无模块级 process cache + 2 sanity test PASS（`TestAutoCompact_BreakerIsolatedAcross{Sessions,CfgInstances}`），详见 [skill-system.md §"crew-go 待审 bug"](../description/three-way-comparison/skill-system.md)

### 中优先级
4. R5 audit channel-engine.md / orchestration-patterns.md / hitl-resume.md（4 轮后边际收益递减但 ≠ 0）

### 低优先级
5. R4 audit CLAUDE.md 架构摘要（已收敛，但作为每 session 加载入口仍值得复审）

## P1-2 / P1-3 写作前检查清单（按 CLAUDE.md "文档撰写纪律"）

写新文档前必做：

```
☐ 1. grep 所有 ^func / ^type / ^var / ^const 在相关包，整理为 fact sheet
☐ 2. grep 所有 default 值（zero value / DefaultXxx 常量）
☐ 3. grep 所有 functional claim（"框架自动 X" 时找实际触发点）
☐ 4. 检查跨文档同病清单（本 issue §跨文档同病清单）确保不再现
☐ 5. 写完立即 spawn audit agent 复审（不要等几小时后）
☐ 6. 修复后必跑 grep 全文 + 跨文档（避免局部 fix bias）
☐ 7. 至少 R3 后再宣布 "可以使用"——不承诺 "扫干净"
```

## 关联

- [`CLAUDE.md` "文档撰写纪律"](../../CLAUDE.md#文档撰写纪律) — 标准流程沉淀
- [`doc/code/todo.md`](../code/todo.md) — Stage 7 后路线图
- [`doc/v2/`](../v2/) — 当前已完成 v2 文档体系
- 已 commit 的 R1-R6 audit 修复 commit list:
  - `0ffe8d6` P0-1 context-compaction.md 恢复 + Stage 7-D 4 修
  - `f0ce5ce` P0-2 channel-engine.md 加
  - `cd77d96` channel-engine.md R1 16 HIGH 修
  - `378b9a8` P0-3 hitl-resume.md 加 + R1 5 HIGH 修
  - `7d16db8` 5 文档批量 R1 audit 26 HIGH 修
  - `7778a90` P1-1 orchestration-patterns.md 加 + R1 11 HIGH 修
  - `3a2baeb` 4 文档 R2 audit 19 HIGH 修
  - `0e80ecc` 4 文档 R2/R3 audit 21 HIGH 修
  - `d113556` 4 文档 R3/R4 audit 16 HIGH 修
  - `f33fd20` 4 文档 R4/R5 audit 17 HIGH 修
  - `30b1fcc` CLAUDE.md "文档撰写纪律" 章节
  - `0c56fc5` CLAUDE.md "写 vs 审认知模式" 段
  - `65bb1c7` 4 文档 R6/R3 audit 14 HIGH 修
