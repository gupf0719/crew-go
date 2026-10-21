# Channel-Based Redesign Migration 历史

`redesign/channel-based` 分支：把 v1 `agent/` + `compose/` (~15000 行 GraphState-based)
迁到 v2 `agent_v2/` + `compose_v2/` (channel-based BSP 模型，参考 LangGraph Pregel)。

> **本文档目的**：跨 session 快速建立项目演化历史 + v2 公开 API 速查。
> Stage 7 已完成（v1 已删，v2 = main 唯一实现）；本文档保留作历史进度 + cheatsheet。
>
> **当初为什么转 channel-based**：见 [`why-channel-based.md`](./why-channel-based.md)
> （直接 trigger / 5 大根本设计问题 / PoC 单一判定 / 实际收益对比表）。
>
> **引擎层完整设计**：见 [`channel-engine.md`](./channel-engine.md)（核心抽象 / cp / Resume /
> fan-out / 嵌套 / 9 编排编译产物 / 43 项不变量速查 / 反模式）。
>
> **HITL + Resume 完整协议**：见 [`hitl-resume.md`](./hitl-resume.md)（InterruptError /
> AskQuery / InterruptID 稳定算法 / ResumeAction 三模式 / 多 tool_call 部分中断 /
> fan-out 并发中断 / 嵌套隔离 / mid-stream / plan_chat_web 实战）。
>
> **编排模式深入用法**：见 [`orchestration-patterns.md`](./orchestration-patterns.md)
> （9 编排选型决策树 / 每编排适用场景 + 完整最小例 + 关键 option + 反模式 / 嵌套组
> 合范式 / 横切能力）。
>
> **4 层上下文压缩完整设计**：见 [`context-compaction.md`](./context-compaction.md)
> （对标 Claude Code / 算法 / 阈值 / 9 段 / NO_TOOLS_PREAMBLE / 附件重注入 / 断路器
> / 时序图 / 排错决策树）。
>
> **业务侧用法**：见 [`usage-guide.md`](./usage-guide.md)
> （30 秒速览 + 9 编排 builder 表 + RunOption + HITL + 子图 + fan-out + 4 层压缩
> / Memory / Skill 业务示例）。

---

## 总览（Stage 1-7 路线图，全部完成）

| Stage | 内容 | 状态 | 主要 commit |
|---|---|---|---|
| 1 | SequentialAgent 迁移到 channel-based（验证模式）| ✓ 完成 | `74bd562` |
| 2-3 | 6 编排批量迁移（Parallel / Loop / Routing / Reflect / Handoff / GraphAgent）| ✓ 完成 | `a71022d` |
| 3.5 | 删 6 对老 PoC 低阶 helper（与新编排重叠）| ✓ 完成 | `150933d` |
| 4 | 11 个 v1 能力 gap 全补齐到 v2 | ✓ 完成 | `30a358e` |
| 5 | 7 项 v1 对齐核对补漏（C1-C7）| ✓ 完成 | `c1988ad` |
| 6.1 | 9 个 mock tests 迁 v2 + 修 streamOrGenerate annotation 丢失 bug | ✓ 完成 | `4b9b050` |
| 6.2 | 9 个 integration tests + adv_helpers 双版本 | ✓ 完成 | `3b6fb60` |
| 6.3 | 3 demo programs 迁 v2（basic_chat + plan_chat + plan_chat_web）| ✓ 完成 | `e5d5229` |
| docs (Stage 6.3 后) | 当时新增 migration-status.md（Stage 7-C 时被合并到本文档 migration-history.md 已删）+ CLAUDE.md/architecture.md/todo.md 加 redesign 提示 | ✓ 完成 | `462663f` |
| plan_chat_web mock smoke fix | interruptHook 补 Query 满足 v2 严格契约 | ✓ 完成 | `020dccb` |
| 7 | 重写 nested_smoke v2 + git rm 5 graph-level adv + git rm v1 (agent/+compose/) | ✓ 完成 | `ad2743e` |
| 7-C/D | 文档批量重写为纯 v2 + 交叉审核 | ✓ 完成 | (本 commit) |
| 7+ P1 | Skill 系统增强：toolCall + adaptive + pre_match 三模式（P1.B）+ listing 预算降级（P1.A）+ skill body 跨 compaction 保鲜（P1.C）+ 自定义 system hint（Task 2）| ✓ 完成 | (P1 系列 commits) |
| 7+ P2.A | Skill 二维控制：`SkillDescriptor.DisableModelInvocation` + `UserInvocable` 字段 + frontmatter parser + 3 处独立强制（listing 过滤 + Call 防御 + adaptive 候选过滤）；R12 audit MED-1 / R13 audit LOW-1 收敛（统一 bool 字段走 `parseBoolOptional` 3 态）| ✓ 完成 | (本 commit) |
| 7+ P2.D | Skill inline body：`WithInlineSkillBody(true)` 启用后 framework `activate_skill` 成功的 tool_result 退化为 terse ack，skill body 作为 `[system_reminder]` UserMessage 注入对话流（marker **terse-ack tool_result + body UserMessage 都打——R19**，AutoCompact 按 group 边界正常分组）；只对 framework 注入的 actTool 生效，业务工具不影响 | ✓ 完成 | (本 commit) |
| 7+ P2.C+B | Skill fork 模式 + per-skill model：`SkillDescriptor.Context = "fork"` 触发 sub-agent spawn；`WithSkillSubAgentResolver(name→Agent)` 业务注入预配置 sub-agent，或框架默认 BasicAgent（SystemPrompt = skill body + parent model 兜底）；`WithSkillModelResolver(name→ChatModel)` per-skill model 切换（仅默认 BasicAgent 路径）；`activate_skill({skill,query})` LLM 必传 query；`fork_with_context` **已删（2026-08-26）**——不在 agentskills.io 标准里，写它现在在解析期报错 | ✓ 完成 | (本 commit) |
| ISSUE-74/75 | BasicAgent 拓扑改 4 节点：preamble 节点 (Memory.Read / HistoryCompactor / Skill 预激活, ISSUE-74) + postamble 节点 (commitFinal + Memory.Write, ISSUE-75) — 抛 InterruptError 时走 graph 内 SaveNS 协议 cp 保留, Resume 自然续跑（**注**：Memory.Read/Write 在 PR 3.3 commit `e6505f9` 已切到 store.Search/Put，本表保留 ISSUE-74/75 commit 时代叙述）| ✓ 完成 | `b07c140` `9886a8a` |
| ISSUE-78 | AgentAsTool / SkillTool fork childNS 加 tc.ID 维度 — `WithCurrentToolCallContext(ctx, tcID, branchIdx)` 一站式 ctx 协议 + `JoinFanoutNS(parent, base, hash(tc.ID))` 防同 LLM 轮多 tc 时 cp 互覆 | ✓ 完成 | `e247106` |
| ISSUE-79 | AgentAsTool / SkillTool fork **默认半共享 child sess**（破坏性默认行为变化）：values 指针共享 + valuesMu 共享 + events 独立 + parentSess 反向指针 + RootSession 链 + RunID 沿用父 — 业务自定义 store.Search 用 `sess.RootSession()` 上追看父对话（**注**：旧叙述写"Memory.Read"，PR 3.3 切到 store.Search）| ✓ 完成 | `7c9c7db` `2fc997b` |
| ISSUE-80 | ParallelAgent fan-out lane events 物理隔离（学 eino lane 模型）— `RunSession.ForkLane()` + `CommitLaneEvents(parent, child)` + branchFn lane 隔离, 删 R31 distinct sub-agent name 限制 | ✓ 完成 | `2da4a77` `51cbbb7` |
| ISSUE-81 | BasicAgent toolsFn fanout 化（学 eino goroutine + WaitGroup）— N=1 inline 零开销 / N>=2 并发 + `InterruptError.Queries` 多 query 容器 + `MergeInterrupts(ies)` helper + `tool.Tool` godoc 加 reentrant 协议 | ✓ 完成 | `233644d` `f7b1ae8` |
| ISSUE-82 | OnAgentEnd hook 双触发 fix（学 eino 单一所有权 + LangGraph try/except 互斥）— 删 ConditionalEdge cond fn 内 fire, agent_v2.BasicAgent.Run defer 唯一源兜底所有退出路径（success / Interrupt / panic / ctx.Err）；**协议: Run defer 1 次 + 每次 Resume defer 1 次, HITL Resume 闭环算 2 次** | ✓ 完成 | `f9d6c51` `e56859d` |
| ISSUE-83 | PlanAgent 加 WithPlanHooks（与其他 8 编排架构对称）— PlanAgent 是 9 编排里唯一漏接业务级 hook.Hooks 的 agent, 修后 Run/Resume defer 调 OnAgentEnd 单一所有权兜底 (audit² 改: SafeOnAgentEnd helper 已删, 改 framework Run 入口 outer defer recover 兜底跟 eino 对齐) | ✓ 完成 | `14d47f5` `2656d04` |
| R32+ audit | 9 agent 单视角并发审 找 24 HIGH, 修 6 HIGH (PlanAgent NS 3 + graph_loop cond 2 + type registry 2) + 删 3 dead API (AddEventInGraph / SupersedeEventsByMarker / EnterFanoutBranchNS) + 删 `AgentEvent.NS` 字段与 `SupersededEventAnnotation` + 同步 9 文档 sync; 1 HIGH 曾留 ISSUE-86 跟踪 (cond visit 漂移)——**已于 2026-07-16 `70e0fcb` 根治**：禁止 cond interrupt + RoutingAgent 路由决策移到 classify node，visit 漂移根因消失（无需改核心 visit 协议） | ✓ 完成 | `91ef3d7` `23d75cb` `b1e5341` `70e0fcb` |

---

## 包依赖方向（v2 唯一）

```
schema ← model/tool/hook/skill/mcp ← compose_v2 ← agent_v2
```

v1 `agent/` + `compose/` 已删（Stage 7 commit `ad2743e`）。新代码一律用 `agent_v2 / compose_v2`。

---

## v2 核心类型速查（增量于 CLAUDE.md 主表）

### compose_v2

| 类型 / 函数 | 说明 |
|---|---|
| `Channel[T]`, `LastValue`, `Append`, `Sum`, `OnceFlag` | channel 抽象 + 4 种 reducer |
| `ChannelManager`, `Get`, `Write`, `ChannelWrite` | channel 读写 API |
| `GraphBuilder`, `NewBuilder`, `Compile`, `Graph` | 图构造 + 编译 |
| `NodeFunc func(ctx, Channels) ([]ChannelWrite, error)` | 节点函数签名（不用 GraphState） |
| `Run(ctx, g, cm, opts...)` | 单一执行入口（替代 v1 RunGraph/ResumeGraph） |
| `WithInitialWrites`, `WithCheckpointer`, `WithSessionID`, `WithRunNS`, `WithFreshRun`, `WithResumeFrom`, `WithResumeAnswers`, `WithResumeUserText`, `WithHooks`, `WithDurability`, `WithSoftCancel`, `WithStaleWriteGuard`, `WithEmitCpEvent`, `WithInputDigest`, `WithInputIdentity` | RunOption（15 个）。末两个是调用方交来的输入摘要：新开时记进存档、续跑沿用，auto-load 时比对——`WithInputDigest` 只核对（对不上告警、照样续），`WithInputIdentity` 的摘要就是调用的身份（对不上 = 撞槽 ⟹ 拒绝：不可重试、不清档——槽里那份属于另一次调用）；`agent_v2.RunSubAgent` 自动给 |
| `SaveNodeState` / `GetNodeState[T]`, `SaveStepResult(ctx, key, v, opts...)` / `GetStepResult[T](ctx, key)`, `Rerunnable()`, `RerunGeneration(ctx)`, `EnterPhase(ctx, scope)`, `HandOver(ctx, scope)`, `CallHook(ctx, call)` / `EnterHook(ctx)`, `WithNodeStateSealed(ctx, scope)`, `SubCallScope(ctx)` | 节点私有中断态：本节点这一次执行已经做成了什么，立即落 cm、中断 / 失败时随存档导出、重跑时读回跳过——**写在副作用提交之后**。前者整体一格，后者按 key 分条（`agent_v2.RunSubAgent` 用它记已完成的嵌套调用；业务 key 先哈希再入档）。`Rerunnable()` 标「这是产出」：被**否决**时作废、重跑时重做——节点自己的条件出边按 `CondFailRerunFrom` 否决 ⟹ 整次执行换代；它里面跑的嵌套图入口否决 ⟹ 只让产出那份输入的分域换代（经 `agent_v2.RunSubAgent` / AgentAsTool 调起的 agent 的主图算调用方产的）、等成员以这个错失败时才推进（标记没随错误传上来——业务 `%v` 包断了链、扇出只报一个错——时，成员以非控制流的错失败也推进；条件边被外面停下——软取消、陈旧写回、调用方 ctx 已经死了——不算否决）。`RerunGeneration` 返当前分域的有效代际（执行级 + 分域链上各层之和；交出去的那一段只算交接那一层往下的，调用方那一层的重做由调用的身份承担）。工具函数里按 tool_call 分域（BasicAgent 另按工具名再分一层）、图的钩子相位与条件边按相位分域、`HandOver`（嵌套调用被调方在图外的那一段、编排成员、决策 LLM、转交候选）/ `EnterPhase`（BasicAgent 的工具钩子，每次调用一个）给交出去的一段执行一个自己的相位——自己的一格、自己的提问与嵌套调用计数、自己的相位名（进提问身份），碰不到节点体那一格；`CallHook` 在采纳业务钩子返回的 ctx 时把节点体那一套连同其余执行状态取回来（执行状态一律取框架自己的，跟 `DetachExecution` 同一份清单）；`WithNodeStateSealed` 只按某个维度再分账（工具体按工具名）。`SubCallScope` 返当前相位名（节点体为空），嵌套调用的身份用它（只在一张图之内区分，跨图要连同 ns）。节点外、顶层图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边里是 no-op。详见 [checkpoint.md](./checkpoint.md) §5 |
| `NewSignalKind`, `EmitSignal` / `EmitSignals`, `CaptureSignals`, `JoinSignals`, `JoinSignalsFirstWins` + `SignalSuperseded`, `Signals{Take, Len}`, `Signal`, `SignalRecord`, `SignalConflictError`, `SignalRecipient`, `CallHook` / `EnterHook`, `ExecutionStateKey` / `EnvironmentKey` | **上行信号**：执行里做出、交给上方消费方的控制决定（转交 / 路由）。一个收件人上是一个单值格子：先后做出的后者为准；同一超步只能有一个成员做决定（两个都做了 ⟹ `SignalConflictError` 并带 `ErrRunAborted`，不重试、清档，值相同也算——`ParallelAgent` 的分支因此拿不到转交 / 路由能力）；业务经 `JoinSignals` 合并的几路值一致算一次、不一致同样报 `SignalConflictError` 并带 `ErrRunAborted`（往上交就终局，要自己裁决就接住或改用 `JoinSignalsFirstWins`）；同一批 tool_call 排在前面的生效（`JoinSignalsFirstWins`，后面给出不同值的那个调用回一条失败结果 `agent_v2.SupersededDecisionErrCode`；在全部 `OnToolEnd` 之后定，钩子改成失败的那次作废）。跟着写入者的提交走——节点成功并提交才并入图的已提交状态（`Checkpoint.Signals`；部分失败超步里成功成员的先记在 `CompletedSignals`，整超步完成时合并；schema v17），图成功跑完时交给调用它的那段执行；失败 / 中断 / 被否决的作废（已完成、已提交的部分在重试 / 续跑拿回时随记录再交出）；`RunWithRetry` 每次尝试单独收。同一超步的冲突在记录提问与提交之前返回（跟 `LastValue` 单写入者冲突同一个位置）。收件人由引擎铸（`SignalRecipient`：这个任务的密钥（一个任务一把，顶层新开时铸、嵌套图沿用）+ ns + 位置 + 分域的 HMAC，`Checkpoint.RecipientKey`；放进 ctx 时包遮罩，打印 ctx 读不出），经正常入口算不出来、手写信号 JSON 冒充不了（故意从存档读出密钥自己算的不在防护范围内）。取代 agent_v2 的 ctx 收件槽、路由 session KV 与 `tool.ControlEffectMetaKey` 声明机制（均已删），见 `compose_v2/signal.go` |
| `WithCheckpointerCtx`, `WithSessionIDCtx`, `WithThreadIDCtx`, `WithFreshRunCtx`, `WithDurabilityCtx`, `WithSoftCancelCtx`, `WithStaleWriteGuardCtx`, `WithOrchHooks` | RunOption 的 **ctx 写法**——agent_v2 九编排内部自己拼 RunOption、不透传业务的，编排层只能用这一组；嵌套子图也靠 ctx 继承 |
| `WithinNodeScope(ctx)`, `NodeScopeSessionID(ctx)` | 「当前在某个节点的执行范围内」（节点体、它的 `OnNodeStart`、从节点体调进来的嵌套图连同图级钩子），以及那次 Run 的 session ID（进节点那一刻记下，业务之后往 ctx 推别的 sid 改不了它）。agent_v2 用前者判顶层钩子抛的中断接不接得住，用后者取「所在任务」的身份——节点里发起的 Run 的 session RunID 必须就是它、对不上入口拒（要开独立任务先 `agent_v2.DetachTask`）；图在进节点之前把自己的 sid 重铺回 ctx，`OnGraphStart` / `OnNodeStart` 推的别的 sid 进不来 |
| `AddSubgraphNode(id, sub, WithImport(ch), WithExport(ch))`, `WithCondFailPolicy(CondFailAbort\|CondFailRerunFrom)`, `WithNodeComponent(kind)` | 装配期 option：子图节点 import/export / cond 报错的 Resume 语义 / node 组件类型（trace span kind）|
| `Durability`(exit/sync/async), `WithDurability`, `WithDurabilityCtx`, `GetDurability` | cp 写入时机：`exit`(默认=历史行为,仅出口 defer 写)/`sync`(每超步同步写,扛硬崩)/`async`(单 worker 后台写,FIFO+join-before-cleanup)；ctx 透传嵌套子图继承父档。详见 [checkpoint-storage.md](../description/three-way-comparison/checkpoint-storage.md) |
| `EstimateTokens`, `EstimateMessageTokens`, `GroupMessagesByAPIRound`, `AdjustIndexToPreserveAPIInvariants` | 4 层压缩共享 helper（业务自定义 compactor 用）。按 API usage 回填那支已收进包内（`estimateTokensFromAPIUsage` 小写，不对外）|
| `Has[T]`, `MustGet[T]` | channel 读 helper 补全 |
| `Checkpointer` 接口 | 5 个 ns-aware 核心方法 + `LoadAllInSession` / `SaveBatch` / `Cleanup`（C2 补；伪历史 `ListByThread`/`LatestByThread` 已删，ISSUE-98） |
| `Checkpoint{ID, NS, ParentNS, Status, SchemaVer, GraphID, TaskID, Epoch, ...}` | cp 物理结构。**`ThreadID` 不在 cp 里**——它是 ctx 维度（`WithThreadIDCtx`），cp 这层不认识会话；全字段见 [checkpoint.md](checkpoint.md) |
| `InMemoryCheckpointer` | 内置实现 |
| `OrchestrationHooks` | 5 回调签名都带 `Channels`（不带 GraphState）|
| `ChainOrchHooks`, `NewChainOrchHooks(hooks...)` | 多 OrchHooks 链（fail-fast，**无** per-hook panic 隔离——audit² 删过度设计，panic 冒泡由 framework Run 入口 outer recover 转 err；ISSUE-63 P1 增量）|
| `NoOpOrchestrationHooks`, `RecordingHooks` | 空 hook + 测试 helper |
| `Event{Type, Content, Metadata}` | OnNodeEvent 简化版 event（不用 *AgentEvent）|
| `EventSink`, `GetEventSink(ctx)` | 节点内主动发 event |
| `WithOrchHooks(ctx, hooks)`, `GetOrchHooks(ctx)` | ctx-aware hook 透传（C1 补） |
| `Graph.ID()` | 防 cp 串台——`cp.GraphID = g.ID()` 直读（C4 补；WithGraphID/GetGraphID ctx pair 后废弃删除）|
| `RetryConfig`, `SimpleRetry`, `ExponentialBackoff`, `RunWithRetry[T]`, `IsRetryable`, `BackoffDelay`, `SleepWithContext` | 重试机制（B1 补） |
| `MaxIterationsError` (BasicAgent（maxRounds+1）/ LoopAgent / Graph maxVisit 触顶时 `compose_v2/graph_loop.go::Run` 内实际抛 — 按 `MaxIterationsError` 符号自查; `errors.As` 解析具体 `Iterations` 值), `MaxRoundsError` (typed 但生产路径不抛 — 预留供业务自定义), `ErrNoSubgraphCheckpoint`, `ErrGraphIDConflict`, `ErrRunAborted`（终局失败不可 Resume，Run 出口主动清 cp 子树；`IsRetryable` 已排除）, `ErrNoPendingCheckpoint`（Resume 对「没有存档 / 存档里没有可续的内容」统一 fail-fast；失败现场的档自 2026-08-13 起放行，`Resume` 不带答案 = 重试失败分支）, `ErrCheckpointStale`（`WithStaleWriteGuard` 开启时的并发写回检测）, `ErrSoftCanceled`（软取消，超步边界善终、可 Resume）, `ErrCheckpointNotResumable`, `ErrInvalidNS`, `ErrInvalidNodeIDForNS` | 哨兵错误 9 个（C3 补 + audit follow-up MaxIterationsError 实际抛勘误）|
| `RegisterValueType[T]`, `LookupType`, `LookupKey`, `MarshalTagged`, `UnmarshalTagged` | type registry + tagged JSON 序列化（C5 补） |
| `AdjustIndexToPreserveAPIInvariants` | 切点配对感知（C7 补） |
| `InterruptError{Reason, Query, Queries, Data}` | error-as-control-flow（B6.3 加 Data；ISSUE-81 加 Queries 多 query 容器：单 query 用 Query，多 query 用 Queries——两字段互斥 Queries 优先）|
| `(e *InterruptError) AllQueries() []*AskQuery` | 协议 helper：Queries 优先 → fallback [Query]——record / 形状校验路径统一访问（ISSUE-81）|
| `MergeInterrupts(ies []*InterruptError) *InterruptError` | 多 InterruptError 聚合到单 ie.Queries——toolsFn fanout 多 tc 同时抛 Interrupt 用，让 graph_loop record 全部 query 防 N-1 query 永久丢失（ISSUE-81）|
| `AskQuery{Question, Options, InterruptID, Reason}` | HITL 中断 payload, 6 字段; InterruptID **必填**（走 `Ask` 或自填，框架不补）（v2 删过 v1 时代的 ToolCallID/NS，后又按需加回 ToolCallID/ToolName（commit `3fb2c65`，标出挂起卡的是哪次工具调用；当前 cp schema 版本见 `compose_v2.CheckpointSchemaVer`）；ns 维度仍不在这里（cp 按 (sid, ns) 物理分离寻址）; Reason 是 audit #5 视角 8 F3 加, per-query 中断原因, MergeInterrupts 自动 propagate ie.Reason → q.Reason）|
| `ResumeContext`, `GetResumeAnswer`, `GetResumeAnswerText`, `GetResumeUserText`, `GetResumeContext`, `StripResumeContext` | Resume 协议 ctx helpers（`GetResumeAnswer` 按 InterruptID 精确取 = **寻址型**；`GetResumeUserText` 取「用户这轮说了什么」= **非寻址型**。曾有个取 map 里任意一条的 `GetResumeAnswerSingle`，会让节点吃到别人的答案，2026-08-06 已删）|
| `Ask(ctx, q)`, `CurrentInterruptSite(ctx)`, `InterruptSite.InterruptID()`, `NextInterruptIndex(ctx)` | 中断寻址：`Ask` 是业务提问的唯一推荐入口，ID 按执行位置派生（`InterruptSite` 另有两维只在被否决之后才计入：`RerunGen` 所在分域的重做代际、`OuterRerunGens` 嵌套图里承载它的每一层外层执行的代际）|
| `WithCurrentToolCallContext(ctx, tcID, toolCallIdx)`, `GetCurrentToolCallID(ctx)` | toolsFn ctx 协议**唯一写侧**（单维的 `WithCurrentToolCallID` 已随 `309cddf` 删除，串行 N=1 也走这个）。一站式注入六样：「在 tool_call 里」的章（`InToolCall`——判在不在 tool_call 里看它、不看 ID 非空；`HandOver` / `EnterPhase` 在交出一段执行时摘掉它）+ tc.ID + tc 下标（批内第几个，不是图级分支下标）+ 独立的提问计数器（atomic.Int64，ISSUE-81 toolsFn 并发化）+ 独立的嵌套调用次序计数器（`NextSubCallIndex`）+ 节点私有态的 tool_call 分域——业务自定义 fanout toolsFn 必须在 goroutine 入口调；业务工具抛 InterruptError 提问走 `compose_v2.Ask`（自填 ID 会被拒）（ID 用 ctx 里注入的 tc 下标等位置维算）|
| `JoinNS`, `ParentNS`, `IsNSInSubtree`, `NSSubtreeSeparator`, `WithGraphNS`, `GetGraphNS`, `ValidateNodeIDForNS`, `JoinFanoutNS`, `JoinVisitNS` | ns 物理分离。`agent_v2.RunSubAgent` 派生的子 ns 恒为 `JoinFanoutNS(parentNS, nodeID, 身份哈希的低 63 位)`（`JoinFanoutNS` 的下标与 `NSSegment.Index` 都是 int64，与平台字长无关）：身份 = **位置**（调用方所在的节点与相位 + 实例维三选一：图级 fan-out 实例下标 / 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态、cond 路径的 visit；fan-out 实例里又起 tool_call 时两维一起进位置）+ **这是哪一次调用**（输入内容 + 同一份输入在本次执行里的第几次（`NextSubCallIndex`），或 `agent_v2.WithCallKey` 给的 key；全历史输入的 `AgentAsTool` 与 skill fork 只看位置）。父节点重跑时已完成的调用拿回记下的结果（`SaveStepResult`）、不再执行。业务侧**别手拼**——判定住在 `RunSubAgent` 里，`compose_v2` 侧的子图节点另有一份二分支实现（`subgraph.go`，无 tcID 那支）。**`IsNSInSubtree` 定义 `DeleteSubtree` 的删除范围**：自身 + 经 `/` 往下的后代，`#`/`@` 兄弟执行不删（2026-09-23 取代 `IsNSAncestor`——后者把兄弟算作后代，一次执行的成功清理会删掉兄弟执行还挂着的存档；`NSSubtreeSeparator` 取代 `NSSegmentSeparators`，redis Lua 从它注入分隔符） |
| `SendFrom`, `SendDirective` | 超步 fan-out 注入分支 |

### fallback（模型故障切换装饰器）

| 类型 / 函数 | 说明 |
|---|---|
| `FallbackChatModel`（实现 `model.ChatModel`）| 有序候选故障切换；注入 `agent_v2.WithModel` 对 9 编排透明；Generate + Stream（仅建流阶段切）+ WithTools（复用 breaker 指针）|
| `New([]model.ChatModel, ...Option)` | 构造；models[0] 主其余备用；空 / 含 nil（含 typed-nil）panic |
| `FailoverPolicy` 接口, `DefaultFailoverPolicy` | 错误分类：`APIError` StatusCode 0(网络)/408/429/5xx 切；其他 4xx + InterruptError + ctx 取消不切 |
| `WithFailoverPolicy / WithPerModelRetry / WithBreaker / WithFallbackHook` | Option：自定义分类 / per-member 重试 / per-member 断路器（maxFailures best-effort，并发 burst 首窗口 overshoot）/ 切换回调（并发触发，业务回调共享态需自锁）|
| `ErrAllMembersUnavailable` | 全员熔断哨兵（`errors.Is`）|
| `model.APIError{Provider,StatusCode,Body,Err}`（model 包）| 配套结构化错误（openai client 回填，qwen/deepseek 复用受益；`IsPromptTooLongError` 检测复用）|

> 独立成包（import model+compose_v2；放 model 包会反转分层 + compose_v2 一旦 import model 即成环）。详见 [model-fallback.md](model-fallback.md)。

### agent_v2

| 类型 / 函数 | 说明 |
|---|---|
| `Agent` 单接口 | `Run(ctx, sess, input, opts...) ([]*AgentEvent, error)` + `Resume(ctx, sid, opts...)` + `Name() / Description()` |
| `BasicAgent` (ReAct) | preamble + model + tools + postamble **四节点** channel-based Graph + 循环回边（ISSUE-74/75）；tools 节点内 N>=2 tool_call **goroutine 并发** fanout（ISSUE-81，N=1 仍 inline）|
| `SequentialAgent`, `ParallelAgent`, `LoopAgent`, `RoutingAgent`, `ReflectAgent`, `HandoffAgent`, `PlanAgent`, `GraphAgent` | 8 编排 + BasicAgent = **9 编排**（PlanAgent ISSUE-83 后接业务级 hook.Hooks 与其他 8 编排架构对称）|
| `BasicAgentBuilder`, `SequentialAgentBuilder`, `ParallelAgentBuilder`, `LoopAgentBuilder`, `RoutingAgentBuilder`, `ReflectAgentBuilder`, `HandoffAgentBuilder` | 7 个低阶编排 builder（构造 `NewBasicAgentBuilder` / `NewSequentialAgentBuilder` / …；从 compose_v2 引擎包移入，每个对应同包高阶 wrapper）；plan **无低层 builder**（已 inline 进 `PlanAgent`，commit `856bb6e`）|
| `RunSession` 半共享 sess 协议 | `NewSharedChildSession(parent)` (ISSUE-79: AgentAsTool / SkillTool fork 默认半共享 child sess) + `ForkLane()` / `CommitLaneEvents(parent, child)` (ISSUE-80: ParallelAgent fan-out lane events 物理隔离) + `ParentSession()` / `RootSession()` (ISSUE-79 反向指针访问父链, 业务自定义 `store.Store.Search` 用 RootSession() 上追看父对话历史) |
| `ResumeOption`（8 个）| `WithResumeUserInputs(map[InterruptID]*Message)`（按 InterruptID 配对答案，HITL 闭环主入口）, `WithResumeSession(sess)`（复用 session；`approve_always` 规则、a2a taskID 续跑必配）, `WithResumeAction(Continue\|Replan\|Abort)`, `WithResumeUserText`, `WithResumeModel`, `WithResumeModelOptions`, `WithResumeMaxRounds`, `WithResumeOrchHooks` |
| `RunOption`（7 个，agent_v2 层）| `WithRunModel`, `WithRunModelOptions`（per-Run thinking 等透传）, `WithRunSystemPrompt`, `WithRunMaxRounds`, `WithOrchHooks`, `WithCallKey`（只对 `RunSubAgent` 生效：给一次嵌套调用业务声明的身份——输入每次都会变、却仍是同一件事时用；输入完全相同的几次要按进度跳过 / 重排时也用它）, `WithRerunnable`（只对 `RunSubAgent` 生效：这次调用的结果是父节点的产出，被否决之后重做的是**整次调用**——身份带上所在分域的重做代际；不声明时怎么重跑都拿回——同一个节点里的扣款、问过人的把关不该跟着重做）|
| `Config` + `Option` 函数式 | `WithName`, `WithDescription`, `WithSystemPrompt`, `WithModel`, `WithToolkit`, `WithHooks`, `WithMaxRounds`, `WithMicroCompact`, `WithAutoCompact`, `WithReactiveCompact`, `WithStore`, `WithStoreMemory`, `WithHistory`, `WithSkills`, `WithMaxHistoryPairs`, `WithHistoryCompactor`（共 38 个，下面三行是本表原先没列的）|
| 工具治理 Option | `WithApprovalMode` / `WithApprovalHeuristic` / `WithApprovalScope`（审批层）, `WithToolSearch`（渐进披露）, `WithToolFilter`（按轮裁子集）, `WithToolMiddleware`（包住每次调用）, `WithToolTimeout` |
| 输出 / 提示 Option | `WithOutputSchema` + `WithMaxValidationRetries`（结构化输出）, `WithPromptTemplate`, `WithAgentsMD`（harness 指令层 transient prepend）, `WithEmitCpEvent` |
| skill 细粒度 Option | `WithSkillSelector`, `WithSkillSystemPrompt`, `WithSkillListingBudget`, `WithSkillSelectionHook`, `WithSkillBodyMaxChars`, `WithInlineSkillBody`, `WithSkillModelResolver`, `WithSkillSubAgentResolver`, `WithSkillActionFunc` |
| GraphAgent Option | `NewGraphAgent(g, WithGraphAgentName, WithGraphInputWriter, WithGraphOutputReader, WithGraphAgentHooks)` |
| 会话历史层（ISSUE-98，opt-in；**配置挂在 BasicAgent 的 `WithHistory`，写入器由编排层拥有**——七编排 Run/Resume 14 入口 + GraphAgent 2 入口经 `startHistoryForOrchestration` 借子 agent 的配置启动，非 BasicAgent 执行体由边界补记兜底）| `WithHistory(hs, owner, opts...)`, `HistoryStore`（Append/Load 两方法）, `HistoryKey{Owner, ConvID}`, `NewInMemoryHistoryStore`, `WithHistoryOwnerFunc`, `WithoutHistoryCtx`（内部决策 LLM 摘除）, `HistoryAgentAnnotation`（归属）, `DerivedInputAnnotation` + `MarkDerivedInput`（业务自写编排合成输入时标派生，避免重复内容进历史）；业务自研 backend 跑 `agent_v2/history_conformance.RunHistoryStoreTests` |
| `RunSession{RunID}` + `History/SelfHistory/SelfHistoryCompacted/HistoryFor/HistoryForRaw/RestoreEvents/AddEvent/SetValue/GetValue/Events/ForkLane/CommitLaneEvents/RootSession/ParentSession` | 富 API（ISSUE-79 半共享 child sess + ISSUE-80 lane events + ISSUE-84 HistoryForRaw owner-aware；audit follow-up 删 stale `AddEventInGraph` / `SupersedeEventsByMarker`）|
| `AgentEvent`, `NewOutputEvent`, `NewExitEvent`, `NewChunkEvent` | 事件构造 |
| `FinalMessage(events)` | 提取 final 消息 |
| `store.Store` 接口, `store.NewInMemoryStore`, `WithStore`, `WithStoreMemory`, `StoreMemoryConfig` | 长期记忆（PR 3.3 commit `e6505f9` 切换：`Memory` / `MemoryConfig` / `InMemoryMemory` / `SpecMemoryInterface` / `WrapMemory` 整套已删；详见 `store.md`）|
| `MicroCompactConfig/Run`, `AutoCompactConfig/Run`, `ReactiveCompactConfig/Run` | 4 层压缩第 1/2/3 层 |
| `StoreInjectedAnnotation`, `AutoCompactSummaryAnnotation`, `MicroCompactClearedAnnotation`, `MicroCompactOffloadedAnnotation`, `CompactionSummaryAnnotationKey`, `SkillBodyAnnotation` | message annotation marker 常量（4 层压缩跨层一致防护; PR 3.3 `MemoryInjectedAnnotation` 改名 `StoreInjectedAnnotation`; audit follow-up 删 stale `SupersededEventAnnotation`; `MicroCompactOffloadedAnnotation` 配套 `MicroCompactOffloadKeyAnnotation` 存 offload key）|
| `WithSubAgentStoreScope(ctx, true)` + `IsSubAgentStoreScope(ctx)` | `store.Put` 跨 ns 协议 ctx helpers (PR 3.3 改名自 `WithSubAgentMemoryScope`/`IsSubAgentMemoryScope`；ISSUE-84 曾让 `BasicAgent.commitFinal` 据它跳过 store.Put。⚠️ **框架自己已不读这个 flag**——那道闸让成员配的 `WithStoreMemory` 一条也写不进去，已撤；防重复写改由「写入者维进 ns」（顶层写基准 ns、成员写 `基准ns/<位置>`）。业务后端可以读它做自己的判断，但别用它整个 skip。**push 点收敛在 4 处**——`runMember` / `runDecisionAgent` / `runNestedCall`（RunSubAgent + AgentAsTool + SkillTool fork）/ Handoff 铸造，见 `agent_v2/member_dispatch.go` 与 `agent_v2/run_subagent.go`)|
| `WithSession(ctx, *RunSession)` / `GetSession(ctx)` | sess ctx helpers (业务自定义 `store.Store` 实现用) |
| `DetachTask(ctx)` / `compose_v2.DetachExecution(ctx)` | **开独立任务的显式动作**：剥掉当前执行留在 ctx 上的状态（session 与任务章、节点位置与 ns、任务代号、新开信号、续跑答案与兑付账本、节点私有态访问器、嵌套调用计数与记录器、历史写入器、转交 / 路由的收件人、信号收集处、收件人密钥与节点授予……），留下调用方配的环境（checkpointer、编排钩子、调用主体、历史配置、store、durability、ctx 的取消）；会话 ID 与软取消信号也剥（独立任务不属于当前会话、不继承上一个 Run 的停止）。在一个正在执行的任务里调 agent 时 session 的 RunID 必须属于这个任务、Resume 一律拒——要给另一个会话开任务、续跑另一个挂起的任务就先调它。脱离之后 RunID 不能是这一路脱离出来之前所在的任何一个仍在执行的任务（`compose_v2.DetachedFrom`，脱离几层、直接用 `DetachExecution` 都算；「仍在执行」按任务判，任务挂起之后用节点里存下的 ctx 脱离再续跑它不算，它又续跑起来之后照样算；不在节点执行范围里的上层用 `compose_v2.DetachExecutionFrom` 补上自己的任务）；独立任务的挂起不能从节点返回（引擎终局拒收，原样返回、取出来返回、再包一层都一样）；入口拒绝恒不可重试。自研 `Agent` 在 Run 入口调 `EnterRun` 走同一个入口（判 session、盖任务章、遮节点授予，`a2a.RemoteAgent` 已接）。每个 ctx key 都要归类——执行状态或环境（上层包的实现 `compose_v2.ExecutionStateKey` / `EnvironmentKey`，机械测试扫全包）|
| `SkillDisabledErrCode` (= `"skill_disabled"`) | **tool error code 非 message marker**——P2.A `activate_skill` 拒绝 disabled skill 时填 `tool.Result.Error.Code`；不在 `tool.IsFrameworkError` 列表，反馈 LLM 让其换 skill 重试 |
| `SkillDescriptor.DisableModelInvocation`, `SkillDescriptor.UserInvocable` | P2.A 二维 skill 控制字段（frontmatter `disable-model-invocation` / `user-invocable`）；DisableModelInvocation 默认 false 强制 3 处独立防御；UserInvocable 默认 true 框架不强制（业务自行用此字段过滤 user-facing 列表）|
| `WithInlineSkillBody(bool)` | P2.D inline body 协议：`WithInlineSkillBody(true)` 启用后 framework `activate_skill` 成功的 tool_result 退化为 terse 激活 ack，skill body 作为 `[system_reminder]` UserMessage 注入对话流；`SkillBodyAnnotation` marker **terse-ack tool_result + body UserMessage 都打（R19——两 group 都受压缩保护，否则 tool_call group 整组压进 summary 丢激活上下文 + 语义倒置；非 422）**。仅 toolCall 模式 + framework 注入的 actTool 时生效——业务自定义同名工具不走 |
| `WithSkillModelResolver`, `WithSkillSubAgentResolver`, `SkillModelResolver`, `SkillSubAgentResolver`, `SkillForkErrCode` | P2.B/C fork 模式协议：`SkillDescriptor.{Context,Agent,Model}` frontmatter 字段触发 fork sub-agent；resolver 业务注入；`activate_skill({skill,query})` LLM 必传 query；`SkillForkErrCode="skill_fork_failed"` 业务级失败让 LLM 重试 |
| `SkillDescriptor.Context`, `SkillDescriptor.Agent`, `SkillDescriptor.Model` | P2.B/C 三 frontmatter 字段（`context: inline\|fork` / `agent: <name>` / `model: <name>`）；`fork_with_context` **已删（2026-08-26）**，写它现在在解析期报错 |
| `OrchestrationHooks`（高层简化签名——不带 Channels，nodeID 是 `string`）| 5 回调全 ctx-based，与 compose_v2 版同形：`OnGraphStart(ctx) (context.Context, error)` / `OnGraphEnd(ctx, err) context.Context` / `OnNodeStart(ctx, nodeID) (context.Context, error)` / `OnNodeEnd(ctx, nodeID, err) context.Context` / `OnNodeEvent(ctx, nodeID, event) context.Context` |
| `WithCtxOrchHooks(ctx, h)`, `GetCtxOrchHooks(ctx)` | agent_v2 层独立 ctx helpers（C1 补，`extractOrchHooksOrCtx` 是包内 helper 不导出）|
| `compose_v2.WithSessionIDCtx(ctx, sid)` / `GetSessionID(ctx)` | **写侧** `WithSessionIDCtx`：非 Run 入口路径业务/observability 显式 push sid 到 ctx (Sub-5f 补，Run 入口已自动 push 业务通常不调；observability 测试 + plan_chat_web 类 demo 直调 ag.Run() 用)。**读侧** `GetSessionID(ctx) string`：业务自定义 NodeFunc / 子图节点内取当前 sid（如 MicroCompact offload per-sid NS 派生）。九编排 Run/Resume 入口经 `agent_v2.WithSession` 把 sid=RunID push 进 ctx（RunID 非空时，覆盖已有的——任务的身份就是 session 的 RunID，ctx 上的 sid 只是它的副本；嵌套位置上 session 的 RunID 必须是所在任务的、跟父相同，沿用父 sid；业务显式推的别的 sid 不算数），**与配没配 checkpointer 无关**；只有直接调 `compose_v2.Run` 且不带 `WithSessionID` 的裸图路径才可能读到空串 |
| `compose_v2.WithThreadIDCtx(ctx, convID)`, `GetThreadID(ctx)` | 跨 run 会话 id (observability `gen_ai.session.id` 来源)；业务 opt-in push **跨 run 不变**的对话 id 把多次 Run 聚类，不 push 则不 emit（对标 LangGraph thread_id；区别于 `WithSessionIDCtx` 的 per-run sid = `gen_ai.request.id`）|
| `observability.{New, AgentHooks, OrchHooks, Counter, Histogram, Gauge, CleanupSession, MetricDroppedCount, MetricCardinalityDropped, Close}` | Phase 2 Sub-5 完整 observability provider — trace span 自动 OTel `gen_ai.*` attrs (含 request.id / session.id / system / response.model，对齐 AI_TRACE_SPEC) + 25 framework auto-fire metric（31 个 name 常量，另 6 个业务自填） (label OTel 对齐 `gen_ai.agent.name` / `gen_ai.request.model` / `gen_ai.system` 等). cap 默认 10000 防业务高基数 label OOM. 详见 [observability-callback.md](../description/three-way-comparison/observability-callback.md) + [observability-trace-spec.md](../description/three-way-comparison/observability-trace-spec.md) |
| `With{Sequential,Parallel,Loop,Routing,Reflect,Handoff,Plan}RetryConfig(compose_v2.RetryConfig)` / `...MaxRetries(n)` | 七编排重试接入。参数直接用 `compose_v2.RetryConfig` / `SimpleRetry` / `ExponentialBackoff`——agent_v2 **不** re-export；`MaxRetries>0` 时 `ShouldRetry` 必填（装配期 `ValidateRetryConfig` 拦）|
| `IsPromptTooLongError`, `ParsePromptTooLongGap` | 第 3 层 ReactiveCompact retry 触发判定（regex 解析 OpenAI/Claude prompt-too-long 错误；业务自定义 ChatModel 复用）。**在 agent_v2 不在 compose_v2** |
| `RoutingChoice`, `RouteTool` const, `WithRouter`, `WithRoutingClassifier`, `WithRouteTool`, `RegrantRouteTo`, `RegrantFromNode`, `WithRoutingFallback`, `WithRoutingDescriptions` | RoutingAgent 的决策来源：业务规则 Router 函数，或决策方 → LLM Classifier → 兜底组成的升级链；两者互斥，同时配构造期 panic（B2 补，2026-09 改互斥） |
| `RouteDecider`, `RouteRequest`, `RouteDecision`, `RouteEscalation`, `WithRouteDecider`, `WithBranchMinConfidence`, `RouteUndecidedError`, `RouteDecisionOf`, `RouteReason*` / `RouteStage*` 常量 | 带置信度的路由决策：拿不准逐档升级；路由决策事件记下没被采纳的决策方，以及 classifier 失败的原因；每一档都没决定又无处升级时返回 `RouteUndecidedError`（2026-09，见 [route-decider.md](route-decider.md)）|
| `decision.Client` + `Decide`, `decision.Protocol`, `decision.NewRouteDecider`, `decision.NewClient` + `Preset`, `jev.NewClient`, `bailian.NewClient` + `bailian.Beijing` / `Singapore` | 决策模型客户端与路由决策方（`decision` 包，2026-10 由 `jev` 包改名拆分）：两家服务方各一个子包——`decision/jev` 直连 TypeSafe 的 Jev，`decision/bailian` 接阿里云百炼的 decision-model-preview（`NewClient`；线上格式 System One 与 OpenAI Decisions 由 `Preset.Protocol` 构造时选）——构造出的都是 `*decision.Client`（见 [route-decider.md](route-decider.md)）|
| `Reflector`, `EvalFunc`, `ReflectOpinion`, `WithReflector`, `WithEvalFunc`, `WithEvaluator`, `WithReflectorAgent` | ReflectAgent 三档评估（Reflector 函数 / Evaluator+ReflectorAgent / EvalFunc 简化；**R33 起互斥 fail-fast**——多档同配构造期 panic）|
| `Aggregator`, `WithAggregator`, `AggregatorToMerger` | ParallelAgent 简化聚合（B6 补） |
| `WithHandoffTransfer`(sub-agent opt-in), `HandoffTransferToolName`, `WithHandoffHistoryMode`(Rewrite默认/Raw/SelfOnly), `HandoffPrompt`, `(h *HandoffAgent).EndConversation`, `State`→`HandoffStateSnapshot` | Handoff **模型 B**：LLM 自主转交(transfer 工具 return-direct) + 持久 active(store.Store 按 ThreadID) + 历史可见性。详见 [handoff.md](./handoff.md) |
| `PlanStep{Description, SubAgentName, TmpSubAgent, InputRefs}`, `RefSpec{Index *int, AgentName}`, `IndexRef`, `AgentRef`, `ExecutedStep`, `Planner`, `ExecInputBuilder`, `TmpSubAgentConfig` | PlanAgent 类型集（B8 补完整能力对齐） |
| `(p *PlanAgent) AddSubAgent / SetExecutor`, `WithSubAgents`, `WithPlanModel`, `WithPlanToolkit`, `WithExecInputBuilder`, `WithPlanMaxSteps`, `WithPlanRetryConfig`, `WithPlanMaxRetries`, `WithPlanHooks` (ISSUE-83 加业务级 hook) | PlanAgent options |
| `AskUserToolName` const, `WithAskUser()` | HITL ask_user 内置工具集成 |
| `AgentAsTool`, `RunnableAsTool`, `WithAgentToolCallKey(fn)` | Agent 包成 tool（plan_chat_web 用）。每次 Call 是一次嵌套调用：一律记完成记录（只记最终那条消息）；产出会被 tools 节点写成 tool_result 时不补记历史（看 tools 节点盖的章，不看在不在 tool_call 里），别处补记；身份默认按交给被调方的输入（默认 schema 只是 `input` 字段，自定义 schema 是整个 args），注入的每次都变的值落进了输入时用 `WithAgentToolCallKey` 从 args 取稳定的 key |
| `NewLLMPlanner(m, opts...) Planner`, `LLMPlannerOption`, `WithLLMPlannerSubAgents`, `WithLLMPlannerSystemPrompt` | 内置 LLM 驱动 Planner（**没有导出的 `LLMPlanner` 类型**，构造函数返 `Planner` 接口）|
| `HistoryCompactor func(ctx, msgs, keepPairs) ([]*Message, error)` | 第 0 层兜底压缩（B4 补） |

---

## v2 vs v1 关键差异（迁移 cheatsheet）

| 概念 | v1 | v2 |
|---|---|---|
| 节点函数签名 | `(ctx, *GraphState) (*GraphState, error)` | `(ctx, Channels) ([]ChannelWrite, error)` |
| 状态传递 | GraphState.Messages / LastOutput / typed local state | channel + reducer |
| 入口 | `RunGraph` / `ResumeGraph` | `Run` 单入口（含 Resume） |
| 构造 builder | `NewGraphBuilder` + `Build` | `NewBuilder` + `Compile` |
| Subgraph | `AddGraphNode` (头等节点) | `AddSubgraphNode` |
| fan-out | `AddFanOutEdge` | `SendFrom` + `SendDirective` |
| 嵌入运行 ctx | `EnclosingParent` | `WithGraphNS` push ns |
| ParallelAgent constructor | `NewParallelAgent([]AgentNode{}, opts...)` | `NewParallelAgent(WithParallelName("p"), opts...).AddBranch(name, ag)` 链式（**`With<X>Name` 构造期必填**，七编排同款 fail-fast panic；BasicAgent / PlanAgent 例外）|
| SequentialAgent constructor | 同上数组 | `NewSequentialAgent(WithSequentialName("s")).AddStep(name, ag)` 链式（同上必填）|
| LoopAgent ExitCondition | `(sess, round, msg) bool` | `(ctx, lastOut, round) (bool, error)` |
| RoutingAgent Classifier 路径 | classifier agent + RouteTool 文本 | `WithRoutingClassifier(ag)` + `WithRouteTool()`（`route_to` tool 主）/ `route_target` annotation 兜底 |
| HandoffAgent 转交信号 | message 内文本 "HANDOFF: target" + `extractHandoffTarget` 解析 | **`transfer_to_agent` 工具**（LLM 自主, return-direct）+ 程序化成员用 `agent_v2.HandoffTo`（模型 B，两条做出的是同一类上行信号）；annotation 通道已删，`HandoffSignalKey` 只是 stepFn 写给条件边的内部字段|
| PlanAgent constructor | `NewPlanAgent(planner, executor, opts...)` | `NewPlanAgent(planner, opts...).SetExecutor(ag)` |
| ReflectAgent constructor | `NewReflectAgent(actor, WithEvaluator(eval), WithReflector(refl))` | `NewReflectAgent(WithReflectName("critic"), WithEvaluator(eval), WithReflectorAgent(refl)).WithActor(actor)` |
| OrchestrationHooks 签名 | `OnNodeStart(ctx, NodeID, *GraphState) error` 等 | `OnNodeStart(ctx, NodeID, Channels) (context.Context, error)` 等 (audit² ctx-based, 跟 eino Handler.OnStart(ctx)→ctx 对齐) |
| OnNodeEvent payload | `*AgentEvent` | `Event{Type, Content, Metadata}` |
| Checkpointer 接口 | 17 方法（旧 + ns + thread） | 8 方法（NS-aware 核心；跨 Run 会话历史移出到 agent_v2 历史层，ISSUE-98） |
| RunSession.RunID | 字段同名 | 字段同名 |
| `GetResumeAnswerSingle` | 返 `(*Message, bool)` → 返 `(string, bool)` | **2026-08-06 整个删除**，非寻址型读法换 `GetResumeUserText` |
| typed local state | `ProcessState[T]` + `NewStateFactory` 主流方式 | **已删**（`compose_v2/state.go` 整个文件移除）；channel（`Channel[T]` + reducer）是唯一 state 模型 |
| TypedGraph | `TypedGraph[I,O]` 泛型包装 | 不需要（channel 本身类型安全） |

---

## typed nil 防护协议 (R32+ audit²-²⁷ 累计 71 真 HIGH 修)

crew-go 跨 9 包（8 个名字）系统化防 Go interface "typed nil" 经典陷阱:

```go
// Go interface "typed nil" 陷阱:
// var m *MyModel  (typed nil pointer)
// 隐式转 model.ChatModel interface → header 持非 nil type tag
// 后续 m == nil 是 false → 业务 m.Generate() 远 stack panic 难定位
```

**9 包 8 名 helper mirror**（按名字 grep 找不全，得按形状找）:

| 包 | helper | 防护 type 集 |
|---|---|---|
| `agent_v2/agent_helpers.go` | `isFrameworkNil(any)` + `isAgentNil(Agent)` | Ptr/Interface/Map/Slice/Chan/Func/UnsafePointer (reflect.IsNil) |
| `compose_v2/helpers.go` | `isFrameworkNil(any)` | 同款 |
| `tool/helpers.go` | `isToolNil(Tool)` | 同款 |
| `hook/helpers.go` | `isHooksNil(Hooks)` | 同款 |
| `mcp/helpers.go` | `isClientNil(Client)` | 同款 |
| `store/context.go` | `isFrameworkNil(any)` | 同款（第三个叫这个名字的）|
| `fallback/fallback.go` | `isNilModel(model.ChatModel)` | 同款 |
| `a2a/options.go` | `isNilIface(any)` | 同款 |
| `a2a/server/serve.go` | `mustAgent(agent_v2.Agent)` | 同款，但**panic 不返 bool**——形状不同，按名字 grep 找不到 |

**入口覆盖 30+ 处** (业务 With* options + builder + 工厂 + 注册入口). 详见
[`usage-guide.md` §8 "Go interface typed nil 陷阱"](usage-guide.md#-go-interface-typed-nil-陷阱--27-轮-audit-修-71-真-high-的根源).

**对比三方**:
- **eino** (Go): `compose.graph::addNode` 完全不防 typed nil — crew-go 优势
- **langgraph** (Python): `None` 单一值无此陷阱, 但仍 explicit None check

## 已知 v2 简化（v1 不再支持的能力）

v2 整体公开 API 表面等价 v1（Stage 4-5 完成），剔除如下"刻意去除"的 v1 能力：
- **typed local state（已删）**：v1 的 `ProcessState[T]` + `NewStateFactory` + `WithStateFactory`（`compose_v2/state.go`）整个文件已移除——channel 模型（`Channel[T]` + reducer）是唯一 state 模型
- **TypedGraph[I,O] 泛型包装**：channel 本身类型安全（`Channel[T]`），不需要泛型壳
- **PlanAgent.WithStepResultMapper**：v2 暂未提供（自定义走 `WithExecInputBuilder` 替代）
- **RoutingAgent.DescriptorPrompt**：v2 暂未导出（caller 自己组装）
- **WithRouteMerger**（fan-out 路由路径合并）：v2 RoutingAgent 单目标（fan-out 用 ParallelAgent 替代）

5 graph-level v1 example test（adv_09 GraphAgent / adv_11 TypedGraph / adv_12 Checkpoint
/ adv_13 Superstep / adv_14 Compaction）+ 旧 v1 nested_smoke 在 Stage 7 一并删除——
未来想要 channel-based 等价 example，从头按 v2 风格写新文件，不"迁移"老的。

---

## 验证现状（任意时刻可重跑）

```bash
# 单测 + race
go test -race ./agent_v2/... ./compose_v2/... ./example/...

# 全仓编译
go build ./...

# CI 测试（mock）
go test ./example/...

# 集成测试 compile-only（不需 LLM key）
go test -tags=integration -run "^___never___$" ./example/...

# 3 个 binary smoke
go run ./example/orchestration/channel_smoke           # 5/5 PASS（compose_v2 底层）
go run ./example/orchestration/nested_smoke            # 12/12 PASS（7 编排嵌套 HITL Resume + cleanup + GraphID；Plan/Graph 嵌套走 agent_v2 单测）
go build -o /tmp/web ./example/orchestration/plan_chat_web && /tmp/web -mock -addr :19999 &
# 详见 doc/description/smoke-test-recipes.md
```

---

## 历史 commit 索引（按时序）

```
74bd562 Stage 1 — SequentialAgent 验证模式
a71022d Stage 2-3 — 6 编排批量迁移
150933d 删 6 对老 PoC 低阶 helper
30a358e Stage 4 — 11 个 v1 能力 gap 补齐
c1988ad Stage 5 — 7 项 v1 对齐核对补漏 (C1-C7)
4b9b050 Stage 6.1 — 9 mock tests + 修 streamOrGenerate annotation 丢失 bug
3b6fb60 Stage 6.2 — 9 integration tests + 双版本 helpers
e5d5229 Stage 6.3 — 3 demo programs + InterruptError.Data 字段
462663f docs (Stage 6.3 后) — migration-status.md 等
020dccb plan_chat_web mock smoke fix — interruptHook 补 Query
ad2743e Stage 7 — 重写 nested_smoke v2 + git rm 5 graph-level adv + git rm v1
```
