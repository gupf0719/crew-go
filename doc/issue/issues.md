# crew-go 问题清单（v1 时代历史 ISSUE + v2 ISSUE-73 起）

> ⚠️ **历史 ISSUE 集合**：本目录 ISSUE-40~72 + 本清单本身都是 v1 时代
> （`agent/` + `compose/`，GraphState-based）的问题跟踪。Stage 7 后 v1 已删，
> 文中所有 `compose/*.go:line` / `agent/*.go:line` 引用都是 v1 路径。
> 这些 ISSUE 解决的设计问题（cp ns 物理分离 / fan-out per-branch delta /
> InterruptID 5 维 / cleanupOnSuccess 等）在 v2 中等价继承，但实现路径已变。
>
> v2 演化历史看 [`../v2/migration-history.md`](../v2/migration-history.md)；
> **当初为什么转 channel-based** 看 [`../v2/why-channel-based.md`](../v2/why-channel-based.md)
> （本目录中很多 ISSUE 最终被 channel-based 重设计自然解决，详见该文档"实际收益验证"表）。

## v2 期 ISSUE 索引（73+）

| ISSUE | 状态 | 主题 | 文件 |
|---|---|---|---|
| **ISSUE-73** | 进行中 | v2 文档 audit 跟踪 | [issue-73-v2-doc-audit-tracking.md](issue-73-v2-doc-audit-tracking.md) |
| **ISSUE-74** | ✅ 已修（2026-05-14）| BasicAgent buildInitialMessages preamble 阶段 InterruptError 不存 cp（R25 audit HIGH-2 发现，方案 A preamble 节点化 6 step 落地）| [issue-74-preamble-hitl-cp-gap.md](issue-74-preamble-hitl-cp-gap.md) |
| **ISSUE-75** | ✅ 已修（2026-05-15）| BasicAgent commitFinal postamble 阶段 InterruptError 后 cp 已 cleanup（R27a audit HIGH-1 发现）—— 方案 A postamble 节点化落地（与 ISSUE-74 完全对称）：commitFinal 迁到 graph 末段独立 postamble 节点，抛 Interrupt 时 cleanupOnSuccess 不触发 cp 保留供 Resume | [issue-75-postamble-hitl-cp-gap.md](issue-75-postamble-hitl-cp-gap.md) |
| **ISSUE-76** | ✅ 已修（2026-05-15）| AutoCompact 在 Memory-enabled + 单 user input + 多 ReAct 轮场景下静默失效（R29 audit 视角 2 MED-1 发现）—— 方案 A 增强版: AutoCompactRun 检测 [0..tailStartIdx) 全 protected (mem/skill marker) 时 reset tailStartIdx 让真实 ReAct 轮组进 early summarize | [issue-76-autocompact-memory-fallback-gap.md](issue-76-autocompact-memory-fallback-gap.md) |
| **ISSUE-77** | ✅ 已修（2026-05-15）| LoopAgent ExitCondition 抛 InterruptError 时 body sub-agent 重复跑（R29 audit 视角 5 MED-1 发现）—— 方案 A exitCheck 节点化：拓扑改 `body → exitCheck → ConditionalEdge`，exitCondition 抛 Interrupt 时 cp 含 exitCheck frontier，Resume 只重跑 exitCheck，body 已 commit 的 lastOut channel 在 cm.Restore 恢复 — body 不重跑 | [issue-77-loop-exitcond-body-double-run.md](issue-77-loop-exitcond-body-double-run.md) |
| **ISSUE-78** | ✅ 已修（2026-05-15）| AgentAsTool childNS 缺 tc.ID 维度（R31 audit 视角 B HIGH-1 发现）—— 预防性修法: compose_v2 加 WithCurrentToolCallID/GetCurrentToolCallID ctx 协议，BasicAgent toolsFn 调 dispatchToolkit.Call 前 inject tc.ID，AgentAsTool.Call 用 hash(tc.ID) → JoinFanoutNS 加 childNS。修后未来 fan-out 化直接 race-free 无需架构改动 | [issue-78-agent-as-tool-tc-id-ns.md](issue-78-agent-as-tool-tc-id-ns.md) |
| **ISSUE-79** | ✅ 已修（2026-05-15）| AgentAsTool 子 sub-agent sess 不可续传——R31 audit 视角 B HIGH-2 发现，方案 3 直接默认化（参考 eino adk withSharedParentSession + 保留 crew-go NS 物理分离优势）：默认半共享 child sess（values 指针共享 + events 独立 + parentSess 反向指针 + RootSession 链 + RunID 沿用父）。破坏性改动按 CLAUDE.md 约定不留 opt-in 兼容路径 | [issue-79-agent-as-tool-child-sess.md](issue-79-agent-as-tool-child-sess.md) |
| **ISSUE-80** | ✅ 已修（2026-05-15）| ParallelAgent fan-out 多 branch 共享 parentSession.events 让 sub-agent.Run() 返回 events 含 sibling 串台——R31 视角 D HIGH-1 临时用 FinalMessageByAgent name filter 缓解（限制业务用 distinct sub-agent 名）。R32 audit 视角 B HIGH-1 + LangGraph/eino 调研后 port eino lane events 模型作根因修复：RunSession 加 laneEvents 字段 + ForkLane / CommitLaneEvents 协议 + ParallelAgent.branchFn 改 lane 隔离。删 R31 distinct name 限制 | [issue-80-parallel-lane-events.md](issue-80-parallel-lane-events.md) |
| **ISSUE-81** | ✅ 已修（2026-05-15）| BasicAgent toolsFn 多 tool_call 串行执行——graph 已 channel-based 但 tools 单节点内部仍 for 循环（用户挑战"BasicAgent 改 graph 了为什么还串行"触发）。三方对照（LangGraph asyncio.gather / eino goroutine+WaitGroup 都并发）+ 三前置 ISSUE-78/79/80 修完 → 实施方案 A 节点内 goroutine + WaitGroup + 加 InterruptError.Queries 多 query 容器 + MergeInterrupts helper + WithCurrentToolCallContext 一站式 ctx 协议 + tool.Tool godoc 加 reentrant 协议。N=1 inline 零开销 / N>=2 并发 | [issue-81-basicagent-toolsfn-fanout.md](issue-81-basicagent-toolsfn-fanout.md) |
| **ISSUE-82** | ✅ 已修（2026-05-15）| OnAgentEnd hook 双触发——`compose_v2/basic_agent.go:478` ConditionalEdge model→terminal 段 + `agent_v2/basic_agent.go:139` Run defer 兜底两处都触发。R34 audit ISSUE-75 commit `9886a8a` 视角 D 发现，单独跟踪。3 方对照（LangGraph try/except 互斥 + eino compose graph callback (Component=Graph) 与 flowAgent agent-level callback (Component=Agent) **不同 RunInfo 类型解耦** + ChatModelAgent 完全不 fire 自己 callback）后实施方案 A：删 ConditionalEdge cond fn 内 fire OnAgentEnd（cond fn 是路由判断不是终止信号 + InterruptError 时 cond fn 不跑漏 fire），agent_v2.BasicAgent.Run defer 唯一源兜底所有退出路径（success/Interrupt/panic/ctx.Err）| [issue-82-onagentend-double-trigger.md](issue-82-onagentend-double-trigger.md) |
| **ISSUE-83** | ✅ 已修（2026-05-15）| PlanAgent 不接业务级 hook.Hooks——9 编排里**唯一**不接业务级 hook 的 agent（ISSUE-82 audit LOW-1 发现）。修法：PlanAgent.Config 加 `hooks hook.Hooks` 字段 + `WithPlanHooks(h)` option + Run/Resume defer 直接调 `p.hooks.OnAgentEnd` 兜底（学 ISSUE-82 BasicAgent 单一所有权 + 与其他 8 编排架构对称; audit² 改: `SafeOnAgentEnd` helper 已删, framework Run 入口 outer defer recover 兜底跟 eino 对齐）。默认 NoOpHooks 让业务不传 option 完全等价 pre-fix 行为；HITL Resume 闭环算 2 次 OnAgentEnd（Run 抛 Interrupt + Resume 完成）| [issue-83-planagent-no-agent-hooks.md](issue-83-planagent-no-agent-hooks.md) |
| **ISSUE-84** | ✅ 已修（2026-05-16）| Memory 协议跨 ns 边界传播改进（PR 3.3 commit `e6505f9` 后 Memory 已替代为 store，下文 API 同步改名为 `WithSubAgentStoreScope` / `IsSubAgentStoreScope` / `store.Put`）（R32+ 视角 5 audit 2 HIGH + 3 MED + 2 LOW + audit follow-up 2 HIGH）—— HIGH-1 加 `agent_v2.WithSubAgentStoreScope(ctx, true)` ctx 协议 + AgentAsTool / SkillTool fork 自动 push, 业务 `store.Put` 查 `IsSubAgentStoreScope(ctx)` 跳过避免双重持久化 + HIGH-2 usage-guide 加 store 嵌套章节 + RootSession 范式端到端测试 + MED-3 加 `HistoryForRaw(owner)` 保原 role + from_agent annotation (适合 LLM 对话历史) + MED-1/2 godoc 决策矩阵 + LOW-1 InMemoryStore godoc 加醒目警示 + LOW-2 hitl-resume 加 store + HITL 嵌套范式; **audit follow-up²**: WithSubAgentStoreScope 协议覆盖 2/8 → 9/9 调用入口 (7 编排 + 2 tool fork) 共 15 push 点全 push + AddSubAgent / WithSubAgents typed nil interface 陷阱修 (reflect.IsNil) + WithSubAgents 合并语义正向测试 + 4 godoc 警示; **audit follow-up³**: isAgentNil 抽包级 agent_v2/agent_helpers.go + 7 编排 + 4 With* options 全协议对称 + AllOrchs typed nil 11 子测试; **audit follow-up⁴**: SetExecutor + WithLLMPlannerSubAgents 漏修补齐 (协议四复证) + 13 子测试; **audit follow-up⁵**: AgentAsTool + RunnableAsTool + SkillSubAgentResolver `subAgent == nil` 漏 typed nil 修 (协议五复证) + 15 子测试 + sanity-revert panic msg 断言拆 WithLLMPlannerSubAgents 假绿 (旧 ag.Description() 远 stack nil deref 让 recover 不区分 fix vs revert) | [issue-84-memory-cross-ns.md](issue-84-memory-cross-ns.md) |
| **ISSUE-85** | ✅ 已修（2026-05-16）| PlanAgent 4 MED + 2 LOW（R32+ 视角 1+2 audit + 2 audit follow-up 4 HIGH 累计）—— HIGH-1/2/3 commit 23d75cb 修; MED-1 测试假绿改真断言 + MED-2 PlanBuilder 加 WithName + MED-3 ExecInputBuilder.executed 改全历史 (扩 PlanWorkerFn 签名加 allExecuted) + MED-4 AddSubAgent NS-safe 校验 + 重复注册 panic + LOW-1 UTF-8 按 rune 切 + LOW-2 resolveRefs 失败 slog.Warn; **audit follow-up**: TmpSubAgent dispatcher allExecuted 漏修 (commit 0aa15bf 谎报修补) + (Step,Agent) 反向映射改倒序; **audit follow-up²** (commit 49d3065 audit): IndexRef regression 自我修补 (worker 删倒序 lookup 直接复用 cur.InputRefs) + WithSubAgents 改合并 + dup check (与 AddSubAgent 协议对称) + AddSubAgent / WithSubAgents typed nil interface 陷阱修 (reflect.IsNil 检 underlying value) + 5 sub-test (3 worker IndexRef 覆盖 + typed nil + merge-preserve 正向) | [issue-85-planagent-improvements.md](issue-85-planagent-improvements.md) |
| **ISSUE-86** | ✅ 已修（2026-05-16）| cond 路径相对 NodeFunc 路径不对称（R32+ 视角 3 audit + audit follow-up 2 HIGH）—— HIGH-1/3 commit 23d75cb 修；MED-1/3 ctx.Err / 业务错也保 frontier=[from] 让 Resume 重跑 + MED-2 hook 注入缺口 by design 文档化 + LOW-1 branchIdx 写死 0 by design + LOW-2 加 R25 fix 回归测试；HIGH-2 visit 漂移**已彻底修**（方案 D2 commit `4282548`→`da90ffc`：cond fn 抛 Interrupt 强制所有 query InterruptID 非空 fail-fast，绕开 visit-1 补 ID）; **audit follow-up**: visit-1 跨 Resume 仍漂局限 godoc 化 (单 Run 内稳定 vs 跨 Resume 节点 IncrVisit 累加) + 测试假绿拆除 (5 类炸弹 #2 单节点拓扑 frontier 一直 [a] → 改多节点 fan-out 拓扑触发真 frontier 收缩)。**2026-07-16 根治 (commit `70e0fcb`)**：D2 的"cond 可 interrupt（须自填 ID）"被**彻底禁止 cond interrupt** 取代——引擎深审 F-A 揭露根因不是 ID 漂移而是「cond 能否 interrupt」（任何 cond interrupt 都让成功成员被拉回重跑、违反 PA-H1，二者不兼容故保 PA-H1）；RoutingAgent 的 Router/classifier 移到 classify node 后原生支持 HITL 且不必自填 ID | [issue-86-graphloop-cond-improvements.md](issue-86-graphloop-cond-improvements.md) |
| **ISSUE-87** | ✅ 已修（2026-05-16）| type registry / channel reducer / cp marshal 改进（R32+ 视角 6 audit 3 MED + 3 LOW + audit follow-up 1 MED）—— HIGH-1/2 commit 23d75cb 修;  MED-1 RegisterValueType LoadOrStore 原子 + MED-2 Restore 跳 untyped-nil + MED-3 ParallelBranchResult.OrigErr `json:"-"` + LOW-1 wire name 跨包统一 schema.Message + LOW-2 typeRep 用 reflect.TypeOf((*T)(nil)).Elem() + LOW-3 reducer godoc 加闭包不持久化警示; **audit follow-up**: LoadOrStore 单 op 原子但跨两 sync.Map 仍 race window → 加 typeRegistryMu 跨两 map mutex (写路径罕见不影响热路径性能) + ParallelBranchResult.OrigErr godoc 加跨 Resume 失败信号丢失警示 + issue87 测试 t.Errorf → t.Fatalf 防 SIGSEGV | [issue-87-type-registry-improvements.md](issue-87-type-registry-improvements.md) |
| **ISSUE-88** | ⏸ 保留观察（2026-05-17）| skill / schema 三处 spec 占位 dead code（audit²⁸ global review 发现）—— `skill.SkillBoxes/SkillBoxBuilder` + `skill.SkillBox.RunSkillAction/Script + ErrActionNotImplemented` + `schema.ToolResultMessage` 第二参数 name. 三方对照（LangGraph / eino / Claude Code）均未采纳同款抽象，但 spec 层（agentdock-spec）仍保留作未来扩展槽位. 业务 0 调用 + 不影响 skill 真业务路径（ListSkills/MatchSkills/ActivateSkill）. 保留原因: spec 同步成本 vs 删 dead 收益不对称. 重审时机: API freeze / spec v2 升级 / 业务诉求触发 RunSkillAction 真实现。<br>**2026-09-18 核验**：描述部分过时——`RunSkillAction` 已有真实现（go-func 进程内 action，`skill/skillbox.go`），`ErrActionNotImplemented` 转成了「自定义 SkillBox 不支持 action」的正当哨兵，`RunSkillScript` 已随脚本型 action 删除；**仍是 dead code 的只剩** `skill.SkillBoxes` / `SkillBoxBuilder`（生产零调用，而 `agent_v2.WithSkills` 的 godoc 示例还在教用它，且那段写法编译不过）与 `schema.ToolResultMessage` 的第二参数 `_` | [issue-88-skill-schema-dead-code.md](issue-88-skill-schema-dead-code.md) |
| **ISSUE-89** | ✅ 已修·长期方案（2026-07-03）| thinking model `reasoning_content` 字段多轮回传缺失—— 切 DeepSeek 测试模型（deepseek-v4-flash thinking 模式）第 2 轮 LLM 调用 400 报"reasoning_content must be passed back to the API". 三方对照 eino 是 first-class `Message.ReasoningContent` / LangGraph 用 `additional_kwargs`. **临时方案**: model/openai 加 WithExtraBody 通用机制 + model/deepseek 默认 `thinking: {type: "disabled"}` 关 thinking 绕过, TestAdv_01 多轮场景 5.77s PASS. **长期方案已落地**（2026-07-03）: `modules/common/Message.ReasoningContent` first-class 字段 + openai client 解析/多轮回传/流式聚合 + token 估算计入, 业务无需自处理, deepseek+qwen 真机验证 (`6e4bd09`/`2869754`/`9f7e6fc`) | [issue-89-reasoning-content-thinking-model.md](issue-89-reasoning-content-thinking-model.md) |
| **ISSUE-90** | 📋 跟踪中（2026-05-17）| plan_chat_web 生产化 lifecycle (历史查询 API + Redis backend + ephemeral 选项)—— 本 session 已对齐 LangGraph 6 状态 (running/interrupted/success/error/cancelled) + TTL sweeper (`-session-ttl-minutes` 60min + `-sweep-interval-minutes` 5min), 删 runs.Delete 让 slot 留 history. 待业务诉求驱动加: 1) `GET /api/sessions?status=&limit=` 历史查询; 2) Redis/Postgres backend 多副本; 3) per-session `ephemeral=true` 选项. YAGNI 不预实施 | [issue-90-plan-chat-web-lifecycle-prod.md](issue-90-plan-chat-web-lifecycle-prod.md) |
| **ISSUE-91** | 📋 待实施（2026-06-01）| 外部/异构 agent 接入编排 —— 已有 agent（别的框架 / 远程 HTTP·gRPC 服务 / 黑盒）接入 Sequential/Parallel/Plan 当前要手写 struct 实现 `Agent` 4 方法 + 事件转换 + Resume 逻辑。提议加 `agent_v2.AgentFunc(name,desc,fn)` 轻量 adapter（类比 LangGraph `RunnableLambda` / eino `compose.Lambda`）。难点：Resume（黑盒 agent 无中断概念，crew-go cp graph 级 → 成功不重跑 / 失败重跑幂等业务负责 / 不能主动 HITL）。YAGNI，业务真要集成外部 agent 时实施 | [issue-91-external-agent-adapter.md](issue-91-external-agent-adapter.md) |
| **ISSUE-92** | 📋 设计完成待实施（2026-06）| skill 源头分层（L0 progressive disclosure）+ 触发缓解 + over-read 预算闸门 —— toolCall 模式 skill body 跨 compaction 累积（#10 压缩保护是治标）→ 业内调研（deep-research 多源 3-0）证实源头分层**大幅缓解**（body 层累积迁到 reference 层，reference 仍需 per-Run 去重）：三级分层（metadata always / core body 触发 / reference 按需 `read_skill_resource`）+ 框架 deterministic 路由（新增 export `MatchReferences` 复用 `keywordScore`，解 Vercel evals 实测 56% 触发漏读）+ over-read 预算闸门 + 移除 toolCall `SkillBodyAnnotation` 保护（inline 保留）。crew-go `SkillResource` 数据模型雏形好（路由/解析/Description 需新建）。详见 skill-system.md §skill 源头分层方案设计 | [issue-92-skill-source-layering.md](issue-92-skill-source-layering.md) |
| **ISSUE-93** | 📋 记录跟踪（2026-06-05）| HandoffAgent 模型 B 阶段 2 audit 遗留 3 LOW（同批已修 MED pending key 碰撞 `34a0c87` + active 跨实例污染 `92f9078` + doc/godoc stale `6bc28bf`）——①historyMode ctx 泄漏非 handoff 编排内层 BasicAgent（未触发，修需 7 编排 churn 大，godoc 已注明）②SelfHistoryCompacted cutoff dead（既有非 handoff 引入，AutoCompact summary 不 AddEvent → 退化等同 SelfHistory，对 handoff 无影响）③~~consumePendingTransfer >64 tool_call 静默丢~~（已随转交信号改走 ctx 收件槽消失）；+ by-design Raw orphan tool_call（opt-in 已知风险文档已警示非 bug）。<br>**2026-09-18 核验**：①② 仍在。① 当初「修需 7 编排 churn 大」的理由已被编排派发收敛（`f9626d1a`）削弱，但仍不是单点——`runMember` 与 `runSubAgent` 都要改，`runDecisionAgent` 与 skill fork 也会把 mode 透传下去；ISSUE-98 之后 historyMode 还决定输入要不要 AddEvent、历史怎么去重，泄漏的影响已不止可见性（未实测）| [issue-93-handoff-modelb-audit-low.md](issue-93-handoff-modelb-audit-low.md) |
| **ISSUE-94** | ⚪ **已解决**（2026-07-27，`2b5c9bc`）—— 加 `CPStatusFailed` | **非-interrupt 致命错留下的存档被当成「有人在等答案」自动捡起**。根因不是「留了 cp」而是**存档在说谎**：`SnapshotForGraph` 无条件标 Pending，破坏了 auto-load 赖以成立的不变量（cp 存在 ⇒ 真有人等答案）。解法：出口按 `IsControlFlowSignal` 判——中断 / 软取消 / 陈旧写回是挂起（照常 Pending），其余标 `CPStatusFailed`。**完全不看错误是瞬时还是确定性**（框架判不了也不必判），绕开了原 backlog 卡住的那个难点。标了之后 `ListPending` 不返它（不再静默重撞），但**存档保留** + 新增 `Status.Resumable()`，业务显式 `WithResumeFrom` 仍能从失败点续跑——crash-recovery 能力不丢。durability 中途写的不受影响（恒 Pending，那是崩溃恢复点）| [issue-94-dangling-cp-nonterminal-error.md](issue-94-dangling-cp-nonterminal-error.md) |
| **ISSUE-98** | 🟢 **已实施**（2026-07-23）—— 历史层落地（`agent_v2.WithHistory` + `HistoryStore` + conformance）、三写入点接上、`prior` 换源、伪历史 cp API（`cp.ThreadID`/`ListByThread`/`LatestByThread`/`ErrThreadNotFound`）连 redis/sql backend 一并删除 | **opt-in 会话历史层**——业务要「列会话 / 接着上次聊 / 回看某轮」，现在做不到。缺两样：① **跨 Run 会话身份**（cp 主键、store key 前缀、`cp.ThreadID` 三处都 per-Run，而业务经 `WithThreadIDCtx` 声明的值只喂了 observability 和 handoff）② **完整对话流不跨 Run**（`basicMessagesCh` 是 append-only 完整流、压缩从不写回、postamble 拿得到全量——但随 cp「成功即清」蒸发，`WithStoreMemory` 就在那个位置却只舀走最后一条 assistant）。**外溢出两个已存在缺陷**：多轮对话喂给模型的 `prior` 残缺（读 events，实测第二轮只剩「问题 + 结论」，工具调用和结果全丢，模型无法判断结论来源）——**三方零例外都没这个问题**，根因是「事件流当审计摘要写、当完整历史读」的契约不一致；`ListByThread`/`LatestByThread` 名不副实。**形态**：真相源位置学 LangGraph（channel）、持久化分层学 OpenAI/MAF（历史独立层、跟 cp 零耦合）。**不走 LangGraph 一套**（cp 无消息级归属 + O(N²) + 要放弃「成功即清」，且它自己的 ShallowPostgresSaver 试过这形状已弃用）；**不走 eino 形态**（它 runSession 框架管、我们 RunSession 业务管，实测不传 sess 的 Resume 会出错）。**决策**：增量写 / 每超步 / 异步不阻塞 / append-only 免游标 / 写失败吞+warn / 内部决策 LLM 不进历史 / 不提供 ListConversations——每条都有业内依据 | [issue-98-conversation-history.md](issue-98-conversation-history.md) |
| **ISSUE-99** | ⚪ **CLOSED**（2026-07-24）—— 随 `WithIncrementalSkillListing` 一并删除，触发路径不再存在 | **增量 skill listing 的静默失效**：「已宣布过哪些 skill」有两个真相源（session KV / prior 消息上的名单）取并集 ⟹ 不变量「回收到的名字其宣布消息这轮真在 prompt 里」可以不成立而框架不知道。业务 `WithHistoryCompactor` 砍掉 listing 后 KV 仍说"发过"→ 不补发，而增量模式下工具描述里本就没有 skill 名 ⟹ 模型失去所有获知渠道且零报错。**没有单独修，而是删掉了整个功能**——这是同一根因的第 6 次表现（前 5 次：deac747 / c26b7d2 / R14 HIGH-1 / R17 MED-2 / R19 HIGH-2），根因是「一份必须一直在模型眼前的状态被放进了会被压缩的对话流」，删除前需 5 文件 14 处单独保护、漏一处即静默失效。实测框架层同侪零家这么放。**留作决策实证 + 通用教训**（状态存放位置决定防护代码量 / 两个真相源取并集 = 不变量可失效而无人知 / 静默失效比报错严重） | [issue-99-listing-invariant-broken-by-kv.md](issue-99-listing-invariant-broken-by-kv.md) |
| **ISSUE-97** | 🟡 **部分缓解**（2026-07-21）—— opt-in 守卫已上线，**核心危害防不住**（见右） | **同一 sessionID 被并发 Run/Resume**（两个进程副本同时处理同一会话）：双方各自载入同一份 cp、各自执行、各自写回 → 后写者静默覆盖先写者，**两边都返回 err=nil**，事后无从察觉。已实证复现（`-race` 干净——是逻辑双重执行不是数据竞争，race detector 永远抓不到）。**五方源码调研结论**：没有任何一家在库/框架的存储层做乐观并发防它，责任统一上移（服务端 / durable runtime / 进程架构 / 应用层）——**副作用双跑物理上防不住**，它发生在 Save 之前，任何存储机制只能在写回时拒绝，那时活已经干了两遍。**我们做的**：`WithStaleWriteGuard()` / `WithStaleWriteGuardCtx(ctx)`（后者是 `agent_v2` 9 编排下唯一开关，也让嵌套子图继承）+ `Checkpoint.Epoch` 世代标记 + `ErrCheckpointStale`，**默认关闭、默认行为一字不变**。它只把最坏的「静默」变成「可见」，让业务能补偿/告警。**已知边界**：① 不阻止副作用双跑 ② 检测非原子（read-then-write 有窗口，两副本可能都通过）③ 真原子需 backend 支持条件写（`UPDATE ... WHERE epoch = ?`）④ **看不见跨层冲突**——多层编排每层各自载入各自比对，子层基线就是它刚读到那代，检测窗口只覆盖本层「读到写」这一段；父层被拒时子层已落盘的也不回滚 ⑤ **只管 cp**——Handoff 控制权转交、StoreMemory 记忆都经 `store.Store` 在图内落盘，早于出口判定，Run 返 stale 时它们可能已生效。真解在调用方：会话亲和路由 / 分布式锁 / 队列；上层 `agui`、`a2a/server` 已各自拒同 threadID/taskID 并发。<br>**2026-09-18 核验**补一条边界 ⑥：**带「新开」信号的 Run 不设世代基线**（`compose_v2/graph_loop.go` 约 604 行），守卫要等本次首写之后才生效，而首写本身不校验——agent_v2 顶层 Run 恒新开，于是默认 exit 档下，新开的 Run 全程不受保护——成功清理时还会把槽里别人的挂起档一并删掉；sync / async 档从本次第二次写入起受保护（每次写成功都会把基线推到自己写的那一代）（探针：Run 执行期间往同一槽写入别人的挂起档，sync 档报 stale 并保住那份档，exit 档不报、成功清理时把它删掉）。「两个任务同 sid」本身违反 RunID 一任务一枚的契约，但 `WithStaleWriteGuard` godoc 没写这条（见 `doc/code/todo.md` 同名条目）| [checkpoint-storage.md §同一会话并发执行](../description/three-way-comparison/checkpoint-storage.md) |
| **ISSUE-96** | 🟢 **已闭合**（2026-07-20）—— 6 侧面全修 + 4 路并发审计再抓 5 个（`SendDirective.instanceID` 漏 wire / 发号器跨重启重号 / 宽度失去兜底 / cond 未注册 target 同族分支 / 越界下标静默退化）也已修完，各配哨兵 + sanity-revert；**收口后又抓到第 7 个同根 bug**：混合 frontier（同 NodeID 既是静态边目标又是 `SendFrom` 目标）下 fan-out 兄弟推高 per-NodeID 共享的 `visitCount`，空身份成员再读就变了值 → InterruptID 跨 Resume 漂移 → **HITL 永不收敛**（每次 Resume 重新提问）。修法：只有空身份成员推进该计数器（`96d026d`）。**它是 flaky 不是稳定坏**——静态实例偶尔恰好读到已推高的值就碰巧对上，回归测试用 channel 屏障强制时序。同批删掉 `branchIdx` 死链（生产/测试零调用，godoc 却在教业务编入它算 InterruptID = 教人踩侧面 1）；cp schema v9 | **记录/身份用 NodeID 粒度，执行单位却是 branch 实例**——同 NodeID 多实例唯一来源是 `SendFrom` 同 Target 投递（静态/cond 边走 `nextSet` map 恒去重）。两个身份维度都不稳：`branchIdx` 是**位置**（部分失败时 frontier 缩成失败子集 → 重排）、`visit` 是 **per-NodeID 共享计数器**（成功兄弟推进它 + 超步内并发竞态）。**3 个已实证静默丢数据**（全同根，全有探针）：① canonical InterruptID 跨 Resume 漂移+不收敛（= ISSUE-95 finding 2）；② `SendFrom`+`RunSubAgent` **子 cp 互覆**（2 实例只活 1 个 cp，另一个 HITL 问题蒸发）——**踩的是 `run_subagent.go:45-47` 文档推荐的 fanout 路径**；③ 多实例+cond `CondFailRerunFrom` → 兄弟实例产出丢 2/3、Run 返 nil。同根历史：`90a94d2` 自引回归（按 NodeID 清 purge 误删兄弟实例 send）+ MED-3 fail-fast（`subgraph.go:114` 注释早已诊断出同一根因，当时只挡 subgraph 那半、明写裸节点那半是「独立议题」）。④ fan-out **宽度被当迭代深度**（`SendFrom` 同 Target > maxVisit=100 → 假 MaxIterationsError，且 worker 一次没跑就存 Pending cp → 每次 Resume 都触顶 → **永久卡死**；对照组 101 distinct NodeID 正常）；⑤ **MED-3 guard 根本没在拦**（只拦 branchIdx>0 + 错被兄弟 Interrupt 顶掉 + Resume 走 fast path 恒 0 → 实测 Run1 报错但子图已跑、Resume 就 err=nil）；⑥ **observability 两个 session 级 map 无实例维度**（3 实例只导出 1 个 span 且 duration 张冠李戴 / chunk 逐 token 编织；-race 抓不到——逻辑碰撞非 data race）。**方案**：身份是**三个正交维度**（图级 token 新增 / **visit 保留**——静态边单实例本就稳且守跨访问代际 / **tc 下标拆独立 getter**——4 个消费点想要的是它、只是借用了同一个 ctx key）。fresh-mint 不透明 token 随 frontier 持久化（碰撞/代际/竞态/漂移四问题一起消失；**不需要确定性**——只有重算才要，我们持久化）。净简化：可删 `purgePrior` 启发式 + 拆 MED-3 guard | [issue-96-branch-instance-identity.md](issue-96-branch-instance-identity.md) |
| **ISSUE-95** | 🟢 已收口（finding 1 ✅ `90a94d2`+`7d6b913` / finding 3 ✅ `bca57e8`；**finding 2 ✅ 已随 [ISSUE-96](issue-96-branch-instance-identity.md) 侧面 1 闭合** —— 它是那个根因的一个侧面，单独修不掉）| Tier 1/2 扩审 3 finding（均 pre-existing，非 commit `9389cbf`）：**MED ✅ 已修** graph_loop 部分失败超步"完成兄弟自身带 SendFrom 且自身 cond 出边又报错成重跑目标"→ 旧 deferred send 没清 + 重跑重产 → 目标节点**双发**（实测 Z==2 应为 1）。修法：`SendDirective.owner`（不导出，框架唯一点 stamp）+ `SerializedSend.Owner` 进 wire + edge 失败块按 owner purge。⚠️ **purge 集合是 `rerun \ frontier` 不是 `rerun`** —— 记录按 NodeID、执行单位却是 branch 实例，按 NodeID 清会误删已成功兄弟实例的 send → 静默丢（`90a94d2` 照原方案实现即翻车，`7d6b913` 修回）；**原记的 `purgeCompletedRerun(rerun)` 方案与「5 PA-H1 回归绿」验收均已证伪**（那套回归对多实例零覆盖，哨兵补在 `compose_v2/multi_instance_test.go`）；**MED 🔵 已转 ISSUE-96 侧面 1** 多实例部分成功后 canonical InterruptID 跨 Resume 漂移（branchIdx 位置重排 + **visit 被成功兄弟污染**双维、Resume 不收敛每轮重问用户，打破 MakeInterruptID godoc 承诺）；**MED ✅ 已修**（`bca57e8`）OnToolEnd 硬编码 Success:true——**非 by-design**：observability 按 !Success 记工具失败（当时的 `gen_ai_tool_errors` 指标，OTel 重写后是工具 span 与工具时长的 `error.type`），生产方恒 true → 该 auto-fire metric 对业务级工具失败**恒 0**（tool.Failure 的结构化声明在 hook 边界被抹平）；顺带 hook 对失败 tool_result 的改写此前被静默丢弃（回写分支进不去，违反 OnToolEnd [消费] 档语义）。修法：makeToolsFn 经不导出 annotation 把 Error.Code/Message 跨层带给 hook fire 层。**既有 metric 测试是假绿**（手工构造 Success:false 直调 hook，从没经过 BasicAgent）→ 补真 E2E| [issue-95-graphloop-sendfrom-double-send.md](issue-95-graphloop-sendfrom-double-send.md) |

---

## v1 历史 ISSUE 列表（40~72）

来源：与 agentdock-spec 对比分析（2026-04-10）
原则：spec 更优的必须对齐，我们更优的保持现状。

---

## ✅ ISSUE-72 · ISSUE-70 后跨进程 / 跨 session / 嵌套 fan-out 9 bug 修复

**优先级**：高
**状态**：已实现（2026-05-05）

5 路并行独立 agent 交叉审计后逐项修复 + 跨 commit 二次 review 1 HIGH + 1 MED
新引入 bug。9 个原始 bug 按"严格只在确认优于现状时才借鉴 LangGraph/eino，不
教条抄袭"的判断原则修复，含 6 个基础修 + 3 个深嵌套 fan-out 修。每个 fix
配回归测试 + sanity revert 设计。

修复列表（commit 链）：
- 2a106c1: 文档 stale 修复 12 处（HITL 矩阵 / 4 层压缩 API / 嵌套 cp 协议 / 包名）
- 113d281: 基础修 6 bug（CircuitBreaker per-session 数据隔离 + IsolatedSession
  fail-fast + summary 去重 + reinject 源端 + IsolatedSession 注释明示 +
  DeleteSubtree 已正确）
- 5859b59: Audit 4 MED-3 sibling 拆 PendingExact（深嵌套误命中）
- 3b63311: Audit 4 MED-4 Directed rerun 精确清子树
- ea097bf: Audit 4 MED-5 RunStep 加 BranchIdx（嵌套同名 fan-out InterruptID 撞车）
- 4d93e49: cross-audit 修 1 HIGH（crossNSPendingInterrupts nil map panic）+
  1 MED（SupersedeEventsByMarker race）+ 文档收尾

剩余跟踪：[issue-71-pa-ra-plan-nestable-agent.md](issue-71-pa-ra-plan-nestable-agent.md)
（PA/RA/Plan 实现 NestableAgent，独立架构改造）

---

## ✅ ISSUE-71 · 接口合并：删 NestableAgent，所有 9 编排都通过 Agent.Run 自动嵌入

**优先级**：高（用户痛点：plan_chat_web 前端测试时发现 PlanAgent 嵌入 HITL 不闭环）
**状态**：✅ 已完成

详见：[issue-71-pa-ra-plan-nestable-agent.md](issue-71-pa-ra-plan-nestable-agent.md)

**最终方案**：原计划仅给 PA/RA/Plan 加 RunNested，实施时改为根本解 —— **删除
NestableAgent 双接口设计，对齐 LangGraph (单 Pregel) + eino (单 Agent) 单接口**。

实施 5 个核心 commit + 1 个 follow-up：
- `d2059b2` ctx helpers（WithEnclosingParent / GetEnclosingParent / ClearEnclosingParent）
  + RunGraph 顶层 strip 防御
- `5cbb159` 9 编排 Run 入口加 GetEnclosingParent 分叉（含 PA/RA/Plan/GraphAgent
  首次支持嵌入）
- `4bb5482` 3 个 sub-agent 调用入口注入 EnclosingParent
  （runStepAgentEvents / dispatchBranchAgent / toolsNodeFunc）
- `45f5e04` 删 NestableAgent 接口 + 5 个 RunNested 方法
- `b5a129b` 补漏 GraphAgent.Run runMaybeNested 分叉（第 1 轮 audit 发现）
- `46d8e80` follow-up：加 `BasicAgent.Resume(ctx, sessionID, opts...)` API
  （`agent/agent.go:106`）—— 之前 BasicAgent 嵌入运行靠父 agent 续跑，
  顶层 chat_entry 模式（plan_chat_web）直接 `BasicAgent.Run + Resume` 需要
  这个 API。签名对齐其他编排（Continue/Abort/Replan reject）

效果：
- ✅ 任意 Agent 嵌入任意 graph，HITL 全闭环
- ✅ chat_entry → AgentAsTool(PlanAgent) → BasicAgent.ask_user → Resume 闭环
- ✅ 嵌套 cp 路径自动派生 `(parentSID, JoinNS(parentNS, parentNodeID))`
- ✅ lifecycle 双重防护（顶层 strip + 用完即删）
- ✅ 接口对齐 LangGraph/eino，未来新加编排无需双接口决策
- ✅ 所有 9 个 Agent 实现（BasicAgent + 8 编排）都有同模式 `Resume(ctx, sessionID, opts...)` API

verify：nested_smoke 10/10 + `go test ./...` 全过 + 2 轮交叉 audit 取并集修 1 个 HIGH（GraphAgent 漏分叉）+ BasicAgent.Resume 4 个黑盒单测（Continue / Abort / Replan reject / NoCheckpointer）。

---

## ✅ ISSUE-70 · Checkpoint 系统演进（namespace + 成功即清 + 三方精华融合）

**优先级**：高
**状态**：Phase 1-3 已完成（2026-05-05）；Phase 4 推迟到业务踩到 / PG 接入

详见：[issue-70-checkpoint-evolution.md](issue-70-checkpoint-evolution.md)

解决当前 cp 系统 14 项不足。经 5 轮方案迭代 + 6 轮交叉审核（14 个独立 agent
+ 2 轮 binary 冒烟），落地 (sessionID, ns) 物理分离 + 嵌套 HITL 闭环 + 三方
精华融合（不照抄）。共借鉴 10 真精华 / 不抄 10 糟粕 / 保留 7 已偏离但更优。

实际工作量 ~10 工作日（vs 原计划 13-16，节流来自决策不抄 typed Address +
InterruptCtx 三态 + Phase 4 推迟）。完整 18 commit + retrospective 见 issue-70
文档。

Phase 4 推迟项触发条件：
* StateFactory migrate：业务 typed state schema 演进
* SelfHistoryByNS：同名 agent 跨嵌套层 OR 按 ns 子树审计
* 性能优化 / Step Ⅲ SaveBatch：PG/Redis 后端接入（2026-09-18 核验：触发条件已满足——`redis/` `sql/` 已内置；但生产代码零调用 `SaveBatch`，接口 godoc 说「原子」而实现是逐条 best-effort，去留见 `doc/code/todo.md` 同名条目）

---

## ✅ ISSUE-69 · 上下文压缩落地后 9 轮交叉审计 retrospective（30+ bug 全修）

**优先级**：高
**状态**：已实现（2026-05-04）

详见：[issue-69-compaction-audit-retrospective.md](issue-69-compaction-audit-retrospective.md)

按 CLAUDE.md 多层系统 bug 审计方法论做 9 轮交叉审计 + 1 轮 panic recovery，
共找到 ~30 个真 bug 全部修复 + 配套回归测试。bug 数量按几何递减但不为 0
（Round 1: 0 → Round 7: 6 → Round 9: 1）。索引所有发现的 bug 类别 + 7 个
设计权衡 + 留给后续的长期跟进项（HITL 矩阵 gap）。

---

## ✅ ISSUE-65~68 · 4 层上下文压缩系统（Micro/Auto/Reactive/SessionMemory）

**优先级**：高  
**状态**：已实现（2026-05-03）

设计参考 Claude Code 4 层压缩，做本地化适配——参见
[doc/v2/context-compaction.md](../v2/context-compaction.md)。

- **ISSUE-65**：基础设施 4 个 helper（`compose/token_estimate.go`、
  `compose/message_group.go`、`compose/safe_truncate.go`、`compose/circuit_breaker.go`）。
  Token 按 ContentBlock 类型估算 + ×4/3 保守填充，优先 API 真实 Usage；
  消息按 RunID 分组（user 归入紧随 assistant 同组）；切点配对感知拉回保 tool_use ↔
  tool_result + 同 RunID 块连续；通用计数型断路器
- **ISSUE-66**：MicroCompact——tools 节点末段跑一次微压缩，按白名单
  （Read/Bash/Grep/Glob/WebSearch/WebFetch/Edit/Write）清旧 tool_result 内容，
  保 tool_use ↔ tool_result 配对结构；KeepRecent 默认 5
- **ISSUE-67**：AutoCompact——modelNode 入口估算 token 超阈值时调用 cfg.Model
  生成 9 段结构化摘要替代早期消息；NO_TOOLS_PREAMBLE 双放、`<analysis>` chain-of-thought
  + 后处理删除、附件重注入（Read tool_result 倒序按 path 收集）、断路器
- **ISSUE-68**：ReactiveCompact——LLM 报 prompt-too-long 时按 token gap（best-effort
  regex 解析 OpenAI/Claude 错误格式）删最旧消息组并重试；MaxAttempts=3；删后首条
  若是 assistant 前补 user marker（API 要求 user 开头）

SessionMemory（第 4 层）由原 RunSession + Memory 接口承担，无新代码。

---

## ✅ ISSUE-64 · Subgraph 作头等节点（GraphBuilder.AddGraphNode）

**优先级**：中
**状态**：已实现（2026-05-03）

详见：[issue-64-subgraph-as-first-class-node.md](issue-64-subgraph-as-first-class-node.md)

`compose.RunSubgraph` / `ResumeSubgraph` 之上的语法糖，把 `*Graph` 作为节点嵌入
另一图：`b.AddGraphNode("step", subGraph, opts...)`。默认契约 input = parent.LastOutput,
output 写回，Session 共享，OrchHooks 继承（streaming 跨子图边界）。新增 ctx-based
`WithOrchHooks` / `GetOrchHooks` 工具让父执行流的 hooks 透到嵌套子图。对标 eino
`AddGraphNode` / LangGraph `add_node(name, subgraph)` 模式。

---

## ✅ ISSUE-63 · 实时 streaming via OrchestrationHooks

**优先级**：高
**状态**：已实现（2026-05-01）

详见：[issue-63-realtime-streaming-via-orch-hooks.md](issue-63-realtime-streaming-via-orch-hooks.md)

把 `OrchestrationHooks.OnNodeEvents([]event)` batch drain 改为 `OnNodeEvent(*event)`
单事件实时投递。`eventSink` 从 slice + drain 重写为持有 `{hook, nodeID, ctx}` 的薄
包装，state.Send 直接同步触发 hook——前端 SSE token-by-token 渲染。BasicAgent
modelNode 走 `chatModel.Stream` 边读边 emit chunk；Resume 启动前先回放 cp.Events
让前端拿到完整历史 + 实时增量同通道。框架级 panic recover 保护所有 hook 调用。
plan_chat_web 前端 `appendChunkToBubble` 按 run_id 累加渲染。

---

## ✅ ISSUE-62 · fan-out 超步化重写（方案 B 落地）

**优先级**：中
**状态**：已实现（2026-05-01）

详见：[issue-62-fanout-superstep-rewrite.md](issue-62-fanout-superstep-rewrite.md)

### 摘要

之前的问题：fan-out 当前实现为「单合成节点内 fork-join」，对图引擎是单节点。
Resume 时整个合成节点重新执行 → 所有分支重跑（成功的也重跑）。

方案 B（采用）：让 fan-out 编译成真正的超步 frontier，复用 ISSUE-46
`superstep_snapshot` 机制（per-branch delta + 入口快照 + ResumeDirected 仅重跑
失败分支）。Step 1 扩展超步基础（subgraphCheckpoints 持久化、HITL 透传）；
Step 2 ParallelAgent / RoutingAgent 拓扑改写为多节点；Step 3 SendFrom 改 sink-driven
调度（对标 LangGraph `Send`）。

冻结测试 `TestParallel_AsymmetricInterrupt_ResumeRerunsSuccessful` 已反转：
success 分支 Resume 后 calls=1（旧版 calls=2），证实成功分支不再重跑。

ISSUE-61 的「裸 NodeFunc Resume 重跑所有分支」临时约定一并消除——所有 fan-out
路径（含 AddFanOutEdge 与 SendFrom）都走超步快照。

### 历史背景（保留对照分析）

### 问题

ISSUE-60 v1 + ISSUE-61 让所有 fan-out 路径支持并发 ask_user Resume，但暴露的限制：
**Resume 时所有分支重新执行，包括上次已成功完成的**。覆盖：

- `agent.ParallelAgentNode`
- `agent.RoutingAgent` Router 多目标 fan-out
- `compose.AddFanOutEdge`
- `compose.SendFrom`

**症状**：
- LLM 调用 / 工具调用浪费一轮（成本 + 时延）
- 不幂等副作用（DB 写、邮件、外部 API）执行第二次
- 同时也是 ISSUE-61"裸 NodeFunc Resume 重跑所有分支"那条文档约定的根因

**根因**：fan-out 当前实现为 **「单合成节点内 fork-join」**——`__fanout_<from>__`
合成节点 / `ParallelAgentNode` body 节点内部用 `sync.WaitGroup` 并发跑所有分支，
对图引擎是单节点。引擎只有"当前节点 ID"粒度的 cp，没法记录"分支 i 已完成、
分支 j 待重跑"。Resume 时整个合成节点重新执行 → 所有分支重跑。

### 对照设计

实证比较了 LangGraph 与 eino（local checkout）：

**LangGraph `pending_writes`**（`libs/checkpoint/.../base/__init__.py:30` +
`libs/langgraph/.../pregel/_loop.py:393, _loop.py:565`）：

```python
PendingWrite = tuple[str, str, Any]   # (task_id, channel, value)
```

- task 完成 → `put_writes(task_id, writes)` 立刻 append 到 `checkpoint_pending_writes`
- 超步内某 task interrupt → cp 保存「超步前 channels 快照 + 已完成 task 的 pending_writes」
- Resume → `prepare_next_tasks` 只放"errored/interrupted" task 进 next_tasks；
  成功 task 的 writes 直接应用到 channels（不重跑）
- **invariant**：任何 superstep，成功 task 最多执行一次

**eino 双层（compose vs adk）**：

- compose 层（`compose/dag.go::dagChannel.Values` + `compose/graph_run.go:511`）
  做了 LangGraph 同款的事——channels 持久化 + RerunNodes 列表，等价 pending_writes
- adk 层（`adk/workflow.go::parallelWorkflowAgent.runParallel`）只有 events 重放
  （`SubAgentEvents map[int][]*agentEventWrapper`），未走 channel 路径——因为
  Agent 在 adk 里是 stateful 黑盒，拿不到结构化 channel writes
- adk 是降级方案，受限于"Agent 必须是黑盒"的 SDK 契约

**我们的处境**：ParallelAgent 直接是 `compose.NodeFunc`，没有 adk 那种黑盒边界
——eino adk 不能做的事我们能做。**因为我们的 agent 编排层和图引擎层没分家**，
不必照抄 eino adk 的 SubAgentEvents 降级路径。

### 方案 B（终态根因修，与 LangGraph 同款）

**让 fan-out 编译成真正的超步 frontier，复用现有 ISSUE-46 superstep_snapshot 机制。**

ISSUE-46 已经实现了核心机制：
- per-branch delta 持久化（成功分支产出存 snapshot）
- 入口快照保 Messages/localState，Resume 时入口与首次一致
- ResumeDirected 模式只重跑失败/中断分支，成功分支从 delta 回放

**fan-out 接入超步路径后此机制自动覆盖两条 ISSUE-62 路径 + ISSUE-61 裸 NodeFunc 限制**。
Agent fan-out 多一层细节但不冲突：分支 Agent 内部 cp 已存 `state.subgraphCheckpoints`，
此 map 是 GraphState 字段，superstep_snapshot 序列化整个 state 时一起持久化——
两套 cp 机制（superstep delta 管"哪些分支已成功"，subgraphCheckpoints 管"中断分支
内层状态"）正交叠加。

### 落地三步

**Step 1：`ParallelAgentNode` / `RoutingAgent` fan-out 超步化**

- 编译期把分支注册为图节点而非合成单节点内闭包
- `dispatchBranchAgent` 调用搬到分支节点的 NodeFunc 闭包内
- 验证 ISSUE-46 superstep_snapshot 是否如预期覆盖 subgraphCheckpoints
- 工作量小，直接验证机制

**Step 2：`compose.AddFanOutEdge` 编译期改写**

- 同 Step 1 模型，删除 `__fanout_<from>__` 合成节点
- 用图原生 fan-out 边 + 多入边收敛
- 同时解决 ISSUE-61 裸 NodeFunc"Resume 重跑所有分支"约定

**Step 3：`compose.SendFrom` sink-driven 调度重构**

- 当前 `SendFrom` 在节点闭包内调 `executeSendDirectives` 完成 fork-join
- 改为：`SendFrom` 仅写 sink，scheduler 在下个超步从 sink 取 directives 派 frontier
- 与 LangGraph `Send()` 同模型——动态分支 = 运行时注入新超步 task
- 工作量最大，但是 LangGraph 都要做的事，不是我们独有难点

每步独立交付且不破坏既有功能。

### 方案 A（仿 eino adk SubAgentEvents 重放，**不推荐**）

之前考虑过的降级方案：在 outer cp metadata 加 `cpKeyBranchOutcomes` 缓存成功
分支的 Output + Events，dispatchBranchAgent 命中缓存时跳过 RunSubgraph。

不推荐原因：
- eino adk 走这条路径是被 SDK 黑盒契约逼的，我们没此约束
- 仅适用 Agent fan-out（裸 NodeFunc 没 events 流）；ISSUE-61 的限制还得另开方案
- 与 ISSUE-46 superstep_snapshot 是两套不兼容的 cp 机制，重叠功能

### 冻结测试

- `agent/parallel_resume_test.go::TestParallel_AsymmetricInterrupt_ResumeRerunsSuccessful`
  断言"成功 success 分支被 Resume 重跑（calls=2）"——方案 B Step 1 落地时
  此断言会主动失败提醒更新为 `calls == 1`

---

## ✅ ISSUE-61 · compose.fanout / RoutingAgent fan-out / SendFrom 并发分支 ask_user

**优先级**：中
**状态**：已实现（2026-05-01）

详见：[issue-61-fanout-paths-concurrent-ask-user.md](issue-61-fanout-paths-concurrent-ask-user.md)

ISSUE-60 v1 后剩余三处 fan-out 路径全部接入并发 ask_user 路径。

**改动一：`agent/routing.go` 多目标 fan-out**

复用 ParallelAgentNode 的 `dispatchBranchAgent` + `state.pendingInterrupts` buffer
模式——分支用 `route:<name>#idx` 命名，`NestableAgent` 走嵌套 cp 协议。同时为
RoutingAgent 加 `Run` 透传 ctx 中 Checkpointer + 新增 `Resume(ctx, sessionID,
opts...)`（仅 Router 路径支持，LLM 分类器路径单目标无 fan-out 需求）。`runWithRouter`
重构为 `buildRouterGraph(input)`，Run/Resume 共享构图；`dispatchNode` 闭包内引入
`stateInput` 让 Resume 路径下 input 为 nil 时能从 `agentInput(state)` 兜底。

**改动二：`compose/fanout.go::makeFanOutFunc` 与 `compose/send.go::executeSendDirectives`**

裸 NodeFunc 路径同样接入 `state.pendingInterrupts` buffer。新增 `compose.collectBranchInterrupts(ctx, state, ids, results)`：扫分支 *InterruptError，
对 Query.InterruptID 为空的（裸 NodeFunc 不经 saveCheckpointOnError 自动回填）
用 `MakeInterruptID(parentRunPath, "<targetNodeID>#<idx>", 1)` 生成稳定 ID 后写
buffer，返回中断分支总数。

**裸 NodeFunc Resume 限制（ISSUE-61 临时决策；根因修在 ISSUE-62 方案 B）**：

- 裸 NodeFunc 没有 inner cp 落点，Resume 时**所有**分支重新执行——包括上次成功
  完成的分支
- 节点函数必须自己保证 Resume 重跑幂等：通过 `compose.GetResumeContext(ctx)`
  判 `nil`/非 nil 区分"首次执行"vs"被恢复过"，或在 `GraphState` typed state
  里写"已完成"标志
- ISSUE-61 落地时此限制是临时约定（避免 lane-aware cp 大改）；ISSUE-62 方案 B
  把 fan-out 编译为超步原语后，复用 ISSUE-46 superstep_snapshot 机制，此限制
  与 Agent fan-out 的 ISSUE-62 重跑问题**一并消除**，无需保留为长期 contract

**改动三：`compose.executeSendDirectives` 的 cp 持久化路径**

ISSUE-61 之前 sendErr 路径只 wrap 错误返回，未触发 saveCheckpointOnError——SendFrom
分支挂起时 outer cp 完全不写，Resume 立刻 "checkpoint not found"。修为
sendErr 非 nil 时显式 save 后再返回。

**端到端测试**：
- `agent/routing_resume_test.go::TestRouting_FanOut_ConcurrentAskUser_Resume`
- `compose/fanout_resume_test.go::TestFanOut_ConcurrentInterrupt_ResumeRoundtrip`
  （断言裸 NodeFunc Resume 后各分支重跑 1 次）
- `compose/sendfrom_resume_test.go::TestSendFrom_ConcurrentInterrupt_ResumeRoundtrip`
  （断言同节点多次派发的两分支拿到不同 InterruptID）

**死代码清理**：删除 `compose.checkParallelInterrupt` / `compose.CheckParallelInterruptResults`，
所有 fan-out 路径不再有"暂不支持"拒绝。

---

## ✅ ISSUE-60 · ParallelAgent 并发 ask_user HITL 端到端

**优先级**：高
**状态**：v1 已实现（2026-04-30）—— 仅覆盖 `agent.ParallelAgentNode`；
其它三处 fan-out 路径在 ISSUE-61 补齐（2026-05-01）

详见：[issue-60-parallel-concurrent-ask-user.md](issue-60-parallel-concurrent-ask-user.md)

ISSUE-59 P1 落地多 pending interrupt 寻址（`InterruptID` + `cpKeyInterrupts map` +
`ResumeOptions.UserInputs map`）后，`agent.ParallelAgentNode` 终于可以让多个
并发分支同时 `ask_user`：

**核心改动**：

1. `compose.GraphState.pendingInterrupts` buffer + `AddPendingInterrupt` /
   `PendingInterrupts` 方法：节点级 outgoing 通道。
2. `compose.saveCheckpointOnError` union `state.pendingInterrupts` 进
   `cpKeyInterrupts`——本节点收集多 query 一起持久化到外层 cp。
3. `agent.ParallelAgentNode`：分支 ID 用 `name#idx` 保证 subgraphCheckpoints 索引
   唯一；`dispatchBranchAgent` 优先走 `NestableAgent.RunNested`（嵌套 cp 协议），
   fallback 才走 `retryRunAgentNodeMsg`；wg.Wait 后扫 InterruptError 把 Query 写
   buffer，最后抛单一 outer InterruptError。
4. `compose.appendResumeMessages` 加嵌套防污染：`id != "" && !pendingInterrupts[id]`
   silently drop——多分支 Resume 时父层把整张 UserInputs map 透给每个子分支，
   子分支只消费匹配自己 `InterruptID` 的条目。
5. `compose.ResumeGraph`：`hasSubgraphCheckpoints(state)` 时跳过 outer Messages
   append（外层无对应 assistant tool_calls，避免 phantom tool_result 污染）。
6. `agent.basicagent_graph.executeReactGraph` nested 路径：从 `ResumeContext` 选
   一个非空字段转发到内层（UserInput 单值优先；否则 UserInputs map）。
7. `agent.ParallelAgent.Run`：从 ctx 取 `Checkpointer` 透给 `GraphRunOptions`。
8. `agent.ParallelAgent.Resume(ctx, sessionID, opts...)`：HITL 恢复入口；
   `WithResumeUserInputs(map[id]*Message)` 对接精确配对。
9. `agent.ResumeConfig.UserInputs` + `WithResumeUserInputs`：agent 层暴露多值答案
   注入选项。

**端到端测试**：`agent/parallel_resume_test.go::TestParallel_ConcurrentAskUser_ResumeWithUserInputs`
覆盖 2 分支并发 ask_user → 外层 cp 持有 2 条 cpKeyInterrupts + 2 条 subgraphCheckpoints
→ Resume(UserInputs) → 两 worker 第二轮 LLM 看到正确 tool_result（且不含兄弟分支
答案）→ merger 合并产出。

**已知局限**：本 issue v1 仅 `agent.ParallelAgentNode`；其它 fan-out 路径在
ISSUE-61 补齐（裸 NodeFunc 路径接受"Resume 重跑所有分支"约定）。

---

## ✅ ISSUE-59 · BasicAgent 内部 ReAct 循环 Graph 化（HITL 端到端闭环）

**优先级**：高
**状态**：✅ 已完成（P1 + P2 已落地，ISSUE-71 完整闭环）

**最终落地**（分两 phase）：

**P1**（ISSUE-59 P1，2026-04-30）：
- AgentEvent 持久化 + AgentAction.CustomizedAction 走 RegisterType 注册
- HITL 中断寻址：AskQuery.InterruptID 由 saveCheckpointOnError 自动填（fnv64a 16 chars），多 pending 用 cpKeyInterrupts map by ID
- ResumeOptions.UserInputs map by InterruptID 配对答案
- 嵌套不冒泡污染靠 query.NS != JoinNS(GraphNS, nodeID) 指纹判断

**P2**（ISSUE-59 P2，2026-05-01）：
- BasicAgent 内部改写为 model + tools 两节点 Graph + 循环回边
- 状态从 Go 栈搬到 typed `basicAgentState`，跨 InterruptError 走 cp round-trip 不丢
- 每轮 LLM/tool 调用立刻 emit `OutputEvent` 到 session（修原中断分支不写 session 的 gap）
- OnAgentEnd 改 defer 兜底（整 Run 一次，不再每轮触发）
- runReactGraphNested 嵌入路径走嵌套 cp 协议
- callModelStreaming：Stream 优先 + chunk 实时投递 + 失败降级 Generate

**ISSUE-71 完整闭环**（2026-05-04）：
- 任意 Agent 嵌入任意 graph，HITL 全闭环（chat_entry → AgentAsTool(PlanAgent) → BasicAgent.ask_user → Resume）
- 嵌套 cp 自动派生 `(parentSID, JoinNS(parentNS, parentNodeID))`

**原依赖**："ISSUE-48 嵌套 checkpoint 子集"已被 ISSUE-64 + ISSUE-70 Phase 1/2 ns 物理分离协议完整实现。

详见：[issue-59-basicagent-graph-react.md](issue-59-basicagent-graph-react.md)

---

## ✅ ISSUE-58 · Interrupt 触发时机语义：立即挂起 vs HITL 协议层

**优先级**：中
**状态**：✅ 已完成（协议层 Steps 1-5 已落地 2026-04-29 + ISSUE-59 P1/P2 端到端闭环 + ISSUE-71 任意嵌入闭环）

三个核心问题已澄清：

1. **单步**：维持节点自主 `return InterruptError`（不引入外力 cancel）—— BasicAgent
   内部 tool 返回 / OnToolStart / OnToolEnd 三处违反"立即挂起"语义的路径已修
2. **超步并行**：保持 ISSUE-46 best-effort 策略（兄弟不 cancel），与业务错统一
3. **Resume 重跑范围**：单分支只重跑该分支（ISSUE-46 快照机制）；超步内并发分支
   ResumeReplan 暂不支持（显式拒绝）

**HITL 协议层实现**（Steps 1-5）：

- `agent.InterruptError.Query *AskQuery` + checkpoint 持久化
- `compose.ResumeOptions{Mode, UserInput, OverrideStartNode, OrchHooks}` +
  按 Query.ToolCallID 包 tool_result 注入
- `compose.ResumeContext` + `GetResumeContext`（不污染 session）
- `agent.ResumeAction` 三档（Continue / Replan / Abort）+ `PlanAgent.Resume`
- `agent.WithAskUser()` opt-in 内置 ask_user tool
- plan_chat_web 接入：`/api/chat?action=continue|replan|abort&text=...` + 三按钮 UI

**原已知 gap 已闭环**：BasicAgent 嵌入场景下 ask_user 端到端 → ISSUE-59 P1/P2 完成后已通；
ISSUE-71 接口合并后任意编排嵌入任意 graph 都闭环。端到端测试
`agent/plan_test.go::TestPlan_Resume_AskUserTool_EndToEnd` 已激活并稳定通过（无 t.Skip）。

详见：[issue-58-interrupt-timing-semantics.md](issue-58-interrupt-timing-semantics.md)

---

## ✅ ISSUE-57 · retry 路径对 InterruptError 盲重试 + InterruptError.Data 跨 Resume 类型保真

**优先级**：高
**状态**：已实现（2026-04-28）

ISSUE-56 实施 plan_chat_web HITL 时审计 retry / Resume 路径发现的两个潜在踩坑：

**Bug 1**：`compose/helpers.go::retryRunAgentNode{,Msg}` 不识别 `*agent.InterruptError`，
用户配 `WithPlanMaxRetries(N>0)` 时 hook 抛 InterruptError 会盲重试 N 次（含退避延迟）才往
上抛——浪费 N 轮 LLM 调用 + 用户体感"挂起按钮卡了几秒"。语义上正确（最终还是把 wrap 后的
InterruptError 透出），但实操上是 bug。

**Bug 2**：`agent.InterruptError.Data` 是 `any` 类型，**同进程 errors.As 拿到原始 struct 工作
正常，但 JSON Save → Load 后退化成 map[string]any / string / float64**。用户塞自定义 struct
进去 Resume 后 type assertion 必失败——只有翻 ISSUE-40 文档或实际调试才发现。

**Bug 1 修复**：抽 `isRetryable(err)` helper（不重试 InterruptError + ctx 取消）；
`retryRunAgentNode{,Msg}` 主循环加 `if !isRetryable(err) { return nil, err }`。3 条回归测试覆盖
（NotRetryable_InterruptError / NotRetryable_CtxCanceled / RetryableErrorDoesRetry 反证）。

**Bug 2 修复**（参考 ISSUE-51 #6 套路：godoc 警告 + Save 时 warn）：
- `agent/errors.go` `InterruptError` godoc 增补完整 "Data 跨 Resume 类型保真度" 段落，含推荐
  用法（运行时随便塞 / 跨 Resume 用 map[string]any 或升格专属字段）
- 新增 `agent.MaybeWarnInterruptDataSerialization(ctx, ie)`：用 reflect 扫 Data 类型，
  struct / pointer-to-struct 时 slog.Warn 提示
- `compose/checkpoint.go` 两个 InterruptError save 路径都调用（saveCheckpointOnError 单节点 +
  buildBranchDelta 超步分支）
- 抽 `containsStructy(t reflect.Type)` 递归 helper：覆盖直接 struct/pointer-to-struct +
  slice/array/map of struct + 多层嵌套 collection（`[][]MyStruct` 等），JSON 反序列化丢类型
  的所有形态都包了
- 16 子用例表驱动测试：8 静默（nil/基础类型/map[string]any/slice of basic/nil pointer 防
  panic）+ 8 警告（直接 struct + slice/array/map 嵌 struct + nested 嵌套）

**设计原则**：类型层面无法保证的契约（`any` 字段跨 Resume），框架必须在持久化前主动警告，
不让用户在 Resume 后现场调试。这是 ISSUE-40/51 #6/55 同款 "godoc + warn 双保险" 模式。

详见：[issue-57-retry-skips-interrupt-and-data-typesafety.md](issue-57-retry-skips-interrupt-and-data-typesafety.md)

---

## ✅ ISSUE-56 · plan_chat_web HITL（暂停 / 恢复）+ PlanAgent.Resume API

**优先级**：高
**状态**：已实现（2026-04-28）

用户实跑 plan_chat_web 时反馈"页面没终止按钮"。最初提"客户端 `eventSource.close()`"
方案被否决：

> 如果只是加前端按钮 eventSource.close() 那不是不能中断恢复了？

→ 真实需求是 **interrupt（可恢复的暂停）**，不是 cancel。框架 `*agent.InterruptError`
本就是"挂起 + 后续可 resume"语义，要做这个就得走 checkpoint 路径。

**调研发现框架原语完整但 PlanAgent 没暴露 HITL 入口**：
- ✅ `agent.InterruptError` / `RunGraph` 自动 save / `ResumeGraph` 都已有
- ❌ `PlanAgent.Run` 写死 `GraphRunOptions{}.Checkpointer = nil`，不存档
- ❌ `PlanAgent` 没有 `Resume` 方法

**框架改造**：
- 抽 `buildGraph(initialInput)` 共用 + `summarizeFinal(final)` 抽 final 输出汇总
- `Run` 从 `agent.GetCheckpointer(ctx)` 取 Checkpointer 透给 GraphRunOptions
- 新增 `Resume(ctx, sessionID, opts...)`：用 `ResumeGraph` 续跑，最终输出对齐 Run

**plan_chat_web 服务端**：
- `runSlot{sess, cp, interrupt atomic.Bool, finished atomic.Bool}` + 全局 `runs sync.Map`
  按 session_id 维度复用（单进程内存模式，上生产前换 Redis）
- `interruptHook` 挂 hook 链：`OnAgentStart` 检查 flag，触发抛 `*InterruptError`
- 路由：`GET /api/chat?q=...` 新建 / `GET /api/chat?session_id=...` 恢复 / `POST /api/interrupt?session_id=...` 触发挂起
- `runOrResume` 统一 goroutine 主体：识别 InterruptError 推 `interrupted` SSE 事件，正常完成
  推 `final` + `done` 后清理 slot
- mock 加 500ms `select ctx.Done()` 延迟，给 HITL 中断留窗口 + 让用户看清流程

**plan_chat_web 前端**：
- 加 `⏸ stop` / `▶ resume` 按钮，按状态机 `idle ↔ running ↔ paused → finished` 切换可见性
- `stop()` POST `/api/interrupt`；`resume()` 用同 session_id 重连 EventSource
- SSE `interrupted` / `resume` 事件分别处理状态机和 session_id 持久化

**端到端冒烟**：脚本验证 `chat → 0.7s 后 interrupt → SSE 推 interrupted → resume → final` 完整闭环通过。

**回归测试**：`TestPlan_InterruptAndResume`（mock sub-agent 抛 InterruptError → checkpoint 自动 save → Resume 续跑到 finalAnswer）+ `TestPlan_Resume_NoCheckpointer`（ctx 缺 Checkpointer 立即报错）。

**已知遗留**（本次不修，记下后续）：
- `retryRunAgentNodeMsg` 不识别 InterruptError 会盲重试 N 次（plan_chat_web 没用 retry 没踩到）
- InterruptError.Data 跨 Resume 的类型保真度（ISSUE-40 已记）
- InMemoryCheckpointer + sync.Map 跨进程不共享（用户确认上生产前换 Redis）

详见：[issue-56-plan-chat-web-hitl.md](issue-56-plan-chat-web-hitl.md)

---

## ✅ ISSUE-55 · PlanAgent step result 双重前缀（mapper 与 defaultExecInput 都加"步骤 X（...）"）

**优先级**：高
**状态**：已实现（2026-04-27）

`plan_chat_web` 真 LLM 跑 SSE 事件流审查发现：sub-agent 的 input prompt 里 `"步骤 1（查资料...）"`
出现两次——一次外层标头，一次塞在 result 内容前。

**Root cause**：默认 `stepResultMapper` 给 `out.Content` 包装成 `"步骤 X（desc）：<原文>"` 存进
`executed[i].Result`；同时 `defaultExecInput` 拼下游 prompt 又加 `"步骤 X（desc）的产出：\n<result>"`
标头。两层叠起来同句话重复出现。

违反 ISSUE-47 Supervisor 范式 "框架不猜不提纯"——mapper 强行替消费方"猜"上下文格式，
但消费方已经从 `ExecutedStep.Step` 字段拿到 description，不需要 result 自带。

**修复**：默认 mapper 改为裸 `out.Content`；上下文标头由消费方拼（`defaultExecInput` 现有
`"步骤 X（...）的产出：\n<result>"` 格式合理保留；`buildReplanPrompt` 已是 self-format 也保留）。
godoc 明写"mapper 只做格式转换，不替消费方拼上下文"。

**收益**：sub-agent prompt 干净一次，token 浪费消除；replan prompt 同步干净。

**回归哨兵**：`TestPlan_DefaultStepResultMapper_NoPrefix` 跑两步 plan，第二步检查
`executed[0].Result == "RAW_LLM_OUTPUT"`（裸内容），回退到加前缀立即红。

**事件流附带审计**：触发本 bug 时顺便把 SSE 事件流过了一遍，除本 bug 外其余皆正常
（plan↔exec 节奏、ReAct 多轮、run_id 唯一、path 嵌套、node 配对都对）。

详见：[issue-55-plan-step-result-double-prefix.md](issue-55-plan-step-result-double-prefix.md)

---

## ✅ ISSUE-54 · PlanStep.InputRefs 解析时机修正（从闭包搬到 PlanAgent.execNode）

**优先级**：高
**状态**：已实现（2026-04-27）

`plan_chat_web` 真 LLM 模式跑出 bug：LLM 初始计划返多步，step 1 (writer) 的 refs:["researcher"]
指向同批 sibling step 0 (researcher)。闭包内 `resolveRefs` 在解析时校验 executed（首次为空），
researcher 不在里面 → 立即报错 `agent "researcher" not in executed`。

**第一层 root cause**：refsPromptHelp 给 LLM 看的示例就是错的——示例里写了 sibling refs，LLM 学
着写命中校验。

**第二层 root cause（真正修的）**：职责错位。PlanAgent 只跑 `remaining[0]`，step[1:] 永远被丢弃；
但闭包对每个 step 都做 ref resolution，对 step[1:] 用的是 stale snapshot 必然错。**只有框架知道
完整运行时状态的事，永远不该让用户代码做**。

**修复**：把 ref resolution 从闭包搬进框架。

- 新类型 `RefSpec`：`{Index int, AgentName string}`，"待解析的引用"
- `PlanStep.InputRefs []int` → `[]RefSpec`
- 新增 `ResolveInputRefs(refs, executed) ([]int, error)`
- `PlanAgent.execNode` 即将执行 step 时调用 ResolveInputRefs，executed 是真实状态
- `ExecInputBuilder` 签名加 `resolvedRefs []int` 参数（drop 死参数 `plan`）
- 闭包退化为纯 JSON 解析：`parseRawRefs` 把 int/string 包成 RefSpec，不再校验
- prompt 从"硬约束"降级为"节奏建议"（每次输出 1 步省 token，但不再是正确性必需）

**收益**：
- sibling refs 不再翻车（step[1:] 永远不进 resolve）
- 错误信息携带真实运行时状态，不是 stale snapshot
- 新 Planner 实现者物理上没法写错——RefSpec 不需要 resolved index
- prompt 不再是安全机制；即使 LLM 完全忽略 prompt 出多步，框架也不崩

详见：[issue-54-planner-refs-resolution-timing.md](issue-54-planner-refs-resolution-timing.md)

---

## ✅ ISSUE-53 · executeRound 拆分（ISSUE-51 #2 拆出独立追踪）

**优先级**：中
**状态**：已实现（2026-04-27）

ISSUE-51 #2 原列为"executeRound 拆分"延后项，2026-04-27 单独拆出独立 issue 后立即落地。

**当初为什么要拆**：`executeRound` 一函承载 model 调用、tool 循环、8 种 hook 触发、memory
注入、history compact、session stamping、runID stamp、skill 路由——743 行测试代码只为覆盖
这一个函数是明确的复杂度信号。ISSUE-51 #1 的 `tc.ID` 笔误漏洞，根因之一就是函数太长，作者
凭记性记"hook 后该读哪个"，漏一处就是 bug。

**落地方案**：抽 3 个未导出 helper，executeRound 主体降到 ~20 行实际逻辑：

- `callModelStep`：OnModelStart → 比对/重绑 chatModel → Generate → stampRunID → OnModelEnd → 再 stampRunID。**不碰 outMessages**
- `runToolCalls`：遍历 toolCalls → 委托 `executeToolCall`，聚合 tool_result + 首个中止错误
- `emitFinalAnswerEvents`：无 tool_call 轮往 session 写 inputEv + exitEv（共享 runID）

**设计取舍**：`defer OnAgentEnd` 留顶层（与 reactLoop 的 OnAgentStart 配对）；`outMessages`
append 由主体显式做（helper 不 mutate）；不打包 `roundDeps` struct（违反 "ctx 只传不存"）；
不返回更新后的 chatModel（避免暗示重绑跨轮可见）。reactLoop 的"准备阶段"45 行但顺序线性、
每步独立——本次不拆，等真正出现复用诉求再单独评估。

**结果**：单函数最大复杂度 110 → 56；agent/compose/hook/mcp/skill 全部单测零改动通过；
原 `react_coverage_test.go`（743 行端到端）行为不变它们照过。

详见：[issue-53-react-loop-split.md](issue-53-react-loop-split.md)

---

## ✅ ISSUE-52 · 测试与 CI 流程缺口

**优先级**：中
**状态**：✅ 已落地（2026-09-07 收尾：#2 MR 模板 + #4 覆盖率 artifact 补齐；CI 矩阵早在 `8f5e626` 落了 GitLab CI 六道关）

**2026-04-27 落地**：integration build tag + Makefile + `doc/description/testing-conventions.md` + CLAUDE.md 测试段落细化。~~未做：PR 模板、CI 矩阵、覆盖率快照（依赖仓库引入 GitHub workflow）~~

**2026-09-07 收尾**：仓库远端是公司内部 GitLab 不是 GitHub——CI 矩阵 = `.gitlab-ci.yml`（build / test / race / vet / lint / coverage 六 job，`8f5e626`）；MR 模板 = `.gitlab/merge_request_templates/Default.md`（「exported API 必带单测」等 7 条检查 + 「怎么验」栏）；覆盖率快照 = coverage job 的 `coverage.out` artifact（留 30 天）+ GitLab 抓 total 进 MR 概览 / 徽章。

与 ISSUE-51 同一轮测试工作中暴露的流程问题：

1. **集成测试与单测未隔离**：`TestAdv_07_PlanAgent` 等走真实千问 API（5 min deadline）混在默认 `go test ./...`，无网络/凭证环境直接 red——建议用 `//go:build integration` tag 分离，CI 拆 `test-unit` / `test-integration` 两条线
2. **关键子系统无基线覆盖**：`agent/skill_tool.go` 0%、`applyRunOptions` 16.7%——合入时没有"新增公共 API 必须带单测"的 PR 约束
3. **黑盒/白盒测试风格未统一**：CLAUDE.md 倾向黑盒但 `compose` 多数白盒（访问 `parsePlanToolArgs` 等未导出函数），现状合理但规约需更新
4. **覆盖率快照未纳入 CI/文档**：历次提交覆盖涨落无记录，只能人工 `go tool cover` 巡检
5. **表驱动用例粒度不统一**：错误路径差异化 stub 本不适合强塞 table，CLAUDE.md "优先表驱动"需要细化

详见：[issue-52-testing-process-gaps.md](issue-52-testing-process-gaps.md)

## ✅ ISSUE-51 · ReAct/Planner/GraphAgent 内部设计债（测试覆盖中暴露）

**优先级**：中
**状态**：全部已修（2026-04-27）；#2 通过 ISSUE-53 落地

在单测覆盖扩展过程（`agent` 76.5%→90.3%、`compose` 69.9%→80.3%）中除修复具体 bug 外暴露的 6 个结构性设计问题：

1. ✅ **#1 Hook 消费规约**：tool-call 循环体抽出 `executeToolCall` helper，签名不接 `schema.ToolCall` —— 物理隔离 `tc.X` 误读，编译期失败。新增 `TestTool_InvalidArgs_HookRewritesCallId_RegressionGuard` 锁住 JSON 解析分支的 `tool_call_id` 必须取自 hook 改写后的值
2. ✅ **#2 executeRound 职责过载**：已通过 ISSUE-53 实现拆分（抽 3 个 helper，主体 110→43 行）
3. ✅ **#3 Planner 哨兵错误**：新增 `ErrPlannerNoToolCall` / `ErrPlannerEmptySteps` / `ErrPlannerBadToolCall` / `ErrPlannerParseArgs` 4 个哨兵，`llm_planner.go` 全 `%w` 包装；4 个原本依赖 `strings.Contains` 的测试切到 `errors.Is`
4. ✅ **#4 GraphAgent nil 兜底**：第三层（LastOutput 与 Messages 都空）改为返回 `NodeFunc 契约违规` 错误，不再静默合成空 assistant 让上层难分"答案为空 vs 节点忘写"
5. ✅ **#5 Memory 软依赖文档**：`Memory` 接口 godoc 增补"# 错误语义（软依赖）"段落 + 升级为硬中断的实现路径
6. ✅ **#6 CustomizedAction warn**：新增 `agent.MaybeWarnCheckpointSerialization`，`InMemoryCheckpointer.Save` 调用；`Checkpointer` 接口 godoc 规约外部实现也调用。扫到 `AgentAction.CustomizedAction != nil` 就 `slog.Warn` 提示不会跨 Resume 存活

详见：[issue-51-react-internal-design-debt.md](issue-51-react-internal-design-debt.md)

---

## ✅ ISSUE-50 · 跨多次 graph 执行的会话（ThreadID + GraphID 防串台）

**优先级**：中
**状态**：P0 ✅ 已实现（GraphID 校验防串台，2026-05-07）；~~P1 ✅ 已实现（ThreadID 一级字段，2026-05-07）~~ **P1 已被 [ISSUE-98](issue-98-conversation-history.md) 推翻并删除**（2026-07-23）——`cp.ThreadID` + `ListByThread` / `LatestByThread` 受制于「成功即清」只能返当前挂起 run，名不副实；会话历史改由 `agent_v2` 独立 append-only 层承担

### P0 · GraphID 校验落地（2026-05-07）

实施位置：
- `compose/graph.go` — `Graph.name` 字段 + `GraphBuilder.WithName(name)` option
- `compose/graph.go` — `Graph.ID()` 方法：优先返回显式 name，否则
  `sha256(sorted(nodeIDs) || stateFactoryTypeName)` 前 16 hex chars 作 fingerprint 兜底
- `compose/event.go` — `Checkpoint.GraphID string` 字段（向后兼容：旧 cp 反序列化为 ""，跳过校验）
- `compose/graph_id_ctx.go` — `WithGraphID(ctx, id)` / `GetGraphID(ctx)` ctx helpers
- `compose/checkpoint.go::RunGraph` — 入口注入 `WithGraphID(ctx, g.ID())`
- `compose/checkpoint.go::saveCheckpointOnError` + `saveSuperstepCheckpoint` — 写 cp 时
  填 `snapshot.GraphID = GetGraphID(ctx)`
- `compose/checkpoint.go::ResumeGraph` — 入口 fail-fast：`cp.GraphID != "" && cp.GraphID != g.ID()`
  → return error "graph identity mismatch"

> **v2 (compose_v2) 更新（上线前 audit）**：v2 重写时 `graph_id_ctx.go` 的 ctx pair 搬了过来，但 `compose_v2.Run` 入口**从未接线**——防串台实际走 `cp.GraphID = g.ID()` 直读（serialization.go / graph_loop.go）+ SaveNS 冲突守卫 + Run/Resume fail-fast，机制完整。`WithGraphID`/`GetGraphID`/`graphIDKey` 成 dead 残留，已删；防串台不变（三方对照决策记录 `checkpoint-storage.md` 确认 g.ID() 直读 + LangGraph/eino 均无 graph 身份概念）。

测试覆盖（`compose/graph_id_test.go` 6 场景）：
- Fingerprint 稳定性（同拓扑两次 Build → 同 ID）
- Fingerprint 拓扑敏感（拓扑变 → ID 变）
- WithName 优先级
- Resume 跨 graph mismatch fail-fast
- Resume 同 graph 通过校验
- 旧 cp（GraphID 空）向后兼容跳过校验

Sanity-revert 验证：临时去掉 ResumeGraph 里的校验 → mismatch 测试不再 fail，
graph B 居然真能加载 graph A 的 cp 接着跑（"中断：test pause" 是 graph A 抛的 IE
冒泡到 B）→ 证明本 P0 防的串台 bug 是真实风险。

Cross-audit Round 1 加固（commit follow-up）：
- F1 fingerprint 注释纠正：仅区分拓扑 + typed state 类型，**不区分 NodeFunc 行为**（同 nodeID 不同 NodeFunc → fingerprint 同），同名不同行为的 graph 必须 WithName 显式声明
- F2 crossNSPendingInterrupts 加 wantGraphID 过滤：深防御场景下（cleanup 失败 + 业务错共用 sid）挡异 graph 的 InterruptID 污染 PendingExact
- F4 注释 "RunSubgraph 入口注入" 是假承诺，实际靠内部递归调 RunGraph 由 sub.ID() 覆盖
- F5 marshalGraphState 函数加 graphID 参数：签名级强制，新增 cp save 调用点忘传编译报错（替代易漏的"调用方后补 cp.GraphID"模式）
- F3/F6 测试加固：Match_OK / LegacyCp_OK 加更严格 sanity assertion；新增 TestGraphID_Resume_Chain_PreservesGraphID 守护多 round Resume cp.GraphID 不丢；新增 TestCrossNSPendingInterrupts_GraphIDFilter 守护 F2 过滤回归

Cross-audit Round 2 加固（4 盲区扫描发现 2 真 bug）：

**盲区 1（高严重度真 bug）fingerprint 100% 碰撞**：原 `reflect.ValueOf(g.stateFactory)
.Type().String()` 拿到的是 StateFactory 命名类型名（`compose.StateFactory`），
所有 `NewStateFactory[*S]` 构造的 factory 类型名都一样 → 不同 typed state（StateA
vs StateB）只要节点集相同，fingerprint 100% 碰撞 → P0 防护对所有用 NewStateFactory
的业务场景失效（框架推荐用法）。**实测确认**：StateA / StateB fingerprint 都是
`394f10dd09bef950`。修：`Graph.ID()` 调一次 factory 拿真实 typed state 实例，用其
`reflect.Type().String()` 区分（cachedID 缓存只算一次）。新加测试
TestGraphID_Fingerprint_DifferentTypedState 守护回归（sanity revert 确认非 tautological）。

**盲区 2（中严重度真 bug）Save 阶段无 GraphID 校验**：两个不同 graph 共享同 sid 同 ns
时，原实现 `c.data[sid][ns] = &saved` 静默覆盖，A 视角下 Resume 才发现 GraphID
mismatch（错误位置远离根因，bisect 不友好）。修：InMemoryCheckpointer.SaveNS
加 fail-fast `if existing.GraphID != "" && saved.GraphID != "" && existing.GraphID
!= saved.GraphID`，错误从 Resume 提前到 Save。新加测试
TestGraphID_Save_ConflictDetection 守护回归。

**盲区 3 / 4（假阳）**：fan-out ctx GraphID 透传 OK（branchCtx 全部 derive 自外层
ctx，闭包捕获自动透传）；4 层压缩（Micro/Auto/Reactive）全无 cp.Save / SaveNS 调用，
压缩失败/中断走 IE 冒泡命中 saveCheckpointOnError 同路径，无漏路径。



当前 `RunSession.ID` 定位等同 LangGraph 的 `run_id`（单次执行 ID），没有"跨多次 `RunGraph`/`ResumeGraph` 调用"的一级会话概念。多轮对话、HITL 长流程、平台多 agent 协作等场景只能靠 `Memory` 接口在应用层粘合，没有框架原语。

同时 `Checkpointer` 按 `sessionID` 单键存储、不记 graph 身份——若用户让两个不相干 `Graph` 共享同一个 sessionID 并发跑（LangGraph 同事指出的经典坑），Save 会**静默互相覆盖**，Resume 加载到"别人"的存档后，超步快照里的 frontier NodeID 对不上当前 graph 的 node 名时会走 `node %q not found`（比 LangGraph 静默 skip 强），但 typed state / Events 仍会串台污染。

**决策方向**：拆两层，独立价值独立推进。

1. **P0 · GraphID 校验（正确性守护）**
   - `Graph` 加身份：`WithName(string)` 显式声明，否则 `sha256(sorted(nodeIDs) || stateFactoryTypeName)` 前 16 hex 位作 fingerprint 兜底
   - `Checkpoint` 新增 `GraphID string` 字段，Save 时 `cp.GraphID = g.ID()`
   - `ResumeGraph` 开头校验 `cp.GraphID != "" && cp.GraphID != g.ID()` → fail-fast
   - 解决串台的"静默错数据"问题，跟 P1 解耦

2. **P1 · ThreadID 一级字段（跨 run 会话）**
   - `RunSession.ThreadID` 已存在（历史就有，作 cp 主索引用）
   - `Checkpoint` 新增 `ThreadID` 字段
   - `Checkpointer` 接口新增 `ListByThread(ctx, threadID) ([]string, error)` 和 `LatestByThread(ctx, threadID) (*Checkpoint, error)`
   - 约定 `Memory` 的分桶 key 用 `session.ThreadID`（纯使用约定，接口不改）

### P1 落地（2026-05-07）

实施位置：
- `compose/event.go` — `Checkpoint.ThreadID string` 字段（json:"thread_id,omitempty"）
- `compose/session.go::SaveNS` — ThreadID == "" 自动设为 ID（每 sid 自成 thread，向后兼容）
- `compose/session.go::Checkpointer` — 加 `ListByThread` / `LatestByThread` 两 API
- `compose/session.go::InMemoryCheckpointer` — 实现两 API（全表扫 + 过滤的简单实现，事务后端可加二级索引优化）

**方案 E（最小破坏面）**：未改 RunSession（它已有 ThreadID 字段），未加 NewRunSessionForThread helper（直接 `&RunSession{ThreadID: ...}` 构造即可），Memory 分桶 key 是文档层面约定不动接口。

业务用法（**示例已按 R5 命名修正后字段更新**：sess 字段叫 RunID，cp.ThreadID 字段保留）：
```go
// 默认（每 sid 自成 thread）：cp.ThreadID 自动 = cp.ID
cp.SaveNS(ctx, "sid_X", "", &Checkpoint{...})  // cp.ThreadID 自动 = "sid_X"

// 跨 run 共存（业务管 cp.ID）：显式设 cp.ThreadID
sess := &RunSession{RunID: "conv_X:run_42"}  // sess.RunID = cp.ID 主键
cp.SaveNS(ctx, "conv_X:run_42", "", &Checkpoint{ThreadID: "conv_X", ...})

// 列出 thread 下所有 run
sids, _ := cp.ListByThread(ctx, "conv_X")  // ["conv_X:run_1", "conv_X:run_2", ...]
latest, _ := cp.LatestByThread(ctx, "conv_X")  // 拿最新的一个
```

测试覆盖（`compose/thread_id_test.go` 5 场景）：
- DefaultEqualsID：未设 ThreadID → 自动 = ID
- ExplicitOverride：显式设保留
- ListByThread：跨 run 列表 + 不污染异 thread + threadID 空报错
- LatestByThread：CreatedAt 最大优先 + 不存在返 (nil, nil)
- RoundTrip：端到端跨多次 SaveNS 同 thread 场景

Sanity-revert：临时删 SaveNS 自动设 ThreadID 行 → TestThreadID_DefaultEqualsID FAIL with 精确信息 "ThreadID 应默认 = ID, got """ → 恢复 → 全过。证明非 tautological。

### P1 Cross-audit Round 1 加固（commit follow-up）

audit 7 真 0 假阳（命中率 100%），其中 1 高 4 中 2 低。修补关键 4 项 + 文档加固：

- **#6（高）Memory 集成空头承诺**：commit message 和 issues.md 都说"Memory 分桶用 ThreadID 是文档约定"但 Memory 接口完全没暴露 ThreadID 给实现方。修：`agent/memory.go::Memory` 接口 godoc 加段说明业务实现"从 `compose.GetSession(ctx).ThreadID` 取分桶 key"，给完整代码示例
- **#1（中）ListByThread godoc 与实现矛盾**：godoc 写"不去重"实际去重了。修：godoc 改"自动按 sid 去重"
- **#5（中）LatestByThread (nil, nil) 反模式**：caller 漏判 err 易踩。修：加 `var ErrThreadNotFound = errors.New(...)` 哨兵，LatestByThread 不存在 thread 返 (nil, ErrThreadNotFound)；对齐 Go 惯例和 Checkpointer.Load 风格
- **#1（中）H2 契约不含 ThreadID 自动条款**：第三方 Save 实现易漏自动设。修：H2 契约段加 ThreadID 自动默认条款
- **#7（中）测试盲点**：补 3 个测试：`TestThreadID_LatestByThread_TieBreak`（CreatedAt 相同时按 (sid, ns) 字典序保稳定）/ `TestThreadID_ListByThread_DedupAcrossNS`（同 sid 多 ns 去重）/ `TestThreadID_Save_AutoFillsThreadID`（Save 旧 API 转调链 OK）
- **LatestByThread 实现加 tie-break**：CreatedAt 相同时按 (sid, ns) 字典序取小，避免每次返回不稳定

Sanity-revert：精确改 LatestByThread 删 ErrThreadNotFound 守卫 → TestThreadID_LatestByThread FAIL with "不存在 thread 应返 ErrThreadNotFound, got: <nil>" → 恢复 → 全过。

audit 假阳（无）：本次 audit 7 项全真。可能与系统 audit 性质（非边缘场景）相关——agent 越对系统理解深，命中率越高。

### P0+P1 配套补强（2026-05-07 follow-up）

ISSUE-50 P0+P1 主体落地后还配了 4 个 follow-up commit + 3 轮 cross-audit：

| commit | 主题 |
|---|---|
| `eff5eff` + `8f2503e` | BasicAgent.Resume 黑盒单测 4 场景 + cross-audit Round 1（events 序列断言 + OrchHooks 透传 + 数字校正）|
| `6b91d74` | 7 编排 Resume 测试一致性补强（`assertResumeOrchHooksAndExit` helper 统一断言 OrchHooks 透传 + ExitEvent）|
| `3289c5f` | nested_smoke 加 BasicAgent.Resume 顶层场景（10→11，覆盖 chat_entry / plan_chat_web 核心路径）|
| `1504950` + `6934a4f` | ISSUE-50 P0+P1 文档同步 8 文件 + cross-audit fixup（避免 ISSUE-71 commit 6/6 漏改 6 文件教训）|

### Cross-audit Round 3（2026-05-07，A+B+C 三路并行 audit）

A 跨 commit 一致性 audit（5 真）：
- F1（高）nested_smoke 缺 ThreadID 跨 run 端到端 → 加 scenarioThreadIDCrossRun
- F2（高）nested_smoke 缺 GraphID fail-fast 端到端 → 加 scenarioGraphIDMismatch
- F3（中）patterns/runtime 漏 ListByThread 去重契约 + 排序说明 → 文档补
- F4（中）testing-conventions vs CLAUDE.md HITL 矩阵口径冲突（GraphAgent 8 vs 9）→ 文档加口径说明（不冲突，"API 全集" vs "测试组合"两口径）
- F5（中）issues.md 缺本批 follow-up commit 留痕 → 本段补

B P0/P1 联合 corner case audit（3 真 + 4 文档 gap + 1 false-positive）：
- Bug 2（中）SaveNS 冲突返普通 error → 加 `ErrGraphIDConflict` 哨兵
- Bug 3（中）旧 cp 反序列化 ThreadID 空兼容 → runtime.md 加第三方实现警告
- Bug 1（false-positive）"嵌套子图 HITL 因 GraphID 过滤静默失效"——nested_smoke 10 个嵌套场景全过反证。原因：每层 ResumeGraph 入口用本层 g.ID() 过滤（子 graph 的 ResumeSubgraph → ResumeGraph 用 sub.ID()），audit 没考虑嵌套递归调用 ResumeGraph 各自过滤
- Gap 1-4：文档 / 示例增强（不修，作 Phase 2 候选）

C helper + nested_smoke 充分性 audit（4 真 + 2 minor）：
- F1（高）helper 缺 events 数量下界 + chunk 泄漏检测 → helper 加 `len(events) >= 1` + `Action.IsChunk == 0`
- F3（高）BasicAgent.Resume 未走 helper 违一致性 → 拆 helper 为 `assertResumeOrchHooksAndExitTopLevel`（顶层严格 ==1）+ `assertResumeOrchHooksAndExit`（嵌入 >=1+对称），BasicAgent 改走 TopLevel 版
- F4（高）nested_smoke 缺 Parallel/Routing fan-out HITL + 3 层嵌套 → Phase 2 跟进（已有专门单元测试覆盖）
- F2/F5/F6（低 minor）跳过

Round 3 修复落地：commit `b1892a9`（测试加固，nested_smoke 11 → 13）+ commit `a169136`（ErrGraphIDConflict 哨兵）+ 当前 commit（文档收尾）。

### Cross-audit Round 4（2026-05-07，专审 Round 3 fix commit）

按"修 bug 后再 audit 验证 fix 是否引入新 bug"规则，spawn 独立 agent 专审
Round 3 三个 commit。audit 报 5 真 bug + 1 反证讨论：

- F1（中）哨兵不对称：SaveNS 加 ErrGraphIDConflict 但 ResumeGraph mismatch
  路径仍 fmt.Errorf 无哨兵 → 修：ResumeGraph 也用 %w 包裹 ErrGraphIDConflict
  （两个路径本质同类设计错误，统一同一哨兵）+ 哨兵 godoc 加 Resume 路径示例
- F2（高）helper chunk 检测是死检查：RunSession.AddEvent 在 source 已过滤
  IsChunk → 检测永远不可能命中 → tautological → 修：注释说明"冗余 paranoid
  防御"（保留作 source 过滤改动时兜底），不删（避免 helper API 不稳定）
- F3（低）scenarioThreadIDCrossRun 路径覆盖弱（用 SaveNS 模拟非 RunGraph
  全链路）— 跳过，语义对（saveCheckpointOnError 走同 SaveNS 路径）
- F5（中）issues.md / commit message 虚假承诺"已隐含同 thread 跨 graph 合法"
  实际 grep 三大文档无明示 → 修：design.md §五.B 第 9 项加"P0+P1 三元组合
  法组合矩阵"明示（同 thread 跨 graph + 不同 sid 合法；同 sid 跨 graph 触发
  ErrGraphIDConflict）
- F6（高）scenarioThreadIDCrossRun 取"最新"语义未真验证：scenario 没断言
  latest.ID == "conv_X:run_3"，只 fmt.Printf；audit 实测某环境拿到 run_1
  （CreatedAt time.Now().UnixNano() 碰撞 → tie-break 字典序），与 commit
  message 自承"run_3 = latest"矛盾 → 修：显式 CreatedAt 拉开（int64(1000+i)）
  + 加 `if latest.ID != "conv_X:run_3"` 严格断言

Bug 1 false-positive 重新审视：audit 第二轮仍认为 nested_smoke 反证依赖
final.Content == finalAnswer（hardcoded model 看 RoleTool 角色就返）"没读
tool_result 内容"——但 Bug 1 描述的是"嵌套 Resume 会 fail"，graph 跑通
拿到 final 已能反证（不会 fail）。结论站得住，但 mock 设计违反"读 input
做断言"独立问题（已知历史 mock 模式，不重写）。

依赖错误字符串文案的 5 处全部改用 errors.Is(ErrGraphIDConflict)：
- compose/graph_id_test.go:143 / :176 / :252
- example/orchestration/nested_smoke/main.go（当年 ThreadID 场景断言，该场景已随 ISSUE-98 删除）

Sanity-revert 验证 F1 fix：临时把 ResumeGraph 改回 fmt.Errorf 不用哨兵 →
TestGraphID_Resume_Mismatch_Reject FAIL with "err 应满足 errors.Is(err,
ErrGraphIDConflict), got: resume: graph identity mismatch: ..."。证明
非 tautological。

Round 4 修复落地：当前 commit（4 项 finding 一并修 + 5 处 strings.Contains
改 errors.Is + design.md §五.B 加三元组矩阵）。

**对齐对照**：LangGraph OSS 有 thread_id 但不防串台、`assistant_id` 只在 Platform 层；eino 两者都没有；本方案做完 P0+P1 两者都覆盖，且把"正确性"和"功能"分层。

详见：[issue-50-cross-run-conversation-thread.md](issue-50-cross-run-conversation-thread.md)

---

## ✅ ISSUE-49 · 前端接入适配层（Agent → SSE/WebSocket 流协议）（2026-07-01 修订：改为做 AG-UI 适配包）

**优先级**：中
**状态**：✅ **已交付**——`agui/` 包（adapter/events/handler/sse 4 文件 ~2010 行 + `agui_test.go` + `example/agui/agui_smoke`），经多轮加固 audit（sessions 并发保护 / LRU 上限 / panic recover / cancelled 终态 wire 合规 / resume entry fail-fast，commit `38a0045`/`f839bf0`/`08f6576`/`47d56da`）。用法见 CLAUDE.md 架构摘要 agui 条目。

> **决策演进**（保留上下文）：**2026-07-01 决策修订——推翻 06-15 的"不做"，改为做 AG-UI 适配包**（独立小包，不侵入核心）。原因：06-15 反对"自己发明 SSE 抽象"，AG-UI 是"对齐外部标准协议 + 复用 CopilotKit 现成前端组件"，定位不同；且本地核实 AG-UI Go community SDK 与官方 TS 事件零落后、同 monorepo 同步。完整调研（生态 / 方案 / 一致性核对 / 工作量）见 [issue-49-frontend-stream-adapter.md](issue-49-frontend-stream-adapter.md) 末"2026-07-01 决策修订"段。**下方 06-15 记录保留作决策演进上下文。**

**（历史）2026-06-15 决策：不做框架级 SSE 包**。demo 层 SSE 适配（`example/orchestration/plan_chat_web/sse.go` + `hooks.go`）作为参考实现保留。

crew-go 已有完整的前端接入参考实现 `example/orchestration/plan_chat_web`（SSE 编码 + OrchHooks
桥接 + chunk 流式 + InterruptError 三按钮 UI + Resume Continue/Replan/Abort）。

**2026-06-15 决策：不抽框架级 SSE 包。** 调研本地三方源码确认：**框架级 SSE 是 serving /
platform 层的活，不是 orchestration 库的活**——LangGraph 库层（`pregel/` 只给 `stream_mode`
迭代器）、eino（只有 `schema/stream.go` StreamReader，零 SSE helper）都把 HTTP SSE 编码留给
业务应用；只有 serving 层（LangGraph Platform `langgraph_sdk/sse.py`、Vercel AI SDK
`toDataStreamResponse`）才内置 SSE 端点。crew-go 作为被集成的 Go 编排库，现状（chunk 走
`OrchHooks.OnNodeEvent` / `EventSink`，业务自接 SSE）**与 LangGraph 库层 / eino 完全一致，
不是缺口**。`plan_chat_web` 已是可直接复制的参考实现；框架强行抽 `compose.NewSSEHandler`
反而把业务的 event schema / 重连 / 鉴权选择固化进框架，得不偿失。业务要 SSE 直接照搬
`plan_chat_web` 即可。下方 LangGraph stream_mode 映射表保留作业务自实现时的参考。

参考 LangGraph `LANGGRAPH_STREAM_FRONTEND.md` 的成熟分层（五种 stream_mode、正文/中间过程节点名约定、tags 标签、重试 chunk 召回），五种 mode 可直接抄作 SSE event type，crew-go 数据源都对得上：

| mode | 数据源 |
|---|---|
| `messages`（token 级） | `model.ChatModel.Stream` → `StreamReader[*Message]` |
| `updates`（节点 delta） | `OrchHooks.OnNodeEnd` + `LastOutput` |
| `values`（State 快照） | `GraphState` 超步边界（需新增 hook） |
| `debug`（生命周期） | `OrchHooks.OnNodeStart/End` + ISSUE-46 superstep 事件 |
| `custom`（业务推送） | 节点内 `eventSink` / Annotations |

详见：[issue-49-frontend-stream-adapter.md](issue-49-frontend-stream-adapter.md)

---

## ✅ ISSUE-48 · 节点原生嵌套子图（AddSubGraphNode）+ state 共享

**优先级**：中
**状态**：✅ 已完成（实际由 ISSUE-64 落地，API 名 `AddGraphNode` 替代原计划的 `AddSubGraphNode`）

**最终落地**（ISSUE-64，2026-05-02）：
- `GraphBuilder.AddGraphNode(id NodeID, sub *Graph, opts ...SubgraphNodeOption)` 头等节点 API
- 4 个 SubgraphNodeOption：`WithSubgraphInput / WithSubgraphOutput / WithIsolatedSession / WithoutHookPropagation`
- 默认契约：input/output = parent.LastOutput；Session/OrchHooks/Checkpointer 共享父
- 嵌套 cp 物理写到 `(parentSID, childGraphNS)` ns 路径（ISSUE-70 Phase 2 协议）
- compose.WithOrchHooks / GetOrchHooks 让嵌套子图 NodeFunc 透传父 hook 实现 streaming 跨边界

**与原计划差异**：原计划"AddSubGraphNode + WithStateBinding[P, S] 父子 typed state 字段映射"。
实际落地选择**保持父子 typed state 类型独立**，通过 `WithSubgraphInput/Output adapter 函数显式映射，
更类型安全 + 避免 LangGraph TypedDict 同名字段隐式共享坑。

详见：[issue-48-native-subgraph-node.md](issue-48-native-subgraph-node.md)（设计文档）+ [graph.md §5](../description/graph.md)（落地说明）

---

## ✅ ISSUE-47 · PlanStep 显式声明数据依赖（InputRefs）

**优先级**：高
**状态**：已完成（Supervisor 范式落地：框架不猜不提纯，Planner 用 `refs` 声明每步原文依赖）

Plan 当前 `defaultExecInput` 无条件塞"上一步产出"——plan 非严格线性时污染 + token 浪费。
eino 走全量注入（赌 LLM attention），crew-go 现状是两头不靠的偏差。

**决策**：走第三条路——`PlanStep` 加 `InputRefs []int`，Planner 每次 replan 显式声明每步
数据依赖；默认不注入 executed。`NewLLMPlanner` 的 `plan` signal tool schema 扩
`depends_on` 字段、`tool_choice=required` 逼 LLM 显式判断。**破坏性改动，不兼容当前默认**。

副收益：为 PlanAgent 动态并行（同批内无依赖 step 自动并发）铺路。

详见：[issue-47-planstep-explicit-input-refs.md](issue-47-planstep-explicit-input-refs.md)

---

## ✅ ISSUE-46 · 超步内部分失败恢复（成功分支冻结 + 只重跑失败节点）

**优先级**：中
**状态**：已实现（2026-04-21），2026-04-22 合并 ISSUE-41 PR 2（并发分支 InterruptError 恢复），2026-04-24 增补 `ResumeFullSuperstep` 模式
**详情**：[issue-46-superstep-partial-failure-recovery.md](./issue-46-superstep-partial-failure-recovery.md)

### 一句话概括

对齐 Spark/Pregel/eino 的超步失败语义：原先超步内任一分支失败就整图中止、Resume 整超步重跑；现改造为 **best-effort 完成 + 成功分支结果冻结进 checkpoint（per-branch delta）+ Resume 只重跑失败/Interrupt 分支且看到与首次一致的超步入口输入**。省掉幂等成本（LLM 重复 token），解决非幂等副作用分支的复现问题；并统一承载 ISSUE-41 PR 2 的"并发分支 InterruptError Resume"需求。

### 核心改动（BranchDelta + superstepSnapshot）

- **`runBatch` 失败策略改写**（[compose/superstep.go](../../compose/superstep.go)）：业务错误与 InterruptError 都**不 cancel 兄弟**（语义对齐——兄弟跑完与首次执行一致），只有 hook 失败 / clone 失败才 cancel。超步入口快照（`origMsgLen` + `origLocalState`）捕获后仅在失败路径序列化，成功路径零开销
- **新增结构 `branchDelta` / `superstepSnapshot` / `interruptPayload`**（[compose/checkpoint.go](../../compose/checkpoint.go)）：`branchDelta` 三态（成功 / `ErrMsg` / `Interrupt`）；`superstepSnapshot` 含 `PreMsgLen` / `PreLocalState` / `Frontier` / `Branches[]`；成功分支保存 `Messages` 增量、`LocalState`、`Nexts`、`Output`；`Interrupt` 记录 `Reason` + JSON-序列化 `Data`。对齐 eino `Checkpoint.Channels` 的 per-node snapshot 思路
- **`buildBranchDelta`**：通过 `errors.As(err, &ie)` 识别 `*agent.InterruptError`，填入 `Interrupt` 字段；其他业务错填 `ErrMsg`
- **Metadata 新增 `cpKeySuperstepSnapshot`**：超步失败路径**不写** `cpKeyCurrentNode`（分流信号），走专用 `saveSuperstepCheckpoint`，避免与单节点 Interrupt 路径混淆
- **`ResumeGraph` 分流**：有 `superstepSnapshot` → `runGraphFromSuperstep`；否则走原 `runGraphFrom`
- **`runGraphFromSuperstep`**：还原 preState（Messages 截到 PreMsgLen + PreLocalState 反序列化）→ `rerunFailedBranches`（过滤 `ErrMsg != "" || Interrupt != nil`）并发重跑失败/Interrupt 分支（各自从 preState clone）→ `applyAllBranchDeltas` 按声明顺序合并所有 delta 得 finalState → 继续主循环。rerun 再次失败会更新 snap 重存 checkpoint
- **`graphLoop` 抽出**：`RunGraph` / `runGraphFrom` / `runGraphFromSuperstep` 共享同一主循环

### 关键不变量（测试覆盖）

- 失败/Interrupt 分支 rerun 时的输入 Messages 与首次执行一致（剥掉兄弟产出）
- typed `localState` rerun 看到的是超步入口快照值，而非成功兄弟写入后的值
- 成功分支在 Resume 时不再执行（计数器精确为首次值）
- Interrupt 分支与成功兄弟并存时，`Interrupt.Reason` / `Interrupt.Data`（JSON）正确往返

### 测试

[compose/superstep_resume_test.go](../../compose/superstep_resume_test.go)：`TestSuperstep_PartialFailure_Resume` / `TestSuperstep_PartialFailure_LocalStateRestored` / `TestSuperstep_PartialFailure_AllSucceedOnResume` / `TestSuperstep_PartialFailure_InterruptBranch`

### 文档化的已知限制

- ParallelAgentNode / FanOut / SendFrom / Routing 的**阻塞型扇出**未实现分支级快照，Interrupt 仍走 `checkParallelInterruptResults` 的"暂不支持"包装（与 `runBatch` 超步路径独立）
- `sendSink` 指令在失败路径丢弃，rerun 全部成功后才统一 drain（用户保证幂等）
- `InterruptError.Data` 是 `any`，JSON 往返尽力而为——用户应确保 Data 为 JSON-friendly 的 struct/map/基本类型

### 不涵盖

- 自动重试策略（`RetryConfig` 保持节点内语义）
- 跨超步回滚（超步仍是失败恢复的最小原子单位）

### 2026-04-24 增补：`ResumeFullSuperstep` 模式

补一类场景：成功分支产出依赖外部状态（时间 / 随机 / 远程），rerun 时希望所有分支看到一致新快照。`ResumeGraph` 签名加 `opts ResumeOptions`，`Mode` 取 `ResumeDirected`（默认，保持原语义）或 `ResumeFullSuperstep`（丢弃成功 delta，整个 frontier 重跑）。实现改动仅 `rerunFailedBranches` 里把"哪些分支算待 rerun"的判断开关化。覆盖测试：`TestSuperstep_FullSuperstepResume`。

---

## ✅ ISSUE-45 · PlanAgent replan-每步 + History 自适应压缩

**优先级**：中
**状态**：A.replan-每步 ✅ 已实现（2026-04-18）；B.token-based compact ✅ 已合并到 ISSUE-67 AutoCompact 完整实现
**详情**：[issue-45-plan-step-deps-and-auto-compact.md](./issue-45-plan-step-deps-and-auto-compact.md)

### 一句话概括

(A) PlanAgent 原本"规划一次 + 线性推进"偏离设计意图，对齐 eino 改造为 **replan-每步**：每执行完一步回到 Planner 重新决定剩余计划；统一 `Planner` 签名为 `(ctx, sess, original, executed) → (remaining, finalAnswer, err)`，`len(remaining)==0` 即收尾。(B) `MaxHistoryPairs` 默认 0 长对话会把 SelfHistory 撑爆 context，临时给了保底值 20，长期应按 token 而非 pair 触发自适应压缩。

### A 已实现（2026-04-18）

- [compose/plan.go](../../compose/plan.go)：`Planner` 新签名带 `executed` 入参；图结构 `plan ↔ exec` 循环；conditional edge 在 plan 节点判断 `Done` 路由 END
- [compose/llm_planner.go](../../compose/llm_planner.go)：内置 `NewLLMPlanner(model, ...)`，借 `plan` / `respond` 两个信号 tool 通过 tool_call 强制结构化输出（对齐 eino，`tool_choice=required` 禁止裸文本）
- 想退化"一次规划 + 线性"的用户：在 Planner 闭包里缓存首次结果按 executed 切片返剩余（零 LLM 成本），`example/orchestration/plan_chat/main.go` 和 `example/adv_07_plan_agent_test.go` 都用此模式
- 不再考虑 `PlanStep.DependsOn []int`：依赖由 LLM 在下一次 replan 时根据 executed 动态决定，无需前置 DAG 声明

### B 临时落地（2026-04-17）

- [agent/options.go](../../agent/options.go)：`defaultConfig().MaxHistoryPairs = 20`，truncate 保底避免 context 爆

### B 长期方向 → ✅ 已被 ISSUE-67 AutoCompact 实现（2026-05-03）

`WithAutoCompact(&AutoCompactConfig{Enabled: true, Model: m, ModelContextWindow: 200_000})`：
modelNode 入口估算 token 超阈值 → LLM 摘要 → 替代 messages。详见 [ISSUE-65~68 4 层压缩](#-issue-6568--4-层上下文压缩系统microautoreactivesessionmemory) + [context-compaction.md](../v2/context-compaction.md)。
原"`Config.MaxContextTokens` + tokenizer 估算 + `HistoryCompactor`"长期方向以 `WithAutoCompact` API 落地。

---

## ✅ ISSUE-44 · 解耦 session 审计与 LLM 输入

**优先级**：高
**状态**：已实现（2026-04-17）
**详情**：[issue-44-decouple-session-audit-from-llm-input.md](./issue-44-decouple-session-audit-from-llm-input.md)

### 一句话概括

ISSUE-42 让 runtime 把 `HistoryFor(name)` 当 LLM 输入源，跨 agent 事件被自动 replay（降级版），而编排层又在 input 里显式 relay，同一内容在 LLM 输入里出现两次。新增 `SelfHistory(name)` 专供 BasicAgent 构造 LLM 请求，`HistoryFor` 降级为审计/查询 API。

### 落地改动

- [agent/rewrite.go](../../agent/rewrite.go)：新增 `SelfHistory(ownerAgentName)`，只返回本 owner 自产事件
- [agent/react.go](../../agent/react.go)：LLM 输入源从 `HistoryFor(cfg.Name)` 切到 `SelfHistory(cfg.Name)`
- [compose/sequential.go](../../compose/sequential.go)：step 1+ 显式拼 "原始目标 + 上一步产出" 作 input
- [compose/plan.go](../../compose/plan.go)：`defaultExecInput` 改为精简默认（原始目标 + 你的任务 + 上一步产出），不再塞完整 plan/executed
- [agent/rewrite_test.go](../../agent/rewrite_test.go)：新增 3 个 `TestSelfHistory_*` case
- 文档同步 [CLAUDE.md](../../CLAUDE.md)、[doc/description/design.md](../description/design.md)、[doc/description/patterns.md](../description/patterns.md)

---

## ✅ ISSUE-43 · AgentName 归属与 HistoryFor 边界收口

**优先级**：高
**状态**：已实现（2026-04-17）—— v1 历史，文中 Memory 接口已由 PR 3.3 commit `e6505f9` 整体替代为 `store.Store`
**详情**：[issue-43-agent-identity-and-history-rewrite-edges.md](./issue-43-agent-identity-and-history-rewrite-edges.md)

### 一句话概括

ISSUE-40/41/42 三连改动后的系统审查发现 6 个边界问题：默认 Name 碰撞 / 空 owner 退化 / omitempty 丢失 / JoinLanes 重复静默 / Memory.Read 无改写 / Sequential Messages[0] 无防御。ISSUE-42 的改写机制是骨架，本 issue 补齐关节。

### 落地改动

- [agent/options.go](../../agent/options.go)：默认 Name 附带 UUID 前缀保证唯一
- [agent/rewrite.go](../../agent/rewrite.go)：空 owner/空 AgentName 一律走 cross-agent 改写；新增 `rewriteMemoryMessages`
- [agent/event.go](../../agent/event.go)：`AgentName` 去 `omitempty`，空值不再隐式等价于 owner
- [agent/session.go](../../agent/session.go)：`JoinLanes` 重复 join 直接 panic
- [agent/react.go](../../agent/react.go)：`Memory.Read` 返回的消息经 `rewriteMemoryMessages` 再注入
- [compose/sequential.go](../../compose/sequential.go)：`compile(originalInput)` 闭包捕获，step 1+ 不再读 `state.Messages[0]`
- [agent/rewrite_test.go](../../agent/rewrite_test.go) / [agent/session_test.go](../../agent/session_test.go)：新增 H1/H2/M1/M2/L1 相关 case

---

## ✅ ISSUE-42 · 共享 session 下跨 agent tool_call 配对失败

**优先级**：高
**状态**：已实现（2026-04-17）
**详情**：[issue-42-cross-agent-tool-call-rewrite.md](./issue-42-cross-agent-tool-call-rewrite.md)

### 一句话概括

ISSUE-41 共享 session 后，`reactLoop` 把 `session.History()` 整体塞给 LLM，A 的 `assistant(tool_call)+tool_result` 原样进入 B 的请求，OpenAI 422（tool_call_id 配对失败）。对齐 eino `rewriteMessage`：非本 agent 的 assistant/tool 消息降级为 `user("For context: [A] ...")`。

### 落地改动

- [agent/rewrite.go](../../agent/rewrite.go)：新增 `rewriteCrossAgentMessage` + `RunSession.HistoryFor(ownerName)`
- [agent/react.go](../../agent/react.go)：`session.History()` → `session.HistoryFor(cfg.Name)`
- [agent/rewrite_test.go](../../agent/rewrite_test.go)：5 个 case（owner 直通 / cross-agent 降级 / user&system 穿透 / 空 owner 退化 / 匿名事件保守处理）

---

## ✅ ISSUE-40 · session.events 持久化（已实现，方向 D）

**优先级**：中
**状态**：已实现（2026-04-17）
**详情**：[issue-40-session-events-persistence.md](./issue-40-session-events-persistence.md)

### 一句话概括

闭环 ISSUE-24·H3 的遗留项：Resume 后 `sess.History()` / `sess.Events()` 不再为空。

### 实际采用方案（方向 D · 借鉴 eino）

- `Checkpoint` 新增 `Events []*AgentEvent` 字段，session 事件流随 checkpoint 一起持久化
- `AgentAction.CustomizedAction any` 标注 `json:"-"`——**运行时字段，不进 checkpoint**；未来若需跨 resume 的结构化控制信号，升格为 `AgentAction` 专属字段（仿 eino 的 `TransferToAgent *TransferToAgentAction`）
- `RunSession.RestoreEvents` 直接赋值，不触发 hook——Resume 是"日志恢复"不是"事件重演"

### 为什么不是方向 B

方向 B（`ActionRecord` + 全局类型注册表）是过度设计：代码库里**没有任何地方写入 `CustomizedAction`**（唯一提及在一段注释示例里）。注册表机制是针对"动态扩展"场景的，当前没有这类需求。方向 D 改动更小、可直接闭环。

### 落地改动

- [agent/event.go](../../agent/event.go)：结构体加 JSON 标签，`Checkpoint` 加 `Events` 字段
- [agent/session.go](../../agent/session.go)：`RunSession.RestoreEvents`
- [compose/checkpoint.go](../../compose/checkpoint.go)：`marshalGraphState` 写入 `session.Events()`，`ResumeGraph` 灌回
- [compose/checkpoint_test.go](../../compose/checkpoint_test.go)：三条测试（round-trip、Resume 后 History 非空、InMemoryCheckpointer 拷贝合约）

---

## ✅ ISSUE-39 · GraphAgent.Validate() 显式 API + AddChoicer（2026-04-16）

**优先级**：低
**状态**：已实现（2026-04-17）

**备注**：Choicer 签名使用 `func(ctx, state) (NodeID, error)` 而非 spec 的 `[]NodeID`
——单路由目标语义更清晰，扇出场景仍走现有 `AddFanOutEdge` / `SendFrom`。
**来源**：对比 spec `compose.tsp:122, 127`

### 背景

1. 我方 `validateGraph` 在 `Build()` 内部跑（`compose/graph.go:319`），调用方想在不生成 Graph 的前提下探测合法性没有显式 API
2. `AddConditionalEdge` 覆盖 spec 的 `AddChoicer` 能力，但没有"命名 choicer 节点"概念，hook 层观察不到分类决策事件

### 设计

1. `GraphBuilder.Validate() error` 暴露现有校验逻辑作为幂等查询
2. `GraphBuilder.AddChoicer(name NodeID, router func(ctx, state) []NodeID) *GraphBuilder` — 生成一个命名节点，执行时发出 `route:<name>` 事件供 `OrchestrationHooks.OnNodeStart/End` 观察

### 改动清单

1. `compose/graph.go` — 加 Validate/AddChoicer
2. `compose/graph_test.go` — 覆盖 Validate 对孤儿节点、不可达 EndNode 报错；AddChoicer 事件被 hook 捕获

---

## ✅ ISSUE-38 · HandoffAgent.State() 暴露当前激活 agent 查询（2026-04-16）

**优先级**：中
**状态**：已实现（2026-04-17）
**来源**：对比 spec `compose.tsp:141`、`handoff_agent.go:13`

### 背景

HandoffAgent 的 `handoffState.Current` 只存于 typed state（`compose/handoff.go:20`），外部无法在不跑完整轮次的前提下查询"当前控制权在谁手上"，调试/可视化成本高。

### 设计

```go
// 通过 AgentEvent 已能获得，但加一个便捷方法：
func (h *HandoffAgent) State(session *agent.RunSession) (current string, hops int, exhausted bool)
```

从 session 的最近 events 或 typed state 读取；若 session 从未跑过则返回 `initial, 0, false`。

### 改动清单

1. `compose/handoff.go` — 加 `State()` 方法
2. 单测：跑一轮 handoff 后调 State 返回目标 agent

---

## ✅ ISSUE-37 · 编排层 Config struct 化（2026-04-16）

**优先级**：中
**状态**：已实现（2026-04-17）
**来源**：对比 spec `compose.tsp` 每种 agent 都有 `XxxAgentConfig{maxRetries, maxRounds}` 默认 10

### 背景

我方重试/轮次上限散落在 `WithXxxMaxRetries` / `WithXxxMaxIterations` / `WithMaxHops` 等不同命名的 option 里（见 `loop.go:43/56`、`handoff.go:42/47`、`plan.go:135/140`、`reflect.go:52/57`），字段名不统一，且无法一次性从 JSON/YAML 灌入配置。spec 统一 `{MaxRetries, MaxRounds}` 字段 + 默认 10，便于配置中心驱动。

### 设计

每种编排 Agent 新增 `XxxConfig` struct 与 `WithConfig(cfg)` 桥接，保留现有 Option：

```go
type SequentialConfig struct {
    MaxRetries int
    RetryCfg   RetryConfig // 可选精细化
}

func WithSequentialConfig(cfg SequentialConfig) SequentialOption { ... }
```

命名对齐准则：
- 循环类（Loop / Handoff / Plan / Reflect / Graph）：`MaxRetries + MaxRounds`，内部按当前字段映射（Loop 的 MaxIterations = MaxRounds；Handoff 的 MaxHops = MaxRounds）
- 非循环（Sequential / Parallel / Routing）：仅 `MaxRetries`

**我方 Reflect 默认 3 vs spec 10 的差异**：保留 3（反射场景 3 轮经验值），在 CLAUDE.md 注释差异。

### 改动清单

1. 每种 compose agent 新增 `XxxConfig` + `WithConfig`
2. 现有 option 保留
3. 单测：表驱动验证"WithConfig 覆盖等价于组合 option"

### 兼容性

- 纯增量，无 breaking change

---

## ✅ ISSUE-36 · 编排层统一 AddAgent builder API（2026-04-16）

**优先级**：高
**状态**：已实现（2026-04-17）
**来源**：对比 spec `compose.tsp:41-93, 118-193`；用户要求降低 SDK 使用门槛

### 背景

当前编排层构造姿势五花八门，SDK 使用者需要为每种 Agent 记住不同的入参形态：

| 现状 | 构造签名 |
|---|---|
| Sequential | `NewSequentialAgent([]AgentNode{{Name,Agent}...}, opts...)` |
| Parallel | `NewParallelAgent([]AgentNode{...}, opts...)` |
| Loop | `NewLoopAgent(body agent.Agent, opts...)` |
| Routing | `NewRoutingAgent(classifier, map[string]Agent, opts...)` |
| Handoff | `NewHandoffAgent(map[string]Agent, initial, opts...)` |
| Plan | `NewPlanAgent(planner, executor, opts...)` / `NewPlanAgentWithPlanner(fn, executor, opts...)` |
| Reflect | `NewReflectAgent(actor, opts...)` + `WithReflector/WithEvaluator/WithEvalFunc` |

spec 统一为 `addAgent(agent, name?, description?)` builder（见 compose.tsp 每个 interface 的 addAgent/addSubAgent）。这种风格语义上自描述、链式调用、便于增量构造，门槛最低。

### 设计

**统一 API**（全部编排 Agent 一致）：

```go
// 构造：无位置参数，只传 opts
seq := compose.NewSequentialAgent(compose.SequentialName("pipe"))
seq.AddAgent("researcher", researcher)
seq.AddAgent("writer", writer).AddAgent("critic", critic)  // 链式

par := compose.NewParallelAgent()
par.AddAgent("a", a).AddAgent("b", b)

lp := compose.NewLoopAgent(compose.WithMaxIterations(5))
lp.SetBody(body)                    // 单体 agent 专用 Set 不是 Add

routing := compose.NewRoutingAgent()
routing.SetClassifier(classifier)   // 或 .SetRouter(fn)
routing.AddAgent("math", math)
routing.AddAgent("code", code)

handoff := compose.NewHandoffAgent()
handoff.AddAgent("l1", l1).AddAgent("l2", l2)
handoff.SetInitial("l1")

plan := compose.NewPlanAgent()
plan.SetPlanner(plannerFn)          // 或 .SetPlannerAgent(a)
plan.AddSubAgent("researcher", researcher)
plan.AddSubAgent("writer", writer)
plan.SetExecutor(writer)            // fallback 执行器

reflect := compose.NewReflectAgent()
reflect.SetActor(actor)
reflect.SetReflector(reflectorFn)   // 或 .SetEvaluator(a) / .SetEvalFunc(fn)
```

**关键约定**：
- `AddAgent(name, a)` 用于**同级多 agent**集合（Sequential 节点序列、Parallel 分支、Routing 路由目标、Handoff 成员、Plan/Reflect 的 sub agent 池）
- `SetXxx(a)` 用于**单一角色**（Loop 的 body、Routing 的 classifier、Plan 的 planner/executor、Reflect 的 actor/reflector）
- 所有 AddAgent/SetXxx 返回 `*XxxAgent` 支持链式
- Options（WithMaxIterations 等）仍走 `NewXxxAgent(opts...)` 构造时传入，保持 Go 惯用法
- `AddAgent` 签名为 `AddAgent(name string, a agent.Agent) *XxxAgent`；如需 description，后续用 options variadic

### 迁移策略

**不做 breaking change**：
- 旧构造函数 `NewSequentialAgent([]AgentNode{...})` / `NewHandoffAgent(map, initial)` 等**保留**，内部转发到新的 builder 实现
- 新构造函数 `NewSequentialAgent(opts...)`（无位置参数）通过**新增 option** 引入：`WithSequentialNodes(nodes)` 作为等价入口
- 更准确：将**现有位置参数改为 functional option**，`NewSequentialAgent(nodes, opts)` 改为 `NewSequentialAgent(append(opts, WithNodes(nodes))...)`；旧签名加 deprecated 注释，两个 release 后移除

具体每种 Agent 的迁移路径（每条都要经得起"旧代码零改动继续过"的检验）：

| Agent | 新空构造 | 新方法 | 旧签名去向 |
|---|---|---|---|
| SequentialAgent | `NewSequentialAgent(opts...)` | `.AddAgent(name, a)` | 旧 `([]AgentNode, opts)` 内部改成循环调 AddAgent，加 `// Deprecated` |
| ParallelAgent | `NewParallelAgent(opts...)` | `.AddAgent(name, a)` | 同上 |
| LoopAgent | `NewLoopAgent(opts...)` | `.SetBody(a)` | 旧 `(body, opts)` 保留，内部调 SetBody |
| RoutingAgent | `NewRoutingAgent(opts...)` | `.SetClassifier(a)` / `.SetRouter(fn)` / `.AddAgent(name, a)` | 旧 `(classifier, map, opts)` 保留 |
| HandoffAgent | `NewHandoffAgent(opts...)` | `.AddAgent(name, a)` / `.SetInitial(name)` | 旧 `(map, initial, opts)` 保留 |
| PlanAgent | `NewPlanAgent(opts...)` | `.SetPlanner(fn)` / `.SetPlannerAgent(a)` / `.SetExecutor(a)` / `.AddSubAgent(name, a)` | 旧 `(plannerAgent, executor, opts)` 与 `NewPlanAgentWithPlanner` 保留 |
| ReflectAgent | `NewReflectAgent(opts...)` | `.SetActor(a)` / `.SetReflector(fn)` / `.SetEvaluator(a)` / `.SetEvalFunc(fn)` | 旧 `(actor, opts)` 保留 |

### 改动清单

1. `compose/sequential.go` / `parallel.go` / `loop.go` / `routing.go` / `handoff.go` / `plan.go` / `reflect.go` — 加 AddAgent/SetXxx 方法
2. 旧构造函数保持可用，内部改调新 API；加 `// Deprecated: 推荐使用 NewXxxAgent().AddAgent(...)` 注释
3. `example/` 下新示例优先用 builder 风格；现有旧示例不动（旧构造函数仍有效）
4. `doc/description/patterns.md` / CLAUDE.md 核心类型速查刷新推荐用法
5. 测试：为每种 Agent 新增"builder 等价于旧构造"的表驱动测试

### 校验

- `go build ./... && go vet ./...`
- 既有 `example/` 下的 13+9 个 TestAdv/TestDemo 全部继续通过
- 新增 7 个 builder 测试（每种 Agent 一个）

---

## ✅ ISSUE-35 · ReflectOpinion 结构化反馈 + 函数式 Reflector（2026-04-16）

**优先级**：高
**状态**：已实现（2026-04-17）
**来源**：对比 spec `compose.tsp:155-163`、`reflect_opinion.go`

### 背景

我方 `compose/reflect.go:269-298` 的 `isAccepted` 依赖关键字匹配（"accept"/"ok"/"good enough"）和 `EvalThreshold` 评分阈值——文本解析脆弱、不同语言 LLM 输出格式漂移时容易误判。spec 用结构化 `ReflectOpinion{opinion string, goalMatched bool}` 直接承载两路信号：是否达标 + 具体改进建议。

### 设计

新增 `Reflector` 函数类型作为 `Evaluator`/`EvalFunc` 并列的第三种评估入口，返回结构化结果并直接驱动下一轮 actor 输入：

```go
// compose/reflect.go
type ReflectOpinion struct {
    GoalMatched bool   // true 时退出循环
    Opinion     string // 非空时作为下一轮 actor 的 feedback 拼入 input
}

type Reflector func(ctx context.Context, session *agent.RunSession, output *schema.Message) (*ReflectOpinion, error)

// 选项：优先级高于 EvalFunc，高于"文本判定"
func WithReflectorFunc(fn Reflector) ReflectOption
```

评估顺序（高到低）：`Reflector` → `EvalFunc` → `Evaluator agent + 阈值 / 关键词`。前两者一旦配置就跳过后续。

### 改动清单

1. `compose/reflect.go` — 加类型、选项、执行路径；在 actor 的下一轮 input builder 里把 Opinion 作为 feedback
2. `compose/reflect_test.go` — 覆盖 Reflector 返回 {false, "改进建议"} 后 actor 读到该建议
3. CLAUDE.md 核心类型速查补 `Reflector`/`ReflectOpinion`

### 兼容性

- `EvalFunc`、文本判定路径保留
- 无 breaking change

---

## ✅ ISSUE-34 · PlanStep 补充 TmpSubAgent 动态配置（2026-04-16）

**优先级**：高
**状态**：已实现（2026-04-17）
**来源**：对比 agentdock-spec `compose.tsp:144-153`、`plan_sub_agent_config.go:3-7`

### 背景

我方 `compose/plan.go:16-19` 的 `PlanStep` 只有 `Description + SubAgentName`，规划阶段必须在预注册的 `subAgents map[string]agent.Agent` 里选一个。spec 增加 `TmpSubAgent{System, Prompt, Tools}` 作为 `SubAgentName` 之外的第二种 variant，允许 Planner **按任务动态生成一次性执行器**——典型场景：同一个 LLM 根据用户输入套不同角色/工具集，不需要提前穷举。

### 设计

`PlanStep` 新增二选一 variant；执行时若 `TmpSubAgent != nil`，用它当场构造 BasicAgent（继承 PlanAgent 的共享 Model 和 Toolkit 池），跑完即丢：

```go
// compose/plan.go
type TmpSubAgentConfig struct {
    Name        string   // 可选，默认 "tmp_<stepIdx>"
    System      string   // systemPrompt
    Prompt      string   // 可选 prefix，拼在 Description 前
    ToolNames   []string // 从 PlanAgent 共享 toolkit 里筛子集；空=不带工具
}

type PlanStep struct {
    Description  string
    SubAgentName string              // 优先级 1
    TmpSubAgent  *TmpSubAgentConfig  // 优先级 2，两者都空时用默认 executor
}
```

PlanAgent 新增 `WithPlanToolkit(tk *tool.Toolkit)` 选项，作为 `TmpSubAgentConfig.ToolNames` 的取值池。执行节点（`plan.go:214-239`）按 `SubAgentName → TmpSubAgent → executor` 三级 fallback。

### 改动清单

1. `compose/plan.go` — 加类型 + 解析逻辑 + `WithPlanToolkit`
2. `compose/plan_test.go` — 覆盖三条 fallback 路径 + 工具筛选
3. `doc/description/patterns.md` — 在 PlanAgent 节补说明
4. CLAUDE.md 核心类型速查：`PlanStep/TmpSubAgentConfig`

### 兼容性

- 现有 `PlanStep{Description, SubAgentName}` 不变，新增字段留空退化成旧行为
- 无 breaking change

---

## 合并总览（ISSUE-34~39）

| ID | 优先级 | 状态 | 核心变更 |
|----|--------|------|---------|
| ISSUE-34 | 高 | ✅ 已实现 | `PlanStep.TmpSubAgent` 动态子 agent 配置 + `WithPlanToolkit` |
| ISSUE-35 | 高 | ✅ 已实现 | `ReflectOpinion` + `Reflector` 函数式评估，替代文本解析 |
| ISSUE-36 | 高 | ✅ 已实现 | 编排层统一 `AddAgent(name, a)` / `SetXxx(a)` builder，旧构造函数仍可用 |
| ISSUE-37 | 中 | ✅ 已实现 | 每种编排 Agent 新增 `XxxConfig{MaxRetries, MaxRounds}` + `WithXxxConfig` |
| ISSUE-38 | 中 | ✅ 已实现 | `HandoffAgent.State(session)` 返回 `HandoffStateSnapshot` |
| ISSUE-39 | 低 | ✅ 已实现 | `GraphBuilder.Validate()` + `AddChoicer(name, fn)` 命名路由节点 |

---

## ✅ ISSUE-33 · 多轮对话 example 剩余 3 种模式（2026-04-16）

**优先级**：中
**状态**：✅ 已完成（2026-06）——`handoff_chat` / `reflect_chat` / `routing_chat` 三个交互式 REPL example，真 qwen 端到端验证（跨 handoff 上下文保持：L2 读出前轮型号 / 用户反馈驱动迭代："太复杂"→读历史重做简化版 / 跨轮路由感知：math→code→综合前两轮 msgs=4），`example/doc.go` 已同步
**延续**：ISSUE-32（P1+P2 已完成）

### 问题

ISSUE-32 只实现了 BasicAgent 和 PlanAgent 两种模式的多轮对话示例。HandoffAgent / ReflectAgent / RoutingAgent 在多轮语境下各有独特行为，仍需单独 demo 才能让用户感受到。

### 待实现

| 模式 | 多轮场景 | 预期目录 |
|---|---|---|
| HandoffAgent | 问题升级转交：第一轮 L1 客服回答，第二轮用户追问复杂问题 → handoff 到 L2，L2 应能读到 L1 对话上下文 | `example/orchestration/handoff_chat/` |
| ReflectAgent | 用户反馈驱动迭代：第一轮生成方案，用户"太复杂" → actor 基于 history 重新生成，reflector 评估 | `example/orchestration/reflect_chat/` |
| RoutingAgent | 跨轮路由：轮 1 路由到 math 专家，轮 2 路由到 code 专家，轮 3 用户问"结合前两轮" → 路由器需支持上下文合并 | `example/orchestration/routing_chat/` |

### 实现要点（沿用 ISSUE-32 的约定）

- 每个模式独立子目录 + `main.go`，交互式 REPL
- 复用 `traceHook` 或等价 hook 打印子 agent 内部事件
- 同步 `example/doc.go` 命令列表

---

## ✅ ISSUE-32 · example 缺少多轮对话示例（2026-04-16）

**优先级**：中
**状态**：P1+P2 已实现（2026-04-16），P3/P4/P5 迁移至 ISSUE-33 跟踪

### 问题

当前所有 example 测试均为单次调用（单输入→单输出），未展示 `RunSession` 跨轮维持上下文的能力，用户无法从示例中了解多轮对话的正确用法。

### 需要补充的模式（优先级排序）

| 优先 | 模式 | 多轮场景 | 状态 |
|---|---|---|---|
| 1 | BasicAgent | 工具调用 + 记忆：第一轮问天气，第二轮问"明天呢"，agent 需记住城市 | ✅ `example/orchestration/basic_chat/` |
| 2 | PlanAgent | 计划→用户追加步骤→调整：多轮协作完善方案；planner 把步骤派发给多个专家子 agent | ✅ `example/orchestration/plan_chat/` |
| 3 | HandoffAgent | 跨 handoff 上下文保持：问题升级转交，新 agent 能读到前序对话 | ⏳ ISSUE-33 |
| 4 | ReflectAgent | 用户参与迭代：用户反馈"太复杂"，agent 根据历史重新生成 | ⏳ ISSUE-33 |
| 5 | RoutingAgent | 跨轮路由感知：不同轮路由到不同专家，第三轮引用前两轮结果 | ⏳ ISSUE-33 |

### 实现决策

- **交互式 REPL**：用 `bufio.Scanner(os.Stdin)` 读用户输入，每轮传同一个 `sess := agent.NewRunSession()` 给 `agent.Run`，框架自动通过 `react.go:66` `append(messages, session.History()...)` 把历史拼进去，不需要调用方手动维护消息数组
- **打破"全 _test.go"约定**：在 `example/` 下新建 `basic_chat/` 和 `plan_chat/` 子目录放 `package main`，因为 `go test` 对 stdin 交互不友好。现有 `_test.go` 示例不动，共存
- **终端可感知**：每个子 agent 都挂 `traceHook`（实现 `hook.Hooks`），打印 `[llm→]` / `[tool→]` / `[tool←]` 并带 `info.Name`，用户能看清"哪个专家在干活"；plan_chat 还在 `ExecInputBuilder` 里打印 `[step N → <subAgent>]` 体现步骤派发

### 实现文件

| 文件 | 说明 |
|---|---|
| `example/orchestration/basic_chat/main.go` | 天气 bot：`get_weather(city, date?)` 假数据；多轮对话推荐脚本"北京天气→明天呢→那上海呢→第一个城市是什么"演示历史继承 |
| `example/orchestration/plan_chat/main.go` | 3 专家子 agent（researcher 带 search 工具 / writer / critic）；自定义 JSON Planner 产出带 `SubAgentName` 的 `[]PlanStep`，PlanAgent 按名称派发；多轮追加需求时 planner 基于上轮历史增量规划 |
| `example/doc.go` | 新增两行 `go run ./example/orchestration/basic_chat` / `go run ./example/orchestration/plan_chat` 运行说明 |

### 验证

- `go build ./example/orchestration/basic_chat && go build ./example/orchestration/plan_chat` — 通过
- `go vet ./example/...` — 通过
- 既有测试 `go test ./example/... -run TestDemo` / `TestAdv_` 不受影响（新增代码在子目录，不属于 `example_test` 包）

---

## ✅ ISSUE-31 · ChatModel/Tool 组件级 callback（2026-04-16）

**优先级**：低
**状态**：已实现（未按 spec 原貌，内收进 `hook.Hooks`）
**来源**：eino 架构调研（doc/description/eino-reference.md）

### 最终方案

未按原提案在 `ChatModel` / `Tool` 上新增 `WithCallback` 可选接口，而是把 model/tool 级 callback 收进 `hook.Hooks`，由 `BasicAgent.reactLoop` 在 ReAct 循环中调用。功能等价、集成路径更直接（用户只需挂一个 hook 就能覆盖 agent + model + tool 三层）。

### 已落地接口（[hook/hooks.go](../../hook/hooks.go)）

| Hook | 触发时机 | 可改字段 |
|---|---|---|
| `OnModelStart` | LLM 调用前 | `input.Messages` / `input.Tools`——runtime 据此重绑 chatModel |
| `OnModelEnd` | LLM 返回后、tool-call 解析前 | `output.Message`（替换响应）/ `output.Usage` |
| `OnModelEndStream` | 流式 LLM 返回 | 以返回值替换 stream reader（当前无调用点，待 StreamAgent） |
| `OnToolStart` | tool 调用前 | `input.Name` / `input.ArgumentsJSON` / `input.CallId` |
| `OnToolEnd` | tool 返回后 | `output.Result.*` / `output.CallId`——反映到写回 LLM 的消息 |

所有 hook 返回 `error`，非 nil 中止当前 round。`NoOpHooks` / `ChainHooks` 都实现；`agent/react.go` 在 model/tool 调用前后调用并消费入参修改（见 react.go:199-319）。

---

## ✅ ISSUE-30 · StreamAgent opt-in 流式接口（2026-04-16，决策：不做）

**优先级**：低  
**状态**：已决策不做（2026-06）  
**来源**：eino 架构调研（doc/description/eino-reference.md）

### 背景

eino 用 `Runnable[I,O]` 四模式统一流式：组件只需实现一个方法，框架自动派生其他三种（Invoke/Stream/Collect/Transform）。crew-go 当前只有 `ChatModel.Stream` 和 `StreamableTool.StreamCall`，编排层无流式传播。

### 当前结论

crew-go 图引擎使用 `*GraphState` 作为节点间数据载体，与 eino 泛型 `Runnable[I,O]` 架构不同，全量照搬四模式代价大。真正需要"编排层节点间流式传播"的场景当前不是核心需求（用户感知流式体验来自最后一个 agent 的 LLM 输出）。

**2026-06 决策：不做。** 派独立 agent 读本地 LangGraph/eino 源码 + WebSearch 共 6 框架（LangGraph / eino / OpenAI Agents SDK / Google ADK / AutoGen / LlamaIndex），**零个有独立 StreamAgent/StreamableAgent 类型**——全是「同一 agent 接口 + 流式 opt-in（flag / 兄弟方法）+ event/iterator 投递」。最该对标的 eino 用「单 `Run` + `EnableStreaming` flag + `AsyncIterator[Event]`」，crew-go 已有等价物（单 `Run` + `NewChunkEvent`(`IsChunk`) + `OrchHooks.OnNodeEvent` 实时投递 + a2a artifact 桥生产验证），且 ISSUE-63 已显式否决 chan-based 流式方案并落地。下方"解决方案"的 `StreamableAgent` 提议**保留作历史、不实施**。唯一可选小增量（非新类型、等场景驱动）：若出现"同一 agent 有时要流、有时不要流"的场景，加 `WithStreaming(bool)` RunOption 显式控制 chunk 投递即可（对齐 eino EnableStreaming / ADK StreamingMode）。

### 解决方案（等场景驱动）

```go
// 可选接口，opt-in 不破坏现有 Agent
type StreamableAgent interface {
    Agent
    Stream(ctx context.Context, session *RunSession, input *schema.Message,
           opts ...RunOption) (*schema.StreamReader[*AgentEvent], error)
}
```

编排层遇到 `StreamableAgent` 自动走流式路径，普通 Agent 用 single-chunk 包装降级。不需要实现 Collect/Transform，只需能把最后一个节点的流式输出透传给调用方。

### 影响范围

- `agent/interface.go`：新增 `StreamableAgent` 可选接口
- `compose/graph.go`：`AddAgentNode` 检测 StreamableAgent，走不同路径
- 等有具体需求再实现

---

## ✅ ISSUE-29 · Superstep 并发调度（2026-04-16）

**优先级**：中  
**状态**：已实现  
**来源**：eino 架构调研（doc/description/eino-reference.md）

### 问题

当前图执行器是严格线性的：每步只执行一个节点，不感知"本轮哪些节点已就绪可并发"。对 diamond 拓扑（A → B, A → C, B → D, C → D）或独立并行路径，无法在图调度层利用并发。FanOut 并发目前完全封装在节点内部（`ParallelAgentNode`），图引擎不可见。

### 解决方案

借鉴 Pregel Superstep 模型：

1. **GraphBuilder 允许多出边静态 FanOut**：移除 `staticCount > 1` 报错，改为允许多出边 + 记录 fanOutEdges；`resolveNextMulti` 返回 `[]NodeID`
2. **调度循环维护 frontier 集合**：`frontier := []NodeID{StartNode}`，每轮用 errgroup 并发执行 frontier 内所有节点
3. **Fan-in 等待**：维护 `inDegree[node]` 和 `completed[node]` 计数；某节点所有前驱完成后才加入下一轮 frontier
4. **State 合并**：并发节点写入独立 state 副本（JSON clone），完成后 Messages append-merge，LastOutput 取最后一个非空

### 实现方案

- `compose/superstep.go`（新增）：`filterFrontier`、`resolveNextMulti`、`superstepConfig`、`runBatch`
- `compose/graph.go`：移除 `staticCount > 1` 限制，移除 `resolveNext`，`RunGraph` 改为 frontier 调度循环
- `compose/checkpoint.go`：`RunGraph` / `runGraphFrom` 同步改为 frontier 调度
- `compose_test.go`：新增 `TestSuperstep_Diamond`（diamond 拓扑）和 `TestSuperstep_FanOutOnly`

### 设计说明

未使用预计算 inDegree；改用**运行时去重**：并发 frontier 执行完后，收集各分支 resolveNextMulti 的目标节点，
去重后即为下一轮 frontier。扇入（B,C→D）在两个分支都指向 D 时，D 在 nextFrontier 中仅出现一次，执行恰好一次。
状态合并：各分支 Messages 追加合并，LastOutput 取最后一个非 nil 分支的输出。Checkpoint 格式不变（仍存单节点 ID）。

---

## ✅ ISSUE-28 · 动态 Fan-out Send API（2026-04-16）

**优先级**：中  
**状态**：已实现  
**来源**：spec `GraphStateInterface.send`

### 问题

FanOut 只支持编译期静态声明目标，节点运行时无法根据输出内容动态决定 fan-out 数量和目标（Map-Reduce 模式）。

### 实现

新增 `compose/send.go`：
- `SendDirective{Target NodeID, Payload *schema.Message}` — 单条 fan-out 指令
- `SendFrom(ctx, directives)` — 节点函数内调用，写入当次执行的 sendSink
- `sendSink` 通过 ctx 注入（`injectSendSink`），图引擎在节点返回后 drain 并并发执行
- `executeSendDirectives` — 并发执行所有 directive，结果追加到 `state.ParallelResults`
- `cloneStateForSend` — 分支状态轻量副本，共享 eventSink

同步新增 `GraphState.Send(*agent.AgentEvent)` 方法（对齐 spec `GraphStateInterface.send`）：
- `eventSink` 非序列化字段，节点内调用 `state.Send(event)` 向图调度器发射事件
- `OrchestrationHooks` 新增 `OnNodeEvents(ctx, nodeID, events)` 方法
- `RunGraph` 每个节点完成后 drain sink 并回调 `OnNodeEvents`
- `cloneStateForFanOut` / `cloneStateForSend` 共享父路径 eventSink，分支事件汇聚到主路径

---

## ✅ ISSUE-27 · 编译期拓扑校验缺失（2026-04-16）

**优先级**：中  
**状态**：已实现  
**来源**：eino 架构调研

### 问题

`Build()` 不检查孤儿节点、EndNode 不可达、纯硬边死循环，运行时才报错，调试体验差。

### 实现

新增 `compose/validate.go`：
- `validateGraph(nodes, adj)` — 编译期静态校验入口，在 `Build()` 末尾调用
- 孤儿节点检测（无入边且不是 StartNode）
- EndNode 可达性检测（DFS）
- 纯硬边死循环检测（仅 static edge 构成的 SCC）
- 保守策略：若存在任意无 `possibleTargets` 的条件边，跳过孤儿/EndNode 检查（运行时动态路由可能覆盖）
- `AddFanOutEdge` 在捕获目标 NodeFunc 后从 `b.nodes` 删除目标节点，避免孤儿误报

---

## ✅ ISSUE-26 · 超时时未保存 checkpoint（2026-04-16）

**优先级**：中  
**状态**：已实现  
**来源**：与 spec 对比

### 问题

`RunGraph` 只在 `InterruptError` 时保存 checkpoint，ctx 超时/取消时直接丢失执行进度，无法恢复。

### 实现

提取 `saveCheckpointOnError(bgCtx, opts, state, err)` 函数，使用独立的 `context.Background()` 而非已过期的 ctx 执行保存，同时处理 `InterruptError` 和 ctx 取消/超时两种情况。`RunGraph` 和 `runGraphFrom` 均调用此函数。

---

## ✅ ISSUE-25 · 重试机制缺少指数退避（2026-04-16）

**优先级**：中  
**状态**：已实现  
**来源**：eino 架构调研

### 问题

各编排模式（ParallelAgent、SequentialAgent 等）的重试逻辑只支持固定次数 `maxRetries int`，没有退避间隔，高并发时会造成请求风暴。

### 实现

新增 `compose/retry.go`：
- `RetryConfig{MaxRetries, InitialDelay, MaxDelay, Multiplier, Jitter}` 结构体
- `SimpleRetry(n int) RetryConfig` — 向后兼容快捷构造
- `ExponentialBackoff(n, initial, max) RetryConfig` — 指数退避 + 可选 jitter
- `backoffDelay(cfg, attempt)` / `sleepWithContext(ctx, d)` 内部实现
- 所有编排模式 `maxRetries int` 字段改为 `retryCfg RetryConfig`；`WithXxxMaxRetries(n)` 保留，`WithXxxRetryConfig(cfg)` 新增

---

## ✅ ISSUE-24 · 数据流转 Bug 专项审查（2026-04-15）

**优先级**：高  
**状态**：已修复（全量）—— v1 历史档案，H4 文中 `Memory.Write` 已由 PR 3.3 commit `e6505f9` 替代为 `store.Put`
**来源**：全量 agent/ + compose/ 代码审查

### 已修复 Bug 清单

| ID | 严重度 | 文件 | 问题 | 修复方式 |
|---|---|---|---|---|
| H1 | 高 | helpers.go:205, fanout.go:159, sequential.go:82, plan.go:197/241, reflect.go:111/146/185, routing.go:412 | 并行/顺序子 agent 共享 session 导致 History() 交叉污染 | `agent.ForkSession(session)` 为每个独立调用提供隔离 session；新增 `agent.ForkSession` 函数 |
| H2 | 高 | helpers.go:50, graph.go:147 | `runAgentNode` 和 `AddAgentNode` 额外 `WithRunStep`，agent.Run 自身也加，RunPath 出现重复段 | 删除 helper/AddAgentNode 层的 WithRunStep，各 Agent.Run 自负 |
| H3 | 高 | checkpoint.go | `marshalGraphState` 未保存 ParallelResults；Resume 创建空 session，History() 为空 | ParallelResults 序列化/恢复（serializedParallelResult + cpKeyParallelResults）；session 丢失已注释说明 |
| H4 | 高 | react.go:111 | `_ = Memory.Write(...)` 吞掉错误，违反 CLAUDE.md | 改为显式处理，通过 OnError hook 上报 |
| H5 | 高 | handoff.go:118 | HandoffAgent 显式 AddEvent，BasicAgent 通过 WithSession 也写，双重记录 | 删除 handoff.go 的 AddEvent |
| L4 | 低 | session.go:InMemoryCheckpointer.Save | 存储 cp 指针，调用方修改 Metadata 会污染存档 | Save 时浅拷贝 Metadata + Messages slice |

### 已知遗留限制

- **H3（session events）**：`session.events` 不做序列化，Resume 后 `session.History()` 为空。需扩展 Checkpoint 结构体才能完整持久化，当前以 `state.Messages` 保留对话记录，留待后续迭代。
- **M2（routing fan-out typed state 隔离）**：代码注释已说明，当前 Router 路径无 StateFactory，不构成实际竞争；若未来加入需改为 cloneStateForFanOut 模式。
- **M3（compactHistory 只在 reactLoop 入口压缩一次）**：多轮 tool-call 后消息可无限增长，后续考虑在 executeRound 循环内添加动态压缩。

---

## ✅ ISSUE-23 · Checkpointer 访问方式：ctx 桥接方案（结合 spec 与现有设计）

**优先级**：高  
**状态**：已修复  
**来源**：与 spec Session 模型对比（2026-04-14），经重新评估

### 背景与权衡

- spec 方案：`checkpointer` 是 `Session` 的字段，Agent 通过 session 访问 —— 概念清晰，但违反 Go "ctx 只传不存" 的约定，且把存储能力塞进数据结构。
- 现状方案：`Checkpointer` 在 `GraphRunOptions`，Agent 不可访问 —— 分离关注点好，但 Agent 级细粒度存档无法实现。

两种都有取舍，结论：**采用 ctx 桥接的组合方案**，兼顾 spec 的"Agent 可访问" 与 Go 的 "ctx 透传、options 控制执行参数" 惯用法。

### 方案设计

三条通道分工明确：

1. **`RunSession`** 只持有身份与状态：`ID string`（UUID）+ events + values。不持有 Checkpointer。
2. **`GraphRunOptions`** 只持有执行参数：`Checkpointer` 可选字段；**移除 `CheckpointID`**，统一用 `session.ID` 作为 checkpoint id。
3. **`ctx`** 作为桥梁：新增 `agent.WithCheckpointer(ctx, cp) / agent.GetCheckpointer(ctx)` 一对函数，`RunGraph` 开头把 `opts.Checkpointer` 自动注入 ctx。

### 典型使用

```go
// 外部调用方：用法不变
opts := compose.GraphRunOptions{Checkpointer: cp}
state, err := compose.RunGraph(ctx, g, initial, opts)

// Agent 内部：需要存档时按需取
if cp := agent.GetCheckpointer(ctx); cp != nil {
    _ = cp.Save(ctx, &agent.Checkpoint{ID: session.ID, ...})
}
```

### 改动清单

1. `agent/session.go` — `RunSession` 加 `ID string` 字段，`NewRunSession()` 自动生成 UUID（合并 ISSUE-22）
2. `agent/checkpointer_ctx.go`（新增）— `WithCheckpointer(ctx, cp)` / `GetCheckpointer(ctx)`，私有 key 类型
3. `compose/graph.go` — `GraphRunOptions.CheckpointID` 删除；`RunGraph` 开头 `if opts.Checkpointer != nil { ctx = agent.WithCheckpointer(ctx, opts.Checkpointer) }`；checkpoint 保存/读取时 ID 取自 `session.ID`
4. `compose/checkpoint.go` / `ResumeGraph` — 适配 ID 来源变更
5. example / 测试同步更新

### 收益

- Agent 级细粒度存档可行（通过 ctx 拿到 cp）
- 保持 Go 惯用法（ctx 透传，不塞入数据结构）
- `session.ID` 天然作为 checkpoint id，少一个字段
- `InterruptError` + graph 执行器持久化的既有模式不变

---

## ✅ ISSUE-22 · RunSession 缺少 sessionId 字段

**优先级**：中  
**状态**：已修复  
**来源**：与 spec Session 模型对比（2026-04-14）

spec `Session` 有 `sessionId: string`，我们的 `RunSession` 没有唯一标识。  
缺失后果：无法从外部关联日志、追踪记录、问题排查；且 checkpoint 缺少天然 ID。

`RunSession` 新增 `ID string` 公开字段，`NewRunSession()` 自动生成 UUID v4（用标准库 `crypto/rand`，无外部依赖）。  
`ID` 可作为 ISSUE-23 中 checkpoint 的默认 ID。

---

## ✅ ISSUE-21 · InMemoryCheckpointer.Save 强制要求 user_id 导致 checkpoint 静默失败

**优先级**：中（测试覆盖时发现）
**状态**：已修复

`agent/session.go` 的 `Save` 方法对 `user_id` 进行了不必要的强制校验：
```go
if uid == "" {
    return fmt.Errorf("checkpoint: user_id is required")
}
```
`marshalGraphState` 不注入 `user_id`，导致图执行遇到 `InterruptError` 时 checkpoint 静默失败、无法 resume。

**修复**：移除 `user_id` 强制校验，user_id 改为可选字段，支持无用户上下文的图执行场景。

---

## ❌ ISSUE-20（已关闭）

eino 的 `graphNode` 是内部私有类型，对外暴露的是一组 `AddXxxNode(key, node)` 方法，并不存在面向用户的 `Node` 类型。  
我们与 eino 的核心差别在于：eino 使用泛型强类型图（`Graph[I,O]`），每个节点持有 `inputType`/`outputType`（`reflect.Type`），框架在 `Build()` 时做全图类型兼容性校验，`graphNode` 的复杂度是该类型系统的必然产物。  
我们统一用 `*GraphState` 传递状态，`NodeFunc` 签名固定，框架无需类型校验，内部"Node 概念"就是 `map[NodeID]NodeFunc`，已经足够。引入面向用户的 `Node` 类型没有实际收益，关闭。
---

## ✅ ISSUE-19 · Skill 选择子系统（SkillSelectionModel）

**优先级**：低  
**状态**：V1 已实现

新包 `skill/`（SkillBox 接口、DefaultSkillBox 文件系统实现、关键词匹配、SKILL.md 解析）；`agent/` 侧新增 `SkillConfig`、`WithSkills`、`WithRunSkillSelectionModel`；`reactLoop` 注入内部 `activate_skill` tool，支持 CJK + 英文关键词匹配。V2（embedding 匹配、adaptive 模式、action 执行）留待后续。

spec 04-13 commit `86ba2f8` 新增 Skill Selection 子系统：
- `SkillSelectionModel { baseUrl, apiKey, model }` — 专门用于多候选 skill 仲裁的 LLM 配置
- 挂载位置：`RuntimeConfig.skillSelectionModel`（全局默认）+ `ChatConfig.skillSelectionModel`（per-call 覆盖）
- 优先级：per-call → RuntimeConfig → env fallback
- `skill/selection.go`：多 skill 匹配时调用独立 LLM endpoint 做仲裁，返回 `{ skill_name, reason, strategy }`

我们目前没有 Skill 系统，此 issue 依赖先完成 Skill 基础设施后再实现。
设计上需参考 spec 的**优先级解析模式**（per-call → global config → env）。

---

## ✅ ISSUE-18 · RunOption 缺少 WithRunModel（model 级别 per-call 覆盖）

**优先级**：低  
**状态**：已实现

`agent/options.go` 的 `runConfig` 加 `model` 字段，`applyRunOptions` 覆盖 `cfg.Model`，新增 `WithRunModel(m model.ChatModel) RunOption`。

spec 04-13 commit `82e63a5` 将 `Agent.run` 的 `opts?: ChatOptions[]` 简化为 `config?: ChatConfig`，
支持单次 `Run` 覆盖使用的模型（`modelConfig`、`stream`）。

我们的 `RunOption` 覆盖 agent 行为层（`maxRounds`、`systemPrompt`），缺少 **model 层**的 per-call 覆盖：
当前无法在同一个 `BasicAgent` 实例上，针对某次调用切换更强的模型。

### 设计方向

```go
// 候选方案：在 RunOption 层新增 model 覆盖
func WithRunModel(m model.ChatModel) RunOption
```

`applyRunOptions` 应用时用覆盖的 model 替换 `cfg.Model`，仅对本次 `Run` 有效。

### 改动范围

- `agent/options.go` — `runConfig` 增加 `model model.ChatModel` 字段，`applyRunOptions` 应用覆盖
- `agent/options.go` — 新增 `WithRunModel(m model.ChatModel) RunOption`

---

## ✅ ISSUE-17 · 各编排模式缺少节点级重试（maxRetries）

**优先级**：低  
**状态**：已实现

`compose/helpers.go` 新增 `retryRunAgentNode` / `retryRunAgentNodeMsg`；7 种编排模式（Sequential/Loop/Routing/Handoff/Plan/Reflect/Parallel）均新增 `WithXxxMaxRetries(n)` 选项，0 退化为无重试。

spec 04-13 commit `9529140` 将统一的 `ComposeConfig` 拆为 **per-mode config**（`SequentialAgentConfig`、`LoopAgentConfig` 等），每种模式只暴露自己需要的字段：
- 无循环语义的模式（Sequential、Parallel、Routing）：只有 `maxRetries`
- 有循环语义的模式（Loop、Graph、Handoff、Plan、Reflect）：`maxRetries` + `maxRounds`

我们原有的统一 `ComposeConfig { MaxRetries, MaxRounds }` 已作为死代码删除（`compose/types.go`）。
我们的 functional options 已是 per-mode 粒度（符合 spec 新设计），但尚未实现 `maxRetries` 实际的重试逻辑。

### 设计方向

- **实现方式**：在各模式的构造 options 里加 `WithMaxRetries(n int)` functional option，**不引入统一 config struct**（Go 更惯用 functional options，spec 的 `setConfig(cfg)` builder 模式是跨语言妥协）
- **重试粒度**：`runAgentNode` 调用失败时自动重试，不区分错误类型（或后续可加 `RetryOn func(error) bool` 谓词）
- **适用范围**：所有编排模式均支持，在 `runAgentNode` / `runAgentNodeMsg` 处统一包装

### 改动范围

- `compose/helpers.go` — `runAgentNode` 增加 retry 包装，接受 `maxRetries int` 参数
- 各模式 options 文件 — 新增 `WithMaxRetries(n int)` 并传入 `runAgentNode`

---

## ✅ ISSUE-16 · Tool 接口缺少流式工具能力的表达

**优先级**：低  
**状态**：已实现

新增 `StreamableTool` 可选接口（`tool/tool.go`），框架通过 type switch 自动识别，未实现此接口的工具自动降级为单 chunk 流（`Toolkit.StreamCall` + `schema.NewSingleChunkReader`）。向后兼容。

### 问题

当前 `Tool` 接口只支持同步调用：

```go
type Tool interface {
    Name() string
    Description() string
    InputSchema() map[string]any
    Call(ctx context.Context, arguments map[string]any) (*Result, error)
}
```

如果工具本身执行耗时较长（如代码执行、文件处理、外部 API 调用），无法边执行边向用户回显进度。

### eino 的做法（参考）

eino 用**接口拆分**而非 ToolType 枚举来区分工具能力：

```go
type BaseTool interface {
    Info(ctx context.Context) (*schema.ToolInfo, error)
}

type InvokableTool interface {        // 普通同步工具
    BaseTool
    InvokableRun(ctx context.Context, argumentsInJSON string, opts ...Option) (string, error)
}

type StreamableTool interface {       // 流式工具（执行过程中实时返回输出）
    BaseTool
    StreamableRun(ctx context.Context, argumentsInJSON string, opts ...Option) (*schema.StreamReader[string], error)
}
```

框架内部用 `type switch` 判断工具类型，不需要 ToolType 字段。

### 我们的选择

- **不需要** ToolType 枚举区分"内置/外部"——我们没有内置工具概念，来源标记可在 Toolkit 注册时附加 tag，无需改接口
- **值得参考**的是流式能力的表达方式：在现有 `Tool` 接口旁**新增可选接口** `StreamableTool`，而不是在 `Call` 上加 bool 参数

```go
// StreamableTool 是可选接口，工具实现后框架自动识别并走流式路径。
// 不实现此接口的工具走普通 Call 路径，向后兼容。
type StreamableTool interface {
    Tool
    StreamCall(ctx context.Context, arguments map[string]any) (*schema.StreamReader[*Result], error)
}
```

### 前置条件

需要先评估实际业务是否有流式工具的需求，再决定是否实现。

---

## ✅ ISSUE-15 · Memory.Read 返回类型与 spec 不一致（v1 历史 — PR 3.3 后 Memory 已替代为 store.Search）

**优先级**：低  
**状态**：已修复 —— v1 历史档案，Memory 接口已由 PR 3.3 commit `e6505f9` 整体替代为 `store.Store`（`store.Search` 返回 `[]store.SearchItem`，签名与本 issue 讨论的 v1 设计已无关）

spec 定义 `MemoryInterface.read` 返回 `string[]`，我们返回 `[]*schema.Message`：

```typescript
interface MemoryInterface {
  read(query: string): string[];
  write(msg: Message[]): void;
}
```

我们的 `Memory.Read` 返回 `[]*schema.Message` 更符合 Go 生态（直接注入对话历史，不需要二次解析），spec 返回 `string[]` 可能是为了跨语言通用性。

**冲突点**：`string[]` 简单但需要调用方自己构造 `Message`；`[]*schema.Message` 直接可用但与 spec 类型不符。

**改动范围**：`agent/memory.go` — `Memory` 接口 `Read` 方法返回类型，以及 `reactLoop` 中的注入逻辑。

---

## ✅ ISSUE-14 · ParallelAgent Aggregator 签名与 spec 不一致

**优先级**：低  
**状态**：已修复

spec 最新定义 `Aggregator` 的 `results` 参数为 `Record<string>`（按名字索引的 map），我们是 `[]ParallelResult`（有序 slice）：

```typescript
op Aggregator(session: Session, results: Record<string>): string;
```

我们的 `Merger func(results []ParallelResult) *schema.Message` 用 slice 保证了声明顺序，且每个 `ParallelResult` 携带 `Name`、`Output`、`Err`，信息更丰富。

**冲突点**：map 无序，slice 有序；spec 的 `string` 值类型 vs 我们的 `*schema.Message`。解决时需权衡是否牺牲顺序保证。

**改动范围**：`compose/parallel.go` — `Merger` 类型签名。

---

## ✅ ISSUE-13 · spec 对齐变更（选择性）

**状态**：已完成选择性对齐

- `SubAgentDescriptor` 重命名为 `RoutingChoice`，保留旧名作为类型别名（`type SubAgentDescriptor = RoutingChoice`）
- 新增 `ComposeConfig{MaxRetries, MaxRounds}` 公共配置结构体
- `CommError` 重命名暂缓（需确认 common 包导出情况）

---

## ✅ ISSUE-12 · Memory 机制完全缺失（v1 历史 — PR 3.3 后整体替代为 store.Store）

**优先级**：高  
**状态**：已修复 —— v1 历史档案，本 issue 引入的 Memory 接口（`Read`/`Write` + `MemoryConfig` + `WithMemory` + `InMemoryMemory`）已由 PR 3.3 commit `e6505f9` 整体替代为 `store.Store`（`store.Search` / `store.Put` + `StoreMemoryConfig` + `WithStore` + `WithStoreMemory` + `InMemoryStore`），详见 [doc/v2/store.md](../v2/store.md)

新文件 `agent/memory.go`：`Memory` 接口（`Read`/`Write`）、`MemoryConfig`、`InMemoryMemory` 实现。  
`Config` 加 `MemoryConfig *MemoryConfig`，`WithMemory(cfg MemoryConfig)` 选项。  
`reactLoop` 开头读 Memory 注入历史，结尾写 Memory 持久化输入/输出。

---

## ❌ ISSUE-11（已关闭）

spec `compose.tsp`（commit `bba5f01`，未合并 main）中 `PlanStep` 只定义输入结构，无 `PlanResult` 返回类型。`ExecutedStep` + session 暴露已满足需求，关闭。

---

## ✅ ISSUE-10 · GraphState 架构重写：用 ProcessState[S] 替换 data map[string]any

**优先级**：高  
**状态**：已修复  
**详细设计**：见下方实现方案

### 问题

当前 `GraphState.data map[string]any` 有两个核心问题：
1. **类型不安全**：每次 `state.Get` 返回 `any`，需要断言，断言失败静默零值
2. **并发约束靠约定**：并行路径里直接写 `state.Messages` 是 race，`AppendMessageSafe` 存在但没人调用

spec 设计（`StateFactory` + `GraphChoicer`）和 eino（泛型 `ProcessState[S]`）思路一致。

### 设计决策

- **保留 `NodeFunc func(ctx, *GraphState) (*GraphState, error)` 签名不变** — 数据流本质是 `*Message`，不需要 eino 的泛型 I/O
- **保留 `GraphState.Messages`、`LastOutput`、`Session` 显式字段** — 主数据流不改
- **只替换 `data map[string]any`** — 用 ctx 注入的 typed state 代替
- **GraphState 新增 `localState any` 字段** — 图执行后编排模式可直接提取 typed state

### 实现方案

#### Phase 1: 新增 compose/state.go（纯新增）

```go
type stateKey struct{}
type internalState struct {
    state  any
    mu     sync.Mutex
    parent *internalState  // 嵌套图支持
}
type StateFactory func(ctx context.Context) any

func ProcessState[S any](ctx context.Context, fn func(ctx context.Context, state S) error) error
func GetLocalState[S any](state *GraphState) (S, bool)
func NewStateFactory[S any](init func(ctx context.Context) *S) StateFactory
func injectState(ctx context.Context, factory StateFactory) (context.Context, any)
```

#### Phase 2: GraphBuilder / Graph / GraphState 改动

- `GraphBuilder` / `Graph` 新增 `stateFactory StateFactory`
- `GraphBuilder.WithStateFactory()` 方法
- `GraphState` 新增 `localState any`
- `RunGraph` / `runGraphFrom` 开头注入 typed state 到 ctx

#### Phase 3: Checkpoint 适配

- `marshalGraphState`：优先序列化 `state.localState`，否则回退 `state.data`
- `unmarshalGraphState` 新增 `stateFactory` 参数，用 factory 创建零值后 JSON unmarshal
- `ResumeGraph` 传递 `g.stateFactory`

#### Phase 4: FanOut clone 适配

- `cloneStateForFanOut` 签名变为 `(ctx, *GraphState) → (context.Context, *GraphState)`
- typed state 通过 JSON round-trip 深拷贝，独立 mutex

#### Phase 5: 逐个迁移编排模式（从简到繁）

| 编排模式 | typed state struct | 原有 key 数 |
|---------|-------------------|-----------|
| RoutingAgent | `routingState{RouteKey string}` | 1 |
| LoopAgent | `loopState{Round int; LastAction *AgentAction; Exhausted bool}` | 3 |
| HandoffAgent | `handoffState{Active string; Hops int; Done bool; Exhausted bool}` | 4 |
| ReflectAgent | `reflectState{Round int; Accepted bool; ActorOut *Message}` | 3 |
| PlanAgent | `planState{Steps []PlanStep; Input string; Results []ExecutedStep; StepIdx int}` | 4 |
| FanOut/Parallel | `GraphState.ParallelResults []ParallelResult`（显式字段） | 2 |

#### Phase 6: 清理

- 删除 `GraphState.data map[string]any` 及 `Get/Set/Update` 方法
- 保留 `AppendMessageSafe`
- 删除旧常量、`stateInt`、`stateStringSlice`

#### Phase 7: 验证

`go build ./...` + `go test ./agent/... ./compose/...` + `go vet ./...`

### 关键文件

| 文件 | 改动 |
|------|------|
| `compose/state.go` | **新增** |
| `compose/graph.go` | GraphState/GraphBuilder/Graph/RunGraph |
| `compose/checkpoint.go` | marshal/unmarshal 适配 |
| `compose/fanout.go` | clone 策略 + ctx |
| `compose/routing.go` | routingState |
| `compose/loop.go` | loopState |
| `compose/handoff.go` | handoffState |
| `compose/reflect.go` | reflectState |
| `compose/plan.go` | planState |
| `compose/helpers.go` | 删 stateInt，ParallelAgentNode 适配 |
| `compose/compose_test.go` | 适配新 API |
| `example/example_test.go` | 用户侧 state 迁移 |

### 风险点

1. **缓存图 + stateFactory**：LoopAgent/HandoffAgent 用 `sync.Once` 缓存图，factory 无状态，每次 Run 创建新实例 — OK
2. **Checkpoint round-trip**：typed state JSON 序列化，`any` 字段可能丢失类型 — 和现在一样，不是新增风险
3. **条件边 ctx**：`EdgeCondition` 已有 `ctx` 参数，可直接 `ProcessState` — 无签名变更
4. **FanOut 隔离**：每个分支 JSON deep copy + 独立 mutex — 安全

---

## ✅ ISSUE-09 · 条件边缺 error 返回、目标无编译期校验

**优先级**：中  
**状态**：已修复

`EdgeCondition` 改为 `func(ctx context.Context, state *GraphState) (NodeID, error)`。  
`AddConditionalEdge` 新增 `possibleTargets ...NodeID` 参数，`Build()` 时校验每个 target 必须存在。  
`resolveNext` 返回 `(NodeID, error)`，`RunGraph` / `runGraphFrom` 传播错误。  
所有 8 处条件边调用已更新。

---

## ✅ ISSUE-08 · RoutingAgent 路由节点未展开为图节点

**优先级**：中  
**状态**：已修复

删除 `dispatch` 单节点，改为每个路由创建独立图节点 `route:<name>`，加条件边 `classify → route:<key>`。  
`OrchestrationHooks` 现在可以看到具体路由节点名称。  
条件边注册了 `possibleTargets`，触发编译期目标校验。

---

## ✅ ISSUE-07 · LoopAgent 退出条件机制错误

**优先级**：中  
**状态**：已修复

条件边按优先级依次检查：  
1. `AgentAction.BreakLoop`（body agent 主动信号退出当前循环层）  
2. `ExitCondition(session, round, output)`（新增 session 参数）  
3. `maxIterations` 兜底

`ExitCondition` 签名改为 `func(session *agent.RunSession, round int, output *schema.Message) bool`。

---

## ✅ ISSUE-06 · PlanAgent 中间结果格式写死

**优先级**：中  
**状态**：已修复（随 ISSUE-04 一起）

`StepResultMapper` 和 `ExecInputBuilder` 均可由调用方自定义。默认实现保留原有格式。

---

## ✅ ISSUE-05 · RoutingAgent 不支持多路由 fan-out

**优先级**：中  
**状态**：已修复

spec 明确定义 `Router` 返回 `string[]`，`choices` 为 `RoutingChoice[]`：

```typescript
model RoutingChoice { name: string; description: string; }
op Router(session: Session, input: string, choices: RoutingChoice[]): string[];

interface RoutingAgent extends Agent {
  setRouter(handler: Router): RoutingAgent;
}
```

当前实现 `classify` 节点只返回单个路由目标，不支持同时派发多个分支。`RoutingChoice` 类型我们已有，但 `Router` 返回 `string[]` 未实现。`AddFanOutEdge` 是手动方式，`RoutingAgent` 内置支持是 spec 要求。

**冲突点**：`Router` 的 Go 函数签名需要从 `func(...) string` 改为 `func(...) []string`，会影响现有调用方。

**改动范围**：`compose/routing.go` — Router 类型改为返回 `[]string`，classify 节点改为扇出边。

---

## ✅ ISSUE-04 · PlanAgent 无结构化 PlanStep

**优先级**：高  
**状态**：已修复

新增类型：`PlanStep{Description, SubAgentName}`、`ExecutedStep`、`Planner`、`StepResultMapper`、`ExecInputBuilder`。  
`execNode` 按 `PlanStep.SubAgentName` 派发到 `subAgents` map 中的专用执行器。  
`NewPlanAgent(planner, executor)` 原构造函数保持向后兼容，内部用 `PlannerFromAgent` 包装。

---

## ✅ ISSUE-03 · EvalFunc / Evaluator 缺少 session 参数

**优先级**：中  
**状态**：已修复

`EvalFunc` 改为 `func(session *agent.RunSession, output *schema.Message) bool`。  
`ReflectAgent` actorNode 中调用时传递 `state.Session`。

---

## ✅ ISSUE-02 · Checkpointer 接口不完整

**优先级**：中  
**状态**：已修复

补全 4 个方法：`Exists`、`GetLatestCheckpoint`、`Cleanup`、`GetMetadata`。  
新增 `CheckpointMetadata` 结构体，`Checkpoint` 加 `CreatedAt int64` 字段。  
`InMemoryCheckpointer` 全部实现。

---

## ✅ ISSUE-01 · Agent.Run 签名缺失 session，返回值丢失事件

**优先级**：高  
**状态**：已修复

`Agent` 接口改为 `Run(ctx, session, input) ([]*AgentEvent, error)`。
- `session` 显式传参，nil 时各实现内部自动创建
- 返回 `[]*AgentEvent` 事件增量，用 `FinalMessage(events)` 提取消息
- 所有 compose 代理（9 种）和 `AddAgentNode` 均已适配
- `runAgentNodeMsg` 帮助函数封装 message 提取，compose 节点改动最小

---
