# ISSUE-86 · graph_loop ConditionalEdge cond 路径加固

**优先级**：MED（HIGH-2 visit 漂移影响 Resume 时 InterruptID 稳定性）
**状态**：✅ 全修 (2026-05-16) — HIGH-1/3 commit 23d75cb + MED-1/3 + LOW-1/2 修；MED-2 hook 注入缺口 by design 文档化即可。
⚠️ **HIGH-2 的方案 D2 已于 2026-07-16 被「cond 禁止 interrupt」重构取代**（commit 70e0fcb，见下方「后续演进」）——D2 的代码与配套测试已移除，本文 D2 段落为历史记录。

## 后续演进：D2 被「cond 禁止 interrupt」取代 (2026-07-16, commit 70e0fcb)

**为什么 D2 不够**：D2 保留了「cond 可以 interrupt（只要自填稳定 ID）」这个能力，只解决了
ID 漂移。但引擎深审（finding F-A）发现：**任何** cond interrupt 都会让 cond 失败路径的
`updateFrontier([from])` 把 `completedFrontier` 里的成功成员写回 CurrentFrontier → 下次
Resume 重跑其 NodeFunc，违反 PA-H1「成功成员不重跑」契约（非幂等副作用双跑 + Append/Sum
reducer 双计）。**ID 稳不稳定不是根因——cond 能不能 interrupt 才是。**

**第一性判断**：cond interrupt 与 PA-H1 天然不兼容（前者必然导致 source 成员重跑，后者承诺
不重跑），二者只能留一个。crew-go 保 PA-H1（避免非幂等副作用双跑是核心优势），故**彻底
禁止 cond interrupt**。

**最终协议**（取代 D2）：
- cond fn 抛**任何** InterruptError（不管 InterruptID 填没填）→ graph_loop 一律转**非-interrupt**
  error 拒绝 + 引导改用 node。**转非-interrupt 是关键**：若让 `IsInterrupt` 仍 true 但框架已不
  `recordInterrupt`，caller 会当 HITL 等答案却无 ID 可配 → **Resume 永久死锁**。
- D2 的「合法路径 (a) sub-agent.Run 透传」不再需要特例——**RoutingAgent 的 Router/classifier
  已整体移到 classify node 内跑**（node 调 router → 写 `routingAgentTargetCh`；cond 退化纯读），
  走标准 node interrupt：nodeID+visit 协议稳定（节点失败不 IncrVisit，跨 Resume ID 不漂），
  业务 Router 因此可**直接 HITL 且不必自填 ID**（framework auto-fill 5 维稳定 ID）。
- 下方「业务迁移」示例的方向（node-level interrupt + 后接 cond 读 state）**转正为唯一路径**。

**代码位置**：`compose_v2/graph_loop.go` computeNextTasks 的 cond 分支（拒绝逻辑）；
`agent_v2/routing_builder.go` 的 classify node（路由决策）+ ConditionalEdge（纯读 targetCh）。
**测试**：`compose_v2/issue86_cond_path_internal_test.go::TestCondInterrupt_AlwaysRejected`
（表驱动 empty_id/explicit_id/nil_query 三态全拒）+
`agent_v2/routing_classifier_test.go::TestRoutingClassifier_HITL_ResumeClosedLoop`（classifier
ask_user→Resume 闭环走 node 路径）。

---

**以下为 D2 时期的历史记录**（协议已被上述重构取代，保留供追溯；其中的测试名/代码位置多已随
重构移除或改名）。

## HIGH-2 彻底修法 (方案 D2, 2026-05-16)

**根因**: 5 维 InterruptID 算法含 `visit` 维度. cond fn 在节点 success 后跑 (节点已 IncrVisit), Resume 重跑节点再次 IncrVisit → visit 漂移 → ID 不稳定.

**之前缓解 (visit-1)**: cond 路径用 `cm.GetVisitCount(from) - 1` 算 ID. 单 Run 内稳定, **跨 Resume 仍漂** (节点 IncrVisit 累加 → visit-1 也变).

**方案 A (节点 IncrVisit 推迟)**: 改 graph_loop 核心 visit 协议. 影响 maxVisit / LoopBody 计数协议 — **风险高**.

**方案 B (godoc 警告)**: 0 代码改, 业务自觉. 只是治标.

**方案 D2 (彻底, 学 LangGraph + eino)**:
- LangGraph: `interrupt()` 函数依赖 ctx `CONFIG_KEY_SCRATCHPAD` — cond eval 时 ctx 没此 key, 业务**不能**在 cond 内调 interrupt
- eino: `branch.invoke` 抛 error 直接 fail, 没 InterruptError 特殊路径
- **crew-go D2**: cond fn 抛 InterruptError 时强制要求 `InterruptID` 非空, 空 → **return error fail-fast** (audit follow-up da90ffc 从 panic 改 error — panic 在 computeNextTasks 不被 Run defer recover 冒泡 caller crash + 跳过 cp save)

**合法路径** (D2 允许):
- (a) **sub-agent.Run 透传** (e.g. RoutingAgent.classifier 内 BasicAgent + ask_user) — sub-agent 内 ask_user 已算稳定 InterruptID, cond fn return err 透传 ID 不漂
- (b) **业务自填 stable ID** — cond fn 自构造 `&AskQuery{InterruptID: "my_workflow_step_1"}` 用 caller-stable identifier

**非法路径** (D2 return error fail-fast):
- cond fn 自构造 `&AskQuery{Question: "..."}` 不填 InterruptID — 框架不再 auto-fill (visit 漂)

**业务迁移**: cond-only HITL 推荐改用 **node-level interrupt + 后接 cond 读 sess state**:
```go
// 旧 (D2 panic):
graph.AddConditionalEdge("X", func(ctx, chs) (NodeID, error) {
    return "", &InterruptError{Query: &AskQuery{Question: "approve?"}}  // ❌ 无 ID panic
})

// 新 (推荐):
graph.AddNode("approval_gate", func(ctx, chs) {
    // node 内调 ask_user (有稳定 ID + 二阶段 resume)
    return askUser(ctx, "approve?")
})
graph.AddConditionalEdge("approval_gate", func(ctx, chs) (NodeID, error) {
    // cond 读 sess state 决定路由
    return chooseTarget(getSessState(ctx)), nil
})
```

**测试覆盖**:
- `TestISSUE86_HIGH2_CondMustHaveInterruptID` — 验空 ID return error fail-fast
- `TestISSUE86_HIGH2_CondWithExplicitIDOK` — 验自填 ID 合法路径
- `TestRoutingAgent_ISSUE86_HIGH2_RouterInterruptMustHaveID` — Router fn 同款
- `TestRoutingAgent_ISSUE86_HIGH2_RouterWithExplicitID` — Router 自填 ID 合法
**触发**：R25 audit 修了 cond 路径"自动补 ID"但 cond 路径相对 NodeFunc 路径
仍有 5 处不对称, R32+ 视角 3 系统审

## 已修 (commit 23d75cb)
- HIGH-1: cond fn panic 不被 recover → safeCondRun helper
- HIGH-3: cond 返未注册 NodeID target 不校验 → 加 g.nodes[t] 存在性校验

## 待修 finding

### HIGH-2: cond fn 抛 InterruptError 时 visit 维度漂移 → Resume 后 InterruptID 不稳定

**位置**: `compose_v2/graph_loop.go:566` (cond 路径) vs `:763` (NodeFunc 路径)

**问题**:
- 节点路径: visit failure 时不 IncrVisit, success 时 IncrVisit, Resume 重跑节点
  visit 仍是原值 → InterruptID 稳定
- cond 路径: cond 在节点 success 后跑 (IncrVisit 已 +1), cond 抛 Interrupt → cp
  save → Resume 重跑 from 节点 (visit 取已 +1 的值再 IncrVisit) → cond 再跑
  (取再 +1 的 visit) → ID 不稳

**HITL 链断**: caller WithResumeAnswers map 配的 ID (visit=1) vs 框架第二次算的
ID (visit=2) 不匹配 → answer 丢失 → cond 再次抛中断 (永久 deadlock 或 silent
走 fallback)。

**修法 (待评估, 风险高 - 影响所有 cond 路径业务)**:
- 选项 A (推荐): 节点 IncrVisit 推迟到 cond 也跑完才递增 — 节点+cond 看作一个
  logical step, 对齐"一次完整 step"语义
- 选项 B: cond 路径用 `visit := cm.GetVisitCount(from) - 1` 模拟节点成功前的 visit
  (脆弱, 不推荐)
- 选项 C: 引入"cond visit 维度"独立计数 — 加新维度协议复杂

### MED-1: cond fn 抛 ctx.Err 不走 Interrupt 路径 → 半挂起态

**位置**: `compose_v2/graph_loop.go:557-577`

cond 内调 `subAgent.Run(ctx, ...)` ctx deadline → 返 `context.DeadlineExceeded`
→ 不进 Interrupt 分支 → frontier 仍是 from + cm.PendingInterrupts 空 → caller
拿 ctx.Err 判需 Resume 但**没有 InterruptID 配答案** → Resume 重 cond 又
deadline (业务大概率改 ctx 才能续) → 无法判别"是否需要 user 输入"。

**修法**: 文档化 cond 抛 ctx.Err 的语义; 或对 ctx.Err / errors.Is(err, context.Canceled)
也走类似 InterruptError 的"保 frontier=[from]"路径 (明确不需要 PendingInterrupt
— infrastructure cancel 不是 HITL)。

### MED-2: cond fn 路径不调 OnNodeStart/OnNodeEnd hook → cond 内调 sub-agent observability 缺口

cond fn 完全在 graph_loop 主循环外执行 (computeNextTasks 内), 不走 runNodeWithHooks:
- OnNodeStart / OnNodeEnd 不触发 (by design, cond 是路由判断不是节点)
- ctx 内**无 EventSink** → cond 内 `GetEventSink(ctx)` 返 nil → 业务想 emit
  progress event 走不通

**修法**: 文档化"cond 内调 sub-agent 的 hook 透传是子 agent 自己的 RunOption/
ctx hooks 责任", 或考虑 cond 调用前后 emit 一对 framework-level event (
"on_cond_start/end")。

### MED-3: cond 抛业务非-Interrupt 错时 frontier 仍是 from, 但 from 已 IncrVisit → Resume 重跑 visit 漂移

**位置**: `compose_v2/graph_loop.go:577`

非 Interrupt err 时不调 updateFrontier。cm.PendingInterrupts 空 + frontier
仍是 [from] (节点已成功设的) → defer SaveNS(rootCp) → rootCp.CurrentFrontier=[from].

Resume 重跑 from → from 已成功跑 1 次 → Resume 又 IncrVisit → 副作用节点
(API call / DB write) 双跑。cond 内业务 retry 后续断路器 / 计数 / billing 受影响。

**修法**: cond 路径返 err 前 `cm.updateFrontier([]NodeID{from})` (与 InterruptError
路径一致), 配合 HIGH-2 的 visit 推迟方案让 from 重跑时 visit 不增。

### LOW-1: MakeInterruptID 在 cond 路径 branchIdx 永远写死 0

**位置**: `compose_v2/graph_loop.go:570`

cond 路径**没有 branchIdx 参数** — computeNextTasks 在主循环每超步算一次, 不区
分 fan-out 分支。fan-out 多实例同 NodeID 多次进入 cond 时 branchIdx 全 0, ID 撞。

实际不太能触发 (cond 抛 Interrupt 立即 return, 不会循环到下一个 from), 但跨
Resume 多次进入同 cond 时 branchIdx 维度信息漂移。

**修法**: 优先级低 (cond 多实例 fan-out 是边缘场景), 文档化 `cond 在 fan-out
多实例下 InterruptID branchIdx 维度不可用` 限制。

### LOW-2: cond 路径无回归测试 (R25 fix 没配 sanity-revert 测试)

R25 audit HIGH-3 修了 cond 自动补 ID, 但没新增测试固化此行为。任何后续重构
cond 路径会 silent 破坏 R25 fix, 无 CI 守护。

**修法**: 加 `TestR25_CondAutoFillsInterruptID` 测试覆盖 cond 抛不带 ID 的
Interrupt → 框架自动算稳定 ID + recordInterrupt → cm.PendingInterrupts 含此
ID → WithResumeAnswers 配同 ID 能 Resume。

## 核心模式

cond fn 路径相对 NodeFunc 路径有 5 处不对称: (1) 无 panic recovery; (2) visit
维度无失败/成功对称; (3) 无 target 校验; (4) 无 ctx.Err 特殊处理; (5) 无 hook
钩子注入。HIGH-1 / HIGH-3 已修, HIGH-2 + 3 MED 待评估实施风险后修。

**长期方案**: 抽 `safeCondRun(ctx, cond, chs, cm, from, hooks)` helper 让 cond
路径与 NodeFunc 路径享同等保护 (panic recovery + visit 协议 + target 校验 +
ctx.Err 处理 + 可选 hook emit)。当前 commit 23d75cb 已加 safeCondRun 但只覆盖
panic recovery, 完整保护待 HIGH-2 visit 协议设计后整合。

## audit follow-up (2026-05-16)

R32+ 视角 3 修完后再 audit, 揭露 2 真 HIGH:

### audit HIGH-1: visit-1 缓解长期局限文档化

视角 C 揭露 — `visit := cm.GetVisitCount(from) - 1` 在**单 Run 内稳定**但**跨 Resume 仍漂**:
- 单 Run: 节点 success → IncrVisit (visit=1) → cond 用 visit-1=0 算 ID → 稳定
- 跨 Resume: 节点重跑 → IncrVisit 再 +1 (visit=2) → cond 用 visit-1=1 算 ID → ID 不同

**完整修法**: 节点 IncrVisit 推迟到 cond 也跑完才递增 — 影响所有 cond 业务测试矩阵大, 留长期评估。当前 visit-1 在**业务非 Resume 场景**已修, godoc 标 跨 Resume 局限。

### audit HIGH-2: ISSUE-86 测试假绿 (5 类炸弹 #2: 早退兜底盖过 fix 路径)

视角 C 揭露 — `TestISSUE86_MED1_CondCtxErrPreservesFrontier` / `TestISSUE86_MED3_CondNonInterruptErrPreservesFrontier` 两测试用**单节点拓扑** (`Start → a`), cond 抛 ctx.Err / 业务错时 frontier 一直是 [a] 不收缩 — fix 加的 `updateFrontier([]NodeID{from})` 路径**无可观测影响**, sanity-revert 仍过 (假绿).

**修法**: 改多节点 fan-out 拓扑 (`Start → split → fan-out [a, b, c]`), 让 cond 抛错时 frontier 真收缩到 [from], 防止"如果 fix revert frontier 一直 [a, b, c]" 路径触发真断言。

## 关联

- R25 audit HIGH-3: cond 抛 Interrupt 自动补 ID + recordInterrupt
- R32+ 视角 3 audit: cond 路径 5 处不对称完整 finding
- R32+ audit follow-up: visit-1 跨 Resume 局限文档化 + 测试假绿拆除 (5 类炸弹 #2)
