# ISSUE-95 · Tier 1/2 扩审：graph_loop SendFrom 部分失败双发（MED）+ 2 LOW

**优先级**：MED（finding 1）+ 2× LOW（finding 2/3）
**状态**：finding 1 ✅ **已修**（2026-07-17，`90a94d2` + `7d6b913`）｜finding 3 ✅ **已修**（2026-07-17，`bca57e8`）｜finding 2 🔴 **已实证待修**（MED，两维漂移+不收敛，修法见该节，等拍板）
**来源**：核心代码块审查——整理 Tier 1/2 🔴 高风险文件清单后扩审，多 agent 单视角
并发审 + 两路独立验证（主 agent 读代码逻辑链 + 独立 agent 真跑测试）。均为
**pre-existing** bug，与触发本轮审计的 commit `9389cbf`（handoff/session/offload
3 fix，审计确认干净）无因果。

---

## finding 1（MED，CONFIRMED 已实证）✅ 已修：部分失败超步"完成兄弟自身带 SendFrom 且自身成重跑目标"→ 目标节点双发

> **修复摘要（2026-07-17）**：`90a94d2` 加 owner + purge，`7d6b913` 修 `90a94d2` 自引的
> 回归。落地与下方「修复方向」记的方案**有关键差异**——照着那段直接实现会踩多实例陷阱，
> 详见 [§ 实际修复](#实际修复2026-07-17)。

### 现象

部分失败超步里，一个**已完成兄弟**节点自身带 `SendFrom` 动态投递，且在后续 Resume
的出边计算阶段**它自己的 cond 出边又中断/失败**、使它从"完成兄弟"降级为"重跑目标"
时——它先前留在 `cm.deferredSends` 里的旧 send 没被清理，重跑时又重新产生同一个
send → **目标节点被调度两次（双发）**。

### 根因

`SendDirective` / `SerializedSend`（`compose_v2/checkpointer.go:176` 附近）**都无
owner 字段**。owner 信息唯一来源 `frontierResult.sendOwners` 是 per-超步临时字段，
`appendCompletedSiblings` 后即丢，cp round-trip 后彻底丢失 → edge 失败回填块**无从
得知**哪条旧 deferred send 属于正在重跑的节点。

机制链（`compose_v2/graph_loop.go` 内，**按符号引用不给行号**——这是 2026-07-14 诊断时的
代码形态，`90a94d2` 修复后 `res.sendOwners` 已删、回填块已重写，钉行号必然 drift）：
- `peekCompletedSiblings()` 非破坏性读——旧完成兄弟记录（含其 SendFrom）留在 `cm`
- edge 失败回填块：`keepSends` 只按 `res.sendOwners` 过滤**本超步新产**的
  `res.SendDirectives`，**不清理** peek 出来的旧 `cm.deferredSends`；
  `appendCompletedSiblings(keepIDs, keepSends)` 只 append 不 purge
- 该块注释"taken 更早的 completed 因 peek 非破坏性仍在 cm，无需回填"——正是盲区：
  旧 deferredSend 的 owner 若变成重跑成员就会残留
- `clearCompletedSiblings()` 只在出边计算**成功**时才到达；cond 失败提前 return，
  旧记录永久残留进 cp
- 下次 Resume 时旧 deferredSends 被**前置合并**重放，叠加重跑新产的 send

### 复现

> ⚠️ **下方为 2026-07-14 记录时的原始复现（旧协议，`Z==2` 系当时实测坐实）。「cond 禁止
> interrupt」重构（2026-07-16 `70e0fcb`）后此步骤**已不能照搬重建**——步骤 2/3 的「A 的
> cond 中断」「答 A 的 cond」在新协议下不成立（cond 抛 interrupt 被转非-interrupt error
> 拒绝，也没有 pending 可答）。**新触发面**：把「A 的 cond HITL 出边」换成「A 的 cond 抛
> 业务错 / ctx.Err」（走同一条 `updateFrontier([from])` → A 同样降级为重跑目标），Resume 2
> 改为 auto-load 重跑、cond 二次评估成功。**该新触发面的双发尚未重跑坐实**（逻辑链见下方
> 「前提更新」，与旧协议同构；修复前建议先按新触发面写测试实证）。

图：`split → {A, B}`；A 的 `NodeFunc` 内 `SendFrom(Z)` 且 A 有一条 cond HITL 出边（**新协议下
改为：cond 抛 transient 业务错**）；B 做 HITL 中断；`Z → End`、`X → End`。用 `atomic.Int32`
数目标节点 Z 实际执行次数。

时序（`compose_v2.Run` + `InMemoryCheckpointer` + `WithCheckpointer`）：
1. **Run 1**（超步 N，frontier=[A,B]）：A 跑成功 + 产 `SendFrom(Z)`；B 中断 → 部分失败。
   cp：`CompletedFrontier=[A]` `DeferredSends=[sendZ]` `CurrentFrontier=[B]`。A 的 cond 未评估。
2. **Resume 1**（答 B）：frontier=[B]，B 成功。出边阶段 `edgeFrontier=[B,A]`：A（作为
   完成兄弟）的 cond 中断（**新协议：cond 抛业务错**）→ A 变重跑目标（frontier=[A]），但 A 的旧 `sendZ` 仍留在
   `cm.deferredSends`（edge 失败块只过滤本超步 sends，不清 peek 出来的旧 deferred）。
   cp：`CompletedFrontier=[A B]` `DeferredSends=[sendZ]` `CurrentFrontier=[A]`。
3. **Resume 2**（答 A 的 cond；**新协议：auto-load 重跑、cond 二次评估成功**）：frontier=[A]，A 重跑 → `SendFrom(Z)` 第 2 次。旧
   `sendZ`（peek）+ 新 `sendZ`（本超步）在 deferred **前置合并**处汇合 → Z 被调度两次。

断言 `Z 执行次数 == 1`。**旧协议下实测 Z==2**（应为 1）。

> ⚠️ **前提更新（2026-07-16 `70e0fcb`「cond 禁止 interrupt」重构后）**：原文"cond 出边**能**抛
> `InterruptError` 置 `frontier=[from]`（query 带非空 InterruptID 即可，如 sub-agent 透传 /
> `routing_agent.classify`）"**已不成立**——cond 抛任何 InterruptError 现在都被转非-interrupt
> error 拒绝，且 `routing_agent` 的 classifier 已移到 classify **node** 内跑。
> **但本 finding 仍成立**：cond 抛**业务错 / ctx.Err** 仍走 `updateFrontier([from])`（cond 分支
> else 路径保留），照样能让"完成兄弟"降级为重跑目标 → 双发链路不变，**只是触发面收窄**
>（从"设计内的 cond HITL"变成"异常的 cond 业务错"）。
> `NodeFunc` **能**同时用 `SendFrom` + cond 出边（两者正交：SendFrom 走 `drainSendSink`，
> cond 走 `g.adj[from]`）仍成立。

### 触发条件（为何窄）

需同时满足：① 节点带 `SendFrom` 动态投递；② 同节点带 cond 出边**且该 cond 抛业务错 /
ctx.Err**（原"cond HITL 出边"形态已随「cond 禁止 interrupt」不可能）；③ 处于部分失败超步
（别的兄弟先中断让它先进 completed）。现实形态：GraphAgent 自定义 cond 抛 transient 业务错
+ SendFrom 的组合（原"RoutingAgent.classify cond 内嵌 sub-agent HITL"形态已消失，触发面
较记录时更窄）。

**血溅半径**：只咬 SendFrom 目标——cond/静态边 target 经 `nextSet` map 去重不受影响，
SendFrom target 按 `graph_loop.go:799` "重复 Target 不去重" by-design 逐条 append，
故恰好是 SendFrom 后继被重复调度。

### 实际修复（2026-07-17）

**commit `90a94d2`**（主修）+ **`7d6b913`**（修 `90a94d2` 自引的回归）。

落地形态：
- `SendDirective.owner NodeID` **不导出**——框架在 `graph_loop.go:1311`（`runNodeWithHooks`
  成功路径 drain sink 之后）**唯一一处**无条件盖写。fast path 与 fan-out 都经该函数、
  且所有 error 路径都在其之前 return，一处覆盖全部；业务构造 `SendDirective` 时伪造不了。
- `SerializedSend.Owner NodeID` **导出**（`json:"owner,omitempty"`，wire + conformance 需要）。
  **零值即旧行为**：重跑集合恒不含空 NodeID（Compile 期 `ValidateNodeIDForNS` 拒空），
  故 `Owner==""` 的旧 cp 恒判"非重跑成员"→ 保留，误清风险为零。
- 顺带删了 `frontierResult.sendOwners` 并列 slice（owner 进结构体字段后多余，且它在
  deferred 前置合并改写 `res.SendDirectives` 后即永久索引错位——潜伏隐患，恰好之后没人再用）。
- edge 失败块改调 `cm.mergeCompletedSiblings(ids, sends, rerun, purgePrior)`
  （`channel_manager.go:351`）：本超步新产的按 `rerun` 过滤（原 audit² M1 语义），
  **已有记录**按 `purgePrior` 清。

#### ⚠️ 陷阱：purge 集合是 `rerun \ frontier`，不是 `rerun`

上方原「修复方向」记的 `purgeCompletedRerun(rerun)`（按 rerun 清）**会引入静默丢数据**，
`90a94d2` 正是照此实现而翻车，`7d6b913` 修回：

**记录以 NodeID 为身份，执行单位却是 branch 实例**——`SendFrom` 同 Target 投 N 次时
frontier 里就有同 NodeID 的 N 份（`graph_loop.go` 的"重复 Target 不去重"是 by-design，
也是 `SendDirective` godoc 的头号示例：N 个并发 worker）。按 NodeID 清 ⟹ 把**已成功的
兄弟实例**的记录连同它的 send 一起清掉，而那个实例永远不会重跑重产（frontier 只重跑
它自己那份）→ **永久丢**。

实测（split 投 Z ×2，一实例中断一实例成功投 W，Resume 时 Z 的 cond 报错走 RerunFrom）：

| | W 执行次数 |
|---|---|
| `90a94d2`（按 rerun 清） | **1** ← 静默丢，比修复前更坏 |
| `90a94d2` 之前 | 2 ✓ |
| `7d6b913`（按 `rerun \ frontier` 清） | 2 ✓ |

判据是第一性的：**一个实例不可能既成功又在重跑集合里**。故若某重跑成员本超步**执行过**
（在 frontier 里），它的已有记录必属另一个并发实例，不能碰；只清"本超步没跑过"的
——那才铁定是它自己上一轮留的。

同理 **`appendCompletedSiblings`（node 阶段失败路径）不能加 rerun 过滤**：同一 NodeID
会**同时**进 `SucceededBranches` 和 `FailedBranches`（实测 `Current=[Z] Completed=[Z]`），
加过滤会丢兄弟实例的 send。`90a94d2` 一度给它写了"成功与失败分支天然不相交"的注释，
是**事实错误**，`7d6b913` 已改写为真实理由 + 明确"别加过滤"。

#### 回归网

- `compose_v2/send_owner_test.go` / `send_owner_internal_test.go`：双发主证（Z==1 +
  CompletedFrontier 不含 A）/ wire 键名（钉 `owner` 而非 round-trip——后者恒真：去掉
  json tag 后 Go 仍按字段名匹配）/ 旧 cp `Owner==""` 保留 / rerun 空退化 / 新产批次过滤
- `compose_v2/multi_instance_test.go`：**同 NodeID 多实例维度此前是覆盖盲区**——正是
  本轮回归漏网的原因（出事时"5 个 PA-H1 回归 + 整包全绿"照样是绿的）。两条守死上述两个陷阱。
- `checkpointer_conformance` 加 `Owner` round-trip 断言：业务 backend 漏它**不报错、
  不丢数据**，只是生产双发，是这批里最隐蔽的字段。

> 原「独立验证 agent 实现此修后 Z==1 且 5 个 PA-H1 回归 + 整包全绿」的验收记录**已被
> 证伪为假绿**：那套回归对多实例零覆盖，按 rerun 清照样全绿。

---

## finding 2（**MED，CONFIRMED 已实证 2026-07-17**——原记 LOW/PLAUSIBLE，实证后升级）：多实例部分成功后 canonical InterruptID 跨 Resume 漂移

> **实证结论（探针存 scratchpad `f2_probe_test.go`，按 `MakeInterruptID` godoc 的 canonical
> 模式写：ctx getter 取 visit/branchIdx 重算 ID）**——比原记录严重，**两个维度都漂**：
>
> ```
> Run1 中断:   visit=0 branchIdx=1 → 029d…    visit=0 branchIdx=2 → f9bd…   （用户按这俩答）
> Resume1 重算: visit=1 branchIdx=0 → 7ef6…    visit=1 branchIdx=1 → 87e4…   （重叠 0/2）
> ```
>
> - **branchIdx 维**（原记录的）：frontier 缩小后 `branchIdxByID` 按位置重排 {1,2}→{0,1}
> - **visit 维（新发现，原记录没有）**：visit 是 **per-NodeID 共享计数器**，成功的兄弟实例
>   `IncrVisitCount` 把它从 0 推到 1 → 失败实例重跑时读到被污染的值。且**同超步内**也竞态：
>   Resume2 按新 ID 供答仍不收敛（answered=1 后另一实例又漂）——先完成的实例实时 +1，后进的
>   实例算出的 ID 又变。N 个实例要 ~N 轮 Resume 才收敛完，每轮重新问用户
> - 打破的是 `MakeInterruptID` godoc 的**框架承诺**：「同节点同 visit 同 branchIdx 同 idx
>   跨 retry/Resume 总是同 ID」——多实例部分成功下该承诺结构性不成立
> - **与 finding 1 同源**（记录/身份按 NodeID、执行单位是 branch 实例），但 finding 1 能靠
>   "本超步执行过 ⟹ 属兄弟实例"绕开，**这里绕不开**——ID 本身编了 branchIdx/visit 维度

`MakeInterruptID`（`interrupt.go:602`）把 `branchIdx` 编入 ID；node 路径在业务不自填
ID 时 auto-fill 用 `branchIdxByID` 重排后的序号（`graph_loop.go:1270-1273`）。

场景：`SendFrom` 同 Target=worker ×3 → frontier=[w,w,w]，branchIdx 0/1/2。实例 0 成功、
1/2 中断，auto-fill ID 含 branchIdx=1/2。Resume 时 frontier 缩为 [w,w]（失败的），
`branchIdxByID`（`graph_loop.go:1088` 建、`:1105` 递增）重排为 0/1 → 原 branchIdx=2 的 query 答案在 Resume 端算成
branchIdx=1，ID 不匹配 → `GetResumeAnswer` miss → 二次中断 / 答案配错分支。

**与 ISSUE-86 同源**：cond 路径**已彻底禁止 interrupt**（2026-07-16 `70e0fcb`——根因不是 ID
漂移本身，而是 cond interrupt 与 PA-H1「成功成员不重跑」天然不兼容；原 D2 的"强制业务自填
InterruptID"已废止）；**node 路径此特定组合仍未覆盖**（SendFrom 同 Target 多份 + 部分中断 +
frontier 缩小 + 依赖 branchIdx 索引差异化 payload）——本 finding 不受该重构影响。

> ⚠️ **原「触发极窄」论据已被实证证伪（2026-07-17）**：原文说"重复 Target 多 worker 通常
> 同进同出：全中断→frontier 不缩；全成功→无中断；只有靠 branchIdx 索引 payload 做差异化
> 才混合结局"。**混合结局其实很容易**——`compose_v2/multi_instance_test.go` 实测：一个
> `atomic.Bool` CAS 让"抢到的那个实例中断、另一个成功"即可，**不需要任何 branchIdx 差异化
> payload**。现实里 N 个并发 worker 有一个被限流 / 超时 / 命中 HITL 是自然的。
>（此段先证伪了前置条件，后果链当时未验；顶部实证块已把后果链也坐实——canonical 重算
> 模式下 miss + 不收敛，含 visit 新维度。）

修法方向（**未实施**，等拍板）：
- 最务实：文档收窄 `MakeInterruptID` godoc 承诺——多实例 fanout 的 HITL 必须业务自填
  **domain-stable ID**（订单号等），canonical ctx 重算模式在多实例部分成功下不适用
  （`multi_instance_test.go` 的自填共享 ID 模式就没这个问题）
- 结构修需给实例引入**跨 Resume 稳定的身份**（branchIdx 按位置重排 + visit 按 NodeID
  共享，两个都不是稳定身份）——动 frontier 持久化形态，代价大，需单独设计

---

## finding 3（**MED，CONFIRMED**）✅ **已修**（2026-07-17，`bca57e8`）——原记 LOW「疑 by-design」，实证后判定**不是 by-design**：OnToolEnd 硬编码 Success:true → `gen_ai_tool_errors` metric 对业务级失败恒 0

> 注（2026-10-22）：下文的 `observability/hooks.go`、`gen_ai_tool_errors`、`MetricToolErrorTotal` 是旧观测实现的文件与指标。
> OTel 重写后工具失败记在工具 span 的 status / `error.type` 与 `gen_ai.execute_tool.duration` 的 `error.type` 维度上，
> 观测侧哨兵是 `observability/observability_test.go` 的 `TestToolFailure_ErrorType`；框架侧哨兵
> `agent_v2/tool_failure_visibility_test.go` 不变。

`agent_v2/basic_builder.go:470` 构造传给 `OnToolEnd` 的 `ToolCallResult` 时 `Success: true`
硬编码，`Error`/`ErrorCode` 字段也不填（只传 `r.Content`）。

> **为什么不是 by-design——framework 内部生产者↔消费者语义拧着（探针存 scratchpad
> `f3_probe_test.go`，四个事实全实证）**：
>
> ```
> 业务工具返回:  tool.Failure("rate_limited", "upstream 429")   ← Success=false + 结构化 Error
> OnToolEnd 收到: Success=true  Error=""  ErrorCode=""  Output="[rate_limited] upstream 429"
> ```
>
> 1. **上游有结构化声明**：`tool.Result` 自带 `Success` 字段、`tool.Failure()` 明确构造
>    `Success:false + Error{Code,Message}`——业务工具的失败声明在 hook 边界被整个抹平
> 2. **消费方按 Success 判**：`observability/hooks.go:790` `if !out.Result.Success` →
>    status="error" → `:841` fire `MetricToolErrorTotal`；设计意图注释（`:92`）明写
>    「ToolErrorTotal(Success=false)」。生产方恒 true ⟹ **该分支永远走不到**
> 3. **后果**：`gen_ai_tool_errors` 是 CLAUDE.md 承诺的 24 个 framework auto-fire metric
>    之一，实际对业务级工具失败**恒为 0**（框架级失败走 toolsFn err → OnError，也不经
>    OnToolEnd）——业务照 metric 协议建告警，工具挂了看不见
> 4. 消息流无损为真（错误文本在 Output/content 里，LLM 能看到）——原 finding 该说法保留
>
> **信息在哪一层丢的**：`makeToolsFn`（basic_agent.go:1643）有完整 `res.Error` 但只格式化
> 进 content 字符串；basic_builder 的 hook fire 层拿到的是 `[]*schema.Message`，结构化
> 信息已不在——修法需让 Success/Error 跨这两层传递（如 message Annotations 携带）。

### 实际修复（2026-07-17）

`makeToolsFn` 经**不导出**的 message annotation（`__tool_error_code__` /
`__tool_error_message__`）把 `tool.Result.Error` 的 Code/Message 跨层带给 toolsNode 的
hook fire 层；后者据此还原 `ToolCallResult{Success:false, Error, ErrorCode}`。
annotation 不导出——框架同时是生产者和消费者，业务读失败信息走 `OnToolEnd` 的类型化字段。

**顺带修好的第二个后果（实证时才发现）**：hook 对失败 tool_result 的**改写此前被静默
丢弃**。toolsNode 的回写分支是 `if !to.Result.Success && to.Result.Error != ""` —— Success
恒 true ⟹ 分支永远进不去 ⟹ 业务在 `OnToolEnd` 里做结果脱敏（改 `Result.Error`）等于白改，
违反 CLAUDE.md 写明的 `OnToolEnd` **[消费] 档**语义。

**范围判定**：
- `res.Error != nil`（业务工具 `tool.Failure`）→ 标记 ✅ 本 finding 主体
- `invalid_arguments` / `tool_not_revealed`（框架级拒绝，工具压根没跑起来）→ **也标记**：
  同样是失败的 tool call，错误码本就在 content 文本里
- `tool_denied`（审批拒绝）→ **不标记**：那是用户的主动决定、不是错误，计进错误率会污染告警

**回归**（`agent_v2/tool_failure_visibility_test.go` 5 条 + 当时的观测侧端到端 2 条，OTel 重写后换成 `TestToolFailure_ErrorType`）：
业务失败 / 成功不受影响 / 框架拒绝 / hook 不改写时 content 原样（回写不变式）/ hook 改写
真生效；再加**真端到端**——跑真 BasicAgent + `tool.Failure` → 断言 `gen_ai_tool_errors` 真 +1。

> ⚠️ **为什么这个 bug 能在 440 个绿测试里活下来**：既有的
> `observability/metric_test.go::TestPhase2Sub5c_ToolCallDurationAndError` 是**假绿**——
> 它直接调 `hooks.OnToolEnd(...)` 并**手工构造** `ToolCallResult{Success:false}`，从没经过
> BasicAgent/makeToolsFn。于是它证明了"消费方在 Success=false 时会计数"，而**生产方到底
> 会不会发出 false 从没被测过**。同 CLAUDE.md 5 类假绿炸弹 #5 —— 测试自己构造了被测链路
> 本该产出的东西，就只剩自己测自己。新增的 E2E 从 `ag.Run()` 入口跑真链路堵这个缺口。

---

## 审计过程记录

- **6 agent 单视角并发审**：abort-sid / handoff-maxHops / offload-NS / compose-并发+interrupt
  / session-race / basic-react ReAct。
- **commit `9389cbf` 3 fix**：全部审 + grep 交叉验证关键支撑事实（`WithSessionIDCtx`
  空串守卫 / `WithMaxHops` n>0 守卫默认档对齐 10 / microcompact 唯一 `.Put` 用
  `OffloadNamespace`）→ **干净、无回归**。
- **finding 1 两路独立验证**：主 agent 读代码建立逻辑链（cond 中断置 `frontier=[from]`
  + `keepSends` 只过滤本超步 sends）+ 独立验证 agent 真跑测试 `Z==2` + sanity 修复后
  `Z==1`（5 PA-H1 回归绿）。取交集确认真 bug。
- `session.go` 跑 `go test -race` + `go vet` copylocks 全绿（无 race）；`basic_agent.go`
  四方向（input dup / fanout 并发 / hook 时机 / 部分中断 Resume）均有测试兜底。
