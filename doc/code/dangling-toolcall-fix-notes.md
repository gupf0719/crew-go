# 悬空 tool_call 修复 —— 实施前的交互面与风险清单

> 2026-07-31 立。**动手前逐条对照**，别只看方案。
>
> 背景与方案定稿见 [`todo.md`](todo.md) 里「审批挂起被放弃后模型记录被抹掉」那条；
> 三方对照见 [`three-way-comparison/checkpoint-storage.md`](../description/three-way-comparison/checkpoint-storage.md)。
>
> 立这份笔记的原因：这轮反复出现「挖到一个结论就动手 → 撞上既有机制 → 推翻重来」。
> 前几次翻车**没有一次败在方案本身，全败在跟既有机制的交互上**。所以实施前先把
> 交互面穷尽一遍。

## 进度

- ✅ **阶段 0** `3fb2c65`：`AskQuery` 加 `ToolCallID`/`ToolName`；`approval.go` 填三个字段
  （含修掉 `Reason` 的单/多路径分叉）；conformance 加断言（一条填、一条留空验空值
  round-trip），用故意剥列的 backend 验过能红。
- ✅ **阶段 1** `27b6240`：抽出 `pairToolResults` / `danglingToolCalls` 两处共用；8 条判据
  用例；承重哨兵（判据改严 → 精确报红）验过。**行为零变化**。
  - 顺带坐实阶段 4 那条缺陷的**确切机制**：`anyReal==false` 时走
    `schema.AssistantMessage(m.Content)`——造纯文本 assistant、**把 tool_calls 整批剥掉**。
    所以带不带文本只决定 assistant 本身留不留，tool_calls 一样丢。已由
    `TestDropDangling_PureToolCallRoundKeepsTheRecord` 钉住（阶段 4 修完后已翻成正例）。
- ✅ **阶段 4**（独立缺陷，`anyReal==false ⟹ 整条丢`）：**修法跟本笔记原先写的不一样，
  是实测推翻的**。原方案「保留 assistant + 补占位，占位文案区分整轮没落地 / 个别没结果」
  ——给该分支装计数器跑全量测试，**31 次命中里 22 次（71%）是 `transfer_to_agent`**，
  return-direct 控制流工具的结果**本就不进 events**、调用真的生效了。对它们补
  「未完成、如需该结果请重新调用」既是撒谎，又会引出转交环。业务工具也能声明
  return-direct（`tool.ReturnDirectMetaKey` 是公开协议），静态认不出来。
  补占位还牵动两处既有机制：结构化输出的重试预算按 assistant 上的 final_output tool_call 计数，
  跨 Run 重建的 `final_output` 会平白烧掉重试预算；合成 tool_result 还要在 R5 那 5 处
  marker 短路点各加防护，漏一处即静默。
  **落地形态**：保留 assistant、**不补占位**，把「提过什么」写成文本记录
  （`describeUnresolvedCalls`）、tool_call 剥掉。记录留下了，又不对「执行了没有」下结论，
  也不发重试指令。有文本的那档同样追加记录——「保文本、剥 tool_call」原本也把动作记录丢了。
- 🟡 **阶段 2**（事件时补，2026-10-28 按九家调研定形）：只在挂起被作废的那一刻补。显式 `ResumeActionAbort` ✅；agui 同一个 thread 来新 run 覆盖时 ✅（`agent_v2.WithAbandonedTasks`，新 run 的入口补，2026-10-29）；淘汰 / 过期回收 / 句柄丢失三处仍只删存档（别的 thread，agui 手上没有它的 agent 与 ctx）。第一版（handler 里另建 agent 走 Abort）审计实测带进回归、已撤回，见 todo.md 第 263 行附近。`NextTask()` 之后旧任务仍可续，不补（补了之后续跑真结果被当成多余的）。两条路都先删存档、删成功才补（2026-10-29 审计实测：先补后删时删档失败或预算到点，任务还能续、续跑的真结果排在「被放弃」之后）。
- ❌ **阶段 3** 不做（2026-10-28）：按会话扫要靠单线程、同时只有一个活回合（做这件事的那家的前提），我们并发 fanout、可多任务并存，只能按被作废任务的作用域补。
- ⬜ **阶段 5**（测试）随各阶段补；文案按种类分档（审批闸挂起的写「未执行」、其余写「结果未知」）是可选项

## 一句话方案

双层：**事件时补**（放弃路径上就地 append 合成 tool_result）+ **写边界幂等修复**
（在「上一轮已定局」的写边界扫历史补未配对的 tool_call，写穿持久化）。
配套：`AskQuery` 加 tool_call ID 字段；单独修「纯 tool_call 轮被整条丢」。

## 三条最容易翻车的（先读这个）

### ① 「事件时补」不能挂在中断抛出那一刻——有测试写死了反向约束

`agent_v2/history_resume_test.go:70` 断言中断当下历史**只有 2 条**（用户输入 +
声明了工具调用的 assistant），注释给的理由是：**工具轮整批作废时不该有结果落库，
否则 Resume 重跑后同一条会进两遍**。

这条约束是结构性的绕不开：Resume 会重跑**整批**工具，真结果会再写一遍；而配对是
「一果一用、先到的赢」（`basic_agent.go:1123` 队列 + `:1166` `nextResult`）⟹
合成结果先到就把真结果吞了。

⟹ **落点只能是「确定不会再 Resume」的写边界**（preamble / `settleHistory` 那 11 个点），
不能是 `InterruptError` 抛出处。**那条测试是对的，别去改它。**

### ② memo 里可能躺着「真的执行了」的证据，而修复看不见它

`WithApprovalHeuristic` 的设计意图就是「安全的放行、危险的问」，所以**一轮里既有
已执行又有待审批是主场景**，不是边角。时序：

```
tools 节点 fanout：
  tc_A → 启发式 Allow → 真执行 → rememberDone(A) → SaveNodeState 立即落 cm
  tc_B → ApprovalAsk  → InterruptError
  ⟹ 整节点失败 ⟹ recordHistory(results) 不执行（basic_builder.go:554 未到达）
  ⟹ cp 里 NodeStates 有 A 的真结果；历史里 0 条 tool_result
用户放弃、发新任务：
  顶层 Run 恒 WithFreshRun ⟹ 不载入那份 cp
  修复只看历史 ⟹ 判定 A 也「未配对」⟹ 给 A 补「未执行」
```

**A 真的执行了、副作用真的发生了，而合成文案在撒谎**；随后那份 cp 被新任务单槽
覆盖，证据永久消失。用户问「刚才那个到底转了没」时模型据此**确信地答错**。

处置三选一：(a) 文案不断言执行与否；(b) **只对 `AskQuery.ToolCallID` 指名的那个 tc
用 abandon 文案，其余走现有 `unfinishedToolResultText``**；(c) 修复前读挂起 cp 的
`NodeStates`——成本最高，`GetNodeState` 需要图内 ctx 访问器，**图外拿不到**。
**推荐 (b)，那正是新字段的价值。**

> 2026-10-14 核：最终落的是 (a)——`closeUnansweredToolCallsOnAbort` 扫会话历史、补一条不断言执行与否的中性文案；
> `AskQuery.ToolCallID` 框架没有读点（留给调用方 / 前端定位）。同批已经执行成功的兄弟调用也被补成「没有结果」，
> 是 (a) 选定的代价。

### ③ 审批中断的身份结构上反查不出 tool_call ID

审批问题的 InterruptID 是位置各维（含 tc.ID 与批内下标）加相位名算出的**哈希**（`compose_v2.InterruptSite`；
审批问题在自己的相位里取序号，相位名带工具名、tc.ID 与下标），反查不出是哪一个 tool_call。
（2026-10-14 改：这里原先写「用的是下标不是 tc.ID、tc.ID 只进了 NS hash」，已过时——tc.ID 后来进了哈希；
「反查不出」的结论不变，理由换成它是哈希。）

⟹ 加 `AskQuery.ToolCallID` **不是图省事，是唯一能把挂起和动作关联起来的办法**，
也是 ② 那条分档处置的前提。

## 一个过时认知的纠正

`dropDanglingToolCalls`（`agent_v2/basic_agent.go`，搜 `func dropDanglingToolCalls`）**已被重写**成「按声明重建配对块 +
缺的补占位」。三条推论变了：

- `unfinishedToolResultText` 占位机制**已存在**，不用新造；
- **「必须紧跟」是出口保证不是入口要求**——入口只要求「结果出现在声明之后」
  （`:1121-1124`），紧跟由块重建（`:1155-1174`）产生。⟹ **末尾 append 的合成结果
  会被正确认领**，`historyStore` 的 append-only 约束不构成障碍；
- ~~`anyReal` 分支是**唯一**还会丢消息的路径~~ —— **阶段 4 已修**：现在保留 assistant +
  写中性记账文本，不再丢消息。

## 风险清单（按「会不会静默出错」排序）

| # | 风险 | 触发点 | 防 | 测 |
|---|---|---|---|---|
| **R1** | **修复扫描不限归属 ⟹ 并发期误补正在跑的 tool_call**。父 tools fanout 中子 agent 的 preamble 写边界看到父的在途 tool_call（同 writer 同 ConvID），补「未执行」；父的真结果随后落库 ⟹ 同 id 两条、只认第一条 ⟹ **真结果被永久吞掉** | 子 preamble（`basic_agent.go:951`）读到父 model 节点（`basic_builder.go:425`）写的 assistant | 只处理 `HistoryAgentAnnotation == historyOwner(ctx, name)` 的消息（判据抄 `history.go:772`）；godoc 写死「只能从 preamble / settleHistory 调，禁止读路径」 | `AgentAsTool` 子 agent + 慢工具兄弟，**真 fanout(N≥2) + WaitGroup 屏障**强制并发，`-race`；断言零合成 |
| **R2** | **归属盖错 ⟹ SelfOnly 下被过滤，修复静默失效**（且历史里多一条谁也看不到的垃圾） | `history.go:772-786` 的 SelfOnly 分支；错误来源是在编排边界补而非 agent 自己那层 | 与 `basic_builder.go:425` 用**逐字相同**的 `historyOwner(ctx, name)`；**不要**在 `settleHistoryAt` 里补 | `Parallel(br1,br2)`，br1 审批被放弃；断言 br1 下轮 prior 看得到、br2 一条看不到 |
| **R3** | **memo 有真结果、修复补「未执行」⟹ 对已发生的副作用撒谎**（见上文 ②） | `basic_agent.go:1841` 写 memo；`basic_builder.go:554` 因节点失败不执行 | 用 `AskQuery.ToolCallID` 分档：指名的用 abandon 文案，其余用 `unfinishedToolResultText` | 启发式对 A 返 Allow、B 返 Default；中断后不 Resume 直接发新任务；断言 A 的合成结果**不**声称「未执行」 |
| **R4** | **「事件时补」落在中断路径 ⟹ Resume 重跑双写**（见上文 ①） | `basic_builder.go:554` 与假想的中断补点；`:1123`/`:1166` 一果一用 | 落点只能是「上一轮已定局」的写边界 | **已存在**：`history_resume_test.go:70` 的 `want 2` 就是哨兵，**别改它** |
| **R5** | **合成结果不打 marker ⟹ 被 MicroCompact 清成占位符，语义反转**（模型读到「调了、结果被清了」而非「没执行」）；开 offload 还会 `store.Put` 一份合成文案 | `microcompact.go:200-202` 按**工具名**收集（白名单含 `Bash`/`Read` 等，业务加自定义工具是支持用法）→ `:305` 替换 | 新增 marker，**5 处同时加短路**：`microcompact.go:277`（挨着 `SkillBodyAnnotation` 那个守卫）、`autocompact.go:241-250`、`autocompact.go:607-620`、`reactive_compact.go:265-285`、`basic_agent.go:1030`。⚠️ **同一判据散五处**是既有形状（`SkillBodyAnnotation`/`MicroCompactOffloadedAnnotation` 各占五处），漏一处即静默 | 审批工具命名为 `Bash`，开 MicroCompact 跑到触发清除；**逐个 revert 五处短路，每处都要能红** |
| **R6** | **`approve_always` 跨任务泄漏**——审批层被绕过，安全语义。**代码层面已坐实**：`approvedToolsKey`(`approval.go:76`) 全仓**零清除点** | `resume_helpers.go:99` 的 abort 只 `DeleteSubtree` 不碰 session；顶层 `WithFreshRun` 什么都不删 | 任务边界清 `approvedToolsKey`+`approvedCallsKey`。⚠️ **判据只有一处，别再抄**：顶层 Run 恒 fresh 由各编排 `runGraph` 里 push 的 `compose_v2.WithFreshRunCtx` 声明，引擎据此同时决定「捡不捡存档」和「apply 不 apply 种子」。当年那五个点（agent 侧自查 + 各处镜像）已连同它们多余的 `LoadNS` 一起删干净，那对拷贝漂移过四次 | `approve_always` 答一次 → 同 `RunSession` 发新任务 → 断言**仍然中断要审批**；revert 清理必红 |
| **R7** | **只写穿持久化不改本轮内存视图 ⟹ 文案分叉 + 本轮仍丢整条** | `basic_agent.go:963` 的 `dropDanglingToolCalls` 用的是读 prior 那份（`:801` 读、`:861` append），修复后 append 的不在里面 | 修复函数返回补出的消息，就地插进 `prior`/`msgs`（`:861` 之后、`:963` 之前） | 一次 Run 内断言 `OnModelStart` 看到合成结果 + 那条 assistant；跨 Run 断言历史里逐字相同 |
| **R8** | **配对判据分叉**：按 `map[id]bool` 判会漏补 id 复用的第二轮；不看顺序会把排在声明前的孤儿当应答 | `basic_agent.go:1112-1132` 是唯一正确实现 | 抽 `pairToolResults(msgs)` 两处**共用同一份代码**。⚠️ 全仓已有 **6 份**独立配对实现（`basic_agent.go:1112`/`:1000`、`safe_truncate.go:53`、`microcompact.go:194`、`autocompact.go:735`、`structured_output.go:271`）——**别加第七份** | 直接复用 `history_shape_matrix_test.go` 的 case 表（已覆盖 id 复用/错位/多声明/同 id 多结果） |
| **R9** | **conformance fixture 漏加新字段 ⟹ 业务自研 backend 静默丢**（Resume 后认不出挂起属于哪个 tool_call，分档退化成一刀切） | `checkpointer_conformance/save_load.go:167-171` | fixture 两条 query：一条填 `ToolCallID`、一条留空（验空值也 round-trip，同 `CurrentFrontier` 空 `InstanceID` 那条的理由） | conformance 自身；用故意漏序列化的假 backend 验断言能红 |
| **R10** | **`q.Reason` 单/多路径分叉**：N=1 时 cp 里读不出「这是审批」，N≥2 时读得出 | `approval.go:212` 只填 `ie.Reason`；单 ie 原样透出不经 `MergeInterrupts`（`basic_agent.go:2055` / `graph_loop.go:1313`） | `approval.go:213` 的 `AskQuery` **直接填** `Reason`，别依赖 `interrupt.go:218` 的拷贝 | 单工具 + 双工具两 case，都断言 `cp.PendingInterrupts[i].Reason == "tool_approval"` |

## 会变红的既有测试（预期内，改断言不改行为）

- `history_internal_test.go:363` `TestDropDanglingToolCalls_MisorderedPair`——断言「结果里
  不能有任何 tool_call」，验的是**实现手段不是契约**。新行为下 assistant 被保留 + 补占位
  （配对合法）⟹ 必红。**应改成调 `assertWireLegal`**。
- `history_shape_matrix_test.go:218` 那条 fixture 的 `why`（「整条丢」）变 stale，且它的形状
  （`user, assistant(tc), user换话题`）**恰好就是放弃场景**，改完应翻成正例。

## 零覆盖的空白

`approval_test.go` 14 条**没有一条**覆盖「审批挂起后放弃」，也没有任何历史断言。
这个场景目前**零测试覆盖**。

## 未实测的推断（实施时验）

R5 的五处 marker 短路清单、R8 的六份配对实现清单、`TestCleanup_Idempotent` 在新行为下
仍绿——三条都是读码推断。

---

# 实施 checklist（对照挖出来的，按阶段做）

> 来源：对端那套双层修复的完整实施细节。**每条都注明「不做会怎样」**——这些是它们
> 用注释标红 / 用回归测试钉住 / 或删过又写回来的东西，不是推演。

## ⚠️ 先记住一个结构差异：对端的历史不是 append-only

它有 `replace_history`——全量重写整个 jsonl（tmp + rename 保 crash 原子性）。所以它能
**原地 splice 插入**合成结果，相邻性免费。

**我们的 `HistoryStore` 只有 `Append`/`Load`，append-only 是写进 conformance 的硬语义**
（10 条里就有「逐字重复的两条都得留」「顺序」）。加个 `Replace` 等于让所有业务自研
backend 改接口 + 破坏核心承诺 ⟹ **不能抄**。

**由此推出一条隐蔽的耦合**：append-only 让**相邻性和幂等性绑在了一起**——
必须「排在下一条 append 之前」（拿相邻）**且**「判定用宽规则」（保幂等）**两条一起**。
只做一条都出问题：只做前者，中间插了消息就失效；只做后者，形态难看且 Raw 视图误导。

## 阶段 0 · 前置缺口（不补则后面全做不了）—— ✅ 已完成 `3fb2c65`

1. **`AskQuery` 加 tool_call 身份**（`ToolCallID` + `ToolName`），`approval.go:211` 抛中断时填。
   不做 ⟹ 补不出能配对的 tool_result，方案卡死在第一步。⚠️ 这是 cp 结构变更，走
   SchemaVer + conformance round-trip 断言，否则业务 backend 漏序列化**静默**退化。
2. **`Reason: "tool_approval"` 直接写进 `Query.Reason`**（现在只在 `InterruptError.Reason`）。
   不做 ⟹ fanout 多工具审批时从 cp 里连「这是审批」都读不出来。

## 阶段 1 · 判定逻辑抽取（改动最小，先做）—— ✅ 已完成 `27b6240`

3. **从 `dropDanglingToolCalls` 抽出只读判定** `danglingToolCalls(msgs) []schema.ToolCall`，
   写穿层和视图层**共用同一次扫描**。不做 ⟹ 两套规则迟早分叉，而我们的分叉后果是
   **store 无限增长**（视图层会自愈、store 不会）。对端用注释标红过这条：宽严不一致 ⟹ 插重复。
4. **判定选「宽」档**：「该 id 的声明之后，流里任意位置有对应 role=tool」。
   **不要**用对端的「紧跟连续段」——我们 append 到流末尾，严格规则下每次修复都判
   「不算数」⟹ 每轮再 append 一条 ⟹ **无限增长且完全静默**。
   （视图层的 `dropDanglingToolCalls` 继续用它现有的逐个认领，两者输出一致。）

## 阶段 2 · 事件时补

5. **落点在「挂起存档被放弃 / 被新任务顶掉」那一刻**，不是进模型前。
   只有那一刻知道「是因为审批被放弃」，能写准文案。
6. **文案单独一档**：说「未执行 + 用户转去做别的了 + 请参考用户的消息」，
   **不说「被拒绝」**（那是用户的显式决定，不能替他做）、**不说「请重新调用」**
   （对一个刚被绕过的带副作用动作是错的引导）。对端原文照此取舍。
7. **分档 switch 的 default 要显眼失败**。对端靠 Rust 穷举 match 拿编译期保证并专门写了
   「no `_ =>` guard，加 variant 时每个渲染点都会编译报错」；Go 没有，只能靠 default 报警。
8. **只加当下有 producer 的那一档**。对端因为这个删过一个 variant（doc 里完整记了理由），
   另一个至今零 producer——**它自己的 doc 就在警告这件事**。
9. **合成结果同时 emit 事件给前端**。对端那个函数一次做两件事（发 UI 状态 + push 消息流）。
   不做 ⟹ 前端看到一个永远 pending 的 tool call。

## 阶段 3 · 写边界幂等修复（核心，风险最高）

> ⚠️ **动手前先看两条实测修正**（2026-08-04，阶段 4 落地后补）：
>
> **① 写边界不是 11 个，是 22 个 + 4 个**（本节第 10 条写的「只有两类」不成立）：
> `settleHistorySlot` 11 处、与之成对的 `settleHistoryOnPause` 11 处（Sequential /
> Parallel / Routing×2 含 fallback / Loop / Handoff / Plan×3 / Reflect / RunSubAgent /
> GraphAgent），另加 `recordHistory` 4 处（preamble 记输入 / model 记产出 / tools 记结果
> / `history_slot.go` 内部）。这 22 个**编排边界补记**跟第 10 条说的「Run/Resume 入口」
> 和「新 input 落笔之前」都不是一回事，要逐个判定「这里算不算上一批 tool 已定局」。
>
> **② 本阶段的主要理由已经不成立了。** 原设计里写穿修复是**通过让 `anyReal` 变 true 来
> 间接消除形态 A**（todo 原文：「一旦有了合成 tool_result…保文本分支不再触发 ⟹ 形态 A
> 自动消失」）。阶段 4 已经把形态 A **直接**修掉了，不需要合成结果、不需要写穿。
> 剩余收益只有两条：(a) 历史里那条悬空 tool_call 永久留着——但这是卫生问题不是正确性
> 问题（视图层每次读都稳定派生同一条文本记录，不写回、不增长）；(b) 文案能写准成因
> ——而这条**阶段 2 单独就能做**，它落在放弃那一刻，不扫历史、不碰并发。
>
> 代价那侧没变：R1 仍是「本方案最严重的失败模式」，且 `danglingToolCalls` 的前置契约
> 已确认 **`-race` 抓不到它**（语义重复不是数据竞争），护栏只能靠调用点纪律。
>
> ⟹ **建议：阶段 2 做，阶段 3 不做**（待拍板，别默认按原计划开工）。


10. **写边界只有两类**：① Run/Resume 入口（`startHistory` / `startHistoryForOrchestration`）；
    ② **新 input 落笔之前**（`basic_agent.go:951` 的 `recordHistory(input)` 前 +
    `history_slot.go:338` 里 `msgs = append(msgs, input)` 前）。② 是相邻性的来源。
11. **`makeToolsFn` 内部 / `loadHistory`·`historyPrior` 读路径 / `historyWriter.loop()`
    后台 goroutine 里一律禁止调，并把这条写进注释标红。** 对端原文：
    *"Do NOT call from read handlers — background tasks run concurrently with tool
    execution and would misidentify in-flight calls as dangling."*
    不做 ⟹ 我们的 N≥2 fanout **百分百**把 in-flight 的 tool_call 误判成悬空，真结果回来后
    同一 id 两条——**而我们没有 dedup 也删不掉**（append-only）。**本方案最严重的失败模式。**
12. **修复走 `historyWriter.submit`，复用它的锁做认领**（对齐 `claimRecordHere`）。
    那是这条流上唯一天然串行的点。另起一把锁 ⟹ 扇出分支各补一份（同款形状实测过份数随调度翻）。
13. **写穿失败只 warn 不阻断**，沿用 `writeOne` 语义（历史是旁路）。
    ⚠️ 但要接受「修复了没写穿」的中间态——那条 assistant 下一轮又会被丢。warn 要够醒目。

## 阶段 4 · 独立缺陷（别靠补结果掩盖）

14. ✅ **单独修 `anyReal==false 且无文本 ⟹ 整条丢`**。
    **两家的反面证据**：一家在「零结果」时照样补齐全部合成结果、assistant 保留（有测试
    断言）；另一家无条件 `append(patched, msg)`。**只有我们丢。**
    不做 ⟹ 写穿一旦有路径没覆盖（失败 / 老会话 / backend 没实现），形态 A 立刻**静默**复现。
    ⚠️ 本条原先提的折中（补占位 + 文案分档）**被实测推翻**——「谁会意外命中」的答案是
    return-direct 控制流工具（71%），对它们任何「未完成」文案都是撒谎。见上面「进度」里
    阶段 4 那条。**留作教训：这一条正是本笔记要防的「撞上既有机制」，而写笔记时没看出来
    ——把交互面穷尽一遍是有上限的，装个计数器跑一遍比接着推理便宜得多。**

## 阶段 5 · 测试（每条都要能 sanity-revert）

15. **幂等**：连跑两次，断言 **store 里 role=tool 的条数不变**（不是断言返回值）。
    append-only 下不幂等 = 无限增长，是最难发现的一类。
16. **写穿**：stub `HistoryStore` 记 `Append` 序列，断言合成结果**排在下一条 input 之前**。
    ⚠️ writer 是异步单 worker，测试必须走 `closeAndJoin` 或等排空，否则**假绿**。
    ⚠️ stub 里**不要调 `t.Fatal`**（`history.go:418-450` 警告过 `runtime.Goexit` 会让积压静默蒸发）。
17. **并发测试改成「静态断言调用点集合」**，不是运行时压测——对端靠的是「结构上不可能」
    而非「测出来没事」，全仓也没有这类运行时测试。运行时并发测试在 Go scheduler 下会被
    自然串行化掉，写出来是假绿。
18. **端到端真 LLM 冒烟**：`ApprovalAsk` → 提审批 → 不批直接发新任务 → 断言第二个任务的
    prompt 里那条 assistant 还在、有配对结果、文案是「未执行」不是「被拒绝」。
    对端最高只到 actor 集成层，但我们的规约要求跨层 feature 端到端测。
19. **自查「记着下标、修复改变长度」的地方**。对端有一条真实的 actor panic 回归：修复改了
    长度而 turn capture 的 offset 没 rebase。我们的 `claimRecordHere` /
    `inputAlreadyInHistorySlot` 按内容/指针判、看起来安全，但**要实测确认**，
    尤其 `truncatePriorPreservingMarkers` 那段建索引的逻辑。

## 三条最容易漏的

1. **append-only 把相邻性和幂等性耦合了**（见开头）——两条一起做，缺一不可。
2. **「有 `dropDanglingToolCalls` 就等于历史被修了」是错的。** 对端的载入档同样只修内存
   不写盘，而且**此后再也不会写盘**（下次修复发现内存已干净就跳过重写）。我们的
   `dropDanglingToolCalls` 只修视图，历史里那条孤儿一直在。
3. **别照抄对端的 reason 分档**——它那两档里有一档在生产侧零 producer，doc 描述的是意图
   不是现状，而它自己的 doc 正在警告这件事（那是另一档被删的理由）。
