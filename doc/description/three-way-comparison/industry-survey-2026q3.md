# 行业增量调查（2026 Q3）— 插话与回合闸门，及其余可迁移发现

> **本文档定位**：2026-09-21 对 10 个本地 clone 的**增量**调查（各仓从上次拉取日到 2026-09 中下旬）。
> 触发问题是两个待拍板的特性（**运行中插话** / **回合结束闸门**），顺带盘各仓这段时间对
> 「增强单个 agent」这一层的变化。上一轮全量调查见 [industry-survey-2026h1.md](industry-survey-2026h1.md)，
> 本文只记**增量**与**结论**，不重复那篇的事实。
>
> **调查方法**：五路并发、全部基于**本地源码逐行核实**（禁 WebFetch）。每路被要求
> 「答案是『他们没做』就直接说，并说明搜了哪些关键词」——所以下文的**否定结论也是证据**，
> 不是「没找到」。涉及 crew-go 自身的论断已 grep 实际代码核实。
>
> **时间锚点**：2026-09-21。

## 一、调查对象与增量

| 仓库 | 上次拉取 → 本次 | 新提交 | 本轮读了什么 |
|---|---|---|---|
| codex | 2026-08-21 → 09-21 | 1446 | steer / Stop hook / Goal / 压缩后置 / Guardian |
| deepseek-harness | 2026-08-19 → 09-17 | 5119 | inbox / turn-stopping / 租约 / 压缩影子事件 |
| ag-ui | 2026-07-01 → 09-17 | 4124 | **1.0 协议定稿**（schema + 规范文档） |
| agent-framework (MAF) | 2026-06-30 → 09-21 | 767 | MessageInjection / AgentLoop / agent-hooks |
| openai-agents-python | 2026-07-01 → 09-21 | 684 | guardrail 分档 / 审批参数绑定 |
| crewAI | 2026-06-30 → 09-19 | 286 | guardrail 重试 / 回灌角色 |
| otel-semconv-genai | 2026-06-18 → 09-16 | 91 | registry / metrics / changelog |
| grok-build | 2026-07-16 → 09-19 | 45 | interjection-core / stop_gate / TodoGate |
| langgraph | 2026-08-11 → 09-20 | 43 | 答案账本 / TracePolicy |
| langgraph-bb | — | 6 | 停滞（最新 2026-04-29） |

## 二、核心问题一：运行中插话

### 2.1 谁做了

| 项目 | 做了吗 | 机制名 | 抽干点 | 进消息流的形态 |
|---|---|---|---|---|
| **codex** | ✅ 一等机制 | `steer` | ReAct 循环顶端、构造本步 context **之前** | 普通 user 消息，**零标记** |
| **deepseek-harness** | ✅ 一等机制 | `steer`（收件箱） | 步骤边界 / 回合边界 | 普通消息 + **必填 `source` 字段** |
| **grok-build** | ✅ 一等机制 | `interjection` | tool batch 后 / 循环顶 / 回合返回前（+ 打断阻塞的等待工具） | **包信封**的合成 user 消息 |
| **MAF** | ✅ | `MessageInjectionMiddleware` | 下一次 model 调用前 | 普通 user 消息，零标记 |
| **LangGraph** | ❌ 库层没有 | （平台层 `multitask_strategy`） | — | — |
| **eino** | ✅（2026-02 已有，本窗口未动） | `TurnLoop.Push` | 安全点抢占 | 业务定 |
| **OpenAI SDK / crewAI** | ❌ | — | — | — |

⚠️ **搜词陷阱**：deepseek-harness 全仓 `interject` 只有 1 处注释命中——这个概念在那儿叫
`steer`。按 `interject` 搜会得出「他们没做」的错误结论。

### 2.2 唯一的共识：没有人把新输入塞进正在跑的那一步

统一形状是**「在一个后代静止的安全点切断 → 下一轮重新构造请求时合并」**。
eino 把这条的判据写得最清楚（`adk/turn_loop.go` 的 `WithPreempt` godoc）：
`AfterChatModel` / `AfterToolCalls` 之所以是安全点，**不是因为语义好听，而是因为那两个位置
整棵 agent 树没有在飞的后代**，所以抢占不需要递归拆除；而会升级成立即中止的
`WithPreemptTimeout` **必须**配递归拆除路径。

> 对我们：`SoftCancelSignal` 切在**超步边界**，判据同构。但**嵌套子图的超步边界跟父图不对齐**
> ——父图在超步边界时子图可能正在自己的超步中间。将来要给 soft cancel 加更细的切点时，
> 先问「这个点上后代静止吗」。

### 2.3 三个各自解决了不同问题的机制细节

**① 提交入口唯一 + 路由模式显式 + 乐观并发检查**（codex，`protocol/src/turn_input.rs:133`）：

```rust
enum TurnInputMode {
    StartOrSteer,                                 // 空闲就开新回合，否则插话
    StartIfIdle,
    ContinueIfIdle { expected_previous_turn_id }, // 内部续跑，带 CAS
    Steer { expected_turn_id },                   // 只插话，且必须是这个回合
}
```

`expected_turn_id` 解决的是「我打字时 A 回合还在跑，按回车时已经是 B 回合了」。
失配返 `ExpectedTurnMismatch { expected, actual }`，前端拿到 actual 后**只重试一次**。

> 对我们：这个 ID 在我们这儿的对应物是 **RunID**——CLAUDE.md 已定死「RunID 一枚只服务一个
> 任务」，语义天然对上。

**② 两条队列 + 三个动词**（deepseek，`packages/core/agent-loop/src/agent.ts:137-147`）：

| 动词 | 目标队列 | 唤醒 idle agent | 语义 |
|---|---|---|---|
| `followup(m)` | `next-turn` | ✅ | 排一个新回合 |
| `steer(m)` | `next-step` | ✅ | 插进正在跑的回合 |
| `inject(m)` | `next-step` | ❌ | 有步骤就带上，但**不要因此开工** |

`inject` 单独存在是设计选择：框架/工具产生的上下文（文件变更通知、cron 通知）
**不应该把一个 idle 的 agent 叫醒**。认领语义：`next-step` 全取、`next-turn` 只取一条。

**③ 唤醒竞态的两处处理**：
- codex `input_queue.rs:101` 的 `subscribe_activity` 同时返回 receiver **和**「订阅之前是否
  已经有东西」的快照——处理经典的 missed-wakeup。
- deepseek `agent.ts:131-132`：当前活动**已经 abort** 时，唤醒型输入从 `next-step` 被
  **重分类**成 `next-turn`（「唤醒输入不能加入一个已死的活动」），且这个判定**在插入之前捕获**，
  防止观察者里的重入 cancel 把它改掉。

### 2.4 「包信封 vs 必填 source」——两家付了不同的代价

**grok 包信封**（`crates/common/xai-interjection-core/src/format.rs:4-9,22-24`）：

```
The user sent a message while you were working:
<user_query>{用户原话}</user_query>
Make sure to complete any unfinished tasks from previous turns.
```

尾部那句是**防「掉活」**的——注释写明「模型收到新 user 消息的本能是放弃手头的事」。
代价是持久化时得额外存 `displayText` 才能还原用户到底打了什么。

**deepseek 不包信封，靠必填字段**（`packages/llm/llm/src/message.ts:130-140`）：
每条 Message 都有必填 `source`：`{kind:'user'|'plugin'|'model'|'tool'}`。
决策记录明确**否决**了信封方案，理由正是 grok 付的那个代价：信封「让 outbox 条目、
会话 projection 和 UI 回放去负责一个本该属于生产者的区分」。

> **对我们最重要的一条**：`repeat-tool-reminder` 里有句注释点破了 `source` 为什么必填
> ——「没标签的上下文在派生历史里会渲染成用户提示」。**这正是我们
> `UnresolvedCallsRecordAnnotation` 那一坑的通解**：我们靠一个可选 annotation + 每处记得调
> `IsUnresolvedCallsRecord`，他们靠必填字段让「忘了标」在编译期就不可能。
>
> 信封那套**别抄**（我们是库，业务的 transcript 归业务，给它一个被框架污染过的 user 消息
> 是添麻烦）；但 grok 的**尾部「别丢下手头的活」提醒**是便宜的好东西，值得作为可选默认文案。

### 2.5 取消安全：两家分歧，而且都对

- **grok：还回去**（`interjection.rs:395-418` 的 `RestoreOnCancel`）。drain 到一半被 abort，
  已取出未提交的条目在 Drop 里**按原顺序塞回队列头部**。理由：pager 已经跟用户说过
  「插话已发送」了，静默丢掉就是撒谎。
- **deepseek：不还**。决策记录明确否决了退回队列：「把否决变成隐藏的队列变更……
  让认领不再是一次原子的所有权转移」。

> 这两个不矛盾——grok 保的是**已向用户回执过的消息不能丢**，deepseek 保的是**认领的原子性**。
> 我们的判据应该是「**有没有向调用方回执过这条已接受**」：`Ask` 那条路已经给了 InterruptID，
> 就必须保证不丢；fire-and-forget 的插话，认领即所有权更干净。**别两个都做一半。**

## 三、核心问题二：回合结束闸门

### 3.1 谁做了，以及两种相反的范式

| 项目 | 做了吗 | 否决者 | 否决的载体 | 上限 |
|---|---|---|---|---|
| **MAF** | ✅ 完整 | 业务谓词 / 内置规则 / **LLM judge** 三档 | 返回值 `bool` 或 `(bool, feedback)` | `max_iterations`，**且在谓词之前短路** |
| **codex** | ✅ 两种正交 | 外部 Stop hook（同 turn 回炉）/ Goal（另起新 turn） | `{"decision":"block","reason":...}` | **框架无上限**（hook 自律）/ Goal 侧三条熔断 |
| **grok** | ✅ | hook | 显式枚举 `TurnControl::{Auto,ForceContinue,ForceStop}` | **8 次/回合**，到顶**一个 hook 都不咨询** |
| **deepseek** | ✅ | 任意监听器 | **数据**（往收件箱塞东西），hook 不返回决策 | **无**（仓内留着 `TODO(stop-loop-guard)`） |
| **crewAI** | ✅ 简版 | `Task.guardrail` | 返回值 | `guardrail_max_retries` |
| **LangGraph** | ⚠️ 只能否决工具，**否决不了终局** | `post_model_hook` | 消息形状 | `remaining_steps` |
| **OpenAI SDK** | ❌ **故意不做** | — | — | — |

### 3.2 OpenAI 那条「故意不做」是最强的证据

它的**工具** guardrail 有 `reject_content` 档——原文「Rejects the tool call/output but
continues execution with a message to the model」，即**打回去让模型重来**；
但**输出** guardrail 只有 tripwire → 抛异常终止。

**「打回重来」这个语义在它词汇表里有，故意没用在终局上。** MAF 同一套分法：
`deny` 在 tool 点是「阻止工具 + 把错误喂回模型、循环继续」，在 input/output 点是
「终止整个 run」。

> 结论：**guardrail 不是闸门**。guardrail 是拒绝，闸门是打回重来，两者在两个仓里
> 都是并存的独立机制。

### 3.3 「数据决定」vs「显式枚举」——建议取前者 + 后者的上限

**deepseek 的数据决定**（`packages/core/agent-loop/src/agent.ts:316-321`）：
闸门 hook **不返回任何决策**；想否决的监听器就去调 `agent.steer(...)`，引擎在 hook 跑完后
**重读收件箱**，非空就再跑一步。godoc 原话：

> Data decides, so listener order cannot change the outcome.

由此**「多个监听器意见冲突谁赢」这个问题根本不存在**，不需要定义合并语义。
而且**「插话」和「闸门否决」是同一个机制，不是两套**。

**grok 的显式枚举**多出四件值得抄的事：
1. **上限 8 次/回合**，且**到顶时一个 hook 都不咨询**——别在已决定忽略的判断上白烧 hook 副作用。
2. **双向**：`prevent_continuation` 优先于 `blocks`，且强制停直接跳过 client 侧 gate。
3. **fail open**：所有 hook skip 或崩 → 允许停止。
4. **hook 知道自己是第几次**：`stop_hook_active` 传给 hook，让朴素实现能自限。

> **建议形状**：取 deepseek 的「数据决定」（我们是被集成的库，hook 可以有任意多个，
> **枚举的合并语义一定会漂**）+ grok 的上限（「让 hook 自律」在我们这儿不成立）。

⚠️ **grok 有一个语义不要抄**：`wants_continuation()` 的定义是「没有 prevent_continuation
**且**（有 block **或**有 additionalContext）」——**一个只想补充上下文、没说要阻止的 hook，
会导致回合继续**。「我有话要补」被当成了「你别停」。

### 3.4 三条必须配套的约束

**① 「拦而不说 = 不拦」**（codex 出现两次，说明是刻意的）：
Stop hook 返回 block 但没给 reason → **block 被忽略并发 Warning**；
PreToolUse hook blocked 但没给 `block_reason` → **降级为 Continue**。

> 这对应我们最该防的一类假绿：一个返回「继续」但没给反馈内容的回调，会让模型在
> **完全相同的输入**上再跑一遍——`err == nil`、日志干净、无限循环。
> **我们的决策类型应该让「继续」和「反馈内容」在类型上绑死**
> （`Continue(feedback string)` 而不是两个独立字段），从结构上消灭这个状态。

**② HITL 逃生口**（MAF `_harness/_loop.py:445` 的 `_has_pending_approval_request`）：
如果这一轮返回的是「等审批」而不是「答完了」，**立即 break，不否决也不注入**。

> **我们如果加闸门，这条是必须的**：`BasicAgent` 抛 `InterruptError` 时最终消息也是
> 「没有 tool_call 的 assistant」形状，闸门会把 HITL 挂起**误判成终局**——正是
> `UnresolvedCallsRecordAnnotation` 那类「长得像最终答复」的坑再来一遍。

**③ turn-scoped vs iteration-scoped 分档**（MAF `_sessions.py:780`）：
闸门每转一轮都是一次完整 agent run，于是**会改写持久历史的 provider 必须延到闸门边界
才跑一次**（注释原文：「firing mid-task would rewrite history the task still needs」）。
MAF 的做法是往 **run options** 里盖一个标记而不是 ctx，理由是 options 只到闸门自己驱动的
那些 run，**嵌套子 agent 的 run 有自己的 options，天然不泄漏**。

> 我们的 postamble 节点跑 `commitFinal` + StoreMemory `store.Put`，`OnAgentEnd` 也是
> Run/Resume 各一次——加闸门就必须先回答「这些是 per-iteration 还是 per-turn」。
> 而「用 options 不用 ctx」这个理由在我们这儿同样成立（CLAUDE.md 已记过
> 「component 跨 graph/agent 边界泄漏」的教训）。

### 3.5 一个不用 LLM、不用业务回调的闸门：grok 的 TodoGate

`turn.rs:3544-3600` + `reminders.rs:104-137`。模型没发 tool_call 想收尾时，
todo 列表里还有 pending/in_progress 就推一条 system reminder 然后 `continue`。
四个细节是精华：

1. **判官是 agent 自己写的状态**（todo 列表），纯函数，零 I/O。
2. **「有后台任务撑着」的 in_progress 不算数**：`backed_count = in_progress.len().min(backing_task_count)`。
3. **必须给逃生舱**：提醒文案明写「如果你有真实的外部阻塞，明确说出来，**并且在同一轮里**
   把受影响的 todo 标成 cancelled」。**没有逃生舱的闸门是陷阱。**
4. **耗尽时告诉模型为什么放它走**：「你尝试了 N 次结束本回合但 todo 仍未完成，现在交还给用户」。

> 这是「**闸门的判据可以不来自业务回调**」的存在性证明。我们有 PlanAgent 的 step 列表，
> 同款闸门立得住。⚠️ 但注意第 4 点：我们已经有 `MaxIterationsError` / `maxRounds`，
> 新闸门**必须和它显式排优先级**，否则就是「两条判据读同一份状态、在边界上分叉」。

### 3.6 codex 的第二种闸门：不拦 turn，另起一个

Goal 续跑挂在 **thread idle** 上而不是 turn 循环里：回合正常结束 → 若 goal 仍 Active →
起一个**新 turn**。它的三条熔断里，**「复位条件」比「阈值」重要**：

| 熔断 | 阈值 | 复位条件 |
|---|---|---|
| 连续空转 turn | 3 | 任何一种 TurnItem 活动 |
| 连续 exec 失败 | 3 | 任何工具成功 |
| token 预算 | 配置 | 触发时中途注入提示，且**每个 goal 只注入一次** |

空转判据写得很紧：`empty = automatic && turn.empty_final && !turn.has_activity`
——**只有框架自动起的续跑 turn 才计数**，用户自己发起的不算。

> **这一档我们其实已经有了**——`LoopAgent` + `ExitCondition` 就是这个形状，只是**没有熔断**。
> 要抄的是熔断的形状（尤其复位条件的粒度），不是 Goal 那个 feature（那是平台层的东西：
> 对象/预算/状态机 + SQLite 表 + 三个 LLM 工具）。

### 3.7 两条泼冷水的

- deepseek 的 `agent/turn-stopping` **生产代码里一处都没当闸门用**（真实调用方是记 git diff
  和桥接外部 hook），「否决终局」只在测试和 README 里。
- grok 的 `TurnControl` 在开源仓里 **0 个消费者**（消费方未开源）。
  上文引的实现细节来自 `stop_gate.rs`，那是**另一套并行机制**，不要当成同一个。

> **形状干净，但没被重度使用验证过。**

## 四、AG-UI 1.0：我们有两处真不合规

我们有 `agui` 适配包（内联精简协议层），1.0 定稿于 2026-09-17。

**好消息**：我们实现的 14 个事件**形状一个没变**；1.0 最大的破坏性动作（退役 `THINKING_*`
五件套）动的恰好是我们从没实现的；HITL 回传 `resume:[{interruptId,status,payload}]`
跟正式定稿的 `ResumeEntry` **逐字段吻合**。

**两处必须跟**（⚠️ **2026-09-21 当日已全部修掉**，连同下面「另两处值得一并修的」——
见 `doc/code/todo.md` 同名条目；本节保留原始分析作为**为什么当初那个方案是对的、
以及它什么时候变成违规**的记录，别当成现状）：

### ① 取消路径违反一条显式 MUST

1.0 落地了 `cancelled` outcome（`spec/1.0/schema.json:924-933`），规范写明：

> A producer that stops a run before completion, for a reason that is not a failure, MUST close
> it with `RUN_FINISHED` carrying the cancelled outcome. It MUST NOT report a stopped run as
> success — **neither with the success outcome nor by omitting the outcome**.

我们走的正是「omitting the outcome」那条，而规范同时规定「absent outcome means success」
⟹ **今天 1.0 前端把我们的取消读成了一个成功的 run**。

schema 作者把我们踩的坑写进了设计理由：「an outcome a consumer does not recognise is
stripped and read as success — a cancellation added later would reach every 1.0 consumer as a
completed run」。

### ② 续跑复用了被中断那个 run 的 `runId`

1.0：`runId` **MUST NOT be reused**，且「continuing means a new run」。
我们两处用 `sess.RunID`，而同一文件的另一条路（`serveRun`）是正确的（优先取 `in.RunID`）。

> 根因正是 CLAUDE.md「借来的量」：**`sess.RunID` 是我们的「一个任务」身份（一次 Run + N 次
> Resume），AG-UI 的 `runId` 是「一次 run」身份**。N=1（没中断）正确，N≥2 静默错。
> ⚠️ 不是改个变量名就完——`adapter.runID` 同时被当 messageId 用。

**另两处值得一并修**：每个 tool result 共用一个 messageId（1.0 把唯一性写成了规范，
叠加 ② 之后续跑的工具结果会被**追加进中断前那条消息**）；`"(empty)"` 占位符的协议依据
存疑（1.0 明写「a result whose every part was dropped is answered with the empty string」）。

**新增能力里契合度最高的**：`SUBAGENT_STARTED/FINISHED/ERROR` + `subagentRunId`
——我们有 9 种编排、满世界的 sub-agent，现在前端看到的是一锅扁平事件，
而**我们的 `graphNS` 天然就是这个东西的现成来源**。

## 五、OTel GenAI semconv：我们在用的一个都没动

- `gen_ai.operation.name` / `request.model` / `response.model` / `agent.name` / `tool.name` /
  `response.finish_reasons` / `usage.*` **全部原样在册，无改名无废弃**。
- `operation.name` 枚举**只加了一个 `fetch_response`、一个都没删**——我们的
  `agent`/`retrieval`/`embedding`/`guardrail` 偏离度**跟三个月前完全一样**。
- **稳定性原地未动**：`model/gen-ai/` 下 151 处 stability 声明**全是 `development`**，零个 stable。
- 窗口内 3 个 breaking + 1 个 deprecation **逐条与我们无关**（`cache_creation` 改名我们不发；
  从 agent span 移除 cache 用量**我们本来就挂在 model span 上**；废弃的是
  `output.messages[].finish_reason` 而**我们发的正是存活的那个**）。

> 两处前提更正：`gen_ai.usage.type` 我们其实不发（发的是 `gen_ai_token_type`，与 OTel 对齐）；
> metric 名整套是我们自己的词表，与 OTel **零重叠**，所以 OTel 的 metric 变更对我们结构性无影响。
> （2026-10 改成标准 OTel 后这两句不再成立：token 指标按 GenAI 约定发 `gen_ai.client.inference.usage.*` +
> `gen_ai.token.modality`，约定的指标变更要跟进，见 [observability-trace-spec.md](observability-trace-spec.md)。）

## 六、其余可迁移发现（按价值排序）

### 1. 「装一半」应该在结构上不可能（MAF `_agent_hooks.py:12-15`）

MAF 的三个 middleware **故意做成私有、作为一个不可分割的整体交付**：

> the three middleware implementations are **deliberately private and travel as one indivisible
> unit**, because installing only part of them would enforce only part of the control contract.

> **直接命中我们**：CLAUDE.md 写着「**必同时接 AgentHooks + OrchHooks**——漏接 OrchHooks 时
> Compact/Cp/Fanout/Node/Skill 8 个 metric 全漏 fire」。**我们靠文档约束，它靠类型。**
> 两个公开 getter 摆在那儿就注定有人只接一个。
> 成本很小，消灭的是一整类静默失效，且与「消灭拷贝而不是写注释提醒同步」的既有规约一致。

### 2. 压缩后置到回合结束（codex `context_window.rs:111-118` + `turn.rs:707-733`）

模型给出终局答复**之后**、回合结束**之前**，占用超过阈值就压一次。默认 0=关。
四个跳过条件：队列有 pending input / 回合被取消 / TokenBudget 模式 / 审批 reviewer 会话。

> ~~**这是我们 4 层压缩里缺的一层**：现有四层全在「要用的时候才压」。后置压缩把延迟挪到
> **用户不等待的时刻**——用户看答复时压，下一轮开局就有余量。我们有 postamble 节点，成本极低。~~
>
> **⛔ 已实现后撤回（2026-09-22），本条判定为非目标。别照着再做一遍。**
> 上面那段的两个判断各错一半：「四层全在要用的时候才压」为真，但**那不是同行做这件事的
> 理由**（codex 的理由是 `serialize history updates`、MAF 是 `rewrites the transcript the
> loop still works from`，都是**历史重写的安全点**，不是延迟）；「成本极低」把「接进去便宜」
> 当成了「接对了」。
>
> 撤回理由（全部实测）：产出写进 `RunSession` 事件靠 marker 截断，而 `WithHistory` 那条路
> 不读 events、Handoff 的 Rewrite(默认)/Raw 读**全部** events 于是摘要被当普通历史**追加**
> （prompt 反而变大 + 合成摘要反复注入活对话）、`agui`/`a2a` 两个 wire 入口**每轮新建
> `RunSession`** 跨轮零历史 ⟹ **框架自带的多轮入口无一能受益**；`AgentAsTool` 路径产出直接
> 消失而 `runMember` 正常 ⟹ 按**派发路径**分叉，config 闸看不到，「加个闸就能修」不成立。
> 另：它静默推翻了本仓**原则#5**（压缩产物不写 `session.events`，见
> [context-compaction.md](../../v2/context-compaction.md)）——那条被评估过并否决、从未被反驳。
> 且「延迟挪到用户不等待的时刻」在框架侧没有前提：`Run()` 同步返回，实测单层 +121ms、
> 链上 N 个 BasicAgent 线性叠加；codex 自己遇到「有人在等回合完成」时是**整个关掉**这个特性的。
>
> 顺带记两条可复用的：五家实现里**没有一家**是「追加 marker + 读取侧自己算截断点」（全都
> 追加一条**自带被替换内容或跨度**的记录）；「把延迟挪走」同行给的是另一个形状——**提前起跑**
> （离阈值还有 N 个百分点时后台先摘好缓存，撞线时只做应用，撞上在飞就等它），它敢放后台的
> 前提是「只读快照、不改共享态」+ 长驻执行体，我们 `Run()` 会返回、活过它的 goroutine 没有主人。

⚠️ 同一个 commit 带了一个必须对照的修复：压缩前是边流式边写 live history，流到一半失败
就留下半截垃圾；修法是先缓冲、**成功且 assistant summary 非空**才替换历史。

> 我们的 `AutoCompactRun` 已有 `empty summary block` 分支（计断路器），这条**已覆盖**。

### 3. 跨进程写所有权：内核锁，刻意不设租约过期（deepseek `lease.ts`）

POSIX 用 `flock(2)`、Windows 用命名内核信号量。**刻意没有过期时间**——
「there is deliberately no expiry that could expropriate a stalled writer whose resumed appends
would tear the log」；进程死亡内核自动释放，所以崩溃的持有者不会堵死后继者。

> 他们是**悲观排他**（拿写句柄那刻就分胜负），我们的 `WithStaleWriteGuard` 是**乐观检测**
> （写回时比世代，副作用已双跑）。两条路代价不同，**不必改**。
> 但他们那句**能力边界声明**值得照搬进 `checkpointer_conformance` 的包 godoc：
> 「你的 backend 要么记录等价保证，要么要求部署方串行化」——而不是留给业务自己发现。

### 4. 反对给子 agent 加专属 system prompt / 专属工具的 KV-cache 论证（deepseek）

> 子专属工具和 prompt 段落排在每个继承的 fork 回合**之前**，让可续 fork 子的请求头与父不同，
> **而且是在 fork 本来要复用的那段历史之前**，迫使 provider 重新预填整个拷贝来的 transcript。

他们的解法是把指令放进**子的初始 user 任务**（在继承的前缀之后）。

> 我们的 `AgentAsTool` / fork skill / Handoff 铸造都在往子 agent 塞东西，
> **这个约束我们从没考虑过。**

### 5. 工具重复调用检测（deepseek `repeat-tool-reminder`，180 行）

对上中心表里「重复工具调用无检测，唯一防线 maxRounds 是悬崖」那条。最值得抄的四点：
- **只增不否**：往 `additionalContexts` 前面加提醒后 **DELEGATE**，让后面的监听器仍能 block。
- **在 post-execute 计数**：因为**被拒绝的调用也走 post-execute**，而「模型死命捶一个被拒的
  调用」恰恰最该打断。→ **直接对上我们的 approval Deny 路径**。
- **预览截断（500 字符）只约束模型可见文本，不约束检测键**：「大 payload 会无界地骑进下一个
  请求——恰恰是在循环场景下」。
- **用户插话重置计数链**：「A user interjection changes the context; repetition across it is not
  a loop.」——插话与循环检测的交叉点，不想清楚就会「用户明明换了方向、agent 还被判死循环」。

### 6. 非阻塞提问（codex `request_user_input_async`）

模型调它 → 工具**立即返回** → agent 继续跑；用户什么时候答都行，答案**以 steer 形式**汇入。
**它没有引入任何新机制**，完全建在 steer 队列上——这是整个调研里最优雅的一处。

> 我们的 `ask_user` 和工具审批**只有阻塞一种形态**。非阻塞提问在「我不确定你偏好哪种，
> 但先按默认做着」这类场景下体验差一个量级。
> **但它以 steer 为前置依赖**——这解释了为什么两个问题要一起决定。
>
> ⚠️ **不该抄的部分**：它的答案回来时**不带任何关联信息**，就是一条普通 user 消息、模型自己
> 认领。我们不能这么做：`Ask` 的 `InterruptID` 是七维位置 + 任务代号（+ 被否决后重做时的重做代际与嵌套图的外层代际链）算出的稳定 ID，
> 整套 `ConsumedAnswers` 账本依赖它。非阻塞提问在我们这儿必须**保留 InterruptID 但不挂起**。
> 这正是「借来的量」的反面教材：codex 借了「普通 user 消息」当答案载体，
> N=1（一次只问一个）对，N≥2 就只能靠模型猜。

其 schema 设计值得抄：`options` 是**建议答案**且第一项预选，同时 UI **永远允许自由文本**，
工具描述里明确禁止模型自己加 "Other" 选项。我们的 `AskQuery.Options` 没写清这一点。

### 7. 审批作用域的 `Turn` 档（codex `request_permissions.rs`）

`PermissionGrantScope::{Turn, Session}`。**`Turn` 档是我们完全没有的**——
「这一轮里别再问我了，下一轮重新问」。

> 在我们这儿实现干净（存活动 Run 的 channel state 而非 session values），
> 而且**正好避开** CLAUDE.md 记录的 `approve_always` 跨任务泄漏坑（那个坑的根因
> 就是作用域挂在 session 对象上）。⚠️ 他们自己标 `UnderDevelopment, default_enabled: false`。

### 8. 归属从授权推导，不从入参取（deepseek 子 agent 消息）

`senderSessionId` 由**已授权的对象**导出，模型可见文本框成 `Agent <sender-id> sent a message:`
——理由是「so attribution cannot diverge from authority」。授权边界是「一条相邻边」，
兄弟/自己/隔代/陈旧对象**都不是备选路由**。

> 跟我们 `a2a/server` 和 `agui` 的 `WithOwnerResolver` 是同一条教训，
> 但他们更进一步：**连展示文本都由授权侧生成**。

### 9. 框架回灌给模型的消息必须是 user 角色（三仓独立收敛）

crewAI `9393a47f3` 的教训最实：原本把「give your best final answer」当 **assistant 消息**
追加靠 prefill 续写，而**当前 Claude 模型对结尾是 assistant turn 的请求直接返 400**
——且是在迭代预算**已经烧完之后**才炸。

> 我们现在触顶是抛 `MaxIterationsError`、**没有回灌路径，暂时不踩**。
> 但只要做闸门（或任何「框架合成一条消息喂回模型」的东西），就得钉死 user 角色。

### 10. 压缩做成影子事件而不是改写消息流（deepseek）

被遮蔽的内容**留在会话日志里**，派生视图去应用压缩决策，所以重放能确定性复现。

> 顺带解决了我们「审计视图 vs LLM 输入视图差异未对齐」的老问题：**两个视图都是同一份日志
> 的派生，不可能漂**。但对我们是**大改**（cp 是覆盖式单槽而非 append-only），记 backlog。

### 11. 只有同步 hook 能改控制流（codex `hooks/src/engine/mod.rs:154`）

异步 hook 的产出只能作为**上下文**在下一轮注入。

> 这是我们那张 [消费] / [只读] 表的**另一个切法**（按执行模式而不是按字段分档），
> 比按字段分更容易讲清楚。

### 12. 内部决策 LLM 不该触发用户配的生命周期 hook（codex `hook_runtime.rs:431`）

`SubagentStop` 是独立事件名，且**内部/合成 subagent 直接返回默认 outcome**。

> **可直接核实的问题**：我们的 `runDecisionAgent` 摘了 ns / store scope / 历史，
> **没摘 hook**。OrchHooks 经 ctx 自动继承 ⟹ 分类器 / 评估器的内部图节点会进业务的
> 观测统计，而业务**分不出来**（没有区分标记）。

### 13. LangGraph：答案账本只提交到实际用到的位置（`types.py:1010`）

由 `resume` 全量改成 `resume[:idx+1]`。解决的是「一个节点里多次 interrupt，第 1 次已答、
第 2 次的答案非法」时把非法值也记成已兑付。

> **我们已经是对的**（已 grep 核实）：`ConsumedAnswers` 只收**成功成员**的
> （`if r.err != nil { continue }` 在前），提交点本来就在「用过之后」。

### 14. LangGraph TracePolicy：体量控制与脱敏是两件事（`types.py:529-567`）

体量杠杆做成 **per-node、opt-in、不递归**的 transform（漏配只是多记点东西，不会丢字段）；
脱敏杠杆放在 **exporter/client 层、全局、递归**（那才需要 fail-closed）。
docstring 明写「**Not intended to redact secrets**」。

> **独立外部印证了 CLAUDE.md 观测三铁律第 2 条**（体量控制走 spec 机制、别加 fail-closed
> 白名单裁 span attr）。两个杠杆分开、作用域相反。

### 15. MAF 可移植过滤器：「都有但各答各的」没有档位

> Execute filtering natively where supported; otherwise implement an explicit connector-local
> fallback, **or reject the unsupported option rather than silently ignoring it**. …
> **it is not a core capability gate.**

且他们**把基于 lambda 源码/AST 解析的过滤器换成了数据树**，因为 lambda 下推不了。

> **纯验证，不用改**：我们的 `store.Store` filter 本来就是数据（`WithFilter(map)`），
> 已在对的一侧。这是一份 2026-09 的新鲜佐证，且来自同一类产品（多后端 SDK）。

### 16. 一个被接受但不被消费的「收窄参数」等于静默放权（LangGraph `5a77be5e8`，安全修复）

`@auth.on.threads.create(resources=..., actions=[...])` 的收窄参数**签名收了但整个没用**，
全部注册成通配 handler。N=1（只注册一个）完全正确，N≥2 才出事。

> 我们已知有两个同形状的（`allowed-tools`、`user-invocable`），
> 但**我们在 godoc 里明说了不强制**——那是正确处理。
> **判别式**：参数名里有「限制/白名单/范围/仅」的意思，就去找它的读取点，找不到就是这个 bug。

## 七、明确不该抄的

| 东西 | 谁的 | 为什么 |
|---|---|---|
| `wants_continuation()` 把「有上下文要补」当成「别停」 | grok | 两个语义塞一个判据 |
| 把四种来源塞进同一条队列 + 用额外状态位事后区分 | codex `TurnInput` | 正是「借来的量」；要做就分两条 |
| `acceptance_order` 会话级单调计数器 | codex | 服务于压缩后重建顺序；我们历史层 append-only + 写入器串行，天然有序 |
| 信封包裹插话 | grok | 我们是库，业务的 transcript 归业务 |
| 事件溯源的收件箱（projection + fold） | deepseek | 我们 cp 是覆盖式单槽；直接放进 `Checkpoint` 即可拿到同等崩溃安全 |
| Goal 整个 feature（对象/预算/状态机 + SQLite + 3 个 LLM 工具） | codex | 平台层的东西；我们 `LoopAgent.ExitCondition` 已能表达 |
| `policy.md` 的规则内容 | codex Guardian | 通篇是 OpenAI 自己的组织假设；CLAUDE.md 已定「规则内容业务自负」 |
| 内置 LLM 审批 | deepseek `auto-review` | 他们自己标 experimental 且默认关 |
| OS 原生沙箱（MXC / Seatbelt / Linux managed） | codex | 对 Go 库不适用；我们 `sandbox.Sandbox` 选的就是「不自己做隔离」 |
| 崩溃后「从历史重放」重开 turn | codex | **反面对照**：他们没有执行点快照所以只能重放（工具会重跑）；我们的 checkpoint 模型严格更强 |

## 八、我们领先的地方（不用抄，记着别被"建议对齐"误导）

- **中断回合的续跑**：deepseek 在已知限制里明写「Synthetic closers are the only crash story
  … there is **no partial-turn resume**」；grok 侧也只有 out-of-band 的历史修复命令。
  **我们的 HITL Resume 在这一层是独有的。**
- **答案账本的提交点**（见六-13）。
- **序列化失败的响亮失败**：eino 刚把「单个值存不下 → 整份存档失败」改成降级（丢值 + 标记
  节点重跑）。**我们保持响亮失败是对的**——丢一个 channel 值在我们这儿就是节点拿零值重跑，
  正是「两头不着的 fresh run」；而我们的错误还带诊断提示。**别顺手加降级。**
- **审批与参数改写的顺序**：`OnToolStart`（[消费]档，可改参数）→ 写回 → `tools()` → 审批
  ⟹ **用户批准的就是真正执行的**。OpenAI 为此专门加了一个模块做执行前重新核对。
  ⚠️ 2026-10-14 补：这条当时只在一次执行之内成立——续跑时 `OnToolStart` 再改写，执行的是新内容、批的是旧内容（实测）。
  现在一批调用的内容在第一次执行时定下、续跑沿用，才跨续跑成立（决策见 [harness-survey.md](harness-survey.md)
  「审批与调用内容的绑定」）。

## 九、决策建议

**两个特性一起做或一起不做。** 理由有二：
1. 「否决终局」不是独立原语，是**插话的一个特例**（把「重做」当成一条插进来的 item）
   ——eino 和 deepseek 都用同一套机制覆盖两个需求。做两套机制会多一份会漂的判据。
2. 非阻塞提问（六-6）**以插话为前置依赖**。

**若要做，建议的形状**（综合五家）：
- 队列挂 `RunSession`（不走 cp），两条队列区分「新任务」与「插进本回合」。
- 抽干点：**model 节点入口、四层压缩之前**（压缩要能看见刚插进来的话）。
  **不要放 tools 节点**——那里是 N≥2 goroutine 并发 fanout，会撞上「同 target 扇出 payload
  塌成一份」那个已知坑。
- 提交入口唯一 + 显式模式 + 用 **RunID 做乐观并发检查**。
- **内部决策 LLM 必须拒绝插话**（`runDecisionAgent` 那条已有分界线，零成本）
  ——否则用户一句话能改写分类器的判断。
- 闸门用**数据决定**、配**框架级上限**，决策类型让「继续」和「反馈内容」**绑死**。
- 闸门必须有 **HITL 逃生口**，且与 `maxRounds` **显式排优先级**。

**若不做**：AG-UI 1.0 那两处不合规与「装一半」那条是独立的、成本小得多的收益，
不依赖这两个特性的决策。
