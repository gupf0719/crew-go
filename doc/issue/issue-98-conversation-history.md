# ISSUE-98：会话历史层

> **状态**：✅ 已实施（2026-07-23）。历史层落地（`agent_v2.WithHistory` + `HistoryStore`
> + `history_conformance`）、三写入点接上、`prior` 换源、伪历史 cp API 连 redis/sql backend
> 一并删除。§6 实施步骤即落地路径，§7 是当时的未定项（实施时的实际取舍见下）。
> **决策日期**：2026-07-22（实施 2026-07-23）
> **调研对象**：OpenAI Agents SDK v0.17.7 / MAF python-1.8.1+dotnet-1.10.0 / LangGraph 1.2.9 /
> Grok harness / CrewAI 1.15.2a1 / eino v0.8.0（全部本地源码逐行核对）

---

## 1. 问题

### 1.1 业务侧要什么

三件事：列出某会话的历史对话、接着上次那轮继续聊、回看某轮谁说了什么。

### 1.2 框架侧缺什么

**缺口一：跨 Run 的会话身份。** `cp` 主键是 per-Run 的 `sessionID`、`store` 的 key 前缀是
per-Run 的 `RunID`、`cp.ThreadID` 在 `SaveNS` 里被 auto-fill 成 `sessionID`
（`compose_v2/checkpointer.go:412-413`）——**三处都只能按 Run 切片**。

会话身份其实**已经被声明过了**——`compose_v2.WithThreadIDCtx(ctx, convID)`（`checkpointer.go:698`）。
而且 producer 不只业务：`a2a/server/executor.go:149` 把 A2A ContextID push 进去、
`agui/handler.go:165` 把 AG-UI 的 `in.ThreadID` push 进去，**框架自己就在注入**。

但那个值只被 observability 消费（`GetThreadID` 读出作
`gen_ai.conversation.id`，OTel 重写前叫 `gen_ai.session.id`）、`a2a/server/executor.go:148` 做
push 前守卫、以及 handoff 的持久转交用
（`store.NS("__handoff_active__", name, threadID)`，`agent_v2/handoff_agent.go:605`）——**不进 cp**。

实测：

```go
for _, sid := range []string{"run_1", "run_2", "run_3"} {
    ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv_42")
    compose_v2.Run(ctx, g, cm, WithCheckpointer(cper), WithSessionID(sid))
}
LatestByThread("conv_42")  →  ErrThreadNotFound
// 中断留下 cp 时可见：cp.ThreadID = "run_1" / "run_2" / "run_3"（各自为政）
// 若三个 Run 都干净跑完，cleanupOnSuccess 已清 cp，结论同样是 ErrThreadNotFound
```

**缺口二：完整对话流不跨 Run。** 流本身每轮都在产生且完整（见 §2.1），但随 cp
「成功即清」蒸发——实测成功 Run 结束后 `ExistsNS` 直接返 `false`。

### 1.3 外溢出的两个已存在缺陷

**(a) 多轮对话喂给模型的历史是残缺的。** `prior` 读自 `RunSession.Events`
（`agent_v2/basic_agent.go:806-814`，三个分支 `HistoryForRaw` / `HistoryFor` /
`SelfHistoryCompacted`），而 events 只记首尾。实测第二轮对话时模型看到：

```
[0] user       "A 还有货吗"
[1] assistant  "A 商品还有 3 件"     ← 结论在，来源没了
[2] user       "那给我留一件"
```

上一轮调过 `查库存` 工具、工具返回「库存只剩3件」——**这两条模型完全看不到**。后果不只是
信息少：模型无法判断「3 件」是刚查的还是编的，也就无法判断该不该重查。

**这个组合在调研过的外部实现里没见到**：要么只有一份消息列表（模型输入就是它本身），
要么有独立事件流但事件流是完整的。「完整的那份存在、却喂另一份残缺的」——查过的几家都没有。

根因是契约不一致：**事件流被当成「审计摘要」写（只记首尾），却被当成「完整历史」读。**

**(b) `ListByThread` / `LatestByThread` 名不副实**（godoc 已于本轮改准，行为未动）：承诺
「列出历史 run」，实际因「成功即清」只能返回当前挂起的。

---

## 2. 关键事实（全部实测，不是推断）

### 2.1 完整对话流一直在，只是没被留下

`basicMessagesCh`（`agent_v2/basic_builder.go:188`）是 `compose_v2.Append` reducer：

| 写入点 | 写什么 |
|---|---|
| `preamble`（写入在 `basic_builder.go:604`） | 初始 messages（含 user 输入） |
| `modelNode`（`:424`） | 只写 `out`（一条 LLM 产出） |
| `toolsNode`（`:547`） | 只写 `results`（工具结果） |

**4 层压缩只作用于 `modelFn` 内的局部变量，从不写回 channel**——所以 channel 里累积的是
完整、未压缩、append-only 的消息流。

而 `postambleNode` 拿到的正是 `Get(chs, basicMessagesCh)` 全量（`basic_builder.go:622`），**那也正是
`WithStoreMemory` 已经在用的位置**（`agent_v2/basic_agent.go:1051` `st.Put(...)`）——它只从中
挑走最后一条 assistant。

> **差距不是「没有历史」，是「守着完整历史只舀了一勺」。**

### 2.2 归属是恒等式，不是需要记录的数据

嵌套实测（父 agent 通过 `AgentAsTool` 调子 agent）：

```
[parent] channel:  user "父的问题" → assistant(tool_call) → tool "子agent的答复"
[child]  channel:  user "去做"
```

每个 agent 的 channel 里**只有它自己的对话**——子 agent 的产出在父那边是 `tool` 角色（父调了
个工具、拿到结果），这在对话语义上正确。所以：

```
channel 里每条消息的归属 = 拥有这个 channel 的那个 agent
```

channel 是 agent 私有的，归属恒定，**不存在「丢了」这回事**。

### 2.3 执行正确性不依赖历史内容

实测：清空 `sess.Events()` 前后，Resume 时工具都执行 **2 次**（by design，toolsNode 整节点
重跑、业务工具自负幂等）。**工具重跑由 cp 的 frontier 决定，与历史无关。**

这条免疫是连锁的：OpenAI 那 837 行 `session_persistence.py` 里处理孤儿 tool_call、去重游标、
rewind 补偿的一大半机制，我们都不需要。

### 2.4 events 是对外接口契约，不能塞满中间过程

`Agent.Run` 的返回值就是 `[]*AgentEvent`（`agent_v2/agent.go:19`），`FinalMessage` 在非测试代码里有约 **88 处调用**
（grep 命中 94 行，其中 6 行是注释；含测试 200+）。所以 events 不能删、也不该当成完整历史的载体。

（`FinalMessage` 本身是反向扫找 `Action.Exit` 标记，`agent_v2/event.go:99-107`，所以**即使**
events 变多也不会干扰它——这条为 §6.2 的备选路径留的余地。）

---

## 3. 最终形态

**真相源的位置学 LangGraph，持久化的分层学 OpenAI/MAF。**

```
compose_v2.WithThreadIDCtx(ctx, "conv_42")      ← 会话身份唯一声明入口
        ↑  producer 三路：业务直接 push / a2a-server 注入 ContextID / agui 注入 threadId
        │
   ┌────┼─────────┬──────────────┐
   ▼    ▼         ▼              ▼
observability  handoff      历史层(新)      cp ✗ 不消费
gen_ai.conversation.id  store ns   按会话组织     只管单次 Run 断点续跑


channel state（basicMessagesCh）── 唯一真相源（完整、归属恒定、框架管）
     │
     ├─→ 进 cp        ：单 Run 内断点续跑，成功即清        ← 与 LangGraph 同
     │
     └─→ 每超步抓走   ：写进历史层，跨 Run 活下来          ← 我们特有
              │
              ├─→ prior 从这读（修 §1.3(a)）
              └─→ 业务回看（opt-in 增值）

events ── 真相源的投影：接口返回值 + 审计，**保持现状不动**
```

### 3.1 为什么不走 LangGraph 那套（历史 = cp 链）

| 它的代价 | 我们为什么付不起 |
|---|---|
| checkpoint 无消息级归属（`CheckpointMetadata` 只有 source/step/parents/run_id） | 我们有 9 编排，owner 归属是核心场景 |
| 40 条消息的会话写 240 行、列一次历史反序列化 1000 个消息对象 | 我们也是每超步 cp、messages 也在 channel 累积 |
| 要保留全链 ⟹ 放弃「成功即清」 | 那是防同 sid 二次 Run 载到旧 cp 的正确性机制 |

**外部有过同形状的尝试并被判为弯路**：`ShallowPostgresSaver`
（`PRIMARY KEY (thread_id, checkpoint_ns)`，只留最新）自 2.0.20 起标弃用、警告称 3.0.0 移除
并改用 `durability='exit'`。而 `durability='exit'` 换的是**写入频率不是保留条数**——退出时
照样写新行，跨 invoke 累积，拿不回"只留最新"。

（两处措辞校正：截至本地 checkout 的 checkpoint-postgres **3.1.0**，`ShallowPostgresSaver`
**仍在仓、仍导出**，承诺的移除尚未落地；另外 `prune` / `delete_for_runs` / `copy_thread`
是**基类各有一行 `raise NotImplementedError`、七个 saver 全部没覆写**，不是"六个文件里全是
NotImplementedError"。）

### 3.2 为什么不走 eino 那套（事件流完整 + 事件流喂模型）

eino 的形态本身没问题（它的事件流确实完整：每次 model 输出、每个 tool 结果都发事件），
但**它的 `runSession` 是框架管的，我们的 `RunSession` 是业务持引用的**。

实测这个分裂的后果——业务不传 sess 的 Resume：

```
err = node "tools": tools: interrupt: ask_user    ← 答案给了，却又中断了
```

cp 里有 channel（框架恢复了执行状态），新建的空 sess 里没有 events。现在 events 残缺反而
「掩盖」了这个问题（本来就没多少内容，丢了影响有限）；一旦它变成模型输入的真相源，
**「历史不全」就升级成「执行都不对」**。

而 eino 形态的好处（`AgentEvent.AgentName` 现成）在 §2.2 之后已经不需要——归属是恒等式。

---

## 4. 接口

```go
// opt-in：不配就一行不写、零开销
// owner 进签名、无默认值——理由见下
agent_v2.WithHistory(hs HistoryStore, owner string, opts ...HistoryOption)

// 接口定义在使用方（agent_v2），实现方可以是 memory / redis / pg
type HistoryStore interface {
    // Append 追加消息。**约定 append-only** —— 这是「增量写不需要游标」的前提（见 §5.3）
    Append(ctx context.Context, key HistoryKey, msgs []*schema.Message) error
    Load(ctx context.Context, key HistoryKey) ([]*schema.Message, error)
}

type HistoryKey struct {
    Owner  string // 租户/用户。**进签名、无默认值**
    ConvID string // = WithThreadIDCtx 的值
}
```

> 2026-10 起 `Load` 改成 `Load(ctx, key, HistoryQuery) (HistoryPage, error)`：分页从末尾往前读（`Before` / `Limit`），
> 按消息 ID / 任务章 / 归属过滤（`MessageID` / `Task` / `Agent`）——长会话每次 Run 不再整场拉一遍，去重直接问后端。
> 见 [usage-guide.md 会话历史节](../v2/usage-guide.md)。本节保留当时的设计。

**`Owner` 无默认值**的理由来自 MAF 的还债：它的 `AgentSessionStore` 签名是
`(agent, conversationId)`，类 doc 自己承认 conversationId「来自 wire，是续接标识符不是授权
令牌」「猜中别人的就能拉到别人的完整历史」，后来在 Foundry.Hosting 补了带 `userId` 的版本，
注释写着 *"required (no default) so every caller consciously decides the scope"*。

注意它的 `userId` 是**可空**类型（"required" 指调用点必须显式写出、没有默认值，不是必须
非 null；注释里还说 "Pass `null` only when there is genuinely no user context"）。我们的
`HistoryKey.Owner` 是不可空的 `string`，**比它更严**——单租户场景填固定值即可，但不给
"忘了填"留口子。

**不复用 `store.Store`**：那层是「提炼后的知识」（`WithStoreMemory` 走 `Search` 检索片段），
历史是「逐字时序回放」——**读取形状就不同**：一个按相似度返片段集合，一个按时序返有序流。

更硬的理由是**写入语义冲突**：知识层为了保持"当前认知"必须能改写和删除既有条目（CrewAI 的
consolidation plan 就带 `update` / `delete` action，会把先写入的记录改掉或删掉），而历史层
的全部价值建立在 **append-only** 上——一条写进去就不能再变。把历史放进一个会自我改写的
存储里，「回看上次那轮说了什么」这个需求从根上就不成立。

**不提供 `ListConversations`**：调研过的几家协议接口里都没有这个方法（只见到某个具体后端
自己私加了一个非协议的枚举）。业务要列会话，自建索引或用 session id 命名约定。

---

## 5. 决策与依据

### 5.1 写入位置：channel 写成功之后

新消息只在三处产生（§2.1），历史就在同一处写。

⚠️ **历史写入必须跟 channel 写入同生共死**：`toolsNode` 整节点失败时 writes 会被丢弃（这正是
Resume 要重跑全部 tool 的原因）。若历史先写、channel 回滚，Resume 重跑后同一条消息会进历史
两次。所以是「channel 写成功了才写历史」，不能并行发，也不能在 `runOne` 里每个 tool 返回就写
——那时还不知道这批会不会整体回滚。

### 5.2 写入时机：增量、每超步、异步不阻塞

调研的几家里多数默认增量（每 turn / 每超步 / 每条消息 / 每 flow method 各有），只有一家
默认整轮一次性。

> ⚠️ 这条只逐行核过其中两家的默认值，其余是调研转述**未逐个复核**——当趋势参考，不当已验证事实。

真做成可配的两家**默认值恰好相反**：MAF 默认一次性、LangGraph 默认增量。原因是定位不同——
LangGraph 的 checkpoint 要支撑 Resume，一次性对它是功能残缺；MAF 的 history 是纯记录层。

**借 LangGraph 的一条拆分**：它的 `sync` / `async` **粒度完全一样**（都每超步写），区别只在
主流程等不等，**默认 async**：

```python
# libs/langgraph/langgraph/pregel/_loop.py:1530-1547（sync；async 版在 :1783-1802）
# 注：:1202-1209 是把上一个 future 传进去排队的调用点，不是这个函数本身
def _checkpointer_put_after_previous(self, prev, ...):
    try:
        if prev is not None:
            prev.result()      # 等上一次（FIFO 保序）
    finally:
        checkpointer.put(...)  # 上次失败了这次照写，异常仍浮出来
```

我们的历史层是旁路 + 写失败吞掉（§5.4），**更该异步**，主流程一次都不等。

### 5.3 增量写不需要游标

查过的实现里只见到一家真做了游标 + 去重，而它的 docstring 明写动机是**流式重试去重**不是
崩溃恢复：

> *"keeping track of what was already saved so retries during streaming do not duplicate tool
> outputs or inputs."*

其余几家靠 append-only 存储天然规避（**未逐个复核**）。结论是：只要接口约定 append-only，
这个代价就能被设计掉——所以 §4 这么定。

### 5.4 写失败：吞 + warn，每次都发

调研的几家里多数硬抛，只有一家吞。**但分歧的原因正好说明我们该学吞的那家**——硬抛的那些，
历史都在主链路上（是 Resume 的唯一依据、或是下一轮 LLM 的输入来源），丢了下一轮就错；而吞
的那家，它的历史文件是旁路（内存里另有一份供 LLM 用）。

**我们跟旁路那家同档**：§2.3 实测过执行正确性挂在 cp 的 frontier 上，历史丢了不影响跑对。

> ⚠️ "多数硬抛"只核过部分实现，未逐个复核。

Grok 在**类型层面**杜绝传播——三个方法全返 `()` 不返 `Result`，后台 worker 统一 `warn!` 后
继续（这个模式在文件里重复二十多次）。

三条可直接抄的：

1. 级别用 **warn 不用 debug**
2. **每次都发不做抑制**——MAF 有条注释把理由写透了：不常见 + 容易漏 + 静默 = 必须每次警告
3. **改状态失败 warn / 读探查失败 debug** 二分（OpenAI 的做法）

**不新造失败回调**：调研中没见到有人为此专门造回调（未穷举），走已有的 hook / metric
通道即可。

### 5.5 内部决策 LLM 的对话不进历史

**主流做法是挡在外面**，手段三种（按侵入性排序）：不给 session（OpenAI 的 guardrail 全
文件零 session 字样；`as_tool` 是默认不给、可显式传）、给独立 session 或独立 key（MAF group
chat manager 有自己的 `AgentSession`；LangGraph 的 `structured_response` 读 messages 写另一个
key）、同容器分层 + 显式提权（CrewAI 三层 + `visibility` 默认 private）。

**但不是零例外——有一个明确反着来的。** MAF 的 Magentic orchestrator 把 manager 自造的
prompt 连同 LLM 输出**四条全 extend 进共享 `chat_history`**
（`_magentic.py:632`），注释写明理由：

> `# Also store individual messages in chat_history for better grounding`
> `# This gives the progress ledger model access to the detailed reasoning`

**这是一条真实的取舍分歧，不是疏漏**：Magentic 的下一个内部决策者（progress ledger）需要
看到上一个决策者的推理过程，所以它选择污染主历史换 grounding。

**我们不学它**——理由是我们那三个组件的下游不需要看彼此的推理：routing 的下游是被选中的
分支、reflect 的下游是下一轮 actor、plan 的下游是执行器，都只需要结论。**但如果将来有
「决策链」型编排（后一个决策依赖前一个的推理过程），这条要重新评估。**

⚠️ 我们这三个组件都拿到了父 session，但**形态不同、要挡的东西也不同**：

| 组件 | 调用 | 内部形态 | 实施时要做什么 |
|---|---|---|---|
| Routing classifier | `routing_agent.go:584` `classifier.Run(subCtx, parentSession, ...)` | 是 `Agent`（内部 BasicAgent 图，**有自己的 channel**）| 挡住它的 channel 被历史层抓走 |
| Reflect evaluator | `reflect_agent.go:524` `reflectorAgent.Run(rfCtx, parentSession, ...)` | 同上 | 同上 |
| Plan planner | `plan_agent.go:699` `p.planner(nodeCtx, session, ...)` | **是函数不是 Agent**——内置 `LLMPlanner` 直接 `boundModel.Generate`（`llm_planner.go:136`），**没有图、没有 channel** | 无 channel 可挡；只需保证它的 `Generate` 不经历史层。业务自定义 Planner 内部跑 agent 时才回到上面那档 |

这跟 `WithSubAgentStoreScope` 那套「sub-agent 不重复持久化」是同一类问题，应一并处理。

---

## 6. 实施步骤

### 6.1 立历史层

1. `HistoryStore` 接口 + `WithHistory` option（§4）
2. 三个写入点接上（§5.1），异步 worker + FIFO 保序（§5.2）
3. 内存参考实现 + conformance 套

### 6.2 `prior` 换源

把 `basic_agent.go:806-814` 的三个分支从读 `session.*History*` 改为读历史层。

**历史 opt-out 时回落到现状**（读 events，残缺）。这是有意的降级，须在 godoc 和
usage-guide 里写明——不能让业务不知道自己关掉了什么。

> 备选路径（**未采纳**，留档）：让 events 变完整（每轮 model 输出 + 每个 tool 结果都
> `AddEvent`），那样 `prior` 不依赖历史层就能修。已验证 `AddEvent` 并发安全（32 goroutine
> `-race` 干净）、三种历史视图在完整 events 下行为正常、`FinalMessage` 不受影响。**否掉的
> 唯一理由是 §3.2 那个分裂**——events 是业务持引用的，不传 sess 的 Resume 会出错。若将来
> `RunSession` 的生命周期改由框架管，这条可以重新评估。

### 6.3 删 cp 上的伪历史 API

`cp.ThreadID` 字段（`checkpointer.go:412-413` 写、`:565`/`:593` 读）+ `ListByThread`
（`:105`）+ `LatestByThread`（`:120`）+ `ErrThreadNotFound`（`:312`）是个**闭环孤岛**：字段只
服务那两个方法，那两个方法零生产调用（只有 example 和 conformance）。

**保留 `WithThreadIDCtx` / `GetThreadID`**——那是会话身份的唯一声明入口，trace 聚类和 handoff
都靠它，跟 cp 字段是两个东西。

连带处理清单（**比看上去长，逐项都要动**）：

| 类别 | 具体 |
|---|---|
| **仓内 backend 实现** | `redis/checkpointer_query.go` + `sql/checkpointer_query.go` **自带完整实现**；还有 redis 的 thread ZSET（`key.go` / `checkpointer.go` 写索引）和 sql 的 `thread_id` 列 + 索引（`checkpointer_schema.go`）。它们是独立 go module，**不属「业务自研」**，删接口时必须一起动（含 SQL schema） |
| conformance | `checkpointer_conformance` 的 Thread 测试 |
| **「10 方法」声明** | 共 5 处：`CLAUDE.md`（两处）、`doc/v2/channel-engine.md`、`doc/v2/migration-history.md`、`doc/v2/skill.md`、`doc/v2/why-channel-based.md`（后者还点名列了这两个方法）|
| example | `example/store/store_threadid_wrap`（它存在的理由就是补 ThreadID） |

**破坏性变更**：业务自研 backend 也要跟着删实现。项目未上线，按规约可直接动。

### 6.4 独立修：Sequential 多记 stepInput

`sequential_builder.go:216` 写 `[]*schema.Message{stepInput, out}`，而 `stepInput` 是
`:183` 用模板拼的（「原始目标：…\n\n上一步（X）的产出：…」）。于是每条产出被**复述两遍**（一次作为
assistant 消息、一次嵌在下一步的 user 模板里），总量翻倍。

> 注：不是平方——`seqAgentLastOutputCh` 是 `LastValue`（`sequential_builder.go:73`，覆盖不
> 累积），`stepInput_i` 只嵌**上一步**产出而非全部历史，所以是 `N·|orig| + 2·Σ|out_i|`，
> 线性。

查过的几家都是**只记实际发生的事，不记派生的中间输入**：节点只返产出 / 只发 model output
与 tool result 两类事件 / 只有「用户消息、模型产出、工具结果」三条写入命令。中间环节的输入
是从这三类派生的，可重算，所以不记。改成只写 `out`——**但 step 0 例外**：它的 stepInput 就是 origMsg 本身（真实用户输入不是派生物），丢了整条流就没有开头。

**这条跟历史层做不做无关，现在就是缺陷。**

---

## 7. 当时的未定项 → 实施时的实际取舍

1. **归属怎么落到历史里** → **已定：`HistoryAgentAnnotation` annotation 键**。写入时
   `stampHistoryAgent` 在一份浅拷贝上盖 `a.config.Name`（不碰原消息——那是 channel 里
   正在喂给模型的同一批指针）。读回时 `historyPrior` 按它区分「自己 vs 别人」。
2. **载体覆盖面** → **已定：只 BasicAgent**。`startHistory` 只在 `BasicAgent.Run/Resume`
   入口调，其余 8 编排不起写入器；嵌套在开了历史的 BasicAgent 下的子 agent 沿用父的
   writer（一场会话一条流）。**已知边界**：`ParallelAgent` 顶层 + 多个 BasicAgent 分支
   各自开历史时，各起各的 writer 写同一 `(Owner, ConvID)`——store 有锁不丢不崩，但分支
   之间的顺序会交错（并发分支本无固有时序，可接受）。
3. **历史视图的去留** → **保留三种 handoff 视图**。`historyPrior` 把历史流投影成
   SelfOnly / Rewrite / Raw，复用 `session.go` 的同一组改写函数（`rewriteCrossAgentMessage`）
   ——行为与 events 那套等价，业务无感知切换。
4. **events 体量** → **不适用**。没走 §6.2 的备选路径（events 保持只记首尾）。
5. **压缩产物的归宿** → **仍开放**。`AutoCompact` 的 LLM 摘要不可重算（有成本、非确定性），
   现在仍是用完即弃。历史层已就位，「摘要要不要跟着存」变成现实问题但尚未拍板：一种做法是
   给业务两个 key 自己选（进 state / 只影响本次），另一种是墓碑法（`_excluded` 标记 +
   存储保留全量 + 加载时投影）——后者不需要回滚补偿，明显更省事。

### 换源打破的那个隐含前提（已修，留档防再犯）

`prior` 从 events 换到历史层，把一个**没写在任何地方的前提**打破了：events 源的 prior
里只有 user + Exit assistant，**没有 `role=tool`**。多处代码依赖它——`dropDanglingToolCalls`
的原注释就写着「今天 framework 路径 events / store 从不含 RoleTool，paired 恒空、**本分支
不可达**」。换源后历史是逐字完整流，tool_call 和 tool_result 都在里面，于是：

- 第 0 层按**条数**截断会把一对切开 → 孤儿 tool_result → **真实 API 400**（不是
  prompt-too-long，第 3 层 retry 兜不住 → 会话永久卡死）
- marker 前置把带 `SkillBodyAnnotation` 的 tool_result 抽到队首 → 跑到它的 assistant 前面
- Raw 视图下父的 tool_call 与 tool_result 之间夹着子 agent 那一段

修法是在 msgs 最终汇合处**按声明重建配对块**（`dropDanglingToolCalls`）：所有结果都从原位
移除、由声明它的 assistant 紧跟输出，缺的补占位。无论上游怎么打散，出口恒满足「每条
role=tool 紧跟在声明它的 assistant 之后、按声明顺序、一个不多一个不少」。回归见
`agent_v2/history_pairing_test.go` + `history_shape_matrix_test.go`（22 个形状的矩阵）。

#### 打破的**不止**这一条

上面这条是最先发现的，也是当时唯一记下来的。后续复查又揪出四条同源的——全都是「换源前
无所谓、换源后成了正确性判据」的东西，全都**静默失效**（不报错、不空指针，只是模型看到的
东西不对）：

| 原本无所谓的前提 | 换源后的后果 | 修法 |
|---|---|---|
| 历史里没有 `role=tool` | 配对被打散 → 真实 API 400 | 按声明重建配对块 |
| 本轮 input 由 `HandoffAgent` 写进 events、`prior` 能捞到 | 转交的共享视图下模型**收不到本轮问题**：第 1 轮空 prompt，第 2 轮拿到上一轮的问题，对着旧问题作答 | 判据从「什么视图」改成「`prior` 从哪来」 |
| agent 名只是日志标签 | 不显式起名时历史读不回来（自动名每次构造都变），且「读成功只是没内容」不回落 events → **开了历史比不开更失忆** | 构造期 fail-fast + 嵌套路径运行时提醒 |
| 读和写各走各的配置 | 嵌套时子读一个从没人写过的 store，每轮失忆 | 读对齐到 ctx 里那个写入器 |
| 写是异步的、但读之前总会落盘 | 同一个 Run 内子读跑到父的异步写前面（后端 200µs 延迟即必现） | 读前排空 |

`tool_call` id 跨轮复用还另外暴露了清洗层的一个独立缺陷（索引按 id 只记一条结果 → 第二轮
的真结果连同整条 assistant 静默蒸发）。

**教训不是「要枚举谁依赖了旧形状」——那句当时就写下了，然后只枚举出一条。** 真正管用的是
两件事：

1. **换源之后，把「这个位置的数据从哪来」当成一个新维度，逐个消费点重问一遍**。上表五条
   里有四条是同一个句式：「X 原本恒成立，所以代码里直接假设了 X」。找它们靠的不是想，是
   对着每个读 `prior` 的地方问「它现在可能是什么形状」。
2. **拿变异体量测试的强度，别信「跑绿了」**。这五条里三条在发现时是**零覆盖**，另两条有
   测试但都是假绿（fixture 恰好落在安全形状上：内存后端 0 延迟掩盖了异步窗口、跨 Run 读
   掩盖了 Run 内的排空缺口）。

### 实施后审计发现的边界（记录，非缺陷）

- **ReflectAgent 的 actor 输入会把 reflector 的批评带进历史**。reflector / evaluator 自己的
  对话已被 `WithoutHistoryCtx` 摘除，但 actor 收到的合成输入含「原目标 + 上轮输出 + 评审
  反馈」（RF-M2 协议，user 角色），actor 是 BasicAgent 时其 preamble 会原样记下。
  **这是对的**——那是 actor 真实的输入，不记则历史里 actor 的产出没有上下文；且 critique
  本身不在历史里（reflector 已摘），不记就永久丢失。区别于 §6.4 Sequential 的派生
  stepInput（复述的是**已在历史里**的产出，纯重复可重算）。
- **嵌套沿用父 writer 时，子 agent 自己配的 `WithHistory(store, owner)` 被忽略**：消息写进
  父的 store + 父的 (Owner, ConvID)。这是「一场会话一条流」的直接结果，靠归属标注区分谁
  说的。业务若期望子 agent 独立存档，需让它不嵌套在开了历史的父下面。
- **编排合成的派生 input**（已处理，判据见下）。Loop 那种「把上轮产出原样转手」的靠角色判断
  挡住（转手的是 assistant 消息）；Sequential / Reflect / Plan 用 `schema.UserMessage(...)`
  **合成**的那些，角色上跟真实用户输入无从区分——判断「这是谁造的」的信息在编排层，所以由
  编排在源头打 `DerivedInputAnnotation`，preamble 据此跳过。

  **判据是「拼进去的东西是不是已经在历史里」，不是「是不是框架拼的」**：

  | 编排 | 派生输入的成分 | 结论 |
  |---|---|---|
  | Sequential | 原始目标 + 上一步产出 | 两样都在历史里 → 纯重复，**标** |
  | Reflect | 原始目标 + 上轮输出 + **评审反馈** | critique 来自被摘除的 reflector → **不标** |
  | Plan | 原始目标 + **你的任务** + 各步产出 | 任务描述来自被摘除的 planner → **不标** |

  后两者的派生输入是那些内容**唯一的落脚点**：内部决策 LLM 被 `WithoutHistoryCtx` 挡在历史
  外，plan 和 critique 本身不进历史，标掉之后历史里只剩「actor 说了 A 然后说了 B」，看不出
  中间发生过什么。

  Sequential 这一条不是新决定——把派生输入移出它自己那条消息流（`seqAgentMessagesCh`）时就
  判过：复述两遍 + 以用户口吻再现别人的话，而它随时可从「原始目标 + 各步产出」重算。历史层
  是后开的第二条记录，当时没继承，这次补上。
- **非 BasicAgent 编排下一场会话有多个 writer**：`startHistory` 只在 `BasicAgent.Run/Resume`
  调，所以 Parallel / Sequential / Loop / … 底下的每个叶子 BasicAgent 各起一个 writer 写同一
  `(Owner, ConvID)`。store 有锁不丢不崩，但**跨 agent 的落盘顺序不确定**（实测 ParallelAgent
  两支跑两次结果不同）。默认 `SelfOnly` 视图按 owner 过滤，所以喂给模型的那份看不出乱序；
  受影响的是「业务回看历史」和 handoff 的 Raw / Rewrite 视图。
- **`CondFailRerunFrom` 重跑已成功的节点会让那段历史进两遍**。`recordHistory` 的「节点成功
  后才写」只防住「节点自己失败回滚」，防不住「成功之后被框架决定重跑」。影响面限于业务
  GraphAgent 显式声明 `RerunFrom` + 节点内嵌开了历史的 BasicAgent。
- **Abort 之后半截对话留在历史里**（append-only 的必然）。那条孤儿 assistant tool_call 会被
  `dropDanglingToolCalls` 剥掉不至于 400，但**被放弃的 user 请求还在**——下一轮模型可能去
  执行它。
- **压缩产物不进历史 ⟹ 每个 Run 重做一遍压缩**。MicroCompact 的 offload 每 Run 换新
  namespace（按 sid 隔离）重写一遍，数据量 ~ O(Run 数 × tool_result 数)，而 offload 数据不走
  cp 自动清理、业务按会话根本清不干净；AutoCompact 每 Run 为同一段早期对话重新付一次摘要
  钱。这是上面第 5 条「压缩产物的归宿仍开放」的直接代价——换源把它从理论问题变成了每轮都在
  付的实际成本。
- **`HistoryCompactor` 的契约没跟着换源更新**：godoc 只说入参是「当前累积消息」+「user/assistant
  对数」，没提入参可能含 tool_call / tool_result、也没要求返回值保配对。业务照字面写个按条数
  切的实现就会造出孤儿（框架侧的 `dropDanglingToolCalls` 会兜住不至于 400，但那条工具结果
  就丢了）。

---

## 8. 调研过程中被推翻的判断（留档，防再犯）

| 一度认为 | 实测推翻 |
|---|---|
| 「我们没有持久化」 | 三层存储都在，缺的是会话维度 |
| 「完整视图只在 modelNode 入参那一瞬间存在」 | 一直在 channel 里，append-only |
| 「每个 BasicAgent 各自抓各自的 channel = 天然按 agent 分层」 | 父 events 里子 agent 完全不可见，`AgentName` 全是 parent |
| 「5 种 owner-aware 视图是相对 OpenAI 的优势」（实为 4 种）| 嵌套下没有可过滤的内容（→ §7.3） |
| 「prior 换源到 channel」 | channel 跨不过 Run（成功即清），必须先有历史层 |
| 「走 eino 形态（事件流喂模型）」 | 它的事件流框架管、我们的业务管，不传 sess 的 Resume 会出错 |

**共同教训：移植设计要连着生命周期一起看。**两次栽在「同名不同命」上——LangGraph 的 state
跨 thread 活着 / 我们的 channel 随 cp 清；eino 的事件流框架管 / 我们的业务管。

---

## 9. 四轮补丁的失效模式清单（2026-07-27，重新设计的输入）

这一层从落地到现在打了四轮补丁，**每一轮都是在修上一轮引出的问题**。把它们并排列出来不是为了记账，是因为「改一个引出一个」这个模式本身就说明**写入所有权的架构没定对**——下一次动它之前，这张表是必须一次性满足的约束集。

当前状态：第 5 轮的修复（递归探测）**已撤回**（`git reset` 掉 `2d2fc22`）——它自己引入了两个更严重的问题，留着比不修更糟。**表里 1-5 现在都是开着的**。

| # | 症状 | 当时的判据 | 为什么失效 |
|---|---|---|---|
| 1 | 归属用 `Agent.Name()`，同名 agent 撞车、历史合并成一条流 | agent 名 | 框架从不校验 agent 名唯一 |
| 2 | 外部 agent（远程 / 自研 / 别的框架的壳）跑完什么都不留，整条链断 | —— | 写入点在 BasicAgent 的节点里，而编排收的是 `Agent` 接口 |
| 3 | 转交多跳，同一条 input 记 N 份 | 读历史看 nonce 在不在 | 顺序多跳成立 |
| 4 | 并发扇出，同一条 input 记 N 份（实测 1/2/2/3/1/2/2/3，每次不一样） | 同上 | 分支同时读，谁都还没写完 → 都判「没记过」 |
| 5 | 多套一层编排就分裂成 N 个写入器（`Handoff(Parallel(Seq(Basic)))`，实测带屏障 20/20 复现） | 只探测直接子 | `historyConfigProvider` 只有 BasicAgent 实现，中间的编排层探不到 |

### 第 5 轮的修复为什么撤回

改成递归探测（让 7 个编排也实现 provider）之后，独立复核实测出两个新问题，都比原问题严重：

- **进程直接死**：环停在一条**永远不会被选中**的 Routing 分支里时，`Run` 正常返回，而探测是静态走结构的、会走进去 → 栈溢出。原 commit 的理由「那样的编排跑起来本身就是无限递归」**不成立**。而且 Go 的栈溢出是 **fatal runtime error 不是 panic**，各 Run 顶上那个 `defer recoverToErr` 兜不住。
- **中间层重复记录**：写入器外移**激活了原本是 no-op 的编排边界补记**（`w == nil` 早退没了），而它的「这个位置记过没」是**精确匹配、不认后代**——`br1` 的边界看不到后代在 `br1/s1` 写过。实测 `Handoff(Parallel(Seq(Basic)×3))` 从 5 条变 8 条（每个 final 记两次），`Parallel(Handoff×3)` 从 6 条变 12 条。全套测试绿，没有一条覆盖它。
- 附带：共享 sub-agent 实例让扫描指数级（实测深度 22 的共享 DAG 让一次 Run 从 336µs 变 52ms），而那正是**没配历史**的默认路径。

### 还开着的其它几条

| # | 症状 | 触发条件 | 备注 |
|---|---|---|---|
| 6 | 编排边界补记仍用「读历史判重」（第 3 轮那套判据） | 并发编排里混有非 BasicAgent 执行体 | 内存后端 + 自然时序下 0/40 不复现，**但真实 5ms 后端延迟下 17-18/20**——不是屏障造出来的假象，是网络后端上的常态 |
| 7 | `Parallel(Handoff×3)` 构造上无法去重 | 并发分支各自套 Handoff | nonce 在 `HandoffAgent.Run` 铸造，而 Parallel 发给各分支时那条 input **还没有 nonce** → 三个分支各铸一个。**且三个分支确实是同一场对话**（同 `HistoryKey`，Rewrite 模式下互相读得到对方消息，实测 Run 2 每个分支的 prompt 里「原始问题」×3）——SelfOnly 那条「每分支各记一份是有意的」豁免**不适用** |
| 8 | 去重把「后端写失败」从"少一份"变成"彻底没有" | 认领成功的那个分支撞上后端抖动 | 认领集是 per-writer 的，所以**限于一个 Run**，下个 Run 的读侧判据会重新记 |
| 9 | GraphAgent 交不出静态子列表 | `Handoff(Parallel(GraphAgent(Basic)))` | 它的 sub-agent 跑在业务自己写的 NodeFunc 里，框架没有清单可列——**任何基于静态结构的探测都穿不过这一层** |

### 这张表暴露的共同点

1-5 全都是「**多个写入者需要互相协调**」的不同表现，6-9 是它的余波。四轮补丁分别试过：换身份（agent 名 → 位置路径）、加补记、给消息盖 nonce、把判据挪进锁、递归探测——**没有一轮碰到「为什么会有多个写入者」**。

而 9 说明这条路有天花板：只要 `Agent` 是个开放接口、业务能塞任何实现进编排，**任何"框架静态遍历编排树"的方案都存在探不到的层**。

⟹ 下一次动它之前先做业内调研（写入所有权模型），把「一场会话到底该有几个写入者、由谁持有」这个问题回答掉，再一次性设计。别再往这张表上加第六轮。
