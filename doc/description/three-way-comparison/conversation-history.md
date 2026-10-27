# 主题：会话历史层（跨 Run 的对话留存）

> 对照对象：**crew-go（ISSUE-98 落地后）/ OpenAI Agents SDK v0.17.x / MAF python-1.8.1+dotnet-1.10.0 /
> LangGraph 1.2.9 / eino v0.8.0 / CrewAI 1.15.2a / Grok harness（xAI，Rust）**。
> 全部读本地 clone 源码，行号为调研当时（2026-07-23）。
>
> **本篇的定位**：ISSUE-98 把会话历史独立成层之后，5 个细节维度的对照调研。
> 设计与决策依据在 [ISSUE-98](../../issue/issue-98-conversation-history.md)，本篇只放
> **三方怎么做的 + 我们的差异 + 由此改了什么**。

## 触发场景

- 改 `HistoryStore` 接口 / `historyWriter` 生命周期 / `historyPrior` 投影
- 改 `dropDanglingToolCalls`、`truncatePriorPreservingMarkers` 等「喂给模型之前」的清洗
- 评估「历史要不要分页 / 要不要删除 / 压缩产物要不要落盘」这类开放项

---

## 一、异步写入器的生命周期与背压

| 维度 | crew-go | eino | LangGraph | Grok | OpenAI / MAF |
|---|---|---|---|---|---|
| 同步/异步 | 异步单 worker | 异步单消费者 | 异步 future 链 | 异步单 actor | **同步 inline** |
| 队列 | **无界**（mutex + slice） | 无界 slice | 无界 executor 队列 | 无界 mpsc | 无 |
| 满了怎么办 | 不会满（无界） | 不会满 | 不会满 | 不会满 | N/A |
| 写失败 | 吞 + warn | — | **出口 re-raise，整个 run 失败** | 吞 + warn | **异常上抛** |
| 谁关闭数据通道 | **无人关**（closed 标志 + Cond） | 发送方关数据、消费方关信号；`turnBuffer` 干脆用 mutex 队列 | 无 channel | **无人关**（sender drop / 哨兵消息） | N/A |
| 晚到的写 | warn + 丢一批 | `TrySend` 返 false → 进 `appendLate` 兜底 lane | 无此场景 | `let _ = send(...)` 静默 Err | N/A |

**两条共识**（各家独立收敛）：

1. **没有任何一方让「既不是发送方也不是接收方」的第三者去 close 数据 channel。** eino 的
   `schema/stream.go` 把数据通道与信号通道拆开（`closeSend` 由发送方、`closeRecv` 由消费方 +
   CAS 守）；`adk/turn_buffer.go` 面对跟我们**完全同款**的处境（`TurnLoop.Push` 是公开 API 任何
   goroutine 可调、`Close()` 由 loop 退出路径这个外部协调者调）时，直接放弃 channel 改用
   mutex 队列，`TrySend` 返 bool 而非 panic；Grok 的持久化 actor 根本不关通道（Rust 的
   `send` 返 `Result`，send-on-closed 在类型层就不是 panic）。
2. **持久历史的写入器不允许阻塞主流程。** Grok 本地 `updates.jsonl` 无界不丢不阻塞；eino
   `turnBuffer` 无界；LangGraph cp writer 无界。唯一「有界 + 丢最旧」出现在 Grok 的**网络中继**
   `RelaySync`（`MAX_PENDING=256`，丢最旧 64 + warn）——同一个作者，**本地耐久日志无界不丢、
   远端尽力镜像有界丢旧**，按重要性分策略，但两条都不阻塞主流程。

**我们据此改了**（commit `eafb512`）：`historyWriter` 从 buffered channel 换成 mutex 队列、
队列改无界。原来的「有界 64 + 满了阻塞」还藏着一个闭环死锁：worker 卡在慢后端 → 队列满 →
submit 阻塞在节点里 → Run 出不来 → `closeAndJoin` 跑不到 → worker 永远等不到收摊信号。

**没抄的**：批量合并写（Grok 的 `maybe_merge_notification` 合并连续文本 chunk 省往返）。
实测合并后一批里任一条触发后端 panic 会让同批全部陪葬——失败隔离是正确性、减少往返只是
性能。且合并的动机本是缓解积压，队列无界后动机不成立。

**没抄的**：`sync.Once` 守 close。eino 的 `doneOnce` 守的是**广播信号通道**不是数据通道；
Once 只能防 double-close，防不住 send-on-closed，不是这个问题的解。

---

## 二、历史读回来喂给模型之前的清洗

| 清洗动作 | OpenAI | LangGraph | eino | MAF | Grok | **crew-go** |
|---|---|---|---|---|---|---|
| 孤儿 tool_result | 不做 | 不做 | 不做 | 仅 MCP 一路 | **丢**（两档：宽 + 邻接严档） | **丢**（顺序游标） |
| 悬空 tool_call | 丢 call（非顺序敏感） | **报错** | **补占位** | 分组原子 | **补占位**（按原因分档文案） | **中和**（补占位配平；零真结果时保留 assistant + 文本记录、剥 tool_call，**不丢消息**） |
| 同 id 重复 tool_result | 去重保**后**者 | id upsert | 不做 | 不做 | 去重保**后**者 | 隐式保**前**者 |
| 要求邻接 | 否 | 否 | **是** | 分组按邻接聚 | **是**（注释：provider 要求） | 否 |
| 截断保配对 | — | `start_on='human'` 交调用方 | 不删只清内容 | 组原子 | 切点前推跳过 leading ToolResult | `AdjustIndexToPreserveAPIInvariants` |
| 裁后独立校验 | 部分 | 无 | 无 | 无 | **三级**：sanitize → validate 只读复验 → 降级到最小安全形状 | 单点出口清洗 |

**关键一条**（Grok 注释直接写明）：判定「谁被应答了」的多遍扫描**必须用同一条规则**——
宽严错配会让两遍对「谁被应答」产生分歧。我们正好踩过这个：丢孤儿用顺序游标、判悬空用
全局集合，于是错位的一对同时满足两边（结果被丢、assistant 被留）→ 悬空 tool_call 上 wire。
commit `eafb512` 统一成顺序敏感。

**我们比三方严的**：孤儿 tool_result 只有我们和 Grok 处理（OpenAI 的
`drop_orphan_function_calls` 只修 call 侧、不看顺序）。

**我们比三方松的**（已知，未改）：
- **多 tool_call 只部分配对时整条保留**（含悬空那半）。三方无一如此：OpenAI 按 item 粒度
  逐个丢，eino / Grok 给缺的补占位。
- **不校验邻接**。我们的 wire 是 Chat Completions，Grok 注释断言这类 provider 要求邻接
  （未在 dashscope 实测）。
- ~~**取消/中止的 tool 轮次没有补偿语义**~~ —— **已补**（2026-08-04）。原文是：纯 tool-call
  轮被中断后我们把 assistant 整条中和掉，模型看不到「我刚试过调这个工具」。现在保留
  assistant 并写一条中性记账文本（见决策记录表对应行）。**没有照抄两家的补占位**——
  实测发现这个分支的命中方在 events 源下多数是 return-direct 控制流工具（它们真的执行
  过），占位文案必须二选一地断言「执行了没有」，对那半就是撒谎。

---

## 三、截断与压缩

| | 裁剪单位 | 切点保护 | 压缩产物归宿 | token 计数 |
|---|---|---|---|---|
| LangGraph | **token**（`trim_messages`） | `start_on` / `end_on` 角色边界 | 两个 key 二选一：只喂模型 / 覆写 state | 可插精确 tokenizer |
| OpenAI | 条数（默认不裁） | 无 | **写回存储**（clear + add，带回滚） | 服务端负责 |
| MAF | token 或消息数 | **组原子**（assistant + 尾随 tool 打成一组） | **墓碑法**：`_excluded` 打标 + 摘要 insert，存储留全量、读时投影 | 4 字符/token |
| eino | token 或消息数 | 不裁只全换 | 覆写 state，原文指向外部文件 | **混合**：usage 基线 + 增量估算 |
| Grok | 窗口百分比 | **轮边界**（最后一个真实 user turn） | 替换会话 + 外部归档目录 | **混合**：provider usage + bytes/4 |
| **我们** | 第 0 层**条数**；1/2/3 层组 | 第 0 层原来无（已改用配对感知切点） | **不落盘、每 Run 重算** | 纯字符估算 |

**三条差异**：

1. **没有一家默认开着「按消息条数裁」**——OpenAI 默认 `None`、MAF/LangGraph opt-in、eino 默认
   160k token、Grok 按百分比。我们 `MaxHistoryPairs=20` 默认开着是独一份，且名字说「对」、
   实现按 `N*2` **条**算，完整流里一轮带工具 = 4+ 条，窗口实际缩水。
2. **「压缩产物不落盘、每 Run 重算」是少数派**。三家持久化派各有理由：保 annotation / 下轮
   直接就是压缩后的 / 原文归档到外部再告诉模型去哪读。我们每 Run 为同一段早期对话重付一次
   摘要钱；MicroCompact 的 offload 还每 Run 换新 namespace 重写一遍。（ISSUE-98 §7 记为开放项。）
3. **两家生产系统独立收敛到同一个 token 公式**：`provider 上报的 usage 作基线 + 只对基线之后
   的增量做字符估算`（eino `defaultTypedTokenCounter`、Grok `get_estimated_total_tokens`）。
   我们判定路径用纯字符估算（有意，理由见 `autocompact.go` 注释）；但 event 上报口径只取
   基线不加增量，换源到完整流之后系统性偏低。

---

## 四、存储模型与 key

| | 方法数 | 分页/游标 | 删除 | key 维度 |
|---|---|---|---|---|
| OpenAI | 4（+可选 compaction） | limit（"最近 N 条"） | pop_item / clear_session | 1 维 session_id |
| MAF Python | **2** | 无 | 无 | 1 维 session_id |
| MAF .NET | 2 | 无 | 无 | 2 维 (agent, conversationId)，owner 靠装饰器外挂 |
| LangGraph | 10+10 | **真游标**（`before`） | delete_thread / delete_for_runs / prune | 3 维，**无 owner** |
| eino | 2（+可选 Deleter） | 无 | **可选能力接口** | 1 维不透明 id |
| CrewAI | 11+3 | limit/offset | 有 | 层级 scope |
| Grok | ~40（产品级） | 字节 offset | 有（+ rewind marker 逻辑删） | 2 维 (cwd, session_id) |
| **我们** | **2** | 无 | 无 | **2 维 (Owner, ConvID)** |

**接口大小我们跟 MAF Python 几乎一样**（最小档），这是对的——历史层的消费者是 LLM prompt，
不是分页 UI。

**Owner 必填是必要的，不是过度设计**。三方的 history 层**一个都没有** owner 维度，但：
它们要么有平台层兜底（LangGraph）、要么 id 是自己生成的 UUID（MAF/OpenAI）、要么单用户
（Grok）——我们三条都不占，ConvID 是**从 wire 收来的**（a2a ContextID / agui threadId）。
MAF .NET 的 `AgentSessionStore` godoc 几乎逐字写了同一段理由（"续接标识符而不是授权令牌"、
"任何知道或猜中别人 conversationId 的调用方都能续上别人的会话"），且它的默认值是
`Strict = true`（缺 owner 抛异常）——同等信任边界上的判断一致。

**值得注意的两条**：

- **长会话全量读是真代价**。Grok 的 `memory_release.rs` 实测「resume 大会话解析整个
  `updates.jsonl` 是**数百 MB 的瞬时占用**」，为此专门做了 jemalloc arena purge 钩子在 replay
  完成后把页还给 OS，并列了 5 类「memory cliff」。我们 `Load` 每 Run 一次全量（不是每轮——
  preamble 只跑首轮），嵌套编排下每个配了 History 的 BasicAgent 各读一次；截断发生在 Load
  **之后**，传输和反序列化成本已经付掉。OpenAI 正是把 limit hint **下推给了后端**。
- **OpenAI 的 rewind + 去重那套（约 300 行）我们不需要**——但理由要说准：不是「我们更聪明」，
  而是**它的 Session 是可变存储**（有 `pop_item` / `clear_session`），retry 要靠 rewind + 指纹
  校验收敛；我们靠「只在节点成功后写」达到同样效果。**这条在 ISSUE-98 早期被过于笼统地
  写成「那些机制我们都不需要」，而后来发现孤儿处理我们恰恰需要**——留档防再犯。

---

## 五、多 agent 归属与嵌套

| | 怎么标 speaker | 一条流还是多条 | 编排派生 input 进不进历史 | 内部决策 LLM 进不进 |
|---|---|---|---|---|
| crew-go | 消息 annotation | **一条**（嵌套沿用父 writer） | **看成分**：全可重算的不进（Sequential），含被摘除 LLM 产出的进（Reflect / Plan） | 不进（框架调内部决策方时一律摘：分类器 / 评估器 / 规划器 / 路由函数与决策方 / Reflector·EvalFunc / Loop 退出判断 / 回合结束闸门） |
| OpenAI | 运行期字段，**不落盘** | 一条，但 sub-agent 默认不共享 | 落在子 run（默认无 session） | 无框架级 |
| MAF | `author_name` **一等字段且序列化** | 两层（workflow 共享 + executor 私有） | Sequential 不合成；Magentic 合成的以 **assistant + author_name** 进 | **分裂**：GroupChat 选择器不进、Magentic manager 全进 |
| LangGraph | 消息级**无归属** | 每 subgraph 一条 | 业务决定 | 无框架级 |
| eino | `AgentEvent.AgentName` + **`HistoryEntry.IsUserInput`** | 看路径 | **不产生**（sub-agent 收 nil，自己从 RootInput 重建） | 无框架级 |
| Grok | child session 独立目录 | 多条 | 进 child 自己的 | — |

**「只有最外层的输入才是用户说的话」——eino 有显式机制**：`RunPath` 深度为 1 时才写
`RootInput`，嵌套调用一律不覆盖；`HistoryEntry` 的 `IsUserInput` 与 `AgentName` 是**互斥两档**。
Grok 走的是另一条路（子会话独立 bootstrap，`InitialContext` 记下来源与 fork 方式；
`resume_inherited_prefix_len` 只数开头连续的 System 项，不是通用的「继承前缀」标记）。

**四条规避「派生 input 混进历史」的路**：不合成（eino / MAF Sequential）/ 合成了但换角色 +
作者名（MAF Magentic）/ 合成了但落在子会话（Grok / OpenAI `as_tool`）/ 根本不是消息（CrewAI
拼进 prompt 文本）。**我们是唯一同时具备「流水线语义的编排」和「共享会话历史」的**——前者必然产生派生物
（step 间传值不传对话，下一步看什么得显式构造），后者给了它一个可污染的去处。对话语义那
四家不产生派生物；CrewAI 跟我们一样是流水线、拼法几乎逐字同构（`"{task}\n\nThis is the
context you're working with:\n{context}"`），但它**没有跨 task 的共享消息历史**，所以不冲突。

我们的处理是按成分分档（见 ISSUE-98 §7）：成分全在历史里的（Sequential）不记，含被摘除
LLM 产出的（Reflect 的 critique / Plan 的任务描述）照记——那是它们唯一的落脚点。最接近的
先例是 MAF Magentic，它把 manager 自造的 prompt 连同输出一起存进共享历史，注释写明理由是
后续 replan 要读自己上一轮的推理，同样是「不记就丢失」。

**内部决策 LLM 我们跟主流一致**（挡在外面）。唯一反例是 MAF 的 Magentic orchestrator：它把
manager 自造的 prompt 连同 LLM 输出四条全 extend 进共享 `chat_history`，注释写明理由
`# Also store individual messages in chat_history for better grounding`。**这是有意的取舍
不是疏漏**——Magentic 的 manager 不是路由器而是持续跟进的协调者，后续 replan 要读自己上一轮
的 facts/plan。正好落在我们 `WithoutHistoryCtx` godoc 预留的那个例外条件上（「若将来有决策链
型编排，后一个决策依赖前一个的推理过程，这条豁免要重新评估」）。

**三视图（SelfOnly / Rewrite / Raw）不是多余的复杂度**：eino 的 `rewriteMessage` 跟我们的
Rewrite 档几乎逐字同构（`"For context: [X] said: ..."` / `"[X] called tool: ..."`）。差别在
**写时定形 vs 读时投影**——三方在 handoff/fork 那一刻算好一份固化下来，我们每次读按模式重算。
OpenAI 也承认「喂模型的视图」和「留档的底稿」必须分开（`HandoffInputData.input_items` 的
docstring 明写「用它构建下个 agent 的输入，同时 `new_items` 保住 session history 的全量」）
——只是它用两个字段并行携带，我们用一份底稿 + 读时投影。两种都成立。

## 六、转交/扇出时原始 input 的去重

转交多跳（HandoffAgent）里每一跳的 input 恒为同一条原始用户输入，共享视图下这场对话的
原始 input 只该在历史里留一份。这看似小事，却是一个反复踩坑的地方——判据放在会随场景丢失
或算错的载体上，就一个接一个出洞：

| 判据（我们试过的三代） | 状态存哪 | 修好的 | 漏的 |
|---|---|---|---|
| `hops==0`（handoff 跳数）| cp（`handoffAgentHopsCh`）| 单层 | 嵌套每层从 0 起 → 2/3 份 |
| session 级标记 | `RunSession` KV（内存，不走 cp）| 嵌套 | 跨进程 Resume 丢 / session 复用跨 Run 存活漏记 |
| **nonce**（`rootInputNonceAnnotation`，`history.go:922`）| 消息 Annotation（随 cp + 历史序列化）| 嵌套 / 跨进程 / session 复用 / 真实重复不误杀 | **并发扇出** |

### 三方的一致原则：原始 input 不由「各 agent 各自读流去重」

挖三方发现一件事：**没有一家让多个 agent 各自往共享流 append 原始 input、再各自读流去重**
——这个模式并发无解（读-判-写非原子）。它们从架构上避开：

| | 机制 | 并发安全靠什么 | 代码锚点 |
|---|---|---|---|
| LangGraph | input 进共享 `messages` channel，但并发 writes 收集到超步 barrier，barrier 后**单线程** `apply_writes` 遍历 sorted tasks、reducer 一次折叠（`add_messages` 的 ID 去重就在这） | 原子 barrier 归约 | `pregel/_algo.py:232` + `channels/binop.py:123` |
| eino | input **不进 append 流**（`RootInput` 单存一份），各 agent 从它 `deepCopy` 重建视图，只 append 自己的产出到 lane | input 根本不重复写 | `adk/runctx.go:347` + `adk/flow.go:276` |
| grok | 各 subagent **独立子会话**（`child_session_id`），不共享 append 历史 | 不共享流 | `subagent/mod.rs`（27 处 `child_session_id`）|

三条路各不相同，但共同点是**原始 input 不落进「各 agent 各自去重的那条 append 流」**——要么
单份不进流（eino）、要么独立会话（grok）、要么进流但有原子 barrier（LangGraph）。

### 我们的现状与边界

我们把 input 和产出都塞进**同一条 append 历史流**，各 sub-agent 的 preamble 各自 record，
所以才需要「判哪个是第一个记的」。nonce 把这个状态编码进消息本身（随 cp/历史走、扫 prior
判断），修好了**顺序**多跳 handoff 的所有维度（嵌套/跨进程/session 复用），回归测试见
`agent_v2/history_identity_test.go` 的 `TestHistory_Handoff_*`。

nonce 一直是对的，**错的是拿它去问了一个并发下没有定论的问题**：判据原本是「读一遍 prior
有没有同 nonce」，顺序多跳成立（前一跳写完了后一跳才读），并发扇出不成立——分支同时起跑，
读历史时谁都还没写完，三个都判「没记过」于是都记。实测连跑八次记了 1/2/2/3/1/2/2/3 份，
**每次都不一样**；后果是下一个 Run 每个分支的 prior 里那句话出现两三遍，模型看到用户把同一
个问题问了好几次。

`hops`/`marker`/`nonce` 三代判据在这一点上是同一个毛病：**都是「事后读状态」**，而并发下
「读的时候还没人写」跟「真的没人写」长得一模一样。

### 已解决（2026-07-27）：判据挪到写入器的原子认领

不需要新的身份，需要换一个**有定论的问位置**。写入器是单 worker，它的锁是这条流上唯一天然
串行的点——`historyWriter.claimRootInput(nonce)` 在锁里做一次 CAS，第一个抢到的记，其余不记。
读历史那半保留：两个判据各管一段，认领管同一个 Run 内的并发（writer 每个 Run 新建、认领集
跟着重置），读历史管跨 Run（Resume 重跑到同一步时前一个 Run 的记录早已落盘）。

**没有走「让 input 不落进 append 流」那条架构级重构**——那是为「读状态去重」这个错前提准备
的解药，前提换掉之后它就不必要了。

**SelfOnly（无转交）下每分支各记一份不在此列，那是有意的**：那样每条分支流各自完整（有问
有答），重复只出现在「把并发分支拼成一条流看」这个视图上，而拼接本身就没有良定义的顺序
（实测三条流的落盘次序每次都不同）。

---

## 决策记录

| 时间 | 决策 | 对照依据 | 我们的取舍 |
|---|---|---|---|
| 2026-07-23 | 写入器换 mutex 队列 | 无一方让第三者关数据 channel；同款处境下 eino 也放弃了 channel | 采纳。晚到的写从 panic 降级成 warn + 丢一批 |
| 2026-07-23 | 队列改无界 | 持久历史写入器不允许阻塞主流程（三家一致） | 采纳。批数有天然上界，无界不等于无限 |
| 2026-07-23 | **不做**批量合并写 | Grok 有（合并连续 chunk 省往返） | 拒绝。实测一批里任一条 panic 会让同批陪葬；失败隔离优先于省往返 |
| 2026-07-23 | 多遍判定统一顺序规则 | Grok 注释明确「故意用同一条规则，宽严不一致会让两遍分歧」 | 采纳，已修 |
| 2026-07-23 | marker tool_result 连声明者一起保 | 三方无人重排消息（重排必破坏配对不变量） | 采纳，已修 |
| 2026-07-24 | 转交 input 去重用 nonce（跟消息走）；并发扇出记边界不当场改 | 三方一致：input 不由各 agent 各自 append 去重（eino 单存 / grok 独立会话 / LangGraph barrier 归约）| 顺序场景 nonce 是务实档；并发扇出根治要历史层分层，独立立项。见 §六 |
| 2026-07-27 | 并发扇出去重：判据从「读 prior」挪到写入器的原子认领（`claimRootInput`），**不**加消息级 id、**不**做历史层分层 | 五家实测（下节）：加 id 这条路的前提在我们这儿不成立——我们已有等效物，而卡点不是身份是判据位置。LangGraph 自己用一个 commit 写明「把 id 生成放 reducer 里是放错地方了」 | 采纳。写入器是单 worker，它的锁是这条流上唯一天然串行的点；读历史那半保留管跨 Run。**没走架构级重构**——那是为「读状态去重」这个错前提准备的解药 |
| 2026-07-23 | 第 0 层切点走配对感知 | LangGraph `start_on` / Grok 轮边界 / MAF 组原子，三家都有边界保护 | 采纳（复用已有的 `AdjustIndexToPreserveAPIInvariants`，它此前生产零调用） |
| 2026-07-23 | Owner 必填保持 | 三方 history 层都没有，但它们各有兜底而我们没有 | 保持。MAF .NET 同等边界上默认 Strict |
| 2026-07-23 | 接口保持 2 方法、不加 Delete/分页 | MAF Python 同为最小档；Grok 的 ~40 方法是产品不是库 | 保持。删除/过期是后端生命周期职责（对齐 eino 的可选 Deleter 表述） |
| 开放 | 压缩产物是否落盘 | 三家持久化（墓碑法 / 覆写 / 外部归档），我们每 Run 重算 | 未决，见 ISSUE-98 §7 |
| 开放 | `Load` 是否支持 limit hint 下推 | OpenAI 下推给后端；Grok 有字节游标 | 未决。注意 limit 必须是**下界 hint**（「至少返回最近 N 条」），切点修正仍在调用方——OpenAI 自己也是「后端粗切 + 调用方修孤儿」 |
| 取消/中止轮次是否补占位 | **不补占位，改写中性文本记录**（保留 assistant、剥 tool_call）| 两家补占位的前提是「能判定该不该重试」；我们判不了——同一个分支里既有真执行过的 return-direct（结果本就不进 events），也有真没跑完的中断轮，而占位文案必须二选一。改为只陈述可验证事实 + 把决定权交回上下文 | 已决 2026-08-04（`describeUnresolvedCalls`）|
| 开放 | events 能不能当跨轮 prior 源 | 六家（OpenAI Agents SDK / MAF / eino adk / LangGraph / Codex / dsh）prior 的存储单元都是模型可见 wire 单元、tool 往返一等成员；没有一家只存最终 assistant；审计流可比 prior 多、不能比它少 | 未决（2026-09-03）：(a') events 降级为纯审计、`buildInitialMessages` 撤 events 源；或 (b') 跨轮复用 sess 时默认挂进程内 `HistoryStore`。原两条出路（events 补存 tool_result / 推业务开历史层）都不是正解。见结论八 |
| 2026-10-21 | 节点处理完之后在提交之前作废时，续跑不重调模型、不重记历史 | LangGraph 产出过写入的 task 算完成、续跑不重跑（观测回调抛错默认还被吞掉）；OpenAI 把已写过的记进可续跑状态、续跑对账；ADK 已落盘的模型输出续跑不重新生成 | 采纳效果、不改引擎：节点私有态记下这一轮模型调用与「这一批已记过历史」（见下「节点处理完之后作废」一节）。引擎把成功路径钩子 panic 变成节点失败的决定留作开放项 |
| 2026-10-21 | 调用方提供的前文（AG-UI 每次请求带完整 messages）：服务端没有这场对话的记录时用它、不按 agent 过滤；历史层为空时写进去一次（标注、无归属、所有视图可见）；有记录时以服务端为准；只取 user / assistant 文本，system / developer 剥掉 | LangGraph / Mastra / MAF / Strands 都是「服务端有就以服务端为准、为空就用客户端补」；注重安全的实现都不给客户端 system 指令地位 | 采纳主流形状，**不做**按消息 ID 对账与分叉重生成（LangGraph 为此修了一串 bug）。见结论九 |
| 2026-10-22（第五版） | **一次运行只认一个来源**：有拥有者写入器（建写入器时不在任何正在执行的任务里；编排从整棵静态子树借配置）、读得出记录、需要写的前文写成了 ⟹ 以服务端为准，前文只在服务端第一次见到对话时写一次；否则以客户端为准。撤掉逐轮对齐与 `CallerRoundAnnotation` | 第四轮审计三条反例（钩子里的 helper 判不成嵌套丢了答复、双层编排里唯一的写入器被当成嵌套、编辑重发一次后没有写入器的轮永久消失），同一条合并规则四轮都有新反例 | 两份来源没有可靠的对法，按配置落进「以服务端为准」或「以客户端为准」其中一边。代价：服务端为准时客户端编辑不生效；各轮要同一套配置；SelfOnly 成员看不到别的成员接的轮（todo）。见结论九第 2 条 |
| 2026-10-22（已被第五版取代） | 「以服务端为准」的粒度从整场对话改成**一轮**，并且**按读的这个 agent 的视角判**：记录盖所在的轮（嵌套写入器写的另盖标记），读时逐轮对上——这个 agent 看得到开头（拥有者写入器写的同轮同内容 user 记录在它的视图里）的轮用记录，一条记录都没有的轮用客户端文本并由下一个写入器补写，其余按内容合并；轮次倒退（前端编辑重发）判对不上、按追加顺序 | 别家「服务端有就以服务端为准」的粒度是**消息**：LangGraph 认 ID 在不在 checkpoint 里，ADK 有「已处理消息 ID」表——服务端没见过的消息照收。我们按整场对话判，覆盖不全时丢轮次（第二轮审计实测：没有写入器的那一轮丢掉；为并发补的「嵌套写入器不写」又让嵌套写入器下每轮出现两遍、顺序颠倒；开写入器时读失败一次就重复一轮） | 按轮不按消息 ID：`WithCallerHistoryCtx` 是通用 API，调用方的 ID 不一定跨请求稳定（`schema.UserMessage` 每次新铸），按 ID 对的话不稳定的调用方每轮重复一遍；AG-UI 的回传 ID 还要跟流式 messageId、历史记录 ID 三处对齐。代价：要求前文逐次追加，只裁掉一轮时识别不出。见结论九 |

## 并发分支写同一条会话：各家怎么去重（2026-07-27 五家实测）

起点是个具体问题：ParallelAgent N 个分支共享一个 ConvID 时，同一句原始 input 被各分支各记
一份。第一轮查完（三家）得出的结论是「要学 LangGraph 就得先给 `Message` 加消息级 id，属
协议级改动，待拍板」。**第二轮深挖五家之后，这个结论作废了**——记在这里是因为「结论怎么错的」
比结论本身更有用。

### 五家的消息级 id 现状

| | 消息级 id | 怎么用 |
|---|---|---|
| **LangGraph / LangChain** | ✅ `BaseMessage.id`，但**默认 `None`**、构造时不生成 | 唯一深度使用者。`add_messages` 按 id 归并（同 id 走 `merged[existing_idx] = m` **覆盖**而非追加），所以同一条消息被 N 个分支写 N 次归并后仍是一条 |
| **eino** | ✅ 有，但在 `Extra` map 里（`adk` 层的 `_eino_msg_id`），`schema` / `compose` 完全无感知 | 只用来给「同一条逻辑消息的两个副本」对号（event sender 发的那份 vs ToolsNode 追加进 state 的那份），**从不拿两个 id 做比较** |
| **OpenAI Agents SDK** | ❌ `RunItemBase` 无自己的 id | 身份完全落在 `raw_item` 上 = provider 分配的 server id，本质是**往返令牌**不是内部概念 |
| **MAF / MEAI** | ⚠️ `ChatMessage.MessageId` 存在但契约很弱 | 只承诺「单次调用内唯一」，核心聚合只拿它做**流式分界提示**（且只跟上一条比），五个 history provider 一律换 GUID 主键、不按它去重 |
| **CrewAI** | ❌ 消息就是 dict | 身份选在别的粒度（Task/Agent/Crew 的 frozen UUID、LLM 调用的 `call_id`）；对话层用 `(session_id, message_index)` |

### 结论作废的两个理由

**一、我们已经有等效物。** `rootInputNonceAnnotation` 就是消息级 id：随 cp 序列化、随历史
序列化、跨嵌套跨进程都在。加一个 struct 字段不解决任何新问题，反而要付两笔代价——**id 参与
相等性判断会让一整套测试变红**（LangChain 至今做不成「构造期生成 id」，唯一阻塞点就是
`HumanMessage("x") != HumanMessage("x")`，被迫发明了一批 id-盲比较器）；**id 放 map 里会被
通用 concat 静默拼坏**（eino 的 `Extra` 走字符串拼接，只能靠「只往第一个 chunk 注入」的纪律
+ 一个名叫 `TestAttack_ConcatCorruptsIDIfMultipleChunksCarryIt` 的对抗测试撑着）。

**二、真正的卡点不是身份，是判据放错了层。** 身份一直是对的，错的是拿它去问了一个并发下
没有定论的问题（「读一遍历史看在不在」）。换成写入器锁里的一次 CAS 就有定论了——不需要动
协议。

### LangGraph 自己写下的教训（最值钱的一条）

commit `5d5a64120` 的说明原文：

> **ID assignment has historically lived in the `add_messages` reducer, but that's the wrong place: reducer logic runs *after* checkpoint serialisation has already started.**

症状是「同一条 HumanMessage 每次 `get_state` / 每次 Resume 都换一个 UUID」——后台线程先把
`id=None` 序列化了，reducer 才补 id。**补齐点越靠后越容易出这种事**。这条对我们的直接映射：
nonce 打在**编排入口**（HandoffAgent.Run），不是打在写入路径里，方向是对的。

连带的一条：**reducer / 写回路径里绝不能出现随机数**。`uuid4()` 让 `add_messages` 不满足
`reducer(reducer(s,xs),ys) == reducer(s,xs+ys)`，LangGraph 因此必须给 `DeltaChannel` 另写
一个 reducer，并在 docstring 里声明它**不做** missing-id 补齐。我们有 cp 重放 + Resume，
同款约束适用。

### 两个必须避开的坑（实测，不是推演）

**坑一：id 只能给「同一条消息的多个副本」去重，给不了「两个分支各自构造的等价消息」去重。**
用真实 `add_messages` 实测：两分支写**同一个对象引用** → 去重成功（1 条）；两分支各自 `new`
一条内容相同的 → 不去重（2 条）。**先确认自己的场景是哪一个**——我们是前者（ParallelAgent
的 N 个 worker 从同一个 channel 读同一个 `*schema.Message` 指针），所以 nonce 能救；如果是
后者，任何 id 方案都救不了，只有内容哈希能。

**坑二：别让去重正确性挂在对象别名上。** LangGraph 那个 case 能成，是因为 `add_messages`
**原地改调用方的对象**（实测 `merged[0] IS caller obj: True`）：第一次调用把共享对象改出了
id，第二次才看到同一个 id。中间任何一次拷贝或序列化往返都会让它**静默退化成重复**——而
LangGraph 自己的 `tool_node.py` 里就有一处 `deepcopy`。只在「构造时铸 id」的前提下这个坑
才自动消失；我们的 nonce 打在 Annotation 上随消息走，不挂别名。

### 分层：三件事不要合并

LangGraph 的做法（我们最终也落在同一个形状上）：

| 层 | 语义 | 我们的对应物 |
|---|---|---|
| state / channel | 可变的 id-keyed 视图（新 id 追加、同 id **原位**覆盖、tombstone 删除） | `compose_v2` 的 channel + reducer |
| checkpoint / history | 不可变的 append-only 记录 | cp（单次 Run 临时态）+ 会话历史层 |
| **投影 / 写入单点** | 跨路径去重 | `historyWriter.claimRootInput` 的 CAS |

**往 append-only 的历史层塞「按 id 覆盖」会跟它的定位直接冲突**——我们自己的
`history_conformance` 铁律就是「逐字重复的两条都得留着」（模型真的会连说两句一样的话），
MAF 五个 history provider 全是裸 append + 换 GUID 主键、OpenAI 的 session 表是 autoincrement
rowid + 不透明 blob，三家一致。这次去重的是**同一条消息的 N 个副本**（同 nonce），不是两条
内容相同的不同消息——nonce 恰好把这两者分开了，所以不违反。

---

# 写入所有权与边界补记（2026-07-28 六方调研）

起因：我们的「边界补记」判据换了三代，每一代都在某类形状上出错。派了六路读本地源码，问同一组问题：**编排调 sub-agent 时历史归谁写？怎么表达归属？并发扇出记几份？多轮循环怎么不重复？有没有边界补记？**

版本：LangGraph v1.2.9 / OpenAI Agents SDK 0.17.7 / eino adk v0.8.0(`922b6a8`) / CrewAI v1.15.x / Grok harness(Rust) / AgentScope / Claude Agent SDK / a2a-go。

## 结论一：六方都没有我们这个问题，靠三条路之一躲开

| 路 | 谁 | 机制 | 代价 |
|---|---|---|---|
| **封闭接口**——不存在「自己会跑、跑完什么也不留」的 sub-agent | OpenAI Agents SDK | `Agent` 是 `@dataclass`（`agent.py:270`）**没有 `run` 方法**，执行 100% 在 `AgentRunner`；`handoffs/__init__.py:319` 还有 `assert isinstance(agent_base, Agent)` | 异构能力必须降维成 tool，其内部过程对 session 不可见 |
| | eino adk | `flowAgent.subAgents []*flowAgent`（`adk/flow.go:45`）+ `workflowAgent.subAgents []*flowAgent`（`adk/workflow.go:42`）——**类型系统杜绝漏包**，三个入口无条件 `toFlowAgent` | 契约是 **emit or be invisible**：没 emit 不只是审计缺一条，是**下游 agent 根本看不到**（下游输入 = `RootInput + Events` 的纯函数，`adk/flow.go:275-328`）|
| **降维成 tool**——I/O 骑在 tool_call/tool_result 那一对上 | Grok harness | `spawn_subagent` 是普通 tool；**输入**天然在父的 `AssistantItem.tool_calls[].arguments`（模型自己生成），**产出**天然在 `ToolResultItem`，跟 `read_file` 走**完全相同的代码路径**（`tool_calls.rs:2188`），无 subagent 专用分支 | 只有一种编排：ReAct + tool。没有 Sequential/Parallel/Loop |
| | CrewAI Crew/Task | 委派是 tool（`delegate_work_tool.py`），`_get_context` 返回类型就是 `str`（`crew.py:1823`），跨 task 传的是 `.raw` 文本拼接 | 工具往返、tool_call 配对、role 结构**全部丢失** |
| **身份先于流动**——去重键是消息自带的 id，不是对流的外部观测 | LangGraph | `BaseMessage.id` + `add_messages` 按 id upsert（`graph/message.py:216-234`）；`ensure_message_ids`（`pregel/_messages.py:426-461`）在持久化前强制补 id | id 不稳 / `id=None` → 退化成重复（但**只重复不丢失**，且写进了测试）|
| | AgentScope | `Msg.id`（构造时 `shortuuid`）+ `memory.add(allow_duplicates=False)` **默认幂等**（`_in_memory_memory.py:130-132`）| 没有共享历史这个概念，per-agent memory |
| | Claude Agent SDK | `uuid` 幂等键，适配器 upsert（`types.py:1459`）；「这条属于哪条流」是**读时**沿 `parentUuid` 回溯（`_internal/sessions.py:931`），不是写时去重 | |

**我们两条都不占**：`Agent` 是开放接口（有意的，不打算封闭），消息没有稳定身份，于是只能对流做**外部观测**。

## 结论二：确有两家做边界补记，形态跟我们同构

**CrewAI Flow**（`conversational_mixin.py:313-319`）——判据是**计数快照**：

```python
assistant_count = self._assistant_message_count()
result = self.kickoff(...)
if result is not None and self._assistant_message_count() == assistant_count and self._is_public_turn_result(result):
    self.append_assistant_message(...)
```

docstring 原话："promotes the result to an assistant message **when the handler didn't**"。跟我们的回执是同一个思路（跑前跑后比，没变就补）。

两点差异值得记：
- 它数的是**全局 assistant 条数**，判不出「增加的那条是不是这一步的」；我们的回执是 per-ctx 派生的，扇出分支互不干扰
- 它补记前多一层**「这值不值得记」过滤**（`_is_public_turn_result`，`:1114-1125`）：排除 `{"conversation","converse","end","answer_from_history","route_to_flow"}` 这些**路由标签**、排除等于 `last_intent` 的返回值。**我们的 Routing/Handoff 返回路由目标名，有同类风险**

**Grok**（`subagent/mod.rs:1980`）——只在**异步旁路**需要真补记：后台 subagent 的 tool_result 位在 spawn 时被 ack 文本占掉了，真结果没地方放，只能事后合成一条 user 消息补进父流。它判「模型是不是已经从别的途径知道了」用的是**共享判据函数** `consumed_completion_ids`，注释强调「共用同一个函数所以不会 drift」（`tool_calls.rs:1979-1990`）。

## 结论三：LangGraph 在同一个位置明确选择「不猜」

`update_state`（`pregel/main.py:1905-1999`）是它唯一的「外部补一笔」入口，三条约束：

1. 必须显式给 `as_node`，推不出来就 `raise InvalidUpdateError("Ambiguous update, specify as_node")`（`:1934-1935`）
2. `as_node` 必须是图里**真实存在**的 node（`:1936-1937`）
3. 补记走**那个 node 自己的 writers**（`:1955` `flat_writers`），跟正常写入同一条 reducer 路径

我们的边界补记恰恰是在**猜**（判断有没有人记过）。

## 结论四：「单条共享流 + 每条标归属」六方都没有先例

- OpenAI：`RunItemBase.agent` 存在（`items.py:90-93`）但**只活在内存**，落盘只吐 `raw_item`（`items.py:144-153`）。而且它明明有「往 input item 塞私有键再落盘」的现成机制（`_agents_tool_description`，`run_internal/items.py:21-22`）却**没拿它存 agent 名**；`agent` 字段反而配了 weakref + `release_agent()`——被当作**可回收的进程内观测数据**
- LangGraph：位置路径只在**流的信封上**（`StreamChunk` 的 ns 元组 + metadata），从不落到消息对象；测试 `test_pregel.py:7271-7280` 直接钉死这个分界。落盘存活的 per-message 归属只有 `BaseMessage.name` 一个字符串，而它被 agent 名和 tool 名**两套语义抢用**
- eino：归属是 Agent 自报的 `Name()`，**无唯一性校验**（`adk/flow.go:174-186` 无查重，只有 `chatmodel.go:261` 一句 doc hint），同名撞位 `getAgent` 返回第一个匹配会转错目标
- CrewAI Flow：归属只在**私有层**（`agent_threads` 按 agent 名分桶），canonical 层 `.messages` 是**扁平无归属**的，提升靠 `visibility` 显式声明
- Grok：`ConversationItem` 全无归属字段，只有 `UserItem.synthetic_reason` 区分真人输入 vs 运行时合成
- AgentScope：`Msg.name` 是发送者名且落盘——但它**恰恰没有共享历史**，`name` 是给 MsgHub 多方对话用的

⟹ 这不必然说明我们错，但要知道**是条无人走过的路**。CLAUDE.md 里对 `HistoryAgentAnnotation` 的告警（「归属是编排结构的函数，改分支名/加 wrapper 都会让旧历史对不上，失效时是彻底空」）正是这条路的代价；OpenAI 用**根本不承担**来规避它。

## 结论五：两条可以直接照做的

**(1) A2A 协议本身带会话历史，而我们零消费。**

`a2a-go/a2a/core.go:353` `Task.History []*Message`；`core.go:202` `Message.ID`（`messageId`，**无 omitempty = 必填**，"generated by the sender"）。A2A **server 框架**在请求边界统一记进 `Task.History`，判重就是 `slices.ContainsFunc(...m.ID == message.ID)`（`a2asrv/agentexec.go:211-216`），注释写着 "message will already be present if we're retrying execution"——它敢用这个判据是因为紧跟着的 `taskStore.Update(PrevVersion:)` 是**乐观并发 CAS**（`taskstore/api.go:27` `ErrConcurrentModification`），并发安全落在**存储层**而不是判据上。

而我们 `a2a/remote_agent.go:113` 注释明写「input 由 host 层记录」，全仓 `a2a/` 对 `Task.History` **零消费**。⟹ **我们最典型的那个「外部 agent 什么都不留」案例，协议其实给了数据。**

**(2) 双视图分离（喂模型 vs 留档）值得设成默认。**

OpenAI 已把它内建成一等概念：`HandoffInputData.input_items`（喂模型）vs `new_items`（留档），落盘时由 `session_items_for_turn()` 二选一（`session_persistence.py:215-222`），有专门测试守护（`tests/test_handoff_history_duplication.py:307-310` docstring："the key field that separates model input from session history"）。LangGraph 同款：`pre_model_hook` 提供第二条 channel `llm_input_messages`，喂模型视图和落进 `messages` 的审计视图**用两个 key 显式分开**。

但**它们的双轨是 opt-in 的**——OpenAI 的 filter 不声明 `input_items` 时（`turn_resolution.py:562`），过滤会一并作用到当轮落盘。我们的 `WithHandoffHistoryMode` 若设成「改写只影响模型视图、留档恒为原始」且不可关，语义上比它们干净。

## 对我们的启示（该学 / 不该学）

**该学**：
- **共享判据函数**（Grok）：判「记过没」的逻辑只放一处。我们目前在 `settleHistorySlot` 一处，保持
- **补记前的「值不值得记」过滤**（CrewAI Flow）：路由目标名之类的控制流返回值不该当用户可见内容记进历史。**我们没有这道过滤**
- **消费 A2A 的 `Task.History`**：现成的数据，白扔

**不该学**：
- **封闭 `Agent` 接口**：那是我们有意的开放性。但要认清代价——**即便把写入收敛到编排层统一写，只要 sub-agent 的内部轮次发生在框架视野之外，边界补记就消不掉**，只会从「判断有没有人记过」变成「只能记边界，记不了内部」
- **降维成 tool**：Grok 只有 ReAct+tool 一种编排，我们有九种确定性编排，搬不过来
- **靠 `Msg.id` 幂等下沉到存储层**（AgentScope/LangGraph）：跟我们 `history_conformance` 明确要求的「逐字重复的两条都得留」直接冲突。**但「身份先于流动」这个不变量本身值得记着**——LangGraph 敢用 id 做唯一判据，是因为消息进 state 前**一定被强制赋过稳定 id**，判据的可靠性来自那个不变量而不是判据本身多聪明

**决策记录（已推翻，2026-07-28）**：原文写「本轮不引入消息级 id，理由是判据无 mode 门 → Run 2 的新问题不进 prompt」。**那个理由实测不复现**——加上构造期铸造的身份之后连跑三个 Run，每轮的新问题都在 prompt 里。我拿一个没验证的实现细节否掉了根因修复。

现已引入 `schema.Message.ID`（构造期铸造 + 写进历史前在拷贝上兜底补铸），`rootInputNonceAnnotation` 整套删除——它本来就是一个消息身份，只是铸造点在扇出之后、拷贝时会落空、只覆盖转交一种场景。

**但身份单独不够**：形状 B/C 的输入份数仍随机，因为那两个形状里多个写入器让写侧的 CAS 协调不到。要配合「整场只有一个写入器」才成立，两件一起上之后各连跑 10 次全是 1。

⚠️ 上一句原文写的是「编排也能交出历史配置（一次遍历共用 visited + 深度封顶）」——**那条路已整段删除**（见下节）。收敛到单写入器的现行办法是运行入口声明配置（`WithHistoryCtx`），不是遍历编排树。

**顺带核实的一条**：本节说的「凡是躲开这类 bug 的要么封闭接口、要么有消息身份」仍然成立，而其中一家在自己的 commit 里明写「构造期铸 id 才是正解」，只是被它上游库的判等语义挡着做不了——**我们现在做到了**。

---

## 结论六：历史是主路产出还是旁路（2026-07-29，源自 GraphAgent 历史全空 backlog）

问题：`GraphAgent` 两个入口都没接历史层的任何钩子——它没有静态子列表（sub-agent 由业务 `NodeFunc` 经 `RunSubAgent` 调起），所以不是漏了一行，是这套机制对它整体没接。顶层直接跑它，历史全程为空。

追问「业务自己写编排代码调 sub-agent 时，谁记历史」，四家的答案形状一致：

| | 历史的落点 | 业务自写编排调 sub-agent 时 |
|---|---|---|
| LangGraph | `messages` state channel + `add_messages` reducer | node 的**返回值**就是历史，不返回 = 没有 |
| eino | `Run(...) *AsyncIterator[*AgentEvent]`（`adk/interface.go:463`）| agent 的产出**就是**事件流，runner 排空它 |
| OpenAI | `RunResult.to_input_list()`（`result.py:287`）| 调用方自己串给下一轮 |
| 我们 | 旁路写入器 + 编排层边界补记 | 无静态子列表 ⟹ 接不上 ⟹ 全空 |

**根因**：别家的历史是**主路产出**（返回值 / 事件流 / state channel），我们的是**旁路**。

- 旁路必须有人去「接」，接不上就是空——而「接不接得上」取决于框架能不能看见那个执行体，这正是开放 `Agent` 接口下**永远存在探不到的层**（`GraphAgent` 只是最明显的一个，业务塞进来的任何自研实现同理）
- 主路不存在这回事：不产出就等于没结果，业务自己当场就发现了，不会安静地少一段历史

**这不等于我们的设计错了**。旁路的好处是真的：业务不改签名就能拿到跨 Run 的历史，`WithHistory` 一个 option 接上，九种编排零改动。代价就是这条 backlog——**任何框架接不到的执行体，历史就是空的**。两者是同一个选择的两面，不是可以两全的。

**修法的方向因此清楚**：不要再想「怎么让框架探到 GraphAgent 的子」（那是遍历编排树的老路，已证伪），而是**给 GraphAgent 一个属于它自己的配置入口**——它的 `OutputReader` 本来就是「这个图的产出」，那就是它主路上唯一天然的落点。

**顺带一条独立确认**：`add_messages` 的 docstring 写的是 "Merges two lists of messages, **updating existing messages by ID**"（`graph/message.py:67`）——消息身份在进 state 之前就有，跟我们构造期铸 `Message.ID` 同形。上一节「靠 `Msg.id` 幂等**不该学**」仍然成立（跟 append-only 冲突），但**身份先于流动**这个不变量再次被印证。

---

## 结论七：输入的持久点在「跨进边界那一刻」，不在「执行完成那一刻」（2026-07-30，源自『非 BasicAgent 子执行体中断时输入无人记』）

问题形状：`Sequential(step=外部执行体)`，step 抛中断 → 父 Run 返中断。此刻输入只活在父编排的
调用栈里：外部执行体不知道历史层的存在、什么都没写；父的边界补记是**成功后**才做的、这次
没触发。经父 Resume 那条路能自愈（step 重跑成功、补记跟上），但调用方直接 Resume 子时输入
**永久缺席**——历史落成「有答无问」。

问四家「用户输入什么时候变成持久的」，四家四种机制、同一条性质：

| | 机制 | 持久点 |
|---|---|---|
| LangGraph | 输入自己是一个 checkpoint（`_loop.py:1033` `self._put_checkpoint({"source": "input"})`，metadata 三档 `input/loop/fork` 里专门有 `input` 一档）| **首个超步之前**——后面任何节点中断都丢不掉它 |
| eino | **中断态自己带着输入**：工具层 `toolsInterruptAndRerunState.Input`（重跑时 `tool_node.go:1062` `input = tnState.Input` 恢复）；编排层 `RunCtx.RootInput` 随运行上下文走 | 挂起物本身持有输入——Resume 从挂起物里取，不需要调用方重给 |
| OpenAI | 回合**开始**时把输入单独存进 session（`run.py:751` `save_result_to_session(session, input_items, [], ...)`——新产出传空 `[]`），还配 rewind 快照供锁重试回退 | **任何 turn 产出之前**——中断分支（`build_interruption_result`）在其后，输入早已落盘 |
| Grok | 事件日志到达即追加（本文件 §一：`updates.jsonl` 无界不丢不阻塞）| **到达那一刻** |

**没有一家把输入的持久化系在「执行完成」上。** 完成与否只影响产出，不影响输入——输入在
跨进系统边界那一刻就该是事实了。

### 我们的差距只在历史层这一半

cp 那一半我们**已经有**：原始输入在父编排的 channel 里、随 cp 持久化，父 Resume 重跑时从
channel 恢复——这就是第二家「挂起物携带输入」的同构物。所以经父 Resume 那条路一直是好的。

缺的只是**历史层**：边界补记把输入的落笔系在了「子调用成功」上。修法与主流性质对齐——
**边界在「可 Resume 的退出」时也结算一次输入**（`settleHistoryOnPause`，11 个边界位置），
判据复用成功路径输入半边的同一套：

- BasicAgent 子：preamble 在原 Run 里已记 ⟹ 范围回执 >0 ⟹ 跳过，不双记
- 外部执行体：什么都没记 ⟹ 落输入，owner = 父给的位置
- 父 Resume 重跑成功后的补记：跨 Run 守卫查到落盘的这条 ⟹ 不再记
- 纯业务硬错**不落**：cp 标 Failed、这层不可 Resume，「报错的外部执行体不记」是既有语义
  （判据用 `LeavesResumableCheckpoint`——框架对这个错的处置意图，不是错的类型）

**直接 Resume 外部执行体时产出仍然记不到**——那条调用路径里没有任何框架代码在跑，这是
开放 `Agent` 接口的固有代价（结论六「主路 vs 旁路」的又一个侧面）。但输入从此在挂起那一刻
就是事实，跟四家一致。

### 结论七补：重造消息的身份（2026-07-30，源自 Plan 任务输入随 HITL 轮次线性重复的修法核对）

修掉 Plan 重复后回头问：**别人重造消息时怎么不记两遍**。答案是——**没有一家在重造，
所以没有一家需要给重造的消息派身份**：

| | 机制 | 关键证据 |
|---|---|---|
| 一 | **只在完成时提交一次**：中断的任务 writes 没 commit，重跑重造随便造，可见的只有最终那一次 | `_runner.py:746` "if it already ran, return the result"——有 writes 的任务不再执行、产物从存的 writes 重放 |
| 二 | **挂起物携带拼好的输入，重跑复用不重拼** | `tool_node.go:1062` `input = tnState.Input`；编排层 `flow.go:276` `deepCopyAgentInput(runCtx.RootInput)` 从持久化的 RootInput 拷 |
| 三 | **状态对象携带全部 items，转交/恢复都续用同一条列表** | `run.py:586-587` `original_input=run_state._original_input, generated_items=run_state._generated_items`；新 agent 不重新摄入输入 |

我们的 Plan 走了**第三条路**（`planStampInputID`：(RunID, childNS, 内容) 三元哈希的确定性
身份）——业内没有这个形态。诚实记下为什么没抄那两条：

- **「只在完成时提交」**正是结论七刚离开的设计：对可 Resume 的退出，历史里要留下这一步
  的输入（Plan 的任务描述是被摘除 planner 的唯一落脚点，挂起时读历史该看得到这个 turn）。
  三家不丢用户输入是因为用户输入另有前置持久点；我们的任务描述若只在完成时提交，挂起
  窗口内这个 turn 在历史里就是空的。
- **「挂起物携带拼好的输入」**是最优形态，但它的载体——**节点私有、随 cp 持久化的中断态**
  ——正是仍开着的引擎层缺失原语（tool_result memo 那条 backlog；2026-09-18 注：该原语已于 `95fae06f` 落地，即 `compose_v2.SaveNodeState` / `GetNodeState`）。载体没有，persist-and-
  reuse 无处落。另一个变体「把拼输入挪进 plan 节点、走 channel 过 cp」会**破坏既有契约**：
  `ExecInputBuilder` 支持抛 InterruptError 做输入审批（HITL），拼输入的位置决定 Resume 重跑
  谁——挪进 plan 节点后审批中断会重跑 planner（LLM 非确定，步骤都可能换），今天重跑的
  只是 worker。这是「照业内形状修会修出新 bug」的具体一例。
- 哈希身份的**可观测结果**与第二条路一致（同内容同位置同 Run ⟹ 同身份 ⟹ 恰记一份），
  机制不同只因载体缺席。**收敛计划**：中断态原语落地后 Plan 切 persist-and-reuse，哈希退役
  ——该原语从此有两个待接的消费者（tool_result memo + Plan 输入），优先级应上调。

Handoff 那半（守卫按历史视图模式选）也对了一遍：业内**没有同款冲突可抄**——三家的流由
编排者单点持有、执行体从不自己写输入，「共享单流一份 / 隔离流各一份」在它们那儿是构造
使然不是判据（第三家转交续用同一条 items 列表 = 我们的共享视图；另一家 child session 独立
bootstrap 自带一份 InitialContext = 我们的 SelfOnly）。我们需要判据是因为「叶子自己写 +
开放执行体」的组合是我们独有的——修法对齐的是**不变量**（去重范围 = 流的共享范围），
不是任何一家的机制。

### 结论七再补：「第三条路」当天退役——重造本可不发生（2026-07-30）

上一节写完几小时后被追问：「没有一家在重造，**为什么我们要重造**？」查下去发现答案推翻
了收敛计划：

**重造不是引擎固有，是 Plan 把拼输入写进了消费它的那个节点里。** 引擎的原子性只是说
「节点没成功，节点内算的东西作废重算」——第一家同款、文档明说。让「作废重算」变成
「重造消息」的，是拼输入的位置。同一仓库里 Reflect 的同类需求（feedback 节点拼派生输入
→ 写 `reflectActorInputCh` 提交 → actor 只读）走的就是业内形状，审计实测免疫。

**我上一节否掉 channel 变体时评估的是错误变体**：「挪进 plan 节点」确实会让审批中断重跑
planner（否得对）；但「**独立 build 节点**」两个问题都没有——审批中断只重跑 build（planner
的写已提交不重跑，审批粒度反而更准），worker 重跑从 cp 恢复同一条输入。核对第一家引擎
细节逐条同构：生产者提交进 state / 消费者重跑时生产者不重跑（writes-skip）/ 拼出的任务作
子调用输入时子有自己的 cp ns、父重跑时子续跑不重来（`task_checkpoint_ns`——我们的
childNS + auto-load 同款）。

已落地（plan_build_input 节点 + `planBuiltInputCh`），哈希身份存在了不到一天即退役。
上一节的「收敛计划」（等引擎中断态原语）作废——**channel 就是我们的 persist 载体**，
不用等。中断态原语的消费者回到一个（tool_result memo）——该原语已于 `95fae06f` 落地，memo 已接上。

留档的教训：对照业内后发现自己是唯一多出一个动作的（重造），第一反应该是「**能不能不做
这个动作**」，而不是「给这个动作打补丁」（哈希是补丁）。上一个同款教训在结论六（旁路 vs
主路），这次栽在同一个思维姿势上。

---

# 悬空 tool_call：在写侧补，还是在读侧修（2026-08-27 实挖）

> 起因两条：(a) `RunSession.Events` **只存 Exit assistant**（带着 tool_call），`tool_result`
> 从来不进 ⟹ 不开会话历史层时，从 events 重建 prior 里的 tool_call **必然悬空**；
> (b) 审批被放弃后历史（append-only）里**永久**留一条悬空 tool_call——tools 节点原子，
> 「节点成功后才写 results」。

## 结论：一家在**写侧**补上，一家干脆没有这个容器，两家都没有「读侧修补」

### A 家（CLI）：中止那一刻就把配对补齐

`aborted_response(&call, secs)` 合成一条 `AbortedToolOutput`，**用同一个 `call_id`**：

```rust
fn aborted_response(call: &ToolCall, secs: f32) -> AnyToolResult {
    AnyToolResult {
        call_id: call.call_id.clone(),
        result: Box::new(AbortedToolOutput { message: Self::abort_message(call, secs) }),
        ...
    }
}
// "aborted by user after 3.2s"；exec_command 另附 wall time
```

配合 `notify_tool_aborted` 发事件。**配对从不悬空**，因为洞在产生的那一刻就被填上了。
文案也诚实——只说「被用户中止 + 跑了多久」，不假装成功也不假装失败。

它的历史条目里 `FunctionCall` 与 `FunctionCallOutput` 是**两个平级的一等变体**
（`CustomToolCall`/`CustomToolCallOutput`、`LocalShellCall` 同理），不存在「只记最终答复」
的有损容器。

### B 家（图引擎）：**零修补**

全仓 grep `dangling` / `orphan` / `unmatched` **0 命中**。它的 state 就是 messages 列表，
`ToolMessage` 跟别的消息一样是一等公民；中断时整个节点的 writes 不提交，续跑时整节点重跑。
**放弃不续跑的话，那条 thread 的状态里确实留着悬空的 tool_call**——它的暴露面跟我们一样，
只是不去补。

## 我们跟它们的差别在**补的位置**

我们是**读侧修补**：`dropDanglingToolCalls`（8 处引用）+ 给零结果的工具轮合成一条
「记账文本」。代价是那条记账文本是 **assistant + 零 tool_call**，**正好长得像一条最终答复**
——于是凡是「挑一条消息代表某件事」的地方都得经 `IsUnresolvedCallsRecord` 跳过它
（仍在扫的还有 2 处），而且业务自己扫 messages 时也得记得调。

**写侧补的方案让上面整串防护都不需要存在。** 这是这次调研里最直接可搬的一条——
不是抄一个 API，是把补丁从读侧挪到写侧。

⚠️ 但两条得分开看：**(b) 适用**（审批放弃是明确的中止时刻，有地方挂）；
**(a) 不完全适用**——它是「容器少存了一半」，写侧补要么让 events 也存 tool_result
（容器语义改动），要么承认 events 本来就不该当消息流重建源。**两家都没有这个容器**，
说明后者更可能是对的。（2026-09-03 六家复核见结论八：全部指向「后者」。）

## 结论八：会话 prior 的存储单元——六家都存 tool 往返，审计流可以比 prior 多、不能比它少（2026-09-03，源自『RunSession.Events 存不下 tool_result』backlog）

背景：`doc/code/todo.md` 那条 backlog 给了两条出路——(a) 让 events 也存 tool_result；(b) 推业务开历史层。
派单视角调研六家，问三件事：prior 的存储单元是什么、存不存 tool_result、悬空 tool_call 谁处理。
路径缩写：OA = `openai-agents-python/src/agents`，MAF = `agent-framework/python/packages/core/agent_framework`，
EINO = `eino/adk`，LG = `langgraph/libs/langgraph/langgraph`，CODEX = `codex/codex-rs`，DSH = `deepseek-harness/packages`。

| 框架 | prior 存储单元 | 存 tool_result | 悬空 tool_call 怎么处理 | 证据 |
|---|---|---|---|---|
| OpenAI Agents SDK | `Session.get_items/add_items` 存 Responses API wire item（sqlite 每条 `json.dumps` 一行） | 存——RunItem 经 `run_item_to_input_item` 全转，只丢 `tool_approval_item` | **读侧** `drop_orphan_function_calls`：建模型输入时整条删无 output 的 function_call（连前置 reasoning），「so resumptions or retries do not replay stale tool calls」 | `OA/memory/session.py:14-54`、`run_internal/items.py:98-106`、`run_internal/session_persistence.py:247-278` |
| MAF | `HistoryProvider.get_messages/save_messages`；没配则框架**自动装** `InMemoryHistoryProvider` | 存——`after_run` 存 input + `response.messages`，`role="tool"` 消息在内 | **不让洞产生**：迭代耗尽再调一次 `tool_choice="none"`「instead of leaving orphaned function_call items」；压缩按 assistant(function_call) + 后续 tool 整组 | `MAF/_sessions.py:413-433,824-826,869-891`、`_tools.py:2631-2640`、`_compaction.py:172-215` |
| eino adk | `runSession.Events` **per-Run**（gob 进 checkpoint）；跨 Run prior 由调用方 `messages` 传入，无内置跨 Run 容器 | 存（Run 内）——`schema.ToolMessage` 是一等事件 | opt-in 中间件 `patchtoolcalls`：每次模型调用前给无响应的 tool_call 插占位 tool message；摘 transfer 是 assistant + tool **成对**摘 | `EINO/runctx.go:33-48,243-256`、`flow.go:276-328`、`middlewares/patchtoolcalls/patchtoolcalls.go:48-66` |
| LangGraph | state `messages` channel（`add_messages` 按 id append-only）随 checkpoint 持久 | 存——`ToolMessage` 直接作节点更新 | 未找到（配对语义在 langchain-core，本地无） | `LG/graph/message.py:61-80,205-245` |
| Codex | rollout JSONL——`ResponseItem`（模型可见）与 `EventMsg`（UI 事件）**同一文件** | 存——`FunctionCall` / `FunctionCallOutput` 全持久 | **读侧**每次建 prompt `normalize_history`：缺 output 的 call 插合成 `"aborted"` 输出（不持久化） | `CODEX/rollout/src/policy.rs:9-59`、`core/src/context_manager/normalize.rs:21-225` |
| dsh | append-only 事件日志；prior 是日志上的 surface 投影（只投 user / assistant / tool result 三种） | 存——`tool/call` 与 `tool/result` 都是一等持久事件 | **写侧**：加载存档给 pending call 合成 error 结果并 `commitRepair` 写回日志；取消时当场写 aborted 的 call / result 对 | `DSH/core/session/src/{types,surface,repair}.ts` |

三条结论：

1. **prior 存储单元 = 模型可见的 wire 单元，tool 往返是一等成员（六家全部）。没有一家只存最终 assistant 当跨轮 prior**——三个「看着像」的都不是：eino `skipTransferMessages` 是 assistant + tool 一起摘（`EINO/flow.go:289-298`）；OpenAI 的 handoff filter 让**模型输入**少于 session（存的仍完整，方向与我们相反）；OpenAI 丢的 `tool_approval_item` 是审批占位，function_call 照样入 session。
2. **审计流与 prior 的关系三种形态**：一份日志、prior 是投影（dsh / Codex，审计 ⊇ prior）；一份存储兼两职、存完整 wire 流（OpenAI / MAF）；无独立审计存储（LangGraph / eino，checkpoint state 即 prior）。**没有一家的审计流比 prior 少**。我们是第四种：events 只存 Output / Exit 的 assistant，比 prior 所需少，却被当 prior 源——六家没有这个形态。
3. **悬空 tool_call 在别人那里是例外路径**（崩溃 / 取消 / 中断 / 预算耗尽），**在我们这里是常态**（每个 return-direct 收尾轮都悬空）——不是补洞不够好，是容器少存一半。

对两条出路的判断：

- **(a) events 也存 tool_result：不支持。** 那是造第二份 append-only 逐字流，与 `HistoryStore` 语义重叠（两份容器、两套写入点、两套配对修补），且 events 仍不进 cp——正是「给借来的容器加能力」而非「给第二种语义自己的载体」。顺带一条 drift：`agent_v2/event.go` 的 `AgentEvent` godoc 曾声称包装「tool 结果」，而生产路径所有 `AddEvent` 零处写 `role=tool`（本次已改注释）。
- **(b) 推业务开历史层：方向最贴三方，但落不了地。** 他们的完整流**默认在**（MAF 自动装 provider、dsh / Codex 日志即主存储、OpenAI `to_input_list()` 完整、eino 干脆交给调用方），我们 opt-in 且门槛高（`Owner` 必填 + `WithName` 必填 + `WithThreadIDCtx`），example 只有 `history_smoke` / `history_boundary_smoke` 两个开。
- **证据指向的形状：events 不该当 prior 用，prior 恒从完整流来。** 二选一待拍板：**(b')** 跨轮复用 sess 时框架默认挂进程内 `HistoryStore`（对齐 MAF 自动装；需给 owner / ConvID 一个零配置取法——三家都只用一个 session id，不是 owner + conv 两维）；**(a')** 把 events 降级为纯审计，从 `buildInitialMessages` 撤掉 events 源（不开历史层 = 无跨轮消息流 prior，对齐 eino「调用方不传就没有」）。两者下「剥 ExitEvent 的 tool_call 不能做」的约束都不再成立——eino 的 `rewriteMessage` 就是对**含 tool 消息的完整流**做的（`EINO/flow.go:188`），"[X] called tool: transfer_to_agent" 在完整流上照样生成，只在「events 是唯一 prior 源」时才需要 Exit 残留 tool_call。

## 结论九：调用方手里的前文——AG-UI 每次请求都带完整 messages（2026-10-21，源自 P0「AG-UI 多轮失忆」）

AG-UI 1.0 规定 producer **必须**把 run input 的 messages 视作完整历史（`docs/spec/1.0/basic/run-input.mdx`）。我们原先每轮
新建会话、只取最后一条 user，没配历史层时第二轮起模型失忆。调研了八家 AG-UI 适配器（读源码为主，pydantic-ai / TanStack
经 WebFetch 转述）：

| 形状 | 谁 | 要点 |
|---|---|---|
| 服务端有就以服务端为准、为空用客户端补 | LangGraph、Mastra、MAF（Python）、Strands | LangGraph 以 checkpoint 为底、ID 不在其中的客户端消息追加，客户端改了早先轮次就从更早的 checkpoint 分叉重生成（bug 最多的地方）；MAF 有存档时只收新增的 user 轮次与服务端工具调用的结果，存档为空时全收；Strands 没配 session manager 时用客户端 messages 重建整段历史 |
| 服务端有持久化也以客户端为准 | pydantic-ai、CrewAI | pydantic-ai 文档原话「a client that can submit history can always fabricate it」，要更强完整性就按 thread 在服务端存 |
| 只取最后一条 | ADK、Claude Agent SDK 适配器 | ADK 的「已处理消息 ID」表让服务端为空时客户端前文被丢弃 |

客户端 system / developer：pydantic-ai 默认剥掉并告警（理由写明防注入，`manage_system_prompt='client'` 才交给前端），
Strands 丢弃，LangGraph 丢 developer 与开头那条 system，MAF 有存档时丢客户端的非 user 消息、客户端 state 降级成不可信的
user 消息。规范本身对这两个角色没有任何信任语义。

**我们的形状**（`agent_v2/caller_history.go`，2026-10-22 第五版）：调用方提供的前文经 `WithCallerHistoryCtx` 进来，
**一次运行只认一个来源**——有拥有这场对话的写入器、建写入器时读得出记录、需要写的前文也写成了，就以服务端为准（服务端
第一次见到这场对话时把前文写进去一次，此后不再看）；其余情形以客户端为准（前文排在 session events 之前，历史层这次不读）。
只取 user / assistant 文本。中间试过逐轮对齐，四轮审计后放弃，经过见下面第 2 条。

两处要专门设计，都是我们自己的结构带来的：

1. **不按 agent 过滤**（LangGraph 把客户端消息并进图的共享 messages，每个节点本来就都看得到；我们得明说）。我们的 prior
   不论来自 events 还是历史层都按归属过滤，而客户端消息没有归属——伪造一个 AgentName
   塞进 events 的话，单个 BasicAgent 对得上，换成编排每个成员按自己的名字取、全部取空（正是「借用」形态，见 CLAUDE.md
   「同一个成因的另一个后果」）。前文是整场对话、编排树里谁说的分不出来，就当作每个读历史的 agent 都该看到的开头；写进
   历史层的那份也一样（标 `CallerHistoryAnnotation`，历史视图对它不过滤）——否则第二轮起服务端为准时成员又失忆。写进去的那份归属是
   保留值 `callerHistoryOwner`（2026-10-22 合并分页读时加的）：「只看自己」的视图只从末尾往前、按归属读，前文另按这个归属取回，
   不必为了它翻整场会话。
2. **写进历史层**（「空时用客户端补」那一档要不要落盘）。不写的话下一轮服务端历史缺这一段，只能每轮再拿客户端的补，于是又
   回到按 ID 对账。代价是伪造的轮次成为服务端的永久记录——可接受：控制流状态（审批、转交、存档）都在服务端，伪造的文本最多
   做到提示词注入；只留 user / assistant 文本、带来源标注，业务直接读历史层时认得出来。

   **我们的历史层是追加写、可以有多个写入器**，别家是每个线程一份快照、由那次运行整份写——「为空才写」在他们那儿是原子的，
   在我们这儿是先读后写。第一轮审计实测三处：两个写入器同时开各写一份（下一轮每种视图都看到两遍）、一份落在别的记录之后
   （对话中间又出现一遍开头）、那一批没写成时同一个运行里后跑的成员看不到。修法不加锁、也不给 `HistoryStore` 加原子接口：
   前文的 ID 按（第几条、角色、内容）确定性派生，读的时候带标注的记录按 ID 去重（幂等键的做法，跨进程成立）。

   **第二轮审计（2026-10-22）推翻了判据的粒度**。「为空才写、非空就整体以服务端为准」的前提是历史层记着整场对话，可它
   只记有写入器的那些轮：某一轮由没配历史层的 agent 接（没有写入器），那一轮就从此丢了；为并发问题加的「嵌套写入器不写、
   每轮照用客户端的」又在嵌套写入器每轮都接的情形下让每一轮出现两遍、顺序颠倒（客户端的整场对话 + 它自己的记录）；开写
   入器时读失败一次，后面的读成功时又把客户端那份整段补上、重复一轮。三处都是「覆盖情况按整场对话判」——别家按**消息**判
   （LangGraph 认 ID 在不在 checkpoint 里，ADK 有已处理消息 ID 表）。改成按**轮**：记录盖所在的轮（这次运行前文里有 k 条
   user 就是第 k+1 轮），读时逐轮对上——开头那条 user 被记过（同轮、同内容）的轮只用记录；一条记录都没有的轮用客户端的
   文本，下一个开写入器的运行补写；有记录、开头却没被记过的轮（AgentAsTool 的子 agent 收到的是工具参数，它记的是自己的
   子对话；或者钩子改写了输入）两边按内容对上的用记录、对不上的都留，不补写（补写会排到子对话后面）。判「开头被记过」
   而不是「这一轮有记录」，正是为了这种轮。不按消息 ID 对：`WithCallerHistoryCtx` 是通用 API，调用方的 ID 不一定跨请求
   稳定；AG-UI 的回传 ID 还要跟流式 messageId、历史记录 ID 三处对齐。

   **第三轮审计（同日）又指出判据与输出不是同一个视角**：「记过没有」拿全部记录判，输出却按这个 agent 的视图过滤——
   Routing 的 SelfOnly 成员碰上别的成员记过的轮什么都看不到（配了历史层反而失忆），Sequential 第二步看到「没有问题的
   答复」；而且「同内容的 user 记录」分不出整轮的开头与子对话的输入（子 agent 收到的工具参数常常就是用户的原问题），那一轮
   给用户的答复被丢掉；按轮排序又让前端「编辑前面的消息重发」之后被放弃的轮排到最近。改成：只有这个 agent 看得到的、
   **拥有这场对话的写入器**写的开头才让一轮以服务端为准（嵌套写入器只看得到子对话，记录另盖标记）；看不到的轮用客户端的
   文本（前文没有归属，跟没配历史层时一致）；agent 记录的轮次倒退就判对不上、按追加顺序出。

   **第四轮审计（同日）说明问题出在前提，按轮对齐整个撤掉**。三条反例：钩子里调起的 helper 判不成嵌套（判据是「在不在
   节点里」，顶层 agent 的钩子不在节点里），用户收到的答复被丢掉；编排只从直接子借配置，`Sequential[Sequential[chat]]`
   里唯一的写入器成了嵌套写入器，客户端改过的答复跟记录一起进了模型；轮次倒退一旦判成对不上就是永久的，编辑重发一次
   之后没有写入器的轮从此在所有视图里消失。同一条合并规则审了四轮、每轮都有新反例：两份描述同一段对话的来源——客户端
   会裁、会改、会重新生成，历史层又有客户端看不到的工具往返与子对话——本来就没有可靠的对法。

   第五版**一次运行只认一个来源**。以服务端为准要同时满足三条：这次运行有拥有这场对话的写入器——建写入器时 ctx 上没有
   正在执行的任务（`enclosingTask`；各 agent 入口都在盖任务章之前建写入器，钩子、节点、工具里调起的都判得出是嵌套），
   编排从整棵静态子树借配置（`borrowHistoryConfig`），树里有成员配了历史层就由最外层的编排拥有；建写入器时读得出这场
   对话的记录；记录为空、需要写的前文同步写成了（写失败这次就以客户端为准，不让这次运行读出一场空对话）。其余情形以
   客户端为准。这对应上表两个阵营各自的形状：有存档时以服务端为准（MAF 有存档时、ADK），没有时以客户端为准（Strands
   没配 session manager 时）——我们按配置落进其中一边，同一次运行里不混用。几个运行同时判出「这场对话是空的」时各写
   一份前文，ID 相同，读时按 ID 去重、排在最前（它们是服务端第一次见到这场对话之前的轮次）。

   代价：以服务端为准时客户端的编辑与重新生成不改写服务端记录（要这类前端功能就让前端拥有历史、不配历史层）；同一场
   对话各轮要用同一套配置，某几轮由没配历史层的 agent 接，以服务端为准的轮看不到它们；前文只在第一次见到对话时写，
   那一次读或写失败之后那几轮不在服务端记录里；SelfOnly 成员（Routing 的分支、Sequential 后面的步）以服务端为准时看不到
   别的成员接的轮——跟没有调用方前文时一样，是 SelfOnly 自己的语义，记在 `doc/code/todo.md`。

**不做**：按消息 ID 对账（AG-UI 的 messageId 由服务端发出、客户端回传，客户端可以给任意 ID；我们给前文新铸身份）、分叉重
生成、客户端改了早先轮次时跟着改（以服务端为准、文档写明）。`context` / `state` 的消费与「前端显式拥有指令」的开关记在
`doc/code/todo.md`。

---

# 节点处理完之后作废：历史不重记、模型不重调（2026-10-21 调研）

起因：BasicAgent 的 model / tools 节点在节点体里记会话历史。节点体成功之后、超步提交之前这次执行被作废（当时是节点之后的
编排钩子 panic、提交出错；前一条 2026-10-22 起不再作废，见下面「我们的取舍」）时，续跑把节点体整个重跑一遍：tools 节点同一批结果记两份（下一次 Run 的 prior 里一个调用
配两条结果），model 节点再调一次模型，作废的那份答复留在历史里、带着没有结果的工具调用。同一个缺口的另一面：
OnModelEnd 钩子拿答复问人之后续跑重新调模型，用户批准的是第一份、批准落到第二份上（均实测）。

## 四家怎么做

| | 已完成的那一步之后出错 | 会话历史怎么不重复 |
|---|---|---|
| LangGraph | task 一完成写入就落盘（`pregel/_runner.py:605-615`），续跑只执行没有写入的 task（`pregel/main.py:3059`），「产出过写入的 task 算完成」（`pregel/_task_status.py:43-44`）；观测回调抛错默认被吞（langchain_core `BaseCallbackHandler.raise_error=False`），显式要求上抛时 task 照样算完成、续跑不重跑（实跑探针） | 历史就是 state，随写入一起提交，没有单独的历史写入 |
| OpenAI Agents SDK | 每轮结束 `save_result_to_session` | 记下已写过多少条 + 内容指纹（`run_internal/session_persistence.py:792-818`）；写前把「待写批次 + 会话尾部摘要」记进 RunState，续跑对账、对不上就报错不重跑已完成的工具（1444-1617） |
| Google ADK | 事件一产生就 `append_event` 再交出（`runners.py:841-851`） | 续跑从事件重建：已经落盘的模型输出不重新生成，只补执行没执行过的工具调用（`flows/llm_flows/core/_resume.py`） |
| MAF | `HistoryProvider.after_run` 整次 run 收尾才写（`_sessions.py:1103-1126`），可延后 / 作废的持久化门（1228-1300） | 写入侧按消息 ID（没有就按 role + 内容哈希）找有序重叠去重（`filter_new_messages`，152-203） |

共同点：**已经完成的那一步不重新生成、不重新写**——要么不重跑（LangGraph、ADK），要么记下写过什么（OpenAI、MAF）。

## 我们的取舍

- **当时先不改引擎这一层**：成功路径的 OnNodeEnd 不另加兜底是当时的既定决策。引擎的契约是「成功的执行可能作废重跑，
  节点要靠私有态做到重跑幂等」——问题出在我们自己的节点没守这条契约，就在节点上修。**2026-10-22 引擎这一层也改了**：
  节点体成功之后的 OnNodeEnd、图成功之后的 OnGraphEnd、任何时候的 OnNodeEvent、任何时候的 OnAgentEnd panic 一律就地
  兜住、带栈记一条 ERROR，结果与写入照常提交（对照 LangGraph 回调出错默认吞掉、显式上抛时 task 照样算完成；OpenAI Agents SDK
  工具输出在 `on_tool_end` 之前已提交）。节点私有态那套留着兜剩下两条路：成功之后的钩子 Goexit、提交出错（reducer panic
  那条路引擎本来就整超步重跑）。
- **model 节点**：模型调用的记录放节点私有态（`agent_v2/model_call_record.go`，按 Rerunnable 记）。模型一返回记下原样，这一轮
  处理完（OnModelEnd 之后）定下。定下之后重放：不调模型、不记历史，钩子也不触发——再交给钩子的只能是处理完的那份，
  不幂等的钩子（解析答复里的 JSON、打序号）会在它上面报错或叠加改写（已完成的 task 续跑时不重跑，它的回调自然也不再触发，
  同 LangGraph）。只有原样时看停下的原因：中断、取消（`compose_v2.MustPropagate` 为真）续跑沿用这份答复、把这一轮处理完，
  不再带用量；别的结局（钩子报错拒掉、置空、panic）一律作废、续跑重新生成——业务用 OnModelEnd 报错拒掉不合格的答复时续跑
  本来就该换一份，内容触发的 panic 留着原样的话续跑永远过不去。这是跟 ADK「已落盘的输出不重新生成」不同的地方：它的事件
  一产生就算数，我们的答复要过了钩子才算。
- **tools 节点**：结果本来就记在工具账里（处理完时定下，见 `finalizeToolMemo`；定下了的续跑不再处理、钩子不触发），另记一笔
  「这一批已记过会话历史」，重放时不再记——对应 OpenAI 把「已写过的」记进可续跑状态。**没照抄写入侧按内容去重**（MAF / OpenAI 按 ID 或内容找重叠）：我们的
  conformance 要求逐字重复的两条都得留（模型真的会连说两句一样的话），按内容去重会吃掉合法记录。

## 顺带两条（同一次调研）

- **配置优先级**：运行时配置按字段覆盖构造时配置是普遍做法（OpenAI 的 SessionSettings、MAF 的 options、LangGraph 的 store /
  checkpointer），只有构造时的显式关闭（`checkpointer=False`）赢。我们的 `WithHistoryCtx` 约定「两者都在时以 ctx 为准」与之
  一致；BasicAgent 之前没照做（只读自己的 `WithHistory`），已改。
- **后端类型保持**：内置后端都按 JSON / msgpack 整条存取，类型保得住；没有一家的 conformance 区分得了布尔与整数（LangGraph
  那条用 `==`，Python 里 `True == 1`）。我们的 `history_conformance` MessageFields 现在逐个类型判（布尔、数值、数组、对象），
  只存得下字符串的后端在这里红。
