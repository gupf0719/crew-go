# 上下文压缩 / 内容卸载 —— 五家对照

> 权威设计文档是 [`doc/v2/context-compaction.md`](../../v2/context-compaction.md)（我们自己的 4 层压缩）。
> 本篇只记**对照结论与决策**。
>
> 立篇缘由：2026-07-31 盘点发现「4 层压缩」是对照文档零覆盖的 7 个子系统之一
> （见 [todo 里那条方法级 backlog](../../code/todo.md)），恰好又实测到一个真问题，
> 于是按那条 backlog 的做法——**动手之前先挖**。
>
> 对照对象：LangGraph 1.2.10 / eino v0.8.4-era / OpenAI Agents SDK v0.17.7 /
> MAF python-1.10.0 / Grok harness。

## 触发调研的问题：卸载到外部存储时每轮重复写

我们的 MicroCompact（第 1 层）把**旧的** tool_result 内容清成占位符，可选
`OffloadEnabled` 模式先把完整内容 `store.Put(ns, toolCallID, msg)` 卸到外部存储。
它有个「已 offload 就跳过」的短路，读消息上的 annotation marker。

**实测坏了**：5 轮工具调用、`KeepRecent=1`，该卸载的 4 条实际 `store.Put` **10 次**
（key 序列 `[o1 | o1 o2 | o1 o2 o3 | o1 o2 o3 o4]`，`o1` 写了 4 次），**随轮数平方增长**。

原因是我们另一条不变量的另一面：marker 只设在压缩产出的 **clone** 上，而
**压缩结果从不写回 channel**（那条不变量是 tool memo 正确性的前提，见
[interrupt-resume.md](interrupt-resume.md) 与 `agent_v2/tool_memo_compact_test.go`）
⟹ 下一轮从 channel 读出的原消息永远没有 marker ⟹ 短路永不命中。

## 结论一：需要去重标记，恰恰因为选了「每轮重扫」；而选重扫是因为判据是全局的

这是本次最有解释力的一条，而且**同一个框架内部就有对照**——eino 把它的机制拆成两相，
两相各选了一种，用的是同一个 Backend、同一套卸载代码：

| 相 | 触发位置 | 判据 | 频次 | 要不要标记 |
|---|---|---|---|---|
| **截断相** | tool 端点包装器内（`reduction.go:435-469`） | `MaxLengthForTrunc`（**长度**，自足） | 每次 tool 执行 1 次 | **不需要**——超长原文从未进 state，下轮无物可扫 |
| **清理相** | `BeforeModelRewriteState`（每次 model 调用） | token 预算 + 保留末 N 轮（**全局**） | 每轮重扫 | **必须**——靠 `msg.Extra` 上的 `_reduction_mw_processed` 推进扫描起点 |

**判据决定形态**：只跟「这一条内容自身」有关的判据（长度、大小）可以在产生时一次性处理；
「按新旧淘汰」这类判据的「旧」是相对全局位置定义的，**产生时不可知，只能重扫**。
我们的 `KeepRecent` 正是后者。

五家的落点：

| | 有卸载？ | 时机 | 标记放哪 |
|---|---|---|---|
| eino 截断相 | ✅ | **产生时一次性** | 不需要 |
| eino 清理相 | ✅ | 每轮重扫 | 消息上 + **写回持久 state** |
| Grok harness | ✅ 写文件 | **产生时一次性**（tool 执行器内联，`mcp_truncate.rs:221`） | 不需要 |
| OpenAI Agents SDK | ❌ 纯内存裁剪 | 每轮重扫（整个历史扫三遍） | **索性没有**，认了重复 CPU |
| LangGraph | ❌ 核心层无卸载概念 | — | — |
| MAF | ❌ 压缩只翻可见性开关 | 每轮重扫 | 消息上（标"已压缩"，无外部写） |

**「重扫 + 无标记」在业内是常见且被接受的**——OpenAI 的 `ToolOutputTrimmer` 结构跟我们
的 MicroCompact 几乎一样（每轮重扫、产出临时 clone、不写回、无标记），差别只有一个：
**它没有 offload**。所以它每轮重做的代价是本地 CPU，白送。

⟹ 问题的准确表述不是「marker 放错地方」，而是：
**我们本来就是「重扫免费」的形态，`OffloadEnabled` 给这条免费路径挂上了远程写副作用，
却没有相应引入跨轮去重机制。**

## 结论二：我们的失效是良性的，抄 eino 会把它换成恶性的

eino 的清理相 marker 能活到下一轮，靠的是两条链：默认档下 `needProcessPart` 就是
**原始消息指针**（`reduction.go:1150-1156`，非深拷）⟹ 原地打标；且
`state.Messages = editTarget`（`:1134`）写回持久 state、框架随即落盘
（`adk/wrappers.go:1238-1244`）。这是它的**接口契约**不是实现细节——
`adk/handler.go:160-171` 明写「Changes here are persisted in state and reflected in
subsequent iterations」，并在 `:246-250` 明确劝退临时视图那条路。

**但两种设计的失效模式是对偶的**：

| | 标记落盘 | 原文还在 state 里吗 | 去重失效时重写什么 | 后果 |
|---|---|---|---|---|
| **crew-go** | ❌ | ✅ 还在 channel | 同 key **同内容** | **纯浪费，正确性无损** |
| **eino** | ✅ | ❌ 已被清空 | 同 key，**占位符** | **原文永久丢失** |

调研 agent 用公开 API 实测复现了 eino 那侧：清理相的去重押在「已清理的消息构成连续
前缀」这个假设上（主循环无逐条 flag 检查，靠 `start` 推进），而**不带 tool_call 的普通
assistant 消息永远不会被打标**——它一旦落在清理窗口靠前的位置，`start` 就永久卡住，
其后已清理过的轮次每轮重新处理。实测三轮后 Backend 里 `call_A` 的真实原文被占位符
覆盖：state 里已清空、Backend 里被覆写，**两头都没了**。
（多轮下的可达性是据 `adk/react.go:369` 的 state 播种代码推断，实测只做到 middleware 层。）

**这条直接支持我们那个不变量**：「压缩不写回 channel」不只是 tool memo 的前提，
它同时让 offload 去重失效**只是成本问题**。eino 必须提供 Backend + 强制业务挂
`read_file` 工具，正因为它的写回是**破坏性**的——卸载对它是恢复的唯一途径，对我们
只是业务侧取用的便利。

⚠️ 但要诚实记下我们**同时放弃了什么**：eino 劝退临时视图的理由有两条，第二条跟 marker
无关——**每轮重新派生临时视图会让模型看到的前缀不断变化，打爆 prompt cache**。这条
成本我们此前没有纳入考虑。

顺带：LangGraph 把两种模式**并列暴露给调用方自己选**
（`chat_agent_executor.py:399-410` 的 `messages` vs `llm_input_messages`，后者注释直书
"will NOT UPDATE `messages` in the state"），说明「压缩产物不写回原始消息」在业内是
**受支持的一等选项**，不是异类。

## 结论三：Get-before-Put 零先例；去重集合的作用域必须等于 key 的作用域

五家的存储接口**都只有 Put，没有 PutIfAbsent / CAS / etag**，没有任何一家在卸载路径上
做过 exists 检查（Grok 的 `persist_noclobber` 是防计数器撞号、MAF 的 `exists()` 从不在
`store()` 前调用）。全语料里唯一一处「重复写值得防」的讨论在 Grok 的遥测落盘队列
（`queue.rs:475`），没被带到卸载路径——**因为那条路径结构上就不会重复**。

「把去重游标放在数据旁边而不是数据里」这个形状倒是有先例：MAF 的增量重扫游标
`_first_untokenized_index`（`_compaction.py:385`）、OpenAI compaction session 的进程内
缓存（`openai_responses_compaction_session.py:130-135`）——**都是进程本地、重启即失、
失效后果仅是重做一次**。

⚠️ **落地时唯一的正确性约束**：去重集合**绝不能挂在 `MicroCompactConfig` 上**。它在
agent 构造期建一次、跨所有 Run 复用，而 offload key 就是
`schema.GetToolResultID(m)`——我们自己刚记过 **`tc.ID 不唯一`**（commit `91d6672`）。
挂 cfg 上会让另一个 session 里撞了 ID 的消息被**错误跳过卸载**，那是真数据丢失。
正解是跟现有 key 作用域对齐：`OffloadNamespace` 已按 sid 隔离
（`microcompact.go:107-108`），去重集合也必须按 sid。**去重范围 = key 范围。**

## 结论四：修复面比看起来小——5 个读取点只有 1 个是死的

`MicroCompactOffloadedAnnotation` 有 5 处读取，其中 4 处
（`autocompact.go:248` / `:617` / `:781`、`reactive_compact.go:280`）读到的是
**MicroCompact 的产物 clone**（`basic_agent.go:1444` 的 `msgs` 直接喂给 `:1449` 的
AutoCompact），marker 正常命中——**跨层保护一直在正常工作**。

真正永不命中的**只有 `microcompact.go:260-271` 这一处跨轮自指读**。

⟹ 问题不是「marker 放错地方」，而是**一个为「同轮跨层」设计的 marker，被同时当成了
「跨轮自指」去重器**。前者不需要持久化，后者需要——它们本该是两个机制。

## 决策记录

| 决策 | 结论 | 依据 |
|---|---|---|
| 压缩结果不写回 channel | **保持** | 它是 tool memo 正确性的前提；且让 offload 去重失效只是成本问题而非数据丢失（eino 那侧实测原文被占位符覆盖）|
| 让 marker 进持久化状态修去重 | **排除** | 等于抄 eino 的写回，会把良性失效换成恶性失效的可能 |
| offload 去重的修法 | **已决：per-Run 已卸载集合**（`554c6a45`；去重范围 = key 范围、不跨 Run）| Get-before-Put 零先例但有 TTL 自愈；per-sid 集合有形状先例但无自愈 |
| `KeepRecent` 这类全局判据要不要改成产生时判定 | **未评估** | 那等于把 MicroCompact 拆成 eino 那样的两相（长度判据一次性 + 全局判据重扫），是架构改动 |

## 结论五：压缩时怎么认出 system prompt——九家里只有一家跟我们同款判据，而它的前提我们没有

### 触发这次调研的问题

我们有三处代码用**「前导位置」**当「这是 agent 的 system prompt」的判据：

| 位置 | 干什么用 |
|---|---|
| `agent_v2/autocompact.go:228`（`convStart`）| 阈值估算时跳过它 |
| `agent_v2/autocompact.go:355`（`sysEnd`）| 摘要时不动它 |
| `agent_v2/reactive_compact.go:150`（`sysEnd`）| 紧急删组时不动它 |

而 `buildInitialMessages` 的装配序是 **模板渲染（`:616`）→ sysPrompt（`:721`）→ 注入记忆（`:826`）→ prior → input**。
模板一旦渲染出 few-shot，真 sysPrompt 就排在 few-shot **之后**，三处判据同时失效——
它降级成一个普通可丢组（`compose_v2/message_group.go` 里中段 `RoleSystem` 恒单独成组）。

实测三条后果，**全部 `err == nil`**：紧急删组走 gap 路径直接丢掉它；AutoCompact 把它送进摘要被摘要文本取代；
`shouldTrigger` 把它算进阈值（同一段短对话，前导时 `false`、非前导时 `true`）——
而 skill 的 body 正是拼进这条 sysPrompt 的（`:697`），触发之后连 skill 正文一起摘掉。

### 对照表

| 框架 | agent 的 system prompt 存在哪 | 压缩时的判据 | 多条 system 怎么办 |
|---|---|---|---|
| LangChain `trim_messages` | 不管（通用工具） | **恰好 `messages[0]`**，比前导块还窄 | 第 2 条起当普通消息，可删 |
| LangGraph `create_react_agent` | agent 配置项，**model 调用时才拼** | 压缩节点（`pre_model_hook`）物理上看不见它 | state 里的 system 全是调用方的 |
| **eino ADK** | `Config.Instruction`，但 `defaultGenModelInput` **恒合成到下标 0** | **前导块扫描——跟我们一字不差** | 前导块整段保留 |
| MAF | `ChatOptions.instructions`，chat client 边界才成消息 | **逐条 role**，位置无关 | 全保护；预算兜不住时按同序连 system 一起排除 |
| OpenAI Agents SDK | `Agent.instructions` 独立参数，wire 转换时才 `insert(0)` | 压缩看不见（服务端 compact 只吃 items） | items 里没有 system 这一档 |
| Codex | `Prompt.base_instructions` 独立字段 | 压缩看不见；层 3 等价物直接删最旧一条 | history 里只有 developer/user 角色的注入片段 |
| DeepSeek Harness | `EpochHeader.system` 独立字段 | surface 里没有它，从 `surface[0]` 起全可压 | surface 里不存在 system 节点 |
| AgentScope | agent 字段，每次调用现合成 | 只压 memory | memory 里无 system |
| CrewAI | 在 messages 里 | **逐条 role**，位置无关 | 全收集，**并重排到最前** |

两句话概括：

1. **六家（LangGraph / MAF / OpenAI SDK / Codex / DeepSeek / AgentScope）的 agent system prompt 根本不在被压缩的那个列表里**——
   它是 agent/请求配置上的一个**有名字的字段**，调 model 时才拼成消息。这六家不需要判据。
2. 剩下三家里，**要在列表里保护 system 消息**的三家（eino / MAF 的 `preserve_system` / CrewAI）中，
   只有 **eino 用位置**，MAF 与 CrewAI 都是**逐条按 role**。（MAF 两边都占：它自己的 instructions 走字段，
   `preserve_system` 保护的是**调用方塞进 history 的** system。LangChain 的 `trim_messages` 是通用工具、
   不服务任何 agent 的 persona，它的 `messages[0]` 判据管的也是调用方自己放的那条。）

### 唯一同款的那家，靠的是一条我们已经破坏的不变量

eino 的 `splitSystemAndContextMsgs`（`adk/middlewares/summarization/summarization.go`）跟我们的写法逐字同构：
从头扫、遇到第一条非 system 就 `break`。它成立是因为 `defaultGenModelInput`（`adk/chatmodel.go`）
**无条件**把 `Instruction` 摆在下标 0、再 append 用户消息——前面不可能有 few-shot。

我们的装配序把模板放在了 sysPrompt 前面，正好破坏了这条前提。
**判据抄对了，前提没抄。**（eino 的 `GenModelInput` 用户可覆盖，覆盖成「模板在前」会踩同一个坑，只是默认路径不会。）

### eino 对「附加指令内容」的三种处理，没有一种是加保护判据

- **AGENTS.md**：transient 注入，压根不进 state，包 doc 明写「naturally excluded from summarization / compression」，
  并附顺序纪律「place this middleware AFTER the summarization middleware」。
- **skill 正文**：拼进 `Instruction` 字符串（跟我们一样）。
- **reduction**（相当于我们的层 1/3）：**不删消息**，只把工具结果卸载换占位符。

### 我们自己的代码里已经有正解

`agent_v2/basic_agent.go:2691` 的 AGENTS.md 注入就是这个形态——在 `callLLM` 内 prepend，
注释写明「压缩路径的 msgs/curMsgs 不含它，故压缩看不到、纯 transient（不持久 / 不进 cp）」。
**框架对「harness 层指令」这一类内容早就知道正确答案**，只是 agent 自己的 system prompt 没搬过去。

### 另一处：摘要 LLM 的 system prompt 靠下标猜

`agent_v2/autocompact.go:438` 的 `systemPrompt = sys[0].Content` 拿**下标**当「agent 的 persona」。
不配 `WithSystemPrompt` 但开了长期记忆注入时，`sys[0]` 是注入的用户记忆 ⟹
**用户的长期记忆被当成摘要模型的 system prompt 发出去**（已实测）。

三方在这一处分两档，**没有一家按位置取**：

| 做法 | 谁 |
|---|---|
| 摘要调用有**自己一份独立的** system prompt | eino / MAF / CrewAI |
| **复用会话自己的** system prompt（摘要指令当最后一条 user） | Codex / DeepSeek Harness |

DeepSeek 把复用的理由写进了注释：让辅助调用成为上次路由请求的真前缀，
好让 provider 的 KV cache 复用而不是失效。**它复用的是一个有名字的字段，不是「列表里第 0 条碰巧是什么」。**

### 有人踩过并修过吗

CrewAI 踩过。2024 年的 `ac331504e`（"Fixing summarization logic"）那版**完全不保 system**，
摘要结尾把整个列表连 system 一起清空；按 role 收集的写法是 2026-02 的 `2ed0c2c04`（"imp compaction"）才加的，
commit message 没写成 bugfix。

## 决策记录（结论五）

| 决策 | 结论 | 依据 |
|---|---|---|
| 三处判据改成**逐条 `Role == system` 豁免** | **不选作为「搬迁的替代品」；但搬迁之后仍然给剩下的 system 加了保护** | ⚠️ **原文给的两条理由后来都被推翻了，如实留档**：<br>① 原写「长期记忆注入也是 system，逐条豁免会让它**永不可丢**，而它本来就该可丢」——**假的**。`isProtectedGroup` 早就保护 `StoreInjectedAnnotation`，它一直不可丢；代码给的理由还正好相反（删了下次 Search 若不稳定内容会突变 → 用户感知「AI 突然失忆」）。<br>② 原写「skill body 与 persona 挤在同一条 system 里会被一起锁死」——**搬迁之后不成立**，那条 system 整个不在被压缩的流里了。<br>**结论怎么变的**：拒绝它当「搬迁的替代品」仍然对——逐条豁免救不了 few-shot（user/assistant 角色），而 few-shot 被摘掉是同一根因的另一半。但搬迁之后剩在流里的 system（调用方塞的）该不该保护是**另一个问题**，答案是该，已于 2026-09-08 落地为「组里**含** system 就整组保护」（判据是「含」不是「全是」，见 backlog）。<br>⚠️ 这一行是**溯源盘点**揪出来的：这条决策没问过任何一家，理由是我自己推的，而且推错了两条。 |
| **装配时把 sysPrompt 并进模板的 system 块** | **排除** | 这是给坏判据造一条不变量去迁就它。且它只挡得住模板这**一个**入口——以后任何东西 prepend 到最前（A2A / agui / 业务塞的）同一个坑再来一次，正撞 CLAUDE.md「立一条新规矩，同一次就要问哪几条路能到这儿」 |
| **把 sysPrompt + 模板渲染搬出被压缩的列表**，跟 AGENTS.md 同处 transient prepend | **选它** | 六家现成先例；**我们自己对 AGENTS.md 已经这么做了**（`basic_agent.go:2691`）。收益是**判据消失**而不是变准——以后谁再往消息流最前面塞东西都不会误伤 |
| 长期记忆注入要不要跟着搬 | **不搬** | 它已有 `StoreInjectedAnnotation`，四层压缩按 marker 一致防护。⚠️ **原文这里还写着「且它本来就该可丢（下轮重 Search 再注入）」——那句是错的**，跟代码和代码给的理由都相反：`isProtectedGroup` 明确保护它，godoc 的理由是「删了下次 Search 若不稳定内容会突变 → 用户感知 AI 突然失忆」。这条决策**没问过任何一家**，是我自己推的，理由还推错了——溯源盘点时揪出来的。结论本身（不搬）仍成立：搬走意味着它每轮 transient 重放且不进 cp，而它跟 sysPrompt 的关键差别是**它不是 agent 配置的纯函数**（要跑 Search，可能中断、可能失败），重算成本与失败面都不一样 |
| `sys[0].Content` | **已修（2026-09-08）**：按 `StoreInjectedAnnotation` 跳过注入的记忆再取，一条不剩时不填 | 它跟前导块是两个 bug，搬迁一点都治不到它——搬完之后 `sys[0]` 反而**更**可能是注入的记忆。这个修法在搬迁前后都对：搬完 `sys` 只剩注入 ⟹ 跳完为空 ⟹ 退化成「摘要调用无 system prompt」，正是六家的形态 |
| 前言要不要拼进 `OnModelStart` 的入参再劈回来 | **试过，已撤** | 零先例；实测两处「N=1 对、N≥2 静默错」（跨半顺序丢失 / 「换」退化成「加」）。改成 `ModelInput.Preamble` 与 `Messages` 两个并列[消费]档字段——合并再拆开这件事本身就是坑的来源 |
| 保护到「一组可丢的都没有」时要不要最后一档连受保护的也丢 | **不加，保持现状** | 见下节。五家里唯一有这一档的，它的两条前提我们都不具备；而两家与我们同构处境的，答案跟我们一样 |
| 搬完之后列表里剩下的 system（业务/调用方塞的）怎么办 | **已落地，但形状跟原文写的两处都不同** | ⚠️ **原文写的是「照 MAF：逐条保护 + 预算兜不住时最后一档一起丢、用不同 reason 标出来」——这是最近这批决策里唯一一条「单家来源且真进了生产代码」的**，独立盘点时揪出来的。两处后来都变了：<br>① **粒度不是「逐条」是「整组」**（`groupContainsSystem`，组里含 system 就整组保护）。改的理由不是抄谁，是量出来的：我们的分组规则里 `RoleAssistant` 不开新组，「system 紧跟一条无 RunID 的 assistant」会混成同一组，逐条/全是 system 两种判法都漏它。<br>② **最后一档没做，而且是查完六家后明确决定不做**——见上一行。上一行才是当前结论，这一行的「一起丢」已作废。<br>⟹ 教训不在结论上（结论后来自己走对了），在**依据的宽度**上：这一行当时只有一家佐证，而它恰恰是六家里前提最不像我们的那家。见本节末尾那条加权原则。 |

### 补调研：可改写的钩子该不该看见 system prompt

搬迁落地时撞上一件调研没覆盖的事：我们的 `OnModelStart` 的 `input.Messages` 是**[消费]档**
（业务改写生效），仓里有 6 条既有断言钉着「业务在这里看得到 agent 的 system prompt」。
前言搬走 = 静默拿掉一个已文档化的能力。第一版实现为此做了「打 marker 拼进 `Messages`
过 hook、返回后按 marker 劈回来」——**这一步是推理出来的，没查过别人**，于是补了一轮。

结论：**零先例，而且我自己的代码里就有两处静默错。**

| 框架 | 模型级回调入参含不含 system prompt | 可不可改写 | 专门改它的钩子 |
|---|---|---|---|
| LangChain `on_chat_model_start` | 含（它本来就是列表里的一条） | **不可**——给的是 tracing 专用副本，返回值被丢弃 | 无 |
| LangGraph `pre_model_hook` | **不含**（prompt runnable 在它下游才拼） | 可（`llm_input_messages` 被消费） | 无（直接改 `prompt` 参数） |
| eino `callbacks.Handler` | 含 | **不可**（godoc 明禁改写，`OnStart` 只返 ctx） | 有：`ChatModelAgentContext.Instruction` 独立字段 |
| MAF `ChatMiddleware` | 不含 | 可，走 `context.options["instructions"]` | 有：同左，独立字段 |
| OpenAI Agents `call_model_input_filter` | 不含 | 可，返回 `ModelInputData{input, instructions}` | 有：`instructions` 与 `input` **并列字段** |
| Codex / DeepSeek / AgentScope / Claude Agent SDK | — | — | 无模型级钩子（DeepSeek 另有 `system-prompt/assemble` 装配 waterfall）|

**只有两种形态，没有第三种**：system prompt 在消息列表里（那压缩层就得自己认，正是我们拆掉的那颗雷），
或者它是独立字段（**凡是给了可改写钩子的，都把它做成钩子入参上的一个独立字段**）。
我那一版处在没人待过的位置：独立存储 + 合并入参，靠一次合并和一次劈开缝起来。

**而那道缝有代价，实测两条**（都是「N=1 对、N≥2 静默错」）：

- 业务把一条消息插到前言**之前** ⟹ 劈完跑到了它**之后**。分区只保各半内部有序，
  跨半的相对位置整个丢掉。实测出 `[REAL SYSPROMPT, BUSINESS FIRST, user]`。
- 业务造一条新 system 想**顶掉**前言 ⟹ **两条都发出去**且旧的排在前面。
  实测出 `[REAL SYSPROMPT, REPLACEMENT PROMPT, user]`。

⟹ 改成 `schema.ModelInput{Preamble, Messages}` 两个并列的[消费]档字段（`ModelInputData` 那个形状），
marker 那套连同 `markAgentPreamble` / `splitAgentPreamble` 一并删除。既有断言改的是**坐标**不是内容
（`in.Messages[0]` → `in.Preamble[0]`），能力一条没少，反而多出「整段替换前言」这个原先静默失效的动作。

**顺带两条比形状更值钱的前提**（都有源码支撑，将来接观测层时用得上）：

- **只读钩子应喂「改写之后」的值**——OpenAI 的 `on_llm_start` 收的是 filter 之后的
  `filtered.instructions, filtered.input`，不是原始值。
- **「完整请求」这个视图属于 trace，不属于 hook**。OTel GenAI semconv 定义
  `gen_ai.system_instructions` 时明写「provider 分开给的就分开记，混在历史里的才记进 input.messages」——
  标准的立场是按实际形状记，不是为观测者伪造一个合并视图。observability 打开内容采集时正是这么记的：前言单独记成
  `gen_ai.system_instructions`，对话流记进 `gen_ai.input.messages`。

### 补调研：保护到腾不出空间时怎么办

给中段 system 加保护之后，「一组可丢的都没有」变得更容易出现。我们现在的行为是
`emergencyDrop` 原样返回，上游 retry 循环发现「压缩后条数没变」当场 `break`，
把原始的 prompt-too-long 还给业务——`isProtectedGroup` 的 godoc 记着这是有意取舍。

**问的是「别人有没有最后一档」，查完的答案是：只有一家有，而它的前提我们不具备。**

| 框架 | 兜不住时 | 最后一档 | 调用方看得见吗 |
|---|---|---|---|
| DeepSeek Harness | 选不出压缩区 → 保留原始 provider 错误（源码注释原文 `preserving the original request error`），重试有上限 | 无——真实溢出时只把「保留尾部」的预算降到 0，**配对约束一步不放松** | 是：原错误 + warn + 事件 |
| Codex 主链路 | 上下文超限 → **直接返错**，不裁剪不重试 | 无 | 是 |
| Codex 摘要调用内 | 逐条丢最旧（自动带走配对方），下限 1 条，触底发 Error 事件 + 返错 | 有，但**只作用于摘要那次调用的输入**，不作用于主请求 | 是 |
| MAF | 丢光非 system → 再丢 system → 仍超就静默返回，可产出空列表 | **有**（`token_budget_fallback_strict`） | 半：只有一个消息注解，无事件/日志/错误 |
| eino reduction | 选不出区间 → 原样返回；省得不够多 → 整次放弃 | 无 | 弱 |
| LangChain `trim_messages` | 静默返回超预算或空列表 | 无 | 否 |

**MAF 那两条前提，我们都没有**：

1. **它没有 error-reactive 路径**。压缩只跑在 `before_run` / `after_run`，全仓 grep
   provider 溢出错误零命中（只命中 skills 的参数校验）。也就是说**「把精确错误还回去」
   对它根本不是一个选项**——它不是在响应一个错误。我们是，而且我们就在那个循环里。
2. **它的预算是软的**：`input_budget = context_window - max_output` 再乘 0.8。strict 档开火时
   离真实上限还有大量余量，「丢 system」在它那儿是配额执行；在我们这儿会是「已经贴着硬
   上限还得丢」。语义不同。

顺带一个反直觉处：MAF 的 strict 档按**文档序**丢，也就是**打头的主 system prompt 比中段
临时指令先死**。像是位置序的副产物而非设计意图——真要抄，这个顺序对我们是错的。

⚠️ **一条加权原则（这轮差点栽在上面）**：问「别人有没有 X」时，**唯一有 X 的那家会自动占据
全部注意力**，而它往往正是前提最不像我们的——因为它有 X 恰恰是因为它的处境不同。正确的权重
是**按前提相似度**排，不是按「谁有这个功能」排。这一轮结构最像我们的两家（有 error-reactive
路径 / 有配对约束下的「选不出来」）给的答案跟我们一样，而版面一度全给了那个前提不成立的。
上一轮同样如此：拿它的 strict 档当过「你担心的腾不出空间？人家有解」的答案，挖开才发现
它丢的是文档序、主 system prompt 比中段指令先死。

**所以保持现状。** 支持它的第一性理由三条（都从源码读出来，不是类比）：配对是硬约束不是
策略（两家在真实溢出下都不放松它）；精确错误比静默降级值钱（业务据此能换模型 / 让 LLM
重读卸载全文 / 开新会话，而 MAF 的注解给不出这个）；受保护内容按定义就是「丢了毁掉正确性」
的东西——尤其卸载占位符，丢了 key 等于永久丢失原文。

**真正的空档不在「兜底丢弃」，在「压缩手段」**：六家里有三家有一档**缩内容而不删消息**的
手段（eino 把工具结果换占位符、dsh 与 LangChain 各有内容截断），那正是绕开配对约束的钥匙。
我们在这一层只有「删组」一种工具，所以才会撞上「全 protected 无组可删」。见 backlog。

### 调研列的两条代价，实测不成立

调研把「历史回放里带着 system 会重复注入」和「Handoff Rewrite 视角要重定语义」列为搬迁**必须配套**的改动。
两条都量过，**都不成立**：

- 真 BasicAgent 跑两轮 + `WithHistory`：`HistoryStore` 里 4 条，**system 0 条**（历史写入点只记 user 输入与 assistant/tool 产出）。
- 不配 `WithHistory` 走 session events 的那条路：`SelfHistoryCompacted` 里同样 **system 0 条**。
- `rewriteCrossAgentMessage`（`agent_v2/session.go`）对 system 原样透传，而它的上游根本送不到 system。

⟹ 搬迁**不需要**配套改历史层。这条差异记在这里是因为它反方向：调研把范围估大了，而按它的清单做会引入没必要的改动。

## 待补的对照角度

本篇现在覆盖了**两个**切口：「卸载去重」（结论一~四）与「压缩时怎么认出 system prompt」（结论五）。
4 层压缩的其余部分**仍是对照零覆盖**：AutoCompact 的摘要**质量**（触发阈值那半已随结论五挖到）、
ReactiveCompact 的 prompt-too-long 检测与重试语义、9 段切分算法、跨层 marker 协议本身。
按那条 backlog 的做法，**改到哪一层之前先挖哪一层**，不做一次性大扫除。

⚠️ 结论五挖出来的东西**比提问范围大**：问的是「system prompt 怎么被认出来」，
但同一个根因下 **few-shot 示例被 AutoCompact 摘成一段散文**是另一半——模板渲染的 few-shot
同样在被压缩的列表里、同样每轮可重算、同样没有任何机制保护它。修 system 那半时要一次算清，
否则就是 CLAUDE.md「立守卫时列全所有入口」那条的下一个实例。

## 决策记录（回合结束后压缩：做过一版，已撤回）

2026-09-21 从增量调查引入「回合末压缩」（`AutoCompactConfig.PostTurnThresholdPercent`），
2026-09-22 撤回（`81403e3f`）。**权威记录与完整实测证据在
[industry-survey-2026q3.md](industry-survey-2026q3.md) 的「压缩后置到回合结束」一节**——
本表只留指针，不重复描述（两处各写一份必然漂）。

一句话：立论的「把延迟挪到用户不等待的时刻」在框架侧**没有前提**（`Run()` 同步返回，
实测单层 +121ms、链上 N 个线性叠加），而产出的消费面窄到**框架自带的两个 wire 入口
无一命中**。它还静默推翻了本篇原则#5。

三条**与撤/留无关、可复用**的对照结论，记在这儿备查：

| 结论 | 证据 |
|---|---|
| 同行做这件事的理由**不是延迟，是历史重写的安全点**——循环还在用那份 transcript，中途改不安全，回合末是唯一安全点 | 两家的注释各自写明（见调查文档引文） |
| 五家实现里**没有一家**是「追加 marker + 读取侧自己算截断点」；全都追加一条**自带被替换内容或跨度**的记录（完整替换后历史 / 跨度端点 / 新父指针） | marker-only 形态 = 写入侧与读取侧各有一份「这儿是哪儿」的判据，必然漂 |
| 「把延迟挪走」同行给的是**另一个形状**：提前起跑（离阈值还有 N 个百分点时后台先摘好缓存，撞线只做应用，撞上在飞就等它）。它敢放后台的前提是「只读快照、不改共享态」+ **长驻执行体** | 我们 `Run()` 会返回，活过它的 goroutine 没有主人（谁启动谁负责退出）⟹ 前提不成立，只备查不照抄 |
