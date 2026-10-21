# ISSUE-99：增量 skill listing 的静默失效——已随功能删除关闭

**状态**：CLOSED（2026-07-24，随 `WithIncrementalSkillListing` 一并删除，不再存在触发路径）
**级别**：曾评 MEDIUM
**价值**：本条**不是**为了追踪一个待修 bug 而留，而是「删掉这个功能」这个决策的实证依据，
以及一条通用教训——**别把「必须一直在模型眼前的状态」放进会被压缩的对话流**。

## 曾经的问题

「已宣布过哪些 skill」有**两个真相源**——session KV 与 prior 消息上的名单——框架取**并集**。
于是这条不变量可以不成立而框架不知道：

> 从 prior 回收到的每个名字，其宣布消息这一轮真的在 prompt 里。

触发链：业务 `WithHistoryCompactor` 砍掉 listing（它是 prior 里最旧的一条，token-aware 压缩
砍头是典型行为）→ KV 仍说"发过" → 不补发 → 而增量模式下 `activate_skill` 的描述里本来就
没有 skill 名 → 模型**失去所有获知 skill 名的渠道**，且全程零报错。

框架自带的两条截断路径（`truncatePriorPreservingMarkers` + 4 层压缩）都保 marker，业务的
`HistoryCompactor` 那条不保——同一个 if/else 的两支做同一件事，待遇不同。

## 为什么最终选择删功能而不是修 bug

这不是一个孤立 bug，是**设计位置错误的又一次表现**。同一根因的历史记录：

| 修复 | 表现 |
|---|---|
| `deac747` | 换 session 接着同一场会话时 listing 每轮重发且删不掉 |
| `c26b7d2` | marker 保护**反转成删除** |
| R14 audit HIGH-1 | 同 Run 内 prior 取到刚写的 listing → 双份 |
| R17 audit MED-2 | 业务把 summary 写回 events → cutoff 截掉早期 listing |
| R19 audit HIGH-2 | 并发 Run 非原子 → events 累积多条 |
| 本条 | HistoryCompactor 砍掉 → 静默永久失效 |

根因一致：**一份必须一直在模型眼前的状态，被放进了会被压缩 / 截断 / 换源 / 重建的对话流**。
于是每一条会动对话流的路径都必须单独记得保护它——删除前 `SkillListingAnnotation` 散布在
**5 个文件 14 处**，漏一条就是静默失效。而且这类 bug 会持续产生：将来每加一条动对话流的
路径（新压缩层、新截断策略、新历史投影模式），都要记得加保护。

考虑过的两个修法都不理想：

- **判定只认 prior 不并 KV**（让不变量精确成立）——**无业内先例**，是自创方案
- **契约声明要求业务保 marker**——依赖业务遵守，不遵守仍是静默失效

而实测六家的结论是：**框架层（eino / MAF / OpenAI Agents SDK）零家把 listing 放对话流**，
全部放在工具 description 或 system prompt 这种「不会被压缩」的位置，因此**根本不会遇到这
一类问题**；只有产品级 CLI（会话极长、上百 skill、自控 provider）才做对话流增量，并为此
付出维护状态同步的代价。

⟹ 与其用一个无先例的自创方案去补一个框架层本不该有的问题，不如回到框架同侪的位置。
详见 [`three-way-comparison/skill-system.md` §2.1](../description/three-way-comparison/skill-system.md)。

## 留下的通用教训

1. **状态的存放位置决定了要写多少防护代码**。放进对话流 ⟹ 每条动对话流的路径都要单独
   保护，漏一条就静默失效；放进每轮重新生成的位置（工具定义 / system prompt）⟹ 零防护代码。
2. **两个真相源取并集 = 不变量可以不成立而没人知道**。要么单一真相源，要么显式对账。
3. **静默失效比报错严重得多**。这个 bug 的全部危害来自"零报错"——模型只是变笨了，没有
   任何信号。设计时优先让失效可观察。

## 同批发现、与本条无关而独立保留的修复

- `agent_v2/agent_tool.go` 里按 annotation 过滤父框架元信息的那段：**本路径读不到
  annotation**（`HistoryFor("")` 的空 owner 让 assistant/tool 走 `rewriteCrossAgentMessage`
  被重建成 `schema.UserMessage`，整个 Annotations 丢掉）。当年它看着"有效"只因挡的 listing
  恰好是 RoleSystem 原样穿过；skill body 打在 tool_result 上，**从来没被挡住过**。已随本次
  删除一并清掉并在注释里写明原因，防止将来再加一个同样无效的过滤。
- 「`SkillBodyAnnotation` 打在 tool_result 上（非 inline 模式）」的表述不准——inline 模式的
  terse-ack tool_result 也打（R19 HIGH-1 fix），两种模式都打。
