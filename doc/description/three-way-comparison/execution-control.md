# 执行控制：断点 / 节点超时与重试 / 审批分档 / 插话与闸门——业内对照

> 2026-10-20 为 [doc/v2/execution-control.md](../../v2/execution-control.md) 那五样能力做的调研记录。全部基于本地克隆
> 逐行读源码（LangGraph 2026-10-13、eino 2026-09-29、MAF 2026-10-13、Codex / OpenAI Agents SDK 2026-10-14、
> DeepSeek harness 2026-10-17、Grok 2026-09-29），Temporal 与 Claude Code 是公开知识、已标明。插话与闸门的业内对照在
> 上一轮已经做过，见 [industry-survey-2026q3.md](industry-survey-2026q3.md) §二、§三，本篇只记这次新增的结论。
> 路径前缀：`LG/` = `langgraph/libs/langgraph/`。

## 一、声明式断点

### 1.1 谁做了、怎么配

| | 配置面 | 通配 | 运行期与编译期 | 子图 |
|---|---|---|---|---|
| LangGraph | 编译期 `interrupt_before/after` + 每次 invoke 传 | `"*"`（排除 hidden 任务） | 运行期**整体替换**编译期（`[] or X` 关不掉编译期的）；运行期列表不校验、不进存档 | 父图运行期列表不往子图传；子图用自己编译期的 |
| eino | 只有编译期 `WithInterruptBeforeNodes/AfterNodes` | 无 | — | 经 `AddGraphNode(…, WithGraphCompileOptions(…))` 配 |
| MAF | **没有**（搜过 breakpoint / interrupt_before / step_mode 等十个词） | — | — | — |

### 1.2 续跑不重复停：两种做法

- **LangGraph：全图版本戳**。续跑时把所有 channel 的版本盖进 `versions_seen[INTERRUPT]`，判据是「有 channel 比上次停时新
  ，且本超步有命中任务」（`LG/langgraph/pregel/_algo.py:162-186`、`_loop.py:1050-1056`）。一次续跑恰好越过一个边界，之后任何
  前进都重新武装。
- **eino：结构**。存档里存的是**算好的下一批任务**，恢复后直接提交、不再判断点（`compose/graph_run.go:157-246`）。

我们选的是 eino 那种结构性做法（存档存 frontier），外加 `FrontierStarted` 区分「恢复出来的边界开没开始执行」——那是
LangGraph 版本戳解决、而纯结构做法解决不了的一类：运行期断点每次 Run 可以不同，软取消停在边界上之后、续跑时新挂的断点
要生效。

### 1.3 他们踩过的坑（逐条对照我们）

| 坑 | 出处 | 我们 |
|---|---|---|
| 协作式停（drain / 软取消）排在断点检查之前 ⟹ 停在断点所在边界的续跑越过断点 | LangGraph `10f1fabb3`（2026-10-12） | 断点紧跟边界算出就判，排在软取消 / ctx 取消 / durability 写之前；哨兵 `TestBreakpoint_WinsOverSoftCancelAtSameBoundary` |
| `Command` 输入没算作续跑 ⟹ 同一断点反复停 | LangGraph `33fe467d1` | 续跑判据只有一份（`cfg.resumeFrom`） |
| 盖戳只覆盖业务 channel ⟹ 只推进控制流的节点之后断点不再触发 | LangGraph `738f725ae` | 不按 channel 判，按边界判 |
| 续跑标记泄漏给后进入的子图 | LangGraph `4e9ed36f7` / `03bf149eb` | 只有恢复出来的那个边界不判；子图有自己的 Run 与存档 |
| 入口那批（START 的后继）不判断点 | eino `69139992` | 入口也判；哨兵 `TestBreakpoint_BeforeStopsWholeSuperstep` |
| 循环里同一子图节点第二次访问复用了上一次的子图存档 | eino `5b1ab13a` | 子图 ns 带 visit 维，原有约定 |
| 某个入口静默丢掉运行期断点 | LangGraph `87f1c8eb9` | GraphAgent 的 Run 与 Resume 都带；哨兵 `TestGraphAgent_Breakpoint_StepThroughCarriedOnResume` |
| 停下时返回值不带标记（与正常结束不可区分） | LangGraph `main.py:4033-4043` | 返回 `InterruptError`，`AskQuery.Breakpoint` 写停在哪 |
| 外部中断与 after 断点共用 `AfterNodes` 字段，分不出 | eino `graph_run.go:476-490` | Reason + `Breakpoint` 字段，由框架填（`Ask` 清掉业务填的；直接构造 `InterruptError` 时业务别填它） |

### 1.4 业内对「拿断点做审批」的态度

LangGraph 文档明确不推荐静态断点做 HITL，定位是调试 / 逐步执行（`579c7831b` 的 add-human-in-the-loop.md）。推断的原因：
断点不带问题、没有答案通道、按节点名同停同放、运行期配置不持久化。我们的断点返回带 ID 的挂起、能经 Resume / AG-UI / A2A
走完一圈，但仍然**不带问题内容、不能改状态**——所以文档里写的是「位置闸门（继续或放弃）」，要人看着内容批准用 `Ask` 或工具审批。

## 二、节点超时与重试

### 2.1 谁做了

| | 重试 | 超时 | 不配合取消的任务 |
|---|---|---|---|
| LangGraph | `add_node(retry_policy=…)`，可多条按错误类型匹配；同超步原地重跑，失败尝试的写清掉；重试时子图带 RESUMING 从存档续 | `TimeoutPolicy{run_timeout, idle_timeout}`，`Send` 可单独带；按单次尝试计；超时**放弃** async 任务、每次尝试一道写入栅栏；sync 节点配超时**编译期拒绝** | 栅栏挡迟到写（但没包 checkpointer：被放弃的尝试里子图存档照写，调研推断） |
| eino | compose 层没有；adk 只有模型调用级重试 | 只有图级的外部中断 + 宽限 | 宽限后丢下 goroutine、记成 rerun 节点；修过「丢下的工具往已关闭的 generator 发事件」panic（`b937b027`） |
| MAF | 没有 executor 级 | 没有 | 取消兄弟之后**等**它们结束才抛错（孤儿任务会改写恢复后的状态，`277a823e2`） |
| Temporal（公开知识） | RetryPolicy + NonRetryableErrorTypes | StartToClose / ScheduleToClose / Heartbeat | 服务端按 attempt 号拒收过期尝试的结果；worker 里的旧 activity 杀不掉 |

### 2.2 对我们有用的几条

- **超时错误可不可重试要显式声明，不能靠类型继承碰巧决定**：LangGraph `c8c58a076` 之前 `NodeTimeoutError` 继承
  `TimeoutError`（`OSError` 子类，默认不重试名单里），配了重试也一次都不重试、不报错。我们的 `NodeTimeoutError` 不在错误链上
  露出 `DeadlineExceeded`，哨兵 `TestNodeTimeout_RetriedByDefaultPredicate`。
- **重试 × 节点内提问**：LangGraph 重试循环不重建提问计数器，第二次尝试把答过的问题再问一遍（调研探针复现）。我们节点重试套
  `RunWithRetry`，提问序号退回；哨兵 `TestNodeRetry_AnsweredQuestionNotReasked`。
- **墙钟超时先上线、三天后改成空闲超时**（`aeff9549c → d177a0db4 → 800071d0d`）：长时间流式生成的节点用墙钟时限会被误杀。
  我们先做墙钟（与工具级 `WithToolTimeout` 一致），空闲超时记 backlog。
- **「放弃 + 栅栏」在 Go 里不成立**：Python async 的取消在下一个 await 点强制生效，Go 只能靠节点自己看 ctx；放弃等待意味着
  每个写入口（节点私有态、嵌套调用存档、事件、信号）都要加栅栏，业内实现的栅栏也漏了子图存档。LangGraph 在「取消不了」的
  sync 节点上的选择是**拒绝配置**——对 Go 的等价翻译是「只承诺能安全做到的」：协作式取消 + 等返回 + 失效可见。

## 三、审批只读放行 + 审批方式进提示词

### 3.1 只读由谁声明

| | 声明 | 没声明 |
|---|---|---|
| Codex（MCP） | server 自报 `readOnlyHint`，用户 / 管理员逐 server / 工具选档位（Auto / Prompt / Writes / Approve）；Writes 档只有 readOnly=true 不问 | fail-closed |
| Grok | 内置工具按 `ToolKind` 穷举派生；MCP 工具不读注解 | fail-closed（但 Dynamic 工具按参数形状猜读写、认不出时落到 Read——反例） |
| MAF | 工具 `approval_mode`；内置 provider 默认需审批 + 按工具名集合的只读自动批准规则 | 普通工具 fail-open |
| OpenAI Agents SDK | 没有只读概念，`needs_approval` 布尔或回调；注解只作发现元数据，**不影响审批**（有测试钉） | fail-open |
| DeepSeek harness / eino | 没有只读档 | — |
| Claude Code（公开知识） | 只读工具（Read / Glob / Grep）免提示；MCP 工具默认要批准 | — |

MCP 规范（Python SDK `types.py:1247-1290`）：注解全是提示，「Clients should never make tool use decisions based on
ToolAnnotations received from untrusted servers.」。默认值 readOnly=false、destructive=true。

### 3.2 我们怎么取

- 工具实例自声明布尔、fail-closed（Codex / Grok 的方向；MAF 的「按名字集合」会批准任何同名工具，它自己源码里警告过）。
- MCP 注解默认不认，`WithTrustedClient` 逐 server 显式信任（Codex 隐含在「用户为这个 server 选了档位」里，我们显式成一个选项）；
  只读 + 破坏性自相矛盾按有副作用（Codex Auto 档的保守做法；它的 Writes 档在同一输入上给出相反结论，父子策略合并时只好取更严
  的那档，`93beee910`）。
- `approve_always` 在只读档照常生效：Codex 在 Writes 档把记忆降级成一次性（`f6e251c3a`），Grok / MAF 让记住的授权优先。证据
  不偏向任何一方；我们取「用户明说的例外优先于档位默认」，与 `ApprovalAsk` 档一致。

### 3.3 审批方式怎么告诉模型

- Codex：developer 角色的独立片段（`<permissions instructions>`），按内容哈希追踪，变了才追加新快照、丢了补发，不改 system
  （`core/src/context/world_state/permissions.rs`）；on-request 档明写「不要先发消息征求许可、不要用别的工具绕过审批」。
- DeepSeek harness：runtime-context 片段，变了才追加快照，明说为了 KV cache 不改写第一条消息。
- MAF：模式（不是审批）每次渲染进 instructions，外部切换时另追加一条 user 通知——只改 system 的话模型会锚定在历史里的旧状态。
- Grok：主提示词不写权限模式；plan 模式用 system-reminder 注入。
- OpenAI Agents SDK：不预先告诉，只在被拒时给结果。
- **没有一家在每个工具描述里逐个标「需审批」**。

我们：审批配置在一个任务里不变（构造期选项），所以放进前言就是稳定前缀，不需要「变了追加快照」那套；文案讲「会发生什么、该
怎么做」（直接调用、别先征求、别绕开、被拒别原样重试），不列工具名。被拒结果区分「用户拒绝」与「策略拒绝」两种措辞。

## 四、插话与回合结束闸门

业内对照与建议形状见 [industry-survey-2026q3.md](industry-survey-2026q3.md) §二、§三、§九。落地时与那份建议的出入：

| 建议 | 落地 | 为什么 |
|---|---|---|
| 队列挂 `RunSession`、两条队列（新回合 / 本回合） | 调用方持有的 `Inbox` 经 ctx 交进来；只做「本回合」一条，没取走的调用方 `Drain` 自己当下一次输入 | 「新回合」队列在库这层就是调用方下一次调 Run，不需要框架持有；收件箱的生命周期归调用方 |
| 提交入口唯一 + RunID 乐观并发检查 | 不做 | 收件箱是调用方为这次 Run 建的对象，不存在「我打字时换了回合」的错投；它在服务端产品层才需要 |
| 闸门「数据决定」（hook 不返回决策、往收件箱塞东西） | 闸门返回 `feedback string`，所有闸门的反馈都进流 | 同样消灭了「多个闸门谁赢」的合并语义，又把「继续」和「为什么继续」绑在一个返回值上（建议里另一条要求） |
| HITL 逃生口 | 闸门只在模型自然收尾时问；工具审批 / ask_user 的中断根本走不到闸门 | 结构上满足 |
| 与 maxRounds 显式排优先级 | 最后一轮不问 | — |
| 内部决策 LLM 必须拒绝插话 | `runDecisionAgent` 摘收件箱；另外嵌套调用也摘 | 用户是在跟调用它的那个 agent 说话 |

## 五、决策记录

| 日期 | 决策 | 关联 |
|---|---|---|
| 2026-10-20 | 断点：超步边界判、续跑靠结构放行、`FrontierStarted` 区分恢复出来的边界、断点先于软取消、编译期∪运行期、无存档的嵌套图报错 | `fc01bb44`、`c5cf9106` |
| 2026-10-20 | 节点超时协作式 + 失效可见 + 迟到成功丢弃；`NodeTimeoutError` 不露 `DeadlineExceeded`；重试套 `RunWithRetry` | `e6ccb2c9`、`c5cf9106` |
| 2026-10-20 | 只读放行：实例自声明 fail-closed；MCP 注解显式信任才认；approve_always 照常；审批方式进前言 | `7de57454` |
| 2026-10-20 | 插话 `Inbox` + 回合结束闸门一起做，返回 `feedback string` | `c7afccc7` |
