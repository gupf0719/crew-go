# 带置信度的路由决策：业内对照

> 调研时间 2026-09-22。本地源码按当时拉取的版本读，网上产品按当日文档。设计本身见
> [`doc/v2/route-decider.md`](../../v2/route-decider.md)。

## 触发场景

给 RoutingAgent 加「决策方报置信度、拿不准就升级」时，要回答几个设计问题：门槛放在哪一层、
看不看第一第二名的差距、报错和低置信走不走同一条路、弃权长什么样、回落到哪里、置信度要
不要进观测。

## 一、对照表

| | 门槛放在哪 | 看前两名差距 | 报错与低置信同路 | 弃权的形态 | 回落目标 / 澄清 | 观测 |
|---|---|---|---|---|---|---|
| **crew-go** | 每一档决策方各自的门槛，可按分支覆盖 | 不看，交给决策方算进置信度 | 同路升级，`Reason` 区分 | Target 为空串 | 业务排的升级链：决策方 → classifier → 兜底；问用户靠业务写一档 | 路由决策事件带全字段，不进指标 |
| Rasa | 独立的 FallbackClassifier；CALM 挂在目标 flow 上 | 看（ambiguity_threshold） | 不同路（CALM 区分内部错误与理解不了） | `nlu_fallback` 意图 | 业务 Rule；确认 → 重述 → 兜底；多个 flow 同时匹配时列候选让用户选 | 保留原排名 |
| Dialogflow CX | 每个 flow 的 classificationThreshold | 不看 | 不同路（webhook 错误单独一族事件） | `NO_MATCH` 事件 | 业务 handler，按次数逐级 | `Match.confidence` |
| Semantic Router | 每条 route 自己的 score_threshold | 不看 | 未核实 | `name=None` | 调用方自己处理 | `similarity_score` |
| NeMo Guardrails | 意图识别配置 | 不看 | 不同路（搜索出错直接抛） | 固定 intent | 固定 intent 或回落 LLM | 无 |
| RouteLLM | 请求里的模型名 | 不看 | 部分同路（输出无效直接升级） | 无 | 强模型 | 只有计数 |
| TypeSafe 官方文档 | 业务代码，按分支风险定 | 置信度基本不含差距，概率另作他用 | 不同路 | none 选项或单独的是非题 | 转人、问用户、换系统 | 响应字段 |
| crewAI 记忆召回 | 框架内部配置 | 不看 | 同路 | — | LLM 深挖 | 无 |
| MAF | 无 | — | 同路（条件异常落到 Default） | 必须配 Default | 没转交就问用户 | 无 |
| LangGraph / eino / openai-agents / agentscope | 无 | — | 报错即失败 | None、报错或直接回答 | 业务自己实现 | 仅 openai-agents 在 span 上记多个 handoff |

（上表是 2026-09-22 调研时的结论。其中 agentscope 读的是 2026-05 的本地克隆，已经过时：它的 Python 版 2026-09-22 当天就合入了
Jev 支持。10 月的完整情况见 §五。）

## 二、各家要点

- **通用编排框架都不带置信度。** LangGraph、eino、openai-agents、agentscope、MAF 的路由原语
  只接收已经定好的标签；未知标签报错或落到默认分支。（这条结论里 agentscope 读的是过时的 5 月克隆；它的 Python 版
  2026-09-22 合入了 Jev 支持，Java 版与 Go 的 agent-sdk-go 也有了，见 §五。）
- **带门槛的都是有自己打分器的平台。** Rasa、Dialogflow、Semantic Router、NeMo、RouteLLM
  各自的门槛只和自家分数比，没有跨打分器比较的需求。
- **门槛常挂在具体目标上。** Rasa CALM 的 flow、Semantic Router 的 route、TypeSafe 文档的
  分支风险（转账审批要 0.9 以上才免确认）。
- **Jev 的置信度主要看第一名。** TypeSafe 的 LLM 适配器让 LLM 自报概率，再按
  (p_max − 1/n)/(1 − 1/n) 算置信度；Jev 没有公开算法，本仓拿 2204 次实际响应比对，同一公式与
  返回值平均相差 0.001，只看 p_max 小于 1 的 342 次平均相差 0.005、最多 0.019。它随候选数 n
  变化，第一、二名的差距对它影响很小。
- **TypeSafe 升级到推理模型的做法**是另问一道「这题难不难」的是非题，不直接用选择题的置信度。
- **报错与低置信**：对话平台大多分两条路，因为两者的用户体验不同（「系统出错」和「没听懂」）；
  内部记忆召回、工作流条件这类场景常合成一条。

## 三、crew-go 取舍

调研的结论是：门槛取多少、拿不准怎么办，这类策略业内不放在通用框架里。本仓照此办理，
框架只提供机制，策略全部留给业务：

- **框架不定门槛值，也不定升级去向。** 门槛由业务按档、按分支给；链上排哪几档、要不要问
  用户，都是业务的配置。
- **框架提供比较、升级、记录三件事。** 理由是一个业务代码补不上的缺口：Router 与 classifier
  互斥，要在 Router 里「拿不准就交给 classifier」，业务只能自己重写 classifier 的调用，连同
  历史摘除、存档命名空间、`route_to` 的路由授予与收件人，这些都是框架内部协议（后两样业务代码根本
  拿不到：授予只经 RoutingAgent 给分类器的 RunOption 下发，收件人由引擎铸）。
- **门槛只和同一档比。** 不同决策方的分数含义不同（Jev 随候选数变，LLM 没有校准过的分数），
  所以不设跨档的统一门槛，classifier 作为不报分数的一档排在决策方之后。
- **报错与低置信同路。** 路由场景的下一档是另一个决策者，决策服务挂了时交给它正是想要的；
  对话平台分两条路是为了给用户不同的回复，这在路由层不存在。两者用不同的 `Reason` 记录，
  报错率与不确定率仍能分开统计。
- **不看前两名差距。** 这件事由决策方自己决定怎么折算进置信度，框架不替它规定。

## 四、决策记录

| 日期 | 决策 | 关联 |
|---|---|---|
| 2026-09-22 | 框架提供升级链机制，门槛与链路由业务配置；门槛按档、可按分支覆盖 | `doc/v2/route-decider.md` |
| 2026-09-22 | 报错与低置信同路升级，用 `Reason` 区分 | 同上 |
| 2026-09-22 | 不内置「问用户澄清」的决策方，用现有接口组合 | 同上 §4.3 |
| 2026-09-22 | 不新增升级率、置信度指标，事件带全字段，指标待对照监控上报规范后再定 | 同上 §6 |
| 2026-10-14 | 影子模式做成 `WithRouteDecider` 的选项 `WithShadow`，按档设、同步执行、对路由不可见；不让业务写「总是弃权」的包装 | 同上 §2.8、§6；本文 §五 |
| 2026-10-15 | 决策模型开不开只在装配时定（不挂 / 影子档 / 正常挂），不加运行时开关 | 同上 §4.7、§6；本文 §六 |

## 五、2026-10-14 补充：影子模式，以及业内怎么接 Jev

调研来源：本地源码（agent-framework 10-13、codex 10-14 拉取；agentscope 读的是 10-14 的上游 `origin/main`），
AgentScope Java 与 LangChain 的官方文档，Pydantic AI 文档，Ingenimax agent-sdk-go 的 README 与示例。

### 影子模式（接管之前先观察）

| | 形态 | 执行方式 | 记录什么 |
|---|---|---|---|
| **crew-go** | `WithShadow()`，按档设；影子档永不采纳，对路由不可见（见 `doc/v2/route-decider.md` §2.8） | 同步，业务自己给超时、自己抽样 | 路由决策事件里一条 `Shadow` 为 true 的记录；够门槛记 `shadow`，其余记平常的原因 |
| AgentScope Java（Jev 扩展） | 每种用途一个 `JevExecution.Mode`：OFF / SHADOW / ENFORCE；切换要显式改配置、重新装配；不同用途可以不同 | SHADOW 同步发起判断，受这次判断的总预算约束（含重试与退避） | 观察器收到 Record：用途、版本、模式、状态（DECIDED / INCONCLUSIVE / ERROR / CANCELLED / SKIPPED）、耗时、建议；不含消息原文、工具参数、密钥、原始错误 |
| Codex | 配置开关 `shadow_selection_enabled`：便宜的 skill 选择器以影子模式运行，不改 prompt | 放在阻塞型 worker 上，不拖慢模型输入的构造 | 拿实际被调用的 skill 做对照，发实验指标；注释写明评估完就删 |
| MAF | 安全中间件 `block_on_violation=False` | 同步 | 违规记告警与审计日志，照样放行 |
| LangChain（langchain-typesafe 的实验中间件） | 没有观察档：`AutoModeMiddleware` 判成有风险就返回错误 ToolMessage，文档说明它不发起审批 | — | — |

**取舍**：照多数做法做成框架自带的开关，而不是让业务包一层。粒度定在「一档」，因为我们的决策方本来就按档挂。
同步执行而不学 Codex 放后台：后台的结果赶不上这次路由的决策事件，续跑时怎么重放也说不清；延迟靠超时与抽样控制。
Codex 那种「拿实际调用做对照」在路由上对应「拿实际走的分支做对照」——两边都不是标准答案，分歧要人工复核。

### 业内怎么接 Jev（2026-10 状态）

两种形态：

- **当成「模型」接**：MAF 的 `agent-framework-typesafe`（alpha）把它做成 `TypeSafeChatClient`，由 Jev 选工具和枚举型参数，
  还用作 `SecureAgentConfig` 的隔离区分类器、循环里的评审；它自己写明不生成文本、不支持流式。Pydantic AI 的 `TypeSafeModel`
  挂在单独的 `DecisionModel` 基类下。agentscope（Python）新加了一层与聊天模型分开的分类模型抽象 `ClassifierModelBase`
  （是非题、选择题、打分题三类问题），实现有两个：`JevClassifierModel`（2026-09-22，同一个提交还加了按 Jev 判断选聊天模型的路由中间件）与基于
  OpenAI Decisions API 的 `OpenAIClassifierModel`（2026-10-10）。
- **当成「决策组件」挂在固定位置**：AgentScope Java（工具选择、执行前防护、模型 / 阶段路由、压缩、记忆准入、证据处理、
  回答审核、监督、轨迹评估等十余种用途，每种都有上面那三档模式）；LangChain 的 `TypeSafeClassifier`，外加实验性的
  `ModelRouterMiddleware`（按一次运行选模型）与 `AutoModeMiddleware`（工具风险门）；Go 的 Ingenimax agent-sdk-go，
  `orchestration.NewJevRouter` 配 `WithJevMinimumConfidence`，README 写明不作为生成文本的 LLM 暴露（示例里看不出低于门槛
  时怎么处理）。本仓（`decision` 包加 `NewRouteDecider`，接 Jev 与百炼两家）属于这一种。

与 Jev 同类的接口出现了第二家：OpenAI Decisions API 2026-10-06 公测（`POST /v1/decisions`，三类问题与 Jev 一一对应，
概率与置信度分开给，单个问题可以拒答）。目前能查到的都是第三方报道，细节以官方文档为准。两家并存之后，「决策接缝与具体
服务无关」更有分量：本仓的 `agent_v2.RouteDecider` 本来就是中立的函数接缝，`decision` 包只是其中一个实现。
2026-10-16 起 `decision` 包两种格式都讲（`decision.ProtocolOpenAIDecisions`）：同一个百炼模型有 System One 与
OpenAI Decisions 两种格式的接口，State 与说明都是文本时实测两种格式的答案逐项相同；对象形输入下
两种格式交给模型的文本不同（System One 由服务端转，OpenAI Decisions 格式由客户端转成 JSON 文本），结果有出入。

没接的：LangGraph、eino、OpenAI Agents SDK、CrewAI 的核心仓（10-13 到 10-14 拉取后仍无；CrewAI 只在它带的 LiteLLM 价目表里
有一条 Jev 的价格）。第三方整理的清单还列了几十个社区接入（路由器、工具调用防护、压缩插件、MCP 服务等），没有逐一核实。

**外部唯一一份防护实测是负面的**：AgentScope Java 2026-09-26 在退款场景上对比（120 个合成会话快照、128 次受保护调用、
阈值 0.8、总预算 2 秒）：Jev（jev-1.13.0）判对 60/128（46.88%），68 次正常退款全部被拦，没有错误放行；qwen3.8-max
判对 118/128（92.19%），1 次错误放行，6 次正常退款受阻（有效响应中准确率 99.16%，另有 9 次响应格式不合格、走了失败兜底）；
检查耗时 P50 475 / 980 ms。对照本仓的路由实测（99.3%）：Jev 擅长在描述清楚的候选里选一个；通用问法下的「这次操作
安不安全」它过于保守。这是把带置信度的决策方推广到审批之前必须面对的证据（见中心 backlog「只有 RoutingAgent 接了带
置信度的决策方」）。

## 六、2026-10-15 补充：决策模型能不能运行时开关

调研来源：本地源码（agentscope 读上游 `origin/main`；agent-framework、openai-agents-python、codex、langgraph 读本地克隆），
AgentScope Java 沿用 §五 的文档调研。

接了决策模型或同类判定组件的几家，开关都在装配时定：

| | 形态 | 运行中能不能切换 |
|---|---|---|
| AgentScope Java（Jev 扩展） | 每种用途一个 `JevExecution.Mode`：OFF / SHADOW / ENFORCE | 不能，切换要改配置、重新装配 |
| agentscope（Python） | `ModelRouterMiddleware(classifier_model, candidates, instructions)`：按判定结果换这一次回复用的聊天模型，判定失败或选了未知候选时用 agent 自己的模型 | 没有开关参数，不用就不挂这个中间件 |
| MAF | `TypeSafeChatClient` 没有开关；安全配置 `SecureAgentConfig` 有 `enable_policy_enforcement` 与 `block_on_violation`（设成 False 时只记不拦） | 都是构造参数 |
| Codex | 影子选择由功能开关 `SkillSearch` 决定（扩展配置里的 `shadow_selection_enabled`），每轮检查一次 | 开关在启动时从配置读入，运行中不变 |

通用框架都有「这一次运行带不带某个组件」的机制，但做在运行这一层，不是给组件加开关：

- MAF：`agent.run(..., middleware=...)`，按次传入 agent、chat 与 function 中间件。
- OpenAI Agents SDK：`RunConfig.input_guardrails` / `output_guardrails`，给这一次运行另配护栏。
- LangGraph：`invoke(..., context=...)` 传入本次运行的上下文，节点读 `runtime.context` 决定怎么做。

**取舍**：只在装配时定，三档与 MAF 安全配置的「关 / 只记不拦 / 拦」同形。「这一次多挂或少挂一档」那种按次组装的形态
不能搬过来：我们的升级链续跑时按真实档的位置对齐，两次运行之间增删档会让在途任务错位。真要运行时开关，只能让关掉的档
原地弃权、不从链上摘掉；目前没有这个需要。用法见 `doc/v2/route-decider.md` §4.7。
