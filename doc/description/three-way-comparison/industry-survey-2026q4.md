# Agent 开发与编排框架进展（2026-09-12 至 2026-11-12）

本轮延续 9 月的行业调查，时间窗口按北京时间计算。发布状态取自官方 release 和文档，执行语义以本地
checkout 的提交、实现和测试为准。§六记录这次同步的起止 SHA。托管能力和没有本地快照的项目按官方公开资料
核对。[9 月行业调查](industry-survey-2026q3.md)

恢复开始区分同一节点里先后发出的每一问。后台任务有了独立于前台回合的身份和通知机制。决策模型进入图节点
和中间件，可独立评估。工具参数、挂起状态和事件流也逐渐进入开发期评测。

## 一、恢复精确到一次提问或一项工具调用

- **LangGraph 1.2.20** 给同一 task 内先后挂起的 `interrupt()` 分配不同 ID。第一个回答槽沿用 namespace 哈希，
  后续把已采纳的答案数量编入 ID。显式开启 `strict_resume` 后，顶层入口恢复当前挂起档时会在写入前以
  `UnknownInterruptError` 拒绝空 map、带陈旧或未知 ID 的 map，以及多个挂起时的裸答案。引擎按键的形状识别 ID
  map，每个键都须由 32 个小写十六进制字符组成。合法 ID map 允许只回答一部分挂起项。SDK 0.4.14 在
  `runs.create`、`runs.stream` 和 `runs.wait`
  暴露该选项，由声明支持它的服务端执行。
  [1.2.20](https://github.com/langchain-ai/langgraph/releases/tag/1.2.20) · [严格续跑实现](https://github.com/langchain-ai/langgraph/commit/dbc60dd11) · [逐问 ID](https://github.com/langchain-ai/langgraph/commit/6ea1751a9) · [SDK 选项](https://github.com/langchain-ai/langgraph/commit/2643345bf)
  9 月发布的 1.2.12 还支持 `interrupt(response_schema=…)`，可把答案结构交给客户端。传 Pydantic 类型时
  引擎在取答处校验，传字典时按原样发布 schema。
  [响应 schema 实现](https://github.com/langchain-ai/langgraph/commit/448a76377)
- **Google ADK 2.11–2.12** 让工作流的工具节点用 `RequestInput` 等待审批，并给 `FunctionNode` 增加
  `require_confirmation`，可以设布尔值或按绑定参数判断。确认请求随节点路径和参数发出。拒绝时，节点跳过
  函数体，返回表示拒绝的输出。2.11 也让 Runner、Workflow 和节点接受 `abort_signal`。
  [2.11.0](https://github.com/google/adk-python/releases/tag/v2.11.0) · [2.12.0](https://github.com/google/adk-python/releases/tag/v2.12.0) · [FunctionNode 实现](https://github.com/google/adk-python/blob/9620b42965305294dba886ff38757dae3478c401/src/google/adk/workflow/_function_node.py)
- **OpenAI Agents SDK 0.24.0** 在 `agents.memory.OpenAIResponsesCompactionSession` 自动压缩前验证
  tool call/result 分组及 hosted MCP 审批请求/响应是否配对，遇到未闭合的组就推迟压缩。该版也补上了同批工具
  一项失败时保留已完成输出。11 月主分支继续修复嵌套审批恢复及工具 guardrail 结果的续跑保留。
  [0.24.0](https://github.com/openai/openai-agents-python/releases/tag/v0.24.0) · [压缩守卫](https://github.com/openai/openai-agents-python/commit/5eb47a5b) · [兄弟工具失败](https://github.com/openai/openai-agents-python/commit/1c1a28df) · [嵌套审批](https://github.com/openai/openai-agents-python/commit/3599632d) · [guardrail](https://github.com/openai/openai-agents-python/commit/3a0c89c8)
- **eino** 在 v0.9.20 补了工具流取消后的存档保留。v0.9.22–0.9.23 主要是消息契约增量，支持 provider 文件引用，
  流式拼接保留 `ToolCall.Extra`。独立的 `alpha/10` 分支在同一进程、同一 store 实例内对同一会话的并发 Run、
  Resume 或 Rollback 返回 `ErrSessionBusy`，同时收窄 `TurnLoop` 的 `GenResume` 回调，将返回值限定为
  续跑参数和缓冲项。
  [工具流取消](https://github.com/cloudwego/eino/commit/b9539ec1) · [v0.9.22](https://github.com/cloudwego/eino/releases/tag/v0.9.22) · [v0.9.23](https://github.com/cloudwego/eino/releases/tag/v0.9.23) · [alpha 会话并发](https://github.com/cloudwego/eino/commit/21ed97b4) · [alpha 续跑](https://github.com/cloudwego/eino/commit/50c4becb)
  v0.10 的设计已把会话事件历史与执行存档分开。窗口内的预发布调整包括动态后台任务心跳，以及减少
  TurnLoop 保存挂起态时的内存放大。
  [v0.10 设计](https://github.com/cloudwego/eino/discussions/1159) · [alpha.33](https://github.com/cloudwego/eino/releases/tag/v0.10.0-alpha.33) · [alpha.34](https://github.com/cloudwego/eino/releases/tag/v0.10.0-alpha.34)
- **CrewAI** 让恢复后的人工反馈沿用被审输出的类型。1.15.40 的 Flow SQLite 持久化可按行 ID 查具体历史状态。
  11 月还加入实验性的单 Agent 对话会话，复用对话 Flow 承载历史与多轮交互。
  [人工反馈恢复](https://github.com/crewAIInc/crewAI/commit/75808b0ec) · [1.15.40](https://github.com/crewAIInc/crewAI/releases/tag/1.15.40) · [实验性对话会话](https://github.com/crewAIInc/crewAI/commit/305d6a0e8)

crew-go 业务代码用 `compose_v2.Ask` 铸造逐问 ID，用 `agent_v2.WithResumeUserInputs` 按 ID 交答案。AG-UI
HTTP 入口要求 Resume 列表对每个当前挂起项提交一条 `resolved` 或 `cancelled`，拒绝未知与重复的 ID。
工具批次首次派发时固定实际工具名和参数，续跑时重放已完成工具的账本条目。节点要按业务内容审批时，可以在副作用前
调用 `Ask`。运行期断点提供位置上的继续或放弃。
[HITL 与 Resume](../../v2/hitl-resume.md) · [执行控制对照](execution-control.md) · [AG-UI 续跑检查](../../../agui/handler.go)

LangGraph 1.2.18 把 `DeltaChannel` 的默认快照间隔从每 1,000 次更新降到 100 次。Postgres saver 新增
`prune(keep_latest)` 后会拒绝含 DeltaChannel 的 thread，因为最新值的重建依赖更早的写入。评估增量存档时，
要在同一批长会话上测写放大、恢复读放大和回收后的可恢复性。
[快照间隔](https://github.com/langchain-ai/langgraph/commit/0fc4f35b6) · [Postgres prune](https://github.com/langchain-ai/langgraph/commit/36bf3887f) · [本仓存档取舍](checkpoint-storage.md)

## 二、前台 Agent 与后台任务各自推进

- **LangChain Deep Agents** 11 月 5 日公布异步子 agent。前台发起任务后立即获得句柄，子 agent 在独立 thread
  执行，可查状态、改指令和取消。改指令可以中断当前子 Run 重开，也可以排到它结束之后。beta 的主动通知依赖
  Agent Server。前台运行中在下次模型调用前收取结果，等待审批时先保留审批。同一部署中的 Deep Agents 子 agent
  完成或失败时还可唤起空闲前台。
  [官方发布](https://www.langchain.com/blog/agents-are-going-async) · [通知条件](https://docs.langchain.com/oss/python/deepagents/async-subagents)
- **Microsoft Agent Framework（MAF）** 把后台子任务的完成结果保留到显式清理或释放，便于前台晚些取回。
  11 月主分支还在 agent-as-tool 共用会话时，将单次 Run 配置的审批中间件纳入父子状态键冲突检查，
  并把声明式 MCP 会话作用域收窄到单次调用。
  [后台任务保留](https://github.com/microsoft/agent-framework/commit/83c719065) · [审批状态键](https://github.com/microsoft/agent-framework/commit/f6aeb41ff) · [MCP 调用作用域](https://github.com/microsoft/agent-framework/commit/df7d02bdc)
- **Codex** 在进程内 mailbox 保留 session 卸载时的仅排队 agent 邮件，并让主动注入唤醒正在 wait/sleep 的回合。
  **DeepSeek Harness** 的 `0.2.2-rc.2` checkout 中，Team 模式按根 Session 记录，SDK 部署可在创建时设默认值，
  已保存的选择优先。审批 projection 持久化策略覆盖值和当前 turn 的打开状态。
  [Codex 邮件](https://github.com/openai/codex/commit/960e878df) · [Codex 唤醒](https://github.com/openai/codex/commit/305db3808) · [DeepSeek Team 模式](https://github.com/deepseek-ai/deepseek-harness/commit/19e5a95802) · [审批 projection](https://github.com/deepseek-ai/deepseek-harness/commit/a9dd195400)
- **OpenAI Responses 的异步工具** 在模型协议上支持延迟交回工具结果。应用在支持的工具定义上设置 `async: true`，
  模型发出调用后可继续生成。应用负责执行，在后续请求用原 `call_id` 交回结果，并管理任务保存、去重和取消。
  [Async tool calling](https://developers.openai.com/api/docs/guides/async-tool-calling)

crew-go 顶层 BasicAgent 的 `WithBackgroundTasks` 可将点名的工具在运行超过 `YieldAfter` 后转入后台，
先返回任务 ID。模型可通过 `background_task` 查、等、取消，也能在发起方下一次调模型时收到结果通知。
任务由进程内 `BackgroundTaskManager` 持有。配置 Checkpointer 时，工具账可以恢复任务 ID。进程重启后
原任务与结果会消失。独立子 agent thread、宿主唤醒和跨进程任务恢复需要额外的任务所有权协议。现有 `Inbox`
和 `WithTurnGate` 已覆盖同一回合的插话与收尾反馈。
[后台工具](../../v2/background-tasks.md) · [执行控制](../../v2/execution-control.md) · [后台子 agent 取舍](harness-survey.md)

## 三、决策模型成为可组合组件

| 项目 | 决策位置与输出 | 回落行为 |
|---|---|---|
| Google ADK 2.13–2.14 | 2.13 引入实验性的 `ClassifierNode` 和 Jev 的 `TypesafeClassifier`。选择题发标签，是非题按 `threshold`（默认 0.5）发布尔值。可将完整答案写入 state，下游收到原输入。2.14 增加 OpenAI Decisions 分类器 | 默认边接住合法但未单独布边的路由。非法标签会报错。2.14 增加 `RefusalAnswer`，节点收到拒答也报错。[2.13.0](https://github.com/google/adk-python/releases/tag/v2.13.0) · [节点实现](https://github.com/google/adk-python/blob/9620b42965305294dba886ff38757dae3478c401/src/google/adk/labs/classifiers/_classifier_node.py) · [2.14.0](https://github.com/google/adk-python/releases/tag/v2.14.0) |
| LangChain 1.5.0 | beta 的 `ModelRouterMiddleware` 在 Run 开头按最近用户消息选聊天模型，保存完整 `ChoiceAnswer`，本次 Run 的各次模型调用沿用所选标签。实验性的 `AutoModeMiddleware` 对配置的工具单独做风险判定 | 模型路由的分类异常直接上抛。工具风险分数达到 0.5 时回错误 `ToolMessage`，工具体不执行。[1.5.0](https://github.com/langchain-ai/langchain/releases/tag/langchain==1.5.0) · [实现](https://github.com/langchain-ai/langchain/commit/b067614) |
| AgentScope | 9 月加入独立分类器抽象、Jev 和聊天模型路由中间件，10 月加入 OpenAI Decisions 后端 | 中间件消费分类结果来选聊天模型。决策模型仍可在 agent 其他位置独立调用。[Jev 与模型路由](https://github.com/agentscope-ai/agentscope/commit/6d32be1d) · [OpenAI Decisions](https://github.com/agentscope-ai/agentscope/commit/98dff833) |
| crew-go | `RoutingAgent` 的 `WithRouteDecider` 选择下游 agent，按档和按分支设门槛。`WithShadow` 同步执行影子档并记录判断，分支选择只采纳非影子档 | 决策失败、弃权、低置信按业务配置逐级升级。现有路由事件保留原因，观测层另有升级计数和置信度直方图。[路由决策](../../v2/route-decider.md) · [指标实现](../../../observability/orch_hooks.go) |

这些接口共享「决策请求 → 带分数的结果 → 调用方执行」的形状。选执行节点、选生成模型、判定工具风险会消费不同输入，也各有失败和降级规则。crew-go 已有升级链和影子档，扩展到其他场景前可以先用同一批样本比较各处的误判代价与延迟。

## 四、开发工具链开始评估 Agent 的过程

- **ADK 2.14** 把 A2A 接入、若干执行环境与成熟 eval API 转为 stable。评测支持每条用例重复执行，`ToolArgsMatcher` 支持精确、大小写无关、数值容差和语义匹配。11 月主分支又加入 pass@k 和 pass^k，分别衡量 k 次里至少一次成功与 k 次全部成功。[2.14.0](https://github.com/google/adk-python/releases/tag/v2.14.0) · [跨次评测指标](https://github.com/google/adk-python/commit/5c7a829)
- **CrewAI** 加了独立的 Jev 离线路由和 guardrail 评测 runner，可在固定数据上比较决策结果。**Cursor Agent SDK** 10 月发布的开发流程则把渠道触发和工具调用断言接进本地及 CI，支持把真实 PR 事件转成可重放的评测 fixture。[CrewAI runner](https://github.com/crewAIInc/crewAI/commit/5d58569ae) · [Cursor Agent SDK](https://cursor.com/changelog/agent-sdk)
- Google Agents CLI 1.5 引入 extension 机制，1.8 提供官方 LangChain 扩展，使初始化、部署和评测共用一套 CLI。
  该机制仍为实验功能。OpenAI 托管 Agents API 在公测中加入沙箱预热和一次性或周期性调度，调度可启动新 session
  或继续已有 session。
  [Google Agents CLI](https://developers.googleblog.com/powering-the-agent-development-lifecycle-with-any-framework-from-adk-to-langchain-and-beyond/) · [OpenAI API 10 月更新](https://developers.openai.com/api/docs/changelog#october-2026)
- **ADK 2.10** 增加实验性的 skill 生命周期配置，可控制临时激活和活跃 skill 数量上限。长期运行时可以按工作阶段回收 skill 的可见工具和资源，减少后续回合携带的内容。[2.10.0](https://github.com/google/adk-python/releases/tag/v2.10.0) · [SkillToolset 指南](https://github.com/google/adk-python/blob/9620b42965305294dba886ff38757dae3478c401/docs/guides/tools/skill_toolset/index.md)

crew-go 可以用脱敏的真实样本扩充路由评测的用例覆盖，复用现有的置信度升级、分歧复核和重复运行。端到端 mock
可核对实际工具名及参数、执行次数和终态事件，真实模型用例可以再测稳定性与延迟。
[现有 routing_eval](../../../example/decision/routing_eval)

## 五、流式续接、协议与可观测性

- **LangGraph** 在服务端提供重连地址、流已关闭且 Run 仍挂起或运行时可重新 join。重放断线期间的事件依赖
  `stream_resumable` 和有效游标。核心也修正了子图消息和工具事件的 namespace，并将 `TracePolicy` 的
  payload 变换限制在 trace 中，保留业务流中的节点数据。
  [SSE 续接](https://github.com/langchain-ai/langgraph/commit/9791c686e) · [事件 namespace](https://github.com/langchain-ai/langgraph/commit/1f0cf816d) · [TracePolicy](https://github.com/langchain-ai/langgraph/commit/8d5637b2d)
  10 月上游也把职责写进仓库指引，LangGraph 聚焦图执行、存档、流和恢复，`prebuilt`、`supervisor`、
  `swarm` 等包进入维护模式，新 agent 开发示例优先用 Deep Agents。
  [上游仓库指引](https://github.com/langchain-ai/langgraph/commit/244ca657e)
- **AG-UI** 的 OpenAI Agents Python 适配器把工具审批映射为协议中断，从客户端回传的消息历史和 `resume`
  条目重建未决调用。获批调用由适配器执行，执行发生在 SDK 的 run hooks 和 tool guardrails 之外。
  LangGraph Python 适配器读取原生 `Interrupt.response_schema`，避免将中断的工具调用记为失败。
  [OpenAI Agents 审批续接](https://github.com/ag-ui-protocol/ag-ui/blob/f6f6c566466c92baf6a8067b5ff8aa059a2b18a9/integrations/openai-agents/python/src/ag_ui_openai_agents/interrupts.py) · [LangGraph 表单 schema](https://github.com/ag-ui-protocol/ag-ui/commit/2acb8d35) · [工具中断事件](https://github.com/ag-ui-protocol/ag-ui/commit/1102ccb7)
- **OTel GenAI semconv** 11 月将进程内 workflow 合入 `gen_ai.invoke_agent.internal` 和
  `gen_ai.invoke_agent.duration`，名称属性统一为 `gen_ai.agent.name`，仍用
  `gen_ai.operation.name=invoke_workflow` 区分工作流。10 月合入 agent transfer 的 mode 和 target，区分工具式委派和
  转交。约定处于 Development。crew-go 目前使用 10 月的 workflow 指标和属性，若跟进需核对仪表盘、告警及 exporter
  配置的消费情况。
  [工作流合并](https://github.com/open-telemetry/semantic-conventions-genai/pull/571) · [agent transfer](https://github.com/open-telemetry/semantic-conventions-genai/pull/447) · [本仓对照](observability-trace-spec.md)

crew-go 的取消终态与 Resume wire `runId` 两处 Q3 问题已有对应回归。调研基线的 AG-UI HTTP Handler 用进程内会话映射维护挂起项。
共享会话的实现和验收见[本轮落地](#九本轮落地)。
[Q3 回查](industry-survey-2026q3.md) · [本仓协议测试](../../../agui/agui_test.go) · [会话映射](../../../agui/handler.go)

复用上游经验时，可以把问题写成可复现的用例。用同一节点的两次提问验证旧答案重发，用同批工具的成功和失败验证已完成结果的重放，用审批挂起时收到后台结果的场景验证通知交付。独立后台子 agent 的设计再根据业务用例确定任务句柄、审批归属、完成通知和退出清理。

## 六、本地源码快照

2026-11-12 同步了 15 个已有参考仓。14 个 checkout 对齐跟踪分支，其中 13 个快进，Grok Build 已对齐。AgentScope 本地与上游分叉，以下以更新后的远端跟踪引用核对。eino 另同步 `origin/alpha/10`，供预发布对照。表里的新增数为同步前后 `git rev-list --count OLD..NEW`，包含 merge 引入的提交，统计区间由各仓起始 SHA 决定。

| 仓库与核对引用 | 同步前 → 同步后 | 新增 | 本轮核对的代表性变化 |
|---|---|---:|---|
| LangGraph `main` | [`a6731f436` → `b4cb0f19e`](https://github.com/langchain-ai/langgraph/compare/a6731f436...b4cb0f19e) | 125 | 逐问 ID、严格恢复、DeltaChannel、SSE 续接 |
| eino `main` | [`58d18430` → `4a4b7186`](https://github.com/cloudwego/eino/compare/58d18430...4a4b7186) | 4 | v0.9.22–0.9.23 消息与流拼接 |
| eino `origin/alpha/10` | [`5b12575f` → `50c4becb`](https://github.com/cloudwego/eino/compare/5b12575f...50c4becb) | 5 | 会话并发、时间线投递、续跑 API |
| LangChain `master` | [`6908e67` → `771d11b`](https://github.com/langchain-ai/langchain/compare/6908e67...771d11b) | 127 | 模型路由、工具风险判断、TypeSafe 决策模型 |
| Google ADK Python `main` | [`f78149a` → `9620b42`](https://github.com/google/adk-python/compare/f78149a...9620b42) | 440 | 分类节点、eval、A2A 与执行环境 |
| MAF `main` | [`1b9879c58` → `22b5e752b`](https://github.com/microsoft/agent-framework/compare/1b9879c58...22b5e752b) | 403 | Python 1.25.0 给 MCP 发现加了页数和项目数上限。主分支还收紧了 MCP 生命周期与审批作用域。[发布记录](https://github.com/microsoft/agent-framework/releases/tag/python-1.25.0) |
| OpenAI Agents Python `main` | [`1c1a28df` → `64fbde5d`](https://github.com/openai/openai-agents-python/compare/1c1a28df...64fbde5d) | 85 | 自动压缩避开挂起审批、嵌套 RunState 恢复 |
| CrewAI `main` | [`75808b0ec` → `7e4efa91c`](https://github.com/crewAIInc/crewAI/compare/75808b0ec...7e4efa91c) | 213 | 对话 Flow 压缩、单 Agent 会话、历史状态查询 |
| AgentScope `origin/main` | [`3a0e73fa` → `3fef100e`](https://github.com/agentscope-ai/agentscope/compare/3a0e73fa...3fef100e) | 137 | SOP、TeamPipeline、分类器。更新远端引用，本地 `main` 有 29 个自身提交 |
| Codex `main` | [`2cab11ca4` → `0702ebed4`](https://github.com/openai/codex/compare/2cab11ca4...0702ebed4) | 736 | 任务所有权、恢复历史、计划附件交接。邮件与 wait 改动见 §二 |
| DeepSeek Harness `master` | [`687f282e86` → `e8f7b93a34`](https://github.com/deepseek-ai/deepseek-harness/compare/687f282e86...e8f7b93a34) | 4,376 | Team 模式、审批 projection、`0.2.2-rc.2` |
| Grok Build `main` | [`2bdd1d6a`](https://github.com/xai-org/grok-build/commit/2bdd1d6a6369de0e8c68132ea4539e9abd9e14a8) | 0 | 公开主分支最后一次同步在北京时间 9 月 30 日。窗口内的同步涉及子 agent attempt 恢复和插话测试 |
| Claude Agent SDK Python `main` | [`c857c31` → `3cf3446`](https://github.com/anthropics/claude-agent-sdk-python/compare/c857c31...3cf3446) | 307 | [v0.2.180](https://github.com/anthropics/claude-agent-sdk-python/releases/tag/v0.2.180)。SDK 等 CLI 空闲后才关 stdin，[保留后续 turn](https://github.com/anthropics/claude-agent-sdk-python/commit/dbc975e)。历史读取[保留并行工具的各条调用和结果](https://github.com/anthropics/claude-agent-sdk-python/commit/beffc50) |
| Pi `main` | [`f1b2e77f5` → `a40abecaa`](https://github.com/earendil-works/pi/compare/f1b2e77f5...a40abecaa) | 1,017 | 11 月主分支合入可挂起工具、持久信号和审批闸门 |
| AG-UI `main` | [`7d14c417` → `f6f6c566`](https://github.com/ag-ui-protocol/ag-ui/compare/7d14c417...f6f6c566) | 931 | 适配器的中断、续跑与事件重放 |
| OTel GenAI semconv `main` | [`06ec68e` → `e6658f14`](https://github.com/open-telemetry/semantic-conventions-genai/compare/06ec68e...e6658f14) | 67 | workflow span / 指标合并、agent transfer、realtime 语义 |

## 七、其他框架与托管运行时

- **AgentScope 2.0.9** 增加两种编排入口。实验性的 SOP 由业务定义有序里程碑，校验通过才推进，挂起时可保存
  `SOPRunState`。框架入口恢复 SOP 自身的运行状态，执行者自己的状态由构造方恢复。`TeamPipeline` 让 leader
  通过工具派单，成员各自维护上下文。同批不同成员可并行，同一成员按派单顺序执行，成员挂起时按 `reply_id` 投回答案。
  [2.0.9](https://github.com/agentscope-ai/agentscope/releases/tag/v2.0.9) · [SOP 状态恢复](https://github.com/agentscope-ai/agentscope/blob/3fef100ee13d70905723d0000608bcce2d7e4a0d/src/agentscope/sop/_engine.py) · [TeamPipeline](https://github.com/agentscope-ai/agentscope/commit/2258f3f4)
- **Claude Managed Agents** 10 月 9 日开放 Dynamic Workflows beta。模型可以生成一个分阶段的工作流，服务端
  在后台运行它、分派子 agent 并汇总结果，宿主从 session 事件流观察 `workflow_run.*`。这是托管服务的更新。
  [10 月发布记录](https://platform.claude.com/docs/en/release-notes/overview) · [Workflow runs](https://platform.claude.com/docs/en/managed-agents/workflow-runs)
- **Pi Durable** 的 11 月 11 日主分支加入实验性的挂起工作。工具可以保存 checkpoint 后挂起，等定时器或带类型的
  外部信号唤醒，调度器会带着原参数和已保存状态重新调用工具。仓内示例以 webhook 交回 CI 结果，挂起期间释放执行槽，
  外部构建靠幂等键在重试时找回。
  这使长等待可以由持久化调度器接手。
  [主分支实现](https://github.com/earendil-works/pi/commit/473515f9c) · [webhook 示例](https://github.com/earendil-works/pi/blob/473515f9c297c75cf2c3ebb3ba027049ed836d27/packages/durable/test/examples/40-tool-waits-for-webhook.ts)
- **Temporal** 10 月 30 日宣布 Google ADK、AWS Strands、LangGraph Python、OpenAI Agents SDK TypeScript 等
  适配 GA。以 ADK Python 为例，模型调用作为 Activity，长生命周期的编排留在 Workflow。OpenAI Agents SDK Python
  适配在这次公告前已 GA。**Pydantic AI** 为 Temporal 长任务加入可选的 `continue_as_new` 回调。
  Temporal 建议开启新历史时，在下一次模型请求前调用该回调，应用决定是否接续并传递所需状态。
  2.66 又给其 harness 接入 Azure Durable Task。
  [Temporal GA](https://temporal.io/blog/ai-framework-integrations) · [Pydantic AI 接续](https://github.com/pydantic/pydantic-ai/pull/7312) · [Pydantic AI 2.66](https://github.com/pydantic/pydantic-ai/releases/tag/v2.66.0)
- **Strands** 9 月推出预装工具、prompt 缓存和上下文管理的通用 harness，10 月发布可本地运行的开源 Decider 2B，
  专门对候选决策打分。双向语音 Agent 也在 10 月 GA，直接支持 Amazon Nova Sonic、OpenAI 和 Gemini。
  [Strands harness](https://strandsagents.com/blog/introducing-strands-harness/) · [Decider 2B](https://strandsagents.com/blog/introducing-strands-decider/) · [Bidi Agents GA](https://strandsagents.com/blog/bidi-agents-now-ga/)
- **Mastra** 10 月 6 日将 Agent Controller 推到 GA。每个会话维护持久 thread，模式可切换模型、指令和工具。
  客户端能订阅实时状态、插话和处理审批，应用层可以围绕同一控制器搭终端或 Web 交互。
  [Agent Controller GA](https://mastra.ai/blog/announcing-agent-controller-ga)

AgentScope SOP 的逐步验收可与 crew-go 通过 GraphAgent 拼接执行和校验节点的做法对照。Pi 和 Temporal 提供了
两种跨进程等待的参考，前者给工具一个可持久化的挂起位置，后者用 Workflow 历史保存调度进度。评估接入时可用同一
业务样本核对谁保存任务归属、谁接收迟到回调、重试从哪一步开始。
[crew-go 编排模式](../../v2/orchestration-patterns.md)

## 八、crew-go 优化建议

先补外部路由样本入口，能用现有评测工具快速验证业务收益。服务需要滚动发布或多副本时，把跨实例审批恢复提到
上线前验证。以下现状以 `47351a0` 为基线。

### 1. 用真实样本决定路由门槛

`routing_eval` 已有升级链、重复运行、标注分歧复核和输入指纹缓存，调研基线的样本入口读内嵌 JSONL。增加外部 JSONL
输入后，先选一套现有业务分支，用脱敏的请求和上下文按分支人工标注，比较同一批样本上的升级率、自动承接比例、
自动承接中的错路由率及 P95 路由延迟。线上有流量时，可用同步的 `WithShadow` 按请求抽样并设短超时，
收集决策分歧供复核。影子结果和线上实际分支都可以作为复核线索，业务标注作为对照答案。
[现有待办](../../code/todo.md) · [评测入口](../../../example/decision/routing_eval/main.go) · [影子档契约](../../v2/route-decider.md)

### 2. 让同一条挂起审批跨实例续跑

用两个 AG-UI Handler 共用一个内存 Checkpointer 做了对照。A 发起 Run、回答第一问并停在第二问，第二问交给新建的 B
返回 `404/no_session`。同一份答复交回 A 返回成功，节点拿到 `one+two`，两问各发出一次。调研基线的通用 Handler
在进程内保存从 `(owner, threadId)` 到挂起任务的映射、原请求前文和已采纳答复。
[Handler 状态](../../../agui/handler.go) · [两问测试基线](../../../agui/resume_coverage_test.go)

首期验证 AG-UI 的挂起恢复。用服务端解析的 owner 和 thread ID 寻址，持久化可重建任务的身份、Run 时前文及已采纳
答复，并让协议侧暴露的挂起集合与执行存档对应。Redis / SQL 的任务租约可保护同一任务的执行与清理。会话认领还需
原子核对当前任务和当前挂起版本，同一个任务从第一问推进到第二问时也要拒绝迟到的第一问答复。

单次工具的审批结论随存档走。`approve_always` 和 `approve_always_tool` 规则存在 RunSession 中，限定到任务与
agent 实例。首期沿用已准入调用的结论，新调用按当前规则判定。验收用真实共享后端，A 挂起后退出，由同一图定义的 B
接手完成。核对并发续跑、租约释放后重发同任务的旧答复、异租户请求，以及存档写成后会话记录提交失败。SSE 在客户端收到
中断 ID 前断开时，重连可查询当期挂起项。
[任务租约](../../../compose_v2/task_lease.go) · [审批规则](../../../agent_v2/approval.go) · [AG-UI 使用说明](../../v2/agui.md)

连续两问的成功对照还观察到 `WithResumeAnswers` 的 unmatched 告警，原因是累计答复包含已消费的第一问。AG-UI
已用当次请求的新增答复校验挂起集合，已采纳的历史答复用于重放。日志可据来源区分用户传错 ID 和正常的历史答复。
[告警实现](../../../compose_v2/graph_loop.go)

### 3. 先用现有编排做一个逐步验收示例

借鉴 AgentScope SOP，用 GraphAgent 表达「生成方案 → 校验 → 人审 → 执行」。校验节点产出结果，条件边按结果决定是否
推进。人审放在独立节点，经 `compose_v2.Ask` 挂起。用失败回到方案生成、审批拒绝结束、同一审批续跑后执行一次这三条
路径展示已有能力。若业务需要一次收集多个字段，再让提问的 schema 随存档往返，前端按 schema 渲染，协议入口在启动
续跑前校验答案。验收时传错类型或缺字段，挂起档可继续答。
[SOP 参考](https://github.com/agentscope-ai/agentscope/releases/tag/v2.0.9) · [LangGraph 响应 schema](https://github.com/langchain-ai/langgraph/commit/448a76377) · [GraphAgent](../../v2/orchestration-patterns.md) · [当前问答投影](../../../agui/client_data.go)

### 4. 用一个外部作业验证可恢复等待

配置 `WithBackgroundTasks` 后，慢工具可交给进程内管理器。跨进程等待先用一个支持幂等提交、可查询和取消的外部
作业验证，例如提交构建后等 webhook。等待点由图节点发起并随存档保存。宿主在提交前保存幂等键和任务归属，
提交后补作业句柄。
回调先保存结果。挂起落档和回调到达时都尝试按任务与等待点原子认领，待二者齐备才触发续跑。验收覆盖提交后写句柄
前退出、回调早于挂起、重复回调，以及完成、超时、取消的竞态。用两个任务验证各自只创建一份作业、只进入一个终态，
结果回到原任务。这个用例能为是否扩展到后台子 agent 提供依据。
[本仓后台工具](../../v2/background-tasks.md) · [Pi webhook 示例](https://github.com/earendil-works/pi/blob/473515f9c297c75cf2c3ebb3ba027049ed836d27/packages/durable/test/examples/40-tool-waits-for-webhook.ts)

### 5. 用现网观测决定指标和存档改造

先核对哪些看板、告警和采集规则消费 `gen_ai.invoke_workflow.duration` 及 `gen_ai.workflow.name`。若跟进 11 月约定，
工作流时长迁入 `gen_ai.invoke_agent.duration`，用 `gen_ai.operation.name=invoke_workflow` 筛选。验收同一次工作流
只记一次，并能在看板区分 workflow 与 agent。上游条目仍为 Development。
[指标对照](observability-trace-spec.md)

评估增量 checkpoint 前，先用业务长会话测持久后端的 P95 存档字节数、保存耗时和恢复耗时。大结果占主要体积时，
先测 `WithToolResultSpill` 带来的变化。若全量快照仍是瓶颈，再比较增量方案的写入节省、恢复读放大，以及回收之后能否
正确恢复旧任务。
[工具结果转存](../../../agent_v2/tool_spill.go) · [存档取舍](checkpoint-storage.md)

## 九、本轮落地

- **路由复测**支持业务 JSONL 和分支定义。报告增加实际升级率、自动承接率、自动承接错路由率与 P95。
  本地决策服务验证了请求内容、标签更新与缓存复用。用脱敏业务样本决定实际接管门槛仍待标注数据和模型凭据。
  [评测用法](../../v2/route-decider.md#用外部业务样本复测)
- **共享审批**在 SQL 持久化任务索引、前文和已采纳答复，由根存档提供挂起集合。
  测试分别覆盖 SSE 中断帧丢失后的接手和宿主进程退出后的接手。并发续跑、旧 ID、异租户、
  答复提交响应丢失和任务替换失败也有回归。
  [接入和重连](../../v2/agui.md#共享会话) · [SQL 回归](../../../sql/agui_session_test.go)
- **逐步验收**有可运行的 GraphAgent 示例。第一版方案缺少验收项会退回生成，第二版挂起审核。
  缺字段或类型错误保留挂起问题。批准后执行一次，拒绝结束。
  [SOP 示例](../../../example/orchestration/sop_demo/main.go)
- **外部作业等待**用独立的构建服务数据库模拟外部 API，宿主和回调通过持久任务键及任务租约协调。
  提交后未保存句柄可用原幂等键恢复。回调可早于存档，回调与挂起落档都会尝试交付，
  周期恢复扫描补偿在途竞争、宿主退出及丢失的回调。扫描先查询作业结果，再判断是否到期。
  完成、取消和超时竞争同一终态，交付状态先落库再清存档。
  [作业示例](../../../sql/example/durable_job/main.go) · [回归场景](../../../sql/example/durable_job/workflow_test.go)
- **指标核对**查过仓内采集和示例配置，工作流时长和普通 Agent 时长有各自的序列。用 GraphAgent 回归确认
  一次工作流只记一个样本。沿用现有导出名，等部署侧看板、告警和采集规则一起核对后决定迁移。
  [指标和迁移约定](observability-trace-spec.md)

### 存档测量

```bash
go run ./sql/example/checkpoint_profile -samples 30 -rounds 12
```

每种模式 30 个模拟任务，每个任务 12 轮、每轮 65,536 字节工具输出。每个任务在最后一批工具等待人答，
然后由新建的 BasicAgent 从存档续跑。`cp_bytes_p95` 取序列化存档的字节数，保存耗时覆盖 SQL 写入，
挂起和续跑耗时取各自完整调用。设置 `CREW_SQL_POSTGRES_DSN` 可切到 PostgreSQL 测试库。

| 后端 | 模式 | P95 存档字节 | 保存 P95 ms | 挂起 P95 ms | 续跑 P95 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 文件 SQLite | 全量 | 1,579,015 | 9.860 | 23.566 | 27.727 |
| 文件 SQLite | 工具结果转存 | 190,142 | 1.964 | 20.465 | 7.008 |
| 本地 PostgreSQL 16.9 | 全量 | 1,579,015 | 49.967 | 72.601 | 44.089 |
| 本地 PostgreSQL 16.9 | 工具结果转存 | 190,142 | 7.308 | 31.668 | 14.100 |

本组负载转存后存档字节约少 88%。挂起耗时已计入转存写入，表中存档字节只计 checkpoint。
下一步用业务会话测同一组指标，依实际剩余瓶颈决定是否引入增量存档。
[测量入口](../../../sql/example/checkpoint_profile/main.go) · [存档取舍](checkpoint-storage.md)
