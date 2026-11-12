# Agent 开发与编排框架进展（2026-09-12 至 2026-11-12）

本轮延续 9 月的行业调查，时间窗口按北京时间计算。发布状态取自官方 release 和文档，执行语义以本地
checkout 的提交、实现和测试为准。文末记录这次同步的起止 SHA。Deep Agents、Cursor Agent SDK 和
OpenAI 托管 Agents API 按官方公开资料核对。[9 月行业调查](industry-survey-2026q3.md)

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

crew-go 的取消终态与 Resume wire `runId` 两处 Q3 问题已有对应回归。AG-UI HTTP handler 用进程内会话映射
维护挂起项。若需要服务重启后从另一实例续跑，还要恢复挂起集合、已采纳的答案和任务归属。
[Q3 回查](industry-survey-2026q3.md) · [本仓协议测试](../../../agui/agui_test.go) · [会话映射](../../../agui/handler.go)

复用上游经验时，可以把问题写成可复现的用例。用同一节点的两次提问验证旧答案重发，用同批工具的成功和失败验证已完成结果的重放，用审批挂起时收到后台结果的场景验证通知交付。独立后台子 agent 的设计再根据业务用例确定任务句柄、审批归属、完成通知和退出清理。

## 六、本地源码快照

2026-11-12 同步了 14 个已有参考仓。13 个 checkout 对齐跟踪分支，其中 12 个快进，Grok Build 已对齐。AgentScope 本地与上游分叉，以下以更新后的远端跟踪引用核对。eino 另同步 `origin/alpha/10`，供预发布对照。表里的新增数为同步前后 `git rev-list --count OLD..NEW`，包含 merge 引入的提交，统计区间由各仓起始 SHA 决定。

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
| AgentScope `origin/main` | [`3a0e73fa` → `3fef100e`](https://github.com/agentscope-ai/agentscope/compare/3a0e73fa...3fef100e) | 137 | 更新远端引用。本地 `main` 有 29 个自身提交，调研取上游快照 |
| Codex `main` | [`2cab11ca4` → `0702ebed4`](https://github.com/openai/codex/compare/2cab11ca4...0702ebed4) | 736 | 任务所有权、恢复历史、计划附件交接。邮件与 wait 改动见 §二 |
| DeepSeek Harness `master` | [`687f282e86` → `e8f7b93a34`](https://github.com/deepseek-ai/deepseek-harness/compare/687f282e86...e8f7b93a34) | 4,376 | Team 模式、审批 projection、`0.2.2-rc.2` |
| Grok Build `main` | [`2bdd1d6a`](https://github.com/xai-org/grok-build/commit/2bdd1d6a6369de0e8c68132ea4539e9abd9e14a8) | 0 | 公开主分支最后一次同步在北京时间 9 月 30 日。窗口内的同步涉及子 agent attempt 恢复和插话测试 |
| Claude Agent SDK Python `main` | [`c857c31` → `3cf3446`](https://github.com/anthropics/claude-agent-sdk-python/compare/c857c31...3cf3446) | 307 | [v0.2.180](https://github.com/anthropics/claude-agent-sdk-python/releases/tag/v0.2.180)。SDK 等 CLI 空闲后才关 stdin，[保留后续 turn](https://github.com/anthropics/claude-agent-sdk-python/commit/dbc975e)。历史读取[保留并行工具的各条调用和结果](https://github.com/anthropics/claude-agent-sdk-python/commit/beffc50) |
| AG-UI `main` | [`7d14c417` → `f6f6c566`](https://github.com/ag-ui-protocol/ag-ui/compare/7d14c417...f6f6c566) | 931 | 适配器的中断、续跑与事件重放 |
| OTel GenAI semconv `main` | [`06ec68e` → `e6658f14`](https://github.com/open-telemetry/semantic-conventions-genai/compare/06ec68e...e6658f14) | 67 | workflow span / 指标合并、agent transfer、realtime 语义 |
