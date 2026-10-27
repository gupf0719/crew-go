# 可观测性：OpenTelemetry 接入

`observability` 包用 OpenTelemetry 记录 crew-go 的执行：agent、工作流、图节点、模型调用、工具调用各一个
span，配套 GenAI 语义约定的指标和框架自己的 `crew.*` 指标。数据怎么导出、发到哪个后端，全由应用配的
OTel SDK 决定，本包只依赖 OTel API。

- 代码：`observability/`（属性名与指标名集中在 `observability/semconv.go`）
- 可运行示例：`example/observability/observability_demo`（内存里打印 span 树与指标，`-otlp` 发给 collector）
- 示例程序共用的 SDK 装配：`example/otelsetup`
- 与 GenAI 语义约定逐项对照、偏离与决策：[three-way-comparison/observability-trace-spec.md](../description/three-way-comparison/observability-trace-spec.md)

## 1. 接入

### 1.1 应用配 SDK

本包不建 TracerProvider / MeterProvider，也不管导出器、采样、Resource。不传 provider 时用全局的
（`otel.SetTracerProvider` / `otel.SetMeterProvider`）；应用什么都没配时 OTel API 全是 no-op，零开销。

```go
tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)), sdkmetric.WithResource(res))
otel.SetTracerProvider(tp)
otel.SetMeterProvider(mp)
defer tp.Shutdown(ctx)
defer mp.Shutdown(ctx)
```

`example/otelsetup.Setup` 是示例程序用的一种写法（OTLP gRPC / stdout），生产按自己的需要配。

### 1.2 装钩子：`Install`

```go
obs := observability.New()                       // 或 observability.New(observability.WithTracerProvider(tp), ...)
ctx, opts := obs.Install(ctx,
    agent_v2.WithName("assistant"),
    agent_v2.WithModel(m),
)
ag := agent_v2.NewBasicAgent(opts...)
events, err := ag.Run(ctx, sess, input)          // 必须用 Install 返回的 ctx
```

`Install` 一次接上两条钩子链：

| 链 | 装在哪 | 管什么 |
|---|---|---|
| 业务级 `obs.AgentHooks()`（`hook.Hooks`） | agent 构造选项 | agent / 模型调用 / 工具调用 span，推理、token、工具、agent 指标，中断与续跑 |
| 编排级 `obs.OrchHooks()`（`agent_v2.OrchestrationHooks`） | ctx（`agent_v2.WithCtxOrchHooks`） | 节点与工作流 span，框架事件（压缩、存档、扇出、规划、路由、fallback）转成 span 事件与指标 |

拿不到 opts 就拿不到 ctx，没有只装一半的用法。opts 里已配的 `WithHooks`、ctx 上已有的编排钩子都被包进
observability 里面（见下一段），不会被顶掉。

**两个安装点分开的拓扑**（agent 构造一次、ctx 每个请求另外准备，典型是 `a2a/server`）收不进一次调用，
两半各装各的：agent 构造时 `agent_v2.WithHooks(obs.AgentHooks())`，每个请求的 ctx 上
`agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())`。漏装编排那半不报错，只是节点 span 与框架指标静默缺失；
框架在第一次 `OnAgentEnd` 发现「业务级接了、编排级一次没跑过」时用 `slog` 告警一次（判据是编排钩子自己
跑过没有，不是 ctx 里有没有——编排钩子也可以走 RunOption `agent_v2.WithOrchHooks`）。

**业务自己也有钩子时交给 observability 包在里面**：`obs.AgentHooks(业务钩子…)`、`obs.OrchHooks(业务编排钩子…)`。
observability 在最外层，业务钩子按顺序串在里面；里面的钩子在开始一侧失败（报错，或像 HITL 闸门那样抛中断）时，
observability 当场按这个错误收掉它刚开的 span。别自己用 `hook.NewChainHooks` / `agent_v2.NewChainOrchHooks` 把
observability 跟别的钩子串：链在后面的钩子失败时交回开始之前的 ctx，框架随后要么不再调结束钩子（agent 级），要么
拿开始之前的 ctx 调（节点 / 图），observability 都取不到自己的 span——它永远不结束（不会被导出），
`crew.agent.active` 也永久多 1。`example/orchestration/plan_chat_web` 是这么装的：

```go
hooks := obs.AgentHooks(sseHooks, interruptHook)   // 业务级
orch := obs.OrchHooks(sseOrchHooks)                // 编排级
```

被里面的钩子拒绝的模型 / 工具调用：span 以失败收尾（`error.type` 为 `model_start_error` / `tool_start_error`，闸门
中断记 `blocked`），不记时长样本、不计入 agent 的调用次数——调用没有发生。

包在里面还带来两点：

- **记改写之后的值**：开始一侧 observability 先开 span（父子关系要它），输入消息、工具名与参数、token 模态等
  里面的钩子跑完再记；结束一侧里面的钩子先跑。业务在钩子里改写 [消费] 字段（PII 过滤输入、脱敏或改判工具结果）
  时，span 记的是改写之后、真正交下去 / 被框架用上的那份。里面的结束钩子报错、等人或被叫停时，模型 / 工具调用本身已经
  完成，照样按这次调用自己的结局收尾（模型调用的用量只在这一遍交来）。
- **外来 ctx**：里面的钩子返回外来的 ctx（缓存下来的、`Background`）属于违约（框架会告警），observability 仍把
  自己的句柄与当前 span 挂回去，span 照常收尾。

同一个 Observability 装了两层（`Install` 两次、`Install` 之外又 `WithHooks(obs.AgentHooks())`、ctx 上已有它的编排
钩子再 `Install`）时只记一份；两个不同的 Observability 叠在一起时各记各的（句柄按实例区分）。顶不掉的范围到
`Install` 拿到的 opts / ctx 为止：之后再接 `agent_v2.WithHooks(x)` 会整个覆盖业务级钩子，Run 时传 RunOption
`agent_v2.WithOrchHooks` 会盖过 ctx 上的编排钩子。

### 1.3 选项

| 选项 | 默认 | 说明 |
|---|---|---|
| `WithTracerProvider(tp)` | `otel.GetTracerProvider()` | |
| `WithMeterProvider(mp)` | `otel.GetMeterProvider()` | |
| `WithPropagator(p)` | W3C Trace Context + Baggage | 只用于 A2A 消息元数据的注入 / 提取；不跟全局走（全局默认是 no-op，A2A 两端会串不起来） |
| `WithContentRecording(bool)` | 关 | 采集输入输出内容，见 §4 |
| `WithRedactPatterns(...*regexp.Regexp)` | 无 | 写进 span 的内容、错误信息、事件里的字符串，命中的片段换成 `[REDACTED]` |
| `WithMaxContentChars(n)` | 16384 | 每段内容按字符截断，超出标 `…[truncated]`；`<= 0` 不限 |

脱敏一律在截断之前做（反过来的话，跨在截断点上的敏感片段被截成半段、正则匹配不上，前半段会漏出去）。
错误信息、事件里的字符串截到 1024 字符。

instrumentation scope 名是 `crew-go/observability`（`observability.ScopeName`）。`obs.Tracer()` /
`obs.Meter()` 返回同一个 scope 的 tracer / meter，业务补自己的 span 与指标（成本、业务成功率）从它们建。

## 2. Trace

### 2.1 span 一览

| span 名 | kind | 由谁建 | `gen_ai.operation.name` |
|---|---|---|---|
| `invoke_agent {agent 名}` | INTERNAL | `OnAgentStart`，`AgentInfo.Type` 不是下一行的编排（BasicAgent 的 `base`、远端 agent 的 `a2a_remote`、业务自研 agent） | `invoke_agent` |
| `invoke_workflow {agent 名}` | INTERNAL | `OnAgentStart`，`AgentInfo.Type` 是 `sequential` / `parallel` / `loop` / `routing` / `handoff` / `plan` / `reflect` / `graph` | `invoke_workflow` |
| `invoke_workflow {工作流名}` | INTERNAL | `OnGraphStart`，**外面没有同一 ns 的 agent span 时**：应用直接 `compose_v2.Run` 的图、节点里再跑的图、`AddSubgraphNode` 的子图 | `invoke_workflow` |
| `node {节点 ID}` | INTERNAL | `OnNodeStart` | 无 |
| `retrieval {节点 ID}` / `embeddings {节点 ID}` / `guardrail {节点 ID}` | INTERNAL | `OnNodeStart`，节点声明了 `compose_v2.WithNodeComponent` | `retrieval` / `embeddings` / 无 |
| `chat {模型}` | CLIENT | `OnModelStart` | `chat` |
| `execute_tool {工具名}` | INTERNAL | `OnToolStart`；加载 skill 的 `activate_skill` 调用是 `execute_tool activate_skill {skill 名}` | `execute_tool` |
| `invoke_agent {远端 agent 名}` | CLIENT | `a2a.RemoteAgent`（配了 `a2a.WithObservability`） | `invoke_agent` |
| `a2a.server {操作}` | SERVER | `a2a/server` executor（配了 `server.WithExecutorObservability`） | 无 |

- `chat` span 的模型名取请求模型（`ModelInput.Config.ModelName`）。BasicAgent 每次调模型前用 `model.RequestConfig`
  填它：按次的 `model.WithModel` 覆盖优先，没有时取模型客户端实现的 `model.ModelNamer`（openai 兼容客户端——含 qwen /
  deepseek——、anthropic 客户端、`fallback` 都实现了；fallback 报起手请求的那个候选）。客户端不报模型名时（业务自研
  的 `ChatModel` 没实现 `ModelNamer`），结束时改名成 `chat {响应模型}`，响应里也没有模型名时就叫 `chat`。
- `OnGraphStart` 建的工作流 span 名取 ns 最后一段的节点 ID，**不带**扇出实例号（`#N`）与重访序号（`@N`）——
  实例号每次投递都新铸，带上的话 span 名与 `gen_ai.workflow.name` 指标维度的取值没有上限。完整 ns 记在
  `crew.graph.ns`；顶层图叫 `root`。
- 远端 agent 同时配 `a2a.WithHooks(obs.AgentHooks())` 与 `a2a.WithObservability(obs)` 时，一次远端调用是
  `invoke_agent`（INTERNAL）下挂 `invoke_agent`（CLIENT）两层；只要 CLIENT 那层时配后者就够。
- 结束钩子只收本次调用自己的句柄（agent 按名字与 ns、节点按节点 ID 与 ns 核对）：框架在开始失败等路径上拿
  开始之前的 ctx 调结束钩子时，那里最内层的句柄可能是外层的，不会被误收。

### 2.2 父子关系

父子关系完全走 ctx：开始一侧的钩子把 span 放进返回的 ctx，框架把这个 ctx 交给这次调用里的一切（节点、
模型调用、工具、嵌套 agent），结束一侧从 ctx 取回。应用在外面开的 span（HTTP 请求等）自然成为 agent
span 的父。BasicAgent 一次带工具调用的 Run：

```
invoke_agent assistant
├── node preamble
├── node model
│   └── chat qwen-plus
├── node tools
│   └── execute_tool get_weather
├── node model
│   └── chat qwen-plus
└── node postamble
```

嵌套时：编排的成员、GraphAgent 节点里经 `agent_v2.RunSubAgent` 调起的 agent，挂在调用它的节点 span 下；
`AgentAsTool` 包成的工具调起的 agent 挂在那次 `execute_tool` span 下（工具体跑在 `OnToolStart` 返回的 ctx 上）。

### 2.3 属性

所有 span 都带 `crew.outcome`（见 §2.4）。其余按 span 类型：

| span | 属性 |
|---|---|
| agent / 编排（`OnAgentStart`） | `gen_ai.agent.name` 或 `gen_ai.workflow.name`、`crew.agent.kind`、`crew.run.id`（`RunSession.RunID`）、`gen_ai.conversation.id`（推了 `compose_v2.WithThreadIDCtx` 才有）、`crew.graph.ns`（嵌套时）；结束时 `crew.agent.inference_calls` / `crew.agent.tool_calls` |
| 工作流（`OnGraphStart`） | `gen_ai.workflow.name`、`crew.run.id`、`gen_ai.conversation.id`、`crew.graph.ns`（非顶层时） |
| node | `crew.node.id`、`crew.graph.ns`、`crew.node.instance`（扇出实例）、`crew.node.component`；有流式片段时 `crew.stream.chunks`、`crew.stream.first_chunk_seconds` |
| chat | `gen_ai.agent.name`、`crew.agent.round`、`gen_ai.request.model` / `.temperature` / `.max_tokens` / `.top_p`（有才记）、`gen_ai.request.stream`（流式时为 true，非流式不记）、`gen_ai.conversation.id`；结束时 `gen_ai.provider.name`、`gen_ai.response.model`、`gen_ai.response.finish_reasons`、`gen_ai.usage.input_tokens` / `.output_tokens`（有用量就记，含 0；输出含推理 token）、`gen_ai.usage.cache_read.input_tokens` / `.cache_write.input_tokens` / `.reasoning.output_tokens`（大于 0 才记）、`gen_ai.response.time_to_first_chunk`（流式） |
| execute_tool | `gen_ai.tool.name`、`gen_ai.tool.type`（`function`）、`gen_ai.tool.call.id`、`gen_ai.agent.name`、`gen_ai.conversation.id`、`crew.tool.category`（`retrieval` / `embedding` / `guardrail` / `skill`）、`gen_ai.skill.name`（`activate_skill` 调用）、`crew.tool.outcome`（`denied`：被审批拒绝）、`crew.tool.replayed`（续跑时从工具账重放） |
| A2A client | `gen_ai.provider.name`=`a2a`、`gen_ai.agent.name`、`crew.a2a.method`、`crew.a2a.transport`、`gen_ai.conversation.id`（A2A contextId）、`crew.a2a.task_id` |
| A2A server | `crew.a2a.operation`、`gen_ai.conversation.id`、`crew.a2a.task_id` |

`gen_ai.provider.name` 来自模型客户端填的 `Message.Metadata.System`（`openai.WithSystem`；deepseek /
qwen 客户端已默认填）。

工具类别：业务工具实现 `tool.Categorizer` 返回 `tool.CategoryRetrieval` 等，框架查到后放进
`ToolInput.Extra["category"]`，观测层据此记 `crew.tool.category`，span 名不变。GraphAgent 里直接调组件的
节点用 `compose_v2.WithNodeComponent` 声明，节点 span 按 GenAI 操作名命名。

### 2.4 结局：`crew.outcome` 与 span status

span status 只有 Unset / Error 两档有用，而「用户点了停止」「等人审批」都不是错误。`crew.outcome` 区分五种：

| 值 | 何时 | span status |
|---|---|---|
| `success` | 正常结束 | Unset |
| `error` | 失败 | Error，带 `error.type` 与 `exception` 事件 |
| `timeout` | 超时 | Error，`error.type`=`timeout` |
| `cancelled` | 取消、`compose_v2.ErrSoftCanceled` | Unset |
| `blocked` | 中断，等人答复（`compose_v2.InterruptError`）；工具调用被审批拒绝（另带 `crew.tool.outcome=denied`） | Unset |

判定（所有 span 同一个口径）：调用方 ctx 已结束时按 ctx 判——到期是 timeout，取消是 cancelled；ctx 还活着时看
错误：陈旧写回（`compose_v2.ErrCheckpointStale`，`error.type`=`checkpoint_stale`）先于中断判——它可能跟中断绑在
同一条错误链上，按中断记会导出一个跟正常挂起一模一样的 span；带 `compose_v2.ErrCheckpointNotPersisted` 的中断
算失败（存档没写成，续不上）；`compose_v2.ErrSoftCanceled` 是 cancelled；错误链上的 `context.DeadlineExceeded`
是 timeout。错误链上的 `context.Canceled` **不算**取消：调用方 ctx 还活着时它来自别处（errgroup 里被连带取消
的兄弟、业务自己派生又取消的子 ctx），这次执行是失败了，记成 cancelled 会把失败藏起来。

A2A client span 按发请求用的 ctx 判（`A2AClientSpan.End(ctx, err)`），其余失败记 `a2a_remote_error`；server span
由 executor 按任务结局判（见下表），被客户端 Cancel 的是 cancelled。

`error.type` 取低基数的值：

| 来源 | 取值 |
|---|---|
| 框架认得的错误 | `run_aborted`、`checkpoint_not_persisted`、`graph_id_conflict`、`no_pending_checkpoint`、`max_iterations`（迭代深度触顶）、`max_width`（扇出宽度失控）、`checkpoint_stale`、`timeout` |
| 工具失败 | `tool.Result` 的错误码（`ToolCallResult.ErrorCode`），没有时 `tool_error` |
| `OnError` 交来的 | `CommError.Code` |
| A2A client | `a2a_remote_error` |
| A2A server | `a2a_checkpoint_not_resumable`、`a2a_checkpoint_stale`、`a2a_graph_id_conflict`、`a2a_checkpoint_not_persisted`、`a2a_agent_error`、`timeout` |
| 其余 | `_OTHER` |

错误信息写进 status 描述与 `exception` 事件前先脱敏、再截到 1024 字符；远端 A2A 的错误详情不写进 span（可能含
对端的敏感信息）。工具失败但没给错误说明时，只有打开内容采集才拿工具输出当错误信息。

**错误路径上的收尾**：模型 / 工具调用失败时框架不调 `OnModelEnd` / `OnToolEnd`，`chat` / `execute_tool` span 由
`OnError` 以失败结束，同时记带 `error.type` 的时长样本。`OnError` 按错误码分层：`model_*` 只收模型 span、
`tool_*` 只收工具 span；agent / 编排层的错误（`agent_start_error`、`branch_error`……）不碰叶子——框架报这些时
交来的 ctx 可能是开始之前的，最内层是外层的工具调用（比如工具里跑的子 agent 开始被拒）。一批工具的开始钩子失败
（`tool_start_error`）或整批执行失败（`tool_call_error`）时 `OnError` 收到的是节点的 ctx、分不出是哪一个，这批
还开着的工具 span 一起以失败结束——开始失败时框架当场让这批失败，前面的调用不会再执行。仍没收尾的（例如抛出中断的
那次工具调用）由所在 agent 的 `OnAgentEnd` 收尾：结局跟 agent 相同（挂起时 `blocked`，不算失败），agent 本身
成功的话记 `error` / `_OTHER`；这些叶子不记时长样本。`OnError` 报到 agent / 节点 / 工作流上的错误不在那里记——
错误要是被处理掉了（重试成功、分支失败被兜住），按 OTel 的 recording-errors 指引不该记；没被处理的话，那个 span
结束时自己以失败收尾。

**一次调用只按它自己的结局记一次**：

- BasicAgent 的一轮在 `OnModelEnd` 里因为等人或被叫停停下时，续跑沿用那次的答复、不再调模型，模型钩子再走一遍
  （`ModelInput.Extra` / `ModelOutput.Extra` 带 `hook.ModelResponseReusedKey`）。那次调用在停下的那一遍已经收尾，
  沿用的那一遍不再开 `chat` span、不记耗时与用量。
- 一批工具调用没整体成功时（同批有调用中断或硬失败），已经有结局的调用照样各自触发 `OnToolEnd`
  （`Extra` 带 `agent_v2.ToolBatchFailedExtraKey`），按自己的结局收尾。续跑时它们从工具账重放、`OnToolEnd` 再触发一次
  （`Extra` 带 `agent_v2.ToolReplayedExtraKey`）：span 标 `crew.tool.replayed`，不记时长样本、不计入 agent 的调用次数。
- 被审批拒绝的调用没有执行：`OnToolEnd` 的 `Extra` 带 `agent_v2.ToolOutcomeDenied`，span 记 `blocked`、不标 Error、
  带 `crew.tool.outcome=denied`，时长样本带同一个属性——跟执行成功的分得开。
- 模型调用整次重来（`agent_v2.WithModelRetry`）仍是一次调用：一个 `chat` span，时长含全部尝试与退避，用量只记交付那次；
  首片时长（`gen_ai.response.time_to_first_chunk` 与对应指标）记交付那份答复的首片，作废尝试的首片不算。作废尝试白付的
  token 不进用量（框架拿不到），成本按用量算会偏低。节点上的 `crew.stream.chunks` 是交给前端的片段数，含之后被撤回的。

### 2.5 span 事件

| 事件 | 在哪个 span | 属性 |
|---|---|---|
| `crew.interrupt` | agent | `crew.interrupt.queries`（这次问了几个）、`crew.interrupt.reason`、`crew.interrupt.id`（第一个提问的 ID）、`crew.interrupt.question`（开内容采集时） |
| `crew.resume` | 续跑的 agent | `crew.resume.action`、`crew.interrupt.wait_seconds`（从挂起到续跑）、`crew.resume.inputs`（调用方给了几条输入） |
| `crew.{事件类型}` | 当前节点（或最内层 span） | 框架经 `EventSink` 发的事件：`crew.compaction`（按层与阶段展开，`crew.compact.*`）、`crew.cp`、`crew.fanout`、`crew.route_decision`、`crew.plan_replan`、`crew.failover`、`crew.member_tripped`、`crew.store`、`crew.history.compact` 等；元数据里的标量（字符串 / 数字 / 布尔）记成事件属性，字符串经脱敏、截断；打开内容采集时事件正文记在 `content` 属性 |
| `crew.stream.partial_output` | node | 节点没成功结束、已经吐出流式片段时，攒下的片段记在 `content` 属性（开内容采集时） |
| `exception` | 以 error / timeout 结束的 span | `exception.type`（就是 `error.type` 的值，不是 Go 类型名）、`exception.message` |

流式片段（`chunk` 事件）不逐条记，只在节点 span 上计数、记首片时间。

### 2.6 跨进程：A2A

client 侧 `a2a.RemoteAgent` 建 `invoke_agent` CLIENT span，把 trace context（W3C `traceparent` /
`tracestate` / `baggage`）写进出站 `Message.metadata`；server 侧 executor 从入站 metadata 取出来作父，建
`a2a.server` SERVER span，远端 agent 的整棵 span 树挂在它下面。走 metadata 不走 HTTP 头，是因为 A2A 的
传输可以不是 HTTP，而且 a2asrv 的执行 ctx 跟 HTTP 请求是断开的。入站没有 trace context 时 server 起一条新
trace。

自定义传输时可以直接用 `obs.InjectToMetadata(ctx, md)` / `obs.ExtractFromMetadata(ctx, md)`。

### 2.7 业务要的 trace ID

`observability.TraceIDFromContext(ctx)` 返回当前 span 的 trace ID（32 位十六进制），没有时空串。
`example/orchestration/plan_chat_web` 在每个 HTTP 请求开一个请求 span，把它的 trace ID 经 SSE 推给前端；
同一个任务的多次请求（Run 与续跑）各是一个 trace，靠 `crew.run.id` 关联（示例的 session_id 就是 RunID）。
跨任务的会话要推 `compose_v2.WithThreadIDCtx`，记成 `gen_ai.conversation.id`。

## 3. 指标

指标在 `New` 时建好。时长直方图用 GenAI 约定建议的桶（0.01s 到 81.92s 翻倍）；`crew.interrupt.wait.duration`
等的是人，桶从 1 秒到 1 天（`1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400, 43200, 86400`）；
计数直方图桶 `0,1,2,3,5,8,13,21,34,55`；`crew.compaction.ratio` 桶 `0.1,0.2,0.3,0.5,0.7,0.9,1`。

### 3.1 GenAI 语义约定的指标

| 指标 | 类型 / 单位 | 属性 |
|---|---|---|
| `gen_ai.client.inference.duration` | 直方图 / s | `gen_ai.operation.name`、`gen_ai.provider.name`、`gen_ai.request.model`、`gen_ai.response.model`（有才记）；失败时（`OnError` 收尾）带 `error.type` |
| `gen_ai.client.inference.time_to_first_chunk` | 直方图 / s | 同上，流式调用成功时才记 |
| `gen_ai.client.inference.usage.input_tokens` | 计数 / {token} | 同上 + `gen_ai.token.modality` |
| `gen_ai.client.inference.usage.output_tokens` | 计数 / {token} | 同上 |
| `gen_ai.client.inference.usage.cache_read.input_tokens` | 计数 / {token} | 同上 |
| `gen_ai.client.inference.usage.cache_write.input_tokens` | 计数 / {token} | 同上 |
| `gen_ai.client.inference.usage.reasoning.output_tokens` | 计数 / {token} | 同上 |
| `gen_ai.invoke_agent.duration` | 直方图 / s | `gen_ai.agent.name`、`gen_ai.request.model`（知道时）；失败时 `error.type` |
| `gen_ai.invoke_agent.inference_calls` | 直方图 / {inference_call} | `gen_ai.agent.name` |
| `gen_ai.invoke_agent.tool_calls` | 直方图 / {tool_call} | `gen_ai.agent.name` |
| `gen_ai.invoke_workflow.duration` | 直方图 / s | `gen_ai.workflow.name`；失败时 `error.type` |
| `gen_ai.execute_tool.duration` | 直方图 / s | `gen_ai.tool.name`、`gen_ai.tool.type`、`gen_ai.agent.name`；失败时 `error.type`，被审批拒绝时 `crew.tool.outcome=denied`；从工具账重放的不记 |

- token 计数只在模型调用成功（`OnModelEnd`）时记，数量为 0 的不记。
- 口径照约定：输入 token 含缓存读写（cache_read / cache_write 是它的子集）；输出 token 含推理 token（约定要求推理计入
  输出、输出取计费口径）。`MessageUsage.OutputTokens` 本来就是这个口径（`ThinkTokens` 是它的子集，openai 兼容客户端取 `completion_tokens_details.reasoning_tokens`、Claude 取 `output_tokens_details.thinking_tokens`），原样记。缓存写入目前只有
  `model/anthropic` 报。
- `gen_ai.token.modality`：供应商只给总数。输入里全是文本时记 `text`；有图片 / 音频 / 文件块时分不出各模态
  各占多少，按约定记 `unknown`。输出同理。cache_read / cache_write 跟输入那一侧、reasoning 跟输出那一侧。
- `gen_ai.request.model` 只填调用方请求的模型（来源同上）；客户端不报模型名时不填，不拿响应里的模型名去顶（两者
  可以不同，比如别名解析到具体版本）。

### 3.2 框架指标 `crew.*`

| 指标 | 类型 / 单位 | 属性 | 什么时候记 |
|---|---|---|---|
| `crew.agent.invocations` | 计数 | `gen_ai.agent.name`、`crew.agent.kind`、`crew.outcome` | 每次 agent / 编排调用结束 |
| `crew.agent.active` | UpDownCounter | `gen_ai.agent.name` | 在途调用数 |
| `crew.interrupts` | 计数 | `gen_ai.agent.name`、`crew.agent.kind` | 以中断结束 |
| `crew.interrupt.wait.duration` | 直方图 / s | `gen_ai.agent.name`、`crew.agent.kind` | 续跑时，从挂起到续跑 |
| `crew.resumes` | 计数 | `gen_ai.agent.name`、`crew.agent.kind`、`crew.resume.action` | 每次 Resume |
| `crew.skill.activations` | 计数 | `gen_ai.agent.name`、`gen_ai.skill.name` | `activate_skill` 调用成功结束 |
| `crew.checkpoint.stale_writes` | 计数 | `crew.checkpoint.stale_op`（拿不到操作名时 `unknown`） | agent 以陈旧写回结束（开了陈旧写回检测，且结束时 ctx 没被取消） |
| `crew.node.executions` | 计数 | `crew.node.id` | 每次节点执行（编排钩子） |
| `crew.compactions` | 计数 | `crew.compact.layer`、`crew.outcome` | 压缩完成（`compaction` 事件的 after 阶段） |
| `crew.compaction.duration` | 直方图 / s | `crew.compact.layer` | 同上 |
| `crew.compaction.ratio` | 直方图 / 1 | `crew.compact.layer` | 同上，压缩后 / 压缩前 token |
| `crew.checkpoint.operations` | 计数 | `crew.checkpoint.op`、`crew.outcome` | 存档 save / load / cleanup，要打开存档事件 |
| `crew.checkpoint.operation.duration` | 直方图 / s | `crew.checkpoint.op` | 同上 |
| `crew.fanout.branches` | 直方图 | `crew.fanout.kind` | 一次扇出的分支数 |
| `crew.plan.replans` | 计数 | `crew.plan.phase` | PlanAgent 每次规划 |
| `crew.route.decisions` | 计数 | `gen_ai.agent.name`、`crew.route.target`（没有目标时 `__none__`） | RoutingAgent 每次路由 |
| `crew.fallback.failovers` | 计数 | `crew.failover.reason`、`crew.failover.from`（候选下标） | `fallback` 候选切换 |
| `crew.fallback.member_trips` | 计数 | `crew.fallback.member`（候选下标）、`crew.fallback.reason` | `fallback` 候选断路器熔断 |

- 从 `crew.node.executions` 往下的指标来自编排钩子与框架事件，只在装了编排钩子时记；上面几个由业务级钩子记。
- 存档事件默认不发：agent_v2 用 `compose_v2.WithEmitCpEventCtx(ctx, true)`（BasicAgent 也可以用
  `agent_v2.WithEmitCpEvent`），直接 `compose_v2.Run` 用 RunOption `compose_v2.WithEmitCpEvent(true)`。
- 陈旧写回检测默认关：agent_v2 用 `compose_v2.WithStaleWriteGuardCtx(ctx)`，直接 `compose_v2.Run` 用
  `compose_v2.WithStaleWriteGuard()`（agent_v2 的编排不透传业务的 RunOption）。

框架拿不到、要业务自己记的：调用成本、业务成功率等。用 `obs.Meter()` 建，维度照上面的属性名取，跟框架指标
对得上。

### 3.3 基数与降本

框架指标的属性取值来自 agent 名、节点 ID、工具名、候选下标与枚举值，基数跟着应用的结构走。要留意的一处：
没给名字的 agent 每次构造都会自动生成一个随机名（`agent_v2_xxxxxxxx`），每个请求新建 agent 又不起名时，
`gen_ai.agent.name` 的取值会一直涨——给 agent 起名（`agent_v2.WithName`）。

业务想关掉某些指标、去掉某个维度或限制基数，用 OTel SDK 的机制，不在本包里另做一套：

```go
mp := sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(reader),
    // 不要节点执行次数
    sdkmetric.WithView(sdkmetric.NewView(
        sdkmetric.Instrument{Name: "crew.node.executions"},
        sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}},
    )),
    // 每个指标最多 500 个时间序列（SDK 默认 2000）
    sdkmetric.WithCardinalityLimit(500),
)
```

超出上限的数据点并进同一个指标上带 `otel.metric.overflow=true` 属性的那一条序列；上限把这条也算在内。

## 4. 内容采集

事件元数据里装内容的键（钩子改写答复时 `reset_stream` 的 `replacement`，即改写之后的完整答复）跟其余内容一样，只在
打开内容采集时记；新加装内容的元数据键登记在 `observability/orch_hooks.go` 的 `contentMetadataKeys`。

默认不采集输入输出内容——可能含个人信息，按 GenAI 约定是 opt-in。`WithContentRecording(true)` 打开后：

| 属性 | 在哪 |
|---|---|
| `gen_ai.input.messages` | agent span（首跑取最后一条用户消息，续跑取调用方这次给的输入）、chat span（这轮交给模型的全部消息） |
| `gen_ai.output.messages` | agent span（最终答复，挂起时不记）、chat span（模型这轮的输出） |
| `gen_ai.system_instructions` | chat span（agent 的前言：系统提示词、模板渲染结果） |
| `gen_ai.tool.call.arguments` / `gen_ai.tool.call.result` | execute_tool span |

另外打开后：中断事件带提问原文、框架事件带正文、节点没成功结束时留下已经吐出的流式片段（§2.5）。

消息按 GenAI 约定的 JSON 结构编码（`role` + `parts`）。part 类型：`text`、`reasoning`、`tool_call`、
`tool_call_response`；多模态块里图片 / 音频 / 文件有 URL 的记 `uri`，内联数据（`data:` URI、base64）记 `blob`，
都带 `modality` 与 `mime_type`；结构化数据块记 `type=data`。每段文本先脱敏（`WithRedactPatterns`）再截断
（`WithMaxContentChars`），内联数据同样截断。工具参数与结果是合法 JSON 时先解码再编码一遍，把 `\uXXXX` 转义
还原成字符再脱敏——模型常把中文写成转义形式，正则在转义文本上匹配不到，读的人解码后看到的却是明文。框架合成的「未完成工具调用的记账」消息
（`agent_v2.IsUnresolvedCallsRecord`）不会被当成最终答复。

## 5. 中断与续跑的状态

续跑时要算「挂起了多久」，`Observability` 在进程内记每个挂起的 agent 是什么时候停下的，键是
（会话 ID、ns、agent 名），续跑时取走即删。挂起的任务被放弃时：

- 调 `obs.CleanupSession(sid)` 丢掉这个会话的记录（`a2a/server` 的 executor 在任务结束、取消时已经调）；
- 放弃续跑（`ResumeActionAbort`）时框架只在最外层触发开始 / 结束钩子，observability 在那里把这棵 ns 子树里各层
  的记录一起丢掉；
- 不调也不会无界增长：每记 256 次挂起顺手清一遍超过一天的记录，只是被清掉的那次等待时长不再记；
- `obs.PendingInterrupts()` 返回当前记录数，可以拿来监控泄漏。

续跑在另一个进程时（没有这条记录）照样有 `crew.resume` 事件与 `crew.resumes` 计数，只是没有等待时长。

## 6. 设计取舍

- **只依赖 API，不配 SDK**：导出器、采样、Resource 是部署决策，各应用不同；库配了就跟应用的配置打架。
  应用不配时 API 是 no-op，不接观测的业务零开销。
- **属性键自己定义**：GenAI 约定已迁到独立仓库、处于 Development 状态，Go 的 `semconv` 包从 v1.40 起不再
  生成 `gen_ai.*`。键名集中在 `semconv.go`，约定改名时只改一处。
- **`crew.outcome` 与 status 并存**：status 只能表达「错没错」，而取消、挂起是正常结局；只看 status 会把
  「用户点了停止」「等人审批」计成错误，或者把它们跟成功混在一起。
- **框架事件转 span 事件，不转子 span**：压缩、存档、路由是节点内的一个瞬间或一小段，用事件挂在节点 span
  上，不打散 span 树；需要聚合的同时转成指标。
- **跨进程走 A2A 消息元数据**：见 §2.6。
- **内容默认关、开了也截断脱敏**：大内容不进指标；span 属性上的内容有长度上限，后端的单属性上限按各自
  配置。
