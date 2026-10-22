# 主题：observability 的 trace / 指标 vs OpenTelemetry GenAI 语义约定

> 对照对象：OpenTelemetry GenAI 语义约定（`open-telemetry/semantic-conventions-genai`，全部处于 Development
> 状态）。本地克隆 `/Users/gupengfei/program/go/src/otel-semconv-genai`（核对时 HEAD `06ec68e`，2026-10-07）：
> `docs/gen-ai/client-inference.md`（模型调用 span 与指标）、`gen-ai-agent-spans.md`（agent / workflow /
> skill）、`gen-ai-spans.md`（execute_tool / retrieval / embeddings）、`gen-ai-metrics.md`、
> `gen-ai-token-metrics.md`、`model/gen-ai/gen-ai-input-messages.json`（消息 JSON 结构）。
>
> crew-go 侧事实取自 `observability/`（属性与指标名在 `semconv.go`）。用法与完整属性表见
> [doc/v2/observability.md](../../v2/observability.md)，本篇只记「跟约定对得上的、偏离的、没做的」与决策。

## 触发场景（主动读本篇）

- 改 span 名、属性键、指标名之前
- 约定发了新版本，要判断跟进哪些
- 有人问「为什么这里不照约定写」

## 一句话结论

- span 形状（名字、kind、`gen_ai.operation.name`）按约定：`chat` / `invoke_agent`（INTERNAL 与 CLIENT）/
  `invoke_workflow` / `execute_tool`；加载 skill 的工具调用按 skill refinement 命名。
- 指标按约定：推理时长、首片时长、token 用量（按 `gen_ai.token.modality` 分）、agent / workflow / 工具时长
  与 agent 内调用次数。
- 偏离两处（§3）：模型客户端不报模型名时 `chat` span 用响应模型名命名；节点组件 span（retrieval /
  embeddings）kind 是 INTERNAL。
- 没做的（§4）主要是框架拿不到的数据（请求参数里的 seed / top_k、服务端地址、按模态拆分的 token）和需要
  Logs API 的 GenAI 事件。
- 框架自己的维度一律 `crew.*`（§5），不往 `gen_ai.*` 里塞约定没有的键。

## 一、span

| 约定的 span | 约定的名字 / kind | crew-go | 说明 |
|---|---|---|---|
| `gen_ai.client.inference` | `{operation} {request.model}` / CLIENT | ✅ `chat {模型}` / CLIENT | 名字见 §3.1 |
| `gen_ai.invoke_agent.internal` | `invoke_agent {agent.name}` / INTERNAL | ✅ BasicAgent | |
| `gen_ai.invoke_agent.client` | `invoke_agent {agent.name}` / CLIENT | ✅ `a2a.RemoteAgent` | `gen_ai.provider.name=a2a` |
| invoke_workflow | `invoke_workflow {workflow.name}` / INTERNAL | ✅ 8 种编排、`GraphAgent`、直接 `compose_v2.Run` 跑的图 | 判据见 §2 |
| execute_tool | `execute_tool {tool.name}` / INTERNAL | ✅ | |
| load skill（execute_tool 的 refinement） | `execute_tool {tool.name} {skill.name}` | ✅ `activate_skill` | 带 `gen_ai.skill.name` |
| retrieval | `{operation} {data_source.id}` / CLIENT | ⚠️ 节点声明 `retrieval` 时 `retrieval {节点 ID}` / INTERNAL | §3.2 |
| embeddings | `{operation} {request.model}` / CLIENT | ⚠️ 同上 `embeddings {节点 ID}` | §3.2 |
| create_agent / plan / memory / read skill resource / command execution / fetch response | — | ❌ | 框架里没有对应的独立操作，或由业务工具自己完成 |

A2A 的 server 侧 span（`a2a.server {操作}`，SERVER）约定里没有对应，是我们自己的；它是远端那棵 span 树的根，
跨进程的父来自 client span（W3C trace context 经 `Message.metadata` 传）。

## 二、invoke_workflow 报还是不报

约定的三条：编排图、协调多个 agent 的过程 SHOULD 报；单独的 agent 调用 SHOULD NOT 报；agent 内部为了实现自己
而起的 runner / 图 SHOULD NOT 报，而应用定义的图即使嵌套（子图作为另一张图的节点）也 SHOULD 报。框架可以自己
定判据。

crew-go 的判据：

- `OnAgentStart` 按 `AgentInfo.Type`：`base`（BasicAgent）→ `invoke_agent`，其余编排 → `invoke_workflow`。
- `OnGraphStart` 只在最内层句柄**不是**同一 ns 的 agent 句柄时建 `invoke_workflow`——agent / 编排内部跑的图
  是它们的实现细节，已经被 agent span 覆盖；应用直接 `compose_v2.Run` 的图、节点里再跑的图、子图节点都会建。

## 三、偏离

### 3.1 `chat` span 的名字

约定：`{gen_ai.operation.name} {gen_ai.request.model}`。请求模型来自 `ModelInput.Config.ModelName`：BasicAgent 用
`model.RequestConfig` 填——按次的 `model.WithModel` 覆盖优先，没有时取模型客户端实现的可选接口 `model.ModelNamer`
（内置的 openai 兼容客户端、anthropic 客户端、`fallback` 都实现了）。这时 span 名就是约定的 `chat {请求模型}`。

偏离只剩一种情况：业务自研的 `ChatModel` 没实现 `ModelNamer`、调用方也没按次指定模型时，请求模型不知道，span 名
在结束时改成 `chat {gen_ai.response.model}`（响应里也没有模型名时就是 `chat`），`gen_ai.request.model` 不记。约定
允许框架定自己的命名格式；不把响应模型名填进 `gen_ai.request.model`——两者可以不同（别名解析到具体版本），填进去
就是错的数据。

`ChatModel` 接口本身不加 `ModelName`：它是三个方法的最小接口，业务的包装 / 测试替身都要实现；模型名对发请求不是
必需的，做成可选接口、包装类转发即可。

### 3.2 retrieval / embeddings 节点的 span

约定里这两种是 CLIENT span，代表一次对检索服务 / 嵌入模型的调用。我们标的是 GraphAgent 里声明了组件类型的
**节点**——节点里是业务代码，可能调一次服务、可能调多次、也可能是本地计算，所以 kind 留 INTERNAL、名字用节点
ID。业务的检索 / 嵌入工具走工具路径：`execute_tool {工具名}` + `crew.tool.category`。

## 四、没做的

| 约定里的 | 级别 | 为什么没做 |
|---|---|---|
| `gen_ai.provider.name`（chat） | Required | 模型客户端填了 `Message.Metadata.System` 才有（openai 兼容客户端用 `WithSystem`，deepseek / qwen 默认已填）；业务自研客户端不填时缺 |
| `gen_ai.request.model` | Cond. Required | 客户端实现 `model.ModelNamer` 或按次指定模型时已记；都没有时缺（§3.1） |
| `gen_ai.request.seed` / `top_k` / `frequency_penalty` / `presence_penalty` / `stop_sequences` / `choice.count` | Cond. / Recommended | `ModelInput.Config` 只带模型名、温度、max_tokens、top_p（按次设了的） |
| `gen_ai.output.type` | Cond. Required | 结构化输出（`WithOutputSchema`）时可以记 `json`，未接 |
| `gen_ai.response.id` | Recommended | `MessageMetadata` 没有这个字段 |
| `server.address` / `server.port` | Recommended | 地址在模型客户端里，钩子看不到 |
| `gen_ai.usage.{text,image,audio}.*` | Recommended | `MessageUsage` 不按模态拆分（缓存读写、推理 token 已记；缓存写入目前只有 `model/anthropic` 报） |
| `gen_ai.agent.description` / `.id` / `.version` | Cond. Required | `hook.AgentInfo` 只有名字与类型 |
| `gen_ai.tool.description` / `gen_ai.tool.definitions` | Recommended / Opt-In | 工具定义不在 `ToolInput` / `ModelInput` 里（`OnModelStart` 的 `Tools` 恒 nil） |
| `gen_ai.client.inference.time_per_output_chunk`、`gen_ai.client.inference.operation.{input,output}_tokens` | Recommended | 第一版只记首片时长与用量计数 |
| `gen_ai.client.operation.duration` | Recommended | 约定把它留给非推理操作；我们的非推理操作已有 agent / workflow / 工具各自的时长指标 |
| `gen_ai.client.inference.operation.details`、`gen_ai.client.operation.exception` 事件 | Opt-In / Recommended | 这两个是日志事件（Logs API），本包只用 trace 与指标；内容走 span 属性（约定的 "Recording content on attributes" 方式），异常走 span 的 `exception` 事件 |

token 的 `gen_ai.token.modality`：供应商只给总数，输入里全是文本时记 `text`；有图片 / 音频 / 文件块时分不出来，
按约定记 `unknown`。输出同理（纯文本输出记 `text`）。

## 五、`crew.*` 扩展

约定没有、框架又必须表达的维度：

- **`crew.outcome`**（success / error / timeout / cancelled / blocked）：span status 只有 Unset / Error 有用，
  取消与等人答复不是错误，按 recording-errors 的指引不该标 Error，但又不能跟成功混在一起。
- **身份与位置**：`crew.run.id`（一个任务）、`crew.graph.ns`、`crew.node.id`、`crew.node.instance`、
  `crew.agent.kind`、`crew.agent.round`。`gen_ai.conversation.id` 承载跨任务的会话。
- **HITL**：`crew.interrupt` / `crew.resume` span 事件，`crew.interrupts` / `crew.resumes` /
  `crew.interrupt.wait.duration` 指标。
- **框架机制**：压缩、存档、扇出、路由、规划、fallback 的事件与指标（`crew.compaction*`、`crew.checkpoint.*`、
  `crew.fanout.branches`、`crew.route.decisions`、`crew.plan.replans`、`crew.fallback.*`）。
- **A2A**：`crew.a2a.*`。

## 六、决策记录

| 决策 | 理由 |
|---|---|
| 只依赖 OTel API，不配 SDK | 导出器、采样、Resource 是部署决策；库配了会跟应用的配置打架。应用不配时 API 是 no-op |
| 属性键在本包定义，不引 `go.opentelemetry.io/otel/semconv` | GenAI 约定迁出主仓库后，Go 的 semconv 包从 v1.40 起不再生成 `gen_ai.*`，旧版又缺后加的键；集中在 `semconv.go`，约定改名时改一处 |
| 结局用 `crew.outcome` 而不是只看 status | 见 §5 |
| 框架事件记成 span 事件、同时转指标，不建子 span | 压缩、存档、路由是节点里的一个瞬间或一小段，子 span 会打散 span 树；聚合需求由指标满足 |
| 内容走 span 属性、默认关、截断加脱敏 | 约定把内容定为 Opt-In；属性方式不需要 Logs 管线。长度上限防止大内容撑爆 span |
| 跨进程走 A2A `Message.metadata` 而不是 HTTP 头 | A2A 传输可以不是 HTTP；a2asrv 的执行 ctx 跟 HTTP 请求断开 |
| 基数控制交给 SDK（View、cardinality limit） | SDK 已有标准机制，本包再做一套会跟它重叠且不一致 |
