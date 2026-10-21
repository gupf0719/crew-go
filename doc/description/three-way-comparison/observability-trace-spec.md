# 主题：observability trace/metric 字段 vs AI_TRACE_SPEC.md（CS-OSS Monitor 规范）

> ⚠️ **行号 drift 提示**：本篇硬编码的 `observability/xxx.go:NNN` 行号会随重构漂移（实测多处偏 ~180-280 行）——
> **以符号名自查为准**（如 `hooks.go::OnModelEnd`）。另：metric name/label 实际 emit 为**下划线 wire 名**
> （`gen_ai_model` / `gen_ai_system` 等，AI_METRICS_SPEC 要求）；**span attr 一律点号**（`gen_ai.model` /
> `gen_ai.system` / `gen_ai.agent.kind` / `gen_ai.status`，对齐 AI_TRACE_SPEC，与 metric label 下划线是两套命名），
> 见 `observability/metric.go` 常量 + impl-guide §2 命名规约。

> 对照对象：`monitor-agent/docs/AI_TRACE_SPEC.md` —— 外部 CS-OSS Monitor 的 **trace-only**
> 观测规范，只定义 AI span attribute + `gen_ai_content` event，**不定义任何 metric**。
> 本篇回答："crew-go 吐出来的 trace/metric 跟那份 spec 对得上吗？多了什么？还缺哪些值得补？"
>
> crew-go 侧事实全部 grep 自 `observability/hooks.go` + `observability/observability.go`
> + `observability/metric.go`（2026-05-21 核对，附行号）。

## 触发场景（主动读本篇）

- 接 CS-OSS Monitor / 改 trace span attr 命名前
- 问 "我们的 `gen_ai.*` 字段跟 spec 一致吗 / 多了什么"
- 决定要不要补 `gen_ai.response.model` / `gen_ai.system` / `gen_ai.session.id` 等 spec 字段
- 区分 "这是 trace 还是 metric"（两层不同 semconv，**极易混**）

## 一句话结论

- **trace**：spec 的**必填 + 建议**字段 crew-go 全覆盖（`operation.name` / `node.*` /
  `response.status` / `system` / `usage.{input,output,total}_tokens` / `tool.*` /
  `finish_reasons` / `error.*` + `gen_ai_content` event），可选字段也补了 `response.model` /
  `session.id` / `request.id`（见 §三），event 名对齐（prompt/response/chunk/tool_input/
  tool_output）。另有 **4 个 spec 未定义的扩展 span attr**。
- **metric**：spec 是 **trace-only，根本没有 metric 概念** → crew-go 整套 **28 metric
  （22 framework auto-fire + 6 业务自填）对这份 spec 而言全是额外维度**，无对照可言。
- **两层 semconv 不要混**（这是上一版表混在一起的根因）：
  - **trace** 用 `gen_ai.node.name` 统一承载 agent / tool / workflow 名（spec line 96
    明确**不要** `gen_ai.tool.name` / `gen_ai.agent.name` / `gen_ai.workflow.name`）。
  - **metric label** 用 `gen_ai.agent.name` / `gen_ai.tool.name`（OTel **metric** semconv，
    跟 trace 是不同层，不冲突）。
  - 唯一跨层复用同名 key 的是 `gen_ai.agent.kind`：trace 里是 agent span attr（§1.2），
    metric 里是 label（`LabelAgentKind`，metric.go:144）—— 同名不同层，知道即可。

---

## 一、Trace：crew-go vs spec

### 1.1 覆盖表（spec 定义的字段 → crew-go 状态）

图例：✅ 已 emit ｜ ⚠️ 部分/有偏差 ｜ ❌ 未 emit（gap）｜ — 不适用

**公共 Attributes（所有 AI span）**

| spec 字段 | spec 要求 | crew-go | 出处 / 说明 |
|---|---|---|---|
| `gen_ai.operation.name` | 必填 | ✅ | startSpan：agent→`agent` / model→`chat` / tool→`tool` / graph·node→`workflow`（hooks.go:160/310/458/654/696）|
| `gen_ai.node.id` | 建议 | ✅ | startSpan（observability.go:604）|
| `gen_ai.node.parent_id` | 建议 | ✅ | startSpan（observability.go:605）|
| `gen_ai.node.name` | 建议 | ✅ | startSpan（observability.go:606）|
| `gen_ai.session.id` | 可选 | ✅ | startSpan：`GetThreadID(ctx)`，业务 `WithThreadIDCtx` opt-in push（跨 run 会话聚类，类比 LangGraph thread_id）；不 push 则不 emit（observability.go）|
| `gen_ai.conversation.id` | 可选 | ❌ | crew-go 暂无对应概念（thread id 已覆盖会话聚类需求）|
| `gen_ai.request.id` | 可选 | ✅ | startSpan：`GetSessionID(ctx)`（= sid = RunID，一次 run，总 emit）（observability.go）|
| `gen_ai.status` | 建议 | ✅ | endSpan：success/error/cancelled/blocked/timeout（observability.go:887，sweepInFlight 兜底 478）|
| `error.type` | 失败时建议 | ✅ | endSpan（observability.go:893，sweepInFlight 兜底 479）|
| `error.message` | 失败时建议 | ✅ | endSpan（observability.go:894，sweepInFlight 兜底 480）；**agent span 编排错误**（MaxIterations 等非通信主体错误不 fire OnError）的 error.message 经 OnAgentEnd 传顶层 retErr 作 **fallback 兜底**（优先级 传入 > lastError > fallback，见 observability-callback.md 决策6）|

**模型调用 Attributes（chat span）**

| spec 字段 | spec 要求 | crew-go | 出处 / 说明 |
|---|---|---|---|
| `gen_ai.system` | 建议 | ✅ | OnModelEnd（`Message.Metadata.System`）；model client 经 `openai.WithSystem` 透传 provider（openai/deepseek/dashscope/qwen），空则不 emit |
| `gen_ai.request.model` | 建议 | ⚠️ | OnModelEnd 写的是**响应** model（`Message.Metadata.Model`）作请求侧代理（metric `LabelRequestModel` 同源）；OnModelStart 路径 `in.Config` 多为 nil 不填 |
| `gen_ai.response.model` | 可选 | ✅ | OnModelEnd（`Message.Metadata.Model`，与 request.model 并存，spec-correct 的响应模型键）|
| `gen_ai.request.temperature` | 可选 | ⚠️ | 仅 `in.Config != nil` 时（hooks.go:299）|
| `gen_ai.request.top_p` | 可选 | ❌ | 未 emit |
| `gen_ai.request.max_tokens` | 可选 | ⚠️ | 仅 `in.Config != nil`（hooks.go:302）|
| `gen_ai.usage.input_tokens` | 建议 | ✅ | OnModelEnd（hooks.go:347）|
| `gen_ai.usage.output_tokens` | 建议 | ✅ | OnModelEnd（hooks.go:348）|
| `gen_ai.usage.total_tokens` | 建议 | ✅ | OnModelEnd（hooks.go:349）|
| `gen_ai.response.finish_reasons` | 可选 | ✅ | OnModelEnd（hooks.go:357，`Metadata.FinishReason`）|

**Agent / Workflow / Tool / Retrieval / Guardrail**

| spec 字段 | spec 要求 | crew-go | 出处 / 说明 |
|---|---|---|---|
| `gen_ai.node.name`（agent/workflow）| 建议 | ✅ | agent / graph / node span 的 node.name |
| `gen_ai.agent.version` | 可选 | ❌ | crew-go 无 agent 版本概念 |
| `gen_ai.node.name`（tool）| Tool **必填** | ✅ | = tool 名 `in.Name`（hooks.go:459）|
| `gen_ai.tool.type` | 建议 | ✅ | 固定 `"function"`（hooks.go:454）|
| `gen_ai.tool.call_id` | 建议 | ✅ | `in.CallId`（hooks.go:453）|
| `gen_ai.retrieval.*` | （retrieval span）| — | crew-go 无 retrieval/rerank span 类型；业务自建 span 时按 spec 自填 |
| `gen_ai.guardrail.*` | （guardrail span）| — | 同上 |

**Events**

| spec 规则 | crew-go | 出处 / 说明 |
|---|---|---|
| 大内容 key 固定 `gen_ai_content` | ✅ | `const attrGenAIContent = "gen_ai_content"`（observability.go:23）；`RecordMessageContent` 开关控制是否采集（默认不采，符合 spec "不默认采集"）|
| event 名表内容类型（prompt/response/tool_input/tool_output/chunk）| ✅ | prompt（hooks.go:334）/ response（:374）/ chunk（:439）/ tool_input（:486）/ tool_output（:540）/ node 事件用 `ev.Type`（:733）|

> **小结**：spec **必填 + 建议**字段 100% 覆盖（含 `gen_ai.system`，已补，见 §三）。
> 剩余未 emit 的都是 **可选** 级（`top_p` / `agent.version` / `conversation.id`）+ 部分
> 条件性 emit（`temperature` / `max_tokens` 仅 `in.Config != nil`）——见 §三末。

### 1.2 crew-go 扩展（spec 未定义的 trace span attr）

这 4 个是 crew-go 在 spec 之外加的 **trace span 属性**（不是 metric）：

| 扩展 attr | 所在 span | 触发 hook | spec 为何没有 |
|---|---|---|---|
| `gen_ai.agent.kind` | agent | OnAgentStart（hooks.go:159）| spec 只有 `agent.version`，不分 base/sequential/parallel/... kind |
| `gen_ai.request.messages_count` | chat | OnModelStart（hooks.go:278）| spec 把正文放进 `gen_ai_content` event，不单独计数 |
| `gen_ai.usage.reasoning.output_tokens` | chat | OnModelEnd（`hooks.go::OnModelEnd`，>0 才填）| spec usage 只有 input/output/total，无 reasoning；**2026-Q2 已对齐 OTel 名**（旧 `reasoning_tokens`）|
| `gen_ai.usage.cache_read.input_tokens` | chat | OnModelEnd（`hooks.go::OnModelEnd`，>0 才填）| spec usage 无 cache 概念；= prompt cache 命中数（业务降本）；**2026-Q2 已对齐 OTel 名**（旧 `cached_input_tokens`）|

> 另有 **event 级**附加 attr（`messages_count` / `output_chars` / `args_size_bytes` /
> `output_size_bytes` / `ok`）也是 spec 未定义，但属 event attribute 非 span attribute，
> 不影响前端按 spec 组装执行树，归为次要扩展，不单列。

---

## 二、Metric：crew-go 全量（spec 未涉及）

AI_TRACE_SPEC.md 是 trace 规范，**不含任何 metric 定义**。所以下表整套对那份 spec 而言
都是 crew-go 独有维度（metric 侧的标准是 CS-OSS Monitor 的 metric 规范，见
[observability-callback.md §决策 4](observability-callback.md) + CLAUDE.md 可观测性章）。

28 metric = **22 framework auto-fire**（业务接 `obs.AgentHooks()` + `obs.OrchHooks()` 后
自动）+ **6 业务自填**（framework 拿不到 / 跨 Run 控制流，业务在自定义 hook/ChatModel 内填）。

> 下表是 **spec 对照 + 分类视角**（type / 来源 / KPI·SRE·容量·质量）。完整 metric 定义
> （label 集合 / 业务含义 / 实证位置）见 [observability-implementation-guide.md §2](../../v2/observability-implementation-guide.md)（**实施权威**，避免两处明细漂移）。

| metric name | 类型 | 来源 | 类别 |
|---|---|---|---|
| `gen_ai.llm.tokens.total` | Counter | auto | KPI |
| `gen_ai.agent.run.total` | Counter | auto | KPI |
| `gen_ai.agent.rounds` | Histogram | auto | KPI |
| `gen_ai.skill.activate.total` | Counter | auto | KPI |
| `gen_ai.llm.cost.usd.total` | Counter | **业务自填** | KPI |
| `gen_ai.interrupt.total` | Counter | auto | KPI |
| `gen_ai.interrupt.resume.duration` | Histogram | auto | KPI |
| `gen_ai.resumes.total` | Counter | auto | KPI |
| `gen_ai.llm.call.duration` | Histogram | auto | SRE |
| `gen_ai.tool.call.duration` | Histogram | auto | SRE |
| `gen_ai.tool.error.total` | Counter | auto | SRE |
| `gen_ai.agent.run.duration` | Histogram | auto | SRE |
| `gen_ai.compact.total` | Counter | auto | SRE |
| `gen_ai.compact.duration` | Histogram | auto | SRE |
| `gen_ai.compact.ratio` | Histogram | auto | SRE |
| `gen_ai.llm.ttft.duration` | Histogram | **业务自填** | SRE |
| `gen_ai.llm.tpot.duration` | Histogram | **业务自填** | SRE |
| `gen_ai.llm.error.total` | Counter | **业务自填** | SRE |
| `gen_ai.active.sessions` | Gauge | auto | 容量 |
| `gen_ai.runs.inprogress` | Gauge | auto | 容量 |
| `gen_ai.cp.save.total` | Counter | auto | 容量 |
| `gen_ai.cp.size.bytes` | Histogram | auto | 容量 |
| `gen_ai.fanout.branches` | Histogram | auto | 容量 |
| `gen_ai.node.execution.total` | Counter | auto | 质量 |
| `gen_ai.plan.replans.total` | Counter | auto | 质量 |
| `gen_ai.route.decisions.total` | Counter | auto | 质量 |
| `gen_ai.tool.calls.per_round` | Histogram | **业务自填** | 质量 |
| `gen_ai.stream.chunks.total` | Counter | **业务自填** | 质量 |

合计：auto-fire 22（KPI 7 + SRE 7 + 容量 5 + 质量 3）｜业务自填 6（KPI 1 + SRE 3 + 质量 2）。

> metric label 命名规约（与本篇无 spec 对照，仅记录）：OTel GenAI semconv 字段用
> `gen_ai.*`（`gen_ai.agent.name` / `gen_ai.request.model` / `gen_ai.usage.type` 等），
> crew-go 自定义维度用 `crew.*` prefix（`crew.compact.layer` /
> `crew.outcome` / `crew.cp.op` / `crew.node.id` / `crew.fanout.kind`）。
> 完整 label 见 `metric.go`（15 个 `Label*` 常量，按符号自查防 line drift）。

---

## 三、3 个 gap 已补（实现记录）

> 原"最有价值的 3 个可补 gap"已落地。验证：observability 单测 `TestHooks_SpecGapFields`
> / `TestHooks_SessionIDOmittedWithoutThread` + openai 白盒 `TestConvertResponse_System`
> + compose_v2 `TestThreadIDCtx` + qwen dashscope 真 LLM 集成（Generate + Stream 均
> `system=dashscope` / `model=qwen3.6-flash`）。

### gap 1：`gen_ai.response.model`（实际响应模型名）✅

OnModelEnd 在保留 `gen_ai.request.model`（metric `LabelRequestModel` 同源 + 请求侧代理）的
同时，额外 emit `gen_ai.response.model = Message.Metadata.Model`（spec-correct 的响应模型键）。
两者当前同值（framework 拿不到请求侧 model 名），但 response.model 现在落在正确的 spec 键上。

### gap 2：`gen_ai.system`（模型供应商）✅

数据流：model client `openai.WithSystem(name)` → `Message.Metadata.System` →（Stream 路经
`streamOrGenerate` 聚合 `finalSystem`）→ OnModelEnd emit `gen_ai.system` span attr。默认值：
openai=`openai` / deepseek=`deepseek` / qwen(dashscope)=`dashscope`；
业务可 `WithSystem` 覆盖；空则不 emit。另加 metric 常量 `LabelSystem` 供业务 cost metric 用。

### gap 3：`gen_ai.session.id` + `gen_ai.request.id`（LangGraph thread/run 模型）✅

区分 thread（跨 run 会话）与 run（单次执行），对齐 LangGraph `thread_id` / `run_id`：
- `gen_ai.request.id` = `GetSessionID(ctx)`（= sid = RunID，单次 run，**总 emit**）。
- `gen_ai.session.id` = `GetThreadID(ctx)`（跨 run 会话聚类）——业务经新加的
  `compose_v2.WithThreadIDCtx(ctx, convID)` **opt-in** push，convID 是**跨多次 run 不变的
  对话标识**（如多轮聊天 app 里浏览器侧持久的会话 id），把"同一会话的多次 Run"聚到一起；
  `observability_demo` 用合成值 `"demo-conversation"` 示范。**业务不 push 时不 emit**
  （不造 = request.id 的无意义重复——这是与"两个都 = sid"方案的关键差异）。
  > 注：convID 必须是**跨 run 稳定**的 id。若拿"每次 run 都新建的 id"（如 `plan_chat_web`
  > 的 `session_id`，实质 = `RunSession.RunID`）当 convID，会让 session.id ≡ request.id，
  > 退化成假区分——这种场景就别 push（保持不 emit）。

> 三方对照（gap 3 决策依据）：LangGraph 区分 thread_id（聚类同会话）vs run_id（单次执行），
> Resume 默认开新 run_id；eino 不内建（handler 自管）。crew-go 取 LangGraph 模型——thread
> opt-in、run 总 emit；详见 [observability-langgraph.md](observability-langgraph.md) 身份模型节。

### 仍未补的次要 gap（spec 可选级，暂不做）

- `gen_ai.request.top_p`：未 emit（framework modelFn 不暴露请求参数）。
- `gen_ai.request.temperature` / `max_tokens`：仅 `in.Config != nil` 时 emit（framework 路径多为 nil）。
- `gen_ai.agent.version`：crew-go 无 agent 版本概念。
- `gen_ai.conversation.id`：thread id（session.id）已覆盖会话聚类需求，不再单列。

---

## 四、OTel GenAI semconv 2026 Q2 变更 + crew-go 对齐（另一套标准轴）

> ⚠️ **两个不同 spec，别混**：本篇主体对照 `AI_TRACE_SPEC.md`（CS-OSS Monitor，trace-only，用 `gen_ai.node.name` 统一承载名字、`operation.name` 用 `agent`/`chat`/`tool`/`workflow`）。本节补充对照 **OTel GenAI semconv**（`open-telemetry/semantic-conventions-genai` **独立新仓**，2026 Q2，用 `gen_ai.operation.name=invoke_agent` + SpanKind）——这是另一套行业标准。crew-go metric label 已部分对齐它（§二注），trace 层是否对齐是**独立决策**。事实来源主纲 [`industry-survey-2026h1.md`](industry-survey-2026h1.md) §1.6 + §三 P0（已 grep 核实）。

### 4.1 `invoke_agent` 拆 client/internal（按本地/远程分 SpanKind）

OTel 把 `invoke_agent` 按 SpanKind 拆两种，reference report（`docs/gen-ai/gen-ai-agent-spans.md`）直接点名框架归属：

| operation.name | SpanKind | 规范点名框架 | crew-go 应归 |
|---|---|---|---|
| `create_agent` | CLIENT | 远程/托管 agent 创建 | A2A 远程创建 |
| `invoke_agent`(client) | CLIENT | aws-bedrock / azure-foundry / openai-assistants（托管） | A2A RemoteAgent |
| `invoke_agent`(internal) | **INTERNAL** | autogen / crewai / google-adk / openai-agents / pydantic-ai（本地） | **本地 9 编排** |
| `invoke_workflow` | INTERNAL | 工作流（可选——`spans.yaml:538` 不可区分时回退 `invoke_agent`） | Sequential/Parallel/Graph |
| `plan` | INTERNAL | 规划 | PlanAgent |
| `execute_tool` | INTERNAL | 工具 | tool span |

**crew-go 现状 vs OTel**：
- agent span `operation.name="agent"`（`hooks.go::OnAgentStart`，本篇 §1.1 同）；OTel 值是 `invoke_agent`——crew-go 注释称「对齐 OTel invoke_agent」但**字面值不同**。
- ✅ **SpanKind 已实现**（2026-Q2，`observability.go::startSpan`）：model/chat=CLIENT（调外部 LLM API）、本地 agent/graph/tool/node=INTERNAL；**A2A RemoteAgent 语义应为 CLIENT 但暂走 agent 路径标 INTERNAL**，待 a2a 层透传 remote 信号细分。
- **对齐建议（主纲 P0）**：本地 9 编排 → `invoke_agent` + SpanKind=INTERNAL；A2A RemoteAgent → CLIENT。
- ⚠️ **operation.name 是硬冲突，不是简单改名**：AI_TRACE_SPEC **明文规定** `operation.name=agent`（`AI_TRACE_SPEC.md:227`）且 CS-OSS Monitor 前端**读它判定节点类型**（`:29`/`:171`「前端必须优先使用它判断节点类型」）。改成 `invoke_agent` **必定**破坏现有前端节点分类——同一 key（`gen_ai.operation.name`）**无法同时满足两套标准**。
- **可分批落地**：`SpanKind`（INTERNAL/CLIENT）AI_TRACE_SPEC 未定义 → **加它不破坏现有对接**，可先做；token 属性改名是不同 key，影响面小于 operation.name。**只有 operation.name value 是硬冲突**，需产品决策（维持 AI_TRACE_SPEC / 迁 OTel / 双标准分批灰度）。

### 4.2 新增 token 属性（trace span）

OTel 2026 Q2 标准化了 reasoning + cache token——本篇 §1.2 把 `reasoning_tokens` / `cached_input_tokens` 列为「AI_TRACE_SPEC 未定义的扩展」，但 **OTel 已标准化（名字不同）**：

| OTel 新属性 | crew-go 现状 | 对齐动作 |
|---|---|---|
| `gen_ai.usage.reasoning.output_tokens` | ✅ **已改名**（旧 `reasoning_tokens`，2026-Q2，测试 `TestHooks_OTelTokenAttrsAndSpanKind`） | done |
| `gen_ai.usage.cache_read.input_tokens` | ✅ **已改名**（旧 `cached_input_tokens`） | done |
| `gen_ai.usage.cache_creation.input_tokens` | ❌ 无 | **deferred**：`message_usage.go` 只有 `CachedInputTokens`(=cache read)，现 openai/qwen/deepseek **不报 cache-write token**，现加字段=死字段；待 Claude-native provider 接入再加 |
| `gen_ai.request.reasoning.level`（low/medium/high） | ❌ 无 | 接 reasoning 模型时可透传 |

> metric 侧（`gen_ai.llm.tokens.total` counter）token type label 用 `think`/`cached`（§二）——与 OTel `gen_ai.usage.type` 的值约定也需对齐（另一层，非 trace 属性名）。

### 4.3 关系小结

crew-go trace 层当前主对 **AI_TRACE_SPEC（node.name 模型）**；**OTel GenAI semconv 是另一套**（operation.name + SpanKind）。**对齐分两类落地**：① `SpanKind`（AI_TRACE_SPEC 未定义）+ token 属性（不同 key）= **加性**，不破坏现有 CS-OSS Monitor 对接，可先做；② `operation.name` `agent`→`invoke_agent` = **硬冲突**（同 key 两标准互斥，见 §4.1 ⚠️），需产品决策。**这是「对齐第二套标准」而非「修 bug」**——主纲 §三 P0 的 OTel 对齐建议应按此分类推进。

> *本节（2026 H1 增量）audit：自审（两套 spec 框架 + 一致性）+ 独立 agent（读 AI_TRACE_SPEC 原文确认 `operation.name=agent` 硬规定 → caveat 从「可能冲突」强化为「硬冲突 + 可分批」）。OTel 事实三重核实。并入最终批量 audit。*

---

## 决策记录

| 决策 | 选择 | 理由 |
|---|---|---|
| trace 用 `node.name` 还是 `tool.name`/`agent.name` | **`node.name` 统一** | 对齐 spec line 96（spec 明确不要分散的 name 字段）|
| `gen_ai.request.model` 写请求还是响应 model | 写**响应** model（Metadata.Model）作请求侧代理 + **另 emit `response.model`** | framework 路径 `in.Config` 多为 nil 拿不到请求 model；request.model 跟 metric `LabelRequestModel` 同源、response.model 落 spec-correct 键（gap 1 已补）|
| `gen_ai.system` 透传机制 | **`Message.Metadata.System` 字段 + `openai.WithSystem` 选项** | 跟 `Metadata.Model` 同模式、加性不破坏；deepseek/qwen 各注入默认 provider 名（gap 2 已补）|
| `gen_ai.session.id` 语义 | **= ThreadID（opt-in `WithThreadIDCtx`），与 request.id=RunID 区分** | 对齐 LangGraph thread/run 模型；不 push 时不 emit，避免 = request.id 无意义重复（gap 3 已补）|
| metric 是否对照 spec | **不对照** | spec 是 trace-only，metric 走 CS-OSS Monitor metric 规范 |

## 工具失败的观测语义：四家 + 规约对照（2026-08-31 调研）

起因：框架自己判定的工具失败（超时 / 参数非法 / 未解锁）要不要对观测层现形为「失败」，
以及 HITL 审批「拒绝」算不算。

| 形态 | crew-go | LangGraph | eino | OpenAI Agents | MAF | 规约立场 |
|---|---|---|---|---|---|---|
| 工具超时 | 标失败（`tool_timeout`）| 无超时能力 | 无 | **有超时但不标 error**（`error_as_result` 直接 return 字符串，绕过 `on_handled_error`）| 无 | 返错误码即算失败（`recording-errors.md:21-24`，`error.type` 官方示例值就有 `timeout`）⟹ **我们与规约同侧、比 OpenAI 严格** |
| 参数解析失败 | 标失败 | `ToolMessage(status="error")` | fire `OnError`（整节点挂）| `ModelBehaviorError` + 专用 span message | `exception=` 字段 | 四家一致，无分歧 |
| 工具未解锁 / 不存在 | 标失败 | 标 | 配 handler 则当成功 | 不建 span | 标（span 外）| 2:2，我们与规约同侧 |
| **HITL 拒绝** | **不标失败**（但见下）| lifecycle 事件，非错误路径（`GraphBubbleUp` 排在 `except Exception` 之前）| 中断是一等控制流 | **标 span error** + `kind="approval_rejected"` | 不传 `exception=`、不建 span | 规约**授权**用自身上下文更精确定状态（`:29-37` 的 404 例子）+ 被处理掉的错误 SHOULD NOT 记录（`:39-40`）⟹ 「不计错误率」有据 |
| 失败结果合成是否收敛单一入口 | ✅ `failedToolResult` | ❌ `status="error"` 手拼 5 处 | 无合成概念 | ✅ 工厂 + 4 调用点 | 🟡 有构造器但 `exception=` 靠自觉 | — |

### 三条对我们的结论

1. **「拒绝不计进错误率」的决定有据**，别改成标失败。
2. **但「记成成功」是我们独有的**——四家里没有一家把拒绝记成成功样本（分别是标 error /
   不建 span / lifecycle 事件）。我们目前 `OnToolEnd` 收 `Success=true` ⟹ span
   `gen_ai.status="success"` + 计入 duration 的成功样本。
3. **「第三档」这个形状零先例**：OTel span status 只有 Unset/Ok/Error，全仓 grep
   `cancel|reject|declin` 在 `recording-errors.md` 零命中——第三档只能靠自定义属性表达，
   属我们自己的综合，不是照搬。

### 一处我们与规约相反的既有形状

`recording-errors.md:65-67` / `:76-77` / `:79-82` 三处 SHOULD：duration histogram 应带
`error.type`、**推荐一个含成败的 metric 而不是按状态分成两个以上**、span 与 metric 的
`error.type` 应一致。我们是 `gen_ai_tool_call_duration`（labels 无 error.type）**加**
一个仅 error 时递增的独立 counter——正是被点名不推荐的形状。MAF 的
`agent_framework.function.invocation.duration` 是按规约做的（attributes 带 error.type）。

⟹ 待拍板项见 [`doc/code/todo.md`](../../code/todo.md)（两条合成一件事做：给 duration
metric 加 outcome 维度，而不是继续往外加 counter）。

## 参考代码定位

- trace span 构造：`observability/observability.go` `startSpan`（:549）/ `endSpan`（:619）/ `addEvent`（:734）
- 业务 hook 埋点：`observability/hooks.go`（OnAgentStart:149 / OnModelStart:260 / OnModelEnd:311 / OnToolStart:422 / OnToolEnd:468 / OnNodeEvent:699）
- metric 名 + label 常量：`observability/metric.go`（28 metric / 15 `Label*` — 按符号自查防 line drift）
- 身份 ctx：`compose_v2.GetSessionID`/`WithSessionIDCtx`（request.id）+ `GetThreadID`/`WithThreadIDCtx`（session.id，gap 3 新加）（compose_v2/checkpointer.go）
- gap 数据流：`MessageMetadata.System`（modules/common）← `openai.WithSystem`（model/openai/client.go）← deepseek/qwen 默认；Stream 聚合 `finalSystem`（agent_v2/basic_agent.go streamOrGenerate）

## 跟现有 doc 的协作

- callback / hook 协议层决策（含 28 metric 分类、event-driven metric 路径）→
  [observability-callback.md](observability-callback.md)
- 三方（LangGraph / eino / crew-go）callback 对照 →
  [observability-langgraph.md](observability-langgraph.md) / [observability-eino.md](observability-eino.md)
- metric 协议总览（28 metric 分类 + label 命名 + cardinality cap）→ CLAUDE.md 可观测性章
