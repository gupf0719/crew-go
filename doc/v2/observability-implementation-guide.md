# crew-go 可观测性 (CS-OSS Monitor 接入) 实施指南

> **范围**: crew-go 接公司内部 **CS-OSS Monitor** 监控系统, 业务直接在 CS-OSS UI 看 trace + metrics, **不用了解 OTel/Prom 标准**。本指南只讲: crew-go 在哪埋点 + 上报哪些字段, **格式按 CS-OSS Monitor 规范**, 不按标准 OTel/Prom。
>
> **CS-OSS Monitor 规范** (业务必读):
> - [AI_TRACE_SPEC.md](file:///Users/gupengfei/program/go/src/monitor-agent/docs/AI_TRACE_SPEC.md) — AI trace 字段规范 (`gen_ai.*` attribute / `gen_ai_content` event)
> - [OTHER_LANGUAGE_SDK_INTEGRATION.md](file:///Users/gupengfei/program/go/src/monitor-agent/docs/OTHER_LANGUAGE_SDK_INTEGRATION.md) — Go SDK 实现规范 (Trace ID 编码 / Resource 字段 / propagation 头)
>
> **设计决策依据**: [observability-langgraph.md](../description/three-way-comparison/observability-langgraph.md) + [observability-eino.md §8](../description/three-way-comparison/observability-eino.md)
>
> ℹ️ 三方对照主体是老 spec 视角写的, **新 spec 维度** (embedding/retrieval/rerank/guardrail / timeout / span attr 128 默认) 已在两文档 §3 末段加"CS-OSS spec 2026-05 新维度对照"差异表呼应.

## 1. Trace 埋点表 (每个 hook fire 什么 span + 字段)

按 CS-OSS Monitor 规范:
- 所有 AI span `InstrumentationLibrary.name = "LLM"` (固定)
- Span name: `gen_ai.{operation.name}.{node_name}`
- `gen_ai.operation.name` **必填** + `gen_ai.status` **建议填** (规范 line 56 + line 63). crew-go 实际产生的 operation.name 分两组:
  - **核心 4 种** (恒产生, 见下 hook→span 映射表):
    - `agent` — 一次智能体执行 (`OnAgentStart`)
    - `workflow` — 图节点 / 编排节点 (`OnNodeStart` 默认; graph 用 `OnGraphStart`)
    - `chat` — 一次大模型生成调用 (`OnModelStart`, chat / completion / responses / messages 统一归 chat)
    - `tool` — 工具 / 函数 / MCP tool 调用 (`OnToolStart` 默认)
  - **component kind 4 种** (业务**声明才产生**, 见下"Component span kind 两路径标注"): `skill` / `retrieval` / `embedding` / `guardrail` — 由 `tool.Categorizer` (tool 路径) / `compose_v2.WithNodeComponent` (GraphAgent node 路径) 两路径标注; `observability.isToolKind` 校验合法值, 非法 fallback 默认 kind
  - **命名对齐** (vs CS-OSS 规范 line 56 列的 8 种): crew-go `retrieval` 对齐规范 `gen_ai.retrieval.*`; crew-go 加 `skill` (规范无); `evaluator` 已退化删除 (CS-OSS/OTel operation.name 无 evaluator, LLM-as-judge sub-agent 走默认 agent kind=invoke_agent); 规范的 `rerank` crew-go 未内置 (业务 `WithNodeComponent("rerank")` 会因 isToolKind 不含而 fallback workflow — 要加先扩 isToolKind)
- `gen_ai.status` 5 态: `success` / `error` / `cancelled` / `blocked` (HITL / 策略阻断) / `timeout` (ctx deadline)
- hierarchy: `gen_ai.node.id` + `gen_ai.node.parent_id` (前端组装优先用, fallback OTel parent_span_id)
- Tool 节点 `gen_ai.node.name` 标为**必填** (规范 line 109), 其他节点建议填
- **限制** (见 §4):
  - **span attribute 数量**: spec line 168 默认 15 (跟 Java SDK pattern 同款本地可配), CS-OSS collector 端不 enforce → crew-go 自家 `observability/` 模块 `Config.MaxSpanAttributes=128` 默认对齐 OTel 通用 (跟 eino / LangGraph 实际体量一致). framework 内置 ~17, 业务自定义实际不超 ~111
  - **attribute value 长度** ≤ 512 char (CS-OSS spec line 169 + line 438 Java SDK 一致)
  - **attribute key 长度** ≤ 255 char (CS-OSS spec line 166)
  - **Metrics export batch size** ≤ 10000 series (CS-OSS spec line 423 — SDK 单批次上限, 不是 attribute 数限; cardinality 防爆由 §4 低基数 label 设计保障)

> ✅ **span attribute 命名（业务在 trace UI 查字段）**：所有 span attr 实际 emit 即点分（`gen_ai.node.id` / `gen_ai.model` / `gen_ai.system` / `gen_ai.agent.kind` / `gen_ai.status` / `gen_ai.usage.*` / `gen_ai.request.*` / `gen_ai.response.*` / `gen_ai.tool.*` / `gen_ai.session.id` / `gen_ai.request.id` / `error.*` / `crew.*` 等一律点分），对齐 AI_TRACE_SPEC（trace attr 点号）。**注意两套命名别混**：span attr 点号（AI_TRACE_SPEC）vs metric label 下划线（`gen_ai_model` / `gen_ai_system`，AI_METRICS_SPEC line 143/147 明确要求，见 §2）——同一语义在 trace/metric 两侧 wire 名不同，`metrics_mapping` value 里存的是 metric label 下划线名。

### crew-go hook → CS-OSS span 映射

| Hook | Span name | operation.name | 关键 attribute |
|---|---|---|---|
| **`compose_v2.OrchestrationHooks`** | | | |
| `OnGraphStart` | `gen_ai.workflow.{graph_id}` | `workflow` | `gen_ai.node.id=GraphNS`, `gen_ai.node.parent_id=""` (实证 `hooks.go` (按符号自查) 传 parentNodeID="", 嵌套 graph 关系按 GraphNS 字符串前缀解), `gen_ai.node.name=graph_id` |
| `OnGraphEnd` | (上 span 结) | — | `gen_ai.status=success\|error\|cancelled\|blocked\|timeout` |
| `OnNodeStart` | `gen_ai.workflow.{graph_id}.{node_id}` | `workflow` | `gen_ai.node.id=GraphNS/node_id`, `gen_ai.node.parent_id=GraphNS`, `gen_ai.node.name=node_id` |
| `OnNodeEnd` | (上 span 结) | — | `gen_ai.status` |
| `OnNodeEvent` | (当前 span event) | — | `event.Type` 真实 const (实证全仓 grep `^const .*EventType`): `"compaction"` (`agent_v2/compact_event.go:33`), `"store"` (`agent_v2/store_event.go:32`), `"cp"` (`compose_v2/cp_event.go:31`), `"fanout"` (`agent_v2/fanout_event.go:23`), `"history.compact"` (`agent_v2/history_event.go:30`), `"route_decision"` (`agent_v2/route_decision_event.go:29`), `"plan_replan"` (`compose_v2/plan_replan_event.go:20`); 各 event Metadata 含 `action: *XxxAction` pointer (业务消费 hook 内 type-assert 取); 注: `interrupt`/`resume` 独立 event.Type 当前不存在 (业务用 OnAgentEnd 上的 InterruptError 判断 + cp.save event 拿信息) |
| **`hook.Hooks`** | | | |
| `OnAgentStart` | `gen_ai.agent.{agent_name}` | `agent` | `gen_ai.node.id={GraphNS}/agent/{agent_name}` (实证 `observability.go` (按符号自查) 真路径含 `/{name}` 段), `gen_ai.node.name=agent_name`, (可选) `gen_ai.agent.version=...`（8-method：Run 同步 1 次 + 每次 Resume 同步 1 次, HITL 闭环 2 次; 原 OnAgentInit 逻辑迁入；`gen_ai.round` attr 已删——轮次走 MetricAgentRounds histogram）|
| `OnAgentEnd` | (上 span 结) | — | `gen_ai.status`, error.type / error.message 失败时 |
| `OnModelStart` | `gen_ai.chat.{agent_name}_model_call` | `chat` | `gen_ai.node.id={GraphNS}/agent/{agent_name}/model/r{round}` (实证 `observability.go` (按符号自查) 真路径含 `/agent/{name}` 段 + `r` 前缀), `gen_ai.node.parent_id={GraphNS}/agent/{agent_name}`, (opt-in 仅 `in.Config.ModelName != ""` 才 emit) `gen_ai.request.model=qwen3.6-flash` (默认测试模型 per CLAUDE.md; deepseek-v4-flash 备选), `gen_ai.request.temperature`, `gen_ai.request.max_tokens` + event `prompt` (元数据 `messages_count` 仅, **messages 全文走 slog**) — 注: `gen_ai.system` attr 真在 OnModelEnd emit, 不在 OnModelStart |
| `OnModelEnd` | (上 span 结) | — | `gen_ai.system=deepseek\|openai\|qwen\|...` (实证 `hooks.go` (按符号自查) 在 OnModelEnd emit 非 OnModelStart), `gen_ai.response.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.usage.total_tokens`, `gen_ai.usage.reasoning.output_tokens` (>0 才填), `gen_ai.response.finish_reasons`, `gen_ai.status=success` + event `response` (元数据 `output_chars` 仅, **message 全文走 slog**) — 注: `gen_ai.usage.cache_read.input_tokens` (>0 才填, OpenAI prompt cache 命中数, HIGH-2.2 — 业务算 cache 命中率 = cached/input 降本核心, 实证 `hooks.go` (按符号自查)); `tool_calls_count` / `finish_reason` 不在 event metadata |
| `OnModelEndStream` | 同 `OnModelEnd` | `chat` | + event `chunk` (实证 `hooks.go` (按符号自查) attrs=nil — 此 model-span `chunk` event 无 attr). 注: BasicAgent 流式逐 chunk 走的是另一条路径——`agent_v2/basic_agent.go` 内 `EventSink.Send(Event{Type:"chunk"})` 源 map 仅 `{run_id, agent}`, 但经 `nodeEventSink.Send` **框架自动注入 `graph_ns`** (所有 EventSink event 同款), OnNodeEvent 收到的是 `{run_id, agent, graph_ns}`; 且这些 chunk **不**逐条成 trace event, 而是 buffer 到 `streaming_response` (见下 Trace event 列表). **`chunk_idx`/`chunk_chars` 当前不 emit**, stream EOF 后 span 结; TTFT 走 `streaming_response` event 的 `gen_ai.response.first_token_time` + metric `gen_ai.llm.ttft.duration`, 不作 span attr |
| `OnToolStart` | `gen_ai.tool.{tool_name}` | `tool` | `gen_ai.node.id={GraphNS}/agent/{agent_name}/tool/{tool_name}/{tc.ID}`, `gen_ai.node.parent_id={GraphNS}/agent/{agent_name}` (实证 `hooks.go` (按符号自查) tool parent 是 agent 不是 model, 业务 trace 树是 tool→agent 两层不是 tool→model→agent 三层), `gen_ai.node.name=tool_name` **(必填)**, `gen_ai.tool.type=function`（当前固定，mcp tool 也标 function）, `gen_ai.tool.call_id=tc.ID` + event `tool_input` (元数据 `args_size_bytes` 仅; **args 全文走 slog**) |
| `OnToolEnd` | (上 span 结) | — | `gen_ai.status` + event `tool_output` (元数据 output_size_bytes/ok; **result 全文走 slog**, web_search 等大返回必走 slog) |
| `OnError` | (当前 span attribute) | — | `gen_ai.status=error\|timeout\|cancelled\|blocked` (按 err 类型分: ctx.DeadlineExceeded→timeout, ctx.Canceled→cancelled, InterruptError→blocked, 其他→error), `error.type`, `error.message`, OTel `Status.code=ERROR` (`blocked` 时按业务语义可选 `OK` 或 `ERROR`, spec line 74 两者平等) |

### Component span kind 两路径标注

crew-go 把 `skill` / `retrieval` / `embedding` / `guardrail` 作为 span 的 **component kind** (体现在 `gen_ai.operation.name` + `spanName`), 业务声明后 trace 树能区分组件调用。两条标注路径:

| 路径 | 场景 | 怎么标 | 落到哪个 span |
|---|---|---|---|
| **① `tool.Categorizer`** | BasicAgent ReAct — 外部能力是 tool (LLM tool_call 驱动) | 业务 tool 实现 `tool.Categorizer` 返 `CategoryRetrieval` 等 → BasicAgent 入口扫 toolkit push lookup → `toolsNode` fire OnToolStart 前填 `ti.Extra["category"]` → `routeToolSpanKind` 标; `activate_skill` → `skill` | tool span (operation.name=retrieval/...) |
| **② `WithNodeComponent`** | GraphAgent 显式编排 — node **直调**组件 (不经 tool) | `AddNode(id, fn, compose_v2.WithNodeComponent("retrieval"))` → graph_loop fire OnNodeStart 前 `WithComponentKind` push ctx → OnNodeStart 读 `GetComponentKind` | node span (operation.name=retrieval, 否则 workflow) |

**统一机制**: 两路径都走 `compose_v2.WithComponentKind(ctx, kind)` ctx 透传 (跟 `WithGraphNS` 同款范式) + observability `OnNodeStart` 读 `GetComponentKind` + `isToolKind` 校验 (非法 fallback 默认 kind)。**agent span 不标 component** (OnAgentStart **不**读 component, 恒 operation.name=agent 对齐 OTel invoke_agent — 原 ReflectAgent evaluator/reflector 标 evaluator 已退化删除)。**只改 operation.name + spanName 展示, registry 配对锚点不变** (见下"Span 配对锚点")。

**consume 防跨层泄漏**: graph_loop 进每个 node 时 `WithComponentKind` **总是 set (含空)** — 无声明 node set 空覆盖**继承的父 component**, 防 retrieval node 的 component 顺 ctx 泄漏到 NodeFunc 内启动的子 graph / sub-agent (否则子 node 全被误标 retrieval)。component 只作用于声明它的那一层。**业界对照**: LangGraph/eino 组件是编排一等公民有独立 callback; crew-go 外部能力统一 tool (ReAct) + GraphAgent 显式编排 node, 用 Categorizer/WithNodeComponent 标 kind 是更轻量的等价设计。

### Span 配对锚点 (callID — 对齐 OTel 句柄 / LangChain run_id)

in-flight span 存 `sess.spans` registry, OnXxxStart 创 / OnXxxEnd 按 key 配对结束。**tool 类 span (含上述 component kind) 用 callID 作唯一配对锚点**:

- `registryKey`: `isToolKind(kind)` 为真 → `"tool|graphNS|callID"` (callID 是 tool_call 全局唯一 id, **不掺 kind/name**); 否则 (graph/agent/model/node, 无 callID) → `"{kind}|graphNS|name|round"`
- **为何**: 对齐 OTel span 对象句柄 / LangChain run_id — kind/name 是**可变展示属性**, 塞进配对 key 会让 OnToolStart/OnToolEnd 两处分别算 kind 写不一致导致失配 (历史 bug)。callID 一旦非空即足以唯一标识, kind/name 降级为 `spanEntry` 字段供 OnToolEnd 读 (不再遍历反查)
- **零冲突保证**: component kind 只改展示 (operation.name/spanName), registry key 仍按 callID (tool 类) / (kind,name,round) (其他) 配对 — 所以三路径加 component kind 不动配对锚点, 不重现失配/泄漏

### LangSmith / 降本对齐字段 (span attribute)

- `crew.dotted_order` (**所有 span**): LangSmith-style "树路径 + 起始毫秒" 编码, 业务 offline 按字符串 sort 即还原执行 timeline 不需重建树 (实证 `observability.go` (按符号自查), run_trees.py:272 同款)
- `gen_ai.usage.cache_read.input_tokens` (chat span, >0 才填): OpenAI prompt cache 命中数, 业务算 cache 命中率 = cached/input 降本核心 (实证 `hooks.go` (按符号自查))
- `gen_ai.response.first_token_time` (TTFT, 在 streaming_response event): 首 token 延迟 = first chunk 时间 − node span start, 业务流式体感 SLO (实证 `hooks.go` (按符号自查))

### 嵌套 span tree (BasicAgent → AgentAsTool(PlanAgent) → sub-agents)

> ⚠️ **两种 root 视角**：下图按"graph 作 root"画（**纯 `GraphAgent.Run` / 裸 `compose_v2.Run` 路径**）。
> 但最常见的 **`BasicAgent.Run`（经 agent_v2 wrapper）路径下 agent span 才是 root**——`OnAgentStart` 早于
> `OnGraphStart`（basic_agent.go:201，runGraph 之前），`OnGraphStart` 检测到已有 in-flight agent span 时
> **跳过建 graph span**（hooks.go::OnGraphStart skip 分支）；此时无顶层 graph workflow span，preamble/model/tools
> 直接挂 agent span 下（图中 `chat_entry` agent 实为 root，非挂在 `/model` 下）。

CS-OSS 前端按 `gen_ai.node.id` + `gen_ai.node.parent_id` 还原树:

```
gen_ai.workflow.chat_entry_graph                       node.id=""              (graph root)
├─ gen_ai.workflow.chat_entry_graph.preamble           node.id="/preamble"     parent=""
├─ gen_ai.workflow.chat_entry_graph.model              node.id="/model"        parent=""
│  └─ gen_ai.agent.chat_entry                          node.id="/agent"        parent="/model"
│     └─ gen_ai.chat.chat_entry_model_call             node.id="/model/0"      parent="/agent"
│        [event "prompt" → gen_ai_content={messages}]
│        [event "response" → gen_ai_content={message}]
└─ gen_ai.workflow.chat_entry_graph.tools              node.id="/tools"        parent=""
   └─ gen_ai.tool.ask_researcher                       node.id="/tool/call_x"  parent="/tools"
      [event "tool_input" → gen_ai_content={args}]
      └─ gen_ai.workflow.ask_researcher_graph          node.id="ask_researcher"            parent="/tool/call_x"
         └─ gen_ai.chat.ask_researcher_model_call      node.id="ask_researcher/model/0"    parent="ask_researcher"
      [event "tool_output" → gen_ai_content={result}]
```

> **注**：上图 node.id 为**简化示意片段**——真实 node.id 含完整前缀：agent=`{graphNS}/agent/{name}`、
> model=`…/agent/{name}/model/r{round}`（**有 `r` 前缀**）、tool=`…/agent/{name}/tool/{tool_name}/{callID}`
> （含 `/agent/{name}` 前缀 + `{tool_name}` 段）。完整派生格式以 §1 hook 表 + `observability.go` startSpan
> node.id switch 为准（按符号自查）。

**关键**: `gen_ai.node.id` 用 `compose_v2.GetGraphNS(ctx)` (子图嵌套唯一标识) 派生, framework 自动维护。业务**不用自管 span stack** (跟 eino 模式不同, 见 [observability-eino.md §2](../description/three-way-comparison/observability-eino.md))。

### Span attribute 跨 operation 不混 — **metric / 聚合视图必读**

CS-OSS 规范允许同一 span 既持 graph 视角字段 (`node.id` / `parent_id` / `name` — 所有 AI span 通用) 又持 LLM 视角字段 (`usage.input_tokens` 等 — 仅 `chat`/`embedding`/`rerank` span 有). 但**前端聚合 / metric 上报 / 业务统计** 都不能跨 operation.name 类型简单叠加:

| 字段 | 出现在 span | 不能直接做的事 | 正确做法 |
|---|---|---|---|
| `gen_ai.node.id` / `parent_id` | **所有** AI span | metric label (cardinality 爆: 嵌套深 NS 无界) | 仅前端构树 + 业务日志关联 |
| `gen_ai.usage.{input,output,total,cached,reasoning}_tokens` | **仅** `chat` / `embedding` / `rerank` | metric label 用 `agent` 维度 (**crew-go framework 设计** — agent span 不上报 usage.*, 业务层若想叠加上报需自管) | metric label = `{model, provider, type}`, agent 总 token = **聚合视图** (前端遍历子 chat span Σ tokens) |
| `gen_ai.agent.version` | 仅 `agent` span | 进 metric (一般 < 100 versions OK 但意义低) | 留 trace 查询用 |
| `gen_ai.tool.call_id` | 仅 `tool` span | metric label (每次唯一, 高基数) | 仅 trace 字段 |
| `gen_ai.retrieval.top_k` / `documents_count` | 仅 `retrieval` span | metric label (top_k 业务参数, documents_count 业务数据) | trace 字段 + 自定义 metric (业务自管) |

**核心原则**:
1. **agent / workflow span 自身不上报 `usage.tokens`** (**crew-go framework 设计选择**, spec line 81 允许业务上报但不强制) — agent 不直接消耗 token, 业务想看 "agent 总 token" 必须按 trace 聚合视图 `Σ 子 chat span tokens` (前端按 `node.id` 构树后遍历求和), **不是从 agent span 直接读**.
2. **metric 维度按 operation.name 独立** — `gen_ai.agent.run.total` (agent span 计数) / `gen_ai.tool.call.total` (tool span 计数) / `gen_ai.llm.tokens.total` (chat/embedding/rerank span 报 token) 三套独立 series, **不能简单相加做 "总调用数"** (一次 agent run 内嵌 N chat + M tool, 三者基数完全不同).
3. **span 数量 ≠ 业务事件数** — operation.name 不同 span 是同一次 Run 的不同层级视图, count 时按 operation.name 维度分桶.

§2 metric 表已按此原则设计 (30 metrics 各自 label 独立, agent/tool/llm 分开统计), 业务实施 PromQL 时注意**不要跨 operation 直接 sum / rate**.

### Trace event — **只塞元数据, 大内容走 log + trace_id 关联**

**关键设计原则: trace 不是 log**。CS-OSS `gen_ai_content` 虽允许 1MB, 但实际业务多轮长对话 + tool 大 JSON 返回, 单 trace 可能 MB 级体量爆 (gRPC batch + 后端持久化 + UI 渲染都卡)。crew-go 严格分层:

| 内容大小 | 去哪里 | 关联方式 |
|---|---|---|
| **< 1KB 元数据** (token count / message idx / layer / status) | trace event attribute | 直接塞 |
| **≤ 1MB 内容** (response / chunk / tool_input 单 arg / 长对话 prompt 等) | trace event `gen_ai_content` (opt-in 默认关) | 直接塞, spec line 141 上限 1MB. crew-go 自家模块 `Config.MaxInlineEventBytes=1024*1024` 默认对齐 spec, 超出按 spec 截断 + 注 `gen_ai_content.truncated=true` marker |
| **> 10KB 大内容** (长对话 prompt / web_search 大 JSON / compact summary 文本) | **业务 slog** | log 后端按 trace_id 反查 (Loki / Datadog Logs / Elastic) |

**event.name 协议** (CS-OSS 规范 + crew-go 扩展):

| event.name | 来源 | 用法 |
|---|---|---|
| `gen_ai` | CS-OSS 通用 | 业务不在意细分类型时用 (单事件含所有 gen_ai_content) |
| `prompt` | CS-OSS 细分 | LLM 调用入参 (messages 全文) |
| `response` | CS-OSS 细分 | LLM 调用出参 (assistant message 全文) |
| `tool_input` | CS-OSS 细分 | tool 入参 (args JSON) |
| `tool_output` | CS-OSS 细分 | tool 出参 (result JSON) |
| `retrieval_documents` | CS-OSS 细分 | retrieval span 召回文档 |
| `chunk` | CS-OSS 细分 | Stream 路径单 chunk (业务全采时用) |
| `error` | CS-OSS 细分 | 错误详情 (stack trace) |
| `agent_input` / `agent_output` | **crew-go 扩展 (span event)** | agent span 上 OnAgentStart 入参 / OnAgentEnd 出参 |
| `interrupt` / `resume` | **crew-go 扩展 (span event)** | agent span 上 HITL 中断 (OnAgentEnd InterruptError) / Resume (OnAgentStart) —— **是 span event.name (addEvent), 非 `compose_v2.Event.Type` const**（后者无 interrupt/resume，业务消费内部事件走 OnNodeEvent；这两个只在 trace span 上） |
| `streaming_response` | **crew-go 扩展 (node span event)** | **node** span 上 stream chunk flush 聚合 (OnNodeEnd, chunk 收尾——非 model span, 走 `addEvent(ctx, "node", ...)`) |
| `reset_stream` | **crew-go 扩展 (node span event)** | node span 上"前端清半流"信号 (OnNodeEvent 收 `Event{Type:"reset_stream"}` 非 chunk → addEvent); Metadata 展平为 event attr (含 framework 注入 `graph_ns`). 是 `Event.Type` 字面量 (无 const) 不映 metric |
| `compaction` / `store` / `cp` / `fanout` / `history.compact` / `route_decision` / `plan_replan` | **crew-go 扩展 (OnNodeEvent Event.Type)** | framework 内部事件 (业务 hook 看不到的内部操作); 各是 `compose_v2.Event.Type` const — 业务 Metadata 含 `action: *XxxAction` pointer type-switch 区分子操作; 这些走 OrchHooks.OnNodeEvent 投递, 也按 `addEvent(ctx, "node", ...)` 落 node span event + 部分路由 metric |

> **`cp` event 需显式 opt-in（默认 `false`）**：cp save/load/cleanup 每个 Run 都在跑，但 event 默认不 fire（业务不订阅时零开销，跟 4 层 compact / store / history.compact event 同设计）。业务想观测 cp 操作时开启——`agent_v2.WithEmitCpEvent(true)`（BasicAgent Option）或 `compose_v2.WithEmitCpEventCtx(ctx, true)`（注入 ctx，9 编排 wrapper 自动透传到嵌套子 graph）。**跨编排一致开关用 ctx helper**：非-BasicAgent 编排（Sequential / Parallel / Loop / …）没有 `WithEmitCpEvent` Option，业务想给整棵嵌套树开 cp event 时用 `WithEmitCpEventCtx(ctx, true)` 一次注入即可（graph_loop 入口 `cfg.emitCpEvent || isEmitCpEventCtx(ctx)` 双路径任一开都 fire）。开启后 fire `cp.{save,load,cleanup}` event，Metadata `action: *CpAction`（顶层仅 `Type` + `Save`/`Load`/`Cleanup` 三个嵌套指针，按 `Type` 取对应那个；`*CpSaveAction` / `*CpLoadAction` / `*CpCleanupAction` 里才含 `SessionID` / `NS` / `DurationMs` / `Ok` / `Err` 等字段）。
>
> **timing 契约**：`cp.save` / `cp.cleanup` fire 在 `OnGraphEnd` defer **之后**（defer LIFO 注册顺序）；`cp.load` 在 `OnGraphStart` 之后、首 `OnNodeStart` 之前。业务若按"收到 `OnGraphEnd` = Run 完全结束"假设关 SSE / commit metric，会在之后又收到 `cp.save`/`cleanup` 时困惑——以 cp event 自身或 `OnAgentEnd` 为 Run 终点锚点。
>
> **framework 级 vs 节点级**：framework cp 操作（Run defer scope，无 NodeFunc node）fire 时 `OnNodeEvent` 的 `nodeID` 为空 `""`；`nodeID != ""` 是业务自定义 NodeFunc 内调 `compose_v2.EmitCpEvent`。

### Trace event 列表 (只元数据, 不塞大内容)

| event.name | 触发 | event attribute (< 1KB 元数据) | 大内容去哪 |
|---|---|---|---|
| `prompt` | OnModelStart | `messages_count` (实证 `hooks.go` (按符号自查) 仅 messages_count, `tools_count` / `total_input_chars` 当前不 emit) | 完整 messages JSON → `slog.InfoContext(ctx, "llm.prompt", "messages", json, "trace_id", traceID)` |
| `response` | OnModelEnd | `output_chars` (实证 `hooks.go` (按符号自查) 仅 output_chars, `tool_calls_count` / `finish_reason` 当前不 emit) | 完整 response message → slog |
| `chunk` (model span) | OnModelEndStream | (此 model-span chunk event attrs=nil — `hooks.go::OnModelEndStream addEvent(..., "chunk", nil)`) | (chunk 通常 < 100 chars) |
| `streaming_response` (node span) | OnNodeEnd (chunk 收尾 flush) | `chunk_count` + (lastNanos>firstNanos 时) `streaming_seconds` + (有 node start 时) `gen_ai.response.first_token_time` (TTFT) | 拼接 content (RecordMessageContent 开时走 `gen_ai_content`, 默认关) |
| `reset_stream` (node span) | OnNodeEvent (流式 3 触发处: ReactiveCompact retry / mid-stream Interrupt / OnModelEnd hook 改写 final, 详见 hooks-and-streaming.md §7) | `ev.Content` → `content` attr + Metadata 各 key 展平为 attr (含 framework 自动注入的 `graph_ns` + `run_id` / `agent` / `reason` 等) | — |
| `tool_input` | OnToolStart | `args_size_bytes` (实证 `hooks.go` (按符号自查) 仅 args_size_bytes, `tool.name` 仅 span name 不在 event metadata) | 完整 args JSON → slog |
| `tool_output` | OnToolEnd | `output_size_bytes` / `ok` | 完整 result JSON → slog (web_search 等大返回必走 slog) |
| `compaction` (event.Type 共享) | 4 层压缩触发 | Metadata `action: *CompactionAction{Layer, Type, Before/After/Generate/Retry}` (注: framework 当前 event.attributes 只有 `action` 一个 key, **不展平**到 `compact.layer/input_tokens/output_tokens/ratio/message_indexes` 等独立 attr — 业务 hook 内 type-switch 取) | summary 全文 → `slog.InfoContext(ctx, "compact.summary", "summary", text, "layer", layer, "trace_id", traceID)` |
| `cp` (event.Type 共享) | Checkpointer 操作触发 | Metadata `action: *CpSaveAction / *CpLoadAction / *CpCleanupAction` (按操作类型独立 struct, 业务 hook 内 type-switch 取; 注: 操作名 `save/load/cleanup` 不是 `save/restore/cleanup`, `cp.age_ms` 字段当前不存在) | (cp body 不入 trace 不入 log, 持久化在 cp store) |
| `store` (event.Type 共享) | preamble store.Search / postamble store.Put | Metadata `action: *StoreAction{Type=search/put, Search/Put}` | (含 docs/messages 走 slog) |
| `fanout` | BasicAgent toolsFn fanout N>=2 tc | Metadata `action: *FanoutAction{Kind="tool_calls", Branches}` (实证 `agent_v2/fanout_event.go`) | — |
| `history.compact` | HistoryCompactor 调用 | Metadata `action: *HistoryCompactAction` (按符号自查) | — |
| `route_decision` | RoutingAgent 路由后 (OrchHooks) | Metadata `action: *RouteDecisionAction{AgentName, Target}` → `routeEventToMetric` fire `MetricRouteDecisionsTotal` | — |
| `plan_replan` | PlanAgent replan 时 (OrchHooks) | Metadata `action: *PlanReplanAction{Phase}` → `routeEventToMetric` fire `MetricPlanReplansTotal` | — |

### slog ↔ CS-OSS trace_id 关联 (业务大内容查回路径)

业务**自实现** slog handler 注 trace_id（framework **不**提供 SlogHandler/OtelSlogHandler），业务接 Loki / Datadog Logs 后:

```go
// framework 不自动装 handler——业务自实现 slog.Handler，Handle 时从 ctx 取 trace_id 注入：
// observability.GetTraceContext(ctx).TraceID

// 业务 / framework 内打日志, 自动含 trace_id 字段
slog.InfoContext(ctx, "compact.summary",
    "layer", "auto",
    "trace_id", observability.GetTraceContext(ctx).TraceID,  // 业务显式注入关联回 trace
    "summary", summaryText,
    "input_tokens", 8000,
    "output_tokens", 800)
// → Loki 按 trace_id 查: "{trace_id=\"crew-go^^abc|123\"} | json"
```

**业务 UI 用法**:
1. 看 CS-OSS Monitor trace timeline → 点 `compact` event 看元数据 (layer / ratio / message indexes)
2. 想看 summary 全文 → 拿 trace_id 跳 Loki / Datadog Logs / Elastic 查 `compact.summary` log entry
3. 同理 prompt / tool_output 等大内容都按此路径

**模块实施时**: opt-in 配置控制大内容采集开关:
```go
observability.Config{
    RecordMessageContent: true,            // 默认 false; opt-in 后采集 messages/tool 内容
    MaxInlineEventBytes:  1024*1024,       // spec 上限 1MB, < 此值塞 trace event, >= 走 slog
    RedactPatterns:       []*regexp.Regexp{...},  // PII regex 替换 [REDACTED]
}
```

## 2. Metric 收集表 (上报到 CS-OSS Monitor Metrics service)

> ⚠️ **命名规约（业务 PromQL / CS-OSS 查询必读）**：下表 metric name 与 label 为**可读展示名**（点分）。
> **实际 emit + 查询用 OTLP wire 名：全下划线、Counter 无 `_total` 后缀**（`observability/metric.go` 常量权威）。
> 映射：metric `gen_ai.llm.tokens.total`→`gen_ai_tokens`、`gen_ai.agent.run.total`→`gen_ai_agent_runs`、
> `gen_ai.llm.call.duration`→`gen_ai_model_calls`、`gen_ai.tool.call.duration`→`gen_ai_tool_calls`（其余：点→下划线、去 `.total`）；
> label `gen_ai.request.model`→`gen_ai_model`、`gen_ai.usage.type`→`gen_ai_token_type`、其余 `gen_ai.x.y`→`gen_ai_x_y` / `crew.x.y`→`crew_x_y`。

按 [OTHER_LANGUAGE_SDK_INTEGRATION.md §Metrics protobuf 映射] 上报到 Collector 5318 端口, `InstrumentationLibrary.name = "LLM"` 跟 trace 一致。

**关键: DataPoint.attributes + deprecated DataPoint.labels 双写** (CS-OSS 老 collector 兼容)。

完整 4 类 30 指标 (Sub-5f LLMError + P0/P1 interrupt/resume/replan/route + failover + member.tripped 扩后). Histogram bucket 区分场景:
- **LLM 友好 (call/ttft/agent.run.duration)**: `0.1s, 0.5s, 1s, 2s, 5s, 10s, 30s, 60s, 120s` (LLM 调用 100ms-2min 范围)
- **小耗时 (tool/compact.duration)**: `1ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s`
- **size (cp/event)**: `1KB, 10KB, 100KB, 1MB, 10MB`

### 业务 KPI metric (产品/运营关心) — 8 个

| Metric name | OTLP type | labels | 业务含义 |
|---|---|---|---|
| `gen_ai.llm.tokens.total` | Sum | `{gen_ai.agent.name, gen_ai.request.model, gen_ai.usage.type="input\|output\|think\|total\|cached"}` | LLM token 消耗 — **直接对应 $ 成本** (实证 hooks.go OnModelEnd emitToken 块 (按符号自查); type 值真为 `input/output/think/total/cached`（5 值，cached 是 prompt cache 命中）非 OTel `prompt/completion/reasoning`) |
| `gen_ai.llm.cost.usd.total` | Sum | **业务自填** (CLAUDE.md 6 业务自填之一); labels 建议 `{gen_ai.request.model, gen_ai.system}` (业务 cost metric 用) | LLM 成本 USD (按 model price table 算, 业务自配 pricing) — **月账核心 KPI** |
| `gen_ai.agent.run.total` | Sum | `{gen_ai.agent.name, gen_ai.agent.kind="base\|sequential\|...", status="success\|error\|cancelled\|blocked\|timeout"}` | agent 调用次数 — 功能使用频次, 9 编排分维 (BasicAgent kind="base") |
| `gen_ai.agent.rounds` | Histogram | `{gen_ai.agent.name, gen_ai.agent.kind}` | 每 Run ReAct 轮数 distribution — agent 效率 KPI |
| `gen_ai.interrupt.total` | Sum | **framework auto-fire** (P0 升级, 走 `agent_v2.WithEndError` ctx 协议, 不再业务自填); `{gen_ai.agent.name, gen_ai.agent.kind}` | HITL 中断次数 (实证 `metric.go` MetricInterruptTotal=`gen_ai_interrupts`) |
| `gen_ai.interrupt.resume.duration` | Histogram | **framework auto-fire** (P0, 走 `WithResumePhase` ctx 协议); `{gen_ai.agent.name, gen_ai.agent.kind}` | 中断→Resume 时长 — **用户决策时长 KPI** (秒-天范围) |
| `gen_ai.resumes.total` | Sum | **framework auto-fire** (P0); `{gen_ai.agent.name, gen_ai.agent.kind, crew.resume.action="continue\|replan\|abort"}` | Resume 次数按 action 分维 (实证 MetricResumesTotal=`gen_ai_resumes`) |
| `gen_ai.skill.activate.total` | Sum | `{gen_ai.agent.name}` | skill 激活次数 (实证 hooks.go OnToolStart skill 分支 (按符号自查) 仅按 agent name 分维; skill / mode / ok 等业务想细分需自加 label) |

### SRE metric (运维/告警关心) — 11 个 (Sub-5f LLMError + failover 扩后)

| Metric name | OTLP type | labels | 业务含义 |
|---|---|---|---|
| `gen_ai.llm.call.duration` | Histogram | `{gen_ai.agent.name, gen_ai.request.model}` | LLM 调用总耗时 — p50/p95/p99 告警 (实证 `hooks.go` (按符号自查)) |
| `gen_ai.llm.ttft.duration` | Histogram | **业务自填** (framework 不 auto-fire); labels 建议 `{gen_ai.request.model, gen_ai.system}` | **Time To First Token** (streaming 首 chunk latency) — 业务体验核心 KPI |
| `gen_ai.llm.tpot.duration` | Histogram | **业务自填**; labels 建议 `{gen_ai.request.model, gen_ai.system}` | **Time Per Output Token** (streaming 速度) — vendor 容量 |
| `gen_ai.llm.error.total` | Sum | **业务自填**; labels 建议 `{gen_ai.request.model, error_code}` | LLM 失败 — 上游健康 |
| `gen_ai.tool.call.duration` | Histogram | `{gen_ai.agent.name, gen_ai.tool.name}` | 工具调用 latency (实证 `hooks.go` (按符号自查)) |
| `gen_ai.tool.error.total` | Sum | `{gen_ai.agent.name, gen_ai.tool.name}` | 工具失败 — 业务工具健康 (实证 `hooks.go` (按符号自查); 无 error_code label) |
| `gen_ai.agent.run.duration` | Histogram | `{gen_ai.agent.name, gen_ai.agent.kind}` | agent 端到端耗时 |
| `gen_ai.compact.total` | Sum | `{crew.compact.layer="micro\|auto\|reactive", crew.outcome="true\|false"}` | 4 层压缩触发 (实证 `hooks.go` (按符号自查); 业务按 outcome=false 算压缩失败率) |
| `gen_ai.compact.duration` | Histogram | `{crew.compact.layer}` | 单次压缩耗时 (autocompact 含 LLM 调用 / micro 几 ms) |
| `gen_ai.compact.ratio` | Histogram | `{crew.compact.layer}` | 压缩率 (output/input tokens) — 业务关心 1% vs 50% |
| `gen_ai.failover.total` | Sum | `{crew.failover.reason="rate_limit\|timeout\|server_error\|network\|quota_exhausted\|api_error\|error", crew.failover.from}` | fallback 候选切换次数 — 按 reason 看哪类故障多 (sink-gated 同 compact，业务用 fallback + 接 observability 自动采集；实证 `fallback/event.go` + `hooks.go::fireFailoverMetric`) |

### 容量 metric (容量规划) — 5 个

| Metric name | OTLP type | labels | 业务含义 |
|---|---|---|---|
| `gen_ai.active.sessions` | Gauge | (无 label, 实证 observability.go session 创建 Gauge(MetricActiveSessions, nil) 按符号自查) | 当前活跃 session 数 |
| `gen_ai.runs.inprogress` | Gauge | (无 label) | 当前在跑 Run 数 |
| `gen_ai.cp.save.total` | Sum | `{crew.cp.op="save\|load\|cleanup", crew.outcome="true\|false"}` | checkpoint 操作次数 (实证 `hooks.go` (按符号自查); metric 命名 historical "cp.save.total" 实际是 cp 全部操作 counter) |
| `gen_ai.cp.size.bytes` | Histogram | (无 label) | cp 体量分布 (大 cp 排查内存/存储压力) |
| `gen_ai.fanout.branches` | Histogram | `{crew.fanout.kind="tool_calls"}` | fan-out 分支数分布 (ISSUE-81 BasicAgent toolsFn fanout; ParallelAgent fanout 当前 0 emit) |

### 业务体验/质量 metric — 5 个 (P1 replan/route 扩后)

| Metric name | OTLP type | labels | 业务含义 |
|---|---|---|---|
| `gen_ai.tool.calls.per_round` | Histogram | **业务自填**; labels 建议 `{gen_ai.agent.name}` | 每 ReAct 轮平均 tool 调用数 — **prompt 质量 KPI** (多了说明 LLM 拆解差) |
| `gen_ai.stream.chunks.total` | Sum | **业务自填**; labels 建议 `{gen_ai.request.model}` | streaming chunk 总数 (单 Run chunk 总数 / token 总数比) |
| `gen_ai.node.execution.total` | Sum | `{crew.node.id}` | 单节点执行次数 — 热点节点排查 (实证 `hooks.go` (按符号自查) 仅 crew.node.id label, 无 graph_id 单独 label) |
| `gen_ai.plan.replans.total` | Sum | **framework auto-fire** (P1); `{crew.plan.phase}` | PlanAgent replan 次数 — replan 多说明初始 plan 质量差 (实证 MetricPlanReplansTotal=`gen_ai_plan_replans`) |
| `gen_ai.route.decisions.total` | Sum | **framework auto-fire** (P1); `{gen_ai.agent.name, crew.route.target}` | RoutingAgent 路由决策按 target 分布 — 业务看流量分布 (实证 MetricRouteDecisionsTotal=`gen_ai_route_decisions`) |

## 3. crew-go 自家 `observability/` 模块实施

### 为什么不用 monitor-agent SDK (`apimachinery/pkg/monitor`)

CS-OSS 团队自家 Go SDK (`apimachinery/pkg/monitor`) 实测**不满足 crew-go 需求** (3 blocker):

| 限制 | monitor SDK 实际 | spec 应有 | crew-go 影响 |
|---|---|---|---|
| **业务 attr 数** | 写死 `MaxUserAttrNum=10`, silent drop ([attr.go:90](file:///Users/gupengfei/program/go/src/apimachinery/pkg/monitor/attr.go:90)) | spec 默认 15 可配 | chat span 框架内置 17 attr → 业务超 13 后**全 silent drop**, 不上报 `dropped_attributes_count` |
| **attr value 长度** | 一刀切 ≤ 512 char silent drop ([attr.go:78](file:///Users/gupengfei/program/go/src/apimachinery/pkg/monitor/attr.go:78)) | `gen_ai_content` event 独立 1MB ([AI_TRACE_SPEC.md:141](file:///Users/gupengfei/program/go/src/monitor-agent/docs/AI_TRACE_SPEC.md:141)) | 业务 LLM message > 512 char 全丢, **1MB 大内容路径完全没实现** |
| **attr value 类型** | `Attribute(key, val string)` 只 string ([span.go:160](file:///Users/gupengfei/program/go/src/apimachinery/pkg/monitor/span.go:160)) | spec long/double/bool/string 都支持 | `usage.input_tokens=320` 转 `"320"`, 后端 PromQL `sum(rate(...))` 无法直接聚合 |

→ crew-go 自家 `observability/` 模块按 spec 真实约束实现, **直接构造 OTLP protobuf + gRPC export**, 不引 monitor SDK.

### 包结构 (9 个 non-test 文件 ~4800 行)

```
crew-go/observability/
├── config.go          — Config (Endpoint / Resource / Limits 可配, 默认 spec)
├── id.go              — CS-OSS TraceID/SpanID 生成 (spec 格式)
├── exporter.go        — gRPC client (TraceService + MetricsService) + batch 5s tick
├── propagation.go     — x-traceid / x-spanid / X-ROOT-MESSAGE-ID inject/extract
├── observability.go   — Observability 主对象 (session/span registry, startSpan/endSpan/addEvent)
├── hooks.go           — hook.Hooks 8 + OrchHooks 5 (共 13 method, ISSUE-82 后 9→8) 实现 (trace + metric 触发点)
├── metric.go          — Counter / Histogram / Gauge OTLP DataPoint 构造 + 30 metric/19 label 常量
└── metrics_mapping.go — metric name → CS-OSS 映射
```

依赖: `go.opentelemetry.io/proto/otlp` 官方 (~100KB).

### Resource 必填字段 (业务 init 时设)

按 [OTHER_LANGUAGE_SDK_INTEGRATION.md §Resource]:

```go
// 业务 main 初始化 crew-go observability 模块
obs, err := observability.New(observability.Config{
    TraceEndpoint:   "127.0.0.1:5317",      // OTLP gRPC TraceService (业务显式配 endpoint)
    MetricsEndpoint: "127.0.0.1:5318",      // OTLP gRPC MetricsService
    // 两个 endpoint 留空 → 对应通道不上报 (本地开发/单测兜底)
    Resource: observability.Resource{
        AppName:    "my-agent-app",          // 必填, 业务自定
        EnvName:    "prod",                  // 必填, prod/test/dev
        EnvLevel:   "online",                // 建议
        Zone:       "sh",                    // 建议
        IP:         getLocalIP(),            // 必填
        SDKVersion: "crew-1.0.0",       // 必填
    },
    // 限制 (按 spec, 业务可上调)
    MaxSpanAttributes:      128,             // 默认 spec line 168 是 15, 上调对齐 OTel 通用
    MaxAttributeKeyChars:   255,             // spec line 166
    MaxAttributeValueChars: 512,             // spec line 169
    MaxInlineEventBytes:    1024 * 1024,     // gen_ai_content 1MB (spec line 141)
    MaxSpanEvents:          128,
    // Batch (spec line 419-423)
    TraceQueueMaxSize:    2048,
    TraceExportInterval:  5 * time.Second,
    TraceExportBatchSize: 512,
    MetricsExportInterval: 5 * time.Second,
    // 业务可选
    TraceRPCTimeout:      10 * time.Second,   // 单次 OTLP RPC 超时
    SamplingRatio:        1.0,                // 0.0-1.0 — ⚠ zero-value=0 视作未设走默认 1.0, 想 100% drop 用 TraceEndpoint="" 或 1e-9
    RecordMessageContent: false,              // gen_ai_content 默认关 (PII)
    RedactPatterns:       []*regexp.Regexp{}, // ⚠ RecordMessageContent=true 时必须配 (framework 不默认 redact, 业务自负 PII 合规)
})
defer obs.Close()  // ⚠ 必 defer — 否则 exporter goroutine 持续跑 + 退出前 queue 内未 flush span 全 drop

// ⚠ MaxInlineEventBytes zero-value=0 视作未设走默认 1MB; 想禁用大内容 inline 用 RecordMessageContent=false (而非 MaxInlineEventBytes=0)
```

### 把模块注入 crew-go — **业务 0 代码自动拿**

**核心立场**: framework 内 8 + 5 = 13 hook method (ISSUE-82 后 hook.Hooks 9→8 删 OnAgentInit) 全埋好按 §1 trace 埋点表 + §2 metric 收集表自动上报。业务**只一行注入**就拿到全套 trace + metrics, 不用在 NodeFunc / Agent 实现里手动 `span.SetAttributes` / `counter.Inc` (除非业务想加自定义维度):

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("my-agent"),
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(tk),
    agent_v2.WithHooks(obs.AgentHooks()),        // hook.Hooks (8 method, 业务级 Option)
)

// ⚠ OrchHooks 是 RunOption (Run 时传) 不是 NewBasicAgent Option (构造时传)
events, err := ag.Run(ctx, sess, input,
    agent_v2.WithOrchHooks(obs.OrchHooks()),     // OrchestrationHooks (5 method, 编排级)
)
```

之后业务 `agent.Run(ctx, sess, input)` 自动产出:
- **完整 trace tree** (workflow → agent → chat / tool span, 含 gen_ai.* 字段 + node.id/parent_id hierarchy + prompt/response/tool_input/tool_output event); 注: tool span parent 是 agent (不是 model), 业务 trace 树 tool→agent 两层不是三层
- **完整 metric** (token / latency / count / active sessions 等 30 指标, 见 §2)
- **直接在 CS-OSS Monitor UI 看**, 业务 0 学习成本

### Trace context propagation (按 CS-OSS 规范, 不用 W3C)

业务 HTTP server (plan_chat_web 等) 接 frontend 请求时拿 CS-OSS 头:

```go
func handler(w http.ResponseWriter, r *http.Request) {
    tc := observability.ExtractFromHeaders(r.Header)  // 返 TraceContext (不接 ctx)
    //  ↑ 读 x-traceid / x-spanid / x-tracestate / X-ROOT-MESSAGE-ID / X-ROOT-PARENT-ID
    ctx := r.Context()
    if tc.IsValid() {
        ctx = observability.WithTraceContext(ctx, tc)
    }
    events, err := agent.Run(ctx, sess, input)
}
```

业务下游 RPC 调用反向 inject — 用 `InjectToHeaders` 把当前 trace 上下文写到出口 header:

```go
tc := observability.GetTraceContext(ctx)
observability.InjectToHeaders(
    headers,           // http.Header (不接 ctx)
    tc.TraceID, tc.SpanID, tc.AppName, tc.NextIdx, tc.Sampled,
)
```

## 4. CS-OSS 限制 + crew-go 应对 (业务实施必读)

按 [OTHER_LANGUAGE_SDK_INTEGRATION.md §Attribute 编码 + §上报批次] 限制, crew-go 自家 `observability/` 模块按 spec 实现:

### Span attribute 数量 — spec 默认 15, crew-go 自家模块 Config 默认 128

CS-OSS spec line 168 写"span attributes 数量 默认最多 15 个" (与 Java SDK 一致的本地限制, line 438) — 是 **SDK 本地默认**, collector 端不 enforce. 业内 LLM trace 字段 (LangSmith / Phoenix / langfuse) 都 20-30 attr (含 cached_tokens / reasoning_tokens / cost / 各 request params), 15 不够用.

**3 方对比**:
| 框架 | 默认 span attr 上限 | 来源 |
|---|---|---|
| LangGraph | **无限** | LangSmith client 自家协议 (不走 OTel) |
| eino | **128** | 通用 OTel SDK `SpanLimits.AttributePerSpanCountLimit` 默认 |
| CS-OSS monitor SDK | **10** (业务 attr, MaxUserAttrNum 写死 silent drop) | [`apimachinery/pkg/monitor/attr.go:90`](file:///Users/gupengfei/program/go/src/apimachinery/pkg/monitor/attr.go:90) |
| CS-OSS spec | **15** (SDK 本地默认, collector 不 enforce) | spec line 168 |

**crew-go 决策** (与 monitor-agent 团队确认 collector 端不 enforce 上限后, 不用 monitor SDK 自家做):
- crew-go 自家 `observability/` 模块 `Config.MaxSpanAttributes=128` 默认对齐 OTel 通用 / eino / LangGraph
- framework 内置 chat span attr (含 reasoning.output_tokens (>0 才填) / finish_reasons / request.{temperature,max_tokens} 全保留; `cache_read.input_tokens` (>0 才填, HIGH-2.2 — `modules/common/MessageUsage.CachedInputTokens` 字段已有 + framework 自动 emit, 实证 `hooks.go` (按符号自查), 跟 §1 一致))
- 业务自定义实际不会超 ~111 (128 - 17), 超 128 时按 OTel `dropped_attributes_count` 上报 (spec line 297 要求)

**上报体量控制** (对齐 AI_TRACE_SPEC): span attr 均为小 value 标量 (operation / model /
token 数 / status 等), 体量大头是 event 的 `gen_ai_content` (prompt / 响应 / 工具出入参,
spec 定 1MB 单独上限)。故体量控制走既有机制, **不砍 span attr**:
- `RecordMessageContent: false` (默认) — 不采集 `gen_ai_content` 大内容 (对齐 spec「内容采集由业务控制、不默认采集」)
- `MaxAttributeValueChars` — 普通 attr value 长度上限
- `SamplingRatio` — 减 span 数 (与上正交: 一个减 span 数, 一个减单 span 内容量)

> **决策记录**: 曾有 `TraceMinimalAttrs` 砍 span attr 的白名单裁剪开关, **已删除**。原因:
> (1) span attr 全小 value 省不了多少体量, 真正大头 `gen_ai_content` 在 event、filter 不碰;
> (2) 白名单 fail-closed — 会砍掉 AI_TRACE_SPEC 定义的 `gen_ai.tool.call_id` /
> `gen_ai.retrieval.*` / `gen_ai.guardrail.*` / `request.temperature` 等一大批 attr, 监控前端
> 按 spec 消费时缺字段, 违反 spec。体量控制回归上述 spec 既有机制。

### Metrics export batch size ≤ 10000 series + cardinality 防爆

CS-OSS spec line 423 限的是 **Metrics export batch size ≤ 10000 series**, 不是 label 数量. 但 Prometheus best practice 是 label 数控低 + 各 label 取值低基数, crew-go §2 各 metric 1-3 labels 都按低基数设计 ✓.

**禁塞 label** (cardinality 爆 → series 超 10000 → drop):
- `user_id` / `session_id` (业务标识用 attribute 而非 label)
- `trace_id` / `span_id` / `run_id` (每 Run 唯一)
- `tool_call_id` / 时间戳 / UUID / 任意大基数枚举

**允许 label** (低基数 < 100):
- `model` (vendor 模型数十个)
- `provider` (openai/anthropic/deepseek 几个)
- `tool` (业务工具名, 几十个)
- `status` (success/error/cancelled/blocked/timeout 5 个)
- `agent.kind` (9 编排)
- `layer` (4 层 compact)
- `operation.name` (核心 agent/workflow/chat/tool + component skill/retrieval/embedding/guardrail, 见 §1; crew-go `retrieval` 对齐 spec、无 `rerank`、evaluator 已退化)

### Attribute value 长度 ≤ 512 字符 — `gen_ai.node.id` 嵌套深可能超

`gen_ai.node.id` 用 `compose_v2.GetGraphNS(ctx)` 派生, 嵌套深时易超:
```
chat_entry_graph/tools/tool/call_x/skill_deep-research/researcher_graph/model/3
                                                                              # 80+ char, 但 5-6 层嵌套可能 200-300 char
```

业务最深嵌套 (chat_entry → AgentAsTool → PlanAgent → sub-agent → SkillTool fork → researcher → model round 5+) 可能 > 512 char。

**应对**:
- 短 NS 直接用 (< 400 char 留 buffer)
- 超 `MaxAttributeValueChars`（默认 512）: `truncString` 头尾各保留约半 + `...`（UTF-8 safe 机械截断，**无 segment 感知 / 无中间 hash**，见 observability.go::truncString）
- 框架在 observability 模块 内自动判断 + 裁, 业务无感

其他易超 512 字段:
- `gen_ai.response.finish_reasons` (多个 reasons 拼接) — 截断
- error.message — 截断保留前 500 char

### Span attribute key 限制
- 非空可打印 ASCII (✓ 我所有 `gen_ai.*` 都符合)
- 长度 ≤ 255 (✓ 实际 emit 的 string attr 均远 < 255；注 `gen_ai.response.system_fingerprint` 当前未 emit)

### 上报批次 + 限流 (高 QPS 业务必读)

CS-OSS 默认:
| 项 | 默认 |
|---|---|
| Trace export interval | 5s |
| Trace queue max | 2048 spans |
| Trace max batch | 512 spans/export |
| Metrics export interval | 5s |
| Metrics export batch | 10000 series |

**crew-go 高 QPS 业务问题**:
- BasicAgent 单 Run 触发多次 hook → ~10-14 spans (深嵌套 50+ spans)
- 100 QPS Run → 1400 spans/秒 → 5s 内 7000 spans → **远超 queue 2048 → drop**

**业务应对**:
1. **业务调大 queue** (observability 模块 config `TraceQueueMaxSize: 10000` 等)
2. **crew-go SDK 必上报 dropped count** — OTLP 字段:
   - `Span.dropped_attributes_count` (>15 attribute 时 dropped 数)
   - `Span.dropped_events_count` (event 超 OTLP batch 时)
   - `Span.dropped_links_count`
   - **跟 observability 模块 协议** queue 满时也要 count + 上报某 system metric 让运维看到
3. **drop policy**: 队满 drop 最旧 (drop-oldest，老 trace 优先 drop 新进先采；exporter.go::Enqueue 无条件 drop 队头，**无** priority 豁免)

**Metrics 限流**:
- DataPoint label cardinality 超 10000 series → drop. 业务**绝不**塞高基数 label (e.g., user_id / session_id / 时间戳), 只用 model / tool / status 等低基数

## 5. crew-go 在 CS-OSS 规范上的扩展 (业务需知道)

**(1) PII 默认不采 messages 内容** — `gen_ai_content` event 默认空, crew-go observability 模块 `Config.RecordMessageContent=false` 默认。业务 opt-in `RecordMessageContent: true` + 配 `RedactPatterns []*regexp.Regexp` 替换身份证 / 手机号 / API key。**Metric labels 绝不塞 LLM content** (cardinality 爆 + PII 双重风险, CS-OSS metric labels 跟 attributes 双写也同样不塞)。

**(2) 嵌套 trace context 框架自动透 ctx** — crew-go `observability/` 模块在 `OnNodeStart` / `OnAgentStart` 创 span 时 `ctx = observability.WithSpan(ctx, span)` 透下去, 嵌套 hook 触发自动拿父 span + 自动派生 `gen_ai.node.id` (from `compose_v2.GetGraphNS(ctx)`)。业务**不用自管 span stack**。

**(3) Stream 路径 span 结束时机** — `OnModelEndStream` 拿 `MessageStreamReader`, span 不能立即 End, 必须 wrap reader 在 stream EOF 时才 `span.End()` (业务 handler 用 goroutine + done channel)。

## 6. ISSUE 实施时机

- **[ISSUE-89](../issue/issue-89-reasoning-content-thinking-model.md)** (reasoning_content): D2 adapter 用 attribute `Extra` 透传过渡, ISSUE-89 长期方案 (modules/common/Message 加 `ReasoningContent` 字段 + openai client 解析回传) post-impl polish 单独 commit. CS-OSS gen_ai 命名建议 `gen_ai.usage.reasoning_tokens` (跟 `gen_ai.usage.input_tokens` 等同款 family) 或业务跟 monitor-agent 团队对齐.
- **[ISSUE-90](../issue/issue-90-plan-chat-web-lifecycle-prod.md)** (plan_chat_web 6 状态): crew-go 已对齐 LangGraph run.status, mapping CS-OSS `gen_ai.status`:
  | crew-go | CS-OSS |
  |---|---|
  | `running` | (运行中 span 不结) |
  | `success` | `success` |
  | `error` | `error` |
  | `cancelled` | `cancelled` |
  | `interrupted` | `blocked` (HITL 中断按 CS-OSS 业务策略) |
  | (ctx.DeadlineExceeded) | `timeout` (CS-OSS 第 5 态) |
