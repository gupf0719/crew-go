# LangGraph 可观测性设计调研 (trace + metrics)

> **目的**: crew-go OpenTelemetry trace + 指标集成（Sub-5 2026-05 落地，2026-10 改成标准 OTel）的 LangGraph 侧调研参考. 本文基于本地 `/Users/gupengfei/program/go/src/langgraph/` + `langgraph-bb/` 真实源码精读, 给出 file:line 证据.
>
> ⚠️ **风格特例说明**: 兄弟 three-way-comparison 文档都是**单文档含三方对照表** (如 `interrupt-resume.md` / `checkpoint-storage.md`). 本主题拆 2 文档 (本 + `observability-eino.md`) 单方面深度调研是**例外**, 原因:
> - **三方设计差异过大**: LangGraph (callback + LangSmith 强绑 + Python contextvars) vs eino (Handler 接口 + 无后端 + Go context chain), 不像 Interrupt/Resume 那样有共同语义可以正面对照
> - **crew-go 的实现已落地**：`observability/` 包按 OpenTelemetry API 实现（SDK 由应用配），属性与指标按 GenAI 语义约定（键名集中在 `semconv.go`）；业务级钩子 `agent_hooks.go` + 编排级钩子 `orch_hooks.go` + 包装层 `wrap.go` + A2A 跨进程传播 `a2a.go`。协议层基础：`hook.Hooks` 8 方法 + `compose_v2.OrchestrationHooks` 5 方法。设计与用法见 [doc/v2/observability.md](../../v2/observability.md)，字段逐项对照见 [observability-trace-spec.md](observability-trace-spec.md)
>
> crew-go 综合决策矩阵见 [observability-eino.md §8](observability-eino.md#8-跨文档启示综合-langgraph--eino--crew-go-决策矩阵).

## 0. 设计哲学一句话

**LangGraph 把可观测性委托给 LangChain callback 系统 + LangSmith SaaS 双层**——框架自己只暴露 `config["callbacks"]` 注入点 + run/trace 数据模型 (中性 callback 接口是模块分离优点, crew-go 可学); 但 LangSmith 通过 metadata 标识 + `_set_tracing_context` 双向绑 (强绑 SaaS 业务级别, crew-go 不学)。框架不内置 metrics, 不直接绑 OpenTelemetry, 业务想要 OTel/Prom 走 community instrumentation (monkey-patch callback manager)。

## 1. Trace 入口 + 接入方式

业务接入 = **`config["callbacks"]` 注入 LangChainTracer**, 不强制 import 任何 tracer 类。

`libs/langgraph/langgraph/_internal/_config.py:223-228`:
```python
manager = CallbackManager.configure(
    inheritable_callbacks=config.get("callbacks"),
    inheritable_tags=all_tags,
    inheritable_metadata=config.get("metadata"),
    langsmith_inheritable_metadata=_get_tracing_metadata_defaults(config),
)
```

业务用法 (基于 `libs/langgraph/tests/test_tracing_interops.py:8-11,92` 测试代码改写, **测试用 `mock_client` fixture, 生产代码需自己 `langsmith.Client()` 初始化**):
```python
import langsmith as ls
from langchain_core.tracers import LangChainTracer

# 生产: 显式初始化 langsmith client (省略时 LangSmith SDK 自动从 LANGSMITH_API_KEY env var 取)
client = ls.Client()  # 测试代码用 mock_client fixture
tracer = LangChainTracer(client=client)
result = await graph.ainvoke(input, {"callbacks": [tracer]})
```

**关键点**:
- 不写 callback 时也工作 (no-op), 但环境变量 `LANGSMITH_API_KEY` 设置后 LangChain 自动激活 LangSmith tracer
- `langsmith_inheritable_metadata` 把 `config["configurable"]` 中的 scalar 值自动塞进 LangSmith span metadata
- 跟 OpenTelemetry 集成必须靠 community library `opentelemetry-instrumentation-langchain` monkey-patch `BaseCallbackManager.__init__` 注入 OTel handler (见 `tests/test_graph_callbacks.py:283` 注释)

## 2. Span 命名 + Hierarchy 协议

### Root span 命名
graph 顶层 invoke 的 span 名 = `config["run_name"]` 或默认 `graph.get_name()` (`libs/langgraph/langgraph/pregel/main.py:2591-2596`):

```python
run_manager = callback_manager.on_chain_start(
    None,
    input,
    name=config.get("run_name", self.get_name()),
    run_id=config.get("run_id"),
)
```

### Node span 命名
每个 node 执行单独 span, name = node 名 (注入到 metadata `langgraph_node` 字段, `libs/langgraph/langgraph/pregel/_algo.py:632-638`):
```python
metadata = {
    "langgraph_step": step,
    "langgraph_node": name,
    "langgraph_triggers": triggers,
    "langgraph_path": task_path[:3],
    "langgraph_checkpoint_ns": task_checkpoint_ns,
}
```

### Hierarchy: 3 字段三层关联
`tests/test_tracing_interops.py:116-118` 实测验证:
```python
assert child_run["parent_run_id"] == traceable_run["id"]
assert traceable_run["parent_run_id"] == parent_run["id"]
assert parent_run["trace_id"] == child_run["trace_id"] == traceable_run["trace_id"]
```

3 维度:
- **trace_id**: 整个调用链共享一个 (全局唯一)
- **parent_run_id**: 直接父 span (单层 parent-child)
- **langgraph_checkpoint_ns**: 子图嵌套层级标识。**wire 上是字符串** (`NS_SEP` 拼接的 `"supervisor:uuid1|worker:uuid2"`), 框架内部 `_messages.py:201` 用 `.split(NS_SEP)` 解析后才得到 tuple `("supervisor:uuid1", "worker:uuid2")`。业务 OTel adapter 看 metadata 时拿到的是 string, 需自行 split 才能按层级建 span tree。

子图 (subgraph) 通过 `langgraph_checkpoint_ns` 嵌套形成 tree (parent-child 单向), LangSmith / langfuse 用此还原完整调用树。

## 3. Trace 属性 (attributes / metadata)

**LangGraph 自身不实现 OpenTelemetry `gen_ai.*` semantic conventions**——span 属性 = `langgraph_*` 字段 + `configurable` 自动提取的 metadata + `_PROPAGATE_TO_METADATA` 配置字段。

### 3 类自动注入字段

**(1) `langgraph_*` 节点级元数据** — `_algo.py:632-638` 注入, 执行时每个 task 5 字段:
- `langgraph_step` / `langgraph_node` / `langgraph_triggers` / `langgraph_path` / `langgraph_checkpoint_ns`

**(2) `_PROPAGATE_TO_METADATA` 配置级字段自动透传** — `_config.py:355-365` 7 字段:
```python
_PROPAGATE_TO_METADATA = frozenset((
    "thread_id", "checkpoint_id", "checkpoint_ns",
    "task_id", "run_id", "assistant_id", "graph_id",
))
```
`config["configurable"]` 中这 7 个 key 若设置, 自动注入 metadata (业务 OTel adapter 用 `thread_id` 聚类同会话 traces, 用 `assistant_id` 区分 graph 实例)。

**(3) `configurable` scalar 自动提取** — `_config.py:340-352`:
```python
def _get_tracing_metadata_defaults(config):
    configurable = config.get("configurable")
    if not configurable: return None
    metadata: dict[str, Any] = {}
    for key, value in configurable.items():
        if _exclude_as_metadata(key, value):
            continue
        metadata[key] = value
    return metadata or None
```
Scalar 值自动转 span metadata (敏感字段 `key/token/secret/password/auth` 按 `_exclude_as_metadata` 过滤, `_OMIT` frozenset 实际 5 字段)。

### LLM / Tool 属性
**不在 langgraph 实现**——由下游 `langchain-core` 的 `ChatModel.invoke` / `BaseTool.run` callback 决定 (token_usage / model_name / latency 字段都来自 langchain-core, LangGraph 透传不加工)。

### OTel GenAI 约定维度对照

LangGraph 走 LangSmith client 自家协议（不走 OTel SDK），下面几个 GenAI 约定的维度在它那里没有直接对应：

| GenAI 约定维度 | LangGraph 处理 | crew-go |
|---|---|---|
| `gen_ai.operation.name`（chat / execute_tool / invoke_agent / invoke_workflow / retrieval / embeddings …） | langchain-core callback 的 `serialized.id`（BaseChatModel / Tool / Retriever / Embedder 等类路径）暗示，接 OTel 要自己映射 | 按钩子类型直接定：OnModelStart → chat、OnToolStart → execute_tool、BasicAgent → invoke_agent、编排 → invoke_workflow；GraphAgent 节点声明组件类型 → retrieval / embeddings |
| 结局（span status 只有 Unset / Error 有用） | `run.status` 6 态（`pending`/`running`/`success`/`error`/`interrupted`/`cancelled`），接 OTel 要自己映射 | `crew.outcome` 五态（success / error / timeout / cancelled / blocked），status 只在 error / timeout 标 Error |
| span 属性数量上限 | LangSmith client 协议不限 | 交给应用配的 OTel SDK（`SpanLimits`），本包不另设 |

## 4. Metrics 入口 + 指标列表

**LangGraph 主仓零内置 metrics**。`grep -r "prometheus\|meter\|counter\|histogram\|gauge"` 仅命中:
- PostgreSQL checkpointer 用 `counter` 字符串作 SQL 参数命名 (非 observability metrics)
- `_runner.py` 的 `LazyAtomicCounter` 内部并发任务计数 (非对外指标)

**没有 Prometheus exporter, 没有 OpenTelemetry Meter**——业务想要 metrics 必须自己:
- 在 callback handler 里手写计数 (业务侧手工统计)
- 或依赖 `opentelemetry-instrumentation-langchain` 之类 community library 的 instrumentation 层

## 5. Context Propagation 嵌套传递

### contextvars + RunnableConfig 双轨制
`libs/langgraph/langgraph/_internal/_runnable.py:65-78`:
```python
def _set_config_context(config: RunnableConfig, run: Any = None):
    config_token = var_child_runnable_config.set(config)
    if run is not None:
        from langsmith.run_helpers import _set_tracing_context
        _set_tracing_context({"parent": run})
    return config_token
```

机制:
- **`var_child_runnable_config`**: Python `ContextVar`, 跨 `async/await` boundary 自动继承
- **LangSmith `_set_tracing_context`**: 直接调 langsmith SDK 设父 run (强绑)
- **`copy_context()`**: `set_config_context` context manager 用 `copy_context()` 让 async task 继承父 ctx

### Resume 时 trace 行为
Resume 时 ExecutionInfo 重新创建, `run_id` 取自 `config["run_id"]`; **若业务不显式传 run_id, 该字段为 `None`, 由更上层 `CallbackManager.on_chain_start` 生成新 UUID** (`pregel/main.py:2591-2596` + `pregel/_algo.py:677/831/993` 3 处同款逻辑):

```python
run_id=str(rid) if (rid := config.get("run_id")) else None
```

效果: 默认 Resume **开新 trace** (新 run_id 由 on_chain_start 生成), 除非业务在 resume config 显式传 `run_id` 沿用原 trace_id。这是有意设计——Resume 是新的 logical execution, 不是原 trace 的延续。

## 6. 三方生态对照

| 维度 | LangSmith | langfuse | OpenTelemetry |
|---|---|---|---|
| LangGraph 原生集成 | ✅ **强绑** (callback metadata 含 `ls_integration:langgraph` 标识 + 直接调 `langsmith.run_helpers._set_tracing_context`) | ❌ 无 | ❌ 仅 community instrumentation |
| 接入 API 表面 | `LangChainTracer(client=langsmith.Client())` 作 callback handler + env var `LANGSMITH_API_KEY` 自动激活 | `langfuse.callback.CallbackHandler()` (langfuse-langchain 包提供, 业务自加) | 业务装 `opentelemetry-instrumentation-langchain` package 让其 monkey-patch `BaseCallbackManager` 注 OTel SpanProcessor |
| Span 协议 | LangChain Run model (parent_run_id + trace_id + dotted_order) | LangChain Run model + langfuse 自己的 trace/observation 模型 (前端兼容 Run model) | OpenTelemetry span model (含 trace_id / span_id / parent_span_id; instrumentation 层转换 Run model → OTel span) |
| 商业模式 | LangChain Inc. SaaS | 开源核心 + cloud 商业版 | CNCF 标准 (无商业产品, 后端任选) |

LangSmith 在 `pregel/main.py:2587` 直接打 `ls_integration:langgraph` metadata 让 SaaS 后端识别这是 langgraph 客户端——这是**反向强绑证据** (LangGraph 知道 LangSmith 存在)。

## 7. 对 crew-go 启示

### ✅ 学
1. **callback 接口承载 trace context** — crew-go 已有 `hook.Hooks` + `compose_v2.OrchestrationHooks` 双轨 callback, 跟 LangChain `BaseCallbackHandler` 思路一致。**trace adapter 实现这俩接口即可**, 不引入新 hook 类型 (本 session 已多次验证 hook 协议复用模式)。
2. **嵌套子图 ns 作 span hierarchy 字段** — LangGraph 用 `langgraph_checkpoint_ns` tuple, crew-go 已有等价的 `compose_v2.GetGraphNS(ctx)` 返回 NS 路径, 直接拿来作 span attribute `crew.graph_ns`。
3. **configurable 字段自动 → span metadata** — LangGraph 把 `config["configurable"]` scalar 自动塞 metadata, crew-go 可类比把 `RunSession.Values` 或 ctx-bound 自定义 key 自动透传到 trace。
4. **敏感字段过滤白名单** — `_exclude_as_metadata` 排除 `key/token/secret/password/auth`, crew-go trace adapter 必须有同款过滤。

### ❌ 不学
1. **强绑 SaaS 商业产品** — LangGraph 跟 LangSmith 双向绑 (`ls_integration:langgraph` + 直接调 `_set_tracing_context`), crew-go 是 framework 不绑任何 SaaS。**adapter 必须 OpenTelemetry standard 输出, 业务自接 LangSmith/langfuse/Datadog/Jaeger**。

### ⚠️ LangGraph 缺失 (crew-go 必补)
- **无 metrics** — LangGraph 主仓零内置 (eino 主仓也无内置, ext 仓 callbacks-helper 实际 adapter 设计需 clone eino-ext 验证, 见姊妹文档)。**crew-go 必须做** Prometheus / OpenTelemetry Meter 指标暴露, 不能学 LangGraph 这条空白。

### 🤔 取舍点
- **Resume 是续 trace 还是新 trace**? LangGraph 默认新 trace (除非显式传 run_id), crew-go plan_chat_web HITL Resume 业务**期望续 trace** (用户感知是一个会话) — 我倾向 crew-go 默认续 trace, 业务可显式新开。这是与 LangGraph 不同的取舍 (业务场景差异: LangGraph Resume 多为 long-running batch task 续跑, crew-go plan_chat_web 是交互聊天)。

> **跨文档启示综合**: 完整决策矩阵 (含 eino 对照) 见 [observability-eino.md §8](observability-eino.md#8-跨文档启示综合-langgraph--eino--crew-go-决策矩阵)。本文档单独看的启示是片面的, 必须配 eino 文档读才能全图。

## 参考

- 源码: `/Users/gupengfei/program/go/src/langgraph/libs/langgraph/`
- 关键文件:
  - `_internal/_config.py` (CallbackManager 配置)
  - `_internal/_runnable.py` (contextvars + LangSmith binding)
  - `pregel/_algo.py` (node metadata)
  - `pregel/main.py` (root span creation)
  - `tests/test_tracing_interops.py` (trace hierarchy 验证示例)
- LangGraph Platform 文档: `/Users/gupengfei/program/go/src/langgraph-bb/LANGGRAPH_PLATFORM.md`
- 姊妹文档: [observability-eino.md](observability-eino.md)
- crew-go 实施 todo: [doc/code/todo.md](../../code/todo.md) §可观测性
