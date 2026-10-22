# eino 可观测性设计调研 (trace + metrics)

> **目的**: crew-go OpenTelemetry trace + 指标集成（Sub-5 2026-05 落地，2026-10 改成标准 OTel）的 eino 侧调研参考. 本文基于本地 `/Users/gupengfei/program/go/src/eino/` + `eino-demo/` 真实源码精读, 给出 file:line 证据.
>
> ⚠️ **风格特例说明**: 兄弟 three-way-comparison 文档都是**单文档含三方对照表** (如 `interrupt-resume.md` / `checkpoint-storage.md`). 本主题拆 2 文档 (本 + `observability-langgraph.md`) 单方面深度调研是**例外**, 原因:
> - **三方设计差异过大**: eino (Handler 接口 + 无后端 + Go context chain) vs LangGraph (callback + LangSmith 强绑 + Python contextvars), 不像 Interrupt/Resume 那样有共同语义可以正面对照
> - **crew-go 的实现已落地**：`observability/` 包按 OpenTelemetry API 实现（SDK 由应用配），属性与指标按 GenAI 语义约定（键名集中在 `semconv.go`）；业务级钩子 `agent_hooks.go` + 编排级钩子 `orch_hooks.go` + 包装层 `wrap.go` + A2A 跨进程传播 `a2a.go`。协议层基础：`hook.Hooks` 8 方法 + `compose_v2.OrchestrationHooks` 5 方法。设计与用法见 [doc/v2/observability.md](../../v2/observability.md)，字段逐项对照见 [observability-trace-spec.md](observability-trace-spec.md)
> - **crew-go 决策综合见本文档 §8** (业务先读 §8 拿三方对照后的决策矩阵, 再回 §1-7 / 姊妹文档 [observability-langgraph.md](observability-langgraph.md) 看证据).

## 0. 设计哲学一句话

**eino 核心库把可观测性抽象为 `callbacks.Handler` 接口 + `RunInfo` 数据模型, 不绑任何后端**——主仓零依赖 prometheus / opentelemetry, 业务实现 Handler 自己接 OTel / Prometheus / 日志。这种"灵活无后端绑定"是设计取舍 — 优点是 vendor-neutral, 代价是嵌套 trace hierarchy 需 handler 自管 span stack (业务实现复杂度高)。`AppendGlobalHandlers` 让 instrumentation 一次注入全局生效。adapter 实际实现期望走 `eino-ext` 仓 (本地无, 本调研基于主仓代码 + demo 推断 ext 仓 adapter 设计, 实际实现未验; 业务想用 eino + OTel 时建议 `git clone https://github.com/cloudwego/eino-ext` 补查)。

## 1. Trace 入口 + 接入方式

业务接入 = **实现 `callbacks.Handler` 接口 + 全局注册或调用时注入**, 不强制 import OTel/Prom。

### 全局注册 (推荐, 全 graph 覆盖)
`callbacks/interface.go:103-105`:
```go
func AppendGlobalHandlers(handlers ...Handler) {
    callbacks.GlobalHandlers = append(callbacks.GlobalHandlers, handlers...)
}
```

godoc 强调用法 (`callbacks/interface.go:95-102`):
> Global handlers run **before** per-invocation handlers provided via `compose.WithCallbacks`, giving them higher priority for instrumentation that **must observe every component invocation (e.g. distributed tracing, metrics)**.
>
> This function is **NOT thread-safe**. Call it once during program initialization (e.g. in main or TestMain), before any graph executions begin.

### 调用时注入 (per-invocation, 业务局部)
demo `eino-demo/demo09_callback/main.go:118-121`:
```go
result, err := runnable.Invoke(ctx, input, compose.WithCallbacks(logHandler))
```

### Handler 接口
公开 type alias `callbacks/interface.go:85`: `type Handler = callbacks.Handler` (实际定义在 `internal/callbacks/`)。

**5 个时机**:
- `OnStart` / `OnEnd` (sync 路径)
- `OnStartWithStreamInput` / `OnEndWithStreamOutput` (stream 路径)
- `OnError` (所有路径)

**可选优化接口 `TimingChecker`** (`callbacks/interface.go:136+`) — **跟 5 时机正交**, 不是第 6 个时机而是性能优化 hook: 实现 `Needed(ctx, info, timing) bool` 返 false 让框架**跳过该时机的 stream copy + goroutine spawn 开销**。对只关心 OnEnd 的 metrics handler 是关键性能优化 (避免 stream copy 大流被无用 handler 浪费)。

## 2. Span 命名 + Hierarchy 协议

### RunInfo 是 span 身份单元
公开 `callbacks/interface.go:41`: `type RunInfo = callbacks.RunInfo` (alias 自 `internal/callbacks/interface.go:26-32`):
```go
type RunInfo struct {
    Name      string                 // graph 节点名 (compose.WithNodeName 注入), 不唯一
    Type      string                 // 实现身份 (Typer.GetType, 如 "OpenAI" / "Qwen"); 未实现 Typer 默认反射结构体名
    Component components.Component   // 组件类别常量 (ComponentOfChatModel / ComponentOfTool / ...)
}
```

注: 公开 `callbacks/interface.go:31` godoc 提到 Type 可被 `ReuseHandlers` 留空, 业务实现 Typer 接口才有稳定 Type 值。

### 嵌套 hierarchy: **不自动**, handler 自管 span stack
**关键设计取舍 (vs LangGraph)**: RunInfo **没有** `parent_run_id` / `parent_span` 字段, eino 框架不自动维护 parent-child。

设计取舍**优点**: 给 handler 实现者最大灵活性 (多后端 / 自定义 hierarchy 协议自由)。**代价**: handler 实现复杂度高 — 自管 span stack 易内存泄漏 + 跨 stream/error 路径 push/pop 易错 (crew-go 应学 LangGraph 的 `parent_run_id` + `trace_id` 框架自动维护, 让业务 adapter 简化 — 见 §7 启示)。

机制 (`internal/callbacks/inject.go:78-100`):
```go
func On[T any](ctx context.Context, inOut T, handle Handle[T], timing CallbackTiming, start bool) (context.Context, T) {
    mgr, ok := managerFromCtx(ctx)
    if !ok { return ctx, inOut }
    nMgr := *mgr
    var info *RunInfo
    if start {
        info = nMgr.runInfo
        nMgr.runInfo = nil
        ctx = context.WithValue(ctx, CtxRunInfoKey{}, info)
    } else {
        if nMgr.runInfo != nil {
            info = nMgr.runInfo
        } else {
            info, _ = ctx.Value(CtxRunInfoKey{}).(*RunInfo)
        }
    }
    hs := make([]Handler, 0, len(nMgr.handlers)+len(nMgr.globalHandlers))
    for _, handler := range append(nMgr.handlers, nMgr.globalHandlers...) {
        // TimingChecker 过滤
        ...
    }
    var out T
    ctx, out = handle(ctx, inOut, info, hs)
    return ctxWithManager(ctx, &nMgr), out
}
```

机制要点:
- 当前 RunInfo 通过 `CtxRunInfoKey{}` 存到 ctx (`OnStart` 时), `OnEnd` 时从 ctx 取
  - **注意**: `CtxRunInfoKey` 定义在 `eino/internal/callbacks/manager.go:22`, **业务无法 import** (Go internal 包禁止外部依赖)。框架内部用此 key, 业务只能通过 callback 接口拿 `*RunInfo` (handler 方法参数)
- **嵌套 graph 调用时**框架不自动建 parent-child 关系
- Handler 必须**业务定义自己的 ctx key** 维护 span stack (典型: `var spanStackKey = &struct{}{}` 存 `[]trace.Span`, OnStart push / OnEnd pop). 业务实现伪代码:
```go
type spanStackKey struct{}

func (h *OtelHandler) OnStart(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
    parent := getStack(ctx) // 业务 helper 从 ctx 取 stack top
    _, span := h.tracer.Start(ctx, info.Name, trace.WithAttributes(...))
    return pushStack(ctx, span) // 业务 helper push 新 span 到 stack 并返新 ctx
}
func (h *OtelHandler) OnEnd(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
    span := popStack(ctx)
    span.End()
    return ctx
}
```

### Stream 路径
`internal/callbacks/inject.go:163+` 处理 `OnStartWithStreamInput` / `OnEndWithStreamOutput`——核心 `OnWithStreamHandle` (`inject.go:143-161`) 是**同步顺序 for 循环 fire handler**:
```go
inOuts := cpy(len(handlers) + 1)
for i, handler := range handlers {
    ctx = handle(ctx, handler, inOuts[i])  // 同步调用, 非 goroutine spawn
}
```

stream 用 `Copy()` 复制 `len(handlers)+1` 份让每个 handler + 下游各拿独立 reader, **handler 必须读完后 close 否则泄漏**, 但 framework 本身不 spawn goroutine——handler 内部若想异步消费 stream (业务实现细节) 才需自己 spawn goroutine + 维护 thread-safety。

OTel adapter 实现关键陷阱: span 结束时机要等 stream drain 完 (业务 handler 通常用 goroutine + wg 在 stream EOF 后调 span.End()), 不能在 OnStartWithStreamInput 返回时直接关 span。

## 3. Trace 属性 (attributes / metadata)

### ChatModel CallbackInput / Output
`components/model/callback_extra.go:65-89`:
```go
type CallbackInput struct {
    Messages   []*schema.Message
    Tools      []*schema.ToolInfo
    ToolChoice *schema.ToolChoice
    Config     *Config            // Model / MaxTokens / Temperature / TopP
    Extra      map[string]any
}

type CallbackOutput struct {
    Message    *schema.Message
    Config     *Config
    TokenUsage *TokenUsage        // ★ 标准 token 属性
    Extra      map[string]any
}
```

### TokenUsage 完整字段
`components/model/callback_extra.go:25-36`:
```go
type TokenUsage struct {
    PromptTokens             int
    PromptTokenDetails       PromptTokenDetails       // CachedTokens
    CompletionTokens         int
    TotalTokens              int
    CompletionTokensDetails  CompletionTokensDetails  // ReasoningTokens (OpenAI/Gemini/ARK/Qwen)
}
```

### Tool CallbackInput / Output
`components/tool/callback_extra.go:24-40`:
```go
type CallbackInput struct {
    ArgumentsInJSON string
    Extra           map[string]any
}
type CallbackOutput struct {
    Response   string
    ToolOutput *schema.ToolResult
    Extra      map[string]any
}
```

### OpenTelemetry `gen_ai.*` 对齐
**eino 自身不对齐 gen_ai.\* semantic conventions**——eino 暴露的是 `TokenUsage` Go struct, 业务实现 OTel adapter 时手动映射:
- `PromptTokens` → `gen_ai.usage.input_tokens`
- `CompletionTokens` → `gen_ai.usage.output_tokens`
- `Config.Model` → `gen_ai.request.model`
- `Config.MaxTokens` → `gen_ai.request.max_tokens`

`Extra map[string]any` 字段是设计的"逃生口"让业务塞 vendor-specific 属性。

### OTel GenAI 约定维度对照

eino 业务侧的 OTel adapter 想对齐 GenAI 约定时，下面几个维度要自己管：

| GenAI 约定维度 | eino 处理 | crew-go |
|---|---|---|
| `gen_ai.operation.name`（chat / execute_tool / invoke_agent / invoke_workflow / retrieval / embeddings …） | 用 Component 类型（ChatModel / Tool / Embedder / Retriever / Indexer / Loader 等）暗示，adapter 按 Component 映射 | 按钩子类型直接定：OnModelStart → chat、OnToolStart → execute_tool、BasicAgent → invoke_agent、编排 → invoke_workflow；GraphAgent 节点声明组件类型 → retrieval / embeddings |
| 结局（span status 只有 Unset / Error 有用） | 只暴露 callback err，adapter 自己映射 | `crew.outcome` 五态：调用方 ctx 到期 → timeout、取消 → cancelled、中断 → blocked，其余 → error；status 只在 error / timeout 标 Error |
| span 属性数量上限 | 走通用 OTel SDK 的 `SpanLimits.AttributePerSpanCountLimit`（默认 128） | 同样交给应用配的 SDK，本包不另设 |

## 4. Metrics 入口 + 指标列表

**eino 核心库零内置 metrics**——`grep "prometheus\|Counter\|Histogram\|Gauge\|Meter" eino/` 主仓**零业务相关命中** (除注释提到 "instrumentation that must observe every component invocation (e.g. distributed tracing, metrics)" 是设计意图说明)。

业务实现 metrics 思路 (demo09_callback 手工示例 `eino-demo/demo09_callback/main.go:136-142`):
```go
callCount := 0
statsHandler := callbacks.NewHandlerBuilder().
    OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
        callCount++
        return ctx
    }).
    Build()
```

这是 demo 级原始示例——生产实现需要在 Handler 里:
- OnStart 时记时间戳到 ctx
- OnEnd 时算 delta → histogram.Record
- 用 atomic counter / OTel SDK Meter 而非裸 `int` 防 race

业务级 metrics 应放在 **`eino-ext/callbacks-helper`** 或 **`eino-ext/observability`** 包 — **本地 `/Users/gupengfei/program/go/src/eino-ext/` 不存在**, 需要 `git clone https://github.com/cloudwego/eino-ext` 补查实际 adapter 实现。

## 5. Context Propagation 嵌套传递

### Handler 链顺序 + ctx 累积传递
`internal/callbacks/inject.go:110+` (OnStartHandle):
```go
// OnStart: 逆序执行 (倒序 for, 让首注入的 handler 最外层)
for i := len(handlers) - 1; i >= 0; i-- {
    ctx = handlers[i].OnStart(ctx, runInfo, input)
}
// OnEnd: 正序执行
for _, handler := range handlers {
    ctx = handler.OnEnd(ctx, runInfo, output)
}
```

**关键**: 每个 Handler 的 OnStart/OnEnd 可修改 ctx 并传给下一个 handler, 是 handler 间状态共享的标准机制。

### Stream 路径
Stream 处理时 callback 通过 `schema.StreamReader.Copy()` 复制给每个 handler, **framework 同步顺序 for 循环 fire handler 不 spawn goroutine** (`OnWithStreamHandle` 见 §2)。handler 内部若想异步消费 stream (典型 OTel adapter 需在 stream EOF 后 span.End()) 才需自己 spawn goroutine + 维护 thread-safety——这是业务实现细节, 不是 framework 行为。

### sub-graph / Lambda 嵌套
框架不自动建 trace context link——嵌套调用时 ctx 含上次 `CtxRunInfoKey`, 但下层 graph 会 set 新 RunInfo 覆盖。**业务 Handler 必须自己用 ctx 维护 span stack** (典型实现: 加自定义 key `spanStackKey{}` 存 `[]trace.Span` slice)。

## 6. eino-ext / callbacks-helper 实际 adapter

### eino-demo 依赖证据
`eino-demo/go.mod`:
```
github.com/cloudwego/eino v0.8.2
github.com/cloudwego/eino-ext/components/model/openai v0.1.9
github.com/cloudwego/eino-ext/libs/acl/openai v0.1.14
```

确认 `eino-ext` 是独立仓 (cloudwego/eino-ext), 主 eino 仓不含 adapter。

### 主仓 `utils/callbacks/template.go` HandlerHelper
`utils/callbacks/template.go:41-74` 提供 type-aware dispatch helper:
```go
func NewHandlerHelper() *HandlerHelper {
    return &HandlerHelper{
        composeTemplates: map[components.Component]callbacks.Handler{},
    }
}

type HandlerHelper struct { ... }

func (c *HandlerHelper) Handler() callbacks.Handler {
    return &handlerTemplate{c}
}
```

机制 (`template.go:160-189`): `handlerTemplate.OnStart` 按 `info.Component` switch 分发到对应的 type-safe handler (`ModelCallbackHandler` / `ToolCallbackHandler` / ...), 业务 register 时按组件类型分别注册回调, 避免 handler 内重复 type-cast。

这是 eino 推荐的**业务 metric/trace adapter 实现起点**, 不是 OTel/Prom 直接 adapter。

### OpenTelemetry / Prometheus adapter
**本地 `/Users/gupengfei/program/go/src/eino-ext/` 不存在 — 需要 `git clone https://github.com/cloudwego/eino-ext` 补查具体实现**。基于主仓设计推断 ext 仓应有:
- `callbacks-helper/opentelemetry/` 或 `observability/otel/` 实现 OTel Tracer adapter
- 同位置或 `observability/prometheus/` 实现 Prometheus Counter/Histogram adapter

crew-go 实施前**建议先 clone eino-ext** 看实际 adapter 设计模式 (尤其 span stack 维护 / gen_ai.* 映射 / handler thread-safety)。

## 7. 对 crew-go 启示

### ✅ 学
1. **Handler 接口承载 trace + metrics, 不内置后端绑定** — crew-go 已有 `hook.Hooks` (8 method) + `compose_v2.OrchestrationHooks` (5 method) 双轨, 跟 eino `Handler` 思路一致。trace + metrics adapter 实现这俩接口即可。
2. **AppendGlobalHandlers 模式** — 业务 init 时一次性注册全局, 后续所有调用自动覆盖。crew-go 可类比加 `agent_v2.AppendGlobalHooks(...)` / `compose_v2.AppendGlobalOrchHooks(...)` helper, **非 thread-safe 仅 init 时调用** (跟 eino godoc 同款警示)。
3. **TimingChecker 优化接口** — crew-go 当前 hook 协议是全时机 fire, 可学 eino 加 `Needed(timing) bool` 让 metrics handler 只 fire 关心的时机, 跳过 stream copy 开销。
4. **`Extra map[string]any` 逃生口** — eino TokenUsage / Tool 输出含 Extra 字段让 vendor-specific 属性透传。crew-go 当前 `hook.AgentInfo` / `ModelInput` 可类比加 Extra 字段。

### ❌ 不学
1. **嵌套 hierarchy 不自动** — eino 让 handler 自管 span stack, 业务踩坑率高 (handler 实现复杂 + 易内存泄漏)。crew-go 应学 LangGraph 的 `parent_run_id` + `trace_id` 标准化, 在 hook payload 加 `ParentSpanID` / `TraceID` 字段框架自动维护, 让 OTel adapter 实现简化。
2. **Stream callback 异步** — eino Stream Handler 需 spawn goroutine + close, 实现复杂易泄漏。crew-go 当前 `OnModelEndStream` 返新 reader 让框架继续消费聚合 final 的模式更简单, 保留。

### 🤔 取舍点
- **Handler 链顺序 (OnStart 逆序 / OnEnd 正序)** — eino 选这个让最先注入的 handler 在最外层 (类似 middleware), crew-go 当前 `ChainHooks` 按 append 顺序串行 fire。**Sub-5 落地后实证决策** (2026-05 [observability-callback.md](observability-callback.md) §"关键决策 2"): **crew-go 仍用 append 序串行不学 eino 逆序**, 但**业务侧推荐 obs.AgentHooks() 在最前** (`NewChainHooks(obs.AgentHooks(), business)`) — 让 obs 最早 fire start + defer 最晚 fire end, 防业务 hook panic 跳过 obs.OnAgentEnd 让 MetricAgentRunTotal 漏 fire.
- **`gen_ai.*` semantic conventions 强制对齐** — eino 没强制, 留给 adapter。crew-go 应**强制对齐 OpenTelemetry gen_ai.* 标准** (LLM 调用 span 用 `gen_ai.request.model` / `gen_ai.usage.*_tokens` 等标准 attribute 名), 让业务接 OTel collector 自动兼容主流 dashboard (Grafana / Honeycomb / Datadog)。**Sub-5 落地实证**: span attr 已对齐 OTel (`gen_ai.agent.kind` / `gen_ai.request.model` 等), 但 metric label 简化为短名 (`kind` / `model`) — 跨源 join 仍需业务自管 mapping. Round 5 audit Q1 决定全改 metric label 对齐 OTel 长名.

## 8. 跨文档启示综合 (LangGraph + eino → crew-go 决策矩阵)

读完本文档 + [observability-langgraph.md](observability-langgraph.md) 后, crew-go Sub-5 落地的**综合决策** (2026-05 已实施):

### ✅ 应学 (跨两份文档一致)
1. **callback / Handler 接口承载 trace context** — 跟 crew-go 现有 `hook.Hooks` + `compose_v2.OrchestrationHooks` 双轨 callback 协议一致, adapter 实现这俩接口即可, 不引新 hook 类型 (LangGraph BaseCallbackHandler 同款思路)
2. **嵌套子图 hierarchy 自动维护** — 学 LangGraph 在 hook payload 加 `ParentSpanID` / `TraceID` / `GraphNS` 字段, **不学 eino 自管 span stack 模式** (业务踩坑率高)。用 `compose_v2.GetGraphNS(ctx)` 拿 NS 作 span attribute `crew.graph_ns`
3. **AppendGlobalHandlers 模式** — 业务 init 时一次性注册, 后续自动覆盖。**非 thread-safe 仅 init 调用** (跟 eino godoc 同款警示)
4. **`gen_ai.*` semantic conventions 强制对齐** — crew-go LLM/Tool span 用 OpenTelemetry 标准 attribute (`gen_ai.request.model` / `gen_ai.usage.input_tokens` 等), 让业务 OTel collector 兼容 Grafana / Honeycomb / Datadog 自动
5. **configurable 字段自动 → span metadata** — 学 LangGraph 把 ctx-bound scalar 自动透传到 trace, 加敏感字段 (`key/token/secret/password/auth`) 过滤白名单

### ❌ 不学 (跨两份文档一致)
1. **强绑 SaaS 商业产品** — LangGraph 双向绑 LangSmith (`ls_integration:langgraph` + 直接调 `_set_tracing_context`), crew-go 是 framework 不绑任何 SaaS。**adapter 必须 OpenTelemetry standard 输出, 业务自接 LangSmith/langfuse/Datadog/Jaeger**
2. **handler 自管 span stack** — eino 让业务在 ctx 维护 span stack 复杂易泄漏。crew-go 框架自动维护 (见"应学"第 2 条)

### 🤔 取舍点 (业务诉求驱动)
- **Resume 是续 trace 还是新 trace**: LangGraph 默认新 trace, crew-go plan_chat_web HITL Resume 业务期望续 trace (用户感知是一个会话). 倾向默认续 trace, 业务可显式新开
- **Handler 链顺序**: eino OnStart 逆序 / OnEnd 正序 (middleware 风格), crew-go ChainHooks 是 append 序串行——Sub-5 落地决定保 append 序简单实现 (multi-handler 时业务自管注册顺序), 不抄 eino middleware 风格 (eino 风格业务侧注册顺序心智负担更高)
- **TimingChecker 优化接口**: crew-go 当前 hook 全时机 fire (没优化点), 学 eino 加 `Needed(timing) bool` 让 metrics handler 跳无关时机 — 性能优化, 业务 metrics 多的场景值得加

### 业务实施 → [doc/v2/observability.md](../../v2/observability.md)

crew-go hook 接口对照 + 5 个特有坑 + ISSUE-89/90 实施时机全在实施指南。本 §8 只给跨文档**决策**。

  - `callbacks/interface.go` (公开 type alias + AppendGlobalHandlers + TimingChecker)
  - `internal/callbacks/interface.go` (RunInfo struct 定义)
  - `internal/callbacks/inject.go` (On() 调度 + handler 链 + stream)
  - `components/model/callback_extra.go` (ChatModel TokenUsage / CallbackInput/Output)
  - `components/tool/callback_extra.go` (Tool CallbackInput/Output)
  - `utils/callbacks/template.go` (HandlerHelper type-aware dispatch)
  - `/Users/gupengfei/program/go/src/eino-demo/demo09_callback/main.go` (业务用法示例)
- **eino-ext 本地缺**: 需 `git clone https://github.com/cloudwego/eino-ext` 看 OTel/Prom adapter 真实现 (本调研基于主仓推断)
- 姊妹文档: [observability-langgraph.md](observability-langgraph.md)
- crew-go 实施 todo: [doc/code/todo.md](../../code/todo.md) §可观测性
