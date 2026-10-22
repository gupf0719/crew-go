# 主题：observability callback / hook 协议层决策

> **跟现有 2 篇 observability doc 的边界**：
> - `observability-langgraph.md` / `observability-eino.md`：Sub-5 实施**前**的预研——trace span 命名 / hierarchy / metrics 入口
> - **本文档**：Sub-5 落地后(跨 Round 2-8 audit) 的 **callback 协议层**决策记录——hook chain / Interrupt 路径 / stream wrap / 框架自动记录的指标 vs 业务自己记的（见下 §决策 4）
> - 2026-10 观测层改成 OpenTelemetry（只依赖 OTel API，属性与指标按 GenAI 语义约定），实现与用法见 [doc/v2/observability.md](../../v2/observability.md)，
>   字段逐项对照见 [observability-trace-spec.md](observability-trace-spec.md)。本文下面凡是旧实现的细节都已按新实现改写或标注

## 触发场景

需要查阅本文档的设计决策：
- 改 hook 接口签名(加新 method / 改返回值协议)
- 决策 InterruptError 是否走 OnError 路径
- 改 ChainHooks / ChainOrchHooks panic 处理协议
- 改 stream callback (OnModelEndStream) 协议
- 增删 framework auto-fire metric vs 业务自填
- 改 metric label 命名规约(跟 trace span attr 一致性)

## 三方对照速查表（callback 协议层）

| 维度 | crew-go | eino ADK | LangGraph |
|---|---|---|---|
| **业务 hook 接口** | `hook.Hooks` 8 method | `callbacks.Handler` 5 method | `BaseCallbackHandler` 40+ method (Python) |
| **编排层 hook** | `compose_v2.OrchestrationHooks` 5 method | (合并在 Handler 内) | (合并在 BaseCallbackHandler 内) |
| **返回签名** | `(ctx, error)` (Start) / `ctx` (End) | `ctx context.Context` 全 method 一致 | void (Python async event 通知) |
| **ctx 链式传播** | ✅ ChainHooks 逐个 propagate ctx | ✅ Handler 链 ctx 串 | ❌ 独立 handler 无 ctx 链 |
| **多 handler 挂法** | `ChainHooks` / `ChainOrchHooks` 显式构造 | `AppendGlobalHandlers` 全局 + `compose.WithCallbacks` 局部 | `CallbackManager` 自动管理 inheritable handlers |
| **panic 隔离** | ❌ 不 isolate, framework outer recover 兜底 | ❌ 不 isolate, fail-fast | ❌ 不 isolate (NotImplementedError 特殊) |
| **InterruptError 路径** | **不走 OnError** (`IsInterrupt(err)` 判控制流) | 不走 OnError (Interrupt 类型识别) | 当前有 bug(#10962 tools 被错标 error) |
| **stream chunk 处理** | `OnModelEndStream` 让 hook **wrap reader** 替换 | handler 自 iterate framework copy 的 reader | framework 自动 fire `on_llm_new_token` |
| **framework auto-fire metric** | ✅ GenAI 约定 11 个 + 框架 `crew.*` 18 个（见下 §决策 4） | ❌ 无内置 (业务自管) | ⚠️ LangSmith SaaS 内置自动 (绑 LangSmith) |

## 关键决策 1：InterruptError 不走 OnError(三方共识)

### 问题
HITL 用户暂停（business tool 抛 `InterruptError`）算 "错误" 还是 "正常控制流"？

### 三方做法
| 框架 | 做法 | grep 证据 |
|---|---|---|
| **crew-go** | InterruptError **控制流**,不走 OnError. framework Run/Resume defer 调用 OnError 前先判 `if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil { OnError(...) }` | grep `IsInterrupt(err); !isInt` 命中 ~16 文件(agent_v2 / compose_v2 各编排 Run/Resume + adaptive_skill + 业务路径) |
| **eino** | 同款分离 — InterruptError 不进 OnError | 调研 agent 报告确认 |
| **LangGraph** | 当前**有 bug** — Issue #10962 报告 tool 抛 `interrupt()` 被错路由到 `on_chain_error` | GitHub Issue #10962 |

### crew-go 决策依据(Sub-5 Round 2 audit 后确认)
**不破坏 hook 协议加 `Err` 字段到 AgentOutput** —— 跟 eino/LangGraph callback 接口一致。理由:
- InterruptError 是 HITL 暂停(normal path),不是 error
- 走 OnError 会被业务监控系统计入"错误率",影响告警 / SLO
- Interrupt 数走框架自动记的 `crew.interrupts`（OnAgentEnd 按 `agent_v2.GetEndError` 认出中断），不靠业务自填

### 副作用(by-design)
中断不是错误：agent span 记 `crew.outcome=blocked`、status 不标 Error，`crew.agent.invocations` 按 `crew.outcome` 分，
挂起与成功、失败分得开。（旧实现在中断路径记 status="success"，OTel 重写时改成 blocked。）

## 关键决策 2：multi-handler chain 不做 panic 隔离

### 问题
业务挂多个 hook（自己的 LangSmith adapter + obs.AgentHooks() + 自定义 audit hook），某个 hook panic 是否中断后续 hook？

### 三方做法
| 框架 | 做法 |
|---|---|
| **crew-go** | ❌ ChainHooks 不做 per-hook isolate. framework 4 层 outer recover 兜底(agent_v2.Run/Resume + compose_v2.Run + makeToolsFn fanout goroutine + graph_loop NodeFunc),hook panic 冒泡到最近 outer recover 转 err 返业务 |
| **eino** | ❌ 同款 fail-fast,不 isolate(audit² 改后 crew-go 跟 eino 对齐) |
| **LangGraph** | ❌ 同款 |

### crew-go 决策依据
- hook panic = 业务 bug,不应静默吞掉
- 业务想 panic-safe 自己在 hook 内 defer recover
- ChainOrchHooks 5 method (OnGraphStart/End/OnNodeStart/End/OnNodeEvent) 都由 framework outer recover 兜底 panic (R29 audit C14 + audit³ A5 fix)；框架发的 cp 事件除外：只作观测，钩子 panic 在发事件那一处兜住、只记日志、不进错误（2026-10-20，成功收尾时档已清掉，报错只会引来重跑）

### 副作用(业务侧需知)
**hook 链顺序敏感** —— 业务 hook 在 obs 前 panic 时 obs.OnAgentEnd 不跑，那次调用的 span 收不了尾。业务钩子交给
`obs.AgentHooks(业务钩子…)` / `obs.OrchHooks(业务编排钩子…)` 包在里面，observability 在最外层（最早 start、最先拿到
end），里面的钩子开始失败时它当场收尾（决策 7）。

> **注**: OnAgentEnd defer 内另有 `recoverPanicToErr`(见决策6)—— 那是 **callback 可见性**(让 OnAgentEnd 看到框架自身 panic),非 hook panic 防 crash 兜底;上述 4 层 outer recover 仍是防 crash 的唯一防线,两者正交。

## 关键决策 3：stream wrap reader(crew-go 创新)

### 问题
LLM stream 路径 hook 怎么观测 chunk？

### 三方做法
| 框架 | 做法 |
|---|---|
| **crew-go** | `OnModelEndStream(ctx, info, sr) → (ctx, newSR, error)` 让 hook **wrap reader 返替换版本**,framework caller 继续用 newSR 迭代 |
| **eino** | `OnEndWithStreamOutput(ctx, info, output)` 把 framework copy 的 reader 给 hook,hook 自己 iterate + Close (注意泄漏风险) |
| **LangGraph** | framework 自动 `on_llm_new_token` per chunk fire,业务无需 wrap reader |

### crew-go 决策依据
- wrap reader 比 framework copy 灵活 — 支持 SSE adapter / 压缩 middleware / 业务自定义观测
- 代价: ChainHooks 错误路径需 defer cleanup 所有 wrapped reader stack(audit³ A4 fix 已修)
- observability 的 `OnModelEndStream` 包一层 reader，只记首片到达时间（`gen_ai.response.time_to_first_chunk` 与同名指标）；chunk 数从编排钩子的 `chunk` 事件数，记在节点 span 的 `crew.stream.chunks`

## 关键决策 4：框架自动记录的指标 vs 业务自己记的

指标名与属性在 `observability/semconv.go`，逐个说明见 [doc/v2/observability.md §3](../../v2/observability.md)。

| 类别 | 个数 | 说明 |
|---|---|---|
| GenAI 语义约定的指标 | 11 | 推理时长、首片时长、4 个 token 用量计数（按 `gen_ai.token.modality` 分）、agent 时长与 agent 内模型 / 工具调用次数、workflow 时长、工具时长 |
| 框架 `crew.*` 指标 | 18 | agent 调用次数与在途数、中断 / 续跑 / 等待时长、skill 激活、陈旧写回、节点执行次数，以及由框架事件转出来的压缩 / 存档 / 扇出 / 规划 / 路由 / fallback 指标 |
| 业务自己记 | — | 调用成本、业务成功率等框架拿不到的：用 `obs.Meter()` 建，维度照框架的属性名取 |

旧实现里的「业务自填」6 个（成本 / TTFT / TPOT / LLM 错误 / 流式片段数 / 每轮工具调用数）现在的去向：首片时长与模型调用错误
框架自动记（`gen_ai.client.inference.time_to_first_chunk`、推理时长的 `error.type` 维度），流式片段数记在节点 span 上，
agent 内工具调用次数由 `gen_ai.invoke_agent.tool_calls` 记；成本仍归业务。

### 关键设计:event-driven metric 路径
**压缩 / 存档 / 扇出 / 规划 / 路由 / fallback 指标走 OrchHooks.OnNodeEvent**，业务漏接 `obs.OrchHooks()` 时这些指标全不记、
而且不报错（框架在首次 `OnAgentEnd` 发现编排钩子一次没跑过时告警一次）。完整接入：

```go
ctx, opts := obs.Install(ctx, agent_v2.WithName("assistant"), agent_v2.WithModel(m))   // 一次接上两条链
events, err := agent_v2.NewBasicAgent(opts...).Run(ctx, sess, input)
```

agent 构造一次、ctx 每个请求另外准备的拓扑（a2a/server）两半各装各的：构造时 `agent_v2.WithHooks(obs.AgentHooks())`，
请求的 ctx 上 `agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())`。

## 关键决策 5：Event self-contained 设计(eino-inspired,跟 eino 不同的细节)

### 问题
Compact / Cp 等"持续过程"event(Before + After 配对)如何让 metric 端取完整数据算 ratio？

### 演化历史(Sub-5d audit 中改)
| 阶段 | 做法 | 问题 |
|---|---|---|
| **Sub-5d 初版** | observability 端用 `sess.compactStartNanos` cache 关联 Before+After | cache lifecycle 复杂 / 跨 observability 实例无法复用 |
| **Sub-5d audit fix** | `CompactionAfterAction` 加 `InputTokens/OutputTokens/DurationMs` 字段,caller 在 defer scope 测好填 After event | event self-contained ✓ observability 零 state ✓ |

### 跟 eino 的对比
- eino `summarization.CustomizedAction` 同款 union (Before/After/GenerateSummary 互斥),After 只带 `Messages` 不带 Before 上下文
- eino 不内置 metric instrumentation,暴露 `Callback(ctx, beforeState, afterState)` 同步 hook 给业务一次性传递 before+after
- crew-go 选**自带字段**而非 callback 模式 — 因为 framework 内自动 fire metric(零侵入)需要 observability 端单 event 取完整数据,而非依赖业务接 callback

## 关键决策 6：顶层错误完整性 / 框架自身 panic 的 callback 可见性

### 问题
agent 执行出错时 observability callback(OnAgentEnd 记 trace)分两层:(a) 会不会 crash 进程;(b) 错误(含框架自身 panic)callback 看不看得到、trace 会不会误报成功。

### 三方做法
| 框架 | 框架自身代码 panic 会 crash? | callback 看得到错误? | 源码 |
|---|---|---|---|
| **crew-go** | ❌ 不 crash(每编排 Run/Resume 顶部 `defer recoverToErr` 转 err 返业务) | ✅ OnAgentEnd defer 内 `recoverPanicToErr(recover())` 先于顶部 recoverToErr 跑、设 retErr 让 OnAgentEnd 看到 | `agent_v2/agent_helpers.go` recoverToErr/recoverPanicToErr |
| **eino** | ❌ runner.run 自身**无 recover** → 框架调度/编译 panic 冒泡 crash(节点/tool panic 才在 graph_manager/tool_node recover) | ❌ callback defer `if err!=nil {onGraphError} else {onGraphEnd}` 读命名返回值,框架自身 panic 时 err=nil → 误走 onGraphEnd 标成功 | `eino/compose/graph_run.go:109-120` |
| **LangGraph** | ❌ 不 crash(re-raise 给用户) | ✅ 顶层 `try`(含调度/编译) `except BaseException: on_chain_error(e); raise` 必看到任何 exception | `langgraph pregel/main.py:2598/2806-2808` |

crew-go 在 (a) 与 LangGraph 一致(不 crash)优于 eino;(b) 取 LangGraph "同块 recover 先、callback 后" 模式补齐(eino 在框架自身 panic 时 callback 误报成功)。

### crew-go 两个 gap 分两次修(2026-06)
1. **编排错误 error.message 落空**(commit `2f19950`):7 编排(BasicAgent + Sequential/Loop/Parallel/Reflect/Routing/Handoff)非通信主体错误(MaxIterations / build graph 失败 / 子 agent 冒泡)不 fire OnError → 旧实现的 endSpan 读 lastError miss → agent span status=error 但 error.message 空。当时的修法是给 endSpan 加兜底参数，把顶层 retErr 分类后传入。OTel 重写后 agent span 的结局与错误信息直接取 `agent_v2.GetEndError`（`observability/handle.go` 的 `classify`），不再依赖 OnError，这条兜底随之并入主路径。
2. **框架自身 panic callback 看不到**(commit `e4f1f2b`):agent wrapper 内框架自身代码(buildComposeGraph / collectEvents 等 compose_v2.Run recover 之外的 wrapper panic)panic 时,OnAgentEnd defer(LIFO 先于顶部 recoverToErr 跑)看到 retErr=nil → trace 误报成功。修:9 编排 OnAgentEnd defer 闭包内、`ctxOut := WithEndError` 前加 `recoverPanicToErr("Xxx.OnAgentEnd", recover(), &retErr)`。

### Go recover 陷阱(实施关键)
`recover()` **只在 deferred function 内直接调用**生效。`defer func(){ recoverToErr(...) }()` 里 recoverToErr 内的 `recover()` **无效**(闭包调的 helper 不算"直接调用" —— 契约测试 `agent_v2/panic_recover_internal_test.go` + go run 实测坐实)。故 `recoverPanicToErr` **不自己 recover**,而接收闭包内**直接** recover() 的值作实参:`recoverPanicToErr(ctx, "X", recover(), &retErr)`(recover() 在闭包栈帧求值有效)。

### 与决策2 的 4 层 outer recover 关系(正交,勿混)
决策2 的 4 层 outer recover 是**防 crash**(hook/NodeFunc/tool panic 转 err 不掀执行树)。本决策的 `recoverPanicToErr` 是 **callback 可见性**(让 OnAgentEnd 看到框架自身 panic 的 err)—— **不增加防 crash 层**(框架自身 panic 本就被顶部 recoverToErr 兜不 crash),只让 callback 先看到。两者正交,故决策2 仍是"4 层 outer recover",不因本决策变 5 层。

### 触发面(诚实，R3 audit 修正早期"纯防御"误判)
gap (2) 主要由**框架自身 wrapper 代码**(buildComposeGraph / collectEvents / 预检 `cper.LoadNS` / `compose_v2.Get`)panic 暴露;**业务侧也有两个窄触发面**——业务自实现 `Checkpointer.LoadNS`(BasicAgent runGraph 预检 `basic_agent.go:440`,在 `compose_v2.Run` 之前、OnAgentEnd defer 之后,**runGraph 自身无 recover**)+ GraphAgent `inputWriter`/`outputReader`(业务必填回调 `graph_agent.go:335/354`,`compose_v2.Run` 前后调)panic 时,同样**直达 OnAgentEnd defer**。仅 model/tool/hook/NodeFunc/OrchHook panic 才走 graph 引擎(`compose_v2.Run` / `runNodeWithHooks`)或 runOne recover 转 err 的正常路径。所以**非纯防御** —— eino 实测踩此坑(三方表),业务自定义 backend / GraphAgent 回调 panic 是真实触发面。契约测试 `panic_recover_internal_test.go` 验证修法**模式**(recover 语义,因 BasicAgent 框架自身 panic 端到端难注入);业务触发面(panic 的 Checkpointer/inputWriter)可另加 e2e 测坐实;9 编排真实修改靠 build + 全回归 + diff review。

## 关键决策 7：observability 把业务钩子包在里面，不靠链

### 问题
钩子链（`hook.ChainHooks` / `ChainOrchHooks`）在后面的钩子开始失败时交回**开始之前**的 ctx；框架随后对 agent 级不再调
OnAgentEnd，对节点 / 图拿开始之前的 ctx 调结束钩子（`OrchestrationHooks` 的约定：开始报错时结束照样调）。排在前面、在开始
一侧开了资源的钩子（observability 的 span、在途计数）在这两条路上都拿不到自己的东西：span 永远不结束（不会被导出），
`crew.agent.active` 永久多 1。鉴权 / 限额钩子拒绝一次调用、HITL 闸门在节点前抛中断，走的正是这条路（审计实测四种形态全中）。
旧实现靠每个会话一张在途 span 表、在 agent 结束与 `CleanupSession` 时扫尾兜住；改成纯靠 ctx 传句柄后这条兜底没了。

### 决策
`obs.AgentHooks(inner…)` / `obs.OrchHooks(inner…)`：observability 在最外层，业务钩子按顺序串在里面；里面的钩子开始失败时，
observability 按这个错误当场收掉刚开的 span（中断记 blocked 并留挂起记录，没发生的模型 / 工具调用不记时长、不计次数）。
`Install` 把 opts 里已配的 `WithHooks`、ctx 上已有的编排钩子包进来。结束钩子另外核对句柄归属（agent 按名字与 ns、节点按
节点 ID 与 ns），框架拿开始之前的 ctx 调结束钩子时不会误收外层的同名 span。

第二轮审计后补的三点（都是「observability 在最外层」才做得到的）：
- **记改写之后的值**：开始一侧先开 span、等里面的钩子跑完再记输入内容 / 工具名与参数；结束一侧里面的钩子先跑——业务在
  [消费] 字段上做的 PII 过滤、结果改判，span 里看到的是改写之后的（原先 observability 先跑，记的是原文）。
- **外来 ctx**：里面的钩子返回缓存的 / `Background` 的 ctx 时把句柄与当前 span 挂回去（框架采纳外来 ctx 时业务值先查钩子那份）。
- **装两层**：句柄的 ctx key 按 Observability 实例区分，同一实例在同一次调用里再进来时认出是重复、不开第二个 span，同一次
  事件投递只处理一次（框架不采纳 OnNodeEvent 的返回值，记号留在链内传给下一层）。

### 为什么不改链 / 框架
「start 失败时逆序收尾已成功的前缀」本是更通用的修法（`ChainHooks.OnModelEndStream` 已经这么关已包过的流），但 agent 级要把
结束错误交给前缀钩子（那个 ctx key 在 agent_v2，hook 包够不着），节点 / 图级又跟「开始报错时结束照样调」的既有约定冲突（前缀
会被收两次）——要动 hook、compose_v2、9 个编排与 a2a 的配对语义。观测层自己包一层就能在现有约定下收尾，影响面只在本包。

## 决策记录

| 日期 | 决策 | 关联 commit/ISSUE |
|---|---|---|
| 2026-05 | InterruptError 不走 OnError,跟 eino/LangGraph 一致 | Sub-5 Round 2 audit + ISSUE-82 单一所有权 |
| 2026-05 | multi-handler chain 不 panic isolate,framework outer recover 兜底 | audit² (跟 eino 对齐) + audit³ A5 + C14 |
| 2026-05 | stream wrap reader 协议保留(差异化创新) | audit³ A4 fix(ChainHooks defer cleanup) |
| 2026-05 | 17 framework auto-fire + 8 业务自填,godoc 显式标注(早期 doc 误写 13,实际 compact 3 + cp 2 展开共 17)| Sub-5 Round 1 audit fix(commit `d875f51`) |
| 2026-05 | 25 metric (Sub-5b/c/d/e 24 + Sub-5f LLMError 扩) 9 编排都修 info.Round(不只 BasicAgent) | Sub-5 Round 2 audit fix(commit `c914006`) |
| 2026-05 | Event self-contained(After 自带 InputTokens/OutputTokens/DurationMs) | Sub-5d audit fix(commit `5f2d8f3`) |
| 2026-05 | ChainHooks 错误路径返入参 ctx 不返 hook 返的 newCtx | audit³ B2 fix |
| 2026-06 | agent span 顶层错误完整性:(1) 编排错误 error.message 经 endSpan fallback 兜底(传入>lastError>fallback);(2) 框架自身 panic 经 OnAgentEnd defer `recoverPanicToErr(recover())` 让 callback 可见(取 LangGraph `except:cb(e);raise` 同块模式)。决策6 | commit `2f19950` + `e4f1f2b` + 三方调研(eino graph_run.go 无 recover / langgraph pregel try/except) |
| 2026-05 | （**已作废**：OTel 重写后导出交给应用配的 SDK，本包不再自管导出 goroutine）exporter goroutine panic **改 per-tick recover**(单次 export panic 只 drop 本次 + 记 log,**goroutine 继续下一 tick 不死**)— 学 **LangSmith SDK** `_tracing_thread_handle_batch` per-batch catch 韧性(单次 upload 失败/异常不杀 background thread)。**三方对照**:crew-go 自管 export goroutine(开箱即用,业务唯一上报路径,跟 eino 不同——eino 不自管 export 推业务/OTel SDK);OTel SDK 自管但 panic 后死不重启(脆弱);LangSmith SDK 自管 + per-batch catch 续命(韧性范例)。**正因自管 export 是业务唯一路径**(无 OTel SDK 兜底),crew-go 学 LangSmith 做 per-tick 韧性,而非 OTel SDK 的脆弱。**不做无脑重启 watchdog**(panic 是 exporter bug,重启会 panic loop;per-tick recover 已足够——单次失败隔离,下次继续) | Round 4 audit L3 + 三方调研(OTel SDK / eino / LangSmith)+ `exporter.go::flushOnce` per-tick recover + 测试 `TestExporter_FlushOnce_PerTickPanicRecover` |
| 2026-10-09 | **采纳钩子返回的 ctx 时，执行状态整份取框架自己的**（`compose_v2.AdoptHookCtx`：所在位置、session、任务代号、相位与计数、答案账本、转交收件人、历史写入器……；其余——业务值、环境、取消与截止时间——跟钩子。环境与取消这一半 10-12 改了，见下一行）。曾经各采纳点各铺回几样，钩子返回别处缓存的 ctx（想把 span 挂在一起的常见写法）时，漏铺的那几样静默出错：转交没人取、模型答复记不进会话历史（实测，`err == nil`）。**对照**：eino 用回调返回的 ctx 跑节点，之后只重铺回调系统自己那一个 key（`internal/callbacks/inject.go` 的 `ctxWithManager`），别的框架状态靠「返回派生 ctx」的约定——就是我们修之前的形状；LangChain / LangGraph 的回调不返回 ctx，结构上换不掉执行上下文。我们保留「钩子能往下游推值」，代价是框架的每个 ctx key 都要登记成执行状态或环境（机械测试兜底） | `b6d33bab`；哨兵 `agent_v2/hook_ctx_adoption_test.go` |
| 2026-10-12 | **采纳改按钩子返回的 ctx 是哪来的分；执行状态与环境的登记挂在 key 类型上**。交给钩子之前 `compose_v2.EnterHookCall` 放一个这次调用独有的标记：返回的 ctx 看得见它（派生的）就听钩子的——业务值、调用方可配的（环境、软取消）、取消与截止时间；看不见（外来的：别处缓存的、`Background`）就只借业务值，可配的与取消（含 `context.Cause`）取框架这次的；框架的身份与位置（含会话 ID）不论哪种都取框架的。外来 ctx 上的业务值照用、每个采纳点首次告警一次（带采纳点位置——全进程只打一次时第一个违约者之后的都不出声，也认不出是哪个钩子，审计实测）：框架分不出 span 与凭证，没加「按请求划分」的第三种标记——跨请求存 ctx 是钩子的 bug。上层包的 key 实现 `ExecutionStateKey` / `EnvironmentKey`，剥离与采纳都按它认，删掉 agent_v2 自己那份清单与采纳函数；工具中间件交给 next 的 ctx 同样采纳。曾经只按 key 分：外来 ctx 上别的请求的调用主体、编排钩子、checkpointer、store 照收——流式片段送到别人的连接、两个租户共用一个转交状态、长期记忆写进别人的 store、调用方取消了停不下来；派生 ctx 上钩子正当装的软取消反被当成执行状态丢掉；中间件为挂 span 沿用存下的 ctx 时工具里的提问每次续跑换 ID；更早两份清单时引擎的 `OnGraphStart` / `OnNodeStart` 不认 agent_v2 的 key（均实测，`err == nil`）。**对照**：没有哪家分辨回调返回的 ctx 的来源——eino 只重铺回调系统自己那一个 key，其余靠「返回派生 ctx」的约定；我们分来源是因为违约的代价是跨请求、跨租户的静默串号。代价：交出 ctx 的每一处都要先放标记（机械哨兵数「放标记次数 ≥ 采纳次数」）；钩子链内部仍不分来源（obs 手动装进链时要排第一；2026-10-22 起改为把业务钩子交给 `obs.AgentHooks(…)` 包在里面，见决策 7） | 哨兵 `TestHookCtxOrigin_*` / `TestAdoptHookCtx_ByOrigin` / `TestHookCtx_Orch*StaleCtx_*` / `TestHookCtx_ModelEndStreamStaleCtx_ChunksToOwnRun` / `TestExecutionStateKey_EngineDetachAndAdoptRecognizeAgentV2Keys` |
| 2026-10-22 | **observability 改成 OpenTelemetry**：只依赖 OTel API，属性与指标按 GenAI 语义约定，框架维度用 `crew.*`；结局用 `crew.outcome`（中断记 blocked、取消不算失败），取消只按 ctx 判 | `doc/v2/observability.md` + `observability-trace-spec.md` |
| 2026-10-22 | **observability 把业务钩子包在里面**（`AgentHooks(inner…)` / `OrchHooks(inner…)`），里面的钩子开始失败时当场收尾；结束钩子核对句柄归属。决策 7 | 哨兵 `observability/observability_test.go` 的 `TestInstall_BusinessAgentStartRejected` / `TestOrchHooks_InnerNodeGate` 等 |
| 2026-10-13 | **采纳收成一个入口 `compose_v2.CallHook`**（工具中间件用 `EnterHook`），放标记与采纳两步不再导出。原先各采纳点各写一遍「放标记 → 调钩子 → 采纳」，靠按源码位置数的哨兵对账，标记放在循环外、只放在一个分支上、交给钩子的是另一个变量这几种写法都查不出；收成一个入口之后要查的是「有没有绕过入口直接调钩子」与「交给钩子的是不是入口放了标记的那个 ctx」——第一版哨兵只查了前一半，闭包里交给钩子外面那个 ctx、取方法值再调、写在包级变量里的函数字面量都漏（审计实测，次日补上，并改为扫全仓）。参照单入口分发：回调由一个函数调起、调用点只写接住返回值。顺带压平逐个采纳时执行状态的查找深度（64 次采纳后查一次 ns 从 1.9µs 降到 10ns；可配项、业务值与取消的查找不压平，语义上要沿钩子那条链查）。业务的组合钩子里再套 CallHook 也认得出（标记带着外层那一个，2026-10-13 补）|

## 参考代码定位

| 协议 | crew-go 文件 | 关键行 |
|---|---|---|
| Hooks 8 method 接口 | `hook/hooks.go` | `type Hooks interface` |
| OrchestrationHooks 5 method | `compose_v2/hooks.go` | `type OrchestrationHooks interface` |
| ChainHooks (panic 协议 + ctx 链) | `hook/hooks.go` | `type ChainHooks` + `NewChainHooks` |
| 指标定义 | `observability/metrics.go` | `newInstruments` |
| `IsInterrupt(err)` 控制流判定 | `compose_v2/interrupt.go` + 7 file 调用点 | `grep "IsInterrupt(err); !isInt"` |
| observability 钩子 | `observability/agent_hooks.go` / `orch_hooks.go` / `wrap.go` | `agentHooks` / `orchHooks` / `wrappedHooks` + `wrappedOrchHooks` |
| EmitCompactionEvent (self-contained After) | `agent_v2/compact_event.go` | `CompactionAfterAction` struct |
| EmitFanoutEvent (N=1 short-circuit) | `agent_v2/fanout_event.go` | `EmitFanoutEvent` |
| panic→err 兜底 + callback 可见(决策6) | `agent_v2/agent_helpers.go` | `recoverToErr`(顶部 defer)+ `recoverPanicToErr`(OnAgentEnd defer 内, 接收 recover() 实参) |
| 结局与错误分类(决策6) | `observability/handle.go` | `classify` / `errorTypeOf` / `finish` |

## 跟现有 doc 的协作

- **入口/命名/hierarchy 决策** → `observability-eino.md` §1-7 + `observability-langgraph.md`
- **§8 实施前决策矩阵** → `observability-eino.md` §8(2026-05 Sub-5 实施**前**预判)
- **本文档** → Sub-5 实施**后**(跨 Round 2-8 audit)的实证决策,纠正 §8 部分预判(如"Handler 链顺序"实证为 ChainHooks append 序串行不变,不学 eino 逆序)
