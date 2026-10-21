# 主题：observability callback / hook 协议层决策

> **跟现有 2 篇 observability doc 的边界**：
> - `observability-langgraph.md` / `observability-eino.md`：Sub-5 实施**前**的预研——trace span 命名 / hierarchy / metrics 入口
> - **本文档**：Sub-5 落地后(跨 Round 2-8 audit) 的 **callback 协议层**决策记录——hook chain / Interrupt 路径 / stream wrap / 28 metric auto-fire vs 业务自填 (22 framework auto-fire + 6 业务自填, 见下 §"28 metric 分类")

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
| **framework auto-fire metric** | ✅ 22 auto-fire (Sub-5b/c/d/e/f + P0/P1) + 6 业务自填(见下 §"28 metric 分类") | ❌ 无内置 (业务自管) | ⚠️ LangSmith SaaS 内置自动 (绑 LangSmith) |

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
- Interrupt 数走 framework auto-fire 的 `MetricInterruptTotal`（P0 升级，OnAgentEnd 自动 fire status=blocked，不再业务自填）

### 副作用(by-design)
observability `MetricAgentRunTotal` Counter 在 Interrupt 路径 status="success" —— 跟业界一致。业务想区分 Interrupt 走单独 metric,非破坏 hook 协议。

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
**hook 链顺序敏感** —— 业务 hook 在 obs 前 panic 时 obs.OnAgentEnd 不跑 → MetricAgentRunTotal Counter 漏 fire。**推荐顺序**: obs.AgentHooks() 在最前(最早 fire start,最晚 fire end),业务 hook 后置。

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
- 当前(Sub-5b 简化): `observability.AgentHooks().OnModelEndStream` 不 wrap reader 仅 add event,等价于"透传"——业务想数 chunk 自填 `MetricStreamChunksTotal` Counter

## 关键决策 4：28 metric framework auto-fire vs 业务自填

### 分类(Sub-5 落地实证)

实际 `observability/metric.go` 定义 28 个 metric name 常量 (Sub-5b/c/d/e 落地 24 + Sub-5f LLMError + P0/P1 interrupt/resume/replan/route 扩 = 28). 22 framework auto-fire (业务接 hook 自动 fire) + 6 业务自填 (framework 拿不到 / 跨 Run 控制流). 详 grep `Metric[A-Z]` `observability/metric.go` 28 个常量定义.

| 类别 | metric 数 | 说明 |
|---|---|---|
| **framework auto-fire (业务接 obs.AgentHooks/OrchHooks 后自动)** | 22 | 见下表 |
| **业务自填**(framework 拿不到数据 / 跨 Run 控制流) | 6 | LLMCostUSD/LLMTTFT/LLMTPOT/LLMError/StreamChunks/ToolCallsPerRound |

(22 + 6 = 28。InterruptTotal/InterruptResumeDuration P0 已升级为 auto-fire（走 `WithEndError`/`WithResumePhase` ctx 协议），不再业务自填。**不存在"半自动"类** — 之前 doc 凑数写"半自动 4"是错的,已删。)

### 22 个 auto-fire metric 与触发 hook

| Metric | 触发 hook | Sub-5 子项 |
|---|---|---|
| `gen_ai.agent.run.total` Counter | OnAgentEnd | Sub-5b |
| `gen_ai.agent.run.duration` Histogram | OnAgentEnd | Sub-5b |
| `gen_ai.agent.rounds` Histogram | OnAgentEnd(info.Round, audit Round 2 后真值) | Sub-5c |
| `gen_ai.runs.inprogress` Gauge | OnAgentStart/End Inc/Dec | Sub-5b |
| `gen_ai.llm.call.duration` Histogram | OnModelStart→OnModelEnd | Sub-5c |
| `gen_ai.llm.tokens.total` Counter (label `gen_ai.usage.type=input/output/think/total/cached`) | OnModelEnd | Sub-5c |
| `gen_ai.tool.call.duration` Histogram | OnToolStart→OnToolEnd | Sub-5c |
| `gen_ai.tool.error.total` Counter | OnToolEnd (Result.Success=false) | Sub-5c |
| `gen_ai.node.execution.total` Counter | OnNodeStart (OrchHooks) | Sub-5c |
| `gen_ai.skill.activate.total` Counter | OnToolStart (in.Name==ActivateSkillToolName) | Sub-5f |
| `gen_ai.active.sessions` Gauge | sessionByID Inc + CleanupSession Dec | Sub-5f |
| `gen_ai.compact.total/duration/ratio` Counter+2Histogram | OnNodeEvent (CompactionEventType After) | Sub-5d |
| `gen_ai.cp.save.total` Counter / `gen_ai.cp.size.bytes` Histogram | OnNodeEvent (CpEventType) | Sub-5e |
| `gen_ai.fanout.branches` Histogram | OnNodeEvent (FanoutEventType, N>=2) | Sub-5f |
| `gen_ai.interrupt.total` Counter | OnAgentEnd (InterruptError, `WithEndError` ctx 协议) | P0 |
| `gen_ai.interrupt.resume.duration` Histogram | OnAgentStart (Resume, `WithResumePhase` ctx 协议) | P0 |
| `gen_ai.resumes.total` Counter (label `resume.action`) | OnAgentStart (Resume) | P0 |
| `gen_ai.plan.replans.total` Counter (label `plan.phase`) | PlanAgent replan | P1 |
| `gen_ai.route.decisions.total` Counter (label `route.target`) | RoutingAgent 路由 | P1 |

(上表把 `compact.{total,duration,ratio}` 合 1 行=3 个、`cp.{save.total,size.bytes}` 合 1 行=2 个,**展开共 22 个 distinct metric**)

### 关键设计:event-driven metric 路径
**Compact/Cp/Fanout 3 类 metric 走 OrchHooks.OnNodeEvent 路由**,业务**漏接 `obs.OrchHooks()` 时这些 metric 全漏 fire**。完整接入路径必须:
```go
agent := agent_v2.NewBasicAgent(
    agent_v2.WithHooks(obs.AgentHooks()),  // 8 method 业务级 hook
)
ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())  // 5 method 编排级 hook
ctx = compose_v2.WithSessionIDCtx(ctx, sess.RunID)     // 让 ActiveSessions 真分 N
events, _ := agent.Run(ctx, sess, input)
defer obs.CleanupSession(sess.RunID)                   // 防 sessions map leak
```

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
1. **编排错误 error.message 落空**(commit `2f19950`):7 编排(BasicAgent + Sequential/Loop/Parallel/Reflect/Routing/Handoff)非通信主体错误(MaxIterations / build graph 失败 / 子 agent 冒泡)不 fire OnError → endSpan 读 lastError miss → agent span status=error 但 error.message 空。修:`endSpan` 加 `fallbackErrType/fallbackErrMsg` 参数(优先级 **传入 > lastError > fallback**),OnAgentEnd 把顶层 retErr 经 `mapErrToStatus` 分类作 fallback 传入。GraphAgent/PlanAgent 已 fire `*_run_error` OnError 不受影响。
2. **框架自身 panic callback 看不到**(commit `e4f1f2b`):agent wrapper 内框架自身代码(buildComposeGraph / collectEvents 等 compose_v2.Run recover 之外的 wrapper panic)panic 时,OnAgentEnd defer(LIFO 先于顶部 recoverToErr 跑)看到 retErr=nil → trace 误报成功。修:9 编排 OnAgentEnd defer 闭包内、`ctxOut := WithEndError` 前加 `recoverPanicToErr("Xxx.OnAgentEnd", recover(), &retErr)`。

### Go recover 陷阱(实施关键)
`recover()` **只在 deferred function 内直接调用**生效。`defer func(){ recoverToErr(...) }()` 里 recoverToErr 内的 `recover()` **无效**(闭包调的 helper 不算"直接调用" —— 契约测试 `agent_v2/panic_recover_internal_test.go` + go run 实测坐实)。故 `recoverPanicToErr` **不自己 recover**,而接收闭包内**直接** recover() 的值作实参:`recoverPanicToErr(ctx, "X", recover(), &retErr)`(recover() 在闭包栈帧求值有效)。

### 与决策2 的 4 层 outer recover 关系(正交,勿混)
决策2 的 4 层 outer recover 是**防 crash**(hook/NodeFunc/tool panic 转 err 不掀执行树)。本决策的 `recoverPanicToErr` 是 **callback 可见性**(让 OnAgentEnd 看到框架自身 panic 的 err)—— **不增加防 crash 层**(框架自身 panic 本就被顶部 recoverToErr 兜不 crash),只让 callback 先看到。两者正交,故决策2 仍是"4 层 outer recover",不因本决策变 5 层。

### 触发面(诚实，R3 audit 修正早期"纯防御"误判)
gap (2) 主要由**框架自身 wrapper 代码**(buildComposeGraph / collectEvents / 预检 `cper.LoadNS` / `compose_v2.Get`)panic 暴露;**业务侧也有两个窄触发面**——业务自实现 `Checkpointer.LoadNS`(BasicAgent runGraph 预检 `basic_agent.go:440`,在 `compose_v2.Run` 之前、OnAgentEnd defer 之后,**runGraph 自身无 recover**)+ GraphAgent `inputWriter`/`outputReader`(业务必填回调 `graph_agent.go:335/354`,`compose_v2.Run` 前后调)panic 时,同样**直达 OnAgentEnd defer**。仅 model/tool/hook/NodeFunc/OrchHook panic 才走 graph 引擎(`compose_v2.Run` / `runNodeWithHooks`)或 runOne recover 转 err 的正常路径。所以**非纯防御** —— eino 实测踩此坑(三方表),业务自定义 backend / GraphAgent 回调 panic 是真实触发面。契约测试 `panic_recover_internal_test.go` 验证修法**模式**(recover 语义,因 BasicAgent 框架自身 panic 端到端难注入);业务触发面(panic 的 Checkpointer/inputWriter)可另加 e2e 测坐实;9 编排真实修改靠 build + 全回归 + diff review。

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
| 2026-05 | exporter goroutine panic **改 per-tick recover**(单次 export panic 只 drop 本次 + 记 log,**goroutine 继续下一 tick 不死**)— 学 **LangSmith SDK** `_tracing_thread_handle_batch` per-batch catch 韧性(单次 upload 失败/异常不杀 background thread)。**三方对照**:crew-go 自管 export goroutine(开箱即用,业务唯一上报路径,跟 eino 不同——eino 不自管 export 推业务/OTel SDK);OTel SDK 自管但 panic 后死不重启(脆弱);LangSmith SDK 自管 + per-batch catch 续命(韧性范例)。**正因自管 export 是业务唯一路径**(无 OTel SDK 兜底),crew-go 学 LangSmith 做 per-tick 韧性,而非 OTel SDK 的脆弱。**不做无脑重启 watchdog**(panic 是 exporter bug,重启会 panic loop;per-tick recover 已足够——单次失败隔离,下次继续) | Round 4 audit L3 + 三方调研(OTel SDK / eino / LangSmith)+ `exporter.go::flushOnce` per-tick recover + 测试 `TestExporter_FlushOnce_PerTickPanicRecover` |
| 2026-10-09 | **采纳钩子返回的 ctx 时，执行状态整份取框架自己的**（`compose_v2.AdoptHookCtx`：所在位置、session、任务代号、相位与计数、答案账本、转交收件人、历史写入器……；其余——业务值、环境、取消与截止时间——跟钩子。环境与取消这一半 10-12 改了，见下一行）。曾经各采纳点各铺回几样，钩子返回别处缓存的 ctx（想把 span 挂在一起的常见写法）时，漏铺的那几样静默出错：转交没人取、模型答复记不进会话历史（实测，`err == nil`）。**对照**：eino 用回调返回的 ctx 跑节点，之后只重铺回调系统自己那一个 key（`internal/callbacks/inject.go` 的 `ctxWithManager`），别的框架状态靠「返回派生 ctx」的约定——就是我们修之前的形状；LangChain / LangGraph 的回调不返回 ctx，结构上换不掉执行上下文。我们保留「钩子能往下游推值」，代价是框架的每个 ctx key 都要登记成执行状态或环境（机械测试兜底） | `b6d33bab`；哨兵 `agent_v2/hook_ctx_adoption_test.go` |
| 2026-10-12 | **采纳改按钩子返回的 ctx 是哪来的分；执行状态与环境的登记挂在 key 类型上**。交给钩子之前 `compose_v2.EnterHookCall` 放一个这次调用独有的标记：返回的 ctx 看得见它（派生的）就听钩子的——业务值、调用方可配的（环境、软取消）、取消与截止时间；看不见（外来的：别处缓存的、`Background`）就只借业务值，可配的与取消（含 `context.Cause`）取框架这次的；框架的身份与位置（含会话 ID）不论哪种都取框架的。外来 ctx 上的业务值照用、每个采纳点首次告警一次（带采纳点位置——全进程只打一次时第一个违约者之后的都不出声，也认不出是哪个钩子，审计实测）：框架分不出 span 与凭证，没加「按请求划分」的第三种标记——跨请求存 ctx 是钩子的 bug。上层包的 key 实现 `ExecutionStateKey` / `EnvironmentKey`，剥离与采纳都按它认，删掉 agent_v2 自己那份清单与采纳函数；工具中间件交给 next 的 ctx 同样采纳。曾经只按 key 分：外来 ctx 上别的请求的调用主体、编排钩子、checkpointer、store 照收——流式片段送到别人的连接、两个租户共用一个转交状态、长期记忆写进别人的 store、调用方取消了停不下来；派生 ctx 上钩子正当装的软取消反被当成执行状态丢掉；中间件为挂 span 沿用存下的 ctx 时工具里的提问每次续跑换 ID；更早两份清单时引擎的 `OnGraphStart` / `OnNodeStart` 不认 agent_v2 的 key（均实测，`err == nil`）。**对照**：没有哪家分辨回调返回的 ctx 的来源——eino 只重铺回调系统自己那一个 key，其余靠「返回派生 ctx」的约定；我们分来源是因为违约的代价是跨请求、跨租户的静默串号。代价：交出 ctx 的每一处都要先放标记（机械哨兵数「放标记次数 ≥ 采纳次数」）；钩子链内部仍不分来源（obs 手动装进链时要排第一） | 哨兵 `TestHookCtxOrigin_*` / `TestAdoptHookCtx_ByOrigin` / `TestHookCtx_Orch*StaleCtx_*` / `TestHookCtx_ModelEndStreamStaleCtx_ChunksToOwnRun` / `TestExecutionStateKey_EngineDetachAndAdoptRecognizeAgentV2Keys` |
| 2026-10-13 | **采纳收成一个入口 `compose_v2.CallHook`**（工具中间件用 `EnterHook`），放标记与采纳两步不再导出。原先各采纳点各写一遍「放标记 → 调钩子 → 采纳」，靠按源码位置数的哨兵对账，标记放在循环外、只放在一个分支上、交给钩子的是另一个变量这几种写法都查不出；收成一个入口之后要查的是「有没有绕过入口直接调钩子」与「交给钩子的是不是入口放了标记的那个 ctx」——第一版哨兵只查了前一半，闭包里交给钩子外面那个 ctx、取方法值再调、写在包级变量里的函数字面量都漏（审计实测，次日补上，并改为扫全仓）。参照单入口分发：回调由一个函数调起、调用点只写接住返回值。顺带压平逐个采纳时执行状态的查找深度（64 次采纳后查一次 ns 从 1.9µs 降到 10ns；可配项、业务值与取消的查找不压平，语义上要沿钩子那条链查）。业务的组合钩子里再套 CallHook 也认得出（标记带着外层那一个，2026-10-13 补）|

## 参考代码定位

| 协议 | crew-go 文件 | 关键行 |
|---|---|---|
| Hooks 8 method 接口 | `hook/hooks.go` | line 103 `type Hooks interface` |
| OrchestrationHooks 5 method | `compose_v2/hooks.go` | line 36 `type OrchestrationHooks interface` |
| ChainHooks (panic 协议 + ctx 链) | `hook/hooks.go` | line 153 `type ChainHooks` + line 161 `NewChainHooks` |
| Counter/Histogram/Gauge | `observability/metric.go` | line 161 / 230 / 329 |
| `IsInterrupt(err)` 控制流判定 | `compose_v2/interrupt.go` + 7 file 调用点 | `grep "IsInterrupt(err); !isInt"` |
| observability hook 转 metric | `observability/hooks.go` | `agentHooks` + `agentOrchAdapter` |
| EmitCompactionEvent (self-contained After) | `agent_v2/compact_event.go` | `CompactionAfterAction` struct |
| EmitFanoutEvent (N=1 short-circuit) | `agent_v2/fanout_event.go` | `EmitFanoutEvent` |
| panic→err 兜底 + callback 可见(决策6) | `agent_v2/agent_helpers.go` | `recoverToErr`(顶部 defer)+ `recoverPanicToErr`(OnAgentEnd defer 内, 接收 recover() 实参) |
| agent span error.message fallback(决策6) | `observability/observability.go` | `endSpan` 的 `fallbackErrType/fallbackErrMsg` 参数 + 优先级块(error.type/message strKV 在 893/894, sweep 路径 479/480) |

## 跟现有 doc 的协作

- **入口/命名/hierarchy 决策** → `observability-eino.md` §1-7 + `observability-langgraph.md`
- **§8 实施前决策矩阵** → `observability-eino.md` §8(2026-05 Sub-5 实施**前**预判)
- **本文档** → Sub-5 实施**后**(跨 Round 2-8 audit)的实证决策,纠正 §8 部分预判(如"Handler 链顺序"实证为 ChainHooks append 序串行不变,不学 eino 逆序)
