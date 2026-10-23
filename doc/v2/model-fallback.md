# 模型故障切换设计：FallbackChatModel

> **文档状态**：**已实现（2026-06 落地）**——见下方「§0 实现状态」（包位置 / API / 落地偏离设计处 / 验证）。§1–§11 为设计与三方对照背景，作为决策依据保留；其中标 **[设计]** 的内部伪代码以「§0 实现状态」+ 实际代码为准。
>
> **决策**：走 **SDK 内置路线**（model 层装饰器），不做侵入编排层的改动，也不强制业务依赖外部网关。理由见 §10。
>
> 设计依据：业内两层做法对照（SDK 内置 / 网关层）+ 本地三方源码深挖——LangChain `RunnableWithFallbacks`（`langchain_core/runnables/fallbacks.py`）、eino `ModelRetryConfig`（`eino/adk/retry_chatmodel.go`）。关键决策与对照见 §4 / §9 / §10。

---

## 0. 实现状态（2026-06 落地）

**已实现并验证**（P0–P3）：

| 阶段 | 落地 | 文件 |
|---|---|---|
| P0 结构化错误 | `model.APIError`（Provider/StatusCode/Body/Err/**RetryAfter**）+ openai client 5 处回填（2 处 HTTP 非 200 带 StatusCode；3 处网络/IO 错 StatusCode=0）| `model/error.go` / `model/openai/client.go` |
| P1+P2 装饰器 | `FallbackChatModel`：Generate + Stream（仅建流阶段切换）+ WithTools（复用 breaker 指针）+ per-member 重试 + per-member 断路器 + onSwitch + `DefaultFailoverPolicy` | `fallback/{fallback,policy,option}.go` |
| P3 集成验证 | 真 LLM 双场景冒烟：主端点故障 → ①同链接 qwen3.7-plus ②跨链接 deepseek，均自动切换应答 | `example/fallback/fallback_smoke` |

**包位置：`crew-go/fallback`（不是设计稿说的 `model` 包）**。原因：本装饰器要复用
`compose_v2` 的退避/断路器原语，而 `model` 是低层叶子（仅依赖 `schema`）——让 `model`
反向 import `compose_v2` 会反转分层，且 `compose_v2` 一旦 import `model`（分层本就允许）
即成环。故独立成包（import model + compose_v2，谁都不 import 它）。**公开 API 为
`fallback.New(...)`**（§7 中 `model.NewFallback` 是设计期命名，已改）。

**落地偏离设计稿处**（其余按设计）：
1. **不用 `compose_v2.RunWithRetry`**（设计 §5.1 原说复用）——它在业务谓词之前有一道编排层拥有的
   guard，按「成员 Run 重试」的语境演化，收敛过去会被那边的演化静默改变行为。仅复用 `BackoffDelay` /
   `SleepWithContext` 退避原语（真正可复用的部分）。重试谓词是本包固定的 `shouldRetryMember`：只重试会
   自己恢复的错误（5xx / 429 / 408 / 网络 / 不认识的错误），跟 `ShouldFailover`（换不换候选）是两个判断
   ——同一个错两边的答案可以相反，额度耗尽不该重试、该切换。曾经两者共用 `ShouldFailover`，额度耗尽先
   在同一个候选上重试、白等退避才换（2026-10-19 拆开，见 §7.3）。
2. **§11.7 取 first_error**：全候选失败抛**第一个**（主候选）错误，诊断价值最高。
3. **§11.5 取哨兵**：全员熔断返 `fallback.ErrAllMembersUnavailable`（`errors.Is` 可判）。
4. **prompt-too-long 不单列**：设计 §4.3 把 `IsPromptTooLongError` 单列一行判不切——实际
   上下文超限是 400（4xx），已被"4xx 不切"规则覆盖（交第 3 层 ReactiveCompact），
   `DefaultFailoverPolicy` 无需依赖 agent_v2 的 prompt-too-long 检测，分层更干净。provider 在响应里
   报的上下文超限（结束原因）是 `model.ErrContextWindowExceeded`，包在 StatusCode 为 0 的 APIError 里，
   按哨兵判不切（2026-10-19，此前被当成网络错误逐个重试再切换）。

**已知行为（commit 后审计确认，非 bug）**：断路器 `maxFailures` 是 best-effort 计数阈值，
**非硬上限**——并发 burst 下首个失败窗口内多个请求会在 trip 前同时通过 `Allow()`，主候选
overshoot 到 ~并发度才熔断（实测 N=200 并发 → 主候选被打 ~6 次）。对结果无影响（都正确兜底
切换），收费/限流主候选在该窗口多打几次。这是 `compose_v2.CircuitBreaker` 计数型原语的固有
特性（AutoCompact/ReactiveCompact 同样复用并接受）；精确截断需 breaker 层加 singleflight
（独立任务）。详见 `WithBreaker` godoc。onSwitch 回调并发触发，业务回调访问共享态需自行加锁。

**待办（设计 §11 仍开放）**：Stream 首-chunk peek 增强切换（需 StreamReader Copy 能力，未核实）。
（`member.tripped` metric 已实现见 §11.6；`agent_v2` 语法糖已决不加见 §11.3。）

---

## 1. 背景与术语

### 1.1 现状：crew-go 不支持模型故障切换

| 机制 | 位置 | 是否「故障切备用模型」 |
|---|---|---|
| `agent_v2.WithModel(m)` | `agent_v2/config.go:242` | ❌ 接受**单个** `model.ChatModel`，无备用列表 |
| `model.ChatModel` 接口 | `model/interface.go:10` | ❌ 单实现，`Generate` / `Stream` / `WithTools` 三方法，无 fallback 概念 |
| `compose_v2.RunWithRetry` / `RetryConfig` | `compose_v2/retry.go` | ⚠️ 只**重试同一个 model**；且只接在 **sub-agent.Run 粒度**——7 个编排（handoff/loop/plan/reflect/parallel/routing/sequential，如 `handoff_agent.go:500` / `loop_agent.go:385`）重试整个子 agent 一轮，**不是** model 调用粒度。BasicAgent `modelFn` 内的 LLM 调用本身**未包 retry**（`basic_agent.go` 中 `RunWithRetry` 0 命中），每轮 5xx/超时直接抛错终止 |
| Stream→Generate 降级 | `agent_v2/basic_agent.go`（`streamOrGenerate`）| ❌ 模型不支持流式时改调同一个 model 的 Generate，不换模型；建流报错原样交给上层 |
| ReactiveCompact retry | 第 3 层压缩 `agent_v2/reactive_compact.go` | ❌ 针对 `prompt-too-long` 压缩后重试，不是故障切换 |

**结论**：BasicAgent 遇到模型 API 故障（5xx / 超时 / 限流 / 网络错）→ 直接抛错，既不重试也不切备用。

### 1.2 术语：failover ≠ fallback（业内已固化）

业内（Portkey / futureagi 等）把这两个常被混用的词区分开：

| 词 | 触发 | 动作 |
|---|---|---|
| **failover**（故障转移）| transient 基础设施故障：5xx / 网络错 / 超时 | 换 **provider**（同一模型，不同接入点）|
| **fallback**（回退）| 语义失败：429 限流 / guardrail 拦截 / context 溢出 / 质量不达标 | 换 **model / 走缓存 / 改策略** |

**本文的命名约定**：`FallbackChatModel` 持有的「有序候选列表」**两者都能表达**——列表成员既可以是同模型的不同 provider 接入（failover），也可以是不同模型（fallback）。哪些错误触发「切到下一个」由可配置的 `FailoverPolicy`（§4）决定。沿用业内 `with_fallbacks`（LangChain）/ model fallbacks（Vercel）的命名习惯，**对外统称 fallback**，下文不再区分二者，只在 §4 错误分类处落到具体状态码。

### 1.3 业内两层做法（调研结论）

| 层 | 代表 | 形态 |
|---|---|---|
| **SDK / 库内置**（代码里声明候选列表）| LangChain `primary.with_fallbacks([b1, b2])`；Vercel AI SDK `gateway.models: [...]`；eino 只有 retry 无 fallback | 静态、自包含、随应用部署；缺点：主 provider 的 caching/logging/rate-limit 不自动跟到备用路径 |
| **网关层**（infra，零业务代码）| LiteLLM（`fallbacks` + cooldown + Redis 跨实例 track）；Portkey（fallback 链 + 断路器）；OpenRouter（`models` 优先级数组）；Bifrost / Vercel AI Gateway | 把 fallback 抽到统一网络跳，与 auth/routing/guardrail/caching 同层；2025–2026 主流趋势 |

crew-go 是被业务集成的 Go 框架，本设计做 **SDK 内置层**；业务若已有网关，可直接把 model 的 baseURL 指向网关、不用本装饰器（两条路并存，§10）。

---

## 2. 设计目标 / 非目标

**目标**：
1. 一个 model 故障时，按预设优先级自动切到下一个候选，对调用方透明
2. 对全部 9 个编排（BasicAgent / Sequential / Parallel / Loop / Routing / Reflect / Handoff / Plan / Graph）**零侵入**——不改任何编排代码
3. 复用既有原语：`compose_v2.RunWithRetry`（重试）+ `compose_v2.CircuitBreaker`（断路器自愈），不重造
4. 错误分类可配置：区分「切了有用」（5xx/超时/网络/429）与「切了没用」（4xx 业务错 / 控制流信号）

**非目标**（第一版不做，列入 §11 待定）：
- 负载均衡 / 加权路由（业内网关的活，本装饰器只做「故障切换」不做「分流」）
- 跨进程共享断路器状态（LiteLLM 用 Redis；本装饰器是 per-process 内存态，对齐 eino/LangGraph 不持久化 breaker 的取舍——见 `autocompact.go:85` 既有决策）
- 质量不达标触发 fallback（需要业务定义「质量」，超出框架职责）
- 流式中途失败的回切（技术上不可行，§6）

---

## 3. 核心设计：装饰器，对编排零侵入

### 3.1 为什么装饰器能零侵入

所有编排最终的 LLM 调用都经 `model.ChatModel` 接口注入：

```
agent_v2.WithModel(m)  →  Config.Model  →  BasicAgent.streamOrGenerate(llm, ...)  →  llm.Stream / llm.Generate
                                          →  toolsFn: llm.WithTools(tools)  (basic_agent.go:1138)
LLMPlanner: m.WithTools(tools)  (llm_planner.go:113)
```

`FallbackChatModel` 本身**实现 `model.ChatModel`**（满足 `Generate` / `Stream` / `WithTools` 三方法），注入哪个 `model.ChatModel` 字段框架都无感：

```go
// 业务用法——编排代码完全不变
m := fallback.New([]model.ChatModel{primaryQwen, backupDeepSeek, backupOpenAI})
ag := agent_v2.NewBasicAgent(agent_v2.WithModel(m), /* ... */)
```

因此它不仅能用于主 model，也能用于任何 `model.ChatModel` 注入点：AutoCompact 摘要 model（`autocompact.go:35`）、Skill 的 `SkillModelResolver`（`config.go:198`）、`LLMPlanner` 的 model——全部自动获得故障切换能力，无需各自改造。

### 3.2 内部结构（草案）

```go
// [设计] FallbackChatModel 按优先级顺序在多个候选 model 间做故障切换。
// 实现 model.ChatModel——对调用方与全部编排透明。
type FallbackChatModel struct {
    members  []*fallbackMember // 有序：members[0] 为主，其余按序备用
    policy   FailoverPolicy    // 哪些错误触发切到下一个
    retry    compose_v2.RetryConfig // 每个 member 切换前的重试策略（零值=不重试）
    onSwitch func(ctx context.Context, fromIdx, toIdx int, err error) // 切换观测回调（可选）
}

// [设计] fallbackMember 包装一个候选 model 及其独立断路器。
type fallbackMember struct {
    model   model.ChatModel
    breaker *compose_v2.CircuitBreaker // 每个候选独立——A 挂不影响 B 的计数
}
```

**为什么 breaker per-member 而非全局**：对齐 `autocompact.go` 既有教训（ISSUE-72：跨 session 共享单例 breaker 是 bug）。某个候选连续失败应只 trip 它自己，不能因为主模型挂了把备用也一起禁用。

---

## 4. 错误分类（核心难点）

### 4.1 现状障碍：model 层错误非结构化

`model/openai/client.go` 当前把 HTTP 状态码拼进字符串：

```go
// 改造前——状态码只拼在 message 文本里（现已回填 model.APIError，见 §4.2 / §0）
return nil, fmt.Errorf("openai: HTTP %d: %s", httpResp.StatusCode, body)
```

`FallbackChatModel` 要按状态码分类（429 限流 vs 503 故障 vs 400 参数错，处理完全不同），但现在只能 regex 抠字符串——脆弱，重蹈 `reactive_compact.go` 用正则解析 prompt-too-long 的覆辙（CLAUDE.md「error 字符串不匹配 regex」炸弹 3）。`model` 包目前**无任何结构化错误类型 / sentinel**（grep 确认）。

### 4.2 前置根因修复：给 model 层加结构化错误

**[设计]** 在 `model` 包定义结构化错误，所有 provider（openai，及复用它的 qwen/deepseek——见 `qwen/client.go:29`、`deepseek/client.go:32`）返回它：

```go
// [设计] model/error.go
// APIError 是 provider HTTP 调用的结构化错误，让上层（如 FallbackChatModel）
// 能按 StatusCode 分类而不必解析错误字符串。
type APIError struct {
    Provider   string // "openai" / "qwen" / "deepseek"
    StatusCode int    // HTTP 状态码；0 表示非 HTTP 错误（网络中断 / 解析失败 / 超时）
    Body       string // 响应体（截断）
    Err        error  // 底层错误（网络层 / 解析层），可为 nil
}

func (e *APIError) Error() string { /* ... */ }
func (e *APIError) Unwrap() error { return e.Err }
```

openai client 改为 `return nil, &model.APIError{Provider: c.system, StatusCode: httpResp.StatusCode, Body: string(body)}`。这是根因修复（符合「拒绝短期方案」），收益不止 fallback：ReactiveCompact、可观测性都能受益于结构化状态码。

### 4.3 FailoverPolicy：可配置的「该不该切」

```go
// [设计] FailoverPolicy 判断一个错误是否应触发切到下一个候选 model。
// 接口定义在使用方（fallback 包），业务可自定义并经 WithFailoverPolicy 注入。
type FailoverPolicy interface {
    ShouldFailover(err error) bool
}
```

> **控制流信号由框架层 guard 无条件豁免，不经业务 policy**：`New()` 构造期把业务
> policy 包进 `guardedPolicy`——`ShouldFailover` 先判 `compose_v2.IsRetryFutile(ctx, err)`
> （InterruptError / 陈旧写回 / 软取消，以及**调用方的** ctx 已死）返 false，再委托业务分类。业务自定义 policy 只需关心
> 「哪些真实 API 错误换候选有用」，即使漏写控制流豁免也不会吞中断（见 §4.4）。

**默认实现 `DefaultFailoverPolicy` 的判定表**（`InterruptError` / ctx 两行是与 guard
层一致的冗余确认——让 `DefaultFailoverPolicy` 作为导出分类器被直接调用时答案也正确）：

| 错误 | ShouldFailover | 理由 |
|---|---|---|
| `*compose_v2.InterruptError` | ❌ false | HITL 控制流信号，切模型会吞掉中断（`IsRetryFutile`，guard 层已兜底） |
| 调用方 ctx 已取消 / 超时（`ctx.Err() != nil`） | ❌ false | 切了也立刻再失败（同上，guard 层已兜底）。⚠️ 只看调用方的 ctx：错误链里的 `DeadlineExceeded` 若来自后端自己的超时（如 `http.Client{Timeout}`）、调用方 ctx 仍活，照常切换（`44b6ccb4`） |
| 上下文超限：HTTP 400 报的走下面的 4xx 规则；响应里报的（`model.ErrContextWindowExceeded`）按哨兵判 | ❌ false | 该走第 3 层 ReactiveCompact 压缩，不是换模型的活。响应里报的包在 StatusCode 为 0 的 APIError 里，不先判哨兵就会被当成网络错误。候选的上下文窗口不同（如 32k → 128k）、想切到大窗口时，自定义 policy 用 `errors.Is` 认它（2026-10-19 之前智谱经结束原因报的超限会先重试 3 次再切，现在不切） |
| 内容过滤：响应里报的（`model.ErrContentFiltered`） | ❌ false | 跟 HTTP 400 报的内容违规同一个答案；要换候选试试，自定义 policy 用 `errors.Is` 认它 |
| `*model.APIError` 4xx（除 408/429）| ❌ false | 请求本身有问题（400 参数错 / 401 鉴权 / 404 模型不存在），换模型同样失败 |
| `*model.APIError` 408 / 429 | ✅ true | 408 请求超时、429 限流——换 provider 有用；429 配合断路器 + cooldown。注意 408 是**结构化 4xx**，不会落到下方「其他未知 error」兜底，必须在此显式列入「该切」|
| `*model.APIError` 5xx | ✅ true | provider 故障 |
| `StatusCode == 0`（网络/超时/解析）| ✅ true | 接入点不可达 |
| 其他未知 error | ✅ true | 保守切换（最后一个候选失败仍会把原错误抛出，不会吞错）|

**与 `IsRetryable` 的关系**：`IsRetryable`（retry 层）问「重试同一个 model 有没有用」；`ShouldFailover`（fallback 层）问「换下一个 model 有没有用」。两者正交——429 是「retry 同模型大概率还限流，但换 model 有用」的典型：`IsRetryable=true`（退避后可重试）且 `ShouldFailover=true`（退避耗尽后切换）。

反过来 `*InterruptError` / `context.Canceled` / `DeadlineExceeded` 三类**既不重试也不切换**——见下 §4.4，它们由框架 guard 层统一豁免，不依赖业务 policy 或 retry 策略。

### 4.4 控制流豁免：框架层 guard（不可被业务 policy 覆盖）

「interrupt / ctx 取消是控制流、不是失败」是**框架内部协议知识**——业务写自定义
`FailoverPolicy` 时想的是「哪些 HTTP 错误该切」，不该被要求知道 `InterruptError` 的
存在。因此这道豁免放在**框架层**，不交给可替换的业务 policy 维护：

- **判据收在 compose_v2**：`compose_v2.IsRetryFutile(ctx, err)` 判「再做一次有没有可能有用」——三个框架自铸哨兵
  （`InterruptError` / `ErrCheckpointStale` / `ErrSoftCanceled`）按 error 判，ctx 那档问调用方的 `ctx.Err()`，不问错误链
  （后端自己的超时也会产 `DeadlineExceeded`）。fallback 层与框架重试守卫用它；retry 层的默认谓词 `IsRetryable`
  不再按错误链上的 ctx 错判（调用方的 ctx 由守卫判，2026-10-23 改）。
- **构造期包装**：`fallback.New()` 把业务 policy 包进 `guardedPolicy`——所有决策点
  （`tryMembers` 切换 + `callMemberWithRetry` 重试 + 未来新增）自动豁免，不依赖逐点
  前置检查的纪律。
- **业务 policy 无法覆盖**：即使业务经 `WithFailoverPolicy` 注入一个对 interrupt 返
  `true` 的 policy，guard 层仍先拦下——防止「漏写豁免 → interrupt 被误判该切 → 换候选
  重跑成功 → HITL gate 被绕过、调用方拿到 `err=nil` 完全无感、断路器被误计」这条静默
  故障链。

> **为什么 fallback 内部重试不复用 `RunWithRetry`**：`callMemberWithRetry` 用本包的
> `shouldRetryMember` 判「在这个候选上再试有没有用」，跟 `ShouldFailover`（换候选有没有用）各答各的。
> 控制流豁免**不能依赖 retry 层**：切换那一侧由 guard 层在业务 policy 之前兜底，这正是把 guard 放构造期
> 包装而非「retry 层已拦」的原因（`shouldRetryMember` 自己也先问 `compose_v2.IsRetryFutile`）。

---

## 5. 三层防护：retry × fallback × circuit breaker

业内共识（Portkey）：三件套各司其职，不互相替代。

```
retry        ── 同一个 model 重试，扛瞬时抖动（复用 compose_v2.RunWithRetry）
  ↓ 重试耗尽仍失败 且 ShouldFailover(err)
fallback     ── 切到下一个候选 model（本装饰器核心）
  ↓ 某候选连续失败达阈值
circuit breaker ── trip 后 Allow()=false，直接跳过该候选试下一个；cooldown 后半开探测自愈
```

### 5.1 复用既有原语

| 原语 | 来源 | 在本设计的角色 |
|---|---|---|
| `compose_v2.RunWithRetry[T]` | `compose_v2/retry.go:131` | 包裹**单个候选**的 `Generate` 调用，切换前先重试 N 次 |
| `compose_v2.CircuitBreaker` | `compose_v2/circuit_breaker.go:41` | 每候选一个；`Allow()` 决定是否跳过，`RecordFailure/Success` 记账，**自带 cooldown 半开自愈**（ISSUE-65~68 已修「trip 后永久禁用」bug，默认 5 分钟探测）|

断路器的 cooldown 自愈正是业内强调的「circuit breaker 防止一次 quota burst 永久打死模型」——crew-go 既有实现已满足，直接 `NewCircuitBreakerWithCooldown(maxFailures, cooldown)` 复用。

### 5.2 Generate 切换流程（伪代码）

```go
// [设计] 简化逻辑——略去 onSwitch 回调与 opts 透传
func (f *FallbackChatModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
    var firstErr error
    for i, m := range f.members {
        if !m.breaker.Allow() {        // 该候选在 cooldown 内，跳过
            continue
        }
        out, err := compose_v2.RunWithRetry(ctx, f.retry, func(ctx context.Context) (*schema.Message, error) {
            return m.model.Generate(ctx, msgs, opts...)
        })
        if err == nil {
            m.breaker.RecordSuccess()
            return out, nil
        }
        if !f.policy.ShouldFailover(err) {  // f.policy 已被 guardedPolicy 包装：控制流(§4.4) / 4xx 立即抛，不切
            return nil, err
        }
        m.breaker.RecordFailure()
        if firstErr == nil {                // 记录首个候选错误（诊断价值最高），继续试下一个
            firstErr = err
        }
        if i+1 < len(f.members) && f.onSwitch != nil {
            f.onSwitch(ctx, i, i+1, err)
        }
    }
    // 全部候选失败（或全在 cooldown）——抛错误，不吞。
    // 错误返回策略：取 first_error（首个候选的真实错误，主候选诊断价值最高——§11.7 已决，
    // 对齐 LangChain fallbacks.py:215）；全员 cooldown 未产生任何 err 时抛哨兵。
    if firstErr == nil {
        firstErr = ErrAllMembersUnavailable  // [设计] 全员 cooldown 哨兵；该分支语义待 §11.5 决议
    }
    return nil, firstErr
}
```

**遍历顺序对齐 LangChain**（`fallbacks.py:189` `for runnable in self.runnables`）：按序试、全失败抛出真实错误而非吞掉。全失败取 **first_error**（首个候选的原始错误，保留主 model 诊断价值——§11.7 已决，对齐 LangChain `fallbacks.py:215`）。

---

## 6. Stream 的硬约束

**流式比非流式难**：一旦第一个 chunk 已经 `send` 给下游消费者，再失败就**无法干净回切**——下游已经收到了半截输出。这是 LangChain `with_fallbacks` 同样存在的限制。

**第一版策略（保守，无歧义）**：只在**建流阶段**切换——即 `m.model.Stream(ctx, ...)` 同步返回 error 时（如 `client.go:141` 的 HTTP 非 200 在拿到首 chunk 前就返回了），按 §5 流程切下一个候选。一旦 `Stream` 成功返回 reader、开始投递 chunk，就**不再切换**，流中途错误照常向下游传递。

**增强项（列入 §11 待定）**，两种思路别混为一谈：
- **eino 式全流预消费**：`retry_chatmodel.go:252` 用 `stream.Copy(2)` 复制一份，`consumeStreamForError`（`:156`）**消费到 EOF** 确认整条流无误后才返回另一份。代价大——长流要等 buffer 完才返回，基本**牺牲流式实时性**，不适合 BasicAgent 的 chunk 实时投递。**这是把「检错窗口」拉到整条流，不是「首 chunk 落地前」。**
- **首 chunk peek（更轻）**：只预读第一个 chunk 检错，把切换窗口从「建流阶段」扩到「首 chunk 落地前」。**不需要**给 `schema.StreamReader` 加 `Copy`——它定义在 `schema/stream.go`（不是 alias），只有 Recv / Close / Done，用这三个写一个转发 wrapper 就能 peek（2026-09-18 核验，探针 `-race` 通过；§11.1）。

第一版两者都不做，只在建流阶段（`Stream()` 同步返回 error）切换。

> `streamOrGenerate`（`agent_v2/basic_agent.go`）只在模型**不支持流式**时改调 Generate：Stream 返回 `model.ErrStreamUnsupported`，或返回 nil reader 且没有错误。建流报错原样返回，由本装饰器切换、由重试层重试。曾经任何错误都改调 Generate，跟本装饰器叠在一起：所有候选建流都失败之后，整条候选链会经 Generate 再走一遍。本装饰器的 Stream 遇到不支持流式的候选，就地用这个候选的 Generate、结果包成只有一项的流：轮到哪个候选就用哪个，顺序不倒退，也不记断路器失败。把 `ErrStreamUnsupported` 交回调用方的话，调用方改调 Generate 会从第一个候选重新走——前面刚失败的候选立刻又被打一次（不看 Retry-After，断路器一次调用记两次失败）。BasicAgent 对只有一个片段的流整条采用、不做增量拼接，拿到的跟直接调这个候选的 Generate 一样（拼接会丢消息 ID、结束原因等字段，还把同一 ID 的两个工具调用并成一个；`OnModelEndStream` 钩子改了片段数时照常拼接）。候选的 Generate 返回 nil 消息按候选故障处理，Generate 与 Stream 两条路一样。

---

## 7. 公开 API 草案

```go
// 全部位于 crew-go/fallback 包（已实现）

// New 构造故障切换装饰器。models 按优先级排序——models[0] 为主，其余按序备用。
// 至少一个；空列表 / 含 nil model（含 typed-nil）panic（fail-fast，对齐框架 nil 防御纪律）。
func New(models []model.ChatModel, opts ...Option) *FallbackChatModel

type Option func(*config)

// WithFailoverPolicy 自定义错误分类；默认 DefaultFailoverPolicy（§4.3）。
func WithFailoverPolicy(p FailoverPolicy) Option

// WithPerModelRetry 设每个候选切换前的重试策略；零值（默认）= 不重试，直接切。
// 只重试会自己恢复的错误（5xx / 429 / 408 / 网络）；计费类额度用完、其余 4xx、声明了 compose_v2.NonRetryable
// 的错误不重试；Retry-After 超过 60 秒时先推迟这个候选、去试别的，别的都没成时回来；等完已过调用方期限的不等（§7.3）。
// ⚠️ 收的是 fallback 自己的 PerModelRetry，**不是** compose_v2.RetryConfig——后者带一个
// 判据字段（ShouldRetry）。曾经复用过那个类型，实测事故：业务照它的文档传了谓词说「别重试」，
// 谓词一次都没被咨询、副作用重放三次，而两边都不报错。拆开类型才能让人**踩不到**（参数表里根本
// 没有判据字段，重试哪些错是本包固定的分类）。
func WithPerModelRetry(r PerModelRetry) Option

// ⚠️ **填数之前先看乘积**：最坏总请求 = (1+编排 MaxRetries) × 候选数 × (1+PerModelRetry
// .MaxRetries)。实测 3 候选 + 2 内层重试 + 编排 SimpleRetry(3) = **27 次真实 LLM 请求**。
// 而断路器压不住它——maxFailures 数的是 **sweep 不是请求**，默认配置下该候选跳闸前已经
// 发了 9 次。三个数字分属三个地方，乘起来没有任何一处会告诉你。详见两个 option 的 godoc。

// PerModelRetry 只有两个字段——没有判据字段是有意的（见上）。
type PerModelRetry struct {
    MaxRetries int                      // 0 = 不重试，失败直接切下一个候选
    Backoff    compose_v2.BackoffPolicy // 纯退避参数，不含判据那半
}

// ⚠️ **服务端明示 Retry-After 时，Backoff 这组参数不生效**——见下方小节。

// WithBreaker 设每候选断路器参数；默认 maxFailures=3、cooldown=5min（复用既有默认）。
func WithBreaker(maxFailures int, cooldown time.Duration) Option

// WithFallbackHook 注册切换观测回调——每次从一个候选换到另一个候选去请求时触发（toIdx 通常是 fromIdx+1，
// 跳过熔断中的、或回到之前推迟的候选时不是）。
func WithFallbackHook(fn func(ctx context.Context, fromIdx, toIdx int, err error)) Option
```

**典型用法**：

```go
// 主 qwen，挂了切 deepseek，再挂切 openai；每个切换前退避重试 2 次
m := fallback.New(
    []model.ChatModel{primaryQwen, backupDeepSeek, backupOpenAI},
    fallback.WithPerModelRetry(fallback.PerModelRetry{
        MaxRetries: 2,
        Backoff:    compose_v2.BackoffPolicy{InitialDelay: 200 * time.Millisecond, MaxDelay: 5 * time.Second},
    }),
    fallback.WithBreaker(3, 5*time.Minute),
    fallback.WithFallbackHook(func(ctx context.Context, from, to int, err error) {
        slog.WarnContext(ctx, "model failover", "from", from, "to", to, "err", err)
    }),
)
ag := agent_v2.NewBasicAgent(agent_v2.WithModel(m), /* ... */)
```

### 7.1 WithTools 的传播（易漏点）

`WithTools` 返回**新的** `model.ChatModel`（`interface.go:19`）。FallbackChatModel 的 `WithTools` 必须**对每个候选都调一次**、再用绑定后的候选重组一个新 FallbackChatModel：

```go
// [设计]
func (f *FallbackChatModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
    bound := make([]*fallbackMember, len(f.members))
    for i, m := range f.members {
        bm, err := m.model.WithTools(tools)
        if err != nil {
            return nil, fmt.Errorf("fallback member %d WithTools: %w", i, err)
        }
        // 复用同一个 breaker 指针——绑定工具不应重置故障计数
        bound[i] = &fallbackMember{model: bm, breaker: m.breaker}
    }
    return &FallbackChatModel{members: bound, policy: f.policy, retry: f.retry, onSwitch: f.onSwitch}, nil
}
```

**关键**：绑定后的新实例必须**复用同一批 breaker 指针**——否则 `basic_agent.go:1138` 每轮 `WithTools` 都会得到一批全新断路器，故障计数永远清零，断路器形同虚设。

### 7.2 opts 透传

`Generate` / `Stream` 收到的 `opts ...model.Option`（`model.Options`：Temperature/MaxTokens/TopP/Stop/Model/Tools/ToolChoice，见 `model/option.go`）**原样透传**给当前候选——装饰器不解析、不改写。

### 7.3 服务端明示 `Retry-After` 时以它为准

限流（429）和部分 503 下，服务端会在 `Retry-After` 响应头里**明说**等多久。这个值是权威的，而我们算的指数退避是猜的——猜早了再吃一次限流（部分 provider 还会因此延长封禁），猜晚了白等。

所以 per-member 重试在睡之前先问一次：拿到正值就**整个替换**掉算出来的退避（`fallback.go` 的重试循环）。同一取舍也在 `compose_v2.RunWithRetry` 里（编排层重试），两处各接各的。

**要等很久的候选先推迟，别的都没成时回来**（2026-10-19）：Retry-After 超过 60 秒（跟 OpenAI 的 Go SDK 只认一分钟
以内、pi-ai 超过 60 秒直接报错一致）、且切换判据允许换候选时，给这个候选设一道共享的门（2026-10-20），先推迟它
（记下最早能再请求的时刻和还剩几次重试）、去试别的候选。这不推翻服务端的指令——T 之前没有调用再请求它。挑候选只有一条规则：**每次挑此刻能请求的
（没试过的、或推迟到期的）里排序最靠前的，都要等时等最早能请求的那个**。要等的时间（Retry-After 或退避）等完已经
到了调用方的期限时不等：这次的错误交给切换判据，换下一个候选或返回（于是推迟的候选回来时一定等得完）。切换判据
不允许换、又等得完时照旧等。每个候选的请求数仍不超过 1 + MaxRetries。

推迟改变了「先等这个候选、再试下一个」的次序，下面四条保证它不比原地等更差（`fallback.go` 的 `tryMembers`）：

1. **服务端说「T 之前别来」，对所有调用一视同仁**：任何一次调用收到超过 60 秒的 Retry-After（且切换判据允许换候选）
   时，给这个候选设一道共享的门（`retryGate`，`WithTools` 派生的模型跟断路器一样共用），T 之前任何调用都不请求它。
   碰到它时断路器闭合就当推迟（先试别的，都没成且期限够时在 T 回来）；断路器已经断开（熔断中，含半开）就当熔断跳过
   ——探测名额在设门的那个调用手里，原地等的话别的调用同样拿不到名额、当场跳过；等不到 T 的也跳过。这次调用没请求过
   它时，它的错误是「门还要关多久」（Retry-After 是剩余时间，不是设门时的原值；错误链上取得到设门的那个 429），请求
   过的以自己拿到的错为准。被门挡住、改去请求别的候选，照常发切换回调与 failover 事件。**门不是失败**：不记断路器
   （候选是好的，只是忙；这次请求占着半开的探测名额时放掉）。**门以服务端说的 T 为准**（2026-10-20 定）：几次设门
   取最晚的 T，T 之前不因为别的信号提前开——门关着时没有新请求发出去，期间能看到的成功只可能来自门设下之前在途、
   限流生效前就被放行的请求，说明不了候选恢复了；并发的几个 429 给出不同的 T 往往对应不同的限额，最晚的那个才是
   「T 之前都会被拒」的界。代价是服务端给的 T 偏保守时这段时间用不上它。另一个选项（看到成功就开门、Retry-After 以
   后到的为准）没采用：在途请求的成功会把门打开、紧接着的请求再吃 429，门来回开关，还要给等门的调用加唤醒机制。
2. **真失败当场记、一次调用里一个候选至多记一次**：候选的最终失败（不是设门的那种）在这次调用不再用它时记。调用方在
   原地等的途中取消，不算这个候选的失败、不记。
3. **回到推迟的候选是这次尝试的继续，不再经过断路器**：跟原地重试一样，等完服务端要求的时间就请求，不因为别的请求
   在此期间把它熔断而作罢。回来那次没有占探测名额，报「换候选没用」时也不放名额。回来之前先看门：门被别的调用推后了
   就跟着推后。只被门挡过、这次调用还没请求过的候选不算继续，门开之后照常经过断路器——不然它因真失败熔断时，T 一到
   所有被门挡过的调用会一起绕过断路器去请求它。
4. **候选 i 报「换候选没用」（切换判据为假、又不是控制流）时，排在 i 后面的不再试**（没试过的、推迟着的都算），**排在
   i 前面推迟着的照样回去**；最终失败时返回排得最靠前的那个「换候选没用」的错——跟原地等一样：原地等的话先等前面的
   候选，它成功就成功，失败了才轮到后面的候选、碰到这个错就返回。

切换回调与 failover 事件在真正去请求下一个候选时发，回到推迟的候选也算一次（to 可以小于 from）；一次调用里的
failover 事件数因此不再以「候选数 − 1」为上限（每个候选最多请求 1 + MaxRetries 次），按次数告警的看板会更敏感。

这条规则之前的两版都是审计实测推翻的：照 Retry-After 干等（429 + 10 分钟，调用方 2 秒的超时先到，第二个候选
一次都没被调用）；只在超过 60 秒、且后面有断路器此刻放行的候选时换、换走就不回来——后面的候选失败时调用方给的
时间明明够、第一个候选 61 秒后就能用，结果立刻失败；最后一个候选要等 10 分钟时去等它而不是回到 61 秒后就能用的
第一个；Retry-After 5 秒对 300 毫秒的超时照样干等；切换判据说不换时既不等也不换；判断「后面能换」与真去换之间
探测名额被并发请求占掉时同样既不等也不换。第三版（推迟时不记失败、调用结束时补记，回来时重新问断路器）又被推翻：
这次调用跑完之前别的请求照样一个个打到它，15 个并发请求全部打到半开的候选；等满 Retry-After 之后被别的请求记的
失败挡住、一次没请求就失败；推迟期间备用候选报了 401，调用直接以它结束、100 毫秒后就能用的第一个候选没被回去
请求。第四版（推迟当场记失败）也被推翻：并发的调用一多，候选就被这些推迟熔断，之后到达的调用在断路器那里直接跳过它、
不推迟也不回来，20 个并发调用只成功 3 个；推迟期间候选报「换候选没用」时不看先后，会去等排在它后面的推迟候选，返回的
也是时间上第一个「换候选没用」的错（上下文超限被换成后面候选的 401）。这条规则同一处被审出第三个反例时停下来重审
规则本身：根因是把「服务端说 T 之前别来」记成了断路器失败——前者是「忙，什么时候好已知」，后者是「坏了，过一阵再
探」，前几版都在两者之间挪来挪去。

```go
// [实现] fallback per-member 重试循环（callMemberWithRetry）
delay := compose_v2.BackoffDelay(retry.Backoff, attempt+1)
ra := compose_v2.RetryAfterFromError(err)
if ra > 0 {
    delay = ra
}
gated = ra > memberRetryAfterLimit && mayDefer(err)
if gated {
    mem.gate.set(time.Now().Add(ra), err) // T 之前所有调用都不请求它
}
if attempt == maxRetries || exceedsDeadline(ctx, delay) {
    return out, err, nil, gated // 重试用完 / 等完也到期了：不等，交给切换判据
}
if gated {
    return out, err, &deferral{readyAt: time.Now().Add(delay), attempt: attempt + 1}, true // 先推迟，tryMembers 回来接着重试
}
```

**取值链路**：`openai` client 在两处 HTTP 错误路径（`Generate` + `Stream`）把响应头解析进 `model.APIError.RetryAfter`；重试层经 `compose_v2.RetryAfterProvider` 接口沿 `Unwrap` 链取首个正值（遍历序同 `errors.As`，多错误树按成员序）。**接口定义在使用方**——retry 层不 import `model`，业务自定义 error 实现同名方法即可接管。

⚠️ **「首个正值」不是「首个实现者」**：链上某一层实现了接口但返 0（业务壳在「我没意见」时的自然写法）时**继续往下找**，不把里层服务端明说的值吞掉。这里不能写成一次 `errors.As` 加一个后置的 `> 0` 判断——那个函数命中第一个可赋值的错误就停了，后置过滤不会让它继续搜索，于是业务包一层就静默失效，且 `errors.Join` 上的取值随成员顺序翻。

⚠️ **delta-seconds 形式有整数溢出带**：`time.Duration(secs) * time.Second` 在 int64 上乘 1e9，超过约 292 年就绕回——`Retry-After: 20000000000` 会绕成**正的约 49 年**并被原样采纳睡下去。解析侧设了上限（`maxRetryAfterSeconds`），超限当「没给」。那道上限挡的是溢出**不是**「等太久」的策略：`86400`（日配额重置）照旧返 24h。

解析覆盖 RFC 9110 的两种合法形式（秒数 / HTTP-date）；**给 0、给负数、给已过期的时间、给解析不了的串，一律当"没给"回落常规退避**，不会产生负的或零等待。

⚠️ **有意不受 `MaxDelay` 约束**。那个字段管的是指数退避别失控增长，不是用来推翻服务端指令——服务端说等 60s 而你把它压到 `MaxDelay=5s`，结果就是再吃一次限流。想给"最多等多久"兜底就设 **ctx deadline**（`SleepWithContext` 在 ctx 取消/超时时立即返回），长跑场景本来就该设。

⚠️ **Retry-After 不改变"要不要重试"**，只改变"等多久"（以及上面那条：太久时先推迟、去试别的候选）。判据仍是 `shouldRetryMember`（fallback 层）/ `ShouldRetry`（retry 层）——一个带 `Retry-After` 的错误若被判为不该重试，这个值根本不会被读到。

---

## 8. 与现有机制的关系（一图厘清边界）

| 机制 | 层 | 问的问题 | 与 FallbackChatModel |
|---|---|---|---|
| `RunWithRetry` / `IsRetryable` | 重试 | 「重试**同一个** model 有用吗」 | 被复用为 §5.1 的内层重试 |
| `CircuitBreaker` | 熔断 | 「这个候选短期还值得试吗」 | 被复用为每候选的 `breaker` |
| ReactiveCompact（第 3 层压缩）| 压缩 | 「prompt 太长了压一压」 | 正交——prompt-too-long 被 `ShouldFailover` 判 false，不切模型，留给 ReactiveCompact |
| `streamOrGenerate`（`agent_v2/basic_agent.go`）| 调用方式 | 「这个 model 支不支持流式」 | 只在 model 声明不支持流式时改调 Generate；本装饰器的 Stream 在候选这一层就地处理（不支持流式的候选用它的 Generate），不跨 model 退化 |
| 4 层上下文压缩 | 上下文 | 「历史太长」 | 无关——压缩在 modelFn 入口先跑，FallbackChatModel 在其下游 |

---

## 9. 业内对照表

| 框架/网关 | 候选声明 | 错误分类 | 重试 | 断路器/cooldown | 流式切换 |
|---|---|---|---|---|---|
| **LangChain** `RunnableWithFallbacks` | `with_fallbacks([...])` 有序 | `exceptions_to_handle` 可配 | ❌（另有 retry runnable）| ❌ | 同样受限 |
| **eino** `ModelRetryConfig` | ❌ 无候选列表 | `IsRetryAble` | ✅ 同模型 | ❌ | `stream.Copy(2)` 消费检错 |
| **LiteLLM** | `fallbacks` + `context_window_fallbacks` | 按错误类型 | ✅ | ✅ Redis 跨实例 + cooldown | 网关层处理 |
| **OpenRouter** | `models: [...]` 优先级 | down/限流/审核拒答 | ✅ | ✅ | 网关层处理 |
| **本设计 FallbackChatModel** | `fallback.New([...])` 有序 | `FailoverPolicy`（默认按 StatusCode）| ✅ 复用 `RunWithRetry` | ✅ 复用 `CircuitBreaker`（per-member + cooldown 自愈）| 仅建流阶段（§6）|

**定位**：能力对齐 LangChain/Vercel 的 SDK 内置层；比 eino 多了「候选列表 + 断路器」（eino 只 retry 同模型）；不追求 LiteLLM 的跨实例共享态（那是网关的活，业务有此需求直接接网关，§10）。

---

## 10. 为什么选 SDK 内置而非网关

| | SDK 内置（本设计）| 依赖外部网关 |
|---|---|---|
| 集成成本 | `fallback.New([...])` 一行 | 部署 + 运维 LiteLLM/Portkey |
| 本地/单测 | ✅ 自包含，无外部依赖 | ❌ 单测要起网关 |
| 跨实例共享断路器 | ❌ per-process | ✅ Redis |
| 跨 provider 统一 observability | ❌ 各自接 | ✅ 网关统一 |
| 与 crew-go 既有原语复用 | ✅ RunWithRetry + CircuitBreaker | — |

**两条路并存**：crew-go 的 openai/qwen client 本就是 OpenAI 兼容，业务若已有网关，把 baseURL 指向网关即可、不用本装饰器。本设计补齐「不想引网关也能用」的自包含能力，是**补充**不是**替代**。这也是为什么不做侵入式编排改动——保持装饰器可选、可被网关方案平替。

---

## 11. 待定问题 / 风险（2026-06 落地后已收敛）

> 落地后本节已同步实现状态（详见 §0）：②③④⑤⑦ 已决/已实现、⑧ 已接受为固有代价；
> 仅 **① 仍真开放**（⑥ `member.tripped` metric 已实现，`cbcfe9fd`；2026-09-18 核验），已进中心 backlog（[`doc/code/todo.md`](../code/todo.md)）。

1. **[OPEN] `schema.StreamReader` 是否支持 `Copy`**——决定 §6 能否做「首 chunk 落地前」增强切换。grep 确认 StreamReader 当前**无 Copy 能力**，第一版只做建流阶段切换。→ 中心 backlog。（2026-09-18 核验：这个问题本身问偏了——peek 不必靠 `Copy`，见 §6。）
2. **[已决] 结构化错误回填**——`model.APIError` 已落地（`model/error.go` + §0 P0，openai client 5 处回填；qwen/deepseek 复用 openai.Client 自动受益）。
3. **[已决] 不加 `agent_v2` 语法糖**——装饰器已在 model 层够用，加 `WithFallbackModels` 反模糊「这是 model 层能力」的边界（grep 确认无此 API，定案不加）。
4. **[已决] onSwitch 独立回调**——已实现 `WithFallbackHook`（§7），不塞进 hook 体系（切换早于 agent/orch hook 触发时机）。
5. **[已决] 全员 cooldown 抛哨兵**——`ErrAllMembersUnavailable`（`fallback/fallback.go`，`errors.Is` 可判）。
6. **[已实现] 观测 metric**——候选切换记 `crew.fallback.failovers{crew.failover.reason, crew.failover.from}`、候选断路器熔断记 `crew.fallback.member_trips{crew.fallback.member, crew.fallback.reason}`（closed→open 边缘，含"全员挂光"——failover 计不到最后一个候选熔断）；`from` / `member` 记候选下标。均 sink-gated（事件定义在 `fallback/event.go`，`observability/orch_hooks.go` 的 `recordEventMetric` 转成指标，业务接 observability 的编排钩子 + 用 fallback 自动采集，边缘检测 `circuit_breaker.go` RecordFailure 返回值）。
7. **[已决] 全失败取 first_error**——主候选原始错误诊断价值最高（`fallback/fallback.go`，§0 落地偏离第 2 条，对齐 LangChain `fallbacks.py:215`）。
8. **[已接受] 半开探测在请求热路径**（断路器固有代价）——某候选 trip 后 cooldown 到期的第一个请求会先探测（含 retry 耗尽）再切健康候选，latency 偏高。代价是**每 cooldown 周期一次**（cooldown 窗口内 `Allow()=false` 直接 skip `circuit_breaker.go:91-92`；到期时 half-open 只放行**单次探测**、并发的其他 `Allow()` 仍返 false 防 thundering herd `circuit_breaker.go:94-102`）。AutoCompact/ReactiveCompact 同款接受；高频长 session + 主 model 长期不可用时可评估后台探测。第一版接受。

---

## 12. 实施计划（分阶段）

| 阶段 | 内容 | 验收 |
|---|---|---|
| **P0 结构化错误** | `model.APIError` + openai client 回填 StatusCode | 单测：构造 429/503/400 响应断言 `errors.As` 拿到正确 StatusCode |
| **P1 装饰器核心** | `FallbackChatModel` + `FailoverPolicy` + Generate 切换 + WithTools 传播 + breaker per-member | 单测：mock 候选注入 429/503，断言按序切换、breaker 计数、4xx 不切、全失败抛真实错误 |
| **P2 Stream + retry + 观测** | Stream 建流阶段切换 + 复用 RunWithRetry + onSwitch 回调 | 单测：建流失败切换；retry 耗尽再切；onSwitch 触发次数 |
| **P3 集成验证** | 真 LLM 冒烟（主 qwen + 备 deepseek，人为让主失败）+ 文档回填「已实现」 | `example/` 下 integration 冒烟跑通故障切换一条路径 |

> 测试纪律（对齐 CLAUDE.md）：mock 候选必须**读 input 做断言**、sanity-revert 必须看到 fail 精确指向 bug；断路器并发路径用 `-race` 跑。
</content>
</invoke>
