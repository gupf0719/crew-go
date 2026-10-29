# 编排模式深入用法（Orchestration Patterns）

> 9 编排（BasicAgent / Sequential / Parallel / Loop / Routing / Reflect / Handoff /
> Plan / GraphAgent）的**业务侧深入用法**——选型决策树、每个编排的典型场景、完整最
> 小例、关键 option、嵌套范式、反模式速查。

> **文档定位**：业务读完 [`usage-guide.md`](./usage-guide.md) 拿到 builder 速查表后，
> 想"我的场景该用哪个编排 + 关键参数怎么配"，看本文档。引擎层"9 编排怎么编译为 Graph"
> 见 [`channel-engine.md`](./channel-engine.md) §12。

## 目录

1. [编排选型决策树](#1-编排选型决策树)
2. [BasicAgent — ReAct 单 Agent](#2-basicagent--react-单-agent)
3. [SequentialAgent — 顺序链](#3-sequentialagent--顺序链)
4. [ParallelAgent — 并发 fan-out + 聚合](#4-parallelagent--并发-fan-out--聚合)
5. [LoopAgent — 条件迭代](#5-loopagent--条件迭代)
6. [RoutingAgent — Router 函数 / 决策方 / LLM Classifier](#6-routingagent--router-函数--决策方--llm-classifier)
7. [HandoffAgent — Agent 间状态机交接](#7-handoffagent--agent-间状态机交接)
8. [PlanAgent — Plan-and-Execute](#8-planagent--plan-and-execute)
9. [ReflectAgent — Actor-Eval-Reflect 循环](#9-reflectagent--actor-eval-reflect-循环)
10. [GraphAgent — 自定义 Graph 包成 Agent](#10-graphagent--自定义-graph-包成-agent)
11. [横切能力：Store / Skill / Retry / Hooks](#11-横切能力store--skill--retry--hooks)
12. [嵌套组合范式（典型业务）](#12-嵌套组合范式典型业务)
13. [反模式速查](#13-反模式速查)

---

## 1. 编排选型决策树

```
我的任务是...                                        → 用哪个编排
─────────────────────────────────────────────────────────────────────
单 LLM + 工具调用循环（ReAct 范式）                  → BasicAgent
多专家依次处理，前一输出作后一输入                   → SequentialAgent
同一输入并发问 N 个专家，结果聚合                    → ParallelAgent
某条件没满足前重复跑一段逻辑（refine / retry / poll）→ LoopAgent
按输入类型 / 意图分流到不同专家                       → RoutingAgent
多专家间动态交接控制权（state machine）              → HandoffAgent
LLM 拆解高层目标 → 逐步执行 → 边执行边规划            → PlanAgent
生成 → 评审 → 改进 反复迭代直到质量达标               → ReflectAgent
完全自定义节点 + 边拓扑（上面都不够灵活）              → GraphAgent
```

### 1.1 选型 5 维度

| 维度 | 单 Agent (BasicAgent) | 静态多 Agent (Sequential / Parallel) | 动态多 Agent (Routing / Handoff / Plan / Reflect) | 自定义 (GraphAgent) |
|---|---|---|---|---|
| **编译期确定步数** | N/A（单节点）| ✅（builder 阶段确定）| ❌（运行时决定）| ❌ |
| **专家数量** | 0-N tools | 2-N agents | 2-N agents | 任意 |
| **决策权** | LLM 决定 tool 序列 | builder 静态 | LLM / 业务 router 动态 | 业务自定义 |
| **嵌套友好度** | ⭐⭐⭐⭐⭐（最常被嵌入）| ⭐⭐⭐⭐（典型嵌入 BasicAgent / 子 Sequential）| ⭐⭐⭐⭐ | ⭐⭐⭐（业务自管 ctx graphNS）|
| **HITL 中断点** | tool 内 / hook 内 | sub-agent 内 / 步骤间 | sub-agent 内 / 路由后 | 业务节点内 |

### 1.2 边界场景：什么时候**不**用编排

- 任务**完全无 LLM**（纯计算 / 纯工具）→ 不需要 crew-go，直接业务代码
- **单次** LLM 调用 + 没工具调用 → `model.ChatModel.Generate(ctx, msgs)` 直接调，不用 BasicAgent
- 任务**需要分布式跨进程协作** → crew-go 不是分布式编排框架，业务自己拆服务

---

## 2. BasicAgent — ReAct 单 Agent

### 2.1 适用场景

- LLM 主导的任务循环：思考 → 调工具 → 看结果 → 继续思考 → 输出
- 工具集相对固定（不需要动态切换专家）
- 业务侧 90% 的"agent"场景都是 BasicAgent + 几个工具

### 2.2 完整最小例

```go
import (
    "crew-go/agent_v2"
    "crew-go/model/qwen"
    "crew-go/tool"
)

ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("researcher"),
    agent_v2.WithDescription("research recent news on a given topic"),  // 嵌入到 PlanAgent / RoutingAgent 时框架自动从 Description() 抽取
    agent_v2.WithSystemPrompt("你是研究员，擅长查资料并总结。"),
    agent_v2.WithModel(qwen.NewClient(apiKey, "")),
    agent_v2.WithToolkit(myToolkit),     // 工具集
    agent_v2.WithMaxRounds(20),          // ReAct 循环最大轮数（默认 10，业务可调）
    agent_v2.WithHooks(myHooks),         // 业务级观测
)

events, err := ag.Run(ctx, sess, schema.UserMessage("研究本周 AI 新闻"))
final := agent_v2.FinalMessage(events)
```

### 2.3 关键 Options

| Option | 用途 | 默认 |
|---|---|---|
| `WithName(name)` | agent 标识（嵌套时用，session 隔离 namespace）| **不传后果不对称**：BasicAgent / PlanAgent 自动生成 `agent_v2_<UUID8>` / `plan_agent_<UUID8>`；其余 7 编排（Sequential/Parallel/Loop/Routing/Reflect/Handoff/Graph）**空 name 构造期 panic**（#7 F6 fail-fast）。注：`WithName` 本身是纯 setter，**不**校验字符；`/` `#` `@`（破坏 ns 解析）的校验发生在**子 agent/step/branch 名**——`AddAgent`(Handoff) / `AddSubAgent`(Plan) / `AddStep`(Sequential) / `AddBranch`(Parallel/Routing) 都在调用点即 `ValidateNodeIDForNS` panic（空串同样拒），另外 Sequential 拒图的起止节点名、Routing 拒 `__fallback__` 与 `__end__`、Plan 拒它自己图里用的节点名；嵌入运行路径（`RunSubAgent`）则返 error |
| `WithDescription(desc)` | 自描述能力（PlanAgent / RoutingAgent 自动抽取）| 嵌入场景必填 |
| `WithSystemPrompt(s)` | system message | 空 |
| `WithModel(m)` | ChatModel | **必填** |
| `WithToolkit(tk)` | 工具集 | 空（纯 LLM 对话）|
| `WithMaxRounds(n)` | ReAct 循环硬上限 | 10 |
| `WithHooks(h)` | 业务级观测 | nil |
| `WithMicroCompact / WithAutoCompact / WithReactiveCompact` | 4 层压缩（详见 `context-compaction.md`）| 全 disabled |
| `WithMaxHistoryPairs(n)` | 第 0 层兜底截断（保留最近 N 对 user/assistant）| 20（默认开启；`WithMaxHistoryPairs(0)` 显式禁用）|
| `WithHistoryCompactor(fn)` | 第 0 层自定义截断逻辑 | nil |
| `WithStore(st) + WithStoreMemory(ns, cfg)` | 跨 Run 长期记忆（替代旧 `WithMemory`，详见 `store.md`）| 无 |
| `WithSkills(src)` | Skill 动态激活（接 `skill.SkillSource`） | 无 |
| `WithAskUser()` | 自动注入 ask_user 工具（HITL）| 无 |

### 2.4 关键不变量（详见 `channel-engine.md` §3 + `hitl-resume.md` §6）

- ReAct 循环：model 节点 → tools 节点 → model 节点 → ... 直到 model 输出无 tool_call
- ✨ **已成功的 tool_call Resume 不重执行**——tools 节点 memo（节点私有中断态，随 cp 走）记产出，重跑时命中即重放；只有中断/失败/没跑到的那些执行（ask_user 二阶段语义自动幂等）。业务幂等只剩 memo 覆盖不到的场景（进程在节点执行中途崩溃——durability 只在超步边界写档、带不上 memo，配了也一样——等），见 `hitl-resume.md` §6.3
- ✨ **chunk 不写 session.Events**——streaming chunk 通过 OrchHooks 投递，最终聚合 message 走审计

### 2.5 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 在工具内 panic 表达 HITL | `return nil, &compose_v2.InterruptError{Query: &AskQuery{...}}` |
| `WithMaxRounds(0)` 期望永不停 | 实际 `WithMaxRounds(n)` 当 `n<=0` 兜底 `n=1`（见 `config.go` 的 `WithMaxRounds`）—— 想跑多轮显式传 `WithMaxRounds(10)`；想真不停业务自定义 LoopAgent + 终止条件 |
| 工具执行写 DB 但不防重 | 同轮中断时已成功的写由节点 memo 挡住不重放，但进程在节点执行中途崩了 memo 也没落盘（durability 只在超步边界写档，配 sync / async 也带不上它）——扛硬崩只能加 idempotency_key |
| `WithStoreMemory(ns, nil)` 或 `WithStore(nil)` | 不传 `WithStoreMemory` / `WithStore` 即可（nil Store 没意义）|

---

## 3. SequentialAgent — 顺序链

### 3.1 适用场景

- 多专家依次处理，**编译期就知道步骤**（不是 LLM 动态规划）
- 流水线：搜资料 → 摘要 → 翻译 → 排版
- 前一步产出作后一步输入（但简单场景；复杂依赖关系用 PlanAgent）

### 3.2 完整最小例

```go
seq := agent_v2.NewSequentialAgent(
    agent_v2.WithSequentialName("article_pipeline"),
    agent_v2.WithSequentialDescription("research → summarize → translate"),
).
    AddStep("research",  researcher).
    AddStep("summarize", summarizer).
    AddStep("translate", translator)

events, err := seq.Run(ctx, sess, schema.UserMessage("本周 AI 新闻"))
```

### 3.3 关键 Options + 链式方法

| API | 用途 |
|---|---|
| `NewSequentialAgent(opts...)` | 构造 |
| `.AddStep(name, agent)` 链式 | 添加 step（顺序按 AddStep 调用顺序）|
| `WithSequentialName(name)` | 标识 |
| `WithSequentialDescription(desc)` | 自描述 |
| `WithSequentialHooks(h)` | 观测 |
| `WithSequentialRetryConfig(cfg)` | 每步 sub-agent.Run 独立重试（每步独立计 attempt，非整链统一重试；见 §13.2）|
| `WithSequentialMaxRetries(n)` | 简化 retry 配置 |

### 3.4 数据流（前一步输出作后一步输入——**实际拼装"原始目标 + 上一步产出"**）

每个 step 跑完后，框架取该 step 的最终 `*schema.Message`，**拼装格式**给下一步：

```
step 0 input = origMsg（caller 传入）
step N+ input = UserMessage("原始目标：{origContent}\n\n上一步（{prevName}）的产出：\n{prevContent}")
```

具体例（research → summarize → translate 流水线）:
```
step "research" 输入 = origMsg("写一篇 AI 周报") → final = "今天 OpenAI 发布 GPT-5"
                                                  ↓
step "summarize" 输入 = "原始目标：写一篇 AI 周报\n\n上一步（research）的产出：\n今天 OpenAI 发布 GPT-5"
                                                  ↓ 跑完 → final = "OpenAI 发 GPT-5"
                                                  ↓
step "translate" 输入 = "原始目标：写一篇 AI 周报\n\n上一步（summarize）的产出：\nOpenAI 发 GPT-5"
                                                  ↓ 跑完 → final = "OpenAI released GPT-5"
```

> ⚠️ **业务实现 step sub-agent 时务必预期此拼装格式**——sub-agent 的 system prompt
> 应设计为能从"原始目标 + 上一步产出"双重信息里抽取真正要做的事，不是裸拿前一步文本。

### 3.5 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 步骤间复杂依赖（step 3 需要 step 1 + step 5）| 用 PlanAgent + InputRefs 显式声明依赖 |
| 步骤数运行时决定 | 用 PlanAgent 让 LLM 动态规划 |
| `AddStep` 加同名 step | 构造期直接 panic——同名 step 在调用点就被拒绝 |

---

## 4. ParallelAgent — 并发 fan-out + 聚合

### 4.1 适用场景

- 同一输入**并发**问 N 个专家
- 多源信息聚合：3 个 researcher 各查不同 source → 综合
- N 个独立子任务并发处理（每个 task 不依赖其他 task 产出）
- LLM 评分多个候选并选最佳

### 4.2 完整最小例

```go
parallel := agent_v2.NewParallelAgent(
    agent_v2.WithParallelName("multi_source_research"),
    agent_v2.WithAggregator(func(ctx context.Context, results map[string]string) (*schema.Message, error) {
        // 业务自定义聚合：合并 3 个 researcher 的输出
        var sb strings.Builder
        for name, text := range results {
            sb.WriteString("【" + name + "】\n" + text + "\n\n")
        }
        return schema.AssistantMessage(sb.String()), nil
    }),
).
    AddBranch("source_a", researcherA).
    AddBranch("source_b", researcherB).
    AddBranch("source_c", researcherC)

events, err := parallel.Run(ctx, sess, schema.UserMessage("研究 GPT-5"))
```

### 4.3 关键 Options + 链式方法

| API | 用途 |
|---|---|
| `NewParallelAgent(opts...)` | 构造 |
| `.AddBranch(name, agent)` 链式 | 添加并发分支 |
| `WithParallelMerger(m ParallelMerger)` | 低阶 merger（保留 BranchResult 元信息）|
| `WithAggregator(fn func(ctx, map[string]string) (*Message, error))` | 高阶简化（只看分支名 + 文本）|
| `WithParallelHooks(h)` | 观测 |
| `WithParallelRetryConfig / WithParallelMaxRetries` | 整 branch 失败重试 |

### 4.4 关键不变量（详见 `channel-engine.md` §7 + `hitl-resume.md` §7）

- 所有 branch 编译为图原生超步 frontier，**并发执行**
- ✨ **per-branch delta 自然继承**——HITL 中断时仅重跑失败/中断分支，成功分支已在 cp.Channels
- ✨ 多分支并发抛 InterruptError 时**全部收集**到 `cp.PendingInterrupts`（R6 Bug 3 修）
- ✨ ctx cancel **传染**所有兄弟（与业务 error 不传染兄弟有别）
- ✨ best-effort 等兄弟跑完——**ctx deadline 必给**避免死锁
- **分支不能做转交 / 路由决定**：当 HandoffAgent 的候选、RoutingAgent 的分类器时，收到的能力不转授给分支
  （分支里的 BasicAgent 没有 transfer / route_to 工具，程序化 `HandoffTo` 返回「没有被授予」）。并行的几个
  分支各自表态，分歧时谁也没有资格裁决；想「先并行出意见、再定转交」，写成 `Sequential[Parallel, 决定者]`

### 4.5 ParallelAgent + Store 协议（ISSUE-80 + ISSUE-84 audit²）

**lane events 物理隔离 (ISSUE-80)**: branch 内 events 写到 lane 私有 slice (lock-free),
与 sibling branch 物理隔离 — 完成后 `CommitLaneEvents` 按 branch 完成顺序整段 append 回 parentSession（单 branch 内时序保持，不跨 branch 按 TS 重排）.
设计参考 eino adk forkRunCtx + joinRunCtxs. branch 内业务可安全 `sess.AddEvent` 不串台.

**store.Put 协议**: branchFn 在 `br.Agent.Run(ctx, laneSess)` 前 push
`WithSubAgentStoreScope(subCtx, true)`，业务自定义 Store 实现可以读它。

⚠️ **别照老写法用它整个 skip**。曾经的建议是「sub-agent 直接 `return nil`，让顶层唯一
写入」——那个建议的前提是「成员和顶层会写到同一个键上」，而写入者维挪进 ns 之后前提没
了（成员写 `ns/分支名`、顶层写 `ns` 本身，本来就不重复）。照老写法实现的后端，净效果是
**成员的 `WithStoreMemory` 一条也落不了盘**（实测：两分支 → Put 调用 2 次、丢弃 2 次、
落盘 0 条、`Run.err=nil`）。

正当用法是「按位置给不同待遇」而不是「按位置丢弃」：

```go
func (m *MyStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
    if agent_v2.IsSubAgentStoreScope(ctx) {
        return m.realPutWithTTL(ns, key, value, 24*time.Hour) // 成员的记忆留短一点
    }
    return m.realPut(ns, key, value)
}
```

### 4.6 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 不设 ctx deadline | `ctx, cancel := context.WithTimeout(ctx, 30*time.Second)` 必给 |
| 分支内 panic 不 recover 让框架接 | safeRunNode 转 error 信息少；业务自己 recover 给精确 error |
| 分支共享 mutable struct 不深拷贝 | channel 模型天然隔离；业务自定义状态 必须深拷贝 |
| 用 `WithParallelMerger` 同时用 `WithAggregator` | **`WithParallelMerger` 恒优先**（无条件覆写 `p.merger`，无论调用顺序）；`WithAggregator` 仅在 merger 未设时生效（内部 `if p.merger != nil { return }`）。业务确定单一选用 |

---

## 5. LoopAgent — 条件迭代

### 5.1 适用场景

- 重复执行直到满足退出条件：refine / retry / poll
- 计数器迭代：跑 N 次然后停
- 条件搜索：跑直到找到满足条件的结果

### 5.2 完整最小例

```go
loop := agent_v2.NewLoopAgent(
    agent_v2.WithLoopName("refine_until_good"),
    agent_v2.WithLoopMaxIterations(5),
    agent_v2.WithLoopExitCondition(func(ctx context.Context, lastOut *schema.Message, round int) (bool, error) {
        // 退出条件：LLM 输出含 "DONE" 或跑满 5 轮
        return lastOut != nil && strings.Contains(lastOut.Content, "DONE"), nil
    }),
).WithBody(refinerAgent)

events, err := loop.Run(ctx, sess, schema.UserMessage("初稿"))
```

### 5.3 关键 Options

| API | 用途 |
|---|---|
| `NewLoopAgent(opts...)` | 构造 |
| `.WithBody(agent)` 链式 | 设置循环体（必填）|
| `WithLoopMaxIterations(n)` | 硬上限（默认 10）|
| `WithLoopExitCondition(fn func(ctx, lastOut, round) (bool, error))` | 退出条件（lastOut + round 都可读；返 `(true, nil)` 退出, 返 `(_, err)` propagate）|
| `WithLoopHooks(h)` | 观测 |
| `WithLoopRetryConfig / WithLoopMaxRetries` | 单轮失败重试 |

### 5.4 数据流

每轮 body 跑完后 framework 取 `FinalMessage(events)` → 给 ExitCondition 判定 + 作为下一轮 body 的 input。

**退出判定优先级**（`loop_agent.go`）：① body output `Annotations["break_loop"]==true` —— **最高优先级**（body agent 主动 break，先于下面两者）；② `WithLoopExitCondition` 返 `(true, nil)`；③ 达 `WithLoopMaxIterations`（默认 10）。业务想让 body 据 LLM 判断提前退出，在 body output 设 `break_loop` annotation（比 ExitCondition 更直接，无需把判断逻辑放外部函数）。

### 5.5 关键不变量

- ✨ **per-round ns 隔离**——nested_smoke 多轮 HITL 验证：第 N 轮中断 cp 写到 `<loopNS>/round_<N>` 不冲突
- ✨ 跑满 maxIterations 返 `*compose_v2.MaxIterationsError`（业务用 `errors.As` 解析具体值）

### 5.6 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 不设 ExitCondition 期望"自然"退出 | 必填——否则只靠 maxIterations 兜底，浪费 N-1 轮 LLM 调用 |
| ExitCondition 内调 LLM | 退出判断在每轮末尾，频繁调 LLM 浪费——把判断逻辑放进 body agent 输出特殊 marker |
| body 内修改外部 state | body 应该是纯函数（基于 input + ctx 输出 message）；外部 state 用 RunSession.SetValue/GetValue |

---

## 6. RoutingAgent — Router 函数 / 决策方 / LLM Classifier

### 6.1 适用场景

- 按输入类型 / 意图分流到不同专家
- 业务规则路由（关键词命中 → 路由到对应专家）
- LLM 分类路由（自然语言意图识别）

### 6.2 决策来源

路由决策二选一：业务写的 Router 函数自己说了算；或者由决策方、LLM classifier、兜底分支
组成升级链，拿不准时逐档交给下一档。两者互斥，同时配构造期 panic。

**Router 函数路径**（业务规则）:

```go
routing := agent_v2.NewRoutingAgent(
    agent_v2.WithRoutingName("assistant"),
    agent_v2.WithRouter(func(ctx context.Context, input *schema.Message) (compose_v2.NodeID, error) {
        // 业务规则——返 (NodeID, error); 返 ("", nil) → 路由 EndNode 无产出; 返未注册名 → fail-fast 报错; 返 (_, err) propagate
        if strings.Contains(input.Content, "天气") {
            return "weather_agent", nil
        }
        if strings.Contains(input.Content, "翻译") {
            return "translator", nil
        }
        return "default_agent", nil
    }),
).
    AddBranch("weather_agent", weatherAgent).
    AddBranch("translator",    translatorAgent).
    AddBranch("default_agent", defaultAgent)
```

**LLM Classifier 路径**（自然语言意图分类）:

```go
routing := agent_v2.NewRoutingAgent(
    agent_v2.WithRoutingName("assistant"),
    agent_v2.WithRoutingClassifier(classifier),  // classifier 是业务自建 Agent（NewBasicAgent + agent_v2.WithRouteTool() 声明愿意定路由 + system prompt）；入参是 Agent 非 LLM/ChatModel，框架不自动包 BasicAgent
    agent_v2.WithRoutingDescriptions(map[string]string{
        "weather_agent":  "回答天气相关问题",
        "translator":     "翻译多语言文本",
        "default_agent":  "其他通用问题",
    }),
    agent_v2.WithRoutingFallback(defaultAgent),  // classifier 报普通错或没选出合法目标时兜底（中断、喊停、终局失败等必传的错不兜底）
).
    AddBranch("weather_agent", weatherAgent).
    AddBranch("translator",    translatorAgent).
    AddBranch("default_agent", defaultAgent)
```

**带置信度的决策方**（快而便宜的决策模型先判，拿不准再交给 LLM classifier）:

```go
routing := agent_v2.NewRoutingAgent(
    agent_v2.WithRoutingName("assistant"),
    agent_v2.WithRouteDecider(decision.NewRouteDecider(client), 0.85), // client 由 jev.NewClient 或 bailian.NewClient 构造
    agent_v2.WithRoutingClassifier(classifier),
    agent_v2.WithRoutingDescriptions(descriptions),
    agent_v2.WithRoutingFallback(defaultAgent),
)
```

升级规则、事件字段与实测数据见 [route-decider.md](route-decider.md)。

### 6.3 关键 Options

| API | 用途 |
|---|---|
| `NewRoutingAgent(opts...)` | 构造 |
| `.AddBranch(name, agent)` 链式 | 添加候选分支 |
| `WithRouter(fn)` | 业务规则路由；与决策方、classifier、兜底互斥，同时配构造期 panic |
| `WithRouteDecider(d, minConfidence, opts...)` | 带置信度的决策方，拿不准交给下一档；可多次调用排成升级链，`WithBranchMinConfidence` 按分支设门槛 |
| `WithRoutingClassifier(ag)` | LLM 分类路由；配了决策方时是它们之后的下一档 |
| `WithRouteTool()` | classifier 声明愿意定路由：被 RoutingAgent 授予路由能力时框架给它注入内置 return-direct `route_to` 工具（**要用工具定路由就只能用它**——裸 ToolInfo/普通 FuncTool 形态做不出路由决定，见 `RouteTool` 常量 godoc；纯文本小模型也可以不配，靠整段文本等于分支名，对格式敏感）。**能力是签名不是环境**：只有分类器这个对象被授予；编排分类器自动转授给成员（`ParallelAgent` 的分支并发、不转授），业务壳要显式 `RegrantRouteTo(opts, self, inner)`，GraphAgent 节点 / BasicAgent 工具里要显式 `RegrantFromNode(ctx, sub)`——分类器请教的别的 agent 没被转授就定不了路由（见 `agent_v2/route_grant.go`）|
| `WithRoutingDescriptions(map)` | 分支描述——框架自动 prepend `branch_name: 描述` 清单到 classifier 收到的 input，让 LLM 按描述选目标（省 caller 在 classifier system prompt 手抄候选清单）；按 branch 注册顺序、仅列非空 description。决策方收到的候选清单也带着这些描述 |
| `WithRoutingFallback(ag)` | 升级链终点：classifier 失败、或决策方都拿不准又没有 classifier 时兜底 |
| `WithRoutingHooks / WithRoutingRetryConfig` | 观测 / 被选中分支的重试（只作用于分支成员，不作用于决策方和 classifier）|

### 6.4 信号优先级（CLAUDE.md 速查）

1. 上行信号（首选 — 被授予路由能力的 Classifier（及它显式转授的组成部分）调 `route_to` 工具做出的路由，
   跟着写入者的提交走；分类器先后两步各定一次时后者为准，同一批两次 route_to 排在前面的生效，同一超步的两路
   都定了路由是图的设计错误：报 `SignalConflictError` 并带 `ErrRunAborted`（终局、不落兜底））
2. 从分类器最终消息解析：先看消息里的 `route_to` tool_call 参数，再看 `msg.Annotations["route_target"]`
   （编程注入；注: `RouteTool="route_to"` 是 tool 名常量, **不是** annotation key, 见 §6.5 关键不变量）
3. 文本内容去掉首尾空白之后整段等于某个分支名（不做关键词匹配）

都取不到时有兜底分支走兜底，没有就返回 `*RouteUndecidedError`。

### 6.5 关键不变量

- ✨ `RoutingAgentRouter` 签名：`func(ctx, input *schema.Message) (compose_v2.NodeID, error)` —— 返 `(NodeID, error)` (实证 `agent_v2/routing_builder.go`)
- ✨ Router 函数返 `""`/EndNode → 路由 **EndNode（无产出，不走 Fallback）**；返未注册分支名 → **fail-fast 报错**（err 含 "unregistered branch"）。Router 是业务显式函数自负返值，路径上没有升级
- ✨ Router 与决策方、Classifier、Fallback 互斥，同时配构造期 panic
- ✨ annotation key 是 `"route_target"`（不是 `RouteTool` 常量值——`RouteTool="route_to"` 是 tool 名）

### 6.6 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 不设 Fallback，又指望 classifier 失败时 Run 照常产出 | classifier 调用失败、没给目标或返不存在的分支名时 Run 返回 `*RouteUndecidedError`（原因在它的 `ClassifierFailure`）。需要一个兜底答复就设 Fallback |
| `WithRoutingDescriptions` 漏某分支（或描述留空）| 该分支不进注入的候选清单，classifier 只有在自己的 system prompt 提到它时才可能选中它；决策方仍会看到它，但只看得到分支名。每个候选分支都该配非空 description |
| Router 与 Classifier / 决策方一起配，指望"规则不中再交给 LLM" | 构造期 panic：Router 没有「拿不准」的出口。要升级就把规则写成决策方，拿不准时弃权 |

---

## 7. HandoffAgent — Agent 间状态机交接

> 本节是业务用法速览；**完整模型 B 设计 + 全部 API + 四方对照见 [handoff.md](./handoff.md)（权威）**。

### 7.1 适用场景

- 多 agent 间动态交接控制权（不是路由 — 是"接力"）
- 客服系统：通用 agent → 转专业 agent（产品 / 售后 / 退款）
- 多角色协作：Researcher → Reviewer → Editor 但 Reviewer 可决定退回 Researcher

### 7.2 完整最小例

sub-agent **必须 opt-in 配 `WithHandoffTransfer()`** 才能被 LLM 自主转交（装备框架内置
`transfer_to_agent` return-direct 工具）——不配则该 agent 无法转交，照抄漏配会建出 LLM 转不走的死 handoff：

```go
greeter := agent_v2.NewBasicAgent(
    agent_v2.WithName("greeter"), agent_v2.WithModel(m),
    agent_v2.WithHandoffTransfer(),  // ← opt-in：LLM 可调 transfer_to_agent({agent_name,reason}) 转交
    agent_v2.WithSystemPrompt(agent_v2.HandoffPrompt("你是接待，识别意图后转给 product/refund")),
)
// product / refund 同样各配 WithHandoffTransfer()

handoff := agent_v2.NewHandoffAgent(
    agent_v2.WithHandoffName("customer_service"),
    agent_v2.WithHandoffInitial("greeter"),
    agent_v2.WithHandoffMaxHops(10),
    agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistoryRewrite),  // 后接看前接历史，默认 Rewrite
).
    AddAgent("greeter", greeter).
    AddAgent("product", product).
    AddAgent("refund",  refund)

events, err := handoff.Run(ctx, sess, schema.UserMessage("我要退款"))

snap := handoff.State(sess)  // 查运行态
fmt.Printf("current=%s hops=%d done=%v exhausted=%v\n", snap.Current, snap.Hops, snap.Done, snap.Exhausted)
```

`HandoffStateSnapshot` 字段：
```go
type HandoffStateSnapshot struct {
    Current   string  // 当前持有控制权的 agent 名（首次 Run 前为 initial）
    Hops      int     // 已发生的 handoff 次数
    Done      bool    // 本会话已正常结束（active agent 不再 handoff）
    Exhausted bool    // 达到 maxHops 上限被动结束
}
```

### 7.3 转交信号（模型 B）

**主路径 = transfer 工具（LLM 自主）**：配了 `WithHandoffTransfer()` 的 sub-agent，LLM 调
`transfer_to_agent({agent_name, reason})`（return-direct 当轮短路转交）即转交控制权。**这是纯 LLM
自主转交的唯一可行路径**——LLM 跑出的 final message 不会自带 annotation。

**程序化成员 = `HandoffTo`**：业务自己实现的 Agent（没有 LLM 调工具）用 `agent_v2.HandoffTo(ctx, opts, self, target)`
转交，跟 transfer 工具走同一套校验（被授予了没有 / 目标合不合法 / 这一层有没有人接）。两条路做出的是同一类决定：
先后做出的后者为准，同一超步两个成员都做了决定是图的设计错误（`SignalConflictError` 并带 `ErrRunAborted`，见
[handoff.md](./handoff.md) §6.3）。`ParallelAgent` 当候选时分支拿不到转交能力——想并行出意见、再定转交，写成
`Sequential[Parallel, 决定者]`。

⚠️ `Annotations[HandoffSignalKey]` **不是**转交通道：它是 stepFn 裁决之后写给条件边的内部字段，入站产出上的这个
字段会被清掉。它曾经是业务可写的兜底通道，那条路绕过全部三道校验（一个连 `WithHandoffTransfer()` 都没配的候选，
一行 annotation 就把对话转走了，err 全程 nil），已删。

转交决定是交给上层 HandoffAgent 的上行信号，跟着写入者的提交走（中断续跑不丢、失败的尝试里没做成的部分
不算；先后做出的后者为准、同一超步两个成员都做了决定按设计错误终局）。完整链见 [handoff.md](./handoff.md) §6。

### 7.4 关键 Options

| API | 用途 |
|---|---|
| `NewHandoffAgent(opts...)` | 构造 |
| `.AddAgent(name, agent)` 链式 | 添加候选 agent |
| `WithHandoffInitial(name)` | 入口 agent（无持久 active 时的起跑点）|
| `WithHandoffMaxHops(n)` | 单 Run 内转交次数硬上限（防死循环；**per-Run 重置**）|
| `agent_v2.WithHandoffTransfer()` | **sub-agent 侧 opt-in**：装备 `transfer_to_agent` return-direct 工具（不配则该 agent 无法被 LLM 自主转交）|
| `WithHandoffHistoryMode(mode)` | 后接看前接历史可见性：`HandoffHistoryRewrite`(默认,第三人称改写,保转交文本痕迹) / `Raw`(保 role;悬空 transfer 由框架剥离、换成一条中性记账文本) / `SelfOnly`(隔离) |
| `(*HandoffAgent).EndConversation(ctx, threadID)` | 显式结束会话、清持久 active（长跑服务主动清理）|
| `(*HandoffAgent).State(sess)` | 查 `HandoffStateSnapshot{Current, Hops, Done, Exhausted}` 运行态 |

### 7.5 关键不变量

- 转交后**不重新跑 initial agent**——直接接力到 target
- maxHops 跑满返 final（不报错）+ snap.Exhausted=true
- **持久 active 跨 Run**：转交后的控制权走 `store.Store`（按 ThreadID）持久化，**不走 cp**
  （cleanupOnSuccess 清 cp）——同 ThreadID 下次 Run 自动从持久 active 起跑、不回 initial。
  `HandoffStateSnapshot` 是 per-RunSession 运行态视图（`sync.Map` 按 RunID），跨 Run 持久态在 store

### 7.6 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 不设 maxHops | 默认 10；业务想长链显式调大 |
| 转交回上一个 agent 死循环 | LLM prompt 教它"避免来回转交"；或用更明确的 maxHops |
| `WithHandoffInitial` 设了不存在的 name | Run 时 fail-fast |

---

## 8. PlanAgent — Plan-and-Execute

### 8.1 适用场景

- LLM 拆解高层目标为子任务序列
- 边执行边规划（每步执行后 LLM 重新看事实决定下一步——replan-每步）
- 复杂多专家协作：planner 不知道用户具体需求，让 LLM 边跑边决定调用哪个 expert
- 需要"用户中途插话改方向"（ResumeAction.Replan）

### 8.2 完整最小例

```go
// 1. 定义子专家 agents（Description 自描述能力）
researcher := agent_v2.NewBasicAgent(
    agent_v2.WithName("researcher"),
    agent_v2.WithDescription("research recent news on a given topic"),
    agent_v2.WithModel(m),
)
summarizer := agent_v2.NewBasicAgent(
    agent_v2.WithName("summarizer"),
    agent_v2.WithDescription("summarize text into 300 words or less"),
    agent_v2.WithModel(m),
)
translator := agent_v2.NewBasicAgent(
    agent_v2.WithName("translator"),
    agent_v2.WithDescription("translate text to English"),
    agent_v2.WithModel(m),
)

// 2. 构造 PlanAgent
subAgents := map[string]agent_v2.Agent{
    "researcher":  researcher,
    "summarizer":  summarizer,
    "translator":  translator,
}
plan := agent_v2.NewPlanAgent(
    agent_v2.NewLLMPlanner(planModel,    // 内置 LLM 驱动 Planner
        agent_v2.WithLLMPlannerSubAgents(subAgents)),  // 自动从 sub-agent.Description() 抽取注入 prompt
    agent_v2.WithSubAgents(subAgents),
    agent_v2.WithPlanMaxSteps(20),
)

events, err := plan.Run(ctx, sess, schema.UserMessage("写一篇英文 AI 新闻周报"))
```

### 8.3 三种执行器优先级（PlanStep 字段）

| 优先级 | 字段 | 适用 |
|---|---|---|
| 1 | `SubAgentName != ""` | 注册池里的专家（最常用）|
| 2 | `TmpSubAgent != nil` | LLMPlanner 不知前面 / 一次性专家（业务动态构造一次性 BasicAgent）|
| 3 | 都为空 | `SetExecutor(...)` 设的兜底默认执行器 |

### 8.4 关键 Options

| API | 用途 |
|---|---|
| `NewPlanAgent(planner, opts...)` | 构造（planner 必填）|
| `NewLLMPlanner(m, opts...)` | 内置 LLM 驱动 Planner |
| `WithLLMPlannerSubAgents(map)` | 自动从 Agent.Description() 抽元数据注入 LLM prompt（**关键 interface contract pattern**）|
| `WithLLMPlannerSystemPrompt(s)` | 自定义 LLMPlanner 的 system prompt（默认含规划纪律 + depends_on 引导）|
| `WithSubAgents(map)` | 注册执行池 |
| `(*PlanAgent).AddSubAgent(name, ag)` 链式 | 单独添加 |
| `(*PlanAgent).SetExecutor(ag)` | 默认执行器（PlanStep SubAgentName 都没匹配时用）|
| `WithPlanMaxSteps(n)` | 步数硬上限（默认 20）|
| `WithExecInputBuilder(fn)` | 自定义 sub-agent input 构造（默认拼"原始目标 + 任务 + 引用步骤产出"）|
| `WithPlanModel(m)` | 共享 model（TmpSubAgent 构造用）|
| `WithPlanToolkit(tk)` | 共享 toolkit（TmpSubAgent 按 ToolNames 筛子集）|

### 8.5 关键设计：InputRefs 显式依赖

每 step 通过 `InputRefs []RefSpec` 声明依赖前面哪些步骤的产出（详见 [`hitl-resume.md`](./hitl-resume.md) 中 PlanAgent 章节及 `agent_v2/plan_agent.go` godoc）：

```go
type RefSpec struct {
    Index     *int   // 0-based executed[i] 引用
    AgentName string // 倒序找最近匹配
}
agent_v2.IndexRef(0)              // 引用第 0 步
agent_v2.AgentRef("researcher")   // 引用最近一个 researcher 跑的步骤
```

LLMPlanner 的 plan tool schema 自动让 LLM 在 `depends_on: [int]` 字段填 0-based 索引，框架按 `InputRefs` 解析后自动把对应 `executed[i].Result` 拼进 sub-agent 的 input。

### 8.6 关键不变量

- ✨ **replan-每步**：每步执行完后都 `Planner(ctx, sess, original, executed)`——LLM 看新事实重新决定剩余步骤
- ✨ **InputRefs 漏了** sub-agent 拿不到前置原文——LLM 瞎编 / 问回 / 报错（详见 `agent_v2/llm_planner.go` 系统 prompt §每步依赖声明 depends_on）
- ✨ **Replan 仅 PlanAgent + BasicAgent + GraphAgent 接受**（R21 audit MED-1 fix `31660b1` 让 GraphAgent 透传给底层 sub-agent）——Sequential/Parallel/Loop/Routing/Reflect/Handoff 6 编排显式 fail-fast `return error "ResumeActionReplan not supported"`（commit `c213263` Phase 5.2 收紧）

### 8.7 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| sub-agent 没填 Description | LLMPlanner 看不到能力描述 — 选错专家 / 不调用专家；Description 必填（嵌入场景）|
| 步骤间复杂依赖不用 InputRefs | sub-agent 拿不到前置原文——必显式声明 depends_on |
| `SetExecutor` 设了但 sub-agent 池也设了 | 三档优先级清楚（SubAgentName > TmpSubAgent > Executor），但避免业务认知混乱 |
| `WithPlanMaxSteps(0)` 期望无限 | 0 走默认 20；想真不停业务自定义 LoopAgent + Plan |

---

## 9. ReflectAgent — Actor-Eval-Reflect 循环

### 9.1 适用场景

- 生成 → 评审 → 改进 反复迭代直到质量达标
- 写作改稿 / 代码 review / 翻译质检
- 单次 LLM 输出不够好需要"自我批判 + 改进"

### 9.2 三档评估器（互斥，多档同配构造期 panic）

> **关键 API 提示**：`WithActor` 是 `(*ReflectAgent).WithActor` **链式方法**，不是
> 全局 option 函数；`WithReflector` / `WithEvaluator` / `WithReflectorAgent` /
> `WithEvalFunc` 是全局 option 函数 — 通过 `NewReflectAgent(opts...)` 传入。

```go
// 档 1：Reflector 函数（最灵活）
ag := agent_v2.NewReflectAgent(
    agent_v2.WithReflector(func(ctx context.Context, goal, output *schema.Message) (*agent_v2.ReflectOpinion, error) {
        // 业务自定义：可调 LLM、规则、外部 API ...
        if isGoodEnough(output) {
            return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
        }
        return &agent_v2.ReflectOpinion{
            GoalMatched: false,
            Opinion:     "需要更详细描述",  // 非空时下一轮 actor 拿到此 feedback
        }, nil
    }),
    agent_v2.WithReflectMaxRounds(5),
).WithActor(writerAgent)  // ← 链式方法

// 档 2：Evaluator + ReflectorAgent（评估 + 反思分两个 sub-agent）
ag := agent_v2.NewReflectAgent(
    agent_v2.WithEvaluator(evaluatorAgent),       // 输出 GoalMatched + Opinion
    agent_v2.WithReflectorAgent(reflectorAgent),  // R33 拆图后跑 feedback 节点：收 user-role 合成消息（原始目标+actor 输出+evaluator 批评全文），产改进意见
    agent_v2.WithReflectMaxRounds(5),
).WithActor(writerAgent)

// 档 3：EvalFunc（最简单）
ag := agent_v2.NewReflectAgent(
    agent_v2.WithEvalFunc(func(ctx context.Context, output *schema.Message) (bool, error) {
        return strings.Contains(output.Content, "GOOD"), nil  // 简单文本匹配
    }),
    agent_v2.WithReflectMaxRounds(5),
).WithActor(writerAgent)
```

### 9.3 关键 Options

| API | 用途 |
|---|---|
| `NewReflectAgent(opts...)` | 构造（option 入口）|
| `(*ReflectAgent).WithActor(ag)` 链式 | 生成者（必填，**链式方法不是 option**）|
| `WithReflector(fn Reflector)` | 档 1：自定义评估函数；`Reflector func(ctx, goal, output *Message) (*ReflectOpinion, error)`（R33 签名扩 goal——评估器须看到原始目标）|
| `WithEvaluator(ag) + WithReflectorAgent(ag)` | 档 2：evaluator 评分 + reflector 改进 |
| `WithEvalFunc(fn EvalFunc)` | 档 3：`func(ctx, output *Message) (bool, error)` (实证 `agent_v2/reflect_builder.go`) |
| `WithReflectMaxRounds(n)` | 迭代轮数硬上限（默认 5）|

`ReflectOpinion` 字段：
```go
type ReflectOpinion struct {
    GoalMatched bool    // true → 接受当前 actor 输出，退出循环
    Opinion     string  // 非空时作为下一轮 actor 输入的 feedback
    Critique    string  // R33：评估器批评全文——feedback 节点喂 reflectorAgent；无 reflectorAgent 时 fallback 作 Opinion
}
```

**档 2（`WithEvaluator(ag)`）evaluator 输出怎么解析为"是否达标"**（`reflect_agent.go::parseGoalMatched`）：① 优先读 evaluator output `Annotations["goal_matched"].(bool)`（业务 evaluator 显式注入最可靠）；② 无该 annotation 时 fallback——content 经 `TrimSpace` + `ToLower` 后**精确全串等于** `yes` / `ok` / `approved` / `true` / `passed` / `good` 之一才视为达标（`switch` case **精确相等，非子串匹配**）。**业务自定义 evaluator agent 应显式设 `goal_matched` annotation**——带解释的输出（如 `"yes, looks good"` / `"not good enough"`）因非精确全串相等都**不**会被判达标，只靠纯关键字极易漏判。

### 9.4 关键不变量

- 三档**互斥**：`Reflector` / `Evaluator(+ReflectorAgent)` / `EvalFunc` 只能配一档——`NewReflectAgent` 构造期 panic 拒绝多档同配（R33 后；compose_v2 builder 低阶层仍是静默优先级 `Reflector > EvalFunc`，agent_v2 高阶层不放行到那一步）
- ✨ **不设 ReflectorAgent 时的反馈退化**——EvalFunc 档评审反馈段为空（只判断不指导）；Evaluator 档 R33 起 fallback 用 evaluator 批评全文（critique）作反馈（原恒留空）。下一轮输入是框架 synthesize 的模板（原始目标 + 上一轮回答 + 评审反馈 + 请改进），非"原 input 原样重发"
- ✨ **跨轮 HITL** — actor / 评估链任一抛 InterruptError 都被 cp 持久化；**actor 侧**有 nested_smoke 场景验证；评估链侧：R33 拆图（actor→evaluate→cond→[End|feedback]）后 verdict 在 evaluate 超步 commit——**reflectorAgent 内中断 Resume 只重跑 feedback 节点，evaluator 不重评**（拆图前整链跑单 eval 节点、Resume 重评 LLM 非确定 verdict 翻面会丢用户答案——TestR33_ReflectSplit_FeedbackInterrupt_NoReeval 锁此协议）

### 9.5 反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| Reflector 内部又调 ReflectAgent | 死递归——Reflector 应该是终止评估，不是再发起一轮 |
| 不设 MaxRounds | 默认 5；想长链业务必须显式 `WithReflectMaxRounds(N)` |
| Actor 输出无任何评估信号 | Evaluator agent / EvalFunc 必须能从 actor 输出判定 GoalMatched |
| 用 `agent_v2.WithActor(ag)` 当全局 option | `WithActor` 是 `(*ReflectAgent).WithActor` 链式方法，必须 `NewReflectAgent(...).WithActor(ag)` |

---

## 10. GraphAgent — 自定义 Graph 包成 Agent

### 10.1 适用场景

- 上面 8 编排都不够灵活——业务需要完全自定义节点 + 边拓扑
- 自定义状态机 / 复杂 fan-out 模式 / 实验性 graph 设计

### 10.2 完整最小例

```go
import "crew-go/compose_v2"

// 1. 自定义 Graph
var inputCh  = compose_v2.LastValue[*schema.Message]("input")
var outputCh = compose_v2.LastValue[*schema.Message]("output")

bld := compose_v2.NewBuilder().WithName("my_graph")
bld = compose_v2.WithChannel(bld, inputCh)
bld = compose_v2.WithChannel(bld, outputCh)
g, _ := bld.
    AddNode("step1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
        in, _ := compose_v2.Get(chs, inputCh)
        out := process(in)
        return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, out)}, nil
    }).
    AddEdge(compose_v2.StartNode, "step1").
    AddEdge("step1", compose_v2.EndNode).
    Compile()

// 2. 包成 Agent
ga := agent_v2.NewGraphAgent(g,
    agent_v2.WithGraphAgentName("my_agent"),
    agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
        return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
    }),
    agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
        out, _ := compose_v2.Get(chs, outputCh)
        return out
    }),
)

events, err := ga.Run(ctx, sess, schema.UserMessage("hello"))
```

### 10.3 关键 Options

| API | 用途 |
|---|---|
| `NewGraphAgent(graph, opts...)` | 包装任意 Graph 为 Agent |
| `WithGraphAgentName(name)` | 标识 |
| `WithGraphAgentDescription(desc)` | 自描述 |
| `WithGraphInputWriter(fn)` | 把 input message 转成 channel writes（必填）|
| `WithGraphOutputReader(fn)` | 从 channels 提取 final message（必填）|
| `WithGraphAgentHooks(h)` | 业务级观测 |

### 10.4 关键责任：嵌套子 agent 必须走 RunSubAgent

> ⚠️ **GraphAgent 用户责任**：自定义 NodeFunc 内调 sub-agent.Run 时**必须走
> `agent_v2.RunSubAgent`**——它一站式 push 三件 ctx（graphNS + NewSharedChildSession +
> storeScope），漏任一件都是静默 bug（events 串台 ISSUE-79 / 业务 store 后端按 storeScope
> 做的区分失效——框架自己已不读这个标记 / 子 cp 落进父槽）。

```go
nodeFunc := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    events, err := agent_v2.RunSubAgent(ctx, "child", childAgent, input)
    // ↑ 内部：childNS（含实例维，见下）+ NewSharedChildSession + WithSubAgentStoreScope
    // ...
}
```

**这里曾给过一份手拼 ns 的「手动控制」配方，已撤。** 原因不是风格偏好——一次嵌套调用的身份是
**位置**（调用方所在的节点与相位 + 实例维三选一，见下表）加上**这是哪一次调用**（不带 key 时是输入
内容 + 同一份输入在本次执行里的第几次；带 `agent_v2.WithCallKey` 时是 key），存档槽由身份哈希而来。
那段判定住在 `RunSubAgent` 里，手拼等于复制一份会漂的拷贝：

| 本次调用处于 | 位置那一维 |
|---|---|
| 图级 fan-out 实例（`SendFrom` 同 Target 投递出的一个） | 实例下标 |
| 某个 tool_call 里（同一 tools 节点的并发 tool_call） | tcID + 批内下标 + tools 节点访问序号 |
| 静态 / cond 路径 | 本节点已成功完成的访问次数（visit） |
| fan-out 实例里业务又自己起了 tool_call 并发 | 实例下标 + tcID + 批内下标——两维都进身份，**不叠 visit**（visit 按节点 ID 共享、会被静态兄弟推高，实例续跑时身份会漂） |

另外每种情况都带上**调用方在哪**：所在节点与相位（节点体 / 某个图钩子 / 某条条件边；节点级钩子与
条件边的相位名含所属节点与它的第几次执行，图级钩子的含这张图的 ns）。存档槽恒为 `JoinFanoutNS(parentNS, nodeID, 身份哈希的低 63
位)`。同一个 nodeID 下的几次调用是兄弟执行：一次成功时只清自己的子树（沿 `/` 往下），不碰别的调用
还挂着的存档。

漏实例维 ⟹ N 个实例撞同一个 childNS、子 cp 互覆（ISSUE-96 侧面 2）；漏 visit 维 ⟹
父循环第 2 轮重进时子层算出与第 1 轮**相同的 InterruptID**，被答案账本判为已兑付、终局
`ErrRunAborted`（答案账本跨子图边界之前是第 2 轮的提问从未呈现就被旧答案静默满足）；漏调用方节点 /
相位 ⟹ 两个节点、或一个节点的两次执行在钩子里用同一份输入调同一个子 agent 撞成一次（同样
`ErrRunAborted`）；漏输入内容 ⟹ 同一个节点里带着新输入的第二次调用捡起第一次的失败存档接着跑，
新输入没进图（第一次是确定性失败时以旧错收尾，瞬时失败时拿旧输入的结果当第二次的返回、
`err == nil`）；tool_call 那一维只用 ID ⟹ 模型跨轮复用同一个 ID 时第 2 轮撞上第 1 轮的槽。手拼也
拿不到下面的完成记录。

**父节点重跑时**（中断续跑 / 失败续跑 / `RunWithRetry` 重试），每次调用按身份找回自己的东西：
**已完成的**直接拿回记下的结果（整组事件记在父节点这次执行的私有态，`compose_v2.SaveStepResult`），
不再执行——副作用不重放、问过的不重问，它当初做出的转交与路由决定（发给上层 HandoffAgent /
RoutingAgent 的上行信号）原样交出；**没完成的**续上自己的存档；**新的**从头跑。拿回的是经过存档往返（JSON）的
那一份，读标注按 JSON 类型读。

输入不同的调用之间身份跟次序无关，所以按进度跳过已完成的调用、条件分支插进一次调用都不会让一个
答案落到别的调用上；**输入完全相同的几次**只能按发起次序区分，别自己按进度跳过，要跳过就给每次一个
key。⚠️ 被调方做出的转交与路由决定只在「再调一次、被拿回」时交出，跳过就丢了——有这种决定的调用
照调一遍（已完成的会拿回）。链式调用（起草 → 审批问人）收敛：起草拿回记下的那一版，审批的输入不变、
续上自己的存档。

**输入变了就是另一次调用**：父节点重跑时对同一个位置给了不同的输入 ⟹ 新调用、从头跑、自己问自己
的问题。输入里带着每次都变的值（请求号、时间戳、节点体里直接调模型的产出）时每次重跑都是新调用、
永不收敛——两条出路：

```go
// 1. 声明身份：输入变了照样认作同一次调用（打一条告警）
events, err := agent_v2.RunSubAgent(ctx, "worker", sub, in, agent_v2.WithCallKey(item.ID))
```

```go
// 2. 把会变的值记住：每次执行给出同一份输入。记的是模型的「产出」，标 Rerunnable——
//    节点的条件出边否决、要求重新产出（CondFailRerunFrom）时它跟着重做
draft, ok := compose_v2.GetStepResult[string](ctx, "draft")
if !ok {
    draft = callModelDirectly(ctx) // 每次调都不一样
    if err := compose_v2.SaveStepResult(ctx, "draft", draft, compose_v2.Rerunnable()); err != nil {
        return nil, err
    }
}
events, err := agent_v2.RunSubAgent(ctx, "review", reviewer, schema.UserMessage(draft))
```

**产出不合格要重来**：`RunWithRetry` 重试的是同一次调用、节点返错之后 Resume 重试的是失败的部分——
两者都原样拿回已完成的调用。要一份新产出，在节点里循环调（同一份输入的第 k 次是第 k 次调用、各自
执行），或把校验放到条件边上声明 `CondFailRerunFrom`，并给产出那一次调用声明
`agent_v2.WithRerunnable()`——条件边否决之后重做的是**整次调用**（身份里带重做代际：新的存档槽，被调方在
自己的图之外记下的东西、它问的问题都换新）；没点名的调用（扣款、问过人的把关）照常拿回。条件边被外面
停下（软取消、陈旧写回、调用方 ctx 已经死了）不算否决。重做代际按分域算：节点里某个 tool_call 跑的嵌套图
入口被否决，只让那个 tool_call 换代，兄弟 tool_call 的提问与挂起调用不受影响；经 `RunSubAgent` 调起的 agent
的主图入口被否决时，换代的是调用方那个分域（被否决的是它交出去的输入）。

同一次执行（节点执行 / 一个 tool_call / 一次图钩子或工具钩子调用 / 被调方在自己图外的那一段）里、同一个
位置上两次用同一个 key 报
`ErrRunAborted`（`RunWithRetry` 的重试不算两次；手写的循环每次都算新调用，带同一个 key 就会撞上）。
不带 key 的调用把输入摘要记进被调方存档，续跑时对不上只可能是另一次输入不同的调用撞进了同一个槽
⟹ 拒绝、不清档（`compose_v2.WithInputIdentity`）。钩子相位里的调用同样被记住：`OnNodeStart` /
`OnNodeEnd` / 条件边里的记在那个节点这次执行名下，嵌套图的 `OnGraphStart` / `OnGraphEnd` 与入口
条件边里的记在外层节点执行名下；只有顶层图的图级钩子与入口条件边没有可挂的地方，那里完成的调用在
重跑时再执行一遍（没完成的照样续自己的存档——嵌套调用的被调方属于调用方的任务，见下）。

**被调方属于调用方的任务**：任务的身份是 session 的 RunID。`RunSubAgent` / `AgentAsTool` / skill fork 给
被调方一个 RunID 就是**所在任务**的 session（直接跑 `compose_v2` 图、ctx 里另推了一个 session 时取图的
sid）并替它盖任务章，被调方的入口不声明新任务、续自己的存档——调用方是九个编排之一、业务自研的 agent，
还是框架外业务自己的循环都一样。业务在自己的循环里开新任务照常换 RunID（`sess.NextTask()`；approve_always
规则按 RunID 分，换了就是新的一套）。

交给被调方的输入是一份独立拷贝：`any` 值按原类型递归深拷（切片、数组、map 一路拷下去，值结构体拷导出字段、连同嵌入结构体里的，
拷完类型不变、nil 与空不混），被调方钩子原地改写消息、标注与这些容器不写回调用方；**指针、函数、channel
原样共享**——经指针改会写回调用方，续跑时换身份重问。

`AgentAsTool` 不分是谁调起的：一律记完成记录（只记最终那条消息）；**产出会被 tools 节点写成 tool_result 时不补记
历史**（看 tools 节点盖的章，自写 toolsFn 也算），别处（节点体、钩子里）补记。全历史输入（且没有自定义
schema）的只按位置定身份（输入由框架现拼、续跑时可能不同）；其余按交给被调方的输入定——`OnToolStart` /
中间件注入的每次都变的值落进了输入时（自定义 schema 的整个 args，或改写了 `input` 本身）用
`agent_v2.WithAgentToolCallKey(fn)` 从 args 取一个稳定的 key，不然每次续跑都是新调用、永不收敛。

**在一个正在执行的任务里，session 必须属于这个任务**：`inner.Run(ctx, someSession, in)` 里 session 的 RunID
对不上所在的任务 ⟹ 入口直接拒（`%w ErrRunAborted`，什么都不执行）；Resume 在那样的位置上一律拒。要在节点里
开一个独立任务（给另一个会话、续跑另一个挂起的任务），先 `agent_v2.DetachTask(ctx)`——它剥掉当前执行留在
ctx 上的位置、答案、新开信号、任务章（连同会话 ID、软取消信号），留下 checkpointer、钩子、调用主体这些环境；
独立任务有自己的 RunID，它的存档、观测会话由调用方自己管。脱离之后的 RunID 不能是这一路脱离出来之前所在的
任何一个仍在执行的任务（经 DetachTask 还是直接用 `compose_v2.DetachExecution`、脱离几层都算，见
`compose_v2.DetachedFrom`；「仍在执行」按任务判：任务挂起之后用当时在节点里存下的 ctx 脱离再续跑它不算，它又续跑
起来之后再拿那个 ctx 脱离照样算）；
独立任务挂起时它的中断不能从节点返回（引擎终局拒收——那个挂起只能在顶层用它自己的 RunID 续；原样返回、
取出来返回、再包一层都一样）。

⚠️ **裸调 `sub.Run(ctx, ...)` 完全不 push ns 的形态不能用**：传父的 session（或不传）时子层的图要落进父图自己的
存档槽（节点、工具、图级钩子、9 个编排的 agent 级钩子里都是；经 `EnterRun` 进入的自研壳自己的阶段里入口不拦，靠图身份兜：
BasicAgent 显式起了名字时图身份是「名字:结构指纹」，裸调的与壳透传的那个名字不同时载入撞 `ErrGraphIDConflict`、什么都不执行，
两个都没起名字、结构又相同时照旧接着跑），配了 checkpointer 时 `compose_v2.Run` 入口当场拒（`ErrRunAborted`，子层什么都不执行，正文指向
`RunSubAgent`；哨兵 `agent_v2/bare_nested_run_slot_test.go`、`compose_v2/nested_run_own_slot_test.go`）。曾经放行：
两张图身份不同时 Resume 撞 `ErrGraphIDConflict`，身份相同时（当时 BasicAgent 的图只按结构指纹算身份，任意两个 BasicAgent
的图身份相同）子层接着跑父图的存档、旁路工具多执行一次、成功时把父图的档删掉，`err == nil`。传新 session 时 agent 入口直接拒
（哨兵 `agent_v2/task_scope_test.go` 的 `TestTask_BareRunWithNewSessionInNode_IsRejected`）。

详见 [`channel-engine.md`](./channel-engine.md) §8.1–§8.2（ns 形态与 API）+ CLAUDE.md "GraphAgent 提示"。

---

## 11. 横切能力：Store / Skill / Retry / Hooks

### 11.1 Store（跨 Run 长期记忆）

> **迁移说明**：旧 `agent_v2.Memory` 接口已删（PR 3.3 commit `e6505f9`），统一改走 `store.Store` 接口 + `WithStore` / `WithStoreMemory` helper。完整接口与高/低层用法见 [`store.md`](./store.md)。

```go
st := store.NewInMemoryStore()
ag := agent_v2.NewBasicAgent(
    agent_v2.WithModel(m),
    agent_v2.WithStore(st),
    agent_v2.WithStoreMemory(
        store.NS("users", userID, "memories"),
        &agent_v2.StoreMemoryConfig{SearchLimit: 20},
    ),
)

// Run 1
ag.Run(ctx, sess1, schema.UserMessage("我喜欢 Go"))
// helper postamble 自动调 store.Put 保存本轮 user/assistant pairs

// Run 2（同 sess？还是新 sess 复用 store？业务决定）
ag.Run(ctx, sess2, schema.UserMessage("我刚才说我喜欢什么？"))
// helper preamble 自动调 store.Search 注入历史 → LLM 知道用户喜欢 Go
```

**关键**：
- ✨ helper preamble 注入的消息打 `StoreInjectedAnnotation` marker——AutoCompact 跳过不重摘
- ✨ Store 是**跨 Run** 长期记忆，与 cp（per-Run 状态快照）正交
- ✨ **第 0 层兜底截断**：业务想限制单 Run 历史长度可加 `WithMaxHistoryPairs(N)` 或 `WithHistoryCompactor(fn)`——比 4 层压缩更轻量
- 业务可实现 `store.Store` 接口接 Redis / Postgres / Vector DB（见 `store.md` §6 低层用法）

### 11.2 Skill（动态能力激活）

```go
import "crew-go/skill"

sb, err := skill.NewDefaultSkillBox(skillsDir)  // 扫描 rootDir/*/SKILL.md（**返两个值含 error**）
if err != nil { /* handle */ }

ag := agent_v2.NewBasicAgent(
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(myToolkit),
    agent_v2.WithSkills(skill.SkillSource{SkillBox: sb}),  // ← 类型是 skill.SkillSource 不是 agent_v2.SkillConfig
)

// 框架自动：
// 1. system prompt 末尾追加 hint 句（不是 skill 列表——列表在 activate_skill 工具的 Description 里动态拼）
// 2. Toolkit 自动注入 activate_skill 工具，Description 列出所有候选 skill
// 3. LLM 调 activate_skill({skill: <name>}) → 框架按 LLM 给的 name 直接激活
//    （注：box.MatchSkills 关键词预筛 V1 不被 activate_skill 调用，业务可单独使用）
```

> **完整 skill 文档**：定义（SKILL.md + frontmatter）/ 3 激活模式（toolCall / adaptive / pre_match）/ body 注入 4 路径 / fork + per-skill model / 二维可见性控制 / 4 层压缩协作 / HITL —— 全部见 [`skill.md`](./skill.md)（权威源，经 5 轮 audit）。本节只给编排视角的最小接入 + 定位，细节不重复（避免跨文档同病 stale）。

**skill 在编排里的定位**：
- ✨ 与 Store / Retry / Hooks 并列的**横切能力**——任意编排嵌的 BasicAgent 都可 `WithSkills` 接入
- ✨ **progressive disclosure**：skill metadata 轻量常驻供 LLM 判断要不要用，body（完整 playbook）按需激活才进 ctx，省 token
- ✨ 默认 **toolCall 模式**（LLM 调 `activate_skill` 主动激活）；`WithSkillSelector` 切 adaptive / pre_match 预激活（见 [`skill.md` §5](./skill.md)）
- ✨ `context: fork` 让 skill body 跑成隔离 sub-agent + per-skill model 切换（见 [`skill.md` §6.3 / §8](./skill.md)）
- ✨ body 注入 / 二维可见性 / 4 层压缩协作细节 → [`skill.md`](./skill.md) §6 / §5.3 / §9

### 11.3 Retry（编排级重试）

7 编排（Plan / Loop / Sequential / Parallel / Reflect / Routing / Handoff）都接 `WithXxxRetryConfig` 选项：

```go
loop := agent_v2.NewLoopAgent(
    agent_v2.WithLoopRetryConfig(compose_v2.ExponentialBackoff(3, 1*time.Second, 10*time.Second)),
).WithBody(myAgent)
```

关键不变量：**InterruptError + ctx.Err() 永不重试**（详见 [`channel-engine.md`](./channel-engine.md) §11.3）。

### 11.4 Hooks（业务观测 + OrchHooks 引擎观测）

业务级 `hook.Hooks`（8 callbacks）：

```go
type myHooks struct{ hook.NoOpHooks }

func (h *myHooks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
    log.Println("agent start:", info.Name, "round:", info.Round)
    return ctx, nil
}
func (h *myHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, output *schema.ModelOutput) (context.Context, error) {
    log.Println("model output tokens:", output.Usage)
    return ctx, nil
}

ag := agent_v2.NewBasicAgent(
    agent_v2.WithHooks(&myHooks{}),
)
```

引擎级 `compose_v2.OrchestrationHooks`（5 callbacks，主要前端 SSE 用）见 [`channel-engine.md`](./channel-engine.md) §10。

---

## 12. 嵌套组合范式（典型业务）

### 12.1 PlanAgent + 多 BasicAgent expert（最常见）

```go
researcher := agent_v2.NewBasicAgent(agent_v2.WithName("researcher"), agent_v2.WithDescription("..."), agent_v2.WithModel(m), agent_v2.WithToolkit(searchTk))
summarizer := agent_v2.NewBasicAgent(agent_v2.WithName("summarizer"), agent_v2.WithDescription("..."), agent_v2.WithModel(m))
translator := agent_v2.NewBasicAgent(agent_v2.WithName("translator"), agent_v2.WithDescription("..."), agent_v2.WithModel(m))

subAgents := map[string]agent_v2.Agent{"researcher": researcher, "summarizer": summarizer, "translator": translator}
plan := agent_v2.NewPlanAgent(
    agent_v2.NewLLMPlanner(m, agent_v2.WithLLMPlannerSubAgents(subAgents)),
    agent_v2.WithSubAgents(subAgents),
)
```

### 12.2 Sequential 流水线（每步嵌 BasicAgent）

```go
seq := agent_v2.NewSequentialAgent().
    AddStep("research",  basicAgentResearcher).
    AddStep("summarize", basicAgentSummarizer).
    AddStep("translate", basicAgentTranslator)
```

### 12.3 ReflectAgent + actor BasicAgent

```go
writer    := agent_v2.NewBasicAgent(agent_v2.WithName("writer"), agent_v2.WithModel(m))
critic    := agent_v2.NewBasicAgent(agent_v2.WithName("critic"), agent_v2.WithModel(m), agent_v2.WithSystemPrompt("评估文章质量..."))
reflector := agent_v2.NewBasicAgent(agent_v2.WithName("reflector"), agent_v2.WithModel(m), agent_v2.WithSystemPrompt("基于 critic 反馈生成 actor 改进 prompt..."))

// 三档评估：Evaluator + ReflectorAgent 让 actor 拿到具体改进方向
reflect := agent_v2.NewReflectAgent(
    agent_v2.WithEvaluator(critic),
    agent_v2.WithReflectorAgent(reflector),  // ← 不设时 actor 拿不到具体改进 prompt
    agent_v2.WithReflectMaxRounds(5),
).WithActor(writer)  // ← 链式
```

### 12.4 RoutingAgent + 后端不同 PlanAgent / Sequential

```go
routing := agent_v2.NewRoutingAgent(
    agent_v2.WithRoutingClassifier(classifier),
    agent_v2.WithRoutingDescriptions(map[string]string{
        "complex_research": "需要多专家协作的复杂研究任务",
        "simple_chat":      "简单聊天",
    }),
).
    AddBranch("complex_research", planAgent).   // PlanAgent 嵌入
    AddBranch("simple_chat",      basicAgent)
```

### 12.5 AgentAsTool — Agent 暴露为 tool（LLM 自主调用，区别于显式编排）

`AgentAsTool(a Agent, opts...)` 把任意 Agent 包成 `tool.Tool`，挂到父 BasicAgent 的 toolkit——**LLM 在 ReAct 循环里自主决定何时调用**（区别于 PlanAgent / Sequential / Routing 的显式编排）。适合"主 agent 自主判断要不要委派子任务给专家 agent"。

```go
researcher := agent_v2.NewBasicAgent(agent_v2.WithName("researcher"),
    agent_v2.WithDescription("检索并汇总最新资料"), agent_v2.WithModel(m), agent_v2.WithToolkit(searchTk))

// description 默认取 agent.Description()；input 默认是单 `input` string 参数（由 LLM 填）
researchTool := agent_v2.AgentAsTool(researcher)

main := agent_v2.NewBasicAgent(agent_v2.WithName("main"), agent_v2.WithModel(m),
    agent_v2.WithToolkit(tool.NewToolkit(researchTool)))
```

**3 个 `AgentAsToolOption`**（`agent_tool.go`）：

| Option | 用途 | 默认 |
|---|---|---|
| `WithAgentToolDescription(desc)` | 覆盖 tool description | 取 `agent.Description()` |
| `WithAgentToolInputSchema(schema map[string]any)` | 自定义结构化入参 schema（Call 时把 args 整体 marshal JSON 作 sub-agent input.Content）| 单 `input` string 参数 |
| `WithAgentToolFullHistoryInput()` | 用父 session 完整对话历史（`HistoryFor("")` 拼 single user message，自动过滤 SkillBody/SkillListing marker）作 input | 关（仅 LLM 填的 `input`）|

**互斥语义**：`WithAgentToolInputSchema` 与 `WithAgentToolFullHistoryInput` 互斥，同时设时 **schema 优先**（history 模式被忽略）。用 `InputSchema` 时业务**必在 sub-agent system prompt 提示"你将收到 JSON 格式参数"**——否则 LLM 把 JSON 串当字面 user text 误读。

**构造期 fail-fast panic**（业务 init 期发现，不延迟到 runtime LLM 调 tool 时）：① `a` 是 nil（含 Go typed nil interface 陷阱）；② `agent.Description()` 空**且**未传 `WithAgentToolDescription`——tool 无 description LLM 无法判断何时调用，eino 范式构造期强制而非运行期静默。

### 12.6 关键不变量（嵌套）

- ✨ **任意 9 编排嵌入任意 9 编排**——单 Agent 接口，无 fallback 路径
- ✨ **嵌套 cp 物理分离**——子 cp 写到 `(parentSID, parentNS/childName)` 不污染父
- ✨ **OrchHooks ctx-aware** 嵌套子 agent 自动继承父 hooks（业务不用每层显式传）
- ✨ **HITL Resume 跨任意嵌套深度自动闭环**（nested_smoke 12 场景验证）

---

## 13. 反模式速查

### 13.1 编排选型反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 用 SequentialAgent 处理"步骤数运行时决定" | 改用 PlanAgent |
| 用 ParallelAgent 处理"分支间有依赖" | 改用 PlanAgent + InputRefs |
| 用 LoopAgent 处理"次数固定" | 改用 SequentialAgent 或 PlanAgent |
| 用 RoutingAgent 处理"需要状态机交接" | 改用 HandoffAgent |
| 用 GraphAgent 处理"上面 8 个能解决" | 优先用现成编排——GraphAgent 嵌套 ns 业务自管，复杂度高 |

### 13.2 通用反模式

| ❌ 反模式 | ✅ 正确做法 |
|---|---|
| 嵌入场景 sub-agent 不填 Description | 必填（PlanAgent / RoutingAgent Classifier 依赖此抽取能力描述）|
| 不设 MaxRounds / MaxIterations / MaxSteps / MaxHops | 各编排有默认值兜底，业务想覆盖必显式设 |
| 不设 ctx deadline | fan-out 死锁的唯一外部解药——必给 |
| 业务工具不防重 | Resume 时全部 tool_call 重跑——idempotency_key 设计 |
| 嵌套时手动传 OrchHooks 每层 | 用 `compose_v2.WithOrchHooks(ctx, h)` ctx 透传，自动继承 |
| 嵌套时手动管 cp ns | 框架自动 push graphNS（除 GraphAgent 自定义 NodeFunc 内业务自管）|
| 误以为 `WithSequentialRetryConfig(...)` 是整链失败统一重试 | Sequential retry 是**每步独立计 attempt**——A 步失败重试 N 次成功后，B 步若也失败仍可重试 N 次；想 retry 整链必须业务侧 wrap 顶层 Sequential.Run |

---

## 关联文档

- [`usage-guide.md`](./usage-guide.md) — 业务侧基础用法（builder 速查 + cp + Resume + 4 层压缩配置）
- [`channel-engine.md`](./channel-engine.md) — 引擎层完整设计（§12 9 编排编译产物）
- [`hitl-resume.md`](./hitl-resume.md) — HITL + Resume 完整协议
- [`context-compaction.md`](./context-compaction.md) — 4 层上下文压缩
- [`migration-history.md`](./migration-history.md) — v2 完整 API 速查 + Stage 1-7 commit 索引
- 实战 demo：`example/orchestration/plan_chat_web/`（HITL Web 服务，PlanAgent + 多 sub-agent + 用户暂停按钮）
- 实战 demo：`example/orchestration/nested_smoke/`（13 嵌套 HITL Resume 场景）
- 实战 demo：`example/orchestration/channel_smoke/`（5 channel-based 引擎核心场景）
