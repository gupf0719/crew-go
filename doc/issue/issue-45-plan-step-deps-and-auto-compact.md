---
id: ISSUE-45
title: PlanAgent replan-每步 + MaxHistoryPairs token-based 自适应压缩
status: ✅ 已实现（A.replan-每步，2026-04-18）/ 🔲 待设计（B.token-based compact）
priority: 中
created: 2026-04-17
updated: 2026-04-18
depends: ISSUE-44
---

# ISSUE-45 · PlanAgent replan-每步 + History 自适应压缩

## 背景

ISSUE-44 后 runtime 的 LLM 输入切到 `SelfHistory`，sub-agent 不再看其他 agent 的
自动 replay。暴露出两个独立但都涉及 "input 上下文规模控制" 的长期议题。

---

## A. PlanStep 显式依赖声明

### 问题

Plan 模式当前默认 `defaultExecInput` 只注入"上一步产出"。线性链合理，但
plan 非严格线性依赖时不对：

实测 plan_chat 轮 2 规划：

```
1. [researcher] 收集原理和机制资料
2. [writer]     整理优点和实际表现
3. [critic]     分析不足
4. [researcher] 编写代码示例         ← 实际依赖 step 0，不依赖 step 2 (critic 审稿)
5. [writer]     汇总
```

step 3 researcher 的"上一步"是 critic 审稿意见，语义不相关。planner 能编排出
DAG 形态的计划，但 `PlanStep` 没地方声明依赖，executor 只能盲取 `executed[-1]`。

### 参考：eino 的真实设计（直接读源码确认，非二手总结）

eino `adk/prebuilt/planexecute/plan_execute.go` 的核心结构：

#### 1. Plan 无任何依赖字段（`plan_execute.go:77-81`）

```go
type defaultPlan struct {
    Steps []string `json:"steps"`   // 纯字符串数组
}
func (p *defaultPlan) FirstStep() string {   // 永远取索引 0
    if len(p.Steps) == 0 { return "" }
    return p.Steps[0]
}
```

#### 2. Executor 注入全量 context（`plan_execute.go:566-584`）

```go
ExecutorPrompt.Format(ctx, map[string]any{
    "input":          formatInput(in.UserInput),            // 原始目标
    "plan":           string(planContent),                    // 剩余计划
    "executed_steps": formatExecutedSteps(in.ExecutedSteps), // 全部已完成
    "step":           in.Plan.FirstStep(),                    // 当前要执行的一步
})
```

#### 3. **核心：replan-每步**（这是 eino 的真正机制）

执行主循环不是"规划一次 + 线性推进"，而是：

```
planner 首规划 → executor.FirstStep() → replanner(input, plan, executed)
  → executor.FirstStep() → replanner(...) → ... → replanner 调 respond tool 收尾
```

每执行完一步，**Replanner 重新生成剩余计划**（ReplannerPrompt 明确要求
`Contains ONLY remaining steps (exclude completed ones)` + `Adapt strategy
based on new information`）。下一步要做什么由 replanner 每次重新判断，而不是
planner 一次定死。

#### 4. Planner prompt 硬约束"无依赖"（`plan_execute.go:156, 216`）

```
- **Independently executable**: Can be performed by an agent
  without dependencies on other steps
```

不是建议，是强约束。依赖信息由 LLM 编进 step description 文本里。

### 设计哲学拆解

eino 不是"全量注入一个 DAG"，是**"动态 replan 替代静态依赖声明"**：
- 不做显式 deps，因为每步之后 replanner 都会重新全局判断
- "step 3 应该依赖谁" 这个决定让 replanner 每次重新做
- linear list of strings 够用，因为 replanner 承担了调整责任

### 最终设计：路线 B（eino 式 replan-每步），统一 Planner 签名

**核心决策：对齐 eino 的"动态 replan 替代静态依赖"哲学，但不照搬 agent 拆分。**

eino 把 Planner / Replanner 拆成两个 `adk.Agent`（各带 `plan` / `respond` tool）——
这是因为 eino ADK 把 agent 作为基本构件，拆开才能组合进 `SequentialAgent(Planner,
LoopAgent(Executor, Replanner))`。我们 `Planner` 是 `func`，不背负 agent 生态负担，
拆成两个函数是纯粹的概念复制没实质收益（见本 issue 讨论记录）。因此统一成单一函数：

```go
type Planner func(
    ctx context.Context,
    session *agent.RunSession,
    original string,
    executed []ExecutedStep,
) (remaining []PlanStep, finalAnswer string, err error)
```

**契约（框架侧）：**
- 首次调用：`executed == nil`，Planner 产出初始计划（`remaining` 非空）
- 后续调用：`executed` 带历史，Planner 决定继续或收尾
- `len(remaining) > 0` → 执行 `remaining[0]`，再次调 Planner
- `len(remaining) == 0` → 终止；`finalAnswer` 非空时作为输出，否则回退到执行结果拼接

**图结构：** `plan → exec → plan → exec → ... → END`（conditional edge 在 plan 节点
判断 `remaining` 是否为空）

### 为什么不显式声明依赖（DependsOn）

eino Planner prompt 硬约束"Independently executable: ...without dependencies on other
steps"——依赖信息藏在 step description 文本里，由 replanner 每步重新判断。我们跟随
同样哲学：非线性依赖场景由 Planner 自己在下一次调用时根据 `executed` 动态决定剩余
步骤，不需要前置 DAG 声明。原先 `DependsOn []int` 方向被废弃。

### 内置默认实现 `NewLLMPlanner`

为把 "plan / respond 决策"的成本对用户藏起来，框架提供内置 Planner：

```go
plan := compose.NewLLMPlanner(model,
    compose.WithLLMPlannerSystemPrompt("..."),
    compose.WithLLMPlannerSubAgents([]string{"researcher", "writer"}),
)
```

内部机制：
- 首次只挂 `plan` tool；重规划挂 `plan` + `respond` 两个
- 通过 tool_call 强制结构化输出（比解析自由 JSON 鲁棒）
- `tool_choice=required` 禁止 LLM 绕过 tool 返裸文本
- 解析 `tool_call.function.name` 映射到 `(remaining, finalAnswer)`

这两个 tool 是"信号 tool"，不真正执行——只借 tool_call schema 让 provider 替我们
强校验结构化输出。

### 想退化成"一次规划 + 线性推进"

不需要框架 flag。用户在 Planner 闭包里缓存首次结果，后续调用按 `executed` 长度切片
返剩余（零额外 LLM 调用）：

```go
var cached []PlanStep
func(ctx, sess, original, executed) ([]PlanStep, string, error) {
    if len(executed) == 0 {
        cached = runInitialPlanning(...)
    }
    if len(executed) >= len(cached) {
        return nil, "", nil
    }
    return cached[len(executed):], "", nil
}
```

`example/orchestration/plan_chat/main.go` 和 `example/adv_07_plan_agent_test.go` 用的都是这个模式。

### 非目标

- 不引入完整 DAG 拓扑 API（已经有 Graph 层做这个），Plan 保持 list of steps 形态
- 不提供 `WithReplanEnabled(bool)` 开关——plan-once 由用户在 Planner 闭包里缓存实现
- 不提供 `PlannerAgent` / `ReplannerAgent` 拆分 API——单一 `Planner func` 覆盖所有场景

---

## B. History 自适应压缩

### 问题

`agent.Config.MaxHistoryPairs` 默认 0 = 不截断不压缩。多轮 plan_chat / handoff
场景下 `SelfHistory` 累积，sub-agent 每次 LLM 调用带越来越长的自己历史入 prompt，
最终触发模型 context 上限 422。

### 临时方案（已落地，2026-04-17）

[`agent/options.go defaultConfig()`](../../agent/options.go) 给
`MaxHistoryPairs` 一个保底值 20：长对话自动 truncate 只保留最后 20 对非系统消息。
`HistoryCompactor` 未设时走 `truncateHistory` 尾部截断，已足够止血。

### 长期方向

轮数（pair 对数）是一个代理指标，真正的瓶颈是 **token 数**——单轮消息可能 1 行
也可能 5K 行，按 pair 截断在长消息场景下仍会 OOM，在短消息场景下又浪费预算。

方案：

1. 引入 token 计数器（按 tokenizer 估算，不是字符数）
2. `Config` 新增 `MaxContextTokens`（例如默认取模型上限的 60%）
3. compactHistory 在触发 token 阈值时：
   - 有 `HistoryCompactor`：调 LLM 做摘要
   - 无 compactor：按 token 从头 truncate，保留 system + tail
4. `MaxHistoryPairs` 作为粗粒度保底保留，不移除

### 开放问题

- tokenizer：每个模型 provider 一套，要不要 `model.TokenCounter` 接口？还是按字符数粗估？
- compactor 默认实现：是否提供一个内置 `LLMSummaryCompactor(model)` 便民函数？
- 流式输出下增量统计 token 的时机

### 非目标

- 不追求完全准确的 token 计数（provider 内部 tokenization 有差异，估算即可）
- 不做 per-message 级别的智能筛选（保留重要、丢弃冗余）——那是更大的议题

---

## 与 ISSUE-44 的关系

ISSUE-44 解耦了"session 审计"和"LLM 输入"，暴露出 SelfHistory 长期累积问题
（B）；也让 `defaultExecInput` 有机会从 eino 风格的"全量注入"改成"上一步产出"
精简默认，同时留出了 deps 声明的扩展空间（A）。两者都是 ISSUE-44 之后的
自然 follow-up。
