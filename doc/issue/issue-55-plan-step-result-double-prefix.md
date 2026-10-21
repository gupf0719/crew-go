---
id: ISSUE-55
title: PlanAgent step result 双重前缀（mapper 与 defaultExecInput 都加"步骤 X（...）"）
status: ✅ 已实现
priority: 高
created: 2026-04-27
resolved: 2026-04-27
source: plan_chat_web 真 LLM 跑 SSE 事件流审查发现
---

# ISSUE-55 · PlanAgent step result 双重前缀

## Bug 现象

跑 `plan_chat_web` 真 LLM 模式，writer 这一步 sub-agent 的 input prompt 里出现重复段落：

```
原始目标：写一段 200 字的 Go goroutine 调度器
你的任务：基于资料写段落：整理 Go goroutine 调度器...

步骤 1（查资料：研究 Go goroutine 调度器...）的产出：
步骤 1（查资料：研究 Go goroutine 调度器...）：Go 语言的 goroutine 调度器...
```

"步骤 1（查资料：研究 Go goroutine 调度器...）" 出现两次——一次是外层标头，一次塞在
result 内容前。后续 critic step 的 input、planner 的 replan prompt（"产出摘要"那一段）
也都有同样问题。

## Root cause

两个地方都在加同一份前缀：

1. **`compose/plan.go` 默认 `stepResultMapper`**：
   ```go
   resultStr = fmt.Sprintf("步骤 %d（%s）：%s", stepIdx+1, currentStep.Description, out.Content)
   ```
   把 LLM 输出包装成 `"步骤 1（查资料...）：<原文>"` 存进 `executed[i].Result`。

2. **`compose/plan.go` `defaultExecInput`**：
   ```go
   fmt.Fprintf(&sb, "\n\n步骤 %d（%s）的产出：\n%s", idx+1, e.Step, e.Result)
   ```
   拼下游 step 的 prompt 时又加一层 `"步骤 1（查资料...）的产出：\n<...>"` 标头。

两层叠起来变成：

```
步骤 1（查资料...）的产出：    ← defaultExecInput 加的
步骤 1（查资料...）：<原文>     ← mapper 加的
```

LLM 上下文里同句话出现两次，token 浪费 + 噪音。Planner 的 replan prompt 里也同款污染。

## 设计原则违反

ISSUE-47 InputRefs 的设计意图是 **Supervisor 范式："框架不猜不提纯，原文透传；需要精简
就插 summarizer step"**。`stepResultMapper` 默认实现给 result 强行加了步骤上下文前缀，
**等于框架在替消费方"猜"上下文格式**——但消费方（defaultExecInput / replan UI）已经从
`ExecutedStep.Step` 字段拿到 description，不需要 result 自带这份信息。

## 修复

让职责单一化：**mapper 只返裸内容，上下文标头由消费方拼接。**

### 默认 mapper 改为裸 content

```go
// 改前
resultStr = fmt.Sprintf("步骤 %d（%s）：%s", stepIdx+1, currentStep.Description, out.Content)

// 改后
resultStr = out.Content
```

### 消费方继续拼自己需要的标头

`defaultExecInput` 不动——它的 `"步骤 X（...）的产出：\n<result>"` 格式合理。

`buildReplanPrompt`（plan_chat / plan_chat_web 的 replan UI 闭包）不动——它已经是 self-format：
```go
fmt.Fprintf(&b, "  [%d] (%s) %s\n      产出摘要：%s\n", i, e.Agent, e.Step, e.Result)
```
之前因为 `.Result` 被 mapper 前缀污染，"产出摘要"那段重复；mapper 修了之后这里自动干净。

### `StepResultMapper` godoc 更新

明写默认行为（裸 content）+ 提示消费方负责拼接前缀。

## 修复后效果

writer 看到的 prompt：
```
原始目标：写一段 200 字的 Go goroutine 调度器
你的任务：基于资料写段落：整理 Go goroutine 调度器...

步骤 1（查资料：研究 Go goroutine 调度器...）的产出：
Go 语言的 goroutine 调度器是一个复杂的系统...
```

planner replan prompt：
```
[0] (researcher) 查资料：研究 Go goroutine 调度器...
    产出摘要：Go 语言的 goroutine 调度器是一个复杂的系统...
```

各干净一次。

## 测试

新增 `TestPlan_DefaultStepResultMapper_NoPrefix` 回归哨兵：
- 跑两步 plan，第一步 stub model 返 `"RAW_LLM_OUTPUT"`
- 第二步 Planner 闭包检查 `executed[0].Result` 必须 == `"RAW_LLM_OUTPUT"`
- 若回退到加前缀，断言会失败

`agent/compose/hook/mcp/skill/example/orchestration/plan_chat` 全部单测通过。

## 设计原则提炼

> **mapper 只做格式转换，不替消费方拼上下文。**
>
> 消费方都能从 `ExecutedStep.{Step, Agent, Result}` 自取所需，mapper 加前缀就是越权——既
> 浪费 token，又制造潜在双重前缀的可能。

延续 ISSUE-54 的精神（"只有框架知道完整状态的事才该框架做"）：mapper 是 transform 函数，
跟 executed 上下文无关，所以**不该**碰上下文格式。

## 改动范围

| 文件 | 改动 |
|---|---|
| `compose/plan.go` | 默认 stepResultMapper 改为裸 `out.Content`；godoc 更新 |
| `compose/plan_test.go` | 新增 TestPlan_DefaultStepResultMapper_NoPrefix |
| 例外：`buildReplanPrompt` 与 `defaultExecInput` | 不动——已经是消费方 self-format 的好榜样 |

## 后端事件流审计（顺手）

ISSUE-55 触发时顺便把整条 SSE 事件流过了一遍，**除本 bug 外其余皆正常**：

- ✅ plan ↔ exec 节点循环节奏正确
- ✅ researcher 内部 ReAct 两轮（tool_call → tool_result → 最终答案），msgs/round 计数正确
- ✅ planner BasicAgent 用直接 JSON 输出（不走内置 LLMPlanner 的 plan/respond 信号 tool）——demo 选择
- ✅ run_id 每轮 LLM 唯一，符合 ISSUE-49 P0
- ✅ path 字段从 PlanAgent name 嵌套到 sub-agent name
- ✅ node_start/end 配对正确，graph_end 收尾干净

## 参考

- ISSUE-47 · InputRefs Supervisor 范式（"框架不猜不提纯"）—— 本 bug 是其精神的违反
- ISSUE-54 · "只有框架知道的事才该框架做" —— 同款职责边界判断的反向案例
- [compose/plan.go](../../compose/plan.go) — defaultStepResultMapper / defaultExecInput
