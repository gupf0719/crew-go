---
id: ISSUE-54
title: PlanStep.InputRefs 解析时机修正（从 Planner 闭包搬到 PlanAgent.execNode）
status: ✅ 已实现
priority: 高
created: 2026-04-27
resolved: 2026-04-27
source: plan_chat_web demo 实跑触发的 bug → 顺藤摸到 ISSUE-47 InputRefs 设计的职责错位
---

# ISSUE-54 · InputRefs 解析时机修正

## Bug 现象

跑 `go run ./example/orchestration/plan_chat_web -addr=:8080` 真 LLM 模式，输入"写一段 200 字的 Go goroutine
调度器介绍"，SSE 立即返回错误：

```
plan agent: node "plan": planner: planner: step 1 refs: element 0: agent "researcher" not in executed
```

LLM 输出的初始计划：

```json
[{"desc":"查资料","agent":"researcher","refs":[]},
 {"desc":"基于资料写段落","agent":"writer","refs":["researcher"]},
 {"desc":"审查段落","agent":"critic","refs":["writer"]}]
```

step 1（writer）的 `refs:["researcher"]` 想引用同一批的 step 0（researcher）—— 但 closure
内的 `resolveRefs` 在解析时检查 executed 数组（首次规划时为空），researcher 不在里面 → 报错。

## 第一层 root cause（症状层）

LLM 学着 LLMCompiler 风格写 sibling refs（"step 1 引用 step 0"），但我们的 `resolveRefs` 闭包
要求 refs 只能指向**已 executed** 的步骤。同批 sibling 不在 executed 里，立即翻车。

`refsPromptHelp` 给的示例本身就是错的：

```
示例：[{"desc":"查资料","agent":"researcher","refs":[]},{"desc":"基于资料写段落","agent":"writer","refs":["researcher"]}]
```

LLM 看着示例学，命中 sibling refs，自然报错。

mock 模式手写代码守规矩：注释里明确"同一 plan 返回的后续步骤之间不能相互 ref"。真 LLM 没这
约束，照写就翻。

## 第二层 root cause（架构层 · 真正要修的）

第一层只是表象。再往下问"为什么会犯这个错"——答案是**职责错位**：

| 谁知道完整状态 | 谁在做 ref resolution |
|---|---|
| **框架（PlanAgent.execNode）**：知道 executed 数组、当前正要跑哪步、ExecutedStep.Agent 一一对应 | ❌ 之前不做 |
| **用户闭包**：拿到的是 Planner 调用时刻的 executed 快照 —— 对 step[0] 而言 OK，对 step[1:] 而言**永远是 stale** | ✅ 之前在做 |

PlanAgent 的真相是 **`remaining[0]` 才是真正会执行的**（`compose/plan.go` 取 `remaining[0]`），
后续 step[1:] 一律丢弃，下一轮 replan 时 LLM 重新生成。所以 step[1:] 的 refs 在闭包里**永远
不该被校验** —— 因为它们永远不会被执行，也就永远不需要解析成 executed 下标。

闭包在 stale snapshot 上做 step[1:] 的校验，本身就是把**只有框架知道完整状态**的事强行
搬到用户代码里做，必然在某些组合下错。错误信息 `agent "researcher" not in executed` 看起来
像"researcher 没注册"，其实是"当前 executed 数组里没有"——双重误导。

## 修复方案：把 ref resolution 搬进框架

### 类型层

```go
// 改前
type PlanStep struct {
    ...
    InputRefs []int  // 闭包必须当场解析成 executed 下标
}

// 改后
type PlanStep struct {
    ...
    InputRefs []RefSpec
}

type RefSpec struct {
    Index     int    `json:"index,omitempty"`
    AgentName string `json:"agent_name,omitempty"`
}

// 构造助手
func IndexRef(i int) RefSpec     { return RefSpec{Index: i} }
func AgentRef(name string) RefSpec { return RefSpec{AgentName: name} }
```

`RefSpec` 是"待解析的引用"，闭包只负责把 LLM 输出的 int / string 包成 `RefSpec` 即可，不做
任何 executed 关联校验。

### 解析逻辑

新增公开 API：

```go
func ResolveInputRefs(refs []RefSpec, executed []ExecutedStep) ([]int, error)
```

错误语义：
- `AgentName` 在 executed 里查不到 → 返回 error（拼写错或 LLM 凭空捏造，必须暴露）
- `Index` 越界（< 0 或 >= len(executed)）→ `slog.Warn` 后跳过（保留旧 graceful 行为，handle
  LLM hallucinate index）

### 调用时机

`PlanAgent.execNode` 在即将执行 `currentStep` 之前调用：

```go
resolvedRefs, refErr := ResolveInputRefs(currentStep.InputRefs, ps.Executed)
if refErr != nil {
    return state, fmt.Errorf("plan step %d (%q): resolve refs: %w", ...)
}
// 此刻 executed 是该 step 真正能看到的状态
```

此时 executed 是该步骤实际执行那一刻的真实状态，agent name lookup 是对的。

### ExecInputBuilder 签名同步更新

```go
// 改前
type ExecInputBuilder func(ctx, original, plan []PlanStep, executed, current PlanStep) *schema.Message

// 改后（drop 死参数 plan，add resolvedRefs）
type ExecInputBuilder func(ctx, original, executed, current PlanStep, resolvedRefs []int) *schema.Message
```

builder 直接消费 `resolvedRefs []int` 取 executed[idx].Result，不再触碰 `RefSpec`。

## 修复带来的好处

1. **sibling refs 不再翻车**：step[1:] 永远不被 resolve（它们永远不进 execNode）。step[0]
   resolve 时 executed 是对的。
2. **错误时机正确**：错误在"该 step 要跑了，但 ref 解析失败"时报，不在"闭包刚解析 LLM 输出"
   时报；错误信息携带的是真实状态，不是 stale snapshot。
3. **闭包退化为纯 JSON 解析**：
   ```go
   for _, it := range items {
       refs := parseRawRefs(it.Refs)  // int → IndexRef, string → AgentRef
       steps = append(steps, compose.PlanStep{Description: it.Desc, ..., InputRefs: refs})
   }
   ```
   不再调用 resolveRefs，不再操心 executed 状态——那是框架的事。
4. **新 Planner 实现者写代码不会再踩同一个坑**：他们物理上没法写错，因为 RefSpec 不要求
   resolved index。
5. **prompt 不再是"安全机制"**：之前 prompt 教 LLM "refs 只能引用 executed"，是为了规避闭包
   校验崩溃；现在崩溃不存在，prompt 降级为"性能建议"（让 LLM 一次只输出 1 步省 token）。
   即使 LLM 完全忽略 prompt 出多步带 sibling refs，框架也不会崩。

## 设计原则提炼

> **只有框架知道完整状态的事，永远不该让用户代码做。**

PlanAgent 的 executed 数组、当前 step 的执行时机、ExecutedStep.Agent 映射——这些都是框架运行
时态信息。让 Planner 闭包在收到 LLM 输出的瞬间就预校验 refs，等于让用户代码用过期快照做框架
的工作。

往后类似的 API 设计要保持警觉：**parse 阶段尽量只做格式转换，validation 推到 execution
阶段在真实状态下做**。

## 与方案 A 的对比（讨论留痕）

讨论过两个方案：

- **方案 A（最小改动）**：闭包只校验 step[0] 的 refs。step[1:] 放过。改动 ~10 行 × 2 文件。
- **方案 C / 本方案（根因修复）**：搬 resolution 进框架。改动 ~150 行 + 测试。

方案 A 缺陷：留下"只校验 [0]，别管 step[1:]" 暗规约。下一个写 Planner 闭包的人会自然地"对每个
item 都校验"——又踩同一个坑。除非每处加大字注释，但那种注释是设计错位的标志。

按 CLAUDE.md "拒绝短期方案只做根因修复" + "项目未上线，破坏性改动随便提"，选方案 C。

## 改动范围

| 文件 | 改动 |
|---|---|
| `compose/plan.go` | 新增 `RefSpec` / `IndexRef` / `AgentRef` / `ResolveInputRefs`；`PlanStep.InputRefs` 改型；`ExecInputBuilder` 签名更新；execNode 加 resolve 调用；`defaultExecInput` 适配 |
| `compose/llm_planner.go` | `parsePlanToolArgs` 把 LLM 输出 int 转成 `IndexRef` |
| `example/orchestration/plan_chat/main.go` | 删 `resolveRefs`；新增 `parseRawRefs`（纯 int/string → RefSpec）；闭包不再做校验；ExecInputBuilder 用 `resolvedRefs` 参数 |
| `example/orchestration/plan_chat_web/main.go` | 同上；plannerSystemPrompt 改示例为 1-step-per-round 范式 |
| `example/orchestration/plan_chat_web/mock.go` | 注释更新 |
| `example/orchestration/plan_chat/scenario_*.go` | 各 scenario 的 PlannerPrompt 示例改为 1-step-per-round（token 优化，非正确性必需） |
| `example/orchestration/plan_chat/main.go` `refsPromptHelp` | 把"硬约束"降级为"节奏建议"，不再撒谎说 sibling refs 报错 |
| `example/orchestration/plan_chat/main_test.go` | TestResolveRefs 删除，新增 TestParseRawRefs |
| `compose/plan_test.go` | `TestResolveInputRefs` 新增；defaultExecInput 测试适配新签名 |
| `compose/llm_planner_test.go` | InputRefs 类型断言改用 `.Index` 字段 |
| `compose/options_coverage_test.go` | ExecInputBuilder 签名更新 |

## 测试

- `compose` 全部单测通过：含 `TestResolveInputRefs` 6 个子用例（nil / int / agent ref latest /
  multiple agent refs / not found error / out of range graceful skip）
- `agent / hook / mcp / skill / example/orchestration/plan_chat` 全部单测通过

## 参考

- [agent/issue-47-planstep-explicit-input-refs.md](issue-47-planstep-explicit-input-refs.md) ——
  InputRefs 设计原文，本 issue 是其 resolution 时机修正
- [compose/plan.go](../../compose/plan.go) — RefSpec / ResolveInputRefs / execNode 改动
- CLAUDE.md "项目特定约定" / "拒绝短期方案只做根因修复"
