---
id: ISSUE-51
title: ReAct/Planner/GraphAgent 内部设计债（测试覆盖中暴露）
status: ✅ 全部已修
priority: 中
created: 2026-04-24
resolved: 2026-04-27
source: 单元测试覆盖扩展过程（hook/compose/agent 包覆盖从 ~70% 提升到 80%+/90%+）中回溯整理
---

# ISSUE-51 · ReAct / Planner / GraphAgent 内部设计债

## 背景

在单元测试覆盖扩展过程中（`agent` 包 76.5% → 90.3%，`compose` 包 69.9% → 80.3%），除了修复的一个具体 bug（`tc.ID` → `toolInput.CallId`，已直接修复）之外，过程中还暴露出若干结构性设计问题。这些问题不会立即导致功能异常，但会在未来演进（新 hook 类型、新 Planner 策略、自定义 NodeFunc）时以隐蔽的方式爆炸，值得在一个 issue 里统筹处理。

## 问题清单

### 1. Hook 消费语义在 `executeRound` 内部没贯彻

CLAUDE.md 写得很清楚："runtime 须消费 hook 指针入参的任何修改"。但被修复的 `tc.ID` 笔误（[agent/react.go:313](../../../crew-go/agent/react.go)）正是这条规约的漏洞——同一个函数里，OnToolStart 的错误分支消费 `toolInput.CallId`，JSON 解析错误的分支却读 `tc.ID`（原始值）。

**根因**：`executeRound` 742 行，hook 后的字段该读哪个全靠作者记性。没有类型系统或测试规约把"hook 后只能读 `toolInput.*`、不许回头引用 `tc.*`"钉住。

**建议**：
- 在 `OnToolStart` 调用之后，把 `tc` 立刻置为 nil（或用块作用域屏蔽），让误读编译失败；
- 或将这段提成独立函数，入参只传 `toolInput`；
- 添加一条"hook 消费覆盖测试"断言所有错误路径的 `tool_call_id` 都取自 hook 改写后的值。现状：`react_coverage_test.go` 对 OnToolStart/OnToolEnd 两路有针对性断言；JSON 解析分支虽被 `TestTool_InvalidArgs_ContinuesWithErrorMessage` 执行到，但只断言最终 `FinalMessage.Content`，未断言 tool_result 的 `tool_call_id` == hook 改写后的 `toolInput.CallId`——缺一个回归哨兵，若将来回退到读 `tc.ID` 测试不会红。

### 2. `executeRound` 职责过载

一个函数承载：model 调用、tool 循环、hook 触发（8 种）、memory 注入、history compact、session stamping、runID stamp、skill 路由。**743 行测试代码只为覆盖这一个函数**，是明确的复杂度信号。

**影响**：
- 新增 hook 类型需要改这一处巨型函数，改坏概率高；
- 局部替换（比如让 tool 循环支持并发）几乎不可能不动 model 调用；
- CLAUDE.md 已列出"OnModelEnd vs OnAgentEnd"这种坑，正是职责混杂的副作用。

**建议**：按阶段拆分——
```
executeRound
 ├── prepareInput(msgs, memory, compactor) → promptMsgs
 ├── callModel(promptMsgs, hooks) → response
 ├── runToolLoop(response, toolkit, hooks) → toolResults
 └── finalizeRound(response, toolResults, session)
```
每段独立可测。

### 3. Planner 错误路径只用字符串

`NewLLMPlanner` 的所有错误路径（`parse plan args` / `empty steps` / `unexpected tool` / `parse respond args`）都是 `fmt.Errorf` 字符串。我在测试里不得不用 `strings.Contains(err.Error(), "empty steps")` 分流。

**影响**：
- 调用方想针对"LLM 返回空 steps"做降级（例如 fallback 到单步模式），只能靠字符串 match，极脆弱；
- 国际化/重写错误消息会悄悄打破调用方的分流。

**建议**：提几个哨兵错误，仅在下游需要分流的场景——
```go
var (
    ErrPlannerEmptySteps  = errors.New("planner returned empty steps")
    ErrPlannerBadToolCall = errors.New("planner emitted unexpected tool")
)
```
`fmt.Errorf("parse plan args: %w", err)` 仍保留字符串包装，但底层可 `errors.Is`。

### 4. `GraphAgent.Run` 的 nil 兜底过于宽容

当前实现（`compose/graphagent.go`）对 NodeFunc 返回的 State 做三层兜底：
1. `LastOutput != nil` → 用它；
2. `Messages` 非空 → 用最后一条；
3. 两者都空 → 合成空 assistant 消息。

**问题**：默认允许 NodeFunc "什么都不设"，等于弱化了 NodeFunc 契约。自定义拓扑里只要有一个节点忘设 `LastOutput`，错误会静默传递到上层 FinalMessage。

**建议**：
- 保留"fallback 到 Messages 最后一条"，但第三层"空 assistant"应改为明确 error（或至少 OnError 记事件）；
- 在 GraphBuilder.Validate 阶段检查 EndNode 前驱节点是否保证会设 LastOutput（静态不可判，但可要求声明）。

### 5. Memory Read/Write 错误只记 OnError 不中断

`reactLoop` 里 Memory Read/Write 失败通过 `OnError(code="memory_read_error")` 记录但不返回 error，继续跑。

**问题**：设计上 Memory 是"软依赖"，但这个选择没写在 `Memory` 接口 godoc 里。用户实现 Memory 时可能以为失败会阻断 run（例如数据库完全宕机却继续跑，用户没做降级处理）。

**建议**：
- 在 `Memory` 接口 godoc 里明写"Read/Write 错误由 runtime 记录为 OnError 但不中断主流程，用户想硬停需在 hook 里显式返回 error"；
- 或提供 `StrictMemory` 包装器，把错误升级为中断。

### 6. `AgentAction.CustomizedAction any` 持久化丢类型（ISSUE-40 已知）

Resume 后 `CustomizedAction` 因 `any` 反序列化丢类型，已用 `json:"-"` 跳过。**问题是这个"静默丢失"没有 runtime warn**——用户把自定义信号塞进 CustomizedAction，Interrupt 存档再 Resume 后该字段变 nil，不看 CLAUDE.md 不会知道。

**建议**：
- Save Checkpoint 时若任何 AgentEvent 的 CustomizedAction != nil，打 `slog.Warn("CustomizedAction will not survive Resume, consider promoting to a typed AgentAction field")`；
- 或在 `RunGraph` 启动前扫一遍已注册 hook，若用户明显依赖 CustomizedAction 就提示升格为专属字段。

## 优先级建议

| 子问题 | 影响面 | 修复成本 | 建议顺位 |
|---|---|---|---|
| #1 hook 消费规约 | 高（已出过 bug） | 低 | **先做** |
| #3 Planner 哨兵错误 | 中（调用方分流） | 低 | 先做 |
| #5 Memory 软依赖文档 | 中（用户理解） | 极低 | 先做 |
| #4 GraphAgent nil 兜底 | 中（契约弱化） | 中 | 次做 |
| #6 CustomizedAction warn | 低（ISSUE-40 已标） | 低 | 次做 |
| #2 executeRound 拆分 | 高（长期演进） | 高 | 最后做，作为独立重构 issue 推 |

## 交付物

- [x] **#1**（2026-04-27）：把 tool-call 循环体提成 `executeToolCall(ctx, cfg, info, toolInput, ...)` helper，函数签名不接 `schema.ToolCall` —— 物理隔离了"hook 后回头读 `tc.X`"的笔误，编译期失败。同时在 `react_coverage_test.go` 新增 `TestTool_InvalidArgs_HookRewritesCallId_RegressionGuard`：hook 改写 `toolInput.CallId` 后，JSON 解析失败分支的 tool_result 必须用改写后的值。
- [x] **#3**（2026-04-27）：新增 4 个哨兵错误 `ErrPlannerNoToolCall` / `ErrPlannerEmptySteps` / `ErrPlannerBadToolCall` / `ErrPlannerParseArgs`，`llm_planner.go` 全路径 `%w` 包装；调用方用 `errors.Is` 分流。原依赖 `strings.Contains` 的 4 个测试切到 `errors.Is`。
- [x] **#5**（2026-04-27）：`Memory` 接口 godoc 增补"# 错误语义（软依赖）"段落，明写 Read/Write 错误只走 `OnError` 不中断；Read/Write 方法注释也加了交叉引用。给出"想升级为硬中断"的两种实现路径（hook 内 return error / 自定义 Memory 包装）。
- [x] **#4**（2026-04-27）：`GraphAgent.Run` 第三层兜底（`LastOutput == nil && Messages == nil`）改为返回 `terminal NodeFunc produced neither LastOutput nor Messages —— NodeFunc 契约违规` 错误，不再静默合成空 assistant。原测试 `TestGraphAgent_Run_LastOutputNil_MessagesEmpty` 改为断言报错。按 CLAUDE.md "项目未上线"约定直接默认 error，不引入 option flag。
- [x] **#6**（2026-04-27）：新增 `agent.MaybeWarnCheckpointSerialization(ctx, cp)` 公共函数；`InMemoryCheckpointer.Save` 序列化前调用。扫到任何 `AgentEvent.Action.CustomizedAction != nil` 就 `slog.Warn` 提示该字段不会跨 Resume 存活，附 `checkpoint_id` / `affected_events` / `hint`。`Checkpointer` 接口 godoc 加规约要求外部实现也调用此函数。新增 2 条单测 `TestMaybeWarnCheckpointSerialization_*` 覆盖命中/不命中两路。
- [x] **#2**（2026-04-27 通过 ISSUE-53 落地）：`executeRound` 拆 3 个 helper（`callModelStep` / `runToolCalls` / `emitFinalAnswerEvents`），主体 110 → 43 行（实际逻辑 ~20 行）。`defer OnAgentEnd` 留顶层、`outMessages` append 留主体；不打包 `roundDeps` struct（违反 ctx 只传不存）；不回传重绑后 chatModel。reactLoop 准备阶段顺序线性不拆。全量单测零改动通过。详见 [issue-53-react-loop-split.md](./issue-53-react-loop-split.md)。

## 参考

- [agent/react.go](../../../crew-go/agent/react.go) — executeRound 主体
- [compose/llm_planner.go](../../../crew-go/compose/llm_planner.go) — Planner 错误路径
- [compose/graphagent.go](../../../crew-go/compose/graphagent.go) — nil 兜底
- CLAUDE.md "Hook 消费语义" 规约
- ISSUE-40 · CustomizedAction 持久化选择
