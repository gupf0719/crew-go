# ISSUE-58 · Interrupt 触发时机语义：立即挂起 vs 等当前步骤完成

**优先级**：中
**状态**：BasicAgent 内部"立即挂起"路径 + HITL 双模式 Resume + ask_user tool（Steps 1-5）已落地（2026-04-29），plan_chat_web 已接入；Step 6（WithPlanConfirmation）+ 并发分支 ResumeReplan 单独立 issue

## 问题描述

`InterruptError` 当前作为 error-as-control-flow 在节点函数内抛出，框架接到后存档挂起。
但以下问题尚未明确设计：

1. **单步场景**：interrupt 是等当前 NodeFunc/agent 执行完毕后挂起，还是立即中止当前步骤？
   目前实现是 NodeFunc 自己 return InterruptError，属于"步骤内部主动退出"——步骤不会被外力打断。
   如果想要"执行到一半强制挂起"，框架目前没有机制。

2. **超步并行场景**：一个超步内多个分支并发跑，其中一个返回 InterruptError，其余分支
   - 是立即 cancel（激进）？
   - 还是等全部跑完再决定是否挂起（保守）？
   - 还是 best-effort 等兄弟跑完但不 cancel（ISSUE-46 现行策略）？

   ISSUE-46 对"业务错"走 best-effort，但 InterruptError 是控制流信号，语义是否应该不同？

## 参考：eino 的设计

eino 的 Graph 执行（`compose/graph_run_param_call.go`）：

- 节点是否被"打断"：eino 依赖 `ctx.Done()`——调用方 cancel ctx 时节点自然退出（前提是节点
  内有 ctx 检查）。框架层面没有"立即 kill 节点"的机制，靠协作式退出。
- 超步并行：eino `graph_run_param_call.go` 里并行分支共享同一个 ctx，一旦某分支出错会
  cancel 整组 ctx，其余分支靠 ctx.Done() 感知并提前退出。InterruptError 在 eino 里是
  `graph.ErrInterrupt`，触发后同样走 cancel-ctx 路径，并行兄弟会被通知退出（但不保证
  立刻停，取决于节点内是否及时检查 ctx）。
- **结论**：eino 的 interrupt 在超步里是"cancel 兄弟，协作退出"，非强制 kill。

## 参考：LangGraph 的设计

LangGraph（Python）的 interrupt：

- `interrupt()` 是在节点函数内调用的原语，效果是抛出一个特殊异常 `GraphInterrupt`，
  框架捕获后**立即**终止当前 superstep 的 checkpoint 并挂起——不会等其他节点完成。
- 并行超步（Send API / fan-out）中，一个节点触发 interrupt，LangGraph 会：
  1. 等待**已经启动**的并行任务完成（不 cancel）——不会强杀。
  2. 但不会再调度新的并行任务。
  3. 存档当前已完成任务的结果，未完成（实际上等它们完成了再存）。
  4. **Resume 时重放整个 superstep**（已成功的部分也会重跑，幂等设计依赖）。
- **结论**：LangGraph interrupt 是"等当前已启动并行任务自然结束，不 cancel，但 resume
  时整个 superstep 重跑"。对幂等性有要求。

## 当前 crew-go 行为

- ISSUE-46 已实现：超步部分失败时 best-effort 等兄弟跑完，只重跑失败分支，成功分支
  从快照回放。
- 但 ISSUE-46 针对的是普通 error（业务失败），InterruptError 被 retry 逻辑跳过
  （ISSUE-57 修复），然后冒泡——**超步里 InterruptError 最终是否也 cancel 兄弟，
  还是 best-effort 等，目前代码路径不清晰，需要 trace 确认**。

## 待决策

| 问题 | 选项 A（激进） | 选项 B（保守/现行） | 推荐 |
|---|---|---|---|
| 超步中一个分支 interrupt，其余分支 | cancel ctx，协作退出 | best-effort 等完再挂起 | **B**，与 ISSUE-46 保持一致，避免数据丢失 |
| Resume 重跑范围 | 整个 superstep（LangGraph 式） | 只重跑 interrupt 分支 | **只重跑 interrupt 分支**，已有 ISSUE-46 快照机制 |
| 单步 interrupt 时机 | 外力 cancel（需 ctx 协作） | 节点自主 return InterruptError | **节点自主**，不引入外力 cancel 复杂度 |

## 需要确认的代码路径

- `compose/graph.go` 超步执行循环中 InterruptError 的传播链
- ISSUE-46 的 `superstep_snapshot` 逻辑是否覆盖 InterruptError 触发的 checkpoint
- ~~`agent/react.go` 里 BasicAgent 抛 InterruptError 时是否已执行完当前 round~~ ✅ 已 trace 并修复，见下

## BasicAgent 内部 InterruptError 路径修复（已落地）

trace 结果：以下三条路径违反"立即挂起"语义，已修复并加回归测试。

| 路径 | 原行为 | 修复后 |
|---|---|---|
| Tool 自身返回 `InterruptError`（`agent/react.go:427-431`） | 用 `fmt.Errorf("...：%s", err.Error())` 包成 string error，类型链断裂，上游 `errors.As` 检测不到——会被当 tool_error 上报 OnError 并冒泡，不进 checkpoint 保存路径 | 进入 err 分支后先 `errors.As(&ie)`，命中直接 `return nil, err` 保留类型链；不调用 OnError，不再 wrap |
| `OnToolStart` hook 抛 `InterruptError`（`agent/react.go:387-394`） | hook 错误被写成一条带错误内容的 tool_result 塞回 LLM，`abortErr` 返回 nil——`runToolCalls` 循环继续跑下一个 toolCall，外层进入下一轮 LLM | hook err 后先 `errors.As(&ie)`，命中直接 `return nil, err` 当 abortErr 冒泡；普通 hook 错误仍走"写 errMsg + 继续"路径 |
| `OnToolEnd` hook 抛 `InterruptError`（`agent/react.go:443-450`） | 同上 | 同上 |

**正确的路径**（无需改动）：

- `OnAgentStart` 抛（plan_chat_web 现行用法）：`react.go:122-124` 直接 return ✓
- `OnModelStart` 抛：`react.go:266-268` 直接 return ✓
- `OnModelEnd` 抛：`react.go:299-301` 直接 return ✓（边界：response 已生成但未 append，Resume 重跑会重复一次 LLM 调用——属"立即挂起"代价）

**回归测试**（`agent/react_coverage_test.go`）：

- `TestTool_ReturnsInterruptError_PropagatesAndHaltsRound`
- `TestOnToolStart_InterruptError_PropagatesAndHaltsRound`
- `TestOnToolEnd_InterruptError_PropagatesAndHaltsRound`

三个测试均构造"两个 toolCall + 第二轮 LLM 响应"，断言：
1. 返回的 err 满足 `errors.As(&InterruptError)`
2. 第二个 tool 不被调用（除 OnToolEnd 场景，第一个 tool 已先执行）
3. `model.Generate` 仅被调用一次（不进入下一轮 LLM）

---

## HITL（user 双向交互）设计扩展

ISSUE-58 修完"立即挂起"之后，HITL 出现两个需求场景：

| 场景 | 触发方 | 期望 Resume 语义 |
|---|---|---|
| **A. 用户主动中断后改方向** | 外部（flag / OnAgentStart hook） | **跳回 plan 节点重新规划**（PlanAgent 专属），把新 user 输入交给 Planner 决定 remaining |
| **B. Agent 主动问 user 后继续** | Agent（专用 `ask_user` tool / plan-confirmation hook） | **重跑被中断节点**，把 user 答案注入到消息流 |

### 三方调研对比（核心借鉴点）

| 维度 | LangGraph | eino | crew-go 现状 |
|---|---|---|---|
| 中断原语 | `interrupt(value)` magic：第一次抛、第二次返回 resume 值 | `Interrupt/StatefulInterrupt/CompositeInterrupt`，含 `InterruptSignal.Address` 树状地址 | `return &InterruptError{Reason, Data}` |
| 用户回答注入 | `Command(resume=X)` → 成为 `interrupt()` 返回值 | `ResumeWithData(ctx, interruptID, data)` / `BatchResumeWithData(map)` | **不支持** |
| 节点判别 resume | 重跑节点时按出现顺序匹配 resume 值 | `GetResumeContext[T](ctx)` 返回 `(isResumeFlow, hasData, data T)` | **不支持** |
| Info / State 分离 | 隐式 | **显式**：Info（前端展示，不持久化）vs State（持久化恢复用） | 混在 `Data any` 里 |
| 并发分支 interrupt | 等兄弟跑完，整 superstep 重跑 | cancel 兄弟（canceledTasks） | best-effort 等兄弟（ISSUE-46 现行） |

### 设计决策（2026-04-28 拍板）

1. **`AskQuery` 作为 `InterruptError` 的字段**（不独立类型）。一个 InterruptError 同时承载"暂停原因"+"问 user 的问题"，Resume 时按 `Query` 是否非 nil 决定要不要消费 answer。
2. **不引入 Address 树**。理由：当前嵌套（GraphAgent + AddAgentNode）语义本就是 inner 整体重跑，单层 NodeID 够用。Address 树是 ISSUE-48 落地后才需要的能力，跟本次 HITL 解耦。
3. **`ask_user` 内置 tool 走 opt-in**（`WithAskUser()`），避免 LLM 在不该问的场景乱调。
4. **顺手补 `compose/graphagent.go:47` 漏传 Checkpointer**——已落地（与 PlanAgent 一致从 ctx 取 Checkpointer 透给 inner RunGraph）。

### 新待决问题（先不做，独立追踪）

**并发分支内的 ResumeReplan 语义**：超步中一个分支抛 InterruptError、用户选 ResumeReplan 时，已完成的兄弟分支产出怎么处理？需要扩展 `executed` 数据结构表达"超步内部分完成"，与 ISSUE-46 superstep_snapshot 强耦合。

本次实现 `WithResumeMode(ResumeReplan)` 时校验中断分支必须是单分支——超步内部触发 ResumeReplan 直接返回明确错误，等真实场景出现再开 issue。

### 设计方案落地（Steps 1-5 完成；Step 6 / 并发分支 deferred）

| Step | 改动 | 状态 |
|---|---|---|
| 1 | `agent/errors.go` 加 `AskQuery`；`compose/checkpoint.go` 在 `interruptPayload` + 单节点 metadata `cpKeyInterruptQuery` 持久化 Query | ✅ |
| 2 | `compose/checkpoint.go` 加 `ResumeOptions{Mode, UserInput, OverrideStartNode, OrchHooks}`；`ResumeGraph` 处理 UserInput append（按 Query.ToolCallID 包 tool_result 或 user 消息）+ OverrideStartNode 跳节点；超步路径显式拒绝 HITL 选项 | ✅ |
| 3 | `compose/resume.go` 新增 `ResumeContext{UserInput}` + `GetResumeContext(ctx)`；`ResumeGraph` 始终注入 ctx value（无论 UserInput 是否 nil）让节点稳定区分常规/resume 流 | ✅ |
| 4 | `agent/resume_options.go` 新增 `ResumeAction` 枚举 + `ResumeConfig` + `WithResumeAction/UserInput/UserText/OrchHooks` 选项；`PlanAgent.Resume(ctx, sessionID, opts ...agent.ResumeOption)` 把三档 Action 映射到 compose.ResumeOptions（Replan→OverrideStartNode="plan"，Abort→Delete checkpoint） | ✅ |
| 5 | `agent/ask_user_tool.go` 内置 ask_user tool；`agent.WithAskUser()` opt-in；react.go executeToolCall 抛 InterruptError 时自动把 `toolInput.CallId` 回填到 `Query.ToolCallID` | ✅ |
| 6 | `compose.WithPlanConfirmation()` plan 节点后预设挂起关卡 | ⏸ deferred（resume 后是否每次 replan 都重新确认 plan 的语义需要单独设计） |

### 落地后的语义约定

- **ResumeContext 是单向通道**：framework→node 的 user 输入注入走 `GetResumeContext(ctx)`，不写入 `session.events`。原因：session 是跨 agent 共享审计流，写进去会被各 agent 内部 prompt 当成"自己人发的"再 echo 回去（实测 Planner 第二次调用 prompt 里看到自己的上次 prompt）。
- **state.Messages 仍 append UserInput**：LLM 通过 messages 自然看到答复（含 tool_result 配对）。Planner 这种基于 prompt 重建的消费方走 ctx 取最新插话。
- **多次 Resume 中的"老 user 注入"**：保留在 state.Messages（持久化），但 ctx 只携带最新一次。下游若需历史用户插话需自行扫 state.Messages。
- **LLMPlanner / 自定义 Planner 的接入**：Planner 闭包通过 `compose.GetResumeContext(ctx)` 取 `rc.UserInput.Content` 塞 prompt 即可（已示例：[compose/llm_planner.go](compose/llm_planner.go) + [example/orchestration/plan_chat_web/main.go](example/orchestration/plan_chat_web/main.go)）。

### plan_chat_web 接入

- `runSlot` 加 `resumeAction` / `resumeText` 字段
- `/api/chat?session_id=...&action=continue|replan|abort&text=...` 三档恢复
- index.html paused 状态：textarea 启用，三按钮 `▶继续` / `↻改方向` / `✕取消`
- 中断事件含 `query.question / options` 时前端额外渲染问题/选项（用于 ask_user tool 路径）
- `aborted` 事件单独显示 ✕ 已取消并清状态

### Step 6 deferred 原因

`WithPlanConfirmation` 需要决策：
1. Resume 后 plan 节点重跑时**何时重新触发 confirmation**——首次 Continue 自动跳过；首次 Replan 改了方向后是否再次触发？
2. 多次 replan 累积的 plan 变化如何与 confirmation 协作（避免每改一次 directive 就再问一次）

需要单独立 issue 设计 plan-fingerprint + confirmation-history。

### 并发分支 ResumeReplan deferred

超步快照 Resume 路径目前显式拒绝 UserInput / OverrideStartNode（[compose/checkpoint.go ResumeGraph](compose/checkpoint.go)）。
原因：超步中一个分支抛 InterruptError 时，已成功的兄弟分支产出怎么处理（丢弃 / 喂 Planner 当 partial executed）需要扩展 `ExecutedStep` 数据结构表达"超步内部分完成"，与 ISSUE-46 superstep_snapshot 强耦合。

### 测试覆盖

- `agent/react_coverage_test.go`：6 条 InterruptError 立即挂起回归（先前修复）
- `agent/ask_user_tool_test.go`：3 条 ask_user tool（ToolCallID 回填、WithAskUser opt-in、不破坏既有 toolkit）
- `compose/checkpoint_test.go`：6 条 HITL 协议（Query 配对、UserInput 注入、OverrideStartNode 跳节点、GetResumeContext 在 resume 内可见、首次 Run 不可见、超步路径拒绝 HITL）
- `compose/plan_test.go`：3 条 PlanAgent.Resume 三档 Action（Continue+Text、Replan+Text、Abort）
- 端到端冒烟：plan_chat_web mock 模式下完整跑通"interrupt → continue+text → final" / "interrupt → replan+text → planner 看到补充 → final" / "interrupt → abort → checkpoint 删除 → aborted 事件"
