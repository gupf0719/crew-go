# 错误处理与失败恢复三方对照

> **v2 baseline** (audit follow-up 2026-05): 本文件 v1 时代写, 三方对照表 +
> 我们的设计取舍仍适用 v2, 但 v2 演化已修正核心几点:
> - **`CPStatusFailed` 删过又加回来了**（`ff6de8a` 删、`2b5c9bc` 重新引入，现为 3-state `Unset`/`Pending`/`Failed` + `Status.Resumable()`）。删它时的理由是「三方都无 failed cp 模型」，但那正是**照抄别人而丢掉自己的问题**：我们跟他们的差别在于「所有 `runErr != nil` 都写 cp」——于是失败现场和「等人回答的挂起」在存档上长得一模一样，auto-load 分不出来，把一次带着确定性错误退出的 Run 当成挂起捡起来继续。加回 `Failed` 时的理由是让「能不能恢复」和「要不要自动恢复」分开（auto-load 只捡 `Pending`）。⚠️ **那道 gate 已于 2026-08-13 撤掉**：闸门改按内容判（`HasResumableWork`），`Status` 退为纯报告位。撤的理由见 checkpoint-storage.md 决策记录
> - **`ResumeMode` 二态 (`ResumeDirected`/`ResumeFullSuperstep`) 在 v2 不存在**: v2 Resume **只有"只重跑失败分支"单一行为**（`compose_v2/graph_loop.go::frontierResult.FailedBranches` 作 cp.CurrentFrontier；成功分支 `SuccessWrites` 在超步内 `ApplyAll` 进 channel、cp 持久化 channel state，Resume 不重跑），**无 `ResumeFullSuperstep`"全重跑 frontier" opt-in**。原 §3.3 表 + §3.7 + §四.4/5 的"二态 ResumeMode"是 stale（行为级，非 file:line）——已 inline 订正
> - **`superstepSnapshot`/`buildBranchDelta`/`PreLocalState`/`branchDelta.ErrMsg` 是 v1 机制**: v2 channel-based 简化为 `frontierResult` 的 `SuccessWrites`（channel 写本身即 delta，apply 后 cp 存 channel state 即可还原）+ `FailedBranches`（失败分支 ID）+ `FirstErr`。原文档"完整序列化 per-branch delta 含 Messages/LocalState/Output/Nexts"是 v1 typed-local-state 模型——v2 优势（不重跑成功分支）仍在，但机制名 stale
> - **Sub-agent retry 协议在 v2 完整保留**: **7 编排** (Sequential / Parallel / Loop / Routing / Reflect / Handoff / Plan) `WithXxxRetryConfig` (agent_v2/*_agent.go) + `compose_v2.SimpleRetry / ExponentialBackoff / RunWithRetry[T]`. BasicAgent / GraphAgent 自定义 NodeFunc 级 retry 仍缺 (业务自己 for 循环)
> - **v1 file:line 路径作历史溯源保留**: 本文档 body 内 `compose/*.go:line` / `agent/*.go:line` 引用都是 v1 路径 (v1 已删 — `find . -name "safe_nodefunc.go"` 等返 0), 业务读时按章节描述对照 `compose_v2/` / `agent_v2/` 实际代码自查. 不全量软引用化是历史决策——保 v1 演化轨迹清晰
> - **v2 路径 `compose_v2/*.go:line` 行号亦可能漂移**: body 内 v2 路径（`graph_loop.go::wg.Wait` / `GraphNodeError` wrap / `safeNodeRun` 等）具体行号随重构 stale，按符号自查；`agent/orch_helpers.go::retryRunAgentNode` 函数 v2 已删——retry 改 inline 进各 `*_agent.go` 的 `compose_v2.RunWithRetry` 调用。新哨兵 `ErrNoPendingCheckpoint`（9 编排 Resume 对不存在 cp 统一 fail-fast）属 §3.5/§3.7 失败-Resume 范畴
>
> 原文档 §3.2 / §五.1 "`compose/retry.go` 87 行无人调用" v1 audit 已修正为
> "sub-agent 路径已支持, GraphAgent 自定义 NodeFunc 级 retry 缺" — v2 仍真.

## 触发场景

需要查阅本文档的设计决策：
- 改 `safeRunNode` panic recovery 范围或 cp save 守卫
- 改超步部分失败的兄弟分支策略（cancel vs best-effort）
- 给 `RetryConfig` 增加生产路径接入（节点级 retry / 默认策略）
- 改 `CircuitBreaker` 共享对象（仅 AutoCompact per-session breaker / 其它 LLM 调用）
- 嵌套子图错误冒泡链路改造
- 与"业务错 vs hook 错 vs InterruptError vs ctx 错"语义边界相关的设计变动

## 一、错误分类对照

三家对错误类型的识别：

| 错误类型 | LangGraph | eino | crew-go |
|---|---|---|---|
| 业务错（NodeFunc return err） | 普通 `Exception`，executor `done()` 进 `tasks.pop` 路径 (`_executor.py:78-89`)，由 reraise 或 retry | `task.err` 非 nil 走 `wrapGraphNodeError` 包成 `internalError{NodeRun}` (`error.go:62-77`) | 业务错经 `safeNodeRun`→`runNodeWithHooks` 转 typed `GraphNodeError{NodeID, GraphNS, Err}` (`graph_loop.go:1238`) → 进 `FailedBranches` → frontier 非空 → cleanup defer 写 cp、标 **`Status=Failed`**（失败现场——⚠️ **照样被 auto-load 捡起**：闸门读 `HasResumableWork() && BelongsToTask()` 不读 Status，2026-08-13 起 Status 只是报告位；实测同 sid 再 Run 从失败点续、成功兄弟不重放）+ error 上抛 caller。(v1 `saveCheckpointOnError`/`branchDelta.ErrMsg` 已 stale，见 baseline note) |
| panic | 不在框架层兜——panic 直接冒泡到 executor 的 `done()`，逻辑层未 recover | `taskManager.execute` defer recover → `safe.NewPanicErr` 转 task.err (`graph_manager.go:286-291`)；pre/post handler 也各自 recover (`graph_manager.go:526-543`) | `safeNodeRun`（`compose_v2/graph_loop.go`）框架级 recover → `node panic recovered` error（panic 值是 error 时 `%w` 保链）；栈作为 `compose_v2.PanicError` 并列挂在错误上（`errors.As` 取，错误文本不含栈），兜底点同时记一条带栈的 ERROR 日志（2026-10-15，见 §七之五；之前不保留栈） |
| ctx canceled | `select <-ctx.Done()` + executor 进入 `__exit__` 取消未启 task (`_executor.py:103-105`) | 主循环顶端 `select case <-ctx.Done()` → `newGraphRunError("context has been canceled: %w")` (`graph_run.go:243-247`) | loop 顶 `if err := ctx.Err(); err != nil` 早返（`graph_loop.go 超步循环顶的 ctx.Err() 检查`），此时 frontier 已 `updateFrontier` 非空 → cleanup defer 用**原 ctx** 写 `Status=Pending` cp（无 v1 的 5s background ctx；按 frontier 守卫，同业务错）|
| ctx deadline | 同 cancel 走 `<-ctx.Done()` | 同 cancel 路径 + `taskManager` 内 `cancelCh` + `deadline` 字段 + `receiveWithDeadline` (`graph_manager.go:434-451`)，超时 task 视为 `interruptRerunNodes` 重跑 (`graph_run.go:278-281`) | 同 ctx canceled——loop 顶 `ctx.Err()` 早返时 frontier 非空 → **写 `Status=Pending` cp**（按 frontier 守卫、原 ctx；v2 **无** `DeadlineExceeded` 特判，grep 0 命中，不是"跟普通 error 一样不写 cp"——普通 error 也写 cp）|
| 中断信号（HITL） | `GraphInterrupt extends GraphBubbleUp`，executor `done()` 单独 `pop` 不 reraise (`errors.py:84-89`)；retry 时也 `except GraphBubbleUp: raise`（不重试） (`_retry.py:60-62, 155-157`) | `*core.InterruptSignal` + `*subGraphInterruptError`；resolveInterruptCompletedTasks 区分子图中断 / 节点中断 (`graph_run.go:456-487`) | **v2**: `*compose_v2.InterruptError` + `AskQuery{Question, Options, InterruptID, Reason}` 4 字段 (v1 时代 `NS/ToolCallID` 字段已删, 详 `interrupt-resume.md` §3.1); 走 `compose_v2/graph_loop.go::Run` defer SaveNS, cp `Status=Pending` |
| 子图错误冒泡 | `ParentCommand` 走 `GraphBubbleUp` 链跨层冒泡 (`errors.py:114-118`)；子图 `is_nested=True` 时 `_suppress_interrupt` 不 suppress 让父接管 (`_loop.py:804`) | `subGraphInterruptError{Info, CheckPoint, signal}` 携带子图 cp 一起向上 (`interrupt.go:340-345`)；`handleInterruptWithSubGraphAndRerunNodes` 处理 (`graph_run.go:292-300`) | 内层抛 InterruptError 时其 cp 已物理写到 `(parentSID, childGraphNS)` (ISSUE-70 Phase 2 Step Ⅰ)；外层 cp 通过 `query.NS != JoinNS(GraphNS, nodeID)` 指纹识别冒泡跳过重写 (`checkpoint.go:947-948`) |
| Hook 失败 | hook 不在 LangGraph 概念里（用 callbacks，独立异常） | callback 错误通过 `runPreHandler` / `runPostHandler` 转 task.err (`graph_manager.go:520-545`) | audit² 跟 eino 对齐: framework 4 层 outer defer recover 兜底 (`compose_v2.Run` + `runNodeWithHooks` + 9 编排各 Run/Resume), hook panic 转 wrapped err 返业务; OnGraphStart/NodeStart 业务 error 立即返; OnNodeStart 失败也调 OnNodeEnd 配对避免资源泄漏 (修 Bug #4) |

## 二、各家失败处理详解

### LangGraph

**核心**：`run_with_retry` (`_retry.py:26-104`) 包每个 task 调用：
- 异常分类：`ParentCommand`/`GraphBubbleUp` 直接 raise 不重试；其余按 `RetryPolicy.retry_on` 谓词决定
- `RetryPolicy(initial_interval=0.5, backoff_factor=2.0, max_interval=128, max_attempts=3, jitter=True, retry_on=default_retry_on)` 节点级配置 (`types.py:106-126`)
- `default_retry_on` 黑名单：`ValueError`/`TypeError`/`ArithmeticError` 等编程错不重试，`ConnectionError`/`HTTPStatusError 5xx` 重试 (`_internal/_retry.py`)
- task 失败前 `task.writes.clear()` 避免重试残留
- executor `done()` 把 `GraphBubbleUp` 异常直接 pop 不视为 error；`__exit__` 中先 cancel `__cancel_on_exit__=True` 的未启 task 再 `concurrent.futures.wait(pending)` 等剩下跑完 (`_executor.py:103-108`)
- 中断挂起靠 `_loop.py::_suppress_interrupt` (`_loop.py:786-842`) 在 `__exit__` 上下文吞 GraphInterrupt，写 checkpoint + writes，对外返回 `output` 不抛错

### eino

**核心**：`runner.run` 主循环 (`graph_run.go:241-360`)：
- 无 retry 机制——`grep RetryPolicy /Users/gupengfei/program/go/src/eino/compose/*.go` 无结果。失败直接 `wrapGraphNodeError` 上抛
- `taskManager.execute` 用 `defer recover` 兜 panic 转 task.err (`graph_manager.go:286-291`)
- `tm.wait()` 拿到第一批 completed task 后调 `resolveInterruptCompletedTasks` (`graph_run.go:456-487`)：
  - `*subGraphInterruptError` → 加入 `subGraphInterrupts` map
  - `*core.InterruptSignal` → 加入 `interruptRerunNodes`
  - 其它 error → 立即 `wrapGraphNodeError` 返回上层（不等兄弟）
- 中断后调 `tm.waitAll()` 等剩余 task 跑完 (`graph_run.go:276`)；canceled task 视为 rerun 节点 (`graph_run.go:278-281`)
- `taskManager` 有 `cancelCh + deadline` 机制：`receiveWithListening` 监听 cancel 信号，触发后改用 `receiveWithDeadline` 在指定时长内继续收（不立即丢） (`graph_manager.go:434-451`)
- 中断快照走 `handleInterrupt` / `handleInterruptWithSubGraphAndRerunNodes`，写 checkpoint 含 `RerunNodes` 字段供 Resume 时定位

### crew-go

**核心**：`graphLoop` + `runBatch`：
- 框架级 panic recovery：`safeNodeRun`（`graph_loop.go`）把 NodeFunc panic recover 转 `node panic recovered` error，栈挂在错误上并记日志（§七之五）。**v2 不特判 panic**——该 error 经 `runNodeWithHooks` 转 `GraphNodeError`→`FailedBranches`→frontier 非空→cleanup defer 写 cp、标 **`Status=Failed`**（v2 选可恢复对齐 LangGraph crash-recovery、非 eino fail-out，详 §3.1 三方对照；channel state 干净非腐败。⚠️ ISSUE-94 后档位是 `Failed` 不是 `Pending`；2026-08-13 起续跑闸门按内容判（`HasResumableWork()`）、不读 Status——**失败档会被 auto-load / `agent_v2.Resume` 续跑**（重试失败分支、成功兄弟不重放），Status 只剩报告位）
- **v2 统一 best-effort**：fan-out 任一分支失败（业务错 / hook 错 / Interrupt）都不 cancel 兄弟——`executeFrontier` `wg.Wait` 让所有分支跑完（`graph_loop.go:1047`），失败分支进 `FailedBranches`、成功分支 writes 保留（ISSUE-46 决策）。（v1 曾对 hook 错 / clone 错立即 `cancel()` 兄弟二分，v2 channel-based 无此二分——也无"clone 错"概念。）
- **v2 cp-save 守卫按 frontier/pending 非空**（`graph_loop.go:315-323`，非按错误类型）：`runErr != nil` 且 `CurrentFrontier` 或 `PendingInterrupts` 非空 → 用原 ctx `SaveNS`（档位由 `shouldStayPending` 定 Pending / Failed）；仅成功完成（frontier 空）走 `DeleteSubtree` 不存。（v1 `saveCheckpointOnError` 曾按 `isInterrupt/isTimeout` 错误类型守卫 + timeout 用 5s background ctx——v2 改 frontier/pending 守卫 + 统一原 ctx。）
- `cleanupOnSuccess` 在 graph 成功末尾 `DeleteSubtree(cpID, GetGraphNS(ctx))` (v2: `compose_v2/graph_loop.go` defer 路径)——失败留 cp 不清, 下次 Resume 拿到 stale cp 由 `Status` 区分 (`Pending` 等答案 / `Failed` 失败现场；2026-08-13 起两者都会被续跑，Status 只剩报告位)

## 三、关键设计点对照

### 3.1 panic recovery

| 维度 | LangGraph | eino | crew-go |
|---|---|---|---|
| 框架兜 panic | ❌ 不兜 | ✅ `taskManager.execute` defer recover | ✅ `safeNodeRun` 统一包装 |
| panic 触发 cp save | N/A | ❌ 不存（非中断 error/panic 走 `wrapGraphNodeError` 直接返，`graph_run.go:476`）| ✅ **写**（panic→`safeNodeRun`→`GraphNodeError`→`FailedBranches` 非空→Pending cp）——对齐 LangGraph durability（非 eino fail-out）；确定性 panic Resume 会再 panic（hitl-resume.md "避免用 panic 表 HITL"）|
| stack 落地 | 由 SDK 用户兜 | `safe.NewPanicErr(r, debug.Stack())`：栈**拼进错误文本**；类型在 `internal/safe` 里不导出、无 Unwrap，调用方 `errors.As` 取不到 | ✅ `compose_v2.PanicError{Value, Stack}` 并列挂在错误上（`errors.As` 取），错误文本不含栈；兜底点记带栈的 ERROR 日志（2026-10-15，§七之五）|

**v1→v2 变化 + 三方对照**：v1 曾故意排除 panic 写 cp。**v2 frontier-based 守卫不特判 panic**：panic→`safeNodeRun` 转 `GraphNodeError`→`FailedBranches`→frontier 非空→写 cp、标 `Status=Failed`。三方对照看 **v2 选"失败可恢复"对齐 LangGraph、不是简化失误**：

- **eino**：非中断 error / panic **不存 cp**（`graph_run.go:476` `wrapGraphNodeError` 直接返，只中断存 cp）——同 v1 fail-out。
- **LangGraph**：durability 模型——完成任务 `put_writes`（`_loop.py:351`）+ per-superstep `_put_checkpoint`（:610），错误后从上个 cp + pending writes 续跑（`_executor.py:85` 捕 BaseException 再 raise，非 GraphBubbleUp 不当中断）——**可恢复**，同 v2。

且 v1 担心的"腐败状态"在 v2 channel 模型下**不成立**：panic 节点的 writes 被丢弃（cp 的 channel state 干净、非部分写入），只有外部副作用同任何 rerun 需业务幂等。唯一真 caveat：**确定性** panic（同输入必再 panic）Resume 会再 panic（hitl-resume.md "避免用 panic 表达 HITL"）；**瞬时**失败（OOM / 临时资源）Resume 有救——这正是 crash-recovery 的价值。业务想干净挂起用显式 InterruptError 而非 panic。

### 3.2 Retry 策略

| 维度 | LangGraph | eino | crew-go |
|---|---|---|---|
| Sub-agent 级 retry | ✅ `add_node(..., retry_policy=RetryPolicy(...))` | ❌ 无 | ✅ `agent/orch_helpers.go:retryRunAgentNode/retryRunAgentNodeMsg` 被 7 编排（Plan/Loop/Sequential/Parallel/Reflect/Routing/Handoff）通过 `WithXxxRetryConfig` 选项消费 |
| **任意 NodeFunc 级 retry**（裸 `compose.GraphBuilder.AddNode(fn)` 接 retry）| ✅ 同上（LangGraph 节点统一）| ❌ 无 | ❌ **缺**——`compose/retry.go` 只在 sub-agent 路径用，裸 NodeFunc 无 retry hook |
| 默认黑名单 | ✅ `default_retry_on` 编程错不重试，5xx 重试 | N/A | ✅ **换了个形状解决**（2026-08-21）：不做黑名单，改成 `RetryConfig.ShouldRetry` **必填**（装配期 `ValidateRetryConfig` panic），判据交给配重试的人；`IsRetryable` 作为**显式选择**的保守默认由 `SimpleRetry`/`ExponentialBackoff` 填入 |
| 退避配置 | ✅ initial / backoff_factor / max / jitter | N/A | ✅ `compose.RetryConfig` 定义 + `BackoffDelay` 计算 |
| 多 Policy 链 | ✅ Sequence[RetryPolicy] 按 retry_on 谓词匹配 | N/A | ❌ 单 Policy |

**事实校正**（2026-05-07 audit）：之前文档错称 "`compose/retry.go` 87 行无人调用 / 节点级 RetryPolicy 接入是真实短板"——实际 `agent/orch_helpers.go:89` 定义 `retryRunAgentNode`、`agent/orch_helpers.go:169` 定义 `retryRunAgentNodeMsg`，被 7 编排通过 `WithXxxRetryConfig` 选项消费。**真实差距**：sub-agent 级 retry 已完备，但**裸 `compose.GraphBuilder.AddNode(fn)` 的 NodeFunc 级 retry hook 仍缺**——业务直接用 GraphAgent + 自定义 NodeFunc 时无法声明 retry，必须 NodeFunc 内自己 for 循环。覆盖面比 sub-agent 路径窄（90% 业务走预定义编排，10% 走 GraphAgent 自定义）。详见五.1。

### 3.3 部分失败时的兄弟分支策略（核心）

fan-out N 分支某一支失败，剩下兄弟怎么办：

| 维度 | LangGraph | eino | crew-go |
|---|---|---|---|
| 业务错时是否 cancel 兄弟 | 通过 `__cancel_on_exit__=True` 标记决定，默认 `BackgroundExecutor.__exit__` cancel 未启 task + 等已启完成 (`_executor.py:103-108`) | `resolveInterruptCompletedTasks` 见非 Interrupt error 后立即 `return nil, err` (`graph_run.go:269-272`)——**业务错路径不主动等剩余兄弟**；但同帧 `tm.wait` 已收完本批一组 task，**未启动的下批不会再 submit**。注：超时/cancel 路径走 `tm.waitAll() + receiveWithDeadline` 在指定时长内拿剩余 task，与业务错路径分开 | **不 cancel**（ISSUE-46 决策）—— wg.Wait 让兄弟跑完，成功分支 writes 存 `frontierResult.SuccessWrites`、失败分支 ID 存 `FailedBranches` |
| Interrupt 时是否 cancel 兄弟 | 不 cancel，suppress 后等兄弟（同 `__exit__` 路径） | 调 `tm.waitAll()` 等所有兄弟 (`graph_run.go:276`)，canceled 算 rerun | 同业务错——不 cancel（与首次执行语义一致） |
| Resume 时重跑范围 | 重跑整个 superstep（write tasks 状态 + 所有未完成 task） | 仅重跑 `RerunNodes` 中的（`SkipPreHandler` 标记跳过 already done 的） | **只重跑失败/中断分支**（`frontierResult.FailedBranches` 作 cp.CurrentFrontier；成功分支 `SuccessWrites` 已 `ApplyAll` 进 channel 不重跑）。**无"全重跑 frontier" opt-in**——v1 `ResumeFullSuperstep` 在 v2 不存在 (`compose_v2/graph_loop.go 的 ApplyAll 失败分支-664, 865-899`) |

| **多分支同时失败时返给调用方什么** | 抛**一个**（`_panic_or_proceed` 从 `done` 这个 set 里 `pop()`，既非完成序也非声明序）；中断例外——全部摊平进一个 `GraphInterrupt`。仓库里 0 处 `ExceptionGroup`（Python 3.11+ 有但不用）。真正的答案是 node-level `error_handler`：配了就不抛、路由回图里，N 个分支各触发各的 | 抛**第一个完成的**（图层 `resolveInterruptCompletedTasks` 见非中断错立即 return）；中断例外——`CompositeInterrupt(errs...)` 结构化聚合。全仓 0 处 `errors.Join`。ToolsNode 那层反而是**声明序**（先 `wg.Wait()` 再按下标扫）——同一个框架里两层不统一 | **中断优先**（`frontierResult.exitErr`）：有任何分支中断就返中断（1 个返该分支原始 error 保第二 sentinel，多个 `MergeInterrupts` 合成带 N 条 query 的），否则返 `FirstErr`（frontier 索引序第一个）。**不聚合**——理由见下 |

**「业务错只给一个」是五家一致的做法**，而且不是没想过聚合：LangGraph 有 `ExceptionGroup` 不用、MAF 在 .NET 上有现成的 `AggregateException` 却 `await Task.WhenAll` 掉（只拿 `InnerExceptions[0]`）、eino 有 `errors.Join` 全仓 0 处。我们也试过聚合版，实测它引入一个比它解决的更隐蔽的问题：`errors.Join` 的 `Unwrap() []error` 让 `errors.Is/As` 遍历所有分支，判断语义从「这个错是不是 X」**静默**变成「有没有哪个分支是 X」——3 个分支里 2 个是可重试业务错、1 个 wrap 了自己下游调用的 `context.DeadlineExceeded`（全局 ctx 健康），聚合后 `IsControlFlowSignal` 返 true → `IsRetryable` 返 false → 那 2 个再也不会重试。不聚合时该误判只有 1/3 概率撞上，聚合让它必现。

消费侧的实证比推演更有说服力：OpenAI SDK 面对 anyio 抛来的 `BaseExceptionGroup` 时，「能不能重试」被迫写成 `all(...)` 而不是 any（`mcp/server.py`）；CrewAI 为了从聚合里挑出「那个真正有意义的错」退化成字符串匹配（`"401" in error_msg`、`"TaskGroup" in str(e)`）。**Python 生态里唯一大规模输出 ExceptionGroup 的地方（anyio），下游都在想办法把它拆掉**——聚合把成本转移给了每一个调用方。

**「看全部失败原因」业内一致走观测通道**：MAF 每个失败分支各发一条带 executorId 的 `ExecutorFailedEvent`（它自己的测试就是从事件流手工拼 `AggregateException`）、LangGraph 把 `(ERROR, exc)` 写进 checkpoint、OpenAI SDK 走 `loop.call_exception_handler`。我们对应的是 `OnNodeEnd(ctx, nodeID, chs, nodeErr)`——每个失败分支各 fire 一次，带分支身份。**返回值给一个可判定的根因，观测通道给全貌**，两件事分两条路。

**一个我们没跟的点**：最大分歧其实不是聚合与否，是**确定性**。MAF（`Task.WhenAll` 数组序）/ eino ToolsNode（tool call 下标）/ OpenAI SDK（优先级 + 声明序仲裁，真错误 > 取消噪声）三家做到了「同一批失败每次报同一个」；LangGraph（`set.pop()`）/ eino 图层 / CrewAI Flow（`asyncio.gather`）三家没有。我们的 `FirstErr` 按 frontier 索引序取——SendFrom 派生的成员是 directive 声明序（确定），但静态边成员的次序来自对 map 的遍历，所以**静态 fan-out 多分支同时失败时报哪个错跨次不稳定**。改它要动 `computeNextTasks` 的 nextSet 遍历（超步调度核心），收益是错误报告可复现，暂未做——记 backlog。

crew-go 与 eino 都"仅重跑失败分支"，但 crew-go 把成功分支的 writes（`frontierResult.SuccessWrites`）在超步内 `ApplyAll` 进 channel、cp 持久化整 channel state——Resume 时 `cm.Restore` 恢复 channel + frontier 只设 `FailedBranches`，成功分支无需重算即可还原（其 writes 已在 channel 里）。（v1 时代是 `superstepSnapshot`/`buildBranchDelta` 完整序列化 Messages/LocalState/Output/Nexts，v2 channel-based 简化——channel 写本身即 delta。）

### 3.4 ctx cancel 传播

v2 `executeFrontier` 所有分支起 goroutine 后 `wg.Wait`（`graph_loop.go:1047`）——任一分支失败（含 hook 错 / Interrupt）都等其余跑完、**不主动 cancel 兄弟**。这是 ISSUE-46 决策——业务错副作用已发生（DB 写、API 调用），让兄弟跑完与首次一致。（v1 `runBatch` 曾 `context.WithCancel` + 对 hook 错 / clone 错 `cancel()` 兄弟，v2 统一 best-effort 去掉了这层二分。）代价：用户 NodeFunc 不响应 ctx 时兄弟会"卡死"等到自然结束，**ctx deadline 是唯一外部兜底**（CLAUDE.md 交付纪律明确）。

### 3.5 InterruptError vs 业务错的语义边界

三家都把 HITL 中断视为"非异常"——LangGraph `GraphBubbleUp` 基类显式标记 "Never raised directly, or surfaced to the user" (`errors.py:80-86`)；eino `InterruptSignal` 用单独类型；crew-go `*InterruptError` 走 `errors.As` 识别。

但**处理路径不同**：
- LangGraph：retry 时 `except GraphBubbleUp: raise` 显式跳过重试
- eino：`resolveInterruptCompletedTasks` 把 `subGraphInterrupt` / `InterruptSignal` 加入 `subGraphInterrupts` / `interruptRerunNodes`，其它 error 立即 `wrapGraphNodeError` 上抛
- crew-go v2：**任何节点失败（InterruptError / ctx 错 / 业务错）只要留下非空 frontier（`FailedBranches`）或 pending-interrupts，Run cleanup defer 都写 cp**（`compose_v2/graph_loop.go:316-355`，crash recovery + Resume 重跑失败分支，见 `:125`）；error 同时上抛给 caller。cp 守卫**按 frontier/pending 非空、非按错误类型**，所以不止 InterruptError。⚠️ 这跟 LangGraph/eino "无 failed cp"**不完全对齐**：v2 失败确实写 cp（仅成功完成才不写 + cleanup）。**存档标哪一档**的判据是 `shouldStayPending`（**比 `IsControlFlowSignal` 宽**：另含 `CondFailRerunFrom` 档的 cond 报错与坏存档 `ErrCheckpointNotResumable`，都是「返业务错但框架要它被重跑」；`compose_v2/retry.go` 函数体为准，别数个数）：中断 / ctx 取消 / 软取消 / 陈旧写回写 `Pending`，其余（业务错、hook 错、基础设施错）写 `Failed`。⚠️ **但 2026-08-13 起档位不再参与控制流**：闸门改按内容判（`HasResumableWork`），两档都会被捡起、从失败节点续跑。「下游永久 500 的 Run 被下次同 sid 调用当成挂起捡起来」这个担心在顶层由编排 Run 入口的新任务判定（`enterRunTask`，新任务恒推「新开」信号）结构性覆盖，而那道 gate 在嵌套层挡掉的是**合法续跑**（实测副作用重放）

### 3.6 失败后 cp 写入粒度

| 维度 | LangGraph | eino | crew-go |
|---|---|---|---|
| 写 cp 的错误类型 | 每 superstep 都写 cp（`durability=sync/async/exit` 控制时机） | InterruptSignal / SubGraphInterrupt / canceled task | **任何留下非空 frontier/pending 的失败**（业务错 / ctx 错 / Interrupt 都让失败分支进 `FailedBranches`）→ 写 cp（按 frontier/pending 守卫、非按错误类型；档位由 `shouldStayPending` 定 Pending / Failed） |
| Status 字段 | 无（每 cp 都写） | 无（cp 自带 RerunNodes 列表） | `Pending`(等答案) / `Failed`(失败现场) / `Unset` **3-state + `Resumable()`，但 2026-08-13 起只是报告位**——写档位判据 `shouldStayPending`（宽于 `IsControlFlowSignal`，见上），而闸门统一按内容判（`HasResumableWork`），与两家「无 status」殊途同归 |
| 业务普通错 | 写 cp（按 durability） | 不写 cp，直接上抛 | **写 cp、标 `Status=Failed`**（失败分支进 frontier；2026-08-13 起续跑闸门按内容判，失败档照样能续）+ error 同时上抛 |

crew-go v2 的 `Status` 现在是 3 档（`Unset` / `Pending` / `Failed`），`Checkpointer.ListPending` 只返 `Pending`——注意它**不等于**「可续的档」：失败档也能续（闸门按内容判），只是 `ListPending` 列不出它（2026-09-18 核验）。 业务想 cp cleanup 调 `DeleteSubtree` (PendingExact 由 `ResumeContext.UserInputs` 显式标 by InterruptID).

### 3.7 Resume 时的失败重跑范围

crew-go v2 **只有"只重跑失败/中断分支"单一行为**（无 v1 设想的 `ResumeMode` 二态）：
- `compose_v2/graph_loop.go 的 ApplyAll 失败分支` `cm.updateFrontier(res.FailedBranches)`——下次 frontier 只设失败/中断分支
- `:647-648` `cm.ApplyAll(res.SuccessWrites)`——成功分支的 writes 当场 apply 进 channel，cp 持久化整 channel state
- Resume 时 `cm.Restore(cp)` 恢复 channel（含成功分支的 writes）+ frontier=`FailedBranches`，**成功分支不重跑**

LangGraph 的等价机制：`skip_done_tasks=True` 跳过已完成的写入（基于 `checkpoint_pending_writes`），但仍重跑未完成 task。eino 的 `RerunNodes` 字段持久化在 cp 里，Resume 时按列表 rerun。三家在分支/超步级**都是"只重跑失败"**。

> **粒度对照（现已对称）**：分支级"只重跑失败"——因 BSP 引擎原子单元是 node/分支、有 per-branch `SuccessWrites` delta。tool_call 级原先 rerun-all（toolsNode 对引擎是原子节点、barrier 内无 per-tool 快照），2026-07-30 起由节点私有中断态（`compose_v2.SaveNodeState`）补上 per-tool memo：已成功的 tool_call 在 Resume 时重放产出、不重执行，只有中断/失败/没跑到的执行。原「rerun-all 是时效性 + 快照一致性的正确性选择」已改判，改判记录与残余幂等边界见 [hitl-resume.md §6.3](../../v2/hitl-resume.md)。

### 3.8 断路器 / 熔断

| 维度 | LangGraph | eino | crew-go |
|---|---|---|---|
| 框架级 CircuitBreaker | ❌ | ❌ | ✅ `compose.CircuitBreaker` (`circuit_breaker.go:40-110`) |
| 自愈 cooldown | N/A | N/A | ✅ 默认 5min，过后允许探测；探测失败重置 `trippedAt` |
| 应用场景 | 用户自包 | 用户自包 | 仅 AutoCompact per-session breaker，mutex 保护并发触发（ReactiveCompact 确定性 emergencyDrop 不调 LLM，无断路器） |

`NewCircuitBreakerWithCooldown` 显式版本暴露 cooldown=0 等价旧行为（永久 trip）。`Bug O 修复`（CLAUDE.md 提及）保证 trip 后能 half-open 探测自愈。

### 3.9 `runtime.Goexit`：Go 独有的第三条终止路径（2026-08 新增）

**3.1 只讲了 panic，而 Go 还有第三条路**：`runtime.Goexit` 跑 defer 但 `recover()` 返 nil。
于是所有「靠 recover 判断出了事」的逻辑在它面前**静默失效**。触发面不罕见——`zap` 的 godoc
主动推荐在 `WithFatalHook` 里调它（`zap@v1.27.0/options.go:168`：「We recommend calling
os.Exit or runtime.Goexit inside custom hooks at minimum」），业务一句 `logger.Fatal` 就走这条；
`os/user` 在 Windows 上有一条（`lookup_windows.go:316`）；testify 的 `require.*` 全系同理。

#### 语言层：这一维在 Python/TS 里不存在

| 终止路径 | Go `defer` | Python `finally` | Python `except Exception` | JS `finally` |
|---|---|---|---|---|
| 正常返回 / 异常 | ✅ | ✅ | ✅ | ✅ |
| 取消（`CancelledError`）| — | ✅ | ❌（非 `Exception` 子类）| — |
| `runtime.Goexit` | ✅ **但 recover 返 nil** | 无此概念 | 无此概念 | 无此概念 |

⚠️ **所以「Python 那几家没处理」不能当证据**——那个问题在它们的语言里不存在。可以引用它们的
**形状**，不能引用它们的**沉默**。而 Python 写 `except Exception`（而非 `BaseException`）会重新
造出同款洞，OTel 为此开过 issue 4484。

#### eino：同语言、同形状，有一模一样的洞（实测复现）

`grep -rn "Goexit" eino/ --include="*.go"` **零命中**。两处实测（探针复刻其 `stream[T]` +
`toStream` defer 形状）：

- **图层**：`eino/compose/graph_run.go:111-120` 的 defer 按 `if err != nil` 二分 ⟹ Goexit 下
  `err` 恒 nil ⟹ 走 `onGraphEnd`，**一个没跑完的 run 得到一个成功的闭合 span**；`Invoke`
  永不返回（调用方 goroutine 被一并掐死）。比缺一个 span 更坏——缺的看得见，假成功看不见。
- **流层**：`eino/schema/stream.go:747-777` 的 `toStream`，Goexit 下消费方收到 `2 chunk + io.EOF`，
  **与正常发完一字不差**。
- **工具层**：`eino/compose/tool_node.go` 的槽里预填了 `callID`（`:789-863` 在 goroutine 启动前填），
  所以同一个 Goexit 下它出「空内容但配对完整」的 tool message，我们出 400/422。**这一个字段
  就是全部差别**——它的收集端因此不需要 nil 判断（`:1128` 无条件构造）。

#### 各家的完整性凭据都在「流之外」

没有任何一家在流原语层区分「干净结束」和「被截断」——没有 `io.ErrUnexpectedEOF` 式的第二个
sentinel。判据一律在更高一层：

| | 流的终止符 | 完整性信号载体 |
|---|---|---|
| OpenAI Agents | `QueueCompleteSentinel` 单值（成功失败同一个）| 旁路字段 `_stored_exception`；sentinel 到了立刻再查一次错误槽 |
| LangGraph | 生成器 StopIteration / raise | `PregelLoop.status` 7 档 → 出口映射成 `GraphDrained` / `NodeCancelledError` |
| eino | `io.EOF`，**没有第二个值** | **无**（`FinishReason` 是 0 consumer 的透传字符串）|
| MAF | 带内终止事件 | `WorkflowRunState` 7 档（但 `CANCELLED` 声明了没接线）|
| deepseek-harness | `turn/end{reason}` 事件 | `TurnEndReasonMap` 6 档 + LLM 层 `FinishReasonMap` 5 档（**唯一带载荷的判别联合**）|

**两家做对的用了同一个切分**：raw delta 归「已交付 / 可审计」（照数交付、全落日志），
聚合产物归「必须有完成凭据才生成」——**不是「半截 + 标 incomplete 一起进历史」，是聚合产物
直接不生成**（OpenAI 没等到 `ResponseCompletedEvent` 就 `raise ModelBehaviorError`；
deepseek-harness 的 `assistant/message` 只在 finish 不是 error/aborted 之后才 append）。

#### 可引用的先例：defer 内部的分工

**无条件释放资源，但写「结论」必须先判 panic。** 两处独立先例：

- eino 的 `schema/stream.go:749-762`：`recover()` 第一句、`send(chunk, e)` 门控、
  `closeSend()` / `r.close()` 无条件
- LangGraph 的 `pregel/_executor.py:110-111` / `:201-202`（一字不差两处）：
  `# if there's already an exception being raised, don't raise another one`

Go 标准库对 Goexit 本身也有两处独立实现，判据都是**一道正向旗**：`testing.tRunner` 的
`t.finished`（→ `errNilPanicOrGoexit`）、`iter.Pull` 的 `seqDone`。`x/sync/singleflight`
用同款双 defer 判别 + `errGoexit` 哨兵；而 `errgroup` **刻意不处理**（v0.15 加过、v0.19 整体
撤回，撤回理由是 panic 传播的三条问题，Goexit 是被连坐删的）。

#### crew-go 的处置（2026-08）

| 层 | 判据 | 兜住什么 |
|---|---|---|
| `Run` 出口 | `bodyReached`（三个成功出口置位）| **存档不说谎**：图主体任何位置的 Goexit 都落这格，档留 Failed 可续 |
| 节点层 | `bodyReturned` | 身份 + `OnNodeEnd` 配对 |
| fan-out 成员 / tool fanout | `normalReturn` / `finished` | 零值槽不被当成功、幽灵成员不进 `CompletedFrontier` |
| 流桥接 | `concluded` | 截断不冒充干净 EOF |
| `ApplyAll` / `Restore` | `applied` / `restored` | 回滚不跳过（否则半应用状态进存档 ⟹ Resume 双计）|
| async cp worker / a2a server | `drained` / `sent` | worker 猝死可见、`<-done` 不永久阻塞 |

**我们比同前提的那家（eino）多的是这套旗**；比 Python 那几家多的是「这个问题存在」这个前提。
完整清单与「还能从哪冒出来」的 sweep 见 `compose_v2/panic_recovery.go`。

## 四、crew-go 已有优势

1. **`safeRunNode` 框架级 panic recovery**：3 处 NodeFunc 调用点（`graphLoop` fast `checkpoint.go:255` / `runBatch` 分支 `superstep.go:212` / `rerunFailedBranches` `checkpoint.go:1589`）统一收口。**LangGraph 不在框架层兜 panic**，eino 仅 `taskManager.execute` 一处兜。我们覆盖最完整。
2. ~~**panic 不触发 cp save**~~（**v1 行为，v2 已变**）：v1 `safeRunNode` 故意把 panic 排除在 cp-save 守卫外。**v2 frontier-based 守卫不特判 panic** → panic 也写 Pending cp（`graph_loop.go:1238/642/323`）。三方对照（§3.1）：**eino 也不存 panic cp**（`graph_run.go:476` `wrapGraphNodeError` 直接返，同 v1 fail-out）；**LangGraph durability 模型错误后可续跑**（同 v2）——v2 选可恢复对齐 LangGraph、非失误。channel state 干净（panic 节点 writes 丢弃，非 v1 担心的腐败）。caveat：确定性 panic Resume 再 panic（hitl-resume.md 引导避免用 panic 表 HITL）。
3. **统一 best-effort 等兄弟**：fan-out 任一分支失败（业务错 / hook 错 / Interrupt）都 `wg.Wait` 等所有分支跑完不 cancel（`executeFrontier`，`graph_loop.go:1047`），与首次执行语义一致——成功分支 delta 靠 `SuccessWrites` 保留而非 cancel 隔离。LangGraph 用 `__cancel_on_exit__` 标记；eino 见非 Interrupt 立即返回。（v1 曾有"hook 错 / clone 错立即 cancel"二分，v2 channel-based 已统一去掉。）
4. **只重跑失败分支 + 成功分支后继保留**（`frontierResult.SuccessWrites/FailedBranches` + R33 `cp.CompletedFrontier/DeferredSends`）：成功分支 writes 当场 `ApplyAll` 进 channel、cp 持久化 channel state，Resume 时 frontier 只设 `FailedBranches` 成功分支不重算；R33 PA-H1 后成功成员**身份 + SendFrom 指令**也持久化——Resume 重跑失败成员成功后并入出边计算（不重跑 NodeFunc），修复前发散拓扑（A→X,B→Y）成功分支后继 X 静默丢失 graph 假成功。三方：LangGraph **不是**"重跑整 superstep"（旧声明已被本地源码证伪）——它 per-task put_writes + resume 只执行无 writes 的 task（matched-writes-skip），真实差异是我们成功 writes 立即 apply 进 cm（它攒到超步末）+ 它有 mid-superstep 持久化；eino 按 `RerunNodes` 但成功 task 状态在 channel 里。（v1 时代是 `superstepSnapshot`/`buildBranchDelta` 完整序列化 delta，v2 channel-based 简化——channel 写即 delta。）
5. **channel-based per-branch 记录**：v2 用 `frontierResult.SuccessWrites` + `FailedBranches` 记录超步结果——成功分支 writes apply 后即进 channel state、cp 恢复 channel 即还原，不需 v1 的 `superstepSnapshot`/`PreLocalState` 入口快照（channel reducer 天然增量，无 typed-local-state 回滚需求）。
6. **`CircuitBreaker` 框架内置** + 仅 AutoCompact per-session breaker（`autocompact.go` `breakers *sync.Map`，ISSUE-72 修跨 session 共享 bug）+ mutex 保护并发触发 + 5min cooldown 自愈。三家中唯一一家把断路器作为框架原语。
7. **ChainHooks panic recovery + 失败时调 OnNodeEnd 配对**：OnNodeStart 返 error 也调 OnNodeEnd（修 Bug #4，`checkpoint.go:241-247`）——hook 实现者用 start/end 配对管理 tracing span / metric inc-dec / 资源 acquire-release，缺 end 会泄漏。三家中只有我们显式保证配对。
8. **Status 3 档**（`Unset` / `Pending` / `Failed`；`CPStatusFailed` 曾在 v2 audit follow-up #3 删掉，ISSUE-94 又加回，2026-08-13 起只是报告位）：`ListPending` 仅返回 `Pending` cp（2026-09-18 注：本条下文「删 failed 状态枚举」的叙述已过时）. LangGraph 每 cp 都写无状态字段; eino 无概念. crew-go 删的是"failed"**状态枚举**（不再有"失败但不可 Resume"的 cp 态）——但失败/中断留非空 frontier 时仍写 `Status=Pending` cp（crash recovery，见 #9），**不是**"任何失败都不写 cp"。
9. **失败/中断 SaveNS 用原 ctx**：Run cleanup defer 在 `runErr != nil` 时用**原 ctx** `SaveNS`（`graph_loop.go:316-355`）。（原文档"`saveCheckpointOnError` 双 ctx / timeout 用 5s background ctx"是 v1 语义——`saveCheckpointOnError` 在 v2 不存在，统一用原 ctx。）

## 五、crew-go 的不足

1. **裸 NodeFunc 级 RetryPolicy 缺失**（**事实校正后的精确版**）：`compose/retry.go` 的 `RetryConfig + BackoffDelay` 在 sub-agent 路径已被 7 编排消费（`agent/orch_helpers.go:retryRunAgentNode`），**但裸 `compose.GraphBuilder.AddNode(fn)` 的 NodeFunc 级 retry hook 仍缺**——业务直接用 GraphAgent + 自定义 NodeFunc 时，必须 NodeFunc 内自己 for 循环。LangGraph `add_node(..., retry_policy=RetryPolicy(...))` 节点统一接 retry 不区分 sub-agent vs 裸 fn。覆盖面：90% 业务走预定义编排（已支持），10% 走 GraphAgent 自定义（缺）。优先级中等。
2. **没有"网络错自动重试"默认行为**：用户 NodeFunc 调 LLM/HTTP 临时 5xx 时框架不重试，失败直接进 cp。LangGraph `default_retry_on` 默认重试 ConnectionError + 5xx，节省用户写样板。
3. **~~嵌套子图业务错冒泡缺结构化字段~~（v2 已解决，保留编号供决策#4 引用）**：v2 已实现 typed `compose_v2.GraphNodeError{NodeID, GraphNS, Err}`（`compose_v2/errors.go:51` + `graph_loop.go:1238` 在 `runNodeWithHooks` 收口——所有节点的业务错 / ctx 错 / panic-转-error 都 wrap，仅 InterruptError 按设计保留 `fmt.Errorf`）——caller `errors.As(&GraphNodeError)` 拿 `NodeID`/`GraphNS` 定位嵌套子图节点。**eino 仍缺**——普通业务错走 `wrapGraphNodeError(nodeKey, err)` 字符串 wrap（`graph_run.go:476`），`subGraphInterruptError` 仅 InterruptError 路径（`interrupt.go:340-347`）。原列为"两家共同短板"，现 **crew-go 已补、仅 eino 缺**（已从短板转为相对 eino 的优势）。
4. **ctx deadline 不能强制关闭未响应 NodeFunc**：业务错 best-effort 策略下，用户 NodeFunc 不响应 ctx.Done() 时兄弟会卡到自然结束（CLAUDE.md 交付纪律已警示）。LangGraph `__cancel_on_exit__` + Future.cancel 在 Python 层也只能信号性 cancel；eino `taskManager.cancelCh + deadline` 通过 `receiveWithDeadline` 在指定时长内拿剩余 task，超时算 rerun。我们没有第二层兜底，全依赖 NodeFunc 自检 ctx。
5. ~~**panic stack 完全不保留**~~ **已解决**（2026-10-15，见 §七之五）。原文：（2026-09-18 核验更正：原写「仅 slog 落地」失实——`safeNodeRun` 只转 error、不打日志，全仓无 `runtime.Stack` / `debug.Stack`）用户 Resume / 排查 bug 时拿不到 stack。⚠️ 原文接着写的「eino 把 stack 包进 error 链，errors.As 可拿」与源码不符：eino 的 `panicErr` 在 `internal/safe` 里、不导出，栈拼在错误文本里，调用方 errors.As 取不到。
6. **跨编排错误冒泡未做语义统一**：Routing 内 sub-agent 抛业务错时，cp 写在 (parentSID, routingNS) 还是 (parentSID, routingNS/sub) 取决于 sub-agent 是否走 push graphNS 嵌套路径。ISSUE-71 接口合并后大部分场景已对齐，但用户自定义 GraphAgent NodeFunc 内手动调子 agent.Run 时**必须**走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input, opts...)` helper (#7 F4 commit `f6ede83`) 一站式 push 3 件 ctx (graphNS + NewSharedChildSession + storeScope)，否则走 standalone 路径 cp 写错位置（v1 `WithEnclosingParent` API 已删, 见 `agent-interface.md` v2 baseline）。
7. **CircuitBreaker 仅用于 AutoCompact**：仅 AutoCompact 持 per-session breaker（ReactiveCompact 是确定性 emergencyDrop 删最旧消息组、不调 LLM，无需断路器），**普通 LLM 调用 / Tool 调用未接入**。一个 sub-agent 反复 LLM 失败时框架不熔断，会反复重跑分支。（2026-09-18 核验：LLM 调用已可经 `fallback.New` 的 per-member 断路器接入——opt-in；Tool 调用仍未接入。）
8. **未深入：eino `WrapInterruptAndRerunIfNeeded` 与 `CompositeInterrupt` 的语义差异**——eino 区分"单中断 / 复合中断 / 旧版中断"三套 API，我们统一一个 `InterruptError + AskQuery`。是简化优势还是缺失能力，未深入对比测试。

## 六、可学习清单

| 他家方案 | 学什么 / 不学什么 / 已学 | 改造代价 |
|---|---|---|
| LangGraph `RetryPolicy(initial_interval, backoff_factor, max_interval, max_attempts, jitter, retry_on)` 节点级配置 | **部分已学**：sub-agent 路径已通过 7 编排 `WithXxxRetryConfig` + `agent/orch_helpers.go:retryRunAgentNode` 消费 `compose/retry.go RetryConfig`。**仍需扩展**：给 `compose.GraphBuilder.AddNode` 加 `WithRetryConfig(cfg)` option 让裸 NodeFunc 也能声明 retry，`safeRunNode` 包一层 retry loop（守卫 InterruptError / ctx 错不重试）| 中：~80 行 + 测试（已有 RetryConfig 复用，仅扩展 GraphBuilder Option + safeRunNode hook）|
| LangGraph `default_retry_on` 黑名单（编程错不重试，5xx 重试） | **已做，但换了形状**（2026-08-21）：没有定义框架级默认谓词，而是让 `ShouldRetry` **必填**——因为 `compose_v2` **不 import `model`**，它在这一层拿不到 `APIError` 之类的分类材料，做不出「5xx 重试、编程错不重试」那种名单；而配重试的人手上有这个信息。⚠️ 那条语言红利也别高估：Python 作者`raise ValueError` 免费落在「不重试」侧，Go 作者 `fmt.Errorf` 落在「重试」侧，**默认档与语言的反射动作反向对齐**才是我们踩坑的结构成因 | 已落地 |
| LangGraph 多 RetryPolicy 链（按 retry_on 谓词匹配） | **仍不学，但论据变了**（2026-08-21）：原论据「需要分类时业务自己包 wrapper」已被架空——per-config 的 `ShouldRetry` 谓词本身就是分类钩子，业务在一个函数里 switch 即可，不必包 wrapper | N/A |
| LangGraph `Durability=sync/async/exit` 三档持久化时机 | **已学**（2026-09-18 更正：原写「不学」已过时）：`compose_v2.WithDurability` exit（默认）/ sync / async，见 `compose_v2/durability.go` | N/A |
| eino `subGraphInterruptError{Info, CheckPoint, signal}` 携带子图 cp 一起冒泡 | **已实现**：v2 已有 typed `GraphNodeError{NodeID, GraphNS, Err}`（`errors.go:51` + `graph_loop.go:1238` runNodeWithHooks 收口）——caller `errors.As` 拿 NodeID/GraphNS 定位嵌套子图节点。eino 此能力仅 InterruptError 路径有，普通业务错仍字符串 wrap，反而是 crew-go 领先 | N/A（已落地） |
| eino `taskManager.cancelCh + deadline + receiveWithDeadline` 兜底超时机制 | **不学**：我们 best-effort 策略下，NodeFunc 不响应 ctx 是用户实现 bug，加框架兜底会鼓励错误模式。CLAUDE.md 已明确"ctx deadline 是唯一外部兜底"，方向正确 | N/A |
| eino panic stack 包进 error 链 (`safe.NewPanicErr(r, debug.Stack())`) | **已做，换了形状**（2026-10-15，§七之五）：学「兜住时就取栈、挂在错误上」；不学「栈拼进错误文本」（我们的错误文本会发给前端、对端、模型和 span），也不学「类型不导出」（调用方取不到）。**修不足 5** | 已落地 |
| eino `runner.run` 主循环顶端 `select <-ctx.Done()` 显式 early-return | **已学**：`graphLoop` 顶 `if err := ctx.Err(); err != nil` 早返 (`checkpoint.go:184-186`) | N/A |
| LangGraph `__cancel_on_exit__` 标记 + executor `__exit__` 选择性 cancel | **不学**：v2 统一 best-effort——任一分支失败（业务 / hook / Interrupt）都 `wg.Wait` 不 cancel 兄弟（无 v1 的"基础设施错 cancel"二分，grep `WithCancel`/`cancel()` 0 命中）。LangGraph 这个标记主要给用户 task submit，我们超步语义里不需要 | N/A |
| LangGraph executor `done()` 把 GraphBubbleUp pop 不视为 error | **已学**：cp-save 守卫**不按错误类型**（frontier/pending 非空即写 Pending cp，业务 / ctx / Interrupt 一视同仁）；区分 InterruptError 的是 query-recording——`frontierResult.AllInterrupts` 额外收中断 query 供 caller 配答（中断分支同时也在 `FailedBranches`，非 partition） | N/A |
| LangGraph `_suppress_interrupt` 在 `__exit__` 上下文吞 GraphInterrupt 写 cp | **已学**：v2 cleanup defer `SaveNS`（`graph_loop.go:316-355`）等价能力——失败/中断（frontier 非空）写 cp（档位由 `shouldStayPending` 定）+ 把 InterruptError 上抛由 caller `errors.As` | N/A |
| eino `CompositeInterrupt(ctx, info, state, errs...)` 复合中断 | **观察**：我们用户场景里多 ask_user 用 `state.pendingInterrupts buffer + cpKeyInterrupts map[ID]*AskQuery` 已覆盖，eino 这套接口暴露给业务用得多吗？未深入。先观察用户反馈再决定 | 高：API 设计 |
| 框架级断路器（crew-go 已有，三家独有） | **已学**：`compose.CircuitBreaker`，应**扩展应用范围**——把"普通 LLM 调用 / Tool 调用"也接入（修不足 7） | 中：~100 行 + 用户控制 opt-in |

## 七、决策建议

**当前必须做**（小修补，1 周内）：

1. ~~**panic stack 包进 typed error**~~ **已落地**（2026-10-15），形状跟这里原来写的不同：不进 trace、不替换原来的错误，而是并列挂上去，另加兜底点日志——见 §七之五。原文：`safe_nodefunc.go` 新增 `*PanicError{Err, Stack}`，`safeRunNode` 返回它替换裸 `fmt.Errorf`。caller `errors.As` 拿到 stack 用于 trace 上报。
2. **CircuitBreaker 文档化默认接入点**：明确"什么场景适合接入"，引导业务在 LLM 调用 / 重试热路径上用——不是改框架，是补文档。

**中期立项**（2-4 周）：

3. **节点级 RetryPolicy 接入**：把 `compose/retry.go` 现有结构接入 `GraphBuilder.AddNode(..., WithRetry(cfg))` + `safeRunNode` 内 retry loop（守卫 InterruptError / ctx 错不重试）+ `compose.DefaultRetryOn` 默认谓词（HTTP 5xx / network error 重试）。**修不足 1+2**，对齐 LangGraph，业务收益高（少写一堆样板）。（未实施，已进中心 backlog；其中「默认谓词」那一半已被「`ShouldRetry` 必填」否决。）
4. ~~**子图错冒泡结构化**~~（**v2 已落地**，不足#3 已解）：`compose_v2.GraphNodeError{NodeID, GraphNS, Err}`（`errors.go:51`）在 `runNodeWithHooks`（`graph_loop.go:1238`）收口——所有节点业务错带结构化定位，caller `errors.As` 可拿 NodeID/GraphNS。无需再立项。

**观察一段时间再决定**：

5. **CircuitBreaker 自动接入 LLM/Tool 调用**：先给业务 opt-in API，看真实使用率；不要默认全开（quota 反弹场景下断路器会让正常恢复延迟 5min）。
6. **eino `CompositeInterrupt` 是否引入**：我们 `pendingInterrupts buffer + map[ID]*AskQuery` 已覆盖大部分多 ask_user 场景，等用户报具体限制再决定。

## 七之二、重试边界与「不重试」的表达方式（2026-08-24 调研）

起因：crew-go 有个错抛在**被重试单元已经成功之后**——转交目标冲突（模型在一批
tool_call 里给了两个不同的目标）是在 `ag.Run()` 成功返回之后取槽时才发现的。当时的
处置是新造一个 `ErrNoRetry` 哨兵表示「别重试这一步、但存档照留」。调研之后**撤销了
那个哨兵**，改成调整边界。

### 7.2.1 「不重试」怎么表达：无人用扁平哨兵列表

| | 机制 | 框架级分类 | 调用方谓词 |
|---|---|---|---|
| LangGraph | 异常**类型继承** + policy | `default_retry_on`（类型denylist） | `retry_on` 收 class / seq / callable |
| OpenAI Agents | **归一化事实结构** + policy callable | provider advice | `RetryPolicy: Callable[[RetryPolicyContext], bool \| RetryDecision]` |
| eino | 谓词，且**能读到成功输出** | `IsRetryAble`（已废弃） | `ShouldRetry(ctx, retryCtx) *RetryDecision` |
| MAF | workflow 层无 | — | — |
| CrewAI | 无（只有校验循环） | — | — |

LangGraph 的结构跟 crew-go 同形——硬 guard 在前、policy 在后（`pregel/_retry.py`
先 `except GraphBubbleUp: raise` 再走 `_should_retry_on`），对应我们的 `IsRetryable`
守卫 + `cfg.ShouldRetry`。**这一层设计有背书。** 但「不该重试」的集合它用**继承**表达
（`GraphBubbleUp` 是 `GraphInterrupt` / `NodeInterrupt` / `ParentCommand` 的基类），
不是哨兵清单。

### 7.2.2 失败时清状态：四家零先例

**没有一家有 error-triggered 的状态清理。**

- LangGraph 的 `delete_thread` 声明在 `checkpoint/base/__init__.py`，`grep` 整个
  `libs/langgraph/langgraph/` **零命中**——是给用户/平台层的 API，引擎不调
- MAF 的 `CheckpointStorage.delete()` 同样，`_workflows/*.py` 里除定义处外零调用
- LangGraph 失败时反而把**错误写进 checkpoint**（`_runner.py` 的
  `task.writes.append((ERROR, exception))`），成功的兄弟分支照常提交——这正是
  「Resume 时成功分支不重跑」的实现基础，跟我们同一个模型

**含义**：`ErrRunAborted` 的破坏性那半（Run 出口主动 `DeleteSubtree`）在四家里没有
对应物。把「不重试」和「清状态」拆开是对的；但拆的方式不该是再加一个哨兵。

### 7.2.3 同一个错，LangGraph 用边界解决

LangGraph 有**一模一样**的错：`LastValue.update()` 在 `len(values) != 1` 时抛
`InvalidUpdateError`——「两个东西给同一个槽写了矛盾的值」。

关键在位置：`apply_writes`（调用 `channel.update` 的地方）只出现在 `_loop.py` 的
`after_tick` 和 `_draw.py`，**`_retry.py` 里零命中**。retry 单元是 node body，冲突
解决在**归约阶段**——retry 够不到。

而且 `InvalidUpdateError` 是普通 Exception，**名义上被 `default_retry_on` 判为可重试**
——无所谓，因为边界让分类不相关。

### 7.2.4 真正的规则：单元有没有副作用

不是「别在重试单元内校验」，而是「**验证后重放，在单元无副作用时才可以**」：

- eino 的唯一 retry 是 **ChatModel 装饰器**（`adk/retry_chatmodel.go`），`compose/` 下
  `grep -rln Retry` 零命中——**没有编排级 retry**。而它的 `ShouldRetry` 明确能读成功
  输出（godoc：「full access to the model output via retryCtx.OutputMessage」）
- MAF 的 Magentic 重试 "Progress ledger JSON parse failed"——同样是重试一次 model 调用
- CrewAI 的 guardrail 在 `agent.execute_task` **成功之后**校验、失败则重执行——它接受
  重放代价，但**把错误喂回去**让重试不是盲目重放

三家的 retry 单元都是**单次 model 调用**。crew-go 的单元里是 `ag.Run()`——完整
agent 执行带工具副作用。**违规在这儿，不在错误分类。**

### 7.2.5 同一场景，OpenAI 不当错误处理

`run_internal/turn_resolution.py` 对「多个 handoff」的处置：取第一个，把其余的通过
**tool output** 告诉模型「Multiple handoffs detected, ignoring this one.」，并记一条
span error。**从不抛异常**，所以重试分类根本不发生。

这条对我们可用：批内声明序是良定义的（我们自己的 godoc 也承认换键之后跨分支不再撞、
剩下的唯一冲突源是同一批内）。

**2026-10-08 已采用**：同一批 tool_call 对同一件事给出不同决定时排在前面的生效、后面那个回一条
失败结果（`agent_v2.SupersededDecisionErrCode`），不再报冲突；决策记录见
[handoff-control-signal.md §8](handoff-control-signal.md)。

### 7.2.6 更彻底的形态：replay-safety 是**调用单元**的属性

OpenAI Agents 把这件事做成了第二个维度，独立于错误身份——`run_internal/model_retry.py`：

```python
if replay_unsafe_request and not decision._approves_replay and not provider_marks_replay_safe:
    return RetryDecision(retry=False, reason=...)
```

policy 可以返 `retry=True` 却被**否决**，因为这次**调用**是 replay-unsafe。而
replay-unsafe 定义在调用上不在错误上：`_is_stateful_request(previous_response_id,
conversation_id)`——「这次调用已经改了别处的状态，重放会双写」。

**这正是我们的「重试会重放一次成功的调用」，但 key 在单元上而不是错误上** ⟹ 覆盖
所有错，包括还没想到的那些。`RetryDecision` 另有 `_hard_veto` / `_approves_replay`
两个独立于 `retry` 的标志位。

### 7.2.7 我们的决策

| 项 | 决定 | 理由 |
|---|---|---|
| 拆开「不重试」与「清状态」 | ✅ 保留 | 四家零先例做捆绑清理，拆开有背书 |
| `ErrNoRetry` 哨兵 | ❌ **撤销** | 标记只保护记得标的那一个错；下一个 post-success 检查照样被重放，且漏标不报错 |
| handoff 的冲突检查 | ✅ **挪出重试闭包** | 对齐「冲突解决属归约阶段」；之后不需要任何标记，且自动覆盖将来所有 post-success 检查 |
| routing 的冲突 | ✅ **改回可重试**（⚠️ 已被 2026-10 的上行信号取代：同一批 route_to 目标不同时排在前面的生效、不报错；同一超步两路都定了路由报 `SignalConflictError`，终局不重试，见 [handoff-control-signal.md](handoff-control-signal.md) 的决策记录） | 重放的是 classifier 的 LLM 调用，跑在任何分支之前、无业务副作用；而「模型自相矛盾」是随机的，正是重试该用的场景 |
| replay-safety 做成单元属性 | 🔵 未做（已进中心 backlog）| 比按错误标记彻底，但要动 `RunWithRetry` 的签名，本轮不做 |

**源码位置**：LangGraph `pregel/_retry.py` / `pregel/_loop.py::after_tick` /
`channels/last_value.py` / `errors.py`；eino `adk/retry_chatmodel.go`；
OpenAI Agents `run_internal/model_retry.py` / `run_internal/turn_resolution.py` /
`retry.py`；MAF `_workflows/_checkpoint.py`；CrewAI `task.py` 的 guardrail 循环。

---

## 七之三、扇出到 N 个分片时的部分失败（2026-08-25 调研）

**触发**：长期记忆注入按写入者分片检索（`agent_v2/basic_agent.go` 的
`gatherMemoryCandidates`），发现步 `ListNamespaces` 的错被完全吞掉、分片扇出的错是
「先到先得」。修之前中断会静默消失（实测 `Run` 返 `nil`，人没被问、Run 照常收尾）。

### 问题 1：发现/枚举步失败，整体失败还是降级？

| 项目 | 发现步失败 | 层 | 关键位置 |
|---|---|---|---|
| eino router / multiquery | **整体失败**（返回空集合也算错） | 库 | `flow/retriever/router/router.go:125-134`、`multiquery/multi_query.go:163-165` |
| MAF context providers | **整体失败**（串行、零 try/except） | 库 | `python/packages/core/agent_framework/_agents.py:1477-1488` |
| mem0 | **整体失败**（跑满不早退，`.result()` 重抛） | 库 | `mem0/memory/main.py:731-742` |
| Elasticsearch | **降级**（`allow_partial_search_results` 默认 true） | 平台 | — |
| Milvus | **整体失败**（分片 leader 不可达即失败） | 平台 | — |

**库层没有一家做静默降级。** ES 做降级，它买得起的前提是**拥有响应信封**：每个响应恒带
`_shards: {total, successful, skipped, failed}`，跨集群另有
`_clusters: {successful, failed, skipped, partial}`——降级**在结构上永远可见**，不是异常
路径而是每次都在的计数器。Go 库返 `([]T, error)` 没有这个位置。

而且 ES 团队自己两次想翻掉这个默认：issue #27435 提案默认 `false`，理由是「客户端多半不看
这些 flag，会把不完整的结果当完整的呈现」；issue #28494（7.0 改成失败）closed as not
planned，默认留在 `true`——**向后兼容赢了设计意图**。这恰好是「Go 库不该学它」最强的论据。
顺带：`skip_unavailable` 在 8.15 把默认从 false 翻成 true，且它为 true 时**直接忽略**
`allow_partial_search_results`——ES 内部这两个旋钮的语义边界本身也在漂。

### 问题 2：N 分片部分失败，返哪一个 error？

**「控制流信号必须先于任何错误处理策略被摘出去」是有独立命名的一等概念，不是我们的发明。**
LangGraph 的 `GraphBubbleUp`（`libs/langgraph/langgraph/errors.py:50`）是专门用来豁免所有
错误处理策略的基类，`GraphInterrupt` / `ParentCommand` / `GraphDrained` 都继承它，仓库里
**14 处**显式 `isinstance(exc, GraphBubbleUp)` 豁免；`pregel/_executor.py:81` 的注释写
「这个异常是中断信号不是错误」，`pregel/_retry.py:632/773` 同样跳过重试。

各家怎么挑那一个错：

- **eino**：跑满全部，再按**声明序**扫，第一个非 nil 就返（`router.go:151-158`）。单错、
  确定、不聚合、结果全丢。**注意它的分片仲裁正是我们缺陷 B 的形状**——eino 没这个问题只
  因为它没有中断概念。
- **LangChain**：`try/except` 只为 fire `on_retriever_error` 然后 `raise`（`ensemble.py:115-119`
  / `152-160`、`merger_retriever.py:71-77` / `105-113`、`multi_query.py:147-155`）——
  **错误进观测通道，返回值仍是单个 raise**。
- **LangGraph store**：一次 `abatch` 抛错 → **同批所有 future 拿到同一个异常**
  （`checkpoint/langgraph/store/base/batch.py:361-366`），比「挑一个」更激进。

**五家一致把全貌放观测通道**（eino 每个失败分片各 fire 一次 `callbacks.OnError` 带分片身份、
LangChain 走 `on_retriever_error`、我们走 `OnNodeEnd`），返回值只给一个可判定的根因。
§3.3 已记录的两个下游反证（`BaseExceptionGroup` 逼出 `all(...)`、字符串匹配 `"401" in ...`）
说明聚合成多错误树的代价。

### 问题 3：有没有「部分结果 + 部分失败」的一等表达？

三档，**我们手上已经有其中两档的零件**：

1. **三槽返回**（Prometheus，库层最成熟）：`(结果, Warnings []error, error)`，
   `storage/interface.go:113/223/311`。最重要的是扇出层怎么用它——`storage/merge.go:221-240`
   是**纯机械的：warning 累加，error 中止**。「这次失败算不算可降级」**由叶子决定**（它选把
   失败放进哪个槽），不由扇出层猜。
   **代价当场可见**：同一份代码里 `LabelNames` 出错时 `return nil, nil, ...` 把已累积的
   warnings 丢了；`lvals`（`merge.go:178-189`）第二次 append 的还是前半段的 `w`。
   **多一个槽 = 多一个会被静默漏掉的东西。**
2. **信封里的计数器**（ES，平台层）：`partial` 是独立于 successful/failed 的第三态，表达力
   最强，前提是拥有 wire format。批评见上（结构可见 ≠ 有人看）。
3. **per-shard 结果结构体**：eino `utils/utils.go:34-41` 的 `RetrieveTask{Result, Err}` ——
   表达存在但只在内部，公开 API 仍收敛成 `([]*Document, error)`。反例是 LangGraph：它的
   `Result` 联合（`store/base/__init__.py:538`）**没有 exception 变体**，所以只能整批同一个
   异常。**我们的 `store.Result`（`store/store.go:280-285`）已有 per-op `Err` 槽**，比
   LangGraph 强一档，只是 `Search` / `ListNamespaces` 这条路没有。

### 我们的决策

**发现步按错误种类分档 + 分片扇出走统一仲裁**（`compose_v2.PickFanoutError`，与图引擎超步
仲裁**共用同一份判据本体** `pickFanoutError`，不是两份要手工同步的拷贝）。

判据不是「返回值几个槽」，而是**有没有一个先于降级策略执行的分类步**——我们用
`compose_v2.IsRetryFutile`（fallback guard 与框架重试守卫共用；retry 默认谓词 `IsRetryable` 仍走 `IsControlFlowSignal`，见中心 backlog），形状对应
LangGraph 的 `GraphBubbleUp`。ES 那种「默认且无条件降级」容不下中断：中断是「必须停下来
等人」，跟「缺就缺了继续给结果」直接冲突。

**已知不足（记一笔，不改）**：这道分档是在补 `store.Store` 签名 `([]*SearchItem, error)` 的
表达力缺口。`doc/v2/store.md` 的「故障降级策略」明写降级策略**业务自定**，但业务没有槽可以
说「我这个配额错也别降级」，于是框架只能按错误种类猜。按 CLAUDE.md「借来的量」判别式：
`error` 这个名字里没有「可不可降级」的意思。不改签名是权衡——Prometheus 的三槽在它自己的
代码里就漏了两处 warning。

**另一处「N=1 正确」形态**：`ListNamespaces` 成功返空 与 报错后降级，走同一条出口
（都落到「查基准 ns」）。目前无害——基准 ns 本来就该查，且报错那条现在多一行 warn 可区分。
真要出问题得等到「本该有 5 个写入者但后端返了空」那天，那天它 `err == nil`。

## 七之四、授权过滤下的分页总数 / 类型开关失败开放 / 无 Close 的 handler 回收（2026-08-25 调研）

三条独立问题，共同点是「我自己推的修法」被证据修正了。

### 1. 带授权过滤的分页，TotalSize 怎么给

**A2A 规范自己是决定性的**：`a2a-tck/specification/specification.md:3075`（§13.1）——
「Authorization checks **MUST** occur **before any database queries or operations that could
leak information** about the existence of resources outside the caller's authorization scope」。
「先无差别查、再在外面逐条滤」正是这条 MUST 禁止的形状。

两个官方实现都是**谓词下推**，没有一个用外层包装：a2a-go `internal/taskstore/store.go:157-161`
（`filterTasks(s.tasks, userName, req)` 后 `totalSize := len(filteredTasks)`）；a2a-python
`database_task_store.py:225-245`（count 从同一个 `base_stmt` 派生，两者结构上不可能分叉）。
a2a-python 的 `TaskStore` ABC 每个方法都收 `ServerCallContext`，三个内置实现全吃
`owner_resolver`——**owner-aware 是 store 的契约不是可选能力**。a2a-go 的接口没有 owner 参数
（只在 ctx 里），这是 Go SDK 相对 Python SDK 的契约弱点。

| 系统 | 做法 | 前提 / 代价 |
|---|---|---|
| PostgreSQL RLS | 谓词在用户查询**之前**求值 ⟹ `count(*)` 天然只数可见行 | 教科书形态；PG 自己承认 referential integrity 绕过 RLS 是明写的 covert channel |
| Kubernetes | 算不准就置 nil，不给一个像总数的别的数（`remainingItemCount`，`store.go:749-757`）| 字段是 `*int64`+omitempty，「不知道」在类型里可表达；且 k8s RBAC 不是行级过滤，它根本没有这个问题 |
| Relay Connection | `totalCount` **不在规范里**（PR #109 被拒，理由是「有些连接根本算不出来」）| 规范层把总数从必答题变成选答题；A2A 相反，proto 标 REQUIRED |
| GitLab | 明确把「短页」标为要避免的：「short pages, which can expose the presence of confidential resources」 | 他们跑的正是先分页后过滤，处方是谓词下推 |
| Shopify | 给下界 + 显式标签（`Count{count, precision}`，EXACT/AT_LEAST）| 「不精确」是类型的一部分不是文档里一句话 |
| Elasticsearch DLS | 坦然承认计数侧信道并写进 limitations | 反例；但它接受的是聚合侧信道，不是主列表的总数字段 |

**我们的实测**（不是推断）：租户 A 30 个 task、B 3 个，B 以 `pageSize=10` 沿 `NextPageToken`
翻 4 页，每页 `PageSize - len(Tasks)` 累加 = A 的精确条数。第一版修法只是把 1 次请求的预言机
降成 4 次。

**决策**：
- 保留外层逐条过滤（纵深防御，抓到过真实内容泄漏；a2a-python 的 `CopyingTaskStoreAdapter`
  也是包装层形态）。
- 把 owner 下推**写进 `WithTaskStore` 契约**并配 `a2a/server/taskstore_conformance`
  （与仓里另外四套并列），断言：跨 owner 列不到 / **满页不因过滤变薄** / TotalSize 是
  per-owner。自检双向：合规后端全过 + 不分区后端实测出 19 条缺口。
- 单条读（`tasks/get` 等）**不进 conformance**：外层包装真正关上了它（比对归属戳 + 错误
  正文归一），而 SDK 自带后端的 Get 本来就不看 user。这套只验「只有后端能修」的计数面。
- `ownerPartitionedTaskStore` 保留但**改措辞**：从「可选 capability」改成「声明遵守了 §13.1
  契约」。不声明 ⟹ TotalSize 给本页条数（偏小但不跨租户）。

### 2. 授权守卫的类型开关失败开放

**编译期路对我们结构上不存在**：`nishanths/exhaustive` 只管 **enum 常量** switch，不管
interface type switch；`go-check-sumtype` 要求接口 sealed（含未导出方法）+ `//sumtype:decl`，
而 `req.Payload` 的静态类型是 `any`、类型全属三方包，两个条件都不成立。

**运行期 default-deny 三家一致**：k8s（`authorization.go:60`，要求产出**肯定**决定，RBAC 不
匹配返 `DecisionNoOpinion` 一路传到底）/ Envoy RBAC（ALLOW = 「if and only if there is a
policy that matches」）/ OPA（`default allow := false`）。

**k8s 还有第二层专门对付「新类型冒出来」**：admissionregistration 在 GA（v1）时把两个默认
都翻成 fail-closed——`FailurePolicy` 从 `Ignore` 改成 **`Fail`**、`MatchPolicy` 从 `Exact` 改成
**`Equivalent`**（`v1/defaults.go:32-38` vs `v1beta1`）。我们踩的正是 Ignore+Exact 的组合。

**「用测试做机械对账」有一等先例且形状几乎相同**：`kubernetes/test/integration/etcd/etcd_storage_path_test.go`
——`t.Fatalf("no test data for %s. Please add a test for your new type...")` + `kindAllowList`
豁免表 + **双向对账**（漏了会红、豁免表 stale 也会红）。

**决策**：机械对账保留（这是这个约束下能拿到的最硬手段）。另补两条：
- 运行期「不认识」信号——测试只在有人跑测试时说话，`go get -u` 之后直接上线的部署一句话都
  不会说。对齐 k8s 的 `DecisionNoOpinion`：不认识是一等状态，不是 `false` 的分身。
- 豁免表从测试搬进生产、两边读同一份（否则又是一份会漂的拷贝）。
- `sdkModuleDir` 的 `t.Skipf` 改 `t.Fatalf`——**跳过的测试是绿的**，而这是唯一一道机械防线。

### 3. 无 Close() 的 handler 里按时间回收挂起会话

| 库 | 起 goroutine | 退出点 | 默认 |
|---|---|---|---|
| `alexedwards/scs/memstore` | 是 | `StopCleanup()` | 1 分钟 |
| `patrickmn/go-cache` | 是（可关）| **`runtime.SetFinalizer`** | 关时靠 Get 惰性判过期 |
| `hashicorp/golang-lru` expirable | 是 | `Close()` | 每 1/100 TTL 扫一桶 + Get 惰性判 |
| `golang.org/x/time/rate` | **否** | — | 纯惰性 |
| `gorilla/sessions` FilesystemStore | 否 | — | **完全不回收**（issue #65 明写） |

**默认值的分界不是「库层 vs 平台层」，是「凭据 vs 工作」**：scs `Lifetime` 24h 开着 /
gorilla CookieStore 30 天开着（凭据缓存，有界寿命本身是安全要求）；LangGraph store
`default_ttl` 默认 None、平台层 `omitted = 不配置`、a2a-python 与 a2a-go 的 task store
**连 TTL 概念都没有**（未完成的工作，删掉是毁数据）。最贴的一条对照是 scs 的
`IdleTimeout`——**同语义，默认 0（关）**。

**决策**：
- 默认关：站得住，证据面齐。
- 惰性：结论对，但我原来给的理由（「没有 Close() 所以只能惰性」）**是假的**——go-cache 的
  `runtime.SetFinalizer` 已经解掉，且返回类型是我们自己选的。godoc 改成「**选择**惰性因为
  它不引入生命周期负担、回收精度本来不重要」。
- 采纳 LangGraph `omit_expired` 的形状：**把「读时判过期」和「何时真删」拆开**
  （`libs/checkpoint/langgraph/store/base/__init__.py:546-570`，`omit_expired` 让过期项在
  真删之前就不再返回）。不拆的话回收延迟就是语义延迟——而惰性扫描恰恰在低流量时最不工作，
  跟「低流量下占几个月」这个动机在同一个维度上对不上。
- 未采纳：交给 checkpointer 层 TTL（只解一半——进程内的 RunSession/busy 标记它管不到，且
  `compose_v2.Checkpointer` 没有 TTL 语义）；注入 clock/sweeper hook（前者只有测试价值，
  后者把生命周期问题原样甩给业务）。

## 七之五、panic 栈放哪儿（2026-10-15 调研与决策）

起因：全仓的 `recover()` 兜底点都不保留调用栈（只有路由影子档那处日志带了栈），线上排查拿不到 panic 发生在哪儿。
要定的是：栈挂在错误上还是只记日志；错误文本里放不放栈；要不要上限；进不进 span / 存档；panic 值是 error 时
要不要经 Unwrap 露出来。

### 各家的选择

| | 选择 | 解决的约束 | 约束成立的条件 | 代价 |
|---|---|---|---|---|
| **eino**（2026-09-29） | 节点任务、并发工具、流、ADK 各层（11 个文件）统一 `internal/safe.NewPanicErr(v, debug.Stack())`：类型不导出、无 Unwrap、**栈拼进错误文本** | 框架自己不记日志、也不给结构化字段，栈只能靠错误文本活到打印它的地方（OnError 回调 → 追踪后端） | 错误文本只给运维看 | 调用方 errors.As 取不到；panic 值是 error 时 errors.Is 认不出；栈跟着错误文本到所有地方（例如可序列化的 `WillRetryError{ErrStr: err.Error()}`） |
| **x/sync/errgroup**（v0.15 加、2025-06 撤回） | `PanicError{Recovered error; Stack}`（有 Unwrap）/ `PanicValue{Recovered any; Stack}`，在 `Wait` 里重新 panic | 初版 Error() **刻意不含栈**，注释原意「Go 的 Error 方法按惯例不含栈，要的从 Stack 字段取」；golang/go#73710 后改成含栈 | 改的理由：`Wait` 重新 panic 后进程崩溃，打印的只有 Error() 和 Wait 处的栈 | 整体撤回（7fad2c9）：panic 被推迟到 Wait 才暴露，本该当场发现的错误被拖后 |
| **sourcegraph/conc** `panics` | `Recovered{Value; Callers; Stack}`，`AsError()` 得到的错误 Error() 含栈、Unwrap 返 panic 值 | 通用并发库，交给调用方处理 | 调用方会拿到并处理这个错误 | 同 eino：栈跟着错误文本走 |
| **net/http**（HTTP/1、HTTP/2）、**gin** | 只记日志：net/http 用 64KB 固定缓冲 `runtime.Stack`；gin 逐帧读源码行打印、无上限。客户端只得到断连 / 没有正文的 500 | handler 没有错误返回通道 | 日志是唯一出口 | — |
| **grpc 恢复中间件** | v1：`status.Errorf(codes.Internal, "%v", p)`，无栈、不记日志。v2 默认：`PanicError{Panic; Stack}`（64KB），Error() 含栈 | — | — | grpc-go 把非 status 错误转成 `Unknown`、原样用 `err.Error()` 作消息（`FromContextError`，本地 v1.83.1 核过）⟹ v2 默认**把栈发给客户端**；同一个函数用 `errors.Is(err, context.Canceled)` 判成 `Canceled`——panic 值若经 Unwrap 露出 ctx 错，就被当成取消 |
| **OTel 语义约定**（本地 2026-06-17 版） | 「异常记在 span 事件」（`exception` 事件 + `exception.stacktrace`）已标**废弃**，改为「异常记成日志，必须关联到对应 span 的上下文」；Go 的 stacktrace 指定为 `runtime.Stack` / `debug.Stack` 的返回值 | — | — | otel-go 的 `RecordError(..., WithStackTrace(true))` 是可选项，栈在调用它的那一刻取、上限 2KB——对已返回的错误调用时取到的不是 panic 处 |

**交给模型 / 前端 / 对端的文本**：调研的 agent 框架都不带 traceback。LangGraph ToolNode 默认只把参数校验错误交给模型、
其余往上抛，开了 handle_tool_errors 也只用 `repr(e)`；OpenAI Agents SDK 从 2026-09-21（#5112）起默认交给模型的是固定文本、
连异常消息都不给，完整 traceback 只进服务端日志；MAF 默认「Error: Function failed.」，traceback 经 OTel `record_exception` 进 span；
Codex 的工具任务 panic 变成 Fatal 结束本轮，消息只有 panic 信息；a2a-python 给对端 `str(error)`；AG-UI 官方 LangGraph 集成的
`RUN_ERROR` 只有 `str(exc)`。唯一的反例是 AG-UI 社区的 agent-spec 集成：traceback 拼进异常文本、随 `RUN_ERROR` 发到浏览器。

**运行时的帧数上限**：`runtime.Stack` 与 `debug.Stack` 走同一个 traceback，只打最内 50 帧 + 最外 50 帧
（`tracebackInnerFrames` / `tracebackOuterFrames`，本地 Go 1.26.4 源码核过；实测 1 万层递归输出 `...9907 frames elided...`、约 20KB）。

### 我们的决策

| 问题 | 决定 | 理由 |
|---|---|---|
| 栈放哪儿 | **挂在错误上 + 兜底点记日志**，两者都要 | 只记日志的前提（没有错误返回通道）我们不成立；只挂错误的前提（调用方拿到错误会自己处理）也不成立——AgentAsTool 把子 agent 的错转成工具失败、AutoCompact 失败只记 WARN、fan-out 多分支失败只返一个，这些路径把错误吃掉只留文本 |
| 错误文本放不放栈 | **不放**，与原来逐字相同 | 错误文本会交给 agui 前端（RUN_ERROR）、a2a 对端（failed 状态）、AgentAsTool 转给模型的工具失败、span 的 `error.message`。errgroup 改成含栈的唯一理由（重新 panic 后崩溃打印只有 Error()）我们不成立；含栈的那几家都是不记日志的通用库，grpc v2 的代价已实证 |
| 上限 | **不另设**，用 `debug.Stack` | 运行时只打 100 帧；栈只进内存里的错误对象和一条日志 |
| span / 存档 | **都不进** | 存档没有错误字段；span 的 `error.message` 仍是不含栈的错误文本。OTel 已把异常从 span 事件挪到日志（GenAI 约定的 `gen_ai.client.operation.exception` 也是日志事件）。以后要在 trace 界面直接看栈，要接 Logs API 记一条关联到 span 的异常日志，按「不默认采集」做成开关 |
| 类型放哪儿 | **一个内部小包 `internal/panicerr`**（只依赖标准库），`compose_v2.PanicError` 是它的类型别名 | eino（`internal/safe`）与 conc（`panics`）都是「全仓一份实现」；mcp / schedule / model/openai 在 compose_v2 之下或完全独立，也能挂同一个类型 |
| panic 值经 Unwrap 露出来 | **不露**：`PanicError` 不实现 Unwrap，作为并列的第二条分支挂上（多分支 `Unwrap() []error`），各兜底点原来用 `%w` 还是 `%v` 照旧 | errgroup / conc 露出来的前提是调用方不靠错误类型判控制流；我们靠（中断、ctx 错、软取消、陈旧写回）。露出来会把「某处 panic 了一个中断」变成真中断。代价：单层 `errors.Unwrap` 对挂了记录的错误返 nil（仓内无此用法，判断一律 errors.Is / errors.As） |
| 持锁区里的兜底点 | `ApplyAll`（业务 reducer）、`Restore`（业务 UnmarshalJSON）当场只挂记录、标「待补记」，由 `compose_v2.Run` 出口在锁外补记 | 它们的 defer 跑在 cm 写锁里，日志 handler 是业务代码；提交区另有机械哨兵不许加调用 |
| 日志 handler 是业务代码 | 格式化与调 handler 放在 `panicerr.Log` 自己起、自己等的 goroutine 里 | 不少兜底点先记日志、后记结论；handler panic 或调 `runtime.Goexit`（测试里「一有错误日志就 t.FailNow」的 handler）时 recover 拦不住 Goexit，结论就没记下——审计实测 fan-out 成员的失败变成幽灵成功、图主体 panic 被当成功、存档被清掉、收尾闭包跳过 OnAgentEnd。放进单独的 goroutine 是结构上隔开，不靠每个兜底点记得把日志排在最后 |

实现与分工见 `compose_v2/panic_recovery.go`「栈与日志」；哨兵 `compose_v2/panic_stack_test.go`、`agent_v2/panic_stack_test.go`、
`internal/panicerr/panicerr_test.go`（栈里有 panic 发生处的函数名、错误文本不含栈、日志条数、两种写法的 errors.Is 判定不变），
17 个变异逐个实跑变红。

## 七之六、流式输出何时算完整，建流出错要不要改走非流式（2026-10-16 调研，2026-10-19 决策）

> 起因：`model/openai` 的 `consumeStream` 读出错、ctx 取消、错误帧都静默收尾，消费方读到 io.EOF，把半截内容当成完整答复；
> `agent_v2.streamOrGenerate` 对 Stream 的任何错误都改调 Generate（429 不看 Retry-After 立刻再打，配了 fallback 时整条候选链再走一遍）。

### 「流完整了」的凭据

| | 凭据 | 缺了怎么办 | 逃生口 |
|---|---|---|---|
| OpenAI Go / Python SDK | 不判，连接干净关闭即结束，留给调用方（Python 的 Responses 流例外，缺 `response.completed` 抛错） | — | — |
| Codex（只接 Responses） | `response.completed`（`codex-api/src/sse/responses.rs`） | 报 `stream closed before response.completed` | 无 |
| pi-ai（接所有 OpenAI 兼容接口） | finish_reason（`ai/src/api/openai-completions.ts`） | 报 `Stream ended without finish_reason` | 逐模型 `supportsFinishReason`，默认开 |
| openai-agents-python | finish_reason | `ModelBehaviorError` | 只对官方 OpenAI 端点强制（`require_finish_reason=is_openai(client)`） |
| Vercel AI SDK（据网页上的源码摘要） | finish_reason | finishReason=error 加一个错误 part | 无 |
| crewAI | 不判 | 流中途异常且已有文本，当成功返回半截（`llm.py`） | — |

错误帧都按 data 里的 `error` 键认（Python SDK 按真值判）；没有一家靠 `event:` 名或顶层 `code`/`message` 判错，非标准的错误帧靠「缺凭据即错」兜住。
`content_filter` 没有一家当正常结束：pi-ai、Codex 报错，ADK 记成 SAFETY 错误码，Vercel 单独标 content-filter。没见过的结束原因 pi-ai、ADK 当错误，
Vercel 归为 other 不报错。SSE 解码合规的是 Python SDK 与 Codex：CR / LF / CRLF、冒号后只去一个空格、多行 data 以 `\n` 连、流末尾没空行的事件丢弃。

### 建流出错时

没有一家对任意错误改走非流式：要么同一个请求按 Retry-After 退避重试，要么换模型；唯一的流式改非流式是 crewAI，只在一个 chunk 都没收到时。
重试与换模型叠在一起时，各家都保证同一个模型上只有一层重试：
- ADK 的 `FallbackModel` 每个模型只试一次，单个模型的重试归它自己的 `retry_options`，注释写明「免得一次失败被两层各重试一遍」；Gemini 不配就不重试。
- eino 的 ChatModelAgent 把重试包在换模型里面：先在当前模型上按 `ModelRetryConfig` 重试，用尽再由 `ModelFailoverConfig` 换下一个。
- openai-agents-python 在框架的重试策略接管后，后续尝试关掉 OpenAI 客户端内置的重试（`max_retries=0`）。
- pi-ai 创建 SDK 客户端时设 `maxRetries: 0`，自己实现一层、判据照抄 SDK。
- LangChain 的 fallbacks 文档提醒：用 fallback 时关掉模型包装层的重试，否则第一个模型会一直重试、轮不到后面的。

默认重试 2 次的是 SDK 本身（OpenAI Go / Python SDK），而且它们不照很长的 Retry-After 干等：Go SDK 只认 1 分钟以内的，Python SDK 只认 2 分钟以内的，
pi-ai 超过 60 秒直接报错。

### 我们的决策

| 项 | 决定 | 理由 |
|---|---|---|
| 完整的凭据 | 收到 finish_reason（Generate 按同一套结束原因判断） | `[DONE]` 与连接关闭只说明传输结束了。读出错、ctx 取消、错误帧、data 不是 JSON、流结束时没收到结束原因，都作为流的错误交出 |
| 逃生口 | `Preset.OptionalFinishReason`，默认强制 | 我们没有「官方端点」可划界，只能做成 provider 的数据（同 pi-ai 的逐模型开关）。实测 dashscope、deepseek、公司网关都发 finish_reason |
| 结束原因 | `content_filter` 与智谱的 `sensitive` 报 `model.ErrContentFiltered`，智谱的 `model_context_window_exceeded` 报 `model.ErrContextWindowExceeded`（都不可重试）；DeepSeek 的 `insufficient_system_resource` / `aborted`、智谱的 `network_error`、vLLM 的 `error` / `abort` / `repetition`、Mistral 的 `error` 报可重试的错；`length` 与没见过的取值按完整处理 | `length` 要不要让业务看见是 2026-10-09 定的「先不动」（见 todo）；没见过的取值多半是正常结束的变体，当错误会让整轮失败 |
| SSE 解码 | 按规范，唯一不同是流末尾没空行的最后一帧照常处理；读出错或行超过上限时，缺了行的事件丢弃 | 规范要求丢弃，丢了会把放在那一帧的 finish_reason 当截断；半截的帧解析 JSON 照样报错 |
| 建流出错 | 只在 `model.ErrStreamUnsupported` 或 nil reader 时改调 Generate；不接受 stream=true 的端点用 `WithStreamUnsupported` / `Preset.StreamUnsupported` 声明；`OnModelEndStream` 报错或返回 nil reader 让这次调用失败 | 业内没有对任意错误改走非流式的；改调 Generate 会让模型被调两次。框架不从 400 去猜「是不是不支持流式」 |
| 重试分层 | 只有 `fallback` 的逐成员重试一层，openai 客户端不重试；fallback 的 Stream 遇到不支持流式的候选就地用它的 Generate | 同 ADK、eino 的形状；客户端再加一层会跟逐成员重试叠加。交回调用方改调 Generate 会从第一个候选重走，刚失败的候选立刻又被打 |
| 只有一个片段的流 | BasicAgent 整条采用那个片段，不做增量拼接 | fallback 把不支持流式的候选的 Generate 结果包成一个片段；拼接只拼内容、推理、拒答、工具调用、标注与用量，会丢消息 ID、其余内容块、结束原因，还把同一 ID 的两个工具调用并成一个，跟直接调 Generate 拿到的不一样。多片段的流仍按 ID 拼工具调用（见 todo） |
| 响应里报的输入问题 | fallback 默认策略对 `ErrContextWindowExceeded`、`ErrContentFiltered` 不切换、不重试；候选的 Generate 返回 nil 消息两条路都按候选故障处理 | 同一类错误经 HTTP 400 报上来时不重试、不切；这两个哨兵包在 StatusCode 为 0 的 APIError 里，按状态码会被当成网络错误，实测每个候选请求 3 次再切换 |
| 消费方看到的顺序 | `schema.StreamReader` 交出的错误排在它之前发出的项后面 | eino 把项与错误放在同一个有序队列里；我们的项与错误分两个通道，取到错误时先交 ch 里剩下的项 |

没做、记在 todo：fallback 在建流成功时就记成功（eino 在流消费完才记，ADK 首个响应之后不再切换）；流没有空闲超时（Codex 默认 300 秒，grok-build 另有「没进展」计时器）；
消费方关流不中止 HTTP 请求。

## 八、参考资料

- LangGraph 源码：`/Library/Python/3.9/site-packages/langgraph/errors.py`、`types.py:106-126`、`pregel/_executor.py:78-108`、`pregel/_retry.py:26-104`、`pregel/_loop.py:786-842`、`_internal/_retry.py`
- eino 源码：`compose/error.go:54-111`、`compose/interrupt.go:110-237, 314-363`、`compose/graph_run.go:241-487`、`compose/graph_manager.go:286-291, 353-451`
- crew-go 源码：`compose/safe_nodefunc.go:30-42`、`compose/checkpoint.go:184-260, 880-992, 1016-1142, 1236-1430, 545-625`、`compose/superstep.go:91-109, 200-377`、`compose/retry.go`、`compose/circuit_breaker.go:40-110`、`compose/orch_hooks.go:63-138`、`compose/errors.go`
- 我们的相关 ISSUE：ISSUE-46（superstep 部分失败 best-effort + per-branch delta）/ ISSUE-65（CircuitBreaker 引入 + AutoCompact per-session breaker）/ ISSUE-68（ReactiveCompact retry MaxAttempts=3）/ ISSUE-70 Phase 1 Status 字段 / ISSUE-70 Phase 2 Step Ⅰ NS 物理分离冒泡识别
- 同目录文档: `checkpoint-storage.md` (cp Status 档位与续跑闸门) / `interrupt-resume.md` (InterruptError + AskQuery 协议) / `subgraph-isolation.md` (嵌套 cp 物理分离)

---

# 工具边界上的错误：传还是不传（2026-08-27 三方实挖）

> 起因：审计发现 `AgentAsTool` 用 `IsControlFlowSignal` 判「要不要跨 tool 边界透传」，
> 而那个谓词是为「要不要重试/切换」造的。实测子 agent 返 `ErrRunAborted` 被吞成
> `tool.Failure` + `err=nil`。
>
> **调研结论部分推翻了我原来的推荐**，记在这里，包括推翻的那部分。

## 三家的机制（源码实挖，非文档）

| | 机制 | 谁做决定 | 落在哪个默认 |
|---|---|---|---|
| **Codex**（`codex-rs`，2026-08-21） | `FunctionCallError` **只有两个变体**：`RespondToModel(String)` / `Fatal(String)` | **错误的产生处**——每个 handler 自己选 | 无默认，**必须选** |
| **LangGraph**（2026-08-11） | `GraphBubbleUp` 标记基类；`ToolNode` 里 `except GraphBubbleUp: raise` 排在通用 handler 之前 | **异常的类型**（继承关系） | 不在集合里 ⟹ 转 `ToolMessage` 给模型 |
| **eino**（2026-07-22） | 无机制。`adk/agent_tool.go`: `if event.Err != nil { return "", event.Err }` | 无 | **全部透传**；`ToolMessage` 只在成功路径构造 |
| **我们** | `IsControlFlowSignal(err) \|\| ctx.Err() != nil` | 一个**为别的问题造的**谓词 | 不在集合里 ⟹ 转 `tool.Failure` |

**三家都把这个决定显式化了，我们把它外包给了一个名字不匹配的谓词。** Codex 的命名最直白：
`RespondToModel` / `Fatal`——**名字里就是它做的事**，正是本仓「给新用途起一个属于它自己的
名字」那条规约的样板。

Codex 的 `Fatal` 消费点：`return Err(CodexErr::Fatal(message))`，**直接终止整轮**，不回模型；
`RespondToModel` 则 `needs_follow_up = true` 继续循环。

## ⚠️ 推翻我原推荐的那条证据

**Codex 的 `spawn_agent` / `wait` 全部用 `RespondToModel`，零个 `Fatal`。** 它**有** Fatal
这个选项，却不给子 agent 失败用。LangGraph 的冒泡集合（`GraphDrained` / `GraphInterrupt` /
`ParentCommand`）**全是控制流，没有终局失败**——所以它在这个问题上的答案跟我们**现在**一样。

**按「谁支持透传」数票：3 家里只有 eino，而 eino 根本没有「工具错误给模型看」这个机制，
不算在这个轴上做过选择。**

## 但票数不是答案——真正的判据是「失败的是任务还是代码」

查 Codex 什么条件才配 `Fatal`：

| 位置 | 条件 |
|---|---|
| `tool_search.rs:197` | payload 来的是**错误的 variant**——框架自身的分发不变量被破坏 |
| `request_permissions.rs:110` | `serde_json::to_string` 在**我们自己控制的**响应结构上失败 |
| `current_time.rs:98` | 读系统时间失败 |

**`Fatal` = 「代码/不变量出错」，`RespondToModel` = 「任务失败，模型可以换条路」。**
Codex 的 spawn 失败之所以是后者，是因为那些失败是 agent 数量上限、模型名非法之类的
**运营性失败**——不是协议违规。

而我们的 `ErrRunAborted` 的来源都是后一类里没有的东西——**按类目说**（权威清单在
`compose_v2/errors.go` 的 `ErrRunAborted` godoc，它自己记着「按类目写、别数个数」的理由：
这里原先枚举的「五条」在写下之后就被加过三次，而个数写在清单之外、加一条时没人会想起改它）：
cond 两档（`CondFailAbort`、返回未注册 target） / 业务抛的中断请求形状非法（由 `validateInterrupt`
统一判） / 单写入者冲突（channel 与上行信号两种） / 扇出宽度失控 / 以及上层包的协议违规（agent_v2：
转交目标非法、session 的任务身份对不上……；a2a 也有）。**这里也不是全集**（2026-10-23 对账：27 个
wrap / Unwrap 点横跨三个包的 10 个文件，当时三份清单一份都不完整），要全集照 godoc 里那条取法取。**全是业务代码的错，不是运营性失败**——要么违反框架
协议，要么把终止条件写错，重跑都走不通（宽度与单写入者那两条的理由另有一层：矛盾/爆炸会随存档
反复复现，见 godoc）。按 Codex 自己的判据，这是 `Fatal` 那一档。

## 结论与代价

**我们的缺陷不是「选错了」，是「从来没做过这个选择」**——借了一个回答第三个问题的谓词。
三家都有明确答案这件事本身，比它们各自选了什么更重要。

**Codex 的形状比它的选择更值得学**：决定做在**错误的产生处**（知道为什么失败的那段代码来
选），不是在边界上事后猜。我们的子 agent 错误来自任意位置，边界谓词是现实选择，但至少要
**给它一个属于自己的名字**，别继续借 `IsControlFlowSignal`。

⚠️ **一条 caveat**：Codex 里 spawned agent 是独立 thread 跑自己的 turn，它的 `Fatal` 终止的
是**它自己那一轮**，父只拿到一个 status。我们的子 agent 是直接把 error 返进父的 tool 调用。
架构不同，所以「Codex 给子 agent 用 RespondToModel」不能直接推出「我们也该」。

## 2026-10-08 复核：LangGraph 现在的默认方向跟我们相反；集合补漏

读本地 clone 的 `libs/prebuilt/langgraph/prebuilt/tool_node.py`：上表 LangGraph 那一行
「不在集合里 ⟹ 转 ToolMessage 给模型」**不是它现在的默认**。ToolNode 的默认 handler
`_default_handle_tool_errors` 只把 `ToolInvocationError`（工具参数不合法）转成 ToolMessage，
**其余原样抛**；`GraphBubbleUp` 一类照旧排在最前、永远抛。要全吞得显式传
`handle_tool_errors=True`。eino 默认整个往上抛（`compose/tool_node.go` 的 `wrapToolCall`）。
OpenAI Agents SDK 默认全吞（`default_tool_error_function` 把任何异常格式化给模型，显式设成
None 才抛），但它没有存档类的哨兵。Codex 没有默认，产生处必须二选一。

所以「默认吞、名单里的传」在这几家里只有 OpenAI 跟我们同向。这次**不翻默认方向**（那会改掉
所有吞点的行为，是另一个决定），只按我们自己的判据（失败的是任务还是代码）补集合里漏掉的：

- **`ErrGraphIDConflict` 收进 `MustPropagate`**：嵌套子 agent 撞上它只有一个含义——同一个父
  sid 上先后跑了拓扑不同的两棵编排树，出路在编排侧，模型换什么参数都撞同一个槽。曾被
  AgentAsTool 转成工具失败、`err == nil`（实测）。顺带让它实现 `NonRetryable`：那个接口的
  godoc 本来就把「身份冲突」列为确定性错，这个哨兵却是 `errors.New`，`IsRetryable` 把它当
  可重试。
  （2026-10-12 合入嵌套调用身份那批改动后补注：撞上它其实有两个含义——另一个是节点里裸调
  `sub.Run`、存档落进父图自己的槽；默认 schema 的 AgentAsTool 换了参数会落到另一个槽、照样跑。
  收进集合的理由改成「两个含义都是编排结构出了问题，模型换不换参数都修不了」，见 `ErrGraphIDConflict`
  godoc。）
- **`ErrNoPendingCheckpoint` 不收**：在吞点上出现时说的是**被续的那个任务**的外部状态（已经
  结束、从没跑过），模型可以据此改口，属于任务失败。哨兵两个方向都钉了。
- **`run_skill_action` 改问 `MustPropagate`**：它手写了「中断 + 链上的 ctx 错」，动作返回
  `ErrRunAborted` / `ErrSoftCanceled` 被转成一条工具结果、模型接着答完、Run 照常成功（实测）。
  它没调 `IsControlFlowSignal`，所以那道机械闸也查不到它——正是 `MustPropagate` godoc 说的
  「谁都不问的吞点对按名字搜的闸完全隐形」。
- **对外的两个出口跟上**（同日审计）：agui 给身份冲突专属 code `graph_id_conflict`（落进通用的
  `agent_error` 会让前端原样重来，而它重跑必然撞同一个槽），a2a server 以 failed 收尾；两边文案都
  脱敏（原文里有 sid / ns / 两张图的 GraphID）。引擎在中断出口写档失败时返回「中断 + 写档原因」
  双 `%w`，「中断 + 身份冲突」那份中断的存档没写成——两边都不再当正常中断留着等续发（跟陈旧写回
  同理）。（2026-10-19 起出口写档失败返 `ErrCheckpointNotPersisted`、中断不在链上，见
  `doc/v2/checkpoint.md` §3.5；两边的排除留着，防自研 agent 把它们跟中断绑在一条链上。）

---

# 重试粒度与节点超时（2026-08-27 实挖）

> 起因：我们的重试单位是**整个 `sub-agent.Run`**（4 处 `RunWithRetry`），重试一次把
> 已成功的上游全重放（实测外层 retry 3 次 ⟹ 上游跑 4 次）；工具级**无超时**（0 处），
> 唯一的墙是调用方整个 Run 的 ctx deadline。

## 重试的单位

| | 重试单位 | 尝试次数存在哪 |
|---|---|---|
| A 家（图引擎） | **单个 task（节点）**——`run_with_retry(task, ...)`，且每次尝试前 `task.writes.clear()` | task 自身 |
| B 家（ReAct） | **模型调用**——重试计数在 agent state 里（`RetryAttempt`，从存档 `Internals["_retryAttempt"]` 恢复） | **存档状态** |
| 我们 | **整个 sub-agent.Run** | `RunWithRetry` 的**局部循环变量** |

**关键不是「它们粒度更细」，是它们的重试单位 == 存档提交的单位。** 我们的重试包在
存档边界**之上**——那一层看到的只有一个不透明的 `func() (T, error)`，它无从知道里面
哪一步成功了，所以只能整个重放。

⚠️ 两家的尝试计数都活在**被存档的状态**里，不是局部变量。这决定了「重试跨 Resume
还算不算数」——局部变量在 Resume 之后归零。

**已有的半解**：`SaveNodeState` 就是记进度用的，重跑时跳过已完成的部分。

## 节点/工具超时

A 家有 **per-node `timeout`** + `TimeoutPolicy{run_timeout, idle_timeout}`。

⚠️ **值得连它的克制一起学**：**同步节点不允许设 timeout**——编译期 `validate_timeout_supported`
直接拒，外加一个运行时兜底 `raise sync_timeout_unsupported(task.name)`。理由是掐不断一个
同步函数，**给不了就不给这个选项**，而不是提供一个悄悄不生效的 `timeout`。

Go 这边能把派生 ctx 传给工具，但**只有协作式的工具会理它**。所以可移植的教训不是
「照抄 per-tool timeout」，是那条克制：**别提供一个在某些形态下静默失效的超时**——
要么能强制、要么不给，中间状态最坏。


---

# return-direct：终止循环的工具，它的产出算不算答案（2026-08-27 实挖）

> ⚠️ **这条是补做的调研。** 起因是发现 `tool.ReturnDirectMetaKey` 的 godoc 承诺
> 「把该 tool_result 作为本次输出」而实测 content 是空的。当时**没有先查别人怎么做**，
> 直接上了自己想的修法（放宽匹配集），当场破了 4 条既有契约。撤回之后才补的这一轮。
>
> 记在这里是因为「跳过调研直接补」这个动作本身比那个 bug 更值得防。

## 两家有同名概念，答案都是「算」

### Go ReAct 那家：把工具产出存成 agent 的产出

```go
event := typedToolInvokeEvent(callID, toolName, result, toolMsgID)  // 事件带 result
if st.getReturnDirectlyToolCallID() == callID {
    st.setReturnDirectlyEvent(event)   // 存为最终产出
} else {
    execCtx.send(event)                // 普通工具：正常发
}
```

**工具产出就是 agent 的产出**——正是我们 godoc 承诺、而代码当时不做的那件事。

### 图引擎那家：`return_direct` 就是 LangChain 传下来的那个名字

```python
def route_tool_responses(state) -> str:
    for m in reversed(state["messages"]):
        if not isinstance(m, ToolMessage): break
        if m.name in should_return_direct: return END
    ...
```

⚠️ **它只路由到 END，完全不改「答案是什么」**——因为它**根本没有「最终答复」这个槽**：
输出就是整条 message 列表，末尾自然是那条 ToolMessage。

**这一条比前一条更值得记**：`FinalMessage(events)`「从一堆事件里挑一条代表答案」是**我们
自己的构造**，那个「挑哪条」的难题他们不必回答。我们这轮在 return-direct 上折腾的一切
——首个命中 / 最后一个胜 / 按语义挑——**都是这个构造的衍生成本**。

多候选时它是「**尾部任一命中即 END**」，既不是首个也不是最后一个。

## 三处设计差异（形状抄对了、细节不补就是下一个 bug）

| | Go ReAct 那家 | 图引擎那家 | 我们 |
|---|---|---|---|
| 谁声明 return-direct | **装配期**按工具名 | **装配期**在工具对象上（`t.return_direct`） | **运行期**工具在 `Result.Metadata` 里返 |
| 何时决定短路 | 工具**执行之前**（扫 tool_calls 名字） | 工具执行之后（扫尾部 ToolMessage） | 执行之后（扫结果 annotation） |
| 一批多个候选 | 不 break，**最后一个胜** | **尾部任一命中即 END** | 曾是「第一个命中就 break」 |
| 有没有「最终答复」这个槽 | 有（存事件） | **没有**（输出即 message 列表） | 有（`FinalMessage`） |

**只有我们是运行期声明**——这让「哪个 tool_call 会短路」在工具跑完之前不可知，
是我们独有的复杂度来源。

第三行：两家的多候选规则都跟**位置**有关（最后一个 / 尾部任一），我们曾是「第一个」。
而我们仓里那条测试的名字就叫 `FinalOutputOrderIndependent`——它早就在要求「不许由位置决定」，
**这一点上我们的要求比两家都严**，所以按语义挑是对的，不是照抄谁。

## 我们的落点：按语义挑，不按位置

判据是**「它短路之后还有没有别人来说话」**：

- `final_output` 恒胜（契约就是「我的产出即答案」）
- 业务 exit 工具：短路之后没人再说话 ⟹ **产出即答复**
- `transfer_to_agent` / `route_to`：控制动作，接手的 agent 才是答复来源 ⟹ 保持旧形态

比「最后一个胜」强的地方在于它**跟声明顺序无关**；比「第一个胜」强的地方在于
`final_output` 不会被业务工具遮蔽。

## 我编过一句处方，删了

撤回那次我在 godoc 里写了「想改得先把首个命中换成**按优先级挑**」。那句同样没有证据——
没有任何一家用「优先级」这个形状。真正的问题不是优先级，是**让位置参与了语义判定**。

---

# 补做的三处调研（2026-08-28）

> 用户指出：这个 session 里有几处修法是**我自己推的、没查三方**。回头逐条补。
> 三条的结论各不相同——**一条推对了、一条问题是自找的、一条没有先例**，
> 记全比记「都对」有用。

## 一、重试时清「新开」信号 —— 推对了，但顺序错了

问题：重试再调一次成员的 `Run`，而 ctx 上还带着顶层那次 push 的「新开」信号 ⟹
成员内部每层都跳过 auto-load、从头再跑（实测已成功那步跑了 4 次）。

我的修法是「第 2 次尝试起清掉信号」。**补查之后发现那家的重试里有逐字对应的一行**：

```python
# signal subgraphs to resume (if available)
config = patch_configurable(config, {CONFIG_KEY_RESUMING: True})
```

三个细节全对上：设在 sleep **之后**（第 2 次尝试起）/ 目的写明是**让嵌套层去续跑** /
`(if available)` 表示没存档时安全降级。极性相反只是因为我们顶层默认 fresh、它们从输入推断。

**这条唯一的问题是顺序**：先推后验，而不是先查后做。推对了是运气，不是流程。

> ⚠️ **2026-09-23 本地源码实测更正：那行代码的注释与它今天的实际效果不符**，所以它只在
> **意图**上是先例，行为上不是。节点级重试时，对方子图调用的计数器不重建，重试那次子图的地址
> 变成 `…|1`、从头跑（子图直接作节点、节点里调子图两种写法都是 `child_a` 跑了 2 次）。当年断言
> 「重试只重跑失败节点」的测试 `test_subgraph_retries` 在 `46b652a74`（2025-03-20）被删，
> 同一个 commit 让存档查找开始认调用序号后缀，commit message 只写了「修掉多个子图意外共享存档」。
>
> 我们这条修法的结论不受影响，但理由要换成自己的：成员重试时子 ns 在重试循环外算一次、跨尝试
> 不变；节点里嵌套调用的次序计数器随每次节点执行新建、从 0 数起（`compose_v2.NextSubCallIndex`），
> 而 `RunWithRetry` 进入时给它拍快照、第 2 次尝试起每次先退回快照——经 `RunWithRetry` 的
> 重试（输入不变时）算出同一个身份，捡得到自己的存档，第一次尝试里已完成的嵌套调用直接拿回
> 记下的结果（2026-09-28 起）。**手写的重试循环没有这个待遇**：计数器分不出「重试同一次调用」
> 和「用同一份输入再调一次」，每次都算新调用、从头跑。（快照是 2026-09-23 审计
> `dec35f8d` 之后补的：只有「随节点执行新建」时，同一次节点执行里的几次尝试共用一个计数器，第 2 次
> 尝试拿到第 1 次调用的编号，实测副作用执行 2 次。）

## 二、记忆键的内容指纹 —— 问题是自找的，别人没有

我给「框架自动写记忆」这条路设计了内容指纹（含 marshal 失败时的局部降级）。**三家都查了**：

| | 结果 |
|---|---|
| 图引擎那家 | store 是 `put(namespace, key, value)`——**键由调用方给**；`fingerprint`/`content_hash`/`digest` 零命中（只有一个 batch 层的**操作**去重，跟内容无关） |
| Go ReAct 那家 | adk 里**根本没有 store 层**；指纹类符号零命中 |
| CLI 那家 | 指纹 / `content_hash` / `blake3` / `sha256` 给记忆定键：零命中 |

**三家都没有这个问题，因为没人替业务自动写记忆、自己编键。** 我们的指纹是
「框架自动持久化最终答复 + 框架自己编键」这个设计选择的**衍生品**。

⟹ 指纹本身在我们的前提下是对的（自动写就必须有幂等键，内容派生是唯一站得住的选择），
但**更上游的问题没人问过**：`WithStoreMemory` 要不要允许业务给键函数？
那样指纹连同它的降级路径一起变成可选项。**记进 backlog，不在这次动。**

## 三、字段「处置标记」+ AST 对账闸 —— 没有先例，因为正解让它没必要

我给 `SkillDescriptor` 每个字段加了 `[框架消费]` / `[业务自负]` 标记，配 `go/ast` 机械闸。

**三家都查了**：**没有一家**有字段级的「解析了但不强制」标记，也**没有一家**对配置结构做
AST 级对账的测试。它的做法是调研里
早就给出的那条——**schema 收到只剩真的兑现的字段**（skill frontmatter 只有 3 个，我们曾 15 个）。
不解析的字段不可能变成「解析了但没兑现」，也就没什么可标的。

**但另一半有先例，而且两家都有**：字段在某个前提下静默不生效时，**出声**——

- CLI 那家：`"mcp_servers.{name} uses unsupported bearer_token; set bearer_token_env_var"`
  ——点名**替代品**。
- 图引擎那家：`` "`durability` has no effect when no checkpointer is present." ``
  ——点名**失效的前提**。

⚠️ **两条都是有条件的**：只在「设了它但它不会生效」时喊，不是每次加载都喊。
我们对 `AllowedTools` 是这么做的（`slog.Warn` 点名 `WithToolFilter`）；
`UserInvocable` 没有 warn，只在 godoc 里说——**那是对的**，因为它不是「不生效」，
是「按设计由业务自己兑现」，每次加载都喊就是噪声。

⟹ 标记机制是我们独有的，**定位应当写清楚**：它不是「表达处置」的通行做法，
是**防回归的闸**（挡住下一个未兑现字段混进来）。正解仍是收窄 schema，标记是它的护栏。


---

# 「这是不是框架的工具」怎么判：名字，还是来源（2026-08-28 实挖）

> 起因：audit F1——return-direct 消费侧用按**名字**匹配的 `isFrameworkTool(n)` 判框架身份，
> 而名字是开放集（业务注册 `respond` / `route_to` 就撞上，`OnToolStart` 改写 `input.Name`
> 是 [消费] 档）。实测撞名工具「体真跑了、短路真发生了、产出被丢了」，err=nil。
> 而 `tool_memo.go` 早为一个真安全洞学过这条教训——**同一个判定在仓里有两份判据**。

## 三家的做法：没有一家在消费侧拿裸名字判「这是我的工具」

| | 机制 | 身份编在哪 |
|---|---|---|
| CLI 那家 | `ToolName` 是**结构体** `{name, namespace: Option<String>}`；MCP（外部）工具的 namespace 由**注册路径**分配（`ToolName::namespaced(server, tool)`），外部工具没有机会落在 default namespace | **来源决定身份**，编进类型 |
| Go ReAct 那家 | `transfer_to_agent` 名字只是常量，用于注册/展示；**转交信号是类型化 Action**，从真的框架工具体内部经 ctx 发出（`SendToolGenAction`）——业务撞名工具的函数体发不出那个信号 | **只有框架体能发的信号** |
| 图引擎那家 | 内置结构化输出**根本不进工具列表**（单独节点 `with_structured_output` 再调一次模型），不与业务工具共享名字空间 | **回避共享名字空间** |

三家形状不同、结论一致：**名字用于展示与注册，身份另有载体**——类型/命名空间、
只有框架能发的信号、或压根不共享空间。

## 我们的落点

写侧（手上有 toolkit 和真实实例的那层）用铸造事实 `isFrameworkControlToolIn` 判掉，
作为 `ReturnDirectMintedAnnotation` 随消息走；消费侧读**结构化事实**，不查名单。
≈ CLI 那家的「来源决定身份、身份随对象走」+ 本仓 `frameworkControlTool` 既有先例
（≈ Go ReAct 那家的「只有框架能铸」方向）。

顺带（audit F2 失重实验）：这道守卫真正承重的是**历史可见性**（Rewrite 模式后接看前接
说了什么），不是注释原来写的「答复来源」——拆掉守卫答复行为完全不变，唯一红的是历史
那条。理由已改写进守卫注释；单元哨兵（铸造 vs 撞名同名分叉）与端到端哨兵（后接看前接）
互补，双向 sanity-revert 各红各的。
