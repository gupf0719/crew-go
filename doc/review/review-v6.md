# Code Review #6 — v2 全代码库 (2026-05-18)

> **⚠️ ISSUE-82 后口径更新 (commit `6bb4c35` 2026-05 中旬)**:
> `hook.Hooks` 9→8 method, `OnAgentInit` 已删, `OnAgentStart` 改为整个 Run 调用 1 次 +
> 每次 Resume 调用 1 次 (agent_v2 wrapper Run/Resume top-level fire, 跟 OnAgentEnd
> 单一所有权对称). 因此**本审 H-05 finding "PlanAgent + GraphAgent OnAgentInit 不
> fire" 已不适用** — 现在所有 9 编排都通过 agent_v2 wrapper 入口 fire OnAgentStart,
> hook 协议对称完成. 下文 line 156 / 162 / 169 / 577 / 597 等 OnAgentInit 提法保留
> 为审计历史记录, 业务对照当前代码请以 [`hook/hooks.go`](../../hook/hooks.go) +
> [CLAUDE.md "Hook 选择速查"](../../CLAUDE.md) 为准.

> **范围**: crew-go v2 全代码库 (`agent_v2/` + `compose_v2/` + `hook/` + `tool/` + `model/` + `schema/` + `skill/` + `mcp/` 共 83 生产文件 ~22k LOC)
>
> **方法论**: 按 CLAUDE.md "多 agent 单视角并发审 > 单 agent 多视角同时审" 规约, 5 视角并发独立审 + HIGH 全 cross-verify (file:line grep 验证真伪)
>
> **5 视角**:
> 1. 并发安全 / race 风险 / 共享状态
> 2. 错误传播 / 控制流 (InterruptError / ctx.Err / panic recover / wrap chain)
> 3. cp / Resume / NS 一致性 (嵌套子图)
> 4. 资源生命周期 (goroutine / channel / stream / file / conn / defer)
> 5. 9 编排 API 对称性 + hook 协议完整性
>
> **汇总**: 9 HIGH (cross-verify 后) + 26 MED. 其中视角 2 HIGH-2 与视角 3 HIGH-1 跨视角共识同一 bug (PlanAgent execInputBuilder ctx 误用), 强可信。

---

## 总览

| 视角 | HIGH | MED | 备注 |
|---|---|---|---|
| 1. 并发 race | 0 | 0 | race detector 全包 PASS (含 ISSUE-81 fanout / R33 nested lane / cross-session breaker stress) |
| 2. 错误传播 | 6 | 9 | (1 HIGH cross-verify 后降 MED) |
| 3. cp/Resume/NS | 2 | 4 | (HIGH-1 与视角 2 HIGH-2 共识同 bug, 合并) |
| 4. 资源生命周期 | 4 | 7 | (2 HIGH cross-verify 后降 MED) |
| 5. API 对称性 | 3 | 9 | |

去重 + 调整后: **9 HIGH + 26 MED**

---

## HIGH 级 finding

### H-01 ★ PlanAgent worker `execInputBuilder` 用 outer ctx 而非 workerCtx (audit³ LOW-A1 commit 自相矛盾)

**严重度**: HIGH
**跨视角共识**: 视角 2 HIGH-2 + 视角 3 HIGH-1 独立报同一 bug
**位置**:
- [agent_v2/plan_agent.go:553](agent_v2/plan_agent.go:553) (SubAgentName worker)
- [agent_v2/plan_agent.go:611](agent_v2/plan_agent.go:611) (TmpSubAgent worker)
- [agent_v2/plan_agent.go:654](agent_v2/plan_agent.go:654) (executor worker)

**证据**: 三处都是 `p.execInputBuilder(ctx, ...)` 用闭包**捕获的 outer ctx** (`buildPlanGraph(ctx, session)` 入参, 即 Plan Run/Resume 入口 ctx), 而**不是**闭包入参 `workerCtx`. **注释明确说"用 workerCtx (worker scope)"——代码与注释 100% 矛盾**:

```go
// audit³ LOW-A1 fix: 用 workerCtx (worker scope) 不是 outer ctx (Run/Resume 入口) — 业务 ExecInputBuilder 读 ctx.Value (trace ID / per-step flag) 应拿 worker scope
input, builderErr := p.execInputBuilder(ctx, original, highExecuted, highCurrent, resolvedRefs)
//                                       ^^^ 应为 workerCtx
```

**触发场景**: 业务自定义 `ExecInputBuilder` 读 ctx-scoped 值 (per-step trace ID / per-worker flag / `compose_v2.GetCurrentNodeID(ctx)` / OnNodeStart 注入的节点元数据).

**影响**:
1. 业务 ExecInputBuilder 读 `compose_v2.GetCurrentNodeID(ctx)` 永远空 — 与 worker 真实节点 ID 脱钩
2. 业务读 trace ID 等 ctx 值 — 拿不到 RunWithRetry / hook 链注入的 per-step 上下文
3. 与 commit 注释 ("audit³ LOW-A1 fix") 完全相反 — 自相矛盾代码 + 测试覆盖明显不足

**修复方向**: 3 处 `ctx → workerCtx`, 与同段 `compose_v2.RunWithRetry(subCtx, ...)` ctx scope 对齐.

**sanity-revert**: 业务自定义 ExecInputBuilder 读 `compose_v2.GetCurrentNodeID(ctx)`, 断言其非空 = workerCtx scope 节点 ID. 当前代码必 fail (拿空字符串), 修后 pass.

---

### H-02 BasicAgent toolsFn N≥2 fanout goroutine 无 panic recover → 业务 tool panic 整进程 crash

**严重度**: HIGH (生产 panic 直接 crash)
**位置**: [agent_v2/basic_agent.go:1058-1071](agent_v2/basic_agent.go:1058)

**证据**: ISSUE-81 N≥2 tool_call 并发分支:
```go
go func(i int, tc schema.ToolCall) {
    defer wg.Done()
    callCtx := compose_v2.WithCurrentToolCallContext(ctx, tc.ID, i)
    outcomes[i] = runOne(callCtx, tc)  // ← runOne 内调 dispatchToolkit.Call(...) → 业务 Tool.Call
}(i, tc)
```

`runOne` 内 `dispatchToolkit.Call(callCtx, name, args)` → [tool/tool.go:197-206](tool/tool.go:197) `Toolkit.Call` → `t.Call(ctx, args)` (业务自定义实现) — **全链无 defer recover**.

`tool.Tool` godoc 明示 reentrant 约定 ([tool/tool.go:13-37](tool/tool.go:13)) 但**没约束"不能 panic"**. 业务 Tool 实现 nil deref / map 并发写 / SDK bug panic 时:
- **N=1 inline 路径** ([basic_agent.go:1054-1057](agent_v2/basic_agent.go:1054)) 走 `safeNodeRun` ([compose_v2/graph_loop.go:864](compose_v2/graph_loop.go:864)) defer recover 保护 ✓
- **N≥2 fanout 路径** goroutine 内 panic → Go runtime fatal → 整进程 crash → 主进程死亡 + 其他在跑的请求全丢 + graph_loop defer cleanup + cp save 全跳过

**修复方向**: `runOne` 入口加 `defer func() { if r := recover(); r != nil { /* 转 outcome.err */ } }()`, 与 `safeNodeRun` 同款.

---

### H-03 9 编排 ResumeActionAbort 强制忽略父 graphNS — nested sub-agent.Resume(Abort) 误删父 cp

**严重度**: HIGH (业务 silent 数据破坏)
**位置**: 9 编排 Resume 入口同款模式:
- [agent_v2/basic_agent.go:190](agent_v2/basic_agent.go:190)
- [agent_v2/sequential_agent.go:208](agent_v2/sequential_agent.go:208)
- [agent_v2/parallel_agent.go:242](agent_v2/parallel_agent.go:242)
- [agent_v2/loop_agent.go:194](agent_v2/loop_agent.go:194)
- [agent_v2/routing_agent.go:270](agent_v2/routing_agent.go:270)
- [agent_v2/handoff_agent.go:248](agent_v2/handoff_agent.go:248)
- [agent_v2/reflect_agent.go:252](agent_v2/reflect_agent.go:252)
- [agent_v2/plan_agent.go:377](agent_v2/plan_agent.go:377)
- [agent_v2/graph_agent.go:183](agent_v2/graph_agent.go:183)

**证据**: 全部 `_, _ = cper.DeleteSubtree(ctx, sessionID, "")` — **用 `""` 作 nsPrefix 不 forward `compose_v2.GetGraphNS(ctx)`**.

**触发场景**: 业务 nested 调用 sub-agent.Resume(WithResumeAction(Abort)), ctx 中已 push 父 graphNS=`"plan/step_2"`:
1. 业务想"只取消这个 sub-agent"
2. 实际执行 `DeleteSubtree(ctx, "session1", "")` — `""` 是任意 ns 的祖先 → 清整 session 下**全部 cp**(含父 PlanAgent + sibling sub-agent + 其他业务在同 sid 下的 cp)
3. 父 PlanAgent 后续 Resume 找不到自己 cp → 从 scratch 重跑 / 报错

**对比 Continue 路径**: 9 编排 .Resume Continue 路径都已修齐 `WithRunNS(GetGraphNS(ctx))` (R29 视角 4/5 HIGH-1), **Abort 路径同协议漏修**.

**修复方向**:
```go
if rcfg.action == ResumeActionAbort {
    ns := compose_v2.GetGraphNS(ctx)
    _, _ = cper.DeleteSubtree(ctx, sessionID, ns)
    return nil, nil
}
```

---

### H-04 PlanAgent TmpSubAgent + HITL Resume 死循环 — `LoadAndDelete→Load` fix 与 `cleanupPendingTmpSteps` defer 互斥

**严重度**: HIGH (TmpSubAgent + HITL 业务路径必踩坑)
**位置**: [agent_v2/plan_agent.go:351](agent_v2/plan_agent.go:351) (Run defer cleanup) + [agent_v2/plan_agent.go:585-594](agent_v2/plan_agent.go:585) (dispatcher Load)

**证据**: 两个 fix 互斥:
- **R32+ 视角 1 HIGH-2 fix** (line 586-589 注释): `LoadAndDelete → Load` — 让 Resume 后 dispatcher 重跑还能 Load 到 cfg
- **R32+ 视角 1 HIGH-3 fix** (line 351 注释): Run defer **无条件** `cleanupPendingTmpSteps(session.RunID)` 清整 sess.RunID 前缀

**死循环触发**: 业务用 `PlanAgent.AddSubAgent` 配 TmpSubAgent + sub-agent 内抛 InterruptError (HITL):
1. dispatcher worker 调 ag.Run, sub-agent 抛 InterruptError
2. Run defer fire **cleanupPendingTmpSteps** → 把 stash 删了
3. graph_loop SaveNS cp 保 frontier=[dispatcherWorker], cp 仅含 `*PlanStep{Description, AgentName=tmpDispatcherWorker}` 不含 cfg
4. 用户 `Resume(...)`, cm.Restore 恢复 frontier 后**直接重跑 dispatcher worker**
5. `pendingTmpSteps.Load(key)` miss (已被 Run defer 清掉)
6. 报 `"plan agent %q: tmp dispatcher: no config for description %q"`, HITL 链彻底断

**修复方向**: 把 `cleanupPendingTmpSteps` 移到**仅 success 路径**或**Resume 完成后**触发; 或在 plannerFn 入口 ctx 含 ResumeContext 时跳过 stash 让 dispatcher 不依赖 stash.

**测试缺口**: 当前测试集**未覆盖** TmpSubAgent + HITL Resume 端到端路径 (否则必 fail).

---

### H-05 PlanAgent + GraphAgent 业务 hook 仅 `OnAgentEnd` fire, 其他 8 method silent

**严重度**: HIGH (业务监控 / SLO / 计费 silent 退化)
**位置**:
- [agent_v2/plan_agent.go:344, 394](agent_v2/plan_agent.go:344) (Run/Resume defer 仅 OnAgentEnd) + [compose_v2/plan.go](compose_v2/plan.go) 整文件无 hook 集成
- [agent_v2/graph_agent.go:144-156, 201-213, 246-277](agent_v2/graph_agent.go:144) (`g.hooks` 仅 Run/Resume defer 用)

**证据**: 9 编排 hook 触发对称性矩阵:

| Hook | Basic | Seq | Parallel | Loop | Routing | Reflect | Handoff | **Plan** | **Graph** |
|---|---|---|---|---|---|---|---|---|---|
| OnAgentInit | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | **❌** | **❌** |
| OnAgentStart | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | **❌** | **❌** |
| OnAgentEnd | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OnError | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | **❌** | **❌** |

**影响**:
- 业务监控按 OnAgentInit (Run 首次入口) 统计时 PlanAgent / GraphAgent silently 漏数据
- OnAgentStart (每轮 plan↔exec 循环) 完全不 fire — 统计"agent 处理多少轮"时 PlanAgent missing
- OnError 在 planner 抛业务错时不 fire — 业务通过 OnError 看错误码 / 上报告警 PlanAgent / GraphAgent 全漏
- OnModel*/OnTool* 不 fire — PlanAgent 内部 LLMPlanner 调 LLM 时业务 hook 看不见

**修复方向**:
- 短期 (文档): `WithPlanHooks` / `WithGraphAgentHooks` godoc 明确列出 8 个不 fire 的 method
- 长期 (协议补齐): `compose_v2.PlanBuilder` 加 `WithAgentHooks(h hook.Hooks)`, plannerFn 入口 fire OnAgentInit/Start, exec dispatcher 失败 fire OnError — 完成与其他 7 编排协议对称

---

### H-06 GraphAgent.Resume 缺 `g.graph` / `g.outputReader` nil 校验 — 远 stack panic 难定位

**严重度**: HIGH (业务直接调 Resume 不经 Run 必踩)
**位置**: [agent_v2/graph_agent.go:175-184](agent_v2/graph_agent.go:175) (Resume 入口仅校验 cper)

**证据**: Run 路径 [line 128-136](agent_v2/graph_agent.go:128) 完整校验 `g.graph / g.inputWriter / g.outputReader`, Resume 路径**只校验 cper**. Resume line 234 `compose_v2.Run(ctx, g.graph, cm, runOpts...)` 若 `g.graph nil` → 远 stack nil deref panic.

**触发场景**: Resume 是公开 API 业务可独立调用, 无 invariant 强制 Run 先于 Resume. 业务用 builder `NewGraphAgent(graph)` 忘传 `WithGraphOutputReader` 然后直接 Resume → panic 不明.

**修复方向**: Resume 入口加同款 fail-fast 校验 `g.graph nil / g.outputReader nil`. `g.inputWriter` Resume 不需要 (cm.Restore 走 cp), 不必校验.

---

### H-07 MCP StdioClient.Close cmd.Wait Kill 失败时永久阻塞 + goroutine 泄漏

**严重度**: HIGH (长跑业务累积 goroutine + FD)
**位置**: [mcp/stdio.go:187-194](mcp/stdio.go:187)

**证据**: Close 超时触发 `cmd.Process.Kill()`, 然后 `<-waitCh` 等 `cmd.Wait()` 返回. 若 Kill 因权限 / 进程已 reparent 等少见原因失败:
1. Close() 永久阻塞
2. 派生 goroutine 永久泄漏, 持 cmd 引用 → 持 pipe FD → 文件句柄不释放
3. 长跑业务每次 Close 失败累积一个 goroutine + 几个 FD

`_ = cmd.Process.Kill()` 显式 ignore 错 — Kill 失败 silent.

**修复方向**:
- Kill 失败 emit warn + 不再 `<-waitCh`, 直接返 error
- 或加二级超时 (Kill 后等 closeTimeout/2 再 abandon goroutine)
- 或 `syscall.Kill(pid, SIGKILL)` 替代 (process group 维度 kill)

---

### H-08 `streamReaderToSchema` 桥接 goroutine 在业务 hook reader Recv 不响应 ctx 时永久阻塞

**严重度**: HIGH (业务 hook 实现 bug 时累积 goroutine + conn)
**位置**: [agent_v2/basic_agent.go:57-75](agent_v2/basic_agent.go:57)

**证据**: 业务 OnModelEndStream hook 返自定义 reader, 其 `Recv(ctx)` 实现不监听 ctx 或第三方 SDK 阻塞读 → 桥接 goroutine 卡在 `hookReader.Recv(ctx)`. caller `reader.Close()` 关 out 时, `send(msg)` 通过 `<-sr.done` 退出当前 send, 但下一轮 `Recv(ctx)` 仍继续阻塞 → 永久泄漏 + `defer hookReader.Close()` 永不执行.

streamOrGenerate 内 retry 路径每次 attempt 重 spawn 桥接 → 业务 hook reader 有 bug 时每次 retry 累积一个 goroutine + 一个 conn → 内存 + FD 双爆.

**修复方向**:
1. goroutine 内 `for {}` 循环顶端加 `select { case <-ctx.Done(): hookReader.Close(); return; default: }` 主动监听 ctx cancel
2. caller ctx 必须 inheriting from request ctx
3. 文档约束: OnModelEndStream hook 返的 wrapped reader 的 Recv 必须 ctx-aware

---

### H-09 RunnableAsTool 静默吞 InterruptError / ctx.Err — vs AgentAsTool 协议不对称

**严重度**: HIGH→MED (边角场景, 业务用 RunnableAsTool 包"普通 Go 函数为 tool" 一般不抛 Interrupt, 但协议不对称真)
**注**: 视角 2 报 HIGH, cross-verify 后我判断**降为 MED 列下面**. 见 [M-01].

---

## MED 级 finding

### M-01 RunnableAsTool 静默吞 InterruptError / ctx.Err

**位置**: [agent_v2/agent_tool.go:163-191](agent_v2/agent_tool.go:163)

**证据**: 业务 fn 返错被无条件转 `tool.Failure(tool.ErrCodeInternal, err.Error())`, 没 IsInterrupt + ctx.Err 检查. 对照 AgentAsTool.Call ([line 141-150](agent_v2/agent_tool.go:141)) 有完整守卫 — 协议不对称.

**修复方向**: 加 IsInterrupt + ctx.Err propagate 与 AgentAsTool 对称.

---

### M-02 ChainHooks.OnModelEndStream cleanup 闭包对 nil entries 调 Close 二次 panic

**位置**: [hook/hooks.go:249-279](hook/hooks.go:249)

**证据**: 业务 hook 返 `(nil, nil)` (笔误 / mock) → wrappedStack 推入 nil → 下一 hook `Recv(nil reader)` panic → cleanup 闭包循环 `wrappedStack[i].Close()` 对 nil 调用 → **二次 panic** (cleanup 闭包内无 defer recover) → 即使外层 recover 也 silent 失败.

**修复方向**:
1. cleanup 内 nil 守卫: `if wrappedStack[i] != nil { _ = wrappedStack[i].Close() }`
2. push 前 fail-fast: `if wrapped == nil { return nil, fmt.Errorf("hook returned nil reader") }`
3. cleanup 闭包整体包 `defer func() { _ = recover() }()`

---

### M-03 BasicAgent runGraph 自己 LoadNS 后未做 GraphID 守卫 — 与 graph_loop auto-load 行为分裂

**位置**: [agent_v2/basic_agent.go:312-322](agent_v2/basic_agent.go:312)

**证据**: runGraph LoadNS 后**仅检 Status + Frontier**, **不查 `savedCp.GraphID != g.ID()`**. 下游 `compose_v2.Run` 入口 graph_loop.go:308 GraphID 守卫返 `ErrGraphIDConflict`. 业务 graph schema 变化 (toolkit / preamble / postamble 函数指针变) 同 sid 复用时:
1. runGraph LoadNS 拿旧 cp, 仅看 Pending+Frontier 判定 `skipInitialWrites = true`
2. compose_v2.Run auto-load LoadNS GraphID 冲突 fail-fast
3. 错误返出但 InitialWrites 已跳过 — 业务 catch 错重试丢了 PreambleInput

LoadNS 被调 2 次 — 自定义 Checkpointer (Redis/Postgres/S3) RPC 双倍开销.

**修复方向**:
- 短期: runGraph LoadNS 同样加 `savedCp.GraphID != "" && savedCp.GraphID != g.ID()` 守卫
- 长期: 删 runGraph 重复 LoadNS, 用 ctx-level cache

---

### M-04 Resume 入口 sessionID 与 session.RunID 不一致时静默错位

**位置**: 9 编排 Resume 入口 (如 [agent_v2/basic_agent.go:200-203](agent_v2/basic_agent.go:200))

**证据**: caller 调 `Resume(ctx, "sess1", WithResumeSession(&RunSession{RunID: "sess2"}))`:
- cp 操作用 `sessionID = "sess1"`
- session.AddEvent 写到 `"sess2"` events
- 无检查 / 无 warn → 静默事件丢失 / 拿陈旧 cp / 调试困难

**修复方向**: Resume 入口加 `if rcfg.session != nil && rcfg.session.RunID != "" && rcfg.session.RunID != sessionID { slog.WarnContext(...) }`.

---

### M-05 AgentAsTool tcID 缺失时 fallback 用 JoinNS(parentNS, agent.Name()) — 与 SkillTool 协议不对称, 违反"不留兼容路径"规约

**位置**: [agent_v2/agent_tool.go:110-117](agent_v2/agent_tool.go:110)

**证据**: 对照 [skill_tool.go:720-724](agent_v2/skill_tool.go:720) 删了同款 fallback 改 fail-fast (`audit C HIGH-3 fix`), AgentAsTool 仍保留 else 分支. 业务自定义 toolsFn / 测试代码直接调时存在 silent 互覆风险.

**修复方向**: 删 else 分支, 改 fail-fast `return tool.Failure(tool.ErrCodeInternal, "AgentAsTool.Call invariant violated: GetCurrentToolCallID empty ...")`.

---

### M-06 同 PlanAgent 实例并发 (ParallelAgent 两 branch) 时 cleanupPendingTmpSteps 跨 branch 误删

**位置**: [agent_v2/plan_agent.go:691-706](agent_v2/plan_agent.go:691) (cleanup) + [agent_v2/plan_agent.go:351, 398](agent_v2/plan_agent.go:351) (defer)

**证据**: cleanup 用 `prefix := runID + ":"` 全删. 业务用 ParallelAgent 包同一 `*PlanAgent` 实例为多 branch, 共享 `parentSession.RunID` (ForkLane 不改 RunID). branch1 完成 (success/interrupt) defer cleanup → 同时 Delete branch2 stash → branch2 dispatcher Load miss 报错.

**修复方向**:
- stashKey 加 graphNS 维度: `"<runID>:<graphNS>:<desc>"`
- 或 godoc 明示"同 PlanAgent 实例并发 unsafe"

---

### M-07 hook.safeCallErr / compose_v2.safeOn* panic value 用 %v wrap 丢 error chain (10 处)

**位置**:
- [hook/hooks.go:60](hook/hooks.go:60) `safeCallErr`
- [compose_v2/agent_hooks.go:70, 82, 102, 114, 144, 155, 167](compose_v2/agent_hooks.go:70) (7 处 safeOn*)
- [compose_v2/hooks.go:151, 171](compose_v2/hooks.go:151) (safeOnGraphStart/safeOnNodeStart)

**证据**: 业务 hook panic 一个 `error` value 包含 typed error → `fmt.Errorf("hook %q panic: %v", hookName, r)` 用 %v 不判 `r.(error)` → caller `errors.As(err, &MyDBErr)` 失败 — 链断.

对照 [graph_loop.go:864-869](compose_v2/graph_loop.go:864) safeNodeRun 已用正确模式:
```go
if rerr, ok := r.(error); ok {
    err = fmt.Errorf("node panic recovered: %w", rerr)  // %w 保链
} else {
    err = fmt.Errorf("node panic recovered: %v", r)
}
```

**修复方向**: 10 处统一改用 `safeNodeRun` 模式.

---

### M-08 9 编排 ResumeActionAbort 静默吞 cper.DeleteSubtree error

**位置**: 9 编排 Resume `_, _ = cper.DeleteSubtree(...)` (同 H-03 位置)

**证据**: 业务 cper backend (Redis/Postgres/S3) 失败时业务以为 abort 成功 cp 清掉, 实际 cp persist → 下次同 sid Run 时 auto-load 拿到老 cp.

**修复方向**: 至少 `slog.WarnContext` 让业务订阅 handler 转告警.

---

### M-09 BasicAgent makeToolsFn 静默吞 LLM tool_call args JSON 解析失败

**位置**: [agent_v2/basic_agent.go:998](agent_v2/basic_agent.go:998)

**证据**: `_ = json.Unmarshal([]byte(tc.Function.Arguments), &args)` 错被吞 → args={} 传给业务 tool → tool 拿空 input 静默执行错误.

**修复方向**: 解析失败返 `tool.Failure(ErrCodeInvalidArguments, "json parse: "+err.Error())` 给 LLM 反馈让其重试.

---

### M-10 reflect_agent.go reflector 路径不调 safeOnError, 与 evalFn 路径不对称

**位置**: [compose_v2/reflect_agent.go:218-228](compose_v2/reflect_agent.go:218)

**证据**: reflector 路径错误**完全不调 safeOnError 也不打 OnError 信号**, evalFn 路径有完整 OnError + IsInterrupt + ctx.Err 守卫 — 协议不对称.

**修复方向**: reflector 路径加同款守卫.

---

### M-11 routing_agent / skill_tool 路径 `ctx.Err() != nil` 守卫漏 errors.Is wrap

**位置**:
- [agent_v2/routing_agent.go:462-484](agent_v2/routing_agent.go:462) (classifier 路径)
- [agent_v2/skill_tool.go:761-766](agent_v2/skill_tool.go:761) (fork mode 路径)

**证据**: 单判 `ctx.Err() != nil` 不防"sub-agent 内部已 cancel + outer ctx 未 cancel" 场景. 业务 wrap 后的 ctx error 漏判.

**修复方向**: `errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil`.

---

### M-12 RoutingAgent / SequentialAgent / HandoffAgent 缺 duplicate name 校验

**位置**:
- [agent_v2/routing_agent.go:193-200](agent_v2/routing_agent.go:193) `AddBranch`
- [agent_v2/sequential_agent.go:119-126](agent_v2/sequential_agent.go:119) `AddStep`
- [agent_v2/handoff_agent.go:134-144](agent_v2/handoff_agent.go:134) `AddAgent`

**证据**: 业务两次 `r.AddBranch("expert", agA).AddBranch("expert", agB)` — silent 覆盖 agA 让其永不被路由 / step / hop. 对照 `ParallelAgent.AddBranch` + `PlanAgent.AddSubAgent` 已 panic on dup — 三个编排漏防同协议.

**修复方向**: 三处入口加 dup name fail-fast panic.

---

### M-13 ParallelAgent OnAgentStart 仅 split 节点 1 次 fire, branch 不 fire

**位置**: [compose_v2/parallel_agent.go:163-186](compose_v2/parallel_agent.go:163) (splitFn) + [compose_v2/parallel_agent.go:193-221](compose_v2/parallel_agent.go:193) (workerFn 不 fire)

**证据**: 业务按 OnAgentStart 计 "agent 实际跑了多少 branch" 时统计偏低 + agentIn.Messages 为空. 与 SequentialAgent 每 step fire 不对称.

**修复方向**: workerFn 入口加 fire OnAgentStart, agentIn.Messages 填 [originalInput]; 或文档化此简化协议.

---

### M-14 agent_v2 wrapper 层完全不 fire `OnError` (与 compose_v2 层不对称)

**位置**: 9 编排 Run/Resume 错误返回路径

**证据**: 9 编排 wrapper 收到 sub-agent / compose_v2.Run 返 err 时直接 return, **不 fire OnError(CommError{...})**. 业务想统一统计"agent 层错误"得依赖 compose_v2 节点级 OnError + 自己累计 → 9 编排粒度不一致.

**修复方向**: 9 wrapper Run/Resume 加 defer fire OnError (只对非 InterruptError / 非 ctx.Err 的真业务错); 或文档化"OnError 仅 compose_v2 节点级 fire".

---

### M-15 agent_v2 缺 `NewChainOrchHooks` 等价 helper

**位置**: [agent_v2/hooks.go](agent_v2/hooks.go) (只有 OrchestrationHooks interface + NoOp + composeHooksAdapter, 无 chain helper)

**证据**: 业务用 `agent_v2.OrchestrationHooks` 简化版想挂 N 个 hook 时框架未提供 — 业务自管 chain 易漏 panic 隔离 / typed nil 过滤.

**修复方向**: 加 `NewChainOrchHooks(...) *ChainOrchHooks` 对应 compose_v2 同款 API, 内走 panic 隔离 + isFrameworkNil 过滤.

---

### M-16 PlanAgent ResumeActionReplan silent 走 Continue 路径 (与其他 6 编排显式 fail-fast 不对称)

**位置**: [agent_v2/plan_agent.go:380-382](agent_v2/plan_agent.go:380)

**证据**: 注释"PoC: ResumeActionReplan 由 caller-driven Planner 闭包消费", 实际走 Continue + user 答案是否被 Planner 正确消费**完全取决于业务 Planner 实现**. 对照 Sequential/Parallel/Loop/Routing/Reflect/Handoff 都 fail-fast 报 `not supported`, PlanAgent 反 silent.

**修复方向**: 业务 Action != Continue 显式区分 (sentinel key); 或文档化 "PlanAgent Replan = Continue + user 答案" 语义.

---

### M-17 SequentialAgent.AddStep 注释承诺"compose_v2 内部已校验 dup" 误导, 实际只校验 NS 字符

**位置**: [agent_v2/sequential_agent.go:113-126](agent_v2/sequential_agent.go:113) + [compose_v2/sequential_agent.go:94](compose_v2/sequential_agent.go:94)

**证据**: 注释 "compose_v2 内部 SequentialAgentBuilder.AddStep 已校验, 业务踩坑早暴露", 实际 compose_v2 内 ValidateNodeIDForNS 不校验 dup. 业务两次 `s.AddStep("transform", agA).AddStep("transform", agB)` — silent 丢失 agA.

**修复方向**: 加 dup check + 改注释删误导描述 (与 M-12 SequentialAgent 部分重叠).

---

### M-18 agent_v2/hooks.go godoc 引用不存在的类型 `agentSinkAdapter`

**位置**: [agent_v2/hooks.go:12](agent_v2/hooks.go:12)

**证据**: godoc 写 "agent_v2 提供 caller-friendly adapter (agentSinkAdapter)", 实际类型是 `composeHooksAdapter`. grep `agentSinkAdapter` 全仓库无定义.

**修复方向**: 改 godoc "agentSinkAdapter" → "composeHooksAdapter".

---

### M-19 `extractOrchHooksOrCtx` 不检 typed nil — defense in depth 缺

**位置**: [agent_v2/options.go:82-87](agent_v2/options.go:82)

**证据**: 当前业务无路径触发 (`runConfig` unexported, 只能走 With* options 已防 typed nil), 但**未来加新 With* option 忘 isFrameworkNil 时无第二层防御**.

**修复方向**: 改 `if optsHooks != nil` → `if !isFrameworkNil(optsHooks)` 与其他防御协议对称.

---

### M-20 GraphAgent.runGraph 的 `hooks hook.Hooks` 参数 dead 未用

**位置**: [agent_v2/graph_agent.go:251](agent_v2/graph_agent.go:251)

**证据**: 参数声明但函数体内未引用 ([line 271](agent_v2/graph_agent.go:271) 用 `hooks := extractOrchHooksOrCtx(...)` shadow). 重构残留 / 业务读签名以为 hooks 会传给 graph 实测无效.

**修复方向**: 删 `hooks hook.Hooks` 参数, Run/Resume defer 处 fire OnAgentEnd 用 outer scope hooks 即可.

---

### M-21 Hub.Start / Hub.Close goroutine 内业务 Client 实现 panic 不 recover

**位置**: [mcp/hub.go:66-83](mcp/hub.go:66) (Start), [mcp/hub.go:127-138](mcp/hub.go:127) (Close)

**证据**: 业务自定义 Client `Initialize` / `ListTools` / `Close` panic → goroutine 内无 recover → 整进程 crash.

**修复方向**: goroutine 内加 defer recover 转 result.err.

---

### M-22 HandoffAgent.stateBySession + AutoCompactConfig.breakers 永不清理 (内存泄漏)

**位置**:
- [agent_v2/handoff_agent.go:48](agent_v2/handoff_agent.go:48) `stateBySession sync.Map`
- [agent_v2/autocompact.go:81](agent_v2/autocompact.go:81) `breakers *sync.Map`

**证据**: 每个 distinct RunID 在两个 map 内各加一 entry, **无任何 Delete 调用** (`grep -n "stateBySession.Delete\|breakers.Delete"` 返空). 长跑进程 + 高 session 流转率累积 MB 级.

对照 PlanAgent.pendingTmpSteps 有 defer cleanup ([line 351, 398](agent_v2/plan_agent.go:351)) 证明设计上知道要 cleanup 但漏这两个.

**修复方向**: Run/Resume defer 加 Delete; 或加 TTL sweeper / Cleanup helper.

---

### M-23 OpenAI consumeStream goroutine 无 panic recover

**位置**: [model/openai/client.go:286-377](model/openai/client.go:286)

**证据**: SSE 流处理某 chunk 字段类型变更 / send 闭包路径 panic → goroutine 无 defer recover → 进程 crash.

**修复方向**: 顶部加 `defer func() { if r := recover(); r != nil { schema.SendError(sr, ...) } }()` — caller `reader.Recv` 拿错走业务错路径不进程死.

---

### M-24 schema.StreamReader send 闭包不监听 ctx, 消费者 abandon 时 producer 永久阻塞

**位置**: [schema/stream.go:42-47](schema/stream.go:42)

**证据**: caller panic / early return 没 Close 也没消费完 → producer `sr.ch <- item` 阻塞 → `<-sr.done` 永不 fire → 即使 ctx cancel 也无效.

**修复方向**:
- 让 send 闭包接 ctx (API 改动)
- 文档强制约束 caller 必须 defer Close

---

### M-25 time.After 在 Go 1.22 未 GC 的 timer 泄漏

**位置**: [mcp/stdio.go:191](mcp/stdio.go:191), `example/{channel,plan}_chat_web/mock.go:51`

**证据**: go.mod L3 `go 1.22`, pre-1.23 `time.After` 创建的 Timer 在 fire 之前不会 GC. 对照 [compose_v2/retry.go:85-93](compose_v2/retry.go:85) 已用正确 NewTimer+Stop 模式.

**修复方向**:
1. 升级 go.mod 到 1.23
2. 改 `timer := time.NewTimer(d); defer timer.Stop()` 模式

---

### M-26 StreamableHTTPClient 非 2xx 路径不读 error body

**位置**: [mcp/http.go:190-193](mcp/http.go:190)

**证据**: server 返 500/4xx 错且 body 含 JSON-RPC error message 被 `io.Copy(io.Discard, ...)` 丢. 业务调试体验差.

**修复方向**: `body, _ := io.ReadAll(resp.Body)` 然后 `return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, body)`.

---

## LOW (列举不展开)

- RunSession.events slice 无 truncation API; 长跑 sess 内存增长
- nodeEventSink.Send 调用的 hooks 用 sink 创建时的 ctx; caller 后续 derive 的 ctx-value 看不到
- BasicAgent.Run 嵌套 sub-agent ctx 父 child 共用同一 ctx (无 child-only timeout)
- example/orchestration/plan_chat_web runSweeper goroutine 无停止信号 (示例代码, 影响小)
- schema.StreamReader recvOnce/sendOnce 对 NoOp chain 多次 Close 安全, 业务自定义 reader 不保证

---

## 已查覆盖范围 (5 视角合并)

✅ 并发安全 (sync.Mutex/RWMutex/Once/Map/atomic/chan/WaitGroup/goroutine 入口)
✅ RunSession 三套 mu (eventsMu/valuesMu/laneEvents.mu)
✅ BasicAgent toolsFn ISSUE-81 fanout (并发协议 + 跨 tc ctx)
✅ ParallelAgent fan-out branch lane events 共享态 (R33 fix 路径)
✅ AutoCompactConfig per-session breaker (ISSUE-72 fix)
✅ ChannelManager ApplyAll/Restore atomic rollback
✅ typed nil interface 协议跨 5 包 30+ 入口 (audit²-²⁷ 71 真 HIGH 修后)
✅ ChainHooks/ChainOrchHooks panic isolation
✅ InterruptError 控制流 (MergeInterrupts/AllQueries/MakeInterruptID/GetResumeAnswer/Strip)
✅ ctx.Err propagate (9 编排 + 4 层压缩)
✅ panic recover 链 (hook.safeCall* / compose_v2.safeOn* / safeNodeRun / safeCondRun / ChainHooks/ChainOrchHooks)
✅ error wrap chain (%w 在大多数路径正确, 7+1+2=10 处 panic %v 链断)
✅ sentinel error (ErrGraphIDConflict / ErrNoSubgraphCheckpoint / ErrThreadNotFound / ErrInvalidNodeIDForNS)
✅ 9 编排 .Run/.Resume NS push + 父 graphNS forward
✅ AgentAsTool / SkillTool fork tcID hash NS 物理分离
✅ PlanAgent 4 个 NS push 点 (runPlanGraph / Resume / 3 worker)
✅ GraphID 防串台 (cp.GraphID vs g.ID() 三处检查)
✅ InterruptID 5 维稳定性 (node 路径 auto-fill；cond fn 已禁 interrupt——2026-07-16 `70e0fcb`)
✅ postamble/preamble idempotent on Resume
✅ goroutine 启动 → 退出 → 资源清理链 (BasicAgent toolsFn / ParallelAgent / OpenAI consumeStream / streamReaderToSchema / MCP StdioClient)
✅ stream/SSE/file/conn close 协议 (StreamReader 协议 + body Close defer)
✅ 9 编排 Run/Resume 签名对称 (统一签名完全一致)
✅ 9 编排 hooks 字段 + WithXxxHooks option (PlanAgent + GraphAgent 仅 OnAgentEnd fire — H-05)
✅ business hook 改写 input.Messages → runtime 真消费协议 (modelNode line 273/285/317)

---

## 本次未审角度 (建议下轮 audit 角度)

- **跨进程 cp 持久化序列化语义**: cp wire 反序列化后 tagged value 类型擦除 → cast 失败链路
- **业务 Memory.Write 双重持久化 + WithSubAgentMemoryScope 协议跨 9 编排实施完整性**（PR 3.3 后改名 `store.Put` / `WithSubAgentStoreScope`）
- **MicroCompact / AutoCompact 算法正确性**: 9 段提取 / NO_TOOLS_PREAMBLE / 文件重注入 / 跨层 marker
- **MaxIterationsError 与 cp 状态**: visit count 触顶时 cp 行为
- **sub-agent 抛 panic (非业务 typed err) 的 cp 状态**: safeNodeRun recover 后 Save / Delete?
- **type registry / MarshalTagged 跨 cp 边界保真度**: 业务自定义 channel value 类型 RegisterValueType 漏注时兜底
- **HITL Resume 时 OnAgentInit 是否重 fire**: godoc 说"整 Run 一次", Resume 时实际行为
- **业务 hook 改写消息时漏剥 SkillBody/SkillListing/MemoryInjected annotation 是否真破坏 4 层压缩**
- **PlanAgent + Replan + sub-agent 嵌入 BasicAgent + ask_user 完整闭环**
- **WithCtxOrchHooks 跨 3+ 层 sub-agent 嵌套 ctx 透传**
- **`select` 内多 case 就绪时优先级偏好** (除 StreamReader 已审外)
- **business prompt injection / SkillBox.MatchSkills security**
- **跨 Checkpointer 实现一致性**: 业务 Redis/Postgres 实现 SaveNS 失败原子性

---

## 修复优先级建议

按"业务可见生产影响 + 修复成本"双维度:

| 优先 | finding | 修复成本 | 推荐时机 |
|---|---|---|---|
| **P0** (本周修) | H-01 (PlanAgent execInputBuilder ctx) | 改 3 行 + 1 sanity-revert 测试 | 立即, 注释与代码 100% 矛盾 |
| **P0** | H-02 (toolsFn fanout 无 panic recover) | 加 defer recover ~10 行 + race 测试 | 立即, 业务 tool panic 整进程死 |
| **P0** | H-03 (9 编排 Abort 误删父 cp) | 9 处加 GetGraphNS forward, ~10 行 | 立即, nested 业务必踩 |
| **P1** (下周修) | H-04 (TmpSubAgent + HITL Resume 死循环) | 重新设计 cleanup 时机, ~30 行 + 端到端测试 | 下周, 需测试覆盖矩阵补 |
| **P1** | H-05 (PlanAgent + GraphAgent hook 协议缺口) | compose_v2.PlanBuilder 加 WithAgentHooks + plannerFn fire OnAgentInit/Start, ~80 行 | 下周, 与 D2 observability 实施同步 |
| **P1** | H-06 (GraphAgent.Resume nil 校验缺) | 加 3 行 fail-fast | 下周, 业务直接调 Resume 必踩 |
| **P2** (下月修) | H-07 (StdioClient.Close goroutine 泄漏) | Kill 失败路径重设计, ~20 行 | 下月, 长跑业务才累积 |
| **P2** | H-08 (streamReaderToSchema 桥接永久阻塞) | goroutine 加 ctx.Done 监听 + 文档化 hook 协议 | 下月, 业务 hook 实现 bug 时才触发 |
| **M-01..M-26** | 见上 | 多数 ~5-10 行 | 按 P3 节奏, 优先 M-03/M-12/M-22 (业务可触发) |

---

## 元审计反思

按 CLAUDE.md "audit agent 命中率 33-43%" 经验值, 本轮 5 视角共报 15 HIGH (含 1 跨视角共识 = 同 bug), cross-verify 后:
- 真 HIGH: 9 个 (命中率 60% — 高于 33-43% 经验值)
- 降为 MED: 3 个 (H-09 RunnableAsTool 是真但严重度边角 / H-02 BasicAgent runGraph LoadNS 是真但 graph_loop fail-fast 兜底降 MED / H-05 BasicAgent makeToolsFn args 静默吞是真但 LLM 罕见 emit 坏 JSON 降 MED)
- 删除 0 个 (所有 HIGH 都是真 bug, 严重度调整非误判)

**为什么命中率高**: 5 视角 prompt 严格"单视角不分散", agent 报告每个 finding 都给 file:line + 触发场景 + 影响 + 修复方向 + 证据 — 我 grep 5 分钟就能 verify 真伪. 视角 2 HIGH-2 + 视角 3 HIGH-1 跨视角独立报同一 bug, 共识极强.

**仍未审的角度**: 见上"本次未审角度". 推荐下轮 audit 优先 **MicroCompact/AutoCompact 算法正确性** + **type registry 跨 cp 边界** + **HITL Resume 时 OnAgentInit 重 fire 行为**.

**审计自身风险**: 本轮纯 read-only 0 修改代码, 无新 bug 引入风险. 但 H-04 (TmpSubAgent + HITL 死循环) 修复时需要重新设计 cleanup 时机, 修复后必须 sanity-revert + 端到端测试覆盖 — 避免重蹈 R29 视角 3 HIGH-1 "我自己引入 regression" 的覆辙.
