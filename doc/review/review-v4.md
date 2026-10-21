# Code Review #4（2026-04-12）

> ⚠️ **历史 Code Review 记录**：v1 时代审查结果。Stage 7 后 v1 (`agent/` + `compose/`) 已删，文中 file:line 引用是 v1 路径。审查发现的设计问题 / bug 类别在 v2 中已等价覆盖。


> 范围：全量代码审查，重点关注 ISSUE-10 重写后的 typed state 集成质量

---

## CR-01 · reflect.go 闭包变量绕过 typed state（中）

**文件**：`compose/reflect.go:92`

`lastActorOut *schema.Message` 作为闭包变量在 `actorNode`、`evalNode`、`reflectNode` 之间共享，未纳入 `reflectState`。`reflectState.ActorContent` 只存了 `string` 内容，`lastActorOut` 是完整 `*Message`。

**影响**：若图执行被 `InterruptError` 中断后通过 `ResumeGraph` 恢复，`lastActorOut` 为 nil，evalNode（line 121）和 reflectNode（line 153）会跳过逻辑，Run 最后（line 221）判断也受影响。

**修复方案**：
- 方案 A：将 `lastActorOut` 完整序列化到 `reflectState`（需处理 `*schema.Message` JSON）
- 方案 B：ReflectAgent 不支持 checkpoint resume（当前实际也不支持，每次 `Run` 编译新图），加注释说明

**建议**：方案 B（加注释），因为 ReflectAgent 每次 Run 创建新图，不缓存，实际不会走 ResumeGraph 路径。

---

## CR-02 · ProcessState 错误忽略缺注释（低）

**文件**：`compose/loop.go:72,90,103,119`、`compose/handoff.go:80,109,129,137`、`compose/reflect.go:103,117,137,148,197`、`compose/plan.go:190,202,242,259`

~20 处 `_ = ProcessState[...](...)`，CLAUDE.md 约定 "不用 `_` 忽略 error，除非有注释说明原因"。

这些忽略是合理的（graph 已配置 StateFactory，ProcessState 必然成功），但缺乏注释。

**修复**：在首次出现处加注释 `// ProcessState error 忽略：graph 已配置 StateFactory，类型匹配保证成功`，后续同文件内不重复。

---

## CR-03 · graphagent.go 缺少 WithRunStep（低）

**文件**：`compose/graphagent.go:37-58`

`GraphAgent.Run()` 没有调用 `agent.WithRunStep(ctx, ...)`，导致 RunPath 中不包含 graph agent 名称。其他所有编排模式（Loop、Handoff、Reflect、Plan、Sequential、Parallel、Routing）都正确调用了。

**修复**：在 `Run()` 开头加 `ctx = agent.WithRunStep(ctx, agent.RunStep{AgentName: g.name})`。

---

## CR-04 · routing.go runWithRouter fan-out 未隔离状态（低）

**文件**：`compose/routing.go:369-382`

`runWithRouter` 的 fan-out goroutines 共享同一个 `ctx`，没有使用 `cloneStateForFanOut`。当前不会出问题（`runAgentNodeMsg` 不写 GraphState），但与 `makeFanOutFunc` 的隔离模式不一致。

**影响**：如果未来 agent 内部通过 ctx 读写 typed state（ProcessState），多个并发分支会竞争同一个锁。当前实际安全，因为 `runWithRouter` 构建的 graph 没有 StateFactory。

**建议**：暂不修改，加 TODO 注释说明。

---

## CR-05 · parallel.go WithAggregator 捕获 session 时机（低）

**文件**：`compose/parallel.go:58`

`WithAggregator(session, fn)` 在选项应用时捕获 `session`，但 `Run()` 中若传入 `session == nil` 会创建新的 session。Aggregator 持有的 session 与实际执行的不同。

**影响**：边缘场景，正常使用时调用方会传入正确的 session。

**建议**：暂不修改，记录为已知限制。

---

## 总结

| ID | 严重度 | 文件 | 状态 |
|----|--------|------|------|
| CR-01 | 中 | reflect.go | 待修复（加注释） |
| CR-02 | 低 | 多文件 | 待修复（加注释） |
| CR-03 | 低 | graphagent.go | 待修复（1 行） |
| CR-04 | 低 | routing.go | 记录 TODO |
| CR-05 | 低 | parallel.go | 已知限制 |

无高优先级阻塞问题，ISSUE-10 重写质量良好。
