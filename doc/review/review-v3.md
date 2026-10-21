# Code Review v3 — 2026-04-10

> ⚠️ **历史 Code Review 记录**：v1 时代审查结果。Stage 7 后 v1 (`agent/` + `compose/`) 已删，文中 file:line 引用是 v1 路径。审查发现的设计问题 / bug 类别在 v2 中已等价覆盖。


## 概览

第三轮审查，聚焦 bug 和鲁棒性。逐文件阅读全部源码后，
共发现 14 个问题，按严重程度分为高/中/低三档。

---

## 🔴 高优先级

### H1 — `agent/react.go` · OnAgentStart/OnAgentEnd 不对称 ✅ 已修复

**文件**: `agent/react.go:69-198`

**问题**:
`reactLoop` 在每轮循环开头调用 `OnAgentStart`（L69），但 `OnAgentEnd` 只在两处被调用：
1. 无工具调用的正常结束路径（L109）
2. 工具执行后的循环末尾（L191）

以下 **5 条错误路径** 在 `OnAgentStart` 已成功调用后直接 return，导致 `OnAgentEnd` 永远不会被调用：

| 行号 | 场景 |
|------|------|
| L78-79 | `OnModelStart` hook 返回错误 |
| L84-86 | `chatModel.Generate()` 失败 |
| L95-96 | `OnModelEnd` hook 返回错误 |
| L160-162 | `Toolkit.Call()` 返回 Go error（非 Result 错误） |
| L166-167 | `IsFrameworkError(result)` 为 true |

**影响**: 任何依赖 Start/End 配对的 hook 实现（如计时器、资源获取/释放、指标计数器）都会出现状态泄漏。这是 hook 契约的违反。

**修法**: 将循环体提取为 `executeRound` 子函数，`OnAgentStart` 成功后在子函数内 `defer OnAgentEnd`，保证所有路径（含 5 条错误路径）均触发 `OnAgentEnd`。

---

### H2 — `model/openai/client.go:108` · consumeStream goroutine 无法感知消费者关闭 ✅ 已修复

**文件**: `model/openai/client.go:107-108, 239-307`

**问题**:
`Stream()` 启动 `go c.consumeStream(httpResp.Body, sr, send)`，但 `consumeStream` 内部只在以下情况退出：
- 收到 `[DONE]` SSE 事件（L273-274）
- `body.Read()` 返回 IO 错误（L303-304）

当消费者调用 `sr.Close()`（关闭 `done` channel）后，`send` 函数内的 `select` 会选中 `<-sr.done` 分支不再阻塞，但 `consumeStream` 会继续 `body.Read()` → 解析 → 调用 `send`（立即丢弃）→ 继续循环。HTTP body 读取是阻塞 IO，如果服务器慢发数据，goroutine 会长期存活。

**影响**: 消费者提前关闭 stream 或 ctx 取消时，goroutine + HTTP 连接泄漏。

**修法**:
1. `schema/stream.go` 新增 `Done() <-chan struct{}` 方法暴露 done channel
2. `consumeStream` 增加 `ctx context.Context` 参数，每轮迭代开头用非阻塞 select 检查 `ctx.Done()` 和 `sr.Done()`，任意关闭则立即退出

---

### H3 — `hook/hooks.go:130-141` · ChainHooks.OnModelEndStream 中间态 stream 泄漏 ✅ 已修复

**文件**: `hook/hooks.go:130-141`

**问题**:
chain 有 3 个 hook [A, B, C] 时：A、B 各自返回 wrappedA、wrappedB。C 失败时只关闭当前 `sr`（即 wrappedB），`wrappedA` 持有的资源不会被关闭。

**影响**: 多 hook 场景下，中间 wrapped stream 的资源可能泄漏。

**修法**: 维护 `wrappedStack`，失败时逆序关闭所有已 wrapped 的 stream（含原始 sr）。

---

## 🟠 中优先级

### M1 — `agent/react.go:198` · MaxRounds 耗尽时 LastOutput 丢失 ✅ 已修复

**文件**: `agent/react.go:196-198`

**问题**:
MaxRounds 耗尽时直接返回 `nil, &MaxRoundsError{}`，调用者丢失最后一轮的 LLM 响应。

**影响**: 调用者无法获取 MaxRounds 前最后一轮的部分结果。

**修法**: `reactLoop` 维护 `lastLLMResp` 变量，`executeRound` 额外返回本轮 LLM 响应；MaxRounds 耗尽时返回 `lastLLMResp, &MaxRoundsError{}`，与 `LoopAgent` 行为一致。

---

### M2 — `compose/handoff.go:155` · HandoffAgent maxHops 耗尽时 LastOutput 可能为 nil ✅ 已修复

**文件**: `compose/handoff.go:152-155`

**问题**:
两条返回路径均直接返回 `final.LastOutput`，若为 nil 则调用者收到 `(nil, ...)` 导致下游解引用 panic。

**修法**: 统一 nil guard，回退 `schema.AssistantMessage("")`；同时在 `Run` 入口增加 `input == nil` guard。

---

### M3 — `compose/reflect.go:183-184` · 将 feedback 和 nextInput 混用导致状态不一致 ✅ 已修复

**文件**: `compose/reflect.go:179-184`

**问题**:
`state.Messages` 末尾追加 `feedback`，但 `state.LastOutput` 被设置为 `nextInput`，破坏了 `Messages[-1] == LastOutput` 的隐含不变量。

**修法**: 将 `feedback` 和 `nextInput` 都追加到 `state.Messages`（`append(state.Messages, feedback, nextInput)`），再设 `state.LastOutput = nextInput`，恢复一致性。

---

### M4 — `compose/routing.go:196` · RoutingAgent 正常返回时未 nil guard LastOutput ✅ 已修复

**文件**: `compose/routing.go:196`

**问题**:
`return state.LastOutput, nil` 未 nil guard，调用者收到 `(nil, nil)` 时下游解引用 panic。

**修法**: 增加 nil guard，回退 `schema.AssistantMessage("")`；同时在 `Run` 入口增加 `input == nil` guard。

---

### M5 — `compose/fanout.go:83` + `compose/helpers.go:213` · results slice 并发写入安全性依赖于索引隔离

**文件**: `compose/fanout.go:83-122`, `compose/helpers.go:213-241`

**状态**: ⚪ 评估为设计合理

Go 内存模型保证不同索引的 slice 元素可被不同 goroutine 安全并发写入，`wg.Wait()` 提供 happens-before 保证。Race detector 下无问题。降为可读性建议，不修改。

---

### M6 — `compose/checkpoint.go:105` · marshalGraphState 读 Messages/LastOutput 未加锁 ✅ 已修复

**文件**: `compose/checkpoint.go:105, 221-248`

**问题**:
`marshalGraphState` 对 `state.data` 加 RLock，但 `state.LastOutput` 和 `state.Messages` 在锁外读取，与 `cloneStateForFanOut` 的做法不一致。

**修法**: 在 `state.mu.RLock()` 内一次性读取 `LastOutput`、`Messages`（深拷贝）、`data`，与 `cloneStateForFanOut` 保持一致的防御性写法。

---

## 🟡 低优先级

### L1 — `agent/react.go:59` · RunPath 在循环外一次性计算，后续轮次 info 中的 RunPath 可能过时

**文件**: `agent/react.go:50-59`

**状态**: ⚪ 当前不影响正确性，暂不修复

`info.RunPath` 在进入循环前计算一次。当前 `reactLoop` 不修改父 ctx 的 RunPath，暂无影响。未来若有 hook 需精确 RunPath 再处理。

---

### L2 — `compose/plan.go:110` · planNode 闭包捕获 `input.Content`，但未 nil guard input ✅ 已修复（含入 L6）

**文件**: `compose/plan.go:110, 125`

**修法**: 见 L6。

---

### L3 — `tool/tool.go:133-139` · Toolkit.Call 工具未找到时返回 Result 而非 error

**文件**: `tool/tool.go:133-139`

**状态**: ⚪ 行为正确，API 语义问题暂不修改

`reactLoop` 中先检查 `err != nil` 再检查 `IsFrameworkError(result)`，两条路径都会中止执行，实际行为正确。API 语义改动会影响 `tool.Toolkit` 接口使用方，属于接口设计问题，放到后续版本讨论。

---

### L4 — `model/openai/client.go:331` · convertMessages 只取第一个 Toolresult ✅ 已修复

**文件**: `model/openai/client.go:318-352`

**问题**: `om.ToolCallID == ""` 条件导致一条 `schema.Message` 中第二个及以后的 `Toolresult` ContentBlock 被静默丢弃。框架自身的 `ToolResultMessage` 每次只生成一个 block，但外部代码可能构造多 block 的消息，额外结果无声消失。

**修法**: 遇到第一个 Toolresult 填入主消息；后续 Toolresult 各自拆为独立的 `openaiMessage` 追加到输出切片。符合 OpenAI API "每条 tool 消息只携带一个 tool_call_id" 的协议约束。

---

### L5 — `compose/reflect.go:244-258` · containsApprovalKeyword 按字节索引处理 Unicode

**文件**: `compose/reflect.go:244-258`

**状态**: ⚪ UTF-8 设计保证 continuation bytes 不与 ASCII 重叠，实际不会误判，不修复

---

### L6 — 所有 compose Agent 缺少 input nil guard ✅ 已修复

**文件**: `compose/loop.go`, `compose/sequential.go`, `compose/handoff.go`, `compose/reflect.go`, `compose/routing.go`, `compose/plan.go`

**问题**: 这些 Agent 的 `Run` 方法都没有对 `input *schema.Message` 做 nil 检查，`PlanAgent` 闭包直接引用 `input.Content` 会 panic。

**修法**: 所有 6 个 compose Agent 的 `Run` 入口统一增加：
```go
if input == nil {
    input = schema.UserMessage("")
}
```

---

## 修复统计

---

## 事后补充 — 修复过程中发现的遗漏问题

以下 4 个问题在三轮审查中均未收录，在修复 M2/M4 时发现模式不一致后补查得出：

### X1 — `compose/agent_tool.go:53` · out == nil 时 panic ✅ 已修复

**问题**: `agentTool.Call` 在 `agent.Run` 成功时直接访问 `out.Content`，若某个 Agent 实现返回 `(nil, nil)` 则 panic。`ReflectAgent`（X2）是最可能的触发源。

**修法**: 增加 `if out == nil { return tool.Success(""), nil }`。

---

### X2 — `compose/reflect.go:228` · ReflectAgent.Run 返回未 nil guard 的 LastOutput ✅ 已修复

**问题**: `ReflectAgent.Run` 最后一行 `return final.LastOutput, nil` 无 nil guard，与已修复的 HandoffAgent（M2）和 RoutingAgent（M4）不一致。

**修法**: 增加 `if final.LastOutput == nil { return schema.AssistantMessage(""), nil }`；同时修复 `reflActorOutKey` 的类型断言，增加 `msg != nil` 判断。

---

### X3 — `compose/graphagent.go` + `compose/parallel.go` · L6 修复不完整 ✅ 已修复

**问题**: L6 修复了 6 个 compose Agent 的 `input == nil` guard，但遗漏了 `GraphAgent` 和 `ParallelAgent`。

**修法**: 两个 Agent 的 `Run` 入口统一增加 `if input == nil { input = schema.UserMessage("") }`。

---

- **已修复**: 14/14（含补充 X1-X3）
- **评估为设计合理 / 暂不修复**: 4（M5, L1, L3, L5）

## 修改文件清单

| 文件 | 修复编号 |
|------|----------|
| `agent/react.go` | H1, M1 |
| `schema/stream.go` | H2（新增 `Done()` 方法） |
| `model/openai/client.go` | H2（`consumeStream` 感知关闭），L4（多 Toolresult 拆分） |
| `hook/hooks.go` | H3 |
| `compose/handoff.go` | M2, L6 |
| `compose/routing.go` | M4, L6 |
| `compose/reflect.go` | M3, L6, X2 |
| `compose/checkpoint.go` | M6 |
| `compose/loop.go` | L6 |
| `compose/sequential.go` | L6 |
| `compose/plan.go` | L2/L6 |
| `compose/agent_tool.go` | X1 |
| `compose/graphagent.go` | X3 |
| `compose/parallel.go` | X3 |
