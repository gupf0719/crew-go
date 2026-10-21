# ISSUE-43 · AgentName 归属与 HistoryFor 改写的边界收口

**优先级**：高（含多个中低）
**状态**：✅ 已实现（2026-04-17）
**来源**：ISSUE-40/41/42 三连改动后的系统审查（2026-04-17）

> **历史档案说明**：本文描述 v1 `agent/` 时代的 Memory 接口（`Memory.Read` / `Memory.Write` / `WithMemory`）。该接口已在 PR 3.3（commit `e6505f9`）整体替代为 `store.Store`（`store.Search` / `store.Put` / `WithStore` + `WithStoreMemory`），且 v1 `agent/react.go` 已不存在。下文保留原历史叙述不动，仅供溯源参考。

---

## 一句话结论

ISSUE-42 的跨 agent 改写生效条件是 "AgentName 能唯一归属产出者"。审查发现三个会打破这条不变式的路径（默认 Name 碰撞 / 空 name 退化 / omitempty 丢失）、一个并发契约漏洞（重复 join 静默）、两个合约债（Memory.Read 无改写 / Sequential Messages[0] 无防御）。本 issue 一次闭环。

---

## 问题清单

### H1 · 默认 Name 碰撞使 HistoryFor 跨 agent 改写失效

**现象**：[agent/options.go:41](../../agent/options.go#L41) `defaultConfig` 设 `Name: "basic_agent"`。多个 BasicAgent 实例默认同名，`HistoryFor("basic_agent")` 把 A 的 assistant(tool_call) 误判为自产消息不改写，B 调 LLM 时仍触发 OpenAI 422。

**根因**：ISSUE-42 假设 "AgentName 唯一"，但 pre-42 的默认值是固定字符串（每个 agent 独立 session 时无所谓）。ISSUE-41 共享 session 后这个默认变成陷阱。

**修复方向**：默认 Name 改为 `basic_agent_<uuid 前 8 位>`，每次 `defaultConfig` 调用生成不同值。显式 `WithName` 覆盖。

---

### H2 · `HistoryFor("")` 退化为 History() 绕过改写

**现象**：[agent/rewrite.go:80](../../agent/rewrite.go#L80) `if ownerAgentName == "" { return s.History() }`。任何传空 name 的路径（显式 `WithName("")` 或框架 bug）会完全绕过 ISSUE-42 的保护。

**根因**：空串当作 "legacy 兼容" 退化通道，但没有合法调用方应当传空；这个退化是设计缺陷。

**修复方向**：空 owner 改为"最保守视角"——视所有事件为 cross-agent，全部走改写。与 History() 语义解耦。

---

### M1 · Checkpoint omitempty + Resume 丢失 AgentName

**现象**：[agent/event.go:55](../../agent/event.go#L55) `AgentName string json:"agent_name,omitempty"`。若某事件 AgentName 为空串，JSON 序列化时字段消失，反序列化后回到空串。HistoryFor 当前对空 name 保守不改写，导致 resume 后跨 agent 的 tool_call 泄漏。

**根因**：omitempty + HistoryFor 保守策略的组合副作用。

**修复方向**：去掉 omitempty（空 name 是数据异常不是节省字节的场景）；HistoryFor 对空 name 改为"视为 cross-agent 改写"（与 H2 同策略）。

---

### M2 · JoinLanes 重复调用静默吞没

**现象**：[agent/session.go:212-237](../../agent/session.go#L212-L237) 清空 `child.lane.events = nil` 标记 join 完成，但没有显式 `joined` 标志。对同一 child 重复 `JoinLanes` 第二次是 no-op 且不报错。框架内部路径目前不会误用，用户自定义 compose 节点一旦重复 join，静默失败极难排查。

**根因**：ISSUE-41 设计时采用"清空"间接标记，缺少幂等显式保护。

**修复方向**：`laneEvents` 加 `joined bool` 字段，`JoinLanes` 首行检查；重复 join 返回错误或 panic（框架内路径不会踩到，误用时尽早暴露）。

---

### L1 · Memory.Read 注入路径无 agent-aware 改写（v1 历史 — PR 3.3 后 Memory 已替代为 store.Search）

**现象**：[agent/react.go:58-63](../../agent/react.go#L58-L63) `Memory.Read` 返回 `[]*schema.Message` 直接 `insertAfterSystem`。框架自带 `Memory.Write` 只写 `[input, finalResp]`（finalResp 保证无 tool_call），但用户自定义 Memory 实现若把 `session.Events()` 完整存回并返回，读到的 tool_call 消息会绕过 ISSUE-42 保护，炸 API。

**根因**：Memory 接口返回的是纯消息列表，不带 AgentName 归属信息；ISSUE-42 的改写机制只覆盖 session.Events 一条路径。

**修复方向**：Memory 返回的消息对当前 agent 永远是"他者上下文"——reactLoop 注入前对每条执行 `rewriteCrossAgentMessage("memory", msg)` 降级。不修改 Memory 接口。

---

### L2 · Sequential/Handoff 依赖 `state.Messages[0]` 无运行时防御

**现象**：[compose/sequential.go](../../compose/sequential.go) 和 [compose/handoff.go](../../compose/handoff.go) 都从 `state.Messages` 取 user input 作为 step input。自定义 Graph 节点若清空/重排/截断 Messages，后续 step 拿到的 input 会漂移。当前仅有 `len==0` fallback，覆盖不到"被篡改"。

**根因**：ISSUE-41 规约问题——对"Messages[0] 始终是初始 user input"这条假设无运行时验证。

**修复方向**：Sequential/Handoff 在 typed state 里固化一份 `OriginalInput *schema.Message`，step/hop 读它而非 `Messages[0]`；原逻辑作为 fallback 保留。

---

## 落地改动

| 文件 | 改动 |
|---|---|
| `agent/options.go` | `defaultConfig().Name` 每次生成 `basic_agent_<uuid>` |
| `agent/rewrite.go` | `HistoryFor("")` 改为全改写；空 AgentName 事件也改写 |
| `agent/event.go` | `AgentName` 去掉 omitempty |
| `agent/session.go` | `laneEvents.joined bool` + `JoinLanes` 幂等检查 |
| `agent/react.go` | Memory.Read 结果走 `rewriteCrossAgentMessage("memory", ...)` |
| `compose/sequential.go` | typed state 固化 OriginalInput |
| `compose/handoff.go` | typed state 固化 OriginalInput（或复用现有 handoffState） |
| `agent/rewrite_test.go` | 新增：空 owner 改写、空 AgentName 改写、默认 Name 唯一性 |
| `agent/session_test.go` | 新增：double-join 被拒 |
| `compose/sequential_test.go` 等 | 新增：Messages 被篡改时 OriginalInput 兜底 |

### 校验

- `go build ./...` + `go test ./agent/... ./compose/... -race -count=1`

---

## 与 ISSUE-42 的关系

42 建立了"跨 agent 消息改写"机制，43 补齐这个机制的边界条件：身份唯一性（H1/H2/M1）、并发契约完整性（M2）、改写路径覆盖性（L1）、输入来源可靠性（L2）。42 是骨架，43 是关节。
