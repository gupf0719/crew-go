# ISSUE-42 · 共享 session 下跨 agent tool_call 配对失败

**优先级**：高
**状态**：✅ 已实现（2026-04-17），对齐 eino `rewriteMessage`
**来源**：ISSUE-41 共享 session 方案推广后暴露的根因；对比 eino `adk/flow.go:171` 的跨 agent 消息改写设计

---

## 一句话结论

ISSUE-41 让编排模式共享同一个 `RunSession`（父可观测、可 checkpoint），但 `BasicAgent.reactLoop` 把 `session.History()` 整体塞进 LLM 请求时**没做跨 agent 身份改写**——A 之前产生的 `assistant(tool_call)` + `role=tool(tool_result)` 原样进入 B 的请求，B 并没"发起"那些 tool_call，OpenAI API 直接 422 报 `tool_call_id` 无法配对。

修复对齐 eino：非本 agent 的 assistant/tool 消息在拼入 LLM 请求前降级为 `user role` 的上下文描述，保证每次请求里的 tool_call/tool_result 配对闭合在发起者自己的消息流内。

---

## 现象

两 agent Sequential，A 带工具、B 不带。A 跑完 → B 跑：

```text
A 阶段 session.events:
  [A] user: "北京天气"
  [A] assistant: tool_call("get_weather",{city:"北京"})  ← tool_call_id=c1
  [A] tool: "sunny, 25°C"                               ← for c1
  [A] assistant: "北京今天晴 25°C"   (exit)

B 的 LLM 请求（改动前）:
  system: B.prompt
  user:   "北京天气"
  assistant: tool_call("get_weather", ...)   ← B 的工具集里没有这个工具
  tool:    "sunny, 25°C"                     ← B 此次请求里没发起者
  assistant: "北京今天晴 25°C"
  user:    <这一轮的 input>

→ OpenAI 422: tool_call_id c1 has no corresponding tool_calls in the request
```

---

## 为什么会这样

### 第一层：共享 session 把 A 的"内部工具交互"暴露给了 B

ISSUE-40 / ISSUE-41 联合让 session 成为"对话总线"——events 不再 fork 隔离，所有子 agent 都写到同一条主干（并发场景用 lane 单 writer + JoinLanes 归并）。设计初衷是父编排能看见、能 checkpoint。

副作用：子 agent 调 LLM 时 `reactLoop` 第 66 行 `append(messages, session.History()...)` 会把另一个 agent 的**完整 ReAct 轨迹**（含 tool_call/tool_result 对）一并塞入当前请求。

### 第二层：OpenAI API 对 tool_call 的配对是硬约束

`tool_call_id` 必须在同一次请求里指回一条携带对应 tool_call 的 assistant 消息。换言之，tool_call/tool_result 是"必须成对出现在同一次请求"的原语，**它表达"模型当前决定要调用的动作"而不是"历史对话事实"**。跨 agent 的共享历史如果不改写，就会把 A 的"动作"伪装成 B 当前请求里的"动作"。

### 第三层：我方 session.History() 无 agent 归属视角

`AgentEvent.AgentName` 字段一直存在，但 `RunSession.History()` 只返回 `[]*schema.Message`，丢了归属。`reactLoop` 也无从区分"这条消息是我的还是别人的"。

---

## eino 的做法（参考）

`eino/adk/flow.go:171` `rewriteMessage` + `flow.go:258` `genAgentInput`：

- ChatModelAgent 每次调 LLM 前 `genAgentInput` 遍历 `runCtx.Session.getEvents()`
- 非本 agent 产生的消息（`entry.AgentName != agentName`）走 `rewriteMessage`
- 改写规则：
  - `assistant(text)` → `user("For context: [A] said: ...")`
  - `assistant(tool_calls)` → `user("For context: [A] called tool X(args)")`
  - `role=tool(tool_result)` → `user("For context: [A] tool returned: ...")`
- 改写后消息一律 `role=user` 且无 tool_calls，不会与 B 自己的 tool_call 流混淆

额外提供 `WithHistoryModifier` 选项让用户手动裁剪 history——但这是可选项，默认仍是**全量加载 + 自动改写**。

---

## 方案

对齐 eino 的"全量 + 自动改写"思路，在 agent 层加一层带归属的 history 视图，`reactLoop` 切换到新视图。

### 1. 新增 `agent/rewrite.go`

```go
// 改写单条消息
func rewriteCrossAgentMessage(fromAgent string, msg *schema.Message) *schema.Message {
    if msg == nil { return nil }
    if msg.Role == schema.RoleUser || msg.Role == schema.RoleSystem {
        return msg  // 共享对话背景原样
    }
    // assistant(tool_calls) / assistant(text) / tool → user role 描述
    // （省略）
}

// session 层的 owner 视角
func (s *RunSession) HistoryFor(ownerAgentName string) []*schema.Message {
    if ownerAgentName == "" { return s.History() } // 退化兼容
    evs := s.Events()
    msgs := make([]*schema.Message, 0, len(evs))
    for _, e := range evs {
        if e.Output == nil || e.Output.Message == nil { continue }
        if e.AgentName == "" || e.AgentName == ownerAgentName {
            msgs = append(msgs, e.Output.Message)
            continue
        }
        if r := rewriteCrossAgentMessage(e.AgentName, e.Output.Message); r != nil {
            msgs = append(msgs, r)
        }
    }
    return msgs
}
```

### 2. `agent/react.go:66` 切换

```go
// 改前
messages = append(messages, session.History()...)
// 改后
messages = append(messages, session.HistoryFor(cfg.Name)...)
```

### 3. `History()` 保留作为 legacy 原始视图

- 老的、非 BasicAgent 的自定义 Agent 实现可能直接读 `session.History()` 做观测/日志，不影响它们
- 所有走 BasicAgent 的路径自动受益

---

## 关键设计决策

**为什么不改 `History()` 直接默认改写**：`History()` 无法知道"当前是哪个 agent 的视角"。改它等于强制所有消费者声明 owner，破坏兼容。新增 `HistoryFor(name)` 显式带视角、语义清楚。

**为什么本 agent 消息要原样保留**：同一 agent 跨多轮 ReAct 迭代的 `assistant(tool_call) → tool_result` 对必须配对完整。改写自己的消息会破坏自己的 tool_call 流。

**为什么 `user/system` 消息不改写**：`user` 代表"共享对话背景"（用户输入、Planner 下发的指令文本），跨 agent 传递时本来就是 user role 语义；`system` 同理（虽然当前框架不把 systemPrompt 写进 events，仍做兼容处理）。

**匿名事件（`AgentName == ""`）保守不改写**：历史遗留数据或框架早期路径产生的事件可能缺归属。视为本 agent 消息直通，避免把已知合法的消息误降级。

---

## 落地改动

| 文件 | 改动 |
|---|---|
| [agent/rewrite.go](../../agent/rewrite.go) | **新增** — `rewriteCrossAgentMessage` + `RunSession.HistoryFor(name)` |
| [agent/react.go](../../agent/react.go) | `session.History()` → `session.HistoryFor(cfg.Name)`（line 66） |
| [agent/rewrite_test.go](../../agent/rewrite_test.go) | **新增** — 5 个 case：owner 直通、cross-agent 降级、user/system 穿透、空 owner 退化、匿名事件保守处理 |

### 校验

- `go build ./...` 通过
- `go test ./agent/... ./compose/... -race -count=1` 全绿
- 新增 5 个单测全部通过

---

## 与 ISSUE-40/41 的关系

- ISSUE-40 让 events 能持久化
- ISSUE-41 让父 session 能观测子 agent events（lane 单 writer + Join 归并）
- ISSUE-42 让子 agent 读这个共享 events 时不炸 LLM API

三者合起来闭环"共享 session"这条路线。缺任何一个，共享 session 的价值或正确性都不完整。

---

## 已知限制 / 后续考虑

- 当前改写函数签名是私有 `rewriteCrossAgentMessage`，不对外；未来若需用户自定义（如参考 eino 的 `WithHistoryModifier`），再把它升格为可注入的 hook 或 option
- `HistoryFor` 每次调用是 O(events) 的线性扫描 + 分配；高频场景如需优化可加 per-agent 视图缓存，当前无实测瓶颈不做预先优化
- 非 BasicAgent 的自定义 Agent 如果自行消费 `session.History()` 做 LLM 请求，仍有相同风险——需要用户自行改用 `HistoryFor(selfName)`，或升级为使用 BasicAgent 包装
