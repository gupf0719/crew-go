# ISSUE-40 · session.events 持久化

**优先级**：中
**状态**：✅ 已实现（2026-04-17），采用方向 D
**来源**：ISSUE-24·H3 遗留项的根因分析

---

## 一句话结论

Resume 一个对话后，`sess.History()` 返回空——不是 bug，是 Checkpoint 从一开始就没把 session 事件流存进去。spec 和我方都没做完这一步。方向 D 补上：`Checkpoint` 加 `Events` 字段，`AgentAction.CustomizedAction` 不进 checkpoint（未来需要结构化控制信号就像 eino 那样升格为专属字段），无需类型注册表。

---

## 现象

调用方的视角：

```go
// 第一轮
sess := agent.NewRunSession()
_, _ = compose.RunGraph(ctx, g, input, agent.WithSession(sess))
// ... 模型触发 InterruptError，checkpoint 自动写盘

// 进程重启 / 换节点
sess2 := agent.NewRunSession()
_, _ = compose.ResumeGraph(ctx, g, checkpointer, sess.ID)

// 改动前：
sess2.History()   // nil ❌
sess2.Events()    // nil ❌

// 改动后：
sess2.History()   // [msg1, msg2, ...] ✅
sess2.Events()    // [event1, event2, ...] ✅
```

Resume 本身一直是成功的，图从中断点继续跑。丢的是"会话记忆"这一面。

---

## 为什么会这样（三层根因）

### 第一层：Checkpoint 只存了 messages，没存 events

`Checkpoint` 数据结构里只有 `Messages []*schema.Message` 字段，没有 events。Resume 时拿到 checkpoint 也只能还原 `state.Messages`，`session.events` 无处可取。

这是 spec 层面本来就留白的事——`Checkpoint` 在 spec 里就只定义了 messages，没定义 events。我方 Go 实现继承了这个定义，没扩展。

### 第二层（前瞻性，不是当前 bug）：`CustomizedAction` 是 `any`

`AgentEvent → AgentAction → CustomizedAction` 是一个 `any`（spec 里是 `unknown`）。如果有代码往这里塞结构体再序列化，JSON round-trip 回来只会是 `map[string]any`，业务代码 `action.(*XxxTarget)` 会 panic。

**注意**：代码库里当前**没有任何地方写入 `CustomizedAction`**（仅 design.md 的一段示例伪代码提及）。这是**未来如果开始使用**才会触发的问题，不是当前阻塞。

### 第三层：为什么留这个 `any` 口子

早期为了灵活——框架不想限定能传什么控制信号，就给了一个"什么都能塞"的口子。spec 也是这么设计的，不是我方多开的。

---

## 我们离 spec 有多远

| 层面 | spec 怎么定 | 我方怎么做 |
|---|---|---|
| `AgentEvent` 类型存在 | ✅ 有 | ✅ 沿用 |
| `Session` 有 events 字段 | ❌ 只有 `store: unknown` | ✅ **我方扩展**了 `RunSession.events` |
| `Checkpoint` 有 events 字段 | ❌ 只有 `messages` | ❌（改动前） → ✅（改动后） |
| `AgentAction.customizedAction` 类型 | `unknown` | `any`（继承）→ `json:"-"` 不持久化 |

订正后的定位：**我方比 spec 多走了半步（加了 session.events 提供运行时可观测），但没走完另半步（没持久化）**。本 issue 把后半步补上。

---

## 方向对比

### 方向 A · 事件溯源（单一数据源）

砍掉 `state.Messages`，`session.events` 作为唯一真相，Messages 永远是投影。

- ✅ 数据不重复
- ❌ 所有读 `state.Messages` 的编排代码（~40 处）要改
- ❌ 每次要 Messages 都得从 events 重算，不能缓存
- 类似：LangGraph

### 方向 B · 拆成两个列表 + 类型注册表

承认 events 里装了两种东西——消息 + 控制信号——拆成两个列表。`ActionRecord{Type string, Payload json.RawMessage}` 加全局 `RegisterAction[T](name)` 查表还原。

- ✅ 四层阻力一并解掉
- ❌ 过度设计——代码库里当前没人写 `CustomizedAction`，注册表是针对"动态扩展"场景的，我方没有这类需求
- 参考：eino schema（但 eino 自己也没走这条路）

### 方向 C · Session 就是 Checkpoint

Checkpoint 直接就是 Session 的序列化形式，`GraphState.Messages` 这条通路彻底消失。

- ✅ 概念最干净
- ❌ 几乎所有编排代码都要改
- ❌ typed state 怎么存需要单独设计

### 方向 D · 常用信号升格专属字段，any 不持久化（**实际采用**）

承认 "有些信号需要持久化（结构化），有些只是运行时通知（可以丢）"。
- 需要持久化的信号：升格为 `AgentAction` 专属字段（如 eino 的 `TransferToAgent *TransferToAgentAction`）
- 不需要持久化的：继续塞 `CustomizedAction any`，但标注 `json:"-"` 明确不进 checkpoint

```go
type AgentAction struct {
    Exit             bool
    BreakLoop        bool
    CustomizedAction any   `json:"-"`   // 运行时字段，不持久化
    // 未来按需追加专属字段，例如：
    // TransferToAgent *HandoffTarget `json:"transfer_to_agent,omitempty"`
}
```

- ✅ 改动最小——代码库里没人用 `CustomizedAction`，标 `json:"-"` 不破坏任何现有路径
- ✅ 不需要类型注册表也不需要换序列化格式
- ✅ 有实战背书：eino 就是这么做的
- ⚠️ 新信号需要改 struct 加字段（我方一年加不了几个，成本可忽略）
- 参考：[eino adk/interface.go](../../../eino/adk/interface.go) 的 `AgentAction`

### 对比矩阵

| 维度 | A 事件溯源 | B 拆列表+注册表 | C Session=Checkpoint | **D 专属字段+any不存** |
|---|---|---|---|---|
| 概念清晰度 | 高 | 中 | 最高 | 高 |
| 改动范围 | 大（40+） | 中（30） | 极大 | **小（~10）** |
| 破坏性 | 高 | 低-中 | 高 | **无** |
| 性能 | O(n) 重算 | 无 | 无 | **无** |
| 能不能逐步加信号 | 一般 | 好 | 差 | **好** |
| 实战参考 | LangGraph | eino schema | 无 | **eino adk** |

---

## 为什么最终选 D

### 一条原则：**持久化的数据结构里不能藏 `any`**

这不是偏好，是 Go 语言层面的事——`encoding/json` 对 `any` 反序列化就是没办法还原具体类型。方向 B（注册表）和方向 D（专属字段）都是为了绕开这条约束。

### 为什么 D 比 B 简单

方向 B 假设"有许多种动态生成的信号类型需要持久化"。我方代码库的现状不是：
- `CustomizedAction` 当前**零使用**
- 未来可预期要持久化的信号就几种（handoff target、break loop payload、interrupt metadata）

对"就几种"的场景，**写成 struct 字段比写注册表更直白**。注册表要维护"类型标签 ↔ Go 类型"的映射，struct 字段自身就是契约。

### eino 的实战背书

eino 的 `AgentAction` 结构（[adk/interface.go:168-180](../../../eino/adk/interface.go)）就是方向 D：

```go
type AgentAction struct {
    Exit            bool
    Interrupted     *InterruptInfo            // 专属
    TransferToAgent *TransferToAgentAction    // 专属
    BreakLoop       *BreakLoopAction          // 专属
    CustomizedAction any                      // 兜底
    internalInterrupted *core.InterruptSignal // 不导出
}
```

eino 在实践中发现：升格专属字段就够用了，`CustomizedAction` 保留但极少使用。他们的 `serialization` 走 gob + `schema.RegisterName` 注册表——但这主要是为解决 `interface{}` + 流式消息这两个特殊问题，不是为 `CustomizedAction`。

### 为什么不换 JSON 为 gob

eino 用 gob。我方**不跟进**，理由：
1. **跨语言不兼容**——spec 初衷是跨语言，Java/Python runtime 读不了 gob
2. **wire format 脆弱**——eino 自己 [adk/interrupt.go:224-261](../../../eino/adk/interrupt.go) 为兼容 v0.8.0-v0.8.3 老 checkpoint 得做 on-wire bytes 替换，这是 gob 的原罪
3. **我方 Checkpoint 已经是 JSON tag**，换 gob 要推倒重来

坚持 JSON，方向 D 在 JSON 约束下也能干净落地。

---

## 实际落地

| 文件 | 改动 |
|---|---|
| [agent/event.go](../../agent/event.go) | `Checkpoint` 加 `Events []*AgentEvent`；`AgentAction.CustomizedAction` 标 `json:"-"`；所有字段加 JSON 标签 |
| [agent/session.go](../../agent/session.go) | 新增 `RunSession.RestoreEvents`（直接赋值不触发 hook）；`InMemoryCheckpointer.Save` 浅拷贝 Events slice |
| [agent/runpath.go](../../agent/runpath.go) | `RunStep` 加 JSON 标签 |
| [compose/checkpoint.go](../../compose/checkpoint.go) | `marshalGraphState` 写入 `session.Events()`；`ResumeGraph` 灌回 `cp.Events`；移除 H3 已知限制注释 |
| [compose/checkpoint_test.go](../../compose/checkpoint_test.go) | 新建 3 条测试：round-trip / Resume 后 History 非空 / InMemoryCheckpointer 拷贝合约 |
| [CLAUDE.md](../../CLAUDE.md) | Checkpoint 速查 + AgentAction 持久化约定 |

### Resume 为什么不触发 hook

`RestoreEvents` 是**直接赋值到 session.events slice**，不走 `AddEvent`，因此：
- OnAgentStart / OnError / 埋点回调不会被二次触发
- Prometheus / OpenTelemetry 不会收到重复样本
- 用户自定义 hook 若假设 idempotent（"收到 Error 就告警"），Resume 不触发假告警

这符合 Resume 的 "continue" 语义——不是事件重演，而是状态快照恢复。

---

## 原方案（方向 B）被证伪的两个论证

最初 issue 版本推荐方向 B，基于两个论据。订正后都被证伪：

### ❌ 论据 1：`HandoffAgent 往 CustomizedAction 塞 HandoffTarget 结构体`

实际查：`grep "CustomizedAction:" .` **整个代码库零匹配**，仅 `design.md` 的一段示例伪代码提及（还用的是字符串）。HandoffAgent 走 [extractHandoffTarget()](../../compose/handoff.go) 解析 message 文本 / tool call / annotations，不走 `AgentAction`。

**影响**：`any` 反序列化丢类型"是前瞻性顾虑，不是当前 bug。方向 B 的注册表因此是过度设计。

### ❌ 论据 2：`state.Messages 和 session.events 70% 重叠`

实际查：[RunSession.History()](../../agent/session.go) 完全从 events 重建——不存在"两条通道各存一份消息"。events 就是消息唯一来源。

**影响**："双数据通道冗余"不成立；方向 A 的价值减弱；方向 D 只需在 events 单源之上加持久化即可。

---

## 未来可做的事（不在本 issue 范围）

1. **HandoffAgent 升格专属字段**：`AgentAction.TransferToAgent *HandoffTarget`，替代当前文本解析。风险：改 extractHandoffTarget 调用点，向后兼容需保留文本 fallback。
2. **反向推 spec**：把 spec 的 `customizedAction: unknown` 改成带类型标签的 discriminated union；`Checkpoint` 加 events 字段。让我方不是"偏离 spec"而是"引领 spec"。
3. **InterruptError 结构化**：当前 `Data any`，同样有持久化问题。按方向 D 思路升格为 `InterruptInfo struct`。

---

## 关联 issue

- 闭环 ISSUE-24·H3 遗留项 ✅
- 不阻塞 ISSUE-30（流式接口）
- 为未来的分布式 Agent（session 放到外部存储）铺路
