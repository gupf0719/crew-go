---
id: ISSUE-44
title: 解耦 session 审计与 LLM 输入——BasicAgent 只看自己的历史
status: 🔲 待实现
priority: 高
created: 2026-04-17
depends: ISSUE-41, ISSUE-42, ISSUE-43
---

# ISSUE-44 · 解耦 session 审计与 LLM 输入

## 一句话结论

ISSUE-42 让 BasicAgent 在每次 LLM 调用前 replay `session.HistoryFor(name)`（降级后的跨 agent 历史）——把"session 审计"和"LLM 输入源"绑成同一条路径。结果：sub-agent 被强喂完整历史（降级版），而父编排层（Plan/Sequential/…）又在 input 里显式 relay，**同一内容在 LLM 输入里出现两次**，白白吃 token。对齐 eino 架构：session 只做审计，BasicAgent 的 LLM 输入 = system + memory + **自己的** ReAct 轮历史 + 本次 input；跨 agent 上下文**由编排层显式注入 input**。

---

## 问题

### 现状链路（`agent/react.go:74`）

```go
messages = append(messages, session.HistoryFor(cfg.Name)...)
messages = append(messages, input)
```

`HistoryFor(name)` 返回整份 session 事件，非本 agent 的事件降级为 `user("For context: [X] ...")`。
每个 BasicAgent（不论是否在 Plan/Sequential 里）都会把**所有** sub-agent 的历史产出拉进自己的 LLM 请求。

### 具体冗余（plan_chat 实测）

writer 的 LLM 输入同时包含：
1. `HistoryFor("writer")` 里的 `user("For context: [researcher] ...")`（researcher 完整原文降级版）
2. `execInputBuilderWithTrace` 里的 `"上一步（researcher）的产出：<完整原文>"`

researcher 的输出**原文出现两次**。随 step 数线性放大，critic 看的是 researcher + writer 两份冗余。

### 根因

ISSUE-42 解决了"shared session 下跨 agent tool_call_id 配对失败"的真问题，但顺手把"自动 replay 全部历史给 sub-agent"做成了默认。后者没有必要：

- tool_call 配对只需要**本 agent 自己**的 assistant+tool 历史配对完整
- 跨 agent 上下文的传递应由**编排层决定**，不是 runtime 无条件注入

---

## 对齐 eino 的架构

eino `adk/prebuilt/planexecute/plan_execute.go:510-584`：
- executor 是 `ChatModelAgent`，用自定义 `GenModelInput` **完全替换** LLM 输入
- 执行器 prompt 由 `PlanSessionKey` / `ExecutedStepsSessionKey` 手搓，session 事件历史**不自动 replay**
- 跨 agent 改写（`flow.go rewriteMessage`）**只在 flow/transfer 场景**使用，不在普通 ChatModelAgent 里跑

拆解后的正确职责：

| 路径 | 现状 | ISSUE-44 后 |
|---|---|---|
| session 审计 | `HistoryFor(name)` 被 runtime 当 LLM 输入源 | `HistoryFor` / `History` 只作查询 API，给父层/用户观测 |
| BasicAgent LLM 输入 | system + memory + `HistoryFor(cfg.Name)` + input | system + memory + **本 agent 自己的事件** + input |
| 跨 agent 上下文 | 默认靠 `HistoryFor` 自动 replay（降级） | 编排层（Sequential/Plan/Handoff）**显式** 注入 input |

---

## 落地改动

### 1. `agent/rewrite.go` 新增 `SelfHistory(name)`

```go
// SelfHistory 返回 session 里仅属于 ownerAgentName 的事件消息。
// 与 HistoryFor 的区别：HistoryFor 把跨 agent 事件降级后一并返回（给审计/用户消费）；
// SelfHistory 只返回本 agent 自产，用于 BasicAgent 构造 LLM 请求——
// 跨 agent 上下文的传递改由编排层通过 input 显式注入（ISSUE-44）。
//
// ownerAgentName == "" → 返回空切片（无法归属，不泄漏）。
func (s *RunSession) SelfHistory(ownerAgentName string) []*schema.Message
```

### 2. `agent/react.go:74` 切换

```go
// ISSUE-44：只 replay 本 agent 自己的 ReAct 轮历史（tool_call 配对需要）。
// 跨 agent 上下文由编排层显式注入 input，不再 runtime 自动 replay。
messages = append(messages, session.SelfHistory(cfg.Name)...)
messages = append(messages, input)
```

### 3. `compose/sequential.go` step 1+ 显式 relay

step 1+ 的 input 从"复用 originalInput"改为：

```text
原始目标：<originalInput>

上一步（<prev node name>）的产出：
<state.LastOutput.Content>
```

removed the comment line "前面步骤的 output 已在 session.History() 中"——该路径不再被 runtime 消费。

### 4. `compose/handoff.go` 保持现状

Handoff 语义 = 路由，接收方只需要"最新用户意图"，当前实现就是取最后 user-role 消息作 input。SelfHistory 下行为不变。

### 5. `compose/plan.go` 保持现状 + 默认 builder 瘦身

`defaultExecInput` 继续显式注入 plan + executed + 当前步骤；切到 SelfHistory 后天然去重，无需再改。
（可选）Plan 的 `execInputBuilder` 默认可以考虑和 Sequential 对齐"goal + 上一步产出"，但不强制，留给调用方。

### 6. 文档同步

- `CLAUDE.md`：更新 `RunSession`、`rewriteCrossAgentMessage` 行；新增 `SelfHistory`
- `doc/description/design.md`：更新 Agent 内部数据流段、Sequential/Plan 流程图
- `doc/description/patterns.md`：Sequential 章节"step 1+ 复用 originalInput"改为"goal + 上一步产出"

---

## 校验

- `go build ./...`
- `go test ./agent/... ./compose/... -race -count=1`
- 新增测试：
  - `TestSelfHistory_OnlyOwnerEvents`：session 里混多 agent 事件，只回本 owner 的
  - `TestSelfHistory_EmptyName_ReturnsEmpty`
  - `TestSequential_StepOneGetsPrevOutput`：step 1 input 含 step 0 output
- plan_chat smoke：观察 writer/critic 的 `OnAgentStart` 输入长度显著下降，researcher 产出只出现一次

---

## 与 ISSUE-42/43 的关系

- 42 引入 `HistoryFor` + 改写机制，解决 tool_call_id 跨 agent 配对失败
- 43 补齐身份边界（默认 Name 唯一、空 owner 全改写等）
- **44 重划 42 的消费边界**：改写机制**保留**（作审计 API 给用户用），但 runtime 的 LLM 输入路径切到 `SelfHistory`，**不再 replay 跨 agent 事件**。42 解决的 tool_call 配对问题在 SelfHistory 下天然不存在（只看自己的，不存在跨 agent 配对）。

---

## 非目标

- 不移除 `HistoryFor` / `rewriteCrossAgentMessage`：它们对需要 replay 的用户自定义场景仍有价值
- 不动 `Memory.Read` 改写路径（L1）：Memory 消息不带 AgentName 归属，注入前降级的做法保留（v1 历史 — PR 3.3 commit `e6505f9` 后 Memory 接口已整体替代为 `store.Store`，此条历史叙述仅供溯源）
