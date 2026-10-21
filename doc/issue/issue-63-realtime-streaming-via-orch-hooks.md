---
id: ISSUE-63
title: 实时 streaming - OrchestrationHooks 从 batch drain 改为 per-event 实时投递
status: ✅ 已实现（v2 channel-based 重设计自然落地 + ChainOrchHooks 增量补）
priority: 高
created: 2026-05-01
implemented: 2026-05-11
---

# ISSUE-63 · 实时 streaming via OrchestrationHooks

> **实施状态**：本 ISSUE 描述的是 v1 架构问题。**v2 channel-based 重设计时已按本
> ISSUE 设计落地**——只是 ISSUE 一直没标"已实现"。本次 P1 验收后正式标完成。
>
> **v2 实现验证**：
> - ✅ `compose_v2.OrchestrationHooks.OnNodeEvent(ctx, nodeID, event)` 单事件签名
>   （`compose_v2/hooks.go:46`）
> - ✅ `safeOnNodeEvent` 框架层 panic recover 包装（`hooks.go:115`）
> - ✅ `nodeEventSink.Send()` 实时同步透传到 hooks（`hooks.go:162`，**不是 batch drain**）
> - ✅ `BasicAgent makeModelFn` 用 `streamOrGenerate` + sink.Send chunk 实时投递
>   （`agent_v2/basic_agent.go:773`）
> - ✅ `RecordingHooks` test helper（`compose_v2/recording_hooks.go`）
> - ✅ `TestHooks_NodeEventSink_RealtimeDelivery` 测试覆盖（`hooks_test.go:259`）
> - ✅ Hook panic 测试（`TestHooks_PanicInOnNodeEnd_NotCrashGraph`）
> - ✅ Resume 路径——v2 cp 不存 events（设计取舍 audit R3 H2 已澄清），业务侧自管
>   SSE 流持久化（如 Redis Stream + 前端 last-event-id 续传）
>
> **本次 ISSUE-73 P1 增量补**（commit 待提交）：
> - ✅ `ChainOrchHooks` 多 OrchHooks 链 + panic 隔离（对齐 hook.ChainHooks 设计；
>   audit Round 7 Bug 2 panic 隔离语义—— void 方法 panic silent recover、err
>   方法 panic 转 error fail-fast）+ 4 测试覆盖

## 背景

`example/orchestration/plan_chat_web` 已在前端测试，定位是生产级 LLM chat / workflow 应用。
当前架构无法支撑「LLM token 实时推送前端」（ChatGPT-grade 体验）：

- `eventSink` 收集事件用 `[]*AgentEvent` slice + 节点 return 后批量 drain
- `OrchestrationHooks.OnNodeEvents([]*Event)` 是节点结束时一次性投递
- 即使节点内 `state.Send(chunk)` 多次，事件**也得等节点 return 才能出**
- 前端体验：「LLM 节点开始 → 等待 → 完整答案瞬间出现」（无 token 流）

## 设计选择

### 为什么不学 LangGraph 的 `astream` 返回 chan

考虑过新增 `RunGraphStream(ctx) (<-chan StreamEvent, <-chan FinalResult)`，
但**砍掉了**——理由：

1. Go 没有 async generator，chan-based 写起来比 callback 更绕（goroutine + select + close 时机）
2. OrchHooks 已经是 callback 接口，**只是当前 batch 投递不实时**——修一下就够
3. 双 API 共存只为「兼容老 caller」，但项目未上线，这个理由对我们无效（CLAUDE.md 规约）

### 为什么不学 LangGraph 的 channel + reducer state 模型

那是 state 管理层，跟 streaming 无关。我们的 state 模型早就跟 eino 对齐
（typed struct + `ProcessState[S]`），不打算改。

### 选定方案：callback 实时投递

```go
// 旧
type OrchestrationHooks interface {
    ...
    OnNodeEvents(ctx, nodeID, events []*AgentEvent)  // 节点 return 后批量
}

// 新
type OrchestrationHooks interface {
    ...
    OnNodeEvent(ctx, nodeID, event *AgentEvent)  // 节点内 state.Send() 直接转发
}
```

`state.Send(event)` 不再 push 到 sink slice，直接同步调 `hooks.OnNodeEvent(event)`。
sink 抽象基本可以删除（或退化为 nil-hook 时的 no-op）。

## 改动清单

### 必做

| 改动 | 文件 |
|---|---|
| `OrchestrationHooks.OnNodeEvents` → `OnNodeEvent` 单事件签名 | `compose/orch_hooks.go` |
| `eventSink` slice + drain → 直接 callback 转发到 hooks | `compose/event_sink.go`（可能整个删） |
| `state.Send()` 改为同步透传到 hook | `compose/graph.go` |
| `runBatch` / `graphLoop` 删除 batch drain → 投递逻辑 | `compose/superstep.go` / `checkpoint.go` |
| 框架级 `defer recover` 包 hook 调用（防 hook panic 拖崩节点） | 新 helper `compose/hooks_safe.go` |
| BasicAgent modelNode 改用 `chatModel.Stream` + 边读边 emit chunk | `agent/basicagent_graph.go` |
| 新增 `state.SendChunk(chunk)` 或扩 `AgentEvent` 含 chunk 类型 | `compose/event.go` |
| `plan_chat_web` orchAdapter 接 `OnNodeEvent` 单事件 + 前端 token 拼接 | `example/orchestration/plan_chat_web/` |

### 配套

| 项 | 说明 |
|---|---|
| `RecordingHooks` helper | 测试用，把所有 OnNodeEvent 收集成 slice 给断言 |
| `BufferedHooks` helper（可选） | 想要非阻塞 dispatch 的 caller 用——内部 chan + goroutine 读 |
| Resume 路径事件回放契约 | 启动时把 cp.Events 一条条投递 OnNodeEvent，之后转实时（前端无需特殊 catch-up） |
| Hook panic 测试 | 故意 panic 的 hook 不应拖崩 graph |

### 不在范围（可后续 ISSUE-64+）

- `Agent.Stream(ctx)` 接口（`compose.RunGraph` + OrchHooks 已够，Agent 层暂不必）
- Stream-aware NodeFunc（eino Runnable 4 模式那种节点接口流式契约——对 LLM agent 场景 overkill）
- 节点间真 stream pipeline（A 流 → B 流式处理）
- 持久化 Checkpointer 后端（SQLite / Redis）

## 架构风险审计

| 风险 | 处理 |
|---|---|
| 同步 callback 阻塞慢 consumer | feature 而非 bug（天然 backpressure），文档说清楚 |
| 多 goroutine 并发触发 | hook 实现自保线程安全（已是现状） |
| Hook panic 拖崩节点 | **框架层 `defer recover`**（必做） |
| Fan-out 事件顺序非严格 | 跟 LangGraph 同 trade-off，调用方需要严格序就自己 sort |
| Resume 历史事件回放 | 选定 replay 模式，文档明确 |

**没有架构缺陷**——callback 模式跟 LangGraph chan 模式表达力等价，仅 Go 习惯不同。

## 实施顺序

1. 改 `OrchestrationHooks` 接口签名 + 框架 panic recover
2. 改 `state.Send()` + 删 / 改 `eventSink`
3. 修所有 hook 实现（plan_chat_web orchAdapter、各 test hook）
4. BasicAgent modelNode 走 `chatModel.Stream`
5. Resume 路径回放契约实现
6. plan_chat_web 前端 token 拼接渲染
7. e2e 测试 + RecordingHooks helper

## 验证

- 单测：节点内 N 次 `state.Send` → 测试用 RecordingHooks 收到 N 次 OnNodeEvent，且**调用时机**是节点 return 之前（用 channel 同步证明）
- e2e：plan_chat_web 接前端，LLM 答案 token-by-token 显示
- 回归：原有所有测试通过（OrchHooks 接口签名变更后）
- Resume：触发 ask_user → 前端断开 → 重连 → 收到历史 events 重放 + 新 events 实时
