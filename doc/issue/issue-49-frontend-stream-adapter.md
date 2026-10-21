---
id: ISSUE-49
title: 前端接入适配层（Agent → SSE/WebSocket 流协议）
status: ✅ 已交付（`agui/` 包，见 issues.md 同名条目）。🔄 决策修订（2026-07-01）：改为做 AG-UI 适配包（独立小包，不侵入核心）——推翻 06-15 的"不做"（AG-UI 是对齐外部标准协议 + 复用 CopilotKit 前端，非自定义 SSE 抽象）。详见本文档末"2026-07-01 决策修订"段
priority: 中
created: 2026-04-23
reference: /Users/gupengfei/program/go/src/langgraph-bb/LANGGRAPH_STREAM_FRONTEND.md
---

# ISSUE-49 · 前端接入适配层

## 背景

crew-go 当前对"前端接入"这个维度是**空白**：

- `basic_chat/` 和 `plan_chat/` 是 CLI demo，通过 stdin/stdout 交互
- 没有 HTTP handler、没有 SSE 编码器、没有 WebSocket 适配层
- 没有"把编排好的 agent 暴露给浏览器"的统一入口

但**数据源已经齐全**——前端需要的一切事件，框架内部已经有了：

| 数据类型 | 现有出处 |
|---|---|
| LLM token 流 | `model.ChatModel.Stream` 返回的 `schema.StreamReader[T]` |
| 节点输入/输出增量 | `compose.OrchestrationHooks.OnNodeStart/OnNodeEnd` |
| 节点事件冒泡 | `OrchHooks.OnNodeEvents`（checkpoint.go L213） |
| 完整 state 快照 | `GraphState` 超步边界 |
| AgentEvent（含 Output/Action/Checkpoint） | `agent.RunSession.Events()` / `eventSink` |
| 中断/存档 | `InterruptError` + `Checkpoint` |

缺的是"**把这些数据源组合编码成前端能消费的流协议**"的中间层。

## 为什么现在记录

LangGraph 的 `LANGGRAPH_STREAM_FRONTEND.md` 把"agent 执行 → 前端展示"的分层打磨得比较成熟——五种 stream_mode 的分类、"正文 vs 中间过程"的前后端约定、重试时的 chunk 召回机制——这些都是成熟产品踩过坑沉淀下来的设计，我们在设计对外协议前先把它的思路记录下来，避免将来动手时从零拍脑袋。

## LangGraph 五种 stream_mode 到 crew-go 数据源的映射

| LangGraph mode | 语义 | crew-go 对应数据源 | 自动 / 手动 |
|---|---|---|---|
| `messages` | LLM token 级增量（打字机效果） | `model.ChatModel.Stream` → `schema.StreamReader[*Message]` | 自动（model 层已支持） |
| `updates` | 每个节点 return 后的 delta `{node_name: 输出}` | `OrchHooks.OnNodeEnd` + 节点输出（LastOutput） | 自动（hook 已存在） |
| `values` | 每个超步 merge 完成后的完整 State 快照 | `GraphState` 在超步边界的快照（需新增 hook 或 adapter 封装） | 半自动（需在超步边界加 hook） |
| `debug` | 执行生命周期事件（节点开始/结束/checkpoint） | `OrchHooks.OnNodeStart/End` + ISSUE-46 的 superstep 事件 | 自动 |
| `custom` | 业务代码主动推的任意数据（进度、状态、中间值） | 节点内往 `eventSink` 写 AgentEvent / 自定义 Annotations | 手动 |

**结论**：五种分类可以直接抄作为 SSE event type，crew-go 的数据源都对得上。

## 前后端约定机制

LangGraph 用两种方式区分"正文 vs 中间过程"：**节点名约定** 和 **tags 约定**。crew-go 的现成资产几乎白拿：

### 节点名约定（粗粒度）

LangGraph：后端给节点命名带前缀（`respond_node` / `plan_node`），前端按前缀路由。

crew-go 对应：
- `compose.NodeID`（`AddAgentNode`/`AddNode` 的 id 参数）
- `BasicAgent.Name()`
- `PlanAgent.ExecutedStep.Agent`（已记录实际执行 agent 名，含 `SubAgentName` / `TmpSubAgent.Name` / 自动 `tmp_step_<N>`——见 CLAUDE.md "核心类型速查"）

约定示例（实现时再定）：

- `respond_*` / `final_*` 前缀 → 展示给用户的正文
- `plan_*` / `research_*` / `tmp_step_*` 前缀 → 中间过程，前端可折叠

### Tags 约定（细粒度）

LangGraph：在 LLM 调用 config 里塞 `tags: ["show_user" / "show_process"]`，前端按 tag 路由。

crew-go 对应：
- `schema.Message.Annotations`（已存在）
- 或在 `model.Options` 里加 `WithTags(...)`（需要新增，侵入面小）
- 信号可以走 `Annotations["stream_channel"] = "user" | "process" | "debug"`

**推荐**：两种机制都留，先上节点名约定（简单够用），精细化场景再加 tags。

## 需要新增的字段：run_id / chunk_id

LangGraph 的 chunk 都带 `id`，前端遇到新 id 就把旧未完成的 chunk 标废弃——这是**重试时不错乱**的关键机制。

crew-go 当前：

- `AgentEvent` 有 `CreatedAt`，**没有 run_id / message_id**
- `model.Stream` 返回的 chunk 也没有稳定 id

**协议设计时必须补**：每条流式消息带 `run_id`（一次 LLM 调用的 id），前端靠 run_id 分组和去重。RetryPolicy / LoopAgent 重试时新调用 → 新 run_id → 前端识别"旧的作废"。

现在不补成本低，等协议定下来再加字段成本大（会破坏已上线的前端契约）。

## 不能直接照搬 LangGraph 的部分

| LangGraph | 对应 crew-go 方案 |
|---|---|
| `astream` / `astream_events` (Python async iterator) | Go channel + `http.ResponseWriter` 流式写（SSE）或 WebSocket handler |
| `adispatch_custom_event` / `get_stream_writer`（context-local callback） | **不要硬翻 context callback**；直接给节点传 `writer` 参数或复用 `eventSink`，Go 风格更自然 |
| LangChain callback 系统自动拦截 LLM token | crew-go 已有 `model.ChatModel.Stream`，adapter 层直接消费 StreamReader 即可，不需要 callback 体系 |
| TypedDict state 的字段级订阅 | crew-go 是 `any` localState + typed 泛型读取，字段级订阅需要反射，复杂度高，暂不考虑 |

## 架构草案（仅记录，不实现）

```
┌────────────────────────────────────────────────┐
│  HTTP/WS Server Layer (新增包 server/ 或 adapter/)  │
│  - POST /agents/:name/run   建会话并流式返回        │
│  - GET  /agents/:name/stream  SSE/WS 事件流        │
│  - POST /sessions/:id/resume  Resume 已存档会话    │
└───────────────┬────────────────────────────────┘
                │ 监听 + 编码
┌───────────────┴────────────────────────────────┐
│  Event Stream Encoder                           │
│  - 订阅 eventSink + OrchHooks + model 层 token  │
│  - 按 LangGraph 五类分型打标：                    │
│     token / update / value / debug / custom   │
│  - 附 run_id + node_name + tags(optional)       │
│  - 编码为 SSE 行或 WS 帧                          │
└───────────────┬────────────────────────────────┘
                │ 消费
┌───────────────┴────────────────────────────────┐
│  Agent 运行时（现有）                              │
│  agent.Agent.Run → compose.Graph → eventSink    │
└────────────────────────────────────────────────┘
```

**关键点**：

1. adapter 层**只读**事件源，不改变 agent runtime 行为（零侵入）
2. 所有事件源已经在，adapter 只做"订阅 + 编码 + HTTP 框架"三件事
3. `run_id` 字段要在 adapter 实现前补到 `AgentEvent` / `model.Stream` chunk 上——这是破坏性变更，**这一条是前置依赖**

## 开放问题（实现前必答）

**A. run_id 补在哪一层**
`AgentEvent.RunID` / `Message.RunID` / `StreamReader chunk meta`——三个位置需要选，选完影响 model 和 agent 两个包的 API。倾向 `AgentEvent` + `Message.Annotations["run_id"]` 双栖，`StreamReader` 由 adapter 层拼。

**B. SSE vs WebSocket 选型**
SSE 够 80% 场景、实现最简；WS 支持双向（前端打断 agent）但复杂度高。倾向**先只做 SSE 单向**，"前端打断" → 单独的 REST endpoint (`POST /sessions/:id/interrupt`) 触发 ctx.Cancel，不走同一条流。

**C. Interrupt / Resume 的前端协议**
中断时 adapter 要把 `Checkpoint.ID` 推给前端；前端提交人工输入后走 `POST /sessions/:id/resume` 触发 `ResumeGraph`。**协议字段：** 中断事件里带 checkpoint_id + interrupt payload（需要 ISSUE-40 的 `any` 序列化问题先解决）。

**D. 事件分类粒度**
是直接透出五种 mode 让前端自己过滤，还是在 adapter 层收敛成"展示层/过程层/调试层"三通道？LangGraph 选了前者（更灵活），我们可以跟。

**E. 多 agent 并发的事件隔离**
ForkLane 分支里产生的事件，adapter 要怎么暴露给前端？是打平到一条流（按 lane_id 打 tag）还是多条子流？倾向**单流 + lane_id tag**，对齐 ForkLane 的 session 合并语义。

**F. Node ID 前缀格式**
ISSUE-48 提到嵌套节点 ID 格式（`inner/stepA`）——adapter 层向前端暴露的 node 字段格式要和 ISSUE-48 保持一致。两个 issue 的 NodeID 契约要统一。

## 当前决定（2026-04-23）

**整体暂缓**——主体功能（ISSUE-44/45/46/47 收尾及后续主线）完成前不做。

### 理由

1. **前端接入是对外协议**，协议一旦定下来就难改，主体 API 还在演进时过早定协议 = 后续要反复破坏性迁移
2. 当前 agent 运行时的事件模型本身还在变（ISSUE-40 的 AgentEvent 持久化、ISSUE-46 的超步事件、ISSUE-48 的子图事件透传），adapter 层要等这些稳定才能定型
3. 没有真实前端用户倒逼协议选型，现在拍的协议多半要推倒

### 本文档的定位

**保留作设计底稿**，不标记为"待办"也不排期。

重启触发条件（满足其一再讨论）：

- 主体功能稳定（ISSUE-44/45/46/47 全部合并，agent/compose 包 API 一段时间不变）
- 有明确前端接入场景（内部 demo / 外部用户要求）倒逼
- ISSUE-40 的 `AgentEvent.CustomizedAction any` 持久化方案落地（影响 interrupt payload 协议）

重启时的入口：本文档"开放问题"A–F **每条都是必答题**，动手前必须先在文档里拍板。不得绕过直接写代码。

## 其他参考项目调研

### eino（2026-04-23 调研）

结论：**eino 在这块也是空白**，而且 Compose 层的事件粒度比 LangGraph 粗一个量级——抄 LangGraph 比抄 eino 合算。

| 维度 | eino 现状 | 对本 issue 的价值 |
|---|---|---|
| HTTP/SSE/WebSocket adapter | 全仓库无 `server`/`http`/`sse`/`websocket` 目录，无前端 demo | 无现成代码可抄 |
| StreamReader 对外语义 | `schema.StreamReader[T]` 只在节点间用（`adk/interface.go:36` 的 `MessageStream = *schema.StreamReader[Message]`），**无"图输出→HTTP流"适配** | 思路一致，无实现参考 |
| Compose 层 callback | `callbacks.Handler`（`callbacks/interface.go:62-85`）5 方法：OnStart/OnEnd/OnError/OnStartWithStreamInput/OnEndWithStreamOutput；`RunInfo` 含 Name/Type/Component | 粒度粗——无 token 级事件、无 tags、无 updates/values/debug 分型 |
| ADK 层事件流 | `adk/interface.go:224-238` 定义 `AgentEvent{Output, Action, Err, RunPath}`；`adk/callback.go:41-44` `AgentCallbackOutput.Events *AsyncIterator[*AgentEvent]` | **唯一值得参考的形状**：事件流订阅 API 用 `AsyncIterator[*AgentEvent]`——我们做 adapter 时对齐 `channel of AgentEvent` 形状，eino 用户迁移成本低 |
| Tags 机制 | **无** | LangGraph 独有；eino 也没想过"正文 vs 中间过程"——我们要做得原创 |
| run_id / message_id | AgentEvent 和 StreamReader chunk 都**无 id 字段** | 佐证"补 run_id 是全新工作"——谁都没做过，早定早赚（见本文档"需要新增的字段"一节） |
| Interrupt/Resume | `adk/interrupt.go:30-54` 的 `ResumeInfo` / `InterruptInfo` | 设计可借鉴；我们已有 `InterruptError` + `Checkpoint` 对齐 |

**关键推论**：

1. eino 的 Compose 层 callback 系统只够做"指标/日志"级别的观测，**不够支撑前端流**（缺 token、tags、run_id）。我们不能直接把它的 callback 形状搬来当 adapter 事件源
2. **抄 LangGraph 的五类分层比抄 eino 合算**——LangGraph 是成熟产品打磨出来的，eino 没走到这一步
3. **"补 run_id"是全新工作**——eino 也没做过，crew-go 要做前端接入时逃不开这条前置依赖

## 实施方案（2026-04-23）

基于现有代码实况（已核对签名）给出可落地的分期方案。**仍维持"暂缓实现"决定**，此节是设计落地稿，重启时照此执行。

### 现有资产核对

| 现成资产 | 位置 | adapter 如何使用 |
|---|---|---|
| `model.ChatModel.Stream` | [model/interface.go:15](model/interface.go:15) 返回 `*schema.StreamReader[*schema.Message]` | Token 流源头 |
| `compose.OrchestrationHooks` | [compose/orch_hooks.go:16](compose/orch_hooks.go:16)：5 方法 OnGraphStart/End + OnNodeStart/End + OnNodeEvents | 节点/图事件源头——**直接注入 streamHooks 实现即可** |
| `compose.RunGraph` | [compose/checkpoint.go:102](compose/checkpoint.go:102) 接受 `GraphRunOptions`（含 OrchHooks + Checkpointer） | 唯一执行入口，adapter 通过 opts 注入 hooks |
| `compose.ResumeGraph` | [compose/checkpoint.go:264](compose/checkpoint.go:264) `(ctx, g, checkpointer, sessionID)` | Resume 端点直接调用 |
| `agent.RunSession` + ID | [agent/session.go:55-85](agent/session.go:55) 自带 UUID | 天然的 session_id，adapter 注册表 key 直接用 |
| `agent.AgentEvent` | [agent/event.go:53-81](agent/event.go:53) 含 AgentName/RunPath/Output/Action/Err/Checkpoint/CreatedAt | 主事件载体，缺 `RunID` |
| `compose.eventSink` | [compose/event_sink.go:14-39](compose/event_sink.go:14)：只是 push/drain buffer | **不扩展**，复用 OrchHooks.OnNodeEvents 拿到 events 即可 |

**核心判断**：不需要重新发明事件管道——OrchHooks 已经是"全局事件订阅点"，adapter 只做"实现一个 OrchHooks → 推到 channel → 编码为 SSE"三步。

### 总体架构

```
server/                       (新增独立包，零侵入 agent/compose/model)
├── types.go                  对外 SSE event schema
├── registry.go               agent name → agent.Agent 注册表
├── session_store.go          SessionStore interface + InMemory 实现
├── stream_hooks.go           实现 compose.OrchestrationHooks，事件推到 channel
├── sse_encoder.go            SSE 编码器
├── handler_run.go            POST /agents/:name/run
├── handler_resume.go         POST /sessions/:id/resume
├── handler_interrupt.go      POST /sessions/:id/interrupt（ctx.Cancel）
└── server.go                 Server struct：wire registry + store + handler
```

### 事件聚合策略（核心决策）

**决策**：adapter 通过实现 `OrchestrationHooks` 接口订阅事件，**不改动 agent/compose 现有运行时**。

```go
// server/stream_hooks.go
type streamHooks struct {
    events chan<- *streamEvent
    runID  atomic.Value // current LLM run_id
}

func (h *streamHooks) OnNodeStart(ctx context.Context, id compose.NodeID, s *compose.GraphState) error {
    h.events <- &streamEvent{Type: "node", Phase: "start", NodeID: string(id), Ts: now()}
    return nil
}
func (h *streamHooks) OnNodeEnd(ctx context.Context, id compose.NodeID, s *compose.GraphState, err error) {
    h.events <- &streamEvent{Type: "node", Phase: "end", NodeID: string(id), Err: err, Ts: now()}
    // 可选：从 s.LastOutput 提 updates 类事件
}
func (h *streamHooks) OnNodeEvents(ctx context.Context, id compose.NodeID, evs []*agent.AgentEvent) {
    for _, ev := range evs {
        h.events <- convertAgentEvent(id, ev)  // AgentEvent → 对外 custom/interrupt/exit 事件
    }
}
```

Token 流（P2 阶段）通过在 `model.ChatModel` 外层包一个 `modelListener` 装饰器，在 Stream chunk 上转发给 adapter channel——**无需改 model 接口**。

### 对外协议（SSE）

```
Content-Type: text/event-stream

event: node
data: {"type":"node","phase":"start","node_id":"planner","run_id":"...","ts":1700000000000}

event: node
data: {"type":"node","phase":"end","node_id":"planner","run_id":"...","output":{...},"ts":...}

event: token                                          (Phase 2+)
data: {"type":"token","run_id":"...","agent":"planner","content":"上","seq":3,"ts":...}

event: state                                          (每超步结束)
data: {"type":"state","superstep":2,"messages":[...],"ts":...}

event: custom                                         (AgentEvent 里非 Exit/Interrupt 的 Output)
data: {"type":"custom","agent":"...","run_path":[...],"payload":{...},"ts":...}

event: debug                                          (OnGraphStart/End 等)
data: {"type":"debug","phase":"graph_start","ts":...}

event: interrupt
data: {"type":"interrupt","checkpoint_id":"ckpt-abc","payload":{...}}

event: done
data: {"type":"done","status":"success|error|interrupted","session_id":"..."}
```

**字段契约**：

| 字段 | 所有事件必带 | 语义 |
|---|---|---|
| `type` | 是 | 5 类之一：node/token/state/custom/debug 或终结事件 interrupt/done |
| `run_id` | token/node | 同一次 LLM 调用的 id，前端用于去重/重试召回 |
| `ts` | 是 | UnixMilli |
| `agent` | token/custom | `AgentEvent.AgentName` |
| `node_id` | node | `compose.NodeID`（ISSUE-48 嵌套格式 `outer/inner`） |
| `seq` | token | 单 run_id 内的 chunk 序号 |

### HTTP API

```
POST /api/v1/agents/:name/run
  Body: {"messages":[...], "config":{...}}
  Response: SSE stream (见上)
  Behavior: 
    1. Registry.Lookup(name) → agent.Agent
    2. NewRunSession() → session_id
    3. SessionStore.Put(session_id, sess)
    4. Attach streamHooks via RunGraph
    5. 流式返回直到 done/interrupt

POST /api/v1/sessions/:id/resume
  Body: {"messages":[...]}   // 可选：用户对 interrupt 的补充输入
  Response: SSE stream
  Behavior:
    1. SessionStore.Get(id) → sess
    2. ResumeGraph(ctx, g, checkpointer, id)
    3. 附 streamHooks 继续流

POST /api/v1/sessions/:id/interrupt
  Body: (空)
  Response: 202 Accepted
  Behavior: ctx.Cancel 对应 session 的 run goroutine
```

### 分期路线

| Phase | 内容 | 破坏性 | 开工依赖 |
|---|---|---|---|
| **P0 · 前置改造** | `AgentEvent` 加 `RunID string`；`BasicAgent.reactLoop` 每次 LLM 调用前生成 run_id 并贴到当轮 AgentEvent + `Message.Annotations["run_id"]` | 小——新字段 omitempty，旧 Checkpoint 反序列化为空串可接受 | 无。可独立 PR，不绑定前端接入 |
| **P1 · MVP** | `server/` 包 + SSE handler；`node/state/custom/debug/interrupt/done` 6 类事件；不含 token 流 | 零——纯新增包 | P0 |
| **P2 · Token 流 + Resume** | `modelListener` 装饰器转发 chunk；`/resume` 和 `/interrupt` endpoint；session 注册表持久化（可选） | 低——model 装饰器是可选注入 | P1 + ISSUE-40 interrupt payload 方案 |
| **P3 · 精细化** | tags 字段（`Message.Annotations["stream_tags"]`）；ForkLane 分支 lane_id tag；WebSocket 支持前端打断 | 中——协议扩展 | P2 稳定后 |

### 前置改造清单（P0 · 可独立落地）

```go
// agent/event.go
type AgentEvent struct {
    AgentName string `json:"agent_name"`
    RunID     string `json:"run_id,omitempty"`  // ← 新增
    RunPath   []RunStep `json:"run_path,omitempty"`
    // ... 其他字段不变
}

// agent/basic.go （BasicAgent.reactLoop 内，LLM 调用前）
runID := newUUID()  // 或 session.ID + "-" + roundNo
// 把 runID 塞到当轮产出的 AgentEvent.RunID 和 Message.Annotations["run_id"]
```

**验证**：改完不需要新增测试——现有 agent 冒烟跑完能正常，事件里多出 run_id 字段就算过。

### 关键取舍

**1. 为什么不扩展 eventSink 加订阅？**
eventSink 设计是 push/drain 的临时 buffer，加订阅会变成全局 broker——复杂度溢出。OrchHooks 本身就是订阅点，复用即可。

**2. 为什么 Token 流放 P2 不放 MVP？**
Token 流需要拦截 model.Stream chunk，adapter 要装饰 model——这是执行路径上的侵入。MVP 先让 node/state/custom 事件跑通，验证 SSE 框架稳定后再加 token。node 级事件已经足够支撑"过程面板"类展示。

**3. 为什么不做 WebSocket？**
SSE 在 Go 里 `http.ResponseWriter.Flush()` 就能实现，标准库够用。WS 需要引入 gorilla/websocket 或类似依赖，双向消息协议复杂。"前端打断" → `POST /interrupt` 单独 REST endpoint 就行，不需要双向流。

**4. 为什么 run_id 不放 schema.Message 字段里？**
Message 是 spec 生成的 type alias（CLAUDE.md 约束），不改。用 `Annotations["run_id"]` 约定即可，侵入面最小。

**5. Session 持久化选型**
P1/P2 用 `sync.Map`（InMemorySessionStore）；P3 如果要跨进程部署再加 Redis/DB 实现——定义成 interface 等扩展。

### LangGraph 深挖调研补充（2026-04-23）

跨 langgraph-bb/ 下 STREAMING/STATE/SUPERSTEP/HITL/RETRY/CHECKPOINT 等多份文档核对后，**方案需要 5 处修订**：

**修订 1 · run_id 粒度明确**
`AIMessageChunk.id` 在 LangGraph 里等于**一次 `llm.invoke()`**——节点内连续两次 LLM 调用对应**两个不同 id**。我们的实现对齐：`BasicAgent.reactLoop` 每轮生成一个 run_id，节点内多次 LLM 调用就是多个 run_id，不按节点粒度共享。文档里 [LANGGRAPH_STREAM_FRONTEND.md § 完整执行时序] 超步 0 `run-0`、超步 2 `run-1` 的示例印证这一点。

**修订 2 · `state` 事件不能照抄 LangGraph 的 values 语义**
LangGraph 的 State 是 `TypedDict` + **字段级 reducer**（`Annotated[list, add_messages]`），超步结束时按字段 merge → 推一条完整 State。我们的 `GraphState.localState` 是**整对象 `any`**，没有字段级 reducer 机制。

- **对外 SSE `state` 事件推什么**，要重新定义：
  - P1：只推 `GraphState.Messages` + `GraphState.LastOutput` + 超步序号（显式字段子集，不含 localState）
  - P3：如果将来引入字段级 reducer 设计（比如参考 LangGraph 的 `Annotated`），再扩展 state 事件载荷
- **并发 N 分支推 1 条** 这一条对齐（我们 JoinLanes 后才推），不变

**修订 3 · interrupt 事件的 payload**
LangGraph `on_interrupt` 事件 data 里**不带 checkpoint_id**，只带"用户展示内容"（`interrupt()` 函数的参数），需要前端按节点名自己推断恢复位置。我们**不抄这个简化**——SSE `interrupt` 事件必须带 `session_id`（因为我们 Resume 入口 `ResumeGraph(ctx, g, checkpointer, sessionID)` 需要 session id）。这是我们的加码，不是照搬。

LangGraph Resume API 是 `graph.invoke(Command(resume=value), config)`，resume 参数是**单值**——我们对外 API `POST /sessions/:id/resume` body 里带 `{"messages":[...], "resume_value": ...}` 两种方式兼容，不强迫前端走单值。

interrupt 子类型（approval / info_request / draft_review）在 LangGraph 里**不是框架定义的**，是应用层在 `interrupt()` 参数里自己约定 `{"type": "approval_request", ...}`——我们也保持一致，不在框架层定义子类型，留给业务代码。

**修订 4 · custom event 作用域对齐**
LangGraph `adispatch_custom_event` 是**整图广播**（不限当前节点），metadata 带 `langgraph_node` 区分来源。我们 `eventSink` 本身就是整图级别，`AgentEvent.AgentName + RunPath` 比 LangGraph 的 `langgraph_node` 字段更细——方案不需要改。

**修订 5 · 可以比 LangGraph 做得更好：显式重试/放弃事件**
LangGraph 重试时**没有**发出 `on_chain_retry` 或 `on_abandon` 事件——"旧 run_id 被放弃"完全靠前端看到新 id 自己推断。这是 LangGraph 文档里公开承认的工程粗糙点（[LANGGRAPH_STREAM_FRONTEND.md § 重试时的重复推送问题]）。

**我们可以做得更好**：
- 在 `BasicAgent` / `LoopAgent` / `RetryPolicy`（将来如果引入）重试分支显式发 `on_abandon` SSE 事件，data 里带 `abandoned_run_id`，前端直接按信号处置，不靠推断
- Checkpoint 持久化 `AgentEvent` 时可选记录 `AbandonedRunIDs []string`，让 resume 后的前端重建状态时能识别历史废弃

这是 P1 阶段就能顺手做的设计亮点，**纳入 P1 范围**。

### LangGraph 文档未覆盖、我们自己拍板的点

- **Checkpoint 是否记录被放弃的 run_id**（LangGraph 文档未讲）→ 我们决定记录（见修订 5）
- **`astream_events` v2 完整 event 枚举**（LangGraph 文档只列 7 类不全）→ 我们不追 LangGraph 全集，自己的 5 类 SSE event 足够
- **一个节点内多次 LLM 调用是否共享"节点级 trace id"**（LangGraph 无）→ 我们用 `AgentEvent.RunPath`（已有）+ `RunID`（新增）两层足够，不再加"节点级 trace id"维度

### 业务侧决策（2026-04-23 已拍）

| 决策 | 结论 |
|---|---|
| **D1. 启动时机** | 方案讨论充分即开工，不设硬门槛（不再等 ISSUE-44/45/46/47 全绿） |
| **D2. Session 持久化** | 走 Redis（InMemorySessionStore 仅作 P1 MVP 快速验证，P1 合并前替换为 RedisSessionStore） |
| **D3. 认证** | **框架不提供，由 SDK 使用者在外层 middleware 处理**。对齐 LangGraph 核心库不绑认证的定位 |
| **D4. 多租户隔离** | **框架不提供，由 SDK 使用者在外层 middleware 处理**。我们只提供 `RunSession.values` 作为身份字段承载点 |

### 框架边界（D3/D4 落地约定）

`server/` 包的对外契约：

1. **返回标准 `http.Handler`**，可挂到任意 mux（`http.ServeMux` / chi / gin / echo）
2. **认证 / 租户 / 限流 / CORS 一律由使用者 middleware 处理**，我们不内置任何 auth/rbac 代码
3. **身份承载点**：约定从 ctx 读特定 key（如 `user_id` / `tenant_id`）自动塞进 `RunSession.values`——使用者在 middleware 里 `ctx = context.WithValue(ctx, "user_id", ...)` 即可，后续 agent/节点通过 `sess.GetValue("user_id")` 取到
4. **典型接入姿态**：
   ```go
   srv := server.NewServer(registry, sessionStore)
   handler := authMiddleware(tenantMiddleware(srv.Handler()))
   http.Handle("/api/v1/", handler)
   ```

这个约定写进 `server/` 包的 godoc 和 README，SDK 使用者自决。

### 因 D2 产生的 P1 额外工作

原方案"P1 用 sync.Map、P3 再上 Redis"调整为 **P1 直接上 Redis**：

- 新增 `server.SessionStore` interface（Put/Get/Delete/TTL）
- `server.InMemorySessionStore`（开发/测试用，nonbinding）
- `server.RedisSessionStore`（生产默认，依赖 `go-redis/redis/v9` 或同等库）
- `Checkpointer` 存储也可共用同一个 Redis 实例（另一组 key 前缀）

持久化字段：`session_id → {run_session_json, last_checkpoint_id, created_at, last_active_at}`，TTL 默认 24h 可配。

### 风险 & 开放点（P1 开工前必答，和原"开放问题"合并）

- **A' Run_id 生成时机**：`BasicAgent.reactLoop` 每个 LLM 调用一个 run_id，重试时换新——**已决**
- **B' 协议选型**：只做 SSE，interrupt 走独立 REST——**已决**
- **C' Interrupt payload**：依赖 ISSUE-40 的 `any` 序列化方案，**阻塞 P2**
- **D' 事件分类粒度**：直接透出 5 类让前端过滤，对齐 LangGraph——**已决**
- **E' ForkLane 事件隔离**：单流 + `lane_id` 字段（P3 加）——**P1 单流不标 lane**
- **F' NodeID 前缀格式**：和 ISSUE-48 约定 `/` 分隔——**等 ISSUE-48 重启时同步拍板**

---

## 2026-04-24 架构对齐与 OrchestrationHooks 分层决策

### 关键否决：ISSUE-50 架构翻转不做

此前讨论过是否把 crew-go 的 `agent ← compose` 方向翻成 eino 的 `compose ← agent`（所谓"ISSUE-50 架构翻转"）。**否决**。

**LangGraph 双证据支撑当前架构方向**：

1. **pyproject 依赖方向**（`/Users/gupengfei/program/go/src/langgraph/libs/`）：
   - `langgraph-checkpoint` deps = langchain-core + ormsgpack
   - `langgraph-prebuilt` deps = checkpoint + langchain-core（不声明依赖主包）
   - `langgraph` 主包 deps 包含 `langgraph-prebuilt`
   - → 声明层面方向：`prebuilt ← 主包`，对应 `agent ← compose` ✅

2. **源码实况**（`libs/prebuilt/langgraph/prebuilt/chat_agent_executor.py:L35`）：
   - prebuilt `create_react_agent` 内部 `from langgraph.graph import StateGraph`
   - 实际运行时 prebuilt 依赖主包 StateGraph（靠命名空间包 `langgraph.prebuilt` / `langgraph.graph` 共存规避 pyproject 循环依赖声明）
   - 但**这不改变对外架构方向**——主包对用户提供的心智仍是"图引擎能编排 agent"

**eino 走相反方向（compose 在底层）是另一种选择，不是唯一正解**。LangGraph 是成熟产品，证明我们的方向可行。

**BasicAgent 不重写成图**：
- 用 for 循环做 ReAct 是成熟做法（eino `BasicAgent` 也是 for 循环）
- LangGraph prebuilt 虽是图实现，但强行包图对 Go 无收益——Go 没有 Runnable 体系，包图反而多一层调度开销
- 当前 `BasicAgent`（for 循环）+ `GraphAgent`（包图为 Agent）双轨已覆盖两种用法

### OrchestrationHooks 最终方案

**定位**：保留在 `compose` 包，**不搬到 hook 包**，不做翻译层。

对齐 LangGraph 分层：

```
hook 包：hook.Hooks             ← 对标 langchain-core.callbacks（agent 生命周期）
compose 包：OrchestrationHooks  ← 对标 langgraph 主包 callbacks.py（图级 GraphInterruptEvent / GraphResumeEvent 等）
```

两层 hook **完全独立、不合并**。理由：
- `OrchestrationHooks` 方法签名访问 `NodeID` / `GraphState` 等 compose 内部类型——放 hook 包会破坏分层（要么暴露 compose 类型到 hook，要么做轻量视图翻译，两者都增加成本无收益）
- LangGraph 主包 `callbacks.py` 自己定义图级事件，不放 `langchain-core`——这就是前例

**接口沿用现状**（`compose/orch_hooks.go:16`，零改动）：

```go
type OrchestrationHooks interface {
    OnGraphStart(ctx, *GraphState) error
    OnGraphEnd(ctx, *GraphState, error)
    OnNodeStart(ctx, NodeID, *GraphState) error
    OnNodeEnd(ctx, NodeID, *GraphState, error)
    OnNodeEvents(ctx, NodeID, []*agent.AgentEvent)
}
```

**注入路径**（新增）：

```
agent.RunOptions.OrchHooks  ──(桥接)──>  compose.GraphRunOptions.OrchHooks  ──>  RunGraphWithOpts
```

`agent.RunOptions` 新增 `OrchHooks compose.OrchestrationHooks` 字段——这是 hook 体系从 agent 层透传到 compose 层的唯一入口。

### server 包的双 hook 订阅模型

ISSUE-49 原方案只提了订阅 `OrchestrationHooks`，实际上 **server 要同时订阅两套 hook** 才能覆盖全部 5 类事件：

| SSE 事件 | 订阅源 | 触发点 |
|---|---|---|
| `node` / `state` / `debug` | `compose.OrchestrationHooks` | 图引擎节点边界 |
| `custom` | `compose.OrchestrationHooks.OnNodeEvents` | 读 `AgentEvent` 列表 |
| `interrupt` | `OrchestrationHooks.OnGraphEnd` + 检查 `InterruptError` | 图级中断 |
| **`token`** | **`hook.Hooks.OnModelEnd` / `OnModelEndStream`** | **agent 层 LLM 调用，不走 OrchHooks** |
| `on_abandon` / `on_retry` | `hook.Hooks`（`BasicAgent.reactLoop` 重试分支新增触发） | agent 层重试 |

`server.streamHooks` 实际上**同时实现 `compose.OrchestrationHooks` 和 `hook.Hooks` 两个接口**，两路事件源合并到一条 SSE 流。

### 9 个编排模式的透传策略

**不改每个模式的接口**——所有编排 agent 最终都编译为 Graph 走 `RunGraphWithOpts`。只需保证：

```
XxxAgent.Run(ctx, opts agent.RunOptions)
   → 桥接成 compose.GraphRunOptions
   → RunGraphWithOpts(ctx, g, state, graphOpts)
```

**开工时逐一核对透传**的模式：
- `SequentialAgent` / `ParallelAgent` / `LoopAgent` / `RoutingAgent` / `HandoffAgent` / `PlanAgent` / `ReflectAgent` / `GraphAgent` / `AgentTool`

`BasicAgent` 对 `OrchestrationHooks` 不可见（它不走 compose 图引擎）——用户要监控 BasicAgent 内部 ReAct 循环只能通过 `hook.Hooks`。如果 BasicAgent 作为 compose 图节点嵌入（`AddAgentNode`），它的事件通过 `state.Send` 写进 `eventSink`，在节点边界被 `OnNodeEvents` 回调冒泡出去。

### 顺便回答：编排 agent 如何 chat？

此前疑问"SequentialAgent / ParallelAgent 这种怎么加入 chat"——**和 BasicAgent 完全一样**：

- 所有编排 agent 都实现 `agent.Agent` 接口（`Run(ctx, session) ([]AgentEvent, error)`）
- `server.Registry` 注册 agent 时不区分具体类型
- `POST /api/v1/agents/:name/run` 对 BasicAgent、SequentialAgent、PlanAgent 一视同仁

对标 LangGraph：`create_react_agent` 返回的 `CompiledStateGraph` 可以直接 `.invoke(...)` chat；任何用户自定义的 `StateGraph.compile()` 也能 `.invoke(...)` chat——同一机制。我们"所有 agent 实现同一个 `Agent` 接口"的设计已经天然覆盖这个用法。

### 工作量修订（P0 前置改造）

此前 "A' 方案"估算 ~200 行，去掉 hook 包搬迁 + 翻译层后**降到 ~85 行**：

| 改动 | 行数 |
|---|---|
| `agent.RunOptions` 新增 `OrchHooks` 字段 + 桥接到 `compose.GraphRunOptions` | ~20 |
| 9 个编排模式检查/补齐 `RunOptions` 透传 | ~50 |
| `AgentEvent.RunID` 字段 + `BasicAgent.reactLoop` 生成 run_id | ~15 |
| **合计** | **~85** |

**不需要改**：
- `compose.OrchestrationHooks` 接口（完整）
- `hook.Hooks` 接口（完整，含 `OnModelEndStream`）
- **任何轻量视图类型**（`NodeInfo` / `StateSnapshot` / `AgentEventView`——A' 方案这项彻底删除）

这 85 行就是 P0 前置改造的完整范围，随时可开工。

---

## 2026-04-24 · P1 协议选型修订：对齐 AG-UI（不自定义）

### 背景与决策

原方案（上文"实施方案"节）定的是"自定义 5 类 SSE 事件（node/token/state/custom/debug）"。**修订为"对齐 AG-UI（Agent–User Interaction Protocol）"**。

**触发决策的观察**：

- SDK 使用者的业务前端和 demo 前端**大概率不是自己写的**——会用 CopilotKit / Vercel AI SDK / LangChain.js 之类的现成组件库
- 自定义协议 = 把适配层成本转嫁给每一个 SDK 使用者（等同于 MCP 出现前每家工具协议不通的状态）
- AG-UI 是这个空白位的**事实标准**：CopilotKit 发起，LangGraph / CrewAI 加入，开放协议，多语言 SDK

**结论**：ISSUE-49 P1 的对外 streaming 协议采用 AG-UI；自定义事件协议降级为"调试 / 内部 demo 专用"。

### AG-UI 简要

- 全称 Agent–User Interaction Protocol（[docs.ag-ui.com](https://docs.ag-ui.com)）
- 传输层：HTTP + SSE / WebSocket
- 应用层：固定事件 schema，包括 `TEXT_MESSAGE_CONTENT` / `TOOL_CALL_START/ARGS/END` / `STATE_SNAPSHOT` / `STATE_DELTA` / `RUN_STARTED/FINISHED` / `CUSTOM` / `RAW` / 人机中断事件等
- 前端生态：React / Vue / Svelte 组件库可直接消费，零适配成本

### crew-go → AG-UI 事件映射（采用"降级映射"策略 · 方案 1）

**核心取舍**：crew-go 编排概念（Graph node / Plan replan diff / ReflectAgent eval 循环 / ForkLane）在 AG-UI 原生事件里没有直接位置。两种映射方案：

1. **降级映射**：把编排概念折叠到 AG-UI 标准事件，精细化信息丢弃或压缩（**本次采纳**）
2. **Custom 扩展**：走 AG-UI `CUSTOM` / `RAW` 事件塞编排细节（**不采纳**，理由见下）

**不采纳方案 2 的理由**：大多数业务前端只关心"agent 说了啥 / 调了哪个工具 / 最终答复"——这些 AG-UI 原生事件完全够用。塞 custom event 意味着前端要针对 crew-go 专写 handler，违背"复用生态组件"的初衷。精细化场景仍可用 `plan_chat_web` 的自定义事件流做内部调试，不走 AG-UI。

**映射表**：

| crew-go 事件源 | AG-UI 事件 | 降级说明 |
|---|---|---|
| `hook.OnModelEnd` 最终 assistant 文本 | `TEXT_MESSAGE_CONTENT` + `TEXT_MESSAGE_END` | 直接输出；`run_id` 填 AG-UI `message_id` |
| `hook.OnModelEndStream` token chunk | `TEXT_MESSAGE_CONTENT`（增量） | 流式对齐 |
| `hook.OnToolStart` | `TOOL_CALL_START` + `TOOL_CALL_ARGS` | 原生对应 |
| `hook.OnToolEnd` | `TOOL_CALL_END` + `TOOL_CALL_RESULT` | 原生对应 |
| `OrchHooks.OnGraphStart/End` | `RUN_STARTED` / `RUN_FINISHED` | 图 = 一次 run |
| `OrchHooks.OnNodeStart/End` | **丢弃**（不映射） | AG-UI 无"节点"概念；前端不需要知道 Graph 内部节点边界 |
| `PlanAgent` 的 step_dispatch | `TOOL_CALL_START`（agent 名当 tool 名） | 把 sub-agent 调度降级为 tool_call——符合 AG-UI 的 agent-as-tool 心智模型 |
| `PlanAgent` replan diff | **丢弃** | 前端看到新的 tool_call 序列即可推断；diff 细节只走自定义 debug 流 |
| `ReflectAgent` 的 actor/critic 循环 | `TOOL_CALL_START`（多次） | 每轮 actor/critic 当一次 tool_call |
| `HandoffAgent` transfer | `TOOL_CALL_START`（transfer_to_xxx） | 对齐 OpenAI Agents SDK 的 handoff-as-tool |
| `InterruptError` + Checkpoint | AG-UI 中断事件（待调研具体 event 名） | 需要验证 AG-UI 如何表达 HITL |
| `AgentEvent.RunID` | `message_id` / `step_id` | 复用 AG-UI 原生 id 字段，不再自定义 run_id 语义 |
| `GraphState.LastOutput` / `Messages` 超步快照 | `STATE_SNAPSHOT` | 超步边界推，对齐 AG-UI state 同步 |
| `ForkLane` 分支 | 暂**丢弃**（P1 单流） | lane_id 若要暴露走 AG-UI custom event，P3 再议 |

### 前置改造调整

ISSUE-49 P0 已落地的部分（commit 4ceaa10）**完全可复用**——run_id + OrchHooks 穿透是任何上层协议的共同底座：

- `AgentEvent.RunID` → 填 AG-UI `message_id`
- `hook.Hooks` 订阅 → 驱动 AG-UI `TEXT_MESSAGE_*` / `TOOL_CALL_*` 事件
- `OrchestrationHooks` 订阅 → 驱动 `RUN_STARTED/FINISHED`

原 P0 工作未浪费。

### P1 新范围

原 P1 的"自定义 SSE 协议 + `server/` 包"调整为：

```
streaming/agui/                  (新增独立包，替代原 server/)
├── types.go                     AG-UI 事件 schema（参考 ag-ui 规范，Go struct）
├── encoder.go                   SSE 编码：AG-UI event → SSE line
├── hooks.go                     实现 hook.Hooks + compose.OrchestrationHooks，
│                                转成 AG-UI 事件推到 channel
├── handler.go                   http.Handler：/agui 端点，SSE 流
├── session.go                   AG-UI thread_id ↔ crew-go RunSession 映射
└── doc.go                       使用说明
```

**对外姿态**（SDK 使用者视角）：

```go
ag := compose.NewPlanAgent(...)
http.Handle("/agui", agui.NewHandler(ag))  // 一行接入
```

前端用 CopilotKit `<CopilotChat agent-url="/agui" />` 或任何 AG-UI 兼容组件，零适配。

### 开放问题（P1 开工前必答，在原 A'–F' 基础上更新）

**G' · AG-UI 规范版本锚定**
AG-UI 是活跃演进的协议。P1 开工前需选定一个版本号作为实现基线（可能 v0.x），升级策略在 doc 里写清楚。

**H' · HITL / interrupt 在 AG-UI 里的表达**
AG-UI 文档里 human-in-the-loop 的 event 形状需要仔细调研。我们的 `InterruptError + Checkpoint` 和 AG-UI 的 HITL 事件可能有 gap，P1 开工前要核对映射。

**I' · Plan replan diff / node 事件是否保留 debug 通道**
决策：AG-UI 主流只推标准事件；debug 细节单独走 `/debug` 端点（自定义事件，即现有 `plan_chat_web` 风格），给开发者看内部执行细节。不混入 AG-UI 流。

**J' · Custom / RAW event 的使用边界**
方案 1 降级映射已避免主通道用 custom，但某些非标信号（比如 `hook.OnAgentInit` / checkpoint saved）确实没法降级——是彻底丢弃还是走 AG-UI `CUSTOM`？**倾向丢弃**（AG-UI 不感知这些）。

### plan_chat_web 的重新定位

- **保留**，但角色降级为"**内部调试 demo**"
- 展示 crew-go 内部视角：graph node / plan replan diff / run_id UUID / ForkLane lane_id
- 不作为 SDK 使用者的对外参考
- 未来建 `example/agui_chat/` 作为 AG-UI 对接 demo，用标准 AG-UI 前端组件库接入——那个才是给 SDK 使用者看的"如何接入前端"参考

### 不做的事

- **不再做自定义 `node/state/custom/debug` 五类 SSE 事件协议**——原"实施方案"节的这部分废弃
- **不做 AG-UI 的 Go 生成代码 / spec 同步工具**——AG-UI 事件 schema 按规范手写 Go struct，避免引入 codegen 复杂度
- **不在框架层定义 interrupt 子类型 / 认证 / 多租户**——AG-UI 规范本身不管这些，SDK 使用者用 middleware 处理（对齐原 D3/D4 决策）

### 当前状态

**不开工**。本节仅作 P1 方向记录。重启时机沿用原"当前决定"节的触发条件。

---

## 2026-07-01 决策修订：做 AG-UI 适配包（推翻 06-15 的"不做"）

领导提出接入 AG-UI（Agent-User Interaction Protocol）。调研后**推翻 06-15"不做框架级
SSE 抽象"的结论**——两者定位不同：06-15 反对的是"自己发明 SSE 事件 schema"，AG-UI 是
"对齐外部标准协议 + 复用现成前端组件生态"，不是自定义。

### 为什么做（第一性理由）

- **复用现成前端**：接 AG-UI 后可直接用生态里的现成前端组件（CopilotKit React 的
  `<CopilotChat>` 等，另有 Vue / Angular 集成），业务不必为 crew-go 专写前端。
- **对齐前端交互标准**：AG-UI 是 agent→前端的事件协议（33 个协议事件类型：
  `TextMessage*` / `ToolCall*` / `RunStarted` / `RunFinished` / `StateSnapshot` /
  `Step*` / `Reasoning*` 等），一个前端可消费不同后端。
- **补 Go 生态空白**：AG-UI 官方主力是 TypeScript / Python，Go 仅 community SDK；据本次
  调研 eino 未接、AG-UI 官方也无 Go 框架侧 adapter——crew-go 做会靠前。

> 生态成熟度佐证（非"从众"理由）：AG-UI 已被 LangGraph / CrewAI / LlamaIndex / Mastra /
> Agno / Pydantic AI / Microsoft Agent Framework 等接入，**均走适配层、非核心内置**——
> 印证这个标准值得对齐，且"核心不管 SSE、适配层接协议"与我们 06-15 的分层判断一致。

### 方案：独立适配包，不侵入核心

参照 `a2a/` 的独立包形态（core 仍不管 SSE，适配是可选集成层）：

1. **协议层——内联精简，不硬依赖官方 Go SDK。** 官方 community Go SDK
   （`github.com/ag-ui-protocol/ag-ui/sdks/community/go`）核心约 8200 行，但：无版本 tag
   （只能拉 monorepo commit）、依赖 `logrus`（crew-go 用 `slog`，日志库不一致）、单人
   community 维护。我们 server 端只用约 10/33 事件（`TextMessage*` / `ToolCall*` /
   `RunStarted` / `RunFinished` / `StateSnapshot` / `StepStarted`）。把这些事件 struct
   （每个 2-4 个扁平字段）+ 一个标准库 `net/http` 的 SSE writer 内联进 crew-go 的 `agui`
   包（约 500-800 行，零重依赖）——同 `modules/` 拷 spec 的套路。协议演进只需跟几个 struct。

2. **映射层——crew-go 事件源 → AG-UI 事件（约 300 行，细节待实现时定）。**

   | crew-go 事件源 | AG-UI 事件（方案） |
   |---|---|
   | `OnModelEnd` 聚合 / chunk / `AgentEvent` | `TextMessageStart/Content/End` |
   | `OnToolStart` / `OnToolEnd`（tool_call） | `ToolCallStart/Args/End/Result` |
   | Run 开始 / 结束 | `RunStarted` / `RunFinished` |
   | `InterruptError`（HITL） | AG-UI 侧用 Custom / state 表达（实现时定） |
   | 超步 state 快照 | `StateSnapshot` |
   | `ForkLane` / Plan replan / 嵌套 ns | 降级 metadata 或丢弃（AG-UI 无原生对应） |

3. **HTTP SSE handler**：挂 `EventSink` / `OrchHooks.OnNodeEvent` → AG-UI SSE 流。

### 一致性核对（2026-07-01，本地 clone 全 monorepo 核实）

拉了整个 ag-ui monorepo 到 `/Users/gupengfei/program/go/src/ag-ui`
（commit `63e0351`，2026-07-01 最新）：

- **事件类型：Go community SDK 的 33 个协议事件 == 官方 TypeScript，零落后**（Go 多的
  `UNKNOWN` 是解析哨兵，非协议事件）。
- **字段级抽查（`ToolCallResult` / `RunFinished`）一致**（字段级未逐个全核，抽查一致 +
  同 monorepo，漂移概率低）。
- **单人 community 却零落后的根因**：Go SDK 与官方 TS / proto **在同一 monorepo 同步演进**
  + CODEOWNERS，不是独立 repo 各自漂移——故内联时照 Go SDK 扒 struct 安全。

### 工作量与风险

- 工作量：约 1.5-2 周（内联协议层 + 映射层 + HTTP handler + demo + 测试）。
- 风险：① AG-UI 仍在演进（有 deprecated，如 Thinking→Reasoning）——但薄协议跟版本便宜；
  ② 编排特有语义（`ForkLane` / Plan replan / 嵌套 ns）AG-UI 无原生对应，降级 metadata。
- 与 `plan_chat_web` 关系：**不替代**——那是自定义 SSE 参考实现（内部调试 / 精细场景保留），
  AG-UI 适配是"对外标准协议"通道，两条并存。
- 本地源码 `/Users/gupengfei/program/go/src/ag-ui` 常驻，以后扒字段 / 对 proto 直接读。

---

## 相关

- 参考文档：[LANGGRAPH_STREAM_FRONTEND.md](/Users/gupengfei/program/go/src/langgraph-bb/LANGGRAPH_STREAM_FRONTEND.md)
- eino 源码：`/Users/gupengfei/program/go/src/eino`（`adk/` 和 `callbacks/` 两个包为主要对照点）
- ISSUE-40：AgentEvent 持久化约定（影响 interrupt/resume 协议里的 payload 序列化）
- ISSUE-41：ForkLane / JoinLanes（影响开放问题 E，多分支事件隔离）
- ISSUE-46：超步部分失败恢复（影响 debug 通道的超步事件模型）
- ISSUE-48：子图事件透传（影响开放问题 F，嵌套 NodeID 格式契约）
