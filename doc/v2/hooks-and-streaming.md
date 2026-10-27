# hooks-and-streaming.md — 两层 hook 模型 + streaming chunk 实时投递

> **业务侧深入版** — `channel-engine.md §10` 是引擎侧速览; 本文档展开 8 callback 时机表 / ctx-aware 透传 / chunk 投递协议 / panic 隔离 / 假绿陷阱.

## 1. 两层 hook 模型

crew-go 有**两套独立 hook 接口**, 服务不同抽象层:

| 层 | 接口 | 包 | 时机 | 业务用 |
|---|---|---|---|---|
| **业务级 Agent hooks** | `hook.Hooks` (8 callback) | `crew-go/hook` | Agent.Run 生命周期 (Start/End/Model/Tool/Error) | 观测/审计/billing — "我的 Agent 跑到哪了" |
| **引擎级 Orch hooks** | `compose_v2.OrchestrationHooks` (5 callback) | `crew-go/compose_v2` | Graph 节点级 (GraphStart/End/NodeStart/End/NodeEvent) | streaming chunk / 节点 debug — "图引擎在做什么" |

**两套共存设计**: 业务 hook 看"Agent 抽象层" (model/tool/round), 引擎 hook 看"channel 图层" (node/edge/event). `BasicAgent.Run` 内部把两套都 fire — 业务可只用一套或都用.

## 2. 业务级 Agent hooks (`hook.Hooks` 8 callback)

```go
type Hooks interface {
    OnAgentStart(ctx, *AgentInfo, *AgentInput) (context.Context, error)
    OnAgentEnd(ctx, *AgentInfo, *AgentOutput) context.Context
    OnModelStart(ctx, *AgentInfo, *schema.ModelInput) (context.Context, error)
    OnModelEnd(ctx, *AgentInfo, *schema.ModelOutput) (context.Context, error)
    OnModelEndStream(ctx, *AgentInfo, specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error)
    OnToolStart(ctx, *AgentInfo, *schema.ToolInput) (context.Context, error)
    OnToolEnd(ctx, *AgentInfo, *schema.ToolOutput) (context.Context, error)
    OnError(ctx, *AgentInfo, *schema.CommError) context.Context
}
```

### 时机表 (BasicAgent ReAct 路径)

| Callback | 触发频次 | 节点 | 返 error 效果 |
|---|---|---|---|
| `OnAgentStart` | Run 同步 **1 次** + 每次 Resume 同步 **1 次** | agent_v2.Run/Resume top-level (图执行之前, 在 preamble / modelNode 之前; **次数**与 OnAgentEnd 对称——但 OnAgentStart 同步先于图执行、OnAgentEnd 是 defer) | 中断 Run |
| `OnModelStart` | 每轮 round | 紧贴 LLM 调用前 (Stream + Generate 都触发) | 中断本轮 |
| `OnModelEnd` | 每轮 round | LLM 返回后 tool-call 解析前 (Stream 路径聚合后调) | 中断本轮 |
| `OnModelEndStream` | 每轮 round (Stream 路径) | `streamOrGenerate` 调 — hook **可 wrap reader 观测每个 chunk + final 仍正确聚合** | 中断本轮（返回 nil reader 同样算错；不改调 Generate；返回的 reader 只有交出 io.EOF 才算流正常结束，关掉底层流之后读到的 `schema.ErrStreamCanceled` 也算失败）|
| `OnToolStart` | per tool_call | tools 节点入口 **串行** 触发 (按 toolCalls idx 序, `agent_v2/basic_builder.go` `OnToolStart` 处) | 中断本轮 |
| `OnToolEnd` | per tool_call | tools 节点 **串行** 触发 (`agent_v2/basic_builder.go` `OnToolEnd` 处) | 中断本轮 |
| `OnAgentEnd` | Run defer **1 次** + 每次 Resume defer **1 次** (ISSUE-82 fix 后单一所有权) | Run/Resume 入口 defer | 不中断 |
| `OnError` | 节点错误时 | 节点抛非 Interrupt err | 不中断 |

**关键不变量**:
> **可写性按字段分两档**（[消费]=改写生效 / [只读]=改写不生效）。OnToolStart/End 改写消费已于 commit `482eabe` 接通，无第三档。权威表见 [hook/hooks.go](../../hook/hooks.go) Hooks 接口 godoc + CLAUDE.md「Hook 消费语义」。

- `OnAgentStart` 入参 `AgentInput.Messages` **[消费]** — 一次调用的原始输入（Resume 时为空），替换/预处理输入，runtime 喂改写后的 messages
- `OnModelStart` 入参 `ModelInput.Messages` **[消费]** — runtime 喂改写后 messages 调 LLM；`Tools` **[只读]** — 框架当前不填充（恒 nil），动态切 tool 集不走 hook（对齐 eino observation-only）
- `OnModelEnd` 入参 `ModelOutput.Message` **[消费]** — 替换 LLM 响应；`Usage` **[只读]** — token 统计事实值，改写不生效
- `OnModelEndStream` 用**返回值 `(context.Context, MessageStreamReader, error)`** wrap 原 reader **[消费]** — 框架继续消费聚合 final (hook 可观测/转换/审计每个 chunk)。要在半路截断、把已有内容当完整输出，就在自己的 Recv 里返回 io.EOF；关掉底层流再把读到的 `ErrStreamCanceled` 原样交出，这次模型调用按失败处理（默认可重试，想不重试就返回实现了 `compose_v2.NonRetryable` 的错误）
- `OnToolStart` 入参 `Name` / `ArgumentsJSON` **[消费]** — 改写后喂业务工具调用（参数注入 / 工具重定向），只在这一批调用第一次交给工具执行时生效，续跑沿用那一次定下的内容（随中断档落盘；每次都要新取的值走钩子返回的 ctx 或工具中间件）；`CallId` **[只读]** — 配对锚点，改它破坏 OpenAI 422
- `OnToolEnd` 入参 `Result.*` **[消费]** — 改写后写回 LLM 历史（结果脱敏 / 注入缓存），in-place 改 content 保留 message Annotations marker；`CallId` **[只读]** — 配对锚点
- `OnAgentEnd` 入参 `AgentOutput.Messages` **[只读]** — defer 触发时 Run/Resume 已返回 events，改写不生效
- `OnError` 入参 `*schema.CommError` **[只读]** — 框架返回原始 error，改 `err.Code / err.Message` 不生效（仅快照给 hook 观测）
- **OnToolStart/End 不并发触发** — 业务 hook 实现 **无需** thread-safe 同步原语. 真并发只在内部 `dispatchToolkit.Call` (ISSUE-81 N>=2 tc fanout); business tool 实现需 reentrant. 业务 hook 跨 tc 共享状态只在 toolsNode 单 goroutine 串行写, 不需 mutex

> **⚠️ 改写消息必须保留 `Annotations`**（R22 audit MED-1）：在 [消费] 档改写 message 时（`OnAgentStart` / `OnModelStart` 的 `Messages`、`OnModelEnd` 的 `Message`、`OnToolEnd` 的 `Result`），**不要**新建 `&schema.Message{...}` 丢掉原 `Annotations`——里面有 framework 内部 marker（`SkillBodyAnnotation` / `StoreInjectedAnnotation` / 4 层压缩 marker 等），丢了会破坏跨层防护（压缩误删 skill body、StoreInjected 内容被重复注入等）。正确范式是 **shallow clone 保留 `Annotations` 指向同一 map**：
>
> ```go
> newM := *m                        // 浅拷贝整个 struct（Annotations 字段指向同一 map）
> newM.Content = redact(m.Content)  // 只改你要改的字段
> messages[i] = &newM
> ```
>
> 若确需改 `Annotations` 本身，先 `maps.Clone(m.Annotations)` 再改副本——别原地写共享 map（其他层还在读它）。

### 用法

```go
type myHooks struct {
    hook.NoOpHooks  // 嵌入默认空实现, 只 override 关心的
}

func (m *myHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
    slog.InfoContext(ctx, "model done",
        "agent", info.Name, "round", info.Round,
        "tool_calls", len(out.Message.ToolCalls()))
    return ctx, nil
}

agent := agent_v2.NewBasicAgent(
    agent_v2.WithName("research"),
    agent_v2.WithModel(m),
    agent_v2.WithHooks(&myHooks{}),
)
```

## 3. 引擎级 OrchestrationHooks (`compose_v2.OrchestrationHooks` 5 callback)

```go
type OrchestrationHooks interface {
    OnGraphStart(ctx, chs Channels) (context.Context, error)
    OnGraphEnd(ctx, chs Channels, err error) context.Context
    OnNodeStart(ctx, id NodeID, chs Channels) (context.Context, error)
    OnNodeEnd(ctx, id NodeID, chs Channels, nodeErr error) context.Context
    OnNodeEvent(ctx, id NodeID, ev Event) context.Context
}
```

| Callback | 时机 | 返 error 效果 |
|---|---|---|
| `OnGraphStart` | graph 开始执行前一次 | graph 终止 (`OnGraphEnd` 仍调) |
| `OnGraphEnd` | graph 完成后一次 (success/fail) | 不中断 (监听语义) |
| `OnNodeStart` | 节点 `NodeFunc` 调用前 (**ParallelAgent 分支 / SendFrom 同超步 fan-out 等真并发场景多 goroutine 触发**, 单 Sequential 路径串行) | 节点跳过执行 (`OnNodeEnd` 仍配对) |
| `OnNodeEnd` | 节点退出 (success/fail) | 不中断 (监听语义) |
| `OnNodeEvent` | 节点内 `EventSink.Send(Event)` 触发 | 不中断 (实时投递) |

**线程安全要求**: `OnNodeStart / OnNodeEnd / OnNodeEvent` 在 fan-out 多 goroutine 并发调用 — hook 实现须 atomic / sync 保护 (典型: 用 `sync.Mutex` 保护 slice append, 或 `chan` queue 串行化).

**与业务 hook 对比**:
- 业务 hook 不知节点拓扑 (只见 "model/tool")
- 引擎 hook 不知 ReAct 轮 (只见 node ID + frontier)

## 4. ctx-aware OrchHooks 跨子 agent 透传

业务嵌套场景 (BasicAgent 内嵌 AgentAsTool → 子 BasicAgent) 需要子 agent 也 fire 同一 OrchHooks (streaming chunk 跨子图边界不丢):

```go
ctx = agent_v2.WithCtxOrchHooks(ctx, sseAdapter)
// 嵌套 sub-agent.Run 自动从 ctx 取 hooks 继续 fire
events, err := topAgent.Run(ctx, sess, input)
```

**协议**:
- **RunOption 显式注入**: `agent_v2.WithOrchHooks(h)` — 仅当次 Run 注入
- **ctx-aware 透传**: `agent_v2.WithCtxOrchHooks(ctx, h)` — 沿 ctx 链给所有下游 sub-agent.Run
- **优先级**: RunOption > ctx > nil

**两套 ctx-aware**:
- `agent_v2.WithCtxOrchHooks / GetCtxOrchHooks` — agent_v2.OrchestrationHooks 类型 (高层)
- `compose_v2.WithOrchHooks / GetOrchHooks` — compose_v2.OrchestrationHooks 类型 (底层, agent_v2 wrapper 转换后注入)

实际业务用 `agent_v2.WithCtxOrchHooks` 即可, 高层 wrapper 自动转底层.

**子 agent 想叠加额外 hook** (父 ctx 已 push base hook + 子分支想加专属 hook): ctx **不能覆盖** RunOption (优先级 RunOption > ctx). 须显式 chain 再当 RunOption 传:

```go
ctxHooks := agent_v2.GetCtxOrchHooks(ctx)
merged := agent_v2.NewChainOrchHooks(ctxHooks, mySubHook)  // 父 hook + 子专属
subAgent.Run(ctx, sess, in, agent_v2.WithOrchHooks(merged))
```

## 5. EventSink — 节点内主动发 event

```go
type EventSink interface {
    Send(ev Event)
}

type Event struct {
    Type     string  // "chunk" / "reset_stream" / 业务自定义
    Content  string  // payload
    Metadata map[string]any  // run_id / 业务字段
}
```

节点内调:

```go
func myNode(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    sink := compose_v2.GetEventSink(ctx)
    sink.Send(compose_v2.Event{Type: "chunk", Content: "hello"})
    sink.Send(compose_v2.Event{Type: "chunk", Content: " world"})
    return nil, nil
}
```

**关键**: `Event` 通过 `OnNodeEvent` 实时投递给 OrchHooks — **不写 `session.Events`** (避免 chunk 污染审计流). 业务 SSE 前端订阅 OrchHooks.OnNodeEvent 实时 push.

> **框架自动注入 `graph_ns`** (`compose_v2/hooks.go::nodeEventSink.Send`): 节点 `Send` 的每个 Event, framework 在投递前**浅拷贝 `ev.Metadata` 并注入 `graph_ns`** (= 当前 graphNS, 业务自填 `graph_ns` 时不覆盖). 所以 OrchHooks.OnNodeEvent 收到的 Metadata 永远比业务 `Send` 时多一个 `graph_ns` key (类比 LangGraph `stream(subgraphs=True)` 的 namespace tuple, 前端可按 ns 分通道展示父子嵌套). 下文及各 doc 写 chunk/reset_stream 的 `Metadata: {run_id, agent, ...}` 都是**业务 Send 时的源 map**, 实际下游还带 `graph_ns`.

**Send 是 fire-and-forget** (`compose_v2/hooks.go::nodeEventSink.Send`): nil sink / nil hooks 早返; OnNodeEvent 只作观测: 业务 OrchHook impl 在这里 panic 时 Send 就地兜住、带栈记一条 ERROR, Send 照常返回、节点照常往下跑 (转成节点错误的话失败档被续跑或重试捡起, Send 之前做过的副作用重放, 实测). 框架自己发的 cp 事件 (cp.load / cp.save / cp.cleanup, 见 `deliverCpEvent`) 同样只作观测, 钩子 panic 不进错误.

## 6. Streaming chunk 实时投递协议

```
LLM Stream 接口             Hook 链                       前端 SSE
─────────────────         ─────────                  ─────────────
chatModel.Stream(...)        ↓
  → MessageStreamReader      ↓ OnModelEndStream wrap
                             ↓ (业务 hook 返新 reader)
                             ↓
streamOrGenerate 消费         ↓
  for chunk := reader.Recv() ↓
    sink.Send(Event{          ↓
      Type: "chunk",          ↓ → OnNodeEvent
      Content: chunk.Content  ↓   (业务 OrchHook impl
      Metadata: {run_id,      ↓    write SSE channel)  → 前端实时 push
                agent}})       ↓
    聚合 chunk 到 final       ↓
                             ↓
OnModelEnd (final 完整)       ↓ (final 走 session.Events 写审计)
```

**两层并存**:
- **chunk 流** — `EventSink.Send(Event{Type:"chunk"})` → `OnNodeEvent` → 业务 SSE — **不写 session**
- **final 流** — `OnModelEnd` 拿到聚合完整 message → 业务 hook 写审计/billing — **进 session.Events**

业务**不应**自己拼 chunk 算 final — 框架自动聚合.

## 7. mid-stream Interrupt + reset_stream + run_id (R10-A)

业务场景: streaming 中途业务工具抛 `InterruptError` (HITL) → 已 emit 半流 chunk 应让前端丢弃, Resume 新流不污染:

```go
// 框架 emit reset_stream 的 4 处（撤回只在失败点 / 作废点发；压缩重试不再单独发）:
// 1. 流读出错（agent_v2/basic_agent.go streamOrGenerate, reason=stream_recv_error）：交出过片段才发
//    Metadata: chunks_emitted / is_interrupt
// 2. 一份答复作废、续跑重新生成（OnModelEnd 钩子报错 / 置空 / panic；agent_v2/basic_builder.go, reason=answer_discarded）
// 3. 编排成员或节点（WithNodeRetry）的一次尝试被取代、或同一个节点失败 / 中断 / 被否决之后再执行（compose_v2/stream_attempt.go,
//    reason=attempt_discarded）：那次交出、
//    之后的尝试没再采用的每个 run_id 各一个——取代它的尝试也失败了时留到下一次尝试再看（它可能在拿回已完成的
//    嵌套调用之前就失败了），到有一次尝试成功或重试用完时才撤；第 2 种已经撤过的不再撤
// 4. OnModelEnd 钩子改写了答复（agent_v2/basic_builder.go, reason=hook_overrode_message）——是替换不是作废：
//    Metadata: new_len / replacement（新答复的正文）
sink.Send(compose_v2.Event{
    Type: "reset_stream",
    Metadata: map[string]any{
        "run_id":         runID,
        "agent":          agentName,
        "reason":         "...",  // 见上 3 路径
    },
})
```

**业务踩点**: 用 `OnModelEnd` 改写 final.Content 重写 LLM 输出时框架 emit reset_stream 让前端清半流 chunk; 业务实现 OnModelEnd override 前需知前端会收 reset_stream 信号.

**前端协议**:
- 监听 `Type: "reset_stream"` Event
- 按 `Metadata.run_id` 精确清空当前流缓冲；带 `Metadata.replacement` 时把那条消息换成它（不是删掉）
- 接新 chunks (Resume 重发的)

agui 与 a2a 流桥已经按这套协议处理（agui 发 `MessagesSnapshot`、a2a 一次模型调用一个 artifact、用 `append=false` 替换），业务前端只在
自己消费 `OnNodeEvent` 时要照做。

防"按 session 清"导致**跨 Run 串台** — 同 sess 多 Run 可能共存 (LoopAgent body 多轮).

详见 `agent_v2/basic_agent.go::streamOrGenerate` + `agent_v2/reset_stream_test.go` + `agent_v2/repro_round10_test.go` 实际触发.

## 8. ChainHooks / ChainOrchHooks panic 语义 (audit² 改: 跟 eino 对齐 fail-fast)

业务装 N 个 hook (audit + billing + metrics) 时:

```go
hooks := hook.NewChainHooks(observer, billing, metrics)
agent_v2.WithHooks(hooks)
```

**panic 语义** (`hook.ChainHooks` / `compose_v2.NewChainOrchHooks`):
- 单 hook panic = 业务 bug **fail-fast 中断剩余 chain handler** (audit² 删 per-handler
  isolation — 跟 eino `internal/callbacks/inject.go` 同款哲学: 业务 hook panic 应业务
  自己修, framework 不 silent isolate)
- panic 冒泡到调用这条链的地方兜住, 不让 Run goroutine crash 进程。多数转 **wrapped err 返业务**;
  跑在工作之后、只作观测的那几处 (节点体成功之后的 OnNodeEnd、图成功之后的 OnGraphEnd、任何时候的
  OnNodeEvent、任何时候的 OnAgentEnd——调用失败时照旧交出原来那个错误) 就地兜住、带栈记一条 ERROR, 结果照常提交 (逐道分工见
  `compose_v2/panic_recovery.go`)。转成错误的那几层:
  1. `compose_v2.Run` 入口 outer recover (`graph_loop.go::Run`)
  2. `runNodeWithHooks` 整函数 defer recover (覆盖 fan-out 跨 goroutine)
  3. `agent_v2.BasicAgent.Run` + 其他 8 编排 Run/Resume 各 outer recover (`agent_v2/*.go`)
  4. 嵌套 AgentAsTool 子 agent.Run 自带 outer recover 间接保护父 ToolsFn 路径

**ctx propagation 协议** (audit² 加): chain 内 hook A 返的 ctx 给 hook B 用 — 业务在
hook A `ctx = context.WithValue(ctx, spanKey{}, span)` 让 hook B `ctx.Value(spanKey{})`
拿到 span. 这是跟 eino `Handler.OnStart(ctx) → ctx` 对齐的 OTel pattern.

**typed nil 元素自动过滤** (audit⁷ 协议七复证):
- `NewChainHooks(nil, real, typedNilHook)` — 元素 nil / typed nil 静默过滤, 剩 `[real]`
- 业务条件性传 hook 安全 (`var h *MyHook; if cond { h = real }; NewChainHooks(h, ...)`)

## 9. RecordingHooks test helper

测试用 — 捕全 hook event 序列让断言简单:

```go
rec := &compose_v2.RecordingHooks{}
agent.Run(ctx, sess, input, agent_v2.WithOrchHooks(rec))

for _, ev := range rec.SnapshotEvents() {
    fmt.Println(ev.Type, ev.Content, ev.Metadata)
}
starts := rec.SnapshotNodeStarts()  // []NodeID
ends := rec.SnapshotNodeEnds()      // []NodeID
graphStart, graphEnd := rec.SnapshotGraphLifecycle()  // (int, int) 次数
```

`RecordingHooks` 内部 `events []recordedEvent` (私有) — 通过 `Snapshot*` 方法读 (内部 `sync.Mutex` 写锁, **不是 RWMutex** — Snapshot* 并发跑会被写阶段的 OnNodeStart/End 阻塞; 测试串行场景无影响, 并发跑测试 + 后台 Snapshot 时需注意). 业务集成测试断"框架真 emit 期望 event 序列"用此 helper 比手写 mock hooks 简洁.

## 10. 反模式

### ❌ 在 OrchHooks 内调阻塞 IO 不带 ctx

```go
func (h *myHook) OnNodeEvent(ctx context.Context, id NodeID, ev Event) context.Context {
    httpClient.Post("...", ...)  // 错: ctx 不传, deadline 失控
}
```

正确: 用 `ctx` 传给所有下游调用 (HTTP / DB / gRPC).

### ❌ chunk 流也写 session.Events

```go
session.AddEvent(NewChunkEvent(...))  // 错: chunk 污染审计流
```

正确: chunk 走 `EventSink.Send(Event{Type:"chunk"})` 不进 session, 业务 SSE 订阅 OrchHook.OnNodeEvent.

### ⚠️ 误解：以为 ChainHooks 给 panic 隔离

`hook.NewChainHooks(...)` 提供 nil/typed-nil 过滤 + 多 hook 链式串联，**不提供 panic 隔离**——一个 hook panic 仍 fail-fast 中断链内剩余 hook（audit² 删了 per-hook recover）。

hook panic **不会崩 Run goroutine**：框架兜住，多数转 err、成功之后的观测钩子只记 ERROR（见 §8），与是否用 ChainHooks 无关。想 per-hook panic-safe 自己在 hook 内 `defer recover()`.

### ❌ fan-out 场景 OrchHook 内写共享 slice 无锁

```go
// 仅 OrchestrationHooks (OnNodeStart/End/Event) 在 ParallelAgent / SendFrom fan-out 时
// 多 goroutine 并发触发, 业务 hook.Hooks 串行 (toolsNode 顺序 fire OnToolStart/End 不并发).
type myOrchHook struct {
    events []string  // 错: ParallelAgent fan-out 多 goroutine 并发 append → race
}
func (h *myOrchHook) OnNodeStart(...) (context.Context, error) {
    h.events = append(h.events, nodeID)  // race!
}
```

正确: 用 `sync.Mutex` / `chan` / `atomic.Value` 保护. 或者 `RecordingHooks` (内部已 sync 保护).

**业务 `hook.Hooks` (OnToolStart/End 等) 无需 thread-safe** — toolsNode 单 goroutine 串行 fire (真并发只在 inner `dispatchToolkit.Call`).

## 11. 假绿陷阱 (炸弹 fixture 防御)

### 测试 mock hook 不读 input

```go
type mockHook struct{}
func (m *mockHook) OnModelStart(ctx, info, input) (context.Context, error) {
    return nil  // 错: 没断言 input.Messages 是期望的
}
```

业务"上游吞消息" bug — mock hook 0 断言 input → 测试假绿. 正确: mock hook 至少断言 `containsString(input.Messages, "expected_field")` 验"框架真传了预期 message".

详见 [`CLAUDE.md` "测试可信度" 章节](../../CLAUDE.md) 炸弹 fixture 清单第 5 条「mock 不读 input」。

## 12. 参考

- 引擎层 hook 速览: [channel-engine.md §10 OrchestrationHooks](channel-engine.md)
- chunk 实时投递场景: [context-compaction.md "Streaming + ReactiveCompact retry 时前端 chunk 重启"](context-compaction.md)
- 嵌套 hook 透传范式: [usage-guide.md §6 业务级 hook](usage-guide.md)
- HITL + Hook 协议: [hitl-resume.md §9](hitl-resume.md)
- 三方对照 (LangGraph callbacks / eino callbacks): [doc/description/three-way-comparison/](../description/three-way-comparison/)
