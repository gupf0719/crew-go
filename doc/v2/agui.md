# AG-UI 前端协议适配（`agui` 包）

把 crew-go 的 agent 执行事件适配成 **AG-UI（Agent-User Interaction Protocol）** 标准
事件流，经 SSE 推给前端——业务可直接接 CopilotKit 等 AG-UI 生态的现成前端组件
（React / Vue / Angular），不必为 crew-go 专写前端。

> 决策溯源见 [issue-49](../issue/issue-49-frontend-stream-adapter.md)「2026-07-01 决策修订」
> （推翻早前「不做框架级 SSE」的结论——AG-UI 是对齐外部标准协议，非自定义 SSE 抽象）。

---

## 1. AG-UI 是什么

AG-UI 是 CopilotKit 主导的 agent→前端事件协议（camelCase JSON + Server-Sent Events），
已被 LangGraph / CrewAI / LlamaIndex / Mastra / Pydantic AI / Microsoft Agent Framework
等主流框架接入。定义约 33 种标准事件（`TextMessage*` / `ToolCall*` / `RunStarted/Finished` /
`StateSnapshot` 等）——**一个前端可消费不同后端**。

`agui` 包**内联精简**了 server 端要用的事件类型（字段 / json tag 照协议 wire），**不硬
依赖**官方 community Go SDK（无版本 tag + 引 `logrus`，与 crew-go 的 `slog` 不一致）——
协议是薄层，内联可控，同 `modules/` 拷 spec 的套路。

---

## 2. 快速上手

```go
cp := compose_v2.NewInMemoryCheckpointer() // HITL Resume 用（不做 HITL 可省）
h := agui.Handler(func(hooks hook.Hooks) agent_v2.Agent {
    return agent_v2.NewBasicAgent(
        agent_v2.WithName("assistant"),
        agent_v2.WithModel(m),
        agent_v2.WithToolkit(tk),
        agent_v2.WithHooks(hooks), // ← adapter 必须注入 agent，否则只剩纯文本
    )
}, agui.WithCheckpointer(cp),
   agui.WithOwnerResolver(func(context.Context) string { return "demo" }))
http.Handle("/agui", h)
```

前端把 CopilotKit 的 `runtimeUrl` 指到 `/agui` 即可。请求是 `POST` + `RunAgentInput`
（`threadId` / `runId` / `messages`；`resume` 非空走 HITL 续跑），响应是
`text/event-stream` 的 AG-UI 事件流。

---

## 3. 为什么是 `AgentFactory`（而非直接传 agent）

每次实际执行使用独立的事件 adapter，`hook.Hooks` 在构造 agent 时注入。业务可在
工厂闭包外预建 model、toolkit、skillbox，再在每次执行时构造带当前 adapter 的 agent。
事件重放直接读日志，不调用工厂。


---

## 4. 事件映射

adapter（`agui/adapter.go`）一个 struct 同时实现 `hook.Hooks` + `agent_v2.OrchestrationHooks`：

| crew-go 事件源 | AG-UI 事件 |
|---|---|
| `OnNodeEvent`（chunk，带 `run_id`） | `TextMessageStart/Content/End`（按 `run_id` 分消息，切换即收尾旧开新） |
| `OnModelEnd`（Generate 非流式路径） | 补发完整 `TextMessage`——model 不支持 Stream 时无 chunk，最终答案在此不丢。续跑沿用原样的那一遍（`hook.ModelResponseReusedKey`，业务在 `OnModelEnd` 里问人之后）不再发：上一次 run 已经交给前端，再发是同一个 messageId 的第二条——适配层要排在会问人的钩子前面，否则非流式时那次没补发、这一遍也不补 |
| `OnToolStart`（`CallId`/`Name`/`ArgumentsJSON`） | `ToolCallStart` + `ToolCallArgs` + `ToolCallEnd`（参数非流式，一次性发） |
| `OnToolEnd`（`Result`） | `ToolCallResult`（`CallId` 作 `toolCallId` 配对锚点） |
| Run 开始 / 结束 / 出错 | `RunStarted` / `RunFinished` / `RunError` |
| `InterruptError` | `RunFinished(outcome=interrupt, interrupts=[{id, reason, message, …}])`：工具审批 reason=`tool_call`（带 `toolCallId`、`responseSchema` 与 `metadata.options`），没有工具调用 ID 时 `confirmation`；普通提问默认使用 `input_required` 和字符串响应 schema，可用 `compose_v2.AskQuery.ResponseSchema` 覆盖 schema。框架断点使用 `crew-go:breakpoint` |
| `EventSink.Send("state_snapshot")` | `StateSnapshot`（业务 opt-in，见 §6） |
| `EventSink.Send("reset_stream")` | 收尾那条消息；作废时连同这次模型调用的工具调用与结果一起作废（按框架标在工具钩子 `Extra` 上的 `agent_v2.ToolModelRunIDExtraKey` 认），带 `replacement`（`OnModelEnd` 钩子改写了答复）时换成新正文；再发 AG-UI 标准的 `MessagesSnapshot`（请求带来的消息原样 + 这个 run 发出、没作废的消息）纠正前端——连着来的几个撤回与改写攒成一份，在适配层下一次往外写别的事件、或 run 收尾时发，有在途的流式消息时等它们都收尾（落在一条流中间时官方客户端的事件压缩回放把快照排到那条流之后，那条消息回放出来只剩半截），另立即发 `Custom(crew.reset_stream)` 给按 run_id 自管缓冲的前端。**请求没带 messages 时不发快照**（快照之外的消息会被前端丢掉，而适配层不知道它手上有什么），标准前端那时留着旧内容 |
| `OnError` | `Custom`（前端可处理，标准组件忽略） |

> **run 边界（RunStarted/Finished/Error）由 handler 发**，中间事件由 adapter 发——单一
> 所有权，不重复。

---

`context` 的描述和值以及非空 `state` 会以 JSON 补到本次送模的用户消息。BasicAgent 和 LLMPlanner 使用这份临时数据，历史和存档保留原对话。默认会话的 Resume 请求可提供新的本轮数据。共享会话续跑时沿用 Run 时的这份数据。客户端的 system / developer 消息仍按前文规则过滤。[AG-UI 输入类型](https://docs.ag-ui.com/sdk/js/core/types) · [中断原因](https://docs.ag-ui.com/concepts/interrupts#reason-taxonomy)

## 5. HITL 闭环（Resume）

```
Run → 中断 → RunFinished(outcome=interrupt, interrupts=[{id, reason, message, …}])
前端渲染追问 → 用户答 → 带同 threadId + resume=[{interruptId, status, payload}] 续发
→ handler 走 Resume（答案按 InterruptID 命中 ask_user）→ 续跑 → 完成 or 再次中断（多轮）
```

- `resume` 每项对齐**官方 AG-UI ResumeEntry**：`interruptId`（= RunFinished 的
  `interrupt.id`）+ `status`（`resolved` 已答 / `cancelled` 用户放弃）+ `payload`（答案）
  ——CopilotKit 等标准前端按此格式发。
- **resume 列表恰好覆盖挂起的中断**（AG-UI 1.0：每个挂起的中断都要答到或明确放弃，不支持只答
  一部分）：有不是挂起中断的 ID、同一个 ID 出现两次、漏了某个挂起的中断，都返回
  HTTP 400 `invalid_resume`，会话保持挂起，前端改好重发。规范把这条交给前端
  检查，handler 仍然查：同一个 ID 两条时后一条曾覆盖前一条，审批的批准与拒绝取决于数组顺序。
- **payload**：工具审批只认 `true` / `false`、`{"approved": bool}`（对象只能有这一个字段——
  不支持带修改的批准，带 `editedArgs` 的答复不会按原参数执行）与四个选项字符串（词表见
  `agent_v2.NormalizeApprovalAnswer`）；认不出返回 HTTP 400 `invalid_resume_payload`，保留整批挂起项。
  其余提问的字符串原样传给节点，结构化值经 JSON 序列化后传入。
- **答复按任务累积**：同一个任务里采纳过的答复都记着，每次续跑整份交给 Resume。没进工具账的
  提问（`ask_user`、节点里直接 `compose_v2.Ask` 的）续跑时会重问，框架要 Resume 的调用方把答过的
  答案带上；只交这一轮的话，一个节点先后问两问会来回重问、永远答不完。新开任务（同 threadId
  不带 resume 再发）时清空。
- 节点可在 `compose_v2.AskQuery.ResponseSchema` 声明 JSON Schema 对象。它随存档往返，
  由 AG-UI 投影到 `responseSchema`。Handler 在续跑前校验 `resolved` 的 payload。
  字段缺失或类型不符时返回 `invalid_resume_payload`，保留原挂起项。
  允许文档内引用，外部引用报配置错误。工具审批沿用其固定答复格式。
- 全部 `cancelled` 时走 `agent_v2.ResumeActionAbort`，清档后发 `RunFinished(outcome=cancelled)`。
  混合答复中，取消项以 `[用户取消了此项]` 传给节点。
- 默认进程内会话配 `agui.WithCheckpointer` 后支持多轮续跑。同一 `(owner, threadId)`
  在途时第二个请求收到 `busy`。新 Run 替换旧挂起会话时清理旧存档。

- **上限防 OOM**：挂起会话超 `WithMaxSessions`（默认 10000）时淘汰一个空闲会话 + 清其
  cp——防前端放弃中断（关页面 / 断网）导致 sessions map 长跑 OOM。**淘汰按 owner 分摊**：
  份额最大的那个 owner 出账，且**到达者不赢平局**（平局时由发起这次请求的 owner 自己出，
  它没有空闲会话可出就谁也不淘汰）。单租户部署只有一个分区，这退化成全局 LRU、可用会话数
  不打折；多租户下开个新会话就挤掉别人挂起的 HITL 会话这条路是堵死的，连「跑完即走」
  那种份额恒为 1 的请求也挤不动。代价是**上限变软**——实测约 `max(maxSessions, 有挂起
  会话的租户数) + 在途请求数`，而增量挂在**身份数**上（一个身份恰好多占一格，跟它发多少
  请求无关）。详见 `WithMaxSessions` 的 godoc。

### 共享会话

持久化部署使用 `agui.WithSessionStore` 同时提供会话与 Checkpointer。进程内模式的 `agui.WithCheckpointer`
要求存档随进程退出而消失，由 `compose_v2.VolatileCheckpointer` 声明。仅经请求中间件注入 Checkpointer 时，Handler 会拒绝请求。
当前任务和嵌套图沿用 Handler 绑定的存档后端，钩子或图选项设置的 Checkpointer 不会替换它。
已有 SQLite、PostgreSQL 或 MySQL 部署可用原 Checkpointer 构造 `sql.NewAGUISessionStore` 并运行 `Setup`。
Redis 部署使用 `redis.NewAGUISessionStore`。每个实例使用相同存储域，同一工作流版本使用兼容的 Agent 图定义。

```go
cp := crewsql.NewCheckpointer(db, crewsql.Postgres{})
sessions := crewsql.NewAGUISessionStore(cp)
if err := sessions.Setup(ctx); err != nil {
    return err
}
mux.Handle("/agui", agui.Handler(build,
    agui.WithOwnerResolver(resolveAuthenticatedOwner),
    agui.WithSessionStore(sessions),
))
```

会话记录由 Handler 在启动 Run 前写入，保存任务 ID、Run 时的调用方前文、临时送模数据和已采纳答复。
根历史写入器在写入前登记实际历史分区，续跑和取消补偿复用这个分区。应用在各实例提供同一 HistoryStore 配置。
续答时缺少该配置会保留挂起存档与已采纳的答复，可在配置恢复后重试。
框架按任务 ID 写图存档。挂起集合以根存档为准，SSE 和查询接口都从根存档投影。
同一 `(owner, threadId)` 的请求先取得共享会话租约，续跑在任务租约内校验当前中断 ID。
替换时，后端原子核对租约、更新会话并清理旧存档。

```mermaid
sequenceDiagram
    participant A as 实例 A
    participant DB as 共享数据库
    participant B as 实例 B
    A->>DB: 保存会话与任务 ID
    A->>DB: 图挂起时写入根存档
    B->>DB: 取得会话和任务租约
    B->>DB: 读取当前问题与已采纳答复
    B->>DB: 校验后保存本次答复
    B->>DB: 续跑并写入下一问或清理已完成任务
```

查询待审批时调用 `GET /agui?threadId=<id>`，仍通过配置的 owner 解析器确定归属。返回
`{"threadId":"...","interrupts":[...]}`，暂无待答问题时 `interrupts` 为空数组。
如果某条当前问题的答复已保存且图还停在该处，响应会带 `acceptedInterruptIds`。
这时可发送同一个 ID、`status:"resolved"` 并省略 `payload`，由服务端重放已采纳的答复。
线程正被驱动时返回 HTTP 409 `busy`。图已进入下一问时，迟到的旧 ID 返回 `invalid_resume`。

共享路径重建 `agent_v2.RunSession`。已准入工具依赖图存档中的调用状态，新调用按当前工具审批规则判定。
挂起上限和 TTL 适用于进程内会话。普通执行写入共享会话终态标记后，后续同线程请求会补偿未完成的清理。
如果终态标记本身未能写入，可以在数据库恢复后重新发起 Run 清理上一任务。
进程在形成根存档前退出时，可以用同一线程重新发起 Run 并清理上一任务。

进程内会话在用户全选 `cancelled` 后进入清理状态。清档失败会发送 `abort_failed` 并保留任务入口，
再次发送原中断 ID 的取消请求可继续清理。送回答案会得到 HTTP 409 `cleanup_pending`。
同线程新 Run 在清理成功后才能启动。容量或 TTL 回收也会用原取消请求的身份完成清档和历史收尾。
补偿入口与存档随默认的进程内会话保存。

### 持久化取消补偿

1. 全部取消时，在会话租约和原任务的放弃许可内核对根存档写代及中断 ID。将取消意图与原任务 ID 持久化，任务进入关闭状态。
2. 存储在同一操作中删除原任务的存档子树并记录清档进度。若该任务有根历史，框架在原分区补齐尚无结果的工具调用。历史后端故障或缺少配置时会保留关闭记录供重试。
3. 历史收尾成功后，存储在两把租约的保护下移除会话记录。持久历史与存档可使用不同后端，重试会跳过已有结果的调用。

关闭期间续答返回 HTTP 409 `cleanup_pending`。同一取消批次可重试，挂起查询和同线程新 Run 也会尝试补偿。
如果保留的原工作流工厂不可用，返回 `workflow_version_unavailable`，待该版本恢复后继续。
进程崩溃留下的有效租约到期后，新实例即可接手。带持久回执时，用新的 `runId` 重试清理，原 `runId` 继续回放原请求的回执。
取消意图写入失败且未提交时，任务仍可通过原请求重试取消。关闭记录依赖上述请求触发补偿。

旧版进程内模式丢失的会话映射无法从 Checkpointer 还原。清理这类存量档时，应用需要用可核实的原任务 ID 确定范围。
未带历史分区的旧共享记录按首次取消请求解析历史配置。旧版实例会把关闭标记当成普通执行收尾，
共享同一会话后端的实例需升级到支持持久化取消补偿的版本。

### 持久回执与断线续传

应用可以配置 `agui.WithRunStore` 保存每次 HTTP Run 或 Resume 的回执和事件。各入口实例共享日志与
会话后端。调用方带固定的 `threadId`、`runId`，重试时保留原请求的 JSON 成员顺序及内容，空白可以变化。
同一归属、线程和 Run ID 的首次提交认领执行，重试订阅原事件，内容变更返回 `409 run_id_conflict`。
每次新的 Run 或审批 Resume 使用新的 `runId`。

```go
runs := crewsql.NewAGUIRunStore(cp)
if err := runs.Setup(ctx); err != nil {
    return err
}
mux.Handle("/agui", agui.Handler(build,
    agui.WithOwnerResolver(resolveAuthenticatedOwner),
    agui.WithSessionStore(sessions),
    agui.WithRunStore(runs, 2*time.Minute),
))
```

- `GET /agui?threadId=t&runId=r` 查询回执。响应包含 `status`、`lastEventId`，收尾后有 `terminal`。
  认领后、开流前拒绝的状态是 `rejected`，并带 `httpStatus`。请求格式、协议版本和游标检查在认领前执行。
  无记录或其他归属查询返回 `404 run_not_found`。
- 同一个 GET 带 `Accept: text/event-stream` 可订阅。用 `Last-Event-ID` 或 `after` 指定已消费的最后一帧，
  从下一帧继续。两处都给时优先取请求头。每个 Run 的 SSE `id` 从 1 递增，连接追上终态后关闭。
  重试 POST 也可带游标。负数或超范围的游标返回 `400 invalid_cursor`。
- Run 认领后独立于连接执行。超时由 `WithRunStore` 配置，零值为五分钟，保留请求上下文中更短的 deadline。
  执行正常收尾时先归还会话与任务租约，再提交终帧，事件与终态同事务。订阅读库失败时连接结束，客户端可以沿最后一个有效游标重连。
- 已存回执的拒绝会在重试时返回原 HTTP 状态和错误。明确收到 `busy` 或版本不可用后，重新提交时使用新的 `runId`。
  超过执行与收尾期限且回执仍在途时，查询会将状态置为 `error`，代码为 `run_interrupted`。业务可以按
  挂起查询与外部工具的幂等凭证对账。
- 日志保存原始 AG-UI 事件，包括文本和工具参数。应用按重连窗口和数据保留策略调用
  `runs.PruneRuns(ctx, before)` 回收已完成记录与事件，回收后的 `runId` 应退役。
  启用前把流量切到均配置共享日志的入口实例。

```mermaid
sequenceDiagram
    participant C as 客户端
    participant A as 实例 A
    participant DB as 共享 SQL
    participant B as 实例 B
    C->>A: POST 带 threadId 和 runId
    A->>DB: 认领回执并保存事件
    A-->>C: SSE 事件与游标
    Note over A: 连接断开后继续执行
    C->>B: GET SSE 带原 runId 和游标
    B->>DB: 读游标之后的事件
    A->>DB: 释放任务租约并提交终帧
    B-->>C: 补发事件和终帧
```

### 挂起任务的工作流版本

用 `agui.WithWorkflowVersions("v2", map[string]agui.AgentFactory{"v1": buildV1})` 配置共享会话时，
`Handler` 传入的工厂承接 v2 的新任务。任务创建时保存版本，后续审批、取消与恢复使用任务记录中的版本。
缺少对应工厂会返回 `409 workflow_version_unavailable`，已挂起的问题与已采纳答复保持原样。
明确收到该拒绝后，把保留的 `interruptId` 和新 `runId` 提交给支持该版本的实例。断线后的原请求重试沿用原 `runId`。

已存在的未标记任务可以通过 `previous` 的空键显式交给原定义。版本标识对应一套兼容的图、工具与审批规则，
修改其语义时使用新版本。内建 SQL 后端会让旧二进制在加载带版本的记录时返回 503。发布时在旧版本的任务处理完前保留其工厂。

---

## 6. StateSnapshot 业务 opt-in

AG-UI 的 `StateSnapshot` 用于前端渲染 agent 结构化 state（如 CopilotKit
`useCoAgentStateRender`）。crew-go **不自动发**——channel 是异构声明集合、**无统一 state
schema**，且 **Go 泛型 runtime 擦除**拿不到字段 schema（不像 Python TypedDict 能
introspect）。业务想同步 state 在节点内主动发：

```go
compose_v2.GetEventSink(ctx).Send(compose_v2.Event{
    Type: "state_snapshot", Metadata: map[string]any{"snapshot": myBusinessState}})
```

对应 LangGraph 的 `manually_emit_state` 路径（它另有 node-exit 自动发，靠统一 State
TypedDict，crew-go 架构上不适用）。决策见
[state-model.md](../description/three-way-comparison/state-model.md) §六-9。

---

## 7. 非 BasicAgent 编排

任意编排（Sequential / Plan / Reflect / Graph…）都支持——**工厂里把 adapter 挂到每个
sub-agent**（不只顶层），嵌套编排的 LLM 输出才都被捕获：

```go
agui.Handler(func(hooks hook.Hooks) agent_v2.Agent {
    mkStep := func(name string) agent_v2.Agent {
        return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m),
            agent_v2.WithHooks(hooks)) // ← 每个 sub-agent 都挂 adapter
    }
    return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
        AddStep("s1", mkStep("step1")).AddStep("s2", mkStep("step2"))
}, agui.WithOwnerResolver(func(context.Context) string { return "demo" }))
```

- chunk 走 `OrchHooks` **嵌套继承**：handler 用 `WithCtxOrchHooks(ctx, adapter)` push，
  sub-agent 沿 ctx 自动继承 → sub-agent 的 chunk 也**逐段真流式**（而非只靠顶层
  `OnModelEnd` 一次性补发）。OrchHooks 无需挂每个 sub-agent。
- tool / model 输出走各 sub-agent 的 `hook.Hooks`——`hook.Hooks` 是构造期注入（不走 ctx），
  故 adapter 要挂全，漏挂的 sub-agent 其 tool / 非流式输出不会进 AG-UI 流。

---

### 用户消息的媒体输入

`role=user` 的 `content` 可用字符串，或混合 `text`、`image`、`audio`、`video`、
`document` 部分的数组。媒体部分的 `source` 使用以下字段。

| `source.type` | `value` | 其他字段 |
| --- | --- | --- |
| `data` | 标准 Base64 正文 | 必填 `mimeType` |
| `url` | HTTP(S) URL | `mimeType` 可选，可从扩展名推断 |
| `file` | 服务商文件 ID | 仅 `document`，可用 `provider` 绑定服务商 |

历史中的用户媒体也会传给 agent。文本部分按顺序合并，媒体保留在内容块中。共享会话续跑时会恢复 Run 时前文中的用户媒体。
OpenAI Chat Completions 适配器支持图片、WAV/MP3 内联音频和 PDF 内联或文件 ID。
DashScope 分支还支持音视频 URL，并按该服务的格式发送内联音视频。音频 URL 无可识别
的扩展名时，显式传入 `audio/wav` 或 `audio/mpeg`。
Anthropic 支持图片、PDF、内联纯文本文档和 Anthropic 文件 ID。具体可用的媒体取决于所选模型。
[AG-UI Schema](https://github.com/ag-ui-protocol/ag-ui/blob/main/docs/spec/1.0/schema.mdx) ·
[OpenAI 文件输入](https://developers.openai.com/api/docs/guides/file-inputs?api-mode=chat) ·
[DashScope 兼容协议](https://docs.modelstudio.console.alibabacloud.com/en/model-studio/qwen-api-via-openai-chat-completions)

## 8. 限制

- **默认连接生命周期**：进程内会话和只配置共享会话的请求随 `r.Context()` 取消。启用
  `WithRunStore` 后，认领的执行拥有独立期限，连接断开后可按游标重连。
- **StateSnapshot 不自动**：Go 泛型擦除，业务 opt-in emit（§6）。
- **编排特有语义丢失**：`ForkLane` / Plan replan / 嵌套 ns 在 AG-UI 无原生对应，降级
  `Custom` / metadata 或不映射（AG-UI 前端心智是 message + tool，不需引擎内部结构）。
- **协议演进**：AG-UI 仍在迭代（有 deprecated 如 Thinking→Reasoning），内联协议层需偶尔
  跟版本——但协议薄，加 / 改几个 struct 即可。

---

## 9. 验证

- 单测 [`agui/agui_test.go`](../../agui/agui_test.go)：SSE wire 格式 / streaming 端到端
  （chunk delta 拼回）/ HITL Run→中断→Resume→完成 / 无 cp resume 报错 / 非 BasicAgent
  （SequentialAgent）。
- [共享审批回归](../../sql/agui_session_test.go)覆盖独立连接接手、断流重连、迟到答复、旧取消与租约失效。
- [SOP 示例](../../example/orchestration/sop_demo/main.go)演示结构化审批、校验回退、拒绝及批准后执行。
- 真 qwen 冒烟 [`example/agui/agui_smoke`](../../example/agui/agui_smoke)：① tool + streaming
  ② 多轮 HITL Resume（qwen 调 ask_user 追问 → 中断 → 多轮续发续跑）。

---

## 10. 与 `plan_chat_web` 的关系

`example/orchestration/plan_chat_web` 是**自定义 SSE 参考实现**（内部调试 / 精细场景），事件是 crew-go
自定义格式、非 AG-UI 标准。`agui` 是**对外标准协议**通道（接 CopilotKit 生态前端）。两条
并存不替代——精细内部调试用 plan_chat_web，对接标准前端生态用 agui。
