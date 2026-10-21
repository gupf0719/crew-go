---
id: ISSUE-56
title: plan_chat_web HITL（暂停 / 恢复）支持 + PlanAgent.Resume API
status: ✅ 已实现
priority: 高
created: 2026-04-28
resolved: 2026-04-28
source: 用户实跑 plan_chat_web 时缺"终止按钮"——澄清后定位为 HITL 中断 + 恢复需求，不是单纯 cancel
---

# ISSUE-56 · plan_chat_web HITL 支持

> **⚠️ v2 标注 (audit follow-up 2026-05)**: 本 ISSUE 文档是 v1 时代写的, 其中
> code reference / error message 字面量是 v1 旧版. v2 演化后部分协议已变:
> - "plan agent: resume requires Checkpointer in ctx" 字面 → v2 现为
>   `"plan agent %q: resume requires compose_v2.Checkpointer in ctx (use compose_v2.WithCheckpointer)"`
>   (commit 8330d6e 9 编排统一格式)
> - `compose/*.go` / `agent/*.go` line ref 都是 v1 路径, v2 等价位置查
>   `compose_v2/` / `agent_v2/` godoc
> - 历史档案保留正文不重写, 业务读时按 v2 实际代码为准

## 需求来源

用户跑 plan_chat_web 时反馈"页面没终止按钮"。最初提了 `eventSource.close()` 单侧关流方案，
用户否决：

> 如果只是加前端按钮 eventSource.close() 那不是不能中断恢复了？

→ 真实需求是 **interrupt（可恢复的暂停）**，不是 cancel。框架里 `*agent.InterruptError` 的
语义本来就是"挂起 + 后续可 resume"，要做这个就得走 checkpoint 路径。

## 框架现状调研

| 项目 | 状态 |
|---|---|
| `agent.InterruptError` | ✅ 已有 |
| `RunGraph` 收到 InterruptError 自动 save checkpoint | ✅ 已有 |
| `ResumeGraph(ctx, g, cp, sessionID, opts)` | ✅ 已有 |
| `agent.WithCheckpointer(ctx, cp)` / `agent.GetCheckpointer(ctx)` | ✅ 已有 |
| **PlanAgent.Run 是否透 Checkpointer 给 RunGraph** | ❌ 写死 nil |
| **PlanAgent 是否暴露 Resume 方法** | ❌ 没有 |

→ **框架原语都有，PlanAgent 缺 HITL 入口**。

## 改造一：PlanAgent.Run/Resume + Checkpointer 透传

### 抽 `buildGraph(initialInput) (*Graph, error)` 共用

原 `Run` 内部把图构造和执行混在一起；抽出来让 `Run` 与新增的 `Resume` 都能复用。
`initialInput` 仅 Run 路径有意义；Resume 路径下 `unmarshalGraphState` 会用 factory 产生空
`planState` 然后用 checkpoint JSON 覆盖填充——传空字符串安全。

### `Run` 透 Checkpointer

```go
final, err := RunGraph(ctx, g, initialState, GraphRunOptions{
    Checkpointer: agent.GetCheckpointer(ctx),  // 从 ctx 取，调用方注入
    OrchHooks:    extractOrchHooks(opts),
})
```

调用方用 `ctx = agent.WithCheckpointer(ctx, cp)` 注入。InterruptError 路径就能 save 了。

### 新增 `Resume(ctx, sessionID, opts...)`

```go
func (p *PlanAgent) Resume(ctx context.Context, sessionID string, opts ...agent.RunOption) ([]*agent.AgentEvent, error) {
    cp := agent.GetCheckpointer(ctx)
    if cp == nil {
        return nil, fmt.Errorf("plan agent: resume requires Checkpointer in ctx")
    }
    g, err := p.buildGraph("")
    if err != nil { return nil, fmt.Errorf("plan agent: %w", err) }
    final, err := ResumeGraph(ctx, g, cp, sessionID, ResumeOptions{})
    if err != nil { return nil, fmt.Errorf("plan agent resume: %w", err) }
    return []*agent.AgentEvent{agent.NewExitEvent(p.name, agent.GetRunPath(ctx), p.summarizeFinal(final))}, nil
}
```

### 抽 `summarizeFinal(final) *Message`

Run / Resume 共用最终输出汇总逻辑（finalAnswer / executed 拼接 / fallback）。

### 端到端单测

`compose/plan_test.go::TestPlan_InterruptAndResume`：
- pausableAgent.handler 在 `pause atomic.Bool` 为 true 时返 *InterruptError
- 第一次 Run：worker 抛 InterruptError → checkpoint 自动 save → Run 返 wrap 后的 error
- `errors.As(&ie)` 解出原始 InterruptError，验证 Reason
- `cp.Load(ctx, sess.ID)` 验证 checkpoint 已存
- 清 pause flag → `ag.Resume(ctx, sess.ID)` 续跑 → 拿到 finalAnswer "all done"

`TestPlan_Resume_NoCheckpointer`：ctx 没注入 Checkpointer 时 Resume 立即报错。

## 改造二：plan_chat_web HITL 服务端

### `runSlot` + 全局 `runs sync.Map`

```go
type runSlot struct {
    sess      *agent.RunSession
    cp        *agent.InMemoryCheckpointer
    interrupt atomic.Bool
    finished  atomic.Bool
}
var runs sync.Map  // session_id → *runSlot
```

同一 session_id 跨 SSE 连接复用：第一次新建 → 跑到一半 interrupt → save checkpoint → SSE
断开 → 用户点"继续" → 二次 GET `/api/chat?session_id=xxx` 走 Resume → 续跑 → 完成清理。

**单进程内存 mode**：用户已确认上生产前再换 Redis（`Checkpointer` 是接口，未来加
`RedisCheckpointer` 是替换问题，不需要现在动结构）。

### `interruptHook`

```go
type interruptHook struct {
    hook.NoOpHooks
    flag *atomic.Bool
}
func (h *interruptHook) OnAgentStart(_ context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
    if h.flag != nil && h.flag.Load() {
        return &agent.InterruptError{
            Reason: "user requested pause",
            Data:   map[string]any{"agent": info.Name, "round": info.Round},
        }
    }
    return nil
}
```

挂在所有 sub-agent + planner 的 `hook.ChainHooks(observer, intHook)` 链上。
`OnAgentStart` 每轮 LLM 调用前最早触发 —— 挂起颗粒度 = 一轮 LLM。

### Endpoint

| Method | Path | 行为 |
|---|---|---|
| GET | `/api/chat?q=...` | 新建会话，启动 PlanAgent.Run |
| GET | `/api/chat?session_id=...` | 恢复挂起会话，调用 PlanAgent.Resume |
| POST | `/api/interrupt?session_id=...` | 设置 slot.interrupt = true，返回 200 |

错误码：
- `400` missing both q and session_id
- `404` session not found in `runs`
- `410` session already completed（finished.Load() == true）

### `runOrResume` 统一 goroutine 主体

每次连接重建 PlanAgent + sub-agents（hooks 含当前 sink，无法跨连接复用）；slot.sess + slot.cp
通过 ctx 透传到框架，PlanStep 历史靠 cp 持久化跨连接续跑。

捕 InterruptError 推 `interrupted` SSE 事件：

```go
if errors.As(err, &ie) {
    sink.send("interrupted", map[string]any{
        "session_id": slot.sess.ID,
        "reason":     ie.Reason,
        "data":       ie.Data,
    })
    sink.send("done", map[string]any{"interrupted": true, "session_id": slot.sess.ID})
    return
}
```

正常完成 → `slot.finished.Store(true)` + `runs.Delete(sess.ID)` + 推 `final` + `done`。

## 改造三：plan_chat_web HITL 前端

### UI

`<button id="stop">⏸</button>` + `<button id="resume">▶</button>`，按状态机切换可见性。

### 状态机

```
idle → running ↔ paused → finished
```

| 状态 | sendBtn | stopBtn | resumeBtn | qEl |
|---|---|---|---|---|
| idle / finished | 显示+启用 | 隐藏 | 隐藏 | 启用 |
| running | 显示+禁用 | 显示+启用 | 隐藏 | 禁用 |
| paused | 隐藏 | 隐藏 | 显示+启用 | 禁用 |

### 网络

- `send()` → `EventSource('/api/chat?q=...')` → `running`
- `stop()` → `fetch POST /api/interrupt?session_id=...` → 等服务端推 `interrupted` 切 `paused`
- `resume()` → `EventSource('/api/chat?session_id=...')` → `running`

### SSE 事件追加处理

- `user_input` / `resume`：拿 session_id 存 `currentSessionID`
- `interrupted`：聊天栏插一条"⏸ 已挂起 · {reason}"，状态切 `paused`
- `done` 带 `interrupted: true`：跳过 finished 清理（state 已切 paused）

## 改造四：mock 加 500ms 延迟

`mockChatModel.Generate` 在每次调用前 `time.After(500ms) select ctx.Done()`：

1. 让用户在浏览器看清各 step 流转，而不是一闪而过
2. 给 HITL 中断留足窗口（点 ⏸ 后下一轮 hook 触发抛 InterruptError）

ctx 取消时立即返回，不阻塞退出。

## 端到端冒烟验证

```bash
# 启动 chat（4 步流程，每步 ~500ms）
curl -sN 'http://localhost:18080/api/chat?q=hitl_test' &
sleep 0.7    # 等 planner 完成、进入 researcher

# 抓 session_id 发 interrupt
SESSION=$(grep -oE '"session_id":"[^"]+"' chat1.log | head -1 | cut -d'"' -f4)
curl -X POST "http://localhost:18080/api/interrupt?session_id=$SESSION"   # 200

# resume
curl -sN "http://localhost:18080/api/chat?session_id=$SESSION"           # 继续跑到 final
```

实际 SSE 事件流：

```
chat1: user_input → graph_start → ... → interrupted → done(interrupted=true)
resume: resume → graph_start → ... → final → done
```

✅ HITL 完整闭环跑通。

## 已知限制 + 后续

### 已知坑（本次未修，记下）

1. **`retryRunAgentNodeMsg` 不识别 InterruptError** —— 用户配 `WithPlanMaxRetries(N>0)` 时
   会盲重试 N 次 InterruptError 才往上抛。plan_chat_web 没用 retry 没踩到，但是潜在 bug，
   值得开 ISSUE 跟进
2. **InterruptError.Data 跨 Resume 丢类型** —— ISSUE-40 已记：JSON 反序列化把 `any` 变成
   `map[string]any`。本 demo Data 只塞 `{agent, round}` 这种 map[string]any，恰好兼容；
   用户塞自定义 struct 跨 Resume 会丢类型
3. **InMemoryCheckpointer + sync.Map 跨进程不可共享** —— 用户已知，上生产前换 Redis

### ISSUE-49 P1 关系

ISSUE-49 P1（前端流适配 AG-UI 协议）规划里包含了完整 HITL 支持。本 issue 是 P1 之前的
"plan_chat_web 自有 SSE 协议"快速实现，不阻塞 P1 推进——P1 启动时把 plan_chat_web 协议
迁移到 AG-UI 即可，框架层 PlanAgent.Resume 已经准备好。

## 改动范围

| 文件 | 改动 |
|---|---|
| `compose/plan.go` | 抽 `buildGraph` / `summarizeFinal`；`Run` 透 Checkpointer；新增 `Resume` |
| `compose/plan_test.go` | 新增 `TestPlan_InterruptAndResume` / `TestPlan_Resume_NoCheckpointer` |
| `example/orchestration/plan_chat_web/hooks.go` | 新增 `interruptHook` |
| `example/orchestration/plan_chat_web/main.go` | `runSlot` + `runs sync.Map` + `interruptHandler` + `runOrResume` + `buildPlanAgent` |
| `example/orchestration/plan_chat_web/mock.go` | 加 500ms select-ctx 延迟（HITL 演示窗口） |
| `example/orchestration/plan_chat_web/index.html` | stop/resume 按钮 + 状态机 + EventSource 重连 + interrupted 事件处理 |

## 参考

- ISSUE-40 · CustomizedAction / InterruptError.Data 持久化语义
- ISSUE-49 P1 · 前端流适配 AG-UI（含完整 HITL 协议规划）
- [compose/plan.go](../../compose/plan.go) — Run/Resume/buildGraph/summarizeFinal
- [example/orchestration/plan_chat_web/](../../example/orchestration/plan_chat_web/) — HITL demo
