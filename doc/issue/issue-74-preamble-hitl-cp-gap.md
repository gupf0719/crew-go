# ISSUE-74 · BasicAgent buildInitialMessages preamble 阶段 InterruptError 不存 cp / Resume 不可达

**优先级**：高（HITL 设计完整性）
**状态**：✅ 已修复（2026-05-14，方案 A 落地）
**发现轮次**：R25 audit HIGH-2（2026-05-14）
**修复 commit**：`d322f2d` (Step 2 compose_v2) + `b07c140` (Step 3 agent_v2) + `e639304` (Step 4 测试)

> **API 变更说明**（PR 3.3 commit e6505f9）：本 issue 描述的 Memory 接口已由 store 体系替代——
> `Memory.Read` → `store.Search` / `Memory.Write` → `store.Put` / `WithMemory` → `WithStore` + `WithStoreMemory`。
> preamble 节点化的 HITL 修复在 store 体系下等价保留（preamble 节点跑 `store.Search` 注入，业务可在
> Store 实现内抛 InterruptError）。下文叙述保留历史 API 名作档案。

## 问题

`BasicAgent.Run` 在 `compose_v2.Run`（graph 启动）**之前**调 `buildInitialMessages`：

```go
// agent_v2/basic_agent.go:141
initial, err := a.buildInitialMessages(ctx, session, input)
if err != nil {
    return nil, fmt.Errorf("agent %q: build initial messages: %w", a.config.Name, err)
}
// ↓ 这里之后才 runGraph → compose_v2.Run（含 defer SaveNS）
finalMsgs, err := a.runGraph(ctx, &cfg, session, initial)
```

`buildInitialMessages` 内调 3 个业务回调点：
- `Memory.Read(ctx)` (line 482)：业务可抛 InterruptError 做"读隐私 memory 前用户授权"
- `HistoryCompactor(ctx, prior, keep)` (line 541)：业务用 LLM-driven compactor 时 LLM 调用可能触发 ask_user
- `SkillBox.ActivateSkill(ctx, name)` (line 359, adaptive 模式)：业务可抛 InterruptError 做"敏感 skill 激活前用户审批"（R5 已加 propagate）

**Bug**：这 3 个回调抛 InterruptError 时，`compose_v2.Run` 还**未启动** → `graph_loop.go:227-258` 的 `defer SaveNS` 不触发 → cp 不存 → caller 调 `BasicAgent.Resume(sid)` 时 `cper.LoadNS` 拿不到 cp → "no Pending cp" → HITL 链断。

err 通过 `fmt.Errorf("...: %w", err)` propagate 到 caller，`compose_v2.IsInterrupt(err)` **能识别**让 caller 知道 HITL 触发——但**框架级 Resume 协议失效**，caller 必须走"应用层 HITL"（业务自己持久化用户答案后重新调 `Run`，不依赖 `Resume`）。

## 修法（两选一）

### 方案 A：preamble 节点重构（彻底）

把 `Memory.Read` / `HistoryCompactor` / Skill 预激活全包成 graph 首段 preamble 节点：

- `BasicAgent.Run` 仅构造最小 input（user message + system prompt） → `WithInitialWrites` 写入
- 新增 graph 首段 `preamble` 节点：读 channel input → 调业务回调 → 写回 enriched messages → edge 到 `model`
- 抛 Interrupt 走 graph 内自动 SaveNS 协议，Resume 自然支持

**优点**：协议完全对称，复用现有 cp round-trip。
**缺点**：buildInitialMessages 拼装逻辑要重构成 channel write，代码改动大；可能影响 4 层压缩集成（MicroCompact/AutoCompact 当前依赖完整 messages 在 modelNode 入口可见）。

### 方案 B：手动 SaveNS minimal cp（侵入小）

`BasicAgent.Run` 检测 buildInitialMessages 抛 Interrupt 时手动构造最小 cp + SaveNS：

```go
if ie, isInt := compose_v2.IsInterrupt(err); isInt {
    // 自动补 InterruptID
    if ie.Query != nil && ie.Query.InterruptID == "" {
        graphNS := compose_v2.GetGraphNS(ctx)
        ie.Query.InterruptID = compose_v2.MakeInterruptID(graphNS, "__preamble__", 0, 0, 0)
    }
    cp := &compose_v2.Checkpoint{
        Status: compose_v2.CPStatusPending,
        CurrentFrontier: []NodeID{compose_v2.StartNode},
        PendingInterrupts: []*compose_v2.AskQuery{ie.Query},
        // 新增 metadata 让 Resume 知道这是 preamble cp
    }
    cper.SaveNS(ctx, sid, graphNS, cp)
}
```

`BasicAgent.Resume` 检测"preamble cp"（如 Channels 为空且 Frontier=[StartNode]）→ 重新调 `buildInitialMessages` 注入 ResumeContext → 业务回调读 `compose_v2.GetResumeAnswerSingle(ctx, ID)` 拿用户答案 → 继续正常 Run。

**优点**：现有协议保留，改动局部。
**缺点**：引入特殊 cp 状态 + 新代码路径，需小心维护"preamble cp 不污染常规 cp"不变量。

## 修复详情（方案 A 落地）

实施按 6 step 分 commit：

| Step | commit | 改动 |
|---|---|---|
| 1 | — | compose_v2 引擎层无需改动（cp / interrupt / channel 协议自然支持）|
| 2 | `d322f2d` | `compose_v2/basic_agent.go` 加 `BasicAgentBuilder.WithPreamble(fn)` + `BasicAgentPreambleInput/Fn` 类型 + `basicPreambleInputCh` channel + Compile 拓扑分支（不传时拓扑保持现状向后兼容）+ `BasicAgentPreambleInputWrites` helper + 3 单元测试 |
| 3 | `b07c140` | `agent_v2/basic_agent.go` 改 Run 去 eager `buildInitialMessages` 调用 + runGraph 写 `PreambleInputWrites` + buildComposeGraph 注入 `WithPreamble(a.preambleFn)` + 新增 `preambleFn` 方法（wrap buildInitialMessages）+ init() 注册 `*BasicAgentPreambleInput` type |
| 4 | `e639304` | 5 个端到端 Resume 闭环回归测试：`TestBasicAgent_PreambleHITL_{MemoryReadInterrupt,HistoryCompactorInterrupt,SkillActivateInterrupt,NestedSubAgent}` + `TestBasicAgent_PreambleIdempotent`，sanity-revert 5/5 fail 验证测试精确 |
| 5 | （本 commit） | godoc + idempotent 契约 + ISSUE-74 标记已修 + `doc/v2/hitl-resume.md` §12 新增 preamble HITL 章节 |
| 6 | （后续） | nested_smoke + plan_chat_web smoke 验证嵌套场景 + 删 R25 slog.Warn 缓解警告 |

## 设计验证

- `go test ./...` 全包绿（agent_v2 / compose_v2 / 全包）
- 现有 HITL 测试（TestBasicAgent_HITLResume）继续 PASS——preamble 拓扑透明
- 4 层压缩集成不变——MicroCompact/AutoCompact 在 modelFn 入口拿
  `Get(chs, basicMessagesCh)` 完整 enriched 视图（plan §2 验证）
- Hook 时序不变（**ISSUE-74 落地时的设计承诺**；audit³ D1 + audit⁴ C HIGH-1
  commit `dc93101` 后**已变更**：`OnAgentInit` 移到 agent_v2 wrapper Run/Resume
  top-level defer，Run + 每次 Resume 各 1 次跟 OnAgentEnd 对称；OnAgentStart /
  OnModelStart 仍在 modelNode 触发不变）——preamble 节点仍不触发业务级 hook

## 三方对照

修法与 LangGraph + eino 设计对齐——三方共识"agent 跑前业务回调必须在 graph 内"：

| 框架 | preamble 节点位置 | 源码引用 |
|---|---|---|
| **crew-go**（修后）| `BasicAgentBuilder.WithPreamble` graph 首段 `preamble` 节点 | `compose_v2/basic_agent.go::Compile` |
| LangGraph | `pre_model_hook` 编译为 graph 入口节点 | `libs/prebuilt/langgraph/prebuilt/chat_agent_executor.py:876-885` |
| eino | `BeforeModelRewriteState` 中间件 graph 内首节点 | `eino/adk/middlewares/summarization/summarization.go:320` |

## 关联

- R5 audit HIGH-1：`selectSkillAdaptive` propagate Interrupt（已修，preamble 节点自然继承）
- R24 audit MED-2：`commitFinal` propagate Interrupt（位置后置走 graph defer 对称路径）
- R25 audit HIGH-1/3 同 audit 修，HIGH-2 即本 ISSUE
- 详细协议文档：`doc/v2/hitl-resume.md` §12 BasicAgent preamble HITL
