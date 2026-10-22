# Harness（agent 运行时横切层）

> **本文档定位**：crew-go 的 **harness** 总览——支撑 agent 稳定长跑的横切能力集合（上下文
> 管理 / 长期记忆 / 能力扩展 / 工具治理 / 远端执行 / 指令注入 / 权限）。这些能力横切所有 9 编排：
> 在 `compose_v2` 图 + hook 层一次实现、全模式受益。
>
> **边界**：本文是**总览 + 组合方式**——各能力的详细机制在各自文档，本文只定位 + 链过去，不复述。

---

## 1. 什么是 harness

agent 的主循环（ReAct / 编排）负责"想 + 调工具"；**harness 是支撑它长跑的运行时层**——处理
"停下来之后要能接着跑、上下文会爆、记忆要跨 Run、能力要动态扩、工具多到 LLM 选不过来、要在
远端安全执行、要注入外部规范、危险操作要审批、跨 Run 的会话历史留不住、每次工具调用要能统一
拦一道、中断重跑会把已经做成的副作用再来一遍"这些**主循环之外、但决定能否稳定长跑**的问题。

crew-go 把 harness 做成**横切能力**：不绑某个编排，在图引擎 + hook 层实现一次，9 编排全受益。

## 2. crew-go harness 组成

| 能力 | 解决什么 | 入口 | 详细文档 | 跑一个 |
|---|---|---|---|---|
| **Checkpoint 断点续跑** | 停下来之后能接着跑（HITL 等人回答 / 崩溃 / 软取消）——**下面几项的底座** | `compose_v2.WithCheckpointerCtx`（9 编排）、`WithCheckpointer`（RunOption）；落盘时机与并发保护另有三个旋钮：`WithDurability` / `WithStaleWriteGuard` / `NewSoftCancelSignal` | [checkpoint.md](checkpoint.md)（权威篇）| `example/orchestration/nested_smoke`（HITL 闭环）、`example/harness/durability_smoke`（落盘时机）|
| **4 层上下文压缩** | ctx 会爆 → 分层压缩不丢关键信息 | `WithMicroCompact` / `WithAutoCompact` / `WithReactiveCompact` + `WithStoreMemory` | [context-compaction.md](context-compaction.md) | `example/compact/compact_demo` |
| **Store 长期记忆** | 跨 Run 记住用户偏好 / 长期事实 / 共享黑板 | `WithStore` / `WithStoreMemory` | [store.md](store.md) | `example/store/v2_store_demo` |
| **会话历史层** | cp 是单次 Run 临时态、成功即清 → 对话流跨 Run 留不住；按会话 append-only 留存，每次 Run 只从末尾读用得上的那段（压缩过的会话从最新的压缩边界读起），去重按消息 ID / 任务 / 归属直接问后端 | `WithHistory` / `WithHistoryCtx` | [usage-guide.md](usage-guide.md) 会话历史节 + [ISSUE-98](../issue/issue-98-conversation-history.md) | `example/agent/history_smoke`、`example/orchestration/history_boundary_smoke` |
| **超大工具输出转存** | 一条几百 KB 的工具结果跟着进每一轮 prompt、每份存档、会话历史 → 这一批调用收尾时（`OnToolEnd` 之后）就转存进 store，消息流里只留预览 + 取回 id，模型用 `read_tool_output` 按需分段读 / 搜；压缩掉之后取回 id 留在摘要里 | `WithToolResultSpill` | [tool-output-spill.md](tool-output-spill.md) | `example/harness/spill_smoke` |
| **长时工具转入后台** | 慢工具阻塞整轮 → 跑过 YieldAfter 还没返回就先给任务 id、这一轮接着走，结束后作为通知送进对话；模型可查询 / 等待 / 取消 | `WithBackgroundTasks` | [background-tasks.md](background-tasks.md) | `example/harness/background_smoke` |
| **Skill 能力包** | 按需激活专项能力（SKILL.md，fork / inline + go-func action 执行） | `WithSkills` | [skill.md](skill.md) | `example/graph/v2_skill_demo`、`example/harness/skill_action_smoke` |
| **toolsearch 渐进披露** | 工具 / MCP 聚合太多爆 ctx → deferred 工具按需揭示（fail-closed） | `WithToolSearch` | [toolsearch.md](toolsearch.md) | `example/harness/toolsearch_smoke` |
| **toolfilter 工具裁剪** | 每轮 model 调用前裁业务工具子集 | `WithToolFilter` | [tool-visibility.md](../description/three-way-comparison/tool-visibility.md) | `example/orchestration/plan_chat_web` |
| **工具调用中间件** | 每次工具调用统一拦一道（观测 / 短路 / 挂起），且**不碰 `tool.Tool` 对象** | `WithToolMiddleware` | godoc `agent_v2/tool_middleware.go` | `example/harness/tool_middleware_demo` |
| **sandbox 远端执行** | LLM 发起的命令在远端沙箱跑（可插拔 seam + 输出不可信框定）；后端另可当远程 skill 源 | `sandbox.NewBashTool(sb)` + `WithToolkit` | [sandbox.md](sandbox.md) | — |
| **AgentsMD 指令注入** | 注入 AGENTS.md 外部规范作 harness 指令（transient，不进历史累积） | `WithAgentsMD` | godoc `agent_v2/agents_md.go` | `example/harness/agents_md_demo` |
| **approval 工具审批** | 危险工具执行前 HITL 审批（权限层，复用 InterruptError / Resume）；`ApprovalAskWrites` 只读放行档（只读工具直接执行、其余问人）；审批生效时自动把审批方式写进系统提示 | `WithApprovalMode` / `WithApprovalHeuristic` / `WithApprovalPrompt` | godoc `agent_v2/approval.go` + [execution-control.md](execution-control.md) §3 | `example/harness/approval_smoke`、`example/harness/execution_control_smoke` ② |
| **运行中插话** | agent 执行过程中用户补充的话，在安全位置接进当前回合（不插进工具调用中间；取走之后随存档走，这次调用续不了时放回收件箱） | `WithInboxCtx` + `NewInbox` | [execution-control.md](execution-control.md) §4 | `example/harness/execution_control_smoke` ③ |
| **回合结束闸门** | 模型要收尾时先过业务检查（可问人、可请评审），不满意带着反馈接着干；有否决上限 | `WithTurnGate` / `WithTurnGateMaxVetoes` | [execution-control.md](execution-control.md) §5 | `example/harness/execution_control_smoke` ④ |
| **节点私有中断态** | 一个节点内做了 N 件带副作用的事、其中一件中断 → 重跑时跳过已做成的（**引擎层原语，不是 Option**）；做的事里要问人时每件的提问放进自己的相位（`EnterPhase`），否则跳过做完的那件会让后面的提问挪到它的 ID 上（累积传答案时拿到它的答案） | `compose_v2.SaveNodeState` / `GetNodeState` | godoc `compose_v2/node_state.go` | `example/harness/node_state_demo` |

## 3. 组合（业务接法）

§2 里除**节点私有中断态**（引擎层原语，由节点自己在 `NodeFunc` 内调）、**checkpoint**（经 ctx 配，
`compose_v2.WithCheckpointerCtx`）与**运行中插话**（收件箱经 ctx 交给这次 Run，`WithInboxCtx`，编排会把它交给
正在对话的成员）外，其余都是 BasicAgent 的 `Option`，按需叠加：

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("assistant"),   // 配了 WithHistory 时**必填**，见下
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(tk),
    // —— harness 横切能力，按需 ——
    agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{Enabled: true, Model: m, ModelContextWindow: 200_000}),
    agent_v2.WithStore(st),
    agent_v2.WithHistory(hs, "tenant-a"),
    agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
    agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcp.IsRemoteTool}),
    agent_v2.WithToolMiddleware(mw),
    agent_v2.WithAgentsMD("AGENTS.md"),
    agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
)

// 上面的 ApprovalAsk（以及 ask_user / skill HITL）要挂起再续跑，靠的是 cp
ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
// 会话历史另需声明**会话身份**：没有它框架每个 Run warn 一次并跳过写入
ctx = compose_v2.WithThreadIDCtx(ctx, "conv-42")
```

**这几处都不是可选风格**：

- **没配 cp**：中断照样抛给你，但现场**存不下来**（`graph_loop.go` 在 `checkpointer == nil ||
  sessionID == ""` 时直接跳过存档），之后 `agent_v2` 的 `Resume` 在入口就拒——
  `resume requires compose_v2.Checkpointer in ctx`（**普通 error 不是哨兵**，`errors.Is` 判不了；
  别跟 `ErrNoPendingCheckpoint` 混，那个是**给了 cp 但没挂起存档**才返的）。开了审批却不给 cp，
  等于把工具卡在半路又没有回来的路。
- **`WithHistory` 构造期 panic**：hs 为 nil / owner 为空且无 ownerFunc / 没配 `WithName`
  ——名字是历史里的**显示名**，会进模型 prompt，而自动名每次构造都不一样。
- **会话身份缺失是静默降级**（warn + 不写）：opt-in 了历史却拿不到记录，多半是漏了那一行。

## 4. 设计原则

- **横切、非绑定**：harness 在 `compose_v2` 图 + hook 层实现，不绑某个编排——一次实现、9 编排
  全受益（BasicAgent 接的 option，嵌进 Sequential / Parallel / Plan 等一样生效）。
  **一个例外是会话历史层**：一场会话一条流，所以写入器由**编排层**持有——嵌套时子 agent 自己的
  `WithHistory` 不生效（读写都沿用父的，且无日志），`GraphAgent` 因为没有静态子列表借不到配置、
  只能走 `WithHistoryCtx`。
- **可插拔 seam**：sandbox（`Sandbox.Exec`）/ Store（`store.Store`）/ Skill（`SkillBox`）/
  HistoryStore（`agent_v2.HistoryStore`）/ Checkpointer 都是"接口在使用方 + 业务自实现后端"，
  配 conformance 套验语义合规。
- **安全分层**：软防护（skill metadata 净化 + 沙箱输出不可信框定 + 工具审批）+ 硬隔离（后端
  沙箱负责逃逸 / 权限 / 资源）——详见 [sandbox.md §3](sandbox.md)。

## 5. 常见误配

harness 是横切层，误配的典型表现不是崩溃，而是**某个能力看起来开了、其实没生效**。按「框架会不会
告诉你」分成两档——**下半张表才是要主动去查的**。

### 会报错（框架挡住，照错误信息改即可）

| 你会看到 | 原因 |
|---|---|
| 构造期 panic：hs 为 nil / owner 为空 / **没配 `WithName`** | `WithHistory` 的三条 fail-fast，见 §3 |
| `tool middleware 调了 N 次 next——契约是至多一次` | 中间件里重试。每多调一次工具里的提问就换一个身份，用户答过的那条永远配不上（实测**永不收敛**）。要重试请在工具体内用 `compose_v2.RunWithRetry` 包住（每次尝试从同一个提问序号、同一个嵌套调用编号数起：答过的问题不再问，已完成的子 agent 调用拿回记下的结果；手写循环每次都算新调用，问题换身份重问、子 agent 从头跑） |
| `tool middleware 吞掉了控制流信号` | 中间件写了 `if err != nil { return fallback, nil }`。框架记下内层返过中断、发现你没原样传出就把原信号顶回去 |

⚠️ **这两条会互相遮蔽**（实测）：工具**本身会中断**时多调 `next`，你先看到的是「**吞掉了控制流信号**」
而不是「至多一次」——两个 guard 叠加，最外层那个先报。排查「至多一次」时别只搜这五个字，先确认
中间件有没有重复调 `next`。
| `[tool_not_revealed]` | toolsearch 是 **fail-closed** 的：deferred 工具没经 `tool_search` 揭示就硬调，不执行 |

### 静默（不报错，只是行为不对）

| 症状 | 原因 | 怎么办 |
|---|---|---|
| 开了审批/HITL，中断抛出来了，`Resume` 却报 `resume requires compose_v2.Checkpointer in ctx` | **没给 cp**——中断照抛，但 `graph_loop` 在 `checkpointer == nil \|\| sessionID == ""` 时直接跳过存档，现场没留下（实测确认）| 补 `WithCheckpointerCtx`（§3）。⚠️ 这个错是**普通 error 不是哨兵**，`errors.Is` 判不了；`ErrNoPendingCheckpoint` 是**给了 cp 但没挂起存档**时才返的，两者别混 |
| opt-in 了会话历史，一条记录都没有 | 没声明会话身份（每 Run warn 一次并跳过写入，**只有 warn**）| 补 `compose_v2.WithThreadIDCtx(ctx, convID)`；**补了仍为空就看下面两条**——历史层有三种误配的症状都是「没记录」 |
| 嵌套时给子 agent 单独配了 `WithHistory`，它的 store / owner / ConvID 全不生效 | **一场会话一条流**：子 agent 读写都沿用**父的**写入器，自己那份被忽略且**无日志** | 配在编排层；子的产出靠归属注解区分，不靠分流 |
| `GraphAgent` 那一层的历史全空 | 把 `WithHistory` 挂在图里某个 BasicAgent 上——那个写入器活在它自己的子 ctx 里，GraphAgent 这层看不到（实测该配法下图自己那层一条不记）| `GraphAgent` 只能走 `WithHistoryCtx` |
| 配了 `Durability=sync/async`，进程崩了照样什么都没恢复 | 配的是 `InMemoryCheckpointer`——每超步写出来的中途 cp 随进程一起没了 | 换 `redis.NewCheckpointer` / `sql.NewCheckpointer`（[checkpoint.md §2.4](checkpoint.md)）|
| 在中间件里改了 `args`，审批没有重新触发 | 顺序是 `memo → 渐进披露 → 审批 → [中间件链] → 工具`，**审批在中间件外面** | 要改参数用 `hook.Hooks.OnToolStart`——那里是消费档且在审批之前 |

⚠️ **上一条有安全含义，实测坐实**：中间件跑在审批**内层**，所以它改掉的是**用户已经批准过的参数**，
且不会二次审批。实测审批对话框显示 `{"amount":"10"}`、工具实际收到 `999999`。**中间件与被审批的
工具处在同一信任级**——把不可信的第三方中间件挂进链里，等于绕过审批。
| 工具体的中断在两次续跑之间"漂"了，答案配不上 | 中间件里**按条件**调了 `compose_v2.Ask`。它和工具体共用同一维计数器，条件一变工具体的 ID 就跟着移 | 中间件里的 `Ask` 要么恒调要么恒不调 |

## 6. 边界：相邻但不在 §2 表里的能力

下面几项常被一起提起，但按 §1 的判据（**主循环之外、支撑它长跑**）不算 harness，各有归属：

| 能力 | 为什么不在表里 | 去哪看 |
|---|---|---|
| 结构化输出 `WithOutputSchema` | 改的是主循环**产出的形态**（最终答复是 typed JSON），不是支撑长跑 | [structured-output.md](structured-output.md) |
| 模型故障切换 `fallback` | model 层装饰器，对 9 编排透明；解的是 provider 不可用不是长跑上下文 | [model-fallback.md](model-fallback.md) |
| observability | 让长跑**看得见**而非让它**跑得下去**；本仓单独成体系 | [observability.md](observability.md) |

⚠️ **`example/harness/` 目录的归类比本文宽**：`structured_smoke` 演示的结构化输出按上表不算
harness（`durability_smoke` 则对应 §2 首行 cp 的落盘时机，是表内的）。以本文为准。
