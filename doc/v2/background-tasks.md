# 长时间运行工具转入后台：WithBackgroundTasks

> **一句话**：业务点名的工具一次调用跑过 `YieldAfter`（默认 10 秒）还没返回，这次的 tool_result 先换成「已转入后台 +
> 任务 id」，这一轮接着往下走。工具完成后，发起它的执行在后续模型调用中可收到通知。模型可以用
> `background_task` 查询、等待、取消、列出。

需要将完整 Agent 派发到独立工作进程时，使用 [可恢复的后台子 Agent](durable-background-agent.md)。

---

## 0. 实现状态（2026-10 落地）

| 件 | 位置 |
|---|---|
| 管理器、配置、作用域、通知、`background_task` 工具 | [`agent_v2/background_task.go`](../../agent_v2/background_task.go) |
| option | `agent_v2.WithBackgroundTasks(cfg)`；`Config.BackgroundTasks`（[`agent_v2/config.go`](../../agent_v2/config.go)） |
| 集成 | Run / Resume 入口放作用域（`withBackgroundScope`）；tools 节点 `runOne` 里可后台的调用走 `BackgroundTaskManager.call`；模型节点注入通知、收尾节点记送达（[`agent_v2/basic_builder.go`](../../agent_v2/basic_builder.go) / `postambleFn`）；`withFrameworkTools` 注入 `background_task`；ToolFilter 挑「这一轮输入」与 AutoCompact 定尾部时跳过通知（`IsBackgroundTaskNotification`） |
| 结果渲染 | [`agent_v2/tool_render.go`](../../agent_v2/tool_render.go) `renderToolContent`：同步执行与后台任务结束时用同一份（`tool.Failure` 格式、转存、截断） |
| 测试 | [`background_task_test.go`](../../agent_v2/background_task_test.go) / [`background_task_regress_test.go`](../../agent_v2/background_task_regress_test.go) / [`background_task_internal_test.go`](../../agent_v2/background_task_internal_test.go) / [`background_task_crash_test.go`](../../agent_v2/background_task_crash_test.go)；真 qwen 冒烟 [`example/harness/background_smoke`](../../example/harness/background_smoke/main.go) |

适用于顶层 BasicAgent 和框架在图节点内明确派发的 BasicAgent。嵌套行为见[跨层回归](../../agent_v2/background_task_nested_test.go)。

---

## 1. 背景：长跑工具阻塞整轮

工具是同步的：`tool.Tool.Call` 返回才算这次调用结束。跑测试、构建、导出报表这类要几十秒到几十分钟的工具，只能让整轮
等着——模型不能先回复用户、不能并行做别的事；业务想不等，只能在工具里自己起 goroutine、自己管它什么时候结束、结果
怎么交回来，而这正是框架该管的生命周期。

缺的是一套契约：**慢的调用不阻塞这一轮**，结果出来时**送得回对话**，模型**查得到状态、取消得了**。

---

## 2. 设计

### 2.1 先同步等一会儿，到点再转后台

可后台的调用照常开始执行，tools 节点同步等它 `YieldAfter`：

- 在这之内返回：跟没开这个功能一样，结果当场交回（转存、截断、工具账照旧）；
- 到点还没返回、又没到上限（§2.7）：转入后台，这次的 tool_result 换成说明 + 任务 id（盖 `BackgroundTaskAnnotation`，
  值是任务 id），这一批照常收尾、这一轮接着往下走；
- 到点还没返回、但已到上限：不转后台，接着同步等，跟没开这个功能一样。

**为什么不让模型事先声明「这次放后台」**：那要模型预判耗时，猜错两头都亏——快的被放进后台、多绕一轮才拿到结果；
慢的同步跑、照样堵住整轮。先等一会儿再决定，快的当场拿结果，只有真的慢的才付后台的代价。

`WithToolTimeout` 在同步等的那段照常生效：`YieldAfter` 比工具超时长时，先到的是超时，按超时收尾（交给工具的 ctx 到点
取消，照样等它返回）。转入后台之后不再受工具超时约束，由管理器的 `MaxDuration` 兜底。

### 2.2 哪些工具：业务点名，点名的工具在脱离当前执行的 ctx 里跑

`BackgroundTasksConfig.Eligible func(tool.Tool) bool` 必填，默认一个工具都不能转后台。框架铸的工具（`ask_user`、
`read_tool_output`、`background_task` 自己……）永不转后台。

点名的工具**从一开始**就在一个脱离当前执行的 ctx 里跑，不论这次最后有没有转入后台——工具跑到一半没法换 ctx，而转入
后台之后节点已经结束，工具若还带着节点的执行状态，就是在往一次已经结束的执行里写（节点私有态、收上行信号处、提问
计数器），而且跟下一个超步并发。这个 ctx 是 `compose_v2.DetachExecution(context.WithoutCancel(callCtx))` 再去掉这次请求
的两样环境（`backgroundTaskCtx`）：任务可能比请求活得久，请求结束之后还用得到的东西不能指向这次请求。所以点名的工具：

- 不能提问：`compose_v2.Ask` 返回错误，不是中断；
- 不能做转交 / 路由决定；
- 不能用节点私有态（`compose_v2.SaveNodeState` / `SaveStepResult`）；
- 拿不到 session（`agent_v2.GetSession` 为 nil）与会话 ID（`compose_v2.GetThreadID` 为空）；
- 拿不到这次请求的编排钩子：两层（`compose_v2.WithOrchHooks` 与 `agent_v2.WithCtxOrchHooks`）都换成空的——它们往这次
  请求的流里写（agui 的 SSE、a2a 的事件），请求结束之后再写就写进了已经返回的连接。工具里调起的子 agent 的流式片段
  因此不再送到前端；
- 拿不到 checkpointer：工具里跑的嵌套 agent 中断时不留存档（留下也没人续）；
- 拿得到 store、调用主体（`compose_v2.GetOwner`）与业务放进 ctx 的别的值——包括 OTel span 这类观测上下文：工具里
  新建的 span 仍挂在这次请求的 trace 下。结束时渲染结果（转存走 store）与记日志用的是保留了这次请求全部值的 ctx，
  这次请求的 ctx 因此被后台 goroutine 引用到工具返回为止（最长 `MaxDuration`）。

需要前面几样的工具别点名；**持有这次请求的资源的工具也别点名**（比如挂着 agui adapter 的子 agent 包成的工具——它的
钩子按请求构造、往这次请求的流里写）。return-direct 的工具也别点名——转入后台之后它的结果只能作为通知送回，不再是
答复（框架会告警）。

同步等的那段里调用方的 ctx 被取消（Run 被撤），任务跟着取消，**照样等工具返回**、再带着 ctx 的错退出——跟同步执行
一样：续跑时这次调用不在工具账里、会再执行，不等的话两次执行可能重叠。转入后台之后不再跟调用方的 ctx。

### 2.3 执行范围

顶层 BasicAgent 和受支持的子调用配置 `WithBackgroundTasks` 后可转后台、使用 `background_task` 和接收通知。顶层通过
Run 或 Resume 进入，`DetachTask` 开出的独立任务也算顶层。嵌套支持框架在图节点内派发的编排成员和 Handoff 候选，以及
通过 `RunSubAgent`、`AgentAsTool` 或 Skill fork 调起的 BasicAgent。嵌套 Resume 经原调用点进入子 Agent 的 Run。

内部决策、无可寻址节点的调用和业务包装内直接调用的 Run 继续同步执行。经 `WithoutHistoryCtx` 明确关闭历史的嵌套分支也是如此。
嵌套实例在自己的消息流里取得句柄后可读结果。
需要在子调用结束前取得结果时，模型用 `background_task` 的 `wait`。子调用先结束时，工具继续运行，宿主用
`OnDone` 或管理器的 `Get` 处理结果。父模型、兄弟调用和后续新任务各自有独立的后台访问范围。

### 2.4 会话与子调用作用域

任务先按调用方身份和会话分组。身份优先取 `compose_v2.GetOwner`，其次取有效历史写入器的 Owner。会话优先取
`compose_v2.WithThreadIDCtx` 声明的 ID，否则用任务 RunID。两段分别哈希，和[转存](tool-output-spill.md)共用
`conversationScope`。没有会话 ID 时，顶层新任务查不到上一任务，也收不到其通知。

- 顶层控制工具可访问同一作用域中的顶层任务。顶层通知按 Agent 名定向。开了后台任务的 Agent 必须显式 `WithName`，
  以便跨请求构造时使用同一身份。
- 嵌套控制工具和通知还要求 RunID、子图 namespace 和 Agent 名匹配，且本次调用的已提交消息流中有原任务句柄。
  同一个 BasicAgent 被派发到不同位置时各自隔离。历史回填保留展示句柄，重试时通过本次执行取得访问资格。
- 后台作用域使用入口的调用主体和会话 ID。钩子更换其中任一值后，其派生调用走同步工具路径。后台中间件沿用准入时的
  主体和工具名，工具重定向由 `OnToolStart` 完成。

### 2.5 结束通知：怎么送、什么时候算送达

任务结束之后，开它的 agent 下一次调模型时，模型节点在 `OnModelStart` **之前**往消息流末尾追加一条 user 消息：

```
[后台任务通知] 任务 id="bg_…"（run_tests，参数 {…}）已完成，用时 2m3s。结果：
<结果正文>
```

（失败写「失败了」+ 错误；取消写「已取消」+ 原因。）消息上盖 `BackgroundTaskNotificationAnnotation`，值是任务 id。
它随这一轮的产出一起写进消息流（之后每一轮都在）与会话历史（记在这一轮的压缩边界之前——边界里的替代历史已经含着它，
排在边界之后的话下一次 Run 读回时会多出一份）。钩子看得到它，改不改由钩子。

**它不是用户说的话**：框架挑「用户这一轮的输入」的地方跳过它（`agent_v2.IsFrameworkNote`，连同工具重复调用提醒、
inline skill 正文这两类同样以 user 角色追加的框架提示一起认）——ToolFilter
收到的这一轮输入、AutoCompact 定尾部时找的「最后一条用户消息」。不跳过的话 ToolFilter 按通知而不是用户的提问裁工具，
AutoCompact 按通知定尾部、把用户刚问的话摘进摘要（审计实测）。业务钩子里自己挑「用户这一轮说了什么」时同样要跳过它。

模型节点和收尾节点在已提交的消息流中看到自动通知或 `background_task` 的终态结果后，确认送达。产出回滚时，结果
仍可由拥有该任务的执行再次取得。还在保留期内的顶层任务可将待送通知交给同一 Agent 的下次运行。嵌套任务只向发起它的那次子调用交付。
已归档的通知和工具结果沿用会话历史的可见性和保留规则。取消请求即时向工具发出取消信号，工具退出时间取决于它如何响应 ctx。

结果正文沿用同步工具的渲染。`tool.Failure` 使用 `[code] message`，超出 `ToolResultMaxBytes` 时截断。
顶层转存时通知带 `ToolResultSpillAnnotation`，模型用 `read_tool_output` 取全文。嵌套结果使用配置的转存阈值作为额外
大小上限，MicroCompact 可裁剪其文本。通用转存与 MicroCompact 卸载均跳过嵌套终态。
自动通知给出文本，`status`、`wait` 或 `cancel` 返回的终态工具结果可带大小预算内的图片和音频。后台任务直接渲染
终态，要求所有交付路径都脱敏时，在随工具执行的中间件处理。

渲染（转存走业务的 store）、结束日志、`OnDone` 都是业务代码，在后台 goroutine 里跑、没有调用方兜着：它们 panic 时框架
兜住——渲染 panic 记为失败（「整理工具结果时出错」），结局与计数照样落下。

### 2.6 `background_task` 工具

框架铸的工具（`BackgroundTaskToolName = "background_task"`），由框架每次 Run 注入、不写进业务的 toolkit；业务 toolkit
里有同名工具时框架那件生效、构造期告警。豁免审批、ToolFilter、tool_search、中间件、工具超时与截断（同别的框架工具）。

| action | 做什么 |
|---|---|
| `status` | 查一次，不等。结局已定时交出结局，消息提交后确认送达 |
| `wait` | 等它结束，最多 `timeout_seconds`（默认 30 秒，上限 `MaxWait`，默认 10 分钟）；调用方 ctx 取消时返回 ctx 的错，软取消触发时提前返回 |
| `cancel` | 取消工具的 ctx，最多等 2 秒。已结束时交出结局，仍在运行时返回取消请求的状态 |
| `list` | 列出当前作用域里的任务（最近 20 个），不交出结果 |

参数有问题或找不到任务时，模型收到 `invalid_arguments` 或 `not_found` 工具结果并可继续执行。钩子改变作用域后，已经绑定的控制工具返回 `unavailable`。

### 2.7 生命周期与上限：BackgroundTaskManager

任务归 `BackgroundTaskManager`，跟 Run 无关——Run 结束、续跑、放弃都不影响它。一个进程建一个、在开了后台任务的 agent
之间共用；进程退出前调 `Close`（取消所有还在跑的、等它们的工具返回，最多等到 ctx 结束；之后可后台的调用照旧同步执行）。

| 配置（`BackgroundTaskManagerConfig`） | 默认 | 作用 |
|---|---|---|
| `MaxRunning` | 64 | 整个管理器同时在跑的后台任务上限 |
| `MaxRunningPerConversation` | 8 | 一个会话内所有顶层与嵌套任务共用的运行上限 |
| `MaxDuration` | 1 小时 | 从工具开始执行计时，到期取消 ctx。工具因超时返回错误时任务记为失败 |
| `Retention` | 1 小时 | 结束了的任务保留多久；过期之后查询「没找到」、没送达的通知不再发 |
| `OnDone` | nil | 任务结束时在后台 goroutine 里调（panic 被兜住），给业务观测 / 落库 / 推送 |

会话任务记录的保留上限为 100。超过后优先清理最早转入后台且已结束的任务，正在运行的继续占位。查询、通知和转入后台时清理
当前会话。转入后台时至多每分钟做一次全局清理。

宿主有管理器的 `Get(id)` 与 `Cancel(id)` 接口。暴露到应用端点时由宿主检查用户权限。
每次 `Get` 与 `OnDone` 领取独立的 `Result`，包括其附件、元数据、动态数据容器和普通数据指针。
错误、map 键、锁及含私有状态的指针句柄保留身份。

Checkpointer 保存工具账中的句柄、已提交的消息和其中的结果。使用原管理器续跑时，同一子调用可用原句柄读取保留期内的任务。
执行中的 goroutine 和管理器持有的未交付结局留在当前进程。更换管理器或重启后，工具账仍重放原句柄，查询返回
`not_found`，框架不会据此重新启动工具。存档写入前进程崩溃时，业务副作用应按既有重试边界做幂等。
需要跨进程等待结局时，工具可调用外部持久作业系统或使用显式的[可恢复后台子 Agent](durable-background-agent.md)。

### 2.8 跟别的机制的关系

- **工具账（续跑不重放）**：「已转入后台」的那条 tool_result 跟普通产出一样进工具账。同一批里别的工具中断、续跑时
  这次调用命中账、不再执行，任务在管理器里接着跑。
- **工具中间件**：在后台 goroutine 里以脱离的 ctx 跑完整条链；中间件改写之后的结果就是渲染进通知的那份。
- **钩子**：`OnToolStart` / `OnToolEnd` 包住的是这一次调用——转入后台时 `OnToolEnd` 收到的是「已转入后台」那条结果，
  工具后来的真实结果不经过它（§2.5）。工具真正跑完的时刻由 `OnDone` 与日志（`agent_v2: 后台任务结束`）报。
- **压缩**：AutoCompact 定尾部（「最后一条 user 消息所在组及之后」原样保留）时跳过通知（§2.5），重复调用提醒、inline
  skill 正文这两类框架提示同样跳过（`agent_v2.IsFrameworkNote`）。

---

## 3. 公开 API

| 符号 | 说明 |
|---|---|
| `WithBackgroundTasks(cfg *BackgroundTasksConfig) Option` | 开启。`Manager`、`Eligible` 必填，缺了构造期 panic |
| `BackgroundTasksConfig{Manager, Eligible, YieldAfter, MaxWait}` | 每个 agent 的配置；`YieldAfter` 默认 10 秒，`MaxWait` 默认 10 分钟 |
| `NewBackgroundTaskManager(cfg *BackgroundTaskManagerConfig) *BackgroundTaskManager` | cfg 为 nil 时全用默认值 |
| `(*BackgroundTaskManager).Get(id) (BackgroundTask, bool)` / `Cancel(id) bool` / `Close(ctx) error` | 业务侧 |
| `BackgroundTask` | 快照：`ID` / `ToolName` / `Arguments` / `Agent` / `Owner` / `ConversationID` / `TaskID` / `Status` / `StartedAt` / `EndedAt` / `Result` / `Err` |
| `BackgroundTaskStatus` | `BackgroundTaskRunning` / `Completed` / `Failed` / `Cancelled`（`running` / `completed` / `failed` / `cancelled`） |
| `BackgroundTaskToolName` | `"background_task"` |
| `BackgroundTaskAnnotation` | 「已转入后台」那条 tool_result 上的标注，值是任务 id |
| `BackgroundTaskNotificationAnnotation` | 结束通知上的标注，值是任务 id |

`Completed` 包括业务工具声明的失败（`tool.Failure`）——那也是工具给出的结果；`Failed` 是工具返回 error、panic，或超过
`MaxDuration`。

## 4. 用法

```go
mgr := agent_v2.NewBackgroundTaskManager(nil)
defer mgr.Close(context.Background())

ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("devops"),
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(tk),
    agent_v2.WithBackgroundTasks(&agent_v2.BackgroundTasksConfig{
        Manager:  mgr,
        Eligible: func(t tool.Tool) bool { return t.Name() == "run_tests" || t.Name() == "build" },
    }),
)
ctx = compose_v2.WithThreadIDCtx(ctx, convID) // 跨 Run 收通知要有会话 ID
events, err := ag.Run(ctx, sess, schema.UserMessage("在后台跑一遍测试，跑完告诉我"))
```

真 qwen 跑通的完整例子见 `example/harness/background_smoke`：同一个 Run 里模型拿到任务 id 之后用 `wait` 等到结果；跨 Run
时 Run 在构建结束之前就结束，下一个 Run 里模型从通知读到构建结果。

## 5. 边界 / 限制

- 子调用结束后由宿主接收剩余的后台结果。希望模型在当次子调用内使用结果时，使用 `wait`。
- 存档恢复已写入的句柄和消息。工具体运行与未提交的管理器结果受进程生命周期和保留期约束。
- **没有会话 ID 时按任务分**（§2.4）：Run 结束之后的通知收不到。
- **点名的工具拿不到执行状态与这次请求的编排钩子、checkpointer**（§2.2）：不能提问、不能做转交 / 路由决定，工具里调起
  的子 agent 的流式片段不送到前端。
- **后台结果不经过 `OnToolEnd`**（§2.5）：钩子里做的结果脱敏对它不生效，放到工具中间件里。
- **任务结束时机不进 trace**：`OnToolEnd` 在转入后台时就收到了结果；工具真正跑完由 `OnDone` 与日志报。

## 6. 不变量与哨兵

| 不变量 | 哨兵（变异让它红） |
|---|---|
| 慢的可后台调用不阻塞这一轮；结果在下一次模型调用里送达且只送达一次 | `TestBackgroundTask_SlowToolYieldsThenNotifiesOnce`（不转后台 / 不发通知 / 送达不记账 / 通知不写进消息流） |
| 在 `YieldAfter` 之内返回的跟没开一样；没点名的照旧同步 | `TestBackgroundTask_FastToolUnaffected` / `TestBackgroundTask_IneligibleToolStaysSync` |
| 终态查询的送达发生在消息提交后 | `TestBackgroundTask_WaitDeliversResultNoLaterNotification` / `TestBackgroundTask_UncommittedWaitLeavesNotificationPending` / `TestBackgroundTask_WaitRewriteDoesNotRevealOriginal` |
| 取消真的取消工具的 ctx | `TestBackgroundTask_CancelStopsTool` / `TestBackgroundTask_CloseCancelsRunning`（Close 不取消） |
| 别的会话查不到、取消不了；通知只发给开它的 agent | `TestBackgroundTask_OtherConversationCannotTouch`（查询不看会话）/ `TestBackgroundTask_NotificationOnlyToStarter`（通知发给所有 agent） |
| 有明确归属的子调用可转后台，同名兄弟和重试的历史回填均隔离 | `TestBackgroundTask_UnaddressedNestedAgentRunsSync` / `TestBackgroundTask_NestedSiblingCannotReadOrCancel` / `TestBackgroundTask_NestedRetryCannotClaimHistoryHandle` |
| 续跑重放原句柄并按管理器存续状况查询结局 | `TestBackgroundTask_ResumeReplaysHandleWithoutRerun` / `TestBackgroundTask_NestedResumeRecoversAsyncResult` |
| 到上限照旧同步等 | `TestBackgroundTask_CapacityFallsBackToSync`（不查上限） |
| 后台工具 panic、结束时渲染 panic 都不让进程崩 | `TestBackgroundTask_PanicBecomesFailed`（panic 不兜）/ `TestBackgroundTask_RenderPanicDoesNotCrashProcess`（渲染 panic 不兜） |
| 送达按已提交的流里看到它记：那一轮没成就下次再发，成了不重发 | `TestBackgroundTask_FailedModelCallRedelivers` / `TestBackgroundNotes_DeliveredOnlyWhenSeenInCommittedStream`（注入即记送达 / 不观测送达）/ `TestBackgroundTask_SameSessionRerunDoesNotRedeliver` / `TestBackgroundTask_ReturnDirectRoundStillDelivers`（收尾节点不观测） |
| 同步段工具超时照常；调用方撤了任务跟着取消、等工具返回 | `TestBackgroundTask_ToolTimeoutBeforeYield` / `TestBackgroundTask_CallerCancelDuringSyncWindow` / `TestBackgroundTask_CallerCancelWaitsForTool`（撤了不等） |
| 点名的工具不带当前执行的状态，也碰不到这次请求的编排钩子 | `TestBackgroundTask_EligibleToolRunsDetached`（工具不脱离执行）/ `TestBackgroundTask_BackgroundToolDoesNotReachRequestHooks`（编排钩子不换空） |
| 开了后台任务的 agent 必须显式命名 | `TestBackgroundTask_RequiresExplicitName`（不要求命名） |
| 通知不是用户这一轮的输入 | `TestBackgroundTask_ToolFilterSeesUserInputNotNotification`（ToolFilter 不跳过）/ `TestBackgroundTask_NotificationDoesNotPushUserQuestionIntoSummary`（定尾部不跳过） |
| 结果被转存时通知带取回 id | `TestBackgroundTask_SpilledResultNoteCarriesID`（通知不带 id） |
| 通知在会话历史里排在压缩边界之前 | `TestBackgroundTask_NotificationRecordedBeforeCompactionBoundary` |
| 过期、超量的结束任务被清掉，在跑的不动 | `TestBackgroundTask_ExpiredAndExcessTasksSwept`（过期不清 / 不顺手清 / 不限保留个数） |

## 7. 决策记录

| 决策 | 理由 |
|---|---|
| 先同步等 `YieldAfter` 再转后台，不让模型声明 | 模型预判不了耗时；快的照旧当场拿结果，只有慢的付后台的代价 |
| 点名制、默认一个都不 | 点名的工具在脱离的 ctx 里跑、失去提问 / 决定 / 私有态 / session，这些是工具自己的语义，只有业务知道哪些工具能接受 |
| 点名的工具从一开始就脱离执行 | 跑到一半换不了 ctx；不脱离的话转入后台之后它在往一次已经结束的执行里写，且与下一个超步并发 |
| 后台 ctx 去掉这次请求的编排钩子与 checkpointer，store 与调用主体留着 | 任务可能比请求活得久：钩子往这次请求的流里写，请求结束之后再写就写进了已经返回的连接；后台里跑的嵌套 agent 中断时留下的存档没人续。store 与调用主体是工具自己的数据访问要用的，不指向这次请求 |
| 调用方撤了照样等工具返回 | 续跑时这次调用不在工具账里、会再执行，不等的话两次执行可能重叠——跟同步执行一致 |
| 要求显式命名 | 通知按 agent 名发给开它的 agent；自动生成的名字每次构造都不同，按请求构造 agent 时永远收不到 |
| 子调用通过已提交句柄接收结果 | 可恢复的执行位置区分兄弟，有效句柄区分同位置的不同尝试 |
| 会话配额与子调用访问分别检查 | 兄弟共享配额，每个子调用只访问自身的任务 |
| 通知作为 user 消息追加在末尾、写进消息流与历史 | 只放进这一轮的模型输入的话，下一轮它就从 prompt 里消失；写进流里之后每一轮都在，历史里也只一份 |
| 送达按已提交的消息流里看到它记 | 「模型已经看到」只有在那一轮的写入提交之后才成立，而之后每个节点读到的都是已提交的流：看到就是送达、没看到就是那一轮没成，跟任务、session 对象无关。曾经按「同一任务里记了送达 + 复查流里有没有」判，同一个 session 连续 Run（没开历史）时把新的一次 Run 当成被回滚、已送达的通知每次重发（审计实测） |
| 通知不当成用户这一轮的输入 | 它是 user 角色，却不是用户说的话；按它裁工具、定压缩尾部都会错（§2.5） |
| 到上限照旧同步等，不拒绝调用 | 拒绝会让一个本来一秒就完的调用因为别处的负载失败；同步等跟没开这个功能一样，是安全的退路 |
| 句柄随工具账持久化，执行和未交付终态由进程管理 | 续跑复用已经启动的调用，跨进程执行可交给持久作业系统 |
