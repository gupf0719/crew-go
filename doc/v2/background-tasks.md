# 长时间运行工具转入后台：WithBackgroundTasks

> **一句话**：业务点名的工具一次调用跑过 `YieldAfter`（默认 10 秒）还没返回，这次的 tool_result 先换成「已转入后台 +
> 任务 id」，这一轮接着往下走；工具在后台跑完之后，结果在开它的 agent 下一次调模型时作为一条通知送进对话。模型可以用
> `background_task` 查询、等待、取消、列出。

---

## 0. 实现状态（2026-10 落地）

| 件 | 位置 |
|---|---|
| 管理器、配置、作用域、通知、`background_task` 工具 | [`agent_v2/background_task.go`](../../agent_v2/background_task.go) |
| option | `agent_v2.WithBackgroundTasks(cfg)`；`Config.BackgroundTasks`（[`agent_v2/config.go`](../../agent_v2/config.go)） |
| 集成 | Run / Resume 入口放作用域（`withBackgroundScope`）；tools 节点 `runOne` 里可后台的调用走 `BackgroundTaskManager.call`；模型节点注入通知、收尾节点记送达（[`agent_v2/basic_builder.go`](../../agent_v2/basic_builder.go) / `postambleFn`）；`withFrameworkTools` 注入 `background_task`；ToolFilter 挑「这一轮输入」与 AutoCompact 定尾部时跳过通知（`IsBackgroundTaskNotification`） |
| 结果渲染 | [`agent_v2/tool_render.go`](../../agent_v2/tool_render.go) `renderToolContent`：同步执行与后台任务结束时用同一份（`tool.Failure` 格式、转存、截断） |
| 测试 | [`background_task_test.go`](../../agent_v2/background_task_test.go) / [`background_task_regress_test.go`](../../agent_v2/background_task_regress_test.go) / [`background_task_internal_test.go`](../../agent_v2/background_task_internal_test.go) / [`background_task_crash_test.go`](../../agent_v2/background_task_crash_test.go)；真 qwen 冒烟 [`example/harness/background_smoke`](../../example/harness/background_smoke/main.go) |

仅对 **BasicAgent** 生效，且只在**顶层**生效（见 §2.3）。

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

### 2.3 只在顶层生效

BasicAgent 的 Run 入口判「这次调用在不在另一个正在执行的任务里」（跟判新任务是同一个判据，`enclosingTask`）：不在里面的
那个才转后台、才注册 `background_task`、才收通知；Resume 只在顶层调，恒算顶层。编排成员（Sequential 的一步、Handoff 的
候选……）、被当工具调的 agent（`AgentAsTool` / `RunSubAgent` / skill fork）照旧同步跑点名的工具。

理由：结果要有一个**确定会再来、又不会跟兄弟撞上**的模型调用接住。顶层 agent 是一场对话里面向用户的那个，下一次
用户说话它一定会跑；嵌套的 agent 这一次调用结束之后可能再也不跑（一次性的子任务），或者跟同名的兄弟并发（并行分支、
同一批两个 tool_call 调同一个 AgentAsTool），通知不知道该发给谁。

经 `agent_v2.DetachTask` 开的独立任务不在原任务里，算顶层；a2a/server、agui 直接 Run 的 agent 是顶层。

### 2.4 作用域：调用方身份 + 会话

任务按「调用方身份（`compose_v2.GetOwner`）+ 会话（`compose_v2.WithThreadIDCtx` 声明的 ConvID；没有时是任务的 RunID）」
分，两段按哈希——跟转存（[tool-output-spill.md](tool-output-spill.md)）同一把尺（`conversationScope`）：

- **查询、等待、取消、列出**只认当前作用域里的任务；别的会话拿着任务 id 查不到（返回「没找到」，不透露它存在）。任务 id
  会出现在模型可见的文本与会话历史里，不能靠它保密。
- **结束通知**只发给开它的那个 agent（按 agent 名）。同一场会话里别的 agent 收不到；开它的 agent 下一次跑时收到。
  所以开了后台任务的 agent 必须显式 `WithName`（构造期检查，缺了 panic）：自动生成的名字每次构造都不同，按请求构造
  agent 时（agui / a2a 的常见用法）下一次请求永远收不到通知。

没有会话 ID 时按任务分：Run 结束、开了新任务（`NextTask`）之后就查不到、也收不到了。跨 Run 收通知要声明会话 ID。

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

**送达按「已提交的消息流里看到它」记，只记一次**：

1. 模型节点每轮开始时、收尾节点（return-direct 收尾也经过它）读到的消息流都是已提交的——里面有这条通知就记为送达。
   注入它的那一轮没成（模型调用失败、写入被回滚）时之后读到的流里没有它，下次再发。跟哪个任务、哪个 session 对象
   无关。
2. 那一批工具中断、任务随后被放弃时不算送达（对话没有往下走），下一个任务里再发；开了会话历史时通知已随那一轮记进
   历史，下一个任务的 prior 里就有它，同样记为送达、不再重发。
3. 模型已经经 `background_task` 拿到过结局（status / wait / cancel 返回了结局）的，不再通知。

结果正文跟同步执行时同一份渲染（`renderToolContent`）：`tool.Failure` 按 `[code] message`，超过转存阈值的转存、超过
`ToolResultMaxBytes` 的截断；return-direct 的豁免不再适用（结果只是给模型看的信息）。结果被转存时通知带上取回 id
（`ToolResultSpillAnnotation`），模型照样用 `read_tool_output` 取全文。后台任务结束时**没有 `OnToolEnd` 这一步**，结果
当场按大小处理——结果脱敏这类改写要放在工具中间件里（在后台 goroutine 里跟工具一起跑），放在钩子里对它不生效。

渲染（转存走业务的 store）、结束日志、`OnDone` 都是业务代码，在后台 goroutine 里跑、没有调用方兜着：它们 panic 时框架
兜住——渲染 panic 记为失败（「整理工具结果时出错」），结局与计数照样落下。

### 2.6 `background_task` 工具

框架铸的工具（`BackgroundTaskToolName = "background_task"`），由框架每次 Run 注入、不写进业务的 toolkit；业务 toolkit
里有同名工具时框架那件生效、构造期告警。豁免审批、ToolFilter、tool_search、中间件、工具超时与截断（同别的框架工具）。

| action | 做什么 |
|---|---|
| `status` | 查一次，不等。结局已定时交出结局并记为已送达 |
| `wait` | 等它结束，最多 `timeout_seconds`（默认 30 秒，上限 `MaxWait`，默认 10 分钟）；调用方 ctx 取消时返回 ctx 的错，软取消触发时提前返回 |
| `cancel` | 取消（取消工具的 ctx），最多等 2 秒看它停下；停下了交出结局，没停下（工具不响应取消）说明一句、结束后照常通知 |
| `list` | 列出当前作用域里的任务（最近 20 个），不交出结果 |

参数有问题、任务找不到时返回给模型看的失败结果（`invalid_arguments` / `not_found`），不让整轮失败。描述里提示模型
不要反复用 status 轮询。

### 2.7 生命周期与上限：BackgroundTaskManager

任务归 `BackgroundTaskManager`，跟 Run 无关——Run 结束、续跑、放弃都不影响它。一个进程建一个、在开了后台任务的 agent
之间共用；进程退出前调 `Close`（取消所有还在跑的、等它们的工具返回，最多等到 ctx 结束；之后可后台的调用照旧同步执行）。

| 配置（`BackgroundTaskManagerConfig`） | 默认 | 作用 |
|---|---|---|
| `MaxRunning` | 64 | 整个管理器同时在跑的后台任务上限 |
| `MaxRunningPerConversation` | 8 | 一个作用域里同时在跑的上限 |
| `MaxDuration` | 1 小时 | 一个任务最长跑多久（从工具开始执行算），超了取消，记为失败 |
| `Retention` | 1 小时 | 结束了的任务保留多久；过期之后查询「没找到」、没送达的通知不再发 |
| `OnDone` | nil | 任务结束时在后台 goroutine 里调（panic 被兜住），给业务观测 / 落库 / 推送 |

一个作用域最多留 100 个任务（在跑的不丢），超出时从最早转入后台、已经结束了的丢起。清理是顺手做的：查询、通知、转入后台时清当前作用域，转入
后台时每隔一分钟顺手把所有作用域过期的清掉。

业务侧另有 `Get(id)`（快照）与 `Cancel(id)`（比如用户在界面上点了取消），不看作用域。

**不落盘**：进程重启之后任务与结果都没了。续跑时转入后台的那次调用命中工具账、不再执行，模型看到的还是同一个任务 id；
那时任务若已随进程消失，模型再查得到「没找到」。

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

- **嵌套的 agent 不转后台**（§2.3）。编排成员要后台执行，目前只能在顶层 agent 里做。
- **不落盘**（§2.7）。要跨进程恢复，业务在 `OnDone` 里自己落库，或把长任务交给外部作业系统、工具只提交 / 查询。
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
| 模型经 `background_task` 拿到结局后不再通知 | `TestBackgroundTask_WaitDeliversResultNoLaterNotification`（wait 之后不记送达） |
| 取消真的取消工具的 ctx | `TestBackgroundTask_CancelStopsTool` / `TestBackgroundTask_CloseCancelsRunning`（Close 不取消） |
| 别的会话查不到、取消不了；通知只发给开它的 agent | `TestBackgroundTask_OtherConversationCannotTouch`（查询不看会话）/ `TestBackgroundTask_NotificationOnlyToStarter`（通知发给所有 agent） |
| 嵌套的 agent 同步跑、拿不到 `background_task` | `TestBackgroundTask_NestedAgentRunsSync`（嵌套也转后台） |
| 续跑不重放转入后台的调用 | `TestBackgroundTask_ResumeReplaysHandleWithoutRerun` |
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
| 只在顶层生效 | 结果要有一个确定会再来、不会跟兄弟撞上的模型调用接住；嵌套的 agent 两条都不保证 |
| 作用域跟转存同一把尺 | 两者回答同一个问题「这是谁的、属于哪场对话」，各写一份会漂 |
| 通知作为 user 消息追加在末尾、写进消息流与历史 | 只放进这一轮的模型输入的话，下一轮它就从 prompt 里消失；写进流里之后每一轮都在，历史里也只一份 |
| 送达按已提交的消息流里看到它记 | 「模型已经看到」只有在那一轮的写入提交之后才成立，而之后每个节点读到的都是已提交的流：看到就是送达、没看到就是那一轮没成，跟任务、session 对象无关。曾经按「同一任务里记了送达 + 复查流里有没有」判，同一个 session 连续 Run（没开历史）时把新的一次 Run 当成被回滚、已送达的通知每次重发（审计实测） |
| 通知不当成用户这一轮的输入 | 它是 user 角色，却不是用户说的话；按它裁工具、定压缩尾部都会错（§2.5） |
| 到上限照旧同步等，不拒绝调用 | 拒绝会让一个本来一秒就完的调用因为别处的负载失败；同步等跟没开这个功能一样，是安全的退路 |
| 不落盘 | 工具的执行本身在进程里，进程没了任务就没了；把记录落盘只会留下一个永远不会结束的任务。需要跨进程的交给外部作业系统 |
