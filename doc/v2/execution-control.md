# 执行控制：断点、节点超时与重试、审批分档、运行中插话、回合结束闸门

> 让人机协作和图编排更好用的五样能力。前两样在引擎层（`compose_v2`，GraphAgent 直接受益），后三样在
> BasicAgent（`agent_v2`）。本篇是它们的权威说明；godoc 是逐个 API 的细节，这里讲语义、边界和为什么这么定。
> 业内对照与调研记录见 [execution-control（三方对照）](../description/three-way-comparison/execution-control.md)。

## 0. 速查

| 能力 | 入口 | 一句话 | 跑一个 |
|---|---|---|---|
| 声明式断点 | `GraphBuilder.WithInterruptBefore/After`、RunOption `compose_v2.WithInterruptBefore/After`、GraphAgent `WithGraphInterruptBefore/After`；`compose_v2.AllNodes` 通配 | 跑到某个节点之前 / 之后停下，不改节点代码；续跑就是放行 | `example/harness/execution_control_smoke` ① |
| 节点超时 | `compose_v2.WithNodeTimeout(d)`（`AddNode` 的选项；子图节点用 `WithSubgraphNodeOptions`） | 每次执行尝试一个时限：到点取消节点的 ctx、照样等它返回，失败是 `*NodeTimeoutError` | 单测 `compose_v2/node_policy_test.go` |
| 节点级重试 | `compose_v2.WithNodeRetry(cfg)` | 节点失败后在同一超步里原地重试；记进节点私有态的步骤与 `RunSubAgent` 已完成的调用不重做，节点体里其余的副作用照常重放 | 同上 |
| 审批只读放行 | `agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites)`；只读声明 `tool.WithReadOnly()` / `tool.ReadOnlyDeclarer`；MCP `mcp.WithTrustedClient` | 只读工具直接执行，其余执行前问人；审批方式自动写进系统提示 | ② |
| 运行中插话 | `agent_v2.NewInbox()` + `agent_v2.WithInboxCtx(ctx, inbox)` | agent 执行过程中用户补充的话，在安全位置接进当前回合；这次调用续不了时放回收件箱 | ③ |
| 回合结束闸门 | `agent_v2.WithTurnGate(fn)` + `WithTurnGateMaxVetoes(n)` | 模型要收尾时先过一道检查，不满意就带着反馈让它接着干 | ④ |

## 1. 声明式断点

### 1.1 用法

```go
// 编译期：随图走（子图里也生效），每次 Run 都在
b := compose_v2.NewBuilder().WithName("pipeline").WithInterruptBefore("deploy")

// 运行期：只这一次 Run、只这一层图，跟编译期的取并集
err := compose_v2.Run(ctx, g, cm, compose_v2.WithInterruptAfter(compose_v2.AllNodes))

// GraphAgent：构造时给，Run 与每次 Resume 都带上
ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("pipeline"),
    agent_v2.WithGraphInputWriter(writeIn), agent_v2.WithGraphOutputReader(readOut),
    agent_v2.WithGraphInterruptBefore(compose_v2.AllNodes)) // 逐步执行
```

节点名必须是这张图的节点（或 `AllNodes`）：编译期写错 `Compile` 报错，运行期写错 `Run` 入口报错（不重试、不碰
存档），GraphAgent 写错构造期 panic。节点不能叫 `"*"`（那是通配）。

GraphAgent 配了断点（它自己的，或者图编译期声明的）时 ctx 上必须有 checkpointer：停在断点上只能经 Resume 续，Resume
要存档；没有存档时这个任务每次都停在第一个断点、永远过不去。Run 入口就报错（不可重试，一个节点都不执行）。直接调
`compose_v2.Run` 的顶层调用方没有 checkpointer 也能用断点，自己留快照续跑（§1.3）。

### 1.2 停在哪

断点判在**超步边界**上：

- **before**：这个边界即将执行的节点里有命中的，这个超步**一个节点都不执行**——包括同一超步里不命中的兄弟。停下
  的必须是一个完整的边界，存档才代表一个确定的位置。图的入口那一批也判。
- **after**：命中节点所在的超步**整体提交之后**停下（写入已生效、下一个超步还没开始）。同一超步里有兄弟中断或失败
  时，这个超步没有整体提交，after 等续跑把它补完的那一刻才停；部分失败时先成功的兄弟也算这个超步的成员。
- 同一个边界上 after 与 before 都命中时**只停一次**，一条挂起里两样都写。
- 图在那个超步之后就结束了的话不停（后面没有可以停在前面的东西，结果就是 Run 的返回）。
- 断点**紧跟着边界算出来就判**，排在这个边界上的软取消、ctx 取消、durability 中途写档之前。after 断点只在超步刚
  提交的那一刻判：别的停止先落了档，续跑恢复出这个边界时不再判 after，after 断点就被越过了。before 断点在恢复出来的、
  还没开始执行的边界上会重判（§1.3），但要先多停一次，而且那一次停下的不是断点挂起。

### 1.3 续跑就是放行

停下时存档留在可续跑的挂起档，frontier 就是这个边界。**续跑（带不带答案都行）恰好越过这一个边界**，之后的边界照常
判——循环回到同一个节点时会再停，逐步执行要的就是这个。顶层图没配 checkpointer 时（只有直接调 `compose_v2.Run` 的调用方能这样；
GraphAgent 必须配，见 §1.1）自己留快照续跑：

```go
snap, err := cm.Snapshot() // 停下之后
if err != nil {
    return err
}
// …
err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithResumeFrom(snap))
```

续跑恢复出来的那个边界判不判，看它是怎么来的（存档字段 `Checkpoint.FrontierStarted` 记着这一超步开没开始执行）：

| 恢复出来的边界 | before 断点 |
|---|---|
| 上次就停在它的断点上 | 不判——续跑就是放行 |
| 这一超步已经开始执行过（成员中断 / 失败之后续跑） | 不判——它的 before 早已过了；判的话答完问题续跑又停在同一个超步前面 |
| 还没开始执行（软取消、ctx 取消停在边界上，崩溃恢复） | **照常判这次 Run 的断点**（编译期 ∪ 运行期）——比如软取消先停一下、续跑时再挂上断点逐步看 |

after 断点只在超步刚整体提交的那一刻判，恢复出来的边界不判 after。

### 1.4 停下时拿到什么

Run 返回 `*compose_v2.InterruptError`，Reason 是 `compose_v2.BreakpointInterruptReason`（`"breakpoint"`）；它的
`AskQuery.Breakpoint` 写着 `Before` / `After`（命中的节点，去重排序）与 `NS`（哪一层图）。这个字段由框架填（`Ask`
会清掉业务填的）：非 nil 就是断点、不是节点在提问。直接构造 `InterruptError` 时别填它——框架会把那条挂起当成断点
对待（续跑时不提醒它没答）。

它随挂起清单进存档。自研 checkpointer 漏了这个字段时，引擎认不出上次停在断点上：停在 before 断点上的档每次续跑都
被重判、在同一个边界再停（第一次续跑的 ID 与原来的不同，之后固定），图永远过不去；停在 after 断点上的档续跑照常
放行（恢复出来的边界不判 after）。conformance 已断言往返。

InterruptID 由任务代号、ns、边界上的成员（实例身份与访问次数）、命中的节点派生：同一个任务里不同边界算不出同一个
ID；停下时算一次、随挂起清单进存档，续跑不重算。agent_v2 的 Resume、AG-UI / A2A 适配层按普通挂起处理它：AG-UI 把
Reason 透给前端；A2A 映射成 input-required，对端回一条文本或空消息即放行，回结构化的审批答复（`approved`）会被当成
形状不对、请对端重答（断点按普通提问处理）。

### 1.5 嵌套

- 运行期断点只管传给它的那一层图，不往嵌套图里传。子图（`AddSubgraphNode`）的断点在子图自己的 builder 上设，随子图
  在父图里生效；停下时中断照常冒到最外层，最外层续跑放行。
- 嵌在别的节点里、又没有 checkpointer 的图停在断点上**报错而不是停**：没有存档可续，续跑会从头再跑那张图、停在同一个
  断点，永远过不去（节点副作用每续一次重放一次）。给整棵图配上 checkpointer，或把断点设在顶层图上。
- GraphAgent 的断点只作用于这张图自己的节点；节点里调起的 agent 不受影响。其余编排（Sequential / Routing / …）不暴露
  断点——它们的内部节点名不是 API。

### 1.6 它不是什么

断点只说「停在这儿」：不带要人回答的问题，也不能在停下时改状态再继续。要人看着具体内容批准或拒绝某个动作，用节点里的
`compose_v2.Ask` 或 BasicAgent 的工具审批（§3）。闸门要拦下不走时，调用方不续跑即可（agent_v2 的 Resume 可以
`WithResumeAction(ResumeActionAbort)`）。

## 2. 节点超时与节点级重试

### 2.1 用法

```go
bld.AddNode("fetch", fetchFn,
    compose_v2.WithNodeTimeout(30*time.Second),
    compose_v2.WithNodeRetry(compose_v2.ExponentialBackoff(2, time.Second, 10*time.Second)))

// 子图节点
bld.AddSubgraphNode("enrich", sub, compose_v2.WithSubgraphNodeOptions(compose_v2.WithNodeTimeout(time.Minute)))
```

`WithNodeRetry` 的 `cfg.MaxRetries > 0` 时 `cfg.ShouldRetry` 必填，不给当场 panic（重试会把节点里的副作用再做一遍，
只有配重试的人答得上「这个错重跑有没有救」）；想用框架的保守谓词就传 `SimpleRetry` / `ExponentialBackoff`。

### 2.2 超时：取消信号，不是掐断

时限按**每一次执行尝试**计，重试时重新计时；没有「连同重试的总时限」——那个交给调用方 ctx 的 deadline。

到点时框架取消交给节点的 ctx（`context.Cause(ctx)` 是一个 `*NodeTimeoutError`，节点据此分得清自己超时还是调用方
喊停），然后**照样等节点函数返回**。响应 ctx 的节点（HTTP、数据库、模型调用）当场被解开；不看 ctx 的不会——框架不丢下
它往前走：被丢下的执行还在跑，它的副作用与对存档的写会发生在无人观测、也无人能拦的时刻。取而代之的是让失效可见：

- 超时之后过了几秒还没返回：`slog.Warn` 点名这个节点「它没有响应 ctx，图在等它」；
- 超时之后才返回成功：同样告警，并且这次结果**按超时丢弃**——超过时限的尝试就是失败的尝试，不看它最后交回了什么，
  也不看调用方那时死没死（时限是先到的）。

这次尝试以 `*NodeTimeoutError` 失败（外面包着带节点位置的 `*GraphNodeError`）。它**不在错误链上露出
`context.DeadlineExceeded`**：错误链上的那个错在本包里被当成「调用方的 ctx 到期了」的标签——出口把存档标成等续跑的
挂起、观测记成超时。节点自己超时是这一次执行失败了：配了重试就按谓词重试（默认谓词会重试），用完就是一个
普通的节点失败——同超步的兄弟照常跑完、成功的照常提交，存档是失败现场，续跑只重跑这个节点。节点返回的原始错误在
`NodeTimeoutError.Err` 里。

不改写成超时、原样往上传的：调用方的 ctx 已经死了——时限之前就死了，或者时限到了之后、节点带着错返回时已经死了（迟到
的成功不看这个，一律按超时丢弃）；节点抛了中断或别的必须上传的错（`MustPropagate`）；节点 panic 了。

### 2.3 重试：同一次节点执行再做一遍

重试在同一个超步里原地进行，不推进访问计数。每次尝试：

- 失败那次尝试的写入、`SendFrom` 投递、上行信号一律作废，只有成功那次的算数（每次尝试一个投递收集器）；
- 节点私有态（`SaveNodeState` / `SaveStepResult`）跨尝试保留：第一次尝试里已经做成、记下的步骤，第二次读回来跳过；
- 提问从同一个序号数起（`compose_v2.Ask` 算出同一个 ID，答过的不重问），嵌套调用从同一个编号数起
  （`agent_v2.RunSubAgent` 已完成的拿回记录、失败的续上它的存档）；节点里直接跑的嵌套图（`compose_v2.Run`）失败或
  中断过的、配了 checkpointer 时从自己的存档续，已经跑完的存档随成功清掉、下一次尝试从头再跑它（要越过它就跑完用
  `SaveStepResult` 记下、后面的尝试跳过，或经 `RunSubAgent` 调）——这些是 `RunWithRetry` 的语义，节点重试就是在
  节点体外面套了一层它；
- 交给前端的流（chunk 事件）：失败那次交出、下一次没再采用的逐个 `reset_stream` 撤回（被拿回的嵌套调用当初交出的照留，
  它的完成记录声明了自己的流）；下一次拿回了说不清对应哪些流的东西（从存档续跑、命中节点私有态、命中没用
  `compose_v2.WithStreams` 声明流的步骤记录）时一个都不撤——宁可留着作废的，也不删掉被采用的。判据与编排成员重试是同一个
  （`compose_v2.StreamAttempts`）。节点体在重试之间自己缓存了产出、不再流出时，用 `compose_v2.RecordStreams` 登记它当初的
  run_id，或用声明了流的步骤记录记它。暂停（中断）之后的续跑不撤上一次的流：续跑是接着办。

不重试的（框架的闸，排在谓词之前）：中断、调用方的 ctx 已经取消或到期（问的是调用方的 ctx，不是错误链上有没有
ctx 错）、软取消、陈旧写回、终局失败、坏存档（`ErrCheckpointNotResumable`）、档没写成（`ErrCheckpointNotPersisted`）；
谓词否决的当然也不重试。节点的钩子（`OnNodeStart` / `OnNodeEnd`）在所有尝试外面各
触发一次；每次重试打一条 `slog.Warn`（节点、第几次、上一次的错）。出边的条件函数报错不归重试管（那是
`WithCondFailPolicy`）。

### 2.4 子图节点

配了 checkpointer 时子图节点的重试 / 超时有一个好性质：子图失败时它的存档留着，下一次尝试从子图自己的存档续跑，子图里
已经跑完的节点不重跑。被超时取消时：子图里响应 ctx 的节点在这一超步中途以 ctx 错失败，存档记着这一超步已经开始
执行（`FrontierStarted` 为真，等续跑的挂起档）；不响应的跑完这一超步，子图在下一个超步边界停下、存档。没配时子图每次尝试从头跑。

### 2.5 没做的

- **心跳续期的空闲超时**（有进度就续期，适合长时间流式生成的模型节点）：要把「进度」沿 ctx 传进嵌套图、按执行状态
  归类，面比墙钟时限大得多，先做墙钟时限。记在 backlog。
- 被超时取消后不配合的节点：不提供「放弃等待」的选项（理由见 §2.2）。

## 3. 审批：只读放行档 + 审批方式写进提示词

### 3.1 只读声明

只读性由**工具实例自己声明**：实现 `tool.ReadOnlyDeclarer`（`ReadOnly() bool` 返回 true），或 `tool.NewFuncTool(…,
tool.WithReadOnly())`；`tool.IsReadOnly(t)` 判。**没声明就按有副作用处理**——拿不准就问，而不是拿不准就放行。声明的是
这个工具的能力、不是某一次调用：参数决定危险性的工具（执行 SQL、跑 shell）别声明只读，按参数判用
`WithApprovalHeuristic`。准不准业务自负。

MCP 工具：tools/list 里的 `annotations` 会解析进 `ToolSpec.Annotations`（`RemoteTool.Annotations()` 读得到），但 server
的只读声明**默认不认**——MCP 规范说注解是提示，不能凭不受信任的 server 的注解做决定。只有经 `mcp.WithTrustedClient`
注册的 server，`readOnlyHint=true` 的工具才算只读；同时声明了 `destructiveHint=true`（自相矛盾）按有副作用处理。
注解逐个工具宽松地解：某个工具的 `annotations` 形状不对（字段类型错之类）只丢掉它自己的注解（按没有注解处理、不算
只读）并告警，不影响这个 server 的其他工具。

### 3.2 `ApprovalAskWrites` 怎么判

```
这次调用记下的结论（续跑重放，不看现在的配置）
  └→ 没有：档位默认（只读 → 放行；其余 → 问）
        └→ 配了启发式：它返回 Allow / Ask / Deny 就以它为准（Ask 连只读工具也问、Deny 连只读也拒），Default 才用上一步
              └→ 问：这次调用已准入过 → 本次答复 → approve_always 规则 → 抛审批中断
```

只读判定是「每次调用只判一次」的一部分：结论记下之后，续跑时工具改了声明也不推翻。业务一个工具都没声明只读时，这一档
等同 `ApprovalAsk`，不会意外放行。框架工具（ask_user / activate_skill / final_output 等，按实例认）照常豁免。

`approve_always` / `approve_always_tool` 在这一档照常生效：那是用户明说的「这件事 / 这个工具以后别问」，档位只决定默认。

### 3.3 审批方式写进系统提示

审批生效时（非 `ApprovalAuto`，或配了启发式），BasicAgent 在前言（系统提示）末尾写一段【工具审批】：哪些调用会停下来等人
（按档位）、需要批准的直接调用就行（系统会去问）、别先在回复里征求同意、别换别的工具绕开、被拒（结果以 `[tool_denied]`
开头）别原样重试、换更稳妥的做法或向用户说明。不列工具名：哪些调用要批准是每次调用判一次的（启发式、approve_always
规则都参与），工具清单还可能随渐进披露变化，静态清单会跟实际对不上。

这一段在前言里：每次模型调用都在、不进会话历史、不被压缩。`WithApprovalPrompt(text)` 替换文案，传空串不写。

拒绝结果给模型的文案也带上了同样的指引（用户拒绝 / 策略拒绝两种措辞）。

### 3.4 PlanAgent 现造的执行体

PlanAgent 的 `TmpSubAgent` 由框架代造，此前没法配审批档，拿着 `WithPlanToolkit` 工具池里的写工具不问任何人。
`WithPlanTmpSubAgentOptions(opts...)` 给它们加 BasicAgent 选项（典型是 `WithApprovalMode`）。审批档只对设置它的那个
BasicAgent 生效，编排里的每个 BasicAgent 各自设置。

## 4. 运行中插话

### 4.1 用法

```go
inbox := agent_v2.NewInbox()
ctx = agent_v2.WithInboxCtx(ctx, inbox)
go func() { for text := range userTyping { _ = inbox.Push(schema.UserMessage(text)) } }()
events, err := agent.Run(ctx, sess, input)
leftover := inbox.Drain() // 回合结束之后才到的：作为下一次 Run 的输入
```

`Push` 只收 user 角色（框架回灌给模型的消息只能是 user）；收进来的是一份独立的拷贝（内容块、标注一并拷），打
`InterjectionAnnotation`；存不进存档的（标注里有 JSON 编不了的值）当场报错。

### 4.2 在哪儿取

BasicAgent 每次调模型之前取一次——接在消息流末尾、工具结果之后，`OnModelStart` 与四层压缩之前（钩子与压缩都看得见它）。
模型给出最终答复、而这一轮之后还能再调模型时再取一次：答复期间到的接在答复后面、再调一次模型，不留到下一次 Run。配了
回合结束闸门时，闸门节点开头再取一次：模型答复之后、闸门之前到的接进来，回去回应它，这一次不问闸门（§5.2）。后两次在
最后一轮不取（没有下一轮回应它，留在收件箱）；调模型之前那一次最后一轮照取。插话不会插进一批工具调用的中间，也不会塞进正在进行的那次模型调用或工具执行里。

ReAct 的出边规则：消息流以「模型还没回应的插话（或闸门反馈）」结尾时再调一次模型。只认这两个标记，不按角色认——钩子把
答复改写成 user 角色的消息时照旧收尾。

### 4.3 谁取

当前正在对话的那个 agent：收件箱经 ctx 交给编排的成员（Sequential 的一步、Handoff 当前接手的那个、Plan 的执行步……），
先到安全位置的那个取走；并发的成员（Parallel 的分支）谁先到谁取，一条只被取一次。**不交给**内部决策方（编排的路由
分类器、评审 agent，以及业务交给编排的路由函数、路由决策方、规划器、Reflector / EvalFunc、Loop 的退出判断、回合结束
闸门——它们里面请教的 agent 也一样；一句话不该改写它们的判断）、被当成工具调用的子 agent（`RunSubAgent` / `AgentAsTool` / skill fork——用户
是在跟调用它的那个 agent 说话）、`DetachTask` 开的独立任务。

### 4.4 取走之后

取走的插话接进消息流，就是这次执行的进度：

- 配了 checkpointer：随存档走。取走的那一刻就记进节点私有态，那次模型调用随后失败或中断，续跑时原样接回（存档跨进程
  也带着）。
- 这次调用（一次 Run 或一次 Resume）以续不了的方式结束——没配 checkpointer 时报错，或者终局失败（`ErrRunAborted`，存档
  已清）——它这次新取的插话按原顺序放回收件箱最前面：调用方重跑、编排的成员重试（没有存档、从头再来）会再取到它，调用方
  也能 `Drain` 出来。已经记进会话历史的不放回：历史是 append-only 的，重来那次从历史上文里看得到它，放回的话会出现两遍。
  先前那次调用取走、随被清掉的存档作废的也不放回（Resume 终局失败时只放回这次 Resume 新取的）。
- 只管这个 agent 自己那次调用：它成功之后、或者它以可续的错结束之后，外层失败、整个从头再来或终局清掉整棵存档时，它取走的
  插话跟它的进度一起作废。

没被取走的一直留在收件箱。最后一次取在模型的最终答复之后（配了闸门时在闸门开头），之后才到的赶不上：最后一轮、工具直接
收尾（return-direct）的回合、闸门放行之后、postamble 期间到的都算。Run 返回之后 `Drain` 出来作为下一次 Run 的输入；续跑
（Resume）时把同一个收件箱放进 ctx 就接着收。

接进来的插话进会话历史（开了 `WithHistory` 时）与会话事件（排在它回应的那条最终答复之前）：没开历史时，下一个任务的上文
与转交的下一个接手者从事件里看得到用户说过它。插话还重置工具重复调用的计数链：用户换了方向，之后的重复不算原地打转。

压缩：消息流末尾是刚接进来、模型还没回应的插话（或闸门反馈）时，AutoCompact 原文保留的那一段从它前面那一轮算起——插话
说的就是那一轮的工具结果，那一轮被摘要掉，模型只能对着摘要回应。

## 5. 回合结束闸门

### 5.1 用法

```go
ag := agent_v2.NewBasicAgent(agent_v2.WithName("support"), agent_v2.WithModel(m),
    agent_v2.WithTurnGate(func(ctx context.Context, in agent_v2.TurnGateInput) (string, error) {
        if !strings.Contains(in.Final.Content, "工单号") {
            return "答复里要带上工单号", nil // 非空 = 不准结束，这句话交给模型
        }
        return "", nil // 放行
    }),
    agent_v2.WithTurnGateMaxVetoes(2))
```

`TurnGateInput` 有 `Final`（这次想以之结束的答复，就是 `Messages` 的最后一条）、`Messages`（到此刻的消息流；不含 agent 的
前言即系统提示，长期记忆注入、调用方自己塞进消息流的 system 消息照样在）、`Vetoes`（这一回合——这个 agent 的这次调用，
连同它的续跑——已经否决过几次）。每道闸门拿一份独立的拷贝：改它不影响交给调用方的答复、消息流与历史，也不影响下一道
闸门。返回的反馈去掉首尾空白之后为空就是放行。「继续」和「为什么继续」是同一个返回值：不存在「要求继续却没说为什么」的状态（那样模型会在同样的输入上把
同样的答复再说一遍）。配了多个闸门时按注册顺序都问，所有非空的反馈都交给模型（各成一条消息），谁先谁后不影响继不继续。

### 5.2 什么时候问、什么时候不问

- 只拦模型的**自然收尾**（最终答复不带工具调用）。以工具直接收尾的回合（final_output、return-direct 的业务工具、转交 /
  路由）不经过闸门——那是工具定义的结束。
- 轮次预算的最后一轮不问：那已经是最后一次能调模型的机会，否决了也没有下一轮（`WithMaxRounds` 优先）。否决也消耗轮次：
  在倒数第二次能调模型时否决，下一次就是摘掉工具的收尾调用，之后 Run 以 `MaxIterationsError` 结束（答复在 events 里）
  ——否决上限要设得远小于轮次预算。
- 这一回合（这个 agent 的这次调用，连同它的续跑；随存档走）否决次数到上限（默认 `DefaultTurnGateMaxVetoes` = 3）不再问、
  直接放行——闸门是成本放大器，判据写错（永远不满意）时要有尽头。同一个 agent 再被调用一次（新任务、编排里再轮到它）
  从 0 数。最后一次否决时框架在反馈后面另补一条消息「这是最后一次被要求继续」：放在最后一次否决的时候而不是放行之后，
  因为放行之后不再调模型，写了也看不到；那一条只给这一回合的模型看、不进会话历史（下一回合从 0 数，那句话对它不成立）。
- 模型答复之后用户又插了话：先回去回应插话，这一次不问闸门、不算一次否决。

### 5.3 闸门里问人、请评审

闸门跑在自己的节点（`turn_gate`）里，每道闸门有自己的相位：里面可以 `compose_v2.Ask` 让人复核这次答复（续跑时只重跑闸门、
不重调模型；对新答复再问是新问题、新 ID），可以经 `RunSubAgent` 请一个评审 agent。闸门是内部决策：它请教的 agent 的对话不进会话历史、不取用户的插话（同编排的
评审 agent）。闸门返回的错照常往上传（本次 Run 失败、
存档可续），拿不准时放行还是拦下由闸门自己决定，框架不替它吞错。

闸门节点只在配了闸门时建：图结构只随构造期配置变，Run 与 Resume 是同一张图。

### 5.4 反馈消息

反馈是 user 角色的消息、打 `TurnGateFeedbackAnnotation`，进消息流与会话历史（它不是从历史里能重算的东西），不进会话事件
（事件只记用户说的话与最终答复）。「最后一次」那一条另打 `DerivedInputAnnotation`，不进历史。压缩时它们跟插话一样：
末尾是还没回应的反馈时，原文保留的那一段从被否决的那条答复所在的那一轮算起（同 §4.4）。

## 6. 决策记录

| 决策 | 选择 | 理由 | 放弃的做法 |
|---|---|---|---|
| 断点的判定对象 | 超步边界上整个超步；before 命中则一个成员都不执行 | 存档要代表一个确定的位置；只停命中的那个会让兄弟跑掉、存档成半个超步 | 只停命中节点、兄弟照跑 |
| 续跑不重复停 | 只判新算出的边界；恢复出来的边界按 `FrontierStarted` + 是否停在断点上分三种 | 存档里存的是算好的下一批成员，恢复即放行是结构性的；只有「还没开始执行、也不是停在断点上」的边界才需要按这次 Run 的配置重判（运行期断点每次 Run 可以不同） | 持久化「已放行」的边界 ID（部分失败续跑时 frontier 被收窄、ID 对不上）；按档位 `Status` 推断（Status 只是报告位） |
| 断点与软取消等同一边界的停止 | 断点先判 | after 断点只在超步刚提交那一刻判，别的停止先落档的话续跑时不再判 after、它就被越过；before 会重判，但要多停一次、而且那一次停下的不是断点挂起 | — |
| 编译期 vs 运行期断点 | 取并集 | 编译期的是图的一部分（闸门），不该被一次调用关掉；运行期的是调试时加的 | 运行期整体替换编译期 |
| 没有 checkpointer 的断点 | 嵌套图停下时报错；GraphAgent 在 Run 入口报错；直接调 `compose_v2.Run` 的顶层不限（调用方自己留快照续） | 停下之后无档可续：嵌套图续跑必然活锁，GraphAgent 的 Resume 必然失败、重跑又停在第一个断点 | 照停（活锁 / 永远停在第一个断点）、静默忽略（接受了却不生效） |
| 节点超时 | 协作式：取消 ctx、等返回、失效可见、迟到成功丢弃 | Go 不能安全中止 goroutine；放弃等待会让副作用与存档写发生在无人能拦的时刻，还要给每个写入口加栅栏 | 超时后放弃 goroutine、靠栅栏挡迟到写 |
| 超时错误的形状 | `*NodeTimeoutError`，不在链上露出 `DeadlineExceeded` | 节点超时是一次执行失败，不是调用方 ctx 到期；露出来会被出口判成挂起（存档标「等续跑」而不是失败现场） | 直接返回 `ctx.Err()` 的包装 |
| 节点重试 | 节点体外套 `RunWithRetry`，每次尝试一个投递收集器 | 复用已有的「同一次调用」语义（编号退回、账本分层、信号按尝试收、嵌套图续跑）；投递收集器不分开的话失败尝试的投递会漏进结果 | 在节点里手写循环（编号不退回，答过的问题重问） |
| 只读的声明者 | 工具实例自声明（布尔），没声明按有副作用 | 只读性是审批要的唯一信息；fail-closed 防「加了危险工具忘了声明就静默绕过」 | 按工具名单、按工具类别枚举、借 trace 类别 |
| MCP 注解 | 默认不认，`WithTrustedClient` 逐 server 显式信任 | 注解是 server 自报的提示；认了的话一个 server 给写操作标只读就绕过了审批 | 默认信任 |
| approve_always 在只读档 | 照常生效 | 用户明说的例外优先于档位默认 | 在这一档把「总是允许」降级成一次性 |
| 审批方式进提示词 | 前言里一段「会发生什么、该怎么做」，不列工具名 | 一个任务里审批配置不变，前言是稳定前缀；工具清单与逐次判定会让静态列表失真 | 在每个工具描述里标「需审批」 |
| 插话的投递点 | 调模型之前 + 最终答复之后 | 这两处都没有在飞的工具调用、tool_use / tool_result 配对完整；放进 tools 节点会撞上并发扇出 | 工具执行中途注入 |
| 插话谁取 | 正在对话的成员，决策 LLM / 被当工具的子 agent / 独立任务不取 | 插话是给对话方的；交给分类器会让一句话改写路由判断 | 只给顶层 agent（编排里收不到）|
| 插话的持久点 | 取走即记进节点私有态、随存档走；这次调用续不了（没配存档时失败、终局失败）就把这次新取的放回收件箱最前面，已经记进会话历史的除外；没取的留在收件箱 | `Push` 成功就是对调用方的回执，这一层能管的范围内不静默丢：有存档时存档是它的家；没有存档时这次执行整个作废，它回到调用方手里；记进了会话历史的由历史带着（再放回就是两遍，审计实测）。只记私有态的话没配存档时失败、成员重试之后它就没了（审计实测，重试成功时 `err == nil`）。外层随后终局清掉整棵存档时成员取走的跟着作废——要管那一层得把成员的账往上交，没做 | 收件箱随存档落盘（调用方持有的对象，框架不拥有它的生命周期）；只在取走它的节点失败时放回（提交之后整次执行作废时照样丢） |
| 插话进会话事件 | 收尾时写进事件、排在最终答复之前；来源是专门的 channel | 没开历史时上文从事件读，不记的话上文里只剩一条在回应一句不存在的话的答复 | 按消息列表里的标记扫（列表含 prior，上一个任务的插话同样带着标记）|
| 内部决策点的 ctx | 路由函数 / 路由决策方 / 规划器 / Reflector·EvalFunc / Loop 的退出判断 / 闸门 / 编排的决策 agent 统一一个 `decisionCtx`：不进会话历史、不取插话（产出是内容的回调——Merger / Aggregator / ExecInputBuilder——不在此列） | 下游只要判定：评审请求进历史会被下一回合当成用户说过的话，插话被决策方取走就到不了对话方 | 逐处各自摘（漏过好几处：函数形态的决策方里请教的 agent 写进了历史）|
| 闸门的返回 | `feedback string`，非空即继续 | 「继续」与理由绑在一个值上，结构上消灭「要求继续却没说为什么」 | `{Continue bool; Reminder string}` 两个字段 |
| 闸门的现场 | 每道闸门一份深拷贝 | 直接交 channel 里的消息指针时，闸门改了答复，调用方拿到的跟历史里记的分叉（审计实测） | 约定「只读」、不拷 |
| 闸门的位置 | 独立节点 | 闸门要写消息、可能问人；放在 model 节点里问人的话续跑会重调模型、拿到的是另一份答复 | 塞进 model 节点或条件边（条件边不能写、不能中断）|
| 闸门与轮次预算 | 最后一轮不问；否决有上限，最后一次否决另补一条只给这一回合看的提示（不进历史） | 两个判据读同一份轮次状态，必须显式排优先级；没有上限的闸门是成本放大器；提示说的是这一回合的次数，进了历史就成了下一回合的假话 | 让闸门自律；把提示拼进反馈那一条（反馈该进历史，提示不该） |
| MCP 注解解析 | 逐个工具宽松解，形状不对丢它的注解并告警 | 注解只是提示，一个写错的提示不该让整个 server 的工具都不可用 | 整份强类型解析（一处写错全废，相对加注解之前是回归）|
| 压缩与待回应的插话 / 反馈 | 原文保留段从它们前面那一轮算起 | 插话说的是那一轮的工具结果、反馈说的是那条答复 | 按最后一条 user 起算（那一轮被摘要掉）|

## 7. 测试与冒烟

- `compose_v2/node_policy_test.go`、`compose_v2/breakpoint_test.go`、`agent_v2/graph_agent_breakpoint_test.go`、
  `agent_v2/approval_readonly_test.go`、`mcp/annotations_test.go`、`tool/readonly_test.go`、`agent_v2/interjection_test.go`、
  `agent_v2/turn_gate_test.go`、`agent_v2/decision_scope_test.go`、`agent_v2/autocompact_pending_tail_test.go`；每条关键
  判据都做过变异验证（改坏对应代码，对应用例红）。
- 存档 conformance 断言了 `AskQuery.Breakpoint` 与 `Checkpoint.FrontierStarted` 的往返。
- 真 qwen 冒烟：`go run ./example/harness/execution_control_smoke`（四个场景）。
