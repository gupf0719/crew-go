# 转交控制信号：五方对照

> 2026-08-19，四路独立调研（能力授予形态 / 框架控制状态隔离 / 并发裁决与信号持久 /
> 派发点收敛）的合并记录。本文是**对照与决策记录**；我们自己的问题清单与待决项见
> [ISSUE-99](../../issue/issue-99-handoff-control-signal.md)。
>
> 读源码路径：LangGraph / eino / openai-agents-python / agent-framework(MAF) / crewAI
> 均在 `~/program/go/src/` 下，本地读，勿 WebFetch。

## 1. 控制态放哪 / 业务能否寻址

| | 控制态放哪 | 业务能否寻址 | 同实例多挂 | 能力载体 |
|---|---|---|---|---|
| **LangGraph** | checkpoint channel | **不能**：节点返回的 dict 只有 state schema 声明过的 key 变 write（`graph/state.py:1461`）；保留名编译期拒（`pregel/_validate.py:23`）；引擎层 `apply_writes` 再兜一次（`_algo.py:296`） | 允许 | 无「转交能力」概念；`Command(goto)` 不校验发起方资格（`_io.py:56`） |
| **OpenAI Agents** | 框架自己 dataclass 的私有属性 `RunState._current_agent`（`run_state.py:200`） | **不能**：业务容器是 `Session` 协议（`memory/session.py:14`），**窄化到只装对话条目**，装不下任意 KV | 允许 | 发起方的 `handoffs` 清单（`agent.py:305`），对目标零写入 |
| **MAF .NET** | `ScopeId(executorId, scopeName)`，executorId **不在方法签名里**（`ScopeId.cs:15`） | **不能**：句柄 `BoundWorkflowContext` 把身份焊死（`InProcessRunnerContext.cs:347`），`private sealed` | 允许 | executor 持有的 `ChatClientAgentRunOptions`（`HandoffAgentExecutor.cs:114`），每次调用当参数传 |
| **MAF Python** | `State` 扁平 KV，`_` 前缀**纯约定、`set()` 零检查**（`_workflows/_state.py:19`） | 能 | 允许（clone 副本，`_handoff.py:261`） | clone + 注入工具 + middleware |
| **eino** | `AddSessionValue` 扁平 KV，key 是**无前缀导出常量**（`planexecute/plan_execute.go:243` 的 `PlanSessionKey = "Plan"`） | **能，完全裸奔** | **禁**（`chatmodel.go:721`，连两个 Sequential 分支都禁） | 目标对象的字段 `subAgents`/`parentAgent`（`chatmodel.go:438`） |
| **crewAI** | 不存在该状态（委派=同步调用返字符串，`delegate_work_tool.py:16`） | — | 允许 | 本次调用的 `tools` 参数 |

**共同点**：除 eino 外，控制态一律**不在业务拥有的容器里**。四家把它挪到两个位置之一——
**框架独占的句柄**，或 **LLM 说不出口的词汇表**（state schema / 工具白名单 / 目标集）。

## 2. 「业务 new 存储」这个前提

**五家全部成立，且没有一家给业务受限视图。** LangGraph 的 `BaseStore` / `BaseCheckpointSaver`、
eino 的 `CheckPointStore`、MAF 的 checkpoint storage、crewAI 的 `FlowPersistence` 都是业务
`new` 之后交给框架，业务保留原始句柄。

唯一有「受限视图」性质的是 OpenAI 的 `Session` 协议——机制不是权限检查，是**能力裁剪**：
接口类型只能承载对话条目，框架想把 `_current_agent` 放进去也放不进去。

**LangGraph 不但不在意业务改 checkpoint，还开了公开 API**：`update_state(config, values, as_node=...)`
（`pregel/main.py:2515`）允许冒充任意已存在节点写入——但仍过该节点 writers 的 schema 白名单。
**它的边界不是「谁能写」，是「能写出什么词」。**

## 3. 保留命名空间守卫：两家上线了，框架照写

- **LangGraph**：`_validate_namespace` 拒根标签 `"langgraph"`（`store/base/__init__.py:1263`），
  但**只在 `put`/`aput` 上调**；`batch`/`abatch` 全放行且被注释标注为 internal
  （`store/memory/__init__.py:206`）。
- **MAF**：`_is_internal_file` 守卫只长在**工具**上（`_harness/_file_memory.py:320`），
  框架自己走 `self.store.write` 原始句柄（`:291`）。

**共同形态：守卫加在「不可信方被迫经过的窄口」，框架另走一条不设防的原始路径。**

⚠️ **前提差异**：这两处的不可信方都是 **LLM**（字面上只能通过工具/schema 动作）。
若不可信方是**同进程的宿主语言代码**（闭包里就捕获着存储句柄），同一手法失效。

**五个仓库的 `SECURITY.md` 全部只讲漏洞上报流程，没有一家把「同进程业务代码」列进威胁模型。**

## 4. 并发多信号裁决

| | 裁决方式 | 跨中断存活 | 工具备忘 |
|---|---|---|---|
| **LangGraph** | **不裁决**——`Command.goto` 翻译成 channel writes，多个是并集全跑（`state.py:1749`；`tool_node.py:862` 多 Command 全保留、多 PARENT 拼接）。例外：`Command(graph=PARENT)` 走异常冒泡，并发时 `done.pop()` 从 set 弹出**顺序任意**（`_runner.py:650`） | 自动——信号即 writes，writes 即 cp 内容 | **有，单位是 task 的 writes**（`_loop.py:736`）；**不区分工具**，因控制流与状态更新在持久层是同一种东西 |
| **eino** | **执行前**、单线程 pre-handler、按 tool_call **下标**定出唯一赢家 `ReturnDirectlyToolCallID`（`adk/react.go:412`）。赢家事件推迟到 post-handler 单线程发射 ⟹ 事件流上恒为最后 ⟹ 编排层 `lastAction` 判据确定（`flow.go:505`） | **不靠重跑**：赢家身份由 pre-handler 从消息**重算**；payload 在 checkpointed `State`（`react.go:47` + `graph_run.go:525/657`）。自测 `interrupt_test.go:1937` 专门覆盖 | **有**：`ExecutedTools map[callID]output` 进 cp（`tool_node.go:306`）。**按结果不按身份，transfer 无豁免** |
| **OpenAI** | **显式取 `run_handoffs[0]`**，其余合成 "Multiple handoffs detected, ignoring this one." tool_result + trace error（`turn_resolution.py:426`）。handoff **不并发**；**有任何中断则整轮不执行 handoff**（`:713`） | `ProcessedResponse.handoffs` 有序列表序列化进 `RunState`（`run_state.py:1296`），续跑重取 `[0]` | **有，但不是缓存**：判据是「历史里有没有该 call_id 的 `function_call_output`」（`turn_resolution.py:1104`） |
| **MAF** | **让它不可能发生**：给 handoff agent 克隆配置时 `allow_multiple_tool_calls = False`（`_handoff.py:285`）。handoff 工具体被 middleware 短路、从不执行（`:130/332`） | 目标写在 tool_result 内容里（`HANDOFF_FUNCTION_RESULT_KEY`），扫消息读回（`:508`）；消息队列进 cp（`_checkpoint.py:79`） | 未找到（handoff 不执行故无需备忘） |
| **crewAI** | 不存在该情形（委派是普通同步工具，tool_call 顺序循环无并发，`llm.py:1097`） | 不适用 | 未找到 |

**五家没有一家把控制流信号放在「per-attempt 新建、按完成顺序写入的 ctx 可变槽」里，
也没有一家在「执行之后」裁决。** 裁决全部发生在执行之前，判据是 LLM 输出的**有序 tool_call 列表**。

**「有工具级 memo、同时又要求某类工具必须重跑」这个组合，五家一个都没有。**
（LangGraph 整节点重跑，但它根本没有工具级 memo，不是同一个取舍。）

## 5. 工具内部跑 agent，怎么防它影响外层控制流

**没有一家用「摘槽」。**

- **eino**：`agentTool.InvokableRun` 返回类型是 `(string, error)`（`agent_tool.go:152`）——内层 Action
  **没有回路可走**，不需要摘。外加位置门 `exactRunPathMatch(runCtx.RunPath, event.RunPath)`
  （`flow.go:530`）：嵌套事件路径严格更长 ⟹ 不匹配 ⟹ Action 被忽略。**按位置判不按 opt-out flag 判，
  业务手写的工具自动落在正确的一侧**；路径铸不出来（`RunStep.agentName` 未导出）。
- **OpenAI**：`as_tool` 内层是**另一次完整的 run**（`agent.py:864`），控制态是 run 循环的局部变量；
  连审批态都刻意不共享（`agent.py:630`）。
- **LangGraph**：**反着来**，明确允许任意工具 `Command(graph=PARENT, goto=...)` 影响父图
  （`tool_node.py:896`）——取舍是「不区分工具的权限档位」。

## 6. 控制信号的多条来路

**MAF 有一条比 annotation 兜底还宽的**：任何函数结果里出现 `"handoff_to"` 就转交，
连 JSON 字符串都解（`_handoff.py:507`），**不看是哪个工具产的**。

**但它的兜底不在通道上，在目标集上**：

```python
if handoff_target not in self._handoff_targets:
    raise ValueError(f"... attempted to handoff to unknown target '{handoff_target}'. "
                     f"Valid targets are: {', '.join(self._handoff_targets)}")   # _handoff.py:396
```

**伪造出来的信号最多只能选中一个本来就合法的候选**——「多一条来路」的危害被压缩成
「在已授权的候选里换一个」，而不是「任意导向」。这是本次调研里最值得带走的一条设计手法。

eino 同样有公开的第二来路：`SendToolGenAction` 是导出的（`adk/react.go:274`），任何工具
一行就能发转交——但那是**文档化的扩展点**不是漏洞。

OpenAI / LangGraph 各只有一条来路（`handoff_map` 只认工具名 / `Command` 只能从返回值来）。

## 7. 能力授予：焊在哪、什么时候焊

| | 焊在哪 | 什么时候 | 业务能否自实现 | 代价 |
|---|---|---|---|---|
| **eino** | **目标对象的字段** | `SetSubAgents` 写字段，首次 Run 兑现成工具并 `frozen=1`（`chatmodel.go:1398`） | **不能**：不实现导出的 `OnSubAgents` 就静默无能力；**`struct{ Agent }` 业务壳静默丢**，零提示 | 两层宿主；**候选不能是子树**（实测：候选是 Sequential 时子树内无人拿到工具且不报错）；同实例多挂全禁 |
| **OpenAI** | 发起方的清单，目标零写入 | 无装配期，每 turn 现读（`turn_preparation.py:88`） | **能，零配合** | 转交靠换 current_agent，作用域靠嵌套 run 收 |
| **MAF Python** | **副本对象** | `build()` | **不能，硬拒**（`_handoff.py:695` `TypeError`） | 只对自家 `Agent` 类可用；clone 与原件状态分叉 |
| **MAF .NET** | **挂载点（executor）持有的 RunOptions**，agent 只读 | 装配期算，**每次调用当参数传** | **能** | per-mount 对象要真实存在；候选只能是单 agent |
| **crewAI** | 本次调用的 `tools` 参数 | 每 task 派发前现算（`crew.py:1780`） | **能**（鸭子类型） | 目标靠 role 字符串匹配；门只有一个布尔 |

**关键判断：「装配期 vs per-call」不是那条真正的轴。** MAF .NET 和 crewAI 都是 per-call，
成本却是零。真轴是 **「持有能力的那个东西，是不是就是发起那次调用的那个东西」**——
MAF .NET 的 executor 持候选名单、自己发起那次调用——这里原记作传递路径长度 **0**、故不需要任何自查；issue-99 §5.5 修订为 **1**，而且有两个洞（嵌套 workflow 当候选时 `options` 声明了却不读、官方推荐的装饰器基类无条件透传给内层）。

## 8. 决策记录

| 日期 | 决策 | 依据 |
|---|---|---|
| 2026-08-19 | **撤销 store 层保留 `__` 前缀守卫** | 不可信方是同进程业务代码而非 LLM，闭包捕获自造句柄即绕过；五家无一在此前提下防住，也无一试图防 |
| 2026-08-19 | **订正**「四家无一做 per-call 授权」 | MAF .NET / crewAI 都是 per-call 且零成本 |
| 2026-08-19 | **订正**「三方的代价是一实例只属一位置」 | 只有 eino 付，且付得更多（连普通编排多挂都禁）；另三家自动允许 |
| 2026-08-19 | **订正**「加了守卫框架自己也写不进去」 | LangGraph / MAF 都上线同款守卫，框架另走 internal 原始路径；原实测只说明守卫加在了唯一一条路上 |
| 2026-10-08 | **转交 / 路由决定改成跟着写入者的提交走的上行信号**（`compose_v2/signal.go`）：写进做出它的那段执行，节点成功并提交才并入图的已提交状态（随存档落盘），图成功跑完时交给调用它的那段执行；失败 / 中断 / 被否决的作废；工具账与嵌套调用的完成记录把那段执行期间收到的随产出记下、拿回时交出。删掉 ctx 收件槽、路由的 session KV、嵌套调用的效果记录器与 `tool.ControlEffectMetaKey` 声明机制 | §4 的对照：LangGraph 的信号就是 writes、writes 就是 cp 内容，跨中断「自动」活下来；五家没有一家把控制信号放在「每次尝试新建、写入当场生效」的可变槽里。我们的槽 / KV 写入不等写入者提交，实测四种静默错误：写入者那一步已提交、续跑不再执行时丢失；同批兄弟工具中断时丢失；失败那次尝试写下的照样生效；并发两路写同一格后写者赢。**没照抄的部分**：LangGraph 的 `Command(graph=PARENT)` 走异常冒泡、子图当场中止，我们保持「候选跑完再裁决」——候选可以是一整个编排（分诊之后还有确认），决定随它的提交往上走，在消费方那一层按收件人取。种类做成不导出的变量（`NewSignalKind` 同名只能铸一次）挡正常入口；**防伪造的是收件人**（见下一行） |
| 2026-10-08（审计后） | **一个收件人上是单值格子：先后做出的后者为准，并发做出的必须一致**（不一致报 `SignalConflictError`：不可重试、不清档）；transfer 工具与 `HandoffTo` 合成同一类决定；部分失败超步里成功成员的信号先记在一边、整超步完成时合并；**收件人由引擎铸**（`SignalRecipient`：这个任务的密钥 + 位置 + 分域，随存档走）；结果是「没成」的工具调用（超时放弃、`tool.Failure`）里做出的作废；Reflect 被评审否掉那一轮的决定作废 | 第一版的两条规则一紧一松都错：「同一次调用里不一致一律报错」把 Sequential / Loop 的先后修正当矛盾（整个 Run 失败，改动之前是后者为准）；「程序化取最后一个 + 工具路径优先」把并行两支的矛盾静默挑掉，赢家随声明顺序走、部分失败续跑后翻转。单值格子的语义接近本仓单写入者 `LastValue` 定下的那条：跨超步后者覆盖；同一超步里 `LastValue` 两个写入者一律拒，信号只在值不同时报冲突、而且不清档。冲突不清档：清档会删掉兄弟分支正等着人回答的挂起档（本仓 2026-08-25 就为转交冲突定过这一条）。收件人原先是 `"handoff:"+ns`，种类不导出挡不住手写的信号 JSON——三处授予边界被绕过（审计实测），所以防线挪到收件人上。**没做**：路由的「资格」仍是环境（分类器那段执行里任何 route_to 都算数，含它裸调的路由 agent），没有像转交那样做成签名——见 todo |
| 2026-10-08（第二轮审计后） | 同一超步里冲突与兄弟的提问并存时，**提问照记进存档、返回冲突**；冲突**让位于**同超步的终局失败（撞 ID、单写入者、成员自己的 `ErrRunAborted`、坏档）；RoutingAgent 分类器这个吞点改走 `MustPropagate` + 冲突；收件人放进 ctx 时包遮罩；防护范围写明到「正常入口」为止 | 冲突排在记录提问之前时，调用方只拿到冲突、存档里没有那条提问（实测）；排在终局判定之前时，终局失败的清档被吞，留下一份每续一次就再撞一次的档（实测）。报冲突不报中断，是因为冲突的两方若是从记录里拿回的，续跑必然再撞，那时答了也白答。分类器曾手写判据，软取消 / 终局失败 / 坏档被降级成兜底（软取消那种续跑直接走兜底分支，实测）。打印 ctx 会把裸字符串的收件人原样打出来（实测四处边界被绕过）。密钥就在存档里，故意读出来自己算的挡不住，也不该挡：同进程业务代码不是信任边界（issue-99 §5.4）。当时仍开着的三项后来都修了：密钥懒铸、落盘晚于子层的决定（崩溃 / 写档失败后续跑认不出）→ 见下面「密钥」那行；冲突的两方从记录里拿回时任务过不去 → 见 2026-10-09 那行；`OnToolEnd` 把结果改成失败不作废决定 → 见「同一批 tool_call」那行 |
| 2026-10-08（第二轮审计后，密钥） | **收件人密钥一个任务一把**：顶层新开时铸，经 ctx（遮罩）交给全部嵌套图沿用，每张图的档都带一份；中间层的档没写成时续跑从上层继承回同一把；顶层的档没写成、只能新开时任务代号也换，子层旧档过不了续跑闸门（`BelongsToTask`），决定由重跑重新做出。不为密钥多写任何一次档 | 每张图各铸一把时，消费方那层的档没写成（exit 档硬崩、写档失败）而子层的档已落盘，续跑时消费方新铸一把、子层档里已提交的决定发给旧收件人，静默丢失（实测）。LangGraph 在非 exit 档下跑之前先写一份输入档、task ID 由位置确定性派生——身份不依赖「哪份档先写成」。我们的等价物是让密钥不属于任何一张图的档：从任务继承。考虑过「第一次铸时同步写一次档」，否掉：给每个 Run 多一次写，而 exit 档本来就承诺不在中途写；试做时还让一条异步写档的哨兵卡死、写档次数 / 顺序的断言变红，exit 档的探针照样失败 |
| 2026-10-08（第二轮审计后，路由资格） | **路由能力改成授予（签名），不再是环境**：RoutingAgent 经 RunOption 只授予分类器这个对象，`WithRouteTool` 降成 opt-in（两把闸都开才注入 route_to）；编排经 runMember 自动转授给成员、HandoffAgent 转给候选，业务壳显式 `RegrantRouteTo`；GraphAgent 节点 / BasicAgent 工具与钩子里显式 `RegrantFromNode`（转交授予也走它） | 资格是环境时，分类器在同一段执行里请教的任何带 route_to 的 agent 都替它定了路由（壳请教 helper 定 math、自己定 chat ⟹ 走 math，审计实测）；转交那侧早已按授予做，路由是唯一的例外。§7 那条判据「持有能力的那个东西，是不是就是发起那次调用的那个东西」在这里同样成立：RoutingAgent 亲手调分类器，授予的传递路径长度为 0。节点里转授为什么不是「自动给节点调起的所有 agent」：那正是环境的形状；节点要显式点名交给谁，跟业务壳调 `RegrantRouteTo` 是同一个动作。`RegrantFromNode` 取最近放进来的那一份：BasicAgent / GraphAgent 在 Run 入口放自己收到的，框架调起另一个 agent 的四个入口给被调方放空的。第一版按「ns 相同」判本层——ns 只是「是不是同一层」的代理量，子图节点、节点里直接 Run 的嵌套图跑在子 ns 里被当成孙辈，显式转授静默落空（同日审计实测），改成在边界上遮住 |
| 2026-10-08（第二轮审计后，同一批 tool_call） | **同一批 tool_call 对同一件事给出不同决定：排在前面的生效**，后面那个调用的结果换成一条「没有生效」的失败结果（`agent_v2.SupersededDecisionErrCode`，带着它原本的结果；`compose_v2.JoinSignalsFirstWins`）；**同一超步的成员间照旧报冲突**。作废按 `OnToolEnd` 改写之后的结果判（超时也一样，不再无条件作废），钩子改成失败的那次也不触发 return-direct 短路 | OpenAI Agents SDK 取第一个 handoff，其余回一条 tool output 告诉模型「忽略了这一次」并记 span error，从不抛异常；LangGraph 的 ToolNode 把多个 Command 交给图，第一个 `ParentCommand` 抛出即胜出；MAF 干脆对 handoff agent 关掉并行 tool call（我们不关：并行 tool call 是性能特性，关掉也挡不住别的工具里的 `HandoffTo`）。批内有一个续跑不翻转的次序（已提交那条回复里的顺序），发起者是收得到反馈的模型；并行分支两样都没有，所以仍报冲突。旧做法（批内也报冲突）实测续跑从工具账原样重放、必然再撞，任务永远过不去，批里先问了人的话答完也卡在冲突上。作废看钩子之后：`OnToolEnd` 的 `Result` 是 [消费] 档，模型看到的是改写之后那条，按钩子之前判两个方向都错（改成失败对话照转、改成成功决定已作废，实测）；只改作废不改短路时作废之后最终答复为空（实测）。被盖掉的那个在全部 `OnToolEnd` 之后才换成失败结果——它依赖前面那几个作不作废，只能最后定，代价是钩子看到的不是最终进历史的那条。**第三轮审计（同日）补四条**：钩子之后失败只让框架控制工具不短路，业务 return-direct 工具照常以失败内容收尾（对所有工具都不短路时，业务 exit 工具一声明失败模型就被再调一次，实测）；「算不算失败」与渲染由同一函数从改写后的 Result 推出（判失败只看 Success、渲染失败格式却要 Error 非空时，钩子整体替换 Result 忘填 Success ⟹ 模型看到「已转交」、转交却作废，实测）；被盖掉的调用按路原子（按条丢弃时结果说失败、它另一个决定照样生效，实测）；OnToolEnd 钩子的决定排在批的决定之后（批在全部钩子之后才交时，工具更早的决定盖掉钩子看过结果之后的决定，实测） |
| 2026-10-09 | **同一超步只能有一个成员做决定**：两个成员都对同一个收件人做了决定（值相同也算）⟹ 图的设计错误，`SignalConflictError`（类型本身带着 `ErrRunAborted`：不重试、清档，在记录提问与提交之前返回，跟 `LastValue` 单写入者冲突同一个位置）；**`ParallelAgent` 的分支不再拿转交 / 路由能力**。推翻 10-08 的「并发不一致才报、不清档」与「值一致算一次」；todo 的方向 (b)（按否决推进重做代际）不做，(c)（合并点与扇出仲裁两处优先级不一致）随之消失 | 根因（实测）：分歧被当成「这一超步失败」处置——整超步不提交、靠续跑重跑全部成员换一个一致的决定；而完成记录 / 工具账存在的意义正是让重跑拿回同样的结果。于是决定来自记录时路由模型始终只调 1 次、次次再撞；没有记录时扣款 2→4→6，连跟决定无关的兄弟也 1→2→3 重做（普通的部分失败只重跑失败那支）。再下一层：一个单值决定被交给了多个并发决定方，分歧时谁也没有资格裁决。业内一致是在结构上只留一个决定方：OpenAI Agents SDK 只有当前 agent 自己的模型输出能 handoff（`as_tool` 嵌套运行不能替父转交，同一轮多个取第一个）；MAF 转交参与者必须是单个聊天 Agent，且关掉并行 tool call；eino 只认本次执行自己 run path 的动作（子执行、并行分支的转交到不了父级）；CrewAI 的路由顺序执行，只有普通监听器并行；LangGraph 单值 channel 同超步收到两个值（相同也拒）报 `INVALID_CONCURRENT_GRAPH_UPDATE`，错误正文给的出路是改图声明 reducer，续跑时成功任务的写入原样补回、不重跑。运行时裁决分歧没有一家做，(b) 也只是让重跑碰运气。**没照抄的部分**：串行编排的转授与显式转授（`RegrantFromNode` / `RegrantRouteTo`）保留，并发决定方仍可能由业务显式造出来——按 LangGraph 的单写入者规则在合并点终局；节点里业务自己起 goroutine 并发调几个被转授的 agent 框架看不见（谁后交回来听谁的，实测 40 次 21:19），跟 LangGraph「节点内并发归节点作者」同一处置，约定写在 `compose_v2/signal.go`「并发合并要显式做」，不另立规则（曾提议「无序合并点一律只许一个来源」，那是为没人写的用法自创规则，撤回）。代价：冲突清档会连带清掉同一棵存档树里别人正等着回答或已经答过的挂起档，任务只能重开（同 `LastValue`）。**审计后（同日）**：业务经 `JoinSignals` 合并的几路给出不同决定时同样带 `ErrRunAborted` 终局（值一致仍算一次，那是业务显式选的合并语义）——第一版只改了引擎那条，按约定分路收、合并的写法冲突往上交之后，续跑时分类器重跑、两个路由 agent 的决定从完成记录原样拿回，次次再撞（审计实测续跑 3 次 3 次冲突），正是这一行要消灭的形态；业务想自己裁决就接住冲突或改用 `JoinSignalsFirstWins` |
| 2026-10-12 | **经 EventSink 发事件、在 OnNodeEvent 里做出的决定，跟着发事件的那一段执行走**（`compose_v2.CaptureSignals` 开一段时把节点的 EventSink 复制一份、只换收信号的那一段，OnNodeEvent 仍在原来的相位里跑）。上一轮只在相位入口（条件边、OnGraphEnd、节点级钩子）重绑了 sink；节点体里 CaptureSignals 开的一段（tool_call、嵌套调用、OnToolEnd 那次钩子调用、重试的一次尝试）里经事件做出的越过了那一段：失败的调用里照样交出、暂停续跑时那一段从工具账 / 完成记录拿回而它没跟着记下、OnToolEnd 的改道排到工具体之前、同一批谁赢随调度（均实测，`err == nil`） | 「经事件做出的跟直接做出的一样」是上一轮立的规矩，这一轮把它推到所有段；哨兵 `agent_v2.TestEventDecision_*`（每条同时跑直接做出作对照） |
| 2026-10-12 | **改为 OnNodeEvent 只作观测**（推翻上一行）：它拿到的 ctx 上没有收信号处，在里面做出的上行信号不生效（`EmitSignal` 返回 false、`HandoffTo` 报错），决定在发事件的那段代码里直接做；按具体类型重绑 sink 的两处（`rebindEventSink` / `rescopeEventSink`）删掉。理由：`EventSink.Send` 不带 ctx，OnNodeEvent 分不出事件是从哪一段执行发出来的，靠在各段入口重绑去补一再漏出新形态（业务包一层转发的 sink、钩子里再发一次事件、钩子共用节点相位的计数器——审计实测三条）；仓内没有经事件做决定的生产用法；回调只作观测、不驱动控制流是通行做法。备选「`Send(ctx, ev)` 让事件带上发送方的段」保留能力但要改 EventSink 接口，未采纳 |
| 2026-10-13 | **「只作观测」补两处**（审计实测）：编排钩子链把前一个钩子返回的 ctx 交给后一个，前一个返回它在 OnNodeStart 存下的 ctx 时，后一个在 OnNodeEvent 里照样做得成决定、交到调用方——链里每个钩子拿到的都改为摘掉收信号处的；删掉重绑之后图级事件（条件边、节点级钩子、图级钩子里发的）一直带着 Run 入口的 ctx，业务按自己在 OnGraphStart 推的 span 挂事件时挂到了调用方的 span 上——改为 OnGraphStart 采纳之后换一次图级 sink（只为观测，不带收信号处）。节点体的事件仍是节点的 ctx；节点级钩子与条件边发的仍按图级事件送达（nodeID 为空），这跟删重绑之前一致 |
