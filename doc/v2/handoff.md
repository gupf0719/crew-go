# Handoff 编排设计：对话控制权转移

> **文档状态**：本文是 `HandoffAgent` 的**权威设计 + 实现文档**，描述「模型 B：对话控制权转移」。
> **模型 B 已全量实现，模型 A（单 Run 接力 annotation-only）已删**——grep `agent_v2/handoff_tool.go` + `handoff_agent.go` + `side_signal.go`：
> `WithHandoffTransfer` / `newHandoffTransferTool` / `readPersistedActive` / `takeHandoffTarget` / `EndConversation`
> 全部存在，`handoff_modelb_test.go` 覆盖。下文 §4 决策表保留「设计依据」溯源；唯 supervisor
> `WithDeterministicReturnTo`（§14 阶段 3）仍是未实现的可选设计。
>
> 设计依据：四方框架（eino / AgentScope / OpenAI Swarm / LangGraph swarm）深挖对照，关键决策与对照见 §3 + §15。

---

## 1. 什么是 handoff——语义澄清

「handoff」在业界有**两种被混用的语义**，先厘清本设计指哪一种：

| 语义 | 控制权走向 | 代表 | crew-go 对应 |
|---|---|---|---|
| **委派**（delegate / orchestrator-workers）| A 委派子任务给 B，B 干完**结果还给 A**，A 继续 | AgentScope 的 "handoffs" 教程、OpenAI Agents SDK `agent.as_tool()` | **已有 `AgentAsTool`** |
| **控制权转移**（transfer / control handoff）| A 把控制权**交给 B，B 接管**对话不返回（除非 B 再转走）| OpenAI Swarm、LangGraph swarm、eino `transfer_to_agent`、OpenAI Agents SDK `handoff()` | **`HandoffAgent`（本文）** |

**本文的 HandoffAgent 是「控制权转移」语义**——这也是 OpenAI Swarm 让「handoff」一词流行的本义。注意 AgentScope 官方教程把「委派」叫 handoff，与本文语义不同（详见 §3）。

### 1.1 两种交互模型，本设计选模型 B

| | **模型 A：单 Run 接力**（已删，仅作对照）| **模型 B：多轮对话控制权转移**（当前实现）|
|---|---|---|
| 交互 | 用户**一次**输入 → 内部 A→B→C 接力处理 → 返回 final | 用户和 active agent **多轮对话**，转交后继续和新 agent 对话 |
| 一次 Run 的 input | 首跳=原始 input；后续跳=**上一跳的 output**（接力传递）| 首跳=用户**新一句话**；单 Run 内转交链的后续跳=**共享历史**（新 active 从 session 历史读上下文，**无新用户输入**）|
| 跨用户输入持久 active | ❌ 每次新 Run 从 `initial` 重新开始 | ✅ 记住上次转交到谁，下次输入直接进 active |
| 目标 agent 类型 | 不限（内部接力，可处理型）| 链上能接管对话的必须**对话型**（见 §13）|
| 像什么 | 动态 pipeline / 分诊流水线 | 客服转接：前台→账单→技术，各自跟你多轮对话 |

> **关键澄清（input 语义）**：模型 B 一次 Run 内可能有多跳转交（A 转 B、B 再转 C），但**用户只在 Run 开始给一句话**。只有 Run 的**首个 active**拿到这句新输入；转交链中后续 active **不拿新用户输入**，而是从共享 session 历史读上下文接管（§8）。这是「单 Run 内转交链」与「跨 Run 持久 active」两层的交界。

**为什么选 B 弃 A**：模型 A 的「去中心化单向接力」在单轮场景里大部分被现有编排覆盖（一次路由→`RoutingAgent`、中心规划→`PlanAgent`、委派后汇总→`AgentAsTool`），且 A 的「控制权不回头」反而做不到汇总。模型 B（对话控制权转移）才是 handoff 不可替代的核心价值——客服转接、专家协作、多轮换手。模型 A（已删）曾是 annotation-only（纯 LLM 无法自主触发，见 §6），是半成品——故弃 A 选 B。

---

## 2. 与其他编排的边界

| 编排 | 控制权 | 决策方 | 典型场景 |
|---|---|---|---|
| **HandoffAgent** | 单向转移、可持久跨轮 | 当前 active agent 自主（LLM）| 客服转接、专家接力对话 |
| `RoutingAgent` | 一次性路由到 1 个分支跑完 | 中心 router/classifier | 按 query 类型派发专家 |
| `AgentAsTool` | 委派后**收回**（结果返调用者）| 调用方 LLM | 主 agent 调子能力并汇总 |
| `PlanAgent` | 中心 planner 规划多步 | 中心 planner | 复杂任务分解执行 |

一句话区分：**Routing 选一次就跑完、AsTool 委派后收回、Plan 中心调度、Handoff 控制权单向交出去且持久**。

---

## 3. 四方对照（设计依据）

### 3.1 LLM 自主转交机制

| 框架 | 机制 | 目标验证 | 转交后终止 |
|---|---|---|---|
| **eino** | 内置 `transfer_to_agent` 工具（ChatModelAgent 注入）→ 结构化 `AgentAction.TransferToAgent` | `getAgent` 查 `subAgents∪parent`，**fail-fast** | **`returnDirectly` 一调即出**（工具调用后短路 ReAct，不再回 LLM）|
| **LangGraph swarm** | `create_handoff_tool` → `Command(goto, graph=PARENT, update={active_agent})` 原子跳转兼写入 | 编译期 `destinations` 校验，运行时非法 goto **静默忽略**（负面教材）| Command 直接跳转，原 agent ReAct 终止 |
| **OpenAI Agents SDK** | `handoff()` 自动生成 `transfer_to_<name>` 工具 + `input_filter`/`on_handoff`/`input_type` | — | 框架接管 |
| **OpenAI Swarm** | function 返回 Agent 对象 → 框架切 active | 无候选校验 | 主循环切 active |
| **AgentScope** | **无控制权转移**——其 "handoffs" 教程是 orchestrator-workers（agent-as-tool 委派），语义等于 crew-go 的 `AgentAsTool` | — | — |

**共识**：四方里做控制权转移的（eino/LangGraph/SDK/Swarm）**一律走 tool-call 触发**，没有一个用「解析 LLM 输出文本」做主路径（文本解析被 LangGraph 调研明确列为「最次设计排序」）。**且 eino 用 `returnDirectly` 保证转交工具一调即出**——这点 crew-go 当前缺失，是本次必须补的（§6.5）。

### 3.2 跨轮持久 active（模型 B 的关键）

| 框架 | 框架级持久 active？ | 机制 |
|---|---|---|
| **LangGraph swarm** | ✅ **四方唯一** | `active_agent` 裸 `str` state channel（LastValue 覆盖）+ checkpointer 按 `thread_id` 持久 + 入口 `add_conditional_edges(START, 读active, 默认default)` 条件路由 |
| **eino** | ❌ | 每次从固定 root 重进 + LLM 看历史重路由；checkpoint 只存中断恢复，正常 run 不留 |
| **AgentScope** | ❌ | 「轮到谁」100% 业务 Python 维护；Session 只序列化各 agent 的 memory，不存编排控制流 |
| **OpenAI Swarm** | ❌（显式 stateless）| 业务循环 `agent = response.agent` 回灌；框架每次 run 纯函数 |

**关键结论**：模型 B 的「跨轮持久 active」**只有 LangGraph swarm 做了框架级支持**，是唯一值得对照的参照系。但它的实现绑定了 LangGraph「checkpoint 默认保留」假设，crew-go 不成立（见 §5）。

### 3.3 swarm 的局限（crew-go 借 OpenAI SDK 补齐）

LangGraph swarm 极简（~400 行）**没做**：防环 maxHops、转交历史过滤（`input_filter`）、转交带结构化 payload（连任务描述都传不了，社区主要痛点）、prompt 引导模板。其中防环 crew-go **已有**（`maxHops`），其余借 OpenAI Agents SDK 设计补齐（§9/§10）。

---

## 4. 核心设计：对齐 LangGraph swarm，三件事 + 两个适配

LangGraph swarm 的本质是**零私有引擎**——把三个 langgraph 通用原语按 handoff 语义组装：① `active_agent` channel（LastValue）② 入口读 active 条件路由 ③ handoff 工具原子跳转兼写入。**这三件事 crew-go 有现成对应**，但持久 active 的载体（cleanupOnSuccess）和转交工具的终止（无 returnDirectly）两处因架构差异必须适配/新增。

### 抄 / 适配 / 超越决策表

| 维度 | LangGraph swarm | crew-go 设计 | 判定 | 锚点 |
|---|---|---|---|---|
| active channel 语义 | `active_agent` LastValue | `handoffAgentActiveCh = LastValue[string]` | ✅ **抄（已实现）** | `agent_v2/handoff_builder.go` |
| 入口读 active 路由 | `add_conditional_edges(START)` | `StartNode→读active→AddConditionalEdge` | 🔧 **[已实现]**（复用 RoutingAgent 范式）| `compose_v2.GraphBuilder.AddConditionalEdge` |
| 单 Run 内转交链 | Command goto 跳兄弟 | HandoffAgent `step→cond→step` 循环 | 🔧 **[已实现]**（复用现有骨架，改造 §8 四点）| `agent_v2/handoff_builder.go` |
| transfer 触发 | handoff 工具 `Command(goto,update)` | transfer 工具（opt-in 配 sub-agent）+ 上行信号（跟着写入者的提交走，§6.3）+ return-direct + 图回边 | 🔧 **[已实现]** | 照 `agent_v2/ask_user_tool.go` 范式 |
| **转交工具一调即出** | — | **BasicAgent 新增 return-direct**（`tools→model` 边改条件短路）| 🔼 **[已实现]（借鉴 eino）** | `agent_v2/basic_builder.go` |
| **持久 active 跨轮** | active channel + checkpointer | **active 存 `store.Store`** | ⚠️ **[已实现]（active 走 store.Store）** | 见 §5 |
| 持久维度 | `thread_id` | **ThreadID**（`WithThreadIDCtx`，无则退化单 Run）| ⚠️ **[已实现]**（ThreadID 维度） | `compose_v2/checkpointer.go` WithThreadIDCtx |
| messages 共享 | add_messages 全共享 | session events 共享 | ✅ **抄（已实现）** | `agent_v2/handoff_agent.go` parentSession |
| 防环 maxHops | ❌ 无 | `maxHops`（**per-Run**，§8）| 🔼 **已超越** | `agent_v2/handoff_builder.go` |
| 转交历史可见性 | ❌（LangGraph 全共享原样）| `WithHandoffHistoryMode` 3 档（默认 Rewrite 看前接）| 🔼 **[已实现]**（真实 LLM 修正）| §9 |
| 转交带 payload | ❌ 无 | transfer 工具参数 `{agent_name, reason}` | 🔼 **[已实现]（超越 swarm）**（借 SDK）| §6 |
| prompt 引导 | ❌ 无 | `HandoffPromptPrefix` | 🔼 **[已实现]（超越 swarm）**（借 SDK）| §10 |

---

## 5. 持久 active：为什么不能照抄（关键决策）

### 5.1 架构差异

> LangGraph 靠 **checkpointer 持久 active**——它的 checkpoint **从不在 invoke 成功后删除**，thread 下的 cp 一直累积，下次同 `thread_id` invoke 自动恢复 `active_agent` state。
>
> crew-go 的 `cleanupOnSuccess`（`compose_v2/graph_loop.go` 的 Run 出口）在 Run **成功后无条件 `DeleteSubtree(sid, graphNS)` 清整个 cp 子树**，且无中断点时直接早退**不 save**——**无 per-channel 豁免机制**。

**结论**：active 放 `cp.Channels` **活不过一个 Run**。这是 crew-go channel-cp 引擎（per-Run lifetime + cleanupOnSuccess）与 LangGraph thread-cp 引擎（默认保留）的本质差异——**不是照抄，是按自己的架构选对载体**。

> 为什么不改 cleanupOnSuccess：它是 CLAUDE.md 标定的关键不变量（Run 1 完成清 cp，防 Run 2 同 sessionID 被旧 Pending cp 污染），动它风险大、影响全局。

### 5.2 适配方案：active 走 store.Store

```
持久维度 = HandoffAgent 名 + owner + ThreadID（name 防跨实例污染；**owner 是调用方身份**，
           不带它时两个租户递同一个 ThreadID 会撞同一个槽；ThreadID 业务 WithThreadIDCtx 传，
           不传则退化成拿 session.RunID 当键——**任务内持久**（含 Resume），NextTask() 开新任务则从 initial 重来）
存储     = store.Store, namespace = store.NS("__handoff_active__", name, owner, threadID)
           （owner 空时退化成 (标记, name, threadID) 三段，跟加这一维之前逐字一致），key = "active"
          ├─ 业务配 WithStore → 真持久（可 Redis/PG 跨进程）
          └─ 不配 → 默认 InMemoryStore（单进程多 Run 有效，类比 InMemoryCheckpointer）
写入方   = HandoffAgent stepFn（统一持久，§8）——transfer 工具只把决定交给它所在的那段执行，不碰 store（§6.3）
```

store 本就是「跨 Run 持久」语义（`store.Store` godoc：跨 thread/跨用户长期 KV），namespace 按 conversation 维度组织，天然不受 cp cleanup 影响。

**清理责任 [已实现]**：active key 不会自动清，每个 ThreadID 一条，长跑服务需主动清理（类比 observability `CleanupSession`）。提供：

```go
// EndConversation 清理某会话的持久 active（store.Put(ns, "active", nil) 等价 Delete，见 store.Store.Put godoc）。
// 业务在会话结束（用户登出 / 会话超时 sweeper）时调用，防 store 按 ThreadID 无限增长。
func (h *HandoffAgent) EndConversation(ctx context.Context, threadID string) error
```

### 5.3 完整数据流（客服例子）

```
Run1("我要退款", threadID=T1):
  入口节点: GetThreadID(ctx)=T1 + store.FromContext(ctx) → store.Get(NS("__handoff_active__",<name>,T1),"active")
           → 空 → fallback initial(frontdesk) → 条件路由进 frontdesk
  → frontdesk(BasicAgent) ReAct: LLM 调 transfer_to_agent("refund")
  → transfer 工具: 从 ctx 取本跳的收件人 → 验证 refund 合法 → EmitSignal(收件人,"refund")
                  → 返回 ack + 标记 return-direct → frontdesk ReAct 当轮短路结束(不再回 LLM)
  → frontdesk.Run 返回 → stepFn: takeHandoffTarget(这次调用交上来的决定)=refund
                        → store.Put(active=refund)(统一持久)
                        → maxHops 检查 → 图回边进 refund(单 Run 内接管)
  → refund(BasicAgent) 看共享 session 完整历史 → 回复"请提供订单号"(不调 transfer)
  → refund.Run 返回 → stepFn: 这次调用交上来的决定里没有发给本跳的 → done → Run1 返回, active=refund 已持久 store
Run2("订单12345", threadID=T1):   ← 同 ThreadID
  入口节点: store.Get(active)=refund → 校验 refund 在候选 → 路由进 refund(不回 frontdesk)
  → refund 处理 → 回复 → 返回
```

---

## 6. transfer 工具与转交信号链

这是 audit 暴露最需写死的主链（信号怎么传 / sub-agent 怎么终止 / store 谁写）。

### 6.1 为什么是 tool-call

§3.1 四方共识：LLM 自主转交一律走 tool-call。原模型 A 只有 `annotation[HandoffSignalKey]`（编程注入），LLM 跑出来的 final message 不会带这个 annotation，**纯 LLM 无法自主转交**——这是模型 A 是半成品的根因。

### 6.2 落地：opt-in 配进 sub-agent（保黑盒抽象）

HandoffAgent 把 sub-agent 当**黑盒 `Agent` 接口**（可以是任意编排，甚至嵌套 HandoffAgent），外层**无法像 eino/SDK 那样侵入注入工具**（它们知道内层是 ChatModelAgent）。所以 transfer 工具做成 **sub-agent opt-in**（照 `WithAskUser()` 范式 `agent_v2/ask_user_tool.go:47-140`）：

```go
newHandoffTransferTool(candidates map[string]bool) *tool.FuncTool // 未导出：只由授予的持有者构造
func WithHandoffTransfer() Option                                  // 只翻一个 opt-in 标志位
```

**两把闸都得开**：上层**授予** + 本 agent 自己 **opt-in**，缺任一把都不往 LLM 的 bind list
里注册 transfer 工具。授予是「这次调用里谁有资格」，opt-in 是「这个 agent 愿不愿意」——
后者让业务能把某个候选做成终点，也让框架**自己合成**的执行体天然不在名单里。

> 控制反转：不是编排层注入工具，而是 sub-agent 实现协议——对齐 CLAUDE.md「接口定义在使用方」。

**失败模式（必须文档化）**：opt-in 模型下，业务给 `initial` agent 忘配 `WithHandoffTransfer()` 时，initial 的 LLM 没有 transfer 工具 → 永远无法转交 → HandoffAgent 静默退化成单 agent。eino/SDK 自动注入不会有此坑，crew-go opt-in 独有。**对策**：`HandoffAgent` 构造期/首 Run 无法直接探测黑盒 sub-agent 的 toolkit，故采**文档化**。运行期软提示（一个 Run 内 active agent 从未发出转交信号、业务又声明了多候选时 `log` 一句「active %q 未配 transfer 工具？」，不 fail——合法场景是该 agent 就是终点）设想过、**未实现**，见 `doc/code/todo.md`。

### 6.3 转交信号机制（拍板，不留 either/or）

**转交是一条上行信号**（机制在 `compose_v2/signal.go`，agent_v2 这侧在 `agent_v2/side_signal.go`）：
stepFn 调候选之前把**本跳的收件人**放进 ctx（`handoffDest`：引擎在 step 节点这次执行里铸，见下文
「收件人别的代码经正常入口算不出来」），transfer 工具把「转给谁」交给**它所在的那段执行**；决定跟着那段执行的
提交一路往上走，stepFn 在候选这次调用**成功返回**之后，从这次调用交上来的决定里取发给本跳的那份。

transfer 工具 fn 执行（LLM 调用时）：

1. **取收件人**：`getHandoffDest(ctx)`。拿不到 = 这儿没人接转交 → 返
   `tool.Failure(HandoffNoCoordinatorErrCode, ...)` **软失败回灌 LLM**，让它改成自己作答。
   两种情形会走到这里：sub-agent 配了 `WithHandoffTransfer()` 但**没跑在 HandoffAgent 底下**
   （业务复用同一个 agent 对象、有时独立跑，是合法用法）；或它是**被当成工具跑的那一层**（§6.8）。
2. **读候选集**：候选名单**跟着授予走**（铸工具时装进闭包），不走 ctx——名单是授予内容的
   一部分，跟「能不能转」同生共死
3. **验证 target**：
   - 非法（不在候选）→ 返 `tool.Failure(HandoffTargetInvalidErrCode, "目标 %q 不在候选 {A,B,C}")` **软失败回灌 LLM** 重选（不终止 Run；对齐 AgentScope `FunctionNotFoundError` + crew-go `SkillDisabledErrCode` 范式）
   - 合法 → 下一步
4. **交出决定**（**不碰 store**，职责单一）：`compose_v2.EmitSignal(ctx, handoffSignal,
   {Dest: 收件人, Value: target, Origin: 所在 ns + 入口 + tool_call})`——Origin 只用于报冲突时定位。
   成员经 `HandoffTo` 做出的是**同一类**决定（同一个种类），跟 transfer 工具按同一套规则裁决
5. **返回 ack + 标记 return-direct** → sub-agent ReAct 当轮短路结束（§6.5）

**职责边界（解决 audit H3）**：
- **transfer 工具**：只表达「我要转给谁」→ 交给它所在的那段执行
- **stepFn**：sub-agent.Run **成功**返回后 `takeHandoffTarget` 取走发给本跳的 → 统一 `store.Put`
  **持久 active**（跨 Run）+ 图回边；剩下的（发给更上方的）交给 stepFn 这次节点执行，跟着它的提交继续往上走

两者职责不重叠：工具交决定、stepFn 碰持久 store。stepFn 靠「这次调用交上来的决定里有没有发给本跳的」
判断「本次调用是否发生转交」，**不靠读 store active**（store active 可能本来就是当前 agent，无法区分新转交）。

#### 一个收件人上是一个单值格子

本跳的收件人上只有一个「转给谁」，语义同单写入者的 `LastValue` channel。几次决定碰到一起时，按它们之间
有没有先后处理：

- **先后做出的，后做的为准**：候选是 `Sequential[分诊, 确认]`，分诊转 billing、确认改转 sales ⟹ sales；
  Loop 下一轮改主意 ⟹ 以下一轮为准；内层 LLM 经 transfer 转交之后业务代码再 `HandoffTo` 修正 ⟹ 以
  `HandoffTo` 为准。后面没再做决定不会抹掉前面的。
- **同一超步只能有一个成员做决定**：两个成员都做了转交决定（工具路径或 `HandoffTo` 都一样，**目标相同也算**）
  ⟹ 图的设计错误，引擎提交同一超步的成员时报 `compose_v2.SignalConflictError`（类型本身带着 `ErrRunAborted`）。并发的
  成员之间没有先后，分歧时谁也没有资格裁决；只在目标不同时才报的话，两个并发的决定方平时一致、偶尔分歧，
  设计错误要等到分歧那一次才暴露。**`ParallelAgent` 的分支因此拿不到转交能力**（分支里的 BasicAgent 没有
  transfer 工具，`HandoffTo` 返回「没有被授予」）；想「并行出意见、再定转交」，写成 `Sequential[Parallel, 决定者]`，
  分支的产出是意见，由那一个决定方来定。能撞上这条的只剩业务显式转授出来的并发决定方（GraphAgent 里几个
  并发节点各自 `RegrantFromNode`）。
- **同一批 tool_call，排在前面的生效**：模型在一条回复里发了两次 transfer（或一次 transfer、一个调了
  `HandoffTo` 的工具），目标不同 ⟹ 排在前面的生效，后面那次的结果换成一条失败结果（错误码
  `agent_v2.SupersededDecisionErrCode`：「这次调用做出的决定没有生效……」，后面带着这次调用原本的结果——
  做出决定的也可能是经嵌套调用定了路由的业务工具，丢了它的产出模型多半会再调一次；transfer_to_agent / route_to
  这类框架控制工具的结果只是回执，不带）进历史。这批调用有一个定死的
  次序（那条已提交的回复里的顺序），也有一个收得到反馈的发起者（模型自己），跟并行分支不同。合并在这批
  调用的 `OnToolEnd` 全部跑完之后做：钩子把结果改成失败的那次调用不参与（决定作废；transfer 不再短路，业务
  return-direct 工具照常以失败内容收尾），改成成功的照算；「算不算失败」与「模型看到的内容」由同一份改写后的
  Result 推出（钩子整体替换 Result 忘了填 `Success` 时零值是 false，按失败处理、模型看到失败格式）。被盖掉的
  那个调用整个不算（它别的决定也不生效）。`OnToolEnd` 钩子里做出的决定排在这批调用的决定之后——它是看过结果
  才做的。哨兵 `TestHandoffBatch_FirstTransferWins` / `TestRouteSignal_MultipleRouteCalls_FirstWins` /
  `TestToolBatch_FirstWinsAfterSiblingAsk` / `TestToolBatch_OnToolEndRewriteToFailure_VoidsTransfer`。

冲突是终局：不重试、Run 出口清档，同一超步里别的成员提的问题不记（这个 Run 已经结束，答了也用不上）——
跟单写入者 `LastValue` 被两个成员写同一个位置、同一个处置。代价要认：同一棵存档树里别人正等着回答、或者
已经答过的挂起档随之清掉，任务只能重开（哨兵 `TestHandoffConflict_IsTerminal_BeatsSiblingQuestion` /
`TestHandoffConflict_AfterAnsweredStep_IsTerminal` / `TestHandoffConflict_AgreeingDecidersStillTerminal`）。

⚠️ 曾经「目标不同才报、不清档、留给续跑」：续跑把整个超步的成员连同副作用重做一遍（跟决定无关的兄弟也
重做——普通的部分失败只重跑失败的那一支），决定是从记录里拿回的（成员节点里经 `RunSubAgent` 定的，完成
记录）时原样重放、次次再撞，任务过不去（实测）。「按否决推进重做代际」那条修法同样只是让重跑碰运气，
没做。

⚠️ 曾经的三条规则都不对：「同一批 tool_call 目标不一致报冲突」——这批调用续跑时从工具账原样重放、模型
不再被调，续跑必然再撞，任务永远过不去；批里另一个工具先问了人的话，用户答完才撞上冲突，那个问题白答
（审计实测）；「同一次调用里目标不一致一律报冲突」把先后修正当矛盾（`Sequential[分诊, 确认]`
两步各转一次，整个 Run 失败——改动之前的实现是后者为准）；「程序化转交取最后一个、工具路径优先」把并行
两支的矛盾静默挑掉（赢家由声明顺序决定，一支停过一次之后赢家翻转——审计实测 SALES ⟶ BILLING），而
「工具路径优先」让业务代码在 LLM 转交之后的修正被静默忽略。

#### 决定跟着写入者的提交走

决定写进**做出它的那段执行**，不直接写进消费方：

- 那段执行（一次工具调用 / 一次节点执行 / 一次嵌套调用 / 重试的一次尝试）**成功并提交** ⟹ 决定随它
  留下：节点提交时并入那张图的已提交状态（随存档落盘，`Checkpoint.Signals`；部分失败超步里成功成员的
  先记在 `Checkpoint.CompletedSignals`，整超步完成时跟续跑成功的合并），图成功跑完时交给调用它的那段
  执行，一层层到 stepFn；
- 那段执行**失败、被中断、被否决** ⟹ 决定随它作废（停下时已提交的那些留在存档里，续跑跑完时一起交上来）；
  工具调用的结果是「没成」——框架按 `WithToolTimeout` 放弃了它、工具返回 `tool.Failure`——同样作废：
  模型看到的是「没成」，决定却照样生效的话两边结论相反；
- ⚠️ 「作废」只管**没做成**的部分：重试的下一次尝试拿回上一次尝试里已完成的嵌套调用、续上被调方已提交的
  步骤时，那些决定随记录 / 存档一起再交出——它们属于已经做成的那部分；
- **跳过执行**的地方把当时做出的决定连同产出一起记下、拿回时原样交出：工具账（`toolMemoEntry.Signals`）
  与嵌套调用的完成记录（`nestedCallRecord.Signals`）；
- Reflect 候选：每一轮 actor 做出的决定先存着，评审通过（或预算耗尽、以这一轮兜底）时才交出——被否掉的
  那一轮，产出被下一轮取代，它的决定也随之作废（曾经第一轮转交、被否、第二轮自己作答并通过，对话照样被
  转走，实测）。

所以候选是「先分诊（转交）、再问人确认」两步时，确认那一步停下、续跑时分诊不再执行，它的转交仍在
分诊那一步的提交里（候选的存档里），候选跑完时交上来。

#### 为什么不是消费方准备的容器

先后用过两种「消费方准备、写入当场生效」的容器：`RunSession` 里按「ns + hop + tool_call 下标」算的
key，以及 stepFn 每次尝试新建、放进 ctx 的收件槽。后者把生命周期从位置换成了对象，解决了位置维度
在两次调用之间重合造成的串台（上一个 Run 的残留、`NextTask()` 开的新任务、被当工具用的孙辈——新 Run
的 hop 同样从 0 起，孙辈继承同一个 ctx）。但两者共有一个问题：**写入当场生效、不等写入者提交**。
实测四种静默错误，全部 `err == nil`（哨兵 `agent_v2/side_signal_commit_test.go`）：

| 形态 | 后果 |
|---|---|
| 写入者那一步已提交、续跑不再执行它 | 转交丢失：候选 `Sequential[分诊, 确认]`，不中断转给 billing，中断续跑后前台自己作答 |
| 业务工具里经 `RunSubAgent` 转交 / 定路由、同批兄弟工具中断 | 续跑时工具账命中、工具体不跑，决定丢失 |
| 失败的那次尝试写下的 | 照样生效：候选里 `RunWithRetry` 包 `RunSubAgent`，被调方 `HandoffTo` 之后自己失败、第二次尝试不转交，结果仍转走 |
| 并行的两路写同一格（有的 provider 每轮都叫 `call_0`）| 谁后写谁赢、不报冲突 |

**两条承重约束**（改代码时别破坏）：
- **收件人续跑时算出同一个值**：`handoffDest` 在 step 节点这次执行里铸（`compose_v2.SignalRecipient`：这个
  任务的密钥 + 位置 + 分域，位置含访问序号——每一跳一个；密钥一个任务一把、随存档恢复）——上一个 Run 写下、存在候选存档里
  的决定，续跑后仍认得出是发给这一跳的。
- **失败的尝试交不出它没做成的那部分**：stepFn 每次尝试用 `compose_v2.CaptureSignals` 收一次，失败 / 中断的
  尝试里没做成的部分做出的随它作废；目标只能从成功返回的 `handoffAttempt` 里出来（错误路径返的是零值），
  取的那一步在重试**之外**做（`ag.Run` 成功之后才做的检查落进重试范围，会让一次已经成功的执行被整个重放
  ——曾经冲突检查在闭包里，实测配 MaxRetries=3 扣款跑了 4 次；现在冲突在合并点就报了，而且不可重试）。

#### 收件人别的代码经正常入口算不出来

`compose_v2.Signals` 的 JSON 形态是给持久化用的（工具账、完成记录要随存档走），能手写；`EmitSignals` 收任何
一组。所以一条转交能不能送到 stepFn 手里，取决于写入者知不知道收件人。收件人由引擎铸
（`compose_v2.SignalRecipient`），带着这个任务的密钥，只经 ctx 上不导出的键交给 transfer 工具与
`HandoffTo`——别的代码经正常入口拿不到、也算不出来：

- **铸的时候带上分域**：step 节点体里铸；候选在自己的图之外跑的那一段（它的 `OnAgentStart` 等钩子，
  `compose_v2.HandOver` 交出去的）是另一个分域，用同一个 label 也铸不出 stepFn 的收件人。没有这一维时，
  候选自己的钩子把 ns 推回上一层（`WithGraphNS` 是导出的）就算出来了，没被授予转交能力的终点候选照样
  转走了对话（实测）。哨兵 `TestSideSignal_CandidateHookCannotMintStepRecipient`。
- **放进 ctx 时包一层遮罩**（`sealedRecipient`）：标准库 context 的 `String()` 会把链上每一层的值原样
  打印，曾经存的是裸字符串，业务代码 `fmt.Sprint(ctx)` 或 slog 记一下 ctx 就读到了它（审计实测四处边界
  全部转走了对话）。现在打印 ctx 只露出 `<recipient>`。哨兵 `TestRecipient_NotPrintedWithCtx`。
- **防护范围到正常入口为止**：密钥就在存档里（`Checkpoint.RecipientKey`），故意经 checkpointer 读出来
  自己算 HMAC、或经 reflect / unsafe 硬读 ctx 的代码算得出收件人——那跟直接改存档是一回事。同一进程里
  的业务代码不是信任边界（issue-99 §5.4），这里挡的是「顺手一拼就转走了对话」。

曾经收件人是 `"handoff:" + 候选所在的 ns`：业务工具 `GetGraphNS` 一拼就得到，手写一份信号 JSON 再
`EmitSignals` 就转走了对话——没配 `WithHandoffTransfer` 的终点候选、被当工具用的孙辈（§6.8 摘掉了它的
收件人，它照样拼得出来）、永不拿转交能力的分类器，三处边界全部绕过，`err == nil`，而且转出去的 active
持久进 store、下一轮前台根本不跑（审计实测）。第四处是候选自己的钩子，见上面「分域」那条。种类放在不导出
的变量里只挡得住正常入口（`EmitSignal` 要 `*SignalKind`），挡不住手写的 JSON。哨兵
`TestSideSignal_ForgedSignalJSON_HasNoEffect`。

### 6.4 非法目标处理（三来源）

| 来源 | 处理 | 依据 |
|---|---|---|
| **transfer 工具**（LLM 自主）| **软失败回灌 LLM 重选**（`HandoffTargetInvalidErrCode`）| LLM 幻觉常见，软失败比 fail-fast 整 Run 鲁棒 |
| **`HandoffTo`**（程序化成员）| **返 error**（带 `ErrRunAborted`：不重试）| 业务代码给非法目标是 bug，应立即暴露（annotation 那条编程注入通道已删，见 §8） |
| **入口读 store 的陈旧 active** | **fallback initial + 清陈旧 store** | 跨进程/跨部署后 agent 集合可能变（改名/下线），store 残留陈旧 active；不能学 LangGraph「静默忽略」死路 |

> 原模型 A 对非法 target 是「静默视为完成」（`agent_v2/handoff_builder.go` 里当时的条件边），与 LangGraph「静默忽略」同属负面教材——模型 B 必须改掉。

### 6.5 return-direct：转交工具一调即出（借鉴 eino，BasicAgent 新增能力）

**问题（audit H1）**：crew-go BasicAgent 当前 `tools → model` 是**无条件边**（`agent_v2/basic_builder.go` `AddEdge("tools","model")`）——transfer 工具执行后结果喂回 model，LLM 会**继续生成**（过渡话/更多工具/甚至违背转交意图自己处理）。我们需要 eino 的 `returnDirectly` 语义：**某工具调用后当轮短路结束 ReAct，不再回 LLM**。

**机制 [已实现]**（通用能力，不只 handoff 用——exit 类工具同样需要）：
- 工具声明自己 `return-direct`（如 `tool.Result` 带标记字段，或 transfer 工具固定标记）
- `toolsNode`（`agent_v2/basic_builder.go`）执行后，若调用了 return-direct 工具 → 写一个 `returnDirectCh`（OnceFlag）
- `bld.AddEdge("tools","model")`（:563）改为 **`AddConditionalEdge("tools", ...)`**：`returnDirectCh` set → 走 `terminalTarget`（postamble/End）；否则 → model（保持原行为）
- 效果：transfer tool_call 后 sub-agent 当轮终止，无多余 LLM 生成（对齐 eino `returnDirectly`，避免违反 §10「别向用户暴露转交」）

### 6.6 单轮多 transfer（audit H2）

BasicAgent tools 节点 **N≥2 tool_call 时 goroutine 并发 fanout**（ISSUE-81）。若 LLM 一轮发多个 `transfer_to_agent`（幻觉/犹豫），并发写共享信号会 race（CLAUDE.md「并发共享态 + thundering herd」）。设计目标：**单 Run 内转交目标唯一且确定性可复现**（控制权不能同时交两个）。

> **组合编排下的撞格问题**（历史）。最早键只有批内下标，ParallelAgent 之类的组合编排里两个分支各自的下标都从 0 起 ⟹ 撞同一格 ⟹ 后写者赢、胜负随调度（实测 30 轮 21:9）。2026-08-24 键补了 callID 维，可两个分支的 tool_call 同 ID 同下标时（有的 provider 每轮都叫 `call_0`）照样撞格。2026-10-08 起没有「格子」可撞：每次 tool_call、每个并发成员各自收自己的决定，在合并点合并——同一批 tool_call 排在前面的生效、同一超步只能有一个成员做决定（§6.3）。

**对照 LangGraph（深挖结论，修正之前"last-write-wins"的误判）**：LangGraph swarm 多 handoff 是**确定性「第一个胜出」**——`_control_branch`（`graph/state.py:1548-1550`）遍历命令列表对**第一个 `graph=PARENT` Command 早抛 `ParentCommand`、后续丢弃**；其 `active_agent`（LastValue）实际只收到 1 个写（多写会 `InvalidUpdateError` 报错，**非 last-write-wins**）。底层 tool 并行执行但 `executor.map`/`gather` 保序，所以"第一个"按 tool_call 索引确定。crew-go 应对齐这个**确定性**，而非用 goroutine 非确定 last-write-wins。

**crew-go 机制**：
- **return-direct 用 OnceFlag**（原子）——生效的那次 transfer set，本轮短路终止（§6.5）——被同批更前面的决定盖掉的、`OnToolEnd` 之后是失败的不 set
- **目标不一致时排在前面的生效**：每个 transfer 把决定交给它所在的那次工具调用（各自一段执行）；tools 节点在这批调用的 `OnToolEnd` 全部跑完之后用 `compose_v2.JoinSignalsFirstWins` 合并——目标一致算一次，不一致时按 tool_call 在回复里的顺序第一个生效，后面那几个的结果换成一条「没有生效」的失败结果（`SupersededDecisionErrCode`）。**结果与 goroutine 调度无关**：次序来自那条已提交的回复、不来自谁先跑完；续跑时从工具账拿回的决定按同一个次序重判，结论不变。曾经报 `SignalConflictError`：这批调用续跑时原样重放、必然再撞，任务永远过不去（审计实测）。同一超步的并发成员不一样——它们之间没有次序可依，两个都做了决定就是图的设计错误（终局；并行分支拿不到转交能力，见 §6.3）。跨实例 / 跨 hop / 跨 Run 的隔离靠「决定只在它所在的那段执行里活着」+ 每一跳一个收件人（§6.3）。哨兵 `TestSideSignal_ConcurrentJoinIsDeterministic`（WaitGroup 屏障强制真并发，300 轮，验合并结果只看下标）+ `TestHandoffBatch_FirstTransferWins` + `TestSideSignal_ConcurrentConflictingTransfersReported`（两个并发节点各自转授、同 ID 同下标，仍报冲突）。
- **多余 transfer 不重选**：落败 transfer 的结果换成失败结果、进历史**留痕**，**不回灌、不重选**——return-direct 已终止本轮（§6.5 轮不回 model）。

> **crew-go 比 LangGraph 干净**：LangGraph 丢弃落败 handoff 时**连同丢弃其 ToolMessage** → 留下无配对 tool_result 的 dangling tool_use → 下一轮喂 LLM 时 OpenAI/Claude 422。crew-go 的 makeToolsFn **为所有 tool_call 生成 tool result**（配对完整），落败 transfer 的 tool result 天然保留 → **无 dangling**，契合 `AdjustIndexToPreserveAPIInvariants` 配对不变量。
> **与 §6.4 区别**：§6.4 是**无 return-direct** 时单非法 target → 回 model 让 LLM 重选；§6.6 是 return-direct **已 fire** 的并发多 transfer → 本轮终止不回灌。

### 6.7 transfer + ask_user 同轮共存（已知限制，LOW-MED）

sub-agent 一轮**同时**调 transfer_to_agent + ask_user（InterruptError）时：makeToolsFn 里 Interrupt
优先 return（MergeInterrupts，先于写 results）。transfer 那次调用已经做完、进了工具账（决定随账记下），
而 tools 节点这次执行因中断作废、决定没往上交。Resume 后 transfer 命中工具账、交出记下的决定，ask_user
拿到答案，return-direct 短路转交走，**ask_user 的答案无 agent 消费（丢弃）**。

- **影响**：LLM 同轮既转交又问用户是矛盾意图（罕见/不连贯）；**持久 store active 不污染**
  （store.Put 只在 stepFn 成功路径，interrupt 路径不到达——abort 后新会话仍正确路由 initial）。
- **严重度 LOW-MED**：边缘 case 丢用户输入，无崩溃、无持久态损坏。
- **业务规避**：sub-agent system prompt 引导"要么转交、要么问用户，不要同轮都做"。
- ⚠️ ~~**transfer 必须留在 tool memo 之外**~~ **已不成立**：transfer **进** memo。它做出的决定随这次
  tool_call 的记账进 cp（`toolMemoEntry.Signals`），命中时原样交出 ⟹ 转交不依赖「工具体一定会重跑」。
  判据是「这次调用做出了上行信号」（`shouldMemoize`），不是名字。**不靠工具声明**：决定只能经框架那几个
  入口做出——种类放在不导出的变量里挡正常入口，手写信号 JSON 冒充不了是因为收件人算不出来（§6.3）。

### 6.8 被当作工具调用的那一层，转交决定不许逃出去

`AgentAsTool(x)` 和 skill fork 把一个 agent 降格成**一件能力**——调用方要的是它的产出，
不是让它替调用方决定这场对话归谁。所以这两处在跑内层 agent 之前把收件人从 ctx 摘掉
（`withoutHandoffDest`），内层的 transfer 拿到 `HandoffNoCoordinatorErrCode` 软失败回灌、
改成正常作答。

不摘的后果实测过：候选 `frontdesk` 把另一个 agent 当工具用，那个孙辈调一次 transfer 就把
**祖父那层**的路由夺走了——而 frontdesk 自己答的是「我自己处理」，`err` 全程为 nil。
ctx 天然下传：不摘的话**孙辈继承的正是同一个 ctx**，它拿到的收件人跟 frontdesk 的一模一样。

**哪些边界不摘（有意的）**：候选是 Sequential / Loop / Routing / Reflect / Plan 时，编排经
`runMember` 自动把转交授予转给它的成员，内层步骤替它做转交决定是对的——**那个编排本身就是候选**，
内层是它的组成部分而不是它调用的一件工具。例外有三：ParallelAgent 的分支拿不到（并发的决定方之间
没有先后，见 §6.3）；Plan 的临时子 agent 拿不到；GraphAgent 不自动转授，节点里要转授用
`RegrantFromNode` 显式交。判据是「这一层是候选，还是被谁当工具使」，而这只有调用点知道。

**嵌套 HandoffAgent 照常工作**：`AgentAsTool(HandoffAgent)`（§12 支持模式）进去之后内层
HandoffAgent 会 push 自己的收件人，内层转交正常发生、且不会漏给外层。哨兵
`TestHandoffTransfer_NestedHandoffStillWorksThroughAgentTool`（正反两条断言：内层必须真转交 +
外层必须没被转走——只有反向断言的话，「把 transfer 一刀切禁掉」也能全绿）。

---

## 7. 入口路由：读持久 active 起始

模型 B 入口由 **agent_v2 runGraph 预读持久 active 写入 active channel** 实现（比设计稿的"图内 entry 节点 + 条件边"更简单——直接复用 `WithInitialWrites` 起始写入 + stepNode 兜底）：

```
runGraph（agent_v2）:
  storeForActive(ctx)              // ctx 有则业务 push 的 store；无则 HandoffAgent 默认
                                   // InMemoryStore（注意 HandoffAgent **不** pushStoreCtx，与 BasicAgent 不同）
  + handoffThreadID(ctx, session)  // ThreadID 优先，无则退化 session.RunID
  → readPersistedActive: store.Get active
       → 命中且在候选 → 该 agent；陈旧/非法 → fallback initial + 清 store（§6.4）；空 → initial
  → WithInitialWrites(Write(activeCh, startActive))  // 起始 active 预写入 channel

compose_v2 stepNode: 读 activeCh（= startActive）跑该 agent；`if active=="" { active=initial }` 兜底
                     （startActive 恒非空——readPersistedActive 总以 initial 兜底，故兜底实际不触发，仅防裸 compose_v2 用法）
```

startActive 为 `initial` 的情形（= swarm `default_active_agent` 语义）：store 无持久值 / 陈旧 / 空——兜底逻辑在 readPersistedActive，不依赖 channel 默认值（照 swarm 做法）。

---

## 8. 单 Run 转交链 + 跨 Run 持久 active

模型 B = **单 Run 内转交链（内存）+ 跨 Run 持久 active（store）**，对齐 LangGraph swarm「单 invoke 内可多次转交直到稳定 + active 持久跨 invoke」。

**单 Run 内**（复用现有 `step→cond→step` 循环，`agent_v2/handoff_builder.go`）：
```
首个 active 处理(input=用户新输入) → 调 transfer → return-direct 终止该 sub-agent
→ stepFn 取发给本跳的转交 + store.Put 持久 active + 图回边 → 新 active 接管(input=共享历史)
→ ... → 某 active 不转交(回复用户) → Run 返回
```
单 Run 内转交链**不上 cp**（内存循环即可——run 内转交不需要持久，OpenAI Swarm 同款）。

**跨 Run**：active 持久在 store；下个 Run 入口（§7）从 store 读 active 直接进。

> ⚠️ **前提：业务声明了 `compose_v2.WithThreadIDCtx`**（2026-08-12 补——原文漏了这句，
> 而它承重）。持久 active 的键取自 `handoffThreadID`：优先 ThreadID，**无则退化
> `session.RunID`**，而 `RunSession.RunID` 的契约是「一枚只服务一个任务」⟹ 业务
> `NextTask()` 开新任务后键就变了，**新任务从 `initial` 重新开始**。
>
> 实测三组：无 ThreadID + 不换 RunID ⟹ 第二轮仍在 billing（这是契约钉死之前的**偶然**，
> 靠业务复用 sess）；无 ThreadID + `NextTask` ⟹ 退回 frontdesk；有 ThreadID + `NextTask`
> ⟹ 仍在 billing。
>
> 退化档给到的是「**任务内**持久」（含 Resume），不是坏事——没声明会话时转交不泄漏进
> 一个无关的新任务，跟 `approve_always` 按任务分是同一条理由。要跨任务连续就声明 ThreadID。

**改造点（相对原模型 A）**：
1. 入口从 store active 起（而非恒 `initial`）
2. 转交信号加 tool 通道（§6.3），保留 annotation 作编程注入第 2 通道（后来删了：那条路绕过授予与候选校验，程序化成员改走 `HandoffTo`，见其 godoc）
3. input 语义：首跳=用户输入，后续 in-Run 跳=共享历史（而非「上一跳 output」）
4. stepFn 统一 `store.Put` 持久 active（§6.3 职责）

**防环 maxHops（per-Run，audit M4 cross-verify）**：`maxHops`（`WithHandoffMaxHops`）是**单 Run 内转交链上限**——防的是「单 Run 内 A→B→A→B 自动 ping-pong 死循环」（无用户介入的失控转交）。**跨 Run 的转交是用户驱动的**（每个 Run 一次用户输入），不是死循环，**不需 maxHops 防**，故 maxHops **per-Run 重置不持久**（不存 store）。

---

## 9. 转交历史可见性 WithHandoffHistoryMode [已实现]

业内（LangGraph swarm / eino / OpenAI Agents SDK）默认都"后接看前接"（接力语义）。crew-go
owner-aware（BasicAgent 默认只读自己 `SelfHistory`）原本看不到前接——曾是 bug（godoc 声称看
前接但实现没做到）。本节修复并给三档控制。

### 三档历史视图（HandoffHistoryMode）

| 档 | 后接看到 | 底层视图 | 适用 |
|---|---|---|---|
| **HandoffHistoryRewrite**（**默认**）| 前接消息**改写第三人称** user（"For context: [X] said/called..."）| `session.HistoryFor` | handoff 转交链通用——拍平 tool_call 成文本 |
| HandoffHistoryRaw（opt-in）| 前接消息**保原 role** + 标 `from_agent` annotation | `session.HistoryForRaw` | 看重"前接对话保原样"的场景（悬空 transfer tool_call 由框架剥离、换成一条中性记账文本——不再 400）|
| HandoffHistorySelfOnly（opt-in）| 只自己历史 + input（owner-aware，看不到前接）| `session.SelfHistoryCompacted` | 隔离（敏感数据不跨 agent）|

```go
func WithHandoffHistoryMode(mode HandoffHistoryMode) HandoffOption  // 默认 HandoffHistoryRewrite
```

### ⚠️ 为什么默认 Rewrite 而非 Raw（真实 LLM 验证修正 + 悬空剥离 fix 后更新）

设计稿与 audit 原选 Raw（保 role，理由"crew-go 有 from_agent 消歧 + 配对防护，不必像 eino
改写、可拿结构保留双赢"）。**真实 LLM（qwen）测试推翻**：Raw 保 role 会把前接的 **transfer
tool_call**（转交本身就是 tool_call）带给后接，但后接多是终点 agent、tools 列表无 transfer 工具
→ API 400（`Can only get item pairs from a mapping`：tool_call 无对应工具定义）。handoff 转交
链前接**必有** transfer tool_call，故 Raw 在 handoff 几乎必然触发。

**悬空剥离 fix 后状态更新**（agent 复访 HIGH-1 修复的连带）：`buildInitialMessages` 现在在
msgs 汇合层统一剥离悬空 tool_call（`dropDanglingToolCalls`——重建历史里的 transfer tool_call
恒无配对 tool_result），**Raw 不再触发上述 400**。但默认仍 Rewrite，理由演进为语义差异：
- **Rewrite**：前接 transfer 拍平成 "For context: [X] called tool `transfer_to_agent`..."
  **文本痕迹**——后接知道"谁转交给我、为什么"，接力语义完整；
- **Raw**：空文本的 transfer assistant 的 tool_call 被剥掉，但**消息本身保留**，换成一条
  中性记账文本（「此前发起过 transfer_to_agent(...)」，见 `UnresolvedCallsRecordAnnotation`）。
  **不再是零痕迹**——两档的差别缩小到**措辞**：Rewrite 是为接力写的第三人称叙述
  （"谁转交给我、为什么"），记账文本是给「历史缺了一块」兜底的中性陈述。看重接力叙述用
  默认 Rewrite，看重"前接对话保原样 role"才 opt-in Raw。

> **教训**（保留，仍有效）：mock 测试 Raw pass（stub 不调真 API），真实 qwen 才暴露 400——
> 跨层 feature 必须真实 LLM 端到端验证（`example/tutorial/adv_06_handoff_agent_test.go`），
> 纯 mock + audit reasoning 会得出错误结论（这次 audit agent 选 Raw 即被真实 LLM 推翻）。

### 落地：ctx 注入 mode + BasicAgent prior 切换

HandoffAgent stepFn push `WithHandoffHistoryModeCtx(subCtx, h.historyMode)` → BasicAgent
`buildInitialMessages` 的 `prior` 据 mode 在 `HistoryFor`/`HistoryForRaw`/`SelfHistoryCompacted`
间切换。复用 BasicAgent prior 管线 + 4 层压缩配对防护（`GroupMessagesByAPIRound`/`AdjustIndexToPreserveAPIInvariants`）。

- **零回归**：`GetHandoffHistoryMode` 缺省 SelfOnly（零值）——非 handoff 场景（BasicAgent 单独用 /
  其他编排嵌 BasicAgent）prior 仍读自己历史，行为不变。
- **input-dup 防护**：Rewrite/Raw 模式 input 已由 HandoffAgent 写进 session、经 prior 可见，
  BasicAgent **不重复 append + 不重复 AddEvent**（否则每 hop 写一份 → HistoryFor 捞多份 dup）。

---

## 10. prompt 引导 HandoffPromptPrefix [已实现，借 OpenAI SDK]

做了 tool-call 转交后，LLM 需被告知「有转交能力 + 别向用户暴露转交动作」（否则会说「我帮您转接到账单部门」）。借 SDK 的 `RECOMMENDED_PROMPT_PREFIX`：

```go
// HandoffPromptPrefix：拼进参与 handoff 的 sub-agent system prompt 的推荐前缀。
const HandoffPromptPrefix = `# 系统上下文
你是多 agent 系统的一员。当用户需求更适合别的 agent 处理时，调用 transfer_to_agent
工具把对话控制权转交给目标 agent。转交在后台无缝进行——不要在回复里向用户提及或解释
转交动作（不要说"我帮您转接"之类）。`

// HandoffPrompt 把前缀拼到业务 prompt 前（非 Option 函数，故不用 With 前缀——
// 对齐 CLAUDE.md「选项函数才用 With 前缀」）。
func HandoffPrompt(userPrompt string) string  // 返 HandoffPromptPrefix + "\n\n" + userPrompt
```

> 黑盒约束下无法强注入 sub-agent prompt，故提供常量+helper 让业务自愿拼（对齐 SDK 也是「推荐」非强制）。仅在 §6 transfer 工具落地后才有意义。

---

## 11. API 形态（完整示例）

```go
// 1) sub-agent 构造时 opt-in 配 transfer 工具 + prompt（保黑盒抽象）
frontdesk := agent_v2.NewBasicAgent(
    agent_v2.WithName("frontdesk"),
    agent_v2.WithModel(m),
    agent_v2.WithHandoffTransfer(),                                 // 注入 transfer_to_agent 工具（return-direct）
    agent_v2.WithSystemPrompt(agent_v2.HandoffPrompt("你是前台...")), // 拼 SDK prompt 前缀
)
refund := agent_v2.NewBasicAgent(agent_v2.WithName("refund"),
    agent_v2.WithHandoffTransfer(), /* ... */)

// 2) 组装 HandoffAgent
h := agent_v2.NewHandoffAgent(
    agent_v2.WithHandoffName("support"),
    agent_v2.WithHandoffInitial("frontdesk"),       // 首轮默认 active（= swarm default_active_agent）
    agent_v2.WithHandoffMaxHops(10),                // per-Run 防环（超过 swarm）
    // 历史视图默认 Rewrite（后接看前接，改写第三人称，保转交文本痕迹）；opt-in：
    // WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly) 隔离 / HandoffHistoryRaw 保 role（转交留一条中性记账文本）
).
    AddAgent("frontdesk", frontdesk).
    AddAgent("refund", refund)

// 3) 多轮对话（同 ThreadID → 持久 active）
ctx = compose_v2.WithThreadIDCtx(ctx, "conv-T1")
ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
events1, _ := h.Run(ctx, sess, schema.UserMessage("我要退款"))      // active→refund 持久
sess.NextTask()                                                    // 每一轮是一个新任务：换 RunID（同一场对话靠 ThreadID 串）
events2, _ := h.Run(ctx, sess, schema.UserMessage("订单 12345"))   // 直接进 refund

// 4) 会话结束清理持久 active（防 store 泄漏）
_ = h.EndConversation(ctx, "conv-T1")

// HITL：transfer 工具与 ask_user 共存——某 agent 中途 ask_user 中断走 Resume（已有闭环）
```

---

## 12. 与现有机制的关系

- **session events 共享 [已实现]**：sub-agent 传 `parentSession` 跑（`agent_v2/handoff_agent.go`），新 active **默认通过 `HistoryFor`（Rewrite 模式，§9）看前接对话做接力决策**（owner-aware：自己原样、前接改写第三人称 user）。注意是 `HistoryFor` 而非 `SelfHistoryCompacted`——后者只看自己（SelfOnly 模式才用）。
- **ThreadID [已实现/扩展用途]**：`WithThreadIDCtx` 当前是 observability span attr（`gen_ai.conversation.id`）；模型 B 复用它作持久 active 的 conversation 维度（**注意**：当前 ThreadID 不驱动 cp 加载，模型 B 用它作 store namespace，不依赖 cp 行为）。
- **HITL Resume [已实现]**：transfer 工具与 `ask_user` 正交——sub-agent 中途 `ask_user` 触发 `InterruptError` 挂起走现有 `Resume` 闭环；transfer 是 sub-agent 跑完后的控制权转移，两者不冲突。
- **嵌套 [已实现]**：HandoffAgent 本身可被 `AgentAsTool` 包装嵌入更外层编排，子 cp 按 graphNS 物理分离。

---

## 13. 目标 agent 类型约束

**链上能「接管对话并可能再转交」的 agent 必须是对话型**（`BasicAgent`/能注入 transfer 工具的 agent）——因为只有它能跑 LLM 回复用户 + 调 transfer 工具发起下一次转交。**只有转交链末端的纯处理终点**（接管后处理完即结束、不再对话也不再转交）可以是非对话型。

**「必须」如何落地（解决 audit M7：硬约束 vs opt-in 衔接）**：框架**无法**从黑盒 `Agent` 接口探测它是否对话型 / 是否配了 transfer 工具，故此约束是**业务自负的约定**，框架不强制校验。运行期兜底：若 active 路由到一个非对话型 agent（它既不回复也不转交），表现为该 agent 跑完即 done、Run 正常返回——不会死锁，只是「转交到此为止」。§6.2 设想过的运行期软提示（靠**行为推断**：Run 内零转交信号 + 业务声明了多候选；**非探测** sub-agent 的 toolkit，故只能 `log`、不能 fail-fast）**未实现**——「忘配 transfer 工具」这种退化目前只能靠文档与业务自查发现。

---

## 14. 实现阶段

每阶段独立 commit + mock & 真实 LLM 测试 + sanity-revert + audit（高风险 `_agent.go` 必 mark-audit）：

| 阶段 | 内容 | 产出 |
|---|---|---|
| **0 前置** ✅ | BasicAgent **return-direct 能力**（`tools→model` 边改条件短路 §6.5）——通用能力，handoff/exit 共用 | 工具可声明一调即出 |
| **1 核心** ✅ | transfer 工具(opt-in,return-direct) + 上行信号(§6.3) + store 持久 active(stepFn 统一写) + 入口条件路由 + 单 Run 转交链改造(§8 四点) + 非法目标三来源处理 + EndConversation + 删模型 A「上轮 output」接力语义 | 纯 LLM 自主转交可用 + 跨轮持久 active |
| **2 增强** ✅ | `WithHandoffHistoryMode`（3 档，默认 Rewrite 看前接——真实 LLM 修正 Raw→400；后经悬空剥离 fix 解除 400，默认不变，见 §9）+ `HandoffPromptPrefix`/`HandoffPrompt` | 追平 swarm + 真实 LLM 验证 |
| **3 supervisor**（可选）| `WithDeterministicReturnTo`（worker 跑完回 supervisor，编排层直控下一跳，比 eino 伪造消息更简单）| 星形拓扑 |

---

## 15. 决策记录

| 决策 | 选择 | 理由 |
|---|---|---|
| 交互模型 | **模型 B**（对话控制权转移）弃模型 A（单 Run 接力）| A 场景窄 + 被 Routing/Plan/AsTool 覆盖；B 是 handoff 本义、不可替代 |
| LLM 自主转交 | **tool-call**（transfer 工具）弃文本标记 | 四方共识，文本解析是「最次设计排序」 |
| transfer 工具落地 | **sub-agent opt-in** 弃外层自动注入 | sub-agent 是黑盒任意编排，外层无法侵入；保「任意编排可嵌套」抽象 |
| **转交工具终止** | **给 BasicAgent 加 return-direct**（一调即出）| audit H1：当前 `tools→model` 无条件边，transfer 后 LLM 继续生成；eino `returnDirectly` 是验证过的正解。**修正之前「不需要 returnDirectly」的误判** |
| 转交信号 | **transfer 工具交上行信号（跟着写入者的提交走），stepFn 取走发给本跳的、统一持久 store** | audit H3：职责单一——工具表达意图、stepFn 持久；stepFn 靠「这次调用交上来的决定里有没有发给本跳的」判断转交，不靠读 store。载体先后从 session KV、ctx 收件槽换成上行信号，理由见 §6.3 |
| **持久 active 载体** | **`store.Store`** 弃 `cp.Channels` | cleanupOnSuccess 清 cp（`graph_loop.go:360`），active 活不过 Run；store 跨 Run 持久语义天然契合。**这是不照抄 LangGraph 的核心点** |
| 持久维度 | **HandoffAgent 名 + owner + ThreadID** | name 防跨实例污染；owner 是调用方身份（不带它时两租户递同一 ThreadID 会撞槽）；不传 ThreadID 退化成 `session.RunID`——任务内持久，开新任务从 initial 重来 |
| 非法目标 | tool 软失败回灌 / `HandoffTo` 返 error / 入口陈旧 fallback+清 | 三来源不同严格度（§6.4）|
| 多次转交 | **一个收件人上是单值格子：先后做出的后者为准，同一超步只能有一个成员做决定**（两个都做了 ⟹ `SignalConflictError` 并带 `ErrRunAborted`：不重试、清档，目标相同也算；`ParallelAgent` 的分支拿不到转交能力）；同一批 tool_call 排在前面的生效；transfer 工具与 `HandoffTo` 是同一类决定 | 曾经是「按 idx 升序取首个」——组合编排下没有良定义的语义（实测 30 轮 21:9）；后来改成「同一次调用里不一致一律报错 + 程序化取最后一个 + 工具路径优先」，又把先后修正当矛盾、把并行矛盾静默挑掉（§6.3）。再后来「并发不一致才报、不清档、留给续跑」：续跑把整超步的成员连同副作用重做一遍、决定从记录里拿回时次次再撞（2026-10-09 改，§6.3）。落败/冲突的 transfer 仍保 tool result 配对（不留 dangling tool_use）|
| 防环 | **maxHops per-Run** | audit M4：单 Run 内防自动 ping-pong；跨 Run 用户驱动不需防，故不持久 |
| **转交历史可见性** | **`WithHandoffHistoryMode` 默认 Rewrite**（改写第三人称看前接）弃 Raw（保 role）| **真实 LLM(qwen) 修正**：Raw 保 role 把前接 transfer tool_call 带后接致 API 400——**推翻 audit 选 Raw**（mock pass 真实 fail，跨层 feature 必真实 LLM 验证）。**演进**：悬空剥离 fix 后 Raw 不再 400，默认不变——理由改为语义（Rewrite 是为接力写的转交叙述 / Raw 留一条中性记账文本），见 §9 |
| prompt 引导 | **`HandoffPromptPrefix` 常量 + `HandoffPrompt` helper** | 借 SDK RECOMMENDED_PROMPT_PREFIX；黑盒不强注入，业务自愿拼 |
| Swarm「function 返回 Agent 切换」 | **不做** | 需中途劫持 ReAct 循环，与 crew-go channel-BSP「Run 跑完才返回」不兼容 |

---

## 关联文档

- [`migration-history.md`](./migration-history.md) — v2 公开 API 速查
- [`usage-guide.md`](./usage-guide.md) — 业务侧用法
- [`store.md`](./store.md) — Store 持久层（持久 active 载体）
- [`hitl-resume.md`](./hitl-resume.md) — HITL Resume（transfer 与 ask_user 共存）
- [`channel-engine.md`](./channel-engine.md) — channel 引擎（return-direct 改造点 / 入口条件路由）
