# 主题：Interrupt / Resume 语义模型

> **v2 baseline**（channel-based + agent_v2 wrapper top-level fire + 9 编排独立 Resume）：
> - `InterruptError{Reason, Query, Queries, Data}` 平铺设计保留, **Queries 新增 (ISSUE-81)** 多 query 模式——toolsFn fanout 多 tool_call 并发抛 Interrupt 用
> - `AskQuery{Question, Options, InterruptID, Reason}` 4 字段——**InterruptID 必填**（业务走 `compose_v2.Ask` 让它按 `InterruptSite` 算，或自填；框架不补，空 ID / 批内撞 ID 当场拒）; **Reason** audit #5 视角 8 F3 加, per-query 中断原因 (MergeInterrupts 自动 propagate ie.Reason → q.Reason)
> - **`InterruptError.Data` 仅同进程透传**——cp 持久化路径只存 AskQuery, Data 字段 Resume 后丢失（compose_v2/interrupt.go godoc 已警示, audit follow-up #5 修）
> - Resume API: `agent_v2.WithResumeUserInputs(map[InterruptID]*Message)` 显式 by ID
> - 节点取答案: `compose_v2.GetResumeAnswer(ctx, myID)` **唯一取法**——纯 map ID 命中（另有 `GetResumeUserText` 取非寻址型的「用户这轮说了什么」，那不是取答案）, 跨子 agent 隔离用 `StripResumeContext` / `WithSubgraphIsolatedResumeContext` opt-in (不依赖 phantom IsResumeTarget 守门)
> - **cond = 纯判断**, 由此两条 (2026-07-16 重构, 取代 ISSUE-86 D2 的"必须自填 ID"):
>   - **禁止 interrupt**: cond 抛任何 InterruptError 都被转**非-interrupt** error 拒绝。理由是语义的——中断的本质是**要新输入**; cond 要新输入才能判断 = 判断所需信息没备齐 = **上游 node 没尽责**。HITL 在 node 内抛; RoutingAgent 的 Router/classifier 已移到 classify node 内跑, 原生支持 HITL 且不必自填 ID
>   - **报错的 Resume 语义业务声明** (`WithCondFailPolicy`): `CondFailAbort` (默认, 终局失败 → wrap `ErrRunAborted` → Run 出口**主动清 cp 子树**) / `CondFailRerunFrom` (存 Pending cp, Resume 重跑 from)


## 触发场景

需要查阅本文档的设计决策：
- 改 `InterruptError` 字段 / 嵌套结构
- 改 Resume 答案配对策略（by ID / by ns / by tool_call_id）
- 设计 InterruptID 生成算法
- 决策是否引入 InterruptCtx 三态分离 / Parent 链
- 跨嵌套场景 user 答案分发机制

## 三方对照速查表（v2 baseline）

| 维度 | crew-go v2 | LangGraph | eino |
|---|---|---|---|
| **Interrupt 数据结构** | 平铺 `*InterruptError{Reason, Query, Queries, Data}`——Query (单) / Queries (多, ISSUE-81) 互斥 | 平铺 `Interrupt{value, id}` | 树 `InterruptSignal{ID, Address, State, Subs[]}` |
| **多 query 协议** | `Queries []*AskQuery` + `AllQueries()` + `MergeInterrupts()` helper (fanout 聚合) | 多 pending 全平铺 `tuple[Interrupt, ...]` | `Subs []*InterruptSignal` 树 |
| **fan-out 多分支同时中断时返给 caller 几个** | **全部**（`MergeInterrupts(res.AllInterrupts)`，2026-07-27 `4bf9568` 补齐——此前只返 FirstErr、cp 侧却记全，业务照 AllQueries() 建答案 map 系统性少答）| **全部**（`_runner.py:688-690` `# raise combined interrupts` → `GraphInterrupt(tuple(i for exc in interrupts for i in exc.args[0]))` 双层展开；与普通异常明确分开处理——普通异常 `raise exc` 直接抛第一个，中断先 `# collect interrupts` 攒起来）| **全部**（`internal/core/interrupt.go:196` `FromInterruptContexts` 收 `[]*InterruptCtx` 列表，合并公共祖先建成一棵树，注释明写 "reconstructs a single InterruptSignal tree from a **list**"）|
| **InterruptID 生成** | `InterruptSite.InterruptID()` fnv-64 hex (16 字符)，维度 = TaskID + graphNS + nodeID + 实例身份⊻visit + tc ID + tc 下标 + idx，被条件边否决重做之后再加重做代际（按分域）与外层代际链（嵌套图里） | `xxh3_128_hexdigest(ns)` 16 字符 hex | runtime 生成 + Address 关联 |
| **嵌套表达** | graphNS 编入 InterruptID + 每层 cp 物理分离 (parentSID, parentNS/childName) | `checkpoint_ns` 链 + hash 自然命中 | `InterruptSignal.Subs` 树 |
| **cond fn 协议** | **cond = 纯判断**（读 channel → 返目标，不产 writes/不改 state），由此两条: ① **禁止 interrupt**——中断的本质是要新输入；cond 要新输入才能判断 = 判断所需信息没备齐 = **上游 node 没尽责**（且 cond 是纯函数、Resume 后重新执行，答案仍得从 channel 读——绕一圈还是得让上游备好）。抛任何 InterruptError 转**非-interrupt** error 拒绝，HITL 归 node（RoutingAgent 的 Router/classifier 已在 classify node 内跑原生支持）。② **报错的 Resume 语义业务声明**（`WithCondFailPolicy`）——纯判断报错只可能是「上游数据缺失/非法」，该重试还是放弃取决于上游是否确定性（纯计算重跑必拿同样坏数据；LLM 上游重跑很可能就好），只有业务知道: `CondFailAbort`(默认，终局失败——wrap `ErrRunAborted` → Run 出口**主动 DeleteSubtree 清子树**，子图终局性经错误链自动向上传播) / `CondFailRerunFrom`(存 Pending cp、Resume 重跑 from，业务须保其 writes 幂等) | path **机械上能** interrupt，但**是实现副作用非契约**(靠 path 被 coerce 成 `trace=True` 重设 config context；官方 25 个 path 测试 0 个用、`interrupt()` docstring 明写 "from within a **node**"); **它同样做 matched-writes-skip**，但 branch 在 task **内** → interrupt 时节点 writes 压根没 commit → 恒整 task 重跑(连 LLM)，**无"已 commit 却重跑"的违约** | branch.invoke 抛 err 直接**整图 fail** (`newGraphRunError` 不做 interrupt 特判), 不支持 branch HITL |
| **Resume 答案 API** | `agent_v2.WithResumeUserInputs(map[InterruptID]*Message)` 显式 by ID | `Command(resume=val \| map)` 单值/map 二选一启发式探测（**多挂起 + 裸值一律 raise**，理由是「中断进队列的顺序不确定」——实测复现） | `BatchResumeWithData(ctx, map[id]any)` typed，**key 是每次中断现铸的 uuid 不是地址**（它自己的 godoc 写成地址串，是 stale，实测打印为 `884efdbd-…`） |
| **节点取答案** | `compose_v2.GetResumeAnswer(ctx, myID)`（**唯一取法**，另有非寻址通道 `GetResumeUserText`） | 节点调 `interrupt(value)` 自动从 ctx 取 | `GetResumeContext[T](ctx)` typed + `isResumeTarget bool` |
| **嵌套场景配对** | `ResumeContext.UserInputs` 整 map 透传 + ID 里编入 graphNS 自然分离 + opt-in `StripResumeContext`/`WithSubgraphIsolatedResumeContext` 隔离 | hash 自然命中 ns 层 (`Interrupt.from_ns(ns)`) | ctx 派生时主动扫 `id2Addr` 找前缀匹配 |
| **跨 Resume Data 持久化** | ❌ `InterruptError.Data` **仅同进程**, cp 只存 AskQuery 的字段（Question/Options/InterruptID/Reason + ToolCallID/ToolName；audit follow-up #5 godoc 已警示） | ❌ Interrupt 仅 value + id, 无独立 Data 字段 | ✅ `InterruptState{State, LayerSpecificPayload}` 落盘 + Resume 重建 |
| **业务调试可观测** | InterruptID hex 不可逆 → framework auto-fill 时 `slog.LogAttrs` 输出 5 维 (audit follow-up #5) | hash 后丢 ns, 调试需反查 | **句柄与位置是两个字段**：`InterruptCtx.ID` 是 uuid（**不是**地址串），可读位置在 `InterruptCtx.Address` |

### 2026-08-05 实测更正：中断身份的两层结构（本仓最该学的一条）

**我们的 `InterruptSite` 六维全是位置**，而位置在新任务里全部重置 ⟹ 同一 sid 下两个任务的
中断 ID **必然相同**（实测 `fde4246703b5d2aa` 一字不差）。后果不是「状态错」，是**给任务 A
的答案被任务 B 的问题消费掉**，`err=nil` 全程无告警。

**关键证据：eino 的位置维度跟我们一样撞**——同一 graph、同一 node、两个不同任务，实测
`addr` 完全相同（`runnable:root;node:lambda`）。它没出事的唯一原因是**它从不把地址当调用方
的句柄**：

- **位置**（`Address`）只用于**内部匹配**——稳定、可重算、天然处理嵌套前缀
- **句柄**（`InterruptCtx.ID`）是**发生时现铸的 uuid**，随存档持久化、加载时读回
- 存档里存二者的**绑定**（`InterruptID2Addr`）

五家逐个查完：**没有一家把纯位置当调用方句柄。** 另外两条路线：

- **LangGraph**：确定性重算，但掺进「本次执行从哪份存档起跑」的 `checkpoint_id`。挂起期间
  那份 cp 不推进 ⟹ 每次 Resume 重算得同值（实测：未答的那个 ID 跨一轮往返一字未变），
  而新任务换了 cp ⟹ ID 不同。**既掺了执行维、又不用把 ID 存下来。**
- **MAF**：`request_id = uuid4()` 现铸 → 存 `pending_request_info_events` → `apply_checkpoint`
  读回。同路线 ②。

**护住它们的不是校验**：LangGraph 和 eino 对「答案 ID 对不上」**完全静默**（比我们的 warn
还松）。静默无害，因为**撞不上**。我们的 `warnUnmatchedAnswerIDs` 位置是对的，它失灵的唯一
原因是被比较的那把键不带发生维——陈旧答案不是 unmatched，而是**正好匹配上了新任务的问题**。

⚠️ **`Checkpoint.Epoch` 不能直接拿来当这一维**：它每次 `SaveNS` 都换（`Durability=Sync` 每
超步一次），是「写代」不是「任务代」，而中断 ID 必须跨 Resume 稳定。要的是**只在新开路径
铸一次、此后随 cp 一路带下去不换**的独立字段（= 路线 ②）。


## crew-go v2 设计

### Interrupt 数据结构

`compose_v2/interrupt.go`:
```go
type AskQuery struct {
    Question    string   `json:"question"`
    Options     []string `json:"options,omitempty"`
    InterruptID string   `json:"interrupt_id,omitempty"` // 框架按 InterruptSite 派生（Ask 不接受自填）
    Reason      string   `json:"reason,omitempty"`       // per-query 中断原因 (audit #5 视角 8 F3 加; MergeInterrupts 自动 propagate ie.Reason → q.Reason)
}

type InterruptError struct {
    Reason  string
    Query   *AskQuery   // 单 query 模式 (99% 场景)
    Queries []*AskQuery // 多 query 模式 (ISSUE-81 toolsFn fanout, 与 Query 互斥)
    Data    any         // ⚠️ 仅同进程, cp 不持久化
}
```

**Query/Queries 互斥**——`AllQueries()` helper 自动 fallback（Queries 优先, 否则 [Query]）。`MergeInterrupts()` 聚合多 InterruptError 到单 ie.Queries 让 graph_loop record 全部 query。

### InterruptID 生成

`compose_v2/interrupt.go` 的 `InterruptSite.InterruptID` (按符号自查, 防 line drift)——
坐标是一个 struct 而不是一串位置参数，因为**维度加过两次**（`ToolCallIdx` / `TaskID`），
而当年那份「另写一遍字段列表」的字面量两次都漏跟，症状是跨 tc / 跨任务的 ID 撞。

维度: TaskID (同会话里的不同任务) + graphNS (嵌套层) + nodeID + **InstanceID ⊻ visit**
+ ToolCallIdx (节点内第几个 tool_call) + idx (该分支内第几次问)。

⚠️ **InstanceID 与 visit 互斥**（有实例身份时 visit 置 0）: `visitCount` 是 per-NodeID
**共享**计数器，成功的兄弟实例会把它推高 ⟹ 中断实例重跑时读到的值跟中断当时不同 ⟹ ID
漂移（实测 3 实例场景 Run 1 的 visit=0 在 Resume 时变成 2）。visit **仅成功路径**
IncrVisitCount，故重 retry 时稳定。

framework auto-fill 路径 (graph_loop.go:1221) 配 `slog.LogAttrs` 输出 5 维——业务开 DEBUG level 反查 hex 对应位置, 不影响生产。

### Resume 答案 API

```go
// ⚠️ 一律走 AllQueries()：同轮多个 tool_call 都要人答时框架合成一条中断
//（Queries 容器），那时 ie.Query 为 nil，写 ie.Query.X 会 panic。
answers := make(map[string]*schema.Message)
for _, q := range ie.AllQueries() {
    answers[q.InterruptID] = schema.UserMessage("ok")
}
events, err := ag.Resume(ctx, sessionID, agent_v2.WithResumeUserInputs(answers))
```

9 编排都通过 `Agent.Resume(ctx, sid, opts...)` 统一接口 (`agent_v2/{basic,sequential,parallel,loop,routing,reflect,handoff,plan,graph}_agent.go::Resume`). Resume 内调 `applyResumeOptions(opts)` 解析 `WithResumeUserInputs / WithResumeAction(Abort/Replan)` 等。

### 节点取答案

`compose_v2/interrupt.go`（`GetResumeAnswer`）:
```go
// 知道自己 InterruptID 时 (典型: tool 实现)
if msg, ok := compose_v2.GetResumeAnswer(ctx, myInterruptID); ok { ... }

// 不知道 ID 时 (单 pending) — 直接 map 第一项 + 多 pending warn
if msg, ok := compose_v2.GetResumeAnswer(ctx, myID); ok { ... }
```

`GetResumeAnswer` 实现是**纯 map lookup**（按 InterruptID 精确命中，无 ns 自检）。跟 LangGraph `interrupt()` 的 hash 自然命中模式等价（不做显式 self/ancestor/sibling 自检）。

⚠️ **2026-08-06 更正**：这里原先还列了个 `GetResumeAnswerSingle`（取 map 里任意一条），**已删**。对照下来五家**没有一家把整个答案 map 交给节点**——要么只给它属于自己的那条，要么让它拿自己的 ID 去反查，「取任意一条」这种 API 别处不存在。我们那个实测会让节点吃到别人的答案（用户批准 A、系统执行 B，`err == nil`）。非寻址型的「用户这轮说了什么」现在走独立通道 `GetResumeUserText`。

### 嵌套场景配对

`ResumeContext.UserInputs` 整张 map 透传到子 agent (默认), framework 不做 ns 自检守门——子 agent 节点直接 `GetResumeAnswer(ctx, id)` 按 ID 拿答案. 嵌套隔离靠两层机制:

1. **ID 编入 graphNS**: 父子 agent 节点 ID 天然分离, **不会撞**
2. **opt-in `StripResumeContext` / `WithSubgraphIsolatedResumeContext()`** (compose_v2): 业务想完全隔离子 agent 看不到父答案时显式调用——不调用时按"全透传"语义跑

⚠️ **历史 doc 修正** (2026-05 audit #5): 多份 doc (interrupt-resume.md / address-routing.md / channel-engine.md / issue-70-checkpoint-evolution.md) 曾描述 v1 时代 `IsResumeTarget(currentNS, ...)` self/ancestor/sibling 三模式守门 helper——但**该函数在 v2 不存在** (grep `func IsResumeTarget` 返 0 hits). v1 → v2 演化已删此自检 helper, 走 LangGraph 风格 map ID 命中, 等价能力通过 graphNS 编入 + opt-in strip 实现。doc audit follow-up #5 已统一删除 phantom 引用。

### cond fn 协议：禁止 interrupt (2026-07-16 重构，取代 ISSUE-86 D2)

`compose_v2/graph_loop.go` computeNextTasks 的 cond 分支: cond fn 抛**任何** InterruptError（不管 `Query.InterruptID` 填没填）→ framework 一律转**非-interrupt** error 拒绝 + 引导改用 node。

**根本理由（第一性，语义论证）**: cond 是**纯判断**——读 channel state → 返目标节点，不产 writes、不改 state。而**中断的本质是要新输入**；cond 需要新输入才能判断，说明**判断所需的信息还没备齐**——那是**上游 node 没尽责**，不该由 cond 兜。让 cond 中断还自相矛盾：cond 是纯函数，Resume 后它要重新执行，用户答案只能从 channel 读——**绕一圈还是得让上游把答案备好**，那不如一开始就让上游 node 去问。所以 HITL 一律归 node：node 负责把判断所需的一切备齐（含必要时中断问用户、答案写 channel），cond 只管拿现成信息判方向。（原 D2 只治 ID 漂移这个**症状**，没治「cond 能不能 interrupt」这个**根因**。）

> 此论证**不依赖 PA-H1**（就算没有那套机制，cond interrupt 照样是错的），也**不会 proves-too-much**——cond **报错**不是"要新输入"、是"判断前提不成立"，两码事，故不受本论证牵连，另由 CondFailPolicy 处理（下）。

**cond 报错（≠ 中断）的 Resume 语义 → 交业务声明**（`WithCondFailPolicy`，2026-07-16）: 纯判断报错只可能是「判断的前提不成立」——上游产出的数据缺失/非法（实测框架自带 7 个 cond 里只有 plan / routing 会报错，报的全是这类）。**该重试还是该放弃，取决于上游是不是确定性的，只有业务知道**：纯计算上游重跑必拿同样的坏数据（白烧）；LLM 上游（`LLMPlanner.Plan` → `Generate`）重跑很可能就好了。故不硬编码：`CondFailAbort`（**默认**，终局失败——wrap `ErrRunAborted`，Run 出口 defer 据此**主动 `DeleteSubtree` 清子树**；结构性问题多为确定性，fail-fast 比给业务"重试能好"的假象诚实。**注意「跳过写」不够**：cp 单槽覆盖式，跳过写只是不覆盖，先前的 HITL 档 / durability 中途 cp 会幸存且 Status=Pending → auto-load 捡起 → 重跑已成功节点 / 重问已答问题 / 子图 livelock，见 `3f88ad0`）/ `CondFailRerunFrom`（保 frontier=[from] + 存 Pending cp → Resume 重跑 from；**业务须保 from 的 writes 幂等**——框架自带编排的 from 只写 LastValue/OnceFlag 安全，业务图若写 Append/Sum 会双计）。ISSUE-86 MED-1/MED-3 的 frontier 收缩精度在 RerunFrom 档保留。

**转非-interrupt 是关键**: 若让 `IsInterrupt` 仍 true 但框架已不 `recordInterrupt`，caller 会当 HITL 等答案却无 ID 可配 → Resume 永久死锁。

**唯一路径**: HITL 在 **node 内**抛（nodeID + visit 协议稳定，节点失败不 IncrVisit 故跨 Resume ID 不漂；且 node writes 未 commit 就中断，重跑天然自洽、无"已 commit 却重跑"的违约），后接 cond 读 channel/state 路由。RoutingAgent 的 Router/classifier 已整体移到 `classify` node 内跑（node 调 router → 写 targetCh；cond 纯读），业务因此可直接 HITL 且**不必自填 ID**。

**三方对照（2026-07 实证修正，勿再沿用旧说法）**:
- **LangGraph 也做「成功成员不重跑」**——matched-writes-skip 属实（`main.py:2966-2968` `match_cached_writes()` 让成功兄弟 `cached=True` 复用、`runner.tick` 只跑 `writes` 为空的 task），与本文 §演进表 R33 PA-H1 行的描述一致。**差异不在有没有 skip，而在被中断成员自身的重跑粒度**: 它的 branch 编译进 source node 的 task（`graph/state.py:1610`），interrupt 时 `_runner.py:585-591` 只存 `(INTERRUPT, ...)`、**不存节点 writes** → 该 task writes 为空 → 恒整 task 重跑（连 LLM——`interrupt()` 的 counter 只让 interrupt 调用本身幂等，**不兜 LLM 重跑**）。**它没有"已 commit 却重跑"的违约，因为 writes 压根没 commit**。crew-go 的 cond 在 task **之外**（node 已 commit + IncrVisit 后才评估）才产生"已成功却被拉回"的中间态——这才是我们必须禁的原因。
- **path 能 interrupt 是实现副作用、非契约**: 机械上能（真实原因是 path 被单独 coerce 成 `trace=True`（`graph/state.py:1006`）在调用时重新 `set_config_context`——**不是** "writer 身份/同 task 有 scratchpad"：`_internal/_runnable.py:672-686` 明确不给 writer 设 config context）。但官方 25 个 conditional path 测试 **0 个**调 `interrupt()`，`interrupt()` docstring（`types.py:812/824`）明写 "from within a **node**"。规律仍是「要 HITL/checkpoint 复用 → 必须 node」（旗舰 `tools_condition` = LLM-in-node + edge 读 `.tool_calls`）。
- **eino**: branch condition 抛 error 直接整图 fail（`newGraphRunError` 不做 interrupt 特判），不支持 branch HITL；LLM 一律在 ChatModelNode/LambdaNode（穷尽搜两 repo，branch 用法零处调 LLM），condition 基本纯读（`flow/agent/multiagent/host/compose.go:233-238` 有 condition 经 `ProcessState` **写** state 的例外）。
- 方向一致（判断/干活分离），但我们的 cond 是**框架生成**的（业务只写 Router 在 node 跑），业务/框架分离比两者更彻底。

## LangGraph 设计（v0.6.x 当前）

### Interrupt 数据结构

`langgraph/types.py:148` (v0.6 起 `ns/when/resumable/interrupt_id` 4 字段已删):
```python
@final
@dataclass(init=False, slots=True)
class Interrupt:
    value: Any  # The value associated with the interrupt
    id: str    # xxh3_128_hexdigest of ns

# 多 interrupt 平铺在 PregelTask.interrupts: tuple[Interrupt, ...]
```

**单层平铺**, ns 信息已被 hash 吃掉, 运行时仅看 ID 字符串。

### InterruptID 生成

`types.py:190-191`:
```python
@classmethod
def from_ns(cls, value: Any, ns: str) -> Interrupt:
    return cls(value=value, id=xxh3_128_hexdigest(ns.encode()))
```

### Resume 答案 API

`Command(resume=value)` 单值或 `Command(resume={id: value})` map 二选一. Resume 注入逻辑 inline 在 `pregel/_loop.py` (line ~622-639, 当前版本无独立 `_put_resumes` 函数), 根据 `isinstance(resume, dict) and all(is_xxh3_128_hexdigest(k) for k in resume)` 探测 map 形态——map 形态 inject `_scratchpad.resume_map[id] = value`; 单值多 pending 时 RuntimeError.

map 形态由 "全是 hex" 探测——没有显式标志位。多 pending 时单值 → 框架报错。

### 节点取答案

节点直接调 `interrupt(value)`，框架从 ctx 取 `_scratchpad`（`types.py:501-518`，**index-based**）：
```python
def interrupt(value: Any) -> Any:                  # types.py:403
    scratchpad = get_config()["configurable"][CONFIG_KEY_SCRATCHPAD]
    idx = scratchpad.interrupt_counter()           # 节点内自增计数（按调用顺序，非 ns hash 内联）
    if idx < len(scratchpad.resume):
        return scratchpad.resume[idx]              # 按"节点内第几个 interrupt"匹配 resume 值
    # 无答案 → raise GraphInterrupt（触发 cp save）；resume 时从节点开头 re-execute 全部逻辑
```

> `namespace_hash` 配对发生在 **scratchpad 构造期**（`_loop.py` 按 `Command(resume={id:v})` 的全-hex key 探测注入 `resume_map`），**不在 `interrupt()` 内联**——节点内取答案纯按 `idx` 顺序。高层"按 ns hash 配对"结论方向对，但 `interrupt()` 函数体是 index-based。

### 优势

- ID 定长（16 字符），ns 多深都不膨胀
- 节点 API 极简：调 `interrupt(value)` 一行
- 嵌套场景靠 hash 自然命中，无需自检 helper

### 缺陷

- ID 是 hash，调试需要反查 ns
- 单值 vs map 形态由"全 hex"启发式探测——业务有 hash-like ID 时会误判

## eino 设计

### Interrupt 数据结构

`internal/core/interrupt.go:32-38`（**3 个嵌入字段**，非命名字段/指针）：
```go
type InterruptSignal struct {
    ID string
    Address          // 嵌入字段（embedded，非命名 "Address Address"）
    InterruptInfo    // 嵌入字段（值嵌入，非 *InterruptInfo 指针）；含 IsRootCause bool
    InterruptState   // 嵌入字段（值）
    Subs []*InterruptSignal  // 显式树结构
}
```

**树结构**——`Subs` 表达"父 graph → 子 graph → tool"嵌套链。落盘时 `SignalToPersistenceMaps`
拍平为 `id → Address` + `id → InterruptState` 双 map。

### Resume 答案 API

`internal/core/address.go:249-273`：
```go
func BatchResumeWithData(ctx context.Context, dataByID map[string]any) context.Context {
    // 注入全局 id2ResumeData map 到 ctx
}
```

### 节点取答案（typed）

`compose/resume.go:77`（转发 `internal/core/resume.go:86`）——**3 个裸返回值，无包装 struct**：
```go
func GetResumeContext[T any](ctx context.Context) (isResumeFlow bool, hasData bool, data T)
```

节点范式：`isResumeFlow, hasData, data := compose.GetResumeContext[MyT](ctx)`。`isResumeFlow`
标识本组件是否 resume 目标（自定向 / 作为子流程导管两种角色，见 godoc）；`hasData` 区分
"恢复但无数据"（`Resume(ctx, ids...)`）vs"恢复且带数据"（`ResumeWithData`）。

### InterruptCtx 三态

`internal/core/interrupt.go:113-127`：跨包 user-facing 视图：
```go
type InterruptCtx struct {
    ID          string         // 全限定地址（join Address 段，如 "agent:A;node:graph_a;tool:..."）
    Address     Address        // 命名字段（注意：InterruptSignal 用嵌入字段，InterruptCtx 用命名字段）
    Info        any            // 用户面信息
    IsRootCause bool           // 是否中断根因
    Parent      *InterruptCtx  // 父链表，top-level 为 nil
}
```

**注意**：这不是"执行/中断/恢复运行时三态分离"，而是"内部 InterruptSignal 树 vs 外部公开扁平视图"双形态。eino 用 `InterruptCtx` 让 adk → compose 跨包传递 + 按 `allowedSegmentTypes` 过滤地址段（隐藏中间层 nodeID）。

### 优势

- 显式树结构精确表达嵌套
- typed `GetResumeContext[T]` 编辑器自动补全
- `InterruptCtx.Parent` 链支持业务定制视图过滤

### 缺陷

- `InterruptSignal.Subs` 树构造 + 落盘双 map 拍平复杂度高
- typed 业务侧成本（`map[id]any` 仍要做类型断言）
- 三态分离的 `InterruptCtx` 是为跨包视图，单包项目用不上

## 我们的优势（v2 保留不动）

1. **`AskQuery` 直接含 `Question/Options`**：业务直接用，不需要再 unwrap typed payload；前端 SSE 渲染极简
2. **坐标显式**：与 LangGraph 同样 16 字符 hex 定长，但 `InterruptSite` 的各维度是显式字段——业务侧出 ID 可追溯。LangGraph hash `"|".join(ns)` 维度隐含在 ns 派生里，调试反查难
3. **v2 走 LangGraph 风格纯 map ID 命中** (无 self/ancestor/sibling 自检): 嵌套靠 ID 里编入 graphNS + opt-in `StripResumeContext` 隔离. 对比 eino 在 `AppendAddressSegment` (76 行) 耦合"自身或后代"派生+resume——crew-go v2 完全删自检走 map lookup 协议更简
4. **Query/Queries 互斥协议 (ISSUE-81) + MergeInterrupts**：toolsFn fanout 多 tc 同时抛 Interrupt 时 framework 聚合到单 ie.Queries 让 graph_loop record 全部 query——LangGraph 单值/map 启发式探测有 footgun (业务 ID 撞 hex 时误判); eino Subs 树构造 + 落盘双 map 拍平复杂度高
5. **`slog.LogAttrs` 输出各维度 (audit follow-up #5)**：业务开 DEBUG level 即可反查 hex ID 对应位置——LangGraph hash 后丢 ns 调试需手动反算 (xxh3 几乎不可能); eino Address 字段强保留代价跨包视图重

## 已知 gap (评估后不抄)

### 嵌套 sibling 表达力 (vs eino Subs 树)

eino `InterruptSignal.Subs []*InterruptSignal` (interrupt.go:32-38) + `InterruptCtx.Parent` 链表让父子嵌套 Interrupt 可拿"父等子等孙"完整结构链, caller 能从 cp 直接读出 `parent.Subs[0].Subs[1]` 嵌套关系做 UI tree 渲染.

crew-go v2 把 graphNS 编进 ID 的 hash 表达嵌套——`cp.PendingInterrupts []*AskQuery` 是**平铺 slice 无显式 Parent 指针**, caller 想"父子 query 关系"只能从 InterruptID 的 graphNS 子串前缀解析 (隐式).

- LangGraph 同款平铺无 Parent 字段, 我们对齐 LangGraph 不对齐 eino
- 业务想嵌套 UI 树时通过解析 `q.InterruptID` graphNS 部分自构造 (业务 hint)
- 不抄 eino Subs 树: 增加 cp 序列化复杂度 + cross-ns ie 引用增加 cp 大小, 平铺
  设计更简单 + 业务无强需求

## 我们的不足（已评估不抄）

1. **typed Resume Data**（eino 的 `GetResumeContext[T]`）
   - 业务塞 `Data any` 到 InterruptError，跨 cp Resume 时 **直接丢失** (audit follow-up #5 godoc 已警示——cp 只存 AskQuery 的字段 Question/Options/InterruptID/Reason + ToolCallID/ToolName，不含 Data)
   - **不做**: Go 无变长泛型 method, 强制 typed 会破坏 Hook / Tool 接口; 业务想跨 Resume 保元数据走 (a) AskQuery.Question/Options 自序列化 (b) `store.Store` 后端 (Redis/PG) 按 InterruptID 关联 (跨进程持久——**不是** `RunSession.SetValue/GetValue`，那是 per-sess 不走 cp，跨 Resume 同丢)

2. **eino `InterruptCtx.Parent` 链表跨包视图**
   - eino 跨 adk → compose 包用 InterruptCtx 让父子 InterruptSignal 跨包传 + `allowedSegmentTypes` 过滤
   - crew-go v2 单包 `compose_v2 + agent_v2`, `AskQuery.InterruptID` 已含 graphNS 维度足够表达父子, doc 决策记录已记
   - **不抄**: 单包项目无此场景

3. **`MED-2 audit 报告` 9 编排 Resume 入口 boilerplate (~50 行 × 9)**
   - 历史教训 (commit `dc142c3` isAgentNil 漏 SetExecutor + WithLLMPlannerSubAgents 2 处) 印证协议变更四复证陷阱
   - 抽 `commonResumePrep` helper 减 ~250 行 + 防漏修
   - **已闭合 (commit `09f21a6`)**: 抽 `commonResumePrep` helper 9 编排全调 — `AbortDone` / Cper / Rcfg / Session 4 件返回统一; 协议变更四复证陷阱已防 (新 编排加 Resume 入口必复用 helper)

## 我们已偏离三方但更优（v2 保留不动）

1. **嵌套不冒泡污染**（graphNS 编入 InterruptID + 每层 cp 物理分离）: 内层 cp 持有 query 真相, 外层 cp 不重复写—— LangGraph hash 一样会自然命中本层 ns, 但若多 pending 时无显式判断; eino Subs 树要求父持有子的引用
2. **嵌套共享父 RunSession** (ISSUE-70 H-B): 嵌套子 agent Resume 时透传父 sess 让 events 写父 sess.History 统一审计——LangGraph / eino 都没 RunSession 概念
3. **cond fn 协议：cond = 纯判断** (2026-07-16 重构取代 ISSUE-86 D2)，由此两条：**① 禁止 interrupt**——抛任何 InterruptError 转非-interrupt error 拒绝。**第一性理由（语义，不依赖 PA-H1）**: 中断的本质是**要新输入**；cond 要新输入才能判断 = 判断所需信息没备齐 = **上游 node 没尽责**。且 cond 是纯函数、Resume 后要重新执行，答案仍只能从 channel 读——绕一圈还是得让上游备好。故 HITL 归 node（协议稳定 + writes 未 commit 就中断、重跑天然自洽）；RoutingAgent 的 Router/classifier 已移到 classify node 内跑，业务可直接 HITL 且不必自填 ID。D2 只治 ID 漂移症状、没治「cond 能否 interrupt」根因。**② 报错的 Resume 语义交业务声明**（`WithCondFailPolicy`）：纯判断报错 = 上游数据缺失/非法，该重试还是放弃取决于上游是否确定性（只有业务知道）→ `CondFailAbort`(默认，终局失败：wrap `ErrRunAborted` → Run 出口主动清 cp 子树) / `CondFailRerunFrom`(存 Pending cp、Resume 重跑 from)。**三方（2026-07 实证修正）**: LangGraph **同样做 matched-writes-skip**（成功兄弟 `cached=True` 不重跑）——差异不在有没有 skip，而在 branch 在 task **内**：interrupt 时节点 writes 压根没 commit → 恒整 task 重跑（连 LLM），**无"已 commit 却重跑"的违约**；其 path 能 interrupt 是实现副作用非契约（官方 path 测试 0 个用、docstring 明写 node）。eino branch.invoke 抛 err 直接整图 fail，不支持 branch HITL。方向一致但我们的 cond 是**框架生成**的（业务只写 Router 在 node 跑），业务/框架分离更彻底
4. **业务调试可观测** (audit follow-up #5): framework auto-fill InterruptID 时 `slog.LogAttrs` 输出 5 维, 业务开 DEBUG level 即可反查——LangGraph 无 equivalent (hash 单向); eino 用 Address 字段持续保留维度代价跨包视图重

## 「已成功的工作单元 Resume 时不重跑」的落点（2026-07-29 补，源自 tool 重执行 backlog）

问题：tools 节点一个 tool_call 抛中断 → 整节点失败 → 写回滚 → Resume 时 N 个 tool_call **全部**重跑，已成功的那几个副作用来第二遍。我们现有的说法是「业务工具自负幂等」——那是把框架能解的推给每个业务工具，而 HTTP 调用 / 写库 / 发消息天然不幂等。

| | 记什么 | 存哪 | 粒度 | 「跑过但没产出」怎么表达 |
|---|---|---|---|---|
| LangGraph | 每个 task 的 writes `(task_id, channel, value)` | cp 的 `checkpoint_pending_writes` | **task**（一节点 = 一 task；`Send` 各自成 task）| 补一条 `NO_WRITES` 哨兵（`_runner.py:611`）|
| eino | 每个 tool call 的输出 `map[callID]output` | **中断错误自身** | **tool call** | 不需要——只在中断时才存在，成功路径压根不建 |
| 我们 | 超步成员身份 + 其 sends | `cp.CompletedFrontier` / `DeferredSends` | 超步成员（节点实例）| 成员列表里在即算跑过 |

**LangGraph 的判定就一行**：`_runner.py:746` `elif next_task.writes:` 注释直书 *"if it already ran, return the result"*——有 writes 的 task **不执行**，结果从存下的 writes 重建；主循环侧对称地按 `if t.writes` / `if not t.writes` 分流（`main.py:1246` / `:1259`）。中断的 task 只写了控制信号、恢复时被排掉 ⟹ writes 空 ⟹ 重跑。

**eino 的判定同样一行**：对端仓 `compose/tool_node.go:806` `if result, executed := executedTools[toolCall.ID]; executed` 直接跳过那个 tool call，`executedTools` 由 `GetInterruptState[*toolsInterruptAndRerunState](ctx)`（对端仓 `compose/resume.go:32`）取回。

### 两个我们缺的细节

**① 中断载荷是两份不是一份。** eino 的 `CompositeInterrupt(ctx, info, state, errs...)`（对端仓 `compose/interrupt.go:174`，调用点 `compose/tool_node.go:1140` / `:1240`）——`info` 给调用方看（UI 渲染），`state` 是**节点私有、随 cp 持久化、只有它自己重跑时读**。

我们只有前者：`AskQuery` 进 `cp.PendingInterrupts`。后者对应的 `InterruptError.Data`，godoc 自陈「**仅同进程透传，跨 Resume 丢失**」（`compose_v2/interrupt.go:84-107`），`AskQuery` 也没有放它的字段。

⚠️ **这跟下面决策记录里 2026-05 那行「不抄 eino typed Data 落盘」是同一件事，但当时只从业务视角评估**（业务用 `map[string]any` 够了 + 跨 thread 走 `store.Store`），没人追到**框架自己也需要这个槽**。后果是：框架要实现 tool 粒度的 skip 时**没有正确的落点**，只能去够别的东西——实际发生过（`973490a` 借 `RunSession` KV，父子共享指针 + 无清除点，7 条 HIGH 后撤回）。**这不是"当时决策错了"，是"决策的射程没覆盖到框架自用"**。

**② 「跑过但没产出」必须能跟「还没跑」区分开。** LangGraph 专门补 `NO_WRITES` 哨兵（`_runner.py:609-611`），不靠「writes 为空」推断。撤回的那版也没这个区分。

### LangGraph 在工具粒度上同样没解决

它的单位是 task，一个 ToolNode 里 N 个工具共用一个 task ⟹ 一个中断整节点重跑 —— **跟我们现在完全一样**。真做到工具粒度的只有 eino，办法是把地址往节点**里面**多接一段（`AddressSegmentTool`，对端仓 `compose/interrupt.go:279`；调用点 `compose/tool_node.go:1112` 把 `callID` 作为地址段拼上去）。

### 结论

正解不是「一个 tool memo」，是**补一个缺失的原语**：节点私有的、随 cp 持久化的中断态。补上之后 ——

- tool memo 只是它第一个使用者，不是自己一套机制
- 生命周期天然对：只在中断时存在，节点跑完 cp 就没了，**不需要清除点**（撤回那版全部 7 条 HIGH 里有 4 条是清除点引出来的）
- 键天然对：跟着中断地址走，而我们的 `InterruptSite` 里 `ToolCallIdx` **那一维已经在了**，正是 eino 多接的那一段

我们已有的 `CompletedFrontier` / `DeferredSends` 是同一个思路（R33 PA-H1，见决策记录），只是停在节点这一层没往里走。

**已落地**（2026-07-30）：原语 = `compose_v2.SaveNodeState` / `GetNodeState`（`compose_v2/node_state.go`），第一个使用者 = tools 节点的 `toolCallMemo`（`agent_v2/tool_memo.go`，键 `(tc.ID, 批内下标, 工具名)`，判据 `shouldMemoize`）。业务侧语义与残余幂等边界见 [hitl-resume.md §6.2 / §6.3](../../v2/hitl-resume.md)；可跑对照 `example/harness/node_state_demo`。

## 决策记录

| 时间 | 决策 | 借鉴 | 不全抄理由 |
|---|---|---|---|
| ISSUE-58 | `AskQuery` 含 `Question/Options/ToolCallID` | LangGraph `interrupt(value)` | 不抄 value any —— typed 字段对前端 SSE 友好 |
| ISSUE-59 P1 | `WithResumeUserInputs(map[ID]Message)` | LangGraph `Command(resume=map)` | 不抄启发式单值/map 探测——显式 map 永远统一 |
| ISSUE-70 波 1.1 | (v1 时代) `IsResumeTarget` 三态自检（self/ancestor/sibling）+ `GetResumeAnswerSingle` 守门 — **v2 已删自检, 走 map ID 命中；`GetResumeAnswerSingle` 本身 2026-08-06 也已删** | eino "自身或后代" + 我们扩兄弟 | (v1 时代理由, v2 已重新决策走 map 命中删自检; 见 2026-05 audit follow-up #5 修正) |
| ISSUE-70 Phase 2 Step Ⅰ | `query.NS` 指纹判断嵌套冒泡（避免外层 phantom tool_result） | （我们独有） | LangGraph hash 自动命中本层；我们物理分离需显式判断 |
| ISSUE-70 Phase 2 Round 1 H-B | `ResumeOptions.ParentSession` 嵌套共享 sess | （我们独有，因为有 RunSession 概念） | — |
| ISSUE-70 Phase 3 | **保留 `*InterruptError + ie.Query` 平铺，不引入 InterruptCtx 三态** | （决策记录） | eino `InterruptCtx` 解决跨包问题，我们 compose 是顶层无此场景 |
| **ISSUE-86 HIGH-2 (2026-05-16, commit 4282548/da90ffc)** | **方案 D2: cond fn 抛 InterruptError 时 `Query.InterruptID` 空 → return error fail-fast** | LangGraph 设计哲学 (interrupt 只能 node 内) + eino branch.invoke 抛 err 直接 fail | **协议根因修**: 旧 visit-1 自动算 ID 缓解跨 Resume 仍漂 (节点重跑 IncrVisit 累加). ⚠️ **本行当年据以立论的 LangGraph 描述「通过 `CONFIG_KEY_SCRATCHPAD` 限制 cond eval 时 ctx 无 SCRATCHPAD → 业务不能在 cond 内调 interrupt()」经 2026-07 本地源码 + 实跑核查为**误**——path 被单独 coerce 成 `trace=True` 会重新 `set_config_context`，机械上**能** interrupt（且该说法至少近两年不成立）。正确表述见下一行与 §cond fn 协议。 eino branch.invoke 抛 error 直接 fail 无 InterruptError 特殊路径 (此条属实). crew-go D2 学三方哲学但保留功能 — 业务**可** cond 内抛 InterruptError, 但**必须**自填 InterruptID 或通过 sub-agent.Run 透传 (sub-agent 内 ask_user 已算稳定 ID). framework 不再 auto-fill visit-1 ID. 合法路径: (a) sub-agent.Run 透传 (RoutingAgent.classifier 典型, BasicAgent + ask_user 内自带稳定 ID); (b) 业务自填 caller-stable identifier. 非法 (D2 拒绝): cond fn 自构造 `&InterruptError{Query: &AskQuery{Question: "..."}}` 不填 ID — framework return error 拒绝, 业务必须改用 node-level interrupt + 后接 cond 读 sess state 路由. **修法演进**: panic-fast (4282548) → return error (audit follow-up da90ffc, 视角 2 HIGH-1: panic 在 computeNextTasks 冒泡 caller crash + 跳过 Run defer cp save → 改 error 让 defer 正常). ⚠️ **本方案已被下行 (2026-07-16) 取代**——D2 代码与配套测试已移除，本行为历史记录。 |
| **cond 禁止 interrupt (2026-07-16, commit 70e0fcb)** | **cond fn 抛任何 InterruptError 一律转非-interrupt error 拒绝** + RoutingAgent 的 Router/classifier 从 ConditionalEdge 整体移到 classify node | LangGraph（**2026-07 实证**）: **同样做 matched-writes-skip**（`main.py:2966-2968` 成功兄弟 `cached=True` 不重跑）——差异不在有没有 skip，而在 **branch 在 task 内**：interrupt 时 `_runner.py:585-591` 只存 `(INTERRUPT,…)` 不存节点 writes → 该 task writes 空 → 恒整 task 重跑（连 LLM；counter 只让 interrupt 调用幂等、不兜 LLM），**无"已 commit 却重跑"的违约**；path 能 interrupt 是**实现副作用非契约**（靠 coerce `trace=True` 重设 config context，非 writer 身份；官方 25 个 path 测试 0 个用、docstring 明写 "within a node"）；旗舰 `tools_condition` = LLM-in-node + edge 只读 `.tool_calls`。eino: branch.invoke 抛 err 直接整图 fail (`newGraphRunError` 不做 interrupt 特判)，官方 router (host multiagent) / react 全是 LLM-in-node（branch 用法零处调 LLM），condition 基本纯读（host multiagent `compose.go:233-238` 有 condition 写 state 的例外） | **根因修 (D2 只治症状)**: 引擎深审 F-A 揭露 **ID 稳不稳定不是根因，cond 能不能 interrupt 才是**。**第一性判断（语义论证，不依赖 PA-H1）**: cond 是**纯判断**；中断的本质是**要新输入**；cond 要新输入才能判断 = 判断所需信息没备齐 = **上游 node 没尽责**。且 cond 是纯函数、Resume 后要重新执行，答案仍只能从 channel 读——绕一圈还是得让上游备好，那不如一开始就让上游去问。故 HITL 一律归 node。**此论证不 proves-too-much**：cond **报错**不是"要新输入"而是"判断前提不成立"，两码事，另由 CondFailPolicy 处理（见 `67e0aa7` 行）。（对照: LangGraph 的 branch 在 task 内，interrupt 时 writes 压根没 commit → 恒整 task 重跑、无违约，故它没有我们这个"已 commit 后才评估"的中间态——**不是"它弃了不重跑"**，它同样做 matched-writes-skip。）**关键实现**: 必须转**非-interrupt** error（不 `%w` 原 err）——否则 `IsInterrupt` 仍 true 但框架已不 `recordInterrupt`，caller 当 HITL 等答案却无 ID 可配 → Resume 永久死锁。**RoutingAgent 无损**: Router/classifier 移到 classify node 后走标准 node interrupt（nodeID+visit 稳定、节点失败不 IncrVisit、writes 未 commit 就中断故重跑自洽），业务反而**新增**能力——显式 Router 也能 HITL 且不必自填 ID (framework auto-fill 5 维)。我们的 cond 是**框架生成**的（业务只写 Router 在 node 跑），业务/框架分离比三方（path 仍是业务判断函数）更彻底。测试: TestCondInterrupt_AlwaysRejected (三态全拒 + fan-out 防假绿) + TestRoutingClassifier_HITL_ResumeClosedLoop (补全库缺失的 classifier Resume 闭环)。 |
| **终局失败主动清 cp 子树 (2026-07-16, commit `3f88ad0`)** | **`ErrRunAborted` 哨兵**: Abort 档 wrap 之 → Run 出口 defer `errors.Is` 命中 → **主动 `DeleteSubtree`**（与 cleanupOnSuccess 共用 `purgeExitSlot`），而非"跳过 SaveNS" | **两家的模型天然没有这个坑，但原因不同**: LangGraph 是 **append-only 父链**（`Checkpoint.id` 单调 + `parent_config`；InMemory `max(ids)` / SQLite `ORDER BY DESC LIMIT 1` 取 latest）——从不覆盖、resume 恒取 latest，**结构上不可能捡到更旧的**；失败时只是"不推进 cp"，latest 仍是「末个成功超步 + owner-tagged pending writes」这个**合法 resume 点**（连 durability=exit 失败退出也写）。eino 则**同样暴露**（同 ID 失败重跑会捡起更旧的 interrupt cp，compose 层零防护），它的解法是**分层**：引擎只 interrupt→写/resume→读、零删除点，把"中断已消解就删 cp"外移到编排层（ADK `turn_loop.go:2027` 干净退出且曾 load 过才删）+ **可选** `CheckPointDeleter` 接口 + 明文文档"不实现就不清理"；另给业务 `WithForceNewRun` / `WithWriteToCheckPointID` 两个显式旋钮 | **学 eino 的分层洞察、不搬 LangGraph 的结构**: LangGraph 免疫靠 append-only 多版本，我们是**单槽覆盖式**——迁移过去是引擎级重构，且它「cp 是 thread 持久资产、成功也不删」与我们「cp 是单次 Run 临时态、cleanupOnSuccess 删」哲学冲突。取 eino 的核心洞察——**"中断是否已消解"只有知道全局结论的那层才能判断，删除决策就该在那层**——而我们的 **Run 出口 defer 恰好就是那层**（已有 cleanupOnSuccess 先例），故不必像 eino 那样外移到编排层。**子图终局性经错误链自动向上传播**（子 abort 错经 `subgraph.go` `%w` 冒泡 → 父出口同样命中 → 父子树一并清），比 eino 要求每层显式协调更省事。**语义决策**：Abort 连业务先前 HITL 档一起删——选 Abort 即确定性失败，旧档 Resume 注定再失败（livelock 即由此而来）；诊断靠可观测性，不靠留一个会被 auto-load 误捡的 Pending cp。测试: abort_purge_test.go 四路径（先前HITL档/子图 livelock/durability 中途 cp/cond interrupt 协议违规），全做 sanity-revert 真跑 |
| **cond 报错的 Resume 语义交业务 (2026-07-16, commit `67e0aa7`)** | **`CondFailPolicy`**: `AddConditionalEdge(from, cond, WithCondFailPolicy(...))` —— `CondFailAbort`(**默认**，终局失败 → wrap `ErrRunAborted` → Run 出口主动清 cp 子树) / `CondFailRerunFrom`(存 Pending cp → Resume 重跑 from) | **两家都没把这个决定权给业务，且各有坑（实测）**: eino **零选择权**——branch 报错 → `newGraphRunError` 整图 fail、**完全不存 cp**（`checkPointer.set` 仅在 interrupt 路径），业务只能框架外自己从头全跑、外部副作用重复无保护；且 `compose/` 内 `grep retry\|backoff` **零命中**。LangGraph **名义有**（`error_handler` 让业务返 `Command(goto=...)` 决定去向、`retry_policy`+`retry_on`，但重试粒度是整个 task 且默认对 `ValueError/TypeError` 不重试），**但 cp+resume 路径实测是坏的**——path 报错后上游 writes 照样 `put_writes` 持久化 → Resume 时 `_reapply_writes_to_succeeded_nodes` 按 channel 过滤不看 task 失没失败 → restore 回 task.writes → `main.py:2968` 的 `if not t.writes` 把它排除 → **上游不重跑、path 不重评、cp.next 直接空 → Resume 静默返回"成功"、下游永不执行**（本地实跑坐实；变体：上游返 None 时 task.writes 空则正常重跑）。**两家均零测试覆盖 "路由函数报错"** —— 是设计盲区而非深思熟虑。 | **不照抄，自己设计**: 「中止 vs 重跑」的**决定权该给业务**（LangGraph 的 `error_handler` 方向对、eino 完全没有），但**重跑得干净是框架的责任**（三家都没做到：LangGraph 干脆不重跑还假成功、eino 逼你从头跑）。我们的落点: ① 给业务显式声明（两档都干净、都可预期）；② **不要"从头重跑"**——我们有 cp 能精确恢复，eino 只能从头是**因为它不存 cp**（无奈非优势）；③ 默认 `Abort`——纯判断报错多是**确定性**结构问题，默认 fail-fast 比假装能重试诚实；④ `RerunFrom` 留给 **LLM 上游**（planner/classifier 非确定，重跑很可能就好）。**双写实测不发生**: 两个会报错的 cond 其 from 只写 LastValue/OnceFlag（plan node 写 planCurrentCh/planAnswerCh/planDoneCh；classify 写 targetCh）——Append 型的 planExecutedCh 是 **worker node** 写的，不是 cond 的 from。业务自定义图若 from 写 Append/Sum 需自保幂等（已文档化为业务义务）。测试: TestCondFailPolicy_DefaultAbort_NoResumableCheckpoint / _RerunFrom_ResumeRerunsUpstream（端到端两档 + 3 处 sanity-revert 全 FAIL 指向断言）。 |
| **R33 PA-H1 (2026-06-11, commit 8d60c3c/a616625)** | **`cp.CompletedFrontier + DeferredSends`：部分失败/edge 失败超步的成功成员持久化，Resume 重跑成功后并入出边计算（不重跑 NodeFunc）** | LangGraph matched-writes-skip（`_runner.py` per-task put_writes + `main.py` 只执行无 writes 的 task）等价物 | 修复前发散 fan-out（A→X,B→Y）B 中断时 Resume 后 X 静默不跑、graph 假成功——cp 只存 FailedBranches。不抄 LangGraph "攒 writes 到超步末"——保留我们成功 writes 立即 apply 的 per-branch delta 优势，只补"成员身份 + sends"维度。audit² H1 改 peek-then-clear（take 破坏性读在 cond 失败点丢记录）+ M1 edge 失败回填（全成功超步 cond 中断丢兄弟）。测试: TestR33_PartialFailure_* + TestR33_EdgeFailure_*（sanity-revert 全实跑）|
| **R33 GA-M2 (2026-06-11, commit 62bd538)** | **`ErrNoPendingCheckpoint` 哨兵：9 编排 Resume 对不存在 cp 统一 fail-fast（commonResumePrep ExistsNS，Abort 豁免）** | eino ADK `Runner.Resume` "checkpoint not exist" 同位同形 + LangGraph intent 层 EmptyInputError | 三方调研：没有任何先例对显式 resume 意图静默跑业务逻辑（唯一静默的 eino compose 层靠"resume 必带真 input"兜底，我们没有）——修复前 GraphAgent 空 channels 跑出垃圾 final 且 err=nil、BasicAgent 报误导性内部错、拼错 sid 写垃圾 Pending cp 污染。比 eino 多做哨兵化（errors.Is 友好）；灰色场景（成功完成后再 Resume）从静默变确定性报错——优于 LangGraph 静默 no-op / eino stale cp 重放。Abort 豁免: DeleteSubtree 幂等清理 + a2a/server Cancel 协调依赖"框架编排 abort 恒返 nil"。测试: TestGAM2_* |
| **R33 Reflect 拆图 (2026-06-11, commit 5cd19b2)** | **ReflectAgent eval 单节点拆 actor→evaluate→cond→[End\|feedback]：verdict/critique 进 channel superstep commit，feedback（reflectorAgent）中断 Resume 不重评 evaluator；budget 判定在 feedback 之前** | LangGraph reflection 教程 generate/reflect 双节点 + should_continue 挂 generate 后 reflect 前（耗尽轮 critique 不白跑）+ role-flip 保留 messages[0]；eino executor/replanner 平级 sub-agent + Resume 按 SubAgentIndex 只续被中断者 | 原单节点：reflectorAgent 中断 → Resume 重跑整节点 → evaluator 重评（LLM 非确定）→ verdict 翻面时用户答案静默丢 + pending 孤儿。Reflector 签名扩 goal + 输入 user-role 合成（两方都不让评估器收 assistant-role、都显式带目标、都保留批评全文——我们三条全违已修齐）。测试: TestR33_ReflectSplit_* ×4 + r34 Resume 矩阵 |
| **2026-05 audit (#5 三方对照)** | (1) `InterruptError.Data` godoc 改 — 谎报"cp 持久化按 JSON-friendly 序列化"实际从未实现 (cp 只存 AskQuery 4 字段（Question/Options/InterruptID/Reason）不含 InterruptError.Data) → 明示"仅同进程, Resume 后丢失" + 业务跨 Resume / 跨进程持久化走 (a) AskQuery.Question/Options 自序列化 (b) `store.Store` 后端 (Redis/PG) 关联 AskQuery.InterruptID; (2) graph_loop.go auto-fill InterruptID 路径加 slog.LogAttrs (类型化 Attr, 非 variadic any) 输出 5 维, 业务开 DEBUG 反查 hex 对应位置 | eino `InterruptState.LayerSpecificPayload` 落盘 + Resume 重建 / eino `Address.String()` 直接读 vs LangGraph hash 单向 | **不抄 eino typed Data 落盘**: Go 无变长泛型 method + 业务 map[string]any 已能 work + 跨 thread 走 store.Store 替代. **借鉴 eino Address 可观测但只加日志不加字段**: 加 5 维 slog 让业务调试时可见, 不引入业务侧 Address struct (协议复杂度). **audit follow-up #5 #2**: 第一版 godoc 又错推荐 `sess.SetValue` 跨 Resume (sess.values 同样不走 cp), 改为 store.Store 跨进程持久化; 第一版 slog.DebugContext 用 variadic any 在 caller 站点 boxing alloc, 改 slog.LogAttrs 几乎零 alloc |
| **a2a human gate 的答复绑定与提问位置（2026-10-16，`fix/a2a-gate-task-binding`）** | 等人答复的那一问按调用位置记进宿主存档，身份 = 调用位置 + 远端 taskID + 第几问；远端任务给了结局就清记录、再跑发原始请求。引擎钩子与条件边里不续任何远端任务，远端要人时报终局错误；**不支持**「钩子里接住 gate 的中断、换成业务 ID 再抛、续跑时自己取答复带原 session 再调」的写法 | ADK `RemoteA2aAgent`（答复按合成调用 ID 找回那一问所在的任务）+ MAF `A2AAgent`（请求 ID = agent + 远端任务 + 第几问，任务句柄归节点、随工作流存档）+ A2A 规范（同一个 taskId 续 input-required，终态任务不收消息） | 五家没有一家让只做观测的钩子挂起提问；能由业务指定、又真能分发答复的 ID 只出现在节点里、给节点自己的提问（MAF），不是在钩子里给别的组件抛出的提问换 ID。那种写法以前能跑通靠的是会话级任务句柄，它会让一次调用续上别的调用挂着的任务（实测），已撤。调用方知道自己在哪儿调，出路是挪到节点体 / 工具 / 编排成员里。详见下文「远端 agent 要人」一节 |

## 开放问题

1. **`InterruptError.Data` 跨 Resume typed**？
   - 当前 `Data any` JSON 反序列化丢类型
   - 候选：用类型注册表（与 ISSUE-59 Phase 1 `AgentAction.CustomizedAction` 同款）—— `RegisterType[*MyData]("...")` 注册即可
   - **当前不做**：业务用 map[string]any 已能 work；强制 typed 注册增加心智负担
   - ⚠️ **2026-07-29 修正射程**：本条问的是「业务的 Data 要不要 typed」，答案仍是不做。但**框架自用的那份**是另一回事——见上节「已成功的工作单元 Resume 时不重跑」：tool 粒度 skip 需要一个**节点私有、随 cp 持久化**的中断态槽，我们一个都没有（`Data` 同进程、`AskQuery` 无字段）。那不是 typed 与否的问题，是**有没有**的问题（2026-09-18 注：已补——`compose_v2.SaveNodeState` / `GetNodeState`，`95fae06f`）
2. **多 pending 时同 InterruptID 撞**？
   - 当前 `InterruptSite.InterruptID()` fnv64a 16hex (`compose_v2/interrupt.go`)
   - fan-out 多分支不同 branchIdx → 不同 ID ✓
   - 同节点重入 (LoopAgent 多轮 / Resume) 不同 visit → 不同 ID ✓
   - 同节点同轮多 tool_call (BasicAgent toolsFn fanout, ISSUE-81) 不同 idx → 不同 ID ✓
   - 嵌套子图同 nodeID 不同 graphNS → 不同 ID ✓
   - 设计已防撞；显式回归测试**已加**（`f7a82794`：`compose_v2/interrupt_site_dims_test.go` 逐维 + 互斥）
3. **`InterruptError` 加 `IsRootCause bool`** 字段（agent 建议）？
   - 嵌套 interrupt 冒泡时让外层标 false、内层标 true
   - Resume 决议时优先派发到 root cause
   - **当前不做**：InterruptID 已编入 graphNS 维度，嵌套冒泡靠它区分，不需额外字段（AskQuery 无 NS 字段，`query.NS` 是 v1 ISSUE-70 产物）

## 节点私有中断态的两个实现风险，两家各给了更好的答案（2026-07-30 立设计）

上一节定了「正解是补一个节点私有、随 cp 持久化的中断态」。落设计时冒出两个风险点，各自
挖了对端在**同一位置**的做法——**两条都推翻了我自己的方案**，留档防再犯。

### 风险一：memo 什么时候清

**我的方案**：节点成功时显式清掉那一条。

**LangGraph 的做法：不清。** `pending_writes` 的生命周期绑在 **checkpoint 版本**上——每个
超步一个新 cp，旧 writes 自然不再被读到。只在 fork / time-travel（`task_id` 会变的场合）
显式过滤，`_loop.py:964-971` 注释直书：

> Clear old INTERRUPT writes from the loaded checkpoint. The fork will have a new
> checkpoint_id which changes task IDs — **stale interrupt writes would accumulate and
> confuse** the multiple-interrupt check in future resumes.

**移植到我们**：没有 cp 版本（单槽覆盖式），但有 `visit`——而它**只在成功路径递增**
（`runNodeWithHooks` 的守卫，探针实测：中断 0→0、成功 0→1）。所以 memo 键含 visit 时，
节点成功后旧 memo **自然不可达**，正确性上不需要清除点；剩下的只是空间回收，时机不敏感、
可以懒清。

⚠️ **这条为什么要紧**：清除点正是撤回的那版（`973490a`）死掉的地方——7 条 HIGH 里 4 条
由它引出（清整张表把兄弟还挂着的条目清了 / 业务错出口在清空点之前 return 泄漏到下一次 Run /
清空点在闭包出口而 hook 在节点之后抛中断 / 加 ns 维度 + Run 入口清的补丁自己三条回归全红）。
我第一版设计又把它放回来了。**「让旧状态自然不可达」比「在正确时刻清除」少一整类 bug。**

### 风险二：嵌套时 memo 会不会被复制到父 cp

**背景**（探针实测）：同一个中断 ID 在父 cp 和子 cp **各存一份**——
`ns="" pending=[iq]` / `ns="wrap" pending=[iq]`。这是既有设计（外层要知道「下面有人在等」
才能把 query 报给调用方）。我担心 memo 照 `PendingInterrupts` 同档处理会跟着冒泡，于是
准备加一条「memo 不向上冒泡」的约束。

**eino 的做法：所有权转移，不是约束。** 它的 cp 是嵌套结构（`SubGraphs map[string]*checkpoint`），
进子图节点时 `forwardCheckPoint` 把子 cp **取出并从父 cp 删掉**：

```go
if subCP, ok := cp.SubGraphs[nodeKey]; ok {
    delete(cp.SubGraphs, nodeKey) // only forward once
```

状态在任一时刻只属于一层，往下走就搬过去。**约束要靠人守，所有权是机制自带。**

另一半：`SignalToPersistenceMaps(is)` 从信号树一次生成 `InterruptID2Addr` +
`InterruptID2State` 写在 cp 上，`PopulateInterruptState` 并进全局 resume 表、节点按**全路径
地址**取自己那份——**扁平表 + 地址寻址，不是逐层各存一份**。

**移植到我们**：我们的 cp 天然按 `(sid, ns)` 物理分离、每层各自一个 cm，探针已证
`visitCount` 是 per-cm 的（`cmA=2 / cmB=1` 互不影响）。所以 memo 写进 cm ⟹ 只落本层 cp，
「不冒泡」是**结构使然不是约束**——不需要 eino 那套转移，因为我们从来没把子 cp 嵌进父 cp。
父 cp 里那份 `PendingInterrupts` 副本属于 two-payload split 的 **info 那半**（给调用方看），
memo 属于 **state 那半**（节点自用）——两半分档，正是那个 split 的意义。

### 由此定下的设计（与我第一版的差异）

| 维度 | 第一版（读码拍的） | 定稿（挖完两家 + 探针） |
|---|---|---|
| 清除 | 成功路径显式清 | **不设清除点**（键含 visit，成功后自然不可达） |
| 嵌套 | 加「不冒泡」约束 | 结构使然（per-cm 作用域），无需约束 |
| 键 | 4 维含 GraphNS | **3 维 `(NodeID, InstanceID, Visit)`**，与 `visitCount` 同 scoping |

另两个卡死其它方案的既有事实（读码 + 探针确认，写在这防下轮重走）：
`drainSendSink` **只在成功路径跑**（所有 error 路径在它上面 return）⟹ 不能复用 ctx-sink
惯例攒 memo；`frontierResult.AllInterrupts` 是裸 `[]*InterruptError` **不带成员身份** ⟹
不能把 memo 挂在 `InterruptError` 字段上（框架收 N 个 ie 时分不出谁是谁、路由不了）。

## memo 的两条边界：一条我领先、一条业内共识跟我相反（2026-07-31 四家对照）

memo 落地后审计抓出两个缺陷，我自己推了修法**没对照**就动手了——补挖五家在同一位置的做法。
结论一半印证一半推翻，都留档。

### 缺陷 A：工具的「产出」不止 tool_result

> **2026-10-08 起已换代**：转交 / 路由决定改成跟着写入者提交走的上行信号（`compose_v2/signal.go`），
> 工具账连同那次调用做出的信号一起记、命中时交出；收件槽与 `ControlEffectMetaKey` 已删。2026-10-13 起
> `tool_search` 的解锁也不再写 RunSession：记在 tools 节点这次执行上、节点成功后写进图的 channel；它仍不进
> 工具账（解锁记录不进 cp，命中跳过函数体就丢了这一批的解锁）。下文是当时的记录。

我们的 `transfer_to_agent` 真信号写进上层 HandoffAgent 那个**只活一次 sub-agent 调用**的收件槽
（`handoffTransferSlot`，挂 ctx 上；早先是 `sess.SetValue(handoffPendingKey…)`，换载体的理由见
[handoff.md §6.3](../../v2/handoff.md)），`tool_search` 写的是 RunSession 里的揭示集。
**两者都不进 cp** ⟹ memo 只覆盖得到 cp 那一半，命中跳过工具体就把另一半丢了，且失效**静默**。
转交那半换成槽之后反而更脆：槽每次尝试新建，跳过工具体等于**永远没人写**。

**我的修法**（2026-08-21 已换代，原文保留在下方对照结论里）：**让工具自述**——控制流工具在
返回值里声明它那半 cp 外的副作用（`tool.ControlEffectMetaKey`），声明随记账进 cp，命中记账时
由框架照声明重放，函数体不必重跑。判据从「名字在名单里」换成「有没有声明」。

`transfer_to_agent` / `route_to` 已接入；`tool_search` 那半仍留在名单里（它的揭示集写 RunSession，
框架无从重放——正解仍是本篇下文那条「改成从消息历史推导」）。

⚠️ 声明者受限于框架自己的工具：现有 Kind 的重放**直接写框架的控制信号槽**，不过工具体里的候选
校验与授予检查，若谁都能声明就等于给未授予方开一条转交/路由通道（实测可复现：未授予的业务工具
声明一句，续跑时凭空转走整场对话，err 全程 nil）。

**2026-10-08 再换代：声明机制已删。** 转交 / 路由决定改成跟着写入者的提交走的上行信号
（`compose_v2/signal.go`）：工具账把那次 tool_call 期间收到的上行信号随账记下、命中时原样交出，
不靠工具声明——业务工具里经 `RunSubAgent` 调的 agent 做出的决定也一并记下（声明那套记不到它们，
实测同批兄弟工具中断续跑后路由落到兜底）。「声明者受限」那道限制要防的事（未授予方凭空转走对话）
换了一道防线：**收件人由引擎铸**（`compose_v2.SignalRecipient`，带这个任务的密钥、随存档走）。
⚠️ 第一版以为「种类放在不导出的变量里，业务做不出」就够了——不成立：`Signals` 的 JSON 能手写，而那时
收件人是 `"handoff:"+ns`，业务工具 `GetGraphNS` 一拼就转走了对话（终点候选、被当工具用的孙辈、分类器
三处边界全绕过，审计实测）。挡住伪造的是收件人经正常入口算不出来（故意从存档读出密钥自己算的不在防护范围内），不是种类。决策见
[handoff-control-signal.md §8](handoff-control-signal.md)。

**原修法**：框架元工具一律不进 memo（永远重跑）。

**五家没有一家这么做**——这是本次对照最一致的一条：

| | 控制信号住在哪 | 有没有「某类工具不缓存」的名单 |
|---|---|---|
| LangGraph | `Command` 是工具**返回值**，唯一消费路径是转 writes（`graph/state.py:1735-1761`），落 `TASKS` / `branch:to:X` 两个真 channel，都进 cp | 无。跳过的判据 `not t.writes` 和重放的载荷 `task.writes` **是同一个对象**，"不进 memo"等于"不算跑过" |
| eino | `SendToolGenAction` 写进 checkpointed graph State（`ToolGenActions` / `ReturnDirectlyEvent`），消费点在**成功路径的 post handler** | 无。`genToolCallTasks` 对 transfer / exit / 业务工具一视同仁 |
| OpenAI Agents | `RunState._current_agent` 是一等序列化字段；连"活在进程内的嵌套 run 缓存"都专门做了序列化 + rehydrate 双向通道（`run_state.py:1469` / `:1651`） | 无。所有客户端执行的工具走同一条 output-index 跳过规则 |
| MAF | 编码成普通 `function_result` content（进对话历史）+ 带 `target_id` 的 workflow 消息，都进 cp | 有 `approval_mode="never_require"`，但那是排除**审批**不是排除缓存，且靠手写约定无代码校验 |
| Grok | 不 unwind（park future 等答案），空洞补一条「未执行」的合成 tool_result | 不适用 |

**业内共识是把控制信号放进本来就会被持久化的载体**，而不是给缓存开后门。三条具体路子：

1. **信号进 cp**（LangGraph / eino / OpenAI / MAF 都是）。
2. **信号根本不落地，每次从消息历史重新推导**——eino 的 tool_search 就是这个
   （`adk/middlewares/dynamictool/toolsearch/toolsearch.go:296-317` 扫历史里的 tool_search
   结果重建已揭示集，工具体是纯函数只返 JSON）。**memo 命中反而是正确的**：tool_result 消息
   在，揭示集就在。LangGraph 的 `chat_agent_executor.py:928-937` 同形——"哪些还没做"从
   messages 里没有配对 ToolMessage 推出来，没有第二份状态可以跟主状态不一致。
3. **确定性重发**（eino 的 deterministic transfer 在恢复后的流末尾无条件重发一遍）。

**为什么我们还是先落了修法 A**：pending 标记是**子 agent 的工具 → 父编排 stepFn** 的跨层通信
（`handoff_tool.go:143` 写、`handoff_agent.go:556` 读），而我们的 cp 按 `(sid, ns)` **物理分离**、
每层各自一个 cm——父读不到子的 cp。RunSession 存在的理由正是这个跨层缝隙，cp 结构上填不了。
所以修法 A 在当前结构下是**止血**，正解要么把信号搬进能跨层看到的地方、要么照第 2 条改成从
历史推导（`tool_search` 那半几乎可以直接照搬，我们的揭示动作也有 tool_result 在历史里）。
已记进 backlog。

⚠️ 顺带记一个**结构性对照**：LangGraph 有跟 RunSession 同款的「不进 cp」档位——
`UntrackedValue`（`channels/untracked_value.py:16` "never checkpointed"，`_loop.py:439-454`
落盘时过滤、内存里保留），它的意图测试放的是带 `threading.Lock` 的 session 对象。
**它给了这个档位，然后没往里放任何信号。** eino 更硬：`SetRunLocalValue` 在写入时
`checkGobEncodability` fail-fast，档位只有「要活过 resume → 进 State」和「不要 → 放 ctx」，
**「进程内活整个 Run 但不进 cp」这一档被主动堵掉**。我们那一格既存在又装着信号，是缺陷 A 的根。

### 缺陷 B：memo 键用裸 `tool_call.ID`

同批两个 tc 同 ID 时后跑的命中前一个、静默不执行。**我的修法**：键扩成
`(tc.ID, 批内下标, 工具名)`。

这条**方向对且有先例，比多数现状都稳**：

- **LangGraph 走得比我更远**：task_id 的输入维度里**根本没有模型给的 id**——Send 派生用
  `str(idx)`（该 Send 在 `TASKS` 里的下标，`_algo.py:996`），functional `call()` 用
  `scratchpad.call_counter()` 序号（`:841`）。全仓 `tool_call["id"]` 做唯一键**零处**，
  只做 ToolMessage 配对。**位置即身份，模型给的 id 只用于回传。**
- **Grok 有直接先例**：并发派发按批内下标把结果配回请求，还带
  `expect("dispatch index should match an approved slot exactly once")` 断言。
- **eino 无先例且更弱**：真 memo 是 `ToolsInterruptAndRerunExtra.ExecutedTools map[string]string`
  （`compose/tool_node.go:806`），裸 callID、**连工具名都不比**，命中后直接
  `toolCallTasks[i].name = toolCall.Function.Name` 把别人的输出当自己的返回。跟我改之前同构。
  （注：`AddressSegmentTool` 的 `(工具名, SubID=callID)` 不是 memo 键，别拿它当对照物——
  我最初就问错了。它的注释 "same name but different tool call IDs" 明说前提是 ID 互不相同。）
- **OpenAI 是 `(type, call_id)` 无下标**；但它在另一处明写承认碰撞——`agent_tool_state.py:17`
  "index by a stable signature to avoid call ID collisions"，身份是**对象身份 + 六元组内容签名
  + scope 限定**，且**歧义就不命中**（`:156-158` `if len(candidate_ids) != 1: return None`）。
- **MAF 最差**：`call_id` 被提升为 workflow 级 request 主键、三处无重复检测，重复策略是
  first-wins / 静默丢弃。

两条从对照里捡的加固，都已落：

- **工具名进键**。eino 那个"贴错内容"是明确的反面教材；我们的
  `OnToolStart.Name` 是[消费]档可被 hook 改写，两次 Resume 间改写逻辑不同就会让同一个
  (ID, 下标) 指向不同工具。带上名字后退化成不命中（重跑），错在安全侧。
  （2026-10-14 起改写只在一批调用第一次执行时生效、续跑沿用定下的内容——见 `agent_v2.fixToolCallContent`，
  同一个 (ID, 下标) 不再指向不同工具；名字这一维留着，让「别的工具的账不认」不依赖那份记录。）
- **下标稳定性要有依据，不能假设**。eino 靠 `tnState.Input` 把整条 assistant 消息连同
  ToolCalls 顺序一起存 cp、Resume 用存档的（`compose/tool_node.go:1062`）。我们**通过 channel
  state 天然拿到同一件事**——toolCalls 取自 `basicMessagesCh` 那条消息，而 channel state 就是
  cp 的内容。已写进 `memoKey` 的 godoc，别改成从别处重新取 toolCalls。

### 一条更根本的观察：LangGraph 面对同一场景是**拆掉这一层**

`libs/prebuilt/tests/test_react_agent.py:1421-1477` 这个用例的形状跟我们完全一样（两个
tool_call，第一个 `interrupt()`，第二个正常），而它的两个版本给出两种答案：

```python
if version == "v1":
    assert result["messages"] == expected_messages[:-1]   # 正常工具的结果丢掉 = 我们修之前
elif version == "v2":
    assert result["messages"] == expected_messages         # 全保住
```

v2 的做法**不是在节点内加 memo，是把每个 tool_call 拆成独立的 Send**
（`chat_agent_executor.py:849-859`）⟹ 1 个 tool_call = 1 个 task = 1 个独立的中断/重试/缓存
单位，而且 v2 是**默认**。也就是说 LangGraph 认为正解是**抬高执行单位的粒度**，让"节点内多件
带副作用的事"这个形状不存在。

我们评估过等价方案（让每个 tool_call 成为 frontier 成员），结论是**当前引擎结构下不可行**：
`graph_loop.go:962-971` 把所有 directive 的 Payload 合并成一份 `dirWrites` 后 `ApplyAll` 到
共享 channel state，per-实例隔离在这个数据流里无法表达；另外还会破坏 hook 串行契约、
`maxVisit=maxRounds+1` 的计数语义和 tool_result 顺序。**所以 NodeStates 是在"不动执行单位
粒度"这个约束下的解**——它多担了一份职责（同一层既能缓存又能发信号），缺陷 A 正是这份多出来
的职责的代价。这个取舍要记着，别当成"我们和 LangGraph 是同一个解法的两种写法"。

## 节点私有中断态的四个相邻机制：对照后逐个落定（2026-07-31）

memo 落地后列了四个「审计说没系统看过」的角度，先挖对照再动手（这是
[todo 里那条方法级 backlog](../../code/todo.md) 说的做法：改之前查，不是改完被问起才补）。
挖了五家：LangGraph 1.2.10 / eino v0.8.4-era / OpenAI Agents SDK v0.17.7 / MAF python-1.8.1 /
Grok harness。**四条全部落定，两条是「我们已经对了」、两条补了规约与覆盖。**

### 一、memo × 上下文压缩：四家全不成立，共同性质不是我猜的那几条

我的怀疑是「memo 存原始 tool_result、压缩清了内容，Resume 命中把未清版本写回 ⟹ 压缩被撤销」。
预设了四个可能的挡法，**全部被推翻或只部分成立**：

| 我的候选 | 裁决 |
|---|---|
| (a) 缓存归约前的原料、重放时重新归约 | **推翻**。eino 恰恰相反——截断跑在 tool endpoint 内部、在 memo **上游**，`ExecutedTools` 存的就是归约后的成品 |
| (b) 裁剪作用在不同数据副本 | 只两家成立（OpenAI 的 RunState 快照 vs session；**我们是最强形态**）。eino 明确写回 `state.Messages`；LangGraph 的删除烘进 channel 值 |
| (c) 裁剪幂等且每轮无条件重跑 | 只两家成立（eino 靠 `msg.Extra` flag 每轮重扫；我们每轮从 channel 重新推导）。LangGraph 的 `RemoveMessage` 是一次性写，不重放 |
| (d) 压根不共存 | 只对 LangGraph 的 tool 层成立（它有裁剪但**没有节点内 memo**） |

**真正的共同性质是第五条：缓存的生命周期被限定在「一次尚未提交的节点尝试」内，而裁剪只
作用于「已提交的历史」，两者定义域在时间上不相交。** 四家各自的实现：
eino 的 memo 是一次性 token 且 tool 结果只在下游成功节点才进 `state.Messages`；LangGraph 的
restored write 不是「后补的一笔」而是**同一个 superstep 的延迟提交**（cp 的 channel 值是
superstep 开始时的快照）；OpenAI 的判据是这一 turn 的 RunState 私有快照；我们的 `visit`
只在成功路径递增 ⟹ 节点成功后 memo 键不可达。

**我们挡得比 eino 和 LangGraph 都彻底**——压缩从不写回 channel、也从不改原消息对象，所以
「压缩效果被撤销」没有可撤销的对象。哨兵见 `agent_v2/tool_memo_compact_test.go`。

⚠️ 另有两条**对端有、我们没有**的做法，值得知道但当前不必抄：
- **OpenAI 会「本轮有本地 tool 产出就推迟压缩」**（把压缩推到工具往返彻底结束之后，从根上
  避免压缩切进未闭合的 tool_call↔tool_result 组）。我们靠 `GroupMessagesByAPIRound` +
  `AdjustIndexToPreserveAPIInvariants` 在切点侧解同一问题，路径不同。
- **eino 的 memo 除了产出还存节点自己的输入**，Resume 时用存档的覆盖从历史读来的那条——
  这样即使压缩改写了 assistant 的 tool_call arguments，待重跑的 tool 拿到的仍是原始参数。
  我们通过 channel state 天然拿到（cp 里存的就是未压缩的完整历史），效果相同。

### 二、memo × 软取消 / ctx 取消：当前安全，但缺一条正面规约

**软取消不产生半成品问题**：检查点在超步顶部，那一刻没有节点在执行中、上一超步全员成功
返回、visit 已递增 ⟹ 那批 memo 键自然不可达，存下去只是空间垃圾。

**真正的暴露面是 ctx 硬取消**：Go 不杀 goroutine，节点自己发现后返错，它中途 `SaveNodeState`
写下的那份已经落进 cm；`shouldStayPending` 含 ctx 错 ⟹ 存 Pending cp；visit 未递增 ⟹ 键完全
重合 ⟹ Resume 原样读回。

eino 对这个场景有一句最直白的立场（`compose/graph_call_options.go:57-70`）：外部停可能落在
节点执行的任意位置 ⟹ 节点根本没机会为它做准备 ⟹ **一律不信任节点产出、只存节点的输入、
整节点重跑**。**但我们的结论应该跟它相反**——它存的是节点无法准备的任意时刻，我们的写点在
业务手里、写的是「已经做成的事」。留着才是对的，那正是这个原语存在的理由。

要定的是**边界**，已落成 `SaveNodeState` 的开篇硬规约：**记的是「已经做成了什么」，不是
「打算做什么」；写 memo 必须在副作用真正提交之后**。反过来在软取消 / ctx 取消两种非中断退出下
会跳过一件根本没做成的事，且都不报错——这两种退出下两种情况在数据上无法区分。进程在节点执行
中途崩溃不在此列：memo 只随节点「没成」的那次出口存档落盘，崩溃时整个丢掉，节点重跑时从头做。

⚠️ **这是我们和对端在同一个解法内部的真实差异**：MAF 的函数式 `@step` 和 LangGraph 的
`@task` 都有同款子单元级 memo，而两家**都把写入权收在框架手里**（绑死在「子单元成功返回」
那条边上，业务拿不到「先记后做」的口子）。我们把口子敞给业务换灵活性，代价就是这条规约
得靠人守——所以必须写在 godoc 最显眼处。

### 三、memo × 批量存档 / 清理：语义没洞，缺的是 conformance 覆盖

三家**没有一家有多 namespace 批量写 cp 的 API，也没有一家有 keepLastN**（LangGraph 的
`prune` 有契约无实现，OSS 全是 `NotImplementedError`）。这一格没有可照搬的东西——我们的
`SaveBatch` / `Cleanup` 是三家都没有的能力，不是我们漏做。

`NodeStates` 是 `Checkpoint` 的普通字段、`SaveBatch` 是 `SaveNS` 的循环、`Cleanup` 按 sid 整删
⟹ 两条路径语义上不需要额外处理。**缺的纯粹是测试覆盖**，已补两条 conformance 子测试。
理由是业务自研 backend 的典型写法就是两条路各写各的（`SaveNS` 走完整序列化、`SaveBatch`
自己拼批量语句），漏列**不报错**只是静默重放。LangGraph 在它那个未实现的 `prune` docstring
里写了同款警告（「silently reconstruct as empty (no error raised)」），值得抄的是这个**报错
文案形态**而不是机制。

### 四、同一份存档恢复两次：我们的猜测被 LangGraph 显式验证

我们的 memo 键含 `visit` 而它只在成功路径递增 ⟹ 同一份 cp 被 Resume 两次时键完全相同、
都读到同一份 memo。此前只是「可能是设计内的」猜测。

**LangGraph 显式区分了这两件事**，代码和注释都在（`_loop.py:874-899` + `:955-971`）：
resume 时**保留** RESUME writes（多中断场景需要先前已解析的值）；time-travel 时**丢弃**它们
并 fork 出新 checkpoint_id ——而 task_id 含 checkpoint_id，换 id 就换掉整把键。注释直书 fork
的理由是「stale interrupt writes would accumulate and confuse the multiple-interrupt check」。

**换成我们的语言：「命中同一份 memo」是 resume 的正确语义；想让它重新执行，必须换掉整把
键。** 猜测证实，无需改动。

⚠️ 但 LangGraph 比我们多一条出口：**从某个存档分叉出一条新执行身份**（fork）。我们目前只有
`WithFreshRun`（整条从头跑）——「从这个存档分叉、让后续重新执行」这个中间档我们没有。当前
无人需要，记在这防以后重新论证。

### 五、memo 命中 vs 审批裁决的排序：已对齐

OpenAI 的语义是 **skip 严格优先于审批**——已有产出的 call 既不再问审批、也不因本次 deny 补
一条 rejection item。我们的 `runOne` 里 memo 命中检查在最前、`checkToolApproval` 在其后，
**与之一致**（审计另外实测过：deny 固化后不再重问）。

它另有一条「不走 memo」的逃生门：嵌套 agent-as-tool 且其内部中断已裁决时**强制重跑**。
**我们结构上不需要**——子 agent 中断时错误冒泡让 `oc.err != nil`，`rememberDone` 的守卫直接
不记；只有子完整跑完才记 memo。它需要那道门是因为它的判据是「有没有 output item」，嵌套
中断时可能已有部分 output。

### 六、答案的投递身份：谁有权命名一条中断（2026-08-12）

**四家没有一家让业务给中断命名、并按那个名字取答案。** 逐家的机制与其成立前提：

| | 身份是什么 | 谁定 | 业务能自填吗 | 投递方式 |
|---|---|---|---|---|
| **LangGraph** | `task_id`（含 `checkpoint_id` + `step` + 位置 + 触发通道）；对外 `Interrupt.id` = `xxh3_128(checkpoint_ns:task_id)` | 引擎 | **不能**——`interrupt(value)` 签名无 id 参数；连键空间都保留（`is_xxh3_128_hexdigest` 不像哈希的键整个 dict 被当成裸值） | 答案作 `(RESUME, ...)` 写入 checkpoint，**owner 就是写入元组的第一元素**（task_id），resume 时按 `w[0]==task_id` 水合 |
| **eino** | `uuid.NewString()`（纯随机，不由位置派生）+ 独立的 `Address` 表位置 | 引擎 | **不能**（无注入口） | `cp.InterruptID2Addr` 独立表；执行进到每个地址点时按 **Address 等值**注入该点 ctx，`id2ResumeDataUsed` 登记消费、一次性 |
| **MAF** | `request_id`（uuid4 默认） | **业务可传**（四家唯一） | **能** | **但它不路由**——投递用事件里记的 `source_executor_id`（请求时捕获），路由给发问的 executor 并把原始请求绑成入参 |
| **openai-agents** | `(tool_name, call_id)`，钉在冻结的上次模型响应上 | 模型 | **不可表达**（`state.approve(item)` 收的是框架给出的对象本身，没有字符串 ID） | 从冻结工件重算 |

**共同结构**：答案要么**按位置/归属投递**（框架找站点），要么**钉在冻结工件上**。
「站点拿自己发明的键去全局 map 里 pull」这个形态四家零命中——异站点读取这个动作在它们的
API 上根本不存在。允许自填的那一家，自填的 id 也**只是把手、从来不是路线**。

**同站点重跑还能再读，各家靠什么**：LangGraph 的 owner 里含 `step` 和 `checkpoint_id`（两个都会变），
它靠「续跑是从同一份 checkpoint 重放」让它们冻住——**我们是同一个原理**（`runNodeWithHooks`
只在成功路径递增 visit，注释写明「失败/中断时不递增让 Resume 重跑时 visit 不变保
InterruptID 稳定」）。eino 不需要这条：Address 无时间维、天然稳定。

**没有 owner 的答案**：LangGraph 静默忽略（实测：`{bogus, real}` 只投递真的那条）；eino
静默不投递；MAF 硬拒 `ValueError: Response provided for unknown request ID`；openai-agents
不可表达。**四家漏了 owner 全部 fail-closed**（表现为「重新提问」而非「投给别人」）。

**我们的决策**（落地见 `compose_v2.Ask` / `NextAnswerSlot`）：把身份的选择权收回框架，
`Ask` 对非空 `AskQuery.InterruptID` 当场拒。自填 ID 作为**标签**仍合法（直接构造
`InterruptError` 那条路，暂停钮在用），因为取答案的入口不认它 ⟹ 它路由不了任何东西。
这与上表的分野一致：**名字可以业务给，路线只能框架定。**

⚠️ **口子未关完**：`ResumeContext.UserInputs` 是公开字段，业务仍能绕过 `Ask` 直读。
收窄它要动 observability 两处读点 + 59 个测试文件 105 处引用，单列 backlog。

## 代际量与「答案一次性消费」：一次栽跟头之后的四家对照（2026-08-14）

**触发**：出边计算归成员那次改动里，代际（`visit`）的推进点挪错一格——从「写入提交
成功那一刻」挪到「本超步全员成功之后」。于是**部分失败超步里成功的那个成员**掉进一个
新的半成功态：写入已提交、进 `CompletedFrontier` 后永不重跑、**代际却没推进**。它下一轮
提的新问题算出与上一轮相同的 `InterruptID`，被调用方累积传的旧答案静默满足——**用户根本
没被问到而 `err == nil`**。变异验证 + 两个 binary 冒烟 + 真 LLM 冒烟**全绿**，靠一个独立
审计才抓到。

### 一、四家的代际量：三种形态，我原来的二分是错的

我原先的假设是「维护式（有人 `++`，放错就错）vs 派生式（从已有事实算，无处可放错）」，
并据此认为「全图一个量、只有一处增量点」更安全。**两条都不成立：**

| | 代际量 | 挂在谁身上 | 参与 HITL 身份吗 |
|---|---|---|---|
| LangGraph | `checkpoint["id"]`（uuid6） | 全图，每超步一枚 | **是**（唯一真正参与的） |
| LangGraph | `step` | 全图 | 是，但**对身份冗余**（删掉它 task id 仍唯一） |
| LangGraph | `versions_seen[node]` | **per-node map**（与我们 `visitCount` 同构，同「超步收口才更新」的推进点） | **否**——只决定「这个节点该不该跑」 |
| eino | 无 | — | 否（**没有代际量**，身份是每次现铸的 uuid） |
| MAF | `_iteration` | 全图 | 否（只用于 max_iterations 与事件上报） |
| dsh | `replaceGeneration` | session surface | 否（缓存失效；且它是**真派生**——定义即「日志里 replace 事件的条数」） |

**订正三条**：

1. **「只有一处增量点」是错的**：LangGraph 有三个 `_put_checkpoint` 生效入口（超步收口 /
   时间旅行分叉 / 首次输入）。
2. **它真正的护城河是「推进跟铸造被焊成了一个动作」**：`create_checkpoint()`（铸新 id）
   与 `step += 1` 在同一个函数体，想推进就必须过这个函数、过了就必然铸新 checkpoint。
   不是「一处」，是**两件必须同步的事被写成了一件**。
3. **主流是第三种形态：铸造式（minted）**——不从别的事实算、也没人 `++`，由「产生那个
   东西」的同一个动作现场创造（LangGraph 的 checkpoint uuid6 / eino 每次中断新铸 uuid /
   MAF 的 `request_id` / dsh 的 approval id）。没有增量点可放错，因为**没有增量**。

### 二、真正的诊断：我们一个量兼了两职

LangGraph **也有**一个 per-node map（`versions_seen`），同结构、同推进点。**但它一次都
没被身份消费**——消费方只有「这个节点该不该触发」与 `should_interrupt`。

> 所以放错它的后果是「多跑一次 / 不跑」，**不是「两个问题共用一个答案位」**。
>
> 它们把「这个节点跑过没有」和「这是这个节点的第几次化身」**拆成了两个量**；
> 我们是**一个量兼两职**——两个语义对「什么时候该动」的要求不同，一个量承载不了。

### 三、为什么不抄铸造式

铸造式的代价 LangGraph 付过（commit `51cbdbd5c`）：代际一推进，**之前发出去的 id 全体
作废**，遗留的中断记录变孤儿要显式扫。它们的注释原话是「the fork will have a new
checkpoint_id which changes task IDs — stale interrupt writes would accumulate」。

而我们的 `InterruptID` 是**刻意可重算、跨进程稳定**的（调用方能攥着它隔天回来，
a2a / agui 依赖这一点）。走铸造式就得交出这个性质。**不抄。**

### 四、最上游的那条差异，也是这次真正动手的地方

> **四家都是「答案一次性消费」，没有一家有跨续跑累积的答案表。**
>
> - LangGraph：答案存 `checkpoint_pending_writes`，`after_tick` 里整表 `clear()`——
>   答案的寿命 = 一个 checkpoint
> - eino：`id2ResumeDataUsed[id] = true`，发过即标用掉；且每次中断把挂起清单整体重建
> - MAF：用完 `pop`
> - dsh：只有 `'allowed-once'`，没有按身份存的永久批准
>
> **我们「调用方累积传旧答案」的模型，是让代际维承重的直接原因。**

关键在于**两者不冲突**：累积传是**调用方的便利**（它不必记谁答过），一次性消费是
**引擎的记账**（eino 就是收一张 map 然后标 used）。所以照常收累积的 map，只把
「这一位兑付过了」记下来——见 `compose_v2/answer_consumption.go`。

### 决策记录

| 决策 | 结论 | 理由 |
|---|---|---|
| 代际改成全图铸造量 | **不做** | 它的护城河附带「id 不稳定、换代即全体作废、要扫孤儿」，而跨进程稳定的 ID 是我们的既有优势 |
| 答案一次性消费（引擎记账） | **已做**（`c1171a2c`） | 与身份算法**正交**：身份判据将来再出错时当场终局失败，而不是静默生效。四家全有先例、跟累积模型不冲突 |
| 把 `visit` 拆成「跑过没有」+「第几次化身」两个量 | **暂不做**（已进中心 backlog）| 是 LangGraph 的实际做法，买到的是「整类失效降一个等级」；但一次性消费已把该类失效的**后果**兜住（当场炸），而拆量动引擎骨架 + memo 键 + cp 字段。等第二个证据说明「降级还不够」再动 |

---

# 「这次算新开还是续跑」由谁回答（2026-08-27 实挖）

> ⚠️ **本节开头我把问题框错了，纠正留在这儿**：起初写的是「裸调 `compose_v2.Run` 的业务
> 一处保护都没有」，读全 backlog 原文后发现**那个行为是有意的**——同款守卫 2026-08-05
> 加过又撤，三条撤销理由里第 ② 条正是「守卫只能对直连 `compose_v2.Run` 且没传
> `WithFreshRun` 的人开火，**而那批人要的就是「恢复否则新开」**」。我读的是被截断的前半段。
>
> 下面「两条判据」的对照仍然成立且有价值（它解释了为什么入口决定 + 往下传是对的），
> 但**它不是一条待修的缺陷**。真正仍开着的是本节末尾那条：**标识维度不够**。

## 对家的判据是**两条**，我们只有一条

图引擎那家在决定要不要从存档续跑时，注释写得很直白（`pregel/_loop.py`）：

> Resuming from a previous checkpoint requires **two things**:
> 1. A prior checkpoint exists (channel_versions is non-empty)
> 2. **The input signals continuation** (not a fresh run with new input)

第 2 条的来源全部是**调用方自己手上的东西**：

| 信号 | 含义 |
|---|---|
| `input is None` | 显式续跑（`invoke(None, config)`） |
| `input` 是 `Command` | 任何 Command 都是对既有状态操作 |
| `run_id` 与存档里的相同 | 重新接入同一次运行（流断线重连） |
| **子图**：父显式设 `CONFIG_KEY_RESUMING` | 嵌套路径才用「上层推一个标记」 |

**我们的引擎只判第 1 条**（有没有存档），所以第 2 条被外包给了调用方——9 编排各自
push 一个 flag 来补。**注意那家的不对称**：外层图**从调用方的输入推断**，只有子图才用
「父推标记」。我们是**所有路径都靠推标记**，于是没人推的那条路径就是裸的。

这正合本仓那条规约：**守卫合法，当且仅当调用方能用它手上已有的信息满足它。**
「有没有存档」是框架的私有状态；「我这次给的是新输入还是空」是调用方自己知道的事。

## 更根上的一句

`compose_v2.Run` 把「新开」和「续跑」两个操作**合成了一个入口**，靠框架状态分辨；
而 `agent_v2` 那层是 `Run` / `Resume` **两个方法**。对家用一个 `invoke`，但给了
「输入为空 = 续跑」这个**区分值**。三种设计里，只有中间那种需要每个调用方额外声明意图。

## ⚠️ 别把这条跟已撤销的那道守卫混起来

2026-08-05 加过又撤的是「有种子 + 存档命中 ⟹ **报错**」，撤销理由是「崩溃恢复就长这样，
那不是矛盾」。这里说的是**默认行为该怎么定**，不是要重新加那个 error——对家在这个位置
也不报错，它只是**据输入决定走哪条路**。两件事别再合并讨论。


---

# 一枚 `RunID` 同时干了两件事（2026-08-27 实挖，接上节）

那家的标识是**四维**，而关键在第四维**不是键**：

| 标识 | 语义 | 存在哪 | 我们的对应物 |
|---|---|---|---|
| `thread_id` | **会话**，跨多次执行；**存档按它做键** | config | ConvID——但**只在历史 / 观测层，cp 这层不认识它** |
| `checkpoint_ns` | 子图隔离 | config | `graphNS` ✓ |
| `checkpoint_id` | **一个具体存档点**（可时间旅行到它） | config | `WithResumeFrom` ✓ |
| `run_id` | **一次执行尝试** | **存档的 metadata，不是键** | — |

`run_id` 存在 metadata 里只用来认一件事：**「这是同一次运行的重新接入」**（流断线重连）——
它是判「要不要续跑」那三个信号之一，不承担寻址。

## 我们把两职合到了一枚上

`RunSession.RunID` 同时是：

- **存档主键 + 业务持久化主键**（那家的 `thread_id` 职）
- **「一枚只服务一个任务」**（那家的 `run_id` 职）

两职对「什么时候该换一枚」的答案**正好相反**：主键要**跨任务稳定**（不然业务攒的东西找不回来），
执行尝试要**逐任务新铸**（不然新任务捡到上个任务的存档）。

**实测的伤害就长在这个分叉上**：业务把 sid 当会话用（`conv_alice`），任务 A「扣 100」中断留下
挂起档，随后发起**全新任务** B「扣 999」（是 `Run` 不是 `Resume`）⟹ B 期间 pay 被调 **0 次**、
最终金额仍是 100——**B 读到了 A 的产出，扣款没发生，全程无错无告警**。

## 我们现在的解法与它的代价

`sess.NextTask()`：换 RunID、**保留 values / events**。

⚠️ 注意「保留 values / events」这半正是**两职合一的补偿**——因为换了主键，跨任务要攒的东西
就得挂在**对象**上而不是 ID 上。那家不需要这个补偿：换的是 metadata 里的 `run_id`，
`thread_id` 原地不动，攒的东西自然还在。

## 判断：这是取舍不是缺陷，但代价该写清楚

两种都自洽：它们按**会话**给存档做键、靠输入信号分辨新旧；我们按**任务**做键、靠换键分辨。
我们这条的代价有三：

1. 业务必须记得 `NextTask()`（有哨兵 `session_task_contract_test.go` 钉契约，但**违约是静默的**）；
2. 跨任务状态得挂对象上，天然不跨进程；
3. `WithThreadIDCtx` 的 ConvID 已经存在，却**只服务历史 / 观测层**——已经有了会话维度，
   但存档这层看不见它。

第 3 条是最可惜的：**拆分所需的那一维已经在仓里了**，只是没接到 cp 这层。

## 远端 agent 要人：答复绑定与提问位置（2026-10-16 五家对照）

**背景**：`a2a.RemoteAgent` 开 `WithHumanGate` 时，远端任务进入 input-required 就抛中断问人。`fix/a2a-gate-task-binding`
把「等人答复的那一问」从 session 上的一个槽改成按调用位置记进宿主存档；引擎钩子与条件边里没有提问位置，gate 在那里
不续任何任务、远端要人时报终局错误。待定的是要不要保留「钩子里接住中断、换成业务 ID 再抛」的变通——**决定不保留**（见决策记录）。

三个问题上各家一致：

| 问题 | 各家做法（源码位置） | 结论 |
|---|---|---|
| 人的答复送到哪个远端任务 | **A2A 规范**：input-required 用同一个 taskId + contextId 续；终态任务不再收消息（UnsupportedOperationError）；客户端不能自带 taskId 新建任务（规范副本 `a2a-tck/.agents/skills/a2a-client/references/specification.md`）。**ADK**：远端要人 → 合成一个长时函数调用（uuid），事件上记 task_id；人的答复是同 ID 的 FunctionResponse，续的是那条事件上记的任务（`adk-python/.../a2a/agent/_remote_a2a_agent.py` `_create_a2a_request_for_user_function_response`）。没有答复时，TASK 模式下只在同一 isolation scope（一次委派 / 一次节点运行）里推断续接，注释明写不续同一个 agent 上一次委派的任务，中间有失败的请求就不续（`_get_resumable_task_id`）。**MAF**：要人请求 ID = uuid5(agent.id + 远端 task_id + 这一问的出现序号)（`agent_framework_a2a/_agent.py` `_user_input_request_id`）；工作流里答复必须对上挂起的请求 ID，A2A 任务句柄在节点（executor）的会话里、随工作流存档（`_workflows/_agent_executor.py` `handle_user_input_response` / `on_checkpoint_save`）。**agentscope / CrewAI** 不问人：追问交还调用方或宿主 LLM（相当于我们的默认模式） | 绑定都落在「问出这一问的那次调用」上，不放会话级的槽。我们的「调用位置 + 远端 taskID + 第几问」与 MAF 的构成基本相同 |
| 能不能在钩子里问人 | **LangGraph**：`interrupt()` 只能在节点里调（取 node config）；条件边作为源节点的 writer 跑在节点任务里，那里中断可以、续跑时整个节点重跑（`graph/state.py` `attach_branch`）。**eino**：回调只返回 ctx、没有 error，结构上中断不了（`internal/callbacks/interface.go`）。**MAF**：`request_info` 只在 executor 的 WorkflowContext 上。**OpenAI Agents SDK**：审批只挂在工具调用上、按 call_id。**ADK** 更保守：配了 after_request 钩子或自定义转换器就完全不推断续接——钩子可能吞掉终态响应、留下旧的暂停记录（`_has_default_response_persistence`） | 没有一家让只做观测的钩子挂起提问。ADK 那条的理由与我们「引擎钩子里不续任何任务」相同。我们禁止条件边中断是引擎既有规矩（见上文 cond fn 协议），gate 只是照着走 |
| 业务能不能换 ID | **LangGraph**：答复按任务 ns 的哈希分发（`pregel/_algo.py` resume_map），自编的 ID 送不到任何任务。**MAF**：executor 可给自己的提问指定 request_id，但只在 executor 里（相当于我们的节点体），答复也由它自己路由。**ADK / OpenAI**：调用 ID | 分发答复的身份由框架或模型给（位置哈希、调用 ID）；业务能指定、又真能分发答复的 ID 只出现在节点里、给节点自己的提问（MAF），答复回到同一个节点。没有一家支持在钩子里把别的组件抛出的提问换个 ID 再交出去 |

**顺带两点**（进了 `doc/code/todo.md`）：

- 「远端收下答复做完、响应在路上丢了之后一直去续已结束的任务」：agentscope 的 `A2AAgent` 在每次发送前、只要手上留着 task_id 就先
  `GetTask` 对账——找不到就新开、submitted / working 就报错不重复执行、终态就新开、在等输入就续。正是 todo 那条写的方向；
  区别是它的终态分支直接新开任务，我们那条应该先取回已完成任务的结果。**2026-10-28 已做**（`reconcileTask`），跟它的差别：
  终态取回结果；submitted / working 重新挂上读结果（不报错）；在等输入时多分一层——它「在等输入就续」在远端收下答复、
  又问了新的一问时会把旧答复发过去、被当成新那一问的答复（我们改之前也这样，实测静默错），我们按任务历史里有没有自己那条
  答复（消息 ID 按那一问派生，重发不变）判送到没有，送到了再按状态消息是不是还是原来那一问分「已回应、问新的」与「在途」。
- 远端任务以失败等终局结束后我们重发原始请求，没带 `referenceTaskIds`；规范对后续消息是 SHOULD，MAF 在任务不在等输入时用它
  带上一个任务；CrewAI 把同一次委派里已完成的任务都记进去（失败的不记）。可选小改进。
