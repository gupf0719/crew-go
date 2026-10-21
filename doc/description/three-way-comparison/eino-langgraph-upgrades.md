# eino + LangGraph 近期升级点速查（2026 Q2 可借鉴）

> **定位**：主纲 [`industry-survey-2026h1.md`](industry-survey-2026h1.md)（权威源）的**动作视角**——把 eino v0.8.0 + LangGraph 1.2.5 近期 commit 里**对 crew-go 有借鉴价值的点**汇成一篇 action 清单（是什么 + crew-go 现状 + 怎么借鉴 + 优先级）。
>
> **cp 类升级（DeltaChannel / 选择性 resume / 嵌套 cp 取舍 / Durability / coordinate-keys）并入本篇 §三**；既有 [`checkpoint-storage.md`](checkpoint-storage.md) 只留指针不重写。
>
> **证据**：file:line 已核实（主纲 §四 R1-R3 双 agent + 交叉验证）；本篇不重复核实细节，引用主纲。优先级口径同主纲 §三。

---

## 一、eino v0.8.0 非-cp 升级点

| # | 升级点 | 是什么（file:line） | crew-go 现状 | 借鉴 / 优先级 |
|---|---|---|---|---|
| 1.1 | **agentsmd 中间件** | `adk/middlewares/agentsmd/agentsmd.go`：transient 注入 AGENTS.md（每轮 model 调用重 prepend、不持久、排 summarization 后不被压缩） | ✅ **已实现 `WithAgentsMD(paths...)`**（2026-Q2，model 节点 transient 注入 harness 层，对齐 eino transient 思路） | ✅ 已做（详见 [`harness-survey.md`](harness-survey.md) §五） |
| 1.2 | **toolsearch 中间件** | `adk/middlewares/dynamictool/toolsearch/toolsearch.go`：`tool_search` meta-tool，动态工具按需可见（client-side 过滤 / model-native 两档），渐进式工具披露 | ✅ **已实现 `WithToolSearch`**（工具级渐进披露：meta-tool + 过滤 bind list + 揭示集 + 可插拔检索 + fail-closed 拦截，详见 [`toolsearch.md`](../../v2/toolsearch.md)） | ✅ 已做（揭示集存图状态、随存档走——同 bigtool 的 graph-state，非 eino history-scan；2026-10-13 之前走 RunSession。多一道 fail-closed 执行拦截 eino/bigtool 都无） |
| 1.3 | **failover + FailoverContext** | `adk/failover_chatmodel.go`：`FailoverContext{Attempt, LastOutputMessage, LastErr}` 把**部分输出**喂 `ShouldFailover` 决策；interrupt 不触发 failover；last-success 缓存 | ✅ `fallback` 包（per-member 断路器 + StatusCode 分类，主纲已领先）；但 `ShouldFailover(err)` **仅收 error** | 🟢 **P2** 扩参：把部分输出 + attempt 喂决策（已生成 80% 时是否值得切） |
| 1.4 | **push-based TurnLoop + 细粒度 cancel** | `adk/turn_loop.go`：显式 turn 三态（planning→active→idle）+ 三档 cancel（Immediate/WaitUntilIdle/AfterTimeout）+ `UntilIdleFor` | ✅ **软取消已实现 `WithSoftCancel`**（超步边界优雅停 + frontier 存档可 Resume，对应 eino WaitUntilIdle / LangGraph request_drain，详见 [`usage-guide.md` 软取消](../../v2/usage-guide.md)）；**显式 turn 三态没做**——超步边界天然是安全点，不引入额外状态机 | ✅ 软取消已做（设计取舍：不引入 turn 三态，复用超步边界） |
| 1.5 | **DeepAgent** | `adk/prebuilt/deep/deep.go:44` + `task_tool.go`：递归任务分解，sub-agent 自动生成 task tool，LLM 自选调用（隐式编排，对标 LangChain deepagents） | ✅ 你有 PlanAgent（显式规划）/ GraphAgent（显式拓扑） | ➖ 设计取舍：你显式编排，eino 隐式自选；非缺口 |
| 1.6 | **AgenticMessage 双格式** | eino `schema/agentic_message.go:71`：Claude 原生 ContentBlock 树（21 block 类型）；泛型 `MessageType` 贯穿 ADK 穷举 | ❌ 仅 OpenAI 线性格式 | ⚪ **决策题**，优劣分析见主纲 §五（建议暂不整体跟进，走适配层中间路） |
| 1.7 | **skill 增强**：可执行脚本 + 注入前 XML 转义 | `adk/middlewares/skill/`（`@ClassSkill.script` 等价 + 转义） | action **go-func 已实现**（进程内 Go 函数 + `run_skill_action` 工具，[`skill.md` §11](../../v2/skill.md)）；声明式脚本 action 路径已删（2026-06-30，spec 占位无 consumer），脚本执行交 tool 层 open-bash；**注入防护已实现**（markdown 非 XML，对应 parse 边界清洗 + listing 框定，见 [`skill.md` §14](../../v2/skill.md)）| ✅ action go-func 已做；脚本执行收敛 tool 层；注入防护✅ 详见 [`skill-system.md`](skill-system.md) 行业收敛节 |
| 1.8 | multimodal 文件工具 | `MultiModalReadFileTool` / `EnhancedRead`（PDF 分页） | ❌ 无内置 | ⚪ 低（按 tool 实现即可，非框架层缺口） |

---

## 二、LangGraph 1.2.5 非-cp 升级点

| # | 升级点 | 是什么（file:line） | crew-go 现状 | 借鉴 / 优先级 |
|---|---|---|---|---|
| 2.1 | **v3 streaming** | `libs/sdk-py/.../stream/` + RemoteGraph v3：SSE/websocket + reconnect + scoped subgraph projections | 无 streaming（`plan_chat_web` 用 SSE + EventSink 纯前端投递） | ⚪ 低（传输层，不改编排引擎） |
| 2.2 | 核心仍无 skill / 自动压缩 | grep 确认空（在独立 deepagents 仓） | ✅ 你 4 层压缩 + skill 系统（主纲已领先） | — 反向印证你领先 |

> LangGraph 本窗口主要投入在 **cp 类**（见 §三），非-cp 升级点少。

---

## 三、cp / resume / 中断 类升级（折叠 checkpoint-storage）

> 既有 [`checkpoint-storage.md`](checkpoint-storage.md)（cp 存储三方对照设计）保留；本节是 **2026 Q2 eino+LangGraph commit 的 cp 新发现**，checkpoint-storage 只加指针指来。

### 3.1 LangGraph DeltaChannel 增量 checkpoint 🟡 P1

- **是什么**：`libs/langgraph/langgraph/channels/delta.py:25`——只存变化量；每 `snapshot_frequency`（默认 1000）或 `DELTA_MAX_SUPERSTEPS_SINCE_SNAPSHOT`（默认 5000）写一次全量快照；读时沿 parent chain 回放 reducer 重建（`get_delta_channel_history`）。
- **crew-go 现状**：❌ `cp.Channels` **全量快照**（grep 确认无 delta）。
- **怎么借鉴**：长 message history 的存储成本解法。注意 reducer 要 **thread-safe**（你有并发 toolsFn fanout，ISSUE-81）+ supersteps 上限兜底防回放无界。

### 3.2 eino 选择性 resume + SkipPreHandler 🟡

- **选择性 resume（逐行核实，与 crew-go 同语义）**：`compose/graph_run.go`——超步内某 node 中断时 `tm.waitAll()`(:276) best-effort 等兄弟；成功兄弟 → 输出提交 channel（`cm.updateValues` :633）**不重跑**；中断/失败/取消 → `cp.RerunNodes`(:685)；`restoreTasks`(:168) resume 只为 RerunNodes 建任务。注释 `// save other completed tasks to channel`(:289)。→ **定位**：这是编排框架的「正确性」而非 crew-go 独家卖点——eino + crew-go **并列**做到，但 **OpenAI Agents SDK / CrewAI 无图超步恢复**（中断后必须重跑整个编排逻辑）。对外讲「与 eino 并列、超 OpenAI/CrewAI」，不讲「独家」。
- **SkipPreHandler 🟢 P2**：`compose/checkpoint.go:110` `SkipPreHandler map`——per-node 控制 resume 是否重跑该 node 的 pre-handler（input 预处理）。你「preamble 只跑首轮」是 BasicAgent 专属粗粒度，可推广到任意 GraphAgent node。

### 3.3 嵌套 cp 取舍（你占优一半）

- **是什么**：eino 嵌套子 cp **内嵌**父 cp（`compose/checkpoint.go:113` `SubGraphs map[string]*checkpoint`，`graph_run.go:681`，整树一次序列化）；crew-go **物理分离** `(sid, ns)` 独立存取。
- **取舍非单方更优**：crew-go 占优省存储（子 save 不重写整树）+ per-ns timeout；eino 占优父子原子一致（一次序列化）+ 无孤儿子 cp。**对外别讲「单方领先」**，讲「取舍」。

### 3.4 eino 三层 interrupt 🟢

- `compose/interrupt.go:110/130/174`：`Interrupt`（无状态）/ `StatefulInterrupt`（存 agent 内部 state，HITL 幂等）/ `CompositeInterrupt`（聚合多个子 interrupt）。你 `Queries` 是平铺，eino 三档 + 组合——`CompositeInterrupt` 的「组合中断」原语可参考。

### 3.5 LangGraph coordinate-key 寻址分层 🟢 P2

- `_internal/_constants.py:100` `_CHECKPOINT_COORDINATE_KEYS`——thread_id/checkpoint_ns/id/map 是「寻址根基字段」，首个显式出现就**替换** ambient（非 merge）；#8053 修 1.2.3 引入的子图 ns 误继承 regression。
- **借鉴**：你 `WithGraphNS` 显式 push 更安全，但「硬 coordinate vs 软 config」分层概念值得吸进 RunOption，防 #8053 类坑。

### 3.6 LangGraph Durability 三档 ✅ 已做（原 🟡 P1；LangGraph 长期已有，非 2026-Q2 新增）

- `libs/langgraph/langgraph/types.py:87` `Literal["sync","async","exit"]`——业务显式控制 cp 写入时机（Run 时框架选项）。
- ⚠️ **非近期 commit 发现**：既有 [`checkpoint-storage.md`](checkpoint-storage.md) 三方对照表（LangGraph **0.6.10** 基线）**已列** `durability=sync/async/exit`——这是 LangGraph **长期已有、crew-go 长期未补**的缺口；放本篇仅为 cp 类 action 汇总完整，**不是「近期发现」**。
- **crew-go 现状**：✅ **已实现 `WithDurability`**（exit 默认 / sync / async 三档，每超步边界写全量 cp 扛硬崩；ctx 透传嵌套子图继承父档，详见 [`usage-guide.md` Durability](../../v2/usage-guide.md) + [`checkpoint-storage.md`](checkpoint-storage.md)）。业务按 HITL（sync 保证）/ 长跑（async 降延迟）/ 省钱（exit 仅退出时）选。

---

## 四、action items：借鉴优先级 + 我们状态（单一来源）

对应主纲 §三。**本表是状态唯一来源**——`借鉴优先级`（业内有、若抄排 P 几，源自主纲 §三）与 `我们状态`（已做 / 不做 / 待决策）在**同一行**，不再各自漂移（优先级表 + 决策记录两张表已合并）。

> ⚠️ **列 backlog / 评估"还没做啥"看 `我们状态` 列，别拿 `借鉴优先级` 当 todo**。2026-06 两次 drift 都因这俩混淆 + 状态没单点落盘：DeltaChannel（P1 优先级被当待办，实 ❌不做）、agentsmd（P0 优先级被当未做，实 ✅已做）。改状态只动本表一处。

| 升级点（#）| 借鉴优先级 | 我们状态 | 理由 / 备注 |
|---|---|---|---|
| agentsmd 注入（1.1）| 🔴 P0 | ✅ 已做 | `WithAgentsMD`（preamble 真消费）。⚠️ **只是 harness 一块**，harness 整体未完——见表后 |
| DeltaChannel 增量 cp（3.1）| 🟡 P1 | ❌ 不做 | 存储成本优化、非健壮性；高复杂（replay 链 + reducer 线程安全，我们有并发 toolsFn fanout 更难保证）；没实测 cp 存储痛点；仅 LangGraph 一家做。有真实长会话存储压力再议 |
| Durability 三档（3.6）| 🟡 P1 | ✅ 已做 | `WithDurability`（exit / sync / async）|
| toolsearch 工具级披露（1.2）| 🟢 P2 | ✅ 已做 | `WithToolSearch` + fail-closed 拦截 |
| 软取消 / 细粒度 cancel（1.4）| 🟢 P2 | ✅ 已做 | `WithSoftCancel`（超步边界优雅停 + 存档可 Resume；未引入 turn 三态）|
| skill action（1.7）| 🟢 P2 | ✅ 已做 | go-func（`WithSkillActionFunc` + `run_skill_action` 工具）已做；声明式脚本 action 路径已删（2026-06-30，spec 占位无 consumer），脚本执行收敛 tool 层 open-bash（见 [`skill-system.md`](skill-system.md) 删除依据）|
| FailoverContext 扩参（1.3）| 🟢 P2 | ❌ 不做 | 「部分输出喂 `ShouldFailover`」eino 独有窄优化；`fallback` 已有 per-member 断路器 + StatusCode 分类（已领先）；无业务诉求 |
| SkipPreHandler（3.2）| 🟢 P2 | ⏸ 暂不做 | BasicAgent「preamble 只跑首轮」已覆盖主场景；泛化 GraphAgent node 属 speculative，无业务诉求；有诉求再做 |
| coordinate-key 分层（3.5）| 🟢 P2 | ❌ 不做（N/A）| 用显式 `WithGraphNS` push，无 ambient-config 自动继承 → 没有 #8053 那类子图 ns 误继承的土壤；Durability 落地已顺手规避 |
| AgenticMessage 双格式（1.6）| ⚪ 决策 | 🤔 待决策 | 架构方向题（主纲 §五；已进中心 backlog）|
| DeepAgent（1.5）| ➖ 非缺口 | ❌ 不跟进 | 有显式编排（Plan / Graph）|
| multimodal 工具（1.8）| ➖ 非缺口 | ❌ 不跟进 | 按 tool 实现 |
| v3 streaming（2.1）| ➖ 非缺口 | ❌ 不跟进 | 传输层 |

> ⏳ **harness 整体接入未完**：agentsmd（1.1 ✅）只是 harness 的一块——经 2026-06 scoping（8 源调研，见 [`harness-survey.md`](harness-survey.md) §四决策记录）：**工具审批层 P1 ✅ 已做**（`agent_v2/approval.go`：WithApprovalMode / ApprovalAuto / ApprovalAsk）、后台 async P2 defer、指令分层 **skip**（eino 同构无缺口）、TodoProvider defer（业务自建）。**别把「agentsmd ✅」读成「harness 完成」。**

> 跨篇延伸（不在本表重复）：**OTel 可观测对齐**（invoke_agent SpanKind + token 名）见 [`observability-trace-spec.md`](observability-trace-spec.md) §四；**harness provider 缺口**（Todo/审批/后台 agent/AGENTS.md）见 [`harness-survey.md`](harness-survey.md) §四；**skill 升级点**（eino 压缩预算 PreserveSkills 🟡P1 / MAF 脚本+CrewAI 自动晋升 🟢P2）见 [`skill-system.md`](skill-system.md) 行业收敛节。

---

> **本篇 audit 状态**：① 自审（对照主纲转录核对）+ 读 `checkpoint-storage.md` **发现 Durability 是 LangGraph 长期已有、非 2026-Q2 新增**（既有 0.6.10 基线已列）→ §3.6 修正——此条独立 agent 对照主纲查不到（主纲同含 recent 框架），只有读既有文档可见；② 独立 agent（转录/优先级/框架一致性核实通过）——采纳 §3.2 措辞优化（加 OpenAI/CrewAI「无图超步恢复」对比 + 去 spin 措辞），驳回「harness-survey.md 待补」（实已写）。facts 全继承自 3 轮审过主纲。并入最终批量 audit。
