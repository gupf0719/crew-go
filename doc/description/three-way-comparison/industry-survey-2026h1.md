# 多 Agent 编排框架行业调查（2026 H1）— 主纲

> **本文档定位**：2026 年 4–6 月对 6 个主流框架/标准的实地代码调查汇总，作为后续专题文档（harness / skill / observability / eino+LangGraph 升级点 / checkpoint）的**权威源**。专题文档引用本文事实，不重复描述（防跨文档同病）。
>
> **调查方法**：全部基于**本地 clone 的源码逐行核实**（非网络/博客）。三个 Explore agent 的过度报告已交叉核实纠正（eino 上推翻 3 处误判：toolsearch「未实现」→ 实有、「无 skill」→ 有完整 skill 包、「全量重跑」→ 实为选择性 resume）。涉及 crew-go 自身现状的论断均 grep 实际代码核实（非凭 CLAUDE.md 印象）。
>
> **时间锚点**：调查于 2026-06-18。各仓库 HEAD 均为 2026-06-17/18。
>
> ⚠️ **增量续篇**：2026-09-21 又拉了一轮全部 10 个本地 clone 做增量调查，见
> [industry-survey-2026q3.md](industry-survey-2026q3.md)（触发问题是「运行中插话」与
> 「回合结束闸门」两个待拍板特性；另含 **AG-UI 1.0 定稿后我们两处真不合规**、
> OTel semconv 三个月零影响的核对结果）。本篇的事实未被推翻，只是被增量补充。

## 调查对象与版本

| 框架/标准 | 本地路径 | 版本（HEAD 2026-06） | 语言 |
|---|---|---|---|
| eino | `~/program/go/src/eino` | **v0.8.0**（网络调查误判为 v0.7.0） | Go |
| LangGraph | `~/program/go/src/langgraph` | **1.2.5**（core） | Python |
| MAF (Microsoft Agent Framework) | `~/program/go/src/agent-framework` | **python-1.8.1** / dotnet-1.10.0 | Python/.NET |
| OpenAI Agents SDK | `~/program/go/src/openai-agents-python` | **v0.17.5** | Python |
| CrewAI | `~/program/go/src/crewAI` | **v1.14.8a** | Python |
| OTel GenAI semconv | `~/program/go/src/otel-semconv-genai` | HEAD 2026-06-18（独立新仓） | spec |

---

## 一、近期升级点（5 框架 + OTel 标准，2026 Q2）

### 1. eino v0.8.0 — 4 月以来 120 commit 几乎全在堆一个 ADK harness 层

| 能力 | 实现（file:line） | 说明 |
|---|---|---|
| **ADK harness 层** | `adk/`（独立于 `compose/` 图引擎） | 模型之上的工具循环 + 中断/恢复 + cancel + 记忆 + skill 全打包 |
| **push-based TurnLoop** | `adk/turn_loop.go` | 显式 turn 三态（planning→active→idle）+ 三档 cancel（Immediate/WaitUntilIdle/AfterTimeout）+ `UntilIdleFor` |
| **agentsmd 中间件** | `adk/middlewares/agentsmd/agentsmd.go` | 自动注入 AGENTS.md（幂等 key `__agentsmd_content__`，插在首个 user 前；#3952b3d 防重复注入） |
| **summarization 中间件** | `adk/middlewares/summarization/summarization.go:374` | 单层 context 压缩，触发阈值常量 `defaultTriggerContextTokens=160000`；含 retry+failover |
| **skill-aware 压缩** | `adk/middlewares/summarization/finalizer_builder.go:147` | `PreserveSkillsConfig{MaxTokensPerSkill, SkillsTokenBudget}` 压缩时保护 skill body（#967） |
| **skill 系统** | `adk/middlewares/skill/`（skill.go/doc.go/prompt.go/filesystem_backend.go） | SKILL.md + frontmatter(name/description/context/agent/model) + **inline/fork/fork_with_context 三模式** + 扫 `<dir>/SKILL.md`——**与 crew-go 几乎逐字段同构**（首引入 #661，本窗口加 token 预算 #967 + fork 扩展钩子 #905） |
| **toolsearch 中间件** | `adk/middlewares/dynamictool/toolsearch/toolsearch.go` | tool_search meta-tool，动态工具按需可见（client-side 过滤 / model-native 两档），渐进式工具披露 |
| **failover** | `adk/failover_chatmodel.go` | `FailoverContext{FailoverAttempt, LastOutputMessage, LastErr}` 把部分输出喂决策；#1065 interrupt 不触发 failover；含 last-success 缓存 |
| **AgenticMessage 双格式** | eino `schema/agentic_message.go:71` | Claude 原生 ContentBlock 树形格式（21 种 block 类型，含 MCP/server tool/reasoning）；泛型 `MessageType = *Message \| *AgenticMessage` 贯穿 ADK 穷举处理（#920/#988） |
| **DeepAgent**（prebuilt） | `adk/prebuilt/deep/deep.go:44` + `task_tool.go` | 递归任务分解：sub-agent 自动生成 task tool，LLM 自选调用（隐式编排，对标 LangChain deepagents）；`TypedConfig[M]` 支持双消息格式 |
| **multimodal 文件工具** | `adk/`（MultiModalReadFileTool / EnhancedRead） | 内置文件读取 + PDF 分页（#993） |
| **三层 interrupt** | `compose/interrupt.go:110/130/174` | `Interrupt`（无状态）/ `StatefulInterrupt`（存 agent 内部 state，HITL 幂等）/ `CompositeInterrupt`（聚合多个子 interrupt）——crew-go `Queries` 平铺，eino 三档 + 组合 |

**选择性 resume（逐行核实，对齐你的差异化卖点）**：eino 与 crew-go **同语义**——
`compose/graph_run.go`：超步内某 node 中断时 `tm.waitAll()`(:276) best-effort 等兄弟跑完；
成功兄弟 → `otherTasks` → 输出提交 channel(`cm.updateValues` :633)，**不重跑**；
中断/失败/取消 node → `cp.RerunNodes`(:685)；`restoreTasks`(:168) resume 时只为 RerunNodes 建任务。
源码注释直接写明 `// save other completed tasks to channel`(:289)。
**差异**：eino 嵌套子 cp 内嵌父 cp（`checkpoint.SubGraphs map[string]*checkpoint`，整树一坨序列化）；crew-go 物理分离 `(sid, ns)` 独立存取。**取舍非单方更优（R3 修正）**：crew-go 占优省存储（子 save 不重写整树）+ per-ns timeout 控制；eino 占优父子原子一致（一次序列化 `graph_run.go:681`）+ 无孤儿子 cp。eino 有 `SkipPreHandler map` per-node 控制 resume 是否重跑 pre-handler——比 crew-go「preamble 只跑首轮」更细。

> eino 另有接口式中间件 hook（`BeforeAgent`/`BeforeModelRewriteState`/`AfterToolCallsHook`/`AfterAgent`）+ `ShouldRetry` callback + tool name/arg aliases（次要对比点，细节见专题文档）。

### 2. LangGraph 1.2.5 — checkpoint/resume 正确性 + 增量化

| 能力 | 实现（file:line） | 说明 |
|---|---|---|
| **DeltaChannel 增量 checkpoint** | `libs/langgraph/langgraph/channels/delta.py:25` | 只存变化量；`snapshot_frequency=1000` 或 `DELTA_MAX_SUPERSTEPS_SINCE_SNAPSHOT=5000` 写一次全量快照；读时沿 parent chain 回放 reducer 重建（`get_delta_channel_history`） |
| **coordinate-key 嵌套 ns** | `_internal/_constants.py:100` `_CHECKPOINT_COORDINATE_KEYS` + `_config.py:364` | thread_id/checkpoint_ns/id/map 是「寻址根基字段」，首个显式出现就替换 ambient（非 merge）；#8053 修 1.2.3 引入的子图 ns 误继承 regression |
| **Durability 三档**（LangGraph 长期已有，非 1.2.5 新增——`checkpoint-storage.md` 0.6.10 基线已列） | `libs/langgraph/langgraph/types.py:87` `Literal["sync","async","exit"]` | 业务显式控制 cp 写入时机 |
| **checkpoint conformance** | `libs/checkpoint-conformance/` | BASE/EXTENDED capability 分级，含 `delta_channel_history` |
| **v3 streaming** | `libs/sdk-py/.../stream/` + RemoteGraph v3 | SSE/websocket + reconnect + scoped subgraph projections（传输层，不改图引擎） |
| **核心仍无 skill/压缩** | grep 确认空 | `libs/langgraph/langgraph` 无 skill 包、无自动 context 压缩/summarization（这些在独立 deepagents 仓） |

### 3. MAF python-1.8.1 — `create_harness_agent()` 把整个 harness 打进 core

`create_harness_agent()`（`python/packages/core/agent_framework/_harness/_agent.py:226`）是 batteries-included harness，自动接好下列 provider（每个可 disable），`_harness/` 子包逐一对应：

| provider | 文件 | 说明 |
|---|---|---|
| 自动 tool 循环 | `_loop.py` | function invocation loop |
| 历史持久化 | (history provider) | 每次 model call 后持久化 |
| context 压缩 | `_compaction.py` | 前/后两策略（`ContextWindowCompactionStrategy` token 预算 + `ToolResultCompactionStrategy`）；用 `token_count` 分组注解 + `_summarized_by_summary_id` 追踪——**与 crew-go `GroupMessagesByAPIRound`+摘要 marker 同 DNA** |
| **TodoProvider** | `_todo.py` | todo 清单管理 |
| **plan/execute Mode** | `_mode.py` | AgentModeProvider |
| 文件型记忆 | `_memory.py` | MemoryContextProvider |
| **SkillsProvider** | `_skills.py` | 自述遵循 Anthropic Agent Skills；Advertise→Load；FileSkill/ClassSkill；**可执行脚本 `@ClassSkill.script` + 资源 `@ClassSkill.resource`**；注入前 **XML 转义**（防注入） |
| **后台子 agent** | `_background_agents.py` | 异步委派 + 监控 + 取回 |
| **工具审批** | `_tool_approval.py` | "don't ask again" standing rules + 启发式 auto-approval |
| shell / web search | `_file_access.py` / option | 内置 |
| OTel | observability layer | — |
| **harness 指令分层** | `_agent.py` `DEFAULT_HARNESS_INSTRUCTIONS` | harness 级操作规范 vs `agent_instructions` 任务级，分开 |

另：workflow graph engine（`_workflows` + 独立 `packages/orchestrations`），原生 A2A（`packages/a2a`）。

### 4. OpenAI Agents SDK v0.17.5 — 极简编排 + 重度 harness 下沉

| 维度 | 实现（file:line） | 说明 |
|---|---|---|
| 编排 | `handoffs/` + agents-as-tools；**grep 确认无** Sequential/Parallel/Loop/Routing 内置类 | 多 agent 仅靠 handoff（tool+回调）+ 工具调用 |
| **harness 下沉** | `sandbox/`（Local/Modal/Daytona/Vercel/RunLoop/Blaxel 多后端）、`editor.py`(ApplyPatchEditor)、`apply_diff.py`(V4A diff)、`computer.py`(computer-use) | 文件编辑/沙箱/计算机操作下沉进 SDK |
| **SKILL.md** | `sandbox/capabilities/skills.py`（`LazySkillSource`/`LocalDirLazySkillSource`/`SkillMetadata`） | 仅沙箱作用域，从本地目录加载 SKILL.md |
| HITL | tool approval → `ToolApprovalItem` → `RunState`（`run_state.py:131` schema v1.11 + 版本摘要表）→ resume | 无图超步快照（无 CompletedFrontier 等价物） |
| **Guardrails** | `guardrail.py:29` `tripwire_triggered` + `tool_guardrails.py` | input/output/tool 三类，tripwire bool 信号触发异常 |
| Hook | `lifecycle.py`（8 个，纯观测无改写） | on_llm/agent/tool/handoff start/end |
| 重试/failover | `retry.py`（RetryPolicy）；**无模型级 failover** | 仅同模型重试 |
| 压缩 | 仅 OpenAI Responses API `responses.compact`（`openai_responses_compaction_session.py`，阈值 10） | API 级，非框架级 |
| 会话 | `memory/session.py:14` `Session` Protocol（SQLiteSession / OpenAIConversationsSession / Compaction 三实现） | per-session 自动历史 + 跨 run 持久化（无跨 run 业务 KV，对比你 `store.Store`） |
| MCP | `mcp/manager.py:108` `MCPServerManager` | 多 server 连接生命周期（strict / drop_failed / reconnect / parallel） |

### 5. CrewAI v1.14.8a — Crew + Flow 双范式

| 维度 | 实现（file:line，主包 `lib/crewai/src/crewai/`） | 说明 |
|---|---|---|
| 编排 | `crew.py`+`process.py`(sequential/hierarchical) + `flow/`(事件驱动 `@start`/`@listen`/`@router`) | Crew 静态任务流水线；Flow 条件分支方法图 |
| **Flow 持久化+resume** | `flow/persistence/base.py:18` `FlowPersistence`(save_state/save_pending_feedback) + `@persist` + SQLite | 比 crew-go 简化：单 pending feedback，无部分失败超步恢复 |
| HITL | `flow/human_feedback.py:43` `HumanFeedbackPending` + `resume(feedback)`（`flow/runtime/__init__.py:1518`）+ **async provider 可插拔**（Slack/webhook） | 单 query，二态 pending |
| **SKILL.md** | `skills/models.py:25` `DisclosureLevel`(1/2/3=METADATA/INSTRUCTIONS/RESOURCES) + `SkillFrontmatter`(含 **`allowed_tools`** 白名单) + `with_disclosure_level()` | 3 级自动晋升；**无 fork/inline 模式** |
| 记忆 | `memory/`(unified vector, scope+importance, recall_flow/encoding_flow) | 不分层，用 scope 代替 |
| A2A | `a2a/`（client 消费 card，TransportType JSONRPC/GRPC/HTTP+JSON，ProtocolVersion 0.2–0.4） | **纯 client，无注册发现** |
| **Guardrails** | `task.py:246` `guardrail` + `guardrail_max_retries=3` | task 级输出校验 + 自动重试 |
| Hook | `hooks/types.py`（4 个：before/after LLM + tool） | — |
| 可观测 | `telemetry/telemetry.py`（OTel） | 未对齐 gen_ai.* |

### 6. OTel GenAI semconv — agent span 拆分 + 新 token 属性（独立新仓）

> ⚠️ GenAI semconv 已整体迁出 `semantic-conventions` 主仓 → 独立仓 `semantic-conventions-genai`。

**agent span 体系（`docs/gen-ai/gen-ai-agent-spans.md` + `model/gen-ai/spans.yaml`）**：`invoke_agent` 按本地/远程拆 SpanKind，reference report 直接点名框架归属——

| operation.name | SpanKind | 规范点名框架 | crew-go 应归 |
|---|---|---|---|
| `create_agent` | CLIENT | 远程/托管 agent 创建 | A2A 远程创建 |
| `invoke_agent`(client) | **CLIENT** | aws-bedrock-agent / azure-ai-foundry / openai-assistants（托管） | A2A RemoteAgent |
| `invoke_agent`(internal) | **INTERNAL** | autogen / crewai / google-adk / openai-agents / pydantic-ai（本地） | **本地 9 编排** |
| `invoke_workflow` | INTERNAL | 工作流（新增） | Sequential/Parallel/Graph 可选 |
| `plan` | INTERNAL | 规划（新增） | PlanAgent 可选 |
| `execute_tool` | INTERNAL | 工具 | tool span |

**新增 token / request 属性（`docs/registry/attributes/gen-ai.md`）**：
- `gen_ai.usage.reasoning.output_tokens`（output 子集）
- `gen_ai.usage.cache_read.input_tokens` + `gen_ai.usage.cache_creation.input_tokens`（input 子集；约定后来把后者改名为 `gen_ai.usage.cache_write.input_tokens`，observability 按新名记）
- `gen_ai.request.reasoning.level`（low/medium/high，#258，2026-06-18 刚加）

> **网络调查期被反驳、未采信的声明**（记录以防被重新断言）：① 「Google ADK 每次 tool call 自动 checkpoint」（3 票全反驳；ADK 实为 `DatabaseSessionService` 显式持久化 + webhook resume）；② 「Anthropic long-running agent = initializer+coding 双 agent 架构」（证据不足，1-2 反驳）。
>
> **Google ADK 仅网络调查层覆盖未本地核实**：deep-research 提到的 ADK 能力（`DatabaseSessionService` 持久化 / 事件驱动 webhook pause-resume）**未本地 clone 逐行核实**；本文中 ADK 仅作 OTel `invoke_agent` internal 名单成员出现。若要纳入对标需补拉 `google/adk-python`。

---

## 二、两大收敛

### 收敛 ①：SKILL.md ×5 — Anthropic Agent Skills 成事实标准

| 框架 | SKILL.md + 渐进披露 | 独特点 |
|---|---|---|
| **crew-go** | ✅ | frontmatter 二维管控（disable-model-invocation / user-invocable）+ **inline / fork** 两执行模式（`fork_with_context` **已删（2026-08-26）**） |
| **eino** | ✅ `adk/middlewares/skill/` | 同款三执行模式（与 crew-go 几乎逐字段一致）+ 压缩时 PreserveSkills token 预算 |
| **MAF** | ✅ `_skills.py`（自述遵循 agentskills.io） | 可执行脚本 + 附加资源 + 注入前 XML 转义 |
| **OpenAI Agents SDK** | ✅ `sandbox/capabilities/skills.py` | 仅沙箱作用域 |
| **CrewAI** | ✅ `skills/models.py` | allowed-tools 白名单 + 3 级自动晋升 |
| LangGraph（对照） | ❌ 核心无 | skill 在独立 deepagents 仓 |

**结论**：**5 个独立框架采纳** SKILL.md + progressive disclosure（crew-go/eino/MAF/OpenAI/CrewAI）；**Claude Code 是标准本身（agentskills.io）不计采纳方——含它共 6 个框架有 SKILL.md**（见 [`skill-system.md`](skill-system.md) 行业收敛节）。**唯一有文档血缘的是「遵循 Anthropic Agent Skills 公开标准」（MAF `_skills.py` 自引 agentskills.io）；其余是收敛非照抄**。crew-go 差异化在**执行模式（inline/fork/fork_with_context 三模式）**，与 eino 并列、超 MAF/OpenAI/CrewAI；**Claude Code 4 模式最全**。

### 收敛 ②：harness 融入 framework ×3（+1）

| 框架 | harness 载体 | 形态 |
|---|---|---|
| **MAF** | `create_harness_agent()` / `_harness/` 子包（core 一等公民） | 最完整：loop+todo+mode+memory+skills+approval+shell+websearch+background |
| **eino** | `adk/` 层 | TurnLoop + agentsmd + summarization + failover + toolsearch + skill |
| **OpenAI Agents SDK** | `sandbox/` + `editor`/`apply_diff`/`computer` | 文件编辑/沙箱/computer-use 下沉 |
| LangChain deepagents（+1） | 独立仓 | harness 模式 |
| LangGraph（对照） | 核心保持极简 | harness 留给上层 |

**结论**：「框架层 ↔ harness 层」边界在合并，3+ 独立框架验证。crew-go 把 hook/4 层压缩/HITL/observability 做进框架，本质同路——**设计方向被反复验证**。

### （观察）收敛 ③：checkpoint/resume/cancel 正确性 frontier

eino 与 LangGraph 本窗口工作高度集中在「嵌套/中断执行的 checkpoint 正确性」——序列化保真（eino #1007/#1081 vs LangGraph #7892/#5d5a64120）、嵌套子图 cp 隔离（eino #1018 vs LangGraph #8053）、cancel 传播 + 区分「中断 vs 真错误」（eino #942/#929/#1065 vs LangGraph #8057/#7920）。与你 R33 audit（PA-H1 DeferredSends / GA-M2 ErrNoPendingCheckpoint）同一战线。**failover 是 eino+crew-go 收敛，LangGraph 缺位；context 压缩是 eino+crew-go 收敛，LangGraph 缺位。**

### （观察）收敛 ④：Guardrails ×2

OpenAI（input/output/tool guardrail + tripwire）与 CrewAI（task 输出校验 + 重试）**都有 guardrail 强制层**，crew-go **无**（仅有 `guardrail` 作为 observability 组件 kind 标签，非强制机制）。

---

## 三、crew-go action items 总表

> 「现状」列均 grep 实际代码核实（2026-06-18）。优先级 = 性价比 × 行业方向强度。

| 优先级 | 做什么 | 参考实现 | crew-go 现状（已核实） |
|---|---|---|---|
| 🔴 P0 | **AGENTS.md/外部指令注入中间件** | eino `agentsmd`（transient 注入） | ✅ **已实现 `WithAgentsMD(paths...)`**（2026-Q2；transient harness 层注入，对齐 eino，测试覆盖；比原提案省掉 marker/压缩保护——transient 自然免） |
| 🔴 P0 | **OTel 合规对齐**：① `operation.name` 值（代码注释为 `"agent"` hooks.go:181，新规范值 `invoke_agent`）② 加 **SpanKind**（本地编排=INTERNAL / A2A 远程=CLIENT）③ **trace span** 属性改名 `reasoning_tokens`→`reasoning.output_tokens`、`cached_input_tokens`→`cache_read.input_tokens`（+ 补 `cache_creation`） | OTel GenAI 新规范 | 部分有：input/output/reasoning/cached 已 emit 于 **trace span 属性**（hooks.go:526-537）**已改名对齐 OTel ✅**（`reasoning.output_tokens`/`cache_read.input_tokens`，2026-Q2，测试覆盖）；metric `gen_ai.llm.tokens.total` label 值 `think`/`cached` 仍待对齐（另一类）；`cache_creation` **deferred**（无 provider 报 cache-write，现加=死字段）；agent span **已设 SpanKind ✅**（model=CLIENT / 本地 agent=INTERNAL；A2A 远程待细分） |
| 🟡 P1 | **DeltaChannel 式增量 checkpoint** | LangGraph `channels/delta.py`（snapshot_frequency + supersteps 上限兜底） | **确认无**（grep 空）；`cp.Channels` 全量快照 |
| 🟡 P1 | **Durability 三档（sync/async/exit）** | LangGraph `types.py:87` | **确认无**；cp 写入时机隐式（OnAgentEnd defer） |
| 🟡 P1 | **skill 压缩预算粒度对齐** | eino `PreserveSkillsConfig{MaxTokensPerSkill, SkillsTokenBudget}` | 已有 `WithSkillBodyMaxChars`/`WithSkillListingBudget` + `SkillBodyAnnotation` 跨层保护——需对比 eino 的 per-skill + 总预算是否更细 |
| 🟢 P2 | **coordinate-key 寻址分层**（哪些字段改寻址根基不可 merge） | LangGraph `_CHECKPOINT_COORDINATE_KEYS` | 你 `WithGraphNS` 显式 push 更安全，可吸收分层概念防 #8053 类坑 |
| 🟢 P2 | **Guardrails 强制层**（input/output/tool + tripwire/重试） | OpenAI `guardrail.py` + CrewAI `task.py` 双证 | **确认无强制层**（仅 observability `guardrail` kind 标签） |
| 🟢 P2 | **checkpoint schema per-version migration**（前向升级） | OpenAI `run_state.py:131` v1.11 + 版本摘要表 | 有 `SchemaVer` 字段 + **刻意不 fail-fast 宽松策略**（Option D，`serialization.go:228`；测试 `TestRestore_SchemaVer_Mismatch_Passes` 证版本不匹配静默通过——非「占位/未激活」是设计选择）；**缺** OpenAI 式 per-version 前向升级逻辑 |
| 🟢 P2 | **一等 harness provider**：Todo / plan-execute Mode / 后台 agent / 工具审批权限 / Shell / WebSearch | MAF `_harness/` 全家桶 | **确认无这些一等 provider**（grep 空）；有 HITL 但无「权限规则」层 |
| 🟢 P2 | **skill 增强**：可执行脚本 + 注入前 XML 转义 / 3 级自动晋升 / allowed-tools / async HITL provider | MAF + CrewAI | 部分缺（fork/inline + **元数据注入清洗/框定**已有 [`skill.md` §14](../../v2/skill.md)；**action go-func 已做**（[`skill.md` §11](../../v2/skill.md)）——声明式脚本 action 路径已删（2026-06-30，spec 占位无 consumer），脚本执行交 tool 层 open-bash；白名单/自动晋升缺） |
| 🟢 P2 | **fork skill 扩展钩子**（`BuildForkMessages` / `FormatForkResult` / `CustomToolParams` / `BuildContent`） | eino `adk/middlewares/skill/skill.go:166-180`（#905） | fork mode + `WithSkillSubAgentResolver`/`WithSkillModelResolver` 已有，但 **per-invocation fork 消息构造 / 结果格式化 / tool param 定制钩子 grep 确认无** |
| 🟢 P2 | **采纳新 OTel span operation 名**：`invoke_workflow`（编排）/ `plan`（PlanAgent）/ `create_agent`（A2A 远程） | OTel `model/gen-ai/spans.yaml:364/523` | 当前编排/Plan/A2A 统一恒 `operation.name="agent"`，未用新细分（`invoke_workflow` 可选——`spans.yaml:538` 不可区分时回退 `invoke_agent`） |
| 🟢 P2 | **harness 指令 vs 任务指令分层** | MAF `DEFAULT_HARNESS_INSTRUCTIONS` | 无此分层（配合 P0 AGENTS.md 一起设计） |
| 🟢 P2 | **per-node SkipPreHandler**（resume 跳过该 node input 预处理重跑） | eino `compose/checkpoint.go:110` `SkipPreHandler map` | 你「preamble 只跑首轮」是 BasicAgent 专属粗粒度，可推广到任意 GraphAgent node |
| 🟢 P2 | **FailoverContext 扩参**（部分输出 + attempt 喂 ShouldFailover 决策） | eino `adk/failover_chatmodel.go` `FailoverContext` | 你 `fallback.ShouldFailover(err)` 仅收 error，可评估扩参（已生成 80% 时是否值得切） |
| 🟢 P2 | **turn 级生命周期 + 细粒度 cancel**（planning/active/idle + WaitUntilIdle/AfterTimeout/UntilIdleFor） | eino `adk/turn_loop.go` | 你 BasicAgent 无显式 turn，cancel 是 `ctx.Done()` 全局中止；eino 可在 active turn 中注入中断 / 延迟停止 |
| ⚪ 决策 | **AgenticMessage 双格式**（Claude 原生 ContentBlock 树）是否跟进 | eino `schema/agentic_message.go` | 仅 OpenAI 线性格式；架构方向题，需拍板 |

> **其它已记录、低优先 DX/打包模式**（非能力缺口，备查，避免遗漏）：① MAF `create_harness_agent()` batteries-included 工厂（一入口接全 provider + per-feature disable）——你 harness 能力散在各 `WithXxx`，可考虑统一工厂；② CrewAI `@persist`/`@human_feedback` 装饰器驱动持久化（元数据标记 + runtime 读 flow 定义驱动）——cp 写法可加 DSL 糖衣；③ eino multimodal 文件工具（`MultiModalReadFileTool`/`EnhancedRead`）/ OpenAI `editor`/`apply_diff`/`computer`——文件读写/编辑/computer-use 下沉，你按 tool 实现即可（非框架层缺口）。

### crew-go 已领先（防「建议照抄」反例）

| 维度 | crew-go | 对照 |
|---|---|---|
| context 压缩 | **4 层**（MicroCompact/AutoCompact/ReactiveCompact/StoreMemory） | eino 仅 1 层；OpenAI 仅 API 级；LangGraph/CrewAI 核心无自动多层 |
| 编排原语 | **9 显式编排**（编译为同一 channel BSP 图） | LangGraph 核心 barebone；OpenAI 仅 handoff；CrewAI 2+Flow |
| 嵌套 cp 隔离 | 物理分离 `(sid, ns)`（省存储 + per-ns timeout） | eino 内嵌父 cp（父子原子一致 / 无孤儿子 cp）——**设计取舍，非单方领先** |
| 选择性 resume | ✅ 只重跑失败分支 + CompletedFrontier/DeferredSends | 与 eino 同语义（并列）；OpenAI/CrewAI 无图超步恢复 |
| 模型 failover | **fallback 包**（有序候选 + per-member 断路器 + StatusCode 分类） | eino 回调式无框架断路器；OpenAI/LangGraph/CrewAI 无 |
| conformance 套件 | **Store + Checkpointer + Directory 三套** | LangGraph 仅 checkpoint；eino/OpenAI/CrewAI 无 |
| A2A 服务发现 | **`registry.Directory` + 三 selector + ZK binding + 4 元组寻址** | CrewAI 纯 client 无发现；MAF 原生 A2A（发现深度未核实） |
| OTel gen_ai.* | 大体对齐（28 metric 常量 + span attr），仅需 P0 那几处补齐 | eino/CrewAI 未对齐 gen_ai.* |

---

## 四、后续专题文档落盘计划

本文档作权威源，专题文档引用本文事实：

| 专题文档 | 内容 | 关系 |
|---|---|---|
| `harness-survey.md`（新建） | harness 融入趋势专题：MAF/eino/OpenAI 三方 harness 解剖 + Claude Code 能力对照（带 ✅官方文档/⚠️运行时观察/❌未证实血缘 三档）+ crew-go harness provider 缺口 | 收敛② 展开 |
| `skill-system.md`（增量） | 补 SKILL.md ×5 收敛表 + 各家差异（fork/脚本/白名单/自动晋升） | 收敛① 展开 |
| `observability-trace-spec.md`（增量） | 补 OTel `invoke_agent` 拆 SpanKind + 新 token 属性 + crew-go P0 对齐项 | §1.6 + P0 展开 |
| `eino-langgraph-upgrades.md`（新建，**升级点主文档**） | eino+LangGraph 近期 commit 对 crew-go 有用的升级点**全集**：agentsmd / AgenticMessage 双格式 / toolsearch / TurnLoop+细粒度 cancel / failover+FailoverContext / SkipPreHandler / 三层 interrupt **＋ cp 类升级一并放此篇：DeltaChannel 增量 cp / 选择性 resume 机制 / 嵌套 cp 取舍 / coordinate-keys / Durability** ＋ 对应 action items | §1.1/1.2 + §三 |
| `checkpoint-storage.md`（**仅加交叉指针**，不重写） | 既有 cp 存储三方对照文档保留；加一节指向 `eino-langgraph-upgrades.md` 的 2026-Q2 cp 新发现（DeltaChannel / 选择性 resume / 嵌套 cp 取舍），避免重复 | 交叉链接 |

> **校验顺序建议**：本文及各专题文档的**外部框架事实已在调查期逐行核实**；落盘后 audit 聚焦 **doc-writing 6 类偏差**（尤其 crew-go 自身列的 anchoring）。先写后审、每篇 3–5 轮多视角 audit（见 `doc-writing` skill）。
>
> **本文 audit 状态**：R1 已完成——2 个单视角 agent（① crew-go 自身列 grep 白盒核实 ② 外部框架 file:line 核实），发现均经人工交叉验证（驳回 1 处 agent 误判：`reasoning_tokens` 被误判为「已新规范」，实测 OTel 新名为 `reasoning.output_tokens`）。R1 净修正 5 处：MAF 行号/路径、CrewAI 引用、checkpoint SchemaVer 精度、token trace 精度、完整性补漏 6 项。R2 已完成（内部一致性 / 表格逻辑 / 孤儿升级点自审）——补 3 处：§一 标题口径（五方→5 框架+OTel）、TurnLoop 细粒度 cancel action item、A2A 服务发现「已领先」行。R2.5 完整性对账（对全对话历史所有「值得做的点」逐项核对，用户抽查触发）——补回 2 行漏项（**fork skill 扩展钩子 #905** 实测真缺口、**OTel 新 span 名** invoke_workflow/plan/create_agent）+ 3 条低优先 DX 模式注。R3 已完成（独立 agent 正交查漏 + 我交叉验证）——8 功能 claim + 4 绝对断言全成立、无新查漏（R2.5 完整性已足）；驳回 1 误判（建议「Durability 非框架控制降 P2」——理由错，Durability 是 Run 时框架选项，保持 P1）；采纳 3 真修正（嵌套 cp「更优」→取舍、SchemaVer「未激活」→刻意宽松策略 Option D、cache_creation 需先加 Usage 字段）+ 自查补 eino 三层 interrupt。**三轮 audit 后接近收敛，不承诺「已扫干净」，剩余可能仍有 <5 处**。R4 跨文档批量 audit（5 篇专题写完后，我整体审 + 2 独立 agent 并发 ① 跨文档同病 ② 链接完整性，三方交叉验证）——同病修 2 处（SKILL.md 计数口径「5 采纳 / 6 有」、执行模式「最全」对齐 Claude Code 4 模式——后者 agent 与我初轮均漏、靠补 grep 抓到）；链接零断链。**至此 6 篇成一致连通知识图。**

---

## 五、决策分析：AgenticMessage 双格式是否跟进

> 对应 §三 action items「⚪ 决策」行。**结论：暂不整体跟进**，走 model client 适配层中间路。

| 维度 | 优（adopt 收益） | 劣（成本） |
|---|---|---|
| 保真 | Claude 原生 ContentBlock 树 1:1 映射（thinking / tool_use 内嵌 / reasoning+text+tool_call 交错），避免 OpenAI 线性扁平丢信息 | `schema.Message` 是 `modules/common` type alias（spec 生成）；引第二格式 = 扩 spec 或并行类型，偏离 spec |
| 流式/MCP/多模态 | ContentBlock 分离 → 细粒度流；MCP/server tool/多模态都有一等 block | — |
| 侵入性 | — | **巨大**：eino 用泛型 `MessageType` 穿透整个 ADK + 每个 middleware（双分支穷举）；跟进 = 4 层压缩 / HITL cp 序列化 / 9 编排 / 所有 hook+tool 全要泛型化 |
| cp 序列化 | — | tree 跨 cp 边界序列化面翻倍（eino 有 GobEncode/Decode 穷举） |
| provider 现实 | — | 测试/生产主力 qwen/deepseek/openai 都是 OpenAI 兼容 wire，线性才是原生；双格式主要利好 Claude 原生 API |

**建议**：核心内部仍用线性 `schema.Message`；在 **model client 适配层**（openai/qwen/claude adapter）保证无损 round-trip Claude 的 thinking/tool_use block——把丰富性收在适配边界，不改核心消息类型。等 Claude 原生 API 成主力 / 需 reasoning+tool 交错流时再重估立项。

---

## 六、2026-06 月度增量（06-18 → 07-01）

> §一~五 截止 2026-06-18。本节记之后一个月的**实质新增**——扫本地 5 clone 全量 + web。
> **langgraph/eino 本窗口无实质新增**（全 bug fix），多数"6 月热点"是 4-5 月转载（Managed
> Agents / A2A v1.0 / MCP RC / ReasoningBank），已剔除，下面只列真新增 + 有参考价值的。

### 1. Sandbox 安全模型（软层本轮已实施 + 硬层未来）⭐

沙箱执行外部 / marketplace skill 内容，既有 prompt 注入面又有执行隔离面（来源：OpenAI
`sandbox/remote_mount_policy.py` 06-18 后硬化 + 6 月沙箱安全共识 E2B/Daytona/OWASP）：

- **软 prompt-policy（✅ 已实现）**：`sandbox_bash` 输出不可信数据框定（随机 nonce 标签防
  file-content 注入，与 skill-metadata `sanitize` 正交）——见 [`doc/v2/sandbox.md` §3.1](../../v2/sandbox.md)。
- **硬执行隔离（后端负责 + 未来 conformance 契约）**：kernel 隔离（microVM/gVisor）/ egress
  default-deny+allowlist / read-only mount / per-task 短命 secret / 3 级超时。后端实现
  各自自控；**未来可 codify 成对可插拔 sandbox backend 的 conformance 契约**（照 Store/
  Checkpointer/Directory 三套模式）。
- **mount policy（不投机）**：per-path 读写 + 命令 allowlist + mode-gated edit 针对对象存储挂载
  ——crew-go 沙箱无挂载、无 consumer，不实现（同删 `ScriptExecutor` seam 的反投机原则）。

### 2. MATM 群体级 trajectory 记忆（新研究，backlog）

arXiv:2606.19911（CMU+Berkeley，06-18）：把 RAG 从单 agent 扩到 **agent 群**——成功 agent 的执行
trajectory 存共享库、供其他 agent（跨编排）检索复用，无需协调 / 联合训练。crew-go 的 `store.Store`
语义是"用户偏好 / 事实"，MATM 是**新维度**（过程性知识跨 agent 复用）；9 编排 + sub-agent 委派天然
是异构 agent 群。**backlog：可评估 `store.Store` 上加"trajectory 复用"层**。

### 3. tournament 编排原语（缺口，backlog）

Anthropic "Dynamic Workflows"（InfoQ 06-15）编排模式：N agent 竞争同题择优。crew-go 有
`ReflectAgent`（1v1 adversarial verification）**但缺 tournament（N 竞争选最优）**——可作 `ParallelAgent`
+ `WithAggregator` 的 best-selection 策略（取最优而非 merge）。**backlog。**

### 4. MAF harness 可借鉴手法（06-18 后 130 commit，主题"收口 + 收紧安全"）

MAF compaction 仍 sliding-window + tool-result 两档，**弱于 crew-go 4 层**——无反驳"4 层领先"。真正
可借鉴的是几个 harness 交互手法（都能挂现有 seam、不伤筋动骨）：

- **plan/execute 模式分离**（`AgentModeProvider`）：轻量 provider 注入"当前 mode + mode_set/get 工具"、
  mode 存 session state，让同一 agent 运行时切换行为模式。crew-go 9 编排但无"agent 内 mode 切换"，
  补 BasicAgent 空白。坑：外部改 mode 必注入 user-role 通知（模型会锚定自己早先的 mode_set tool call，
  光 system 指令改不动）。
- **loop progress-log + fresh_context**（`AgentLoopMiddleware`）：跨轮 feedback log 注入 + 每轮清历史只
  留 progress（省 token）——可增强 `LoopAgent` / `ReflectAgent`。
- **文件工具 surgical edit + 只读档**：`replace_lines`（按行号批量改 + 越界检测）+ 只读档 + 分档
  approval。crew-go 只有 `sandbox_bash` 一个 Exec 入口，可在其上加结构化 file 工具（复用 sandbox seam
  当 backend）。

### 5. 验证 crew-go 已领先 / 决策对（防照抄反例）

- **A2A registry 仍未标准化**（官方"community is considering"）→ crew-go 的 registry（Directory +
  selector + ZK binding + conformance）**超前协议标准**，持续跟进 registry SEP 保 wire 对齐即可。
- CrewAI 06-18 加 inline skill loader = crew-go `NewInMemorySkillBox`（crew-go 更通用）；CrewAI 仍留
  `call: script` + 文档标"scripts not sandboxed" → **印证 crew-go 删声明式脚本 action、收敛 `sandbox_bash`
  更对**。
- Codex 06-25 把 MCP tool-search 设默认 = crew-go `WithToolSearch` 早有。
