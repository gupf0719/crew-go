# Harness 融入 Framework 趋势深析（2026 H1）

> 配套主纲 [`industry-survey-2026h1.md`](industry-survey-2026h1.md)（权威源，§二 收敛②）。本篇深挖**三方 harness 解剖 + Claude Code 能力对照 + crew-go harness 缺口**；主纲已列的事实不重述，只引用。
>
> **证据纪律**：外部框架 file:line 已在调查期逐行核实（主纲 §四 R1）。Claude Code 部分按三档标注——**✅ 官方文档确证 / ⚠️ 运行时观察 / ❌ 未证实血缘**——能力相同 ≠ 照抄。

## 一、趋势结论

**「框架层 ↔ harness 层」边界正在合并。** harness = 模型推理之上「真正执行」的那一层：shell/文件访问、HITL 审批、长会话 context 管理、工具循环、记忆、skill。2026 Q2 起三个库/SDK 把它做进框架本体（+1 独立仓）；**2026 Q3 又加入两个整体即 harness 的产品**（dsh / Codex），LangGraph 核心保持极简：

| 框架 | harness 载体 | 完整度 |
|---|---|---|
| **MAF** | `create_harness_agent()` / `_harness/` 子包（core 一等公民） | **SDK 里最完整**（10+ provider） |
| **eino** | `adk/` 层（独立于 `compose/` 图引擎） | 中（TurnLoop + 8 middleware） |
| **OpenAI Agents SDK** | `sandbox/` + `editor`/`apply_diff`/`computer` | 偏「执行工具下沉」 |
| LangChain deepagents（+1） | 独立仓 | harness 模式 |
| **DeepSeek Harness (`dsh`)**（2026-08-13 开源） | 整个产品即 harness——50 个包组，everything is a plugin | **产品档**（执行类能力全家桶，见 §七）|
| **Codex**（OpenAI，Rust，2026-08 复核） | 整个产品即 harness——100+ crates，近一月 1232 commit | **产品档**（见 §八）|
| LangGraph（对照） | 核心保持极简 | harness 留给上层 |

⚠️ **2026-08 新增的一条共同信号：Code Mode**——把工具暴露成代码命名空间、让模型写代码在受限 cell 里调用，而不是逐次 tool_call。Codex（`code-mode` 四 crate）与 dsh（`code-runtime` 的 Code Mode Consumer）**独立做了同一件事**，我们零覆盖，详见 §8.3。

crew-go 把 hook（8 业务 + 5 编排）/ 4 层压缩 / HITL / observability 做进框架——**本质同路**，趋势验证了设计方向。本篇聚焦差距：**别人的 harness 含哪些 provider，crew-go 缺哪些**。

## 二、三方 harness 解剖（调查期三家；后加入的 dsh / Codex 见 §七 / §八）

### 2.1 MAF `create_harness_agent()` — batteries-included 工厂

`python/packages/core/agent_framework/_harness/_agent.py:226` 一个工厂自动接好下列 provider（每个可 disable），`_harness/` 子包逐一对应。**这是「harness 融入 framework」最强的参照实现**——在 core 包，不是附加件：

| provider | 文件 | crew-go 对应 |
|---|---|---|
| 自动 tool 循环 | `_loop.py` | ✅ BasicAgent 四节点 |
| 历史持久化（每 model call 后） | history provider | ✅ Session/cp |
| context 压缩（前/后两策略） | `_compaction.py` | ✅ 4 层（你更多，见主纲已领先表） |
| **TodoProvider** | `_todo.py` | ❌ 无 |
| **plan/execute ModeProvider** | `_mode.py` | ⚠️ 有 PlanAgent，无 mode provider 抽象 |
| 文件型记忆 | `_memory.py` | ✅ StoreMemory |
| **SkillsProvider**（渐进披露） | `_skills.py` | ✅ skill 系统（见 [`skill-system.md`](skill-system.md)） |
| **后台子 agent** | `_background_agents.py` | ❌ 无（仅同步嵌套） |
| **工具审批**（don't-ask-again） | `_tool_approval.py` | ✅ `WithApprovalMode`（`agent_v2/approval.go`，2026-06-24 落地——**本行原记 ❌，是调查期实况**；§四 表为准） |
| shell / web search | `_file_access.py` / option | ➖ 按 tool 实现（`sandbox.NewBashTool`），非框架缺口——见 §四 末行 |
| OTel | observability | ✅（见 [`observability-trace-spec.md`](observability-trace-spec.md)） |
| **harness 指令分层** | `DEFAULT_HARNESS_INSTRUCTIONS` | ✅ `WithAgentsMD` transient prepend（见 §五-1；**本行原记 ❌**） |

compaction 细节：前 `ContextWindowCompactionStrategy`（token 预算）+ 后 `ToolResultCompactionStrategy`，用 `token_count` 分组注解 + `_summarized_by_summary_id` 追踪——**与 crew-go `GroupMessagesByAPIRound` + 摘要 marker 同 DNA**。

### 2.2 eino ADK 层 — TurnLoop + middleware

`adk/`（独立于 `compose/` 图引擎）。核心：push-based `TurnLoop`（`adk/turn_loop.go`，显式 turn 三态 planning→active→idle + 三档 cancel + `UntilIdleFor`）+ middleware 栈 **8 个**（`dynamictool` 渐进工具披露 / `skill` / `summarization` 压缩 / `agentsmd` AGENTS.md 注入 / `filesystem` 文件访问 / `plantask` / `patchtoolcalls` / `reduction`）；`failover`/`retry`/`interrupt`/`cancel` 在 adk 顶层（非 middleware）。详见 [`eino-langgraph-upgrades.md`](eino-langgraph-upgrades.md)。

### 2.3 OpenAI Agents SDK — 执行工具下沉

不是「provider 全家桶」而是**把执行类能力下沉进 SDK**：`sandbox/`（Local/Modal/Daytona/Vercel/RunLoop/Blaxel 多后端）、`editor.py`（ApplyPatchEditor）、`apply_diff.py`（V4A diff）、`computer.py`（computer-use ABC）。编排极简（仅 handoff + agents-as-tools，无内置编排原语）。

## 三、Claude Code 能力对照（三档证据）

> **目的**：MAF/eino 的 harness 与 Claude Code harness 能力高度重合，但「重合 ≠ 照抄」。本节按官方文档严格区分「能力事实」与「血缘断言」，供设计评审在有人说「照抄 Claude Code X」时核对。

**Claude Code harness 官方能力清单**（少数运行时可观测但官方文档未明确者标 ⚠️）：

> ✅ **URL 已复核**（`claude-code-guide` agent WebFetch，2026-06-18）：域名/路径根 `code.claude.com/docs/en/` 确认正确，11 条中 9 条精确、2 条锚点已按复核微调（MEMORY.md → `/memory`、子 agent → `/sub-agents`）；能力描述与官方文档 **100% 相符**。

| 能力 | 出处 | 档 |
|---|---|---|
| agentic loop（工具循环） | code.claude.com/docs/en/how-claude-code-works | ✅ 官方 |
| 会话持久化/恢复（JSONL） | 同上 #work-with-sessions | ✅ 官方 |
| context 自动压缩（先清旧 tool 输出，再摘要） | 同上 #when-context-fills-up | ✅ 官方 |
| Skills（SKILL.md + 渐进披露，遵循 agentskills.io 开放标准） | code.claude.com/docs/en/skills | ✅ 官方 |
| 工具权限规则（allow/ask/deny + "don't ask again"） | code.claude.com/docs/en/permissions | ✅ 官方 |
| Shell / Web Search / MCP | how-claude-code-works #tools / mcp | ✅ 官方 |
| Hooks（PreToolUse 等生命周期） | code.claude.com/docs/en/hooks-guide | ✅ 官方 |
| CLAUDE.md（项目级持久指令） | how-claude-code-works #what-claude-can-access | ✅ 官方 |
| 自动记忆（MEMORY.md） | `code.claude.com/docs/en/memory` #auto-memory | ✅ 官方 |
| 子 agent（独立 context） | `code.claude.com/docs/en/sub-agents` | ✅ 官方 |
| Checkpoint（文件编辑快照回滚） | 同上 #undo-changes-with-checkpoints | ✅ 官方 |

**血缘结论**（严格）：

- **能力收敛 ~70–80%**：agentic loop / context 压缩 / 会话持久化 / 权限 / todo 等都是当代 agent harness 通用做法，**设计独立、无因果证据**。
- **唯一可溯源血缘**：Skills → [agentskills.io](https://agentskills.io) **公开标准**。Claude Code 官方文档原文（`code.claude.com/docs/en/skills`，已复核）：*"Claude Code skills follow the Agent Skills open standard...extends the standard with invocation control / subagent execution / dynamic context injection"*；MAF `_skills.py` 源码亦自引 agentskills.io。即 **Claude Code 与 MAF 同遵一个公开标准**，非互相照抄、更非 Claude Code 独创。
- **❌ 不可断言**：「MAF/eino 照抄 Claude Code 的 X」——除 skill 外**均无文档/源码证据**。功能相同、设计相似、两边都支持，**都不构成血缘证明**。
- **架构差异反映生态定位**：Claude Code 是 CLI harness（文件系统中心：CLAUDE.md/MEMORY.md/skills 目录、shell hooks）；MAF 是 SDK framework（Python 对象中心：context provider / middleware）。差异是定位不同，非模仿。

## 四、crew-go harness 缺口

主纲 §三 action items 已列优先级，本节按 harness 维度归类（现状均 grep 核实）：

| 缺口 | 参考 | 决策（2026-06 scoping，理由见下方决策记录）|
|---|---|---|
| **AGENTS.md/外部指令注入中间件** ✅ **已实现 `WithAgentsMD`** | eino `agentsmd`（transient） | ✅ 已做 |
| **工具审批/权限规则层**（don't-ask-again + 启发式） | MAF `_tool_approval.py` | ✅ **已做**（P1，`agent_v2/approval.go` commit b81776c/fddeed1）|
| **后台子 agent**（异步委派 + 监控 + 取回） | MAF `_background_agents.py` | ⏸ **defer P2**（backlog）|
| **harness 指令 vs 任务指令分层** | MAF `DEFAULT_HARNESS_INSTRUCTIONS` | ⏭ **skip P3**（eino 同构无缺口）|
| **TodoProvider** | MAF `_todo.py` / dsh `tool-todo` / Claude Code TodoWrite | ⏸ **defer**（2026-08-13 重估：**产品层能力，非 SDK 层**——见下方决策记录）|
| Shell / 文件读写编辑 / computer-use（按 tool 实现即可，非框架层缺口） | MAF / OpenAI / eino（`adk/filesystem` + filesystem middleware） | ➖ 非框架缺口（eino 做进 adk，crew-go 定位 Go 最简 SDK、文件访问按 tool/MCP 实现——定位差异非缺口）|

### 决策记录：harness 4 块 scoping（2026-06，8 源本地源码调研后）

> **定位锚点**：crew-go 是 **Go 原生最简 SDK**，对标 claude-agent-sdk（通用 SDK 层）非 Claude Code（端到端 CLI 产品）。这是下面所有 do/defer/skip 的依据；**列 backlog 以本表 verdict 为准，别再当 🟢P2 待办重列**。

| 块 | 决策 | 理由 | 若做的最小设计 |
|---|---|---|---|
| 工具审批/权限层 | **✅ 已做（P1）** | 真缺口（纯 interrupt 派每次工具调用重复中断、无 don't-ask-again）；横切层业务自建必重复易漏；5/7 框架做进 SDK；零新并发原语 | **实际实现**（`agent_v2/approval.go`）：`ApprovalMode`（Auto 默认零开销 / Ask）+ `WithApprovalHeuristic` callback seam（**不**内置 shell allowlist；per-tool 策略经 heuristic 覆盖，未做 ApprovalRule struct）+ `approve_always` / call-approved 规则存 RunSession KV（跨 Resume 需 `WithResumeSession`）。拦截在 **`makeToolsFn` runOne 内**（inline+fanout 都过，规则 RMW 用 `approvalMu` 守护非"串行预查"），`Ask` 复用 ask_user 稳定 ID + `InterruptError` + cp 挂起，fanout 多工具 `MergeInterrupts` 一次问完。**ReadOnly/Bypass 模式未做**（ReadOnly 需 tool 只读语义判据，待 `tool.Categorizer` 明确）|
| 后台 async agent | **defer P2** | 同步嵌套（RunSubAgent/Parallel）覆盖 90%；token 效率优化非正确性缺口；BSP 下裸 goroutine 是项目 godoc 明令禁止的反模式 → 须框架托管 ns/cp/cancel 生命周期，不能留业务 | `BackgroundRunner` map[taskID] + detach ctx（`WithoutCancel`+`WithCancel`）+ 独立 ns `bg/<taskID>` 防撞 cp + 四态对齐 MAF（崩溃标 Lost）+ defer Cleanup；后台 agent fail-fast 拒 HITL |
| harness 指令分层 | **skip P3** | eino（最近 Go ADK）同构：`agentsmd` middleware + 单 `Instruction`，**无独立 harness 槽、无默认文案**——这是 Go SDK 标准；crew-go `WithSystemPrompt`+`WithAgentsMD` 已等同。只 MAF/Codex（batteries/全 CLI）拆第 3 槽 | （不做）若真需 operation vs project 分槽：加可选 `WithHarnessPrompt(text)` 复用 AGENTS.md transient prepend；**绝不内置默认文案**（双语项目硬编码单语言是负债 + 经验法则会 stale + 一套全局对 9 编排平庸）|
| TodoProvider | **defer P3** | **（2026-08-13 重估，原三条理由换掉两条）** 判据是**产品层 / SDK 层的分野**，实测 3:3 清晰对立——内置的三家都是产品（Claude Code / dsh / MAF harness），不内置的三家都是 SDK（`claude-agent-sdk-python` grep 零命中、OpenAI Agents SDK 仅在 `extensions/experimental/codex/` 出现、LangGraph 无）。我们是 SDK，故 defer。⚠️ **原理由「现有三件套全覆盖」已删**：它跟同表审批那条的论证互相矛盾（那条用「横切层业务自建必重复易漏」支持做），「能自建」不能同时既支持做又支持不做。真正区分两者的是：审批业务**做不了**（要挂中断 + 跨 Resume 持久 + fanout 合并），todo 业务**做得了且漏了不致命**（顶多列表不准，不是安全问题），且外部采纳度相反（审批 5/7 进 SDK，todo 0/3 进 SDK）。⚠️ **原理由「形态高度业务相关（TUI/SSE/批处理各异）」半错**：差异不在渲染通道——dsh 明确把渲染分离了（工具只 append `todo/write` 事件，UI 订阅事件流自渲染）；真实差异在**数据契约的设计选择**：dsh/Claude Code 是「全量替换 + 三态 + 无 id」，MAF 是「按 id 增量完成 + 布尔态 + 完成要给 reason」，两条路| `example/todo_tool/`：FuncTool over `GetSession` + `SetValue("todos")` + `OnModelStart` 回灌；跨 Run 接 `store.Store`；文档讲清 vs PlanAgent 边界（PlanAgent 是 replan 编排器，todo 是单 agent 自管 checklist）。**若将来真做，形态选「全量替换」不选「按 id 增量」**——理由是我们独有的约束：Resume 会重跑节点，全量替换是 last-write-wins、天然幂等；按 id 增量在重跑时会重放「完成第 3 项」这类操作，得额外自证幂等。这一条外部两条路都没考虑（它们没有超步重跑）|

> crew-go **已具备**的 harness 关切（不需补）：tool 循环、4 层 context 压缩（>MAF）、HITL resume、StoreMemory、skill（fork/inline 模式）、observability、fallback——见主纲「已领先」表。

### 决策记录：审批与调用内容的绑定（2026-10-14，五家本地源码调研后）

**问题**：一次工具调用在续跑之间可能被 `OnToolStart` 改写成别的内容（注入时间戳、请求号，改工具名），而审批结论、
用户的答复、工具账都是对第一次那份做的。逐个反例打补丁改了三版（只按调用重放 → 被改写的参数没被判过就执行；按内容
重判 → 钩子每次注入新值时启发式一改口就推翻用户的拒绝；只在审批生效时固定内容 → 工具账按没固定的内容记、审批关掉时
工具自己的提问照样拿旧答复执行新内容），均实测。

**五家怎么做**（源码位置在 `/Users/gupengfei/program/go/src/` 下）：

| 家 | 改写 | 审批 | 提问身份 |
|---|---|---|---|
| LangChain v1 / LangGraph | `wrap_tool_call` 用 `override()` 临时改写、不写回，续跑整节点重跑、改写重算（`prebuilt/tool_node.py:178-207`）| 独立的 `after_model` 步骤对模型原始调用提问，结论写回 messages + 以 tool_call_id 为键的 edit 表；**批准之后别的中间件再改参数不复核**（`langchain_v1/.../human_in_the_loop.py:356-357,456-525`）| 每个任务一个计数器，审批与每个工具调用各是一个任务（`agents/factory.py:2031-2032`），物理上串不了 |
| OpenAI Agents | **没有改写口子**，护栏只能放行 / 拒绝 / 抛错（`tool_guardrails.py:60-117`）| 按 call_id 记，call_id 绑内容的 sha256，同一个 ID 换了内容直接报错（`_tool_invocation.py:155-219`）| 工具体没有提问原语 |
| MAF | 中间件每次执行重算、不写回（`_tools.py:2308-2315`）| 在中间件之前对模型原调用提问；执行时比对批准时的参数，变了就发新 ID 重问（`_tools.py:1053-1065,2077-2106`）——改写每次注入新值时重问收不住（调研员推断，未实测）| 工具体问人与审批按消息类型区分 |
| eino | `ToolArgumentsHandler` 每次重算、不写回（`compose/tool_node.go:215-224`）| 框架不带；**官方示例在中断时把参数存进中断状态、批准后执行存下的那一份**（`eino-examples adk/common/tool/approval_wrapper.go:63-92`）| 审批包装与工具体共用一个地址，框架没处理 |
| Codex | PreToolUse 改写只生成内存里的新调用（`core/src/tools/registry.rs:161-185`）| 改写之后才审批，turn 一中断审批就作废，**没有中途续跑** | 不重跑，不存在 |

**结论**：主流把审批当成执行前的一次性决定、结论按调用身份持久化、重跑时不再推导。「允许改写 + 整节点重跑 + 批的
就是执行的」三件同时成立的只有 eino 示例与我们这条路（存首次内容）。我们原先的问题是框窄了：内容是「每次调用」的
属性，不该只在审批生效时固定。

**决策**（已实现）：
- 一批调用的内容在第一次交给工具执行时定下（`agent_v2.fixToolCallContent`，在 `OnToolStart` 改写被消费的那一处、
  对所有调用、不看审批开没开；只另存被改写过的，没改写的那份本来就在存档的 assistant 消息上）。约定随之变成
  「`OnToolStart` 对 Name / ArgumentsJSON 的改写只在这一批第一次执行时生效」。
- 审批问题在自己的相位里占提问序号（同 LangChain 的「审批与工具调用各是一个任务」），工具自己的提问不随审批问不问挪位。
- 审批结论只形成一次：审批生效时一律记（含无启发式的 ApprovalAsk），续跑重放、不看当时的配置——加上或去掉启发式、
  开关审批都不推翻已形成的结论（用户的拒绝照样作数）。
- 一次调用只准入一次（审计复核后补，同日）：approve_always 规则放行也记下「已准入」，「已准入」排在本次答复之前判
  （调用方续跑时改答 deny 不推翻已开始执行的调用）；启发式拿参数的拷贝，就地改了不影响执行。「批的就是执行的」
  只对 `OnToolStart` 的改写成立：工具中间件在审批之后每次执行都跑，它改的参数不经审批。

**每次执行的值放哪儿**（审计复核后补，同日）：固定内容的代价是改写过的参数随中断档落盘、续跑沿用。各家都把每次执行
要新取的值放在模型看得见的参数之外：LangGraph 的 `InjectedToolArg` / `ToolRuntime` 在执行时注入到调用的拷贝上
（`langgraph/prebuilt/tool_node.py:1349-1462`，还剥掉模型伪造的同名字段），OpenAI 的 run context 随 `RunState`
序列化、续跑时可用 `context_override` 换一份新的（`run_state.py:2333,2365`），MAF 是 `FunctionInvocationContext`，
eino 是 ctx 与工具 Option。我们照这个分法、不加通道：凭证、短时令牌、trace_id 放钩子返回的 ctx（只用于这次执行、
不落盘）或用 `WithToolMiddleware` 在每次执行时注入，`OnToolStart` 改参数只用于改这次调用的内容（hook 包 godoc、
usage-guide 写明）。与各家不同的一点是固定内容本身：各家改写每次重算，修好钩子续跑就生效；我们修好钩子也救不回
已经定下的那一批，只能放弃任务重开——这是保住「答的、批的就是执行的」的代价，写进了文档。

**不选的**：按内容哈希绑定、对不上就报错（OpenAI 的做法）——前提是没有改写口子，我们的 `OnToolStart` 是[消费]档，
钩子每次注入新值时每次续跑都报错；对不上就换 ID 重问（MAF 的做法）——同样的钩子下永不收敛。

## 五、设计建议

1. ✅ **AGENTS.md 注入（已实现 `WithAgentsMD(paths...)`，2026-Q2）**：最终采 eino **transient 思路**——在 **model 节点每轮** prepend 为首条 system（harness 层，排 `WithSystemPrompt` 任务层之前），**不持久 / 不进 cp / 不被压缩**；比原计划「preamble 注一次 + marker + 压缩保护」更简洁（transient 自然免 marker）。实现了 harness 指令 / 任务指令分层（对齐 MAF `DEFAULT_HARNESS_INSTRUCTIONS` + Claude Code CLAUDE.md）。
2. **harness provider 化（P2，可渐进）**：Todo / 工具审批 / 后台 agent 作为可选 provider 接入，不强制——crew-go 哲学是「框架 + 编排」，harness provider 走 opt-in，避免核心臃肿。
3. **batteries-included 工厂（DX，可选）**：当前 harness 能力散在各 `WithXxx`，可考虑一个 `NewHarnessAgent(...)` 工厂统一接全 + per-feature disable，对齐 MAF `create_harness_agent()`。
4. **别照抄的**：computer-use / sandbox 多后端等「执行工具下沉」——crew-go 按 tool/MCP 实现即可，不必做进框架层。

---

## 六、2026-07~08 增量复核（复核日 2026-08-13）

> **取证方式**：各仓 `git fetch` 后按 `--since=2026-06-25` 读一手 commit（非二手综述）；Claude Code 取官方 `CHANGELOG.md` raw，读 v2.1.197~v2.1.229（该文件**不带日期**，边界经 GitHub tags/commits API 定位：v2.1.196=06-29、v2.1.202=07-06、v2.1.203=07-07、v2.1.229=08-12）。**本节只记增量，§一~§五 的调查期结论不重述。**

### 6.1 外部动向

**MAF——harness 集体毕业出 experimental，是本期最强信号**：`create_harness_agent` 本身（#7120，2026-07-20，BREAKING）、mode + todo provider（#7053）、`ToolApprovalMiddleware`（#7106）、`FileMemoryProvider`（#7113）。配套是**安全收紧而非扩表**：`FileAccessProvider` 改 opt-in（#7094）、file-access 审批可 opt-out（#6879）、auto-approval 名字碰撞告警（#7090）、Windows junction 拒绝（#7291/#7507）。另两条与我们直接同题：`agent-hooks` 拦截契约成一等 experimental core feature（#7515，2026-08-06；AGENT-HOOKS-0.1 定 8 个拦截点＝agent seam input/output + chat seam pre/post_model_call + function seam pre/post_tool_call + startup/shutdown，fail-closed，流式**全缓冲**——verdict 出来前不放行任何 update）；`scope under-specified approve-for-session permission decisions`（#7607，2026-08-13）。

**OpenAI Agents SDK——把「身份」和「持久化」补齐**：审批绑定到具体调用（#4257，新建 `_tool_invocation.py`/`_tool_identity.py`）、审批在 resume 时不生效（#4245）、hosted MCP 审批按 server 身份限域（#4256）、`durable pending input to RunState`（#4325，+1719 行含 965 行测试）、限长会话里剪掉孤儿 tool output（#4324）、session 变更原子化（#4212）、压缩替换被取消时恢复历史（#4298）、允许应用批准不安全重放（#4319）。sandbox 侧持续加固（mount 凭据配置拒绝 #4255、mount 脱敏时保住错误契约 #4344）。

**eino——adk 进入稳定期**：主线近两月仅 10 个 commit（7 fix + 1 feat + 1 refactor + 1 chore），**无新 middleware**（仍是调查期那 8 个），且**最后一次提交停在 07-22**（复核日前三周零动静；`--all` 有 293 个 commit 但都在非主线分支）。fix 集中在嵌套 cancel 作用域（#1143 统一 checkpoint-aware child cancel scope、#1151 隔离直连嵌套 agent 的 cancel scope）、summarization middleware 出错返 ctx 而非 nil（#1157）、reduction 保住流式工具输出（#1090）、tool call↔result 配对（#1091）。

**LangGraph——核心继续极简**：近两月只有 `trace_policy` 暴露到 `add_node`（#8362/#8402/#8523）和 checkpoint `omit_expired`（#8354）。§一「LangGraph 把 harness 留给上层」的判断未变。

**Claude Code**：本期几乎全部投在**权限绕过面**与 **sandbox 硬化**上——Bash 命令藏匿自身片段绕过检查、tab/不可见 Unicode 在审批对话框里遮蔽命令、zsh `[[ ]]` 内的隐藏执行、PowerShell 引号路径、>10000 字符命令一律改为提示、fd 重定向形式 fail-closed、agent 定义的 `bypassPermissions` 无视组织禁用策略、`PreToolUse` auto-allow hook 绕过后台 agent 的工具限制；sandbox 侧加了凭据掩码（`extract` / JWT `maskClaims` / AWS SigV4 重签）、`mode:"mask"`、`network.strictAllowlist`、修 deny 项尾斜杠可绕过、并把违规详情回灌进 Bash 结果让模型看得见。skill 侧对**不可信来源**下手：claude.ai 同步来的 skill 不再 shadow 本地命令、描述被净化并标注、body 不执行 `!` 命令不展开 `@` 文件。子 agent 侧本期从「能力」转向「治理」：默认后台运行 + 完成/待输入触发 `Notification` hook（v2.1.198）、并发上限 20 与每 session 200 上限、嵌套深度默认 1→3、`--max-budget-usd` 到顶即停后台 agent。

> **其中一条值得单独记**（v2.1.198）：子 agent 把「启动它的那个 agent 发来的消息」当作普通任务指令，但**agent 的消息永远不算用户的批准**。这与 §6.2#1 是同一个命题的两面——**授权的来源必须与指令的来源分开**；我们的 `approve_always` 泄漏（`62d9f76` 实测）是同一命题在**时间维**上的失败（上个任务的批准漏给下个任务），它这条是**主体维**（上游 agent 的话冒充用户的批准）。crew-go 目前只解了时间维；主体维在我们这里**暂时不成立**——审批答案的注入口只有 `agent_v2.WithResumeUserInputs`（`agent_v2/options.go:291`；a2a server 与 agui 两个远端适配层也都走它，无旁路），框架内不存在让 agent 自己产生审批答案的路径。**但这是「当前没有」不是「禁止」**：若将来开放 agent 代答，这条约束必须先立起来。

### 6.2 同期撞题（本期最有价值的部分）

**七条独立撞题，都发生在 7~8 月这 6 周内**——外部与 crew-go 互不知情地修同一类缺陷，可作为「这批改动是否修在真问题上」的外部佐证：

| # | 题目 | crew-go | 外部 | 时序 | 修法是否同形 |
|---|---|---|---|---|---|
| 1 | don't-ask-again 授权作用域过宽 | `b99ddae`+`8c3b473`（08-04，per-call 记忆搬进节点私有中断态） | MAF #7607（08-13） | 我们早 9 天 | **不同形**：我们改**存储位置**（进 cp 的 `NodeStates`，跨任务自然不可达）；MAF 按触发请求**推导作用域**，无 session 级作用域者降级一次性 + 告警 |
| 2 | 审批须绑定到具体调用而非工具名 | `3fb2c65`（08-04，`AskQuery` 带工具身份） | OpenAI #4257（08-07，新建 `_tool_invocation.py`/`_tool_identity.py` 两个模块专管调用身份） | 差 3 天 | 同形（都是给调用铸身份）。⚠️ 它的**身份算法未逐行核**（本地工作树停在该文件出现之前，仅从 commit diff 看到模块新建与 `import hashlib`）——要拿它的形态做设计依据前需先 `git pull` 后读实现 |
| 3 | 审批在 Resume 后不生效 | `fddeed1`（06-24） | OpenAI #4245（08-06） | 我们早 6 周 | 同形 |
| 4 | 待处理输入的持久化 | `38f897b`（07-30，输入持久点移到跨进边界，11 个边界补可 Resume 退出的结算） | OpenAI #4325（08-09） | 我们早 10 天 | 同形 |
| 5 | 悬空 / 孤儿 tool_call | `dbcd524`+`27b6240`（08-04） | OpenAI #4324（08-09）、eino #1091（06-30） | 三家同期 | **不同形**：我们 append-only 剪不掉，补一条中性记账文本；OpenAI 直接剪枝。分歧根源是历史层可变性，非优劣 |
| 6 | 工具调用需要 call-level seam | `143b558`（08-10，`ToolMiddleware`） | MAF #7515（08-06） | 差 4 天 | **同题不同范围**：MAF 覆盖 8 个拦截点 + 流式全缓冲；我们只做 tool call 一点，且定「`next` 至多调一次」（中断身份含「本次调用里第几次问」一维，多调会让用户答过的那条永远配不上）+ 框架工具不过链 |
| 7 | 跨会话/跨来源的归属 | `29765ab`（07-27，归属改用编排注册位置） | MAF #7041（07-15） | MAF 早 12 天 | 同题不同层：它给 context message 标来源，我们解的是「同名 agent 撞归属导致历史合流」 |

另有一条**同期但非撞题**：压缩 × 持久化的交互 bug 三家都出（我们 `eb3c16b` 07-14 offloaded tool_result 整组保留修 422；MAF #7055 07-13；OpenAI #4298 08-09）——说明这是该组合的固有难点，不是谁实现得糙。

### 6.3 §四 缺口表的增量结论

- **TodoProvider**：MAF 已毕业（#7053）。**结论仍是 defer，但判据已于 2026-08-13 重估换掉**（见 §四 决策记录 + §7.3-2）——毕业不构成翻案理由，真正的判据是「产品层 / SDK 层分野」。
- **后台子 agent**：外部两侧都在加码——MAF #6735（06-29，background agent loop 自动解析 provider + feedback message builder），Claude Code 把子 agent 改成**默认后台**并补上通知 hook 与并发/深度/预算三道闸（v2.1.198~2.1.224）。我们仍 defer P2，理由同 §四；但**外部的重心已从「能不能后台跑」转到「跑起来怎么管住」**——真要做时，闸门（并发上限 / 深度 / 预算 / 崩溃态）与 runner 本体同等重要，别只照 §四 那格的最小设计做完就收工。
- **本期新增、原表未列的三项**：会话历史层（`WithHistory`，[ISSUE-98](../../issue/issue-98-conversation-history.md)）/ 工具调用中间件（`WithToolMiddleware`）/ 节点私有中断态（`SaveNodeState`，让中断重跑跳过已成功的带副作用工具）。**血缘要分清**：中间件是**看了外部三家都提供 call-level seam** 才立项的（`143b558` 明记「零家提供包装 Tool 对象的 helper」）；另两项是本仓自己踩出来的（cp 成功即清留不住历史 / 一轮 N 个 tool_call 里一个中断则全部重放），外部有对应物但不是照着补的。
- **本期未复核**：CrewAI（近两月 332 commit）、grok-build（squash 同步无细粒度信息）、AG-UI（527 commit，属前端协议非 harness）——留待需要时再挖，本节不作断言。

## 七、DeepSeek Harness（`dsh`）对照（2026-08-13 开源当天挖）

> **取证**：本地 clone `/Users/gupengfei/program/go/src/deepseek-harness`，读 `packages/*/README.md`
> 的 ctx-key 映射表 + `docs/capability-seams.md`。版本 `0.1.0-rc.5`，标注 developer preview、
> 明说会有破坏性变更。**它是 CLI/Web 产品不是库**，但 harness 那一层跟我们做的是同一件事
> （增强单 agent），所以差距不能用「定位不同」化解。

### 7.1 它的统一范式：能力家族四件套

每个能力域都长成同一个形状——**seam（Service Definition）+ N 个实现 + policy 层 + 面向模型的工具**。
以 `fs/` 为例：契约（路径规范 / 有界文本 IO / 原子变更 + 版本守卫）→ `fs-local` / `fs-e2b` /
`fs-sandbox` 三实现 → `fs-observation-policy`（observed-state + read-before-edit + 版本守卫写）
→ `tool-fs`（模型面 read/write/edit + 读窗口化）。`shell` / `sandbox` / `terminal` / `spill` /
`compaction` 全同构。

**我们只有第一件和最后一件**（`Sandbox.Exec` 接口 + `NewBashTool`），缺中间两件：实现的多样性、
以及独立的 policy 层。

### 7.2 逐域对比

| 域 | dsh | crew-go | 判断 |
|---|---|---|---|
| 压缩 | seam + 摘要 provider + model-free tool-result pruner + 人工 `/compact` 命令；**basic provider 内含 overflow recovery**（provider 确认溢出经 `agent/request-error` 进入，绕过常规压力阈值，剪枝 + 一次最大化平衡头部缩减、保留最新不可分割单元；失败时保留原始 provider 错误）| 4 层（Micro / Auto / Reactive / Store） | **组织方式不同，不是层次多少**：它的 overflow recovery 就是我们 `Reactive` 的对应物（⚠️ 本表原写「它没有」，错）。两边都有「无实质进展不重试」的保护——它看 `surface.replaceGeneration` 是否前进，我们看压缩后长度是否缩短（`reactive_compact.go`「长度 >= curMsgs 时提前 break」）。**真差距是人工触发**：我们导出了 `MicroCompactRun`/`AutoCompactRun`/`ReactiveCompactRun` 纯函数，但它们不写回，agent 运行中没有能让主动压缩生效的入口 |
| 溢出 | **独立 seam** `ctx.spillStore` + local + policy：**超大**工具输出转文件，inline 换成有界预览 + 检索定位符 | MicroCompact 内部的 offload 动作 | **它更正交**——原文限定的是 **oversized** 工具输出（不是任何输出），但存储可换后端、且不依赖压缩触发；我们的 offload 只在 MicroCompact 触发时发生 |
| 文件 | 三实现 + **观察策略层**（read-before-edit、版本守卫写） | 无 | 纯差距；那个策略层是正确性约束不是便利功能 |
| shell | seam + bash-local / bash-sandbox / pwsh-local + 托管 `DSH_*` 环境 | 单一路径 | 无 Windows、无环境管理、无沙箱变体 |
| 沙箱 | seam + 平台后端（bwrap / Landlock / Seatbelt）+ **持久 per-session 策略** | 接口 seam，零内置后端 | 纯差距 |
| 终端 | 持久 PTY（owner-scoped，6 个模型工具），明确定位「补充 one-shot bash，不替代其更强的 per-operation 契约」 | 无 | 纯差距 |
| 子 agent | 一个 seam **6 种 provider**（注册 `ctx.subagents` 的：进程内新起 / **从父的已完成历史 fork** / ACP 跨进程 / 直接拉起 Codex、Claude Code、dsh 子进程；另有一个不注册的共享 in-process driver 与 `tool-subagent{,-control,-report}` 三个模型面工具）| 编排原语 + A2A 远端 | **形态不同**：它是运行时可插拔 provider（接产品），我们是编译期编排 + 协议互联（接协议，覆盖面更广）；但**进程内 fork 父历史**我们只在 skill 层有，非通用能力 |
| 任务追踪 | `todo`（单包）+ `goal`（状态 + 续跑驱动 + 模型面/人面双工具） | 无（PlanAgent 是编排级，单 agent 用不上）。⚠️ 我们代码里确实有 `goal`，但那是 **ReflectAgent 的一次性评估目标**（`func(ctx, goal, actorOutput)` + `goal_matched` annotation），同名不同物，不是持久目标状态 | 纯差距 |
| 循环卫生 | `guard`：重复调用**建议性提醒**（作为 `user/message` 事件注入，模型看得见）+ per-call 超时 | 仅 maxRounds 硬上限 | 纯差距，且实现成本低 |
| 会话数据 | 事件流（JSONL / SQLite）+ checkpoint policy + projection + `session-query`（血缘 / 语义过滤 / 全文搜索） | cp（执行状态快照）+ 历史层 | **正交**：它存事件流能回看，我们存执行态能续跑 |

### 7.3 三个可直接借鉴的设计手法

1. **policy 独立成层**：`fs-observation-policy` / `sandbox-policy` / `spill-policy` / `timeout-policy`
   ——正确性约束既不混进实现也不混进工具。我们的审批逻辑目前长在 `makeToolsFn` 内。
2. **不是所有能力都该做 seam**：`todo` 明确单包、不做 provider 契约，理由是「一个会话拥有这张
   列表，没有可替换的 provider 契约」。⚠️ **这触发了 §四 TodoProvider 的重估（2026-08-13 已完成）**——
   我们原写「形态高度业务相关（TUI/SSE/批处理各异）」，而它把渲染分离了（工具只 append
   `todo/write` 事件，UI 订阅事件流自渲染），所以「渲染各异」不构成不能内置契约的理由。
   重估结论：**defer 不变，但三条理由换掉两条**，新判据是产品层/SDK 层的 3:3 分野，详见 §四
   决策记录。⚠️ 顺带纠正一个反例：形态并非唯一——MAF 走的是「按 id 增量 + 布尔态」，与
   dsh/Claude Code 的「全量替换 + 三态」是两条路。
3. **持久化不变量要对「策略收紧」前向兼容**（读实现才看到的一条，README 未突出）：`tool-todo` 的
   `invariant.ts` **刻意不校验有几个 `in_progress`**，注释写明理由——那是部署策略的事，写进不变量的话，
   「一份在旧策略下合法写入的历史，会在策略收紧后被判非法」。⚠️ 这条直指我们：`checkpointer_conformance`
   与历史层的校验一旦加严，**旧存档会不会被判非法**？我们没系统想过这个方向。
4. **checkpoint 切在语义耐久点**：model adapter 收请求前、顶层工具体可能产生外部副作用前、
   每个 `agent/pre-step` 边界；且 **persistence backend 与 checkpoint policy 是两个独立插件**
   （只装 backend 不装 policy 是合法配置，代价是崩溃丢批量窗口内的事件）。我们的 `WithDurability`
   是引擎按超步边界写、与 backend 耦合。⚠️ **这条不宜照抄**：它是单线程对话循环，没有 frontier /
   并发分支，不需要执行状态快照；我们的 cp 要存 frontier + channel + 挂起清单。切法差异源于执行
   模型差异，不是优劣。

### 7.4 我们独有（对照它确认）

图引擎 + cp（并发分支、超步部分失败只重跑失败成员、跨嵌套层存档物理隔离、多分支中断各自寻址）；
9 种编排原语可无限嵌套；conformance 套（给业务自研 backend 的验收标准）。⚠️ 措辞更正：它的 `test-support/` 组自述是「支持仓库开发/测试与示例、**而非产品 API**」，故确无面向第三方实现的合规套；**但它有另一条路**——`runtime-diagnostics/invariants`（`ctx.invariants`）做**开发期运行时契约断言**。同一个目标两种解法：我们在测试期验第三方实现，它在运行期验自己；协议接入
（A2A 双向 / AG-UI / MCP）；observability 对齐外部 spec。

### 7.5 缺口优先级（建议，未拍板）

1. **`fs` 家族含观察策略层**——单 agent 干活的地基，且业务每接一次就重造一次
2. **`guard` 那一层**——监听工具事件 + 注入提醒，成本极低，收益是防 agent 空转
3. **`spill` 拆成独立 seam**——实现我们已有（offload），只是绑在压缩层里
4. ~~**重估 `todo`**~~——**已完成**（2026-08-13，结论仍 defer 但换了判据，见 §四 决策记录）
5. terminal / goal 靠后

> **本节未挖（调查期实况；这些已于 §九 全部补挖完成，结论见 §9.1 / §9.7）**：`extensions`（模型自写插件热挂载）/ `session-query` / `identity` / `feedback` /
> `attachment` / `lsp` / `jobs` / `workflow` 的内部设计，以及全部源码级实现（只读了 README 的
> ctx-key 映射）。**不作断言**。

## 八、Codex 对照（2026-08-21 复核）

> **取证**：本地 clone `/Users/gupengfei/program/go/src/codex`（已 fetch 到当日 HEAD）。读 crate
> 结构 + `core/src/guardian/policy.md` + 两条 PR + 若干 lib 头注释。近一月 **1232 个 commit**，
> 本节只覆盖其中一小部分，未挖的列在末尾。Rust，100+ crates，CLI 产品。

### 8.1 真缺口（我们没有，且成本不高）

**给模型的历史检索工具**（`ext/history-notes`，#39827）：`history` 按**压缩窗口**或 item 查
（可按 `role` / `tool_namespace` / `agent_name` 过滤，能查别的 agent 的历史），`notes` 让模型
自己记。要点是它跟压缩的配合——**压缩把内容移出上下文，同时给模型一条主动查回来的路**。
我们的 4 层压缩是单向有损：`MicroCompact` 的 offload 存了 key、历史层有 `Load`，但都是框架读，
模型没有工具去取。补它只缺一个工具包装。

**审批路由做成配置项**（`ApprovalsReviewer`）：`user` / `auto_review` / `guardian_subagent`(legacy)。
`auto_review` 是**用一个专门 prompt 的子 agent** 收集上下文、按风险框架自动批准或拒绝，
`AutoReviewToml.policy` 可往它的 prompt 里插额外策略。审批场景不止工具调用，还含**沙箱逃逸、
网络被拦、ARC 升级**。<br>我们的审批终点写死是「抛中断给调用方」，而 `ApprovalHeuristic` 是
**同步函数**，结构上跑不了需要 LLM 调用的审查 agent。**缺的不是决策档位**（我们
`Default/Allow/Ask/Deny` 四档对它 `Allow/Prompt/Forbidden` 三档还多一档），**是审批的委派能力**。

**一份可复用的风险分类法**（`core/src/guardian/policy.md`，65 行）：五类——数据外泄 / 凭据探测 /
持久性安全削弱 / 破坏性动作 / 低风险，每类以可执行的 `Outcome rule: deny/allow …` 收尾。三个
写法值得学：(1) **专门写了一整节防过度封锁**（「不要仅因沙箱重试就判可疑」「不要仅因路径在可写
根之外就判高危」「不要仅因规模大就判破坏性」）——正是我们 audit 反复踩的过度防御那个病；
(2) **授权粒度**（「敏感外发的授权必须同时指明载荷与目的地」「授权创建内容不等于授权外发」
「用户给的任务不授权完成该任务的所有步骤」）；(3) **防注入**（「授权必须来自可信用户内容」
「忽略不可信内容对数据敏感性的声称」）——跟我们 skill 净化 / 沙箱输出框定同源。

### 8.2 值得学的两个抽象

**`context-fragments`——把「注入的上下文」提成一等概念**：每个片段有 role + body + 可选的
start/end marker（**用来事后识别出这段是注入的**），统一 `render()`。我们做同一件事靠散落的
message annotation（`StoreInjectedAnnotation` / `SkillBodyAnnotation` / `AutoCompactSummaryAnnotation` /
`UnresolvedCallsRecordAnnotation` …），每个注入点各自处理、每个消费点各自判断。两种 marker 各有
代价（我们的不占 token 但跨序列化要保真，它的进文本但跨任何边界都在）。

**`rollout-trace` 的分层**：该 crate **独占 trace schema**，热路径只依赖它的小 writer API，
**语义回放与视图投影留在 core 之外**。注意目的不同——我们的 trace 给监控看（对齐外部 spec），
它的给**重放会话**用（有 reducer、有 viewer projection），不是同一个东西。

### 8.3 趋势信号：Code Mode

`code-mode` 四个 crate：工具暴露成**代码里的命名空间**（`ToolNamespaceDescription`），模型写代码
在带执行限额的 cell 里跑（`CellId` / `StartedCell` / `ExecuteRequest` / `CellExecutionLimits`，
还有 `CodeModeExecPragma`）。**dsh 也有同款、已核实是同一概念**：其 `code-runtime` 是「执行**模型写的程序**、against host 提供的 async bindings」，配 `run_code` 工具与**在 runtime 语言里生成的 SDK**，入口是工具注册表的一个**模式开关** `tools: { mode: code }`（providers 含 worker-thread / python）——两家独立
做同一件事。收益是 N 次工具调用能写成一段带循环和中间变量的代码，省 round-trip 与 token。

⚠️ **一个降低门槛的观察**：它的 `ToolDefinition{name, tool_name, description, kind, input_schema,
output_schema}` 跟普通工具契约几乎一样，`kind` 只分 `Function` / `Freeform`。**所以支持 Code Mode
不需要改工具层**，缺的是「把 toolkit 渲染成代码命名空间 + 提供执行 cell」这一层。我们零覆盖。

### 8.4 记忆的范式对照（含我们）

| | 谁来决定记什么 | 形态 |
|---|---|---|
| crew-go | 框架机械存 | postamble 自动 `store.Put` final 结果，preamble `Search` 注入 |
| dsh | **无独立记忆层** | ⚠️ **本行原写「模型主动记 / `notes` 工具」是错的**（2026-08-21 复核订正）——dsh **没有 notes / memory 包**（`packages/` 与 45 个 subsystem 文档均零命中），那套 notes 工具是 **Codex** 的。它的答案是「**共享工作区即长期记忆**」（`docs/tool-catalog.md`：the shared workspace is long-term memory），模型用现成的读写文件工具即可 |
| Codex | **框架用 LLM 离线提炼** | 读写拆成两个 crate。写侧**从会话存档批量离线跑**（`phase1::run` 步骤：认领 eligible rollout jobs → 建 stage-1 请求上下文 → **并行**跑抽取 jobs → 出指标），输出用 **JSON schema 约束**；另有 consolidation 子 agent 整合（`build_consolidation_prompt`）与 dead memory 清理（`prune`）；大 payload 截断到有效输入窗口的 **70%、保头尾** |

三条路并不互斥。我们目前只有最机械的那条。

### 8.5 明确不该抄的三个

- **`shell-escalation` 的 per-exec fd 机制**：打补丁的 shell 每次 `exec()` 都拦，经继承的
  `CODEX_ESCALATE_SOCKET` 发 `EscalateRequest`，server 逐条决定「沙箱内跑」还是「提到 server 侧跑」；
  请求带响应用的 fd，故后代共用一个 socket 发、每请求独立 socket 收，支持并发升级。精巧，但强依赖
  本地 Unix 进程模型，而我们的 `Sandbox.Exec` 后端可能是远程 HTTP。**可学的是思路：拒绝不该是终点，
  可以变成一个升级请求**（那正是 §8.1 审批路由的上游）。
- **`agent-graph-store`**（storage-neutral parent/child topology for thread-spawned agents）：它要独立
  存拓扑，是因为子 agent 运行时 spawn、拓扑动态形成；我们静态编排，拓扑编译期已知且已体现在 graphNS
  与 cp 的 `(sid, ns)` 里。执行模型差异，非差距。
- **`rollout` 数据层**（JSONL + SQLite 索引 + 反向 JSONL 扫描 + 搜索 + GC + 持久化指标）：产品层的活。

### 8.6 顺带验证我们一个决策

`#39630` 退役 `untrusted` 审批策略时**一并删掉了内置的 known-safe 命令 allowlist**——标为
untrusted 的项目此后每条命令都要审批，除非有显式 exec policy 规则放行。我们
`WithApprovalHeuristic` 的 godoc 写明「框架**不内置** shell allowlist」。**他们内置过又删了**，
改成「给写规则的地方，不给默认清单」（`execpolicy` 是 prefix-based Starlark 规则：
`PrefixRule{pattern, decision}`，pattern 每个 token 可含 alternatives，另有 `NetworkRuleProtocol`
覆盖网络规则，支持运行时 `add_prefix_rule`）。形态不同（DSL vs Go callback），**取舍一致**。

> **本节未挖（调查期实况；除 1232 commit 变更历史与有意跳过的服务化外，其余已于 §九 补挖完成，见 §9.1 / §9.4 / §9.6 / §9.8）**：近一月 1232 个 commit 的绝大部分；plugin 体系（近期改动密集）、`app-server` /
> `exec-server` 服务化（产品架构，我们用不上，有意跳过）、Code Mode 的实际 prompt 形态、
> Guardian v2 的 `async_scorer`、`thread-store` / `connectors` / `code-mode-runtime` 内部。**不作断言。**

## 九、二轮深挖补录（2026-08-21）

> 补 §七 / §八 末尾清单里未挖的高价值项。按「缺口 / 现有功能优化 / 明确不适用」三档记，
> 不适用的也记——省得下次重新判一遍。

### 9.1 能力缺口（新发现）

| 缺口 | 它怎么做 | 我们的现状 | 判断 |
|---|---|---|---|
| **长跑工具的统一 job 协议** | dsh `ctx.jobs`：一套 owner 围栏的后台 job 契约，覆盖观察 / 取消 / 等待 / 完成通知，配 `tool-jobs` 让**模型**能查状态和取消 | 工具是同步的（`tool.Tool.Call` 返回即结束）。长跑只能阻塞，或业务自己在工具内起 goroutine 并自管生命周期 | **真缺口**。⚠️ 注意它跟 §四 defer 的「后台子 agent」**不是一回事**：这是**工具级**，不涉及 ns/cp，比 agent 级简单得多。后台能力的最小单元可能是工具而非 agent——先有它，再谈后台 agent |
| **两级审批：异步打分 → 按分数升级为同步拦截** | Codex `guardian-v2/async_scorer`：第一级用小模型**异步非阻塞**打分，评估「当前动作风险」**与「agent 的轨迹是否会导向高风险动作」**；**分数 ≥ 0.50 才让后续动作进入阻塞式全量审查** | 审批只有一档：`ApprovalAsk` 下每个业务工具都抛中断等答案 | **缺口，且解法值得学**。它解的约束是「全量同步审批太贵、全不审不安全」——用便宜的异步观察**动态调节**审批强度 |
| **授权化的会话检索** | dsh `ctx.sessionQuery`：trusted reads + 关系查询 + SQLite 全文搜索，**独立于压缩**，配 `tool-session-query` 暴露**workspace-authorized**查询给模型 | 历史层只有 `Load(HistoryKey{Owner, ConvID})`，是给 `prior` 用的框架读，无检索、无模型入口 | 缺口。但见 §9.2 第 3 条——我们的 `Owner` 维度正是现成的授权边界 |

### 9.2 现有功能的优化点（不是缺口，是我们已有的东西可以做得更好）

1. **信任边界缺一份统一声明。** Codex 的 `async_scorer/classifier_instructions.md` 把它写成声明式清单：**可信**=用户消息 / developer 消息 / `AGENTS.md` / `request_user_input` 的回答；**不可信**=工具输出、skill 与 plugin 描述、**assistant 自己的输出**；**例外**=用户显式要求 agent 遵循某不可信内容时授权可延伸过去。<br>我们**已经在做这件事**，但是分散的：`skill/sanitize.go` 净化元数据、`<sandbox_output_XXXX>` nonce 框定沙箱输出、listing 信任框定。**缺的是一份统一定义**，而且「**assistant 自己之前的输出不能作为授权依据**」这条我们大概没考虑过。
2. **持久化不变量要对「策略收紧」前向兼容**（见 §7.3-3）——`checkpointer_conformance` 与历史层的校验一旦加严，旧存档会不会被判非法。
3. **历史层的 `Owner` 维度可以直接当授权边界。** 我们做历史检索工具时不必新造授权层——`HistoryKey.Owner` 本来就是多租户隔离维度（且 `WithHistoryOwnerFunc` 能按请求覆盖）。这是**已有优势没用在这个方向**。

### 9.3 明确不适用（记下来省得重判）

| 项 | 为什么不适用 |
|---|---|
| dsh `extensions`（模型自写插件热挂载） | 模型检视自己运行的插件系统、定义动态包、在 `node:vm` 里跑、可撤回。**依赖运行时代码加载**——Go 没有对等能力（`plugin` 包限制严苛且实践中不用）。形态可记，路走不通 |
| dsh `workflow`（模型自写编排脚本） | `ctx.workflowEngine` + worker-thread 跑**模型生成的**编排脚本（另有固定策略的 `tool-ralph`）。跟我们「编排由开发者静态声明、编译期类型安全」是根本不同的取舍，不是缺口。⚠️ 但它提示了一个我们没有的维度：**编排能否运行时生成**——我们的 `GraphAgent` 要业务写 `NodeFunc`，模型不能自己组装 |
| Codex `thread-store`（单一 `ThreadId` 作唯一持久句柄） | 它统一成一个 id，实现负责解析到本地文件 / RPC / 其他后端。我们是**分层身份**（`RunID` 服务一个任务、`ConvID` 跨任务、cp 按 `(sid, ns)`），那是有意设计——它没有超步 / frontier 概念才能统一。不同取舍 |

### 9.4 压缩子系统的细节对照（自挖，源码级）

三家的摘要 prompt **同源**（都是 Claude Code 那套段落结构的变体），但详细程度差很多：

| | 摘要 prompt | 备注 |
|---|---|---|
| crew-go | **111 行**：9 段结构化 + `<analysis>`/`<summary>` 双块 CoT + 禁工具指令**前后双放** | 双放是实测出来的（注释：模型会忽略弱尾部指令去调工具） |
| dsh | 中等：8 段左右 + 单条尾部禁工具令 | `compaction-basic/src/summarizer.ts` |
| Codex | **9 行**：4 个要点 + 一句「简洁、结构化」 | `prompts/templates/compact/prompt.md`，**原样使用**（`core/src/compact.rs:118` 只在 `config.compact_prompt` 未设时用它，代码里不追加） |

**这块我们领先**，记下来免得日后看到对方「更简洁」而误判我们过度设计。

**但对方有两条指令我们缺，改 prompt 文本即可**（`agent_v2/autocompact_prompts.go`）：

1. **空段落必须写占位符、不许省**（dsh：`Write "(none)" for an empty section — never drop a section.`）。我们只有第 9 段写了「不确定就写『无明确下一步』」，**其余 8 段没有** ⟹ 模型可能直接省掉空段，下游分不清那段是「没有」还是「漏了」。
2. **禁止提及压缩本身**（dsh：`Do NOT mention this summarization request or that the context was compacted.`）。我们**完全没有** ⟹ 摘要里可能冒出「根据压缩要求…」，而它会进入后续对话，让模型看到一个它不该知道的动作。

**区域选择算法三条对得上**（dsh `region.ts:91-93`：head-anchored range + 保留计价的近期尾部 + 绝不切开 assistant tool-call/result 对 ↔ 我们压 early + 保 tail + `AdjustIndexToPreserveAPIInvariants`）。差异只在**尾部保留的单位**：它是 **token 预算**（`retainTokens`），我们是**对数**（`MaxHistoryPairs`）——按 token 更精确（一轮的长度差异很大），可作候选优化。

**一条它有、我们不需要**：摘要生成完若替换边界已变则拒绝该摘要（`region.ts:71-73`，且区分「边界变了」与「摘要/缩减失败」两种原因）。我们的 AutoCompact 在 model 节点入口**同步**跑，不存在「摘要期间对话前进」的窗口——**若将来做异步压缩，这条就得补**。

**一条待量化的取舍**：压缩调用的请求前缀。dsh 刻意**逐字复用原对话的 system prompt、tools 与前缀消息**，压缩指令是「唯一的新增输入」，为的是 **prefix-cache 对齐**（`summarizer.ts:26/74-76`）；我们复用了 system prompt（`autocompact.go:436-438`）但**不传 tools**（`Generate` 无 `WithTools`），前缀因此与原对话不同、缓存对不上。⚠️ 我们不传是**有实测理由的**（禁工具双放那条），所以这是取舍不是缺陷——但压缩本身是省 token 的动作，如果压缩调用自己不命中缓存，收益要打折。**建议先量化再定**。

### 9.6 两份 prompt 模板，对我们最有直接价值的一批（自挖，读的是实际文件）

Codex 把「权限」和「代码审查」的标准**写成 prompt 模板喂给模型**（`codex-rs/prompts/templates/`）。
这两份是本轮最有直接可用价值的东西。

#### 权限模式进 prompt——让模型预判而不是试错

`permissions/` 下按档位分文件：`approval_policy/{never, on_request, on_request_rule_request_permission, unless_trusted}.md`
+ `sandbox_mode/{read_only, workspace_write, danger_full_access}.md`（后者各一句，还带 `{{ network_access }}` 模板变量）。

- `never` 档只有一句：**「不要出于任何理由提供 `sandbox_permissions`，命令会被拒绝」**——省掉模型白费力气请求再被拒的那一轮
- `read-only` / `workspace-write` 档告诉模型**文件系统边界在哪**（workspace-write：可读、可改 cwd 与 writable_roots，**改别处要审批**）
- `on_request` 档 **57 行完整教程**：怎么请求升级（`sandbox_permissions: "require_escalated"` + `justification` 写一句问用户的话 + 可选 `prefix_rule` 建议一条可持久化规则）；命令按 shell 控制符（`|`、`&&`、`;`、`(...)`、`$(...)`）**切段各自评估**；**含重定向/替换/环境变量/通配符的命令一律不参与规则匹配**（限制已批准规则的作用范围，防绕过）；何时该升级（写受限目录 / GUI app / 因沙箱失败或疑似沙箱网络错误就带 `require_escalated` 重跑 / 破坏性动作）；**「别试图用别的工具绕过审批」**；以及 `prefix_rule` 的**禁区**——不许请求 `["python3"]`、`["python","-"]` 这类允许任意脚本的宽前缀，破坏性命令（`rm`）和用了 heredoc/herestring 的**绝不给规则**

⚠️ **对我们**：`ApprovalAuto`/`ApprovalAsk` 两档模型**完全不知道**，只能靠工具被拦才发现。后果是
`ApprovalAsk` 下模型不会主动解释理由（我们的审批问句是框架生成的干巴巴一句），也不知道哪些工具会被
heuristic 放行。**成本极低**——我们已有 `WithSystemPrompt` 与 AgentsMD 的 transient prepend，加一段
说明当前模式即可；`prompt.Template` 也现成能填动态值。

> 顺带核实掉一个我原本怀疑的安全隐患：**不成立**。我们的 `approve_always` 是**两档分开**的——
> `ApprovalOptionApproveAlways`（这件事）与 `ApprovalOptionApproveAlwaysTool`（这个工具的任何调用），
> godoc 写明拆开的理由：「用户心里的『一直允许』通常指**这件事**而不是**这个能力**，混成一个答案框架
> 只能猜，而猜宽了就是把用户没打算给的权限给出去」。这块设计是对的，不动。

#### `review/rubric.md`（95 行）——直击我们 audit 的老毛病

它把「什么才算一条 finding」写成**事前判据 8 条**，其中两条正是治我们 67% 误判率的：

- **「不依赖关于代码库或作者意图的未声明假设」**——正是治「看到代码无显式防御就报缺陷」
- **「不能只推测某处改动会影响别处；要算 bug，必须指出可证明受影响的具体位置」**

另外 6 条我们的规约里也没有明文：有实质影响（准确性/性能/安全/可维护性）；**离散且可行动**（不是「代码库的一般性问题」或多个问题的组合）；**修它不能要求超出代码库现有严格度**；**是本次改动引入的**（pre-existing 不报）；**原作者知道了大概会修**；**明显是作者有意的改动就不算**。

「报多少条」也有明文：**输出所有「原作者知道了会修」的 finding；如果没有任何一条是人会明确想看到并修的，宁可零 finding；但符合标准的要报全，不要报到第一条就停。**

优先级判据比我们的 HIGH/MED/LOW 精确：**`[P0]` 只用于「不依赖任何输入假设」的普遍问题**（P1 紧急 / P2 常规 / P3 nice to have），且要求输出 numeric priority 字段。

评论写法 8 条同样可用：说清为什么是 bug、**不得夸大严重度**、至多一段、代码片段不超 3 行、**必须写明触发所需的场景/环境/输入并立刻表明严重度取决于它们**、语气 matter-of-fact 不指责也不过度正面、**避免「Great job…」这类奉承**、行范围**别超过 5–10 行**要挑最能定位问题的子范围。

还有一条「规则归属」值得学：finding 只有在**项目指令文件提供了超出通用正确性建议的仓库特定内容**时才算「规则支撑」，且要引用**最小支撑行范围**、**不许伪造引用**，同时**不得因为存在规则文件就漏报普通 finding 或编造 finding**。

⚠️ **这批直接对应 CLAUDE.md「交叉审计流程」的短板**：我们靠的是事后统计（「命中率 33-43%，所以要交叉验证」），它给的是**事前判据**。是否把这 8 条写进我们的 audit 规约，值得单独拍板。

### 9.5 Codex 侧实现抽查（补三轮 audit 欠的对称性）

前几轮 dsh 侧抽查了 3 处、Codex 侧只 1 处。本轮补到 5 处，**结论全部属实**：

| 我的原判断 | 验证 |
|---|---|
| `context-fragments` 有 marker 用于事后识别注入内容 | ✓ `trait ContextualUserFragment` 带 `render()` + `matches_marked_text(start, end, text)`；且 `matches_text` 是**关联函数**（不拿实例就能判一段文本是否由该类型注入，比原文写的更具体） |
| `rollout-trace` 热路径只依赖小 writer API | ✓ `TraceWriter` 仅 `create` / `write_json_payload` / `append` / `append_with_context` 四个方法 |
| `agent-graph-store` 的 spawn 边有状态 | ✓ `ThreadSpawnEdgeStatus{Open, Closed}` 两态（比原文暗示的更简单） |
| `memories` 用 LLM 分阶段离线提炼 | ✓（上轮已验，见 §8.4） |
| Codex 压缩 prompt 原样使用不追加 | ✓ `core/src/compact.rs:118` |

### 9.7 dsh 剩余四组（自挖；两组价值低、两组有可用的设计原则）

| 组 | 结论 |
|---|---|
| `identity` | **价值低**。只有 `anonymous-user-id`（一个匿名 Harness-home 关联 id，用于遥测/反馈/请求），README 明说「**不代表已认证账户**」。我们是 SDK，身份归业务，无对应需求 |
| `attachment` | **价值低**。`ctx.attachments` seam + content-addressed 本地存储。一条设计可记：**未发送的浏览器草稿刻意不在此能力内**，字节只在「用户提交 prompt」或「provider 适配器提交结构化模型输出」时才落盘——我们没有 UI/草稿概念 |
| `feedback` | **设计原则有价值**：两个**刻意分开**的契约——`command-feedback` 是**不可变**的 remark 进 canonical session log（log-only），`message-feedback` 是**可编辑**的 per-message 评分/备注存**本地 sidecar**（既不是 session event 也不是 projection）。关键那句：「**Neither form enters the model conversation**」。⟹ **评分/标注这类「关于对话的元数据」不该进对话流**。⚠️ 对我们的提醒：我们的历史层是 append-only 消息流，业务要加评分天然会想塞 annotation，而 annotation 在某些路径**会进 prompt**（`HistoryAgentDisplayAnnotation` 的显示名跨归属改写就会）。真做 feedback 时要明确它不进模型视图 |
| `lsp` | **设计原则有价值**（见下） |

**`lsp` 的 seam 设计**：只暴露**恰好四个语义操作**（`goToDefinition` / `findReferences` /
`goToImplementation` / `hover`），**刻意不给通用 JSON-RPC 逃生舱**。理由原文：换 provider 不改变
模型询问导航的方式，且「**no protocol payload or unreviewed mutation reaches the model contract**」。
配套一条：**provider 注册的是「能力」不是「工具」**，`tool-lsp` 是模型面名称、schema、prompt 指引与
呈现的**唯一 owner**。

⚠️ **这条原则引出一个自查，结果是我们有处不一致**：

- skill 侧**有**不可信源注入防护——`skill/sanitize.go` 的 `sanitizeMetadata` / `sanitizeDescriptor`，
  配 listing 信任框定，明确是「防第三方 / marketplace skill 包经描述文本注入」
- MCP 侧**没有**——`mcp/client.go:96` 的 `sanitize` 只把工具名转义成合法标识符
  （`[A-Za-z0-9_]`，非 ASCII 用 `_%x` 防碰撞），那是**命名规范化不是注入防护**；工具的
  **`description` 未经任何清洗**就进 LLM 的工具列表

两者威胁模型相同（第三方提供的文本进模型可见面），处理却不同。**但这可能是有意的**——MCP server
是业务**主动配置连接**的，信任级或许本就高于下载来的 skill 包。⚠️ **本条是「不一致」的事实陈述，
不是断言缺陷**；要不要对齐需拍板。

> **一处更正**：`connectors` 在 dsh 里**不存在**（我此前把它列进 dsh 的未挖清单，写错了）——
> 那是 Codex 的 crate。

### 9.8 Codex 的 plugin / Code Mode 运行时 / connectors（自挖，收尾）

**plugin 体系 —— 产品分发层，不适用，但它的单元划分值得记。** manifest
（`core-plugins/src/agent_plugin_manifest.rs:32`）**只有元数据**：`$schema` / name / version /
description / author / homepage / repository / license / keywords，**没有任何能力声明字段**。
能力面在发现结构里（`discoverable.rs:59`）：`has_skills` + `mcp_server_names` +
`app_connector_ids` ⟹ **一个 plugin = skills + MCP servers + connectors 的打包分发单元**，
另配 curated 清单与「建议安装」入口（`list_tool_suggest_discoverable_plugins`）。
⟹ 对我们是产品分发层（Go SDK 里业务自己组装），**但记一条**：skill / MCP / connector 这三者
在产品层被当作**同一类可插拔能力包**——我们已有前两者，只是没有打包这一层。

**Code Mode 运行时 —— 实现路径清楚了，且成本比想象低。** 跑在 **V8** 里
（`code-mode-runtime/src/runtime/globals.rs:15` 的 `install_globals`）：

1. **先删危险全局**：`console` / `Atomics` / `SharedArrayBuffer` / `WebAssembly`
   （⚠️ 注意是**主动从全局对象删**，不是只靠外层沙箱）
2. 注入 **`tools` 对象**（工具即方法，命名空间就是 `tools.<name>`）
3. 注入 **`ALL_TOOLS`**（可枚举清单，模型不用记）
4. 注入受控 `setTimeout` / `clearTimeout`（`timers.rs`）与 `text`
5. 另有 `module_loader.rs`（受控模块加载）

⟹ **对我们的成本判断**：§8.3 已确认它的 `ToolDefinition` 跟普通工具契约几乎一样，**不用改工具层**；
真正要加的是「脚本引擎 + 工具绑定 + 执行限额」这一层。Go 侧的引擎选择（goja / v8go / Starlark /
WASM）是主要决策点，而**工具绑定本身不难**——我们的 `tool.Toolkit` 已有工具列表与 JSON schema，
渲染成一个对象命名空间 + 回调进 Go 即可。**沙箱要照它那样「主动删能力」而不是只靠外层隔离。**

**connectors —— 纯产品层，不适用。** `AppInfo` / `AppMetadata` / `AppBranding` / `AppReview` /
`AppScreenshot` / `AppToolPolicy` / `ConnectorDirectoryCacheContext`（`connectors/src/`）：这是
**应用目录 / 市场**（品牌、评价、截图、按 app 的工具策略、目录缓存）。

### 9.9 五个已知有价值项的源码级深挖（含成本量级）

> 前面几节判定「有价值」的项，这里挖到实现层给**成本量级**。⚠️ 本节结论经交叉验证：关于
> crew-go 自身的断言我逐条 grep 核过，一条需要精确化（见 A-1 脚注）。

#### A. 能力缺口

**A-1 每次工具调用的协作式超时 —— 性价比最高，且谁都不碰。**
对方全部实现 81 行：从工具自己声明的预算派生一个「融合了调用方 abort 与本层定时器」的信号，
跑完再换回去，**只有本层定时器赢了**才把结果换成结构化超时。唯一非平凡处是那个「谁赢了」的
判定——Go 侧有 1:1 等价物：`context.WithTimeoutCause` + `context.Cause`（Cause 返最内层先触发
的那个，语义一致）。<br>⚠️ **必须照抄的约束**：这是**协作式不是硬杀**——只有真把 ctx 往下透传
的工具才该声明预算；Go 里阻塞在 syscall 上的工具连「注意到」都做不到，**godoc 必须写死这条**，
否则业务会以为声明了就有保护。<br>**成本：50–100 行 + 测试**，落点在 `makeToolsFn` 的 `runOne`
内（与 approval 拦截同处，那里已有 `approvalMu` 处理 fanout 并发的先例），不动任何公开接口。

> ⚠️ **一处措辞精确化**（原报告说「框架代码零命中、全在 example」）：实际 `observability/` 有 3 处
> `context.WithTimeout`，但那是 trace 上报的 **RPC 超时**。**实质结论成立**——`compose_v2` /
> `agent_v2` / `tool` 这条 agent 执行路径上确实**一个 deadline 都不 arm**，今天唯一的时间约束是
> 调用方给整个 `Run` 传的粗粒度 ctx：它均匀作用于底下所有东西、无 per-tool 粒度，且触发后
> `errors.Is(ctx.Err(), DeadlineExceeded)` **分不出是哪一层的 deadline**。

**A-2 重复工具调用检测 —— 成本小，但有个决策要先拍。**
检测本身平凡（参数深度 key-sort 后序列化作链 key，与上次相同则计数、不同则重置，命中阈值发提醒）。
值得抄的是几条**非显然判据**，每条对应一个真实绕过路径：**未跟踪的调用对链「透明」**（被排除的
工具既不计数也不重置，否则「记账工具夹在循环里就能洗白」）；**计数挂在 post-execute，所以被拒绝
的调用也计数**（「模型对着一个被拒的调用猛捶，正是最该打断的循环」——这条**直接对上我们的
approval Deny 路径**：今天业务连拒 20 次同一个工具，框架毫无反应）；阈值分两档（先泛化 nudge 再
给含工具名/次数/参数的详版）；**用户插话重置链**；参数预览截断但 key 用全串。<br>**成本：约
250–300 行 + 测试**（依据：对方实现 233 行、零依赖；Go 侧无 WeakMap 需自管状态生命周期故上浮），状态可挂 `RunSession` 的 KV（fanout 并发要像 `approvalMu` 那样上锁）。
⚠️ **先拍的决策**：提醒是一条**合成 user 消息**，会流进 `HistoryStore` 与 4 层压缩——打哪个
annotation、进不进历史。**先例现成**：inline skill body 的 `[system_reminder]` + `DerivedInputAnnotation`
就是同一个形状。另注：**这个功能做不成 hook**（`OnToolEnd` 只对 `Result.*` 是消费档，塞不进新消息），
只能是框架级特性。

**A-3 后台 job —— 一半便宜一半是架构级，可以只取便宜的一半。**
*便宜的一半（registry）*：生产者契约干净——`run()` 同步返回 `cancel`（同步+幂等+最终 settle）、
`done`（**「在生产者释放资源之后**才 resolve，不只是活干完」）、可选的消费式 `readOutput`；Go 直译
就是 `CancelFunc` + `<-chan Outcome` + `ReadOutput()`。**取消传播不在 registry 里**（bash 生产者的
cancel 就是 kill 进程）。围栏就是 session id 相等比较，job id 故意可预测（「靠归属授权而非靠保密」）。
**崩溃/跨进程明确不做**——所以我们对齐**不需要**解决持久化。两条值得抄的不变量：**没有 controller
服务这个 owner 时直接拒绝 start**（不允许启动没人能观察/停止的后台任务）；teardown 时**先标已报告
再 cancel**（owner 都要销毁了没人会读通知）。<br>*贵的一半（通知怎么回到模型）*：按 owner 忙/闲
分两条道——忙就塞进下一 step 的 inbox **不唤醒**（「几个 job 一起结算只花一个 step 而不是各一个
turn」），闲就**开新 turn 唤醒**。⚠️ **映射到我们**：`inject` 那条**很便宜**——BasicAgent 本就是
四节点 + 循环回边，在 modelFn 入口（与 4 层压缩同挂点）drain 待发通知并 prepend 即可，且我们已有
「外部 goroutine 异步发信号、超步边界消费」的先例（`SoftCancelSignal`）；**`followup` 那条做不了**
——`Agent.Run` 是 call-and-return，返回后没有常驻对象可被唤醒，要做等于换掉执行模型。**但这个洞
没那么严重**：我们的 Run 由调用方驱动，调用方本就知道何时再发起；而对方自己给唤醒加了预算上限，
理由是**这条链会自激**（被唤醒的 turn 可能又启动一个 job，其完成再唤醒它），且只有**真人输入**
才回填预算。**成本：registry + 三个工具 + inject 道 ≈ 中等（约 800–1200 行）**（⚠️ 这是**估算**：对方该组 `src/` 非测试合计约 3700 行，但含 Web/UI 与跨进程门面；我们只取 registry + 工具 + inject 那一部分，故估在其 1/3 上下——**动手前应先按实际取舍范围重估**），核心难点不是并发
而是**生命周期归属**——owner 销毁要 cancel 并 await 所有 owned job，而我们的 `RunSession` **没有
disposal 钩子**，得先长一个。

#### B. 现有功能的优化点

**B-1 决策做成有序枚举、冲突取 max。** 对方整个策略引擎的冲突裁决就是一行
`matched.map(decision).max()`，配合枚举序 `Allow < Prompt < Forbidden` ⟹ **最严格者胜、无条件**，
没有最长前缀优先、没有规则顺序依赖；一条管道拆成的多段也走同一个 max（**管道的安全级别 = 其中
最不安全的一段**）。<br>我们今天 `approve_always` 规则集与 heuristic 之间**没有定义好的裁决顺序**
（实现上规则命中就放行，即隐含地压过 heuristic）。改成有序枚举 + 多来源取 max 是很小的重构，但把
「以后再加一个来源会不会出事」这个问题**永久关掉**。成本小，但动的是已导出符号语义，要走
CLAUDE.md 那条 checklist（grep 全调用点 + `go doc` 看一眼）。

**B-2 规则带 `justification`，且加载期自测。** 每条规则可带理由，用于提问文案或拒绝消息（禁止类
规则的理由里直接写替代方案）——我们的 Ask 中断今天只给工具名 + 参数，**用户不知道为什么在问他**，
业务 heuristic 也没地方解释自己，加个 `Reason string` 即可。另一条近乎免费：**规则文件里的正例/
反例在加载期验证**（正例必须命中、反例必须全不命中，否则加载失败）——**规则即配置、配置自带单测**。

**B-3 `HistoryStore` 缺「读尾巴」。** 对方有个反向扫描器（从 EOF 往回按块读、按行切、坏记录跳过
而非报错、可扫一个**冻结前缀**以避开并发追加），两个消费者说明用途：在 append-only 日志上实现
可变 KV（改名=再 append 一条，读时从尾往前第一个命中即胜，**零重写**）；以及**只为拿最后一条的
序号**而不读整个文件（长会话续跑从 O(n) 变 O(1)）。<br>我们的 `HistoryStore` 只有 `Append` /
`Load`（已核实），而 `Load` 的契约是「返回该会话**全部**消息」——一个一万条消息的会话**每次 Run
都要全量拉一遍**，然后 4 层压缩再把大部分扔掉。这是会随使用时长线性恶化的真实成本。⚠️ 但加方法
= **破 `history_conformance` 契约**、所有业务自研 backend 都要跟。**建议先量再改**。

#### C. 明确不适用

**C-1 前缀规则本体 —— 缺一个我们不该引入的前置依赖，本次调研支持维持现状。**
它的匹配函数本身平凡（pattern 必须是 argv 前缀、逐 token 精确相等、无通配无 glob 不解析 flag），
关键在于它吃的是**已 tokenize 好的 argv**——要对 `bash -lc "..."` 有意义，必须先用 **tree-sitter-bash**
把脚本拆成 plain command，拆不动就整条 fallback 成「不透明命令」⟹ 匹配不上任何规则 ⟹ 落启发式 ⟹
大概率提问（**fail-safe 是靠这个 fallback 撑住的**）。它源码里还写着一句给自己的警告：那个更宽松的
解析器「适合识别危险的字面命令，但**绝不能用来证明一个命令是安全的**」。<br>我们的 sandbox 只有
一个 `Exec(ctx, cmd)`、cmd 是 shell 字符串——**在字符串上做前缀匹配不只是没用，是主动有害**
（`ls; rm -rf /` 的前缀是 `ls`）。要做就得引入 shell 解析器，而解析器与真实 shell 的语义差异本身
就是攻击面。⟹ **CLAUDE.md「不内置 shell allowlist、规则内容业务自负」是正确判断。**

**C-2 存档压缩与全文检索 —— backend 的职责不是框架的。** 压缩那套是一整套运维工程（只压 7 天
未动的、进程级文件锁、并发上限、worker 时限、透明读、append 前先解压）换本地磁盘小一点；检索是
shell out 到 ripgrep（不在则退回进程内扫描），有个漂亮技巧：**把待搜串先 JSON 转义再去 grep 原始
JSONL 字节**，省掉「为了搜索而解析一万个文件」。我们的 history backend 是业务自己的 Redis/PG，
这些是 backend 的事。<br>⚠️ 顺带一个价差巨大的对照，将来真要做会话检索时有用：一家是 **350 行 +
shell out 到 ripgrep**，另一家是 **1783 行 SQLite FTS5 派生索引**（口径：`src/` 非测试）；后者的收获点不是 FTS 本身，而是
它把索引当**一次性派生读模型**——schema 版本一变就**原地重建**，另加守卫防止把别人的库当成自己的
索引清掉。

**C-3 授权模型 —— 我们现有做法更好，这条要反着记。**
对方所谓「工作区授权」落到代码就是 **`cwd` 字符串精确相等**，且它自己承认「符号链接等价的路径
不共享权限」；更关键的是**引擎层根本没有授权**（其文档 Known Limitations 第一条：「无调用方授权
——这是全上下文可信的基础设施」），授权整个是工具层贴上去的。<br>我们的 `HistoryKey{Owner, ConvID}`
且 `Owner` 必填无默认，godoc 里那句「ConvID 来自 wire，是**续接标识非授权令牌**——猜中别人的就能
拉到别人的完整对话」——**是一个比 cwd 相等更明确、更早、更难绕过的隔离维**，因为它在**存储 key
里**，不是某个可选工具的前置检查。**这半边不是缺口。**<br>倒是三个细节值得单独记（与授权模型
无关的安全设计）：未授权的族谱节点被替换成**不含任何 id 的 marker**（连「存在这么个东西」都不
泄露）；后端错误过一层**封闭枚举消毒器**再给模型（防路径/SQL/他人 id 随错误信息带出）；当前会话
搜自己历史时**停在发起本次调用的那一步之前**（防自指）。<br>至于「关系查询」，其中的**事件替换链**
在我们这里**无从谈起**——history 是扁平 append-only 列表，没有「这条取代了那条」的边；要做得先造
一整套关系模型，**不建议**。

#### 一条前瞻警告（不是现在的缺陷，但动手前必读）

对方有一个**一次性、带防重跑标记的迁移**，干的事是从用户已存的策略文件里**逐行删掉**形如
「永远允许 `bash -lc`」的规则，并加一张黑名单让界面再也不提议它们（`bash` / `sh -c` / `zsh` /
`cmd /c` / `bun -e` / `Rscript` …）。翻译过来：**他们让用户把「永久授权任意代码执行」写进了持久
策略文件，然后不得不发版把这些行从用户机器上删掉。**

⚠️ **我们今天之所以安全，恰恰因为 `approve_always` 存在 `RunSession` values 里、会话结束就没**
（已核实）。若将来要做成跨进程持久（合理的产品需求），**两条硬约束是现成的教训**：(a) 规则 key
**绝不能是一个 shell 解释器前缀**——我们的工具级 wildcard 档按**工具名**匹配（已核实
`tc.Function.Name + wildcard`），若那个工具是 `sandbox_bash`（任意命令入口），**持久化后就等价于
永久放行任意命令**；(b) **从第一天就要有迁移路径**，因为你一定会需要撤销已经发出去的授权。

> **未挖清单：本轮已清空。** §七 / §八 末尾列的项已全部挖完（dsh 的 extensions / session-query /
> jobs / workflow / identity / feedback / attachment / lsp，Codex 的 Guardian async_scorer /
> thread-store / plugin 体系 / Code Mode 运行时 / connectors / prompts 模板），另补了压缩子系统的
> 源码级对照与 5 处实现抽查。
>
> **仍未覆盖**（如实标注，不作断言）：Codex 近一月 1232 个 commit 的绝大部分变更历史；两边
> 剩余 crate/包的内部实现（本轮深挖的是列出的那些，其余仍只有组 README 级认知）；`app-server` /
> `exec-server` 服务化（**有意跳过**，产品架构与 SDK 定位无关）。

---

> **§九 的 audit 状态（2026-08-21，三轮）**：① **示例可执行性**——抓到一条硬的：上一版修过的
> 审批示例在 fanout 场景**仍然不 work**（只改了取值、没改前置条件），已按 query 粒度重写并实测
> 单/双工具两种场景跑通；② **外部引用抽查**——四条 file:line 全对，另补验了一个原报告没提但结论
> 依赖的前提（那个枚举确实 derive 了 `Ord`，否则取 max 的裁决不成立）；三处数字补上口径与依据
> （其中一处我差点误判成「数字不实」，实为我的统计范围错）；③ **正文断言逐条核**——提取 §九 里
> 关于本仓的 17 条具体技术断言，抽验 8 条**全部成立**（`RunSession` 确无 disposal 钩子 /
> `WithHistoryOwnerFunc` 确按 ctx 取 / `GraphAgent` 确是包业务建好的图 / `autocompact.go:436-438`
> 行号精确 / `OnToolEnd` 确只对 `Result.*` 是消费档 / 软取消确是任意 goroutine 触发+超步边界停 /
> inline skill body 确用 `[system_reminder]` + `DerivedInputAnnotation` / 显示名确会经转交视图的
> `For context: [X] said:` 进模型 prompt）。
>
> ④ **外部行为断言核实现**（第四轮，补上一轮所欠）——挑五条只凭 README / 头注释下过结论的去读
> 实现，**全部成立，且其中三条实现比文档更严格**：语言服务的操作集是**封闭联合类型**（恰好四个，
> 结果也是封闭 union，全文件无 raw request / jsonrpc 逃生舱）；压缩的区域选择不是「尽量不切开
> 工具调用配对」而是**边界不平衡直接抛错**；取消传播确实不在 registry 里（生产者自己 kill 进程）；
> 重复检测「未跟踪调用对链透明」逐字对上实现注释；todo 拒绝非 agent 调用者有明确的抛错点。
>
> **仍未审**：§9.9 里那些**判断性结论**（「最值得先做」「成本量级」）——那类无法靠 grep 证伪，
> 只能等真动手时被现实检验；以及两家源码中未被本篇引用的绝大部分。**不承诺已扫干净。**

> **本篇 audit 状态**：① 自查（发现 Claude Code URL 未复核 → 已解决）；② `claude-code-guide` WebFetch 复核 11 条 URL（9 精确 + 2 锚点已修，能力描述 100% 相符，agentskills.io 血缘官方原文佐证）；③ 独立 Explore agent 审新增内容（MAF provider 表 grep / crew-go 缺口 grep / 三档逻辑 / 内部一致性 / 查漏）——0 真问题。事实多继承自主纲（3 轮审过）。**不承诺「已扫干净」**，并入最终跨文档批量 audit 再过一遍。

---

## §10 便签/记忆的三道设计题——八仓源码实证（2026-08-26）

> **缘起**：`doc/v2/self-tracking.md` 的设计方案卡在三道二选一（ns 分区 / 索引注入位置 /
> 租户维折入方式）。三个独立 agent 并发挖本地八仓源码，结论**推翻了其中两道题的前提**。
> 决策正文（匿名化）在 [self-tracking.md §3.2e](../../v2/self-tracking.md)，本节记厂商级细节。
>
> **状态：调研结论，未拍板。**

### §10.1 可改写性与作用域绑定（决策 1）

**Codex 有两套便签，工具集不同**——这是本次最有解释力的一条：

| | 路径 | 工具 | 可改写 |
|---|---|---|---|
| **线程便签** `ext/history-notes` | **写**限当前线程、**读可跨线程**——原文（`models.json:82`）：「Relative note paths belong to the current thread; **absolute paths may read other threads' notes, but** writes are limited to the current thread」⚠️ **初稿引用省掉了中间从句且未标省略，而那正是反证** | `list_files_by_prefix` / `read_file` / `search_contents` / **`append_to_file`** / **`write_file`** | ✅ |
| **全局记忆** `ext/memories` | `~/.codex/memories`，跨线程 | **只有** `add_ad_hoc_note` / `list` / `read` / `search`（`lib.rs:19-22` 常量表实测穷举）| ❌ 无 edit / delete |

全局侧撞名 **O_EXCL 硬失败**（`local/ad_hoc_note.rs:28-36` `create_new(true)` → `AdHocNoteAlreadyExists`）；
「忘记 / 更新」也是**再写一条新 note**，真正 mutation 由**持全局锁的 Phase-2 consolidation agent** 串行做。

⚠️ **「读合并 → 改 → 写回来源那格」——初稿写「七仓零命中」，第 8 轮独立找反例时推翻。**
**反例：CrewAI 的 unified memory 写入流水线**（三步全对得上，入口还是模型可调的工具）：
`RememberTool` → `Memory.remember(content)` **不传 scope** → `EncodingFlow.parallel_find_similar`
（`encoding_flow.py:165-183`，无 scope 时 `scope_prefix=None`；⚠️ **限定**：还要求 `root_scope` 也为空，crew 用法下 `root_scope=/crew/{name}` ⟹ 检索被限在该子树，**「跨全部 scope」这个措辞在最常见场景下不成立**——但**不影响**「写回照抄 `existing.scope`」这个核心反例）→
`parallel_analyze` 交 LLM 出 `ConsolidationPlan`（schema 定义 `analyze.py:103-140`；真正交 LLM 在 `:358`）→ 写回
`encoding_flow.py:452-465` 的 **`scope=existing.scope`**。工具 docstring 亦明写
「**The system infers scope**」（`tools/memory_tools.py:84`）。
⚠️ **但它不支持「模型读合并后自己改」**——写回决策由框架 + 分析 LLM 接管（按主键 update），
模型连键都不给。
✅ **加上限定词后成立**：在「作用域 = 带优先级的**配置覆盖层**」范围内八仓零命中——多层配置
**一律硬编码写用户层**（codex `marketplace_edit.rs:27-36` 函数名就叫 `record_user_marketplace`；
dsh settings「merges a sparse patch over the **user section only** (never into `base`)」），
且 dsh 把「写回来源层」**显式否决并存档**（`2026-07-28-user-settings-seam.md:27`，理由：会把用户
偏好绑死在某一份组合文件上，而用户层必须跨模板升级存活）。

其余各家确为**拒绝合流**：

- CrewAI `MemorySlice`（`memory/memory_scope.py:227-236`）docstring 逐字：
  「recall searches all, **remember is a no-op when read_only**」；`read_only` **默认 True**，
  非只读时 `scope: str` 是**必填位置参数无默认**。配套 `create_memory_tools` 在 read_only 时
  **根本不注册 RememberTool**。
- claude-agent-sdk 权限层：读三层合并（`setting_sources: user/project/local`），写有一个显式的「落哪一层」枚举 `PermissionUpdateDestination`
  （`types.py:105-107`——⚠️ **初稿写「必须」且行号指到 `ToolsPreset` 上，双错已订正**：该字段是
  `PermissionUpdateDestination | None = None`，`to_dict` 里 `if self.destination is not None` 才带上，
  **可选不是必填**）。

其余形状：MAF `FileMemoryProvider`（`scope` 参数，默认 `session_id`，读写对称）vs 并排的
`FileAccessProvider`（**没有 scope 旋钮**＝设计上共享）；eino 维度完全外包；
**CrewAI 由 LLM 推断 scope**（模型连键都不给）。

⚠️ **两条硬规律**：(1) **没有一家靠「相信模型不会撞名」**——框架强塞时间/单调/随机分量 + O_EXCL
兜底；模型自由命名的两家，撞名语义**本来就是「覆盖/合并」**（那个空间只有一个写入者）。
(2) **没有一家从「分区」退回「不分区」**，movement 全是往更细分区走。

### §10.2 开放式便签「不注入索引」是主流（决策 2）

**分野不在厂商，在被索引的东西有没有界**：

| 类别 | 做法 | 实例 |
|---|---|---|
| **开放式便签**（无上界、模型自写）| **不注入**，只给 list/search/read | Codex notes、MAF `FileAccessProvider`、LangGraph store、CrewAI |
| **有界外部预定义集合** | 注入 name+description + 预算 + 降级 | Codex skills（context 2%、上限 10k token、**四级降级**）、dsh skills、MAF `MemoryContextProvider` |

**实测**：Codex 的 `prompts/templates/` + `core/templates/` 里 **`notes` 零命中**。模型靠工具可用性 +
`models.json` 静态 guidance 知道有便签。

**新鲜度不是靠每轮重注解决的**——两个独立 CLI harness 各自收敛到同一方案：**按内容 digest 差分注入
到持久历史 + 检测被压缩吃掉后重发**，理由都是 **KV cache 前缀复用**：

- dsh（`tool-skill/src/index.ts:213-250`）：每 `agent/pre-step` 算 digest，跟「历史里最后一条**仍可见**
  的 catalog」比；不一致才追加**整表替换**消息。**digest 只覆盖 name/description，正文改动不触发**。
  设计文档给了两头的理由：「startup-only catalog leaves the model **able to call deleted names**」
  vs「每次正文编辑都当 revision 会导致 **unnecessary prompt churn**」。
- Codex world-state `render_diff` + retained-fragment matcher（`world_state/mod.rs:402-452`）：
  按 body 文本在保留历史里匹配，匹配不到就当从没注入过、重新渲染。

⚠️ **「压缩豁免标记 / pin」——初稿写「七仓零命中」，第 8 轮推翻，实际要拆三类**：

| 豁免形态 | 家数 | 证据（举例）|
|---|---|---|
| **按角色**（永远保留 system / user）| **≥5** | MAF `_compaction.py:667/703-714` `preserve_system=True`；eino `summarization.go:506-517`；CrewAI `agent_utils.py:946-947`；**openai-agents 更强**——把**全部 user 消息**排除出压缩候选（`openai_responses_compaction_session.py:30-51`）；codex `compact_remote_v2.rs:512-544` |
| **按工具名白名单**（只有名单内的工具输出可被裁）| 1 | openai-agents `extensions/tool_output_trimmer.py:60`（字段声明 `:69`）的 `trimmable_tools`。⭐ **与 crew-go 的 `CompactableTools` 是同一个形状** |
| **按标记**（打标 → 压缩读它并跳过）| 1 | codex `Feature::RetainClientDeveloperMessages`：`history/src/lib.rs:44-49` 的 `client_authored`，`compact_remote_v2.rs:471-479` 尊重它（而 `should_keep_compacted_history_item` 对 developer 角色一律返 false ⟹ **留下来完全靠那个标记**）|

⚠️ **按标记那条有三个限定**：`features/src/lib.rs:1532-1537` **默认关**（`UnderDevelopment`）；
载体固定（只有 `developer` 角色**且**经 app-server 客户端注入的条目拿得到）；标记是**框架按来源
自动盖的 provenance**，不是调用方设的 `pinned: true`。
⟹ **「给任意消息打 pin」确实零命中**，这半站得住。
另有第四类**结构性豁免**（干脆不放进可压缩那层——dsh 的压缩事件仅写日志、有意不扩展其 surface 事件类型，故结构上到不了模型；⚠️ 此条形状有支撑但**未逐条锚定行号**）；以及 MAF 把「按 `source_id` 豁免」
**写进 ADR 却零落地**（`Message` 上根本没有 `source_id` 字段）。

「**检测缺失后重发**」确为主流。

MAF 反而把压缩当成**写便签的理由**写进 instruction（`_file_memory.py:72-75`，属 `FileMemoryProvider`）。

⚠️ **初稿把 MAF 的两个 provider 混成了一个（已拆）**：紧随其后那三条——写工具当场 `rebuild_index()`
并把**新记录**返回 tool_result（重建 `_memory.py:1225-1230`、返记录 `:1231`）、`list_memory_topics` **现算现返**
（`:1196-1205`）、instruction 里那句「**do not assume every topic file was loaded**」（`:1289`）
——**全部属于另一个 `MemoryContextProvider`**。`FileMemoryProvider` 的 write 也当场
`_rebuild_index()`（`_file_memory.py:335`），但只返 `"File 'x' written."`、**不返记录**。

⚠️ **§10.2 的「开放式便签不注入」有一个同仓反例，初稿略掉了（已补）**：MAF 的
**`FileMemoryProvider` 正是开放式便签**（模型自由命名、无上界），却**每次 run 都注入 `memories.md`
索引**——godoc 逐字「维护一份 `memories.md` 索引并**注入 agent 的上下文**，好让模型知道已经有哪些
记忆」（`_file_memory.py:9-10`），注入代码 `:494-517`，条数上限 `_MAX_INDEX_ENTRIES = 50`（`:79`）。
本节表格里放的是并排的 `FileAccessProvider`（那个确实不注入）——**选样没错，但同一家的反例不该略**。
⟹ 「多数不注入」成立，「**一律**不注入」**不成立**。

**无索引的代价，CrewAI 写在自己 prompt 里**（`translations/en.json` `slices.memory`）：检索是
「automatic selection and **may be INCOMPLETE**」，遇到 how many / list all 类问题**必须**换多个
query 反复搜。⚠️ **我们比它多一件 `list` 工具**，代价小得多。

**第三档形态（Codex 独有）**：注入的不是「有什么」而是「**怎么问**」——`<context_window>` 块只放
agent 名 + first/current/**previous** window id（`token_budget_context.rs:55-70`），
「previous 存在」＝发生过 reset ⟹ guidance 教它去读 checkpoint + 用 `history` 工具找回。
（⚠️ 初稿写「**只放**」这三样不准——同一个 body 还会可选追加一条 MCP 结果字符串；**结论不受影响**。）
**成本恒定（三行 UUID），与便签数量无关**；代价是模型必须真的记得去调。

### §10.3 身份是平台层才被发明出来的（决策 3）

| 层 | 谁给身份 | 框架做什么 | 实例 |
|---|---|---|---|
| **库层（8 家）** | 调用方 | ⚠️ **初稿写「不认识身份」是错的**（第 8 轮推翻）——**有 typed 身份槽，但存储层不消费**：LangGraph `runtime.py:60-77` 的 `ServerInfo.user: BaseUser \| None`，**解析在 OSS 库里**（`pregel/main.py:4322` 读 `configurable["langgraph_auth_user"]`），但**没有存储代码读它**（纯观测面）；CrewAI `memory/types.py:60-73` 的 `source` + `private`，**框架自己执行可见性**（`unified_memory.py:746` `if not include_private: ... r.source == source`）——⚠️ 但 `source` **调用方自报、无认证**，且 `include_private=True` 可整体绕过 ⟹ 是「身份维度」不是「租户隔离」。另 `langgraph_sdk.auth`（pip 库）整套 `BaseUser`/`AuthContext`/`StoreGet\|Put\|Search` handler 也在库里 | LangGraph `BaseStore`、eino `CheckPointStore`、CrewAI `MemoryScope`、OpenAI `Session` |
| **存储产品层** | 调用方**每次调用传** | 身份是**一等必填参数**，不给报错 | mem0 |
| **平台 / 宿主层** | **平台注入** | **自动折进存储路径** | MAF hosting、LangGraph Platform `@auth.on.store` |

- **LangGraph `BaseStore` 对 `user_id` / `owner` / `tenant` 零命中**（实测）。官方立场原文在自己的
  测试注释里（`checkpoint-postgres/tests/test_store.py:360`）：「**Callers isolate tenants by
  namespace**」——而那句话的语境正是代价的证明：因为调用方拿 namespace 当租户边界，**前缀匹配必须在
  段边界停**，否则 `("u","1")` 读到 `("u","12")`；该 bug 2026-07 才修。
- **⚠️ 最反直觉**：CrewAI **确实自动折了一维**（`crew.py:637` `/crew/{crew_name}`，再拼 agent role），
  但折的是**生产者身份不是调用方身份**。**那恰好是 crew-go 现在的形状**——「自动折」那个位置
  早被「谁写的」占了，「写给谁的」**库层全行业空白**。
- **MAF 宿主层做了自动折，⚠️ 但初稿把两条链混成了一条（已拆）**：
  - **会话维**：`x-agent-**chat**-isolation-key` → `ChannelSession(isolation_key=…)`
    （`hosting-responses/_channel.py:221-223`）→ 折进 checkpoint 路径（`hosting/_host.py:1205`），
    **无 key 时 fail-closed**（`:1187-1189` 逐字：宁可跳过 checkpoint 也不污染那个共享桶）。
  - **用户维**：`IsolationKeys.user_key` 在**生产代码零消费者**（写入它的中间件在 `_host.py:569-580`，但**没有任何生产代码读它**；`_host.py:554` 那句 docstring 点名的 history provider 类**仓里根本不存在**）。真正按 user 折路径的在
    **另一个包**：`foundry_hosting/_responses.py:253-283`，用 **`x-agent-user-id`** 拼
    `<root>/<user_id>/<context_id>`，且**那条不是 fail-closed**（缺 user_id 回落无 scope 布局）。
  ⟹ **「业内已端到端做好用户维自动折 + fail-closed」不成立**——**会话维做好了、用户维没有**。⚠️ 中间件**只在确知跑在平台里时才装**，且安全注释明写
  「**never trust raw wire headers to identify a session bucket**」。
- **反面**：MAF 把下游 mem0 的 **per-call `user_id` 降级成了 `__init__` 参数**（⚠️ 初稿写「两个 provider 包 mem0」不确——包 mem0 的**只有一个**；第二个同形状的是 Redis provider、**后端不是 mem0**；其 .NET 侧反而收一个按 session 定 scope 的函数、不属这形状），
  而 MAF 宿主是**单实例服务全部请求** ⟹ 那个坑实证存在。CrewAI 更刺眼：**有** `AuthenticatedUser`
  （JWT + `sub` claim 齐全），但 auth 模块之外**零引用**——身份在门口验完就丢了，永远到不了 memory 层。
- **第三形状（claude-agent-sdk）**：身份是 key 类型里的**独立字段**——`SessionKey{project_key,
  session_id, subpath}`（`types.py:1331-1345`），**结构化非拼串**（不存在前缀撞兄弟）+ 枚举 API
  签名上就没有「列出全部」这个选项 + **conformance 套盯后端**（**14** 条里 2 条打隔离——⚠️ 初稿写 13，实际模块 docstring 两处都写 14、编号 1..14 齐全，`#14` 乱序夹在 `#8` 与 `#9` 之间是漏数的原因）。
  代价：**有默认值（cwd）就不能 fail-fast**，多租户忘了设不报错、静默合桶。

### §10.4 形态豁免（不构成证据的那几家）

Codex / dsh 是**单租户 CLI**，租户边界＝OS 用户。存储分别是 `~/.codex/sessions/`（⚠️ 初稿写「扁平」
**是错的**——实为 `YYYY/MM/DD` **三层日期分区**，`rollout/src/recorder.rs:1607-1624`；但**三层全是
日期、无身份段**，豁免论证不受影响）与 domain/table/record-key 三层零身份维。
**它们的「没做」不构成证据。**

⚠️ **本节数字曾三处不自洽（已订正）**：标题「七仓」/ §10.3 表格「8 家全部」/ 本段「五家」互相打架。
**统一为：实际引用 8 个仓**（codex / dsh / MAF / LangGraph / CrewAI / claude-agent-sdk / eino /
openai-agents）；**豁免掉两家 CLI 后库层证据来自 6 家**——初稿的「五家」**漏了 claude-agent-sdk**，
而它正是 §10.3「第三形状」整段的主角。

> **本节 audit 状态**：三份报告均**逐条自行跑代码交叉验证**后才采信——已实测复核的承重断言：
> LangGraph `BaseStore` 身份零命中 / 「Callers isolate tenants by namespace」原文 /
> CrewAI `/crew/{name}` 生产者维 / MAF isolation header / Codex `ext/memories` 四工具常量表 /
> Codex O_EXCL / CrewAI `MemorySlice` docstring / Codex prompt 模板 notes 零命中。
> **三处全称否定已在第 8 轮由一个专职 agent 独立重扫找反例**（不再是「未复核」）——结果：
> **A、C 两条被推翻、B 拆成三类**，本节各条已按结果逐格订正。第 9 轮另有一路**逐条核本节 47 条
> 引用**（2 ❌ 9 ⚠️，已全部订正）与一路核**订正文本本身**的引用（2 ❌ 6 ⚠️，已订正）。
>
> **仍未复核**：各报告中未被本节引用的其余细节；「结构性豁免（dsh 反复用）」那条**只有形状支撑、
> 未逐条锚定行号**；以及跨语言侧（.NET / JS）只在文件名层面扫过、未逐行读。

### §10.5 注入的三层 + 那条判别式（2026-08-27 证伪轮，**推翻了 §10.2 的统计**）

⚠️ **先订正 §10.2**：那里写的「开放式便签多数不注入」和方案篇里的「4:1」**方向都是反的**。
重数原因：① 按「厂商」数而非按**机制**数；② 把**两类性质相反**的东西混成一类——「内容其实还在
上下文里」（模型自己的 tool_call 参数没走）vs「真的什么都没有」。另有两家实际注入却被记反
（CrewAI `agent/core.py:575` 把召回内容拼进 task_prompt；OpenAI sandbox memory 每轮注入且预算
达 15k token）。**按机制重数约 9 注入 : 1 就地指针 : 1 已在历史 : 2 不注入。**

**修正后的分类（五类，不是两类）**：

| 类 | 特征 | 谁 |
|---|---|---|
| **A1 注目录** | 名字+描述清单，正文在工具后面 | MAF FileMemory / MAF MemoryContext(索引半) / dsh skill catalog 等 |
| **A2 注内容** | 检索/蒸馏出的材料本身 | OpenAI sandbox memory / CrewAI / MAF Todo |
| **B′ 就地指针** | **在内容被移走的原位**替换成 locator + 取回方式 | dsh spill |
| **C 已在历史里** | 模型自己的 tool_call 参数还在 | dsh todo |
| **D 完全不注** | 只有工具描述 | LangGraph store / MAF FileAccess |

⚠️ **「带外的轻量信号」（另发一条「你有 N 条便签」）——一家都没有。**

**Codex 的注入分三层，且两套产品各缺一层、缺的正好相反**（line-level 铁证：
`ext/history-notes/src/extension.rs:115-117` 注册 3 个 contributor **无 `prompt_contributor`**；
`ext/memories/src/extension.rs:124-127` 有）：

| | 信号层 | 索引层 | 指导层 |
|---|---|---|---|
| 线程便签 | ✅ ~106 tok | ❌ **零** | ✅ ~331 tok |
| 全局记忆 | ❌ 零 | ✅ ≤2500 tok | ✅ ~1536 tok |

**信号层内容**（`core/src/context/token_budget_context.rs:55-70`，全部定长、**与便签数量无关**）：
`Agent name` / `First|Current context window id` / **`Previous context window id`**（仅 reset 后出现，
载荷是一个布尔位「刚才断片了」）；另有 `You have {n} tokens left`，以及压缩前 reminder + fallback
prompt（各**每窗口至多一次**）。配置里有一笔**专门的** `auto_compact_fallback_buffer_tokens`——
schema 描述逐字「Additional tokens available after the compaction threshold for **fallback
note-taking**」。

⭐ **判别式（本次调研最大的收获）**：

> **这些内容是「模型需要时去查」，还是「不查也必须生效」？**
> 后者（用户偏好这类）**必须注入**——模型不会主动想到去查偏好；前者（便签 checkpoint）
> **信号 + 确定性寻址 + 零参列表工具就够**。Codex 在同一产品里做了相反两个选择，分界就在这一条，
> **不在便签多不多**。

**「只做信号层」够用的三个前提**：① 便签地址是**模型身份的确定性函数**且身份已在上下文里
（那家的路径 = `<agent_name>/notes`，而 agent 名就在信号块第一行）；② 列表工具**零必填参数**；
③ 一段常数大小的指导词讲清何时去读。**缺 ①② ⟹ 不知道去哪找；缺 ③ ⟹ 不知道该找。**

⚠️ **一条反向证据**：那家在信号块尾部**留了索引层的插槽**（给外部 MCP server 的 `thread_hint`
钩子，`core/src/session/mod.rs:3632-3658`），但**内置实现从不填**——想到了、做了插槽、然后不用。

⚠️ **注入不省提醒句，注入创造提醒句**：4/4 注入方都得再写一句「这是自动挑选的、**可能不完整**」
——注入一个**选择**会让模型以为选择是**完整的**。这是注入的**固有成本**。
