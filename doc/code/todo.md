# 项目待办

2026-11-20 以 `origin/test` 的 `fd40a78` 为基线。终局扇出、持久续跑准入、跨分区清档、剩余六项缺陷及 C01–C15 已合入。2026-11-18 核验时 [GitHub](https://github.com/gupf0719/crew-go/issues) 无开放 Issue，仓内编号见 [Issue 索引](../issue/issues.md)。

[逐条核验台账](backlog-audit.md)记录关闭、归并、已决和待验收的证据。[历史 TODO 快照](todo-history.md)保留原表、旧优先级快照和审计过程。

共 **44 项**。P1 **0**、P2 **4**、P3 **40**。缺陷 **0**、能力缺口 **2**、待决 **26**、验收 **16**。

- **缺陷**已有当前复现，或可从实际生产调用路径确认契约不一致。
- **能力缺口**对应现有实现边界外的功能，完成条件独立列出。
- **待决**需要选择业务边界、回放语义或设计选项。
- **验收**需要集成、凭据、目标环境，或先复核旧探针在当前版本上的结果。

当前部署使用独立存档。A02 和 A09 中共用存档的升级、故障切换和跨副本续跑验收暂缓。发版 CI、实际使用的后端及单实例入口验收仍有各自的完成条件。

## P2 · 业务回归与发布验收

| ID | 类型 | 待办 | 完成条件和证据 |
| --- | --- | --- | --- |
| A02 | 验收 | 发版候选 CI 与存档升级 | 收集发布 SHA 对应的 PostgreSQL/MySQL CI 结果。在实际启用的后端验证旧挂起档、迁移与回退。共用存档的互斥和跨副本恢复验收暂缓。 [CI](../../.gitlab-ci.yml) · [升级说明](../release/next.md) |
| A03 | 验收 | 路由模型接管门槛 | 仓内已有真服务报告与按输入、配置指纹做的离线回放。用当前业务的脱敏样本、人工标签及候选模型结果确定错路由率、承接率和端到端 P95 门槛。 [历史实测](../../example/decision/routing_eval/REPORT.md) · [接入和评测](../v2/route-decider.md) · [回放测试](../../example/decision/routing_eval/replay_test.go) |
| A04 | 验收 | OpenAI 真模型流式和工具调用 | Chat Completions 和 Responses 各有流式与工具调用冒烟，共四个独立用例。有 `OPENAI_API_KEY` 时收集四项结果。 [冒烟测试](../../example/model/openai_smoke/openai_smoke_test.go) |
| A05 | 验收 | Qwen 真模型链路和 A2A | 用现有 `llm_smoke` 验流式 artifact、服务端 HITL 和 Resume。按发布范围覆盖编排矩阵、Skill Action、工具审批与存档 durability。依赖 `QWEN_API_KEY`。 [冒烟命令](../description/smoke-test-recipes.md) |

## P3 · 窄路径、扩展与设计决策

| ID | 类型 | 待办 | 完成条件和证据 |
| --- | --- | --- | --- |
| C17 | 能力缺口 | 给内部决策和结构化输出补可观测区分 | 明确独立的路由/评估模型统计维度，补结构化输出类型。fallback 已 emit 指标，端到端断言归 A08。历史 TODO 350、382。 [可观测性](../v2/observability.md) |
| C18 | 能力缺口 | 按 Handoff 来源收窄候选与按次暂停转交 | 源自 ISSUE-99 的 #7 和 #8。分别验证候选权限和同一次执行中 tool/程序化转交都受控制。 [控制信号](../issue/issue-99-handoff-control-signal.md) |
| A06 | 验收 | Anthropic 真流、错误分类与服务端上下文清理 | 核对空闲流处理、SSE 间隔、真实计费拒绝及 prompt-too-long 原文的可识别性。跑 `TestSmoke_ContextEditing` 确认两种清理的实际统计、与 compaction 组合的请求以及严格前缀回放。另核对实际触发压缩后的交互和编辑前后的缓存用量。依赖 `ANTHROPIC_API_KEY` 与实际返回条件。 [流式冒烟命令](../description/smoke-test-recipes.md) · [清理冒烟](../../example/model/anthropic_smoke/context_editing_test.go) |
| A07 | 验收 | 跑齐 thinking 多轮回放、关停和 provider 变体 | 使用现有模型集成入口补 3 轮以上及中途关闭的对照。Anthropic 的空闲/计费另见 A06。历史 TODO 570。 |
| A08 | 验收 | 复核有具体目标的弱测试与历史探针 | 核对 Reflect/Plan 失败重试代数、Toolkit.StreamCall 的流式分派与退化，以及观测扇出实例和真实 fallback 指标。ISSUE-99 的现有嵌套测试已断言各成员输出，补慢历史后端下共享输入只写一次的断言。provider 候选归 A18。历史 TODO 207、291、569、639–640、659 及 ISSUE-16、99。 |
| A09 | 验收 | Plan Web 生产入口与生命周期 | mock 已覆盖挂起、续跑及完成态。目标环境仍需验鉴权、Cookie、反向代理、状态 TTL 和故障恢复。共享 Redis 迁移、双副本切换和跨副本续跑暂缓。 [ISSUE-90](../issue/issue-90-plan-chat-web-lifecycle-prod.md) |
| A10 | 验收 | 明确 MySQL Store 的有限数值写入域与范围过滤 | 现有后端方言和 conformance 已覆盖默认类型；验证边界、越界和下推过滤的真实 MySQL 行为，再固定告警、返回或约束。历史 TODO 502。 [SQL 方言](../../sql/dialect.go) |
| A11 | 验收 | GraphAgent 多层脱离与嵌套调用的任务边界 | 图身份、显式源档、钩子换分区及条件清档已有回归。余下核对图外钩子、外来 ctx、裸调和闭包逻辑改版，以及直接完成子图后再恢复父图时的完成记录归属。旧档部署验收归 A02。 [脱离入口](../../agent_v2/detach_origin_test.go) · [直接恢复子图](../../agent_v2/handoff_commit_test.go) · [分区回归](../../agent_v2/graph_checkpoint_late_backend_test.go) |
| A13 | 验收 | 复核禁止重做的入口否决与重试 | 现有回归已覆盖有权重做的上游及若干钩子相位。余下用 AddSubgraphNode、成员主图和 RunWithRetry 对照不可重做的输入，检查上游调用次数、重试之间的代际与取消时的存档。 [条件边](../../compose_v2/out_edges.go) · [入口否决回归](../../compose_v2/entry_reject_paths_test.go) |
| A14 | 验收 | 低阶 BasicAgentBuilder 的工具 tracing 使用自身类型查表 | 嵌套节点运行同名工具时，验证观测分类来自自身的位置与工具绑定，而不是调用方的表。历史 TODO 532。 [构建器](../../agent_v2/basic_builder.go) |
| A15 | 验收 | 把 Anthropic 回放的隐藏 thinking 纳入预算核对 | `ReasoningContent` 文本已计入，签名块可能仅存在标注。优先用真实回放的服务端 input usage 对照触发点。历史 TODO 329。 [估算](../../compose_v2/token_estimate.go) · [Anthropic](../../model/anthropic/client.go) |
| A16 | 验收 | 复测同一消息对象跨两个调用槽的历史归属 | 旧探针中第二份回复被判成透传。用当前真实 GraphAgent/HistoryStore 复测，同 ID 独立来源与真正透传作对照。历史 TODO 428。 [历史归属](../../agent_v2/history_slot.go) |
| A17 | 验收 | 复测自定义模型的非 UTF-8 工具调用 ID | provider JSON 下不会产生这类 ID。通过自定义模型和持久存档复核中断寻址，确认入口应规范化还是拒绝。历史 TODO 341。Redis 旧 GraphID 的迁移修复见 [D07](backlog-audit.md#2026-11-18-剩余缺陷修复)。 |
| A18 | 验收 | 核对仍有独立使用意义的历史审查条目 | 对只有档案结论的事项按台账定位。先核对现行的长 HistoryKey 可移植边界、旧候选可用性及低阶/自定义入口，再决定是否补测试。 [历史 TODO 逐条表](backlog-audit.md#历史-todo-逐条核验) |
| Q01 | 待决 | 可插拔 Sandbox 的一致性边界 | 现有 `Sandbox` 和工具适配器只提供约定及单测。确定哪些文件、网络、进程及取消语义由每个应用实现保证，是否要落可共享 conformance。 [现行契约](../v2/sandbox.md) · [接口](../../sandbox/sandbox.go) |
| Q02 | 待决 | 多分支停机时对外报告与挂起记录的关系 | 终局错误与身份冲突并存时保留两条错误链，受保护的子档保留。余下决定某支存档失败时，哪些兄弟问题需要对外展示及如何继续处理，以及成功清理兄弟挂起档时是否报告数量。 [超步退出](../../compose_v2/graph_loop.go) · [历史记录](todo-history.md) |
| Q03 | 待决 | MicroCompact 转存生命周期与取回契约 | 确定 offload 键回收 / TTL 的责任方及引用有效期。与按真实消息位置保留近期结果、存档引用仍可读取的现有回归一起验。 [压缩](../../agent_v2/microcompact.go) · [ISSUE-98](../issue/issue-98-conversation-history.md) |
| Q05 | 待决 | 引入 AgenticMessage 或继续使用现行消息契约 | 对照现行 `schema.Message`、历史与流式适配器，决定是否需要双格式。依赖明确的跨 provider/消息生命周期诉求。 [演进状态](../description/three-way-comparison/eino-langgraph-upgrades.md) |
| Q06 | 待决 | 故障分析 RCA 立项 | 已有设计提案。确认业务场景、知识来源和可信验收样本，再选择最小实现范围。 [设计](../v2/fault-analysis-system.md) |
| Q07 | 待决 | SelfOnly 与历史裁剪保留哪些用户轮次和派生上下文 | 分开核对成员接手后能见哪些用户轮次，以及 Reflect/Plan 加载截断时是否保留唯一的派生输入和后继产出。历史 TODO 305、593。 |
| Q08 | 待决 | 让 Claude 的签名 thinking 与压缩、工具和缓存前缀共存 | 列清历史/System/工具表改变导致签名失效的形状，分别决定何时 drop，何时重建，何时不承诺缓存复用。历史 TODO 321、352、525。 |
| Q09 | 待决 | AgentAsTool 对模型呈现的错误文本边界 | 确保控制流错误保真，决定普通失败的统一文案、业务错误映射和日志保留范围。历史 TODO 333。 [AgentAsTool](../../agent_v2/agent_tool.go) |
| Q10 | 待决 | ReactiveCompact 是否优先复用 AutoCompact 产物 | 对照现有最旧组急救、摘要重用与原问题保留，在节省上下文和回放正确性之间选明确语义。历史 TODO 347、372、409。 |
| Q11 | 待决 | 工具审批如何按风险升级和允许模型请求 | 按风险选择默认行为、异步观察转同步确认、模型主动请求理由是余项。提示已有 WithApprovalPrompt，候选签名和 Deny 优先为既有契约。历史 TODO 410、417、438。 |
| Q12 | 待决 | 框架任务状态和调度约束的演进 | 检查 visit/化身、按调用单元声明 replay-safety、节点外围部署的预算、goal 的显式激活。保留现有超步和审批不变量。历史 TODO 356、407、574。 |
| Q13 | 待决 | 观测数据源与历史写入的权威 | 确定 Events 回放的作用域、历史异步写失败策略、条件否决重算时的 append 审计记录与送模投影、用户放弃时的原话/标记。历史 TODO 106、448、579、593 及 ISSUE-98。 |
| Q14 | 待决 | Sandbox/Code Mode 与框架信任边界 | 在 Q01 确定一致性接口后再决定可执行能力和隔离模型。授权化检索、审批工具提示另有行。历史 TODO 408、414、439。 |
| Q15 | 待决 | 文档与测试门禁是否扩展 | 候选含 godoc 位置、守护测试的符号覆盖、全称 AST 断言、golden、并行审计隔离和 finding 判据。先以可复现错误验证收益。历史 TODO 363、400、401、418、492 及 ISSUE-73。 |
| Q16 | 待决 | 复用各编排的 Run 序幕与钩子相位 | 核对构建/载入/失败存档与 start/end 内嵌子调用身份。保留现有入口独立覆盖，按真实漂移决定是否抽取。历史 TODO 402、561、585。 |
| Q17 | 待决 | Store、历史及压缩数据的保留和成本 | 独立核对 HistoryStore 留存/导出、Redis 空 namespace 回收、PG 表达式索引与 CASE 类型守卫成本、AutoCompact 体量及摘要边界格式。以实际负载建立基线。历史 TODO 392、425、493、500 及 ISSUE-99。 |
| Q18 | 待决 | 决策模型推广至其他决策点 | RoutingAgent 已有；Reflect 评估/Plan 规划等先明确各自结果、错误和人审策略，避免套用路由阈值。历史 TODO 293。 |
| Q19 | 待决 | Plan 步骤同批依赖与动态并行 | 明确何时启用 DAG，同批 InputRefs 的解析时机和并行失败语义。当前 Plan 逐轮只执行首步。 [ISSUE-47](../issue/issue-47-planstep-explicit-input-refs.md) |
| Q20 | 待决 | Plan 级主动审批 | 现有 ExecInputBuilder 的 Ask 可在单步审批。若要内置 plan gate，先定义首次计划、重规划和版本变化时审批复用。 [ISSUE-58](../issue/issue-58-interrupt-timing-semantics.md) |
| Q21 | 待决 | 无明确取消意图时的历史保留与回收 | 普通 TTL/LRU、共享 Closing 和异常丢句柄可只完成清档。定义什么终止证据允许补历史，以及历史后端失败时重试责任。已接受的共享取消已有持久收尾。 [历史收尾复核](backlog-audit.md#悬空工具调用历史收尾--test-e185749) |
| Q22 | 待决 | 组合钩子链内部采用 ctx 的规则 | 当前公开契约逐跳传递上一个 hook 返回的 ctx，框架外层已有来源检查和嵌套 scope 回归。先用当前调用复核观测或授权丢失，再决定是否修改组合钩子契约。历史 TODO 520、522。 [钩子链](../../hook/hooks.go) |
| Q23 | 待决 | 是否允许 AG-UI 客户端为单轮提供指令 | context/state 已按用户数据进入送模视图。若需显式客户端指令，先定义授权、优先级和持久化边界。历史 TODO 302。 [数据入口](../../agui/client_data.go) |
| Q24 | 待决 | StoreMemory 的自定义键和过滤后配额 | 键函数影响身份和覆盖策略。非合规 Store 返回越界项时，现行过滤避免泄漏，配额可能被占用。按实际 backend 决定是否需要透明补采。历史 TODO 367、395、403。 |
| Q25 | 待决 | 软降落遭遇不守 tool_choice 的模型时如何收尾 | 目前越权工具不执行。决定是否提供额外只读收尾机会，以及已确定不能执行时是否进入可产生 HITL 的 OnToolStart。历史 TODO 194、202。 |
| Q26 | 待决 | 是否持久记录 tool_search 同批解锁动作 | 依赖同批重放的当前路径有保护。按不同工具快照和脱离调用层的对照决定是否需要动作记录。历史 TODO 599。 |
| Q27 | 待决 | 持久化业务状态演进与按执行 namespace 查历史 | ISSUE-70 Phase 4 的通用状态迁移需要版本化调用方。SelfHistoryByNS 希望按执行 namespace 查写入记录，先核对执行位置能否成为历史身份。 [ISSUE-70](../issue/issue-70-checkpoint-evolution.md) |

## 已关闭及归并去向

- **C16 已实现。** 固定前驱可跨超步汇合，已触发的收尾节点等普通工作排空后执行。SQLite 回归覆盖子图两次挂起、原地址续跑、等待实例的私有输入、已完成成员只执行一次和清档。 [调度用法](../v2/channel-engine.md#46-固定前驱汇合与收尾节点) · [跨层回归](../../sql/graph_schedule_test.go)

- **C15 已实现。** 自研 Agent 可用 `EnterResume` 校验独立续跑请求并保留原会话审批。低阶图嵌套时上下文绑定子会话，回归覆盖原提问续跑、完成记录重放、历史归属和清档。 [自研入口](../v2/usage-guide.md#自研-agent-的续跑) · [跨层回归](../../agent_v2/custom_nested_graph_test.go)

- **C14 已实现。** A2A human gate 在旧任务失败、被拒绝或取消后重发原始请求时带上旧任务引用。冷续跑恢复引用，新问题使用新任务身份。 [重发契约](../v2/a2a.md#远端终局后的原请求重发) · [跨层回归](../../a2a/gate_task_reference_test.go)

- **C13 已实现。** 普通慢工具可在有明确身份的子调用中转后台。存档恢复句柄，同一管理器可返回终态及附件。管理器丢失时报告找不到，工具账仍阻止已转后台的调用重跑。 [慢工具](../v2/background-tasks.md) · [跨层回归](../../agent_v2/background_task_nested_test.go)

- **C12 已实现。** 历史查询先检查本次请求授权，再读取当前成员的服务端会话摘录。便签按认证 Owner 和写入者分区，经业务审批写入并可在摘要后按需读取。 [用法](../v2/self-tracking.md#c12-使用方法) · [历史跨层回归](../../agui/history_search_test.go) · [便签跨层回归](../../agent_v2/note_tools_integration_test.go)

- **C11 已实现。** Skill 增加可选递归发现和原子重载，启用时执行绑定目录快照并校验续跑版本。`tool_search` 可限制任务内累计解锁。工作流和分发通过现有编排及内存 SkillBox 接入。 [Skill 用法](../v2/skill.md#文件目录与热重载) · [目录跨层回归](../../agent_v2/skill_reload_test.go) · [额度跨层回归](../../agent_v2/tool_search_limit_test.go)

- **C10 已实现。** 已完成的候选可按配置退回 supervisor。跨层回归覆盖历史接力、挂起续跑和只写一次。诊断按注册名报告有工具要求的候选漏配。 [用法](../v2/handoff.md#81-确定性退回) · [退回回归](../../agent_v2/handoff_return_test.go) · [诊断回归](../../agent_v2/handoff_diagnostic_test.go)

- **C09 已实现。** SQL Store 可按表启用 TTL，旧行保持永久。Get、Search、ListNamespaces 和 Batch 使用数据库时钟检查可见性，显式清扫只清当前 Store 表。 [用法](../v2/store.md#342-sql-store-ttl) · [跨层回归](../../sql/store_ttl_test.go)

- **C08 已实现。** 支持按供应商返回的标签词元概率校准 LLM 路由置信度。跨层回归覆盖校准后的门槛判定、元数据缺失和普通 classifier 的接手。真实业务校准和接管门槛由 A03 验收。 [用法](../v2/route-decider.md#48-校准-llm-路由的置信度) · [跨层回归](../../decision/llm_route_decider_test.go)

- **C07 已实现。** AG-UI 能按存档或线上消息身份准备编辑与重生分支。原会话保留，分支读取服务端前缀。SQLite 回归覆盖 BasicAgent 流式入口、媒体回放与工具实例映射。 [用法](../v2/agui.md#历史分叉与消息寻址) · [跨层回归](../../sql/agui_history_fork_test.go)

- **C05 已实现。** GraphAgent 可为每次 Run 或 Resume 添加断点。在原边界继续后按新配置停下，经 SQLite 重连仍以当前中断 ID 寻址。 [用法](../v2/execution-control.md#11-用法) · [跨层回归](../../sql/agui_breakpoint_test.go)

- **C04 已实现。** 节点空闲预算按进度续期，固定时限与调用方期限继续约束尝试。回归覆盖嵌套流、重试预算、超时后迟到结果及子图存档恢复。 [节点时限](../v2/execution-control.md#22-固定时限空闲时限与取消) · [跨层回归](../../agent_v2/node_idle_timeout_test.go)

- **C03 已实现。** AG-UI 和 A2A 的断点挂起包含 before、after 与图 namespace，继续时核对当轮中断 ID。回归覆盖同名子图、同边界双向命中、旧地址、同批提问和共享会话重连。 [AG-UI](../v2/agui.md#声明式断点) · [A2A](../v2/a2a.md#声明式断点)

- **C02 已实现。** MCP 图片和音频跟随原工具结果，Anthropic 与 OpenAI Responses 使用原生图片块，适用的 Chat Completions 模型按需接收图片或音频。BasicAgent 回归覆盖同批配对、结构化大整数、序列化续跑及附件清理。 [用法](../v2/mcp.md#10-工具结果的图片和音频) · [模型请求](../../mcp/tool_media_test.go) · [存档续跑](../../mcp/tool_media_resume_test.go)

- **C01 已实现。** MCP 客户端可独立订阅服务端通知，并按流游标接回握手或工具响应。Hub 回归确认写操作只执行一次，旧会话的迟到失败只影响原调用。 [用法](../v2/mcp.md#9-sse-推送与续订) · [传输回归](../../mcp/http_sse_test.go) · [会话并发回归](../../mcp/http_sse_lifecycle_test.go)

- **C06 已实现。** Anthropic 客户端可按需清理旧工具结果和 thinking。HTTP/SSE 回归覆盖请求合成、清理统计解析和签名回放，并核对了缓存请求选项。真实服务验收归 A06。 [用法](../v2/model-adaptation.md#51-默认请求与选项) · [回归](../../model/anthropic/context_editing_test.go)

- **D05、D07 已修。** AG-UI 的慢页能跨轮询窗口继续读取和派发。Redis 旧档迁移识别 JSON 对非法 UTF-8 的替换，保留原始元数据和索引。 [回归与关闭记录](backlog-audit.md#2026-11-18-剩余缺陷修复)

- **D06、D11、D08、D10 已修。** 写档保留调用方输入，确认整任务收尾后回收审批规则，Handoff 共享历史只在直接候选调用生效，批量写档按 namespace 字节序停止在首个错误。 [回归与关闭记录](backlog-audit.md#2026-11-17-四项缺陷修复)

- **终局扇出与跨分区清档已修。** `0f72f6b` 保留终局失败和图身份冲突两条错误链，封存本层存档并保留冲突子档。`0588906` 把续跑绑定到实际分区，由后端原子核对清档范围。取得清档清单后出现新后代时，整次删除被拒绝。 [扇出回归](../../agent_v2/terminal_checkpoint_conflict_test.go) · [分区回归](../../agent_v2/graph_checkpoint_late_backend_test.go) · [条件清档回归](../../sql/checkpointer_atomic_graph_test.go)
- **持久 Resume 准入已完成。** `0588906` 在派发节点之前领取存档世代。根档封存与 AG-UI 会话解绑都失败时，新请求或直接 Resume 都不能重复执行旧写入。Sync 和 Async 的子图过程档也有回归。合并态 `make test` 四个模块通过，`make cover` 的 10 个核心包达标，总覆盖率 88.2%。 [双失败回归](../../sql/agui_terminal_conflict_test.go) · [过程档回归](../../compose_v2/checkpoint_resume_test.go) · [本轮核验](backlog-audit.md#2026-11-17-合入-test-后的核验)
- **A12 已修。** 错误树中的中断按分支顺序公开及落档，合法问题可全部续跑，非法叶子终止本批。聚合结果有独立的问题副本。 [回归](../../compose_v2/interrupt_fanout_regression_test.go)
- **新任务准入和流式 fallback 已修。** 同 RunID 的可续档受任务登记保护。流预读有限，选中流的长限流影响后续请求，旧流的结束不会解除本次熔断。 [新任务回归](../../agent_v2/fresh_run_checkpoint_test.go) · [流式回归](../../fallback/stream_regression_test.go)
- **D01、D02、D21 已修。** AG-UI 阻止迟到的新 Run 重复执行，撤回后丢弃同一模型调用的迟到工具事件，跨历史分区的替换在原历史库收尾且可补偿。 [复核台账](backlog-audit.md#最近复审发布与设计入口)
- **D04、D09 已修。** Redis 清档复用已有权限并在写入前完成核对。AG-UI 和 A2A 共用运行结果分类，分别返回协议错误码与安全正文。 [复核台账](backlog-audit.md#最近复审发布与设计入口)
- OTel 错误正文脱敏与截断、fallback 首片段前切换、显式后台子 Agent 的跨进程恢复、离线路由回放和 PostgreSQL/MySQL CI 接入均有源码与回归依据。目标 CI 和业务数据的验收仍在 A02、A03。 [关闭依据](backlog-audit.md#最近复审发布与设计入口)
- **A01 已关闭。** AG-UI 展示定稿、AutoCompact 答复归属和摘要审批视图的合并态跨层回归通过。旧存档首次续跑遇到可重试错误后仍能继续，摘要按新问题重新确认，业务答复继续按原 ID 使用。四模块 `make test` 和 `make cover` 通过，核心包合并覆盖率 88.5%。 [验证记录](backlog-audit.md#最近复审发布与设计入口) · [升级约定](../release/next.md)
- ISSUE-48 与 ISSUE-64 同属子图接入。ISSUE-52 的外部测试归 A02、A04–A06。ISSUE-93、97、98 和 99 的现存范围及去向见[编号台账](backlog-audit.md#issue-6599-逐号核验)。
