# 项目待办

2026-11-16 逐项核验以 `test` 的 `e185749` 为基线。A01 已完成 `fb1f22e` 与该基线的集成验证。GitHub 仓库当前没有独立 Issue 记录，仓内编号见 [Issue 索引](../issue/issues.md)。

[逐条核验台账](backlog-audit.md)记录关闭、归并、已决和待验收的证据。[历史 TODO 快照](todo-history.md)保留原表、旧优先级快照和审计过程。

共 **67 项**。P1 **0**、P2 **5**、P3 **62**。缺陷 **6**、能力缺口 **18**、待决 **26**、验收 **17**。

- **缺陷**已有当前复现，或可从实际生产调用路径确认契约不一致。
- **能力缺口**对应现有实现边界外的功能，完成条件独立列出。
- **待决**需要选择业务边界、回放语义或设计选项。
- **验收**需要集成、凭据、目标环境，或先复核旧探针在当前版本上的结果。

## P2 · 业务回归与发布验收

| ID | 类型 | 待办 | 完成条件和证据 |
| --- | --- | --- | --- |
| C06 | 能力缺口 | 提供 Anthropic 服务端 context editing | 当前已有签名回放和服务端 compaction。分别验证工具结果与 thinking 的编辑参数、beta 请求头、与既有缓存和前缀绑定的交互。历史 TODO 328。 [Anthropic](../../model/anthropic/client.go) |
| A02 | 验收 | 发版候选的目标 CI 和存档升级验收 | `test-postgres` 和 `test-mysql` 已定义，`e185749` 提交记录含本机真库结果。收集目标 GitLab job 结果，准备回放 Redis 旧键迁移、SQL 身份列和 AG-UI 轮询表迁移，验证挂起恢复及回退。 [CI](../../.gitlab-ci.yml) · [升级说明](../release/next.md) |
| A03 | 验收 | 路由模型接管门槛 | e185749 实现按输入和配置指纹做缓存回放、阈值扫描，已有 89 条构造会话验证工具链。需要脱敏业务样本、人审标签和模型调用结果来定错路由率、承接率和 P95 门槛。 [接入和评测](https://github.com/gupf0719/crew-go/blob/e18574950566e8b5a52f1f9ebff0f3da50028bca/doc/v2/route-decider.md) · [回放测试](https://github.com/gupf0719/crew-go/blob/e18574950566e8b5a52f1f9ebff0f3da50028bca/example/decision/routing_eval/replay_test.go) |
| A04 | 验收 | OpenAI 真模型流式和工具调用 | Chat Completions 和 Responses 各有流式与工具调用冒烟，共四个独立用例。有 `OPENAI_API_KEY` 时收集四项结果。 [冒烟测试](../../example/model/openai_smoke/openai_smoke_test.go) |
| A05 | 验收 | Qwen 真模型链路和 A2A | 用现有 `llm_smoke` 验流式 artifact、服务端 HITL 和 Resume。按发布范围覆盖编排矩阵、Skill Action、工具审批与存档 durability。依赖 `QWEN_API_KEY`。 [冒烟命令](../description/smoke-test-recipes.md) |

## P3 · 窄路径、扩展与设计决策

| ID | 类型 | 待办 | 完成条件和证据 |
| --- | --- | --- | --- |
| D05 | 缺陷 | 修复合法慢后端的后台取消持续超时 | 守约的 6 秒逐页返回后端在现行 5 秒扫描预算下从不派发。在租约接管仍可推进的同时，恢复单页调用的有效预算。 [取消扫描](../../agui/cancellation_recovery.go#L150) |
| D06 | 缺陷 | 修复内存 Checkpointer 复用输入指针导致的存档身份覆盖 | 同一 `Checkpoint` 顺序保存到两个任务后，第一槽读出的 ID/NS 被改写。独立输入对象作为控制组，核对身份及旧存档。 [内存实现](../../compose_v2/checkpointer.go#L687) · [ISSUE-24](../issue/issues.md#-issue-24--数据流转-bug-专项审查2026-04-15) |
| D07 | 缺陷 | Redis 迁移兼容旧 GraphID 的 JSON 字节归一化 | 旧 writer 允许含非 UTF-8 字节的 GraphID，元数据使用原字节。真实 Redis 对照中迁移对普通 Unicode 成功，对该存档报错中止。验证迁移保留原元数据和结果可继续枚举。 [迁移校验](../../redis/checkpointer_migration.go#L55) |
| D08 | 缺陷 | 限制 Handoff 历史模式的继承范围 | 源码及 ISSUE-93 记录显示 Handoff 模式随 ctx 进入内层非 Handoff 成员。复测成员选择的 prompt、事件和历史去重，再为拥有该配置的调用限定边界。 [ISSUE-93](../issue/issue-93-handoff-modelb-audit-low.md) · [成员派发](../../agent_v2/member_dispatch.go) |
| D10 | 缺陷 | 统一 Checkpointer.SaveBatch 的文档承诺与行为 | 接口同时写了原子批量和逐条报错，三个后端实际逐条执行。当前内部没有生产调用者。用中间项失败的 conformance 固定选定的契约。 [接口](../../compose_v2/checkpointer.go) · [持久化决策](../description/three-way-comparison/checkpoint-storage.md) |
| D11 | 缺陷 | 回收已结束任务的 `approve_always` 记录 | `NextTask` 切换任务后，复用的 Session 仍持有旧规则。回收只针对终结任务，并与挂起、嵌套共享和在途审批对照。 [审批](../../agent_v2/approval.go) |
| C01 | 能力缺口 | MCP Streamable HTTP 的独立 GET 推送和 SSE 断线续订 | POST 响应 SSE、会话协商、过期重建和 DELETE 已覆盖。余下 GET/事件续订按同一会话和游标对账。 [MCP 已知缺口](../v2/mcp.md#9-已知缺口) · [现有会话回归](../../mcp/http_session_test.go) |
| C02 | 能力缺口 | MCP 工具的图片、音频结果透传 | 现行适配器生成文字说明。各支持媒体的模型适配器需要保留工具结果的内容块和调用配对。 [适配器](../../mcp/tool_adapter.go) · [MCP 已知缺口](../v2/mcp.md#9-已知缺口) |
| C03 | 能力缺口 | 从协议层报告细粒度断点信息 | 核心已有断点。按协议分别定义 AG-UI 和 A2A 的调试暂停事件、节点身份、续跑地址和状态映射。 [中断契约](../v2/hitl-resume.md) |
| C04 | 能力缺口 | 节点按进度续期的空闲超时 | 固定 `WithNodeTimeout` 与 provider SSE 空闲超时都已实现。单列图节点进度驱动的预算和救济。历史 TODO 457。 |
| C05 | 能力缺口 | GraphAgent 单次调用级断点 | 核心已有图级断点。单次诊断配置及其 Resume 寻址需额外接线。历史 TODO 459。 |
| C07 | 能力缺口 | 需要编辑或重生时让前端与服务端历史共用消息身份 | 服务端历史权威既定。定义编辑的分叉与保留策略，以及调用 ID、SSE 消息 ID 和 HistoryStore 记录的映射。历史 TODO 306。 |
| C08 | 能力缺口 | 路由模型输出可用的分类置信度 | 单独验证当前 classifier 输出契约和 RouteDecider 档位置信度。只在 provider 可提供可校准的值时接入。历史 TODO 282。 |
| C09 | 能力缺口 | SQL Store 可选 TTL | 已有清理流程和显式删除。定义到期后的读取、命名空间枚举、历史保留和跨方言时钟行为。历史 TODO 358。 |
| C10 | 能力缺口 | Handoff 确定性退回和缺 transfer 的诊断 | 阶段 3 是可选拓扑，忘装 transfer 的软提示需能区分合法终点。两条分别验收。历史 TODO 359、518。 [Handoff](../v2/handoff.md) |
| C11 | 能力缺口 | Skill 递归发现、热更新与产品化选项 | 结合实际场景选递归发现、重载、按需工具激活上限、技能编排和可选分发方式。既有资源分层、解析、Go Action 与调用预算已经实现。 [Skill](../v2/skill.md) · [历史备选项](../description/three-way-comparison/skill-system.md) |
| C12 | 能力缺口 | 用户授权后的历史查询与摘要后 notes | 明确键空间、可读用户和检索结果来源，保留现有存储或消息投影的访问约束。历史 TODO 406、415。 |
| C13 | 能力缺口 | 普通嵌套慢工具的后台执行与持久性边界 | e185749 关闭显式持久后台子 Agent 方案。独立记录普通 `WithBackgroundTasks` 在嵌套运行的支持范围和异步工具结果恢复。历史 TODO 422、423。 [慢任务](../v2/background-tasks.md) |
| C14 | 能力缺口 | A2A human gate 的旧任务引用 | 在远端任务已终局且需要发新请求的路径带回旧任务引用，保持初次请求与重挂语义。历史 TODO 554。 [远端实现](../../a2a/remote_agent.go) |
| C15 | 能力缺口 | 扩充自研 Agent 的续跑和低阶嵌套能力 | 当前 `EnterRun` 面向 Run，仓内远端 Agent 不靠这里 Resume。提供独立续跑入口前先锁定授权、历史和退出边界。历史 TODO 534。 |
| C16 | 能力缺口 | 手写 Graph 的 AND-join 与收尾屏障 | 当不同深度前驱汇入同一节点时提供全到齐的策略。普通 any-of 保留现行契约。历史 TODO 571。 |
| C17 | 能力缺口 | 给内部决策和结构化输出补可观测区分 | 明确独立的路由/评估模型统计维度，补结构化输出类型。fallback 已 emit 指标，端到端断言归 A08。历史 TODO 350、382。 [可观测性](../v2/observability.md) |
| C18 | 能力缺口 | 按 Handoff 来源收窄候选与按次暂停转交 | 源自 ISSUE-99 的 #7 和 #8。分别验证候选权限和同一次执行中 tool/程序化转交都受控制。 [控制信号](../issue/issue-99-handoff-control-signal.md) |
| A06 | 验收 | Anthropic 真流和错误分类 | 核对空闲流处理、SSE 间隔、真实计费拒绝及 prompt-too-long 原文的可识别性。依赖 `ANTHROPIC_API_KEY` 与实际返回条件。 [冒烟命令](../description/smoke-test-recipes.md) |
| A07 | 验收 | 跑齐 thinking 多轮回放、关停和 provider 变体 | 使用现有模型集成入口补 3 轮以上及中途关闭的对照。Anthropic 的空闲/计费另见 A06。历史 TODO 570。 |
| A08 | 验收 | 复核有具体目标的弱测试与历史探针 | 核对 Reflect/Plan 失败重试代数、Toolkit.StreamCall 的流式分派与退化，以及观测扇出实例和真实 fallback 指标。ISSUE-99 的现有嵌套测试已断言各成员输出，补慢历史后端下共享输入只写一次的断言。provider 候选归 A18。历史 TODO 207、291、569、639–640、659 及 ISSUE-16、99。 |
| A09 | 验收 | Plan Web 目标环境双副本和生产入口 | 已实现共享 Redis、生命周期和 mock 恢复。验真实用户鉴权、Cookie/反向代理、状态 TTL、部署迁移、双副本切换及故障窗口。 [ISSUE-90](../issue/issue-90-plan-chat-web-lifecycle-prod.md) |
| A10 | 验收 | 明确 MySQL Store 的有限数值写入域与范围过滤 | 现有后端方言和 conformance 已覆盖默认类型；验证边界、越界和下推过滤的真实 MySQL 行为，再固定告警、返回或约束。历史 TODO 502。 [SQL 方言](../../sql/dialect.go) |
| A11 | 验收 | GraphAgent 多层脱离与嵌套调用的任务边界 | 已验证有名和匿名图拓扑冲突、显式快照、GraphAgent 包装名和跨后端的子树归属。复核图外钩子、外来 ctx、裸调及闭包逻辑版本的剩余变体。旧档部署验收归 A02。 [任务隔离](../../agent_v2/detach_task.go) · [图存档身份](../v2/checkpoint.md#43-graphid-防串台) |
| A13 | 验收 | 解决禁止重做的入口否决在重试中反复沿用原输入 | AddSubgraphNode、成员主图、RunWithRetry 有不同所有权边界。核对上游调用次数、可否合法推进重做代际和取消时存档保留，按条件避免原地耗尽重试。历史 TODO 562、563。 [条件边](../../compose_v2/out_edges.go) |
| A14 | 验收 | 低阶 BasicAgentBuilder 的工具 tracing 使用自身类型查表 | 嵌套节点运行同名工具时，验证观测分类来自自身的位置与工具绑定，而不是调用方的表。历史 TODO 532。 [构建器](../../agent_v2/basic_builder.go) |
| A15 | 验收 | 把 Anthropic 回放的隐藏 thinking 纳入预算核对 | `ReasoningContent` 文本已计入，签名块可能仅存在标注。优先用真实回放的服务端 input usage 对照触发点。历史 TODO 329。 [估算](../../compose_v2/token_estimate.go) · [Anthropic](../../model/anthropic/client.go) |
| A16 | 验收 | 复测同一消息对象跨两个调用槽的历史归属 | 旧探针中第二份回复被判成透传。用当前真实 GraphAgent/HistoryStore 复测，同 ID 独立来源与真正透传作对照。历史 TODO 428。 [历史归属](../../agent_v2/history_slot.go) |
| A17 | 验收 | 复测自定义模型的非 UTF-8 工具调用 ID | provider JSON 下不会产生这类 ID。通过自定义模型和持久存档复核中断寻址，确认入口应规范化还是拒绝。历史 TODO 341。Redis 旧 GraphID 另见 D07。 |
| A18 | 验收 | 核对仍有独立使用意义的历史审查条目 | 对只有档案结论的事项按台账定位。先核对现行的长 HistoryKey 可移植边界、旧候选可用性及低阶/自定义入口，再决定是否补测试。 [历史 TODO 逐条表](backlog-audit.md#历史-todo-逐条核验) |
| Q01 | 待决 | 可插拔 Sandbox 的一致性边界 | 现有 `Sandbox` 和工具适配器只提供约定及单测。确定哪些文件、网络、进程及取消语义由每个应用实现保证，是否要落可共享 conformance。 [现行契约](../v2/sandbox.md) · [接口](../../sandbox/sandbox.go) |
| Q02 | 待决 | 多分支停机时对外报告与挂起记录的关系 | 确定一支存档失败时另一支已生成问题该如何报告和恢复，以及终局清除兄弟挂起时需否记录数量。保持现行错误优先级和错误链。 [超步退出](../../compose_v2/graph_loop.go) · [历史记录](todo-history.md) |
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

- **A12 已修。** 错误树中的中断按分支顺序公开及落档，合法问题可全部续跑，非法叶子终止本批。聚合结果有独立的问题副本。 [回归](../../compose_v2/interrupt_fanout_regression_test.go)
- **新任务准入和流式 fallback 已修。** 同 RunID 的可续档受任务登记保护。流预读有限，选中流的长限流影响后续请求，旧流的结束不会解除本次熔断。 [新任务回归](../../agent_v2/fresh_run_checkpoint_test.go) · [流式回归](../../fallback/stream_regression_test.go)
- **D01、D02、D21 已修。** AG-UI 阻止迟到的新 Run 重复执行，撤回后丢弃同一模型调用的迟到工具事件，跨历史分区的替换在原历史库收尾且可补偿。 [复核台账](backlog-audit.md#最近复审发布与设计入口)
- **D04、D09 已修。** Redis 清档复用已有权限并在写入前完成核对。AG-UI 和 A2A 共用运行结果分类，分别返回协议错误码与安全正文。 [复核台账](backlog-audit.md#最近复审发布与设计入口)
- OTel 错误正文脱敏与截断、fallback 首片段前切换、显式后台子 Agent 的跨进程恢复、离线路由回放和 PostgreSQL/MySQL CI 接入均有源码与回归依据。目标 CI 和业务数据的验收仍在 A02、A03。 [关闭依据](backlog-audit.md#最近复审发布与设计入口)
- **A01 已关闭。** AG-UI 展示定稿、AutoCompact 答复归属和摘要审批视图的合并态跨层回归通过。旧存档首次续跑遇到可重试错误后仍能继续，摘要按新问题重新确认，业务答复继续按原 ID 使用。四模块 `make test` 和 `make cover` 通过，核心包合并覆盖率 88.5%。 [验证记录](backlog-audit.md#最近复审发布与设计入口) · [升级约定](../release/next.md)
- ISSUE-48 与 ISSUE-64 同属子图接入。ISSUE-52 的外部测试归 A02、A04–A06。ISSUE-93、97、98 和 99 的现存范围及去向见[编号台账](backlog-audit.md#issue-6599-逐号核验)。
- MCP 的 POST SSE 与会话生命周期已完成。余下的独立推送、续订及多媒体分别保留为 C01、C02。
