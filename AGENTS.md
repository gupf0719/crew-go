# crew-go 开发指南

crew-go 是 Go 原生 LLM Agent 编排框架。九种编排统一编译成 channel-based BSP 图，由 `compose_v2` 执行。按任务范围阅读下方入口，核对当前实现、公开契约和相应测试。

## 协作与项目约定

- 代码注释和 godoc 使用中文。文档默认用 Markdown，任务指定其他格式时按任务要求交付。
- 项目处于上线前开发阶段。设计需要调整时，直接更新 API 和仓内调用方、测试、示例及文档，避免为假定的旧调用方引入弃用提示、兼容开关或双轨实现。
- `modules/` 是本项目自有基础类型，独立演进。`schema` 对 `modules/common` 的类型用别名复用。编排通过 Graph 实现，复用引擎的调度和存档机制。
- `compose_v2` 是通用图引擎，依赖 `schema` 等底层契约。`agent_v2` 在其上组合模型、工具、钩子、技能、提示词和存储。`a2a`、`a2a/server`、`agui` 在集成层，`a2a/registry` 是只依赖标准库的叶子，ZooKeeper binding 放在独立 Go 模块 `a2a/zk/`。各层需要捕获 panic 时共用 `internal/panicerr`。
- 遇到实质性契约选择，先写清哪些调用方受影响、默认行为如何变化，沿现有决策或用户指定的方向推进。调研先验证提问里的前提，再比较具体做法。日志、实际调用和可复现探针优先于行为推断。

## Go 代码

- 错误处理用 early return。忽略 error 时写明原因。错误消息以小写开头且末尾无标点，带上下文包装时用 `%w`。调用方需要 `errors.Is` 判断时才定义哨兵错误。
- 接口定义在使用方。包名用单数名词，避免 `util`、`common`、`misc` 一类万能包。遇到循环依赖时先调整分层。
- 缩写全大写，例如 `ID`、`URL`。构造函数用 `New` 前缀，选项函数用 `With` 前缀。`ctx` 是首个参数，沿调用链传递，避免放进结构体字段。`context.WithValue` 用私有类型作为 key。
- 谁启动 goroutine 谁负责退出和等待。含锁结构体避免拷贝，锁留在内部。已知容量的 slice 预分配，热路径拼接优先用 `strings.Builder`。频繁分配的小对象在确认收益后考虑 `sync.Pool`。
- 公开入口核对 nil、空结果和 interface typed nil，优先复用所属包的校验。插件或模型返回 `(nil, nil)` 时检查业务成功条件。断路器要能在 cooldown 后半开恢复。

## 测试与交付

先按改动范围选检查，从最小可复现用例开始，再跑适用的模块测试和专用冒烟。命中多行时叠加相应检查。

| 改动范围 | 交付检查 |
| --- | --- |
| 文档或注释 | 检查改动文件的链接、符号和行为描述，按需运行 `commentaudit` 和 `go doc`。 |
| 普通 Go 行为 | 先跑受影响包的单测。跨模块或核心契约改动用 `make test` 覆盖全部模块。 |
| 示例或 demo | 本地跑完一条实际路径。交互式程序用 `printf '行1\n行2\n' \| go run ...` 喂 stdin。 |
| 共享状态、会话、channel 或同步原语 | 在受影响包实际跑 `go test -race`，按下方审计规则检查。 |
| Checkpoint、嵌套、Resume、namespace | 跑 `go run ./example/orchestration/nested_smoke` 和 [plan_chat_web mock](doc/description/smoke-test-recipes.md#2-plan_chat_web-mock--hitl-resume30-秒)。前者直接查存档清理，后者验 SSE 中断、续跑和 HTTP 终态。PlanAgent 和 GraphAgent 的嵌套覆盖见 [HITL 测试矩阵](doc/v2/hitl-resume.md)。 |
| `a2a/` 协议接入 | 跑 `go run ./example/a2a/hitl_smoke` 和 `go run ./example/a2a/llm_smoke`，后者需要可用的 Qwen 凭据。 |
| 持久化或服务发现 backend | 跑对应 conformance 套件，覆盖实际 backend。 |

- 用标准库 `testing` 和手写 interface stub。公开 API 用黑盒 `package xxx_test`，未导出逻辑用同包 `*_internal_test.go`，避免为测试暴露内部函数。纯输入输出或边界枚举可用表驱动，需要不同 stub 的错误路径拆为独立测试。
- 新增或改变公开 API 的行为、改动关键内部逻辑或修复具体缺陷时，配套测试锁住相应契约。HITL、Resume、压缩等跨层回归使用真实 `BasicAgent`、`NewLLMPlanner` 等生产组件，按需替换模型或存储边界。stub 要读取并断言下游收到的输入。
- `make test` 跑根模块及 `redis/`、`sql/`、`a2a/zk/` 子模块。根目录的 `go test ./...` 只覆盖根模块。并发回归用屏障和短超时搭出真正重叠的执行。
- 真 LLM 或外部服务用例放在 `example/` 或相应后端包，并用 `//go:build integration` 隔离。`make test-integration` 跑 `example/` 下的 LLM 集成用例，默认用 `model/qwen` 的 `qwen.NewTestClient` 和 `qwen.TestModelCandidates`，凭据从 `QWEN_API_KEY` 读取。需要自动切换候选时沿用 `fallback.New`。详见 [测试规约](doc/description/testing-conventions.md)。
- 每个相关提交前跑对应冒烟，完整命令和预期输出见 [冒烟测试速查](doc/description/smoke-test-recipes.md)。交付时记录实际检查及结果，以及受环境或凭据限制的项目。相关提交说明记录实际通过的 race 检查。完整 CI 门禁见 [GitLab CI](.gitlab-ci.yml)。

### Backend 一致性

自定义或修改持久化、服务发现 backend 时，针对实际 backend 跑对应 conformance 套件。

| 接口 | 套件入口 |
| --- | --- |
| `store.Store` | [`RunStoreTests`](store/conformance/conformance.go) |
| `compose_v2.Checkpointer` | [`RunCheckpointerTests`](compose_v2/checkpointer_conformance/conformance.go) |
| `agent_v2.HistoryStore` | [`RunHistoryStoreTests`](agent_v2/history_conformance/conformance.go) |
| `registry.Directory` | [`RunDirectoryTests`](a2a/registry/conformance/conformance.go) |

- **Store** 在 `Put` 入口用 `store.NormalizeValue` 归一化。客户端过滤复用 `store.ValidateFilter` 和 `store.MatchFilter`，SQL 下推参考 [`JSONField` 系列方言方法](sql/dialect.go)。范围查询的非法 filter 值在空 namespace 也报错，存量值类型不匹配时该行跳过。`Batch` 逐项 best-effort。namespace 与 key 逐字节区分，`ListNamespaces` 按字节序排序，深度截断去重后再分页。
- **Checkpointer** 保留全字段往返，重点核对挂起问题、已完成分支顺序、延迟发送及 payload。`SaveNS` 对当前存档原子判断 GraphID 冲突，放行时整份替换，拒绝时保留原档。`(sid, ns)` 逐字节区分。`DeleteSubtree` 清自身和 `/` 后代，保留 `#`、`@` 兄弟执行。
- **HistoryStore** 用 `Owner` 和 `ConvID` 隔离，按时序追加，重复记录照样保留。返回快照并完整往返 tool calls 和 annotations。分页从最近记录往前，追加时游标稳定。按 `MessageID`、`Task`、`Agent` 联合过滤，匹配的重复记录全部返回。
- **Directory** 按 namespace、service、agent name、instance ID 四元组区分。服务聚合发现同服务下的多个 agent，重复注册、幂等注销、默认值和 metadata 快照遵循 [注册发现规范](doc/description/a2a-registry-discovery-spec.md)。语义搜索、真实 wire format、性能和 binding 特有行为另行验证。

## 跨层改动与审计

- 先用一句话写清改动要守的不变量，再列对抗场景。为复杂链路画时序与状态所有权图，标出每个值的 producer、consumer 和寿命，区分单次调用、Run、Session 和 checkpoint。审计错误、取消、重试、嵌套、并发、恢复和清理路径，检查实际的消息和事件输出。
- 改协议前用 `rg` 对账全部生产入口与读写点，包括普通节点、钩子、批内分支和提前返回。新守卫放到共同的决策点，逐条确认哪个入口会命中、哪些原有合法调用会受影响。增删字段时检查序列化往返，特别是零值、nil 和空容器。删改 API 后搜索全部调用、测试、godoc 和活文档。删判据时也搜旧规则的措辞及代码形状。
- 把判定收敛到拥有权威状态的一处，由该处传出判定结果。一个字段若同时承载身份、归属或授权等不同语义，拆开各自的载体，用至少两个调用者、任务或批次验证隔离。公开守卫只要求调用方根据自己能掌握的信息行动。定义 backend 契约时以所有实现都能满足的统一语义为准。同一规则遇到第二个反例时重新审视规则和所有权。
- 增加编排层 fail-fast 时，分别验证是否重试、是否会覆盖或清理原有挂起档、错误能否定位到 namespace、节点及违规值。确定性协议违规沿用 `compose_v2.ErrRunAborted` 的语义，清理前核对实际退出路径是否拥有存档槽。为编排配置 `MaxRetries > 0` 时显式提供 `ShouldRetry`，或使用 `compose_v2.SimpleRetry` 等现成配置，并检查整次调用重放的副作用。

### 回归测试的对照组

- 至少有一条正向断言钉住产出、调用次数或状态变化。选择缺陷发生时确实会变的量，例如工具体实际执行次数。避免固定 stub 输出、碰巧相同的过滤位置、错误文案触发其他兜底，或宽泛断言被另一道守卫满足。
- 先确认旧行为能复现。对关键回归在隔离目录临时撤掉修复，实际跑出预期失败，再确认修复版通过。声称跨层行为保持一致时，用同一个探针对比前后两版输出。
- 对照组固定无关身份维。派生 ID 优先在同一 Run 内比较，或明确固定生成它的任务身份。测试 fixture 保留框架本应自行推导的数据。验跨任务泄漏时让任务 A 中断后放弃，确认 `ListPending` 仍有残留，再启动任务 B。

### 审计触发与收尾

- 共享状态、并发原语、Checkpoint/Resume 或破坏性 API 的高风险改动逐次独立审计。多个相关提交完成后再审整体设计一致性。改写既有关键断言也按高风险行为检查。纯文档、拼写、清理死代码或带测试的单点修复按实际范围验证。
- 独立复审按本次风险分派单一视角。提供已证不变量、现有测试、关键代码和可复用 helper，要求审计 agent 自行完成该视角的检查，限制继续派生。行为发现用探针复现，文档发现用源码核实，再决定是否修复。临时探针使用 `zz_` 前缀，验证结束后清理。
- 提交说明和自评写本项目的问题、决策、理由和实测证据。框架对照结论记录在 [三方对照文档](doc/description/three-way-comparison/README.md)。
- [Claude Code Stop hooks](.claude/settings.json) 按提交检查 audit、cross-verify、注释 drift 和未推送提交说明。Codex 对本次实际 diff 运行对应检查，包含新建文件。若已有提交并使用 Claude Code 的 marker，在检查真实完成后按对应 SHA 标记。触发条件和命令见 [原审计流程](CLAUDE.md#强制执行机制stop-hook--audit-marker)。

## 容易跨层失效的契约

### Checkpoint、HITL 和嵌套

- 任务身份来自 session 的 `RunID`，`NextTask` 切换任务。调用方确定放弃旧挂起任务时，显式 Abort 或在新 Run 声明 `agent_v2.WithAbandonedTasks`。执行中的任务里另开或续跑独立任务，先用 `agent_v2.DetachTask`。调用子 agent 经 `agent_v2.RunSubAgent`，让框架派生 namespace、归属和完成记录。编排内部成员和决策 agent 走 [`runMember` / `runDecisionAgent`](agent_v2/member_dispatch.go)。并行嵌套用 `ParallelAgent` 或 `compose_v2.SendFrom`，由引擎分配实例身份。
- 嵌套调用身份由位置、输入和同输入出现次序等维度组成。输入含每次变化的值时，给同一次调用稳定的 `agent_v2.WithCallKey`，或在 `AgentAsTool` 使用 `agent_v2.WithAgentToolCallKey`。续跑时重新经过已完成调用的入口，框架会重放记录及其路由或转交信号。校验失败后确实需要重新产出时，发起新调用，或让条件边用 `CondFailRerunFrom` 并将该调用标为 `agent_v2.WithRerunnable()`。详见 [`RunSubAgent` godoc](agent_v2/run_subagent.go)。
- 业务提问用 `compose_v2.Ask`，由框架铸造 `InterruptID`，答案用 `agent_v2.WithResumeUserInputs` 按 ID 配对。合法中断至少含一个 query。直接构造 `InterruptError` 时自填 ID 仅作展示标签。条件边只读 channel 做判断，需要 HITL 时由上游节点提问并写入答案。条件失败默认 `CondFailAbort`，明确需要重算上游时选择 `CondFailRerunFrom`。
- 载入由存档是否有可续工作、是否属于当前任务等条件决定，`Status` 用于报告。显式 Resume 找不到可续工作时按 `errors.Is(err, compose_v2.ErrNoPendingCheckpoint)` 判断，无档时 Abort 保持幂等。`WithInitialWrites` 交给引擎决定何时注入。配 Checkpointer 的 `compose_v2.Run` 要有调用方 deadline，`WithNodeTimeout` 仍依赖 `NodeFunc` 响应取消。参见 [Checkpoint](doc/v2/checkpoint.md) 和 [HITL 与 Resume](doc/v2/hitl-resume.md)。
- 超步业务失败后等待兄弟分支，Resume 只重跑尚未成功的分支。工具批次部分中断时，通过持久化的工具账重放已完成调用的结果和信号，进程在账本落盘前崩溃等场景仍需业务幂等。成功后清理本次执行拥有的存档子树，兄弟执行保留。核对 GraphID、任务身份和 namespace 隔离。
- 超步退出错误优先级为终局失败 → 存档不可续 → 存档未写成 → 中断 → 首个业务错误。单个中断保留原始错误链，多个中断合并查询。各分支详细错误从 `OnNodeEnd` 获取，避免把所有分支的错误 join 进出口并改变 `errors.Is` / `errors.As` 的判定。详见 [超步失败语义](doc/v2/channel-engine.md#74-fan-out-失败语义表)。

### 钩子与流式事件

- 改 BasicAgent hook 前读 [四节点实现](agent_v2/basic_agent.go) 的 `preambleFn`、`makeModelFn`、`makeToolsFn`、`postambleFn`，确认触发时的消息视图。`OnAgentStart` 在 Run 或 Resume 入口触发，成功后注册一次 `OnAgentEnd`。模型钩子按轮次，工具钩子按 tool call。同一工具批次内，开始和结束钩子分别串行，工具体可并发执行。
- 改写按 [`hook.Hooks` 字段契约](hook/hooks.go) 执行。消息改写保留 annotations 与 tool call/result 配对。`OnModelStart` 的 `Preamble`、`Messages` 是压缩前的不同输入，`OnAgentEnd` 的消息供观测。钩子返回的 ctx 从入参派生，`OnToolStart` 的返回值只作用于当前工具调用。
- `OnToolStart` 改写的工具名和参数在批次首次派发时定下，续跑沿用，短期凭据走钩子返回的 ctx 或工具中间件。`OnToolEnd` 整体替换结果时显式保留正确的 `Success`。计数、计时和计费按 `hook.ModelResponseReusedKey`、`agent_v2.ToolReplayedExtraKey` 等标记识别重用。流式 chunk 经事件钩子实时投递，最终聚合消息才进历史或审计。详见 [钩子协议](doc/v2/hooks.md) 和 [流式协议](doc/v2/hooks-and-streaming.md)。

### 路由和转交

- Routing 和 Handoff 优先消费授权执行提交的上行信号。失败、中断或被否决的未提交执行丢弃信号，已完成执行恢复时重放信号。顺序执行以后一次决定为准，同一工具批次内以最先声明且成功的决定为准，同一超步多个成员做决定按终局冲突处理。Routing 无信号时依次解析 `route_to` 参数、`route_target` annotation、去空白后完整匹配的文本，最后走兜底分支或 `RouteUndecidedError`。Handoff 转交走 `transfer_to_agent` 或 `HandoffTo`，框架按授权和候选验证。见 [路由实现](agent_v2/routing_agent.go) 和 [转交协议](doc/v2/handoff.md)。

### 上下文压缩

- 每轮按 MicroCompact → AutoCompact → LLM 调用（超长时 ReactiveCompact 重试）→ tools 执行。StoreMemory 在 preamble 从 Store 注入跨 Run 记忆。agent 前言走独立的 `Preamble`，对话压缩只处理 `Messages`。
- 用 `compose_v2.GroupMessagesByAPIRound` 保 tool call/result 在同组，用 `compose_v2.AdjustIndexToPreserveAPIInvariants` 保 RunID 块连续。保留业务 system 消息。system 夹在一对工具消息中时保留 system 本身，该组其余内容仍可压缩。裁剪后首条是 assistant 时按既有逻辑补 user marker。
- AutoCompact 在可压缩量不足时跳过。摘要超预算、缩减不足或调用失败时记断路器并沿用原消息。结果作为单 Run 内备忘或跨 Run 压缩边界复用，保留原消息流。ReactiveCompact 只改本轮重试送模视图。变更时分别核对当前调用、下一轮和跨 Run 三种视图。详见 [上下文压缩](doc/v2/context-compaction.md)。

### 可观测性

- `obs.Install` 同时装 agent 和编排钩子。已有业务钩子作为 `obs.AgentHooks`、`obs.OrchHooks` 的内层，确保开始阶段的拒绝、中断或 panic 能收尾。若 agent 与请求 ctx 分开构建，分别用 `agent_v2.WithHooks(obs.AgentHooks())` 和 `agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())`。
- `crew.outcome` 区分 error、timeout、cancelled、blocked 和 success，只有 error、timeout 标 span Error。取消以调用方 ctx 或软取消为准。模型答复复用跳过重复 chat span，工具重放不计执行时长，审批拒绝标 blocked。放弃挂起任务时调用 `obs.CleanupSession`。A2A 通过 metadata 和 `obs.InjectToMetadata` / `obs.ExtractFromMetadata` 传 W3C trace context。
- trace 沿 ctx 透传，结构化日志用 `slog` 和字段常量。OTel SDK、导出器和指标基数由应用配置。内容采集默认关闭，文本先脱敏再截断，合法 JSON 在脱敏前还原字符转义，大段内容留在 span 侧。属性和指标名集中在 [`observability/semconv.go`](observability/semconv.go)，`gen_ai.*` 按语义约定，框架维度用 `crew.*`。变更时同步 [可观测性文档](doc/v2/observability.md)。

## 文档与查阅入口

- 本文件收录执行规则和跨包不变量。具体行为从当前源码、公开 godoc 和回归测试核对。长篇 API 表、决策背景和复现脚本各自收在对应专题，规则改动时更新该处并保留入口。
- 写设计文档、README 或超过 30 行的 godoc 前，按 [doc-writing](.claude/skills/doc-writing/SKILL.md) 核对实际符号、签名、默认值和行为分支。改导出符号或相邻声明后用 `go doc <包> <符号>` 检查注释归属。同一契约变更同步检查示例、活文档和速查表。
- 大规模注释改动、公开 API 重命名或删改后，按 [comment-audit](.claude/skills/comment-audit/SKILL.md) 的流程校验。增量机械检查用 `go run ./.claude/scripts/commentaudit/main.go -root . -files path/to/file.go,path/to/doc.md`，含测试注释时加 `-include-tests`。外部符号写包限定名，逐条读语境确认候选，再核对行为描述与实际实现。
- 真正开放的工作登记到 [待办](doc/code/todo.md) 或 [问题清单](doc/issue/issues.md) 并链接回相关设计。决定后同步清理正文的旧状态。文档治理或大功能合并前，扫描设计正文的待定、待办、未实现等标记，逐条核实。
- 了解架构从 [架构导航](doc/description/architecture.md)、[v2 架构总览](doc/v2/architecture-overview.md) 和 [v2 API 迁移速查](doc/v2/migration-history.md) 开始。使用侧示例见 [使用指南](doc/v2/usage-guide.md) 和 [example 索引](example/README.md)。
- 设计改动涉及 Checkpoint、子图、Interrupt/Resume、寻址或会话历史时，先读 [三方对照与决策记录](doc/description/three-way-comparison/README.md) 对应主题。实质性决策变更同步更新相关记录。
- 按需查 [CLAUDE.md](CLAUDE.md) 的[审计案例](CLAUDE.md#多层系统-bug-审计方法论)、[核心类型速查](CLAUDE.md#核心类型速查)和 [HITL 嵌套约定](CLAUDE.md#编排-hitl-矩阵)。
