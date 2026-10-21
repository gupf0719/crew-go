# SDK 版本控制 / API 稳定性——eino · langgraph 对照 + crew-go 决策

> 2026-06 调研（本地源码：`/Users/gupengfei/program/go/src/{eino,langgraph}`）。
> crew-go 是 Go 原生 SDK、**未上线不留兼容路径**——本篇定"何时冻 1.0 + 怎么划公私边界"。

## 一、两家做法对照

| 维度 | **eino**（Go，CloudWeGo）| **langgraph**（Python）|
|---|---|---|
| 当前版本 | v0.8（**从未发 1.0**）| 核心 `langgraph` 1.2.5（已 1.0 Production/Stable）/ `langgraph-sdk`·`cli` 仍 0.x / `checkpoint` 4.x |
| module 拆分 | 单仓单 module（根 `go.mod` 无 `/v2`，v0.x 不需要）；**生态拆独立仓**（`eino-ext` 放各 LLM client/backend、`eino-examples`）| monorepo **8 包各自独立版本号** + 包间 `>=x,<next-major` 依赖区间锁 |
| 公私边界 | **`internal/` 编译器强制隔离**（47 个非测试文件 import `internal/*`：图执行核心 `internal/core`、序列化、泛型工具）；公开包做薄 facade | `_internal/` 物理隔离（docstring 声明"不保证稳定"）+ `__all__` 导出控制（32 文件）+ `@deprecated`/`@beta` 装饰器 + `py.typed` + ruff banned-api |
| 版本号 | SemVer + **pre-release 通道**（每 minor 前走 `-alpha.N`→`-beta.N`→正式）| SemVer；**稳定性按包分级**（核心稳定、sdk/cli 故意停 0.x、checkpoint 迭代到 4.x）|
| 破坏性变更 | 攒进 **minor bump**（不在 patch 破坏）+ 官网逐版 migration guide（不在仓内）| **deprecated-in-vX → 下个 major 才删**（跨整个 1.x，超保守，最少一个 major 缓冲）|
| Deprecated | 42 处 `// Deprecated:`（Go 惯例 + `[Symbol]` 交叉引用 + 解释原因）；**41/42 不给移除时间线**（pre-1.0 软弃用）| 22 处运行时 `DeprecationWarning` + 14 处 `@deprecated`；**删除时间机读**（`expected_removal=(2,0)` 字段 → `__str__` 自动渲染"Deprecated in V1.0 to be removed in V2.0"）；module-level `__getattr__` 软搬迁（旧 import 仍可用 + warn）|
| beta/experimental | 无独立标记（靠 `internal/` + alpha/beta tag）| **独立第三态**：`@beta` 装饰器；DeltaChannel 精确框定"已写数据保证可读、但这 3 个接口/blob 形状 not yet stable"（cp wire-format 不稳定声明）|

**一句话**：两家都在/曾在 v0.x 阶段保留破坏性自由；公私边界都靠**编译器/物理强制的私有区**（不靠 godoc 自觉）；deprecation 是 1.0 后的事，langgraph 的"机读删除时间 + 软搬迁"比 eino 的"含糊 future release"更可学。

## 二、对 crew-go 的决策（2026-06）

1. **保持 v0.x 破坏性自由，别急 1.0。** "未上线不留兼容"= 标准 v0.x，与 eino（至今没 1.0）/ langgraph（0.x 一路过来）一致。1.0 是"想清楚公开 API 才冻"的承诺，不必赶。

2. **现在做 `internal/` 公开面收敛（最高 ROI、越拖越贵）。**（2026-06-29 实施后改判：只做了 Phase 1——小写 6 个纯内部 helper，`10da9b23`；`internal/` 抽取价值边际，Phase 2 不做，见 §四补。） 把"业务永不该 import 的引擎内核"下沉 `internal/`，用编译器强制划界——这样将来"public API = 非 internal 导出"自动成立，免维护一份散文 API 清单。langgraph 是 1.0 前才专门做公私分离重构（commit `8c4e698c5`），迁移面已很大；crew-go 趁未上线现在划线成本最低。**具体 scope 见 §四**。

3. **多 module 拆分方向已对，继续。** `a2a/zk`/`redis`/`sql` 已是隔离三方依赖的独立 go module。长期可学 eino-ext 把 model client（openai/qwen/deepseek）外移让核心 `go.mod` 依赖收敛——非紧急。

4. **Deprecation 机制留到 1.0 后。** pre-1.0 直接删（现规约对，别积累 Deprecated）。真到 1.0：用 Go `// Deprecated:`（gopls 识别）+ **删除版本写进注释**（学 langgraph 机读式 `// Deprecated: 自 vX 起改用 Z，将在 vN.0 移除`，别学 eino 含糊"future release"）+ 可选 `internal/deprecation` 运行时 `slog.Warn`（`sync.Once` 防刷屏）。

5. **现在给 cp 持久化格式子系统加"wire-format 不稳定"声明。** Durability / `CompletedFrontier`·`DeferredSends` 序列化 / skill-action `SkillExecutionResult` / ToolSearch 揭示集——这些有 on-disk / 跨进程格式的，学 langgraph DeltaChannel 在 godoc 标"**序列化格式可能变，业务勿依赖 wire 细节**"，让业务区分"deprecated（要迁）"vs"beta（别依赖内部表示）"。

## 三、决策记录

| 项 | 决策 | 优先级 | 理由 |
|---|---|---|---|
| `internal/` 公开面收敛 | ~~DO（现在 scope，单独一轮实施）~~ **Phase 1 已做（`10da9b23`），Phase 2 评估后不做**（见 §四补） | P1 | 编译器强制公私边界；未上线现在划界最便宜；1.0 自动受益。见 §四 |
| 冻结 1.0 | **暂不** | — | 仍在快速演化，公开 API 未稳定；eino 至今没 1.0 是合理范本 |
| v0.x 打 tag + minor 攒 breaking | **✅ 已起（v0.1.0，2026-06）** | — | 多 module 各打 tag，见 §五约定；已有 conventional commit 可 git-cliff 生成 changelog |
| model client 外移独立仓/module | **暂缓** | P3 | 收敛核心依赖，但非紧急；先做 internal/ |
| Deprecation 机制 | **defer 到 1.0** | — | pre-1.0 直接删；1.0 用机读删除时间（langgraph 式）|
| cp wire-format beta 声明 | **可做（加 godoc）**——未做，已进中心 backlog | P2 | 有 cp/跨进程格式的子系统标"序列化格式可能变" |

## 四、internal/ 隔离 scope（设计，2026-06 投研）

> `compose_v2`（~312 导出）+ `agent_v2`（~286 导出）逐符号**外部引用分析**后的迁移设计（判据=包外真实代码引用，排除 comment/godoc）。
> **硬约束**：`redis`/`sql` 是独立 go module（`crew-go/redis`/`crew-go/sql`）**无法访问 `crew-go/internal/`**——它们用的符号（`Checkpointer` / 序列化 `Marshal*`/`CPStatus*` / sentinel `Err*` / `ParentNS` / `IsNSInSubtree` / `NSSubtreeSeparator` / `SimpleRetry`）**必须留公开**，是迁移上界。

### 分三相（风险递增）

**Phase 1 — 包内收敛（零外部影响，立即可做）**

仅本包内 + 自测用、跨包零真实引用 → 下沉 `compose_v2/internal/` + `agent_v2/internal/`，**改 0 个外部文件**：
- `compose_v2/internal/`（~12）：emit helper（`EmitCpEvent`/`EmitPlanReplanEvent`/`EmitCpCleanupHelper`/`EstimateTokensFromAPIUsage`）+ 内部错误（`ErrInvalidNodeIDForNS`/`GraphNodeError`）（原列 legacy 兼容族 `ProcessState`/`StateFactory`/`GetLocalState`/`WithStateFactory` 已整体删除——channel 是唯一 state 模型，见下文 §四补）
- `agent_v2/internal/`（~45-60）：message annotation/marker 常量（`StoreInjectedAnnotation`/`*CompactSummaryAnnotation`/`SkillBodyAnnotation` 等 ~12）+ prompt 模板（`NoToolsPreambleCN/EN` 等）+ skill 选择枚举值（`SkillSelectionStrategy*`）+ 内部工具工厂（`NewHandoffTransferTool`/`NewRouteTool`/`NewUUID`）+ listing 预算常量
- **净收益 ~55-70 符号下公开面、零迁移成本。已确认 `makeModelFn`/`makeToolsFn`/`buildInitialMessages` 本就未导出，无需动。**

**Phase 2 — 模块级框架内部（主体价值，需改 agent_v2 导入）**

6 编排的低阶 builder 全家桶（`SequentialAgentBuilder`/`ParallelAgentBuilder`/… ~75-85 符号）——**仅 agent_v2 wrapper 用**（业务用高阶 `agent_v2.SequentialAgent`，从不碰低阶图）→ 下沉 `crew-go/internal/`（模块级，agent_v2 可访问、业务挡住）+ ask_user/审批/寻址内部协议（`InterruptSite`/`NextInterruptIndex`/`WithCurrentToolCallContext`）。
- **最低风险子集先动**：`*StepFn` 类型名（`SequentialStepFn`/`LoopBodyFn` 等，跨包**零引用**，连 agent_v2 都靠 builder-chain 推断不显式命名）——移走零编译影响。
- 9 个 `type X = compose_v2.X` 别名（`ParallelMerger`/`Reflector`/`EvalFunc` 等）：移 internal 后 agent_v2 仍可 alias 重导出（公开身份=`agent_v2.X`，业务不受影响）。
- 成本：agent_v2 import 重写（机械但触多文件）。

**Phase 3 — 决策门（先定产品意图，别凭引用计数下沉）**

两类灰区，下沉前**必须先决策**：
1. **observability 事件契约**：`CompactionAction`/`FanoutAction`/`RouteDecisionAction` + 一串 enum。**【决策 2026-06：收敛 internal（B）】**（2026-06-29 §四补改判：不做） —— 业务 0 消费（唯一 consumer 是 `observability/hooks.go` metric 适配器，example/a2a 只取 `chunk`）+ 弱 `map[string]any` 契约 + OTel 标准侧（压缩仅布尔 span 属性 `gen_ai.conversation.compacted`、fanout/route 标准无对应、6 框架 hook 全纯观测）→ 移 `crew-go/internal/`（observability 经模块内访问、业务挡住）+ 撤 godoc"业务订阅"措辞。真有业务订阅诉求时做成 proper typed 事件（eino 式），非现弱 map。
2. **"声明但零 caller"**：`SoftCancel` 族 / `RunSubAgent` / `AutoCompactRun`/`MicroCompactRun`/`ReactiveCompactRun` / `NewSharedChildSession` / `IsPromptTooLongError`。**【决策 2026-06：KEEP-PUBLIC】** —— 有意的公开扩展点，"0 example"是缺 demo 非过度暴露；**补 example 而非藏 internal**（文档即契约，藏了文档 stale）。（2026-09-18 核验：example 未补，已进中心 backlog；`RunSubAgent` 已有 example 调用。）

### 建议执行序
- **Phase 1 立即做**（零风险、~55-70 符号、改 0 外部文件）——最便宜的即时收益。
- **Phase 2 单独一轮**（agent_v2 import 重写）。
- **Phase 3 决策已定**（2026-06）：observability 事件 → 收敛 internal、文档化未用 feature → KEEP-PUBLIC（补 example）。三相可随时实施（单独一轮）。（2026-06-29 改判：observability 事件不移 internal；KEEP-PUBLIC 那半的补 example 未做，已进中心 backlog。）

### 四补：2026-06-29 intent-review 修正（实施 Phase 1 后回填）

> **⚠️ 上面 2026-06 投研严重 over-scope**。真动手实施时（intent-review + pilot）发现：**逐符号外部引用分析（grep usage=0）≠ 可内化**，必须叠加 **intent 层**判断。真正能干净小写的只有 **~6 个**，远非投研说的 ~55-70。结论：**internal/ 抽取价值边际，收在 Phase 1；将来勿再照 ~23/~55 误判。**

**intent vs usage 鸿沟**（grep-agent 查不到、必须人读语境）——usage=0 的导出仍可能是**有意公开 API**，三类全 KEEP-PUBLIC：
1. **error 类型 / sentinel**（`GraphNodeError` / `ErrInvalidNodeIDForNS`）：typed error 存在意义就是让 caller `errors.As`/`errors.Is`；`graph_node_error_test.go` 注释明写"caller 可 errors.As 定位 NodeID"。0 当前调用 ≠ 内部。
2. **文档化公开选项**（当时举的是 `NewRouteTool`——2026-10-08 起已不导出：路由能力改成授予，导出的构造函数正是「谁都能给任意 agent 装上 route_to」的来路）：CLAUDE.md 列为公开 API，藏 = 破坏文档契约。（原此处举的 `ProcessState`·`StateFactory`·`GetLocalState`·`WithStateFactory` 族后已整体删除——channel 唯一 state 模型，不再作示例。）
3. **测试当业务低阶 API 用**（6 编排低阶 builder + `BasicAgentBuilder`）：`r31_parallel_dedup_test` 注释"业务直接用低阶 API 时也防"、`r34` 用 `.Compile()` 取 `*Graph`；且转白盒会断**共享黑盒 helper**（`newStubModel` 等被一大批黑盒测试共用）级联——churn 远超投研估的"258 行转 package"。

**Phase 1 实落地**（commit `10da9b2`）：只小写 6 个纯内部 helper（0 黑盒测试 + 0 文档）——`estimateTokensFromAPIUsage`（compose_v2）+ `newUUID`·`newHandoffTransferTool`·`noToolsPreambleCN`·`noToolsPreambleEN`·`emitPlanReplanEvent`（agent_v2）。build+vet+单测+真 qwen TestAdv 全编排全绿。

**Phase 2 评估：不做。** observability 3 事件（`CompactionAction`/`FanoutAction`/`RouteDecisionAction`）虽 0 文档化、可移 crew-go/internal/（observability 同 module 能 import），但为藏 3 个 undocumented 类型新建 module 级 internal/ 包 + 重写 observability+2 测试+emit 码，scaffolding 成本 > 边际收益。

**2026-09-22 补：`RouteDecisionAction` 转为公开契约。** 带置信度的路由决策落地后，业务确实要订阅它来统计升级率与置信度分布（[route-decider.md](../../v2/route-decider.md) §2.7、§4.5），上面「业务 0 消费、0 文档化」对它不再成立。沿 Phase 3 第 1 条留的方向走了一步：载荷字段写进文档，并加带类型的取值函数 `RouteDecisionOf(ev agent_v2.Event) (*RouteDecisionAction, bool)`，业务与 observability 都经它取，不再各自对 `Metadata["action"]` 做类型断言。载荷仍经 `Metadata["action"]` 传，没有做成那一条设想的带类型事件，类型断言只是收到了一处。`CompactionAction` / `FanoutAction` 维持原判。

**核心教训**：compose_v2 的"大 API 表面"绝大多数是 **legit-public**（GraphAgent 业务引擎 API + redis/sql 后端 + 文档化选项 + caller error 类型），不是需要藏的内部。纸面 audit（2 agent 报"ready to code、~23 候选"）和真动手隔着 **intent vs usage 鸿沟**——pilot 才暴露。

### 四 · 三方 internal/ 对照（2026-06-29 grep 本地源码核实）

| 框架 | 隐藏机制 | 公开 API 控制 | 数据 |
|---|---|---|---|
| **langgraph** | 下划线私有模块（`pregel` 引擎 `_algo`·`_loop`·`_executor`·`_io`·`_checkpoint` 全私有）+ `_internal` 包 | **`__all__` 显式白名单**（32 文件逐符号策展）| 整个执行引擎 `_` 私有 |
| **eino** | `internal/` 编译器强制包（root + `adk/internal` + middleware）+ lowercase | Go 导出 + internal/ | internal/ 占 ~13% .go（跨子包共享私有）|
| **crew-go** | **Go 原生 lowercase（包内），无 internal/ 包** | 仅 Go 导出规则 | compose_v2 函数 38% 未导出；graph_loop 8 导出 / 6 未导出 |

**判断：**
1. **三方都藏执行机器，只是机制不同。** crew-go lowercase（graph_loop 的 computeNextTasks 等已小写）**没建 internal/ 包是对的**——私有符号都包内私有（lowercase 够），不像 eino 有跨子包共享私有才需 internal/ 编译边界。**架构差异**（crew-go 扁平 compose_v2+agent_v2 vs eino 多子包），非哲学差异。
2. **crew-go 与 langgraph 都暴露宽图构造 API**（GraphAgent / StateGraph 公开）——印证四补"低阶 API legit-public"。
3. **唯一不如 langgraph 处**：langgraph `__all__` 逐符号策展公开 API；crew-go"导出=公开"（Go 无 `__all__` 等价物）。compose_v2 325 导出函数大半是公开类型的方法，但没逐个策展——要更克制需 audit 325 函数，边际递减不急。

**结论**：crew-go"无 internal/ 包 + lowercase"对 Go 合理——机器藏了、引擎 API 该公开、跨包私有少，无需为隐藏新建编译边界。

## 五、版本化约定（2026-06 起，v0.1.0 首发）

crew-go 是 monorepo 多 go module，**各 module 独立打 tag**（Go 规则：子 module 用路径前缀）：

| module | tag 格式 | 首发 |
|---|---|---|
| `crew-go`（根）| `vX.Y.Z` | `v0.1.0` |
| `crew-go/redis` | `redis/vX.Y.Z` | `redis/v0.1.0` |
| `crew-go/sql` | `sql/vX.Y.Z` | `sql/v0.1.0` |
| `crew-go/a2a/zk` | `a2a/zk/vX.Y.Z` | `a2a/zk/v0.1.0` |

**bump 策略（v0.x 阶段，对齐 eino/langgraph）**：破坏性 → **minor**（v0.1.0→v0.2.0，v0.x 破坏性自由、攒进 minor 不在 patch 破坏）；修复 / 兼容新增 → **patch**（v0.1.1）；子 module 可独立演化版本；pre-release 可选 `-alpha.N`/`-beta.N`（`go get` 默认不拉）。

业务锁版本：`go get crew-go@v0.1.0` / `go get crew-go/redis@redis/v0.1.0`。一键打全 module tag：`make tag VERSION=v0.1.0`（见 Makefile）。
