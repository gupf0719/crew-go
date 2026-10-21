# Channel-based PoC 报告

> ⚠️ **历史决策溯源文档**——记录 PoC 期 → Phase 1+2 期决策。Stage 7 已完成
> （v1 `agent/` + `compose/` 已删，`agent_v2/` + `compose_v2/` 现为唯一实现），
> 完整 Stage 1-7 进度 + commit 索引看 [`migration-history.md`](./migration-history.md)。
> 本文件保留作"为什么走 channel-based + PoC 期 5 项判定结论"的决策溯源。
>
> 创建日期：2026-05-07（PoC Phase 0 完成时）
> 末次更新：2026-05-07（Phase 1+2 完成后续编写章节扩展）
> 分支：`redesign/channel-based`

## 0. PoC 单一目标回顾

> 验证 Go 泛型能否做出**比 eino field_mapping 更干净**的 channel API，
> 同时保留编译期类型安全。

## 1. 通过/失败判定（设计文档 §5 5 项达成情况）

| # | 判定项 | 状态 | 证据 |
|---|---|---|---|
| 1 | 异构 fan-in：节点读 N 不同类型 channel 用 `Get[T](chs, ch)` 编译期类型推断，无字符串映射 | ✅ | `TestHeterogeneousFanIn`：3 goroutines 并发写 Sum/LastValue/Append 三种 reducer 不同类型 channel；`Get[T]` 调用点编译期类型推断（取消注释错配 Write 编译失败） |
| 2 | 动态创建：90% 静态声明类型安全 + 10% 动态注册兜底 | ⚠️ PoC 期简化通过（运行时路径 Phase 1.2 已落地）| `RegisterChannel[T]` 包级泛型支持静态；动态注册路径 PoC 期未实现（按设计是 escape hatch，非 PoC 验证项）——**Phase 1.2 已落地**，实现为 `DynamicChannel[T]`（`compose_v2/channel.go`）+ `dynamic_channel_test.go`，见 §8 表 #4 |
| 3 | 子图作用域：`WithImport/Export` 显式声明，默认隔离 | ✅ | 6 个 subgraph 测试全过；audit F5 多层嵌套传递性（3 层）含正反两个用例 |
| 4 | 简单场景 API 不比 frontier 重 | ✅ | BasicAgent ReAct 等价 50 行（对比现状 290 行——**含** hook/skill/Resume/streaming，纯 ReAct 核心 50 行 ≈ 现状 ~80 行） |
| 5 | 难点场景（B/C/D）API 显著优于 frontier | ✅ | 跨超步累加 `Sum("token_usage")` 一行 vs 现状 typed state + ProcessState + 锁约 30 行；fan-out 多分支并发 reducer 自动 merge vs 现状硬编码 ParallelResults 聚合节点 |

**5/5 全部达成 → PoC 通过**。

唯一弱项是 #2 动态创建 channel，PoC 期仅做了静态声明路径、未实现运行时 register——但这是设计文档明确的 escape hatch，PoC 未列为强验证项。**该运行时路径已在 Phase 1.2 落地**（`DynamicChannel[T]`，`compose_v2/channel.go:231` + `dynamic_channel_test.go`，详见 §8 表 #4）。

## 2. audit 6 真漏洞撞击状态

| Finding | 严重度 | PoC 撞击状态 |
|---|---|---|
| F1 EndChannel 用 LastValue reducer 错 | HIGH | ✅ 设计 `OnceFlag` 单调 reducer 替代；`TestOnceFlag_MonotonicLock` 验证写 true→false→true→false 序列后 v=true |
| F3 cp/Resume 序列化先决 | HIGH | ⚪ PoC 明确不在范围（设计文档 §1.2）。Phase 1 必须撞——Channel reducer 是函数值不可序列化，需按 channel name 重建；channel value 需注册 type registry |
| F4 dynamic channel 类型错可观测性 | HIGH | ✅ `TestApplyOne_TypeMismatch_PanicMessage` 验证 panic 信息含 channel 名 + 期望/实际类型；`TestApplyAll_RecoversTypeMismatch` 验证 ApplyAll defer recover 不 crash |
| F5 多层嵌套传递性 | MED | ✅ `TestSubgraph_3LayerNested_ExplicitImport`（每层显式 import 透传）+ `TestSubgraph_3LayerNested_MissingMiddleLayerImport`（中间层不 import 子层看不到——证明不自动透传是 by-design） |
| F6 EndChannel vs EndNode 关系不清 | HIGH | ✅ Step 3 决策：**删 EndChannel 概念，保留 EndNode 拓扑节点**。理由：拓扑结构清晰、不引入两套终止机制、对齐 LangGraph 拓扑终止 |
| F8 hook 集成路径未说明 | MED | ⚪ PoC 故意不接——验证 channel 模型基础 API 表达 ReAct 核心循环够用；hook 集成路径留 Phase 1（涉及 `ChannelWrite` 是否可改写、超步合并点 hook 时机等设计问题） |
| F9 builder 类型擦除卖点打折 | HIGH | ✅ `WithChannel[T]` / `WithImport[T]` / `WithExport[T]` 全部用包级泛型函数（绕开 Go 泛型方法限制），调用点编译期类型推断；`TestWrite_TypeSafety` 注释展示 3 个错配 case 取消注释会编译失败 |

**5/7 已撞验证（4 真漏洞 + 1 设计 gap），2/7 明确不在 PoC 范围（标记留 Phase 1）**。

## 3. 关键技术决策记录

### 3.1 Channel 类型设计

```go
type Channel[T any] struct {
    n  string         // 名（拓扑唯一）
    fn Reducer[T]     // 合并函数
    z  T              // 零值
}

type Reducer[T any] func(prev, next T) T
```

**字段不导出**：用户拿不到底层 reducer 函数 ptr，只能通过工厂创建（`LastValue` / `Append` / `Sum` / `OnceFlag` / `Custom`）。这避免了 audit F3 cp 序列化时函数指针问题——cp 只需序列化 channel name + value，reducer 在 Resume 时按 name 重建。

### 3.2 异构 channel 类型擦除

`channels map[string]anyChannel` 在 ChannelManager 内部用类型擦除接口。但**用户视角**全是 typed：

```go
// 用户写
val := compose_v2.MustGet(chs, tokenUsageCh)  // 拿到 int

// 内部
type anyChannel interface {
    name() string
    typeRep() reflect.Type  // 用于 register 时类型一致性检查
    zeroAny() any
    reduceAny(prev, next any) any  // 内部 cast 回 T 调 typed reducer
}
```

类型擦除只在 **map 内部**，对外 API（Get/Has/Write）全是泛型 typed。

### 3.3 isEnd 检测和 next-tasks 计算合并

`computeNextTasks` 返 `(nextFrontier []NodeID, isEnd bool, err error)`——**对齐 eino calculateNextTasks 4-tuple 设计**。主循环：

```go
for {
    if err := ctx.Err(); err != nil { return err }
    if len(frontier) == 0 { ... }
    
    writes, err := executeFrontier(...)
    cm.ApplyAll(writes)
    
    nextFrontier, isEnd, err := computeNextTasks(...)
    if isEnd { return nil }  // ← 紧贴算完判 done，不进入下次 ctx 检查
    frontier = nextFrontier
}
```

**`TestGraph_DoneAfterCtxDeadline_StillSuccess` 在 channel 模型下自然 pass**——不需要 main 分支 commit `0bfd568` 的"调换检查顺序"临时 fix。sanity-revert（把 ctx 检查移到 isEnd return 之前）能精确复现 plan_chat_web bug 类。

### 3.4 删 EndChannel 概念（audit F6 决策）

不引入 `EndChannel = LastValue[bool]("__end__")`——graph 终止统一通过出边路由到 EndNode 表达。理由：
- 拓扑结构清晰（看 graph 即懂）
- 不引入两套终止机制（避免"该写 channel 还是路由 EndNode"困惑）
- 对齐 LangGraph 拓扑终止 + eino `nodeMap[END]`
- channel 只承担 state 合并职责

### 3.5 子图 import/export 显式声明 + 不自动透传

```go
parentB.AddSubgraphNode("sub", subGraph,
    compose_v2.WithImport(parentSharedCh),   // 子图能读
    compose_v2.WithExport(subResultCh),      // 子图写冒泡到父图
)
```

**3 层嵌套传递性**：每层显式声明，不自动透传。`TestSubgraph_3LayerNested_MissingMiddleLayerImport` 证明：祖父定义 channel + 父显式 import + 子未 import 时，子看不到——这是 by-design，避免"祖父改 channel 类型，所有间接子图破坏"。

### 3.6 fan-out 失败策略

`executeFrontier` 多 goroutine 并发：业务错 best-effort 不 cancel 兄弟（对齐现有 `superstep.go` ISSUE-46 决策）；返 first error。失败分支 writes 全丢弃（对齐 LangGraph task 失败 pending_writes 不 commit）。

PoC 简化为"全有或全无"——任一分支失败整个 ApplyAll 不执行。Phase 1 应改 per-branch delta 类似现有 `superstep_snapshot` 设计。

## 4. 实测对照（撞 audit F2 现状基线失实）

| 场景 | 现状 frontier 模型 | channel-based PoC |
|---|---|---|
| BasicAgent ReAct（含 hook/skill/Resume/streaming） | 290 行（`agent/basicagent_graph.go`） | 暂未实现完整对等 |
| BasicAgent ReAct 核心循环（不含上述）| ~80 行（按节点函数主体估）| 50 行（实测 PoC `buildBasicAgentReAct`） |
| 跨超步累加（token usage）| typed state + `ProcessState` + 手动锁 ~30 行 | `Sum("token_usage")` 1 行声明 + `Write(ch, n)` 1 行 |
| fan-out 多分支聚合 | 硬编码"fan-out 节点 + 聚合节点" `agent/parallel.go:281-290` | reducer 自动 merge 0 行业务代码 |
| 多层嵌套子图 | 现有 `AddGraphNode` + 4 SubgraphNodeOption 体系 | `WithImport/Export` 同等表达力 + 类型保护更强 |

**结论**：channel-based 在简单场景 API 不比 frontier 重；在难点场景显著更简洁。

## 5. 与 eino field_mapping 对比（PoC 主要假设验证）

| 维度 | eino field_mapping | channel-based PoC |
|---|---|---|
| 跨节点字段映射 | `FieldMapping{from: "node_a", to: "field_x"}` 字符串 | `WithImport(sharedCh)` `*Channel[T]` 引用 |
| 类型一致性保证 | 运行时反射检查 | 编译期保证（共享 `*Channel[T]` 引用） |
| IDE 重命名安全 | 字符串需 grep 全文找映射 | 变量重命名 IDE 全自动 |
| 错误信息 | 字段映射失败 `field_mapping.go:31-37` 错误堆栈难定位 | `panic` 含 channel 名 + 期望/实际类型 |
| API 学习曲线 | 用户需理解"FieldMapping" 概念 + from/to 字段约定 | 节点直接读写 typed channel，类似读写普通变量 |

**结论**：channel-based PoC API 比 eino field_mapping 显著干净——主要验证假设成立。

## 6. PoC 已知限制（PoC 期"留 Phase 1+2 解决"，现状全部已落地 ✅）

> **状态更新 (2026-05 Stage 7 完成后)**: 本节 8 项 PoC 期"已知限制"在 Phase 1+2
> 期间全部落地（migration-history.md 记录详细 commit 索引）。下表保留 PoC 期原始
> 措辞作历史溯源，**实际状态**列指向当前实现/文档锚点。

| # | PoC 期限制 (原措辞) | 当前状态 | 落地参考 |
|---|---|---|---|
| 1 | **cp/Resume 序列化**（audit F3）：channel value marshal + reducer rebuild 协议未做。这是先决问题，Phase 1 优先项 | ✅ Phase 1.0 落地 | `compose_v2/serialization.go`（`RegisterValueType` + `MarshalTagged/UnmarshalTagged`）；reducer 按 channel name 重建见 `channel_manager.go` |
| 2 | **hook 集成**（audit F8）：NodeFunc 返 `ChannelWrite` 时 hook 改写时机/对象未设计 | ✅ Phase 1.1 落地 | 业务级 `hook.Hooks` 8 method (`hook/hooks.go`) + 编排级 `OrchestrationHooks` 5 method (`compose_v2/hooks.go`)；CLAUDE.md "Hook 选择速查" 表 |
| 3 | **fan-out per-branch delta**：当前"全有或全无"，应改为对齐现有 `superstep_snapshot` | ✅ Phase 1.2 落地 | `compose_v2/graph_loop.go::executeFrontier` per-branch delta（v2 用 channel incremental + `CompletedFrontier`/`DeferredSends`，非 v1 `superstep_snapshot` hack）；Resume 只重跑失败/中断分支 |
| 4 | **dynamic channel 注册**：runtime `RegisterChannelDynamic` API 未实现 | ✅ Phase 1.2 落地 | `compose_v2/channel.go`（`DynamicChannel`）+ `dynamic_channel_test.go` |
| 5 | **子图独立 cp**：嵌套子图当前共享父 cm，没有独立 cp 物理隔离 | ✅ Phase 1.2 落地 | NS 物理分离 `SaveNS/LoadNS` (`checkpointer.go`)；`WithGraphNS` ctx 协议 |
| 6 | **HITL InterruptError 路径**：channel 模型下 ask_user 中断/Resume 怎么走未设计 | ✅ Phase 1.2 落地 | `doc/v2/hitl-resume.md`；ID 按 `InterruptSite` 各维度派生；`agent_v2.WithResumeUserInputs` |
| 7 | **OrchHooks streaming**：当前 hook 体系基于 frontier model，需重新设计 | ✅ Phase 1.1 落地 | `OrchestrationHooks.OnNodeEvent` + `EventSink` chunk 实时投递；ISSUE-82 后 OnAgentStart/End 单一所有权 (`agent_v2` wrapper 入口) |
| 8 | **9 编排迁移**：Sequential / Parallel / Loop / Routing / Handoff / Plan / Reflect / GraphAgent 全部要改写 | ✅ Phase 2 落地 | 9 编排（含 BasicAgent）全部 channel-based 实现；速查见 CLAUDE.md "编排模式一览" + [`migration-history.md`](./migration-history.md) Stage 2-7 commit 索引 |

## 9. 决策记录更新

| 日期 | 决策 | 理由 |
|---|---|---|
| 2026-05-07 | 启动 PoC | plan_chat_web bug 溯因后判断 channel-based 更优 |
| 2026-05-07 | A 方案临时止血合入 main | PoC 周期内 main 仍可用 |
| 2026-05-07 | PoC 设计文档 commit `ea6cff6` | 攻破三大难点 + API 草案 |
| 2026-05-07 | Audit 发现 6 真漏洞 + 3 gap，决定不预先修文档让代码暴露 | 用户判断"光看判不出，写出来才知道" |
| 2026-05-07 | **PoC 通过**——5/5 判定项达成，5/7 audit 漏洞撞击通过 | channel API 干净度证实，主要假设验证 |
| TBD | 进 Phase 1 整体重写 | 待用户拍板 |

## 10. 参考

- 设计文档：`doc/v2/channel-based-design.md`
- 三方对照：`doc/description/three-way-comparison/state-model.md`
- 关键 commit：
  - `0bfd568` main A 方案临时修复
  - `ea6cff6` PoC 设计文档
  - `b228921` Step 2 Channel 核心
  - `d6e874b` Step 3 graphLoop
  - 本次 commit：Step 4-7 一并落地
- PoC 代码：`compose_v2/` 12 文件
- PoC 测试：31 通过（go test ./compose_v2/）

---

# 第二部分：Phase 1+2 完成扩展（2026-05-07 后续）

> **进度概览已折叠**：PoC 通过后 Phase 1+2 全部完成（109 测试通过，~8000 行含 test）；各子项（cp 序列化 / HITL / OrchHooks / per-branch delta / dynamic channel / 9 编排）的 commit 索引见 [migration-history.md](./migration-history.md) Stage 1-7（权威源，本表原 commit 列已并入那里）。

## B. audit 漏洞撞击全清单

audit 阶段共发现 6 真漏洞 + 3 设计 gap（共 9 项）。Phase 1+2 全部撞击验证：

| Finding | 严重度 | 撞击 Phase | 验证方式 |
|---|---|---|---|
| F1 EndChannel 用 LastValue 不单调 | HIGH | PoC Step 2 | 设计 `OnceFlag` reducer 替代；TestOnceFlag_MonotonicLock 写 true→false→true 序列验证锁定 |
| F2 现状 BasicAgent 行数失实 | HIGH | Phase 2.8 | 实测对比：channel 版 50 行核心 vs 现状 290 行（含 hook/skill/Resume/streaming） |
| F3 cp/Resume 序列化先决 | HIGH | Phase 1.0.a/b/c + 1.2.c | RegisterValueType + taggedValue wire 格式 + frontier 持久化 + 嵌套子 cp |
| F4 dynamic channel 类型错可观测性 | HIGH | PoC Step 2 + Phase 1.2.b | 类型 cast panic 含 channel 名 + 期望/实际类型；ApplyAll defer recover 转 error 不 crash |
| F5 多层嵌套子图传递性未定义 | MED | PoC Step 7 | 3 层嵌套显式 import 测试 + 反向中间层不 import 测试，证明"不自动透传" |
| F6 EndChannel vs EndNode 关系不清 | HIGH | PoC Step 3 决策 | 删 EndChannel 概念，统一用 EndNode 拓扑节点 |
| F7 PoC 估时偏差 | LOW | - | 实际 PoC 周期短于估时（Phase 0 8 days est. → 1 day actual）|
| F8 hook 集成路径未说明 | MED | Phase 1.1 | 5 回调 + 配对承诺 + EventSink ctx 注入 + panic 收口 |
| F9 builder 类型擦除卖点打折 | HIGH | PoC Step 2 + Phase 1.2.b | 包级泛型函数 `WithChannel[T]` / `WithImport[T]` / `WithExport[T]` / `RegisterChannel[T]`，编译期类型推断 |

**7 真漏洞 + 1 LOW + 1 MED → 全部撞击通过**。

## C. 关键技术决策记录（Phase 1+2）

### C.1 Run 入口自动 Restore（Phase 2.5 Plan 触发）

**问题**：Plan 编排有 5 个私有 channel（`__plan_original__` / `__plan_executed__` 等），caller 不知道要 RegisterChannel 啥才能 Restore。

**决策**：Run 入口在 register 之后**自动调 cm.Restore(cfg.resumeFrom)**——caller 只需 `WithResumeFrom(snap)`。

**影响**：现有所有 Resume 测试 caller 多调 `cm2.Restore(snap)` 一次现在变 no-op（同 snap 应用幂等）。无破坏。

### C.2 per-branch delta 失败粒度（Phase 1.2.a）

**问题**：fan-out 多分支某一支失败时，PoC 简化"全有或全无"——所有 writes 丢，Resume 重跑全部分支（对齐 LangGraph，但**不如**现有 superstep_snapshot）。

**决策**：改为 `frontierResult { SuccessWrites, FailedBranches, FirstErr, FirstInterrupt }`。成功分支 writes 立即 commit，失败分支记录到 frontierAtExit 让 Resume 只重跑这些。

**意义**：对齐现有 crew-go 优势（LangGraph 没有此能力），不被 PoC 简化损失。

### C.3 嵌套子图 cp 用 SubgraphSnapshots map 递归（Phase 1.2.c，已否决删除）

> ⚠️ **此设计已被 Phase 5.1.a Step 4 否决删除**——`SubgraphSnapshots` map 字段 +
> `recordSubgraphSnapshot/getSubgraphSnapshot/clearSubgraphSnapshot` helpers 均已从代码移除
> （见 `serialization.go` / `checkpointer.go` 注释）。现行嵌套子图 cp 是 **(sessionID, ns)
> 二级 key 物理分离**（`SaveNS/LoadNS`，见上表 #5 + [channel-engine.md](./channel-engine.md) §8）。
> 下文保留作 PoC 期决策溯源。

**自创设计**——没系统查 LangGraph `checkpoint_ns` / eino `subGraphInterruptError` 怎么处理。

**实现**：`CheckpointSnapshot.SubgraphSnapshots map[NodeID]*CheckpointSnapshot` + `cm.subgraphSnapshots` 内部字段 + `recordSubgraphSnapshot/getSubgraphSnapshot/clearSubgraphSnapshot` helpers。

**已支持**：
- 多层嵌套递归（Restore 自动恢复嵌套层级）
- 子图正常完成自动清掉父中的 snapshot 避免下次 Resume 误读
- 子图中断时父 graph 自动 record + Resume 时自动 restore

**待确认**（属于"难点查三方"清单）：LangGraph checkpoint_ns 协议是否在某些边角场景（多层嵌套 + fan-out 子图）有更优表达。

### C.4 删 EndChannel 概念（PoC Step 3）

**audit F6 决策**：原 PoC 设计文档预留 `EndChannel = LastValue[bool]("__end__")` 作终止信号——但与 EndNode 拓扑节点重叠，造成"该写 channel 还是路由 EndNode"困惑。

**决策**：**删 EndChannel，统一用 EndNode 拓扑节点终止**。理由：
- 拓扑结构清晰
- 不引入两套终止机制
- 对齐 LangGraph 拓扑终止 + eino `nodeMap[END]`
- channel 只承担 state 合并职责

## D. PoC 学到的工作方法论

PoC 过程中验证有效的方法 / 用户反馈纠正：

### D.1 sanity-revert 验证测试有效性

每个修复都做 sanity-revert——临时把 fix 改坏，看测试是否 fail 并精确指向 bug。这次落地：
- TestGraph_DoneAfterCtxDeadline_StillSuccess（main bug 修复）
- TestParallel_PartialFailure_PerBranchDelta（per-branch delta 修复）
- TestParallel_PartialFailure_ResumeOnlyFailedBranch（同上）

不做 sanity-revert 的测试可能"假绿"——bug 修复前后行为相同，测试无区分能力。

### D.2 难点先查三方源码（用户 2026-05-07 反馈纠正）

**之前**：Phase 1.2.c 嵌套子图独立 cp 时直接自创 SubgraphSnapshots map，**没查** LangGraph checkpoint_ns / eino subGraphInterruptError。

**纠正**：遇到设计 stuck（如类型擦除某个边角、嵌套 cp 多层场景）**先 spawn 调研 agent 查三方再动手**——避免自己想方案做完才发现绕弯。

但**不是全量扫描**——按需触发，只在真"犹豫不决"时启动。

### D.3 用代码暴露问题胜于看文档判断

设计文档可以模拟假象的简洁——但实际写代码时类型擦除 / cp 序列化 / hook 集成等 真实约束才会暴露。本次 PoC 多个 audit 漏洞（F1/F4/F5/F8）就是写代码后实测才暴露。

### D.4 模型选择的 communication clarity

中间发生过用户 catch："为什么还有 frontier 持久化，不是切换 channel 了么"。复盘：我自己用词不准把 PoC 标为"路线 C 混合"，但实际是 LangGraph 等价设计（BSP 拓扑 + Channel State）。frontier 持久化是 BSP 模型固有产物，非"妥协残留"。

教训：模型术语用词必须精确——用户问"frontier vs channel" 时区分清楚"调度模型"vs"状态模型"。

> **后续路线图已折叠**：原 §E（Phase 3+4 + "main 9 编排 production migration 15-22 周"估时）**全部已落地**——4 层压缩沿用现成 ISSUE-65~68、9 编排已 channel-based production 化、example（plan_chat_web / nested_smoke）+ 文档已重写。演化全程见 [migration-history.md](./migration-history.md) Stage 1-7；触发回退条件未发生（PoC 5/5 通过 + migration 全程无能力不可表达）。

## F. 决策记录续

| 日期 | 决策 | 理由 |
|---|---|---|
| 2026-05-07 | Phase 1.0/1.1/1.2/2 全部完成 | 7 真 audit 漏洞撞击通过 |
| 2026-05-07 | 不 push 累积 commits | 用户："本地可见就行了" |
| 2026-05-07 | 4 层压缩沿用现成不重做 | 用户："压缩比他们成熟" |
| 2026-05-07 | Memory 不在迁移范围 | 用户："晚点接 mem0" |
| 2026-05-07 | redesign 分支继续 production migration 直到全测试过再合 main | 用户策略：金丝雀分支 |
| TBD | 启动 Phase 3+4 production migration | 业务拍板 |

## G. 整体评估

**PoC 阶段成功**：
- 5/5 判定通过
- 7 真漏洞撞击验证
- 109 测试 + ~8000 行代码
- 9 编排全覆盖（9/9）
- channel 模型相对 frontier 模型核心红利（per-branch delta / 嵌套 cp / fan-in 自动聚合）全部实测验证

**PoC 局限**：
- compose_v2 是 PoC 包不是 production——不接 hook.Hooks（业务级 hook，非 OrchHooks）/ skill / 4 层压缩 / streaming chunk 等
- main migration 是真大工程（4-6 月），PoC 仅证可行不证已实施
- 嵌套 cp `SubgraphSnapshots` 设计未对照 LangGraph checkpoint_ns 调研（未来 stuck 时再查）

**整体可信度评级**：高——5/5 判定 + 109 测试 + 7 漏洞撞击 + sanity-revert 严格验证。可作为业务拍板进 production migration 的可信依据。
