---
id: ISSUE-48
title: 节点原生嵌套子图（AddSubGraphNode）+ state 共享
status: 🔴 整体暂缓（主体功能完成前不做，重启时间未定）
priority: 中
created: 2026-04-22
reviewed: 2026-04-23
---

# ISSUE-48 · 节点原生嵌套子图 + state 共享

## 背景

当前 crew-go 嵌套子图的路径是 `GraphAgent` 包装 + `AddAgentNode`：

```go
innerGraph, _ := innerBuilder.Build()
innerAsAgent := compose.NewGraphAgent(innerGraph, compose.GraphAgentName("inner"))
outerBuilder.AddAgentNode("inner", innerAsAgent)
```

能跑，但**隔离过重**，与 LangGraph 的原生 subgraph 语义有实际差距。

## 问题

### 1. State 不共享

外层 `GraphState` / typed `localState` 内层完全看不到；内层有自己独立的 `GraphState`，
写入**只能通过 `LastOutput` 走 message 串联**。

**后果**：
- 外层 `UserSession` 贯穿多图流程时只能每层显式 JSON-encode 进 message 再 decode，丢类型
- 内层中间节点的产出无法被外层其他节点读（只有内层终态的 `LastOutput` 能冒泡出来）
- 共享计数器、进度指标、累积 state 类场景（这些在 LangGraph 里一行字段共享就搞定）都要手动编码

### 2. Checkpoint 粒度粗

外层在 inner 节点中断时，**内层是黑盒**——checkpoint 保存外层 state + "inner 节点未完成"标记，
resume 时 inner 整体从头重跑。内层内部的中间节点进度丢了。

### 3. 事件通道分层

内层 `eventSink` 独立，事件以 `AgentEvent` 形式冒泡到外层 session.events。
`OnNodeStart/End` 等 `OrchestrationHooks` 在外层看不到内层节点——想统一观测全流程需要用户自己
合并两层的事件流。

### 4. 多一层抽象

用户要做两步：`Compile inner graph → wrap as Agent → AddAgentNode`。
LangGraph 直接 `parent_builder.add_node("sub", subgraph.compile())`，子图**就是节点**。

## LangGraph 参考

```python
class ParentState(TypedDict):
    messages: list
    foo: str

class SubState(TypedDict):
    messages: list  # 与父同名 → 自动共享
    bar: str        # 私有

subgraph = StateGraph(SubState)
# ... 构建 ...
parent.add_node("sub", subgraph.compile())   # 直接嵌，不需要 wrap 成 agent
```

**核心机制**：TypedDict 同名字段作为 state 共享接口——子图写同名字段即写父 state，私有字段子图独享。
类型驱动、无需显式映射。

## 决策方向

新增 `AddSubGraphNode(id NodeID, sub *Graph, opts ...SubGraphOption)`：

```go
outerBuilder.AddSubGraphNode("inner", innerGraph,
    compose.WithStateBinding[ParentState, SubState](
        // 父 → 子：子图入口前从父 state 抽取字段
        func(p *ParentState) *SubState { return &SubState{Messages: p.Messages} },
        // 子 → 父：子图出口时把字段写回父 state
        func(p *ParentState, s *SubState) { p.Messages = s.Messages },
    ),
)
```

**核心能力**：

| 能力 | 目标 | 默认是否启用 |
|---|---|---|
| 事件透传 | 内层 `eventSink` 事件复用外层通道，`OrchHooks.OnNodeStart/End` 能看到嵌套 node ID（如 `inner/stepA`） | ✅ 总是启用（相对 GraphAgent 的核心差异） |
| Checkpoint 嵌套 | 内层中断时保存**含内层进度**的复合 checkpoint；Resume 时精准回到内层中断节点 | ✅ 父有 Checkpointer 时自动启用 |
| State 双向绑定 | 父子 typed state 可声明字段映射；子图内 `ProcessState[SubState]` 读写的变更 exit 时反射回父 | ⚙️ 可选（`WithStateBinding`） |
| Session 透传 | 默认内外共享 `RunSession`；ForkLane 分支内嵌子图时使用**当前 lane 的 session**（非 root） | ✅ 总是启用 |

### Messages 流向

和 typed state 分开处理——`Messages` 不走 `WithStateBinding`，而是框架约定的固定语义：

- **入口**：父 `state.LastOutput`（或末尾 Message）作为子图初始 Messages 的尾项，对齐 `AddAgentNode` 现行行为
- **出口**：子图的 `LastOutput` 追加到父 `state.Messages` 并覆盖父 `state.LastOutput`；**子图中间累积的 Messages 不整体回流父**，仅终态一条
- 需要全量冒泡 Messages 的场景请手写 `AddNode` 闭包拼装，不做成默认行为（会让外层历史失控膨胀）

### 相对 AddAgentNode(GraphAgent) 的真正差异

即便不传 `WithStateBinding`，`AddSubGraphNode` 仍 ≠ `AddAgentNode(GraphAgent)`：

| 维度 | AddAgentNode(GraphAgent) | AddSubGraphNode |
|---|---|---|
| 事件 | 内层独立 eventSink，只以 `AgentEvent` 形式冒泡 | 内层节点事件直接复用父 eventSink，`OrchHooks` 看得到嵌套节点 |
| Checkpoint | 父 checkpoint 视 inner 为黑盒"未完成"；resume 时内层整体重跑 | 父 checkpoint 含内层断点，resume 精准回到内层节点 |
| State | 完全隔离 | 默认同 GraphAgent；加 `WithStateBinding` 后开启 typed 字段共享 |

三项差异里**前两项是 AddSubGraphNode 存在的根本理由**，state 共享只是锦上添花。不能因为用户没开 `WithStateBinding` 就退化成纯 GraphAgent 行为。

## API 草案

```go
type SubGraphOption func(*subGraphNodeConfig)

// WithStateBinding 声明父子 typed state 的字段映射。
// 省略则子图 typed state 完全隔离（事件透传 / checkpoint 嵌套仍启用，不退回 GraphAgent）。
func WithStateBinding[P, S any](
    extract func(*P) *S,
    writeBack func(*P, *S),
) SubGraphOption

// WithEventPrefix 控制嵌套节点 ID 在外层 hooks 里的前缀（默认用父节点 id）。
func WithEventPrefix(prefix string) SubGraphOption

// WithSubCheckpoint 控制"父 Checkpointer 触发时是否把内层中间节点进度纳入父快照"。
//
// 语义前提是父已配置 Checkpointer——父没 Checkpointer 时本选项为 no-op。
// 默认 true。置为 false 时子图中断行为退化为 GraphAgent 黑盒（内层整体重跑）。
func WithSubCheckpoint(enabled bool) SubGraphOption

func (b *GraphBuilder) AddSubGraphNode(id NodeID, sub *Graph, opts ...SubGraphOption) *GraphBuilder
```

### 泛型类型擦除兜底

`WithStateBinding[P, S]` 在 `opts ...SubGraphOption` 这一层必然擦除类型。实现策略：

```go
type stateBindingOpt struct {
    extract   any // func(*P) *S
    writeBack any // func(*P, *S)
    pType, sType reflect.Type
}
```

- 绑定存 `any` + 预缓存 reflect.Type（从泛型 `*new(P)` / `*new(S)` 拿）
- AddSubGraphNode 执行时取父/子 `StateFactory` 生成的实例，对比 reflect.Type
- **类型不匹配直接 panic**，错误信息包含期望类型与实际类型，不沉默 no-op——让编译期笔误在首轮运行立刻暴露，而不是生产环境静默丢数据

## 和 GraphAgent 的分工

**不删除** `GraphAgent`——它的定位"把图当 agent 用"仍然合理（比如子图要和 BasicAgent 并列作为 `AddAgentNode` 候选）。

```
AddAgentNode(id, agent.Agent)     ← 把 agent 作为节点，包括 GraphAgent
AddSubGraphNode(id, *Graph, opts) ← 原生子图嵌套，支持 state 共享
```

前者是"agent 即节点"（黑盒单元），后者是"图即节点"（透明子流程）。两套 API 对应两种心智模型，不重叠。

## 实现拆解

1. **compose/subgraph.go（新增）**
   - `AddSubGraphNode` 实现
   - `subGraphNodeConfig` 配置结构
   - 父子 state 绑定适配层（反射 or 泛型）
2. **compose/graph.go**
   - `subGraphNode` 实现 `NodeFunc` 签名
   - 入口：按 `extract` 构造子图初始 state（Messages / localState）
   - 执行：调用内部 `runGraph` 子流程
   - 出口：按 `writeBack` 回写父 state + `LastOutput`
3. **compose/event_sink.go**
   - 子图执行时共用父 `eventSink`，节点 ID 加前缀
4. **compose/checkpoint.go**
   - 父 checkpoint Metadata 里加 `subgraph_state` 子区
   - Resume 时检测到子区 → 先进子图 runGraphFrom，再继续父流程
5. **doc/description/patterns.md / runtime.md**
   - 补嵌套子图说明，明确和 GraphAgent 的分工
6. **CLAUDE.md**
   - compose 速查表补 `AddSubGraphNode`

## 开放问题

- **非 typed state 场景**：两边都没 `StateFactory` 时 `WithStateBinding` 省略；事件透传 + checkpoint 嵌套仍生效（这才是相对 GraphAgent 的核心差异，state 绑定只是加餐）
- **嵌套层数**：允许任意深度，但 Checkpoint 深度要测——递归 Metadata 可能爆炸。实现时加运行期深度统计，超过阈值（如 16 层）打 warn
- **InterruptError 嵌套 payload × ISSUE-40**：ISSUE-40 已明确 `CustomizedAction any` 反序列化丢类型。嵌套子图中断时父 Checkpoint 要带含子图 `localState` 的复合结构，同样走 `any`——采用 ISSUE-40 方向 D 的对策：`Checkpoint.Metadata["subgraph_state"]` 存原始 JSON bytes + 子图 NodeID（用于反查 StateFactory 重建 localState）。Resume 时按子图 NodeID 找到 factory 实例化，再 `json.Unmarshal` 到具体类型。不把任意 `any` 直接塞 Metadata
- **ForkLane × 子图 session**：外层 ParallelAgent / PlanAgent 并行分支经 ForkLane 产生 lane session 后，AddSubGraphNode 使用**当前 lane 的 session**（即 ctx 透传下来的那一个），不回溯到 root——否则子图写入会污染兄弟分支的 session 视图。JoinLanes 由外层分支聚合触发，子图自身不参与
- **并发安全**：外层 fanout 分支里嵌子图时，子图 state 绑定要保证 per-branch 独立——`extract`/`writeBack` 在分支 state 克隆后调用，绑定内读写不跨分支
- **Go 泛型选择**：接受 option 内部弱化（见"泛型类型擦除兜底"），替代方案"GraphBuilder 带 P 泛型参数"侵入面广，不值

## 不涵盖

- 子图作为 agent 供 BasicAgent tool_call（这是 tool 系统的事，不是嵌套图）
- 跨子图的 typed state 继承（只支持"子继承父"的一层绑定，不做多级）
- 子图热替换 / 运行时动态编译（编译期能力）

## 相关

- 当前实现：[compose/graphagent.go](../../compose/graphagent.go)
- LangGraph 参考：[subgraph 文档](https://langchain-ai.github.io/langgraph/how-tos/subgraph/)
- ISSUE-47（PlanStep InputRefs）当前是 sequential 执行；若将来扩展到"同批无依赖 step 自动并发"，子图作为"一个并发分支的单元"时价值更大
- ISSUE-40（AgentEvent 持久化）：本 issue 的嵌套 checkpoint payload 遵循同一套"不塞 any，走具名字段 + 可反序列化"约定
- ISSUE-41（ForkLane / JoinLanes）：本 issue 明确子图在 lane 分支内使用 lane session

---

## 2026-04-23 设计审核 & 分期决策

### 核心心智模型（写进 P0 文档的公共契约）

**"嵌套 ≠ 拍平"**。`AddSubGraphNode(id, sub)` 对父图而言**就是一个 node**——父 frontier 走到这里时内部完整 run 一遍子图，再把结果回写给父。子图内部的 `stepA/stepB` 不会进父的 nodes map，父的 `AddEdge` 不能直接连到内层节点。"嵌套节点 ID `inner/stepA`" 只是 OrchHooks 展示层的前缀，**拓扑仍然两层**，各自有独立 frontier / StateFactory / Start/End。

这条是最容易误读的地方，文档正文里要加一句警示。

### 并发分支内嵌子图的 barrier 语义

假设拓扑 `a → [b ‖ c] → d`，c 是子图 `x→y→z`：

- **d 必须等 x→y→z 全跑完**。父图 fan-in 把子图整体视作一个 node，"完成"定义 = 内部所有节点跑完 + writeBack + LastOutput 就位
- d 看到的 `ParallelResults` 里只有 b 和 c 两条，**不会透出 x/y/z 三条**——想让 d 看到内层中间值只能靠 state binding writeBack，或把图写平
- 子图内部的并发/超步是子图自己的事，父图 barrier 不感知层数

### 审核发现的实打实的问题（P1/P2 实现前必须先拍板）

**A. `extract` 返回值的所有权语义未定义**
`extract func(*P) *S` 示例里 `&SubState{Messages: p.Messages}` slice header 是 alias——子图并发读写会绕过父 GraphState 的 mu 锁（`compose/graph.go:36-61`）。**必须写死：extract 返回深拷贝，writeBack 是唯一反射回父的通道**。否则 ParallelResults / typed state 在并发分支里会被同时写坏。

**B. 子图自身 StateFactory 与 extract 的优先级冲突**
子图 Build 时可能已经 `WithStateFactory` 注册过（`compose/state.go:22`）。AddSubGraphNode 接入时两个产生 localState 的路径冲突，设计没拍板。**建议写死：有 binding 时走 extract、跳过子图 factory**——不要做"factory 产生基底 + extract 覆盖字段"的字段级合并，那需要反射深合并，又是一处坑。

**C. eventSink 共享需要 RunGraph API 改动**
当前 eventSink 是 GraphState 字段，RunGraph 内部自己 new。"事件透传"实现上要求 **RunGraph 接受外部传入的 eventSink**（或通过 `GraphRunOptions` 扩展），子图执行时复用父 sink 而不是再 new。这是 compose API 的破坏性改动，属于 P0 必做的前置工作。

**D. 超步 × 子图 × 中断的三层嵌套 checkpoint**
ISSUE-46 已经在 Metadata 里放了 `superstep_snapshot`。场景 `a→[b‖c]→d`，c 是子图，c 内部 y 抛 InterruptError——Metadata 要同时存：

- `superstep_snapshot`：b 已完成的 delta + c 未完成标记 + 入口快照（ISSUE-46）
- `subgraph_state`：c 子图内部 y 的中断位置 + 子图 localState（ISSUE-48 新增）

Resume 顺序必须是"先加载超步快照只重跑 c 分支 → 进 c 分支后加载 subgraph_state 从 y 续跑"。**两层 Metadata key 独立性、读取顺序、子图 localState 反序列化 factory 索引**都要在 P2 开工前写清楚。

**E. `WithSubCheckpoint(false)` 的语义陷阱**
置 false 时"退化为 GraphAgent 黑盒"要求：subGraphNode wrapper **截住子图 InterruptError，不让它的 localState 流进父 Metadata**，否则 resume 时父看到残留会走错分支。这条必须明写。

**F. 并发分支 writeBack 父 localState 的合并策略缺失**
`a→[b‖c]→d` 里 b 和 c 都在并发写父 localState。ForkLane 现行语义（`agent/session.go:188-247`）只处理 session.events 的合并（按 CreatedAt 排序 commit），**没有 typed localState 的合并逻辑**。子图 writeBack 写的是 c 分支 cloned localState，父图 fan-in 时必须定义合并策略（覆盖？字段级 merge？last-writer-wins？）——现在文档里没定，是个硬缺口。

### 需要澄清的歧义

- **入口 `assistant → user` 转换**：AddAgentNode 在 `compose/graph.go:239-241` 做了降级，AddSubGraphNode 要不要沿用？设计说"对齐"但没点名。**建议写死：沿用**
- **NodeID 前缀格式契约**：带前缀后变 `inner/stepA`，现有 OrchHooks 消费方是否假设 NodeID 不含 `/`？**建议固定分隔符为 `/` 并声明为 public contract**
- **反射 panic 权衡**：已选 runtime panic 而非 `GraphBuilder[P]` 泛型，开放问题里写得太软。**建议改成断言式"runtime panic 是终局选择"**，避免后人再提

---

## 当前决定（2026-04-23）

**子图这一整块先完全不做**——不拆 P0/P1，不做 MVP，整体停在设计阶段。

### 理由

1. **GraphAgent + AddAgentNode 当前已经能兜底**"图嵌套图"场景，线上没有用户痛诉需要原生子图
2. 审核发现的 6 个问题（A–F）和 3 处歧义，任何一块动起来都会引入大量新状态机维护成本——特别是 D（超步×子图×中断的三层 checkpoint）会和 ISSUE-46 的部分失败策略绑死
3. 当前复杂度预算应投向主体功能（ISSUE-44/45/46/47 收尾及后续主线），不开新战线
4. LangGraph 的 TypedDict 字段级 reducer 在 Go 里对应不上，硬翻的代价和体验差值都不划算——等真实场景出现再定设计比现在拍脑袋稳

### 本文档的定位

**保留作设计底稿**，不标记为"待办"也不排期。

重启触发条件（满足其一再讨论）：

- 出现真实用户场景要求父子图 typed state 字段共享
- OrchHooks 观测需求强到不能忍受 GraphAgent 的黑盒事件冒泡
- 主体功能全部稳定、团队有富余复杂度预算

重启时的入口：本文档"2026-04-23 设计审核"那节列出的 6 个问题（A–F）和 3 处歧义**每条都是必答题**，动手前必须先在文档里拍板。不得绕过直接写代码。
