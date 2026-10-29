# 主题：子图隔离 / 嵌套（Subgraph Isolation / Nesting）

> **v2 baseline**（channel-based + agent_v2 wrapper top-level fire）：
> - 子图机制已**从"头等节点 AddGraphNode"演化为"agent_v2 wrapper 嵌入 sub.Run + ctx push graphNS"** —— 不再有 `subgraph_node.go` 文件
> - cp 物理分离按 `(parentSID, parentNS/childName)` 二级 key（`compose_v2/checkpointer.go` Checkpointer 接口 8 方法）
> - **7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) `agent_v2/*.go` 都通过 `compose_v2.WithGraphNS(ctx, JoinNS(parentNS, "<childName>"))` push 父位置 NS; **BasicAgent ReAct 无 sub-agent / GraphAgent 业务自定义 NodeFunc 走 `agent_v2.RunSubAgent` helper** (#7 F4 commit `f6ede83`)
> - `agent_v2.WithCtxOrchHooks(ctx, h)` 让嵌套 sub-agent 自动继承父 hooks（业务层 OrchHooks ctx 透传）


## 触发场景

需要查阅本文档的设计决策：
- 设计 / 改动子图注册 API（`AddGraphNode` / `AddNode(subgraph)` / 类似）
- 决策子图与父图的 session / 事件流共享策略
- 决策子图 cp store 是否独立（sandbox 场景）
- 设计子图 typed state 类型隔离机制
- 评估"子图作头等节点" vs "子图作 NodeFunc 内部调用"

## 三方对照速查表（v2 baseline）

| 维度 | crew-go v2 | LangGraph | eino |
|---|---|---|---|
| **子图注册** | agent_v2 wrapper 内 sub.Run（无单独 AddGraphNode API；GraphAgent 包任意 graph 为 Agent 给业务自定义拓扑） | `add_node(name, pregel)` 任意 Pregel 实例作节点 | `AddGraphNode(key, AnyGraph, opts)` 头等节点 |
| **session 隔离** | `RunSession` 共享 / `ForkLane` 隔离 / `AgentAsTool` 半共享 (ISSUE-79) | 无 session 概念 | 无 session 概念 |
| **事件流（OnNodeEvent）** | 默认继承父 OrchHooks（`WithCtxOrchHooks`）+ `ev.Metadata["graph_ns"]` **自动注入**（业务前端可按 ns 分通道） | streaming `subgraphs=True` 控制 namespace tuple | callback ctx 透传（无开关 + 无 ns 字段） |
| **cp store 隔离** | 共享父 cp store + `(parentSID, parentNS/childName)` 物理分离（8 method Checkpointer 接口）；`LoadNS` 非破坏性幂等 | 共享 store + (thread_id, ns) 寻址；ns = `父ns\|节点名:task_id[\|k]`，k 是同一次任务执行里第几次调子图（第 0 次不加）。⚠️ 节点级重试时 k 的计数器不重建，重试那次拿到 `…\|1`、子图从头跑（`46b652a74` 起） | 共享 store 全树嵌套；`forwardCheckPoint` 一次性 delete 消费（重入脆弱） |
| **typed state** | channel-based（唯一 state 模型，v1 typed local state 已删） | 各 StateGraph 独立 schema | 各 graph 独立 `WithGenLocalState[S]` + `internalState.parent` 链查 |
| **fanout NS 防互覆** | `JoinFanoutNS(parent, branchID, index)` 含 index | `task_id = xxh3(checkpoint_id, ns, step, name, PULL, *triggers)` 含 step | 父 cp `SubGraphs` map 按 nodeKey（无 retry 区分） |
| **同一次执行里多次调同名子图** | 身份 = 位置（调用方所在的节点与相位 + 实例维）+ 输入内容（输入相同的几次再按 `NextSubCallIndex` 分第几次；计数器随每次节点执行、每个 tool_call、每次钩子调用新建，重跑从 0 数），或 `WithCallKey` 给的 key；槽号是身份哈希的低 63 位。已完成的调用把整组事件（连同写给上层的转交 / 路由信号）记在父节点这次执行里（`SaveStepResult`），重跑时拿回、不再执行——钩子相位里的也记。输入不同的调用之间身份跟次序无关，跳过、插入调用都照常对；输入相同的几次靠次序区分，要跳过得给 key | 调用序号 `\|k`（`d6492ef04` 起，之前直接抛 `MultipleSubgraphsError`）；一次任务执行共用一个计数器，调用顺序一变就错位（实测后一个调用读到前一个的存档，`err == nil`） | 不编号：Lambda 里调同一个编译图两次会撞地址（实测答案被第一次调用拿走，真正中断的第二次拿零值输入当首次跑完） |
| **同一次调用重跑时输入变了** | 输入变了就是另一次调用：新开、从头跑、自己问（带 key 时没完成的续存档、已完成的拿回记录，都打告警）。不带 key 的调用存档记输入摘要，续跑时对不上 = 撞槽 ⟹ 拒绝、不清档（`WithInputIdentity`） | 续存档（父下传 `CONFIG_KEY_RESUMING`），新输入静默丢弃，官方测试钉着这个行为 | 存下来的输入说了算（组件把输入存进自己的中断状态，续跑时覆盖传入值） |
| **Sandbox 场景** | `RunSession` 独立 + `DeleteSubtree` 成功清理（业务显式） | 用独立 thread_id（断 Resume / 审计关联） | 不支持 |

## crew-go v2 设计

### 嵌套机制（agent_v2 wrapper push graphNS）

v2 没有"子图作头等节点"的 AddGraphNode API——任何 Agent 实现都通过 `Agent.Run(ctx, sess, input)` 嵌入。**7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) 在调用 sub-agent.Run 前 push 父位置 NS (BasicAgent ReAct 无 sub-agent; GraphAgent 业务自定义 NodeFunc 走 `agent_v2.RunSubAgent` helper):

```go
// agent_v2/parallel_agent.go::branch dispatch 范式 (按符号自查 — line 软引用防 drift;
// push 点收敛在 4 处：runMember（6 个编排的成员，不含 Handoff）/ runDecisionAgent（内部决策 LLM）/
// runNestedCall（RunSubAgent + AgentAsTool + skill_tool fork）/ Handoff 铸造。
// 曾是 15 处散在各编排内、靠人肉计数维持，5afb8eb 那次就漏了 2 处）
childNS := compose_v2.JoinNS(parentNS, branchName) // 或 JoinFanoutNS 含 index 防 retry 互覆
subCtx := compose_v2.WithGraphNS(ctx, childNS)
events, err := sub.Run(subCtx, session, input, opts...)
```

`compose_v2/ns.go`：
- `JoinNS(parent, nodeID)` 普通拼接
- `JoinFanoutNS(parent, branchNodeID, index)` 含 index 防 fanout 多实例 cp 互覆（ISSUE-78）
- `IsNSInSubtree`：`DeleteSubtree` 的删除范围——自身 + 经 `/` 往下的后代，`#`/`@` 兄弟执行不算
- `ParentNS`：寻址辅助（把 `a#0` / `a@1` 归到 `a` 下，只用于观测按节点归组）

详细嵌套见 [`doc/v2/channel-engine.md`](../../v2/channel-engine.md) §8 NS 命名空间体系，cp 协议见 [`doc/v2/checkpoint.md`](../../v2/checkpoint.md)。

### GraphAgent（业务自定义拓扑）

`agent_v2/graph_agent.go`——把任意 `compose_v2.Graph` 包成 Agent。业务自定义 NodeFunc 内若要嵌套 sub-agent，**走 `agent_v2.RunSubAgent`**（它一站式 push graphNS + child session + storeScope，并负责调用身份的全部维度——调用方所在的节点与相位、实例维 / visit 维、输入内容或 key——以及已完成调用的完成记录；手拼 `JoinNS` 会漏掉这些，2026-09-18 改正）：

```go
// 业务代码范式
graphBuilder.AddNode("worker", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
    events, err := agent_v2.RunSubAgent(ctx, "worker", mySubAgent, input)
    // ...
})
```

CLAUDE.md "实施前 checklist 第 1 条" 已警示：业务自定义 NodeFunc 嵌套 sub.Run 时漏 push → 子 cp 与父 cp 互覆。**7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) 已自动 push; **GraphAgent 业务自负责** (推荐走 `agent_v2.RunSubAgent` helper 一站式 push 3 件 ctx — #7 F4 commit `f6ede83`).

### cp 物理分离

`Checkpointer` 接口 8 方法（`compose_v2/checkpointer.go`）按 `(sessionID, ns)` 二级 key：
- `SaveNS / LoadNS / ExistsNS`：精确寻址
- `DeleteSubtree`：删 P 自身与经 `/` 往下的后代，同一节点的 `#`/`@` 兄弟执行不删（cleanup-on-success / 终局失败 / Resume Abort 用）
- `ListPending`：Resume 寻址（跨 run 会话历史移出 cp 层，见 ISSUE-98）
- `LoadAllInSession / SaveBatch`：批量
- `Cleanup`：TTL 清理

**`LoadNS` 非破坏性幂等承诺**——重复调用同 (sid, ns) 返同 cp，业务可在 LoopAgent 多 round / ReAct 多 round / retry / Resume 路径自由复用，**对照 eino `forwardCheckPoint` 一次性 delete 消费**（compose/checkpoint.go:163-166 "only forward once"）的脆弱设计更优。

### 事件流（OnNodeEvent）跨子图边界

`compose_v2/hooks.go::nodeEventSink.Send` 自动注入 `ev.Metadata["graph_ns"]`——前端 SSE 流可按 ns 分通道展示父子嵌套（类比 LangGraph `stream(input, subgraphs=True)` 返 namespace tuple，但 crew-go 是细粒度自动注入而非全局开关，业务无需声明特殊参数）。

业务 `OrchestrationHooks.OnNodeEvent(ctx, nodeID, ev)` 读 `ev.Metadata["graph_ns"]` 分父子；业务自填 `graph_ns` 时不覆盖（业务优先）。

### typed state

v2 推荐 channel-based 不用 typed state。channel 物理分离（不跨 ns 共享），跨 ns 共享业务数据走两层协议：
- 短期 KV（per-sess）：`sess.SetValue / GetValue`
- 长期跨 thread：`store.Store`（业务可自实现 Redis / PG / mem0 后端）

channel 是唯一 state 模型（v1 `ProcessState` / typed-state 已删）。

## LangGraph 设计（v0.6.10）

### 子图注册

`add_node(name, action)` 中 action 可以是另一个 `Pregel` 实例（任何 compiled graph）。无需特殊 API——子图就是节点。

每个 task 自动获得 `CONFIG_KEY_CHECKPOINT_NS = task_checkpoint_ns`（`pregel/_algo.py:615`），子图运行时 ns 自动派生：
```python
checkpoint_ns = f"{parent_ns}{NS_SEP}{name}" if parent_ns else name
task_checkpoint_ns = f"{checkpoint_ns}:{task_id}"
```

### 事件流隔离

streaming 通过 `subgraphs=True` 参数控制是否暴露子图事件给上层（`pregel/main.py:2491-2495`）：
```python
def stream(self, input, config=None, *, subgraphs: bool = False, ...):
    # ...
```

事件在 super-step 内通过 stream channel 透传，namespace tuple `("parent_node:tid", "child_node:tid")` 隔离。

### cp store 隔离

**所有子图共享父 `BaseCheckpointSaver` 实例**——天然按 `(thread_id, ns)` 二元组隔离寻址。父调用配 `CONFIG_KEY_CHECKPOINTER` 透传给子（`pregel/_algo.py:607-609`），子图独占自己的 ns。

**没有"独立 store"或"独立 thread_id"的 option**——thread_id 全程共享（`pregel/main.py:1049,1059`）。隔离只通过 ns 层级实现。

### typed state

每个 `StateGraph` 有独立 schema，子图作节点时框架自动调 `input_schema` 转换（`graph/state.py:362, 377`）。允许父子异构 state。

### 优势

- **同 store + ns 寻址**对 PG/Redis 等持久化后端友好（一次连接、统一事务）
- ns 自动派生跨 resume 稳定（task_id 与调用序号都能重算出来）。⚠️ 跨**节点级重试不稳定**（2026-09-23 本地源码实测更正，原文写「跨 retry/resume 不变」）：重试不重建调用计数器，重试那次拿到 `…|1`、子图从头跑；断言「重试只重跑失败节点」的 `test_subgraph_retries` 在 `46b652a74`（2025-03）被删
- 子图作头等节点的 API 极简（add_node 接受任意 Pregel）

### 缺陷

- **无 session 概念**，所有审计混一起
- **不区分 sandbox vs 共享子图**——sandbox 场景只能换 thread_id（断 Resume / 关联审计）
- ns 派生**默认刚性**（2026-09-23 更正，原文写「完全刚性、调用方无法覆盖」）：调子图时传显式坐标（`thread_id` / `checkpoint_ns`，#8053）能让它脱离父图的存档链、成为独立根图；`checkpointer=True` 档去掉 task_id、按「节点路径 + 调用位置」跨次调用累积。但**留在父图存档链内时，没有公开 API 能让「这一次调用」强制新开**
- 无 cp store 显式生命周期 option（"用完就扔"做不到）

## eino 设计

### 子图注册

`graph.AddGraphNode(key, node AnyGraph, opts ...GraphAddNodeOpt)`（`compose/graph.go:406-409`）：
```go
func (g *graph) AddGraphNode(key string, node AnyGraph, opts ...GraphAddNodeOpt) error {
    gNode, options := toAnyGraphNode(node, opts...)
    return g.addNode(key, gNode, options)
}
```

`AnyGraph` 可以是 `Graph[I,O]` 或 `Chain[I,O]`，编译期作头等节点。

### session / 事件流隔离

**无 session 概念**。子图共享父 ctx，cb manager 通过 ctx 透传（`callback` 包）。无 hook propagation 开关。

### cp store 隔离

父 store 唯一。子图 cp 通过 `forwardCheckPoint` 从父 ctx 取出嵌入的 sub cp 后置入子 ctx（`compose/checkpoint.go:157-168`）；子图结束时父 runner 写回父 cp 的 SubGraphs map（`graph_run.go:679`）。逻辑上"单 store 全树序列化"。

### typed state

每个 `graph` 通过 `WithGenLocalState[S]` 各自独立 typed state（`state.go:30`），编译期类型独立。`internalState.parent` 字段支持向上链查（`state.go:34-38`）但不强制共享。

### 优势

- 接口极简（AnyGraph 抽象）
- 父子 state 类型完全独立
- typed state 父子链查机制（`internalState.parent`）

### 缺陷

- 全树 marshal 性能问题（cp 大时所有子图一起序列化）
- 无 session / 事件流分层
- 无 sandbox 隔离机制
- `forwardCheckPoint` 一次性 `delete` 消费，重入逻辑脆弱

## 我们的优势（v2 保留不动）

- **GraphAgent 显式 InputWriter/OutputReader vs 三方自动 IO 映射（R33 review 固化）**：LangGraph subgraph-as-node 靠父子 state schema 共享 key 自动映射（异构 schema 官方让你 "wrap in a function"）；eino `AddGraphNode` 编译期 typed IO 对齐。我们 channel 模型无"约定字段"可自动映射——Writer/Reader（等价 LangGraph 的 wrap-in-function）被设为唯一路径 + 构造/Run 双层 fail-fast。换来零隐式 key 碰撞；代价是"业务写任意 reducer"的新风险面（R33 GA-H1 嵌套 auto-resume 播种双重应用即此类）。**修法两轮**：先是让 GraphAgent 镜像 BasicAgent 那份「自查这次算不算续跑」的守卫——但那对拷贝随后漂移过四次；现在收敛成引擎侧一个 RunOption（公开的 `WithInitialWrites` 收窄成「只在新开路径 apply」，「两条都 apply」那个语义降为不导出的 `withImportOverlay`、子图独家），两个 agent 的自查连同各自那次 `LoadNS` 一并删除。

1. **`RunSession` 三档隔离机制**（ISSUE-79 + ISSUE-80）：共享 / 半共享（`AgentAsTool` 让父 LLM 不见 sub-agent 内部）/ `ForkLane` 完全隔离——LangGraph + eino 都无 session 概念
2. **`OrchHooks` 默认继承 + `agent_v2.WithCtxOrchHooks` 业务层 ctx 透传**：streaming 跨子图边界开箱即用，业务无需声明特殊参数
3. **`Event.Metadata["graph_ns"]` 自动注入**（本次新增）：前端 SSE 按 ns 分通道——业务零成本接入，对照 LangGraph `stream(subgraphs=True)` 粗粒度 caller 开关更细
4. **`LoadNS` 非破坏性 + 幂等承诺**：业务可在 LoopAgent / ReAct 多 round / retry / Resume 自由复用——对照 eino `forwardCheckPoint` 一次性 delete 消费的脆弱设计
5. **`JoinFanoutNS` 含 index 防 fanout 多实例 cp 互覆**（ISSUE-78）：BasicAgent toolsFn fanout N tc 经 `RunSubAgent` helper (`agent_v2/run_subagent.go`, 含 AgentAsTool.Call delegate) / `SkillTool fork` (`agent_v2/skill_tool.go`) 调嵌套 sub-agent 时, 槽恒为 `JoinFanoutNS(parentNS, nodeID, 身份哈希的低 63 位)`，位置的 tool_call 维 = tc.ID + 批内下标 + tools 节点访问序号，再叠调用方所在的节点与相位（全历史输入的 AgentAsTool 与 skill fork 只看位置，其余再叠输入内容或 key）; ParallelAgent fanout 不走此路径——按 `br.Name` 唯一 + AddBranch panic 防重名 已保 cp 物理分离 (`parallel_agent.go` `JoinNS(parentNS, br.Name)`)
6. **HITL Resume `UserInputs map` 按 `InterruptID` 精配**：嵌套并发分支 Resume 闭环——LangGraph 需 caller 自己拼 ns，eino 不支持嵌套并发分支

## 已闭合的旧不足

1. **GraphAgent 业务自定义 NodeFunc 内 sub.Run 漏 push 父位置 NS** (**已闭合 #7 F4 commit `f6ede83`**)
   - LangGraph `add_node(name, pregel)` 注册时自动派生子 NS, 业务无负担
   - crew-go v2 GraphAgent 让业务自定义拓扑灵活性高但需走 `agent_v2.RunSubAgent`（手拼 `JoinNS(parentNS, nodeID)` 会漏掉实例维 / visit 维 / 调用方节点与相位 / 输入内容，也拿不到完成记录）
   - **已加** `agent_v2.RunSubAgent(ctx, nodeID, sub Agent, input *schema.Message, opts ...RunOption)` helper — 一站式 push 3 件 ctx (graphNS + NewSharedChildSession + storeScope); GraphAgent 业务自定义 NodeFunc 内推荐用此 helper 避免漏 push silent bug. **7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) 已内置 push 协议

2. **子图 import / export 把「复制值」实现成「经 reducer 写」——对累加 reducer 是相加**
   （**已闭合，2026-09-02**）
   - 症状（实测，全程 `err == nil`）：export 侧，子图没碰过的 import channel 被原样回送，
     父 `Sum` 5→10、`Append` [a]→[a a]；import 侧，带子 cp 续跑时 import 再灌一次，子图读到
     13、父最终 18（正确 8）；`LastValue` 侧，子图改写过的值在续跑时被父图的值盖回去。
   - 根因一个：**reducer 写被借去做状态复制**——只在 `LastValue` 上碰巧成立（「N=1 对、N≥2 静默
     错」那一族）。
   - 修法：子 `ChannelManager` 加一层不经 reducer 的 `baseline`（`setBaseline`）。import 只摆这层，
     子图自己的写只进 `values`，读 = reduce(baseline, own)，export 只送 own（`ownValue`）。
     baseline 不进 Snapshot——每次进入都从父图重新摆，所以续跑天然拿到父图最新值（F5 不变量
     保住）、不会二次累加、也不会盖掉子图进度。**零新 cp 字段**。
   - **三方对照**：
     - LangGraph **同款坑，且没有防护**。子图作为节点的返回值是 `read_channels(output_channels)`
       ——**全部** output key 的当前值（`pregel/_io.py::map_output_values`），父图当普通节点返回
       处理、经父 reducer 写入。`tests/test_pregel.py::test_subgraph_checkpoint_true` 里 inner
       `my_key: Annotated[str, operator.add]` + `checkpointer=True` 再进入时，父图传入的值经
       `operator.add` 叠在 inner 自己已存的值上——测试把翻倍后的
       `" got here and there got here and there ..."` 当**期望**快照。他们的出路是让父图那个 key
       不带 reducer、或让子图只返回增量，都是业务纪律，不是机制。
     - eino **结构上免疫**：子图输出是 END 节点收到的**单个类型值**（`compose/graph_run.go`
       `nodeMap[END]`），父图把它当一个普通节点的输出，没有 channel 级的 import/export 映射，
       所以没有「回声」这条路。fan-in 的 `mergeValues` 是多前驱合一，不是同一个问题。
     - 我们走的是第三条：保留 channel 级 import/export 的表达力，用 baseline 层把「复制」和
       「写」分开。比 LangGraph 多一层机制、比 eino 多一种能力。
   - 哨兵：`compose_v2/subgraph_baseline_test.go`（7 条，逐条正向断言 + 三处 sanity-revert
     各自精确红：import 退回 ApplyAll / export 退回 read / read 不合成 baseline）。

## 我们的不足（已评估不抄）

1. **eino `internalState.parent` typed state 链查**
   - eino 业务子 graph 通过 `internalState.parent` 沿父链查 typed state
   - crew-go v2 channel-based 不跨 ns 共享是 by-design (物理分离前提); 业务跨 ns 共享走 `sess.SetValue/GetValue` (per-sess KV) + `store.Store` (跨 thread)
   - **不抄**：协议已有, channel 不跨 ns 是 cp 物理分离前提

## 决策记录

> ⚠️ 下表 ISSUE-70 Phase 2 行涉及的 `WithEphemeralCp` / `WithIsolatedSession` /
> `WithIsolatedCheckpointStore` / `AddGraphNode` 等 API 均为 v1 / Phase-2 时代设计，已在
> **v2 重构（末行）随子图节点体系删除**（grep 全仓 0 生产引用）——现行 sandbox 等价能力走
> `WithFreshRun` + agent wrapper push graphNS + `DeleteSubtree` 显式清理。下表保留作决策溯源。


| 时间 | 决策 | 借鉴 | 不全抄理由 |
|---|---|---|---|
| ISSUE-64 | 加 `AddGraphNode` 头等节点 + 4 个 SubgraphNodeOption | eino `AddGraphNode(AnyGraph)` | 不抄 AnyGraph 抽象——Go 泛型够用，不需要中间接口 |
| ISSUE-64 | OrchHooks 默认继承 | LangGraph streaming 透传 | 不抄 `subgraphs=True` 全局开关——要 builder 侧细粒度 |
| ISSUE-70 Phase 2 | 删嵌入式 cp 改 ns 物理分离 | LangGraph (thread_id, ns) 寻址 | 不抄"完全刚性 ns 派生"——保留 `WithEphemeralCp` 给 sandbox（「完全刚性」这个判断 2026-09-23 核验不准确，见上文 LangGraph 缺陷一节）|
| ISSUE-70 Phase 2 | **不**加 `WithIsolatedCheckpointStore` | （三方都没有） | 同 thread 两个 store 原子性问题真实——sandbox 改用 `WithEphemeralCp` 显式生命周期 |
| ISSUE-70 Phase 2 | 加 `WithEphemeralCp`（成功后 DeleteSubtree） | 三方都没有（我们独有） | LangGraph 用换 thread_id 实现，断 Resume / 审计；我们用同 sid + ns 子树 + 显式清理，更优 |
| ISSUE-70 Phase 2 Step Ⅰ | `WithIsolatedSession` 子图首次执行使用独立 cp store（NewInMemoryCheckpointer）替代共享父 store | 三方都没有（我们独有） | sandbox 子图独立 sessionID + 独立 cp store + 独立事件流——完全隔离不污染父，但**不支持 Resume**（与 sandbox 语义一致）。共享 store 会让 cp 写到 (innerSID, ns) 留孤儿 cp，父查不到 |
| ISSUE-70 Phase 2 Step Ⅳ + Round 1 | (1) `subgraph_node` AddGraphNode 闭包入口判断 cp Status=Pending（替代仅 ExistsNS），Failed cp 不触发 ResumeSubgraph 续跑；(2) `ResumeSubgraph` 透传父 RunSession 给内层 ResumeGraph(opts.ParentSession)；(3) AddGraphNode 闭包内 inheritSession=false 时 RunSubgraph opts 显式独立 cp store | 三方都没有（精细 Resume 控制） | 修 H-B 嵌套 Resume 子图独立 sess 破坏共享不变量；修 M-C 业务错残留 cp 卡住下次 Run；与 H2+H3 cleanup 配合让 cp 生命周期明确 |
| v2 重构 | 删 AddGraphNode 头等节点 API + 删 `compose/subgraph_node.go` + 删 4 个 SubgraphNodeOption；嵌套统一走 `agent_v2 wrapper.Run` + `compose_v2.WithGraphNS(ctx, JoinNS(...))` push 协议 | （三方都没等价 wrapper 化设计） | channel-based 模型下"子图作头等节点"语义弱化——任何 Agent 都是 `Run(ctx, sess, input)` 入口，嵌套自然递归。**7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) 内置 push 协议; BasicAgent ReAct 无 sub-agent / GraphAgent 业务自定义拓扑走 `agent_v2.RunSubAgent` helper (#7 F4 commit `f6ede83`) |
| 2026-05 audit (#4 三方对照) | (1) `nodeEventSink.Send` 自动注入 `ev.Metadata["graph_ns"]` (`compose_v2/hooks.go:239`)——业务前端按 ns 分通道展示父子嵌套；(2) `Checkpointer.LoadNS` 明示"非破坏性 + 幂等" godoc 承诺 | LangGraph `stream(subgraphs=True)` 返 namespace tuple（粗粒度 caller 开关）/ eino `forwardCheckPoint` 一次性 delete 消费 | 自动注入比 LangGraph 全局开关更友好（不需声明参数 + 业务自填 graph_ns 时不覆盖）；LoadNS 幂等承诺 vs eino 一次性消费——业务可在 LoopAgent 多 round / retry 路径自由复用无副作用 |
| 2026-09-02 | 子图 import baseline 收成 **OnGraphStart 之前的一处**（原 fresh/resume 两处、都在 hook 之后 ⟹ 子图 OnGraphStart 里 `Get(importCh)` 拿到 `ok=false`，独立审计实测） | LangGraph 图入口的 start 回调拿的就是本图 `input`（`pregel/main.py:2792` `callback_manager.on_chain_start(dumpd(self), input, …)`），子图作节点时即父传入的 state——「start hook 看得到本图输入」是既有形状 | 能收成一处靠的是 baseline 与 Restore 互不相干（Restore 只写 values）；顺带消掉一对会漂的拷贝。哨兵 `TestSubgraph_OnGraphStartSeesImportBaseline` |
| 2026-09-02 | 子图 import 改摆 **baseline**（不经 reducer）、export 只送**子图自己写的**（`setBaseline` / `ownValue`）；`withImportOverlay` 语义随之从「覆盖写」变「摆底层」 | 三方都没等价机制：LangGraph 子图返回全量 output 经父 reducer 写、同款翻倍在测试里被当期望快照；eino 子图输出是单个类型值、无 channel 级映射 | 不抄 LangGraph（那是「让业务自己只返增量」的纪律，不是机制，实测他们自己的测试就翻倍）；不抄 eino（放弃 channel 级 import/export 表达力）。baseline 层零新 cp 字段，F5「续跑读父图最新值」不变量保住 |
| 2026-09-23 | 嵌套调用的存档身份加「第几次调用」：同一次节点执行里对同一个名字的第 k 次调用（k ≥ 1）另落 `#hash(第 0 次的 ns, k)`；tool_call 那一维叠批内下标与 tools 节点访问序号。同一次调用在父节点重跑时换了输入：照样续存档，存档记新开时的输入摘要，对不上只告警（`WithInputDigest`） | LangGraph 的调用序号 `\|k`（同样第 0 次不加）；三家的续跑判定都不看输入 | 不抄它「一次任务执行共用一个计数器」：按位置分开计，别的位置上的调用增减不移动本位置的编号（它那边顺序一变就错位，实测）；计数器随每次节点执行新建，经 `RunWithRetry` 的重试每次尝试回退到同一个编号，不会像它那样拿到 `…\|1`（手写的重试循环每次都算新调用）。不做「按输入内容判续跑还是新开」：三家都不这么做，输入不稳定的父节点重跑时子层会被当成新调用从头跑，挂着的提问不是被旧答案套上、就是被一遍遍重问。详见下节。**身份部分已被 09-28 取代**（改按输入内容，配完成记录） |
| 2026-09-23 | `DeleteSubtree(sid, P)` 只删**自己这次执行**：P 自身与经 `/` 往下的后代；`#`（fan-out 实例）/ `@`（重访）段是同一个节点的兄弟执行，不删（`IsNSInSubtree` 取代 `IsNSAncestor`，三个后端 + conformance 同步） | 无可借鉴：LangGraph 只有整 thread 删（`delete_thread`），eino 没有清理操作（见 checkpoint-storage.md「生命周期清理」一行） | 旧语义按 `ParentNS` 的归组删，于是一次执行的成功清理把兄弟执行还挂着的存档一起删了：同一节点里对同一个名字的第 k 次调用（实测：模型每次换 tool_call ID 时永不收敛）、子图节点同超步 SendFrom 实例的挂起档。框架的每一处清理要的都是「删我这次执行留下的」，清整个任务走空前缀；`ParentNS` 的归组只留给观测。详见下节 |
| 2026-09-24 | 调用次序漂移：能证明的接错终局失败（`TrackSubCall` 调用表 + 摘要比对，两个检查时刻）；加 `agent_v2.WithCallKey`（身份 = 位置 + key，跟次序无关）；摘要改取调用方交来的、OnAgentStart 钩子改写之前的输入，用到时才算 | LangGraph 的调用序号 `\|k` 与我们的次序维同形，没有漂移检测（09-23 探针：跳过前一个调用，后一个读到前一个已完成的存档） | 否决「按输入内容定身份」：链式调用（起草 → 审批问人）实测续跑 4 次、问到第 5 版仍不收敛；位置 + 次序 1 次收敛，审批用的正是用户看过的那一版。详见下节。**已被 09-28 取代** |
| 2026-09-28 | 嵌套调用身份改为「位置 + 输入内容（+ 同一份输入的第几次）」或 key；已完成的调用把整组事件记进父节点这次执行的私有态（新增 `compose_v2.SaveStepResult`），重跑时拿回、不再执行；不带 key 的调用在被调方存档记身份摘要，对不上 = 撞槽 ⟹ `ErrRunAborted`；槽号 63 位；钩子相位每次调用各配计数器；删除 09-24 的调用表 | LangGraph 函数式 API 的 `@task` 同样记住已完成的任务（结果存为 pending writes，重放时同一 task_id 直接返回），但 task_id 按调用次序派生、不看输入，要求重放确定 | 09-24 那版审计实测三条 HIGH，都是「已完成的调用在父节点重跑时必然从头跑、提问身份跟上次相同」这一结构性缺口；09-24 否决按内容定身份的反例（链式调用不收敛）是在没有完成记录的前提下成立的。详见「嵌套调用身份：位置 + 输入内容，记住已完成的调用」一节。撞槽处置与钩子相位两项已被同日第二轮修正 |
| 2026-09-28（第二轮） | 审计后补全：完成记录连同写给上层的转交 / 路由信号一起记、拿回时重放；节点私有态按 tool_call / 钩子相位 / 交出去的执行分域；钩子相位里的调用也记住（身份含调用方节点与相位）；`CondFailRerunFrom` 让按 `Rerunnable` 记的产出作废；tool_call 里的 AgentAsTool / skill fork 只按位置定身份、AgentAsTool 不另记；交给被调方输入的深拷贝；「是不是新任务」挪到 OnAgentStart 之前判、判据改成「上面有没有已建立的任务」；撞槽改为拒绝不清档；槽号固定 63 位；PG 存档列 JSONB → JSON | 取舍「完成 = 采纳」：`@task` 的结果同样一旦记下就在重放里原样返回，要新产出就在代码里再调一次（新的 task_id） | 默认拿回、不按「失败之后重跑」自动作废：框架分不出「后面一步抖了」与「这份产出被否决了」，自动作废会让前者的副作用重放、问过的重问；而「要一份新产出」有两条显式的路（循环再调 / 条件边声明 `CondFailRerunFrom`）。详见「第二轮」一节。其中「`CondFailRerunFrom` 自动作废」、新任务判据、AgentAsTool 按「是不是本身」分档、「深拷贝 → 按存档往返」几项已被同日第三轮 / 第四轮修正 |
| 2026-09-28（第三轮） | 审计后再补：新任务判定的任务章带 sid（换了会话就是换了任务），嵌套调用的执行器替被调方盖章（被调方属于调用方的任务，调用方是谁都一样），`NextTask` 丢 approve_always 规则；AgentAsTool 按「是不是这次 tool_call 调起的工具本身」而不是「祖先里有没有 tool_call」分档；嵌套调用的完成记录默认**不**作废，`CondFailRerunFrom` 只作废调用方用 `agent_v2.WithRerunnable` 点名的那次，且只在条件边真的判过（被打断不算）；BasicAgent 的工具钩子与工具体按钩子 / 工具名分域；嵌套图入口条件边记在外层节点执行名下；被调方输入里业务类型的值按存档往返拷贝 | — | 自动作废（第二轮）在同一个节点里有副作用调用、问过人的把关时把它们一起重做，而框架分不出哪次是「被判定的产出」；作废要调用方点名。只认编排入口的章时，框架外 / 自研 agent 下的嵌套调用每次都被当成新任务、永远续不上。详见「第三轮」一节。其中「任务章带 sid」「`NextTask` 丢规则」「按存档往返拷贝」「否决判据排除 ctx 那档」几项已被同日第四轮修正 |
| 2026-09-28（第四轮） | 任务的身份只认 session 的 RunID（`WithSession` 恒推、存档主索引与任务章都用它、审批规则按它分）；AgentAsTool 不分是谁调起的：一律只记最终消息、全历史输入按位置定身份、不补记历史；`WithRerunnable` 重做整次调用（身份带重做代际）；提问身份带重做代际；嵌套图入口条件边否决推进外层节点的代际；图级相位按「第几遍」分、亲口 `WithFreshRun` 不续；否决判据的 ctx 那档要「ctx 死了且错是它带来的」；输入拷贝不丢类型；摘要逐值兜底 | — | 两把尺（ctx sid 与 RunID）在最自然的写法下分叉；调用栈上的名字分不可靠「是不是这次 tool_call 本身」；「作废外层那条记录」不等于「重做这次调用」。详见「第四轮」一节。其中「`WithSession` 恒推」「嵌套位置上给新 session = 新任务」「AgentAsTool 永不补记」「入口否决当场推进外层整次执行」「否决判据要链上有 ctx 错」「只拷到元素是标量那一层」「摘要只兜底顶层值」几项已被 09-29 第五轮修正 |
| 2026-09-29（第五轮） | 嵌套位置上 session 的 RunID 必须属于所在任务、对不上入口直接拒，Resume 只在顶层、`sessionID` 与 session.RunID 必须一致，开独立任务显式调 `agent_v2.DetachTask`（底下 `compose_v2.DetachExecution`，每个 ctx key 两份清单二选一登记）；重做代际按分域记、入口否决推迟到成员以它失败时才推进；嵌套图里的提问带外层代际链；否决判据回到「ctx 死了就不算」；输入按原类型递归深拷（指针共享）；摘要逐叶子；AgentAsTool 加 `WithAgentToolCallKey`、在 tool_call 之外补记历史；图级相位编号只看「是不是嵌套」 | — | 嵌套位置上新 session 的两种读法各有一种静默出错（当新任务 ⟹ 活锁 + 孤儿存档；当同一任务 ⟹ 别的会话被静默并进来），用户拍板「对不上就拒 + 显式 DetachTask」。否决当场框架不知道外层会不会因它重做，推进整次执行会让兄弟 tool_call 换 ID 重问。(ctx, err) 分不出「判完才到点」与「被打断」，两种误判代价不对称。详见「第五轮」一节。其中「入口否决推进承载它的那一格」（对 RunSubAgent 调起的图推进的是被调方那一格）、「被调方的 agent 级钩子只封私有态」（共用调用方的提问与嵌套调用计数）、「AgentAsTool 按在不在 tool_call 里判补不补」「DetachTask 留下会话 ID 与软取消」「结构体整体算一个叶子」「存档投影宁可多带」几项已被 09-30 第六轮修正 |
| 2026-09-30（第六轮） | 交接（`compose_v2.HandOver`：嵌套调用 / 编排成员 / 决策 LLM / 转交候选）与业务钩子（`EnterPhase`：BasicAgent 工具钩子）各开一个**相位**——自己的私有态一格、提问与嵌套调用计数、相位名；提问身份加「在不在 tool_call 里」与「相位」两维；交接主图的输入算调用方产的、提问不带调用方的外层代际；否决在宿主上另记一笔、成员以非控制流的错失败时标记没传上来也推进；投影只留每步最新一代；图 / 编排入口在钩子之后重铺 sid；DetachTask 记下从哪个任务脱离、剥会话 ID 与软取消、独立运行的挂起不许从节点原样返回；入口拒绝恒不可重试；AgentAsTool 按 tools 节点盖的章判补不补、暂停结算过的输入按调用身份记；摘要线性化并展开结构体 / 非字符串键 map；嵌入结构体深拷 | — | 被调方在自己图外跑的那一段是「被调方的 ns + 调用方的节点与计数」这第三种位置：只封私有态时它问一次就占掉调用方一个序号、跟同名节点撞 ID、否决推进错了对象（相对第四轮是回退）。详见「第六轮」一节 |
| 2026-10-15（第六轮补审） | 交接出去的那一格（`HandOver` 封的那一格与它下面的分域）的重做代际从交接那一层数起、不算调用方那一层；被调方主图的外层代际链不再特判（「外层链 + 承载这张图的那一格」，承载它的正是交接那一格）；交接那一格里的可重做记录换标记 `:hredo`，存档投影对它只按「同一步留最新一代」丢；编排成员 / 内部决策 LLM 的交接相位补哨兵 | — | 第六轮把「交接主图清空外层代际链」当成「调用的身份决定是不是同一次调用」的实现，清掉的却是整条链：调用方所在图之外的层被否决重做时完成记录已丢、被调方重新产出，旧批准兑付给新方案；图外的提问又反过来带着调用方那一层，挂着的调用里答过的问题换 ID 重问。两处都改成按「调用方那一层」切。另：独立运行的中断记号只挂在外面那层包装上、脱离来源只由 DetachTask 记一层，改成中断对象上也打记号、来源由引擎记成一条链（`compose_v2.DetachedFrom`）。详见「第六轮补审」一节 |

## 嵌套调用的存档身份：位置 + 第几次调用（2026-09-23）

> ⚠️ 本节的「第几次调用」按发起次序计，2026-09-28 起改为按输入内容定身份（输入相同的几次才再
> 按次序分），并记住已完成的调用——见「嵌套调用身份：位置 + 输入内容，记住已完成的调用」一节。
> 本节保留作决策历史。

**问题**：节点代码里经 `RunSubAgent` / `AgentAsTool` / skill fork 调子 agent，子存档按
`(sid, 子 ns)` 寻址，而子 ns 原先只由位置派生（父 ns + 名字 + 实例维）。一个节点里对同一个
名字调两次时，两次落在同一个槽上：

- 第一次失败留下可续的失败存档；续跑时节点重跑，第二次调用（没有「新开」信号）把它捡起来
  接着跑，第二次的新输入根本没进子 agent。续错存档这一步不报错：第一次是确定性失败时以
  第一次的旧错收尾（实测：路由失败 → 问用户 → 带补充重路由，子 agent 三次收到的都是最初
  那句话，最后仍报「判不出该去哪个分支」），瞬时失败时拿旧输入的结果当第二次的返回、
  `err == nil`。
- 第二次挂起时，续跑里第一次调用先把第二次的挂起档捡走、把别人的结果当自己的返回，第二次
  再问时撞上已兑付的答案位被终局拒绝。
- tool_call 那一维只用模型给的 ID：provider 每轮都叫 `call_0` 时第 2 轮撞上第 1 轮的槽，
  同批两个 tool_call 同 ID 时两次并发调用写同一个槽。

**三家怎么做**（本地源码 + 探针实测）：

| | 子调用身份 | 同一次执行里调两次 | 续跑时输入变了 |
|---|---|---|---|
| LangGraph | `父ns\|节点名:task_id[\|k]`，k 是本次任务执行里的调用计数 | `d6492ef04`（2025-01）之前抛 `MultipleSubgraphsError`，之后改成计数；顺序一变就错位（探针：跳过前一个调用，后一个读到前一个已完成的存档） | 续存档，新输入静默丢弃（`CONFIG_KEY_RESUMING`；`tests/test_subgraph_persistence.py` 钉着） |
| eino | 结构地址（节点键 / 工具调用 ID）；Lambda 里的图是独立根图、身份由调用方给 | 不编号，撞地址时答案被第一个进入的调用拿走（探针） | 存下来的输入说了算 |
| MAF | 消息驱动，request_id 生成一次随存档保存；函数式 API 按 (step 名, 调用序号) | 共享实例，只告警 | 重新投递同样的消息 |

**没有一家按输入内容判「续跑还是新开」**，唯一对输入做哈希的（LangGraph 的 `CachePolicy`）
缓存的是成功结果、不是续跑判定。

**我们的选择**：

1. **加「第几次调用」这一维**（`compose_v2.NextSubCallIndex`）：第 0 次不加任何东西（与之前
   逐字相同），第 k 次把 (位置, k) 哈希进 `#`，不新增 ns 分隔符（ns 语法不变）。第 k 次与第
   0 次是**兄弟执行**，删除范围随之收窄成「自身 + `/` 后代」（见下一节），否则第 0 次的成功
   清理会删掉第 k 次的挂起档。跟 LangGraph 的两处不同：**按位置分开计**（它一个任务执行共用
   一个计数器，别的调用增减会让后面所有调用错位）；**计数器随每次节点执行新建**（进节点与进
   每个 tool_call 时），续跑从 0 数，经 `RunWithRetry` 的重试每次尝试回退到进入重试时的编号
   ——它那边节点级重试不重建计数器，重试那次拿到 `…|1`。手写的重试循环分不出「重试同一次
   调用」和「带着新输入再调一次」，每次都算新调用。
2. **tool_call 那一维叠批内下标与 tools 节点访问序号**：两者都锚在存档里（下标在那条
   assistant 消息上，访问序号在 `NodeVisits` 里），续跑时算出同一个值。首轮、批内第 0 个时
   只用 ID，与之前逐字相同。SendFrom 实例节点里业务又自己起 tool_call 并发时，实例下标与
   tool_call 两维一起哈希。
3. **同一次调用重跑时换了输入：照样续存档，只告警**。按内容判的话，输入不稳定的父节点（重跑时
   又调了一次模型）每次都会让子层从头开始，已完成的步骤连同副作用重放；它挂着的提问要么身份
   不变（身份按位置算），用户给上一版输入的答案被直接套到这一版的提问上，要么把内容编进身份，
   用户答过的问题被一遍遍重问——两种都比「新输入被丢 + 告警」更糟。所以身份只由位置决定；
   存档新开时记下输入摘要（`Checkpoint.InputDigest`），auto-load 时对不上打一条告警指出 ns，
   让不稳定的父节点看得见。摘要由调用方算（`WithInputDigest`）：`agent_v2` 九个编排取交给图的
   那条输入消息，先按存档的 JSON 往返归一化（否则标注里的 struct、大整数经存档解回后字节不同，
   每次续跑都误报），不含每次构造都铸新的消息 ID、模型响应元数据与每次调模型都新铸的 `run_id`
   标注，也不含转交编排从持久层读回的当前成员；PlanAgent 只对播进图的文本算；顶层调用不算
   （顶层 Run 恒新开，不会被比）。⚠️ 2026-09-24 修订：摘要改取调用方交来的、钩子改写之前的
   输入，用到时才算；能证明是次序变了、接错了存档时不再只告警，改为终局失败——见下节。

**代价与边界**：

- 同一位置上的调用次序在两次执行之间变了，身份就对不上——跟 `Ask` 的提问序号是同一个前提；
  按位置分开计把它收窄到「同一位置」。（2026-09-24 起：能证明的接错终局失败，次序会变的写法用
  `WithCallKey`，见下节。）
- 同一次节点执行里**并发**调同一个名字，谁拿到几号取决于调度；要并发请走 `SendFrom` /
  ParallelAgent / tools 节点的并发 tool_call（每个实例各有计数器）。
- 不在图执行里（没有计数器）时恒第 0 次：那里没有「节点重跑」，名字由调用方自己定。嵌套图的
  钩子相位（OnNodeStart 等）同样不在任何节点执行里，进新图时计数器被清空，那里的调用也恒第
  0 次——与顶层一致，不接着外层节点数（接着数的话序号取决于子图里此前已触发过钩子的节点数，
  续跑时子图 frontier 缩了、序号就漂，实测用户答过的问题被再问一次）。代价是**同一个钩子里
  对同一个名字调两次会落在同一个槽**（钩子相位没有次序这一维），要调两次请给不同的名字。
- 手写的重试循环每次都算新调用、子 agent 从头跑；重试同一次调用要用 `RunWithRetry`。

哨兵：`agent_v2/nested_call_identity_test.go`（失败后带补充再调一次、同名三连调——子 agent
两节点、断言第二次调用的第一个节点只跑一次、第二次调用问两问能收敛、节点里用 `RunWithRetry`
重试续自己的存档、tool_call ID 跨轮复用（AgentAsTool 与 skill fork 各一格）、同批重复 ID、
混合 frontier 下实例里的 tool_call 续跑不漂、换输入告警与「同一句话重建不误报」、摘要经存档
往返稳定、Plan 只对文本算、九个编排各一格）、`agent_v2/nested_call_ns_internal_test.go`、
`compose_v2/subcall_test.go`、conformance 的 `DeleteSubtree_Sibling_Executions_Survive`。

## 调用次序漂移：能证明的接错报错 + call key（2026-09-24）

> ⚠️ **本节方案已被 2026-09-28 取代**（见下一节）：调用表（`TrackSubCall`）与次序漂移检测已删除，
> 身份改为按输入内容。本节保留作决策历史——尤其是「否决按内容定身份」那段理由为什么后来不再成立。

**问题**（上一节方案的审计实测）：按次序定身份的前提是「节点重跑时按同样的次序发起调用」。
常见写法里有几种违反它，后果都是静默的：

- **按进度跳过已完成的调用**（`SaveNodeState` godoc 教的典型写法）：item1 成功、item2 失败；
  续跑时 item1 被跳过，item2 成了第 0 次、item3 成了第 1 次——落到 item2 的失败存档上，把
  item2 的进度续完、拿「处理了:item2」当自己的结果，`err == nil`。
- **条件分支在续跑时插进一次调用**：首跑只调 B（第 0 次，停在提问上）；续跑时先调 A，A 接走
  B 的存档，用户对 B 的批准记在 A 名下，B 在新槽上从头再问一遍。
- **放弃的调用**：同一个名字这次做的是另一件事。

**考虑过、否决的方案：按输入内容定身份**。直觉上「输入变了 = 另一次调用」最干净，实验否了它：
节点里链式调用（起草 → 审批，审批问用户）时，续跑里起草重跑、产出变了，审批就成了新调用、
提问换了身份——实测续跑 4 次、问到第 5 版仍不收敛；按次序定身份 1 次收敛，审批用的正是用户
看过的那一版。「同一次调用、上游重跑后输入变了」与「同一个名字做了另一件事」在框架这一侧长得
一模一样，按内容区分必然牺牲前者，而前者正是 HITL 的主流形态。退一步「输入变了就在同一个槽上
新开」也不行：提问身份按位置算、跟上次相同，用户对上一版的批准被直接套到新一版上（变异实测：
最终产出「批准了:草稿v2」）。

**采用：保持位置 + 次序，加三件事**：

1. **能证明的接错终局失败**（`ErrRunAborted`，错误正文指出出路）。不带 key 的调用登记进本次
   执行的调用表（`compose_v2.TrackSubCall`，按位置分组），被调那层在两个时刻检查：
   - auto-load 时：接到的存档，其输入摘要属于本次执行里**已经发起过**的另一次调用（跳过已完成
     的那种）；
   - 入口处：自己的存档已被本次执行里更早的一次调用以对不上的输入接走（插入的那种）。这一种
     发现时，先接走存档的那次调用已经跑完（它续的是别人的进度、其中的副作用已经发生），但这次
     节点执行随之失败（节点把错误返回出去、而不是吞掉时），错的结果不会被提交。

   `RunWithRetry` 的下一次尝试落回同一个槽，登记时替换旧的那条——重试的是同一次调用，不是它的
   兄弟（否则「每次尝试重建了输入」的重试会被误判成接错）。
2. **`agent_v2.WithCallKey(key)`**：次序会变的写法给每次调用一个业务身份，槽变成
   `#hash(位置, key)`、跟次序无关、不占次序编号；同一次节点执行里重复用一个 key 报
   `ErrRunAborted`（`RunWithRetry` 的重试不算两次）。
3. **摘要改由调用方的输入算、用到时才算**：取调用方交来的那条（OnAgentStart 钩子改写之前，
   做一份浅快照防钩子原地改写），子 agent 自己的钩子往输入里加时间戳不算父节点给了不同的输入；
   只在 auto-load 比对与第一次写存档时算，默认 exit 档的成功路径一次都不算（上一版每层入口都序列化一次整条
   输入，多模态大输入是实打实的回退）。

**剩下的原理性歧义**：同一个名字在两次执行里做了两件不同的事（上次那次被放弃、这次换了输入），
与「同一次调用、上游重跑后输入变了」在框架这一侧不可区分——只告警。两种检查都建立在「输入相同
⟹ 同一件事」上，于是另有一种误报：某次调用合法地换了输入，而本次执行里后面另一次调用的输入
恰好等于它原来那份。两者的出路都是 `WithCallKey`。

哨兵：`agent_v2/nested_call_order_drift_test.go`（跳过已完成：不带 key 终局失败 / 带 key 续自己
的存档；插入：不带 key 终局失败 / 带 key 各自拿到自己的答案；重复 key；带 key 的重试；重试换输入
不误报；带 key 换输入只告警；成功路径零次摘要；钩子改写不误报——原地改写与换消息各一格；链式
调用一次收敛——钉住否决按内容定身份的理由）、`agent_v2/nested_call_ns_internal_test.go`（key 的
槽与次序无关、不占编号、重试不算重复）。10 个变异逐条确认承重。

## 嵌套调用身份：位置 + 输入内容，记住已完成的调用（2026-09-28）

**为什么推翻 09-24 那版**：五个单视角审计 agent 并发复审，实测三条 HIGH，全是同一个结构性缺口
——**已完成的调用在父节点重跑时必然从头跑**（它成功时存档已清），而它的身份按位置 + 次序算、跟
上次相同，于是它问出的问题身份也跟上次相同：

1. **按进度跳过已完成的调用 + 调用方累积传答案**：跳过 a 之后 b 成了「第 0 次」、落在 a 的槽上，
   提问身份与 a 那一问相同——累积答案里 a 的「是」直接套到 b 头上，用户对 b 的拒绝从没被用上；
   开框架工具审批时，被拒的转账照样执行。
2. **链式调用 + 父节点再问一句**（起草 → 审批问人 → 父节点问「发布吗」）：续第二问时审批（已完成）
   从头跑、起草也重跑出新版本，审批提问的身份跟上次相同 ⟹ 用户对 v1 的「是」批准了一版根本没给用户看过
   的草稿。
3. **`RunWithRetry` 包住会提问的子 agent，后面一步瞬时失败**：重试把已完成的子 agent 从头跑，提问
   撞上已兑付的答案位 ⟹ 整个任务 `ErrRunAborted`。

09-24 的调用表只认得「接到的存档属于别人」，对「已完成的那次从头再来、拿到跟上次一样的提问身份」
无能为力；再往表里补规则只会继续追着形态跑。

**新做法**（`agent_v2/run_subagent.go` 的 `enterNestedCall` / `nestedCallNS`）：

- **身份** = 位置 + 「这是哪一次调用」：不带 key 时是（输入摘要，同一份输入在本次节点执行里是第
  几次）；带 `WithCallKey` 时是 key。存档槽 = `JoinFanoutNS(parentNS, name, 身份的 63 位哈希)`。
  身份跟调用次序无关：跳过、插入别的调用都不会让一次调用落到另一次调用的槽上、拿到另一次调用的
  答案。
- **完成记录**：调用成功后把整组事件记进父节点这次执行的私有态（`compose_v2.SaveStepResult`，
  `SaveNodeState` 的按 key 分条版本，随父节点的存档落盘）；父节点重跑时同一个身份直接拿回、不再
  执行。skill fork 同样接上（`activate_skill` 是框架工具、不进 tools 节点的工具记账，挡住重跑的只有
  它自己的完成记录）。
- **撞槽核对**：不带 key 的调用把身份摘要交给被调方（`compose_v2.WithInputIdentity`），记进被调方
  存档；auto-load 时对不上只可能是另一次调用撞进了同一个槽 ⟹ ~~`ErrRunAborted`~~（同日第二轮已改：拒绝、
  不可重试、不清档）。带 key 的只作核对
  （`WithInputDigest`，输入变了告警、照样续）。
- **摘要按存档往返之后的样子算**：续跑时父节点交来的输入可能是从存档里解回来的，`any` 字段按存档
  同样的 JSON 解码归一（struct 变 map、大整数变 float64），非法 UTF-8 按 JSON 规则替换；不含消息
  ID、响应元数据与 `run_id` 标注。
- **钩子相位**（OnGraphStart / OnNodeStart / OnNodeEnd / OnGraphEnd / 条件边）每次调用各配一个次序
  计数器；~~图的钩子相位里发起的嵌套调用盖「嵌套调用章」，不被当成顶层新任务~~（同日第二轮改成
  「上面有没有已建立的任务」、第三轮再改成任务章带 sid + 嵌套调用执行器替被调方盖章、第四轮把任务的身份收成
  session 的 RunID 一把尺）。进新图时清掉节点
  私有态访问器（嵌套图钩子相位曾把 `SaveNodeState` 写进外层节点名下）。

**09-24 否决「按内容定身份」的理由为什么不再成立**：那个反例（链式调用永不收敛）是在**没有完成
记录**的前提下成立的——起草每次重跑产出都变，审批按内容定身份就每次都是新调用。有了完成记录，
起草在续跑里拿回记下的那一版，审批的输入不变、续上自己的存档，一次收敛。变异实测：关掉完成记录
时 `TestNestedCall_ChainedCallsConverge` 八次续跑仍不收敛（问到草稿 v9），打开时一次收敛、起草只
执行一次。

**对照**：LangGraph 函数式 API 的 `@task` 也记住已完成的任务（结果存为 pending writes，重放时同一
task_id 直接返回），它的 task_id 按调用次序派生（父任务路径 + 本次执行里第几次 PUSH），不看输入
——要求 entrypoint 代码确定性重放，次序一变就错位。我们按内容定身份，次序变了照样对；代价落在
「输入不稳定」这一侧（见下）。三家都没有撞槽核对。

**有意取舍（剩余风险）**：

1. **输入每次都变 ⟹ 每次重跑都是新调用、永不收敛**（请求号、时间戳、节点体里直接调模型的产出）。
   出路二选一：`WithCallKey` 声明身份，或 `SaveStepResult` 把那个值记住。`TestNestedCall_VolatileInputNeedsCallKey`
   三格钉住问题本身与两条出路。
2. ~~**钩子相位里完成的调用不被记住**~~（同日第二轮已改：钩子相位里的调用也记住，见下节）。
3. **完成记录存整组事件**：本次 Run 结束前一直留在内存、中断时随存档落盘；`NodeStates` 不设清除点，
   被调方产出很大、调用很多时内存与存档跟着变大。
4. **存档往返有损的值按往返后的样子计入身份**：存档分不出的两个值（相差 1 的两个 > 2^53 整数）
   身份也不区分——续跑时业务自己同样拿不回那个区别。

**一个只有真库才抓得到的坑**：步骤记录在 `NodeStates` 里是派生键，第一版用 `\x00step\x00` 作
分隔符，PostgreSQL 的 JSONB 拒收含 NUL 的字符串 ⟹ 带完成记录的存档一份都写不进 PG（错误只挂在
中断后面，续跑撞 `ErrNoPendingCheckpoint`）。SQLite 上的 conformance 看不出来。现在分隔符是 `:step:`、
业务 key 先哈希再入档，conformance 的 NodeStates 用例带一条 key 里有 NUL 与非法 UTF-8 的步骤记录，
embedded-postgres 上实跑通过。

哨兵：`agent_v2/nested_call_order_drift_test.go`（跳过 / 插入 / 累积答案 + 工具审批 / 链式收敛 /
投票 / 重试 / 带 key 的完成记录）、`agent_v2/nested_call_identity_test.go`（九个编排作被调方三种情形、
存档往返身份稳定、易变输入的两条出路、skill fork 完成记录）、`agent_v2/nested_graph_hook_scope_test.go`
（钩子相位）、`agent_v2/nested_call_collision_internal_test.go`（撞槽）、`agent_v2/nested_call_ns_internal_test.go`
（身份各维、摘要归一与字段覆盖）、`compose_v2/step_result_test.go`。29 个变异逐条确认承重（`0c88ad9a`
提交信息），分隔符那条另由 `a14ab5d1` 的回退实验确认。

## 第二轮：审计之后的补全（2026-09-28）

09-28 那版提交后派了四个单视角审计 agent（身份收敛 / 完成记录保真 / 引擎存档后端 / 钩子相位与新任务
判定），又派两个文档审计。实测的问题逐条复现后归成下面几类，每类都配了哨兵和变异（43 个变异逐条确认
承重，脚本在会话草稿里、未入仓）。

**1. 完成记录只记了 events，漏了「写在别处的副作用」**。被调方经 transfer 工具 / `HandoffTo` 写进上层
HandoffAgent 的收件槽、经 `route_to` 写进 RoutingAgent 的路由信号——都不在 events 里；拿回时子 agent
不执行，转交 / 路由决定静默丢失（实测前台自己作答、路由走了 fallback，`err == nil`）。修法：嵌套调用
挂一个效果记录器，收件槽与路由信号的写入点通知它（只记写进「进调用时看得见的那个通道」的，被调方
内部自己那层 HandoffAgent 的不算），效果随完成记录存、拿回时经原写入点重写。tool_call 的完成记账早就
这样处理控制效果（`replayControlEffect`），区别是那边由工具声明、这边由框架在写入点观测——被调方是
任意 agent，没法要求它声明。（2026-10-08 换代：效果记录器、收件槽、路由的 session KV 与工具声明机制
一起删掉，转交 / 路由改成跟着写入者的提交走的上行信号——完成记录与工具账把那段执行期间收到的信号
随产出记下、拿回时交出；见 [handoff-control-signal.md §8](handoff-control-signal.md)。）

**2. 节点私有态的作用域比「节点这次执行」更细**。三处「不是节点体、却跑在节点执行里」的代码直接写
节点体那一格：工具函数（覆盖 tools 节点自己的工具记账，实测已执行的工具续跑时再执行一遍）、嵌套调用
被调方的 agent 级钩子（同样覆盖）、图的钩子相位。修法是**分域**：`WithCurrentToolCallContext` 派生
tool_call 分域，钩子相位与条件边按相位分域，`WithNodeStateSealed` 给交出去的一次执行（嵌套调用、编排
派发成员、内部决策 LLM）一个分域——各有自己的一格和步骤记录。曾经另加过一层「只记步骤、那一格不可
见」，变异实测它被分域完全覆盖（撤掉它哨兵照绿），按「测不出来的防御是负债」删掉了。

**3. 钩子相位里的调用**：身份里没有调用方节点与 visit（两个节点、同一个节点的两次执行在钩子里用同一份
输入调同一个子 agent 撞成一次，终局 `ErrRunAborted`）；完成了也不记（续跑时重问，只答当前一问的前端
走不完）。修法：引擎给每个钩子相位一个相位名（`compose_v2.SubCallScope`，含所属节点与它的第几次执行），
嵌套调用的身份带上它与调用方节点；钩子相位记在那个节点这次执行名下（嵌套图的图级钩子记在外层节点
执行名下），于是能记住。只剩顶层图的 `OnGraphStart` / `OnGraphEnd` 没有可挂的节点执行（第三轮补：
嵌套图的入口条件边同样记在外层节点执行名下；顶层图的入口条件边与图级钩子一样无处可挂）。

**4. 「完成」被当成「采纳」**。节点拿到嵌套调用的产出、校验不合格要重来——`CondFailRerunFrom`、节点
返错后 Resume、`RunWithRetry` 包「调用 + 校验」、编排对成员重试——完成记录让每一种都拿回同一份坏产出。
这是真冲突：框架分不出「这次调用完成了、后面一步抖了」（该拿回，否则副作用重放、问过的重问）与「这份
产出被否决了」（该重做）。取舍：

- `CondFailRerunFrom` 的契约本来就是「上游产出不行、重跑它换一份」——这一档自动作废：步骤记录可以标
  `Rerunnable`（是产出不是副作用），cond 按这一档报错时节点这次执行的**重做代际**加一，旧代际的记录
  读不到、也不再导出进存档。~~嵌套调用的完成记录按这一档记~~（第三轮已改：默认普通记录，调用方用
  `agent_v2.WithRerunnable` 点名才按这一档记）。节点状态设计里「不设清除点」的不变量不动
  ——作废靠代际让旧记录不可达，不删。
- 其余几种默认**拿回**：失败之后的重跑本来就是「重试失败的部分、成功的不重放」，自动作废会把「后面
  一步抖了」那种情况的副作用重放掉。「要一份新产出」有两条显式的路：在节点里循环调（同一份输入的第 k
  次就是第 k 次调用、各自执行），或把校验放到条件边上声明 `CondFailRerunFrom`。godoc
  （`RunSubAgent` / `RunWithRetry` / `CondFailRerunFrom`）写明了这件事。
- 对照：`@task` 的结果同样一旦记下就在重放里原样返回，要新产出就在代码里再调一次（新的 task_id）——
  同一个取舍。

**5. 身份漂移的两个入口**：被调方的 `OnAgentStart` 原地改写调用方交来的消息（通常就是 channel 里那
条），改动顺着指针进了调用方的存档，续跑时摘要变了、换槽重问（实测每次续跑换一个提问 ID）——交给被调方
的改成深拷贝（逐字段，`any` 值里 JSON 形状的递归拷；新增字段时字段覆盖用例会红）。`AgentAsTool` 的
全历史输入从 session 事件现拼、事件不进存档——tool_call 里的 AgentAsTool（第三轮收窄为「tool_call 调起的
AgentAsTool 本身」，第四轮改为「全历史输入的 AgentAsTool」）与 skill fork 改为只按位置定
身份（一个 tool_call 就是一次调用），~~AgentAsTool 在 tool_call 里也不再另记完成记录（tools 节点已记账，
另记只是把整组事件重复存一份）~~（第四轮已改：AgentAsTool 一律记，只记最终那条消息）。

**6. 新任务判定排在钩子之后**。`OnAgentStart` 里经 `RunSubAgent` 发起的调用那一刻新任务还没宣告：
用上个任务的 approve_always 静默执行工具、续上被放弃任务留下的存档；BasicAgent 的判定只进了跑图的
局部 ctx，`OnAgentEnd` 看不到。改成九个编排入口在 `OnAgentStart` 之前判，判据从「是不是嵌套调用」换成
「上面有没有一个已经建立的任务」（入口盖章、往下继承；~~框架外的调用上面没有任务，就是新任务~~——第三轮
已改，见下），钩子之后重铺；机械对账钉住九个入口。

**7. 零碎**：撞槽从终局清档改为拒绝不清档（槽里那份属于另一次调用，且抛在图开始执行之前）；标注表
整张一起按存档往返归一（含非法 UTF-8 的键替换后顺序翻转 / 合并时摘要曾变）；槽号固定 63 位
（`JoinFanoutNS` / `NSSegment.Index` 改 int64，与平台字长无关）；PostgreSQL 存档列 JSONB → JSON——完成
记录把子 agent 的转录带进父存档，转录里一个 NUL 就让整份存档写不进 PG（真 PG 实测；JSON 类型接受
`\u0000`，真 MySQL 同样实测通过）；`reactive_compact` 读整数标注只认 `int`，续跑后（标注经 JSON 往返
成 `float64`）读不到——同一成因的既有缺陷，顺手修。

**仍在的取舍**：

1. 「产出不合格要重来」要走显式的两条路（见 4），不自动作废。
2. 输入完全相同的几次调用靠次序区分，按进度跳过要给 key（见 [checkpoint.md](../../v2/checkpoint.md) §4.2）。
3. 完成记录本次 Run 结束前留在内存；进程在节点执行中途崩溃时已记下的步骤跟着丢（中途存档不带它）。
4. 拿回的 events 是存档往返后的形态（标注值按 JSON 类型），与首跑不逐字节一致。
5. 顶层图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边里完成的调用不记（没有节点执行可挂）。

## 第三轮：再审之后的修正（2026-09-28）

第二轮提交后又派了四个单视角审计（逃逸信号记录与重放 / 私有态分域与重做代际 / 任务判定·身份·输入拷贝
/ 文档声明），发现逐条用它们的探针在本分支复现后修；23 个变异逐条确认承重（脚本在会话草稿里、未入仓）。

**1. 「完成」与「产出」要调用方点名，不能框架替它判**。第二轮让 `CondFailRerunFrom` 作废节点这次执行里
**全部**嵌套调用的完成记录，实测同一个节点里先扣款、再调 planner，条件边否决计划时扣款也重做（扣两次）；
`OnNodeStart` 钩子里问过人的把关也重问。框架分不出哪次调用是「被判定的产出」、哪次是「顺带做成的事」，
所以默认改回**普通记录**（怎么重跑都拿回），调用方用 `agent_v2.WithRerunnable()` 点名产出那一次。默认选
「拿回」是因为两种错的代价不对称：漏点名的后果是条件边一直否决同一份产出（响亮、重试预算耗尽就报错），
多作废的后果是副作用静默重放。

**2. 被打断不是否决**。条件边里的判官（经 `RunSubAgent` 调的 LLM）跑着时用户按了停止、或调用方 ctx 被
取消，`CondFailRerunFrom` 档把这个错也包成「否决」、推进重做代际——产出根本没被判过就作废了，续跑重做一遍
（实测连同同一节点的扣款）。现在只有判过的错推进代际（排除 `IsRetryFutile` 那一档：软取消、调用方 ctx
取消、陈旧写回）；判官自己的基础设施错（它调的模型 503）分不出来，仍按否决处理。（第四轮把 ctx 那档改成「ctx 死了
且链上有 ctx 错」，第五轮又改回只看 ctx，见下。）

**3. 任务章带 sid，嵌套调用的执行器替被调方盖章**。第二轮的判据「上面有没有已建立的任务」有两个方向的错：
- 章不认会话：拿着任务里的 ctx（被业务留下的钩子 ctx、节点体、工具体）给**另一个会话**开独立任务，也被当成
  这个任务的一部分——不清那个会话上个任务的 approve_always（实测工具静默执行）、不推「新开」信号（实测新
  任务的输入被那个会话上个任务留下的存档吞掉）。现在章里存任务的 sid（~~就是存档用的那个：ctx 上已推的
  sid，否则 session 的 RunID~~——第四轮改为只认 session 的 RunID，见下），节点执行范围那枚章也在进节点时
  记下图的 sid（`compose_v2.NodeScopeSessionID`），对得上才算同一个任务。
- 只认编排入口的章：业务自研的顶层 agent（不是九个编排之一）、框架外业务自己的循环、直接跑 `compose_v2`
  图时图级钩子里发起的 `RunSubAgent` / `AgentAsTool.Call`，每次都被当成新任务——推「新开」信号、不续自己
  的存档、换一个 ID 重问，**永远续不上**（实测）。嵌套调用按定义属于调用方的任务，现在由执行器替被调方盖章。
  业务在自己的循环里开新任务照常换 RunID；~~`NextTask` 同时丢弃 approve_always 规则~~（第四轮改为规则按
  RunID 分、不在哪一刻清，见下）。

**4. AgentAsTool 的分档判据**（第四轮整个撤掉，见下）。第二轮按「ctx 里有没有 tool_call ID」判「在 tool_call 里」——那说的是「祖先
里有 tool_call」。业务的组合工具体里、被调方钩子里再调 `AgentAsTool.Call` 时也满足，于是按 tool_call 那套
只看位置、不另记完成结果；可 tools 节点记的是外面那个工具的账，里面这次调用完成了没人记，续跑时整个重跑
（实测：用户批准的是草稿 v1，续跑重新起草，执行的是 v2，`err == nil`）。现在 tools 节点在调起工具前盖一枚
「这次调起的是谁」的章（工具名，`OnToolStart` 改写之后的），AgentAsTool 只在名字对得上时走 tool_call 那套，
嵌套调用的执行器交给被调方之前摘掉它。按名字比不按实例比：业务包一层同名的转发装饰器时调起的仍是这次
tool_call 的工具本身。

**5. 分域补漏**。BasicAgent 的 `OnToolStart` / `OnToolEnd` 钩子跑在 tools 节点体里、拿节点体的 ctx——钩子里
一次 `SaveNodeState` 就覆盖 tools 节点的工具账，已执行的工具续跑时重放（实测，第二轮之前就有，第二轮删掉
「别在工具里调」的警告后更容易踩）。现在调这几个钩子时封住节点那一格（`WithNodeStateSealed`，按钩子名 +
所属 tool_call 分域；第四轮再带上工具名），采纳钩子返回的 ctx 时铺回节点那一格（新增 `compose_v2.WithNodeStateFrom`，第六轮换成 `WithPhaseFrom`，2026-10-09 并进 `AdoptHookCtx`——执行状态整份取框架的）。工具体的
分域另按工具名再分一层：同一个 tool_call 两次执行之间被钩子改到另一个工具时，工具账（键带工具名）不命中、
重跑，工具体里记的步骤也得跟着不命中（实测新工具读到旧工具记的「发过了」）。嵌套图的入口条件边改为跟
图级钩子一样记在外层节点执行名下。

**6. 零碎**：路由信号的记录器只比 ns、不比 session，被调方另开一个 session 跑路由 agent（那次写入本来不
生效）时照样记下，拿回时重放进调用方的 session、凭空生效——现在同时核对写进的是不是进调用时那个 session 的
存储。交给被调方的输入里，业务类型的 `any` 值（`[]string`、结构体……）原先共享，被调方钩子原地改就写回了
调用方（续跑时摘要漂、重问）——~~现在按存档往返一遍，被调方首跑看到的形态跟续跑时一样~~（第四轮改为
按原类型拷、不再往返：按存档往返实测首跑就丢精度、丢类型；第五轮改为按原类型递归深拷，见下）。

**当时仍在的问题（2026-10-08 已修，见 [handoff.md §6.3](../../v2/handoff.md) 与 [handoff-control-signal.md](handoff-control-signal.md) 决策记录）**：转交 / 路由这两条**旁路信号**的生命周期——写入者
所在的那一步已经提交、续跑不再调它时（两步分类图第一步定路由第二步问人；HandoffAgent 候选是
`Sequential[分诊(转交), 确认(问人)]`；业务工具里经 `RunSubAgent` 定路由、同批另一个工具中断），信号在中断
出口被清、没人重写，续跑后转交 / 路由丢失，`err == nil`；失败的那次尝试写下的转交留在活通道里生效；被调方
内部并发写同一个信号键不报冲突。这些在第二轮之前就有（完成记录那套只覆盖「再调一次、被拿回」这一条路），
根治要让信号跟随写入者的提交走（随节点提交、进存档、在图完成时向上交），是 HandoffAgent / RoutingAgent 信号
传递的重做，单独立项。

## 第四轮：再审之后的修正（2026-09-28）

第三轮提交后又派了四个单视角审计（任务判定 / tools 节点一侧 / 可重做点名·否决判据·输入拷贝 / 文档声明），
逐条用它们的探针在本分支复现后修；22 个变异逐条确认承重（脚本在会话草稿里、未入仓）。

**1. 任务的身份只有一把尺：session 的 RunID**。第三轮的章存「ctx 上已推的 sid，否则 RunID」——两把尺，在
最自然的写法下分叉：长寿 ctx 上钉着上一个任务的 sid（`WithSession` 只在没有 sid 时才推），业务按契约
`NextTask` 换了 RunID，框架照旧认旧 sid，新任务接着跑被放弃任务的存档（实测：新输入没进图、重问旧问题）；
拿着任务里的 ctx 给另一个会话开任务也被当成那个任务的一部分（既有用例恰好都显式推了新会话的 sid，把前提
藏住了）。现在任务的身份就是 session 的 RunID：`WithSession` 恒把 ctx 上的 sid 推成它，九个编排跑图用它当
存档主索引，嵌套调用的执行器给被调方同一个 RunID 的 session（调用方没有 session 时沿用 ctx 上的 sid），字面量
session 在 Run 入口就地补上 RunID。（第五轮：嵌套位置上 RunID 对不上所在任务的直接拒、字面量空 RunID 也拒、被调方
的 RunID 取所在任务，见下。）approve_always 规则按 RunID 分：第三轮让 `NextTask` 清规则集，values 表被
几个任务共用时（半共享子 session），旁路任务开始的那一刻把父任务的规则一起清了，反过来旁路任务又拿得到父任务
的规则；按 RunID 分之后换了 RunID 就是新的一套，不必在哪一刻清。

**2. AgentAsTool 不分是谁调起的**。第三轮按「tools 节点调起的是不是它本身（名字对得上）」分档，实测三个方向
都错：同名装饰器 / 中间件在转发之后问人——这次 tool_call 没完成、tools 节点的账里没有它，AgentAsTool 又被认作
「本身」不记，续跑重新起草，用户批准 v1、执行的是 v2；组合工具里调全历史输入的 AgentAsTool——不是本身 ⟹
按内容定身份，而全历史输入续跑时拼出来的不一样 ⟹ 换槽重问；异名转发壳 / 中间件改名 / 自写 toolsFn——不是
本身 ⟹ 边界补记，而产出照样成为 tool_result ⟹ 历史里两份。调用栈上的名字可以被改写、转发、复用，框架分
不可靠，现在不分：一律记完成记录（只记最终那条消息，存档不膨胀）、全历史输入只按位置定身份、~~永不补记历史~~
（第五轮改为只在 tool_call 里不补记：节点体、钩子里调时没有 tool_result 兜底，见下；组合工具要留痕改用
`RunSubAgent`）。provider 不回 tool_call ID 时，「在不在 tool_call 里」看
`WithCurrentToolCallContext` 盖的章（`compose_v2.InToolCall`）而不是 ID 非空（按 ID 判时同批两个相同输入的
调用落进同一个槽、提问撞 ID 终局失败）；tools 节点的工具账也照记空 ID（键带批内下标，曾经整个不记、续跑重放
扣款）。工具钩子（OnToolStart / OnToolEnd / OnError）的分域带上工具名：续跑时 OnToolStart 把同一个
tool_call 改到另一个工具上，OnToolEnd 曾读到旧工具的记录、新结果没过审计就放行。

**3. `WithRerunnable` 重做的是整次调用**。只作废外层那条完成记录的话，被调方是业务自研 agent（它在自己的 Run
里经 `RunSubAgent` 调真正干活的 planner）、或它的钩子 / 它的图的入口条件边里调了东西时，外面这次重新执行
了、里面那次原样拿回，产出不变，续跑多少次都一样（实测）。现在点名的调用身份里带上父节点这次执行的重做代际
（`compose_v2.RerunGeneration`）：存档槽、被调方在自己的图之外记下的东西、它问的问题都换新——跟被调方是图时
（成功时存档已清、整个重跑）同一个语义。只有父节点直接发起的那一层点名才算数：外层是普通记录时整次拿回，
里层的声明走不到。

**4. 重做时问的是新问题**。否决不推进 visit，节点重做时提问的身份跟被否决那次完全相同：调用方累积传历次答案
时，对「批准方案 v1 吗」的回答直接兑付给了 v2，v2 的问句从头到尾没出现（第二轮起就有）。提问身份加一维重做
代际（`InterruptSite.RerunGen`，为 0 时不计入——从没被否决过的提问 ID 不变）。只传当前挂起那条答案的调用方
本来就会被重新问到，这一维让两种写法结果一致；想让一个回答跨重做保留，用 `SaveStepResult`（不带
`Rerunnable`）记住，那一问放进自己的相位（`EnterPhase`）里问——跟后面的提问同一个相位的话，续跑时跳过它会让
后面的提问挪到它的 ID 上、拿到它的答案（2026-10-15 补，见 `compose_v2.Ask`「按同样的次序问」）。按进度跳过
做完的几件事（`SaveNodeState` 的典型用法）、每件都问人时同样会挪，那几处的说明 2026-10-16 补齐。跳过时调
`NextAnswerSlot` 占位也能让后面的不挪，不采用：占几格要跟被跳过的那段问了几问一致——那段问了两问、只占一格时，
批准拿到的是第二问的答案（实测）；放进自己的相位不用数，同一次执行里相位名重复时第二问被当场拒（终局失败），
不会静默。

**5. 入口条件边与图级相位**。嵌套图的入口条件边自第三轮起记在外层节点执行名下，但它按 `CondFailRerunFrom`
否决时没人推进代际：里面点名可重做的 router 原样拿回，永久活锁。现在推进外层节点这次执行的代际——「从
StartNode 重跑、让它重新产出」的上游就是外层节点。（第五轮改为推迟到成员以这个错失败时、只推进产出那份输入的
分域，见下。）同一次外层执行在循环里对同一个 ns 跑两遍嵌套图时，图级
相位只按 ns 分，第二遍拿回第一遍的路由；现在按「外层这次执行里第几次跑这个 ns」编号（外层的次序计数器，按 ns
分开数，外层重跑时从 0 重新数）。嵌套图亲口声明 `WithFreshRun`（ctx 上本来没有这个信号）时图级相位什么也不
续、不记——外层节点因为别的错重跑时，入口条件边曾照搬上一次的路由。

**6. 否决的判据**。第三轮 ctx 那档只看 `ctx.Err()`：判官判完了、给出否决，ctx 恰在检查之前到点，就不推进代际
（多一次续跑、多判一次）。现在要「ctx 死了、且错是它带来的（链上有 ctx 错）」两个都成立才算被外面停下；判官
调的后端自己超时（调用方 ctx 还活着）仍按否决。（第五轮改回只看 ctx，见下。）cond 自己 panic 也按否决：上游数据缺了一块、读到 nil，正是
`CondFailRerunFrom` 要重做上游的情形。

**7. 输入拷贝与摘要**。第三轮把业务类型的值按存档往返一遍，实测首跑就丢东西：`int64` 超过 2^53 丢精度、
`json.Number` 成 `float64`、只有未导出字段的句柄与 error 成空 map、`[]byte` 成 base64 字符串，业务
MarshalJSON 的 panic 直接冒出去；自引用的 map 让进程栈溢出退出（recover 不住）。存档往返的损失是存档自己的
限制、续跑时才有，不该让每一次首跑先吃一遍。现在拷完类型不变：标量（按种类判，`json.Number`、具名枚举也算）原样，
JSON 形状的值与元素是标量的切片 / map 按原类型拷一份（第三轮要防的 `[]string` 原地改写照样防住），其余业务值
原样共享，拷到路径上已出现过的容器时原样共享。输入摘要里一个序列化不了的**顶层**标注值（NaN、函数）曾把整张标注
表换成同一个标记——两份只在工单上不同的输入算出同一个身份，父节点重跑时换了工单，对工单-1 的批准用在了工单-2 上；
现在只把那个值换成标记。（第五轮：只拷到「元素是标量」那一层不够、空切片被拷成 nil、坏值嵌在里层时照样塌缩，
改为按原类型递归深拷与逐叶子摘要，见下。）

**仍在的问题（记在 [todo.md](../../code/todo.md)）**：
- 在另一个任务的执行范围里开独立任务或续跑别的任务：继承来的「新开」信号、任务代号、ns 不按任务分——在任务 Y
  的工具里续跑任务 X 撞 `ErrNoPendingCheckpoint`；独立任务的存档落在外层 ns 下、从根上续不上；节点里拿新
  session 裸调时子层每次从头跑（问题却配得上答案，因为任务代号是继承的）。
- 节点 / 工具体 / 钩子里拿**父的** session 裸调别的 agent（没有自己的 ns）：存档落进父图自己的槽；两个
  BasicAgent 的图不带名字、身份相同，GraphID 守卫拦不住，子层接着跑父图的存档，成功时还把它删掉。**2026-10-28 已修**：
  配了 checkpointer 时 `compose_v2.Run` 入口判「在节点执行范围里 + sid 与 ns 都等于所在那张图的」⟹ 当场拒、指向
  `RunSubAgent`（调用方手上就有出路，守卫合法）；当时没给 BasicAgent 的图起显式名字——那只能挡住身份相同的那一半。
- 经 `EnterRun` 进入的自研壳在自己的阶段里裸调 x、再透传给同 ns 的 inner：槽里是 inner 的挂起档，却没有执行占着它，
  x 接着跑那份档（旁路工具执行一次、inner 那一问的答复没被用上，`err == nil`）。入口分不出「壳把调用交给 inner」和
  「壳裸调 x」，在入口占槽会挡掉合法的透传。**2026-10-29 改成靠图身份**：LangGraph 让同一节点里的多个子图按调用次序
  自动落在不同地址，eino ADK 每个 agent 进入时把自己的名字追加进地址——都是让地址在结构上不撞，没有显式移交凭证这种
  做法。按次序编号分不清透传与裸调（壳先调 x 再透传时 inner 被挪走、续不上），取后者的思路：BasicAgent 显式起了名字时
  图身份是「名字:结构指纹」（`compose_v2.GraphBuilder.WithIdentityPrefix`；各编排的图本来就用名字），x 载入 inner 的档撞
  `ErrGraphIDConflict`、不执行。我们得到的是「报错」而不是 eino 那样「两者都能跑」——裸调本身是误用，出路是
  `RunSubAgent`。没起名字的仍只用指纹（自动名每次构造都变，每个请求重建 agent 的调用方续不上自己的档），两个都没起名字、
  结构又相同时照旧接着跑。
- `isFrameworkTool` 按名字判：业务工具叫 `plan` / `respond` 时绕过审批、不进工具账。
- 中间件在 `next` 之后挂起时，被它包住的普通工具续跑时重放（工具账只在整个 tool_call 完成时记）。
- 自写 toolsFn 拿不到「工具体按工具名分域」。
- 旁路信号（转交 / 路由）的生命周期（第三轮已记）。

## 第五轮：再审之后的修正（2026-09-29）

第四轮提交后派了五个单视角审计（任务身份 / AgentAsTool 与 tools 节点 / 重做代际与图级相位 / 输入拷贝与摘要 /
文档声明），发现逐条用它们的探针在本分支复现后修；34 个变异逐条确认承重（脚本在会话草稿里、未入仓）。

**1. 嵌套位置上的 session：对不上就拒，开独立任务是显式动作**（用户拍板）。第四轮「任务的身份只认 RunID」在
顶层对，嵌套时实测三种形态把一个任务劈成两半：M-04 续跑（`Resume(A, WithResumeSession(s))`、s.RunID=B）让嵌套
层按 B 找存档、从头重放，成功后按 B 清子树把任务 B 挂着的存档删掉（`err=nil`）；嵌套调用给新 session
（`inner.Run(ctx, NewRunSession(), in)`，教程就这么写）每次父节点重跑都新开一个任务，内层问两个问题就永远答
不完（`asked=[Q1 Q2 Q1 Q2 …]`、每轮一份孤儿存档），观测会话与卸载数据也跟着泄漏；直接跑 `compose_v2` 图、ctx 里
另推了一个 session 时，`RunSubAgent` 的被调方跟 session 走、落在图的任务之外。框架分不出「有意开旁路任务」与
「为隔离事件随手 new 了一个」，两种读法各有一种静默出错，所以不猜：
- **所在任务**（`enclosingTask`）：节点执行范围记下的图 sid 优先，其次任务章；在没配 sid 的图的节点里时「有任务
  但没有身份」——不新开、也没有 RunID 可核对。
- 在任务里，session 的 RunID 必须属于它，对不上入口直接拒（`%w ErrRunAborted`：确定性错误、不重试、所在任务的
  存档清掉）；RunID 为空的字面量 session 也拒（框架替它补就得改业务的对象，同一个字面量并发用时那一写是 data
  race）；Resume 在任务里一律拒，`sessionID` 与 `WithResumeSession` 的 RunID 对不上也拒——顶层的拒绝不 wrap
  `ErrRunAborted`（什么都还没执行，存档原样在，改对了再来）。
- 嵌套调用的执行器给被调方的 session 的 RunID 取所在任务（不取父 session 的——两者只在「直接跑图、ctx 里另推了
  session」时不同），框架补的 session（调用方传 nil）同理。
- **`agent_v2.DetachTask(ctx)`** 开独立任务：剥掉当前执行留在 ctx 上的状态（session 与任务章、节点位置与 ns、任务
  代号、新开信号、续跑答案与兑付账本、节点私有态访问器、嵌套调用计数、历史写入器、转交 / 路由槽（2026-10 起换成收件人、信号收集处、收件人密钥与节点授予）、per-call 覆盖
  ……），留下调用方配的环境（checkpointer、两层编排钩子、调用主体、会话 ID、历史配置、store、durability、取消；会话 ID 第六轮改成剥掉，见下文第六轮第 4 节）。
  compose_v2 那一半是 `DetachExecution`。剥不剥是一个要想清楚的决定：每个 ctx key 都在「剥 / 留」两份清单里二选一
  登记，机械测试扫全包的 `context.WithValue` / `Value`，漏登记就红。
- 代价：教程 adv_09 与壳里 `NewRunSession()` 的写法会报错，改成传收到的 session / `RunSubAgent`（已改）。

**2. 重做代际按分域记、推迟推进**。第四轮在嵌套图入口否决的**当场**推进外层节点**整次执行**的代际。否决当场框架
不知道外层会不会因它重做——错可能被 AgentAsTool 吞成工具失败结果、被 `RunWithRetry` 消化、被业务兜底——而推进
整次执行会让同一次执行里别处的身份跟着变：同一批兄弟 tool_call 挂着的提问换 ID 再问（不加同步 30 次里 29 次）、
挂起中的可重做调用换槽重来重新起草、钩子相位的否决作废节点体的草稿。现在：
- 代际按分域记（执行级 `<键>:rerun`、分域 `<键>:rerun:<分域哈希>`），一个分域的有效代际 = 执行级 + 分域链上每一层
  之和（每项只增不减，和也只增不减）；访问器记下自己的分域链。
- 入口否决时给错误打上标记（`nestedEntryRejected`：哪个 cm、哪次执行、哪个分域产出了这张图的输入），外层图在
  **那个成员以这个错失败时**推进那个分域（多错误树里每个标记各推一次）；被吞掉、被消化就不推进。成员自己的出边
  否决照旧推进执行级。
- 存档投影只丢「代际比执行级还小」的可重做记录——派生键里只有分域的哈希、拼不回分域链，判不出分域推进之后的
  过期；宁可多带几条读不到的，不能丢一条读得到的。

**3. 嵌套图里的提问带外层代际链**（第四轮之前就有）。外层节点被条件边否决、重做时，节点里跑的嵌套图（直接跑、
`AddSubgraphNode` 都一样）从头跑、在同一个位置问——嵌套图有自己的 cm，它的节点执行代际恒为 0，提问 ID 跟被否决
那次完全相同，累积传的「批准 plan-v1」兑付给了 plan-v2（`err=nil`）。`InterruptSite` 加一维 `OuterRerunGens`：
进每张嵌套图时记下承载它的每一层外层执行此刻的有效代际（从外到里的元组，全 0 时为空、不计入哈希）。用元组不用
和：内层图重跑时自己的代际从 0 数起，求和会让「外层没重做、内层重做过一次」与「外层重做过一次、内层没有」撞成
同一个值。

**4. 否决判据回到保守侧**。第四轮要「ctx 死了且链上有 ctx 错」才算被外面停下，为的是「判完给出否决、ctx 恰在这之后
到点」那一格。但判官被打断时返回的错不一定 `%w` 着 ctx 错（业务自写的判官、第三方 SDK 用 `%v` 包一层就断链），
那种情况被判成否决：用户按了停止，续跑重做了已经完成的产出。(ctx, err) 分不出这两格，而两种误判代价不对称——
把「判完才到点」当没判完只多一次续跑、多判一次；把「被打断」当否决会重做产出连同里面的副作用。现在 ctx 死了就
一律按没判完（`!IsRetryFutile`）。

**5. 图级相位编号只看「是不是嵌套」**。第四轮按「亲口新开之后被置 nil 的外层访问器」决定带不带「第几遍」编号：
agent_v2 顶层任务首跑时 ctx 上带着新开信号（算继承、带编号）、续跑时没有（算亲口、不带），同一张嵌套图首跑与续跑
的图级相位名不同，那里发起的调用换了身份（实测同一个把关问题问两遍）。现在编号看置 nil 之前的那个（承载它的外层
执行），入口否决时推进的、提问带的外层代际也是它——亲口新开只影响「记在外层名下的图级相位记录拿不拿回」。

**6. 输入拷贝按原类型递归深拷，摘要逐叶子**。第四轮只拷到「元素是标量」那一层：`[]map[string]any`、
`map[string][]string`、具名的 `map[string]any` 这些最常见的 JSON 形业务容器照样共享，被调方钩子原地脱敏一个元素就
写回了调用方——续跑换身份重问、已完成的扣款重放（实测 7 种形态）；并发扇出时框架读摘要（遍历原件）与兄弟被调方
的钩子写同一个 map，进程直接 fatal（recover 不住，5/5）；空的 `[]any{}` 被拷成 nil，交给远程 A2A agent 时整条消息
被对端拒收。现在按原类型递归：切片、数组、map 不管元素是什么都拷，接口里的值拷动态值，值结构体整体复制再逐个拷
导出字段；已拷过的容器按「类型 + 底层指针 + 长度」记下拷贝（环与共享在拷贝里保持同样的形状——曾按「指针 + 长度」
相加认身份，同一数组上一长一短两个切片能撞）；**指针、函数、channel 原样共享**（指针的身份可能就是意义：句柄、
客户端、锁，深拷会拷坏）——经指针原地改仍会写回调用方，文档写明别这么改。摘要：整体往返得回来就是原来那一步（好值
的摘要不变）；往返不回来时逐叶子走，坏叶子按「路径 + 怎么坏的」单独进哈希（不再换成一个合法的字符串标记，也就
不会跟业务字符串撞）——曾经坏值嵌在标注的 map 里、在数据块里、或是一个解得出却解不回来的大数（`1e999`）时整个值
塌成一个标记，工单-1 的批准兑付给工单-2（实测三种都中）。结构体、自定义编码的值整体算一个叶子。

**7. AgentAsTool 与工具侧**。
- 参数在模型消息与工具体之间被改写（`OnToolStart` 往 `ArgumentsJSON` 注入时间、中间件往 args 塞 trace_id）时，
  按内容定身份每次续跑都是新调用，被调方从头跑、换 ID 重问，永远收敛不了（实测续跑 4 轮仍在问）。改回「tool_call
  里只按位置」不行：组合工具里按进度跳过时，后一项静默拿到前一项的结果（比不收敛更坏，实测）。加
  `WithAgentToolCallKey(fn)` 让业务从 args 取不含注入值的 key（同 `WithCallKey`），`OnToolStart` 与中间件的 godoc
  写明这个坑，unmatched 答案的告警补上这一种成因。（2026-10-14 起 `OnToolStart` 的改写只在一批调用第一次执行时生效、
  续跑沿用，它注入的值续跑不变、不声明 key 也一次收敛；这个坑只剩中间件那一种成因。）
- 第四轮让 AgentAsTool 永不补记历史，理由「产出多半会成为 tool_result」在节点体、被调方钩子里根本不成立——非
  BasicAgent 的被调方在那两处调用时，输入与产出在会话历史里一条不剩（实测 0/0）。现在只在 tool_call 里不补记
  （`!compose_v2.InToolCall`）；嵌套调用的执行器交给被调方的 ctx 摘掉「在 tool_call 里」的章（新导出
  `compose_v2.LeaveToolCall`：被调方的 agent 级钩子不是那次 tool_call 本身；第六轮删掉，换成 `HandOver`，补不补也改按 tools 节点盖的章判）。组合工具里仍不补，要留痕改用
  `RunSubAgent`。
- 自定义 schema 与全历史输入同时设时 schema 优先，身份、派生标记、输入分支三处改用同一个判据（曾经身份按全历史
  只取位置，换参数重试时 3 次尝试都续上第一次的失败存档）。
- skill fork 判「在不在 tool_call 里」改看章（provider 不回 tool_call ID 时每次 fork 都失败）。

**8. 零碎**：`compose_v2.Run` 的 godoc 写明「同一次执行里对同一个 ns 跑几遍嵌套图，每一遍都照跑、别按进度跳过」
（跳过前一遍的话后一遍的编号前移、拿回前一遍记下的路由）；`CondFailRerunFrom` 的 godoc 写明顶层图入口没有上游、
不存档（直接重跑那次 Run）。

**仍在的问题（记在 [todo.md](../../code/todo.md)）**：
- **节点里直接跑的嵌套图跑完之后节点又失败**（本轮自查发现，第五轮之前就有）：续跑时嵌套图从头再跑（它的存档
  在成功时已清），在同一个位置问同一个 ID，累积传的旧答案被兑付给重跑出来的新内容（实测：只问过「批准 v1」，
  执行的是「v2 已批准」）。嵌套图成功时把兑付记进外层节点的在途账本，外层节点随后失败时那份账作废。godoc 已写明
  出路（记下它跑完了续跑时跳过 / 经 `RunSubAgent` 调）；根治需要给「从头重生的嵌套图」一个新身份，单独立项。
- `a2a.RemoteAgent` 的远端 task 句柄只按「agent 名 + RunID」分，同一个任务里两次调同一个远程 agent 共用一个远端
  task（第五轮之前就有）。
- approve_always 规则不分 agent（沙箱子 agent 里答的「一直允许」让父 agent 同名同参的生产操作静默执行，第五轮之前
  就有）；按 RunID 分的规则集在任务结束后不回收。
- 调用方现拼输入、被调方中断过时，续跑完成后被调方的输入在历史里记两份（第五轮之前就有）。
- 编排成员经 `runMember` 拿到的输入不拷贝（ParallelAgent 分支同时原地改同一条输入 `-race` 报竞争，第五轮之前就有）。
- 第四轮记的几条（`isFrameworkTool` 按名字、中间件 next 之后挂起时普通工具重放、自写 toolsFn 拿不到工具名分域、
  拿父 session 裸调与 BasicAgent 图身份相同、旁路信号生命周期）仍在。「在另一个任务的执行范围里开独立任务 / 续跑
  别的任务」由第 1 条的拒绝 + `DetachTask` 解决。

## 第六轮：再审之后的修正（2026-09-30）

第五轮提交后又派了五个单视角审计（任务身份 / 重做代际 / 输入拷贝与摘要 / AgentAsTool 与工具侧 / 文档声明），
四十来条发现逐条用它们的探针在本分支复现后修；42 个变异逐条确认承重（脚本在会话草稿里、未入仓）。

**1. 交出去的一段执行是一个相位**。第五轮的嵌套调用执行器把被调方的执行交出去时只封了节点私有态
（`WithNodeStateSealed`）、另摘掉「在 tool_call 里」的章（`LeaveToolCall`）。被调方在自己图外（agent 级钩子里）
跑的那一段因此是第三种位置：ns 是被调方的，节点、tool_call 维、提问与嵌套调用计数却是调用方的。实测三个后果：
- 被调方在图外问一次就占掉调用方一个提问序号；这次调用完成、续跑时被拿回（不再执行）之后，调用方后面那一问换了
  序号、换了 ID，已经答过的问题被再问一遍（a2a 的 human gate 就是这个形状）。
- 调用方节点恰好叫 `tools` 时，被调方图外的提问跟它自己 tools 节点钩子里的提问撞成同一个 ID，整个 Run 终局失败
  （不需要空 tool_call ID）。
- 被调方主图的入口条件边否决时，推进的是封给被调方的那一格——调用方用 `WithRerunnable` 起草的方案原样拿回，续跑
  永不收敛（相对第四轮是**回退**：第四轮当场推进外层整次执行，碰巧对）；节点里直接跑的嵌套图收到的旧批准被兑付给
  新方案（`err=nil`）。

现在 `compose_v2.HandOver(ctx, scope)` 统一交接（嵌套调用、编排成员、内部决策 LLM、转交候选四处）：私有态一格、
**自己的提问与嵌套调用计数**、**相位名**（`handover\x00<scope>`，进提问身份）、不算在 tool_call 里；还记下交接前
调用方那一格与交接 ns——落在交接 ns 上的那张图是被调方的主图，它的输入是调用方产的，入口否决推进调用方那一格；主图
里的提问也不带调用方的外层代际（调用的身份已经决定它是不是同一次调用——带上的话调用方因别的分域被否决而重做时，
这次没变、正挂着的调用里答过的问题换 ID 重问，实测）。被调方钩子里另跑的图不在交接 ns 上，输入是钩子自己产的，
推进的仍是被调方那一格。BasicAgent 的工具钩子每次调用走 `EnterPhase`（同上但不记产出方）：两个 tool_call 的
`OnToolStart` 带同一个 call key 按订单号调同一个 agent 不再撞（曾终局失败）。采纳钩子返回的 ctx 用
`WithPhaseFrom` 把节点体那一套（私有态、相位名、两个计数器、在不在 tool_call 里）整体铺回（2026-10-09 起由 `AdoptHookCtx` 取回全部执行状态）。`LeaveToolCall` 删掉，
`WithNodeStateSealed` 只剩「按某个维度再分账」一个用途（工具体按工具名）。

**2. 提问身份加两维**。`InterruptSite` 加 `InToolCall`（`InToolCall(ctx)`）与 `Phase`（`SubCallScope(ctx)`），取默认值时
不进哈希。第五轮的文档把「tool_call ID 为空」写成「不在任何 tool_call 里」——有的 provider 不回 ID，那时钩子里
的提问与 tool_call #0 里的提问撞成同一个 ID，用户答了前一问、后一问被判「答案已兑付」、整个 Run 终局失败（这个
碰撞第五轮之前就有）。

**3. 否决推进的几个漏口**。
- 节点级钩子相位的访问器是直接填 `scope` 的字面量，分域链为空：钩子里嵌套图的入口否决推进了钩子这一格，钩子自己
  看不见——按 `Rerunnable` 记的草稿续跑永不收敛；草稿不记时嵌套图里的提问不换 ID、对 v1 的批准兑付给 v3。改走
  `scopedOf`，并加 AST 哨兵：`nodeStateAccessor` 字面量不许直接填 `scope` / `chain` / `handover`。
- 否决标记只能沿错误链往上走：业务用 `%v` 包一层、扇出的节点只把一个错往上报（`PickFanoutError`），标记就丢了。
  否决当场另在宿主的 cm 上记一笔；成员以**非控制流**的错失败时，记过却没在错误里找到标记的否决照样推进（中断 /
  被停下时仍只认标记——否决被吞掉、转而问人的执行要原样重放到那一问）。代价是「否决被吞掉之后又因别的业务错失败」
  时多重做一次产出，那个分域里的提问随之换新、不会兑付旧答案。
- 存档投影只丢「比执行级还旧」的可重做记录，分域级否决推进的代际不看：每续跑一次存档里多一份整条草稿（8 次后
  9 条 / 17KB）。现在同一个分域里同一步只导出代际最大的那条——有效代际只增不减，更旧的一定过期。
- 如实写进 godoc 的限制：`RunWithRetry` 包住嵌套运行时否决不在重试之间推进（判官按 `Rerunnable` 记下的判断依据
  原样拿回）；判官**每次**都在调用方 ctx 到点之后才给出否决时一次都记不上；`AddSubgraphNode` 的子图、编排成员的
  主图推不动真正的上游（第六轮之前就有）。

**4. 任务身份的几个缺口**。
- 钩子改写 ctx 上的 sid：`OnAgentStart` 推了会话 ID 当 sid 时，没配 checkpointer 的编排拿它当图的 sid，自己的成员
  被判成别的任务的而拒掉；`OnGraphStart` 推的 sid 进了节点执行范围，被调方的存档落到别的 sid 下、另一个任务成功
  收尾时把它清掉（均实测）。编排入口在钩子之后、图在 `OnGraphStart` 之后、节点在 `OnNodeStart` 之后都把自己的 sid
  重铺回 ctx。
- `DetachTask` 之后传当前任务的 session、或续跑当前任务：不是独立任务，是在当前任务自己的执行里再开一遍它——清掉
  它的审批规则、成功收尾时删掉兄弟分支挂着的存档、正在跑的节点被再跑一遍（实测）。`DetachTask` 记下从哪个任务
  脱离，Run / Resume 的 RunID 等于它就拒。
- 独立任务挂起、节点把它的错原样返回：当前任务把它当自己的挂起落档，续跑时答案进的是节点的 ctx、被脱离剥掉，
  独立任务按新任务从头再问、换一个 ID，永不收敛（实测）。`DetachExecution` 盖一枚记号，脱离后的根图（不在节点执行
  范围里）中断时给错误包一层（原中断照样取得到），外层图 `validateInterrupt` 见到就终局拒收。
- `DetachTask` 之后的拒绝被当成顶层、返可重试的普通错误：外层编排的重试把扣款放大到 4 次。入口拒绝恒不可重试
  （`NonRetryable`），在任务里（含脱离出来的）再 `%w ErrRunAborted`。拒绝正文不再推荐 `GetSession(ctx)`（节点里裸调
  拿父 session 没有自己的 ns，照做会撞 `ErrGraphIDConflict`）。
- `DetachExecution` 把会话 ID 与软取消信号改成执行状态：留着会话 ID 时独立任务写进当前会话的历史、排在用户这一轮
  的提问之前；留着软取消时从被停下的 Run 里带出来的 ctx 开的独立任务一个节点都不跑就停下（均实测）。
- `a2a.RemoteAgent`（第十个 Agent 实现）不走会话校验：节点里给新 session 时用户的回答被当成新远端 task 的第一条
  消息发出去。导出 `ResolveRunSession` 给自研实现复用（2026-10-12 并进 `agent_v2.EnterRun`：判 session 之外还盖任务章、遮节点授予），RemoteAgent 接上；`Agent` 接口 godoc 写明契约。

**5. 历史补记**。
- AgentAsTool 按「在不在 tool_call 里」判补不补：自写 toolsFn 不调 `WithCurrentToolCallContext` 时产出记两份，
  GraphAgent 节点体自盖 tool_call 章做扇出时一条不剩。改由 BasicAgent 的 tools 节点在调工具前盖章（记下自己这次
  执行的位置，AgentAsTool 比位置）。
- 调用方每次重跑都现拼输入（AgentAsTool 从参数拼的也是）、被调方中断过时，历史层按消息 ID 认不出上一次落过的
  那条：每多一次中断多一份输入（第五轮在补记位置上新引入的形态，`RunSubAgent` 那一半第五轮之前就有）。嵌套调用
  暂停时结算过的输入按调用身份记进父节点的步骤记录，续跑完成、再次暂停都不再记；GraphAgent 作为子时不再自己在
  暂停出口结算（先于边界那套落地的冗余路径，按消息 ID 认同一条，正是重复的来源之一）。

**6. 输入拷贝与摘要**。
- 摘要的逐叶子路径（第五轮新增）有复杂度爆炸：每个键都全表扫一遍坏叶子清单（二次方，4000 行 NaN 表 2 秒）、共享的
  子容器每条路径都重走（深 14 的共享结构 20 秒没算完）、路径串逐层拼接（十万层 10GB），而这个计算每次 `RunSubAgent`
  都同步做、不看 ctx。现在只在真撞键时才清坏叶子、每个容器只走一遍（第二次遇到记一条「引用 #序号」，走的顺序
  确定、序号也确定）、路径按段压栈只在记坏叶子时拼串——线性。
- 逐叶子只在整体往返不回来时才走，那样的输入永远不会从存档里解回来跟首跑比对，所以只要确定、分得开：结构体按
  字段展开（含嵌入的未导出结构体提升上来的字段）、键不是字符串的 map 把键拼成带种类前缀的串再展开、复数记值、
  指针接收者的编码方法在值可寻址时照样调（跟 encoding/json 一致）。曾经结构体整体当一个叶子：一个字段是 NaN
  时业务 ID 的区别整体丢掉，工单 T-1 的批准兑付给 T-2（`err=nil`，第六轮之前就有）。只剩自定义编码出错的值整体算
  一个叶子。
- 拷贝：嵌入的未导出结构体提升上来的导出字段（encoding/json 照样编码、摘要也计入）跟着深拷——曾经共享，被调方
  钩子一改就写回调用方、续跑换身份重问，扇出时跟兄弟调用的摘要读撞成数据竞争。

**仍在的问题（记在 [todo.md](../../code/todo.md)）**：第五轮记下的几条（节点里直接跑的嵌套图跑完之后节点又失败
时旧答案被兑付、approve_always 不分 agent 与不回收、`runMember` 不拷贝输入、a2a 的 typed nil、RemoteAgent 句柄不分
嵌套调用）仍在；另加：`RemoteAgent` 拿到 nil session 时远端句柄存在一次性对象里、续跑时丢；agent 级钩子
（`OnAgentStart` / `OnAgentEnd`）同在一个相位，两处用同一个 call key 会撞；顶层图的图级钩子相位不算在任务里
（第六轮之前就有，实害有限）。

## 第六轮补审（2026-10-15）

第六轮（`d13b0509`）提交后没有做单视角审计，这次补上。审的是这些机制在当前 dev 上的形态。先在当前代码上做了 35 条变异，
30 条让哨兵变红，说明仍承重（其中转交候选去掉 `HandOver` 那条，只是被另一条测试顺带抓到）。5 条不红：编排成员
（`runMember`）和内部决策 LLM（`runDecisionAgent`）去掉 `HandOver`，这两处的交接相位没有哨兵；相位自己的嵌套调用计数器
（身份串里已有相位名，它是第二道）；两处显式 sid 重铺（见文末）。再按两条不变量各写探针，下面两个问题都在当前代码上复现了：

- **不变量 1：同一次调用里的提问不随调用方那一层的重做换 ID**。调用方那一层指调用方节点的这次执行，连同交接之前
  所在的分域。被调方问的问题，不管在它自己的图里还是图外，都受这一条约束；按 `Rerunnable` 记在交接那一格的东西
  也不跟着作废。
- **不变量 2：调用方所在图之外的执行被否决重做时，被调方的提问必须换新**。那时调用方所在的图会从头跑，这次调用的
  完成记录跟着丢了，被调方会重新产出内容。交接那一格自己被推进时同理，被调方的主图也会重跑。

**1. 交接主图把整条外层代际链清空了**。第六轮说「交接过来的主图不带调用方的外层代际」，代码却是 `withOuterRerunGens(ctx, nil)`，
清掉的是**整条**链。复现场景：外层节点在节点体里直接跑嵌套图（`AddSubgraphNode` 也一样），嵌套图的节点经 `RunSubAgent`
调一个「起草 + 问批准」的被调方；外层节点的出边按 `CondFailRerunFrom` 否决之后，嵌套图从头跑，被调方重新起草出 v2，
提问算出的 ID 却跟 v1 那一问相同。实测：只问过 `[批准 plan-v1 吗]`，产出 `plan-v2 已批准(批准#1)`，`err=nil`。
外层代际链本来就是为这类问题加的，第六轮的清空把它在深一层的位置又放开了。

**2. 被调方在图外的提问带着调用方那一层**。交接那一格的有效代际是「执行级 + 整条分域链」，调用方节点的执行级也算在
里面。所以调用方因为兄弟分域被否决而重做时，正挂着的同一次调用里在图外问过、已经答了的问题（a2a `RemoteAgent` 的
human gate 就是这个形状）会换 ID 再问一遍。实测问了两遍、gate 执行了 3 次。被调方主图里的提问第六轮已经修过
（`TestRunSubAgent_PendingPlainCall_NotReaskedWhenSiblingRejected`），图外的没有跟上。

**修法**：
- 交接那一格，以及在它下面叠的分域，有效代际只算交接那一层往下的。实现上 `nodeStateAccessor` 加了 `handedOver` / `genFrom`，
  由 `HandOver` 设置、`scopedOf` 往下带，`effectiveRerunGen` 据此只加 `chain[genFrom:]`。
- 外层代际链不再为交接主图特判，统一用 `outerRerunGensFor`：外层链加上承载这张图的那一格的有效代际。承载交接主图的
  正是交接那一格，于是调用方那一层自然被排除，调用方所在图之外的各层照带。我第一版写的是「交接主图只带调用方所在图的
  链」，变异验证显示通用规则同样满足不变量 1，还能多覆盖交接那一格被推进的情形，于是改用通用规则、删掉特判。
- 交接那一格里的可重做记录，键改用 `:hredo` 标记（`handedRedoKeySep`）。存档投影对这种记录只按「同一格同一步留最新一代」
  丢，普通记录（`:redo`）照旧还要比执行级代际。原因是交接那一格读得到的记录，代际可以比执行级小；如果投影拿执行级来丢，
  被调方在图外记下的方案会掉出存档，续跑时重新产出一份，而提问的 ID 没变，对旧方案的批准就兑付给了新方案。这一点由
  变异实测确认：只改代际、不改投影时，`TestSameCall_HandedOverRerunnableRecord_KeptAcrossCallerRedo` 报 `plan-v2 已批准(批准#1)`。

**新哨兵**（`agent_v2/handover_rerun_gens_test.go`）：`TestOuterRedo_NestedGraphCallee_*`、`TestOuterRedo_SubgraphNodeCallee_*`、
`TestCalleeCellRedo_*`（不变量 2）；`TestSameCall_OutOfGraphQuestion_*`（执行级与 tool_call 分域两种推进）、
`TestSameCall_HandedOverRerunnableRecord_*`、`TestSameCall_CalleeToolCallQuestion_*`、`TestSameCall_MemberOutOfGraphQuestion_*`
（不变量 1，最后一条同时是 `runMember` 交接相位的哨兵）；`TestDecisionAgent_OutOfGraphNodeState_*`（`runDecisionAgent`
交接相位的哨兵：自研分类器在图外 `SaveNodeState`，不能顶掉 classify 节点记的升级链进度）。修复的 11 条变异逐条变红。

**修复后的审计（审计 B）又找到两处，都是上面的修法带出来的**：
- 外层代际链按 ctx 取时，从嵌套图的 `OnGraphStart` 里发起的交接多算了一层。图级钩子记在外层节点执行名下，调用方
  那一层就是外层节点；但钩子拿到的 ctx 上已经是嵌套图自己的链，里面含外层节点的代际。外层节点因兄弟分域被否决
  重做时，挂着的调用里已答的问题被再问一遍（方向安全，但违反不变量 1）。现在 Run 入口把这张图的链记在
  `ChannelManager.outerGens`，`outerRerunGensFor` 按承载新图的那一格所在的 cm 取上一层的链，不按 ctx 取；
  哨兵 `TestSameCall_GraphHookHandover_QuestionKeptAcrossHostRedo`。
- 交接那一格的 `:hredo` 记录不再拿执行级代际比。调用方重做后交来的输入变了、调用换了身份，旧身份那一格最后那条
  读不到，却照样导出：3 次否决多 3 条（修法之前是 0 条——当时连读得到的也一起丢）。只占空间，节点执行成功之后整串
  不可达。精确回收要知道「这一格的调用已完成」、需要新增导出 API，先记进 todo、不做。

**3. 脱离的两处只堵了一条路**（审计 A 实测，第六轮之前就有这两条入口）。
- 独立运行挂起时，第六轮只在外面包一层 `detachedInterruptError`，`validateInterrupt` 在节点返回的错误链上找它。业务常写
  `if ie, ok := compose_v2.IsInterrupt(err); ok { return nil, ie }`，或者 `fmt.Errorf("…: %w", ie)`：这两种写法下包装丢了，
  外层把独立任务的提问当成自己的挂起收下。带累积答案续跑 5 轮问了 5 次、每次换一个 ID，留下 6 份孤儿挂起档——正是
  第六轮说已经修掉的那个活锁。现在记号也打在中断对象本身上（不导出字段，中断对象不进存档），`MergeInterrupts` 合并时
  往下传。
- 「从哪个任务脱离」第六轮只由 `agent_v2.DetachTask` 记、只记一层。直接用 `compose_v2.DetachExecution` 脱离时什么都不记：
  拿当前任务的 session 跑 agent 不被拒，那个 agent 当新任务跑完收尾，把当前任务名下兄弟分支挂着的存档删了（1 → 0）。
  两层脱离之后拿外层任务的 session 同样不被拒。现在来源由引擎记成一条链（`compose_v2.DetachedFrom`）：在节点执行范围里
  脱离时记下那次 Run 的 sid，接在之前几次脱离记下的后面；编排的 agent 级钩子跑在图外、引擎这层看不到所在任务，
  `DetachTask` 经 `compose_v2.DetachExecutionFrom` 把本包的任务章交过去。agent_v2 那份单层记录删掉。
- 哨兵 `agent_v2/detach_origin_test.go`（四条，含四种返回中断的写法），8 条变异逐条变红。
- 修复后的复审（审计 A）又找到两处：记号直接写在业务的 `*InterruptError` 上，业务把中断对象当包级变量复用时，它经
  一次脱离运行就永远带着记号，之后普通任务返回它被终局拒收（并发用时还是写竞争）；来源链只看 ctx 上有没有、不看
  那次执行还在不在跑，任务挂起之后用当时存下的 ctx 脱离再续跑它被拒，还报 `ErrRunAborted`（DetachTask 这条第六轮
  就如此，DetachExecution 这条是上面的修法带出来的）。现在记号打在交出去的拷贝上（`IsInterrupt` / `errors.As` 走到
  包装时拿到拷贝），业务的原对象不动；每次不在节点执行范围里的 Run 铸一个存活标记、出口置否，来源链每一项带着它，
  `DetachedFrom` 只返回仍在执行的任务。哨兵 `TestDetach_ReusedInterruptObject_NotStampedForLegitTask`、
  `TestDetach_StoredCtxAfterTaskPaused_ResumeAllowed` 与四种返回写法里补的 `errors.As` 一种；6 条变异逐条变红。
- 再次复审：存活标记跟的是执行实例不是任务——同一个任务续跑期间拿上一次执行存下的 ctx 脱离，会被放过（实测清掉了
  这个任务挂着的存档）。要求跨执行留着 ctx，现实中少见；按任务判要在进程里登记正在执行的 sid，记进 todo、这次不做
  （同一条规则已改到第三版，按停止条件先停下来）。

**顺带核实、没改的**：
- 外层节点因普通业务错（不是否决）失败时，嵌在直跑嵌套图里的 `RunSubAgent` 同样会把旧批准兑付给新内容（实测）。这是
  [todo.md](../../code/todo.md) 里那条「节点里直接跑的嵌套图跑完之后节点又失败」的同一个形状，只是深了一层，仍然 OPEN。
- OnGraphStart / OnNodeStart 之后显式重铺 sid 的那两段，删掉之后哨兵也不红。真正承重的是 `CallHook` 的规则：会话 ID 是
  执行状态，采纳时取框架自己的值。

## 删除范围：只删自己这次执行（2026-09-23）

**问题**：`DeleteSubtree(sid, P)` 原先删 P 与所有「以 P 为祖先」的 ns，而祖先判定把 `#` / `@`
段也算作下级——`w#1418765591` 算 `w` 的后代。可它们是**同一个节点的其他执行**：

- 同一节点里对同一个名字调两次，第 0 次落 `w`、第 1 次落 `w#<hash>`（09-23 的布局；09-28 起每次调用
  都落 `#身份哈希`，下面的决策仍成立——SendFrom 实例那一条与它无关）。续跑时第 0 次重跑成功，
  成功清理 `DeleteSubtree(sid, "w")` 把第 1 次还挂着的存档一起删了，第 1 次只能从头跑：两节点
  子 agent 的副作用被重放；子 agent 问两问时依次被问 `[Q1 Q2 Q1 Q2 …]`；模型每次换 tool_call
  ID 时永不收敛（实测）。
- 子图节点同一超步既有静态成员又有 SendFrom 实例时，静态成员成功把实例 `sub#4` 的挂起档删了
  （改动前就有）。

**选择**：范围只沿 `/` 往下——P 自身与 `P/…`。`#`/`@` 兄弟执行各自在成功或终局失败时清自己
的那棵；清整个任务走 `P = ""`。判据 `compose_v2.IsNSInSubtree` 是唯一真相源：内存实现直接调，
sql 后端枚举后调，redis 的 Lua 从 `NSSubtreeSeparator()` 注入分隔符；conformance 的
`DeleteSubtree_Sibling_Executions_Survive` 钉住 `sub@1`、`sub#0`、`sub@11` 等必须存活。
`ParentNS` 仍把 `a#0` 归到 `a` 下——那是观测层按节点归组找父 span 用的，不表示所有权。

**代价**：兄弟执行留下的档不再被某一次执行的清理顺带带走，只在它自己成功 / 终局失败时，或
外层那次执行清自己的子树时（它们都在外层的 `/` 之下）被清掉。

## 开放问题

1. **嵌套子图深度限制**
   - 当前无限制——3 层嵌套可能 ns 字符串过长（"a/b/c/d/..."）
   - LangGraph 也无显式限制，靠运行时 frame 深度兜底
   - 暂不加，需求出现再说
2. **GraphAgent 业务自定义 NodeFunc 嵌套 sub.Run 漏 push 父位置 NS** (**已闭合** — #7 F4 commit `f6ede83` 加 RunSubAgent helper)
   - **7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) 已内置 push 协议
   - GraphAgent 用户自定义 NodeFunc 内 sub.Run 推荐走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input, opts...)` helper — 一站式 push 3 件 ctx (graphNS + NewSharedChildSession + storeScope), 避免漏 push silent bug
   - ~~业务想手动控制时仍可走 `WithGraphNS(ctx, JoinNS(GetGraphNS(ctx), "<nodeID>"))`~~ **别手拼**（2026-09-18 改正，CLAUDE.md 早已撤回这条教法）：`RunSubAgent` 的子 ns 恒为 `#身份哈希`，身份 = 位置（调用方所在的节点与相位 + 实例维三选一：图级 fanout 实例 / 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态重访 visit）+ 输入内容（同一份输入再分第几次）或 key，那段判定只在 `RunSubAgent` 里——手拼漏实例维 ⟹ 子 cp 互覆，漏 visit 维 ⟹ 父循环第 2 轮的提问与第 1 轮撞同一个 InterruptID，漏输入内容 ⟹ 带着新输入的第二次调用续上第一次的存档
   - GraphAgent godoc + CLAUDE.md "实施前 checklist 第 1 条" 已警示
