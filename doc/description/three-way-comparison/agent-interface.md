# 主题：Agent / 子图嵌入接口（Agent Interface for Embedding）

> **v2 baseline** (R30 audit follow-up 全文重写):
> - 单 `agent_v2.Agent` 接口 (`Run + Resume + Name + Description`) — ISSUE-71 决策落地, v1 NestableAgent / RunNested 双接口已删
> - 父子位置 ctx 协议: `compose_v2.WithGraphNS(ctx, JoinNS(parentNS, "<childName>"))` 单维度 push (v1 `EnclosingParent{ParentState, ParentNodeID}` 复合结构 ctx-value 已在 v1→v2 channel-based 重构期间删 — commit `45f5e04` "NestableAgent 接口合并 Commit 4/6 - 删 NestableAgent + RunNested"; v2 内 audit follow-up #6 commit `08bd6a7` 又删了短期引入的 `currentNS / EnterChildNS` 第二维度, 现 v2 仅余单维 `graphNS`)
> - 自动 push 范围: **7 编排 wrapper** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan) 在调 sub-agent.Run 前 push graphNS; **2 tool fork** (AgentAsTool / SkillTool fork mode) 自动 push 3 件 ctx
> - 业务自定义 NodeFunc: GraphAgent 业务自定义 NodeFunc 内嵌套 sub-agent.Run 走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input, opts...)` helper (#7 F4 commit `f6ede83`) — 一站式 push 3 件 ctx (graphNS + NewSharedChildSession + storeScope)


## 触发场景

需要查阅本文档的设计决策：
- 设计 / 改动 Agent 接口（增删方法）
- 决策"嵌入运行 vs 独立运行"如何区分（v2 单接口 + ctx push）
- 评估新增编排是否要"专门为嵌入加方法"（不需要 — 实现 Agent 接口即可）
- 评估父调子 agent 的注入协议（v2 通过 ctx push graphNS）
- 决策 sub-agent 调用点（7 编排 wrapper 内置 push / 2 tool fork / GraphAgent 业务 NodeFunc）的责任划分

## 三方对照速查表（v2 baseline）

| 维度 | crew-go v2 | LangGraph | eino |
|---|---|---|---|
| **核心接口** | 单 `agent_v2.Agent` interface (`Run(ctx, sess, input, opts...) ([]*AgentEvent, error) + Resume(ctx, sid, opts...) + Name() + Description()`) | 单 `Pregel` 类 (`invoke / stream / ainvoke`) | 单 `adk.Agent` interface (`Generate(ctx, input, opts...)`) |
| **嵌入 vs 独立区分** | ctx push `graphNS`, 子 Run 入口取 `GetGraphNS` 派生子 ns + cp 物理分离；「是不是新任务」读框架盖的章（Run 入口在 OnAgentStart 之前判：上面有没有一个正在执行的任务——节点执行范围记下的图 sid 或任务章；有的话 session 的 RunID 必须属于它、对不上直接拒，开独立任务要显式 `DetachTask`），不看 ns 空不空 | `add_node(name, pregel)` 把子 Pregel 当节点, runtime 自动 dispatch | `AddGraphNode(key, AnyGraph)` 头等节点 + callback ctx 透传 |
| **父位置传递** | `compose_v2.WithGraphNS(ctx, JoinNS(parentNS, "<childName>"))` 单维度 push (ctx value); 配 `InterruptSite.InterruptID()` 稳定 ID（graphNS 编入 hash）| 父节点上下文隐式在 Pregel runtime channel | callback ctx 透传 + `RunInfo` |
| **嵌入点的 cp 行为** | 子 cp 物理写到 `(parentSID, JoinNS(parentNS, childName))` ns 路径 (二级 key); `Checkpointer.SaveNS / LoadNS / ExistsNS` 8 method 接口 | 子 cp 写到父 thread_id 的 ns (`("subgraph_id",)` channel namespace) | 共享 cp store 全树嵌套; `forwardCheckPoint` 一次性 delete 消费 |
| **fan-out 派生型编排嵌入** | ✅ ParallelAgent fanout 多 branch 各自 push `JoinNS(parentNS, branch.Name)` (按 br.Name 唯一保 cp 物理分离, AddBranch panic 防重名); BasicAgent toolsFn fanout N tc 本身仅 push `WithCurrentToolCallContext(tcID, branchIdx)` (ISSUE-81 节点内 goroutine + WaitGroup), 嵌套 sub-agent 时由 `RunSubAgent` helper (含 `AgentAsTool.Call` delegate) 或 `SkillTool fork` push `JoinFanoutNS(parentNS, nodeID, hash(身份))` 防 tc.ID 撞 NS (ISSUE-78；位置的 tool_call 维 = tcID + 批内下标 + tools 节点访问序号，模型给的 ID 跨轮、批内都可能重复；全历史输入的 AgentAsTool 与 skill fork 只看位置，其余再叠输入内容或 key) | ✅ Pregel 一视同仁 | ✅ AnyGraph 一视同仁 |
| **HITL Resume 嵌入** | ✅ 9 编排各有 Resume 方法 + commonResumePrep helper (commit `09f21a6`) 统一 boilerplate | ✅ `interrupt() + Command(resume=)` | ✅ `addSubGraphInterruptInfo` |

## crew-go v2 设计

### Agent 接口 (`agent_v2/agent.go`)

```go
type Agent interface {
    Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) ([]*AgentEvent, error)
    Resume(ctx context.Context, sessionID string, opts ...ResumeOption) ([]*AgentEvent, error)
    Name() string
    Description() string
}
```

**没有"嵌入专用"方法** — 任何 Agent 实现都能被嵌入到任何 graph. 父子关系通过 ctx 隐式传递. 9 编排 (BasicAgent + 8 高阶) 都满足此接口.

### graphNS ctx 协议 (`compose_v2/ns_ctx.go`)

```go
func WithGraphNS(ctx context.Context, ns string) context.Context  // push 当前 graph 整体 ns
func GetGraphNS(ctx context.Context) string                       // 取当前 graph ns
```

配套 ns 派生 / 解析 helpers (`compose_v2/ns.go`):
- `JoinNS(parent, nodeID)` 普通拼接 — 嵌套子图常用
- `JoinFanoutNS(parent, branchNodeID, idx)` 含 index 防 fanout 多实例 cp 互覆 (ISSUE-78)
- `ValidateNodeIDForNS(nodeID)` 编译期拦 `/` `#` `@` 字符冲突
- `ParentNS`（观测层按节点找父 span、`Checkpoint.ParentNS` 自动填）/ `SplitNS`（skill fork 递归检测按段比较）
- `IsNSInSubtree` — `DeleteSubtree` 的删除范围：自身 + 经 `/` 往下的后代，`#`/`@` 兄弟执行不删

v1 时代 `EnclosingParent{ParentState, ParentNodeID}` + `WithEnclosingParent / GetEnclosingParent / ClearEnclosingParent` 复合结构 ctx-value 协议在 v1→v2 channel-based 重构期间删 (commit `45f5e04` "NestableAgent 接口合并 Commit 4/6"); v2 内 audit follow-up #6 (commit `08bd6a7`) 又删了短期引入的 `currentNS / EnterChildNS` 双维度协议 — channel-based 模型下"父子关系"由 graphNS 字符串前缀自然表达, 不需要额外 ctx-value 或第二维度.

### 父调子 agent 的注入点 (3 类自动 + 1 类业务责任)

| 注入点 | 位置 | 责任 |
|---|---|---|
| 7 编排 wrapper 嵌套 sub-agent | `agent_v2/{sequential,parallel,loop,routing,reflect,handoff,plan}_agent.go` 内 `sub.Run` 调用前 push (共 13 push 点 — wrapper inline; 加 2 tool fork (skill_tool inline + RunSubAgent helper, 后者含 AgentAsTool.Call delegate) 后全口径 15 push, grep `WithGraphNS(`) | 框架自动 |
| `AgentAsTool.Call` 调嵌套 sub-agent | `agent_v2/agent_tool.go` 内走 `RunSubAgent` helper (一站式 push 3 件 ctx) | 框架自动 |
| SkillTool fork mode 调 sub-agent | `agent_v2/skill_tool.go` (P2.C+B fork mode) | 框架自动 |
| GraphAgent 业务自定义 NodeFunc 调 sub-agent | 业务代码 → 推荐走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input, opts...)` helper | **业务责任** (helper 已封装) |

**关键**: BasicAgent ReAct 内部 **preamble + model + tools + postamble 4 节点循环** (ISSUE-74/75 后 agent_v2 wrapper 生产路径强制 4 节点; preamble 跑 StoreMemory/HistoryCompactor/Skill 预激活, postamble 跑 store.Put + commitFinal), 无 sub-agent 嵌套 (tools 节点内 N tc fanout 是 BasicAgent 内部并发, 不属于 sub-agent 调用); GraphAgent 业务自定义 NodeFunc 拓扑由业务自己决定何处嵌套 sub-agent — 但有 `RunSubAgent` helper 一站式 push 避免漏 push silent bug.

### 子 agent.Run 入口 — 嵌入与独立同一路径

v2 取消"嵌入路径分叉"——任何 Agent.Run 入口都一样:

```go
func (a *SequentialAgent) Run(ctx, sess, input, opts...) ([]*AgentEvent, error) {
    // ... 编排自身 cfg 解析

    // 子 step 嵌套 push graphNS (SequentialStep.Name / Agent 是 exported 字段)
    for _, step := range a.steps {
        parentNS := compose_v2.GetGraphNS(ctx)
        childNS := compose_v2.JoinNS(parentNS, step.Name)
        subCtx := compose_v2.WithGraphNS(ctx, childNS)
        subCtx = WithSubAgentStoreScope(subCtx, true)  // store.Put 防双重持久化 (ISSUE-84)
        events, err := step.Agent.Run(subCtx, sess, input, opts...)
        // ...
    }
}
```

子 Agent.Run 入口读 `GetGraphNS(ctx)` 定 cp 寻址位置，**不需要显式 ClearGraphNS**。「这次是不是新任务」另由框架的章判定（`agent_v2` 的 `enterRunTask`：上面有没有一个正在执行的任务——节点执行范围记下的图 sid、编排入口与嵌套调用执行器盖的任务章；有的话这次 Run 是它的一部分，session 的 RunID 必须属于它，入口核对、对不上直接拒），**不从 ns 空不空推**——业务顶层自己 push 了 ns（租户前缀）或节点里裸调 `sub.Run` 不 push ns 时，按 ns 推都会判错。

### lifecycle 防护差异 vs v1

| 维度 | v1 EnclosingParent (复合 ctx-value) | v2 graphNS (string ctx-value) |
|---|---|---|
| ctx value 类型 | `*EnclosingParent` 含 ParentState + ParentNodeID | `string` ns 路径 |
| 一次性数据 | 是 (用完即删 ClearEnclosingParent) | 否 (push 覆盖即可, 子 push 子 ns 不破坏父 ns 在父 ctx scope) |
| 顶层防护 | `RunGraph` 入口 ClearEnclosingParent 防误识别为嵌入 | 不靠 ns：Run 入口按框架盖的任务章 / 节点执行范围章判「是不是新任务」（见上） |
| 子入口防护 | 子 Run 读完 EP 后立即 Clear 防子的子继续透传 | 不需要 — 子 push 覆盖父 ns 即可 |

v2 简化的根因: graphNS 是**字符串叠加**而非 marker, 不需要"用完即删". 父 push "a", 子 push "a/b", 在子 ctx scope 内永远是 "a/b"; 业务不会"看到 a/b 误以为是 a".

## 我们的优势

1. **统一接口避免双轨制** (ISSUE-71 后): 任何 Agent 嵌入任意 graph, HITL 全闭环. v1 时代 5 编排实现 NestableAgent + 3 fan-out 派生型 (ParallelAgent / RoutingAgent / PlanAgent) 不实现导致 v1 坑 (PlanAgent 不能嵌入 Sequential / ParallelAgent + RoutingAgent 不能嵌入任何编排) 已通过 ISSUE-71 接口合并消除. **GraphAgent 业务自定义 NodeFunc 内调 sub-agent 协议** 是 v2 时代独立 gap, 由 #7 F4 commit `f6ede83` 加 `RunSubAgent` helper 单独闭合 (跟 ISSUE-71 不同时段).
2. **接口稳定性**: 未来加新编排只要实现 `Agent`, 自动获得嵌入 + HITL Resume 能力, 不需要再"为嵌入加专门方法".
3. **ctx 透传比双方法签名更灵活**:
   - LangGraph 把父位置藏在 Pregel runtime (不可见); crew-go 通过 `GetGraphNS(ctx)` 显式可读
   - GraphAgent 业务自定义 NodeFunc 内调 sub-agent 可手动控制 — 改 nodeID 名 (影响 cp ns) / 加 fanout idx (并发 sub-agent) / 业务 namespace 隔离
4. **`RunSubAgent` helper 让业务自定义 NodeFunc 嵌套 0 心智负担** (#7 F4 commit `f6ede83`): 一站式 push 3 件 ctx (graphNS + NewSharedChildSession + storeScope), GraphAgent 业务自定义 NodeFunc 调 sub-agent.Run 用此 helper 避免漏 push silent bug
5. **反模式警示** (godoc 显式): 业务**不要**自己 `for i := 0; i < N; i++ { go RunSubAgent(ctx, "worker", ...) }` 跑并发 — 这是绕开 BSP 框架的反模式 (LangGraph / crew-go 都不支持). 业务想 fanout 走 `ParallelAgent` (内置 branchIdx) 或 framework `compose_v2.SendFrom` (computeNextTasks 自动算 branchIdx)
6. **InterruptID 按执行位置稳定**: `InterruptSite.InterruptID()` fnv64a 16 hex — 父子嵌套自然不撞 (graphNS 编入 hash), fanout 多实例不撞 (InstanceID), LoopAgent 多轮不撞 (visit), 同节点多 tc 不撞 (ToolCallIdx), 跨任务不撞 (TaskID); 业务提问走 `Ask` 全自动

## 我们的不足

1. **GraphAgent 业务自定义 NodeFunc 仍是业务责任** (已闭合 #7 F4): 业务可走 `RunSubAgent` helper 避免漏 push silent bug, 但仍需业务记得调用此 helper (而非直接 `sub.Run(ctx, ...)`). LangGraph `add_node(name, pregel)` 注册时自动派生 ns, 业务零负担.
   - **缓解**: GraphAgent godoc + CLAUDE.md "实施前 checklist 第 1 条" + `example/orchestration/nested_smoke` 12 场景示范
2. **ctx push 跨 goroutine 必须 caller 显式透传**: 框架内部 ParallelAgent fanout 走 inline closure (`branchFn`) + `bld.AddBranch(br.Name, branchFn)` (`agent_v2/parallel_agent.go::Run` 内构造, 复用父 ctx + push 子 NS), 但业务自起 goroutine 调 sub-agent 时需注意 ctx 透传 — Go 通用问题不止 agent_v2 域.
3. ~~**没有像 LangGraph 一样的"父子 cp 自动级联清理"**~~ **已补**（2026-09-18 核验）：终局失败（`ErrRunAborted`）出口主动 `DeleteSubtree` 清整棵子树（`compose_v2/exit_checkpoint.go` 的 `purgeExitSlot`），`ResumeActionAbort` 同样清；非终局错误保留 cp 供 Resume 是设计。（原文：父 abort 时子 cp 不主动 delete, 依赖业务 caller 显式调 `cp.DeleteSubtree(sid, ns)`。）

## 决策记录

| 时间 | 决策 | ISSUE | 关键考量 |
|---|---|---|---|
| ISSUE-70 Phase 2 Round 3 | 加 `NestableAgent` 接口（双接口 Run + RunNested）, 4 编排首批实现 (后续 v1 时代扩到 5 编排: BasicAgent / Sequential / Loop / Reflect / Handoff) | ISSUE-70 | 当时认为"嵌入有专门 cp 协议, 独立没有, 分两接口语义清晰" |
| ISSUE-71 Phase 0 调研 | 合并到单 Agent + ctx 标记 | ISSUE-71 | 三方都单接口; 双接口导致 fan-out 派生型无法嵌入; 用户文档要解释复杂矩阵 |
| ISSUE-71 Phase 1 实施 | 删 NestableAgent + RunNested, 统一 ctx 协议 | ISSUE-71 | 完成 9 编排嵌入闭环 |
| v1→v2 channel-based 重构 | 删 v1 `EnclosingParent` 复合结构 ctx-value 协议 + 改 ctx string `graphNS` | commit `45f5e04` "NestableAgent 接口合并 Commit 4/6" | channel-based 模型下 graphNS 字符串叠加足够; "用完即删"防护不需要; 简化心智负担 |
| v2 内 NS 单维度化 | 删短期引入的 `currentNS / EnterChildNS` 第二维度 (生产 0 caller) | audit follow-up #6 commit `08bd6a7` | v2 内一度有 graphNS + currentNS 两个 ctx 维度, 实际生产仅用 graphNS, 删 currentNS 简化协议 |
| #7 F4 commit `f6ede83` | 加 `agent_v2.RunSubAgent` helper | R30 audit | GraphAgent 业务自定义 NodeFunc 嵌套 sub.Run "用户责任" 闭合; 一站式 push 3 件 ctx 业务零负担 |
| #5 commit `09f21a6` | 抽 `commonResumePrep` helper | R30 audit | 9 编排 Resume 入口 boilerplate ~50 行 × 9 → 抽 helper 减重复 + 防"修一个漏一个"协议变更陷阱 |

## 关联文档

- [`subgraph-isolation.md`](./subgraph-isolation.md) — 子图隔离 (cp store / session / OrchHooks)
- [`address-routing.md`](./address-routing.md) — ns 寻址 + 5 维 InterruptID
- [`checkpoint-storage.md`](./checkpoint-storage.md) — cp ns 物理分离协议 (嵌套 cp 写到哪)
- [`interrupt-resume.md`](./interrupt-resume.md) — Resume 路由 (嵌入路径下 Resume 怎么找回内层 cp)
- [`doc/v2/channel-engine.md`](../../v2/channel-engine.md) §8 嵌套 NS — 引擎层完整设计
- [`doc/v2/checkpoint.md`](../../v2/checkpoint.md) — cp 与 Resume 权威篇
- [`doc/v2/usage-guide.md`](../../v2/usage-guide.md) — 业务侧用法 (9 编排 + AgentAsTool + RunSubAgent helper)

## 三方源码定位

| 项目 | 关键文件 | 说明 |
|---|---|---|
| LangGraph | `langgraph/pregel/main.py::Pregel.add_node` | 子 Pregel 当 node 注册 |
| eino | `eino/adk/agent.go::Agent` interface | 单方法 `Generate` |
| eino | `eino/compose/graph.go::AddGraphNode` | AnyGraph 嵌入头等节点 |
| crew-go v2 | `agent_v2/agent.go::Agent` | 单接口 (Run + Resume + Name + Description) |
| crew-go v2 | `compose_v2/ns_ctx.go::WithGraphNS / GetGraphNS` | ctx 单维度协议 (取代 v1 EnclosingParent 双维度) |
| crew-go v2 | `compose_v2/ns.go::JoinNS / JoinFanoutNS / ValidateNodeIDForNS` | ns 派生 / 校验 helpers |
| crew-go v2 | `agent_v2/run_subagent.go::RunSubAgent` | GraphAgent 业务自定义 NodeFunc 嵌套 helper (#7 F4) |
| crew-go v2 | `agent_v2/resume_helpers.go::commonResumePrep` | 9 编排 Resume 入口共享 helper (commit `09f21a6`) |
