# 为什么转 channel-based 重设计

> 决策记录文档 — Stage 1-7 完成后回顾"当初为什么做这件事"。
> 内容浓缩自 [`channel-based-design.md`](./channel-based-design.md) §0 背景 +
> [`poc-report.md`](./poc-report.md) §0 PoC 单一目标 + Stage 1-7 实际经历总结。

## TL;DR

- **直接 trigger**：`plan_chat_web` demo 跑出真 bug "已完成 graph 被 ctx 拦下"
- **深层动机**：v1 frontier-based + GraphState 模型为绕过限制堆了太多 hack（`superstep_snapshot` / NestableAgent 双接口 / `SubGraphs` 递归嵌入 / `ProcessState` 锁包装）
- **PoC 决策点**：Go 泛型能否做出比 eino field_mapping 更干净的 channel API
- **实际收益**：设计统一对齐 LangGraph Pregel + 嵌套 / fan-out / cp 三大头疼问题自然解决 + 12/12 nested_smoke 覆盖度比 v1 11/11 更高

---

## 1. 直接 trigger（真 bug）

`plan_chat_web` demo 在某些时序下出现 **"已完成 graph 被 ctx 拦下"** ——本应正常退出的 graph 被 deadline 拦截，看起来像 hang。

根因追查发现不是 ctx 用法问题，而是 **frontier-based 调度模型缺一个"全图同步点"判 isEnd**：
- `isEnd` 判断分散在每个节点 return 后
- fan-out 分支异步完成，frontier 已空但 cleanup 未走完
- 凑巧时序下"是否结束"语义不明确

LangGraph / eino 的 channel-based 模型有"全图 channel merge → 自然得到 isEnd"的统一同步点（详见 [`../description/three-way-comparison/state-model.md`](../description/three-way-comparison/state-model.md) 第五章可学习清单 + 第六章决策记录），我们没有。

## 2. 根本设计问题

调试过程中浮现的 v1 GraphState-based 模型 5 大深层问题：

### 2.1 单一 GraphState mutex 抢锁瓶颈

所有节点读写同一个 `*GraphState`，fan-out 分支共享态难隔离。得加 `ProcessState[T]` typed local state + 锁包装绕过去——业务侧反直觉，写起来像写 SQL transaction。

### 2.2 cp 整体序列化包袱

每个超步把整个 GraphState 一锅端 marshal，部分失败要补 `superstep_snapshot` per-branch delta hack 才能 Resume 只重跑失败分支（ISSUE-46）。这是个明显的 workaround——本质上 GraphState 粒度错了，应该 per-channel 自描述独立序列化。

### 2.3 嵌套 cp 物理分离绕弯（ISSUE-70 Phase 1-2）

子图 cp 内嵌父 cp metadata 的 `SubGraphs map[string]*checkpoint` 一路递归 marshal，跨进程 Resume 解包又一路 `forwardCheckPoint` 转 ctx。复杂且易出 bug——后来 Phase 2 改为 (sid, ns) 二级 key 物理分离才稳定，但改动横跨 N 个文件。

### 2.4 嵌入运行接口割裂（ISSUE-71 之前）

`Agent` vs `NestableAgent` 双接口设计，9 编排有的能嵌入有的不能。ISSUE-71 接口合并后才补齐——但是补 PlanAgent + GraphAgent 嵌入路径的工作量本身就证明"v1 接口设计有问题"。

### 2.5 跟 LangGraph / eino 离得远

[三方对比文档](../description/three-way-comparison/) 反复出现"我们的 frontier-based 离 channel-based 还有距离"。未来想借鉴 LangGraph Pregel 的新特性（如 stream events / channel topic 路由）都要先翻译，等于二次开发。

### 最后一根稻草

设计文档写：**"channel-based 是更优设计，唯一阻挡是 Go 泛型实现 channel API 是否够干净"**——因为 eino 的 `field_mapping` 是 Go 类型擦除妥协的反面教材，业务难用。

## 3. PoC 单一判定

[`channel-based-design.md`](./channel-based-design.md) 当初设计稿只问一个问题：

> **Go 泛型能不能做出比 eino field_mapping 更干净的 channel API，同时保留编译期类型安全？**

PoC 5/5 通过。`Channel[T]` 泛型 + `Get(chs, ch) (T, bool)` / `Write(ch, v) ChannelWrite` 直接拿到 typed value，业务侧无需类型断言，比 eino 干净一个量级。

```go
// v2: 编译期类型安全 + 业务侧零类型断言
scoreCh := compose_v2.LastValue[int]("score")
score, ok := compose_v2.Get(chs, scoreCh)  // score 是 int

// vs eino field_mapping 风格（Go 泛型不能用时的妥协）：
val, _ := chs.GetField("score")  // val 是 any，要 cast
score := val.(int)
```

那一刻拍板进 production migration（PoC 报告详见 [`poc-report.md`](./poc-report.md)）。

## 4. 实际收益验证（Stage 1-7 后）

| 维度 | v1 | v2 |
|---|---|---|
| 调度统一同步点 | 无（分散判 isEnd 引发原始 bug） | 全图 channel merge 自然同步 |
| Fan-out 分支隔离 | 共享 GraphState + mutex + ProcessState 锁 | 每分支写自己的 channel slot 天然隔离 |
| cp 序列化粒度 | 整 GraphState 一锅 marshal + superstep_snapshot 补丁 | per-channel marshal，cp.Channels 直存 typed values |
| 嵌套 cp 物理分离 | `SubGraphs` 递归 metadata 嵌入 + `forwardCheckPoint` ctx | 直接按 `(sid, ns)` 二级 key 物理分离写 store |
| 嵌入运行接口 | `Agent` + `NestableAgent` 双接口（ISSUE-71 才补齐）| 单 `Agent` 接口 + ctx `graphNS` push 自动支持嵌入 |
| 对齐 LangGraph Pregel | 远 | 直接对应（reducer / channel / superstep frontier 一一映射）|
| typed local state 复杂度 | `ProcessState[T]` + `NewStateFactory` + 锁包装 | 业务直接定义 `Channel[T]` + reducer，不需要泛型壳 |
| `TypedGraph[I,O]` 泛型包装 | 单独抽象包装类型安全 | 不需要——`Channel[T]` 本身就类型安全 |

## 5. 副产品（意外收获的质量提升）

转 v2 的过程不是单纯"换实现"，**意外收获了一堆质量提升**：

- **Stage 4** 系统补 11 个 v1 能力 gap：Retry / RoutingClassifier / Skill / HistoryCompactor / Reflect sub-agent / Aggregator / HandoffState / PlanAgent 完整能力 / WrapMemory（已于 PR 3.3 删，由 store 包 + WithStoreMemory 替代）/ RunSession 富 API / RunSession KV
- **Stage 5** 补 7 项 v1 对齐核对：OrchHooks ctx-aware / Checkpointer 完整接口（8 方法；跨 Run 会话历史后移到 agent_v2 历史层 ISSUE-98）/ 哨兵错误 / GraphID 防串台（Graph.ID() 直读，原 ctx pair 后废弃删）/ 序列化导出 wrappers / Marker 常量 / `AdjustIndexToPreserveAPIInvariants`
- **Stage 6** 期间撞出并修了 v2 的几个边角 bug：
  - `streamOrGenerate` 重组 message 时丢失 chunk Annotations（adv_06 触发 HandoffSignalKey 丢失）
  - `InterruptError.Query nil` v2 严格 fail-fast（plan_chat_web interruptHook 必须填虚拟 Query）
  - `compose_v2.InterruptError.Data` 字段补加（v1 有 v2 漏）
- **nested_smoke v2 重写**新加 Parallel/Routing 嵌入场景（v1 nested_smoke 漏了这两个）+ GraphID 防串台 ⇒ **12/12 比 v1 11/11 覆盖度更高**

## 6. 一句话总结

**直接 trigger** 是一个真 bug；**深层动机**是 v1 frontier-based + GraphState 模型为绕过限制堆了太多 hack；**PoC 决策点**是 Go 泛型能否做出干净 channel API；**实际收益**是设计统一对齐 LangGraph Pregel + 嵌套 / fan-out / cp 三大头疼问题自然解决；**副产品**是顺带补全了 v1 时代积累的 18+ 项能力 gap 与对齐债务。

---

## 关联文档

- [`channel-based-design.md`](./channel-based-design.md) — 设计原始决策
- [`channel-engine.md`](./channel-engine.md) — **引擎层完整设计**（核心抽象 / cp + Resume / fan-out / 嵌套 / 9 编排编译产物 / 43 项不变量速查 / 反模式）
- [`poc-report.md`](./poc-report.md) — PoC 5/5 判定 + Phase 1+2 完成后续扩展
- [`migration-history.md`](./migration-history.md) — Stage 1-7 完整 commit 索引 + v2 公开 API 速查
- [`usage-guide.md`](./usage-guide.md) — 业务侧用法
- [`../description/three-way-comparison/`](../description/three-way-comparison/) — crew-go vs LangGraph vs eino 三方对照决策记录（v1 时代写，结论仍适用 v2）
