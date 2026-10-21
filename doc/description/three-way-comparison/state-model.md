# 状态 / 数据流模型三方对照

> 本文档对照 **crew-go v2（channel-based）/ LangGraph 0.6.10 / eino 主分支**。最初为 v1 时代"frontier 模型要不要切 channel-based"的决策研究（2026-05-07）；channel-based 重设计落地后已更新到 v2 基线，crew-go 侧代码引用均为 `compose_v2/` 路径。


> 最初研究：2026-05-07（v1 frontier 模型 vs channel-based 决策）
> v2 更新：channel-based 重设计落地后
> 范围：LangGraph Pregel channels / eino channel + field_mapping / crew-go v2 `Channel[T]` + reducer + BSP

## 触发场景

需要查阅本文档的设计决策：
- 加新 reducer 类型（超出 `LastValue`/`Append`/`Sum`/`OnceFlag`/`Custom`）
- 设计新编排模式时纠结"用 channel reducer 还是 typed local state"
- 加新 fan-in 形态（动态 barrier / pubsub 累积——v2 仍缺）
- 评估是否引入 channel 级 ephemeral / `WithReducer` 语法糖等三方特性

## 一、三方模型对照速查表

| 维度 | LangGraph (Pregel) | eino | crew-go v2 |
|---|---|---|---|
| 状态抽象 | `dict[channel_name, BaseChannel]`，每 key 独立类型 | `map[string]channel` + 业务侧 typed state（option） | 业务声明的 `Channel[T]` 集合 + 节点侧 `Channels` 只读视图（无固定 struct，`compose_v2/access.go:14`） |
| 节点输入来源 | 节点订阅 channel，channel ready 即触发 | 节点订阅 channel，channel ready 即触发 | 节点用 `Get[T](chs, ch)` / `MustGet` / `Has` 读 typed channel（`access.go`） |
| 节点输出去向 | `apply_writes` 调 `channel.update(values)` | `reportValues` 写到目标 channel | 返回 `[]ChannelWrite`（`Write[T](ch, v)` 构造），graphLoop 在超步合并点调 `cm.ApplyAll` 统一 reduce |
| Reducer 抽象 | 9+ 内置 channel type（LastValue/Topic/BinaryOperatorAggregate/NamedBarrierValue/Ephemeral/Untracked/...） | 2 种内置 channel（Pregel / DAG）+ FanInMergeConfig + 业务 mergeValues | 5 个 reducer 工厂：`LastValue`/`Append`/`Sum`/`OnceFlag`/`Custom`，外加 `DynamicChannel` 运行时注册（`compose_v2/channel.go`） |
| 跨节点字段映射 | channel name 隐式对齐 | `FieldMapping{fromNodeKey, from, to}` 显式声明 | `Channel[T]` 引用（包级 `Get`/`Write` 传引用），无字符串映射、IDE 重命名安全 |
| 类型安全 | Python TypedDict + 运行时校验 | Go 类型擦除（`any`）+ 反射 | `Channel[T]` Go 泛型，节点读写编译期类型保护；类型擦除只在 `ChannelManager` 内部（`channelHandle[T]`） |
| End 检测 | `apply_writes` 后 `if not self.tasks: status="done"`（统一同步点，`pregel/_loop.py:496-498`） | `if v, ok := nodeMap[END]; ok` 在 `calculateNextTasks` 出口（统一同步点，`graph_run.go:720-722`） | `computeNextTasks` 算下一帧 frontier，全为 EndNode 即 `isEnd`，与 `ApplyAll` 合并为统一同步点（`graph_loop.go`，对齐三方） |
| 多前驱 join（fan-in 触发语义） | 默认 any-of；`add_edge([a,b], c)` 编译成 `NamedBarrierValue`（all-of、跨超步累积）；`defer=True` 编译成 `NamedBarrierValueAfterFinish`（等整图收尾）；`Send` 扇出的汇聚不经它、靠超步 | DAG channel 通过 `controlPredecessors` 静态依赖（AND-join） | **只有 any-of**：`assembleFrontier` 只对本超步目标去重、无前驱账本——同超步前驱塌成一次（ParallelAgent finalize 靠这个），深度不等则 join 跑两次、首跑只见部分数据、`err == nil`（实测，见决策 11）。动态扇出的汇聚与 LangGraph 同款靠超步（`SendFrom` → 下一超步并发 → 本帧全跑完才进下一帧）；缺的是声明式 AND-join 与 defer |
| 跨超步累积 | channel 自身持久 + 可选 reducer 累加 | channel 自身持久 + 业务 typed state | `Sum`/`Append` reducer 直接表达（channel value 随 cp 持久化、跨 Resume 安全）——channel 是**唯一** state 模型（v1 `ProcessState[S]` 已删） |
| Send / 动态投递 | `Send(node, payload)` 转 task | `controls` map 显式控制流写入 | `SendFrom(ctx, ...SendDirective)`（`SendDirective{Target, Payload []ChannelWrite}`），Payload 走 channel reducer 注入下一超步（`compose_v2/send.go:67`） |

## 二、各家详解

### LangGraph：channel-based + 多种 reducer

每个 state key 是独立 channel object，`tick()` 调度按"哪些 channel 被更新→哪些节点订阅了→trigger 它们"推进（`pregel/_loop.py:439-519`）。优势：

- **End 检测在统一同步点**：`apply_writes` 后 `prepare_next_tasks` 决定下批 task；空集即 `status="done"`——所有控制流路径必经此点
- **reducer 多样**：`BinaryOperatorAggregate`（任意二元操作累加，`channels/binop.py:86-94`）、`Topic`（pubsub 可选 accumulate，`channels/topic.py:23-32`）、`NamedBarrierValue`（`add_edge([a,b],c)` 的编译产物：names 编译期固定、全到才 ready，`channels/named_barrier_value.py:13-82`；用户代码与 tests 零处直接构造）覆盖大量"难手写"场景
- **EphemeralValue / UntrackedValue**：表达"step 内可见、跨 step 不持久"或"完全不参与 cp"的语义（`channels/ephemeral_value.py:15-26`）

历史包袱标记：channel name 字符串隐式对齐 + Python TypedDict 是 [Python 历史包袱]——Go 不需要，强类型签名即可表达。

### eino：channel-based + field_mapping

`channelManager.updateAndGet` 是 LangGraph tick 的对应物：合并 writeChannelValues → channel.get → 返回 ready node map，End 节点订阅父图所有 channel，统一在 `calculateNextTasks` 出口判 `nodeMap[END]`（`graph_run.go:708-722`）。优势同 LangGraph 的"统一同步点"。

历史包袱标记：

- `FieldMapping{fromNodeKey, from, to}`（`field_mapping.go:31-37`）是 [Go 类型擦除妥协产物]——Go 没有 TypedDict 隐式对齐，又想做"channel 命名 wire"，只能让用户显式声明每个 from→to 字段映射，API 体感繁琐
- 仅 2 种内置 channel（pregel / dag）也较少，业务想要 BinaryOperatorAggregate 类语义须自实现 mergeValues

### crew-go v2：channel-based + `Channel[T]` 泛型 + reducer

节点签名 `NodeFunc func(ctx context.Context, chs Channels) ([]ChannelWrite, error)`（`compose_v2/graph.go:39`）。节点用包级泛型 `Get[T](chs, ch)` / `MustGet` / `Has` 读 typed channel（`compose_v2/access.go`），返回 `[]ChannelWrite`（`Write[T](ch, value)` 构造）；write 不立即生效，由 graphLoop 在超步合并点调 `ChannelManager.ApplyAll`（`channel_manager.go`）统一 reduce。

特点：

- **5 个 reducer 工厂覆盖 90% 场景**（`compose_v2/channel.go`）：`LastValue[T]`（next 覆盖 prev）/ `Append[T]`（追加，预分配新 backing 防并发 race，audit R9 C3）/ `Sum`（int 累加）/ `OnceFlag`（单调 bool，写入 true 后锁定）/ `Custom[T]`（业务自定义 reducer）；另有 `DynamicChannel` 运行时注册 escape hatch（10% 动态场景）
- **reducer 由用户显式声明**，不内嵌默认 LastValue——避免 EndChannel 用 LastValue 时被覆盖为 false 的语义陷阱（`channel.go:51-53`）；终止信号用 `OnceFlag` 表达单调
- **`Channel[T]` 泛型编译期类型保护**：`Get`/`Write` 传 channel 引用，value 类型编译期校验；类型擦除只在 `ChannelManager` 持 `[]anyChannel` 时发生（`channelHandle[T]`），节点 API 仍 typed
- **跨超步累积**用 `Sum`/`Append` reducer（channel value 随 cp 持久化、跨 Resume 安全）——channel 是**唯一** state 模型（v1 GraphState / `ProcessState[S]` typed local state 已删：生产零 caller + fan-out 抢锁瓶颈，理由见 [`why-channel-based.md`](../../v2/why-channel-based.md) §2.1）
- **End 检测统一同步点**：`computeNextTasks` 算下一帧 frontier 与 `ApplyAll` 合并写入，消除 v1 "done 检测散落 + ctx-vs-done 窗口" 根因（详见 `channel-engine.md`）

reducer 约定（`channel.go:16-47` godoc）：纯函数、内部禁止访问 `cm`（ApplyAll 归并段持锁调 reducer，重入 RLock 死锁）、满足关联 + 交换律（fan-out 多分支并发 reduce 顺序受调度影响）、无 error 返回、闭包不持久化跨 Resume（状态用 channel value 自身承载）。

## 三、crew-go 已有优势（具体可证伪）

| # | 优势 | 证据 / 行号 | 反例对照 |
|---|---|---|---|
| 1 | Go 编译期类型安全的 channel 读写 | `Get[T](chs, ch)` / `Write[T](ch, v)` 泛型，类型不符编译期飘红（`compose_v2/access.go:24,122`） | eino `field_mapping{from, to}` 字符串 + 反射 cast，重构靠 grep |
| 2 | reducer 极简：4 内置 + Custom 覆盖 90% | `LastValue/Append/Sum/OnceFlag`（`channel.go:114-174`）+ `Custom`（:189）；业务无需先理解一套 channel 类型体系 | LangGraph 9+ channel 类型，业务须先分清各自何时用 |
| 3 | PlanAgent 用 5 个 channel 声明式表达状态机 | `planOriginalCh`(LastValue) / `planExecutedCh`(Append) / `planCurrentCh`(LastValue) / `planAnswerCh`(LastValue) / `planDoneCh`(OnceFlag)（`agent_v2/plan_agent.go-85`）；Done 用 OnceFlag 单调、随 cp 持久化 | LangGraph 等价物需声明多 channel + 给 Done 设独立单调 channel |
| 4 | 嵌入运行不需要 channel 协议 | 子 agent 通过 ctx `compose_v2.WithGraphNS(ctx, JoinNS(parentNS, "<childName>"))` 注入位置（ISSUE-71 接口合并; v1 `EnclosingParent` ctx-value 已删跟 channel-based 模型对齐），不必把子 agent 输出绑父 channel | — |
| 5 | 无 FieldMapping 字符串配置 | 节点持 `*Channel[T]` 引用读写，channel 变量重命名编译期飘红 + IDE 安全（`access.go` godoc 对比 eino） | eino `FieldMapping{from, to}` 字符串配字段（`field_mapping.go:31-37`） |
| 6 | 超步快照粒度对齐 Pregel | per-branch delta + 失败分支 rerun + 成功分支不重跑（`graph_loop.go` executeFrontier） | 与 LangGraph pending_writes 等价；eino 无此粒度（整超步重跑） |

## 四、crew-go 不足（具体可证伪）

**v1→v2 channel-based 重设计已解决的不足**（保留备查）：

| v1 不足 | v2 现状 |
|---|---|
| End 检测无统一同步点 / ctx-vs-done 窗口 | 已修：`computeNextTasks` + `ApplyAll` 统一同步点（`graph_loop.go`，详见 `channel-engine.md`） |
| 跨超步累积只能走 typed state | 已修：`Sum` / `Append` reducer 直接表达，且随 cp 持久化 |
| fan-out 合并规则不可配置（强制 append / last-writer） | 已修：每 channel 自带 reducer，`Custom` 表达任意合并规则 |

**v2 仍存在的不足**：

| # | 不足 | 证据 | 他家对照 |
|---|---|---|---|
| 1 | 多前驱节点只有 any-of 触发，缺声明式 AND-join 与 defer | 深度不等的 join 节点跑两次、首跑吃半份数据、`err == nil`（探针实测 `order=up,down,side,other,down`；LangGraph 同图 down 跑一次且在 side 之后）。9 编排不撞——builder 生成的图前驱恒同超步（ParallelAgent 一次 `SendFrom` 投 N 个 worker → finalize）；GraphAgent 手搓图会撞。绕法：前驱各 `Write(Sum,1)` + join 空转 + cond→EndNode（多一次 visit）。⚠️ 旧描述「缺动态 fan-in barrier（`NamedBarrierValue` 式）」是张冠李戴：它守的是静态 AND-join；动态扇出「运行时 spawn N 分支、全到才汇总」两家都靠超步（`SendFrom` + `Append` 汇聚，`send.go` map-reduce 范例），我们已等价覆盖 | LangGraph `add_edge(list, end)`（`graph/state.py:928-935` docstring「wait for ALL」）→ `attach_edge` 编译 `NamedBarrierValue`（`:1551-1575`）；`defer=True` → `NamedBarrierValueAfterFinish` + `apply_writes` 末尾 `finish()`（`pregel/_algo.py:337-342`） |
| 2 | ~~无 ephemeral（channel 一律持久）~~ **非目标（2026-09-03 改判）** | LangGraph 的 `EphemeralValue` 是**引擎内部触发机制**：`START` 与每个节点的 `branch:to:<node>` 触发 channel（`graph/state.py:1354,1526-1530`），静态边 / 条件边往它写 `None`、按版本触发；用户态 `Annotated[T, EphemeralValue]` 在它自己的 StateGraph 测试与 docs 里零用例。我们的控制流不在 channel 里（frontier 是 `[]FrontierMember`、Send 走 ctx sink），那个内部需求不存在；用户态两家皆零消费者。channel value 一律随 cp 持久化这一点不变，也不再视为缺口 | LangGraph `channels/ephemeral_value.py:15-26` |
| 3 | Send 是"投递给 Target"非"纯 pub" | `SendDirective{Target, Payload}` 需指定 Target 节点（空 / EndNode Target 被静默跳过而非报错，`graph_loop.go`）；Payload 已走 channel reducer，但仍非"写共享 channel 让若干订阅者各自按调度激活" | LangGraph `Send` / channel pub-sub 订阅模型 |

## 五、可学习清单（v2 落地后刷新）

| 他家方案 | 学 / 不学 / 已学 | 状态 |
|---|---|---|
| LangGraph `apply_writes → if not tasks: done` 统一同步点 | **已学**：v2 `computeNextTasks` 返回 `(frontier, isEnd)` 合并判定 + `ApplyAll` 统一 reduce（`graph_loop.go`） | 已落地 |
| LangGraph `apply_writes` 任务路径排序（fan-in reduce 顺序确定性）| **部分学**：整超步重跑路径已确定；只重跑失败分支路径顺序会变、需该场景 reducer 顺序无关；不学"每次全排序"——它靠整超步重跑实现、与我们 per-branch delta 优势冲突 | 取舍（见决策 6）|
| LangGraph `BinaryOperatorAggregate` reducer | **已学**：`Sum` 内置 + `Custom[T](name, reducer, zero)` 表达任意二元累加（`channel.go:160,189`） | 已落地 |
| LangGraph `Topic`（pubsub append/accumulate） | **部分已学**：`Append[T]` 覆盖累积语义；纯 pub-sub 多订阅者调度未做 | 部分 |
| LangGraph `add_edge([a,b],c)` 静态 AND-join（编译成 `NamedBarrierValue`）+ `defer=True` 收尾节点 | **按需做**：仓内零消费者、9 编排不撞；将来候选 `AddJoinEdge([]NodeID, to)`（cp 存 seen 集合）或节点级 defer（`assembleFrontier` 算出 live 为空时再放一批），都动 cp 序列化与 Resume。教学面已补：`AddEdge` godoc 写明 any-of 与「深度不等 ⟹ 双触发」 | 待需求（见决策 11） |
| LangGraph `EphemeralValue`（step 内可见跨 step 清） | **非目标**：它是引擎内部触发机制，我们的 frontier 模型让需求不存在；用户态两家零消费者（见 §四 不足 2） | 已决策（2026-09-03） |
| LangGraph 9+ channel 类型完整体系 | **已决策不学**：v2 选 4 内置 + Custom，保留极简学习曲线，验证够用 | 已决策 |
| eino `FieldMapping{from, to}` 字符串字段映射 | **已决策不学**：v2 `Channel[T]` 引用 + 包级 `Get`/`Write` 编译期类型安全，优于字符串映射 | 已落地（反向案例） |
| eino `controls` 单独走控制流 channel | **已学**：`SendDirective` 支持空 Payload"只调度不传参"（`send.go:43`），等价控制流投递 | 已落地 |
| eino DAG channel `dataPredecessors`/`controlPredecessors` 双轴依赖 | **不学**：frontier + `SendFrom` 已表达动态投递，分轴增加心智负担 | 已决策 |
| eino DAG 模式环路检测 | **已决策不学**：BSP/Pregel 循环回边是一等公民（BasicAgent `model→tools→model` 自身即环），环检测是 DAG 范式卫生检查、与 BSP 自相矛盾；见决策 7 | 已决策 |
| eino `Graph[I,O]` / `Runnable[I,O]` 泛型图外壳（端到端 typed I/O） | **已决策不学**：`Channel[T]` 节点读写层类型安全已强于"图外壳 I/O 签名"那层，再包泛型壳净负；图 I/O 统一 `*schema.Message` 无需类型参数；见决策 8 | 已决策 |
| LangGraph `interrupt_before` / `interrupt_after` 节点级中断 | **部分学**（2026-09-18 更正）：节点内动态中断已落地——`InterruptError` / `compose_v2.Ask` + `InterruptSite`（位置、相位、任务、代际各维）寻址 + 嵌套子图（参见 `interrupt-resume.md`）；**声明式中断点（按节点名配置、不改节点代码）没有**，见中心 backlog「没有声明式中断点」 | 部分落地 |
| LangGraph 5 stream_mode | **已学**：`OrchestrationHooks.OnNodeEvent`（实时投递）+ `OnNodeStart/End`，前端 SSE 已用 | 已落地 |
| LangGraph `_pending_interrupts` 多 interrupt 按 ID 路由 | **已学**：`WithResumeUserInputs(map[InterruptID]*Message)` + 框架按 8 个维度（7 个位置维 + 任务代号）算稳定 ID，被条件边否决重做之后再加重做代际（按分域）与外层代际链（嵌套图里） | 已落地 |
| eino subgraph 节点头等公民 | **已学**：嵌套子图 cp 物理分离（`(sid, ns)` 二级 key），参见 `subgraph-isolation.md` | 已落地 |
| LangGraph cp time-travel（parent_id 串联历史 cp） | **不学**：与 LLM agent 场景不匹配，cp 体积线性涨；成功即清更合适 | 已决策 |

## 六、决策记录（channel-based 已落地）

> channel-based 重设计已落地——v2 即结果。本节保留最初决策 + 落地结论。

1. **v1 today bug 根因消除**：v1 A 方案（`graphLoop` 调换 done/ctx 顺序）是临时止血；v2 channel-based 用 `computeNextTasks` 合并 isEnd + `ApplyAll` 统一同步点彻底消除根因（详见 `channel-engine.md`）
2. **channel-based 重设计完成**：`Channel[T]` Go 泛型做出比 eino `field_mapping` 更干净的 API（引用传递 + 编译期类型）+ 保留类型安全；9 编排全部迁移到 channel 模型
3. **不学 eino FieldMapping**：已落地——保持 channel 引用 + 包级泛型读写，不引入字符串字段映射层
4. **v1 优势保留**：编译期类型安全、嵌入不需 channel 协议、"零/极简 reducer 学习曲线"（v2 用 4 内置 reducer 替代 v1 的零 reducer，仍远少于 LangGraph 9+）这些优势 channel-based 后保留
5. **剩余待补**：~~动态 fan-in barrier（`NamedBarrierValue` 式）+ channel 级 ephemeral~~ → 2026-09-03 调研改写为「多前驱只有 any-of，缺声明式 AND-join / defer」（见决策 11、第四节不足 1）；ephemeral 转非目标
6. **fan-in reduce 顺序确定性**：不学 LangGraph"每次 apply 按任务路径全排序"——它无 per-branch delta、整超步重跑、排序免费；我们有 per-branch delta 优势（只重跑失败分支省 LLM 调用），全排序需 cp 存各分支 delta + fan-out 前快照、成本高且收益场景罕见。接受现状：① 同 Run 内顺序确定（`executeFrontier` 按 frontier 索引收集）；② 跨 Resume 两种行为——**整超步重跑**（`ApplyAll`/reducer panic 异常路径）顺序一致、**只重跑失败分支**（业务失败 / 中断主路径）顺序变，故该场景 reducer 需顺序无关（`channel.go` reducer godoc 已注明两种行为）。eino 无防御（fan-in 走 Go map 随机顺序、`mergeValues` 无 sort）不参考。
7. **不做图拓扑环路检测 / 全图可达性校验**：`GraphBuilder.validate()`（`compose_v2/graph.go`）只做编译期基础校验——图非空、每个 nodeID 过 ns 命名规范（`ValidateNodeIDForNS`，防 `/`、`#` 错位导致 cp 跨 ns 串台）且不是起止节点名、静态边的源/目标节点存在且不指回 START（条件边目标运行时才知、跳过）、至少一条 START 出边；**不检测环路，也不做可达性 / 孤儿节点 / dead-end 校验**。
   - **环检测——对 BSP 执行模型是反模式**：v2 是 superstep + frontier 的 BSP 引擎，循环回边是一等公民——BasicAgent 的 `model → tools → model`、LoopAgent / ReflectAgent / PlanAgent 的回边都是环。"检测到环就报错"等于让框架拒绝自己赖以运行的核心拓扑。环检测属于 DAG 范式的卫生检查、不属于 BSP 范式——我们是 BSP，不做是模型自洽。
   - **可达性 / 孤儿 / dead-end**：核心 9 编排的图拓扑由框架 builder 程序化生成、正确性由框架保证，运行时再校验无收益；唯一有价值的场景是 GraphAgent 用户手搓任意图时的早报错（如没有边能到 END 会 maxVisit 耗尽才暴露）。列为**可选低优先级补强、仅对 GraphAgent 有意义**，非核心欠账。
8. **不做 `TypedGraph[I,O]` 泛型图外壳**：v1 曾有 `compose/typed_graph.go`（入口/出口转换函数 + Go 泛型编译期校验），v2 channel-based 重设计时移除（对照表 §一"类型安全"行 + 已有优势 §三-1）。**我们的类型安全在更关键的层、且强于做图外壳的收益**：`TypedGraph[I,O]` 保住的只是"图最外层 I/O 签名"那一层；而真正高频的节点读写走 `Channel[T]` + 包级 `Get[T]` / `Write[T]`（`compose_v2/access.go:24,122`），是 Go 泛型编译期类型保护（channel 变量重命名编译期飘红、IDE 安全）。再包一层 `Graph[I,O]` 泛型壳只换来外壳那层签名，而我们在更要紧的节点读写层本就更强 → 净负。定位上图入口/出口统一是 `*schema.Message`，不需要任意 `I/O` 类型参数（那是通用数据流引擎、节点输入输出可为任意类型时才需要的抽象）。
9. **删 `ProcessState[S]` typed-state 子系统（2026-07）**：v1 GraphState 遗留的 typed local state（`ProcessState`/`GetLocalState`/`NewStateFactory`/`WithStateFactory`，原 `compose_v2/state.go`）**生产零 caller**——9 编排 + example 全用 channel，只测试和框架注入机制引用（同 currentNS 协议因零 caller 删净的先例 commit `08bd6a7`）。删后 **channel 是唯一 state 模型**。
   - **为什么删而非留**：`why-channel-based.md` §2.1 已定性它是"绕过 v1 GraphState 抢锁的 hack、业务反直觉像写 SQL transaction"——fan-out 下单一 state + 单 mutex 抢锁串行、不随 cp 持久，channel（细粒度 reducer + 随 cp 持久 + 分支天然隔离）全覆盖且更优。留一个"推荐别用、生产没人用"的 v1 遗留违反"项目未上线不留兼容路径"。
   - **代价**：从此**无 Run 级 ephemeral**（ProcessState 曾提供）——但因零 caller，删除无实际影响；真需要 step 内 ephemeral 时评估加 channel 级 ephemeral reducer（第四节不足 2）。
   - **连带确认 agui/AG-UI 的 StateSnapshot 只能业务 opt-in**：StateSnapshot 需"发业务结构化 state"。LangGraph 能 node-exit 自动发，是因为它有统一 State TypedDict（Python runtime 可 introspect schema keys 后过滤）；crew-go 删 ProcessState 后 channel 是异构声明集合、无统一 state schema，且 Go 泛型 runtime 擦除拿不到字段 schema——故 agui StateSnapshot 走业务 `EventSink.Send` opt-in（对应 LangGraph 的 `manually_emit_state` 路径），是 channel-based + Go 类型系统的自然结果、非缺陷。
10. **`LastValue` 是单写入者 channel，并发写当场拒（2026-09-02，`06f8260` + `b021b56`）**：同一超步里两个成员写同一个 `LastValue`、或两条 `SendFrom` 指令的 payload 写它 ⟹ 引擎在归并前拒（`%w ErrRunAborted`，正文点出 channel + 两个写入者 + 三条出路），不再静默丢一个。它的 reducer 是 `return next`，并发下赢家由 frontier 索引序（声明序）决定、跟意图无关——「N=1 对、N≥2 静默错、err==nil」那一族。
   - **LangGraph 同款不变量、且是内建的**：`channels/last_value.py:56-64` 的 `LastValue.update()` 在 `len(values) != 1` 时抛 `InvalidUpdateError(ErrorCode.INVALID_CONCURRENT_GRAPH_UPDATE)`，错误正文直接给出路（"Use an Annotated key to handle multiple values"）；逃生舱是 `AnyValue`（`channels/any_value.py:15-17`，docstring "assumes that if multiple values are received, they are all equal"，取最后一个不报错）。这道守卫不是我们自创的防御，是这一类引擎的既有共识。
   - **我们不加新 channel 类型**：`Custom(name, func(_, next T) T { return next }, zero)` 已经是现成的「谁最后写谁赢、我不在乎」——显式写下同一个 reducer 的不受管。少一个 API、同一个形状。
   - **一处设计差异决定了我们多一条要拦的路**：LangGraph 的 `Send(node, arg)` 把 `arg` **直接**当目标节点的入参（`types.py:704-715`，"the sent state can differ from the core graph's state... invoke the same node multiple times in parallel with different states"），不过共享 channel；crew-go 的 `SendDirective.Payload` 灌的是**共享** channel。所以 N 条 payload 写同一个 `LastValue` 只剩最后一条、N 个 target 全看到同一个值——这条路在 LangGraph 里结构上不存在，在我们这儿要单独检测（按 directive 分组，成员分组抓不到——N 条常出自同一个节点）。`send.go` godoc 的 map-reduce 示例原来教的正是这个写法。⚠️ **我一度把它改成「`Append` 装全部输入、目标按自己那份的标记取」并在这儿记成已决——那也不成立**：worker 拿不到自己是第几个（`InstanceFanoutIndex` 返回进程级单调计数器，不是 0..N-1，也没有别的 API 给序号）。所以「同 target 扇出 N 个实例、每个实例不同输入」**今天没有正确写法**，godoc 已改成如实声明；不同输入只能走不同 target（ParallelAgent 的 `worker_<name>`）。
11. **多前驱 join 是 any-of，缺的是静态 AND-join / defer——不是「动态 fan-in barrier」也不是 ephemeral（2026-09-03 单视角调研 + 主会话复跑探针）**：backlog 那条「动态 fan-in barrier（`NamedBarrierValue` 式）+ channel 级 ephemeral」两半都不成立。
   - **`NamedBarrierValue` 不动态、不是用户 API**：`names` 由 `add_edge(list, end)` 编译期 `set(starts)` 定死（`graph/state.py:1551-1575`），写未知名字直接 `InvalidUpdateError`（`channels/named_barrier_value.py:56-67`）；LangGraph 自己的 tests / docs 零处直接构造。`Send` 扇出的汇聚走保留 channel `TASKS`（`Topic(accumulate=False)`）+ 超步，链上没有它——「动态 fan-in」两家都是超步本身，我们已等价覆盖。
   - **我们的多前驱是 any-of**：`assembleFrontier`（`compose_v2/out_edges.go:121-152`）只对本超步目标去重、无前驱账本。探针（`-overlay` 编译、仓库零改动）：START→A→C + START→B→B2→C ⟹ `order=B,A,B2,C,C`；逐字镜像 LangGraph `test_simple_multi_edge` ⟹ `order=up,down,side,other,down`（LangGraph 里 down 跑一次且在 side 之后）；手写 barrier（`Sum` 计数 + join 空转 + cond→EndNode）可行，`C_visits=2 C_realWork=1`。LangGraph 默认同样是 any-of（两条 `add_edge(x,c)` 深度不等也双触发——正是它要 `defer` 的原因），差别是它另给两个 opt-in。
   - **取舍**：先不做功能——仓内零消费者（`okr.md` KR7.3 讲的是 merge 函数可插拔，reducer 早覆盖，触发语义没提）、9 编排前驱恒同超步、手写绕法能跑；**教学面当场补**（`AddEdge` godoc）。将来真需要时两个候选（`AddJoinEdge` / 节点级 defer）都动 cp 序列化与 Resume，属高风险 commit。
   - **ephemeral 转非目标**：见第四节不足 2。
   - **开放决策（未定，进 `doc/code/todo.md` backlog）**：A. 把 `Payload` 做成目标实例的**私有入参**（复用出边计算已有的叠加视图机制；payload 不再进共享状态；`FrontierMember` 需加持久化字段随实例活到跑完；可顺带删掉 `pendingSendWrites` 补 apply 那套与 payload 冲突检测）——这正是 LangGraph `Send(node, arg)` 的形态；B. 只暴露扇出序号 `FanoutOrdinal(ctx)`（同样要持久化到 `FrontierMember`），业务照「Append 全量 + 按序号取」写。生产侧 `Payload` 零使用者，两条路的爆炸半径都只在业务 GraphAgent。
   - **`ErrRunAborted` 的理由不是「重跑必然一样」**（独立审计实测证伪两次：条件写换批数据就 err=nil；冲突方之一第一次恰好失败时守卫看不到它），而是「这个矛盾会随存档反复复现，留 Pending 档只会让 auto-load 一遍遍撞」；代价（清子树连带清掉已答过的挂起档）写进 godoc。

## 七、参考资料

- LangGraph 源码：`/Library/Python/3.9/site-packages/langgraph/pregel/_loop.py:439-552`、`channels/{base,binop,topic,named_barrier_value,last_value,ephemeral_value}.py`
- eino 源码：`/Users/gupengfei/program/go/src/eino/compose/{graph_run.go:708-731, graph_manager.go:115-217, pregel.go, field_mapping.go:31-60}`
- crew-go v2 关键文件：`compose_v2/{channel.go, access.go, channel_manager.go, graph.go:39, graph_loop.go, send.go}`（`state.go` 已删，见上第 9 条）、编排状态承载示例 `agent_v2/{plan_agent.go, parallel_agent.go}`
- 兄弟文档：`subgraph-isolation.md`（子图边界）/ `interrupt-resume.md`（HITL 寻址）/ `checkpoint-storage.md`（cp 存储）/ `address-routing.md`（路由）/ `agent-interface.md`（接口合并）
