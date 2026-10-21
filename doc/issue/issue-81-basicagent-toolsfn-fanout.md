# ISSUE-81 · BasicAgent toolsFn fanout 化

**优先级**：MED → 落地后 HIGH（性能 + 工程一致性）
**状态**：✅ 已修（2026-05-15）— 节点内 goroutine + WaitGroup + MergeInterrupts，完全
复用 ISSUE-78/79/80 三前置基础
**触发**：用户挑战"BasicAgent 改 graph 了为什么 toolsFn 还是串行" + 三方对照
（LangGraph asyncio.gather / eino goroutine+WaitGroup 都并发）

## 问题

`agent_v2/basic_agent.go::makeToolsFn` 历史是简单 for 循环，多 tool_call 串行执行：

```go
for _, tc := range toolCalls {
    callCtx := compose_v2.WithCurrentToolCallID(ctx, tc.ID)
    res, err := dispatchToolkit.Call(callCtx, ...)
    if err != nil {
        return nil, err  // 任一 tc 抛错 early return → 后续 tc 不跑
    }
    ...
}
```

**矛盾点**：
1. BasicAgent 已是 channel-based Graph (preamble + model + tools 三节点)，graph 引擎
   原生支持 fanout (Send / SendFrom)
2. ParallelAgent / Plan / Reflect / 7 编排都已 goroutine 并发
3. 但 `tools` 单节点内部 N 个 tc 仍按 for 循环串行
4. ISSUE-78 / 79 / 80 三前置全修完 (NS hash 隔离 + 半共享 sess + lane events)

**三方对照**:
| | LangGraph | eino | crew-go 旧 |
|---|---|---|---|
| 多 tc 调度 | asyncio.gather / executor.map | go func + sync.WaitGroup | for 串行 |
| NS 隔离 | ToolNode 单 NS / Send 用 idx | AddressSegment.SubID = tc.ID 字面 | fnv32a(tc.ID) hash |
| 多 Interrupt 聚合 | scratchpad 多 idx 平铺 | InterruptSignal 树 + Subs[] | 之前**单 Query 字段**漏 N-1 query |

## 修法

### 1. compose_v2 协议扩展

**`InterruptError.Queries` 字段**（multi-query 容器）：
```go
type InterruptError struct {
    Reason  string
    Query   *AskQuery     // 单 query (向后兼容, 99% 业务场景)
    Queries []*AskQuery   // 多 query (toolsFn fanout 多 tc 同时抛 Interrupt)
    Data    any
}

// AllQueries 协议: Queries 优先 → fallback [Query] 单元素
func (e *InterruptError) AllQueries() []*AskQuery
```

**`MergeInterrupts(ies []*InterruptError) *InterruptError` helper**：
- len==0 返 nil
- len==1 直接返该 ie 不包装（保 errors.As / IsInterrupt 兼容）
- len>=2 创建新 InterruptError 收集所有 ie.AllQueries() 到 Queries 字段

**`WithCurrentToolCallContext(ctx, tcID, branchIdx)` 一站式 ctx 协议**：
3 维度同时注入让多 goroutine 各 tc 独立：
- `currentToolCallIDKey` (tc.ID): AgentAsTool / skill_tool fork hash 派生 childNS（ISSUE-78）
- `nodeBranchIdxKey` (i): ask_user 等业务工具算 InterruptID 5 维第 4 维让 N tc 并发不撞
- `nodeInterruptCounterKey` (新 atomic.Int64=0): 每 tc 独立 ask_user 多次调用 idx 计数

### 2. graph_loop 4 处适配 AllQueries 协议

`runNodeWithHooks` (line 768-781) / SendDirective ConditionalEdge (line 552-561) /
`recordInterrupt` 收集 (line 425) 全部改 `for q := range ie.AllQueries() { ... }`，
向后兼容单 query 路径。

InterruptID auto-fill 用 `i` 作 idx 维度让 multi-query 各自不同：
```go
q.InterruptID = MakeInterruptID(graphNS, nodeID, visit, branchIdx, int64(i))
```

### 3. agent_v2 makeToolsFn fanout

- 抽 `runOne(callCtx, tc) outcome` 闭包处理单 tc 完整流程（含 P2.D inline body）
- N==1 inline 跑（零 goroutine 开销）
- N>=2 `for + go func + WaitGroup` 并发，各 goroutine 调
  `WithCurrentToolCallContext(ctx, tc.ID, i)` 独立 ctx 协议
- 收集 outcomes 按 idx 顺序保 tool_result 配对，多 InterruptError 用 MergeInterrupts
  聚合返单 ie

### 4. tool.Tool godoc reentrant 协议

新增协议声明：业务实现 Tool.Call 必须 reentrant 安全——同 *Tool 实例可能被多
goroutine 同时调 Call。framework 内置 *agentTool / *skillTool / *FuncTool 都已
reentrant；业务自定义实例字段共享必须 mutex 保护。

## 关键不变量

1. **tool_result 顺序按 toolCalls idx 保留**——OpenAI/Claude API 要求 tool_call ↔
   tool_result 配对顺序（不是按完成时间）
2. **P2.D inline body deferredBodyMsgs 全 tool_result 之后统一 append** (R16 audit
   HIGH-1 fix 协议保留)
3. **多 InterruptError 用 MergeInterrupts 聚合**——graph_loop AllQueries 协议
   record 全部 query 防 N-1 query 永久丢失
4. **InterruptError 优先于业务错 propagate**——多 tc 同发 Interrupt + 业务错时
   Interrupt 优先（HITL 控制流必须暂停，业务错被 swallow 在 Resume 后 LLM 看
   missing tool_result 自然重试）
5. **业务 tool 必须 reentrant**——godoc 协议化（之前隐式串行假设掩盖）

## 测试

- `issue81_toolsfn_fanout_test.go` — 4 测试：
  - `Concurrent_NToolCalls_AllRun`: N=10 同 LLM 轮 tc 都并发跑（barrier 强制并发）
  - `OrderPreserved_ToolResultPairing`: 4 tc 完成顺序乱序但 tool_result 按 idx 顺序
  - `MultiInterrupts_MergedAndRecorded`: 3 tc 同时抛 ask_user Interrupt → 3 query 都进
    cm.PendingInterrupts (sanity-revert: 删 MergeInterrupts → 仅 1 query)
  - `InterruptPriorityOverBizErr`: tc1 抛 Interrupt + tc2 抛业务错 → 返 Interrupt
- `-race` 跑全 suite 验 thread-safety
- `nested_smoke` 13/13 PASS 验编排嵌套不破坏

## 性能分析

- N=1: 零 goroutine 开销（仍 inline）
- N=2 (典型): 网络 IO 类工具延迟 ~ max(tc_i) 而非 sum，2× 速度
- N=10 (LLM 偶发): ~10× 加速

## 破坏性变化（视角 D LOW-2 audit 文档化）

**InterruptID 行为变化**（项目未上线 CLAUDE.md "不留兼容路径"允许）：

- N=1 业务场景（最常见）：`InterruptID = MakeID(ns, "tools", visit, 0, 0)` —
  完全等价 pre-ISSUE-81，向后兼容 ✓
- N>=2 同 LLM 轮多 tc：`InterruptID = MakeID(ns, "tools", visit, i, 0)`，i=0/1/2..
  - **pre-ISSUE-81 串行 + early return**：实际只有 tc[0] 抛 ask_user 时算 ID，
    tc[1+] 永远不被跑 → 旧版本 N>=2 时 tc[1+] 没 InterruptID 可记
  - **post-ISSUE-81 并发**：N tc 都跑，各自抛 ask_user 拿 distinct InterruptID
  - **跨版本 cp 不兼容**：旧 cp 含 N=1 视角 InterruptID + 新版本 Resume 时按 N>=2
    视角算 → 不命中。但项目未上线，旧 cp 不存在
- 业务跨版本无影响：N=1 路径完全兼容，N>=2 是新功能（旧版串行不真触发 multi
  Interrupt）→ 无破坏性

**Tool.Call reentrant 协议显式化**：

- pre-ISSUE-81：tool.Tool 接口 godoc 隐式假设串行执行（单 goroutine 调 Call）
- post-ISSUE-81：godoc 显式声明 reentrant 协议——业务实现 Tool.Call 必须
  reentrant 安全（实例字段共享需 mutex 保护）
- framework 内置 *agentTool / *skillTool / *FuncTool 都已 reentrant
- 业务自定义 Tool.Call 隐含状态共享的会 race——godoc 显式化让业务 build 期就
  发现，不再被串行假设掩盖

## 关联

- ISSUE-78: tc.ID NS hash 隔离 (ISSUE-81 前置)
- ISSUE-79: 半共享 child sess (ISSUE-81 前置)
- ISSUE-80: lane events 物理隔离 (ISSUE-81 前置)
- ISSUE-72: 历史"BasicAgent fanout 备选方向" — 此次落地

## 三方对照决策

学 eino 设计：
- ✅ goroutine + WaitGroup（最简洁）
- ✅ 多 InterruptError 聚合（同 eino CompositeInterrupt 思想）
- ❌ 不学 eino Address.SubID 字符串直拼（保留我们 ISSUE-78 hash NS 协议——
  hash 收益是 NS path safety + 与现有 fanout idx 协议一致；hash 成本是碰撞
  概率 N=10 时 2.10e-08 (十亿分之二) 可接受，详见 agent_tool.go::Call godoc
  audit B MED-1 决策）

学 LangGraph 设计：
- ❌ 不学 Send 动态 fanout（需扩 graph 协议"循环内动态 fanout"，触动
  ParallelAgent / Loop / Reflect 基础。方案 A 改动局部、风险低）
