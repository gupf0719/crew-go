# 工具级渐进披露：tool_search

> **一句话**：工具多到全量 schema 喂 LLM 会爆 context 时，开 `WithToolSearch` 让标记为
> deferred 的工具**初始不进 LLM 的 bind list**，LLM 必须先调 `tool_search(query)` 检索解锁，
> 之后才能调用对应工具。

---

## 0. 实现状态（2026-06 落地）

| 件 | 位置 |
|---|---|
| 配置 + 核心逻辑 | [`agent_v2/tool_search.go`](../../agent_v2/tool_search.go)（`ToolSearchConfig` / 揭示集 / meta-tool / 默认关键词检索 / `filterVisibleTools`） |
| option | `agent_v2.WithToolSearch(cfg)`（[`agent_v2/config.go`](../../agent_v2/config.go)） |
| 集成 | `makeModelFn` 过滤 bind list + `makeToolsFn` 注册 meta-tool + runOne fail-closed（[`agent_v2/basic_agent.go`](../../agent_v2/basic_agent.go)） |
| 测试 | mock 单测 [`tool_search_internal_test.go`](../../agent_v2/tool_search_internal_test.go) / [`tool_search_test.go`](../../agent_v2/tool_search_test.go) + 真 qwen smoke [`example/harness/toolsearch_smoke`](../../example/harness/toolsearch_smoke/main.go) |

仅对 **BasicAgent（ReAct）** 生效——它是唯一有「LLM 自选工具」语义的编排。

---

## 1. 背景：工具多爆 context

MCP Hub 聚合多个 server 后，一个 agent 的 toolkit 可能有几十上百个工具。全量 schema 喂给
LLM 有两个问题：

1. **context 成本**：每个工具的 name + description + JSON Schema 都进每次 LLM 调用的 prompt。
2. **选择干扰**：工具一多，LLM 选错 / 漏选的概率上升。

渐进披露的思路：**先不给 LLM 看全部工具**，给它一个 `tool_search` 元工具；LLM 想做某事时先
检索，框架按 query 揭示相关工具，LLM 再调。常驻 bind list 的只有「非 deferred 工具 +
tool_search」。

---

## 2. 设计

### 2.1 核心机制：meta-tool + 过滤 bind list

- **`Deferred` 谓词**判定哪些工具延迟披露（返 `true` = 延迟）。常用 `mcp.IsRemoteTool` 延迟所有
  MCP 工具。
- 每轮 model 节点构建 bind list 时，`filterVisibleTools` 保留「非 deferred 工具」+「已揭示的
  deferred 工具」，丢弃「未揭示的 deferred 工具」。**`tool_search` 自己恒在 bind list**（否则
  LLM 永远调不到它）。
- LLM 调 `tool_search(query)` → 框架按 query 检索 deferred 工具 → 把匹配的**揭示**（记进这一批的
  解锁记录，tools 节点成功后并入揭示集）→ 返回「解锁了哪些工具」给 LLM。
- 下一轮 model 节点构建 bind list 时，已揭示的工具进列表，LLM 可调。

**不是靠 prompt 自觉**——未揭示工具是**物理过滤**出 bind list 的，LLM 在 API 层就看不到。

### 2.2 揭示集存在图状态里（按任务、按调用流隔离）

已解锁集存在 BasicAgent 图的 channel `basicRevealedCh`（`Append[string]`，随存档走），这是**唯一的副本**：
model 节点据它建 bind list，tools 节点据它当这一批的执行闸（§2.4）。一批里 `tool_search` 解锁的名字先记在
tools 节点这次执行上，节点成功后写进 channel；节点中断或失败时随这次执行丢掉，续跑重跑这一批时检索重新记
（`tool_search` 不进工具账）。

- **本任务内累积**：一个任务（一次 Run 连同它的续跑）里解锁过的工具一直可见。续跑带不带原 session
  （含跨进程）都一样。换了任务（`NextTask`，或复用同一个 RunID 再跑一次顶层 Run）是一张新开的图，从空开始。
- **每次从头跑的 Run 都从空开始**：编排配了重试、没配 checkpointer 时，成员的第二次尝试是一张新图，失败那次尝试
  解锁的工具要重新检索（配了 checkpointer 时第二次尝试从存档续上，照样认得）；节点里不经 `RunSubAgent` /
  `AgentAsTool`、在同一个位置直接调两次 `Run` 也是两张图。曾经状态挂在会话 KV 上、按「任务 + 编排位置 + agent 名」
  分，这两种情形下两次 Run 共用一份——第二次尝试一开始就绑着上一次解锁的工具，重复调用提醒把被丢弃那次尝试里的
  调用也算进去（哨兵 `TestToolSearch_RetryWithoutCheckpointerStartsEmpty`）。
- **按调用流隔离**：并行分支、嵌套的被调方各跑各的图、各有各的揭示集，同名 agent 也不共享。
- **同一批里刚解锁的工具从下一批起可调**：执行闸只认这一批开始前已提交的解锁集。模型在这一批里本来就
  看不到刚解锁的工具（这一轮的 bind list 里没有它），同一批里直调属于越界，一律被拒，结果不取决于并发调度。
  曾经闸读的是同一批里检索正在往里加的那份，放不放行看批内排列：实测 `[lookup, tool_search]` 50 次全放行、
  `[tool_search, lookup, noop]` 放行 40 次、`[tool_search, lookup]` 一次不放；现在一律拒、多一轮。
- **会话 KV 上不留东西**：长期复用一个 session、每条消息 `NextTask` 的聊天循环不会随消息数增长。

> 演化（2026-10-13 收敛到上面的形态）：揭示集最早只挂在 `RunSession` 上，不进存档 ⟹ 换 session 的续跑之后
> 是空的，fail-closed 把模型正要调的工具静默拦下（哨兵 `agent_v2/tool_search_resume_test.go`）。后来加了
> channel 作权威副本、会话上那份当缓存，缓存的键先后漏过两维：
>
> - **编排位置**（2026-09-21 `94323cdb`）：`RunSession.values` 在并发分支之间共享（`ForkLane` /
>   `NewSharedChildSession` 共享 map 与 mu），只按 agent Name 分时两个**同名**并行分支读写同一份揭示集——
>   对照组实测，隔壁分支检索一次，从没检索过的分支 `query_db` **工具体真的执行了 1 次**；隔壁不检索则 0 次。
>   哨兵 `agent_v2/tool_search_branch_isolation_test.go`（断言工具体有没有真执行，不是「有没有被拒」）。
>   本文更早把「同名 agent 共享同一揭示集」写成了有意约定，那是失实的——它是个安全缺陷。
> - **任务**（2026-10-13 `4352b056`）：会话活得比任务长，上一个任务解锁的工具下一个任务没检索就能调。
>
> 两份拷贝还会分叉（换任务后不带原 session 续跑，缓存是空的、channel 里有），会话上的键随任务数增长。
> 去掉缓存之后这几类问题整类不存在，上面两条哨兵守的是结构本身。

### 2.3 可插拔检索器

`Retriever` 字段决定「query → 揭示哪些工具」：

- **默认（`Retriever` 为 nil）**：关键词检索——对每个 deferred 候选用
  `skill.KeywordScore(query, name+" "+description)` 评分，过 `MinScore` 后按分降序取前
  `MaxResults`。零向量依赖。
- **自定义**：业务传 `func(ctx, query, candidates) []string` 插语义检索（如包装
  `store.Store.Search` / 向量后端），返回要揭示的工具名。

默认值：`MaxResults` 不设 = 5（`defaultToolSearchMaxResults`）；`MinScore` 不设 = 0（只要命中
即算）。

### 2.4 fail-closed 执行拦截

如果 LLM **越过 bind list** 硬调一个未揭示的 deferred 工具（幻觉，或对约束不严格的 provider
直接 emit 了不在 tools 列表里的 tool_call），`makeToolsFn` 的 runOne 在真正 dispatch 前拦截：
返回 `[tool_not_revealed]` tool_result（**工具本身不执行**），反馈引导 LLM 先调 `tool_search`
解锁。闸认的是这一批开始前已提交的解锁集，同一批里检索刚记下的不算（见 §2.2）。

这是 eino/bigtool 都没有的一道额外防御——真 qwen 实测能触发（见 §6）。

---

## 3. 公开 API

```go
// ToolSearchConfig 配置工具级渐进披露。
type ToolSearchConfig struct {
    Enabled  bool                     // 总开关
    Deferred func(tool.Tool) bool     // 判定哪个工具延迟披露（true=延迟）；nil 或全 false = 无工具延迟（等于关闭）
    Retriever func(ctx context.Context, query string, candidates []tool.Tool) []string // nil = 默认关键词检索
    MaxResults int     // 默认关键词检索单次最多揭示几个（默认 5）；自定义 Retriever 时忽略
    MinScore   float64 // 默认关键词检索的最低分阈值 [0,1]（默认 0=只要命中即算）；自定义 Retriever 时忽略
}

func WithToolSearch(cfg *ToolSearchConfig) agent_v2.Option
```

常用 `Deferred`：`mcp.IsRemoteTool`（延迟所有 MCP 远程工具，本地手写工具仍常驻）。

---

## 4. 用法

```go
import (
    "crew-go/agent_v2"
    "crew-go/mcp"
)

// toolkit 里既有本地工具，又经 MCP Hub 聚合了大量远程工具
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("assistant"),
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(tk),
    agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{
        Enabled:  true,
        Deferred: mcp.IsRemoteTool, // 所有 MCP 工具按需揭示；本地工具常驻
    }),
)
```

自定义语义检索：

```go
agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{
    Enabled:  true,
    Deferred: func(t tool.Tool) bool { return strings.HasPrefix(t.Name(), "mcp_") },
    Retriever: func(ctx context.Context, query string, candidates []tool.Tool) []string {
        // 包装业务向量检索，返回要揭示的工具名
        return myVectorSearch(ctx, query, candidates)
    },
    MaxResults: 8,
})
```

---

## 5. 时序（一次 ReAct）

```
round1 model：bind list = 本地工具 + tool_search（deferred 全隐藏）
              → LLM 调 tool_search("查天气")
round1 tools：tool_search.Call 检索 → 揭示 get_weather（记进这一批的解锁记录，节点成功后并入揭示集）→ 返回解锁列表
round2 model：bind list 现含 get_weather（已揭示）→ LLM 调 get_weather
round2 tools：执行 get_weather
round3 model：拿到结果 → 出最终答案
```

图拓扑 model 节点严格先于 tools 节点（BSP 超步），round2 的 model 一定看到 round1 tools 揭示
的工具。

---

## 6. 测试

**mock 单测**（确定性，`go test ./agent_v2/ -run ToolSearch`）：

- 纯函数：`filterVisibleTools` 三态（非延迟恒可见 / 已揭示可见 / 未揭示隐藏）、这一批解锁记录的并发记
  （`-race`）、默认检索器关键词 + MaxResults、保留名排除（防死锁）、按调用流隔离（含同名并行分支不互串）、
  执行闸不认同一批刚记下的解锁（`TestToolsGate_IgnoresSameBatchReveals`）、会话 KV 不随任务数增长
  （`TestTaskState_SessionKVStaysEmpty`）。
- 端到端：`TestToolSearch_EndToEnd`（deferred 隐藏 → tool_search → 揭示可见）、
  `TestToolSearch_FailClosed`（未揭示工具直调被拒、不执行）。

**真 qwen smoke**（[`example/harness/toolsearch_smoke`](../../example/harness/toolsearch_smoke/main.go)）：

- **Part 1 渐进披露**：deferred 隐藏 → qwen 自主调 tool_search 解锁 → 调 get_weather → 答案落地。
- **Part 2 fail-closed**：强提示 qwen「直接调 get_weather」（未揭示）→ qwen 越过 bind list emit
  get_weather → 框架返 `[tool_not_revealed]`（**第一次直调被拦、不执行**——PASS 判定 gate 在「见到
  拦截反馈」）→ qwen 收到反馈后重走 tool_search。**实测 fail-closed 真触发，非仅 mock 覆盖。**

模型走 fallback 链 `qwen3.6-flash → qwen3.6-flash-2026-04-16 → qwen3.6-plus → qwen3.7-plus`
（额度满/限流/欠费自动切）。

---

## 7. 三方对照

| | 揭示状态存哪 | fail-closed | 检索 |
|---|---|---|---|
| **eino toolsearch 中间件** | history-scan（从对话历史推已揭示） | 无 | client-side 过滤 / model-native 两档 |
| **langgraph-bigtool** | graph-state | 无 | 向量检索 |
| **crew-go** | graph-state（BasicAgent 图的 channel，随存档走；每个任务、每条调用流一张图）| ✅ `[tool_not_revealed]` 拦截 | 可插拔（默认关键词，零向量依赖；业务可插语义） |

crew-go 的揭示集存图状态而非 history-scan（不必每轮重扫历史推断已揭示，也不受压缩层改写消息影响），
并多一道 fail-closed 执行拦截。

---

## 8. 边界 / 限制

- **保留名 `tool_search`**：业务工具若误叫 `tool_search` 且被 `Deferred` 命中，`collectDeferred`
  会**排除**它（保留名给 meta-tool）——否则 `filterVisibleTools` 会把 meta-tool 自己过滤出 bind
  list 导致 LLM 永远调不到 tool_search = 静默死锁。
- **揭示集按任务分**（2026-10-13 定）：同一个任务里的续跑（含跨进程、换了 session）照样认得解锁过的
  工具（揭示集在图状态里、随存档走，见 §2.2）；换了任务从零开始，上一个任务解锁的工具要重新
  检索才看得见、调得了。此前这里写的是「跨 `NextTask` 存活是有意的——揭示不是授权，同一场会话的下个任务
  免重搜是上下文优化」，实际效果是上一个任务解锁的工具下一个任务没检索就绑着、直接调也执行（审计实测），
  跟「揭示集在本 Run 内累积」的说法、跟 `approve_always` 按任务分都不一致；改为按任务分，代价是新任务里
  要再检索一次。
- **fail-closed 依赖 LLM 越界才触发**：对工具调用约束严格的 provider，LLM 本就只能调 bind list
  里的工具，fail-closed 没机会触发（披露机制本身已足够）——它是 belt-and-suspenders，防越界
  /幻觉。
- **仅 BasicAgent 生效**：确定性编排（Sequential / Parallel / Loop 等）没有「LLM 自选工具」语义，
  `WithToolSearch` 对它们无意义。
