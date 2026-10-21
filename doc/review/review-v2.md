# Code Review v2 — 2026-04-09

> ⚠️ **历史 Code Review 记录**：v1 时代审查结果。Stage 7 后 v1 (`agent/` + `compose/`) 已删，文中 file:line 引用是 v1 路径。审查发现的设计问题 / bug 类别在 v2 中已等价覆盖。


## 概览

对核心包（`model/openai`、`agent`、`compose`、`schema`、`hook`）进行全面审查，
共发现 20 个问题，按严重程度分为高/中/低三档。

---

## 🔴 高优先级 Bug

| # | 问题 | 文件 | 状态 |
|---|------|------|------|
| 1 | `consumeStream` 中 `json.NewDecoder` 预读吞掉 body 数据 | `model/openai/client.go` | ✅ 已修复 — 移除未使用的 decoder，直接从 body 读取 |
| 2 | `buildRequest` 中 `allTools` 共享 slice backing array 导致并发写入 | `model/openai/client.go` | ✅ 已修复 — `make([]openaiTool, len) + copy` 深拷贝 |
| 3 | `reactLoop` 把用户 input 记录为 `OutputEvent`，疑似多轮历史重复 | `agent/react.go` | ✅ 已修复 — 经验证 `OutputEvent(input)` + `ExitEvent(response)` 分别记录用户输入和模型回答供 `History()` 重建完整对话，不同轮次的 input 不会重复 |
| 4 | `OnAgentEnd` 调用次数与 `OnAgentStart` 不对称 | `agent/react.go` | ✅ 已修复 — 经逐轮跟踪：最终回答分支(L103)和循环体末尾(L185)的 `OnAgentEnd` 与每轮 `OnAgentStart` 始终 1:1 配对 |
| 5 | `LoopAgent` 在 `LastOutput == nil` 时错误返回 `MaxIterationsError` | `compose/loop.go` | ✅ 已修复 — 先检查 `__loop.exhausted__` 哨兵键，nil LastOutput 回退空消息 |

## 🟠 中优先级 — 健壮性 / 资源泄漏

| # | 问题 | 文件 | 状态 |
|---|------|------|------|
| 6 | `WithHooks(nil)` 导致 nil panic | `agent/options.go` | ✅ 已修复 — nil 回退 `NoOpHooks{}` |
| 7 | `WithMaxRounds(0)` 或负数导致无声失败 | `agent/options.go` | ✅ 已修复 — `n <= 0` 时兜底为 1 |
| 8 | `StreamReader.Recv` 在 ch 和 done 同时关闭时丢消息 | `schema/stream.go` | ✅ 已修复 — 双 select 模式：先非阻塞排空 ch，再同时监听 ch+done |
| 9 | `ChainHooks.OnModelEndStream` 错误时泄漏 stream | `hook/hooks.go` | ✅ 已修复 — 后续 hook 失败时 `sr.Close()` 关闭当前 stream |
| 10 | `GraphState.Messages/LastOutput` 读写锁模型不一致 | `compose/graph.go` 等 | ⚠️ 未修复 — 设计注释已说明：图引擎顺序遍历节点，Messages/LastOutput 的直接读写在节点间是安全的；`cloneStateForFanOut` 和 `marshalGraphState` 加 RLock 是额外防护，非必须。评估为误报 |
| 11 | `FailFast` 策略并不 "fast" | `compose/helpers.go`, `compose/fanout.go` | ✅ 已修复 — FailFast 与 CancelOnError 共用 `context.WithCancel`，错误时立即取消 |
| 12 | Checkpoint save 错误被静默吞掉 | `compose/checkpoint.go` | ✅ 已修复 — save 失败时将错误包装到返回值中传播 |

## 🟡 低优先级 — 边界情况 / 性能

| # | 问题 | 文件 | 状态 |
|---|------|------|------|
| 13 | nil input 传给 `agent.Run()` 未校验 | `agent/agent.go` | ✅ 已修复 — `Run()` 入口处 nil 守卫 |
| 14 | `resolveNext` 只返回第一条匹配边 | `compose/graph.go` | ✅ 已修复 — `Build()` 时检测同源节点条件边+静态边共存或多条静态边，编译期报错 |
| 15 | 所有组合 Agent 每次 Run 都重新编译 Graph | `compose/sequential.go`, `compose/loop.go` | ✅ 已修复 — `SequentialAgent` 和 `LoopAgent` 使用 `sync.Once` 缓存编译结果（`HandoffAgent`/`RoutingAgent`/`ReflectAgent`/`PlanAgent` 闭包捕获运行时 input，无法缓存） |
| 16 | `resolveNext` 线性扫描全部边 | `compose/graph.go` | ✅ 已修复 — `Build()` 时构建 `map[NodeID][]edge` 邻接表，`resolveNext` O(1) 查找 |
| 17 | `GetToolResultID` 未防 nil message | `schema/message.go` | ✅ 已修复 — 添加 nil 守卫 |
| 18 | `DefaultPlanParser` 按字节索引处理 Unicode | `compose/plan.go` | ✅ 已修复 — 改为 `[]rune` 遍历 |
| 19 | Session history 无限增长 | `agent/react.go`, `agent/options.go`, `compose/compact.go` | ✅ 已修复 — 新增 `HistoryCompactor` 函数类型 + `WithMaxHistoryPairs(n)` / `WithHistoryCompactor(fn)` 选项；消息超限时优先调用 `HistoryCompactor`（如 `compose.NewAutoCompactor` 提供的 LLM 摘要），失败回退到简单截断 |
| 20 | SSE 解析不处理 `\r\n` 分隔符 | `model/openai/client.go` | ✅ 已修复 — 同时识别 `\n\n` 和 `\r\n\r\n` |

---

## 修复统计

- **已修复**: 20/20（#1-#9, #11-#20）
- **评估为设计合理**: 1（#10 — 锁模型与顺序执行语义一致）

## 修改文件清单

| 文件 | 修复编号 |
|------|----------|
| `model/openai/client.go` | #1, #2, #20 |
| `agent/react.go` | #3, #4 |
| `compose/loop.go` | #5 |
| `agent/options.go` | #6, #7 |
| `schema/stream.go` | #8 |
| `hook/hooks.go` | #9 |
| `compose/helpers.go` | #11 |
| `compose/fanout.go` | #11 |
| `compose/checkpoint.go` | #12 |
| `agent/agent.go` | #13 |
| `schema/message.go` | #17 |
| `compose/plan.go` | #18 |
| `compose/graph.go` | #14, #16 |
| `compose/sequential.go` | #15 |
| `compose/loop.go` | #15 |
| `agent/react.go`, `agent/options.go` | #19 |
| `compose/compact.go` | #19 |
