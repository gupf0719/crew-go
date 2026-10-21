# crew-go vs agentdock-spec / eino 设计对比

> ⚠️ **历史 Code Review 记录**：v1 时代审查结果。Stage 7 后 v1 (`agent/` + `compose/`) 已删，文中 file:line 引用是 v1 路径。审查发现的设计问题 / bug 类别在 v2 中已等价覆盖。


> 更新时间：2026-04-13  
> 对比来源：agentdock-spec `.tsp` 文件 + `go-runtime/` Go 类型；eino `components/tool/interface.go`

---

## 我们优于 spec 的设计（不对齐，保持现状）

| 维度 | spec | 我们 | 原因 |
|---|---|---|---|
| `ChatModel` | `Chat(messages, stream bool) MessageVariant` 单方法 | `Generate()` + `Stream()` 分方法 | bool flag 是反模式，两条路径返回类型完全不同；分方法编译期类型安全 |
| `Tool` 接口 | 9 方法，含 `Name(ctx)(string,error)` | 4 方法，`Name()` 返回 string | 工具名是静态元数据，每次调用都可能失败的设计不合理；9 方法实现成本过高 |
| `StreamReader[T]` | `MessageStream`（双向）+ `MessageStreamReader`（单向），`recv()` 返回单值 | 泛型 `StreamReader[T]`，`Recv()` 返回 `(T, bool)`，含 `Done()` channel | `(T, bool)` 与 Go channel 惯例一致；`Done()` 让生产者感知消费者退出，避免 goroutine 泄漏；spec 无 EOF 信号 |
| `Tool.Call` 结果 | `output: string` + 平铺 `error/errorCode` | `output: any` + 嵌套 `Error{Code, Message}` | `string` 限制结构化返回；嵌套 Error 更符合 Go 习惯 |
| `Hook` 方法签名 | 无 ctx，返回 void | 全部带 ctx，返回 error | 无 ctx 写不了 trace-aware 日志；无 error 返回无法做鉴权拦截/熔断 |
| `Memory.Read` | 返回 `string[]` | 返回 `[]*schema.Message` | `[]*Message` 保留 role/timestamp，可直接注入对话历史，无需二次解析（PR 3.3 后我们改名 `store.Search`，spec 对照点不变） |
| `ParallelResult` | `Aggregator` 接收 `map[string]string`（有损，无序） | `Merger` 接收 `[]ParallelResult`（含 Err/RunPath，有序） | map 无序且丢失错误信息；slice 保证声明顺序，支持 `ContinueOnError` |

---

## eino 有、我们不需要的设计

| 设计 | eino 用途 | 我们不需要的原因 |
|---|---|---|
| `controlEdge` | 强类型节点间排序但不传数据 | 我们用 `GraphState` 共享状态，不存在节点间类型不兼容问题 |
| `InvokableTool` / `StreamableTool` 接口拆分 | 区分同步/流式工具能力，框架内部 type switch 派发 | 暂无流式工具需求；若将来需要，参考此设计新增可选 `StreamableTool` 接口（见 ISSUE-16） |

---

## spec 有道理但我们做了取舍的设计

| 维度 | spec | 我们 | 取舍说明 |
|---|---|---|---|
| `Checkpoint.metadata` | 强制 `userId` 字段（多租户保障） | 自由 `map[string]any` | 单机场景不需要 userId 约束；多租户时调用方自行约定 key |
| `Tool` 的 `ToolType` 字段 | 区分内置/外部工具来源 | 无此字段 | 我们没有内置工具概念；来源标记可在 Toolkit 注册时附加 tag，不需要改接口 |
| `Agent.Run` 的 `opts []any` | 运行时覆盖选项 | 无此参数 | `[]any` 类型不安全；目前需求可通过构造时 options 满足，不需要运行时覆盖 |

---

## 两边基本对齐的设计

- `Router` 返回 `[]string` 支持 fan-out（ISSUE-05 已修复；fan-out `ParallelErrorPolicy` + cancel 已对齐 `ParallelAgentNode`）
- `Checkpointer` 接口方法集（8 个方法，基本一致）；`Save` 强制校验 `user_id`（对齐 spec `CheckpointMetadata.userId` 必填）
- `AgentAction` 控制流信号结构
- `RoutingChoice{Name, Description}` 类型
