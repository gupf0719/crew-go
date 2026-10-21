# Code Review Round 1

> ⚠️ **历史 Code Review 记录**：v1 时代审查结果。Stage 7 后 v1 (`agent/` + `compose/`) 已删，文中 file:line 引用是 v1 路径。审查发现的设计问题 / bug 类别在 v2 中已等价覆盖。


日期：2026-04-09  
范围：全量代码（agent / compose / schema / tool / hook）  
结果：3 Critical、4 Major、4 Minor 全部修复，测试全绿。

---

## Critical

### C1 — `schema/stream.go` · CloseSend 并发 panic

**问题**  
`Close()` 用了 `sync.Once` 保护 `close(done)`，但 `CloseSend()` 直接 `close(ch)` 没有任何保护。两个 goroutine 同时调用 `CloseSend`，或 `Close` 与 `CloseSend` 交叉调用，都会触发 `close of closed channel` panic。

**修法**  
给 `StreamReader` 新增独立的 `sendOnce sync.Once`（与 consumer 侧的 `recvOnce` 分开），`CloseSend` 通过它保证幂等。同时把原来 `closed bool + mu Mutex` 的冗余状态移除，逻辑更清晰。

```go
// 修前
func CloseSend[T any](sr *StreamReader[T]) {
    sr.mu.Lock()
    defer sr.mu.Unlock()
    if !sr.closed {
        sr.closed = true
        close(sr.ch)   // 并发调用仍可 panic
    }
}

// 修后
func CloseSend[T any](sr *StreamReader[T]) {
    sr.sendOnce.Do(func() { close(sr.ch) })
}
```

---

### C2 — `compose/fanout.go` · cloneStateForFanOut 锁外读共享字段

**问题**  
`cloneStateForFanOut` 在 `state.mu.RUnlock()` 之后才读取 `state.Messages`（第 143 行）和 `state.LastOutput`（第 148 行）。`state.mu` 只保护 `state.data`，但 fan-out 是在并发环境下调用的——若主 goroutine 同时修改这两个字段，就产生 data race，属于未定义行为。

**修法**  
将三个字段的读取全部移进 `RLock` 范围内，一次性完成拷贝。

```go
// 修前
state.mu.RLock()
// 只拷贝 data
state.mu.RUnlock()
msgsCopy := make(...) // ← 锁外读 Messages
copy(msgsCopy, state.Messages)
return &GraphState{..., LastOutput: state.LastOutput} // ← 锁外读 LastOutput

// 修后
state.mu.RLock()
dataCopy   := ...
msgsCopy   := make([]*schema.Message, len(state.Messages))
copy(msgsCopy, state.Messages)
lastOutput := state.LastOutput   // 全部在锁内
state.mu.RUnlock()
```

---

### C3 — `agent/runpath.go` · 裸类型断言

**问题**  
`GetRunPath` 用 `v.([]RunStep)` 裸断言。`runPathKey{}` 虽是私有类型，正常情况下不会被外部写入，但任何未来的重构或测试伪造都会导致 panic 而非返回 nil。

**修法**  
改为 comma-ok 单行写法，断言失败时返回 nil 而非 panic。

```go
// 修前
return v.([]RunStep)

// 修后
v, _ := ctx.Value(runPathKey{}).([]RunStep)
return v
```

---

## Major

### M1 — `compose/{plan,loop,handoff,reflect}.go` · checkpoint 恢复后类型断言静默失败

**问题**  
这几个文件把运行时控制变量（`plan_steps []string`、`plan_step_idx int`、`loop_round int`、`hop_count int` 等）存入 `GraphState.data`（`map[string]any`）。

checkpoint 恢复时，`unmarshalGraphState` 用 `json.Unmarshal` 把 JSON 写回 `state.data`，JSON 默认类型映射是：
- `number` → `float64`（不是 `int`）
- `array` → `[]interface{}`（不是 `[]string`）

之后的裸断言 `v.(int)` 和 `v.([]string)` 全部静默失败，返回零值：
- 循环计数器归零 → 死循环或错误的轮次判断
- 步骤列表为 nil → plan 执行完全跳空

**修法**  
在 `compose/helpers.go` 新增两个辅助函数，自动处理 JSON 类型兼容：

```go
// stateInt: int / float64 / int64 均可转换
func stateInt(state *GraphState, key string) int

// stateStringSlice: []string 和 []interface{} 均可转换
func stateStringSlice(state *GraphState, key string) []string
```

全部调用点替换为这两个函数，不再有裸断言。

---

### M2 — `compose/parallel.go` · 默认错误策略与并行语义相反

**问题**  
`NewParallelAgent` 默认策略是 `FailFast`：任意一个分支失败，整个并行操作立即报错。但并行操作的通常预期是"允许部分失败、聚合所有结果"，`FailFast` 是最严苛的策略，极易让用户在没有显式配置的情况下踩坑。

**修法**  
默认改为 `ContinueOnError`。需要严格行为的调用方显式传 `WithErrorPolicy(FailFast)`。同步更新测试，把依赖旧默认值的 `TestParallelAgent_Error` 改为显式指定策略。

```go
// 修前
policy: FailFast,

// 修后
policy: ContinueOnError,  // 失败分支记录在 merged output，不中止整体
```

---

### M3 — `compose/reflect.go` · reflectNode 读 `state.LastOutput.Content` 无 nil guard

**问题**  
`reflectNode` 构造 reflector 输入时直接引用 `state.LastOutput.Content`。若 `evaluator` 为 nil，`evalNode` 直接返回而不更新 `LastOutput`；若此时 actor 输出恰好也未写入 `LastOutput`（异常路径），就会 nil pointer dereference panic。

**修法**  
先把 `evalContent` 从 `state.LastOutput` 安全提取，后续全部用 `evalContent` 字符串。同时修复 `else` 分支（无 reflector 时）也依赖 `state.LastOutput` 的问题：改为 `schema.AssistantMessage(evalContent)` 构造 feedback。

```go
// 修前
reflIn := fmt.Sprintf("...Evaluation:\n%s\n...", state.LastOutput.Content)

// 修后
evalContent := ""
if state.LastOutput != nil {
    evalContent = state.LastOutput.Content
}
reflIn := fmt.Sprintf("...Evaluation:\n%s\n...", evalContent)
```

---

### M4 — `compose/routing.go` · lookup 每次 O(n) 全表扫描

**问题**  
`lookup` 先做一次精确匹配，失败后对整个 `routes` map 逐条 `strings.ToLower` 比较。路由表条数固定，但每次请求都重复遍历，路由表较大时影响 P99 延迟。

**修法**  
`NewRoutingAgent` 构造时一次性建好 `routesLower map[string]agent.Agent`，`lookup` 退化为两次 O(1) map 查找。

```go
// 构造时
lower := make(map[string]agent.Agent, len(routes))
for k, a := range routes {
    lower[strings.ToLower(k)] = a
}
r.routesLower = lower

// lookup 时
func (r *RoutingAgent) lookup(key string) (agent.Agent, bool) {
    if a, ok := r.routes[key]; ok { return a, true }
    a, ok := r.routesLower[strings.ToLower(key)]
    return a, ok
}
```

---

## Minor 修复

以下 4 个 Minor 问题已在同一轮完成修复：

| # | 位置 | 描述 | 状态 |
|---|---|---|---|
| m1 | `compose/helpers.go` | 新增 `HasParallelError` / `ParallelErrors` 结构化错误查询函数；导出 `ParallelResultsKey` / `FanOutResultsKey` 常量；DefaultMerger 注释补充说明 | ✅ 已修复 |
| m2 | `compose/plan.go:135` | 循环内字符串拼接改 `strings.Builder` | ✅ 已修复 |
| m3 | 各 compose 文件 | 内部 state key 统一改为 `__namespace.field__` 格式命名常量（loop / plan / handoff / reflect / fanout），降低外部意外覆盖风险 | ✅ 已修复 |
| m4 | `compose/fanout.go:48` | `AddFanOutEdge` 新增 `from ∈ targets` 自循环检查，冲突时返回 build error | ✅ 已修复 |

---

## 变更文件汇总

| 文件 | 变更类型 |
|---|---|
| `schema/stream.go` | C1：CloseSend 改用 sendOnce，移除冗余字段 |
| `compose/fanout.go` | C2：cloneStateForFanOut 锁内读全部共享字段 |
| `agent/runpath.go` | C3：GetRunPath 改 comma-ok 断言 |
| `compose/helpers.go` | M1：新增 stateInt / stateStringSlice |
| `compose/loop.go` | M1：替换裸断言 |
| `compose/plan.go` | M1：替换裸断言 |
| `compose/handoff.go` | M1：替换裸断言 |
| `compose/reflect.go` | M1 + M3：替换裸断言 + nil guard |
| `compose/parallel.go` | M2：默认策略改为 ContinueOnError |
| `compose/routing.go` | M4：构造时建 routesLower 索引 |
| `compose/compose_test.go` | 同步更新 TestParallelAgent_Error（显式 FailFast） |
