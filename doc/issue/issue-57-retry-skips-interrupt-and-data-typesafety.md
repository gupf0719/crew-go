---
id: ISSUE-57
title: retry 路径对 InterruptError 盲重试 + InterruptError.Data 跨 Resume 类型保真
status: ✅ 已实现
priority: 高
created: 2026-04-28
resolved: 2026-04-28
source: ISSUE-56 实施 plan_chat_web HITL 时审计 retryRunAgentNodeMsg 发现 + InterruptError godoc 调研
---

# ISSUE-57 · retry 不识别 InterruptError + InterruptError.Data 类型保真

## 两个独立但相关的 bug

ISSUE-56 实施 plan_chat_web HITL 时审计 retry / Resume 路径时发现的两个潜在踩坑：

1. **`compose/helpers.go::retryRunAgentNode{,Msg}` 不识别 `*agent.InterruptError`** —— 用户配
   `WithPlanMaxRetries(N>0)` 时 hook 抛 InterruptError 会盲重试 N 次才往上抛。语义上正确（最终
   还是把 wrap 后的 InterruptError 透出），但浪费 N 轮 LLM 调用 + 用户体感"挂起按钮卡了几秒"
2. **`agent.InterruptError.Data` 跨 Resume 类型保真度** —— 同进程 errors.As 拿到原始 struct 工作
   正常，但 JSON Save → Load 后 Data 退化成 `map[string]any` / `string` / `float64` 等基础类型。
   用户塞自定义 struct 进去，Resume 后 type assertion 必失败，**只有翻 issue / 实际调试才发现**

## Bug 1 修复

### 现状

```go
for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
    ...
    msg, err := runAgentNodeMsg(ctx, ...)
    if err == nil { return msg, nil }
    lastErr = err   // ← 拿到 InterruptError 也只是 lastErr，下轮继续 retry
}
```

`MaxRetries=3` 时：hook 第一次抛 InterruptError → retry 触发等待+重试 → hook 又抛 →
retry → ... 浪费 4 次调用 + (1+2+4) × 退避秒数延迟，才把 InterruptError 通过
`fmt.Errorf("after %d retries: %w", lastErr)` 透出。

### 修法

抽 `isRetryable(err)` helper，retry 主循环跳过控制流错误：

```go
func isRetryable(err error) bool {
    if err == nil { return false }
    var ie *agent.InterruptError
    if errors.As(err, &ie) { return false }
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) { return false }
    return true
}
```

`retryRunAgentNode` / `retryRunAgentNodeMsg` 都加 check：

```go
msg, err := runAgentNodeMsg(ctx, ...)
if err == nil { return msg, nil }
if !isRetryable(err) { return nil, err }   // ← 新增
lastErr = err
```

### 不重试的错误清单

- **`*agent.InterruptError`** —— 控制流信号，重试只是浪费
- **`context.Canceled` / `context.DeadlineExceeded`** —— ctx 已死，重试间隙 sleep 立即返回
  本来也快，但显式拦截让"runAgentNode 内部返 ctx.Err()"路径不再多走一遍 sleep

### 回归测试

`compose/options_coverage_test.go` 三条：
- `TestRetry_NotRetryable_InterruptError`: 配 `MaxRetries=5`，验证只调 1 次（不重试）
- `TestRetry_NotRetryable_CtxCanceled`: 已 cancel ctx 配 `MaxRetries=5`，验证只调 1 次
- `TestRetry_RetryableErrorDoesRetry`: 反证 sanity check，普通 error 仍按 cfg 重试 4 次

## Bug 2 修复

### 现状

```go
type InterruptError struct {
    Reason string
    Data   any  // ← 跨 Resume 后类型信息丢失，但 godoc 没说
}
```

参考 ISSUE-40 对 `AgentAction.CustomizedAction` 的同款踩坑，是设计上的 known limitation，
但**用户必须翻 ISSUE-40 / ISSUE-56 文档才知道**——实际开发中很容易塞个 struct 进去然后
Resume 后 type assertion 失败只能现场调试。

### 修法（参考 ISSUE-51 #6 的设计原则：godoc 警告 + Save 时 warn）

**1. `agent/errors.go` 加完整 godoc 段落**：

```go
// # Data 跨 Resume 类型保真度（ISSUE-57）
//
// 同进程运行时直接透传——hook 抛 InterruptError 后 runtime 用 errors.As 拿到
// 的就是原始结构。但跨 Checkpoint Resume 时类型信息会丢失：JSON 反序列化只能
// 还原成 map[string]any / string / float64 / bool / []any 等基础类型，原 struct
// 类型 type assertion 必失败。
//
// 推荐用法：
//   - 运行时控制信号 + 同进程读 → 随便塞，type assertion 安全
//   - 跨 Resume 仍要 typed signal → 用 map[string]any 自描述（key 区分语义），
//     或把信号升格为 InterruptError 的专属字段（参考 ISSUE-40 同款建议）
```

**2. `agent.MaybeWarnInterruptDataSerialization(ctx, ie)` 新公开函数**：

参考 `MaybeWarnCheckpointSerialization` 套路。Save 前扫 `ie.Data` 类型，**递归**检查
是否直接或间接包含 struct：

| Data 类型 | 跨 Resume 类型保真？ | warn? |
|---|---|---|
| nil / 基础类型（int/string/bool/float） | ✅ 保真 | ❌ |
| `map[string]any{}` / `[]any{}` / `map[string]string{}` / `[]int{}` | ✅ 保真 | ❌ |
| nil pointer | ✅ 防御静默 | ❌ |
| `MyStruct{}` / `&MyStruct{}` | ❌ 丢 | ✅ |
| `[]MyStruct{}` / `[N]MyStruct{}` / `[]*MyStruct{}` | ❌ 丢 | ✅ |
| `map[K]MyStruct{}` / `map[K]*MyStruct{}` | ❌ 丢 | ✅ |
| `[][]MyStruct{}`（任意嵌套深度） | ❌ 丢 | ✅ |

实现递归 helper `containsStructy(t reflect.Type) bool`：
- `Struct` → true
- `Pointer / Slice / Array` → `containsStructy(t.Elem())`
- `Map` → `containsStructy(t.Elem())`（key 通常 string，对 JSON 保真无影响）
- 其它 → false

```go
v := reflect.ValueOf(ie.Data)
if v.Kind() == reflect.Pointer && v.IsNil() { return }  // 防御
if !containsStructy(v.Type()) { return }
slog.WarnContext(ctx, "InterruptError.Data contains struct; type info lost across Resume",
    "type", fmt.Sprintf("%T", ie.Data),
    "reason", ie.Reason,
    "hint", "use map[string]any with explicit keys, or promote the signal to a typed InterruptError field",
)
```

**3. `compose/checkpoint.go` 在两个 InterruptError save 路径调用**：

- `saveCheckpointOnError`（单节点 Interrupt 入口）：识别 InterruptError 后调用 warn helper
- `buildBranchDelta`（superstep 并发分支 Interrupt 入口）：同上

### 回归测试

`agent/memory_errors_test.go` 加 `TestMaybeWarnInterruptDataSerialization` 表驱动 16 子用例：

**静默 case（8 个）**：
- nil data
- `map[string]any{"k": "v", "n": 1}`
- string / int 基础类型
- `[]any{"a", 1}` / `[]int{1, 2, 3}` / `map[string]string{...}`
- `(*myStruct)(nil)` —— nil pointer 防 panic

**警告 case（8 个）**：
- `myStruct{...}` / `&myStruct{...}` —— 直接 struct + pointer
- `[]myStruct{...}` / `[N]myStruct{...}` —— slice/array of struct
- `[]*myStruct{...}` —— slice of pointer-to-struct
- `map[string]myStruct{...}` / `map[string]*myStruct{...}` —— map value 是 struct
- `[][]myStruct{...}` —— 多层嵌套 collection 也覆盖

外加 `TestMaybeWarnInterruptDataSerialization_NilInterruptError` 验证 ie==nil 不 panic。

## 设计原则提炼

延续 ISSUE-51 #6 / ISSUE-55 的精神：

> **类型层面无法保证的契约，必须在使用前给用户主动警告**——别让用户在 Resume 后才发现。

`Data any` / `CustomizedAction any` / 其他用 `any` 暴露给用户的字段，框架都该提供
`MaybeWarnXxx` 系列扫描函数，在持久化路径主动警告 struct/pointer 用法。文档 + warn 双
保险，不留隐性踩坑。

## 改动范围

| 文件 | 改动 |
|---|---|
| `compose/helpers.go` | 抽 `isRetryable`；`retryRunAgentNode{,Msg}` 加跳过控制流错误的 check |
| `compose/options_coverage_test.go` | 新增 3 条 retry 行为测试 |
| `agent/errors.go` | `InterruptError` godoc 增补 "Data 跨 Resume 类型保真度" 完整段落 |
| `agent/event.go` | 新增 `MaybeWarnInterruptDataSerialization(ctx, ie)` |
| `agent/memory_errors_test.go` | 新增 8 子用例表驱动 + nil 防御测试 |
| `compose/checkpoint.go` | `saveCheckpointOnError` / `buildBranchDelta` 两路 InterruptError save 加 warn 调用 |

## 测试

`agent / compose / hook / mcp / skill / example/orchestration/plan_chat` 全过。

## 参考

- ISSUE-40 · `AgentAction.CustomizedAction` 持久化丢类型（同款 `any` 字段问题）
- ISSUE-51 #6 · `MaybeWarnCheckpointSerialization` 同款 warn 设计
- ISSUE-56 · plan_chat_web HITL 集成（本 issue 触发点）
- [compose/helpers.go](../../compose/helpers.go) — retry 路径
- [agent/errors.go](../../agent/errors.go) — InterruptError 类型 + godoc
- [agent/event.go](../../agent/event.go) — MaybeWarnInterruptDataSerialization
