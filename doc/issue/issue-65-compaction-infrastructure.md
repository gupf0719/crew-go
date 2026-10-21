---
id: ISSUE-65
title: 上下文压缩基础设施（token 估算 / 消息分组 / 配对感知切割 / 断路器）
status: ✅ 已实现
priority: 高
created: 2026-05-03
---

# ISSUE-65 · 上下文压缩基础设施

## 背景

ISSUE-66/67/68 的 4 层上下文压缩共用一组基础能力。先把这层做扎实再上各层
压缩，避免每层各自实现一遍踩同样的坑（典型：Bug 1 tool_call 配对在
truncateHistory + AutoCompact 两处都踩同一坑）。

设计参考 Claude Code 4 层压缩的实施细节（参见
[doc/v2/context-compaction.md](../v2/context-compaction.md)）。

## 范围

新增 4 个 helper，全部位于 `compose/` 包：

### 1. Token 精细估算（`compose/token_estimate.go`）

```go
// EstimateMessageTokens 估算消息序列的 token 数（按 block 类型精细计算）。
func EstimateMessageTokens(messages []*schema.Message) int

// EstimateTokensFromAPIUsage 优先返回最近一轮 LLM Usage.InputTokens。
// 调用方：先试 EstimateTokensFromAPIUsage，nil 时回退 EstimateMessageTokens。
func EstimateTokensFromAPIUsage(messages []*schema.Message) (int, bool)
```

**估算规则（per block）**：
- `text`：len/4
- `tool_use`：len(name + json(args))/4
- `tool_result`：字符串 len/4；数组逐项；image/document 固定 2000
- `thinking`：len(content)/4（不算 signature 元数据）
- 兜底：len(json(block))/4

**最后 ×4/3 保守填充**：API 实际 token 通常比字符估算多 ~33%，宁多估不少估
（少估会触发 prompt-too-long）。

### 2. 消息分组（`compose/message_group.go`）

```go
// GroupMessagesByAPIRound 按 message.id（如可拿到）或 role 边界分组。
//
// 同一 API 请求的所有消息（含流式分块、tool_use、tool_result）应在同一组——
// 删消息时按组删保 thinking + tool 配对完整性。
func GroupMessagesByAPIRound(messages []*schema.Message) [][]*schema.Message
```

**分组规则**：
- 当出现新的 assistant message.id（或 RunID 标记）时，开始新一组
- 同 message.id 的 assistant 块（streaming chunk / tool_use / thinking）在同组
- 该 assistant 后的 tool_result 也在同组
- 下一个 user 消息开始新一组

**注意**：我们的 schema 没有原生 `message.id` 字段。设计选择：
- 用 `Annotations["run_id"]` 作分组 key（ISSUE-49 / ISSUE-63 已用 RunID 标识 LLM 调用）
- 同 run_id 的 assistant + 后续 tool_result 在同组
- run_id 缺失的消息按 role 边界分组（user / assistant / tool）

### 3. 配对感知切割（`compose/safe_truncate.go`）

```go
// AdjustIndexToPreserveAPIInvariants 调整 start 切点保 tool_call / tool_result
// 配对 + thinking 块完整性。
//
// 输入：messages 完整列表，start 是 caller 想要切割的索引（保留 [start:end]）
// 返回：调整后的 start 索引（>= 原 start，可能向前扩展以维持 API 不变量）
//
// 算法：
//   步骤 1：修复 tool_use / tool_result 配对
//     1a. 收集 [start, end] 内所有 tool_result 的 tool_use_id
//     1b. 收集 [start, end] 内已有的 tool_use_id
//     1c. 找出缺失的 tool_use_id（在 start 之前）
//     1d. 向前扩展 start，包含所有缺失 tool_use 的 assistant 消息
//
//   步骤 2：修复 thinking 块分离
//     2a. 收集 [start, end] 内所有 assistant 消息的 message id
//     2b. 向前扩展 start，包含同 id 的更早 assistant 消息
func AdjustIndexToPreserveAPIInvariants(messages []*schema.Message, start int) int
```

**Bug 修复**：替代当前 `agent/compact.go::truncateHistory` 和 `AutoCompact` 的
裸 slice 索引——它们都不管配对，长对话 + tool 频繁场景必触发 OpenAI 422。

### 4. 断路器（`compose/circuit_breaker.go`）

```go
// CircuitBreaker 通用断路器：连续失败 N 次后跳过，成功重置。
//
// 用法（AutoCompact / ReactiveCompact 共享）：
//   if !cb.Allow() { return /* skip */ }
//   err := doExpensiveWork()
//   if err != nil { cb.RecordFailure() } else { cb.RecordSuccess() }
type CircuitBreaker struct {
    MaxConsecutiveFailures int
    // 内部状态省略
}

func NewCircuitBreaker(maxFailures int) *CircuitBreaker
func (cb *CircuitBreaker) Allow() bool
func (cb *CircuitBreaker) RecordFailure()
func (cb *CircuitBreaker) RecordSuccess()
func (cb *CircuitBreaker) Tripped() bool  // 仅观测，不影响 Allow
```

**线程安全**：内部 mutex 保护——AutoCompact / ReactiveCompact 在 fan-out 分支
内可能并发触发。

## 测试覆盖

每个 helper 独立单测：

- `token_estimate_test.go`：覆盖各 block 类型 + ×4/3 填充 + APIUsage 优先级
- `message_group_test.go`：覆盖 run_id 分组 + role 边界兜底 + tool_result 跟随
- `safe_truncate_test.go`：覆盖
  - 切口落在 tool_result 上（向前扩展到 tool_use）
  - 切口落在 assistant tool_call 中间（向前扩展到 message id 起点）
  - 切口已经干净（不调整）
  - 多个 tool_use_id 缺失（一次性向前扩展到全包含）
- `circuit_breaker_test.go`：覆盖 trip / reset / 并发安全

## 不做的

- **token 估算 tokenizer**：不接 tiktoken / sentencepiece，用字符估算。tokenizer
  per-provider 一套维护成本高，估算够用（×4/3 兜底）+ APIUsage 真实数字优先。
- **断路器持久化**：仅 in-memory。跨 Run / 进程重启重置——足够，因为压缩失败
  通常是单会话特征（特定对话内容触发），不需要跨会话记忆。

## 验收

- 4 个 helper 全部实现 + 单测全绿
- `go test ./compose/ -count=1 -race` 不引入 race
- 现有调用点（agent/compact.go::truncateHistory / AutoCompact）暂时**不改**——
  ISSUE-66/67 实施时切到新 helper

## 后续

ISSUE-66/67/68 的实施依赖此 issue 完成。
