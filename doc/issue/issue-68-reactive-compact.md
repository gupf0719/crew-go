---
id: ISSUE-68
title: ReactiveCompact 被动压缩（LLM 报 prompt-too-long 后兜底重试）
status: ✅ 已实现
priority: 中
created: 2026-05-03
depends: ISSUE-65
---

# ISSUE-68 · ReactiveCompact 被动压缩

## 背景

第 3 层压缩——AutoCompact 没拦住的最后兜底。当 LLM 调用返回 prompt-too-long 错误
时，被动启动压缩并重试调用，最多 N 次后报错给 caller。

参见设计文档：[doc/v2/context-compaction.md](../v2/context-compaction.md#第-3-层reactivecompact-被动压缩)

## 设计

### 触发时机

- `agent/react.go::callModelStreaming` LLM 调用返回错误
- 错误经 `isPromptTooLongError` 检测为上下文超限
- 配置 `ReactiveCompactConfig.Enabled == true`（默认 true）
- 当前重试次数 < `MaxAttempts`（默认 3）

### 错误检测

各 provider 的 prompt-too-long 错误格式不同。我们用字符串匹配 + 已知关键词列表：

```go
// agent/reactive_compact.go
var promptTooLongMarkers = []string{
    "prompt too long",
    "context length",
    "context window",
    "too many tokens",
    "exceeds maximum",
    "maximum context",
    "input is too long",
    "request too large",
    // 各 provider 字符串持续维护
}

func isPromptTooLongError(err error) bool {
    if err == nil {
        return false
    }
    msg := strings.ToLower(err.Error())
    for _, m := range promptTooLongMarkers {
        if strings.Contains(msg, m) {
            return true
        }
    }
    return false
}
```

### 算法

```
1. LLM 调用失败 → isPromptTooLongError 检测
2. 是 → 触发 ReactiveCompactRun(messages, attempt)
   a. 优先策略（PreferAutoCompact && AutoCompact enabled）：
      跑一次 AutoCompact 摘要压缩
      → 失败回退「删最旧消息组」抢救
   b. 兜底策略：
      groupMessagesByApiRound 分组
      解析错误 token gap（部分 provider 返回"超出 N tokens"信息）
        有 gap：从最旧组累加 token 删到达 gap
        无 gap：删最旧 EmergencyDropPercent (默认 20) % 组
      保证至少剩 1 组
      若删后首条是 assistant，前面补 user marker
3. 用压缩后 messages 重试 LLM 调用
4. 仍失败 → 步骤 2 再压一次（attempt++）
5. attempt >= MaxAttempts → 报错给 caller
```

### 「删最旧消息组」抢救实现

```go
// agent/reactive_compact.go
func emergencyDropOldestGroups(messages []*Message, dropPercent int, gap int, marker string) []*Message {
    groups := compose.GroupMessagesByAPIRound(messages)
    if len(groups) <= 1 {
        return messages  // 不能再删
    }

    var dropCount int
    if gap > 0 {
        // 解析出了精确 gap：从最旧累加 token 直到达到 gap
        var acc int
        for _, g := range groups {
            acc += compose.EstimateMessageTokens(g)
            dropCount++
            if acc >= gap {
                break
            }
        }
    } else {
        // 无 gap：删最旧 dropPercent %
        dropCount = max(1, len(groups)*dropPercent/100)
    }

    // 保证至少剩 1 组
    if dropCount >= len(groups) {
        dropCount = len(groups) - 1
    }

    kept := flattenGroups(groups[dropCount:])

    // 删后首条若是 assistant，补 user marker（API 要求 user 开头）
    if len(kept) > 0 && kept[0].Role == schema.RoleAssistant {
        userMarker := schema.UserMessage(marker)
        return append([]*schema.Message{userMarker}, kept...)
    }
    return kept
}
```

### token gap 解析（best-effort）

```go
// 部分 provider 错误消息含"超出 N tokens"信息
// 例：OpenAI: "This model's maximum context length is 8192 tokens. However, your messages resulted in 9123 tokens. ..."
// 例：Claude: "Input is too long: 234156 input tokens (max 200000)"
//
// 用 regex 尽力解析 gap = current - max
func parsePromptTooLongGap(err error) int {
    if err == nil {
        return 0
    }
    msg := err.Error()

    // OpenAI 格式
    re := regexp.MustCompile(`(\d+)\s+tokens\.\s+However,\s+your\s+messages\s+resulted\s+in\s+(\d+)`)
    if m := re.FindStringSubmatch(msg); len(m) == 3 {
        max, _ := strconv.Atoi(m[1])
        cur, _ := strconv.Atoi(m[2])
        return cur - max
    }

    // Claude 格式
    re2 := regexp.MustCompile(`(\d+)\s+input\s+tokens\s+\(max\s+(\d+)\)`)
    if m := re2.FindStringSubmatch(msg); len(m) == 3 {
        cur, _ := strconv.Atoi(m[1])
        max, _ := strconv.Atoi(m[2])
        return cur - max
    }

    return 0  // 无法解析，调用方退回 dropPercent 兜底
}
```

### 配置

```go
// agent/reactive_compact.go
type ReactiveCompactConfig struct {
    Enabled              bool   // 默认 true
    MaxAttempts          int    // 默认 3
    PreferAutoCompact    bool   // 默认 true（如果 AutoCompact enabled，先用它摘要）
    EmergencyDropPercent int    // 默认 20
    UserMarker           string // 默认 "[continued after context overflow]"
}

// 通过 BasicAgent 选项注入
agent := NewBasicAgent(
    WithModel(m),
    WithReactiveCompact(&ReactiveCompactConfig{
        MaxAttempts: 3,
    }),
)
```

### 集成点

`agent/react.go::callModelStreaming` 错误路径：

```go
// 当前
stream, err := chatModel.Stream(ctx, messages)
if err != nil {
    return chatModel.Generate(ctx, messages)  // 降级
}

// 改为
for attempt := 0; attempt < cfg.ReactiveCompact.MaxAttempts; attempt++ {
    stream, err := chatModel.Stream(ctx, messages)
    if err == nil {
        // 成功路径
        ...
        return aggregated, nil
    }
    if !isPromptTooLongError(err) {
        // 其它错走原降级
        return chatModel.Generate(ctx, messages)
    }
    // 是 prompt-too-long → 压缩重试
    state.Send(reactiveCompactTriggerEvent(attempt, err))
    messages, err = ReactiveCompactRun(ctx, cfg.ReactiveCompact, messages, err)
    if err != nil {
        return nil, fmt.Errorf("reactive compact failed: %w", err)
    }
}
return nil, fmt.Errorf("reactive compact: max %d attempts exhausted", cfg.ReactiveCompact.MaxAttempts)
```

### 可观测性

```go
state.Send(NewOutputEvent("compaction:reactive:trigger", runPath, ...))
state.Send(NewOutputEvent("compaction:reactive:emergency_drop", runPath, ...))
state.Send(NewOutputEvent("compaction:reactive:end", runPath, ...))
```

## 测试覆盖

`agent/reactive_compact_test.go`：

1. **错误检测**：表驱动各 provider 已知错误字符串 → isPromptTooLongError 正确返回
2. **gap 解析**：OpenAI / Claude 格式错误消息正确解析 gap
3. **gap 解析失败**：未知格式 → gap=0，调用方走 dropPercent 兜底
4. **抢救路径**：mock LLM 报 prompt-too-long → emergencyDropOldestGroups 删最旧 20%
   → 重试成功
5. **首条 assistant 补 marker**：删后第一条是 assistant → 前补 user marker
6. **MaxAttempts 兜底**：连续 3 次 LLM 报错 → 第 4 次不再重试，报错给 caller
7. **AutoCompact 优先**：PreferAutoCompact=true 且 AutoCompact 启用 → 优先用 AutoCompact 摘要
8. **配对完整性**：删消息组用 GroupMessagesByAPIRound 保证 tool_use/tool_result 配对

## 验收

- `ReactiveCompactRun` + 8 个测试全绿
- 集成到 callModelStreaming
- `go test ./... -count=1 -race` 全绿
- 真实场景：构造超长 prompt 触发 → 自动压缩重试成功

## 不做的

- **provider-specific error 类型识别**：用字符串匹配（耐用、跨 provider 一致）
  而非 type assertion——provider 错误类型变化时仍能 work
- **持久化重试历史**：仅 in-process 重试计数，跨调用不记忆——压缩失败通常是单次
  调用上下文特征，不需要跨调用记忆
