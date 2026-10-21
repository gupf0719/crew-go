---
id: ISSUE-66
title: MicroCompact 微压缩（清除旧 tool_result 内容，无 LLM 调用）
status: ✅ 已实现
priority: 高
created: 2026-05-03
depends: ISSUE-65
---

# ISSUE-66 · MicroCompact 微压缩

## 背景

第 1 层压缩——纯规则操作，每次 tool 节点完成后跑一次。把白名单内的旧 tool 调用
结果内容替换为占位符，保留 tool_use / tool_result 块结构（API 配对完整），但释放
大块内容占用的 tokens。

参见设计文档：[doc/v2/context-compaction.md](../v2/context-compaction.md#第-1-层microcompact-微压缩)

## 设计

### 触发时机

- BasicAgent 内部 tools 节点完成后（`agent/basicagent_graph.go::toolsNodeFunc` 出口）
- 配置 `MicroCompactConfig.Enabled == true`（默认开启）

### 算法

```
1. 遍历 messages，收集所有 tool_use 块（按出现顺序）
2. 对每个白名单内的 tool name：
   a. 找出该 tool 的所有 tool_use_id（按出现顺序）
   b. 保留最近 N 个（默认 keepRecent=5）
   c. 早于第 N 个的对应 tool_result 块的 Content 替换为占位符
   d. tool_use 块完全保留（保配对完整性）
3. 不在白名单的 tool（Agent / Skill / MCP / 用户自定义）跳过
```

### 配置

```go
// agent/microcompact.go
type MicroCompactConfig struct {
    Enabled          bool      // 默认 true
    KeepRecent       int       // 默认 5
    CompactableTools []string  // 默认 DefaultCompactableTools
    Placeholder      string    // 默认 "[Old tool result content cleared by microcompact]"
}

var DefaultCompactableTools = []string{
    "Read", "Bash", "Grep", "Glob",
    "WebSearch", "WebFetch", "Edit", "Write",
}

// 通过 BasicAgent 选项注入
agent := NewBasicAgent(
    WithModel(m),
    WithMicroCompact(&MicroCompactConfig{
        KeepRecent: 3,
        CompactableTools: append(DefaultCompactableTools, "MyCustomTool"),
    }),
)
```

### 集成点

`agent/basicagent_graph.go::toolsNodeFunc` 末段加：

```go
if cfg.MicroCompact != nil && cfg.MicroCompact.Enabled {
    state.Messages = MicroCompactRun(state.Messages, cfg.MicroCompact)
}
return state, nil
```

### 可观测性

每次触发 emit `OrchestrationHooks.OnNodeEvent`：

```go
state.Send(&AgentEvent{
    AgentName: "compaction:micro",
    Output: &AgentOutput{Message: schema.AssistantMessage(fmt.Sprintf(
        "MicroCompact: cleared %d tool results across %d tools",
        cleared, toolsAffected,
    ))},
})
```

## 实现细节

```go
// agent/microcompact.go (新)
package agent

import (
    "crew-go/schema"
)

type MicroCompactConfig struct {
    Enabled          bool
    KeepRecent       int
    CompactableTools []string
    Placeholder      string
}

var DefaultCompactableTools = []string{
    "Read", "Bash", "Grep", "Glob",
    "WebSearch", "WebFetch", "Edit", "Write",
}

const defaultMicroCompactPlaceholder = "[Old tool result content cleared by microcompact]"

// MicroCompactRun 应用 MicroCompact 算法到 messages，原地修改并返回。
//
// 不变量：
//   - tool_use 块永不修改
//   - tool_result 块的元数据（tool_use_id / role）永不修改
//   - 仅 tool_result.Content 被替换为占位符
//   - 不在 CompactableTools 白名单的 tool 完全跳过
func MicroCompactRun(messages []*schema.Message, cfg *MicroCompactConfig) []*schema.Message {
    if cfg == nil || !cfg.Enabled || len(messages) == 0 {
        return messages
    }
    keep := cfg.KeepRecent
    if keep <= 0 {
        keep = 5
    }
    placeholder := cfg.Placeholder
    if placeholder == "" {
        placeholder = defaultMicroCompactPlaceholder
    }
    whitelist := make(map[string]bool, len(cfg.CompactableTools))
    for _, t := range cfg.CompactableTools {
        whitelist[t] = true
    }
    if len(whitelist) == 0 {
        for _, t := range DefaultCompactableTools {
            whitelist[t] = true
        }
    }

    // Step 1: 收集白名单内的 tool_use_id（按 tool name 分组、按出现顺序）
    type toolUseRef struct {
        toolName  string
        callID    string
    }
    perToolIDs := map[string][]string{}  // tool_name → []tool_use_id（出现顺序）
    for _, m := range messages {
        for _, tc := range schema.GetToolCalls(m) {
            if whitelist[tc.Function.Name] {
                perToolIDs[tc.Function.Name] = append(perToolIDs[tc.Function.Name], tc.ID)
            }
        }
    }

    // Step 2: 计算「待清除」的 tool_use_id 集合（每个 tool 只保留最近 keep 个）
    toClear := map[string]bool{}
    for _, ids := range perToolIDs {
        if len(ids) <= keep {
            continue
        }
        for _, id := range ids[:len(ids)-keep] {
            toClear[id] = true
        }
    }
    if len(toClear) == 0 {
        return messages
    }

    // Step 3: 替换对应 tool_result 的 Content
    out := make([]*schema.Message, len(messages))
    for i, m := range messages {
        if m.Role == schema.RoleTool {
            id := schema.GetToolResultID(m)
            if toClear[id] {
                cloned := *m
                cloned.Content = placeholder
                out[i] = &cloned
                continue
            }
        }
        out[i] = m
    }
    return out
}
```

## 测试覆盖

`agent/microcompact_test.go`：

1. **基础**：白名单 tool 调用 N+keep+M 次，最早 N 个 tool_result 被清除，最近
   keep 个保留，tool_use 块全保留
2. **白名单外**：非白名单 tool（如 "MyAgent"）的 tool_result 不被清除
3. **配对完整**：清除后 tool_use ↔ tool_result ID 仍一一对应
4. **空配置**：Enabled=false 时不动消息
5. **未达阈值**：调用次数 ≤ keep 时不动消息
6. **多个 tool 混合**：Read 调 10 次 + Bash 调 8 次，各自独立按 keep=5 清除

## 验收

- `MicroCompactRun` + 配置 + BasicAgent 集成完成
- 6 个测试全绿
- `go test ./... -count=1 -race` 全绿
- plan_chat_web 跑长对话 tool 调用场景，前端可见 `compaction:micro` 事件

## 后续

- ISSUE-67 AutoCompact 在 model 节点入口跑，相互独立但顺序为 Micro → Auto
- 缓存编辑路径（不改本地、改 API cache_edits）暂不实现，看 provider 支持后续追加
