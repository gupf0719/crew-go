---
id: ISSUE-67
title: AutoCompact 自动压缩（LLM 摘要 + 附件重注入 + 断路器）
status: ✅ 已实现
priority: 高
created: 2026-05-03
depends: ISSUE-65
---

# ISSUE-67 · AutoCompact 自动压缩

## 背景

第 2 层压缩——主动触发 LLM 摘要早期对话，输出 9 段结构化 summary 替代原始消息。
是 4 层中最复杂的一层，也是承担主要 token 控制工作的核心层。

参见设计文档：[doc/v2/context-compaction.md](../v2/context-compaction.md#第-2-层autocompact-自动压缩)

## 设计

### 触发时机

- BasicAgent 内部 model 节点入口（`agent/basicagent_graph.go::modelNodeFunc`）
- 配置 `AutoCompactConfig.Enabled == true`（默认 false——需显式启用 + 配 Model）
- 当前估算 token >= 阈值
- 断路器未触发

### 阈值计算（对标 Claude Code）

```go
const (
    DefaultSummaryMaxTokens     = 20_000  // p99.99 摘要输出
    DefaultSummaryBufferTokens  = 13_000  // 触发距硬上限的安全缓冲
)

// EffectiveContextWindow = ModelContextWindow - SummaryMaxTokens
// AutoCompactThreshold   = EffectiveContextWindow - SummaryBufferTokens
//
// 例（200K 上下文）：200_000 - 20_000 - 13_000 = 167_000 tokens 触发
func (cfg *AutoCompactConfig) Threshold() int {
    return cfg.ModelContextWindow - cfg.SummaryMaxTokens - cfg.SummaryBufferTokens
}
```

### 算法（两次 LLM 调用）

**Call 1：生成摘要**

```go
input := []*Message{
    cfg.SystemPrompt,                         // 不动
    ...messages[:lastAssistantBeforeTrigger], // 完整原始对话
    UserMessage(buildExtractionPrompt(cfg)),  // NO_TOOLS_PREAMBLE + 9 段要求
}
output, err := cfg.Model.Generate(ctx, input)
// output: <analysis>...</analysis><summary>...</summary>
```

**后处理 `formatCompactSummary`**：

```go
// 1. 删 <analysis> 块
re1 := regexp.MustCompile(`(?s)<analysis>.*?</analysis>`)
out := re1.ReplaceAllString(output.Content, "")
// 2. 提取 <summary> 内容
re2 := regexp.MustCompile(`(?s)<summary>(.*?)</summary>`)
match := re2.FindStringSubmatch(out)
summary := strings.TrimSpace(match[1])
```

**Call 2：用 summary 替代原始历史**

`messages` 重组：

```go
newMessages := []*Message{
    cfg.SystemPrompt,                                   // 不动
    UserMessage("# Previous Conversation Summary\n\n" + summary),  // 摘要
    ...reinjectedFiles,                                 // 最近 5 个文件，每个 ≤5K，总 ≤50K
    ...messages[lastBoundary:],                          // 触发后保留的最近消息（如有）
}
```

之后正常进入 model 节点跑用户新消息——本轮的 `chatModel.Stream` / `Generate` 用
新 `messages`。

### 关键设计点

#### 1. NO_TOOLS_PREAMBLE 双放

提示词**前后各放一次**——Claude Code 实测 Sonnet 4.6+ 会忽略弱尾部指令尝试调用
工具，导致 maxTurns:1 时无文本输出回退到流式备用路径。

```go
const NoToolsPreamble = `CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.

- Do NOT use Read, Bash, Grep, Glob, Edit, Write, or ANY other tool.
- You already have all the context you need in the conversation above.
- Tool calls will be REJECTED and will waste your only turn — you will fail the task.
- Your entire response must be plain text: an <analysis> block followed by a <summary> block.`

const NoToolsTrailer = `REMINDER: Do NOT call any tools. Respond with plain text only — an <analysis> block followed by a <summary> block. Tool calls will be rejected and you will fail the task.`

func buildExtractionPrompt(cfg *AutoCompactConfig) string {
    var sb strings.Builder
    sb.WriteString(NoToolsPreamble)
    sb.WriteString("\n\n")
    sb.WriteString(MainExtractionPromptCN)  // 中文 9 段要求
    if cfg.CustomInstructions != "" {
        sb.WriteString("\n\n## Compact Instructions\n")
        sb.WriteString(cfg.CustomInstructions)
    }
    sb.WriteString("\n\n")
    sb.WriteString(NoToolsTrailer)
    return sb.String()
}
```

#### 2. `<analysis>` chain-of-thought + 后处理删除

要求 LLM 先 `<analysis>` 思考再 `<summary>` 输出——提升摘要质量。事后删除
`<analysis>` 块只保留 `<summary>`，避免污染压缩后上下文。

#### 3. 9 段结构化输出

固定 9 个 section（标题保英文，便于程序化解析）：

1. Primary Request and Intent
2. Key Technical Concepts
3. Files and Code Sections
4. Errors and fixes
5. Problem Solving
6. All user messages
7. Pending Tasks
8. Current Work
9. Optional Next Step

#### 4. 附件重注入

压缩后摘要丢失了文件具体内容，下一轮 LLM 需要引用近期读过的文件时无可用数据。
解决：从原对话扫出最近读过的 N 个文件（通过 Read tool 的 tool_result），每个
≤5K tokens，总预算 50K，重新作为 user message 注入 Call 2 的 input：

```go
type reinjectedFile struct {
    Path    string
    Content string
}

func extractRecentFiles(messages []*Message, maxFiles, perFileBudget, totalBudget int) []reinjectedFile {
    // 倒序扫描 tool_result（Read tool）
    // 收集最近 maxFiles 个不重复 path
    // 单文件超 perFileBudget 截断
    // 总 token 超 totalBudget 停止
}

func buildReinjectionMessage(files []reinjectedFile) *Message {
    var sb strings.Builder
    sb.WriteString("# Recently Read Files (re-injected after compaction)\n\n")
    for _, f := range files {
        sb.WriteString(fmt.Sprintf("## %s\n```\n%s\n```\n\n", f.Path, f.Content))
    }
    return UserMessage(sb.String())
}
```

#### 5. 断路器

```go
if !cfg.breaker.Allow() {
    return messages, nil  // 跳过压缩
}
err := runAutoCompact(...)
if err != nil {
    cfg.breaker.RecordFailure()
    return messages, fmt.Errorf("auto compact: %w", err)
}
cfg.breaker.RecordSuccess()
```

### 配置

```go
// agent/autocompact.go
type AutoCompactConfig struct {
    Enabled              bool             // 默认 false
    Model                model.ChatModel  // 摘要用 LLM（推荐用更便宜 model）
    ModelContextWindow   int              // 必填，model context window 大小
    SummaryMaxTokens     int              // 默认 20_000
    SummaryBufferTokens  int              // 默认 13_000
    MaxConsecutiveFailures int            // 默认 3
    CustomInstructions   string           // 附加摘要指令

    ReinjectRecentFiles  bool             // 默认 true
    ReinjectMaxFiles     int              // 默认 5
    ReinjectPerFileBudget int             // 默认 5_000 tokens
    ReinjectTotalBudget  int              // 默认 50_000 tokens

    SystemPromptOverride string           // 摘要 Call 1 的 system prompt 自定义（默认从原 messages 取）

    breaker *compose.CircuitBreaker
}

// 通过 BasicAgent 选项注入
agent := NewBasicAgent(
    WithModel(mainModel),
    WithAutoCompact(&AutoCompactConfig{
        Enabled:            true,
        Model:              cheapModel,        // 推荐 GPT-4o-mini / Qwen-Turbo 等
        ModelContextWindow: 200_000,
        CustomInstructions: "重点关注代码变更和测试结果",
    }),
)
```

### 集成点

`agent/basicagent_graph.go::modelNodeFunc` 入口：

```go
// 当前
if cfg.MaxHistoryPairs > 0 {
    gs.Messages = compactHistory(ctx, cfg, gs.Messages)
}

// 改为
if cfg.AutoCompact != nil && cfg.AutoCompact.Enabled {
    var err error
    gs.Messages, err = AutoCompactRun(ctx, cfg.AutoCompact, gs.Messages)
    if err != nil {
        slog.WarnContext(ctx, "AutoCompact 失败，继续走原 messages（断路器已计数）", "err", err)
    }
}
if cfg.MaxHistoryPairs > 0 {
    gs.Messages = compactHistory(ctx, cfg, gs.Messages)  // 兜底 truncate（保留作 P0 补偿）
}
```

### 可观测性

```go
state.Send(NewOutputEvent("compaction:auto:start", runPath, ...))
// ... 摘要 ...
state.Send(NewOutputEvent("compaction:auto:summary_generated", runPath, ...))
state.Send(NewOutputEvent("compaction:auto:end", runPath, ...))
```

## 提示词模板

完整内容（中英版）保存在 `agent/autocompact_prompts.go`：

- `NoToolsPreamble` / `NoToolsTrailer`（中英对应）
- `MainExtractionPromptEN` / `MainExtractionPromptCN`
- 默认用中文版（plan_chat_web 用户群体）
- 通过 `WithAutoCompactLanguage(LangEN)` 切换

## 测试覆盖

`agent/autocompact_test.go`：

1. **基础流程**：长对话 → 触发 → Call 1 stub 返回 `<analysis>...<summary>...</summary>`
   → 后处理提取 → Call 2 input 校验（system + summary + new user）
2. **附件重注入**：原对话含 3 个 Read tool_result → 触发后摘要前注入 3 个文件
3. **附件预算限制**：10 个文件超 50K → 仅注入到预算内的最近 N 个
4. **断路器触发**：连续 3 次摘要 LLM 报错 → 第 4 次跳过不调
5. **断路器重置**：失败 2 次 → 成功 1 次 → 失败计数归 0
6. **CustomInstructions**：附加指令正确插入提示词末尾
7. **配对感知**：摘要切点用 `AdjustIndexToPreserveAPIInvariants` 保 tool 配对
8. **NoTools 双放**：提示词前后各含一次 NoToolsPreamble/Trailer

## 验收

- `AutoCompactRun` + 8 个测试全绿
- 提示词中英两版对照齐全
- 集成到 BasicAgent，`go test ./... -count=1 -race` 全绿
- plan_chat_web 真实 LLM 长对话验证：触发 + 前端可见 `compaction:auto:*` 事件

## 不做的

- **Partial compact 两方向（from/up_to）**：初版只做 from（保留旧 + 摘要新）。up_to
  方向（摘要旧 + 保留新作前导）复杂度高，看实际需求后续追加
- **Cache edits 路径**：依赖 provider cache_edits API，不在本期范围
- **Plan 文件重注入 / MCP / 技能增量**：我们没有这些机制，不实现

## 后续

- ISSUE-68 ReactiveCompact 优先复用本 Config 的 Model + 摘要逻辑
- 文件重注入算法可独立提出供其它场景用
