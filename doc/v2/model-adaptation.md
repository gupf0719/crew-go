# 多模型适配 + 运行时参数控制

> 业内五方对照与判定见 [`doc/description/three-way-comparison/runtime-model-params.md`](../description/three-way-comparison/runtime-model-params.md)。本篇讲 crew-go 落地 API 与用法。

## 一、核心原则：接口是唯一扩展点

全系统（编排层 / agent）只依赖 `model.ChatModel` 接口（`Generate` + `Stream` + `WithTools`）。一个 provider = 任何满足该接口的值。**加/删 provider 对核心 `model` 包与编排层零影响**——没有中心登记表 / switch 要动，影响面只在构造点。这是"模型列表频繁增删"低成本的根。

适配分两档：

| 档 | 适用 | 做法 | 成本 |
|---|---|---|---|
| **Tier A：OpenAI 兼容** | 国产模型主力（qwen/deepseek/GLM/Kimi/豆包/MiniMax/Ollama/Azure/Grok…）| 复用同一个 `openai.Client`，只换 `Preset` 数据 | 加/删一个 = 一个 Preset 数据 |
| **Tier B：原生 wire** | Claude 原生 Messages API（`model/anthropic`，见第五节）；Gemini 原生 `generateContent`（**未做**，留缝）| 独立包各自原生实现 `model.ChatModel` | 一个新适配包 |

> 业务 agent 以国产模型为主，国产模型基本都是 OpenAI 兼容 → 主力在 Tier A。Claude 的 thinking 签名、服务端回退等没有 OpenAI 兼容形态，所以走 Tier B（2026-10-21 加的 `model/anthropic`）；Gemini 用到再写，不投机实现。

## 二、Tier A：Preset + NewCompatible

`model/openai/compatible.go`：

```go
// Preset 声明一个 OpenAI 兼容 provider 的默认配置（纯数据）。
type Preset struct {
    System       string         // provider 身份（gen_ai.system 观测标识）: "qwen"/"deepseek"/"zhipu"
    BaseURL      string         // OpenAI 兼容 endpoint
    DefaultModel string         // modelName 留空时的默认
    ExtraBody    map[string]any // 默认 wire 扩展字段（顶层 merge）；业务 opts 后值覆盖同 key
    // 结构化输出能力画像；nil = 保守默认 model.NonStrictToolCapability（非官方 OpenAI 多不支持 strict）
    StructuredCapability *model.StructuredCapability
    // true = 该 provider 的流式响应可能不带 finish_reason，流正常结束就算输出完整；
    // 默认 false：流结束时没收到 finish_reason 判为截断（读出错、错误帧照样报错）
    OptionalFinishReason bool
}

// NewCompatible 用 Preset 构造。modelName 留空用 DefaultModel；opts 追加在 Preset 默认后
// （同类 ClientOption 后值覆盖）。返回 model.ChatModel。
func NewCompatible(p Preset, apiKey, modelName string, opts ...ClientOption) model.ChatModel
```

**加一个国产模型**（业务侧内联，或放独立薄包）：

```go
m := openai.NewCompatible(openai.Preset{
    System: "zhipu", BaseURL: "https://open.bigmodel.cn/api/paas/v4", DefaultModel: "glm-4.6",
}, apiKey, "")
```

**删一个** = 删掉那个 Preset 值 / 那个薄包 + grep 引用点（叶子包，只有业务代码引用，编排层无感）。

**qwen / deepseek 是这套机制的用例**：`qwen.NewClient`/`NewTestClient`、`deepseek.NewClient` 公开签名不变，内部走 `NewCompatible(preset, ...)`——thinking 默认关等 wire 怪癖落在各自 Preset 的 `ExtraBody`。

## 三、运行时参数控制（thinking/reasoning 按次开关）

### 3.1 三层 API

| 层 | API | 作用 |
|---|---|---|
| **model 通用逃生舱** | `model.WithExtraBody(map[string]any) model.Option` | per-request 顶层 wire 字段，叠在构造期默认上（同 key per-request 赢、不同 key 保留） |
| **provider 强类型 helper** | `deepseek.WithThinkingOption(bool) model.Option`<br>`qwen.WithEnableThinkingOption(bool) model.Option` | 各 provider 的 thinking 开关（内部包 `model.WithExtraBody`，wire 各不同） |
| **agent per-Run 透传** | `agent_v2.WithRunModelOptions(...model.Option) RunOption`<br>`agent_v2.WithResumeModelOptions(...model.Option) ResumeOption` | 把 model.Option 串到本次 Run/Resume 的每次主推理 model 调用（含 ReactiveCompact retry）。例外：`WithToolChoice(required)` 只管到模型第一次调工具为止、之后复位（强制是为了让模型先去调工具；调过之后还强制，模型就永远不能以文本收尾、一路烧到轮次预算的最后一轮）；「调过工具」按模型发起过调用算、不看结果（失败、被拒的也算）；只有第一批全是框架的 tool_search、且解锁出新工具的不算（它只是把别的工具解锁出来，开了渐进披露时第一轮往往只能调它，强制要留给解锁出来的那件）；只豁免这一批，什么都没解锁的、之后再检索的照算（不然检索不到、或工具库比轮次预算大时，模型一路检索烧到最后一轮），这一轮没有工具可绑时也摘掉 |

### 3.2 业务用法（页面开关）

```go
// 页面开关按次切 thinking，单 client、无需重建
events, err := ag.Run(ctx, sess, input,
    agent_v2.WithRunModelOptions(deepseek.WithThinkingOption(pageToggle)))

// HITL 续跑要沿用同款按次参数，Resume 显式重传（对齐 WithRunModel 的 Run/Resume 对称）
events, err = ag.Resume(ctx, sess.RunID,
    agent_v2.WithResumeUserInputs(answers),
    agent_v2.WithResumeModelOptions(deepseek.WithThinkingOption(pageToggle)))
```

### 3.3 语义与约束

- **默认关 thinking**，`WithRunModelOptions` 按次开。理由：用户要的是按次开关而非默认开；关更快/省，且兼容框架强制 tool_choice 的路径（`LLMPlanner`——dashscope qwen3 thinking 与 forced tool_choice 互斥会 400）。
- **思考模式下 `tool_choice: none` 不在禁列**：BasicAgent 的软降落轮保留工具、发 none（见 §5.4），开着 thinking 也会发。DeepSeek 与百炼 Qwen 的官方文档在思考模式下只禁 `required` 与指定函数（百炼的报错是 `The tool_choice parameter does not support being set to required or object in thinking mode`），none 两家都写明可用。**未用真 key 验证**（2026-10-22 只查了文档）。
- **覆盖语义**：per-request 覆盖构造期默认，同 key per-request 赢、不同 key 保留（`model/openai` buildRequest 合并 `c.extraBody` ⊕ `o.ExtraBody`）。
- **仅 BasicAgent 生效**：composite 编排（Sequential/Parallel/Loop/…）收到 per-call 覆盖 fail-fast 返 `agent_v2.ErrCompositeOverride`（含 model-options）——业务应在构造期设或在具体 BasicAgent 上用。
- **不泄漏进嵌套子 agent**：`withRunOverrides` 总是 push（即使空），父 BasicAgent 的 per-Run 参数不会经 AgentAsTool 泄漏给子 BasicAgent（子只用自己的 Run/ResumeOption）。
- **不影响** AutoCompact 摘要 / skill fork 的 model 调用（那些不该带 thinking）。

## 四、ReasoningContent：thinking 模型多轮回传（ISSUE-89 长期方案）

thinking 模型（DeepSeek deepseek-reasoner / Qwen3 thinking 等）的 wire 协议要求**多轮把上一轮 assistant 的 `reasoning_content` 原样回传**，服务端用于延续推理上下文——不回传则第 2 轮 400（`"The reasoning_content in the thinking mode must be passed back to the API."`）。

**框架兜住，业务无需自己处理**：

- `schema.Message.ReasoningContent string`（一等字段，spec 无、crew-go 为此新增；`modules/common/Message`，`schema.Message` 经 type alias 继承）。
- `model/openai` client：`convertResponse` 解析响应 `reasoning_content` → `Message.ReasoningContent`；`convertMessages` 拼下一轮请求时 assistant 消息附回；流式 delta per-chunk emit，`streamOrGenerate` 聚合成 final message 的 ReasoningContent。
- `EstimateMessageTokens` 计入 ReasoningContent（多轮回传上 wire，不计入会让 AutoCompact 低估）。

因此业务开 thinking 跑多轮 ReAct（流式 / 非流式都可）不会 400——reasoning_content 自动 round-trip。

> **已验证范围**：真 deepseek 端到端——非流式多轮（手工构造历史）+ BasicAgent 流式多轮 tool_call（`WithRunModelOptions` 开 thinking，"7 的平方是 49" 无 400）。3+ 轮 / 中途关 thinking 的历史带旧 reasoning 场景尚未真机覆盖，属已知待验证边界。

## 五、Tier B：Claude 原生（`model/anthropic`）

`anthropic.NewClient(apiKey, modelName, opts...)`，底层是官方 Go SDK（`github.com/anthropics/anthropic-sdk-go`）的 Beta Messages API。默认模型 `claude-opus-5-5`、`max_tokens` 64000；SDK 自己的重试与 OpenTelemetry span 默认关掉（重试交给 crew-go 的重试层，模型调用的观测由 hook 层出），要改用 `WithRequestOptions` 传 SDK 选项。

### 5.1 默认请求与选项

| 默认 | 关 / 改 |
|---|---|
| adaptive thinking + `block_binding.prefix_mismatch_behavior: "drop_block"`（beta `thinking-binding-controls-2026-08-01`）| `WithPrefixMismatch(PrefixMismatchError / PrefixMismatchUnset)`、`WithoutThinkingConfig()`（给不接受 adaptive 的旧模型）|
| 顶层 `cache_control: {type: "ephemeral"}`（自动 prompt cache）| `WithPromptCaching(false)`、`WithCacheTTL(CacheTTL1h)` |
| `fallbacks: "default"`（beta `server-side-fallback-2026-07-01`，被安全分类器拒绝时由 Anthropic 指定的回退模型接着答）| `WithFallbacks(false)`（Bedrock / Vertex / Foundry 等不支持的端点要关）|
| effort、thinking display 用 API 默认 | `WithEffort` / 按次 `WithEffortOption`；`WithThinkingDisplay(DisplaySummarized / DisplayUpdates)` |

`WithExtraBody`（构造期）与 `model.WithExtraBody`（按次）按 **JSON merge patch（RFC 7386）** 并进请求体：对象逐键递归合并、null 删除键——跟 `model/openai` 的顶层整键覆盖不同，`{"output_config": {"effort": "high"}}` 不会冲掉 `output_config.format`。

### 5.2 assistant 消息原样回放

thinking 块带签名，签名绑定它产生时的前缀（`system`、`tools`、之前的每条消息）；回放时 API 重算比对，对不上的块按 `prefix_mismatch_behavior` 丢掉或让请求 400。`schema.Message` 把文本拼成一个 `Content`、工具调用单列，从它重建 assistant 消息会改掉块的顺序与边界（Opus 5.5 的进度说明 thinking 块就夹在工具调用之间）。所以：

- 返回的 assistant 消息带标注 `anthropic.assistant_content`：API 返回的 content 按请求形状原样存下（字符串，存档 JSON 往返一字不差），连同 `Content` 与工具调用的指纹。
- 下一轮转换时指纹对得上就原样回放；对不上（文本或工具调用被钩子 / 业务改过）时按快照原来的块序合并：thinking 等块原位保留；文本没改就原样保留文本块，改了就在第一个文本块的位置放新的 `Content`；工具调用按 ID 原位替换、去掉的删除、新增的放最后。排在第一处改动之前的 thinking 块前缀不变、照样有效；排在它之后的（比如两个工具调用之间的进度说明）前缀变了，按 `prefix_mismatch_behavior` 处理。
- 快照里 `tool_use.input` 用响应原文：SDK 把块转成请求形状时会把 input 解成 `any`，超过 2^53 的整数（雪花 ID、纳秒时间戳）与高精度小数会变成 float64 近似值——值变了既是改了前缀，也让模型看到的参数跟实际执行的不一样。
- `max_tokens` 截在工具调用中间（结束原因 `max_tokens` 且最后一块是 `tool_use`）：这个调用带着解析不了的参数交出（流式是收到的原文，非流式是去掉末尾 `}` 的 input），框架按「参数非法」拒掉、以失败结果交回模型，不会拿残缺的参数执行（SDK 累积流式响应时会把截断的参数换成 `{}`，照它交出工具就会拿空参数跑）。同一条响应里更早的、完整的工具调用照常执行。
- 服务端回退在中途切换模型时，切换点之前被拒模型的 thinking 与工具调用作废：不回放、工具调用不交出（所以流式把工具调用压到最后一片才交），文本照常保留；切换经过记在标注 `anthropic.fallback`（`anthropic.Fallbacks(msg)` 读）。
- API 对输入的改动（丢掉的 thinking 块等）按（种类, 原因）归并计数，记在标注 `anthropic.input_transformations`（`anthropic.InputTransformations(msg)` 读），每条响应打一条告警。不逐块记：历史改过之后，后面每次请求都会把同一批块再丢一遍，逐块记会让会话标注按轮数的平方增长。

### 5.3 映射规则

- 开头连续的 system 消息 → 顶层 `system`；其余 system 消息 → user 文本，包在 `<system-reminder>` 里（原生的 mid-conversation `role: "system"` 有位置约束，crew-go 插入的位置不保证满足；写法只取决于消息本身，不随后面追加的内容变）。
- 相邻同角色消息合并（tool 与 user 都映射成 user）；user 消息里 `tool_result` 排最前；`ToolresultContentBlock.IsError` → `is_error: true`（工具调用失败时置上：框架合成的失败结果与业务工具返回 `tool.Failure` 都算，OnToolEnd 改写结论时同步；审批拒绝不算失败、不置）。
- `tool_choice`：`required` 与 `auto` 都不发（Opus 5.5 对强制选工具返回 400），`none` 原样发；`StructuredCapability` 报 `SupportsToolChoiceRequired=false`、`SupportsJSONSchema=true`（`output_config.format`）、`SupportsJSONObject=false`（`json_object` 直接报错）。工具按名字排序，`ToolInfo.Extra["strict"] == true` 的发 `strict: true`。
- 结束原因：`refusal` → `*anthropic.RefusalError`（包 `model.ErrContentFiltered`，不可重试）；`model_context_window_exceeded` → 包 `model.ErrContextWindowExceeded`；`max_tokens` 按完整处理（同 openai 的 `length`）。
- 用量：`InputTokens` 含 cache 读写两部分，`CachedInputTokens` = cache 读，`CacheCreationInputTokens` = cache 写，`OutputTokens` 不含 thinking，`ThinkTokens` = `output_tokens_details.thinking_tokens`。服务端回退时只计接着答的那次尝试（被拒的那次在 `usage.iterations` 里，不汇总）。
- 采样参数（`model.WithTemperature` / `WithTopP`）原样转发：Opus 4.7 起的 Opus 模型只要带上就返回 400，Fable 5.1 / Haiku 5.5 只接受默认值；确定性失败、不重试。

### 5.4 跟 crew-go 其他功能的冲突

下列功能会改写更早的消息、`system` 或工具集，之后的 thinking 块对不上前缀。默认 `drop_block` 下请求照常，但 API 丢的是第一个对不上的块**以及它之后的每一个 thinking 块**（直到下一个 compaction 块），并且之后每次请求都再丢一遍——改动之后这场会话基本没有回放的推理可用，模型可能多想补回来；`PrefixMismatchError` 下之后每次请求都 400。同时 prompt cache 从改动处失效：

- MicroCompact / AutoCompact / ReactiveCompact（改写或删掉旧消息）、`MaxHistoryPairs`（**默认 20，滚动截断**：跨 Run 的历史超过 40 条消息后，每次 Run 开始都从开头截掉超出部分，窗口随之前移；开了会话历史时工具往返也算条数）。
- 工具集变化：`WithToolSearch` 揭示工具、`WithToolFilter`、`model.WithAllowedTools`、**配了 `WithOutputSchema` 时的软降落轮**（轮次预算最后一轮只留 `final_output`；保住前缀得用 tool_choice 指定只能调它，而 Opus 5.5 对指定工具返回 400）。没配结构化输出时软降落轮不改工具集：工具照旧，只发 `tool_choice: none`（Anthropic 建议的做法——tool_choice 不在前缀里，prompt cache 也只有 messages 段因它失效），那一轮模型硬调的工具不执行（记成失败的工具结果）。真 API 实测（2026-10-22，`claude-haiku-5-5`、effort high、`prefix_mismatch_behavior: "error"`）：同一段带 thinking 的历史，最后一轮工具照旧 + none 成功；摘掉工具、或只改一个工具的描述，都返回 400（`Invalid signature in thinking block … The tools list differs from the one this block was created with`）。BasicAgent 端到端（`WithMaxRounds(2)`）三次请求的 tools 逐字节相同，第三次发 none 并回放了 thinking，没有 400，也没有 input_transformations。
- 每次 Run 变化的开头 system 消息：`WithStoreMemory` 每次 Run 检索一次，结果作为开头的 system 消息进顶层 `system`。
- Handoff 的 Raw 历史（`HistoryForRaw`）把别的 agent 的 assistant 消息连同回放快照原样交给接手的 agent：那些 thinking 是在另一套 `system` / tools 下产生的，必然对不上。

`agent_v2.NewLLMPlanner` 固定发 `tool_choice=required`、不看 `StructuredCapability`：本适配器把它降为不指定，模型回自由文本时 planner 报 `ErrPlannerNoToolCall`。

另外，`EstimateMessageTokens` 看不到回放快照里的 thinking，而 preserved thinking 计入输入 token——按估算触发的压缩会偏晚。

### 5.5 测试

单测（`model/anthropic/*_test.go`）用 httptest 桩断言请求体；真 API 冒烟 `example/model/anthropic_smoke`（`//go:build integration`，需 `ANTHROPIC_API_KEY`），全部用例都在 `prefix_mismatch_behavior: "error"` 下跑：BasicAgent 跨两次 Run 的多轮工具调用、中途 system 消息、prompt cache 命中、并行工具结果合并。

## 六、相关 commit

| commit | 内容 |
|---|---|
| `728d8f9` | Tier A：`openai.Preset` + `NewCompatible`，qwen/deepseek 改写为 preset-based |
| `b911da4` | per-request `model.WithExtraBody` + `deepseek.WithThinkingOption`/`qwen.WithEnableThinkingOption` |
| `6e4bd09` | `Message.ReasoningContent` 一等字段 + openai 解析/多轮回传 |
| `2869754` | `agent_v2.WithRunModelOptions`/`WithResumeModelOptions` 透传 + 流式 reasoning 聚合 |
| `9f7e6fc` | `EstimateMessageTokens` 计入 ReasoningContent（审计 follow-up） |
