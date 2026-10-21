# 多模型适配 + 运行时参数控制

> 业内五方对照与判定见 [`doc/description/three-way-comparison/runtime-model-params.md`](../description/three-way-comparison/runtime-model-params.md)。本篇讲 crew-go 落地 API 与用法。

## 一、核心原则：接口是唯一扩展点

全系统（编排层 / agent）只依赖 `model.ChatModel` 接口（`Generate` + `Stream` + `WithTools`）。一个 provider = 任何满足该接口的值。**加/删 provider 对核心 `model` 包与编排层零影响**——没有中心登记表 / switch 要动，影响面只在构造点。这是"模型列表频繁增删"低成本的根。

适配分两档：

| 档 | 适用 | 做法 | 成本 |
|---|---|---|---|
| **Tier A：OpenAI 兼容** | 国产模型主力（qwen/deepseek/GLM/Kimi/豆包/MiniMax/Ollama/Azure/Grok…）| 复用同一个 `openai.Client`，只换 `Preset` 数据 | 加/删一个 = 一个 Preset 数据 |
| **Tier B：原生 wire** | Claude 原生 `/v1/messages`、Gemini 原生 `generateContent`（当前**未做**，留缝）| 独立包各自原生实现 `model.ChatModel` | 一个新适配包 |

> 业务 agent 以国产模型为主，国产模型基本都是 OpenAI 兼容 → 现状聚焦 Tier A。Tier B 是接口留好的将来路径，用到再写，不投机实现。

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

## 五、相关 commit

| commit | 内容 |
|---|---|
| `728d8f9` | Tier A：`openai.Preset` + `NewCompatible`，qwen/deepseek 改写为 preset-based |
| `b911da4` | per-request `model.WithExtraBody` + `deepseek.WithThinkingOption`/`qwen.WithEnableThinkingOption` |
| `6e4bd09` | `Message.ReasoningContent` 一等字段 + openai 解析/多轮回传 |
| `2869754` | `agent_v2.WithRunModelOptions`/`WithResumeModelOptions` 透传 + 流式 reasoning 聚合 |
| `9f7e6fc` | `EstimateMessageTokens` 计入 ReasoningContent（审计 follow-up） |
