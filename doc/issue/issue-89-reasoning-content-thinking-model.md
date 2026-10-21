# ISSUE-89 · thinking model `reasoning_content` 字段多轮回传缺失

**优先级**：MED（影响所有支持 thinking 模式的 DeepSeek / Qwen3 等模型多轮对话）
**状态**：✅ **长期方案落地**（2026-07-03）—— first-class `Message.ReasoningContent`
（`modules/common` 加字段 + openai client 解析/多轮回传 + 流式聚合），thinking 多轮框架
自动兜住、业务无需自处理；原临时方案（默认关 thinking）保留为默认档，按次开用
`WithThinkingOption`。deepseek + qwen 真机验证。commit `6e4bd09` / `2869754` / `9f7e6fc`
**触发**：本 session 切 `model/deepseek` 后跑 `TestAdv_01_BasicAgent` 真实调用,
第 0 轮 LLM 调用成功 (1.5s) + 工具触发,第 2 轮 LLM 调用返:
```
HTTP 400: {"error":{"message":"The `reasoning_content` in the thinking mode
must be passed back to the API.","type":"invalid_request_error"}}
```

## 现象

`model/openai` client (基础适配器,DeepSeek / Qwen 共用) 在 `Generate` /
`Stream` 返回的 `*schema.Message` 不存 `reasoning_content` 字段; 下一轮把
assistant message 回传给 API 时也不附 reasoning_content。DeepSeek thinking 模式
模型 (含 `deepseek-v4-flash` / `deepseek-reasoner`) **要求多轮交互时把上一轮
assistant 的 reasoning_content 原样回传**——服务端用于继续推理上下文。

## 影响范围 (按 wire 协议明示需求)

| 模型 | reasoning_content 多轮回传 |
|---|---|
| DeepSeek `deepseek-v4-flash` / `-pro` / `-reasoner` | ✅ 强制 (wire 明示要求) |
| Qwen3 thinking 模型 | ✅ 同款 DeepSeek 协议 |
| OpenAI `o1` / `o1-mini` / `o3` | ❌ **不暴露** reasoning_content, server-side managed |
| Anthropic Claude extended thinking | ✅ 但走 Anthropic SDK 不走 OpenAI compat |
| 非 thinking 模型 (GPT-4o / Sonnet 3.5 / DeepSeek V3 chat) | ❌ 不存在该字段 |

## 三方对照 (eino vs LangGraph)

| 框架 | reasoning_content 处理 | 设计 |
|---|---|---|
| **eino** | ✅ first-class `Message.ReasoningContent` (`eino/schema/message.go:683`) + ConcatMessages/String 集成 (lines 1036-1038, 1857-1949) | base struct 加字段, 全 model adapter 共用 |
| **LangGraph** | ❌ 无 first-class, 用 `additional_kwargs` 动态扩展 (langchain BaseMessage) | 灵活但易出错 |
| **crew-go** (本 ISSUE 前) | ❌ silent 丢弃 | — |

## 根因

1. **`model/openai` client** 不解析 / 不存 `reasoning_content` 字段
2. **`schema.Message`** 没有 `ReasoningContent` 字段对应位
3. **`modules/common/Message`** 也没 `ReasoningContent` (spec 层缺)
4. 多轮 LLM 调用回传 assistant message 时 framework 只装 `content` + `tool_calls`,
   缺 `reasoning_content`

## 🩹 临时方案 (本 session 落地, 2026-05-17)

**修法**: `model/deepseek.NewClient` 默认通过 `openai.WithExtraBody` 注入
`thinking: {"type": "disabled"}` 顶层 wire 字段, 关 DeepSeek thinking 模式
→ response 不返 reasoning_content → 多轮无回传需求 → 绕过 400.

**改动**:
1. `model/openai/client.go` 加 `WithExtraBody(map[string]any)` 通用 option +
   `Client.extraBody` 字段 + `newHTTPRequest` 注入点 + `mergeExtraFields` helper
   (通用机制, 任何 OpenAI compat provider 都能扩展)
2. `model/deepseek/client.go` `NewClient` 默认 prepend `WithThinking(false)` +
   暴露 `WithThinking(true)` helper 让业务真要 thinking 时显式启用 (业务自负多
   轮回传)

**验证**: `TestAdv_01_BasicAgent` 多轮场景 ed4792e 测试时 1.5s 后 400 → 本 fix
后 PASS (5.77s 含真实 LLM 多轮 + 工具调用 + 退款流程聚合).

**业务用法**:
```go
// 默认 (thinking disabled, 多轮安全)
m := deepseek.NewClient(apiKey, "")

// 真要 thinking (业务自处理多轮回传, 见长期方案)
m := deepseek.NewClient(apiKey, "", deepseek.WithThinking(true))
```

## 🎯 长期方案 (✅ 已落地 2026-07-03)

以下步骤已落地 (commit `6e4bd09` first-class 字段+解析回传 / `2869754` agent 透传+流式
聚合 / `9f7e6fc` token 估算);first-class `Message.ReasoningContent` 设计。**注:step 4 实现时
优化——默认保留关 thinking (非原草案的翻 enabled),改经 per-request 按次开,更贴业务页面
开关 + 兼容 forced tool_choice**:

1. `modules/common/Message` 加 `ReasoningContent string \`json:"reasoning_content,omitempty"\`` 字段
   (CLAUDE.md "modules/ 有差异以我们的设计为准并注释说明原因" 允许动)
2. `schema/message.go` type alias 不变 (自动继承)
3. `model/openai` client `Generate` / `Stream` 路径:
   - 解析响应 `choices[0].message.reasoning_content` → 存到 `Message.ReasoningContent`
   - 拼请求 `messages` 数组时若 assistant message 有 ReasoningContent → 附到 wire payload
4. `model/deepseek/client.go` **保留默认关 thinking** (原草案是翻 enabled, 实现优化为
   默认关 + `WithThinkingOption(true)` per-request 按次开); 开 thinking 时 reasoning_content
   自动多轮 round-trip, 业务无需自处理 (framework 兜住)
5. sanity-revert 测试: `model/openai/reasoning_test.go::TestReasoningContent_PassedBackInRequest`
   mock server + 构造带 reasoning_content 的多轮历史, 断言第 2 轮请求 wire payload 含
   reasoning_content (revert convertMessages 回传即 fail)
6. 多轮 thinking 场景集成测试 PASS

## 相关代码

- `model/openai/client.go` — base OpenAI adapter (本 session 加 WithExtraBody)
- `model/deepseek/client.go` — DeepSeek 适配 (本 session 加 WithThinking + 默认关)
- `schema/message.go` — Message type alias (来自 modules/common)
- `modules/common/message.go` — spec Message struct (含 Content / ContentBlocks 但
  无 ReasoningContent — 长期方案需加)
- `agent_v2/basic_agent.go` ReAct 多轮回传 — 拼 LLM input 时调 ChatModel.WithTools

## 重审时机

- 测试覆盖加 thinking model 多轮场景必修 (业务要用 thinking 推理时)
- DeepSeek / Qwen3 thinking 模型在生产路径常见 → 修优先级 MED → 上线前评估
  thinking-mode 真业务诉求强度决定是否实施长期方案
- 临时关 thinking 不影响 framework 路径正确性, 仅模型推理能力差异 (DeepSeek
  普通 chat 仍可用 tool_call + 多轮 + HITL 全套)
