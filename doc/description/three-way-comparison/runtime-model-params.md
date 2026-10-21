# 运行时模型参数控制（thinking/reasoning 按次开关）五方对照

## 决策点

业务要在**运行时按次**控制 provider 专属模型参数——典型：页面上一个 thinking 开关，"这几次提问要深度推理、后面几次不要"。核心问题：这类参数（DeepSeek/Qwen3 的 thinking、OpenAI 的 reasoning_effort、Anthropic 的 extended thinking）是**构造期烧死在 client 上**，还是有 **per-request 覆盖通道**？机制是什么、控制在哪一层、参数用强类型还是逃生舱？

> 本篇是五方调研（eino / OpenAI Agents SDK / Microsoft Agent Framework / LangChain·LangGraph / CrewAI），非三方——运行时参数控制是行业普遍能力，多采样点才能判"是否真共识"。源码本地路径见文末。

## 五方对照

| 框架 | per-call 覆盖 | 机制 | 类型策略 | 覆盖语义 | 控制层 |
|---|---|---|---|---|---|
| **eino** | ✅ | per-request `model.Option` 双通道（通用 `apply` + 类型擦除 `implSpecificOptFn any`）；adk `WithChatModelOptions` 串到 `model.Generate(ctx,msgs,opts...)` | 强类型 option（`WithReasoningEffort`）+ `WithExtraFields(map)` 逃生舱 | base 用构造期 Config 填，per-call **replace** 赢 | model 层（option），agent/compose 透传 |
| **OpenAI Agents SDK** | ✅ | `ModelSettings` 对象，run 层 `ModelSettings.resolve(override)` overlay 合并 | 强类型 field（`reasoning: Reasoning`）+ `extra_body`/`extra_args` dict 逃生舱 | Agent 级 base，`RunConfig.model_settings` 覆盖，非 None 赢；`extra_args` key 级 merge、`extra_body` 整体 replace | run 层解析、model 层消费 |
| **MAF** | ✅ | per-request `ChatOptions`（TypedDict），两级 `_merge_options` | 强类型 field（TypedDict 子类 `reasoning`/`thinking`）+ `total=False` 透传 + `client_kwargs`（.NET：`AdditionalProperties` + `RawRepresentationFactory`）| 构造默认，`agent.run(options=)` 覆盖，None-skip，标量 replace | chat-client 层消费，agent/run 层注入 |
| **LangChain·LangGraph** | ✅ | `.bind(**kwargs)` / `.invoke(**kwargs)` 弱类型 kwargs；LangGraph `(state,runtime)->model` dynamic callable 每轮重选 | **弱类型 kwargs**（`thinking=`/`reasoning_effort=` 任意 kwarg）；决策侧 `runtime.context` 才强类型 | 越靠近调用越赢，per-key replace | model runnable 层（kwargs/config），LangGraph node 层决策 |
| **CrewAI** | ❌ | 无 per-call 参数覆盖，参数烧在 `LLM` 实例；仅 `stop`/`stream` 有 contextvar override | 强类型 field（`reasoning_effort`、`AnthropicThinkingConfig`）+ `additional_params` dict | 仅构造期，换参数要换 `LLM` 实例 | LLM 层（构造期） |

## 判定：哪些一致、哪些分裂

**✅ 一致（可跟的 pattern）**——4/5 支持 per-call 覆盖，且有覆盖处语义完全统一：

1. **构造期默认 + per-call/per-run 覆盖、per-call 赢、标量/provider 参数按 key 整体 replace（不深合并）**
2. **同一 model 实例复用、不重建**（eino/OpenAI SDK/MAF/langchain-bind；CrewAI 是唯一靠换实例的反例）
3. **provider 专属参数一律留 map/dict 逃生舱兜底**（连强类型的几家都留）

**❌ 分裂（不能照搬）**：

1. **类型策略**：强类型 field 为主（eino/OpenAI/MAF/CrewAI，4/5）vs 弱类型 kwargs（langchain，1/5）。多数派是**强类型 field + dict 逃生舱**。
2. **强类型那几家，reasoning field 都挂在各自 provider 的 option 命名空间**（eino `acl/openai.WithReasoningEffort`、MAF `OpenAIChatOptions.reasoning`、OpenAI SDK openai 专属）——**没有一家把 reasoning 塞进"跨 provider 共享"的 options**。
3. **结构**：option 串下去（eino/OpenAI/MAF，3/5）vs dynamic callable 每轮重选（langgraph）vs 换实例（CrewAI）。

## crew-go 的决策

**跟一致的语义，机制挑贴合我们已有缝的那个，类型策略按我们的 Tier-A 现实调整，不照抄任何一家。**

1. **语义照多数派**：构造期默认 + per-call 覆盖 + per-call 赢 + 按 key replace + 不重建。这套我们**早就在 `runOverrides` 上落地了**（`model`/`systemPrompt`/`maxRounds` 三个 per-call 覆盖字段，`agent_v2/options.go`），本次只是加第四个维度 `modelOpts`。

2. **机制选"option 串下去"（eino/OpenAI/MAF 的 3/5 多数派），不选 langchain 的弱类型 kwargs / dynamic callable**：因为我们的 `runOverrides` 经 ctx 透传 + `resolveXxx` 读取的结构，天然就是"把覆盖串到 model 调用"，加 `modelOpts []model.Option` 复用同一套 fail-fast / 泄漏防护 / Run-Resume 对称，零新子系统。

3. **类型策略：共享层用 map 逃生舱、强类型 helper 挂各 provider 包（对齐多数派的真实结构，而非照抄"共享 reasoning field"）**。关键——多数派把 reasoning 做成共享 options 的强类型 field，是因为那 field 服务**单一 provider**；**crew-go 不行**：Tier-A 是 N 个国产模型共用一个 `openai.Client`，而它们 thinking 的 wire 各不同（qwen 顶层 `enable_thinking:bool`、deepseek 嵌套 `thinking:{type}`、GLM 又不同）。若在共享 `model.Options` 上塞统一 typed reasoning field，`openai.Client` 就得内置每个 provider 的 thinking wire 翻译——provider 知识泄进共享 client，错。正确做法是多数派的真实结构：**共享 `model.Options.ExtraBody map[string]any`（client 只 merge、provider 无关）+ 强类型 helper 挂 provider 包**（`deepseek.WithThinkingOption` / `qwen.WithEnableThinkingOption`，对齐 eino 把 `WithReasoningEffort` 放 `acl/openai` 不放 core）。

4. **reasoning_content 多轮回传做成一等字段**（`Message.ReasoningContent`）：thinking 模型 wire 要求多轮把上轮 reasoning_content 原样回传（不回传第 2 轮 400，ISSUE-89）。这点五方凡支持 thinking 的都处理（OpenAI SDK `auto_chain_reasoning`、CrewAI `auto_chain`）——躲不掉，与按次开关捆一起做。

落地细节（API + 用法）见 [`doc/v2/model-adaptation.md`](../../v2/model-adaptation.md)。

## 决策记录

| 决策 | 选择 | 第一性理由 | commit |
|---|---|---|---|
| 运行时参数覆盖机制 | 复用 `runOverrides` ctx 覆盖，加 `modelOpts []model.Option` 维度 | 我们已有"构造默认+按次覆盖"缝，加维度零新子系统；语义符合 4/5 多数派 | `2869754` |
| provider 参数类型策略 | 共享 `model.Options.ExtraBody` map 逃生舱 + provider 包强类型 helper | Tier-A N provider 共用一个 client、thinking wire 各不同，统一 typed field 会让 provider 知识泄进共享 client | `b911da4` |
| thinking 默认值 | 默认 **关**，`WithRunModelOptions` 按次开 | 用户要的是按次开关，非默认开；关更快/省/兼容 forced tool_choice（LLMPlanner） | `b911da4` |
| reasoning_content 回传 | 一等字段 `Message.ReasoningContent` + openai client 解析/回传 | thinking 模型 wire 硬要求多轮回传，framework 兜住业务不用自己处理 | `6e4bd09` |
| 拒绝方案 | 不做 langchain `init_chat_model` 弱类型 kwargs、不做 dynamic-callable-only | 弱类型丢 IDE 补全/类型检查；dynamic callable 是 LangGraph 无 per-call option 通道的替代，我们有通道不需要 | — |

## 五方源码本地路径

| 框架 | 路径 | 版本 |
|---|---|---|
| eino | `/Users/gupengfei/program/go/src/eino` + `pkg/mod/.../eino-ext/...openai@v0.1.9` / `acl/openai@v0.1.14` | v0.8.0 |
| OpenAI Agents SDK | `/Users/gupengfei/program/go/src/openai-agents-python` | v0.17.x |
| MAF | `/Users/gupengfei/program/go/src/agent-framework` | python-1.8.1 / dotnet-1.10.0 |
| LangGraph | `/Users/gupengfei/program/go/src/langgraph`（langchain-core partner 包非本地，部分背景知识）| — |
| CrewAI | `/Users/gupengfei/program/go/src/crewAI` | v1.14.8a |
