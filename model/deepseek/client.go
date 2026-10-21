// Package deepseek 提供 DeepSeek ChatModel 适配器。
//
// DeepSeek 兼容 OpenAI API，本包复用 model/openai.Client（经 openai.NewCompatible +
// Preset 装配），仅默认 base URL / 模型名 / thinking 默认值不同。业务用法同 qwen 包：
// `deepseek.NewClient(apiKey, "")`。
//
// 测试 / example **备选**模型（默认走 qwen.NewTestClient，见 model/qwen；deepseek
// 需业务显式传 DEEPSEEK_API_KEY）。
//
// **thinking 模式默认关闭**: 默认关 thinking——快/省, 且兼容框架强制 tool_choice 的
// 路径 (LLMPlanner 等结构化输出). 业务要 thinking 推理: 构造期 WithThinking(true) 或
// per-request WithThinkingOption(true) (页面开关按次切).
//
// thinking 模型多轮要求把上轮 assistant 的 reasoning_content 原样回传 (不回传第 2 轮
// 400). **框架已做成 first-class Message.ReasoningContent 自动解析 + 多轮回传**
// (openai client convertResponse/convertMessages, ISSUE-89 长期方案), 业务无需自己
// 处理, 开 thinking 多轮安全.
package deepseek

import (
	"crew-go/model"
	"crew-go/model/openai"
)

const (
	defaultBaseURL = "https://api.deepseek.com"
	defaultModel   = "deepseek-v4-flash"
)

// deepseekCap DeepSeek 结构化输出能力画像：明确不支持 json_schema（官方拒），走
// json_object + 覆盖最广的 forced tool（ModeTool）。
var deepseekCap = model.NonStrictToolCapability()

// preset DeepSeek 默认配置。默认 wire payload 附 `thinking:{type:disabled}` 关 thinking
// （见包 godoc ISSUE-89 说明）；业务传 WithThinking(true) 后值覆盖。
var preset = openai.Preset{
	System:               "deepseek",
	BaseURL:              defaultBaseURL,
	DefaultModel:         defaultModel,
	ExtraBody:            map[string]any{"thinking": map[string]any{"type": "disabled"}},
	StructuredCapability: &deepseekCap,
}

// NewClient 创建 DeepSeek ChatModel。DeepSeek 兼容 OpenAI API，直接复用 openai.Client。
//
// apiKey 对应 DEEPSEEK_API_KEY (envloader.Load 从项目根 .env 注入)。
// modelName 留空时使用 deepseek-v4-flash。
//
// 默认 wire payload 附 `thinking: {type: "disabled"}` 关 thinking 模式 (见包 godoc
// ISSUE-89 说明). 业务想用 thinking 用 `WithThinking(true)` 覆盖.
func NewClient(apiKey string, modelName string, opts ...openai.ClientOption) model.ChatModel {
	return openai.NewCompatible(preset, apiKey, modelName, opts...)
}

// WithThinking 控制 DeepSeek thinking 推理模式 (构造期 ClientOption; per-request 按次
// 切换用 WithThinkingOption)。
//
//   - false (默认): 走普通 chat 路径, response 无 reasoning_content 字段.
//   - true: 启 thinking 推理, response 含 reasoning_content. 框架自动解析进
//     Message.ReasoningContent 并多轮回传 (ISSUE-89 已修), 业务无需自己处理, 多轮安全.
//
// 多次调用累积 (后值覆盖). 业务在 deepseek.NewClient 后调 `WithThinking(true)` 即可.
func WithThinking(enabled bool) openai.ClientOption {
	state := "disabled"
	if enabled {
		state = "enabled"
	}
	return openai.WithExtraBody(map[string]any{
		"thinking": map[string]any{"type": state},
	})
}

// WithThinkingOption 是 WithThinking 的 per-request 版本（返 model.Option 而非
// ClientOption）——业务按次控制 thinking（页面开关场景：这次开、下次不开），配合
// agent_v2.WithRunModelOptions 透传到本次 Run 的每次 model 调用，无需为切换重建 client。
//
// 语义同 WithThinking：true=启 thinking（response 含 reasoning_content，多轮由 framework
// 自动回传，见 Message.ReasoningContent），false=关。构造期默认 thinking 关；本 option
// 按次覆盖（per-request 同 key 赢）。
func WithThinkingOption(enabled bool) model.Option {
	state := "disabled"
	if enabled {
		state = "enabled"
	}
	return model.WithExtraBody(map[string]any{
		"thinking": map[string]any{"type": state},
	})
}
