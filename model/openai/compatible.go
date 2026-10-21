package openai

import "crew-go/model"

// Preset 声明一个 OpenAI 兼容 provider 的默认配置（纯数据，无行为）。
//
// 加一个兼容 provider = 造一个 Preset 值（业务可内联，也可放独立包如 qwen/deepseek
// 复用）；删一个 = 删掉它。core model 包与编排层只认 model.ChatModel 接口，不感知
// Preset——增删 provider 不触碰任何中心登记表 / switch，影响面只在构造点。
type Preset struct {
	// System 是 provider 身份标识（gen_ai.system observability attr），如
	// "qwen" / "deepseek" / "dashscope"。填进每条响应的 Message.Metadata.System。
	System string
	// BaseURL 是该 provider 的 OpenAI 兼容 endpoint（chat/completions 的前缀）。
	BaseURL string
	// DefaultModel 是 NewCompatible 的 modelName 留空时使用的默认模型名。
	DefaultModel string
	// ExtraBody 是该 provider 的默认 wire 扩展字段（顶层 merge 进请求体）。典型：
	// deepseek 的 `thinking:{type:disabled}` / qwen dashscope 的 `enable_thinking:false`。
	// 业务经 opts 追加的 WithExtraBody 后值覆盖同 key（见 WithExtraBody 累积语义）。空则不附加。
	ExtraBody map[string]any
	// StructuredCapability 是该 provider 的结构化输出能力画像（nil=NewCompatible 用保守
	// 默认 model.NonStrictToolCapability——非官方 OpenAI provider 多不支持 strict，避免误
	// 判发 400）。qwen/deepseek 等已知 provider 显式给，未知兼容 provider 留 nil 走保守默认。
	StructuredCapability *model.StructuredCapability
	// OptionalFinishReason 为 true 表示该 provider 的流式响应可能不带 finish_reason，NewCompatible
	// 据此加上 WithOptionalFinishReason。默认 false：流结束时没收到 finish_reason 判为截断。
	OptionalFinishReason bool
	// StreamUnsupported 为 true 表示该 provider 不接受 stream=true，NewCompatible 据此加上
	// WithStreamUnsupported：Stream 直接返回 model.ErrStreamUnsupported，BasicAgent 改调 Generate。
	StreamUnsupported bool
}

// NewCompatible 用 Preset 构造 OpenAI 兼容 ChatModel（qwen/deepseek/GLM/Kimi/豆包/
// Ollama/Azure… 全走这里，只是 Preset 数据不同）。
//
// modelName 留空时用 p.DefaultModel。opts 追加在 Preset 默认之后——同类 ClientOption
// 后值覆盖（业务可覆盖 baseURL、追加/覆盖 extraBody、换 HTTP client 等）。
//
// 这是 Tier-A（OpenAI 兼容）provider 的统一入口：底层复用同一个 openai.Client 实现，
// provider 差异全部落在 Preset 数据里，不写新实现、不碰核心。
func NewCompatible(p Preset, apiKey, modelName string, opts ...ClientOption) model.ChatModel {
	if modelName == "" {
		modelName = p.DefaultModel
	}
	base := make([]ClientOption, 0, len(opts)+4)
	base = append(base, WithBaseURL(p.BaseURL), WithSystem(p.System))
	if len(p.ExtraBody) > 0 {
		base = append(base, WithExtraBody(p.ExtraBody))
	}
	// 结构化输出能力画像：Preset 显式给则用之，否则非 openai 兼容 provider 保守默认
	// （非 strict + ModeTool，覆盖最广）——避免未知 provider 误判支持 strict 发 400。
	// 排在 opts 之前，业务经 opts 传 WithStructuredCapability 可覆盖（后值赢）。
	if p.StructuredCapability != nil {
		base = append(base, WithStructuredCapability(*p.StructuredCapability))
	} else {
		base = append(base, WithStructuredCapability(model.NonStrictToolCapability()))
	}
	if p.OptionalFinishReason {
		base = append(base, WithOptionalFinishReason())
	}
	if p.StreamUnsupported {
		base = append(base, WithStreamUnsupported())
	}
	base = append(base, opts...)
	return NewClient(apiKey, modelName, base...)
}
