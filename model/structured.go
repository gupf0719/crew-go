package model

// 结构化输出的机制档位 + per-model 能力画像（model 层数据类型）。业务只声明一次 output
// schema，agent 层按 provider 的 StructuredCapability 选机制（见 doc/v2/structured-output.md）。
// model 层只承载数据 + provider 自报能力，机制选择 / 校验回炉在 agent_v2 层。

// StructuredMode 是结构化输出的机制档位。
//
// ModeResponseFormat / ModeJSONObject 落到 model 层的 response_format wire；ModeTool 是
// agent 层 final_output 工具编排（不经 response_format，任何支持 function calling 的
// provider 都认，异构 fallback 天然安全）；ModeAuto 让 agent 层按 StructuredCapability 选。
// model 层只消费 ResponseFormat——ModeTool/ModeAuto 对 model 层是 advisory（capability
// 自报的推荐默认），由 agent 层落地。
type StructuredMode int

const (
	// ModeAuto 按 provider capability 自动选机制（默认）。**当前 agent 层固定 ModeTool，
	// ModeAuto 的自动选择属 P2/P3 未实现（见 doc/v2/structured-output.md）。**
	ModeAuto StructuredMode = iota
	// ModeTool forced output tool（覆盖最广，非 strict provider 默认落点）。
	ModeTool
	// ModeResponseFormat 原生 response_format json_schema（strict-capable provider）。
	ModeResponseFormat
	// ModeJSONObject response_format json_object + prompt 注入 + 客户端校验（弱档兜底）。
	ModeJSONObject
)

// String 便于日志 / 错误信息可读。
func (m StructuredMode) String() string {
	switch m {
	case ModeAuto:
		return "auto"
	case ModeTool:
		return "tool"
	case ModeResponseFormat:
		return "response_format"
	case ModeJSONObject:
		return "json_object"
	default:
		return "unknown"
	}
}

// StructuredCapability 是 per-model 的结构化输出能力画像（provider client 自报 + 用户可
// override）。多字段能力集（非单个 strict bool），让 agent 层按能力选机制、未知保守降级，
// 不查中央表、不运行时探测。
type StructuredCapability struct {
	// SupportsJSONSchema 原生 response_format json_schema（OpenAI 支持；
	// qwen-flash/deepseek 不支持——只 json_object）。
	SupportsJSONSchema bool
	// SupportsJSONObject response_format json_object（qwen/deepseek/OpenAI 都支持；
	// qwen 要求 prompt 含 "json" 关键词 + 关 thinking）。
	SupportsJSONObject bool
	// SupportsToolChoiceRequired tool_choice=required（forced-tool 路径前提；某些 qwen
	// 变体如 qwen-3-coder 为 false，需业务 override）。
	SupportsToolChoiceRequired bool
	// SupportsStrictTool forced output tool 能否带 strict（OpenAI 支持；xAI/Grok 等支持
	// 普通 tool 但不支持 strict tool definition）。
	SupportsStrictTool bool
	// DefaultMode 该 model 的推荐默认机制（**当前 agent 层不消费——固定 ModeTool，P2/P3**）。
	DefaultMode StructuredMode
}

// StructuredCapable 是 model.ChatModel 的可选能力接口：实现它的 client 自报结构化输出
// 能力画像。agent 层类型断言此接口选机制；未实现的 client 由 agent 层走保守默认（非
// strict）。定义在 model 层（实现者 / 消费者都已 import model，零额外耦合）。
type StructuredCapable interface {
	// StructuredCapability 返回对给定 model 的结构化输出能力画像。
	StructuredCapability(model string) StructuredCapability
}

// DefaultOpenAICapability 是官方 OpenAI 的能力画像（全支持 + strict，默认走
// response_format json_schema）。openai.NewClient 直连官方端点时的默认。
func DefaultOpenAICapability() StructuredCapability {
	return StructuredCapability{
		SupportsJSONSchema:         true,
		SupportsJSONObject:         true,
		SupportsToolChoiceRequired: true,
		SupportsStrictTool:         true,
		DefaultMode:                ModeResponseFormat,
	}
}

// NonStrictToolCapability 是非 strict provider 的能力画像（json_object + forced tool，无
// strict 硬保证，默认走覆盖最广的 ModeTool）。qwen/deepseek 及未知 OpenAI 兼容 provider
// 的保守默认——对齐"任何支持 function calling 的 provider 都认 final_output 工具"。
func NonStrictToolCapability() StructuredCapability {
	return StructuredCapability{
		SupportsJSONSchema:         false,
		SupportsJSONObject:         true,
		SupportsToolChoiceRequired: true,
		SupportsStrictTool:         false,
		DefaultMode:                ModeTool,
	}
}
