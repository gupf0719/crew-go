package model

import "crew-go/schema"

// Options 为 ChatModel 调用保持每个请求的配置。
type Options struct {
	Temperature *float32
	MaxTokens   *int
	TopP        *float32
	Stop        []string
	Model       *string
	Tools       []*schema.ToolInfo
	ToolChoice  *schema.ToolChoice
	// AllowedTools 是工具白名单——非空时本次调用只向 LLM 暴露 Name 命中的工具
	// （从已 bind 的工具里按名过滤），其余不发。配合 ToolChoice=Required 可强制
	// LLM 只能从子集里调一个，等价"指定工具"。空/nil 表示不限制（全部可用）。
	AllowedTools []string
	// ExtraBody 是 per-request 的 provider 扩展字段逃生舱（顶层 merge 进 wire 请求体）。
	// provider 专属参数（thinking / reasoning 开关等）按次控制走这里，叠在构造期 client
	// 的默认 extra body 之上——同 key per-request 赢、不同 key 保留。空则不附加。各 provider
	// 包提供强类型便利包装（如 deepseek.WithThinkingOption / qwen.WithEnableThinkingOption）。
	ExtraBody map[string]any
	// ResponseFormat 声明本次调用期望的响应格式（结构化输出路径 1：原生 response_format）。
	// nil 表示自由文本。见 ResponseFormat / WithResponseFormat。
	ResponseFormat *ResponseFormat
}

// Option 是 ChatModel 调用的功能选项。
type Option func(*Options)

// Apply 应用所有选项并返回合并的 Options。
func Apply(opts ...Option) *Options {
	o := &Options{}
	for _, fn := range opts {
		fn(o)
	}
	return o
}

func WithTemperature(t float32) Option {
	return func(o *Options) { o.Temperature = &t }
}

func WithMaxTokens(n int) Option {
	return func(o *Options) { o.MaxTokens = &n }
}

func WithTopP(p float32) Option {
	return func(o *Options) { o.TopP = &p }
}

func WithStop(stop []string) Option {
	return func(o *Options) { o.Stop = stop }
}

func WithModel(name string) Option {
	return func(o *Options) { o.Model = &name }
}

func WithTools(tools []*schema.ToolInfo) Option {
	return func(o *Options) { o.Tools = tools }
}

func WithToolChoice(choice schema.ToolChoice) Option {
	return func(o *Options) { o.ToolChoice = &choice }
}

// WithAllowedTools 设工具白名单：本次调用只向 LLM 暴露 names 中的工具（从已 bind
// 工具里按名过滤）。空/不传表示不限制。配合 WithToolChoice(ToolChoiceRequired)
// 可强制 LLM 只能从子集里选一个调用。
func WithAllowedTools(names ...string) Option {
	return func(o *Options) { o.AllowedTools = names }
}

// WithExtraBody 设 per-request 的 provider 扩展字段（顶层 merge 进 wire 请求体，叠在
// 构造期 client 默认 extra body 之上，同 key per-request 赢）。用于按次控制 provider
// 专属参数（thinking / reasoning 开关等），典型经 agent_v2.WithRunModelOptions 透传到
// 本次 Run 的每次 model 调用。多次调用累积合并，同 key 后值覆盖。
//
// 与构造期 openai.WithExtraBody（ClientOption）平行：那个设 client 默认，这个按次覆盖。
func WithExtraBody(fields map[string]any) Option {
	return func(o *Options) {
		if o.ExtraBody == nil {
			o.ExtraBody = make(map[string]any, len(fields))
		}
		for k, v := range fields {
			o.ExtraBody[k] = v
		}
	}
}

// WithResponseFormat 设本次调用的响应格式（结构化输出路径 1：原生 response_format）。
// 传 nil 等价不设（自由文本）。json_schema strict 档要求 rf.JSONSchema.Schema 已 strict
// 兼容（业务或 agent 层先过 NormalizeJSONSchema），否则 strict-capable provider 返 400。
func WithResponseFormat(rf *ResponseFormat) Option {
	return func(o *Options) { o.ResponseFormat = rf }
}

// WithJSONSchema 便利包装：设 response_format=json_schema（结构化输出 strict 路径）。
// schema 应已 strict 兼容（见 NormalizeJSONSchema）。strict=false 时仍发 json_schema
// 但不要求 provider 硬保证（尽力而为）。
func WithJSONSchema(name string, schema map[string]any, strict bool) Option {
	return WithResponseFormat(&ResponseFormat{
		Type:       "json_schema",
		JSONSchema: &ResponseFormatJSONSchema{Name: name, Strict: strict, Schema: schema},
	})
}

// WithJSONObject 便利包装：设 response_format=json_object（弱约束，只保证合法 JSON、
// 不约束具体结构）。qwen/deepseek 要求 prompt 里含 "json" 关键词 + 关 thinking。
func WithJSONObject() Option {
	return WithResponseFormat(&ResponseFormat{Type: "json_object"})
}
