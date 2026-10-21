package model

// ResponseFormat 声明本次调用期望的响应格式（结构化输出路径 1：原生 response_format）。
// 对齐 OpenAI /chat/completions 的 response_format 字段，qwen/deepseek 兼容端点同款。
//   - Type="json_object"：要求返回合法 JSON 对象（provider 通常要求 prompt 含 "json"）
//   - Type="json_schema"：按 JSONSchema 约束结构（strict-capable provider 硬保证）
//   - Type="text" 或 nil：自由文本（默认）
//
// 结构化输出的机制选择 / 校验回炉在 agent_v2 层（见 doc/v2/structured-output.md）；
// model 层只承载 wire 载荷，不做校验。
type ResponseFormat struct {
	// Type 是格式类型："text" / "json_object" / "json_schema"。
	Type string
	// JSONSchema 是 Type="json_schema" 时的 schema 载荷（其他类型忽略）。
	JSONSchema *ResponseFormatJSONSchema
}

// ResponseFormatJSONSchema 是 response_format json_schema 档的 schema 载荷。
type ResponseFormatJSONSchema struct {
	// Name 是 schema 名（provider 要求，标识本次结构）。
	Name string
	// Strict 决定是否走 provider 的 strict 硬约束。为 true 时要求 Schema 已 strict
	// 兼容（见 NormalizeJSONSchema），否则 strict-capable provider 返 400。
	Strict bool
	// Schema 是 JSONSchema 本体（map[string]any，Go 原生零依赖，不引反射库）。
	Schema map[string]any
}
