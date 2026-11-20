package common

type ModelConfig struct {
	ModelName   string         `json:"modelName"`
	Temperature float32        `json:"temperature"`
	MaxTokens   int32          `json:"maxTokens"`
	TopP        float32        `json:"topP"`
	Extra       map[string]any `json:"extra"`

	// OutputType 是请求的输出类型，例如 json 或 text。空表示调用未声明类型。
	OutputType string `json:"outputType,omitempty"`
}
