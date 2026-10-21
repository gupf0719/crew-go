package common

type ModelConfig struct {
	ModelName   string         `json:"modelName"`
	Temperature float32        `json:"temperature"`
	MaxTokens   int32          `json:"maxTokens"`
	TopP        float32        `json:"topP"`
	Extra       map[string]any `json:"extra"`
}
