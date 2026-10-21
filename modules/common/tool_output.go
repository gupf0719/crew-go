package common

type ToolOutput struct {
	CallId string          `json:"callId"`
	Result *ToolCallResult `json:"result"`
	Extra  map[string]any  `json:"extra"`
}
