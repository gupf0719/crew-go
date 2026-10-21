package common

type ToolInput struct {
	Name          string         `json:"name"`
	CallId        string         `json:"callId"`
	ArgumentsJSON string         `json:"argumentsJSON"`
	Extra         map[string]any `json:"extra"`
}
