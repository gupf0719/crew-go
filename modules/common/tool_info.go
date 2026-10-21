package common

type ToolInfo struct {
	Name   string         `json:"name"`
	Desc   string         `json:"desc"`
	Params map[string]any `json:"params"`
	Extra  map[string]any `json:"extra"`
}
