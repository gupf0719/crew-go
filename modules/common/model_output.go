package common

type ModelOutput struct {
	Message *Message       `json:"message"`
	Usage   *MessageUsage  `json:"usage"`
	Extra   map[string]any `json:"extra"`
}
