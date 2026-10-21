package common

type ToolresultContentBlock struct {
	Type       ContentBlockType `json:"type"`
	ToolCallId string           `json:"toolCallId"`
	Content    string           `json:"content"`
}
