package common

type ToolcallContentBlock struct {
	Type      ContentBlockType `json:"type"`
	Id        string           `json:"id"`
	Arguments string           `json:"arguments"`
	Name      string           `json:"name"`
}
