package common

type TextContentBlock struct {
	Type ContentBlockType `json:"type"`
	Text string           `json:"text"`
}
