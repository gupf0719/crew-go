package common

type ContentBlock struct {
	Type       string                  `json:"type"`
	Text       *TextContentBlock       `json:"text,omitempty"`
	ImageUrl   *ImageUrlContentBlock   `json:"imageUrl,omitempty"`
	InputAudio *InputAudioContentBlock `json:"inputAudio,omitempty"`
	File       *FileContentBlock       `json:"file,omitempty"`
	Toolcall   *ToolcallContentBlock   `json:"toolcall,omitempty"`
	Toolresult *ToolresultContentBlock `json:"toolresult,omitempty"`
	// Data + MediaType: A2A 新版 Part 对齐（crew-go 为跨语言互通新增，spec 无此二字段）。
	// Data 承载结构化 JSON（A2A data part）；MediaType 是 part 级媒体类型（text/plain、
	// image/png、application/json——对齐 A2A Part.MediaType，所有 part 类型通用）。现有 LLM
	// 轮转的 block（text/toolcall/toolresult/image）不填这俩，omitempty 序列化/cp 向后兼容。
	Data      *DataContentBlock `json:"data,omitempty"`
	MediaType string            `json:"mediaType,omitempty"`
}
