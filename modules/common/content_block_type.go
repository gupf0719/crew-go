package common

type ContentBlockType string

const (
	ContentBlockTypeText       ContentBlockType = "text"
	ContentBlockTypeImageUrl   ContentBlockType = "image_url"
	ContentBlockTypeInputAudio ContentBlockType = "input_audio"
	ContentBlockTypeFile       ContentBlockType = "file"
	ContentBlockTypeToolcall   ContentBlockType = "toolcall"
	ContentBlockTypeToolresult ContentBlockType = "toolresult"
	// ContentBlockTypeData 承载结构化 JSON（A2A 新版 data part 对齐）。spec 原枚举无此项——
	// crew-go 为 A2A 各语言互通新增（A2A Part 4 种 content: text/raw/url/data，见
	// a2a/convert.go）。现有 LLM 轮转不产生 data block；仅 A2A 接收结构化数据时才填。
	ContentBlockTypeData ContentBlockType = "data"
)
