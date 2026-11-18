package common

type ToolresultContentBlock struct {
	Type       ContentBlockType `json:"type"`
	ToolCallId string           `json:"toolCallId"`
	Content    string           `json:"content"`
	// Parts 保留带附件的有序内容。Content 是同一组内容的文本投影，用于旧存档和纯文本模型。
	Parts []ToolResultPart `json:"parts,omitempty"`
	// IsError 标记这次工具调用失败了（参数非法、超时、工具自己返回失败……）。框架在合成失败结果的
	// 同一个入口置上它（agent_v2 的 markToolFailure），OnToolEnd 改写结论时跟着改。
	//
	// 原生支持失败标记的 provider 据它发 wire 字段（Anthropic 的 tool_result.is_error），不认的
	// provider 忽略它——失败的说明照旧在 Content 里（`[code] message`），模型两边都看得见。
	// omitempty：成功的结果不带这个键，存档与之前的版本一致。
	IsError bool `json:"isError,omitempty"`
}

// ToolResultPart 是工具结果中的文本或内嵌媒体。媒体用 MIME 类型和标准 base64 保存，Text 是它的文本说明。
type ToolResultPart struct {
	Text      string `json:"text,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
}
