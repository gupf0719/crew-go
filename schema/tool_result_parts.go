package schema

import "strings"

// ToolResultPartsText 用空行分隔有文字的部分，生成可交给纯文本模型的正文。
func ToolResultPartsText(parts []ToolResultPart) string {
	var text strings.Builder
	for _, part := range parts {
		if part.Text == "" {
			continue
		}
		if text.Len() > 0 {
			text.WriteString("\n\n")
		}
		text.WriteString(part.Text)
	}
	return text.String()
}

// ToolResultTextForMessage 接收消息的平面正文。带附件的首个结果若只改写了嵌套正文，采用该改写。
// 平面正文也被改写时，以调用方传入的平面正文为准。
func ToolResultTextForMessage(result *ToolresultContentBlock, text string) string {
	if result != nil && len(result.Parts) > 0 && result.Content != text && ToolResultPartsText(result.Parts) == text {
		return result.Content
	}
	return text
}

// ToolResultPartsForText 取与本次实际正文对应的有序内容。失败、旧格式或正文已改写时返回 nil。
func ToolResultPartsForText(result *ToolresultContentBlock, text string) []ToolResultPart {
	if result == nil || result.IsError || len(result.Parts) == 0 || result.Content != text || ToolResultPartsText(result.Parts) != text {
		return nil
	}
	return result.Parts
}
