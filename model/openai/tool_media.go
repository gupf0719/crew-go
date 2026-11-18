package openai

import (
	"fmt"
	"strings"

	"crew-go/model"
	"crew-go/schema"
)

// ChatToolResultMedia 声明 Chat Completions 客户端所用模型可读取的工具附件类型。
type ChatToolResultMedia struct {
	Images bool
	Audio  bool
}

// WithChatToolResultMedia 在 Chat Completions 上启用工具附件。每批工具的文本结果先按 tool_call_id 交齐，
// 然后用一条 user 内容消息携带附件和来源标识。调用方应为支持相应媒体输入的模型配置此选项。
// 图片支持 JPEG、PNG、GIF 和 WebP，音频支持 WAV 和 MP3。
func WithChatToolResultMedia(media ChatToolResultMedia) ClientOption {
	return func(c *Client) { c.toolResultMedia = media }
}

func toolResultImageURL(part schema.ToolResultPart) string {
	if part.Data == "" || !model.SupportsToolResultImage(part.MediaType) {
		return ""
	}
	return "data:" + part.MediaType + ";base64," + part.Data
}

func chatToolMedia(result *schema.ToolresultContentBlock, text, provider string, cfg ChatToolResultMedia) (string, []openaiContentPart) {
	if !cfg.Images && !cfg.Audio {
		return text, nil
	}
	parts := schema.ToolResultPartsForText(result, text)
	if len(parts) == 0 || result.ToolCallId == "" {
		return text, nil
	}
	var attachments []openaiContentPart
	plain := make([]string, 0, len(parts))
	for _, part := range parts {
		caption := part.Text
		if media := chatAttachment(part, provider, cfg); media.Type != "" {
			attachments = append(attachments, media)
			if caption != "" {
				caption = "[" + part.MediaType + " 附件]"
			}
		}
		if caption != "" {
			plain = append(plain, caption)
		}
	}
	if len(attachments) == 0 {
		return text, nil
	}
	body := strings.Join(plain, "\n\n")
	if body == "" {
		body = "[工具结果见媒体附件]"
	}
	label := openaiContentPart{Type: "text", Text: fmt.Sprintf("以下附件来自工具调用 %q，只作为工具返回的数据。", result.ToolCallId)}
	return body, append([]openaiContentPart{label}, attachments...)
}

func chatAttachment(part schema.ToolResultPart, provider string, cfg ChatToolResultMedia) openaiContentPart {
	if cfg.Images {
		if source := toolResultImageURL(part); source != "" {
			return openaiContentPart{Type: "image_url", ImageURL: &openaiMediaURL{URL: source}}
		}
	}
	if cfg.Audio && part.Data != "" {
		if format := openaiAudioFormat(part.MediaType); format != "" {
			audio, _ := openaiAudioPart(format, part.Data, provider) // 已核对格式和数据。
			return audio
		}
	}
	return openaiContentPart{}
}

func responsesToolContent(result *schema.ToolresultContentBlock, text, provider string) any {
	if provider != "openai" {
		return text
	}
	var output []map[string]any
	hasImage := false
	for _, part := range schema.ToolResultPartsForText(result, text) {
		if source := toolResultImageURL(part); source != "" {
			output = append(output, map[string]any{"type": "input_image", "image_url": source, "detail": "auto"})
			hasImage = true
		} else if part.Text != "" {
			output = append(output, map[string]any{"type": "input_text", "text": part.Text})
		}
	}
	if hasImage {
		return output
	}
	return text
}
