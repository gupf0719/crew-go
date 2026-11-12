package openai

import (
	"encoding/json"
	"fmt"
	"strings"

	"crew-go/schema"
)

type openaiContentPart struct {
	Type       string             `json:"type"`
	Text       string             `json:"text,omitempty"`
	ImageURL   *openaiMediaURL    `json:"image_url,omitempty"`
	InputAudio *schema.InputAudio `json:"input_audio,omitempty"`
	File       *openaiInputFile   `json:"file,omitempty"`
	VideoURL   *openaiMediaURL    `json:"video_url,omitempty"`
}

type openaiMediaURL struct {
	URL string `json:"url"`
}

type openaiInputFile struct {
	ID       string `json:"file_id,omitempty"`
	Data     string `json:"file_data,omitempty"`
	Filename string `json:"filename,omitempty"`
}

func (m openaiMessage) MarshalJSON() ([]byte, error) {
	type plain openaiMessage
	if len(m.contentParts) == 0 {
		return json.Marshal(plain(m))
	}
	return json.Marshal(struct {
		plain
		Content []openaiContentPart `json:"content"`
	}{plain: plain(m), Content: m.contentParts})
}

// userContentParts 保留纯文本的 string wire 形式。有内容块时，Content 是第一个文本部分。
func userContentParts(m *schema.Message, provider string) ([]openaiContentPart, error) {
	if len(m.ContentBlocks) == 0 {
		return nil, nil
	}
	parts := make([]openaiContentPart, 0, len(m.ContentBlocks)+1)
	if m.Content != "" {
		parts = append(parts, openaiContentPart{Type: "text", Text: m.Content})
	}
	before := len(parts)
	for i, block := range m.ContentBlocks {
		if block == nil {
			continue
		}
		part, err := userContentPart(block, provider)
		if err != nil {
			return nil, fmt.Errorf("content block %d: %w", i, err)
		}
		if part.Type != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == before {
		return nil, nil
	}
	return parts, nil
}

func userContentPart(block *schema.ContentBlock, provider string) (openaiContentPart, error) {
	switch {
	case block.Text != nil && block.Text.Text != "":
		return openaiContentPart{Type: "text", Text: block.Text.Text}, nil
	case block.ImageUrl != nil:
		if block.ImageUrl.ImageUrl == nil || block.ImageUrl.ImageUrl.Url == "" {
			return openaiContentPart{}, fmt.Errorf("image block requires a URL or data URL")
		}
		return openaiContentPart{Type: "image_url", ImageURL: &openaiMediaURL{URL: block.ImageUrl.ImageUrl.Url}}, nil
	case block.InputAudio != nil:
		audio := block.InputAudio.InputAudio
		if audio == nil {
			return openaiContentPart{}, fmt.Errorf("audio block has no data")
		}
		return openaiAudioPart(audio.Format, audio.Data, provider)
	case block.File != nil:
		if block.File.File == nil {
			return openaiContentPart{}, fmt.Errorf("file block has no source")
		}
		return openaiFilePart(block.File.File, block.MediaType, provider)
	default:
		return openaiContentPart{}, nil
	}
}

func openaiFilePart(file *schema.FileData, mediaType, provider string) (openaiContentPart, error) {
	if file.ID != "" {
		if (file.Provider != "" && file.Provider != provider) || (provider != "openai" && file.Provider == "") {
			return openaiContentPart{}, fmt.Errorf("file handle is not for provider %q", provider)
		}
		if mediaType != "" && mediaType != "application/pdf" {
			return openaiContentPart{}, fmt.Errorf("chat completions supports PDF document handles")
		}
		return openaiContentPart{Type: "file", File: &openaiInputFile{ID: file.ID}}, nil
	}
	source := file.URL
	if file.FileData != "" && mediaType != "" {
		source = "data:" + mediaType + ";base64," + file.FileData
	}
	switch {
	case strings.HasPrefix(mediaType, "image/") && source != "":
		return openaiContentPart{Type: "image_url", ImageURL: &openaiMediaURL{URL: source}}, nil
	case strings.HasPrefix(mediaType, "audio/"):
		var format string
		switch mediaType {
		case "audio/wav", "audio/x-wav", "audio/wave", "audio/vnd.wave":
			format = "wav"
		case "audio/mpeg", "audio/mp3":
			format = "mp3"
		default:
			return openaiContentPart{}, fmt.Errorf("unsupported audio media type %q", mediaType)
		}
		data := file.FileData
		if data == "" && provider == "dashscope" {
			data = file.URL
		}
		return openaiAudioPart(format, data, provider)
	case strings.HasPrefix(mediaType, "video/") && source != "" && provider == "dashscope":
		return openaiContentPart{Type: "video_url", VideoURL: &openaiMediaURL{URL: source}}, nil
	case mediaType == "application/pdf" && file.FileData != "" && provider != "dashscope":
		filename := file.Filename
		if filename == "" {
			filename = "document.pdf"
		}
		return openaiContentPart{Type: "file", File: &openaiInputFile{Data: source, Filename: filename}}, nil
	default:
		return openaiContentPart{}, fmt.Errorf("unsupported file source for media type %q", mediaType)
	}
}

func openaiAudioPart(format, data, provider string) (openaiContentPart, error) {
	if data == "" || (format != "wav" && format != "mp3") {
		return openaiContentPart{}, fmt.Errorf("audio input requires wav or mp3 data")
	}
	if provider == "dashscope" && !strings.HasPrefix(data, "https://") && !strings.HasPrefix(data, "http://") && !strings.HasPrefix(data, "data:") {
		mediaType := "audio/wav"
		if format == "mp3" {
			mediaType = "audio/mpeg"
		}
		data = "data:" + mediaType + ";base64," + data
	}
	return openaiContentPart{Type: "input_audio", InputAudio: &schema.InputAudio{Data: data, Format: format}}, nil
}
