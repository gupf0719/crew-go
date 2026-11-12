package agui

import (
	"encoding/base64"
	"fmt"
	"mime"
	"net/url"
	"path"
	"strings"

	"crew-go/schema"
)

type clientContentPart struct {
	Type   string `json:"type"`
	Text   string `json:"text"`
	Source struct {
		Type     string `json:"type"`
		Value    string `json:"value"`
		MIMEType string `json:"mimeType"`
		Provider string `json:"provider"`
	} `json:"source"`
}

func (m AGUIMessage) userMessage() *schema.Message {
	msg := schema.UserMessage(m.Content)
	msg.ContentBlocks = m.media
	return msg
}

func (p clientContentPart) mediaBlock() (*schema.ContentBlock, error) {
	switch p.Type {
	case "image", "audio", "video", "document":
	default:
		return nil, nil // 未来的内容类型由上层按未知部分处理。
	}
	src := p.Source
	if src.Value == "" {
		return nil, fmt.Errorf("%s part requires a source value", p.Type)
	}
	mt := ""
	if src.MIMEType != "" {
		var err error
		mt, _, err = mime.ParseMediaType(src.MIMEType)
		if err != nil {
			return nil, fmt.Errorf("%s part has invalid MIME type", p.Type)
		}
	}
	file := &schema.FileData{}
	switch src.Type {
	case "data":
		if mt == "" {
			return nil, fmt.Errorf("%s data source requires a MIME type", p.Type)
		}
		if _, err := base64.StdEncoding.DecodeString(src.Value); err != nil {
			return nil, fmt.Errorf("%s data source contains invalid base64", p.Type)
		}
		file.FileData = src.Value
	case "url":
		u, err := url.Parse(src.Value)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
			return nil, fmt.Errorf("%s URL source requires an HTTP or HTTPS URL", p.Type)
		}
		file.URL = src.Value
		if mt == "" {
			mt, _, _ = mime.ParseMediaType(mime.TypeByExtension(path.Ext(u.Path))) // URL 可以省略 MIME。
		}
	case "file":
		if p.Type != "document" {
			return nil, fmt.Errorf("%s file handles are not supported", p.Type)
		}
		file.ID, file.Provider = src.Value, src.Provider
	default:
		return nil, fmt.Errorf("%s part has an unsupported source type", p.Type)
	}
	switch p.Type {
	case "image":
		if mt != "" && !strings.HasPrefix(mt, "image/") {
			return nil, fmt.Errorf("image part requires an image MIME type")
		}
		imageURL := file.URL
		if imageURL == "" {
			imageURL = "data:" + mt + ";base64," + file.FileData
		}
		return &schema.ContentBlock{Type: string(schema.ContentBlockTypeImageUrl), MediaType: mt,
			ImageUrl: &schema.ImageUrlContentBlock{Type: schema.ContentBlockTypeImageUrl, ImageUrl: &schema.ImageUrl{Url: imageURL}}}, nil
	case "audio", "video":
		if mt == "" {
			// URL 没声明媒体类型且后缀不可推断时，保留媒体的类别交给 provider 判断。
			mt = p.Type + "/unknown"
		}
		if !strings.HasPrefix(mt, p.Type+"/") {
			return nil, fmt.Errorf("%s part has a MIME type for a different medium", p.Type)
		}
	case "document":
		if mt == "" && file.ID != "" {
			mt = "application/pdf"
		}
	}
	return &schema.ContentBlock{Type: string(schema.ContentBlockTypeFile), MediaType: mt,
		File: &schema.FileContentBlock{Type: schema.ContentBlockTypeFile, File: file}}, nil
}
