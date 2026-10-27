package a2a

import (
	"encoding/base64"
	"strings"
	"unicode"

	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

// buildMessage 把要发给远程的文本 + 可选 (taskID, contextID) 转 A2A Message。
// 有 task/context 时用 NewMessageForTask 续跑同一远程 task；否则新建（让 server 建 task）。
//
// 仅文本 helper；生产多模态发送走 buildMessageFromParts + MessageToA2AParts
// （text/raw/url/data 全支持，convert_internal_test.go 验）。本 helper 现仅测试用。
func buildMessage(text, taskID, contextID string) *a2atypes.Message {
	part := a2atypes.NewTextPart(text)
	if taskID != "" || contextID != "" {
		return a2atypes.NewMessageForTask(
			a2atypes.MessageRoleUser,
			a2atypes.TaskInfo{TaskID: a2atypes.TaskID(taskID), ContextID: contextID},
			part,
		)
	}
	return a2atypes.NewMessage(a2atypes.MessageRoleUser, part)
}

// a2aPartsText 提 A2A Part 列表的文本（拼接 TextPart）。仅文本——多模态/结构化用
// PartsToContent 拿完整 Content + ContentBlocks。
func a2aPartsText(parts a2atypes.ContentParts) string {
	var sb strings.Builder
	for _, p := range parts {
		if p == nil {
			continue
		}
		if t := p.Text(); t != "" {
			sb.WriteString(t)
		}
	}
	return sb.String()
}

// MessageToA2AParts 把 schema.Message 转 A2A ContentParts——完整对齐 A2A 新版 Part
// (text/raw/url/data + media_type/filename，Go/Java/Python A2A 新版 proto wire 一致)。
// Content 文本 + ContentBlocks 各类型映射。toolcall/toolresult 是 agent 内部 ReAct 状态、
// 不跨 A2A（agent 间传内容不传工具调用），跳过。
func MessageToA2AParts(msg *schema.Message) a2atypes.ContentParts {
	if msg == nil {
		return nil
	}
	var parts a2atypes.ContentParts
	if msg.Content != "" {
		parts = append(parts, a2atypes.NewTextPart(msg.Content))
	}
	for _, cb := range msg.ContentBlocks {
		if cb == nil {
			continue
		}
		if p := blockToA2APart(cb); p != nil {
			parts = append(parts, p)
		}
	}
	return parts
}

// blockToA2APart 把单个 ContentBlock 转 A2A Part（nil = 不跨 A2A 的 block，如 toolcall/
// toolresult，或文件 base64 解码失败）。File 的 inline base64→raw part、URL→url part。
func blockToA2APart(cb *schema.ContentBlock) *a2atypes.Part {
	switch {
	case cb.Text != nil:
		return a2atypes.NewTextPart(cb.Text.Text)
	case cb.Data != nil && cb.Data.Data != nil:
		// 守内部值非 nil（audit HIGH）：nil 值序列化成 {"data":null}，A2A SDK（含 Go 自己）
		// UnmarshalJSON 把它判为"无 content"→ 拒绝**整条** message（跨语言 hazard）。内部 nil
		// 直接跳过该 block，不发毒 wire（falsy 值 false/0/""/{}/[]​ 是合法非 nil，正常发）。
		p := a2atypes.NewDataPart(cb.Data.Data)
		p.MediaType = firstNonEmpty(cb.MediaType, "application/json")
		return p
	case cb.ImageUrl != nil && cb.ImageUrl.ImageUrl != nil:
		return a2atypes.NewFileURLPart(a2atypes.URL(cb.ImageUrl.ImageUrl.Url), cb.MediaType)
	case cb.InputAudio != nil && cb.InputAudio.InputAudio != nil:
		if raw, err := base64.StdEncoding.DecodeString(cb.InputAudio.InputAudio.Data); err == nil {
			p := a2atypes.NewRawPart(raw)
			fmtStr := string(cb.InputAudio.InputAudio.Format)
			switch { // audit LOW：format 已是完整 MIME（含 /）时不再前缀 audio/，避免 audio/audio/wav
			case cb.MediaType != "":
				p.MediaType = cb.MediaType
			case strings.Contains(fmtStr, "/"):
				p.MediaType = fmtStr
			default:
				p.MediaType = "audio/" + fmtStr
			}
			return p
		}
	case cb.File != nil && cb.File.File != nil:
		fd := cb.File.File
		if fd.URL != "" { // URI 引用 → A2A url part
			p := a2atypes.NewFileURLPart(a2atypes.URL(fd.URL), cb.MediaType)
			p.Filename = fd.Filename
			return p
		}
		if raw, err := base64.StdEncoding.DecodeString(fd.FileData); err == nil { // inline base64 → A2A raw part
			p := a2atypes.NewRawPart(raw)
			p.MediaType = cb.MediaType
			p.Filename = fd.Filename
			return p
		}
	}
	return nil
}

// buildMessageFromParts 用已构造的 A2A parts 建 Message（多模态发送），taskID/contextID 续跑同
// 远程 task。parts 空时兜底一个空 text part（A2A Message.Parts 不应为空）。
func buildMessageFromParts(parts a2atypes.ContentParts, taskID, contextID string) *a2atypes.Message {
	if len(parts) == 0 {
		parts = a2atypes.ContentParts{a2atypes.NewTextPart("")}
	}
	if taskID != "" || contextID != "" {
		return a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
			a2atypes.TaskInfo{TaskID: a2atypes.TaskID(taskID), ContextID: contextID}, parts...)
	}
	return a2atypes.NewMessage(a2atypes.MessageRoleUser, parts...)
}

// PartsToContent 把 A2A parts 转 schema 的 Content（拼接 text）+ ContentBlocks（raw/url/data
// → File/Data block）——a2aPartsText 的完整版，接收远程多模态应答 / server 入站多模态 query 用。
//
// **已知归一（audit LOW）**：A2A 只有 text/raw/url/data 4 类 Part，无 image/audio 判别——发送侧
// ImageUrl/InputAudio block 经 url/raw part 往返后归一为 File block（URL/bytes 内容无损，但
// typed 身份 image_url/input_audio → file）；多个 text part 拼进 Content 无分隔。内容无损，
// block 类型身份与跨类型顺序不保证。
// **model 消费限制（超本 wire 层 scope）**：A2A wire 完整传多模态，但 LLM 消费这些 block 需
// model 适配器支持——当前 model/openai 仅读 toolcall/toolresult，File/Data/ImageUrl block 喂
// LLM 待扩（host ReAct 收到多模态 A2A 响应时 block 留在 ContentBlocks 但不进下轮 LLM 请求）。
func PartsToContent(parts a2atypes.ContentParts) (string, []*schema.ContentBlock) {
	var sb strings.Builder
	var blocks []*schema.ContentBlock
	for _, p := range parts {
		if p == nil {
			continue
		}
		if t := p.Text(); t != "" {
			sb.WriteString(t)
			continue
		}
		if b := PartToBlock(p); b != nil {
			blocks = append(blocks, b)
		}
	}
	return sb.String(), blocks
}

// PartToBlock 把非文本 A2A Part 转 ContentBlock（raw→File inline base64 / url→File URL /
// data→Data）。文本 part 由 caller 拼进 Content，不走这里。
func PartToBlock(p *a2atypes.Part) *schema.ContentBlock {
	if raw := p.Raw(); raw != nil {
		return &schema.ContentBlock{
			Type:      string(schema.ContentBlockTypeFile),
			MediaType: p.MediaType,
			File:      &schema.FileContentBlock{Type: schema.ContentBlockTypeFile, File: &schema.FileData{FileData: base64.StdEncoding.EncodeToString(raw), Filename: p.Filename}},
		}
	}
	if u := p.URL(); u != "" {
		return &schema.ContentBlock{
			Type:      string(schema.ContentBlockTypeFile),
			MediaType: p.MediaType,
			File:      &schema.FileContentBlock{Type: schema.ContentBlockTypeFile, File: &schema.FileData{URL: string(u), Filename: p.Filename}},
		}
	}
	if d := p.Data(); d != nil {
		return &schema.ContentBlock{
			Type:      string(schema.ContentBlockTypeData),
			MediaType: firstNonEmpty(p.MediaType, "application/json"),
			Data:      &schema.DataContentBlock{Type: schema.ContentBlockTypeData, Data: d},
		}
	}
	return nil
}

// sanitizeName 把远程 AgentCard.Name 转 OpenAI 合规工具名（字母数字下划线，非法字符转 _）。
func sanitizeName(name string) string {
	if name == "" {
		return "a2a_remote_agent"
	}
	var sb strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			sb.WriteRune(unicode.ToLower(r))
		} else {
			sb.WriteRune('_')
		}
	}
	return sb.String()
}

// assistantWithBlocks 建带 ContentBlocks 的 assistant message（接收远程多模态应答用）——
// content 是拼接的文本，blocks 是 raw/url/data 等非文本 part。blocks 空时退化纯文本消息，
// 跟现有 schema.AssistantMessage 行为一致（不影响纯文本应答）。
func assistantWithBlocks(content string, blocks []*schema.ContentBlock) *schema.Message {
	m := schema.AssistantMessage(content)
	if len(blocks) > 0 {
		m.ContentBlocks = blocks
	}
	return m
}

// firstNonEmpty 返回第一个非空串。
func firstNonEmpty(s, fallback string) string {
	if strings.TrimSpace(s) != "" {
		return s
	}
	return fallback
}
