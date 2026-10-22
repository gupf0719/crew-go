package observability

import (
	"encoding/json"
	"strings"
	"sync"
	"unicode/utf8"

	"crew-go/schema"
)

// redactString 把命中 WithRedactPatterns 的片段换成 [REDACTED]。没配时原样返回。
func (o *Observability) redactString(s string) string {
	for _, re := range o.redact {
		s = re.ReplaceAllString(s, "[REDACTED]")
	}
	return s
}

// truncate 按字符截断，截在完整字符边界上。max <= 0 不截。
func truncate(s string, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max {
			return s[:i] + "…[truncated]"
		}
		n++
	}
	return s
}

// text 是写进内容属性之前对每段文本的处理：脱敏、截断。
func (o *Observability) text(s string) string {
	return truncate(o.redactString(s), o.maxContent)
}

// jsonText 处理可能是 JSON 的文本（工具参数、工具结果）：合法 JSON 先解码再编码一遍，把 \uXXXX 转义还原成
// 字符再脱敏——模型常把中文写成转义形式，正则在转义文本上匹配不到，读的人解码后看到的却是明文。
func (o *Observability) jsonText(s string) string {
	return o.text(normalizeJSON(s))
}

// normalizeJSON 把合法 JSON 规整成不转义的形式；不是合法 JSON 时原样返回。
func normalizeJSON(s string) string {
	if !json.Valid([]byte(s)) {
		return s
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return s
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return s
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// 下面几个类型按 GenAI 语义约定的 JSON 结构（gen_ai.input.messages 等）编码消息。
type msgPart struct {
	Type      string `json:"type"`
	Content   string `json:"content,omitempty"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments any    `json:"arguments,omitempty"`
	Response  any    `json:"response,omitempty"`
	Modality  string `json:"modality,omitempty"`
	MimeType  string `json:"mime_type,omitempty"`
	URI       string `json:"uri,omitempty"`
}

type chatMsg struct {
	Role         string    `json:"role"`
	Parts        []msgPart `json:"parts"`
	FinishReason string    `json:"finish_reason,omitempty"`
}

// rawJSONOrString 让合法 JSON 的工具参数按对象写出，不合法的按字符串写出。
func rawJSONOrString(s string) any {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	return s
}

func (o *Observability) parts(m *schema.Message) []msgPart {
	var ps []msgPart
	if m.ReasoningContent != "" {
		ps = append(ps, msgPart{Type: "reasoning", Content: o.text(m.ReasoningContent)})
	}
	if m.Role == schema.RoleTool {
		ps = append(ps, msgPart{Type: "tool_call_response", ID: schema.GetToolResultID(m), Response: o.jsonText(m.Content)})
		return ps
	}
	if m.Content != "" {
		ps = append(ps, msgPart{Type: "text", Content: o.text(m.Content)})
	}
	ps = append(ps, o.blockParts(m)...)
	for _, tc := range schema.GetToolCalls(m) {
		ps = append(ps, msgPart{Type: "tool_call", ID: tc.ID, Name: tc.Function.Name,
			Arguments: rawJSONOrString(o.jsonText(tc.Function.Arguments))})
	}
	if ps == nil {
		ps = []msgPart{}
	}
	return ps
}

// blockParts 把多模态内容块编码成 GenAI 消息 part：文本块 → text；图片 / 音频 / 文件有 URL 时 → uri，
// 内联数据（data: URI、base64）→ blob；结构化数据 → type=data 的通用 part。工具调用 / 结果块走
// tool_call / tool_call_response，这里跳过。内联数据同样按 WithMaxContentChars 截断。
func (o *Observability) blockParts(m *schema.Message) []msgPart {
	var ps []msgPart
	for _, cb := range m.ContentBlocks {
		if cb == nil {
			continue
		}
		switch {
		case cb.Text != nil && cb.Text.Text != "":
			ps = append(ps, msgPart{Type: "text", Content: o.text(cb.Text.Text)})
		case cb.ImageUrl != nil && cb.ImageUrl.ImageUrl != nil && cb.ImageUrl.ImageUrl.Url != "":
			ps = append(ps, o.mediaPart(cb.ImageUrl.ImageUrl.Url, cb.MediaType, "image"))
		case cb.InputAudio != nil && cb.InputAudio.InputAudio != nil:
			mime := cb.MediaType
			if mime == "" && cb.InputAudio.InputAudio.Format != "" {
				mime = "audio/" + cb.InputAudio.InputAudio.Format
			}
			ps = append(ps, msgPart{Type: "blob", Modality: "audio", MimeType: mime, Content: o.text(cb.InputAudio.InputAudio.Data)})
		case cb.File != nil && cb.File.File != nil:
			f := cb.File.File
			def := modalityOfMime(cb.MediaType, "document")
			if f.URL != "" {
				ps = append(ps, o.mediaPart(f.URL, cb.MediaType, def))
			} else {
				ps = append(ps, msgPart{Type: "blob", Modality: def, MimeType: cb.MediaType, Content: o.text(f.FileData)})
			}
		case cb.Data != nil:
			if b, err := json.Marshal(cb.Data.Data); err == nil {
				ps = append(ps, msgPart{Type: "data", Content: o.text(string(b))})
			}
		}
	}
	return ps
}

// mediaPart 按 URL 形态编码一个媒体块：data: URI 拆成 blob（mime 取自 URI），其余记成 uri。
func (o *Observability) mediaPart(url, mime, defModality string) msgPart {
	if rest, ok := strings.CutPrefix(url, "data:"); ok {
		meta, data, _ := strings.Cut(rest, ",")
		if mt, _, _ := strings.Cut(meta, ";"); mt != "" && mime == "" {
			mime = mt
		}
		return msgPart{Type: "blob", Modality: modalityOfMime(mime, defModality), MimeType: mime, Content: o.text(data)}
	}
	return msgPart{Type: "uri", Modality: modalityOfMime(mime, defModality), MimeType: mime, URI: o.text(url)}
}

// modalityOfMime 从 MIME 类型推 GenAI 的 modality（image / audio / video / document），推不出时用 def。
func modalityOfMime(mime, def string) string {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return "image"
	case strings.HasPrefix(mime, "audio/"):
		return "audio"
	case strings.HasPrefix(mime, "video/"):
		return "video"
	case mime == "application/pdf" || strings.HasPrefix(mime, "text/"):
		return "document"
	}
	return def
}

// tokenModality 是一组消息对应 token 的 gen_ai.token.modality：全是文本时 text；含图片 / 音频 / 文件块时
// 供应商给的总数里混着别的模态、分不出来，按 GenAI 约定记 unknown。
func tokenModality(groups ...[]*schema.Message) string {
	for _, msgs := range groups {
		for _, m := range msgs {
			if m == nil {
				continue
			}
			for _, cb := range m.ContentBlocks {
				if cb != nil && (cb.ImageUrl != nil || cb.InputAudio != nil || cb.File != nil) {
					return "unknown"
				}
			}
		}
	}
	return "text"
}

// messagesJSON 把一组消息编码成 gen_ai.input.messages / gen_ai.output.messages 的值。
func (o *Observability) messagesJSON(msgs []*schema.Message, finishReason string) string {
	out := make([]chatMsg, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}
		out = append(out, chatMsg{Role: string(m.Role), Parts: o.parts(m), FinishReason: finishReason})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(b)
}

// systemJSON 把前言里的 system 消息编码成 gen_ai.system_instructions 的值（只有 parts，没有 role）。
func (o *Observability) systemJSON(msgs []*schema.Message) string {
	var ps []msgPart
	for _, m := range msgs {
		if m != nil && m.Content != "" {
			ps = append(ps, msgPart{Type: "text", Content: o.text(m.Content)})
		}
	}
	if len(ps) == 0 {
		return ""
	}
	b, err := json.Marshal(ps)
	if err != nil {
		return ""
	}
	return string(b)
}

// lastUserMessage 倒序找最后一条 user 消息——agent 这次调用真正收到的输入。
func lastUserMessage(msgs []*schema.Message) *schema.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil && msgs[i].Role == schema.RoleUser {
			return msgs[i]
		}
	}
	return nil
}

// lastAssistantMessage 倒序找最后一条 assistant 消息——这次调用的最终答复。跳过框架合成的记账文本
// （「此前发起过以下工具调用，但历史中没有它们的结果」）：它是「assistant + 零 tool_call」，长得
// 正像最终答复，而出事的恰好是模型没产出就退出的错误路径——正是有人看 trace 排障的时候。
func lastAssistantMessage(msgs []*schema.Message) *schema.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if m := msgs[i]; m != nil && m.Role == schema.RoleAssistant && !isUnresolvedCallsRecord(m) {
			return m
		}
	}
	return nil
}

// boundedBuffer 攒流式片段，到上限后不再追加（只用来在中断 / 出错时看看模型已经吐出了什么）。
type boundedBuffer struct {
	mu  sync.Mutex
	b   strings.Builder
	max int
}

func (b *boundedBuffer) append(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max > 0 && b.b.Len() >= b.max {
		return
	}
	b.b.WriteString(s)
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
