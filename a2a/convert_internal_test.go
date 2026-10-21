package a2a

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

// TestConvertRoundTrip 验 schema.Message ↔ A2A parts 双向多模态无损（text/raw/url/data +
// media_type/filename）——A2A 完整化核心保证。wire 字段对齐 A2A 新版 proto（Go/Java/Python
// 三方一致：bytes raw / string url / Value data + media_type），跨语言互通。
func TestConvertRoundTrip(t *testing.T) {
	imgBytes := []byte("\x89PNG fake image bytes")
	msg := &schema.Message{
		Content: "请看这张图和数据",
		ContentBlocks: []*schema.ContentBlock{
			{Type: string(schema.ContentBlockTypeFile), MediaType: "image/png",
				File: &schema.FileContentBlock{Type: schema.ContentBlockTypeFile,
					File: &schema.FileData{FileData: base64.StdEncoding.EncodeToString(imgBytes), Filename: "photo.png"}}},
			{Type: string(schema.ContentBlockTypeFile), MediaType: "application/pdf",
				File: &schema.FileContentBlock{Type: schema.ContentBlockTypeFile,
					File: &schema.FileData{URL: "https://example.com/doc.pdf", Filename: "doc.pdf"}}},
			{Type: string(schema.ContentBlockTypeData), MediaType: "application/json",
				Data: &schema.DataContentBlock{Type: schema.ContentBlockTypeData, Data: map[string]any{"temp": "22.5", "unit": "C"}}},
		},
	}

	parts := MessageToA2AParts(msg)
	if len(parts) != 4 { // text(Content) + raw(image) + url(pdf) + data
		t.Fatalf("期望 4 part（text/raw/url/data），got %d", len(parts))
	}

	// ── A2A Part 形态：raw(bytes)/url/data + media_type/filename ──
	var rawPart, urlPart, dataPart *a2atypes.Part
	for _, p := range parts {
		switch {
		case p.Raw() != nil:
			rawPart = p
		case p.URL() != "":
			urlPart = p
		case p.Data() != nil:
			dataPart = p
		}
	}
	if rawPart == nil || string(rawPart.Raw()) != string(imgBytes) || rawPart.MediaType != "image/png" || rawPart.Filename != "photo.png" {
		t.Errorf("raw part 错：%+v", rawPart)
	}
	if urlPart == nil || string(urlPart.URL()) != "https://example.com/doc.pdf" || urlPart.MediaType != "application/pdf" {
		t.Errorf("url part 错：%+v", urlPart)
	}
	if dataPart == nil || dataPart.MediaType != "application/json" {
		t.Errorf("data part 错：%+v", dataPart)
	}

	// ── JSON wire 跨语言对照：字段名 raw/url/data/mediaType（A2A 新版 proto，Java/Python 一致）──
	wire, err := json.Marshal(parts)
	if err != nil {
		t.Fatalf("marshal parts: %v", err)
	}
	ws := string(wire)
	for _, key := range []string{`"raw"`, `"url"`, `"data"`, `"mediaType"`} {
		if !strings.Contains(ws, key) {
			t.Errorf("wire JSON 缺字段 %s（A2A 新版 proto 跨语言字段）：%s", key, ws)
		}
	}

	// ── round-trip：parts → schema.Message 无损 ──
	content, blocks := PartsToContent(parts)
	if content != "请看这张图和数据" {
		t.Errorf("round-trip content 错：%q", content)
	}
	if len(blocks) != 3 { // raw/url/data（text 已拼进 content）
		t.Fatalf("round-trip 期望 3 block，got %d", len(blocks))
	}
	var gotRaw, gotURL, gotData *schema.ContentBlock
	for _, b := range blocks {
		switch {
		case b.Data != nil:
			gotData = b
		case b.File != nil && b.File.File.URL != "":
			gotURL = b
		case b.File != nil && b.File.File.FileData != "":
			gotRaw = b
		}
	}
	if gotRaw == nil || gotRaw.MediaType != "image/png" || gotRaw.File.File.Filename != "photo.png" {
		t.Errorf("round-trip raw block 错：%+v", gotRaw)
	} else if dec, _ := base64.StdEncoding.DecodeString(gotRaw.File.File.FileData); string(dec) != string(imgBytes) {
		t.Error("round-trip raw bytes 丢失（base64 解码 != 原 bytes）")
	}
	if gotURL == nil || gotURL.File.File.URL != "https://example.com/doc.pdf" || gotURL.MediaType != "application/pdf" {
		t.Errorf("round-trip url block 错：%+v", gotURL)
	}
	if gotData == nil || gotData.Data.Data == nil || gotData.MediaType != "application/json" {
		t.Errorf("round-trip data block 错：%+v", gotData)
	}
}

// TestConvertTextOnly 验纯文本 message（现有轮转主场景）双向不受多模态扩展影响。
func TestConvertTextOnly(t *testing.T) {
	parts := MessageToA2AParts(schema.UserMessage("你好"))
	if len(parts) != 1 || parts[0].Text() != "你好" {
		t.Fatalf("纯文本应 1 个 text part，got %+v", parts)
	}
	content, blocks := PartsToContent(parts)
	if content != "你好" || len(blocks) != 0 {
		t.Errorf("纯文本 round-trip 错：content=%q blocks=%d", content, len(blocks))
	}
}

// TestConvertNilDataSkipped 验 audit HIGH 修复：DataContentBlock 内部 nil 值不发 {"data":null}
// （A2A SDK UnmarshalJSON 把它判"无 content" → 拒绝整条 message，跨语言 hazard）——跳过该 block
// 不发毒 wire；falsy 非 nil 值（false/0/""）合法、正常发。
func TestConvertNilDataSkipped(t *testing.T) {
	// nil 内部值 → 跳过（只剩 text part，无 data part）
	parts := MessageToA2AParts(&schema.Message{
		Content: "answer",
		ContentBlocks: []*schema.ContentBlock{
			{Type: string(schema.ContentBlockTypeData), MediaType: "application/json",
				Data: &schema.DataContentBlock{Type: schema.ContentBlockTypeData, Data: nil}},
		},
	})
	if len(parts) != 1 || parts[0].Text() != "answer" {
		t.Fatalf("nil-data block 应跳过，只剩 text part，got %d parts", len(parts))
	}
	// 整条 message 序列化无毒 wire + A2A SDK 能反序列化（修复前 {"data":null} 会让整条 unmarshal 失败）
	wire, _ := json.Marshal(buildMessageFromParts(parts, "", ""))
	if strings.Contains(string(wire), `"data":null`) {
		t.Errorf("不应含毒 wire {\"data\":null}：%s", string(wire))
	}
	var rt a2atypes.Message
	if err := json.Unmarshal(wire, &rt); err != nil {
		t.Errorf("nil-data 修复后整条 message 应能反序列化，got err: %v", err)
	}

	// falsy 非 nil 值正常发（不跳过）——false/0/"" 是合法 data，A2A 应传
	for _, falsy := range []any{false, 0, ""} {
		p := MessageToA2AParts(&schema.Message{ContentBlocks: []*schema.ContentBlock{
			{Type: string(schema.ContentBlockTypeData), Data: &schema.DataContentBlock{Data: falsy}},
		}})
		if len(p) != 1 {
			t.Errorf("falsy 值 %v(%T) 应正常发 data part（合法非 nil），got %d parts", falsy, falsy, len(p))
		}
	}
}
