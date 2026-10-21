package anthropic_test

import (
	"context"
	"encoding/base64"
	"slices"
	"strings"
	"testing"

	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

// sendOnce 用 opts 构造 Client，发一次 Generate，返回请求。
func sendOnce(t *testing.T, clientOpts []anthropic.ClientOption, msgs []*schema.Message, callOpts ...model.Option) captured {
	t.Helper()
	srv, rec := newServer(t, textReply)
	if msgs == nil {
		msgs = []*schema.Message{schema.UserMessage("q")}
	}
	if _, err := newClient(srv, clientOpts...).Generate(context.Background(), msgs, callOpts...); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return rec.at(t, 0)
}

// TestClientOptions 钉住每个构造期选项在请求上的效果。
func TestClientOptions(t *testing.T) {
	cases := []struct {
		name  string
		opts  []anthropic.ClientOption
		check func(t *testing.T, req captured)
	}{
		{"display=updates 带上 beta 头", []anthropic.ClientOption{anthropic.WithThinkingDisplay(anthropic.DisplayUpdates)},
			func(t *testing.T, req captured) {
				if got := str(req.body, "thinking", "display"); got != "updates" {
					t.Errorf("thinking.display = %q", got)
				}
				if !slices.Contains(betas(req.header), "thinking-display-updates-2026-08-18") {
					t.Errorf("anthropic-beta = %v", betas(req.header))
				}
			}},
		{"prefix_mismatch=error", []anthropic.ClientOption{anthropic.WithPrefixMismatch(anthropic.PrefixMismatchError)},
			func(t *testing.T, req captured) {
				if got := str(req.body, "thinking", "block_binding", "prefix_mismatch_behavior"); got != "error" {
					t.Errorf("prefix_mismatch_behavior = %q", got)
				}
			}},
		{"prefix_mismatch 不设：不发字段，仍带头", []anthropic.ClientOption{anthropic.WithPrefixMismatch(anthropic.PrefixMismatchUnset)},
			func(t *testing.T, req captured) {
				if got := canon(t, req.body["thinking"]); got != `{"type":"adaptive"}` {
					t.Errorf("thinking = %s", got)
				}
				if !slices.Contains(betas(req.header), "thinking-binding-controls-2026-08-01") {
					t.Errorf("anthropic-beta = %v", betas(req.header))
				}
			}},
		{"不发 thinking 配置", []anthropic.ClientOption{anthropic.WithoutThinkingConfig()},
			func(t *testing.T, req captured) {
				if _, ok := req.body["thinking"]; ok {
					t.Errorf("不该有 thinking：%s", canon(t, req.body["thinking"]))
				}
				if slices.Contains(betas(req.header), "thinking-binding-controls-2026-08-01") {
					t.Errorf("不发 thinking 时不该带绑定控制的头：%v", betas(req.header))
				}
			}},
		{"关回退", []anthropic.ClientOption{anthropic.WithFallbacks(false)},
			func(t *testing.T, req captured) {
				if _, ok := req.body["fallbacks"]; ok {
					t.Errorf("不该有 fallbacks")
				}
				if slices.Contains(betas(req.header), "server-side-fallback-2026-07-01") {
					t.Errorf("关了回退不该带它的头：%v", betas(req.header))
				}
			}},
		{"关 prompt cache", []anthropic.ClientOption{anthropic.WithPromptCaching(false)},
			func(t *testing.T, req captured) {
				if _, ok := req.body["cache_control"]; ok {
					t.Errorf("不该有 cache_control")
				}
			}},
		{"cache 1 小时", []anthropic.ClientOption{anthropic.WithCacheTTL(anthropic.CacheTTL1h)},
			func(t *testing.T, req captured) {
				if got := canon(t, req.body["cache_control"]); got != `{"ttl":"1h","type":"ephemeral"}` {
					t.Errorf("cache_control = %s", got)
				}
			}},
		{"effort", []anthropic.ClientOption{anthropic.WithEffort(anthropic.EffortHigh)},
			func(t *testing.T, req captured) {
				if got := canon(t, req.body["output_config"]); got != `{"effort":"high"}` {
					t.Errorf("output_config = %s", got)
				}
			}},
		{"max_tokens", []anthropic.ClientOption{anthropic.WithMaxTokens(1000)},
			func(t *testing.T, req captured) {
				if got := num(t, req.body, "max_tokens"); got != 1000 {
					t.Errorf("max_tokens = %d", got)
				}
			}},
		{"额外 beta 去重", []anthropic.ClientOption{anthropic.WithBetas("x-beta", "server-side-fallback-2026-07-01")},
			func(t *testing.T, req captured) {
				got := betas(req.header)
				if !slices.Contains(got, "x-beta") {
					t.Errorf("anthropic-beta = %v，缺 x-beta", got)
				}
				n := 0
				for _, b := range got {
					if b == "server-side-fallback-2026-07-01" {
						n++
					}
				}
				if n != 1 {
					t.Errorf("anthropic-beta = %v，回退的头出现 %d 次", got, n)
				}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.check(t, sendOnce(t, tc.opts, nil)) })
	}
}

// TestCallOptions 钉住按次选项：模型、max_tokens、采样参数、停止序列。
func TestCallOptions(t *testing.T) {
	req := sendOnce(t, nil, nil,
		model.WithModel("claude-sonnet-5-5"), model.WithMaxTokens(2048),
		model.WithTemperature(0.5), model.WithTopP(0.25), model.WithStop([]string{"END"}))
	if got := str(req.body, "model"); got != "claude-sonnet-5-5" {
		t.Errorf("model = %q", got)
	}
	if got := num(t, req.body, "max_tokens"); got != 2048 {
		t.Errorf("max_tokens = %d", got)
	}
	if got := str(req.body, "temperature"); got != "0.5" {
		t.Errorf("temperature = %q", got)
	}
	if got := str(req.body, "top_p"); got != "0.25" {
		t.Errorf("top_p = %q", got)
	}
	if got := canon(t, req.body["stop_sequences"]); got != `["END"]` {
		t.Errorf("stop_sequences = %s", got)
	}
}

// TestTools 钉住工具表：绑定的与按次的合并（同名按次的赢）、按名字排序、schema 原样（缺 type 补
// object）、Extra 里的 strict。
func TestTools(t *testing.T) {
	srv, rec := newServer(t, textReply)
	bound, err := newClient(srv).WithTools([]*schema.ToolInfo{
		{Name: "zeta", Desc: "最后", Params: map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}},
			"required": []any{"a"}, "additionalProperties": false}, Extra: map[string]any{anthropic.StrictToolExtraKey: true}},
		{Name: "alpha", Desc: "绑定的", Params: map[string]any{"properties": map[string]any{}}},
	})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	if _, err := bound.Generate(context.Background(), []*schema.Message{schema.UserMessage("q")},
		model.WithTools([]*schema.ToolInfo{{Name: "alpha", Desc: "按次的"}})); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := `[{"description":"按次的","input_schema":{"properties":{},"type":"object"},"name":"alpha"},` +
		`{"description":"最后","input_schema":{"additionalProperties":false,"properties":{"a":{"type":"string"}},"required":["a"],"type":"object"},"name":"zeta","strict":true}]`
	if got := canon(t, rec.at(t, 0).body["tools"]); got != want {
		t.Errorf("tools =\n%s\n期望\n%s", got, want)
	}
}

// TestWithTools_ReturnsCopy：WithTools 返回新 Client，原 Client 不带工具。
func TestWithTools_ReturnsCopy(t *testing.T) {
	srv, rec := newServer(t, textReply)
	c := newClient(srv)
	if _, err := c.WithTools([]*schema.ToolInfo{{Name: "f"}}); err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	if _, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("q")}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, ok := rec.at(t, 0).body["tools"]; ok {
		t.Errorf("原 Client 被 WithTools 改了")
	}
}

// TestToolChoice：required 降为不指定（Opus 5.5 对强制选工具 400）；none 原样发；没有工具时都不发。
func TestToolChoice(t *testing.T) {
	tools := model.WithTools([]*schema.ToolInfo{{Name: "f"}, {Name: "g"}})
	cases := []struct {
		name   string
		opts   []model.Option
		want   string // tool_choice 的规范 JSON，空串表示不该有
		nTools int
	}{
		{"required 不发", []model.Option{tools, model.WithToolChoice(schema.ToolChoiceRequired)}, "", 2},
		{"auto 不发", []model.Option{tools, model.WithToolChoice(schema.ToolChoiceAuto)}, "", 2},
		{"none 原样", []model.Option{tools, model.WithToolChoice(schema.ToolChoiceNone)}, `{"type":"none"}`, 2},
		{"没有工具不发", []model.Option{model.WithToolChoice(schema.ToolChoiceNone)}, "", 0},
		{"白名单滤光了也不发", []model.Option{tools, model.WithAllowedTools("h"), model.WithToolChoice(schema.ToolChoiceNone)}, "", 0},
		{"白名单只留命中的", []model.Option{tools, model.WithAllowedTools("g")}, "", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := sendOnce(t, nil, nil, tc.opts...)
			got := ""
			if v, ok := req.body["tool_choice"]; ok {
				got = canon(t, v)
			}
			if got != tc.want {
				t.Errorf("tool_choice = %q，期望 %q", got, tc.want)
			}
			if n := len(asSlice(req.body["tools"])); n != tc.nTools {
				t.Errorf("tools 有 %d 个，期望 %d", n, tc.nTools)
			}
		})
	}
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// TestExtraBody_MergePatch：构造期与按次的 ExtraBody 按 JSON merge patch 并进请求体——对象逐键合并
// （output_config 里的其他字段不丢），null 删除键。
func TestExtraBody_MergePatch(t *testing.T) {
	req := sendOnce(t,
		[]anthropic.ClientOption{
			anthropic.WithEffort(anthropic.EffortHigh),
			anthropic.WithExtraBody(map[string]any{
				"output_config": map[string]any{"task_budget": map[string]any{"type": "tokens", "total": 1000}},
				"metadata":      map[string]string{"user_id": "u1"},
			}),
		},
		nil,
		anthropic.WithEffortOption(anthropic.EffortLow),
		model.WithExtraBody(map[string]any{"fallbacks": nil}),
	)
	if got := canon(t, req.body["output_config"]); got != `{"effort":"low","task_budget":{"total":1000,"type":"tokens"}}` {
		t.Errorf("output_config = %s", got)
	}
	if got := str(req.body, "metadata", "user_id"); got != "u1" {
		t.Errorf("metadata.user_id = %q", got)
	}
	if _, ok := req.body["fallbacks"]; ok {
		t.Errorf("fallbacks 应被 null 删掉")
	}
	if got := str(req.body, "thinking", "type"); got != "adaptive" {
		t.Errorf("没被 patch 的字段应保留：thinking.type = %q", got)
	}
}

// TestResponseFormat：json_schema 发 output_config.format；json_object 没有原生机制，明确报错且不发请求。
func TestResponseFormat(t *testing.T) {
	schemaMap := map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "integer"}},
		"required": []any{"x"}, "additionalProperties": false}
	req := sendOnce(t, nil, nil, model.WithJSONSchema("out", schemaMap, true))
	want := `{"format":{"schema":{"additionalProperties":false,"properties":{"x":{"type":"integer"}},"required":["x"],"type":"object"},"type":"json_schema"}}`
	if got := canon(t, req.body["output_config"]); got != want {
		t.Errorf("output_config = %s", got)
	}

	srv, rec := newServer(t, textReply)
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")}, model.WithJSONObject())
	if err == nil || !strings.Contains(err.Error(), "json_object") {
		t.Fatalf("err = %v，期望 json_object 不支持", err)
	}
	if n := rec.count(); n != 0 {
		t.Errorf("出错时不该发请求，发了 %d 次", n)
	}
}

// TestUserContentBlocks：user 消息里的多模态块——data URL 图片发 base64，http 图片发 url，PDF 与纯文本
// 文件发 document，结构化数据发 JSON 文本。
func TestUserContentBlocks(t *testing.T) {
	msg := &schema.Message{Role: schema.RoleUser, Content: "看看这些", ContentBlocks: []*schema.ContentBlock{
		{Type: "image_url", ImageUrl: &schema.ImageUrlContentBlock{ImageUrl: &schema.ImageUrl{Url: "data:image/png;base64,iVBOR"}}},
		{Type: "image_url", ImageUrl: &schema.ImageUrlContentBlock{ImageUrl: &schema.ImageUrl{Url: "https://example.com/a.jpg"}}},
		{Type: "file", MediaType: "application/pdf", File: &schema.FileContentBlock{File: &schema.FileData{FileData: "JVBERi0=", Filename: "a.pdf"}}},
		{Type: "file", MediaType: "text/plain", File: &schema.FileContentBlock{File: &schema.FileData{FileData: base64.StdEncoding.EncodeToString([]byte("纯文本"))}}},
		{Type: "data", Data: &schema.DataContentBlock{Data: map[string]any{"k": 1}}},
	}}
	req := sendOnce(t, nil, []*schema.Message{msg})
	want := `[{"content":[` +
		`{"text":"看看这些","type":"text"},` +
		`{"source":{"data":"iVBOR","media_type":"image/png","type":"base64"},"type":"image"},` +
		`{"source":{"type":"url","url":"https://example.com/a.jpg"},"type":"image"},` +
		`{"source":{"data":"JVBERi0=","media_type":"application/pdf","type":"base64"},"title":"a.pdf","type":"document"},` +
		`{"source":{"data":"纯文本","media_type":"text/plain","type":"text"},"type":"document"},` +
		`{"text":"{\"k\":1}","type":"text"}` +
		`],"role":"user"}]`
	if got := canon(t, req.body["messages"]); got != want {
		t.Errorf("messages =\n%s\n期望\n%s", got, want)
	}
}

// TestUserContentBlocks_Unsupported：Anthropic 不支持的块报错，不静默丢掉用户给的内容，也不发请求。
func TestUserContentBlocks_Unsupported(t *testing.T) {
	srv, rec := newServer(t, textReply)
	msg := &schema.Message{Role: schema.RoleUser, ContentBlocks: []*schema.ContentBlock{
		{Type: "input_audio", InputAudio: &schema.InputAudioContentBlock{InputAudio: &schema.InputAudio{Format: "wav", Data: "AAA"}}},
	}}
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{msg})
	if err == nil || !strings.Contains(err.Error(), "input_audio") {
		t.Fatalf("err = %v，期望 input_audio 不支持", err)
	}
	if n := rec.count(); n != 0 {
		t.Errorf("出错时不该发请求，发了 %d 次", n)
	}
}

// TestStructuredCapability 钉住自报的结构化输出能力。
func TestStructuredCapability(t *testing.T) {
	got := anthropic.NewClient("k", "").StructuredCapability("")
	want := model.StructuredCapability{SupportsJSONSchema: true, SupportsJSONObject: false,
		SupportsToolChoiceRequired: false, SupportsStrictTool: true, DefaultMode: model.ModeTool}
	if got != want {
		t.Errorf("StructuredCapability = %+v，期望 %+v", got, want)
	}
}
