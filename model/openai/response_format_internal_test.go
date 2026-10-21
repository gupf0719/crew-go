package openai

import (
	"encoding/json"
	"testing"

	"crew-go/model"
)

// TestBuildRequest_ResponseFormat_JSONObject 验 WithJSONObject 映射进 wire
// response_format={type:"json_object"}（无 json_schema 子字段）。白盒测未导出 buildRequest。
func TestBuildRequest_ResponseFormat_JSONObject(t *testing.T) {
	c := NewClient("k", "m")
	req := c.buildRequest(nil, model.Apply(model.WithJSONObject()), false)
	if req.ResponseFormat == nil {
		t.Fatal("ResponseFormat nil; want json_object")
	}
	if req.ResponseFormat.Type != "json_object" {
		t.Errorf("Type = %q; want json_object", req.ResponseFormat.Type)
	}
	if req.ResponseFormat.JSONSchema != nil {
		t.Errorf("json_object 不应带 JSONSchema, got %+v", req.ResponseFormat.JSONSchema)
	}
	// wire 断言：确认序列化后的实际请求体（json tag + omitempty）
	rf := marshalField(t, req, "response_format")
	if rf["type"] != "json_object" {
		t.Errorf("wire response_format.type = %v; want json_object", rf["type"])
	}
	if _, has := rf["json_schema"]; has {
		t.Errorf("json_object omitempty 应省略 json_schema, got %v", rf["json_schema"])
	}
}

// TestBuildRequest_ResponseFormat_JSONSchema 验 WithJSONSchema 映射 name/strict/schema
// 完整过 wire（strict=true 必须发出、schema 本体不丢）。
func TestBuildRequest_ResponseFormat_JSONSchema(t *testing.T) {
	c := NewClient("k", "m")
	sc := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"name": map[string]any{"type": "string"}},
		"required":             []any{"name"},
		"additionalProperties": false,
	}
	req := c.buildRequest(nil, model.Apply(model.WithJSONSchema("person", sc, true)), false)
	if req.ResponseFormat == nil || req.ResponseFormat.JSONSchema == nil {
		t.Fatal("ResponseFormat/JSONSchema nil")
	}
	if req.ResponseFormat.Type != "json_schema" {
		t.Errorf("Type = %q; want json_schema", req.ResponseFormat.Type)
	}
	if js := req.ResponseFormat.JSONSchema; js.Name != "person" || !js.Strict {
		t.Errorf("Name=%q Strict=%v; want person/true", js.Name, js.Strict)
	}
	rf := marshalField(t, req, "response_format")
	jsw, ok := rf["json_schema"].(map[string]any)
	if !ok {
		t.Fatalf("wire 缺 json_schema 子对象, got %v", rf)
	}
	if jsw["name"] != "person" || jsw["strict"] != true {
		t.Errorf("wire json_schema name/strict 错: %v", jsw)
	}
	if jsw["schema"] == nil {
		t.Error("wire json_schema.schema 丢失")
	}
}

// TestBuildRequest_NoResponseFormat 验不设时 omitempty 省略 response_format（不污染默认）。
func TestBuildRequest_NoResponseFormat(t *testing.T) {
	c := NewClient("k", "m")
	req := c.buildRequest(nil, model.Apply(), false)
	if req.ResponseFormat != nil {
		t.Errorf("无 WithResponseFormat 时应为 nil, got %+v", req.ResponseFormat)
	}
	var wire map[string]any
	b, _ := json.Marshal(req)
	if err := json.Unmarshal(b, &wire); err != nil {
		t.Fatal(err)
	}
	if _, has := wire["response_format"]; has {
		t.Errorf("omitempty 应省略 response_format, got %s", b)
	}
}

// TestConvertResponse_Refusal 验 json_schema strict 拒答回填进 Message.Refusal（§4.6）。
func TestConvertResponse_Refusal(t *testing.T) {
	c := NewClient("k", "m")
	resp := &openaiResponse{
		Choices: []openaiChoice{{
			Message:      openaiMessage{Role: "assistant", Refusal: "I cannot comply."},
			FinishReason: "stop",
		}},
	}
	msg := c.convertResponse(resp)
	if msg.Refusal != "I cannot comply." {
		t.Errorf("Refusal = %q; want 'I cannot comply.'", msg.Refusal)
	}
}

// marshalField marshal req 后取出顶层字段 name 的 map（wire 断言 helper）。
func marshalField(t *testing.T, req *openaiRequest, name string) map[string]any {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(b, &wire); err != nil {
		t.Fatal(err)
	}
	m, ok := wire[name].(map[string]any)
	if !ok {
		t.Fatalf("wire 缺字段 %q, got %s", name, b)
	}
	return m
}
