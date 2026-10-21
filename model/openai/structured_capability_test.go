package openai_test

import (
	"testing"

	"crew-go/model"
	"crew-go/model/openai"
)

// capOf 类型断言 client 实现 model.StructuredCapable 并取其能力画像。
func capOf(t *testing.T, m model.ChatModel) model.StructuredCapability {
	t.Helper()
	sc, ok := m.(model.StructuredCapable)
	if !ok {
		t.Fatal("client 未实现 model.StructuredCapable")
	}
	return sc.StructuredCapability("")
}

// TestStructuredCapability_DefaultOpenAI 验 NewClient 默认报官方 OpenAI 全 strict 能力。
func TestStructuredCapability_DefaultOpenAI(t *testing.T) {
	got := capOf(t, openai.NewClient("k", "gpt-4o"))
	if !got.SupportsJSONSchema || !got.SupportsStrictTool {
		t.Errorf("官方默认应支持 json_schema + strict tool, got %+v", got)
	}
	if got.DefaultMode != model.ModeResponseFormat {
		t.Errorf("DefaultMode = %v; want response_format", got.DefaultMode)
	}
}

// TestStructuredCapability_Override 验 WithStructuredCapability 覆盖默认。
func TestStructuredCapability_Override(t *testing.T) {
	custom := model.StructuredCapability{SupportsJSONObject: true, DefaultMode: model.ModeJSONObject}
	got := capOf(t, openai.NewClient("k", "m", openai.WithStructuredCapability(custom)))
	if got.SupportsJSONSchema {
		t.Error("override 后不应仍报 json_schema")
	}
	if got.DefaultMode != model.ModeJSONObject {
		t.Errorf("DefaultMode = %v; want json_object", got.DefaultMode)
	}
}

// TestNewCompatible_ConservativeDefault 验 Preset 无 capability 时非 openai provider 保守
// 默认（非 strict + ModeTool），避免误判 strict 发 400。
func TestNewCompatible_ConservativeDefault(t *testing.T) {
	p := openai.Preset{System: "unknown", BaseURL: "http://x", DefaultModel: "m"}
	got := capOf(t, openai.NewCompatible(p, "k", ""))
	if got.SupportsJSONSchema {
		t.Error("未知 provider 不应默认支持 json_schema strict")
	}
	if got.DefaultMode != model.ModeTool {
		t.Errorf("DefaultMode = %v; want tool", got.DefaultMode)
	}
}

// TestNewCompatible_PresetCapability 验 Preset 显式 capability 生效。
func TestNewCompatible_PresetCapability(t *testing.T) {
	strict := model.DefaultOpenAICapability()
	p := openai.Preset{System: "s", BaseURL: "http://x", DefaultModel: "m", StructuredCapability: &strict}
	got := capOf(t, openai.NewCompatible(p, "k", ""))
	if !got.SupportsJSONSchema || got.DefaultMode != model.ModeResponseFormat {
		t.Errorf("Preset capability 未生效, got %+v", got)
	}
}

// TestNewCompatible_OptsOverrideCapability 验业务经 opts 传 WithStructuredCapability 覆盖
// Preset（后值赢——sanity 依赖 NewCompatible 里 preset cap 排在 opts 之前）。
func TestNewCompatible_OptsOverrideCapability(t *testing.T) {
	presetCap := model.NonStrictToolCapability()
	p := openai.Preset{System: "s", BaseURL: "http://x", DefaultModel: "m", StructuredCapability: &presetCap}
	override := model.DefaultOpenAICapability() // 业务应对 provider 演进：现在支持 strict 了
	got := capOf(t, openai.NewCompatible(p, "k", "", openai.WithStructuredCapability(override)))
	if !got.SupportsJSONSchema {
		t.Error("opts 覆盖应赢过 Preset（后值赢）")
	}
}
