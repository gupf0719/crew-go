package deepseek_test

import (
	"testing"

	"crew-go/model"
	"crew-go/model/deepseek"
)

// TestDeepseekStructuredCapability 验 DeepSeek client 自报非 strict 能力：明确不支持
// json_schema（官方拒），走 json_object + 覆盖最广的 ModeTool。构造仅装配、不打网络。
func TestDeepseekStructuredCapability(t *testing.T) {
	m := deepseek.NewClient("k", "")
	sc, ok := m.(model.StructuredCapable)
	if !ok {
		t.Fatal("deepseek client 未实现 model.StructuredCapable")
	}
	got := sc.StructuredCapability("deepseek-v4-flash")
	if got.SupportsJSONSchema {
		t.Error("deepseek 明确拒 json_schema, 不应报支持")
	}
	if !got.SupportsJSONObject || got.DefaultMode != model.ModeTool {
		t.Errorf("deepseek 应 json_object + ModeTool, got %+v", got)
	}
}
