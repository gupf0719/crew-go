package qwen_test

import (
	"testing"

	"crew-go/model"
	"crew-go/model/qwen"
)

// TestQwenStructuredCapability 验千问 client（NewClient + NewTestClient）自报非 strict 能力：
// 不支持 json_schema（dashscope qwen3 官方未确认 / flash 大概率无），走 json_object +
// 覆盖最广的 ModeTool。构造仅装配、不打网络。
func TestQwenStructuredCapability(t *testing.T) {
	for name, m := range map[string]model.ChatModel{
		"NewClient":     qwen.NewClient("k", ""),
		"NewTestClient": qwen.NewTestClient(""),
	} {
		sc, ok := m.(model.StructuredCapable)
		if !ok {
			t.Fatalf("%s: qwen client 未实现 model.StructuredCapable", name)
		}
		got := sc.StructuredCapability("qwen3.6-flash")
		if got.SupportsJSONSchema {
			t.Errorf("%s: qwen 不应报支持 json_schema strict", name)
		}
		if !got.SupportsJSONObject || got.DefaultMode != model.ModeTool {
			t.Errorf("%s: qwen 应 json_object + ModeTool, got %+v", name, got)
		}
	}
}
