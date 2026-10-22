package openai

import "testing"

// TestConvertResponse_System 验 WithSystem 把 provider 名填进 Message.Metadata.System
// (gen_ai.provider.name observability span attr 的数据来源)。白盒测未导出的 convertResponse。
func TestConvertResponse_System(t *testing.T) {
	c := NewClient("k", "m", WithSystem("dashscope"))
	resp := &openaiResponse{
		Model: "qwen3.6-flash",
		Choices: []openaiChoice{{
			Message:      openaiMessage{Role: "assistant", Content: "hi"},
			FinishReason: "stop",
		}},
	}
	msg := c.convertResponse(resp)
	if msg.Metadata == nil {
		t.Fatal("Metadata nil")
	}
	if msg.Metadata.System != "dashscope" {
		t.Errorf("Metadata.System = %q; want dashscope", msg.Metadata.System)
	}
	if msg.Metadata.Model != "qwen3.6-flash" {
		t.Errorf("Metadata.Model = %q; want qwen3.6-flash", msg.Metadata.Model)
	}
}

// TestConvertResponse_SystemOnlyTriggersMetadata 是 condition 变更的 sanity 测:
// resp 无 model/usage/finish, 仅靠 c.system != "" 进 Metadata 创建分支。
// (revert `|| c.system != ""` 后本测 Metadata 为 nil → fail, 证明改动真生效。)
func TestConvertResponse_SystemOnlyTriggersMetadata(t *testing.T) {
	c := NewClient("k", "m", WithSystem("openai"))
	resp := &openaiResponse{
		Choices: []openaiChoice{{Message: openaiMessage{Role: "assistant", Content: "x"}}}, // 无 model/usage/finish
	}
	msg := c.convertResponse(resp)
	if msg.Metadata == nil || msg.Metadata.System != "openai" {
		t.Fatalf("system 非空应创建 Metadata 并填 System=openai, got %+v", msg.Metadata)
	}
}

// TestConvertResponse_NoSystem 验无 WithSystem 时 System 空 (不污染默认行为)。
func TestConvertResponse_NoSystem(t *testing.T) {
	c := NewClient("k", "m")
	resp := &openaiResponse{
		Model:   "gpt-4o",
		Choices: []openaiChoice{{Message: openaiMessage{Role: "assistant"}, FinishReason: "stop"}},
	}
	msg := c.convertResponse(resp)
	if msg.Metadata != nil && msg.Metadata.System != "" {
		t.Errorf("无 WithSystem 时 System 应空, got %q", msg.Metadata.System)
	}
}
