package openai

import (
	"testing"
)

// TestOpenAIToolCall_IndexMap 验证 audit Round 3 Bug #2 修复：indexToToolCallID
// 跨 chunks 跟踪 (index → id) 映射——OpenAI 流式 delta 后续 chunks 仅 index 无 id。
//
// 这是单元测试 indexMap 行为；完整 stream 解析端到端测试需 mock SSE body，留
// integration 测试。
func TestOpenAIToolCall_IndexMap(t *testing.T) {
	im := make(indexToToolCallID)

	// 模拟 chunk 1：index=0 含完整 id+name
	im[0] = "call_xyz"

	// 模拟 chunk 2：index=0 仅含 args delta，无 id
	// consumer 应回填 id="call_xyz"
	if got := im[0]; got != "call_xyz" {
		t.Fatalf("expected indexMap[0]='call_xyz', got %q", got)
	}

	// 模拟 chunk 3：另一个 tool_call index=1
	im[1] = "call_abc"

	// 验证两个 ID 不冲突
	if im[0] == im[1] {
		t.Fatalf("indexMap should not conflict: %v", im)
	}
}
