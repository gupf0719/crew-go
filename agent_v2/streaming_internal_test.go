package agent_v2

import (
	"context"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

// 内部 white-box 测试——直接验 streamOrGenerate 而非走完整 BasicAgent.Run
// （后者需要 toolkit 等 setup，dot test 太多 noise）

// TestRepro_R7_Bug5_EmptyToolCallIDDoesNotMerge_Internal 验证 audit Round 7 Bug 5：
// streamOrGenerate 用 cb.Toolcall.Id 直接作 toolCallByID map key，empty ID 时
// 多个不同 tool_call 串台合并成 1 个 args 乱码。
//
// 内部测试：直接调 streamOrGenerate（agent_v2 包内私有函数）。
//
// **预期行为**：2 个 empty-ID tool_call 不合并（按位置作隐式 key）。
//
// **当前 bug**（修前）：合并成 1 个 entry，args="{"x":1}{"y":2}" 串台。
func TestRepro_R7_Bug5_EmptyToolCallIDDoesNotMerge_Internal(t *testing.T) {
	mkChunk := func(name, args string) *schema.Message {
		return &schema.Message{
			Role: schema.RoleAssistant,
			ContentBlocks: []*schema.ContentBlock{{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id:        "", // 空 ID 触发 Bug 5
					Name:      name,
					Arguments: args,
				},
			}},
		}
	}
	stub := &incrModel{
		chunks: []*schema.Message{
			mkChunk("tool_a", `{"x":1}`),
			mkChunk("tool_b", `{"y":2}`),
		},
	}

	out, err := streamOrGenerate(context.Background(), stub, nil, "test", "rid_bug5", nil, nil)
	if err != nil {
		t.Fatalf("streamOrGenerate: %v", err)
	}

	calls := schema.GetToolCalls(out)
	if len(calls) != 2 {
		argsList := make([]string, len(calls))
		for i, c := range calls {
			argsList[i] = c.Function.Arguments
		}
		t.Fatalf("Bug 5 reproduced: 2 个 empty-ID tool_call 合并成 1 个\n"+
			"期望 2 个独立 tool_call，实际 %d 个 (args=%v)\n"+
			"修复方向：empty id chunk 用 fmt.Sprintf(\"__pos_%%d__\", len(toolCallIDs)) 隐式 key",
			len(calls), argsList)
	}
}

type incrModel struct {
	chunks []*schema.Message
}

func (m *incrModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(""), nil
}
func (m *incrModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](len(m.chunks))
	for _, c := range m.chunks {
		send(c)
	}
	schema.CloseSend(sr)
	return sr, nil
}
func (m *incrModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestStreamOrGenerate_IncrementalToolCallAccumulation 验证 audit Bug #5 修复：
// OpenAI 流式 tool_call 是 incremental——3 chunks 同 ID 含 partial JSON args
// `{"que` + `ry":` + ` "go scheduler"}` —— 按 ID 累加得 final
// `{"query": "go scheduler"}` 不丢前面 partial。
//
// sanity-revert：basic_agent.go 改回 lastChunk.ContentBlocks 模式，args 只剩最后
// 一个 chunk → 测试 fail（args != wantArgs）。
func TestStreamOrGenerate_IncrementalToolCallAccumulation(t *testing.T) {
	mkChunk := func(args string) *schema.Message {
		return &schema.Message{
			Role: schema.RoleAssistant,
			ContentBlocks: []*schema.ContentBlock{{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id:        "tc_streaming",
					Name:      "search",
					Arguments: args,
				},
			}},
		}
	}
	stub := &incrModel{
		chunks: []*schema.Message{
			mkChunk(`{"que`),
			mkChunk(`ry":`),
			mkChunk(` "go scheduler"}`),
		},
	}

	out, err := streamOrGenerate(context.Background(), stub, nil, "test", "test_run_id", nil, nil)
	if err != nil {
		t.Fatalf("streamOrGenerate: %v", err)
	}

	calls := schema.GetToolCalls(out)
	if len(calls) != 1 {
		t.Fatalf("expected 1 tool_call, got %d", len(calls))
	}
	wantArgs := `{"query": "go scheduler"}`
	if calls[0].Function.Arguments != wantArgs {
		t.Fatalf("incremental args accumulation failed:\n  expected %q\n  got      %q\n"+
			"sanity-revert：streamOrGenerate 改回 lastChunk.ContentBlocks 模式，args 只剩最后一个 chunk",
			wantArgs, calls[0].Function.Arguments)
	}
	if calls[0].Function.Name != "search" {
		t.Fatalf("expected name 'search', got %q", calls[0].Function.Name)
	}
}

// TestStreamOrGenerate_MultipleToolCallsByID 验证多个不同 ID 的 tool_call 各自
// 按 ID 累加不串台。
func TestStreamOrGenerate_MultipleToolCallsByID(t *testing.T) {
	mkChunk := func(id, args string) *schema.Message {
		return &schema.Message{
			Role: schema.RoleAssistant,
			ContentBlocks: []*schema.ContentBlock{{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id:        id,
					Name:      "tool_" + id,
					Arguments: args,
				},
			}},
		}
	}
	stub := &incrModel{
		chunks: []*schema.Message{
			mkChunk("tc_a", `{"x`),
			mkChunk("tc_b", `{"y`),
			mkChunk("tc_a", `":1}`),
			mkChunk("tc_b", `":2}`),
		},
	}

	out, err := streamOrGenerate(context.Background(), stub, nil, "test", "test_run_id_2", nil, nil)
	if err != nil {
		t.Fatalf("streamOrGenerate: %v", err)
	}

	calls := schema.GetToolCalls(out)
	if len(calls) != 2 {
		t.Fatalf("expected 2 tool_calls, got %d", len(calls))
	}
	// 按插入顺序：tc_a 先（首次见 tc_a 早于 tc_b），tc_b 后
	if calls[0].ID != "tc_a" || calls[0].Function.Arguments != `{"x":1}` {
		t.Fatalf("tc_a args wrong: %+v", calls[0])
	}
	if calls[1].ID != "tc_b" || calls[1].Function.Arguments != `{"y":2}` {
		t.Fatalf("tc_b args wrong: %+v", calls[1])
	}
}
