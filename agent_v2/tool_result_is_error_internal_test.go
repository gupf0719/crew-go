package agent_v2

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// TestFailedToolResult_SetsIsError：框架合成的失败结果在 tool_result 块上置 IsError——原生支持失败
// 标记的 provider（Anthropic 的 is_error）据它发 wire 字段；只打 annotation 的话模型那边看不到。
func TestFailedToolResult_SetsIsError(t *testing.T) {
	m := failedToolResult("call_1", "tool_timeout", "超时")
	if len(m.ContentBlocks) != 1 || m.ContentBlocks[0].Toolresult == nil || !m.ContentBlocks[0].Toolresult.IsError {
		t.Fatalf("失败结果的 tool_result 块没置 IsError：%+v", m.ContentBlocks)
	}
	ok := schema.ToolResultMessage("call_2", "正常")
	if ok.ContentBlocks[0].Toolresult.IsError {
		t.Fatalf("正常结果不该置 IsError")
	}
}

// isErrorVerdictHooks 在 OnToolEnd 里把结论改成 success。
type isErrorVerdictHooks struct {
	hook.NoOpHooks
	success bool
}

func (h *isErrorVerdictHooks) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	out.Result.Success = h.success
	if h.success {
		out.Result.Output = "修好了"
		out.Result.Error, out.Result.ErrorCode = "", ""
	} else {
		out.Result.Error, out.Result.ErrorCode = "钩子判失败", "hook_failed"
	}
	return ctx, nil
}

// TestToolEndHook_IsErrorFollowsVerdict：OnToolEnd 改了结论时，IsError 跟着模型看到的结论走——失败改成
// 成功要清掉，成功改成失败要置上。只改 Content 不改 IsError 的话，模型看到「成功的内容 + 失败标记」。
func TestToolEndHook_IsErrorFollowsVerdict(t *testing.T) {
	cases := []struct {
		name      string
		toolFails bool // 工具本身的结果是失败
		hookOK    bool // 钩子改成的结论
		want      bool // 模型第 2 轮看到的 IsError
	}{
		{"失败改成成功", true, true, false},
		{"成功改成失败", false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seen *schema.ToolresultContentBlock
			round := 0
			model := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
				round++
				if round == 1 {
					return schema.AssistantToolCallMessage([]schema.ToolCall{{
						ID: "call_1", Type: "function", Function: schema.FunctionCall{Name: "t", Arguments: "{}"},
					}}), nil
				}
				for _, m := range msgs {
					if m.Role == schema.RoleTool && len(m.ContentBlocks) > 0 {
						seen = m.ContentBlocks[0].Toolresult
					}
				}
				return schema.AssistantMessage("done"), nil
			}
			tools := func(_ context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
				if tc.toolFails {
					return []*schema.Message{failedToolResult(tcs[0].ID, "boom", "工具失败")}, nil
				}
				return []*schema.Message{schema.ToolResultMessage(tcs[0].ID, "正常")}, nil
			}
			g, err := NewBasicAgentBuilder(model, tools).WithAgentHooks(&isErrorVerdictHooks{success: tc.hookOK}).Compile()
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
				compose_v2.WithInitialWrites(BasicAgentInputWrites(schema.UserMessage("hi"))...),
			); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if seen == nil {
				t.Fatalf("模型第 2 轮没看到 tool_result")
			}
			if seen.IsError != tc.want {
				t.Errorf("模型看到 IsError=%v（内容 %q），期望 %v", seen.IsError, seen.Content, tc.want)
			}
		})
	}
}

// TestInputDigest_CountsIsError：失败标记是输入的一部分——只差 IsError 的两条消息摘要不同；没置上时
// 不写入，加这个字段之前的存档摘要不变。
func TestInputDigest_CountsIsError(t *testing.T) {
	msg := func(isErr bool) *schema.Message {
		return &schema.Message{Role: schema.RoleTool, ContentBlocks: []*schema.ContentBlock{{
			Type: "tool_result", Toolresult: &schema.ToolresultContentBlock{ToolCallId: "c", Content: "x", IsError: isErr},
		}}}
	}
	if inputDigest(msg(true)) == inputDigest(msg(false)) {
		t.Errorf("只差 IsError 的两条消息算出了同一个摘要")
	}
}
