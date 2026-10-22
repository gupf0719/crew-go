package agent_v2_test

// 开了转存时 OnToolEnd 改写结果之后：按大小处理照样作用在改写之后的那份上，消息上的失败标注跟着改写之后的结论走；
// 被批内裁决换掉的大结果按替换消息自己的错误码处理。

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// replaceResultHook 在 OnToolEnd 里改写 Result。
type replaceResultHook struct {
	hook.NoOpHooks
	repl func(out *schema.ToolOutput)
}

func (h *replaceResultHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil {
		h.repl(out)
	}
	return ctx, nil
}

// resultsForCall 挑出模型各轮输入里配对到 callID 的 tool_result。
func resultsForCall(seen []*schema.Message, callID string) []*schema.Message {
	var r []*schema.Message
	for _, m := range seen {
		if m != nil && m.Role == schema.RoleTool && schema.GetToolResultID(m) == callID {
			r = append(r, m)
		}
	}
	return r
}

// runWithSpillHook 开转存（阈值 1000、预览 300）跑一次，返回模型各轮看到的输入。
func runWithSpillHook(t *testing.T, tk *tool.Toolkit, h hook.Hooks, rounds ...*schema.Message) []*schema.Message {
	t.Helper()
	var seen []*schema.Message
	m := newStubModelCapturing(rounds, &seen)
	opts := []agent_v2.Option{agent_v2.WithName("hk"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300})}
	if h != nil {
		opts = append(opts, agent_v2.WithHooks(h))
	}
	if _, err := agent_v2.NewBasicAgent(opts...).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	return seen
}

func lastResultFor(t *testing.T, seen []*schema.Message, callID string) *schema.Message {
	t.Helper()
	rs := resultsForCall(seen, callID)
	if len(rs) == 0 {
		t.Fatalf("模型没看到 %s 的结果", callID)
	}
	return rs[len(rs)-1]
}

// TestToolResultSpill_HookEnlargesResult_Spilled：钩子把一个小结果整体换成大结果（成功），照样转存。
func TestToolResultSpill_HookEnlargesResult_Spilled(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("small", "s", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	h := &replaceResultHook{repl: func(out *schema.ToolOutput) {
		out.Result = &schema.ToolCallResult{Success: true, Output: bigOutput(3000)}
	}}
	seen := runWithSpillHook(t, tk, h, schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "small")}), schema.AssistantMessage("done"))
	got := lastResultFor(t, seen, "c1")
	if len(got.Content) > 2000 || !spillIDRe.MatchString(got.Content) {
		t.Fatalf("钩子把结果改大之后没转存：模型看到 %d 字节", len(got.Content))
	}
}

// TestToolResultSpill_HookTurnsIntoBigFailure_Spilled：钩子把成功改成一条很大的失败：模型看到 `[code]` + 预览。
func TestToolResultSpill_HookTurnsIntoBigFailure_Spilled(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("small", "s", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	h := &replaceResultHook{repl: func(out *schema.ToolOutput) {
		out.Result = &schema.ToolCallResult{Success: false, ErrorCode: "hook_veto", Error: bigOutput(3000)}
	}}
	seen := runWithSpillHook(t, tk, h, schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "small")}), schema.AssistantMessage("done"))
	got := lastResultFor(t, seen, "c1")
	if len(got.Content) > 2000 || !strings.HasPrefix(got.Content, "[hook_veto] ") || !spillIDRe.MatchString(got.Content) {
		t.Fatalf("钩子改成大失败之后没按大小处理 / 错误码不对：%d 字节，开头 %q", len(got.Content), got.Content[:min(60, len(got.Content))])
	}
}

// TestToolResultSpill_HookTurnsBigFailureIntoSuccess_ClearsFailureMarker：工具返回很大的失败、钩子改成成功：消息上不再
// 带失败标注。标注随消息进存档与会话历史，留着的话钩子之前的全文照样落盘，而且这条按标注判成失败。
func TestToolResultSpill_HookTurnsBigFailureIntoSuccess_ClearsFailureMarker(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("bad", "b", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Failure("build_failed", bigOutput(3000)), nil
	}))
	h := &replaceResultHook{repl: func(out *schema.ToolOutput) {
		out.Result = &schema.ToolCallResult{Success: true, Output: "已忽略构建失败"}
	}}
	seen := runWithSpillHook(t, tk, h, schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "bad")}), schema.AssistantMessage("done"))
	got := lastResultFor(t, seen, "c1")
	if got.Content != "已忽略构建失败" {
		t.Fatalf("模型应看到钩子给的内容，实际 %q", got.Content[:min(80, len(got.Content))])
	}
	if _, still := got.Annotations["__tool_error_code__"]; still {
		ann, _ := got.Annotations["__tool_error_message__"].(string)
		t.Fatalf("钩子把失败改成成功之后，消息上还带着失败标注（说明 %d 字节）", len(ann))
	}
}

// TestToolResultSpill_HookRedactsBigFailure_FailureMarkerRedacted：工具返回带秘密的大失败，钩子把失败说明脱敏成一句话：
// 消息上的失败标注也是脱敏之后的那句，不含秘密。
func TestToolResultSpill_HookRedactsBigFailure_FailureMarkerRedacted(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("bad", "b", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Failure("build_failed", spillSecretOutput()), nil
	}))
	h := &replaceResultHook{repl: func(out *schema.ToolOutput) {
		if out.Result != nil && !out.Result.Success {
			out.Result.Error = "构建失败（日志已脱敏）"
		}
	}}
	seen := runWithSpillHook(t, tk, h, schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "bad")}), schema.AssistantMessage("done"))
	got := lastResultFor(t, seen, "c1")
	if strings.Contains(got.Content, "SECRET-TOKEN") {
		t.Fatalf("fixture 没搭对：模型看到了秘密 %q", got.Content[:min(80, len(got.Content))])
	}
	if got.Content != "[build_failed] 构建失败（日志已脱敏）" {
		t.Fatalf("模型应看到脱敏之后的失败，实际 %q", got.Content[:min(80, len(got.Content))])
	}
	ann, _ := got.Annotations["__tool_error_message__"].(string)
	if strings.Contains(ann, "SECRET-TOKEN") {
		t.Fatalf("钩子脱敏了失败说明，消息上的失败标注仍是脱敏之前的全文（%d 字节、含秘密）——随存档与会话历史落盘", len(ann))
	}
}

// TestToolResultSpill_SupersededBigResult_Spilled：同一批 [c1 定 first, c2 定 second 并返回大结果]，c2 的结果被换成失败
// 结果——它照样按大小处理、错误码是替换后的那个、替换消息的失败标注也跟着处理。
func TestToolResultSpill_SupersededBigResult_Spilled(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("d1", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		evSegDecide(ctx, "first")
		return tool.Success("ok1"), nil
	}))
	tk.Register(tool.NewFuncTool("d2", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		time.Sleep(20 * time.Millisecond)
		evSegDecide(ctx, "second")
		return &tool.Result{Success: true, Output: bigOutput(3000)}, nil
	}))
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "d1"), evSegCall("c2", "d2")}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ev_seg"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300}))
	sigs, _ := evSegDrive(t, ag, nil)
	if got := evSegDecision(sigs); got != "first" {
		t.Fatalf("fixture 没搭对：排在前面的应生效，实际 %s", got)
	}
	c2 := lastResultFor(t, seen, "c2")
	if want := "[" + agent_v2.SupersededDecisionErrCode + "] "; !strings.HasPrefix(c2.Content, want) {
		t.Fatalf("被盖掉的 c2 错误码不对：开头 %q", c2.Content[:min(60, len(c2.Content))])
	}
	if len(c2.Content) > 2000 || !spillIDRe.MatchString(c2.Content) {
		t.Fatalf("被裁决换掉的大结果没按大小处理：模型看到 %d 字节", len(c2.Content))
	}
	if ann, _ := c2.Annotations["__tool_error_message__"].(string); len(ann) > 5000 {
		t.Fatalf("替换消息的失败标注没跟着处理：%d 字节", len(ann))
	}
}
