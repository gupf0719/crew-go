package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// nilMsgHook 在 OnModelEnd 把 output.Message 置 nil——模拟 guardrail / 内容过滤类业务
// hook 想"拦掉"一条模型回复时误把整条消息删掉（OnModelEnd 的 Message 是 [消费] 档，
// 业务可改写，置 nil 是合法操作的边界）。
type nilMsgHook struct {
	hook.NoOpHooks
}

func (h *nilMsgHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message = nil
	return ctx, nil
}

// nilMsgAnswerModel 返回一条无 tool_call 的 assistant 消息（一轮即终止；Stream 返
// (nil,nil) 让 streamOrGenerate 降级 Generate）。
type nilMsgAnswerModel struct{}

func (m *nilMsgAnswerModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("hello"), nil
}
func (m *nilMsgAnswerModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *nilMsgAnswerModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestBasicAgent_OnModelEndNilMessage_CleanError 验证（BasicAgent 对照 review 牵出）：
// 业务 OnModelEnd hook 把 output.Message 置 nil 时，框架 fail-fast 返干净 error
// （对称 agent_v2/basic_builder.go model 自身 nil 守卫 + :475 OnToolEnd 置 nil 守卫），
// 而不是 reset_stream 的 len(out.Content) 解引用 nil panic。
//
// 修前 bug：modelNode 在 hook 改写检测块内 :360 已守卫 `if out != nil`，但同块 :372
// `len(out.Content)` 漏守卫——hook 置 nil 时 `out != preHookMsg`(nil≠非nil) 进块 +
// GetEventSink 恒非 nil(graph_loop:1061 每节点挂 nodeEventSink) → nil deref panic
// （被 Run outer defer recover 成误导性 "node panic recovered" 错误）。
func TestBasicAgent_OnModelEndNilMessage_CleanError(t *testing.T) {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("nil-hook-agent"),
		agent_v2.WithModel(&nilMsgAnswerModel{}),
		agent_v2.WithHooks(&nilMsgHook{}),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("OnModelEnd hook 置 Message=nil 应返干净 error，实际 err=nil")
	}
	// 修前会是 "node \"model\": node panic recovered: ... nil pointer dereference"
	// （误导性框架崩溃外观）；修后应明确指向 hook 置 nil。
	if !strings.Contains(err.Error(), "set Message to nil") {
		t.Errorf("error 应明确指向 OnModelEnd hook 置 nil（含 'set Message to nil'），实际: %v", err)
	}
	if strings.Contains(err.Error(), "panic") {
		t.Errorf("不应是 panic recover 错误（修前 :372 nil deref），实际: %v", err)
	}
}
