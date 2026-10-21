package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// alwaysInterruptModel 第 1 次 Generate 即触发 interrupt（通过 hook）。
// 但更可靠的方式是构造一个 sub-agent，其 hook 立刻 throw InterruptError。
type interruptingAgent struct {
	name string
}

func (a *interruptingAgent) Name() string        { return a.name }
func (a *interruptingAgent) Description() string { return "always interrupts" }
func (a *interruptingAgent) Run(_ context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "sub-agent forced interrupt",
		Query: &compose_v2.AskQuery{
			InterruptID: "sub_ask",
			Question:    "sub-agent says hi",
		},
	}
}
func (a *interruptingAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, fmt.Errorf("not impl")
}

var _ agent_v2.Agent = (*interruptingAgent)(nil)

// TestAgentAsTool_PropagatesInterruptError 验证 audit Bug #1 修复：
// sub-agent 抛 InterruptError 时 AgentAsTool.Call 必须 propagate 而非吞为 tool.Failure。
//
// 链路：
//   - parent BasicAgent 调 ask_sub tool（包了 interruptingAgent）
//   - sub-agent.Run 抛 InterruptError
//   - AgentAsTool.Call 之前 fix：返 tool.Failure 静默吞 → parent 当 tool 成功继续 ReAct
//   - 修复后：返 (nil, InterruptError) → parent 框架 SaveNS cp + propagate 给 caller
//
// sanity-revert：把 agent_tool.go IsInterrupt 检查去掉，本测试应 fail——
// parent.Run 返 nil err 而非 InterruptError。
func TestAgentAsTool_PropagatesInterruptError(t *testing.T) {
	subAgent := &interruptingAgent{name: "sub_interrupting"}
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(subAgent, agent_v2.WithAgentToolDescription("always interrupts")))

	// parent BasicAgent：用 stub model 调 ask_sub tool 一次
	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "sub_interrupting", Arguments: `{"input":"go"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	parentModel := &stubModel{responses: []*schema.Message{resp1}}

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("delegate"))
	if err == nil {
		t.Fatal("parent.Run 应返 error（sub-agent InterruptError 应 propagate）")
	}
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("parent.Run 返的 err 应能 unwrap 为 InterruptError，实际 %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("InterruptError 应含 Query.InterruptID，实际 %+v", ie)
	}

	// 验证 cp 已 SaveNS（HITL 端到端不变量）
	saved, _ := cper.LoadNS(context.Background(), sess.RunID, "")
	if saved == nil {
		t.Fatalf("sub-agent InterruptError 后 parent 应 SaveNS cp，实际未保存\n" +
			"sanity-revert：agent_tool.go 去掉 IsInterrupt 检查，本断言 fail")
	}
}

// 防 unused
var _ = errors.New
