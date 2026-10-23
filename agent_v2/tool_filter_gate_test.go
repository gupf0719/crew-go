package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// WithToolFilter 裁掉的工具，模型照样发了 tool_call 也不执行：模型能调的就是这一轮交给它的那些（不变量）。
//
// 只裁绑定、不拦执行时，模型凭上文记得的工具名照样调得动（2026-10-16 探针：被过滤的工具执行 1 次）。

type countingTools struct {
	lookups, deletes atomic.Int32
}

func (c *countingTools) toolkit() *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup", "查询", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			c.lookups.Add(1)
			return tool.Success("balance=42"), nil
		}))
	tk.Register(tool.NewFuncTool("delete_account", "注销账户", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			c.deletes.Add(1)
			return tool.Success("deleted"), nil
		}))
	return tk
}

func callTool(id, name string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
		Function: schema.FunctionCall{Name: name, Arguments: `{}`}}})
}

// toolResultCodes 从模型看到的消息里挑出每个 tool_call 的结果文本。
func toolResultTexts(seen []*schema.Message) map[string]string {
	out := map[string]string{}
	for _, m := range seen {
		if m.Role == schema.RoleTool {
			out[schema.GetToolResultID(m)] = m.Content
		}
	}
	return out
}

func TestToolFilter_FilteredToolNotExecuted(t *testing.T) {
	ct := &countingTools{}
	var seen []*schema.Message
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("filtered"),
		agent_v2.WithModel(newStubModelCapturing([]*schema.Message{
			callTool("c1", "delete_account"),
			callTool("c2", "lookup"),
			schema.AssistantMessage("done"),
		}, &seen)),
		agent_v2.WithToolkit(ct.toolkit()), agent_v2.WithMaxRounds(5),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string { return []string{"lookup"} }))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查一下余额")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := ct.deletes.Load(); n != 0 {
		t.Errorf("被过滤掉的 delete_account 执行了 %d 次，want 0", n)
	}
	if n := ct.lookups.Load(); n != 1 {
		t.Errorf("放行的 lookup 执行了 %d 次，want 1——闸不能把放行的也拦了", n)
	}
	res := toolResultTexts(seen)
	if !strings.Contains(res["c1"], agent_v2.ToolNotAvailableErrCode) {
		t.Errorf("模型应拿到 %s 失败结果，got %q", agent_v2.ToolNotAvailableErrCode, res["c1"])
	}
}

// 最后一轮软降落摘掉了业务工具：模型在这一轮硬调业务工具也不执行。
func TestToolFilter_FinalRoundStrippedToolNotExecuted(t *testing.T) {
	ct := &countingTools{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("final_round"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			callTool("c1", "lookup"),
			callTool("c2", "delete_account"), // 第二次调模型是最后一轮（maxRounds=1），工具已被摘掉
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(ct.toolkit()), agent_v2.WithMaxRounds(1))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q"))
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Fatalf("fixture：最后一轮还在调工具，应以轮次超限收尾，got %v", err)
	}
	if ct.lookups.Load() != 1 {
		t.Fatalf("fixture：第一轮的 lookup 应执行 1 次，got %d", ct.lookups.Load())
	}
	if n := ct.deletes.Load(); n != 0 {
		t.Errorf("最后一轮被摘掉的 delete_account 执行了 %d 次，want 0", n)
	}
}

// 最后一轮软降落连框架工具一起摘：模型在这一轮硬调 ask_user 不执行（不去问人），Run 以轮次超限收尾。ToolFilter 的
// 轮次豁免框架工具，软降落轮不豁免——豁免的话人答完那一问也被丢掉（审计实测）。
func TestToolFilter_FinalRoundFrameworkToolNotExecuted(t *testing.T) {
	ct := &countingTools{}
	ask := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "a1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"要继续吗"}`}}})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("final_round_ask"),
		agent_v2.WithModel(newStubModel([]*schema.Message{callTool("c1", "lookup"), ask, schema.AssistantMessage("done")})),
		agent_v2.WithToolkit(ct.toolkit()), agent_v2.WithMaxRounds(1), agent_v2.WithAskUser())
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("q"))
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("最后一轮被摘掉的 ask_user 去问了人：%v", err)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("应以轮次超限收尾，got %v", err)
	}
}

// ToolFilter 的轮次里框架工具不受闸：白名单里没写 ask_user，模型调它照样问人。
func TestToolFilter_FrameworkToolExemptInFilteredRound(t *testing.T) {
	ct := &countingTools{}
	ask := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "a1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"要继续吗"}`}}})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("filtered_ask"),
		agent_v2.WithModel(newStubModel([]*schema.Message{ask, schema.AssistantMessage("done")})),
		agent_v2.WithToolkit(ct.toolkit()), agent_v2.WithMaxRounds(5), agent_v2.WithAskUser(),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string { return []string{"lookup"} }))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("q"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("ToolFilter 豁免框架工具：ask_user 应照常问人，got %v", err)
	}
}

// 续跑按产出这一批的那一轮的绑定判：中断之后 ToolFilter 的返回变了，已经交给模型、模型也调了的那个工具照样执行；
// 那一轮没交给模型的照样不执行。
func TestToolFilter_ResumeUsesBindingOfThatRound(t *testing.T) {
	ct := &countingTools{}
	var refunds atomic.Int32
	tk := ct.toolkit()
	tk.Register(tool.NewFuncTool("refund", "退款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			refunds.Add(1)
			return tool.Success("refunded"), nil
		}))
	var allowRefund atomic.Bool
	allowRefund.Store(true)
	batch := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "r1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{}`}},
		{ID: "d1", Type: "function", Function: schema.FunctionCall{Name: "delete_account", Arguments: `{}`}},
	})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("resume_binding"),
		agent_v2.WithModel(newStubModel([]*schema.Message{batch, schema.AssistantMessage("done")})),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string {
			if allowRefund.Load() {
				return []string{"refund", "lookup"}
			}
			return []string{"lookup"}
		}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("退款"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture：refund 应停在审批上，got %v", err)
	}
	if qs := ie.AllQueries(); len(qs) != 1 {
		t.Fatalf("fixture：只有 refund 要审批（delete_account 这一轮没交给模型、在审批之前就被拦下），got %d 条", len(qs))
	}
	allowRefund.Store(false) // 环境变了：此刻再算 ToolFilter 的话 refund 不在名单里
	answers := map[string]*schema.Message{}
	for _, q := range ie.AllQueries() {
		answers[q.InterruptID] = schema.UserMessage(agent_v2.ApprovalOptionApprove)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if n := refunds.Load(); n != 1 {
		t.Errorf("那一轮交给了模型、用户也批准了的 refund 执行了 %d 次，want 1", n)
	}
	if n := ct.deletes.Load(); n != 0 {
		t.Errorf("那一轮没交给模型的 delete_account 执行了 %d 次，want 0", n)
	}
}
