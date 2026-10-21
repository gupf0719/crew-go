package agent_v2_test

// 审批中断抛出的 AskQuery 必须带上**这次挂起卡的是哪个工具调用**。
//
// # 为什么单独一条
//
// conformance 保护的是「backend 会不会把字段漏存」，跟「producer 到底填没填」**正交**
// ——它自己构造 AskQuery 字面量，从不经过 approval.go。所以在这条测试之前，把
// `ToolCallID: tc.ID` 那行删掉全套照样绿。
//
// 而这个字段一旦恒空，下游「挂起被放弃时给悬空 tool_call 补合成结果」的分档就退化成
// 一刀切——**会给真的执行过的工具补一条「未执行」**（启发式审批下「一轮里既有已执行
// 又有待审批」是主场景，不是边角）。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func approvalProbeAgent(t *testing.T, tcs []schema.ToolCall) (*agent_v2.BasicAgent, *compose_v2.InMemoryCheckpointer) {
	t.Helper()
	tk := tool.NewToolkit()
	for _, tc := range tcs {
		name := tc.Function.Name
		tk.Register(tool.NewFuncTool(name, name, map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				return tool.Success("done"), nil
			}))
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("apprAg"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage(tcs),
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithMaxRounds(4))
	return ag, compose_v2.NewInMemoryCheckpointer()
}

// TestApprovalQuery_SingleToolCarriesIdentity 单工具：cp 里读得出工具身份和「这是审批」。
//
// 单个中断走「原样透出」**不经 MergeInterrupts**，而拷 ie.Reason 到 query 的动作只发生
// 在 Merge 里——所以这条同时钉住那个单/多路径分叉。
func TestApprovalQuery_SingleToolCarriesIdentity(t *testing.T) {
	ag, cper := approvalProbeAgent(t, []schema.ToolCall{
		{ID: "tc_solo", Function: schema.FunctionCall{Name: "transfer", Arguments: `{}`}},
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("go")); err == nil {
		t.Fatal("应因审批中断")
	}
	cp, _ := cper.LoadNS(ctx, sess.RunID, "")
	if cp == nil || len(cp.PendingInterrupts) != 1 {
		t.Fatalf("应有 1 条挂起, cp=%v", cp)
	}
	q := cp.PendingInterrupts[0]
	if q.ToolCallID != "tc_solo" {
		t.Errorf("ToolCallID = %q, want tc_solo——挂起认不出它卡的是哪次调用，\n"+
			"  下游补合成结果时只能一刀切（会给真执行过的工具补「未执行」）", q.ToolCallID)
	}
	if q.ToolName != "transfer" {
		t.Errorf("ToolName = %q, want transfer", q.ToolName)
	}
	if q.Reason != "tool_approval" {
		t.Errorf("Query.Reason = %q, want tool_approval——**单个中断不经 MergeInterrupts**，\n"+
			"  只填 InterruptError.Reason 的话 cp 里读不出「这是审批挂起」，\n"+
			"  而多工具时反而读得出：同一语义两条路径行为分叉", q.Reason)
	}
}

// TestApprovalQuery_FanoutEachCarriesOwnIdentity fanout：每个 query 带**各自**的身份。
//
// 这条防的是「合并时被同一个值覆盖」——ToolCallID 是 per-query 事实不是 per-ie 事实，
// 任何 ie 级的拷贝都会把一个值盖到全部 query 上。
func TestApprovalQuery_FanoutEachCarriesOwnIdentity(t *testing.T) {
	ag, cper := approvalProbeAgent(t, []schema.ToolCall{
		{ID: "tc_a", Function: schema.FunctionCall{Name: "transfer", Arguments: `{}`}},
		{ID: "tc_b", Function: schema.FunctionCall{Name: "refund", Arguments: `{}`}},
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("go")); err == nil {
		t.Fatal("应因审批中断")
	}
	cp, _ := cper.LoadNS(ctx, sess.RunID, "")
	if cp == nil || len(cp.PendingInterrupts) != 2 {
		t.Fatalf("应有 2 条挂起, got %v", cp)
	}
	seen := map[string]string{} // ToolCallID → ToolName
	for _, q := range cp.PendingInterrupts {
		if q.ToolCallID == "" {
			t.Fatalf("有 query 的 ToolCallID 为空: %+v", q)
		}
		if prev, dup := seen[q.ToolCallID]; dup {
			t.Fatalf("两个 query 带了同一个 ToolCallID %q（都是 %q）——"+
				"身份被 ie 级拷贝覆盖了。ToolCallID 是 per-query 事实不是 per-ie 事实",
				q.ToolCallID, prev)
		}
		seen[q.ToolCallID] = q.ToolName
	}
	if seen["tc_a"] != "transfer" || seen["tc_b"] != "refund" {
		t.Errorf("身份配对错位: %v（want tc_a→transfer, tc_b→refund）", seen)
	}
}
