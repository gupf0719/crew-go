package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestHandoffTransfer_SurvivesSiblingInterrupt 端到端钉住 ISSUE-99 位 6：
// **转交与一个需要确认的工具同批**（真 LLM 实测确会发生，见
// example/scenarios/transfer_batch_probe：强诱导下 3/3 同批），兄弟中断存档之后，
// 续跑必须照样转交。
//
// 最早这条靠「transfer 函数体一定会重跑」活着——而那要求它被排除在重放记账之外，判据是
// **工具名字**。现在转交是上行信号，随这次 tool_call 的记账进 cp、命中时原样交出。承重的
// 变异（记账不记信号 / 命中不交出）在 tool_memo_framework_test.go 与 side_signal_commit_test.go
// 上实跑过。
func TestHandoffTransfer_SurvivesSiblingInterrupt(t *testing.T) {
	askTK := tool.NewToolkit()
	askTK.Register(tool.NewFuncTool("ask_confirm", "问一句", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认转交吗"})
			if err != nil {
				return nil, err
			}
			return tool.Success("confirmed:" + ans.Content), nil
		}))

	// 同一批里两个 tool_call：转交 + 要确认的工具。下标 0 是 transfer。
	sameBatch := schema.AssistantToolCallMessage([]schema.ToolCall{
		{
			ID: "tc_transfer", Type: "function",
			Function: schema.FunctionCall{
				Name:      agent_v2.HandoffTransferToolName,
				Arguments: `{"agent_name":"billing"}`,
			},
		},
		{
			ID: "tc_ask", Type: "function",
			Function: schema.FunctionCall{Name: "ask_confirm", Arguments: `{}`},
		},
	})

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newScopeStub(sameBatch, schema.AssistantMessage("FRONT 自己答了"))),
		agent_v2.WithToolkit(askTK),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := h.Run(ctx, sess, schema.UserMessage("我要处理账单"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：应停在 ask_confirm 的中断上，实际 err=%v", err)
	}
	if n := billingModel.count(); n != 0 {
		t.Fatalf("fixture 没搭对：中断时还没轮到 billing，它却被调了 %d 次", n)
	}

	qs := ie.AllQueries()
	if len(qs) != 1 {
		t.Fatalf("fixture 没搭对：应恰好一条挂起，实际 %d 条", len(qs))
	}
	events, err := h.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{qs[0].InterruptID: schema.UserMessage("确认")}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// 承重的量是「billing 真的被执行了」——不是「最终文本看着对」。转交丢失时
	// front 会自己答完收尾，最终文本同样是一句像样的回复，只是没人转交过。
	if n := billingModel.count(); n != 1 {
		t.Fatalf("续跑后转交必须仍然生效：billing 应被真调 1 次，实际 %d 次"+
			"（0 = 转交跨存档丢失，且 err 全程为 nil）", n)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != "BILLING 接手了" {
		got := "<nil>"
		if fm != nil {
			got = fm.Content
		}
		t.Fatalf("最终产出应来自接手方 billing，实际 %q", got)
	}
}
