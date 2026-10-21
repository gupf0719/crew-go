package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestHandoffBatchVisibility_HistoryCarriesSiblingToolResults 钉住**同批工具的产出对
// 转交后接可见——开会话历史层时**。
//
// backlog「return-direct 同批工具产出无人可见」（真 LLM 实测同批组答不出金额）的架构
// 定位（2026-08-29 实测）：不是短路语义的问题，是**视图源**——
//
//   - 不开历史层：视图从 `RunSession.Events` 重建，而 events 只存 Exit assistant、
//     **tool_result 从来不进** ⟹ Rewrite 只能改写出「called tool: query_order」半句，
//     产出（¥238）丢失。这是 events 容器的**文档化有意降级**（见 usage-guide 会话历史节），
//     不是本测试要修的。
//   - 开历史层：视图换源为逐字完整流 ⟹ 「tool returned result: 订单金额 ¥238.00」
//     完整可见。`rewriteCrossAgentMessage` 对 RoleTool 的改写分支一直都在，只是上游
//     没喂过它。
//
// 本测试钉**开历史层那半的行为承诺**（它是引导业务的出路，断了出路就没了）。
func TestHandoffBatchVisibility_HistoryCarriesSiblingToolResults(t *testing.T) {
	// frontdesk 同批：[query_order, transfer_to_agent(billing)]
	batch := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_q", Function: schema.FunctionCall{Name: "query_order", Arguments: `{}`}},
		{ID: "tc_t", Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName,
			Arguments: `{"agent_name":"billing"}`}},
	})
	tk := agent_v2_newToolkit()
	tk.Register(tool.NewFuncTool("query_order", "查订单", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success("订单金额 ¥238.00"), nil
		}))
	hs := agent_v2.NewInMemoryHistoryStore()
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"), agent_v2.WithToolkit(tk), agent_v2.WithHistory(hs, "t"),
		agent_v2.WithModel(newStubModel([]*schema.Message{batch})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(5),
	)
	billingCap := &captureModel{resp: schema.AssistantMessage("billing done")}
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"), agent_v2.WithModel(billingCap), agent_v2.WithMaxRounds(5),
		agent_v2.WithHistory(hs, "t"),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"), agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", frontdesk).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-batch")
	evs, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查订单并转给账务"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	found := false
	for _, s := range billingCap.seen {
		if contains238(s) {
			found = true
		}
	}
	if !found {
		t.Errorf("开了会话历史层，转交后接仍看不到同批 query_order 的产出（¥238）——"+
			"业务被引导「开历史层解决同批可见性」，这条出路断了。billing 实际看到: %v",
			billingCap.seen)
	}
	// 事件形状顺带钉住：billing 真的接管并收尾
	if fm := agent_v2.FinalMessage(evs); fm == nil || fm.Content != "billing done" {
		t.Fatalf("fixture 自检：转交没完成，final=%v", fm)
	}
}

func contains238(s string) bool {
	for i := 0; i+3 <= len(s); i++ {
		if s[i:i+3] == "238" {
			return true
		}
	}
	return false
}
