package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestRepro_R5_AgentAsToolNestedBasicAgentOrphanSid 验证 audit Round 5 真 bug 1：
// AgentAsTool 嵌套真 BasicAgent + 子内 ask_user 中断 → 子 cp 写到独立 sid（孤儿）→
// caller 用父 sid Resume 时找不到子的 ask_user pending state。
//
// **预期行为**：子 cp 应写到 (父 sid, 子 graphNS) — 父 sid + audit Round 3 修的
// graphNS push 让子 cp 物理分离但仍在父 sid 树内。Resume 父 sid 时 framework
// 能找到子 cp。
//
// **当年的 bug**：子 BasicAgent 拿到的 session 是自建的 NewRunSession（完全独立 sid），
// 存档跟着它走。修法不是让 runGraph 改读 ctx 里的父 sid（那是第二把尺：长寿 ctx 上钉着
// 的旧 sid 会让新任务接着跑被放弃任务的存档），而是让嵌套位置上的 session 的 RunID 就是
// 父 sid——被调方的 session 由框架给（RunID = 所在任务），业务传了别的任务的入口直接拒。
func TestRepro_R5_AgentAsToolNestedBasicAgentOrphanSid(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	// 子 agent：调 ask_user 抛中断
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("sub"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_1", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"sub_q"}`}},
			}),
		})),
		agent_v2.WithToolkit(tk),
	)

	// 父 agent：toolkit 含 AgentAsTool(sub)
	parentTk := tool.NewToolkit()
	parentTk.Register(agent_v2.AgentAsTool(subAgent, agent_v2.WithAgentToolDescription("delegate to sub")))

	parentAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_p", Function: schema.FunctionCall{Name: "sub", Arguments: `{"input":"do it"}`}},
			}),
		})),
		agent_v2.WithToolkit(parentTk),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	parentSID := "parent_sid_test"
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	parentSess := &agent_v2.RunSession{RunID: parentSID}

	_, err := parentAgent.Run(ctx, parentSess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("expected interrupt from sub-agent ask_user")
	}
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("expected InterruptError, got %v", err)
	}

	// 验证：cp 应写在 parentSID 下（含子图 cp ns 物理分离）
	pending, _ := cper.ListPending(context.Background(), parentSID)
	t.Logf("Pending cps in parentSID '%s': %d", parentSID, len(pending))
	for _, p := range pending {
		t.Logf("  - ns=%q graph_id=%q", p.NS, p.Checkpoint.GraphID)
	}
	if len(pending) == 0 {
		t.Fatalf("Bug #1 reproduced: parent sid 下没有 cp（子 BasicAgent 用了独立 sid）\n" +
			"查：嵌套调用的被调方拿到的 session 的 RunID 应与父相同（所在任务）")
	}
	// 应该有 2 个 cp：父根 cp (ns="") + 子 cp (ns 含子 graph 名)
	if len(pending) < 2 {
		t.Fatalf("Bug #1 reproduced: parent sid 下只有 %d cp（应有 2：父根 + 子）", len(pending))
	}
}
