package tutorial_test

// adv_06 mock：HandoffAgent 多 agent 转交 reception → triage → emergency
//
// 迁移到 agent_v2：
//   - constructor 改链式：NewHandoffAgent + AddAgent + WithHandoffInitial
//   - handoff signal 走内置 transfer_to_agent 工具（LLM 自主转交）。
//     ⚠️ 曾经还能写产出消息的 annotation，那条路已删——它绕过全部三道校验
//     （有没有被授予 / 目标合不合法 / 这一层有没有人接）。业务自实现的 Agent
//     （没有 LLM 的那种）改用 agent_v2.HandoffTo。

import (
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// makeHandoffMsg 给 stub 用——target 非空时产出一条 transfer_to_agent 的 tool_call
// （模型自主转交的真实形状），空则是普通答复。
func makeHandoffMsg(content, target string) *schema.Message {
	if target == "" {
		return schema.AssistantMessage(content)
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc_" + target, Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.HandoffTransferToolName,
			Arguments: `{"agent_name":"` + target + `"}`,
		},
	}})
}

func TestAdv_06_HandoffAgent_Mock(t *testing.T) {
	makeAgent := func(name string, resp *schema.Message) agent_v2.Agent {
		stub := &stubModel{reply: func(_, _ string, _ []*schema.Message) *schema.Message {
			return resp
		}}
		return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithSystemPrompt(name),
			agent_v2.WithModel(stub),
			agent_v2.WithHandoffTransfer(), // 允许它把对话交出去（转交的两个必要条件之一）
		)
	}
	reception := makeAgent("reception", makeHandoffMsg("初诊登记完成", "triage"))
	triage := makeAgent("triage", makeHandoffMsg("胸痛 + 左臂麻木 → 紧急", "emergency"))
	emergency := makeAgent("emergency", makeHandoffMsg("急诊绿色通道，<5 分钟接诊", ""))
	internal := makeAgent("internal_medicine", makeHandoffMsg("内科候诊 30 分钟", ""))
	ortho := makeAgent("orthopedics", makeHandoffMsg("建议 X 光", ""))

	ha := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("hospital_triage"),
		agent_v2.WithHandoffInitial("reception"),
		agent_v2.WithHandoffMaxHops(6),
	).
		AddAgent("reception", reception).
		AddAgent("triage", triage).
		AddAgent("emergency", emergency).
		AddAgent("internal_medicine", internal).
		AddAgent("orthopedics", ortho)

	events, err := ha.Run(ctx60sMock(t), nil,
		schema.UserMessage("胸痛 + 左臂麻木"))
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("nil")
	}
	t.Logf("最终: %s", out.Content)
	if !strings.Contains(out.Content, "急诊") {
		t.Errorf("期望转交到 emergency，实际 %q", out.Content)
	}
}
