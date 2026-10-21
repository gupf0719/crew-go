//go:build integration

package tutorial_test

import (
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 6. HandoffAgent（进阶）— 医院四级分诊流程（模型 B：transfer_to_agent 工具自主转交）
//
// **模型 B**（doc/v2/handoff.md）：sub-agent 配 WithHandoffTransfer() 后，LLM 调
// transfer_to_agent({agent_name}) 工具自主发起转交 → return-direct 短路当前 agent →
// 控制权转移给目标 agent 接管。替代 v1 文本 "HANDOFF: x" / v2 早期 annotation-only
// （纯 LLM 无法产生 annotation，不可用）方案——本测试真实 LLM 端到端验证转交链。
//
// 转交链：reception（前台）→ triage（分诊）→ 科室（emergency/internal_medicine/
// orthopedics）。reception/triage 配 transfer 工具（能转交）；科室是终点（不配）。
// ──────────────────────────────────────────────────────────────────

func TestAdv_06_HandoffAgent(t *testing.T) {
	provider := requireAPIKey(t)

	reception := agent_v2.NewBasicAgent(
		agent_v2.WithName("reception"),
		agent_v2.WithSystemPrompt(
			"你是医院前台。患者描述任何身体症状时，必须调用 transfer_to_agent 工具转给分诊护士"+
				"（参数 agent_name=\"triage\"），不要自己评估病情。仅当患者咨询纯行政事务"+
				"（停车、探视时间等）才直接回答。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithHandoffTransfer(),
	)
	triage := agent_v2.NewBasicAgent(
		agent_v2.WithName("triage"),
		agent_v2.WithSystemPrompt(
			"你是分诊护士。评估患者症状紧急程度，调用 transfer_to_agent 工具转给对应科室：\n"+
				"- 胸痛/偏瘫/呼吸困难等急症 → 参数 agent_name=\"emergency\"\n"+
				"- 普通内科症状（发烧、咳嗽等）→ 参数 agent_name=\"internal_medicine\"\n"+
				"- 骨骼/关节问题 → 参数 agent_name=\"orthopedics\"\n"+
				"必须调用工具转交，不要自己处置。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithHandoffTransfer(),
	)
	// 科室是转交链终点——不配 transfer 工具，处理完即回复（done）。
	emergency := newBasicAgentV2(provider, "emergency",
		"你是急诊科医生。患者情况紧急，立即给出处置指导，告知安排绿色通道，预计等待 < 5 分钟。")
	internalMed := newBasicAgentV2(provider, "internal_medicine",
		"你是内科医生。详细询问症状持续时间，给出初步诊断建议和检查项目。")
	ortho := newBasicAgentV2(provider, "orthopedics",
		"你是骨科医生。评估骨骼关节问题，建议是否需要 X 光检查。")

	ha := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("hospital_triage"),
		agent_v2.WithHandoffInitial("reception"),
		agent_v2.WithHandoffMaxHops(6),
	).
		AddAgent("reception", reception).
		AddAgent("triage", triage).
		AddAgent("emergency", emergency).
		AddAgent("internal_medicine", internalMed).
		AddAgent("orthopedics", ortho)

	events, err := ha.Run(ctx300s(t), nil,
		schema.UserMessage("我突然感到左胸剧烈疼痛，已经持续 10 分钟，伴有左臂麻木"))
	if err != nil {
		t.Fatalf("HandoffAgent 运行失败: %v", err)
	}

	final := mustFinalMessageV2(t, events)
	t.Logf("分诊最终响应: %s", final.Content)
	if final.Content == "" {
		t.Error("输出不应为空")
	}

	// **模型 B 核心断言**：真实 LLM 通过 transfer_to_agent 工具完成转交链——
	// 转交必须到达某科室（emergency/internal_medicine/orthopedics 之一），证明
	// reception/triage 真的调了 transfer 工具自主转交。若转交未生效（模型 A
	// annotation-only 纯 LLM 不可用），科室不被调 → reachedDept=false → fail。
	reachedDept := ""
	for _, dept := range []string{"emergency", "internal_medicine", "orthopedics"} {
		if f := agent_v2.FinalMessageByAgent(events, dept); f != nil {
			reachedDept = dept
			t.Logf("✓ 转交链到达科室 %q: %s", dept, f.Content)
		}
	}
	if reachedDept == "" {
		t.Error("转交链未到达任何科室——模型 B 真实 LLM 自主转交未生效（reception/triage 未调 transfer_to_agent）")
	}
}
