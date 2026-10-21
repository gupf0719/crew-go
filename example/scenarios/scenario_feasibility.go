package scenarios

import (
	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
)

// scenario_feasibility（方向 B）：调研 X 库能否做 Y → 不行就早收尾砍掉后续步骤。
// replan 价值：第一步 researcher 拿到事实后，可能让原计划的 writer/critic 步骤
// 全部变成废操作，replan 直接砍掉。
func init() {
	Register("feasibility", buildFeasibilityScenario)
}

func buildFeasibilityScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	researcher := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("researcher"),
		agent_v2.WithDescription("技术可行性调研——首行输出 FEASIBLE: YES/NO/PARTIAL + 支撑事实"),
		agent_v2.WithSystemPrompt(`你是技术可行性调研员。
- 收到调研任务：直接给出对该库/方案的关键事实（是否官方支持、限制、坑）。
- 第一行必须是 "FEASIBLE: YES" / "FEASIBLE: NO" / "FEASIBLE: PARTIAL"。
- 后续行给出 2-4 条支撑事实。
- 不要 hedge，给明确判断。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	writer := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("writer"),
		agent_v2.WithDescription("基于可行结论写完整方案（含步骤、注意事项、示例代码）"),
		agent_v2.WithSystemPrompt(`你是技术写作者。基于上一步调研结果写一份"可行方案"或"绕路方案"，
包含步骤、注意事项、示例代码片段（伪代码也可）。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	concluder := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("concluder"),
		agent_v2.WithDescription("写 1-3 句话最终结论（能/不能/部分能 + 关键原因）"),
		agent_v2.WithSystemPrompt(`你是结论撰写者。基于已有调研给出 1-3 句话的最终结论，
说清"能/不能/部分能"以及关键原因，不要再展开方案。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。任务是判断某技术方案是否可行，并视情况撰写方案或直接给结论。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"，不要 markdown、不要解释。

可选 agent 列表（含各自职责）由 user 消息动态注入。

规划规则（每次只输出 1 步，replan-每步 范式自然展开依赖）：
- 没有 executed → 输出 [{researcher 调研可行性, refs:[]}]
- 上一步是 researcher 且产出包含 "FEASIBLE: NO" → 输出 [{concluder 写"不可行"结论, refs:["researcher"]}]
- 上一步是 researcher 且产出包含 "FEASIBLE: PARTIAL" → 输出 [{researcher 查找绕路方案, refs:["researcher"]}]
- 上一步是 researcher 且产出包含 "FEASIBLE: YES" → 输出 [{writer 写方案, refs:["researcher"]}]
- 上一步是 writer → 输出 [{concluder 总结, refs:["writer"]}]
- 上一步是 concluder → 输出 []（收尾）` + RefsPromptHelp

	return Scenario{
		Name:          "feasibility",
		Description:   "可行性调研：第一步事实可能让后续 writer 步骤变废，replan 砍掉",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"researcher": researcher,
			"writer":     writer,
			"concluder":  concluder,
		},
		Fallback: concluder,
		DialogueScript: []string{
			`研究一下 Go 标准库 net/http 能否实现 HTTP/3 服务端`,
			`再看看 sync.Map 是否适合做高频写入的本地缓存`,
		},
		ReplanHint: "FEASIBLE: NO → 砍 writer 直接 concluder；YES → 保留全链路",
	}
}
