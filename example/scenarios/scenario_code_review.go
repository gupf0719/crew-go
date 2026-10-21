package scenarios

import (
	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
)

// scenario_code_review（方向 A）：写代码 → 评审 → 修复 → 再评审 循环。
// replan 价值：critic 的反馈决定还要不要再循环、修哪里。循环次数事前不可定。
func init() {
	Register("code_review", buildCodeReviewScenario)
}

func buildCodeReviewScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	coder := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("coder"),
		agent_v2.WithDescription("写或修改 Go 代码——基于任务/critic 反馈输出代码块"),
		agent_v2.WithSystemPrompt(`你是 Go 工程师。
- 收到"写一版"任务：直接给出 Go 函数实现，加必要注释。代码块用 ` + "```" + `go 包裹。
- 收到"修复"任务：基于上一步 critic 的具体意见修改代码，给出新版本。
- 不要解释，直接给代码。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	critic := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("critic"),
		agent_v2.WithDescription("审查代码并输出 VERDICT: PASS/FAIL + 1-3 条改进点"),
		agent_v2.WithSystemPrompt(`你是严厉的 Go 代码审查员。
检查上一步代码的：边界条件、错误处理、命名、性能、可读性。
输出格式严格如下：
- 第一行必须是 "VERDICT: PASS" 或 "VERDICT: FAIL"
- PASS：代码已无明显问题
- FAIL：列出 1-3 条具体可改进点
- 第一轮基本不要 PASS，至少挑出一个边界问题`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。任务是协调 coder 和 critic 完成代码 review/fix 循环。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"，不要 markdown、不要解释。每次只输出 1 步。

可选 agent 列表（含各自职责）由 user 消息动态注入。

规划规则（每次只输出 1 步，replan-每步 范式自然交替 coder ↔ critic）：
- 没有 executed → 输出 [{coder 写一版, refs:[]}]
- 上一步是 coder → 输出 [{critic 审查, refs:["coder"]}]
- 上一步是 critic 且产出包含 "VERDICT: PASS" → 输出 []（收尾）
- 上一步是 critic 且产出包含 "VERDICT: FAIL"，**只想看最近一版**：
    输出 [{coder 根据反馈修复, refs:["coder","critic"]}]，下一轮 critic 再审用 refs:["coder"]
  **想看所有历史版本**（强烈建议第 2 轮起这么做，避免重复踩坑）：
    refs 改用 0-based int 列出所有 coder/critic 的 [N]，例如已完成 4 步时 refs:[0,1,2,3]
- 已经循环 3 次以上 critic 仍 FAIL → 输出 []（强制收尾，避免无限循环）` + RefsPromptHelp

	return Scenario{
		Name:          "code_review",
		Description:   "代码 review/fix 循环：critic 反馈决定还要不要再修",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"coder":  coder,
			"critic": critic,
		},
		Fallback: coder,
		DialogueScript: []string{
			`实现一个 Go 函数 FizzBuzz(n int) []string，返回 1..n 的 fizzbuzz 序列`,
			`再实现一个并发安全的 LRU 缓存`,
		},
		ReplanHint: "critic FAIL → 追加 coder+critic；critic PASS → 直接收尾",
	}
}
