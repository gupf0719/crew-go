package scenarios

import (
	"context"
	"fmt"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/tool"
)

// scenario_default 是基线场景：researcher → writer → critic 写作流水线。
// 任务结构线性，replan 价值低，作为对照组帮助看出其他场景里 replan 的差异。
func init() {
	Register("default", buildDefaultScenario)
}

func buildDefaultScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	searchTool := tool.NewFuncTool(
		"search",
		"根据关键词检索资料，返回相关要点的字符串。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
			"required": []string{"query"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			q, _ := args["query"].(string)
			return tool.Success(fmt.Sprintf("[mock search] 关于 %q 的要点：\n- 要点 A\n- 要点 B\n- 要点 C", q)), nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(searchTool)

	// ISSUE-72 R7：每个 sub-agent 通过 WithDescription 自描述，PlannerPrompt 不再
	// 硬编码"可选 agent"列表——webPlanner / LLMPlanner 自动抽 description 注入 prompt。
	researcher := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("researcher"),
		agent_v2.WithDescription("查资料、列事实——必要时调用 search 工具检索"),
		agent_v2.WithSystemPrompt("你是资料调研员。收到子任务后，必要时调用 search 工具，最终输出结构化的资料要点。"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	writer := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("writer"),
		agent_v2.WithDescription("整理资料、写流畅的段落或教程章节"),
		agent_v2.WithSystemPrompt("你是文案作者。基于已有资料要点，写出流畅的段落或教程章节。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	critic := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("critic"),
		agent_v2.WithDescription("审查前序产出在事实/逻辑/结构上的问题，给出改进建议"),
		agent_v2.WithSystemPrompt("你是审稿人。指出前序产出在事实、逻辑、结构上的问题，给出改进建议。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。每次被调用时，会拿到原始目标和已完成步骤摘要，
负责输出"下一步要执行什么"的 JSON 数组（每次只 1 步）。已完成的步骤不要重复输出；如果目标已达成，
输出空数组 []。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"，不要 markdown 代码块、不要解释。

可选 agent 列表（含各自职责）由 user 消息动态注入，请按目标和已完成步骤选最合适的一个。

规划规则（按 executed 状态判断，每次只输出 1 步）：
- 没有 executed → 输出 [{"desc":"查资料 ...","agent":"researcher","refs":[]}]
- 上一步是 researcher → 输出 [{"desc":"基于资料写段落 ...","agent":"writer","refs":["researcher"]}]
- 上一步是 writer → 输出 [{"desc":"审查段落 ...","agent":"critic","refs":["writer"]}]
- 上一步是 critic → 输出 []（收尾）` + RefsPromptHelp

	return Scenario{
		Name:          "default",
		Description:   "基线：researcher/writer/critic 线性写作（replan 价值低，仅作对照）",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"researcher": researcher,
			"writer":     writer,
			"critic":     critic,
		},
		Fallback: writer,
		DialogueScript: []string{
			`整理 "Go goroutine 调度" 的博客素材，要有批评视角`,
			`再加一段代码示例`,
			`去掉批评部分，改成面向初学者的入门教程`,
		},
		ReplanHint: "线性流水线，replan 通常只切片不改动，作为基线对照",
	}
}
